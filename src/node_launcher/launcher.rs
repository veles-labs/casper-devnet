use crate::assets::{self, PendingPostStageProtocolHookResult};
use crate::consensus_key_provider::{ConsensusKeyProvider, ConsensusKeyProviderConfig};
use std::{
    collections::BTreeMap,
    mem,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
};

use anyhow::{Result, anyhow, bail};
use futures::FutureExt;
use semver::Version;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::fs as tokio_fs;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tracing::{debug, info, warn};

use super::utils::{self, NodeExitCode};

/// The name of the per-node file containing the embedded launcher state.
pub(crate) const NODE_LAUNCHER_STATE_FILE: &str = "casper-node-launcher-state.toml";
/// The name of the casper-node binary.
const NODE_BINARY_NAME: &str = "casper-node";
/// The name of the config file for casper-node.
const NODE_CONFIG_NAME: &str = "config.toml";

/// The subcommands and args for casper-node.
const MIGRATE_SUBCOMMAND: &str = "migrate-data";
const OLD_CONFIG_ARG: &str = "--old-config";
const NEW_CONFIG_ARG: &str = "--new-config";
const VALIDATOR_SUBCOMMAND: &str = "validator";

/// Details of the node and its files.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct NodeInfo {
    /// The version of the node software.
    pub version: Version,
    /// The path to the node binary.
    pub binary_path: PathBuf,
    /// The path to the node's config file.
    pub config_path: PathBuf,
}

/// The state of the launcher, cached to disk every time it changes.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug)]
#[serde(tag = "mode")]
enum State {
    RunNodeAsValidator(NodeInfo),
    MigrateData {
        old_info: NodeInfo,
        new_info: NodeInfo,
    },
}

impl Default for State {
    fn default() -> Self {
        let node_info = NodeInfo {
            version: Version::new(0, 0, 0),
            binary_path: PathBuf::new(),
            config_path: PathBuf::new(),
        };
        State::RunNodeAsValidator(node_info)
    }
}

/// The object responsible for running casper-node as a child process.
///
/// It operates as a state machine, iterating between running the node in validator mode and running
/// it in migrate-data mode.
///
/// At each state transition, it caches its state to disk so that it can resume the same operation
/// if restarted.
#[derive(Debug)]
struct HookContext {
    hooks_dir: PathBuf,
    network_dir: PathBuf,
}

#[derive(Debug)]
pub struct Launcher {
    binary_root_dir: PathBuf,
    config_root_dir: PathBuf,
    state: State,
    child_pid: Arc<AtomicU32>,
    env: BTreeMap<String, String>,
    stdout_path: Option<PathBuf>,
    stderr_path: Option<PathBuf>,
    cwd: Option<PathBuf>,
    hook_context: Option<HookContext>,
    rust_log: Option<String>,
    consensus_key_provider: Option<ConsensusKeyProviderConfig>,
}

impl Launcher {
    /// Create a launcher rooted at the given binary/config directories.
    pub async fn new_with_roots(
        forced_version: Option<Version>,
        binary_root_dir: PathBuf,
        config_root_dir: PathBuf,
    ) -> Result<Self> {
        let installed_binary_versions = utils::versions_from_path(&binary_root_dir).await?;
        let installed_config_versions = utils::versions_from_path(&config_root_dir).await?;

        if installed_binary_versions != installed_config_versions {
            bail!(
                "installed binary versions ({}) don't match installed configs ({})",
                utils::iter_to_string(installed_binary_versions),
                utils::iter_to_string(installed_config_versions),
            );
        }

        let mut launcher = Launcher {
            binary_root_dir,
            config_root_dir,
            state: Default::default(),
            child_pid: Arc::new(AtomicU32::new(0)),
            env: BTreeMap::new(),
            stdout_path: None,
            stderr_path: None,
            cwd: None,
            hook_context: None,
            rust_log: None,
            consensus_key_provider: None,
        };

        match forced_version {
            Some(forced_version) => {
                if installed_binary_versions.contains(&forced_version) {
                    launcher
                        .set_state(State::RunNodeAsValidator(
                            launcher.new_node_info(forced_version),
                        ))
                        .await?;
                    Ok(launcher)
                } else {
                    info!(%forced_version, "the requested version is not installed");
                    bail!(
                        "the requested version ({}) is not installed",
                        forced_version
                    )
                }
            }
            None => {
                let maybe_state = launcher.try_load_state().await?;
                match maybe_state {
                    Some(read_state) => {
                        info!(path=%launcher.state_path().display(), "read stored state");
                        launcher.state = read_state;
                        Ok(launcher)
                    }
                    None => {
                        let node_info =
                            launcher.new_node_info(launcher.most_recent_version().await?);
                        launcher
                            .set_state(State::RunNodeAsValidator(node_info))
                            .await?;
                        Ok(launcher)
                    }
                }
            }
        }
    }

    /// Redirect node stdout/stderr to the provided files.
    pub fn set_log_paths(&mut self, stdout_path: PathBuf, stderr_path: PathBuf) {
        self.stdout_path = Some(stdout_path);
        self.stderr_path = Some(stderr_path);
    }

    /// Set the working directory for node processes.
    pub fn set_cwd(&mut self, cwd: PathBuf) {
        self.cwd = Some(cwd);
    }

    /// Set the hook execution context for network-scoped post-stage hooks.
    pub fn set_hook_context(&mut self, network_dir: PathBuf, hooks_dir: PathBuf) {
        self.hook_context = Some(HookContext {
            hooks_dir,
            network_dir,
        });
    }

    /// Set environment variables for the node process.
    pub fn set_envs(&mut self, env: BTreeMap<String, String>) {
        self.env = env;
    }

    /// Override `RUST_LOG` for the node process.
    pub fn set_rust_log(&mut self, rust_log: String) {
        self.rust_log = Some(rust_log);
    }

    /// Configure one-shot inherited pipe consensus key delivery for node child processes.
    pub(crate) fn set_consensus_key_provider(&mut self, config: ConsensusKeyProviderConfig) {
        self.consensus_key_provider = Some(config);
    }

    /// Atomically tracks the PID of the currently running node.
    pub fn child_pid(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.child_pid)
    }

    /// Snapshot the current node command and args based on launcher state.
    pub fn current_command(&self) -> (PathBuf, Vec<String>) {
        match &self.state {
            State::RunNodeAsValidator(node_info) => (
                node_info.binary_path.clone(),
                vec![
                    VALIDATOR_SUBCOMMAND.to_string(),
                    node_info.config_path.to_string_lossy().to_string(),
                ],
            ),
            State::MigrateData { old_info, new_info } => (
                new_info.binary_path.clone(),
                vec![
                    MIGRATE_SUBCOMMAND.to_string(),
                    OLD_CONFIG_ARG.to_string(),
                    old_info.config_path.to_string_lossy().to_string(),
                    NEW_CONFIG_ARG.to_string(),
                    new_info.config_path.to_string_lossy().to_string(),
                ],
            ),
        }
    }

    /// Run the launcher loop, exiting cleanly when shutdown is requested.
    pub async fn run_with_shutdown(&mut self, shutdown: &AtomicBool) -> Result<()> {
        loop {
            if shutdown.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.step().await?;
            if shutdown.load(Ordering::SeqCst) {
                return Ok(());
            }
        }
    }

    /// Provides the path of the file for recording the state of the node-launcher.
    fn state_path(&self) -> PathBuf {
        self.config_root_dir.join(NODE_LAUNCHER_STATE_FILE)
    }

    /// Sets the given launcher state and stores it on disk.
    async fn set_state(&mut self, state: State) -> Result<()> {
        self.state = state;
        self.write().await
    }

    /// Tries to load the stored state from disk.
    async fn try_load_state(&self) -> Result<Option<State>> {
        let state_path = self.state_path();
        if tokio_fs::metadata(&state_path).await.is_ok() {
            debug!(path=%state_path.display(), "trying to read stored state");
            let contents = utils::map_and_log_error(
                tokio_fs::read_to_string(&state_path).await,
                format!("failed to read {}", state_path.display()),
            )?;

            Ok(Some(utils::map_and_log_error(
                toml::from_str(&contents),
                format!("failed to parse {}", state_path.display()),
            )?))
        } else {
            debug!(path=%state_path.display(), "stored state doesn't exist");
            Ok(None)
        }
    }

    /// Writes `self` to the hard-coded location as a TOML-encoded file.
    async fn write(&self) -> Result<()> {
        let path = self.state_path();
        debug!(path=%path.display(), "trying to store state");
        let contents = utils::map_and_log_error(
            toml::to_string_pretty(&self.state),
            "failed to encode state as toml".to_string(),
        )?;
        utils::map_and_log_error(
            tokio_fs::write(&path, contents.as_bytes()).await,
            format!("failed to write {}", path.display()),
        )?;
        info!(path=%path.display(), state=?self.state, "stored state");
        Ok(())
    }

    /// Gets the most recent installed binary version.
    ///
    /// Returns an error when no correct versions can be detected.
    async fn most_recent_version(&self) -> Result<Version> {
        let all_versions = utils::versions_from_path(&self.binary_root_dir).await?;

        // We are guaranteed to have at least one version in the `all_versions` container,
        // because if there are no valid versions installed the `utils::versions_from_path()` bails.
        Ok(all_versions
            .into_iter()
            .next_back()
            .expect("must have at least one version"))
    }

    /// Gets the next installed version of the node binary and config.
    ///
    /// Returns an error if the versions cannot be deduced, or if the two versions are different.
    async fn next_installed_version(&self, current_version: &Version) -> Result<Version> {
        let next_binary_version =
            utils::next_installed_version(&self.binary_root_dir, current_version).await?;
        let next_config_version =
            utils::next_installed_version(&self.config_root_dir, current_version).await?;
        if next_config_version != next_binary_version {
            warn!(%next_binary_version, %next_config_version, "next version mismatch");
            bail!(
                "next binary version {} != next config version {}",
                next_binary_version,
                next_config_version,
            );
        }
        Ok(next_binary_version)
    }

    /// Gets the previous installed version of the node binary and config.
    ///
    /// Returns an error if the versions cannot be deduced, or if the two versions are different.
    async fn previous_installed_version(&self, current_version: &Version) -> Result<Version> {
        let previous_binary_version =
            utils::previous_installed_version(&self.binary_root_dir, current_version).await?;
        let previous_config_version =
            utils::previous_installed_version(&self.config_root_dir, current_version).await?;
        if previous_config_version != previous_binary_version {
            warn!(%previous_binary_version, %previous_config_version, "previous version mismatch");
            bail!(
                "previous binary version {} != previous config version {}",
                previous_binary_version,
                previous_config_version,
            );
        }
        Ok(previous_binary_version)
    }

    /// Constructs a new `NodeInfo` based on the given version.
    fn new_node_info(&self, version: Version) -> NodeInfo {
        let subdir_name = version.to_string().replace('.', "_");
        NodeInfo {
            version,
            binary_path: self
                .binary_root_dir
                .join(&subdir_name)
                .join(NODE_BINARY_NAME),
            config_path: self
                .config_root_dir
                .join(&subdir_name)
                .join(NODE_CONFIG_NAME),
        }
    }

    fn active_log_version_fs(&self) -> String {
        match &self.state {
            State::RunNodeAsValidator(node_info) => node_info.version.to_string().replace('.', "_"),
            State::MigrateData { new_info, .. } => new_info.version.to_string().replace('.', "_"),
        }
    }

    fn versioned_log_target(alias_path: &Path, version_fs: &str) -> Result<PathBuf> {
        let parent = alias_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("log path {} has no parent", alias_path.display()))?;
        let file_name = alias_path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("log path {} has no file name", alias_path.display()))?;
        let file_name = file_name.to_string_lossy();

        if let Some((base, ext)) = file_name.rsplit_once('.') {
            Ok(parent.join(format!("{base}-{version_fs}.{ext}")))
        } else {
            Ok(parent.join(format!("{file_name}-{version_fs}")))
        }
    }

    async fn replace_symlink_atomic(link_path: &Path, target_path: &Path) -> Result<()> {
        let parent = link_path.parent().ok_or_else(|| {
            anyhow::anyhow!("log link path {} has no parent", link_path.display())
        })?;
        tokio_fs::create_dir_all(parent).await?;

        let file_name = link_path.file_name().ok_or_else(|| {
            anyhow::anyhow!("log link path {} has no file name", link_path.display())
        })?;
        let file_name = file_name.to_string_lossy();
        let tmp_link = parent.join(format!(".{file_name}.tmp-{}", std::process::id()));

        if let Ok(metadata) = tokio_fs::symlink_metadata(link_path).await
            && metadata.is_dir()
        {
            tokio_fs::remove_dir_all(link_path).await?;
        }
        let _ = tokio_fs::remove_file(&tmp_link).await;
        tokio_fs::symlink(target_path, &tmp_link).await?;
        tokio_fs::rename(&tmp_link, link_path).await?;
        Ok(())
    }

    async fn prepare_versioned_log_alias(alias_path: &Path, version_fs: &str) -> Result<PathBuf> {
        let target_path = Self::versioned_log_target(alias_path, version_fs)?;
        let parent = alias_path.parent().ok_or_else(|| {
            anyhow::anyhow!("log alias path {} has no parent", alias_path.display())
        })?;
        tokio_fs::create_dir_all(parent).await?;

        if let Ok(metadata) = tokio_fs::symlink_metadata(alias_path).await {
            if metadata.is_dir() {
                tokio_fs::remove_dir_all(alias_path).await?;
            } else if !metadata.file_type().is_symlink() {
                if tokio_fs::symlink_metadata(&target_path).await.is_err() {
                    tokio_fs::rename(alias_path, &target_path).await?;
                } else {
                    tokio_fs::remove_file(alias_path).await?;
                }
            }
        }

        Self::replace_symlink_atomic(alias_path, &target_path).await?;
        Ok(target_path)
    }

    async fn configure_command(&self, command: &mut Command) -> Result<()> {
        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }
        if let (Some(stdout_path), Some(stderr_path)) = (&self.stdout_path, &self.stderr_path) {
            let version_fs = self.active_log_version_fs();
            let stdout_target = Self::prepare_versioned_log_alias(stdout_path, &version_fs).await?;
            let stderr_target = Self::prepare_versioned_log_alias(stderr_path, &version_fs).await?;
            let stdout = tokio_fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&stdout_target)
                .await?;
            let stderr = tokio_fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&stderr_target)
                .await?;
            let stdout = stdout.into_std().await;
            let stderr = stderr.into_std().await;
            command.stdout(stdout).stderr(stderr);
        }
        if !self.env.is_empty() {
            command.envs(&self.env);
        }
        if let Some(rust_log) = &self.rust_log {
            command.env_remove("RUST_LOG");
            command.env("RUST_LOG", rust_log);
        }

        Ok(())
    }

    async fn append_launcher_note(&self, message: impl AsRef<str>) {
        let Some(stdout_path) = &self.stdout_path else {
            return;
        };

        let timestamp = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "unknown-time".to_string());
        let line = format!("{timestamp} {}\n", message.as_ref());

        let mut file = match tokio_fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(stdout_path)
            .await
        {
            Ok(file) => file,
            Err(error) => {
                warn!(
                    path=%stdout_path.display(),
                    %error,
                    "failed to open node stdout log for launcher note"
                );
                return;
            }
        };

        if let Err(error) = file.write_all(line.as_bytes()).await {
            warn!(
                path=%stdout_path.display(),
                %error,
                "failed to append launcher note to node stdout log"
            );
        }
    }

    /// Sets `self.state` to a new state corresponding to upgrading the current node version.
    ///
    /// If `self.state` is currently `RunNodeAsValidator`, then finds the next installed version
    /// and moves to `MigrateData` if that version is newer (else errors).  If it's currently
    /// `MigrateData`, moves to `RunNodeAsValidator` using the next installed version.
    async fn upgrade_state(&mut self) -> Result<()> {
        let new_state = match mem::take(&mut self.state) {
            State::RunNodeAsValidator(old_info) => {
                let next_version = self.next_installed_version(&old_info.version).await?;
                if next_version <= old_info.version {
                    let msg = format!(
                        "no higher version than current {} installed",
                        old_info.version
                    );
                    warn!("{}", msg);
                    bail!(msg);
                }

                info!(
                    old_version=%old_info.version,
                    new_version=%next_version,
                    "detected upgrade boundary from node exit code 0"
                );
                self.append_launcher_note(format!(
                    "launcher detected upgrade boundary (node exited with code 0); migrating {} -> {}",
                    old_info.version, next_version
                ))
                .await;

                let new_info = self.new_node_info(next_version);
                State::MigrateData { old_info, new_info }
            }
            State::MigrateData { new_info, .. } => {
                self.append_launcher_note(format!(
                    "launcher finished migrate-data; starting validator {}",
                    new_info.version
                ))
                .await;
                State::RunNodeAsValidator(new_info)
            }
        };

        self.state = new_state;
        Ok(())
    }

    /// Sets `self.state` to a new state corresponding to downgrading the current node version.
    ///
    /// Regardless of the current state variant, the returned state is `RunNodeAsValidator` with the
    /// previous installed version.
    async fn downgrade_state(&mut self) -> Result<()> {
        let node_info = match &self.state {
            State::RunNodeAsValidator(old_info) => old_info,
            State::MigrateData { new_info, .. } => new_info,
        };

        let previous_version = self.previous_installed_version(&node_info.version).await?;
        if previous_version >= node_info.version {
            let msg = format!(
                "no lower version than current {} installed",
                node_info.version
            );
            warn!("{}", msg);
            bail!(msg);
        }

        let new_info = self.new_node_info(previous_version);
        self.state = State::RunNodeAsValidator(new_info);
        Ok(())
    }

    /// Moves the launcher state forward.
    async fn transition_state(&mut self, previous_exit_code: NodeExitCode) -> Result<()> {
        match previous_exit_code {
            NodeExitCode::Success => self.upgrade_state().await?,
            NodeExitCode::ShouldDowngrade => self.downgrade_state().await?,
            NodeExitCode::ShouldExitLauncher => {
                bail!("node requested launcher exit");
            }
        }
        self.write().await
    }

    /// Runs the process for the current state and moves the state forward if the process exits with
    /// success.
    async fn step(&mut self) -> Result<()> {
        let exit_code = match &self.state {
            State::RunNodeAsValidator(node_info) => {
                let provider = self
                    .prepare_consensus_key_provider(std::slice::from_ref(&node_info.config_path))
                    .await?;
                let config_path = provider
                    .as_ref()
                    .map(|provider| provider.config_paths()[0].clone())
                    .unwrap_or_else(|| node_info.config_path.clone());
                let mut command = Command::new(&node_info.binary_path);
                command.arg(VALIDATOR_SUBCOMMAND).arg(&config_path);
                self.configure_command(&mut command).await?;
                let exit_code = self
                    .run_validator(command, &node_info.version, provider)
                    .await?;
                info!(version=%node_info.version, "finished running node as validator");
                exit_code
            }
            State::MigrateData { old_info, new_info } => {
                let provider = self
                    .prepare_consensus_key_provider(&[
                        old_info.config_path.clone(),
                        new_info.config_path.clone(),
                    ])
                    .await?;
                let config_paths = provider.as_ref().map_or_else(
                    || vec![old_info.config_path.clone(), new_info.config_path.clone()],
                    ConsensusKeyProvider::config_paths,
                );
                let mut command = Command::new(&new_info.binary_path);
                command
                    .arg(MIGRATE_SUBCOMMAND)
                    .arg(OLD_CONFIG_ARG)
                    .arg(&config_paths[0])
                    .arg(NEW_CONFIG_ARG)
                    .arg(&config_paths[1]);
                self.configure_command(&mut command).await?;
                let exit_status = self.run_node_command(&mut command, provider, None).await?;
                let exit_code = node_exit_code_from_status(&command, exit_status)?;
                info!(
                    old_version=%old_info.version,
                    new_version=%new_info.version,
                    "finished data migration"
                );
                exit_code
            }
        };

        self.transition_state(exit_code).await
    }

    async fn prepare_consensus_key_provider(
        &self,
        config_paths: &[PathBuf],
    ) -> Result<Option<ConsensusKeyProvider>> {
        match &self.consensus_key_provider {
            Some(config) => Ok(Some(config.prepare(config_paths).await?)),
            None => Ok(None),
        }
    }

    async fn run_validator(
        &self,
        mut command: Command,
        version: &Version,
        provider: Option<ConsensusKeyProvider>,
    ) -> Result<NodeExitCode> {
        let exit_status = self
            .run_node_command(&mut command, provider, Some(version))
            .await?;
        node_exit_code_from_status(&command, exit_status)
    }

    async fn run_node_command(
        &self,
        command: &mut Command,
        mut provider: Option<ConsensusKeyProvider>,
        post_stage_version: Option<&Version>,
    ) -> Result<ExitStatus> {
        let command_debug = format!("{command:?}");
        if let Some(provider) = provider.as_ref() {
            provider.install_on_command(command);
        }

        let mut child = match utils::map_and_log_error(
            command.spawn(),
            format!("failed to execute {command_debug}"),
        ) {
            Ok(child) => child,
            Err(err) => {
                if let Some(provider) = provider {
                    let cleanup = provider.cleanup().await;
                    if let Err(cleanup_err) = cleanup {
                        return Err(anyhow!("{err}; additionally {cleanup_err}"));
                    }
                }
                return Err(err);
            }
        };
        if let Some(pid) = child.id() {
            self.child_pid.store(pid, Ordering::SeqCst);
        }

        let provider_task = if provider.is_some() {
            let delivery_task = {
                let provider = provider.as_mut().expect("provider checked above");
                provider.close_parent_read_ends();
                provider.spawn_delivery(self.stdout_path.clone())
            };
            match delivery_task {
                Ok(task) => Some(task.fuse()),
                Err(err) => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    self.child_pid.store(0, Ordering::SeqCst);
                    let cleanup = match provider.take() {
                        Some(provider) => provider.cleanup().await,
                        None => Ok(()),
                    };
                    if let Err(cleanup_err) = cleanup {
                        return Err(anyhow!("{err}; additionally {cleanup_err}"));
                    }
                    return Err(err);
                }
            }
        } else {
            None
        };

        if let Some(version) = post_stage_version {
            self.spawn_post_stage_protocol_hook(version);
        }

        let exit_status = match provider_task {
            Some(provider_task) => {
                wait_for_child_and_key_provider(&mut child, provider_task, &command_debug).await
            }
            None => utils::map_and_log_error(
                child.wait().await,
                format!("failed to wait for completion of {command_debug}"),
            ),
        };
        self.child_pid.store(0, Ordering::SeqCst);

        let cleanup = match provider {
            Some(provider) => provider.cleanup().await,
            None => Ok(()),
        };
        match (exit_status, cleanup) {
            (Ok(exit_status), Ok(())) => Ok(exit_status),
            (Err(err), Ok(())) => Err(err),
            (Ok(_), Err(err)) => Err(err),
            (Err(err), Err(cleanup_err)) => Err(anyhow!("{err}; additionally {cleanup_err}")),
        }
    }

    fn spawn_post_stage_protocol_hook(&self, version: &Version) {
        let Some(hook_context) = &self.hook_context else {
            return;
        };
        let hook_context = HookContext {
            hooks_dir: hook_context.hooks_dir.clone(),
            network_dir: hook_context.network_dir.clone(),
        };
        let version = version.clone();
        tokio::spawn(async move {
            match assets::run_pending_post_stage_protocol_hook(
                &hook_context.hooks_dir,
                &hook_context.network_dir,
                &version,
            )
            .await
            {
                Ok(PendingPostStageProtocolHookResult::NotRun)
                | Ok(PendingPostStageProtocolHookResult::Succeeded) => {}
                Ok(PendingPostStageProtocolHookResult::Failed(error)) => {
                    warn!(
                        %error,
                        %version,
                        network_dir=%hook_context.network_dir.display(),
                        "post-stage-protocol hook failed"
                    );
                }
                Err(error) => {
                    warn!(
                        %error,
                        %version,
                        network_dir=%hook_context.network_dir.display(),
                        "failed to run post-stage-protocol hook"
                    );
                }
            }
        });
    }
}

async fn wait_for_child_and_key_provider(
    child: &mut tokio::process::Child,
    mut provider_task: futures::future::Fuse<tokio::task::JoinHandle<Result<()>>>,
    command_debug: &str,
) -> Result<ExitStatus> {
    let mut provider_done = false;
    loop {
        tokio::select! {
            exit_status = child.wait() => {
                let exit_status = utils::map_and_log_error(
                    exit_status,
                    format!("failed to wait for completion of {command_debug}"),
                )?;
                if !provider_done {
                    let provider_result = provider_task
                        .await
                        .map_err(|err| anyhow!("consensus key provider task failed: {}", err))?;
                    provider_result?;
                }
                return Ok(exit_status);
            }
            provider_join = &mut provider_task, if !provider_done => {
                provider_done = true;
                let provider_result = provider_join
                    .map_err(|err| anyhow!("consensus key provider task failed: {}", err))?;
                if let Err(err) = provider_result {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    return Err(err);
                }
            }
        }
    }
}

fn node_exit_code_from_status(command: &Command, exit_status: ExitStatus) -> Result<NodeExitCode> {
    match exit_status.code() {
        Some(code) if code == NodeExitCode::Success as i32 => Ok(NodeExitCode::Success),
        Some(code) if code == NodeExitCode::ShouldDowngrade as i32 => {
            Ok(NodeExitCode::ShouldDowngrade)
        }
        Some(code) if code == NodeExitCode::ShouldExitLauncher as i32 => {
            Ok(NodeExitCode::ShouldExitLauncher)
        }
        _ => {
            warn!(%exit_status, "failed running {command:?}");
            bail!("{command:?} exited with error");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;
    use tokio::time::{Duration, sleep};

    fn test_launcher(network_dir: &Path, hooks_dir: &Path) -> Launcher {
        Launcher {
            binary_root_dir: PathBuf::new(),
            config_root_dir: PathBuf::new(),
            state: State::default(),
            child_pid: Arc::new(AtomicU32::new(0)),
            env: BTreeMap::new(),
            stdout_path: None,
            stderr_path: None,
            cwd: None,
            hook_context: Some(HookContext {
                hooks_dir: hooks_dir.to_path_buf(),
                network_dir: network_dir.to_path_buf(),
            }),
            rust_log: None,
            consensus_key_provider: None,
        }
    }

    async fn write_executable(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            tokio_fs::create_dir_all(parent).await.unwrap();
        }
        tokio_fs::write(path, contents).await.unwrap();
        tokio_fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
            .await
            .unwrap();
    }

    async fn write_pending_post_hook(
        hooks_dir: &Path,
        command_path: &Path,
        network_name: &str,
        protocol_version: &str,
        activation_point: u64,
    ) {
        let version_fs = protocol_version.replace('.', "_");
        let logs_dir = hooks_dir.join("logs");
        tokio_fs::create_dir_all(hooks_dir.join(".pending"))
            .await
            .unwrap();
        tokio_fs::create_dir_all(&logs_dir).await.unwrap();
        let pending = serde_json::json!({
            "asset_name": "dev",
            "network_name": network_name,
            "protocol_version": protocol_version,
            "activation_point": activation_point,
            "command_path": command_path,
            "stdout_path": logs_dir.join(format!("post-stage-protocol-{version_fs}.stdout.log")),
            "stderr_path": logs_dir.join(format!("post-stage-protocol-{version_fs}.stderr.log")),
        });
        tokio_fs::write(
            hooks_dir
                .join(".pending")
                .join(format!("post-stage-protocol-{version_fs}.json")),
            serde_json::to_vec_pretty(&pending).unwrap(),
        )
        .await
        .unwrap();
    }

    async fn wait_for_path(path: &Path) {
        for _ in 0..200 {
            if tokio_fs::metadata(path).await.is_ok() {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {}", path.display());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn post_hook_runs_once_with_network_cwd_and_log_redirection() {
        let temp_dir = TempDir::new().unwrap();
        let network_dir = temp_dir.path().join("casper-dev");
        let hooks_dir = crate::assets::network_hooks_dir(&network_dir);
        tokio_fs::create_dir_all(&network_dir).await.unwrap();

        let hook_script = temp_dir.path().join("post-hook.sh");
        write_executable(
            &hook_script,
            "#!/bin/sh\nset -eu\nprintf '%s\n' \"$PWD\" > \"$PWD/post-hook-cwd\"\nprintf '%s,%s\n' \"$1\" \"$2\" > \"$PWD/post-hook-args\"\necho hook-stdout\necho hook-stderr >&2\necho run >> \"$PWD/post-hook-count\"\n",
        )
        .await;
        write_pending_post_hook(&hooks_dir, &hook_script, "casper-dev", "2.0.0", 123).await;

        let launcher_a = test_launcher(&network_dir, &hooks_dir);
        let launcher_b = test_launcher(&network_dir, &hooks_dir);
        let version = Version::new(2, 0, 0);
        launcher_a.spawn_post_stage_protocol_hook(&version);
        launcher_b.spawn_post_stage_protocol_hook(&version);

        let completion_path = hooks_dir
            .join(".status")
            .join("post-stage-protocol-2_0_0.json");
        wait_for_path(&completion_path).await;

        let expected_hook_dir = tokio_fs::canonicalize(crate::assets::network_hook_work_dir(
            &network_dir,
            "post-stage-protocol",
        ))
        .await
        .unwrap();
        assert_eq!(
            tokio_fs::read_to_string(
                crate::assets::network_hook_work_dir(&network_dir, "post-stage-protocol")
                    .join("post-hook-cwd"),
            )
            .await
            .unwrap()
            .trim(),
            expected_hook_dir.display().to_string()
        );
        assert_eq!(
            tokio_fs::read_to_string(
                crate::assets::network_hook_work_dir(&network_dir, "post-stage-protocol")
                    .join("post-hook-args"),
            )
            .await
            .unwrap()
            .trim(),
            "casper-dev,2.0.0"
        );
        assert_eq!(
            tokio_fs::read_to_string(
                crate::assets::network_hook_work_dir(&network_dir, "post-stage-protocol")
                    .join("post-hook-count"),
            )
            .await
            .unwrap()
            .lines()
            .count(),
            1
        );
        assert_eq!(
            tokio_fs::read_to_string(
                hooks_dir
                    .join("logs")
                    .join("post-stage-protocol-2_0_0.stdout.log"),
            )
            .await
            .unwrap()
            .trim(),
            "hook-stdout"
        );
        assert_eq!(
            tokio_fs::read_to_string(
                hooks_dir
                    .join("logs")
                    .join("post-stage-protocol-2_0_0.stderr.log"),
            )
            .await
            .unwrap()
            .trim(),
            "hook-stderr"
        );
        assert!(
            tokio_fs::metadata(
                hooks_dir
                    .join(".pending")
                    .join("post-stage-protocol-2_0_0.json"),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_post_hook_is_marked_consumed_after_first_attempt() {
        let temp_dir = TempDir::new().unwrap();
        let network_dir = temp_dir.path().join("casper-dev");
        let hooks_dir = crate::assets::network_hooks_dir(&network_dir);
        tokio_fs::create_dir_all(&network_dir).await.unwrap();

        let hook_script = temp_dir.path().join("post-hook-fail.sh");
        write_executable(
            &hook_script,
            "#!/bin/sh\nset -eu\necho run >> \"$PWD/post-hook-count\"\necho failing >&2\nexit 7\n",
        )
        .await;
        write_pending_post_hook(&hooks_dir, &hook_script, "casper-dev", "2.0.0", 123).await;

        let launcher = test_launcher(&network_dir, &hooks_dir);
        let version = Version::new(2, 0, 0);
        launcher.spawn_post_stage_protocol_hook(&version);

        let completion_path = hooks_dir
            .join(".status")
            .join("post-stage-protocol-2_0_0.json");
        wait_for_path(&completion_path).await;

        let completion: Value =
            serde_json::from_slice(&tokio_fs::read(&completion_path).await.unwrap()).unwrap();
        assert_eq!(completion["status"], "failure");
        assert_eq!(completion["exit_code"], 7);

        launcher.spawn_post_stage_protocol_hook(&version);
        sleep(Duration::from_millis(100)).await;

        assert_eq!(
            tokio_fs::read_to_string(
                crate::assets::network_hook_work_dir(&network_dir, "post-stage-protocol")
                    .join("post-hook-count"),
            )
            .await
            .unwrap()
            .lines()
            .count(),
            1
        );
        assert!(
            tokio_fs::metadata(
                hooks_dir
                    .join(".pending")
                    .join("post-stage-protocol-2_0_0.json"),
            )
            .await
            .is_err()
        );
    }
}
