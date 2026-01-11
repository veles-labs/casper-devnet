use std::{
    collections::BTreeMap,
    mem,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
};

use anyhow::{bail, Result};
use semver::Version;
use serde::{Deserialize, Serialize};
use tokio::fs as tokio_fs;
use tokio::process::Command;
use tracing::{debug, info, warn};

use super::utils::{self, NodeExitCode};

/// The name of the file for the on-disk record of the node-launcher state.
const STATE_FILE_NAME: &str = "casper-node-launcher-state.toml";
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
pub struct Launcher {
    binary_root_dir: PathBuf,
    config_root_dir: PathBuf,
    state: State,
    child_pid: Arc<AtomicU32>,
    env: BTreeMap<String, String>,
    stdout_path: Option<PathBuf>,
    stderr_path: Option<PathBuf>,
    cwd: Option<PathBuf>,
    rust_log: Option<String>,
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
            rust_log: None,
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

    /// Set environment variables for the node process.
    pub fn set_envs(&mut self, env: BTreeMap<String, String>) {
        self.env = env;
    }

    /// Override `RUST_LOG` for the node process.
    pub fn set_rust_log(&mut self, rust_log: String) {
        self.rust_log = Some(rust_log);
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
        self.config_root_dir.join(STATE_FILE_NAME)
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

    async fn configure_command(&self, command: &mut Command) -> Result<()> {
        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }
        if let (Some(stdout_path), Some(stderr_path)) = (&self.stdout_path, &self.stderr_path) {
            let stdout = tokio_fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(stdout_path)
                .await?;
            let stderr = tokio_fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(stderr_path)
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

                let new_info = self.new_node_info(next_version);
                State::MigrateData { old_info, new_info }
            }
            State::MigrateData { new_info, .. } => State::RunNodeAsValidator(new_info),
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
                let mut command = Command::new(&node_info.binary_path);
                command
                    .arg(VALIDATOR_SUBCOMMAND)
                    .arg(&node_info.config_path);
                self.configure_command(&mut command).await?;
                let exit_code = utils::run_node(command, self.child_pid.as_ref()).await?;
                info!(version=%node_info.version, "finished running node as validator");
                exit_code
            }
            State::MigrateData { old_info, new_info } => {
                let mut command = Command::new(&new_info.binary_path);
                command
                    .arg(MIGRATE_SUBCOMMAND)
                    .arg(OLD_CONFIG_ARG)
                    .arg(&old_info.config_path)
                    .arg(NEW_CONFIG_ARG)
                    .arg(&new_info.config_path);
                self.configure_command(&mut command).await?;
                let exit_code = utils::run_node(command, self.child_pid.as_ref()).await?;
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
}
