use crate::assets::{self, AssetsLayout};
use anyhow::{Context, Result, anyhow, bail};
use nix::fcntl::{FcntlArg, FdFlag, fcntl};
use nix::unistd::{dup2, pipe};
use std::fs::File as StdFile;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::{Builder as TempFileBuilder, NamedTempFile};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::fs as tokio_fs;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

const SECRET_KEY_PEM: &str = "secret_key.pem";
const INHERITED_FD_BASE: RawFd = 42;

#[derive(Debug)]
pub(crate) struct ConsensusKeyProviderConfig {
    stale_key_path: PathBuf,
    node_id: u32,
    seed: Arc<str>,
}

impl ConsensusKeyProviderConfig {
    pub(crate) fn for_layout(layout: &AssetsLayout, node_id: u32, seed: Arc<str>) -> Self {
        let stale_key_path = layout.node_dir(node_id).join("keys").join(SECRET_KEY_PEM);
        Self {
            stale_key_path,
            node_id,
            seed,
        }
    }

    pub(crate) async fn prepare(&self, config_paths: &[PathBuf]) -> Result<ConsensusKeyProvider> {
        self.remove_stale_key_path().await?;
        if config_paths.is_empty() {
            bail!("consensus key pipe provider requires at least one config path");
        }

        let mut deliveries = Vec::with_capacity(config_paths.len());
        let mut temp_configs = Vec::with_capacity(config_paths.len());
        for (index, source_config_path) in config_paths.iter().enumerate() {
            let target_fd = INHERITED_FD_BASE + RawFd::try_from(index)?;
            let fd_path = inherited_fd_path(target_fd)?;
            let temp_config = create_temp_config(source_config_path, &fd_path).await?;
            let (read_fd, write_fd) = create_cloexec_pipe()?;
            let read_fd = move_read_fd_out_of_target_range(read_fd, config_paths.len())?;

            deliveries.push(KeyDelivery {
                target_fd,
                read_fd: Some(read_fd),
                write_fd: Some(write_fd),
                config_path: temp_config.path().to_path_buf(),
            });
            temp_configs.push(temp_config);
        }

        Ok(ConsensusKeyProvider {
            node_id: self.node_id,
            seed: Arc::clone(&self.seed),
            deliveries,
            temp_configs,
        })
    }

    async fn remove_stale_key_path(&self) -> Result<()> {
        if let Ok(metadata) = tokio_fs::symlink_metadata(&self.stale_key_path).await {
            if metadata.file_type().is_dir() {
                bail!(
                    "consensus key path {} is a directory",
                    self.stale_key_path.display()
                );
            }
            tokio_fs::remove_file(&self.stale_key_path)
                .await
                .with_context(|| {
                    format!(
                        "failed to remove stale consensus key path {}",
                        self.stale_key_path.display()
                    )
                })?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ConsensusKeyProvider {
    node_id: u32,
    seed: Arc<str>,
    deliveries: Vec<KeyDelivery>,
    temp_configs: Vec<NamedTempFile>,
}

impl ConsensusKeyProvider {
    pub(crate) fn config_paths(&self) -> Vec<PathBuf> {
        self.deliveries
            .iter()
            .map(|delivery| delivery.config_path.clone())
            .collect()
    }

    pub(crate) fn install_on_command(&self, command: &mut Command) {
        let read_fds = self
            .deliveries
            .iter()
            .filter_map(|delivery| {
                delivery
                    .read_fd
                    .as_ref()
                    .map(|read_fd| (read_fd.as_raw_fd(), delivery.target_fd))
            })
            .collect::<Vec<_>>();

        unsafe {
            command.pre_exec(move || install_inherited_pipe_fds(&read_fds));
        }
    }

    pub(crate) fn close_parent_read_ends(&mut self) {
        for delivery in &mut self.deliveries {
            delivery.read_fd.take();
        }
    }

    pub(crate) fn spawn_delivery(
        &mut self,
        log_path: Option<PathBuf>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let mut write_fds = Vec::with_capacity(self.deliveries.len());
        for delivery in &mut self.deliveries {
            let write_fd = delivery.write_fd.take().ok_or_else(|| {
                anyhow!(
                    "consensus key pipe for node-{} was already delivered",
                    self.node_id
                )
            })?;
            write_fds.push(write_fd);
        }

        let node_id = self.node_id;
        let seed = Arc::clone(&self.seed);
        Ok(tokio::spawn(async move {
            deliver_to_pipes(node_id, seed, write_fds, log_path).await
        }))
    }

    pub(crate) async fn cleanup(self) -> Result<()> {
        tokio::task::spawn_blocking(move || {
            for temp_config in self.temp_configs {
                let path = temp_config.path().to_path_buf();
                temp_config.close().with_context(|| {
                    format!("failed to remove temp node config {}", path.display())
                })?;
            }
            Ok(())
        })
        .await
        .map_err(|err| anyhow!("consensus key temp config cleanup task failed: {}", err))?
    }
}

#[derive(Debug)]
struct KeyDelivery {
    target_fd: RawFd,
    read_fd: Option<OwnedFd>,
    write_fd: Option<OwnedFd>,
    config_path: PathBuf,
}

async fn create_temp_config(source_config_path: &Path, fd_path: &str) -> Result<NamedTempFile> {
    let source_config_path = source_config_path.to_path_buf();
    let fd_path = fd_path.to_string();
    let config_contents = tokio_fs::read_to_string(&source_config_path)
        .await
        .with_context(|| {
            format!(
                "failed to read node config {}",
                source_config_path.display()
            )
        })?;

    tokio::task::spawn_blocking(move || -> Result<NamedTempFile> {
        let parent = source_config_path.parent().ok_or_else(|| {
            anyhow!(
                "node config path {} has no parent",
                source_config_path.display()
            )
        })?;
        let mut config_value: toml::Value =
            toml::from_str(&config_contents).with_context(|| {
                format!(
                    "failed to parse node config {}",
                    source_config_path.display()
                )
            })?;
        set_consensus_secret_key_path(&mut config_value, fd_path)?;
        let updated_config = toml::to_string(&config_value).with_context(|| {
            format!(
                "failed to encode temp node config {}",
                source_config_path.display()
            )
        })?;

        let mut temp_config = TempFileBuilder::new()
            .prefix(".config.consensus-key.")
            .suffix(".toml")
            .tempfile_in(parent)
            .with_context(|| {
                format!(
                    "failed to create temp node config beside {}",
                    source_config_path.display()
                )
            })?;
        temp_config.write_all(updated_config.as_bytes())?;
        temp_config.flush()?;
        Ok(temp_config)
    })
    .await
    .map_err(|err| anyhow!("temp node config task failed: {}", err))?
}

fn set_consensus_secret_key_path(root: &mut toml::Value, fd_path: String) -> Result<()> {
    let table = root
        .get_mut("consensus")
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| anyhow!("node config is missing [consensus] table"))?;
    table.insert("secret_key_path".to_string(), toml::Value::String(fd_path));
    Ok(())
}

fn inherited_fd_path(fd: RawFd) -> Result<String> {
    #[cfg(target_os = "linux")]
    {
        Ok(format!("/proc/self/fd/{fd}"))
    }

    #[cfg(target_os = "macos")]
    {
        Ok(format!("/dev/fd/{fd}"))
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        bail!(
            "inherited pipe consensus key delivery is unsupported on {}",
            std::env::consts::OS
        )
    }
}

fn create_cloexec_pipe() -> Result<(OwnedFd, OwnedFd)> {
    let (read_fd, write_fd) = pipe().context("failed to create consensus key pipe")?;
    set_close_on_exec(read_fd.as_raw_fd()).context("failed to mark pipe read fd close-on-exec")?;
    set_close_on_exec(write_fd.as_raw_fd())
        .context("failed to mark pipe write fd close-on-exec")?;
    Ok((read_fd, write_fd))
}

fn move_read_fd_out_of_target_range(read_fd: OwnedFd, target_count: usize) -> Result<OwnedFd> {
    let raw_fd = read_fd.as_raw_fd();
    let target_count = RawFd::try_from(target_count)?;
    let target_end = INHERITED_FD_BASE + target_count;
    if raw_fd < INHERITED_FD_BASE || raw_fd >= target_end {
        return Ok(read_fd);
    }

    let new_raw_fd = fcntl(raw_fd, FcntlArg::F_DUPFD_CLOEXEC(target_end))
        .context("failed to move consensus key pipe fd out of inherited fd range")?;
    drop(read_fd);
    Ok(unsafe { OwnedFd::from_raw_fd(new_raw_fd) })
}

fn set_close_on_exec(fd: RawFd) -> io::Result<()> {
    set_close_on_exec_flag(fd, true)
}

fn clear_close_on_exec(fd: RawFd) -> io::Result<()> {
    set_close_on_exec_flag(fd, false)
}

fn set_close_on_exec_flag(fd: RawFd, enabled: bool) -> io::Result<()> {
    let flags = fcntl(fd, FcntlArg::F_GETFD).map_err(io::Error::other)?;
    let mut flags = FdFlag::from_bits_truncate(flags);
    let updated_flags = if enabled {
        flags.insert(FdFlag::FD_CLOEXEC);
        flags
    } else {
        flags.remove(FdFlag::FD_CLOEXEC);
        flags
    };
    fcntl(fd, FcntlArg::F_SETFD(updated_flags)).map_err(io::Error::other)?;
    Ok(())
}

fn install_inherited_pipe_fds(read_fds: &[(RawFd, RawFd)]) -> io::Result<()> {
    for (read_fd, target_fd) in read_fds {
        if read_fd != target_fd {
            dup2(*read_fd, *target_fd).map_err(io::Error::other)?;
        }
        clear_close_on_exec(*target_fd)?;
    }
    Ok(())
}

async fn deliver_to_pipes(
    node_id: u32,
    seed: Arc<str>,
    write_fds: Vec<OwnedFd>,
    log_path: Option<PathBuf>,
) -> Result<()> {
    let secret_key_pem = assets::derive_node_secret_key_pem(seed, node_id).await?;
    for write_fd in write_fds {
        let mut writer = tokio_fs::File::from_std(StdFile::from(write_fd));
        writer
            .write_all(secret_key_pem.as_bytes())
            .await
            .map_err(|err| {
                anyhow!(
                    "failed to write consensus key for node-{} to inherited pipe: {}",
                    node_id,
                    err
                )
            })?;
        writer.flush().await.map_err(|err| {
            anyhow!(
                "failed to flush consensus key for node-{} to inherited pipe: {}",
                node_id,
                err
            )
        })?;
        drop(writer);
    }

    eprintln!(
        "delivered consensus secret key over inherited pipe for node-{} ({} bytes)",
        node_id,
        secret_key_pem.len()
    );
    append_delivery_log(log_path, node_id, secret_key_pem.len()).await;
    Ok(())
}

async fn append_delivery_log(log_path: Option<PathBuf>, node_id: u32, bytes: usize) {
    let Some(log_path) = log_path else {
        return;
    };
    let timestamp = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string());
    let line = format!(
        "{timestamp} delivered consensus secret key over inherited pipe for node-{node_id} ({bytes} bytes)\n"
    );

    let Ok(mut file) = tokio_fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await
    else {
        return;
    };
    let _ = file.write_all(line.as_bytes()).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use casper_types::SecretKey;
    use serde_json::json;
    use std::process::Stdio;
    use tempfile::TempDir;
    use tokio::process::Command;

    fn test_layout(temp_dir: &TempDir) -> AssetsLayout {
        AssetsLayout::new(temp_dir.path().to_path_buf(), "testnet".to_string())
    }

    fn source_config() -> &'static str {
        r#"[consensus]
secret_key_path = "../../keys/secret_key.pem"

[storage]
path = "../../storage"
"#
    }

    async fn write_source_config(path: &Path) {
        tokio_fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio_fs::write(path, source_config()).await.unwrap();
    }

    async fn secret_key_path_from_config(config_path: &Path) -> String {
        let contents = tokio_fs::read_to_string(config_path).await.unwrap();
        let value: toml::Value = toml::from_str(&contents).unwrap();
        value["consensus"]["secret_key_path"]
            .as_str()
            .unwrap()
            .to_string()
    }

    async fn run_reader_child(
        provider: &mut ConsensusKeyProvider,
        output_path: &Path,
    ) -> Result<()> {
        let config_paths = provider
            .config_paths()
            .into_iter()
            .map(|path| path.to_string_lossy().to_string())
            .collect::<Vec<_>>()
            .join(";");
        let mut command = Command::new(std::env::current_exe()?);
        command
            .arg("--ignored")
            .arg("--exact")
            .arg("consensus_key_provider::tests::inherited_pipe_child_reader")
            .env("CASPER_DEVNET_FD_CHILD", "1")
            .env("CASPER_DEVNET_FD_CONFIGS", config_paths)
            .env("CASPER_DEVNET_FD_OUTPUT", output_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        provider.install_on_command(&mut command);

        let mut child = command.spawn()?;
        provider.close_parent_read_ends();
        let delivery = provider.spawn_delivery(None)?;
        let status = child.wait().await?;
        let delivery_result = delivery
            .await
            .map_err(|err| anyhow!("delivery task failed: {}", err))?;
        delivery_result?;
        assert!(status.success(), "reader child exited with {status}");
        Ok(())
    }

    #[test]
    #[ignore]
    fn inherited_pipe_child_reader() {
        if std::env::var("CASPER_DEVNET_FD_CHILD").ok().as_deref() != Some("1") {
            return;
        }

        let configs = std::env::var("CASPER_DEVNET_FD_CONFIGS").unwrap();
        let output_path = PathBuf::from(std::env::var("CASPER_DEVNET_FD_OUTPUT").unwrap());
        let mut results = Vec::new();
        for config_path in configs.split(';') {
            let contents = std::fs::read_to_string(config_path).unwrap();
            let value: toml::Value = toml::from_str(&contents).unwrap();
            let fd_path = value["consensus"]["secret_key_path"].as_str().unwrap();
            let first = std::fs::read(fd_path).unwrap();
            let second = std::fs::read(fd_path).unwrap();
            results.push(json!({
                "first": String::from_utf8(first).unwrap(),
                "second_len": second.len(),
            }));
        }
        std::fs::write(output_path, serde_json::to_vec(&results).unwrap()).unwrap();
    }

    #[tokio::test]
    async fn temp_config_overrides_secret_key_path_without_changing_source() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(&temp_dir);
        let source_path = layout.node_config_root(1).join("1_0_0/config.toml");
        write_source_config(&source_path).await;
        let source_before = tokio_fs::read_to_string(&source_path).await.unwrap();

        let provider = ConsensusKeyProviderConfig::for_layout(&layout, 1, Arc::from("seed"))
            .prepare(std::slice::from_ref(&source_path))
            .await
            .unwrap();
        let config_paths = provider.config_paths();
        assert_eq!(config_paths.len(), 1);
        assert_ne!(config_paths[0], source_path);
        assert_eq!(config_paths[0].parent(), source_path.parent());
        assert_eq!(
            tokio_fs::read_to_string(&source_path).await.unwrap(),
            source_before
        );
        assert_eq!(
            secret_key_path_from_config(&config_paths[0]).await,
            inherited_fd_path(INHERITED_FD_BASE).unwrap()
        );

        provider.cleanup().await.unwrap();
        assert!(!tokio_fs::try_exists(&config_paths[0]).await.unwrap());
    }

    #[tokio::test]
    async fn child_receives_valid_key_and_second_read_is_eof() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(&temp_dir);
        let source_path = layout.node_config_root(1).join("1_0_0/config.toml");
        let output_path = temp_dir.path().join("output.json");
        write_source_config(&source_path).await;

        let mut provider = ConsensusKeyProviderConfig::for_layout(&layout, 1, Arc::from("seed"))
            .prepare(std::slice::from_ref(&source_path))
            .await
            .unwrap();
        let temp_config_paths = provider.config_paths();
        run_reader_child(&mut provider, &output_path).await.unwrap();
        provider.cleanup().await.unwrap();

        let output = tokio_fs::read_to_string(output_path).await.unwrap();
        let results: serde_json::Value = serde_json::from_str(&output).unwrap();
        let first = results[0]["first"].as_str().unwrap();
        SecretKey::from_pem(first.as_bytes()).unwrap();
        assert_eq!(results[0]["second_len"].as_u64().unwrap(), 0);
        assert!(!tokio_fs::try_exists(&temp_config_paths[0]).await.unwrap());
    }

    #[tokio::test]
    async fn two_config_pipes_deliver_keys_independently() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(&temp_dir);
        let old_config = layout.node_config_root(1).join("1_0_0/config.toml");
        let new_config = layout.node_config_root(1).join("1_0_1/config.toml");
        let output_path = temp_dir.path().join("output.json");
        write_source_config(&old_config).await;
        write_source_config(&new_config).await;

        let mut provider = ConsensusKeyProviderConfig::for_layout(&layout, 1, Arc::from("seed"))
            .prepare(&[old_config, new_config])
            .await
            .unwrap();
        run_reader_child(&mut provider, &output_path).await.unwrap();
        provider.cleanup().await.unwrap();

        let output = tokio_fs::read_to_string(output_path).await.unwrap();
        let results: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(results.as_array().unwrap().len(), 2);
        for result in results.as_array().unwrap() {
            let first = result["first"].as_str().unwrap();
            SecretKey::from_pem(first.as_bytes()).unwrap();
            assert_eq!(result["second_len"].as_u64().unwrap(), 0);
        }
    }

    #[tokio::test]
    async fn prepare_removes_stale_regular_key_file() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(&temp_dir);
        let source_path = layout.node_config_root(1).join("1_0_0/config.toml");
        let key_path = layout.node_dir(1).join("keys").join(SECRET_KEY_PEM);
        write_source_config(&source_path).await;
        tokio_fs::create_dir_all(key_path.parent().unwrap())
            .await
            .unwrap();
        tokio_fs::write(&key_path, "stale").await.unwrap();

        let provider = ConsensusKeyProviderConfig::for_layout(&layout, 1, Arc::from("seed"))
            .prepare(std::slice::from_ref(&source_path))
            .await
            .unwrap();
        assert!(!tokio_fs::try_exists(&key_path).await.unwrap());
        provider.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn prepare_rejects_directory_key_path() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(&temp_dir);
        let source_path = layout.node_config_root(1).join("1_0_0/config.toml");
        let key_path = layout.node_dir(1).join("keys").join(SECRET_KEY_PEM);
        write_source_config(&source_path).await;
        tokio_fs::create_dir_all(&key_path).await.unwrap();

        let err = ConsensusKeyProviderConfig::for_layout(&layout, 1, Arc::from("seed"))
            .prepare(std::slice::from_ref(&source_path))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("is a directory"));
    }
}
