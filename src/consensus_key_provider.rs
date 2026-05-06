use crate::assets::{self, AssetsLayout};
use anyhow::{Result, anyhow};
use nix::sys::stat::Mode;
use nix::unistd::mkfifo as nix_mkfifo;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::fs as tokio_fs;
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;

const SECRET_KEY_PEM: &str = "secret_key.pem";
const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug)]
pub(crate) struct ConsensusKeyProviderConfig {
    path: PathBuf,
    node_id: u32,
    seed: Arc<str>,
}

impl ConsensusKeyProviderConfig {
    pub(crate) fn for_layout(layout: &AssetsLayout, node_id: u32, seed: Arc<str>) -> Self {
        let path = layout.node_dir(node_id).join("keys").join(SECRET_KEY_PEM);
        Self {
            path,
            node_id,
            seed,
        }
    }

    pub(crate) async fn prepare(&self) -> Result<ConsensusKeyProvider> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| anyhow!("consensus key path {} has no parent", self.path.display()))?;
        tokio_fs::create_dir_all(parent).await?;

        if let Ok(metadata) = tokio_fs::symlink_metadata(&self.path).await {
            if metadata.file_type().is_dir() {
                return Err(anyhow!(
                    "consensus key path {} is a directory",
                    self.path.display()
                ));
            }
            tokio_fs::remove_file(&self.path).await?;
        }

        create_fifo(&self.path).await?;
        tokio_fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(|err| {
                anyhow!(
                    "failed to set consensus key FIFO permissions on {}: {}",
                    self.path.display(),
                    err
                )
            })?;

        Ok(ConsensusKeyProvider {
            path: self.path.clone(),
            node_id: self.node_id,
            seed: Arc::clone(&self.seed),
        })
    }
}

async fn create_fifo(path: &Path) -> Result<()> {
    let path = path.to_path_buf();
    let display_path = path.display().to_string();
    tokio::task::spawn_blocking(move || nix_mkfifo(&path, Mode::from_bits_truncate(0o600)))
        .await
        .map_err(|err| anyhow!("failed to create FIFO {display_path}: {err}"))?
        .map_err(|err| anyhow!("failed to create FIFO {display_path}: {err}"))
}

#[derive(Debug)]
pub(crate) struct ConsensusKeyProvider {
    path: PathBuf,
    node_id: u32,
    seed: Arc<str>,
}

impl ConsensusKeyProvider {
    pub(crate) async fn run_once(self, shutdown: Arc<AtomicBool>) -> Result<()> {
        let result = self.serve_once(shutdown).await;
        let cleanup = self.cleanup().await;
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), Ok(())) => Err(err),
            (Ok(()), Err(err)) => Err(err),
            (Err(err), Err(cleanup_err)) => Err(anyhow!("{err}; additionally {cleanup_err}")),
        }
    }

    async fn serve_once(&self, shutdown: Arc<AtomicBool>) -> Result<()> {
        while !shutdown.load(Ordering::SeqCst) {
            match self.open_writer() {
                Ok(mut writer) => {
                    let secret_key_pem =
                        assets::derive_node_secret_key_pem(Arc::clone(&self.seed), self.node_id)
                            .await?;
                    writer
                        .write_all(secret_key_pem.as_bytes())
                        .await
                        .map_err(|err| {
                            anyhow!(
                                "failed to write consensus key for node-{} to {}: {}",
                                self.node_id,
                                self.path.display(),
                                err
                            )
                        })?;
                    writer.flush().await.map_err(|err| {
                        anyhow!(
                            "failed to flush consensus key for node-{} to {}: {}",
                            self.node_id,
                            self.path.display(),
                            err
                        )
                    })?;
                    eprintln!(
                        "delivered consensus secret key over FIFO for node-{} ({} bytes)",
                        self.node_id,
                        secret_key_pem.len()
                    );
                    drop(writer);
                    return Ok(());
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    sleep(POLL_INTERVAL).await;
                }
                Err(err) => {
                    return Err(anyhow!(
                        "failed to open consensus key FIFO {} for node-{}: {}",
                        self.path.display(),
                        self.node_id,
                        err
                    ));
                }
            }
        }

        Ok(())
    }

    fn open_writer(&self) -> std::io::Result<tokio_fs::File> {
        match OpenOptions::new()
            .write(true)
            .custom_flags(nix::libc::O_NONBLOCK)
            .open(&self.path)
        {
            Ok(file) => Ok(tokio_fs::File::from_std(file)),
            Err(err) if err.raw_os_error() == Some(nix::libc::ENXIO) => {
                Err(std::io::Error::new(ErrorKind::WouldBlock, err))
            }
            Err(err) => Err(err),
        }
    }

    async fn cleanup(&self) -> Result<()> {
        match tokio_fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(anyhow!(
                "failed to remove consensus key FIFO {}: {}",
                self.path.display(),
                err
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casper_types::SecretKey;
    use std::os::unix::fs::FileTypeExt;
    use tempfile::TempDir;

    fn test_layout(temp_dir: &TempDir) -> AssetsLayout {
        AssetsLayout::new(temp_dir.path().to_path_buf(), "testnet".to_string())
    }

    async fn assert_fifo(path: &PathBuf) {
        let metadata = tokio_fs::symlink_metadata(path).await.unwrap();
        assert!(metadata.file_type().is_fifo());
        assert!(!metadata.file_type().is_file());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }

    async fn read_fifo(path: PathBuf) -> String {
        let bytes = tokio::task::spawn_blocking(move || std::fs::read(path))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8(bytes).unwrap()
    }

    #[tokio::test]
    async fn provider_creates_fifo_and_serves_valid_key_once() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(&temp_dir);
        let seed: Arc<str> = Arc::from("seed");
        let provider = ConsensusKeyProviderConfig::for_layout(&layout, 1, Arc::clone(&seed))
            .prepare()
            .await
            .unwrap();
        let path = provider.path.clone();
        assert_fifo(&path).await;

        let shutdown = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(provider.run_once(Arc::clone(&shutdown)));
        let pem = read_fifo(path.clone()).await;
        SecretKey::from_pem(pem.as_bytes()).unwrap();

        task.await.unwrap().unwrap();
        assert!(!tokio_fs::try_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn provider_rejects_second_read_by_removing_fifo() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(&temp_dir);
        let provider = ConsensusKeyProviderConfig::for_layout(&layout, 2, Arc::from("seed"))
            .prepare()
            .await
            .unwrap();
        let path = provider.path.clone();

        let shutdown = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(provider.run_once(Arc::clone(&shutdown)));
        let pem = read_fifo(path.clone()).await;
        SecretKey::from_pem(pem.as_bytes()).unwrap();

        task.await.unwrap().unwrap();
        let err = std::fs::read(path).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn shutdown_before_read_removes_fifo() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(&temp_dir);
        let provider = ConsensusKeyProviderConfig::for_layout(&layout, 1, Arc::from("seed"))
            .prepare()
            .await
            .unwrap();
        let path = provider.path.clone();
        assert_fifo(&path).await;

        let shutdown = Arc::new(AtomicBool::new(true));
        provider.run_once(shutdown).await.unwrap();
        assert!(!tokio_fs::try_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn prepare_replaces_stale_regular_file() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(&temp_dir);
        let path = layout.node_dir(1).join("keys").join(SECRET_KEY_PEM);
        tokio_fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio_fs::write(&path, "stale").await.unwrap();

        let provider = ConsensusKeyProviderConfig::for_layout(&layout, 1, Arc::from("seed"))
            .prepare()
            .await
            .unwrap();
        assert_fifo(&path).await;

        let shutdown = Arc::new(AtomicBool::new(true));
        provider.run_once(shutdown).await.unwrap();
        assert!(!tokio_fs::try_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn prepare_rejects_directory_key_path() {
        let temp_dir = TempDir::new().unwrap();
        let layout = test_layout(&temp_dir);
        let path = layout.node_dir(1).join("keys").join(SECRET_KEY_PEM);
        tokio_fs::create_dir_all(&path).await.unwrap();

        let err = ConsensusKeyProviderConfig::for_layout(&layout, 1, Arc::from("seed"))
            .prepare()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("is a directory"));
    }
}
