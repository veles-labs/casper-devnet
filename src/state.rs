use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32},
};
use std::time::{SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use tokio::fs as tokio_fs;

pub const STATE_FILE_NAME: &str = "state.json";

/// Process classification used in logs and reporting.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProcessKind {
    Node,
    Sidecar,
}

/// Logical process grouping (kept for parity with NCTL UX).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProcessGroup {
    Validators1,
    Validators2,
    Validators3,
}

/// Runtime status of a tracked process.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProcessStatus {
    Running,
    Stopped,
    Exited,
    Unknown,
    Skipped,
}

/// Persisted record of a process and its lifecycle details.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessRecord {
    pub id: String,
    pub node_id: u32,
    pub kind: ProcessKind,
    pub group: ProcessGroup,
    pub command: String,
    pub args: Vec<String>,
    pub cwd: String,
    pub pid: Option<u32>,
    #[serde(skip)]
    pub pid_handle: Option<Arc<AtomicU32>>,
    #[serde(skip)]
    pub shutdown_handle: Option<Arc<AtomicBool>>,
    pub stdout_path: String,
    pub stderr_path: String,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub stopped_at: Option<OffsetDateTime>,
    pub exit_code: Option<i32>,
    pub exit_signal: Option<i32>,
    pub last_status: ProcessStatus,
}

/// State snapshot stored for process bookkeeping.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct State {
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    pub last_block_height: Option<u64>,
    pub processes: Vec<ProcessRecord>,
    #[serde(skip)]
    path: PathBuf,
}

impl State {
    pub async fn new(path: PathBuf) -> Result<Self> {
        let now = OffsetDateTime::now_utc();
        let state = Self {
            created_at: now,
            updated_at: now,
            last_block_height: None,
            processes: Vec::new(),
            path,
        };
        state.persist().await?;
        Ok(state)
    }

    pub async fn touch(&mut self) -> Result<()> {
        self.updated_at = OffsetDateTime::now_utc();
        self.persist().await
    }

    async fn persist(&self) -> Result<()> {
        ensure_parent(&self.path).await?;
        let contents = serde_json::to_string_pretty(self)?;
        write_atomic(&self.path, contents).await
    }
}

async fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio_fs::create_dir_all(parent).await?;
    }
    Ok(())
}

async fn write_atomic(path: &Path, contents: String) -> Result<()> {
    let base_name = path
        .file_name()
        .unwrap_or_else(|| OsStr::new(STATE_FILE_NAME))
        .to_string_lossy();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let tmp_dir = tempfile::Builder::new()
        .prefix("casper-devnet-state")
        .tempdir()?;
    let tmp_name = format!("{}.{}.{}.tmp", base_name, std::process::id(), suffix);
    let tmp_path = tmp_dir.path().join(tmp_name);
    tokio_fs::write(&tmp_path, contents).await?;
    if let Err(err) = tokio_fs::rename(&tmp_path, path).await {
        let _ = tokio_fs::remove_file(&tmp_path).await;
        return Err(err.into());
    }
    Ok(())
}
