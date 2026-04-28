use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use tokio::fs as tokio_fs;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tracing::warn;

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

impl ProcessRecord {
    pub fn current_pid(&self) -> Option<u32> {
        if let Some(handle) = &self.pid_handle {
            let pid = handle.load(Ordering::SeqCst);
            if pid != 0 {
                return Some(pid);
            }
        }
        self.pid
    }
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
        let mut snapshot = self.clone();
        for process in &mut snapshot.processes {
            process.pid = process.current_pid();
            process.pid_handle = None;
            process.shutdown_handle = None;
        }
        let contents = serde_json::to_string_pretty(&snapshot)?;
        write_atomic(&self.path, contents).await
    }
}

pub async fn spawn_pid_sync_tasks(state: Arc<Mutex<State>>) {
    let tracked = {
        let state = state.lock().await;
        state
            .processes
            .iter()
            .filter_map(|process| {
                process
                    .pid_handle
                    .as_ref()
                    .map(|handle| (process.id.clone(), Arc::clone(handle)))
            })
            .collect::<Vec<_>>()
    };

    for (process_id, pid_handle) in tracked {
        spawn_pid_sync_task(Arc::clone(&state), process_id, pid_handle);
    }
}

pub async fn spawn_pid_sync_tasks_for_ids(state: Arc<Mutex<State>>, process_ids: &[String]) {
    let tracked = {
        let state = state.lock().await;
        state
            .processes
            .iter()
            .filter(|process| process_ids.iter().any(|id| id == &process.id))
            .filter_map(|process| {
                process
                    .pid_handle
                    .as_ref()
                    .map(|handle| (process.id.clone(), Arc::clone(handle)))
            })
            .collect::<Vec<_>>()
    };

    for (process_id, pid_handle) in tracked {
        spawn_pid_sync_task(Arc::clone(&state), process_id, pid_handle);
    }
}

fn spawn_pid_sync_task(state: Arc<Mutex<State>>, process_id: String, pid_handle: Arc<AtomicU32>) {
    tokio::spawn(async move {
        let mut last_seen = Some(u32::MAX);
        loop {
            let current_pid = {
                let pid = pid_handle.load(Ordering::SeqCst);
                (pid != 0).then_some(pid)
            };
            let should_exit;

            if current_pid != last_seen {
                last_seen = current_pid;
                let mut state = state.lock().await;
                let Some(process) = state
                    .processes
                    .iter_mut()
                    .find(|process| process.id == process_id)
                else {
                    return;
                };

                process.pid = current_pid;
                should_exit =
                    !matches!(process.last_status, ProcessStatus::Running) && current_pid.is_none();

                if let Err(error) = state.touch().await {
                    warn!(
                        %error,
                        process_id,
                        "failed to persist updated process pid"
                    );
                    return;
                }
            } else {
                let state = state.lock().await;
                let Some(process) = state
                    .processes
                    .iter()
                    .find(|process| process.id == process_id)
                else {
                    return;
                };
                should_exit =
                    !matches!(process.last_status, ProcessStatus::Running) && current_pid.is_none();
            }

            if should_exit {
                return;
            }

            sleep(Duration::from_millis(100)).await;
        }
    });
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use tempfile::TempDir;

    fn test_record(pid: Option<u32>, pid_handle: Option<Arc<AtomicU32>>) -> ProcessRecord {
        ProcessRecord {
            id: "node-1".to_string(),
            node_id: 1,
            kind: ProcessKind::Node,
            group: ProcessGroup::Validators1,
            command: "/tmp/casper-node".to_string(),
            args: vec!["validator".to_string()],
            cwd: "/tmp/network".to_string(),
            pid,
            pid_handle,
            shutdown_handle: None,
            stdout_path: "/tmp/stdout.log".to_string(),
            stderr_path: "/tmp/stderr.log".to_string(),
            started_at: None,
            stopped_at: None,
            exit_code: None,
            exit_signal: None,
            last_status: ProcessStatus::Running,
        }
    }

    async fn read_pid(path: &Path) -> Option<u64> {
        let contents = tokio_fs::read_to_string(path).await.unwrap();
        let value: Value = serde_json::from_str(&contents).unwrap();
        value["processes"][0]["pid"].as_u64()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn touch_persists_current_pid_from_handle() {
        let temp_dir = TempDir::new().unwrap();
        let state_path = temp_dir.path().join("state.json");
        let pid_handle = Arc::new(AtomicU32::new(4242));

        let mut state = State::new(state_path.clone()).await.unwrap();
        state
            .processes
            .push(test_record(Some(1111), Some(Arc::clone(&pid_handle))));
        state.touch().await.unwrap();

        assert_eq!(read_pid(&state_path).await, Some(4242));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pid_sync_task_updates_state_when_pid_changes() {
        let temp_dir = TempDir::new().unwrap();
        let state_path = temp_dir.path().join("state.json");
        let pid_handle = Arc::new(AtomicU32::new(5001));

        let mut state = State::new(state_path.clone()).await.unwrap();
        state
            .processes
            .push(test_record(Some(5001), Some(Arc::clone(&pid_handle))));
        state.touch().await.unwrap();

        let state = Arc::new(Mutex::new(state));
        spawn_pid_sync_tasks(Arc::clone(&state)).await;
        pid_handle.store(6002, Ordering::SeqCst);

        for _ in 0..50 {
            if read_pid(&state_path).await == Some(6002) {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }

        panic!("timed out waiting for pid sync task to persist updated pid");
    }
}
