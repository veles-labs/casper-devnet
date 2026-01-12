use crate::assets::{AssetsLayout, BOOTSTRAP_NODES};
use crate::node_launcher::Launcher;
use crate::state::{ProcessGroup, ProcessKind, ProcessRecord, ProcessStatus, State};
use anyhow::{anyhow, Result};
use nix::errno::Errno;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::fs as tokio_fs;
use tokio::fs::OpenOptions;
use tokio::process::Command;
use tokio::time::sleep;

/// A started process plus its handle for lifecycle tracking.
pub struct RunningProcess {
    pub record: ProcessRecord,
    pub handle: ProcessHandle,
}

/// Handle types for started processes.
pub enum ProcessHandle {
    Child(tokio::process::Child),
    Task(tokio::task::JoinHandle<Result<()>>),
}

/// Start parameters derived from CLI arguments.
pub struct StartPlan {
    pub rust_log: String,
}

/// Start all nodes (and sidecars if available), updating state on success.
pub async fn start(
    layout: &AssetsLayout,
    plan: &StartPlan,
    state: &mut State,
) -> Result<Vec<RunningProcess>> {
    let total_nodes = layout.count_nodes().await?;
    if total_nodes == 0 {
        return Err(anyhow!(
            "no nodes found under {}",
            layout.nodes_dir().display()
        ));
    }
    let node_ids: Vec<u32> = (1..=total_nodes).collect();

    let mut started = Vec::new();
    for node_id in node_ids {
        if node_id > total_nodes {
            return Err(anyhow!(
                "node {} exceeds total nodes {}",
                node_id,
                total_nodes
            ));
        }
        let mut records = start_node(layout, node_id, total_nodes, &plan.rust_log).await?;
        started.append(&mut records);
    }

    state.processes = started.iter().map(|proc| proc.record.clone()).collect();
    state.touch().await?;

    Ok(started)
}

/// Stop all running processes tracked in state.
pub async fn stop(state: &mut State) -> Result<()> {
    let mut running: Vec<&mut ProcessRecord> = state
        .processes
        .iter_mut()
        .filter(|record| matches!(record.last_status, ProcessStatus::Running))
        .collect();

    let mut errors = Vec::new();

    for record in &mut running {
        if let Some(shutdown) = &record.shutdown_handle {
            shutdown.store(true, Ordering::SeqCst);
        }

        if let Some(pid) = current_pid(record) {
            println!(
                "sending {} to {} (pid {})",
                signal_name(Signal::SIGTERM),
                record.id,
                pid
            );
            if let Err(err) = send_signal(pid as i32, Signal::SIGTERM) {
                errors.push(format!(
                    "failed to send {} to {} (pid {}): {}",
                    signal_name(Signal::SIGTERM),
                    record.id,
                    pid,
                    err
                ));
            }
        }
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    for record in &mut running {
        if let Some(pid) = current_pid(record) {
            while process_alive(pid as i32) && Instant::now() < deadline {
                sleep(Duration::from_millis(200)).await;
            }
            if process_alive(pid as i32) {
                println!(
                    "sending {} to {} (pid {})",
                    signal_name(Signal::SIGKILL),
                    record.id,
                    pid
                );
                if let Err(err) = send_signal(pid as i32, Signal::SIGKILL) {
                    errors.push(format!(
                        "failed to send {} to {} (pid {}): {}",
                        signal_name(Signal::SIGKILL),
                        record.id,
                        pid,
                        err
                    ));
                }
                if process_alive(pid as i32) {
                    errors.push(format!(
                        "{} (pid {}) still running after SIGKILL",
                        record.id, pid
                    ));
                    record.last_status = ProcessStatus::Unknown;
                    continue;
                }
            }
            record.last_status = ProcessStatus::Stopped;
            record.stopped_at = Some(OffsetDateTime::now_utc());
            record.exit_code = None;
            record.exit_signal = None;
            continue;
        }
        errors.push(format!("missing pid for {} while stopping", record.id));
        record.last_status = ProcessStatus::Unknown;
    }

    state.touch().await?;
    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(errors.join("\n")))
    }
}

/// Start a single node and optional sidecar.
async fn start_node(
    layout: &AssetsLayout,
    node_id: u32,
    total_nodes: u32,
    rust_log: &str,
) -> Result<Vec<RunningProcess>> {
    let mut records = Vec::new();

    let node_record = spawn_node(layout, node_id, total_nodes, rust_log).await?;
    records.push(node_record);

    if let Some(sidecar_record) = spawn_sidecar(layout, node_id, total_nodes, rust_log).await? {
        records.push(sidecar_record);
    }

    Ok(records)
}

/// Spawn the embedded launcher to run a node process in this runtime.
async fn spawn_node(
    layout: &AssetsLayout,
    node_id: u32,
    total_nodes: u32,
    rust_log: &str,
) -> Result<RunningProcess> {
    let node_dir = layout.node_dir(node_id);

    let stdout_path = layout.node_logs_dir(node_id).join("stdout.log");
    let stderr_path = layout.node_logs_dir(node_id).join("stderr.log");
    create_log_symlinks(
        layout,
        node_id,
        ProcessKind::Node,
        &stdout_path,
        &stderr_path,
    )
    .await?;

    let mut launcher = Launcher::new_with_roots(
        None,
        layout.node_bin_dir(node_id),
        layout.node_config_root(node_id),
    )
    .await?;
    launcher.set_log_paths(stdout_path.clone(), stderr_path.clone());
    launcher.set_cwd(node_dir.clone());
    launcher.set_rust_log(rust_log.to_string());

    let mut env = BTreeMap::new();
    env.insert(
        "CASPER_CONFIG_DIR".to_string(),
        layout
            .node_config_root(node_id)
            .to_string_lossy()
            .to_string(),
    );
    launcher.set_envs(env);

    let (command_path, command_args) = launcher.current_command();
    let child_pid = launcher.child_pid();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_thread = Arc::clone(&shutdown);
    let handle =
        tokio::spawn(async move { launcher.run_with_shutdown(shutdown_thread.as_ref()).await });
    let pid = wait_for_pid(child_pid.as_ref()).await;

    Ok(RunningProcess {
        record: ProcessRecord {
            id: format!("node-{}", node_id),
            node_id,
            kind: ProcessKind::Node,
            group: process_group(node_id, total_nodes),
            command: command_path.to_string_lossy().to_string(),
            args: command_args,
            cwd: node_dir.to_string_lossy().to_string(),
            pid,
            pid_handle: Some(child_pid),
            shutdown_handle: Some(shutdown),
            stdout_path: stdout_path.to_string_lossy().to_string(),
            stderr_path: stderr_path.to_string_lossy().to_string(),
            started_at: Some(OffsetDateTime::now_utc()),
            stopped_at: None,
            exit_code: None,
            exit_signal: None,
            last_status: ProcessStatus::Running,
        },
        handle: ProcessHandle::Task(handle),
    })
}

/// Spawn the sidecar process if binary and config are available.
async fn spawn_sidecar(
    layout: &AssetsLayout,
    node_id: u32,
    total_nodes: u32,
    rust_log: &str,
) -> Result<Option<RunningProcess>> {
    let version_dir = layout.latest_protocol_version_dir(node_id).await?;
    let command_path = layout
        .node_bin_dir(node_id)
        .join(&version_dir)
        .join("casper-sidecar");
    let config_path = layout
        .node_config_root(node_id)
        .join(&version_dir)
        .join("sidecar.toml");

    if !is_file(&command_path).await || !is_file(&config_path).await {
        return Ok(None);
    }

    let node_dir = layout.node_dir(node_id);
    let stdout_path = layout.node_logs_dir(node_id).join("sidecar-stdout.log");
    let stderr_path = layout.node_logs_dir(node_id).join("sidecar-stderr.log");
    create_log_symlinks(
        layout,
        node_id,
        ProcessKind::Sidecar,
        &stdout_path,
        &stderr_path,
    )
    .await?;

    let args = vec![
        "--path-to-config".to_string(),
        config_path.to_string_lossy().to_string(),
    ];

    let mut env = BTreeMap::new();
    env.insert("RUST_LOG".to_string(), rust_log.to_string());

    let child = spawn_process(
        &command_path,
        &args,
        &env,
        &node_dir,
        &stdout_path,
        &stderr_path,
    )
    .await?;
    let pid = child.id();

    Ok(Some(RunningProcess {
        record: ProcessRecord {
            id: format!("sidecar-{}", node_id),
            node_id,
            kind: ProcessKind::Sidecar,
            group: process_group(node_id, total_nodes),
            command: command_path.to_string_lossy().to_string(),
            args,
            cwd: node_dir.to_string_lossy().to_string(),
            pid,
            pid_handle: None,
            shutdown_handle: None,
            stdout_path: stdout_path.to_string_lossy().to_string(),
            stderr_path: stderr_path.to_string_lossy().to_string(),
            started_at: Some(OffsetDateTime::now_utc()),
            stopped_at: None,
            exit_code: None,
            exit_signal: None,
            last_status: ProcessStatus::Running,
        },
        handle: ProcessHandle::Child(child),
    }))
}

/// Spawn a tokio-managed child process with redirected logs.
async fn spawn_process(
    command: &Path,
    args: &[String],
    env: &BTreeMap<String, String>,
    cwd: &Path,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Result<tokio::process::Child> {
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(stdout_path)
        .await?;
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(stderr_path)
        .await?;
    let stdout = stdout.into_std().await;
    let stderr = stderr.into_std().await;

    let mut cmd = Command::new(command);
    cmd.args(args)
        .envs(env)
        .current_dir(cwd)
        .stdout(stdout)
        .stderr(stderr);

    Ok(cmd.spawn()?)
}

async fn create_log_symlinks(
    layout: &AssetsLayout,
    node_id: u32,
    kind: ProcessKind,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Result<()> {
    let data_dir = layout.net_dir();
    tokio_fs::create_dir_all(&data_dir).await?;
    let prefix = match kind {
        ProcessKind::Node => format!("node-{}", node_id),
        ProcessKind::Sidecar => format!("sidecar-{}", node_id),
    };
    let stdout_link = data_dir.join(format!("{}.stdout", prefix));
    let stderr_link = data_dir.join(format!("{}.stderr", prefix));
    create_symlink(&stdout_link, stdout_path).await?;
    create_symlink(&stderr_link, stderr_path).await?;
    Ok(())
}

async fn create_symlink(link_path: &Path, target_path: &Path) -> Result<()> {
    if let Ok(metadata) = tokio_fs::symlink_metadata(link_path).await {
        if metadata.is_dir() {
            tokio_fs::remove_dir_all(link_path).await?;
        } else {
            tokio_fs::remove_file(link_path).await?;
        }
    }
    tokio_fs::symlink(target_path, link_path).await?;
    Ok(())
}

fn process_group(node_id: u32, total_nodes: u32) -> ProcessGroup {
    let genesis_nodes = total_nodes / 2;
    if node_id <= BOOTSTRAP_NODES.min(genesis_nodes) {
        ProcessGroup::Validators1
    } else if node_id <= genesis_nodes {
        ProcessGroup::Validators2
    } else {
        ProcessGroup::Validators3
    }
}

fn process_alive(pid: i32) -> bool {
    match kill(Pid::from_raw(pid), None) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false,
        Err(_) => true,
    }
}

fn send_signal(target: i32, signal: Signal) -> Result<()> {
    match kill(Pid::from_raw(target), signal) {
        Ok(()) => Ok(()),
        Err(Errno::ESRCH) => Ok(()),
        Err(err) => Err(anyhow!(err)),
    }
}

fn signal_name(signal: Signal) -> &'static str {
    match signal {
        Signal::SIGTERM => "SIGTERM",
        Signal::SIGKILL => "SIGKILL",
        _ => "signal",
    }
}

async fn is_file(path: &Path) -> bool {
    tokio_fs::metadata(path)
        .await
        .map(|meta| meta.is_file())
        .unwrap_or(false)
}

async fn wait_for_pid(pid_handle: &AtomicU32) -> Option<u32> {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let pid = pid_handle.load(Ordering::SeqCst);
        if pid != 0 {
            return Some(pid);
        }
        if Instant::now() >= deadline {
            return None;
        }
        sleep(Duration::from_millis(20)).await;
    }
}

fn current_pid(record: &ProcessRecord) -> Option<u32> {
    if let Some(handle) = &record.pid_handle {
        let pid = handle.load(Ordering::SeqCst);
        if pid != 0 {
            return Some(pid);
        }
    }
    record.pid
}
