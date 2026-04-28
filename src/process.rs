use crate::assets::{AssetsLayout, BOOTSTRAP_NODES};
use crate::node_launcher::Launcher;
use crate::state::{ProcessGroup, ProcessKind, ProcessRecord, ProcessStatus, State};
use anyhow::{Result, anyhow};
use nix::errno::Errno;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
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
            let err = anyhow!("node {} exceeds total nodes {}", node_id, total_nodes);
            return Err(cleanup_started_processes(err, started).await);
        }
        match start_node(layout, node_id, total_nodes, &plan.rust_log).await {
            Ok(mut records) => started.append(&mut records),
            Err(err) => return Err(cleanup_started_processes(err, started).await),
        }
    }

    state.processes = started.iter().map(|proc| proc.record.clone()).collect();
    if let Err(err) = state.touch().await {
        state.processes.clear();
        return Err(cleanup_started_processes(err, started).await);
    }

    Ok(started)
}

async fn cleanup_started_processes(
    err: anyhow::Error,
    started: Vec<RunningProcess>,
) -> anyhow::Error {
    let cleanup_errors = stop_started_processes(started).await;
    if cleanup_errors.is_empty() {
        err
    } else {
        anyhow!(
            "{}; failed to clean up partially started processes: {}",
            err,
            cleanup_errors.join("; ")
        )
    }
}

async fn stop_started_processes(processes: Vec<RunningProcess>) -> Vec<String> {
    let mut errors = Vec::new();

    for running in &processes {
        if let Some(shutdown) = &running.record.shutdown_handle {
            shutdown.store(true, Ordering::SeqCst);
        }

        let Some(pid) = current_pid(&running.record) else {
            continue;
        };
        if let Err(err) = send_signal(pid as i32, Signal::SIGTERM) {
            errors.push(format!(
                "failed to send {} to {} (pid {}): {}",
                signal_name(Signal::SIGTERM),
                running.record.id,
                pid,
                err
            ));
        }
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    for running in &processes {
        let Some(pid) = current_pid(&running.record) else {
            continue;
        };
        while process_alive(pid as i32) && Instant::now() < deadline {
            sleep(Duration::from_millis(100)).await;
        }
        if process_alive(pid as i32)
            && let Err(err) = send_signal(pid as i32, Signal::SIGKILL)
        {
            errors.push(format!(
                "failed to send {} to {} (pid {}): {}",
                signal_name(Signal::SIGKILL),
                running.record.id,
                pid,
                err
            ));
        }
    }

    for running in processes {
        match running.handle {
            ProcessHandle::Child(mut child) => {
                if tokio::time::timeout(Duration::from_secs(1), child.wait())
                    .await
                    .is_err()
                {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                }
            }
            ProcessHandle::Task(mut handle) => {
                tokio::select! {
                    _ = &mut handle => {}
                    _ = sleep(Duration::from_secs(1)) => {
                        handle.abort();
                        let _ = handle.await;
                    }
                }
            }
        }
    }

    errors
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
            eprintln!(
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
                eprintln!(
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

/// Restart sidecars for all nodes, updating persisted process state.
pub async fn restart_sidecars(
    layout: &AssetsLayout,
    state: &mut State,
    rust_log: &str,
) -> Result<Vec<RunningProcess>> {
    let total_nodes = layout.count_nodes().await?;
    if total_nodes == 0 {
        return Err(anyhow!(
            "no nodes found under {}",
            layout.nodes_dir().display()
        ));
    }

    let mut restarted = Vec::new();
    let mut errors = Vec::new();

    for node_id in 1..=total_nodes {
        if let Some(record) = state
            .processes
            .iter_mut()
            .find(|record| record.node_id == node_id && matches!(record.kind, ProcessKind::Sidecar))
        {
            if let Some(pid) = current_pid(record) {
                if let Err(err) = send_signal(pid as i32, Signal::SIGTERM) {
                    errors.push(format!(
                        "failed to send {} to {} (pid {}): {}",
                        signal_name(Signal::SIGTERM),
                        record.id,
                        pid,
                        err
                    ));
                } else if !wait_for_process_exit(pid as i32, Duration::from_secs(5)).await
                    && let Err(err) = send_signal(pid as i32, Signal::SIGKILL)
                {
                    errors.push(format!(
                        "failed to send {} to {} (pid {}): {}",
                        signal_name(Signal::SIGKILL),
                        record.id,
                        pid,
                        err
                    ));
                }
            }
            record.last_status = ProcessStatus::Stopped;
            record.stopped_at = Some(OffsetDateTime::now_utc());
            record.exit_code = None;
            record.exit_signal = None;
            record.pid = None;
            record.pid_handle = None;
            record.shutdown_handle = None;
        }

        match spawn_sidecar(layout, node_id, total_nodes, rust_log).await {
            Ok(Some(running)) => {
                if let Some(slot) = state.processes.iter_mut().find(|record| {
                    record.node_id == node_id && matches!(record.kind, ProcessKind::Sidecar)
                }) {
                    *slot = running.record.clone();
                } else {
                    state.processes.push(running.record.clone());
                }
                restarted.push(running);
            }
            Ok(None) => {}
            Err(err) => {
                errors.push(format!(
                    "failed to restart sidecar for node-{}: {}",
                    node_id, err
                ));
            }
        }
    }

    state.touch().await?;
    if errors.is_empty() {
        Ok(restarted)
    } else {
        Err(anyhow!(errors.join("\n")))
    }
}

/// Start newly added nodes and track their node/sidecar processes in state.
pub async fn start_added_nodes(
    layout: &AssetsLayout,
    state: &mut State,
    node_ids: &[u32],
    total_nodes: u32,
    rust_log: &str,
) -> Result<Vec<RunningProcess>> {
    let mut started = Vec::new();

    for node_id in node_ids {
        if state.processes.iter().any(|record| {
            record.node_id == *node_id && matches!(record.last_status, ProcessStatus::Running)
        }) {
            let err = anyhow!("node-{} already has running process records", node_id);
            return Err(cleanup_started_processes(err, started).await);
        }

        match start_node(layout, *node_id, total_nodes, rust_log).await {
            Ok(mut records) => started.append(&mut records),
            Err(err) => return Err(cleanup_started_processes(err, started).await),
        }
    }

    let process_ids = started
        .iter()
        .map(|proc| proc.record.id.clone())
        .collect::<Vec<_>>();
    for running in &started {
        if let Some(slot) = state.processes.iter_mut().find(|record| {
            record.node_id == running.record.node_id
                && std::mem::discriminant(&record.kind)
                    == std::mem::discriminant(&running.record.kind)
        }) {
            *slot = running.record.clone();
        } else {
            state.processes.push(running.record.clone());
        }
    }

    if let Err(err) = state.touch().await {
        state
            .processes
            .retain(|record| !process_ids.iter().any(|id| id == &record.id));
        return Err(cleanup_started_processes(err, started).await);
    }

    Ok(started)
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

    match spawn_sidecar(layout, node_id, total_nodes, rust_log).await {
        Ok(Some(sidecar_record)) => records.push(sidecar_record),
        Ok(None) => {}
        Err(err) => return Err(cleanup_started_processes(err, records).await),
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
    launcher.set_hook_context(layout.net_dir(), layout.hooks_dir());
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
    let logs_dir = layout.node_logs_dir(node_id);
    let stdout_alias_path = logs_dir.join("sidecar-stdout.log");
    let stderr_alias_path = logs_dir.join("sidecar-stderr.log");
    let (stdout_target_path, stderr_target_path) =
        prepare_versioned_log_aliases(&stdout_alias_path, &stderr_alias_path, &version_dir).await?;
    create_log_symlinks(
        layout,
        node_id,
        ProcessKind::Sidecar,
        &stdout_alias_path,
        &stderr_alias_path,
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
        &stdout_target_path,
        &stderr_target_path,
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
            stdout_path: stdout_alias_path.to_string_lossy().to_string(),
            stderr_path: stderr_alias_path.to_string_lossy().to_string(),
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
    let parent = link_path
        .parent()
        .ok_or_else(|| anyhow!("link path {} has no parent", link_path.display()))?;
    tokio_fs::create_dir_all(parent).await?;

    if let Ok(metadata) = tokio_fs::symlink_metadata(link_path).await
        && metadata.is_dir()
    {
        tokio_fs::remove_dir_all(link_path).await?;
    }

    let link_name = link_path
        .file_name()
        .ok_or_else(|| anyhow!("link path {} has no file name", link_path.display()))?
        .to_string_lossy()
        .to_string();
    let tmp_link = parent.join(format!(".{link_name}.tmp-{}", std::process::id()));
    let _ = tokio_fs::remove_file(&tmp_link).await;
    tokio_fs::symlink(target_path, &tmp_link).await?;
    tokio_fs::rename(&tmp_link, link_path).await?;
    Ok(())
}

fn versioned_log_target(alias_path: &Path, version_fs: &str) -> Result<PathBuf> {
    let parent = alias_path
        .parent()
        .ok_or_else(|| anyhow!("log alias {} has no parent", alias_path.display()))?;
    let file_name = alias_path
        .file_name()
        .ok_or_else(|| anyhow!("log alias {} has no file name", alias_path.display()))?
        .to_string_lossy()
        .to_string();

    if let Some((base, ext)) = file_name.rsplit_once('.') {
        Ok(parent.join(format!("{base}-{version_fs}.{ext}")))
    } else {
        Ok(parent.join(format!("{file_name}-{version_fs}")))
    }
}

async fn prepare_versioned_log_alias(alias_path: &Path, version_fs: &str) -> Result<PathBuf> {
    let target_path = versioned_log_target(alias_path, version_fs)?;
    let parent = alias_path
        .parent()
        .ok_or_else(|| anyhow!("log alias {} has no parent", alias_path.display()))?;
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

    create_symlink(alias_path, &target_path).await?;
    Ok(target_path)
}

async fn prepare_versioned_log_aliases(
    stdout_alias: &Path,
    stderr_alias: &Path,
    version_fs: &str,
) -> Result<(PathBuf, PathBuf)> {
    let stdout_target = prepare_versioned_log_alias(stdout_alias, version_fs).await?;
    let stderr_target = prepare_versioned_log_alias(stderr_alias, version_fs).await?;
    Ok((stdout_target, stderr_target))
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

async fn wait_for_process_exit(pid: i32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !process_alive(pid) {
            return true;
        }
        sleep(Duration::from_millis(100)).await;
    }
    !process_alive(pid)
}

fn current_pid(record: &ProcessRecord) -> Option<u32> {
    record.current_pid()
}
