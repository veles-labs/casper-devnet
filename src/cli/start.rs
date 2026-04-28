use crate::assets::{
    self, AddNodesOptions, AssetSelector, AssetsLayout, SetupOptions, StageProtocolOptions,
};
use crate::cli::common::{DEFAULT_SEED, is_file, shorten_home_path};
use crate::control::{ControlRequest, ControlResponse, ControlResult};
use crate::diagnostics_port;
use crate::process::{self, ProcessHandle, RunningProcess, StartPlan};
use crate::state::{
    ProcessKind, ProcessRecord, ProcessStatus, STATE_FILE_NAME, State, spawn_pid_sync_tasks,
    spawn_pid_sync_tasks_for_ids,
};
use anyhow::{Result, anyhow};
use backoff::ExponentialBackoff;
use backoff::backoff::Backoff;
use casper_types::U512;
use casper_types::contract_messages::{Message, MessagePayload};
use casper_types::execution::ExecutionResult;
use clap::{ArgGroup, Args};
use futures::StreamExt;
use nix::errno::Errno;
use nix::sys::signal::kill;
use nix::unistd::Pid;
use serde_json::json;
use spinners::{Spinner, Spinners};
use std::collections::{HashMap, HashSet};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::fs as tokio_fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::time::MissedTickBehavior;
use veles_casper_rust_sdk::sse::event::SseEvent;
use veles_casper_rust_sdk::sse::{self, config::ListenerConfig};

const SSE_WAIT_MESSAGE: &str = "Waiting for SSE connection...";
const BLOCK_WAIT_MESSAGE: &str = "Waiting for new blocks...";
const REACTOR_STATE_POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Args, Clone)]
#[command(group(
    ArgGroup::new("asset_selector")
        .multiple(false)
        .args(["asset", "custom_asset"])
))]
pub(crate) struct StartArgs {
    /// Network name used in assets paths and configs.
    #[arg(long, default_value = "casper-dev")]
    network_name: String,

    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,

    /// Versioned asset to use from the assets store (e.g. 2.1.3).
    #[arg(long)]
    asset: Option<String>,

    /// Custom asset name from assets/custom.
    #[arg(long)]
    custom_asset: Option<String>,

    /// Protocol version to write into the generated chainspec.
    #[arg(long)]
    protocol_version: Option<String>,

    /// Number of nodes to create and start.
    #[arg(long = "node-count", aliases = ["nodes", "validators"], default_value_t = 4)]
    node_count: u32,

    /// Number of user accounts to generate (defaults to node count).
    #[arg(long)]
    users: Option<u32>,

    /// Genesis activation delay in seconds.
    #[arg(long, default_value_t = 3)]
    delay: u64,

    /// Log level for child processes (passed as `RUST_LOG`).
    #[arg(long = "log-level", default_value = "info")]
    log_level: String,

    /// Log format for node config files.
    #[arg(long, default_value = "json")]
    node_log_format: String,

    /// Create assets and exit without starting processes.
    #[arg(long)]
    setup_only: bool,

    /// Rebuild assets even if they already exist.
    #[arg(long)]
    force_setup: bool,

    /// Deterministic seed for devnet key generation.
    #[arg(long, default_value = DEFAULT_SEED)]
    seed: Arc<str>,
}

pub(crate) async fn run(args: StartArgs) -> Result<()> {
    let assets_root = match &args.net_path {
        Some(path) => path.clone(),
        None => assets::default_assets_root()?,
    };
    let layout = AssetsLayout::new(assets_root, args.network_name.clone());
    let assets_path = shorten_home_path(&layout.net_dir().display().to_string());
    println!("assets path: {}", assets_path);
    let assets_exist = layout.exists().await;
    if !args.setup_only && !args.force_setup && assets_exist {
        println!("resuming network operations on {}", layout.network_name());
    }

    if args.setup_only {
        return run_setup_only(&layout, &args).await;
    }

    let setup_result = if args.force_setup {
        assets::teardown(&layout).await?;
        Some(assets::setup_local(&layout, &setup_options(&args)?).await?)
    } else if !assets_exist {
        Some(assets::setup_local(&layout, &setup_options(&args)?).await?)
    } else {
        None
    };

    if !layout.exists().await {
        return Err(anyhow!(
            "assets missing under {}; run with --setup-only to create them",
            shorten_home_path(&layout.net_dir().display().to_string())
        ));
    }

    assets::ensure_network_hook_samples(&layout).await?;

    if !args.force_setup && assets_exist {
        let restored = assets::ensure_consensus_keys(&layout, Arc::clone(&args.seed)).await?;
        if restored > 0 {
            println!("recreated consensus keys for {} node(s)", restored);
        }
    }

    let rust_log = args.log_level.clone();
    let plan = StartPlan {
        rust_log: rust_log.clone(),
    };

    let state_path = layout.net_dir().join(STATE_FILE_NAME);
    if !is_file(&state_path).await {
        let protocol_version = match &setup_result {
            Some(result) => result.protocol_version.clone(),
            None => latest_layout_protocol_version(&layout).await?,
        };
        assets::prepare_genesis_hooks(&layout, &protocol_version).await?;
    }
    let state = Arc::new(Mutex::new(State::new(state_path).await?));
    let started = {
        let mut state = state.lock().await;
        process::start(&layout, &plan, &mut state).await?
    };
    spawn_pid_sync_tasks(Arc::clone(&state)).await;

    print_pids(&started);
    print_start_banner(&layout, &started).await;
    print_derived_accounts_summary(&layout).await;

    let node_ids = unique_node_ids(&started);
    let details = format_network_details(&layout, &started).await;
    let health = Arc::new(Mutex::new(SseHealth::new(node_ids.clone(), details)));
    start_sse_spinner(&health).await;
    spawn_sse_listeners(
        layout.clone(),
        &node_ids,
        Arc::clone(&health),
        Arc::clone(&state),
    )
    .await;
    spawn_reactor_state_pollers(&node_ids);
    let mut diagnostics_proxy = match diagnostics_port::spawn(&layout).await {
        Ok(proxy) => Some(proxy),
        Err(err) => {
            eprintln!("warning: failed to start diagnostics proxy: {}", err);
            None
        }
    };

    let (event_tx, mut event_rx) = unbounded_channel();
    spawn_ctrlc_listener(event_tx.clone());
    spawn_exit_watchers(started, event_tx.clone());
    let planned_exits = Arc::new(Mutex::new(HashSet::<(String, u32)>::new()));
    let mut control_server = match spawn_control_server(
        layout.clone(),
        Arc::clone(&state),
        Arc::clone(&health),
        event_tx.clone(),
        Arc::clone(&planned_exits),
        rust_log.clone(),
        Arc::clone(&args.seed),
    )
    .await
    {
        Ok(server) => Some(server),
        Err(err) => {
            eprintln!(
                "warning: failed to start control socket server at {}: {}",
                layout.control_socket_path().display(),
                err
            );
            None
        }
    };

    while let Some(event) = event_rx.recv().await {
        match event {
            RunEvent::CtrlC => {
                if let Some(proxy) = diagnostics_proxy.take() {
                    proxy.shutdown();
                }
                if let Some(server) = control_server.take() {
                    server.shutdown().await;
                }
                let mut state = state.lock().await;
                process::stop(&mut state).await?;
                break;
            }
            RunEvent::ProcessExit {
                id,
                pid,
                code,
                signal,
            } => {
                if is_planned_process_exit(&planned_exits, &id, pid).await {
                    continue;
                }
                if let Some(proxy) = diagnostics_proxy.take() {
                    proxy.shutdown();
                }
                if let Some(server) = control_server.take() {
                    server.shutdown().await;
                }
                let mut state = state.lock().await;
                update_exited_process(&mut state, &id, code, signal).await?;
                log_exit(&id, pid, code, signal);
                process::stop(&mut state).await?;
                break;
            }
        }
    }

    Ok(())
}

async fn run_setup_only(layout: &AssetsLayout, args: &StartArgs) -> Result<()> {
    if args.force_setup {
        assets::teardown(layout).await?;
        assets::setup_local(layout, &setup_options(args)?).await?;
        print_derived_accounts_summary(layout).await;
        return Ok(());
    }

    if layout.exists().await {
        println!(
            "assets already exist at {}; use --force-setup to rebuild",
            shorten_home_path(&layout.net_dir().display().to_string())
        );
        print_derived_accounts_summary(layout).await;
        return Ok(());
    }

    assets::setup_local(layout, &setup_options(args)?).await?;
    print_derived_accounts_summary(layout).await;
    Ok(())
}

fn setup_options(args: &StartArgs) -> Result<SetupOptions> {
    let asset =
        assets::optional_asset_selector(args.asset.as_deref(), args.custom_asset.as_deref())?;
    Ok(SetupOptions {
        nodes: args.node_count,
        users: args.users,
        delay_seconds: args.delay,
        network_name: args.network_name.clone(),
        asset,
        protocol_version: args.protocol_version.clone(),
        node_log_format: args.node_log_format.clone(),
        seed: Arc::clone(&args.seed),
    })
}

async fn latest_layout_protocol_version(layout: &AssetsLayout) -> Result<String> {
    Ok(layout
        .latest_protocol_version_dir(1)
        .await?
        .replace('_', "."))
}

fn control_stage_asset_selector(
    asset: Option<&str>,
    custom_asset: Option<&str>,
    legacy_asset_name: Option<&str>,
) -> Result<AssetSelector> {
    let custom_asset = match (custom_asset, legacy_asset_name) {
        (Some(_), Some(_)) => {
            return Err(anyhow!(
                "custom_asset and asset_name are mutually exclusive"
            ));
        }
        (Some(custom_asset), None) | (None, Some(custom_asset)) => Some(custom_asset),
        (None, None) => None,
    };
    assets::required_asset_selector(asset, custom_asset)
}

fn record_pid(record: &ProcessRecord) -> Option<u32> {
    record.current_pid()
}

fn print_pids(records: &[RunningProcess]) {
    for record in records {
        if let Some(pid) = record_pid(&record.record) {
            println!(
                "{} pid={} ({:?})",
                record.record.id, pid, record.record.kind
            );
        }
    }
}

async fn format_network_details(layout: &AssetsLayout, processes: &[RunningProcess]) -> String {
    let symlink_root = layout.net_dir();
    let mut node_pids: HashMap<u32, u32> = HashMap::new();
    let mut sidecar_pids: HashMap<u32, u32> = HashMap::new();
    let mut process_logs: HashMap<u32, Vec<(ProcessKind, u32)>> = HashMap::new();

    for process in processes {
        if let Some(pid) = record_pid(&process.record) {
            match process.record.kind {
                ProcessKind::Node => {
                    node_pids.insert(process.record.node_id, pid);
                }
                ProcessKind::Sidecar => {
                    sidecar_pids.insert(process.record.node_id, pid);
                }
            }
            process_logs
                .entry(process.record.node_id)
                .or_default()
                .push((process.record.kind.clone(), pid));
        }
    }

    let node_ids = unique_node_ids(processes);

    let mut lines = Vec::new();
    lines.push("network details".to_string());
    for node_id in node_ids {
        let node_pid = node_pids
            .get(&node_id)
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".to_string());
        let sidecar_pid = sidecar_pids
            .get(&node_id)
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".to_string());
        lines.push(format!("  node-{}", node_id));
        lines.push(format!(
            "    pids: node={} sidecar={}",
            node_pid, sidecar_pid
        ));
        if let Some(entries) = process_logs.get(&node_id) {
            let mut entries = entries.clone();
            entries.sort_by_key(|entry| process_kind_label(&entry.0).to_string());
            lines.push("    logs".to_string());
            for (kind, pid) in entries {
                let (stdout_link, stderr_link) = log_symlink_paths(&symlink_root, &kind, node_id);
                lines.push(format!(
                    "      {} pid={} stdout={} stderr={}",
                    process_kind_label(&kind),
                    pid,
                    stdout_link,
                    stderr_link
                ));
            }
        }
        lines.push("    endpoints".to_string());
        lines.push(format!("      rest:   {}", assets::rest_endpoint(node_id)));
        lines.push(format!("      sse:    {}", assets::sse_endpoint(node_id)));
        lines.push(format!("      rpc:    {}", assets::rpc_endpoint(node_id)));
        lines.push(format!("      binary: {}", assets::binary_address(node_id)));
        lines.push(format!(
            "      diagnostics: {}",
            assets::diagnostics_socket_path(layout.network_name(), node_id)
        ));
        lines.push(format!(
            "      diagnostics-ws: {}",
            assets::diagnostics_ws_endpoint(node_id)
        ));
        lines.push(format!(
            "      gossip: {}",
            assets::network_address(node_id)
        ));
    }

    lines.join("\n")
}

fn process_kind_label(kind: &ProcessKind) -> &'static str {
    match kind {
        ProcessKind::Node => "node",
        ProcessKind::Sidecar => "sidecar",
    }
}

fn log_symlink_paths(symlink_root: &Path, kind: &ProcessKind, node_id: u32) -> (String, String) {
    let base = match kind {
        ProcessKind::Node => format!("node-{}", node_id),
        ProcessKind::Sidecar => format!("sidecar-{}", node_id),
    };
    let stdout_link = symlink_root.join(format!("{}.stdout", base));
    let stderr_link = symlink_root.join(format!("{}.stderr", base));
    (
        shorten_home_path(&stdout_link.display().to_string()),
        shorten_home_path(&stderr_link.display().to_string()),
    )
}

async fn print_derived_accounts_summary(layout: &AssetsLayout) {
    if let Some(summary) = assets::derived_accounts_summary(layout).await {
        if let Some(parsed) = parse_derived_accounts_csv(&summary) {
            println!("derived accounts");
            if !parsed.validators.is_empty() {
                println!("  validators");
                print_account_group(&parsed.validators);
            }
            if !parsed.users.is_empty() {
                println!("  users");
                print_account_group(&parsed.users);
            }
            if !parsed.other.is_empty() {
                println!("  other");
                print_account_group(&parsed.other);
            }
        } else {
            println!("derived accounts");
            for line in summary.lines() {
                println!("  {}", line);
            }
        }
    }
}

struct DerivedAccountRow {
    name: String,
    key_type: String,
    derivation: String,
    path: String,
    account_hash: String,
    balance: String,
}

struct DerivedAccountsParsed {
    validators: Vec<DerivedAccountRow>,
    users: Vec<DerivedAccountRow>,
    other: Vec<DerivedAccountRow>,
}

fn parse_derived_accounts_csv(summary: &str) -> Option<DerivedAccountsParsed> {
    let mut lines = summary.lines();
    let header = lines.next()?.trim();
    if header != "kind,name,key_type,derivation,path,account_hash,balance" {
        return None;
    }

    let mut parsed = DerivedAccountsParsed {
        validators: Vec::new(),
        users: Vec::new(),
        other: Vec::new(),
    };

    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(7, ',');
        let kind = parts.next()?.to_string();
        let name = parts.next()?.to_string();
        let key_type = parts.next()?.to_string();
        let derivation = parts.next()?.to_string();
        let path = parts.next()?.to_string();
        let account_hash = parts.next()?.to_string();
        let balance = parts.next()?.to_string();
        let row = DerivedAccountRow {
            name,
            key_type,
            derivation,
            path,
            account_hash,
            balance,
        };
        match kind.as_str() {
            "validator" => parsed.validators.push(row),
            "user" => parsed.users.push(row),
            _ => parsed.other.push(row),
        }
    }

    Some(parsed)
}

fn print_account_group(rows: &[DerivedAccountRow]) {
    for row in rows {
        println!("    {}:", row.name);
        println!("      key_type: {}", row.key_type);
        println!("      derivation: {}", row.derivation);
        println!("      path: {}", row.path);
        println!("      account_hash: {}", row.account_hash);
        println!("      balance: {}", row.balance);
    }
}

fn unique_node_ids(processes: &[RunningProcess]) -> Vec<u32> {
    let mut nodes = HashSet::new();
    for process in processes {
        nodes.insert(process.record.node_id);
    }
    let mut ids: Vec<u32> = nodes.into_iter().collect();
    ids.sort_unstable();
    ids
}

enum RunEvent {
    CtrlC,
    ProcessExit {
        id: String,
        pid: Option<u32>,
        code: Option<i32>,
        signal: Option<i32>,
    },
}

struct ControlServerHandle {
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

struct ControlContext {
    layout: AssetsLayout,
    state: Arc<Mutex<State>>,
    health: Arc<Mutex<SseHealth>>,
    event_tx: UnboundedSender<RunEvent>,
    planned_exits: Arc<Mutex<HashSet<(String, u32)>>>,
    asset_mutation_lock: Mutex<()>,
    default_rust_log: String,
    seed: Arc<str>,
}

impl ControlServerHandle {
    async fn shutdown(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}

fn spawn_ctrlc_listener(tx: UnboundedSender<RunEvent>) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = tx.send(RunEvent::CtrlC);
        }
    });
}

fn spawn_exit_watchers(processes: Vec<RunningProcess>, tx: UnboundedSender<RunEvent>) {
    for running in processes {
        let tx = tx.clone();
        tokio::spawn(async move {
            let id = running.record.id.clone();
            match running.handle {
                ProcessHandle::Child(mut child) => {
                    if let Ok(status) = child.wait().await {
                        let pid = record_pid(&running.record).or_else(|| child.id());
                        let code = status.code();
                        let signal = status.signal();
                        let _ = tx.send(RunEvent::ProcessExit {
                            id: id.clone(),
                            pid,
                            code,
                            signal,
                        });
                    }
                }
                ProcessHandle::Task(handle) => {
                    let status = handle.await;
                    let pid = record_pid(&running.record);
                    let (code, signal) = match status {
                        Ok(Ok(())) => (Some(0), None),
                        Ok(Err(_)) => (None, None),
                        Err(_) => (None, None),
                    };
                    let _ = tx.send(RunEvent::ProcessExit {
                        id: id.clone(),
                        pid,
                        code,
                        signal,
                    });
                }
            }
        });
    }
}

async fn spawn_control_server(
    layout: AssetsLayout,
    state: Arc<Mutex<State>>,
    health: Arc<Mutex<SseHealth>>,
    event_tx: UnboundedSender<RunEvent>,
    planned_exits: Arc<Mutex<HashSet<(String, u32)>>>,
    rust_log: String,
    seed: Arc<str>,
) -> Result<ControlServerHandle> {
    let socket_path = layout.control_socket_path();
    if let Ok(metadata) = tokio_fs::symlink_metadata(&socket_path).await {
        if metadata.is_dir() {
            tokio_fs::remove_dir_all(&socket_path).await?;
        } else {
            tokio_fs::remove_file(&socket_path).await?;
        }
    }
    if let Some(parent) = socket_path.parent() {
        tokio_fs::create_dir_all(parent).await?;
    }

    let listener = UnixListener::bind(&socket_path)?;
    let context = Arc::new(ControlContext {
        layout,
        state,
        health,
        event_tx,
        planned_exits,
        asset_mutation_lock: Mutex::new(()),
        default_rust_log: rust_log,
        seed,
    });
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_loop = Arc::clone(&shutdown);
    let task = tokio::spawn(async move {
        loop {
            if shutdown_loop.load(Ordering::SeqCst) {
                break;
            }

            let accepted =
                tokio::time::timeout(Duration::from_millis(250), listener.accept()).await;
            let (stream, _) = match accepted {
                Ok(Ok(pair)) => pair,
                Ok(Err(err)) => {
                    eprintln!("warning: control socket accept failed: {}", err);
                    break;
                }
                Err(_) => continue,
            };
            let context = Arc::clone(&context);
            tokio::spawn(async move {
                handle_control_stream(stream, context).await;
            });
        }

        let _ = tokio_fs::remove_file(&socket_path).await;
    });

    Ok(ControlServerHandle { shutdown, task })
}

async fn handle_control_stream(mut stream: tokio::net::UnixStream, context: Arc<ControlContext>) {
    let mut request_bytes = Vec::new();
    let response = match stream.read_to_end(&mut request_bytes).await {
        Ok(_) => match serde_json::from_slice::<ControlRequest>(&request_bytes) {
            Ok(request) => handle_control_request(&context, request).await,
            Err(err) => ControlResponse::Error {
                error: format!("invalid control request: {}", err),
            },
        },
        Err(err) => ControlResponse::Error {
            error: format!("failed to read control request: {}", err),
        },
    };

    let response_bytes = serde_json::to_vec(&response).unwrap_or_else(|err| {
        format!(
            "{{\"status\":\"error\",\"error\":\"failed to serialize control response: {}\"}}",
            err
        )
        .into_bytes()
    });
    let _ = stream.write_all(&response_bytes).await;
    let _ = stream.shutdown().await;
}

async fn handle_control_request(
    context: &ControlContext,
    request: ControlRequest,
) -> ControlResponse {
    match request {
        ControlRequest::RuntimeStatus => {
            let state = context.state.lock().await;
            ControlResponse::Ok {
                result: ControlResult::RuntimeStatus {
                    running_node_ids: running_node_ids(&state),
                    last_block_height: state.last_block_height,
                },
            }
        }
        ControlRequest::StageProtocol {
            asset,
            custom_asset,
            asset_name,
            protocol_version,
            activation_point,
            restart_sidecars,
            rust_log,
        } => {
            let asset_selector = match control_stage_asset_selector(
                asset.as_deref(),
                custom_asset.as_deref(),
                asset_name.as_deref(),
            ) {
                Ok(asset_selector) => asset_selector,
                Err(err) => {
                    return ControlResponse::Error {
                        error: err.to_string(),
                    };
                }
            };
            let _asset_mutation_guard = context.asset_mutation_lock.lock().await;
            match assets::ensure_consensus_keys(&context.layout, Arc::clone(&context.seed)).await {
                Ok(restored) => {
                    if restored > 0 {
                        println!("recreated consensus keys for {} node(s)", restored);
                    }
                }
                Err(err) => {
                    return ControlResponse::Error {
                        error: format!("failed to recreate consensus keys: {}", err),
                    };
                }
            }

            let stage = assets::stage_protocol(
                &context.layout,
                &StageProtocolOptions {
                    asset: asset_selector,
                    protocol_version,
                    activation_point,
                },
            )
            .await;
            let stage = match stage {
                Ok(stage) => stage,
                Err(err) => {
                    return ControlResponse::Error {
                        error: err.to_string(),
                    };
                }
            };

            let mut restarted_sidecars = Vec::new();
            if restart_sidecars {
                let mut state = context.state.lock().await;
                let planned = sidecar_exit_keys(&state);
                if !planned.is_empty() {
                    let mut planned_exits = context.planned_exits.lock().await;
                    planned_exits.extend(planned);
                }

                let rust_log = rust_log.unwrap_or_else(|| context.default_rust_log.clone());
                match process::restart_sidecars(&context.layout, &mut state, &rust_log).await {
                    Ok(restarted) => {
                        for proc in &restarted {
                            restarted_sidecars.push(proc.record.node_id);
                        }
                        spawn_exit_watchers(restarted, context.event_tx.clone());
                    }
                    Err(err) => {
                        return ControlResponse::Error {
                            error: err.to_string(),
                        };
                    }
                }
            }

            ControlResponse::Ok {
                result: ControlResult::StageProtocol {
                    live_mode: true,
                    staged_nodes: stage.staged_nodes,
                    restarted_sidecars,
                },
            }
        }
        ControlRequest::AddNodes { count } => {
            let _asset_mutation_guard = context.asset_mutation_lock.lock().await;
            let added = match assets::add_nodes(
                &context.layout,
                &AddNodesOptions {
                    count,
                    seed: Arc::clone(&context.seed),
                },
            )
            .await
            {
                Ok(added) => added,
                Err(err) => {
                    return ControlResponse::Error {
                        error: err.to_string(),
                    };
                }
            };

            let started = {
                let mut state = context.state.lock().await;
                process::start_added_nodes(
                    &context.layout,
                    &mut state,
                    &added.added_node_ids,
                    added.total_nodes,
                    &context.default_rust_log,
                )
                .await
            };
            let started = match started {
                Ok(started) => started,
                Err(err) => {
                    let error =
                        rollback_added_nodes_after_start_error(&context.layout, &added, err).await;
                    return ControlResponse::Error { error };
                }
            };
            let process_ids = started
                .iter()
                .map(|proc| proc.record.id.clone())
                .collect::<Vec<_>>();
            let started_processes = started.len() as u32;
            let details = format_network_details(&context.layout, &started).await;
            println!(
                "{} added {} node(s) through control plane",
                timestamp_prefix(),
                added.added_node_ids.len()
            );
            println!("{}", details);
            register_expected_sse_nodes(&context.health, &added.added_node_ids).await;
            spawn_sse_listeners(
                context.layout.clone(),
                &added.added_node_ids,
                Arc::clone(&context.health),
                Arc::clone(&context.state),
            )
            .await;
            spawn_pid_sync_tasks_for_ids(Arc::clone(&context.state), &process_ids).await;
            spawn_reactor_state_pollers(&added.added_node_ids);
            spawn_exit_watchers(started, context.event_tx.clone());

            ControlResponse::Ok {
                result: ControlResult::AddNodes {
                    added_node_ids: added.added_node_ids,
                    total_nodes: added.total_nodes,
                    started_processes,
                },
            }
        }
    }
}

async fn rollback_added_nodes_after_start_error(
    layout: &AssetsLayout,
    added: &assets::AddNodesResult,
    err: anyhow::Error,
) -> String {
    match assets::rollback_added_nodes(layout, &added.added_node_ids).await {
        Ok(()) => err.to_string(),
        Err(rollback_err) => format!(
            "{}; failed to roll back added node assets: {}",
            err, rollback_err
        ),
    }
}

fn sidecar_exit_keys(state: &State) -> Vec<(String, u32)> {
    state
        .processes
        .iter()
        .filter(|record| {
            matches!(record.kind, ProcessKind::Sidecar)
                && matches!(record.last_status, ProcessStatus::Running)
        })
        .filter_map(|record| record_pid(record).map(|pid| (record.id.clone(), pid)))
        .collect()
}

async fn is_planned_process_exit(
    planned_exits: &Arc<Mutex<HashSet<(String, u32)>>>,
    id: &str,
    pid: Option<u32>,
) -> bool {
    let Some(pid) = pid else {
        return false;
    };
    let key = (id.to_string(), pid);
    let mut planned = planned_exits.lock().await;
    planned.remove(&key)
}

struct SseHealth {
    expected_nodes: HashSet<u32>,
    versions: HashMap<u32, String>,
    announced: bool,
    block_seen: bool,
    last_block_hook_hash: Option<String>,
    last_uniform_version_announced: Option<String>,
    sse_spinner: Option<Spinner>,
    block_spinner: Option<Spinner>,
    details: String,
}

impl SseHealth {
    fn new(node_ids: Vec<u32>, details: String) -> Self {
        Self {
            expected_nodes: node_ids.into_iter().collect(),
            versions: HashMap::new(),
            announced: false,
            block_seen: false,
            last_block_hook_hash: None,
            last_uniform_version_announced: None,
            sse_spinner: None,
            block_spinner: None,
            details,
        }
    }
}

async fn register_expected_sse_nodes(health: &Arc<Mutex<SseHealth>>, node_ids: &[u32]) {
    let mut state = health.lock().await;
    for node_id in node_ids {
        state.expected_nodes.insert(*node_id);
    }
}

async fn should_log_primary(node_id: u32, health: &Arc<Mutex<SseHealth>>) -> bool {
    if node_id != 1 {
        return false;
    }
    let state = health.lock().await;
    state.announced
}

async fn claim_block_hook(block_hash: &str, health: &Arc<Mutex<SseHealth>>) -> bool {
    let mut state = health.lock().await;
    if state.last_block_hook_hash.as_deref() == Some(block_hash) {
        return false;
    }
    state.last_block_hook_hash = Some(block_hash.to_string());
    true
}

fn start_spinner(message: &str) -> Spinner {
    Spinner::new(Spinners::Dots, message.to_string())
}

async fn start_sse_spinner(health: &Arc<Mutex<SseHealth>>) {
    let mut state = health.lock().await;
    if state.sse_spinner.is_none() {
        state.sse_spinner = Some(start_spinner(SSE_WAIT_MESSAGE));
    }
}

async fn spawn_sse_listeners(
    layout: AssetsLayout,
    node_ids: &[u32],
    health: Arc<Mutex<SseHealth>>,
    state: Arc<Mutex<State>>,
) {
    for node_id in node_ids {
        let node_id = *node_id;
        let endpoint = assets::sse_endpoint(node_id);
        let layout = layout.clone();
        let health = Arc::clone(&health);
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            run_sse_listener(node_id, endpoint, health, state, layout).await;
        });
    }
}

fn spawn_reactor_state_pollers(node_ids: &[u32]) {
    for node_id in node_ids {
        let node_id = *node_id;
        tokio::spawn(async move {
            run_reactor_state_poller(node_id).await;
        });
    }
}

async fn run_reactor_state_poller(node_id: u32) {
    let client = match reqwest::Client::builder()
        .no_proxy()
        .timeout(REACTOR_STATE_POLL_INTERVAL)
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            eprintln!(
                "warning: failed to create reactor state poller for node-{}: {}",
                node_id, err
            );
            return;
        }
    };
    let url = format!("{}/status", assets::rest_endpoint(node_id));
    let mut last_state = None;
    let mut interval = tokio::time::interval(REACTOR_STATE_POLL_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        if let Ok(status) = fetch_reactor_status(&client, &url).await
            && let Some(current_state) = reactor_state_from_status_payload(&status)
            && last_state.as_deref() != Some(current_state.as_str())
        {
            let prefix = timestamp_prefix();
            match last_state.replace(current_state.clone()) {
                Some(previous_state) => println!(
                    "{} node-{} reactor state: {} -> {}",
                    prefix, node_id, previous_state, current_state
                ),
                None => println!(
                    "{} node-{} reactor state: {}",
                    prefix, node_id, current_state
                ),
            }
        }
    }
}

async fn fetch_reactor_status(client: &reqwest::Client, url: &str) -> Result<serde_json::Value> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<serde_json::Value>()
        .await?)
}

fn reactor_state_from_status_payload(status: &serde_json::Value) -> Option<String> {
    status
        .get("reactor_state")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

async fn run_sse_listener(
    node_id: u32,
    endpoint: String,
    health: Arc<Mutex<SseHealth>>,
    state: Arc<Mutex<State>>,
    layout: AssetsLayout,
) {
    let mut backoff = ExponentialBackoff::default();
    let mut connection_version: Option<String> = None;

    loop {
        let config = match ListenerConfig::builder()
            .with_endpoint(endpoint.clone())
            .build()
        {
            Ok(config) => config,
            Err(_) => {
                if !sleep_backoff(&mut backoff).await {
                    return;
                }
                continue;
            }
        };

        let stream = match sse::listener(config).await {
            Ok(stream) => {
                backoff.reset();
                stream
            }
            Err(_) => {
                if !sleep_backoff(&mut backoff).await {
                    return;
                }
                continue;
            }
        };

        futures::pin_mut!(stream);
        let mut disconnect_reason: Option<String> = None;
        while let Some(event) = stream.next().await {
            match event {
                Ok(sse_event) => match sse_event {
                    SseEvent::ApiVersion(version) => {
                        let version = version.to_string();
                        connection_version = Some(version.clone());
                        record_api_version(node_id, version, &health).await;
                    }
                    SseEvent::BlockAdded { block_hash, block } => {
                        if let Err(err) = record_last_block_height(&state, block.height()).await {
                            eprintln!("warning: failed to record last block height: {}", err);
                        }
                        mark_block_seen(&health, &layout).await;
                        if let Some(protocol_version) = connection_version.as_deref()
                            && claim_block_hook(&block_hash.to_string(), &health).await
                        {
                            assets::spawn_block_added_hook(
                                layout.clone(),
                                protocol_version.to_string(),
                                json!({
                                    "block_hash": block_hash.to_string(),
                                    "height": block.height(),
                                    "era_id": block.era_id().value(),
                                }),
                            );
                        }
                        assets::spawn_pending_post_genesis_hook(layout.clone());
                        if should_log_primary(node_id, &health).await {
                            let prefix = timestamp_prefix();
                            println!(
                                "{} Block {} added (height={} era={})",
                                prefix,
                                block_hash,
                                block.height(),
                                block.era_id().value()
                            );
                        }
                    }
                    SseEvent::TransactionAccepted(transaction) => {
                        if node_id == 1 {
                            let prefix = timestamp_prefix();
                            println!("{} Transaction {} accepted", prefix, transaction.hash());
                        }
                    }
                    SseEvent::TransactionProcessed {
                        transaction_hash,
                        execution_result,
                        messages,
                        ..
                    } => {
                        if node_id == 1 {
                            let tx_hash = transaction_hash.to_string();
                            let prefix = timestamp_prefix();
                            log_transaction_processed(
                                &prefix,
                                &tx_hash,
                                &execution_result,
                                &messages,
                            );
                        }
                    }
                    SseEvent::Shutdown => {
                        let prefix = timestamp_prefix();
                        let version = connection_version.as_deref().unwrap_or("unknown");
                        println!(
                            "{} node-{} reported shutdown over SSE (api_version={})",
                            prefix, node_id, version
                        );
                        connection_version = None;
                    }
                    _ => {}
                },
                Err(err) => {
                    disconnect_reason = Some(format!("stream error: {}", err));
                    break;
                }
            }
        }

        if disconnect_reason.is_none() {
            disconnect_reason = Some("stream closed".to_string());
        }
        if let Some(version) = connection_version.as_deref()
            && let Some(reason) = disconnect_reason.as_deref()
        {
            let prefix = timestamp_prefix();
            println!(
                "{} node-{} SSE connection lost (api_version={}, reason={})",
                prefix, node_id, version, reason
            );
        }
        connection_version = None;

        if !sleep_backoff(&mut backoff).await {
            return;
        }
    }
}

async fn record_api_version(node_id: u32, version: String, health: &Arc<Mutex<SseHealth>>) {
    let (summary, details, sse_spinner, should_log) = {
        let mut state = health.lock().await;
        if !state.expected_nodes.contains(&node_id) {
            return;
        }
        state.versions.insert(node_id, version);
        if state.versions.len() != state.expected_nodes.len() {
            return;
        }

        let summary = version_summary(&state.versions);
        let uniform = uniform_network_version(&state.versions);
        let mut details = None;
        let mut sse_spinner = None;
        let should_log = if !state.announced {
            details = Some(state.details.clone());
            sse_spinner = state.sse_spinner.take();
            if !state.block_seen && state.block_spinner.is_none() {
                state.block_spinner = Some(start_spinner(BLOCK_WAIT_MESSAGE));
            }
            state.announced = true;
            if let Some(uniform) = uniform {
                state.last_uniform_version_announced = Some(uniform);
            }
            true
        } else if let Some(uniform) = uniform {
            if state.last_uniform_version_announced.as_deref() != Some(uniform.as_str()) {
                state.last_uniform_version_announced = Some(uniform);
                true
            } else {
                false
            }
        } else {
            false
        };

        (summary, details, sse_spinner, should_log)
    };

    if should_log {
        if let Some(mut spinner) = sse_spinner {
            spinner.stop_with_message("SSE connection established.".to_string());
        }
        println!("Network is healthy ({})", summary);
        if let Some(details) = details {
            println!("{}", details);
        }
    }
}

async fn mark_block_seen(health: &Arc<Mutex<SseHealth>>, layout: &AssetsLayout) {
    let (block_spinner, node_ids) = {
        let mut state = health.lock().await;
        if state.block_seen {
            return;
        }
        state.block_seen = true;
        (
            state.block_spinner.take(),
            state.expected_nodes.iter().copied().collect::<Vec<_>>(),
        )
    };

    if let Some(mut spinner) = block_spinner {
        spinner.stop_with_message(BLOCK_WAIT_MESSAGE.to_string());
    }

    match assets::remove_consensus_keys(layout, &node_ids).await {
        Ok(removed) => {
            if removed > 0 {
                println!("Consensus secret keys removed from disk.");
            }
        }
        Err(err) => {
            eprintln!("warning: failed to remove consensus secret keys: {}", err);
        }
    }
}

async fn record_last_block_height(state: &Arc<Mutex<State>>, height: u64) -> Result<()> {
    let mut state = state.lock().await;
    if state.last_block_height == Some(height) {
        return Ok(());
    }
    state.last_block_height = Some(height);
    state.touch().await?;
    Ok(())
}

fn version_summary(versions: &HashMap<u32, String>) -> String {
    let mut unique: Vec<String> = versions.values().cloned().collect();
    unique.sort();
    unique.dedup();
    if unique.len() == 1 {
        format!("version {}", unique[0])
    } else {
        format!("versions {}", unique.join(", "))
    }
}

fn uniform_network_version(versions: &HashMap<u32, String>) -> Option<String> {
    let mut unique: Vec<String> = versions.values().cloned().collect();
    unique.sort();
    unique.dedup();
    if unique.len() == 1 {
        unique.into_iter().next()
    } else {
        None
    }
}

async fn sleep_backoff(backoff: &mut ExponentialBackoff) -> bool {
    if let Some(delay) = backoff.next_backoff() {
        tokio::time::sleep(delay).await;
        return true;
    }
    false
}

fn log_transaction_processed(
    prefix: &str,
    transaction_hash: &str,
    execution_result: &ExecutionResult,
    messages: &[Message],
) {
    let consumed = execution_result.consumed();
    let consumed_cspr = format_cspr_u512(&consumed);
    if let Some(error) = execution_result.error_message() {
        println!(
            "{} Transaction {} processed failed ({}) gas={} gas_cspr={}",
            prefix, transaction_hash, error, consumed, consumed_cspr
        );
    } else {
        println!(
            "{} Transaction {} processed succeeded gas={} gas_cspr={}",
            prefix, transaction_hash, consumed, consumed_cspr
        );
    }

    for message in messages {
        let entity = message.entity_addr().to_formatted_string();
        let topic = message.topic_name();
        let payload = format_message_payload(message.payload());
        println!("{} 📨 {} {}: {}", prefix, entity, topic, payload);
    }
}

fn timestamp_prefix() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown-time".to_string())
}

fn format_message_payload(payload: &MessagePayload) -> String {
    match payload {
        MessagePayload::Bytes(bytes) => format!("0x{}", encode_hex(bytes.as_ref())),
        MessagePayload::String(value) => format!("{:?}", value),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut out, "{:02x}", byte);
    }
    out
}

fn format_cspr_u512(motes: &U512) -> String {
    let motes_str = motes.to_string();
    let digits = motes_str.len();
    if digits <= 9 {
        let frac = format!("{:0>9}", motes_str);
        let frac = frac.trim_end_matches('0');
        if frac.is_empty() {
            return "0".to_string();
        }
        return format!("0.{}", frac);
    }

    let split = digits - 9;
    let (whole, frac) = motes_str.split_at(split);
    let frac = frac.trim_end_matches('0');
    if frac.is_empty() {
        return whole.to_string();
    }
    format!("{}.{}", whole, frac)
}

async fn update_exited_process(
    state: &mut State,
    id: &str,
    code: Option<i32>,
    signal: Option<i32>,
) -> Result<()> {
    for record in &mut state.processes {
        if record.id == id {
            record.last_status = ProcessStatus::Exited;
            record.exit_code = code;
            record.exit_signal = signal;
            record.stopped_at = Some(time::OffsetDateTime::now_utc());
            break;
        }
    }
    state.touch().await?;
    Ok(())
}

fn log_exit(id: &str, pid: Option<u32>, code: Option<i32>, signal: Option<i32>) {
    if let Some(pid) = pid {
        if let Some(signal) = signal {
            println!(
                "process {} (pid {}) exited due to signal {}",
                id, pid, signal
            );
        } else if let Some(code) = code {
            println!("process {} (pid {}) exited with code {}", id, pid, code);
        } else {
            println!("process {} (pid {}) exited", id, pid);
        }
    } else if let Some(signal) = signal {
        println!("process {} exited due to signal {}", id, signal);
    } else if let Some(code) = code {
        println!("process {} exited with code {}", id, code);
    } else {
        println!("process {} exited", id);
    }
}

async fn print_start_banner(layout: &AssetsLayout, processes: &[RunningProcess]) {
    let total_nodes = layout.count_nodes().await.unwrap_or(0);
    let target = format!("all nodes ({})", total_nodes);
    let sidecars = processes
        .iter()
        .filter(|proc| matches!(proc.record.kind, crate::state::ProcessKind::Sidecar))
        .count();
    println!(
        "started {} process(es) for {} (sidecars: {})",
        processes.len(),
        target,
        sidecars
    );
}

fn running_node_ids(state: &State) -> Vec<u32> {
    let mut node_ids = HashSet::new();
    for process in &state.processes {
        if !matches!(process.kind, ProcessKind::Node) {
            continue;
        }
        if !matches!(process.last_status, ProcessStatus::Running) {
            continue;
        }
        let Some(pid) = record_pid(process) else {
            continue;
        };
        if !is_pid_running(pid) {
            continue;
        }
        node_ids.insert(process.node_id);
    }
    let mut node_ids = node_ids.into_iter().collect::<Vec<_>>();
    node_ids.sort_unstable();
    node_ids
}

fn is_pid_running(pid: u32) -> bool {
    let pid = Pid::from_raw(pid as i32);
    match kill(pid, None) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false,
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_SEED, SseHealth, format_cspr_u512, format_message_payload,
        reactor_state_from_status_payload, spawn_control_server,
    };
    use crate::assets::{self, CustomAssetInstallOptions};
    use crate::control::{ControlRequest, ControlResponse, ControlResult, send_request};
    use crate::state::{ProcessGroup, ProcessKind, ProcessRecord, ProcessStatus, State};
    use casper_types::U512;
    use casper_types::contract_messages::MessagePayload;
    use std::collections::HashSet;
    use std::env;
    use std::ffi::OsString;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::{Arc, MutexGuard};
    use std::time::Duration;
    use tokio::fs as tokio_fs;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex as TokioMutex;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::task::JoinHandle;

    const TEST_HOOK_FILE_MODE: u32 = 0o755;

    struct TestDataEnv {
        _lock: MutexGuard<'static, ()>,
        temp_dir: tempfile::TempDir,
        old_home: Option<OsString>,
        old_xdg_data_home: Option<OsString>,
    }

    impl TestDataEnv {
        fn new() -> Self {
            let lock = crate::assets::test_env_lock()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let temp_dir = tempfile::tempdir().expect("temp dir");
            let old_home = env::var_os("HOME");
            let old_xdg_data_home = env::var_os("XDG_DATA_HOME");
            unsafe {
                env::set_var("HOME", temp_dir.path());
                env::set_var("XDG_DATA_HOME", temp_dir.path().join("xdg-data"));
            }
            Self {
                _lock: lock,
                temp_dir,
                old_home,
                old_xdg_data_home,
            }
        }

        fn root(&self) -> &std::path::Path {
            self.temp_dir.path()
        }
    }

    impl Drop for TestDataEnv {
        fn drop(&mut self) {
            if let Some(value) = &self.old_home {
                unsafe {
                    env::set_var("HOME", value);
                }
            } else {
                unsafe {
                    env::remove_var("HOME");
                }
            }
            if let Some(value) = &self.old_xdg_data_home {
                unsafe {
                    env::set_var("XDG_DATA_HOME", value);
                }
            } else {
                unsafe {
                    env::remove_var("XDG_DATA_HOME");
                }
            }
        }
    }

    async fn write_executable_script(path: &std::path::Path, contents: &str) {
        if let Some(parent) = path.parent() {
            tokio_fs::create_dir_all(parent).await.unwrap();
        }
        tokio_fs::write(path, contents).await.unwrap();
        tokio_fs::set_permissions(path, std::fs::Permissions::from_mode(TEST_HOOK_FILE_MODE))
            .await
            .unwrap();
    }

    async fn create_fake_binary(path: &std::path::Path, label: &str) {
        write_executable_script(
            path,
            &format!(
                "#!/bin/sh\nset -eu\nif [ \"${{1:-}}\" = \"--version\" ]; then\n  echo \"{label} 1.0.0\"\n  exit 0\nfi\nexit 0\n"
            ),
        )
        .await;
    }

    async fn create_test_custom_asset_sources(
        root: &std::path::Path,
        name: &str,
    ) -> CustomAssetInstallOptions {
        let source_dir = root.join("sources").join(name);
        tokio_fs::create_dir_all(&source_dir).await.unwrap();
        let node_path = source_dir.join("casper-node");
        let sidecar_path = source_dir.join("casper-sidecar");
        create_fake_binary(&node_path, "casper-node").await;
        create_fake_binary(&sidecar_path, "casper-sidecar").await;
        let chainspec = source_dir.join("chainspec.toml");
        let node_config = source_dir.join("node-config.toml");
        let sidecar_config = source_dir.join("sidecar-config.toml");
        tokio_fs::write(
            &chainspec,
            "\
[protocol]
activation_point = 1
version = '1.0.0'

[network]
name = 'casper-dev'

[core]
validator_slots = 4
rewards_handling = { type = 'standard' }
",
        )
        .await
        .unwrap();
        tokio_fs::write(&node_config, "").await.unwrap();
        tokio_fs::write(&sidecar_config, "").await.unwrap();
        CustomAssetInstallOptions {
            name: name.to_string(),
            casper_node: node_path,
            casper_sidecar: sidecar_path,
            chainspec,
            node_config,
            sidecar_config,
        }
    }

    async fn install_test_custom_asset(env: &TestDataEnv, name: &str) -> PathBuf {
        let options = create_test_custom_asset_sources(env.root(), name).await;
        assets::install_custom_asset(&options).await.unwrap();
        assets::custom_assets_root().unwrap().join(name)
    }

    async fn spawn_test_status_server() -> Option<JoinHandle<()>> {
        let listener = TcpListener::bind("127.0.0.1:14101").await.ok()?;
        Some(tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut request = [0_u8; 1024];
                    let _ = stream.read(&mut request).await;
                    let body = r#"{"last_added_block_info":{"hash":"test-trusted-hash"}}"#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        }))
    }

    async fn create_test_network_layout(
        root: &std::path::Path,
        network_name: &str,
        current_version: &str,
    ) -> crate::assets::AssetsLayout {
        let layout =
            crate::assets::AssetsLayout::new(root.join("networks"), network_name.to_string());
        let version_fs = current_version.replace('.', "_");
        tokio_fs::create_dir_all(layout.node_bin_dir(1).join(&version_fs))
            .await
            .unwrap();
        tokio_fs::create_dir_all(layout.node_config_root(1).join(&version_fs))
            .await
            .unwrap();
        tokio_fs::create_dir_all(layout.node_dir(1).join("logs"))
            .await
            .unwrap();
        tokio_fs::create_dir_all(layout.node_dir(1).join("storage"))
            .await
            .unwrap();
        tokio_fs::create_dir_all(layout.node_dir(1).join("keys"))
            .await
            .unwrap();
        tokio_fs::create_dir_all(layout.net_dir().join("chainspec"))
            .await
            .unwrap();
        create_fake_binary(
            &layout.node_bin_dir(1).join(&version_fs).join("casper-node"),
            "casper-node",
        )
        .await;
        create_fake_binary(
            &layout
                .node_bin_dir(1)
                .join(&version_fs)
                .join("casper-sidecar"),
            "casper-sidecar",
        )
        .await;
        tokio_fs::write(
            layout
                .node_config_root(1)
                .join(&version_fs)
                .join("config.toml"),
            "\
[logging]
format = \"text\"

[consensus]
secret_key_path = \"old.pem\"

[network]
bind_address = \"0.0.0.0:1\"
known_addresses = []

[storage]
path = \"old-storage\"

[rest_server]
address = \"0.0.0.0:2\"

[event_stream_server]
address = \"0.0.0.0:3\"

[diagnostics_port]
socket_path = \"old.sock\"

[binary_port_server]
address = \"0.0.0.0:4\"
allow_request_get_trie = false
allow_request_speculative_exec = false
",
        )
        .await
        .unwrap();
        tokio_fs::write(
            layout
                .node_config_root(1)
                .join(&version_fs)
                .join("chainspec.toml"),
            "",
        )
        .await
        .unwrap();
        tokio_fs::write(
            layout
                .node_config_root(1)
                .join(&version_fs)
                .join("accounts.toml"),
            "",
        )
        .await
        .unwrap();
        tokio_fs::write(
            layout
                .node_config_root(1)
                .join(&version_fs)
                .join("sidecar.toml"),
            "\
[rpc_server.main_server]
ip_address = \"127.0.0.1\"
port = 1

[rpc_server.node_client]
ip_address = \"127.0.0.1\"
port = 2
",
        )
        .await
        .unwrap();
        tokio_fs::write(
            layout
                .node_config_root(1)
                .join("casper-node-launcher-state.toml"),
            format!(
                "mode = \"RunNodeAsValidator\"\nversion = \"{}\"\nbinary_path = \"{}\"\nconfig_path = \"{}\"\n",
                current_version,
                layout
                    .node_bin_dir(1)
                    .join(&version_fs)
                    .join("casper-node")
                    .display(),
                layout
                    .node_config_root(1)
                    .join(&version_fs)
                    .join("config.toml")
                    .display()
            ),
        )
        .await
        .unwrap();
        tokio_fs::write(
            layout.net_dir().join("derived-accounts.csv"),
            "kind,name,key_type,derivation,path,account_hash,balance\n",
        )
        .await
        .unwrap();
        tokio_fs::write(layout.net_dir().join("chainspec/accounts.toml"), "")
            .await
            .unwrap();
        layout
    }

    async fn test_state(layout: &crate::assets::AssetsLayout) -> State {
        let mut state = State::new(layout.net_dir().join(crate::state::STATE_FILE_NAME))
            .await
            .unwrap();
        state.processes.push(ProcessRecord {
            id: "node-1".to_string(),
            node_id: 1,
            kind: ProcessKind::Node,
            group: ProcessGroup::Validators1,
            command: "/tmp/casper-node".to_string(),
            args: vec!["validator".to_string()],
            cwd: layout.node_dir(1).display().to_string(),
            pid: Some(std::process::id()),
            pid_handle: None,
            shutdown_handle: None,
            stdout_path: layout
                .node_logs_dir(1)
                .join("stdout.log")
                .display()
                .to_string(),
            stderr_path: layout
                .node_logs_dir(1)
                .join("stderr.log")
                .display()
                .to_string(),
            started_at: None,
            stopped_at: None,
            exit_code: None,
            exit_signal: None,
            last_status: ProcessStatus::Running,
        });
        state.touch().await.unwrap();
        state
    }

    #[test]
    fn format_cspr_u512_handles_whole_and_fractional() {
        assert_eq!(format_cspr_u512(&U512::zero()), "0");
        assert_eq!(format_cspr_u512(&U512::from(1u64)), "0.000000001");
        assert_eq!(format_cspr_u512(&U512::from(1_000_000_000u64)), "1");
        assert_eq!(
            format_cspr_u512(&U512::from(1_000_000_001u64)),
            "1.000000001"
        );
        assert_eq!(
            format_cspr_u512(&U512::from_dec_str("123000000000").unwrap()),
            "123"
        );
        assert_eq!(
            format_cspr_u512(&U512::from_dec_str("123000000456").unwrap()),
            "123.000000456"
        );
    }

    #[test]
    fn format_message_payload_renders_string_with_quotes() {
        let payload = MessagePayload::String("hello".to_string());
        assert_eq!(format_message_payload(&payload), "\"hello\"");
    }

    #[test]
    fn reactor_state_from_status_payload_reads_status_field() {
        assert_eq!(
            reactor_state_from_status_payload(&serde_json::json!({"reactor_state": "CatchUp"})),
            Some("CatchUp".to_string())
        );
        assert_eq!(
            reactor_state_from_status_payload(&serde_json::json!({"reactor_state": null})),
            None
        );
        assert_eq!(
            reactor_state_from_status_payload(&serde_json::json!({"status": "CatchUp"})),
            None
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn control_server_handles_runtime_status_while_stage_protocol_is_in_flight() {
        let env = TestDataEnv::new();
        install_test_custom_asset(&env, "dev").await;
        let layout = create_test_network_layout(env.root(), "casper-dev", "1.0.0").await;
        write_executable_script(
            &layout.hooks_dir().join("pre-stage-protocol"),
            "#!/bin/sh\nset -eu\nprintf 'started\n' > \"$PWD/pre-hook-started\"\nsleep 1\n",
        )
        .await;

        let state = Arc::new(TokioMutex::new(test_state(&layout).await));
        let health = Arc::new(TokioMutex::new(SseHealth::new(
            vec![1],
            "test details".to_string(),
        )));
        let (event_tx, _event_rx) = unbounded_channel();
        let planned_exits = Arc::new(TokioMutex::new(HashSet::new()));
        let control_server = spawn_control_server(
            layout.clone(),
            Arc::clone(&state),
            health,
            event_tx,
            planned_exits,
            "info".to_string(),
            Arc::<str>::from(DEFAULT_SEED),
        )
        .await
        .unwrap();

        let stage_socket_path = layout.control_socket_path();
        let stage_request = ControlRequest::StageProtocol {
            asset: None,
            custom_asset: Some("dev".to_string()),
            asset_name: None,
            protocol_version: "2.0.0".to_string(),
            activation_point: 123,
            restart_sidecars: false,
            rust_log: None,
        };
        let stage_task =
            tokio::spawn(async move { send_request(&stage_socket_path, &stage_request).await });

        let marker_path = layout
            .hook_work_dir("pre-stage-protocol")
            .join("pre-hook-started");
        for _ in 0..50 {
            if tokio_fs::metadata(&marker_path)
                .await
                .map(|meta| meta.is_file())
                .unwrap_or(false)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            tokio_fs::metadata(&marker_path)
                .await
                .map(|meta| meta.is_file())
                .unwrap_or(false),
            "timed out waiting for pre-stage hook to start"
        );

        let response = tokio::time::timeout(
            Duration::from_millis(250),
            send_request(
                &layout.control_socket_path(),
                &ControlRequest::RuntimeStatus,
            ),
        )
        .await
        .expect("runtime_status request timed out")
        .unwrap();
        match response {
            ControlResponse::Ok {
                result:
                    ControlResult::RuntimeStatus {
                        running_node_ids, ..
                    },
            } => assert_eq!(running_node_ids, vec![1]),
            other => panic!("unexpected runtime_status response: {other:?}"),
        }

        let response = stage_task.await.unwrap().unwrap();
        match response {
            ControlResponse::Ok {
                result:
                    ControlResult::StageProtocol {
                        live_mode,
                        staged_nodes,
                        restarted_sidecars,
                    },
            } => {
                assert!(live_mode);
                assert_eq!(staged_nodes, 1);
                assert!(restarted_sidecars.is_empty());
            }
            other => panic!("unexpected stage_protocol response: {other:?}"),
        }

        control_server.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn control_server_add_nodes_starts_and_tracks_new_processes() {
        let env = TestDataEnv::new();
        let layout = create_test_network_layout(env.root(), "casper-dev", "1.0.0").await;
        let status_server = spawn_test_status_server().await;
        let state = Arc::new(TokioMutex::new(test_state(&layout).await));
        let health = Arc::new(TokioMutex::new(SseHealth::new(
            vec![1],
            "test details".to_string(),
        )));
        let (event_tx, _event_rx) = unbounded_channel();
        let planned_exits = Arc::new(TokioMutex::new(HashSet::new()));
        let control_server = spawn_control_server(
            layout.clone(),
            Arc::clone(&state),
            Arc::clone(&health),
            event_tx,
            planned_exits,
            "info".to_string(),
            Arc::<str>::from(DEFAULT_SEED),
        )
        .await
        .unwrap();

        let response = send_request(
            &layout.control_socket_path(),
            &ControlRequest::AddNodes { count: 1 },
        )
        .await
        .unwrap();

        match response {
            ControlResponse::Ok {
                result:
                    ControlResult::AddNodes {
                        added_node_ids,
                        total_nodes,
                        started_processes,
                    },
            } => {
                assert_eq!(added_node_ids, vec![2]);
                assert_eq!(total_nodes, 2);
                assert_eq!(started_processes, 2);
            }
            other => panic!("unexpected add_nodes response: {other:?}"),
        }

        let state = state.lock().await;
        assert!(
            state
                .processes
                .iter()
                .any(|record| record.id == "node-2" && matches!(record.kind, ProcessKind::Node))
        );
        assert!(
            state
                .processes
                .iter()
                .any(|record| record.id == "sidecar-2"
                    && matches!(record.kind, ProcessKind::Sidecar))
        );
        drop(state);

        let health = health.lock().await;
        assert!(health.expected_nodes.contains(&2));
        drop(health);

        control_server.shutdown().await;
        if let Some(status_server) = status_server {
            status_server.abort();
        }
    }
}
