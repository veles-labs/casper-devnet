use crate::assets::{
    self, AssetsLayout, CustomAssetInstallOptions, SetupOptions, StageProtocolOptions,
};
use crate::control::{ControlRequest, ControlResponse, ControlResult, send_request};
use crate::diagnostics_port;
use crate::mcp::{self, McpArgs};
use crate::process::{self, ProcessHandle, RunningProcess, StartPlan};
use crate::state::{ProcessKind, ProcessRecord, ProcessStatus, STATE_FILE_NAME, State};
use anyhow::{Result, anyhow};
use backoff::ExponentialBackoff;
use backoff::backoff::Backoff;
use casper_types::U512;
use casper_types::contract_messages::MessagePayload;
use casper_types::execution::ExecutionResult;
use clap::{Args, Parser, Subcommand};
use dialoguer::Confirm;
use directories::BaseDirs;
use futures::StreamExt;
use nix::errno::Errno;
use nix::sys::signal::kill;
use nix::unistd::Pid;
use serde::Deserialize;
use spinners::{Spinner, Spinners};
use std::collections::{HashMap, HashSet};
use std::os::unix::fs::FileTypeExt;
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
use veles_casper_rust_sdk::sse::event::SseEvent;
use veles_casper_rust_sdk::sse::{self, config::ListenerConfig};

/// CLI entrypoint for the devnet launcher.
#[derive(Parser)]
#[command(name = "nctl")]
#[command(
    about = "casper-devnet launcher for local Casper Network development networks",
    long_about = None
)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Top-level CLI subcommands.
#[derive(Subcommand)]
enum Command {
    /// Setup assets (if needed) and start the devnet.
    Start(StartArgs),
    /// Run MCP control plane server.
    Mcp(McpArgs),
    /// Manage assets bundles.
    Assets(AssetsArgs),
    /// Manage local network directories.
    Networks(NetworksArgs),
    /// Stage a protocol upgrade from a custom asset.
    StageProtocol(StageProtocolArgs),
    /// Check whether a network has observed a block yet.
    IsReady(IsReadyArgs),
}

/// Arguments for `nctl start`.
#[derive(Args, Clone)]
struct StartArgs {
    /// Network name used in assets paths and configs.
    #[arg(long, default_value = "casper-dev")]
    network_name: String,

    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,

    /// Protocol version to use from the assets store (e.g. 2.1.1).
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
    #[arg(long, default_value = "default")]
    seed: Arc<str>,
}

/// Asset management arguments.
#[derive(Args)]
struct AssetsArgs {
    #[command(subcommand)]
    command: AssetsCommand,
}

/// Asset management subcommands.
#[derive(Subcommand)]
enum AssetsCommand {
    /// Extract a local assets bundle into the assets store.
    Add(AssetsAddArgs),
    /// Download assets bundles from the upstream release.
    Pull(AssetsPullArgs),
    /// List available protocol versions and custom assets in the assets store.
    List,
    /// Print absolute path to a named custom asset directory.
    Path(AssetsPathArgs),
}

/// Arguments for `nctl assets add`.
#[derive(Args, Clone)]
struct AssetsAddArgs {
    /// Asset bundle path (tar.gz) or custom asset name when override flags are used.
    #[arg(value_name = "PATH_OR_NAME")]
    path_or_name: String,

    /// Local casper-node binary path for custom asset install mode.
    #[arg(long, value_name = "PATH")]
    casper_node: Option<PathBuf>,

    /// Local casper-sidecar binary path for custom asset install mode.
    #[arg(long, value_name = "PATH")]
    casper_sidecar: Option<PathBuf>,

    /// Local chainspec.toml path for custom asset install mode.
    #[arg(long, value_name = "PATH")]
    chainspec: Option<PathBuf>,

    /// Local node-config.toml path for custom asset install mode.
    #[arg(long, value_name = "PATH")]
    node_config: Option<PathBuf>,

    /// Local sidecar-config.toml path for custom asset install mode.
    #[arg(long, value_name = "PATH")]
    sidecar_config: Option<PathBuf>,
}

/// Arguments for `nctl assets pull`.
#[derive(Args, Clone)]
struct AssetsPullArgs {
    /// Target triple to select from release assets.
    #[arg(long)]
    target: Option<String>,

    /// Re-download and replace any existing assets.
    #[arg(long)]
    force: bool,
}

/// Arguments for `nctl assets path`.
#[derive(Args, Clone)]
struct AssetsPathArgs {
    /// Custom asset name.
    #[arg(value_name = "ASSET_NAME")]
    asset_name: String,
}

/// Network directory management arguments.
#[derive(Args)]
struct NetworksArgs {
    #[command(subcommand)]
    command: NetworksCommand,
}

/// Network directory management subcommands.
#[derive(Subcommand)]
enum NetworksCommand {
    /// List managed network names.
    List(NetworksListArgs),
    /// Remove a managed network from disk.
    Rm(NetworksRmArgs),
}

/// Arguments for `nctl networks list`.
#[derive(Args, Clone)]
struct NetworksListArgs {
    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,
}

/// Arguments for `nctl networks rm`.
#[derive(Args, Clone)]
struct NetworksRmArgs {
    /// Managed network name.
    #[arg(value_name = "NETWORK_NAME")]
    network_name: String,

    /// Remove without interactive confirmation.
    #[arg(short = 'y', long)]
    yes: bool,

    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,
}

/// Arguments for `nctl is-ready`.
#[derive(Args, Clone)]
struct IsReadyArgs {
    /// Network name used in assets paths and configs.
    #[arg(long, default_value = "casper-dev")]
    network_name: String,

    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,
}

/// Arguments for `nctl stage-protocol`.
#[derive(Args, Clone)]
struct StageProtocolArgs {
    /// Asset name in custom assets store.
    #[arg(value_name = "ASSET_NAME")]
    asset_name: String,

    /// Protocol version to stage (e.g. 2.2.0).
    #[arg(long)]
    protocol_version: String,

    /// Future era id for activation.
    #[arg(long)]
    activation_point: u64,

    /// Network name used in assets paths and configs.
    #[arg(long, default_value = "casper-dev")]
    network_name: String,

    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,
}

/// Parses CLI and runs the selected subcommand.
pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Start(args) => run_start(args).await,
        Command::Mcp(args) => mcp::run(args).await,
        Command::Assets(args) => run_assets(args).await,
        Command::Networks(args) => run_networks(args).await,
        Command::StageProtocol(args) => run_stage_protocol(args).await,
        Command::IsReady(args) => run_is_ready(args).await,
    }
}

async fn run_start(args: StartArgs) -> Result<()> {
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
    let protocol_version = resolve_protocol_version(&args.protocol_version).await?;

    if args.setup_only {
        return run_setup_only(&layout, &args, &protocol_version).await;
    }

    if args.force_setup {
        assets::teardown(&layout).await?;
        assets::setup_local(&layout, &setup_options(&args, &protocol_version)).await?;
    } else if !assets_exist {
        assets::setup_local(&layout, &setup_options(&args, &protocol_version)).await?;
    }

    if !layout.exists().await {
        return Err(anyhow!(
            "assets missing under {}; run with --setup-only to create them",
            shorten_home_path(&layout.net_dir().display().to_string())
        ));
    }

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
    let state = Arc::new(Mutex::new(State::new(state_path).await?));
    let started = {
        let mut state = state.lock().await;
        process::start(&layout, &plan, &mut state).await?
    };

    print_pids(&started);
    print_start_banner(&layout, &started).await;
    print_derived_accounts_summary(&layout).await;

    let node_ids = unique_node_ids(&started);
    let details = format_network_details(&layout, &started).await;
    let health = Arc::new(Mutex::new(SseHealth::new(node_ids.clone(), details)));
    start_sse_spinner(&health).await;
    spawn_sse_listeners(layout.clone(), &node_ids, health, Arc::clone(&state)).await;
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

async fn run_setup_only(
    layout: &AssetsLayout,
    args: &StartArgs,
    protocol_version: &str,
) -> Result<()> {
    if args.force_setup {
        assets::teardown(layout).await?;
        assets::setup_local(layout, &setup_options(args, protocol_version)).await?;
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

    assets::setup_local(layout, &setup_options(args, protocol_version)).await?;
    print_derived_accounts_summary(layout).await;
    Ok(())
}

fn record_pid(record: &ProcessRecord) -> Option<u32> {
    if let Some(handle) = &record.pid_handle {
        let pid = handle.load(Ordering::SeqCst);
        if pid != 0 {
            return Some(pid);
        }
    }
    record.pid
}

fn setup_options(args: &StartArgs, protocol_version: &str) -> SetupOptions {
    SetupOptions {
        nodes: args.node_count,
        users: args.users,
        delay_seconds: args.delay,
        network_name: args.network_name.clone(),
        protocol_version: protocol_version.to_string(),
        node_log_format: args.node_log_format.clone(),
        seed: Arc::clone(&args.seed),
    }
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

fn shorten_home_path(path: &str) -> String {
    let path = Path::new(path);
    let Some(base_dirs) = BaseDirs::new() else {
        return path.display().to_string();
    };
    let home = base_dirs.home_dir();
    match path.strip_prefix(home) {
        Ok(stripped) => {
            if stripped.as_os_str().is_empty() {
                return "~".to_string();
            }
            let mut shorthand = PathBuf::from("~");
            shorthand.push(stripped);
            shorthand.display().to_string()
        }
        Err(_) => path.display().to_string(),
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
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_loop = Arc::clone(&shutdown);
    let task = tokio::spawn(async move {
        loop {
            if shutdown_loop.load(Ordering::SeqCst) {
                break;
            }

            let accepted =
                tokio::time::timeout(Duration::from_millis(250), listener.accept()).await;
            let (mut stream, _) = match accepted {
                Ok(Ok(pair)) => pair,
                Ok(Err(err)) => {
                    eprintln!("warning: control socket accept failed: {}", err);
                    break;
                }
                Err(_) => continue,
            };

            let mut request_bytes = Vec::new();
            let response = match stream.read_to_end(&mut request_bytes).await {
                Ok(_) => match serde_json::from_slice::<ControlRequest>(&request_bytes) {
                    Ok(request) => {
                        handle_control_request(
                            &layout,
                            &state,
                            &event_tx,
                            &planned_exits,
                            &rust_log,
                            &seed,
                            request,
                        )
                        .await
                    }
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

        let _ = tokio_fs::remove_file(&socket_path).await;
    });

    Ok(ControlServerHandle { shutdown, task })
}

async fn handle_control_request(
    layout: &AssetsLayout,
    state: &Arc<Mutex<State>>,
    event_tx: &UnboundedSender<RunEvent>,
    planned_exits: &Arc<Mutex<HashSet<(String, u32)>>>,
    default_rust_log: &str,
    seed: &Arc<str>,
    request: ControlRequest,
) -> ControlResponse {
    match request {
        ControlRequest::StageProtocol {
            asset_name,
            protocol_version,
            activation_point,
            restart_sidecars,
            rust_log,
        } => {
            match assets::ensure_consensus_keys(layout, Arc::clone(seed)).await {
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
                layout,
                &StageProtocolOptions {
                    asset_name,
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
                let mut state = state.lock().await;
                let planned = sidecar_exit_keys(&state);
                if !planned.is_empty() {
                    let mut planned_exits = planned_exits.lock().await;
                    planned_exits.extend(planned);
                }

                let rust_log = rust_log.unwrap_or_else(|| default_rust_log.to_string());
                match process::restart_sidecars(layout, &mut state, &rust_log).await {
                    Ok(restarted) => {
                        for proc in &restarted {
                            restarted_sidecars.push(proc.record.node_id);
                        }
                        spawn_exit_watchers(restarted, event_tx.clone());
                    }
                    Err(err) => {
                        return ControlResponse::Error {
                            error: err.to_string(),
                        };
                    }
                }
            }

            ControlResponse::Ok {
                result: crate::control::ControlResult::StageProtocol {
                    live_mode: true,
                    staged_nodes: stage.staged_nodes,
                    restarted_sidecars,
                },
            }
        }
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

const SSE_WAIT_MESSAGE: &str = "Waiting for SSE connection...";
const BLOCK_WAIT_MESSAGE: &str = "Waiting for new blocks...";

struct SseHealth {
    expected_nodes: HashSet<u32>,
    versions: HashMap<u32, String>,
    announced: bool,
    block_seen: bool,
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
            last_uniform_version_announced: None,
            sse_spinner: None,
            block_spinner: None,
            details,
        }
    }
}

async fn should_log_primary(node_id: u32, health: &Arc<Mutex<SseHealth>>) -> bool {
    if node_id != 1 {
        return false;
    }
    let state = health.lock().await;
    state.announced
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
                        if node_id == 1
                            && let Err(err) = record_last_block_height(&state, block.height()).await
                        {
                            eprintln!("warning: failed to record last block height: {}", err);
                        }
                        if should_log_primary(node_id, &health).await {
                            mark_block_seen(&health, &layout).await;
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
            if state.block_spinner.is_none() {
                state.block_spinner = Some(start_spinner(BLOCK_WAIT_MESSAGE));
            }
            state.announced = true;
            state.block_seen = false;
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
    messages: &[casper_types::contract_messages::Message],
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

fn looks_like_url(path: &Path) -> bool {
    let value = path.to_string_lossy();
    value.starts_with("http://") || value.starts_with("https://")
}

async fn is_dir(path: &Path) -> bool {
    tokio_fs::metadata(path)
        .await
        .map(|meta| meta.is_dir())
        .unwrap_or(false)
}

async fn run_assets(args: AssetsArgs) -> Result<()> {
    match args.command {
        AssetsCommand::Add(add) => run_assets_add(add).await,
        AssetsCommand::Pull(pull) => run_assets_pull(pull).await,
        AssetsCommand::List => run_assets_list().await,
        AssetsCommand::Path(path) => run_assets_path(path).await,
    }
}

async fn run_networks(args: NetworksArgs) -> Result<()> {
    match args.command {
        NetworksCommand::List(list) => run_networks_list(list).await,
        NetworksCommand::Rm(remove) => run_networks_rm(remove).await,
    }
}

async fn run_is_ready(args: IsReadyArgs) -> Result<()> {
    let assets_root = match &args.net_path {
        Some(path) => path.clone(),
        None => assets::default_assets_root()?,
    };
    let layout = AssetsLayout::new(assets_root, args.network_name);
    let argv0 = std::env::args()
        .next()
        .unwrap_or_else(|| "casper-devnet".to_string());
    let setup_cmd = format!("{} start --setup-only", argv0);
    let net_dir = layout.net_dir();
    if !is_dir(&net_dir).await {
        return Err(anyhow!(
            "assets for {} not found; run `{}`",
            layout.network_name(),
            setup_cmd
        ));
    }

    let state_path = net_dir.join(STATE_FILE_NAME);
    let contents = match tokio_fs::read_to_string(&state_path).await {
        Ok(contents) => contents,
        Err(_) => return Err(anyhow!("network is not ready yet")),
    };
    let state =
        match tokio::task::spawn_blocking(move || serde_json::from_str::<State>(&contents)).await {
            Ok(Ok(state)) => state,
            _ => return Err(anyhow!("network is not ready yet")),
        };

    ensure_processes_running(&state)?;

    if state.last_block_height.is_none() {
        return Err(anyhow!("network is not ready yet"));
    }

    ensure_rest_ready(&state).await?;
    Ok(())
}

async fn run_assets_add(args: AssetsAddArgs) -> Result<()> {
    if is_custom_asset_add_requested(&args) {
        let mut missing = Vec::new();
        if args.casper_node.is_none() {
            missing.push("--casper-node");
        }
        if args.casper_sidecar.is_none() {
            missing.push("--casper-sidecar");
        }
        if args.chainspec.is_none() {
            missing.push("--chainspec");
        }
        if args.node_config.is_none() {
            missing.push("--node-config");
        }
        if args.sidecar_config.is_none() {
            missing.push("--sidecar-config");
        }
        if !missing.is_empty() {
            return Err(anyhow!(
                "custom asset mode requires: {}",
                missing.join(", ")
            ));
        }

        let opts = CustomAssetInstallOptions {
            name: args.path_or_name.clone(),
            casper_node: args.casper_node.expect("checked above"),
            casper_sidecar: args.casper_sidecar.expect("checked above"),
            chainspec: args.chainspec.expect("checked above"),
            node_config: args.node_config.expect("checked above"),
            sidecar_config: args.sidecar_config.expect("checked above"),
        };
        assets::install_custom_asset(&opts).await?;
        println!(
            "custom asset '{}' installed into {}",
            opts.name,
            assets::custom_assets_root()?.display()
        );
        return Ok(());
    }

    let path = PathBuf::from(&args.path_or_name);
    if looks_like_url(&path) {
        return Err(anyhow!(
            "assets URL is not supported yet; provide a local .tar.gz path"
        ));
    }

    assets::install_assets_bundle(&path).await?;
    println!(
        "assets installed into {}",
        assets::assets_bundle_root()?.display()
    );
    Ok(())
}

async fn run_assets_pull(args: AssetsPullArgs) -> Result<()> {
    assets::pull_assets_bundles(args.target.as_deref(), args.force).await?;
    Ok(())
}

async fn run_assets_list() -> Result<()> {
    let mut versions = assets::list_bundle_versions().await?;
    let custom_assets = assets::list_custom_asset_names().await?;
    if versions.is_empty() && custom_assets.is_empty() {
        return Err(anyhow!("no assets bundles or custom assets found"));
    }
    versions.sort_by(|a, b| b.cmp(a));
    for version in versions {
        println!("{}", version);
    }
    for name in custom_assets {
        println!("custom/{}", name);
    }
    Ok(())
}

async fn run_assets_path(args: AssetsPathArgs) -> Result<()> {
    let path = assets::custom_asset_path(&args.asset_name).await?;
    println!("{}", path.display());
    Ok(())
}

async fn run_networks_list(args: NetworksListArgs) -> Result<()> {
    let networks_root = match args.net_path {
        Some(path) => path,
        None => assets::default_assets_root()?,
    };
    if !is_dir(&networks_root).await {
        return Ok(());
    }

    let mut names = Vec::new();
    let mut entries = tokio_fs::read_dir(&networks_root).await?;
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        names.push(entry.file_name().to_string_lossy().to_string());
    }
    names.sort();
    for name in names {
        println!("{}", name);
    }
    Ok(())
}

async fn run_networks_rm(args: NetworksRmArgs) -> Result<()> {
    let networks_root = match args.net_path {
        Some(path) => path,
        None => assets::default_assets_root()?,
    };
    let net_dir = networks_root.join(&args.network_name);
    if !is_dir(&net_dir).await {
        return Err(anyhow!(
            "managed network '{}' not found at {}",
            args.network_name,
            net_dir.display()
        ));
    }

    let confirmed = if args.yes {
        true
    } else {
        Confirm::new()
            .with_prompt(format!(
                "Remove managed network '{}' from disk?",
                args.network_name
            ))
            .default(false)
            .interact()?
    };

    if !confirmed {
        println!("aborted");
        return Ok(());
    }

    tokio_fs::remove_dir_all(&net_dir).await?;
    println!("removed {}", net_dir.display());
    Ok(())
}

async fn run_stage_protocol(args: StageProtocolArgs) -> Result<()> {
    let assets_root = match &args.net_path {
        Some(path) => path.clone(),
        None => assets::default_assets_root()?,
    };
    let layout = AssetsLayout::new(assets_root, args.network_name.clone());
    if !layout.exists().await {
        return Err(anyhow!(
            "assets for {} not found under {}; run `start --setup-only` first",
            layout.network_name(),
            shorten_home_path(&layout.net_dir().display().to_string())
        ));
    }

    let request = ControlRequest::StageProtocol {
        asset_name: args.asset_name.clone(),
        protocol_version: args.protocol_version.clone(),
        activation_point: args.activation_point,
        restart_sidecars: true,
        rust_log: None,
    };
    let socket_path = layout.control_socket_path();
    if is_control_socket(&socket_path).await {
        if let Ok(Some(current_era)) = read_current_era_from_status(1).await
            && args.activation_point <= current_era
        {
            return Err(anyhow!(
                "activation point {} must be greater than current era {}",
                args.activation_point,
                current_era
            ));
        }

        return match send_request(&socket_path, &request).await {
            Ok(ControlResponse::Ok { result }) => {
                let (staged_nodes, restarted_sidecars, live_mode) = match result {
                    ControlResult::StageProtocol {
                        live_mode,
                        staged_nodes,
                        restarted_sidecars,
                    } => (staged_nodes, restarted_sidecars, live_mode),
                };
                println!(
                    "staged protocol {} from custom asset '{}' for {} node(s) (live_mode={})",
                    args.protocol_version, args.asset_name, staged_nodes, live_mode
                );
                if restarted_sidecars.is_empty() {
                    println!("restarted sidecars: none");
                } else {
                    let mut ids = restarted_sidecars;
                    ids.sort_unstable();
                    println!(
                        "restarted sidecars: {}",
                        ids.into_iter()
                            .map(|id| format!("node-{}", id))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
                Ok(())
            }
            Ok(ControlResponse::Error { error }) => Err(anyhow!(error)),
            Err(err) => Err(anyhow!(
                "failed to stage protocol via live control socket {}: {}",
                socket_path.display(),
                err
            )),
        };
    }

    let staged = assets::stage_protocol(
        &layout,
        &StageProtocolOptions {
            asset_name: args.asset_name.clone(),
            protocol_version: args.protocol_version.clone(),
            activation_point: args.activation_point,
        },
    )
    .await?;
    println!(
        "staged protocol {} from custom asset '{}' for {} node(s) (offline mode; sidecars not restarted)",
        args.protocol_version, args.asset_name, staged.staged_nodes
    );
    Ok(())
}

async fn is_control_socket(path: &Path) -> bool {
    tokio_fs::symlink_metadata(path)
        .await
        .map(|metadata| metadata.file_type().is_socket())
        .unwrap_or(false)
}

fn is_custom_asset_add_requested(args: &AssetsAddArgs) -> bool {
    args.casper_node.is_some()
        || args.casper_sidecar.is_some()
        || args.chainspec.is_some()
        || args.node_config.is_some()
        || args.sidecar_config.is_some()
}

async fn resolve_protocol_version(candidate: &Option<String>) -> Result<String> {
    if let Some(raw) = candidate {
        let version = assets::parse_protocol_version(raw)?;
        if !assets::has_bundle_version(&version).await? {
            let argv0 = std::env::args()
                .next()
                .unwrap_or_else(|| "casper-devnet".to_string());
            let pull_cmd = format!("{} assets pull", argv0);
            let add_cmd = format!("{} assets add <path-to-assets.tar.gz>", argv0);
            return Err(anyhow!(
                "assets for version {} not found; run `{}` or `{}`",
                version,
                pull_cmd,
                add_cmd
            ));
        }
        return Ok(version.to_string());
    }
    let versions = assets::list_bundle_versions().await?;
    if versions.is_empty() {
        let argv0 = std::env::args()
            .next()
            .unwrap_or_else(|| "casper-devnet".to_string());
        let pull_cmd = format!("{} assets pull", argv0);
        let add_cmd = format!("{} assets add <path-to-assets.tar.gz>", argv0);
        return Err(anyhow!(
            "no assets found; run `{}` or `{}`",
            pull_cmd,
            add_cmd
        ));
    }
    let version = versions
        .into_iter()
        .max()
        .expect("non-empty assets versions");
    Ok(version.to_string())
}

fn ensure_processes_running(state: &State) -> Result<()> {
    if state.processes.is_empty() {
        return Err(anyhow!("network is not ready yet"));
    }
    for process in &state.processes {
        if !matches!(process.last_status, ProcessStatus::Running) {
            return Err(anyhow!("network is not ready yet"));
        }
        let pid = match process.pid {
            Some(pid) => pid,
            None => return Err(anyhow!("network is not ready yet")),
        };
        if !is_pid_running(pid) {
            return Err(anyhow!("network is not ready yet"));
        }
    }
    Ok(())
}

fn is_pid_running(pid: u32) -> bool {
    let pid = Pid::from_raw(pid as i32);
    match kill(pid, None) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false,
        Err(_) => true,
    }
}

async fn read_current_era_from_status(node_id: u32) -> Result<Option<u64>> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()?;
    let url = format!("{}/status", assets::rest_endpoint(node_id));
    let response = client.get(&url).send().await?;
    if response.status() != reqwest::StatusCode::OK {
        return Ok(None);
    }
    let status = response.json::<NodeStatus>().await?;
    Ok(extract_era_id(status.last_added_block_info.as_ref()))
}

fn extract_era_id(value: Option<&serde_json::Value>) -> Option<u64> {
    let value = value?;
    if let Some(era_id) = value.get("era_id").and_then(|era_id| era_id.as_u64()) {
        return Some(era_id);
    }
    value
        .get("era_id")
        .and_then(|era_id| era_id.as_str())
        .and_then(|era_id| era_id.parse::<u64>().ok())
}

async fn ensure_rest_ready(state: &State) -> Result<()> {
    let node_ids: HashSet<u32> = state
        .processes
        .iter()
        .filter_map(|process| {
            if matches!(process.kind, ProcessKind::Node) {
                Some(process.node_id)
            } else {
                None
            }
        })
        .collect();
    if node_ids.is_empty() {
        return Err(anyhow!("network is not ready yet"));
    }

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()?;

    for node_id in node_ids {
        let url = format!("{}/status", assets::rest_endpoint(node_id));
        let response = match client.get(&url).send().await {
            Ok(response) => response,
            Err(_) => return Err(anyhow!("network is not ready yet")),
        };
        if response.status() != reqwest::StatusCode::OK {
            return Err(anyhow!("network is not ready yet"));
        }
        let status = match response.json::<NodeStatus>().await {
            Ok(status) => status,
            Err(_) => return Err(anyhow!("network is not ready yet")),
        };
        if !status
            .reactor_state
            .as_deref()
            .map(is_ready_reactor_state)
            .unwrap_or(false)
        {
            return Err(anyhow!("network is not ready yet"));
        }
    }
    Ok(())
}

fn is_ready_reactor_state(state: &str) -> bool {
    state == "Validate"
}

#[derive(Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
struct NodeStatus {
    peers: Option<Vec<NodePeer>>,
    api_version: Option<String>,
    build_version: Option<String>,
    chainspec_name: Option<String>,
    starting_state_root_hash: Option<String>,
    last_added_block_info: Option<serde_json::Value>,
    our_public_signing_key: Option<String>,
    round_length: Option<serde_json::Value>,
    next_upgrade: Option<serde_json::Value>,
    uptime: Option<String>,
    reactor_state: Option<String>,
    last_progress: Option<String>,
    available_block_range: Option<BlockRange>,
    block_sync: Option<BlockSync>,
    latest_switch_block_hash: Option<serde_json::Value>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
struct NodePeer {
    node_id: Option<String>,
    address: Option<String>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
struct BlockRange {
    low: Option<u64>,
    high: Option<u64>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "snake_case")]
struct BlockSync {
    historical: Option<serde_json::Value>,
    forward: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::{
        encode_hex, extract_era_id, format_cspr_u512, format_message_payload, shorten_home_path,
    };
    use casper_types::U512;
    use casper_types::contract_messages::MessagePayload;
    use directories::BaseDirs;
    use serde_json::json;

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
    fn encode_hex_renders_lowercase() {
        assert_eq!(encode_hex(&[0x00, 0xAB, 0x0f]), "00ab0f");
    }

    #[test]
    fn shorten_home_path_replaces_home_prefix() {
        let Some(base_dirs) = BaseDirs::new() else {
            return;
        };
        let home = base_dirs.home_dir();
        let shortened = shorten_home_path(&home.to_string_lossy());
        assert_eq!(shortened, "~");

        let nested = home.join("devnet/logs/stdout.log");
        let shortened_nested = shorten_home_path(&nested.to_string_lossy());
        assert!(shortened_nested.starts_with("~"));
        assert!(shortened_nested.contains("devnet"));
    }

    #[test]
    fn shorten_home_path_keeps_relative_paths() {
        let input = "relative/path";
        assert_eq!(shorten_home_path(input), input);
    }

    #[test]
    fn extract_era_id_from_status_payload() {
        assert_eq!(extract_era_id(Some(&json!({"era_id": 123}))), Some(123));
        assert_eq!(extract_era_id(Some(&json!({"era_id": "456"}))), Some(456));
        assert_eq!(extract_era_id(Some(&json!({"era_id": "abc"}))), None);
        assert_eq!(extract_era_id(None), None);
    }
}
