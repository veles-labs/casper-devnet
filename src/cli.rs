use crate::assets::{self, AssetsLayout, SetupOptions};
use crate::process::{self, ProcessHandle, RunningProcess, StartPlan};
use crate::state::{ProcessKind, ProcessRecord, ProcessStatus, State};
use anyhow::{anyhow, Result};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use casper_types::contract_messages::MessagePayload;
use casper_types::execution::ExecutionResult;
use casper_types::U512;
use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::Mutex;
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
    /// Manage assets bundles.
    Assets(AssetsArgs),
}

/// Arguments for `nctl start`.
#[derive(Args, Clone)]
struct StartArgs {
    /// Network name used in assets paths and configs.
    #[arg(long, default_value = "casper-dev")]
    network_name: String,

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
    #[arg(long, default_value_t = 30)]
    delay: u64,

    /// Log level for child processes (passed as `RUST_LOG`).
    #[arg(long, default_value = "info")]
    loglevel: String,

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
    /// List available protocol versions in the assets store.
    List,
}

/// Arguments for `nctl assets add`.
#[derive(Args, Clone)]
struct AssetsAddArgs {
    /// Path to a local assets bundle (.tar.gz).
    #[arg(value_name = "PATH")]
    path: PathBuf,
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

/// Parses CLI and runs the selected subcommand.
pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Start(args) => run_start(args).await,
        Command::Assets(args) => run_assets(args).await,
    }
}

async fn run_start(args: StartArgs) -> Result<()> {
    let assets_root = assets::default_assets_root()?;
    let layout = AssetsLayout::new(assets_root, args.network_name.clone());
    println!("assets path: {}", layout.net_dir().display());
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
            layout.net_dir().display()
        ));
    }

    let rust_log = args.loglevel.clone();

    let plan = StartPlan { rust_log };

    let state_path = layout.net_dir().join("state.json");
    let mut state = State::new(state_path).await?;
    let started = process::start(&layout, &plan, &mut state).await?;

    print_pids(&started);
    print_start_banner(&layout, &started).await;
    print_derived_accounts_summary(&layout).await;

    let node_ids = unique_node_ids(&started);
    let details = format_network_details(&layout, &started).await;
    let health = Arc::new(Mutex::new(SseHealth::new(node_ids.clone(), details)));
    println!("Waiting for SSE connection...");
    spawn_sse_listeners(&layout, &node_ids, health).await;

    let (event_tx, mut event_rx) = unbounded_channel();
    spawn_ctrlc_listener(event_tx.clone());
    spawn_exit_watchers(started, event_tx);

    if let Some(event) = event_rx.recv().await {
        match event {
            RunEvent::CtrlC => {
                process::stop(&mut state).await?;
            }
            RunEvent::ProcessExit {
                id,
                pid,
                code,
                signal,
            } => {
                update_exited_process(&mut state, &id, code, signal).await?;
                log_exit(&id, pid, code, signal);
                process::stop(&mut state).await?;
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
            layout.net_dir().display()
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
    let mut node_pids: HashMap<u32, u32> = HashMap::new();
    let mut sidecar_pids: HashMap<u32, u32> = HashMap::new();
    let mut process_logs: HashMap<u32, Vec<(ProcessKind, u32, String, String)>> = HashMap::new();

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
                .push((
                    process.record.kind.clone(),
                    pid,
                    process.record.stdout_path.clone(),
                    process.record.stderr_path.clone(),
                ));
        }
    }

    let node_ids = unique_node_ids(processes);

    let mut lines = Vec::new();
    lines.push("network details".to_string());
    lines.push(format!("assets: {}", layout.net_dir().display()));
    for node_id in node_ids {
        let node_pid = node_pids
            .get(&node_id)
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".to_string());
        let sidecar_pid = sidecar_pids
            .get(&node_id)
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "-".to_string());
        lines.push(format!(
            "node-{} pid={} sidecar pid={}",
            node_id, node_pid, sidecar_pid
        ));
        if let Some(entries) = process_logs.get(&node_id) {
            let mut entries = entries.clone();
            entries.sort_by_key(|entry| process_kind_label(&entry.0).to_string());
            for (kind, pid, stdout, stderr) in entries {
                lines.push(format!(
                    "  {} pid={} stdout={} stderr={}",
                    process_kind_label(&kind),
                    pid,
                    stdout,
                    stderr
                ));
            }
        }
        lines.push(format!("  rest:   {}", assets::rest_endpoint(node_id)));
        lines.push(format!("  sse:    {}", assets::sse_endpoint(node_id)));
        lines.push(format!("  rpc:    {}", assets::rpc_endpoint(node_id)));
        lines.push(format!("  binary: {}", assets::binary_address(node_id)));
        lines.push(format!("  gossip: {}", assets::network_address(node_id)));
    }

    lines.join("\n")
}

async fn print_derived_accounts_summary(layout: &AssetsLayout) {
    if let Some(summary) = assets::derived_accounts_summary(layout).await {
        println!("derived accounts");
        for line in summary.lines() {
            println!("  {}", line);
        }
    }
}

fn process_kind_label(kind: &ProcessKind) -> &'static str {
    match kind {
        ProcessKind::Node => "node",
        ProcessKind::Sidecar => "sidecar",
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

struct SseHealth {
    expected_nodes: HashSet<u32>,
    versions: HashMap<u32, String>,
    announced: bool,
    details: String,
}

impl SseHealth {
    fn new(node_ids: Vec<u32>, details: String) -> Self {
        Self {
            expected_nodes: node_ids.into_iter().collect(),
            versions: HashMap::new(),
            announced: false,
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

async fn spawn_sse_listeners(
    _layout: &AssetsLayout,
    node_ids: &[u32],
    health: Arc<Mutex<SseHealth>>,
) {
    for node_id in node_ids {
        let node_id = *node_id;
        let endpoint = assets::sse_endpoint(node_id);
        let health = Arc::clone(&health);
        tokio::spawn(async move {
            run_sse_listener(node_id, endpoint, health).await;
        });
    }
}

async fn run_sse_listener(node_id: u32, endpoint: String, health: Arc<Mutex<SseHealth>>) {
    let mut backoff = ExponentialBackoff::default();

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
        let mut stream_failed = false;
        while let Some(event) = stream.next().await {
            match event {
                Ok(sse_event) => match sse_event {
                    SseEvent::ApiVersion(version) => {
                        record_api_version(node_id, version.to_string(), &health).await;
                    }
                    SseEvent::BlockAdded { block_hash, block } => {
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
                    _ => {}
                },
                Err(_) => {
                    stream_failed = true;
                    break;
                }
            }
        }

        if stream_failed && !sleep_backoff(&mut backoff).await {
            return;
        }
    }
}

async fn record_api_version(node_id: u32, version: String, health: &Arc<Mutex<SseHealth>>) {
    let mut state = health.lock().await;
    if !state.expected_nodes.contains(&node_id) {
        return;
    }
    state.versions.insert(node_id, version);
    if state.announced || state.versions.len() != state.expected_nodes.len() {
        return;
    }

    let summary = version_summary(&state.versions);
    println!("Network is healthy ({})", summary);
    println!("{}", state.details);
    println!("Waiting for new blocks...");
    state.announced = true;
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

async fn run_assets(args: AssetsArgs) -> Result<()> {
    match args.command {
        AssetsCommand::Add(add) => run_assets_add(add).await,
        AssetsCommand::Pull(pull) => run_assets_pull(pull).await,
        AssetsCommand::List => run_assets_list().await,
    }
}

async fn run_assets_add(args: AssetsAddArgs) -> Result<()> {
    if looks_like_url(&args.path) {
        return Err(anyhow!(
            "assets URL is not supported yet; provide a local .tar.gz path"
        ));
    }
    assets::install_assets_bundle(&args.path).await?;
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
    if versions.is_empty() {
        println!("no assets found");
        return Ok(());
    }
    versions.sort_by(|a, b| b.cmp(a));
    for version in versions {
        println!("{}", version);
    }
    Ok(())
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
    match assets::most_recent_bundle_version().await {
        Ok(version) => Ok(version.to_string()),
        Err(_) => {
            let argv0 = std::env::args()
                .next()
                .unwrap_or_else(|| "casper-devnet".to_string());
            let pull_cmd = format!("{} assets pull", argv0);
            let add_cmd = format!("{} assets add <path-to-assets.tar.gz>", argv0);
            Err(anyhow!(
                "no assets found; run `{}` or `{}`",
                pull_cmd,
                add_cmd
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{encode_hex, format_cspr_u512, format_message_payload};
    use casper_types::contract_messages::MessagePayload;
    use casper_types::U512;

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
}
