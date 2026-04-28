use super::stage_protocol;
use crate::assets::{self, AssetsLayout};
use crate::cli::common::{
    is_control_socket, is_dir, is_file, is_ready_reactor_state, shorten_home_path,
};
use crate::control::{ControlRequest, ControlResponse, ControlResult, send_request};
use crate::state::{ProcessKind, ProcessStatus, STATE_FILE_NAME, State};
use anyhow::{Result, anyhow};
use clap::{ArgGroup, Args, Subcommand};
use nix::errno::Errno;
use nix::sys::signal::kill;
use nix::unistd::Pid;
use rand::prelude::IndexedRandom;
use serde::Deserialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs as tokio_fs;

const LIVE_CONTROL_QUERY_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Args, Clone)]
pub(crate) struct NetworkArgs {
    /// Managed network name.
    #[arg(value_name = "NETWORK_NAME")]
    network_name: String,

    #[command(subcommand)]
    command: NetworkCommand,
}

#[derive(Subcommand, Clone)]
enum NetworkCommand {
    /// Add managed nodes to a running network.
    AddNodes(NetworkAddNodesArgs),
    /// Check whether a network has observed a block yet.
    IsReady(NetworkIsReadyArgs),
    /// Print the chainspec path for a staged protocol version.
    Path(NetworkPathArgs),
    /// Print a random live endpoint for a running node in the network.
    Port(NetworkPortArgs),
    /// Print REST /status for a specific node.
    Status(NetworkStatusArgs),
    /// Stage a protocol upgrade for this network.
    StageProtocol(stage_protocol::StageProtocolArgs),
}

#[derive(Args, Clone)]
struct NetworkAddNodesArgs {
    /// Number of nodes to add.
    #[arg(long)]
    count: u32,

    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,
}

#[derive(Args, Clone)]
struct NetworkIsReadyArgs {
    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,
}

#[derive(Args, Clone)]
#[command(group(
    ArgGroup::new("endpoint")
        .required(true)
        .multiple(false)
        .args(["rpc", "sse", "rest", "binary", "diagnostics"])
))]
struct NetworkPortArgs {
    /// Print a random node RPC URL.
    #[arg(long, group = "endpoint")]
    rpc: bool,

    /// Print a random node SSE URL.
    #[arg(long, group = "endpoint")]
    sse: bool,

    /// Print a random node REST URL.
    #[arg(long, group = "endpoint")]
    rest: bool,

    /// Print a random node binary port address.
    #[arg(long, group = "endpoint")]
    binary: bool,

    /// Print a random node diagnostics socket path.
    #[arg(long, group = "endpoint")]
    diagnostics: bool,

    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,
}

#[derive(Args, Clone)]
struct NetworkPathArgs {
    /// Protocol version to inspect (e.g. 2.2.0). When omitted, prints the network root.
    #[arg(value_name = "PROTOCOL_VERSION")]
    protocol_version: Option<String>,

    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,
}

#[derive(Args, Clone)]
struct NetworkStatusArgs {
    /// Node id to query.
    #[arg(long)]
    node_id: u32,

    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PortSelection {
    Rpc,
    Sse,
    Rest,
    Binary,
    Diagnostics,
}

impl NetworkPortArgs {
    fn selection(&self) -> PortSelection {
        if self.rpc {
            PortSelection::Rpc
        } else if self.sse {
            PortSelection::Sse
        } else if self.rest {
            PortSelection::Rest
        } else if self.binary {
            PortSelection::Binary
        } else {
            PortSelection::Diagnostics
        }
    }
}

pub(crate) async fn run(args: NetworkArgs) -> Result<()> {
    match args.command {
        NetworkCommand::AddNodes(add_nodes) => {
            run_network_add_nodes(args.network_name, add_nodes).await
        }
        NetworkCommand::IsReady(is_ready) => {
            run_network_is_ready(args.network_name, is_ready).await
        }
        NetworkCommand::Path(path) => run_network_path(args.network_name, path).await,
        NetworkCommand::Port(port) => run_network_port(args.network_name, port).await,
        NetworkCommand::Status(status) => run_network_status(args.network_name, status).await,
        NetworkCommand::StageProtocol(stage_protocol_args) => {
            stage_protocol::run(args.network_name, stage_protocol_args).await
        }
    }
}

async fn run_network_add_nodes(network_name: String, args: NetworkAddNodesArgs) -> Result<()> {
    if args.count == 0 {
        return Err(anyhow!("--count must be greater than 0"));
    }

    let assets_root = match &args.net_path {
        Some(path) => path.clone(),
        None => assets::default_assets_root()?,
    };
    let layout = AssetsLayout::new(assets_root, network_name);
    let net_dir = layout.net_dir();
    if !is_dir(&net_dir).await {
        return Err(anyhow!(
            "assets for {} not found under {}; run `start` first",
            layout.network_name(),
            shorten_home_path(&net_dir.display().to_string())
        ));
    }

    let socket_path = layout.control_socket_path();
    if !is_control_socket(&socket_path).await {
        return Err(anyhow!(
            "network {} is not running under live control socket {}; start it first",
            layout.network_name(),
            socket_path.display()
        ));
    }

    let response = send_request(
        &socket_path,
        &ControlRequest::AddNodes { count: args.count },
    )
    .await
    .map_err(|err| {
        anyhow!(
            "failed to add nodes via live control socket {}: {}",
            socket_path.display(),
            err
        )
    })?;

    match response {
        ControlResponse::Ok {
            result:
                ControlResult::AddNodes {
                    added_node_ids,
                    total_nodes,
                    started_processes,
                },
        } => {
            let added = added_node_ids
                .into_iter()
                .map(|id| format!("node-{}", id))
                .collect::<Vec<_>>()
                .join(", ");
            println!(
                "added {} node(s): {} (total_nodes={}, started_processes={})",
                args.count, added, total_nodes, started_processes
            );
            Ok(())
        }
        ControlResponse::Ok { result } => Err(anyhow!(
            "unexpected control response from {}: {:?}",
            socket_path.display(),
            result
        )),
        ControlResponse::Error { error } => Err(anyhow!(error)),
    }
}

async fn run_network_port(network_name: String, args: NetworkPortArgs) -> Result<()> {
    let assets_root = match &args.net_path {
        Some(path) => path.clone(),
        None => assets::default_assets_root()?,
    };
    let selection = args.selection();
    let layout = AssetsLayout::new(assets_root, network_name);
    let net_dir = layout.net_dir();
    if !is_dir(&net_dir).await {
        return Err(anyhow!(
            "assets for {} not found under {}; run `start --setup-only` first",
            layout.network_name(),
            shorten_home_path(&net_dir.display().to_string())
        ));
    }

    let node_ids = if let Some(node_ids) = query_live_running_node_ids(&layout).await? {
        node_ids
    } else {
        let state = read_state_snapshot(&net_dir)
            .await
            .map_err(|_| anyhow!("network is not running yet"))?;
        running_node_ids(&state)
    };
    if node_ids.is_empty() {
        return Err(anyhow!(
            "no running node processes found for {}",
            layout.network_name()
        ));
    }

    let mut rng = rand::rng();
    let node_id = *node_ids.choose(&mut rng).expect("non-empty node ids");
    println!("{}", endpoint_for_selection(&layout, node_id, selection));
    Ok(())
}

async fn run_network_status(network_name: String, args: NetworkStatusArgs) -> Result<()> {
    if args.node_id == 0 {
        return Err(anyhow!("--node-id must be greater than 0"));
    }

    let assets_root = match &args.net_path {
        Some(path) => path.clone(),
        None => assets::default_assets_root()?,
    };
    let layout = AssetsLayout::new(assets_root, network_name);
    let net_dir = layout.net_dir();
    if !is_dir(&net_dir).await {
        return Err(anyhow!(
            "assets for {} not found under {}; run `start` first",
            layout.network_name(),
            shorten_home_path(&net_dir.display().to_string())
        ));
    }

    let url = format!("{}/status", assets::rest_endpoint(args.node_id));
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(4))
        .build()?;
    let status = client
        .get(&url)
        .send()
        .await
        .map_err(|err| anyhow!("failed to query {}: {}", url, err))?
        .error_for_status()
        .map_err(|err| anyhow!("failed to query {}: {}", url, err))?
        .json::<serde_json::Value>()
        .await
        .map_err(|err| anyhow!("failed to parse {} response: {}", url, err))?;

    println!("{}", format_status(&status));
    Ok(())
}

async fn run_network_path(network_name: String, args: NetworkPathArgs) -> Result<()> {
    let assets_root = match &args.net_path {
        Some(path) => path.clone(),
        None => assets::default_assets_root()?,
    };
    let layout = AssetsLayout::new(assets_root, network_name);
    let paths = resolve_network_paths(&layout, args.protocol_version.as_deref()).await?;
    for path in paths {
        println!("{}", path.display());
    }
    Ok(())
}

pub(crate) async fn resolve_network_paths(
    layout: &AssetsLayout,
    protocol_version: Option<&str>,
) -> Result<Vec<PathBuf>> {
    let net_dir = layout.net_dir();
    if !is_dir(&net_dir).await {
        return Err(anyhow!(
            "assets for {} not found under {}; run `start --setup-only` first",
            layout.network_name(),
            shorten_home_path(&net_dir.display().to_string())
        ));
    }

    let Some(protocol_version) = protocol_version else {
        return Ok(vec![net_dir]);
    };

    let protocol_version = assets::parse_protocol_version(protocol_version)?;
    let version_fs = protocol_version.to_string().replace('.', "_");
    let node_ids = layout.node_ids().await?;
    if node_ids.is_empty() {
        return Err(anyhow!(
            "no nodes found under {}; run `start --setup-only` first",
            layout.nodes_dir().display()
        ));
    }

    let mut paths = Vec::with_capacity(node_ids.len());
    let mut missing = Vec::new();
    for node_id in node_ids {
        let config_dir = layout.node_config_root(node_id).join(&version_fs);
        let chainspec_path = config_dir.join("chainspec.toml");
        if !is_file(&chainspec_path).await {
            missing.push(format!("node-{} ({})", node_id, config_dir.display()));
            continue;
        }
        paths.push(config_dir);
    }

    if !missing.is_empty() {
        return Err(anyhow!(
            "staged config directories for protocol {} not found for {}",
            protocol_version,
            missing.join(", ")
        ));
    }

    Ok(paths)
}

async fn run_network_is_ready(network_name: String, args: NetworkIsReadyArgs) -> Result<()> {
    let assets_root = match &args.net_path {
        Some(path) => path.clone(),
        None => assets::default_assets_root()?,
    };
    let layout = AssetsLayout::new(assets_root, network_name);
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

    let state = read_state_snapshot(&net_dir)
        .await
        .map_err(|_| anyhow!("network is not ready yet"))?;

    ensure_processes_running(&state)?;

    if state.last_block_height.is_none() {
        return Err(anyhow!("network is not ready yet"));
    }

    ensure_rest_ready(&state).await?;
    Ok(())
}

async fn read_state_snapshot(net_dir: &Path) -> Result<State> {
    let state_path = net_dir.join(STATE_FILE_NAME);
    let contents = tokio_fs::read_to_string(&state_path).await?;
    match tokio::task::spawn_blocking(move || serde_json::from_str::<State>(&contents)).await {
        Ok(Ok(state)) => Ok(state),
        Ok(Err(err)) => Err(err.into()),
        Err(err) => Err(anyhow!("failed to parse {}: {}", state_path.display(), err)),
    }
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
        let Some(pid) = process.current_pid() else {
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

fn endpoint_for_selection(layout: &AssetsLayout, node_id: u32, selection: PortSelection) -> String {
    match selection {
        PortSelection::Rpc => assets::rpc_endpoint(node_id),
        PortSelection::Sse => assets::sse_endpoint(node_id),
        PortSelection::Rest => assets::rest_endpoint(node_id),
        PortSelection::Binary => assets::binary_address(node_id),
        PortSelection::Diagnostics => {
            assets::diagnostics_socket_path(layout.network_name(), node_id)
        }
    }
}

fn format_status(status: &serde_json::Value) -> String {
    let Some(status) = status.as_object() else {
        return "Cannot parse status return.".to_string();
    };

    if let Some(error) = status.get("error") {
        return format!("status error: {}", display_json_value(error, "None"));
    }

    let is_new_status = status.get("available_block_range").is_some();
    let mut output = Vec::new();

    if let Some(block_info) = status
        .get("last_added_block_info")
        .and_then(serde_json::Value::as_object)
    {
        let height = block_info
            .get("height")
            .map(|value| display_json_value(value, "None"))
            .unwrap_or_else(|| "None".to_string());
        let era = block_info
            .get("era_id")
            .map(|value| display_json_value(value, "None"))
            .unwrap_or_else(|| "None".to_string());
        output.push(format!("Last Block: {} (Era: {})", height, era));
    }

    output.extend([
        format!(
            "Peer Count: {}",
            status
                .get("peers")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or(0)
        ),
        format!("Uptime: {}", display_object_string(status, "uptime", "")),
        format!(
            "Build: {}",
            display_object_value(status, "build_version", "None")
        ),
        format!(
            "Key: {}",
            display_object_value(status, "our_public_signing_key", "None")
        ),
        format!(
            "Next Upgrade: {}",
            display_object_value(status, "next_upgrade", "None")
        ),
        String::new(),
    ]);

    if is_new_status {
        output.push(format!(
            "Reactor State: {}",
            display_object_string(status, "reactor_state", "")
        ));
        let (low, high) = status
            .get("available_block_range")
            .and_then(serde_json::Value::as_object)
            .map(|range| {
                (
                    range
                        .get("low")
                        .map(|value| display_json_value(value, ""))
                        .unwrap_or_default(),
                    range
                        .get("high")
                        .map(|value| display_json_value(value, ""))
                        .unwrap_or_default(),
                )
            })
            .unwrap_or_default();
        output.push(format!(
            "Available Block Range - Low: {}  High: {}",
            low, high
        ));
    }
    output.push(String::new());

    output.join("\n")
}

fn display_object_string(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    default: &str,
) -> String {
    object
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or(default)
        .to_string()
}

fn display_object_value(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    default: &str,
) -> String {
    object
        .get(key)
        .map(|value| display_json_value(value, default))
        .unwrap_or_else(|| default.to_string())
}

fn display_json_value(value: &serde_json::Value, default: &str) -> String {
    match value {
        serde_json::Value::Null => default.to_string(),
        serde_json::Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

pub(crate) async fn query_live_running_node_ids(layout: &AssetsLayout) -> Result<Option<Vec<u32>>> {
    query_live_running_node_ids_with_timeout(layout, LIVE_CONTROL_QUERY_TIMEOUT).await
}

pub(crate) async fn query_live_running_node_ids_with_timeout(
    layout: &AssetsLayout,
    timeout: Duration,
) -> Result<Option<Vec<u32>>> {
    let socket_path = layout.control_socket_path();
    if !is_control_socket(&socket_path).await {
        return Ok(None);
    }

    let response = match tokio::time::timeout(
        timeout,
        send_request(&socket_path, &ControlRequest::RuntimeStatus),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => {
            eprintln!(
                "warning: failed to query live control socket {}: {}; falling back to {}",
                socket_path.display(),
                err,
                layout.net_dir().join(STATE_FILE_NAME).display()
            );
            return Ok(None);
        }
        Err(_) => {
            eprintln!(
                "warning: live control socket {} did not respond within {:?}; falling back to {}",
                socket_path.display(),
                timeout,
                layout.net_dir().join(STATE_FILE_NAME).display()
            );
            return Ok(None);
        }
    };

    match response {
        ControlResponse::Ok {
            result:
                ControlResult::RuntimeStatus {
                    running_node_ids, ..
                },
        } => Ok(Some(running_node_ids)),
        ControlResponse::Ok { .. } => Err(anyhow!(
            "unexpected control response from {}",
            socket_path.display()
        )),
        ControlResponse::Error { error } => Err(anyhow!(
            "live control socket {} returned error: {}",
            socket_path.display(),
            error
        )),
    }
}

fn ensure_processes_running(state: &State) -> Result<()> {
    if state.processes.is_empty() {
        return Err(anyhow!("network is not ready yet"));
    }
    for process in &state.processes {
        if !matches!(process.last_status, ProcessStatus::Running) {
            return Err(anyhow!("network is not ready yet"));
        }
        let pid = match process.current_pid() {
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
        PortSelection, endpoint_for_selection, format_status, query_live_running_node_ids,
        query_live_running_node_ids_with_timeout, resolve_network_paths,
    };
    use crate::control::{ControlRequest, ControlResponse, ControlResult};
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::fs as tokio_fs;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;

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
        tokio_fs::write(
            layout
                .node_config_root(1)
                .join(&version_fs)
                .join("config.toml"),
            "[logging]\nformat = \"text\"\n",
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
        tokio_fs::write(layout.net_dir().join("chainspec/accounts.toml"), "")
            .await
            .unwrap();
        layout
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_network_paths_without_version_returns_network_root() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = create_test_network_layout(temp_dir.path(), "casper-dev", "1.0.0").await;

        let paths = resolve_network_paths(&layout, None).await.unwrap();

        assert_eq!(paths, vec![layout.net_dir()]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_network_paths_with_version_returns_each_node_config_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let layout = create_test_network_layout(temp_dir.path(), "casper-dev", "1.0.0").await;
        tokio_fs::create_dir_all(layout.node_config_root(2).join("1_0_0"))
            .await
            .unwrap();
        tokio_fs::write(
            layout
                .node_config_root(2)
                .join("1_0_0")
                .join("chainspec.toml"),
            "",
        )
        .await
        .unwrap();

        let paths = resolve_network_paths(&layout, Some("1.0.0")).await.unwrap();

        assert_eq!(
            paths,
            vec![
                layout.node_config_root(1).join("1_0_0"),
                layout.node_config_root(2).join("1_0_0"),
            ]
        );
    }

    #[test]
    fn endpoint_for_selection_maps_expected_urls() {
        let layout = crate::assets::AssetsLayout::new(
            PathBuf::from("/tmp/networks"),
            "casper-dev".to_string(),
        );
        assert_eq!(
            endpoint_for_selection(&layout, 2, PortSelection::Rpc),
            "http://127.0.0.1:11102/rpc"
        );
        assert_eq!(
            endpoint_for_selection(&layout, 2, PortSelection::Diagnostics),
            crate::assets::diagnostics_socket_path("casper-dev", 2)
        );
    }

    #[test]
    fn format_status_renders_new_rest_status() {
        let status = serde_json::json!({
            "last_added_block_info": { "height": 42, "era_id": 7 },
            "peers": [{ "node_id": "a" }, { "node_id": "b" }],
            "uptime": "10s",
            "build_version": "2.0.0",
            "our_public_signing_key": "public-key",
            "next_upgrade": { "activation_point": 50 },
            "reactor_state": "Validate",
            "available_block_range": { "low": 1, "high": 42 }
        });

        assert_eq!(
            format_status(&status),
            "Last Block: 42 (Era: 7)\nPeer Count: 2\nUptime: 10s\nBuild: 2.0.0\nKey: public-key\nNext Upgrade: {\"activation_point\":50}\n\nReactor State: Validate\nAvailable Block Range - Low: 1  High: 42\n"
        );
    }

    #[test]
    fn format_status_handles_legacy_and_missing_fields() {
        let status = serde_json::json!({
            "peers": [],
            "last_added_block_info": { "height": 3, "era_id": 1 }
        });

        assert_eq!(
            format_status(&status),
            "Last Block: 3 (Era: 1)\nPeer Count: 0\nUptime: \nBuild: None\nKey: None\nNext Upgrade: None\n\n"
        );
    }

    #[test]
    fn format_status_renders_error() {
        let status = serde_json::json!({ "error": "node unavailable" });

        assert_eq!(format_status(&status), "status error: node unavailable");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_live_running_node_ids_uses_control_socket_runtime_status() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let network_name = format!("casper-dev-test-{suffix}");
        let layout = crate::assets::AssetsLayout::new(PathBuf::from("/tmp/networks"), network_name);
        let socket_path = layout.control_socket_path();
        let _ = tokio_fs::remove_file(&socket_path).await;

        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request_bytes = Vec::new();
            stream.read_to_end(&mut request_bytes).await.unwrap();
            let request: ControlRequest = serde_json::from_slice(&request_bytes).unwrap();
            assert!(matches!(request, ControlRequest::RuntimeStatus));

            let response = ControlResponse::Ok {
                result: ControlResult::RuntimeStatus {
                    running_node_ids: vec![2, 4],
                    last_block_height: Some(123),
                },
            };
            let response_bytes = serde_json::to_vec(&response).unwrap();
            stream.write_all(&response_bytes).await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let node_ids = query_live_running_node_ids(&layout).await.unwrap();
        assert_eq!(node_ids, Some(vec![2, 4]));

        server.await.unwrap();
        let _ = tokio_fs::remove_file(&socket_path).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_live_running_node_ids_times_out_without_hanging() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let network_name = format!("casper-dev-stalled-{suffix}");
        let layout = crate::assets::AssetsLayout::new(PathBuf::from("/tmp/networks"), network_name);
        let socket_path = layout.control_socket_path();
        let _ = tokio_fs::remove_file(&socket_path).await;

        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request_bytes = Vec::new();
            stream.read_to_end(&mut request_bytes).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let node_ids = query_live_running_node_ids_with_timeout(&layout, Duration::from_millis(50))
            .await
            .unwrap();
        assert_eq!(node_ids, None);

        server.abort();
        let _ = server.await;
        let _ = tokio_fs::remove_file(&socket_path).await;
    }
}
