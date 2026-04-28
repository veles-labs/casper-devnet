use anyhow::{Result, anyhow};
use bip32::{DerivationPath, XPrv};
use blake2::Blake2bVar;
use blake2::digest::{Update, VariableOutput};
use casper_types::account::AccountHash;
use casper_types::{AsymmetricType, PublicKey, SecretKey};
use directories::ProjectDirs;
use flate2::read::GzDecoder;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Cursor, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tar::Archive;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};
use tokio::fs as tokio_fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::task;

use crate::node_launcher::NODE_LAUNCHER_STATE_FILE;

pub const BOOTSTRAP_NODES: u32 = 3;

const DEVNET_BASE_PORT_RPC: u32 = 11000;
const DEVNET_BASE_PORT_REST: u32 = 14000;
const DEVNET_BASE_PORT_SSE: u32 = 18000;
const DEVNET_BASE_PORT_NETWORK: u32 = 22000;
const DEVNET_BASE_PORT_BINARY: u32 = 28000;
const DEVNET_DIAGNOSTICS_PROXY_PORT: u32 = 32000;
const DEVNET_NET_PORT_OFFSET: u32 = 100;

const DEVNET_INITIAL_BALANCE_USER: u128 = 1_000_000_000_000_000_000_000_000_000_000_000_000;
const DEVNET_INITIAL_BALANCE_VALIDATOR: u128 = 1_000_000_000_000_000_000_000_000_000_000_000_000;
const DEVNET_INITIAL_DELEGATION_AMOUNT: u128 = 1_000_000_000_000_000_000;
const DEVNET_VALIDATOR_BASE_WEIGHT: u128 = 1_000_000_000_000_000_000;
const DEVNET_SEED_DOMAIN: &[u8] = b"casper-unsafe-devnet-v1";
const DERIVATION_PATH_PREFIX: &str = "m/44'/506'/0'/0";
const USER_DERIVATION_START: u32 = 100;
const DERIVED_ACCOUNTS_FILE: &str = "derived-accounts.csv";
const SECRET_KEY_PEM: &str = "secret_key.pem";
const MOTE_PER_CSPR: u128 = 1_000_000_000;
const PROGRESS_TICK_MS: u64 = 120;
const CONTROL_SOCKET_NAME_MAX: usize = 80;
const HOOKS_DIR_NAME: &str = "hooks";
const HOOKS_PENDING_DIR_NAME: &str = "pending";
const HOOKS_STATUS_DIR_NAME: &str = "status";
const HOOKS_LOGS_DIR_NAME: &str = "logs";
const HOOKS_WORK_DIR_NAME: &str = "work";
const PRE_GENESIS_HOOK: &str = "pre-genesis";
const POST_GENESIS_HOOK: &str = "post-genesis";
const BLOCK_ADDED_HOOK: &str = "block-added";
const PRE_STAGE_PROTOCOL_HOOK: &str = "pre-stage-protocol";
const POST_STAGE_PROTOCOL_HOOK: &str = "post-stage-protocol";
const PRE_GENESIS_SAMPLE: &str = "pre-genesis.sample";
const POST_GENESIS_SAMPLE: &str = "post-genesis.sample";
const BLOCK_ADDED_SAMPLE: &str = "block-added.sample";
const PRE_STAGE_PROTOCOL_SAMPLE: &str = "pre-stage-protocol.sample";
const POST_STAGE_PROTOCOL_SAMPLE: &str = "post-stage-protocol.sample";
const HOOK_FILE_MODE: u32 = 0o755;
const PRE_GENESIS_SAMPLE_SCRIPT: &str = r#"#!/bin/sh
set -eu

# Copy this file to hooks/pre-genesis to activate it.
# argv: <network_name> <protocol_version>
# cwd:  a dedicated hook work directory under the running network
#       directory (networks/<network>/hooks/work/pre-genesis)

network_name="${1:?missing network name}"
protocol_version="${2:?missing protocol version}"

echo "sample pre-genesis hook: network=${network_name} protocol_version=${protocol_version}"

if config_dirs="$(casper-devnet network "${network_name}" path "${protocol_version}" 2>/dev/null)"; then
  printf '%s\n' "${config_dirs}" | while IFS= read -r config_dir; do
    [ -n "${config_dir}" ] || continue
    echo "staged config dir: ${config_dir}"
  done
fi
"#;
const POST_GENESIS_SAMPLE_SCRIPT: &str = r#"#!/bin/sh
set -eu

# Copy this file to hooks/post-genesis to activate it.
# argv: <network_name> <protocol_version>
# cwd:  a dedicated hook work directory under the running network
#       directory (networks/<network>/hooks/work/post-genesis)

network_name="${1:?missing network name}"
protocol_version="${2:?missing protocol version}"

echo "sample post-genesis hook: network=${network_name} protocol_version=${protocol_version}"

if rpc_url="$(casper-devnet network "${network_name}" port --rpc 2>/dev/null)"; then
  curl -sS "${rpc_url}" \
    -H 'content-type: application/json' \
    --data '{"jsonrpc":"2.0","id":1,"method":"info_get_status","params":[]}' \
    >/dev/null || true
fi
"#;
const BLOCK_ADDED_SAMPLE_SCRIPT: &str = r#"#!/bin/sh
set -eu

# Copy this file to hooks/block-added to activate it.
# argv:  <network_name> <protocol_version>
# stdin: JSON payload for the BlockAdded SSE event
# cwd:   a dedicated hook work directory under the running network
#        directory (networks/<network>/hooks/work/block-added)

network_name="${1:?missing network name}"
protocol_version="${2:?missing protocol version}"
payload="$(cat)"

height="$(printf '%s\n' "${payload}" | sed -n 's/.*"height":[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1)"
if [ -n "${height}" ] && [ $((height % 10)) -ne 0 ]; then
  exit 0
fi

echo "sample block-added hook: network=${network_name} protocol_version=${protocol_version} payload=${payload}"
"#;
const PRE_STAGE_PROTOCOL_SAMPLE_SCRIPT: &str = r#"#!/bin/sh
set -eu

# Copy this file to hooks/pre-stage-protocol to activate it.
# argv: <network_name> <protocol_version> <activation_point>
# cwd:  a dedicated hook work directory under the running network
#       directory (networks/<network>/hooks/work/pre-stage-protocol)

network_name="${1:?missing network name}"
protocol_version="${2:?missing protocol version}"
activation_point="${3:?missing activation point}"

echo "sample pre-stage-protocol hook: network=${network_name} protocol_version=${protocol_version} activation_point=${activation_point}"

if rpc_url="$(casper-devnet network "${network_name}" port --rpc 2>/dev/null)"; then
  curl -sS "${rpc_url}" \
    -H 'content-type: application/json' \
    --data '{"jsonrpc":"2.0","id":1,"method":"info_get_status","params":[]}' \
    >/dev/null || true
fi
"#;
const POST_STAGE_PROTOCOL_SAMPLE_SCRIPT: &str = r#"#!/bin/sh
set -eu

# Copy this file to hooks/post-stage-protocol to activate it.
# argv: <network_name> <protocol_version>
# cwd:  a dedicated hook work directory under the running network
#       directory (networks/<network>/hooks/work/post-stage-protocol)

network_name="${1:?missing network name}"
protocol_version="${2:?missing protocol version}"

echo "sample post-stage-protocol hook: network=${network_name} protocol_version=${protocol_version}"

if rpc_url="$(casper-devnet network "${network_name}" port --rpc 2>/dev/null)"; then
  curl -sS "${rpc_url}" \
    -H 'content-type: application/json' \
    --data '{"jsonrpc":"2.0","id":1,"method":"info_get_status","params":[]}' \
    >/dev/null || true
fi
"#;

#[derive(Debug)]
struct DerivedAccountMaterial {
    path: DerivationPath,
    public_key_hex: String,
    account_hash: String,
    secret_key_pem: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct DerivedPathMaterial {
    pub public_key_hex: String,
    pub account_hash: String,
    pub secret_key_pem: String,
}

#[derive(Debug, Clone)]
struct DerivedAccountInfo {
    kind: &'static str,
    name: String,
    id: u32,
    path: DerivationPath,
    public_key_hex: String,
    account_hash: String,
    balance_motes: u128,
}

#[derive(Debug)]
struct DerivedAccounts {
    nodes: Vec<DerivedAccountInfo>,
    users: Vec<DerivedAccountInfo>,
}

impl DerivedAccountInfo {
    fn line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{}",
            self.kind,
            self.name,
            "secp256k1",
            "bip32",
            self.path,
            self.account_hash,
            format_cspr(self.balance_motes)
        )
    }
}

/// Layout of generated assets for a given network.
#[derive(Clone, Debug)]
pub struct AssetsLayout {
    assets_root: PathBuf,
    network_name: String,
}

impl AssetsLayout {
    /// Create a new layout rooted at `assets_root/<network_name>`.
    pub fn new(assets_root: PathBuf, network_name: String) -> Self {
        Self {
            assets_root,
            network_name,
        }
    }

    /// Root folder for assets (contains all networks).
    pub fn assets_root(&self) -> &Path {
        &self.assets_root
    }

    /// Network name used in paths and configs.
    pub fn network_name(&self) -> &str {
        &self.network_name
    }

    /// Base directory for this network's assets.
    pub fn net_dir(&self) -> PathBuf {
        self.assets_root.join(&self.network_name)
    }

    /// Directory that contains all node folders.
    pub fn nodes_dir(&self) -> PathBuf {
        self.net_dir().join("nodes")
    }

    /// Directory for a single node.
    pub fn node_dir(&self, node_id: u32) -> PathBuf {
        self.nodes_dir().join(format!("node-{}", node_id))
    }

    /// Directory for a node's binaries.
    pub fn node_bin_dir(&self, node_id: u32) -> PathBuf {
        self.node_dir(node_id).join("bin")
    }

    /// Directory for a node's configs.
    pub fn node_config_root(&self, node_id: u32) -> PathBuf {
        self.node_dir(node_id).join("config")
    }

    /// Directory for a node's logs.
    pub fn node_logs_dir(&self, node_id: u32) -> PathBuf {
        self.node_dir(node_id).join("logs")
    }

    /// Directory for network-scoped hooks.
    pub fn hooks_dir(&self) -> PathBuf {
        network_hooks_dir(&self.net_dir())
    }

    /// Directory for pending hook metadata.
    pub fn hooks_pending_dir(&self) -> PathBuf {
        network_hooks_pending_dir(&self.net_dir())
    }

    /// Directory for hook completion state.
    pub fn hooks_status_dir(&self) -> PathBuf {
        network_hooks_status_dir(&self.net_dir())
    }

    /// Directory for hook stdout/stderr logs.
    pub fn hook_logs_dir(&self) -> PathBuf {
        network_hook_logs_dir(&self.net_dir())
    }

    /// Root directory for per-hook working directories.
    pub fn hook_work_root(&self) -> PathBuf {
        network_hook_work_root(&self.net_dir())
    }

    /// Dedicated working directory for a specific hook.
    pub fn hook_work_dir(&self, hook_name: &str) -> PathBuf {
        network_hook_work_dir(&self.net_dir(), hook_name)
    }

    /// Path to the control socket for runtime operations.
    pub fn control_socket_path(&self) -> PathBuf {
        control_socket_path_for_network(&self.network_name)
    }

    /// Returns true if the network's nodes directory exists.
    pub async fn exists(&self) -> bool {
        tokio_fs::metadata(self.nodes_dir())
            .await
            .map(|meta| meta.is_dir())
            .unwrap_or(false)
    }

    /// List node IDs under `nodes/`.
    pub async fn node_ids(&self) -> Result<Vec<u32>> {
        let nodes_dir = self.nodes_dir();
        if !is_dir(&nodes_dir).await {
            return Ok(Vec::new());
        }
        let mut node_ids = Vec::new();
        let mut entries = tokio_fs::read_dir(&nodes_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(node_id) = name
                .strip_prefix("node-")
                .and_then(|suffix| suffix.parse::<u32>().ok())
            {
                node_ids.push(node_id);
            }
        }
        node_ids.sort_unstable();
        Ok(node_ids)
    }

    /// Count node directories under `nodes/`.
    pub async fn count_nodes(&self) -> Result<u32> {
        Ok(self.node_ids().await?.len() as u32)
    }

    /// Find the newest protocol version directory for a node.
    pub async fn latest_protocol_version_dir(&self, node_id: u32) -> Result<String> {
        let bin_dir = self.node_bin_dir(node_id);
        let mut versions: Vec<(Version, String)> = Vec::new();
        let mut entries = tokio_fs::read_dir(&bin_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            let version_str = name.replace('_', ".");
            if let Ok(version) = Version::parse(&version_str) {
                versions.push((version, name));
            }
        }
        versions.sort_by(|a, b| a.0.cmp(&b.0));
        versions
            .last()
            .map(|(_, name)| name.clone())
            .ok_or_else(|| {
                anyhow!(
                    "no protocol version directories found in {}",
                    bin_dir.display()
                )
            })
    }

    /// Return all config.toml paths for a node.
    pub async fn node_config_paths(&self, node_id: u32) -> Result<Vec<PathBuf>> {
        let config_root = self.node_config_root(node_id);
        let mut paths = Vec::new();
        if !is_dir(&config_root).await {
            return Ok(paths);
        }
        let mut entries = tokio_fs::read_dir(&config_root).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let path = entry.path().join("config.toml");
            if is_file(&path).await {
                paths.push(path);
            }
        }
        Ok(paths)
    }
}

fn control_socket_path_for_network(network_name: &str) -> PathBuf {
    let socket_name = format!("{}.socket", normalize_control_socket_name(network_name));
    std::env::temp_dir().join(socket_name)
}

fn normalize_control_socket_name(network_name: &str) -> String {
    let mut normalized = network_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if normalized.is_empty() {
        normalized.push_str("casper-dev");
    }
    if normalized.len() > CONTROL_SOCKET_NAME_MAX {
        normalized.truncate(CONTROL_SOCKET_NAME_MAX);
    }
    normalized
}

pub fn data_dir() -> Result<PathBuf> {
    let project_dirs = ProjectDirs::from("xyz", "veleslabs", "casper-devnet")
        .ok_or_else(|| anyhow!("unable to resolve data directory"))?;
    Ok(project_dirs.data_dir().to_path_buf())
}

pub fn default_assets_root() -> Result<PathBuf> {
    Ok(data_dir()?.join("networks"))
}

pub fn assets_bundle_root() -> Result<PathBuf> {
    Ok(data_dir()?.join("assets"))
}

pub fn custom_assets_root() -> Result<PathBuf> {
    Ok(assets_bundle_root()?.join("custom"))
}

pub async fn custom_asset_path(name: &str) -> Result<PathBuf> {
    validate_custom_asset_name(name)?;
    let asset_dir = custom_assets_root()?.join(name);
    if !is_dir(&asset_dir).await {
        return Err(anyhow!(
            "custom asset '{}' not found at {}",
            name,
            asset_dir.display()
        ));
    }
    Ok(asset_dir)
}

pub fn file_name(path: &Path) -> Option<&OsStr> {
    path.file_name()
}

pub fn sse_endpoint(node_id: u32) -> String {
    format!(
        "http://127.0.0.1:{}/events",
        node_port(DEVNET_BASE_PORT_SSE, node_id)
    )
}

pub fn rest_endpoint(node_id: u32) -> String {
    format!(
        "http://127.0.0.1:{}",
        node_port(DEVNET_BASE_PORT_REST, node_id)
    )
}

pub fn rpc_endpoint(node_id: u32) -> String {
    format!(
        "http://127.0.0.1:{}/rpc",
        node_port(DEVNET_BASE_PORT_RPC, node_id)
    )
}

pub fn binary_address(node_id: u32) -> String {
    format!("127.0.0.1:{}", node_port(DEVNET_BASE_PORT_BINARY, node_id))
}

pub fn network_address(node_id: u32) -> String {
    format!("127.0.0.1:{}", node_port(DEVNET_BASE_PORT_NETWORK, node_id))
}

pub fn diagnostics_proxy_port() -> u32 {
    DEVNET_DIAGNOSTICS_PROXY_PORT
}

pub fn diagnostics_ws_endpoint(node_id: u32) -> String {
    format!(
        "ws://127.0.0.1:{}/diagnostics/node-{}/",
        DEVNET_DIAGNOSTICS_PROXY_PORT, node_id
    )
}

pub fn diagnostics_socket_path(network_name: &str, node_id: u32) -> String {
    let socket_name = format!("{}-{}.sock", network_name, node_id);
    tempfile::env::temp_dir()
        .join(socket_name)
        .to_string_lossy()
        .to_string()
}

pub(crate) fn network_hooks_dir(network_dir: &Path) -> PathBuf {
    network_dir.join(HOOKS_DIR_NAME)
}

pub(crate) fn network_hooks_pending_dir(network_dir: &Path) -> PathBuf {
    network_hooks_dir(network_dir).join(format!(".{HOOKS_PENDING_DIR_NAME}"))
}

pub(crate) fn network_hooks_status_dir(network_dir: &Path) -> PathBuf {
    network_hooks_dir(network_dir).join(format!(".{HOOKS_STATUS_DIR_NAME}"))
}

pub(crate) fn network_hook_logs_dir(network_dir: &Path) -> PathBuf {
    network_hooks_dir(network_dir).join(HOOKS_LOGS_DIR_NAME)
}

pub(crate) fn network_hook_work_root(network_dir: &Path) -> PathBuf {
    network_hooks_dir(network_dir).join(HOOKS_WORK_DIR_NAME)
}

pub(crate) fn network_hook_work_dir(network_dir: &Path, hook_name: &str) -> PathBuf {
    network_hook_work_root(network_dir).join(hook_name)
}

/// Parameters for building a local devnet asset tree.
pub struct SetupOptions {
    pub nodes: u32,
    pub users: Option<u32>,
    pub delay_seconds: u64,
    pub network_name: String,
    pub protocol_version: String,
    pub node_log_format: String,
    pub seed: Arc<str>,
}

pub struct CustomAssetInstallOptions {
    pub name: String,
    pub casper_node: PathBuf,
    pub casper_sidecar: PathBuf,
    pub chainspec: PathBuf,
    pub node_config: PathBuf,
    pub sidecar_config: PathBuf,
}

pub struct StageProtocolOptions {
    pub asset_name: String,
    pub protocol_version: String,
    pub activation_point: u64,
}

#[derive(Debug)]
pub struct StageProtocolResult {
    pub staged_nodes: u32,
}

pub struct AddNodesOptions {
    pub count: u32,
    pub seed: Arc<str>,
}

#[derive(Debug)]
pub struct AddNodesResult {
    pub added_node_ids: Vec<u32>,
    pub total_nodes: u32,
}

#[derive(Clone, Debug)]
struct HookLogPaths {
    stdout: PathBuf,
    stderr: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingPostStageProtocolHook {
    asset_name: String,
    network_name: String,
    protocol_version: String,
    activation_point: u64,
    command_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PendingPostGenesisHook {
    network_name: String,
    protocol_version: String,
    command_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompletedPostStageProtocolHook {
    asset_name: String,
    network_name: String,
    protocol_version: String,
    activation_point: u64,
    command_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    status: HookRunStatus,
    exit_code: Option<i32>,
    error: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    completed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompletedPostGenesisHook {
    network_name: String,
    protocol_version: String,
    command_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    status: HookRunStatus,
    exit_code: Option<i32>,
    error: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    completed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum HookRunStatus {
    Success,
    Failure,
}

pub(crate) enum PendingPostStageProtocolHookResult {
    NotRun,
    Succeeded,
    Failed(String),
}

pub(crate) enum PendingPostGenesisHookResult {
    NotRun,
    Succeeded,
    Failed(String),
}

/// Create or refresh local assets for a devnet.
pub async fn setup_local(layout: &AssetsLayout, opts: &SetupOptions) -> Result<()> {
    let genesis_nodes = opts.nodes;
    if genesis_nodes == 0 {
        return Err(anyhow!("nodes must be greater than 0"));
    }
    let total_nodes = genesis_nodes;
    let users = opts.users.unwrap_or(total_nodes);
    let bundle_root = assets_bundle_root()?;
    let protocol_version = parse_protocol_version(&opts.protocol_version)?;
    let protocol_version_chain = protocol_version.to_string();
    let protocol_version_fs = protocol_version_chain.replace('.', "_");
    let bundle_dir = bundle_dir_for_version(&bundle_root, &protocol_version).await?;

    let chainspec_path = bundle_dir.join("chainspec.toml");
    let config_path = bundle_dir.join("node-config.toml");
    let sidecar_config_path = bundle_dir.join("sidecar-config.toml");

    let net_dir = layout.net_dir();
    tokio_fs::create_dir_all(&net_dir).await?;

    setup_directories(layout, total_nodes, &protocol_version_fs).await?;
    preflight_bundle(&bundle_dir, &chainspec_path, &config_path).await?;
    setup_binaries(layout, total_nodes, &bundle_dir, &protocol_version_fs).await?;

    let derived_accounts =
        setup_seeded_keys(layout, total_nodes, users, Arc::clone(&opts.seed)).await?;

    setup_chainspec(
        layout,
        total_nodes,
        &chainspec_path,
        opts.delay_seconds,
        &protocol_version_chain,
        &opts.network_name,
    )
    .await?;

    setup_accounts(layout, total_nodes, genesis_nodes, users, &derived_accounts).await?;

    setup_node_configs(
        layout,
        total_nodes,
        &protocol_version_fs,
        &config_path,
        &sidecar_config_path,
        &opts.node_log_format,
    )
    .await?;
    ensure_network_hook_samples(layout).await?;

    Ok(())
}

pub async fn install_custom_asset(opts: &CustomAssetInstallOptions) -> Result<()> {
    validate_custom_asset_name(&opts.name)?;

    let node_src = canonicalize_required_file(&opts.casper_node, "casper-node").await?;
    let sidecar_src = canonicalize_required_file(&opts.casper_sidecar, "casper-sidecar").await?;
    let chainspec_src = canonicalize_required_file(&opts.chainspec, "chainspec").await?;
    let node_config_src = canonicalize_required_file(&opts.node_config, "node-config").await?;
    let sidecar_config_src =
        canonicalize_required_file(&opts.sidecar_config, "sidecar-config").await?;

    verify_binary_version(&node_src, "casper-node").await?;
    verify_binary_version(&sidecar_src, "casper-sidecar").await?;

    let root = custom_assets_root()?;
    let asset_dir = root.join(&opts.name);
    if tokio_fs::symlink_metadata(&asset_dir).await.is_ok() {
        return Err(anyhow!(
            "custom asset '{}' already exists at {}",
            opts.name,
            asset_dir.display()
        ));
    }

    tokio_fs::create_dir_all(asset_dir.join("bin")).await?;
    symlink_file(&node_src, &asset_dir.join("bin").join("casper-node")).await?;
    symlink_file(&sidecar_src, &asset_dir.join("bin").join("casper-sidecar")).await?;
    symlink_file(&chainspec_src, &asset_dir.join("chainspec.toml")).await?;
    symlink_file(&node_config_src, &asset_dir.join("node-config.toml")).await?;
    symlink_file(&sidecar_config_src, &asset_dir.join("sidecar-config.toml")).await?;
    create_default_hook_samples(&asset_dir).await?;

    Ok(())
}

pub async fn stage_protocol(
    layout: &AssetsLayout,
    opts: &StageProtocolOptions,
) -> Result<StageProtocolResult> {
    validate_custom_asset_name(&opts.asset_name)?;

    let protocol_version = parse_protocol_version(&opts.protocol_version)?;
    let protocol_version_chain = protocol_version.to_string();
    let protocol_version_fs = protocol_version_chain.replace('.', "_");
    let custom_asset = load_custom_asset(&opts.asset_name).await?;

    verify_binary_version(&custom_asset.casper_node, "casper-node").await?;
    verify_binary_version(&custom_asset.casper_sidecar, "casper-sidecar").await?;

    let total_nodes = layout.count_nodes().await?;
    if total_nodes == 0 {
        return Err(anyhow!(
            "no nodes found under {}; run start --setup-only first",
            layout.nodes_dir().display()
        ));
    }

    let node_log_format = detect_current_node_log_format(layout).await?;
    let accounts_path = layout.net_dir().join("chainspec/accounts.toml");
    run_pre_stage_protocol_hook(
        layout,
        &custom_asset,
        &protocol_version_chain,
        opts.activation_point,
    )
    .await?;

    for node_id in 1..=total_nodes {
        let node_bin_version_dir = layout.node_bin_dir(node_id).join(&protocol_version_fs);
        let node_config_version_dir = layout.node_config_root(node_id).join(&protocol_version_fs);

        if is_dir(&node_bin_version_dir).await {
            tokio_fs::remove_dir_all(&node_bin_version_dir).await?;
        }
        if is_dir(&node_config_version_dir).await {
            tokio_fs::remove_dir_all(&node_config_version_dir).await?;
        }
        tokio_fs::create_dir_all(&node_bin_version_dir).await?;
        tokio_fs::create_dir_all(&node_config_version_dir).await?;

        symlink_file(
            &custom_asset.casper_node,
            &node_bin_version_dir.join("casper-node"),
        )
        .await?;
        symlink_file(
            &custom_asset.casper_sidecar,
            &node_bin_version_dir.join("casper-sidecar"),
        )
        .await?;

        let chainspec_dest = node_config_version_dir.join("chainspec.toml");
        copy_file(&custom_asset.chainspec, &chainspec_dest).await?;
        let staged_chainspec = tokio_fs::read_to_string(&chainspec_dest).await?;
        let network_name = layout.network_name().to_string();
        let activation_point = opts.activation_point as i64;
        let protocol_version_chain = protocol_version_chain.clone();
        let updated_chainspec = spawn_blocking_result(move || {
            update_chainspec_contents(
                &staged_chainspec,
                &protocol_version_chain,
                &activation_point.to_string(),
                false,
                &network_name,
                total_nodes,
            )
        })
        .await?;
        tokio_fs::write(&chainspec_dest, updated_chainspec).await?;

        if is_file(&accounts_path).await {
            copy_file(
                &accounts_path,
                &node_config_version_dir.join("accounts.toml"),
            )
            .await?;
        }

        let node_config_dest = node_config_version_dir.join("config.toml");
        copy_file(&custom_asset.node_config, &node_config_dest).await?;
        let config_contents = tokio_fs::read_to_string(&node_config_dest).await?;
        let bind_address = format!("0.0.0.0:{}", node_port(DEVNET_BASE_PORT_NETWORK, node_id));
        let known = known_addresses(node_id, total_nodes);
        let rest_address = format!("0.0.0.0:{}", node_port(DEVNET_BASE_PORT_REST, node_id));
        let sse_address = format!("0.0.0.0:{}", node_port(DEVNET_BASE_PORT_SSE, node_id));
        let binary_address = format!("0.0.0.0:{}", node_port(DEVNET_BASE_PORT_BINARY, node_id));
        let diagnostics_socket = diagnostics_socket_path(layout.network_name(), node_id);
        let node_log_format = node_log_format.clone();
        let updated_config = spawn_blocking_result(move || {
            let mut config_value: toml::Value = toml::from_str(&config_contents)?;
            set_string(
                &mut config_value,
                &["consensus", "secret_key_path"],
                "../../keys/secret_key.pem".to_string(),
            )?;
            set_string(&mut config_value, &["logging", "format"], node_log_format)?;
            set_string(
                &mut config_value,
                &["network", "bind_address"],
                bind_address,
            )?;
            set_array(&mut config_value, &["network", "known_addresses"], known)?;
            set_string(
                &mut config_value,
                &["storage", "path"],
                "../../storage".to_string(),
            )?;
            set_string(&mut config_value, &["rest_server", "address"], rest_address)?;
            set_string(
                &mut config_value,
                &["event_stream_server", "address"],
                sse_address,
            )?;
            set_string(
                &mut config_value,
                &["diagnostics_port", "socket_path"],
                diagnostics_socket,
            )?;
            set_string(
                &mut config_value,
                &["binary_port_server", "address"],
                binary_address,
            )?;
            set_bool(
                &mut config_value,
                &["binary_port_server", "allow_request_get_trie"],
                true,
            )?;
            set_bool(
                &mut config_value,
                &["binary_port_server", "allow_request_speculative_exec"],
                true,
            )?;
            Ok(toml::to_string(&config_value)?)
        })
        .await?;
        tokio_fs::write(&node_config_dest, updated_config).await?;

        let sidecar_dest = node_config_version_dir.join("sidecar.toml");
        copy_file(&custom_asset.sidecar_config, &sidecar_dest).await?;
        let sidecar_contents = tokio_fs::read_to_string(&sidecar_dest).await?;
        let rpc_port = node_port(DEVNET_BASE_PORT_RPC, node_id) as i64;
        let binary_port = node_port(DEVNET_BASE_PORT_BINARY, node_id) as i64;
        let updated_sidecar = spawn_blocking_result(move || {
            let mut sidecar_value: toml::Value = toml::from_str(&sidecar_contents)?;
            set_string(
                &mut sidecar_value,
                &["rpc_server", "main_server", "ip_address"],
                "0.0.0.0".to_string(),
            )?;
            set_integer(
                &mut sidecar_value,
                &["rpc_server", "main_server", "port"],
                rpc_port,
            )?;
            set_string(
                &mut sidecar_value,
                &["rpc_server", "node_client", "ip_address"],
                "0.0.0.0".to_string(),
            )?;
            set_integer(
                &mut sidecar_value,
                &["rpc_server", "node_client", "port"],
                binary_port,
            )?;
            Ok(toml::to_string(&sidecar_value)?)
        })
        .await?;
        tokio_fs::write(&sidecar_dest, updated_sidecar).await?;
    }

    refresh_post_stage_protocol_hook(layout, &custom_asset, opts, &protocol_version_chain).await?;

    Ok(StageProtocolResult {
        staged_nodes: total_nodes,
    })
}

/// Remove assets for the given network while preserving network hook state.
pub async fn teardown(layout: &AssetsLayout) -> Result<()> {
    let net_dir = layout.net_dir();
    if !is_dir(&net_dir).await {
        return Ok(());
    }

    let hooks_dir = layout.hooks_dir();
    if !is_dir(&hooks_dir).await {
        tokio_fs::remove_dir_all(&net_dir).await?;
        return Ok(());
    }

    let mut entries = tokio_fs::read_dir(&net_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path == hooks_dir {
            continue;
        }
        let file_type = entry.file_type().await?;
        if file_type.is_dir() {
            tokio_fs::remove_dir_all(path).await?;
        } else {
            tokio_fs::remove_file(path).await?;
        }
    }
    Ok(())
}

/// Remove consensus secret keys for the provided node IDs.
pub async fn remove_consensus_keys(layout: &AssetsLayout, node_ids: &[u32]) -> Result<usize> {
    let mut removed = 0;
    for node_id in node_ids {
        let path = layout.node_dir(*node_id).join("keys").join(SECRET_KEY_PEM);
        match tokio_fs::remove_file(&path).await {
            Ok(()) => removed += 1,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(removed)
}

/// Ensure consensus secret keys are present for all nodes.
pub async fn ensure_consensus_keys(layout: &AssetsLayout, seed: Arc<str>) -> Result<usize> {
    let total_nodes = layout.count_nodes().await?;
    if total_nodes == 0 {
        return Ok(0);
    }
    let seed_for_root = seed.to_string();
    let root = spawn_blocking_result(move || unsafe_root_from_seed(&seed_for_root)).await?;
    let mut restored = 0;

    for node_id in 1..=total_nodes {
        let key_path = layout.node_dir(node_id).join("keys").join(SECRET_KEY_PEM);
        if is_file(&key_path).await {
            continue;
        }
        if let Some(parent) = key_path.parent() {
            tokio_fs::create_dir_all(parent).await?;
        }
        let path =
            DerivationPath::from_str(&format!("{}/{}", DERIVATION_PATH_PREFIX, node_id - 1))?;
        let secret_key_pem = spawn_blocking_result({
            let root = root.clone();
            move || {
                let child = derive_xprv_from_path(&root, &path)?;
                let secret_key = SecretKey::secp256k1_from_bytes(child.to_bytes())?;
                Ok(secret_key.to_pem()?)
            }
        })
        .await?;
        tokio_fs::write(&key_path, secret_key_pem).await?;
        restored += 1;
    }

    Ok(restored)
}

/// Prepare filesystem assets for appending managed non-genesis nodes to a live network.
pub async fn add_nodes(layout: &AssetsLayout, opts: &AddNodesOptions) -> Result<AddNodesResult> {
    add_nodes_with_trusted_hash(layout, opts, None).await
}

async fn add_nodes_with_trusted_hash(
    layout: &AssetsLayout,
    opts: &AddNodesOptions,
    trusted_hash_override: Option<String>,
) -> Result<AddNodesResult> {
    if opts.count == 0 {
        return Err(anyhow!("count must be greater than 0"));
    }

    let existing_node_ids = layout.node_ids().await?;
    if existing_node_ids.is_empty() {
        return Err(anyhow!(
            "no nodes found under {}; run start first",
            layout.nodes_dir().display()
        ));
    }
    ensure_contiguous_node_ids(&existing_node_ids)?;

    let active_version = uniform_active_protocol_version(layout, &existing_node_ids).await?;
    let active_version_fs = active_version.to_string().replace('.', "_");
    let first_new_node_id = existing_node_ids
        .last()
        .copied()
        .expect("checked non-empty")
        .checked_add(1)
        .ok_or_else(|| anyhow!("node id overflow"))?;
    let total_nodes = existing_node_ids
        .len()
        .try_into()
        .ok()
        .and_then(|current: u32| current.checked_add(opts.count))
        .ok_or_else(|| anyhow!("node count overflow"))?;
    let added_node_ids = (first_new_node_id..=total_nodes).collect::<Vec<_>>();

    let source_node_id = existing_node_ids[0];
    let source_bin_dir = layout.node_bin_dir(source_node_id).join(&active_version_fs);
    let source_config_dir = layout
        .node_config_root(source_node_id)
        .join(&active_version_fs);
    ensure_active_version_assets(&source_bin_dir, &source_config_dir).await?;

    let trusted_hash = match trusted_hash_override {
        Some(trusted_hash) => trusted_hash,
        None => trusted_hash_for_joining_node(&existing_node_ids).await?,
    };

    for node_id in &added_node_ids {
        if let Err(err) = prepare_added_node(
            layout,
            source_node_id,
            *node_id,
            total_nodes,
            &active_version_fs,
            &opts.seed,
            &trusted_hash,
        )
        .await
        {
            return Err(rollback_added_nodes_after_error(layout, &added_node_ids, err).await);
        }
    }

    if let Err(err) =
        append_added_nodes_to_derived_accounts(layout, &added_node_ids, Arc::clone(&opts.seed))
            .await
    {
        return Err(rollback_added_nodes_after_error(layout, &added_node_ids, err).await);
    }

    Ok(AddNodesResult {
        added_node_ids,
        total_nodes,
    })
}

pub async fn rollback_added_nodes(layout: &AssetsLayout, node_ids: &[u32]) -> Result<()> {
    let mut errors = Vec::new();
    for node_id in node_ids {
        let node_dir = layout.node_dir(*node_id);
        match tokio_fs::remove_dir_all(&node_dir).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => errors.push(format!("failed to remove {}: {}", node_dir.display(), err)),
        }
    }

    if let Err(err) = remove_added_nodes_from_derived_accounts(layout, node_ids).await {
        errors.push(err.to_string());
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(errors.join("; ")))
    }
}

async fn rollback_added_nodes_after_error(
    layout: &AssetsLayout,
    node_ids: &[u32],
    err: anyhow::Error,
) -> anyhow::Error {
    match rollback_added_nodes(layout, node_ids).await {
        Ok(()) => err,
        Err(rollback_err) => anyhow!(
            "{}; failed to roll back added node assets: {}",
            err,
            rollback_err
        ),
    }
}

async fn trusted_hash_for_joining_node(node_ids: &[u32]) -> Result<String> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(StdDuration::from_secs(4))
        .build()?;
    let mut errors = Vec::new();

    for node_id in node_ids {
        let url = format!("{}/status", rest_endpoint(*node_id));
        match fetch_trusted_hash(&client, &url).await {
            Ok(trusted_hash) => return Ok(trusted_hash),
            Err(err) => errors.push(format!("node-{node_id}: {err}")),
        }
    }

    Err(anyhow!(
        "failed to get trusted hash from existing node status endpoints: {}",
        errors.join("; ")
    ))
}

async fn fetch_trusted_hash(client: &reqwest::Client, url: &str) -> Result<String> {
    let status = client
        .get(url)
        .send()
        .await
        .map_err(|err| anyhow!("failed to query {url}: {err}"))?
        .error_for_status()
        .map_err(|err| anyhow!("failed to query {url}: {err}"))?
        .json::<serde_json::Value>()
        .await
        .map_err(|err| anyhow!("failed to parse {url} response: {err}"))?;

    status
        .pointer("/last_added_block_info/hash")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            status
                .pointer("/last_added_block_info/block_hash")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::to_string)
        .ok_or_else(|| anyhow!("{url} response missing last_added_block_info.hash"))
}

fn ensure_contiguous_node_ids(node_ids: &[u32]) -> Result<()> {
    for (index, node_id) in node_ids.iter().enumerate() {
        let expected = (index as u32) + 1;
        if *node_id != expected {
            return Err(anyhow!(
                "node directories must be contiguous from node-1; expected node-{}, found node-{}",
                expected,
                node_id
            ));
        }
    }
    Ok(())
}

async fn uniform_active_protocol_version(
    layout: &AssetsLayout,
    node_ids: &[u32],
) -> Result<Version> {
    let mut active = None;
    for node_id in node_ids {
        let version = active_protocol_version_for_node(layout, *node_id).await?;
        match &active {
            Some(active) if active != &version => {
                return Err(anyhow!(
                    "active protocol versions are mixed: node-1 is {}, node-{} is {}",
                    active,
                    node_id,
                    version
                ));
            }
            Some(_) => {}
            None => active = Some(version),
        }
    }
    active.ok_or_else(|| anyhow!("no active protocol version found"))
}

async fn active_protocol_version_for_node(layout: &AssetsLayout, node_id: u32) -> Result<Version> {
    let state_path = layout
        .node_config_root(node_id)
        .join(NODE_LAUNCHER_STATE_FILE);
    let contents = tokio_fs::read_to_string(&state_path)
        .await
        .map_err(|err| anyhow!("failed to read {}: {}", state_path.display(), err))?;
    let value: toml::Value = toml::from_str(&contents)
        .map_err(|err| anyhow!("failed to parse {}: {}", state_path.display(), err))?;
    let mode = value
        .get("mode")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| anyhow!("{} missing launcher mode", state_path.display()))?;
    if mode != "RunNodeAsValidator" {
        return Err(anyhow!(
            "node-{} is not running as a validator (launcher mode: {}); try again after migration completes",
            node_id,
            mode
        ));
    }
    let version = value
        .get("version")
        .and_then(toml::Value::as_str)
        .ok_or_else(|| anyhow!("{} missing active version", state_path.display()))?;
    parse_protocol_version(version)
}

async fn ensure_active_version_assets(
    source_bin_dir: &Path,
    source_config_dir: &Path,
) -> Result<()> {
    for path in [
        source_bin_dir.join("casper-node"),
        source_bin_dir.join("casper-sidecar"),
        source_config_dir.join("chainspec.toml"),
        source_config_dir.join("accounts.toml"),
        source_config_dir.join("config.toml"),
        source_config_dir.join("sidecar.toml"),
    ] {
        if !is_file(&path).await {
            return Err(anyhow!("missing active version asset {}", path.display()));
        }
    }
    Ok(())
}

async fn prepare_added_node(
    layout: &AssetsLayout,
    source_node_id: u32,
    node_id: u32,
    total_nodes: u32,
    active_version_fs: &str,
    seed: &Arc<str>,
    trusted_hash: &str,
) -> Result<()> {
    let source_bin_dir = layout.node_bin_dir(source_node_id).join(active_version_fs);
    let source_config_dir = layout
        .node_config_root(source_node_id)
        .join(active_version_fs);
    let node_dir = layout.node_dir(node_id);
    let dest_bin_dir = layout.node_bin_dir(node_id).join(active_version_fs);
    let dest_config_dir = layout.node_config_root(node_id).join(active_version_fs);

    tokio_fs::create_dir_all(&dest_bin_dir).await?;
    tokio_fs::create_dir_all(&dest_config_dir).await?;
    tokio_fs::create_dir_all(node_dir.join("keys")).await?;
    tokio_fs::create_dir_all(node_dir.join("logs")).await?;
    tokio_fs::create_dir_all(node_dir.join("storage")).await?;

    copy_file(
        &source_bin_dir.join("casper-node"),
        &dest_bin_dir.join("casper-node"),
    )
    .await?;
    copy_file(
        &source_bin_dir.join("casper-sidecar"),
        &dest_bin_dir.join("casper-sidecar"),
    )
    .await?;
    copy_file(
        &source_config_dir.join("chainspec.toml"),
        &dest_config_dir.join("chainspec.toml"),
    )
    .await?;
    copy_file(
        &source_config_dir.join("accounts.toml"),
        &dest_config_dir.join("accounts.toml"),
    )
    .await?;
    copy_file(
        &source_config_dir.join("config.toml"),
        &dest_config_dir.join("config.toml"),
    )
    .await?;
    copy_file(
        &source_config_dir.join("sidecar.toml"),
        &dest_config_dir.join("sidecar.toml"),
    )
    .await?;

    rewrite_added_node_config(
        layout,
        node_id,
        total_nodes,
        &dest_config_dir.join("config.toml"),
        trusted_hash,
    )
    .await?;
    rewrite_added_sidecar_config(node_id, &dest_config_dir.join("sidecar.toml")).await?;
    write_consensus_key_for_node(layout, node_id, Arc::clone(seed)).await
}

async fn rewrite_added_node_config(
    layout: &AssetsLayout,
    node_id: u32,
    total_nodes: u32,
    config_path: &Path,
    trusted_hash: &str,
) -> Result<()> {
    let config_contents = tokio_fs::read_to_string(config_path).await?;
    let bind_address = format!("0.0.0.0:{}", node_port(DEVNET_BASE_PORT_NETWORK, node_id));
    let known = known_addresses(node_id, total_nodes);
    let rest_address = format!("0.0.0.0:{}", node_port(DEVNET_BASE_PORT_REST, node_id));
    let sse_address = format!("0.0.0.0:{}", node_port(DEVNET_BASE_PORT_SSE, node_id));
    let binary_address = format!("0.0.0.0:{}", node_port(DEVNET_BASE_PORT_BINARY, node_id));
    let diagnostics_socket = diagnostics_socket_path(layout.network_name(), node_id);
    let trusted_hash = trusted_hash.to_string();
    let updated_config = spawn_blocking_result(move || {
        let mut config_value: toml::Value = toml::from_str(&config_contents)?;
        set_string(&mut config_value, &["node", "trusted_hash"], trusted_hash)?;
        set_joining_sync_mode(&mut config_value)?;
        set_string(
            &mut config_value,
            &["consensus", "secret_key_path"],
            "../../keys/secret_key.pem".to_string(),
        )?;
        set_string(
            &mut config_value,
            &["network", "bind_address"],
            bind_address,
        )?;
        set_array(&mut config_value, &["network", "known_addresses"], known)?;
        set_string(
            &mut config_value,
            &["storage", "path"],
            "../../storage".to_string(),
        )?;
        set_string(&mut config_value, &["rest_server", "address"], rest_address)?;
        set_string(
            &mut config_value,
            &["event_stream_server", "address"],
            sse_address,
        )?;
        set_string(
            &mut config_value,
            &["diagnostics_port", "socket_path"],
            diagnostics_socket,
        )?;
        set_string(
            &mut config_value,
            &["binary_port_server", "address"],
            binary_address,
        )?;
        set_bool(
            &mut config_value,
            &["binary_port_server", "allow_request_get_trie"],
            true,
        )?;
        set_bool(
            &mut config_value,
            &["binary_port_server", "allow_request_speculative_exec"],
            true,
        )?;
        Ok(toml::to_string(&config_value)?)
    })
    .await?;
    tokio_fs::write(config_path, updated_config).await?;
    Ok(())
}

async fn rewrite_added_sidecar_config(node_id: u32, sidecar_path: &Path) -> Result<()> {
    let sidecar_contents = tokio_fs::read_to_string(sidecar_path).await?;
    let rpc_port = node_port(DEVNET_BASE_PORT_RPC, node_id) as i64;
    let binary_port = node_port(DEVNET_BASE_PORT_BINARY, node_id) as i64;
    let updated_sidecar = spawn_blocking_result(move || {
        let mut sidecar_value: toml::Value = toml::from_str(&sidecar_contents)?;
        set_string(
            &mut sidecar_value,
            &["rpc_server", "main_server", "ip_address"],
            "0.0.0.0".to_string(),
        )?;
        set_integer(
            &mut sidecar_value,
            &["rpc_server", "main_server", "port"],
            rpc_port,
        )?;
        set_string(
            &mut sidecar_value,
            &["rpc_server", "node_client", "ip_address"],
            "0.0.0.0".to_string(),
        )?;
        set_integer(
            &mut sidecar_value,
            &["rpc_server", "node_client", "port"],
            binary_port,
        )?;
        Ok(toml::to_string(&sidecar_value)?)
    })
    .await?;
    tokio_fs::write(sidecar_path, updated_sidecar).await?;
    Ok(())
}

async fn write_consensus_key_for_node(
    layout: &AssetsLayout,
    node_id: u32,
    seed: Arc<str>,
) -> Result<()> {
    let key_path = layout.node_dir(node_id).join("keys").join(SECRET_KEY_PEM);
    let account = derive_node_account(seed, node_id, true).await?;
    let secret_key_pem = account
        .secret_key_pem
        .ok_or_else(|| anyhow!("missing secret key material for node-{}", node_id))?;
    tokio_fs::write(key_path, secret_key_pem).await?;
    Ok(())
}

async fn append_added_nodes_to_derived_accounts(
    layout: &AssetsLayout,
    node_ids: &[u32],
    seed: Arc<str>,
) -> Result<()> {
    let mut lines = Vec::new();
    for node_id in node_ids {
        let account = derive_node_account(Arc::clone(&seed), *node_id, false).await?;
        lines.push(
            DerivedAccountInfo {
                kind: "node",
                name: format!("node-{}", node_id),
                id: *node_id,
                path: account.path,
                public_key_hex: account.public_key_hex,
                account_hash: account.account_hash,
                balance_motes: 0,
            }
            .line(),
        );
    }

    let path = derived_accounts_path(layout);
    let mut contents = match tokio_fs::read_to_string(&path).await {
        Ok(contents) => {
            let trimmed = contents.trim_end();
            if trimmed.is_empty() {
                "kind,name,key_type,derivation,path,account_hash,balance".to_string()
            } else {
                trimmed.to_string()
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            "kind,name,key_type,derivation,path,account_hash,balance".to_string()
        }
        Err(err) => return Err(err.into()),
    };
    for line in lines {
        contents.push('\n');
        contents.push_str(&line);
    }
    contents.push('\n');
    tokio_fs::write(path, contents).await?;
    Ok(())
}

async fn remove_added_nodes_from_derived_accounts(
    layout: &AssetsLayout,
    node_ids: &[u32],
) -> Result<()> {
    let path = derived_accounts_path(layout);
    let contents = match tokio_fs::read_to_string(&path).await {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    let node_names = node_ids
        .iter()
        .map(|node_id| format!("node-{}", node_id))
        .collect::<Vec<_>>();
    let mut retained = Vec::new();
    for line in contents.lines() {
        if !is_added_node_account_line(line, &node_names) {
            retained.push(line);
        }
    }

    let mut updated = retained.join("\n");
    if !updated.is_empty() {
        updated.push('\n');
    }
    tokio_fs::write(path, updated).await?;
    Ok(())
}

fn is_added_node_account_line(line: &str, node_names: &[String]) -> bool {
    let mut parts = line.splitn(3, ',');
    matches!(
        (parts.next(), parts.next()),
        (Some("node"), Some(name)) if node_names.iter().any(|candidate| candidate == name)
    )
}

pub fn parse_protocol_version(raw: &str) -> Result<Version> {
    let trimmed = raw.trim();
    let normalized = trimmed.strip_prefix('v').unwrap_or(trimmed);
    Version::parse(normalized).map_err(|err| anyhow!("invalid protocol version {}: {}", raw, err))
}

fn bundle_version_dir(bundle_root: &Path, protocol_version: &Version) -> PathBuf {
    bundle_root.join(format!("v{}", protocol_version))
}

async fn bundle_dir_for_version(bundle_root: &Path, protocol_version: &Version) -> Result<PathBuf> {
    let version_dir = bundle_version_dir(bundle_root, protocol_version);
    if is_dir(&version_dir).await {
        return Ok(version_dir);
    }
    Err(anyhow!("assets bundle missing {}", version_dir.display()))
}

pub async fn has_bundle_version(protocol_version: &Version) -> Result<bool> {
    let bundle_root = assets_bundle_root()?;
    Ok(is_dir(&bundle_version_dir(&bundle_root, protocol_version)).await)
}

async fn extract_assets_bundle(bundle_path: &Path, bundle_root: &Path) -> Result<()> {
    if !is_file(bundle_path).await {
        return Err(anyhow!("missing assets bundle {}", bundle_path.display()));
    }

    let bundle_path = bundle_path.to_path_buf();
    let bundle_root = bundle_root.to_path_buf();
    spawn_blocking_result(move || {
        std::fs::create_dir_all(&bundle_root)?;
        let file = File::open(&bundle_path)?;
        let decoder = GzDecoder::new(file);
        let mut archive = Archive::new(decoder);
        println!("unpacking assets into {}", bundle_root.display());
        archive.unpack(&bundle_root)?;
        Ok(())
    })
    .await
}

pub async fn install_assets_bundle(bundle_path: &Path) -> Result<()> {
    let bundle_root = assets_bundle_root()?;
    println!(
        "unpacking local assets bundle {} into {}",
        bundle_path.display(),
        bundle_root.display()
    );
    extract_assets_bundle(bundle_path, &bundle_root).await
}

pub async fn pull_assets_bundles(target_override: Option<&str>, force: bool) -> Result<()> {
    let bundle_root = assets_bundle_root()?;
    let target = target_override
        .map(str::to_string)
        .unwrap_or_else(default_target);
    println!("assets pull target: {}", target);
    let release = fetch_latest_release().await?;
    println!("release tag: {}", release.tag_name);

    let mut assets = Vec::new();
    for asset in release.assets {
        if let Some(version) = parse_release_asset_version(&asset.name, &target) {
            assets.push(ReleaseAsset {
                url: asset.browser_download_url,
                version,
            });
        }
    }

    if assets.is_empty() {
        return Err(anyhow!(
            "no assets found for target {} in release {}",
            target,
            release.tag_name
        ));
    }

    for asset in assets {
        let bytes = download_asset(&asset.url, &asset.version).await?;
        let expected_hash = download_asset_sha512(&asset.url).await?;
        let actual_hash = sha512_hex(&bytes);
        if expected_hash != actual_hash {
            return Err(anyhow!(
                "sha512 mismatch for {} (expected {}, got {})",
                asset.url,
                expected_hash,
                actual_hash
            ));
        }
        let remote_manifest = extract_manifest_from_bytes(&bytes).await?;
        let version_dir = bundle_version_dir(&bundle_root, &asset.version);
        let local_manifest = read_local_manifest(&version_dir).await?;

        if !force
            && let (Some(remote), Some(local)) = (&remote_manifest, &local_manifest)
            && remote == local
        {
            println!("already have this file v{}", asset.version);
            continue;
        }

        if is_dir(&version_dir).await {
            tokio_fs::remove_dir_all(&version_dir).await?;
        }

        println!("saving assets bundle v{}", asset.version);
        unpack_assets_with_progress(&bytes, &bundle_root, &asset.version).await?;
    }

    tokio_fs::write(bundle_root.join("latest"), release.tag_name).await?;
    Ok(())
}

pub async fn most_recent_bundle_version() -> Result<Option<Version>> {
    let mut versions = list_bundle_versions().await?;
    versions.sort();
    Ok(versions.pop())
}

pub async fn list_bundle_versions() -> Result<Vec<Version>> {
    let bundle_root = assets_bundle_root()?;
    if !is_dir(&bundle_root).await {
        return Ok(Vec::new());
    }
    let mut versions: Vec<Version> = Vec::new();
    let mut entries = tokio_fs::read_dir(&bundle_root).await?;
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with('v') {
            continue;
        }
        let dir_version = match parse_protocol_version(&name) {
            Ok(version) => version,
            Err(err) => {
                eprintln!("warning: skipping assets bundle {}: {}", name, err);
                continue;
            }
        };
        let dir_path = entry.path();
        let chainspec_path = dir_path.join("chainspec.toml");
        if !is_file(&chainspec_path).await {
            continue;
        }
        let contents = tokio_fs::read_to_string(&chainspec_path).await?;
        let chainspec_version =
            spawn_blocking_result(move || parse_chainspec_version(&contents)).await?;
        if chainspec_version != dir_version {
            eprintln!(
                "warning: bundle directory {} does not match chainspec protocol version {}; using directory version {}",
                name, chainspec_version, dir_version
            );
        }
        versions.push(dir_version);
    }
    Ok(versions)
}

pub async fn list_custom_asset_names() -> Result<Vec<String>> {
    let root = custom_assets_root()?;
    if !is_dir(&root).await {
        return Ok(Vec::new());
    }

    let mut names = Vec::new();
    let mut entries = tokio_fs::read_dir(&root).await?;
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if validate_custom_asset_name(&name).is_ok() {
            names.push(name);
        }
    }
    names.sort();
    Ok(names)
}

fn parse_chainspec_version(contents: &str) -> Result<Version> {
    let value: toml::Value = toml::from_str(contents)?;
    let protocol = value
        .get("protocol")
        .and_then(|v| v.as_table())
        .ok_or_else(|| anyhow!("chainspec missing [protocol] section"))?;
    let version = protocol
        .get("version")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("chainspec missing protocol.version"))?;
    parse_protocol_version(version)
}

#[derive(Deserialize)]
struct GithubRelease {
    tag_name: String,
    assets: Vec<GithubAsset>,
}

#[derive(Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
}

struct ReleaseAsset {
    url: String,
    version: Version,
}

async fn fetch_latest_release() -> Result<GithubRelease> {
    let client = reqwest::Client::builder()
        .user_agent("casper-devnet")
        .build()?;
    let url = "https://api.github.com/repos/veles-labs/devnet-launcher-assets/releases/latest";
    println!("GET {}", url);
    let response = client.get(url).send().await?.error_for_status()?;
    Ok(response.json::<GithubRelease>().await?)
}

fn parse_release_asset_version(name: &str, target: &str) -> Option<Version> {
    let trimmed = name.strip_prefix("casper-v")?;
    let trimmed = trimmed.strip_suffix(".tar.gz")?;
    let (version, asset_target) = trimmed.split_once('-')?;
    if asset_target != target {
        return None;
    }
    parse_protocol_version(version).ok()
}

fn download_progress_style() -> ProgressStyle {
    ProgressStyle::with_template("{msg} {bar:40.cyan/blue} {bytes:>7}/{total_bytes:7} ({eta})")
        .expect("valid download progress template")
        .progress_chars("█▉▊▋▌▍▎▏ ")
}

fn download_spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{msg} {spinner:.cyan} {bytes:>7}")
        .expect("valid download spinner template")
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
}

fn unpack_spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{msg} {spinner:.magenta} {elapsed_precise}")
        .expect("valid unpack spinner template")
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
}

async fn download_asset(url: &str, version: &Version) -> Result<Vec<u8>> {
    let client = reqwest::Client::builder()
        .user_agent("casper-devnet")
        .build()?;
    println!("GET {}", url);
    let response = client.get(url).send().await?.error_for_status()?;
    let total = response.content_length();
    let pb = match total {
        Some(total) if total > 0 => {
            let pb = ProgressBar::new(total);
            pb.set_style(download_progress_style());
            pb
        }
        _ => {
            let pb = ProgressBar::new_spinner();
            pb.set_style(download_spinner_style());
            pb.enable_steady_tick(StdDuration::from_millis(PROGRESS_TICK_MS));
            pb
        }
    };
    pb.set_message(format!("⬇️  v{} download", version));

    let mut bytes = Vec::new();
    if let Some(total) = total
        && total <= usize::MAX as u64
    {
        bytes.reserve(total as usize);
    }

    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(chunk) => {
                pb.inc(chunk.len() as u64);
                bytes.extend_from_slice(&chunk);
            }
            Err(err) => {
                pb.finish_with_message(format!("❌  v{} download failed", version));
                return Err(err.into());
            }
        }
    }

    pb.finish_with_message(format!("✅  v{} downloaded", version));
    Ok(bytes)
}

async fn download_asset_sha512(url: &str) -> Result<String> {
    let sha_url = format!("{url}.sha512");
    let client = reqwest::Client::builder()
        .user_agent("casper-devnet")
        .build()?;
    println!("GET {}", sha_url);
    let response = client.get(sha_url).send().await?.error_for_status()?;
    let text = response.text().await?;
    parse_sha512(&text)
}

fn parse_sha512(text: &str) -> Result<String> {
    let token = text
        .split_whitespace()
        .next()
        .ok_or_else(|| anyhow!("invalid sha512 file contents"))?;
    let token = token.trim();
    if token.len() != 128 || !token.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(anyhow!("invalid sha512 hash {}", token));
    }
    Ok(token.to_lowercase())
}

fn sha512_hex(bytes: &[u8]) -> String {
    let digest = Sha512::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(&mut out, "{:02x}", byte);
    }
    out
}

async fn extract_manifest_from_bytes(bytes: &[u8]) -> Result<Option<serde_json::Value>> {
    let bytes = bytes.to_vec();
    spawn_blocking_result(move || {
        let cursor = Cursor::new(bytes);
        let decoder = GzDecoder::new(cursor);
        let mut archive = Archive::new(decoder);
        let entries = archive.entries()?;
        for entry in entries {
            let mut entry = entry?;
            let path = entry.path()?;
            if path.file_name() == Some(OsStr::new("manifest.json")) {
                let mut contents = String::new();
                entry.read_to_string(&mut contents)?;
                let value = serde_json::from_str(&contents)?;
                return Ok(Some(value));
            }
        }
        Ok(None)
    })
    .await
}

async fn read_local_manifest(version_dir: &Path) -> Result<Option<serde_json::Value>> {
    let path = version_dir.join("manifest.json");
    if !is_file(&path).await {
        return Ok(None);
    }
    let contents = tokio_fs::read_to_string(&path).await?;
    let value = serde_json::from_str(&contents)?;
    Ok(Some(value))
}

async fn unpack_assets_with_progress(
    bytes: &[u8],
    bundle_root: &Path,
    version: &Version,
) -> Result<()> {
    let pb = ProgressBar::new_spinner();
    pb.set_style(unpack_spinner_style());
    pb.set_message(format!("📦  v{} unpack", version));
    pb.enable_steady_tick(StdDuration::from_millis(PROGRESS_TICK_MS));

    let result = extract_assets_from_bytes(bytes, bundle_root).await;
    match result {
        Ok(()) => pb.finish_with_message(format!("✅  v{} unpacked", version)),
        Err(_) => pb.finish_with_message(format!("❌  v{} unpack failed", version)),
    }
    result
}

async fn extract_assets_from_bytes(bytes: &[u8], bundle_root: &Path) -> Result<()> {
    let bytes = bytes.to_vec();
    let bundle_root = bundle_root.to_path_buf();
    spawn_blocking_result(move || {
        std::fs::create_dir_all(&bundle_root)?;
        let cursor = Cursor::new(bytes);
        let decoder = GzDecoder::new(cursor);
        let mut archive = Archive::new(decoder);
        archive.unpack(&bundle_root)?;
        Ok(())
    })
    .await
}

fn default_target() -> String {
    env!("BUILD_TARGET").to_string()
}

async fn setup_directories(
    layout: &AssetsLayout,
    total_nodes: u32,
    protocol_version_fs: &str,
) -> Result<()> {
    let net_dir = layout.net_dir();
    let chainspec_dir = net_dir.join("chainspec");
    let nodes_dir = net_dir.join("nodes");

    tokio_fs::create_dir_all(chainspec_dir).await?;
    tokio_fs::create_dir_all(&nodes_dir).await?;

    for node_id in 1..=total_nodes {
        let node_dir = layout.node_dir(node_id);
        tokio_fs::create_dir_all(node_dir.join("bin").join(protocol_version_fs)).await?;
        tokio_fs::create_dir_all(node_dir.join("config").join(protocol_version_fs)).await?;
        tokio_fs::create_dir_all(node_dir.join("keys")).await?;
        tokio_fs::create_dir_all(node_dir.join("logs")).await?;
        tokio_fs::create_dir_all(node_dir.join("storage")).await?;
    }

    Ok(())
}

async fn setup_binaries(
    layout: &AssetsLayout,
    total_nodes: u32,
    bundle_dir: &Path,
    protocol_version_fs: &str,
) -> Result<()> {
    let node_bin_src = bundle_dir.join("bin").join("casper-node");
    let sidecar_src = bundle_dir.join("bin").join("casper-sidecar");

    for node_id in 1..=total_nodes {
        let node_bin_dir = layout.node_bin_dir(node_id);
        let version_dir = node_bin_dir.join(protocol_version_fs);

        let node_dest = version_dir.join("casper-node");
        hardlink_file(&node_bin_src, &node_dest).await?;

        let sidecar_dest = version_dir.join("casper-sidecar");
        hardlink_file(&sidecar_src, &sidecar_dest).await?;
    }

    Ok(())
}

async fn preflight_bundle(
    bundle_dir: &Path,
    chainspec_path: &Path,
    config_path: &Path,
) -> Result<()> {
    let mut missing = Vec::new();

    let node_bin = bundle_dir.join("bin").join("casper-node");
    let sidecar_bin = bundle_dir.join("bin").join("casper-sidecar");
    if !is_file(&node_bin).await {
        missing.push(node_bin.clone());
    }
    if !is_file(&sidecar_bin).await {
        missing.push(sidecar_bin.clone());
    }
    if !is_file(chainspec_path).await {
        missing.push(chainspec_path.to_path_buf());
    }
    if !is_file(config_path).await {
        missing.push(config_path.to_path_buf());
    }

    if !missing.is_empty() {
        let message = missing
            .into_iter()
            .map(|path| format!("missing source file {}", path.display()))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(anyhow!(message));
    }

    verify_binary_version(&node_bin, "casper-node").await?;
    verify_binary_version(&sidecar_bin, "casper-sidecar").await?;
    Ok(())
}

async fn verify_binary_version(path: &Path, label: &str) -> Result<()> {
    let output = Command::new(path).arg("--version").output().await?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    Err(anyhow!(
        "{} --version failed (status={}): {}{}",
        label,
        output.status,
        stdout,
        stderr
    ))
}

///
/// Derive an unsafe deterministic root key from an arbitrary seed string.
///
/// DEVNET ONLY, NOT BIP-39, NOT WALLET-COMPATIBLE.
///
fn unsafe_root_from_seed(seed: &str) -> Result<XPrv> {
    if seed.is_empty() {
        return Err(anyhow!("seed must not be empty"));
    }
    let mut hasher = Blake2bVar::new(32).map_err(|_| anyhow!("invalid blake2b output size"))?;
    hasher.update(DEVNET_SEED_DOMAIN);
    hasher.update(seed.as_bytes());

    let mut entropy = [0u8; 32];
    hasher
        .finalize_variable(&mut entropy)
        .map_err(|_| anyhow!("failed to finalize blake2b"))?;

    Ok(XPrv::new(entropy)?)
}

fn derive_xprv_from_path(root: &XPrv, path: &DerivationPath) -> Result<XPrv> {
    let mut key = root.clone();
    for child in path.iter() {
        key = key.derive_child(child)?;
    }
    Ok(key)
}

///
/// Derive a single Casper account from a given root and derivation path.
///
fn derive_account_material(
    root: &XPrv,
    path: &DerivationPath,
    write_secret: bool,
) -> Result<DerivedAccountMaterial> {
    let child = derive_xprv_from_path(root, path)?;
    let secret_key = SecretKey::secp256k1_from_bytes(child.to_bytes())?;
    let public_key = PublicKey::from(&secret_key);
    let public_key_hex = public_key.to_hex();
    let account_hash = AccountHash::from(&public_key).to_hex_string();
    let secret_key_pem = if write_secret {
        Some(secret_key.to_pem()?)
    } else {
        None
    };

    Ok(DerivedAccountMaterial {
        path: path.clone(),
        public_key_hex,
        account_hash,
        secret_key_pem,
    })
}

async fn derive_node_account(
    seed: Arc<str>,
    node_id: u32,
    write_secret: bool,
) -> Result<DerivedAccountMaterial> {
    let seed_for_root = seed.to_string();
    spawn_blocking_result(move || {
        let root = unsafe_root_from_seed(&seed_for_root)?;
        let path =
            DerivationPath::from_str(&format!("{}/{}", DERIVATION_PATH_PREFIX, node_id - 1))?;
        derive_account_material(&root, &path, write_secret)
    })
    .await
}

async fn write_node_keys(dir: &Path, account: &DerivedAccountMaterial) -> Result<()> {
    tokio_fs::create_dir_all(dir).await?;
    if let Some(secret_key_pem) = &account.secret_key_pem {
        tokio_fs::write(dir.join(SECRET_KEY_PEM), secret_key_pem).await?;
    }
    Ok(())
}

async fn setup_seeded_keys(
    layout: &AssetsLayout,
    total_nodes: u32,
    users: u32,
    seed: Arc<str>,
) -> Result<DerivedAccounts> {
    let seed_for_root = seed.to_string();
    let root = spawn_blocking_result(move || unsafe_root_from_seed(&seed_for_root)).await?;
    let mut summary = Vec::new();
    let mut derived = DerivedAccounts {
        nodes: Vec::new(),
        users: Vec::new(),
    };

    for node_id in 1..=total_nodes {
        let path =
            DerivationPath::from_str(&format!("{}/{}", DERIVATION_PATH_PREFIX, node_id - 1))?;
        let account = spawn_blocking_result({
            let root = root.clone();
            let path = path.clone();
            move || derive_account_material(&root, &path, true)
        })
        .await?;
        write_node_keys(&layout.node_dir(node_id).join("keys"), &account).await?;

        let info = DerivedAccountInfo {
            kind: "validator",
            name: format!("node-{}", node_id),
            id: node_id,
            path: account.path.clone(),
            public_key_hex: account.public_key_hex.clone(),
            account_hash: account.account_hash.clone(),
            balance_motes: DEVNET_INITIAL_BALANCE_VALIDATOR,
        };
        summary.push(info.clone());
        derived.nodes.push(info);
    }

    for user_id in 1..=users {
        let path = DerivationPath::from_str(&format!(
            "{}/{}",
            DERIVATION_PATH_PREFIX,
            USER_DERIVATION_START + user_id - 1
        ))?;
        let account = spawn_blocking_result({
            let root = root.clone();
            let path = path.clone();
            move || derive_account_material(&root, &path, false)
        })
        .await?;
        let info = DerivedAccountInfo {
            kind: "user",
            name: format!("user-{}", user_id),
            id: user_id,
            path: account.path.clone(),
            public_key_hex: account.public_key_hex.clone(),
            account_hash: account.account_hash.clone(),
            balance_motes: DEVNET_INITIAL_BALANCE_USER,
        };
        summary.push(info.clone());
        derived.users.push(info);
    }

    write_derived_accounts_summary(layout, &summary).await?;

    Ok(derived)
}

async fn setup_chainspec(
    layout: &AssetsLayout,
    total_nodes: u32,
    chainspec_template: &Path,
    delay_seconds: u64,
    protocol_version_chain: &str,
    network_name: &str,
) -> Result<()> {
    let chainspec_dest = layout.net_dir().join("chainspec/chainspec.toml");
    copy_file(chainspec_template, &chainspec_dest).await?;

    let activation_point = genesis_timestamp(delay_seconds)?;
    let chainspec_contents = tokio_fs::read_to_string(&chainspec_dest).await?;
    let protocol_version_chain = protocol_version_chain.to_string();
    let network_name = network_name.to_string();
    let updated = spawn_blocking_result(move || {
        update_chainspec_contents(
            &chainspec_contents,
            &protocol_version_chain,
            &activation_point,
            true,
            &network_name,
            total_nodes,
        )
    })
    .await?;

    tokio_fs::write(&chainspec_dest, updated).await?;

    Ok(())
}

async fn setup_accounts(
    layout: &AssetsLayout,
    total_nodes: u32,
    genesis_nodes: u32,
    users: u32,
    derived_accounts: &DerivedAccounts,
) -> Result<()> {
    let accounts_path = layout.net_dir().join("chainspec/accounts.toml");
    struct NodeAccount {
        node_id: u32,
        public_key: String,
        is_genesis: bool,
    }

    struct UserAccount {
        user_id: u32,
        public_key: String,
        validator_key: Option<String>,
    }

    if derived_accounts.nodes.len() != total_nodes as usize {
        return Err(anyhow!(
            "expected {} validator accounts, got {}",
            total_nodes,
            derived_accounts.nodes.len()
        ));
    }
    if derived_accounts.users.len() != users as usize {
        return Err(anyhow!(
            "expected {} user accounts, got {}",
            users,
            derived_accounts.users.len()
        ));
    }

    let mut node_accounts = Vec::new();
    let mut user_accounts = Vec::new();

    for node in &derived_accounts.nodes {
        node_accounts.push(NodeAccount {
            node_id: node.id,
            public_key: node.public_key_hex.clone(),
            is_genesis: node.id <= genesis_nodes,
        });
    }

    for user in &derived_accounts.users {
        let validator_key = if user.id <= genesis_nodes {
            Some(
                derived_accounts
                    .nodes
                    .get((user.id - 1) as usize)
                    .map(|node| node.public_key_hex.clone())
                    .ok_or_else(|| anyhow!("missing validator key for node {}", user.id))?,
            )
        } else {
            None
        };
        user_accounts.push(UserAccount {
            user_id: user.id,
            public_key: user.public_key_hex.clone(),
            validator_key,
        });
    }

    let contents = spawn_blocking_result(move || {
        let mut lines = Vec::new();
        for node in node_accounts {
            if node.node_id > 1 {
                lines.push(String::new());
            }
            lines.push(format!("# VALIDATOR {}.", node.node_id));
            lines.push("[[accounts]]".to_string());
            lines.push(format!("public_key = \"{}\"", node.public_key));
            lines.push(format!(
                "balance = \"{}\"",
                DEVNET_INITIAL_BALANCE_VALIDATOR
            ));
            if node.is_genesis {
                lines.push(String::new());
                lines.push("[accounts.validator]".to_string());
                lines.push(format!(
                    "bonded_amount = \"{}\"",
                    validator_weight(node.node_id)
                ));
                lines.push(format!("delegation_rate = {}", node.node_id));
            }
        }

        for user in user_accounts {
            lines.push(String::new());
            lines.push(format!("# USER {}.", user.user_id));
            if let Some(validator_key) = user.validator_key {
                lines.push("[[delegators]]".to_string());
                lines.push(format!("validator_public_key = \"{}\"", validator_key));
                lines.push(format!("delegator_public_key = \"{}\"", user.public_key));
                lines.push(format!("balance = \"{}\"", DEVNET_INITIAL_BALANCE_USER));
                lines.push(format!(
                    "delegated_amount = \"{}\"",
                    DEVNET_INITIAL_DELEGATION_AMOUNT + user.user_id as u128
                ));
            } else {
                lines.push("[[accounts]]".to_string());
                lines.push(format!("public_key = \"{}\"", user.public_key));
                lines.push(format!("balance = \"{}\"", DEVNET_INITIAL_BALANCE_USER));
            }
        }

        Ok(format!("{}\n", lines.join("\n")))
    })
    .await?;
    tokio_fs::write(&accounts_path, contents).await?;

    Ok(())
}

async fn setup_node_configs(
    layout: &AssetsLayout,
    total_nodes: u32,
    protocol_version_fs: &str,
    config_template: &Path,
    sidecar_template: &Path,
    log_format: &str,
) -> Result<()> {
    let chainspec_path = layout.net_dir().join("chainspec/chainspec.toml");
    let accounts_path = layout.net_dir().join("chainspec/accounts.toml");
    let log_format = log_format.to_string();

    for node_id in 1..=total_nodes {
        let config_root = layout.node_config_root(node_id).join(protocol_version_fs);
        tokio_fs::create_dir_all(&config_root).await?;

        copy_file(&chainspec_path, &config_root.join("chainspec.toml")).await?;
        copy_file(&accounts_path, &config_root.join("accounts.toml")).await?;
        copy_file(config_template, &config_root.join("config.toml")).await?;

        let config_contents = tokio_fs::read_to_string(config_root.join("config.toml")).await?;
        let log_format = log_format.clone();
        let bind_address = format!("0.0.0.0:{}", node_port(DEVNET_BASE_PORT_NETWORK, node_id));
        let known = known_addresses(node_id, total_nodes);
        let rest_address = format!("0.0.0.0:{}", node_port(DEVNET_BASE_PORT_REST, node_id));
        let sse_address = format!("0.0.0.0:{}", node_port(DEVNET_BASE_PORT_SSE, node_id));
        let binary_address = format!("0.0.0.0:{}", node_port(DEVNET_BASE_PORT_BINARY, node_id));

        let diagnostics_socket = diagnostics_socket_path(layout.network_name(), node_id);

        let updated_config = spawn_blocking_result(move || {
            let mut config_value: toml::Value = toml::from_str(&config_contents)?;

            set_string(
                &mut config_value,
                &["consensus", "secret_key_path"],
                "../../keys/secret_key.pem".to_string(),
            )?;
            set_string(&mut config_value, &["logging", "format"], log_format)?;
            set_string(
                &mut config_value,
                &["network", "bind_address"],
                bind_address,
            )?;
            set_array(&mut config_value, &["network", "known_addresses"], known)?;
            set_string(
                &mut config_value,
                &["storage", "path"],
                "../../storage".to_string(),
            )?;
            set_string(&mut config_value, &["rest_server", "address"], rest_address)?;
            set_string(
                &mut config_value,
                &["event_stream_server", "address"],
                sse_address,
            )?;

            set_string(
                &mut config_value,
                &["diagnostics_port", "socket_path"],
                diagnostics_socket,
            )?;

            set_string(
                &mut config_value,
                &["binary_port_server", "address"],
                binary_address,
            )?;

            // Enable requests that are disabled by default for security reasons.
            set_bool(
                &mut config_value,
                &["binary_port_server", "allow_request_get_trie"],
                true,
            )?;

            // Enable speculative execution requests.
            set_bool(
                &mut config_value,
                &["binary_port_server", "allow_request_speculative_exec"],
                true,
            )?;

            Ok(toml::to_string(&config_value)?)
        })
        .await?;

        tokio_fs::write(config_root.join("config.toml"), updated_config).await?;

        if is_file(sidecar_template).await {
            let sidecar_path = config_root.join("sidecar.toml");
            copy_file(sidecar_template, &sidecar_path).await?;

            let sidecar_contents = tokio_fs::read_to_string(&sidecar_path).await?;
            let rpc_port = node_port(DEVNET_BASE_PORT_RPC, node_id) as i64;
            let binary_port = node_port(DEVNET_BASE_PORT_BINARY, node_id) as i64;

            let updated_sidecar = spawn_blocking_result(move || {
                let mut sidecar_value: toml::Value = toml::from_str(&sidecar_contents)?;
                set_string(
                    &mut sidecar_value,
                    &["rpc_server", "main_server", "ip_address"],
                    "0.0.0.0".to_string(),
                )?;
                set_integer(
                    &mut sidecar_value,
                    &["rpc_server", "main_server", "port"],
                    rpc_port,
                )?;
                set_string(
                    &mut sidecar_value,
                    &["rpc_server", "node_client", "ip_address"],
                    "0.0.0.0".to_string(),
                )?;
                set_integer(
                    &mut sidecar_value,
                    &["rpc_server", "node_client", "port"],
                    binary_port,
                )?;

                Ok(toml::to_string(&sidecar_value)?)
            })
            .await?;

            tokio_fs::write(&sidecar_path, updated_sidecar).await?;
        }
    }

    Ok(())
}

fn node_port(base: u32, node_id: u32) -> u32 {
    base + DEVNET_NET_PORT_OFFSET + node_id
}

fn bootstrap_address(node_id: u32) -> String {
    format!("127.0.0.1:{}", node_port(DEVNET_BASE_PORT_NETWORK, node_id))
}

fn known_addresses(node_id: u32, total_nodes: u32) -> Vec<String> {
    let bootstrap_nodes = BOOTSTRAP_NODES.min(total_nodes);
    let mut addresses = Vec::new();
    addresses.push(bootstrap_address(1));

    if node_id < bootstrap_nodes {
        for id in 2..=bootstrap_nodes {
            addresses.push(bootstrap_address(id));
        }
    } else {
        let limit = node_id.min(total_nodes);
        for id in 2..=limit {
            addresses.push(bootstrap_address(id));
        }
    }

    addresses
}

fn validator_weight(node_id: u32) -> u128 {
    DEVNET_VALIDATOR_BASE_WEIGHT + node_id as u128
}

fn genesis_timestamp(delay_seconds: u64) -> Result<String> {
    let ts = OffsetDateTime::now_utc() + Duration::seconds(delay_seconds as i64);
    Ok(ts.format(&Rfc3339)?)
}

fn format_cspr(motes: u128) -> String {
    let whole = motes / MOTE_PER_CSPR;
    let rem = motes % MOTE_PER_CSPR;
    if rem == 0 {
        return whole.to_string();
    }
    let frac = format!("{:09}", rem);
    let frac = frac.trim_end_matches('0');
    format!("{}.{}", whole, frac)
}

fn derived_accounts_path(layout: &AssetsLayout) -> PathBuf {
    layout.net_dir().join(DERIVED_ACCOUNTS_FILE)
}

async fn write_derived_accounts_summary(
    layout: &AssetsLayout,
    accounts: &[DerivedAccountInfo],
) -> Result<()> {
    let mut lines = Vec::new();
    lines.push("kind,name,key_type,derivation,path,account_hash,balance".to_string());
    for account in accounts {
        lines.push(account.line());
    }
    tokio_fs::write(derived_accounts_path(layout), lines.join("\n")).await?;
    Ok(())
}

pub async fn derived_accounts_summary(layout: &AssetsLayout) -> Option<String> {
    tokio_fs::read_to_string(derived_accounts_path(layout))
        .await
        .ok()
}

pub(crate) async fn derive_account_from_seed_path(
    seed: Arc<str>,
    path: &str,
) -> Result<DerivedPathMaterial> {
    let seed_for_root = seed.to_string();
    let path_for_parse = path.to_string();
    spawn_blocking_result(move || {
        let root = unsafe_root_from_seed(&seed_for_root)?;
        let path = DerivationPath::from_str(&path_for_parse)?;
        let material = derive_account_material(&root, &path, true)?;
        let secret_key_pem = material
            .secret_key_pem
            .ok_or_else(|| anyhow!("missing secret key material for {}", path_for_parse))?;

        Ok(DerivedPathMaterial {
            public_key_hex: material.public_key_hex,
            account_hash: material.account_hash,
            secret_key_pem,
        })
    })
    .await
}

struct CustomAssetPaths {
    casper_node: PathBuf,
    casper_sidecar: PathBuf,
    chainspec: PathBuf,
    node_config: PathBuf,
    sidecar_config: PathBuf,
    pre_stage_protocol_hook: Option<PathBuf>,
    post_stage_protocol_hook: Option<PathBuf>,
}

pub async fn ensure_network_hook_samples(layout: &AssetsLayout) -> Result<()> {
    let hooks_dir = layout.hooks_dir();
    tokio_fs::create_dir_all(&hooks_dir).await?;
    write_executable_script_if_missing(
        &hooks_dir.join(PRE_GENESIS_SAMPLE),
        PRE_GENESIS_SAMPLE_SCRIPT,
    )
    .await?;
    write_executable_script_if_missing(
        &hooks_dir.join(POST_GENESIS_SAMPLE),
        POST_GENESIS_SAMPLE_SCRIPT,
    )
    .await?;
    write_executable_script_if_missing(
        &hooks_dir.join(BLOCK_ADDED_SAMPLE),
        BLOCK_ADDED_SAMPLE_SCRIPT,
    )
    .await?;
    Ok(())
}

async fn create_default_hook_samples(asset_dir: &Path) -> Result<()> {
    let hooks_dir = asset_dir.join(HOOKS_DIR_NAME);
    tokio_fs::create_dir_all(&hooks_dir).await?;
    write_executable_script_if_missing(
        &hooks_dir.join(PRE_STAGE_PROTOCOL_SAMPLE),
        PRE_STAGE_PROTOCOL_SAMPLE_SCRIPT,
    )
    .await?;
    write_executable_script_if_missing(
        &hooks_dir.join(POST_STAGE_PROTOCOL_SAMPLE),
        POST_STAGE_PROTOCOL_SAMPLE_SCRIPT,
    )
    .await?;
    Ok(())
}

async fn write_executable_script(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio_fs::create_dir_all(parent).await?;
    }
    tokio_fs::write(path, contents).await?;
    tokio_fs::set_permissions(path, std::fs::Permissions::from_mode(HOOK_FILE_MODE)).await?;
    Ok(())
}

async fn write_executable_script_if_missing(path: &Path, contents: &str) -> Result<()> {
    if tokio_fs::symlink_metadata(path).await.is_ok() {
        return Ok(());
    }
    write_executable_script(path, contents).await
}

fn protocol_version_fs(protocol_version: &str) -> String {
    protocol_version.replace('.', "_")
}

fn hook_log_paths(logs_dir: &Path, hook_name: &str, protocol_version: &str) -> HookLogPaths {
    let base = format!("{hook_name}-{}", protocol_version_fs(protocol_version));
    HookLogPaths {
        stdout: logs_dir.join(format!("{base}.stdout.log")),
        stderr: logs_dir.join(format!("{base}.stderr.log")),
    }
}

fn pending_post_stage_protocol_hook_path(hooks_dir: &Path, protocol_version: &str) -> PathBuf {
    hooks_dir
        .join(format!(".{HOOKS_PENDING_DIR_NAME}"))
        .join(format!(
            "{POST_STAGE_PROTOCOL_HOOK}-{}.json",
            protocol_version_fs(protocol_version)
        ))
}

fn post_stage_protocol_claim_path(hooks_dir: &Path, protocol_version: &str) -> PathBuf {
    hooks_dir
        .join(format!(".{HOOKS_STATUS_DIR_NAME}"))
        .join(format!(
            "{POST_STAGE_PROTOCOL_HOOK}-{}.claimed",
            protocol_version_fs(protocol_version)
        ))
}

fn post_stage_protocol_completion_path(hooks_dir: &Path, protocol_version: &str) -> PathBuf {
    hooks_dir
        .join(format!(".{HOOKS_STATUS_DIR_NAME}"))
        .join(format!(
            "{POST_STAGE_PROTOCOL_HOOK}-{}.json",
            protocol_version_fs(protocol_version)
        ))
}

fn pending_post_genesis_hook_path(hooks_dir: &Path, protocol_version: &str) -> PathBuf {
    hooks_dir
        .join(format!(".{HOOKS_PENDING_DIR_NAME}"))
        .join(format!(
            "{POST_GENESIS_HOOK}-{}.json",
            protocol_version_fs(protocol_version)
        ))
}

fn post_genesis_claim_path(hooks_dir: &Path, protocol_version: &str) -> PathBuf {
    hooks_dir
        .join(format!(".{HOOKS_STATUS_DIR_NAME}"))
        .join(format!(
            "{POST_GENESIS_HOOK}-{}.claimed",
            protocol_version_fs(protocol_version)
        ))
}

fn post_genesis_completion_path(hooks_dir: &Path, protocol_version: &str) -> PathBuf {
    hooks_dir
        .join(format!(".{HOOKS_STATUS_DIR_NAME}"))
        .join(format!(
            "{POST_GENESIS_HOOK}-{}.json",
            protocol_version_fs(protocol_version)
        ))
}

pub async fn prepare_genesis_hooks(layout: &AssetsLayout, protocol_version: &str) -> Result<()> {
    let protocol_version = parse_protocol_version(protocol_version)?.to_string();
    ensure_network_hook_samples(layout).await?;
    run_pre_genesis_hook(layout, &protocol_version).await?;
    refresh_post_genesis_hook(layout, &protocol_version).await?;
    Ok(())
}

async fn run_pre_stage_protocol_hook(
    layout: &AssetsLayout,
    custom_asset: &CustomAssetPaths,
    protocol_version: &str,
    activation_point: u64,
) -> Result<()> {
    let Some(command_path) = &custom_asset.pre_stage_protocol_hook else {
        return Ok(());
    };

    let log_paths = hook_log_paths(
        &layout.hook_logs_dir(),
        PRE_STAGE_PROTOCOL_HOOK,
        protocol_version,
    );
    let args = vec![
        layout.network_name().to_string(),
        protocol_version.to_string(),
        activation_point.to_string(),
    ];
    let exit_status = execute_hook_command(
        command_path,
        &args,
        &layout.hook_work_dir(PRE_STAGE_PROTOCOL_HOOK),
        &log_paths,
        None,
        false,
    )
    .await?;
    if !exit_status.success() {
        return Err(anyhow!(
            "{} hook {} exited with status {}",
            PRE_STAGE_PROTOCOL_HOOK,
            command_path.display(),
            exit_status
        ));
    }
    Ok(())
}

async fn lookup_network_hook(layout: &AssetsLayout, hook_name: &str) -> Result<Option<PathBuf>> {
    canonicalize_optional_hook(&layout.hooks_dir().join(hook_name)).await
}

async fn run_pre_genesis_hook(layout: &AssetsLayout, protocol_version: &str) -> Result<()> {
    let Some(command_path) = lookup_network_hook(layout, PRE_GENESIS_HOOK).await? else {
        return Ok(());
    };

    let log_paths = hook_log_paths(&layout.hook_logs_dir(), PRE_GENESIS_HOOK, protocol_version);
    let args = vec![
        layout.network_name().to_string(),
        protocol_version.to_string(),
    ];
    let exit_status = execute_hook_command(
        &command_path,
        &args,
        &layout.hook_work_dir(PRE_GENESIS_HOOK),
        &log_paths,
        None,
        false,
    )
    .await?;
    if !exit_status.success() {
        return Err(anyhow!(
            "{} hook {} exited with status {}",
            PRE_GENESIS_HOOK,
            command_path.display(),
            exit_status
        ));
    }
    Ok(())
}

async fn refresh_post_genesis_hook(layout: &AssetsLayout, protocol_version: &str) -> Result<()> {
    clear_post_genesis_hook_state(layout, protocol_version).await?;

    let Some(command_path) = lookup_network_hook(layout, POST_GENESIS_HOOK).await? else {
        return Ok(());
    };

    tokio_fs::create_dir_all(layout.hooks_pending_dir()).await?;
    tokio_fs::create_dir_all(layout.hooks_status_dir()).await?;
    tokio_fs::create_dir_all(layout.hook_logs_dir()).await?;

    let log_paths = hook_log_paths(&layout.hook_logs_dir(), POST_GENESIS_HOOK, protocol_version);
    let pending = PendingPostGenesisHook {
        network_name: layout.network_name().to_string(),
        protocol_version: protocol_version.to_string(),
        command_path,
        stdout_path: log_paths.stdout,
        stderr_path: log_paths.stderr,
    };
    write_json_atomic(
        &pending_post_genesis_hook_path(&layout.hooks_dir(), protocol_version),
        &pending,
    )
    .await
}

async fn clear_post_genesis_hook_state(
    layout: &AssetsLayout,
    protocol_version: &str,
) -> Result<()> {
    let hooks_dir = layout.hooks_dir();
    remove_file_if_exists(&pending_post_genesis_hook_path(
        &hooks_dir,
        protocol_version,
    ))
    .await?;
    remove_file_if_exists(&post_genesis_claim_path(&hooks_dir, protocol_version)).await?;
    remove_file_if_exists(&post_genesis_completion_path(&hooks_dir, protocol_version)).await?;
    Ok(())
}

async fn refresh_post_stage_protocol_hook(
    layout: &AssetsLayout,
    custom_asset: &CustomAssetPaths,
    opts: &StageProtocolOptions,
    protocol_version: &str,
) -> Result<()> {
    clear_post_stage_protocol_hook_state(layout, protocol_version).await?;

    let Some(command_path) = &custom_asset.post_stage_protocol_hook else {
        return Ok(());
    };

    tokio_fs::create_dir_all(layout.hooks_pending_dir()).await?;
    tokio_fs::create_dir_all(layout.hooks_status_dir()).await?;
    tokio_fs::create_dir_all(layout.hook_logs_dir()).await?;

    let log_paths = hook_log_paths(
        &layout.hook_logs_dir(),
        POST_STAGE_PROTOCOL_HOOK,
        protocol_version,
    );
    let pending = PendingPostStageProtocolHook {
        asset_name: opts.asset_name.clone(),
        network_name: layout.network_name().to_string(),
        protocol_version: protocol_version.to_string(),
        activation_point: opts.activation_point,
        command_path: command_path.clone(),
        stdout_path: log_paths.stdout,
        stderr_path: log_paths.stderr,
    };
    write_json_atomic(
        &pending_post_stage_protocol_hook_path(&layout.hooks_dir(), protocol_version),
        &pending,
    )
    .await
}

async fn clear_post_stage_protocol_hook_state(
    layout: &AssetsLayout,
    protocol_version: &str,
) -> Result<()> {
    let hooks_dir = layout.hooks_dir();
    remove_file_if_exists(&pending_post_stage_protocol_hook_path(
        &hooks_dir,
        protocol_version,
    ))
    .await?;
    remove_file_if_exists(&post_stage_protocol_claim_path(
        &hooks_dir,
        protocol_version,
    ))
    .await?;
    remove_file_if_exists(&post_stage_protocol_completion_path(
        &hooks_dir,
        protocol_version,
    ))
    .await?;
    Ok(())
}

pub(crate) async fn run_pending_post_stage_protocol_hook(
    hooks_dir: &Path,
    network_dir: &Path,
    protocol_version: &Version,
) -> Result<PendingPostStageProtocolHookResult> {
    let protocol_version = protocol_version.to_string();
    let pending_path = pending_post_stage_protocol_hook_path(hooks_dir, &protocol_version);
    if !is_file(&pending_path).await {
        return Ok(PendingPostStageProtocolHookResult::NotRun);
    }

    let claim_path = post_stage_protocol_claim_path(hooks_dir, &protocol_version);
    if !try_create_claim_marker(&claim_path).await? {
        return Ok(PendingPostStageProtocolHookResult::NotRun);
    }

    let pending = read_json_file::<PendingPostStageProtocolHook>(&pending_path).await?;
    let args = vec![
        pending.network_name.clone(),
        pending.protocol_version.clone(),
    ];
    let completion = match execute_hook_command(
        &pending.command_path,
        &args,
        &network_hook_work_dir(network_dir, POST_STAGE_PROTOCOL_HOOK),
        &HookLogPaths {
            stdout: pending.stdout_path.clone(),
            stderr: pending.stderr_path.clone(),
        },
        None,
        false,
    )
    .await
    {
        Ok(exit_status) if exit_status.success() => CompletedPostStageProtocolHook {
            asset_name: pending.asset_name.clone(),
            network_name: pending.network_name.clone(),
            protocol_version: pending.protocol_version.clone(),
            activation_point: pending.activation_point,
            command_path: pending.command_path.clone(),
            stdout_path: pending.stdout_path.clone(),
            stderr_path: pending.stderr_path.clone(),
            status: HookRunStatus::Success,
            exit_code: exit_status.code(),
            error: None,
            completed_at: OffsetDateTime::now_utc(),
        },
        Ok(exit_status) => CompletedPostStageProtocolHook {
            asset_name: pending.asset_name.clone(),
            network_name: pending.network_name.clone(),
            protocol_version: pending.protocol_version.clone(),
            activation_point: pending.activation_point,
            command_path: pending.command_path.clone(),
            stdout_path: pending.stdout_path.clone(),
            stderr_path: pending.stderr_path.clone(),
            status: HookRunStatus::Failure,
            exit_code: exit_status.code(),
            error: Some(format!(
                "{} hook {} exited with status {}",
                POST_STAGE_PROTOCOL_HOOK,
                pending.command_path.display(),
                exit_status
            )),
            completed_at: OffsetDateTime::now_utc(),
        },
        Err(err) => CompletedPostStageProtocolHook {
            asset_name: pending.asset_name.clone(),
            network_name: pending.network_name.clone(),
            protocol_version: pending.protocol_version.clone(),
            activation_point: pending.activation_point,
            command_path: pending.command_path.clone(),
            stdout_path: pending.stdout_path.clone(),
            stderr_path: pending.stderr_path.clone(),
            status: HookRunStatus::Failure,
            exit_code: None,
            error: Some(err.to_string()),
            completed_at: OffsetDateTime::now_utc(),
        },
    };

    write_json_atomic(
        &post_stage_protocol_completion_path(hooks_dir, &protocol_version),
        &completion,
    )
    .await?;
    remove_file_if_exists(&pending_path).await?;
    Ok(match completion.status {
        HookRunStatus::Success => PendingPostStageProtocolHookResult::Succeeded,
        HookRunStatus::Failure => PendingPostStageProtocolHookResult::Failed(
            completion
                .error
                .clone()
                .unwrap_or_else(|| format!("{POST_STAGE_PROTOCOL_HOOK} hook failed")),
        ),
    })
}

pub(crate) async fn run_pending_post_genesis_hook(
    layout: &AssetsLayout,
) -> Result<PendingPostGenesisHookResult> {
    let Some(pending_path) =
        find_pending_hook_path(&layout.hooks_pending_dir(), POST_GENESIS_HOOK).await?
    else {
        return Ok(PendingPostGenesisHookResult::NotRun);
    };

    let pending = read_json_file::<PendingPostGenesisHook>(&pending_path).await?;
    let claim_path = post_genesis_claim_path(&layout.hooks_dir(), &pending.protocol_version);
    if !try_create_claim_marker(&claim_path).await? {
        return Ok(PendingPostGenesisHookResult::NotRun);
    }

    let args = vec![
        pending.network_name.clone(),
        pending.protocol_version.clone(),
    ];
    let completion = match execute_hook_command(
        &pending.command_path,
        &args,
        &layout.hook_work_dir(POST_GENESIS_HOOK),
        &HookLogPaths {
            stdout: pending.stdout_path.clone(),
            stderr: pending.stderr_path.clone(),
        },
        None,
        false,
    )
    .await
    {
        Ok(exit_status) if exit_status.success() => CompletedPostGenesisHook {
            network_name: pending.network_name.clone(),
            protocol_version: pending.protocol_version.clone(),
            command_path: pending.command_path.clone(),
            stdout_path: pending.stdout_path.clone(),
            stderr_path: pending.stderr_path.clone(),
            status: HookRunStatus::Success,
            exit_code: exit_status.code(),
            error: None,
            completed_at: OffsetDateTime::now_utc(),
        },
        Ok(exit_status) => CompletedPostGenesisHook {
            network_name: pending.network_name.clone(),
            protocol_version: pending.protocol_version.clone(),
            command_path: pending.command_path.clone(),
            stdout_path: pending.stdout_path.clone(),
            stderr_path: pending.stderr_path.clone(),
            status: HookRunStatus::Failure,
            exit_code: exit_status.code(),
            error: Some(format!(
                "{} hook {} exited with status {}",
                POST_GENESIS_HOOK,
                pending.command_path.display(),
                exit_status
            )),
            completed_at: OffsetDateTime::now_utc(),
        },
        Err(err) => CompletedPostGenesisHook {
            network_name: pending.network_name.clone(),
            protocol_version: pending.protocol_version.clone(),
            command_path: pending.command_path.clone(),
            stdout_path: pending.stdout_path.clone(),
            stderr_path: pending.stderr_path.clone(),
            status: HookRunStatus::Failure,
            exit_code: None,
            error: Some(err.to_string()),
            completed_at: OffsetDateTime::now_utc(),
        },
    };

    write_json_atomic(
        &post_genesis_completion_path(&layout.hooks_dir(), &pending.protocol_version),
        &completion,
    )
    .await?;
    remove_file_if_exists(&pending_path).await?;
    Ok(match completion.status {
        HookRunStatus::Success => PendingPostGenesisHookResult::Succeeded,
        HookRunStatus::Failure => PendingPostGenesisHookResult::Failed(
            completion
                .error
                .clone()
                .unwrap_or_else(|| format!("{POST_GENESIS_HOOK} hook failed")),
        ),
    })
}

pub fn spawn_pending_post_genesis_hook(layout: AssetsLayout) {
    tokio::spawn(async move {
        match run_pending_post_genesis_hook(&layout).await {
            Ok(PendingPostGenesisHookResult::NotRun | PendingPostGenesisHookResult::Succeeded) => {}
            Ok(PendingPostGenesisHookResult::Failed(err)) => {
                eprintln!("warning: {}", err);
            }
            Err(err) => {
                eprintln!("warning: failed to run {POST_GENESIS_HOOK} hook: {}", err);
            }
        }
    });
}

pub fn spawn_block_added_hook(
    layout: AssetsLayout,
    protocol_version: String,
    payload: serde_json::Value,
) {
    tokio::spawn(async move {
        if let Err(err) = run_block_added_hook(&layout, &protocol_version, payload).await {
            eprintln!("warning: failed to run {BLOCK_ADDED_HOOK} hook: {}", err);
        }
    });
}

async fn run_block_added_hook(
    layout: &AssetsLayout,
    protocol_version: &str,
    payload: serde_json::Value,
) -> Result<()> {
    let Some(command_path) = lookup_network_hook(layout, BLOCK_ADDED_HOOK).await? else {
        return Ok(());
    };

    let log_paths = hook_log_paths(&layout.hook_logs_dir(), BLOCK_ADDED_HOOK, protocol_version);
    let args = vec![
        layout.network_name().to_string(),
        protocol_version.to_string(),
    ];
    let stdin = hook_payload_stdin(&payload)?;
    let exit_status = execute_hook_command(
        &command_path,
        &args,
        &layout.hook_work_dir(BLOCK_ADDED_HOOK),
        &log_paths,
        Some(&stdin),
        true,
    )
    .await?;
    if !exit_status.success() {
        return Err(anyhow!(
            "{} hook {} exited with status {}",
            BLOCK_ADDED_HOOK,
            command_path.display(),
            exit_status
        ));
    }
    Ok(())
}

async fn execute_hook_command(
    command_path: &Path,
    args: &[String],
    cwd: &Path,
    log_paths: &HookLogPaths,
    stdin: Option<&[u8]>,
    append_logs: bool,
) -> Result<std::process::ExitStatus> {
    ensure_executable_hook(command_path).await?;
    tokio_fs::create_dir_all(cwd).await?;
    let hook_name = command_path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or("hook")
        .to_string();
    let stdout = open_hook_log_file(&log_paths.stdout, append_logs).await?;
    let stderr = open_hook_log_file(&log_paths.stderr, append_logs).await?;
    let mut command = Command::new(command_path);
    command
        .args(args)
        .current_dir(cwd)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    if let Some(stdin) = stdin {
        let mut child_stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to capture stdin for {}", command_path.display()))?;
        child_stdin.write_all(stdin).await?;
        child_stdin.shutdown().await?;
    }
    let child_stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("failed to capture stdout for {}", command_path.display()))?;
    let child_stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("failed to capture stderr for {}", command_path.display()))?;

    let stdout_task = tokio::spawn(stream_hook_output(
        hook_name.clone(),
        "stdout",
        BufReader::new(child_stdout),
        stdout,
    ));
    let stderr_task = tokio::spawn(stream_hook_output(
        hook_name.clone(),
        "stderr",
        BufReader::new(child_stderr),
        stderr,
    ));

    let exit_status = child.wait().await?;
    stdout_task
        .await
        .map_err(|err| anyhow!("failed to join {} stdout task: {}", hook_name, err))??;
    stderr_task
        .await
        .map_err(|err| anyhow!("failed to join {} stderr task: {}", hook_name, err))??;
    if let Some(message) = hook_finished_message(&hook_name, &exit_status) {
        eprintln!("{}", message);
    }
    Ok(exit_status)
}

async fn open_hook_log_file(path: &Path, append: bool) -> Result<tokio_fs::File> {
    if let Some(parent) = path.parent() {
        tokio_fs::create_dir_all(parent).await?;
    }
    let mut options = tokio_fs::OpenOptions::new();
    options.create(true).write(true);
    if append {
        options.append(true);
    } else {
        options.truncate(true);
    }
    options.open(path).await.map_err(Into::into)
}

async fn stream_hook_output<R>(
    hook_name: String,
    stream_name: &'static str,
    mut reader: BufReader<R>,
    mut log_file: tokio_fs::File,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    loop {
        buf.clear();
        let bytes_read = reader.read_until(b'\n', &mut buf).await?;
        if bytes_read == 0 {
            break;
        }

        log_file.write_all(&buf).await?;
        let line = normalize_hook_output_line(&buf);
        eprintln!("{hook_name} {stream_name}: {line}");
    }
    log_file.flush().await?;
    Ok(())
}

fn normalize_hook_output_line(bytes: &[u8]) -> String {
    let mut line = String::from_utf8_lossy(bytes).into_owned();
    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }
    line
}

fn hook_finished_message(hook_name: &str, status: &std::process::ExitStatus) -> Option<String> {
    match status.code() {
        Some(0) => None,
        Some(code) => Some(format!("{hook_name} finished with exit code {code}")),
        None => Some(format!("{hook_name} finished with status {status}")),
    }
}

fn hook_payload_stdin(payload: &serde_json::Value) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(payload)?;
    bytes.push(b'\n');
    Ok(bytes)
}

async fn ensure_executable_hook(path: &Path) -> Result<()> {
    let metadata = tokio_fs::metadata(path).await?;
    if !metadata.is_file() {
        return Err(anyhow!("hook path {} is not a file", path.display()));
    }
    if metadata.permissions().mode() & 0o111 == 0 {
        return Err(anyhow!("hook path {} is not executable", path.display()));
    }
    Ok(())
}

async fn try_create_claim_marker(path: &Path) -> Result<bool> {
    if let Some(parent) = path.parent() {
        tokio_fs::create_dir_all(parent).await?;
    }
    match tokio_fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .await
    {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(err) => Err(err.into()),
    }
}

async fn find_pending_hook_path(dir: &Path, hook_name: &str) -> Result<Option<PathBuf>> {
    if !is_dir(dir).await {
        return Ok(None);
    }

    let prefix = format!("{hook_name}-");
    let mut matches = Vec::new();
    let mut entries = tokio_fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name.starts_with(&prefix) && file_name.ends_with(".json") {
            matches.push(entry.path());
        }
    }
    matches.sort();
    Ok(matches.into_iter().next())
}

async fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    if let Some(parent) = path.parent() {
        tokio_fs::create_dir_all(parent).await?;
    }
    let tmp_path = path.with_extension(format!(
        "{}.tmp-{}",
        path.extension().and_then(OsStr::to_str).unwrap_or("json"),
        std::process::id()
    ));
    tokio_fs::write(&tmp_path, bytes).await?;
    tokio_fs::rename(&tmp_path, path).await?;
    Ok(())
}

async fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let contents = tokio_fs::read(path).await?;
    Ok(serde_json::from_slice(&contents)?)
}

async fn remove_file_if_exists(path: &Path) -> Result<()> {
    match tokio_fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

async fn detect_current_node_log_format(layout: &AssetsLayout) -> Result<String> {
    let version_dir = match layout.latest_protocol_version_dir(1).await {
        Ok(version_dir) => version_dir,
        Err(_) => return Ok("json".to_string()),
    };
    let config_path = layout
        .node_config_root(1)
        .join(version_dir)
        .join("config.toml");
    if !is_file(&config_path).await {
        return Ok("json".to_string());
    }

    let config_contents = tokio_fs::read_to_string(config_path).await?;
    spawn_blocking_result(move || {
        let value: toml::Value = toml::from_str(&config_contents)?;
        let format = value
            .get("logging")
            .and_then(|logging| logging.get("format"))
            .and_then(|format| format.as_str())
            .unwrap_or("json")
            .to_string();
        Ok(format)
    })
    .await
}

async fn load_custom_asset(name: &str) -> Result<CustomAssetPaths> {
    let asset_dir = custom_asset_path(name).await?;

    let casper_node =
        canonicalize_required_file(&asset_dir.join("bin").join("casper-node"), "casper-node")
            .await?;
    let casper_sidecar = canonicalize_required_file(
        &asset_dir.join("bin").join("casper-sidecar"),
        "casper-sidecar",
    )
    .await?;
    let chainspec =
        canonicalize_required_file(&asset_dir.join("chainspec.toml"), "chainspec").await?;
    let node_config =
        canonicalize_required_file(&asset_dir.join("node-config.toml"), "node-config").await?;
    let sidecar_config =
        canonicalize_required_file(&asset_dir.join("sidecar-config.toml"), "sidecar-config")
            .await?;
    let pre_stage_protocol_hook =
        canonicalize_optional_hook(&asset_dir.join(HOOKS_DIR_NAME).join(PRE_STAGE_PROTOCOL_HOOK))
            .await?;
    let post_stage_protocol_hook = canonicalize_optional_hook(
        &asset_dir
            .join(HOOKS_DIR_NAME)
            .join(POST_STAGE_PROTOCOL_HOOK),
    )
    .await?;

    Ok(CustomAssetPaths {
        casper_node,
        casper_sidecar,
        chainspec,
        node_config,
        sidecar_config,
        pre_stage_protocol_hook,
        post_stage_protocol_hook,
    })
}

async fn canonicalize_optional_hook(path: &Path) -> Result<Option<PathBuf>> {
    match tokio_fs::symlink_metadata(path).await {
        Ok(metadata) => {
            if metadata.is_dir() || !is_file(path).await {
                return Err(anyhow!("hook path {} is not a file", path.display()));
            }
            Ok(Some(tokio_fs::canonicalize(path).await?))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

async fn canonicalize_required_file(path: &Path, label: &str) -> Result<PathBuf> {
    if !is_file(path).await {
        return Err(anyhow!("missing {} file {}", label, path.display()));
    }
    let canonical = tokio_fs::canonicalize(path).await?;
    if !is_file(&canonical).await {
        return Err(anyhow!("missing {} file {}", label, canonical.display()));
    }
    Ok(canonical)
}

fn validate_custom_asset_name(name: &str) -> Result<()> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("custom asset name must not be empty"));
    }
    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains("..") {
        return Err(anyhow!(
            "custom asset name '{}' contains forbidden path characters",
            name
        ));
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.')
    {
        return Err(anyhow!(
            "custom asset name '{}' must use [A-Za-z0-9._-]",
            name
        ));
    }
    Ok(())
}

async fn copy_file(src: &Path, dest: &Path) -> Result<()> {
    if !is_file(src).await {
        return Err(anyhow!("missing source file {}", src.display()));
    }
    if let Some(parent) = dest.parent() {
        tokio_fs::create_dir_all(parent).await?;
    }
    tokio_fs::copy(src, dest).await?;
    Ok(())
}

async fn symlink_file(src: &Path, dest: &Path) -> Result<()> {
    if !is_file(src).await {
        return Err(anyhow!("missing source file {}", src.display()));
    }
    if let Some(parent) = dest.parent() {
        tokio_fs::create_dir_all(parent).await?;
    }
    if let Ok(metadata) = tokio_fs::symlink_metadata(dest).await {
        if metadata.is_dir() {
            tokio_fs::remove_dir_all(dest).await?;
        } else {
            tokio_fs::remove_file(dest).await?;
        }
    }
    tokio_fs::symlink(src, dest).await?;
    Ok(())
}

async fn hardlink_file(src: &Path, dest: &Path) -> Result<()> {
    if !is_file(src).await {
        return Err(anyhow!("missing source file {}", src.display()));
    }
    if let Some(parent) = dest.parent() {
        tokio_fs::create_dir_all(parent).await?;
    }
    if let Ok(metadata) = tokio_fs::symlink_metadata(dest).await {
        if metadata.is_dir() {
            return Err(anyhow!("destination {} is a directory", dest.display()));
        }
        tokio_fs::remove_file(dest).await?;
    }
    tokio_fs::hard_link(src, dest).await?;
    Ok(())
}

async fn is_dir(path: &Path) -> bool {
    tokio_fs::metadata(path)
        .await
        .map(|meta| meta.is_dir())
        .unwrap_or(false)
}

async fn is_file(path: &Path) -> bool {
    tokio_fs::metadata(path)
        .await
        .map(|meta| meta.is_file())
        .unwrap_or(false)
}

async fn spawn_blocking_result<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(err) => Err(anyhow!("blocking task failed: {}", err)),
    }
}

fn update_chainspec_contents(
    contents: &str,
    protocol_version: &str,
    activation_point: &str,
    activation_is_string: bool,
    network_name: &str,
    total_nodes: u32,
) -> Result<String> {
    let activation_value = if activation_is_string {
        format!("'{activation_point}'")
    } else {
        activation_point.to_string()
    };
    let updated =
        replace_toml_section_value(contents, "protocol", "activation_point", &activation_value)?;
    let updated = replace_toml_section_value(
        &updated,
        "protocol",
        "version",
        &format!("'{protocol_version}'"),
    )?;
    let updated =
        replace_toml_section_value(&updated, "network", "name", &format!("'{network_name}'"))?;
    replace_toml_section_value(
        &updated,
        "core",
        "validator_slots",
        &total_nodes.to_string(),
    )
}

fn replace_toml_section_value(
    contents: &str,
    section: &str,
    key: &str,
    value: &str,
) -> Result<String> {
    let mut lines = Vec::new();
    let mut current_section = String::new();
    let mut replaced = false;

    for line in contents.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            current_section = trimmed
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_string();
            lines.push(line.to_string());
            continue;
        }

        if current_section == section && trimmed.starts_with(&format!("{key} =")) {
            let indent = &line[..line.len() - trimmed.len()];
            lines.push(format!("{indent}{key} = {value}"));
            replaced = true;
            continue;
        }

        lines.push(line.to_string());
    }

    if !replaced {
        return Err(anyhow!("missing {}.{} in chainspec", section, key));
    }

    let mut output = lines.join("\n");
    if contents.ends_with('\n') {
        output.push('\n');
    }
    Ok(output)
}

fn set_string(root: &mut toml::Value, path: &[&str], value: String) -> Result<()> {
    set_value(root, path, toml::Value::String(value))
}

fn set_integer(root: &mut toml::Value, path: &[&str], value: i64) -> Result<()> {
    set_value(root, path, toml::Value::Integer(value))
}

fn set_array(root: &mut toml::Value, path: &[&str], values: Vec<String>) -> Result<()> {
    let array = values.into_iter().map(toml::Value::String).collect();
    set_value(root, path, toml::Value::Array(array))
}

fn set_bool(root: &mut toml::Value, path: &[&str], value: bool) -> Result<()> {
    set_value(root, path, toml::Value::Boolean(value))
}

fn set_joining_sync_mode(root: &mut toml::Value) -> Result<()> {
    let table = root
        .as_table()
        .ok_or_else(|| anyhow!("TOML root is not a table"))?;
    let node = table.get("node").and_then(toml::Value::as_table);

    if node
        .map(|node| node.contains_key("sync_to_genesis"))
        .unwrap_or(false)
    {
        set_bool(root, &["node", "sync_to_genesis"], false)
    } else if table.contains_key("sync_to_genesis") {
        set_bool(root, &["sync_to_genesis"], false)
    } else {
        set_string(root, &["node", "sync_handling"], "ttl".to_string())
    }
}

fn set_value(root: &mut toml::Value, path: &[&str], value: toml::Value) -> Result<()> {
    let table = root
        .as_table_mut()
        .ok_or_else(|| anyhow!("TOML root is not a table"))?;

    let mut current = table;
    for key in &path[..path.len() - 1] {
        current = ensure_table(current, key);
    }
    current.insert(path[path.len() - 1].to_string(), value);
    Ok(())
}

fn ensure_table<'a>(table: &'a mut toml::value::Table, key: &str) -> &'a mut toml::value::Table {
    if !table.contains_key(key) {
        table.insert(
            key.to_string(),
            toml::Value::Table(toml::value::Table::new()),
        );
    }
    table
        .get_mut(key)
        .and_then(|v| v.as_table_mut())
        .expect("table entry is not a table")
}

#[cfg(test)]
pub(crate) fn test_env_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::env;
    use std::ffi::OsString;
    use std::os::unix::process::ExitStatusExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, MutexGuard};
    use tempfile::TempDir;

    struct TestDataEnv {
        _lock: MutexGuard<'static, ()>,
        temp_dir: TempDir,
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

        fn root(&self) -> &Path {
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

    async fn create_fake_binary(path: &Path, label: &str) -> Result<()> {
        write_executable_script(
            path,
            &format!(
                "#!/bin/sh\nset -eu\nif [ \"${{1:-}}\" = \"--version\" ]; then\n  echo \"{label} 1.0.0\"\n  exit 0\nfi\nexit 0\n"
            ),
        )
        .await
    }

    async fn create_test_custom_asset_sources(
        root: &Path,
        name: &str,
    ) -> Result<CustomAssetInstallOptions> {
        let source_dir = root.join("sources").join(name);
        tokio_fs::create_dir_all(&source_dir).await?;
        let node_path = source_dir.join("casper-node");
        let sidecar_path = source_dir.join("casper-sidecar");
        create_fake_binary(&node_path, "casper-node").await?;
        create_fake_binary(&sidecar_path, "casper-sidecar").await?;
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
        .await?;
        tokio_fs::write(&node_config, "").await?;
        tokio_fs::write(&sidecar_config, "").await?;
        Ok(CustomAssetInstallOptions {
            name: name.to_string(),
            casper_node: node_path,
            casper_sidecar: sidecar_path,
            chainspec,
            node_config,
            sidecar_config,
        })
    }

    async fn install_test_custom_asset(env: &TestDataEnv, name: &str) -> Result<PathBuf> {
        let options = create_test_custom_asset_sources(env.root(), name).await?;
        install_custom_asset(&options).await?;
        custom_asset_path(name).await
    }

    async fn create_test_network_layout(
        root: &Path,
        network_name: &str,
        current_version: &str,
    ) -> Result<AssetsLayout> {
        let layout = AssetsLayout::new(root.join("networks"), network_name.to_string());
        let version_fs = protocol_version_fs(current_version);
        tokio_fs::create_dir_all(layout.node_bin_dir(1).join(&version_fs)).await?;
        tokio_fs::create_dir_all(layout.node_config_root(1).join(&version_fs)).await?;
        tokio_fs::create_dir_all(layout.node_dir(1).join("logs")).await?;
        tokio_fs::create_dir_all(layout.node_dir(1).join("storage")).await?;
        tokio_fs::create_dir_all(layout.node_dir(1).join("keys")).await?;
        tokio_fs::create_dir_all(layout.net_dir().join("chainspec")).await?;
        tokio_fs::write(
            layout
                .node_config_root(1)
                .join(&version_fs)
                .join("config.toml"),
            "[logging]\nformat = \"text\"\n",
        )
        .await?;
        tokio_fs::write(layout.net_dir().join("chainspec/accounts.toml"), "").await?;
        Ok(layout)
    }

    async fn create_add_nodes_network_layout(
        root: &Path,
        network_name: &str,
        current_version: &str,
        node_count: u32,
    ) -> Result<AssetsLayout> {
        let layout = AssetsLayout::new(root.join("networks"), network_name.to_string());
        let version_fs = protocol_version_fs(current_version);
        tokio_fs::create_dir_all(layout.net_dir().join("chainspec")).await?;
        tokio_fs::write(
            layout.net_dir().join("chainspec/accounts.toml"),
            "network accounts\n",
        )
        .await?;
        tokio_fs::write(
            derived_accounts_path(&layout),
            "kind,name,key_type,derivation,path,account_hash,balance\n",
        )
        .await?;

        for node_id in 1..=node_count {
            let bin_dir = layout.node_bin_dir(node_id).join(&version_fs);
            let config_dir = layout.node_config_root(node_id).join(&version_fs);
            tokio_fs::create_dir_all(&bin_dir).await?;
            tokio_fs::create_dir_all(&config_dir).await?;
            tokio_fs::create_dir_all(layout.node_dir(node_id).join("keys")).await?;
            tokio_fs::create_dir_all(layout.node_dir(node_id).join("logs")).await?;
            tokio_fs::create_dir_all(layout.node_dir(node_id).join("storage")).await?;
            tokio_fs::write(bin_dir.join("casper-node"), "node binary\n").await?;
            tokio_fs::write(bin_dir.join("casper-sidecar"), "sidecar binary\n").await?;
            tokio_fs::write(
                config_dir.join("chainspec.toml"),
                format!("chainspec for node {node_id}\n"),
            )
            .await?;
            tokio_fs::write(config_dir.join("accounts.toml"), "accounts snapshot\n").await?;
            tokio_fs::write(
                config_dir.join("config.toml"),
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
            .await?;
            tokio_fs::write(
                config_dir.join("sidecar.toml"),
                "\
[rpc_server.main_server]
ip_address = \"127.0.0.1\"
port = 1

[rpc_server.node_client]
ip_address = \"127.0.0.1\"
port = 2
",
            )
            .await?;
            tokio_fs::write(
                layout.node_config_root(node_id).join(NODE_LAUNCHER_STATE_FILE),
                format!(
                    "mode = \"RunNodeAsValidator\"\nversion = \"{}\"\nbinary_path = \"{}\"\nconfig_path = \"{}\"\n",
                    current_version,
                    bin_dir.join("casper-node").display(),
                    config_dir.join("config.toml").display(),
                ),
            )
            .await?;
        }

        Ok(layout)
    }

    fn stage_options(asset_name: &str, protocol_version: &str) -> StageProtocolOptions {
        StageProtocolOptions {
            asset_name: asset_name.to_string(),
            protocol_version: protocol_version.to_string(),
            activation_point: 123,
        }
    }

    fn add_nodes_options(count: u32) -> AddNodesOptions {
        AddNodesOptions {
            count,
            seed: Arc::from("default"),
        }
    }

    async fn add_nodes_for_test(
        layout: &AssetsLayout,
        opts: &AddNodesOptions,
    ) -> Result<AddNodesResult> {
        add_nodes_with_trusted_hash(layout, opts, Some("trusted-hash".to_string())).await
    }

    #[test]
    fn format_cspr_handles_whole_and_fractional() {
        assert_eq!(format_cspr(0), "0");
        assert_eq!(format_cspr(1), "0.000000001");
        assert_eq!(format_cspr(1_000_000_000), "1");
        assert_eq!(format_cspr(1_000_000_001), "1.000000001");
        assert_eq!(format_cspr(123_000_000_000), "123");
        assert_eq!(format_cspr(123_000_000_456), "123.000000456");
    }

    #[test]
    fn custom_asset_name_validation() {
        assert!(validate_custom_asset_name("dev").is_ok());
        assert!(validate_custom_asset_name("v2_2_0-local").is_ok());
        assert!(validate_custom_asset_name("").is_err());
        assert!(validate_custom_asset_name("../bad").is_err());
        assert!(validate_custom_asset_name("with space").is_err());
    }

    #[test]
    fn control_socket_path_uses_system_temp_dir() {
        let layout = AssetsLayout::new(PathBuf::from("/tmp/networks"), "casper-dev".to_string());
        let socket_path = layout.control_socket_path();
        assert_eq!(socket_path, std::env::temp_dir().join("casper-dev.socket"));
    }

    #[test]
    fn control_socket_path_sanitizes_network_name() {
        let layout = AssetsLayout::new(
            PathBuf::from("/tmp/networks"),
            "my/network with spaces".to_string(),
        );
        let socket_path = layout.control_socket_path();
        assert_eq!(
            socket_path,
            std::env::temp_dir().join("my_network_with_spaces.socket")
        );
    }

    #[test]
    fn normalize_hook_output_line_trims_newlines() {
        assert_eq!(normalize_hook_output_line(b"hello\n"), "hello");
        assert_eq!(normalize_hook_output_line(b"hello\r\n"), "hello");
        assert_eq!(normalize_hook_output_line(b"hello"), "hello");
        assert_eq!(normalize_hook_output_line(b"\n"), "");
    }

    #[test]
    fn hook_finished_message_uses_exit_code_when_available() {
        let status = std::process::ExitStatus::from_raw(7 << 8);
        assert_eq!(
            hook_finished_message("post-stage-protocol", &status),
            Some("post-stage-protocol finished with exit code 7".to_string())
        );
    }

    #[test]
    fn hook_finished_message_omits_zero_exit_code() {
        let status = std::process::ExitStatus::from_raw(0);
        assert_eq!(hook_finished_message("block-added", &status), None);
    }

    #[test]
    fn update_chainspec_contents_preserves_inline_core_tables() {
        let original = "\
[protocol]
activation_point = 1
version = '2.1.2'

[network]
name = 'casper-dev'

[core]
validator_slots = 4
rewards_handling = { type = 'sustain', purse_address = 'uref-abc-007' }
trap_on_ambiguous_entity_version = false

[highway]
maximum_round_length = '17 seconds'
";

        let updated =
            update_chainspec_contents(original, "2.1.3", "2", false, "casper-dev", 5).unwrap();

        assert!(
            updated.contains(
                "rewards_handling = { type = 'sustain', purse_address = 'uref-abc-007' }"
            )
        );
        assert!(!updated.contains("[core.rewards_handling]"));
        assert!(updated.contains("activation_point = 2"));
        assert!(updated.contains("version = '2.1.3'"));
        assert!(updated.contains("validator_slots = 5"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn add_nodes_creates_managed_non_genesis_node_assets() {
        let env = TestDataEnv::new();
        let layout = create_add_nodes_network_layout(env.root(), "casper-dev", "1.0.0", 1)
            .await
            .unwrap();
        let accounts_before =
            tokio_fs::read_to_string(layout.net_dir().join("chainspec/accounts.toml"))
                .await
                .unwrap();

        let result = add_nodes_for_test(&layout, &add_nodes_options(1))
            .await
            .unwrap();

        assert_eq!(result.added_node_ids, vec![2]);
        assert_eq!(result.total_nodes, 2);
        let version_dir = layout.node_config_root(2).join("1_0_0");
        assert!(is_file(&layout.node_bin_dir(2).join("1_0_0/casper-node")).await);
        assert!(is_file(&layout.node_bin_dir(2).join("1_0_0/casper-sidecar")).await);
        assert!(is_file(&version_dir.join("chainspec.toml")).await);
        assert!(is_file(&version_dir.join("accounts.toml")).await);
        assert!(is_file(&version_dir.join("config.toml")).await);
        assert!(is_file(&version_dir.join("sidecar.toml")).await);
        assert!(is_file(&layout.node_dir(2).join("keys/secret_key.pem")).await);
        assert_eq!(
            tokio_fs::read_to_string(layout.net_dir().join("chainspec/accounts.toml"))
                .await
                .unwrap(),
            accounts_before
        );

        let config: toml::Value = toml::from_str(
            &tokio_fs::read_to_string(version_dir.join("config.toml"))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            config
                .get("network")
                .and_then(|value| value.get("bind_address"))
                .and_then(toml::Value::as_str),
            Some("0.0.0.0:22102")
        );
        assert_eq!(
            config
                .get("rest_server")
                .and_then(|value| value.get("address"))
                .and_then(toml::Value::as_str),
            Some("0.0.0.0:14102")
        );
        assert_eq!(
            config
                .get("node")
                .and_then(|value| value.get("trusted_hash"))
                .and_then(toml::Value::as_str),
            Some("trusted-hash")
        );
        assert_eq!(
            config.get("sync_handling").and_then(toml::Value::as_str),
            None
        );
        assert_eq!(
            config
                .get("node")
                .and_then(|value| value.get("sync_handling"))
                .and_then(toml::Value::as_str),
            Some("ttl")
        );

        let summary = tokio_fs::read_to_string(derived_accounts_path(&layout))
            .await
            .unwrap();
        assert!(summary.contains("node,node-2,secp256k1,bip32,m/44'/506'/0'/0/1,"));
        assert!(summary.trim_end().ends_with(",0"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rollback_added_nodes_removes_assets_and_summary_rows() {
        let env = TestDataEnv::new();
        let layout = create_add_nodes_network_layout(env.root(), "casper-dev", "1.0.0", 1)
            .await
            .unwrap();
        add_nodes_for_test(&layout, &add_nodes_options(1))
            .await
            .unwrap();

        rollback_added_nodes(&layout, &[2]).await.unwrap();

        assert!(!is_dir(&layout.node_dir(2)).await);
        let summary = tokio_fs::read_to_string(derived_accounts_path(&layout))
            .await
            .unwrap();
        assert!(!summary.contains("node,node-2,"));
        assert_eq!(
            summary.trim_end(),
            "kind,name,key_type,derivation,path,account_hash,balance"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn add_nodes_rejects_zero_count() {
        let env = TestDataEnv::new();
        let layout = create_add_nodes_network_layout(env.root(), "casper-dev", "1.0.0", 1)
            .await
            .unwrap();

        let err = add_nodes_for_test(&layout, &add_nodes_options(0))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("count must be greater than 0"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn add_nodes_rejects_non_contiguous_existing_nodes() {
        let env = TestDataEnv::new();
        let layout = create_add_nodes_network_layout(env.root(), "casper-dev", "1.0.0", 1)
            .await
            .unwrap();
        tokio_fs::rename(layout.node_dir(1), layout.node_dir(2))
            .await
            .unwrap();

        let err = add_nodes_for_test(&layout, &add_nodes_options(1))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("contiguous"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn add_nodes_copies_only_active_version() {
        let env = TestDataEnv::new();
        let layout = create_add_nodes_network_layout(env.root(), "casper-dev", "1.0.0", 1)
            .await
            .unwrap();
        tokio_fs::create_dir_all(layout.node_bin_dir(1).join("2_0_0"))
            .await
            .unwrap();
        tokio_fs::create_dir_all(layout.node_config_root(1).join("2_0_0"))
            .await
            .unwrap();
        tokio_fs::write(layout.node_bin_dir(1).join("2_0_0/casper-node"), "future")
            .await
            .unwrap();

        add_nodes_for_test(&layout, &add_nodes_options(1))
            .await
            .unwrap();

        assert!(is_dir(&layout.node_bin_dir(2).join("1_0_0")).await);
        assert!(!is_dir(&layout.node_bin_dir(2).join("2_0_0")).await);
        assert!(!is_dir(&layout.node_config_root(2).join("2_0_0")).await);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn add_nodes_disables_legacy_genesis_sync_mode() {
        let env = TestDataEnv::new();
        let layout = create_add_nodes_network_layout(env.root(), "casper-dev", "1.0.0", 1)
            .await
            .unwrap();
        let source_config = layout.node_config_root(1).join("1_0_0/config.toml");
        let contents = tokio_fs::read_to_string(&source_config).await.unwrap();
        tokio_fs::write(
            &source_config,
            format!("sync_to_genesis = true\n\n{contents}"),
        )
        .await
        .unwrap();

        add_nodes_for_test(&layout, &add_nodes_options(1))
            .await
            .unwrap();

        let generated_config = layout.node_config_root(2).join("1_0_0/config.toml");
        let config: toml::Value =
            toml::from_str(&tokio_fs::read_to_string(generated_config).await.unwrap()).unwrap();
        assert_eq!(
            config.get("sync_to_genesis").and_then(toml::Value::as_bool),
            Some(false)
        );
        assert!(config.get("sync_handling").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn add_nodes_disables_legacy_node_genesis_sync_mode() {
        let env = TestDataEnv::new();
        let layout = create_add_nodes_network_layout(env.root(), "casper-dev", "1.0.0", 1)
            .await
            .unwrap();
        let source_config = layout.node_config_root(1).join("1_0_0/config.toml");
        let contents = tokio_fs::read_to_string(&source_config).await.unwrap();
        tokio_fs::write(
            &source_config,
            format!("{contents}\n[node]\nsync_to_genesis = true\n"),
        )
        .await
        .unwrap();

        add_nodes_for_test(&layout, &add_nodes_options(1))
            .await
            .unwrap();

        let generated_config = layout.node_config_root(2).join("1_0_0/config.toml");
        let config: toml::Value =
            toml::from_str(&tokio_fs::read_to_string(generated_config).await.unwrap()).unwrap();
        assert_eq!(
            config
                .get("node")
                .and_then(|value| value.get("sync_to_genesis"))
                .and_then(toml::Value::as_bool),
            Some(false)
        );
        assert!(
            config
                .get("node")
                .and_then(|value| value.get("sync_handling"))
                .is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn add_nodes_rejects_migrating_launcher_state() {
        let env = TestDataEnv::new();
        let layout = create_add_nodes_network_layout(env.root(), "casper-dev", "1.0.0", 1)
            .await
            .unwrap();
        tokio_fs::write(
            layout.node_config_root(1).join(NODE_LAUNCHER_STATE_FILE),
            "mode = \"MigrateData\"\n",
        )
        .await
        .unwrap();

        let err = add_nodes_for_test(&layout, &add_nodes_options(1))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("migration completes"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn add_nodes_rejects_mixed_active_versions() {
        let env = TestDataEnv::new();
        let layout = create_add_nodes_network_layout(env.root(), "casper-dev", "1.0.0", 2)
            .await
            .unwrap();
        tokio_fs::write(
            layout.node_config_root(2).join(NODE_LAUNCHER_STATE_FILE),
            "mode = \"RunNodeAsValidator\"\nversion = \"2.0.0\"\n",
        )
        .await
        .unwrap();

        let err = add_nodes_for_test(&layout, &add_nodes_options(1))
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("active protocol versions are mixed")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ensure_network_hook_samples_creates_executable_samples() {
        let env = TestDataEnv::new();
        let layout = create_test_network_layout(env.root(), "casper-dev", "1.0.0")
            .await
            .unwrap();

        ensure_network_hook_samples(&layout).await.unwrap();

        let pre_sample = layout.hooks_dir().join(PRE_GENESIS_SAMPLE);
        let post_sample = layout.hooks_dir().join(POST_GENESIS_SAMPLE);
        let block_added_sample = layout.hooks_dir().join(BLOCK_ADDED_SAMPLE);

        assert!(is_file(&pre_sample).await);
        assert!(is_file(&post_sample).await);
        assert!(is_file(&block_added_sample).await);
        assert_ne!(
            tokio_fs::metadata(&pre_sample)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );
        assert_ne!(
            tokio_fs::metadata(&post_sample)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );
        assert_ne!(
            tokio_fs::metadata(&block_added_sample)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn teardown_preserves_network_hooks_and_removes_other_assets() {
        let env = TestDataEnv::new();
        let layout = create_test_network_layout(env.root(), "casper-dev", "1.0.0")
            .await
            .unwrap();
        ensure_network_hook_samples(&layout).await.unwrap();

        let active_hook = layout.hooks_dir().join(PRE_GENESIS_HOOK);
        let hook_marker = layout.hook_work_dir(PRE_GENESIS_HOOK).join("marker.txt");
        let node_marker = layout.node_dir(1).join("storage").join("marker.txt");
        let state_path = layout.net_dir().join("state.json");

        write_executable_script(&active_hook, "#!/bin/sh\nset -eu\n")
            .await
            .unwrap();
        tokio_fs::create_dir_all(layout.hook_work_dir(PRE_GENESIS_HOOK))
            .await
            .unwrap();
        tokio_fs::write(&hook_marker, "keep").await.unwrap();
        tokio_fs::write(&node_marker, "remove").await.unwrap();
        tokio_fs::write(&state_path, "{}").await.unwrap();

        teardown(&layout).await.unwrap();

        assert!(is_dir(&layout.net_dir()).await);
        assert!(is_file(&active_hook).await);
        assert!(is_file(&hook_marker).await);
        assert!(!is_dir(&layout.nodes_dir()).await);
        assert!(!is_dir(&layout.net_dir().join("chainspec")).await);
        assert!(!is_file(&state_path).await);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn install_custom_asset_creates_executable_hook_samples() {
        let env = TestDataEnv::new();
        let asset_dir = install_test_custom_asset(&env, "dev").await.unwrap();

        let pre_sample = asset_dir
            .join(HOOKS_DIR_NAME)
            .join(PRE_STAGE_PROTOCOL_SAMPLE);
        let post_sample = asset_dir
            .join(HOOKS_DIR_NAME)
            .join(POST_STAGE_PROTOCOL_SAMPLE);

        assert!(is_file(&pre_sample).await);
        assert!(is_file(&post_sample).await);
        assert_ne!(
            tokio_fs::metadata(&pre_sample)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );
        assert_ne!(
            tokio_fs::metadata(&post_sample)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn install_custom_asset_rejects_duplicate_names_without_mutation() {
        let env = TestDataEnv::new();
        let asset_dir = install_test_custom_asset(&env, "dev").await.unwrap();
        let sentinel = asset_dir.join("sentinel.txt");
        tokio_fs::write(&sentinel, "keep").await.unwrap();

        let duplicate = create_test_custom_asset_sources(env.root(), "dev")
            .await
            .unwrap();
        let err = install_custom_asset(&duplicate).await.unwrap_err();

        assert!(err.to_string().contains("already exists"));
        assert_eq!(tokio_fs::read_to_string(&sentinel).await.unwrap(), "keep");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stage_protocol_ignores_sample_hook_files() {
        let env = TestDataEnv::new();
        let asset_dir = install_test_custom_asset(&env, "dev").await.unwrap();
        let layout = create_test_network_layout(env.root(), "casper-dev", "1.0.0")
            .await
            .unwrap();
        write_executable_script(
            &asset_dir
                .join(HOOKS_DIR_NAME)
                .join(PRE_STAGE_PROTOCOL_SAMPLE),
            "#!/bin/sh\nset -eu\necho sample-ran > \"$PWD/sample-hook-ran\"\n",
        )
        .await
        .unwrap();

        stage_protocol(&layout, &stage_options("dev", "2.0.0"))
            .await
            .unwrap();

        assert!(!is_file(&layout.net_dir().join("sample-hook-ran")).await);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stage_protocol_aborts_before_mutation_when_pre_hook_fails() {
        let env = TestDataEnv::new();
        let asset_dir = install_test_custom_asset(&env, "dev").await.unwrap();
        let layout = create_test_network_layout(env.root(), "casper-dev", "1.0.0")
            .await
            .unwrap();
        write_executable_script(
            &asset_dir.join(HOOKS_DIR_NAME).join(PRE_STAGE_PROTOCOL_HOOK),
            "#!/bin/sh\nset -eu\necho pre-hook-start >&2\nexit 7\n",
        )
        .await
        .unwrap();

        let err = stage_protocol(&layout, &stage_options("dev", "2.0.0"))
            .await
            .unwrap_err();

        assert!(err.to_string().contains(PRE_STAGE_PROTOCOL_HOOK));
        assert!(!is_dir(&layout.node_bin_dir(1).join("2_0_0")).await);
        assert!(!is_dir(&layout.node_config_root(1).join("2_0_0")).await);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prepare_genesis_hooks_runs_pre_hook_and_writes_pending_post_hook_metadata() {
        let env = TestDataEnv::new();
        let layout = create_test_network_layout(env.root(), "casper-dev", "1.0.0")
            .await
            .unwrap();
        write_executable_script(
            &layout.hooks_dir().join(PRE_GENESIS_HOOK),
            "#!/bin/sh\nset -eu\nprintf '%s\n' \"$PWD\" > \"$PWD/pre-genesis-cwd\"\nprintf '%s,%s\n' \"$1\" \"$2\" > \"$PWD/pre-genesis-args\"\n",
        )
        .await
        .unwrap();
        write_executable_script(
            &layout.hooks_dir().join(POST_GENESIS_HOOK),
            "#!/bin/sh\nset -eu\necho post-genesis\n",
        )
        .await
        .unwrap();

        prepare_genesis_hooks(&layout, "1.0.0").await.unwrap();

        let expected_hook_dir = tokio_fs::canonicalize(layout.hook_work_dir(PRE_GENESIS_HOOK))
            .await
            .unwrap();
        assert_eq!(
            tokio_fs::read_to_string(
                layout
                    .hook_work_dir(PRE_GENESIS_HOOK)
                    .join("pre-genesis-cwd")
            )
            .await
            .unwrap()
            .trim(),
            expected_hook_dir.display().to_string()
        );
        assert_eq!(
            tokio_fs::read_to_string(
                layout
                    .hook_work_dir(PRE_GENESIS_HOOK)
                    .join("pre-genesis-args"),
            )
            .await
            .unwrap()
            .trim(),
            "casper-dev,1.0.0"
        );

        let pending_path = pending_post_genesis_hook_path(&layout.hooks_dir(), "1.0.0");
        let pending: Value =
            serde_json::from_slice(&tokio_fs::read(&pending_path).await.unwrap()).unwrap();
        assert_eq!(pending["network_name"], "casper-dev");
        assert_eq!(pending["protocol_version"], "1.0.0");
        assert_eq!(
            pending["stdout_path"],
            layout
                .hook_logs_dir()
                .join("post-genesis-1_0_0.stdout.log")
                .display()
                .to_string()
        );
        assert_eq!(
            pending["stderr_path"],
            layout
                .hook_logs_dir()
                .join("post-genesis-1_0_0.stderr.log")
                .display()
                .to_string()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stage_protocol_runs_pre_hook_and_writes_pending_post_hook_metadata() {
        let env = TestDataEnv::new();
        let asset_dir = install_test_custom_asset(&env, "dev").await.unwrap();
        let layout = create_test_network_layout(env.root(), "casper-dev", "1.0.0")
            .await
            .unwrap();
        write_executable_script(
            &asset_dir.join(HOOKS_DIR_NAME).join(PRE_STAGE_PROTOCOL_HOOK),
            "#!/bin/sh\nset -eu\nprintf '%s\n' \"$PWD\" > \"$PWD/pre-hook-cwd\"\nprintf '%s,%s,%s\n' \"$1\" \"$2\" \"$3\" > \"$PWD/pre-hook-args\"\necho pre-stdout\necho pre-stderr >&2\n",
        )
        .await
        .unwrap();
        write_executable_script(
            &asset_dir
                .join(HOOKS_DIR_NAME)
                .join(POST_STAGE_PROTOCOL_HOOK),
            "#!/bin/sh\nset -eu\necho post-hook\n",
        )
        .await
        .unwrap();

        let stale_claim = post_stage_protocol_claim_path(&layout.hooks_dir(), "2.0.0");
        let stale_completion = post_stage_protocol_completion_path(&layout.hooks_dir(), "2.0.0");
        tokio_fs::create_dir_all(layout.hooks_status_dir())
            .await
            .unwrap();
        tokio_fs::write(&stale_claim, "stale").await.unwrap();
        tokio_fs::write(&stale_completion, "stale").await.unwrap();

        stage_protocol(&layout, &stage_options("dev", "2.0.0"))
            .await
            .unwrap();

        let expected_net_dir =
            tokio_fs::canonicalize(layout.hook_work_dir(PRE_STAGE_PROTOCOL_HOOK))
                .await
                .unwrap();
        assert_eq!(
            tokio_fs::read_to_string(
                layout
                    .hook_work_dir(PRE_STAGE_PROTOCOL_HOOK)
                    .join("pre-hook-cwd")
            )
            .await
            .unwrap()
            .trim(),
            expected_net_dir.display().to_string()
        );
        assert_eq!(
            tokio_fs::read_to_string(
                layout
                    .hook_work_dir(PRE_STAGE_PROTOCOL_HOOK)
                    .join("pre-hook-args"),
            )
            .await
            .unwrap()
            .trim(),
            "casper-dev,2.0.0,123"
        );

        let pre_logs = hook_log_paths(&layout.hook_logs_dir(), PRE_STAGE_PROTOCOL_HOOK, "2.0.0");
        assert_eq!(
            tokio_fs::read_to_string(&pre_logs.stdout)
                .await
                .unwrap()
                .trim(),
            "pre-stdout"
        );
        assert_eq!(
            tokio_fs::read_to_string(&pre_logs.stderr)
                .await
                .unwrap()
                .trim(),
            "pre-stderr"
        );

        let pending_path = pending_post_stage_protocol_hook_path(&layout.hooks_dir(), "2.0.0");
        let pending: Value =
            serde_json::from_slice(&tokio_fs::read(&pending_path).await.unwrap()).unwrap();
        assert_eq!(pending["asset_name"], "dev");
        assert_eq!(pending["network_name"], "casper-dev");
        assert_eq!(pending["protocol_version"], "2.0.0");
        assert_eq!(pending["activation_point"], 123);
        assert_eq!(
            pending["stdout_path"],
            layout
                .hook_logs_dir()
                .join("post-stage-protocol-2_0_0.stdout.log")
                .display()
                .to_string()
        );
        assert_eq!(
            pending["stderr_path"],
            layout
                .hook_logs_dir()
                .join("post-stage-protocol-2_0_0.stderr.log")
                .display()
                .to_string()
        );
        assert!(!is_file(&stale_claim).await);
        assert!(!is_file(&stale_completion).await);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_pending_post_genesis_hook_runs_once_with_network_cwd_and_log_redirection() {
        let env = TestDataEnv::new();
        let layout = create_test_network_layout(env.root(), "casper-dev", "1.0.0")
            .await
            .unwrap();
        write_executable_script(
            &layout.hooks_dir().join(POST_GENESIS_HOOK),
            "#!/bin/sh\nset -eu\nprintf '%s\n' \"$PWD\" > \"$PWD/post-genesis-cwd\"\nprintf '%s,%s\n' \"$1\" \"$2\" > \"$PWD/post-genesis-args\"\necho hook-stdout\necho hook-stderr >&2\necho run >> \"$PWD/post-genesis-count\"\n",
        )
        .await
        .unwrap();

        prepare_genesis_hooks(&layout, "1.0.0").await.unwrap();

        assert!(matches!(
            run_pending_post_genesis_hook(&layout).await.unwrap(),
            PendingPostGenesisHookResult::Succeeded
        ));
        assert!(matches!(
            run_pending_post_genesis_hook(&layout).await.unwrap(),
            PendingPostGenesisHookResult::NotRun
        ));

        let expected_net_dir = tokio_fs::canonicalize(layout.hook_work_dir(POST_GENESIS_HOOK))
            .await
            .unwrap();
        assert_eq!(
            tokio_fs::read_to_string(
                layout
                    .hook_work_dir(POST_GENESIS_HOOK)
                    .join("post-genesis-cwd"),
            )
            .await
            .unwrap()
            .trim(),
            expected_net_dir.display().to_string()
        );
        assert_eq!(
            tokio_fs::read_to_string(
                layout
                    .hook_work_dir(POST_GENESIS_HOOK)
                    .join("post-genesis-args"),
            )
            .await
            .unwrap()
            .trim(),
            "casper-dev,1.0.0"
        );
        assert_eq!(
            tokio_fs::read_to_string(
                layout
                    .hook_work_dir(POST_GENESIS_HOOK)
                    .join("post-genesis-count"),
            )
            .await
            .unwrap()
            .trim(),
            "run"
        );

        let post_logs = hook_log_paths(&layout.hook_logs_dir(), POST_GENESIS_HOOK, "1.0.0");
        assert_eq!(
            tokio_fs::read_to_string(&post_logs.stdout)
                .await
                .unwrap()
                .trim(),
            "hook-stdout"
        );
        assert_eq!(
            tokio_fs::read_to_string(&post_logs.stderr)
                .await
                .unwrap()
                .trim(),
            "hook-stderr"
        );
        assert!(is_file(&post_genesis_completion_path(&layout.hooks_dir(), "1.0.0")).await);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_block_added_hook_passes_payload_on_stdin_and_appends_logs() {
        let env = TestDataEnv::new();
        let layout = create_test_network_layout(env.root(), "casper-dev", "1.0.0")
            .await
            .unwrap();
        write_executable_script(
            &layout.hooks_dir().join(BLOCK_ADDED_HOOK),
            "#!/bin/sh\nset -eu\nprintf '%s,%s\n' \"$1\" \"$2\" >> \"$PWD/block-added-args\"\ncat >> \"$PWD/block-added-payloads\"\necho hook-stdout\necho hook-stderr >&2\n",
        )
        .await
        .unwrap();

        run_block_added_hook(
            &layout,
            "1.0.0",
            json!({
                "block_hash": "abc",
                "height": 1,
                "era_id": 0,
            }),
        )
        .await
        .unwrap();
        run_block_added_hook(
            &layout,
            "1.0.0",
            json!({
                "block_hash": "def",
                "height": 2,
                "era_id": 0,
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            tokio_fs::read_to_string(
                layout
                    .hook_work_dir(BLOCK_ADDED_HOOK)
                    .join("block-added-args"),
            )
            .await
            .unwrap(),
            "casper-dev,1.0.0\ncasper-dev,1.0.0\n"
        );
        assert_eq!(
            tokio_fs::read_to_string(
                layout
                    .hook_work_dir(BLOCK_ADDED_HOOK)
                    .join("block-added-payloads"),
            )
            .await
            .unwrap(),
            "{\"block_hash\":\"abc\",\"height\":1,\"era_id\":0}\n{\"block_hash\":\"def\",\"height\":2,\"era_id\":0}\n"
        );

        let logs = hook_log_paths(&layout.hook_logs_dir(), BLOCK_ADDED_HOOK, "1.0.0");
        assert_eq!(
            tokio_fs::read_to_string(&logs.stdout).await.unwrap(),
            "hook-stdout\nhook-stdout\n"
        );
        assert_eq!(
            tokio_fs::read_to_string(&logs.stderr).await.unwrap(),
            "hook-stderr\nhook-stderr\n"
        );
    }
}
