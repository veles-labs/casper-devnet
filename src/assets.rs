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
use serde::Deserialize;
use sha2::{Digest, Sha512};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tar::Archive;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};
use tokio::fs as tokio_fs;
use tokio::process::Command;
use tokio::task;

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

#[derive(Debug)]
struct DerivedAccountMaterial {
    path: DerivationPath,
    public_key_hex: String,
    account_hash: String,
    secret_key_pem: Option<String>,
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

    /// Directory for daemon-related artifacts.
    pub fn daemon_dir(&self) -> PathBuf {
        self.net_dir().join("daemon")
    }

    /// Returns true if the network's nodes directory exists.
    pub async fn exists(&self) -> bool {
        tokio_fs::metadata(self.nodes_dir())
            .await
            .map(|meta| meta.is_dir())
            .unwrap_or(false)
    }

    /// Count node directories under `nodes/`.
    pub async fn count_nodes(&self) -> Result<u32> {
        let nodes_dir = self.nodes_dir();
        let mut count = 0u32;
        if !is_dir(&nodes_dir).await {
            return Ok(0);
        }
        let mut entries = tokio_fs::read_dir(&nodes_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("node-") {
                count += 1;
            }
        }
        Ok(count)
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

    Ok(())
}

/// Remove assets for the given network (and local dumps).
pub async fn teardown(layout: &AssetsLayout) -> Result<()> {
    let net_dir = layout.net_dir();
    if is_dir(&net_dir).await {
        tokio_fs::remove_dir_all(&net_dir).await?;
    }
    let dumps = layout.net_dir().join("dumps");
    if is_dir(&dumps).await {
        tokio_fs::remove_dir_all(dumps).await?;
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
        let dir_path = entry.path();
        let chainspec_path = dir_path.join("chainspec.toml");
        if !is_file(&chainspec_path).await {
            continue;
        }
        let contents = tokio_fs::read_to_string(&chainspec_path).await?;
        let version = spawn_blocking_result(move || parse_chainspec_version(&contents)).await?;
        let expected_dir = format!("v{}", version);
        if name != expected_dir {
            return Err(anyhow!(
                "bundle directory {} does not match chainspec protocol version {}",
                name,
                version
            ));
        }
        versions.push(version);
    }
    Ok(versions)
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
    let bin_dir = net_dir.join("bin");
    let chainspec_dir = net_dir.join("chainspec");
    let daemon_dir = net_dir.join("daemon");
    let nodes_dir = net_dir.join("nodes");

    tokio_fs::create_dir_all(bin_dir).await?;
    tokio_fs::create_dir_all(chainspec_dir).await?;
    tokio_fs::create_dir_all(daemon_dir.join("config")).await?;
    tokio_fs::create_dir_all(daemon_dir.join("logs")).await?;
    tokio_fs::create_dir_all(daemon_dir.join("socket")).await?;
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
        let mut value: toml::Value = toml::from_str(&chainspec_contents)?;
        set_string(
            &mut value,
            &["protocol", "activation_point"],
            activation_point,
        )?;
        set_string(&mut value, &["protocol", "version"], protocol_version_chain)?;
        set_string(&mut value, &["network", "name"], network_name)?;

        set_integer(&mut value, &["core", "validator_slots"], total_nodes as i64)?;

        Ok(toml::to_string(&value)?)
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
mod tests {
    use super::format_cspr;

    #[test]
    fn format_cspr_handles_whole_and_fractional() {
        assert_eq!(format_cspr(0), "0");
        assert_eq!(format_cspr(1), "0.000000001");
        assert_eq!(format_cspr(1_000_000_000), "1");
        assert_eq!(format_cspr(1_000_000_001), "1.000000001");
        assert_eq!(format_cspr(123_000_000_000), "123");
        assert_eq!(format_cspr(123_000_000_456), "123.000000456");
    }
}
