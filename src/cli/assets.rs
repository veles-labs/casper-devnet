use crate::assets::{self, CustomAssetInstallOptions};
use anyhow::{Result, anyhow};
use clap::{Args, Subcommand};
use std::path::{Path, PathBuf};

#[derive(Args)]
pub(crate) struct AssetsArgs {
    #[command(subcommand)]
    command: AssetsCommand,
}

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

#[derive(Args, Clone)]
struct AssetsPullArgs {
    /// Target triple to select from release assets.
    #[arg(long)]
    target: Option<String>,

    /// Re-download and replace any existing assets.
    #[arg(long)]
    force: bool,
}

#[derive(Args, Clone)]
struct AssetsPathArgs {
    /// Custom asset name.
    #[arg(value_name = "ASSET_NAME")]
    asset_name: String,
}

pub(crate) async fn run(args: AssetsArgs) -> Result<()> {
    match args.command {
        AssetsCommand::Add(add) => run_assets_add(add).await,
        AssetsCommand::Pull(pull) => run_assets_pull(pull).await,
        AssetsCommand::List => run_assets_list().await,
        AssetsCommand::Path(path) => run_assets_path(path).await,
    }
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

fn looks_like_url(path: &Path) -> bool {
    let value = path.to_string_lossy();
    value.starts_with("http://") || value.starts_with("https://")
}

fn is_custom_asset_add_requested(args: &AssetsAddArgs) -> bool {
    args.casper_node.is_some()
        || args.casper_sidecar.is_some()
        || args.chainspec.is_some()
        || args.node_config.is_some()
        || args.sidecar_config.is_some()
}
