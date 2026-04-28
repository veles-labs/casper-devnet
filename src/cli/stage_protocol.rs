use crate::assets::{self, AssetSelector, AssetsLayout, StageProtocolOptions};
use crate::cli::common::{is_control_socket, read_current_era_from_status, shorten_home_path};
use crate::control::{ControlRequest, ControlResponse, ControlResult, send_request};
use anyhow::{Result, anyhow};
use clap::{ArgGroup, Args};
use std::path::PathBuf;

#[derive(Args, Clone)]
#[command(group(
    ArgGroup::new("asset_selector")
        .required(true)
        .multiple(false)
        .args(["asset_arg", "asset", "custom_asset"])
))]
pub(crate) struct StageProtocolArgs {
    /// Versioned asset to use from the assets store.
    #[arg(value_name = "ASSET")]
    asset_arg: Option<String>,

    /// Versioned asset to use from the assets store.
    #[arg(long)]
    asset: Option<String>,

    /// Custom asset name from assets/custom.
    #[arg(long)]
    custom_asset: Option<String>,

    /// Protocol version to stage (e.g. 2.2.0).
    #[arg(long)]
    protocol_version: String,

    /// Future era id for activation.
    #[arg(long)]
    activation_point: u64,

    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,
}

impl StageProtocolArgs {
    fn asset_selector(&self) -> Result<AssetSelector> {
        match (
            self.asset_arg.as_deref(),
            self.asset.as_deref(),
            self.custom_asset.as_deref(),
        ) {
            (Some(asset), None, None) => Ok(AssetSelector::Versioned(asset.to_string())),
            (None, Some(asset), None) => Ok(AssetSelector::Versioned(asset.to_string())),
            (None, None, Some(custom_asset)) => Ok(AssetSelector::Custom(custom_asset.to_string())),
            _ => Err(anyhow!(
                "exactly one of positional asset, --asset, or --custom-asset is required"
            )),
        }
    }

    fn asset_display(&self) -> String {
        if let Some(asset) = &self.asset_arg {
            return asset.clone();
        }
        if let Some(asset) = &self.asset {
            return asset.clone();
        }
        format!(
            "custom/{}",
            self.custom_asset
                .as_deref()
                .expect("clap validates asset selector")
        )
    }
}

pub(crate) async fn run(network_name: String, args: StageProtocolArgs) -> Result<()> {
    let assets_root = match &args.net_path {
        Some(path) => path.clone(),
        None => assets::default_assets_root()?,
    };
    let layout = AssetsLayout::new(assets_root, network_name);
    if !layout.exists().await {
        return Err(anyhow!(
            "assets for {} not found under {}; run `start --setup-only` first",
            layout.network_name(),
            shorten_home_path(&layout.net_dir().display().to_string())
        ));
    }

    let asset_selector = args.asset_selector()?;
    let request = ControlRequest::StageProtocol {
        asset: match &asset_selector {
            AssetSelector::Versioned(asset) => Some(asset.clone()),
            AssetSelector::LatestVersioned | AssetSelector::Custom(_) => None,
        },
        custom_asset: match &asset_selector {
            AssetSelector::Custom(asset) => Some(asset.clone()),
            AssetSelector::LatestVersioned | AssetSelector::Versioned(_) => None,
        },
        asset_name: None,
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
                    ControlResult::RuntimeStatus { .. } => {
                        return Err(anyhow!(
                            "unexpected runtime_status response from {}",
                            socket_path.display()
                        ));
                    }
                    ControlResult::AddNodes { .. } => {
                        return Err(anyhow!(
                            "unexpected add_nodes response from {}",
                            socket_path.display()
                        ));
                    }
                };
                println!(
                    "staged protocol {} from asset '{}' for {} node(s) (live_mode={})",
                    args.protocol_version,
                    args.asset_display(),
                    staged_nodes,
                    live_mode
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
            asset: asset_selector,
            protocol_version: args.protocol_version.clone(),
            activation_point: args.activation_point,
        },
    )
    .await?;
    println!(
        "staged protocol {} from asset '{}' for {} node(s) (offline mode; sidecars not restarted)",
        args.protocol_version,
        args.asset_display(),
        staged.staged_nodes
    );
    Ok(())
}
