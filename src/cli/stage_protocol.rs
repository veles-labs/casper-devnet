use crate::assets::{self, AssetsLayout, StageProtocolOptions};
use crate::cli::common::{is_control_socket, read_current_era_from_status, shorten_home_path};
use crate::control::{ControlRequest, ControlResponse, ControlResult, send_request};
use anyhow::{Result, anyhow};
use clap::Args;
use std::path::PathBuf;

#[derive(Args, Clone)]
pub(crate) struct StageProtocolArgs {
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

pub(crate) async fn run(args: StageProtocolArgs) -> Result<()> {
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
