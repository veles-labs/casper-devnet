use crate::assets;
use crate::cli::common::is_dir;
use anyhow::{Result, anyhow};
use clap::{Args, Subcommand};
use dialoguer::Confirm;
use std::path::PathBuf;
use tokio::fs as tokio_fs;

#[derive(Args)]
pub(crate) struct NetworksArgs {
    #[command(subcommand)]
    command: NetworksCommand,
}

#[derive(Subcommand)]
enum NetworksCommand {
    /// List managed network names.
    List(NetworksListArgs),
    /// Remove a managed network from disk.
    Rm(NetworksRmArgs),
}

#[derive(Args, Clone)]
struct NetworksListArgs {
    /// Override the base path for network runtime assets.
    #[arg(long, value_name = "PATH")]
    net_path: Option<PathBuf>,
}

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

pub(crate) async fn run(args: NetworksArgs) -> Result<()> {
    match args.command {
        NetworksCommand::List(list) => run_networks_list(list).await,
        NetworksCommand::Rm(remove) => run_networks_rm(remove).await,
    }
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
