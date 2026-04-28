mod assets;
mod common;
mod derive;
mod network;
mod networks;
mod stage_protocol;
mod start;

#[cfg(test)]
mod tests;

use crate::mcp::{self, McpArgs};
use anyhow::Result;
use clap::{Parser, Subcommand};

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
    Start(start::StartArgs),
    /// Run MCP control plane server.
    Mcp(McpArgs),
    /// Manage assets bundles.
    Assets(assets::AssetsArgs),
    /// Derive deterministic account material from a BIP32 path.
    Derive(derive::DeriveArgs),
    /// Inspect a managed network.
    Network(network::NetworkArgs),
    /// Manage local network directories.
    Networks(networks::NetworksArgs),
}

/// Parses CLI and runs the selected subcommand.
pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Start(args) => start::run(args).await,
        Command::Mcp(args) => mcp::run(args).await,
        Command::Assets(args) => assets::run(args).await,
        Command::Derive(args) => derive::run(args).await,
        Command::Network(args) => network::run(args).await,
        Command::Networks(args) => networks::run(args).await,
    }
}
