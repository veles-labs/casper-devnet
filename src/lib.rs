pub mod assets;
pub mod cli;
mod node_launcher;
pub mod process;
pub mod state;

pub async fn run() -> anyhow::Result<()> {
    cli::run().await
}
