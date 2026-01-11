#[tokio::main]
async fn main() {
    if let Err(err) = casper_devnet::run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
