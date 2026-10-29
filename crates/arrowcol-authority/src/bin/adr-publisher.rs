use arrowcol_authority::{
    Config,
    delta_runtime::{PrivateConfig, Publication, serve_publication},
};
use clap::Parser;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: String,
    #[arg(long)]
    private: String,
    #[arg(long, default_value = "0.0.0.0:50061")]
    listen: String,
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Config::load(&args.config)?;
    anyhow::ensure!(config.delta.is_some(), "publisher requires explicit Delta mode");
    serve_publication(
        Publication::new(config, PrivateConfig::load(&args.private)?).await?,
        &args.listen,
    )
    .await
}
