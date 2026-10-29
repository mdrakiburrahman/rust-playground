use arrowcol_authority::{
    Config,
    flight::{Service, serve},
};
use clap::Parser;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: String,
    #[arg(long)]
    collector: Option<String>,
    #[arg(long, default_value = "0.0.0.0:50051")]
    listen: String,
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Config::load(&args.config)?;
    let service = match args.collector {
        Some(id) => Service::collector(config, &id)?,
        None => Service::gateway(config)?,
    };
    serve(service, &args.listen).await
}
