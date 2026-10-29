use arrowcol_authority::{
    Config,
    authority::{Authority, serve},
};
use clap::Parser;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    private: Option<String>,
    #[arg(long)]
    config: String,
    #[arg(long, default_value = "0.0.0.0:50060")]
    listen: String,
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Config::load(&args.config)?;
    if config.delta.is_some() {
        use arrowcol_authority::delta_runtime::{Control, PrivateConfig, adr_select, serve_control};
        let private = PrivateConfig::load(
            args.private
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("Delta private config required"))?,
        )?;
        let store = private.store(&config).await?;
        return serve_control(Control::start(config, store, adr_select).await?, &args.listen).await;
    }
    anyhow::ensure!(
        args.private.is_none(),
        "private configuration requires explicit Delta mode"
    );
    serve(Authority::start(config).await?, &args.listen).await
}
