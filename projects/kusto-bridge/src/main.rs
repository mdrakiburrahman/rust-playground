use anyhow::Result;
use clap::{Parser, Subcommand};
use kusto_bridge::{
    config::Config,
    server::{self, AppState},
    status::Status,
};
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the bridge service.
    Serve(Box<Config>),
    /// Exit 0 when the bridge answers `/healthz`.
    Healthcheck {
        #[arg(long, default_value = "http://127.0.0.1:9464/healthz")]
        url: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();
    match Cli::parse().command {
        Command::Healthcheck { url } => server::healthcheck(&url).await,
        Command::Serve(config) => serve(*config).await,
    }
}

async fn serve(config: Config) -> Result<()> {
    let status = Status::default();
    let (trigger, _triggered) = mpsc::channel(1);
    tracing::warn!(
        sources = config.sources.0.len(),
        "reconciler not wired yet; serving status only"
    );
    server::serve(config.listen, AppState { status, trigger }, shutdown()).await
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(term) => term,
            Err(error) => {
                tracing::warn!(%error, "SIGTERM handler unavailable");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
