use anyhow::{Context, Result};
use arrowcol_rust::{Cli, CollectorConfig, RunOutcome, run};
use clap::Parser;
use delta_kernel::metrics::WithMetricsReporterLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = CollectorConfig::load(&cli.config)?;
    let _metrics = arrowcol_buffer::MetricsGuard::install("arrowcol-rust")?;
    initialize_tracing(&config)?;
    arrow_deltalake::delta::initialize_shared_io()?;
    let outcome = if cli.validate_and_exit {
        info!(config = %cli.config.display(), "direct collector configuration is valid");
        RunOutcome::Clean
    } else {
        run(config).await?
    };
    match outcome {
        RunOutcome::Clean => {
            arrow_deltalake::publisher::PublisherSession::close_shared_async()
                .await
                .map_err(|error| {
                    eprintln!("publisher session close failed: {error:#}");
                    error.context("close publisher sessions after collector drain")
                })?;
            Ok(())
        }
        RunOutcome::IncompleteDrain { incomplete } => {
            anyhow::bail!("collector shutdown left {incomplete} incomplete publication group(s)")
        }
    }
}

fn initialize_tracing(config: &CollectorConfig) -> Result<()> {
    let filter = EnvFilter::try_new(&config.telemetry.log_filter)
        .with_context(|| format!("parse telemetry.log_filter {:?}", config.telemetry.log_filter))?;
    if config.telemetry.json_logs {
        tracing_subscriber::registry()
            .with_metrics_reporter_layer(arrow_deltalake::telemetry::kernel_metrics_reporter())
            .with(tracing_subscriber::fmt::layer().json().with_filter(filter))
            .try_init()
            .map_err(|error| anyhow::anyhow!("initialize JSON tracing: {error}"))?;
    } else {
        tracing_subscriber::registry()
            .with_metrics_reporter_layer(arrow_deltalake::telemetry::kernel_metrics_reporter())
            .with(tracing_subscriber::fmt::layer().with_filter(filter))
            .try_init()
            .map_err(|error| anyhow::anyhow!("initialize tracing: {error}"))?;
    }
    Ok(())
}
