//! `delta-maintenance` command line: dry-run plans by default, `--apply` to mutate.

use anyhow::{Context, Result, anyhow};
use arrow_deltalake::maintenance::{Granularity, OptimizeConfig};
use clap::Parser;
use delta_kernel::metrics::WithMetricsReporterLayer;
use delta_maintenance::{Metrics, Operation, Settings, run, serve_metrics};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;
use url::Url;

const MIB: u64 = 1024 * 1024;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Bounded Delta OPTIMIZE, PURGE and VACUUM LITE over allowlisted tables"
)]
struct Cli {
    /// Root URL containing the allowlisted tables (file:///..., abfss://...).
    #[arg(long)]
    root: Url,
    /// Allowlisted table directory names under the root (repeatable).
    #[arg(long = "table", required = true)]
    tables: Vec<String>,
    /// Operations to run per table, in optimize, purge, vacuum order (repeatable).
    #[arg(long = "operation", value_enum, required = true)]
    operations: Vec<Operation>,
    /// Mutate the tables. Without this flag only dry-run plans are reported.
    #[arg(long)]
    apply: bool,
    /// Run on a schedule with this interval; omitted means one-shot.
    #[arg(long)]
    interval_secs: Option<u64>,
    /// Upper bound of the idle exponential backoff.
    #[arg(long, default_value_t = 3600)]
    max_idle_backoff_secs: u64,
    /// Stop a scheduled service after this many cycles.
    #[arg(long)]
    max_cycles: Option<u64>,
    #[arg(long, default_value_t = 1)]
    max_concurrent_tables: usize,
    /// Per-operation time budget; work stops between bounded steps once exceeded.
    #[arg(long, default_value_t = 900)]
    operation_timeout_secs: u64,
    /// Ingestion partition granularity (minute, hour, day).
    #[arg(long)]
    granularity: Option<String>,
    #[arg(long)]
    purge_retention_secs: Option<u64>,
    /// Explicit UTC anchor in Unix milliseconds; defaults to the wall clock.
    #[arg(long)]
    anchor_ms: Option<i64>,
    #[arg(long, default_value_t = 7 * 24 * 3600)]
    vacuum_retention_secs: u64,
    /// Allow VACUUM retention below 7 days on a marked disposable temp root only.
    #[arg(long)]
    unsafe_short_vacuum_for_tests: bool,
    #[arg(long, default_value_t = 128)]
    target_file_mib: u64,
    #[arg(long)]
    latest_partitions: Option<usize>,
    /// Prometheus listen address.
    #[arg(long, default_value = "127.0.0.1:9467")]
    listen: SocketAddr,
    #[arg(long)]
    no_metrics_listener: bool,
}

fn millis(secs: u64) -> Result<i64> {
    i64::try_from(secs.checked_mul(1000).context("duration overflow")?).context("duration overflow")
}

fn settings(cli: Cli) -> Result<Settings> {
    let target = cli.target_file_mib.checked_mul(MIB).context("target overflow")?;
    let defaults = OptimizeConfig::default();
    let settings = Settings {
        root: cli.root,
        tables: cli.tables,
        operations: cli.operations,
        apply: cli.apply,
        interval: cli.interval_secs.map(Duration::from_secs),
        max_idle_backoff: Duration::from_secs(cli.max_idle_backoff_secs),
        max_cycles: cli.max_cycles,
        max_concurrent_tables: cli.max_concurrent_tables,
        operation_timeout: Duration::from_secs(cli.operation_timeout_secs),
        granularity: cli.granularity.as_deref().map(Granularity::parse_name).transpose()?,
        purge_retention_ms: cli.purge_retention_secs.map(millis).transpose()?,
        anchor_ms: cli.anchor_ms,
        vacuum_retention_ms: millis(cli.vacuum_retention_secs)?,
        unsafe_short_vacuum_for_tests: cli.unsafe_short_vacuum_for_tests,
        optimize: OptimizeConfig {
            target_file_bytes: target,
            small_file_bytes: (target / 2).max(1),
            latest_partitions: cli.latest_partitions,
            ..defaults
        },
    };
    settings.validate()?;
    Ok(settings)
}

/// Install the OTLP http/protobuf meter provider when an endpoint is configured,
/// then the Kernel metrics layer on the global tracing subscriber.
fn telemetry() -> Result<Option<SdkMeterProvider>> {
    let provider = if std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
        || std::env::var_os("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").is_some()
    {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build()?;
        let reader = PeriodicReader::builder(exporter)
            .with_interval(Duration::from_millis(2000))
            .build();
        let provider = SdkMeterProvider::builder()
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name("delta-maintenance")
                    .build(),
            )
            .with_reader(reader)
            .build();
        opentelemetry::global::set_meter_provider(provider.clone());
        Some(provider)
    } else {
        None
    };
    let filter = EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("warn"))?;
    tracing_subscriber::registry()
        .with_metrics_reporter_layer(arrow_deltalake::telemetry::kernel_metrics_reporter())
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(filter),
        )
        .try_init()
        .map_err(|error| anyhow!("install tracing subscriber: {error}"))?;
    Ok(provider)
}

fn main() -> ExitCode {
    match main_inner() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(error) => {
            eprintln!("delta-maintenance: {error:#}");
            ExitCode::from(2)
        }
    }
}

fn main_inner() -> Result<bool> {
    let cli = Cli::parse();
    let (listen, no_listener) = (cli.listen, cli.no_metrics_listener);
    let settings = settings(cli)?;
    let provider = telemetry()?;
    let cancel = Arc::new(AtomicBool::new(false));
    let handler = Arc::clone(&cancel);
    ctrlc::set_handler(move || handler.store(true, Ordering::SeqCst))?;
    let metrics = Metrics::new();
    let listener = if settings.interval.is_some() && !no_listener {
        Some(serve_metrics(listen, metrics.clone(), Arc::clone(&cancel))?)
    } else {
        None
    };
    let healthy = run(&settings, &metrics, &cancel);
    cancel.store(true, Ordering::SeqCst);
    if let Some(listener) = listener {
        listener.join().map_err(|_| anyhow!("metrics listener panicked"))?;
    }
    // Every table has been dropped; an undrained or uncertain member stays registered.
    arrow_deltalake::publisher::PublisherSession::close_shared()
        .map_err(|error| anyhow!("publisher session close failed; registration retained: {error:#}"))?;
    if let Some(provider) = provider {
        provider
            .shutdown()
            .map_err(|error| anyhow!("telemetry shutdown failed: {error}"))?;
    }
    Ok(healthy)
}
