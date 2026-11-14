//! `delta-maintenance` command line: dry-run plans by default, `--apply` to mutate.

use anyhow::{Context, Result, anyhow};
use arrow_deltalake::maintenance::{Granularity, OptimizeConfig};
use clap::Parser;
use delta_kernel::metrics::WithMetricsReporterLayer;
use delta_maintenance::{Metrics, Operation, Settings, run, serve_metrics, startup_configuration};
use delta_maintenance::{catalog::CatalogDiscovery, run_with_discovery};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use std::collections::BTreeMap;
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
    about = "Bounded Delta OPTIMIZE, PURGE, VACUUM LITE, checkpoint and proven log pruning over allowlisted tables"
)]
struct Cli {
    /// Root URL containing the allowlisted tables (file:///..., abfss://...).
    #[arg(long, env = "ARROWCOL_MAINT_ROOT", required_unless_present = "catalog_endpoint")]
    root: Option<Url>,
    /// Shared schema-catalog gRPC endpoint (for example http://catalog:8817).
    #[arg(long, env = "ARROWCOL_MAINT_CATALOG_ENDPOINT")]
    catalog_endpoint: Option<String>,
    /// Allowlisted table directory names under the root (repeatable).
    #[arg(long = "table", env = "ARROWCOL_MAINT_TABLES", value_delimiter = ',')]
    tables: Vec<String>,
    /// Operations to run per table, in purge, optimize, vacuum, checkpoint, prune-log order (repeatable).
    #[arg(
        long = "operation",
        env = "ARROWCOL_MAINT_OPERATIONS",
        value_delimiter = ',',
        value_enum,
        required = true
    )]
    operations: Vec<Operation>,
    /// Mutate the tables. Without this flag only dry-run plans are reported.
    #[arg(long, env = "ARROWCOL_MAINT_APPLY")]
    apply: bool,
    /// Run on a schedule with this interval; omitted means one-shot.
    #[arg(long, env = "ARROWCOL_MAINT_INTERVAL_SECS")]
    interval_secs: Option<u64>,
    /// Upper bound of the idle exponential backoff.
    #[arg(long, env = "ARROWCOL_MAINT_MAX_IDLE_BACKOFF_SECS", default_value_t = 3600)]
    max_idle_backoff_secs: u64,
    /// Stop a scheduled service after this many cycles.
    #[arg(long, env = "ARROWCOL_MAINT_MAX_CYCLES")]
    max_cycles: Option<u64>,
    #[arg(long, env = "ARROWCOL_MAINT_MAX_CONCURRENT_TABLES", default_value_t = 1)]
    max_concurrent_tables: usize,
    /// Bound concurrent per-bin decoded limits; this is not a process RSS limit.
    #[arg(long, env = "ARROWCOL_MAINT_MAX_CONCURRENT_DECODED_BYTES")]
    max_concurrent_decoded_bytes: Option<u64>,
    /// Per-operation time budget; work stops between bounded steps once exceeded.
    #[arg(long, env = "ARROWCOL_MAINT_OPERATION_TIMEOUT_SECS", default_value_t = 900)]
    operation_timeout_secs: u64,
    #[arg(long, env = "ARROWCOL_MAINT_COMMIT_ATTEMPTS", default_value_t = 8)]
    commit_attempts: u32,
    #[arg(long, env = "ARROWCOL_MAINT_COMMIT_BUDGET_SECS", default_value_t = 10)]
    commit_budget_secs: u64,
    /// Ingestion partition granularity (minute, hour, day).
    #[arg(long, env = "ARROWCOL_MAINT_GRANULARITY")]
    granularity: Option<String>,
    #[arg(long, env = "ARROWCOL_MAINT_PURGE_RETENTION_SECS")]
    purge_retention_secs: Option<u64>,
    /// Explicit UTC anchor in Unix milliseconds; defaults to the wall clock.
    #[arg(long, env = "ARROWCOL_MAINT_ANCHOR_MS")]
    anchor_ms: Option<i64>,
    #[arg(long, env = "ARROWCOL_MAINT_VACUUM_RETENTION_SECS", default_value_t = 7 * 24 * 3600)]
    vacuum_retention_secs: u64,
    /// DANGER: acknowledge that VACUUM below 7 days (including zero) can break old
    /// snapshot/QAP readers on ordinary local or Azure roots. Does NOT lower
    /// log retention or bypass active-file/publication safety checks.
    #[arg(long, env = "ARROWCOL_MAINT_ALLOW_SHORT_VACUUM_RETENTION")]
    allow_short_vacuum_retention: bool,
    /// Allow VACUUM retention below 7 days, and log retention below the table's
    /// `delta.logRetentionDuration`, on a marked disposable temp root only.
    #[arg(long, env = "ARROWCOL_MAINT_UNSAFE_SHORT_VACUUM_FOR_TESTS")]
    unsafe_short_vacuum_for_tests: bool,
    #[arg(long, env = "ARROWCOL_MAINT_TARGET_FILE_MIB", default_value_t = 128)]
    target_file_mib: u64,
    #[arg(long, env = "ARROWCOL_MAINT_SMALL_FILE_BYTES")]
    small_file_bytes: Option<u64>,
    #[arg(long, env = "ARROWCOL_MAINT_MIN_FILES")]
    min_files: Option<usize>,
    #[arg(long, env = "ARROWCOL_MAINT_VACUUM_MAX_DELETES")]
    vacuum_max_deletes: Option<usize>,
    #[arg(long, env = "ARROWCOL_MAINT_VACUUM_MAX_LISTED_FILES")]
    vacuum_max_listed_files: Option<usize>,
    #[arg(long, env = "ARROWCOL_MAINT_LAYOUT_ENABLED")]
    layout_enabled: Option<bool>,
    #[arg(long, env = "ARROWCOL_MAINT_LAYOUT_SORT_COLUMNS")]
    layout_sort_columns: Option<String>,
    #[arg(long, env = "ARROWCOL_MAINT_LAYOUT_BLOOM_COLUMNS")]
    layout_bloom_columns: Option<String>,
    #[arg(long, env = "ARROWCOL_MAINT_LAYOUT_MAX_ROWS")]
    layout_max_rows: Option<usize>,
    #[arg(long, env = "ARROWCOL_MAINT_LAYOUT_MAX_INPUT_BYTES")]
    layout_max_input_bytes: Option<usize>,
    #[arg(long, env = "ARROWCOL_MAINT_LAYOUT_ROW_GROUP_ROWS")]
    layout_row_group_rows: Option<usize>,
    #[arg(long, env = "ARROWCOL_MAINT_LAYOUT_PAGE_ROWS")]
    layout_page_rows: Option<usize>,
    #[arg(long, env = "ARROWCOL_MAINT_MAX_FILES_PER_BIN", default_value_t = 64)]
    max_files_per_bin: usize,
    #[arg(long, env = "ARROWCOL_MAINT_MAX_BINS_PER_COMMIT", default_value_t = 16)]
    max_bins_per_commit: usize,
    #[arg(long, env = "ARROWCOL_MAINT_MAX_ACTIONS_PER_COMMIT", default_value_t = 2048)]
    max_actions_per_commit: usize,
    #[arg(long, env = "ARROWCOL_MAINT_MAX_PLAN_BYTES", default_value_t = 1024 * 1024)]
    max_plan_bytes: usize,
    #[arg(long, env = "ARROWCOL_MAINT_MAX_DECODED_BYTES_PER_BIN", default_value_t = 1024 * 1024 * 1024)]
    max_decoded_bytes_per_bin: u64,
    #[arg(long, env = "ARROWCOL_MAINT_MAX_ROW_GROUP_BYTES", default_value_t = 128 * 1024 * 1024)]
    max_row_group_bytes: u64,
    #[arg(long, env = "ARROWCOL_MAINT_MAX_BATCH_BYTES", default_value_t = 64 * 1024 * 1024)]
    max_batch_bytes: usize,
    #[arg(long, env = "ARROWCOL_MAINT_BATCH_ROWS", default_value_t = 8192)]
    batch_rows: usize,
    #[arg(long, env = "ARROWCOL_MAINT_OUTPUT_ROW_GROUP_ROWS", default_value_t = 65_536)]
    output_row_group_rows: usize,
    #[arg(long, env = "ARROWCOL_MAINT_MAX_OUTPUT_FILE_BYTES", default_value_t = 1023 * 1024 * 1024)]
    max_output_file_bytes: u64,
    #[arg(long, env = "ARROWCOL_MAINT_MAX_ACTIVE_FILES", default_value_t = 1_000_000)]
    max_active_files: usize,
    #[arg(long, env = "ARROWCOL_MAINT_LATEST_PARTITIONS")]
    latest_partitions: Option<usize>,
    /// Include newest/open OPTIMIZE partitions; caller supplies lifecycle proof.
    /// One-shot optimize only, in either dry-run or apply mode.
    #[arg(long, env = "ARROWCOL_MAINT_SEALED_FINAL")]
    sealed_final: bool,
    /// `checkpoint` writes once this many commits follow the hinted checkpoint.
    #[arg(long, env = "ARROWCOL_MAINT_CHECKPOINT_EVERY_COMMITS", default_value_t = 100)]
    checkpoint_every_commits: u64,
    /// `prune-log` minimum commit age; defaults to `delta.logRetentionDuration` (else 30 days).
    #[arg(long, env = "ARROWCOL_MAINT_LOG_RETENTION_SECS")]
    log_retention_secs: Option<u64>,
    /// Maximum log objects one `prune-log` apply deletes; the rest is backlog.
    #[arg(long, env = "ARROWCOL_MAINT_MAX_PRUNE_FILES", default_value_t = 10_000)]
    max_prune_files: usize,
    /// Prometheus listen address.
    #[arg(long, env = "ARROWCOL_MAINT_LISTEN", default_value = "127.0.0.1:9467")]
    listen: SocketAddr,
    #[arg(long, env = "ARROWCOL_MAINT_NO_METRICS_LISTENER")]
    no_metrics_listener: bool,
}

fn millis(secs: u64) -> Result<i64> {
    i64::try_from(secs.checked_mul(1000).context("duration overflow")?).context("duration overflow")
}

fn settings(cli: Cli) -> Result<Settings> {
    let catalog = cli.catalog_endpoint.is_some();
    let target = cli.target_file_mib.checked_mul(MIB).context("target overflow")?;
    let defaults = OptimizeConfig::default();
    let mut process_properties = BTreeMap::new();
    for (key, value) in [
        (
            "arrowcol.optimize.smallFileSize",
            cli.small_file_bytes.map(|v| v.to_string()),
        ),
        ("arrowcol.optimize.minFiles", cli.min_files.map(|v| v.to_string())),
        (
            "arrowcol.vacuum.maxDeletes",
            cli.vacuum_max_deletes.map(|v| v.to_string()),
        ),
        (
            "arrowcol.vacuum.maxListedFiles",
            cli.vacuum_max_listed_files.map(|v| v.to_string()),
        ),
        ("arrowcol.layout.enabled", cli.layout_enabled.map(|v| v.to_string())),
        ("arrowcol.layout.sortColumns", cli.layout_sort_columns.clone()),
        ("arrowcol.layout.bloomColumns", cli.layout_bloom_columns.clone()),
        ("arrowcol.layout.maxRows", cli.layout_max_rows.map(|v| v.to_string())),
        (
            "arrowcol.layout.maxInputSize",
            cli.layout_max_input_bytes.map(|v| v.to_string()),
        ),
        (
            "arrowcol.layout.rowGroupRows",
            cli.layout_row_group_rows.map(|v| v.to_string()),
        ),
        ("arrowcol.layout.pageRows", cli.layout_page_rows.map(|v| v.to_string())),
    ] {
        if let Some(value) = value {
            process_properties.insert(key.to_owned(), value);
        }
    }
    let settings = Settings {
        root: match cli.root {
            Some(root) => root,
            None if catalog => Url::parse("file:///")?,
            None => return Err(anyhow!("--root is required without --catalog-endpoint")),
        },
        tables: cli.tables,
        operations: cli.operations,
        apply: cli.apply,
        interval: cli.interval_secs.map(Duration::from_secs),
        max_idle_backoff: Duration::from_secs(cli.max_idle_backoff_secs),
        max_cycles: cli.max_cycles,
        max_concurrent_tables: cli.max_concurrent_tables,
        max_concurrent_decoded_bytes: cli.max_concurrent_decoded_bytes,
        operation_timeout: Duration::from_secs(cli.operation_timeout_secs),
        commit_attempts: cli.commit_attempts,
        commit_budget: Duration::from_secs(cli.commit_budget_secs),
        granularity: cli
            .granularity
            .as_deref()
            .map(Granularity::parse_name)
            .transpose()?
            .or(catalog.then_some(Granularity::Hour)),
        purge_retention_ms: cli
            .purge_retention_secs
            .map(millis)
            .transpose()?
            .or(catalog.then_some(30 * 24 * 60 * 60 * 1000)),
        anchor_ms: cli.anchor_ms,
        vacuum_retention_ms: millis(cli.vacuum_retention_secs)?,
        allow_short_vacuum_retention: cli.allow_short_vacuum_retention,
        unsafe_short_vacuum_for_tests: cli.unsafe_short_vacuum_for_tests,
        checkpoint_every_commits: cli.checkpoint_every_commits,
        log_retention_ms: cli.log_retention_secs.map(millis).transpose()?,
        max_prune_files: cli.max_prune_files,
        process_properties,
        optimize: OptimizeConfig {
            target_file_bytes: target,
            small_file_bytes: cli.small_file_bytes.unwrap_or_else(|| (target / 2).max(1)),
            max_files_per_bin: cli.max_files_per_bin,
            max_bins_per_commit: cli.max_bins_per_commit,
            max_actions_per_commit: cli.max_actions_per_commit,
            max_plan_bytes: cli.max_plan_bytes,
            max_decoded_bytes_per_bin: cli.max_decoded_bytes_per_bin,
            max_row_group_bytes: cli.max_row_group_bytes,
            max_batch_bytes: cli.max_batch_bytes,
            batch_rows: cli.batch_rows,
            output_row_group_rows: cli.output_row_group_rows,
            max_output_file_bytes: cli.max_output_file_bytes,
            max_active_files: cli.max_active_files,
            latest_partitions: cli.latest_partitions,
            sealed_final: cli.sealed_final,
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
    let catalog_endpoint = cli.catalog_endpoint.clone();
    let (listen, no_listener) = (cli.listen, cli.no_metrics_listener);
    let settings = settings(cli)?;
    eprintln!(
        "arrowcol_maintenance_effective_config {}",
        startup_configuration(&settings)?
    );
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
    let healthy = if let Some(endpoint) = catalog_endpoint {
        let discovery = CatalogDiscovery::connect(&endpoint, settings.tables.clone())?;
        run_with_discovery(&settings, &discovery, &metrics, &cancel)
    } else {
        run(&settings, &metrics, &cancel)
    };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_limits_are_validated_and_propagated() -> Result<()> {
        let args = [
            "delta-maintenance",
            "--root",
            "file:///tmp/",
            "--operation",
            "optimize",
            "--max-files-per-bin",
            "7",
            "--min-files",
            "3",
            "--output-row-group-rows",
            "1024",
        ];
        let config = settings(Cli::try_parse_from(args)?)?;
        assert_eq!(config.optimize.max_files_per_bin, 7);
        assert_eq!(config.optimize.output_row_group_rows, 1024);
        assert_eq!(
            config
                .process_properties
                .get("arrowcol.optimize.minFiles")
                .map(String::as_str),
            Some("3")
        );
        let invalid = [
            "delta-maintenance",
            "--root",
            "file:///tmp/",
            "--operation",
            "optimize",
            "--output-row-group-rows",
            "0",
        ];
        assert!(settings(Cli::try_parse_from(invalid)?).is_err());
        let invalid_retries = [
            "delta-maintenance",
            "--root",
            "file:///tmp/",
            "--operation",
            "optimize",
            "--commit-attempts",
            "0",
        ];
        assert!(settings(Cli::try_parse_from(invalid_retries)?).is_err());
        Ok(())
    }
}
