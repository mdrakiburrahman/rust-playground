//! Standalone Delta maintenance service: bounded OPTIMIZE, PURGE, VACUUM LITE,
//! checkpointing and proven `_delta_log` pruning over an allowlist of tables,
//! dry-run by default, with Prometheus metrics.

use anyhow::{Context, Result, bail};
use arrow_deltalake::delta::DeltaTable;
use arrow_deltalake::maintenance::{
    Granularity, MaintenanceControl, MaintenanceErrorKind, MaintenancePlan, MaintenanceReport, NegativeProbeConfig,
    OptimizeConfig, OptimizePlan, Outcome, ProbeOperation, PruneConfig, PurgeConfig, VacuumConfig, error_details,
    now_ms,
};
use arrowcol_props::{ResolvedProps, resolve};
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use url::Url;

/// Maximum number of allowlisted tables per service instance.
pub const MAX_TABLES: usize = 256;
/// Maximum concurrently maintained tables.
pub const MAX_CONCURRENT_TABLES: usize = 16;
const MAX_HTTP_REQUEST: usize = 8 * 1024;

/// Maintenance operation, run one at a time per table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Optimize,
    Purge,
    Vacuum,
    /// Kernel checkpoint once enough commits accumulate; never deletes history.
    Checkpoint,
    /// Delete `_delta_log` history proven equivalent to a checkpoint.
    PruneLog,
}

impl Operation {
    const ALL: [Self; 5] = [
        Self::Purge,
        Self::Optimize,
        Self::Vacuum,
        Self::Checkpoint,
        Self::PruneLog,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::Optimize => "optimize",
            Self::Purge => "purge",
            Self::Vacuum => "vacuum",
            Self::Checkpoint => "checkpoint",
            Self::PruneLog => "prune-log",
        }
    }
}

/// Validated service settings.
#[derive(Clone, Debug)]
pub struct Settings {
    pub root: Url,
    pub tables: Vec<String>,
    pub operations: Vec<Operation>,
    pub apply: bool,
    pub interval: Option<Duration>,
    pub max_idle_backoff: Duration,
    pub max_cycles: Option<u64>,
    pub max_concurrent_tables: usize,
    pub operation_timeout: Duration,
    pub granularity: Option<Granularity>,
    pub purge_retention_ms: Option<i64>,
    pub anchor_ms: Option<i64>,
    pub vacuum_retention_ms: i64,
    /// Explicit acknowledgement of old snapshot/QAP reader risk; VACUUM only.
    pub allow_short_vacuum_retention: bool,
    pub unsafe_short_vacuum_for_tests: bool,
    pub optimize: OptimizeConfig,
    /// Minimum commits after the hinted checkpoint before `checkpoint` writes one.
    pub checkpoint_every_commits: u64,
    /// `prune-log` commit-age retention; `None` uses `delta.logRetentionDuration`
    /// (default 30 days). Shorter values need the unsafe test override.
    pub log_retention_ms: Option<i64>,
    /// Maximum log objects one `prune-log` apply deletes.
    pub max_prune_files: usize,
}

impl Settings {
    /// Reject unbounded or unsafe settings before touching any table.
    ///
    /// # Errors
    /// Returns a description of the first invalid setting.
    pub fn validate(&self) -> Result<()> {
        if self.optimize.sealed_final
            && (self.interval.is_some() || self.max_cycles.is_some() || self.operations != [Operation::Optimize])
        {
            bail!("--sealed-final requires one-shot optimize only, without --interval-secs or --max-cycles");
        }
        if !matches!(self.root.scheme(), "file" | "abfss" | "az" | "azure" | "https") {
            bail!("unsupported root URL scheme {}", self.root.scheme());
        }
        if self.tables.len() > MAX_TABLES || (self.tables.is_empty() && self.root.scheme() != "file") {
            bail!("at most {MAX_TABLES} tables; non-filesystem roots require a catalog or explicit table allowlist");
        }
        for name in &self.tables {
            let valid = !name.is_empty()
                && name.len() <= 128
                && name != "."
                && name != ".."
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
            if !valid {
                bail!("invalid table name {name:?}: use [A-Za-z0-9_.-] without traversal");
            }
        }
        if self.checkpoint_every_commits == 0 {
            bail!("--checkpoint-every-commits must be nonzero");
        }
        if self.max_prune_files == 0 {
            bail!("--max-prune-files must be nonzero");
        }
        if self.log_retention_ms.is_some_and(|retention| retention < 0) {
            bail!("log retention must not be negative");
        }
        if self.operations.is_empty() {
            bail!("at least one --operation is required");
        }
        if self.max_concurrent_tables == 0 || self.max_concurrent_tables > MAX_CONCURRENT_TABLES {
            bail!("max concurrent tables must be within 1..={MAX_CONCURRENT_TABLES}");
        }
        if self.operation_timeout.is_zero() || self.interval.is_some_and(|interval| interval.is_zero()) {
            bail!("timeouts and intervals must be positive");
        }
        if self.interval.is_some_and(|interval| self.max_idle_backoff < interval) {
            bail!("max idle backoff must be at least the interval");
        }
        if self.operations.contains(&Operation::Purge)
            && (self.granularity.is_none() || self.purge_retention_ms.is_none())
        {
            bail!("purge requires --granularity and --purge-retention-secs");
        }
        if self.purge_retention_ms.is_some_and(|retention| retention <= 0) {
            bail!("purge retention must be positive");
        }
        if self.vacuum_retention_ms < 0 {
            bail!("VACUUM retention must not be negative");
        }
        self.optimize.validate()
    }
}

/// Current catalog identity and properties for one table.
#[derive(Clone, Debug)]
pub struct DiscoveredTable {
    pub name: String,
    pub url: Url,
    pub properties: BTreeMap<String, String>,
    /// Filesystem property reads are bound to a log version; catalog reads may
    /// carry their own stronger event-stream consistency contract instead.
    pub log_version: Option<u64>,
}

/// The catalog client implements this interface without filesystem discovery.
pub trait TableDiscovery: Sync {
    /// List table identities for this service's bounded pass.
    ///
    /// # Errors
    /// Returns catalog or filesystem discovery failures.
    fn tables(&self) -> Result<Vec<String>>;

    /// Fetch current table location and properties.
    ///
    /// # Errors
    /// Returns missing-table, corrupt metadata or authority failures.
    fn table(&self, name: &str) -> Result<DiscoveredTable>;
}

type PropertyCache = BTreeMap<String, (u64, BTreeMap<String, String>)>;

/// Compatibility discovery for local roots and explicit cloud allowlists.
/// Only metadata is cached, fenced by the current log version each pass.
pub struct FilesystemDiscovery {
    root: Url,
    allowlist: Vec<String>,
    cache: Mutex<PropertyCache>,
}

impl FilesystemDiscovery {
    #[must_use]
    pub fn new(root: Url, allowlist: Vec<String>) -> Self {
        Self {
            root,
            allowlist,
            cache: Mutex::new(BTreeMap::new()),
        }
    }

    fn url(&self, name: &str) -> Result<Url> {
        let mut root = self.root.clone();
        if !root.path().ends_with('/') {
            root.set_path(&format!("{}/", root.path()));
        }
        Ok(root.join(&format!("{name}/"))?)
    }
}

impl TableDiscovery for FilesystemDiscovery {
    fn tables(&self) -> Result<Vec<String>> {
        if !self.allowlist.is_empty() {
            return Ok(self.allowlist.clone());
        }
        let root = self
            .root
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("catalog required for cloud discovery"))?;
        let mut names = Vec::new();
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() && entry.path().join("_delta_log").is_dir() {
                names.push(
                    entry
                        .file_name()
                        .into_string()
                        .map_err(|_| anyhow::anyhow!("non-UTF8 table name"))?,
                );
            }
            if names.len() > MAX_TABLES {
                bail!("discovery exceeds {MAX_TABLES} tables; configure an allowlist");
            }
        }
        names.sort();
        Ok(names)
    }

    fn table(&self, name: &str) -> Result<DiscoveredTable> {
        let url = self.url(name)?;
        let probe = DeltaTable::open_for_maintenance_probe(&url)?;
        let version = probe.probe_log_version(100_000)?;
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| anyhow::anyhow!("property cache poisoned"))?;
        let properties = match cache.get(name) {
            Some((cached_version, properties)) if *cached_version == version => properties.clone(),
            _ => {
                let properties = probe.maintenance_properties()?;
                if probe.probe_log_version(100_000)? != version {
                    bail!("table advanced during property discovery; retry");
                }
                if cache.len() >= MAX_TABLES && !cache.contains_key(name) {
                    cache.clear();
                }
                cache.insert(name.into(), (version, properties.clone()));
                properties
            }
        };
        Ok(DiscoveredTable {
            name: name.into(),
            url,
            properties,
            log_version: Some(version),
        })
    }
}

fn process_defaults(settings: &Settings) -> BTreeMap<String, String> {
    let mut props = BTreeMap::new();
    for (key, value) in [
        (
            "arrowcol.optimize.enabled",
            settings.operations.contains(&Operation::Optimize).to_string(),
        ),
        (
            "arrowcol.purge.enabled",
            settings.operations.contains(&Operation::Purge).to_string(),
        ),
        (
            "arrowcol.vacuum.enabled",
            settings.operations.contains(&Operation::Vacuum).to_string(),
        ),
        (
            "arrowcol.optimize.targetFileSize",
            settings.optimize.target_file_bytes.to_string(),
        ),
        (
            "arrowcol.optimize.smallFileSize",
            settings.optimize.small_file_bytes.to_string(),
        ),
        (
            "arrowcol.optimize.maxFilesPerBin",
            settings.optimize.max_files_per_bin.to_string(),
        ),
        (
            "arrowcol.optimize.maxBinsPerCommit",
            settings.optimize.max_bins_per_commit.to_string(),
        ),
        (
            "arrowcol.optimize.maxActionsPerCommit",
            settings.optimize.max_actions_per_commit.to_string(),
        ),
        (
            "arrowcol.optimize.maxPlanSize",
            settings.optimize.max_plan_bytes.to_string(),
        ),
        (
            "arrowcol.optimize.maxDecodedSizePerBin",
            settings.optimize.max_decoded_bytes_per_bin.to_string(),
        ),
        (
            "arrowcol.optimize.maxOutputFileSize",
            settings.optimize.max_output_file_bytes.to_string(),
        ),
        (
            "arrowcol.optimize.maxActiveFiles",
            settings.optimize.max_active_files.to_string(),
        ),
        (
            "arrowcol.optimize.maxRowGroupSize",
            settings.optimize.max_row_group_bytes.to_string(),
        ),
        (
            "arrowcol.optimize.maxBatchSize",
            settings.optimize.max_batch_bytes.to_string(),
        ),
        ("arrowcol.optimize.batchRows", settings.optimize.batch_rows.to_string()),
        (
            "arrowcol.optimize.latestPartitions",
            settings.optimize.latest_partitions.unwrap_or(0).to_string(),
        ),
        (
            "arrowcol.optimize.sealedPartitionsOnly",
            (!settings.optimize.sealed_final).to_string(),
        ),
        (
            "delta.checkpointInterval",
            settings.checkpoint_every_commits.to_string(),
        ),
        (
            "arrowcol.maintenance.operationTimeout",
            settings.operation_timeout.as_millis().to_string(),
        ),
    ] {
        props.insert(key.into(), value);
    }
    if let Some(granularity) = settings.granularity {
        props.insert(
            "arrowcol.partition.granularity".into(),
            match granularity {
                Granularity::Minute => "minute",
                Granularity::Hour => "hour",
                Granularity::Day => "day",
            }
            .into(),
        );
    }
    if settings.vacuum_retention_ms >= arrow_deltalake::maintenance::MIN_VACUUM_RETENTION_MS {
        props.insert(
            "arrowcol.vacuum.retention".into(),
            settings.vacuum_retention_ms.to_string(),
        );
        props.insert(
            "delta.deletedFileRetentionDuration".into(),
            settings.vacuum_retention_ms.to_string(),
        );
    }
    if let Some(retention) = settings.purge_retention_ms {
        props.insert("arrowcol.purge.retention".into(), retention.to_string());
    }
    if let Some(retention) = settings.log_retention_ms
        && retention >= arrow_deltalake::maintenance::MIN_VACUUM_RETENTION_MS
    {
        props.insert("delta.logRetentionDuration".into(), retention.to_string());
    }
    if let Some(interval) = settings.interval {
        props.insert("arrowcol.maintenance.interval".into(), interval.as_millis().to_string());
    }
    props
}

fn effective_settings(settings: &Settings, properties: &BTreeMap<String, String>) -> Result<(Settings, ResolvedProps)> {
    let props = resolve(properties, &process_defaults(settings))?;
    let mut effective = settings.clone();
    if !props.boolean("arrowcol.maintenance.enabled")? {
        effective.operations.clear();
        return Ok((effective, props));
    }
    let optimize_enabled = props.boolean("arrowcol.optimize.enabled")?;
    let purge_enabled = props.boolean("arrowcol.purge.enabled")?;
    let vacuum_enabled = props.boolean("arrowcol.vacuum.enabled")?;
    effective.operations.retain(|operation| match operation {
        Operation::Optimize => optimize_enabled,
        Operation::Purge => purge_enabled,
        Operation::Vacuum => vacuum_enabled,
        _ => true,
    });
    effective.granularity = Some(Granularity::parse_name(props.text("arrowcol.partition.granularity")?)?);
    effective.purge_retention_ms = Some(i64::try_from(props.unsigned("arrowcol.purge.retention")?)?);
    // CLI-only disposable-test overrides remain explicit invocation controls.
    // They cannot be enabled, persisted or weakened by table properties.
    if properties.contains_key("arrowcol.vacuum.retention")
        || settings.vacuum_retention_ms >= arrow_deltalake::maintenance::MIN_VACUUM_RETENTION_MS
    {
        effective.vacuum_retention_ms = i64::try_from(props.unsigned("arrowcol.vacuum.retention")?)?;
    }
    if properties.contains_key("delta.logRetentionDuration") || !settings.unsafe_short_vacuum_for_tests {
        effective.log_retention_ms = Some(i64::try_from(props.unsigned("delta.logRetentionDuration")?)?);
    }
    effective.checkpoint_every_commits = props.unsigned("delta.checkpointInterval")?;
    effective.operation_timeout = Duration::from_millis(props.unsigned("arrowcol.maintenance.operationTimeout")?);
    effective.optimize.target_file_bytes = props.unsigned("arrowcol.optimize.targetFileSize")?;
    effective.optimize.small_file_bytes = props.unsigned("arrowcol.optimize.smallFileSize")?;
    effective.optimize.max_files_per_bin = usize::try_from(props.unsigned("arrowcol.optimize.maxFilesPerBin")?)?;
    effective.optimize.max_bins_per_commit = usize::try_from(props.unsigned("arrowcol.optimize.maxBinsPerCommit")?)?;
    effective.optimize.max_actions_per_commit =
        usize::try_from(props.unsigned("arrowcol.optimize.maxActionsPerCommit")?)?;
    effective.optimize.max_plan_bytes = usize::try_from(props.unsigned("arrowcol.optimize.maxPlanSize")?)?;
    effective.optimize.max_decoded_bytes_per_bin = props.unsigned("arrowcol.optimize.maxDecodedSizePerBin")?;
    effective.optimize.max_output_file_bytes = props.unsigned("arrowcol.optimize.maxOutputFileSize")?;
    effective.optimize.max_active_files = usize::try_from(props.unsigned("arrowcol.optimize.maxActiveFiles")?)?;
    effective.optimize.max_row_group_bytes = props.unsigned("arrowcol.optimize.maxRowGroupSize")?;
    effective.optimize.max_batch_bytes = usize::try_from(props.unsigned("arrowcol.optimize.maxBatchSize")?)?;
    effective.optimize.batch_rows = usize::try_from(props.unsigned("arrowcol.optimize.batchRows")?)?;
    let latest = usize::try_from(props.unsigned("arrowcol.optimize.latestPartitions")?)?;
    effective.optimize.latest_partitions = (latest > 0).then_some(latest);
    effective.optimize.sealed_final = !props.boolean("arrowcol.optimize.sealedPartitionsOnly")?;
    if props.boolean("arrowcol.layout.enabled")? {
        effective.optimize.layout = Some(arrow_deltalake::layout::LayoutPolicy::from_properties(&props)?);
    }
    effective.optimize.validate()?;
    Ok((effective, props))
}

fn table_gate(name: &str) -> &'static Mutex<()> {
    use std::hash::{Hash, Hasher};
    static GATES: OnceLock<[Mutex<()>; MAX_TABLES]> = OnceLock::new();
    let gates = GATES.get_or_init(|| std::array::from_fn(|_| Mutex::new(())));
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut hash);
    &gates[(hash.finish() % MAX_TABLES as u64) as usize]
}

/// Result of one operation on one table.
#[derive(Clone, Debug, Serialize)]
pub struct OperationResult {
    pub table: String,
    pub operation: Operation,
    pub applied: bool,
    /// This pass proved no useful work without loading a Kernel snapshot.
    pub negative_probe: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<MaintenanceReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optimize: Option<OptimizeEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vacuum: Option<VacuumEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub duration_ms: u64,
}

/// Effective VACUUM retention and the caller's per-invocation acknowledgements.
#[derive(Clone, Debug, Serialize)]
pub struct VacuumEvidence {
    pub retention_ms: i64,
    pub allow_short_vacuum_retention: bool,
    pub unsafe_short_vacuum_for_tests: bool,
}

/// Exact planner inputs and effective bounds, retained even if applying fails.
#[derive(Clone, Debug, Serialize)]
pub struct OptimizeEvidence {
    pub config: serde_json::Value,
    pub plan: OptimizePlan,
}

impl OptimizeEvidence {
    fn new(config: &OptimizeConfig, plan: OptimizePlan) -> Self {
        Self {
            config: serde_json::json!({
                "sealed_final": config.sealed_final,
                "granularity": config.granularity,
                "now_ms": config.now_ms,
                "latest_partitions": config.latest_partitions,
                "target_file_bytes": config.target_file_bytes,
                "small_file_bytes": config.small_file_bytes,
                "max_files_per_bin": config.max_files_per_bin,
                "max_bins_per_commit": config.max_bins_per_commit,
                "max_actions_per_commit": config.max_actions_per_commit,
                "max_plan_bytes": config.max_plan_bytes,
                "max_decoded_bytes_per_bin": config.max_decoded_bytes_per_bin,
                "max_row_group_bytes": config.max_row_group_bytes,
                "max_batch_bytes": config.max_batch_bytes,
                "batch_rows": config.batch_rows,
                "max_output_file_bytes": config.max_output_file_bytes,
                "max_active_files": config.max_active_files,
                "layout": config.layout.as_ref().map(|layout| serde_json::json!({
                    "sort_columns": layout.sort_columns,
                    "max_rows": layout.max_rows,
                    "max_input_bytes": layout.max_input_bytes,
                    "row_group_rows": layout.row_group_rows,
                    "page_rows": layout.page_rows,
                    "bloom_columns": layout.bloom_columns,
                })),
            }),
            plan,
        }
    }
}

impl OperationResult {
    fn outcome(&self) -> &'static str {
        match (&self.report, self.error_kind) {
            (_, Some(kind)) => kind,
            (Some(report), None) => report.outcome.label(),
            (None, None) => "noop",
        }
    }

    fn did_work(&self) -> bool {
        self.report.as_ref().is_some_and(|report| {
            report.files_removed > 0 || report.files_added > 0 || report.files_deleted > 0 || report.backlog_files > 0
        })
    }
}

struct Instruments {
    runs: opentelemetry::metrics::Counter<u64>,
    totals: opentelemetry::metrics::Counter<u64>,
    backlog: opentelemetry::metrics::Gauge<u64>,
    cutoff: opentelemetry::metrics::Gauge<i64>,
    cycles: opentelemetry::metrics::Counter<u64>,
}

impl Instruments {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("delta-maintenance");
        Self {
            runs: meter.u64_counter("delta_maintenance.runs").build(),
            totals: meter.u64_counter("delta_maintenance.items").build(),
            backlog: meter.u64_gauge("delta_maintenance.backlog_files").build(),
            cutoff: meter
                .i64_gauge("delta_maintenance.retention_cutoff")
                .with_unit("ms")
                .build(),
            cycles: meter.u64_counter("delta_maintenance.cycles").build(),
        }
    }
}

#[derive(Default)]
struct Counters {
    runs: BTreeMap<(&'static str, &'static str), u64>,
    totals: BTreeMap<(&'static str, &'static str), u64>,
    gauges: BTreeMap<(&'static str, &'static str), i64>,
    heartbeat_ms: i64,
    cycles: u64,
}

/// Prometheus and OpenTelemetry metrics. Labels are restricted to the bounded
/// `operation` and `outcome` sets (item counters use the item name as outcome).
#[derive(Clone)]
pub struct Metrics(Arc<Mutex<Counters>>, Arc<Instruments>);

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create metrics bound to the current global OTel meter provider.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::default(), Arc::new(Instruments::new()))
    }

    fn with(&self, update: impl FnOnce(&mut Counters)) {
        if let Ok(mut counters) = self.0.lock() {
            update(&mut counters);
        }
    }

    fn heartbeat(&self) {
        let now = now_ms().unwrap_or_default();
        self.with(|counters| {
            counters.heartbeat_ms = now;
            counters.cycles += 1;
        });
        self.1.cycles.add(1, &[]);
    }

    fn record(&self, result: &OperationResult) {
        let operation = result.operation.label();
        let outcome = result.outcome();
        let op = opentelemetry::KeyValue::new("operation", operation);
        self.1
            .runs
            .add(1, &[op.clone(), opentelemetry::KeyValue::new("outcome", outcome)]);
        if let Some(report) = result
            .report
            .as_ref()
            .filter(|report| report.outcome != Outcome::Planned)
        {
            for (name, value) in report_items(report) {
                self.1
                    .totals
                    .add(value, &[op.clone(), opentelemetry::KeyValue::new("outcome", name)]);
            }
        }
        if let Some(report) = &result.report {
            self.1.backlog.record(report.backlog_files, std::slice::from_ref(&op));
            if let Some(cutoff) = report.cutoff_ms {
                self.1.cutoff.record(cutoff, &[op]);
            }
        }
        self.with(|counters| {
            *counters.runs.entry((operation, outcome)).or_default() += 1;
            let report = result.report.as_ref();
            if let Some(report) = report {
                for (name, value) in report_items(report) {
                    if report.outcome != Outcome::Planned {
                        *counters.totals.entry((operation, name)).or_default() += value;
                    }
                }
                counters.gauges.insert(
                    (operation, "backlog_files"),
                    i64::try_from(report.backlog_files).unwrap_or(i64::MAX),
                );
                if let Some(cutoff) = report.cutoff_ms {
                    counters.gauges.insert((operation, "retention_cutoff_ms"), cutoff);
                }
                if let Some(watermark) = report.watermark_ms {
                    counters.gauges.insert((operation, "purge_watermark_ms"), watermark);
                }
            }
        });
    }

    /// Render the text exposition format.
    #[must_use]
    pub fn render(&self) -> String {
        let mut text = String::new();
        let Ok(counters) = self.0.lock() else {
            return text;
        };
        let _ = writeln!(text, "# TYPE delta_maintenance_runs_total counter");
        for ((operation, outcome), value) in &counters.runs {
            let _ = writeln!(
                text,
                "delta_maintenance_runs_total{{operation=\"{operation}\",outcome=\"{outcome}\"}} {value}"
            );
        }
        let mut last = "";
        for ((operation, name), value) in &counters.totals {
            if *name != last {
                let _ = writeln!(text, "# TYPE delta_maintenance_{name}_total counter");
            }
            last = name;
            let _ = writeln!(
                text,
                "delta_maintenance_{name}_total{{operation=\"{operation}\"}} {value}"
            );
        }
        for ((operation, name), value) in &counters.gauges {
            let _ = writeln!(text, "# TYPE delta_maintenance_{name} gauge");
            let _ = writeln!(text, "delta_maintenance_{name}{{operation=\"{operation}\"}} {value}");
        }
        let _ = writeln!(text, "# TYPE delta_maintenance_heartbeat_timestamp_ms gauge");
        let _ = writeln!(
            text,
            "delta_maintenance_heartbeat_timestamp_ms {}",
            counters.heartbeat_ms
        );
        let _ = writeln!(text, "# TYPE delta_maintenance_cycles_total counter");
        let _ = writeln!(text, "delta_maintenance_cycles_total {}", counters.cycles);
        text
    }
}

fn report_items(report: &MaintenanceReport) -> [(&'static str, u64); 10] {
    [
        ("files_removed", report.files_removed),
        ("files_added", report.files_added),
        ("rows_removed", report.rows_removed),
        ("rows_added", report.rows_added),
        ("bytes_removed", report.bytes_removed),
        ("bytes_added", report.bytes_added),
        ("files_deleted", report.files_deleted),
        ("files_already_absent", report.files_already_absent),
        ("collisions", u64::from(report.collisions)),
        ("retries", u64::from(report.retries)),
    ]
}

/// Serve `GET /metrics` on a bounded, sequential listener until cancelled.
///
/// # Errors
/// Returns bind errors.
pub fn serve_metrics(
    listen: SocketAddr,
    metrics: Metrics,
    cancel: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>> {
    let listener = TcpListener::bind(listen).with_context(|| format!("bind metrics listener {listen}"))?;
    listener.set_nonblocking(true)?;
    Ok(std::thread::spawn(move || {
        while !cancel.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let _ = respond(stream, &metrics);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => std::thread::sleep(Duration::from_millis(200)),
            }
        }
    }))
}

fn respond(mut stream: TcpStream, metrics: &Metrics) -> Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") && request.len() < MAX_HTTP_REQUEST {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
    }
    let (status, body) = if request.starts_with(b"GET /metrics ") {
        ("200 OK", metrics.render())
    } else {
        ("404 Not Found", String::from("not found\n"))
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    Ok(())
}

fn run_operation(
    settings: &Settings,
    table: &DeltaTable,
    operation: Operation,
    control: &MaintenanceControl,
    optimize: &mut Option<OptimizeEvidence>,
    props: &ResolvedProps,
) -> Result<MaintenanceReport> {
    let anchor = match settings.anchor_ms {
        Some(anchor) => anchor,
        None => now_ms()?,
    };
    let plan = match operation {
        Operation::Checkpoint => {
            return table.checkpoint_if_due(settings.checkpoint_every_commits, settings.apply);
        }
        Operation::PruneLog => {
            let config = PruneConfig {
                log_retention_ms: settings.log_retention_ms,
                unsafe_allow_short_retention_for_tests: settings.unsafe_short_vacuum_for_tests,
                max_prune_files: settings.max_prune_files,
                ..PruneConfig::new(anchor)
            };
            let plan = table.plan_prune_log(&config)?;
            if settings.apply {
                return table.apply_prune_log(&plan, &config, control);
            }
            return Ok(plan.report());
        }
        Operation::Optimize => {
            let config = OptimizeConfig {
                granularity: settings.granularity,
                now_ms: Some(anchor),
                ..settings.optimize.clone()
            };
            let mut plan = table.plan_optimize(&config)?;
            let minimum = usize::try_from(props.unsigned("arrowcol.optimize.minFiles")?)?;
            plan.bins.retain(|bin| bin.files.len() >= minimum);
            *optimize = Some(OptimizeEvidence::new(&config, plan.clone()));
            if settings.apply && !plan.bins.is_empty() {
                return table.apply_optimize(&plan, &config, control);
            }
            MaintenancePlan::Optimize(plan)
        }
        Operation::Purge => {
            let granularity = settings.granularity.context("purge granularity")?;
            let retention = settings.purge_retention_ms.context("purge retention")?;
            let config = PurgeConfig::new(retention, anchor, granularity);
            let plan = table.plan_purge(&config)?;
            if settings.apply {
                return table.apply_purge(&plan, &config, control);
            }
            MaintenancePlan::Purge(plan)
        }
        Operation::Vacuum => {
            let mut config = VacuumConfig::new(settings.vacuum_retention_ms, anchor);
            config.allow_short_vacuum_retention = settings.allow_short_vacuum_retention;
            config.unsafe_allow_short_retention_for_tests = settings.unsafe_short_vacuum_for_tests;
            config.max_deletes = usize::try_from(props.unsigned("arrowcol.vacuum.maxDeletes")?)?;
            let plan = table.plan_vacuum(&config)?;
            if settings.apply {
                return table.apply_vacuum(&plan, &config, control);
            }
            MaintenancePlan::Vacuum(plan)
        }
    };
    let mut report = plan.report();
    report.version = report.read_version;
    Ok(report)
}

fn maintain_table(
    settings: &Settings,
    discovery: &dyn TableDiscovery,
    name: &str,
    cancel: &Arc<AtomicBool>,
    emit: &dyn Fn(&OperationResult),
    schedule: Option<&Mutex<BTreeMap<String, Instant>>>,
) -> Vec<OperationResult> {
    let mut results = Vec::new();
    let setup = (|| -> Result<_> {
        let discovered = discovery.table(name)?;
        let gate = table_gate(discovered.url.as_str())
            .lock()
            .map_err(|_| anyhow::anyhow!("per-table maintenance gate poisoned"))?;
        let (effective, props) = effective_settings(settings, &discovered.properties)?;
        if let Some(schedule) = schedule {
            let mut schedule = schedule
                .lock()
                .map_err(|_| anyhow::anyhow!("maintenance schedule poisoned"))?;
            let now = Instant::now();
            if schedule.get(name).is_some_and(|due| *due > now) {
                return Ok(None);
            }
            let delay = Duration::from_millis(props.unsigned("arrowcol.maintenance.interval")?);
            schedule.insert(
                name.into(),
                now.checked_add(delay)
                    .context("maintenance interval exceeds clock range")?,
            );
        }
        let probe = DeltaTable::open_for_maintenance_probe(&discovered.url)?;
        if let Some(version) = discovered.log_version {
            if probe.probe_log_version(100_000)? != version {
                bail!("table advanced after discovery; retry");
            }
        }
        Ok(Some((discovered, effective, props, probe, gate)))
    })();
    if matches!(&setup, Ok(None)) {
        return results;
    }
    let operations = match &setup {
        Ok(Some((_, effective, _, _, _))) => &effective.operations,
        _ => &settings.operations,
    };
    let mut table = None;
    for operation in Operation::ALL
        .into_iter()
        .filter(|operation| operations.contains(operation))
    {
        if cancel.load(Ordering::SeqCst) {
            break;
        }
        let started = Instant::now();
        let mut optimize = None;
        let mut negative_probe = false;
        let effective = match &setup {
            Ok(Some((_, effective, _, _, _))) => effective,
            _ => settings,
        };
        let control = MaintenanceControl::new(Arc::clone(cancel), Some(started + effective.operation_timeout));
        let outcome = match &setup {
            Ok(Some((discovered, effective, props, probe, _))) => (|| {
                let anchor = effective.anchor_ms.map_or_else(now_ms, Ok)?;
                let config = NegativeProbeConfig {
                    now_ms: anchor,
                    granularity: effective.granularity.context("effective partition granularity")?,
                    purge_retention_ms: effective.purge_retention_ms.context("effective purge retention")?,
                    vacuum_retention_ms: effective.vacuum_retention_ms,
                    log_retention_ms: effective
                        .log_retention_ms
                        .unwrap_or(arrow_deltalake::maintenance::DEFAULT_LOG_RETENTION_MS),
                    checkpoint_interval: effective.checkpoint_every_commits,
                    optimize_small_file_bytes: effective.optimize.small_file_bytes,
                    optimize_min_files: usize::try_from(props.unsigned("arrowcol.optimize.minFiles")?)?,
                    sealed_partitions_only: !effective.optimize.sealed_final,
                    max_listed_files: usize::try_from(props.unsigned("arrowcol.vacuum.maxListedFiles")?)?,
                };
                let probe_operation = match operation {
                    Operation::Purge => ProbeOperation::Purge,
                    Operation::Optimize => ProbeOperation::Optimize,
                    Operation::Vacuum => ProbeOperation::Vacuum,
                    Operation::Checkpoint => ProbeOperation::Checkpoint,
                    Operation::PruneLog => ProbeOperation::PruneLog,
                };
                if let Some(report) = probe.probe_no_work(probe_operation, &config, &control)? {
                    negative_probe = true;
                    return Ok(report);
                }
                if table.is_none() {
                    table = Some(DeltaTable::open_for_maintenance(&discovered.url)?);
                }
                run_operation(
                    effective,
                    table.as_ref().context("maintenance table")?,
                    operation,
                    &control,
                    &mut optimize,
                    props,
                )
            })(),
            Err(error) => Err(anyhow::anyhow!("discovery/configuration failed: {error:#}")),
            Ok(None) => unreachable!(),
        };
        let mut result = OperationResult {
            table: name.to_owned(),
            operation,
            applied: effective.apply,
            negative_probe,
            report: None,
            optimize,
            vacuum: (operation == Operation::Vacuum).then_some(VacuumEvidence {
                retention_ms: effective.vacuum_retention_ms,
                allow_short_vacuum_retention: effective.allow_short_vacuum_retention,
                unsafe_short_vacuum_for_tests: effective.unsafe_short_vacuum_for_tests,
            }),
            error_kind: None,
            error: None,
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        };
        match outcome {
            Ok(report) => result.report = Some(report),
            Err(error) => {
                let details = error_details(&error);
                result.error_kind = Some(details.map_or("failure", |details| details.kind.label()));
                result.report = details.and_then(|details| details.report.as_deref().cloned());
                result.error = Some(format!("{error:#}"));
            }
        }
        emit(&result);
        let stop = result.error_kind == Some(MaintenanceErrorKind::Cancelled.label());
        results.push(result);
        if stop {
            break;
        }
    }
    results
}

/// Run one maintenance cycle over every allowlisted table with bounded concurrency.
#[must_use]
pub fn run_cycle(settings: &Settings, metrics: &Metrics, cancel: &Arc<AtomicBool>) -> Vec<OperationResult> {
    let source = FilesystemDiscovery::new(settings.root.clone(), settings.tables.clone());
    match cycle_with_discovery(settings, &source, metrics, cancel, None) {
        Ok(results) => results,
        Err(error) => {
            let result = discovery_failure(settings, error);
            metrics.record(&result);
            vec![result]
        }
    }
}

fn discovery_failure(settings: &Settings, error: anyhow::Error) -> OperationResult {
    OperationResult {
        table: String::from("<discovery>"),
        operation: Operation::Optimize,
        applied: settings.apply,
        negative_probe: false,
        report: None,
        optimize: None,
        vacuum: None,
        error_kind: Some("failure"),
        error: Some(format!("{error:#}")),
        duration_ms: 0,
    }
}

fn cycle_with_discovery(
    settings: &Settings,
    discovery: &dyn TableDiscovery,
    metrics: &Metrics,
    cancel: &Arc<AtomicBool>,
    schedule: Option<&Mutex<BTreeMap<String, Instant>>>,
) -> Result<Vec<OperationResult>> {
    let names = discovery.tables()?;
    if names.len() > MAX_TABLES {
        bail!("catalog discovery exceeds {MAX_TABLES} tables");
    }
    let queue = Mutex::new(names.iter().cloned().collect::<VecDeque<_>>());
    let results = Mutex::new(Vec::new());
    let emit = |result: &OperationResult| {
        metrics.record(result);
        if let Ok(line) = serde_json::to_string(result) {
            println!("{line}");
        }
    };
    let workers = settings.max_concurrent_tables.min(names.len());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    if cancel.load(Ordering::SeqCst) {
                        break;
                    }
                    let next = queue.lock().ok().and_then(|mut queue| queue.pop_front());
                    let Some(name) = next else { break };
                    let mut done = maintain_table(settings, discovery, &name, cancel, &emit, schedule);
                    if let Ok(mut results) = results.lock() {
                        results.append(&mut done);
                    }
                }
            });
        }
    });
    metrics.heartbeat();
    results
        .into_inner()
        .map_err(|_| anyhow::anyhow!("maintenance results mutex poisoned"))
}

/// Run once or on a schedule with idle exponential backoff until cancelled.
/// Returns `false` if any operation failed.
#[must_use]
pub fn run(settings: &Settings, metrics: &Metrics, cancel: &Arc<AtomicBool>) -> bool {
    let source = FilesystemDiscovery::new(settings.root.clone(), settings.tables.clone());
    run_with_discovery(settings, &source, metrics, cancel)
}

/// Run the same bounded scheduler over catalog-client discovery.
#[must_use]
pub fn run_with_discovery(
    settings: &Settings,
    discovery: &dyn TableDiscovery,
    metrics: &Metrics,
    cancel: &Arc<AtomicBool>,
) -> bool {
    let mut healthy = true;
    let schedule = Mutex::new(BTreeMap::new());
    let mut delay = settings.interval;
    let mut cycles = 0_u64;
    loop {
        let results = match cycle_with_discovery(settings, discovery, metrics, cancel, Some(&schedule)) {
            Ok(results) => results,
            Err(error) => {
                let result = discovery_failure(settings, error);
                metrics.record(&result);
                eprintln!(
                    "delta-maintenance discovery failed: {}",
                    result.error.as_deref().unwrap_or("unknown")
                );
                vec![result]
            }
        };
        cycles += 1;
        healthy &= results.iter().all(|result| result.error_kind.is_none());
        let Some(interval) = settings.interval else { break };
        if settings.max_cycles.is_some_and(|max| cycles >= max) || cancel.load(Ordering::SeqCst) {
            break;
        }
        let idle = !results.iter().any(OperationResult::did_work);
        let current = if idle { delay.unwrap_or(interval) } else { interval };
        eprintln!(
            "delta-maintenance heartbeat cycle={cycles} idle={idle} next_in_ms={}",
            current.as_millis()
        );
        delay = Some(if idle {
            current.saturating_mul(2).min(settings.max_idle_backoff)
        } else {
            interval
        });
        let wake = Instant::now() + current;
        while Instant::now() < wake && !cancel.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(100).min(wake.saturating_duration_since(Instant::now())));
        }
        if cancel.load(Ordering::SeqCst) {
            break;
        }
    }
    healthy
}
