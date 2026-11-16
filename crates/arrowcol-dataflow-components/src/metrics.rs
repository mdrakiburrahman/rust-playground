use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant};

use cpu_time::ProcessTime;
use opentelemetry::metrics::{Counter, Gauge, Histogram};
use opentelemetry::{KeyValue, global};

use arrowcol_catalog::SchemaVersion;
use arrowcol_protocol::{
    BasisPoints, Capability, CollectorInstanceId, ImplementationPool, Measurement, PositiveMeasurement, PressureGauge,
    ProtocolVersion, PublicationId, TableIdentity, UnixMillis,
};
use arrowcol_routing::{RichPressureSnapshot, RoutingStore, SubsystemState, TablePressure};
use arrowcol_tunables::CpuPressureMode;
use serde::Serialize;
use tokio::sync::watch;

use crate::config::now_unix_millis;

static PROCESS_INSTANCE_ID: LazyLock<CollectorInstanceId> = LazyLock::new(CollectorInstanceId::new);
static GLOBAL_METRICS: LazyLock<Arc<RuntimeMetrics>> =
    LazyLock::new(|| Arc::new(RuntimeMetrics::new(*PROCESS_INSTANCE_ID)));
const MAX_TABLE_PRESSURE_ENTRIES: usize = 256;
/// A fixed ArrowCol pipeline stage used by the throughput campaign.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PipelineStage {
    Collect,
    Build,
    Sort,
    Encode,
    Upload,
    Commit,
}

const STAGE_DURATION_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];
const FILE_SIZE_BUCKETS: &[f64] = &[
    1_048_576.0,
    8_388_608.0,
    16_777_216.0,
    33_554_432.0,
    67_108_864.0,
    100_663_296.0,
    134_217_728.0,
    167_772_160.0,
    268_435_456.0,
    536_870_912.0,
];
const FILE_ROW_BUCKETS: &[f64] = &[
    1.0,
    1_024.0,
    4_096.0,
    16_384.0,
    65_536.0,
    131_072.0,
    262_144.0,
    524_288.0,
    1_048_576.0,
];

/// A bounded reason for a staged Parquet flush.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlushReason {
    TargetParquetBytes,
    MinInputBytes,
    MaxInputBytes,
    RowCap,
    BatchCap,
    MemoryCap,
    Age,
    SchemaChange,
    Explicit,
    ShutdownDrain,
}

impl FlushReason {
    const ALL: [Self; 10] = [
        Self::TargetParquetBytes,
        Self::MinInputBytes,
        Self::MaxInputBytes,
        Self::RowCap,
        Self::BatchCap,
        Self::MemoryCap,
        Self::Age,
        Self::SchemaChange,
        Self::Explicit,
        Self::ShutdownDrain,
    ];

    const fn index(self) -> usize {
        match self {
            Self::TargetParquetBytes => 0,
            Self::MinInputBytes => 1,
            Self::MaxInputBytes => 2,
            Self::RowCap => 3,
            Self::BatchCap => 4,
            Self::MemoryCap => 5,
            Self::Age => 6,
            Self::SchemaChange => 7,
            Self::Explicit => 8,
            Self::ShutdownDrain => 9,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::TargetParquetBytes => "target_parquet_bytes",
            Self::MinInputBytes => "min_input_bytes",
            Self::MaxInputBytes => "max_input_bytes",
            Self::RowCap => "row_cap",
            Self::BatchCap => "batch_cap",
            Self::MemoryCap => "memory_cap",
            Self::Age => "age",
            Self::SchemaChange => "schema_change",
            Self::Explicit => "explicit",
            Self::ShutdownDrain => "shutdown_drain",
        }
    }
}

struct FixedHistogram {
    bounds: &'static [f64],
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum: AtomicU64,
}

impl FixedHistogram {
    fn new(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            buckets: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    fn record(&self, value: f64, sum_units: u64) {
        let bucket = self
            .bounds
            .iter()
            .position(|bound| value <= *bound)
            .unwrap_or(self.bounds.len());
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(sum_units, Ordering::Relaxed);
    }

    fn write(&self, output: &mut String, name: &str, labels: &str, sum_divisor: f64) {
        let mut cumulative = 0_u64;
        for (index, bound) in self.bounds.iter().enumerate() {
            cumulative = cumulative.saturating_add(self.buckets[index].load(Ordering::Acquire));
            let le = if bound.fract() == 0.0 {
                format!("{bound:.0}")
            } else {
                bound.to_string()
            };
            let _ = writeln!(output, "{name}_bucket{{{labels},le=\"{le}\"}} {cumulative}");
        }
        cumulative = cumulative.saturating_add(self.buckets[self.bounds.len()].load(Ordering::Acquire));
        let _ = writeln!(output, "{name}_bucket{{{labels},le=\"+Inf\"}} {cumulative}");
        let _ = writeln!(
            output,
            "{name}_sum{{{labels}}} {}",
            self.sum.load(Ordering::Acquire) as f64 / sum_divisor
        );
        let _ = writeln!(
            output,
            "{name}_count{{{labels}}} {}",
            self.count.load(Ordering::Acquire)
        );
    }
}

struct PipelineMetrics {
    run: String,
    variant: String,
    stages: Vec<FixedHistogram>,
    submission_wait: FixedHistogram,
    flushes: [AtomicU64; 10],
    file_bytes: FixedHistogram,
    file_rows: FixedHistogram,
}

impl PipelineMetrics {
    fn new() -> Self {
        Self {
            run: std::env::var("ARROWCOL_OAT_RUN_ID").unwrap_or_else(|_| "unknown".to_owned()),
            variant: std::env::var("ARROWCOL_OAT_VARIANT").unwrap_or_else(|_| "unknown".to_owned()),
            stages: (0..6).map(|_| FixedHistogram::new(STAGE_DURATION_BUCKETS)).collect(),
            submission_wait: FixedHistogram::new(STAGE_DURATION_BUCKETS),
            flushes: std::array::from_fn(|_| AtomicU64::new(0)),
            file_bytes: FixedHistogram::new(FILE_SIZE_BUCKETS),
            file_rows: FixedHistogram::new(FILE_ROW_BUCKETS),
        }
    }

    fn record_stage(&self, stage: PipelineStage, duration: Duration) {
        self.stages[stage.index()].record(
            duration.as_secs_f64(),
            duration.as_nanos().min(u128::from(u64::MAX)) as u64,
        );
    }

    fn record_flush(&self, reason: FlushReason) {
        self.flushes[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    fn record_file(&self, physical_bytes: u64, rows: u64) {
        self.file_bytes.record(physical_bytes as f64, physical_bytes);
        self.file_rows.record(rows as f64, rows);
    }

    fn write(&self, output: &mut String) {
        let run = escape_prometheus_label(&self.run);
        let variant = escape_prometheus_label(&self.variant);
        let component = "arrowcol-dataflow-rust";
        let _ = writeln!(
            output,
            "# HELP arrowcol_pipeline_stage_duration_seconds Pipeline stage duration."
        );
        output.push_str("# TYPE arrowcol_pipeline_stage_duration_seconds histogram\n");
        for stage in [
            PipelineStage::Collect,
            PipelineStage::Build,
            PipelineStage::Sort,
            PipelineStage::Encode,
            PipelineStage::Upload,
            PipelineStage::Commit,
        ] {
            let labels = format!(
                "run=\"{run}\",variant=\"{variant}\",component=\"{component}\",stage=\"{}\"",
                stage.as_str()
            );
            self.stages[stage.index()].write(
                output,
                "arrowcol_pipeline_stage_duration_seconds",
                &labels,
                1_000_000_000.0,
            );
        }
        let _ = writeln!(
            output,
            "# HELP arrowcol_pipeline_flushes_total Pipeline flushes by bounded reason."
        );
        output.push_str("# TYPE arrowcol_pipeline_flushes_total counter\n");
        for reason in FlushReason::ALL {
            let _ = writeln!(
                output,
                "arrowcol_pipeline_flushes_total{{reason=\"{}\"}} {}",
                reason.as_str(),
                self.flushes[reason.index()].load(Ordering::Acquire)
            );
        }
        let file_labels = format!("run=\"{run}\",variant=\"{variant}\",component=\"{component}\"");
        output.push_str(
            "# HELP arrowcol_pipeline_submission_wait_seconds Time waiting to submit work to the commit worker, excluding durable commit execution.\n\
             # TYPE arrowcol_pipeline_submission_wait_seconds histogram\n",
        );
        self.submission_wait.write(
            output,
            "arrowcol_pipeline_submission_wait_seconds",
            &file_labels,
            1_000_000_000.0,
        );
        let _ = writeln!(
            output,
            "# HELP arrowcol_parquet_file_physical_bytes Physical Parquet file size including the footer."
        );
        output.push_str("# TYPE arrowcol_parquet_file_physical_bytes histogram\n");
        self.file_bytes
            .write(output, "arrowcol_parquet_file_physical_bytes", &file_labels, 1.0);
        let _ = writeln!(
            output,
            "# HELP arrowcol_parquet_file_rows Rows in each physical Parquet file."
        );
        output.push_str("# TYPE arrowcol_parquet_file_rows histogram\n");
        self.file_rows
            .write(output, "arrowcol_parquet_file_rows", &file_labels, 1.0);
    }
}

fn escape_prometheus_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

static PIPELINE_METRICS: LazyLock<PipelineMetrics> = LazyLock::new(PipelineMetrics::new);

/// OpenTelemetry recorder for the fixed pipeline stages.
#[derive(Clone, Debug)]
pub struct StageMetrics {
    duration: Histogram<f64>,
    submission_wait: Histogram<f64>,
    flushes: Counter<u64>,
    physical_bytes: Histogram<u64>,
    file_rows: Histogram<u64>,
    run: String,
    variant: String,
}

impl Default for StageMetrics {
    fn default() -> Self {
        let meter = global::meter("arrowcol-dataflow-components");
        Self {
            duration: meter
                .f64_histogram("arrowcol.pipeline.stage.duration")
                .with_unit("s")
                .build(),
            submission_wait: meter
                .f64_histogram("arrowcol.pipeline.submission.wait")
                .with_unit("s")
                .with_boundaries(STAGE_DURATION_BUCKETS.to_vec())
                .build(),
            flushes: meter.u64_counter("arrowcol.pipeline.flushes").with_unit("1").build(),
            physical_bytes: meter
                .u64_histogram("arrowcol.parquet.file.physical_bytes")
                .with_unit("By")
                .build(),
            file_rows: meter.u64_histogram("arrowcol.parquet.file.rows").with_unit("1").build(),
            run: std::env::var("ARROWCOL_OAT_RUN_ID").unwrap_or_else(|_| "unknown".to_owned()),
            variant: std::env::var("ARROWCOL_OAT_VARIANT").unwrap_or_else(|_| "unknown".to_owned()),
        }
    }
}

impl StageMetrics {
    /// Records a duration for one fixed pipeline stage.
    pub fn record(&self, stage: PipelineStage, duration: Duration) {
        PIPELINE_METRICS.record_stage(stage, duration);
        self.duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("run", self.run.clone()),
                KeyValue::new("variant", self.variant.clone()),
                KeyValue::new("component", "arrowcol-dataflow-rust"),
                KeyValue::new("stage", stage.as_str()),
            ],
        );
    }

    /// Records commit-worker submission backpressure, not durable commit execution.
    pub fn record_submission_wait(&self, duration: Duration) {
        PIPELINE_METRICS.submission_wait.record(
            duration.as_secs_f64(),
            duration.as_nanos().min(u128::from(u64::MAX)) as u64,
        );
        self.submission_wait.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("run", self.run.clone()),
                KeyValue::new("variant", self.variant.clone()),
                KeyValue::new("component", "arrowcol-dataflow-rust"),
            ],
        );
    }

    pub fn record_flush(&self, reason: FlushReason) {
        PIPELINE_METRICS.record_flush(reason);
        self.flushes.add(1, &[KeyValue::new("reason", reason.as_str())]);
    }

    pub fn record_file(&self, physical_bytes: u64, rows: u64) {
        PIPELINE_METRICS.record_file(physical_bytes, rows);
        let attributes = [
            KeyValue::new("run", self.run.clone()),
            KeyValue::new("variant", self.variant.clone()),
            KeyValue::new("component", "arrowcol-dataflow-rust"),
        ];
        self.physical_bytes.record(physical_bytes, &attributes);
        self.file_rows.record(rows, &attributes);
    }
}

impl PipelineStage {
    const fn index(self) -> usize {
        match self {
            Self::Collect => 0,
            Self::Build => 1,
            Self::Sort => 2,
            Self::Encode => 3,
            Self::Upload => 4,
            Self::Commit => 5,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Collect => "collect",
            Self::Build => "build",
            Self::Sort => "sort",
            Self::Encode => "encode",
            Self::Upload => "upload",
            Self::Commit => "commit",
        }
    }
}

struct CollectorOtelMetrics {
    attributes: Vec<KeyValue>,
    received_rows: Counter<u64>,
    enqueue_failures: Counter<u64>,
    downstream_acks: Counter<u64>,
    downstream_nacks: Counter<u64>,
    refused_batches: Counter<u64>,
    delta_rows: Counter<u64>,
    delta_failures: Counter<u64>,
    buffered_rows: Gauge<u64>,
    buffered_batches: Gauge<u64>,
    oldest_pending_age: Gauge<f64>,
    pending_commits: Gauge<u64>,
}

impl CollectorOtelMetrics {
    fn new() -> Self {
        let meter = global::meter("arrowcol-dataflow-components");
        let attributes = vec![
            KeyValue::new(
                "run",
                std::env::var("ARROWCOL_OAT_RUN_ID").unwrap_or_else(|_| "unknown".to_owned()),
            ),
            KeyValue::new(
                "variant",
                std::env::var("ARROWCOL_OAT_VARIANT").unwrap_or_else(|_| "unknown".to_owned()),
            ),
            KeyValue::new("component", "arrowcol-dataflow-rust"),
        ];
        Self {
            attributes,
            received_rows: meter.u64_counter("arrowcol.receiver.rows").with_unit("1").build(),
            enqueue_failures: meter
                .u64_counter("arrowcol.dfe.enqueue.failures")
                .with_unit("1")
                .build(),
            downstream_acks: meter.u64_counter("arrowcol.dfe.downstream.acks").with_unit("1").build(),
            downstream_nacks: meter
                .u64_counter("arrowcol.dfe.downstream.nacks")
                .with_unit("1")
                .build(),
            refused_batches: meter.u64_counter("arrowcol.dfe.refused.batches").with_unit("1").build(),
            delta_rows: meter
                .u64_counter("arrowcol.delta.committed.rows")
                .with_unit("1")
                .build(),
            delta_failures: meter
                .u64_counter("arrowcol.delta.commit.failures")
                .with_unit("1")
                .build(),
            buffered_rows: meter.u64_gauge("arrowcol.dfe.buffered.rows").with_unit("1").build(),
            buffered_batches: meter.u64_gauge("arrowcol.dfe.buffered.batches").with_unit("1").build(),
            oldest_pending_age: meter
                .f64_gauge("arrowcol.dfe.oldest.pending.age")
                .with_unit("s")
                .build(),
            pending_commits: meter.u64_gauge("arrowcol.delta.pending.commits").with_unit("1").build(),
        }
    }

    fn record_pressure(
        &self,
        buffered_rows: u64,
        buffered_batches: u64,
        oldest_age_seconds: f64,
        pending_commits: u64,
    ) {
        self.buffered_rows.record(buffered_rows, &self.attributes);
        self.buffered_batches.record(buffered_batches, &self.attributes);
        self.oldest_pending_age.record(oldest_age_seconds, &self.attributes);
        self.pending_commits.record(pending_commits, &self.attributes);
    }
}

/// Durable-backlog utilization at which quiver_state reports degraded.
const QUIVER_DEGRADED_BPS: u16 = 8_000;

/// Returns the process-lifetime collector identifier shared by every DFE core.
#[must_use]
pub fn process_instance_id() -> CollectorInstanceId {
    *PROCESS_INSTANCE_ID
}

/// Returns the stable deployment key (`registration.instance_key`) of this
/// process's `receiver:arrow_flight`.
///
/// The receiver records the key when it is created. Unlike
/// [`process_instance_id`], the key survives a restart, which lets the
/// exporter recognize durable-buffer replays admitted by a crashed predecessor
/// of the same deployment slot. Returns `None` until a receiver was created.
#[must_use]
pub fn process_instance_key() -> Option<String> {
    GLOBAL_METRICS.instance_key().map(str::to_owned)
}

/// Returns process-wide Arrowcol component metrics.
#[must_use]
pub fn global_metrics() -> Arc<RuntimeMetrics> {
    Arc::clone(&GLOBAL_METRICS)
}

/// Delivery labels shared by capabilities, readiness, and Prometheus.
#[derive(Clone, Copy)]
struct DeliveryContract {
    delivery_guarantee: &'static str,
    completion_mode: &'static str,
    at_least_once: bool,
    help: &'static str,
}

impl DeliveryContract {
    const BEST_EFFORT: Self = Self {
        delivery_guarantee: "best_effort",
        completion_mode: "best_effort_admission",
        at_least_once: false,
        help: "Admission is best-effort and has no receipt polling.",
    };
    const DURABLE_ADMISSION: Self = Self {
        delivery_guarantee: "durable_admission",
        completion_mode: "durable_admission",
        at_least_once: true,
        help: "Streams complete after durable-buffer persistence; delivery is at-least-once without receipt polling.",
    };

    const fn select(native_quiver: bool) -> Self {
        if native_quiver {
            Self::DURABLE_ADMISSION
        } else {
            Self::BEST_EFFORT
        }
    }
}

/// Signed durable-backlog counters for one table.
///
/// The receiver adds a batch when the durable buffer acknowledges it, and the
/// exporter subtracts it once publication succeeds or fails permanently. The
/// exporter can observe a release before the receiver processes the matching
/// acknowledgement, so values may be transiently negative; reports clamp them
/// at zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DurableBacklog {
    batches: i128,
    rows: i128,
    bytes: i128,
}

impl DurableBacklog {
    fn admit(&mut self, rows: u64, bytes: u64) {
        self.adjust(1, i128::from(rows), i128::from(bytes));
    }

    fn release(&mut self, rows: u64, bytes: u64) {
        self.adjust(-1, -i128::from(rows), -i128::from(bytes));
    }

    fn adjust(&mut self, batches: i128, rows: i128, bytes: i128) {
        self.batches = self.batches.saturating_add(batches);
        self.rows = self.rows.saturating_add(rows);
        self.bytes = self.bytes.saturating_add(bytes);
        if self.batches == 0 {
            // A release measures the replayed batch, whose Arrow layout can differ from the admitted
            // one by a few bytes (for example an elided all-valid null bitmap). Re-anchoring whenever
            // the backlog is empty keeps such measurement differences from accumulating.
            self.rows = 0;
            self.bytes = 0;
        }
    }

    fn totals(self) -> DurableTotals {
        DurableTotals {
            batches: clamp_backlog(self.batches),
            rows: clamp_backlog(self.rows),
            bytes: clamp_backlog(self.bytes),
        }
    }
}

/// Non-negative durable-backlog totals.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DurableTotals {
    batches: u64,
    rows: u64,
    bytes: u64,
}

impl DurableTotals {
    fn add(self, other: Self) -> Self {
        Self {
            batches: self.batches.saturating_add(other.batches),
            rows: self.rows.saturating_add(other.rows),
            bytes: self.bytes.saturating_add(other.bytes),
        }
    }
}

fn clamp_backlog(value: i128) -> u64 {
    u64::try_from(value.max(0)).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug, Default)]
struct TableState {
    active_streams: u64,
    buffered_bytes: u64,
    buffered_rows: u64,
    buffered_batches: u64,
    pending_commits: u64,
    oldest_buffered_at: Option<Instant>,
    source_schema_version: Option<u64>,
    target_schema_version: Option<u64>,
    last_delta_version: Option<u64>,
    publication_failures: u64,
    durable: DurableBacklog,
}

#[derive(Default)]
struct ProcessCpuSampler {
    last_cpu: Option<ProcessTime>,
    last_wall: Option<Instant>,
}

impl ProcessCpuSampler {
    fn sample(&mut self) -> Result<u16, String> {
        let now_cpu = ProcessTime::try_now().map_err(|error| format!("read process CPU time: {error}"))?;
        let now_wall = Instant::now();
        let previous = self.last_cpu.zip(self.last_wall);
        self.last_cpu = Some(now_cpu);
        self.last_wall = Some(now_wall);
        let Some((last_cpu, last_wall)) = previous else {
            return Ok(0);
        };
        let cpu_delta = last_cpu
            .try_elapsed()
            .map_err(|error| format!("read process CPU delta: {error}"))?;
        process_cpu_ratio_bps(
            cpu_delta,
            now_wall.saturating_duration_since(last_wall),
            effective_cpu_cores()?,
        )
    }
}

fn process_cpu_ratio_bps(cpu_delta: Duration, wall_delta: Duration, effective_cores: f64) -> Result<u16, String> {
    if wall_delta.is_zero() || !effective_cores.is_finite() || effective_cores <= 0.0 {
        return Err("CPU pressure ratio requires a positive wall interval and effective core count".to_owned());
    }
    let ratio = cpu_delta.as_secs_f64() / (wall_delta.as_secs_f64() * effective_cores);
    if !ratio.is_finite() || ratio < 0.0 {
        return Err("CPU pressure ratio is not finite and non-negative".to_owned());
    }
    Ok((ratio * 10_000.0).round().clamp(0.0, 10_000.0) as u16)
}

fn effective_cpu_cores() -> Result<f64, String> {
    #[cfg(target_os = "linux")]
    match std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        Ok(contents) => {
            let mut parts = contents.split_whitespace();
            let quota = parts
                .next()
                .ok_or_else(|| "cgroup cpu.max is missing quota".to_owned())?;
            let period = parts
                .next()
                .ok_or_else(|| "cgroup cpu.max is missing period".to_owned())?
                .parse::<u64>()
                .map_err(|error| format!("parse cgroup cpu.max period: {error}"))?;
            if quota != "max" {
                let quota = quota
                    .parse::<u64>()
                    .map_err(|error| format!("parse cgroup cpu.max quota: {error}"))?;
                if quota == 0 || period == 0 {
                    return Err("cgroup cpu.max quota and period must be positive".to_owned());
                }
                return Ok(quota as f64 / period as f64);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("read cgroup cpu.max: {error}")),
    }
    std::thread::available_parallelism()
        .map(|cores| cores.get() as f64)
        .map_err(|error| format!("read available CPU parallelism: {error}"))
}

/// Queue-admitted publications that have not yet confirmed a durable commit.
///
/// Unknown accounting is sticky for this process lifetime. Missing values are
/// never interchangeable with an empty, fully observed publication cohort.
#[derive(Clone, Debug)]
pub struct PublicationBacklogSnapshot {
    pub available: bool,
    pub publications: Option<u64>,
    pub rows: Option<u64>,
    pub logical_bytes: Option<u64>,
    pub oldest_age: Option<Duration>,
    pub unknown_reason: Option<&'static str>,
}

impl PublicationBacklogSnapshot {
    fn write(&self, output: &mut String) {
        output.push_str(
            "# HELP arrowcol_publication_backlog_available Complete queue-admission-to-durable-commit accounting is available.\n\
             # TYPE arrowcol_publication_backlog_available gauge\n",
        );
        let _ = writeln!(
            output,
            "arrowcol_publication_backlog_available {}",
            u8::from(self.available)
        );
        for (name, help, value) in [
            (
                "publications",
                "Distinct admitted publications without confirmed durable success.",
                self.publications.map(|value| value.to_string()),
            ),
            (
                "rows",
                "Rows in the publication backlog.",
                self.rows.map(|value| value.to_string()),
            ),
            (
                "bytes",
                "Logical input Arrow bytes in the publication backlog, not physical Parquet bytes.",
                self.logical_bytes.map(|value| value.to_string()),
            ),
            (
                "oldest_age_seconds",
                "Monotonic age since the oldest publication first entered its TableOwner queue.",
                self.oldest_age.map(|value| value.as_secs_f64().to_string()),
            ),
        ] {
            let _ = writeln!(output, "# HELP arrowcol_publication_backlog_{name} {help}");
            let _ = writeln!(output, "# TYPE arrowcol_publication_backlog_{name} gauge");
            let _ = writeln!(
                output,
                "arrowcol_publication_backlog_{name} {}",
                value.as_deref().unwrap_or("NaN")
            );
        }
        output.push_str("# TYPE arrowcol_publication_backlog_status gauge\n");
        let _ = writeln!(
            output,
            "arrowcol_publication_backlog_status{{status=\"{}\"}} 1",
            self.unknown_reason.unwrap_or("available")
        );
    }
}

struct PublicationBacklogEntry {
    rows: u64,
    logical_bytes: u64,
    accepted_at: Instant,
}

#[derive(Default)]
struct PublicationBacklog {
    max_publications: Option<u32>,
    entries: HashMap<PublicationId, PublicationBacklogEntry>,
    rows: u64,
    logical_bytes: u64,
    unknown_reason: Option<&'static str>,
}

impl PublicationBacklog {
    fn fail(&mut self, reason: &'static str) -> String {
        let reason = *self.unknown_reason.get_or_insert(reason);
        format!("publication backlog accounting unavailable: {reason}")
    }

    fn configure(&mut self, max_publications: u32) -> Result<(), String> {
        if max_publications == 0 {
            return Err(self.fail("invalid_bound"));
        }
        if self
            .max_publications
            .is_some_and(|existing| existing != max_publications)
        {
            return Err(self.fail("conflicting_bound"));
        }
        if let Some(reason) = self.unknown_reason {
            return Err(self.fail(reason));
        }
        self.max_publications = Some(max_publications);
        Ok(())
    }

    fn start(&mut self, id: PublicationId, rows: u64, logical_bytes: u64, accepted_at: Instant) -> Result<(), String> {
        if let Some(reason) = self.unknown_reason {
            return Err(self.fail(reason));
        }
        let Some(max_publications) = self.max_publications else {
            return Err(self.fail("unconfigured"));
        };
        if accepted_at > Instant::now() {
            return Err(self.fail("invalid_clock"));
        }
        if let Some(entry) = self.entries.get(&id) {
            return if entry.rows == rows && entry.logical_bytes == logical_bytes {
                Ok(())
            } else {
                Err(self.fail("replay_mismatch"))
            };
        }
        if self.entries.len() >= max_publications as usize {
            return Err(self.fail("capacity_exceeded"));
        }
        let Some(total_rows) = self.rows.checked_add(rows) else {
            return Err(self.fail("overflow"));
        };
        let Some(total_bytes) = self.logical_bytes.checked_add(logical_bytes) else {
            return Err(self.fail("overflow"));
        };
        self.entries.insert(
            id,
            PublicationBacklogEntry {
                rows,
                logical_bytes,
                accepted_at,
            },
        );
        self.rows = total_rows;
        self.logical_bytes = total_bytes;
        Ok(())
    }

    fn commit(&mut self, id: PublicationId) -> Result<(), String> {
        let Some(entry) = self.entries.remove(&id) else {
            return Err(self.fail("unknown_commit"));
        };
        self.rows -= entry.rows;
        self.logical_bytes -= entry.logical_bytes;
        if let Some(reason) = self.unknown_reason {
            return Err(self.fail(reason));
        }
        Ok(())
    }

    fn snapshot(&mut self, now: Instant) -> PublicationBacklogSnapshot {
        let oldest = self.entries.values().map(|entry| entry.accepted_at).min();
        let age = oldest.map_or(Some(Duration::ZERO), |accepted| now.checked_duration_since(accepted));
        if age.is_none() {
            self.fail("invalid_clock");
        }
        let unknown_reason = self
            .unknown_reason
            .or_else(|| self.max_publications.is_none().then_some("unconfigured"));
        let available = unknown_reason.is_none();
        PublicationBacklogSnapshot {
            available,
            publications: available.then_some(self.entries.len() as u64),
            rows: available.then_some(self.rows),
            logical_bytes: available.then_some(self.logical_bytes),
            oldest_age: if available { age } else { None },
            unknown_reason,
        }
    }
}

/// Atomic process-wide counters shared by receiver, exporter, admin, and registration.
pub struct RuntimeMetrics {
    instance_id: CollectorInstanceId,
    instance_key: OnceLock<String>,
    native_quiver: OnceLock<bool>,
    accepting: AtomicBool,
    healthy: AtomicBool,
    catalog_ready: AtomicBool,
    registered: AtomicBool,
    active_streams: AtomicU64,
    received_bytes: AtomicU64,
    received_rows: AtomicU64,
    received_batches: AtomicU64,
    buffered_bytes: AtomicU64,
    buffered_rows: AtomicU64,
    buffered_batches: AtomicU64,
    pending_commits: AtomicU64,
    publication_backlog: Mutex<PublicationBacklog>,
    admitted_streams: AtomicU64,
    aborted_streams: AtomicU64,
    downstream_acks: AtomicU64,
    downstream_nacks: AtomicU64,
    delta_files: AtomicU64,
    delta_rows: AtomicU64,
    delta_bytes: AtomicU64,
    delta_failures: AtomicU64,
    durable_admitted_batches: AtomicU64,
    durable_released_batches: AtomicU64,
    durable_admission_failures: AtomicU64,
    replayed_batches: AtomicU64,
    rebound_batches: AtomicU64,
    replay_deferred_batches: AtomicU64,
    max_concurrent_streams: AtomicU64,
    stream_permits: OnceLock<(usize, Arc<tokio::sync::Semaphore>)>,
    rss_limit_bytes: AtomicU64,
    buffered_bytes_limit: AtomicU64,
    buffered_rows_limit: AtomicU64,
    buffered_batches_limit: AtomicU64,
    max_pending_commits: AtomicU64,
    dfe_channel_capacity: AtomicU64,
    tables: Mutex<BTreeMap<TableIdentity, TableState>>,
    cpu_pressure_mode: OnceLock<CpuPressureMode>,
    cpu_utilization_bps: AtomicU64,
    process_cpu_sampler: Mutex<ProcessCpuSampler>,
    enqueue_failures: AtomicU64,
    otel: CollectorOtelMetrics,
    pressure_publication: tokio::sync::Mutex<Option<UnixMillis>>,
}

impl RuntimeMetrics {
    /// Creates isolated metrics for tests or an embedded collector.
    #[must_use]
    pub fn new(instance_id: CollectorInstanceId) -> Self {
        Self {
            instance_id,
            instance_key: OnceLock::new(),
            native_quiver: OnceLock::new(),
            accepting: AtomicBool::new(true),
            healthy: AtomicBool::new(true),
            catalog_ready: AtomicBool::new(false),
            registered: AtomicBool::new(false),
            active_streams: AtomicU64::new(0),
            received_bytes: AtomicU64::new(0),
            received_rows: AtomicU64::new(0),
            received_batches: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            buffered_rows: AtomicU64::new(0),
            buffered_batches: AtomicU64::new(0),
            pending_commits: AtomicU64::new(0),
            publication_backlog: Mutex::new(PublicationBacklog::default()),
            admitted_streams: AtomicU64::new(0),
            aborted_streams: AtomicU64::new(0),
            downstream_acks: AtomicU64::new(0),
            downstream_nacks: AtomicU64::new(0),
            delta_files: AtomicU64::new(0),
            delta_rows: AtomicU64::new(0),
            delta_bytes: AtomicU64::new(0),
            delta_failures: AtomicU64::new(0),
            durable_admitted_batches: AtomicU64::new(0),
            durable_released_batches: AtomicU64::new(0),
            durable_admission_failures: AtomicU64::new(0),
            replayed_batches: AtomicU64::new(0),
            rebound_batches: AtomicU64::new(0),
            replay_deferred_batches: AtomicU64::new(0),
            max_concurrent_streams: AtomicU64::new(1),
            stream_permits: OnceLock::new(),
            rss_limit_bytes: AtomicU64::new(1),
            buffered_bytes_limit: AtomicU64::new(1),
            buffered_rows_limit: AtomicU64::new(1),
            buffered_batches_limit: AtomicU64::new(1),
            max_pending_commits: AtomicU64::new(1),
            dfe_channel_capacity: AtomicU64::new(1),
            tables: Mutex::new(BTreeMap::new()),
            cpu_pressure_mode: OnceLock::new(),
            cpu_utilization_bps: AtomicU64::new(0),
            process_cpu_sampler: Mutex::new(ProcessCpuSampler::default()),
            enqueue_failures: AtomicU64::new(0),
            otel: CollectorOtelMetrics::new(),
            pressure_publication: tokio::sync::Mutex::new(None),
        }
    }

    fn publication_backlog(&self) -> std::sync::MutexGuard<'_, PublicationBacklog> {
        self.publication_backlog.lock().unwrap_or_else(|poisoned| {
            let mut tracker = poisoned.into_inner();
            tracker.fail("tracker_poisoned");
            tracker
        })
    }

    /// Reuses the existing global admission batch capacity as the tracking bound.
    ///
    /// Call once during exporter setup; identical calls from other cores are
    /// accepted. A zero or conflicting bound makes accounting explicitly unknown.
    ///
    /// # Errors
    ///
    /// Returns an error if the bound is invalid, conflicts, or accounting was lost.
    pub fn configure_publication_backlog(&self, max_publications: u32) -> Result<(), String> {
        self.publication_backlog().configure(max_publications)
    }

    /// Records successful TableOwner queue admission, deduplicating active replays.
    ///
    /// Reserve the queue slot first, then call this and synchronously send using
    /// that permit, without an intervening await. This prevents a worker from
    /// committing before registration. Replays retain the first accepted Instant.
    ///
    /// # Errors
    ///
    /// Fails closed for unconfigured, overflowing, mismatched, or out-of-bound
    /// accounting. This does not reject data admission; report the error explicitly.
    pub fn publication_started(
        &self,
        id: PublicationId,
        rows: u64,
        logical_bytes: u64,
        accepted_at: Instant,
    ) -> Result<(), String> {
        self.publication_backlog().start(id, rows, logical_bytes, accepted_at)
    }

    /// Removes a publication only after confirmed durable success.
    ///
    /// # Errors
    ///
    /// An untracked ID, duplicate completion, or previous accounting loss returns
    /// an error and leaves availability unknown. Errors never clear the cohort.
    pub fn publication_committed(&self, id: PublicationId) -> Result<(), String> {
        self.publication_backlog().commit(id)
    }

    /// Marks uncertain or lost publication outcomes unavailable until restart.
    pub fn publication_mark_unknown(&self) {
        self.publication_backlog().fail("unknown_outcome");
    }

    /// Samples publication counts and oldest monotonic age at the time of this call.
    #[must_use]
    pub fn publication_backlog_snapshot(&self) -> PublicationBacklogSnapshot {
        self.publication_backlog().snapshot(Instant::now())
    }

    /// Records the receiver's stable instance key and delivery mode once per process.
    ///
    /// Every pipeline core creates its own receiver from the same node
    /// configuration, so repeated identical calls succeed while conflicting
    /// values are rejected.
    pub(crate) fn configure_admission(&self, instance_key: &str, native_quiver: bool) -> Result<(), String> {
        if self.instance_key.get_or_init(|| instance_key.to_owned()) != instance_key {
            return Err("conflicting process-wide collector instance keys".to_owned());
        }
        if *self.native_quiver.get_or_init(|| native_quiver) != native_quiver {
            return Err("conflicting process-wide native Quiver settings".to_owned());
        }
        Ok(())
    }

    pub(crate) fn configure_cpu_pressure(&self, mode: CpuPressureMode) -> Result<(), String> {
        if *self.cpu_pressure_mode.get_or_init(|| mode) != mode {
            return Err("conflicting process-wide collector CPU pressure settings".to_owned());
        }
        Ok(())
    }

    fn sample_cpu_pressure(&self) -> Result<(), String> {
        let utilization = match self.cpu_pressure_mode.get().copied().unwrap_or_default() {
            CpuPressureMode::Off => 0,
            CpuPressureMode::Process => self
                .process_cpu_sampler
                .lock()
                .map_err(|_| "process CPU sampler lock is poisoned".to_owned())?
                .sample()?,
        };
        self.cpu_utilization_bps
            .store(u64::from(utilization), Ordering::Release);
        Ok(())
    }

    pub(crate) const fn instance_id(&self) -> CollectorInstanceId {
        self.instance_id
    }

    pub(crate) fn instance_key(&self) -> Option<&str> {
        self.instance_key.get().map(String::as_str)
    }

    /// Returns whether the receiver enabled durable admission through a native
    /// Quiver `processor:durable_buffer`.
    #[must_use]
    pub fn native_quiver_enabled(&self) -> bool {
        self.native_quiver.get().copied().unwrap_or(false)
    }

    /// Returns the delivery guarantee label of the configured admission mode.
    #[must_use]
    pub fn delivery_guarantee(&self) -> &'static str {
        DeliveryContract::select(self.native_quiver_enabled()).delivery_guarantee
    }

    pub(crate) fn stream_permits(&self, limit: usize) -> Result<Arc<tokio::sync::Semaphore>, String> {
        let (configured, permits) = self
            .stream_permits
            .get_or_init(|| (limit, Arc::new(tokio::sync::Semaphore::new(limit))));
        if *configured != limit {
            return Err("conflicting process-wide Arrow Flight stream limits".to_owned());
        }
        Ok(Arc::clone(permits))
    }

    pub(crate) fn configure_receiver(
        &self,
        max_concurrent_streams: usize,
        rss_limit_bytes: u64,
        buffered_bytes_limit: u64,
        buffered_rows_limit: u64,
        buffered_batches_limit: u64,
        dfe_channel_capacity: usize,
    ) {
        self.max_concurrent_streams.store(
            u64::try_from(max_concurrent_streams).unwrap_or(u64::MAX),
            Ordering::Release,
        );
        self.rss_limit_bytes.store(rss_limit_bytes, Ordering::Release);
        self.buffered_bytes_limit.store(buffered_bytes_limit, Ordering::Release);
        self.buffered_rows_limit.store(buffered_rows_limit, Ordering::Release);
        self.buffered_batches_limit
            .store(buffered_batches_limit, Ordering::Release);
        self.dfe_channel_capacity.store(
            u64::try_from(dfe_channel_capacity).unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }

    pub(crate) fn configure_exporter(&self, max_pending_commits: usize) {
        self.max_pending_commits.store(
            u64::try_from(max_pending_commits).unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }

    pub(crate) fn set_accepting(&self, accepting: bool) {
        self.accepting.store(accepting, Ordering::Release);
    }

    pub(crate) fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::Release);
    }

    pub(crate) fn set_catalog_ready(&self, ready: bool) {
        self.catalog_ready.store(ready, Ordering::Release);
    }

    pub(crate) fn set_registered(&self, registered: bool) {
        self.registered.store(registered, Ordering::Release);
    }

    pub(crate) fn stream_started(&self, table: &TableIdentity) {
        self.active_streams.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut tables) = self.tables.lock() {
            let state = tables.entry(table.clone()).or_default();
            state.active_streams = state.active_streams.saturating_add(1);
        }
    }

    pub(crate) fn stream_completed(&self, table: &TableIdentity) {
        saturating_sub_atomic(&self.active_streams, 1);
        self.admitted_streams.fetch_add(1, Ordering::Relaxed);
        self.update_table(table, |state| {
            state.active_streams = state.active_streams.saturating_sub(1);
        });
    }

    pub(crate) fn stream_aborted(&self, table: &TableIdentity) {
        saturating_sub_atomic(&self.active_streams, 1);
        self.aborted_streams.fetch_add(1, Ordering::Relaxed);
        self.update_table(table, |state| {
            state.active_streams = state.active_streams.saturating_sub(1);
        });
    }

    pub(crate) fn batch_received(&self, rows: u64, bytes: u64) {
        self.received_batches.fetch_add(1, Ordering::Relaxed);
        self.received_rows.fetch_add(rows, Ordering::Relaxed);
        self.otel.received_rows.add(rows, &self.otel.attributes);
        self.received_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn enqueue_failed(&self) {
        self.enqueue_failures.fetch_add(1, Ordering::Relaxed);
        self.otel.enqueue_failures.add(1, &self.otel.attributes);
    }

    pub(crate) fn batch_buffered(
        &self,
        table: &TableIdentity,
        rows: u64,
        bytes: u64,
        source: SchemaVersion,
        target: SchemaVersion,
        enqueued_at: Instant,
    ) {
        self.buffered_batches.fetch_add(1, Ordering::Relaxed);
        self.buffered_rows.fetch_add(rows, Ordering::Relaxed);
        self.buffered_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.update_table(table, |state| {
            state.buffered_batches = state.buffered_batches.saturating_add(1);
            state.buffered_rows = state.buffered_rows.saturating_add(rows);
            state.buffered_bytes = state.buffered_bytes.saturating_add(bytes);
            state.oldest_buffered_at = Some(
                state
                    .oldest_buffered_at
                    .map_or(enqueued_at, |oldest| oldest.min(enqueued_at)),
            );
            state.source_schema_version = Some(source.get());
            state.target_schema_version = Some(target.get());
        });
    }

    pub(crate) fn batch_resolved(
        &self,
        table: &TableIdentity,
        rows: u64,
        bytes: u64,
        downstream_ack: bool,
        remaining_oldest: Option<Instant>,
    ) {
        saturating_sub_atomic(&self.buffered_batches, 1);
        saturating_sub_atomic(&self.buffered_rows, rows);
        saturating_sub_atomic(&self.buffered_bytes, bytes);
        if downstream_ack {
            self.downstream_acks.fetch_add(1, Ordering::Relaxed);
            self.otel.downstream_acks.add(1, &self.otel.attributes);
        } else {
            self.downstream_nacks.fetch_add(1, Ordering::Relaxed);
            self.otel.downstream_nacks.add(1, &self.otel.attributes);
            self.otel.refused_batches.add(1, &self.otel.attributes);
        }
        self.update_table(table, |state| {
            state.buffered_batches = state.buffered_batches.saturating_sub(1);
            state.buffered_rows = state.buffered_rows.saturating_sub(rows);
            state.buffered_bytes = state.buffered_bytes.saturating_sub(bytes);
            state.oldest_buffered_at = remaining_oldest;
        });
    }

    pub(crate) fn publication_started(&self, table: &TableIdentity) {
        self.pending_commits.fetch_add(1, Ordering::Relaxed);
        self.update_table(table, |state| {
            state.pending_commits = state.pending_commits.saturating_add(1);
        });
    }

    pub(crate) fn observe_schema_versions(&self, table: &TableIdentity, source: SchemaVersion, target: SchemaVersion) {
        self.update_table(table, |state| {
            state.source_schema_version = Some(source.get());
            state.target_schema_version = Some(target.get());
        });
    }

    pub(crate) fn publication_finished(
        &self,
        table: &TableIdentity,
        rows: u64,
        bytes: u64,
        delta_version: Option<u64>,
        files: u64,
    ) {
        saturating_sub_atomic(&self.pending_commits, 1);
        self.update_table(table, |state| {
            state.pending_commits = state.pending_commits.saturating_sub(1);
            if let Some(version) = delta_version {
                state.last_delta_version = Some(version);
            } else {
                state.publication_failures = state.publication_failures.saturating_add(1);
            }
        });
        if delta_version.is_some() {
            self.delta_files.fetch_add(files, Ordering::Relaxed);
            self.delta_rows.fetch_add(rows, Ordering::Relaxed);
            self.otel.delta_rows.add(rows, &self.otel.attributes);
            self.delta_bytes.fetch_add(bytes, Ordering::Relaxed);
        } else {
            self.delta_failures.fetch_add(1, Ordering::Relaxed);
            self.otel.delta_failures.add(1, &self.otel.attributes);
        }
    }

    fn update_table(&self, table: &TableIdentity, update: impl FnOnce(&mut TableState)) {
        if let Ok(mut tables) = self.tables.lock() {
            update(tables.entry(table.clone()).or_default());
        }
    }

    /// Adds one batch the durable buffer acknowledged to this process's durable backlog.
    pub(crate) fn durable_backlog_admitted(&self, table: &TableIdentity, rows: u64, bytes: u64) {
        self.durable_admitted_batches.fetch_add(1, Ordering::Relaxed);
        self.update_table(table, |state| state.durable.admit(rows, bytes));
    }

    /// Removes one batch admitted by this process once it was published or permanently refused.
    pub(crate) fn durable_backlog_released(&self, table: &TableIdentity, rows: u64, bytes: u64) {
        self.durable_released_batches.fetch_add(1, Ordering::Relaxed);
        self.update_table(table, |state| state.durable.release(rows, bytes));
    }

    pub(crate) fn durable_admission_failed(&self) {
        self.durable_admission_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn replayed_batch(&self) {
        self.replayed_batches.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn rebound_batch(&self) {
        self.rebound_batches.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn replay_deferred(&self) {
        self.replay_deferred_batches.fetch_add(1, Ordering::Relaxed);
    }

    /// Sums the clamped per-table durable backlog; zero while native Quiver is disabled.
    fn durable_totals(&self) -> DurableTotals {
        if !self.native_quiver_enabled() {
            return DurableTotals::default();
        }
        self.tables.lock().map_or_else(
            |_| DurableTotals::default(),
            |tables| {
                tables.values().fold(DurableTotals::default(), |total, state| {
                    total.add(state.durable.totals())
                })
            },
        )
    }

    /// Returns current process health.
    #[must_use]
    pub fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    /// Returns routing readiness and admission state.
    #[must_use]
    pub fn ready(&self) -> bool {
        self.healthy()
            && self.catalog_ready.load(Ordering::Acquire)
            && self.registered.load(Ordering::Acquire)
            && self.accepting.load(Ordering::Acquire)
    }

    /// Returns the dashboard capability document with explicit delivery semantics.
    ///
    /// With native Quiver enabled the stream completes only after the durable
    /// buffer persisted every batch, so acknowledged data survives a crash and
    /// is replayed (`at_least_once`; duplicates are possible after a retry or
    /// a replay). Neither mode creates SQL receipts or supports receipt polling.
    #[must_use]
    pub fn capabilities(&self, max_request_bytes: u64) -> DashboardCapabilities {
        let native_quiver = self.native_quiver_enabled();
        let contract = DeliveryContract::select(native_quiver);
        DashboardCapabilities {
            protocol_version: ProtocolVersion::CURRENT,
            instance_id: self.instance_id,
            implementation_pool: ImplementationPool::Dataflow,
            delivery_guarantee: contract.delivery_guarantee,
            completion_mode: contract.completion_mode,
            receipt_polling: false,
            sql_delivery_receipts: false,
            at_least_once: contract.at_least_once,
            max_request_bytes,
            max_concurrent_streams: self.max_concurrent_streams.load(Ordering::Acquire),
            capabilities: [
                Capability::DeltaPublication,
                Capability::TableLeaseFencing,
                Capability::PressureReporting,
            ],
            arbitrary_arrow: true,
            pdata_signal_discriminator: "logs",
            pdata_signal_semantics: "representation_local_not_otap",
            strict_catalog_validation: true,
            flight_sql_control: true,
            dfe_backpressure: true,
            native_quiver: NativeQuiverCapability {
                supported: true,
                enabled: native_quiver,
            },
        }
    }

    pub(crate) async fn publish_pressure(&self, store: &dyn RoutingStore) -> Result<(), String> {
        let mut previous = self.pressure_publication.lock().await;
        self.sample_cpu_pressure()?;
        let mut snapshot = self.pressure()?;
        while let Some(last) = *previous
            && snapshot.rich.observed_at_unix_ms <= last
        {
            let wait = last.get() - snapshot.rich.observed_at_unix_ms.get() + 1;
            tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
            snapshot = self.pressure()?;
        }
        // Reserve the timestamp before I/O, including when cancellation leaves its outcome unknown.
        *previous = Some(snapshot.rich.observed_at_unix_ms);
        store
            .publish_pressure(&snapshot.rich)
            .await
            .map_err(|error| error.to_string())
    }

    /// Builds the bounded router pressure document and dashboard annotations.
    ///
    /// Buffered bytes, rows, and batches cover Flight batches awaiting their
    /// first DFE acknowledgement plus, with native Quiver enabled, the durable
    /// backlog: batches the durable buffer acknowledged to this process's
    /// receiver that the exporter has not yet published or permanently
    /// refused. `quiver_state` and `quiver_pressure_bps` describe that backlog
    /// against the configured `registration.buffered_*_limit` values, while
    /// `dfe_state` and `dfe_pressure_bps` keep describing in-memory DFE work.
    /// `oldest_buffer_age_ms` covers only in-memory DFE work.
    ///
    /// # Errors
    ///
    /// Returns a clock or bounded-measurement error when a snapshot cannot be
    /// represented by the routing contract.
    pub fn pressure(&self) -> Result<DashboardPressure, String> {
        let now = now_unix_millis()?;
        let native_quiver = self.native_quiver_enabled();
        let active = self.active_streams.load(Ordering::Acquire);
        let dfe_bytes = self.buffered_bytes.load(Ordering::Acquire);
        let dfe_rows = self.buffered_rows.load(Ordering::Acquire);
        let dfe_batches = self.buffered_batches.load(Ordering::Acquire);
        let backlog = self.durable_totals();
        let buffered_bytes = dfe_bytes.saturating_add(backlog.bytes);
        let buffered_rows = dfe_rows.saturating_add(backlog.rows);
        let buffered_batches = dfe_batches.saturating_add(backlog.batches);
        let pending_commits = self.pending_commits.load(Ordering::Acquire);
        let max_streams = self.max_concurrent_streams.load(Ordering::Acquire).max(1);
        let rss_limit = self.rss_limit_bytes.load(Ordering::Acquire).max(1);
        let actual_rss = resident_memory_bytes().unwrap_or(0);
        let bounded_rss = actual_rss.min(rss_limit);
        let bytes_limit = self.buffered_bytes_limit.load(Ordering::Acquire).max(1);
        let rows_limit = self.buffered_rows_limit.load(Ordering::Acquire).max(1);
        let batches_limit = self.buffered_batches_limit.load(Ordering::Acquire).max(1);
        let commit_limit = self.max_pending_commits.load(Ordering::Acquire).max(1);
        let available = max_streams.saturating_sub(active.min(max_streams));
        let oldest_age = self.oldest_buffer_age();
        let dfe_accepting = self.accepting.load(Ordering::Acquire)
            && actual_rss <= rss_limit
            && dfe_bytes <= bytes_limit
            && dfe_rows <= rows_limit
            && dfe_batches <= batches_limit
            && pending_commits <= commit_limit;
        let accepting = dfe_accepting
            && buffered_bytes <= bytes_limit
            && buffered_rows <= rows_limit
            && buffered_batches <= batches_limit;
        let tables = self.table_pressure(native_quiver);
        let dfe_pressure = ratio_bps(dfe_batches.min(batches_limit), batches_limit)?;
        let delta_pressure = ratio_bps(pending_commits.min(commit_limit), commit_limit)?;
        let (quiver_pressure, quiver_state) = if native_quiver {
            let pressure = [
                ratio_bps(backlog.bytes, bytes_limit)?.get(),
                ratio_bps(backlog.rows, rows_limit)?.get(),
                ratio_bps(backlog.batches, batches_limit)?.get(),
            ]
            .into_iter()
            .max()
            .unwrap_or(0);
            let state = if backlog.bytes > bytes_limit || backlog.rows > rows_limit || backlog.batches > batches_limit {
                SubsystemState::Blocked
            } else if pressure >= QUIVER_DEGRADED_BPS {
                SubsystemState::Degraded
            } else {
                SubsystemState::Ready
            };
            (BasisPoints::new(pressure).map_err(|error| error.to_string())?, state)
        } else {
            (
                BasisPoints::new(0).map_err(|error| error.to_string())?,
                SubsystemState::Disabled,
            )
        };
        let rich = RichPressureSnapshot {
            instance_id: self.instance_id,
            observed_at_unix_ms: now,
            cpu_utilization_bps: BasisPoints::new(
                u16::try_from(self.cpu_utilization_bps.load(Ordering::Acquire)).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?,
            rss_bytes: measurement(bounded_rss)?,
            rss_limit_bytes: positive(rss_limit)?,
            active_streams: gauge(active, max_streams)?,
            received_bytes: measurement(self.received_bytes.load(Ordering::Acquire))?,
            received_rows: measurement(self.received_rows.load(Ordering::Acquire))?,
            received_batches: measurement(self.received_batches.load(Ordering::Acquire))?,
            buffered_bytes: gauge(buffered_bytes, bytes_limit)?,
            buffered_rows: gauge(buffered_rows, rows_limit)?,
            buffered_batches: gauge(buffered_batches, batches_limit)?,
            available_permits: gauge(available, max_streams)?,
            pending_commits: gauge(pending_commits, commit_limit)?,
            oldest_buffer_age_ms: measurement(oldest_age)?,
            delta_pressure_bps: delta_pressure,
            quiver_pressure_bps: quiver_pressure,
            dfe_pressure_bps: dfe_pressure,
            delta_state: if pending_commits > commit_limit {
                SubsystemState::Blocked
            } else if pending_commits > 0 {
                SubsystemState::Degraded
            } else {
                SubsystemState::Ready
            },
            quiver_state,
            dfe_state: if !dfe_accepting {
                SubsystemState::Blocked
            } else if dfe_batches > 0 {
                SubsystemState::Degraded
            } else {
                SubsystemState::Ready
            },
            accepting_deliveries: accepting,
            tables,
        };
        Ok(DashboardPressure {
            rich,
            table_versions: self.table_versions(),
            actual_rss_bytes: actual_rss,
            dfe_channel_capacity: self.dfe_channel_capacity.load(Ordering::Acquire),
            dfe_channel_occupancy: dfe_batches,
            dfe_channel_occupancy_source: if native_quiver {
                "ack_tracked_until_durable_admission"
            } else {
                "ack_tracked_end_to_end"
            },
            native_quiver_enabled: native_quiver,
            native_quiver_buffered_bytes: native_quiver.then_some(backlog.bytes),
            native_quiver_buffered_rows: native_quiver.then_some(backlog.rows),
            native_quiver_buffered_batches: native_quiver.then_some(backlog.batches),
            native_quiver_backlog_source: native_quiver.then_some("durable_acks_minus_exporter_resolutions"),
            replayed_batches: self.replayed_batches.load(Ordering::Acquire),
            rebound_batches: self.rebound_batches.load(Ordering::Acquire),
            replay_deferred_batches: self.replay_deferred_batches.load(Ordering::Acquire),
        })
    }

    fn oldest_buffer_age(&self) -> u64 {
        self.tables
            .lock()
            .ok()
            .and_then(|tables| tables.values().filter_map(|state| state.oldest_buffered_at).min())
            .map_or(0, |oldest| {
                duration_millis(Instant::now().saturating_duration_since(oldest))
            })
    }

    fn table_pressure(&self, native_quiver: bool) -> Vec<TablePressure> {
        self.tables.lock().map_or_else(
            |_| Vec::new(),
            |tables| {
                tables
                    .iter()
                    .take(MAX_TABLE_PRESSURE_ENTRIES)
                    .filter_map(|(table, state)| {
                        let backlog = if native_quiver {
                            state.durable.totals()
                        } else {
                            DurableTotals::default()
                        };
                        let buffered_batches = state.buffered_batches.saturating_add(backlog.batches);
                        let effective_active = state
                            .active_streams
                            .saturating_add(buffered_batches)
                            .saturating_add(state.pending_commits);
                        Some(TablePressure {
                            table: table.clone(),
                            active_streams: Measurement::new(effective_active).ok()?,
                            buffered_bytes: Measurement::new(state.buffered_bytes.saturating_add(backlog.bytes))
                                .ok()?,
                            buffered_rows: Measurement::new(state.buffered_rows.saturating_add(backlog.rows)).ok()?,
                            buffered_batches: Measurement::new(buffered_batches).ok()?,
                            oldest_buffer_age_ms: Measurement::new(state.oldest_buffered_at.map_or(0, |oldest| {
                                duration_millis(Instant::now().saturating_duration_since(oldest))
                            }))
                            .ok()?,
                            pending_commits: Measurement::new(state.pending_commits).ok()?,
                        })
                    })
                    .collect()
            },
        )
    }

    fn table_versions(&self) -> Vec<TableVersionStatus> {
        self.tables.lock().map_or_else(
            |_| Vec::new(),
            |tables| {
                tables
                    .iter()
                    .take(MAX_TABLE_PRESSURE_ENTRIES)
                    .map(|(table, state)| TableVersionStatus {
                        table: table.clone(),
                        source_schema_version: state.source_schema_version,
                        target_schema_version: state.target_schema_version,
                        last_delta_version: state.last_delta_version,
                        publication_failures: state.publication_failures,
                    })
                    .collect()
            },
        )
    }

    /// Encodes a compact Prometheus text exposition.
    #[must_use]
    pub fn prometheus(&self) -> String {
        let mut output = String::new();
        self.publication_backlog_snapshot().write(&mut output);
        self.otel.record_pressure(
            self.buffered_rows.load(Ordering::Acquire),
            self.buffered_batches.load(Ordering::Acquire),
            self.oldest_buffer_age() as f64 / 1_000.0,
            self.pending_commits.load(Ordering::Acquire),
        );
        for (name, value) in [
            (
                "arrowcol_flight_active_streams",
                self.active_streams.load(Ordering::Acquire),
            ),
            (
                "arrowcol_received_batches_total",
                self.received_batches.load(Ordering::Acquire),
            ),
            (
                "arrowcol_received_rows_total",
                self.received_rows.load(Ordering::Acquire),
            ),
            (
                "arrowcol_received_bytes_total",
                self.received_bytes.load(Ordering::Acquire),
            ),
            (
                "arrowcol_dfe_buffered_batches",
                self.buffered_batches.load(Ordering::Acquire),
            ),
            (
                "arrowcol_dfe_buffered_bytes",
                self.buffered_bytes.load(Ordering::Acquire),
            ),
            (
                "arrowcol_delta_pending_commits",
                self.pending_commits.load(Ordering::Acquire),
            ),
            ("arrowcol_delta_files_total", self.delta_files.load(Ordering::Acquire)),
            ("arrowcol_delta_rows_total", self.delta_rows.load(Ordering::Acquire)),
            ("arrowcol_delta_bytes_total", self.delta_bytes.load(Ordering::Acquire)),
            (
                "arrowcol_delta_failures_total",
                self.delta_failures.load(Ordering::Acquire),
            ),
            (
                "arrowcol_downstream_acks_total",
                self.downstream_acks.load(Ordering::Acquire),
            ),
            (
                "arrowcol_downstream_nacks_total",
                self.downstream_nacks.load(Ordering::Acquire),
            ),
        ] {
            let _ = writeln!(output, "# TYPE {name} gauge");
            let _ = writeln!(output, "{name} {value}");
        }
        let _ = writeln!(
            output,
            "# HELP arrowcol_dfe_enqueue_failures_total Batches rejected before DFE queue admission."
        );
        output.push_str("# TYPE arrowcol_dfe_enqueue_failures_total counter\n");
        let _ = writeln!(
            output,
            "arrowcol_dfe_enqueue_failures_total {}",
            self.enqueue_failures.load(Ordering::Acquire)
        );
        let _ = writeln!(
            output,
            "# HELP arrowcol_dfe_buffered_rows Rows awaiting DFE acknowledgement."
        );
        output.push_str("# TYPE arrowcol_dfe_buffered_rows gauge\n");
        let _ = writeln!(
            output,
            "arrowcol_dfe_buffered_rows {}",
            self.buffered_rows.load(Ordering::Acquire)
        );
        let _ = writeln!(
            output,
            "# HELP arrowcol_dfe_oldest_pending_age_seconds Age of the oldest batch awaiting DFE acknowledgement."
        );
        output.push_str("# TYPE arrowcol_dfe_oldest_pending_age_seconds gauge\n");
        let age_seconds = self.oldest_buffer_age() as f64 / 1_000.0;
        let _ = writeln!(output, "arrowcol_dfe_oldest_pending_age_seconds {age_seconds}");
        let backlog = self.durable_totals();
        for (name, help, value) in [
            (
                "arrowcol_dataflow_durable_backlog_batches",
                "Batches the durable buffer acknowledged to this process that are not yet published or permanently refused.",
                backlog.batches,
            ),
            (
                "arrowcol_dataflow_durable_backlog_rows",
                "Rows in the durable backlog.",
                backlog.rows,
            ),
            (
                "arrowcol_dataflow_durable_backlog_bytes",
                "Logical Arrow bytes in the durable backlog.",
                backlog.bytes,
            ),
        ] {
            let _ = writeln!(output, "# HELP {name} {help}");
            let _ = writeln!(output, "# TYPE {name} gauge");
            let _ = writeln!(output, "{name} {value}");
        }
        for (name, help, value) in [
            (
                "arrowcol_dataflow_durable_admitted_batches_total",
                "Batches the durable buffer acknowledged to this process's receiver.",
                self.durable_admitted_batches.load(Ordering::Acquire),
            ),
            (
                "arrowcol_dataflow_durable_released_batches_total",
                "Batches admitted by this process that the exporter published or permanently refused.",
                self.durable_released_batches.load(Ordering::Acquire),
            ),
            (
                "arrowcol_dataflow_durable_admission_failures_total",
                "Flight streams failed because the durable buffer rejected a batch or did not acknowledge it in time.",
                self.durable_admission_failures.load(Ordering::Acquire),
            ),
            (
                "arrowcol_dataflow_replayed_batches_total",
                "Export attempts for batches admitted by a predecessor process lifetime, including retries.",
                self.replayed_batches.load(Ordering::Acquire),
            ),
            (
                "arrowcol_dataflow_rebound_batches_total",
                "Export attempts whose envelope lease was rebound to the current lease held by this process.",
                self.rebound_batches.load(Ordering::Acquire),
            ),
            (
                "arrowcol_dataflow_replay_deferred_total",
                "Export attempts refused transiently because the batch could not be bound to a lease held by this process.",
                self.replay_deferred_batches.load(Ordering::Acquire),
            ),
        ] {
            let _ = writeln!(output, "# HELP {name} {help}");
            let _ = writeln!(output, "# TYPE {name} counter");
            let _ = writeln!(output, "{name} {value}");
        }
        let contract = DeliveryContract::select(self.native_quiver_enabled());
        let _ = writeln!(output, "# HELP arrowcol_delivery_guarantee_info {}", contract.help);
        output.push_str("# TYPE arrowcol_delivery_guarantee_info gauge\n");
        let _ = writeln!(
            output,
            "arrowcol_delivery_guarantee_info{{delivery_guarantee=\"{}\",completion_mode=\"{}\",receipt_polling=\"false\",at_least_once=\"{}\"}} 1",
            contract.delivery_guarantee, contract.completion_mode, contract.at_least_once,
        );
        PIPELINE_METRICS.write(&mut output);
        output
    }
}

/// Dashboard-facing capability document.
#[derive(Clone, Debug, Serialize)]
pub struct DashboardCapabilities {
    protocol_version: ProtocolVersion,
    instance_id: CollectorInstanceId,
    implementation_pool: ImplementationPool,
    delivery_guarantee: &'static str,
    completion_mode: &'static str,
    receipt_polling: bool,
    sql_delivery_receipts: bool,
    at_least_once: bool,
    max_request_bytes: u64,
    max_concurrent_streams: u64,
    capabilities: [Capability; 3],
    arbitrary_arrow: bool,
    pdata_signal_discriminator: &'static str,
    pdata_signal_semantics: &'static str,
    strict_catalog_validation: bool,
    flight_sql_control: bool,
    dfe_backpressure: bool,
    native_quiver: NativeQuiverCapability,
}

/// Native durable-buffer capability.
///
/// `supported` is always true: this build persists `arrowcol.flight@1.1`
/// through `processor:durable_buffer`. `enabled` reports whether the receiver
/// was configured with `native_quiver: true`.
#[derive(Clone, Debug, Serialize)]
pub struct NativeQuiverCapability {
    supported: bool,
    enabled: bool,
}

/// Rich pressure plus implementation-specific occupancy provenance.
#[derive(Clone, Debug, Serialize)]
pub struct DashboardPressure {
    /// Router-consumable rich pressure document.
    pub rich: RichPressureSnapshot,
    /// Catalog and Delta versions observed per bounded table entry.
    pub table_versions: Vec<TableVersionStatus>,
    actual_rss_bytes: u64,
    dfe_channel_capacity: u64,
    dfe_channel_occupancy: u64,
    dfe_channel_occupancy_source: &'static str,
    native_quiver_enabled: bool,
    native_quiver_buffered_bytes: Option<u64>,
    native_quiver_buffered_rows: Option<u64>,
    native_quiver_buffered_batches: Option<u64>,
    native_quiver_backlog_source: Option<&'static str>,
    replayed_batches: u64,
    rebound_batches: u64,
    replay_deferred_batches: u64,
}

/// Version lineage retained for one logical table.
#[derive(Clone, Debug, Serialize)]
pub struct TableVersionStatus {
    table: TableIdentity,
    source_schema_version: Option<u64>,
    target_schema_version: Option<u64>,
    last_delta_version: Option<u64>,
    publication_failures: u64,
}

#[derive(Clone, Debug)]
struct PendingBatch {
    table: TableIdentity,
    rows: u64,
    bytes: u64,
    enqueued_at: Instant,
    durable: Option<Arc<DurableStream>>,
}

/// Feedback counts for one durable-admission Flight stream.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DurableProgress {
    /// Batches sent downstream with durable-admission feedback requested.
    pub(crate) sent: u64,
    /// Batches the durable buffer acknowledged after its WAL append.
    pub(crate) acknowledged: u64,
    /// Batches the durable buffer refused, or whose send failed or was cancelled.
    pub(crate) rejected: u64,
}

impl DurableProgress {
    /// Returns whether the stream's outcome is known: one rejection fails it,
    /// and it succeeds once every sent batch was acknowledged.
    pub(crate) const fn settled(self) -> bool {
        self.rejected > 0 || self.acknowledged >= self.sent
    }
}

/// Durable-admission progress for one Flight stream.
///
/// The stream lifecycle and each of its pending DFE entries share this state,
/// so feedback for any correlation ID wakes the stream's completion. Entries
/// release their reference when their feedback arrives, and an abandoned
/// stream holds nothing else, so aborted streams leak no waiters.
#[derive(Debug)]
pub(crate) struct DurableStream {
    progress: watch::Sender<DurableProgress>,
}

impl DurableStream {
    pub(crate) fn new() -> Self {
        Self {
            progress: watch::Sender::new(DurableProgress::default()),
        }
    }

    fn register(&self) {
        self.progress
            .send_modify(|progress| progress.sent = progress.sent.saturating_add(1));
    }

    fn record(&self, ack: bool) {
        self.progress.send_modify(|progress| {
            if ack {
                progress.acknowledged = progress.acknowledged.saturating_add(1);
            } else {
                progress.rejected = progress.rejected.saturating_add(1);
            }
        });
    }

    /// Returns the current feedback counts.
    pub(crate) fn progress(&self) -> DurableProgress {
        *self.progress.borrow()
    }

    /// Waits until the stream is settled, or returns `None` after `timeout`.
    pub(crate) async fn settled(&self, timeout: Duration) -> Option<DurableProgress> {
        let mut progress = self.progress.subscribe();
        let settled = tokio::time::timeout(timeout, progress.wait_for(|progress| progress.settled())).await;
        match settled {
            Ok(Ok(progress)) => Some(*progress),
            Ok(Err(_)) | Err(_) => None,
        }
    }
}

/// Correlates DFE Ack/Nack feedback with pressure accounting and durable admission.
pub(crate) struct PendingTracker {
    metrics: Arc<RuntimeMetrics>,
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, PendingBatch>>,
}

impl PendingTracker {
    pub(crate) fn new(metrics: Arc<RuntimeMetrics>) -> Self {
        Self {
            metrics,
            next_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    pub(crate) fn insert(
        &self,
        table: TableIdentity,
        rows: u64,
        bytes: u64,
        source: SchemaVersion,
        target: SchemaVersion,
    ) -> Result<u64, String> {
        self.insert_for_stream(table, rows, bytes, source, target, None)
    }

    /// Registers one outbound batch. With a durable stream, its feedback also
    /// settles the stream and an ACK moves the batch into the durable backlog.
    pub(crate) fn insert_for_stream(
        &self,
        table: TableIdentity,
        rows: u64,
        bytes: u64,
        source: SchemaVersion,
        target: SchemaVersion,
        durable: Option<Arc<DurableStream>>,
    ) -> Result<u64, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            return Err("DFE message correlation ID exhausted".to_owned());
        }
        let enqueued_at = Instant::now();
        self.metrics
            .batch_buffered(&table, rows, bytes, source, target, enqueued_at);
        if let Some(stream) = &durable {
            stream.register();
        }
        self.pending
            .lock()
            .map_err(|_| "pending DFE tracker lock poisoned".to_owned())?
            .insert(
                id,
                PendingBatch {
                    table,
                    rows,
                    bytes,
                    enqueued_at,
                    durable,
                },
            );
        Ok(id)
    }

    pub(crate) fn resolve(&self, id: u64, ack: bool) {
        let Ok(mut pending) = self.pending.lock() else {
            return;
        };
        let Some(batch) = pending.remove(&id) else {
            return;
        };
        let remaining_oldest = pending
            .values()
            .filter(|other| other.table == batch.table)
            .map(|other| other.enqueued_at)
            .min();
        drop(pending);
        // Count the durable backlog before releasing in-memory pressure so a
        // concurrent snapshot never under-reports the acknowledged batch.
        if ack && batch.durable.is_some() {
            self.metrics
                .durable_backlog_admitted(&batch.table, batch.rows, batch.bytes);
        }
        self.metrics
            .batch_resolved(&batch.table, batch.rows, batch.bytes, ack, remaining_oldest);
        if let Some(stream) = batch.durable {
            stream.record(ack);
        }
    }

    #[cfg(test)]
    pub(crate) fn pending_ids(&self) -> Vec<u64> {
        self.pending
            .lock()
            .map(|pending| pending.keys().copied().collect())
            .unwrap_or_default()
    }
}

fn measurement(value: u64) -> Result<Measurement, String> {
    Measurement::new(value).map_err(|error| error.to_string())
}

fn positive(value: u64) -> Result<PositiveMeasurement, String> {
    PositiveMeasurement::new(value).map_err(|error| error.to_string())
}

fn gauge(current: u64, limit: u64) -> Result<PressureGauge, String> {
    PressureGauge::new(measurement(current.min(limit))?, positive(limit)?).map_err(|error| error.to_string())
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn ratio_bps(current: u64, limit: u64) -> Result<BasisPoints, String> {
    let scaled = u128::from(current) * u128::from(BasisPoints::FULL_SCALE);
    let ratio = scaled / u128::from(limit.max(1));
    let ratio = u16::try_from(ratio)
        .unwrap_or(BasisPoints::FULL_SCALE)
        .min(BasisPoints::FULL_SCALE);
    BasisPoints::new(ratio).map_err(|error| error.to_string())
}

fn saturating_sub_atomic(value: &AtomicU64, decrement: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(decrement))
    });
}

fn resident_memory_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    kib.checked_mul(1_024)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use arrowcol_protocol::{CatalogName, SchemaName, TableName};

    use super::*;

    #[test]
    fn process_cpu_pressure_ratio_uses_effective_cores_and_clamps() {
        assert_eq!(
            process_cpu_ratio_bps(Duration::from_millis(500), Duration::from_secs(1), 2.0).expect("valid CPU ratio"),
            2_500
        );
        assert_eq!(
            process_cpu_ratio_bps(Duration::from_secs(2), Duration::from_secs(1), 1.0).expect("clamped CPU ratio"),
            10_000
        );
        assert!(process_cpu_ratio_bps(Duration::ZERO, Duration::ZERO, 1.0).is_err());
    }

    #[test]
    fn first_process_cpu_pressure_sample_is_zero() {
        assert_eq!(ProcessCpuSampler::default().sample().expect("first sample"), 0);
    }

    #[test]
    fn cpu_pressure_off_keeps_the_legacy_zero_gauge() {
        let pressure = RuntimeMetrics::new(CollectorInstanceId::new())
            .pressure()
            .expect("pressure snapshot");
        assert_eq!(pressure.rich.cpu_utilization_bps.get(), 0);
    }

    fn table() -> TableIdentity {
        TableIdentity::new(
            CatalogName::new("demo").expect("catalog"),
            SchemaName::new("generated").expect("schema"),
            TableName::new("events").expect("table"),
        )
    }

    /// Scenario: DFE work remains outstanding after Flight best-effort completion.
    /// Guarantees: Pressure remains nonzero until downstream Ack/Nack feedback resolves it.
    #[test]
    fn ack_tracked_pressure_is_honest() {
        let metrics = Arc::new(RuntimeMetrics::new(CollectorInstanceId::new()));
        metrics.configure_receiver(4, 1_000_000, 1_000_000, 1_000, 10, 10);
        metrics.configure_exporter(4);
        let tracker = PendingTracker::new(Arc::clone(&metrics));
        let id = tracker
            .insert(table(), 3, 128, SchemaVersion::FIRST, SchemaVersion::FIRST)
            .expect("insert");
        let before = metrics.pressure().expect("pressure");
        assert_eq!(before.rich.buffered_batches.current().get(), 1);
        tracker.resolve(id, true);
        let after = metrics.pressure().expect("pressure");
        assert_eq!(after.rich.buffered_batches.current().get(), 0);
    }

    #[test]
    fn prometheus_exposes_pending_rows_and_monotonic_oldest_age() {
        let metrics = Arc::new(RuntimeMetrics::new(CollectorInstanceId::new()));
        let tracker = PendingTracker::new(Arc::clone(&metrics));
        let id = tracker
            .insert(table(), 3, 128, SchemaVersion::FIRST, SchemaVersion::FIRST)
            .expect("insert");
        std::thread::sleep(Duration::from_millis(2));

        let exposition = metrics.prometheus();
        assert!(exposition.contains("arrowcol_dfe_buffered_rows 3\n"));
        let age = exposition
            .lines()
            .find(|line| line.starts_with("arrowcol_dfe_oldest_pending_age_seconds "))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<f64>().ok())
            .expect("oldest pending age metric");
        assert!(age > 0.0);

        tracker.resolve(id, true);
        let exposition = metrics.prometheus();
        assert!(exposition.contains("arrowcol_dfe_buffered_rows 0\n"));
        assert!(exposition.contains("arrowcol_dfe_oldest_pending_age_seconds 0\n"));
    }

    /// Scenario: Operators inspect DFE delivery claims while native Quiver is disabled.
    /// Guarantees: Capability metadata stays best-effort without receipt polling or an at-least-once
    /// claim, reports native Quiver as supported but disabled, and keeps the legacy info metric.
    #[test]
    fn capabilities_label_best_effort() {
        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        metrics.configure_admission("dfe", false).expect("admission mode");
        let encoded = serde_json::to_value(metrics.capabilities(1024)).expect("capabilities JSON");
        assert_eq!(encoded["delivery_guarantee"], "best_effort");
        assert_eq!(encoded["completion_mode"], "best_effort_admission");
        assert_eq!(encoded["receipt_polling"], false);
        assert_eq!(encoded["sql_delivery_receipts"], false);
        assert_eq!(encoded["at_least_once"], false);
        assert_eq!(encoded["pdata_signal_discriminator"], "logs");
        assert_eq!(encoded["pdata_signal_semantics"], "representation_local_not_otap");
        assert_eq!(
            encoded["native_quiver"],
            serde_json::json!({"supported": true, "enabled": false})
        );
        assert_eq!(metrics.delivery_guarantee(), "best_effort");
        assert!(metrics.prometheus().contains(
            "arrowcol_delivery_guarantee_info{delivery_guarantee=\"best_effort\",completion_mode=\"best_effort_admission\",receipt_polling=\"false\",at_least_once=\"false\"} 1\n"
        ));
        let pressure = serde_json::to_value(metrics.pressure().expect("pressure")).expect("pressure JSON");
        assert_eq!(pressure["rich"]["quiver_state"], "disabled");
        assert_eq!(pressure["native_quiver_enabled"], false);
        assert_eq!(pressure["native_quiver_buffered_bytes"], serde_json::Value::Null);
        assert_eq!(pressure["dfe_channel_occupancy_source"], "ack_tracked_end_to_end");
        assert!(pressure.get("native_quiver_feature_gap").is_none());
    }

    /// Scenario: Operators inspect DFE delivery claims while native Quiver durable admission is enabled.
    /// Guarantees: Capabilities, readiness labels, and the Prometheus info metric report
    /// `durable_admission` with at-least-once delivery and without receipts or receipt polling.
    #[test]
    fn capabilities_label_durable_admission() {
        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        metrics.configure_admission("dfe", true).expect("admission mode");
        let encoded = serde_json::to_value(metrics.capabilities(1024)).expect("capabilities JSON");
        assert_eq!(encoded["delivery_guarantee"], "durable_admission");
        assert_eq!(encoded["completion_mode"], "durable_admission");
        assert_eq!(encoded["receipt_polling"], false);
        assert_eq!(encoded["sql_delivery_receipts"], false);
        assert_eq!(encoded["at_least_once"], true);
        assert_eq!(
            encoded["native_quiver"],
            serde_json::json!({"supported": true, "enabled": true})
        );
        assert_eq!(metrics.delivery_guarantee(), "durable_admission");
        let exposition = metrics.prometheus();
        assert!(exposition.contains(
            "arrowcol_delivery_guarantee_info{delivery_guarantee=\"durable_admission\",completion_mode=\"durable_admission\",receipt_polling=\"false\",at_least_once=\"true\"} 1\n"
        ));
        assert!(exposition.contains("# TYPE arrowcol_dataflow_rebound_batches_total counter\n"));
        let pressure = serde_json::to_value(metrics.pressure().expect("pressure")).expect("pressure JSON");
        assert_eq!(pressure["rich"]["quiver_state"], "ready");
        assert_eq!(pressure["native_quiver_enabled"], true);
        assert_eq!(pressure["native_quiver_buffered_bytes"], 0);
        assert_eq!(
            pressure["native_quiver_backlog_source"],
            "durable_acks_minus_exporter_resolutions"
        );
    }

    /// Scenario: The receiver admits durable batches and the exporter releases them, including a release
    /// observed before its acknowledgement and a replayed layout measured a few bytes smaller.
    /// Guarantees: Global and per-table pressure include the backlog, `quiver_state` follows the
    /// configured limits without blocking DFE state, and counters re-anchor to zero once the backlog empties.
    #[test]
    fn durable_backlog_pressure_is_honest() {
        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        metrics.configure_receiver(4, u64::MAX / 4, 1_000, 100, 4, 4);
        metrics.configure_exporter(4);
        metrics.configure_admission("dfe", true).expect("admission mode");
        let identity = table();
        metrics.durable_backlog_admitted(&identity, 3, 128);
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.buffered_batches.current().get(), 1);
        assert_eq!(pressure.rich.buffered_rows.current().get(), 3);
        assert_eq!(pressure.rich.buffered_bytes.current().get(), 128);
        assert_eq!(pressure.rich.quiver_pressure_bps.get(), 2_500);
        assert_eq!(pressure.rich.quiver_state, SubsystemState::Ready);
        assert_eq!(pressure.rich.tables[0].buffered_batches.get(), 1);
        assert_eq!(pressure.rich.tables[0].active_streams.get(), 1);
        assert!(!pressure.rich.tables[0].is_drained());

        for _ in 0..3 {
            metrics.durable_backlog_admitted(&identity, 3, 128);
        }
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.quiver_pressure_bps.get(), 10_000);
        assert_eq!(pressure.rich.quiver_state, SubsystemState::Degraded);
        assert!(pressure.rich.accepting_deliveries);

        metrics.durable_backlog_admitted(&identity, 3, 128);
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.quiver_state, SubsystemState::Blocked);
        assert_eq!(pressure.rich.dfe_state, SubsystemState::Ready);
        assert!(!pressure.rich.accepting_deliveries);

        for _ in 0..5 {
            metrics.durable_backlog_released(&identity, 3, 120);
        }
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.buffered_batches.current().get(), 0);
        assert_eq!(pressure.rich.buffered_bytes.current().get(), 0);
        assert!(pressure.rich.tables[0].is_drained());
        assert_eq!(pressure.rich.quiver_state, SubsystemState::Ready);

        metrics.durable_backlog_released(&identity, 3, 128);
        assert_eq!(
            metrics
                .pressure()
                .expect("pressure")
                .rich
                .buffered_batches
                .current()
                .get(),
            0
        );
        metrics.durable_backlog_admitted(&identity, 3, 128);
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.buffered_batches.current().get(), 0);
        assert_eq!(pressure.rich.buffered_rows.current().get(), 0);
        assert!(
            metrics
                .prometheus()
                .contains("arrowcol_dataflow_durable_released_batches_total 6\n")
        );
    }

    /// Scenario: Several pipeline cores record the receiver's instance key and delivery mode.
    /// Guarantees: Identical settings are shared process-wide, while a conflicting key or mode is rejected.
    #[test]
    fn admission_identity_is_process_wide() {
        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        assert_eq!(metrics.instance_key(), None);
        assert!(!metrics.native_quiver_enabled());
        metrics.configure_admission("dfe-0", true).expect("first core");
        metrics.configure_admission("dfe-0", true).expect("second core");
        assert_eq!(metrics.instance_key(), Some("dfe-0"));
        assert!(metrics.native_quiver_enabled());
        assert!(metrics.configure_admission("dfe-1", true).is_err());
        assert!(metrics.configure_admission("dfe-0", false).is_err());
    }

    /// Scenario: Multiple pipeline cores request the collector's stream allowance.
    /// Guarantees: Admission shares one process-wide limit and rejects conflicting configurations.
    #[test]
    fn stream_capacity_is_shared_across_cores() {
        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        let first = metrics.stream_permits(2).expect("first");
        let second = metrics.stream_permits(2).expect("second");
        assert!(Arc::ptr_eq(&first, &second));
        let _permits = first.try_acquire_many(2).expect("capacity");
        assert!(second.try_acquire().is_err());
        assert!(metrics.stream_permits(3).is_err());
    }
    #[test]
    fn publication_backlog_replay_preserves_first_age_until_durable_success() {
        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        metrics.configure_publication_backlog(2).expect("bound");
        metrics.configure_publication_backlog(2).expect("same bound");
        let first = PublicationId::new();
        let second = PublicationId::new();
        let now = Instant::now();
        let accepted = now - Duration::from_secs(5);
        metrics.publication_started(first, 3, 128, accepted).expect("first");
        metrics.publication_started(first, 3, 128, now).expect("replay");
        metrics.publication_started(second, 2, 64, now).expect("second");
        let snapshot = metrics.publication_backlog().snapshot(now);
        assert!(snapshot.available);
        assert_eq!(
            (snapshot.publications, snapshot.rows, snapshot.logical_bytes),
            (Some(2), Some(5), Some(192))
        );
        assert_eq!(snapshot.oldest_age, Some(Duration::from_secs(5)));
        assert_eq!(
            metrics
                .publication_backlog()
                .snapshot(now + Duration::from_secs(4))
                .oldest_age,
            Some(Duration::from_secs(9))
        );
        metrics.publication_committed(first).expect("durable first");
        assert_eq!(metrics.publication_backlog_snapshot().rows, Some(2));
        metrics.publication_committed(second).expect("durable second");
        let snapshot = metrics.publication_backlog_snapshot();
        assert_eq!(
            (snapshot.publications, snapshot.rows, snapshot.logical_bytes),
            (Some(0), Some(0), Some(0))
        );
        assert_eq!(snapshot.oldest_age, Some(Duration::ZERO));
    }

    #[test]
    fn publication_backlog_unknown_is_sticky_and_never_scrapes_as_zero() {
        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        assert!(!metrics.publication_backlog_snapshot().available);
        let initial = metrics.prometheus();
        assert!(initial.contains("arrowcol_publication_backlog_rows NaN\n"));
        metrics.configure_publication_backlog(1).expect("bound");
        let id = PublicationId::new();
        metrics
            .publication_started(id, 4, 256, Instant::now())
            .expect("admitted");
        metrics.publication_mark_unknown();
        assert!(metrics.publication_committed(id).is_err());
        assert!(metrics.configure_publication_backlog(1).is_err());
        let snapshot = metrics.publication_backlog_snapshot();
        assert!(!snapshot.available);
        assert_eq!(
            (
                snapshot.publications,
                snapshot.rows,
                snapshot.logical_bytes,
                snapshot.oldest_age
            ),
            (None, None, None, None)
        );
        let exposition = metrics.prometheus();
        for suffix in ["publications", "rows", "bytes", "oldest_age_seconds"] {
            assert!(exposition.contains(&format!("arrowcol_publication_backlog_{suffix} NaN\n")));
        }
        assert!(exposition.contains("arrowcol_publication_backlog_available 0\n"));
        assert!(exposition.contains("arrowcol_publication_backlog_status{status=\"unknown_outcome\"} 1\n"));
        assert!(!exposition.contains(&id.to_string()));
    }

    #[test]
    fn publication_backlog_rejects_mismatches_overflow_and_unknown_commits() {
        for (first_rows, first_bytes, next_rows, next_bytes, replay, reason) in [
            (1, 8, 2, 8, true, "replay_mismatch"),
            (1, 8, 1, 9, true, "replay_mismatch"),
            (u64::MAX, 8, 1, 8, false, "overflow"),
            (1, u64::MAX, 1, 1, false, "overflow"),
        ] {
            let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
            metrics.configure_publication_backlog(2).expect("bound");
            let first = PublicationId::new();
            metrics
                .publication_started(first, first_rows, first_bytes, Instant::now())
                .expect("first");
            let next = if replay { first } else { PublicationId::new() };
            assert!(
                metrics
                    .publication_started(next, next_rows, next_bytes, Instant::now())
                    .is_err()
            );
            assert_eq!(metrics.publication_backlog_snapshot().unknown_reason, Some(reason));
            assert_eq!(metrics.publication_backlog().entries.len(), 1);
        }
        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        metrics.configure_publication_backlog(1).expect("bound");
        assert!(metrics.publication_committed(PublicationId::new()).is_err());
        assert_eq!(
            metrics.publication_backlog_snapshot().unknown_reason,
            Some("unknown_commit")
        );
    }

    #[test]
    fn publication_backlog_enforces_existing_admission_bound_without_eviction() {
        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        metrics.configure_publication_backlog(1).expect("bound");
        metrics
            .publication_started(PublicationId::new(), 2, 16, Instant::now())
            .expect("first");
        assert!(
            metrics
                .publication_started(PublicationId::new(), 3, 24, Instant::now())
                .is_err()
        );
        let tracker = metrics.publication_backlog();
        assert_eq!(tracker.entries.len(), 1);
        assert_eq!((tracker.rows, tracker.logical_bytes), (2, 16));
        drop(tracker);
        assert_eq!(
            metrics.publication_backlog_snapshot().unknown_reason,
            Some("capacity_exceeded")
        );

        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        metrics.configure_publication_backlog(1).expect("bound");
        assert!(metrics.configure_publication_backlog(2).is_err());
        assert_eq!(
            metrics.publication_backlog_snapshot().unknown_reason,
            Some("conflicting_bound")
        );
    }

    #[test]
    fn publication_backlog_scrape_reports_admitted_cohort_not_dfe_ack_pressure() {
        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        metrics.configure_publication_backlog(1).expect("bound");
        let id = PublicationId::new();
        metrics
            .publication_started(id, 3, 128, Instant::now() - Duration::from_secs(2))
            .expect("admitted");
        let exposition = metrics.prometheus();
        assert!(exposition.contains("arrowcol_publication_backlog_available 1\n"));
        assert!(exposition.contains("arrowcol_publication_backlog_rows 3\n"));
        assert!(exposition.contains("arrowcol_publication_backlog_bytes 128\n"));
        assert!(exposition.contains("arrowcol_dfe_buffered_rows 0\n"));
        let age = exposition
            .lines()
            .find_map(|line| {
                line.strip_prefix("arrowcol_publication_backlog_oldest_age_seconds ")
                    .map(|value| value.parse::<f64>().expect("age"))
            })
            .expect("age sample");
        assert!(age >= 2.0);
        metrics.publication_committed(id).expect("durable success");
        assert!(metrics.prometheus().contains("arrowcol_publication_backlog_rows 0\n"));
    }

    #[test]
    fn submission_wait_does_not_record_pipeline_execution() {
        let metrics = PipelineMetrics::new();
        metrics.submission_wait.record(0.012, 12_000_000);
        assert!(
            metrics
                .stages
                .iter()
                .all(|stage| stage.count.load(Ordering::Relaxed) == 0)
        );
        let mut exposition = String::new();
        metrics.write(&mut exposition);
        let bucket = exposition
            .lines()
            .find(|line| {
                line.starts_with("arrowcol_pipeline_submission_wait_seconds_bucket{") && line.contains("le=\"0.025\"")
            })
            .expect("submission wait bucket");
        assert!(bucket.ends_with(" 1"));
        assert!(!bucket.contains("stage="));
        assert!(exposition.lines().any(|line| {
            line.starts_with("arrowcol_pipeline_submission_wait_seconds_sum{") && line.ends_with(" 0.012")
        }));
    }

    #[test]
    fn prometheus_exports_bounded_pipeline_flush_and_file_histograms() {
        let stage = StageMetrics::default();
        stage.record(PipelineStage::Build, Duration::from_millis(12));
        stage.record_submission_wait(Duration::from_millis(12));
        stage.record_flush(FlushReason::TargetParquetBytes);
        stage.record_file(134_217_728, 250_000);

        let exposition = RuntimeMetrics::new(CollectorInstanceId::new()).prometheus();
        assert!(exposition.contains("arrowcol_pipeline_stage_duration_seconds_bucket{run=\""));
        assert!(exposition.contains("arrowcol_pipeline_submission_wait_seconds_bucket{run=\""));
        assert!(exposition.contains("component=\"arrowcol-dataflow-rust\",stage=\"build\",le=\"0.025\"}"));
        assert!(exposition.contains("arrowcol_pipeline_flushes_total{reason=\"target_parquet_bytes\"}"));
        assert!(exposition.contains("arrowcol_parquet_file_physical_bytes_bucket{run=\""));
        assert!(exposition.contains("le=\"134217728\"}"));
        assert!(exposition.contains("arrowcol_parquet_file_rows_bucket{run=\""));
        assert!(!exposition.contains("publication_id="));
    }
}
