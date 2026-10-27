//! Delta maintenance over Delta Kernel 0.29: OPTIMIZE, PURGE and VACUUM LITE.
//!
//! Every operation is a serializable dry-run plan plus an apply step. Apply
//! revalidates the table id, metadata, protocol and its complete read set
//! against a fresh snapshot immediately before each publication or deletion,
//! and fails explicitly (never silently falls back) on unsupported tables.

use crate::delta::{DeltaTable, INGESTION_PARTITION, PURGE_WATERMARK, collision_backoff, object_store_for_url};
use crate::delta_io::shared_executor;
use crate::layout::{FileSortBuffer, LayoutPolicy};
use crate::sync_directory;
use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, RecordBatch, RecordBatchOptions, make_array, new_null_array,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Int64Type, Schema, SchemaRef};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::expressions::Scalar;
use delta_kernel::transaction::{BoundWriteContext, CommitResult};
use delta_kernel::{Engine, FileMeta, FilteredEngineData, Snapshot, SnapshotRef};
use delta_kernel_default_engine::executor::TaskExecutor;
use delta_kernel_default_engine::parquet::DataFileMetadata;
use delta_kernel_default_engine::stats::FileStatsAccumulator;
use delta_kernel_default_engine::{DefaultEngineBuilder, build_add_file_metadata};
use object_store::{ObjectStore, ObjectStoreExt, PutMode};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use url::Url;

/// Serialized plan format understood by this build.
pub const PLAN_FORMAT: u32 = 2;
/// Tolerated clock skew for explicit retention anchors.
pub const MAX_ANCHOR_SKEW_MS: i64 = 5 * 60_000;
/// Reserved application-transaction namespace for idempotent maintenance plans.
/// Collector writers reject every `arrowcol.purge*` identity.
pub const PLAN_APP_PREFIX: &str = "arrowcol.purge.plan.";
/// Marker that must exist in a disposable test root before short-retention VACUUM.
pub const DISPOSABLE_TEST_MARKER: &str = ".arrowcol-disposable-test-root";
/// Minimum VACUUM LITE tombstone retention without the unsafe test override.
pub const MIN_VACUUM_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;
/// Exclusive upper bound for OPTIMIZE target and output file sizes.
pub const MAX_TARGET_FILE_BYTES: u64 = 1024 * 1024 * 1024;
/// Bounded Kernel publication attempts per apply.
pub const MAX_COMMIT_ATTEMPTS: u32 = 8;
/// Bounded wall-clock budget for collision retries.
pub const COMMIT_BUDGET: Duration = Duration::from_secs(10);
const ENGINE_INFO: &str = "arrow-deltalake-maintenance";
const COMPANION_SUFFIX: &str = "__unix_nanos";
const SUPPORTED_FEATURES: &[&str] = &[
    "appendOnly",
    "invariants",
    "timestampNtz",
    "domainMetadata",
    "v2Checkpoint",
    "vacuumProtocolCheck",
    "inCommitTimestamp",
];

/// Failure classes callers and the service report as bounded outcomes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceErrorKind {
    /// The planned read set or expired predicate changed; replan.
    StalePlan,
    /// Table identity, schema, partitioning, configuration or protocol changed. Not retried.
    MetadataChanged,
    /// A table feature or layout this implementation refuses to handle.
    Unsupported,
    /// The plan is malformed, foreign or outside configured limits.
    InvalidPlan,
    /// The request would be unsafe (short retention, unsafe path, re-added tombstone).
    UnsafeRequest,
    /// A configured memory, file, action or payload bound was exceeded.
    ResourceLimit,
    /// Commit collisions or retries exhausted the bounded budget.
    Exhausted,
    /// Cancellation or deadline interrupted work.
    Cancelled,
    /// A publication outcome is unknown; staged outputs were retained.
    Uncertain,
}

impl MaintenanceErrorKind {
    /// Stable bounded label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::StalePlan => "stale_plan",
            Self::MetadataChanged => "metadata_changed",
            Self::Unsupported => "unsupported",
            Self::InvalidPlan => "invalid_plan",
            Self::UnsafeRequest => "unsafe_request",
            Self::ResourceLimit => "resource_limit",
            Self::Exhausted => "exhausted",
            Self::Cancelled => "cancelled",
            Self::Uncertain => "uncertain",
        }
    }
}

/// Classified maintenance failure, optionally carrying partial progress.
#[derive(Debug)]
pub struct MaintenanceError {
    pub kind: MaintenanceErrorKind,
    pub message: String,
    pub report: Option<Box<MaintenanceReport>>,
}

impl Display for MaintenanceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.label(), self.message)
    }
}

impl std::error::Error for MaintenanceError {}

fn fail(kind: MaintenanceErrorKind, message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(MaintenanceError {
        kind,
        message: message.into(),
        report: None,
    })
}

fn fail_with(kind: MaintenanceErrorKind, message: impl Into<String>, report: &MaintenanceReport) -> anyhow::Error {
    anyhow::Error::new(MaintenanceError {
        kind,
        message: message.into(),
        report: Some(Box::new(report.clone())),
    })
}

/// Find the classified failure anywhere in an error chain.
#[must_use]
pub fn error_details(error: &anyhow::Error) -> Option<&MaintenanceError> {
    error.chain().find_map(|cause| cause.downcast_ref::<MaintenanceError>())
}

/// Classified failure kind, if any.
#[must_use]
pub fn error_kind(error: &anyhow::Error) -> Option<MaintenanceErrorKind> {
    error_details(error).map(|details| details.kind)
}

/// Cooperative cancellation and deadline shared with the caller.
#[derive(Clone, Debug, Default)]
pub struct MaintenanceControl {
    cancel: Arc<AtomicBool>,
    deadline: Option<Instant>,
}

impl MaintenanceControl {
    #[must_use]
    pub const fn new(cancel: Arc<AtomicBool>, deadline: Option<Instant>) -> Self {
        Self { cancel, deadline }
    }

    /// True once cancellation was requested or the deadline passed.
    #[must_use]
    pub fn interrupted(&self) -> bool {
        self.cancel.load(Ordering::SeqCst) || self.deadline.is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Fail with [`MaintenanceErrorKind::Cancelled`] when interrupted.
    ///
    /// # Errors
    /// Returns a cancellation error after cancellation or deadline expiry.
    pub fn check(&self) -> Result<()> {
        if self.interrupted() {
            return Err(fail(
                MaintenanceErrorKind::Cancelled,
                "maintenance cancelled or deadline exceeded",
            ));
        }
        Ok(())
    }
}

/// Current wall-clock UTC milliseconds.
///
/// # Errors
/// Returns an error if the clock is before the Unix epoch.
pub fn now_ms() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

/// Reject negative anchors and anchors beyond the wall clock plus skew.
fn validate_anchor(name: &str, anchor_ms: i64) -> Result<()> {
    if anchor_ms < 0 || anchor_ms > now_ms()?.saturating_add(MAX_ANCHOR_SKEW_MS) {
        return Err(fail(
            MaintenanceErrorKind::InvalidPlan,
            format!("{name} anchor {anchor_ms} is negative or in the future"),
        ));
    }
    Ok(())
}

/// UTC ingestion partition granularity (`ARROWCOL_PARTITION_GRANULARITY`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Granularity {
    Minute,
    Hour,
    Day,
}

impl Granularity {
    /// Parse `minute`, `hour` or `day`.
    ///
    /// # Errors
    /// Rejects any other name.
    pub fn parse_name(name: &str) -> Result<Self> {
        match name {
            "minute" => Ok(Self::Minute),
            "hour" => Ok(Self::Hour),
            "day" => Ok(Self::Day),
            other => Err(fail(
                MaintenanceErrorKind::InvalidPlan,
                format!("unknown partition granularity {other:?}"),
            )),
        }
    }

    /// Partition width in milliseconds.
    #[must_use]
    pub const fn width_ms(self) -> i64 {
        match self {
            Self::Minute => 60_000,
            Self::Hour => 3_600_000,
            Self::Day => 86_400_000,
        }
    }

    /// Start of the partition window containing `ms`.
    #[must_use]
    pub const fn floor_ms(self, ms: i64) -> i64 {
        ms.div_euclid(self.width_ms()) * self.width_ms()
    }

    const fn format(self) -> &'static str {
        match self {
            Self::Minute => "%Y-%m-%d-%H-%M",
            Self::Hour => "%Y-%m-%d-%H",
            Self::Day => "%Y-%m-%d",
        }
    }

    /// Format a stamp exactly as collectors name ingestion partitions.
    ///
    /// # Errors
    /// Rejects out-of-range timestamps.
    pub fn format_ms(self, ms: i64) -> Result<String> {
        let stamp = chrono::DateTime::from_timestamp_millis(ms).context("timestamp out of range")?;
        Ok(stamp.format(self.format()).to_string())
    }

    /// Strictly parse a full partition value into its window start (UTC ms).
    ///
    /// # Errors
    /// Rejects malformed, partial, non-canonical or pre-epoch values.
    pub fn parse_partition(self, value: &str) -> Result<i64> {
        let malformed = || {
            fail(
                MaintenanceErrorKind::InvalidPlan,
                format!("malformed ingestion partition {value:?}"),
            )
        };
        let widths: &[usize] = match self {
            Self::Minute => &[4, 2, 2, 2, 2],
            Self::Hour => &[4, 2, 2, 2],
            Self::Day => &[4, 2, 2],
        };
        let parts: Vec<&str> = value.split('-').collect();
        if parts.len() != widths.len() {
            return Err(malformed());
        }
        let mut numbers = [0_u32; 5];
        for (index, (part, width)) in parts.iter().zip(widths).enumerate() {
            if part.len() != *width || !part.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(malformed());
            }
            numbers[index] = part.parse().map_err(|_| malformed())?;
        }
        let year = i32::try_from(numbers[0]).map_err(|_| malformed())?;
        let date = chrono::NaiveDate::from_ymd_opt(year, numbers[1], numbers[2]).ok_or_else(malformed)?;
        let start = date
            .and_hms_opt(numbers[3], numbers[4], 0)
            .ok_or_else(malformed)?
            .and_utc()
            .timestamp_millis();
        if start < 0 || self.format_ms(start)? != value {
            return Err(malformed());
        }
        Ok(start)
    }
}

/// Table identity, metadata and protocol a plan was derived from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFingerprint {
    pub table_id: String,
    pub schema_string: String,
    pub partition_columns: Vec<String>,
    pub configuration: BTreeMap<String, String>,
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    pub reader_features: Option<Vec<String>>,
    pub writer_features: Option<Vec<String>>,
}

impl TableFingerprint {
    fn of(snapshot: &SnapshotRef) -> Self {
        let config = snapshot.table_configuration();
        let metadata = config.metadata();
        let protocol = config.protocol();
        let names = |features: Option<&[delta_kernel::table_features::TableFeature]>| {
            features.map(|features| {
                let mut names: Vec<String> = features.iter().map(ToString::to_string).collect();
                names.sort();
                names
            })
        };
        Self {
            table_id: metadata.id().to_string(),
            schema_string: metadata.schema_string().to_string(),
            partition_columns: metadata.partition_columns().to_vec(),
            configuration: metadata
                .configuration()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            min_reader_version: protocol.min_reader_version(),
            min_writer_version: protocol.min_writer_version(),
            reader_features: names(protocol.reader_features()),
            writer_features: names(protocol.writer_features()),
        }
    }

    /// Reject protocols, features and settings this implementation cannot preserve.
    fn validate_supported(&self, purge: bool) -> Result<()> {
        let unsupported = |message: String| Err(fail(MaintenanceErrorKind::Unsupported, message));
        if self.min_reader_version > 3 || self.min_writer_version > 7 {
            return unsupported(format!(
                "protocol ({}, {}) is newer than supported",
                self.min_reader_version, self.min_writer_version
            ));
        }
        if self.reader_features.is_none() && self.min_reader_version > 1 {
            return unsupported("legacy reader protocol above 1 (column mapping) is unsupported".into());
        }
        if self.writer_features.is_none() && self.min_writer_version > 2 {
            return unsupported(
                "legacy writer protocol above 2 (constraints/CDF/generated columns) is unsupported".into(),
            );
        }
        for feature in self.reader_features.iter().chain(&self.writer_features).flatten() {
            if !SUPPORTED_FEATURES.contains(&feature.as_str()) {
                return unsupported(format!("table feature {feature} is unsupported by maintenance"));
            }
        }
        for (key, value) in &self.configuration {
            let enabled = value.eq_ignore_ascii_case("true");
            let rejected = match key.as_str() {
                "delta.columnMapping.mode" => !value.eq_ignore_ascii_case("none"),
                "delta.enableChangeDataFeed" | "delta.enableDeletionVectors" | "delta.enableRowTracking" => enabled,
                "delta.setTransactionRetentionDuration" => true,
                "delta.appendOnly" => purge && enabled,
                _ => key.starts_with("delta.constraints."),
            };
            if rejected {
                return unsupported(format!("table setting {key}={value} is unsupported by maintenance"));
            }
        }
        Ok(())
    }

    fn require_same(&self, current: &Self) -> Result<()> {
        if self.table_id != current.table_id {
            return Err(fail(
                MaintenanceErrorKind::MetadataChanged,
                "table id changed (table replaced)",
            ));
        }
        if self != current {
            return Err(fail(
                MaintenanceErrorKind::MetadataChanged,
                "table schema, partitioning, configuration or protocol changed since planning",
            ));
        }
        Ok(())
    }
}

/// One active data file from Kernel scan metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataFile {
    pub path: String,
    pub size: u64,
    pub rows: Option<u64>,
    pub partition_values: BTreeMap<String, Option<String>>,
}

/// Exact active read set of one snapshot, with the scan rows needed for removes.
struct ActiveState {
    snapshot: SnapshotRef,
    fingerprint: TableFingerprint,
    files: Vec<DataFile>,
    locations: HashMap<String, (usize, usize, usize)>,
    batches: Vec<RecordBatch>,
}

impl ActiveState {
    fn file(&self, path: &str) -> Option<&DataFile> {
        self.locations
            .get(path)
            .and_then(|(_, _, index)| self.files.get(*index))
    }

    /// Scan rows selecting exactly `paths`, preserving Kernel's full scan-row schema.
    fn removal_data(&self, paths: &BTreeSet<String>) -> Result<Vec<FilteredEngineData>> {
        let mut masks: Vec<Option<Vec<bool>>> = vec![None; self.batches.len()];
        for path in paths {
            let (batch, row, _) = self.locations.get(path).ok_or_else(|| {
                fail(
                    MaintenanceErrorKind::StalePlan,
                    format!("planned input {path} is no longer active"),
                )
            })?;
            let mask = masks[*batch].get_or_insert_with(|| vec![false; self.batches[*batch].num_rows()]);
            mask[*row] = true;
        }
        let mut data = Vec::new();
        for (batch, mask) in self.batches.iter().zip(masks) {
            if let Some(mask) = mask {
                let selected = filter_record_batch(batch, &BooleanArray::from(mask))?;
                data.push(FilteredEngineData::with_all_rows_selected(Box::new(
                    ArrowEngineData::new(selected),
                )));
            }
        }
        Ok(data)
    }
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    batch.column_by_name(name).ok_or_else(|| {
        fail(
            MaintenanceErrorKind::Unsupported,
            format!("Kernel scan row lacks {name}"),
        )
    })
}

fn parse_scan_rows(batch: &RecordBatch, partition_columns: &[String]) -> Result<Vec<DataFile>> {
    let shape = |name: &str| {
        fail(
            MaintenanceErrorKind::Unsupported,
            format!("unexpected Kernel scan row type for {name}"),
        )
    };
    let paths = column(batch, "path")?
        .as_string_opt::<i32>()
        .ok_or_else(|| shape("path"))?;
    let sizes = column(batch, "size")?
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(|| shape("size"))?;
    let stats = match batch.column_by_name("stats") {
        Some(stats) => Some(stats.as_string_opt::<i32>().ok_or_else(|| shape("stats"))?),
        None => None,
    };
    let vectors = match batch.column_by_name("deletionVector") {
        Some(vectors) => Some(vectors.as_struct_opt().ok_or_else(|| shape("deletionVector"))?),
        None => None,
    };
    let constants = column(batch, "fileConstantValues")?
        .as_struct_opt()
        .ok_or_else(|| shape("fileConstantValues"))?;
    let partitions = constants
        .column_by_name("partitionValues")
        .ok_or_else(|| shape("partitionValues"))?
        .as_map_opt()
        .ok_or_else(|| shape("partitionValues"))?;
    let mut files = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if paths.is_null(row) || sizes.is_null(row) {
            return Err(shape("path/size"));
        }
        if let Some(vectors) = vectors
            && vectors.is_valid(row)
            && vectors
                .column_by_name("storageType")
                .is_none_or(|storage| storage.is_valid(row))
        {
            return Err(fail(
                MaintenanceErrorKind::Unsupported,
                "deletion vectors are unsupported by maintenance",
            ));
        }
        let rows = match stats {
            Some(stats) if stats.is_valid(row) => serde_json::from_str::<serde_json::Value>(stats.value(row))
                .ok()
                .and_then(|value| value.get("numRecords").and_then(serde_json::Value::as_u64)),
            _ => None,
        };
        let mut values = BTreeMap::new();
        if partitions.is_valid(row) {
            let entries = partitions.value(row);
            let keys = entries
                .column(0)
                .as_string_opt::<i32>()
                .ok_or_else(|| shape("partition keys"))?;
            let items = entries
                .column(1)
                .as_string_opt::<i32>()
                .ok_or_else(|| shape("partition values"))?;
            for entry in 0..entries.len() {
                let value = items.is_valid(entry).then(|| items.value(entry).to_owned());
                if values.insert(keys.value(entry).to_owned(), value).is_some() {
                    return Err(shape("duplicate partition key"));
                }
            }
        }
        if values.len() != partition_columns.len() || partition_columns.iter().any(|name| !values.contains_key(name)) {
            return Err(fail(
                MaintenanceErrorKind::Unsupported,
                "file partition values do not match table partitioning",
            ));
        }
        files.push(DataFile {
            path: paths.value(row).to_owned(),
            size: u64::try_from(sizes.value(row)).map_err(|_| shape("negative size"))?,
            rows,
            partition_values: values,
        });
    }
    Ok(files)
}

fn percent_decode(raw: &str) -> Result<String> {
    let bytes = raw.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = raw.get(index + 1..index + 3).context("truncated percent escape")?;
            decoded.push(u8::from_str_radix(hex, 16).context("invalid percent escape")?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Ok(String::from_utf8(decoded)?)
}

impl DeltaTable {
    /// Open an EXISTING local or ADLS table for maintenance without creating it,
    /// without the collector writer lock and without mutating its schema.
    /// Cloud handles enforce the container reset fence before each mutation.
    ///
    /// # Errors
    /// Rejects missing tables, unsupported URLs and authority failures.
    pub fn open_for_maintenance(url: &Url) -> Result<Arc<Self>> {
        let mut url = url.clone();
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        if url.scheme() == "file" {
            let root = url
                .to_file_path()
                .map_err(|()| anyhow::anyhow!("invalid local table URL"))?;
            if !root.join("_delta_log").is_dir() {
                return Err(fail(
                    MaintenanceErrorKind::Unsupported,
                    format!("{} is not an existing Delta table", root.display()),
                ));
            }
        }
        let engine = DefaultEngineBuilder::new(object_store_for_url(&url)?)
            .with_task_executor(shared_executor()?)
            .build();
        let snapshot = Snapshot::builder_for(url.as_str()).build(&engine).map_err(|error| {
            fail(
                MaintenanceErrorKind::Unsupported,
                format!("existing Delta table required: {error}"),
            )
        })?;
        let partition_columns = snapshot.table_configuration().metadata().partition_columns().to_vec();
        let canonical: Schema = snapshot.schema().as_ref().try_into_arrow()?;
        // Normalization is not idempotent for timestamp companions; pass only the
        // required columns so validation never evolves or recreates the table.
        let required: Vec<_> = canonical
            .fields()
            .iter()
            .filter(|field| !field.is_nullable() && !field.name().ends_with(COMPANION_SUFFIX))
            .map(Arc::clone)
            .collect();
        let table = Self::open_url(url, Arc::new(Schema::new(required)), partition_columns)?;
        if table.snapshot()?.table_configuration().metadata().id() != snapshot.table_configuration().metadata().id() {
            return Err(fail(
                MaintenanceErrorKind::MetadataChanged,
                "table replaced while opening",
            ));
        }
        Ok(table)
    }

    fn active_state(&self, max_files: usize) -> Result<ActiveState> {
        let snapshot = self.snapshot()?;
        let fingerprint = TableFingerprint::of(&snapshot);
        let scan = Arc::clone(&snapshot).scan_builder().build()?;
        let mut state = ActiveState {
            snapshot,
            fingerprint,
            files: Vec::new(),
            locations: HashMap::new(),
            batches: Vec::new(),
        };
        for metadata in scan.scan_metadata(&self.engine)? {
            let batch = metadata?.scan_files.apply_selection_vector()?.try_into_record_batch()?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch_index = state.batches.len();
            for (row, file) in parse_scan_rows(&batch, &state.fingerprint.partition_columns)?
                .into_iter()
                .enumerate()
            {
                if state.files.len() >= max_files {
                    return Err(fail(
                        MaintenanceErrorKind::ResourceLimit,
                        format!("active file count exceeds {max_files}"),
                    ));
                }
                self.safe_object_path(&file.path)?;
                if state
                    .locations
                    .insert(file.path.clone(), (batch_index, row, state.files.len()))
                    .is_some()
                {
                    return Err(fail(
                        MaintenanceErrorKind::Unsupported,
                        format!("duplicate active path {}", file.path),
                    ));
                }
                state.files.push(file);
            }
            state.batches.push(batch);
        }
        Ok(state)
    }

    /// Validate a log-relative data path and map it to its object-store location.
    fn safe_object_path(&self, raw: &str) -> Result<object_store::path::Path> {
        let unsafe_path = |why: &str| {
            fail(
                MaintenanceErrorKind::UnsafeRequest,
                format!("unsafe data path {raw:?}: {why}"),
            )
        };
        if raw.is_empty() || raw.len() > 4096 || raw.starts_with('/') || raw.contains(['\\', '\0']) {
            return Err(unsafe_path("empty, absolute or non-portable"));
        }
        if Url::parse(raw).is_ok() {
            return Err(unsafe_path("external absolute URL"));
        }
        let decoded = percent_decode(raw).map_err(|_| unsafe_path("invalid escape"))?;
        if decoded.contains(['\\', '\0'])
            || decoded
                .split('/')
                .any(|segment| segment.is_empty() || segment == "." || segment == "..")
            || decoded.split('/').next() == Some("_delta_log")
        {
            return Err(unsafe_path("traversal or log path"));
        }
        self.object_path(raw).map_err(|error| unsafe_path(&error.to_string()))
    }

    fn data_url(&self, relative: &str) -> Result<Url> {
        let mut url = self.url.clone();
        url.path_segments_mut()
            .map_err(|()| anyhow::anyhow!("invalid table base URL"))?
            .pop_if_empty()
            .extend(relative.split('/'));
        Ok(url)
    }
}

/// Common identity of every serialized plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanHeader {
    pub format: u32,
    pub plan_id: String,
    pub table_url: String,
    pub read_version: u64,
    pub created_at_ms: i64,
    pub fingerprint: TableFingerprint,
}

impl PlanHeader {
    fn new(table: &DeltaTable, state: &ActiveState) -> Result<Self> {
        Ok(Self {
            format: PLAN_FORMAT,
            plan_id: uuid::Uuid::new_v4().to_string(),
            table_url: table.url.to_string(),
            read_version: state.snapshot.version(),
            created_at_ms: now_ms()?,
            fingerprint: state.fingerprint.clone(),
        })
    }

    /// Reserved idempotence transaction recorded with the plan's commit.
    #[must_use]
    pub fn app_id(&self, operation: &str) -> String {
        format!("{PLAN_APP_PREFIX}{operation}.{}", self.plan_id)
    }

    fn validate(&self, table: &DeltaTable) -> Result<()> {
        if self.format != PLAN_FORMAT {
            return Err(fail(
                MaintenanceErrorKind::InvalidPlan,
                format!("unsupported plan format {}", self.format),
            ));
        }
        uuid::Uuid::parse_str(&self.plan_id)
            .map_err(|_| fail(MaintenanceErrorKind::InvalidPlan, "plan id is not a UUID"))?;
        if self.table_url != table.url.as_str() {
            return Err(fail(
                MaintenanceErrorKind::InvalidPlan,
                "plan belongs to a different table URL",
            ));
        }
        Ok(())
    }
}

/// Final outcome of plan or apply.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Dry-run plan produced; nothing changed.
    Planned,
    /// No eligible work; no commit and no deletion.
    #[default]
    Noop,
    /// One Delta commit published.
    Committed,
    /// The plan's idempotence transaction is already present; nothing changed.
    AlreadyApplied,
    /// VACUUM LITE physically deleted (or found absent) every candidate.
    Deleted,
    /// Interrupted with partial physical progress (VACUUM only).
    Partial,
}

impl Outcome {
    /// Stable bounded label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Noop => "noop",
            Self::Committed => "committed",
            Self::AlreadyApplied => "already_applied",
            Self::Deleted => "deleted",
            Self::Partial => "partial",
        }
    }
}

/// Counters for plan/apply, including partial progress.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceReport {
    pub operation: String,
    pub plan_id: Option<String>,
    pub outcome: Outcome,
    pub read_version: u64,
    pub version: u64,
    pub files_removed: u64,
    pub rows_removed: u64,
    pub bytes_removed: u64,
    pub files_added: u64,
    pub rows_added: u64,
    pub bytes_added: u64,
    pub attempts: u32,
    pub collisions: u32,
    pub retries: u32,
    pub cutoff_ms: Option<i64>,
    pub watermark_ms: Option<i64>,
    pub files_deleted: u64,
    pub files_already_absent: u64,
    pub backlog_files: u64,
    pub skipped_partitions: u64,
    /// OPTIMIZE bins whose actual rewrite would not reduce the file count.
    pub nonbeneficial_bins: u64,
    pub interrupted: bool,
    pub outputs: Vec<String>,
}

impl MaintenanceReport {
    fn new(operation: &str, header: &PlanHeader) -> Self {
        Self {
            operation: operation.to_owned(),
            plan_id: Some(header.plan_id.clone()),
            read_version: header.read_version,
            version: header.read_version,
            ..Self::default()
        }
    }
}

/// A durable, immutable output not yet referenced by the log.
struct StagedOutput {
    relative: String,
    local: PathBuf,
    url: Url,
    modified_ms: i64,
    bytes: u64,
    rows: u64,
    stats: arrow::array::StructArray,
    context: BoundWriteContext,
    uploaded: bool,
}

/// Exact removals (and optional purge watermark) revalidated for one attempt.
struct Prepared {
    remove: BTreeSet<String>,
    watermark: Option<i64>,
}

enum Published {
    Committed(u64),
    AlreadyApplied(u64),
}

impl DeltaTable {
    /// Bounded, revalidating publication loop shared by OPTIMIZE and PURGE.
    #[allow(clippy::too_many_arguments)]
    fn publish(
        &self,
        header: &PlanHeader,
        operation: &str,
        data_change: bool,
        max_active: usize,
        control: &MaintenanceControl,
        outputs: &[StagedOutput],
        report: &mut MaintenanceReport,
        mut prepare: impl FnMut(&ActiveState) -> Result<Prepared>,
    ) -> Result<Published> {
        let app_id = header.app_id(&operation.to_ascii_lowercase());
        let started = Instant::now();
        let mut uncertain = false;
        for attempt in 0..MAX_COMMIT_ATTEMPTS {
            report.attempts = attempt + 1;
            let mut in_flight = false;
            let result = (|| -> Result<Option<Published>> {
                control.check()?;
                self.check_publication()?;
                let state = self.active_state(max_active)?;
                header.fingerprint.require_same(&state.fingerprint)?;
                if state.snapshot.get_app_id_version(&app_id, &self.engine)?.is_some() {
                    return Ok(Some(Published::AlreadyApplied(state.snapshot.version())));
                }
                state.fingerprint.validate_supported(operation == "DELETE")?;
                let prepared = prepare(&state)?;
                if prepared.remove.is_empty() {
                    return Err(fail(
                        MaintenanceErrorKind::InvalidPlan,
                        "maintenance commit without removals",
                    ));
                }
                let removal = state.removal_data(&prepared.remove)?;
                let mut transaction = Arc::clone(&state.snapshot)
                    .transaction(self.committer(), &self.engine)?
                    .with_operation(operation.to_owned())
                    .with_engine_info(ENGINE_INFO)
                    .with_data_change(data_change)
                    .with_transaction_id(app_id.clone(), 1);
                if let Some(watermark) = prepared.watermark {
                    let existing = state.snapshot.get_app_id_version(PURGE_WATERMARK, &self.engine)?;
                    transaction = transaction.with_transaction_id(
                        PURGE_WATERMARK.to_owned(),
                        existing.map_or(watermark, |e| e.max(watermark)),
                    );
                }
                for output in outputs {
                    let metadata = DataFileMetadata::new(
                        FileMeta::new(output.url.clone(), output.modified_ms, output.bytes),
                        output.stats.clone(),
                    );
                    transaction.add_files(build_add_file_metadata(metadata, &output.context)?);
                }
                for data in removal {
                    transaction.remove_files(data);
                }
                // Final authority and cancellation checks after slow replay/staging.
                control.check()?;
                self.check_publication()?;
                in_flight = true;
                match transaction.commit(&self.engine)? {
                    CommitResult::Committed(commit) => {
                        if self.url.scheme() == "file" {
                            sync_directory(&self.root)?;
                        }
                        Ok(Some(Published::Committed(commit.commit_version())))
                    }
                    CommitResult::Conflicted(_) => {
                        in_flight = false;
                        report.collisions += 1;
                        Ok(None)
                    }
                    CommitResult::Retryable(retry) => {
                        report.retries += 1;
                        tracing_note(&format!("retryable maintenance commit: {}", retry.error));
                        Ok(None)
                    }
                }
            })();
            uncertain |= in_flight;
            match result {
                Ok(Some(published)) => return Ok(published),
                Ok(None) => {}
                Err(error) => {
                    if !uncertain {
                        return Err(self.discard_after(outputs, error, report));
                    }
                    // A raw commit error can follow actual publication: reconcile
                    // by plan id and never clean up outputs blindly.
                    return self.unknown_outcome(&app_id, outputs, report, &format!("{error:#}"));
                }
            }
            if started.elapsed() > COMMIT_BUDGET {
                break;
            }
            collision_backoff(attempt);
        }
        if uncertain {
            return self.unknown_outcome(
                &app_id,
                outputs,
                report,
                "commit budget exhausted after an uncertain attempt",
            );
        }
        let error = fail_with(
            MaintenanceErrorKind::Exhausted,
            format!(
                "maintenance commit budget exhausted ({MAX_COMMIT_ATTEMPTS} attempts / {}s)",
                COMMIT_BUDGET.as_secs()
            ),
            report,
        );
        Err(self.discard_after(outputs, error, report))
    }

    /// Resolve an attempt whose publication may have happened. A visible plan id
    /// means it was published; otherwise (or if the refresh fails) every output
    /// is retained and the outcome is reported as unknown.
    fn unknown_outcome(
        &self,
        app_id: &str,
        outputs: &[StagedOutput],
        report: &mut MaintenanceReport,
        cause: &str,
    ) -> Result<Published> {
        let reconciled = self.snapshot().and_then(|snapshot| {
            Ok(snapshot
                .get_app_id_version(app_id, &self.engine)?
                .map(|_| snapshot.version()))
        });
        if let Ok(Some(version)) = reconciled {
            return Ok(Published::AlreadyApplied(version));
        }
        report.outputs = outputs.iter().map(|output| output.relative.clone()).collect();
        let detail = match reconciled {
            Ok(_) => "plan id not visible after refresh".to_owned(),
            Err(error) => format!("reconciliation failed: {error:#}"),
        };
        Err(fail_with(
            MaintenanceErrorKind::Uncertain,
            format!("publication outcome unknown ({detail}); outputs retained: {cause}"),
            report,
        ))
    }

    /// Remove one output that was definitively never published. An authority
    /// check precedes every table mutation; `NotFound` is success and every
    /// other failure is returned.
    fn discard_output(&self, local: &std::path::Path, url: &Url, uploaded: bool) -> Result<()> {
        if self.url.scheme() == "file" {
            self.check_publication()?;
        }
        match fs::remove_file(local) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(anyhow::Error::new(error).context(format!("remove {}", local.display()))),
        }
        if uploaded {
            let path = object_store::path::Path::from_url_path(url.path())?;
            self.check_publication()?;
            let store = Arc::clone(&self.store);
            match shared_executor()?.block_on(async move { store.delete(&path).await }) {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(anyhow::Error::new(error).context(format!("delete {url}"))),
            }
        }
        Ok(())
    }

    /// Delete outputs that were definitively never published, collecting every failure.
    fn discard_outputs(&self, outputs: &[StagedOutput]) -> std::result::Result<(), String> {
        let failures: Vec<String> = outputs
            .iter()
            .filter_map(|output| {
                self.discard_output(&output.local, &output.url, output.uploaded)
                    .err()
                    .map(|error| format!("{}: {error:#}", output.relative))
            })
            .collect();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    /// Clean up unpublished outputs after `error`. A cleanup failure keeps the
    /// original error's kind and text, adds the cleanup failures and reports
    /// the retained outputs.
    fn discard_after(
        &self,
        outputs: &[StagedOutput],
        error: anyhow::Error,
        report: &mut MaintenanceReport,
    ) -> anyhow::Error {
        match self.discard_outputs(outputs) {
            Ok(()) => error,
            Err(cleanup) => {
                report.outputs = outputs.iter().map(|output| output.relative.clone()).collect();
                fail_with(
                    error_kind(&error).unwrap_or(MaintenanceErrorKind::Uncertain),
                    format!("{error:#}; cleanup of unpublished outputs failed (retained): {cleanup}"),
                    report,
                )
            }
        }
    }

    fn write_context(
        &self,
        snapshot: &SnapshotRef,
        values: &BTreeMap<String, Option<String>>,
    ) -> Result<BoundWriteContext> {
        let transaction = Arc::clone(snapshot).transaction(self.committer(), &self.engine)?;
        let state = transaction.write_state()?;
        let mut builder = state.write_context_builder();
        if !values.is_empty() {
            let typed = values
                .iter()
                .map(|(name, value)| {
                    let scalar = value
                        .clone()
                        .map_or(Scalar::Null(delta_kernel::schema::DataType::STRING), Scalar::String);
                    (name.clone(), scalar)
                })
                .collect::<HashMap<_, _>>();
            builder = builder.with_partition_values(typed);
        }
        Ok(builder.build()?)
    }
}

fn tracing_note(message: &str) {
    if std::env::var_os("ARROWCOL_MAINTENANCE_DEBUG").is_some() {
        eprintln!("{message}");
    }
}

/// OPTIMIZE bounds. Every limit is enforced on plan and again on apply.
#[derive(Clone, Debug)]
pub struct OptimizeConfig {
    /// Target output size; must stay below 1 GiB.
    pub target_file_bytes: u64,
    /// Only files strictly smaller than this are eligible and read (bounded input buffer).
    pub small_file_bytes: u64,
    pub max_files_per_bin: usize,
    pub max_bins_per_commit: usize,
    /// Removes plus estimated adds per commit.
    pub max_actions_per_commit: usize,
    pub max_plan_bytes: usize,
    /// Only the newest N eligible (unprotected) partitions; `None` means all.
    pub latest_partitions: Option<usize>,
    /// Required for `__arrowcol_ingestion` tables.
    pub granularity: Option<Granularity>,
    /// Wall-clock anchor for open-partition protection (defaults to now).
    pub now_ms: Option<i64>,
    /// Actual decoded Arrow bytes read per bin (not compressed sizes).
    pub max_decoded_bytes_per_bin: u64,
    /// Uncompressed Parquet row-group bound for inputs.
    pub max_row_group_bytes: u64,
    /// Decoded Arrow bytes per input batch.
    pub max_batch_bytes: usize,
    pub batch_rows: usize,
    pub max_output_file_bytes: u64,
    pub max_active_files: usize,
    /// Whole-output sort using the shared layout helper.
    pub layout: Option<LayoutPolicy>,
}

impl Default for OptimizeConfig {
    fn default() -> Self {
        Self {
            target_file_bytes: 128 * 1024 * 1024,
            small_file_bytes: 64 * 1024 * 1024,
            max_files_per_bin: 64,
            max_bins_per_commit: 16,
            max_actions_per_commit: 2048,
            max_plan_bytes: 1024 * 1024,
            latest_partitions: None,
            granularity: None,
            now_ms: None,
            max_decoded_bytes_per_bin: 1024 * 1024 * 1024,
            max_row_group_bytes: 128 * 1024 * 1024,
            max_batch_bytes: 64 * 1024 * 1024,
            batch_rows: 8192,
            max_output_file_bytes: 1023 * 1024 * 1024,
            max_active_files: 1_000_000,
            layout: None,
        }
    }
}

impl OptimizeConfig {
    /// Validate bounded, nonzero limits.
    ///
    /// # Errors
    /// Rejects zero or unbounded limits and targets of 1 GiB or more.
    pub fn validate(&self) -> Result<()> {
        let invalid = |message: &str| Err(fail(MaintenanceErrorKind::InvalidPlan, message.to_owned()));
        if self.target_file_bytes == 0 || self.target_file_bytes >= MAX_TARGET_FILE_BYTES {
            return invalid("target file size must be positive and below 1 GiB");
        }
        if self.max_output_file_bytes < self.target_file_bytes || self.max_output_file_bytes >= MAX_TARGET_FILE_BYTES {
            return invalid("max output size must cover the target and stay below 1 GiB");
        }
        if self.small_file_bytes == 0 || self.small_file_bytes > self.target_file_bytes {
            return invalid("small-file threshold must be positive and at most the target");
        }
        if self.max_files_per_bin < 2 || self.max_bins_per_commit == 0 || self.max_actions_per_commit < 3 {
            return invalid("bins need at least two files and commits at least three actions");
        }
        if self.max_plan_bytes < 1024 || self.max_decoded_bytes_per_bin == 0 || self.max_row_group_bytes == 0 {
            return invalid("plan and decode bounds must be positive");
        }
        if self.max_batch_bytes == 0 || self.batch_rows == 0 || self.max_active_files == 0 {
            return invalid("batch and active-file bounds must be positive");
        }
        if self.latest_partitions == Some(0) {
            return invalid("latest partition count must be positive");
        }
        Ok(())
    }
}

/// Files of one partition rewritten into new outputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptimizeBin {
    pub partition_values: BTreeMap<String, Option<String>>,
    pub files: Vec<DataFile>,
}

/// Serializable OPTIMIZE dry-run plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptimizePlan {
    pub header: PlanHeader,
    pub target_file_bytes: u64,
    pub bins: Vec<OptimizeBin>,
    pub backlog_files: u64,
    pub protected_partitions: u64,
    pub skipped_partitions: u64,
}

impl OptimizePlan {
    fn report(&self, outcome: Outcome) -> MaintenanceReport {
        let mut report = MaintenanceReport::new("optimize", &self.header);
        report.outcome = outcome;
        report.backlog_files = self.backlog_files;
        report.skipped_partitions = self.skipped_partitions;
        report.files_removed = self.bins.iter().map(|bin| bin.files.len() as u64).sum();
        report.bytes_removed = self.bins.iter().flat_map(|bin| &bin.files).map(|file| file.size).sum();
        report.rows_removed = self
            .bins
            .iter()
            .flat_map(|bin| &bin.files)
            .filter_map(|file| file.rows)
            .sum();
        report
    }
}

fn output_estimate(bytes: u64, target: u64) -> usize {
    usize::try_from(bytes.div_ceil(target).max(1)).unwrap_or(usize::MAX)
}

fn partition_columns_are_strings(snapshot: &SnapshotRef, columns: &[String]) -> Result<()> {
    let schema: Schema = snapshot.schema().as_ref().try_into_arrow()?;
    for name in columns {
        let field = schema.field_with_name(name)?;
        if field.data_type() != &DataType::Utf8 {
            return Err(fail(
                MaintenanceErrorKind::Unsupported,
                format!("partition column {name} is not a string"),
            ));
        }
    }
    Ok(())
}

impl DeltaTable {
    /// Plan OPTIMIZE without changing the table.
    ///
    /// # Errors
    /// Returns unsupported-table, limit or snapshot errors.
    pub fn plan_optimize(&self, config: &OptimizeConfig) -> Result<OptimizePlan> {
        config.validate()?;
        let state = self.active_state(config.max_active_files)?;
        state.fingerprint.validate_supported(false)?;
        let columns = state.fingerprint.partition_columns.clone();
        partition_columns_are_strings(&state.snapshot, &columns)?;
        let ingestion = columns == [INGESTION_PARTITION];
        let granularity = match (ingestion, config.granularity) {
            (true, None) => {
                return Err(fail(
                    MaintenanceErrorKind::InvalidPlan,
                    "ingestion-partitioned tables require a granularity",
                ));
            }
            (_, granularity) => granularity,
        };
        let mut groups: BTreeMap<Vec<Option<String>>, Vec<&DataFile>> = BTreeMap::new();
        for file in &state.files {
            let key = columns
                .iter()
                .map(|name| file.partition_values.get(name).cloned().flatten())
                .collect();
            groups.entry(key).or_default().push(file);
        }
        let mut skipped = 0_u64;
        let mut protected = 0_u64;
        let mut ordered: Vec<(Vec<Option<String>>, Vec<&DataFile>)> = Vec::new();
        if let (true, Some(granularity)) = (ingestion, granularity) {
            let now = match config.now_ms {
                Some(now) => now,
                None => now_ms()?,
            };
            let mut valid = Vec::new();
            for (key, files) in groups {
                match key
                    .first()
                    .cloned()
                    .flatten()
                    .map(|value| granularity.parse_partition(&value))
                {
                    Some(Ok(start)) => valid.push((start, key, files)),
                    _ => skipped += 1,
                }
            }
            valid.sort_by_key(|entry| std::cmp::Reverse(entry.0));
            for (index, (start, key, files)) in valid.into_iter().enumerate() {
                if index == 0 || start.saturating_add(granularity.width_ms()) > now {
                    protected += 1;
                } else {
                    ordered.push((key, files));
                }
            }
        } else {
            ordered = groups.into_iter().rev().collect();
        }
        if let Some(latest) = config.latest_partitions {
            ordered.truncate(latest);
        }
        let mut bins = Vec::new();
        let mut backlog = 0_u64;
        let mut actions = 0_usize;
        for (key, mut files) in ordered {
            files.retain(|file| file.size < config.small_file_bytes);
            files.sort_by(|left, right| left.path.cmp(&right.path));
            let values: BTreeMap<String, Option<String>> = columns.iter().cloned().zip(key).collect();
            let mut current: Vec<DataFile> = Vec::new();
            let mut bytes = 0_u64;
            let mut candidates = Vec::new();
            for file in files {
                if !current.is_empty()
                    && (current.len() >= config.max_files_per_bin
                        || bytes.saturating_add(file.size) > config.target_file_bytes)
                {
                    candidates.push((std::mem::take(&mut current), bytes));
                    bytes = 0;
                }
                bytes = bytes.saturating_add(file.size);
                current.push(file.clone());
            }
            candidates.push((current, bytes));
            for (files, bytes) in candidates {
                if files.len() < 2 {
                    continue;
                }
                let cost = files
                    .len()
                    .saturating_add(output_estimate(bytes, config.target_file_bytes));
                if bins.len() >= config.max_bins_per_commit
                    || actions.saturating_add(cost) > config.max_actions_per_commit
                {
                    backlog += files.len() as u64;
                    continue;
                }
                actions += cost;
                bins.push(OptimizeBin {
                    partition_values: values.clone(),
                    files,
                });
            }
        }
        let mut plan = OptimizePlan {
            header: PlanHeader::new(self, &state)?,
            target_file_bytes: config.target_file_bytes,
            bins,
            backlog_files: backlog,
            protected_partitions: protected,
            skipped_partitions: skipped,
        };
        while serde_json::to_vec(&plan)?.len() > config.max_plan_bytes {
            let Some(bin) = plan.bins.pop() else {
                return Err(fail(
                    MaintenanceErrorKind::ResourceLimit,
                    "empty plan exceeds the payload bound",
                ));
            };
            plan.backlog_files += bin.files.len() as u64;
        }
        Ok(plan)
    }

    fn check_optimize_plan(&self, plan: &OptimizePlan, config: &OptimizeConfig) -> Result<()> {
        plan.header.validate(self)?;
        let invalid = |message: &str| Err(fail(MaintenanceErrorKind::InvalidPlan, message.to_owned()));
        if plan.target_file_bytes != config.target_file_bytes {
            return invalid("plan target differs from the apply configuration");
        }
        if plan.bins.len() > config.max_bins_per_commit || serde_json::to_vec(plan)?.len() > config.max_plan_bytes {
            return invalid("plan exceeds bin or payload bounds");
        }
        let mut paths = BTreeSet::new();
        let mut actions = 0_usize;
        for bin in &plan.bins {
            if bin.files.len() < 2 || bin.files.len() > config.max_files_per_bin {
                return invalid("plan bin violates file-count bounds");
            }
            let bytes: u64 = bin.files.iter().map(|file| file.size).sum();
            actions = actions.saturating_add(bin.files.len() + output_estimate(bytes, config.target_file_bytes));
            for file in &bin.files {
                if file.size >= config.small_file_bytes || file.partition_values != bin.partition_values {
                    return invalid("plan input is not an eligible file of its bin partition");
                }
                if !paths.insert(file.path.as_str()) {
                    return invalid("plan repeats an input file");
                }
            }
        }
        if actions > config.max_actions_per_commit {
            return invalid("plan exceeds the action bound");
        }
        Ok(())
    }

    /// Rewrite the planned bins and atomically publish `dataChange=false`
    /// adds and removes. Unrelated appends are allowed; every planned input
    /// must still be active with its planned size.
    ///
    /// # Errors
    /// Returns stale-plan, metadata, limit, cancellation, authority or commit errors.
    pub fn apply_optimize(
        &self,
        plan: &OptimizePlan,
        config: &OptimizeConfig,
        control: &MaintenanceControl,
    ) -> Result<MaintenanceReport> {
        config.validate()?;
        self.check_optimize_plan(plan, config)?;
        let mut report = plan.report(Outcome::Noop);
        let state = self.active_state(config.max_active_files)?;
        plan.header.fingerprint.require_same(&state.fingerprint)?;
        report.version = state.snapshot.version();
        if state
            .snapshot
            .get_app_id_version(&plan.header.app_id("optimize"), &self.engine)?
            .is_some()
        {
            report.outcome = Outcome::AlreadyApplied;
            return Ok(report);
        }
        if plan.bins.is_empty() {
            return Ok(report);
        }
        state.fingerprint.validate_supported(false)?;
        let still_active = |state: &ActiveState, bins: &[&OptimizeBin]| -> Result<()> {
            for file in bins.iter().flat_map(|bin| &bin.files) {
                if state.file(&file.path).is_none_or(|active| active.size != file.size) {
                    return Err(fail(
                        MaintenanceErrorKind::StalePlan,
                        format!("planned input {} is no longer active", file.path),
                    ));
                }
            }
            Ok(())
        };
        still_active(&state, &plan.bins.iter().collect::<Vec<_>>())?;
        let mut outputs = Vec::new();
        let mut kept: Vec<&OptimizeBin> = Vec::new();
        let mut actions = 0_usize;
        for bin in &plan.bins {
            let written = match self.rewrite_bin(&state.snapshot, bin, config, control) {
                Ok(written) => written,
                Err(error) => return Err(self.discard_after(&outputs, error, &mut report)),
            };
            let expected: Option<u64> = bin.files.iter().map(|file| file.rows).sum();
            if expected.is_some_and(|rows| rows != written.iter().map(|output| output.rows).sum::<u64>()) {
                outputs.extend(written);
                let error = fail(
                    MaintenanceErrorKind::StalePlan,
                    "rewritten row count differs from input statistics",
                );
                return Err(self.discard_after(&outputs, error, &mut report));
            }
            // Verify the actual outputs: never commit a rewrite that does not
            // reduce the file count or that exceeds the per-commit action budget.
            let cost = bin.files.len() + written.len();
            let beneficial = written.len() < bin.files.len();
            if !beneficial || actions + cost > config.max_actions_per_commit {
                if beneficial {
                    report.backlog_files += bin.files.len() as u64;
                } else {
                    report.nonbeneficial_bins += 1;
                }
                if let Err(cleanup) = self.discard_outputs(&written) {
                    outputs.extend(written);
                    let error = fail(
                        MaintenanceErrorKind::Uncertain,
                        format!("dropping an unpublishable rewrite failed: {cleanup}"),
                    );
                    return Err(self.discard_after(&outputs, error, &mut report));
                }
                continue;
            }
            actions += cost;
            kept.push(bin);
            outputs.extend(written);
        }
        let inputs: BTreeSet<String> = kept
            .iter()
            .flat_map(|bin| &bin.files)
            .map(|file| file.path.clone())
            .collect();
        report.files_removed = inputs.len() as u64;
        report.rows_removed = kept
            .iter()
            .flat_map(|bin| &bin.files)
            .filter_map(|file| file.rows)
            .sum();
        report.bytes_removed = kept.iter().flat_map(|bin| &bin.files).map(|file| file.size).sum();
        report.files_added = outputs.len() as u64;
        report.rows_added = outputs.iter().map(|output| output.rows).sum();
        report.bytes_added = outputs.iter().map(|output| output.bytes).sum();
        if kept.is_empty() {
            return Ok(report);
        }
        let published = self.publish(
            &plan.header,
            "OPTIMIZE",
            false,
            config.max_active_files,
            control,
            &outputs,
            &mut report,
            |state| {
                still_active(state, &kept)?;
                Ok(Prepared {
                    remove: inputs.clone(),
                    watermark: None,
                })
            },
        )?;
        match published {
            Published::Committed(version) => {
                report.outcome = Outcome::Committed;
                report.version = version;
                report.outputs = outputs.iter().map(|output| output.relative.clone()).collect();
            }
            Published::AlreadyApplied(version) => {
                report.outcome = Outcome::AlreadyApplied;
                report.version = version;
                report.outputs = outputs.iter().map(|output| output.relative.clone()).collect();
            }
        }
        Ok(report)
    }

    fn rewrite_bin(
        &self,
        snapshot: &SnapshotRef,
        bin: &OptimizeBin,
        config: &OptimizeConfig,
        control: &MaintenanceControl,
    ) -> Result<Vec<StagedOutput>> {
        let probe = self.write_context(snapshot, &bin.partition_values)?;
        let logical: SchemaRef = Arc::new(probe.logical_data_schema().as_ref().try_into_arrow()?);
        let physical: SchemaRef = Arc::new(probe.physical_data_schema().as_ref().try_into_arrow()?);
        // Default to the shared publication layout so compaction preserves the
        // customer's whole-file sort order instead of concatenating inputs.
        let layout = match &config.layout {
            Some(layout) => Some(layout.clone()),
            None => crate::delta::configured_publication_layout(&logical)?,
        };
        let properties = match &layout {
            Some(layout) => {
                layout.validate(&logical)?;
                layout.writer_properties(&physical)?
            }
            None => WriterProperties::builder()
                .set_max_row_group_row_count(Some(65_536))
                .build(),
        };
        let mut writer = BinWriter {
            table: self,
            snapshot,
            values: &bin.partition_values,
            logical: Arc::clone(&logical),
            properties,
            config,
            layout,
            buffer: None,
            current: None,
            outputs: Vec::new(),
        };
        let result = (|| -> Result<()> {
            let mut decoded = 0_u64;
            for file in &bin.files {
                control.check()?;
                let rows = self.read_input(file, config, &mut decoded, &logical, &mut writer)?;
                if file.rows.is_some_and(|expected| expected != rows) {
                    return Err(fail(
                        MaintenanceErrorKind::StalePlan,
                        format!("{} row count differs from statistics", file.path),
                    ));
                }
            }
            control.check()?;
            writer.finish()
        })();
        match result {
            Ok(()) => Ok(writer.outputs),
            Err(error) => match writer.abandon() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(fail(
                    error_kind(&error).unwrap_or(MaintenanceErrorKind::Uncertain),
                    format!("{error:#}; cleanup of unpublished outputs failed (retained): {cleanup}"),
                )),
            },
        }
    }

    fn read_input(
        &self,
        file: &DataFile,
        config: &OptimizeConfig,
        decoded: &mut u64,
        logical: &SchemaRef,
        writer: &mut BinWriter<'_>,
    ) -> Result<u64> {
        let path = self.safe_object_path(&file.path)?;
        let store = Arc::clone(&self.store);
        let limit = config.small_file_bytes;
        let expected = file.size;
        let bytes = shared_executor()?.block_on(async move {
            let object = store.get(&path).await?;
            if object.meta.size != expected || object.meta.size >= limit {
                return Err(fail(
                    MaintenanceErrorKind::StalePlan,
                    "input object size differs from the planned size",
                ));
            }
            Ok(object.bytes().await?)
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
        for group in builder.metadata().row_groups() {
            if u64::try_from(group.total_byte_size()).unwrap_or(u64::MAX) > config.max_row_group_bytes {
                return Err(fail(
                    MaintenanceErrorKind::ResourceLimit,
                    format!("{} row group exceeds the decode bound", file.path),
                ));
            }
        }
        // Sorted outputs need batches no larger than one sorted file.
        let batch_rows = writer
            .layout
            .as_ref()
            .map_or(config.batch_rows, |layout| config.batch_rows.min(layout.max_rows));
        let reader = builder.with_batch_size(batch_rows).build()?;
        let mut rows = 0_u64;
        for batch in reader {
            let batch = batch?;
            let size = batch.get_array_memory_size();
            *decoded = decoded.saturating_add(size as u64);
            if size > config.max_batch_bytes || *decoded > config.max_decoded_bytes_per_bin {
                return Err(fail(
                    MaintenanceErrorKind::ResourceLimit,
                    "decoded Arrow input exceeds the configured bound",
                ));
            }
            rows += batch.num_rows() as u64;
            writer.push(conform(&batch, logical)?)?;
        }
        Ok(rows)
    }
}

fn utc(zone: Option<&str>) -> bool {
    matches!(zone, Some("UTC" | "+00:00" | "Z" | "Etc/UTC"))
}

/// Exactly conform a decoded batch to the table's logical data schema.
fn conform(batch: &RecordBatch, logical: &SchemaRef) -> Result<RecordBatch> {
    let mismatch = |message: String| fail(MaintenanceErrorKind::Unsupported, message);
    for field in batch.schema().fields() {
        if logical.field_with_name(field.name()).is_err() {
            return Err(mismatch(format!(
                "data file column {} is not in the table schema",
                field.name()
            )));
        }
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(logical.fields().len());
    for field in logical.fields() {
        let column = match batch.column_by_name(field.name()) {
            Some(column) if column.data_type() == field.data_type() => Arc::clone(column),
            Some(column) => {
                let relabel = column.data_type().equals_datatype(field.data_type())
                    || matches!((column.data_type(), field.data_type()),
                        (DataType::Timestamp(left, a), DataType::Timestamp(right, b))
                            if left == right && utc(a.as_deref()) && utc(b.as_deref()));
                if !relabel {
                    return Err(mismatch(format!(
                        "data file column {} has type {}",
                        field.name(),
                        column.data_type()
                    )));
                }
                make_array(
                    column
                        .to_data()
                        .into_builder()
                        .data_type(field.data_type().clone())
                        .build()?,
                )
            }
            None if field.is_nullable() => new_null_array(field.data_type(), batch.num_rows()),
            None => {
                return Err(mismatch(format!(
                    "required column {} is missing from a data file",
                    field.name()
                )));
            }
        };
        if !field.is_nullable() && column.null_count() > 0 {
            return Err(mismatch(format!("required column {} contains nulls", field.name())));
        }
        columns.push(column);
    }
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(logical),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

struct OpenOutput {
    local: PathBuf,
    relative: String,
    writer: ArrowWriter<File>,
    stats: FileStatsAccumulator,
    context: BoundWriteContext,
    rows: u64,
}

struct BinWriter<'a> {
    table: &'a DeltaTable,
    snapshot: &'a SnapshotRef,
    values: &'a BTreeMap<String, Option<String>>,
    logical: SchemaRef,
    properties: WriterProperties,
    config: &'a OptimizeConfig,
    layout: Option<LayoutPolicy>,
    buffer: Option<FileSortBuffer>,
    current: Option<OpenOutput>,
    outputs: Vec<StagedOutput>,
}

impl BinWriter<'_> {
    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if let Some(layout) = self.layout.clone() {
            if self.buffer.is_none() {
                self.buffer = Some(FileSortBuffer::new(Arc::clone(&self.logical), layout.clone())?);
            }
            let buffer = self.buffer.as_mut().context("missing sort buffer")?;
            if !buffer.try_push(&batch)? {
                self.flush_sorted()?;
                let mut buffer = FileSortBuffer::new(Arc::clone(&self.logical), layout)?;
                if !buffer.try_push(&batch)? {
                    return Err(fail(
                        MaintenanceErrorKind::ResourceLimit,
                        "batch exceeds the sorted-file budget",
                    ));
                }
                self.buffer = Some(buffer);
            }
            return Ok(());
        }
        self.write(&batch)?;
        let full = self.current.as_ref().is_some_and(|output| {
            (output.writer.bytes_written() + output.writer.in_progress_size()) as u64 >= self.config.target_file_bytes
        });
        if full {
            self.close_current()?;
        }
        Ok(())
    }

    fn flush_sorted(&mut self) -> Result<()> {
        if let Some(buffer) = self.buffer.take()
            && buffer.rows() > 0
        {
            let sorted = buffer.finish()?;
            self.write(&sorted)?;
            self.close_current()?;
        }
        Ok(())
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let output = match self.current.as_mut() {
            Some(output) => output,
            None => {
                let output = self.open()?;
                self.current.insert(output)
            }
        };
        let evaluator = self.table.engine.evaluation_handler().new_expression_evaluator(
            Arc::clone(output.context.logical_data_schema()),
            output.context.logical_to_physical(),
            Arc::clone(output.context.physical_data_schema()).into(),
        )?;
        let physical = evaluator
            .evaluate(&ArrowEngineData::new(batch.clone()))?
            .try_into_record_batch()?;
        output.stats.merge(&physical)?;
        output.writer.write(&physical)?;
        output.rows += physical.num_rows() as u64;
        if (output.writer.bytes_written() + output.writer.in_progress_size()) as u64 > self.config.max_output_file_bytes
        {
            return Err(fail(
                MaintenanceErrorKind::ResourceLimit,
                "output exceeds the maximum output file size",
            ));
        }
        Ok(())
    }

    fn open(&self) -> Result<OpenOutput> {
        let context = self.table.write_context(self.snapshot, self.values)?;
        let physical: SchemaRef = Arc::new(context.physical_data_schema().as_ref().try_into_arrow()?);
        let mut relative = String::new();
        for (name, value) in self.values {
            let value = value.as_deref().unwrap_or("__HIVE_DEFAULT_PARTITION__");
            if name.contains(['/', '\\', '=']) || value.contains(['/', '\\']) || value == "." || value == ".." {
                return Err(fail(
                    MaintenanceErrorKind::Unsupported,
                    "unsafe Hive partition component",
                ));
            }
            relative.push_str(&format!("{name}={value}/"));
        }
        relative.push_str(&format!("part-{}.parquet", uuid::Uuid::new_v4()));
        let local = self.table.root.join(&relative);
        let directory = local.parent().context("missing output directory")?;
        fs::create_dir_all(directory)?;
        let file = fs::OpenOptions::new().write(true).create_new(true).open(&local)?;
        let writer = ArrowWriter::try_new(file, physical, Some(self.properties.clone()))?;
        let stats = FileStatsAccumulator::new(context.stats_columns(), context.physical_data_schema());
        Ok(OpenOutput {
            local,
            relative,
            writer,
            stats,
            context,
            rows: 0,
        })
    }

    fn close_current(&mut self) -> Result<()> {
        let Some(output) = self.current.take() else {
            return Ok(());
        };
        let OpenOutput {
            local,
            relative,
            writer,
            stats,
            context,
            rows,
        } = output;
        let staged = (|| -> Result<StagedOutput> {
            writer.close()?;
            File::open(&local)?.sync_all()?;
            self.table.sync_local_output(&local)?;
            let metadata = fs::metadata(&local)?;
            if metadata.len() > self.config.max_output_file_bytes {
                return Err(fail(
                    MaintenanceErrorKind::ResourceLimit,
                    "output exceeds the maximum output file size",
                ));
            }
            let modified_ms = i64::try_from(metadata.modified()?.duration_since(UNIX_EPOCH)?.as_millis())?;
            let stats = stats.finish()?.context("refusing to publish an empty output")?;
            Ok(StagedOutput {
                url: self.table.data_url(&relative)?,
                relative: relative.clone(),
                local: local.clone(),
                modified_ms,
                bytes: metadata.len(),
                rows,
                stats,
                context,
                uploaded: false,
            })
        })();
        let staged = match staged {
            Ok(staged) => staged,
            Err(error) => {
                let _ = fs::remove_file(&local);
                return Err(error);
            }
        };
        self.outputs.push(staged);
        let index = self.outputs.len() - 1;
        self.table.upload_output(&mut self.outputs[index])?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.flush_sorted()?;
        self.close_current()
    }

    fn abandon(&mut self) -> std::result::Result<(), String> {
        let mut failures = Vec::new();
        if let Some(output) = self.current.take() {
            drop(output.writer);
            if let Err(error) = self.table.discard_output(&output.local, &self.table.url, false) {
                failures.push(format!("{}: {error:#}", output.relative));
            }
        }
        if let Err(error) = self.table.discard_outputs(&self.outputs) {
            failures.push(error);
        }
        self.outputs.clear();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}

impl DeltaTable {
    /// Upload a staged cloud output with create-only semantics; local outputs are already in place.
    fn upload_output(&self, output: &mut StagedOutput) -> Result<()> {
        if self.url.scheme() == "file" {
            return Ok(());
        }
        self.check_publication()?;
        let bytes = fs::read(&output.local)?;
        if bytes.len() as u64 != output.bytes {
            return Err(fail(
                MaintenanceErrorKind::Uncertain,
                "staged output changed before upload",
            ));
        }
        let path = object_store::path::Path::from_url_path(output.url.path())?;
        let store = Arc::clone(&self.store);
        shared_executor()?
            .block_on(async move { store.put_opts(&path, bytes.into(), PutMode::Create.into()).await })?;
        output.uploaded = true;
        match fs::remove_file(&output.local) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(anyhow::Error::new(error).context("remove uploaded staging file")),
        }
    }
}

/// PURGE bounds and its explicit UTC retention anchor.
#[derive(Clone, Debug)]
pub struct PurgeConfig {
    pub retention_ms: i64,
    /// Explicit wall-clock UTC anchor; the cutoff is `floor(anchor - retention)`.
    pub anchor_ms: i64,
    pub granularity: Granularity,
    pub max_files_per_commit: usize,
    pub max_plan_bytes: usize,
    pub max_active_files: usize,
}

impl PurgeConfig {
    /// Defaults for everything except retention, anchor and granularity.
    #[must_use]
    pub const fn new(retention_ms: i64, anchor_ms: i64, granularity: Granularity) -> Self {
        Self {
            retention_ms,
            anchor_ms,
            granularity,
            max_files_per_commit: 10_000,
            max_plan_bytes: 4 * 1024 * 1024,
            max_active_files: 1_000_000,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.retention_ms <= 0 || self.anchor_ms < 0 || self.max_files_per_commit == 0 || self.max_plan_bytes < 1024
        {
            return Err(fail(
                MaintenanceErrorKind::InvalidPlan,
                "purge retention, anchor and bounds must be positive",
            ));
        }
        validate_anchor("purge", self.anchor_ms)
    }

    fn cutoff(&self) -> Result<i64> {
        purge_cutoff(self.granularity, self.anchor_ms, self.retention_ms)
    }
}

fn purge_cutoff(granularity: Granularity, anchor_ms: i64, retention_ms: i64) -> Result<i64> {
    Ok(granularity.floor_ms(
        anchor_ms
            .checked_sub(retention_ms)
            .ok_or_else(|| fail(MaintenanceErrorKind::InvalidPlan, "retention exceeds the anchor"))?,
    ))
}

/// One whole expired ingestion partition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgePartition {
    pub value: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub files: Vec<DataFile>,
}

/// Serializable PURGE dry-run plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgePlan {
    pub header: PlanHeader,
    pub granularity: Granularity,
    pub retention_ms: i64,
    pub anchor_ms: i64,
    pub cutoff_ms: i64,
    /// Every valid partition ending at or before this is removed; also the replay fence.
    pub watermark_ms: Option<i64>,
    pub partitions: Vec<PurgePartition>,
    pub backlog_files: u64,
    pub skipped_partitions: u64,
}

impl PurgePlan {
    fn report(&self, outcome: Outcome) -> MaintenanceReport {
        let mut report = MaintenanceReport::new("purge", &self.header);
        report.outcome = outcome;
        report.cutoff_ms = Some(self.cutoff_ms);
        report.watermark_ms = self.watermark_ms;
        report.backlog_files = self.backlog_files;
        report.skipped_partitions = self.skipped_partitions;
        report.files_removed = self
            .partitions
            .iter()
            .map(|partition| partition.files.len() as u64)
            .sum();
        report.bytes_removed = self
            .partitions
            .iter()
            .flat_map(|partition| &partition.files)
            .map(|file| file.size)
            .sum();
        report.rows_removed = self
            .partitions
            .iter()
            .flat_map(|partition| &partition.files)
            .filter_map(|file| file.rows)
            .sum();
        report
    }
}

type PartitionMap = BTreeMap<i64, (String, Vec<DataFile>)>;

/// Valid ingestion partitions (start ms -> files) plus the count of null/malformed ones.
fn ingestion_partitions(state: &ActiveState, granularity: Granularity) -> Result<(PartitionMap, u64)> {
    if state.fingerprint.partition_columns != [INGESTION_PARTITION] {
        return Err(fail(
            MaintenanceErrorKind::Unsupported,
            format!("PURGE requires exactly the {INGESTION_PARTITION} partition"),
        ));
    }
    let mut valid: PartitionMap = BTreeMap::new();
    let mut malformed = BTreeSet::new();
    for file in &state.files {
        let value = file.partition_values.get(INGESTION_PARTITION).cloned().flatten();
        match value.as_deref().map(|value| granularity.parse_partition(value)) {
            Some(Ok(start)) => valid
                .entry(start)
                .or_insert_with(|| (value.clone().unwrap_or_default(), Vec::new()))
                .1
                .push(file.clone()),
            _ => {
                malformed.insert(value);
            }
        }
    }
    Ok((valid, malformed.len() as u64))
}

impl DeltaTable {
    /// Plan PURGE of whole expired ingestion partitions without changing the table.
    ///
    /// # Errors
    /// Returns unsupported-table, limit or snapshot errors.
    pub fn plan_purge(&self, config: &PurgeConfig) -> Result<PurgePlan> {
        config.validate()?;
        let state = self.active_state(config.max_active_files)?;
        state.fingerprint.validate_supported(true)?;
        let (valid, skipped) = ingestion_partitions(&state, config.granularity)?;
        let cutoff = config.cutoff()?;
        let width = config.granularity.width_ms();
        let newest = valid.keys().next_back().copied();
        let mut partitions = Vec::new();
        let mut files = 0_usize;
        let mut backlog = 0_u64;
        for (start, (value, members)) in &valid {
            let end = start + width;
            if end > cutoff || Some(*start) == newest {
                break;
            }
            if backlog > 0 || files + members.len() > config.max_files_per_commit {
                if partitions.is_empty() {
                    return Err(fail(
                        MaintenanceErrorKind::ResourceLimit,
                        format!("partition {value} exceeds the per-commit file bound"),
                    ));
                }
                backlog += members.len() as u64;
                continue;
            }
            files += members.len();
            partitions.push(PurgePartition {
                value: value.clone(),
                start_ms: *start,
                end_ms: end,
                files: members.clone(),
            });
        }
        let watermark = partitions.last().map(|last: &PurgePartition| {
            if backlog > 0 {
                last.end_ms
            } else {
                newest.map_or(cutoff, |newest| newest.min(cutoff))
            }
        });
        let plan = PurgePlan {
            header: PlanHeader::new(self, &state)?,
            granularity: config.granularity,
            retention_ms: config.retention_ms,
            anchor_ms: config.anchor_ms,
            cutoff_ms: cutoff,
            watermark_ms: watermark,
            partitions,
            backlog_files: backlog,
            skipped_partitions: skipped,
        };
        if serde_json::to_vec(&plan)?.len() > config.max_plan_bytes {
            return Err(fail(
                MaintenanceErrorKind::ResourceLimit,
                "purge plan exceeds the payload bound",
            ));
        }
        Ok(plan)
    }

    /// Remove exactly the planned expired partitions (`dataChange=true`) and
    /// advance the retained `arrowcol.purge.before-ms` fence in the same commit.
    /// Any concurrent add or removal inside the expired predicate makes the plan stale.
    ///
    /// # Errors
    /// Returns stale-plan, metadata, cancellation, authority or commit errors.
    pub fn apply_purge(
        &self,
        plan: &PurgePlan,
        config: &PurgeConfig,
        control: &MaintenanceControl,
    ) -> Result<MaintenanceReport> {
        config.validate()?;
        plan.header.validate(self)?;
        let invalid = |message: &str| Err(fail(MaintenanceErrorKind::InvalidPlan, message.to_owned()));
        if plan.granularity != config.granularity {
            return invalid("plan granularity differs from the apply configuration");
        }
        // Bind the plan to its recorded policy and to the current one before any I/O:
        // a forged cutoff or a now-longer retention never authorizes removal.
        if plan.retention_ms <= 0 {
            return invalid("plan retention must be positive");
        }
        validate_anchor("purge plan", plan.anchor_ms)?;
        if purge_cutoff(plan.granularity, plan.anchor_ms, plan.retention_ms)? != plan.cutoff_ms {
            return invalid("plan cutoff does not match its recorded anchor and retention");
        }
        if plan.cutoff_ms > config.cutoff()? {
            return Err(fail(
                MaintenanceErrorKind::StalePlan,
                "plan cutoff is later than the apply configuration allows (retention increased or anchor moved); replan",
            ));
        }
        let planned: BTreeSet<(String, u64)> = plan
            .partitions
            .iter()
            .flat_map(|partition| &partition.files)
            .map(|file| (file.path.clone(), file.size))
            .collect();
        if planned.len() > config.max_files_per_commit {
            return invalid("plan exceeds the per-commit file bound");
        }
        let width = plan.granularity.width_ms();
        for partition in &plan.partitions {
            if plan.granularity.parse_partition(&partition.value)? != partition.start_ms
                || partition.end_ms != partition.start_ms + width
                || plan.watermark_ms.is_none_or(|watermark| partition.end_ms > watermark)
                || partition
                    .files
                    .iter()
                    .any(|file| file.partition_values.get(INGESTION_PARTITION) != Some(&Some(partition.value.clone())))
            {
                return invalid("plan partition is inconsistent with its watermark");
            }
        }
        if plan.watermark_ms.is_some_and(|watermark| watermark > plan.cutoff_ms) {
            return invalid("plan watermark exceeds its cutoff");
        }
        let mut report = plan.report(Outcome::Noop);
        let state = self.active_state(config.max_active_files)?;
        plan.header.fingerprint.require_same(&state.fingerprint)?;
        report.version = state.snapshot.version();
        if state
            .snapshot
            .get_app_id_version(&plan.header.app_id("delete"), &self.engine)?
            .is_some()
        {
            report.outcome = Outcome::AlreadyApplied;
            return Ok(report);
        }
        let Some(watermark) = plan.watermark_ms else {
            return Ok(report);
        };
        if planned.is_empty() {
            return invalid("purge watermark without removals");
        }
        let granularity = plan.granularity;
        let published = self.publish(
            &plan.header,
            "DELETE",
            true,
            config.max_active_files,
            control,
            &[],
            &mut report,
            |state| {
                let (valid, _) = ingestion_partitions(state, granularity)?;
                let newest = valid.keys().next_back().copied();
                let mut current = BTreeSet::new();
                for (start, (_, files)) in &valid {
                    if start + width > watermark {
                        break;
                    }
                    if Some(*start) == newest {
                        return Err(fail(
                            MaintenanceErrorKind::StalePlan,
                            "planned partition is now the newest partition",
                        ));
                    }
                    current.extend(files.iter().map(|file| (file.path.clone(), file.size)));
                }
                if current != planned {
                    return Err(fail(
                        MaintenanceErrorKind::StalePlan,
                        "expired partitions changed since planning; replan",
                    ));
                }
                Ok(Prepared {
                    remove: planned.iter().map(|(path, _)| path.clone()).collect(),
                    watermark: Some(watermark),
                })
            },
        )?;
        match published {
            Published::Committed(version) => {
                report.outcome = Outcome::Committed;
                report.version = version;
            }
            Published::AlreadyApplied(version) => {
                report.outcome = Outcome::AlreadyApplied;
                report.version = version;
            }
        }
        Ok(report)
    }
}

/// VACUUM LITE bounds. Retention below seven days requires the unsafe test
/// override plus a disposable local root under the system temp directory.
#[derive(Clone, Debug)]
pub struct VacuumConfig {
    pub retention_ms: i64,
    /// Explicit wall-clock UTC anchor for the tombstone cutoff.
    pub now_ms: i64,
    pub unsafe_allow_short_retention_for_tests: bool,
    pub max_log_files: usize,
    pub max_log_file_bytes: u64,
    pub max_deletes: usize,
    pub delete_chunk: usize,
    pub max_plan_bytes: usize,
    pub max_active_files: usize,
}

impl VacuumConfig {
    /// Defaults for everything except retention and anchor.
    #[must_use]
    pub const fn new(retention_ms: i64, now_ms: i64) -> Self {
        Self {
            retention_ms,
            now_ms,
            unsafe_allow_short_retention_for_tests: false,
            max_log_files: 100_000,
            max_log_file_bytes: 64 * 1024 * 1024,
            max_deletes: 10_000,
            delete_chunk: 256,
            max_plan_bytes: 8 * 1024 * 1024,
            max_active_files: 1_000_000,
        }
    }

    fn cutoff(&self) -> Result<i64> {
        validate_anchor("vacuum", self.now_ms)?;
        self.now_ms
            .checked_sub(self.retention_ms)
            .ok_or_else(|| fail(MaintenanceErrorKind::InvalidPlan, "retention exceeds the anchor"))
    }
}

/// One expired tombstone eligible for physical deletion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VacuumCandidate {
    pub path: String,
    pub deletion_timestamp_ms: i64,
}

/// Serializable VACUUM LITE dry-run plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VacuumPlan {
    pub header: PlanHeader,
    pub retention_ms: i64,
    /// Explicit UTC anchor the plan was computed at; `cutoff_ms = now_ms - retention_ms`.
    pub now_ms: i64,
    pub cutoff_ms: i64,
    pub candidates: Vec<VacuumCandidate>,
    pub undated_tombstones: u64,
    pub retained_tombstones: u64,
    pub backlog_files: u64,
}

struct LogReplay {
    active: BTreeSet<String>,
    tombstones: BTreeMap<String, (String, Option<i64>)>,
}

impl DeltaTable {
    fn check_vacuum_retention(&self, retention_ms: i64, unsafe_override: bool) -> Result<()> {
        let unsafe_request = |message: &str| Err(fail(MaintenanceErrorKind::UnsafeRequest, message.to_owned()));
        if retention_ms < 0 {
            return unsafe_request("negative retention");
        }
        if retention_ms >= MIN_VACUUM_RETENTION_MS {
            return Ok(());
        }
        if !unsafe_override {
            return unsafe_request("VACUUM retention below 7 days requires the unsafe test override");
        }
        if self.url.scheme() != "file" {
            return unsafe_request("short VACUUM retention is only allowed for disposable local test roots");
        }
        let temp = fs::canonicalize(std::env::temp_dir())?;
        if !self.root.starts_with(&temp) || self.root == temp || !self.root.join(DISPOSABLE_TEST_MARKER).is_file() {
            return unsafe_request("short VACUUM retention requires a marked disposable root under the temp directory");
        }
        Ok(())
    }

    fn log_key(&self, raw: &str) -> Result<String> {
        Ok(self
            .relative_url(raw)
            .map_err(|_| fail(MaintenanceErrorKind::UnsafeRequest, format!("unsafe log path {raw:?}")))?
            .to_string())
    }

    /// Replay complete JSON history 0..=version without listing data directories.
    fn replay_log(&self, version: u64, config: &VacuumConfig) -> Result<LogReplay> {
        let unsupported = |message: String| fail(MaintenanceErrorKind::Unsupported, message);
        if usize::try_from(version).unwrap_or(usize::MAX) >= config.max_log_files {
            return Err(fail(
                MaintenanceErrorKind::ResourceLimit,
                "log history exceeds the replay bound",
            ));
        }
        let prefix = self.object_path("_delta_log/")?;
        let store = Arc::clone(&self.store);
        let limit = config.max_log_files.saturating_mul(4);
        let names = shared_executor()?.block_on(async move {
            use futures::StreamExt;
            let mut names = BTreeSet::new();
            let mut entries = store.list(Some(&prefix));
            let mut seen = 0_usize;
            while let Some(entry) = entries.next().await {
                let entry = entry?;
                seen += 1;
                if seen > limit {
                    return Err(fail(
                        MaintenanceErrorKind::ResourceLimit,
                        "log listing exceeds its bound",
                    ));
                }
                if let Some(name) = entry.location.filename()
                    && entry
                        .location
                        .prefix_match(&prefix)
                        .is_some_and(|mut parts| parts.nth(1).is_none())
                {
                    names.insert(name.to_owned());
                }
            }
            Ok::<_, anyhow::Error>(names)
        })?;
        let mut replay = LogReplay {
            active: BTreeSet::new(),
            tombstones: BTreeMap::new(),
        };
        for current in 0..=version {
            let name = format!("{current:020}.json");
            if !names.contains(&name) {
                return Err(unsupported(format!(
                    "VACUUM LITE requires complete JSON log history; {name} is missing (checkpoint-only history unsupported)"
                )));
            }
            let path = self.object_path(&format!("_delta_log/{name}"))?;
            let store = Arc::clone(&self.store);
            let limit = config.max_log_file_bytes;
            let bytes = shared_executor()?.block_on(async move {
                let object = store.get(&path).await?;
                if object.meta.size > limit {
                    return Err(fail(
                        MaintenanceErrorKind::ResourceLimit,
                        "log file exceeds its read bound",
                    ));
                }
                Ok(object.bytes().await?)
            })?;
            for line in bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
            {
                let action: serde_json::Value = serde_json::from_slice(line)?;
                if let Some(add) = action.get("add") {
                    let raw = add
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .context("add without path")?;
                    if add.get("deletionVector").is_some_and(|vector| !vector.is_null()) {
                        return Err(unsupported("deletion vectors are unsupported by VACUUM LITE".into()));
                    }
                    let key = self.log_key(raw)?;
                    replay.tombstones.remove(&key);
                    replay.active.insert(key);
                } else if let Some(remove) = action.get("remove") {
                    let raw = remove
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .context("remove without path")?;
                    if remove.get("deletionVector").is_some_and(|vector| !vector.is_null()) {
                        return Err(unsupported("deletion vectors are unsupported by VACUUM LITE".into()));
                    }
                    let key = self.log_key(raw)?;
                    replay.active.remove(&key);
                    let stamp = remove.get("deletionTimestamp").and_then(serde_json::Value::as_i64);
                    replay.tombstones.insert(key, (raw.to_owned(), stamp));
                }
            }
        }
        Ok(replay)
    }

    fn active_keys(&self, state: &ActiveState) -> Result<BTreeSet<String>> {
        state.files.iter().map(|file| self.log_key(&file.path)).collect()
    }

    /// Plan VACUUM LITE from tombstones in the log; never lists data directories.
    ///
    /// # Errors
    /// Rejects unsafe retention, incomplete history, unsupported tables and bounds.
    pub fn plan_vacuum(&self, config: &VacuumConfig) -> Result<VacuumPlan> {
        self.check_vacuum_retention(config.retention_ms, config.unsafe_allow_short_retention_for_tests)?;
        if config.max_deletes == 0 || config.delete_chunk == 0 {
            return Err(fail(
                MaintenanceErrorKind::InvalidPlan,
                "delete bounds must be positive",
            ));
        }
        let state = self.active_state(config.max_active_files)?;
        state.fingerprint.validate_supported(false)?;
        let replay = self.replay_log(state.snapshot.version(), config)?;
        if replay.active != self.active_keys(&state)? {
            return Err(fail(
                MaintenanceErrorKind::Unsupported,
                "log replay disagrees with Kernel's active file set",
            ));
        }
        let cutoff = config.cutoff()?;
        let mut candidates = Vec::new();
        let (mut undated, mut retained, mut backlog) = (0_u64, 0_u64, 0_u64);
        for (raw, stamp) in replay.tombstones.values() {
            match stamp {
                None => undated += 1,
                Some(stamp) if *stamp >= cutoff => retained += 1,
                Some(stamp) => {
                    self.safe_object_path(raw)?;
                    if candidates.len() >= config.max_deletes {
                        backlog += 1;
                    } else {
                        candidates.push(VacuumCandidate {
                            path: raw.clone(),
                            deletion_timestamp_ms: *stamp,
                        });
                    }
                }
            }
        }
        let plan = VacuumPlan {
            header: PlanHeader::new(self, &state)?,
            retention_ms: config.retention_ms,
            now_ms: config.now_ms,
            cutoff_ms: cutoff,
            candidates,
            undated_tombstones: undated,
            retained_tombstones: retained,
            backlog_files: backlog,
        };
        if serde_json::to_vec(&plan)?.len() > config.max_plan_bytes {
            return Err(fail(
                MaintenanceErrorKind::ResourceLimit,
                "vacuum plan exceeds the payload bound",
            ));
        }
        Ok(plan)
    }

    /// Physically delete planned expired tombstones. No Delta commit is written.
    /// Each chunk re-checks authority and the fresh active set immediately before
    /// deleting; already-absent files count as success. Cancellation returns a
    /// `Partial` report with the progress made.
    ///
    /// # Errors
    /// Returns unsafe, stale, metadata, authority or storage errors (with partial progress attached).
    pub fn apply_vacuum(
        &self,
        plan: &VacuumPlan,
        config: &VacuumConfig,
        control: &MaintenanceControl,
    ) -> Result<MaintenanceReport> {
        plan.header.validate(self)?;
        self.check_vacuum_retention(plan.retention_ms, config.unsafe_allow_short_retention_for_tests)?;
        self.check_vacuum_retention(config.retention_ms, config.unsafe_allow_short_retention_for_tests)?;
        if plan.candidates.len() > config.max_deletes || config.delete_chunk == 0 {
            return Err(fail(MaintenanceErrorKind::InvalidPlan, "plan exceeds the delete bound"));
        }
        // Bind the plan to its recorded policy and to the current one before any
        // delete: only tombstones older than BOTH cutoffs may be removed.
        validate_anchor("vacuum plan", plan.now_ms)?;
        if plan.now_ms.checked_sub(plan.retention_ms) != Some(plan.cutoff_ms) {
            return Err(fail(
                MaintenanceErrorKind::InvalidPlan,
                "plan cutoff does not match its recorded anchor and retention",
            ));
        }
        let effective = plan.cutoff_ms.min(config.cutoff()?);
        let mut report = MaintenanceReport::new("vacuum", &plan.header);
        report.cutoff_ms = Some(effective);
        report.backlog_files = plan.backlog_files;
        let state = self.active_state(config.max_active_files)?;
        plan.header.fingerprint.require_same(&state.fingerprint)?;
        report.version = state.snapshot.version();
        if plan.candidates.is_empty() {
            return Ok(report);
        }
        let replay = self.replay_log(state.snapshot.version(), config)?;
        for candidate in &plan.candidates {
            let key = self.log_key(&candidate.path)?;
            match replay.tombstones.get(&key) {
                Some((_, Some(stamp))) if *stamp == candidate.deletion_timestamp_ms && *stamp < effective => {}
                Some((_, Some(stamp))) if *stamp == candidate.deletion_timestamp_ms => {
                    return Err(fail(
                        MaintenanceErrorKind::StalePlan,
                        format!(
                            "{} is not older than the effective cutoff {effective} (retention changed); replan",
                            candidate.path
                        ),
                    ));
                }
                _ => {
                    return Err(fail(
                        MaintenanceErrorKind::StalePlan,
                        format!("{} is no longer an expired tombstone", candidate.path),
                    ));
                }
            }
        }
        for chunk in plan.candidates.chunks(config.delete_chunk) {
            if control.interrupted() {
                report.outcome = Outcome::Partial;
                report.interrupted = true;
                return Ok(report);
            }
            let fresh = self.active_state(config.max_active_files).map_err(|error| {
                fail_with(
                    MaintenanceErrorKind::StalePlan,
                    format!("fresh active-set check failed: {error}"),
                    &report,
                )
            })?;
            if let Err(error) = plan.header.fingerprint.require_same(&fresh.fingerprint) {
                return Err(fail_with(
                    MaintenanceErrorKind::MetadataChanged,
                    error.to_string(),
                    &report,
                ));
            }
            let active = self.active_keys(&fresh)?;
            for candidate in chunk {
                if active.contains(&self.log_key(&candidate.path)?) {
                    return Err(fail_with(
                        MaintenanceErrorKind::UnsafeRequest,
                        format!("tombstoned path {} is active again; aborting", candidate.path),
                        &report,
                    ));
                }
            }
            if let Err(error) = self.check_publication() {
                return Err(fail_with(
                    MaintenanceErrorKind::UnsafeRequest,
                    format!("authority check failed: {error}"),
                    &report,
                ));
            }
            for candidate in chunk {
                let path = self.safe_object_path(&candidate.path)?;
                let store = Arc::clone(&self.store);
                let result = shared_executor()?.block_on(async move { store.delete(&path).await });
                match result {
                    Ok(()) => report.files_deleted += 1,
                    Err(object_store::Error::NotFound { .. }) => report.files_already_absent += 1,
                    Err(error) => {
                        return Err(fail_with(
                            MaintenanceErrorKind::Uncertain,
                            format!("delete failed: {error}"),
                            &report,
                        ));
                    }
                }
            }
        }
        report.files_removed = report.files_deleted + report.files_already_absent;
        report.outcome = Outcome::Deleted;
        Ok(report)
    }
}

/// Any serialized maintenance plan, tagged by operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum MaintenancePlan {
    Optimize(OptimizePlan),
    Purge(PurgePlan),
    Vacuum(VacuumPlan),
}

impl MaintenancePlan {
    /// Dry-run report of the plan.
    #[must_use]
    pub fn report(&self) -> MaintenanceReport {
        match self {
            Self::Optimize(plan) => plan.report(Outcome::Planned),
            Self::Purge(plan) => plan.report(Outcome::Planned),
            Self::Vacuum(plan) => {
                let mut report = MaintenanceReport::new("vacuum", &plan.header);
                report.outcome = Outcome::Planned;
                report.cutoff_ms = Some(plan.cutoff_ms);
                report.files_removed = plan.candidates.len() as u64;
                report.backlog_files = plan.backlog_files;
                report
            }
        }
    }

    /// Identity header.
    #[must_use]
    pub const fn header(&self) -> &PlanHeader {
        match self {
            Self::Optimize(plan) => &plan.header,
            Self::Purge(plan) => &plan.header,
            Self::Vacuum(plan) => &plan.header,
        }
    }
}
