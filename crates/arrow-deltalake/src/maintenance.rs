//! Delta maintenance over Delta Kernel 0.29: OPTIMIZE, PURGE and VACUUM LITE.
//!
//! Every operation is a serializable dry-run plan plus an apply step. Apply
//! revalidates the table id, metadata, protocol and its complete read set
//! against a fresh snapshot immediately before each publication or deletion,
//! and fails explicitly (never silently falls back) on unsupported tables.

use crate::delta::{DeltaTable, INGESTION_PARTITION, PURGE_WATERMARK, collision_backoff, object_store_for_url};
use crate::delta_io::shared_executor;
use crate::layout::{FileSortBuffer, LayoutPolicy};
use crate::{sync_directory, sync_directory_chain, sync_file};
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
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
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
    /// A Kernel checkpoint was written; no commit and no history deletion.
    Checkpointed,
    /// Proven-redundant `_delta_log` history was deleted; no commit.
    Pruned,
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
            Self::Checkpointed => "checkpointed",
            Self::Pruned => "pruned",
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
                Ok(Some(Published::AlreadyApplied(version))) => {
                    return self.durable_applied(version, outputs, report);
                }
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

    /// Acknowledge a visible plan identity only after local replay is durable; a
    /// failed repair keeps the outcome uncertain and every output retained.
    fn durable_applied(
        &self,
        version: u64,
        outputs: &[StagedOutput],
        report: &mut MaintenanceReport,
    ) -> Result<Published> {
        if let Err(error) = self.sync_replayed_local_state(version) {
            report.outputs = outputs.iter().map(|output| output.relative.clone()).collect();
            return Err(fail_with(
                MaintenanceErrorKind::Uncertain,
                format!("plan visible at version {version} but local durability unproven; outputs retained: {error:#}"),
                report,
            ));
        }
        Ok(Published::AlreadyApplied(version))
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
            return self.durable_applied(version, outputs, report);
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
            self.sync_replayed_local_state(report.version).map_err(|error| {
                fail(
                    MaintenanceErrorKind::Uncertain,
                    format!("plan already visible but local durability unproven: {error:#}"),
                )
            })?;
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
            self.sync_replayed_local_state(report.version).map_err(|error| {
                fail(
                    MaintenanceErrorKind::Uncertain,
                    format!("plan already visible but local durability unproven: {error:#}"),
                )
            })?;
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
    /// Read bound for the floor checkpoint seeding replay after log pruning.
    pub max_checkpoint_bytes: u64,
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
            max_checkpoint_bytes: DEFAULT_MAX_CHECKPOINT_BYTES,
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

#[derive(Default)]
struct LogReplay {
    active: BTreeSet<String>,
    /// Recovery-relevant state of each active add, keyed like `active`.
    adds: BTreeMap<String, AddState>,
    tombstones: BTreeMap<String, (String, Option<i64>)>,
    /// Latest version per application transaction id.
    txns: BTreeMap<String, i64>,
    /// Set when a later txn action carried a lower version than an earlier one.
    txn_regressed: bool,
    table_id: Option<String>,
    /// Last `protocol` action replayed.
    protocol: Option<ProtocolState>,
    /// Last `metaData` action replayed.
    metadata: Option<MetadataState>,
}

/// Recovery-relevant fields of an active add. Statistics are deliberately not
/// compared: they are advisory and checkpoint writers may re-encode them.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AddState {
    size: i64,
    partition_values: BTreeMap<String, Option<String>>,
}

/// Normalized `protocol` action: feature lists compare as sets.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ProtocolState {
    min_reader_version: i64,
    min_writer_version: i64,
    reader_features: Option<BTreeSet<String>>,
    writer_features: Option<BTreeSet<String>>,
}

/// Normalized `metaData` action: the schema is compared as parsed JSON (object
/// keys order-insensitive) and an absent configuration equals an empty one.
#[derive(Clone, Debug, PartialEq, Eq)]
struct MetadataState {
    id: String,
    schema: serde_json::Value,
    partition_columns: Vec<String>,
    configuration: BTreeMap<String, String>,
}

impl MetadataState {
    fn new(
        id: String,
        schema_string: &str,
        partition_columns: Vec<String>,
        configuration: BTreeMap<String, String>,
    ) -> Result<Self> {
        let schema = serde_json::from_str(schema_string).map_err(|error| {
            fail(
                MaintenanceErrorKind::Unsupported,
                format!("metaData.schemaString is not valid JSON: {error}"),
            )
        })?;
        Ok(Self {
            id,
            schema,
            partition_columns,
            configuration,
        })
    }
}

fn json_string_set(value: Option<&serde_json::Value>, what: &str) -> Result<Option<BTreeSet<String>>> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| fail(MaintenanceErrorKind::Unsupported, format!("{what} must hold strings")))
            })
            .collect::<Result<BTreeSet<_>>>()
            .map(Some),
        Some(_) => Err(fail(
            MaintenanceErrorKind::Unsupported,
            format!("{what} must be an array"),
        )),
    }
}

fn json_protocol(protocol: &serde_json::Value) -> Result<ProtocolState> {
    let version = |field: &str| {
        protocol.get(field).and_then(serde_json::Value::as_i64).ok_or_else(|| {
            fail(
                MaintenanceErrorKind::Unsupported,
                format!("protocol.{field} is missing"),
            )
        })
    };
    Ok(ProtocolState {
        min_reader_version: version("minReaderVersion")?,
        min_writer_version: version("minWriterVersion")?,
        reader_features: json_string_set(protocol.get("readerFeatures"), "protocol.readerFeatures")?,
        writer_features: json_string_set(protocol.get("writerFeatures"), "protocol.writerFeatures")?,
    })
}

fn json_metadata(metadata: &serde_json::Value) -> Result<MetadataState> {
    let unsupported = |message: &str| fail(MaintenanceErrorKind::Unsupported, message.to_owned());
    let id = metadata
        .get("id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| unsupported("metaData.id is missing"))?;
    let schema = metadata
        .get("schemaString")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| unsupported("metaData.schemaString is missing"))?;
    let partition_columns = metadata
        .get("partitionColumns")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| unsupported("metaData.partitionColumns is missing"))?
        .iter()
        .map(|column| {
            column
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| unsupported("metaData.partitionColumns must hold strings"))
        })
        .collect::<Result<Vec<_>>>()?;
    let configuration = match metadata.get("configuration") {
        None | Some(serde_json::Value::Null) => BTreeMap::new(),
        Some(serde_json::Value::Object(entries)) => entries
            .iter()
            .map(|(key, value)| {
                value
                    .as_str()
                    .map(|value| (key.clone(), value.to_owned()))
                    .ok_or_else(|| unsupported("metaData.configuration values must be strings"))
            })
            .collect::<Result<BTreeMap<_, _>>>()?,
        Some(_) => return Err(unsupported("metaData.configuration must be an object")),
    };
    MetadataState::new(id.to_owned(), schema, partition_columns, configuration)
}

fn json_add(add: &serde_json::Value) -> Result<AddState> {
    let unsupported = |message: &str| fail(MaintenanceErrorKind::Unsupported, message.to_owned());
    let size = add
        .get("size")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| unsupported("add.size is missing"))?;
    let partition_values = match add.get("partitionValues") {
        None | Some(serde_json::Value::Null) => BTreeMap::new(),
        Some(serde_json::Value::Object(entries)) => entries
            .iter()
            .map(|(key, value)| match value {
                serde_json::Value::Null => Ok((key.clone(), None)),
                serde_json::Value::String(value) => Ok((key.clone(), Some(value.clone()))),
                _ => Err(unsupported("add.partitionValues values must be strings or null")),
            })
            .collect::<Result<BTreeMap<_, _>>>()?,
        Some(_) => return Err(unsupported("add.partitionValues must be an object")),
    };
    Ok(AddState { size, partition_values })
}

/// Facts about one replayed JSON commit used by the log-pruning proof.
struct CommitFacts {
    /// Latest of `commitInfo.timestamp` and `commitInfo.inCommitTimestamp`, if any.
    timestamp: Option<i64>,
    remove_stamps: Vec<Option<i64>>,
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
        self.check_disposable_test_root("VACUUM retention")
    }

    /// Short-retention overrides are confined to marked disposable local roots under the temp directory.
    fn check_disposable_test_root(&self, what: &str) -> Result<()> {
        let unsafe_request = |message: String| Err(fail(MaintenanceErrorKind::UnsafeRequest, message));
        if self.url.scheme() != "file" {
            return unsafe_request(format!("short {what} is only allowed for disposable local test roots"));
        }
        let temp = fs::canonicalize(std::env::temp_dir())?;
        if !self.root.starts_with(&temp) || self.root == temp || !self.root.join(DISPOSABLE_TEST_MARKER).is_file() {
            return unsafe_request(format!(
                "short {what} requires a marked disposable root under the temp directory"
            ));
        }
        Ok(())
    }

    fn log_key(&self, raw: &str) -> Result<String> {
        Ok(self
            .relative_url(raw)
            .map_err(|_| fail(MaintenanceErrorKind::UnsafeRequest, format!("unsafe log path {raw:?}")))?
            .to_string())
    }

    /// Version named by the advisory `_last_checkpoint` hint, if readable.
    /// Strict mode (log pruning) rejects a malformed hint; checkpointing treats it
    /// as absent so a fresh checkpoint can repair it.
    fn hinted_checkpoint_version(&self, strict: bool) -> Result<Option<u64>> {
        let path = self.object_path("_delta_log/_last_checkpoint")?;
        let store = Arc::clone(&self.store);
        let bytes = shared_executor()?.block_on(async move {
            match store.get(&path).await {
                Ok(object) if object.meta.size <= 1024 * 1024 => Ok(Some(object.bytes().await?)),
                Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(error) => Err(anyhow::Error::from(error)),
            }
        })?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        if !strict {
            return Ok(serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .and_then(|hint| hint.get("version").and_then(serde_json::Value::as_u64)));
        }
        let hint: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
            fail(
                MaintenanceErrorKind::Unsupported,
                format!("malformed _last_checkpoint hint: {error}"),
            )
        })?;
        hint.get("version")
            .and_then(serde_json::Value::as_u64)
            .map(Some)
            .ok_or_else(|| {
                fail(
                    MaintenanceErrorKind::Unsupported,
                    "_last_checkpoint hint has no version",
                )
            })
    }

    /// Write a Kernel checkpoint once at least `min_commits` commits follow the
    /// hinted checkpoint, bounding snapshot replay for writers and readers.
    ///
    /// Checkpoints retain every application transaction (no txn retention) and
    /// the table metadata including the purge watermark. Checkpointing never
    /// deletes JSON history; only `prune_log` does, after proving the checkpoint
    /// equivalent to it. Without `apply` a due checkpoint is only reported as planned.
    ///
    /// # Errors
    /// Rejects `min_commits == 0`; returns authority, replay or write failures.
    pub fn checkpoint_if_due(&self, min_commits: u64, apply: bool) -> Result<MaintenanceReport> {
        if min_commits == 0 {
            return Err(fail(
                MaintenanceErrorKind::InvalidPlan,
                "checkpoint interval must be nonzero",
            ));
        }
        let version = self.version()?;
        let mut report = MaintenanceReport {
            operation: "checkpoint".to_owned(),
            read_version: version,
            version,
            ..MaintenanceReport::default()
        };
        let due = match self.hinted_checkpoint_version(false)? {
            Some(hinted) => version.saturating_sub(hinted) >= min_commits,
            None => version.saturating_add(1) >= min_commits,
        };
        report.outcome = match (due, apply) {
            (false, _) => Outcome::Noop,
            (true, false) => Outcome::Planned,
            (true, true) => {
                report.version = self.checkpoint()?;
                Outcome::Checkpointed
            }
        };
        Ok(report)
    }

    /// List the direct children of `_delta_log/` with their metadata, bounded.
    fn list_log(&self, limit: usize) -> Result<BTreeMap<String, ObjectMeta>> {
        let prefix = self.object_path("_delta_log/")?;
        let store = Arc::clone(&self.store);
        shared_executor()?.block_on(async move {
            use futures::StreamExt;
            let mut names = BTreeMap::new();
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
                    names.insert(name.to_owned(), entry.clone());
                }
            }
            Ok::<_, anyhow::Error>(names)
        })
    }

    /// Read one bounded JSON commit; a missing commit is an unexplained history gap.
    fn read_commit(&self, version: u64, limit: u64) -> Result<Vec<u8>> {
        let name = format!("{version:020}.json");
        let path = self.object_path(&format!("_delta_log/{name}"))?;
        let store = Arc::clone(&self.store);
        shared_executor()?.block_on(async move {
            let object = match store.get(&path).await {
                Ok(object) => object,
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(fail(
                        MaintenanceErrorKind::Unsupported,
                        format!("log history gap: {name} is missing"),
                    ));
                }
                Err(error) => return Err(error.into()),
            };
            if object.meta.size > limit {
                return Err(fail(
                    MaintenanceErrorKind::ResourceLimit,
                    "log file exceeds its read bound",
                ));
            }
            Ok(object.bytes().await?.to_vec())
        })
    }

    /// Apply one JSON commit to a hand-written replay; never consults Kernel.
    fn replay_commit(&self, replay: &mut LogReplay, bytes: &[u8]) -> Result<CommitFacts> {
        let unsupported = |message: &str| fail(MaintenanceErrorKind::Unsupported, message.to_owned());
        let mut facts = CommitFacts {
            timestamp: None,
            remove_stamps: Vec::new(),
        };
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
                    return Err(unsupported("deletion vectors are unsupported by VACUUM LITE"));
                }
                let key = self.log_key(raw)?;
                replay.tombstones.remove(&key);
                replay.adds.insert(key.clone(), json_add(add)?);
                replay.active.insert(key);
            } else if let Some(remove) = action.get("remove") {
                let raw = remove
                    .get("path")
                    .and_then(serde_json::Value::as_str)
                    .context("remove without path")?;
                if remove.get("deletionVector").is_some_and(|vector| !vector.is_null()) {
                    return Err(unsupported("deletion vectors are unsupported by VACUUM LITE"));
                }
                let key = self.log_key(raw)?;
                replay.active.remove(&key);
                replay.adds.remove(&key);
                let stamp = remove.get("deletionTimestamp").and_then(serde_json::Value::as_i64);
                facts.remove_stamps.push(stamp);
                replay.tombstones.insert(key, (raw.to_owned(), stamp));
            } else if let Some(txn) = action.get("txn") {
                let app_id = txn
                    .get("appId")
                    .and_then(serde_json::Value::as_str)
                    .context("txn without appId")?;
                let version = txn
                    .get("version")
                    .and_then(serde_json::Value::as_i64)
                    .context("txn without version")?;
                let latest = replay.txns.entry(app_id.to_owned()).or_insert(version);
                if version < *latest {
                    replay.txn_regressed = true;
                }
                *latest = version;
            } else if let Some(metadata) = action.get("metaData") {
                let metadata = json_metadata(metadata)?;
                replay.table_id = Some(metadata.id.clone());
                replay.metadata = Some(metadata);
            } else if let Some(protocol) = action.get("protocol") {
                replay.protocol = Some(json_protocol(protocol)?);
            } else if let Some(info) = action.get("commitInfo") {
                for field in ["timestamp", "inCommitTimestamp"] {
                    if let Some(stamp) = info.get(field).and_then(serde_json::Value::as_i64) {
                        facts.timestamp = Some(facts.timestamp.map_or(stamp, |current| current.max(stamp)));
                    }
                }
            }
        }
        Ok(facts)
    }

    /// Direct Parquet read of the classic checkpoint at `version`, independent of Kernel.
    fn read_checkpoint(&self, version: u64, limit: u64) -> Result<LogReplay> {
        let unsupported = |message: String| fail(MaintenanceErrorKind::Unsupported, message);
        let name = format!("{version:020}.checkpoint.parquet");
        let path = self.object_path(&format!("_delta_log/{name}"))?;
        let store = Arc::clone(&self.store);
        let missing = name.clone();
        let rows = shared_executor()?.block_on(async move {
            let object = match store.get(&path).await {
                Ok(object) => object,
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(fail(
                        MaintenanceErrorKind::Unsupported,
                        format!("checkpoint {missing} is missing"),
                    ));
                }
                Err(error) => return Err(error.into()),
            };
            if object.meta.size > limit {
                return Err(fail(
                    MaintenanceErrorKind::ResourceLimit,
                    "checkpoint exceeds its read bound",
                ));
            }
            checkpoint_rows(object.bytes().await?)
        })?;
        let mut replay = LogReplay::default();
        for (raw, add) in rows.adds {
            let key = self.log_key(&raw)?;
            if !replay.active.insert(key.clone()) || replay.adds.insert(key, add).is_some() {
                return Err(unsupported(format!("{name} adds {raw} twice")));
            }
        }
        for (raw, stamp) in rows.removes {
            let key = self.log_key(&raw)?;
            if replay.active.contains(&key) || replay.tombstones.insert(key, (raw.clone(), stamp)).is_some() {
                return Err(unsupported(format!("{name} has conflicting actions for {raw}")));
            }
        }
        for (app_id, txn_version) in rows.txns {
            if replay.txns.insert(app_id.clone(), txn_version).is_some() {
                return Err(unsupported(format!("{name} repeats txn {app_id}")));
            }
        }
        let (Ok([protocol]), Ok([metadata])) = (
            <[ProtocolState; 1]>::try_from(rows.protocols),
            <[MetadataState; 1]>::try_from(rows.metadata),
        ) else {
            return Err(unsupported(format!(
                "{name} must carry exactly one protocol and one metaData action"
            )));
        };
        replay.table_id = Some(metadata.id.clone());
        replay.protocol = Some(protocol);
        replay.metadata = Some(metadata);
        Ok(replay)
    }

    /// Replay JSON history for VACUUM LITE without listing data directories.
    ///
    /// Complete history `0..=version` is replayed directly. After log pruning,
    /// replay instead starts from the durable floor checkpoint `F` (read
    /// directly from Parquet) followed by contiguous JSON `F+1..=version`, and
    /// only while a progress record proves every tombstone that checkpoint may
    /// have dropped was already vacuumed. Any other gap fails with `Unsupported`.
    fn replay_log(&self, version: u64, table_id: &str, config: &VacuumConfig) -> Result<LogReplay> {
        let names = self.list_log(config.max_log_files.saturating_mul(4))?;
        let json = |current: u64| format!("{current:020}.json");
        let gap = (0..=version).find(|current| !names.contains_key(&json(*current)));
        let (mut replay, first) = match gap {
            None => {
                if usize::try_from(version).unwrap_or(usize::MAX) >= config.max_log_files {
                    return Err(fail(
                        MaintenanceErrorKind::ResourceLimit,
                        "log history exceeds the replay bound",
                    ));
                }
                (LogReplay::default(), 0)
            }
            Some(gap) => {
                let floor = self.pruned_history_floor(table_id, version, &names).map_err(|error| {
                    fail(
                        MaintenanceErrorKind::Unsupported,
                        format!(
                            "VACUUM LITE requires complete JSON log history; {} is missing and no proven log floor explains it: {error:#}",
                            json(gap)
                        ),
                    )
                })?;
                if usize::try_from(version - floor).unwrap_or(usize::MAX) >= config.max_log_files {
                    return Err(fail(
                        MaintenanceErrorKind::ResourceLimit,
                        "log history exceeds the replay bound",
                    ));
                }
                let seeded = self.read_checkpoint(floor, config.max_checkpoint_bytes)?;
                if seeded.table_id.as_deref() != Some(table_id) {
                    return Err(fail(
                        MaintenanceErrorKind::Unsupported,
                        "floor checkpoint belongs to a different table id",
                    ));
                }
                (seeded, floor + 1)
            }
        };
        for current in first..=version {
            if !names.contains_key(&json(current)) {
                return Err(fail(
                    MaintenanceErrorKind::Unsupported,
                    format!(
                        "VACUUM LITE requires contiguous JSON log history above the floor; {} is missing",
                        json(current)
                    ),
                ));
            }
            let bytes = self.read_commit(current, config.max_log_file_bytes)?;
            self.replay_commit(&mut replay, &bytes)?;
        }
        Ok(replay)
    }

    /// The durable floor `F` that explains missing JSON at or below it.
    fn pruned_history_floor(&self, table_id: &str, version: u64, names: &BTreeMap<String, ObjectMeta>) -> Result<u64> {
        let floor = self.log_floor()?.context("no log floor record")?;
        if floor.table_id != table_id {
            anyhow::bail!("log floor record belongs to a different table id");
        }
        let checkpoint = floor.checkpoint_version;
        if checkpoint > version {
            anyhow::bail!("log floor {checkpoint} is above the replayed version {version}");
        }
        if !names.contains_key(&format!("{checkpoint:020}.checkpoint.parquet")) {
            anyhow::bail!("floor checkpoint {checkpoint} is missing");
        }
        let progress = self.vacuum_lite_progress()?.context("no VACUUM LITE progress record")?;
        if progress.table_id != table_id || progress.version < checkpoint {
            anyhow::bail!("VACUUM LITE progress does not cover the log floor");
        }
        Ok(checkpoint)
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
        let replay = self.replay_log(state.snapshot.version(), &state.fingerprint.table_id, config)?;
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
            self.record_vacuum_progress(plan, effective, config)
                .map_err(|error| progress_failure(&error, &report))?;
            return Ok(report);
        }
        let replay = self.replay_log(state.snapshot.version(), &state.fingerprint.table_id, config)?;
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
        self.sync_vacuumed_parents(&plan.candidates).map_err(|error| {
            fail_with(
                MaintenanceErrorKind::Uncertain,
                format!("deleted files' directories are not durable; progress not recorded: {error:#}"),
                &report,
            )
        })?;
        self.record_vacuum_progress(plan, effective, config)
            .map_err(|error| progress_failure(&error, &report))?;
        Ok(report)
    }

    /// On local roots, fsync the parent directory of every candidate (deleted or
    /// already absent; the nearest existing ancestor when it is gone) up to the
    /// table root, so unlinks are durable before progress lets pruning drop the
    /// tombstones that describe them. Repeats re-fsync, repairing earlier failures.
    fn sync_vacuumed_parents(&self, candidates: &[VacuumCandidate]) -> Result<()> {
        if self.url.scheme() != "file" {
            return Ok(());
        }
        let mut leaves = BTreeSet::new();
        for candidate in candidates {
            self.safe_object_path(&candidate.path)?;
            let local = self.root.join(percent_decode(&candidate.path)?);
            let mut directory = local.parent().context("data file without a parent directory")?;
            while !directory.is_dir() && directory != self.root {
                directory = directory.parent().context("data file outside the table root")?;
            }
            leaves.insert(directory.to_path_buf());
        }
        for leaf in &leaves {
            sync_directory_chain(leaf, &self.root)?;
        }
        Ok(())
    }

    /// Record durable VACUUM LITE progress after a complete, uninterrupted apply.
    ///
    /// The version is the plan's read version (the history actually scanned),
    /// and the claim is re-proven by replaying that version: every tombstone
    /// older than `cutoff_ms` must be one of the plan's (now deleted or absent)
    /// candidates. Plans with a backlog never record progress.
    fn record_vacuum_progress(&self, plan: &VacuumPlan, cutoff_ms: i64, config: &VacuumConfig) -> Result<()> {
        if plan.backlog_files != 0 {
            return Ok(());
        }
        let table_id = &plan.header.fingerprint.table_id;
        let replay = self.replay_log(plan.header.read_version, table_id, config)?;
        let planned = plan
            .candidates
            .iter()
            .map(|candidate| self.log_key(&candidate.path))
            .collect::<Result<BTreeSet<_>>>()?;
        if let Some((key, _)) = replay
            .tombstones
            .iter()
            .find(|(key, (_, stamp))| stamp.is_some_and(|stamp| stamp < cutoff_ms) && !planned.contains(*key))
        {
            return Err(fail(
                MaintenanceErrorKind::StalePlan,
                format!("expired tombstone {key} is not covered by the plan; progress not recorded"),
            ));
        }
        let record = VacuumLiteProgress {
            table_id: table_id.clone(),
            version: plan.header.read_version,
            cutoff_ms,
        };
        self.update_record(VACUUM_PROGRESS_RECORD, |current| merge_progress(current, &record))
    }
}

fn progress_failure(error: &anyhow::Error, report: &MaintenanceReport) -> anyhow::Error {
    fail_with(
        error_kind(error).unwrap_or(MaintenanceErrorKind::Uncertain),
        format!("deletes finished but the VACUUM LITE progress record was not written: {error:#}"),
        report,
    )
}

/// Table-root directory for maintenance records; never inside `_delta_log`.
pub const MAINTENANCE_DIR: &str = "_arrowcol_maintenance";
/// VACUUM LITE progress record under [`MAINTENANCE_DIR`].
pub const VACUUM_PROGRESS_RECORD: &str = "vacuum-lite.json";
/// Durable log floor record under [`MAINTENANCE_DIR`].
pub const LOG_FLOOR_RECORD: &str = "log-floor.json";
/// Default `delta.logRetentionDuration` (30 days).
pub const DEFAULT_LOG_RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1000;
/// Default read bound for one checkpoint file.
pub const DEFAULT_MAX_CHECKPOINT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_RECORD_BYTES: u64 = 64 * 1024;
const MAX_RECORD_ATTEMPTS: u32 = 8;

/// Durable VACUUM LITE progress: every tombstone in the replayed history
/// `0..=version` with `deletionTimestamp < cutoff_ms` was physically deleted
/// or confirmed absent. Monotonic: a record only replaces one it dominates.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VacuumLiteProgress {
    pub table_id: String,
    pub version: u64,
    pub cutoff_ms: i64,
}

/// Durable log floor written before any log deletion: history at or below
/// `checkpoint_version` may be missing and is represented by that checkpoint.
/// Monotonic: never lowered.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogFloor {
    pub table_id: String,
    pub checkpoint_version: u64,
}

fn merge_progress(
    current: Option<&VacuumLiteProgress>,
    next: &VacuumLiteProgress,
) -> Result<Option<VacuumLiteProgress>> {
    match current {
        None => Ok(Some(next.clone())),
        Some(current) if current.table_id != next.table_id => Err(fail(
            MaintenanceErrorKind::MetadataChanged,
            "VACUUM LITE progress record belongs to a different table id",
        )),
        Some(current) if next.version >= current.version && next.cutoff_ms >= current.cutoff_ms && next != current => {
            Ok(Some(next.clone()))
        }
        // Equal, older or incomparable claims keep the existing record.
        Some(_) => Ok(None),
    }
}

fn merge_floor(current: Option<&LogFloor>, next: &LogFloor) -> Result<Option<LogFloor>> {
    match current {
        None => Ok(Some(next.clone())),
        Some(current) if current.table_id != next.table_id => Err(fail(
            MaintenanceErrorKind::MetadataChanged,
            "log floor record belongs to a different table id",
        )),
        Some(current) if next.checkpoint_version > current.checkpoint_version => Ok(Some(next.clone())),
        Some(_) => Ok(None),
    }
}

async fn fetch_record(
    store: Arc<dyn ObjectStore>,
    path: object_store::path::Path,
) -> Result<Option<(Vec<u8>, UpdateVersion)>> {
    match store.get(&path).await {
        Ok(object) => {
            if object.meta.size > MAX_RECORD_BYTES {
                return Err(fail(
                    MaintenanceErrorKind::ResourceLimit,
                    "maintenance record exceeds its read bound",
                ));
            }
            let version = UpdateVersion {
                e_tag: object.meta.e_tag.clone(),
                version: object.meta.version.clone(),
            };
            Ok(Some((object.bytes().await?.to_vec(), version)))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Rows of a classic checkpoint relevant to the pruning proof.
#[derive(Default)]
struct CheckpointRows {
    adds: Vec<(String, AddState)>,
    removes: Vec<(String, Option<i64>)>,
    txns: Vec<(String, i64)>,
    protocols: Vec<ProtocolState>,
    metadata: Vec<MetadataState>,
}

fn checkpoint_shape(what: &str) -> anyhow::Error {
    fail(
        MaintenanceErrorKind::Unsupported,
        format!("checkpoint {what} has an unexpected shape"),
    )
}

/// A `map<string, string>` child at `row`; `None` when absent or null.
fn child_map(
    parent: &arrow::array::StructArray,
    field: &str,
    row: usize,
) -> Result<Option<BTreeMap<String, Option<String>>>> {
    let Some(column) = parent.column_by_name(field).filter(|column| column.is_valid(row)) else {
        return Ok(None);
    };
    let entries = column.as_map_opt().ok_or_else(|| checkpoint_shape(field))?.value(row);
    if entries.num_columns() != 2 {
        return Err(checkpoint_shape(field));
    }
    let keys = arrow::compute::cast(entries.column(0), &DataType::Utf8)?;
    let keys = keys.as_string::<i32>();
    let values = arrow::compute::cast(entries.column(1), &DataType::Utf8)?;
    let values = values.as_string::<i32>();
    let mut map = BTreeMap::new();
    for entry in 0..entries.len() {
        if keys.is_null(entry) {
            return Err(checkpoint_shape(field));
        }
        let value = values.is_valid(entry).then(|| values.value(entry).to_owned());
        if map.insert(keys.value(entry).to_owned(), value).is_some() {
            return Err(checkpoint_shape(field));
        }
    }
    Ok(Some(map))
}

/// A `list<string>` child at `row`; `None` when absent or null.
fn child_string_list(parent: &arrow::array::StructArray, field: &str, row: usize) -> Result<Option<Vec<String>>> {
    let Some(column) = parent.column_by_name(field).filter(|column| column.is_valid(row)) else {
        return Ok(None);
    };
    let items = column
        .as_list_opt::<i32>()
        .ok_or_else(|| checkpoint_shape(field))?
        .value(row);
    let items = arrow::compute::cast(&items, &DataType::Utf8)?;
    let items = items.as_string::<i32>();
    (0..items.len())
        .map(|item| {
            items
                .is_valid(item)
                .then(|| items.value(item).to_owned())
                .ok_or_else(|| checkpoint_shape(field))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn checkpoint_protocol(protocol: &arrow::array::StructArray, row: usize) -> Result<ProtocolState> {
    let version = |field: &str| -> Result<i64> {
        child_i64(protocol, field)?
            .filter(|values| values.is_valid(row))
            .map(|values| values.value(row))
            .ok_or_else(|| checkpoint_shape(field))
    };
    let features = |field: &str| -> Result<Option<BTreeSet<String>>> {
        Ok(child_string_list(protocol, field, row)?.map(|features| features.into_iter().collect()))
    };
    Ok(ProtocolState {
        min_reader_version: version("minReaderVersion")?,
        min_writer_version: version("minWriterVersion")?,
        reader_features: features("readerFeatures")?,
        writer_features: features("writerFeatures")?,
    })
}

fn checkpoint_metadata(metadata: &arrow::array::StructArray, row: usize) -> Result<MetadataState> {
    let id = required_string(child_strings(metadata, "id")?.as_ref(), row, "metaData.id")?;
    let schema = required_string(
        child_strings(metadata, "schemaString")?.as_ref(),
        row,
        "metaData.schemaString",
    )?;
    let partition_columns =
        child_string_list(metadata, "partitionColumns", row)?.ok_or_else(|| checkpoint_shape("partitionColumns"))?;
    let configuration = child_map(metadata, "configuration", row)?
        .unwrap_or_default()
        .into_iter()
        .map(|(key, value)| {
            value
                .map(|value| (key, value))
                .ok_or_else(|| checkpoint_shape("configuration"))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    MetadataState::new(id, &schema, partition_columns, configuration)
}

fn struct_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<Option<&'a arrow::array::StructArray>> {
    batch
        .column_by_name(name)
        .map(|column| {
            column.as_struct_opt().ok_or_else(|| {
                fail(
                    MaintenanceErrorKind::Unsupported,
                    format!("checkpoint column {name} is not a struct"),
                )
            })
        })
        .transpose()
}

fn child_strings(parent: &arrow::array::StructArray, field: &str) -> Result<Option<arrow::array::StringArray>> {
    parent
        .column_by_name(field)
        .map(|column| {
            Ok(arrow::compute::cast(column, &DataType::Utf8)?
                .as_string::<i32>()
                .clone())
        })
        .transpose()
}

fn child_i64(parent: &arrow::array::StructArray, field: &str) -> Result<Option<arrow::array::Int64Array>> {
    parent
        .column_by_name(field)
        .map(|column| {
            Ok(arrow::compute::cast(column, &DataType::Int64)?
                .as_primitive::<Int64Type>()
                .clone())
        })
        .transpose()
}

fn required_string(values: Option<&arrow::array::StringArray>, row: usize, what: &str) -> Result<String> {
    values
        .filter(|values| values.is_valid(row))
        .map(|values| values.value(row).to_owned())
        .ok_or_else(|| {
            fail(
                MaintenanceErrorKind::Unsupported,
                format!("checkpoint {what} is missing"),
            )
        })
}

fn child_valid(parent: &arrow::array::StructArray, field: &str, row: usize) -> bool {
    parent.column_by_name(field).is_some_and(|column| column.is_valid(row))
}

/// Parse a classic checkpoint with the Parquet reader only (no Kernel replay).
fn checkpoint_rows<R: parquet::file::reader::ChunkReader + 'static>(reader: R) -> Result<CheckpointRows> {
    let unsupported = |message: &str| fail(MaintenanceErrorKind::Unsupported, message.to_owned());
    let mut rows = CheckpointRows::default();
    for batch in ParquetRecordBatchReaderBuilder::try_new(reader)?.build()? {
        let batch = batch?;
        for forbidden in ["sidecar", "checkpointMetadata"] {
            if batch
                .column_by_name(forbidden)
                .is_some_and(|column| column.null_count() < column.len())
            {
                return Err(unsupported(
                    "V2 checkpoints with sidecars or checkpoint metadata are unsupported",
                ));
            }
        }
        if let Some(add) = struct_column(&batch, "add")? {
            let paths = child_strings(add, "path")?;
            let sizes = child_i64(add, "size")?;
            for row in 0..add.len() {
                if add.is_valid(row) {
                    if child_valid(add, "deletionVector", row) {
                        return Err(unsupported("deletion vectors are unsupported"));
                    }
                    let size = sizes
                        .as_ref()
                        .filter(|sizes| sizes.is_valid(row))
                        .map(|sizes| sizes.value(row))
                        .ok_or_else(|| unsupported("checkpoint add.size is missing"))?;
                    rows.adds.push((
                        required_string(paths.as_ref(), row, "add.path")?,
                        AddState {
                            size,
                            partition_values: child_map(add, "partitionValues", row)?.unwrap_or_default(),
                        },
                    ));
                }
            }
        }
        if let Some(remove) = struct_column(&batch, "remove")? {
            let paths = child_strings(remove, "path")?;
            let stamps = child_i64(remove, "deletionTimestamp")?;
            for row in 0..remove.len() {
                if remove.is_valid(row) {
                    if child_valid(remove, "deletionVector", row) {
                        return Err(unsupported("deletion vectors are unsupported"));
                    }
                    let stamp = stamps
                        .as_ref()
                        .and_then(|stamps| stamps.is_valid(row).then(|| stamps.value(row)));
                    rows.removes
                        .push((required_string(paths.as_ref(), row, "remove.path")?, stamp));
                }
            }
        }
        if let Some(txn) = struct_column(&batch, "txn")? {
            let app_ids = child_strings(txn, "appId")?;
            let versions = child_i64(txn, "version")?;
            for row in 0..txn.len() {
                if txn.is_valid(row) {
                    let version = versions
                        .as_ref()
                        .filter(|versions| versions.is_valid(row))
                        .map(|versions| versions.value(row))
                        .ok_or_else(|| unsupported("checkpoint txn.version is missing"))?;
                    rows.txns
                        .push((required_string(app_ids.as_ref(), row, "txn.appId")?, version));
                }
            }
        }
        if let Some(metadata) = struct_column(&batch, "metaData")? {
            for row in 0..metadata.len() {
                if metadata.is_valid(row) {
                    rows.metadata.push(checkpoint_metadata(metadata, row)?);
                }
            }
        }
        if let Some(protocol) = struct_column(&batch, "protocol")? {
            for row in 0..protocol.len() {
                if protocol.is_valid(row) {
                    rows.protocols.push(checkpoint_protocol(protocol, row)?);
                }
            }
        }
    }
    Ok(rows)
}

/// First difference between an independent JSON replay and a checkpoint view.
fn proof_mismatch(
    source: &str,
    expected_active: &BTreeSet<String>,
    expected_txns: &BTreeMap<String, i64>,
    actual_active: &BTreeSet<String>,
    actual_txns: &BTreeMap<String, i64>,
) -> Option<String> {
    if let Some(path) = expected_active.symmetric_difference(actual_active).next() {
        return Some(format!(
            "{source} active file set differs from the JSON replay at {path}"
        ));
    }
    for (app_id, version) in expected_txns {
        if actual_txns.get(app_id) != Some(version) {
            return Some(format!(
                "{source} txn {app_id} is {:?}; the JSON replay has {version}",
                actual_txns.get(app_id)
            ));
        }
    }
    actual_txns
        .keys()
        .find(|app_id| !expected_txns.contains_key(*app_id))
        .map(|app_id| format!("{source} has txn {app_id} absent from the JSON replay"))
}

/// First difference in recovery-relevant state (per-add partition values and
/// size, protocol and metadata) between an independent JSON replay and a
/// checkpoint view. Statistics are not compared.
fn state_mismatch(
    source: &str,
    expected: &LogReplay,
    adds: &BTreeMap<String, AddState>,
    protocol: Option<&ProtocolState>,
    metadata: Option<&MetadataState>,
) -> Option<String> {
    if let Some(path) = expected
        .adds
        .keys()
        .chain(adds.keys())
        .find(|path| expected.adds.get(*path) != adds.get(*path))
    {
        return Some(format!(
            "{source} add {path} differs from the JSON replay: {:?} vs {:?}",
            adds.get(path),
            expected.adds.get(path)
        ));
    }
    if expected.protocol.is_none() || expected.protocol.as_ref() != protocol {
        return Some(format!(
            "{source} protocol {protocol:?} differs from the JSON replay {:?}",
            expected.protocol
        ));
    }
    match (expected.metadata.as_ref(), metadata) {
        (Some(expected), Some(actual)) if expected == actual => None,
        (Some(expected), Some(actual)) => {
            let field = if expected.id != actual.id {
                "id"
            } else if expected.schema != actual.schema {
                "schemaString"
            } else if expected.partition_columns != actual.partition_columns {
                "partitionColumns"
            } else {
                "configuration"
            };
            Some(format!("{source} metaData.{field} differs from the JSON replay"))
        }
        _ => Some(format!("{source} lacks metaData or the JSON replay does")),
    }
}

/// Parse a Delta interval such as `interval 30 days`; `None` when unrecognized.
fn parse_interval_ms(value: &str) -> Option<i64> {
    let mut parts = value.split_whitespace();
    let first = parts.next()?;
    let (count, unit) = if first.eq_ignore_ascii_case("interval") {
        (parts.next()?, parts.next()?)
    } else {
        (first, parts.next()?)
    };
    if parts.next().is_some() {
        return None;
    }
    let count: i64 = count.parse().ok().filter(|count| *count >= 0)?;
    let unit = unit.to_ascii_lowercase();
    let unit_ms = match unit.strip_suffix('s').unwrap_or(&unit) {
        "millisecond" => 1,
        "second" => 1_000,
        "minute" => 60_000,
        "hour" => 3_600_000,
        "day" => 86_400_000,
        "week" => 604_800_000,
        _ => return None,
    };
    count.checked_mul(unit_ms)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum LogObject {
    Json(u64),
    Checkpoint(u64),
    Crc(u64),
    Other(u64),
}

/// Classify a `_delta_log/` child name; `None` for unversioned names such as `_last_checkpoint`.
fn classify_log_name(name: &str) -> Option<LogObject> {
    let (digits, rest) = name.split_at_checked(20)?;
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let version = digits.parse().ok()?;
    Some(match rest.strip_prefix('.')? {
        "json" => LogObject::Json(version),
        "checkpoint.parquet" => LogObject::Checkpoint(version),
        "crc" => LogObject::Crc(version),
        _ => LogObject::Other(version),
    })
}

/// Validate one planned deletion against checkpoint `C`; returns its sort key.
fn deletion_key(name: &str, checkpoint: u64) -> Result<(u64, u8)> {
    let invalid = || {
        fail(
            MaintenanceErrorKind::InvalidPlan,
            format!("{name} is not prunable below checkpoint {checkpoint}"),
        )
    };
    match name.strip_prefix("_delta_log/").and_then(classify_log_name) {
        Some(LogObject::Json(version)) if version < checkpoint => Ok((version, 0)),
        Some(LogObject::Checkpoint(version)) if version < checkpoint => Ok((version, 1)),
        _ => Err(invalid()),
    }
}

/// `_delta_log` pruning bounds. Log retention defaults to the table's
/// `delta.logRetentionDuration` (30 days when unset); a shorter override needs
/// the unsafe test override plus a marked disposable local temp root.
#[derive(Clone, Debug)]
pub struct PruneConfig {
    pub log_retention_ms: Option<i64>,
    /// Explicit wall-clock UTC anchor for the commit-age cutoff.
    pub now_ms: i64,
    pub unsafe_allow_short_retention_for_tests: bool,
    /// Maximum log objects deleted by one apply; the rest is reported as backlog.
    pub max_prune_files: usize,
    pub max_log_files: usize,
    pub max_log_file_bytes: u64,
    pub max_checkpoint_bytes: u64,
    pub max_active_files: usize,
}

impl PruneConfig {
    /// Defaults for everything except the anchor.
    #[must_use]
    pub const fn new(now_ms: i64) -> Self {
        Self {
            log_retention_ms: None,
            now_ms,
            unsafe_allow_short_retention_for_tests: false,
            max_prune_files: 10_000,
            max_log_files: 100_000,
            max_log_file_bytes: 64 * 1024 * 1024,
            max_checkpoint_bytes: DEFAULT_MAX_CHECKPOINT_BYTES,
            max_active_files: 1_000_000,
        }
    }
}

/// Serializable `_delta_log` pruning dry-run plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrunePlan {
    pub header: PlanHeader,
    pub log_retention_ms: i64,
    /// Explicit UTC anchor; `log_cutoff_ms = now_ms - log_retention_ms`.
    pub now_ms: i64,
    /// Every pruned commit is older than this.
    pub log_cutoff_ms: i64,
    /// Newest proven checkpoint `C`; `None` when nothing is eligible.
    pub checkpoint_version: Option<u64>,
    /// The VACUUM LITE progress record the proof relied on.
    pub progress: Option<VacuumLiteProgress>,
    /// Log objects to delete in ascending version order: `NNN.json` for
    /// `0..C` (`C.json` is retained, as in Delta log cleanup) and classic checkpoints below `C`.
    pub deletions: Vec<String>,
    pub backlog_files: u64,
}

impl PrunePlan {
    /// Dry-run report: `Planned` with deletions, otherwise `Noop`.
    #[must_use]
    pub fn report(&self) -> MaintenanceReport {
        let mut report = MaintenanceReport::new("prune_log", &self.header);
        report.outcome = if self.deletions.is_empty() {
            Outcome::Noop
        } else {
            Outcome::Planned
        };
        report.version = self.checkpoint_version.unwrap_or(self.header.read_version);
        report.cutoff_ms = Some(self.log_cutoff_ms);
        report.files_removed = u64::try_from(self.deletions.len()).unwrap_or(u64::MAX);
        report.backlog_files = self.backlog_files;
        report
    }
}

/// Kernel's snapshot view used to cross-check a checkpoint.
struct KernelView {
    protocol: ProtocolState,
    metadata: MetadataState,
    active: BTreeSet<String>,
    adds: BTreeMap<String, AddState>,
    txns: BTreeMap<String, i64>,
}

/// A checkpoint `C` proven equivalent to the history it replaces.
struct PruneProof {
    checkpoint_version: u64,
    deletions: Vec<String>,
    backlog: u64,
}

impl DeltaTable {
    /// The durable VACUUM LITE progress record, if any.
    ///
    /// # Errors
    /// Returns storage or malformed-record errors.
    pub fn vacuum_lite_progress(&self) -> Result<Option<VacuumLiteProgress>> {
        self.read_record(VACUUM_PROGRESS_RECORD)
    }

    /// The durable log floor record, if any.
    ///
    /// # Errors
    /// Returns storage or malformed-record errors.
    pub fn log_floor(&self) -> Result<Option<LogFloor>> {
        self.read_record(LOG_FLOOR_RECORD)
    }

    fn read_record<T: serde::de::DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        let path = self.object_path(&format!("{MAINTENANCE_DIR}/{name}"))?;
        let fetched = shared_executor()?.block_on(fetch_record(Arc::clone(&self.store), path))?;
        fetched
            .map(|(bytes, _)| {
                serde_json::from_slice(&bytes).map_err(|error| {
                    fail(
                        MaintenanceErrorKind::Unsupported,
                        format!("malformed maintenance record {name}: {error}"),
                    )
                })
            })
            .transpose()
    }

    /// Durably replace a maintenance record with `merge(current)` when it returns
    /// `Some`. Local roots serialize writers with an exclusive lock, publish by
    /// fsynced rename, and fsync the visible record, its directory and the root
    /// on every return (no-op merges included); object stores use
    /// create-if-absent or etag-conditional puts.
    fn update_record<T>(&self, name: &str, merge: impl Fn(Option<&T>) -> Result<Option<T>>) -> Result<()>
    where
        T: Serialize + serde::de::DeserializeOwned,
    {
        let parse = |bytes: &[u8]| {
            serde_json::from_slice::<T>(bytes).map_err(|error| {
                fail(
                    MaintenanceErrorKind::Unsupported,
                    format!("malformed maintenance record {name}: {error}"),
                )
            })
        };
        if self.url.scheme() == "file" {
            let directory = self.root.join(MAINTENANCE_DIR);
            // Always repair durability of the directory entry: an earlier create
            // may have become visible while its root fsync failed.
            fs::create_dir_all(&directory)?;
            sync_directory(&self.root)?;
            let lock = fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(directory.join(".lock"))?;
            fs2::FileExt::lock_exclusive(&lock)?;
            let target = directory.join(name);
            let current = match fs::read(&target) {
                Ok(bytes) => Some(parse(&bytes)?),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            if let Some(next) = merge(current.as_ref())? {
                let staged = directory.join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4()));
                fs::write(&staged, serde_json::to_vec(&next)?)?;
                sync_file(&staged)?;
                fs::rename(&staged, &target)?;
            }
            // Every path, including a no-op merge, makes the visible record durable
            // before returning: a prior rename may be visible while its fsyncs failed.
            match fs::metadata(&target) {
                Ok(_) => sync_file(&target)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            sync_directory(&directory)?;
            sync_directory(&self.root)?;
            fs2::FileExt::unlock(&lock)?;
            return Ok(());
        }
        let path = self.object_path(&format!("{MAINTENANCE_DIR}/{name}"))?;
        for _ in 0..MAX_RECORD_ATTEMPTS {
            let fetched = shared_executor()?.block_on(fetch_record(Arc::clone(&self.store), path.clone()))?;
            let (current, mode) = match fetched {
                Some((bytes, version)) => (Some(parse(&bytes)?), PutMode::Update(version)),
                None => (None, PutMode::Create),
            };
            let Some(next) = merge(current.as_ref())? else {
                return Ok(());
            };
            let payload = serde_json::to_vec(&next)?;
            let store = Arc::clone(&self.store);
            let target = path.clone();
            let result = shared_executor()?
                .block_on(async move { store.put_opts(&target, payload.into(), PutOptions::from(mode)).await });
            match result {
                Ok(_) => return Ok(()),
                Err(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(fail(
            MaintenanceErrorKind::Exhausted,
            format!("maintenance record {name} kept changing concurrently"),
        ))
    }

    /// Effective log retention: the table property (default 30 days) unless a
    /// configured override applies; shorter overrides need the unsafe test gate.
    fn log_retention(&self, config: &PruneConfig, fingerprint: &TableFingerprint) -> Result<i64> {
        let table = match fingerprint.configuration.get("delta.logRetentionDuration") {
            Some(value) => parse_interval_ms(value).ok_or_else(|| {
                fail(
                    MaintenanceErrorKind::Unsupported,
                    format!("unrecognized delta.logRetentionDuration {value:?}"),
                )
            })?,
            None => DEFAULT_LOG_RETENTION_MS,
        };
        let Some(requested) = config.log_retention_ms else {
            return Ok(table);
        };
        if requested < 0 {
            return Err(fail(MaintenanceErrorKind::UnsafeRequest, "negative log retention"));
        }
        if requested < table {
            if !config.unsafe_allow_short_retention_for_tests {
                return Err(fail(
                    MaintenanceErrorKind::UnsafeRequest,
                    "log retention below delta.logRetentionDuration requires the unsafe test override",
                ));
            }
            self.check_disposable_test_root("log retention")?;
        }
        Ok(requested)
    }

    /// Kernel's view of version `version`: protocol and metadata, active adds
    /// and the requested application transactions.
    fn kernel_view_at(&self, version: u64, app_ids: &BTreeSet<String>, max_active: usize) -> Result<KernelView> {
        let unsupported = |message: String| fail(MaintenanceErrorKind::Unsupported, message);
        let snapshot = Snapshot::builder_for(self.url.as_str())
            .at_version(version)
            .build(&self.engine)
            .map_err(|error| unsupported(format!("Kernel cannot load version {version}: {error}")))?;
        if snapshot.version() != version {
            return Err(unsupported(format!(
                "Kernel loaded {} instead of {version}",
                snapshot.version()
            )));
        }
        let fingerprint = TableFingerprint::of(&snapshot);
        if fingerprint
            .configuration
            .contains_key("delta.setTransactionRetentionDuration")
        {
            return Err(unsupported(
                "transaction retention must remain unset to prune history".to_owned(),
            ));
        }
        let scan = Arc::clone(&snapshot).scan_builder().build()?;
        let mut adds = BTreeMap::new();
        for metadata in scan.scan_metadata(&self.engine)? {
            let batch = metadata?.scan_files.apply_selection_vector()?.try_into_record_batch()?;
            for file in parse_scan_rows(&batch, &fingerprint.partition_columns)? {
                if adds.len() >= max_active {
                    return Err(fail(
                        MaintenanceErrorKind::ResourceLimit,
                        format!("active file count exceeds {max_active}"),
                    ));
                }
                let state = AddState {
                    size: i64::try_from(file.size)?,
                    partition_values: file.partition_values,
                };
                if adds.insert(self.log_key(&file.path)?, state).is_some() {
                    return Err(unsupported(format!("duplicate active path {}", file.path)));
                }
            }
        }
        let mut txns = BTreeMap::new();
        for app_id in app_ids {
            if let Some(txn_version) = snapshot.get_app_id_version(app_id, &self.engine)? {
                txns.insert(app_id.clone(), txn_version);
            }
        }
        let features = |features: Option<Vec<String>>| features.map(|features| features.into_iter().collect());
        Ok(KernelView {
            protocol: ProtocolState {
                min_reader_version: i64::from(fingerprint.min_reader_version),
                min_writer_version: i64::from(fingerprint.min_writer_version),
                reader_features: features(fingerprint.reader_features),
                writer_features: features(fingerprint.writer_features),
            },
            metadata: MetadataState::new(
                fingerprint.table_id,
                &fingerprint.schema_string,
                fingerprint.partition_columns,
                fingerprint.configuration,
            )?,
            active: adds.keys().cloned().collect(),
            adds,
            txns,
        })
    }

    /// Compare an independent replay with checkpoint `version`, both through a
    /// direct Parquet read of the checkpoint file and through Kernel loading it:
    /// the active add set with each add's partition values and size, every
    /// application transaction, the last protocol and the last metaData (id,
    /// schema, partition columns and configuration). Statistics are not compared.
    fn verify_checkpoint(
        &self,
        version: u64,
        expected: &LogReplay,
        table_id: &str,
        config: &PruneConfig,
    ) -> Result<()> {
        let unsupported = |message: String| fail(MaintenanceErrorKind::Unsupported, message);
        if expected.txn_regressed {
            return Err(unsupported(
                "a txn version regressed in the JSON replay; refusing to prune".to_owned(),
            ));
        }
        if expected.table_id.as_deref() != Some(table_id) {
            return Err(unsupported("replayed table id differs from the table".to_owned()));
        }
        let parquet = self.read_checkpoint(version, config.max_checkpoint_bytes)?;
        if parquet.table_id.as_deref() != Some(table_id) {
            return Err(unsupported(format!(
                "checkpoint {version} belongs to a different table id"
            )));
        }
        let source = format!("checkpoint {version} Parquet");
        if let Some(mismatch) = proof_mismatch(
            &source,
            &expected.active,
            &expected.txns,
            &parquet.active,
            &parquet.txns,
        )
        .or_else(|| {
            state_mismatch(
                &source,
                expected,
                &parquet.adds,
                parquet.protocol.as_ref(),
                parquet.metadata.as_ref(),
            )
        }) {
            return Err(unsupported(mismatch));
        }
        let mut app_ids: BTreeSet<String> = expected.txns.keys().cloned().collect();
        app_ids.insert(PURGE_WATERMARK.to_owned());
        let kernel = self.kernel_view_at(version, &app_ids, config.max_active_files)?;
        if kernel.metadata.id != table_id {
            return Err(unsupported(format!(
                "Kernel version {version} has a different table id"
            )));
        }
        let source = format!("Kernel snapshot {version}");
        if let Some(mismatch) = proof_mismatch(&source, &expected.active, &expected.txns, &kernel.active, &kernel.txns)
            .or_else(|| {
                state_mismatch(
                    &source,
                    expected,
                    &kernel.adds,
                    Some(&kernel.protocol),
                    Some(&kernel.metadata),
                )
            })
        {
            return Err(unsupported(mismatch));
        }
        Ok(())
    }

    /// Hand-written replay of `first..=last` on top of `replay`; every JSON must exist.
    fn replay_range(
        &self,
        replay: &mut LogReplay,
        first: u64,
        last: u64,
        config: &PruneConfig,
        mut visit: impl FnMut(u64, &CommitFacts) -> bool,
    ) -> Result<()> {
        for version in first..=last {
            let bytes = self.read_commit(version, config.max_log_file_bytes)?;
            let facts = self.replay_commit(replay, &bytes)?;
            if !visit(version, &facts) {
                break;
            }
        }
        Ok(())
    }

    /// Whether every log object left below `floor` (JSON and checkpoints `< floor`,
    /// plus the `floor` JSON and checkpoint when `include_floor` because a newer
    /// checkpoint will make them deletable) is eligible: commit timestamp (when present) and
    /// object modification time older than `log_cutoff_ms`, and every remove
    /// older than the progress cutoff, with progress covering the floor.
    #[allow(clippy::too_many_arguments)]
    fn leftovers_eligible(
        &self,
        floor: u64,
        json: &BTreeSet<u64>,
        checkpoints: &BTreeSet<u64>,
        names: &BTreeMap<String, ObjectMeta>,
        progress: &VacuumLiteProgress,
        log_cutoff_ms: i64,
        config: &PruneConfig,
        include_floor: bool,
    ) -> Result<bool> {
        if progress.version < floor {
            return Ok(false);
        }
        let modified = |name: String| names.get(&name).map(|meta| meta.last_modified.timestamp_millis());
        let old = |stamp: Option<i64>| stamp.is_some_and(|stamp| stamp < log_cutoff_ms);
        let end = if include_floor { floor.saturating_add(1) } else { floor };
        for version in json.range(..end) {
            let bytes = self.read_commit(*version, config.max_log_file_bytes)?;
            let facts = self.replay_commit(&mut LogReplay::default(), &bytes)?;
            let stamp = [facts.timestamp, modified(format!("{version:020}.json"))]
                .into_iter()
                .flatten()
                .max();
            if !old(stamp)
                || !old(modified(format!("{version:020}.json")))
                || !facts
                    .remove_stamps
                    .iter()
                    .all(|stamp| stamp.is_some_and(|stamp| stamp < progress.cutoff_ms))
            {
                return Ok(false);
            }
        }
        Ok(checkpoints
            .range(..end)
            .all(|version| old(modified(format!("{version:020}.checkpoint.parquet")))))
    }

    /// Find (or, with `target`, re-prove) the newest checkpoint `C` whose
    /// history may be deleted. See the crate README for the proof conditions.
    fn prove_prune(
        &self,
        config: &PruneConfig,
        log_cutoff_ms: i64,
        table_id: &str,
        head: u64,
        target: Option<u64>,
    ) -> Result<Option<PruneProof>> {
        let unsupported = |message: String| fail(MaintenanceErrorKind::Unsupported, message);
        let stale = |message: &str| Err(fail(MaintenanceErrorKind::StalePlan, message.to_owned()));
        let names = self.list_log(config.max_log_files.saturating_mul(4))?;
        let (mut json, mut checkpoints, mut other) = (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
        for name in names.keys() {
            match classify_log_name(name) {
                Some(LogObject::Json(version)) => json.insert(version),
                Some(LogObject::Checkpoint(version)) => checkpoints.insert(version),
                Some(LogObject::Other(version)) => other.insert(version),
                Some(LogObject::Crc(_)) | None => false,
            };
        }
        let (Some(hint), Some(progress)) = (self.hinted_checkpoint_version(true)?, self.vacuum_lite_progress()?) else {
            return if target.is_some() {
                stale("checkpoint hint or VACUUM LITE progress record disappeared")
            } else {
                Ok(None)
            };
        };
        if progress.table_id != table_id {
            return Err(fail(
                MaintenanceErrorKind::MetadataChanged,
                "VACUUM LITE progress record belongs to a different table id",
            ));
        }
        let floor = match self.log_floor()? {
            Some(floor) if floor.table_id != table_id => {
                return Err(fail(
                    MaintenanceErrorKind::MetadataChanged,
                    "log floor record belongs to a different table id",
                ));
            }
            floor => floor.map(|floor| floor.checkpoint_version),
        };
        let upper = hint.min(progress.version).min(head);
        // A delayed older checkpoint may legally rewrite `_last_checkpoint` below
        // the durable floor; nothing at or below the floor is re-provable then.
        if floor.is_some_and(|floor| floor > upper) {
            return if target.is_some() {
                stale("checkpoint hint, VACUUM progress or head is below the durable log floor")
            } else {
                Ok(None)
            };
        }
        if let Some(version) = other.range(..=upper).next() {
            return Err(unsupported(format!(
                "unsupported log object at version {version} (multi-part, V2 or compacted); refusing to prune"
            )));
        }
        let candidates: Vec<u64> = checkpoints
            .range(floor.unwrap_or(0)..=upper)
            .rev()
            .copied()
            .filter(|candidate| target.is_none_or(|target| target == *candidate))
            .collect();
        let above_floor = |candidate: u64| floor.is_none_or(|floor| candidate > floor);
        let seed = |replay: &mut LogReplay| -> Result<u64> {
            match floor {
                Some(floor) => {
                    *replay = self.read_checkpoint(floor, config.max_checkpoint_bytes)?;
                    Ok(floor + 1)
                }
                None => Ok(0),
            }
        };
        let mut chosen = None;
        if let Some(&newest) = candidates.iter().find(|candidate| above_floor(**candidate)) {
            let mut replay = LogReplay::default();
            let first = seed(&mut replay)?;
            if usize::try_from(newest - first).unwrap_or(usize::MAX) >= config.max_log_files {
                return Err(fail(
                    MaintenanceErrorKind::ResourceLimit,
                    "log history exceeds the replay bound",
                ));
            }
            if let Some(missing) = (first..=newest).find(|version| !json.contains(version)) {
                return Err(unsupported(format!(
                    "log history gap: {missing:020}.json is missing above the log floor"
                )));
            }
            let vacuumed = |stamp: &Option<i64>| stamp.is_some_and(|stamp| stamp < progress.cutoff_ms);
            let seeded = replay.tombstones.values().all(|(_, stamp)| vacuumed(stamp));
            // Candidate `C` needs every remove in `..=C` vacuumed (later VACUUM LITE
            // seeds from checkpoint `C`, which may drop tombstones) but only the
            // deleted JSON `< C` aged; `C.json` itself is retained.
            let (mut aged_below, mut vacuumed_through) = (seeded.then_some(first), None);
            if seeded {
                self.replay_range(&mut replay, first, newest, config, |version, facts| {
                    let modified = names
                        .get(&format!("{version:020}.json"))
                        .map(|meta| meta.last_modified.timestamp_millis());
                    let stamp = [facts.timestamp, modified].into_iter().flatten().max();
                    let vacuumed_here = facts.remove_stamps.iter().all(vacuumed);
                    let aged = stamp.is_some_and(|stamp| stamp < log_cutoff_ms);
                    if vacuumed_here {
                        vacuumed_through = Some(version);
                        if aged {
                            aged_below = Some(version + 1);
                        }
                    }
                    vacuumed_here && aged
                })?;
            }
            chosen = candidates.iter().copied().find(|candidate| {
                above_floor(*candidate)
                    && aged_below.is_some_and(|limit| *candidate <= limit)
                    && vacuumed_through.is_some_and(|through| *candidate <= through)
            });
        }
        // History left at or below the floor by an interrupted prune must satisfy
        // the same eligibility as a fresh proof under the effective cutoff.
        if let Some(floor) = floor
            && !self.leftovers_eligible(
                floor,
                &json,
                &checkpoints,
                &names,
                &progress,
                log_cutoff_ms,
                config,
                chosen.is_some(),
            )?
        {
            return if target.is_some() {
                stale("history left below the log floor is not eligible for pruning under the current policy")
            } else {
                Ok(None)
            };
        }
        let checkpoint = match (chosen, floor) {
            (Some(checkpoint), _) => {
                let mut replay = LogReplay::default();
                let first = seed(&mut replay)?;
                self.replay_range(&mut replay, first, checkpoint, config, |_, _| true)?;
                self.verify_checkpoint(checkpoint, &replay, table_id, config)?;
                checkpoint
            }
            // Completing an interrupted prune: the full proof held before the floor
            // was written; re-verify that Kernel still agrees with the checkpoint.
            (None, Some(floor))
                if candidates.contains(&floor)
                    && (json.range(..floor).next().is_some() || checkpoints.range(..floor).next().is_some()) =>
            {
                let replay = self.read_checkpoint(floor, config.max_checkpoint_bytes)?;
                self.verify_checkpoint(floor, &replay, table_id, config)?;
                floor
            }
            _ if target.is_some() => return stale("the planned checkpoint is no longer provably prunable"),
            _ => return Ok(None),
        };
        // Every checkpoint planned for deletion (all `< C`, including an old floor
        // checkpoint on advance) must be older than the log cutoff.
        let young_checkpoint = checkpoints.range(..checkpoint).any(|version| {
            names
                .get(&format!("{version:020}.checkpoint.parquet"))
                .is_none_or(|meta| meta.last_modified.timestamp_millis() >= log_cutoff_ms)
        });
        if young_checkpoint {
            return if target.is_some() {
                stale("a checkpoint below the planned checkpoint is younger than the log retention")
            } else {
                Ok(None)
            };
        }
        let mut deletions: Vec<(u64, u8)> = json
            .range(..checkpoint)
            .map(|version| (*version, 0))
            .chain(checkpoints.range(..checkpoint).map(|version| (*version, 1)))
            .collect();
        deletions.sort_unstable();
        let backlog = u64::try_from(deletions.len().saturating_sub(config.max_prune_files)).unwrap_or(u64::MAX);
        deletions.truncate(config.max_prune_files);
        Ok(Some(PruneProof {
            checkpoint_version: checkpoint,
            deletions: deletions
                .into_iter()
                .map(|(version, kind)| {
                    if kind == 0 {
                        format!("_delta_log/{version:020}.json")
                    } else {
                        format!("_delta_log/{version:020}.checkpoint.parquet")
                    }
                })
                .collect(),
            backlog,
        }))
    }

    /// Plan `_delta_log` pruning: choose the newest checkpoint `C` proven
    /// equivalent to JSON history `0..=C` (or to the previous floor checkpoint
    /// plus later JSON), whose tombstones VACUUM LITE already processed and
    /// whose deletable commits (`< C`; `C.json` is retained) are all older
    /// than the log retention. Deletes nothing.
    ///
    /// # Errors
    /// Rejects unsafe retention, unsupported logs, record or checkpoint
    /// disagreements and bounds.
    pub fn plan_prune_log(&self, config: &PruneConfig) -> Result<PrunePlan> {
        if config.max_prune_files == 0 {
            return Err(fail(MaintenanceErrorKind::InvalidPlan, "prune bound must be positive"));
        }
        let state = self.active_state(config.max_active_files)?;
        state.fingerprint.validate_supported(false)?;
        let retention = self.log_retention(config, &state.fingerprint)?;
        validate_anchor("prune", config.now_ms)?;
        let log_cutoff_ms = config
            .now_ms
            .checked_sub(retention)
            .ok_or_else(|| fail(MaintenanceErrorKind::InvalidPlan, "log retention exceeds the anchor"))?;
        let header = PlanHeader::new(self, &state)?;
        let proof = self.prove_prune(
            config,
            log_cutoff_ms,
            &state.fingerprint.table_id,
            state.snapshot.version(),
            None,
        )?;
        let (checkpoint_version, deletions, backlog_files) = match proof {
            Some(proof) => (Some(proof.checkpoint_version), proof.deletions, proof.backlog),
            None => (None, Vec::new(), 0),
        };
        Ok(PrunePlan {
            header,
            log_retention_ms: retention,
            now_ms: config.now_ms,
            log_cutoff_ms,
            checkpoint_version,
            progress: self.vacuum_lite_progress()?,
            deletions,
            backlog_files,
        })
    }

    /// Delete planned `_delta_log` history after re-proving it. No Delta commit
    /// is written. The durable log floor is recorded before the first delete;
    /// deletes run in ascending version order, treat absence as success, and
    /// never touch checkpoint `C`, `_last_checkpoint` or anything newer.
    /// Cancellation between deletes returns a `Partial` report.
    ///
    /// # Errors
    /// Returns invalid, stale, unsafe, metadata, authority or storage errors
    /// (with partial progress attached once deletion started).
    pub fn apply_prune_log(
        &self,
        plan: &PrunePlan,
        config: &PruneConfig,
        control: &MaintenanceControl,
    ) -> Result<MaintenanceReport> {
        plan.header.validate(self)?;
        let mut report = MaintenanceReport::new("prune_log", &plan.header);
        report.cutoff_ms = Some(plan.log_cutoff_ms);
        report.backlog_files = plan.backlog_files;
        let Some(checkpoint) = plan.checkpoint_version else {
            return Ok(report);
        };
        report.version = checkpoint;
        if plan.deletions.is_empty() {
            return Ok(report);
        }
        if plan.deletions.len() > config.max_prune_files {
            return Err(fail(MaintenanceErrorKind::InvalidPlan, "plan exceeds the prune bound"));
        }
        let mut previous = None;
        for name in &plan.deletions {
            let key = deletion_key(name, checkpoint)?;
            if previous.is_some_and(|previous| previous >= key) {
                return Err(fail(
                    MaintenanceErrorKind::InvalidPlan,
                    "planned deletions must be strictly ascending",
                ));
            }
            previous = Some(key);
        }
        validate_anchor("prune plan", plan.now_ms)?;
        if plan.now_ms.checked_sub(plan.log_retention_ms) != Some(plan.log_cutoff_ms) {
            return Err(fail(
                MaintenanceErrorKind::InvalidPlan,
                "plan cutoff does not match its recorded anchor and retention",
            ));
        }
        let state = self.active_state(config.max_active_files)?;
        plan.header.fingerprint.require_same(&state.fingerprint)?;
        let planned_policy = PruneConfig {
            log_retention_ms: Some(plan.log_retention_ms),
            ..config.clone()
        };
        self.log_retention(&planned_policy, &state.fingerprint)?;
        let current = self.log_retention(config, &state.fingerprint)?;
        validate_anchor("prune", config.now_ms)?;
        let current_cutoff = config
            .now_ms
            .checked_sub(current)
            .ok_or_else(|| fail(MaintenanceErrorKind::InvalidPlan, "log retention exceeds the anchor"))?;
        let effective = plan.log_cutoff_ms.min(current_cutoff);
        report.cutoff_ms = Some(effective);
        let table_id = state.fingerprint.table_id.clone();
        self.prove_prune(config, effective, &table_id, state.snapshot.version(), Some(checkpoint))?
            .ok_or_else(|| {
                fail(
                    MaintenanceErrorKind::StalePlan,
                    "planned checkpoint is no longer provable",
                )
            })?;
        if control.interrupted() {
            report.outcome = Outcome::Partial;
            report.interrupted = true;
            return Ok(report);
        }
        if let Err(error) = self.check_publication() {
            return Err(fail_with(
                MaintenanceErrorKind::UnsafeRequest,
                format!("authority check failed: {error}"),
                &report,
            ));
        }
        // The proof relied on the progress record; make it durable before acting on it.
        self.update_record::<VacuumLiteProgress>(VACUUM_PROGRESS_RECORD, |_| Ok(None))
            .map_err(|error| {
                fail_with(
                    error_kind(&error).unwrap_or(MaintenanceErrorKind::Uncertain),
                    format!("VACUUM LITE progress record is not durable; nothing deleted: {error:#}"),
                    &report,
                )
            })?;
        let floor = LogFloor {
            table_id,
            checkpoint_version: checkpoint,
        };
        self.update_record(LOG_FLOOR_RECORD, |current| merge_floor(current, &floor))
            .map_err(|error| {
                fail_with(
                    error_kind(&error).unwrap_or(MaintenanceErrorKind::Uncertain),
                    format!("log floor record not written; nothing deleted: {error:#}"),
                    &report,
                )
            })?;
        let mut interrupted = false;
        for name in &plan.deletions {
            if control.interrupted() {
                interrupted = true;
                break;
            }
            if let Err(error) = self.check_publication() {
                return Err(fail_with(
                    MaintenanceErrorKind::UnsafeRequest,
                    format!("authority check failed: {error}"),
                    &report,
                ));
            }
            let path = self.object_path(name)?;
            let store = Arc::clone(&self.store);
            match shared_executor()?.block_on(async move { store.delete(&path).await }) {
                Ok(()) => report.files_deleted += 1,
                Err(object_store::Error::NotFound { .. }) => report.files_already_absent += 1,
                Err(error) => {
                    return Err(fail_with(
                        MaintenanceErrorKind::Uncertain,
                        format!("delete of {name} failed: {error}"),
                        &report,
                    ));
                }
            }
        }
        if self.url.scheme() == "file" {
            sync_directory(&self.root.join("_delta_log"))?;
        }
        report.files_removed = report.files_deleted + report.files_already_absent;
        if interrupted {
            report.outcome = Outcome::Partial;
            report.interrupted = true;
        } else {
            report.outcome = Outcome::Pruned;
        }
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

#[cfg(test)]
mod durability_tests {
    use super::*;
    use crate::delta::INGESTION_PARTITION;
    use crate::sync_hook;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};
    use std::collections::BTreeMap;

    const BASE: i64 = 1_704_067_200_000;

    /// Scenario: OPTIMIZE's JSON commit becomes visible but the log fsync fails.
    /// Guarantees: neither reconciliation nor a repeated apply acknowledges the
    /// visible plan until replayed log state is durable; outputs stay retained, and
    /// a retry after repair succeeds as already applied without a second commit.
    #[test]
    fn visible_plan_is_not_acknowledged_until_replay_is_durable() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let root = fs::canonicalize(directory.path())?;
        let url = Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("URL"))?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(INGESTION_PARTITION, DataType::Utf8, false),
        ]));
        let table = DeltaTable::open_url(url.clone(), Arc::clone(&schema), vec![INGESTION_PARTITION.into()])?;
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        for chunk in 0..6_i64 {
            let minute = chunk / 2;
            let partition = format!("2024-01-01-00-{minute:02}");
            runtime.block_on(async {
                let mut file = table
                    .new_file_partitioned(
                        Arc::clone(&schema),
                        BTreeMap::from([(INGESTION_PARTITION.to_owned(), Some(partition.clone()))]),
                    )?
                    .with_ingestion_time(BASE + minute * 60_000 + 1)?;
                file.write(&RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from_iter_values(chunk * 10..chunk * 10 + 5)),
                        Arc::new(arrow::array::StringArray::from(vec![partition.as_str(); 5])),
                    ],
                )?)
                .await?;
                file.finish().await.map(|_| ())
            })?;
        }
        drop(table);
        let maintenance = DeltaTable::open_for_maintenance(&url)?;
        let config = OptimizeConfig {
            granularity: Some(Granularity::Minute),
            now_ms: Some(BASE + 60 * 60_000),
            ..OptimizeConfig::default()
        };
        let plan = maintenance.plan_optimize(&config)?;
        assert!(!plan.bins.is_empty());
        let before = maintenance.version()?;
        let log = root.join("_delta_log");
        sync_hook::fail_on(&log);

        let control = MaintenanceControl::new(Arc::new(std::sync::atomic::AtomicBool::new(false)), None);
        let first = maintenance.apply_optimize(&plan, &config, &control);
        assert!(matches!(
            first.as_ref().err().and_then(error_kind),
            Some(MaintenanceErrorKind::Uncertain)
        ));
        let committed = before + 1;
        assert!(
            log.join(format!("{committed:020}.json")).is_file(),
            "publication must be visible"
        );
        assert_eq!(maintenance.version()?, committed);
        let outputs = first
            .err()
            .and_then(|error| error.downcast::<MaintenanceError>().ok())
            .map(|error| error.report.map(|report| report.outputs).unwrap_or_default())
            .unwrap_or_default();
        assert!(!outputs.is_empty());
        for output in &outputs {
            assert!(root.join(output).is_file(), "retained output {output}");
        }

        let repeated = maintenance.apply_optimize(&plan, &config, &control);
        assert!(matches!(
            repeated.as_ref().err().and_then(error_kind),
            Some(MaintenanceErrorKind::Uncertain)
        ));

        sync_hook::reset(&root);
        let repaired = maintenance.apply_optimize(&plan, &config, &control)?;
        assert_eq!(repaired.outcome, Outcome::AlreadyApplied);
        assert_eq!(maintenance.version()?, committed);
        let synced = sync_hook::synced_under(&root);
        assert!(synced.contains(&log));
        assert!(synced.contains(&log.join(format!("{committed:020}.json"))));
        sync_hook::reset(&root);
        Ok(())
    }
}

#[cfg(test)]
mod prune_tests {
    use super::*;

    fn set(paths: &[&str]) -> BTreeSet<String> {
        paths.iter().map(|path| (*path).to_owned()).collect()
    }

    fn txns(entries: &[(&str, i64)]) -> BTreeMap<String, i64> {
        entries
            .iter()
            .map(|(id, version)| ((*id).to_owned(), *version))
            .collect()
    }

    #[test]
    fn proof_mismatch_detects_every_difference() {
        let active = set(&["a", "b"]);
        let expected = txns(&[("arrowcol.publication.1", 1), (PURGE_WATERMARK, 5)]);
        assert_eq!(proof_mismatch("x", &active, &expected, &active, &expected), None);
        assert!(proof_mismatch("x", &active, &expected, &set(&["a"]), &expected).is_some());
        assert!(proof_mismatch("x", &active, &expected, &set(&["a", "b", "c"]), &expected).is_some());
        let missing = txns(&[(PURGE_WATERMARK, 5)]);
        assert!(
            proof_mismatch("x", &active, &expected, &active, &missing)
                .is_some_and(|message| message.contains("arrowcol.publication.1"))
        );
        let older = txns(&[("arrowcol.publication.1", 1), (PURGE_WATERMARK, 4)]);
        assert!(proof_mismatch("x", &active, &expected, &active, &older).is_some());
        let extra = txns(&[("arrowcol.publication.1", 1), (PURGE_WATERMARK, 5), ("other", 1)]);
        assert!(proof_mismatch("x", &active, &expected, &active, &extra).is_some());
    }

    #[test]
    fn records_are_table_bound_and_monotonic() -> Result<()> {
        let progress = |version, cutoff_ms| VacuumLiteProgress {
            table_id: "t".into(),
            version,
            cutoff_ms,
        };
        assert_eq!(merge_progress(None, &progress(3, 10))?, Some(progress(3, 10)));
        assert_eq!(
            merge_progress(Some(&progress(3, 10)), &progress(4, 11))?,
            Some(progress(4, 11))
        );
        assert_eq!(merge_progress(Some(&progress(3, 10)), &progress(3, 10))?, None);
        assert_eq!(merge_progress(Some(&progress(3, 10)), &progress(2, 11))?, None);
        assert_eq!(merge_progress(Some(&progress(3, 10)), &progress(4, 9))?, None);
        let other = VacuumLiteProgress {
            table_id: "u".into(),
            ..progress(9, 99)
        };
        assert_eq!(
            merge_progress(Some(&progress(3, 10)), &other)
                .err()
                .as_ref()
                .and_then(error_kind),
            Some(MaintenanceErrorKind::MetadataChanged)
        );
        let floor = |checkpoint_version| LogFloor {
            table_id: "t".into(),
            checkpoint_version,
        };
        assert_eq!(merge_floor(None, &floor(2))?, Some(floor(2)));
        assert_eq!(merge_floor(Some(&floor(2)), &floor(5))?, Some(floor(5)));
        assert_eq!(merge_floor(Some(&floor(5)), &floor(2))?, None);
        assert_eq!(merge_floor(Some(&floor(5)), &floor(5))?, None);
        let foreign = LogFloor {
            table_id: "u".into(),
            checkpoint_version: 9,
        };
        assert!(merge_floor(Some(&floor(5)), &foreign).is_err());
        Ok(())
    }

    #[test]
    fn state_mismatch_compares_recovery_state_semantically() -> Result<()> {
        let add = |value: &str, size| AddState {
            size,
            partition_values: BTreeMap::from([("p".to_owned(), Some(value.to_owned()))]),
        };
        let protocol = ProtocolState {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(set(&["a", "b"])),
            writer_features: Some(set(&["c"])),
        };
        let configuration = BTreeMap::from([("k".to_owned(), "v".to_owned())]);
        let metadata = MetadataState::new(
            "t".into(),
            r#"{"type":"struct","fields":[]}"#,
            vec!["p".into()],
            configuration.clone(),
        )?;
        let expected = LogReplay {
            adds: BTreeMap::from([("x".to_owned(), add("1", 10))]),
            protocol: Some(protocol.clone()),
            metadata: Some(metadata.clone()),
            ..LogReplay::default()
        };
        let check = |adds: &BTreeMap<String, AddState>, protocol: &ProtocolState, metadata: &MetadataState| {
            state_mismatch("x", &expected, adds, Some(protocol), Some(metadata))
        };
        // Key order inside the schema JSON is not significant.
        let reordered = MetadataState::new(
            "t".into(),
            r#"{"fields":[],"type":"struct"}"#,
            vec!["p".into()],
            configuration.clone(),
        )?;
        assert_eq!(check(&expected.adds, &protocol, &reordered), None);
        let changed = BTreeMap::from([("x".to_owned(), add("2", 10))]);
        assert!(check(&changed, &protocol, &metadata).is_some());
        let resized = BTreeMap::from([("x".to_owned(), add("1", 11))]);
        assert!(check(&resized, &protocol, &metadata).is_some());
        let features = ProtocolState {
            writer_features: Some(set(&["c", "d"])),
            ..protocol.clone()
        };
        assert!(check(&expected.adds, &features, &metadata).is_some());
        let schema = MetadataState::new(
            "t".into(),
            r#"{"type":"struct","fields":[{"name":"y"}]}"#,
            vec!["p".into()],
            configuration,
        )?;
        assert!(check(&expected.adds, &protocol, &schema).is_some_and(|message| message.contains("schemaString")));
        let config = MetadataState {
            configuration: BTreeMap::new(),
            ..metadata.clone()
        };
        assert!(check(&expected.adds, &protocol, &config).is_some_and(|message| message.contains("configuration")));
        let partitioned = MetadataState {
            partition_columns: Vec::new(),
            ..metadata
        };
        assert!(check(&expected.adds, &protocol, &partitioned).is_some());
        assert!(state_mismatch("x", &expected, &expected.adds, None, None).is_some());
        Ok(())
    }

    #[test]
    fn log_names_intervals_and_deletion_keys_parse_strictly() {
        assert_eq!(parse_interval_ms("interval 30 days"), Some(DEFAULT_LOG_RETENTION_MS));
        assert_eq!(parse_interval_ms("INTERVAL 1 hour"), Some(3_600_000));
        assert_eq!(parse_interval_ms("2 weeks"), Some(2 * 604_800_000));
        assert_eq!(parse_interval_ms("interval -1 days"), None);
        assert_eq!(parse_interval_ms("interval 1 fortnight"), None);
        assert_eq!(parse_interval_ms("interval 1 day extra"), None);
        assert_eq!(classify_log_name("00000000000000000007.json"), Some(LogObject::Json(7)));
        assert_eq!(
            classify_log_name("00000000000000000007.checkpoint.parquet"),
            Some(LogObject::Checkpoint(7))
        );
        assert_eq!(classify_log_name("00000000000000000007.crc"), Some(LogObject::Crc(7)));
        assert_eq!(
            classify_log_name("00000000000000000007.checkpoint.0000000001.0000000002.parquet"),
            Some(LogObject::Other(7))
        );
        assert_eq!(classify_log_name("_last_checkpoint"), None);
        assert_eq!(classify_log_name("0000000000000000007.json"), None);
        assert!(deletion_key("_delta_log/00000000000000000002.json", 3).is_ok());
        assert!(
            deletion_key("_delta_log/00000000000000000003.json", 3).is_err(),
            "C.json is retained"
        );
        assert!(deletion_key("_delta_log/00000000000000000004.json", 3).is_err());
        assert!(deletion_key("_delta_log/00000000000000000002.checkpoint.parquet", 3).is_ok());
        assert!(deletion_key("_delta_log/00000000000000000003.checkpoint.parquet", 3).is_err());
        assert!(deletion_key("_delta_log/_last_checkpoint", 3).is_err());
        assert!(deletion_key("part-0.parquet", 3).is_err());
    }
}

#[cfg(test)]
mod record_durability_tests {
    use super::*;
    use crate::delta::INGESTION_PARTITION;
    use crate::sync_hook;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Field;

    const BASE: i64 = 1_704_067_200_000;

    fn control() -> MaintenanceControl {
        MaintenanceControl::new(Arc::new(AtomicBool::new(false)), None)
    }

    fn short_vacuum() -> Result<VacuumConfig> {
        let mut config = VacuumConfig::new(0, now_ms()? + 1_000);
        config.unsafe_allow_short_retention_for_tests = true;
        Ok(config)
    }

    fn short_prune() -> Result<PruneConfig> {
        Ok(PruneConfig {
            log_retention_ms: Some(0),
            unsafe_allow_short_retention_for_tests: true,
            ..PruneConfig::new(now_ms()? + 1_000)
        })
    }

    fn json_count(root: &std::path::Path) -> Result<usize> {
        Ok(fs::read_dir(root.join("_delta_log"))?
            .filter(|entry| {
                entry
                    .as_ref()
                    .is_ok_and(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
            })
            .count())
    }

    fn kind_of<T>(result: &Result<T>) -> Option<MaintenanceErrorKind> {
        result.as_ref().err().and_then(error_kind)
    }

    /// A marked disposable root holding OPTIMIZE tombstones.
    fn tombstoned_table() -> Result<(tempfile::TempDir, PathBuf, Arc<DeltaTable>)> {
        let directory = tempfile::tempdir()?;
        let root = fs::canonicalize(directory.path())?;
        let url = Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("URL"))?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(INGESTION_PARTITION, DataType::Utf8, false),
        ]));
        let table = DeltaTable::open_url(url.clone(), Arc::clone(&schema), vec![INGESTION_PARTITION.into()])?;
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        for chunk in 0..6_i64 {
            let minute = chunk / 2;
            let partition = format!("2024-01-01-00-{minute:02}");
            runtime.block_on(async {
                let mut file = table
                    .new_file_partitioned(
                        Arc::clone(&schema),
                        BTreeMap::from([(INGESTION_PARTITION.to_owned(), Some(partition.clone()))]),
                    )?
                    .with_ingestion_time(BASE + minute * 60_000 + 1)?;
                file.write(&RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from_iter_values(chunk * 10..chunk * 10 + 5)),
                        Arc::new(StringArray::from(vec![partition.as_str(); 5])),
                    ],
                )?)
                .await?;
                file.finish().await.map(|_| ())
            })?;
        }
        drop(table);
        let maintenance = DeltaTable::open_for_maintenance(&url)?;
        let config = OptimizeConfig {
            granularity: Some(Granularity::Minute),
            now_ms: Some(BASE + 60 * 60_000),
            ..OptimizeConfig::default()
        };
        let plan = maintenance.plan_optimize(&config)?;
        assert!(!plan.bins.is_empty());
        maintenance.apply_optimize(&plan, &config, &control())?;
        fs::write(root.join(DISPOSABLE_TEST_MARKER), b"")?;
        Ok((directory, root, maintenance))
    }

    /// Scenario: the log floor rename becomes visible but its file fsync, then
    /// the maintenance-directory fsync, fail.
    /// Guarantees: apply_prune_log deletes nothing while the floor is not
    /// durable, a repeat (whose merge is a no-op) still fsyncs and fails, and
    /// after repair the repeat prunes having fsynced the floor, directory and root.
    #[test]
    fn prune_never_deletes_behind_a_non_durable_floor() -> Result<()> {
        let (_directory, root, maintenance) = tombstoned_table()?;
        let vacuum = short_vacuum()?;
        maintenance.apply_vacuum(&maintenance.plan_vacuum(&vacuum)?, &vacuum, &control())?;
        maintenance.checkpoint_if_due(1, true)?;
        let config = short_prune()?;
        let plan = maintenance.plan_prune_log(&config)?;
        let checkpoint = plan.checkpoint_version.context("provable checkpoint")?;
        let before = json_count(&root)?;
        assert_eq!(plan.deletions.len(), before - 1, "C.json is retained");
        let records = root.join(MAINTENANCE_DIR);
        let floor = records.join(LOG_FLOOR_RECORD);

        sync_hook::fail_on(&floor);
        let first = maintenance.apply_prune_log(&plan, &config, &control());
        assert_eq!(kind_of(&first), Some(MaintenanceErrorKind::Uncertain));
        assert!(floor.is_file(), "the floor rename is visible");
        assert_eq!(json_count(&root)?, before, "nothing deleted behind a non-durable floor");
        let repeat = maintenance.apply_prune_log(&plan, &config, &control());
        assert_eq!(kind_of(&repeat), Some(MaintenanceErrorKind::Uncertain));
        assert_eq!(json_count(&root)?, before, "a no-op merge still requires durability");

        sync_hook::reset(&root);
        sync_hook::fail_on(&records);
        let directory_fault = maintenance.apply_prune_log(&plan, &config, &control());
        assert_eq!(kind_of(&directory_fault), Some(MaintenanceErrorKind::Uncertain));
        assert_eq!(json_count(&root)?, before);

        sync_hook::reset(&root);
        let report = maintenance.apply_prune_log(&plan, &config, &control())?;
        assert_eq!(report.outcome, Outcome::Pruned);
        assert_eq!(json_count(&root)?, 1);
        assert_eq!(
            maintenance.log_floor()?.map(|floor| floor.checkpoint_version),
            Some(checkpoint)
        );
        let synced = sync_hook::synced_under(&root);
        for path in [&floor, &records, &root] {
            assert!(synced.contains(path), "{} fsynced", path.display());
        }
        sync_hook::reset(&root);
        Ok(())
    }

    /// Scenario: VACUUM LITE unlinks files but a partition-directory fsync fails,
    /// then the progress record's directory fsync fails.
    /// Guarantees: no progress is recorded while unlinks are not durable, so
    /// pruning waits; repeats (all files already absent) re-fsync and still
    /// fail; a visible but non-durable progress record is fsynced again on
    /// repeat; after repair progress is recorded and pruning is allowed.
    #[test]
    fn vacuum_progress_waits_for_durable_unlinks_and_record() -> Result<()> {
        let (_directory, root, maintenance) = tombstoned_table()?;
        let vacuum = short_vacuum()?;
        let plan = maintenance.plan_vacuum(&vacuum)?;
        let candidate = plan.candidates.first().context("vacuum candidate")?;
        let partition = root
            .join(percent_decode(&candidate.path)?)
            .parent()
            .context("partition")?
            .to_path_buf();
        assert!(partition != root);

        sync_hook::fail_on(&partition);
        let first = maintenance.apply_vacuum(&plan, &vacuum, &control());
        assert_eq!(kind_of(&first), Some(MaintenanceErrorKind::Uncertain));
        assert!(!root.join(&candidate.path).exists(), "unlinked");
        assert_eq!(maintenance.vacuum_lite_progress()?, None, "progress not advanced");
        maintenance.checkpoint_if_due(1, true)?;
        let config = short_prune()?;
        assert_eq!(maintenance.plan_prune_log(&config)?.report().outcome, Outcome::Noop);
        let repeat = maintenance.apply_vacuum(&plan, &vacuum, &control());
        assert_eq!(kind_of(&repeat), Some(MaintenanceErrorKind::Uncertain));
        assert_eq!(maintenance.vacuum_lite_progress()?, None);

        sync_hook::reset(&root);
        let records = root.join(MAINTENANCE_DIR);
        sync_hook::fail_on(&records);
        let record_fault = maintenance.apply_vacuum(&plan, &vacuum, &control());
        assert_eq!(kind_of(&record_fault), Some(MaintenanceErrorKind::Uncertain));
        assert!(records.join(VACUUM_PROGRESS_RECORD).is_file(), "visible, not durable");
        let again = maintenance.apply_vacuum(&plan, &vacuum, &control());
        assert_eq!(kind_of(&again), Some(MaintenanceErrorKind::Uncertain));

        sync_hook::reset(&root);
        let report = maintenance.apply_vacuum(&plan, &vacuum, &control())?;
        assert_eq!(report.outcome, Outcome::Deleted);
        assert_eq!(report.files_deleted, 0, "repeat finds every file already absent");
        let progress = maintenance.vacuum_lite_progress()?.context("progress")?;
        assert_eq!(progress.version, plan.header.read_version);
        let synced = sync_hook::synced_under(&root);
        for path in [&partition, &records.join(VACUUM_PROGRESS_RECORD), &records, &root] {
            assert!(synced.contains(path), "{} fsynced", path.display());
        }
        let prune = maintenance.plan_prune_log(&short_prune()?)?;
        assert_eq!(prune.report().outcome, Outcome::Planned);
        sync_hook::reset(&root);
        Ok(())
    }
}
