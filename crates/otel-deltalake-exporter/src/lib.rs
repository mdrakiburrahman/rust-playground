//! OTEL-specific routing and joins; storage and Delta semantics live in arrow-lake.

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_lake::commit::{BatchedTable, CommitOptions};
use arrow_lake::delta::{DeltaFileWriter, StagedAppend};
use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_core_nodes::exporters::parquet_exporter::idgen::PARTITION_METADATA_KEY;
use otel_arrow_dfe_core_nodes::exporters::parquet_exporter::records::OtapParquetRecords;
use otel_arrow_dfe_core_nodes::exporters::parquet_exporter::writer::{FileSink, FileSinkFactory};
use otel_arrow_dfe_core_nodes::exporters::parquet_exporter::{ParquetExporter, config::Config};
use otel_arrow_dfe_engine::{
    ExporterFactory, config::ExporterConfig, context::PipelineContext, exporter::ExporterWrapper, node::NodeId,
};
use otel_arrow_dfe_otap::{OTAP_EXPORTER_FACTORIES, pdata::OtapPdata};
use parquet::errors::ParquetError;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Namespace persisted alongside every parent and child ID.
pub const JOIN_NAMESPACE: &str = "_otel_join_namespace";
const DELTA_URN: &str = "urn:otel:exporter:deltalake";

fn external(error: anyhow::Error) -> ParquetError {
    ParquetError::External(error.into())
}

/// Local backend factory retained for the exporter lifetime.
#[derive(Clone)]
pub struct DeltaBackend(Arc<BackendState>);

struct BackendState {
    base: PathBuf,
    options: CommitOptions,
    tables: Mutex<HashMap<String, Arc<BatchedTable>>>,
    draining: AtomicBool,
}

impl DeltaBackend {
    /// Construct a backend for independent tables beneath a local directory.
    pub fn new(base: impl AsRef<Path>) -> Self {
        Self::with_options(base, CommitOptions::default())
    }

    /// Construct a backend with validated bounded commit settings.
    pub fn with_options(base: impl AsRef<Path>, options: CommitOptions) -> Self {
        Self(Arc::new(BackendState {
            base: base.as_ref().to_path_buf(),
            options,
            tables: Mutex::new(HashMap::new()),
            draining: AtomicBool::new(false),
        }))
    }

    async fn table(&self, name: &str) -> anyhow::Result<Arc<BatchedTable>> {
        let existing = self
            .0
            .tables
            .lock()
            .map_err(|_| anyhow::anyhow!("Delta backend lock poisoned"))?
            .get(name)
            .cloned();
        if let Some(table) = existing {
            return Ok(table);
        }
        let table = BatchedTable::open(self.0.base.join(name), self.0.options.clone()).await?;
        if self.0.draining.load(Ordering::Acquire) {
            table.start_draining();
        }
        let mut tables = self
            .0
            .tables
            .lock()
            .map_err(|_| anyhow::anyhow!("Delta backend lock poisoned"))?;
        Ok(Arc::clone(tables.entry(name.to_string()).or_insert(table)))
    }
}

struct DeltaSink {
    backend: DeltaBackend,
    name: String,
    schema: SchemaRef,
    table: Option<Arc<BatchedTable>>,
    writer: Option<DeltaFileWriter>,
    staged: Option<StagedAppend>,
}

impl DeltaSink {
    async fn stage_file(&mut self) -> anyhow::Result<()> {
        if self.staged.is_none() {
            let writer = self
                .writer
                .take()
                .ok_or_else(|| anyhow::anyhow!("Delta file has no writable data"))?;
            self.staged = Some(writer.stage().await?);
        }
        Ok(())
    }

    async fn publish(mut self, force: bool) -> anyhow::Result<()> {
        self.stage_file().await?;
        let table = self
            .table
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Delta file has no table owner"))?;
        let append = self
            .staged
            .take()
            .ok_or_else(|| anyhow::anyhow!("Delta file was not finalized"))?;
        table.submit(append, force).await?.wait().await?;
        Ok(())
    }
}

#[async_trait(?Send)]
impl FileSink for DeltaSink {
    async fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        if self.staged.is_some() {
            return Err(ParquetError::General("cannot write a finalized Delta file".into()));
        }
        if self.writer.is_none() {
            let table = self.backend.table(&self.name).await.map_err(external)?;
            self.writer = Some(table.new_file(Arc::clone(&self.schema)).await.map_err(external)?);
            self.table = Some(table);
        }
        self.writer
            .as_mut()
            .ok_or_else(|| ParquetError::General("missing Delta file writer".into()))?
            .write(batch)
            .await
            .map_err(external)
    }

    async fn close(self: Box<Self>) -> Result<(), ParquetError> {
        (*self).publish(false).await.map_err(external)
    }

    async fn stage(&mut self) -> Result<(), ParquetError> {
        self.stage_file().await.map_err(external)
    }

    async fn close_forced(self: Box<Self>) -> Result<(), ParquetError> {
        (*self).publish(true).await.map_err(external)
    }
}

impl FileSinkFactory for DeltaBackend {
    fn create(&self, path: &str, schema: SchemaRef) -> Result<Box<dyn FileSink>, ParquetError> {
        let name = path
            .split('/')
            .next()
            .filter(|name| !name.is_empty() && name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'))
            .ok_or_else(|| ParquetError::General(format!("invalid payload path {path}")))?;
        Ok(Box::new(DeltaSink {
            backend: self.clone(),
            name: name.to_string(),
            schema,
            table: None,
            writer: None,
            staged: None,
        }))
    }

    fn max_pending_files(&self) -> Option<usize> {
        Some(self.0.options.max_pending_files)
    }

    fn start_draining(&self) -> Result<(), ParquetError> {
        self.0.draining.store(true, Ordering::Release);
        let tables = self
            .0
            .tables
            .lock()
            .map_err(|_| ParquetError::General("Delta backend lock poisoned".into()))?;
        for table in tables.values() {
            table.start_draining();
        }
        Ok(())
    }

    fn prepare(&self, records: &mut OtapParquetRecords) -> Result<(), ParquetError> {
        let namespace = records
            .allowed_payload_types()
            .iter()
            .find_map(|payload| {
                records
                    .get(*payload)
                    .and_then(|batch| batch.schema_ref().metadata().get(PARTITION_METADATA_KEY).cloned())
            })
            .ok_or_else(|| ParquetError::General("missing OTEL join namespace".into()))?;
        for payload in records.allowed_payload_types() {
            if let Some(batch) = records.get(*payload) {
                if batch.schema_ref().fields().find(JOIN_NAMESPACE).is_some() {
                    return Err(ParquetError::General(format!("reserved column {JOIN_NAMESPACE}")));
                }
                let mut fields = batch.schema_ref().fields().iter().cloned().collect::<Vec<_>>();
                fields.push(Arc::new(Field::new(JOIN_NAMESPACE, DataType::Utf8, false)));
                let mut columns = batch.columns().to_vec();
                columns.push(Arc::new(StringArray::from(vec![namespace.as_str(); batch.num_rows()])));
                let schema = Arc::new(Schema::new(fields).with_metadata(batch.schema_ref().metadata().clone()));
                let batch = RecordBatch::try_new(schema, columns)?;
                records.set(*payload, batch);
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CommitConfig {
    #[serde(with = "humantime_serde")]
    batch_window: Duration,
    max_files_per_commit: usize,
    max_pending_files: usize,
}

impl Default for CommitConfig {
    fn default() -> Self {
        let options = CommitOptions::default();
        Self {
            batch_window: options.batch_window,
            max_files_per_commit: options.max_files_per_commit,
            max_pending_files: options.max_pending_files,
        }
    }
}

fn parse_config(
    value: &serde_json::Value,
) -> Result<(serde_json::Value, Config, CommitOptions), otel_arrow_dfe_config::error::Error> {
    use otel_arrow_dfe_config::error::Error::InvalidUserConfig;
    let mut shared = value.clone();
    let object = shared.as_object_mut().ok_or_else(|| InvalidUserConfig {
        error: "Delta config must be an object".into(),
    })?;
    let commit: CommitConfig = object
        .remove("commit_options")
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| InvalidUserConfig {
            error: error.to_string(),
        })?
        .unwrap_or_default();
    let options = CommitOptions {
        batch_window: commit.batch_window,
        max_files_per_commit: commit.max_files_per_commit,
        max_pending_files: commit.max_pending_files,
    };
    options.validate().map_err(|error| InvalidUserConfig {
        error: error.to_string(),
    })?;
    let config: Config = serde_json::from_value(shared.clone()).map_err(|error| InvalidUserConfig {
        error: error.to_string(),
    })?;
    #[allow(unreachable_patterns)]
    match &config.storage {
        otel_arrow_dfe_otap::object_store::StorageType::File { base_uri } => {
            if !Path::new(base_uri).is_absolute() {
                return Err(InvalidUserConfig {
                    error: "Delta base_uri must be an absolute local path".into(),
                });
            }
        }
        _ => {
            return Err(InvalidUserConfig {
                error: "deltalake supports only local file storage".into(),
            });
        }
    }
    if config.partitioning_strategies.is_some() || config.retry.is_some() {
        return Err(InvalidUserConfig {
            error: "Delta partitioning and cloud retry settings are not supported".into(),
        });
    }
    if let Some(options) = &config.writer_options
        && (options.target_rows_per_file == Some(0) || options.flush_when_older_than.is_some_and(|age| age.is_zero()))
    {
        return Err(InvalidUserConfig {
            error: "Delta flush thresholds must be positive".into(),
        });
    }
    Ok((shared, config, options))
}

fn validate(value: &serde_json::Value) -> Result<(), otel_arrow_dfe_config::error::Error> {
    parse_config(value).map(|_| ())
}

/// External registration linked by the rust-playground collector.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Exporter)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static DELTA_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: DELTA_URN,
    create: |pipeline: PipelineContext,
             node: NodeId,
             user: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig,
             _capabilities| {
        let (shared, config, options) = parse_config(&user.config)?;
        #[allow(unreachable_patterns)]
        let base = match config.storage {
            otel_arrow_dfe_otap::object_store::StorageType::File { base_uri } => base_uri,
            _ => {
                return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                    error: "non-local Delta storage".into(),
                });
            }
        };
        let exporter = ParquetExporter::from_config(pipeline, &shared)?
            .with_backend(Arc::new(DeltaBackend::with_options(base, options)));
        Ok(ExporterWrapper::local(exporter, node, user, exporter_config))
    },
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: validate,
};

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_pdata::otap::raw_batch_store::RawLogsStore;
    use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

    fn records() -> anyhow::Result<OtapParquetRecords> {
        let schema = Arc::new(
            Schema::new(vec![Field::new("id", DataType::UInt32, false)]).with_metadata(HashMap::from([(
                PARTITION_METADATA_KEY.to_string(),
                "session".to_string(),
            )])),
        );
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arrow::array::UInt32Array::from(vec![1]))])?;
        let mut store = RawLogsStore::new();
        store.set(ArrowPayloadType::Logs, batch.clone());
        store.set(ArrowPayloadType::LogAttrs, batch);
        Ok(OtapParquetRecords::Logs(store))
    }

    /// Scenario: OTAP parent and child records have a generator namespace in root metadata.
    /// Guarantees: every persisted payload carries the same join namespace and collisions fail.
    #[test]
    fn namespaces_cover_children() -> anyhow::Result<()> {
        let mut records = records()?;
        let backend = DeltaBackend::new("/unused");
        backend.prepare(&mut records)?;
        for payload in [ArrowPayloadType::Logs, ArrowPayloadType::LogAttrs] {
            let batch = records.get(payload).ok_or_else(|| anyhow::anyhow!("missing batch"))?;
            let namespace = batch
                .column_by_name(JOIN_NAMESPACE)
                .ok_or_else(|| anyhow::anyhow!("missing namespace"))?;
            let namespace = namespace
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("wrong namespace type"))?;
            assert_eq!(namespace.value(0), "session");
        }
        assert!(backend.prepare(&mut records).is_err());
        Ok(())
    }

    /// Scenario: callers configure remote locations or unsupported partitioning/retry settings.
    /// Guarantees: invalid configuration is rejected before accepting telemetry.
    #[test]
    fn validates_local_scope() {
        assert!(validate(&serde_json::json!({"storage": {"file": {"base_uri": "/tmp/output"}}})).is_ok());
        assert!(validate(&serde_json::json!({"storage": {"file": {"base_uri": "s3://bucket"}}})).is_err());
        assert!(
            validate(&serde_json::json!({"storage": {"file": {"base_uri": "/tmp/output"}},
            "partitioning_strategies": []}))
            .is_err()
        );
        assert!(
            validate(&serde_json::json!({"storage": {"file": {"base_uri": "/tmp/output"}},
            "writer_options": {"target_rows_per_file": 0}}))
            .is_err()
        );
    }

    /// Scenario: the injected backend receives an ordinary prepared payload batch.
    /// Guarantees: it writes and commits through the generic SDK without using delta-rs.
    #[tokio::test]
    async fn backend_commits_payload() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let backend = DeltaBackend::new(directory.path());
        let mut records = records()?;
        backend.prepare(&mut records)?;
        let batch = records
            .get(ArrowPayloadType::Logs)
            .ok_or_else(|| anyhow::anyhow!("missing logs"))?;
        let mut sink = backend.create("logs//part.parquet", batch.schema())?;
        sink.write(batch).await?;
        sink.close_forced().await?;
        assert!(
            directory
                .path()
                .join("logs/_delta_log/00000000000000000001.json")
                .exists()
        );
        assert!(backend.create("../part.parquet", batch.schema()).is_err());
        Ok(())
    }

    /// Scenario: two exporter instances finalize files for the same local payload table.
    /// Guarantees: table ownership is shared and one Delta transaction publishes both files.
    #[tokio::test]
    async fn exporters_share_batched_publication() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let options = CommitOptions {
            max_files_per_commit: 2,
            max_pending_files: 4,
            ..Default::default()
        };
        let left = DeltaBackend::with_options(directory.path(), options.clone());
        let right = DeltaBackend::with_options(directory.path(), options);
        let mut records = records()?;
        left.prepare(&mut records)?;
        let batch = records
            .get(ArrowPayloadType::Logs)
            .ok_or_else(|| anyhow::anyhow!("missing logs"))?;
        let mut a = left.create("logs/a.parquet", batch.schema())?;
        let mut b = right.create("logs/b.parquet", batch.schema())?;
        a.write(batch).await?;
        b.write(batch).await?;
        a.stage().await?;
        b.stage().await?;
        assert!(
            !directory
                .path()
                .join("logs/_delta_log/00000000000000000001.json")
                .exists()
        );
        let (a, b) = tokio::time::timeout(Duration::from_secs(3), async { tokio::join!(a.close(), b.close()) }).await?;
        a?;
        b?;
        let log = std::fs::read_to_string(directory.path().join("logs/_delta_log/00000000000000000001.json"))?;
        assert_eq!(log.lines().filter(|line| line.contains("\"add\":")).count(), 2);
        Ok(())
    }

    /// Scenario: root-owned commit settings are supplied alongside the shared file configuration.
    /// Guarantees: the fork receives only its own fields and malformed or misspelled limits fail early.
    #[test]
    fn commit_config_is_root_owned_and_strict() -> anyhow::Result<()> {
        let value = serde_json::json!({
            "storage": {"file": {"base_uri": "/tmp/output"}},
            "commit_options": {"batch_window": "10s", "max_files_per_commit": 8, "max_pending_files": 16}
        });
        let (shared, _, options) = parse_config(&value)?;
        assert!(shared.get("commit_options").is_none());
        assert_eq!(options.batch_window, Duration::from_secs(10));
        assert_eq!(options.max_files_per_commit, 8);
        for invalid in [
            serde_json::json!({"batch_window": "0s"}),
            serde_json::json!({"max_files_per_commit": 0}),
            serde_json::json!({"max_pending_files": 1}),
            serde_json::json!({"batch_windows": "1s"}),
        ] {
            let mut value = value.clone();
            value["commit_options"] = invalid;
            assert!(validate(&value).is_err());
        }
        Ok(())
    }

    /// Scenario: OTAP contains histogram/summary datapoints and spans with events and binary links.
    /// Guarantees: upstream preparation and the Delta backend preserve nested counts, quantiles, timestamps, and link bytes.
    #[tokio::test]
    async fn rich_payloads_reuse_upstream_preparation() -> anyhow::Result<()> {
        use arrow::array::{
            ArrayRef, BinaryArray, Decimal128Array, FixedSizeBinaryArray, Float64Array, ListArray, ListBuilder,
            StructArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt64Builder,
        };
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use otel_arrow_dfe_core_nodes::exporters::parquet_exporter::{
            config::WriterOptions,
            idgen::PartitionSequenceIdGenerator,
            schema::transform_to_known_schema,
            writer::{WriteBatch, WriterManager},
        };
        use otel_arrow_dfe_pdata::{
            otap::raw_batch_store::{RawMetricsStore, RawTracesStore},
            schema::consts,
        };
        let directory = tempfile::tempdir()?;
        let backend = Arc::new(DeltaBackend::new(directory.path()));
        let mut counts = ListBuilder::new(UInt64Builder::new());
        counts.values().append_value(u64::MAX);
        counts.values().append_value(0);
        counts.append(true);
        let histogram = RecordBatch::try_from_iter(vec![
            (consts::ID, Arc::new(UInt32Array::from(vec![1])) as ArrayRef),
            (consts::PARENT_ID, Arc::new(UInt16Array::from(vec![0])) as ArrayRef),
            (
                consts::HISTOGRAM_COUNT,
                Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
            ),
            (consts::HISTOGRAM_BUCKET_COUNTS, Arc::new(counts.finish()) as ArrayRef),
        ])?;
        let fields: arrow::datatypes::Fields = vec![
            Field::new(consts::SUMMARY_QUANTILE, DataType::Float64, false),
            Field::new(consts::SUMMARY_VALUE, DataType::Float64, false),
        ]
        .into();
        let values = StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(Float64Array::from(vec![0.5])),
                Arc::new(Float64Array::from(vec![42.0])),
            ],
            None,
        )?;
        let quantiles = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Struct(fields), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(values),
            None,
        )?;
        let summary = RecordBatch::try_from_iter(vec![
            (consts::ID, Arc::new(UInt32Array::from(vec![2])) as ArrayRef),
            (consts::PARENT_ID, Arc::new(UInt16Array::from(vec![0])) as ArrayRef),
            (consts::SUMMARY_QUANTILE_VALUES, Arc::new(quantiles) as ArrayRef),
        ])?;
        let mut metrics = RawMetricsStore::new();
        metrics.set(
            ArrowPayloadType::UnivariateMetrics,
            RecordBatch::try_from_iter(vec![(consts::ID, Arc::new(UInt16Array::from(vec![0])) as ArrayRef)])?,
        );
        metrics.set(ArrowPayloadType::HistogramDataPoints, histogram);
        metrics.set(ArrowPayloadType::SummaryDataPoints, summary);
        let mut traces = RawTracesStore::new();
        traces.set(
            ArrowPayloadType::Spans,
            RecordBatch::try_from_iter(vec![(consts::ID, Arc::new(UInt16Array::from(vec![0])) as ArrayRef)])?,
        );
        traces.set(
            ArrowPayloadType::SpanEvents,
            RecordBatch::try_from_iter(vec![
                (consts::ID, Arc::new(UInt32Array::from(vec![1])) as ArrayRef),
                (consts::PARENT_ID, Arc::new(UInt16Array::from(vec![0])) as ArrayRef),
                (
                    consts::TIME_UNIX_NANO,
                    Arc::new(TimestampNanosecondArray::from(vec![-1])) as ArrayRef,
                ),
            ])?,
        );
        traces.set(
            ArrowPayloadType::SpanLinks,
            RecordBatch::try_from_iter(vec![
                (consts::ID, Arc::new(UInt32Array::from(vec![2])) as ArrayRef),
                (consts::PARENT_ID, Arc::new(UInt16Array::from(vec![0])) as ArrayRef),
                (
                    consts::TRACE_ID,
                    Arc::new(FixedSizeBinaryArray::try_from_iter([[128; 16]].into_iter())?) as ArrayRef,
                ),
                (
                    consts::SPAN_ID,
                    Arc::new(FixedSizeBinaryArray::try_from_iter([[255; 8]].into_iter())?) as ArrayRef,
                ),
            ])?,
        );
        let mut manager = WriterManager::new(
            otel_arrow_dfe_otap::object_store::from_storage_type_with_retry_and_token_provider(
                &otel_arrow_dfe_otap::object_store::StorageType::File {
                    base_uri: directory.path().to_string_lossy().into_owned(),
                },
                None,
                None,
            )?,
            WriterOptions {
                target_rows_per_file: Some(1),
                flush_when_older_than: None,
            },
        )
        .with_backend(backend.clone());
        for (index, mut records) in [OtapParquetRecords::Metrics(metrics), OtapParquetRecords::Traces(traces)]
            .into_iter()
            .enumerate()
        {
            PartitionSequenceIdGenerator::new().generate_unique_ids(&mut records)?;
            transform_to_known_schema(&mut records)?;
            backend.prepare(&mut records)?;
            let _ = manager
                .write(&[WriteBatch::new(i64::try_from(index)?, &records, None)])
                .await?;
        }
        let _ = manager.flush_all().await?;
        let read = |name: &str| -> anyhow::Result<RecordBatch> {
            let path = std::fs::read_dir(directory.path().join(name))?
                .map(|entry| entry.map(|entry| entry.path()))
                .collect::<std::io::Result<Vec<_>>>()?
                .into_iter()
                .find(|path| path.extension().is_some_and(|value| value == "parquet"))
                .ok_or_else(|| anyhow::anyhow!("missing payload file"))?;
            let mut reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?
                    .build()?;
            Ok(reader.next().ok_or_else(|| anyhow::anyhow!("missing rows"))??)
        };
        let histogram = read("histogram_data_points")?;
        let count = histogram
            .column_by_name(consts::HISTOGRAM_COUNT)
            .and_then(|column| column.as_any().downcast_ref::<Decimal128Array>())
            .ok_or_else(|| anyhow::anyhow!("not decimal count"))?;
        assert_eq!(count.value(0), i128::from(u64::MAX));
        let summary = read("summary_data_points")?;
        let quantiles = summary
            .column_by_name(consts::SUMMARY_QUANTILE_VALUES)
            .and_then(|column| column.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| anyhow::anyhow!("not quantile list"))?;
        let values = quantiles
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| anyhow::anyhow!("not quantile struct"))?;
        assert_eq!(
            values
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("not quantile value"))?
                .value(0),
            42.0
        );
        let events = read("span_events")?;
        assert_eq!(
            events
                .column_by_name(&format!(
                    "{}{suffix}",
                    consts::TIME_UNIX_NANO,
                    suffix = arrow_lake::normalize::NANOS_SUFFIX
                ))
                .and_then(|column| column.as_any().downcast_ref::<arrow::array::Int64Array>())
                .ok_or_else(|| anyhow::anyhow!("missing exact event timestamp"))?
                .value(0),
            -1
        );
        let links = read("span_links")?;
        assert_eq!(
            links
                .column_by_name(consts::TRACE_ID)
                .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(|| anyhow::anyhow!("missing trace bytes"))?
                .value(0),
            &[128; 16]
        );
        Ok(())
    }
}
