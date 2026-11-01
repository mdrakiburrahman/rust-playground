use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::commit::CommitOptions;
use arrow_deltalake::sizing::AdaptiveFileSizeConfig;
use arrow_deltalake::{CompletedFileObservation, record_batch_logical_bytes};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::{FlightData, FlightDescriptor};
use arrowcol_buffer::{
    BufferConfig, BufferManager, CapacityLimits, FlushReason, PublicationContext, PublicationOutcome,
    PublicationSession, PublicationSink, QuiverStage, QuiverStageConfig, SinkError, StageLeases,
};
use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest, MemoryCatalogStore,
    MemoryMetastore,
};
use arrowcol_flight::{AllowAllAuthenticator, ArrowColFlightService, PutResultMetadata, RuntimeLimits};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, CatalogName, ClaimedSchemaFingerprint, CollectorInstanceId,
    DeltaPublicationResult, FencingToken, ImplementationPool, IngestMetadata, LeaseId, Measurement, PrincipalId,
    PrincipalIssuer, PrincipalScope, ProtocolVersion, ReceiptId, ReceiptState, SchemaName, TableIdentity, TableLease,
    TableName, UnixMillis, fingerprint_schema,
};
use arrowcol_receipts::{
    OwnerDrain, OwnerRegistration, PublicationBatch, ReceiptAdoption, ReceiptAdoptionReport, ReceiptError,
    ReceiptErrorKind, ReceiptFailure, ReceiptRecord, ReceiptResult, ReceiptStore, ReceiptVersion, ReconciliationReport,
    RejectedAdoption, TableLeaseRequest, ValidatedDelivery,
};
use arrowcol_routing::{
    CollectorState, LeaseCas, LeaseClaim, LeaseKey, LeaseState, LeaseTransfer, RoutingLease, RoutingResult,
    RoutingStore, RoutingView,
};
use arrowcol_rust::control::TableActivity;
use arrowcol_rust::delta_sink::ProductionDeltaSink;
use arrowcol_rust::ingest::DirectAdmission;
use arrowcol_rust::lease::TableLeaseManager;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
async fn malicious_schema_is_rejected_before_any_receipt() -> Result<()> {
    let fixture = Fixture::new(8).await?;
    let malicious = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&malicious),
        vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2]))],
    )?;
    let request = ingest_request(
        fixture.identity.clone(),
        malicious.as_ref(),
        vec![batch],
        fingerprint_schema(malicious.as_ref()),
    )
    .await?;

    let status = match fixture.service.handle_do_put(request).await {
        Ok(_) => return Err(anyhow!("breaking schema was admitted")),
        Err(status) => status,
    };
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(fixture.receipts.created_count(), 0);
    fixture.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn identical_batches_receive_distinct_results_and_publish_together() -> Result<()> {
    let fixture = Fixture::new(8).await?;
    let batch = fixture.batch(1)?;
    let request = ingest_request(
        fixture.identity.clone(),
        fixture.schema.as_ref(),
        vec![batch.clone(), batch],
        fingerprint_schema(fixture.schema.as_ref()),
    )
    .await?;
    let results = fixture
        .service
        .handle_do_put(request)
        .await?
        .into_inner()
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(results.len(), 2);
    let ids = results
        .iter()
        .map(PutResultMetadata::from_put_result)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|metadata| metadata.receipt_id().context("missing receipt"))
        .collect::<Result<Vec<_>>>()?;
    assert_ne!(ids[0], ids[1]);
    assert_eq!(fixture.receipts.created_count(), 2);
    let before = fixture.receipts.receipts(&ids).await?;
    assert!(before.iter().flatten().all(|record| !record.can_client_purge()));

    fixture.buffer.drain().await?;
    let after = fixture.receipts.receipts(&ids).await?;
    assert!(after.iter().flatten().all(ReceiptRecord::can_client_purge));
    let groups = fixture.receipts.publication_groups().await;
    assert_eq!(groups, ids.into_iter().map(|id| vec![id]).collect::<Vec<_>>());
    fixture.shutdown().await?;
    Ok(())
}

/// Scenario: Native Quiver is enabled and a client sends two batches over Flight.
/// Guarantees: Each receipt is returned only after its batch is persisted to
/// disk, and the batches are then published from Quiver so both receipts reach
/// `published` and the persisted backlog drains.
#[tokio::test]
async fn quiver_persists_before_receipt_and_publishes_from_disk() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let fixture = Fixture::with_options(
        8,
        64,
        Duration::from_secs(5),
        Arc::new(FakeSink::default()),
        Some(directory.path()),
    )
    .await?;
    let stage = Arc::clone(fixture.quiver.as_ref().context("Quiver stage missing")?);
    let request = ingest_request(
        fixture.identity.clone(),
        fixture.schema.as_ref(),
        vec![fixture.batch(1)?, fixture.batch(3)?],
        fingerprint_schema(fixture.schema.as_ref()),
    )
    .await?;
    let results = fixture
        .service
        .handle_do_put(request)
        .await?
        .into_inner()
        .try_collect::<Vec<_>>()
        .await?;
    let ids = results
        .iter()
        .map(PutResultMetadata::from_put_result)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|metadata| metadata.receipt_id().context("missing receipt"))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(ids.len(), 2);
    assert_eq!(stage.snapshot().persisted_bundles, 2);

    stage.start();
    let mut published = false;
    for _ in 0..500 {
        fixture.buffer.drain().await?;
        let records = fixture.receipts.receipts(&ids).await?;
        if records
            .iter()
            .flatten()
            .filter(|record| record.can_client_purge())
            .count()
            == ids.len()
        {
            published = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(published, "persisted batches were not published from disk");
    assert!(
        stage.drain(Duration::from_secs(5)).await,
        "Quiver backlog did not drain"
    );
    assert_eq!(stage.snapshot().published_bundles, 2);
    fixture.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn bounded_capacity_returns_backpressure_after_one_incremental_receipt() -> Result<()> {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let sink: Arc<dyn PublicationSink> = Arc::new(BlockingSink {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    });
    let fixture = Fixture::with_options(1, 1, Duration::from_millis(100), sink, None).await?;
    let request = ingest_request(
        fixture.identity.clone(),
        fixture.schema.as_ref(),
        vec![fixture.batch(1)?, fixture.batch(10)?],
        fingerprint_schema(fixture.schema.as_ref()),
    )
    .await?;
    let mut results = fixture.service.handle_do_put(request).await?.into_inner();
    let first = results.next().await.context("missing first result")??;
    assert!(PutResultMetadata::from_put_result(&first)?.receipt_id().is_some());
    started.notified().await;
    let status = match results.next().await.context("missing backpressure status")? {
        Ok(_) => return Err(anyhow!("second batch was not rejected")),
        Err(status) => status,
    };
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    release.notify_waiters();
    fixture.buffer.drain().await?;
    fixture.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn production_delta_sink_is_visible_to_a_parquet_reader() -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let identity = TableIdentity::new(
        CatalogName::new("telemetry")?,
        SchemaName::new("prod")?,
        TableName::new("reader_events")?,
    );
    let catalog = Arc::new(MemoryCatalogStore::new());
    let control = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&catalog),
        Arc::new(MemoryMetastore::new()),
    );
    control
        .execute_ddl(FlightSqlDdlRequest::new(
            "CREATE TABLE telemetry.prod.reader_events (id BIGINT NOT NULL, body VARCHAR)",
        ))
        .await?;
    let current = catalog
        .current_schema(&identity)
        .await?
        .context("missing reader schema")?;
    let schema = Arc::new(current.arrow_schema()?);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![7, 8])),
            Arc::new(StringArray::from(vec!["seven", "eight"])),
        ],
    )?;
    let owner = CollectorInstanceId::new();
    let receipts: Arc<dyn ReceiptStore> = Arc::new(FakeReceipts::default());
    let leases = Arc::new(TableLeaseManager::new(
        Arc::clone(&receipts),
        Arc::new(FakeRouting::owned(owner, identity.clone())?),
        owner,
        Duration::from_secs(60),
    ));
    let lease = leases.lease_for(current.table_id(), &identity).await?.current()?;
    let root = std::path::PathBuf::from(format!("projects/arrowcol-rust/.data/tests/{}", Uuid::new_v4()));
    let sink = ProductionDeltaSink::new(
        root.clone(),
        CommitOptions {
            batch_window: Duration::from_millis(10),
            max_files_per_commit: 1,
            max_pending_files: 4,
        },
        Arc::clone(&leases),
        arrowcol_rust::metrics::CollectorMetrics::new()?,
    );
    let context = PublicationContext {
        receipt_id: ReceiptId::new(),
        ingested_at_ms: now()?,
        table: identity.clone(),
        schema_version: current.version(),
        schema_fingerprint: current.fingerprint(),
        arrow_schema: Arc::clone(&schema),
        fence: lease.publication_fence(),
        flush_reason: FlushReason::ExplicitDrain,
    };
    let mut session = sink.begin(context.clone()).await?;
    session.write(&batch).await?;
    let outcome = session.finish().await?;
    assert_eq!(outcome.publication.rows_published().get(), 2);
    // A replay of this publication is skipped, but a separate submission of
    // the same rows must retain its independent receipt identity.
    let mut replay = sink.begin(context.clone()).await?;
    replay.write(&batch).await?;
    replay.finish().await?;
    let mut independent = context;
    independent.receipt_id = ReceiptId::new();
    let mut duplicate_payload = sink.begin(independent).await?;
    duplicate_payload.write(&batch).await?;
    duplicate_payload.finish().await?;

    let table_root = sink.table_path(&identity);
    let mut logs = std::fs::read_dir(table_root.join("_delta_log"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "json"))
        .collect::<Vec<_>>();
    logs.sort();
    let mut data_path = None;
    let mut published_files = 0;
    for log in logs {
        for line in std::fs::read_to_string(log)?.lines() {
            let value: serde_json::Value = serde_json::from_str(line)?;
            if let Some(path) = value
                .get("add")
                .and_then(|add| add.get("path"))
                .and_then(serde_json::Value::as_str)
            {
                published_files += 1;
                data_path = Some(table_root.join(path));
            }
        }
    }
    assert_eq!(
        published_files, 2,
        "one replay was deduplicated; independent equal payload was retained"
    );
    let file = std::fs::File::open(data_path.context("Delta log contained no add action")?)?;
    let rows = ParquetRecordBatchReaderBuilder::try_new(file)?
        .build()?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let ids = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("id column is not Int64")?;
    assert_eq!(ids.values(), &[7, 8]);

    leases.release_all().await?;
    drop(sink);
    drop(leases);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn lost_ownership_fences_delta_before_visibility() -> Result<()> {
    let identity = TableIdentity::new(
        CatalogName::new("telemetry")?,
        SchemaName::new("prod")?,
        TableName::new("fenced_events")?,
    );
    let catalog = Arc::new(MemoryCatalogStore::new());
    let control = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&catalog),
        Arc::new(MemoryMetastore::new()),
    );
    control
        .execute_ddl(FlightSqlDdlRequest::new(
            "CREATE TABLE telemetry.prod.fenced_events (id BIGINT NOT NULL, body VARCHAR)",
        ))
        .await?;
    let current = catalog
        .current_schema(&identity)
        .await?
        .context("missing fenced schema")?;
    let schema = Arc::new(current.arrow_schema()?);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["one"])),
        ],
    )?;
    let owner = CollectorInstanceId::new();
    let receipts: Arc<dyn ReceiptStore> = Arc::new(FakeReceipts::default());
    let leases = Arc::new(TableLeaseManager::new(
        Arc::clone(&receipts),
        Arc::new(FakeRouting::owned(owner, identity.clone())?),
        owner,
        Duration::from_secs(60),
    ));
    let lease = leases.lease_for(current.table_id(), &identity).await?.current()?;
    let root = std::path::PathBuf::from(format!("projects/arrowcol-rust/.data/tests/{}", Uuid::new_v4()));
    let sink = ProductionDeltaSink::new(
        root.clone(),
        CommitOptions {
            batch_window: Duration::from_millis(10),
            max_files_per_commit: 1,
            max_pending_files: 4,
        },
        Arc::clone(&leases),
        arrowcol_rust::metrics::CollectorMetrics::new()?,
    );
    let mut session = sink
        .begin(PublicationContext {
            receipt_id: ReceiptId::new(),
            ingested_at_ms: now()?,
            table: identity,
            schema_version: current.version(),
            schema_fingerprint: current.fingerprint(),
            arrow_schema: schema,
            fence: lease.publication_fence(),
            flush_reason: FlushReason::ExplicitDrain,
        })
        .await?;
    session.write(&batch).await?;
    leases.invalidate_all("injected ownership loss").await;
    let error = match session.finish().await {
        Ok(_) => return Err(anyhow!("stale owner published successfully")),
        Err(error) => error,
    };
    assert_eq!(error.kind(), arrowcol_buffer::SinkErrorKind::Fenced);
    drop(sink);
    drop(leases);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

struct Fixture {
    identity: TableIdentity,
    schema: Arc<Schema>,
    owner: CollectorInstanceId,
    receipts: Arc<FakeReceipts>,
    buffer: Arc<BufferManager>,
    service: ArrowColFlightService,
    leases: Arc<TableLeaseManager>,
    quiver: Option<Arc<QuiverStage>>,
}

impl Fixture {
    async fn new(max_batches_per_flush: u64) -> Result<Self> {
        Self::with_options(
            max_batches_per_flush,
            64,
            Duration::from_secs(5),
            Arc::new(FakeSink::default()),
            None,
        )
        .await
    }

    async fn with_options(
        max_batches_per_flush: u64,
        capacity_batches: u64,
        admission_timeout: Duration,
        sink: Arc<dyn PublicationSink>,
        quiver_directory: Option<&std::path::Path>,
    ) -> Result<Self> {
        let identity = TableIdentity::new(
            CatalogName::new("telemetry")?,
            SchemaName::new("prod")?,
            TableName::new("events")?,
        );
        let catalog = Arc::new(MemoryCatalogStore::new());
        let metastore = Arc::new(MemoryMetastore::new());
        let control = CatalogControlPlane::new(
            DdlParser::default(),
            Arc::<MemoryCatalogStore>::clone(&catalog),
            metastore,
        );
        control
            .execute_ddl(FlightSqlDdlRequest::new(
                "CREATE TABLE telemetry.prod.events (id BIGINT NOT NULL, body VARCHAR)",
            ))
            .await?;
        let current = catalog.current_schema(&identity).await?.context("missing schema")?;
        let schema = Arc::new(current.arrow_schema()?);
        let owner = CollectorInstanceId::new();
        let routing: Arc<dyn RoutingStore> = Arc::new(FakeRouting::owned(owner, identity.clone())?);
        let receipts = Arc::new(FakeReceipts::default());
        let receipt_store: Arc<dyn ReceiptStore> = Arc::<FakeReceipts>::clone(&receipts);
        let leases = Arc::new(TableLeaseManager::new(
            Arc::clone(&receipt_store),
            routing,
            owner,
            Duration::from_secs(60),
        ));
        let buffer = BufferManager::new(
            BufferConfig {
                global_capacity: CapacityLimits::new(32 * 1024 * 1024, 100_000, capacity_batches)?,
                per_table_capacity: CapacityLimits::new(16 * 1024 * 1024, 50_000, capacity_batches)?,
                max_arrow_memory_per_flush: 16 * 1024 * 1024,
                max_rows_per_flush: 50_000,
                max_batches_per_flush,
                max_age: Duration::from_secs(60),
                age_check_interval: Duration::from_secs(1),
                max_concurrent_flushes: 2,
                receipt_lease_duration: Duration::from_secs(30),
                adaptive_size: AdaptiveFileSizeConfig {
                    target_parquet_bytes: 128 * 1024 * 1024,
                    min_input_bytes: 8 * 1024 * 1024,
                    max_input_bytes: 64 * 1024 * 1024,
                    ..AdaptiveFileSizeConfig::default()
                },
            },
            Arc::clone(&receipt_store),
            sink,
        )?;
        let catalog_store: Arc<dyn CatalogStore> = catalog;
        let mut direct = DirectAdmission::new(
            Arc::clone(&catalog_store),
            Arc::clone(&receipt_store),
            Arc::clone(&buffer),
            Arc::clone(&leases),
            Arc::new(TableActivity::default()),
            owner,
            Duration::from_secs(30),
            admission_timeout,
        );
        let quiver = match quiver_directory {
            Some(directory) => {
                let stage_leases: Arc<dyn StageLeases> = Arc::<TableLeaseManager>::clone(&leases);
                let stage = QuiverStage::open(
                    QuiverStageConfig {
                        directory: directory.to_path_buf(),
                        capacity_bytes: 256 * 1024 * 1024,
                        wal_flush_interval: Duration::ZERO,
                        segment_max_open: Duration::from_millis(20),
                        max_in_flight: 16,
                        retry_initial: Duration::from_millis(10),
                        retry_max: Duration::from_millis(100),
                    },
                    Arc::clone(&buffer),
                    Arc::clone(&receipt_store),
                    stage_leases,
                    owner,
                    Duration::from_secs(30),
                )
                .await?;
                direct = direct.with_quiver(Arc::clone(&stage));
                Some(stage)
            }
            None => None,
        };
        let admission = Arc::new(direct);
        let service = ArrowColFlightService::new(
            ImplementationPool::Direct,
            catalog_store,
            Arc::new(AllowAllAuthenticator::new(auth_context()?)),
            admission,
            RuntimeLimits::default(),
        )?;
        Ok(Self {
            identity,
            schema,
            owner,
            receipts,
            buffer,
            service,
            leases,
            quiver,
        })
    }

    fn batch(&self, offset: i64) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(Int64Array::from(vec![offset, offset + 1])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )?)
    }

    async fn shutdown(self) -> Result<()> {
        if let Some(stage) = &self.quiver {
            stage.shutdown().await;
        }
        let report = self.buffer.shutdown().await?;
        assert!(report.incomplete.is_empty());
        self.leases.release_all().await?;
        let _ = self.owner;
        Ok(())
    }
}

struct BlockingSink {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl PublicationSink for BlockingSink {
    async fn begin(&self, context: PublicationContext) -> Result<Box<dyn PublicationSession>, SinkError> {
        Ok(Box::new(BlockingSession {
            context,
            started: Arc::clone(&self.started),
            release: Arc::clone(&self.release),
            rows: 0,
            bytes: 0,
        }))
    }
}

struct BlockingSession {
    context: PublicationContext,
    started: Arc<Notify>,
    release: Arc<Notify>,
    rows: usize,
    bytes: u64,
}

#[async_trait]
impl PublicationSession for BlockingSession {
    async fn write(&mut self, batch: &RecordBatch) -> Result<(), SinkError> {
        self.rows += batch.num_rows();
        self.bytes += record_batch_logical_bytes(batch).map_err(|error| SinkError::retryable(error.to_string()))?;
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<PublicationOutcome, SinkError> {
        self.started.notify_one();
        self.release.notified().await;
        let rows = u64::try_from(self.rows).map_err(|_| SinkError::publication_unknown("row overflow"))?;
        Ok(PublicationOutcome {
            publication: DeltaPublicationResult::new(
                Measurement::new(1).map_err(|error| SinkError::publication_unknown(error.to_string()))?,
                Measurement::new(1).map_err(|error| SinkError::publication_unknown(error.to_string()))?,
                Measurement::new(rows).map_err(|error| SinkError::publication_unknown(error.to_string()))?,
                Measurement::new(self.bytes).map_err(|error| SinkError::publication_unknown(error.to_string()))?,
                self.context.schema_fingerprint,
                now().map_err(|error| SinkError::publication_unknown(error.to_string()))?,
                self.context.fence,
            ),
            file_observation: CompletedFileObservation {
                rows: self.rows,
                input_bytes: self.bytes,
                parquet_bytes: self.bytes.max(1),
            },
        })
    }
}

async fn ingest_request(
    identity: TableIdentity,
    schema: &Schema,
    batches: Vec<RecordBatch>,
    claimed: arrowcol_protocol::SchemaFingerprint,
) -> Result<Request<impl futures::Stream<Item = Result<FlightData, tonic::Status>> + Send + 'static>> {
    let current = ProtocolVersion::CURRENT;
    let descriptor = FlightDescriptor::new_path(vec![
        identity.catalog().to_string(),
        identity.schema().to_string(),
        identity.table().to_string(),
        "v1".to_owned(),
    ]);
    let encoded = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(descriptor))
        .build(stream::iter(batches.into_iter().map(Ok)))
        .map(|item| item.map_err(|error| tonic::Status::internal(error.to_string())))
        .collect::<Vec<_>>()
        .await;
    let mut request = Request::new(stream::iter(encoded));
    let metadata = IngestMetadata::new(
        current,
        ImplementationPool::Direct,
        identity,
        ClaimedSchemaFingerprint::new(claimed),
    );
    for (key, value) in metadata.to_ascii_pairs() {
        request
            .metadata_mut()
            .insert(key, value.parse().map_err(|_| anyhow!("invalid test metadata"))?);
    }
    let _ = schema;
    Ok(request)
}

fn auth_context() -> Result<AuthContext> {
    let scopes = ["arrowcol/ingest", "arrowcol/catalog"]
        .into_iter()
        .map(PrincipalScope::new)
        .collect::<Result<_, _>>()?;
    let principal = AuthenticatedPrincipal::new(PrincipalId::new("test")?, PrincipalIssuer::new("tests")?, scopes)?;
    Ok(AuthContext::new(principal, AuthMethod::WorkloadIdentity, now()?, None)?)
}

#[derive(Default)]
struct FakeReceipts {
    records: AsyncMutex<HashMap<ReceiptId, ReceiptRecord>>,
    publications: AsyncMutex<Vec<Vec<ReceiptId>>>,
    leases: AsyncMutex<HashMap<TableIdentity, TableLease>>,
    fence: AtomicU64,
    created: AtomicU64,
}

impl FakeReceipts {
    fn created_count(&self) -> u64 {
        self.created.load(Ordering::Acquire)
    }

    async fn publication_groups(&self) -> Vec<Vec<ReceiptId>> {
        self.publications.lock().await.clone()
    }

    async fn transition(
        &self,
        receipts: &[ReceiptVersion],
        from: ReceiptState,
        to: ReceiptState,
        deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        let mut records = self.records.lock().await;
        let mut output = Vec::with_capacity(receipts.len());
        for expected in receipts {
            let record = records
                .get_mut(&expected.receipt_id)
                .ok_or_else(|| ReceiptError::new(ReceiptErrorKind::NotFound, "receipt missing"))?;
            if record.revision != expected.revision || record.state != from {
                return Err(ReceiptError::new(ReceiptErrorKind::Conflict, "transition conflict"));
            }
            record.state = to;
            record.revision += 1;
            record.lease_deadline_unix_ms = deadline;
            record.heartbeat_at_unix_ms = now().map_err(receipt_error)?;
            record.updated_at_unix_ms = record.heartbeat_at_unix_ms;
            output.push(record.clone());
        }
        Ok(output)
    }
}

#[async_trait]
impl ReceiptStore for FakeReceipts {
    async fn register_owner(&self, _registration: OwnerRegistration) -> ReceiptResult<()> {
        Ok(())
    }

    async fn heartbeat_owner(&self, _owner: CollectorInstanceId, _lease_deadline: UnixMillis) -> ReceiptResult<()> {
        Ok(())
    }

    async fn start_owner_drain(&self, _owner: CollectorInstanceId) -> ReceiptResult<OwnerDrain> {
        Ok(OwnerDrain {
            active_receipts: 0,
            active_table_leases: 0,
        })
    }

    async fn finish_owner_drain(&self, _owner: CollectorInstanceId, _stopped_at: UnixMillis) -> ReceiptResult<()> {
        Ok(())
    }

    async fn acquire_table_lease(&self, request: TableLeaseRequest) -> ReceiptResult<TableLease> {
        let token = self.fence.fetch_add(1, Ordering::AcqRel) + 1;
        let lease = TableLease::new(
            LeaseId::new(),
            request.table.clone(),
            request.owner_instance_id,
            request.implementation_pool,
            FencingToken::new(token).map_err(receipt_error)?,
            request.issued_at_unix_ms,
            request.expires_at_unix_ms,
        )
        .map_err(receipt_error)?;
        self.leases.lock().await.insert(request.table, lease.clone());
        Ok(lease)
    }

    async fn renew_table_lease(&self, lease: &TableLease, expires_at: UnixMillis) -> ReceiptResult<TableLease> {
        let renewed = TableLease::new(
            lease.lease_id(),
            lease.table().clone(),
            lease.holder_instance_id(),
            lease.implementation_pool(),
            lease.fencing_token(),
            lease.issued_at_unix_ms(),
            expires_at,
        )
        .map_err(receipt_error)?;
        self.leases.lock().await.insert(lease.table().clone(), renewed.clone());
        Ok(renewed)
    }

    async fn release_table_lease(&self, lease: &TableLease, _released_at: UnixMillis) -> ReceiptResult<()> {
        self.leases.lock().await.remove(lease.table());
        Ok(())
    }

    async fn create_flight_batch(
        &self,
        owner: CollectorInstanceId,
        lease: &TableLease,
        lease_deadline: UnixMillis,
        deliveries: &[ValidatedDelivery],
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        let timestamp = now().map_err(receipt_error)?;
        let mut records = self.records.lock().await;
        let mut output = Vec::with_capacity(deliveries.len());
        for delivery in deliveries {
            let receipt_id = ReceiptId::new();
            let record = ReceiptRecord {
                receipt_id,
                table_id: delivery.table_id(),
                table: delivery.table().clone(),
                implementation_pool: ImplementationPool::Direct,
                owner_instance_id: owner,
                lease_id: lease.lease_id(),
                fencing_epoch: lease.fencing_token(),
                schema_version: delivery.schema_version(),
                schema_fingerprint: delivery.schema_fingerprint(),
                state: ReceiptState::Accepted,
                failure_disposition: None,
                revision: 0,
                rows: delivery.rows(),
                batches: delivery.batches(),
                arrow_memory_bytes: delivery.arrow_memory_bytes(),
                lease_deadline_unix_ms: lease_deadline,
                heartbeat_at_unix_ms: timestamp,
                created_at_unix_ms: timestamp,
                updated_at_unix_ms: timestamp,
                publication: None,
                error: None,
            };
            records.insert(receipt_id, record.clone());
            output.push(record);
            self.created.fetch_add(1, Ordering::AcqRel);
        }
        Ok(output)
    }

    async fn mark_buffered(
        &self,
        _owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.transition(receipts, ReceiptState::Accepted, ReceiptState::Buffered, lease_deadline)
            .await
    }

    async fn mark_flushing(
        &self,
        _owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.transition(
            receipts,
            ReceiptState::Buffered,
            ReceiptState::Publishing,
            lease_deadline,
        )
        .await
    }

    async fn heartbeat_receipts(
        &self,
        _owner: CollectorInstanceId,
        _receipts: &[ReceiptVersion],
        _lease_deadline: UnixMillis,
    ) -> ReceiptResult<()> {
        Ok(())
    }

    async fn publish_commit(
        &self,
        _owner: CollectorInstanceId,
        batch: &PublicationBatch,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        let mut records = self.records.lock().await;
        let mut output = Vec::with_capacity(batch.receipts.len());
        for expected in &batch.receipts {
            let record = records
                .get_mut(&expected.receipt_id)
                .ok_or_else(|| ReceiptError::new(ReceiptErrorKind::NotFound, "receipt missing"))?;
            if record.revision != expected.revision || record.state != ReceiptState::Publishing {
                return Err(ReceiptError::new(ReceiptErrorKind::Conflict, "publish conflict"));
            }
            record.state = ReceiptState::Published;
            record.revision += 1;
            record.publication = Some(batch.publication.clone());
            record.updated_at_unix_ms = batch.publication.published_at_unix_ms();
            output.push(record.clone());
        }
        self.publications
            .lock()
            .await
            .push(batch.receipts.iter().map(|receipt| receipt.receipt_id).collect());
        Ok(output)
    }

    async fn fail_batch(
        &self,
        _owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        failure: &ReceiptFailure,
        failed_at: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        let mut records = self.records.lock().await;
        let mut output = Vec::new();
        for expected in receipts {
            let record = records
                .get_mut(&expected.receipt_id)
                .ok_or_else(|| ReceiptError::new(ReceiptErrorKind::NotFound, "receipt missing"))?;
            record.state = if failure.error().code() == arrowcol_protocol::ErrorCode::PublicationOutcomeUnknown {
                ReceiptState::PublicationUnknown
            } else {
                ReceiptState::Failed
            };
            record.revision += 1;
            record.failure_disposition = Some(failure.disposition());
            record.error = Some(failure.error().clone());
            record.updated_at_unix_ms = failed_at;
            output.push(record.clone());
        }
        Ok(output)
    }

    async fn receipt(&self, receipt_id: ReceiptId) -> ReceiptResult<Option<ReceiptRecord>> {
        Ok(self.records.lock().await.get(&receipt_id).cloned())
    }

    async fn receipts(&self, receipt_ids: &[ReceiptId]) -> ReceiptResult<Vec<Option<ReceiptRecord>>> {
        let records = self.records.lock().await;
        Ok(receipt_ids.iter().map(|id| records.get(id).cloned()).collect())
    }

    async fn reconcile_expired(&self, _now: UnixMillis, _limit: usize) -> ReceiptResult<ReconciliationReport> {
        Ok(ReconciliationReport {
            reconciled: Vec::new(),
            limit_reached: false,
        })
    }

    async fn adopt_receipts(&self, request: &ReceiptAdoption) -> ReceiptResult<ReceiptAdoptionReport> {
        request.validate()?;
        let mut records = self.records.lock().await;
        let mut report = ReceiptAdoptionReport::default();
        for candidate in &request.receipts {
            match records.get_mut(&candidate.receipt_id) {
                Some(record) if candidate.matches(record) && record.table == *request.lease.table() => {
                    record.owner_instance_id = request.successor;
                    record.lease_id = request.lease.lease_id();
                    record.fencing_epoch = request.lease.fencing_token();
                    record.lease_deadline_unix_ms = request.lease_deadline;
                    record.heartbeat_at_unix_ms = request.adopted_at;
                    record.updated_at_unix_ms = request.adopted_at;
                    report.adopted.push(record.clone());
                }
                current => report.rejected.push(RejectedAdoption {
                    receipt_id: candidate.receipt_id,
                    current: current.cloned(),
                }),
            }
        }
        Ok(report)
    }

    async fn fail_unbuffered_predecessor_receipts(
        &self,
        _successor: CollectorInstanceId,
        _failed_at: UnixMillis,
        _limit: usize,
    ) -> ReceiptResult<ReconciliationReport> {
        Ok(ReconciliationReport {
            reconciled: Vec::new(),
            limit_reached: false,
        })
    }
}

#[derive(Default)]
struct FakeSink {
    version: AtomicU64,
}

#[async_trait]
impl PublicationSink for FakeSink {
    async fn begin(&self, context: PublicationContext) -> Result<Box<dyn PublicationSession>, SinkError> {
        Ok(Box::new(FakeSession {
            context,
            version: self.version.fetch_add(1, Ordering::AcqRel),
            rows: 0,
            bytes: 0,
        }))
    }
}

struct FakeSession {
    context: PublicationContext,
    version: u64,
    rows: usize,
    bytes: u64,
}

#[async_trait]
impl PublicationSession for FakeSession {
    async fn write(&mut self, batch: &RecordBatch) -> Result<(), SinkError> {
        self.rows += batch.num_rows();
        self.bytes += record_batch_logical_bytes(batch).map_err(|error| SinkError::retryable(error.to_string()))?;
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<PublicationOutcome, SinkError> {
        let rows = u64::try_from(self.rows).map_err(|_| SinkError::publication_unknown("row overflow"))?;
        Ok(PublicationOutcome {
            publication: DeltaPublicationResult::new(
                Measurement::new(self.version + 1)
                    .map_err(|error| SinkError::publication_unknown(error.to_string()))?,
                Measurement::new(1).map_err(|error| SinkError::publication_unknown(error.to_string()))?,
                Measurement::new(rows).map_err(|error| SinkError::publication_unknown(error.to_string()))?,
                Measurement::new(self.bytes).map_err(|error| SinkError::publication_unknown(error.to_string()))?,
                self.context.schema_fingerprint,
                now().map_err(|error| SinkError::publication_unknown(error.to_string()))?,
                self.context.fence,
            ),
            file_observation: CompletedFileObservation {
                rows: self.rows,
                input_bytes: self.bytes,
                parquet_bytes: self.bytes.max(1),
            },
        })
    }
}

struct FakeRouting {
    view: Mutex<RoutingView>,
}

impl FakeRouting {
    fn owned(owner: CollectorInstanceId, table: TableIdentity) -> Result<Self> {
        let now = now()?;
        let expiry = UnixMillis::new(now.get() + 120_000)?;
        let key = LeaseKey::new(ImplementationPool::Direct, table);
        let lease = RoutingLease {
            key: key.clone(),
            lease_id: LeaseId::new(),
            holder_instance_id: owner,
            fencing_epoch: FencingToken::new(1)?,
            lease_version: 1,
            state: LeaseState::Active,
            issued_at_unix_ms: now,
            renewed_at_unix_ms: now,
            expires_at_unix_ms: expiry,
            drain_started_at_unix_ms: None,
        };
        Ok(Self {
            view: Mutex::new(RoutingView {
                loaded_at_unix_ms: now,
                collectors: BTreeMap::new(),
                leases: BTreeMap::from([(key, lease)]),
            }),
        })
    }
}

#[async_trait]
impl RoutingStore for FakeRouting {
    async fn register_collector(&self, _registration: &arrowcol_routing::CollectorRegistration) -> RoutingResult<()> {
        Ok(())
    }

    async fn heartbeat_collector(
        &self,
        _instance_id: CollectorInstanceId,
        _heartbeat_at: UnixMillis,
        _lease_expires_at: UnixMillis,
    ) -> RoutingResult<()> {
        Ok(())
    }

    async fn set_collector_state(
        &self,
        _instance_id: CollectorInstanceId,
        _state: CollectorState,
        _changed_at: UnixMillis,
    ) -> RoutingResult<()> {
        Ok(())
    }

    async fn publish_pressure(&self, _snapshot: &arrowcol_routing::RichPressureSnapshot) -> RoutingResult<()> {
        Ok(())
    }

    async fn load_view(&self, loaded_at: UnixMillis) -> RoutingResult<RoutingView> {
        let mut view = self
            .view
            .lock()
            .map_err(|_| {
                arrowcol_routing::RoutingError::new(
                    arrowcol_routing::RoutingErrorKind::CorruptState,
                    "fake routing lock poisoned",
                )
            })?
            .clone();
        view.loaded_at_unix_ms = loaded_at;
        Ok(view)
    }

    async fn claim_lease(&self, _claim: &LeaseClaim) -> RoutingResult<RoutingLease> {
        unreachable!("unused")
    }

    async fn begin_lease_drain(&self, _request: &LeaseCas) -> RoutingResult<RoutingLease> {
        unreachable!("unused")
    }

    async fn transfer_drained_lease(&self, _request: &LeaseTransfer) -> RoutingResult<RoutingLease> {
        unreachable!("unused")
    }

    async fn transfer_to_successor(&self, _request: &arrowcol_routing::LeaseSuccession) -> RoutingResult<RoutingLease> {
        Err(arrowcol_routing::RoutingError::new(
            arrowcol_routing::RoutingErrorKind::InvalidConfiguration,
            "fake routing does not support crash succession",
        ))
    }
}

fn receipt_error(error: impl std::fmt::Display) -> ReceiptError {
    ReceiptError::new(ReceiptErrorKind::InvalidConfiguration, error.to_string())
}

fn now() -> Result<UnixMillis> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(UnixMillis::new(u64::try_from(millis)?)?)
}
