use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, ensure};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::commit::CommitOptions;
use arrow_deltalake::sizing::AdaptiveFileSizeConfig;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::{FlightData, FlightDescriptor};
use arrowcol_buffer::{BufferConfig, BufferManager, CapacityLimits, PublicationSink};
use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest, HiveMetastore,
    HiveMetastoreConfig, Metastore, SchemaValidator, SqlServerCatalogStore, SqlServerConfig as CatalogSqlConfig,
};
use arrowcol_flight::{AllowAllAuthenticator, ArrowColFlightService, PutResultMetadata, RuntimeLimits};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, BasisPoints, CatalogName, ClaimedSchemaFingerprint,
    CollectorInstanceId, DeltaPublicationResult, ImplementationPool, IngestMetadata, Measurement, PositiveMeasurement,
    PressureGauge, PrincipalId, PrincipalIssuer, PrincipalScope, ProtocolVersion, ReceiptId, ReceiptState, SchemaName,
    TableIdentity, TableName, UnixMillis, fingerprint_schema,
};
use arrowcol_receipts::{
    OwnerRegistration, ReceiptStore, ReceiptVersion, SqlServerReceiptConfig, SqlServerReceiptStore, ValidatedDelivery,
};
use arrowcol_routing::{
    CollectorRegistration, CollectorState, LeaseClaim, LeaseKey, RichPressureSnapshot, RoutingStore,
    SqlServerRoutingConfig, SqlServerRoutingStore, SubsystemState,
};
use arrowcol_rust::control::TableActivity;
use arrowcol_rust::delta_sink::ProductionDeltaSink;
use arrowcol_rust::ingest::DirectAdmission;
use arrowcol_rust::lease::TableLeaseManager;
use arrowcol_sql_server::{ConnectionConfig, PoolConfig, SqlPool};
use futures::{StreamExt, TryStreamExt, stream};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tiberius::{AuthMethod as SqlAuthMethod, Client, Config as TiberiusConfig, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tonic::Request;
use uuid::Uuid;

type SqlClient = Client<Compat<TcpStream>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires arrowcol-infra SQL Server and Hive Compose services"]
async fn live_compose_end_to_end() -> Result<()> {
    let live = LiveConfig::load()?;
    let password = live.password()?;
    let connection = ConnectionConfig::new(
        &live.sql_host,
        live.sql_port,
        &live.database,
        &live.username,
        password.clone(),
    )
    .trust_server_certificate(true)
    .with_timeouts(Duration::from_secs(5), Duration::from_secs(15));
    let pool_config = PoolConfig {
        max_size: 4,
        acquire_timeout: Duration::from_secs(5),
    };
    let catalog_pool = SqlPool::new(connection.clone(), pool_config)?;
    let receipt_pool = SqlPool::new(connection.clone(), pool_config)?;
    let routing_pool = SqlPool::new(connection, pool_config)?;
    let catalog: Arc<dyn CatalogStore> = Arc::new(
        SqlServerCatalogStore::new(
            CatalogSqlConfig::new(
                &live.sql_host,
                live.sql_port,
                &live.database,
                &live.username,
                password.clone(),
            )
            .trust_server_certificate(true)
            .with_timeouts(Duration::from_secs(5), Duration::from_secs(15)),
        )
        .with_pool(catalog_pool.clone())?,
    );
    let metastore: Arc<dyn Metastore> = Arc::new(HiveMetastore::new(
        HiveMetastoreConfig::new(&live.hive_host, live.hive_port)
            .with_warehouse_root("file:///opt/hive/data/warehouse")
            .with_owner("arrowcol-live")
            .with_connect_timeout(Duration::from_secs(5)),
    ));
    let (control, startup) =
        CatalogControlPlane::start(DdlParser::default(), Arc::clone(&catalog), Arc::clone(&metastore)).await?;
    ensure!(
        startup.failures.is_empty(),
        "catalog startup reconciliation left failures"
    );
    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("live_{}", &suffix[..20]);
    let identity = TableIdentity::new(
        CatalogName::new("telemetry")?,
        SchemaName::new("prod")?,
        TableName::new(table_name.clone())?,
    );
    control
        .execute_ddl(FlightSqlDdlRequest::new(format!(
            "CREATE TABLE telemetry.prod.{table_name} (id BIGINT NOT NULL, body VARCHAR)"
        )))
        .await?;
    let current = catalog
        .current_schema(&identity)
        .await?
        .context("live schema was not activated")?;
    let schema = Arc::new(current.arrow_schema()?);

    let receipts: Arc<dyn ReceiptStore> = Arc::new(
        SqlServerReceiptStore::new(
            SqlServerReceiptConfig::new(
                &live.sql_host,
                live.sql_port,
                &live.database,
                &live.username,
                password.clone(),
            )
            .trust_server_certificate(true)
            .with_timeouts(Duration::from_secs(5), Duration::from_secs(15))
            .with_batch_limits(256, 256)?,
        )
        .with_pool(receipt_pool.clone())?,
    );
    let routing: Arc<dyn RoutingStore> = Arc::new(
        SqlServerRoutingStore::new(
            SqlServerRoutingConfig::new(&live.sql_host, live.sql_port, &live.database, &live.username, password)
                .trust_server_certificate(true)
                .with_timeouts(Duration::from_secs(5), Duration::from_secs(15)),
        )
        .with_pool(routing_pool.clone())?,
    );
    let owner = CollectorInstanceId::new();
    register_live_owner(owner, &table_name, receipts.as_ref(), routing.as_ref()).await?;
    claim_live_route(owner, &identity, routing.as_ref()).await?;

    let root = live.delta_root.join(format!("live-{}", Uuid::new_v4().simple()));
    let first = LiveStack::start(
        owner,
        &identity,
        Arc::clone(&schema),
        Arc::clone(&catalog),
        Arc::clone(&receipts),
        Arc::clone(&routing),
        root.clone(),
    )
    .await?;

    let before = receipt_count(&live, current.table_id()).await?;
    let malicious = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let malicious_batch = RecordBatch::try_new(
        Arc::clone(&malicious),
        vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
    )?;
    let malicious_request = ingest_request(
        identity.clone(),
        vec![malicious_batch],
        fingerprint_schema(malicious.as_ref()),
    )
    .await?;
    let status = match first.service.handle_do_put(malicious_request).await {
        Ok(_) => return Err(anyhow!("malicious schema reached live admission")),
        Err(status) => status,
    };
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(receipt_count(&live, current.table_id()).await?, before);

    let first_batch = batch(Arc::clone(&schema), 10)?;
    let results = first
        .service
        .handle_do_put(
            ingest_request(
                identity.clone(),
                vec![first_batch.clone(), first_batch],
                current.fingerprint(),
            )
            .await?,
        )
        .await?
        .into_inner()
        .try_collect::<Vec<_>>()
        .await?;
    let receipt_ids = decode_receipts(&results)?;
    assert_eq!(receipt_ids.len(), 2);
    assert_ne!(receipt_ids[0], receipt_ids[1]);
    first.buffer.drain().await?;
    let published = receipts.receipts(&receipt_ids).await?;
    assert!(published.iter().flatten().all(ReceiptRecordExt::published));
    let versions = published
        .iter()
        .flatten()
        .filter_map(|record| record.publication.as_ref())
        .map(DeltaPublicationResult::delta_version)
        .map(Measurement::get)
        .collect::<BTreeSet<_>>();
    assert_eq!(versions.len(), 1, "grouped receipts must share one Delta version");
    assert_eq!(read_delta_rows(&first.table_root)?, 4);

    let validation = SchemaValidator::new(Arc::clone(&catalog))
        .validate(
            &identity,
            ClaimedSchemaFingerprint::new(current.fingerprint()),
            schema.as_ref(),
        )
        .await?;
    let delivery = ValidatedDelivery::new(&validation, 1, 1, 16)?;
    let lease = first.leases.lease_for(current.table_id(), &identity).await?.current()?;
    let short_deadline = add_millis(now()?, 100)?;
    let uncertain = receipts
        .create_flight_batch(owner, &lease, short_deadline, &[delivery])
        .await?;
    let uncertain = receipts
        .mark_buffered(
            owner,
            &[ReceiptVersion::new(uncertain[0].receipt_id, uncertain[0].revision)],
            short_deadline,
        )
        .await?;
    let uncertain = receipts
        .mark_flushing(
            owner,
            &[ReceiptVersion::new(uncertain[0].receipt_id, uncertain[0].revision)],
            short_deadline,
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(160)).await;
    let reconciled = receipts.reconcile_expired(now()?, 256).await?;
    assert!(reconciled.reconciled.iter().any(|record| {
        record.receipt_id == uncertain[0].receipt_id && record.state == ReceiptState::PublicationUnknown
    }));

    first.stop().await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let restarted = LiveStack::start(
        owner,
        &identity,
        Arc::clone(&schema),
        Arc::clone(&catalog),
        Arc::clone(&receipts),
        Arc::clone(&routing),
        root.clone(),
    )
    .await?;
    let resend = restarted
        .service
        .handle_do_put(ingest_request(identity, vec![batch(Arc::clone(&schema), 10)?], current.fingerprint()).await?)
        .await?
        .into_inner()
        .try_collect::<Vec<_>>()
        .await?;
    let resend_id = decode_receipts(&resend)?[0];
    assert!(!receipt_ids.contains(&resend_id));
    restarted.buffer.drain().await?;
    assert_eq!(read_delta_rows(&restarted.table_root)?, 6);
    restarted.stop().await?;

    let crash_key = format!("crash-{}", &suffix[..12]);
    let crashed_owner = CollectorInstanceId::new();
    let crash_started = now()?;
    receipts
        .register_owner(OwnerRegistration {
            instance_id: crashed_owner,
            instance_key: crash_key.clone(),
            generation: 1,
            host_name: Some("crashed".to_owned()),
            build_version: Some("test".to_owned()),
            started_at_unix_ms: crash_started,
            lease_deadline_unix_ms: add_millis(crash_started, 100)?,
            durable_recovery_window_ms: None,
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(160)).await;
    let replacement_owner = CollectorInstanceId::new();
    let replacement_started = now()?;
    receipts
        .register_owner(OwnerRegistration {
            instance_id: replacement_owner,
            instance_key: crash_key,
            generation: 2,
            host_name: Some("replacement".to_owned()),
            build_version: Some("test".to_owned()),
            started_at_unix_ms: replacement_started,
            lease_deadline_unix_ms: add_millis(replacement_started, 60_000)?,
            durable_recovery_window_ms: None,
        })
        .await?;
    assert!(
        receipts
            .heartbeat_owner(crashed_owner, add_millis(now()?, 60_000)?)
            .await
            .is_err(),
        "expired predecessor must remain fenced"
    );
    receipts.start_owner_drain(replacement_owner).await?;
    receipts.finish_owner_drain(replacement_owner, now()?).await?;

    let drain = receipts.start_owner_drain(owner).await?;
    assert_eq!(drain.active_receipts, 0);
    receipts.finish_owner_drain(owner, now()?).await?;
    routing
        .set_collector_state(owner, CollectorState::Stopped, now()?)
        .await?;
    for pool in [&catalog_pool, &receipt_pool, &routing_pool] {
        let stats = pool.stats();
        assert!(stats.reused > 0, "live collector did not reuse SQL connections");
        assert!(stats.created < stats.acquired, "every operation opened a connection");
        assert!(stats.size <= 4);
        pool.close();
    }
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "run only after the live E2E harness intentionally stops SQL Server"]
async fn sql_outage_is_observed() -> Result<()> {
    ensure!(
        std::env::var("ARROWCOL_EXPECT_SQL_OUTAGE").as_deref() == Ok("1"),
        "ARROWCOL_EXPECT_SQL_OUTAGE=1 is required"
    );
    let live = LiveConfig::load()?;
    let connection = ConnectionConfig::new(
        &live.sql_host,
        live.sql_port,
        &live.database,
        &live.username,
        live.password()?,
    )
    .trust_server_certificate(true)
    .with_timeouts(Duration::from_millis(500), Duration::from_millis(500));
    let pool = SqlPool::new(
        connection,
        PoolConfig {
            max_size: 1,
            acquire_timeout: Duration::from_millis(500),
        },
    )?;
    let store = SqlServerCatalogStore::new(
        CatalogSqlConfig::new(
            &live.sql_host,
            live.sql_port,
            &live.database,
            &live.username,
            live.password()?,
        )
        .trust_server_certificate(true)
        .with_timeouts(Duration::from_millis(500), Duration::from_millis(500)),
    )
    .with_pool(pool.clone())?;
    assert!(store.incomplete_sagas().await.is_err());
    assert!(pool.stats().acquire_failures > 0);
    pool.close();
    Ok(())
}

struct LiveStack {
    service: ArrowColFlightService,
    buffer: Arc<BufferManager>,
    leases: Arc<TableLeaseManager>,
    table_root: PathBuf,
}

impl LiveStack {
    #[allow(clippy::too_many_arguments)]
    async fn start(
        owner: CollectorInstanceId,
        identity: &TableIdentity,
        schema: Arc<Schema>,
        catalog: Arc<dyn CatalogStore>,
        receipts: Arc<dyn ReceiptStore>,
        routing: Arc<dyn RoutingStore>,
        root: PathBuf,
    ) -> Result<Self> {
        let current = catalog
            .current_schema(identity)
            .await?
            .context("live table disappeared")?;
        ensure!(current.arrow_schema()? == *schema, "live schema changed unexpectedly");
        let leases = Arc::new(TableLeaseManager::new(
            Arc::clone(&receipts),
            routing,
            owner,
            Duration::from_secs(60),
        ));
        let sink = Arc::new(ProductionDeltaSink::new(
            root,
            CommitOptions {
                batch_window: Duration::from_millis(50),
                max_files_per_commit: 8,
                max_pending_files: 32,
            },
            Arc::clone(&leases),
            arrowcol_rust::metrics::CollectorMetrics::new()?,
        ));
        let table_root = sink.table_path(identity);
        let publication: Arc<dyn PublicationSink> = sink;
        let buffer = BufferManager::new(
            BufferConfig {
                global_capacity: CapacityLimits::new(64 * 1024 * 1024, 1_000_000, 64)?,
                per_table_capacity: CapacityLimits::new(32 * 1024 * 1024, 500_000, 32)?,
                max_arrow_memory_per_flush: 32 * 1024 * 1024,
                max_rows_per_flush: 500_000,
                max_batches_per_flush: 8,
                max_age: Duration::from_secs(30),
                age_check_interval: Duration::from_millis(50),
                max_concurrent_flushes: 2,
                receipt_lease_duration: Duration::from_secs(30),
                adaptive_size: AdaptiveFileSizeConfig {
                    target_parquet_bytes: 128 * 1024 * 1024,
                    min_input_bytes: 8 * 1024 * 1024,
                    max_input_bytes: 64 * 1024 * 1024,
                    ..AdaptiveFileSizeConfig::default()
                },
            },
            Arc::clone(&receipts),
            publication,
        )?;
        let admission = Arc::new(DirectAdmission::new(
            Arc::clone(&catalog),
            receipts,
            Arc::clone(&buffer),
            Arc::clone(&leases),
            Arc::new(TableActivity::default()),
            owner,
            Duration::from_secs(30),
            Duration::from_secs(5),
        ));
        let service = ArrowColFlightService::new(
            ImplementationPool::Direct,
            catalog,
            Arc::new(AllowAllAuthenticator::new(auth_context()?)),
            admission,
            RuntimeLimits::default(),
        )?;
        Ok(Self {
            service,
            buffer,
            leases,
            table_root,
        })
    }

    async fn stop(self) -> Result<()> {
        let report = self.buffer.shutdown().await?;
        ensure!(report.incomplete.is_empty(), "live buffer left incomplete work");
        self.leases.release_all().await
    }
}

trait ReceiptRecordExt {
    fn published(&self) -> bool;
}

impl ReceiptRecordExt for arrowcol_receipts::ReceiptRecord {
    fn published(&self) -> bool {
        self.state == ReceiptState::Published && self.can_client_purge()
    }
}

async fn register_live_owner(
    owner: CollectorInstanceId,
    instance_key: &str,
    receipts: &dyn ReceiptStore,
    routing: &dyn RoutingStore,
) -> Result<()> {
    let now = now()?;
    let expiry = add_millis(now, 60_000)?;
    receipts
        .register_owner(OwnerRegistration {
            instance_id: owner,
            instance_key: format!("live-{instance_key}"),
            generation: 1,
            host_name: Some("live-compose".to_owned()),
            build_version: Some("test".to_owned()),
            started_at_unix_ms: now,
            lease_deadline_unix_ms: expiry,
            durable_recovery_window_ms: None,
        })
        .await?;
    routing
        .register_collector(&CollectorRegistration {
            instance_id: owner,
            instance_key: format!("live-{instance_key}"),
            implementation_pool: ImplementationPool::Direct,
            flight_endpoint: "127.0.0.1:50051".parse()?,
            catalog_capable: true,
            max_concurrent_streams: PositiveMeasurement::new(64)?,
            started_at_unix_ms: now,
            heartbeat_at_unix_ms: now,
            lease_expires_at_unix_ms: expiry,
        })
        .await?;
    routing.publish_pressure(&ready_pressure(owner, now)?).await?;
    Ok(())
}

async fn claim_live_route(owner: CollectorInstanceId, table: &TableIdentity, routing: &dyn RoutingStore) -> Result<()> {
    let now = now()?;
    let lease = routing
        .claim_lease(&LeaseClaim {
            key: LeaseKey::new(ImplementationPool::Direct, table.clone()),
            candidate_instance_id: owner,
            now_unix_ms: now,
            expires_at_unix_ms: add_millis(now, 60_000)?,
            minimum_snapshot_at_unix_ms: UnixMillis::new(now.get().saturating_sub(15_000))?,
        })
        .await?;
    ensure!(
        lease.holder_instance_id == owner,
        "live routing lease went to another owner"
    );
    Ok(())
}

fn ready_pressure(owner: CollectorInstanceId, now: UnixMillis) -> Result<RichPressureSnapshot> {
    let zero = gauge(0, 64)?;
    Ok(RichPressureSnapshot {
        container_memory: None,
        effective_cpu_millicores: None,
        instance_id: owner,
        observed_at_unix_ms: now,
        cpu_utilization_bps: BasisPoints::new(0)?,
        rss_bytes: Measurement::ZERO,
        rss_limit_bytes: PositiveMeasurement::new(4 * 1024 * 1024 * 1024)?,
        active_streams: zero,
        received_bytes: Measurement::ZERO,
        received_rows: Measurement::ZERO,
        received_batches: Measurement::ZERO,
        buffered_bytes: gauge(0, 64 * 1024 * 1024)?,
        buffered_rows: gauge(0, 1_000_000)?,
        buffered_batches: gauge(0, 64)?,
        available_permits: gauge(64, 64)?,
        pending_commits: gauge(0, 2)?,
        oldest_buffer_age_ms: Measurement::ZERO,
        delta_pressure_bps: BasisPoints::new(0)?,
        quiver_pressure_bps: BasisPoints::new(0)?,
        dfe_pressure_bps: BasisPoints::new(0)?,
        delta_state: SubsystemState::Ready,
        quiver_state: SubsystemState::Ready,
        dfe_state: SubsystemState::Disabled,
        accepting_deliveries: true,
        tables: Vec::new(),
    })
}

fn gauge(current: u64, limit: u64) -> Result<PressureGauge> {
    Ok(PressureGauge::new(
        Measurement::new(current)?,
        PositiveMeasurement::new(limit)?,
    )?)
}

fn batch(schema: Arc<Schema>, offset: i64) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![offset, offset + 1])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )?)
}

async fn ingest_request(
    identity: TableIdentity,
    batches: Vec<RecordBatch>,
    fingerprint: arrowcol_protocol::SchemaFingerprint,
) -> Result<Request<impl futures::Stream<Item = Result<FlightData, tonic::Status>> + Send + 'static>> {
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
    let metadata = IngestMetadata::new(
        ProtocolVersion::CURRENT,
        ImplementationPool::Direct,
        identity,
        ClaimedSchemaFingerprint::new(fingerprint),
    );
    let mut request = Request::new(stream::iter(encoded));
    for (key, value) in metadata.to_ascii_pairs() {
        request
            .metadata_mut()
            .insert(key, value.parse().map_err(|_| anyhow!("invalid live metadata"))?);
    }
    Ok(request)
}

fn decode_receipts(results: &[arrow_flight::PutResult]) -> Result<Vec<ReceiptId>> {
    results
        .iter()
        .map(PutResultMetadata::from_put_result)
        .map(|result| result?.receipt_id().context("live PutResult did not contain a receipt"))
        .collect()
}

fn auth_context() -> Result<AuthContext> {
    let scopes = ["arrowcol/ingest", "arrowcol/catalog"]
        .into_iter()
        .map(PrincipalScope::new)
        .collect::<Result<_, _>>()?;
    let principal = AuthenticatedPrincipal::new(
        PrincipalId::new("live-compose")?,
        PrincipalIssuer::new("arrowcol-live-test")?,
        scopes,
    )?;
    Ok(AuthContext::new(principal, AuthMethod::WorkloadIdentity, now()?, None)?)
}

fn read_delta_rows(table_root: &Path) -> Result<usize> {
    let mut logs = std::fs::read_dir(table_root.join("_delta_log"))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "json"))
        .collect::<Vec<_>>();
    logs.sort();
    let mut rows = 0;
    for log in logs {
        for line in std::fs::read_to_string(log)?.lines() {
            let value: serde_json::Value = serde_json::from_str(line)?;
            let Some(path) = value
                .get("add")
                .and_then(|add| add.get("path"))
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            let file = std::fs::File::open(table_root.join(path))?;
            rows += ParquetRecordBatchReaderBuilder::try_new(file)?
                .build()?
                .collect::<Result<Vec<_>, _>>()?
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>();
        }
    }
    Ok(rows)
}

async fn receipt_count(config: &LiveConfig, table_id: Uuid) -> Result<i64> {
    let mut client = sql_client(config).await?;
    let row = client
        .query(
            "SELECT COUNT_BIG(*) AS receipt_count FROM arrowcol.delivery_receipts WHERE table_id = @P1;",
            &[&table_id],
        )
        .await?
        .into_row()
        .await?
        .context("receipt count query returned no row")?;
    row.get::<i64, _>("receipt_count")
        .context("receipt count column is missing")
}

async fn sql_client(config: &LiveConfig) -> Result<SqlClient> {
    let mut sql = TiberiusConfig::new();
    sql.host(&config.sql_host);
    sql.port(config.sql_port);
    sql.database(&config.database);
    sql.authentication(SqlAuthMethod::sql_server(&config.username, config.password()?));
    sql.encryption(EncryptionLevel::Required);
    sql.trust_cert();
    let address = sql.get_addr();
    let tcp = TcpStream::connect(address).await?;
    tcp.set_nodelay(true)?;
    Ok(Client::connect(sql, tcp.compat_write()).await?)
}

struct LiveConfig {
    sql_host: String,
    sql_port: u16,
    database: String,
    username: String,
    password_file: PathBuf,
    hive_host: String,
    hive_port: u16,
    delta_root: PathBuf,
}

impl LiveConfig {
    fn load() -> Result<Self> {
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .context("collector manifest is not under the workspace projects directory")?
            .to_path_buf();
        Ok(Self {
            sql_host: std::env::var("ARROWCOL_SQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned()),
            sql_port: std::env::var("ARROWCOL_SQL_PORT")
                .unwrap_or_else(|_| "14333".to_owned())
                .parse()?,
            database: "arrowcol".to_owned(),
            username: "arrowcol_app".to_owned(),
            password_file: std::env::var("ARROWCOL_SQL_PASSWORD_FILE").map_or_else(
                |_| workspace.join("projects/arrowcol-infra/secrets/arrowcol-app-password.txt"),
                PathBuf::from,
            ),
            hive_host: std::env::var("ARROWCOL_HIVE_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned()),
            hive_port: std::env::var("ARROWCOL_HIVE_PORT")
                .unwrap_or_else(|_| "9083".to_owned())
                .parse()?,
            delta_root: workspace.join("projects/arrowcol-rust/.data/live"),
        })
    }

    fn password(&self) -> Result<String> {
        let password = std::fs::read_to_string(&self.password_file)
            .with_context(|| format!("read {}", self.password_file.display()))?;
        let password = password.trim_end_matches(['\r', '\n']);
        ensure!(!password.is_empty(), "SQL password is empty");
        ensure!(!password.contains(['\r', '\n']), "SQL password is multiline");
        Ok(password.to_owned())
    }
}

fn now() -> Result<UnixMillis> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(UnixMillis::new(u64::try_from(millis)?)?)
}

fn add_millis(value: UnixMillis, delta: u64) -> Result<UnixMillis> {
    Ok(UnixMillis::new(
        value.get().checked_add(delta).context("timestamp overflow")?,
    )?)
}
