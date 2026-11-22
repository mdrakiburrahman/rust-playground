use super::*;
use arrowcol_protocol::{ClaimedSchemaFingerprint, IngestMetadata, ProtocolVersion, SchemaFingerprint};
use arrowcol_routing::{
    ControlOperation, ControlRoute, SqlOperationObserver, SqlServerRoutingConfig, SqlServerRoutingStore,
};
use arrowcol_sql_server::{ConnectionConfig, SqlConnection, SqlPool};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const CARDINALITY: usize = 16;
const WARM_REQUEST_PAIRS: usize = 512;
const INSTANCE: &str = "00000000-0000-0000-0000-000000000079";

#[derive(Default)]
struct Observer {
    opened: AtomicU64,
    operations: Mutex<BTreeMap<&'static str, u64>>,
    failures: AtomicU64,
}

impl SqlOperationObserver for Observer {
    fn connection_opened(&self) {
        self.opened.fetch_add(1, Ordering::Relaxed);
    }

    fn operation(&self, name: &'static str, _: Duration, success: bool) {
        let mut counts = self
            .operations
            .lock()
            .unwrap_or_else(|error| panic!("SQL observer poisoned: {error}"));
        *counts.entry(name).or_default() += 1;
        if !success {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Observer {
    fn value(&self, pool: Option<&SqlPool>) -> Value {
        let counts = self
            .operations
            .lock()
            .unwrap_or_else(|error| panic!("SQL observer poisoned: {error}"));
        json!({
            "operations": *counts,
            "failures": self.failures.load(Ordering::Relaxed),
            "physical_connections_created": pool.map_or_else(
                || self.opened.load(Ordering::Relaxed), |pool| pool.stats().created),
            "pool_reused": pool.map_or(0, |pool| pool.stats().reused),
        })
    }
}

fn require_fixture() -> anyhow::Result<PathBuf> {
    anyhow::ensure!(cfg!(target_os = "linux"), "fixture requires Linux");
    anyhow::ensure!(
        std::env::var("ARROWCOL_ROUTER_SQL_FIXTURE").as_deref() == Ok("1"),
        "ARROWCOL_ROUTER_SQL_FIXTURE=1 is mandatory; absent SQL prerequisites are not a skip"
    );
    let isolated = std::env::var("ARROWCOL_ROUTER_SQL_PROJECT")?;
    anyhow::ensure!(
        isolated.starts_with("router-sql-"),
        "an isolated router-sql-* Compose project is mandatory"
    );
    let output = PathBuf::from(std::env::var("ARROWCOL_ROUTER_SQL_OUTPUT")?);
    anyhow::ensure!(
        output.is_absolute() && output.is_dir(),
        "private output directory must already exist"
    );
    Ok(output)
}

fn sql_config() -> anyhow::Result<SqlServerRoutingConfig> {
    Ok(SqlServerRoutingConfig::with_password_file(
        std::env::var("ARROWCOL_SQLSERVER_HOST")?,
        std::env::var("ARROWCOL_SQLSERVER_PORT")?.parse::<u16>()?,
        "arrowcol",
        "arrowcol_app",
        std::env::var("ARROWCOL_APP_PASSWORD_FILE")?,
    )?
    .trust_server_certificate(true)
    .with_timeouts(Duration::from_secs(5), Duration::from_secs(15)))
}

async fn reset_fixture() -> anyhow::Result<()> {
    let password = std::fs::read_to_string(std::env::var("ARROWCOL_APP_PASSWORD_FILE")?)?;
    let config = ConnectionConfig::new(
        std::env::var("ARROWCOL_SQLSERVER_HOST")?,
        std::env::var("ARROWCOL_SQLSERVER_PORT")?.parse::<u16>()?,
        "arrowcol",
        "arrowcol_app",
        password.trim_end_matches(['\r', '\n']),
    )
    .trust_server_certificate(true);
    let mut connection = SqlConnection::connect(&config).await?;
    connection
        .execute(
            "DELETE FROM arrowcol.routing_table_leases WHERE holder_instance_id = @P1;
         DELETE FROM arrowcol.routing_pressure_history WHERE instance_id = @P1;
         DELETE FROM arrowcol.routing_pressure_latest WHERE instance_id = @P1;
         DELETE FROM arrowcol.routing_collectors WHERE instance_id = @P1;",
            &[&uuid::Uuid::parse_str(INSTANCE)?],
        )
        .await?;
    connection.finish().await?;
    let view = SqlServerRoutingStore::new(sql_config()?).load_view(now()?).await?;
    anyhow::ensure!(
        view.collectors.is_empty() && view.leases.is_empty(),
        "isolated SQL fixture is not empty"
    );
    Ok(())
}

pub(super) fn run_child() -> anyhow::Result<()> {
    require_fixture()?;
    let mode = std::env::var("ARROWCOL_ROUTER_SQL_MODE")?;
    let listen: SocketAddr = std::env::var(LISTEN_ENV)?.parse()?;
    let backend: SocketAddr = std::env::var(BACKEND_ENV)?.parse()?;
    let state = std::env::var(STATE_ENV)?;
    let runtime = tokio::runtime::Runtime::new()?;
    let observer = Arc::new(Observer::default());
    let mut pool = None;
    let store: Arc<dyn RoutingStore> = match mode.as_str() {
        "sql_unpooled" | "sql_pooled" => {
            let config = sql_config()?;
            let mut store = SqlServerRoutingStore::new(config.clone());
            if mode == "sql_pooled" {
                let shared = config.create_pool(2, Duration::from_secs(5))?;
                store = store.with_pool(shared.clone())?;
                pool = Some(shared);
            }
            Arc::new(store.with_operation_observer(Arc::<Observer>::clone(&observer)))
        }
        "memory_control" => Arc::new(MemoryRoutingStore::default()),
        _ => anyhow::bail!("unsupported fixture mode {mode}"),
    };
    let current = now()?;
    let instance = INSTANCE.parse::<CollectorInstanceId>()?;
    runtime.block_on(async {
        store
            .register_collector(&CollectorRegistration {
                instance_id: instance,
                instance_key: "router-sql-fixture".to_owned(),
                implementation_pool: ImplementationPool::Dataflow,
                flight_endpoint: backend,
                catalog_capable: true,
                max_concurrent_streams: positive(256)?,
                started_at_unix_ms: subtract(current, 120_000)?,
                heartbeat_at_unix_ms: current,
                lease_expires_at_unix_ms: add(current, 3_600_000)?,
            })
            .await?;
        store.publish_pressure(&snapshot(instance, current)?).await?;
        anyhow::Ok(())
    })?;
    let policy = RoutingPolicy {
        warmup_ms: 0,
        lease_ttl_ms: 300_000,
        readiness_max_staleness_ms: 300_000,
        ..RoutingPolicy::default()
    };
    let routing = Arc::new(RoutingEngine::new(store, policy)?);
    runtime.block_on(async {
        routing.refresh(current).await?;
        let stale = add(current, 15_001)?;
        let metadata = IngestMetadata::new(
            ProtocolVersion::CURRENT,
            ImplementationPool::Dataflow,
            table(0)?,
            ClaimedSchemaFingerprint::new(SchemaFingerprint::from_bytes([0; 32])),
        );
        anyhow::ensure!(
            routing.route_ingest(&metadata, stale).await.is_err(),
            "stale SQL pressure admitted ingest"
        );
        anyhow::ensure!(
            routing
                .route_control(
                    &ControlRoute {
                        implementation_pool: ImplementationPool::Dataflow,
                        operation: ControlOperation::NewTable,
                        routing_key: "telemetry.prod.fixture_control".to_owned(),
                    },
                    stale
                )
                .await
                .is_err(),
            "stale SQL pressure admitted catalog control"
        );
        anyhow::Ok(())
    })?;
    drop(runtime);
    let stats_path = PathBuf::from(&state).join("sql-operations.json");
    std::thread::spawn(move || {
        loop {
            let value = observer.value(pool.as_ref());
            let temporary = stats_path.with_extension("next");
            std::fs::write(&temporary, value.to_string()).unwrap_or_else(|error| panic!("write SQL stats: {error}"));
            std::fs::rename(&temporary, &stats_path).unwrap_or_else(|error| panic!("publish SQL stats: {error}"));
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|error| panic!("backend runtime: {error}"));
        runtime
            .block_on(
                Server::builder()
                    .add_service(FlightServiceServer::new(TestFlight))
                    .serve(backend),
            )
            .unwrap_or_else(|error| panic!("fixture backend: {error}"));
    });
    let metrics = RouterMetrics::new()?;
    let router = ProductionRouter::new(
        Arc::clone(&routing),
        Arc::new(AllowAllAuthenticator),
        Arc::new(EdgeVerifier::new(vec![1; 32], 30_000)?),
        Arc::new(InternalIdentitySigner::new(vec![2; 32], 30_000)?),
        upstream_config(),
        metrics.clone(),
    );
    let config = test_config(listen, state);
    // No periodic refresh in the bounded trial: fresh-cache routing and lease claims
    // are measured separately, without a background SQL query landing in one arm.
    let control = RouterControlLoop::new(routing, metrics, Duration::from_secs(3_600));
    build_server(&config, router, control)?.run_forever();
}

fn table(index: usize) -> anyhow::Result<TableIdentity> {
    Ok(TableIdentity::new(
        CatalogName::new("telemetry")?,
        SchemaName::new("prod")?,
        TableName::new(format!("fixture_{index:02}"))?,
    ))
}

async fn put(
    mut client: FlightServiceClient<Channel>,
    signer: &EdgeRequestSigner,
    nonce: Arc<AtomicU64>,
    index: usize,
) -> anyhow::Result<()> {
    let batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int64Array::from(vec![index as i64])) as arrow::array::ArrayRef,
    )])?;
    let messages: Vec<_> = FlightDataEncoderBuilder::new()
        .build(stream::iter([Ok(batch)]))
        .try_collect()
        .await?;
    let mut request = Request::new(stream::iter(messages));
    let name = format!("fixture_{index:02}");
    add_ingest_metadata(&mut request, signer, nonce, true, None, Some(&name))?;
    let response = client.do_put(request).await?;
    anyhow::ensure!(
        response.metadata().get(BACKEND_HEADER).and_then(|v| v.to_str().ok()) == Some("catalog-a"),
        "unexpected backend owner"
    );
    let results: Vec<_> = response.into_inner().try_collect().await?;
    anyhow::ensure!(
        results.len() == 1 && results[0].app_metadata.starts_with(b"backend=catalog-a;messages="),
        "invalid upstream completion"
    );
    Ok(())
}

async fn phase(
    channel: &Channel,
    signer: &EdgeRequestSigner,
    nonce: &Arc<AtomicU64>,
    pairs: usize,
) -> anyhow::Result<Value> {
    let mut control_us = Vec::with_capacity(pairs);
    let mut ingest_us = Vec::with_capacity(pairs);
    let started = Instant::now();
    for index in 0..pairs {
        let timer = Instant::now();
        control_request(FlightServiceClient::new(channel.clone()), signer, Arc::clone(nonce)).await?;
        control_us.push(timer.elapsed().as_secs_f64() * 1e6);
        let timer = Instant::now();
        put(
            FlightServiceClient::new(channel.clone()),
            signer,
            Arc::clone(nonce),
            index % CARDINALITY,
        )
        .await?;
        ingest_us.push(timer.elapsed().as_secs_f64() * 1e6);
    }
    let seconds = started.elapsed().as_secs_f64();
    Ok(json!({
        "request_count": pairs * 2, "control_count": pairs, "ingest_count": pairs,
        "success_count": pairs * 2, "error_count": 0, "elapsed_seconds": seconds,
        "requests_per_second": (pairs * 2) as f64 / seconds,
        "control_latency_us": latency(control_us), "ingest_latency_us": latency(ingest_us),
    }))
}

fn latency(mut values: Vec<f64>) -> Value {
    values.sort_by(f64::total_cmp);
    let at = |p: usize| values[(values.len() * p).div_ceil(100).saturating_sub(1)];
    json!({"count":values.len(), "p50":at(50), "p95":at(95), "p99":at(99), "max":values.last()})
}

async fn stats(state: &Path) -> anyhow::Result<Value> {
    tokio::time::sleep(Duration::from_millis(40)).await;
    Ok(serde_json::from_slice(&std::fs::read(
        state.join("sql-operations.json"),
    )?)?)
}

async fn verify_sql_owners() -> anyhow::Result<()> {
    let view = SqlServerRoutingStore::new(sql_config()?).load_view(now()?).await?;
    anyhow::ensure!(
        view.collectors.len() == 1 && view.leases.len() == CARDINALITY,
        "SQL route cardinality changed"
    );
    for index in 0..CARDINALITY {
        let key = LeaseKey::new(ImplementationPool::Dataflow, table(index)?);
        let lease = view
            .leases
            .get(&key)
            .ok_or_else(|| anyhow::anyhow!("SQL owner missing"))?;
        anyhow::ensure!(
            lease.holder_instance_id == INSTANCE.parse()? && lease.is_routable_at(now()?),
            "SQL owner or lease freshness changed"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit local SQL component fixture; prerequisites must be provided"]
async fn router_sql_component() -> anyhow::Result<()> {
    let output = require_fixture()?;
    // An independently labelled memory control cannot satisfy the SQL prerequisite.
    reset_fixture().await?;
    let mut records = Vec::new();
    for (trial, mode) in [
        "sql_unpooled",
        "sql_pooled",
        "sql_pooled",
        "sql_unpooled",
        "sql_unpooled",
        "sql_pooled",
        "memory_control",
    ]
    .into_iter()
    .enumerate()
    {
        reset_fixture().await?;
        let state = output.join(format!("trial-{trial}-{mode}"));
        std::fs::create_dir(&state)?;
        let runtime_state = state.join("runtime");
        std::fs::create_dir(&runtime_state)?;
        let proxy = reserve_address()?;
        let backend = reserve_address()?;
        let logfile = std::fs::File::create(state.join("child.log"))?;
        let child = Command::new(std::env::current_exe()?)
            .args(["--ignored", "--exact", "pingora_child_process", "--nocapture"])
            .env(CHILD_ENV, "1")
            .env(LISTEN_ENV, proxy.to_string())
            .env(BACKEND_ENV, backend.to_string())
            .env(STATE_ENV, &runtime_state)
            .env("ARROWCOL_ROUTER_SQL_MODE", mode)
            .stdout(Stdio::from(logfile.try_clone()?))
            .stderr(Stdio::from(logfile))
            .spawn()?;
        let mut guard = ChildGuard {
            child: Some(child),
            state: runtime_state.to_string_lossy().into_owned(),
        };
        wait_for_port(proxy, Duration::from_secs(30)).await?;
        wait_for_port(backend, Duration::from_secs(30)).await?;
        let channel = Endpoint::from_shared(format!("http://{proxy}"))?
            .timeout(Duration::from_secs(10))
            .connect()
            .await?;
        let signer = EdgeRequestSigner::new(vec![1; 32])?;
        let nonce = Arc::new(AtomicU64::new(1));
        let unauthorized = upload(
            FlightServiceClient::new(channel.clone()),
            signer.clone(),
            Arc::clone(&nonce),
            1,
            16,
            false,
            false,
        )
        .await;
        anyhow::ensure!(
            unauthorized.is_err_and(|error| error.code() == Code::Unauthenticated),
            "unsigned request admitted"
        );
        let before = stats(&runtime_state).await?;
        let cold = phase(&channel, &signer, &nonce, CARDINALITY).await?;
        let after_cold = stats(&runtime_state).await?;
        if mode != "memory_control" {
            verify_sql_owners().await?;
            anyhow::ensure!(
                after_cold["operations"]["claim_lease"].as_u64() == Some(CARDINALITY as u64),
                "cold requests did not claim real SQL leases"
            );
        }
        let warm = phase(&channel, &signer, &nonce, WARM_REQUEST_PAIRS).await?;
        let after_warm = stats(&runtime_state).await?;
        anyhow::ensure!(
            after_cold["operations"] == after_warm["operations"],
            "warm requests unexpectedly consulted SQL"
        );
        anyhow::ensure!(after_warm["failures"] == 0, "SQL operation failed");
        if mode != "memory_control" {
            verify_sql_owners().await?;
        }
        let record = json!({
            "trial":trial, "pair":if trial < 6 {Some(trial/2)} else {None}, "mode":mode,
            "route_cardinality":CARDINALITY, "request_mix":"signed DoAction then valid IPC DoPut, round-robin 16 tables",
            "cold":cold, "warm":warm, "sql_before":before, "sql_after_cold":after_cold, "sql_after_warm":after_warm,
            "auth_rejection_checked":true, "stale_control_and_ingest_checked":true, "sql_owners_checked":mode != "memory_control",
        });
        std::fs::write(state.join("measurement.json"), serde_json::to_vec_pretty(&record)?)?;
        records.push(record);
        guard.terminate()?;
        guard.wait(Duration::from_secs(20))?;
    }
    reset_fixture().await?;
    std::fs::write(
        output.join("fixture-results.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version":1, "baseline":"sql_unpooled", "candidate":"sql_pooled (existing pool max size 2)",
            "matched_pairs":3, "order":"AB BA AB", "production_source_changed":false, "records":records,
        }))?,
    )?;
    Ok(())
}
