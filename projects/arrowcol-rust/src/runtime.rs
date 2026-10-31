use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use arrowcol_buffer::{BufferManager, PublicationSink, QuiverStage, StageLeases};
use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, HiveMetastore, HiveMetastoreConfig, Metastore,
    SqlServerCatalogStore, SqlServerConfig as CatalogSqlConfig,
};
use arrowcol_flight::{
    AdminHandlers, ArrowColFlightService, Authenticator, BatchAdmission, CapabilityReporter, DrainController,
    DrainOutcome, HealthReporter, PressureHooks, PressureReporter, ReadinessReporter, TrustedIdentityValidator,
};
use arrowcol_protocol::CollectorInstanceId;
use arrowcol_receipts::{ReceiptStore, SqlServerReceiptConfig, SqlServerReceiptStore};
use arrowcol_routing::{CollectorState, RoutingStore, SqlServerRoutingConfig, SqlServerRoutingStore};
use arrowcol_sql_server::{ConnectionConfig, PoolConfig, SqlPool};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::{error, info, warn};

use crate::admin::{AdminState, serve as serve_admin};
use crate::auth::{CollectorAuthenticator, RejectInternalIdentityClaims, RouterIdentityValidator, allow_all_context};
use crate::config::{AuthMode, CollectorConfig};
use crate::control::{
    CollectorStatus, TableActivity, finish_owner_drain, health_probe_table, set_routing_state, start_control_loops,
    start_owner_drain,
};
use crate::delta_sink::{ProductionDeltaSink, verify_writable_root};
use crate::ingest::DirectAdmission;
use crate::lease::{TableLeaseManager, now_unix_millis};
use crate::metrics::CollectorMetrics;

/// Final graceful-shutdown result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunOutcome {
    /// Every admitted stream, buffer, Delta commit, receipt, and lease drained.
    Clean,
    /// One or more publication groups require reconciliation.
    IncompleteDrain {
        /// Number of incomplete groups.
        incomplete: usize,
    },
}

/// Runs the production direct collector until a signal or fatal control error.
///
/// # Errors
///
/// Returns startup, listener, control-loop, or graceful-shutdown failures.
pub async fn run(mut config: CollectorConfig) -> Result<RunOutcome> {
    config.validate()?;
    let root = config.delta.root.to_string_lossy().into_owned();
    let generation = tokio::task::spawn_blocking(move || arrowcol_buffer::CloudGeneration::capture(&root))
        .await
        .context("validate cloud generation task")??;
    if let Some(generation) = &generation {
        config.identity.instance_key = generation.bind_instance_key(&config.identity.instance_key);
        config.validate()?;
        if let Some(stage) = config.buffer.native_quiver.stage_config()? {
            generation.bind_durable_directory(&stage.directory)?;
            let persisted = generation.persisted_directory_generation(&stage.directory)?;
            generation.require_persisted_generation(&persisted).await?;
        }
    }
    let instance_id = config.instance_id();
    let password = String::from_utf8(read_secret(&config.sql_server.password_file)?)
        .context("SQL Server password secret is not UTF-8")?;
    let sql_connect = Duration::from_millis(config.sql_server.connect_timeout_ms);
    let sql_command = Duration::from_millis(config.sql_server.command_timeout_ms);
    let connection = ConnectionConfig::new(
        &config.sql_server.host,
        config.sql_server.port,
        &config.sql_server.database,
        &config.sql_server.username,
        password.clone(),
    )
    .trust_server_certificate(config.sql_server.trust_server_certificate)
    .with_timeouts(sql_connect, sql_command);
    let create_pool = |max_size| {
        SqlPool::new(
            connection.clone(),
            PoolConfig {
                max_size,
                acquire_timeout: Duration::from_millis(config.sql_server.pool.acquire_timeout_ms),
            },
        )
    };
    let receipt_pool =
        create_pool(config.sql_server.pool.receipts_max_size).context("create receipt SQL connection pool")?;
    let routing_pool =
        create_pool(config.sql_server.pool.routing_max_size).context("create routing SQL connection pool")?;
    let catalog_pool =
        create_pool(config.sql_server.pool.catalog_max_size).context("create catalog SQL connection pool")?;

    let catalog_store: Arc<dyn CatalogStore> = Arc::new(
        SqlServerCatalogStore::new(
            CatalogSqlConfig::new(
                &config.sql_server.host,
                config.sql_server.port,
                &config.sql_server.database,
                &config.sql_server.username,
                password.clone(),
            )
            .trust_server_certificate(config.sql_server.trust_server_certificate)
            .with_timeouts(sql_connect, sql_command),
        )
        .with_pool(catalog_pool.clone())?,
    );
    let metastore: Arc<dyn Metastore> = Arc::new(HiveMetastore::new(
        HiveMetastoreConfig::new(&config.hive.host, config.hive.port)
            .with_warehouse_root(&config.hive.warehouse_root)
            .with_owner(&config.hive.owner)
            .with_connect_timeout(Duration::from_millis(config.hive.connect_timeout_ms)),
    ));
    let (catalog_control, reconciliation) =
        CatalogControlPlane::start(DdlParser::default(), Arc::clone(&catalog_store), Arc::clone(&metastore))
            .await
            .context("start catalog control plane and reconcile incomplete changes")?;
    if !reconciliation.failures.is_empty() {
        anyhow::bail!(
            "catalog reconciliation left {} incomplete schema change(s)",
            reconciliation.failures.len()
        );
    }
    catalog_store
        .incomplete_sagas()
        .await
        .context("probe catalog SQL store")?;
    metastore
        .table(&health_probe_table()?)
        .await
        .context("probe Hive Metastore")?;
    verify_writable_root(&config.delta.root)
        .await
        .context("verify writable Delta root")?;

    let receipt_store: Arc<dyn ReceiptStore> = Arc::new(
        SqlServerReceiptStore::new(
            SqlServerReceiptConfig::new(
                &config.sql_server.host,
                config.sql_server.port,
                &config.sql_server.database,
                &config.sql_server.username,
                password.clone(),
            )
            .trust_server_certificate(config.sql_server.trust_server_certificate)
            .with_timeouts(sql_connect, sql_command)
            .with_batch_limits(config.receipts.mutation_batch_size, config.receipts.status_batch_size)?,
        )
        .with_pool(receipt_pool.clone())?,
    );
    let routing_store: Arc<dyn RoutingStore> = Arc::new(
        SqlServerRoutingStore::new(
            SqlServerRoutingConfig::new(
                &config.sql_server.host,
                config.sql_server.port,
                &config.sql_server.database,
                &config.sql_server.username,
                password,
            )
            .trust_server_certificate(config.sql_server.trust_server_certificate)
            .with_timeouts(sql_connect, sql_command),
        )
        .with_pool(routing_pool.clone())?,
    );

    let drain = DrainController::new();
    let metrics = CollectorMetrics::new()?;
    metrics.register_sql_pools(vec![
        ("receipts", receipt_pool.clone()),
        ("routing", routing_pool.clone()),
        ("catalog", catalog_pool.clone()),
    ])?;
    let activity = Arc::new(TableActivity::default());
    let leases = Arc::new(TableLeaseManager::new(
        Arc::clone(&receipt_store),
        Arc::clone(&routing_store),
        instance_id,
        Duration::from_millis(config.receipts.table_lease_ttl_ms),
    ));
    let sink: Arc<dyn PublicationSink> = Arc::new(ProductionDeltaSink::new(
        config.delta.root.clone(),
        config.delta.commit_options()?,
        Arc::clone(&leases),
        metrics.clone(),
    ));
    let buffer = BufferManager::new(config.buffer.buffer_config()?, Arc::clone(&receipt_store), sink)?;
    let quiver = match config.buffer.native_quiver.stage_config()? {
        Some(stage_config) => {
            let stage_leases: Arc<dyn StageLeases> = Arc::<TableLeaseManager>::clone(&leases);
            let stage = QuiverStage::open(
                stage_config,
                Arc::clone(&buffer),
                Arc::clone(&receipt_store),
                stage_leases,
                instance_id,
                Duration::from_millis(config.buffer.receipt_lease_duration_ms),
            )
            .await
            .context("open Quiver durable stage")?;
            metrics
                .register_quiver(Arc::clone(&stage))
                .context("register Quiver metrics")?;
            Some(stage)
        }
        None => None,
    };
    let mut collector_status = CollectorStatus::new(
        &config,
        instance_id,
        drain.clone(),
        Arc::clone(&buffer),
        Arc::clone(&leases),
        Arc::clone(&activity),
        metrics.clone(),
    )?;
    if let Some(stage) = &quiver {
        collector_status = collector_status.with_quiver(Arc::clone(stage));
    }
    let status = Arc::new(collector_status);
    status.set_dependencies_ready(true);
    status.set_delta_writable(true);

    let (authenticator, trusted_identity) = build_auth(&config, Arc::clone(&routing_store), instance_id)?;
    let mut direct_admission = DirectAdmission::new(
        Arc::clone(&catalog_store),
        Arc::clone(&receipt_store),
        Arc::clone(&buffer),
        Arc::clone(&leases),
        activity,
        instance_id,
        Duration::from_millis(config.receipts.receipt_lease_duration_ms),
        Duration::from_millis(config.receipts.admission_timeout_ms),
    );
    if let Some(stage) = &quiver {
        direct_admission = direct_admission.with_quiver(Arc::clone(stage));
    }
    let admission: Arc<dyn BatchAdmission> = Arc::new(direct_admission);
    let pressure_hooks: Arc<dyn PressureHooks> = Arc::new(metrics.clone());
    let ddl: Arc<dyn FlightSqlDdlHandler> = Arc::new(catalog_control);
    let flight = ArrowColFlightService::new(
        arrowcol_protocol::ImplementationPool::Direct,
        Arc::clone(&catalog_store),
        authenticator,
        admission,
        config.flight.runtime_limits()?,
    )?
    .with_trusted_identity_validator(trusted_identity)
    .with_ddl_handler(ddl)
    .with_pressure_hooks(pressure_hooks)
    .with_drain_controller(drain.clone())
    .into_tonic_service();

    let health: Arc<dyn HealthReporter> = Arc::<CollectorStatus>::clone(&status);
    let readiness: Arc<dyn ReadinessReporter> = Arc::<CollectorStatus>::clone(&status);
    let capabilities: Arc<dyn CapabilityReporter> = Arc::<CollectorStatus>::clone(&status);
    let pressure: Arc<dyn PressureReporter> = Arc::<CollectorStatus>::clone(&status);
    let admin_handlers = AdminHandlers::new(health, readiness, capabilities, pressure);
    let admin_state = AdminState::new(
        admin_handlers,
        Arc::clone(&receipt_store),
        config.admin.max_receipt_ids,
        metrics.clone(),
    );
    let flight_listener = TcpListener::bind(config.flight.bind)
        .await
        .with_context(|| format!("bind Flight listener {}", config.flight.bind))?;
    let admin_listener = TcpListener::bind(config.admin.bind)
        .await
        .with_context(|| format!("bind admin listener {}", config.admin.bind))?;
    let flight_builder = flight_server_builder(&config)?;

    let cancellation = CancellationToken::new();
    let mut loops = start_control_loops(
        &config,
        instance_id,
        Arc::clone(&status),
        Arc::clone(&routing_store),
        Arc::clone(&receipt_store),
        Arc::clone(&catalog_store),
        Arc::clone(&metastore),
        Arc::clone(&leases),
        drain.clone(),
        metrics.clone(),
        cancellation.clone(),
    )
    .await?;

    if let Some(stage) = &quiver {
        // The receipt owner is registered now, so persisted batches (including
        // a crashed predecessor's) can be adopted and published.
        stage.start();
    }

    let server_cancel = CancellationToken::new();
    let mut flight_task = spawn_flight_server(flight_builder, flight_listener, flight, server_cancel.clone());
    let mut admin_task = tokio::spawn(serve_admin(admin_listener, admin_state, server_cancel.clone()));
    info!(
        %instance_id,
        flight = %config.flight.bind,
        admin = %config.admin.bind,
        delta_root = %config.delta.root.display(),
        "direct Arrow Flight collector is running"
    );

    let stop = tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            StopReason::Signal
        }
        fatal = loops.fatal.recv() => {
            StopReason::Fatal(fatal.unwrap_or_else(|| anyhow!("all control loops stopped unexpectedly")))
        }
        result = &mut flight_task => {
            StopReason::Fatal(join_server("Flight", result))
        }
        result = &mut admin_task => {
            StopReason::Fatal(join_server("admin", result))
        }
    };
    let fatal = match stop {
        StopReason::Signal => None,
        StopReason::Fatal(error) => {
            status.set_fatal("collector server or control loop failed");
            Some(error)
        }
    };

    let outcome = shutdown(
        &config,
        instance_id,
        &status,
        &routing_store,
        &receipt_store,
        &drain,
        &buffer,
        quiver.as_ref(),
        &leases,
        &metrics,
        &cancellation,
    )
    .await;
    cancellation.cancel();
    server_cancel.cancel();
    for task in loops.tasks {
        let _ = task.await;
    }
    if !flight_task.is_finished() {
        let _ = flight_task.await;
    }
    if !admin_task.is_finished() {
        let _ = admin_task.await;
    }
    receipt_pool.close();
    routing_pool.close();
    catalog_pool.close();

    match (fatal, outcome) {
        (Some(fatal), Ok(outcome)) => Err(fatal.context(format!("collector stopped after {outcome:?}"))),
        (Some(fatal), Err(shutdown)) => Err(fatal.context(format!("graceful shutdown also failed: {shutdown:#}"))),
        (None, outcome) => outcome,
    }
}

enum StopReason {
    Signal,
    Fatal(anyhow::Error),
}

#[allow(clippy::too_many_arguments)]
async fn shutdown(
    config: &CollectorConfig,
    instance_id: CollectorInstanceId,
    status: &CollectorStatus,
    routing: &Arc<dyn RoutingStore>,
    receipts: &Arc<dyn ReceiptStore>,
    drain: &DrainController,
    buffer: &Arc<BufferManager>,
    quiver: Option<&Arc<QuiverStage>>,
    leases: &Arc<TableLeaseManager>,
    metrics: &CollectorMetrics,
    control_loops: &CancellationToken,
) -> Result<RunOutcome> {
    info!("stopping new Flight admission");
    drain.begin_drain();
    status.set_registration_ready(false);
    let mut incomplete = 0_usize;
    if let Err(error) = set_routing_state(routing.as_ref(), instance_id, CollectorState::Draining).await {
        warn!(%error, "could not mark routing registration draining");
        incomplete = incomplete.saturating_add(1);
    }
    match drain
        .graceful_stop(Duration::from_millis(config.shutdown.stream_drain_timeout_ms))
        .await
    {
        DrainOutcome::Drained => {}
        DrainOutcome::TimedOut { active } => {
            error!(active, "Flight stream drain timed out and was force-cancelled");
            incomplete = incomplete.saturating_add(active.max(1));
        }
    }
    match start_owner_drain(receipts.as_ref(), instance_id).await {
        Ok(owner) => {
            info!(
                active_receipts = owner.active_receipts,
                active_table_leases = owner.active_table_leases,
                "receipt owner entered draining state"
            );
        }
        Err(error) => {
            warn!(%error, "could not persist receipt owner drain state");
            incomplete = incomplete.saturating_add(1);
        }
    }

    leases.start_delta_draining().await;
    // One publication budget covers the Quiver drain and the buffer shutdown,
    // so a bounded failure is recorded before the supervisor's stop deadline.
    let publication_deadline =
        tokio::time::Instant::now() + Duration::from_millis(config.shutdown.publication_drain_timeout_ms);
    let mut quiver_drained = false;
    if let Some(stage) = quiver {
        quiver_drained = drain_quiver(stage, buffer, publication_deadline).await;
        if !quiver_drained {
            error!("Quiver backlog did not drain; persisted batches remain on disk for the next generation");
            incomplete = incomplete.saturating_add(1);
        }
    }
    let buffer_report = tokio::time::timeout_at(publication_deadline, buffer.shutdown()).await;
    match buffer_report {
        Ok(Ok(report)) => {
            incomplete = incomplete.saturating_add(report.incomplete.len());
            for work in &report.incomplete {
                error!(
                    table = %work.table,
                    receipts = ?work.receipt_ids,
                    reason = %work.reason,
                    durable_publication = ?work.durable_publication,
                    "publication group requires reconciliation"
                );
            }
        }
        Ok(Err(error)) => {
            error!(%error, "buffer shutdown failed");
            incomplete = incomplete.saturating_add(1);
        }
        Err(_) => {
            error!("buffer publication drain timed out");
            leases.invalidate_all("publication drain timed out").await;
            incomplete = incomplete.saturating_add(1);
        }
    }

    if quiver_drained && let Err(error) = fail_unbuffered_predecessor_receipts(receipts.as_ref(), instance_id).await {
        warn!(%error, "could not fail predecessor receipts that were never durably buffered");
        incomplete = incomplete.saturating_add(1);
    }

    if let Err(error) = leases.release_all().await {
        warn!(%error, "one or more table leases could not be released");
        incomplete = incomplete.saturating_add(1);
    }
    // The owner and routing heartbeats keep both leases alive through drain.
    // Stop them before the terminal writes, so a tick after `stopped` cannot
    // be fenced and fail the already-finished collector closed.
    control_loops.cancel();
    if incomplete == 0
        && let Err(error) = finish_owner_drain(receipts.as_ref(), instance_id).await
    {
        warn!(%error, "could not finish receipt owner drain");
        incomplete = incomplete.saturating_add(1);
    }
    let terminal_state = if incomplete == 0 {
        CollectorState::Stopped
    } else {
        CollectorState::Failed
    };
    if let Err(error) = set_routing_state(routing.as_ref(), instance_id, terminal_state).await {
        warn!(%error, "could not persist terminal routing state");
        incomplete = incomplete.saturating_add(1);
    }
    metrics.incomplete_shutdown(incomplete);
    if incomplete == 0 {
        Ok(RunOutcome::Clean)
    } else {
        Ok(RunOutcome::IncompleteDrain { incomplete })
    }
}

/// Bound on sweeps of predecessor receipts per shutdown, at the store's batch limit.
const UNBUFFERED_RECEIPT_SWEEPS: usize = 64;
const UNBUFFERED_RECEIPT_BATCH: usize = 256;

/// Fails `accepted` receipts a crashed predecessor created but never persisted
/// to Quiver. The predecessor never acknowledged them, and once this
/// generation has drained every persisted bundle no recovery can publish
/// them, so they would otherwise stay active forever.
async fn fail_unbuffered_predecessor_receipts(
    receipts: &dyn ReceiptStore,
    instance_id: CollectorInstanceId,
) -> Result<()> {
    for _ in 0..UNBUFFERED_RECEIPT_SWEEPS {
        let report = receipts
            .fail_unbuffered_predecessor_receipts(instance_id, now_unix_millis()?, UNBUFFERED_RECEIPT_BATCH)
            .await
            .context("fail unbuffered predecessor receipts")?;
        for record in &report.reconciled {
            warn!(
                receipt_id = %record.receipt_id,
                table = %record.table,
                "failed a predecessor receipt that was never durably buffered or acknowledged"
            );
        }
        if !report.limit_reached {
            return Ok(());
        }
    }
    anyhow::bail!("unbuffered predecessor receipts remain after {UNBUFFERED_RECEIPT_SWEEPS} bounded sweeps")
}

/// Publishes every persisted batch before the buffer stops, flushing buffered
/// groups immediately instead of waiting for their age trigger. Returns whether
/// the Quiver backlog drained before the deadline; the stage is stopped either way.
async fn drain_quiver(stage: &Arc<QuiverStage>, buffer: &Arc<BufferManager>, deadline: tokio::time::Instant) -> bool {
    let drained = loop {
        if let Err(error) = buffer.drain().await {
            warn!(%error, "explicit buffer drain failed while draining Quiver");
        }
        if stage.drain(Duration::from_millis(250)).await {
            break true;
        }
        if tokio::time::Instant::now() >= deadline {
            break false;
        }
    };
    stage.shutdown().await;
    drained
}

fn build_auth(
    config: &CollectorConfig,
    routing: Arc<dyn RoutingStore>,
    instance_id: CollectorInstanceId,
) -> Result<(Arc<dyn Authenticator>, Arc<dyn TrustedIdentityValidator>)> {
    let trusted: Arc<dyn TrustedIdentityValidator> = match config.auth.mode {
        AuthMode::RouterHmac => Arc::new(RouterIdentityValidator::new(
            read_secret(
                config
                    .auth
                    .internal_hmac_secret_file
                    .as_deref()
                    .context("auth.internal_hmac_secret_file is required")?,
            )?,
            config.auth.internal_identity_max_ttl_ms,
        )?),
        AuthMode::StaticBearer | AuthMode::AllowAll => Arc::new(RejectInternalIdentityClaims),
    };
    let bearer = match config.auth.mode {
        AuthMode::StaticBearer => Some(read_secret(
            config
                .auth
                .bearer_token_file
                .as_deref()
                .context("auth.bearer_token_file is required")?,
        )?),
        AuthMode::RouterHmac | AuthMode::AllowAll => None,
    };
    let allow_all = match config.auth.mode {
        AuthMode::AllowAll => Some(allow_all_context(
            config
                .auth
                .allow_all_subject
                .as_deref()
                .context("auth.allow_all_subject is required")?,
        )?),
        AuthMode::RouterHmac | AuthMode::StaticBearer => None,
    };
    let authenticator: Arc<dyn Authenticator> = Arc::new(CollectorAuthenticator::new(
        config.auth.mode,
        bearer,
        allow_all,
        routing,
        instance_id,
        config.routing.require_lease_headers,
    ));
    Ok((authenticator, trusted))
}

fn flight_server_builder(config: &CollectorConfig) -> Result<Server> {
    let mut builder = Server::builder().concurrency_limit_per_connection(
        usize::try_from(config.flight.max_concurrent_streams).context("flight.max_concurrent_streams exceeds usize")?,
    );
    if config.flight.tls.enabled {
        let certificate = std::fs::read(
            config
                .flight
                .tls
                .certificate_file
                .as_deref()
                .context("Flight TLS certificate is missing")?,
        )
        .context("read Flight TLS certificate")?;
        let private_key = std::fs::read(
            config
                .flight
                .tls
                .private_key_file
                .as_deref()
                .context("Flight TLS private key is missing")?,
        )
        .context("read Flight TLS private key")?;
        let mut tls = ServerTlsConfig::new().identity(Identity::from_pem(certificate, private_key));
        if let Some(client_ca) = config.flight.tls.client_ca_file.as_deref() {
            tls = tls.client_ca_root(Certificate::from_pem(
                std::fs::read(client_ca).context("read Flight client CA")?,
            ));
        }
        builder = builder.tls_config(tls)?;
    }
    Ok(builder)
}

fn spawn_flight_server(
    mut builder: Server,
    listener: TcpListener,
    service: arrow_flight::flight_service_server::FlightServiceServer<ArrowColFlightService>,
    cancellation: CancellationToken,
) -> JoinHandle<Result<(), tonic::transport::Error>> {
    let incoming = TcpListenerStream::new(listener);
    tokio::spawn(
        builder
            .add_service(service)
            .serve_with_incoming_shutdown(incoming, cancellation.cancelled_owned()),
    )
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .context("install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("wait for Ctrl-C"),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("wait for Ctrl-C")
    }
}

fn join_server<E: std::fmt::Display>(
    name: &'static str,
    result: Result<Result<(), E>, tokio::task::JoinError>,
) -> anyhow::Error {
    match result {
        Ok(Ok(())) => anyhow!("{name} server stopped unexpectedly"),
        Ok(Err(error)) => anyhow!("{name} server failed: {error}"),
        Err(error) => anyhow!("{name} server task failed: {error}"),
    }
}

fn read_secret(path: &Path) -> Result<Vec<u8>> {
    let value = std::fs::read(path).with_context(|| format!("read secret file {}", path.display()))?;
    let value = value
        .strip_suffix(b"\n")
        .and_then(|value| value.strip_suffix(b"\r").or(Some(value)))
        .unwrap_or(&value);
    ensure!(!value.is_empty(), "secret file {} is empty", path.display());
    ensure!(
        !value.contains(&b'\n') && !value.contains(&b'\r'),
        "secret file {} must contain one line",
        path.display()
    );
    Ok(value.to_vec())
}
