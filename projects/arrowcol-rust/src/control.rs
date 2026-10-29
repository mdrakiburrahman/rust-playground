use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use arrowcol_buffer::{BufferManager, BufferPressureSnapshot, QuiverStage};
use arrowcol_catalog::{CatalogStore, Metastore};
use arrowcol_flight::{
    CapabilityReporter, DrainController, HealthReporter, HealthStatus, PressureReporter, ReadinessReporter,
    ReadinessStatus, RuntimeError, RuntimeResult,
};
use arrowcol_protocol::{
    BasisPoints, Capability, CollectorCapabilities, CollectorInstanceId, ImplementationPool, Measurement,
    PositiveMeasurement, PressureGauge, PressureSnapshot, ReceiptState, TableIdentity, UnixMillis,
};
use arrowcol_receipts::{OwnerRegistration, ReceiptErrorKind, ReceiptStore};
use arrowcol_routing::{
    CollectorRegistration, CollectorState, RichPressureSnapshot, RoutingStore, SubsystemState, TablePressure,
};
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::{CollectorConfig, RoutingConfig};
use crate::delta_sink::verify_writable_root;
use crate::lease::{TableLeaseManager, add_duration, now_unix_millis};
use crate::metrics::CollectorMetrics;

/// Exact active Flight stream counts keyed by table identity.
#[derive(Default)]
pub struct TableActivity {
    active: Mutex<ActivityState>,
}

#[derive(Default)]
struct ActivityState {
    streams: HashMap<TableIdentity, u64>,
    blocked: BTreeSet<TableIdentity>,
}

impl TableActivity {
    /// Enters one table-scoped stream and returns a drop guard.
    ///
    /// # Errors
    ///
    /// Returns an error if table admission is draining, the activity lock was
    /// poisoned, or the counter overflowed.
    pub fn enter(self: &Arc<Self>, table: TableIdentity) -> Result<ActiveTableStream> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| anyhow!("table activity lock poisoned"))?;
        if active.blocked.contains(&table) {
            return Err(anyhow!("table admission is draining"));
        }
        let count = active.streams.entry(table.clone()).or_default();
        *count = count.checked_add(1).context("table activity counter overflow")?;
        Ok(ActiveTableStream {
            activity: Arc::clone(self),
            table,
        })
    }

    fn snapshot(&self) -> Result<HashMap<TableIdentity, u64>> {
        self.active
            .lock()
            .map(|active| active.streams.clone())
            .map_err(|_| anyhow!("table activity lock poisoned"))
    }

    /// Serializes the per-table admission barrier with stream entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the activity lock was poisoned.
    pub fn block(&self, table: &TableIdentity) -> Result<u64> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| anyhow!("table activity lock poisoned"))?;
        active.blocked.insert(table.clone());
        Ok(active.streams.get(table).copied().unwrap_or(0))
    }

    /// Opens admission after authoritative target activation.
    ///
    /// # Errors
    ///
    /// Returns an error if the activity lock was poisoned.
    pub fn unblock(&self, table: &TableIdentity) -> Result<()> {
        self.active
            .lock()
            .map_err(|_| anyhow!("table activity lock poisoned"))?
            .blocked
            .remove(table);
        Ok(())
    }
}

/// Drop guard for one active table stream.
pub struct ActiveTableStream {
    activity: Arc<TableActivity>,
    table: TableIdentity,
}

impl Drop for ActiveTableStream {
    fn drop(&mut self) {
        let Ok(mut active) = self.activity.active.lock() else {
            return;
        };
        if let Some(count) = active.streams.get_mut(&self.table) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                active.streams.remove(&self.table);
            }
        }
    }
}

/// Synchronous health/readiness state plus async pressure/capability reporters.
pub struct CollectorStatus {
    instance_id: CollectorInstanceId,
    drain: DrainController,
    buffer: Arc<BufferManager>,
    quiver: Option<Arc<QuiverStage>>,
    leases: Arc<TableLeaseManager>,
    activity: Arc<TableActivity>,
    metrics: CollectorMetrics,
    max_request_bytes: PositiveMeasurement,
    max_concurrent_streams: PositiveMeasurement,
    max_pending_receipts: PositiveMeasurement,
    max_pending_publications: u64,
    routing: RoutingConfig,
    flags: RwLock<StatusFlags>,
    resources: Mutex<ResourceSampler>,
}

#[derive(Clone, Debug)]
struct StatusFlags {
    healthy: bool,
    fatal_reason: Option<String>,
    dependencies_ready: bool,
    registration_ready: bool,
    pressure_ready: bool,
    lease_loop_ready: bool,
    delta_writable: bool,
}

impl CollectorStatus {
    /// Creates process status before dependency startup completes.
    ///
    /// # Errors
    ///
    /// Rejects protocol measurement overflow.
    pub fn new(
        config: &CollectorConfig,
        instance_id: CollectorInstanceId,
        drain: DrainController,
        buffer: Arc<BufferManager>,
        leases: Arc<TableLeaseManager>,
        activity: Arc<TableActivity>,
        metrics: CollectorMetrics,
    ) -> Result<Self> {
        Ok(Self {
            instance_id,
            drain,
            buffer,
            quiver: None,
            leases,
            activity,
            metrics,
            max_request_bytes: PositiveMeasurement::new(
                u64::try_from(config.flight.max_message_bytes).context("Flight request size exceeds protocol range")?,
            )?,
            max_concurrent_streams: PositiveMeasurement::new(config.flight.max_concurrent_streams)?,
            max_pending_receipts: PositiveMeasurement::new(config.buffer.global_capacity.batches)?,
            max_pending_publications: publication_capacity(config)?,
            routing: config.routing.clone(),
            flags: RwLock::new(StatusFlags {
                healthy: true,
                fatal_reason: None,
                dependencies_ready: false,
                registration_ready: false,
                pressure_ready: false,
                lease_loop_ready: false,
                delta_writable: false,
            }),
            resources: Mutex::new(ResourceSampler::new()?),
        })
    }

    /// Reports the Quiver durable stage in capabilities and pressure.
    #[must_use]
    pub fn with_quiver(mut self, quiver: Arc<QuiverStage>) -> Self {
        self.quiver = Some(quiver);
        self
    }

    /// Marks SQL/catalog/Hive dependency readiness.
    pub fn set_dependencies_ready(&self, ready: bool) {
        if let Ok(mut flags) = self.flags.write() {
            flags.dependencies_ready = ready;
        }
    }

    /// Marks routing and receipt owner registration readiness.
    pub fn set_registration_ready(&self, ready: bool) {
        if let Ok(mut flags) = self.flags.write() {
            flags.registration_ready = ready;
        }
    }

    /// Marks successful rich pressure publication readiness.
    pub fn set_pressure_ready(&self, ready: bool) {
        if let Ok(mut flags) = self.flags.write() {
            flags.pressure_ready = ready;
        }
    }

    /// Marks the routing/publication lease renewal loop readiness.
    pub fn set_lease_loop_ready(&self, ready: bool) {
        if let Ok(mut flags) = self.flags.write() {
            flags.lease_loop_ready = ready;
        }
    }

    /// Marks the Delta root writable probe.
    pub fn set_delta_writable(&self, ready: bool) {
        if let Ok(mut flags) = self.flags.write() {
            flags.delta_writable = ready;
        }
    }

    /// Fails health/readiness closed with a safe operator reason.
    pub fn set_fatal(&self, reason: impl Into<String>) {
        if let Ok(mut flags) = self.flags.write() {
            flags.healthy = false;
            flags.fatal_reason = Some(reason.into());
            flags.registration_ready = false;
            flags.pressure_ready = false;
            flags.lease_loop_ready = false;
        }
        self.drain.begin_drain();
    }

    /// Returns whether every required startup/control signal is currently ready.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        let Ok(flags) = self.flags.read() else {
            return false;
        };
        flags.healthy
            && flags.dependencies_ready
            && flags.registration_ready
            && flags.pressure_ready
            && flags.lease_loop_ready
            && flags.delta_writable
            && self.drain.is_accepting()
            && self
                .buffer
                .pressure()
                .is_ok_and(|pressure| pressure.accepting && pressure.tables.iter().all(|table| !table.poisoned))
    }

    /// Builds the richer SQL routing pressure snapshot.
    ///
    /// # Errors
    ///
    /// Returns buffer synchronization or protocol measurement failures.
    pub async fn rich_pressure(&self) -> Result<RichPressureSnapshot> {
        let now = now_unix_millis()?;
        let pressure = self.buffer.pressure().map_err(|error| anyhow!(error))?;
        let known: HashMap<_, _> = self.leases.known_tables().await.into_iter().collect();
        let active_by_table = self.activity.snapshot()?;
        let mut seen = BTreeSet::new();
        let mut tables = pressure
            .tables
            .iter()
            .filter_map(|table_pressure| {
                let table_id = table_pressure.table_id?;
                let table = known.get(&table_id)?.clone();
                seen.insert(table.clone());
                Some(table_pressure_document(
                    table.clone(),
                    table_pressure,
                    active_by_table.get(&table).copied().unwrap_or(0),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        for (table, active) in active_by_table {
            if seen.insert(table.clone()) {
                tables.push(TablePressure {
                    table,
                    active_streams: Measurement::new(active)?,
                    buffered_bytes: Measurement::ZERO,
                    buffered_rows: Measurement::ZERO,
                    buffered_batches: Measurement::ZERO,
                    oldest_buffer_age_ms: Measurement::ZERO,
                    pending_commits: Measurement::ZERO,
                });
            }
        }
        for table in known.values() {
            if seen.insert(table.clone()) {
                tables.push(TablePressure {
                    table: table.clone(),
                    active_streams: Measurement::ZERO,
                    buffered_bytes: Measurement::ZERO,
                    buffered_rows: Measurement::ZERO,
                    buffered_batches: Measurement::ZERO,
                    oldest_buffer_age_ms: Measurement::ZERO,
                    pending_commits: Measurement::ZERO,
                });
            }
        }
        tables.sort_by(|left, right| left.table.cmp(&right.table));
        tables.truncate(self.routing.max_table_entries);

        let active = u64::try_from(self.drain.active_streams()).unwrap_or(u64::MAX);
        let active = gauge(active, self.max_concurrent_streams.get())?;
        let quiver = self.quiver.as_ref().map(|stage| stage.snapshot());
        // Persisted-but-unpublished batches still count as buffered work so a
        // router never treats this collector as drained while Quiver holds data.
        let backlog = quiver.map_or(0, |snapshot| snapshot.backlog_bundles);
        let buffered_bytes = gauge(pressure.global.bytes, pressure.global.limits.bytes)?;
        let buffered_rows = gauge(pressure.global.rows, pressure.global.limits.rows)?;
        let buffered_batches = gauge(
            pressure.global.batches.saturating_add(backlog),
            pressure.global.limits.batches,
        )?;
        let active_flushes = u64::try_from(pressure.active_flushes).unwrap_or(u64::MAX);
        let pending_commits = gauge(active_flushes, self.max_pending_publications)?;
        let available = self.max_concurrent_streams.get().saturating_sub(active.current().get());
        let available_permits = gauge(available, self.max_concurrent_streams.get())?;
        let resources = self
            .resources
            .lock()
            .map_err(|_| anyhow!("resource sampler lock poisoned"))?
            .sample()?;
        let rss = resources.rss_bytes;
        let rss_limit = resources
            .memory_limit
            .unwrap_or(self.routing.rss_limit_bytes)
            .min(self.routing.rss_limit_bytes);
        tracing::debug!(
            cpu_bps = resources.cpu_bps,
            rss_bytes = rss,
            container_memory_bytes = resources.memory_bytes,
            container_memory_limit = resources.memory_limit,
            effective_cpu_cores = resources.cores,
            "measured collector resources"
        );
        let delta_pressure = pending_commits.utilization();
        let buffer_pressure = [
            buffered_bytes.utilization().get(),
            buffered_rows.utilization().get(),
            buffered_batches.utilization().get(),
        ]
        .into_iter()
        .max()
        .unwrap_or(0);
        let poisoned = pressure.tables.iter().any(|table| table.poisoned);
        let quiver_pressure = match quiver {
            Some(snapshot) => disk_pressure_bps(snapshot.disk_used_bytes, snapshot.disk_capacity_bytes),
            None => buffer_pressure,
        };
        Ok(RichPressureSnapshot {
            container_memory: match (resources.memory_bytes, resources.memory_limit) {
                (Some(current), Some(limit)) => Some(arrowcol_routing::ContainerMemory {
                    current_bytes: Measurement::new(current)?,
                    limit_bytes: PositiveMeasurement::new(limit)?,
                }),
                _ => None,
            },
            effective_cpu_millicores: Some(PositiveMeasurement::new((resources.cores * 1000.0).ceil() as u64)?),
            instance_id: self.instance_id,
            observed_at_unix_ms: now,
            cpu_utilization_bps: BasisPoints::new(resources.cpu_bps)?,
            rss_bytes: Measurement::new(rss)?,
            rss_limit_bytes: PositiveMeasurement::new(rss_limit)?,
            active_streams: active,
            received_bytes: Measurement::new(self.metrics.received_bytes())?,
            received_rows: Measurement::new(self.metrics.received_rows())?,
            received_batches: Measurement::new(self.metrics.received_batches())?,
            buffered_bytes,
            buffered_rows,
            buffered_batches,
            available_permits,
            pending_commits,
            oldest_buffer_age_ms: Measurement::ZERO,
            delta_pressure_bps: delta_pressure,
            quiver_pressure_bps: BasisPoints::new(quiver_pressure)?,
            dfe_pressure_bps: BasisPoints::new(0)?,
            delta_state: if poisoned {
                SubsystemState::Blocked
            } else if delta_pressure.get() >= 8_000 {
                SubsystemState::Degraded
            } else {
                SubsystemState::Ready
            },
            quiver_state: if poisoned || quiver_pressure >= 10_000 {
                SubsystemState::Blocked
            } else if quiver_pressure >= 8_000 {
                SubsystemState::Degraded
            } else {
                SubsystemState::Ready
            },
            dfe_state: SubsystemState::Disabled,
            accepting_deliveries: self.is_ready() && pressure.accepting,
            tables,
        })
    }
}

impl HealthReporter for CollectorStatus {
    fn health(&self) -> HealthStatus {
        match self.flags.read() {
            Ok(flags) if flags.healthy => HealthStatus::healthy(),
            Ok(flags) => HealthStatus::unhealthy(
                flags
                    .fatal_reason
                    .clone()
                    .unwrap_or_else(|| "collector control plane failed".to_owned()),
            ),
            Err(_) => HealthStatus::unhealthy("collector status lock failed"),
        }
    }
}

impl ReadinessReporter for CollectorStatus {
    fn readiness(&self) -> ReadinessStatus {
        let active = self.drain.active_streams();
        let accepting = self.drain.is_accepting();
        let ready = self.is_ready();
        let reason = if ready {
            None
        } else if !accepting {
            Some("collector is draining".to_owned())
        } else {
            Some("SQL/catalog, registration, lease, or Delta readiness is incomplete".to_owned())
        };
        ReadinessStatus::new(ready, accepting && ready, active, reason)
    }
}

#[async_trait]
impl CapabilityReporter for CollectorStatus {
    async fn capabilities(&self) -> RuntimeResult<CollectorCapabilities> {
        Ok(CollectorCapabilities::new(
            self.instance_id,
            ImplementationPool::Direct,
            self.max_request_bytes,
            self.max_concurrent_streams,
            self.max_pending_receipts,
            {
                let mut capabilities = BTreeSet::from([
                    Capability::ReceiptStatus,
                    Capability::DeltaPublication,
                    Capability::TableLeaseFencing,
                    Capability::PressureReporting,
                ]);
                if self.quiver.is_some() {
                    capabilities.insert(Capability::DurablePayloadStorage);
                }
                capabilities
            },
        ))
    }
}

#[async_trait]
impl PressureReporter for CollectorStatus {
    async fn pressure(&self) -> RuntimeResult<PressureSnapshot> {
        let now = now_unix_millis().map_err(|_| RuntimeError::internal())?;
        let pressure = self.buffer.pressure().map_err(|_| RuntimeError::internal())?;
        let active = gauge(
            u64::try_from(self.drain.active_streams()).unwrap_or(u64::MAX),
            self.max_concurrent_streams.get(),
        )
        .map_err(|_| RuntimeError::internal())?;
        let buffered =
            gauge(pressure.global.bytes, pressure.global.limits.bytes).map_err(|_| RuntimeError::internal())?;
        let publication = gauge(
            u64::try_from(pressure.active_flushes).unwrap_or(u64::MAX),
            self.max_pending_publications,
        )
        .map_err(|_| RuntimeError::internal())?;
        let memory = buffered.utilization();
        Ok(PressureSnapshot::new(
            self.instance_id,
            now,
            active,
            buffered,
            publication,
            BasisPoints::new(0).map_err(|_| RuntimeError::internal())?,
            memory,
            self.is_ready() && pressure.accepting,
        ))
    }
}

/// Background loops and their fatal-error receiver.
pub struct ControlLoops {
    /// Loop tasks.
    pub tasks: Vec<JoinHandle<()>>,
    /// First fail-closed loop error.
    pub fatal: mpsc::UnboundedReceiver<anyhow::Error>,
}

/// Registers the process, reconciles receipts, and starts heartbeat, pressure,
/// lease-renewal, and dependency-probe loops.
///
/// # Errors
///
/// Returns startup registration or reconciliation failures.
#[allow(clippy::too_many_arguments)]
pub async fn start_control_loops(
    config: &CollectorConfig,
    instance_id: CollectorInstanceId,
    status: Arc<CollectorStatus>,
    routing: Arc<dyn RoutingStore>,
    receipts: Arc<dyn ReceiptStore>,
    catalog: Arc<dyn CatalogStore>,
    metastore: Arc<dyn Metastore>,
    leases: Arc<TableLeaseManager>,
    drain: DrainController,
    metrics: CollectorMetrics,
    cancellation: CancellationToken,
) -> Result<ControlLoops> {
    reconcile_receipts(
        Arc::clone(&receipts),
        config.receipts.reconciliation_batch_size,
        &metrics,
    )
    .await?;
    let now = register_owner_when_predecessor_expires(config, instance_id, receipts.as_ref()).await?;
    let registration = registration(config, instance_id, now)?;
    routing
        .register_collector(&registration)
        .await
        .context("register routing collector")?;
    status.set_registration_ready(true);
    leases.renew_all().await?;
    status.set_lease_loop_ready(true);
    let initial_pressure = status.rich_pressure().await?;
    routing
        .publish_pressure(&initial_pressure)
        .await
        .context("publish initial routing pressure")?;
    status.set_pressure_ready(true);

    let (fatal_tx, fatal) = mpsc::unbounded_channel();
    let tasks = vec![
        tokio::spawn(owner_heartbeat_loop(
            config.receipts.owner_heartbeat_ms,
            config.receipts.owner_lease_ttl_ms,
            instance_id,
            Arc::clone(&receipts),
            Arc::clone(&status),
            Arc::clone(&leases),
            drain.clone(),
            metrics.clone(),
            cancellation.clone(),
            fatal_tx.clone(),
        )),
        tokio::spawn(routing_loop(
            config.routing.registration_heartbeat_ms,
            config.routing.registration_ttl_ms,
            config.routing.pressure_interval_ms,
            registration,
            Arc::clone(&routing),
            Arc::clone(&status),
            Arc::clone(&leases),
            drain.clone(),
            metrics.clone(),
            cancellation.clone(),
            fatal_tx.clone(),
        )),
        tokio::spawn(lease_loop(
            config.receipts.table_lease_renew_ms,
            routing.is_remote_authority(),
            Arc::clone(&leases),
            Arc::clone(&status),
            drain.clone(),
            metrics.clone(),
            cancellation.clone(),
            fatal_tx.clone(),
        )),
        tokio::spawn(dependency_loop(
            config.routing.dependency_probe_interval_ms,
            config.delta.root.clone(),
            Arc::clone(&catalog),
            metastore,
            Arc::clone(&status),
            metrics,
            cancellation.clone(),
        )),
        tokio::spawn(migration_loop(
            instance_id,
            routing,
            catalog,
            leases,
            status,
            cancellation,
        )),
    ];
    Ok(ControlLoops { tasks, fatal })
}

async fn migration_loop(
    instance_id: CollectorInstanceId,
    routing: Arc<dyn RoutingStore>,
    catalog: Arc<dyn CatalogStore>,
    leases: Arc<TableLeaseManager>,
    status: Arc<CollectorStatus>,
    cancellation: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => {
                let result = async {
                    for operation in routing.migration_work(instance_id).await? {
                        use arrowcol_routing::{MigrationAck, MigrationPhase};
                        match operation.phase {
                            MigrationPhase::Preparing if operation.target == instance_id => {
                                catalog.current_schema(&operation.key.table).await?
                                    .context("migration target schema is not registered")?;
                                if !status.is_ready() { anyhow::bail!("migration target dependencies not ready"); }
                                routing.acknowledge_migration(&operation, instance_id, MigrationAck::Prepared).await?;
                            }
                            MigrationPhase::Draining if operation.source == instance_id => {
                                // The barrier and stream entry share one mutex, so a zero count
                                // cannot race a new locally admitted RPC after this point.
                                let active = status.activity.block(&operation.key.table)?;
                                leases.start_table_draining(&operation.key.table).await;
                                let pressure = status.rich_pressure().await?;
                                if active != 0 || pressure.table_is_drained(&operation.key.table) != Some(true)
                                    || status.quiver.as_ref().is_some_and(|stage| stage.snapshot().backlog_bundles != 0) { continue; }
                                routing.publish_pressure(&pressure).await?;
                                leases.release_table(&operation.key.table).await?;
                                routing.acknowledge_migration(&operation, instance_id, MigrationAck::SourceReleased).await?;
                            }
                            MigrationPhase::Activating if operation.target == instance_id => {
                                let schema = catalog.current_schema(&operation.key.table).await?
                                    .context("target schema disappeared before activation")?;
                                leases.activate_table(schema.table_id(), &operation.key.table).await?;
                                routing.acknowledge_migration(&operation, instance_id, MigrationAck::TargetActive).await?;
                                status.activity.unblock(&operation.key.table)?;
                            }
                            MigrationPhase::Complete if operation.target == instance_id => {
                                let view = routing.load_view(now_unix_millis()?).await?;
                                let key = &operation.key;
                                if view.leases.get(key).is_some_and(|lease|
                                    lease.state == arrowcol_routing::LeaseState::Active
                                    && lease.holder_instance_id == instance_id
                                    && lease.fencing_epoch > operation.source_epoch) {
                                    status.activity.unblock(&operation.key.table)?;
                                }
                            }
                            _ => {}
                        }
                    }
                    Ok::<_, anyhow::Error>(())
                }.await;
                if let Err(error) = result {
                    // Lost acknowledgements/SQL uncertainty leave the move blocked; no forced transfer.
                    warn!(%error, "controlled migration pending; retrying exact operation");
                }
            }
        }
    }
}

/// Registers the receipt owner, waiting for a crashed predecessor with the
/// same instance key to expire. A restart right after a crash otherwise fails
/// because the predecessor's owner lease is still live.
async fn register_owner_when_predecessor_expires(
    config: &CollectorConfig,
    instance_id: CollectorInstanceId,
    receipts: &dyn ReceiptStore,
) -> Result<UnixMillis> {
    let retry_until = tokio::time::Instant::now()
        + Duration::from_millis(config.receipts.owner_lease_ttl_ms)
        + Duration::from_secs(5);
    loop {
        let now = now_unix_millis()?;
        let owner_expiry = add_duration(now, Duration::from_millis(config.receipts.owner_lease_ttl_ms))?;
        let registration = OwnerRegistration {
            instance_id,
            instance_key: config.identity.instance_key.clone(),
            generation: config.identity.generation,
            host_name: config.identity.host_name.clone(),
            build_version: config.identity.build_version.clone(),
            started_at_unix_ms: now,
            lease_deadline_unix_ms: owner_expiry,
            durable_recovery_window_ms: config.buffer.native_quiver.recovery_window_ms(),
        };
        match receipts.register_owner(registration).await {
            Ok(()) => return Ok(now),
            Err(error) if error.kind() == ReceiptErrorKind::Conflict && tokio::time::Instant::now() < retry_until => {
                info!(%error, "waiting for the previous owner lease to expire before registering");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(anyhow!(error)).context("register receipt owner"),
        }
    }
}

fn disk_pressure_bps(used: u64, capacity: u64) -> u16 {
    if capacity == 0 {
        return 10_000;
    }
    let bps = u128::from(used).saturating_mul(10_000) / u128::from(capacity);
    u16::try_from(bps.min(10_000)).unwrap_or(10_000)
}

async fn reconcile_receipts(receipts: Arc<dyn ReceiptStore>, limit: usize, metrics: &CollectorMetrics) -> Result<()> {
    loop {
        let report = receipts
            .reconcile_expired(now_unix_millis()?, limit)
            .await
            .context("reconcile expired receipts")?;
        for record in &report.reconciled {
            metrics.reconciled_receipt(match record.state {
                ReceiptState::Failed => "failed_retryable",
                ReceiptState::PublicationUnknown => "publication_unknown",
                _ => "unexpected",
            });
        }
        if !report.limit_reached {
            return Ok(());
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn owner_heartbeat_loop(
    interval_ms: u64,
    ttl_ms: u64,
    instance_id: CollectorInstanceId,
    receipts: Arc<dyn ReceiptStore>,
    status: Arc<CollectorStatus>,
    leases: Arc<TableLeaseManager>,
    drain: DrainController,
    metrics: CollectorMetrics,
    cancellation: CancellationToken,
    fatal: mpsc::UnboundedSender<anyhow::Error>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => {
                let result = async {
                    let now = now_unix_millis()?;
                    receipts
                        .heartbeat_owner(instance_id, add_duration(now, Duration::from_millis(ttl_ms))?)
                        .await
                        .context("heartbeat receipt owner")
                }.await;
                if let Err(error) = result {
                    fail_closed("receipt_owner", error, &status, &leases, &drain, &metrics, &fatal).await;
                    return;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn routing_loop(
    heartbeat_ms: u64,
    ttl_ms: u64,
    pressure_ms: u64,
    mut registration: CollectorRegistration,
    routing: Arc<dyn RoutingStore>,
    status: Arc<CollectorStatus>,
    leases: Arc<TableLeaseManager>,
    drain: DrainController,
    metrics: CollectorMetrics,
    cancellation: CancellationToken,
    fatal: mpsc::UnboundedSender<anyhow::Error>,
) {
    let instance_id = registration.instance_id;
    let mut heartbeat = tokio::time::interval(Duration::from_millis(heartbeat_ms));
    let mut pressure = tokio::time::interval(Duration::from_millis(pressure_ms));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    pressure.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    pressure.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = heartbeat.tick() => {
                let result = async {
                    let now = now_unix_millis()?;
                    routing
                        .heartbeat_collector(instance_id, now, add_duration(now, Duration::from_millis(ttl_ms))?)
                        .await
                        .context("heartbeat routing registration")
                }.await;
                match result {
                    Ok(()) => status.set_registration_ready(true),
                    Err(error) if routing.is_remote_authority() => {
                        status.set_registration_ready(false);
                        metrics.control_failure("routing_registration");
                        warn!(%error, "controller unavailable; admission remains unready");
                        // Recover only this still-running, authenticated process. The
                        // authority retains leases/retirement intent, never old health.
                        let recovery = async {
                            let now = now_unix_millis()?;
                            let view = routing.load_view(now).await?;
                            if !view.collectors.contains_key(&instance_id) {
                                registration.heartbeat_at_unix_ms = now;
                                registration.lease_expires_at_unix_ms =
                                    add_duration(now, Duration::from_millis(ttl_ms))?;
                                routing.register_collector(&registration).await?;
                            }
                            Ok::<(), anyhow::Error>(())
                        }.await;
                        if let Err(error) = recovery {
                            warn!(%error, "controller registration recovery remains blocked");
                        }
                    }
                    Err(error) => {
                        fail_closed("routing_registration", error, &status, &leases, &drain, &metrics, &fatal).await;
                        return;
                    }
                }
            }
            _ = pressure.tick() => {
                match status.rich_pressure().await {
                    Ok(snapshot) => {
                        if let Err(error) = routing.publish_pressure(&snapshot).await {
                            status.set_pressure_ready(false);
                            metrics.control_failure("pressure");
                            warn!(%error, "could not publish routing pressure snapshot");
                        } else {
                            status.set_pressure_ready(true);
                        }
                    }
                    Err(error) => {
                        status.set_pressure_ready(false);
                        metrics.control_failure("pressure");
                        warn!(%error, "could not construct routing pressure snapshot");
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn lease_loop(
    renew_ms: u64,
    remote_authority: bool,
    leases: Arc<TableLeaseManager>,
    status: Arc<CollectorStatus>,
    drain: DrainController,
    metrics: CollectorMetrics,
    cancellation: CancellationToken,
    fatal: mpsc::UnboundedSender<anyhow::Error>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(renew_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => {
                match leases.renew_all().await {
                    Ok(()) => status.set_lease_loop_ready(true),
                    Err(error) if remote_authority => {
                        status.set_lease_loop_ready(false);
                        metrics.control_failure("table_lease");
                        // SQL lease expiry/fencing and invalidated handles remain
                        // authoritative. A routing transport outage grants nothing.
                        warn!(%error, "publication lease unavailable; admission remains unready");
                    }
                    Err(error) => {
                        status.set_lease_loop_ready(false);
                        fail_closed("table_lease", error, &status, &leases, &drain, &metrics, &fatal).await;
                        return;
                    }
                }
            }
        }
    }
}

async fn dependency_loop(
    interval_ms: u64,
    delta_root: std::path::PathBuf,
    catalog: Arc<dyn CatalogStore>,
    metastore: Arc<dyn Metastore>,
    status: Arc<CollectorStatus>,
    metrics: CollectorMetrics,
    cancellation: CancellationToken,
) {
    let probe_table = match health_probe_table() {
        Ok(table) => table,
        Err(error) => {
            status.set_fatal("could not construct dependency probe identity");
            error!(%error, "dependency probe identity is invalid");
            return;
        }
    };
    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => {
                let catalog_ready = catalog.incomplete_sagas().await.is_ok();
                let hive_ready = metastore.table(&probe_table).await.is_ok();
                let delta_ready = verify_writable_root(&delta_root).await.is_ok();
                status.set_dependencies_ready(catalog_ready && hive_ready);
                status.set_delta_writable(delta_ready);
                if !catalog_ready || !hive_ready {
                    metrics.control_failure("dependency_probe");
                }
                if !delta_ready {
                    metrics.control_failure("delta_probe");
                }
            }
        }
    }
}

async fn fail_closed(
    subsystem: &'static str,
    error: anyhow::Error,
    status: &CollectorStatus,
    leases: &TableLeaseManager,
    drain: &DrainController,
    metrics: &CollectorMetrics,
    fatal: &mpsc::UnboundedSender<anyhow::Error>,
) {
    metrics.control_failure(subsystem);
    status.set_fatal(format!("{subsystem} control loop failed"));
    drain.begin_drain();
    leases.invalidate_all(&format!("{subsystem} control loop failed")).await;
    error!(%error, subsystem, "collector failed closed");
    let _ = fatal.send(error.context(format!("{subsystem} control loop failed")));
}

fn registration(
    config: &CollectorConfig,
    instance_id: CollectorInstanceId,
    now: UnixMillis,
) -> Result<CollectorRegistration> {
    Ok(CollectorRegistration {
        instance_id,
        instance_key: config.identity.instance_key.clone(),
        implementation_pool: ImplementationPool::Direct,
        flight_endpoint: config.flight.advertised_endpoint,
        catalog_capable: true,
        max_concurrent_streams: PositiveMeasurement::new(config.flight.max_concurrent_streams)?,
        started_at_unix_ms: now,
        heartbeat_at_unix_ms: now,
        lease_expires_at_unix_ms: add_duration(now, Duration::from_millis(config.routing.registration_ttl_ms))?,
    })
}

fn table_pressure_document(
    table: TableIdentity,
    pressure: &arrowcol_buffer::TablePressureSnapshot,
    active_streams: u64,
) -> Result<TablePressure> {
    Ok(TablePressure {
        table,
        active_streams: Measurement::new(active_streams)?,
        buffered_bytes: Measurement::new(pressure.bytes)?,
        buffered_rows: Measurement::new(pressure.rows)?,
        buffered_batches: Measurement::new(pressure.batches)?,
        oldest_buffer_age_ms: Measurement::ZERO,
        pending_commits: Measurement::ZERO,
    })
}

fn gauge(current: u64, limit: u64) -> Result<PressureGauge> {
    let limit = PositiveMeasurement::new(limit)?;
    PressureGauge::new(Measurement::new(current.min(limit.get()))?, limit).map_err(Into::into)
}

fn publication_capacity(config: &CollectorConfig) -> Result<u64> {
    u64::try_from(config.delta.max_pending_files).context("publication queue capacity exceeds protocol range")
}

/// Builds the portable table identity used only for dependency read probes.
///
/// # Errors
///
/// Returns an identity-validation error if a checked-in static component is
/// changed to an invalid value.
pub fn health_probe_table() -> Result<TableIdentity> {
    use arrowcol_protocol::{CatalogName, SchemaName, TableName};
    Ok(TableIdentity::new(
        CatalogName::new("_health")?,
        SchemaName::new("probe")?,
        TableName::new("connectivity")?,
    ))
}

fn read_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    kib.checked_mul(1_024)
}

struct ResourceSample {
    cpu_bps: u16,
    cores: f64,
    rss_bytes: u64,
    memory_bytes: Option<u64>,
    memory_limit: Option<u64>,
}

struct ResourceSampler {
    sampled_at: Instant,
    runtime_ticks: u64,
    ticks_per_second: f64,
    cgroup_cpu_micros: Option<u64>,
}

impl ResourceSampler {
    fn runtime_ticks() -> Result<u64> {
        let stat = std::fs::read_to_string("/proc/self/stat")?;
        let (_, fields) = stat.rsplit_once(')').context("invalid process stat")?;
        let values = fields.split_whitespace().collect::<Vec<_>>();
        let user = values.get(11).context("missing process user time")?.parse::<u64>()?;
        let system = values.get(12).context("missing process system time")?.parse::<u64>()?;
        user.checked_add(system).context("CPU time overflow")
    }

    fn new() -> Result<Self> {
        let output = std::process::Command::new("getconf").arg("CLK_TCK").output()?;
        ensure_resource(output.status.success(), "getconf CLK_TCK failed")?;
        let ticks_per_second = std::str::from_utf8(&output.stdout)?.trim().parse::<f64>()?;
        ensure_resource(
            ticks_per_second.is_finite() && ticks_per_second > 0.0,
            "invalid clock tick frequency",
        )?;
        Ok(Self {
            sampled_at: Instant::now(),
            runtime_ticks: Self::runtime_ticks()?,
            ticks_per_second,
            cgroup_cpu_micros: read_cgroup_cpu_micros()?,
        })
    }

    fn sample(&mut self) -> Result<ResourceSample> {
        // Process utime/stime include exited worker threads, unlike summing live schedstat files.
        let runtime_ticks = Self::runtime_ticks()?;
        let cgroup_cpu_micros = read_cgroup_cpu_micros()?;
        let now = Instant::now();
        let mut cores = std::thread::available_parallelism()?.get() as f64;
        if std::path::Path::new("/sys/fs/cgroup/cpu.max").exists() {
            let quota = std::fs::read_to_string("/sys/fs/cgroup/cpu.max")?;
            let parts = quota.split_whitespace().collect::<Vec<_>>();
            ensure_resource(parts.len() == 2, "invalid cgroup CPU quota")?;
            if parts[0] != "max" {
                let quota = parts[0].parse::<f64>()?;
                let period = parts[1].parse::<f64>()?;
                ensure_resource(quota > 0.0 && period > 0.0, "invalid CPU quota")?;
                cores = cores.min(quota / period);
            }
        }
        let elapsed = now.duration_since(self.sampled_at).as_secs_f64();
        ensure_resource(elapsed > 0.0, "CPU sample interval is empty")?;
        let cpu_ticks = runtime_ticks
            .checked_sub(self.runtime_ticks)
            .context("process CPU counter regressed")?;
        let mut utilization = cpu_ticks as f64 / self.ticks_per_second / elapsed / cores;
        if let (Some(current), Some(previous)) = (cgroup_cpu_micros, self.cgroup_cpu_micros) {
            let used = current.checked_sub(previous).context("cgroup CPU counter regressed")?;
            utilization = utilization.max(used as f64 / 1_000_000.0 / elapsed / cores);
        }
        self.runtime_ticks = runtime_ticks;
        self.cgroup_cpu_micros = cgroup_cpu_micros;
        self.sampled_at = now;
        let memory_bytes = read_cgroup_number("memory.current")?;
        let memory_limit = read_cgroup_number("memory.max")?;
        Ok(ResourceSample {
            cpu_bps: (utilization * 10_000.0).ceil().clamp(0.0, 10_000.0) as u16,
            cores,
            rss_bytes: read_rss_bytes().context("actual RSS measurement unavailable")?,
            memory_bytes,
            memory_limit,
        })
    }
}

fn ensure_resource(valid: bool, message: &str) -> Result<()> {
    if valid { Ok(()) } else { Err(anyhow!("{message}")) }
}

fn read_cgroup_number(name: &str) -> Result<Option<u64>> {
    let path = std::path::Path::new("/sys/fs/cgroup").join(name);
    if !path.exists() {
        return Ok(None);
    }

    let value = std::fs::read_to_string(path)?;
    if value.trim() == "max" {
        return Ok(None);
    }
    Ok(Some(value.trim().parse()?))
}

fn read_cgroup_cpu_micros() -> Result<Option<u64>> {
    let path = std::path::Path::new("/sys/fs/cgroup/cpu.stat");
    if !path.exists() {
        return Ok(None);
    }
    let value = std::fs::read_to_string(path)?;
    let usage = value
        .lines()
        .find_map(|line| line.strip_prefix("usage_usec "))
        .context("cgroup CPU usage is unavailable")?;
    Ok(Some(usage.trim().parse()?))
}

/// Marks the routing registration as draining or terminal.
///
/// # Errors
///
/// Returns routing-store or timestamp failures.
pub async fn set_routing_state(
    routing: &dyn RoutingStore,
    instance_id: CollectorInstanceId,
    state: CollectorState,
) -> Result<()> {
    routing
        .set_collector_state(instance_id, state, now_unix_millis()?)
        .await
        .context("update routing collector state")
}

/// Begins receipt-owner drain.
///
/// # Errors
///
/// Returns receipt-store failures.
pub async fn start_owner_drain(
    receipts: &dyn ReceiptStore,
    instance_id: CollectorInstanceId,
) -> Result<arrowcol_receipts::OwnerDrain> {
    receipts
        .start_owner_drain(instance_id)
        .await
        .context("start receipt owner drain")
}

/// Completes receipt-owner drain.
///
/// # Errors
///
/// Returns receipt-store or timestamp failures.
pub async fn finish_owner_drain(receipts: &dyn ReceiptStore, instance_id: CollectorInstanceId) -> Result<()> {
    receipts
        .finish_owner_drain(instance_id, now_unix_millis()?)
        .await
        .context("finish receipt owner drain")
}

/// Converts a buffer pressure snapshot into a bounded pending count.
#[must_use]
pub fn pending_receipts(pressure: &BufferPressureSnapshot) -> u64 {
    pressure.global.batches
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    #[test]
    fn admission_barrier_drains_existing_streams_and_rejects_new_streams() -> Result<()> {
        let activity = Arc::new(TableActivity::default());
        let table = health_probe_table()?;
        let existing = activity.enter(table.clone())?;
        assert_eq!(activity.block(&table)?, 1);
        assert!(activity.enter(table.clone()).is_err());
        drop(existing);
        assert_eq!(activity.block(&table)?, 0);
        assert!(activity.enter(table.clone()).is_err());
        activity.unblock(&table)?;
        assert!(activity.enter(table).is_ok());
        Ok(())
    }

    #[test]
    fn process_cpu_includes_finished_native_worker_and_counter_failure_is_not_idle() -> Result<()> {
        let mut sampler = ResourceSampler::new()?;
        std::thread::spawn(|| {
            let started = Instant::now();
            let mut value = 1_u64;
            while started.elapsed() < Duration::from_millis(200) {
                value = std::hint::black_box(value.wrapping_mul(17).wrapping_add(3));
            }
            value
        })
        .join()
        .map_err(|_| anyhow!("CPU worker panicked"))?;
        let measured = sampler.sample()?;
        assert!(
            measured.cpu_bps > 0,
            "CPU from exited native threads must not disappear"
        );
        assert!(measured.rss_bytes > 0);
        sampler.runtime_ticks = u64::MAX;
        assert!(sampler.sample().is_err(), "failed measurement must not become zero");
        Ok(())
    }

    /// Scenario: Every bounded file-writing slot is doing useful work.
    /// Guarantees: Normal worker utilization does not become hard publication-queue pressure.
    #[test]
    fn busy_flush_workers_are_not_a_full_publication_queue() {
        let config = CollectorConfig::from_yaml(include_bytes!("../config.yaml")).expect("configuration");
        let capacity = publication_capacity(&config).expect("capacity");
        assert_eq!(capacity, config.delta.max_pending_files as u64);
        let busy = gauge(config.buffer.max_concurrent_flushes as u64, capacity).expect("busy");
        assert!(busy.utilization().get() < 9_500);
        let full = gauge(capacity, capacity).expect("full");
        assert_eq!(full.utilization().get(), BasisPoints::FULL_SCALE);
    }
}
