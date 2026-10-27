use std::collections::HashMap;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_deltalake::delta::{PublicationFence, PublicationFenceContext, PublicationFenceError};
use arrowcol_flight::{RuntimeError, RuntimeResult};
use arrowcol_protocol::{
    CollectorInstanceId, ErrorCode, FencingToken, ImplementationPool, LeaseId, TableIdentity, UnixMillis,
};
use arrowcol_routing::{LeaseKey, LeaseState, RoutingLease, RoutingStore};
use async_trait::async_trait;
use tonic::Code;

use crate::config::now_unix_millis;
use crate::envelope::ArrowcolBatchEnvelope;

/// How long an authoritative lease observed before staging is reused.
pub(crate) const LEASE_VIEW_TTL: Duration = Duration::from_secs(1);
const MAX_CACHED_LEASES: usize = 1_024;

/// Resolves authoritative table ownership before a Flight stream is admitted.
#[async_trait]
pub trait LeaseResolver: Send + Sync {
    /// Returns the active Dataflow lease held by this collector.
    async fn resolve(&self, table: &TableIdentity) -> RuntimeResult<RoutingLease>;
}

/// Routing-store-backed lease resolver.
pub struct StoreLeaseResolver {
    store: Arc<dyn RoutingStore>,
    instance_id: CollectorInstanceId,
}

impl StoreLeaseResolver {
    /// Creates a resolver for one collector process lifetime.
    #[must_use]
    pub fn new(store: Arc<dyn RoutingStore>, instance_id: CollectorInstanceId) -> Self {
        Self { store, instance_id }
    }
}

#[async_trait]
impl LeaseResolver for StoreLeaseResolver {
    async fn resolve(&self, table: &TableIdentity) -> RuntimeResult<RoutingLease> {
        let now = now_unix_millis().map_err(|_| RuntimeError::internal())?;
        let view = self.store.load_view(now).await.map_err(|_| {
            RuntimeError::new(
                ErrorCode::PoolUnavailable,
                Code::Unavailable,
                "routing lease could not be verified",
            )
        })?;
        let key = LeaseKey::new(ImplementationPool::Dataflow, table.clone());
        let lease = view.leases.get(&key).cloned().ok_or_else(|| {
            RuntimeError::new(
                ErrorCode::LeaseFenced,
                Code::FailedPrecondition,
                "collector does not own an active table lease",
            )
        })?;
        if lease.holder_instance_id != self.instance_id
            || lease.state != LeaseState::Active
            || !lease.is_routable_at(now)
        {
            return Err(RuntimeError::new(
                ErrorCode::LeaseFenced,
                Code::FailedPrecondition,
                "collector does not own an active table lease",
            ));
        }
        Ok(lease)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LeaseEvidence {
    table: TableIdentity,
    lease_id: LeaseId,
    holder_instance_id: CollectorInstanceId,
    fencing_token: FencingToken,
}

impl LeaseEvidence {
    pub(crate) fn from_envelope(envelope: &ArrowcolBatchEnvelope) -> Self {
        Self {
            table: envelope.table.clone(),
            lease_id: envelope.lease_id,
            holder_instance_id: envelope.holder_instance_id,
            fencing_token: envelope.fencing_token,
        }
    }
}

/// Identity of the collector process lifetime that is exporting a batch.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExportingProcess<'a> {
    /// Process-lifetime identifier ([`crate::process_instance_id`]).
    pub(crate) instance_id: CollectorInstanceId,
    /// Stable deployment key recorded by this process's receiver, when known.
    pub(crate) instance_key: Option<&'a str>,
}

/// Decision binding one decoded batch to the authoritative lease before staging.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LeaseBinding {
    /// The envelope already carries the current lease, held by this process.
    Current,
    /// Overwrite the envelope's lease evidence with the current lease held by this process.
    Rebind {
        /// Current lease lineage.
        lease_id: LeaseId,
        /// This process.
        holder_instance_id: CollectorInstanceId,
        /// Current fencing epoch.
        fencing_token: FencingToken,
    },
    /// This process cannot publish the batch yet; the durable buffer must retry it.
    Defer(&'static str),
}

impl LeaseBinding {
    pub(crate) fn apply(self, envelope: &mut ArrowcolBatchEnvelope) {
        if let Self::Rebind {
            lease_id,
            holder_instance_id,
            fencing_token,
        } = self
        {
            envelope.lease_id = lease_id;
            envelope.holder_instance_id = holder_instance_id;
            envelope.fencing_token = fencing_token;
        }
    }
}

/// Decides whether this process may publish a batch under the current lease.
///
/// Only the current holder publishes. A batch whose envelope already matches
/// the current lease keeps it, including while that lease drains. Otherwise
/// the batch is rebound to the current lease when that lease is active and
/// valid, held by this process, and the batch was admitted either by this
/// process (an older lease) or by a predecessor with this process's instance
/// key (a crash replay). Everything else is deferred, never refused
/// permanently, so the durable buffer keeps the batch and retries it.
pub(crate) fn lease_binding(
    envelope: &ArrowcolBatchEnvelope,
    current: Option<&RoutingLease>,
    process: ExportingProcess<'_>,
    now: UnixMillis,
) -> LeaseBinding {
    let Some(lease) = current else {
        return LeaseBinding::Defer("authoritative routing lease is absent; retrying until this collector holds it");
    };
    if lease.holder_instance_id != process.instance_id {
        return LeaseBinding::Defer(
            "authoritative routing lease is held by another collector; retrying until this collector holds it",
        );
    }
    if envelope.lease_id == lease.lease_id
        && envelope.holder_instance_id == lease.holder_instance_id
        && envelope.fencing_token == lease.fencing_epoch
    {
        return LeaseBinding::Current;
    }
    if !lease.is_routable_at(now) {
        return LeaseBinding::Defer(
            "authoritative routing lease held by this collector is not active; retrying before rebinding",
        );
    }
    let admitted_here = envelope.holder_instance_id == process.instance_id;
    let same_deployment = process.instance_key.is_some_and(|key| key == envelope.instance_key);
    if !admitted_here && !same_deployment {
        return LeaseBinding::Defer(
            "batch was admitted by a collector with a different or unknown instance key; its routing lease cannot be rebound",
        );
    }
    LeaseBinding::Rebind {
        lease_id: lease.lease_id,
        holder_instance_id: lease.holder_instance_id,
        fencing_token: lease.fencing_epoch,
    }
}

struct CachedLease {
    lease: Option<RoutingLease>,
    loaded_at: Instant,
}

/// Binds batches to authoritative leases with a brief per-table view cache.
///
/// Reusing a view for up to [`LEASE_VIEW_TTL`] keeps routing reads off the
/// per-batch path. A stale decision is safe: the commit-boundary
/// [`RoutingPublicationFence`] re-reads the authoritative lease before any
/// Delta version becomes visible, so a stale binding fails transiently and
/// the durable buffer retries with a refreshed view.
pub(crate) struct LeaseBinder {
    store: Arc<dyn RoutingStore>,
    ttl: Duration,
    cache: Mutex<HashMap<TableIdentity, CachedLease>>,
}

impl LeaseBinder {
    pub(crate) fn new(store: Arc<dyn RoutingStore>, ttl: Duration) -> Self {
        Self {
            store,
            ttl,
            cache: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_ttl(&mut self, ttl: Duration) {
        self.ttl = ttl;
    }

    pub(crate) async fn bind(
        &self,
        envelope: &ArrowcolBatchEnvelope,
        process: ExportingProcess<'_>,
    ) -> Result<LeaseBinding, String> {
        let now = now_unix_millis()?;
        if let Some(binding) = self.cached_binding(envelope, process, now) {
            return Ok(binding);
        }
        let view = self
            .store
            .load_view(now)
            .await
            .map_err(|error| format!("authoritative routing lease could not be loaded: {error}"))?;
        let lease = view
            .leases
            .get(&LeaseKey::new(ImplementationPool::Dataflow, envelope.table.clone()))
            .cloned();
        let binding = lease_binding(envelope, lease.as_ref(), process, now_unix_millis()?);
        self.remember(envelope.table.clone(), lease);
        Ok(binding)
    }

    /// Decides from a fresh cached lease unless the envelope proves the entry outdated.
    fn cached_binding(
        &self,
        envelope: &ArrowcolBatchEnvelope,
        process: ExportingProcess<'_>,
        now: UnixMillis,
    ) -> Option<LeaseBinding> {
        let cache = self.cache.lock().ok()?;
        let cached = cache.get(&envelope.table)?;
        // Absent or expired leases, and envelopes newer than the cached epoch,
        // always trigger a reload instead of a decision from the cache.
        let lease = cached.lease.as_ref()?;
        if cached.loaded_at.elapsed() >= self.ttl
            || envelope.fencing_token > lease.fencing_epoch
            || !lease.is_valid_at(now)
        {
            return None;
        }
        Some(lease_binding(envelope, Some(lease), process, now))
    }

    fn remember(&self, table: TableIdentity, lease: Option<RoutingLease>) {
        let Ok(mut cache) = self.cache.lock() else {
            return;
        };
        if cache.len() >= MAX_CACHED_LEASES {
            let ttl = self.ttl;
            cache.retain(|_, entry| entry.loaded_at.elapsed() < ttl);
            if cache.len() >= MAX_CACHED_LEASES {
                cache.clear();
            }
        }
        cache.insert(
            table,
            CachedLease {
                lease,
                loaded_at: Instant::now(),
            },
        );
    }
}

struct AuthorityRequest {
    evidence: LeaseEvidence,
    reply: std::sync::mpsc::Sender<Result<(), PublicationFenceError>>,
}

/// Synchronous commit-boundary facade backed by a dedicated async routing worker.
#[derive(Clone)]
pub(crate) struct AuthorityClient {
    requests: SyncSender<AuthorityRequest>,
    timeout: Duration,
}

impl AuthorityClient {
    pub(crate) fn start(
        store: Arc<dyn RoutingStore>,
        timeout: Duration,
        queue_capacity: usize,
    ) -> Result<Self, String> {
        let (requests, receiver) = std::sync::mpsc::sync_channel(queue_capacity.max(1));
        std::thread::Builder::new()
            .name("arrowcol-lease-authority".to_owned())
            .spawn(move || authority_worker(store, receiver))
            .map_err(|error| format!("start routing authority worker: {error}"))?;
        Ok(Self { requests, timeout })
    }

    fn check(&self, evidence: LeaseEvidence) -> Result<(), PublicationFenceError> {
        let (reply, response) = std::sync::mpsc::channel();
        self.requests
            .try_send(AuthorityRequest { evidence, reply })
            .map_err(|error| {
                PublicationFenceError::check_failed(format!("routing authority queue unavailable: {error}"))
            })?;
        response.recv_timeout(self.timeout).map_err(|error| {
            PublicationFenceError::check_failed(format!("routing authority check timed out: {error}"))
        })?
    }
}

fn authority_worker(store: Arc<dyn RoutingStore>, receiver: Receiver<AuthorityRequest>) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build();
    let Ok(runtime) = runtime else {
        while let Ok(request) = receiver.recv() {
            let _ = request.reply.send(Err(PublicationFenceError::check_failed(
                "routing authority runtime could not start",
            )));
        }
        return;
    };
    while let Ok(request) = receiver.recv() {
        let result = runtime.block_on(check_authority(&store, &request.evidence));
        let _ = request.reply.send(result);
    }
}

async fn check_authority(store: &Arc<dyn RoutingStore>, expected: &LeaseEvidence) -> Result<(), PublicationFenceError> {
    let now = now_unix_millis().map_err(PublicationFenceError::check_failed)?;
    let view = store
        .load_view(now)
        .await
        .map_err(|error| PublicationFenceError::check_failed(error.to_string()))?;
    let key = LeaseKey::new(ImplementationPool::Dataflow, expected.table.clone());
    let lease = view
        .leases
        .get(&key)
        .ok_or_else(|| PublicationFenceError::stale("authoritative routing lease is absent"))?;
    let exact = lease.lease_id == expected.lease_id
        && lease.holder_instance_id == expected.holder_instance_id
        && lease.fencing_epoch == expected.fencing_token
        && lease.is_valid_at(now);
    if !exact {
        return Err(PublicationFenceError::stale(
            "authoritative routing lease no longer matches the admitted fence",
        ));
    }
    Ok(())
}

pub(crate) struct RoutingPublicationFence {
    expected: Mutex<LeaseEvidence>,
    authority: AuthorityClient,
}

impl RoutingPublicationFence {
    pub(crate) fn new(evidence: LeaseEvidence, authority: AuthorityClient) -> Self {
        Self {
            expected: Mutex::new(evidence),
            authority,
        }
    }

    pub(crate) fn replace_expected(&self, evidence: LeaseEvidence) -> Result<(), String> {
        *self
            .expected
            .lock()
            .map_err(|_| "Delta publication fence lock poisoned".to_owned())? = evidence;
        Ok(())
    }
}

impl PublicationFence for RoutingPublicationFence {
    fn check_before_commit(&self, _context: &PublicationFenceContext<'_>) -> Result<(), PublicationFenceError> {
        let expected = self
            .expected
            .lock()
            .map(|evidence| evidence.clone())
            .map_err(|_| PublicationFenceError::check_failed("Delta publication fence lock poisoned"))?;
        self.authority.check(expected)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use arrowcol_protocol::{
        BasisPoints, CatalogName, Measurement, PositiveMeasurement, PressureGauge, SchemaName, TableName, UnixMillis,
    };
    use arrowcol_routing::{
        CollectorRegistration, MemoryRoutingStore, RichPressureSnapshot, RoutingStore, SubsystemState,
    };

    use super::*;

    fn time(value: u64) -> UnixMillis {
        UnixMillis::new(value).expect("time")
    }

    fn table() -> TableIdentity {
        TableIdentity::new(
            CatalogName::new("demo").expect("catalog"),
            SchemaName::new("generated").expect("schema"),
            TableName::new("events").expect("table"),
        )
    }

    async fn leased_store(holder: CollectorInstanceId) -> (Arc<MemoryRoutingStore>, RoutingLease) {
        let store = Arc::new(MemoryRoutingStore::default());
        store
            .register_collector(&CollectorRegistration {
                instance_id: holder,
                instance_key: "collector".to_owned(),
                implementation_pool: ImplementationPool::Dataflow,
                flight_endpoint: "127.0.0.1:32010".parse().expect("address"),
                catalog_capable: true,
                max_concurrent_streams: PositiveMeasurement::new(4).expect("positive"),
                started_at_unix_ms: time(1),
                heartbeat_at_unix_ms: time(1),
                lease_expires_at_unix_ms: time(u64::MAX / 4),
            })
            .await
            .expect("register");
        let zero =
            PressureGauge::new(Measurement::ZERO, PositiveMeasurement::new(4).expect("positive")).expect("gauge");
        store
            .publish_pressure(&RichPressureSnapshot {
                instance_id: holder,
                observed_at_unix_ms: time(1),
                cpu_utilization_bps: BasisPoints::new(0).expect("bps"),
                rss_bytes: Measurement::ZERO,
                rss_limit_bytes: PositiveMeasurement::new(1).expect("positive"),
                active_streams: zero,
                received_bytes: Measurement::ZERO,
                received_rows: Measurement::ZERO,
                received_batches: Measurement::ZERO,
                buffered_bytes: zero,
                buffered_rows: zero,
                buffered_batches: zero,
                available_permits: PressureGauge::new(
                    Measurement::new(4).expect("measurement"),
                    PositiveMeasurement::new(4).expect("positive"),
                )
                .expect("gauge"),
                pending_commits: zero,
                oldest_buffer_age_ms: Measurement::ZERO,
                delta_pressure_bps: BasisPoints::new(0).expect("bps"),
                quiver_pressure_bps: BasisPoints::new(0).expect("bps"),
                dfe_pressure_bps: BasisPoints::new(0).expect("bps"),
                delta_state: SubsystemState::Ready,
                quiver_state: SubsystemState::Disabled,
                dfe_state: SubsystemState::Ready,
                accepting_deliveries: true,
                tables: Vec::new(),
            })
            .await
            .expect("pressure");
        let now = now_unix_millis().expect("now");
        let lease = store
            .claim_lease(&arrowcol_routing::LeaseClaim {
                key: LeaseKey::new(ImplementationPool::Dataflow, table()),
                candidate_instance_id: holder,
                now_unix_ms: now,
                expires_at_unix_ms: UnixMillis::new(now.get() + 60_000).expect("expiry"),
                minimum_snapshot_at_unix_ms: time(0),
            })
            .await
            .expect("claim");
        (store, lease)
    }

    /// Scenario: A Flight stream reaches the wrong Dataflow collector.
    /// Guarantees: Admission rejects it before pdata enters a DFE channel.
    #[tokio::test]
    async fn resolver_rejects_non_owner() {
        let owner = CollectorInstanceId::new();
        let (store, _) = leased_store(owner).await;
        let resolver = StoreLeaseResolver::new(store as Arc<dyn RoutingStore>, CollectorInstanceId::new());
        let error = resolver.resolve(&table()).await.expect_err("fenced");
        assert_eq!(error.code(), ErrorCode::LeaseFenced);
    }

    /// Scenario: Routing ownership changes before a Delta publication boundary.
    /// Guarantees: The dedicated authority worker returns a typed stale-ownership fence.
    #[tokio::test]
    async fn authority_rejects_stale_evidence() {
        let owner = CollectorInstanceId::new();
        let (store, lease) = leased_store(owner).await;
        let authority =
            AuthorityClient::start(store as Arc<dyn RoutingStore>, Duration::from_secs(1), 1).expect("authority");
        let mut evidence = LeaseEvidence {
            table: table(),
            lease_id: lease.lease_id,
            holder_instance_id: owner,
            fencing_token: lease.fencing_epoch,
        };
        authority.check(evidence.clone()).expect("current lease");
        evidence.fencing_token = evidence.fencing_token.next().expect("next");
        let error = authority.check(evidence).expect_err("stale");
        assert_eq!(
            error.kind(),
            arrow_deltalake::delta::PublicationFenceErrorKind::StaleOwnership
        );
    }

    /// Scenario: The lease held by this process is draining, expired, or absent when batches are bound.
    /// Guarantees: Batches already carrying a draining lease keep publishing so the drain can finish,
    /// no batch is rebound onto a draining or expired lease, and an absent lease defers the batch.
    #[test]
    fn binding_respects_lease_state() {
        let process = CollectorInstanceId::new();
        let identity = ExportingProcess {
            instance_id: process,
            instance_key: Some("collector"),
        };
        let draining = RoutingLease {
            key: LeaseKey::new(ImplementationPool::Dataflow, table()),
            lease_id: LeaseId::new(),
            holder_instance_id: process,
            fencing_epoch: FencingToken::new(3).expect("fence"),
            lease_version: 3,
            state: LeaseState::Draining,
            issued_at_unix_ms: time(1_000),
            renewed_at_unix_ms: time(5_000),
            expires_at_unix_ms: time(20_000),
            drain_started_at_unix_ms: Some(time(5_000)),
        };
        let active = RoutingLease {
            state: LeaseState::Active,
            drain_started_at_unix_ms: None,
            ..draining.clone()
        };
        let current = ArrowcolBatchEnvelope {
            publication_id: Some(arrowcol_protocol::ReceiptId::new()),
            ingested_at_ms: Some(time(1_000)),
            table: table(),
            source_schema_version: arrowcol_catalog::SchemaVersion::FIRST,
            target_schema_version: arrowcol_catalog::SchemaVersion::FIRST,
            lease_id: draining.lease_id,
            holder_instance_id: process,
            fencing_token: draining.fencing_epoch,
            instance_key: "collector".to_owned(),
        };
        let older = ArrowcolBatchEnvelope {
            lease_id: LeaseId::new(),
            fencing_token: FencingToken::new(2).expect("fence"),
            ..current.clone()
        };

        assert_eq!(
            lease_binding(&current, Some(&draining), identity, time(10_000)),
            LeaseBinding::Current
        );
        assert!(matches!(
            lease_binding(&older, Some(&draining), identity, time(10_000)),
            LeaseBinding::Defer(_)
        ));
        let rebind = lease_binding(&older, Some(&active), identity, time(10_000));
        assert_eq!(
            rebind,
            LeaseBinding::Rebind {
                lease_id: active.lease_id,
                holder_instance_id: process,
                fencing_token: active.fencing_epoch,
            }
        );
        let mut rebound = older.clone();
        rebind.apply(&mut rebound);
        assert_eq!(
            LeaseEvidence::from_envelope(&rebound),
            LeaseEvidence::from_envelope(&current)
        );
        assert!(matches!(
            lease_binding(&older, Some(&active), identity, time(20_000)),
            LeaseBinding::Defer(_)
        ));
        assert!(matches!(
            lease_binding(&current, None, identity, time(10_000)),
            LeaseBinding::Defer(_)
        ));
    }
}
