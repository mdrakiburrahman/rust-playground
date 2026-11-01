use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arrowcol_protocol::{CollectorInstanceId, FencingToken, LeaseId, UnixMillis};
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{
    CollectorRecord, CollectorRegistration, CollectorState, LeaseCas, LeaseClaim, LeaseState, LeaseSuccession,
    LeaseTransfer, RichPressureSnapshot, RoutingError, RoutingErrorKind, RoutingLease, RoutingResult, RoutingStore,
    RoutingView,
};

#[derive(Default)]
struct MemoryState {
    collectors: BTreeMap<CollectorInstanceId, CollectorRecord>,
    leases: BTreeMap<crate::LeaseKey, RoutingLease>,
}

/// Operation counts used to prove request-path cache behavior.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryOperationCounts {
    /// View loads.
    pub load_view: u64,
    /// Lease claims.
    pub claim_lease: u64,
    /// Drain compare-and-set operations.
    pub begin_drain: u64,
    /// Transfer compare-and-set operations.
    pub transfer: u64,
    /// Crash-succession compare-and-set operations.
    pub succession: u64,
}

/// Deterministic in-memory implementation for unit and race tests.
#[derive(Clone)]
pub struct MemoryRoutingStore {
    state: Arc<Mutex<MemoryState>>,
    available: Arc<AtomicBool>,
    load_view: Arc<AtomicU64>,
    claim_lease: Arc<AtomicU64>,
    begin_drain: Arc<AtomicU64>,
    transfer: Arc<AtomicU64>,
    succession: Arc<AtomicU64>,
}

impl Default for MemoryRoutingStore {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryState::default())),
            available: Arc::new(AtomicBool::new(true)),
            load_view: Arc::new(AtomicU64::new(0)),
            claim_lease: Arc::new(AtomicU64::new(0)),
            begin_drain: Arc::new(AtomicU64::new(0)),
            transfer: Arc::new(AtomicU64::new(0)),
            succession: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl MemoryRoutingStore {
    /// Enables or disables the in-memory persistence service.
    ///
    /// This is intended for deterministic SQL-outage tests.
    pub fn set_available(&self, available: bool) {
        self.available.store(available, Ordering::Relaxed);
    }

    /// Returns persistence operation counts.
    #[must_use]
    pub fn operation_counts(&self) -> MemoryOperationCounts {
        MemoryOperationCounts {
            load_view: self.load_view.load(Ordering::Relaxed),
            claim_lease: self.claim_lease.load(Ordering::Relaxed),
            begin_drain: self.begin_drain.load(Ordering::Relaxed),
            transfer: self.transfer.load(Ordering::Relaxed),
            succession: self.succession.load(Ordering::Relaxed),
        }
    }

    /// Inserts a lease fixture after validation.
    ///
    /// # Errors
    ///
    /// Returns corrupt state for an invalid lease fixture.
    pub async fn insert_lease(&self, lease: RoutingLease) -> RoutingResult<()> {
        lease.validate()?;
        self.state.lock().await.leases.insert(lease.key.clone(), lease);
        Ok(())
    }
}

#[async_trait]
impl RoutingStore for MemoryRoutingStore {
    async fn register_collector(&self, registration: &CollectorRegistration) -> RoutingResult<()> {
        self.ensure_available()?;
        registration.validate()?;
        let mut state = self.state.lock().await;
        let mut registration = registration.clone();
        if let Some(existing) = state.collectors.get(&registration.instance_id) {
            if existing.state != CollectorState::Ready {
                return Err(RoutingError::new(
                    RoutingErrorKind::Conflict,
                    "collector registration was fenced: the instance already left the ready state",
                ));
            }
            registration.heartbeat_at_unix_ms = registration
                .heartbeat_at_unix_ms
                .max(existing.registration.heartbeat_at_unix_ms);
            registration.lease_expires_at_unix_ms = registration
                .lease_expires_at_unix_ms
                .max(existing.registration.lease_expires_at_unix_ms);
        }
        state.collectors.insert(
            registration.instance_id,
            CollectorRecord {
                registration,
                state: CollectorState::Ready,
                pressure: None,
            },
        );
        Ok(())
    }

    async fn heartbeat_collector(
        &self,
        instance_id: CollectorInstanceId,
        heartbeat_at: UnixMillis,
        lease_expires_at: UnixMillis,
    ) -> RoutingResult<()> {
        self.ensure_available()?;
        let mut state = self.state.lock().await;
        let collector = state.collectors.get_mut(&instance_id).ok_or_else(|| {
            RoutingError::new(
                RoutingErrorKind::PoolUnavailable,
                "collector registration was not found",
            )
        })?;
        if lease_expires_at <= heartbeat_at {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "collector heartbeat timestamps are invalid",
            ));
        }
        if !matches!(collector.state, CollectorState::Ready | CollectorState::Draining) {
            return Err(RoutingError::new(
                RoutingErrorKind::Conflict,
                "collector heartbeat was fenced: the instance is missing or terminal",
            ));
        }
        collector.registration.heartbeat_at_unix_ms = collector.registration.heartbeat_at_unix_ms.max(heartbeat_at);
        collector.registration.lease_expires_at_unix_ms =
            collector.registration.lease_expires_at_unix_ms.max(lease_expires_at);
        Ok(())
    }

    async fn set_collector_state(
        &self,
        instance_id: CollectorInstanceId,
        new_state: CollectorState,
        changed_at: UnixMillis,
    ) -> RoutingResult<()> {
        self.ensure_available()?;
        let mut state = self.state.lock().await;
        let collector = state.collectors.get_mut(&instance_id).ok_or_else(|| {
            RoutingError::new(
                RoutingErrorKind::PoolUnavailable,
                "collector registration was not found",
            )
        })?;
        if !collector.state.may_transition_to(new_state) {
            return Err(RoutingError::new(
                RoutingErrorKind::Conflict,
                "collector state change was fenced: the instance is missing or the transition would regress",
            ));
        }
        collector.state = new_state;
        collector.registration.heartbeat_at_unix_ms = collector.registration.heartbeat_at_unix_ms.max(changed_at);
        Ok(())
    }

    async fn publish_pressure(&self, snapshot: &RichPressureSnapshot) -> RoutingResult<()> {
        self.ensure_available()?;
        snapshot.validate_at(snapshot.observed_at_unix_ms, &crate::PressurePolicy::default())?;
        let mut state = self.state.lock().await;
        let collector = state.collectors.get_mut(&snapshot.instance_id).ok_or_else(|| {
            RoutingError::new(
                RoutingErrorKind::PoolUnavailable,
                "collector registration was not found",
            )
        })?;
        if collector
            .pressure
            .as_ref()
            .is_some_and(|current| current.observed_at_unix_ms >= snapshot.observed_at_unix_ms)
        {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidSnapshot,
                "collector pressure timestamp did not advance",
            ));
        }
        collector.pressure = Some(snapshot.clone());
        Ok(())
    }

    async fn load_view(&self, loaded_at: UnixMillis) -> RoutingResult<RoutingView> {
        self.load_view.fetch_add(1, Ordering::Relaxed);
        self.ensure_available()?;
        let state = self.state.lock().await;
        Ok(RoutingView {
            loaded_at_unix_ms: loaded_at,
            collectors: state.collectors.clone(),
            leases: state.leases.clone(),
        })
    }

    async fn claim_lease(&self, claim: &LeaseClaim) -> RoutingResult<RoutingLease> {
        self.claim_lease.fetch_add(1, Ordering::Relaxed);
        self.ensure_available()?;
        if claim.expires_at_unix_ms <= claim.now_unix_ms {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "routing lease expiry must be later than issuance",
            ));
        }
        let mut state = self.state.lock().await;
        if let Some(existing) = state.leases.get(&claim.key)
            && existing.is_valid_at(claim.now_unix_ms)
        {
            return Ok(existing.clone());
        }
        validate_candidate(
            &state,
            claim.candidate_instance_id,
            claim.key.implementation_pool,
            claim.minimum_snapshot_at_unix_ms,
        )?;
        let (epoch, version) = state.leases.get(&claim.key).map_or((1, 1), |existing| {
            (
                existing.fencing_epoch.get().saturating_add(1),
                existing.lease_version.saturating_add(1),
            )
        });
        let lease = RoutingLease {
            key: claim.key.clone(),
            lease_id: LeaseId::new(),
            holder_instance_id: claim.candidate_instance_id,
            fencing_epoch: FencingToken::new(epoch).map_err(|cause| {
                RoutingError::new(
                    RoutingErrorKind::CorruptState,
                    format!("fencing epoch is exhausted: {cause}"),
                )
            })?,
            lease_version: version,
            state: LeaseState::Active,
            issued_at_unix_ms: claim.now_unix_ms,
            renewed_at_unix_ms: claim.now_unix_ms,
            expires_at_unix_ms: claim.expires_at_unix_ms,
            drain_started_at_unix_ms: None,
        };
        lease.validate()?;
        state.leases.insert(claim.key.clone(), lease.clone());
        Ok(lease)
    }

    async fn begin_lease_drain(&self, request: &LeaseCas) -> RoutingResult<RoutingLease> {
        self.begin_drain.fetch_add(1, Ordering::Relaxed);
        self.ensure_available()?;
        let mut state = self.state.lock().await;
        let existing = state
            .leases
            .get_mut(&request.key)
            .ok_or_else(|| RoutingError::new(RoutingErrorKind::Conflict, "routing lease was not found"))?;
        if existing.lease_version != request.expected_version || existing.state != LeaseState::Active {
            return Err(RoutingError::new(
                RoutingErrorKind::Conflict,
                "routing lease changed before drain compare-and-set",
            ));
        }
        existing.lease_version = existing
            .lease_version
            .checked_add(1)
            .ok_or_else(|| RoutingError::new(RoutingErrorKind::CorruptState, "routing lease version is exhausted"))?;
        existing.state = LeaseState::Draining;
        existing.renewed_at_unix_ms = request.now_unix_ms;
        existing.drain_started_at_unix_ms = Some(request.now_unix_ms);
        existing.validate()?;
        Ok(existing.clone())
    }

    async fn transfer_drained_lease(&self, request: &LeaseTransfer) -> RoutingResult<RoutingLease> {
        self.transfer.fetch_add(1, Ordering::Relaxed);
        self.ensure_available()?;
        let mut state = self.state.lock().await;
        let existing = state
            .leases
            .get(&request.key)
            .ok_or_else(|| RoutingError::new(RoutingErrorKind::Conflict, "routing lease was not found"))?;
        if existing.lease_version != request.expected_version || existing.state != LeaseState::Draining {
            return Err(RoutingError::new(
                RoutingErrorKind::Conflict,
                "routing lease changed before transfer compare-and-set",
            ));
        }
        validate_candidate(
            &state,
            request.candidate_instance_id,
            request.key.implementation_pool,
            request.minimum_snapshot_at_unix_ms,
        )?;
        let existing = state
            .leases
            .get_mut(&request.key)
            .ok_or_else(|| RoutingError::new(RoutingErrorKind::Conflict, "routing lease was not found"))?;

        existing.lease_id = LeaseId::new();
        existing.holder_instance_id = request.candidate_instance_id;
        existing.fencing_epoch = existing.fencing_epoch.next().map_err(|cause| {
            RoutingError::new(
                RoutingErrorKind::CorruptState,
                format!("fencing epoch is exhausted: {cause}"),
            )
        })?;
        existing.lease_version = existing
            .lease_version
            .checked_add(1)
            .ok_or_else(|| RoutingError::new(RoutingErrorKind::CorruptState, "routing lease version is exhausted"))?;
        existing.state = LeaseState::Active;
        existing.issued_at_unix_ms = request.now_unix_ms;
        existing.renewed_at_unix_ms = request.now_unix_ms;
        existing.expires_at_unix_ms = request.expires_at_unix_ms;
        existing.drain_started_at_unix_ms = None;
        existing.validate()?;
        Ok(existing.clone())
    }

    async fn transfer_to_successor(&self, request: &LeaseSuccession) -> RoutingResult<RoutingLease> {
        self.succession.fetch_add(1, Ordering::Relaxed);
        self.ensure_available()?;
        request.validate()?;
        let mut state = self.state.lock().await;
        let existing = state
            .leases
            .get(&request.key)
            .ok_or_else(|| RoutingError::new(RoutingErrorKind::Conflict, "routing lease was not found"))?;
        if existing.lease_version != request.expected_version
            || existing.holder_instance_id != request.predecessor_instance_id
            || !existing.is_valid_at(request.now_unix_ms)
        {
            return Err(RoutingError::new(
                RoutingErrorKind::Conflict,
                "routing lease changed or expired before succession compare-and-set",
            ));
        }
        validate_successor(&state, request)?;
        let lease = RoutingLease {
            key: request.key.clone(),
            lease_id: LeaseId::new(),
            holder_instance_id: request.successor_instance_id,
            fencing_epoch: existing.fencing_epoch.next().map_err(|cause| {
                RoutingError::new(
                    RoutingErrorKind::CorruptState,
                    format!("fencing epoch is exhausted: {cause}"),
                )
            })?,
            lease_version: existing.lease_version.checked_add(1).ok_or_else(|| {
                RoutingError::new(RoutingErrorKind::CorruptState, "routing lease version is exhausted")
            })?,
            state: LeaseState::Active,
            issued_at_unix_ms: request.now_unix_ms,
            renewed_at_unix_ms: request.now_unix_ms,
            expires_at_unix_ms: request.expires_at_unix_ms,
            drain_started_at_unix_ms: None,
        };
        lease.validate()?;
        state.leases.insert(request.key.clone(), lease.clone());
        Ok(lease)
    }
}

impl MemoryRoutingStore {
    fn ensure_available(&self) -> RoutingResult<()> {
        if self.available.load(Ordering::Relaxed) {
            Ok(())
        } else {
            Err(RoutingError::new(
                RoutingErrorKind::Database,
                "in-memory routing store is simulating an outage",
            ))
        }
    }
}

fn validate_candidate(
    state: &MemoryState,
    candidate: CollectorInstanceId,
    implementation_pool: arrowcol_protocol::ImplementationPool,
    minimum_snapshot_at: UnixMillis,
) -> RoutingResult<()> {
    let collector = state.collectors.get(&candidate).ok_or_else(|| {
        RoutingError::new(
            RoutingErrorKind::PoolUnavailable,
            "selected collector registration was not found",
        )
    })?;
    if !candidate_is_ready(collector, implementation_pool, minimum_snapshot_at) {
        return Err(RoutingError::new(
            RoutingErrorKind::PoolUnavailable,
            "selected collector is no longer ready with fresh accepting pressure",
        ));
    }
    Ok(())
}

fn candidate_is_ready(
    collector: &CollectorRecord,
    implementation_pool: arrowcol_protocol::ImplementationPool,
    minimum_snapshot_at: UnixMillis,
) -> bool {
    collector.state == CollectorState::Ready
        && collector.registration.implementation_pool == implementation_pool
        && collector.pressure.as_ref().is_some_and(|pressure| {
            pressure.observed_at_unix_ms >= minimum_snapshot_at && pressure.accepting_deliveries
        })
}

fn validate_successor(state: &MemoryState, request: &LeaseSuccession) -> RoutingResult<()> {
    let implementation_pool = request.key.implementation_pool;
    let now = request.now_unix_ms;
    let predecessor = state
        .collectors
        .get(&request.predecessor_instance_id)
        .filter(|predecessor| {
            predecessor.registration.implementation_pool == implementation_pool && predecessor.is_terminated_at(now)
        })
        .ok_or_else(|| {
            RoutingError::new(
                RoutingErrorKind::Conflict,
                "routing lease predecessor registration is missing or still live",
            )
        })?;
    let is_same_key_restart = state
        .collectors
        .get(&request.successor_instance_id)
        .is_some_and(|successor| {
            successor.registration.instance_id != predecessor.registration.instance_id
                && successor.registration.instance_key == predecessor.registration.instance_key
                && successor.registration.lease_expires_at_unix_ms > now
                && candidate_is_ready(successor, implementation_pool, request.minimum_snapshot_at_unix_ms)
        });
    if !is_same_key_restart {
        return Err(RoutingError::new(
            RoutingErrorKind::Conflict,
            "routing lease successor is not a live, ready restart with the predecessor's instance key",
        ));
    }
    Ok(())
}
