use std::net::SocketAddr;
use std::sync::Arc;

use arrowcol_protocol::{CollectorInstanceId, ImplementationPool, IngestMetadata, TableIdentity, UnixMillis};
use tokio::sync::RwLock;

use crate::{
    LeaseCas, LeaseClaim, LeaseKey, LeaseState, LeaseSuccession, LeaseTransfer, RoutingError, RoutingErrorKind,
    RoutingLease, RoutingPolicy, RoutingResult, RoutingStore, RoutingView, collector_is_eligible, rank_collectors,
};

/// Flight SQL control operation declared before the request body.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ControlOperation {
    /// Catalog or schema discovery.
    CatalogRead,
    /// Catalog mutation for an existing table.
    CatalogWrite,
    /// Table creation before any data lease exists.
    NewTable,
}

/// Explicit routing metadata for a Flight SQL control operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlRoute {
    /// Requested implementation pool.
    pub implementation_pool: ImplementationPool,
    /// Operation class.
    pub operation: ControlOperation,
    /// Stable bounded rendezvous key.
    pub routing_key: String,
}

/// Whether a request used cache-only routing or acquired a lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteSource {
    /// A valid cached lease required no SQL.
    CachedLease,
    /// SQL compare-and-set created or returned the authoritative lease.
    LeaseClaim,
    /// A catalog-capable backend was selected without a table lease.
    ControlSelection,
}

/// Selected upstream and ownership evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteAssignment {
    /// Collector process lifetime.
    pub instance_id: CollectorInstanceId,
    /// Upstream Flight endpoint.
    pub flight_endpoint: SocketAddr,
    /// Table lease for ingest requests.
    pub lease: Option<RoutingLease>,
    /// Decision source.
    pub source: RouteSource,
}

/// Summary of one background rebalance pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RebalanceReport {
    /// Active leases changed to draining.
    pub drains_started: u64,
    /// Fully drained leases transferred with a greater epoch.
    pub transfers_completed: u64,
    /// Crashed holders' leases handed to their same-instance-key restart with
    /// a greater epoch.
    pub successions_completed: u64,
    /// Compare-and-set races safely lost to another router.
    pub conflicts: u64,
}

/// Cached routing engine shared by Pingora worker threads and control loops.
pub struct RoutingEngine {
    store: Arc<dyn RoutingStore>,
    policy: RoutingPolicy,
    view: RwLock<Option<Arc<RoutingView>>>,
}

impl RoutingEngine {
    /// Creates a routing engine.
    ///
    /// # Errors
    ///
    /// Returns invalid configuration when policy bounds are inconsistent.
    pub fn new(store: Arc<dyn RoutingStore>, policy: RoutingPolicy) -> RoutingResult<Self> {
        policy.validate()?;
        Ok(Self {
            store,
            policy,
            view: RwLock::new(None),
        })
    }

    /// Returns the configured policy.
    #[must_use]
    pub const fn policy(&self) -> &RoutingPolicy {
        &self.policy
    }

    /// Refreshes the immutable cache from SQL.
    ///
    /// A failed refresh leaves the prior cache untouched.
    ///
    /// # Errors
    ///
    /// Returns the underlying store failure.
    pub async fn refresh(&self, now: UnixMillis) -> RoutingResult<Arc<RoutingView>> {
        let view = Arc::new(self.store.load_view(now).await?);
        *self.view.write().await = Some(Arc::clone(&view));
        Ok(view)
    }

    /// Returns a snapshot of the current immutable cache.
    #[must_use]
    pub async fn cached_view(&self) -> Option<Arc<RoutingView>> {
        self.view.read().await.clone()
    }

    /// Returns whether a successful cache load is recent enough for readiness.
    #[must_use]
    pub async fn is_ready_at(&self, now: UnixMillis) -> bool {
        self.view.read().await.as_ref().is_some_and(|view| {
            now.get().saturating_sub(view.loaded_at_unix_ms.get()) <= self.policy.readiness_max_staleness_ms
        })
    }

    /// Routes one ingest stream from validated pre-body protocol metadata.
    ///
    /// Valid active cached leases never execute a SQL operation. Missing or
    /// expired leases execute a single compare-and-set claim.
    ///
    /// # Errors
    ///
    /// Returns a structured failure when the cache is uninitialized, the
    /// authoritative lease is draining/unsafe, no collector is eligible, or a
    /// required lease compare-and-set fails.
    pub async fn route_ingest(&self, metadata: &IngestMetadata, now: UnixMillis) -> RoutingResult<RouteAssignment> {
        let key = LeaseKey::new(metadata.implementation_pool(), metadata.table().clone());
        let view = self.view.read().await.clone().ok_or_else(|| {
            RoutingError::new(
                RoutingErrorKind::PoolUnavailable,
                "routing cache has not completed an initial load",
            )
        })?;
        if now.get().saturating_sub(view.loaded_at_unix_ms.get()) > self.policy.readiness_max_staleness_ms {
            return Err(RoutingError::new(
                RoutingErrorKind::PoolUnavailable,
                "routing cache is stale",
            ));
        }

        if let Some(lease) = view.leases.get(&key) {
            if lease.is_routable_at(now) {
                return assignment_for_lease(&view, lease, now, &self.policy, RouteSource::CachedLease);
            }
            if lease.is_valid_at(now) && lease.state == LeaseState::Draining {
                return Err(RoutingError::new(
                    RoutingErrorKind::LeaseDraining,
                    "table ownership is draining; retry after transfer",
                ));
            }
        }

        let candidate = rank_collectors(&view, &key, now, &self.policy, false)
            .into_iter()
            .next()
            .ok_or_else(|| RoutingError::new(RoutingErrorKind::PoolUnavailable, "no eligible collector"))?;
        let lease = self
            .store
            .claim_lease(&LeaseClaim {
                key: key.clone(),
                candidate_instance_id: candidate,
                now_unix_ms: now,
                expires_at_unix_ms: add_millis(now, self.policy.lease_ttl_ms)?,
                minimum_snapshot_at_unix_ms: subtract_millis(now, self.policy.pressure.max_snapshot_age_ms)?,
            })
            .await?;
        let updated = self.cache_lease(lease.clone()).await?;
        assignment_for_lease(&updated, &lease, now, &self.policy, RouteSource::LeaseClaim)
    }

    /// Selects a healthy catalog-capable backend for explicit Flight SQL control.
    ///
    /// Control routing does not require or invent a table lease.
    ///
    /// # Errors
    ///
    /// Returns invalid configuration for malformed metadata or pool unavailable
    /// when no fresh catalog-capable backend is eligible.
    pub async fn route_control(&self, route: &ControlRoute, now: UnixMillis) -> RoutingResult<RouteAssignment> {
        if route.routing_key.is_empty()
            || route.routing_key.len() > 512
            || route.routing_key.chars().any(char::is_control)
        {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "control routing key must be 1..=512 bytes without controls",
            ));
        }
        let synthetic = synthetic_control_table(route)?;
        let key = LeaseKey::new(route.implementation_pool, synthetic);
        let view = self.view.read().await.clone().ok_or_else(|| {
            RoutingError::new(
                RoutingErrorKind::PoolUnavailable,
                "routing cache has not completed an initial load",
            )
        })?;
        if now.get().saturating_sub(view.loaded_at_unix_ms.get()) > self.policy.readiness_max_staleness_ms {
            return Err(RoutingError::new(
                RoutingErrorKind::PoolUnavailable,
                "routing cache is stale",
            ));
        }
        let instance_id = rank_collectors(&view, &key, now, &self.policy, true)
            .into_iter()
            .next()
            .ok_or_else(|| {
                RoutingError::new(
                    RoutingErrorKind::PoolUnavailable,
                    "no healthy catalog-capable collector is available",
                )
            })?;
        let collector = view.collectors.get(&instance_id).ok_or_else(|| {
            RoutingError::new(
                RoutingErrorKind::CorruptState,
                "selected collector disappeared from the cache",
            )
        })?;
        Ok(RouteAssignment {
            instance_id,
            flight_endpoint: collector.registration.flight_endpoint,
            lease: None,
            source: RouteSource::ControlSelection,
        })
    }

    /// Refreshes SQL state, hands crashed holders' leases to their
    /// same-instance-key restart, and performs fenced two-phase drain
    /// transfers.
    ///
    /// # Errors
    ///
    /// Returns a store, cache, or lease invariant failure.
    pub async fn rebalance(&self, now: UnixMillis) -> RoutingResult<RebalanceReport> {
        let view = self.refresh(now).await?;
        let mut report = RebalanceReport::default();
        for lease in view.leases.values() {
            if !lease.is_valid_at(now) {
                continue;
            }
            // A crashed holder can never publish a drain proof. Its same-key
            // restart owns the durable buffer and replays the backlog instead.
            if let Some(successor) = crash_successor(&view, lease, now, &self.policy) {
                match self
                    .store
                    .transfer_to_successor(&LeaseSuccession {
                        key: lease.key.clone(),
                        expected_version: lease.lease_version,
                        predecessor_instance_id: lease.holder_instance_id,
                        successor_instance_id: successor,
                        now_unix_ms: now,
                        expires_at_unix_ms: add_millis(now, self.policy.lease_ttl_ms)?,
                        minimum_snapshot_at_unix_ms: subtract_millis(now, self.policy.pressure.max_snapshot_age_ms)?,
                    })
                    .await
                {
                    Ok(updated) => {
                        self.cache_lease(updated).await?;
                        report.successions_completed = report.successions_completed.saturating_add(1);
                    }
                    Err(error) if error.kind() == RoutingErrorKind::Conflict => {
                        report.conflicts = report.conflicts.saturating_add(1);
                    }
                    Err(error) => return Err(error),
                }
                continue;
            }
            match lease.state {
                LeaseState::Active => {
                    let holder_is_safe = view
                        .collectors
                        .get(&lease.holder_instance_id)
                        .is_some_and(|collector| collector_is_eligible(collector, now, &self.policy, false));
                    let has_alternative = rank_collectors(&view, &lease.key, now, &self.policy, false)
                        .into_iter()
                        .any(|candidate| candidate != lease.holder_instance_id);
                    // Draining without a transfer target would only strand the table.
                    if !holder_is_safe && has_alternative {
                        match self
                            .store
                            .begin_lease_drain(&LeaseCas {
                                key: lease.key.clone(),
                                expected_version: lease.lease_version,
                                now_unix_ms: now,
                            })
                            .await
                        {
                            Ok(updated) => {
                                self.cache_lease(updated).await?;
                                report.drains_started = report.drains_started.saturating_add(1);
                            }
                            Err(error) if error.kind() == RoutingErrorKind::Conflict => {
                                report.conflicts = report.conflicts.saturating_add(1);
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
                LeaseState::Draining => {
                    if !holder_is_drained(&view, lease, now, &self.policy) {
                        continue;
                    }
                    let candidate = rank_collectors(&view, &lease.key, now, &self.policy, false)
                        .into_iter()
                        .find(|candidate| *candidate != lease.holder_instance_id);
                    let Some(candidate) = candidate else {
                        continue;
                    };
                    match self
                        .store
                        .transfer_drained_lease(&LeaseTransfer {
                            key: lease.key.clone(),
                            expected_version: lease.lease_version,
                            candidate_instance_id: candidate,
                            now_unix_ms: now,
                            expires_at_unix_ms: add_millis(now, self.policy.lease_ttl_ms)?,
                            minimum_snapshot_at_unix_ms: subtract_millis(
                                now,
                                self.policy.pressure.max_snapshot_age_ms,
                            )?,
                        })
                        .await
                    {
                        Ok(updated) => {
                            self.cache_lease(updated).await?;
                            report.transfers_completed = report.transfers_completed.saturating_add(1);
                        }
                        Err(error) if error.kind() == RoutingErrorKind::Conflict => {
                            report.conflicts = report.conflicts.saturating_add(1);
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        Ok(report)
    }

    async fn cache_lease(&self, lease: RoutingLease) -> RoutingResult<Arc<RoutingView>> {
        lease.validate()?;
        let mut guard = self.view.write().await;
        let current = guard.as_ref().ok_or_else(|| {
            RoutingError::new(
                RoutingErrorKind::PoolUnavailable,
                "routing cache has not completed an initial load",
            )
        })?;
        let mut updated = (**current).clone();
        updated.leases.insert(lease.key.clone(), lease);
        let updated = Arc::new(updated);
        *guard = Some(Arc::clone(&updated));
        Ok(updated)
    }
}

fn assignment_for_lease(
    view: &RoutingView,
    lease: &RoutingLease,
    now: UnixMillis,
    policy: &RoutingPolicy,
    source: RouteSource,
) -> RoutingResult<RouteAssignment> {
    if !lease.is_routable_at(now) {
        return Err(RoutingError::new(
            if lease.state == LeaseState::Draining {
                RoutingErrorKind::LeaseDraining
            } else {
                RoutingErrorKind::PoolUnavailable
            },
            "routing lease is not active for a new stream",
        ));
    }
    let collector = view.collectors.get(&lease.holder_instance_id).ok_or_else(|| {
        RoutingError::new(
            RoutingErrorKind::LeaseHolderUnavailable,
            "routing lease holder is absent from the cached view",
        )
    })?;
    if !collector_is_eligible(collector, now, policy, false) {
        return Err(RoutingError::new(
            RoutingErrorKind::LeaseHolderUnavailable,
            "routing lease holder is stale, draining, or under hard pressure",
        ));
    }
    Ok(RouteAssignment {
        instance_id: lease.holder_instance_id,
        flight_endpoint: collector.registration.flight_endpoint,
        lease: Some(lease.clone()),
        source,
    })
}

fn holder_is_drained(view: &RoutingView, lease: &RoutingLease, now: UnixMillis, policy: &RoutingPolicy) -> bool {
    let Some(collector) = view.collectors.get(&lease.holder_instance_id) else {
        return false;
    };
    let Some(pressure) = collector.pressure.as_ref() else {
        return false;
    };
    let Some(drain_started_at) = lease.drain_started_at_unix_ms else {
        return false;
    };
    if pressure.observed_at_unix_ms < drain_started_at || pressure.validate_at(now, &policy.pressure).is_err() {
        return false;
    }
    pressure
        .table_is_drained(&lease.key.table)
        .unwrap_or_else(|| pressure.is_globally_drained())
}

/// Returns the live, eligible restart of a terminated lease holder.
///
/// The restart must share the holder's instance key and the lease's pool. When
/// several restarts qualify, the most recently started one wins.
fn crash_successor(
    view: &RoutingView,
    lease: &RoutingLease,
    now: UnixMillis,
    policy: &RoutingPolicy,
) -> Option<CollectorInstanceId> {
    let holder = view.collectors.get(&lease.holder_instance_id)?;
    if holder.registration.implementation_pool != lease.key.implementation_pool || !holder.is_terminated_at(now) {
        return None;
    }
    view.collectors
        .values()
        .filter(|collector| {
            collector.registration.instance_id != holder.registration.instance_id
                && collector.registration.instance_key == holder.registration.instance_key
                && collector.registration.implementation_pool == lease.key.implementation_pool
                && collector_is_eligible(collector, now, policy, false)
        })
        .max_by_key(|collector| {
            (
                collector.registration.started_at_unix_ms,
                collector.registration.instance_id,
            )
        })
        .map(|collector| collector.registration.instance_id)
}

fn synthetic_control_table(route: &ControlRoute) -> RoutingResult<TableIdentity> {
    use arrowcol_protocol::{CatalogName, SchemaName, TableName};

    let digest = sha256_hex(route.routing_key.as_bytes());
    let operation = match route.operation {
        ControlOperation::CatalogRead => "read",
        ControlOperation::CatalogWrite => "write",
        ControlOperation::NewTable => "new",
    };
    Ok(TableIdentity::new(
        CatalogName::new("_control").map_err(invalid_control_identity)?,
        SchemaName::new(operation).map_err(invalid_control_identity)?,
        TableName::new(format!("k_{}", &digest[..24])).map_err(invalid_control_identity)?,
    ))
}

fn invalid_control_identity(cause: impl std::fmt::Display) -> RoutingError {
    RoutingError::new(
        RoutingErrorKind::InvalidConfiguration,
        format!("could not construct control routing identity: {cause}"),
    )
}

fn sha256_hex(value: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(value);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn add_millis(value: UnixMillis, delta: u64) -> RoutingResult<UnixMillis> {
    UnixMillis::new(
        value
            .get()
            .checked_add(delta)
            .ok_or_else(|| RoutingError::new(RoutingErrorKind::InvalidConfiguration, "routing timestamp overflow"))?,
    )
    .map_err(|cause| RoutingError::new(RoutingErrorKind::InvalidConfiguration, cause.to_string()))
}

fn subtract_millis(value: UnixMillis, delta: u64) -> RoutingResult<UnixMillis> {
    UnixMillis::new(value.get().saturating_sub(delta))
        .map_err(|cause| RoutingError::new(RoutingErrorKind::InvalidConfiguration, cause.to_string()))
}
