use std::net::SocketAddr;
use std::sync::Arc;

use arrowcol_protocol::{
    CatalogName, CollectorInstanceId, ImplementationPool, IngestMetadata, SchemaName, TableIdentity, TableName,
    UnixMillis,
};
use tokio::sync::RwLock;

use crate::{
    LeaseCas, LeaseClaim, LeaseKey, LeaseState, LeaseSuccession, LeaseTransfer, RoutingError, RoutingErrorKind,
    RoutingLease, RoutingPolicy, RoutingResult, RoutingStore, RoutingView, RunTablePin, collector_is_eligible,
    rank_collectors,
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
        let mut view = self.view.read().await.clone().ok_or_else(|| {
            RoutingError::new(
                RoutingErrorKind::PoolUnavailable,
                "routing cache has not completed an initial load",
            )
        })?;

        if let Some(lease) = view.leases.get(&key) {
            if lease.is_routable_at(now) {
                if self.policy.distinct_table_owners
                    && has_other_active_table_lease(&view, &key, lease.holder_instance_id, now)
                {
                    return Err(RoutingError::new(
                        RoutingErrorKind::PoolUnavailable,
                        "active table lease conflicts with distinct table-owner policy",
                    ));
                }
                return assignment_for_lease(&view, lease, now, &self.policy, RouteSource::CachedLease);
            }
            if lease.is_valid_at(now) && lease.state == LeaseState::Draining {
                return Err(RoutingError::new(
                    RoutingErrorKind::LeaseDraining,
                    "table ownership is draining; retry after transfer",
                ));
            }
        }

        for _ in 0..view.collectors.len().max(1) {
            let candidate = rank_collectors(&view, &key, now, &self.policy, false)
                .into_iter()
                .find(|candidate| {
                    !self.policy.distinct_table_owners || !has_other_active_table_lease(&view, &key, *candidate, now)
                })
                .ok_or_else(|| {
                    RoutingError::new(
                        RoutingErrorKind::PoolUnavailable,
                        "no eligible collector is available for a distinct table lease",
                    )
                })?;
            let result = self
                .store
                .claim_lease(&LeaseClaim {
                    key: key.clone(),
                    candidate_instance_id: candidate,
                    now_unix_ms: now,
                    expires_at_unix_ms: add_millis(now, self.policy.lease_ttl_ms)?,
                    minimum_snapshot_at_unix_ms: subtract_millis(now, self.policy.pressure.max_snapshot_age_ms)?,
                    distinct_table_owners: self.policy.distinct_table_owners,
                })
                .await;
            match result {
                Ok(lease) => {
                    let updated = self.cache_lease(lease.clone()).await?;
                    return assignment_for_lease(&updated, &lease, now, &self.policy, RouteSource::LeaseClaim);
                }
                Err(error) if self.policy.distinct_table_owners && error.kind() == RoutingErrorKind::Conflict => {
                    view = self.refresh(now).await?;
                    if let Some(lease) = view.leases.get(&key).filter(|lease| lease.is_routable_at(now)) {
                        return assignment_for_lease(&view, lease, now, &self.policy, RouteSource::CachedLease);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(RoutingError::new(
            RoutingErrorKind::PoolUnavailable,
            "no eligible collector is available for a distinct table lease",
        ))
    }

    /// Routes an OAT request through the durable immutable run/table binding.
    ///
    /// The caller must pass the run expiry from trusted run configuration,
    /// not a value supplied by the request. Existing pins are checked against
    /// the authoritative store on every request; a missing, expired, changed,
    /// or unavailable pin never falls back to ordinary collector selection.
    pub async fn route_ingest_for_run(
        &self,
        metadata: &IngestMetadata,
        run_id: &str,
        run_expires_at: UnixMillis,
        now: UnixMillis,
    ) -> RoutingResult<RouteAssignment> {
        if self.policy.oat_run_id.as_deref() != Some(run_id) {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "OAT run ID is not authorized by this router configuration",
            ));
        }
        validate_oat_run_id(run_id, run_expires_at, now)?;
        let metadata = canonical_oat_metadata(metadata)?;
        let key = LeaseKey::new(metadata.implementation_pool(), metadata.table().clone());
        let pin = self.store.get_run_table_pin(run_id, &key, now).await?.ok_or_else(|| {
            RoutingError::new(
                RoutingErrorKind::Conflict,
                "immutable OAT run-table pin is missing; refusing to initialize on an ingest request",
            )
        })?;
        self.assignment_for_run_pin(&pin, now).await
    }

    /// Initializes or resumes the trusted, complete run/table pin set.
    ///
    /// This operation is reserved for the bounded migrator-authenticated setup
    /// process. Request routing must use [`Self::route_ingest_for_run`] and
    /// never invoke this initializer.
    ///
    /// # Errors
    ///
    /// Returns an error if the run/set is invalid, SQL authority is unavailable,
    /// any expected table lacks an eligible collector, or an existing pin or
    /// lease no longer matches its immutable binding.
    pub async fn initialize_run_table_pins(
        &self,
        run_id: &str,
        expected: &[LeaseKey],
        expires_at: UnixMillis,
        now: UnixMillis,
    ) -> RoutingResult<Vec<RunTablePin>> {
        let started_at = std::time::Instant::now();
        self.initialize_run_table_pins_with_clock(run_id, expected, expires_at, now, move || {
            let elapsed_ms = u64::try_from(started_at.elapsed().as_millis()).map_err(|_| {
                RoutingError::new(
                    RoutingErrorKind::InvalidConfiguration,
                    "OAT setup elapsed time overflow",
                )
            })?;
            add_millis(now, elapsed_ms)
        })
        .await
    }

    /// Initializes a complete pin set using a caller-supplied monotonic setup clock.
    ///
    /// The initial timestamp establishes the lower bound; every subsequent
    /// authority operation samples `clock` and rejects regression or expiry.
    /// The expiry remains immutable for the entire setup attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the run/set is invalid, the clock regresses, setup
    /// reaches the fixed expiry, SQL authority is unavailable, any expected
    /// table lacks an eligible collector, or an existing binding changed.
    pub async fn initialize_run_table_pins_with_clock<F>(
        &self,
        run_id: &str,
        expected: &[LeaseKey],
        expires_at: UnixMillis,
        initial_now: UnixMillis,
        clock: F,
    ) -> RoutingResult<Vec<RunTablePin>>
    where
        F: Fn() -> RoutingResult<UnixMillis> + Send + Sync,
    {
        if self.policy.oat_run_id.as_deref() != Some(run_id) {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "OAT run ID is not authorized by this router configuration",
            ));
        }
        validate_oat_run_id(run_id, expires_at, initial_now)?;
        if expected.is_empty()
            || expected.len() > 32
            || expected.windows(2).any(|pair| pair[0] >= pair[1])
            || expected
                .iter()
                .any(|key| key.implementation_pool != ImplementationPool::Dataflow)
        {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "OAT table set must contain 1..=32 strictly sorted, unique dataflow identities",
            ));
        }
        let mut last_now = initial_now;
        let now = setup_now(&clock, &mut last_now, expires_at)?;
        self.store
            .initialize_run_table_set(run_id, expected, expires_at, now)
            .await?;
        let refresh_now = setup_now(&clock, &mut last_now, expires_at)?;
        let mut view = self.refresh(refresh_now).await?;
        let mut pins = Vec::with_capacity(expected.len());
        for key in expected {
            let now = setup_now(&clock, &mut last_now, expires_at)?;
            if let Some(pin) = self.store.get_run_table_pin(run_id, key, now).await? {
                if pin.expires_at_unix_ms != expires_at {
                    return Err(RoutingError::new(
                        RoutingErrorKind::Conflict,
                        "existing OAT run-table pin has a different immutable expiry",
                    ));
                }
                let assignment_now = setup_now(&clock, &mut last_now, expires_at)?;
                self.assignment_for_run_pin(&pin, assignment_now).await?;
                pins.push(pin);
                continue;
            }

            let mut bound_pin = None;
            for _ in 0..view.collectors.len().max(1) {
                let now = setup_now(&clock, &mut last_now, expires_at)?;
                view = self.refresh(now).await?;
                let candidate = rank_collectors(&view, key, now, &self.policy, false)
                    .into_iter()
                    .find(|candidate| {
                        !self.policy.distinct_table_owners || !has_other_active_table_lease(&view, key, *candidate, now)
                    })
                    .ok_or_else(|| {
                        RoutingError::new(
                            RoutingErrorKind::PoolUnavailable,
                            "no eligible collector is available to initialize the OAT table pin",
                        )
                    })?;
                let claim_now = setup_now(&clock, &mut last_now, expires_at)?;
                let claim = self
                    .store
                    .claim_lease(&LeaseClaim {
                        key: key.clone(),
                        candidate_instance_id: candidate,
                        now_unix_ms: claim_now,
                        expires_at_unix_ms: add_millis(claim_now, self.policy.lease_ttl_ms)?,
                        minimum_snapshot_at_unix_ms: subtract_millis(
                            claim_now,
                            self.policy.pressure.max_snapshot_age_ms,
                        )?,
                        distinct_table_owners: self.policy.distinct_table_owners,
                    })
                    .await;
                let lease = match claim {
                    Ok(lease) => lease,
                    Err(error) if error.kind() == RoutingErrorKind::Conflict => {
                        let refresh_now = setup_now(&clock, &mut last_now, expires_at)?;
                        self.refresh(refresh_now).await?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if lease.key != *key || !lease.is_routable_at(claim_now) {
                    return Err(RoutingError::new(
                        RoutingErrorKind::Conflict,
                        "SQL lease claim did not return a live lease for the expected OAT table",
                    ));
                }
                let pin = RunTablePin {
                    run_id: run_id.to_owned(),
                    key: key.clone(),
                    holder_instance_id: lease.holder_instance_id,
                    lease_id: lease.lease_id,
                    fencing_epoch: lease.fencing_epoch,
                    expires_at_unix_ms: expires_at,
                };
                let bind_now = setup_now(&clock, &mut last_now, expires_at)?;
                match self.store.bind_initial_run_table_pin(&pin, bind_now).await {
                    Ok(bound) if bound == pin => {
                        bound_pin = Some(bound);
                        break;
                    }
                    Ok(_) => {
                        return Err(RoutingError::new(
                            RoutingErrorKind::Conflict,
                            "SQL run-table initializer returned a different immutable owner",
                        ));
                    }
                    Err(error) if error.kind() == RoutingErrorKind::Conflict => {
                        let refresh_now = setup_now(&clock, &mut last_now, expires_at)?;
                        self.refresh(refresh_now).await?;
                    }
                    Err(error) => return Err(error),
                }
            }
            pins.push(bound_pin.ok_or_else(|| {
                RoutingError::new(
                    RoutingErrorKind::PoolUnavailable,
                    "could not claim and bind an eligible collector for the OAT table",
                )
            })?);
            let refresh_now = setup_now(&clock, &mut last_now, expires_at)?;
            view = self.refresh(refresh_now).await?;
        }
        setup_now(&clock, &mut last_now, expires_at)?;
        Ok(pins)
    }

    async fn assignment_for_run_pin(&self, pin: &RunTablePin, now: UnixMillis) -> RoutingResult<RouteAssignment> {
        let view = self.refresh(now).await?;
        let lease = view.leases.get(&pin.key).ok_or_else(|| {
            RoutingError::new(
                RoutingErrorKind::Conflict,
                "pinned routing lease is missing from the fresh view",
            )
        })?;
        if !lease.is_routable_at(now)
            || lease.holder_instance_id != pin.holder_instance_id
            || lease.lease_id != pin.lease_id
            || lease.fencing_epoch != pin.fencing_epoch
        {
            return Err(RoutingError::new(
                RoutingErrorKind::Conflict,
                "immutable OAT run-table owner changed or expired",
            ));
        }
        assignment_for_lease(&view, lease, now, &self.policy, RouteSource::CachedLease)
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
                        distinct_table_owners: self.policy.distinct_table_owners,
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
                        .any(|candidate| {
                            candidate != lease.holder_instance_id
                                && (!self.policy.distinct_table_owners
                                    || !has_other_active_table_lease(&view, &lease.key, candidate, now))
                        });
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
                        .find(|candidate| {
                            *candidate != lease.holder_instance_id
                                && (!self.policy.distinct_table_owners
                                    || !has_other_active_table_lease(&view, &lease.key, *candidate, now))
                        });
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
                            distinct_table_owners: self.policy.distinct_table_owners,
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

fn has_other_active_table_lease(
    view: &RoutingView,
    key: &LeaseKey,
    instance_id: CollectorInstanceId,
    now: UnixMillis,
) -> bool {
    view.leases.values().any(|lease| {
        lease.key.implementation_pool == key.implementation_pool
            && lease.key != *key
            && lease.holder_instance_id == instance_id
            && lease.is_routable_at(now)
    })
}

fn canonical_oat_metadata(metadata: &IngestMetadata) -> RoutingResult<IngestMetadata> {
    let table = metadata.table();
    let canonical = TableIdentity::new(
        CatalogName::new(table.catalog().as_str().to_ascii_lowercase()).map_err(|cause| {
            RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                format!("invalid canonical OAT catalog identity: {cause}"),
            )
        })?,
        SchemaName::new(table.schema().as_str().to_ascii_lowercase()).map_err(|cause| {
            RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                format!("invalid canonical OAT schema identity: {cause}"),
            )
        })?,
        TableName::new(table.table().as_str().to_ascii_lowercase()).map_err(|cause| {
            RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                format!("invalid canonical OAT table identity: {cause}"),
            )
        })?,
    );
    Ok(IngestMetadata::new(
        metadata.protocol_version(),
        metadata.implementation_pool(),
        canonical,
        metadata.claimed_schema_fingerprint(),
    ))
}

fn validate_oat_run_id(run_id: &str, expires_at: UnixMillis, now: UnixMillis) -> RoutingResult<()> {
    if uuid::Uuid::parse_str(run_id).is_err() {
        return Err(RoutingError::new(
            RoutingErrorKind::InvalidConfiguration,
            "OAT run ID must be a UUID",
        ));
    }
    if expires_at <= now {
        return Err(RoutingError::new(
            RoutingErrorKind::InvalidConfiguration,
            "OAT run expiry must be later than the current time",
        ));
    }
    Ok(())
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

fn setup_now<F>(clock: &F, last_now: &mut UnixMillis, expires_at: UnixMillis) -> RoutingResult<UnixMillis>
where
    F: Fn() -> RoutingResult<UnixMillis>,
{
    let now = clock()?;
    if now < *last_now {
        return Err(RoutingError::new(
            RoutingErrorKind::InvalidConfiguration,
            "OAT setup clock regressed",
        ));
    }
    if now >= expires_at {
        return Err(RoutingError::new(
            RoutingErrorKind::Conflict,
            "immutable OAT run pin set expired during setup",
        ));
    }
    *last_now = now;
    Ok(now)
}

fn subtract_millis(value: UnixMillis, delta: u64) -> RoutingResult<UnixMillis> {
    UnixMillis::new(value.get().saturating_sub(delta))
        .map_err(|cause| RoutingError::new(RoutingErrorKind::InvalidConfiguration, cause.to_string()))
}
