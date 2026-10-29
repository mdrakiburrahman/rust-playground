use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use arrowcol_protocol::{
    BasisPoints, CollectorInstanceId, FencingToken, ImplementationPool, LeaseId, Measurement, PositiveMeasurement,
    PressureGauge, TableIdentity, UnixMillis,
};
use serde::{Deserialize, Serialize};

use crate::{RoutingError, RoutingErrorKind, RoutingResult};

/// Maximum table-specific observations accepted in one collector snapshot.
pub const MAX_TABLE_PRESSURE_ENTRIES: usize = 256;

/// Collector lifecycle state used by the routing control plane.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectorState {
    /// The process can receive newly routed work.
    Ready,
    /// The process refuses new work while existing streams finish.
    Draining,
    /// The process stopped cleanly.
    Stopped,
    /// The process is unhealthy.
    Failed,
}

/// Readiness state for a collector subsystem.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubsystemState {
    /// The subsystem is fully available.
    Ready,
    /// The subsystem is available but experiencing pressure.
    Degraded,
    /// The subsystem cannot safely accept new work.
    Blocked,
    /// The implementation does not use this subsystem.
    Disabled,
}

/// Registration for one collector process lifetime.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct CollectorRegistration {
    /// Process-lifetime identifier.
    pub instance_id: CollectorInstanceId,
    /// Stable deployment key.
    pub instance_key: String,
    /// Collector implementation pool.
    pub implementation_pool: ImplementationPool,
    /// Cleartext or internally terminated HTTP/2 Flight endpoint.
    pub flight_endpoint: SocketAddr,
    /// Whether this backend can execute Flight SQL catalog operations.
    pub catalog_capable: bool,
    /// Advertised concurrent-stream capacity.
    pub max_concurrent_streams: PositiveMeasurement,
    /// Registration timestamp.
    pub started_at_unix_ms: UnixMillis,
    /// Last heartbeat timestamp.
    pub heartbeat_at_unix_ms: UnixMillis,
    /// Exclusive registration lease expiry.
    pub lease_expires_at_unix_ms: UnixMillis,
}

impl CollectorRegistration {
    /// Validates registration identity and timestamps.
    ///
    /// # Errors
    ///
    /// Returns invalid configuration for an invalid key or time range.
    pub fn validate(&self) -> RoutingResult<()> {
        if self.instance_key.is_empty()
            || self.instance_key.len() > 200
            || self.instance_key.chars().any(char::is_control)
        {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "collector instance key must be 1..=200 bytes without control characters",
            ));
        }
        if self.heartbeat_at_unix_ms < self.started_at_unix_ms
            || self.lease_expires_at_unix_ms <= self.heartbeat_at_unix_ms
        {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "collector registration timestamps are not monotonic",
            ));
        }
        Ok(())
    }

    /// Returns whether the registration lease is valid at an instant.
    #[must_use]
    pub fn is_live_at(&self, now: UnixMillis) -> bool {
        now >= self.started_at_unix_ms && now < self.lease_expires_at_unix_ms
    }
}

/// Per-table pressure required to prove drain completion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TablePressure {
    /// Fully qualified table.
    pub table: TableIdentity,
    /// Streams currently admitted for this table.
    pub active_streams: Measurement,
    /// Buffered logical Arrow bytes.
    pub buffered_bytes: Measurement,
    /// Buffered rows.
    pub buffered_rows: Measurement,
    /// Buffered record batches.
    pub buffered_batches: Measurement,
    /// Age of the oldest buffered delivery.
    pub oldest_buffer_age_ms: Measurement,
    /// Pending Delta commit attempts.
    pub pending_commits: Measurement,
}

impl TablePressure {
    /// Returns whether this table has no admitted, buffered, or committing work.
    #[must_use]
    pub fn is_drained(&self) -> bool {
        self.active_streams.get() == 0
            && self.buffered_bytes.get() == 0
            && self.buffered_rows.get() == 0
            && self.buffered_batches.get() == 0
            && self.pending_commits.get() == 0
    }
}

/// Rich bounded collector pressure document.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerMemory {
    /// Unclamped cgroup memory.current.
    pub current_bytes: Measurement,
    /// Finite cgroup memory.max.
    pub limit_bytes: PositiveMeasurement,
}

/// Rich bounded collector pressure document.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RichPressureSnapshot {
    /// Actual container usage/limit when a finite cgroup limit is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_memory: Option<ContainerMemory>,
    /// Effective affinity/cgroup CPU capacity, in thousandths of one core.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_cpu_millicores: Option<PositiveMeasurement>,
    /// Collector process lifetime.
    pub instance_id: CollectorInstanceId,
    /// Observation timestamp.
    pub observed_at_unix_ms: UnixMillis,
    /// CPU utilization.
    pub cpu_utilization_bps: BasisPoints,
    /// Resident memory bytes.
    pub rss_bytes: Measurement,
    /// Configured resident memory bound.
    pub rss_limit_bytes: PositiveMeasurement,
    /// Active inbound streams and configured limit.
    pub active_streams: PressureGauge,
    /// Cumulative bytes received.
    pub received_bytes: Measurement,
    /// Cumulative rows received.
    pub received_rows: Measurement,
    /// Cumulative batches received.
    pub received_batches: Measurement,
    /// Buffered logical bytes and configured limit.
    pub buffered_bytes: PressureGauge,
    /// Buffered rows and configured limit.
    pub buffered_rows: PressureGauge,
    /// Buffered batches and configured limit.
    pub buffered_batches: PressureGauge,
    /// Currently available permits and total permits.
    pub available_permits: PressureGauge,
    /// Pending commits and configured limit.
    pub pending_commits: PressureGauge,
    /// Age of the oldest buffered delivery.
    pub oldest_buffer_age_ms: Measurement,
    /// Delta publication pressure.
    pub delta_pressure_bps: BasisPoints,
    /// Quiver/buffer pressure.
    pub quiver_pressure_bps: BasisPoints,
    /// Dataflow-engine pressure.
    pub dfe_pressure_bps: BasisPoints,
    /// Delta subsystem state.
    pub delta_state: SubsystemState,
    /// Quiver/buffer subsystem state.
    pub quiver_state: SubsystemState,
    /// Dataflow-engine subsystem state.
    pub dfe_state: SubsystemState,
    /// Explicit admission decision from the collector.
    pub accepting_deliveries: bool,
    /// Bounded table-specific pressure entries.
    pub tables: Vec<TablePressure>,
}

impl RichPressureSnapshot {
    /// Validates freshness, table bounds, and duplicate table entries.
    ///
    /// # Errors
    ///
    /// Returns invalid snapshot when the observation is stale, future-dated,
    /// oversized, duplicated, or reports an impossible RSS value.
    pub fn validate_at(&self, now: UnixMillis, policy: &PressurePolicy) -> RoutingResult<()> {
        if self.observed_at_unix_ms.get() > now.get().saturating_add(policy.max_future_skew_ms) {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidSnapshot,
                "collector pressure snapshot is too far in the future",
            ));
        }
        if now.get().saturating_sub(self.observed_at_unix_ms.get()) > policy.max_snapshot_age_ms {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidSnapshot,
                "collector pressure snapshot is stale",
            ));
        }
        if self.rss_bytes.get() > self.rss_limit_bytes.get() {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidSnapshot,
                "collector RSS exceeds its declared hard limit",
            ));
        }
        if self.tables.len() > policy.max_table_entries.min(MAX_TABLE_PRESSURE_ENTRIES) {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidSnapshot,
                "collector pressure snapshot has too many table entries",
            ));
        }
        if self.oldest_buffer_age_ms.get() > policy.max_oldest_buffer_age_ms {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidSnapshot,
                "collector pressure snapshot exceeds the oldest-buffer-age bound",
            ));
        }
        let mut tables = BTreeSet::new();
        for table in &self.tables {
            if table.oldest_buffer_age_ms.get() > policy.max_oldest_buffer_age_ms {
                return Err(RoutingError::new(
                    RoutingErrorKind::InvalidSnapshot,
                    "table pressure exceeds the oldest-buffer-age bound",
                ));
            }
            if !tables.insert(table.table.clone()) {
                return Err(RoutingError::new(
                    RoutingErrorKind::InvalidSnapshot,
                    "collector pressure snapshot contains a duplicate table",
                ));
            }
        }
        Ok(())
    }

    /// Returns the maximum admission pressure in basis points.
    #[must_use]
    pub fn maximum_pressure_bps(&self) -> u16 {
        let rss = ratio_bps(self.rss_bytes.get(), self.rss_limit_bytes.get());
        let permit_pressure = BasisPoints::FULL_SCALE.saturating_sub(self.available_permits.utilization().get());
        [
            self.container_memory.map_or(0, |memory| {
                ratio_bps(memory.current_bytes.get(), memory.limit_bytes.get())
            }),
            self.cpu_utilization_bps.get(),
            rss,
            self.active_streams.utilization().get(),
            self.buffered_bytes.utilization().get(),
            self.buffered_rows.utilization().get(),
            self.buffered_batches.utilization().get(),
            permit_pressure,
            self.pending_commits.utilization().get(),
            self.delta_pressure_bps.get(),
            self.quiver_pressure_bps.get(),
            self.dfe_pressure_bps.get(),
        ]
        .into_iter()
        .max()
        .unwrap_or(0)
    }

    /// Returns whether a subsystem explicitly blocks new work.
    #[must_use]
    pub fn subsystem_blocked(&self) -> bool {
        [self.delta_state, self.quiver_state, self.dfe_state]
            .into_iter()
            .any(|state| state == SubsystemState::Blocked)
    }

    /// Returns active streams for a table when the collector reported it.
    #[must_use]
    pub fn table_active_streams(&self, table: &TableIdentity) -> Option<u64> {
        self.tables
            .iter()
            .find(|entry| &entry.table == table)
            .map(|entry| entry.active_streams.get())
    }

    /// Returns whether a reported table has no admitted, buffered, or
    /// committing work.
    #[must_use]
    pub fn table_is_drained(&self, table: &TableIdentity) -> Option<bool> {
        self.tables
            .iter()
            .find(|entry| &entry.table == table)
            .map(TablePressure::is_drained)
    }

    /// Returns whether aggregate counters show no admitted, buffered, or
    /// committing work.
    #[must_use]
    pub fn is_globally_drained(&self) -> bool {
        self.active_streams.current().get() == 0
            && self.buffered_bytes.current().get() == 0
            && self.buffered_rows.current().get() == 0
            && self.buffered_batches.current().get() == 0
            && self.pending_commits.current().get() == 0
    }
}

/// Current collector registration and pressure.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct CollectorRecord {
    /// Registration document.
    pub registration: CollectorRegistration,
    /// Routing lifecycle state.
    pub state: CollectorState,
    /// Latest accepted pressure observation.
    pub pressure: Option<RichPressureSnapshot>,
}

impl CollectorRecord {
    /// Returns whether this process lifetime has ended at an instant: its
    /// registration lease expired or it reported a terminal stopped or failed
    /// state.
    ///
    /// A terminated holder can never publish a post-drain pressure snapshot.
    #[must_use]
    pub fn is_terminated_at(&self, now: UnixMillis) -> bool {
        now >= self.registration.lease_expires_at_unix_ms
            || matches!(self.state, CollectorState::Stopped | CollectorState::Failed)
    }
}

/// Unique routing ownership key.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LeaseKey {
    /// Implementation pool.
    pub implementation_pool: ImplementationPool,
    /// Fully qualified table.
    pub table: TableIdentity,
}

impl LeaseKey {
    /// Creates a routing lease key.
    #[must_use]
    pub const fn new(implementation_pool: ImplementationPool, table: TableIdentity) -> Self {
        Self {
            implementation_pool,
            table,
        }
    }
}

/// Routing lease state.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LeaseState {
    /// New streams may use the holder.
    Active,
    /// New streams are rejected while admitted streams finish.
    Draining,
}

/// Fenced ownership for one implementation-pool/table pair.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct RoutingLease {
    /// Lease key.
    pub key: LeaseKey,
    /// Lease lineage identifier.
    pub lease_id: LeaseId,
    /// Current collector holder.
    pub holder_instance_id: CollectorInstanceId,
    /// Monotonic ownership fencing epoch.
    pub fencing_epoch: FencingToken,
    /// Monotonic compare-and-set version.
    pub lease_version: u64,
    /// Lease state.
    pub state: LeaseState,
    /// Issuance timestamp.
    pub issued_at_unix_ms: UnixMillis,
    /// Last renewal or state mutation.
    pub renewed_at_unix_ms: UnixMillis,
    /// Exclusive expiry.
    pub expires_at_unix_ms: UnixMillis,
    /// Drain start when state is draining.
    pub drain_started_at_unix_ms: Option<UnixMillis>,
}

impl RoutingLease {
    /// Validates persisted lease invariants.
    ///
    /// # Errors
    ///
    /// Returns corrupt state for an invalid version or timestamp relationship.
    pub fn validate(&self) -> RoutingResult<()> {
        if self.lease_version == 0 {
            return Err(RoutingError::new(
                RoutingErrorKind::CorruptState,
                "routing lease version must be positive",
            ));
        }
        if self.renewed_at_unix_ms < self.issued_at_unix_ms || self.expires_at_unix_ms <= self.renewed_at_unix_ms {
            return Err(RoutingError::new(
                RoutingErrorKind::CorruptState,
                "routing lease timestamps are not monotonic",
            ));
        }
        match (self.state, self.drain_started_at_unix_ms) {
            (LeaseState::Active, None) => {}
            (LeaseState::Draining, Some(started))
                if started >= self.issued_at_unix_ms && started <= self.renewed_at_unix_ms => {}
            _ => {
                return Err(RoutingError::new(
                    RoutingErrorKind::CorruptState,
                    "routing lease drain metadata disagrees with its state",
                ));
            }
        }
        Ok(())
    }

    /// Returns whether a new stream may use this lease.
    #[must_use]
    pub fn is_routable_at(&self, now: UnixMillis) -> bool {
        self.state == LeaseState::Active && now >= self.issued_at_unix_ms && now < self.expires_at_unix_ms
    }

    /// Returns whether the lease has not yet expired.
    #[must_use]
    pub fn is_valid_at(&self, now: UnixMillis) -> bool {
        now >= self.issued_at_unix_ms && now < self.expires_at_unix_ms
    }
}

/// Pressure-document validation policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PressurePolicy {
    /// Maximum observation age.
    pub max_snapshot_age_ms: u64,
    /// Maximum accepted clock skew into the future.
    pub max_future_skew_ms: u64,
    /// Maximum table entries per snapshot.
    pub max_table_entries: usize,
    /// Maximum accepted oldest-buffer age.
    pub max_oldest_buffer_age_ms: u64,
}

impl Default for PressurePolicy {
    fn default() -> Self {
        Self {
            max_snapshot_age_ms: 15_000,
            max_future_skew_ms: 2_000,
            max_table_entries: MAX_TABLE_PRESSURE_ENTRIES,
            max_oldest_buffer_age_ms: 86_400_000,
        }
    }
}

/// Routing, warm-up, lease, and readiness policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutingPolicy {
    /// Pressure validation.
    pub pressure: PressurePolicy,
    /// Hard exclusion threshold.
    pub hard_pressure_bps: u16,
    /// Collector warm-up interval.
    pub warmup_ms: u64,
    /// Minimum warm-up weight.
    pub minimum_warmup_bps: u16,
    /// New lease duration.
    pub lease_ttl_ms: u64,
    /// Maximum cached-view age for readiness.
    pub readiness_max_staleness_ms: u64,
}

impl Default for RoutingPolicy {
    fn default() -> Self {
        Self {
            pressure: PressurePolicy::default(),
            hard_pressure_bps: 9_500,
            warmup_ms: 60_000,
            minimum_warmup_bps: 1_000,
            lease_ttl_ms: 30_000,
            readiness_max_staleness_ms: 30_000,
        }
    }
}

impl RoutingPolicy {
    /// Validates policy ranges.
    ///
    /// # Errors
    ///
    /// Returns invalid configuration for zero timeouts or invalid basis points.
    pub fn validate(&self) -> RoutingResult<()> {
        if self.hard_pressure_bps == 0
            || self.hard_pressure_bps > BasisPoints::FULL_SCALE
            || self.minimum_warmup_bps == 0
            || self.minimum_warmup_bps > BasisPoints::FULL_SCALE
            || self.minimum_warmup_bps > self.hard_pressure_bps
            || self.pressure.max_snapshot_age_ms == 0
            || self.pressure.max_table_entries == 0
            || self.lease_ttl_ms == 0
            || self.readiness_max_staleness_ms == 0
        {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "routing policy contains an invalid zero or basis-point bound",
            ));
        }
        Ok(())
    }
}

/// Immutable routing cache contents.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutingView {
    /// Successful SQL refresh timestamp.
    pub loaded_at_unix_ms: UnixMillis,
    /// Registered collectors keyed by process lifetime.
    pub collectors: BTreeMap<CollectorInstanceId, CollectorRecord>,
    /// Current leases keyed by pool and table.
    pub leases: BTreeMap<LeaseKey, RoutingLease>,
}

impl RoutingView {
    /// Creates an empty initialized view.
    #[must_use]
    pub fn empty(loaded_at_unix_ms: UnixMillis) -> Self {
        Self {
            loaded_at_unix_ms,
            collectors: BTreeMap::new(),
            leases: BTreeMap::new(),
        }
    }
}

fn ratio_bps(current: u64, limit: u64) -> u16 {
    let scaled = u128::from(current) * u128::from(BasisPoints::FULL_SCALE);
    let ratio = scaled / u128::from(limit.max(1));
    u16::try_from(ratio)
        .unwrap_or(BasisPoints::FULL_SCALE)
        .min(BasisPoints::FULL_SCALE)
}
