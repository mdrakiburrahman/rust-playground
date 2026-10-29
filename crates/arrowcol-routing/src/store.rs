use arrowcol_protocol::{CollectorInstanceId, UnixMillis};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    CollectorRegistration, CollectorState, LeaseKey, RichPressureSnapshot, RoutingError, RoutingErrorKind,
    RoutingLease, RoutingResult, RoutingView,
};

/// Request to create or observe an active lease.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct LeaseClaim {
    /// Ownership key.
    pub key: LeaseKey,
    /// Deterministically selected candidate.
    pub candidate_instance_id: CollectorInstanceId,
    /// Lease issue time.
    pub now_unix_ms: UnixMillis,
    /// Exclusive lease expiry.
    pub expires_at_unix_ms: UnixMillis,
    /// Oldest pressure timestamp acceptable at compare-and-set time.
    pub minimum_snapshot_at_unix_ms: UnixMillis,
}

/// Compare-and-set request to begin drain.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct LeaseCas {
    /// Ownership key.
    pub key: LeaseKey,
    /// Expected logical version.
    pub expected_version: u64,
    /// Mutation time.
    pub now_unix_ms: UnixMillis,
}

/// Compare-and-set request to transfer a fully drained lease.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct LeaseTransfer {
    /// Ownership key.
    pub key: LeaseKey,
    /// Expected draining version.
    pub expected_version: u64,
    /// New owner.
    pub candidate_instance_id: CollectorInstanceId,
    /// New lease issue time.
    pub now_unix_ms: UnixMillis,
    /// New exclusive expiry.
    pub expires_at_unix_ms: UnixMillis,
    /// Oldest pressure timestamp acceptable for the new owner.
    pub minimum_snapshot_at_unix_ms: UnixMillis,
}

/// Compare-and-set request to hand a crashed holder's lease directly to the
/// restarted process registered under the same instance key.
///
/// No drain proof is required: the successor owns the predecessor's durable
/// buffer volume and replays its persisted backlog.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct LeaseSuccession {
    /// Ownership key.
    pub key: LeaseKey,
    /// Expected active or draining version.
    pub expected_version: u64,
    /// Current holder whose registration expired or reached a terminal state.
    pub predecessor_instance_id: CollectorInstanceId,
    /// Live process lifetime registered under the predecessor's instance key.
    pub successor_instance_id: CollectorInstanceId,
    /// New lease issue time.
    pub now_unix_ms: UnixMillis,
    /// New exclusive expiry.
    pub expires_at_unix_ms: UnixMillis,
    /// Oldest pressure timestamp acceptable for the successor.
    pub minimum_snapshot_at_unix_ms: UnixMillis,
}

impl LeaseSuccession {
    /// Validates request bounds before any persistence access.
    ///
    /// # Errors
    ///
    /// Returns invalid configuration when the new lease window is empty or the
    /// predecessor and successor are the same process lifetime.
    pub fn validate(&self) -> RoutingResult<()> {
        if self.expires_at_unix_ms <= self.now_unix_ms {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "routing lease expiry must be later than issuance",
            ));
        }
        if self.predecessor_instance_id == self.successor_instance_id {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "routing lease successor must be a different process lifetime",
            ));
        }
        Ok(())
    }
}

/// Persistence contract shared by router replicas.
#[async_trait]
pub trait RoutingStore: Send + Sync {
    /// Whether transport failures may be retried while independent SQL fencing remains enforced.
    fn is_remote_authority(&self) -> bool {
        false
    }

    /// Returns explicit controlled moves relevant to a process lifetime.
    async fn migration_work(&self, _instance_id: CollectorInstanceId) -> RoutingResult<Vec<crate::TableMigration>> {
        Ok(Vec::new())
    }

    /// Records a participant's idempotent, exact-operation acknowledgement.
    async fn acknowledge_migration(
        &self,
        _migration: &crate::TableMigration,
        _instance_id: CollectorInstanceId,
        _ack: crate::MigrationAck,
    ) -> RoutingResult<()> {
        Err(RoutingError::new(
            RoutingErrorKind::InvalidConfiguration,
            "migration acknowledgements unsupported",
        ))
    }

    /// Requests a controller-selected explicit move.
    async fn request_migration(
        &self,
        _key: &LeaseKey,
        _reason: &str,
        _force_scale_in: bool,
    ) -> RoutingResult<crate::TableMigration> {
        Err(RoutingError::new(
            RoutingErrorKind::InvalidConfiguration,
            "controlled migration unsupported",
        ))
    }
    /// Registers or refreshes one collector process lifetime.
    async fn register_collector(&self, registration: &CollectorRegistration) -> RoutingResult<()>;

    /// Extends a collector registration lease.
    async fn heartbeat_collector(
        &self,
        instance_id: CollectorInstanceId,
        heartbeat_at: UnixMillis,
        lease_expires_at: UnixMillis,
    ) -> RoutingResult<()>;

    /// Changes collector lifecycle state.
    async fn set_collector_state(
        &self,
        instance_id: CollectorInstanceId,
        state: CollectorState,
        changed_at: UnixMillis,
    ) -> RoutingResult<()>;

    /// Publishes a validated latest pressure snapshot plus history.
    async fn publish_pressure(&self, snapshot: &RichPressureSnapshot) -> RoutingResult<()>;

    /// Loads the complete immutable routing view.
    async fn load_view(&self, loaded_at: UnixMillis) -> RoutingResult<RoutingView>;

    /// Atomically creates a lease or returns the unexpired winner.
    async fn claim_lease(&self, claim: &LeaseClaim) -> RoutingResult<RoutingLease>;

    /// Atomically changes an active lease to draining.
    async fn begin_lease_drain(&self, request: &LeaseCas) -> RoutingResult<RoutingLease>;

    /// Atomically transfers a draining lease with a greater fencing epoch.
    async fn transfer_drained_lease(&self, request: &LeaseTransfer) -> RoutingResult<RoutingLease>;

    /// Atomically transfers a valid active or draining lease from a dead
    /// holder to its same-instance-key successor without a drain proof.
    ///
    /// The predecessor must still hold the expected lease version, and its
    /// registration must have expired or reached the stopped or failed state.
    /// The successor must be a different, live, ready process lifetime in the
    /// lease's pool with the predecessor's instance key and fresh accepting
    /// pressure. Success issues a new active lease ID with the next fencing
    /// epoch and lease version. Invalid request bounds return
    /// [`RoutingErrorKind::InvalidConfiguration`]; any unmet persisted
    /// precondition returns [`RoutingErrorKind::Conflict`] without mutation.
    async fn transfer_to_successor(&self, request: &LeaseSuccession) -> RoutingResult<RoutingLease>;
}
