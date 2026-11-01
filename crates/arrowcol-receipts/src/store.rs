use arrowcol_protocol::{CollectorInstanceId, ReceiptId, TableLease, UnixMillis};
use async_trait::async_trait;

use crate::{
    OwnerDrain, OwnerRegistration, PublicationBatch, ReceiptAdoption, ReceiptAdoptionReport, ReceiptFailure,
    ReceiptRecord, ReceiptResult, ReceiptVersion, ReconciliationReport, TableLeaseRequest, ValidatedDelivery,
};

/// Durable receipt operations used by direct collector and buffer workers.
#[async_trait]
pub trait ReceiptStore: Send + Sync {
    /// Registers a process lifetime as active for receipt ownership.
    ///
    /// A registration with a durable recovery window protects the owner's
    /// active receipts from expiry reconciliation; see
    /// [`OwnerRegistration::durable_recovery_window_ms`].
    async fn register_owner(&self, registration: OwnerRegistration) -> ReceiptResult<()>;

    /// Extends an active owner lease.
    async fn heartbeat_owner(&self, owner: CollectorInstanceId, lease_deadline: UnixMillis) -> ReceiptResult<()>;

    /// Starts graceful drain and returns outstanding owned work.
    async fn start_owner_drain(&self, owner: CollectorInstanceId) -> ReceiptResult<OwnerDrain>;

    /// Marks a fully drained owner stopped and releases its table leases.
    async fn finish_owner_drain(&self, owner: CollectorInstanceId, stopped_at: UnixMillis) -> ReceiptResult<()>;

    /// Acquires a fresh table lease and monotonically advances its fence.
    async fn acquire_table_lease(&self, request: TableLeaseRequest) -> ReceiptResult<TableLease>;

    /// Extends one active table lease without changing its fencing epoch.
    async fn renew_table_lease(&self, lease: &TableLease, expires_at: UnixMillis) -> ReceiptResult<TableLease>;

    /// Releases one active table lease.
    async fn release_table_lease(&self, lease: &TableLease, released_at: UnixMillis) -> ReceiptResult<()>;

    /// Transactionally creates fresh server receipt IDs for validated streams.
    ///
    /// Duplicate metadata always creates independent receipts.
    async fn create_flight_batch(
        &self,
        owner: CollectorInstanceId,
        lease: &TableLease,
        lease_deadline: UnixMillis,
        deliveries: &[ValidatedDelivery],
    ) -> ReceiptResult<Vec<ReceiptRecord>>;

    /// Transactionally transitions accepted receipts into the bounded buffer.
    async fn mark_buffered(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>>;

    /// Transactionally transitions buffered receipts into a flush attempt.
    async fn mark_flushing(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>>;

    /// Extends in-process receipt ownership without changing state revision.
    async fn heartbeat_receipts(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<()>;

    /// Atomically completes every receipt made visible by one Delta commit.
    async fn publish_commit(
        &self,
        owner: CollectorInstanceId,
        batch: &PublicationBatch,
    ) -> ReceiptResult<Vec<ReceiptRecord>>;

    /// Atomically records a structured retryable or permanent failure.
    async fn fail_batch(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        failure: &ReceiptFailure,
        failed_at: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>>;

    /// Returns one receipt status.
    async fn receipt(&self, receipt_id: ReceiptId) -> ReceiptResult<Option<ReceiptRecord>>;

    /// Returns a bounded set of receipt statuses in request order.
    async fn receipts(&self, receipt_ids: &[ReceiptId]) -> ReceiptResult<Vec<Option<ReceiptRecord>>>;

    /// Reconciles expired in-process work without blind publication replay.
    ///
    /// Accepted or buffered work becomes retryable. Expired flushing work is
    /// recorded as publication-outcome-unknown and requires explicit operator
    /// or storage reconciliation before any resend. Receipts whose owner
    /// registered a durable recovery window are skipped while that owner's
    /// lease is unexpired and until the window elapses after the owner lease
    /// expires, so durably buffered backlog may outlive receipt deadlines and
    /// a restarted successor can adopt it.
    async fn reconcile_expired(&self, now: UnixMillis, limit: usize) -> ReceiptResult<ReconciliationReport>;

    /// Atomically adopts recovered active receipts for a successor owner.
    ///
    /// A candidate is adopted only when the receipt still has its expected
    /// owner, state, and revision and is accepted, buffered, or flushing; the
    /// successor is active with an unexpired owner lease; the supplied lease is
    /// the successor's unreleased, unexpired lease for the receipt's table; the
    /// new deadline fits within both leases; and the expected owner is either
    /// the successor itself (rebinding to a newer table lease) or a failed or
    /// stopped predecessor with the same instance key and a lower generation.
    ///
    /// Adoption rebinds the owner, table lease, fencing epoch, deadline, and
    /// heartbeat, keeps state and revision, and appends an `adopted` receipt
    /// event. Every other candidate is reported as rejected together with its
    /// current record so the caller can drop, retry, or replay it. Adopting
    /// flushing work does not resolve its publication uncertainty.
    async fn adopt_receipts(&self, request: &ReceiptAdoption) -> ReceiptResult<ReceiptAdoptionReport>;

    /// Fails accepted receipts that a crashed predecessor never durably buffered.
    ///
    /// A durable-buffer collector creates an `accepted` receipt before it
    /// persists the batch locally and only acknowledges the client after the
    /// batch is persisted. A predecessor that crashed in between leaves an
    /// `accepted` receipt with no persisted bundle, so no recovery can adopt
    /// or publish it. Call this only after the successor has drained every
    /// persisted bundle: any predecessor receipt that still is `accepted` then
    /// had no bundle. It atomically records at most `limit` such receipts of
    /// failed or stopped predecessors that share the successor's instance key
    /// and have a lower generation as retryable failures, with a receipt
    /// event. Buffered and flushing receipts are never touched.
    async fn fail_unbuffered_predecessor_receipts(
        &self,
        successor: CollectorInstanceId,
        failed_at: UnixMillis,
        limit: usize,
    ) -> ReceiptResult<ReconciliationReport>;
}
