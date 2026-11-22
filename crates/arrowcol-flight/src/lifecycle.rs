use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use arrowcol_catalog::{CatalogSchemaVersion, SchemaAlignmentPlan};
use arrowcol_protocol::{AuthContext, IngestMetadata, SchemaFingerprint};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{PutResultMetadata, RuntimeResult, StreamCompletion};

/// Fully validated stream context supplied before any admission side effect.
#[derive(Clone, Debug)]
pub struct StreamContext {
    metadata: IngestMetadata,
    authentication: AuthContext,
    received_schema: SchemaRef,
    registered_schema: CatalogSchemaVersion,
    current_schema: CatalogSchemaVersion,
    alignment: SchemaAlignmentPlan,
    actual_fingerprint: SchemaFingerprint,
    deadline: Option<Instant>,
    trusted_client_send_observation: Option<(String, u64)>,
}

impl StreamContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        metadata: IngestMetadata,
        authentication: AuthContext,
        received_schema: SchemaRef,
        registered_schema: CatalogSchemaVersion,
        current_schema: CatalogSchemaVersion,
        alignment: SchemaAlignmentPlan,
        actual_fingerprint: SchemaFingerprint,
        deadline: Option<Instant>,
        trusted_client_send_observation: Option<(String, u64)>,
    ) -> Self {
        Self {
            metadata,
            authentication,
            received_schema,
            registered_schema,
            current_schema,
            alignment,
            actual_fingerprint,
            deadline,
            trusted_client_send_observation,
        }
    }

    /// Returns validated routing metadata.
    #[must_use]
    pub const fn metadata(&self) -> &IngestMetadata {
        &self.metadata
    }

    /// Returns the authenticated principal context.
    #[must_use]
    pub const fn authentication(&self) -> &AuthContext {
        &self.authentication
    }

    /// Returns the exact Arrow schema decoded from the request.
    #[must_use]
    pub const fn received_schema(&self) -> &SchemaRef {
        &self.received_schema
    }

    /// Returns the exact registered historical or current schema version.
    #[must_use]
    pub const fn registered_schema(&self) -> &CatalogSchemaVersion {
        &self.registered_schema
    }

    /// Returns the current authoritative schema version.
    #[must_use]
    pub const fn current_schema(&self) -> &CatalogSchemaVersion {
        &self.current_schema
    }

    /// Returns the exact/copy/null-fill alignment plan.
    #[must_use]
    pub const fn alignment(&self) -> &SchemaAlignmentPlan {
        &self.alignment
    }

    /// Returns the fingerprint computed from the received Arrow schema.
    #[must_use]
    pub const fn actual_fingerprint(&self) -> SchemaFingerprint {
        self.actual_fingerprint
    }

    /// Returns the authenticated client DoPut observation, if supplied by a trusted forwarder.
    #[must_use]
    pub fn trusted_client_send_observation(&self) -> Option<(&str, u64)> {
        self.trusted_client_send_observation
            .as_ref()
            .map(|(id, timestamp)| (id.as_str(), *timestamp))
    }

    /// Returns the effective stream deadline.
    #[must_use]
    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}

/// Incremental summary of admitted record batches.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct StreamSummary {
    pub(crate) batches: u64,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
}

impl StreamSummary {
    /// Creates a summary from already measured values.
    #[must_use]
    pub const fn new(batches: u64, rows: u64, bytes: u64) -> Self {
        Self { batches, rows, bytes }
    }

    /// Returns the admitted batch count.
    #[must_use]
    pub const fn batches(self) -> u64 {
        self.batches
    }

    /// Returns the admitted row count.
    #[must_use]
    pub const fn rows(self) -> u64 {
        self.rows
    }

    /// Returns approximate Arrow array bytes admitted.
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.bytes
    }

    pub(crate) fn record(&mut self, batch: &RecordBatch) {
        self.batches = self.batches.saturating_add(1);
        self.rows = self
            .rows
            .saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        self.bytes = self
            .bytes
            .saturating_add(u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX));
    }
}

/// Reason an admitted stream was aborted before normal completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamAbortReason {
    /// The client cancelled or dropped the response stream.
    ClientCancelled,
    /// The effective deadline elapsed.
    DeadlineExceeded,
    /// Forced shutdown cancelled active work.
    Shutdown,
    /// Arrow Flight decoding or sequencing failed.
    DecodeFailure,
    /// The implementation rejected a batch or completion.
    AdmissionFailure,
}

/// Final runtime outcome reported to pressure hooks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamOutcome {
    /// Admission completed normally.
    Completed,
    /// The client cancelled the stream.
    Cancelled,
    /// A deadline elapsed.
    DeadlineExceeded,
    /// Forced shutdown interrupted the stream.
    Shutdown,
    /// Protocol or Arrow decoding failed.
    DecodeFailed,
    /// The implementation rejected admitted work.
    AdmissionFailed,
}

/// Runtime pressure event emitted without coupling to a metrics backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PressureEvent {
    /// A validated stream began admission.
    StreamStarted,
    /// A decoded batch entered the bounded channel.
    BatchQueued {
        /// Approximate Arrow array bytes.
        bytes: u64,
    },
    /// A batch left the bounded channel for implementation admission.
    BatchDequeued {
        /// Approximate Arrow array bytes.
        bytes: u64,
    },
    /// A stream finished or aborted.
    StreamFinished {
        /// Final admitted summary.
        summary: StreamSummary,
        /// Final outcome.
        outcome: StreamOutcome,
    },
}

/// Non-blocking pressure observation hook.
pub trait PressureHooks: Send + Sync {
    /// Observes one runtime pressure transition.
    fn observe(&self, event: PressureEvent);
}

/// Pressure hook that intentionally performs no work.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopPressureHooks;

impl PressureHooks for NoopPressureHooks {
    fn observe(&self, _event: PressureEvent) {}
}

/// Sink implementations can use for incremental `PutResult` acknowledgements.
#[async_trait]
pub trait PutResultEmitter: Send + Sync {
    /// Emits stable application metadata with response-stream backpressure.
    ///
    /// # Errors
    ///
    /// Returns cancellation when the client no longer receives results.
    async fn emit(&self, metadata: PutResultMetadata) -> RuntimeResult<()>;
}

/// Starts implementation-specific admission after schema validation succeeds.
#[async_trait]
pub trait BatchAdmission: Send + Sync {
    /// Creates one stream lifecycle.
    ///
    /// This method is the earliest point at which direct implementations may
    /// create receipts. The runtime never calls it for unknown or breaking
    /// schemas.
    ///
    /// # Errors
    ///
    /// Returns a safe admission failure.
    async fn begin(
        &self,
        context: StreamContext,
        emitter: Arc<dyn PutResultEmitter>,
    ) -> RuntimeResult<Box<dyn StreamLifecycle>>;
}

/// Implementation-specific lifecycle for one incrementally decoded stream.
#[async_trait]
pub trait StreamLifecycle: Send {
    /// Admits one current-schema record batch.
    ///
    /// # Errors
    ///
    /// Returns a safe capacity, receipt, publication, or internal failure.
    async fn push_batch(&mut self, batch: RecordBatch) -> RuntimeResult<()>;

    /// Completes admission and selects durable-receipt or best-effort mode.
    ///
    /// # Errors
    ///
    /// Returns a safe completion failure.
    async fn complete(&mut self, summary: StreamSummary) -> RuntimeResult<StreamCompletion>;

    /// Releases or marks partial state after cancellation or failure.
    async fn abort(self: Box<Self>, reason: StreamAbortReason);
}

#[derive(Debug)]
struct DrainState {
    accepting: AtomicBool,
    active: AtomicUsize,
    notify: Notify,
    force_stop: CancellationToken,
}

/// Shared stop-admission and active-stream drain controller.
#[derive(Clone, Debug)]
pub struct DrainController {
    state: Arc<DrainState>,
}

impl Default for DrainController {
    fn default() -> Self {
        Self::new()
    }
}

impl DrainController {
    /// Creates an accepting drain controller with no active streams.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(DrainState {
                accepting: AtomicBool::new(true),
                active: AtomicUsize::new(0),
                notify: Notify::new(),
                force_stop: CancellationToken::new(),
            }),
        }
    }

    /// Returns whether new requests may begin.
    #[must_use]
    pub fn is_accepting(&self) -> bool {
        self.state.accepting.load(Ordering::Acquire)
    }

    /// Returns the current number of active requests.
    #[must_use]
    pub fn active_streams(&self) -> usize {
        self.state.active.load(Ordering::Acquire)
    }

    /// Atomically stops admitting new requests.
    pub fn begin_drain(&self) {
        self.state.accepting.store(false, Ordering::Release);
        self.state.notify.notify_waiters();
    }

    /// Cancels active requests after their implementation abort hooks run.
    pub fn force_stop(&self) {
        self.begin_drain();
        self.state.force_stop.cancel();
    }

    /// Waits for active requests to drain, then force-cancels on timeout.
    pub async fn graceful_stop(&self, timeout: Duration) -> DrainOutcome {
        self.begin_drain();
        if tokio::time::timeout(timeout, self.wait_for_drain()).await.is_ok() {
            DrainOutcome::Drained
        } else {
            let active = self.active_streams();
            self.force_stop();
            DrainOutcome::TimedOut { active }
        }
    }

    pub(crate) fn try_acquire(&self) -> Option<DrainPermit> {
        if !self.is_accepting() {
            return None;
        }
        self.state.active.fetch_add(1, Ordering::AcqRel);
        if !self.is_accepting() {
            release_active(&self.state);
            return None;
        }
        Some(DrainPermit {
            state: Arc::clone(&self.state),
        })
    }

    pub(crate) fn force_stop_token(&self) -> CancellationToken {
        self.state.force_stop.clone()
    }

    async fn wait_for_drain(&self) {
        loop {
            if self.active_streams() == 0 {
                return;
            }
            let notified = self.state.notify.notified();
            if self.active_streams() == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// Implementation-neutral graceful shutdown control surface.
#[async_trait]
pub trait ShutdownCoordinator: Send + Sync {
    /// Returns whether new requests may begin.
    fn is_accepting(&self) -> bool;

    /// Returns the number of active requests.
    fn active_streams(&self) -> usize;

    /// Stops admitting new requests while active work continues.
    fn begin_drain(&self);

    /// Force-cancels active requests.
    fn force_stop(&self);

    /// Stops admission and waits for active work up to `timeout`.
    async fn graceful_stop(&self, timeout: Duration) -> DrainOutcome;
}

#[async_trait]
impl ShutdownCoordinator for DrainController {
    fn is_accepting(&self) -> bool {
        DrainController::is_accepting(self)
    }

    fn active_streams(&self) -> usize {
        DrainController::active_streams(self)
    }

    fn begin_drain(&self) {
        DrainController::begin_drain(self);
    }

    fn force_stop(&self) {
        DrainController::force_stop(self);
    }

    async fn graceful_stop(&self, timeout: Duration) -> DrainOutcome {
        DrainController::graceful_stop(self, timeout).await
    }
}

/// Result of a graceful stop request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainOutcome {
    /// All admitted work completed before the timeout.
    Drained,
    /// The timeout elapsed and active work was force-cancelled.
    TimedOut {
        /// Number of requests still active when force cancellation began.
        active: usize,
    },
}

pub(crate) struct DrainPermit {
    state: Arc<DrainState>,
}

impl Drop for DrainPermit {
    fn drop(&mut self) {
        release_active(&self.state);
    }
}

fn release_active(state: &DrainState) {
    let previous = state.active.fetch_sub(1, Ordering::AcqRel);
    if previous <= 1 {
        state.notify.notify_waiters();
    }
}
