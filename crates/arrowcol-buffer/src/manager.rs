use std::collections::{BTreeSet, HashMap, VecDeque, hash_map::Entry};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_deltalake::CompletedFileObservation;
use arrow_deltalake::record_batch_logical_bytes;
use arrow_deltalake::sizing::AdaptiveFileSizeEstimator;
use arrowcol_catalog::SchemaVersion;
use arrowcol_protocol::{
    DeltaPublicationResult, ErrorStatus, ReceiptId, SchemaFingerprint, TableIdentity, TableLease, UnixMillis,
    fingerprint_schema,
};
use arrowcol_receipts::{
    FailureDisposition, PublicationBatch, ReceiptFailure, ReceiptRecord, ReceiptStore, ReceiptVersion,
    ValidatedDelivery,
};
use tokio::sync::{Notify, Semaphore, oneshot};
use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    BufferConfig, BufferError, BufferErrorKind, BufferResult, CapacityLimits, PublicationContext, PublicationOutcome,
    PublicationSink, SinkError, SinkErrorKind,
};

/// Why one table/schema group was flushed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlushReason {
    /// Adaptive logical-input threshold for the compressed target was reached.
    AdaptiveCompressedSize,
    /// Configured Arrow memory threshold was reached.
    ArrowMemory,
    /// Configured row threshold was reached.
    Rows,
    /// Configured record-batch threshold was reached.
    Batches,
    /// The oldest delivery reached its maximum age.
    MaxAge,
    /// A new registered schema version arrived for the same table.
    SchemaBoundary,
    /// A new table lease/fencing epoch arrived for the same table.
    LeaseBoundary,
    /// A caller explicitly requested an immediate drain.
    ExplicitDrain,
    /// Admission stopped and shutdown flushed remaining work.
    Shutdown,
}

/// One validated Flight delivery offered to the bounded buffer.
pub struct Admission {
    /// Accepted receipt and expected revision.
    pub receipt: ReceiptVersion,
    /// Metadata captured from actual catalog schema validation.
    pub delivery: ValidatedDelivery,
    /// Active owner/table lease used for all later Delta publication fencing.
    pub lease: TableLease,
    /// Record batches retained without concatenation or deep copy.
    pub batches: Vec<RecordBatch>,
}

/// Durable receipt state of a delivery admitted from a local durable stage.
///
/// Fresh deliveries are `Accepted`. Deliveries recovered from durable storage
/// (for example after crash adoption) may already be `Buffered` or `Flushing`;
/// the manager then skips the receipt transitions that already happened.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrackedState {
    /// The receipt is `accepted`; admission marks it buffered.
    Accepted,
    /// The receipt is already `buffered` by the admitting owner.
    Buffered,
    /// The receipt is already `flushing`; its earlier publication outcome is
    /// unknown, so publication is retried with the same stable transaction.
    Flushing,
}

/// Final outcome of one tracked delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryOutcome {
    /// Delta committed the rows and the receipt reached `published`.
    Published,
    /// Delta committed the rows but the receipt transaction failed.
    PublishedUnrecorded,
    /// The receipt reached a terminal failure that the client observes.
    Failed,
    /// The durable outcome is unknown; the receipt may still be in progress.
    Unknown,
}

/// Resolves to the [`DeliveryOutcome`] of one tracked delivery.
///
/// A delivery dropped before publication (for example after table poisoning
/// or shutdown) resolves to [`DeliveryOutcome::Unknown`].
pub struct DeliveryCompletion {
    receiver: oneshot::Receiver<DeliveryOutcome>,
}

impl DeliveryCompletion {
    /// Waits for the delivery outcome.
    pub async fn outcome(self) -> DeliveryOutcome {
        self.receiver.await.unwrap_or(DeliveryOutcome::Unknown)
    }
}

/// Current global retained/in-flight pressure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BufferPressureSnapshot {
    /// Whether new admission is accepted.
    pub accepting: bool,
    /// Process-wide current/limit counters.
    pub global: TablePressureSnapshot,
    /// Current counters for known tables.
    pub tables: Vec<TablePressureSnapshot>,
    /// Number of publication jobs started or waiting for a worker permit.
    pub active_flushes: usize,
    /// Number of admissions currently waiting or persisting buffered state.
    pub active_admissions: usize,
}

/// Current counters for one capacity scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TablePressureSnapshot {
    /// Table identifier, absent for the process-wide row.
    pub table_id: Option<Uuid>,
    /// Retained/in-flight logical Arrow bytes.
    pub bytes: u64,
    /// Retained/in-flight rows.
    pub rows: u64,
    /// Retained/in-flight record batches.
    pub batches: u64,
    /// Configured capacity.
    pub limits: CapacityLimits,
    /// True when publication is poisoned for this table.
    pub poisoned: bool,
}

/// Work that could not be fully reflected in durable receipt state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncompleteWork {
    /// Portable table identity.
    pub table: TableIdentity,
    /// Receipts whose final durable status is incomplete.
    pub receipt_ids: Vec<ReceiptId>,
    /// Trigger that formed the publication group.
    pub flush_reason: FlushReason,
    /// Durable publication observation, when storage completed before SQL failed.
    pub durable_publication: Option<DeltaPublicationResult>,
    /// Safe failure detail.
    pub reason: String,
}

/// Result of stopping admission and draining all possible work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    /// Final pressure after all retained batches were released.
    pub pressure: BufferPressureSnapshot,
    /// Work requiring operator/client reconciliation.
    pub incomplete: Vec<IncompleteWork>,
}

/// Concurrent bounded buffer and flush coordinator.
pub struct BufferManager {
    config: BufferConfig,
    receipts: Arc<dyn ReceiptStore>,
    sink: Arc<dyn PublicationSink>,
    capacity: Arc<CapacityGate>,
    flush_slots: Arc<Semaphore>,
    state: Mutex<ManagerState>,
    idle: Notify,
}

struct ManagerState {
    lifecycle: Lifecycle,
    active_admissions: usize,
    active_flushes: usize,
    tables: HashMap<Uuid, TableState>,
    incomplete: Vec<IncompleteWork>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Lifecycle {
    Accepting,
    Draining,
    Stopped,
}

struct TableState {
    identity: TableIdentity,
    queue: Option<TableQueue>,
    pending: VecDeque<FlushJob>,
    in_flight: bool,
    estimator: AdaptiveFileSizeEstimator,
    poisoned: Option<String>,
}

struct TableQueue {
    context: QueueContext,
    entries: Vec<QueuedDelivery>,
    usage: Usage,
    first_enqueued: Instant,
}

#[derive(Clone)]
struct QueueContext {
    table_id: Uuid,
    table: TableIdentity,
    schema_version: SchemaVersion,
    schema_fingerprint: SchemaFingerprint,
    arrow_schema: SchemaRef,
    lease: TableLease,
}

struct QueuedDelivery {
    receipt: ReceiptVersion,
    ingested_at_ms: UnixMillis,
    batches: Vec<RecordBatch>,
    already_flushing: bool,
    completion: Option<oneshot::Sender<DeliveryOutcome>>,
    _capacity: CapacityReservation,
}

struct FlushJob {
    context: QueueContext,
    entries: Vec<QueuedDelivery>,
    reason: FlushReason,
}

struct JobResult {
    observation: Option<CompletedFileObservation>,
    incomplete: Option<IncompleteWork>,
    poison: Option<String>,
    outcome: DeliveryOutcome,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Usage {
    bytes: u64,
    rows: u64,
    batches: u64,
}

impl Usage {
    const fn as_limits(self) -> CapacityLimits {
        CapacityLimits {
            bytes: self.bytes,
            rows: self.rows,
            batches: self.batches,
        }
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            bytes: self.bytes.checked_add(other.bytes)?,
            rows: self.rows.checked_add(other.rows)?,
            batches: self.batches.checked_add(other.batches)?,
        })
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        Some(Self {
            bytes: self.bytes.checked_sub(other.bytes)?,
            rows: self.rows.checked_sub(other.rows)?,
            batches: self.batches.checked_sub(other.batches)?,
        })
    }
}

struct CapacityGate {
    limits: CapacityLimits,
    per_table_limits: CapacityLimits,
    state: Mutex<CapacityState>,
    changed: Notify,
}

struct CapacityState {
    accepting: bool,
    global: Usage,
    tables: HashMap<Uuid, Usage>,
}

struct CapacityReservation {
    gate: Arc<CapacityGate>,
    table_id: Uuid,
    usage: Usage,
    released: bool,
}

struct AdmissionGuard {
    manager: Weak<BufferManager>,
}

impl BufferManager {
    /// Creates a buffer and starts its bounded age-trigger worker.
    ///
    /// # Errors
    ///
    /// Returns invalid configuration when any limit is inconsistent.
    pub fn new(
        config: BufferConfig,
        receipts: Arc<dyn ReceiptStore>,
        sink: Arc<dyn PublicationSink>,
    ) -> BufferResult<Arc<Self>> {
        config.validate()?;
        let manager = Arc::new(Self {
            capacity: Arc::new(CapacityGate::new(config.global_capacity, config.per_table_capacity)),
            flush_slots: Arc::new(Semaphore::new(config.max_concurrent_flushes)),
            receipts,
            sink,
            state: Mutex::new(ManagerState {
                lifecycle: Lifecycle::Accepting,
                active_admissions: 0,
                active_flushes: 0,
                tables: HashMap::new(),
                incomplete: Vec::new(),
            }),
            idle: Notify::new(),
            config,
        });
        Self::spawn_age_worker(&manager);
        Ok(manager)
    }

    /// Admits one validated delivery, waiting asynchronously for all global and
    /// per-table byte/row/batch capacity before retaining it.
    ///
    /// # Errors
    ///
    /// Returns validation, capacity, receipt-store, poison, or shutdown errors.
    pub async fn admit(self: &Arc<Self>, admission: Admission) -> BufferResult<ReceiptRecord> {
        self.admit_inner(admission, TrackedState::Accepted, None, now_unix_millis()?)
            .await
    }

    /// Admits one delivery from a local durable stage and returns a completion
    /// that resolves once its publication outcome is known.
    ///
    /// `state` is the receipt's current durable state. `Buffered` and
    /// `Flushing` skip the transitions that already happened, so recovered
    /// deliveries can be published by their (adopting) owner. The returned
    /// record is the receipt as admitted.
    ///
    /// # Errors
    ///
    /// Returns validation, capacity, receipt-store, poison, or shutdown errors.
    pub async fn admit_tracked(
        self: &Arc<Self>,
        admission: Admission,
        state: TrackedState,
    ) -> BufferResult<(ReceiptVersion, DeliveryCompletion)> {
        self.admit_tracked_at(admission, state, now_unix_millis()?).await
    }

    /// Admits durable work with its original UTC admission timestamp.
    ///
    /// # Errors
    ///
    /// Returns the same validation, capacity, receipt, and lifecycle errors as
    /// [`Self::admit_tracked`].
    pub async fn admit_tracked_at(
        self: &Arc<Self>,
        admission: Admission,
        state: TrackedState,
        ingested_at_ms: UnixMillis,
    ) -> BufferResult<(ReceiptVersion, DeliveryCompletion)> {
        let (sender, receiver) = oneshot::channel();
        let receipt = self
            .admit_tracked_inner(admission, state, sender, ingested_at_ms)
            .await?;
        Ok((receipt, DeliveryCompletion { receiver }))
    }

    /// Checks, without reserving capacity, that a delivery could be admitted.
    ///
    /// Durable stages call this before persisting a delivery so that a batch
    /// which can never fit, or a poisoned table, is rejected while the client
    /// can still be told synchronously.
    ///
    /// # Errors
    ///
    /// Returns [`BufferErrorKind::CapacityExceeded`] when one delivery exceeds
    /// the global or per-table capacity, or the same lifecycle and poison
    /// errors as [`Self::admit`].
    pub fn check_admissible(&self, table_id: Uuid, rows: u64, bytes: u64, batches: u64) -> BufferResult<()> {
        let usage = Usage { bytes, rows, batches };
        if !self.config.global_capacity.contains(usage.as_limits())
            || !self.config.per_table_capacity.contains(usage.as_limits())
        {
            return Err(BufferError::new(
                BufferErrorKind::CapacityExceeded,
                "one delivery exceeds global or per-table buffer capacity",
            ));
        }
        self.ensure_table_accepting(table_id)
    }

    /// Admits one delivery with a bounded backpressure wait.
    ///
    /// # Errors
    ///
    /// Returns [`BufferErrorKind::BackpressureTimeout`] when the deadline wins,
    /// otherwise the same failures as [`Self::admit`].
    pub async fn admit_with_timeout(
        self: &Arc<Self>,
        admission: Admission,
        timeout: Duration,
    ) -> BufferResult<ReceiptRecord> {
        tokio::time::timeout(
            timeout,
            self.admit_inner(admission, TrackedState::Accepted, None, now_unix_millis()?),
        )
        .await
        .map_err(|_| {
            BufferError::new(
                BufferErrorKind::BackpressureTimeout,
                "timed out waiting for bounded buffer capacity",
            )
        })?
    }

    /// Flushes every currently queued table/schema group without closing admission.
    ///
    /// # Errors
    ///
    /// Returns a synchronization error if manager state was poisoned.
    pub async fn drain(self: &Arc<Self>) -> BufferResult<()> {
        self.detach_all(FlushReason::ExplicitDrain)?;
        self.schedule_ready()?;
        self.wait_for_quiescence(false).await
    }

    /// Stops admission, flushes all retained work, waits for workers, and
    /// reports every incomplete durable transition.
    ///
    /// # Errors
    ///
    /// Returns a synchronization error if manager state was poisoned.
    pub async fn shutdown(self: &Arc<Self>) -> BufferResult<ShutdownReport> {
        {
            let mut state = self.lock_state()?;
            if state.lifecycle == Lifecycle::Stopped {
                return Ok(ShutdownReport {
                    pressure: self.pressure_locked(&state)?,
                    incomplete: state.incomplete.clone(),
                });
            }
            state.lifecycle = Lifecycle::Draining;
        }
        self.capacity.close()?;
        self.detach_all(FlushReason::Shutdown)?;
        self.schedule_ready()?;
        self.wait_for_quiescence(true).await?;
        let mut state = self.lock_state()?;
        state.lifecycle = Lifecycle::Stopped;
        let pressure = self.pressure_locked(&state)?;
        Ok(ShutdownReport {
            pressure,
            incomplete: state.incomplete.clone(),
        })
    }

    /// Returns exact retained/in-flight capacity and worker pressure.
    ///
    /// # Errors
    ///
    /// Returns a synchronization error if state was poisoned.
    pub fn pressure(&self) -> BufferResult<BufferPressureSnapshot> {
        let state = self.lock_state()?;
        self.pressure_locked(&state)
    }

    async fn admit_tracked_inner(
        self: &Arc<Self>,
        admission: Admission,
        state: TrackedState,
        completion: oneshot::Sender<DeliveryOutcome>,
        ingested_at_ms: UnixMillis,
    ) -> BufferResult<ReceiptVersion> {
        if state == TrackedState::Accepted {
            let record = self
                .admit_inner(admission, state, Some(completion), ingested_at_ms)
                .await?;
            return Ok(ReceiptVersion::new(record.receipt_id, record.revision));
        }
        let _admission_guard = self.start_admission()?;
        let (usage, context) = validate_admission(&admission)?;
        self.ensure_table_accepting(context.table_id)?;
        let reservation = self.capacity.acquire(context.table_id, usage).await?;
        self.enqueue(
            context,
            QueuedDelivery {
                receipt: admission.receipt,
                ingested_at_ms,
                batches: admission.batches,
                already_flushing: state == TrackedState::Flushing,
                completion: Some(completion),
                _capacity: reservation,
            },
            usage,
        )?;
        self.schedule_ready()?;
        Ok(admission.receipt)
    }

    async fn admit_inner(
        self: &Arc<Self>,
        admission: Admission,
        state: TrackedState,
        completion: Option<oneshot::Sender<DeliveryOutcome>>,
        ingested_at_ms: UnixMillis,
    ) -> BufferResult<ReceiptRecord> {
        debug_assert_eq!(state, TrackedState::Accepted);
        let _admission_guard = self.start_admission()?;
        let (usage, context) = validate_admission(&admission)?;
        self.ensure_table_accepting(context.table_id)?;
        let reservation = self.capacity.acquire(context.table_id, usage).await?;
        let deadline = receipt_deadline(&context.lease, self.config.receipt_lease_duration)?;
        let mut buffered = self
            .receipts
            .mark_buffered(context.lease.holder_instance_id(), &[admission.receipt], deadline)
            .await
            .map_err(receipt_error)?;
        let buffered = buffered.pop().ok_or_else(|| {
            BufferError::new(
                BufferErrorKind::ReceiptStore,
                "receipt store returned no buffered record",
            )
        })?;
        if buffered.state != arrowcol_protocol::ReceiptState::Buffered {
            return Err(BufferError::new(
                BufferErrorKind::ReceiptStore,
                "receipt store returned a non-buffered record",
            ));
        }
        if buffered.table_id != admission.delivery.table_id()
            || buffered.table != *admission.delivery.table()
            || buffered.owner_instance_id != admission.lease.holder_instance_id()
            || buffered.lease_id != admission.lease.lease_id()
            || buffered.fencing_epoch != admission.lease.fencing_token()
            || buffered.schema_version != admission.delivery.schema_version()
            || buffered.schema_fingerprint != admission.delivery.schema_fingerprint()
            || buffered.rows != admission.delivery.rows()
            || buffered.batches != admission.delivery.batches()
            || buffered.arrow_memory_bytes != admission.delivery.arrow_memory_bytes()
        {
            self.fail_or_record_incomplete(
                &buffered,
                FlushReason::Shutdown,
                "buffered receipt metadata did not match its validated delivery",
            )
            .await;
            return Err(BufferError::new(
                BufferErrorKind::ReceiptStore,
                "buffered receipt metadata does not match the validated delivery and table lease",
            ));
        }
        let buffered_version = ReceiptVersion::new(buffered.receipt_id, buffered.revision);
        let enqueue = self.enqueue(
            context,
            QueuedDelivery {
                receipt: buffered_version,
                ingested_at_ms,
                batches: admission.batches,
                already_flushing: false,
                completion,
                _capacity: reservation,
            },
            usage,
        );
        if let Err(error) = enqueue {
            self.fail_or_record_incomplete(
                &buffered,
                FlushReason::Shutdown,
                "buffer closed after receipt entered buffered state",
            )
            .await;
            return Err(error);
        }
        self.schedule_ready()?;
        Ok(buffered)
    }

    fn enqueue(&self, context: QueueContext, entry: QueuedDelivery, usage: Usage) -> BufferResult<()> {
        let mut state = self.lock_state()?;
        if state.lifecycle != Lifecycle::Accepting {
            return Err(BufferError::new(
                BufferErrorKind::AdmissionClosed,
                "buffer is draining or stopped",
            ));
        }
        if let Entry::Vacant(entry) = state.tables.entry(context.table_id) {
            let estimator = AdaptiveFileSizeEstimator::new(self.config.adaptive_size).map_err(|cause| {
                BufferError::new(
                    BufferErrorKind::InvalidConfiguration,
                    format!("adaptive estimator configuration changed unexpectedly: {cause}"),
                )
            })?;
            entry.insert(TableState {
                identity: context.table.clone(),
                queue: None,
                pending: VecDeque::new(),
                in_flight: false,
                estimator,
                poisoned: None,
            });
        }
        let table = state.tables.get_mut(&context.table_id).ok_or_else(|| {
            BufferError::new(
                BufferErrorKind::Synchronization,
                "table state disappeared during admission",
            )
        })?;
        if let Some(reason) = &table.poisoned {
            return Err(BufferError::new(
                BufferErrorKind::TablePoisoned,
                format!("{}: {reason}", table.identity),
            ));
        }
        if table.queue.as_ref().is_some_and(|queue| {
            queue.context.schema_version != context.schema_version
                || queue.context.schema_fingerprint != context.schema_fingerprint
                || queue.context.arrow_schema.as_ref() != context.arrow_schema.as_ref()
        }) {
            detach_queue(table, FlushReason::SchemaBoundary);
        } else if table
            .queue
            .as_ref()
            .is_some_and(|queue| !same_fence(&queue.context.lease, &context.lease))
        {
            detach_queue(table, FlushReason::LeaseBoundary);
        }
        let queue = table.queue.get_or_insert_with(|| TableQueue {
            context,
            entries: Vec::new(),
            usage: Usage::default(),
            first_enqueued: Instant::now(),
        });
        queue.usage = queue.usage.checked_add(usage).ok_or_else(|| {
            BufferError::new(BufferErrorKind::InvalidConfiguration, "table queue counters overflowed")
        })?;
        queue.entries.push(entry);
        if let Some(reason) = flush_reason(queue, &table.estimator, &self.config) {
            detach_queue(table, reason);
        }
        Ok(())
    }

    fn ensure_table_accepting(&self, table_id: Uuid) -> BufferResult<()> {
        let state = self.lock_state()?;
        if state.lifecycle != Lifecycle::Accepting {
            return Err(BufferError::new(
                BufferErrorKind::AdmissionClosed,
                "buffer is draining or stopped",
            ));
        }
        if let Some(table) = state.tables.get(&table_id)
            && let Some(reason) = &table.poisoned
        {
            return Err(BufferError::new(
                BufferErrorKind::TablePoisoned,
                format!("{}: {reason}", table.identity),
            ));
        }
        Ok(())
    }

    fn start_admission(self: &Arc<Self>) -> BufferResult<AdmissionGuard> {
        let mut state = self.lock_state()?;
        if state.lifecycle != Lifecycle::Accepting {
            return Err(BufferError::new(
                BufferErrorKind::AdmissionClosed,
                "buffer is draining or stopped",
            ));
        }
        state.active_admissions = state
            .active_admissions
            .checked_add(1)
            .ok_or_else(|| BufferError::new(BufferErrorKind::Synchronization, "active admission counter overflowed"))?;
        Ok(AdmissionGuard {
            manager: Arc::downgrade(self),
        })
    }

    fn detach_all(&self, reason: FlushReason) -> BufferResult<()> {
        let mut state = self.lock_state()?;
        for table in state.tables.values_mut() {
            detach_queue(table, reason);
        }
        Ok(())
    }

    fn schedule_ready(self: &Arc<Self>) -> BufferResult<()> {
        let mut jobs = Vec::new();
        {
            let mut state = self.lock_state()?;
            let mut started = 0_usize;
            for (table_id, table) in &mut state.tables {
                if !table.in_flight
                    && table.poisoned.is_none()
                    && let Some(job) = table.pending.pop_front()
                {
                    table.in_flight = true;
                    started = started.checked_add(1).ok_or_else(|| {
                        BufferError::new(BufferErrorKind::Synchronization, "scheduled flush counter overflowed")
                    })?;
                    jobs.push((*table_id, job));
                }
            }
            state.active_flushes = state
                .active_flushes
                .checked_add(started)
                .ok_or_else(|| BufferError::new(BufferErrorKind::Synchronization, "active flush counter overflowed"))?;
        }
        for (table_id, job) in jobs {
            let manager = Arc::clone(self);
            tokio::spawn(async move {
                let result = manager.process_job(&job).await;
                manager.complete_job(table_id, job, result);
            });
        }
        Ok(())
    }

    async fn process_job(&self, job: &FlushJob) -> JobResult {
        let permit = match Arc::clone(&self.flush_slots).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                return incomplete_job(
                    job,
                    "flush concurrency limiter closed",
                    None,
                    Some("flush concurrency limiter closed".to_owned()),
                );
            }
        };
        let _permit = permit;
        let versions: Vec<_> = job
            .entries
            .iter()
            .filter(|entry| !entry.already_flushing)
            .map(|entry| entry.receipt)
            .collect();
        let recovered_flushing: Vec<_> = job
            .entries
            .iter()
            .filter(|entry| entry.already_flushing)
            .map(|entry| entry.receipt)
            .collect();
        let lease = match self.sink.current_lease(&job.context.table, &job.context.lease).await {
            Ok(lease) => lease,
            Err(error) => {
                return incomplete_job(job, error.message(), None, Some(error.message().to_owned()));
            }
        };
        if !job.context.lease.publication_fence().matches_lease(&lease) {
            let reason = "queued publication lease changed ownership";
            return incomplete_job(job, reason, None, Some(reason.to_owned()));
        }
        if lease != job.context.lease
            && let Err(error) = job.context.lease.validate_successor(&lease)
        {
            let reason = format!("queued publication lease renewal is invalid: {error}");
            return incomplete_job(job, &reason, None, Some(reason.clone()));
        }
        let deadline = match receipt_deadline(&lease, self.config.receipt_lease_duration) {
            Ok(deadline) => deadline,
            Err(error) => {
                return incomplete_job(job, error.message(), None, Some(error.message().to_owned()));
            }
        };
        let flushing = if versions.is_empty() {
            Vec::new()
        } else {
            match self
                .receipts
                .mark_flushing(job.context.lease.holder_instance_id(), &versions, deadline)
                .await
            {
                Ok(flushing) => flushing,
                Err(error) => {
                    return incomplete_job(
                        job,
                        &format!("could not persist flushing transition: {error}"),
                        None,
                        Some("receipt store failed before publication".to_owned()),
                    );
                }
            }
        };
        let mut flushing_versions: Vec<_> = flushing
            .iter()
            .map(|record| ReceiptVersion::new(record.receipt_id, record.revision))
            .collect();
        flushing_versions.extend(recovered_flushing);
        let publication = self.publish_job(job).await;
        match publication {
            Ok(outcome) => {
                let batch = PublicationBatch {
                    receipts: flushing_versions,
                    publication: outcome.publication.clone(),
                };
                match self
                    .receipts
                    .publish_commit(job.context.lease.holder_instance_id(), &batch)
                    .await
                {
                    Ok(_) => JobResult {
                        observation: Some(outcome.file_observation),
                        incomplete: None,
                        poison: None,
                        outcome: DeliveryOutcome::Published,
                    },
                    Err(error) => JobResult {
                        outcome: DeliveryOutcome::PublishedUnrecorded,
                        ..incomplete_job(
                            job,
                            &format!("Delta committed but receipt transaction failed: {error}"),
                            Some(outcome.publication),
                            Some("durable Delta publication is missing SQL receipt completion".to_owned()),
                        )
                    },
                }
            }
            Err(error) => {
                let (incomplete, receipt_failure) = self.persist_sink_failure(job, &flushing_versions, &error).await;
                let poison = match error.kind() {
                    SinkErrorKind::Retryable | SinkErrorKind::Cancelled if incomplete.is_none() => None,
                    SinkErrorKind::Retryable
                    | SinkErrorKind::Cancelled
                    | SinkErrorKind::Fenced
                    | SinkErrorKind::PublicationUnknown
                    | SinkErrorKind::Permanent => Some(format!("publication failed: {}", error.message())),
                };
                let outcome = if incomplete.is_none() && receipt_failure.is_none() {
                    DeliveryOutcome::Failed
                } else {
                    DeliveryOutcome::Unknown
                };
                JobResult {
                    observation: None,
                    incomplete: incomplete.or(receipt_failure),
                    poison,
                    outcome,
                }
            }
        }
    }

    async fn publish_job(&self, job: &FlushJob) -> Result<PublicationOutcome, SinkError> {
        let entry = job
            .entries
            .first()
            .ok_or_else(|| SinkError::retryable("empty publication job"))?;
        debug_assert_eq!(job.entries.len(), 1);
        let mut session = self
            .sink
            .begin(PublicationContext {
                receipt_id: entry.receipt.receipt_id,
                ingested_at_ms: entry.ingested_at_ms,
                table: job.context.table.clone(),
                schema_version: job.context.schema_version,
                schema_fingerprint: job.context.schema_fingerprint,
                arrow_schema: Arc::clone(&job.context.arrow_schema),
                fence: job.context.lease.publication_fence(),
                flush_reason: job.reason,
            })
            .await?;
        for entry in &job.entries {
            for batch in &entry.batches {
                session.write(batch).await?;
            }
        }
        session.finish().await
    }

    async fn persist_sink_failure(
        &self,
        job: &FlushJob,
        versions: &[ReceiptVersion],
        sink_error: &SinkError,
    ) -> (Option<IncompleteWork>, Option<IncompleteWork>) {
        let message = safe_error_message(sink_error.message());
        for entry in &job.entries {
            tracing::warn!(
                table = %job.context.table,
                receipt_id = %entry.receipt.receipt_id,
                sink_kind = ?sink_error.kind(),
                sink_code = ?sink_error.code(),
                error_message = %message,
                "Delta publication failed"
            );
        }
        let error = match ErrorStatus::new(sink_error.code(), message) {
            Ok(error) => error,
            Err(cause) => {
                return (
                    Some(incomplete(
                        job,
                        format!("could not construct receipt failure: {cause}"),
                        None,
                    )),
                    None,
                );
            }
        };
        let disposition = if sink_error.code().is_retryable() {
            FailureDisposition::Retryable
        } else {
            FailureDisposition::Permanent
        };
        let failure = match ReceiptFailure::new(disposition, error) {
            Ok(failure) => failure,
            Err(cause) => {
                return (
                    Some(incomplete(
                        job,
                        format!("could not validate receipt failure: {cause}"),
                        None,
                    )),
                    None,
                );
            }
        };
        let failed_at = match now_unix_millis() {
            Ok(now) => now,
            Err(error) => {
                return (Some(incomplete(job, error.message().to_owned(), None)), None);
            }
        };
        match self
            .receipts
            .fail_batch(job.context.lease.holder_instance_id(), versions, &failure, failed_at)
            .await
        {
            Ok(_) => (None, None),
            Err(error) => (
                Some(incomplete(
                    job,
                    format!("sink failed and receipt failure transaction also failed: {error}"),
                    None,
                )),
                None,
            ),
        }
    }

    fn complete_job(self: &Arc<Self>, table_id: Uuid, mut job: FlushJob, result: JobResult) {
        for entry in &mut job.entries {
            if let Some(completion) = entry.completion.take() {
                let _ = completion.send(result.outcome);
            }
        }
        let lock = self.state.lock();
        let Ok(mut state) = lock else {
            return;
        };
        if let Some(incomplete) = result.incomplete {
            state.incomplete.push(incomplete);
        }
        let mut drained_incomplete = Vec::new();
        if let Some(table) = state.tables.get_mut(&table_id) {
            table.in_flight = false;
            if let Some(observation) = result.observation
                && let Err(error) = table.estimator.observe(observation)
            {
                table.poisoned = Some(format!("adaptive estimator rejected durable observation: {error}"));
            }
            if let Some(reason) = result.poison.or_else(|| table.poisoned.clone()) {
                table.poisoned = Some(reason.clone());
                if let Some(queue) = table.queue.take() {
                    drained_incomplete.push(incomplete_from_entries(
                        &queue.context.table,
                        &queue.entries,
                        FlushReason::Shutdown,
                        format!("table poisoned before queued work could publish: {reason}"),
                    ));
                }
                while let Some(pending) = table.pending.pop_front() {
                    drained_incomplete.push(incomplete(
                        &pending,
                        format!("table poisoned before pending work could publish: {reason}"),
                        None,
                    ));
                }
            }
        }
        state.incomplete.extend(drained_incomplete);
        state.active_flushes = state.active_flushes.saturating_sub(1);
        drop(job);
        drop(state);
        self.idle.notify_waiters();
        let _scheduled = self.schedule_ready();
    }

    async fn fail_or_record_incomplete(&self, record: &ReceiptRecord, reason: FlushReason, detail: &str) {
        let message = safe_error_message(detail);
        let error = match ErrorStatus::new(arrowcol_protocol::ErrorCode::Internal, message) {
            Ok(error) => error,
            Err(_) => return,
        };
        let failure = match ReceiptFailure::new(FailureDisposition::Retryable, error) {
            Ok(failure) => failure,
            Err(_) => return,
        };
        let failed_at = match now_unix_millis() {
            Ok(value) => value,
            Err(_) => return,
        };
        if self
            .receipts
            .fail_batch(
                record.owner_instance_id,
                &[ReceiptVersion::new(record.receipt_id, record.revision)],
                &failure,
                failed_at,
            )
            .await
            .is_err()
            && let Ok(mut state) = self.state.lock()
        {
            state.incomplete.push(IncompleteWork {
                table: record.table.clone(),
                receipt_ids: vec![record.receipt_id],
                flush_reason: reason,
                durable_publication: None,
                reason: detail.to_owned(),
            });
        }
    }

    async fn wait_for_quiescence(&self, require_admissions: bool) -> BufferResult<()> {
        loop {
            let notified = self.idle.notified();
            {
                let state = self.lock_state()?;
                let queues_empty = state
                    .tables
                    .values()
                    .all(|table| table.queue.is_none() && table.pending.is_empty() && !table.in_flight);
                if queues_empty && state.active_flushes == 0 && (!require_admissions || state.active_admissions == 0) {
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    fn pressure_locked(&self, state: &ManagerState) -> BufferResult<BufferPressureSnapshot> {
        let capacity = self.capacity.snapshot()?;
        let table_ids: BTreeSet<_> = capacity.tables.keys().chain(state.tables.keys()).copied().collect();
        let mut tables = Vec::with_capacity(table_ids.len());
        for table_id in table_ids {
            let usage = capacity.tables.get(&table_id).copied().unwrap_or_default();
            tables.push(TablePressureSnapshot {
                table_id: Some(table_id),
                bytes: usage.bytes,
                rows: usage.rows,
                batches: usage.batches,
                limits: self.config.per_table_capacity,
                poisoned: state
                    .tables
                    .get(&table_id)
                    .is_some_and(|table| table.poisoned.is_some()),
            });
        }
        Ok(BufferPressureSnapshot {
            accepting: state.lifecycle == Lifecycle::Accepting && capacity.accepting,
            global: TablePressureSnapshot {
                table_id: None,
                bytes: capacity.global.bytes,
                rows: capacity.global.rows,
                batches: capacity.global.batches,
                limits: self.config.global_capacity,
                poisoned: false,
            },
            tables,
            active_flushes: state.active_flushes,
            active_admissions: state.active_admissions,
        })
    }

    fn spawn_age_worker(manager: &Arc<Self>) {
        let weak = Arc::downgrade(manager);
        let interval = manager.config.age_check_interval;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(manager) = weak.upgrade() else {
                    break;
                };
                let continue_running = manager.flush_aged();
                if continue_running.is_err() || !continue_running.unwrap_or(false) {
                    break;
                }
            }
        });
    }

    fn flush_aged(self: &Arc<Self>) -> BufferResult<bool> {
        let mut detached = false;
        let accepting = {
            let mut state = self.lock_state()?;
            if state.lifecycle == Lifecycle::Stopped {
                return Ok(false);
            }
            for table in state.tables.values_mut() {
                if table
                    .queue
                    .as_ref()
                    .is_some_and(|queue| queue.first_enqueued.elapsed() >= self.config.max_age)
                {
                    detach_queue(table, FlushReason::MaxAge);
                    detached = true;
                }
            }
            state.lifecycle == Lifecycle::Accepting
        };
        if detached {
            self.schedule_ready()?;
        }
        Ok(accepting)
    }

    fn lock_state(&self) -> BufferResult<std::sync::MutexGuard<'_, ManagerState>> {
        self.state.lock().map_err(|_| {
            BufferError::new(
                BufferErrorKind::Synchronization,
                "buffer manager state lock was poisoned",
            )
        })
    }
}

impl CapacityGate {
    fn new(limits: CapacityLimits, per_table_limits: CapacityLimits) -> Self {
        Self {
            limits,
            per_table_limits,
            state: Mutex::new(CapacityState {
                accepting: true,
                global: Usage::default(),
                tables: HashMap::new(),
            }),
            changed: Notify::new(),
        }
    }

    async fn acquire(self: &Arc<Self>, table_id: Uuid, usage: Usage) -> BufferResult<CapacityReservation> {
        if !self.limits.contains(usage.as_limits()) || !self.per_table_limits.contains(usage.as_limits()) {
            return Err(BufferError::new(
                BufferErrorKind::CapacityExceeded,
                "one delivery exceeds global or per-table buffer capacity",
            ));
        }
        loop {
            let notified = self.changed.notified();
            {
                let mut state = self.state.lock().map_err(|_| {
                    BufferError::new(BufferErrorKind::Synchronization, "capacity state lock was poisoned")
                })?;
                if !state.accepting {
                    return Err(BufferError::new(
                        BufferErrorKind::AdmissionClosed,
                        "buffer admission is closed",
                    ));
                }
                let table_usage = state.tables.get(&table_id).copied().unwrap_or_default();
                let next_global = state.global.checked_add(usage);
                let next_table = table_usage.checked_add(usage);
                if let (Some(next_global), Some(next_table)) = (next_global, next_table)
                    && self.limits.contains(next_global.as_limits())
                    && self.per_table_limits.contains(next_table.as_limits())
                {
                    state.global = next_global;
                    state.tables.insert(table_id, next_table);
                    return Ok(CapacityReservation {
                        gate: Arc::clone(self),
                        table_id,
                        usage,
                        released: false,
                    });
                }
            }
            notified.await;
        }
    }

    fn close(&self) -> BufferResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| BufferError::new(BufferErrorKind::Synchronization, "capacity state lock was poisoned"))?;
        state.accepting = false;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn snapshot(&self) -> BufferResult<CapacityState> {
        let state = self
            .state
            .lock()
            .map_err(|_| BufferError::new(BufferErrorKind::Synchronization, "capacity state lock was poisoned"))?;
        Ok(CapacityState {
            accepting: state.accepting,
            global: state.global,
            tables: state.tables.clone(),
        })
    }

    fn release(&self, table_id: Uuid, usage: Usage) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Some(global) = state.global.checked_sub(usage) {
            state.global = global;
        }
        if let Some(table) = state.tables.get(&table_id).copied()
            && let Some(remaining) = table.checked_sub(usage)
        {
            if remaining == Usage::default() {
                state.tables.remove(&table_id);
            } else {
                state.tables.insert(table_id, remaining);
            }
        }
        drop(state);
        self.changed.notify_waiters();
    }
}

impl Drop for CapacityReservation {
    fn drop(&mut self) {
        if !self.released {
            self.gate.release(self.table_id, self.usage);
            self.released = true;
        }
    }
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        let Ok(mut state) = manager.state.lock() else {
            return;
        };
        state.active_admissions = state.active_admissions.saturating_sub(1);
        drop(state);
        manager.idle.notify_waiters();
    }
}

fn validate_admission(admission: &Admission) -> BufferResult<(Usage, QueueContext)> {
    if admission.batches.is_empty() {
        return Err(BufferError::new(
            BufferErrorKind::InvalidConfiguration,
            "a buffered delivery must contain at least one record batch",
        ));
    }
    if admission.delivery.table() != admission.lease.table() {
        return Err(BufferError::new(
            BufferErrorKind::InvalidConfiguration,
            "validated delivery table does not match its publication lease",
        ));
    }
    let arrow_schema = admission.batches[0].schema();
    let mut usage = Usage::default();
    for batch in &admission.batches {
        if batch.schema_ref().as_ref() != arrow_schema.as_ref()
            || fingerprint_schema(batch.schema_ref().as_ref()) != admission.delivery.schema_fingerprint()
        {
            return Err(BufferError::new(
                BufferErrorKind::InvalidConfiguration,
                "record batch schema does not match the validated delivery",
            ));
        }
        let bytes = record_batch_logical_bytes(batch).map_err(|cause| {
            BufferError::new(
                BufferErrorKind::InvalidConfiguration,
                format!("could not measure Arrow batch memory: {cause:#}"),
            )
        })?;
        usage = usage
            .checked_add(Usage {
                bytes,
                rows: u64::try_from(batch.num_rows())
                    .map_err(|_| BufferError::new(BufferErrorKind::InvalidConfiguration, "row count exceeds u64"))?,
                batches: 1,
            })
            .ok_or_else(|| BufferError::new(BufferErrorKind::InvalidConfiguration, "delivery counters overflowed"))?;
    }
    if usage.rows != admission.delivery.rows().get()
        || usage.batches != admission.delivery.batches().get()
        || usage.bytes != admission.delivery.arrow_memory_bytes().get()
    {
        return Err(BufferError::new(
            BufferErrorKind::InvalidConfiguration,
            "validated delivery counts do not match supplied record batches",
        ));
    }
    let now = now_unix_millis()?;
    if !admission.lease.is_active_at(now) {
        return Err(BufferError::new(
            BufferErrorKind::TablePoisoned,
            "table lease is not active at admission",
        ));
    }
    Ok((
        usage,
        QueueContext {
            table_id: admission.delivery.table_id(),
            table: admission.delivery.table().clone(),
            schema_version: admission.delivery.schema_version(),
            schema_fingerprint: admission.delivery.schema_fingerprint(),
            arrow_schema,
            lease: admission.lease.clone(),
        },
    ))
}

fn detach_queue(table: &mut TableState, reason: FlushReason) {
    if let Some(queue) = table.queue.take()
        && !queue.entries.is_empty()
    {
        // Replay can regroup receipts differently. Each file and completion
        // must therefore retain one publication identity, not a flush identity.
        for entry in queue.entries {
            table.pending.push_back(FlushJob {
                context: queue.context.clone(),
                entries: vec![entry],
                reason,
            });
        }
    }
}

fn flush_reason(
    queue: &TableQueue,
    estimator: &AdaptiveFileSizeEstimator,
    config: &BufferConfig,
) -> Option<FlushReason> {
    if estimator.should_flush(queue.usage.bytes) {
        Some(FlushReason::AdaptiveCompressedSize)
    } else if queue.usage.bytes >= config.max_arrow_memory_per_flush {
        Some(FlushReason::ArrowMemory)
    } else if queue.usage.rows >= config.max_rows_per_flush {
        Some(FlushReason::Rows)
    } else if queue.usage.batches >= config.max_batches_per_flush {
        Some(FlushReason::Batches)
    } else {
        None
    }
}

fn same_fence(left: &TableLease, right: &TableLease) -> bool {
    left.lease_id() == right.lease_id()
        && left.holder_instance_id() == right.holder_instance_id()
        && left.fencing_token() == right.fencing_token()
}

pub(crate) fn receipt_deadline(lease: &TableLease, duration: Duration) -> BufferResult<UnixMillis> {
    let now = now_unix_millis()?;
    if !lease.is_active_at(now) {
        return Err(BufferError::new(
            BufferErrorKind::TablePoisoned,
            "table lease expired before receipt transition",
        ));
    }
    let extension = u64::try_from(duration.as_millis()).map_err(|_| {
        BufferError::new(
            BufferErrorKind::InvalidConfiguration,
            "receipt lease duration exceeds protocol range",
        )
    })?;
    let desired = now.get().saturating_add(extension);
    UnixMillis::new(desired.min(lease.expires_at_unix_ms().get()))
        .map_err(|cause| BufferError::new(BufferErrorKind::InvalidConfiguration, cause.to_string()))
}

pub(crate) fn now_unix_millis() -> BufferResult<UnixMillis> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| {
            BufferError::new(
                BufferErrorKind::Synchronization,
                "system clock is before the Unix epoch",
            )
        })?
        .as_millis();
    let millis = u64::try_from(millis).map_err(|_| {
        BufferError::new(
            BufferErrorKind::Synchronization,
            "system time exceeds the protocol range",
        )
    })?;
    UnixMillis::new(millis).map_err(|cause| BufferError::new(BufferErrorKind::Synchronization, cause.to_string()))
}

fn incomplete_job(
    job: &FlushJob,
    reason: &str,
    durable_publication: Option<DeltaPublicationResult>,
    poison: Option<String>,
) -> JobResult {
    JobResult {
        observation: None,
        incomplete: Some(incomplete(job, reason.to_owned(), durable_publication)),
        poison,
        outcome: DeliveryOutcome::Unknown,
    }
}

fn incomplete(job: &FlushJob, reason: String, durable_publication: Option<DeltaPublicationResult>) -> IncompleteWork {
    incomplete_from_entries_with_publication(
        &job.context.table,
        &job.entries,
        job.reason,
        reason,
        durable_publication,
    )
}

fn incomplete_from_entries(
    table: &TableIdentity,
    entries: &[QueuedDelivery],
    reason: FlushReason,
    detail: String,
) -> IncompleteWork {
    incomplete_from_entries_with_publication(table, entries, reason, detail, None)
}

fn incomplete_from_entries_with_publication(
    table: &TableIdentity,
    entries: &[QueuedDelivery],
    reason: FlushReason,
    detail: String,
    durable_publication: Option<DeltaPublicationResult>,
) -> IncompleteWork {
    IncompleteWork {
        table: table.clone(),
        receipt_ids: entries.iter().map(|entry| entry.receipt.receipt_id).collect(),
        flush_reason: reason,
        durable_publication,
        reason: detail,
    }
}

fn receipt_error(error: arrowcol_receipts::ReceiptError) -> BufferError {
    BufferError::new(BufferErrorKind::ReceiptStore, error.to_string())
}

/// Redacts credentials, URIs, absolute paths and private identifiers from sink diagnostics.
///
/// Preserves causal text, replaces sensitive values with `[REDACTED]`, removes
/// control characters and bounds the result to 1,024 UTF-8 bytes. Redaction runs
/// before truncation, so a partially truncated credential cannot escape.
#[must_use]
pub fn safe_error_message(message: &str) -> String {
    use std::borrow::Cow;
    use std::sync::LazyLock;

    use regex::Regex;

    // Match loadgen kusto::redact_text (34b899) URI/path/GUID/UPN/JWT canaries
    // and auth-automation's named-secret/Bearer replacement convention.
    static REDACTIONS: LazyLock<Result<Vec<(Regex, &'static str)>, regex::Error>> = LazyLock::new(|| {
        [
            (
                r#"(?i)\b(?:authorization|proxy-authorization)\s*[:=]\s*(?:"[^"]*"|'[^']*'|(?:bearer|basic)\s+[^\s,;]+|[^\s,;]+)"#,
                "[REDACTED]",
            ),
            (r"(?i)\bbearer\s+[a-z0-9._~+/\-=]+", "[REDACTED]"),
            (
                r#"(?i)["']?\b(?:password|pwd|account_?key|key|accountname|sharedaccesssignature|sharedaccesskey|access[_-]?token|refresh[_-]?token|client[_-]?secret|sig|se|sv|sp)["']?\s*[:=]\s*(?:"[^"]*"|'[^']*'|\{(?:[^}]|}})*\}|[^\s;,&"'<>]+)"#,
                "[REDACTED]",
            ),
            (
                r#"(?i)(?:"[a-z][a-z0-9+.-]*://[^"]*"|'[a-z][a-z0-9+.-]*://[^']*')"#,
                "[REDACTED]",
            ),
            (
                r#"(?i)\b[a-z][a-z0-9+.-]*://[^\s"'<>)\]}]+"#,
                "[REDACTED]",
            ),
            (
                r"\b[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]{8,}\b",
                "[REDACTED]",
            ),
            (
                r"(?i)\beyj[a-z0-9._-]{29,}",
                "[REDACTED]",
            ),
            (
                r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
                "[REDACTED]",
            ),
            (
                r#"[^\s"'`()\[\]{},;<>:@]+@[^\s"'`()\[\]{},;<>:@]+\.[^\s"'`()\[\]{},;<>:@]+"#,
                "[REDACTED]",
            ),
            (r#"["'](?:/|[a-zA-Z]:[\\/]|\\\\)[^"']*["']"#, "[REDACTED]"),
            (r#"\b[a-zA-Z]:[\\/][^\s"'<>),;\]}]+"#, "[REDACTED]"),
            (r#"\\\\[^\s"'<>),;\]}]+"#, "[REDACTED]"),
            (r#"(^|[\s"'(=,:])[/\\][^\s"'<>),;:\]}]+"#, " [REDACTED]"),
        ]
        .into_iter()
        .map(|(pattern, replacement)| Regex::new(pattern).map(|regex| (regex, replacement)))
        .collect()
    });
    let Ok(redactions) = REDACTIONS.as_ref() else {
        return "publication failed (diagnostic redaction unavailable)".to_owned();
    };
    let mut redacted = Cow::Borrowed(message);
    for (pattern, replacement) in redactions {
        redacted = Cow::Owned(pattern.replace_all(&redacted, *replacement).into_owned());
    }
    let mut safe = String::with_capacity(redacted.len().min(1_024));
    for character in redacted.chars() {
        if safe.len() >= 1_024 {
            break;
        }
        let character = if character.is_control() { ' ' } else { character };
        if safe.len() + character.len_utf8() > 1_024 {
            break;
        }
        safe.push(character);
    }
    if safe.is_empty() {
        "publication failed".to_owned()
    } else {
        safe
    }
}

#[cfg(test)]
mod tests {
    use super::safe_error_message;

    const CANARY_ERROR: &str = concat!(
        "transient publication failed: Int16 vs UInt8; ",
        "https://canary-user:canary-url-password@canary-host.invalid/canary-url-path?sig=canary-url-sig&se=canary-expiry; ",
        "sv=canary-sv&sp=canary-sp&sig=canary-sas; ",
        "Authorization: Bearer canary-auth-token; Bearer canary-bearer-token; ",
        "abfss://canary-container@canary-account.dfs.core.windows.net/canary-storage-path; ",
        "https://canary-blob.blob.core.windows.net/canary-blob-container/canary-blob-path; ",
        "private tenant 01234567-89ab-cdef-0123-456789abcdef; ",
        "Password=canary-password;AccountKey=canary-account-key;SharedAccessSignature=canary-connection-sas; ",
        "file:///canary-file/path; /canary-local/path; C:\\canary-windows\\path; \\\\canary-server\\share; ",
        "eyJhbGciOiJIUzI1NiJ9.eyJjYW5hcnkiOiJwcml2YXRlIn0.canaryJWTsignature"
    );

    #[test]
    fn runner_jsonl_spaced_quoted_secrets_and_basic_auth_are_redacted() -> anyhow::Result<()> {
        let cause = "unsupported UInt8 statistic https://private.example/path?sig=credential C:\\Users\\private\\table \
                 /private/table 12345678-1234-1234-1234-123456789abc Password=opaque-secret \
                 Authorization: Bearer opaque-token AccountKey = spaced-secret key = second-secret \
                 abfss://private@account.dfs.core.windows.net/root Password=\"three word credential\" \
                 Password = \"combined private credential\" AccountKey : 'joined private words' \
                 Authorization:Basic opaque-basic-token Password = \"https://private.example quoted url-secret\"";
        let safe = safe_error_message(cause);
        let mut receipt_jsonl = serde_json::to_string(&serde_json::json!({ "error_message": safe }))?;
        receipt_jsonl.push('\n');
        let mut admission_jsonl = serde_json::to_string(&serde_json::json!({ "error": safe }))?;
        admission_jsonl.push('\n');
        for jsonl in [&receipt_jsonl, &admission_jsonl] {
            assert!(jsonl.contains("unsupported UInt8 statistic"));
            for canary in [
                "private.example",
                "credential",
                "C:\\",
                "/private",
                "opaque-secret",
                "opaque-token",
                "spaced-secret",
                "second-secret",
                "private@account",
                "three word",
                "combined",
                "joined",
                "private words",
                "opaque-basic-token",
                "url-secret",
                "quoted",
            ] {
                assert!(!jsonl.contains(canary), "leaked runner JSONL canary: {canary}");
            }
        }
        for message in [
            r#"schema Int16 vs UInt8: password = "two words here""#,
            "schema Int16 vs UInt8: client_secret : 'a b c'",
            "schema Int16 vs UInt8: Authorization: Basic opaque-basic-token",
            r#"schema Int16 vs UInt8: "https://private.example/path?sig=credential quoted url-secret""#,
            "schema Int16 vs UInt8: 'https://private.example/path?sig=credential quoted url-secret'",
        ] {
            let safe = safe_error_message(message);
            assert!(safe.contains("schema Int16 vs UInt8"));
            for canary in [
                "two words",
                "a b c",
                "opaque-basic-token",
                "private.example",
                "credential",
                "quoted",
                "url-secret",
            ] {
                assert!(!safe.contains(canary), "leaked quoted-secret canary: {canary}");
            }
        }
        Ok(())
    }

    #[test]
    fn direct_warn_and_receipt_diagnostics_redact_canaries_but_keep_schema_cause() -> anyhow::Result<()> {
        let error = crate::SinkError::retryable(CANARY_ERROR);
        let message = safe_error_message(error.message());
        let receipt_error = arrowcol_protocol::ErrorStatus::new(error.code(), message.clone())?;
        assert_eq!(receipt_error.message(), message);
        assert!(!message.contains("canary"), "{message}");
        assert!(!message.contains("01234567-89ab-cdef-0123-456789abcdef"));
        assert!(!message.contains("eyJ"));
        assert!(message.contains("transient publication failed: Int16 vs UInt8"));
        assert!(message.contains("[REDACTED]"));
        Ok(())
    }

    #[test]
    fn sensitive_values_are_redacted_before_byte_truncation() {
        for value in [
            "Authorization=Basic canary-basic-token",
            r#"password="canary password with spaces""#,
            "Pwd={canary password;with semicolon}",
            r#"{"client_secret":"canary-secret","access_token":"canary-token"}"#,
            r#""/canary folder/private file""#,
            r#""C:\canary folder\private file""#,
        ] {
            assert!(!safe_error_message(value).contains("canary"));
        }
        let message = format!(
            "{} https://canary-user:canary-password@canary-host.invalid",
            "x".repeat(1_000)
        );
        assert!(!safe_error_message(&message).contains("canary"));
        assert_eq!(
            safe_error_message("schema Int16/UInt8: permanent mismatch"),
            "schema Int16/UInt8: permanent mismatch"
        );
    }

    #[test]
    fn native_client_canaries_and_diagnostic_contract_stay_aligned() {
        let message = concat!(
            "Partial query failure: abfss://private@acct.dfs.core.windows.net/x?sig=CANARYSIG ",
            "on /subscriptions/11111111-2222-3333-4444-555555555555/resourceGroups/rg identity ",
            "99999999-aaaa-bbbb-cccc-dddddddddddd ",
            "bearer eyJhbGciOiJIUzI1NiJ9.privateCanaryJwtPayload.privateCanarySignature ",
            "/delta/private/x C:\\privateCanaryWindows\\x;Password=privateCanaryPassword; ",
            r#"{"client_secret":"privateCanarySecret"} authorization: Bearer privateCanaryOpaqueToken "#,
            "privateCanaryUser@example.invalid: schema Int16 vs UInt8"
        );
        let safe = safe_error_message(message);
        for canary in [
            "CANARYSIG",
            "acct.dfs",
            "11111111-2222",
            "99999999-aaaa",
            "eyJhbGciOi",
            "/delta/private",
            "privateCanary",
        ] {
            assert!(!safe.contains(canary), "{canary} leaked: {safe}");
        }
        assert!(safe.contains("Partial query failure"));
        assert!(safe.contains("schema Int16 vs UInt8"));
        assert_eq!(
            safe_error_message("TableVersion 4 at 2026-10-27T03:41:46Z"),
            "TableVersion 4 at 2026-10-27T03:41:46Z"
        );
        let bounded = safe_error_message(&format!("{}\n", "界".repeat(1_100)));
        assert!(bounded.chars().count() <= 1_024);
        assert!(bounded.len() <= 1_024);
        assert!(!bounded.chars().any(char::is_control));
    }

    #[test]
    fn sink_diagnostics_are_bounded_control_free_and_utf8_safe() {
        assert_eq!(safe_error_message(""), "publication failed");
        assert_eq!(safe_error_message("outer:\ninner\tcause\u{1b}"), "outer: inner cause ");
        assert_eq!(safe_error_message(&"é".repeat(600)), "é".repeat(512));
        assert_eq!(safe_error_message(&"x".repeat(1_025)).len(), 1_024);
        assert_eq!(safe_error_message("safe error chain"), "safe error chain");
    }
}
