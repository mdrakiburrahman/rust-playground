//! Quiver-backed durable admission stage for direct collectors.
//!
//! Every accepted delivery is persisted to Quiver (an Arrow write-ahead log
//! plus immutable segment files) before the collector acknowledges it. A
//! consumer then reads the persisted bundles back from disk, admits them to
//! the bounded [`BufferManager`], and acknowledges each bundle only after its
//! receipt is published to Delta. After a crash, the restarted collector
//! replays unacknowledged bundles and adopts their receipts.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use arrow::array::RecordBatch;
use arrowcol_catalog::SchemaVersion;
use arrowcol_protocol::{
    CollectorInstanceId, ErrorCode, ErrorStatus, FencingToken, LeaseId, ReceiptId, ReceiptState, SchemaFingerprint,
    TableIdentity, TableLease, UnixMillis,
};
use arrowcol_receipts::{
    AdoptionCandidate, FailureDisposition, ReceiptAdoption, ReceiptFailure, ReceiptRecord, ReceiptStore,
    ReceiptVersion, ValidatedDelivery,
};
use async_trait::async_trait;
use otel_arrow_dfe_quiver::config::RetentionPolicy;
use otel_arrow_dfe_quiver::record_bundle::{BundleDescriptor, PayloadRef, RecordBundle, SlotDescriptor, SlotId};
use otel_arrow_dfe_quiver::subscriber::{BundleHandle, BundleRef, RegistryCallback, SubscriberId};
use otel_arrow_dfe_quiver::{
    CancellationToken, DiskBudget, QuiverConfig, QuiverEngine, QuiverEngineBuilder, SegmentStore,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::manager::{now_unix_millis, receipt_deadline};
use crate::{
    Admission, BufferError, BufferErrorKind, BufferManager, BufferResult, DeliveryCompletion, DeliveryOutcome,
    TrackedState,
};

/// Quiver slot holding the single record batch of a persisted delivery.
pub const DELIVERY_SLOT: SlotId = SlotId::new(0);
/// Format identifier of a [`DeliveryManifest`].
pub const DELIVERY_MANIFEST_FORMAT: &str = "arrowcol.direct";
/// Current [`DeliveryManifest`] version.
pub const DELIVERY_MANIFEST_VERSION: u16 = 1;
/// Quiver subscriber that publishes persisted deliveries to Delta.
pub const DELTA_SUBSCRIBER: &str = "arrowcol-delta";
/// Upper bound on one wait for the next finalized bundle.
const POLL_TIMEOUT: Duration = Duration::from_millis(250);
/// Interval between Quiver maintenance passes (progress flush and cleanup).
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);
/// Pause between persistence attempts while the disk budget is exhausted.
const CAPACITY_RETRY: Duration = Duration::from_millis(20);

type QuiverHandle = BundleHandle<RegistryCallback<SegmentStore>>;

/// Settings for the Quiver durable stage.
#[derive(Clone, Debug)]
pub struct QuiverStageConfig {
    /// Directory holding the write-ahead log, segments, and progress files.
    pub directory: PathBuf,
    /// Hard cap on disk space used by the stage, in bytes.
    pub capacity_bytes: u64,
    /// Maximum time between WAL fsyncs. Zero fsyncs before every acknowledgement.
    pub wal_flush_interval: Duration,
    /// Maximum time a segment stays open before it is finalized and published.
    pub segment_max_open: Duration,
    /// Maximum bundles admitted from disk but not yet resolved.
    pub max_in_flight: usize,
    /// First delay before retrying a bundle whose outcome was unknown.
    pub retry_initial: Duration,
    /// Maximum delay between retries of one bundle.
    pub retry_max: Duration,
}

impl QuiverStageConfig {
    /// Builds the Quiver engine configuration for this stage.
    #[must_use]
    pub fn quiver_config(&self) -> QuiverConfig {
        let mut config = QuiverConfig::default().with_data_dir(&self.directory);
        config.wal.flush_interval = self.wal_flush_interval;
        config.segment.max_open_duration = self.segment_max_open;
        config
    }

    /// Validates the settings against Quiver's own constraints.
    ///
    /// # Errors
    ///
    /// Returns [`BufferErrorKind::InvalidConfiguration`] when the capacity is
    /// below Quiver's minimum for the configured WAL and segment sizes, or a
    /// limit is zero.
    pub fn validate(&self) -> BufferResult<()> {
        if self.max_in_flight == 0 || self.retry_initial.is_zero() || self.retry_max < self.retry_initial {
            return Err(BufferError::new(
                BufferErrorKind::InvalidConfiguration,
                "Quiver stage needs a positive in-flight limit and retry_max >= retry_initial > 0",
            ));
        }
        if self.segment_max_open.is_zero() {
            return Err(BufferError::new(
                BufferErrorKind::InvalidConfiguration,
                "Quiver segment_max_open must be positive",
            ));
        }
        let config = self.quiver_config();
        config
            .validate()
            .map_err(|cause| BufferError::new(BufferErrorKind::InvalidConfiguration, cause.to_string()))?;
        DiskBudget::for_config(self.capacity_bytes, &config, RetentionPolicy::Backpressure)
            .map(|_| ())
            .map_err(|cause| BufferError::new(BufferErrorKind::InvalidConfiguration, cause.to_string()))
    }
}

/// Supplies this collector's current publication lease for a table.
#[async_trait]
pub trait StageLeases: Send + Sync {
    /// Returns the collector's active lease for the table, acquiring it when
    /// routing assigns the table to this collector.
    ///
    /// # Errors
    ///
    /// Returns a safe message when the collector does not (yet) own the table.
    async fn current_lease(&self, table_id: Uuid, table: &TableIdentity) -> Result<TableLease, String>;
}

/// One accepted delivery to persist.
pub struct PersistRequest<'a> {
    /// Fresh `accepted` receipt.
    pub receipt: ReceiptVersion,
    /// Receipt owner.
    pub owner: CollectorInstanceId,
    /// Validated delivery metadata.
    pub delivery: &'a ValidatedDelivery,
    /// Table lease the receipt is bound to.
    pub lease: &'a TableLease,
    /// The single record batch of the delivery.
    pub batch: RecordBatch,
}

/// Point-in-time view of the stage for pressure documents and metrics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QuiverStageSnapshot {
    /// Bytes currently used on disk (WAL and segments).
    pub disk_used_bytes: u64,
    /// Configured disk capacity.
    pub disk_capacity_bytes: u64,
    /// Persisted bundles not yet published, rejected, or dropped.
    pub backlog_bundles: u64,
    /// Bundles persisted since start.
    pub persisted_bundles: u64,
    /// Logical Arrow bytes persisted since start.
    pub persisted_bytes: u64,
    /// Bundles whose receipt was published (or committed) since start.
    pub published_bundles: u64,
    /// Bundles dropped because their receipt reached a terminal failure.
    pub rejected_bundles: u64,
    /// Bundle deliveries deferred for a later retry.
    pub deferred_bundles: u64,
    /// Distinct bundles persisted by a predecessor process and replayed by this one.
    pub recovered_bundles: u64,
    /// Receipts adopted from a predecessor or rebound to a newer lease.
    pub adopted_receipts: u64,
    /// Persistence attempts that waited for disk capacity.
    pub backpressure_waits: u64,
}

#[derive(Default)]
struct Counters {
    persisted_bundles: AtomicU64,
    persisted_bytes: AtomicU64,
    published_bundles: AtomicU64,
    rejected_bundles: AtomicU64,
    deferred_bundles: AtomicU64,
    recovered_bundles: AtomicU64,
    adopted_receipts: AtomicU64,
    backpressure_waits: AtomicU64,
}

/// Manifest persisted as Quiver bundle metadata with every delivery.
///
/// It carries the receipt, lease, and schema identity a restarted collector
/// needs to publish the delivery from disk. It is serialized as JSON.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryManifest {
    /// Always [`DELIVERY_MANIFEST_FORMAT`].
    pub format: String,
    /// Always [`DELIVERY_MANIFEST_VERSION`].
    pub version: u16,
    /// Server receipt of the delivery.
    pub receipt_id: ReceiptId,
    /// Original UTC admission time. Legacy manifests recover it from the
    /// receipt's immutable creation time, never from the replay clock.
    #[serde(default)]
    pub ingested_at_ms: Option<UnixMillis>,
    /// Receipt revision when the delivery was persisted.
    pub receipt_revision: u64,
    /// Collector that owned the receipt.
    pub owner_instance_id: CollectorInstanceId,
    /// Table lease the receipt was bound to.
    pub lease_id: LeaseId,
    /// Fencing epoch of that lease.
    pub fencing_token: u64,
    /// Catalog table identifier.
    pub table_id: Uuid,
    /// Fully qualified table.
    pub table: TableIdentity,
    /// Registered schema version.
    pub schema_version: SchemaVersion,
    /// Fingerprint of the registered Arrow schema.
    pub schema_fingerprint: SchemaFingerprint,
    /// Rows in the delivery.
    pub rows: u64,
    /// Record batches in the delivery; always 1 for a persisted delivery.
    pub batches: u64,
    /// Arrow memory of the delivery's batch.
    pub arrow_memory_bytes: u64,
}

impl DeliveryManifest {
    /// Decodes and checks a manifest read from Quiver bundle metadata.
    ///
    /// # Errors
    ///
    /// Rejects malformed JSON and other manifest formats or versions.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let manifest: Self = serde_json::from_slice(bytes).map_err(|error| format!("invalid manifest: {error}"))?;
        if manifest.format != DELIVERY_MANIFEST_FORMAT || manifest.version != DELIVERY_MANIFEST_VERSION {
            return Err(format!(
                "unsupported manifest format {}@{}",
                manifest.format, manifest.version
            ));
        }
        Ok(manifest)
    }

    /// Serializes the manifest as Quiver bundle metadata.
    ///
    /// # Errors
    ///
    /// Returns a serialization failure.
    pub fn encode(&self) -> BufferResult<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|error| BufferError::new(BufferErrorKind::InvalidConfiguration, error.to_string()))
    }

    fn delivery(&self) -> BufferResult<ValidatedDelivery> {
        ValidatedDelivery::recovered(
            self.table_id,
            self.table.clone(),
            self.schema_version,
            self.schema_fingerprint,
            self.rows,
            self.batches,
            self.arrow_memory_bytes,
        )
        .map_err(|error| BufferError::new(BufferErrorKind::InvalidConfiguration, error.to_string()))
    }

    fn fencing_token(&self) -> Option<FencingToken> {
        FencingToken::new(self.fencing_token).ok()
    }
}

/// One persisted delivery as a Quiver record bundle: its single record batch
/// in [`DELIVERY_SLOT`] plus a [`DeliveryManifest`] as bundle metadata.
pub struct DeliveryBundle {
    descriptor: BundleDescriptor,
    batch: RecordBatch,
    fingerprint: [u8; 32],
    manifest: Vec<u8>,
    rows: u64,
    bytes: u64,
    ingestion_time: SystemTime,
}

impl DeliveryBundle {
    /// Builds the bundle the Direct collector persists for one delivery.
    ///
    /// # Errors
    ///
    /// Rejects a manifest that does not describe exactly this one batch.
    pub fn new(manifest: &DeliveryManifest, batch: RecordBatch) -> BufferResult<Self> {
        if manifest.batches != 1 || u64::try_from(batch.num_rows()).ok() != Some(manifest.rows) {
            return Err(BufferError::new(
                BufferErrorKind::InvalidConfiguration,
                "a persisted delivery must hold exactly one record batch with the manifest's rows",
            ));
        }
        Ok(Self {
            descriptor: BundleDescriptor::new(vec![SlotDescriptor::new(DELIVERY_SLOT, "ArrowcolDirect")]),
            fingerprint: *manifest.schema_fingerprint.as_bytes(),
            batch,
            manifest: manifest.encode()?,
            rows: manifest.rows,
            bytes: manifest.arrow_memory_bytes,
            ingestion_time: SystemTime::now(),
        })
    }
}

impl RecordBundle for DeliveryBundle {
    fn descriptor(&self) -> &BundleDescriptor {
        &self.descriptor
    }

    fn ingestion_time(&self) -> SystemTime {
        self.ingestion_time
    }

    fn payload(&self, slot: SlotId) -> Option<PayloadRef<'_>> {
        (slot == DELIVERY_SLOT).then_some(PayloadRef {
            schema_fingerprint: self.fingerprint,
            batch: &self.batch,
        })
    }

    fn item_count(&self) -> u64 {
        self.rows
    }

    fn byte_count(&self) -> Option<u64> {
        Some(self.bytes)
    }

    fn metadata(&self) -> Option<&[u8]> {
        Some(&self.manifest)
    }
}

/// How one delivery attempt of a persisted bundle ends.
enum Resolution {
    /// The receipt is published; acknowledge the bundle.
    Ack,
    /// The receipt reached a terminal failure; drop the bundle.
    Reject,
    /// The outcome is not known yet; keep the bundle and retry later.
    Retry(String),
}

/// Quiver-backed durable admission stage.
pub struct QuiverStage {
    engine: Arc<QuiverEngine>,
    subscriber: SubscriberId,
    buffer: Arc<BufferManager>,
    receipts: Arc<dyn ReceiptStore>,
    leases: Arc<dyn StageLeases>,
    owner: CollectorInstanceId,
    receipt_lease_duration: Duration,
    config: QuiverStageConfig,
    in_flight: Arc<Semaphore>,
    /// One receipt adoption transaction at a time: each one locks the table
    /// lease and owner rows, so overlapping adoptions only add SQL lock cycles.
    adoption: AsyncMutex<()>,
    /// Ingest and finalize calls in progress. While a segment is finalized its
    /// bundles are briefly in neither the open segment nor the subscriber's
    /// pending progress, so the backlog must not read as empty then.
    storage_operations: AtomicU64,
    /// Bundles currently being delivered (claimed and not yet resolved or deferred).
    processing: AtomicU64,
    counters: Counters,
    cancel: CancellationToken,
    tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl QuiverStage {
    /// Opens (and crash-recovers) the Quiver engine and registers the Delta
    /// subscriber. Call [`Self::start`] once the collector owns its receipts.
    ///
    /// # Errors
    ///
    /// Returns invalid configuration or durable-storage errors.
    pub async fn open(
        config: QuiverStageConfig,
        buffer: Arc<BufferManager>,
        receipts: Arc<dyn ReceiptStore>,
        leases: Arc<dyn StageLeases>,
        owner: CollectorInstanceId,
        receipt_lease_duration: Duration,
    ) -> BufferResult<Arc<Self>> {
        config.validate()?;
        let quiver_config = config.quiver_config();
        let budget = DiskBudget::for_config(config.capacity_bytes, &quiver_config, RetentionPolicy::Backpressure)
            .map_err(|cause| BufferError::new(BufferErrorKind::InvalidConfiguration, cause.to_string()))?;
        let engine = QuiverEngineBuilder::new(quiver_config)
            .with_budget(Arc::new(budget))
            .with_wal_item_counter(Arc::new(|bundle: &dyn RecordBundle| {
                bundle
                    .metadata()
                    .and_then(|bytes| DeliveryManifest::decode(bytes).ok())
                    .map(|manifest| manifest.rows)
            }))
            .with_wal_byte_counter(Arc::new(|bundle: &dyn RecordBundle| {
                bundle
                    .metadata()
                    .and_then(|bytes| DeliveryManifest::decode(bytes).ok())
                    .map(|manifest| manifest.arrow_memory_bytes)
            }))
            .build()
            .await
            .map_err(durable_error)?;
        let subscriber = SubscriberId::new(DELTA_SUBSCRIBER).map_err(durable_error)?;
        engine.register_subscriber(subscriber.clone()).map_err(durable_error)?;
        engine.activate_subscriber(&subscriber).map_err(durable_error)?;
        let stage = Arc::new(Self {
            engine,
            subscriber,
            buffer,
            receipts,
            leases,
            owner,
            receipt_lease_duration,
            in_flight: Arc::new(Semaphore::new(config.max_in_flight)),
            adoption: AsyncMutex::new(()),
            storage_operations: AtomicU64::new(0),
            processing: AtomicU64::new(0),
            config,
            counters: Counters::default(),
            cancel: CancellationToken::new(),
            tasks: std::sync::Mutex::new(Vec::new()),
        });
        info!(
            directory = %stage.config.directory.display(),
            backlog_bundles = stage.backlog_bundles(),
            "opened Quiver durable stage"
        );
        Ok(stage)
    }

    /// Starts the consumer that publishes persisted bundles and the
    /// maintenance loop that finalizes segments and reclaims disk space.
    pub fn start(self: &Arc<Self>) {
        let consumer = tokio::spawn(Arc::clone(self).consume());
        let maintenance = tokio::spawn(Arc::clone(self).maintain());
        if let Ok(mut tasks) = self.tasks.lock() {
            tasks.push(consumer);
            tasks.push(maintenance);
        }
    }

    /// Persists one accepted delivery, waiting up to `timeout` for disk
    /// capacity. Returns once the WAL append (and, when configured, its fsync)
    /// completed.
    ///
    /// # Errors
    ///
    /// Returns [`BufferErrorKind::BackpressureTimeout`] when disk capacity did
    /// not free up in time and [`BufferErrorKind::DurableStorage`] for other
    /// storage failures.
    pub async fn persist(&self, request: PersistRequest<'_>, timeout: Duration) -> BufferResult<()> {
        let bundle = direct_bundle(request)?;
        let started = Instant::now();
        loop {
            let result = {
                let _operation = StorageOperation::start(&self.storage_operations);
                self.engine.ingest(&bundle).await
            };
            match result {
                Ok(()) => {
                    self.counters.persisted_bundles.fetch_add(1, Ordering::Relaxed);
                    self.counters.persisted_bytes.fetch_add(bundle.bytes, Ordering::Relaxed);
                    return Ok(());
                }
                Err(error) if error.is_at_capacity() => {
                    self.counters.backpressure_waits.fetch_add(1, Ordering::Relaxed);
                    if started.elapsed() >= timeout {
                        return Err(BufferError::new(
                            BufferErrorKind::BackpressureTimeout,
                            "timed out waiting for durable storage capacity",
                        ));
                    }
                    tokio::time::sleep(CAPACITY_RETRY).await;
                }
                Err(error) => return Err(durable_error(error)),
            }
        }
    }

    /// Waits until every persisted bundle is resolved or `timeout` elapses.
    /// Returns whether the stage drained completely.
    pub async fn drain(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut empty_checks = 0_u8;
        loop {
            self.finalize_open_segment().await;
            let idle = self.processing.load(Ordering::Acquire) == 0;
            if idle && self.backlog_bundles() == 0 {
                empty_checks = empty_checks.saturating_add(1);
                // Require a stable reading so a concurrent finalization or a
                // bundle between claim and admission cannot end the drain early.
                if empty_checks >= 2 {
                    return true;
                }
            } else {
                empty_checks = 0;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn finalize_open_segment(&self) {
        if self.engine.open_segment_bundle_summaries().is_empty() {
            return;
        }
        let _operation = StorageOperation::start(&self.storage_operations);
        if let Err(error) = self.engine.flush().await {
            warn!(%error, "could not finalize the open Quiver segment");
        }
    }

    /// Stops the consumer and maintenance loops and finalizes the open
    /// segment. Unresolved bundles stay on disk for the next start.
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        let tasks = self
            .tasks
            .lock()
            .map(|mut tasks| std::mem::take(&mut *tasks))
            .unwrap_or_default();
        for task in tasks {
            let _ = task.await;
        }
        if let Err(error) = self.engine.shutdown().await {
            warn!(%error, "Quiver shutdown left data recoverable only through WAL replay");
        }
        if let Err(error) = self.engine.maintain().await {
            warn!(%error, "final Quiver maintenance failed");
        }
    }

    /// Returns the current disk usage, backlog, and counters.
    #[must_use]
    pub fn snapshot(&self) -> QuiverStageSnapshot {
        let budget = self.engine.budget();
        QuiverStageSnapshot {
            disk_used_bytes: budget.used(),
            disk_capacity_bytes: budget.hard_cap(),
            backlog_bundles: self.backlog_bundles(),
            persisted_bundles: self.counters.persisted_bundles.load(Ordering::Relaxed),
            persisted_bytes: self.counters.persisted_bytes.load(Ordering::Relaxed),
            published_bundles: self.counters.published_bundles.load(Ordering::Relaxed),
            rejected_bundles: self.counters.rejected_bundles.load(Ordering::Relaxed),
            deferred_bundles: self.counters.deferred_bundles.load(Ordering::Relaxed),
            recovered_bundles: self.counters.recovered_bundles.load(Ordering::Relaxed),
            adopted_receipts: self.counters.adopted_receipts.load(Ordering::Relaxed),
            backpressure_waits: self.counters.backpressure_waits.load(Ordering::Relaxed),
        }
    }

    fn backlog_bundles(&self) -> u64 {
        let finalized = self
            .engine
            .registry()
            .pending_segment_progress(&self.subscriber)
            .map(|segments| {
                segments
                    .values()
                    .map(|progress| u64::from(progress.bundle_count().saturating_sub(progress.resolved_count())))
                    .sum::<u64>()
            })
            .unwrap_or(0);
        let open = u64::try_from(self.engine.open_segment_bundle_summaries().len()).unwrap_or(u64::MAX);
        let backlog = finalized.saturating_add(open);
        if backlog == 0 && self.storage_operations.load(Ordering::Acquire) > 0 {
            // A bundle is being ingested or finalized and is not visible yet.
            return 1;
        }
        backlog
    }

    async fn consume(self: Arc<Self>) {
        loop {
            let permit = tokio::select! {
                () = self.cancel.cancelled() => return,
                permit = Arc::clone(&self.in_flight).acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => return,
                },
            };
            match self
                .engine
                .next_bundle(&self.subscriber, Some(POLL_TIMEOUT), Some(&self.cancel))
                .await
            {
                Ok(Some(handle)) => {
                    let stage = Arc::clone(&self);
                    tokio::spawn(async move {
                        stage.process(handle, 0).await;
                        drop(permit);
                    });
                }
                Ok(None) => drop(permit),
                Err(error) if error.is_cancelled() => return,
                Err(error) => {
                    drop(permit);
                    warn!(%error, "could not read the next persisted bundle");
                    tokio::select! {
                        () = self.cancel.cancelled() => return,
                        () = tokio::time::sleep(self.config.retry_initial) => {}
                    }
                }
            }
        }
    }

    async fn maintain(self: Arc<Self>) {
        let mut finalize = tokio::time::interval(self.config.segment_max_open);
        finalize.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_maintenance = Instant::now();
        loop {
            tokio::select! {
                () = self.cancel.cancelled() => return,
                _ = finalize.tick() => {}
            }
            // Ingest only finalizes on size or age when new data arrives, so
            // finalize here to publish idle tails promptly.
            self.finalize_open_segment().await;
            if last_maintenance.elapsed() >= MAINTENANCE_INTERVAL {
                last_maintenance = Instant::now();
                if let Err(error) = self.engine.maintain().await {
                    warn!(%error, "Quiver maintenance failed");
                }
            }
        }
    }

    /// Delivers one persisted bundle and resolves it in Quiver; bundles whose
    /// outcome is unknown are deferred and retried with backoff.
    async fn process(self: Arc<Self>, handle: QuiverHandle, attempt: u32) {
        let resolution = {
            let _processing = StorageOperation::start(&self.processing);
            self.deliver(&handle, attempt).await
        };
        if let Some(bundle_ref) = self.resolve_handle(handle, resolution, attempt) {
            let stage = Arc::clone(&self);
            tokio::spawn(stage.retry(bundle_ref, attempt.saturating_add(1)));
        }
    }

    /// Re-claims a deferred bundle after a backoff until it is resolved or the
    /// stage stops.
    async fn retry(self: Arc<Self>, mut bundle_ref: BundleRef, mut attempt: u32) {
        loop {
            let delay = self.retry_delay(attempt.saturating_sub(1));
            tokio::select! {
                () = self.cancel.cancelled() => return,
                () = tokio::time::sleep(delay) => {}
            }
            let handle = match self.engine.claim_bundle(&self.subscriber, bundle_ref) {
                Ok(handle) => handle,
                Err(error) => {
                    debug!(%error, "deferred bundle is no longer claimable");
                    return;
                }
            };
            let resolution = {
                let _processing = StorageOperation::start(&self.processing);
                self.deliver(&handle, attempt).await
            };
            match self.resolve_handle(handle, resolution, attempt) {
                Some(next) => {
                    bundle_ref = next;
                    attempt = attempt.saturating_add(1);
                }
                None => return,
            }
        }
    }

    /// Applies a resolution to the bundle. Returns the bundle reference when
    /// the bundle was deferred for another attempt.
    fn resolve_handle(&self, handle: QuiverHandle, resolution: Resolution, attempt: u32) -> Option<BundleRef> {
        match resolution {
            Resolution::Ack => {
                self.counters.published_bundles.fetch_add(1, Ordering::Relaxed);
                handle.ack();
                None
            }
            Resolution::Reject => {
                self.counters.rejected_bundles.fetch_add(1, Ordering::Relaxed);
                handle.reject();
                None
            }
            Resolution::Retry(reason) => {
                self.counters.deferred_bundles.fetch_add(1, Ordering::Relaxed);
                debug!(%reason, attempt, "deferring persisted bundle");
                Some(handle.defer())
            }
        }
    }

    fn retry_delay(&self, attempt: u32) -> Duration {
        let factor = 1_u32 << attempt.min(6);
        self.config
            .retry_initial
            .saturating_mul(factor)
            .min(self.config.retry_max)
    }

    async fn deliver(&self, handle: &QuiverHandle, attempt: u32) -> Resolution {
        let bundle = handle.data();
        let manifest = match bundle.metadata().map(DeliveryManifest::decode) {
            Some(Ok(manifest)) => manifest,
            Some(Err(error)) => {
                error!(%error, "dropping persisted bundle with an unreadable manifest");
                return Resolution::Reject;
            }
            None => {
                error!("dropping persisted bundle without a manifest");
                return Resolution::Reject;
            }
        };
        let Some(batch) = bundle.payload(DELIVERY_SLOT).cloned() else {
            error!(receipt_id = %manifest.receipt_id, "dropping persisted bundle without its record batch");
            return Resolution::Reject;
        };
        let delivery = match manifest.delivery() {
            Ok(delivery) => delivery,
            Err(error) => {
                error!(receipt_id = %manifest.receipt_id, %error, "dropping persisted bundle with an invalid delivery");
                return Resolution::Reject;
            }
        };
        let persisted_here = manifest.owner_instance_id == self.owner && manifest.receipt_revision == 0;
        if attempt == 0 && persisted_here {
            match self.fresh(&manifest, &delivery, batch.clone()).await {
                Ok(completion) => return resolve(completion.outcome().await),
                Err(reason) => debug!(receipt_id = %manifest.receipt_id, %reason, "reconciling persisted bundle"),
            }
        } else if attempt == 0 {
            // First delivery of a bundle a predecessor persisted; retries are
            // counted as deferrals, not as further recoveries.
            self.counters.recovered_bundles.fetch_add(1, Ordering::Relaxed);
        }
        match self.reconcile(&manifest, delivery, batch).await {
            Ok(completion) => resolve(completion.outcome().await),
            Err(resolution) => resolution,
        }
    }

    /// Fast path: the bundle was persisted by this process and its receipt is
    /// still `accepted` on the lease it was created with.
    async fn fresh(
        &self,
        manifest: &DeliveryManifest,
        delivery: &ValidatedDelivery,
        batch: RecordBatch,
    ) -> Result<DeliveryCompletion, String> {
        let lease = self.leases.current_lease(manifest.table_id, &manifest.table).await?;
        let ingested_at_ms = manifest
            .ingested_at_ms
            .ok_or("legacy manifest requires receipt timestamp")?;
        if lease.lease_id() != manifest.lease_id || Some(lease.fencing_token()) != manifest.fencing_token() {
            return Err("table lease changed after persistence".to_owned());
        }
        let admission = Admission {
            receipt: ReceiptVersion::new(manifest.receipt_id, manifest.receipt_revision),
            delivery: delivery.clone(),
            lease,
            batches: vec![batch],
        };
        self.buffer
            .admit_tracked_at(admission, TrackedState::Accepted, ingested_at_ms)
            .await
            .map(|(_, completion)| completion)
            .map_err(|error| error.message().to_owned())
    }

    /// Recovery path: reads the durable receipt, adopts it when it belongs to
    /// a predecessor or an older lease, and admits it in its current state.
    async fn reconcile(
        &self,
        manifest: &DeliveryManifest,
        delivery: ValidatedDelivery,
        batch: RecordBatch,
    ) -> Result<DeliveryCompletion, Resolution> {
        let record = match self.receipts.receipt(manifest.receipt_id).await {
            Ok(Some(record)) => record,
            Ok(None) => {
                warn!(receipt_id = %manifest.receipt_id, "persisted bundle has no receipt; dropping it");
                return Err(Resolution::Reject);
            }
            Err(error) => return Err(Resolution::Retry(format!("read receipt: {error}"))),
        };
        let state = match record.state {
            ReceiptState::Published => return Err(Resolution::Ack),
            ReceiptState::Failed | ReceiptState::PublicationUnknown => return Err(Resolution::Reject),
            ReceiptState::Accepted => TrackedState::Accepted,
            ReceiptState::Buffered => TrackedState::Buffered,
            ReceiptState::Publishing => TrackedState::Flushing,
        };
        let ingested_at_ms = manifest.ingested_at_ms.unwrap_or(record.created_at_unix_ms);
        let lease = self
            .leases
            .current_lease(manifest.table_id, &manifest.table)
            .await
            .map_err(Resolution::Retry)?;
        let record = if record.owner_instance_id == self.owner
            && record.lease_id == lease.lease_id()
            && record.fencing_epoch == lease.fencing_token()
        {
            record
        } else {
            self.adopt(&record, &lease).await?
        };
        let admission = Admission {
            receipt: ReceiptVersion::new(record.receipt_id, record.revision),
            delivery,
            lease,
            batches: vec![batch],
        };
        match self.buffer.admit_tracked_at(admission, state, ingested_at_ms).await {
            Ok((_, completion)) => Ok(completion),
            Err(error) if error.kind() == BufferErrorKind::CapacityExceeded => {
                self.fail_receipt(&record, "persisted delivery exceeds buffer capacity")
                    .await;
                Err(Resolution::Reject)
            }
            Err(error) => Err(Resolution::Retry(error.message().to_owned())),
        }
    }

    async fn adopt(&self, record: &ReceiptRecord, lease: &TableLease) -> Result<ReceiptRecord, Resolution> {
        let now = now_unix_millis().map_err(|error| Resolution::Retry(error.message().to_owned()))?;
        let deadline = receipt_deadline(lease, self.receipt_lease_duration)
            .map_err(|error| Resolution::Retry(error.message().to_owned()))?;
        let request = ReceiptAdoption {
            successor: self.owner,
            lease: lease.clone(),
            lease_deadline: deadline,
            adopted_at: now,
            receipts: vec![AdoptionCandidate::from_record(record)],
        };
        let mut report = {
            let _adopting = self.adoption.lock().await;
            self.receipts
                .adopt_receipts(&request)
                .await
                .map_err(|error| Resolution::Retry(format!("adopt receipt: {error}")))?
        };
        if let Some(adopted) = report.adopted.pop() {
            self.counters.adopted_receipts.fetch_add(1, Ordering::Relaxed);
            info!(
                receipt_id = %adopted.receipt_id,
                previous_owner = %record.owner_instance_id,
                state = ?adopted.state,
                "adopted receipt of a persisted bundle"
            );
            return Ok(adopted);
        }
        let current = report.rejected.pop().and_then(|rejected| rejected.current);
        match current.map(|record| record.state) {
            Some(ReceiptState::Published) => Err(Resolution::Ack),
            Some(ReceiptState::Failed | ReceiptState::PublicationUnknown) | None => Err(Resolution::Reject),
            Some(_) => Err(Resolution::Retry("receipt could not be adopted yet".to_owned())),
        }
    }

    async fn fail_receipt(&self, record: &ReceiptRecord, message: &str) {
        let Ok(error) = ErrorStatus::new(ErrorCode::CapacityExhausted, message) else {
            return;
        };
        let Ok(failure) = ReceiptFailure::new(FailureDisposition::Retryable, error.with_receipt_id(record.receipt_id))
        else {
            return;
        };
        let Ok(now) = now_unix_millis() else {
            return;
        };
        let version = ReceiptVersion::new(record.receipt_id, record.revision);
        if let Err(error) = self
            .receipts
            .fail_batch(record.owner_instance_id, &[version], &failure, now)
            .await
        {
            warn!(receipt_id = %record.receipt_id, %error, "could not fail an unadmittable persisted receipt");
        }
    }
}

/// Counts one in-progress operation for as long as the guard lives.
struct StorageOperation<'a> {
    counter: &'a AtomicU64,
}

impl<'a> StorageOperation<'a> {
    fn start(counter: &'a AtomicU64) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self { counter }
    }
}

impl Drop for StorageOperation<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

fn resolve(outcome: DeliveryOutcome) -> Resolution {
    match outcome {
        DeliveryOutcome::Published | DeliveryOutcome::PublishedUnrecorded => Resolution::Ack,
        DeliveryOutcome::Failed => Resolution::Reject,
        DeliveryOutcome::Unknown => Resolution::Retry("publication outcome unknown".to_owned()),
    }
}

fn direct_bundle(request: PersistRequest<'_>) -> BufferResult<DeliveryBundle> {
    let PersistRequest {
        receipt,
        owner,
        delivery,
        lease,
        batch,
    } = request;
    let manifest = DeliveryManifest {
        format: DELIVERY_MANIFEST_FORMAT.to_owned(),
        version: DELIVERY_MANIFEST_VERSION,
        receipt_id: receipt.receipt_id,
        ingested_at_ms: Some(now_unix_millis()?),
        receipt_revision: receipt.revision,
        owner_instance_id: owner,
        lease_id: lease.lease_id(),
        fencing_token: lease.fencing_token().get(),
        table_id: delivery.table_id(),
        table: delivery.table().clone(),
        schema_version: delivery.schema_version(),
        schema_fingerprint: delivery.schema_fingerprint(),
        rows: delivery.rows().get(),
        batches: delivery.batches().get(),
        arrow_memory_bytes: delivery.arrow_memory_bytes().get(),
    };
    DeliveryBundle::new(&manifest, batch)
}

fn durable_error(error: impl std::fmt::Display) -> BufferError {
    BufferError::new(BufferErrorKind::DurableStorage, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrowcol_protocol::{CatalogName, SchemaName, TableName, fingerprint_schema};

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn batch(values: &[i64]) -> TestResult<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
        Ok(RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )?)
    }

    fn manifest(rows: u64) -> TestResult<DeliveryManifest> {
        Ok(DeliveryManifest {
            format: DELIVERY_MANIFEST_FORMAT.to_owned(),
            version: DELIVERY_MANIFEST_VERSION,
            receipt_id: ReceiptId::new(),
            ingested_at_ms: Some(UnixMillis::new(1_700_000_012_345)?),
            receipt_revision: 0,
            owner_instance_id: CollectorInstanceId::new(),
            lease_id: LeaseId::new(),
            fencing_token: 3,
            table_id: Uuid::new_v4(),
            table: TableIdentity::new(
                CatalogName::new("telemetry")?,
                SchemaName::new("sqlserver")?,
                TableName::new("sqlserver_wait_stats")?,
            ),
            schema_version: SchemaVersion::FIRST,
            schema_fingerprint: fingerprint_schema(batch(&[1])?.schema().as_ref()),
            rows,
            batches: 1,
            arrow_memory_bytes: 64,
        })
    }

    /// Scenario: A delivery manifest is persisted as Quiver bundle metadata
    /// and read back by a restarted collector or an inspection tool.
    /// Guarantees: Decoding returns the encoded manifest; other formats,
    /// versions, and malformed bytes are rejected.
    #[test]
    fn delivery_manifest_round_trips_and_rejects_other_formats() -> TestResult {
        let manifest = manifest(2)?;
        assert_eq!(DeliveryManifest::decode(&manifest.encode()?)?, manifest);
        assert_eq!(
            DeliveryManifest::decode(&manifest.encode()?)?.ingested_at_ms,
            Some(UnixMillis::new(1_700_000_012_345)?)
        );
        let mut legacy = serde_json::to_value(&manifest)?;
        legacy
            .as_object_mut()
            .ok_or("manifest was not an object")?
            .remove("ingested_at_ms");
        assert_eq!(
            DeliveryManifest::decode(&serde_json::to_vec(&legacy)?)?.ingested_at_ms,
            None
        );
        let mut other = manifest.clone();
        other.version = DELIVERY_MANIFEST_VERSION + 1;
        assert!(DeliveryManifest::decode(&other.encode()?).is_err());
        assert!(DeliveryManifest::decode(b"QPA\x01").is_err());
        Ok(())
    }

    /// Scenario: The Direct collector or quiver-cli builds the bundle it
    /// persists for one delivery.
    /// Guarantees: The batch sits in DELIVERY_SLOT with the manifest's schema
    /// fingerprint, the manifest is the bundle metadata, and a manifest that
    /// does not describe exactly this batch is rejected.
    #[test]
    fn delivery_bundle_carries_its_batch_and_manifest() -> TestResult {
        let manifest = manifest(3)?;
        let bundle = DeliveryBundle::new(&manifest, batch(&[1, 2, 3])?)?;
        let payload = bundle.payload(DELIVERY_SLOT).ok_or("payload")?;
        assert_eq!(payload.batch.num_rows(), 3);
        assert_eq!(&payload.schema_fingerprint, manifest.schema_fingerprint.as_bytes());
        assert!(bundle.payload(SlotId::new(1)).is_none());
        assert_eq!(
            DeliveryManifest::decode(bundle.metadata().ok_or("metadata")?)?,
            manifest
        );
        assert_eq!(bundle.item_count(), 3);
        assert!(DeliveryBundle::new(&manifest, batch(&[1])?).is_err());
        let mut two_batches = manifest.clone();
        two_batches.batches = 2;
        assert!(DeliveryBundle::new(&two_batches, batch(&[1, 2, 3])?).is_err());
        Ok(())
    }
}
