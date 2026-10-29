use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, Weak};

use arrow_deltalake::commit::{BatchedTable, CommitReceiptError};
use arrow_deltalake::sizing::AdaptiveFileSizeEstimator;
use async_trait::async_trait;
use futures::future::{FutureExt, LocalBoxFuture};
use futures::stream::{FuturesUnordered, StreamExt};
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::{AckMsg, NackCause, NackMsg, NodeControlMsg};
use otel_arrow_dfe_engine::error::{Error, ExporterErrorKind};
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter;
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otel_arrow_dfe_otap::OTAP_EXPORTER_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::PayloadData;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::time::Instant;

use crate::DeltaExporterConfig;
use crate::config::{BatchCapacity, FileBatchConfig, config_error, parse_config, table_path};
use crate::envelope::{ArrowcolBatchEnvelope, DecodedArrowBatch, EnvelopeError, decode_record_set};
use crate::metrics::{RuntimeMetrics, global_metrics};
use crate::routing::{
    AuthorityClient, ExportingProcess, LEASE_VIEW_TTL, LeaseBinder, LeaseBinding, LeaseEvidence,
    RoutingPublicationFence,
};

/// DFE URN for arbitrary Arrow Delta output.
pub const ARROW_DELTALAKE_EXPORTER_URN: &str = "urn:otel:exporter:arrow_deltalake";

type BackendRegistry = Mutex<HashMap<PathBuf, Weak<DeltaBackend>>>;
static BACKENDS: LazyLock<BackendRegistry> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Generic Delta exporter runtime.
pub struct GenericDeltaExporter {
    backend: Arc<DeltaBackend>,
}

impl GenericDeltaExporter {
    fn shared(config: DeltaExporterConfig, metrics: Arc<RuntimeMetrics>) -> Result<Self, String> {
        let mut registry = BACKENDS
            .lock()
            .map_err(|_| "Delta backend registry lock poisoned".to_owned())?;
        if let Some(existing) = registry.get(&config.base_path).and_then(Weak::upgrade) {
            if existing.configuration.as_ref() != Some(&config) {
                return Err("conflicting Delta exporter configuration for the same root".to_owned());
            }
            return Ok(Self { backend: existing });
        }
        let store = config.routing.build()?;
        let options = config.commit.build()?;
        metrics.configure_exporter(options.max_pending_files);
        let authority =
            AuthorityClient::start(Arc::clone(&store), config.authority_timeout, options.max_pending_files)?;
        let backend = Arc::new(DeltaBackend {
            configuration: Some(config.clone()),
            base_path: config.base_path.clone(),
            options,
            batching: config.batching.clone(),
            capacity: Capacity::new(config.batching.global_capacity),
            flush_slots: Arc::new(Semaphore::new(config.batching.max_concurrent_flushes)),
            draining: std::sync::atomic::AtomicBool::new(false),
            authority,
            leases: LeaseBinder::new(store, LEASE_VIEW_TTL),
            metrics,
            tables: tokio::sync::Mutex::new(HashMap::new()),
        });
        registry.insert(config.base_path, Arc::downgrade(&backend));
        Ok(Self { backend })
    }

    #[cfg(test)]
    fn with_store(
        base_path: PathBuf,
        store: Arc<dyn arrowcol_routing::RoutingStore>,
        options: arrow_deltalake::commit::CommitOptions,
        metrics: Arc<RuntimeMetrics>,
    ) -> Result<Self, String> {
        Self::with_batching(
            base_path,
            store,
            options,
            FileBatchConfig {
                max_batches_per_flush: 1,
                ..Default::default()
            },
            metrics,
        )
    }

    #[cfg(test)]
    fn with_batching(
        base_path: PathBuf,
        store: Arc<dyn arrowcol_routing::RoutingStore>,
        options: arrow_deltalake::commit::CommitOptions,
        batching: FileBatchConfig,
        metrics: Arc<RuntimeMetrics>,
    ) -> Result<Self, String> {
        batching.validate()?;
        metrics.configure_exporter(options.max_pending_files);
        Ok(Self {
            backend: Arc::new(DeltaBackend {
                configuration: None,
                base_path,
                authority: AuthorityClient::start(
                    Arc::clone(&store),
                    std::time::Duration::from_secs(2),
                    options.max_pending_files,
                )?,
                leases: LeaseBinder::new(store, LEASE_VIEW_TTL),
                options,
                capacity: Capacity::new(batching.global_capacity),
                flush_slots: Arc::new(Semaphore::new(batching.max_concurrent_flushes)),
                batching,
                draining: std::sync::atomic::AtomicBool::new(false),
                metrics,
                tables: tokio::sync::Mutex::new(HashMap::new()),
            }),
        })
    }

    #[cfg(test)]
    async fn export(&self, pdata: OtapPdata) -> Result<(), ExportFailure> {
        self.stage(pdata).await?.wait().await
    }

    /// Decodes, binds, and stages one pdata batch.
    ///
    /// Batches admitted by this process under native Quiver carry a backlog
    /// release that settles with the final outcome: publication or a permanent
    /// failure removes the batch from the durable backlog, while a transient
    /// failure keeps it there because the durable buffer retries it.
    async fn stage(&self, pdata: OtapPdata) -> Result<PendingDelta, ExportFailure> {
        let (_, payload) = pdata.into_parts();
        let records = match payload.into_data() {
            PayloadData::PdataArrowRecords(records) => *records,
            PayloadData::OtlpBytes(_) | PayloadData::OtapArrowRecords(_) => {
                return Err(ExportFailure::Permanent(
                    "generic Delta exporter accepts only Arrowcol pluggable Arrow pdata".to_owned(),
                ));
            }
        };
        let mut decoded = decode_record_set(records).map_err(ExportFailure::Envelope)?;
        if decoded.len() != 1 {
            return Err(ExportFailure::Permanent(
                "Arrowcol Flight pdata must contain exactly one root table".to_owned(),
            ));
        }
        let table = decoded
            .pop()
            .ok_or_else(|| ExportFailure::Permanent("Arrowcol Flight pdata contains no root table".to_owned()))?;
        let rows = u64::try_from(table.batch.num_rows()).unwrap_or(u64::MAX);
        let bytes = arrow_deltalake::record_batch_logical_bytes(&table.batch)
            .map_err(|error| ExportFailure::Permanent(error.to_string()));
        let release = self
            .backend
            .backlog_release(&table.envelope, rows, bytes.as_ref().copied().unwrap_or(0));
        let staged = match bytes {
            Ok(bytes) => self.backend.bind_and_stage(table, rows, bytes).await,
            Err(error) => Err(error),
        };
        match staged {
            Ok(mut publication) => {
                publication.release = release;
                Ok(publication)
            }
            Err(error) => {
                if let Some(release) = release {
                    release.settle(Err(&error));
                }
                Err(error)
            }
        }
    }
}

struct DeltaBackend {
    configuration: Option<DeltaExporterConfig>,
    base_path: PathBuf,
    options: arrow_deltalake::commit::CommitOptions,
    batching: FileBatchConfig,
    capacity: Capacity,
    flush_slots: Arc<Semaphore>,
    draining: std::sync::atomic::AtomicBool,
    authority: AuthorityClient,
    leases: LeaseBinder,
    metrics: Arc<RuntimeMetrics>,
    tables: tokio::sync::Mutex<HashMap<arrowcol_protocol::TableIdentity, Arc<TableOwner>>>,
}

impl Drop for DeltaBackend {
    fn drop(&mut self) {
        for owner in self.tables.get_mut().values() {
            owner.drain.send_replace(true);
        }
    }
}

impl DeltaBackend {
    async fn owner(&self, decoded: &DecodedArrowBatch) -> Result<Arc<TableOwner>, ExportFailure> {
        let mut tables = self.tables.lock().await;
        if self.draining.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ExportFailure::Transient("Delta exporter is draining".to_owned()));
        }
        if let Some(owner) = tables.get(&decoded.envelope.table) {
            if !owner.retired.load(std::sync::atomic::Ordering::Acquire) {
                return Ok(Arc::clone(owner));
            }
            // A failed commit poisons the shared commit worker. Its retired owner already released
            // the worker, so reopening below waits for that worker to stop and starts a fresh one.
            tables.remove(&decoded.envelope.table);
        }
        let evidence = LeaseEvidence::from_envelope(&decoded.envelope);
        let fence = Arc::new(RoutingPublicationFence::new(evidence.clone(), self.authority.clone()));
        let publication_fence: Arc<dyn arrow_deltalake::delta::PublicationFence> =
            Arc::clone(&fence) as Arc<dyn arrow_deltalake::delta::PublicationFence>;
        let table = BatchedTable::open_with_fence(
            table_path(&self.base_path, &decoded.envelope.table),
            self.options.clone(),
            publication_fence,
        )
        .await
        .map_err(|error| ExportFailure::Transient(error.to_string()))?;
        let (sender, receiver) = mpsc::channel(self.batching.per_table_capacity.batches as usize);
        let (drain, drain_rx) = watch::channel(false);
        let estimator = AdaptiveFileSizeEstimator::new(self.batching.adaptive_file.build())
            .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
        let retired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let owner = Arc::new(TableOwner {
            sender,
            drain,
            capacity: Capacity::new(self.batching.per_table_capacity),
            fence,
            state: Mutex::new(OwnerState { evidence, pending: 0 }),
            retired: Arc::clone(&retired),
        });
        tokio::spawn(run_batch_worker(
            table,
            receiver,
            drain_rx,
            self.batching.clone(),
            estimator,
            Arc::clone(&self.flush_slots),
            Arc::clone(&self.metrics),
            decoded.envelope.table.clone(),
            retired,
        ));
        tables.insert(decoded.envelope.table.clone(), Arc::clone(&owner));
        Ok(owner)
    }

    /// Returns the durable-backlog release for a batch this process admitted under native Quiver.
    ///
    /// Batches replayed from a predecessor process lifetime never entered this
    /// process's backlog, so they are counted only by the replay metrics.
    fn backlog_release(&self, envelope: &ArrowcolBatchEnvelope, rows: u64, bytes: u64) -> Option<BacklogRelease> {
        (self.metrics.native_quiver_enabled() && envelope.holder_instance_id == self.metrics.instance_id()).then(|| {
            BacklogRelease {
                metrics: Arc::clone(&self.metrics),
                table: envelope.table.clone(),
                rows,
                bytes,
            }
        })
    }

    /// Binds the envelope to the authoritative lease held by this process, then stages it.
    ///
    /// A batch whose lease cannot be bound fails transiently, never
    /// permanently, so the durable buffer retains and retries it.
    async fn bind_and_stage(
        &self,
        mut decoded: DecodedArrowBatch,
        rows: u64,
        bytes: u64,
    ) -> Result<PendingDelta, ExportFailure> {
        if self.draining.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ExportFailure::Transient("Delta exporter is draining".to_owned()));
        }
        let process = ExportingProcess {
            instance_id: self.metrics.instance_id(),
            instance_key: self.metrics.instance_key(),
        };
        if decoded.envelope.holder_instance_id != process.instance_id {
            self.metrics.replayed_batch();
        }
        let binding = self
            .leases
            .bind(&decoded.envelope, process)
            .await
            .map_err(ExportFailure::Transient)?;
        match binding {
            LeaseBinding::Current => {}
            LeaseBinding::Rebind { .. } => {
                binding.apply(&mut decoded.envelope);
                self.metrics.rebound_batch();
            }
            LeaseBinding::Defer(reason) => {
                self.metrics.replay_deferred();
                return Err(ExportFailure::Transient(reason.to_owned()));
            }
        }
        self.stage_table(decoded, rows, bytes).await
    }

    async fn stage_table(
        &self,
        decoded: DecodedArrowBatch,
        rows: u64,
        bytes: u64,
    ) -> Result<PendingDelta, ExportFailure> {
        if self.draining.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ExportFailure::Transient("Delta exporter is draining".to_owned()));
        }
        let owner = self.owner(&decoded).await?;
        let evidence = LeaseEvidence::from_envelope(&decoded.envelope);
        let pending_owner = owner.begin(evidence)?;
        self.metrics.observe_schema_versions(
            &decoded.envelope.table,
            decoded.envelope.source_schema_version,
            decoded.envelope.target_schema_version,
        );
        let mut capacity = self.capacity.acquire(rows, bytes).await?;
        capacity.extend(owner.capacity.acquire(rows, bytes).await?);
        let (reply, receipt) = oneshot::channel();
        owner
            .sender
            .try_send(WorkerCommand::Batch(QueuedBatch {
                batch: decoded.batch,
                target_schema_version: decoded.envelope.target_schema_version.get(),
                rows,
                bytes,
                accepted: Instant::now(),
                reply: Some(reply),
                _pending_owner: pending_owner,
                _capacity: capacity,
            }))
            .map_err(|error| ExportFailure::Transient(format!("Delta file queue admission failed: {error}")))?;
        Ok(PendingDelta { receipt, release: None })
    }

    async fn flush_pending(&self) -> Result<(), ExportFailure> {
        let owners = self.tables.lock().await.values().cloned().collect::<Vec<_>>();
        for owner in owners {
            let (reply, completion) = oneshot::channel();
            let flushed = match owner.sender.send(WorkerCommand::Flush(reply)).await {
                Ok(()) => completion
                    .await
                    .map_err(|error| format!("Delta flush worker disappeared: {error}")),
                Err(error) => Err(format!("Delta flush request failed: {error}")),
            };
            // A retired owner already failed every batch it held, so it has nothing left to flush.
            if let Err(message) = flushed
                && !owner.retired.load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(ExportFailure::Transient(message));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    async fn start_draining(&self) {
        self.draining.store(true, std::sync::atomic::Ordering::Release);
        let tables = self.tables.lock().await;
        for owner in tables.values() {
            owner.drain.send_replace(true);
        }
    }
}

struct PendingDelta {
    receipt: oneshot::Receiver<Result<(), ExportFailure>>,
    release: Option<BacklogRelease>,
}

impl PendingDelta {
    async fn wait(self) -> Result<(), ExportFailure> {
        let Self { receipt, release } = self;
        let result = receipt.await.unwrap_or_else(|error| {
            Err(ExportFailure::Transient(format!(
                "Delta file publication worker disappeared: {error}"
            )))
        });
        if let Some(release) = release {
            release.settle(result.as_ref().map(|_| ()));
        }
        result
    }
}

/// Durable-backlog accounting for one batch admitted by this process.
struct BacklogRelease {
    metrics: Arc<RuntimeMetrics>,
    table: arrowcol_protocol::TableIdentity,
    rows: u64,
    bytes: u64,
}

impl BacklogRelease {
    /// Releases the batch once its outcome is final: published, or refused
    /// permanently so the durable buffer drops it. Transient failures keep it
    /// in the backlog because the durable buffer retries the batch.
    fn settle(self, outcome: Result<(), &ExportFailure>) {
        if outcome.is_ok() || outcome.is_err_and(ExportFailure::permanent) {
            self.metrics
                .durable_backlog_released(&self.table, self.rows, self.bytes);
        }
    }
}

struct Capacity {
    limits: BatchCapacity,
    bytes: Arc<Semaphore>,
    rows: Arc<Semaphore>,
    batches: Arc<Semaphore>,
}

impl Capacity {
    fn new(limits: BatchCapacity) -> Self {
        Self {
            limits,
            bytes: Arc::new(Semaphore::new(limits.bytes as usize)),
            rows: Arc::new(Semaphore::new(limits.rows as usize)),
            batches: Arc::new(Semaphore::new(limits.batches as usize)),
        }
    }

    async fn acquire(&self, rows: u64, bytes: u64) -> Result<Vec<OwnedSemaphorePermit>, ExportFailure> {
        let rows = u32::try_from(rows).map_err(|error| ExportFailure::Permanent(error.to_string()))?;
        let bytes = u32::try_from(bytes).map_err(|error| ExportFailure::Permanent(error.to_string()))?;
        if rows > self.limits.rows || bytes > self.limits.bytes {
            return Err(ExportFailure::Transient(
                "Arrow file buffering capacity exhausted: batch exceeds configured capacity".to_owned(),
            ));
        }
        let mut permits = Vec::with_capacity(3);
        for (capacity, amount) in [
            (Arc::clone(&self.bytes), bytes),
            (Arc::clone(&self.rows), rows),
            (Arc::clone(&self.batches), 1),
        ] {
            permits.push(capacity.acquire_many_owned(amount).await.map_err(|error| {
                ExportFailure::Transient(format!("Arrow file buffering capacity unavailable: {error}"))
            })?);
        }
        Ok(permits)
    }
}

enum WorkerCommand {
    Batch(QueuedBatch),
    Flush(oneshot::Sender<()>),
}

struct QueuedBatch {
    batch: arrow_array::RecordBatch,
    target_schema_version: u64,
    rows: u64,
    bytes: u64,
    accepted: Instant,
    reply: Option<oneshot::Sender<Result<(), ExportFailure>>>,
    _pending_owner: PendingOwner,
    _capacity: Vec<OwnedSemaphorePermit>,
}

impl QueuedBatch {
    fn complete(&mut self, result: &Result<(), ExportFailure>) {
        if let Some(reply) = self.reply.take() {
            let result = match result {
                Ok(()) => Ok(()),
                Err(error) if error.permanent() => Err(ExportFailure::Permanent(error.to_string())),
                Err(error) => Err(ExportFailure::Transient(error.to_string())),
            };
            let _ = reply.send(result);
        }
    }
}

impl Drop for QueuedBatch {
    fn drop(&mut self) {
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(Err(ExportFailure::Transient(
                "Delta file worker stopped before publication completed".to_owned(),
            )));
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_batch_worker(
    table: Arc<BatchedTable>,
    mut receiver: mpsc::Receiver<WorkerCommand>,
    mut drain: watch::Receiver<bool>,
    policy: FileBatchConfig,
    mut estimator: AdaptiveFileSizeEstimator,
    flush_slots: Arc<Semaphore>,
    metrics: Arc<RuntimeMetrics>,
    identity: arrowcol_protocol::TableIdentity,
    retired: Arc<std::sync::atomic::AtomicBool>,
) {
    let mut table = Some(table);
    let mut group: Vec<QueuedBatch> = Vec::new();
    let mut rows = 0;
    let mut bytes = 0;
    let mut closing = false;
    let mut publisher = GroupPublisher {
        table: &mut table,
        retired: &retired,
        flush_slots: &flush_slots,
        metrics: &metrics,
        identity: &identity,
    };
    loop {
        let next = if closing {
            receiver.recv().await
        } else if let Some(first) = group.first() {
            tokio::select! {
                next = receiver.recv() => next,
                _ = tokio::time::sleep_until(first.accepted + policy.max_age) => {
                    publisher.publish(&mut group, &mut estimator).await;
                    rows = 0;
                    bytes = 0;
                    if publisher.retired() {
                        break;
                    }
                    continue;
                }
                _ = drain.changed() => {
                    receiver.close();
                    closing = true;
                    continue;
                }
            }
        } else {
            tokio::select! {
                next = receiver.recv() => next,
                _ = drain.changed() => {
                    receiver.close();
                    closing = true;
                    continue;
                }
            }
        };
        let Some(next) = next else {
            publisher.publish(&mut group, &mut estimator).await;
            if let Some(table) = publisher.table.as_ref() {
                table.start_draining();
            }
            return;
        };
        let mut next = match next {
            WorkerCommand::Batch(batch) => batch,
            WorkerCommand::Flush(reply) => {
                publisher.publish(&mut group, &mut estimator).await;
                rows = 0;
                bytes = 0;
                let _ = reply.send(());
                if publisher.retired() {
                    break;
                }
                continue;
            }
        };
        if group.first().is_some_and(|first| {
            first.target_schema_version != next.target_schema_version
                || first.batch.schema_ref() != next.batch.schema_ref()
        }) {
            publisher.publish(&mut group, &mut estimator).await;
            rows = 0;
            bytes = 0;
            if publisher.retired() {
                next.complete(&Err(retired_owner_failure()));
                break;
            }
        }
        rows += next.rows;
        bytes += next.bytes;
        group.push(next);
        if estimator.should_flush(bytes)
            || bytes >= policy.max_arrow_memory_per_flush
            || rows >= policy.max_rows_per_flush
            || group.len() as u64 >= policy.max_batches_per_flush
        {
            publisher.publish(&mut group, &mut estimator).await;
            rows = 0;
            bytes = 0;
            if publisher.retired() {
                break;
            }
        }
    }
    // The owner retired after its commit worker failed: refuse everything still queued so the
    // durable buffer retries it through the replacement owner.
    receiver.close();
    while let Some(command) = receiver.recv().await {
        match command {
            WorkerCommand::Batch(mut batch) => batch.complete(&Err(retired_owner_failure())),
            WorkerCommand::Flush(reply) => {
                let _ = reply.send(());
            }
        }
    }
}

fn retired_owner_failure() -> ExportFailure {
    ExportFailure::Transient(
        "Delta table owner retired after a failed publication; the batch will be retried with a fresh owner".to_owned(),
    )
}

/// Publishes grouped files through one table owner's shared commit worker.
///
/// The commit worker caches its first commit failure (a fence rejection, an
/// I/O error, or a stopped worker) and refuses every later file. After such a
/// failure the publisher releases the worker and retires the owner before it
/// settles the failed batches, so their retries reach a fresh owner that
/// reopens the table under the current lease.
struct GroupPublisher<'a> {
    table: &'a mut Option<Arc<BatchedTable>>,
    retired: &'a std::sync::atomic::AtomicBool,
    flush_slots: &'a Arc<Semaphore>,
    metrics: &'a RuntimeMetrics,
    identity: &'a arrowcol_protocol::TableIdentity,
}

impl GroupPublisher<'_> {
    fn retired(&self) -> bool {
        self.table.is_none()
    }

    async fn publish(&mut self, group: &mut Vec<QueuedBatch>, estimator: &mut AdaptiveFileSizeEstimator) {
        if group.is_empty() {
            return;
        }
        let Some(table) = self.table.as_ref() else {
            for mut entry in group.drain(..) {
                entry.complete(&Err(retired_owner_failure()));
            }
            return;
        };
        let rows = group.iter().map(|entry| entry.rows).sum();
        let bytes = group.iter().map(|entry| entry.bytes).sum();
        self.metrics.publication_started(self.identity);
        let mut accounting = PublicationAccounting {
            metrics: self.metrics,
            identity: self.identity,
            rows,
            bytes,
            finished: false,
        };
        let mut worker_failed = false;
        let result = async {
            let _slot = self
                .flush_slots
                .acquire()
                .await
                .map_err(|error| ExportFailure::Transient(error.to_string()))?;
            let mut writer = table.new_file(group[0].batch.schema()).await.map_err(|error| {
                worker_failed = true;
                ExportFailure::Transient(error.to_string())
            })?;
            for entry in group.iter() {
                writer
                    .write(&entry.batch)
                    .await
                    .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
            }
            let append = writer
                .stage()
                .await
                .map_err(|error| ExportFailure::Transient(error.to_string()))?;
            let observation = append.observation();
            let mut updated_estimator = estimator.clone();
            updated_estimator
                .observe(observation)
                .map_err(|error| ExportFailure::Permanent(format!("file sizing observation failed: {error}")))?;
            let receipt = table.submit(append, true).await.map_err(|error| {
                worker_failed = true;
                ExportFailure::Transient(error.to_string())
            })?;
            let committed = receipt.wait_typed().await.map_err(|error| {
                worker_failed = true;
                map_receipt_error(error)
            })?;
            *estimator = updated_estimator;
            Ok::<_, ExportFailure>(committed.version)
        }
        .await;
        self.metrics
            .publication_finished(self.identity, rows, bytes, result.as_ref().ok().copied());
        accounting.finished = true;
        if worker_failed {
            // Release the poisoned worker before advertising retirement, so the replacement owner
            // can reopen the table as soon as it observes the flag.
            *self.table = None;
            self.retired.store(true, std::sync::atomic::Ordering::Release);
        }
        let outcome = result.map(|_| ());
        for mut entry in group.drain(..) {
            entry.complete(&outcome);
        }
    }
}

struct PublicationAccounting<'a> {
    metrics: &'a RuntimeMetrics,
    identity: &'a arrowcol_protocol::TableIdentity,
    rows: u64,
    bytes: u64,
    finished: bool,
}

impl Drop for PublicationAccounting<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics
                .publication_finished(self.identity, self.rows, self.bytes, None);
            self.metrics.set_healthy(false);
        }
    }
}

struct OwnerState {
    evidence: LeaseEvidence,
    pending: usize,
}

struct TableOwner {
    sender: mpsc::Sender<WorkerCommand>,
    drain: watch::Sender<bool>,
    capacity: Capacity,
    fence: Arc<RoutingPublicationFence>,
    state: Mutex<OwnerState>,
    /// Set once the owner's commit worker failed; the next batch replaces the owner.
    retired: Arc<std::sync::atomic::AtomicBool>,
}

impl TableOwner {
    fn begin(self: &Arc<Self>, evidence: LeaseEvidence) -> Result<PendingOwner, ExportFailure> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ExportFailure::Transient("table owner lock poisoned".to_owned()))?;
        if state.evidence != evidence {
            if state.pending != 0 {
                return Err(ExportFailure::Transient(
                    "table lease changed while prior publications remain pending".to_owned(),
                ));
            }
            self.fence
                .replace_expected(evidence.clone())
                .map_err(ExportFailure::Transient)?;
            state.evidence = evidence;
        }
        state.pending = state.pending.saturating_add(1);
        Ok(PendingOwner {
            owner: Arc::clone(self),
        })
    }
}

struct PendingOwner {
    owner: Arc<TableOwner>,
}

impl Drop for PendingOwner {
    fn drop(&mut self) {
        if let Ok(mut state) = self.owner.state.lock() {
            state.pending = state.pending.saturating_sub(1);
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ExportFailure {
    #[error("{0}")]
    Permanent(String),
    #[error("{0}")]
    Transient(String),
    #[error("{0}")]
    Envelope(EnvelopeError),
}

impl ExportFailure {
    fn permanent(&self) -> bool {
        matches!(self, Self::Permanent(_) | Self::Envelope(_))
    }
}

fn map_receipt_error(error: CommitReceiptError) -> ExportFailure {
    match error {
        CommitReceiptError::PublicationFenced(error) => ExportFailure::Transient(error.to_string()),
        CommitReceiptError::CommitFailed { message } => ExportFailure::Transient(message),
        CommitReceiptError::WorkerStopped => ExportFailure::Transient("Delta commit worker stopped".to_owned()),
        _ => ExportFailure::Transient("Delta commit failed".to_owned()),
    }
}

fn validate_exporter_config(value: &serde_json::Value) -> Result<(), otel_arrow_dfe_config::error::Error> {
    let config: DeltaExporterConfig = parse_config(value)?;
    config.validate().map_err(config_error)
}

fn create_exporter(
    _pipeline: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    exporter_config: &ExporterConfig,
    _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities,
) -> Result<ExporterWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    let config: DeltaExporterConfig = parse_config(&node_config.config)?;
    config.validate().map_err(config_error)?;
    let exporter = GenericDeltaExporter::shared(config, global_metrics()).map_err(config_error)?;
    Ok(ExporterWrapper::local(exporter, node, node_config, exporter_config))
}

/// External DFE exporter registration.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Exporter)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static ARROW_DELTALAKE_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: ARROW_DELTALAKE_EXPORTER_URN,
    create: create_exporter,
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED
        .with_pdata(otel_arrow_dfe_engine::wiring_contract::PdataWiringContract::PLUGGABLE_ARROW_SINK),
    validate_config: validate_exporter_config,
};

#[async_trait(?Send)]
impl exporter::Exporter<OtapPdata> for GenericDeltaExporter {
    async fn start(
        self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        effect_handler: exporter::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let mut pending: FuturesUnordered<LocalBoxFuture<'static, (OtapPdata, Result<(), ExportFailure>)>> =
            FuturesUnordered::new();
        loop {
            let message = if pending.is_empty() {
                Some(inbox.recv().await?)
            } else {
                tokio::select! {
                    message = inbox.recv() => Some(message?),
                    completion = pending.next() => {
                        if let Some((pdata, result)) = completion {
                            complete_publication(&effect_handler, pdata, result).await?;
                        }
                        None
                    }
                }
            };
            let Some(message) = message else {
                continue;
            };
            match message {
                Message::PData(pdata) => {
                    let export_copy = pdata.clone();
                    match self.stage(export_copy).await {
                        Ok(publication) => pending.push(
                            async move {
                                let result = publication.wait().await;
                                (pdata, result)
                            }
                            .boxed_local(),
                        ),
                        Err(error) => {
                            complete_publication(&effect_handler, pdata, Err(error)).await?;
                        }
                    }
                }
                Message::Control(NodeControlMsg::Shutdown { .. }) => {
                    self.backend
                        .flush_pending()
                        .await
                        .map_err(|error| Error::ExporterError {
                            exporter: effect_handler.exporter_id(),
                            kind: ExporterErrorKind::Shutdown,
                            error: error.to_string(),
                            source_detail: String::new(),
                        })?;
                    while let Some((pdata, result)) = pending.next().await {
                        complete_publication(&effect_handler, pdata, result).await?;
                    }
                    break;
                }
                Message::Control(_) => {}
            }
        }
        Ok(TerminalState::default())
    }
}

async fn complete_publication(
    effect_handler: &exporter::EffectHandler<OtapPdata>,
    pdata: OtapPdata,
    result: Result<(), ExportFailure>,
) -> Result<(), Error> {
    match result {
        Ok(()) => effect_handler.notify_ack(AckMsg::new(pdata)).await,
        Err(error) if error.permanent() => {
            effect_handler
                .notify_nack(NackMsg::new_permanent_with_cause(
                    error.to_string(),
                    pdata,
                    NackCause::Refused,
                ))
                .await
        }
        Err(error) => effect_handler.notify_nack(NackMsg::new(error.to_string(), pdata)).await,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::sync::Arc;

    use arrow_array::{ArrayRef, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use arrowcol_catalog::SchemaVersion;
    use arrowcol_protocol::{
        BasisPoints, CatalogName, CollectorInstanceId, FencingToken, ImplementationPool, LeaseId, Measurement,
        PositiveMeasurement, PressureGauge, SchemaName, TableIdentity, TableName, UnixMillis,
    };
    use arrowcol_routing::{
        CollectorRegistration, LeaseClaim, LeaseKey, MemoryRoutingStore, RichPressureSnapshot, RoutingStore,
        SubsystemState,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;
    use crate::envelope::{ArrowcolBatchEnvelope, encode_record_batch};
    use crate::metrics::RuntimeMetrics;

    fn now() -> UnixMillis {
        crate::config::now_unix_millis().expect("now")
    }

    fn table() -> TableIdentity {
        TableIdentity::new(
            CatalogName::new("demo").expect("catalog"),
            SchemaName::new("generated").expect("schema"),
            TableName::new("events").expect("table"),
        )
    }

    async fn lease(store: &Arc<MemoryRoutingStore>, holder: CollectorInstanceId) -> arrowcol_routing::RoutingLease {
        let observed = now();
        store
            .register_collector(&CollectorRegistration {
                instance_id: holder,
                instance_key: "test".to_owned(),
                implementation_pool: ImplementationPool::Dataflow,
                flight_endpoint: "127.0.0.1:32010".parse().expect("address"),
                catalog_capable: true,
                max_concurrent_streams: PositiveMeasurement::new(4).expect("positive"),
                started_at_unix_ms: observed,
                heartbeat_at_unix_ms: observed,
                lease_expires_at_unix_ms: UnixMillis::new(observed.get() + 60_000).expect("expiry"),
            })
            .await
            .expect("register");
        let zero =
            PressureGauge::new(Measurement::ZERO, PositiveMeasurement::new(4).expect("positive")).expect("gauge");
        store
            .publish_pressure(&RichPressureSnapshot {
                container_memory: None,
                effective_cpu_millicores: None,
                instance_id: holder,
                observed_at_unix_ms: observed,
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
        store
            .claim_lease(&LeaseClaim {
                key: LeaseKey::new(ImplementationPool::Dataflow, table()),
                candidate_instance_id: holder,
                now_unix_ms: observed,
                expires_at_unix_ms: UnixMillis::new(observed.get() + 60_000).expect("expiry"),
                minimum_snapshot_at_unix_ms: observed,
            })
            .await
            .expect("lease")
    }

    fn pdata(lease: &arrowcol_routing::RoutingLease, fencing_token: FencingToken) -> OtapPdata {
        versioned_pdata(lease, fencing_token, SchemaVersion::FIRST)
    }

    fn versioned_pdata(
        lease: &arrowcol_routing::RoutingLease,
        fencing_token: FencingToken,
        target_schema_version: SchemaVersion,
    ) -> OtapPdata {
        let mut envelope = envelope_for(lease);
        envelope.fencing_token = fencing_token;
        envelope.target_schema_version = target_schema_version;
        envelope_pdata(envelope)
    }

    /// Envelope admitted under `lease` by a collector registered with the `test` instance key.
    fn envelope_for(lease: &arrowcol_routing::RoutingLease) -> ArrowcolBatchEnvelope {
        ArrowcolBatchEnvelope {
            table: table(),
            source_schema_version: SchemaVersion::FIRST,
            target_schema_version: SchemaVersion::FIRST,
            lease_id: lease.lease_id,
            holder_instance_id: lease.holder_instance_id,
            fencing_token: lease.fencing_epoch,
            instance_key: "test".to_owned(),
        }
    }

    fn envelope_pdata(envelope: ArrowcolBatchEnvelope) -> OtapPdata {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
            ],
        )
        .expect("batch");
        let records = encode_record_batch(envelope, batch).expect("records");
        OtapPdata::new_todo_context(otel_arrow_dfe_pdata::OtapPayload::from_pdata_arrow(records))
    }

    /// Re-issues the table lease with a new lineage and the next epoch, as a reclaim, transfer, or
    /// crash succession does.
    async fn reissue_lease(
        store: &Arc<MemoryRoutingStore>,
        previous: &arrowcol_routing::RoutingLease,
        holder: CollectorInstanceId,
    ) -> arrowcol_routing::RoutingLease {
        let next = arrowcol_routing::RoutingLease {
            lease_id: LeaseId::new(),
            holder_instance_id: holder,
            fencing_epoch: previous.fencing_epoch.next().expect("next epoch"),
            lease_version: previous.lease_version + 1,
            ..previous.clone()
        };
        store.insert_lease(next.clone()).await.expect("re-issued lease");
        next
    }

    fn delta_version_exists(directory: &std::path::Path, version: u64) -> bool {
        directory
            .join(format!("demo/generated/events/_delta_log/{version:020}.json"))
            .exists()
    }

    fn durable_metrics(holder: CollectorInstanceId, instance_key: Option<&str>) -> Arc<RuntimeMetrics> {
        let metrics = Arc::new(RuntimeMetrics::new(holder));
        metrics.configure_receiver(4, u64::MAX / 4, 1_000_000, 1_000, 16, 4);
        if let Some(instance_key) = instance_key {
            metrics.configure_admission(instance_key, true).expect("admission mode");
        }
        metrics
    }

    fn fast_exporter(
        directory: &std::path::Path,
        store: &Arc<MemoryRoutingStore>,
        metrics: &Arc<RuntimeMetrics>,
    ) -> GenericDeltaExporter {
        GenericDeltaExporter::with_store(
            directory.to_path_buf(),
            Arc::clone(store) as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions {
                max_files_per_commit: 1,
                max_pending_files: 2,
                ..Default::default()
            },
            Arc::clone(metrics),
        )
        .expect("exporter")
    }

    /// Scenario: A generated non-OTAP Arrow schema reaches the generic Delta exporter.
    /// Guarantees: The original fields and values are written without OTAP normalization.
    #[tokio::test]
    async fn writes_generic_delta_output() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let metrics = Arc::new(RuntimeMetrics::new(holder));
        let exporter = GenericDeltaExporter::with_store(
            directory.path().to_path_buf(),
            store as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions {
                max_files_per_commit: 1,
                max_pending_files: 2,
                ..Default::default()
            },
            Arc::clone(&metrics),
        )
        .expect("exporter");

        exporter
            .export(pdata(&lease, lease.fencing_epoch))
            .await
            .expect("export");

        let data_file = std::fs::read_dir(directory.path().join("demo/generated/events"))
            .expect("table")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| path.extension().is_some_and(|extension| extension == "parquet"))
            .expect("parquet");
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(data_file).expect("file"))
            .expect("reader")
            .build()
            .expect("build");
        let batch = reader.next().expect("batch").expect("read");
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "value");
        assert_eq!(batch.num_rows(), 2);
        let pressure = serde_json::to_value(metrics.pressure().expect("pressure")).expect("pressure JSON");
        assert_eq!(pressure["table_versions"][0]["source_schema_version"], 1);
        assert_eq!(pressure["table_versions"][0]["target_schema_version"], 1);
        assert_eq!(pressure["table_versions"][0]["last_delta_version"], 1);
    }

    /// Scenario: The authoritative lease moves to another collector after a batch was staged under the
    /// current lease.
    /// Guarantees: The commit-boundary fence fails the publication transiently before a Delta log
    /// version becomes visible.
    #[tokio::test]
    async fn stale_fence_blocks_publication() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let exporter = GenericDeltaExporter::with_batching(
            directory.path().to_path_buf(),
            Arc::clone(&store) as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions::default(),
            FileBatchConfig {
                max_age: std::time::Duration::from_secs(60),
                ..Default::default()
            },
            Arc::new(RuntimeMetrics::new(holder)),
        )
        .expect("exporter");
        let publication = exporter.stage(pdata(&lease, lease.fencing_epoch)).await.expect("stage");
        reissue_lease(&store, &lease, CollectorInstanceId::new()).await;
        exporter.backend.start_draining().await;

        let error = tokio::time::timeout(std::time::Duration::from_secs(3), publication.wait())
            .await
            .expect("bounded publication")
            .expect_err("fenced");
        assert!(!error.permanent());
        assert!(error.to_string().contains("routing lease"));
        assert!(!delta_version_exists(directory.path(), 1));
    }

    /// Scenario: Shutdown begins with one file below the normal Delta batch threshold.
    /// Guarantees: Draining bypasses the collection window and publishes the bounded pending file.
    #[tokio::test]
    async fn graceful_drain_flushes_partial_publication() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let exporter = GenericDeltaExporter::with_store(
            directory.path().to_path_buf(),
            store as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions {
                batch_window: std::time::Duration::from_secs(60),
                max_files_per_commit: 2,
                max_pending_files: 2,
            },
            Arc::new(RuntimeMetrics::new(holder)),
        )
        .expect("exporter");
        let publication = exporter.stage(pdata(&lease, lease.fencing_epoch)).await.expect("stage");
        exporter.backend.start_draining().await;
        tokio::time::timeout(std::time::Duration::from_secs(3), publication.wait())
            .await
            .expect("bounded drain")
            .expect("publication");
        assert!(
            directory
                .path()
                .join("demo/generated/events/_delta_log/00000000000000000001.json")
                .exists()
        );
    }

    #[tokio::test]
    async fn core_shutdown_flushes_without_poisoning_shared_recovery() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let metrics = Arc::new(RuntimeMetrics::new(holder));
        let config = DeltaExporterConfig {
            base_path: directory.path().to_path_buf(),
            routing: crate::config::RoutingStoreConfig::Memory,
            commit: crate::config::CommitConfig::default(),
            batching: FileBatchConfig {
                max_age: std::time::Duration::from_secs(60),
                ..Default::default()
            },
            authority_timeout: std::time::Duration::from_secs(2),
        };
        let mut first = GenericDeltaExporter::with_batching(
            config.base_path.clone(),
            store as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions::default(),
            config.batching.clone(),
            Arc::clone(&metrics),
        )
        .expect("first");
        Arc::get_mut(&mut first.backend)
            .expect("exclusive backend")
            .configuration = Some(config.clone());
        BACKENDS
            .lock()
            .expect("registry")
            .insert(config.base_path.clone(), Arc::downgrade(&first.backend));
        let second = GenericDeltaExporter::shared(config.clone(), Arc::clone(&metrics)).expect("second");
        let publication = first.stage(pdata(&lease, lease.fencing_epoch)).await.expect("stage");
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            first.backend.flush_pending().await.expect("core flush");
            publication.wait().await.expect("publication");
        })
        .await
        .expect("bounded partial flush");
        drop(first);

        let replacement = GenericDeltaExporter::shared(config, Arc::clone(&metrics)).expect("replacement");
        assert!(Arc::ptr_eq(&replacement.backend, &second.backend));
        assert!(!replacement.backend.draining.load(std::sync::atomic::Ordering::Acquire));
        let publication = replacement
            .stage(pdata(&lease, lease.fencing_epoch))
            .await
            .expect("recovered stage");
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            replacement.backend.flush_pending().await.expect("final flush");
            publication.wait().await.expect("recovered publication");
        })
        .await
        .expect("bounded final flush");
        assert_eq!(
            replacement.backend.capacity.batches.available_permits(),
            replacement.backend.batching.global_capacity.batches as usize
        );
        let publication = replacement
            .stage(pdata(&lease, lease.fencing_epoch))
            .await
            .expect("last stage");
        drop(second);
        drop(replacement);
        tokio::time::timeout(std::time::Duration::from_secs(3), publication.wait())
            .await
            .expect("last core drains backend")
            .expect("last publication");
        assert!(metrics.prometheus().contains("arrowcol_delta_rows_total 6\n"));
    }

    /// Scenario: Two same-schema pdata batches reach the shared file-size threshold.
    /// Guarantees: One Parquet file publishes both batches and releases their capacity only after commit.
    #[tokio::test]
    async fn coalesces_batches_before_acknowledgement() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let metrics = Arc::new(RuntimeMetrics::new(holder));
        let exporter = GenericDeltaExporter::with_batching(
            directory.path().to_path_buf(),
            store as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions::default(),
            FileBatchConfig {
                max_batches_per_flush: 2,
                max_age: std::time::Duration::from_secs(60),
                ..Default::default()
            },
            Arc::clone(&metrics),
        )
        .expect("exporter");
        let first = exporter.stage(pdata(&lease, lease.fencing_epoch)).await.expect("first");
        assert!(exporter.backend.capacity.batches.available_permits() < 4096);
        assert!(
            !directory
                .path()
                .join("demo/generated/events/_delta_log/00000000000000000001.json")
                .exists()
        );
        let second = exporter
            .stage(pdata(&lease, lease.fencing_epoch))
            .await
            .expect("second");
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            first.wait().await.expect("first publication");
            second.wait().await.expect("second publication");
        })
        .await
        .expect("bounded publication");
        let files = std::fs::read_dir(directory.path().join("demo/generated/events"))
            .expect("files")
            .map(|entry| entry.expect("entry").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "parquet"))
            .collect::<Vec<_>>();
        assert_eq!(files.len(), 1);
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&files[0]).expect("file"))
            .expect("reader")
            .build()
            .expect("build");
        assert_eq!(reader.map(|batch| batch.expect("batch").num_rows()).sum::<usize>(), 4);
        assert!(metrics.prometheus().contains("arrowcol_delta_files_total 1\n"));
        assert_eq!(exporter.backend.capacity.batches.available_permits(), 4096);
    }

    /// Scenario: A sparse table receives one batch below the size and row thresholds.
    /// Guarantees: The age timer publishes it without additional traffic or a shutdown request.
    #[tokio::test]
    async fn sparse_tables_flush_at_max_age() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let exporter = GenericDeltaExporter::with_batching(
            directory.path().to_path_buf(),
            store as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions::default(),
            FileBatchConfig {
                max_age: std::time::Duration::from_millis(20),
                ..Default::default()
            },
            Arc::new(RuntimeMetrics::new(holder)),
        )
        .expect("exporter");
        let publication = exporter.stage(pdata(&lease, lease.fencing_epoch)).await.expect("stage");
        tokio::time::timeout(std::time::Duration::from_secs(3), publication.wait())
            .await
            .expect("age flush")
            .expect("publication");
    }

    /// Scenario: Admitted work temporarily exhausts publication capacity.
    /// Guarantees: Work waits without a NACK, and cancellation refunds partial reservations.
    #[tokio::test]
    async fn temporary_capacity_waits_and_cancellation_refunds_reservations() {
        let capacity = Arc::new(Capacity::new(BatchCapacity {
            bytes: 16,
            rows: 2,
            batches: 2,
        }));
        let held = capacity.acquire(1, 8).await.expect("first reservation");
        let mut queued = Box::pin(capacity.acquire(2, 8));
        assert!(queued.as_mut().now_or_never().is_none());
        assert_eq!(capacity.bytes.available_permits(), 0);
        drop(queued);
        assert_eq!(capacity.bytes.available_permits(), 8);
        assert_eq!(capacity.rows.available_permits(), 1);
        let mut queued = Box::pin(capacity.acquire(1, 16));
        assert!(queued.as_mut().now_or_never().is_none());
        drop(held);
        let resumed = tokio::time::timeout(std::time::Duration::from_secs(2), queued)
            .await
            .expect("reservation resumed")
            .expect("reservation succeeded");
        assert_eq!(capacity.bytes.available_permits(), 0);
        drop(resumed);
        assert_eq!(capacity.bytes.available_permits(), 16);
        assert_eq!(capacity.rows.available_permits(), 2);
        assert_eq!(capacity.batches.available_permits(), 2);
    }

    /// Scenario: A batch exceeds the configured row capacity.
    /// Guarantees: Admission fails explicitly, refunds partial capacity, and never creates a data file.
    #[tokio::test]
    async fn exhausted_capacity_is_explicit_and_refunded() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let mut policy = FileBatchConfig::default();
        policy.global_capacity.rows = 1;
        policy.per_table_capacity.rows = 1;
        policy.max_rows_per_flush = 1;
        let exporter = GenericDeltaExporter::with_batching(
            directory.path().to_path_buf(),
            store as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions::default(),
            policy.clone(),
            Arc::new(RuntimeMetrics::new(holder)),
        )
        .expect("exporter");
        let result = exporter.stage(pdata(&lease, lease.fencing_epoch)).await;
        assert!(matches!(result, Err(ExportFailure::Transient(message)) if message.contains("capacity exhausted")));
        assert_eq!(
            exporter.backend.capacity.bytes.available_permits(),
            policy.global_capacity.bytes as usize
        );
        assert!(!directory.path().join("demo/generated/events/_delta_log").exists());
    }

    /// Scenario: The authoritative lease is re-issued to the same collector with a new epoch while a
    /// partial old-epoch file is retained.
    /// Guarantees: The new-epoch batch is refused transiently instead of mixing epochs in one file, the
    /// retained old-epoch batch fails its commit fence transiently, and its retry is rebound to the new
    /// epoch and published.
    #[tokio::test]
    async fn ownership_transition_cannot_mix_pending_batches() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let metrics = Arc::new(RuntimeMetrics::new(holder));
        let exporter = GenericDeltaExporter::with_batching(
            directory.path().to_path_buf(),
            Arc::clone(&store) as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions::default(),
            FileBatchConfig {
                max_age: std::time::Duration::from_secs(60),
                ..Default::default()
            },
            Arc::clone(&metrics),
        )
        .expect("exporter");
        let publication = exporter
            .stage(pdata(&lease, lease.fencing_epoch))
            .await
            .expect("old epoch");
        let next = reissue_lease(&store, &lease, holder).await;
        let mixed = exporter.stage(pdata(&next, next.fencing_epoch)).await;
        assert!(matches!(mixed, Err(ExportFailure::Transient(message)) if message.contains("lease changed")));

        exporter.backend.flush_pending().await.expect("flush");
        let error = tokio::time::timeout(std::time::Duration::from_secs(3), publication.wait())
            .await
            .expect("bounded publication")
            .expect_err("old epoch fenced");
        assert!(!error.permanent());
        assert!(!delta_version_exists(directory.path(), 1));

        let retry = exporter
            .stage(pdata(&lease, lease.fencing_epoch))
            .await
            .expect("rebound retry");
        exporter.backend.flush_pending().await.expect("flush");
        tokio::time::timeout(std::time::Duration::from_secs(3), retry.wait())
            .await
            .expect("bounded publication")
            .expect("rebound publication");
        assert!(delta_version_exists(directory.path(), 1));
        assert!(
            metrics
                .prometheus()
                .contains("arrowcol_dataflow_rebound_batches_total 1\n")
        );
    }

    /// Scenario: Same-shaped batches have different catalog schema versions.
    /// Guarantees: The catalog boundary splits files, and draining publishes the residual group.
    #[tokio::test]
    async fn schema_version_transition_splits_files() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let exporter = GenericDeltaExporter::with_batching(
            directory.path().to_path_buf(),
            store as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions::default(),
            FileBatchConfig {
                max_age: std::time::Duration::from_secs(60),
                ..Default::default()
            },
            Arc::new(RuntimeMetrics::new(holder)),
        )
        .expect("exporter");
        let first = exporter.stage(pdata(&lease, lease.fencing_epoch)).await.expect("first");
        let second = exporter
            .stage(versioned_pdata(
                &lease,
                lease.fencing_epoch,
                SchemaVersion::new(2).expect("version"),
            ))
            .await
            .expect("second");
        tokio::time::timeout(std::time::Duration::from_secs(3), first.wait())
            .await
            .expect("schema boundary")
            .expect("first publication");
        exporter.backend.start_draining().await;
        tokio::time::timeout(std::time::Duration::from_secs(3), second.wait())
            .await
            .expect("drain")
            .expect("second publication");
        let files = std::fs::read_dir(directory.path().join("demo/generated/events"))
            .expect("files")
            .map(|entry| entry.expect("entry").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "parquet"))
            .count();
        assert_eq!(files, 2);
    }

    /// Scenario: Multiple batches in one grouped file lose their authoritative lease before the commit
    /// boundary.
    /// Guarantees: Every completion fails transiently, capacity is refunded, and no output is reported
    /// as published.
    #[tokio::test]
    async fn grouped_publication_failure_settles_all_batches() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let metrics = Arc::new(RuntimeMetrics::new(holder));
        let exporter = GenericDeltaExporter::with_batching(
            directory.path().to_path_buf(),
            Arc::clone(&store) as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions::default(),
            FileBatchConfig {
                max_batches_per_flush: 3,
                max_age: std::time::Duration::from_secs(60),
                ..Default::default()
            },
            Arc::clone(&metrics),
        )
        .expect("exporter");
        let first = exporter.stage(pdata(&lease, lease.fencing_epoch)).await.expect("first");
        let second = exporter
            .stage(pdata(&lease, lease.fencing_epoch))
            .await
            .expect("second");
        reissue_lease(&store, &lease, CollectorInstanceId::new()).await;
        exporter.backend.flush_pending().await.expect("flush");
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            assert!(
                first
                    .wait()
                    .await
                    .expect_err("first fenced")
                    .to_string()
                    .contains("routing lease")
            );
            assert!(
                second
                    .wait()
                    .await
                    .expect_err("second fenced")
                    .to_string()
                    .contains("routing lease")
            );
        })
        .await
        .expect("bounded failures");
        assert_eq!(exporter.backend.capacity.batches.available_permits(), 4096);
        assert!(metrics.prometheus().contains("arrowcol_delta_files_total 0\n"));
        assert!(metrics.prometheus().contains("arrowcol_delta_failures_total 1\n"));
    }

    /// Scenario: A batch this process admitted under an older lease is retried after the lease was
    /// re-issued to this process with a new lineage and epoch.
    /// Guarantees: The exporter rebinds the envelope to the current lease, publishes it, and counts a
    /// rebind without counting a predecessor replay.
    #[tokio::test]
    async fn same_process_older_lease_is_rebound() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let older = lease(&store, holder).await;
        reissue_lease(&store, &older, holder).await;
        let metrics = Arc::new(RuntimeMetrics::new(holder));
        let exporter = fast_exporter(directory.path(), &store, &metrics);

        exporter
            .export(pdata(&older, older.fencing_epoch))
            .await
            .expect("rebound publication");
        assert!(delta_version_exists(directory.path(), 1));
        let exposition = metrics.prometheus();
        assert!(exposition.contains("arrowcol_dataflow_rebound_batches_total 1\n"));
        assert!(exposition.contains("arrowcol_dataflow_replayed_batches_total 0\n"));
    }

    /// Scenario: After a crash, the restarted process replays a batch whose envelope names its dead
    /// predecessor's lease, and the lease was handed to this successor with the same instance key.
    /// Guarantees: The batch is rebound to the successor's lease and published; the replay is counted
    /// separately and never subtracted from the successor's own durable backlog.
    #[tokio::test]
    async fn predecessor_replay_with_same_instance_key_is_rebound() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let predecessor = CollectorInstanceId::new();
        let successor = CollectorInstanceId::new();
        let predecessor_lease = lease(&store, predecessor).await;
        reissue_lease(&store, &predecessor_lease, successor).await;
        let metrics = durable_metrics(successor, Some("test"));
        metrics.durable_backlog_admitted(&table(), 2, 64);
        let exporter = fast_exporter(directory.path(), &store, &metrics);

        exporter
            .export(pdata(&predecessor_lease, predecessor_lease.fencing_epoch))
            .await
            .expect("replayed publication");
        assert!(delta_version_exists(directory.path(), 1));
        let exposition = metrics.prometheus();
        assert!(exposition.contains("arrowcol_dataflow_replayed_batches_total 1\n"));
        assert!(exposition.contains("arrowcol_dataflow_rebound_batches_total 1\n"));
        assert!(exposition.contains("arrowcol_dataflow_durable_backlog_batches 1\n"));
        assert!(exposition.contains("arrowcol_dataflow_durable_released_batches_total 0\n"));
    }

    /// Scenario: A replayed batch was admitted by a collector with a different instance key, or this
    /// process has not learned its own instance key.
    /// Guarantees: The exporter refuses it transiently, never permanently, so the durable buffer keeps
    /// retrying it, and no Delta output appears.
    #[tokio::test]
    async fn unrelated_or_unknown_instance_key_is_deferred() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let predecessor = CollectorInstanceId::new();
        let successor = CollectorInstanceId::new();
        let predecessor_lease = lease(&store, predecessor).await;
        reissue_lease(&store, &predecessor_lease, successor).await;

        let metrics = durable_metrics(successor, Some("another-deployment"));
        let exporter = fast_exporter(directory.path(), &store, &metrics);
        let error = exporter
            .stage(pdata(&predecessor_lease, predecessor_lease.fencing_epoch))
            .await
            .err()
            .expect("different instance key");
        assert!(matches!(&error, ExportFailure::Transient(message) if message.contains("instance key")));
        assert!(!error.permanent());
        assert!(
            metrics
                .prometheus()
                .contains("arrowcol_dataflow_replay_deferred_total 1\n")
        );

        let unknown = durable_metrics(successor, None);
        let exporter = fast_exporter(directory.path(), &store, &unknown);
        let error = exporter
            .stage(pdata(&predecessor_lease, predecessor_lease.fencing_epoch))
            .await
            .err()
            .expect("unknown instance key");
        assert!(!error.permanent());
        assert!(!directory.path().join("demo/generated/events/_delta_log").exists());
    }

    /// Scenario: A replayed batch's lease is still held by its dead predecessor, then moves to an
    /// unrelated collector, and is finally handed to this process.
    /// Guarantees: The exporter never publishes under another collector's lease and never drops the
    /// batch: it defers transiently until this process holds the lease, then rebinds and publishes it.
    #[tokio::test]
    async fn lease_held_elsewhere_is_deferred_until_succession() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let predecessor = CollectorInstanceId::new();
        let successor = CollectorInstanceId::new();
        let predecessor_lease = lease(&store, predecessor).await;
        let metrics = durable_metrics(successor, Some("test"));
        let mut exporter = fast_exporter(directory.path(), &store, &metrics);
        Arc::get_mut(&mut exporter.backend)
            .expect("exclusive backend")
            .leases
            .set_ttl(std::time::Duration::ZERO);

        for holder in [None, Some(CollectorInstanceId::new())] {
            let current = match holder {
                None => predecessor_lease.clone(),
                Some(other) => reissue_lease(&store, &predecessor_lease, other).await,
            };
            let error = exporter
                .stage(pdata(&predecessor_lease, predecessor_lease.fencing_epoch))
                .await
                .err()
                .expect("lease held elsewhere");
            assert!(
                matches!(&error, ExportFailure::Transient(message) if message.contains("held by another collector"))
            );
            assert!(!error.permanent());
            assert_ne!(current.holder_instance_id, successor);
        }
        assert!(!directory.path().join("demo/generated/events/_delta_log").exists());

        let unrelated = store
            .load_view(now())
            .await
            .expect("view")
            .leases
            .get(&LeaseKey::new(ImplementationPool::Dataflow, table()))
            .cloned()
            .expect("lease");
        reissue_lease(&store, &unrelated, successor).await;
        exporter
            .export(pdata(&predecessor_lease, predecessor_lease.fencing_epoch))
            .await
            .expect("published after succession");
        assert!(delta_version_exists(directory.path(), 1));
        let exposition = metrics.prometheus();
        assert!(exposition.contains("arrowcol_dataflow_replay_deferred_total 2\n"));
        assert!(exposition.contains("arrowcol_dataflow_replayed_batches_total 3\n"));
    }

    /// Scenario: Batches this process durably admitted are published, refused permanently, or refused
    /// transiently.
    /// Guarantees: Publication and permanent refusal remove a batch from the durable backlog, while a
    /// transient refusal keeps it counted because the durable buffer retries it.
    #[tokio::test]
    async fn durable_backlog_is_released_on_final_outcomes() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let metrics = durable_metrics(holder, Some("test"));
        let backlog = |metrics: &RuntimeMetrics| {
            serde_json::to_value(metrics.pressure().expect("pressure")).expect("pressure JSON")
                ["native_quiver_buffered_batches"]
                .clone()
        };
        metrics.durable_backlog_admitted(&table(), 2, 32);
        let exporter = fast_exporter(directory.path(), &store, &metrics);
        exporter
            .export(pdata(&lease, lease.fencing_epoch))
            .await
            .expect("publication");
        assert_eq!(backlog(&metrics), 0);

        for _ in 0..2 {
            metrics.durable_backlog_admitted(&table(), 2, 32);
        }
        let release = |metrics: &Arc<RuntimeMetrics>| BacklogRelease {
            metrics: Arc::clone(metrics),
            table: table(),
            rows: 2,
            bytes: 32,
        };
        release(&metrics).settle(Err(&ExportFailure::Transient("retry".to_owned())));
        assert_eq!(backlog(&metrics), 2);
        release(&metrics).settle(Err(&ExportFailure::Permanent("refused".to_owned())));
        assert_eq!(backlog(&metrics), 1);
    }

    /// Scenario: More batches for an already-open table arrive with the current lease within the view TTL.
    /// Guarantees: They are bound from the cached authoritative lease without another routing read per batch.
    #[tokio::test]
    async fn hot_path_reuses_fresh_routing_view() {
        let directory = tempfile::tempdir().expect("directory");
        let store = Arc::new(MemoryRoutingStore::default());
        let holder = CollectorInstanceId::new();
        let lease = lease(&store, holder).await;
        let exporter = GenericDeltaExporter::with_batching(
            directory.path().to_path_buf(),
            Arc::clone(&store) as Arc<dyn RoutingStore>,
            arrow_deltalake::commit::CommitOptions::default(),
            FileBatchConfig {
                max_age: std::time::Duration::from_secs(60),
                ..Default::default()
            },
            Arc::new(RuntimeMetrics::new(holder)),
        )
        .expect("exporter");
        let mut publications = vec![exporter.stage(pdata(&lease, lease.fencing_epoch)).await.expect("first")];
        let before = store.operation_counts().load_view;
        for _ in 0..3 {
            publications.push(exporter.stage(pdata(&lease, lease.fencing_epoch)).await.expect("stage"));
        }
        assert_eq!(store.operation_counts().load_view, before);
        exporter.backend.start_draining().await;
        for publication in publications {
            tokio::time::timeout(std::time::Duration::from_secs(3), publication.wait())
                .await
                .expect("bounded drain")
                .expect("publication");
        }
    }

    /// Scenario: Concurrent pipeline cores create exporters for the same root.
    /// Guarantees: They share one backend, and incompatible policies are rejected rather than silently reused.
    #[test]
    fn concurrent_exporters_share_only_matching_configuration() {
        let directory = tempfile::tempdir().expect("directory");
        let config = DeltaExporterConfig {
            base_path: directory.path().to_path_buf(),
            routing: crate::config::RoutingStoreConfig::Memory,
            commit: crate::config::CommitConfig::default(),
            batching: FileBatchConfig::default(),
            authority_timeout: std::time::Duration::from_secs(2),
        };
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let metrics = Arc::new(RuntimeMetrics::new(CollectorInstanceId::new()));
        let exporters = std::thread::scope(|scope| {
            (0..4)
                .map(|_| {
                    let config = config.clone();
                    let barrier = Arc::clone(&barrier);
                    let metrics = Arc::clone(&metrics);
                    scope.spawn(move || {
                        barrier.wait();
                        GenericDeltaExporter::shared(config, metrics).expect("shared")
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|thread| thread.join().expect("thread"))
                .collect::<Vec<_>>()
        });
        assert!(
            exporters
                .iter()
                .all(|exporter| Arc::ptr_eq(&exporter.backend, &exporters[0].backend))
        );
        let mut different = config;
        different.batching.max_age = std::time::Duration::from_secs(1);
        assert!(GenericDeltaExporter::shared(different, metrics).is_err());
    }

    /// Scenario: A grouped file task disappears after accounting begins.
    /// Guarantees: Pending publication pressure is released and health reports the failure.
    #[test]
    fn cancelled_publication_releases_accounting_and_marks_unhealthy() {
        let metrics = RuntimeMetrics::new(CollectorInstanceId::new());
        let identity = table();
        metrics.publication_started(&identity);
        {
            let _accounting = PublicationAccounting {
                metrics: &metrics,
                identity: &identity,
                rows: 2,
                bytes: 32,
                finished: false,
            };
        }
        assert!(!metrics.healthy());
        assert!(metrics.prometheus().contains("arrowcol_delta_failures_total 1\n"));
        assert_eq!(
            metrics
                .pressure()
                .expect("pressure")
                .rich
                .pending_commits
                .current()
                .get(),
            0
        );
    }

    /// Scenario: Repository SQL Server, Hive Metastore, routing leases, and local Delta are live.
    /// Guarantees: Strict catalog validation and authoritative fencing precede generic Delta publication.
    #[tokio::test]
    #[ignore = "requires projects/arrowcol-infra"]
    async fn live_sql_hive_delta() {
        if std::env::var("ARROWCOL_RUN_LIVE_TESTS").as_deref() != Ok("1") {
            return;
        }
        use crate::{CatalogConfig, HiveMetastoreConnectionConfig, RoutingStoreConfig, SqlServerConnectionConfig};
        use arrow_array::Int64Array;
        use arrowcol_catalog::{FlightSqlDdlHandler, SchemaValidator};
        use arrowcol_protocol::{ClaimedSchemaFingerprint, fingerprint_schema};

        let repository_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let password_file = std::env::var("ARROWCOL_SQL_PASSWORD_FILE")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| repository_root.join("projects/arrowcol-infra/secrets/arrowcol-app-password.txt"));
        let sql = SqlServerConnectionConfig {
            host: std::env::var("ARROWCOL_SQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned()),
            port: std::env::var("ARROWCOL_SQL_PORT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(14_333),
            database: "arrowcol".to_owned(),
            username: "arrowcol_app".to_owned(),
            password_file,
            trust_server_certificate: true,
            connect_timeout: std::time::Duration::from_secs(15),
            command_timeout: std::time::Duration::from_secs(30),
        };
        let catalog_config = CatalogConfig::SqlHive {
            application_database: Box::new(sql.clone()),
            hive_metastore: Box::new(HiveMetastoreConnectionConfig {
                host: std::env::var("ARROWCOL_HIVE_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned()),
                port: std::env::var("ARROWCOL_HIVE_PORT")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(9_083),
                warehouse_root: "file:///opt/hive/data/warehouse".to_owned(),
                owner: "arrowcol".to_owned(),
                connect_timeout: std::time::Duration::from_secs(10),
            }),
            timestamp_timezone: "UTC".to_owned(),
        };
        let (catalog_store, control) = catalog_config.start().await.expect("start live catalog");
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let table_name = format!("dfe_events_{}", &suffix[..12]);
        let completion = control
            .execute_ddl(arrowcol_catalog::FlightSqlDdlRequest::new(format!(
                "CREATE TABLE arrowcol.live.{table_name} (id BIGINT NOT NULL, label STRING)"
            )))
            .await
            .expect("live DDL");
        let schema = catalog_store
            .current_schema(completion.identity())
            .await
            .expect("current schema")
            .expect("registered schema");
        let arrow_schema = Arc::new(schema.arrow_schema().expect("Arrow schema"));
        SchemaValidator::new(Arc::clone(&catalog_store))
            .validate(
                completion.identity(),
                ClaimedSchemaFingerprint::new(fingerprint_schema(arrow_schema.as_ref())),
                arrow_schema.as_ref(),
            )
            .await
            .expect("exact live validation");

        let routing_config = RoutingStoreConfig::SqlServer { connection: sql };
        let routing = routing_config.build().expect("routing store");
        let holder = CollectorInstanceId::new();
        let observed = now();
        routing
            .register_collector(&CollectorRegistration {
                instance_id: holder,
                instance_key: "dfe-live-e2e".to_owned(),
                implementation_pool: ImplementationPool::Dataflow,
                flight_endpoint: "127.0.0.1:32010".parse().expect("address"),
                catalog_capable: true,
                max_concurrent_streams: PositiveMeasurement::new(4).expect("positive"),
                started_at_unix_ms: observed,
                heartbeat_at_unix_ms: observed,
                lease_expires_at_unix_ms: UnixMillis::new(observed.get() + 60_000).expect("expiry"),
            })
            .await
            .expect("live registration");
        let zero =
            PressureGauge::new(Measurement::ZERO, PositiveMeasurement::new(4).expect("positive")).expect("gauge");
        routing
            .publish_pressure(&RichPressureSnapshot {
                container_memory: None,
                effective_cpu_millicores: None,
                instance_id: holder,
                observed_at_unix_ms: observed,
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
            .expect("live pressure");
        let lease = routing
            .claim_lease(&LeaseClaim {
                key: LeaseKey::new(ImplementationPool::Dataflow, completion.identity().clone()),
                candidate_instance_id: holder,
                now_unix_ms: observed,
                expires_at_unix_ms: UnixMillis::new(observed.get() + 60_000).expect("expiry"),
                minimum_snapshot_at_unix_ms: observed,
            })
            .await
            .expect("live lease");

        let output = repository_root.join("projects/arrowcol-dataflow-rust/.temp/live-sql-hive-delta");
        let _ = std::fs::remove_dir_all(&output);
        std::fs::create_dir_all(&output).expect("create output");
        let delta_log = output
            .join("arrowcol/live")
            .join(&table_name)
            .join("_delta_log/00000000000000000001.json");
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(output.clone());
        let exporter = GenericDeltaExporter::with_store(
            output,
            Arc::clone(&routing),
            arrow_deltalake::commit::CommitOptions {
                max_files_per_commit: 1,
                max_pending_files: 2,
                ..Default::default()
            },
            Arc::new(RuntimeMetrics::new(holder)),
        )
        .expect("live exporter");
        let batch = arrow_array::RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("live")])) as ArrayRef,
            ],
        )
        .expect("live batch");
        let records = encode_record_batch(
            ArrowcolBatchEnvelope {
                table: completion.identity().clone(),
                source_schema_version: schema.version(),
                target_schema_version: schema.version(),
                lease_id: lease.lease_id,
                holder_instance_id: lease.holder_instance_id,
                fencing_token: lease.fencing_epoch,
                instance_key: "dfe-live-e2e".to_owned(),
            },
            batch,
        )
        .expect("live pdata");
        exporter
            .export(OtapPdata::new_todo_context(
                otel_arrow_dfe_pdata::OtapPayload::from_pdata_arrow(records),
            ))
            .await
            .expect("live Delta export");
        assert!(delta_log.exists());
    }
}
