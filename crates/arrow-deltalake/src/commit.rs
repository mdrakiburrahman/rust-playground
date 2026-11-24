//! Bounded, process-shared, per-table Delta publication.

use crate::delta::{
    AppendResult, CoalescedCommitGroup, DeltaFileWriter, DeltaTable, PublicationFence, PublicationFenceError,
    StagedAppend,
};
use anyhow::{Context, Result, bail};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, Weak};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::time::Instant;

/// Limits for eligible files awaiting a shared transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitOptions {
    pub txn_lookup_mode: crate::TxnLookupMode,
    pub batch_window: Duration,
    pub max_files_per_commit: usize,
    pub max_pending_files: usize,
}

impl Default for CommitOptions {
    fn default() -> Self {
        Self {
            batch_window: Duration::from_secs(10),
            max_files_per_commit: 64,
            max_pending_files: 256,
            txn_lookup_mode: Default::default(),
        }
    }
}

impl CommitOptions {
    /// Validate capacities before starting a worker.
    ///
    /// # Errors
    /// Rejects zero, inconsistent, or unrepresentable capacities/windows.
    pub fn validate(&self) -> Result<()> {
        if self.batch_window.is_zero()
            || Instant::now().checked_add(self.batch_window).is_none()
            || self.max_files_per_commit == 0
            || self.max_pending_files < self.max_files_per_commit
            || self.max_pending_files > Semaphore::MAX_PERMITS
        {
            bail!("invalid Delta commit limits: positive window and 0 < batch files <= pending capacity required");
        }
        Ok(())
    }
}

struct RegistryEntry {
    owner: Weak<BatchedTable>,
    stopped: watch::Receiver<bool>,
}

// Only table metadata crosses cores: sharing this owner is necessary because
// every core appends to the same transaction log. Bulk Arrow batches stay local.
static TABLES: LazyLock<Mutex<HashMap<PathBuf, RegistryEntry>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// One bounded commit worker, shared by all writers of a canonical local table.
pub struct BatchedTable {
    options: CommitOptions,
    publication_fence: Option<Arc<dyn PublicationFence>>,
    sender: mpsc::Sender<Request>,
    slots: Arc<Semaphore>,
    flush: watch::Sender<u64>,
    draining: Arc<AtomicBool>,
    preparation: OnceLock<Arc<DeltaTable>>,
}

struct Publication {
    appends: Vec<StagedAppend>,
    identities: Vec<(String, i64, i64)>,
    reply: oneshot::Sender<PublicationReply>,
    _slot: OwnedSemaphorePermit,
    force: bool,
}

impl Publication {
    fn identity_names(&self) -> impl Iterator<Item = &str> {
        self.identities.iter().map(|(id, _, _)| id.as_str()).chain(
            self.appends
                .iter()
                .filter_map(|append| append.publication.as_ref().map(|(id, _)| id.as_str())),
        )
    }
}

enum Request {
    Ready(oneshot::Sender<std::result::Result<Arc<DeltaTable>, CommitReceiptError>>),
    NewFile(
        SchemaRef,
        oneshot::Sender<std::result::Result<DeltaFileWriter, CommitReceiptError>>,
    ),
    NewPublication(
        SchemaRef,
        String,
        i64,
        i64,
        oneshot::Sender<std::result::Result<DeltaFileWriter, CommitReceiptError>>,
    ),
    NewPartitionedPublication(
        RecordBatch,
        String,
        i64,
        i64,
        oneshot::Sender<std::result::Result<Vec<(DeltaFileWriter, RecordBatch)>, CommitReceiptError>>,
    ),
    LookupPublications(
        Vec<(String, i64)>,
        oneshot::Sender<std::result::Result<Vec<bool>, CommitReceiptError>>,
    ),
    Publish(Box<Publication>),
}

/// A submitted file's eventual durable Delta version.
pub struct CommitReceipt(oneshot::Receiver<PublicationReply>);

/// One atomic coalesced publication's durable Delta version and output files.
pub struct GroupCommitReceipt(oneshot::Receiver<PublicationReply>);

/// Successful result for every physical file in one atomic publication group.
#[derive(Debug)]
pub struct GroupAppendResult {
    /// Shared evidence for the entire physical publication call.
    pub evidence: Arc<crate::PublicationEvidence>,
    pub version: u64,
    pub files: Vec<crate::CompletedFile>,
    pub commit_duration: Option<Duration>,
    pub file_observations: Vec<crate::CompletedFileObservation>,
}

/// Staged physical outputs and every constituent logical identity for one commit.
pub struct PreparedPublicationGroup {
    pub appends: Vec<StagedAppend>,
    /// Time spent preparing the coalesced inputs and partition/file plan.
    pub preparation_build: Duration,
    /// Time spent sizing sorted chunks during coalesced preparation.
    pub preparation_sort: Duration,
    /// Identities still pending after the preparation-time Delta replay filter.
    pub identities: Vec<(String, i64, i64)>,
    /// Identities already committed before any group encoding or staging.
    pub committed_identities: Vec<(String, i64, i64)>,
}

impl Drop for PreparedPublicationGroup {
    fn drop(&mut self) {
        crate::delta::reclaim_abandoned_group(std::mem::take(&mut self.appends));
    }
}

/// Structured failure delivered to every waiter in a failed publication batch.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommitReceiptError {
    /// The caller-provided ownership check rejected the commit.
    PublicationFenced(PublicationFenceError),
    /// Delta preparation or commit failed.
    CommitFailed { message: String },
    /// The worker disappeared without delivering the batch result.
    WorkerStopped,
    /// A replay race committed only part of an atomic coalesced publication group.
    RetryablePublicationGroup { committed: usize, total: usize },
    /// Preparation found that the group needs more files than one atomic commit permits.
    GroupCapacityExceeded { required_files: usize, max_files: usize },
}

/// Failure of this publication attempt. Admission and stopped-worker failures have no evidence.
#[derive(Clone, Debug)]
pub struct PublicationFailure {
    pub error: CommitReceiptError,
    pub evidence: Option<Arc<crate::PublicationEvidence>>,
}

type PublicationReply = std::result::Result<GroupAppendResult, PublicationFailure>;

impl From<CommitReceiptError> for PublicationFailure {
    fn from(error: CommitReceiptError) -> Self {
        Self { error, evidence: None }
    }
}

impl PublicationFailure {
    fn from_anyhow(error: anyhow::Error) -> Self {
        let evidence = error
            .downcast_ref::<crate::PublicationEvidence>()
            .map(|e| Arc::new(e.clone()));
        Self {
            evidence,
            error: CommitReceiptError::from_anyhow(error),
        }
    }
}

impl CommitReceiptError {
    fn commit_failed(message: impl Into<String>) -> Self {
        Self::CommitFailed {
            message: message.into(),
        }
    }

    fn from_anyhow(error: anyhow::Error) -> Self {
        if let Some(error) = error.downcast_ref::<PublicationFenceError>() {
            Self::PublicationFenced(error.clone())
        } else if let Some(error) = error.downcast_ref::<crate::delta::PartialPublicationGroup>() {
            Self::RetryablePublicationGroup {
                committed: error.committed,
                total: error.total,
            }
        } else if let Some(error) = error.downcast_ref::<crate::delta::CoalescedGroupCapacityExceeded>() {
            Self::GroupCapacityExceeded {
                required_files: error.required_files,
                max_files: error.max_files,
            }
        } else {
            Self::commit_failed(format!("{error:#}"))
        }
    }

    fn is_retryable(&self) -> bool {
        matches!(self, Self::RetryablePublicationGroup { .. })
    }
}

impl Display for CommitReceiptError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::PublicationFenced(error) => Display::fmt(error, formatter),
            Self::CommitFailed { message } => write!(formatter, "Delta commit failed: {message}"),
            Self::WorkerStopped => formatter.write_str("Delta commit worker stopped before completion"),
            Self::RetryablePublicationGroup { committed, total } => write!(
                formatter,
                "coalesced publication group is partially committed ({committed}/{total} identities); rebuild the group and retry"
            ),
            Self::GroupCapacityExceeded {
                required_files,
                max_files,
            } => write!(
                formatter,
                "coalesced publication group requires at least {required_files} files, exceeding the atomic commit capacity {max_files}; increase max_files_per_commit or reduce the publication's partition count"
            ),
        }
    }
}

impl Error for CommitReceiptError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::PublicationFenced(error) => Some(error),
            Self::CommitFailed { .. }
            | Self::WorkerStopped
            | Self::RetryablePublicationGroup { .. }
            | Self::GroupCapacityExceeded { .. } => None,
        }
    }
}

impl CommitReceipt {
    /// Wait for publication, not merely queue admission.
    ///
    /// # Errors
    /// Returns worker failure, conflict, or uncertain commit errors.
    pub async fn wait(self) -> Result<AppendResult> {
        self.wait_typed().await.map_err(anyhow::Error::new)
    }

    /// Wait for publication while preserving structured fence failures.
    ///
    /// # Errors
    /// Returns a typed fence, commit, or worker-stopped error.
    pub async fn wait_typed(self) -> std::result::Result<AppendResult, CommitReceiptError> {
        self.wait_with_evidence().await.map_err(|failure| failure.error)
    }

    /// # Errors
    /// Returns both the original worker error classification and any publication evidence.
    pub async fn wait_with_evidence(self) -> std::result::Result<AppendResult, PublicationFailure> {
        let result = self
            .0
            .await
            .unwrap_or_else(|_| Err(CommitReceiptError::WorkerStopped.into()))?;
        let mut files = result.files.into_iter();
        let file = files
            .next()
            .ok_or_else(|| CommitReceiptError::commit_failed("single-file commit returned no result"))?;
        if files.next().is_some() {
            return Err(CommitReceiptError::commit_failed("single-file commit returned multiple files").into());
        }
        Ok(AppendResult {
            evidence: result.evidence,
            version: result.version,
            file,
            commit_duration: result.commit_duration,
            file_observations: result.file_observations,
        })
    }
}

impl GroupCommitReceipt {
    /// Wait for every physical output file in the atomic publication group.
    ///
    /// # Errors
    /// Returns worker failures, replay races, or uncertain commit errors.
    pub async fn wait(self) -> Result<GroupAppendResult> {
        self.wait_typed().await.map_err(anyhow::Error::new)
    }

    /// Wait for the group while preserving structured retryable publication races.
    ///
    /// # Errors
    /// Returns typed worker, replay-race or commit failures.
    pub async fn wait_typed(self) -> std::result::Result<GroupAppendResult, CommitReceiptError> {
        self.wait_with_evidence().await.map_err(|failure| failure.error)
    }

    /// # Errors
    /// Returns both the original worker error classification and any publication evidence.
    pub async fn wait_with_evidence(self) -> std::result::Result<GroupAppendResult, PublicationFailure> {
        self.0
            .await
            .unwrap_or_else(|_| Err(CommitReceiptError::WorkerStopped.into()))
    }
}

impl BatchedTable {
    /// Share local table ownership across cores in this process.
    ///
    /// # Errors
    /// Rejects conflicting options, another process's writer, or filesystem/worker errors.
    pub async fn open(root: impl AsRef<Path>, options: CommitOptions) -> Result<Arc<Self>> {
        Self::open_inner(root, options, None).await
    }

    /// Share local table ownership with a check at every data commit boundary.
    ///
    /// Reopening an in-process owner requires the same `Arc` fence instance.
    ///
    /// # Errors
    /// Rejects conflicting options/fences, another writer, or filesystem/worker errors.
    pub async fn open_with_fence(
        root: impl AsRef<Path>,
        options: CommitOptions,
        publication_fence: Arc<dyn PublicationFence>,
    ) -> Result<Arc<Self>> {
        Self::open_inner(root, options, Some(publication_fence)).await
    }

    /// Share a fenced publication coordinator for a local path or canonical ADLS URI.
    ///
    /// # Errors
    /// Rejects inconsistent options, invalid authority or worker startup failures.
    pub async fn open_uri_with_fence(
        root: &str,
        options: CommitOptions,
        publication_fence: Arc<dyn PublicationFence>,
    ) -> Result<Arc<Self>> {
        Self::open_inner(Path::new(root), options, Some(publication_fence)).await
    }

    async fn open_inner(
        root: impl AsRef<Path>,
        options: CommitOptions,
        publication_fence: Option<Arc<dyn PublicationFence>>,
    ) -> Result<Arc<Self>> {
        options.validate()?;
        let root = if root.as_ref().to_str().is_some_and(|root| root.contains("://")) {
            root.as_ref().to_path_buf()
        } else {
            tokio::fs::create_dir_all(root.as_ref()).await?;
            tokio::fs::canonicalize(root).await?
        };
        loop {
            let (owner, mut stopped) = {
                let mut registry = TABLES.lock().map_err(|_| anyhow::anyhow!("Delta registry poisoned"))?;
                if let Some(entry) = registry.get(&root) {
                    if let Some(owner) = entry.owner.upgrade() {
                        if owner.options != options
                            || !same_publication_fence(&owner.publication_fence, &publication_fence)
                        {
                            bail!(
                                "conflicting Delta commit options or publication fence for {}",
                                root.display()
                            );
                        }
                        (Some(owner), entry.stopped.clone())
                    } else {
                        (None, entry.stopped.clone())
                    }
                } else {
                    let (sender, receiver) = mpsc::channel(options.max_pending_files);
                    let (flush, flush_rx) = watch::channel(0);
                    let (done, stopped) = watch::channel(false);
                    let owner = Arc::new(Self {
                        slots: Arc::new(Semaphore::new(options.max_pending_files)),
                        options: options.clone(),
                        publication_fence: publication_fence.clone(),
                        sender,
                        flush,
                        draining: Arc::new(AtomicBool::new(false)),
                        preparation: OnceLock::new(),
                    });
                    let worker_root = root.clone();
                    let worker_options = options.clone();
                    let worker_fence = publication_fence.clone();
                    let draining = Arc::clone(&owner.draining);
                    std::thread::Builder::new().name("delta-commit".into()).spawn(move || {
                        run_worker(&worker_root, worker_options, receiver, flush_rx, draining, worker_fence);
                        // Release the filesystem owner before permitting a replacement.
                        if let Ok(mut registry) = TABLES.lock()
                            && registry
                                .get(&worker_root)
                                .is_some_and(|entry| entry.owner.strong_count() == 0)
                        {
                            registry.remove(&worker_root);
                        }
                        done.send_replace(true);
                    })?;
                    registry.insert(
                        root.clone(),
                        RegistryEntry {
                            owner: Arc::downgrade(&owner),
                            stopped: stopped.clone(),
                        },
                    );
                    (Some(owner), stopped)
                }
            };
            if let Some(owner) = owner {
                owner.ready().await?;
                return Ok(owner);
            }
            if !*stopped.borrow() {
                stopped
                    .changed()
                    .await
                    .context("Delta worker lost shutdown notification")?;
            }
        }
    }

    async fn ready(&self) -> Result<()> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(Request::Ready(reply))
            .await
            .context("Delta worker stopped")?;
        let table = receive
            .await
            .context("Delta worker stopped during initialization")?
            .map_err(anyhow::Error::new)?;
        self.preparation.get_or_init(|| table);
        Ok(())
    }

    /// Read current adaptive scheduling policy on the table's IO worker.
    ///
    /// # Errors
    /// Returns metadata, configuration or worker failures without preparing a publication.
    pub async fn adaptive_file_config(
        &self,
        defaults: crate::sizing::AdaptiveFileSizeConfig,
    ) -> Result<crate::sizing::AdaptiveFileSizeConfig> {
        let table = Arc::clone(self.preparation.get().context("Delta owner is not initialized")?);
        crate::delta_io::run_cpu_async(move || table.adaptive_file_config(&defaults)).await?
    }

    /// Obtain a file without doing synchronous Kernel work on a collector core.
    ///
    /// # Errors
    /// Returns schema, ownership, worker, or file creation errors.
    pub async fn new_file(&self, schema: SchemaRef) -> Result<DeltaFileWriter> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(Request::NewFile(schema, reply))
            .await
            .context("Delta worker stopped")?;
        receive
            .await
            .context("Delta worker stopped during file creation")?
            .map_err(anyhow::Error::new)
    }

    /// Create a file carrying the stable identity and stamp of one admitted publication.
    ///
    /// # Errors
    /// Returns schema, fencing, partition or worker failures.
    pub async fn new_file_with_publication(
        &self,
        schema: SchemaRef,
        app_id: String,
        version: i64,
        ingested_at_ms: i64,
    ) -> Result<DeltaFileWriter> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(Request::NewPublication(schema, app_id, version, ingested_at_ms, reply))
            .await
            .context("Delta worker stopped")?;
        receive
            .await
            .context("Delta worker stopped during publication preparation")?
            .map_err(anyhow::Error::new)
    }

    /// Prepare separate replay-safe files for the batch's committed table partition keys.
    ///
    /// # Errors
    /// Rejects invalid batch values and schema, ownership or worker failures.
    pub async fn new_partitioned_publication_files(
        &self,
        batch: RecordBatch,
        app_id: String,
        version: i64,
        stamp: i64,
    ) -> Result<Vec<(DeltaFileWriter, RecordBatch)>> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(Request::NewPartitionedPublication(batch, app_id, version, stamp, reply))
            .await
            .context("Delta worker stopped")?;
        receive
            .await
            .context("Delta worker stopped during partition preparation")?
            .map_err(anyhow::Error::new)
    }

    /// Check exact logical identities before callers concatenate publication batches.
    ///
    /// `true` means that exact ID/version is already committed and can be excluded
    /// from a rebuilt coalesced batch. Commit admission repeats this check.
    ///
    /// # Errors
    /// Returns transaction-history, fencing, validation or worker failures.
    pub async fn lookup_publications(&self, publications: Vec<(String, i64)>) -> Result<Vec<bool>> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(Request::LookupPublications(publications, reply))
            .await
            .context("Delta worker stopped")?;
        receive
            .await
            .context("Delta worker stopped during publication lookup")?
            .map_err(anyhow::Error::new)
    }

    /// Partition and stage compatible logical publications as one bounded commit group.
    ///
    /// Inputs must share a schema. Output files retain each actual table partition;
    /// sorted layouts are split at their configured row and memory limits before writing.
    ///
    /// # Errors
    /// Rejects incompatible schemas, invalid identities, resource-cap overflow, or stage failures.
    pub async fn stage_coalesced_publications(
        &self,
        entries: Vec<(RecordBatch, String, i64, i64)>,
    ) -> Result<PreparedPublicationGroup> {
        self.stage_coalesced_publications_with_concurrency(entries, 1).await
    }

    /// Stage up to `stage_file_concurrency` sibling files at once, in one atomic group.
    ///
    /// # Errors
    /// Rejects widths outside 1..=16 and propagates planning and staging errors.
    pub async fn stage_coalesced_publications_with_concurrency(
        &self,
        entries: Vec<(RecordBatch, String, i64, i64)>,
        stage_file_concurrency: usize,
    ) -> Result<PreparedPublicationGroup> {
        let table = Arc::clone(self.preparation.get().context("Delta owner is not initialized")?);
        let (appends, identities, committed_identities, preparation_build, preparation_sort) = table
            .stage_coalesced_publications(entries, self.options.max_files_per_commit, stage_file_concurrency)
            .await
            .map_err(CommitReceiptError::from_anyhow)?;
        Ok(PreparedPublicationGroup {
            appends,
            identities,
            committed_identities,
            preparation_build,
            preparation_sort,
        })
    }

    /// Admit one eligible durable file, waiting asynchronously when capacity is full.
    ///
    /// # Errors
    /// Returns admission/worker errors. The receipt separately reports publication errors.
    pub async fn submit(&self, append: StagedAppend, force: bool) -> Result<CommitReceipt> {
        let receive = self.submit_publication(vec![append], Vec::new(), force).await?;
        Ok(CommitReceipt(receive))
    }

    /// Admit all physical files and logical identities as one indivisible Delta commit.
    ///
    /// Group sizes above either file cap are rejected before queue admission; the worker
    /// never splits the group to satisfy ordinary batching thresholds.
    ///
    /// # Errors
    /// Rejects empty/oversized groups, invalid identities, or admission/worker failures.
    pub async fn submit_group(
        &self,
        appends: Vec<StagedAppend>,
        identities: Vec<(String, i64, i64)>,
        force: bool,
    ) -> Result<GroupCommitReceipt> {
        self.validate_group(&appends, &identities)?;
        let receive = self.submit_publication(appends, identities, force).await?;
        Ok(GroupCommitReceipt(receive))
    }

    fn validate_group(&self, appends: &[StagedAppend], identities: &[(String, i64, i64)]) -> Result<()> {
        if identities.is_empty() {
            bail!("coalesced publication group requires logical identities");
        }
        if appends.len() > self.options.max_files_per_commit || appends.len() > self.options.max_pending_files {
            bail!(
                "coalesced publication group has {} files, exceeding the atomic commit capacity",
                appends.len()
            );
        }
        if appends.iter().any(|append| append.publication.is_some()) {
            bail!("coalesced physical files must not carry per-file publication identities");
        }
        let mut unique = std::collections::HashSet::with_capacity(identities.len());
        for (app_id, version, stamp) in identities {
            crate::delta::validate_transaction_id(app_id, *version)?;
            if *stamp < 0 || !unique.insert(app_id.as_str()) {
                bail!("invalid or duplicate coalesced publication identity");
            }
        }
        Ok(())
    }

    /// Submit a prepared coalesced group without separating its identity set from its files.
    ///
    /// # Errors
    /// Returns admission/worker errors; the receipt reports commit and replay-race failures.
    pub async fn submit_prepared_group(
        &self,
        mut group: PreparedPublicationGroup,
        force: bool,
    ) -> Result<GroupCommitReceipt> {
        let identities = if group.identities.is_empty() {
            std::mem::take(&mut group.committed_identities)
        } else {
            std::mem::take(&mut group.identities)
        };
        self.validate_group(&group.appends, &identities)?;
        let (slot, queue) = self.reserve_publication(group.appends.len()).await?;
        // No cancellation point between disarming the cleanup guard and handoff.
        let receive = Self::enqueue_publication(queue, slot, std::mem::take(&mut group.appends), identities, force);
        Ok(GroupCommitReceipt(receive))
    }

    async fn submit_publication(
        &self,
        appends: Vec<StagedAppend>,
        identities: Vec<(String, i64, i64)>,
        force: bool,
    ) -> Result<oneshot::Receiver<PublicationReply>> {
        let (slot, queue) = self.reserve_publication(appends.len()).await?;
        Ok(Self::enqueue_publication(queue, slot, appends, identities, force))
    }

    async fn reserve_publication(&self, files: usize) -> Result<(OwnedSemaphorePermit, mpsc::Permit<'_, Request>)> {
        let permit_count = u32::try_from(files).context("Delta publication file count exceeds u32")?;
        let slot = Arc::clone(&self.slots).acquire_many_owned(permit_count).await?;
        Ok((
            slot,
            self.sender.reserve().await.context("Delta commit worker stopped")?,
        ))
    }

    fn enqueue_publication(
        queue: mpsc::Permit<'_, Request>,
        slot: OwnedSemaphorePermit,
        appends: Vec<StagedAppend>,
        identities: Vec<(String, i64, i64)>,
        force: bool,
    ) -> oneshot::Receiver<PublicationReply> {
        let (reply, receive) = oneshot::channel();
        queue.send(Request::Publish(Box::new(Publication {
            appends,
            identities,
            reply,
            _slot: slot,
            force,
        })));
        receive
    }

    /// Wake a partially filled batch immediately. Future forced submissions also bypass the window.
    pub fn flush_pending(&self) {
        self.flush
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    /// Disable collection delays for this owner's remaining dependency waves.
    pub fn start_draining(&self) {
        self.draining.store(true, Ordering::Release);
        self.flush_pending();
    }
}

fn same_publication_fence(left: &Option<Arc<dyn PublicationFence>>, right: &Option<Arc<dyn PublicationFence>>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        (None, Some(_)) | (Some(_), None) => false,
    }
}

fn run_worker(
    root: &Path,
    options: CommitOptions,
    mut receiver: mpsc::Receiver<Request>,
    mut flush: watch::Receiver<u64>,
    draining: Arc<AtomicBool>,
    publication_fence: Option<Arc<dyn PublicationFence>>,
) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build();
    let table = publication_fence
        .map_or_else(
            || DeltaTable::open(root),
            |publication_fence| {
                root.to_str()
                    .context("invalid table URI")
                    .and_then(|root| DeltaTable::open_uri_with_fence(root, publication_fence))
            },
        )
        .and_then(|mut table| {
            Arc::get_mut(&mut table)
                .context("new Delta worker table is already shared")?
                .txn_lookup_mode = options.txn_lookup_mode;
            Ok(table)
        });
    let mut failure = table
        .as_ref()
        .err()
        .map(|error| CommitReceiptError::commit_failed(format!("{error:#}")));
    match runtime {
        Ok(runtime) => runtime.block_on(async {
            let mut batch = Vec::new();
            let mut deadline = None;
            let mut flush_open = true;
            let mut flushing = false;
            let mut flush_remaining = 0;
            loop {
                // A flush signal can arrive before the worker has consumed the
                // already admitted files. Drain that bounded queue before
                // publishing the partial batch, so the wakeup cannot be lost.
                let request = if flushing {
                    if flush_remaining == 0 {
                        publish_batch(&table, &mut batch, &mut failure);
                        deadline = None;
                        flushing = false;
                        continue;
                    }
                    match receiver.try_recv() {
                        Ok(request) => {
                            flush_remaining -= 1;
                            Some(request)
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {
                            publish_batch(&table, &mut batch, &mut failure);
                            deadline = None;
                            flushing = false;
                            continue;
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => None,
                    }
                } else {
                    tokio::select! {
                        biased;
                        _ = async {
                            if let Some(deadline) = deadline {
                                tokio::time::sleep_until(deadline).await;
                            } else {
                                std::future::pending::<()>().await;
                            }
                        } => {
                            publish_batch(&table, &mut batch, &mut failure);
                            deadline = None;
                            continue;
                        }
                        changed = flush.changed(), if flush_open => {
                            flush_open = changed.is_ok();
                            flushing = true;
                            // Snapshot this cohort: producers refilling the
                            // queue must not postpone a forced partial commit.
                            flush_remaining = receiver.len();
                            continue;
                        }
                        request = receiver.recv() => request,
                    }
                };
                match request {
                    Some(Request::Ready(reply)) => {
                        let result = match (&table, &failure) {
                            (Ok(table), None) => Ok(Arc::clone(table)),
                            _ => Err(failure.clone().unwrap_or_else(|| {
                                CommitReceiptError::commit_failed("Delta table initialization failed")
                            })),
                        };
                        let _ = reply.send(result);
                    }
                    Some(Request::NewFile(schema, reply)) => {
                        let result = match (&table, &failure) {
                            (Ok(table), None) => table.new_file(schema).map_err(CommitReceiptError::from_anyhow),
                            _ => Err(failure.clone().unwrap_or_else(|| {
                                CommitReceiptError::commit_failed("Delta table initialization failed")
                            })),
                        };
                        if let Err(error) = &result {
                            failure = Some(error.clone());
                            publish_batch(&table, &mut batch, &mut failure);
                            deadline = None;
                        }
                        let _ = reply.send(result);
                    }
                    Some(Request::NewPublication(schema, app_id, version, stamp, reply)) => {
                        let result = match (&table, &failure) {
                            (Ok(table), None) => table
                                .new_file_with_publication(schema, app_id, version, stamp)
                                .map_err(CommitReceiptError::from_anyhow),
                            _ => Err(failure.clone().unwrap_or_else(|| {
                                CommitReceiptError::commit_failed("Delta table initialization failed")
                            })),
                        };
                        let _ = reply.send(result);
                    }
                    Some(Request::NewPartitionedPublication(batch, app_id, version, stamp, reply)) => {
                        let result = match (&table, &failure) {
                            (Ok(table), None) => table
                                .new_partitioned_publication_files(batch, app_id, version, stamp)
                                .map_err(CommitReceiptError::from_anyhow),
                            _ => Err(failure.clone().unwrap_or_else(|| {
                                CommitReceiptError::commit_failed("Delta table initialization failed")
                            })),
                        };
                        let _ = reply.send(result);
                    }
                    Some(Request::LookupPublications(publications, reply)) => {
                        let result = match (&table, &failure) {
                            (Ok(table), None) => table
                                .lookup_publications(&publications)
                                .map_err(CommitReceiptError::from_anyhow),
                            _ => Err(failure.clone().unwrap_or_else(|| {
                                CommitReceiptError::commit_failed("Delta table initialization failed")
                            })),
                        };
                        let _ = reply.send(result);
                    }
                    Some(Request::Publish(publication)) => {
                        if let Some(error) = &failure {
                            let _ = publication.reply.send(Err(error.clone().into()));
                            continue;
                        }
                        let file_count = batch
                            .iter()
                            .map(|publication: &Publication| publication.appends.len())
                            .sum::<usize>();
                        let overlaps = publication.identity_names().any(|id| {
                            batch
                                .iter()
                                .any(|previous| previous.identity_names().any(|other| other == id))
                        });
                        if !batch.is_empty()
                            && (file_count + publication.appends.len() > options.max_files_per_commit || overlaps)
                        {
                            publish_batch(&table, &mut batch, &mut failure);
                            deadline = None;
                        }
                        let force = publication.force;
                        batch.push(*publication);
                        if deadline.is_none() {
                            deadline = Some(Instant::now() + options.batch_window);
                        }
                        let file_count = batch.iter().map(|publication| publication.appends.len()).sum::<usize>();
                        if force || draining.load(Ordering::Acquire) || file_count >= options.max_files_per_commit {
                            publish_batch(&table, &mut batch, &mut failure);
                            deadline = None;
                        }
                    }
                    None => {
                        publish_batch(&table, &mut batch, &mut failure);
                        break;
                    }
                }
            }
        }),
        Err(error) => {
            let message = CommitReceiptError::commit_failed(format!("Delta worker runtime failed: {error}"));
            while let Some(request) = receiver.blocking_recv() {
                match request {
                    Request::Ready(reply) => {
                        let _ = reply.send(Err(message.clone()));
                    }
                    Request::NewFile(_, reply) | Request::NewPublication(_, _, _, _, reply) => {
                        let _ = reply.send(Err(message.clone()));
                    }
                    Request::NewPartitionedPublication(_, _, _, _, reply) => {
                        let _ = reply.send(Err(message.clone()));
                    }
                    Request::LookupPublications(_, reply) => {
                        let _ = reply.send(Err(message.clone()));
                    }
                    Request::Publish(publication) => {
                        let _ = publication.reply.send(Err(message.clone().into()));
                    }
                }
            }
        }
    }
}

fn publish_batch(
    table: &Result<Arc<DeltaTable>>,
    batch: &mut Vec<Publication>,
    failure: &mut Option<CommitReceiptError>,
) {
    if batch.is_empty() {
        return;
    }
    let publications = std::mem::take(batch);
    let mut appends = Vec::new();
    let mut groups = Vec::new();
    let mut replies = Vec::with_capacity(publications.len());
    for publication in publications {
        let start = appends.len();
        appends.extend(publication.appends);
        let end = appends.len();
        if !publication.identities.is_empty() {
            groups.push(CoalescedCommitGroup {
                start,
                end,
                identities: publication.identities,
            });
        }
        replies.push((start, end, publication.reply, publication._slot));
    }
    let result = match (table, failure.as_ref()) {
        (Ok(table), None) if groups.is_empty() => table
            .commit_files_timed(&appends)
            .map_err(PublicationFailure::from_anyhow),
        (Ok(table), None) => table
            .commit_files_with_groups_timed(&appends, &groups)
            .map_err(PublicationFailure::from_anyhow),
        _ => Err(failure
            .clone()
            .unwrap_or_else(|| CommitReceiptError::commit_failed("Delta table unavailable"))
            .into()),
    };
    let result =
        result.map(|(head, duration, observations, evidence)| (head, duration, observations, Arc::new(evidence)));
    if let Err(error) = &result
        && !error.error.is_retryable()
    {
        *failure = Some(error.error.clone());
    }
    let mut files = appends.into_iter().map(|append| Some(append.file)).collect::<Vec<_>>();
    let mut commit_duration = result.as_ref().ok().and_then(|(_, duration, _, _)| *duration);
    for (start, end, reply, _slot) in replies {
        let delivered = match result.as_ref() {
            Ok((version, _, observations, evidence)) => files[start..end]
                .iter_mut()
                .map(|file| file.take().context("Delta worker lost staged output file"))
                .collect::<Result<Vec<_>>>()
                .map(|files| GroupAppendResult {
                    evidence: Arc::clone(evidence),
                    version: *version,
                    files,
                    commit_duration: commit_duration.take(),
                    file_observations: observations
                        .iter()
                        .filter(|(index, _)| (start..end).contains(index))
                        .map(|(_, observation)| *observation)
                        .collect(),
                })
                .map_err(PublicationFailure::from_anyhow),
            Err(error) => Err(error.clone()),
        };
        // Cancellation never replays a possibly committed file. A live waiter
        // receives the result; an abandoned file stays visible only if committed.
        if let Err(Ok(abandoned)) = reply.send(delivered) {
            commit_duration = abandoned.commit_duration;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};

    fn input() -> Result<RecordBatch> {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .map_err(Into::into)
    }

    async fn staged_file(table: &BatchedTable, input: &RecordBatch) -> Result<StagedAppend> {
        let mut writer = table.new_file(input.schema()).await?;
        writer.write(input).await?;
        writer.stage().await
    }

    async fn partitioned_stage_fixture(root: &Path) -> Result<(Arc<BatchedTable>, RecordBatch)> {
        let url = url::Url::from_directory_path(root).map_err(|()| anyhow::anyhow!("invalid test path"))?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
            Field::new("bucket", DataType::Utf8, false),
        ]));
        DeltaTable::open_catalog(url)?.commit_catalog(
            None,
            Arc::clone(&schema),
            vec!["bucket".into()],
            Default::default(),
            None,
        )?;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![11, 12, 13])),
                Arc::new(arrow::array::StringArray::from(vec!["b", "a", "b"])),
            ],
        )?;
        let table = BatchedTable::open(root, CommitOptions::default()).await?;
        Ok((table, batch))
    }

    #[tokio::test]
    async fn parallel_partition_stages_remain_ordered_atomic_and_reclaimable() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let (table, batch) = partitioned_stage_fixture(&root).await?;
        let entries = vec![(batch, "logical".to_owned(), 0, 10)];
        let group = table
            .stage_coalesced_publications_with_concurrency(entries.clone(), 2)
            .await?;
        let paths: Vec<_> = group.appends.iter().map(|a| a.file.path.clone()).collect();
        assert_eq!(paths.len(), 2);
        assert!(paths[0].starts_with(root.join("bucket=a")));
        assert!(paths[1].starts_with(root.join("bucket=b")));
        assert_eq!(group.appends[0].file.rows, 1);
        assert_eq!(group.appends[1].file.rows, 2);

        // Cancel while the entire group is waiting for commit-file admission.
        let slots = Arc::clone(&table.slots)
            .acquire_many_owned(u32::try_from(table.options.max_pending_files)?)
            .await?;
        let mut submitting = Box::pin(table.submit_prepared_group(group, true));
        assert!(futures::poll!(submitting.as_mut()).is_pending());
        drop(submitting);
        drop(slots);
        tokio::time::timeout(Duration::from_secs(5), async {
            while paths.iter().any(|path| path.exists()) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        let log = root.join("_delta_log/00000000000000000001.json");
        assert!(!log.exists());
        let group = table.stage_coalesced_publications_with_concurrency(entries, 2).await?;
        let result = table.submit_prepared_group(group, true).await?.wait().await?;
        assert_eq!(result.version, 1);
        assert_eq!(result.files.len(), 2);
        let actions = std::fs::read_to_string(log)?;
        assert_eq!(actions.lines().filter(|line| line.contains("\"add\":")).count(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn saturated_coalesced_publish_and_reclaim() -> Result<()> {
        const CHILD: &str = "ARROWCOL_TEST_SATURATED_STAGE";
        if std::env::var_os(CHILD).is_none() {
            // Runtime configuration is process-wide. Bound the child as well as its operations.
            let child = tokio::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "commit::tests::saturated_coalesced_publish_and_reclaim",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env("ARROWCOL_COLLECTOR_CPU_WORKERS", "2")
                .env("ARROWCOL_COLLECTOR_CPU_QUEUE_JOBS", "1")
                .env("ARROWCOL_COLLECTOR_CPU_DISPATCH", "bounded_shards")
                .env("ARROWCOL_COLLECTOR_PARQUET_COLUMN_WORKERS", "2")
                .kill_on_drop(true)
                .output();
            let output = tokio::time::timeout(Duration::from_secs(30), child).await??;
            anyhow::ensure!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            return Ok(());
        }
        tokio::time::timeout(Duration::from_secs(15), exercise_saturated_staging()).await?
    }

    async fn exercise_saturated_staging() -> Result<()> {
        for cancel in [true, false] {
            let directory = tempfile::tempdir()?;
            let root = directory.path().canonicalize()?;
            let (table, batch) = partitioned_stage_fixture(&root).await?;
            let entries = vec![(batch, "logical".to_owned(), 0, 10)];
            let log = root.join("_delta_log/00000000000000000001.json");
            // Stop both real files in the directory fsync at the end of staging. Both
            // CPU workers must be available *before* we unblock those I/O operations.
            let (a, release_a) = crate::sync_hook::pause_next(root.join("bucket=a"));
            let (b, release_b) = crate::sync_hook::pause_next(root.join("bucket=b"));
            let mut stage = Box::pin(table.stage_coalesced_publications_with_concurrency(entries, 16));
            tokio::select! {
                result = &mut stage => anyhow::bail!("staging finished before sync gates: {:?}", result.err()),
                gates = async { tokio::try_join!(a, b) } => { gates?; }
            }
            let mut releases = Vec::new();
            let mut workers = Vec::new();
            for _ in 0..2 {
                let (entered, ready) = oneshot::channel();
                let (release, wait) = std::sync::mpsc::channel::<()>();
                releases.push(release);
                workers.push(tokio::spawn(crate::delta_io::run_cpu_async(move || {
                    let _ = entered.send(());
                    let _ = wait.recv();
                })));
                ready.await?;
            }
            let mut queued = Box::pin(crate::delta_io::run_cpu_async(|| ()));
            assert!(futures::poll!(queued.as_mut()).is_pending());
            let paths = ["bucket=a", "bucket=b"].map(|bucket| -> Result<_> {
                let files = std::fs::read_dir(root.join(bucket))?.collect::<std::io::Result<Vec<_>>>()?;
                anyhow::ensure!(files.len() == 1, "expected one staged file per partition");
                Ok(files[0].path())
            });
            let [a, b] = paths;
            let paths = [a?, b?];
            if cancel {
                // Drop the caller before the file tasks have registered their outputs.
                drop(stage);
                drop((release_a, release_b));
                while paths.iter().any(|p| p.exists()) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                assert!(!log.exists());
            } else {
                drop((release_a, release_b));
                let group = stage.await?;
                assert_eq!(group.appends.len(), 2);
                for ((append, path), expected) in group.appends.iter().zip(&paths).zip([vec![2], vec![1, 3]]) {
                    assert_eq!(&append.file.path, path);
                    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                        std::fs::File::open(path)?,
                    )?
                    .build()?;
                    let mut ids = Vec::new();
                    for batch in reader {
                        let batch = batch?;
                        ids.extend_from_slice(
                            batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .context("id column")?
                                .values(),
                        );
                    }
                    assert_eq!(ids, expected);
                }
                let result = table.submit_prepared_group(group, true).await?.wait().await?;
                assert_eq!(result.version, 1);
                assert_eq!(result.files.len(), 2);
                let actions = std::fs::read_to_string(&log)?;
                assert_eq!(actions.lines().filter(|line| line.contains("\"add\":")).count(), 2);
            }
            drop(releases);
            for worker in workers {
                worker.await??;
            }
            queued.await?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coalesced_preparation_progresses_while_prior_commit_is_fenced() -> Result<()> {
        struct Gate {
            entered: Mutex<Option<oneshot::Sender<()>>>,
            release: Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl PublicationFence for Gate {
            fn check_before_commit(
                &self,
                context: &crate::delta::PublicationFenceContext<'_>,
            ) -> std::result::Result<(), PublicationFenceError> {
                if context.file_count() > 0
                    && let Some(entered) = self
                        .entered
                        .lock()
                        .map_err(|_| PublicationFenceError::check_failed("test entry gate poisoned"))?
                        .take()
                {
                    let _ = entered.send(());
                    self.release
                        .lock()
                        .map_err(|_| PublicationFenceError::check_failed("test release gate poisoned"))?
                        .recv_timeout(Duration::from_secs(10))
                        .map_err(|_| PublicationFenceError::check_failed("test gate was not released"))?;
                }
                Ok(())
            }
        }
        let directory = tempfile::tempdir()?;
        let (entered, observed) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let fence = Arc::new(Gate {
            entered: Mutex::new(Some(entered)),
            release: Mutex::new(wait),
        });
        let table = BatchedTable::open_with_fence(directory.path(), CommitOptions::default(), fence).await?;
        let prepared = table
            .stage_coalesced_publications(vec![(input()?, "first".into(), 0, 10)])
            .await?;
        let first = table.submit_prepared_group(prepared, true).await?;
        tokio::time::timeout(Duration::from_secs(3), observed)
            .await
            .context("first append reached fence")??;
        let second = tokio::time::timeout(
            Duration::from_secs(3),
            table.stage_coalesced_publications(vec![(input()?, "second".into(), 0, 20)]),
        )
        .await;
        release.send(())?;
        let prepared = second.context("preparation must not wait for a prior commit")??;
        assert_eq!(prepared.appends.len(), 1);
        assert_eq!(first.wait().await?.version, 1);
        assert_eq!(
            table.submit_prepared_group(prepared, true).await?.wait().await?.version,
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn coalesced_groups_share_versions_without_replaying_overlapping_identities() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = BatchedTable::open(
            directory.path(),
            CommitOptions {
                batch_window: Duration::from_secs(60),
                max_files_per_commit: 2,
                max_pending_files: 4,
                ..Default::default()
            },
        )
        .await?;
        let first = table
            .stage_coalesced_publications(vec![(input()?, "first".into(), 0, 10)])
            .await?;
        let duplicate = table
            .stage_coalesced_publications(vec![(input()?, "first".into(), 0, 10)])
            .await?;
        let second = table
            .stage_coalesced_publications(vec![(input()?, "second".into(), 0, 20)])
            .await?;
        let a = table.submit_prepared_group(first, false).await?;
        let replay = table.submit_prepared_group(duplicate, false).await?;
        let b = table.submit_prepared_group(second, true).await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), a.wait()).await??.version,
            1
        );
        let (replay, b) = tokio::try_join!(replay.wait(), b.wait())?;
        assert_eq!(replay.version, 2);
        assert_eq!(b.version, 2);
        assert!(replay.file_observations.is_empty());
        assert_eq!(b.file_observations.len(), 1);
        let adds = [1, 2]
            .into_iter()
            .map(|version| {
                std::fs::read_to_string(directory.path().join(format!("_delta_log/{version:020}.json")))
                    .map(|log| log.lines().filter(|line| line.contains("\"add\":")).count())
            })
            .collect::<std::io::Result<Vec<_>>>()?;
        assert_eq!(adds, [1, 1]);
        Ok(())
    }

    /// Scenario: one file is queued and the remaining capacity is occupied by in-flight work.
    /// Guarantees: another submission remains asynchronous and cannot enter until capacity is released.
    #[tokio::test]
    async fn occupied_capacity_blocks_admission_until_released() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = BatchedTable::open(
            directory.path(),
            CommitOptions {
                max_files_per_commit: 2,
                max_pending_files: 2,
                ..Default::default()
            },
        )
        .await?;
        let input = input()?;
        let mut left = table.new_file(input.schema()).await?;
        let mut right = table.new_file(input.schema()).await?;
        left.write(&input).await?;
        right.write(&input).await?;
        let occupied = Arc::clone(&table.slots).acquire_owned().await?;
        let a = table.submit(left.stage().await?, false).await?;
        assert_eq!(table.slots.available_permits(), 0);
        let mut b = Box::pin(table.submit(right.stage().await?, false));
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut b).await.is_err());
        drop(occupied);
        let b = b.await?;
        let a = tokio::time::timeout(Duration::from_secs(3), a.wait()).await??;
        let b = tokio::time::timeout(Duration::from_secs(3), b.wait()).await??;
        assert_eq!(a.version, 1);
        assert_eq!(b.version, 1);
        assert_eq!(
            usize::from(a.commit_duration.is_some()) + usize::from(b.commit_duration.is_some()),
            1,
            "one physical commit reports one durable duration"
        );
        assert_eq!(a.file_observations.len() + b.file_observations.len(), 2);
        Ok(())
    }

    /// Scenario: a coalesced group is larger than the worker's file cap.
    /// Guarantees: it is rejected before queueing and no constituent ID is published.
    #[tokio::test]
    async fn oversized_coalesced_group_is_rejected_without_partial_publish() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = BatchedTable::open(
            directory.path(),
            CommitOptions {
                max_files_per_commit: 1,
                max_pending_files: 2,
                ..Default::default()
            },
        )
        .await?;
        let input = input()?;
        let appends = vec![staged_file(&table, &input).await?, staged_file(&table, &input).await?];
        let error = table
            .submit_group(
                appends,
                vec![("logical-a".into(), 0, 10), ("logical-b".into(), 0, 20)],
                true,
            )
            .await
            .err()
            .context("oversized group unexpectedly entered the queue")?;
        assert!(error.to_string().contains("exceeding the atomic commit capacity"));
        assert_eq!(
            table
                .lookup_publications(vec![("logical-a".into(), 0), ("logical-b".into(), 0)])
                .await?,
            vec![false, false]
        );
        Ok(())
    }

    /// Scenario: another delivery commits one constituent after group encoding but before admission.
    /// Guarantees: the atomic group returns a retryable rebuild signal and adds no output.
    #[tokio::test]
    async fn partial_group_replay_is_retryable_and_does_not_publish_remaining_rows() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = BatchedTable::open(
            directory.path(),
            CommitOptions {
                max_files_per_commit: 2,
                max_pending_files: 2,
                ..Default::default()
            },
        )
        .await?;
        let input = input()?;
        let group = table
            .stage_coalesced_publications(vec![
                (input.clone(), "logical-a".into(), 0, 10),
                (input.clone(), "logical-b".into(), 0, 20),
            ])
            .await?;
        assert_eq!(group.identities.len(), 2);
        let mut competing = table
            .new_file_with_publication(input.schema(), "logical-a".into(), 0, 10)
            .await?;
        competing.write(&input).await?;
        assert_eq!(
            table
                .submit(competing.stage().await?, true)
                .await?
                .wait()
                .await?
                .version,
            1
        );
        let failure = match table
            .submit_prepared_group(group, true)
            .await?
            .wait_with_evidence()
            .await
        {
            Err(error) => error,
            Ok(_) => bail!("partially committed coalesced group unexpectedly succeeded"),
        };
        assert!(matches!(
            failure.error,
            CommitReceiptError::RetryablePublicationGroup { committed: 1, total: 2 }
        ));
        let evidence = failure.evidence.context("missing partial-group evidence")?;
        assert!(!evidence.clean && evidence.reasons.contains(&crate::PublicationReason::PartialGroup));
        assert_eq!(
            evidence.outcomes[&("logical-a".into(), 0)],
            crate::PublicationOutcome::AlreadyCommitted(1)
        );
        assert_eq!(
            evidence.outcomes[&("logical-b".into(), 0)],
            crate::PublicationOutcome::Unknown(crate::PublicationReason::PartialGroup)
        );
        assert_eq!(
            table
                .lookup_publications(vec![("logical-a".into(), 0), ("logical-b".into(), 0)])
                .await?,
            vec![true, false]
        );
        Ok(())
    }

    /// Scenario: multiple physical outputs and logical identities are accepted together.
    /// Guarantees: one Delta version contains every staged file and every stable identity.
    #[tokio::test]
    async fn coalesced_group_commits_all_files_and_identities_atomically() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = BatchedTable::open(
            directory.path(),
            CommitOptions {
                max_files_per_commit: 2,
                max_pending_files: 2,
                ..Default::default()
            },
        )
        .await?;
        let input = input()?;
        let appends = vec![staged_file(&table, &input).await?, staged_file(&table, &input).await?];
        let committed = table
            .submit_group(
                appends,
                vec![("logical-a".into(), 0, 10), ("logical-b".into(), 0, 20)],
                true,
            )
            .await?
            .wait()
            .await?;
        assert_eq!(committed.version, 1);
        assert_eq!(committed.files.len(), 2);
        assert_eq!(committed.files.iter().map(|file| file.rows).sum::<usize>(), 2);
        assert!(committed.commit_duration.is_some());
        assert_eq!(committed.file_observations.len(), 2);
        assert_eq!(
            table
                .lookup_publications(vec![("logical-a".into(), 0), ("logical-b".into(), 0)])
                .await?,
            vec![true, true]
        );
        Ok(())
    }

    /// Scenario: two compatible logical batches are staged into one output file.
    /// Guarantees: rows/schema survive coalescing and both IDs share the same Delta version.
    #[tokio::test]
    async fn coalesced_preparation_preserves_rows_and_schema() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = BatchedTable::open(
            directory.path(),
            CommitOptions {
                max_files_per_commit: 2,
                max_pending_files: 2,
                ..Default::default()
            },
        )
        .await?;
        let input = input()?;
        let group = table
            .stage_coalesced_publications(vec![
                (input.clone(), "logical-a".into(), 0, 10),
                (input.clone(), "logical-b".into(), 0, 20),
            ])
            .await?;
        assert_eq!(group.appends.len(), 1);
        assert_eq!(group.appends[0].file.rows, 2);
        assert_eq!(group.appends[0].schema.as_ref(), input.schema().as_ref());
        assert_eq!(group.identities.len(), 2);
        assert_eq!(group.identities[0].2, 10);
        assert_eq!(group.identities[1].2, 20);
        assert!(!group.preparation_build.is_zero());
        let result = table.submit_prepared_group(group, true).await?.wait().await?;
        assert_eq!(result.version, 1);
        assert_eq!(result.files.len(), 1);
        assert_eq!(
            table
                .lookup_publications(vec![("logical-a".into(), 0), ("logical-b".into(), 0)])
                .await?,
            vec![true, true]
        );
        Ok(())
    }

    /// Scenario: preparation finds every constituent already replayed.
    /// Guarantees: the empty prepared group resolves to the current version without a new commit.
    #[tokio::test]
    async fn fully_replayed_prepared_group_returns_existing_version() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = BatchedTable::open(
            directory.path(),
            CommitOptions {
                max_files_per_commit: 2,
                max_pending_files: 2,
                ..Default::default()
            },
        )
        .await?;
        let input = input()?;
        for app_id in ["logical-a", "logical-b"] {
            let mut writer = table
                .new_file_with_publication(input.schema(), app_id.into(), 0, 10)
                .await?;
            writer.write(&input).await?;
            let receipt = table.submit(writer.stage().await?, true).await?;
            receipt.wait().await?;
        }
        assert_eq!(
            table
                .lookup_publications(vec![("logical-a".into(), 0), ("logical-b".into(), 0)])
                .await?,
            vec![true, true]
        );
        let group = table
            .stage_coalesced_publications(vec![
                (input.clone(), "logical-a".into(), 0, 10),
                (input, "logical-b".into(), 0, 10),
            ])
            .await?;
        assert!(group.appends.is_empty());
        assert!(group.identities.is_empty());
        assert_eq!(group.committed_identities.len(), 2);
        let result = table.submit_prepared_group(group, true).await?.wait().await?;
        assert_eq!(result.version, 2);
        assert!(result.files.is_empty());
        Ok(())
    }

    /// Scenario: the legacy one-file publication is submitted twice with the same ID.
    /// Guarantees: ordinary replay still returns the existing Delta version without duplication.
    #[tokio::test]
    async fn ordinary_publication_replay_semantics_are_unchanged() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = BatchedTable::open(
            directory.path(),
            CommitOptions {
                max_files_per_commit: 1,
                max_pending_files: 1,
                ..Default::default()
            },
        )
        .await?;
        let input = input()?;
        let mut first = table
            .new_file_with_publication(input.schema(), "ordinary".into(), 0, 10)
            .await?;
        first.write(&input).await?;
        assert_eq!(table.submit(first.stage().await?, true).await?.wait().await?.version, 1);
        let mut replay = table
            .new_file_with_publication(input.schema(), "ordinary".into(), 0, 10)
            .await?;
        replay.write(&input).await?;
        assert_eq!(
            table.submit(replay.stage().await?, true).await?.wait().await?.version,
            1
        );
        assert_eq!(
            table.lookup_publications(vec![("ordinary".into(), 0)]).await?,
            vec![true]
        );
        Ok(())
    }
}
