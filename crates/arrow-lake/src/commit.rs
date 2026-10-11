//! Bounded, process-shared, per-table Delta publication.

use crate::delta::{AppendResult, DeltaFileWriter, DeltaTable, StagedAppend};
use anyhow::{Context, Result, bail};
use arrow::datatypes::SchemaRef;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::time::Instant;

/// Limits for eligible files awaiting a shared transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitOptions {
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
    sender: mpsc::Sender<Request>,
    slots: Arc<Semaphore>,
    flush: watch::Sender<u64>,
    draining: Arc<AtomicBool>,
}

struct Publication {
    append: StagedAppend,
    reply: oneshot::Sender<std::result::Result<AppendResult, String>>,
    _slot: OwnedSemaphorePermit,
    force: bool,
}

enum Request {
    Ready(oneshot::Sender<std::result::Result<(), String>>),
    NewFile(SchemaRef, oneshot::Sender<std::result::Result<DeltaFileWriter, String>>),
    Publish(Box<Publication>),
}

/// A submitted file's eventual durable Delta version.
pub struct CommitReceipt(oneshot::Receiver<std::result::Result<AppendResult, String>>);

impl CommitReceipt {
    /// Wait for publication, not merely queue admission.
    ///
    /// # Errors
    /// Returns worker failure, conflict, or uncertain commit errors.
    pub async fn wait(self) -> Result<AppendResult> {
        self.0
            .await
            .context("Delta commit worker stopped before completion")?
            .map_err(anyhow::Error::msg)
    }
}

impl BatchedTable {
    /// Share local table ownership across cores in this process.
    ///
    /// # Errors
    /// Rejects conflicting options, another process's writer, or filesystem/worker errors.
    pub async fn open(root: impl AsRef<Path>, options: CommitOptions) -> Result<Arc<Self>> {
        options.validate()?;
        tokio::fs::create_dir_all(root.as_ref()).await?;
        let root = tokio::fs::canonicalize(root).await?;
        loop {
            let (owner, mut stopped) = {
                let mut registry = TABLES.lock().map_err(|_| anyhow::anyhow!("Delta registry poisoned"))?;
                if let Some(entry) = registry.get(&root) {
                    if let Some(owner) = entry.owner.upgrade() {
                        if owner.options != options {
                            bail!("conflicting Delta commit options for {}", root.display());
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
                        sender,
                        flush,
                        draining: Arc::new(AtomicBool::new(false)),
                    });
                    let worker_root = root.clone();
                    let worker_options = options.clone();
                    let draining = Arc::clone(&owner.draining);
                    std::thread::Builder::new().name("delta-commit".into()).spawn(move || {
                        run_worker(&worker_root, worker_options, receiver, flush_rx, draining);
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
        receive
            .await
            .context("Delta worker stopped during initialization")?
            .map_err(anyhow::Error::msg)
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
            .map_err(anyhow::Error::msg)
    }

    /// Admit one eligible durable file, waiting asynchronously when capacity is full.
    ///
    /// # Errors
    /// Returns admission/worker errors. The receipt separately reports publication errors.
    pub async fn submit(&self, append: StagedAppend, force: bool) -> Result<CommitReceipt> {
        let slot = Arc::clone(&self.slots).acquire_owned().await?;
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(Request::Publish(Box::new(Publication {
                append,
                reply,
                _slot: slot,
                force,
            })))
            .await
            .context("Delta commit worker stopped")?;
        Ok(CommitReceipt(receive))
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

fn run_worker(
    root: &Path,
    options: CommitOptions,
    mut receiver: mpsc::Receiver<Request>,
    mut flush: watch::Receiver<u64>,
    draining: Arc<AtomicBool>,
) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build();
    let table = DeltaTable::open(root);
    let mut failure = table.as_ref().err().map(|error| format!("{error:#}"));
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
                        let _ = reply.send(failure.as_ref().map_or(Ok(()), |error| Err(error.clone())));
                    }
                    Some(Request::NewFile(schema, reply)) => {
                        let result = match (&table, &failure) {
                            (Ok(table), None) => table.new_file(schema).map_err(|error| format!("{error:#}")),
                            _ => Err(failure
                                .clone()
                                .unwrap_or_else(|| "Delta table initialization failed".into())),
                        };
                        if let Err(error) = &result {
                            failure = Some(error.clone());
                            publish_batch(&table, &mut batch, &mut failure);
                            deadline = None;
                        }
                        let _ = reply.send(result);
                    }
                    Some(Request::Publish(publication)) => {
                        if let Some(error) = &failure {
                            let _ = publication.reply.send(Err(error.clone()));
                            continue;
                        }
                        let force = publication.force;
                        batch.push(*publication);
                        if deadline.is_none() {
                            deadline = Some(Instant::now() + options.batch_window);
                        }
                        if force || draining.load(Ordering::Acquire) || batch.len() >= options.max_files_per_commit {
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
            let message = format!("Delta worker runtime failed: {error}");
            while let Some(request) = receiver.blocking_recv() {
                match request {
                    Request::Ready(reply) => {
                        let _ = reply.send(Err(message.clone()));
                    }
                    Request::NewFile(_, reply) => {
                        let _ = reply.send(Err(message.clone()));
                    }
                    Request::Publish(publication) => {
                        let _ = publication.reply.send(Err(message.clone()));
                    }
                }
            }
        }
    }
}

fn publish_batch(table: &Result<Arc<DeltaTable>>, batch: &mut Vec<Publication>, failure: &mut Option<String>) {
    if batch.is_empty() {
        return;
    }
    let publications = std::mem::take(batch);
    let (appends, replies): (Vec<_>, Vec<_>) = publications.into_iter().map(|p| (p.append, (p.reply, p._slot))).unzip();
    let result = match (table, failure.as_ref()) {
        (Ok(table), None) => table.commit_files(&appends).map_err(|error| format!("{error:#}")),
        _ => Err(failure.clone().unwrap_or_else(|| "Delta table unavailable".into())),
    };
    if let Err(error) = &result {
        *failure = Some(error.clone());
    }
    for (append, (reply, _slot)) in appends.into_iter().zip(replies) {
        let result = result
            .as_ref()
            .map(|version| AppendResult {
                version: *version,
                file: append.file,
            })
            .map_err(Clone::clone);
        // Cancellation never replays a possibly committed file. A live waiter
        // receives the result; an abandoned file stays visible only if committed.
        let _ = reply.send(result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};

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
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )?;
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
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), a.wait()).await??.version,
            1
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), b.wait()).await??.version,
            1
        );
        Ok(())
    }
}
