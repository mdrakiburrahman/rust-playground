//! Durable, single-writer streaming checkpoint using Spark Structured Streaming's on-disk
//! layout for a one-source, stateless micro-batch query.
//!
//! Layout below the checkpoint root (byte formats are pinned by golden tests):
//!
//! * `metadata`: `{"id":"<uuid>"}` with no header and no trailing newline. Stable query identity.
//! * `offsets/<batch>`: `v1\n{"batchWatermarkMs":0,"batchTimestampMs":<ms>,"conf":{}}\n<cursor>`
//!   where `<cursor>` is the compact JSON of the source-owned cursor, without a trailing newline.
//!   It is written ahead of the sink write.
//! * `commits/<batch>`: `v1\n{"nextBatchWatermarkMs":0}`, written after the sink write succeeded.
//! * Immutable documents created with [`Checkpoint::write_immutable`] (for example the source
//!   manifest or per-batch plans). Spark ignores unknown root entries.
//!
//! Batch ids are `i64`, start at 0 and increase by one. Every log entry is created exactly once:
//! re-creating an entry with identical bytes is a no-op, different bytes are a conflict. Only the
//! v1 offset and commit formats are accepted; newer versions (offset v2, commit v2/v3) are
//! rejected explicitly instead of being misread. Only the file formats match Spark; resuming a
//! Spark query from this checkpoint, or the reverse, is not supported.
//!
//! # Single writer
//!
//! * [`Checkpoint::open_local`] holds a non-blocking exclusive OS lock (`fs2`) on
//!   `<root>/writer.flock` for the lifetime of the value. The OS drops it when the process dies.
//!   Files are written to a temporary sibling, `fsync`ed, published with a no-clobber hard link
//!   and its directories up to the root are `fsync`ed (directory `fsync` is skipped on non-Unix
//!   platforms). Visibility is not persistence: an entry that already exists (a retried write,
//!   recovery, `metadata` at open, or a found immutable document) is `fsync`ed again, file and
//!   directories, before it is accepted, so a publication whose `fsync` failed is never trusted.
//! * [`Checkpoint::open_store`] creates `<prefix>/writer.lock` with a create-only put. The lock has
//!   no expiry and is never stolen automatically. [`Checkpoint::close`] releases it with an
//!   ETag/version-conditional update to a `released` state; a later writer takes over a released
//!   lock with the same kind of conditional update. When the store cannot do conditional updates
//!   (or returns neither ETag nor version), release fails closed and leaves the lock held. A
//!   crashed writer, or a dropped value, also leaves the lock held, so the next open fails with
//!   [`CheckpointLocked`]. There is no expiry and no automatic steal: only an operator who has
//!   confirmed that every previous writer process for this prefix has stopped may run
//!   [`Checkpoint::clear_store_lock`].
//! * Every checkpoint write first re-reads the lock and refuses to continue if this writer no
//!   longer owns it. That check is a read before the write, not atomic with it, and sink writes
//!   are not fenced at all. Clearing the lock while an old writer is still alive is therefore
//!   unsafe: the old writer can finish an in-flight checkpoint write and any sink write. This
//!   module does not provide atomic sink fencing. Durability is whatever the store guarantees for
//!   a completed put; no fsync is claimed for object stores. Stores without create-only puts are
//!   rejected.
//!
//! * [`Checkpoint::open_store_with_authority`] additionally runs a caller-supplied
//!   [`StoreAuthority`] check immediately before and immediately after every object-store
//!   mutation (lock create, takeover and release, `metadata`, offsets, commits and immutable
//!   documents). A failed check, including one after a put that already landed, fences the handle
//!   permanently: the operation reports failure, progress does not advance, and no later mutation
//!   (including lock release in [`Checkpoint::close`]) is attempted. A lock left behind this way
//!   needs the same operator clear as a crash. Plain [`Checkpoint::open_store`] runs no such check;
//!   callers using it are responsible for any storage generation fencing.
//!
//! Every read is bounded before its bytes are buffered: log entries, `metadata` and immutable
//! documents are limited to [`MAX_DOCUMENT_BYTES`], the store lock to 4 KiB.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{ErrorKind, Write};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use futures::StreamExt;
use object_store::path::Path as StorePath;
use object_store::{GetOptions, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde_json::Value;
use uuid::Uuid;

const METADATA: &str = "metadata";
const OFFSETS: &str = "offsets";
const COMMITS: &str = "commits";
const LOCAL_LOCK: &str = "writer.flock";
const STORE_LOCK: &str = "writer.lock";
const RESERVED_NAMES: &[&str] = &[METADATA, OFFSETS, COMMITS, "state", "sources", LOCAL_LOCK, STORE_LOCK];
const COMMIT_V1: &[u8] = b"v1\n{\"nextBatchWatermarkMs\":0}";
/// Largest checkpoint entry (log file, `metadata` or immutable document) read or written.
pub const MAX_DOCUMENT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_LOCK_BYTES: u64 = 4 * 1024;

/// Where a recovered query resumes.
#[derive(Debug, Clone, PartialEq)]
pub struct Recovery {
    /// The next batch to run: the pending batch if one exists, otherwise the batch after the last commit.
    pub batch_id: i64,
    /// The planned cursor of a batch whose offset was written but whose commit was not. Replay it unchanged.
    pub pending_offset: Option<Value>,
    /// The cursor of `batch_id - 1`, which is where `batch_id` starts reading.
    pub previous_offset: Option<Value>,
}

/// Another writer owns the checkpoint. Downcast an [`anyhow::Error`] to detect it.
#[derive(Debug, Clone)]
pub struct CheckpointLocked {
    /// The lock file or object.
    pub location: String,
    /// What is known about the current holder.
    pub holder: String,
}

impl fmt::Display for CheckpointLocked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "checkpoint lock {} is held by another writer ({}); if that writer crashed and is confirmed \
             stopped, an operator may clear the lock",
            self.location, self.holder
        )
    }
}

impl std::error::Error for CheckpointLocked {}

/// External fencing consulted immediately before and after every object-store mutation made by a
/// checkpoint opened with [`Checkpoint::open_store_with_authority`]. Return an error when the
/// storage generation captured at open time is no longer current or cannot be confirmed.
#[async_trait::async_trait]
pub trait StoreAuthority: Send + Sync {
    /// Confirms this process may still mutate the store.
    ///
    /// # Errors
    ///
    /// Returns an error when the authority is lost, changed, or cannot be verified.
    async fn check(&self) -> Result<()>;
}

#[derive(Clone, Default)]
struct Guard {
    authority: Option<Arc<dyn StoreAuthority>>,
    fenced: Arc<AtomicBool>,
}

impl fmt::Debug for Guard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Guard")
            .field("authority", &self.authority.is_some())
            .field("fenced", &self.fenced.load(Ordering::SeqCst))
            .finish()
    }
}

impl Guard {
    fn new(authority: Option<Arc<dyn StoreAuthority>>) -> Self {
        Self {
            authority,
            fenced: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn check(&self, when: &str, path: &StorePath) -> Result<()> {
        ensure!(
            !self.fenced.load(Ordering::SeqCst),
            "storage authority failed earlier; this checkpoint writer is fenced and must stop"
        );
        let Some(authority) = &self.authority else {
            return Ok(());
        };
        if let Err(e) = authority.check().await {
            self.fenced.store(true, Ordering::SeqCst);
            return Err(e.context(format!(
                "storage authority check {when} mutating {path} failed; this checkpoint writer is fenced and must stop"
            )));
        }
        Ok(())
    }

    /// Runs `op` between two authority checks. A failed check after `op` wins over its result.
    async fn mutate<R, F, Fut>(&self, path: &StorePath, op: F) -> Result<R>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = R>,
    {
        self.check("before", path).await?;
        let result = op().await;
        self.check("after", path).await?;
        Ok(result)
    }
}

/// A Spark-format checkpoint owned by exactly one writer.
#[derive(Debug)]
pub struct Checkpoint {
    backend: Backend,
    query_id: String,
    progress: Option<Progress>,
}

#[derive(Debug, Clone, Copy)]
struct Progress {
    next: i64,
    pending: bool,
}

#[derive(Debug)]
enum Backend {
    Local {
        root: PathBuf,
        _lock: File,
    },
    Store {
        store: Arc<dyn ObjectStore>,
        prefix: StorePath,
        token: String,
        guard: Guard,
    },
}

enum Created {
    New,
    Existing(Vec<u8>),
}

impl Checkpoint {
    /// Opens or creates a checkpoint in a local directory and locks it for this process. Every
    /// open `fsync`s `offsets/`, `commits/`, the root and each ancestor of the absolute root, so a
    /// freshly created root (including one created by the caller) is durable before it is used.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointLocked`] when another handle holds the lock, and an error for I/O
    /// failures, a corrupt `metadata` file, or logs without `metadata`.
    pub async fn open_local(root: impl AsRef<FsPath>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).with_context(|| format!("creating checkpoint directory {}", root.display()))?;
        let lock_path = root.join(LOCAL_LOCK);
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("opening {}", lock_path.display()))?;
        if let Err(e) = fs2::FileExt::try_lock_exclusive(&lock) {
            let contended = e.kind() == ErrorKind::WouldBlock
                || (e.raw_os_error().is_some() && e.raw_os_error() == fs2::lock_contended_error().raw_os_error());
            if contended {
                return Err(CheckpointLocked {
                    location: lock_path.display().to_string(),
                    holder: "another open handle holds the OS file lock".into(),
                }
                .into());
            }
            return Err(e).with_context(|| format!("locking {}", lock_path.display()));
        }
        for dir in [OFFSETS, COMMITS] {
            let dir = root.join(dir);
            fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
            remove_stale_temp_files(&dir)?;
            sync_dir(&dir)?;
        }
        remove_stale_temp_files(&root)?;
        sync_dir(&root)?;
        // The root and any ancestors may have just been created (here or by the caller); fsync the
        // whole absolute ancestry so the root's own directory entry survives a power loss.
        let absolute = fs::canonicalize(&root).with_context(|| format!("resolving {}", root.display()))?;
        for ancestor in absolute.ancestors().skip(1) {
            sync_dir(ancestor)?;
        }
        Self::init(Backend::Local { root, _lock: lock }).await
    }

    /// Opens or creates a checkpoint under `prefix` in an object store and takes its writer lock.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointLocked`] when the lock object is held (including by a crashed writer),
    /// and an error when the store lacks create-only puts, on store failures, a corrupt
    /// `metadata` object, or logs without `metadata`.
    ///
    /// No external storage authority is consulted; the caller is responsible for any generation
    /// fencing. Use [`Self::open_store_with_authority`] when one exists.
    pub async fn open_store(store: Arc<dyn ObjectStore>, prefix: StorePath) -> Result<Self> {
        Self::open_store_guarded(store, prefix, Guard::new(None)).await
    }

    /// Like [`Self::open_store`], but runs `authority.check()` immediately before and after every
    /// object-store mutation, starting with the lock acquisition. A failed check fences this
    /// handle for good (see the module docs).
    ///
    /// # Errors
    ///
    /// As [`Self::open_store`], plus authority failures.
    pub async fn open_store_with_authority(
        store: Arc<dyn ObjectStore>,
        prefix: StorePath,
        authority: Arc<dyn StoreAuthority>,
    ) -> Result<Self> {
        Self::open_store_guarded(store, prefix, Guard::new(Some(authority))).await
    }

    async fn open_store_guarded(store: Arc<dyn ObjectStore>, prefix: StorePath, guard: Guard) -> Result<Self> {
        let token = Uuid::new_v4().to_string();
        acquire_store_lock(store.as_ref(), &prefix, &token, &guard).await?;
        let backend = Backend::Store {
            store: Arc::clone(&store),
            prefix: prefix.clone(),
            token: token.clone(),
            guard: guard.clone(),
        };
        match Self::init(backend).await {
            Ok(checkpoint) => Ok(checkpoint),
            Err(e) => {
                let _ = release_store_lock(store.as_ref(), &prefix, &token, &guard).await;
                Err(e)
            }
        }
    }

    /// Operator-only recovery: deletes the store writer lock unconditionally and reports whether
    /// it existed. Never called automatically. Run it only after confirming that every previous
    /// writer process for this prefix has stopped. Clearing while an old writer is alive is unsafe:
    /// its ownership check is not atomic with its writes and sink writes are not fenced, so two
    /// writers can interleave.
    ///
    /// # Errors
    ///
    /// Returns store failures.
    pub async fn clear_store_lock(store: Arc<dyn ObjectStore>, prefix: StorePath) -> Result<bool> {
        clear_lock(store, prefix, Guard::new(None)).await
    }

    /// [`Self::clear_store_lock`] with `authority.check()` immediately before and after the delete.
    /// The same operator-only rules apply.
    ///
    /// # Errors
    ///
    /// Returns store failures and authority failures.
    pub async fn clear_store_lock_with_authority(
        store: Arc<dyn ObjectStore>,
        prefix: StorePath,
        authority: Arc<dyn StoreAuthority>,
    ) -> Result<bool> {
        clear_lock(store, prefix, Guard::new(Some(authority))).await
    }

    async fn init(backend: Backend) -> Result<Self> {
        let id = match backend.read(None, METADATA).await? {
            Some(bytes) => {
                backend.ensure_durable(None, METADATA)?;
                decode_metadata(&bytes).with_context(|| backend.describe(None, METADATA))?
            }
            None => {
                let offsets = batch_ids(&backend, OFFSETS).await?;
                let commits = batch_ids(&backend, COMMITS).await?;
                ensure!(
                    offsets.is_empty() && commits.is_empty(),
                    "checkpoint {} has offset/commit logs but no metadata; refusing to invent a new query id",
                    backend.describe(None, "")
                );
                let id = Uuid::new_v4();
                match backend.create(None, METADATA, &encode_metadata(&id)).await? {
                    Created::New => id,
                    Created::Existing(bytes) => decode_metadata(&bytes)?,
                }
            }
        };
        Ok(Self {
            backend,
            query_id: id.hyphenated().to_string(),
            progress: None,
        })
    }

    /// The stable query id stored in `metadata`.
    #[must_use]
    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    /// Validates the logs and returns where to resume. Must be called before [`Self::prepare`].
    ///
    /// # Errors
    ///
    /// Fails on holes in either log, commits without offsets, more than one uncommitted batch,
    /// unsupported versions, malformed entries, or a lost writer lock.
    pub async fn recover(&mut self) -> Result<Recovery> {
        self.backend.ensure_owner().await?;
        let last_offset = last_contiguous(batch_ids(&self.backend, OFFSETS).await?, OFFSETS)?;
        let last_commit = last_contiguous(batch_ids(&self.backend, COMMITS).await?, COMMITS)?;
        let (batch_id, pending) = match (last_offset, last_commit) {
            (None, None) => (0, false),
            (Some(o), Some(c)) if o == c => (o.checked_add(1).ok_or_else(|| anyhow!("batch id overflow"))?, false),
            (Some(o), Some(c)) if o == c + 1 => (o, true),
            (Some(0), None) => (0, true),
            (o, c) => bail!(
                "inconsistent checkpoint: last offset batch {o:?}, last commit batch {c:?}; commits must \
                 trail offsets by at most one batch"
            ),
        };
        if let Some(c) = last_commit {
            let bytes = self.read_required(COMMITS, c).await?;
            decode_commit(&bytes).with_context(|| self.backend.describe(Some(COMMITS), &c.to_string()))?;
        }
        let pending_offset = if pending {
            Some(self.read_offset(batch_id).await?.source)
        } else {
            None
        };
        let previous_offset = if batch_id > 0 {
            Some(self.read_offset(batch_id - 1).await?.source)
        } else {
            None
        };
        self.progress = Some(Progress {
            next: batch_id,
            pending,
        });
        Ok(Recovery {
            batch_id,
            pending_offset,
            previous_offset,
        })
    }

    /// Writes `offsets/<batch_id>` ahead of the sink write. For a pending batch returned by
    /// [`Self::recover`] (or one already prepared), it only verifies that `offset` is unchanged.
    ///
    /// # Errors
    ///
    /// Fails when called before [`Self::recover`], for an out-of-order batch, a negative
    /// timestamp, a different cursor for a pending batch, conflicting existing bytes, or a lost lock.
    pub async fn prepare(&mut self, batch_id: i64, offset: &Value, timestamp_ms: i64) -> Result<()> {
        let progress = self
            .progress
            .ok_or_else(|| anyhow!("recover() must be called before prepare()"))?;
        ensure!(
            batch_id == progress.next,
            "cannot prepare batch {batch_id}; the next batch is {}",
            progress.next
        );
        let bytes = encode_offset(offset, timestamp_ms)?;
        self.backend.ensure_owner().await?;
        if progress.pending {
            let stored = self.read_offset(batch_id).await?;
            ensure!(
                stored.source == *offset,
                "batch {batch_id} is already planned with cursor {}; replay it with that cursor instead of {offset}",
                stored.source
            );
            return Ok(());
        }
        self.publish(Some(OFFSETS), &batch_id.to_string(), &bytes).await?;
        self.progress = Some(Progress {
            next: batch_id,
            pending: true,
        });
        Ok(())
    }

    /// Writes `commits/<batch_id>` after the sink write for that batch succeeded.
    ///
    /// # Errors
    ///
    /// Fails when `batch_id` is not the prepared batch, on conflicting existing bytes, or a lost lock.
    pub async fn commit(&mut self, batch_id: i64) -> Result<()> {
        let progress = self
            .progress
            .ok_or_else(|| anyhow!("recover() must be called before commit()"))?;
        ensure!(
            progress.pending && progress.next == batch_id,
            "cannot commit batch {batch_id}: it has not been prepared"
        );
        let next = batch_id.checked_add(1).ok_or_else(|| anyhow!("batch id overflow"))?;
        self.backend.ensure_owner().await?;
        self.publish(Some(COMMITS), &batch_id.to_string(), COMMIT_V1).await?;
        self.progress = Some(Progress { next, pending: false });
        Ok(())
    }

    /// Creates the immutable document `name` once. Identical bytes are accepted again; different
    /// bytes are an error. `name` is one or more `/`-separated segments matching
    /// `[A-Za-z0-9][A-Za-z0-9._-]*` (at most 255 bytes in total), for example `bootstrap.json` or
    /// `batches/7.json`; its first segment must not be a checkpoint entry such as `offsets`.
    ///
    /// # Errors
    ///
    /// Fails for invalid or reserved names, conflicting bytes, I/O errors, or a lost lock.
    pub async fn write_immutable(&self, name: &str, bytes: &[u8]) -> Result<()> {
        validate_name(name)?;
        check_size(bytes.len(), name)?;
        self.backend.ensure_owner().await?;
        self.publish(None, name, bytes).await
    }

    /// Reads a document written with [`Self::write_immutable`]. On a local checkpoint a found
    /// document is `fsync`ed (file and directories) before it is returned, so a document that is
    /// visible after an earlier failed publication is never reused without being durable.
    ///
    /// # Errors
    ///
    /// Fails for invalid or reserved names, documents larger than [`MAX_DOCUMENT_BYTES`], and I/O errors.
    pub async fn read_immutable(&self, name: &str) -> Result<Option<Vec<u8>>> {
        validate_name(name)?;
        let bytes = self.backend.read(None, name).await?;
        if bytes.is_some() {
            self.backend.ensure_durable(None, name)?;
        }
        Ok(bytes)
    }

    /// Releases the writer lock. Dropping a local checkpoint also releases it; dropping a store
    /// checkpoint leaves the lock held, exactly like a crash.
    ///
    /// # Errors
    ///
    /// Fails when the store lock is no longer owned by this writer or on store failures.
    pub async fn close(self) -> Result<()> {
        match &self.backend {
            Backend::Local { .. } => Ok(()),
            Backend::Store {
                store,
                prefix,
                token,
                guard,
            } => release_store_lock(store.as_ref(), prefix, token, guard).await,
        }
    }

    async fn publish(&self, dir: Option<&str>, name: &str, bytes: &[u8]) -> Result<()> {
        match self.backend.create(dir, name, bytes).await? {
            Created::New => Ok(()),
            Created::Existing(existing) if existing == bytes => Ok(()),
            Created::Existing(_) => bail!(
                "checkpoint conflict: {} already exists with different content; another writer used this checkpoint",
                self.backend.describe(dir, name)
            ),
        }
    }

    async fn read_required(&self, dir: &str, batch_id: i64) -> Result<Vec<u8>> {
        let name = batch_id.to_string();
        let bytes = self.backend.read(Some(dir), &name).await?.ok_or_else(|| {
            anyhow!(
                "{} disappeared during recovery",
                self.backend.describe(Some(dir), &name)
            )
        })?;
        self.backend.ensure_durable(Some(dir), &name)?;
        Ok(bytes)
    }

    async fn read_offset(&self, batch_id: i64) -> Result<OffsetRecord> {
        let bytes = self.read_required(OFFSETS, batch_id).await?;
        decode_offset(&bytes).with_context(|| self.backend.describe(Some(OFFSETS), &batch_id.to_string()))
    }
}
impl Backend {
    fn describe(&self, dir: Option<&str>, name: &str) -> String {
        match self {
            Self::Local { root, .. } => local_path(root, dir, name).display().to_string(),
            Self::Store { prefix, .. } => store_path(prefix, dir, name).to_string(),
        }
    }

    async fn read(&self, dir: Option<&str>, name: &str) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Local { root, .. } => {
                let path = local_path(root, dir, name);
                match File::open(&path) {
                    Ok(file) => local_read_bounded(file, &path).map(Some),
                    Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
                }
            }
            Self::Store { store, prefix, .. } => {
                store_read(store.as_ref(), &store_path(prefix, dir, name), MAX_DOCUMENT_BYTES).await
            }
        }
    }

    /// Local only: re-establishes durability of an entry that is visible but may come from a
    /// publication whose `fsync` failed. Visibility alone is never treated as persistence.
    fn ensure_durable(&self, dir: Option<&str>, name: &str) -> Result<()> {
        match self {
            Self::Local { root, .. } => local_make_durable(root, &local_path(root, dir, name)),
            Self::Store { .. } => Ok(()),
        }
    }

    async fn create(&self, dir: Option<&str>, name: &str, bytes: &[u8]) -> Result<Created> {
        match self {
            Self::Local { root, .. } => local_create(root, &local_path(root, dir, name), bytes),
            Self::Store {
                store, prefix, guard, ..
            } => {
                let path = store_path(prefix, dir, name);
                let put = guard
                    .mutate(&path, || {
                        store.put_opts(
                            &path,
                            PutPayload::from(bytes.to_vec()),
                            PutOptions::from(PutMode::Create),
                        )
                    })
                    .await?;
                match put {
                    Ok(_) => Ok(Created::New),
                    Err(object_store::Error::AlreadyExists { .. }) => {
                        store_read(store.as_ref(), &path, MAX_DOCUMENT_BYTES)
                            .await?
                            .map(Created::Existing)
                            .ok_or_else(|| anyhow!("{path} reported as existing but cannot be read"))
                    }
                    Err(e @ object_store::Error::NotImplemented { .. }) => {
                        Err(anyhow!(e)
                            .context("object store does not support create-only puts; refusing to checkpoint"))
                    }
                    Err(e) => Err(anyhow!(e).context(format!("creating {path}"))),
                }
            }
        }
    }

    async fn list(&self, dir: &str) -> Result<Vec<String>> {
        match self {
            Self::Local { root, .. } => {
                let path = root.join(dir);
                let entries = match fs::read_dir(&path) {
                    Ok(entries) => entries,
                    Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
                    Err(e) => return Err(e).with_context(|| format!("listing {}", path.display())),
                };
                entries
                    .map(|entry| {
                        let entry = entry.with_context(|| format!("listing {}", path.display()))?;
                        entry
                            .file_name()
                            .into_string()
                            .map_err(|n| anyhow!("non UTF-8 entry {n:?} in {}", path.display()))
                    })
                    .collect()
            }
            Self::Store { store, prefix, .. } => {
                let path = prefix.clone().join(dir);
                let listed = store
                    .list_with_delimiter(Some(&path))
                    .await
                    .with_context(|| format!("listing {path}"))?;
                let mut names: Vec<String> = listed
                    .objects
                    .iter()
                    .filter_map(|meta| meta.location.filename().map(str::to_owned))
                    .collect();
                names.extend(
                    listed
                        .common_prefixes
                        .iter()
                        .filter_map(|p| p.filename().map(str::to_owned)),
                );
                Ok(names)
            }
        }
    }

    async fn ensure_owner(&self) -> Result<()> {
        let Self::Store {
            store,
            prefix,
            token,
            guard,
        } = self
        else {
            return Ok(());
        };
        ensure!(
            !guard.fenced.load(Ordering::SeqCst),
            "storage authority failed earlier; this checkpoint writer is fenced and must stop"
        );
        let path = prefix.clone().join(STORE_LOCK);
        let bytes = store_read(store.as_ref(), &path, MAX_LOCK_BYTES)
            .await?
            .ok_or_else(|| anyhow!("writer lock {path} was removed; this writer is fenced and must stop"))?;
        let owned = matches!(decode_lock(&bytes), Ok((state, owner)) if state == "held" && owner == *token);
        ensure!(
            owned,
            "writer lock {path} is no longer owned by this writer; this writer is fenced and must stop"
        );
        Ok(())
    }
}

async fn batch_ids(backend: &Backend, dir: &str) -> Result<Vec<i64>> {
    let mut ids = Vec::new();
    for name in backend.list(dir).await? {
        if name.starts_with('.') {
            continue;
        }
        let canonical =
            !name.is_empty() && name.bytes().all(|b| b.is_ascii_digit()) && (name == "0" || !name.starts_with('0'));
        let id = canonical.then(|| name.parse::<i64>().ok()).flatten();
        ids.push(id.ok_or_else(|| anyhow!("unexpected entry {name:?} in {}", backend.describe(Some(dir), "")))?);
    }
    Ok(ids)
}

fn last_contiguous(mut ids: Vec<i64>, dir: &str) -> Result<Option<i64>> {
    ids.sort_unstable();
    for (expected, id) in (0_i64..).zip(&ids) {
        ensure!(
            *id == expected,
            "hole in {dir} log: expected batch {expected}, found {id}"
        );
    }
    Ok(ids.last().copied())
}

fn validate_name(name: &str) -> Result<()> {
    let segment_ok = |s: &str| {
        s.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    };
    ensure!(
        name.len() <= 255 && name.split('/').all(segment_ok),
        "invalid checkpoint document name {name:?}"
    );
    let first = name.split('/').next().unwrap_or_default();
    ensure!(
        !RESERVED_NAMES.contains(&first),
        "checkpoint document name {name:?} is reserved"
    );
    Ok(())
}

fn local_path(root: &FsPath, dir: Option<&str>, name: &str) -> PathBuf {
    let mut path = root.to_path_buf();
    path.extend(dir.into_iter().chain(name.split('/')).filter(|s| !s.is_empty()));
    path
}

fn local_create(root: &FsPath, path: &FsPath, bytes: &[u8]) -> Result<Created> {
    let dir = path
        .parent()
        .ok_or_else(|| anyhow!("{} has no parent directory", path.display()))?;
    fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("entry");
    let temp = dir.join(format!(".{name}.{}.tmp", Uuid::new_v4().simple()));
    let written = (|| -> std::io::Result<()> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()
    })();
    let published = written.and_then(|()| fs::hard_link(&temp, path));
    let _ = fs::remove_file(&temp);
    match published {
        Ok(()) => {
            sync_dirs_to_root(root, dir)?;
            Ok(Created::New)
        }
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {
            let file = File::open(path).with_context(|| format!("reading {}", path.display()))?;
            let existing = local_read_bounded(file, path)?;
            // The entry may be visible from an earlier attempt whose fsync failed; make it durable
            // before reporting it as persisted.
            local_make_durable(root, path)?;
            Ok(Created::Existing(existing))
        }
        Err(e) => Err(e).with_context(|| format!("publishing {}", path.display())),
    }
}

fn remove_stale_temp_files(dir: &FsPath) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') && name.ends_with(".tmp") && entry.file_type()?.is_file() {
            fs::remove_file(entry.path()).with_context(|| format!("removing {}", entry.path().display()))?;
        }
    }
    Ok(())
}

/// `fsync`s `path` and every directory from its parent up to and including `root`.
fn local_make_durable(root: &FsPath, path: &FsPath) -> Result<()> {
    OpenOptions::new()
        .read(true)
        .write(cfg!(not(unix)))
        .open(path)
        .and_then(|f| f.sync_all())
        .with_context(|| format!("fsync of {}", path.display()))?;
    let dir = path
        .parent()
        .ok_or_else(|| anyhow!("{} has no parent directory", path.display()))?;
    sync_dirs_to_root(root, dir)
}

/// `fsync`s `dir` and each ancestor up to and including `root` (`dir` must be below `root`), so
/// newly created subdirectories and their entries are durable.
fn sync_dirs_to_root(root: &FsPath, dir: &FsPath) -> Result<()> {
    let mut current = dir;
    loop {
        sync_dir(current)?;
        if current == root {
            return Ok(());
        }
        current = current
            .parent()
            .filter(|p| p.starts_with(root))
            .ok_or_else(|| anyhow!("{} is not below checkpoint root {}", dir.display(), root.display()))?;
    }
}

fn sync_dir(dir: &FsPath) -> Result<()> {
    #[cfg(test)]
    tests::fault::on_sync_dir(dir)?;
    sync_dir_os(dir)
}

#[cfg(unix)]
fn sync_dir_os(dir: &FsPath) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("fsync of directory {}", dir.display()))
}

#[cfg(not(unix))]
fn sync_dir_os(_dir: &FsPath) -> Result<()> {
    Ok(())
}

fn store_path(prefix: &StorePath, dir: Option<&str>, name: &str) -> StorePath {
    dir.into_iter()
        .chain(name.split('/'))
        .filter(|s| !s.is_empty())
        .fold(prefix.clone(), |p, s| p.join(s))
}

fn check_size(len: usize, what: &str) -> Result<()> {
    ensure!(
        u64::try_from(len).is_ok_and(|n| n <= MAX_DOCUMENT_BYTES),
        "{what} is {len} bytes; checkpoint entries are limited to {MAX_DOCUMENT_BYTES} bytes"
    );
    Ok(())
}

fn local_read_bounded(file: File, path: &FsPath) -> Result<Vec<u8>> {
    use std::io::Read;
    let len = file
        .metadata()
        .with_context(|| format!("reading {}", path.display()))?
        .len();
    ensure!(
        len <= MAX_DOCUMENT_BYTES,
        "{} is {len} bytes; limit is {MAX_DOCUMENT_BYTES}",
        path.display()
    );
    let mut bytes = Vec::new();
    file.take(MAX_DOCUMENT_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("reading {}", path.display()))?;
    check_size(bytes.len(), &path.display().to_string())?;
    Ok(bytes)
}

async fn bounded_bytes(got: object_store::GetResult, path: &StorePath, limit: u64) -> Result<Vec<u8>> {
    ensure!(
        got.meta.size <= limit,
        "{path} is {} bytes; limit is {limit}",
        got.meta.size
    );
    let mut stream = got.into_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("reading {path}"))?;
        bytes.extend_from_slice(&chunk);
        ensure!(
            u64::try_from(bytes.len()).is_ok_and(|n| n <= limit),
            "{path} exceeds {limit} bytes"
        );
    }
    Ok(bytes)
}

async fn store_read(store: &dyn ObjectStore, path: &StorePath, limit: u64) -> Result<Option<Vec<u8>>> {
    match store.get_opts(path, GetOptions::default()).await {
        Ok(got) => Ok(Some(bounded_bytes(got, path, limit).await?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(anyhow!(e).context(format!("reading {path}"))),
    }
}

async fn clear_lock(store: Arc<dyn ObjectStore>, prefix: StorePath, guard: Guard) -> Result<bool> {
    let path = prefix.join(STORE_LOCK);
    if store_read(store.as_ref(), &path, MAX_LOCK_BYTES).await?.is_none() {
        return Ok(false);
    }
    guard.mutate(&path, || store_delete(store.as_ref(), &path)).await??;
    Ok(true)
}

async fn store_delete(store: &dyn ObjectStore, path: &StorePath) -> Result<()> {
    let target = path.clone();
    let mut results =
        store.delete_stream(futures::stream::once(async move { Ok::<_, object_store::Error>(target) }).boxed());
    while let Some(result) = results.next().await {
        match result {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(anyhow!(e).context(format!("deleting {path}"))),
        }
    }
    Ok(())
}

fn encode_lock(state: &str, token: &str) -> Vec<u8> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    format!(
        "{{\"state\":\"{state}\",\"owner\":\"{token}\",\"pid\":{},\"atMs\":{now}}}",
        std::process::id()
    )
    .into_bytes()
}

fn decode_lock(bytes: &[u8]) -> Result<(String, String)> {
    let doc: Value = serde_json::from_slice(bytes).context("malformed writer lock")?;
    let field = |key: &str| doc.get(key).and_then(Value::as_str).map(str::to_owned);
    field("state")
        .zip(field("owner"))
        .ok_or_else(|| anyhow!("malformed writer lock"))
}

async fn acquire_store_lock(store: &dyn ObjectStore, prefix: &StorePath, token: &str, guard: &Guard) -> Result<()> {
    let path = prefix.clone().join(STORE_LOCK);
    let held = encode_lock("held", token);
    let created = guard
        .mutate(&path, || {
            store.put_opts(&path, PutPayload::from(held.clone()), PutOptions::from(PutMode::Create))
        })
        .await?;
    match created {
        Ok(_) => return Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {}
        Err(e @ object_store::Error::NotImplemented { .. }) => {
            return Err(anyhow!(e).context("object store does not support create-only puts; cannot lock checkpoint"));
        }
        Err(e) => return Err(anyhow!(e).context(format!("creating writer lock {path}"))),
    }
    let locked = |holder: String| CheckpointLocked {
        location: path.to_string(),
        holder,
    };
    let got = match store.get_opts(&path, GetOptions::default()).await {
        Ok(got) => got,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(locked("lock changed while acquiring; retry".into()).into());
        }
        Err(e) => return Err(anyhow!(e).context(format!("reading writer lock {path}"))),
    };
    let version = UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let bytes = bounded_bytes(got, &path, MAX_LOCK_BYTES).await?;
    let released = matches!(decode_lock(&bytes), Ok((state, _)) if state == "released");
    if !released || (version.e_tag.is_none() && version.version.is_none()) {
        return Err(locked(String::from_utf8_lossy(&bytes).into_owned()).into());
    }
    let taken = guard
        .mutate(&path, || {
            store.put_opts(
                &path,
                PutPayload::from(held),
                PutOptions::from(PutMode::Update(version)),
            )
        })
        .await?;
    match taken {
        Ok(_) => Ok(()),
        Err(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. }) => {
            Err(locked("another writer took the released lock first".into()).into())
        }
        Err(e) => Err(anyhow!(e).context(format!("taking over released writer lock {path}"))),
    }
}

async fn release_store_lock(store: &dyn ObjectStore, prefix: &StorePath, token: &str, guard: &Guard) -> Result<()> {
    let path = prefix.clone().join(STORE_LOCK);
    let got = match store.get_opts(&path, GetOptions::default()).await {
        Ok(got) => got,
        Err(object_store::Error::NotFound { .. }) => bail!("writer lock {path} was removed; nothing to release"),
        Err(e) => return Err(anyhow!(e).context(format!("reading writer lock {path}"))),
    };
    let version = UpdateVersion {
        e_tag: got.meta.e_tag.clone(),
        version: got.meta.version.clone(),
    };
    let bytes = bounded_bytes(got, &path, MAX_LOCK_BYTES).await?;
    let owned = matches!(decode_lock(&bytes), Ok((state, owner)) if state == "held" && owner == token);
    ensure!(owned, "writer lock {path} is not owned by this writer");
    ensure!(
        version.e_tag.is_some() || version.version.is_some(),
        "object store returned no ETag or version for writer lock {path}; leaving it held (fail closed). \
         Clear it with clear_store_lock only after this process has stopped"
    );
    let released = PutPayload::from(encode_lock("released", token));
    let updated = guard
        .mutate(&path, || {
            store.put_opts(&path, released, PutOptions::from(PutMode::Update(version)))
        })
        .await?;
    match updated {
        Ok(_) => Ok(()),
        Err(object_store::Error::NotImplemented { .. }) => bail!(
            "object store cannot conditionally update writer lock {path}; leaving it held (fail closed). \
             Clear it with clear_store_lock only after this process has stopped"
        ),
        Err(object_store::Error::Precondition { .. }) => bail!("writer lock {path} changed during release"),
        Err(e) => Err(anyhow!(e).context(format!("releasing writer lock {path}"))),
    }
}

fn encode_metadata(id: &Uuid) -> Vec<u8> {
    format!("{{\"id\":\"{}\"}}", id.hyphenated()).into_bytes()
}

fn decode_metadata(bytes: &[u8]) -> Result<Uuid> {
    ensure!(!bytes.is_empty(), "checkpoint metadata is empty");
    let doc: Value = serde_json::from_slice(bytes).context("checkpoint metadata is not valid JSON")?;
    let id = doc
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("checkpoint metadata has no string \"id\""))?;
    Uuid::parse_str(id).with_context(|| format!("checkpoint metadata id {id:?} is not a UUID"))
}

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(not(test), allow(dead_code))]
struct OffsetRecord {
    batch_watermark_ms: i64,
    batch_timestamp_ms: i64,
    conf: BTreeMap<String, String>,
    source: Value,
}

fn encode_offset(source: &Value, timestamp_ms: i64) -> Result<Vec<u8>> {
    ensure!(timestamp_ms >= 0, "batch timestamp {timestamp_ms} must not be negative");
    let cursor = serde_json::to_string(source)?;
    check_size(cursor.len(), "source cursor")?;
    Ok(
        format!("v1\n{{\"batchWatermarkMs\":0,\"batchTimestampMs\":{timestamp_ms},\"conf\":{{}}}}\n{cursor}")
            .into_bytes(),
    )
}

fn split_log<'a>(bytes: &'a [u8], kind: &str) -> Result<Vec<&'a str>> {
    let text = std::str::from_utf8(bytes).with_context(|| format!("{kind} log is not UTF-8"))?;
    ensure!(!text.is_empty(), "{kind} log is empty (incomplete write)");
    let lines: Vec<&str> = text.split('\n').collect();
    let digits = lines[0]
        .strip_prefix('v')
        .filter(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()))
        .ok_or_else(|| anyhow!("malformed {kind} log version line {:?}", lines[0]))?;
    match digits.parse::<u64>() {
        Ok(1) => Ok(lines),
        Ok(0) => bail!("malformed {kind} log version {:?}", lines[0]),
        _ => bail!(
            "unsupported {kind} log version {}; only v1 is supported (written by a newer or different writer)",
            lines[0]
        ),
    }
}

fn int_field(doc: &serde_json::Map<String, Value>, key: &str, kind: &str) -> Result<i64> {
    doc.get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("malformed {kind} log: missing integer {key:?}"))
}

fn decode_offset(bytes: &[u8]) -> Result<OffsetRecord> {
    let lines = split_log(bytes, "offset")?;
    ensure!(
        lines.len() == 3,
        "malformed offset log: expected version, metadata and one source offset line, found {} lines",
        lines.len()
    );
    let meta: Value = serde_json::from_str(lines[1]).context("malformed offset log metadata")?;
    let meta = meta
        .as_object()
        .ok_or_else(|| anyhow!("malformed offset log metadata: not an object"))?;
    let conf = match meta.get("conf") {
        None => BTreeMap::new(),
        Some(Value::Object(conf)) => conf
            .iter()
            .map(|(k, v)| {
                v.as_str()
                    .map(|v| (k.clone(), v.to_owned()))
                    .ok_or_else(|| anyhow!("malformed offset log conf value for {k:?}"))
            })
            .collect::<Result<_>>()?,
        Some(_) => bail!("malformed offset log metadata: conf is not an object"),
    };
    ensure!(lines[2] != "-", "offset log has no offset for the source");
    Ok(OffsetRecord {
        batch_watermark_ms: int_field(meta, "batchWatermarkMs", "offset")?,
        batch_timestamp_ms: int_field(meta, "batchTimestampMs", "offset")?,
        conf,
        source: serde_json::from_str(lines[2]).context("malformed source offset JSON")?,
    })
}

fn decode_commit(bytes: &[u8]) -> Result<i64> {
    let lines = split_log(bytes, "commit")?;
    ensure!(
        lines.len() == 2,
        "malformed commit log: expected 2 lines, found {}",
        lines.len()
    );
    let meta: Value = serde_json::from_str(lines[1]).context("malformed commit log metadata")?;
    let meta = meta
        .as_object()
        .ok_or_else(|| anyhow!("malformed commit log metadata: not an object"))?;
    int_field(meta, "nextBatchWatermarkMs", "commit")
}
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    /// Deterministic per-thread directory-fsync fault injection (`#[tokio::test]` runs the
    /// synchronous local I/O on the test thread).
    pub(super) mod fault {
        use std::cell::Cell;
        use std::path::Path;

        thread_local! {
            pub(super) static FAIL: Cell<bool> = const { Cell::new(false) };
            pub(super) static SYNCS: Cell<usize> = const { Cell::new(0) };
            pub(super) static SYNCED: std::cell::RefCell<Vec<std::path::PathBuf>> =
                const { std::cell::RefCell::new(Vec::new()) };
        }

        pub(in super::super) fn on_sync_dir(dir: &Path) -> anyhow::Result<()> {
            SYNCS.with(|c| c.set(c.get() + 1));
            SYNCED.with(|s| s.borrow_mut().push(dir.to_path_buf()));
            anyhow::ensure!(
                !FAIL.with(Cell::get),
                "injected fsync failure of directory {}",
                dir.display()
            );
            Ok(())
        }

        pub(super) fn fail(on: bool) {
            FAIL.with(|f| f.set(on));
        }

        pub(super) fn syncs() -> usize {
            SYNCS.with(Cell::get)
        }

        pub(super) fn take_synced() -> Vec<std::path::PathBuf> {
            SYNCED.with(|s| std::mem::take(&mut *s.borrow_mut()))
        }
    }
    use serde_json::json;

    const ID: &str = "4b0f5c8e-2d1a-4c3b-9e7f-0a1b2c3d4e5f";

    fn err<T: fmt::Debug>(result: Result<T>) -> String {
        format!("{:#}", result.expect_err("expected an error"))
    }

    fn memory() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn prefix() -> StorePath {
        StorePath::from("tenants/t1/_checkpoint")
    }

    async fn put_raw(store: &Arc<dyn ObjectStore>, name: &str, bytes: &[u8]) {
        let path = StorePath::from(format!("tenants/t1/_checkpoint/{name}"));
        store
            .put_opts(&path, PutPayload::from(bytes.to_vec()), PutOptions::default())
            .await
            .unwrap();
    }

    async fn get_raw(store: &Arc<dyn ObjectStore>, name: &str) -> Option<Vec<u8>> {
        store_read(
            store.as_ref(),
            &StorePath::from(format!("tenants/t1/_checkpoint/{name}")),
            MAX_DOCUMENT_BYTES,
        )
        .await
        .unwrap()
    }

    fn write_local(root: &FsPath, rel: &str, bytes: &[u8]) {
        let path = root.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    fn seed_local(root: &FsPath) {
        write_local(root, "metadata", format!("{{\"id\":\"{ID}\"}}").as_bytes());
    }

    fn offset_file(cursor: u32) -> String {
        format!("v1\n{{\"batchWatermarkMs\":0,\"batchTimestampMs\":1,\"conf\":{{}}}}\n{{\"c\":{cursor}}}")
    }

    #[test]
    fn golden_bytes() {
        assert_eq!(
            encode_metadata(&Uuid::parse_str(ID).unwrap()),
            b"{\"id\":\"4b0f5c8e-2d1a-4c3b-9e7f-0a1b2c3d4e5f\"}".to_vec()
        );
        assert_eq!(
            encode_offset(&json!({"cursor": [3, "a\nb"]}), 1_700_000_000_123).unwrap(),
            b"v1\n{\"batchWatermarkMs\":0,\"batchTimestampMs\":1700000000123,\"conf\":{}}\n{\"cursor\":[3,\"a\\nb\"]}"
                .to_vec()
        );
        assert_eq!(COMMIT_V1, b"v1\n{\"nextBatchWatermarkMs\":0}");
        assert_eq!(decode_commit(b"v1\n{\"nextBatchWatermarkMs\":0}").unwrap(), 0);
        assert!(err(encode_offset(&json!(1), -1)).contains("negative"));
    }

    #[test]
    fn decodes_spark_written_offset_with_conf() {
        let bytes = b"v1\n{\"batchWatermarkMs\":5,\"batchTimestampMs\":1700000000000,\"conf\":{\"spark.sql.shuffle.partitions\":\"200\",\"spark.sql.streaming.multipleWatermarkPolicy\":\"min\"}}\n{\"cursor\":7}";
        let record = decode_offset(bytes).unwrap();
        assert_eq!(record.batch_watermark_ms, 5);
        assert_eq!(record.batch_timestamp_ms, 1_700_000_000_000);
        assert_eq!(
            record.conf.get("spark.sql.shuffle.partitions").map(String::as_str),
            Some("200")
        );
        assert_eq!(record.source, json!({"cursor": 7}));
        let ours = encode_offset(&json!({"cursor": 7}), 9).unwrap();
        assert_eq!(decode_offset(&ours).unwrap().source, json!({"cursor": 7}));
    }

    #[test]
    fn rejects_unsupported_versions_and_malformed_logs() {
        let meta = "{\"batchWatermarkMs\":0,\"batchTimestampMs\":1,\"conf\":{}}";
        for (bytes, needle) in [
            (format!("v2\n{meta}\n0:{{}}"), "unsupported offset log version v2"),
            (format!("v99\n{meta}\n{{}}"), "unsupported offset log version v99"),
            (format!("v0\n{meta}\n{{}}"), "malformed offset log version"),
            (format!("x1\n{meta}\n{{}}"), "malformed offset log version"),
            (format!("v\n{meta}\n{{}}"), "malformed offset log version"),
            (String::new(), "empty"),
            ("v1".into(), "found 1 lines"),
            (format!("v1\n{meta}"), "found 2 lines"),
            (format!("v1\n{meta}\n{{}}\n"), "found 4 lines"),
            (format!("v1\n{meta}\n{{}}\n{{}}"), "found 4 lines"),
            (format!("v1\n{meta}\n-"), "no offset for the source"),
            (format!("v1\n{meta}\n{{\"cursor\":"), "malformed source offset JSON"),
            ("v1\n{\"batchTimestampMs\":1}\n{}".into(), "batchWatermarkMs"),
            (
                "v1\n{\"batchWatermarkMs\":0,\"batchTimestampMs\":1,\"conf\":{\"k\":1}}\n{}".into(),
                "conf value",
            ),
            (format!("v1\r\n{meta}\n{{}}"), "malformed offset log version"),
        ] {
            let message = err(decode_offset(bytes.as_bytes()));
            assert!(message.contains(needle), "{bytes:?}: {message}");
        }
        assert!(err(decode_offset(b"v1\n\xff\n{}")).contains("UTF-8"));
        for (bytes, needle) in [
            ("v2\n{\"nextBatchWatermarkMs\":0}", "unsupported commit log version v2"),
            ("v3\n{\"nextBatchWatermarkMs\":0}", "unsupported commit log version v3"),
            ("", "empty"),
            ("v1", "found 1"),
            ("v1\n", "malformed commit log metadata"),
            ("v1\n{}", "nextBatchWatermarkMs"),
            ("v1\n{\"nextBatchWatermarkMs\":0}\n", "found 3"),
        ] {
            let message = err(decode_commit(bytes.as_bytes()));
            assert!(message.contains(needle), "{bytes:?}: {message}");
        }
        for bytes in ["", "{}", "{\"id\":\"nope\"}", "{\"id\":7}", "not json"] {
            assert!(decode_metadata(bytes.as_bytes()).is_err(), "{bytes:?}");
        }
        assert_eq!(
            decode_metadata(format!("{{\"id\":\"{ID}\"}}").as_bytes())
                .unwrap()
                .to_string(),
            ID
        );
    }

    #[tokio::test]
    async fn local_lifecycle_writes_golden_files_and_recovers_after_crashes() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("cp");
        let (a, b, c) = (json!({"cursor": "a"}), json!({"cursor": "b"}), json!({"cursor": "c"}));

        let mut cp = Checkpoint::open_local(&root).await.unwrap();
        let id = cp.query_id().to_owned();
        assert_eq!(
            fs::read(root.join("metadata")).unwrap(),
            format!("{{\"id\":\"{id}\"}}").into_bytes()
        );
        assert_eq!(Uuid::parse_str(&id).unwrap().hyphenated().to_string(), id);
        assert_eq!(
            cp.recover().await.unwrap(),
            Recovery {
                batch_id: 0,
                pending_offset: None,
                previous_offset: None
            }
        );
        cp.prepare(0, &a, 1_700_000_000_000).await.unwrap();
        assert_eq!(
            fs::read(root.join("offsets/0")).unwrap(),
            b"v1\n{\"batchWatermarkMs\":0,\"batchTimestampMs\":1700000000000,\"conf\":{}}\n{\"cursor\":\"a\"}".to_vec()
        );
        cp.commit(0).await.unwrap();
        assert_eq!(
            fs::read(root.join("commits/0")).unwrap(),
            b"v1\n{\"nextBatchWatermarkMs\":0}".to_vec()
        );
        cp.prepare(1, &b, 1_700_000_000_500).await.unwrap();
        drop(cp); // crash after the offset write, before the commit

        let mut cp = Checkpoint::open_local(&root).await.unwrap();
        assert_eq!(cp.query_id(), id);
        let recovery = cp.recover().await.unwrap();
        assert_eq!(
            recovery,
            Recovery {
                batch_id: 1,
                pending_offset: Some(b.clone()),
                previous_offset: Some(a)
            }
        );
        let before = fs::read(root.join("offsets/1")).unwrap();
        cp.prepare(1, &b, 1_800_000_000_000).await.unwrap();
        assert_eq!(fs::read(root.join("offsets/1")).unwrap(), before);
        assert!(err(cp.prepare(1, &c, 1).await).contains("already planned"));
        assert!(err(cp.prepare(2, &c, 1).await).contains("next batch is 1"));
        assert!(err(cp.commit(0).await).contains("not been prepared"));
        cp.commit(1).await.unwrap();
        drop(cp); // crash after the commit

        let mut cp = Checkpoint::open_local(&root).await.unwrap();
        let recovery = cp.recover().await.unwrap();
        assert_eq!(
            recovery,
            Recovery {
                batch_id: 2,
                pending_offset: None,
                previous_offset: Some(b)
            }
        );
        assert!(err(cp.commit(2).await).contains("not been prepared"));
        cp.close().await.unwrap();

        for sub in ["", "offsets", "commits"] {
            for entry in fs::read_dir(root.join(sub)).unwrap() {
                let name = entry.unwrap().file_name().into_string().unwrap();
                assert!(!name.ends_with(".tmp"), "temp file left behind: {name}");
            }
        }
    }

    #[tokio::test]
    async fn requires_recover_before_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut cp = Checkpoint::open_local(dir.path()).await.unwrap();
        assert!(err(cp.prepare(0, &json!(1), 1).await).contains("recover()"));
        assert!(err(cp.commit(0).await).contains("recover()"));
    }

    #[tokio::test]
    async fn local_lock_excludes_concurrent_writers() {
        let dir = tempfile::tempdir().unwrap();
        let first = Checkpoint::open_local(dir.path()).await.unwrap();
        let second = Checkpoint::open_local(dir.path())
            .await
            .expect_err("second writer must fail");
        assert!(second.downcast_ref::<CheckpointLocked>().is_some(), "{second:#}");
        drop(first);
        Checkpoint::open_local(dir.path()).await.unwrap();
    }

    #[tokio::test]
    async fn local_cleans_stale_temp_files_and_ignores_hidden_entries() {
        let dir = tempfile::tempdir().unwrap();
        seed_local(dir.path());
        write_local(dir.path(), "offsets/.0.deadbeef.tmp", b"partial");
        write_local(dir.path(), "offsets/.0.crc", b"crc");
        let mut cp = Checkpoint::open_local(dir.path()).await.unwrap();
        assert!(!dir.path().join("offsets/.0.deadbeef.tmp").exists());
        assert_eq!(cp.query_id(), ID);
        assert_eq!(cp.recover().await.unwrap().batch_id, 0);
    }

    #[tokio::test]
    async fn local_rejects_inconsistent_or_corrupt_checkpoints() {
        let commit = "v1\n{\"nextBatchWatermarkMs\":0}".to_owned();
        let cases: Vec<(Vec<(&str, String)>, &str)> = vec![
            (
                vec![("offsets/0", offset_file(0)), ("offsets/2", offset_file(2))],
                "hole in offsets log",
            ),
            (vec![("offsets/1", offset_file(1))], "hole in offsets log"),
            (
                vec![("offsets/0", offset_file(0)), ("commits/1", commit.clone())],
                "hole in commits log",
            ),
            (vec![("commits/0", commit.clone())], "inconsistent checkpoint"),
            (
                vec![
                    ("offsets/0", offset_file(0)),
                    ("offsets/1", offset_file(1)),
                    ("offsets/2", offset_file(2)),
                    ("commits/0", commit.clone()),
                ],
                "inconsistent checkpoint",
            ),
            (
                vec![("offsets/0", offset_file(0)), ("offsets/1", offset_file(1))],
                "inconsistent checkpoint",
            ),
            (
                vec![("offsets/0", offset_file(0)), ("commits/0", "v3\n{}".into())],
                "unsupported commit log version v3",
            ),
            (
                vec![("offsets/0", offset_file(0)), ("commits/0", String::new())],
                "commit log is empty",
            ),
            (
                vec![("offsets/0", "v1\n{\"batchWatermarkMs\":0".into())],
                "malformed offset log",
            ),
            (
                vec![("offsets/0", "v2\n{}\n0:{}".into())],
                "unsupported offset log version v2",
            ),
            (vec![("offsets/abc", offset_file(0))], "unexpected entry \"abc\""),
            (vec![("offsets/00", offset_file(0))], "unexpected entry \"00\""),
        ];
        for (files, needle) in cases {
            let dir = tempfile::tempdir().unwrap();
            seed_local(dir.path());
            for (rel, bytes) in &files {
                write_local(dir.path(), rel, bytes.as_bytes());
            }
            let mut cp = Checkpoint::open_local(dir.path()).await.unwrap();
            let message = err(cp.recover().await);
            assert!(message.contains(needle), "{files:?}: {message}");
        }

        let dir = tempfile::tempdir().unwrap();
        write_local(dir.path(), "offsets/0", offset_file(0).as_bytes());
        assert!(err(Checkpoint::open_local(dir.path()).await).contains("no metadata"));
        let dir = tempfile::tempdir().unwrap();
        write_local(dir.path(), "metadata", b"{\"id\":\"4b0f5c8e\"}");
        assert!(err(Checkpoint::open_local(dir.path()).await).contains("not a UUID"));
    }

    #[tokio::test]
    async fn conflicting_bytes_fail_and_identical_bytes_reconcile() {
        let dir = tempfile::tempdir().unwrap();
        let mut cp = Checkpoint::open_local(dir.path()).await.unwrap();
        cp.recover().await.unwrap();
        // A write whose outcome was unknown (identical bytes already present) reconciles.
        write_local(dir.path(), "offsets/0", &encode_offset(&json!({"c": 0}), 5).unwrap());
        cp.prepare(0, &json!({"c": 0}), 5).await.unwrap();
        write_local(dir.path(), "commits/0", b"v1\n{\"nextBatchWatermarkMs\":9}");
        assert!(err(cp.commit(0).await).contains("checkpoint conflict"));

        let dir = tempfile::tempdir().unwrap();
        let mut cp = Checkpoint::open_local(dir.path()).await.unwrap();
        cp.recover().await.unwrap();
        write_local(dir.path(), "offsets/0", &encode_offset(&json!({"c": 1}), 5).unwrap());
        assert!(err(cp.prepare(0, &json!({"c": 0}), 5).await).contains("checkpoint conflict"));
    }

    #[tokio::test]
    async fn immutable_documents() {
        let dir = tempfile::tempdir().unwrap();
        let local = Checkpoint::open_local(dir.path()).await.unwrap();
        let store = Checkpoint::open_store(memory(), prefix()).await.unwrap();
        for cp in [&local, &store] {
            for name in ["bootstrap.json", "batches/7.json"] {
                assert_eq!(cp.read_immutable(name).await.unwrap(), None);
                cp.write_immutable(name, b"{\"v\":1}").await.unwrap();
                cp.write_immutable(name, b"{\"v\":1}").await.unwrap();
                assert!(err(cp.write_immutable(name, b"{\"v\":2}").await).contains("conflict"));
                assert_eq!(cp.read_immutable(name).await.unwrap(), Some(b"{\"v\":1}".to_vec()));
            }
            let long = "x".repeat(256);
            for bad in [
                "",
                ".hidden",
                "a//b",
                "a/",
                "/a",
                "a/../b",
                "a\\b",
                "metadata",
                "offsets/9",
                "commits",
                "writer.lock",
                "sources",
                &long,
            ] {
                assert!(cp.write_immutable(bad, b"x").await.is_err(), "{bad:?}");
                assert!(cp.read_immutable(bad).await.is_err(), "{bad:?}");
            }
        }
        assert_eq!(
            fs::read(dir.path().join("batches").join("7.json")).unwrap(),
            b"{\"v\":1}".to_vec()
        );
    }

    #[tokio::test]
    async fn reads_and_writes_are_size_bounded() {
        let over = usize::try_from(MAX_DOCUMENT_BYTES).unwrap() + 1;
        let dir = tempfile::tempdir().unwrap();
        let local = Checkpoint::open_local(dir.path()).await.unwrap();
        File::create(dir.path().join("big.json"))
            .unwrap()
            .set_len(MAX_DOCUMENT_BYTES + 1)
            .unwrap();
        assert!(err(local.read_immutable("big.json").await).contains("limit"));
        assert!(err(local.write_immutable("huge.json", &vec![b'x'; over]).await).contains("limited"));
        assert_eq!(local.read_immutable("huge.json").await.unwrap(), None);

        let store = memory();
        let cp = Checkpoint::open_store(Arc::clone(&store), prefix()).await.unwrap();
        put_raw(&store, "big.json", &vec![b'x'; over]).await;
        assert!(err(cp.read_immutable("big.json").await).contains("limit"));

        let other = memory();
        put_raw(&other, "writer.lock", &vec![b' '; 5000]).await;
        assert!(Checkpoint::open_store(Arc::clone(&other), prefix()).await.is_err());
        assert_eq!(get_raw(&other, "writer.lock").await.unwrap().len(), 5000);
    }

    /// Allows `allowed` more checks, then fails every check.
    #[derive(Debug)]
    struct TestAuthority {
        allowed: std::sync::atomic::AtomicUsize,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl TestAuthority {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                allowed: usize::MAX.into(),
                calls: 0.into(),
            })
        }

        fn allow(&self, n: usize) {
            self.allowed.store(n, Ordering::SeqCst);
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl StoreAuthority for TestAuthority {
        async fn check(&self) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let pass = self
                .allowed
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            ensure!(pass, "storage generation changed");
            Ok(())
        }
    }

    fn dyn_auth(auth: &Arc<TestAuthority>) -> Arc<dyn StoreAuthority> {
        Arc::<TestAuthority>::clone(auth)
    }

    async fn lock_state(store: &Arc<dyn ObjectStore>) -> String {
        decode_lock(&get_raw(store, "writer.lock").await.unwrap()).unwrap().0
    }

    #[tokio::test]
    async fn authority_checked_before_and_after_every_mutation() {
        let store = memory();
        let auth = TestAuthority::new();
        let mut cp = Checkpoint::open_store_with_authority(Arc::clone(&store), prefix(), dyn_auth(&auth))
            .await
            .unwrap();
        assert_eq!(auth.calls(), 4, "lock create + metadata create");
        cp.recover().await.unwrap();
        assert_eq!(auth.calls(), 4, "recover does not mutate");
        cp.write_immutable("bootstrap.json", b"{}").await.unwrap();
        cp.prepare(0, &json!(1), 1).await.unwrap();
        cp.commit(0).await.unwrap();
        assert_eq!(auth.calls(), 10);
        cp.close().await.unwrap();
        assert_eq!(auth.calls(), 12);
        assert_eq!(lock_state(&store).await, "released");

        let cp = Checkpoint::open_store_with_authority(Arc::clone(&store), prefix(), dyn_auth(&auth))
            .await
            .unwrap();
        assert_eq!(
            auth.calls(),
            16,
            "rejected create + conditional takeover; metadata already exists"
        );
        drop(cp);
        assert!(
            Checkpoint::clear_store_lock_with_authority(Arc::clone(&store), prefix(), dyn_auth(&auth))
                .await
                .unwrap()
        );
        assert_eq!(auth.calls(), 18);
        assert_eq!(get_raw(&store, "writer.lock").await, None);
    }

    #[tokio::test]
    async fn authority_failure_before_mutation_writes_nothing_and_fences() {
        let store = memory();
        let auth = TestAuthority::new();
        auth.allow(0);
        let open = Checkpoint::open_store_with_authority(Arc::clone(&store), prefix(), dyn_auth(&auth)).await;
        assert!(err(open).contains("before mutating"));
        assert_eq!(get_raw(&store, "writer.lock").await, None);

        auth.allow(usize::MAX);
        let mut cp = Checkpoint::open_store_with_authority(Arc::clone(&store), prefix(), dyn_auth(&auth))
            .await
            .unwrap();
        cp.recover().await.unwrap();
        auth.allow(0);
        assert!(err(cp.prepare(0, &json!(1), 1).await).contains("before mutating"));
        assert_eq!(get_raw(&store, "offsets/0").await, None);
        auth.allow(usize::MAX);
        let calls = auth.calls();
        assert!(err(cp.prepare(0, &json!(1), 1).await).contains("fenced"));
        assert!(cp.commit(0).await.is_err());
        assert!(err(cp.write_immutable("bootstrap.json", b"{}").await).contains("fenced"));
        assert!(err(cp.recover().await).contains("fenced"));
        assert!(cp.close().await.is_err());
        assert_eq!(auth.calls(), calls, "a fenced handle never consults or mutates again");
        assert_eq!(get_raw(&store, "offsets/0").await, None);
        assert_eq!(get_raw(&store, "bootstrap.json").await, None);
        assert_eq!(lock_state(&store).await, "held");
        assert!(
            Checkpoint::open_store_with_authority(Arc::clone(&store), prefix(), dyn_auth(&auth))
                .await
                .unwrap_err()
                .downcast_ref::<CheckpointLocked>()
                .is_some()
        );
        auth.allow(0);
        assert!(
            Checkpoint::clear_store_lock_with_authority(Arc::clone(&store), prefix(), dyn_auth(&auth))
                .await
                .is_err()
        );
        assert_eq!(lock_state(&store).await, "held");
    }

    #[tokio::test]
    async fn authority_failure_after_mutation_reports_failure_and_stops_progress() {
        let store = memory();
        let auth = TestAuthority::new();
        auth.allow(1);
        let open = Checkpoint::open_store_with_authority(Arc::clone(&store), prefix(), dyn_auth(&auth)).await;
        assert!(err(open).contains("after mutating"));
        assert_eq!(
            lock_state(&store).await,
            "held",
            "landed lock is not released through a stale generation"
        );
        assert_eq!(get_raw(&store, "metadata").await, None);
        Checkpoint::clear_store_lock(Arc::clone(&store), prefix())
            .await
            .unwrap();

        auth.allow(usize::MAX);
        let mut cp = Checkpoint::open_store_with_authority(Arc::clone(&store), prefix(), dyn_auth(&auth))
            .await
            .unwrap();
        cp.recover().await.unwrap();
        cp.prepare(0, &json!(1), 1).await.unwrap();
        auth.allow(1);
        assert!(err(cp.commit(0).await).contains("after mutating"));
        assert!(
            get_raw(&store, "commits/0").await.is_some(),
            "the put landed but was not reported as persisted"
        );
        auth.allow(usize::MAX);
        assert!(err(cp.commit(0).await).contains("fenced"));
        assert!(cp.prepare(1, &json!(2), 2).await.is_err());
        assert!(err(cp.write_immutable("batches/1.json", b"{}").await).contains("fenced"));
        assert!(cp.close().await.is_err());
        assert_eq!(get_raw(&store, "offsets/1").await, None);
        assert_eq!(get_raw(&store, "batches/1.json").await, None);
        assert_eq!(lock_state(&store).await, "held");

        Checkpoint::clear_store_lock(Arc::clone(&store), prefix())
            .await
            .unwrap();
        let mut next = Checkpoint::open_store_with_authority(Arc::clone(&store), prefix(), dyn_auth(&auth))
            .await
            .unwrap();
        assert_eq!(next.recover().await.unwrap().batch_id, 1);
    }

    #[tokio::test]
    async fn local_open_fsyncs_new_root_and_its_whole_ancestry() {
        let dir = tempfile::tempdir().unwrap();
        let base = fs::canonicalize(dir.path()).unwrap();
        let root = base.join("tenants").join("t1").join("cp");

        fault::fail(true);
        assert!(err(Checkpoint::open_local(&root).await).contains("injected fsync"));
        fault::fail(false);
        assert!(
            root.exists(),
            "the root is visible even though its creation was not made durable"
        );

        fault::take_synced();
        let cp = Checkpoint::open_local(&root).await.unwrap();
        let synced = fault::take_synced();
        for expected in [root.join("offsets"), root.join("commits"), root.clone()]
            .into_iter()
            .chain(root.ancestors().skip(1).map(FsPath::to_path_buf))
        {
            assert!(
                synced.contains(&expected),
                "{} not fsynced; synced {synced:?}",
                expected.display()
            );
        }
        drop(cp);

        // Reopening an existing checkpoint re-establishes the same ancestry durability.
        let cp = Checkpoint::open_local(&root).await.unwrap();
        let synced = fault::take_synced();
        assert!(
            synced.contains(&base) && synced.contains(&base.join("tenants")),
            "{synced:?}"
        );
        drop(cp);
    }

    #[tokio::test]
    async fn local_directory_fsync_failure_is_never_treated_as_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("cp");
        let mut cp = Checkpoint::open_local(&root).await.unwrap();
        cp.recover().await.unwrap();

        // Offset is published (visible) but the directory fsync fails.
        fault::fail(true);
        assert!(err(cp.prepare(0, &json!(1), 1).await).contains("injected fsync"));
        assert!(root.join("offsets").join("0").exists());
        // Neither a retry nor recovery may accept the visible entry while durability still fails.
        assert!(err(cp.prepare(0, &json!(1), 1).await).contains("injected fsync"));
        assert!(err(cp.recover().await).contains("injected fsync"));
        fault::fail(false);
        let before = fault::syncs();
        cp.prepare(0, &json!(1), 1).await.unwrap();
        assert!(fault::syncs() >= before + 2, "retry must fsync offsets/ and the root");

        // Same for the commit: a failed fsync never advances, even though commits/0 is visible.
        fault::fail(true);
        assert!(err(cp.commit(0).await).contains("injected fsync"));
        assert!(root.join("commits").join("0").exists());
        assert!(err(cp.commit(0).await).contains("injected fsync"));
        assert!(err(cp.recover().await).contains("injected fsync"));
        fault::fail(false);
        let before = fault::syncs();
        assert_eq!(cp.recover().await.unwrap().batch_id, 1);
        assert!(
            fault::syncs() >= before + 2,
            "recovery must fsync the accepted commit and offset"
        );

        // A new immutable subdirectory: plan visible after a failed fsync is not reused undurably.
        fault::fail(true);
        assert!(err(cp.write_immutable("batches/1.json", b"{}").await).contains("injected fsync"));
        assert!(err(cp.read_immutable("batches/1.json").await).contains("injected fsync"));
        assert!(err(cp.write_immutable("batches/1.json", b"{}").await).contains("injected fsync"));
        fault::fail(false);
        let before = fault::syncs();
        assert_eq!(cp.read_immutable("batches/1.json").await.unwrap(), Some(b"{}".to_vec()));
        assert!(fault::syncs() >= before + 2, "batches/ and the root are fsynced");
        cp.write_immutable("batches/1.json", b"{}").await.unwrap();

        // Opening fails closed on fsync failure; a clean reopen re-fsyncs metadata and resumes.
        drop(cp);
        let other = dir.path().join("fresh");
        fault::fail(true);
        assert!(Checkpoint::open_local(&other).await.is_err());
        fault::fail(false);
        let mut cp = Checkpoint::open_local(&root).await.unwrap();
        assert_eq!(cp.recover().await.unwrap().batch_id, 1);
        cp.prepare(1, &json!(2), 2).await.unwrap();
    }

    #[tokio::test]
    async fn store_keys_are_physical_multi_segment_paths() {
        for (prefix, expected) in [
            (
                "cp",
                [
                    "cp/batches/0.json",
                    "cp/commits/0",
                    "cp/metadata",
                    "cp/offsets/0",
                    "cp/writer.lock",
                ],
            ),
            (
                "tenants/t1/_checkpoint",
                [
                    "tenants/t1/_checkpoint/batches/0.json",
                    "tenants/t1/_checkpoint/commits/0",
                    "tenants/t1/_checkpoint/metadata",
                    "tenants/t1/_checkpoint/offsets/0",
                    "tenants/t1/_checkpoint/writer.lock",
                ],
            ),
        ] {
            let store = memory();
            let mut cp = Checkpoint::open_store(Arc::clone(&store), StorePath::parse(prefix).unwrap())
                .await
                .unwrap();
            cp.recover().await.unwrap();
            cp.write_immutable("batches/0.json", b"{}").await.unwrap();
            cp.prepare(0, &json!(1), 1).await.unwrap();
            cp.commit(0).await.unwrap();
            let mut keys: Vec<StorePath> = store
                .list(None)
                .map(|meta| meta.unwrap().location)
                .collect::<Vec<_>>()
                .await;
            keys.sort();
            let expected: Vec<StorePath> = expected.iter().map(|k| StorePath::parse(k).unwrap()).collect();
            assert_eq!(keys, expected);
            assert!(keys.iter().all(|k| !k.as_ref().contains("%2F")), "{keys:?}");
            assert_eq!(cp.read_immutable("batches/0.json").await.unwrap(), Some(b"{}".to_vec()));
            assert_eq!(cp.recover().await.unwrap().batch_id, 1);
            let offset = StorePath::parse(format!("{prefix}/offsets/0")).unwrap();
            assert_eq!(
                store_read(store.as_ref(), &offset, MAX_DOCUMENT_BYTES)
                    .await
                    .unwrap()
                    .unwrap(),
                b"v1\n{\"batchWatermarkMs\":0,\"batchTimestampMs\":1,\"conf\":{}}\n1".to_vec()
            );
        }
    }

    #[tokio::test]
    async fn store_lifecycle_golden_bytes_and_crash_recovery() {
        let store = memory();
        let mut cp = Checkpoint::open_store(Arc::clone(&store), prefix()).await.unwrap();
        let id = cp.query_id().to_owned();
        assert_eq!(
            get_raw(&store, "metadata").await.unwrap(),
            format!("{{\"id\":\"{id}\"}}").into_bytes()
        );
        assert_eq!(cp.recover().await.unwrap().batch_id, 0);
        cp.prepare(0, &json!({"cursor": 1}), 42).await.unwrap();
        assert_eq!(
            get_raw(&store, "offsets/0").await.unwrap(),
            b"v1\n{\"batchWatermarkMs\":0,\"batchTimestampMs\":42,\"conf\":{}}\n{\"cursor\":1}".to_vec()
        );
        cp.commit(0).await.unwrap();
        assert_eq!(
            get_raw(&store, "commits/0").await.unwrap(),
            b"v1\n{\"nextBatchWatermarkMs\":0}".to_vec()
        );
        cp.prepare(1, &json!({"cursor": 2}), 43).await.unwrap();
        drop(cp); // crash: the store lock stays held

        let locked = Checkpoint::open_store(Arc::clone(&store), prefix())
            .await
            .expect_err("crashed lock must hold");
        assert!(locked.downcast_ref::<CheckpointLocked>().is_some(), "{locked:#}");
        assert!(
            Checkpoint::clear_store_lock(Arc::clone(&store), prefix())
                .await
                .unwrap()
        );
        assert!(
            !Checkpoint::clear_store_lock(Arc::clone(&store), prefix())
                .await
                .unwrap()
        );

        let mut cp = Checkpoint::open_store(Arc::clone(&store), prefix()).await.unwrap();
        assert_eq!(cp.query_id(), id);
        assert_eq!(
            cp.recover().await.unwrap(),
            Recovery {
                batch_id: 1,
                pending_offset: Some(json!({"cursor": 2})),
                previous_offset: Some(json!({"cursor": 1}))
            }
        );
        cp.prepare(1, &json!({"cursor": 2}), 99).await.unwrap();
        cp.commit(1).await.unwrap();
        cp.close().await.unwrap();
        assert_eq!(
            decode_lock(&get_raw(&store, "writer.lock").await.unwrap()).unwrap().0,
            "released"
        );

        let mut cp = Checkpoint::open_store(Arc::clone(&store), prefix()).await.unwrap();
        assert_eq!(cp.recover().await.unwrap().batch_id, 2);
        let busy = Checkpoint::open_store(Arc::clone(&store), prefix())
            .await
            .expect_err("lock is held");
        assert!(busy.downcast_ref::<CheckpointLocked>().is_some());
        cp.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn store_lock_admits_exactly_one_concurrent_writer() {
        for released in [false, true] {
            let store = memory();
            if released {
                Checkpoint::open_store(Arc::clone(&store), prefix())
                    .await
                    .unwrap()
                    .close()
                    .await
                    .unwrap();
            }
            let tasks: Vec<_> = (0..16)
                .map(|_| {
                    let store = Arc::clone(&store);
                    tokio::spawn(async move { Checkpoint::open_store(store, prefix()).await })
                })
                .collect();
            let mut winners = Vec::new();
            for task in tasks {
                match task.await.unwrap() {
                    Ok(cp) => winners.push(cp),
                    Err(e) => assert!(e.downcast_ref::<CheckpointLocked>().is_some(), "{e:#}"),
                }
            }
            assert_eq!(winners.len(), 1, "released={released}");
        }
    }

    #[tokio::test]
    async fn cleared_lock_fences_the_old_writer() {
        let store = memory();
        let mut old = Checkpoint::open_store(Arc::clone(&store), prefix()).await.unwrap();
        old.recover().await.unwrap();
        Checkpoint::clear_store_lock(Arc::clone(&store), prefix())
            .await
            .unwrap();
        assert!(err(old.prepare(0, &json!(1), 1).await).contains("fenced"));
        let mut new = Checkpoint::open_store(Arc::clone(&store), prefix()).await.unwrap();
        new.recover().await.unwrap();
        assert!(err(old.prepare(0, &json!(1), 1).await).contains("fenced"));
        assert!(err(old.write_immutable("bootstrap.json", b"x").await).contains("fenced"));
        assert!(err(old.close().await).contains("not owned"));
        new.prepare(0, &json!(1), 1).await.unwrap();
    }

    #[tokio::test]
    async fn store_rejects_missing_metadata_corruption_and_foreign_writes() {
        let store = memory();
        put_raw(&store, "offsets/0", &encode_offset(&json!(0), 1).unwrap()).await;
        assert!(err(Checkpoint::open_store(Arc::clone(&store), prefix()).await).contains("no metadata"));
        assert_eq!(
            decode_lock(&get_raw(&store, "writer.lock").await.unwrap()).unwrap().0,
            "released"
        );

        let store = memory();
        put_raw(&store, "metadata", b"{\"id\":").await;
        assert!(err(Checkpoint::open_store(Arc::clone(&store), prefix()).await).contains("metadata"));

        let store = memory();
        put_raw(&store, "writer.lock", b"garbage").await;
        let locked = Checkpoint::open_store(Arc::clone(&store), prefix())
            .await
            .expect_err("malformed lock fails closed");
        assert!(locked.downcast_ref::<CheckpointLocked>().is_some());

        let store = memory();
        let mut cp = Checkpoint::open_store(Arc::clone(&store), prefix()).await.unwrap();
        cp.recover().await.unwrap();
        put_raw(&store, "offsets/0", &encode_offset(&json!(0), 1).unwrap()).await;
        cp.prepare(0, &json!(0), 1).await.unwrap();
        put_raw(&store, "commits/0", b"v1\n{\"nextBatchWatermarkMs\":1}").await;
        assert!(err(cp.commit(0).await).contains("checkpoint conflict"));
        put_raw(&store, "offsets/1", &encode_offset(&json!(5), 1).unwrap()).await;
        cp.close().await.unwrap();
        let mut cp = Checkpoint::open_store(Arc::clone(&store), prefix()).await.unwrap();
        // offsets 0..=1 and commit 0 (foreign v1 bytes) is a valid pending state; a later hole is not.
        assert_eq!(cp.recover().await.unwrap().pending_offset, Some(json!(5)));
        put_raw(&store, "offsets/3", &encode_offset(&json!(6), 1).unwrap()).await;
        assert!(err(cp.recover().await).contains("hole in offsets log"));
    }
}
