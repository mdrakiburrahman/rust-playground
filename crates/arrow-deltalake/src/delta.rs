//! Local, append-only Delta tables backed exclusively by Delta Kernel.

use crate::delta_io::{BATCH_ROWS, READ_AHEAD, SharedExecutor, shared_executor};
use crate::{
    CompletedFile, CompletedFileObservation, ParquetFileWriter, normalize, record_batch_logical_bytes, sync_directory,
};
use anyhow::{Context, Result, bail};
use arrow::array::{RecordBatch, StructArray};
use arrow::datatypes::SchemaRef;
use delta_kernel::committer::{CommitMetadata, CommitResponse, Committer, FileSystemCommitter, PublishMetadata};
use delta_kernel::engine::arrow_conversion::{TryFromArrow, TryIntoArrow};
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::schema::StructType;
use delta_kernel::transaction::{BoundWriteContext, CommitResult};
use delta_kernel::{DeltaResult, DeltaResultIterator, Engine, FileMeta, FilteredEngineData, Snapshot, SnapshotRef};
use delta_kernel_default_engine::parquet::DataFileMetadata;
use delta_kernel_default_engine::stats::FileStatsAccumulator;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder, build_add_file_metadata};
use fs2::FileExt;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;
use url::Url;

type LocalEngine = DefaultEngine<SharedExecutor>;

/// Initialize the process-wide Delta I/O runtime before applying thread affinity.
///
/// Both collectors should call this from their unpinned main thread. Table
/// opening initializes it lazily for callers that do not use thread affinity.
/// The runtime uses two async workers and at most 32 blocking workers; engines
/// read ahead eight files and yield at most 256 rows per JSON/Parquet batch.
///
/// # Errors
/// Returns runtime creation errors, including a previous initialization failure.
pub fn initialize_shared_io() -> Result<()> {
    shared_executor().map(|_| ())
}

#[cfg(test)]
mod io_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashSet;

    fn input_batch() -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(arrow::array::Int32Array::from(vec![42]))],
        )?)
    }

    /// Scenario: a concurrent subprocess inherits the writer's descriptor before exec.
    /// Guarantees: the last owner releases its lock even while a duplicate descriptor remains open.
    #[cfg(unix)]
    #[test]
    fn writer_lock_is_released_before_inherited_descriptor_closes() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = DeltaTable::open(directory.path())?;
        let inherited = table._lock.0.try_clone()?;
        let shared = Arc::clone(&table);
        drop(table);
        assert!(DeltaTable::open(directory.path()).is_err());
        drop(shared);
        let reopened = DeltaTable::open(directory.path())?;
        drop(inherited);
        assert!(DeltaTable::open(directory.path()).is_err());
        drop(reopened);
        DeltaTable::open(directory.path())?;
        Ok(())
    }

    #[derive(Default)]
    struct RecordingFence(Mutex<Vec<(u64, u64)>>);

    impl PublicationFence for RecordingFence {
        fn check_before_commit(
            &self,
            context: &PublicationFenceContext<'_>,
        ) -> std::result::Result<(), PublicationFenceError> {
            self.0
                .lock()
                .map_err(|error| PublicationFenceError::check_failed(error.to_string()))?
                .push((context.read_version(), context.proposed_version()));
            Ok(())
        }
    }

    /// Scenario: a warm snapshot is reused while an external transaction evolves the schema.
    /// Guarantees: latest listing advances the base, staged old schemas fail, and fencing uses the fresh version.
    #[tokio::test(flavor = "current_thread")]
    async fn incremental_snapshot_observes_external_schema_and_fresh_fence() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let fence = Arc::new(RecordingFence::default());
        let table = DeltaTable::open_with_fence(directory.path(), Arc::<RecordingFence>::clone(&fence))?;
        let input = input_batch()?;
        let mut file = table.new_file(input.schema())?;
        file.write(&input).await?;
        assert_eq!(file.finish().await?.version, 1);
        let base = table.snapshot()?;
        let unchanged = table.snapshot()?;
        #[cfg(unix)]
        assert!(Arc::ptr_eq(&base, &unchanged));
        assert_eq!(unchanged.version(), 1);
        let mut stale = table.new_file(input.schema())?;
        stale.write(&input).await?;
        let stale = stale.stage().await?;
        // A Kernel writer bypasses the SDK lock and cache, as an external writer
        // would. Its metadata change must be discovered before SDK publication.
        let transaction = base
            .alter_table()
            .add_column(delta_kernel::schema::StructField::new(
                "extra",
                delta_kernel::schema::DataType::STRING,
                true,
            ))
            .build(&table.engine, Box::new(DurableCommitter))?;
        assert_eq!(committed(transaction.commit(&table.engine)?)?, 2);
        assert_eq!(table.version()?, 2);
        assert!(table.snapshot()?.schema().field("extra").is_some());
        assert!(table.commit_files(&[stale]).is_err());
        let mut file = table.new_file(input.schema())?;
        file.write(&input).await?;
        assert_eq!(file.finish().await?.version, 3);
        assert_eq!(
            *fence.0.lock().map_err(|error| anyhow::anyhow!("{error}"))?,
            vec![(0, 1), (2, 3)]
        );
        Ok(())
    }

    /// Scenario: an external writer publishes after a warm snapshot's final freshness check.
    /// Guarantees: atomic version creation still detects conflict and never retries or overwrites it.
    #[tokio::test(flavor = "current_thread")]
    async fn incremental_snapshot_preserves_external_commit_conflict() -> Result<()> {
        use std::io::Write;
        use std::sync::atomic::{AtomicBool, Ordering};

        struct ExternalCommitFence {
            armed: AtomicBool,
        }
        impl PublicationFence for ExternalCommitFence {
            fn check_before_commit(
                &self,
                context: &PublicationFenceContext<'_>,
            ) -> std::result::Result<(), PublicationFenceError> {
                if self.armed.swap(false, Ordering::SeqCst) {
                    let publish = || -> std::io::Result<()> {
                        let log = context.table_path().join("_delta_log");
                        let append = fs::read(log.join(format!("{:020}.json", context.read_version())))?;
                        let mut file = OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(log.join(format!("{:020}.json", context.proposed_version())))?;
                        file.write_all(&append)?;
                        file.sync_all()
                    };
                    publish().map_err(|error| PublicationFenceError::check_failed(error.to_string()))?;
                }
                Ok(())
            }
        }
        let directory = tempfile::tempdir()?;
        let fence = Arc::new(ExternalCommitFence {
            armed: AtomicBool::new(false),
        });
        let table = DeltaTable::open_with_fence(directory.path(), Arc::<ExternalCommitFence>::clone(&fence))?;
        let input = input_batch()?;
        let mut first = table.new_file(input.schema())?;
        first.write(&input).await?;
        assert_eq!(first.finish().await?.version, 1);
        assert_eq!(table.version()?, 1);
        let mut file = table.new_file(input.schema())?;
        file.write(&input).await?;
        fence.armed.store(true, Ordering::SeqCst);
        let error = file.finish().await.err().context("expected external commit conflict")?;
        assert!(error.to_string().contains("conflicted"));
        assert_eq!(table.version()?, 2);
        assert!(!directory.path().join("_delta_log/00000000000000000003.json").exists());
        Ok(())
    }

    /// Scenario: historical JSON is corrupted without changing its length or modification time.
    /// Guarantees: Unix identity/change-time checks invalidate the base and corruption remains visible.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn incremental_snapshot_does_not_mask_historical_corruption() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = DeltaTable::open(directory.path())?;
        let input = input_batch()?;
        table.new_file(input.schema())?;
        let base = table.snapshot()?;
        assert!(Arc::ptr_eq(&base, &table.snapshot()?));
        let path = directory.path().join("_delta_log/00000000000000000000.json");
        let metadata = fs::metadata(&path)?;
        fs::write(&path, vec![b'!'; usize::try_from(metadata.len())?])?;
        File::open(path)?.set_times(std::fs::FileTimes::new().set_modified(metadata.modified()?))?;
        assert!(table.version().is_err());
        assert!(table.new_file(input.schema()).is_err());
        assert!(
            table
                .snapshot_cache
                .lock()
                .map_err(|error| anyhow::anyhow!("{error}"))?
                .is_none()
        );
        Ok(())
    }

    /// Scenario: an otherwise unchanged local log gains a nonstandard entry.
    /// Guarantees: unsupported layouts disable reuse without changing Kernel's normal loading behavior.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn incremental_snapshot_falls_back_for_nonstandard_layout() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = DeltaTable::open(directory.path())?;
        table.new_file(input_batch()?.schema())?;
        let base = table.snapshot()?;
        assert!(Arc::ptr_eq(&base, &table.snapshot()?));
        let extra = directory.path().join("_delta_log/cache-observer");
        fs::write(&extra, b"not a Delta log")?;
        assert_eq!(table.version()?, 0);
        assert!(
            table
                .snapshot_cache
                .lock()
                .map_err(|error| anyhow::anyhow!("{error}"))?
                .is_none()
        );
        fs::remove_file(extra)?;
        assert_eq!(table.version()?, 0);
        assert!(Arc::ptr_eq(&table.snapshot()?, &table.snapshot()?));
        Ok(())
    }

    /// Scenario: an external writer evolves metadata and checkpoints after a JSON-only base was cached.
    /// Guarantees: the full loader observes new schema/fence versions and checkpoint corruption is surfaced.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn incremental_snapshot_falls_back_for_new_checkpoint() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let fence = Arc::new(RecordingFence::default());
        let table = DeltaTable::open_with_fence(directory.path(), Arc::<RecordingFence>::clone(&fence))?;
        let input = input_batch()?;
        let mut file = table.new_file(input.schema())?;
        file.write(&input).await?;
        assert_eq!(file.finish().await?.version, 1);
        let base = table.snapshot()?;
        assert!(Arc::ptr_eq(&base, &table.snapshot()?));
        let mut stale = table.new_file(input.schema())?;
        stale.write(&input).await?;
        let stale = stale.stage().await?;
        let transaction = base
            .alter_table()
            .add_column(delta_kernel::schema::StructField::new(
                "checkpoint_extra",
                delta_kernel::schema::DataType::STRING,
                true,
            ))
            .build(&table.engine, Box::new(DurableCommitter))?;
        assert_eq!(committed(transaction.commit(&table.engine)?)?, 2);
        let external = Snapshot::builder_for(table.url.as_str()).build(&table.engine)?;
        let writer = external.create_checkpoint_writer(&table.engine)?;
        let data = writer.checkpoint_data(&table.engine)?;
        let state = data.state();
        // Drain Kernel's lazy replay on the caller, not inside the asynchronous
        // Parquet writer where it would recursively block the I/O workers.
        let data = data
            .map(|batch| batch.and_then(|batch| batch.apply_selection_vector()))
            .collect::<DeltaResult<Vec<_>>>()?;
        let checkpoint_url = writer.checkpoint_path()?;
        table
            .engine
            .parquet_handler()
            .write_parquet_file(checkpoint_url.clone(), Box::new(data.into_iter().map(Ok)))?;
        let checkpoint = checkpoint_url
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("invalid checkpoint URL"))?;
        let state = Arc::into_inner(state).context("checkpoint state remains shared")?;
        let statistics = delta_kernel::checkpoint::LastCheckpointHintStats::from_reconciliation_state(
            state,
            fs::metadata(&checkpoint)?.len(),
            0,
        )?;
        writer.finalize(&table.engine, &statistics)?;
        let log = directory.path().join("_delta_log");
        assert!(log.join("_last_checkpoint").exists());
        assert_eq!(table.version()?, 2);
        assert!(table.snapshot()?.schema().field("checkpoint_extra").is_some());
        assert!(table.commit_files(&[stale]).is_err());
        assert!(
            table
                .snapshot_cache
                .lock()
                .map_err(|error| anyhow::anyhow!("{error}"))?
                .is_none()
        );
        let mut file = table.new_file(input.schema())?;
        file.write(&input).await?;
        assert_eq!(file.finish().await?.version, 3);
        assert_eq!(
            *fence.0.lock().map_err(|error| anyhow::anyhow!("{error}"))?,
            vec![(0, 1), (2, 3)]
        );
        fs::write(checkpoint, b"corrupt checkpoint")?;
        assert!(table.version().is_err());
        Ok(())
    }

    /// Scenario: thirteen tables accumulate 16 then 32 real appends, retaining every log across reopen.
    /// Guarantees: current-thread calls share one runtime, replay growing histories, and keep I/O workers bounded.
    #[tokio::test(flavor = "current_thread")]
    async fn thirteen_tables_share_bounded_executor_with_growing_retained_histories() -> Result<()> {
        initialize_shared_io()?;
        initialize_shared_io()?;
        let directory = tempfile::tempdir()?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int32Array::from(vec![42]))],
        )?;
        let mut tables = Vec::new();
        let mut runtimes = HashSet::new();
        for index in 0..13 {
            let table = DeltaTable::open(directory.path().join(index.to_string()))?;
            {
                let _guard = table.engine.enter();
                runtimes.insert(tokio::runtime::Handle::current().id());
            }
            table.new_file(Arc::clone(&schema))?;
            assert_eq!(table.version()?, 0);
            tables.push(table);
        }
        assert_eq!(runtimes.len(), 1);
        for (previous_version, version) in [(0, 16), (16, 32)] {
            for table in &tables {
                for expected_version in previous_version + 1..=version {
                    let mut file = table.new_file(input.schema())?;
                    file.write(&input).await?;
                    assert_eq!(file.finish().await?.version, expected_version);
                }
                assert_eq!(table.version()?, version);
            }
            #[cfg(target_os = "linux")]
            crate::delta_io::assert_worker_bound()?;
            drop(tables);
            tables = Vec::new();
            for index in 0..13 {
                let path = directory.path().join(index.to_string());
                let table = DeltaTable::open(&path)?;
                assert_eq!(table.version()?, version);
                let log = path.join("_delta_log");
                let retained_json_logs = fs::read_dir(&log)?
                    .map(|entry| {
                        entry.map(|entry| {
                            usize::from(entry.path().extension().is_some_and(|extension| extension == "json"))
                        })
                    })
                    .collect::<std::io::Result<Vec<_>>>()?
                    .into_iter()
                    .sum::<usize>();
                assert_eq!(u64::try_from(retained_json_logs)?, version + 1);
                assert!(log.join("00000000000000000000.json").exists());
                {
                    let _guard = table.engine.enter();
                    assert!(runtimes.contains(&tokio::runtime::Handle::current().id()));
                }
                tables.push(table);
            }
            assert_eq!(tables.len(), 13);
            #[cfg(target_os = "linux")]
            crate::delta_io::assert_worker_bound()?;
        }
        Ok(())
    }

    /// Scenario: thirteen current-thread commit workers concurrently replay 650 retained logs per table.
    /// Guarantees: read-ahead does not exhaust task admission, publication stays fresh, and workers stay bounded.
    #[tokio::test(flavor = "current_thread")]
    async fn thirteen_concurrent_workers_replay_650_retained_logs() -> Result<()> {
        use crate::commit::{BatchedTable, CommitOptions};
        use std::time::{Duration, Instant};

        initialize_shared_io()?;
        let directory = tempfile::tempdir()?;
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(arrow::array::Int32Array::from(vec![42]))],
        )?;
        for index in 0..13 {
            let path = directory.path().join(index.to_string());
            let table = DeltaTable::open(&path)?;
            let mut file = table.new_file(input.schema())?;
            file.write(&input).await?;
            assert_eq!(file.finish().await?.version, 1);
            // Append-only fixtures without metadata force schema replay back to
            // version zero; these are synthetic histories, not 650 real appends.
            let log = path.join("_delta_log");
            let append = fs::read(log.join("00000000000000000001.json"))?;
            for version in 2..650 {
                fs::write(log.join(format!("{version:020}.json")), &append)?;
            }
        }
        let barrier = Arc::new(tokio::sync::Barrier::new(13));
        let started = Instant::now();
        let operations = (0..13).map(|index| {
            let path = directory.path().join(index.to_string());
            let input = input.clone();
            let barrier = Arc::clone(&barrier);
            async move {
                let table = BatchedTable::open(path, CommitOptions::default()).await?;
                barrier.wait().await;
                for version in 650..=651 {
                    let mut file = table.new_file(input.schema()).await?;
                    file.write(&input).await?;
                    let append = file.stage().await?;
                    assert_eq!(table.submit(append, true).await?.wait_typed().await?.version, version);
                }
                Ok::<_, anyhow::Error>(table)
            }
        });
        let results = tokio::time::timeout(Duration::from_secs(60), futures::future::join_all(operations)).await?;
        let tables = results.into_iter().collect::<Result<Vec<_>>>()?;
        assert_eq!(tables.len(), 13);
        #[cfg(target_os = "linux")]
        crate::delta_io::assert_worker_bound()?;
        for index in 0..13 {
            let log = directory.path().join(index.to_string()).join("_delta_log");
            assert!(log.join("00000000000000000000.json").exists());
            assert!(log.join("00000000000000000649.json").exists());
            assert!(log.join("00000000000000000651.json").exists());
        }
        println!(
            "13 concurrent 650-log replays and 26 fresh commits completed in {:?}",
            started.elapsed()
        );
        Ok(())
    }

    /// Scenario: a large JSON commit spans more rows than the SDK's batch limit.
    /// Guarantees: the shared engine yields bounded batches without losing or reordering rows.
    #[tokio::test(flavor = "current_thread")]
    async fn json_batches_have_bounded_rows() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = DeltaTable::open(directory.path())?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let path = directory.path().join("rows.json");
        let rows = (0..1025).map(|id| format!("{{\"id\":{id}}}\n")).collect::<String>();
        fs::write(&path, &rows)?;
        let url = Url::from_file_path(&path).map_err(|()| anyhow::anyhow!("invalid JSON path"))?;
        let files = [FileMeta::new(url, 0, u64::try_from(rows.len())?)];
        let batches =
            table
                .engine
                .json_handler()
                .read_json_files(&files, Arc::new(StructType::try_from_arrow(schema)?), None)?;
        let mut count = 0;
        for batch in batches {
            let batch = batch?;
            let batch = batch.try_into_record_batch()?;
            assert!((1..=BATCH_ROWS).contains(&batch.num_rows()));
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .context("unexpected JSON column type")?;
            for id in ids.values() {
                assert_eq!(*id, count);
                count += 1;
            }
        }
        assert_eq!(count, 1025);
        Ok(())
    }
}

// Older and renamed writers must contend for the same persisted lock.
const WRITER_LOCK: &str = ".arrow-lake-writer.lock";

/// Immutable details passed to a publication fence immediately before commit.
#[derive(Clone, Copy, Debug)]
pub struct PublicationFenceContext<'a> {
    table_path: &'a Path,
    read_version: u64,
    proposed_version: u64,
    file_count: usize,
}

impl PublicationFenceContext<'_> {
    /// Canonical local table path.
    #[must_use]
    pub const fn table_path(&self) -> &Path {
        self.table_path
    }

    /// Snapshot version used to build the pending append.
    #[must_use]
    pub const fn read_version(&self) -> u64 {
        self.read_version
    }

    /// Version the pending append intends to publish.
    #[must_use]
    pub const fn proposed_version(&self) -> u64 {
        self.proposed_version
    }

    /// Number of completed files in the pending append.
    #[must_use]
    pub const fn file_count(&self) -> usize {
        self.file_count
    }
}

/// Machine-readable category returned by a [`PublicationFence`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PublicationFenceErrorKind {
    /// The caller no longer owns the table generation or lease.
    StaleOwnership,
    /// Ownership could not be checked, so publication must stop.
    CheckFailed,
}

/// A structured rejection from a caller-provided publication fence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationFenceError {
    kind: PublicationFenceErrorKind,
    message: String,
}

impl PublicationFenceError {
    /// Reject publication because the expected table ownership is stale.
    #[must_use]
    pub fn stale(message: impl Into<String>) -> Self {
        Self {
            kind: PublicationFenceErrorKind::StaleOwnership,
            message: message.into(),
        }
    }

    /// Reject publication because the ownership check itself failed.
    #[must_use]
    pub fn check_failed(message: impl Into<String>) -> Self {
        Self {
            kind: PublicationFenceErrorKind::CheckFailed,
            message: message.into(),
        }
    }

    /// Return the machine-readable rejection category.
    #[must_use]
    pub const fn kind(&self) -> PublicationFenceErrorKind {
        self.kind
    }

    /// Return caller-provided diagnostic detail.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for PublicationFenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "Delta publication fence rejected commit: {}", self.message)
    }
}

impl Error for PublicationFenceError {}

/// Synchronous, storage-agnostic ownership check at the Delta commit boundary.
///
/// Implementations should compare an expected lease/generation with caller-owned
/// state and return promptly. The SDK does not retry a rejection.
pub trait PublicationFence: Send + Sync + 'static {
    /// Check ownership after the transaction is prepared and immediately before
    /// its versioned Delta log commit.
    ///
    /// # Errors
    /// Returns a stale-ownership or check-failure rejection. The SDK propagates
    /// it without retrying the pending commit.
    fn check_before_commit(
        &self,
        context: &PublicationFenceContext<'_>,
    ) -> std::result::Result<(), PublicationFenceError>;
}

/// Local table ownership and Kernel engine. Hold one instance per table.
pub struct DeltaTable {
    root: PathBuf,
    url: Url,
    engine: LocalEngine,
    _lock: WriterLock,
    commit_lock: Mutex<()>,
    snapshot_cache: Mutex<Option<CachedSnapshot>>,
    reuse_snapshots: bool,
    publication_fence: Option<Arc<dyn PublicationFence>>,
}

/// Exclusive local writer ownership, released when its owner drops.
struct WriterLock(File);

impl Drop for WriterLock {
    fn drop(&mut self) {
        // A concurrently spawned child briefly shares this lock until it executes;
        // closing alone would keep ownership. Close still releases it on failure.
        let _ = FileExt::unlock(&self.0);
    }
}

type LogFingerprints = BTreeMap<u64, LogFingerprint>;

struct CachedSnapshot {
    snapshot: SnapshotRef,
    files: LogFingerprints,
}

#[derive(Debug, PartialEq, Eq)]
struct LogFingerprint {
    length: u64,
    modified: std::time::SystemTime,
    #[cfg(unix)]
    identity: (u64, u64, i64, i64),
}

struct DurableCommitter;

impl Committer for DurableCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: DeltaResultIterator<'_, FilteredEngineData>,
        metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        let response = FileSystemCommitter::new().commit(engine, actions, metadata)?;
        if let CommitResponse::Committed { file_meta } = &response {
            let path = file_meta
                .location
                .to_file_path()
                .map_err(|_| delta_kernel::Error::generic("non-local commit location"))?;
            File::open(&path)?.sync_all()?;
            let parent = path
                .parent()
                .ok_or_else(|| delta_kernel::Error::generic("missing log parent"))?;
            File::open(parent)?.sync_all()?;
            if let Some(root) = parent.parent() {
                File::open(root)?.sync_all()?;
            }
        }
        Ok(response)
    }

    fn is_catalog_committer(&self) -> bool {
        false
    }

    fn publish(&self, engine: &dyn Engine, metadata: PublishMetadata) -> DeltaResult<()> {
        FileSystemCommitter::new().publish(engine, metadata)
    }
}

fn committed<S>(result: CommitResult<S>) -> Result<u64> {
    match result {
        CommitResult::Committed(commit) => Ok(commit.commit_version()),
        CommitResult::Conflicted(_) => {
            bail!("Delta commit conflicted with an external writer; publication was not retried")
        }
        CommitResult::Retryable(retry) => bail!(
            "Delta commit failed; retry not attempted after uncertain I/O: {}",
            retry.error
        ),
    }
}

impl DeltaTable {
    /// Acquire exclusive local writer ownership, creating the directory if needed.
    ///
    /// # Errors
    /// Rejects another writer, invalid paths, and filesystem errors.
    pub fn open(root: impl AsRef<Path>) -> Result<Arc<Self>> {
        Self::open_inner(
            root,
            Arc::new(delta_kernel::object_store::local::LocalFileSystem::new()),
            None,
            true,
        )
    }

    /// Acquire local writer ownership with a check immediately before each data commit.
    ///
    /// # Errors
    /// Rejects another writer, invalid paths, and filesystem errors.
    pub fn open_with_fence(root: impl AsRef<Path>, publication_fence: Arc<dyn PublicationFence>) -> Result<Arc<Self>> {
        Self::open_inner(
            root,
            Arc::new(delta_kernel::object_store::local::LocalFileSystem::new()),
            Some(publication_fence),
            true,
        )
    }

    /// Inject storage for local-file URLs (for example, a fault-injecting store).
    /// Cloud URLs and cloud commit semantics are deliberately not supported.
    ///
    /// # Errors
    /// Returns local ownership and filesystem errors.
    pub fn open_with_store(
        root: impl AsRef<Path>,
        store: Arc<dyn delta_kernel::object_store::ObjectStore>,
    ) -> Result<Arc<Self>> {
        Self::open_inner(root, store, None, false)
    }

    /// Inject local-URL storage and a publication fence.
    ///
    /// # Errors
    /// Returns local ownership and filesystem errors.
    pub fn open_with_store_and_fence(
        root: impl AsRef<Path>,
        store: Arc<dyn delta_kernel::object_store::ObjectStore>,
        publication_fence: Arc<dyn PublicationFence>,
    ) -> Result<Arc<Self>> {
        Self::open_inner(root, store, Some(publication_fence), false)
    }

    fn open_inner(
        root: impl AsRef<Path>,
        store: Arc<dyn delta_kernel::object_store::ObjectStore>,
        publication_fence: Option<Arc<dyn PublicationFence>>,
        reuse_snapshots: bool,
    ) -> Result<Arc<Self>> {
        fs::create_dir_all(root.as_ref())?;
        let root = fs::canonicalize(root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(WRITER_LOCK))?;
        lock.try_lock_exclusive()
            .with_context(|| format!("Delta table already has a writer: {}", root.display()))?;
        let lock = WriterLock(lock);
        let url = Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("invalid table path"))?;
        let engine = DefaultEngineBuilder::new(store)
            .with_task_executor(shared_executor()?)
            .with_buffer_size(NonZeroUsize::new(READ_AHEAD).context("invalid Delta read-ahead")?)
            .with_batch_size(NonZeroUsize::new(BATCH_ROWS).context("invalid Delta batch size")?)
            .build();
        Ok(Arc::new(Self {
            root,
            url,
            engine,
            _lock: lock,
            commit_lock: Mutex::new(()),
            snapshot_cache: Mutex::new(None),
            reuse_snapshots,
            publication_fence,
        }))
    }

    fn snapshot(&self) -> Result<SnapshotRef> {
        let full_build = || {
            Snapshot::builder_for(self.url.as_str())
                .build(&self.engine)
                .map_err(anyhow::Error::new)
        };
        // Injected stores need not reflect filesystem metadata faithfully.
        if !self.reuse_snapshots || !cfg!(unix) {
            return full_build();
        }
        let mut cache = self
            .snapshot_cache
            .lock()
            .map_err(|_| anyhow::anyhow!("Delta snapshot cache lock poisoned"))?;
        let previous = cache.take();
        let before = self.log_fingerprints()?;
        let base = previous.filter(|previous| {
            before.as_ref().is_some_and(|files| {
                previous
                    .files
                    .iter()
                    .all(|(version, fingerprint)| files.get(version) == Some(fingerprint))
            })
        });
        // JSON-only bases need only the new listing tail. Both integrity scans
        // still cover all history; a newly appearing checkpoint forces full loading.
        // No pinned version or connector-supplied freshness is used.
        let snapshot = match &base {
            Some(base) => Snapshot::builder_from(Arc::clone(&base.snapshot))
                .skip_new_checkpoints()
                .build(&self.engine)?,
            None => full_build()?,
        };
        let after = self.log_fingerprints()?;
        if let (Some(before), Some(mut after)) = (before, after) {
            after.retain(|version, _| *version <= snapshot.version());
            if before.range(..=snapshot.version()).eq(after.iter()) {
                *cache = Some(CachedSnapshot {
                    snapshot: Arc::clone(&snapshot),
                    files: after,
                });
                return Ok(snapshot);
            }
        }
        // Mutation during an incremental load invalidates its historical baseline.
        // Never retain a snapshot with fingerprints captured after that mutation.
        if base.is_some() {
            return full_build();
        }
        Ok(snapshot)
    }

    fn log_fingerprints(&self) -> Result<Option<LogFingerprints>> {
        let mut files = BTreeMap::new();
        for entry in fs::read_dir(self.root.join("_delta_log"))? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(version) = name
                .to_str()
                .and_then(|name| name.strip_suffix(".json"))
                .filter(|version| version.len() == 20 && version.bytes().all(|byte| byte.is_ascii_digit()))
            else {
                // Checkpoints, CRCs, and nonstandard log layouts keep the existing
                // full-build behavior rather than adding cache rules for them.
                return Ok(None);
            };
            let Ok(version) = version.parse::<u64>() else {
                return Ok(None);
            };
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            if !metadata.is_file() {
                return Ok(None);
            }
            #[cfg(unix)]
            let identity = {
                use std::os::unix::fs::MetadataExt;
                (metadata.dev(), metadata.ino(), metadata.ctime(), metadata.ctime_nsec())
            };
            files.insert(
                version,
                LogFingerprint {
                    length: metadata.len(),
                    modified: metadata.modified()?,
                    #[cfg(unix)]
                    identity,
                },
            );
        }
        Ok(Some(files))
    }

    /// Initialize or evolve the schema using a native Kernel transaction.
    ///
    /// This publishes metadata immediately. Mediated publishers must call it
    /// only on their private preparation table, never on the reader-visible table.
    ///
    /// # Errors
    /// Returns incompatible schema, ownership, storage, or Kernel errors.
    pub fn ensure_schema(&self, incoming: SchemaRef) -> Result<SnapshotRef> {
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("Delta commit lock poisoned"))?;
        if !self.root.join("_delta_log").exists() {
            for entry in fs::read_dir(&self.root)? {
                let entry = entry?;
                if entry.file_name() != WRITER_LOCK {
                    bail!("refusing to adopt non-Delta directory {}", self.root.display());
                }
            }
            let schema = Arc::new(StructType::try_from_arrow(Arc::clone(&incoming))?);
            let transaction =
                delta_kernel::transaction::create_table::create_table(self.url.as_str(), schema, "arrow-deltalake")
                    .build(&self.engine, Box::new(DurableCommitter))?;
            committed(transaction.commit(&self.engine)?)?;
        }
        let snapshot = self.snapshot()?;
        let existing: arrow::datatypes::Schema = snapshot.schema().as_ref().try_into_arrow()?;
        for field in existing.fields() {
            if !field.is_nullable() && incoming.fields().find(field.name()).is_none() {
                bail!("missing required column {}", field.name());
            }
        }
        let mut additions = Vec::new();
        for field in incoming.fields() {
            if let Some((_, old)) = existing.fields().find(field.name()) {
                if old.data_type() != field.data_type() {
                    bail!("incompatible type/nested evolution for {}", field.name());
                }
                if old.metadata().get(normalize::ORIGINAL_TYPE) != field.metadata().get(normalize::ORIGINAL_TYPE) {
                    bail!("incompatible logical Arrow type for {}", field.name());
                }
            } else {
                if !field.is_nullable() {
                    bail!("new column {} must be nullable", field.name());
                }
                additions.push(delta_kernel::schema::StructField::try_from_arrow(field.as_ref())?);
            }
        }
        if !additions.is_empty() {
            let mut additions = additions.into_iter();
            let first = additions.next().context("missing schema addition")?;
            let mut builder = snapshot.alter_table().add_column(first);
            for field in additions {
                builder = builder.add_column(field);
            }
            let transaction = builder.build(&self.engine, Box::new(DurableCommitter))?;
            committed(transaction.commit(&self.engine)?)?;
            return self.snapshot();
        }
        // Preparation and publication both refresh storage state; the cache only
        // avoids re-parsing historical metadata after checking log integrity.
        Ok(snapshot)
    }

    /// Open a streaming file using a raw Arrow schema, evolving nullable top-level columns.
    ///
    /// # Errors
    /// Returns ownership, schema, Kernel, and file creation errors.
    pub fn new_file(self: &Arc<Self>, input: SchemaRef) -> Result<DeltaFileWriter> {
        let snapshot = self.ensure_schema(normalize::schema(&input)?)?;
        let schema = Arc::new(snapshot.schema().as_ref().try_into_arrow()?);
        let transaction = snapshot.transaction(Box::new(DurableCommitter), &self.engine)?;
        let context = transaction.write_state()?.write_context_builder().build()?;
        let path = self.root.join(format!("part-{}.parquet", uuid::Uuid::new_v4()));
        let physical = Arc::new(context.physical_data_schema().as_ref().try_into_arrow()?);
        let parquet = ParquetFileWriter::create(path, physical)?;
        let stats = FileStatsAccumulator::new(context.stats_columns(), context.physical_data_schema());
        Ok(DeltaFileWriter {
            table: Arc::clone(self),
            schema,
            context,
            parquet,
            stats,
            failed: false,
        })
    }

    /// Latest committed version, for diagnostics.
    ///
    /// # Errors
    /// Returns snapshot/log errors.
    pub fn version(&self) -> Result<u64> {
        Ok(self.snapshot()?.version())
    }

    pub(crate) fn commit_files(&self, files: &[StagedAppend]) -> Result<u64> {
        if files.is_empty() {
            bail!("cannot publish an empty Delta data commit");
        }
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("Delta commit lock poisoned"))?;
        let snapshot = self.snapshot()?;
        let read_version = snapshot.version();
        let proposed_version = read_version.checked_add(1).context("Delta version overflow")?;
        let canonical: arrow::datatypes::Schema = snapshot.schema().as_ref().try_into_arrow()?;
        let mut transaction = snapshot
            .transaction(Box::new(DurableCommitter), &self.engine)?
            .with_operation("INSERT".into())
            .with_engine_info("arrow-deltalake")
            .with_data_change(true)
            .with_blind_append();
        for append in files {
            if append.table.root != self.root || canonical != *append.schema {
                bail!("table or schema changed while file was open; file remains uncommitted");
            }
            let url = Url::from_file_path(&append.file.path).map_err(|()| anyhow::anyhow!("invalid data file path"))?;
            let modified = i64::try_from(
                fs::metadata(&append.file.path)?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_millis(),
            )?;
            let metadata = DataFileMetadata::new(FileMeta::new(url, modified, append.file.bytes), append.stats.clone());
            transaction.add_files(build_add_file_metadata(metadata, &append.context)?);
        }
        if let Some(publication_fence) = &self.publication_fence {
            publication_fence
                .check_before_commit(&PublicationFenceContext {
                    table_path: &self.root,
                    read_version,
                    proposed_version,
                    file_count: files.len(),
                })
                .map_err(anyhow::Error::new)?;
        }
        let version = committed(transaction.commit(&self.engine)?)?;
        sync_directory(&self.root)?;
        Ok(version)
    }
}

/// Streaming Delta data file. `finish` closes, syncs, and commits it.
pub struct DeltaFileWriter {
    table: Arc<DeltaTable>,
    schema: SchemaRef,
    context: BoundWriteContext,
    parquet: ParquetFileWriter,
    stats: FileStatsAccumulator,
    failed: bool,
}

/// Completed append, including the table version.
#[derive(Debug)]
pub struct AppendResult {
    pub version: u64,
    pub file: CompletedFile,
}

/// A durable Parquet file that is not yet visible in a Delta snapshot.
pub struct StagedAppend {
    pub(crate) table: Arc<DeltaTable>,
    schema: SchemaRef,
    context: BoundWriteContext,
    pub(crate) file: CompletedFile,
    stats: StructArray,
}

impl StagedAppend {
    /// Return counters for the durable file awaiting Delta publication.
    #[must_use]
    pub const fn observation(&self) -> CompletedFileObservation {
        self.file.observation()
    }
}

impl DeltaFileWriter {
    /// Normalize and append a raw Arrow batch.
    ///
    /// # Errors
    /// Rejects incompatible batches and returns write/statistics errors.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.failed {
            bail!("Delta file previously failed; refusing further writes");
        }
        let result = self.write_batch(batch).await;
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Rows accepted by successful writes to the current file.
    #[must_use]
    pub const fn rows_written(&self) -> usize {
        self.parquet.rows_written()
    }

    /// Logical bytes from caller-provided Arrow batches accepted by this file.
    #[must_use]
    pub const fn input_bytes_written(&self) -> u64 {
        self.parquet.input_bytes_written()
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let input_bytes = record_batch_logical_bytes(batch)?;
        let normalized = normalize::align(&normalize::batch(batch)?, Arc::clone(&self.schema))?;
        let data = ArrowEngineData::new(normalized);
        let evaluator = self.table.engine.evaluation_handler().new_expression_evaluator(
            Arc::clone(self.context.logical_data_schema()),
            self.context.logical_to_physical(),
            Arc::clone(self.context.physical_data_schema()).into(),
        )?;
        let physical = evaluator.evaluate(&data)?.try_into_record_batch()?;
        self.stats.merge(&physical)?;
        self.parquet
            .write_accounted(&physical, batch.num_rows(), input_bytes)
            .await
    }

    /// Finalize and synchronize Parquet without publishing it.
    ///
    /// # Errors
    /// Returns close, synchronization, or statistics errors.
    pub async fn stage(self) -> Result<StagedAppend> {
        if self.failed {
            bail!("Delta file previously failed; refusing to commit its partial data");
        }
        let file = self.parquet.finish().await?;
        let stats = self.stats.finish()?.context("cannot commit an empty Delta file")?;
        Ok(StagedAppend {
            table: self.table,
            schema: self.schema,
            context: self.context,
            file,
            stats,
        })
    }

    /// Immediately publish this file, without an exporter batching delay.
    ///
    /// # Errors
    /// Returns finalization or commit errors, including uncertain I/O. Never retries.
    pub async fn finish(self) -> Result<AppendResult> {
        let append = self.stage().await?;
        // Kernel is synchronous. This offload is bounded by the caller's open files;
        // the collector uses the dedicated, bounded coordinator instead.
        tokio::task::spawn_blocking(move || {
            let version = append.table.commit_files(std::slice::from_ref(&append))?;
            Ok(AppendResult {
                version,
                file: append.file,
            })
        })
        .await?
    }
}

/// Convenience writer that flushes at schema boundaries.
pub struct DeltaTableWriter {
    table: Arc<DeltaTable>,
    current: Option<DeltaFileWriter>,
    input: Option<SchemaRef>,
    failed: bool,
}

impl DeltaTableWriter {
    /// Open a local table writer.
    ///
    /// # Errors
    /// Returns local ownership and filesystem errors.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            table: DeltaTable::open(path)?,
            current: None,
            input: None,
            failed: false,
        })
    }

    /// Open a local table writer with a check before every data commit.
    ///
    /// # Errors
    /// Returns local ownership and filesystem errors.
    pub fn open_with_fence(path: impl AsRef<Path>, publication_fence: Arc<dyn PublicationFence>) -> Result<Self> {
        Ok(Self {
            table: DeltaTable::open_with_fence(path, publication_fence)?,
            current: None,
            input: None,
            failed: false,
        })
    }

    /// Append a batch; schema changes flush the preceding file before evolution.
    ///
    /// # Errors
    /// Returns schema, write, and commit errors.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.failed {
            bail!("Delta writer previously failed; abandon it and inspect the table before reopening");
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self
            .input
            .as_ref()
            .is_some_and(|schema| schema.as_ref() != batch.schema_ref().as_ref())
        {
            self.flush().await?;
        }
        if self.current.is_none() {
            self.current = Some(self.table.new_file(batch.schema())?);
            self.input = Some(batch.schema());
        }
        let result = self
            .current
            .as_mut()
            .context("missing Delta writer")?
            .write(batch)
            .await;
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Commit buffered data; no-op when empty.
    ///
    /// # Errors
    /// Returns completion/commit errors.
    pub async fn flush(&mut self) -> Result<Option<AppendResult>> {
        if self.failed {
            bail!("Delta writer previously failed; refusing to commit its partial file");
        }
        match self.current.take() {
            Some(writer) => {
                let result = writer.finish().await;
                if result.is_err() {
                    self.failed = true;
                }
                result.map(Some)
            }
            None => Ok(None),
        }
    }

    /// Flush and release ownership.
    ///
    /// # Errors
    /// Returns completion/commit errors.
    pub async fn close(mut self) -> Result<Option<AppendResult>> {
        self.flush().await
    }
}
