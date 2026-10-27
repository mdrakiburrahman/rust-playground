//! Local, append-only Delta tables backed exclusively by Delta Kernel.

use crate::delta_io::{BATCH_ROWS, READ_AHEAD, SharedExecutor, shared_executor};
use crate::publisher::{PublisherSession, SessionMode};
use crate::{
    CompletedFile, CompletedFileObservation, ParquetFileWriter, normalize, record_batch_logical_bytes, sync_ancestors,
    sync_directory, sync_directory_chain, sync_file,
};
use anyhow::{Context, Result, bail};
use arrow::array::{Array, RecordBatch, StructArray};
use arrow::datatypes::SchemaRef;
use delta_kernel::committer::{CommitMetadata, CommitResponse, Committer, FileSystemCommitter, PublishMetadata};
use delta_kernel::engine::arrow_conversion::{TryFromArrow, TryIntoArrow};
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::schema::StructType;
use delta_kernel::transaction::{BoundWriteContext, CommitResult};
use delta_kernel::{DeltaResult, DeltaResultIterator, Engine, FileMeta, FilteredEngineData, Snapshot, SnapshotRef};
use delta_kernel_default_engine::executor::TaskExecutor;
use delta_kernel_default_engine::parquet::DataFileMetadata;
use delta_kernel_default_engine::stats::FileStatsAccumulator;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder, build_add_file_metadata};
use fs2::FileExt;
use object_store::{ObjectStore, ObjectStoreExt, PutMode};
use parquet::file::properties::WriterProperties;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;
use std::time::{Duration, Instant};
use url::Url;

type LocalEngine = DefaultEngine<SharedExecutor>;

pub(crate) fn configured_publication_layout(schema: &SchemaRef) -> Result<Option<crate::layout::LayoutPolicy>> {
    let columns = |name| -> Result<Option<Vec<String>>> {
        match std::env::var(name) {
            Ok(value) => Ok(Some(value.split(',').map(str::trim).map(str::to_owned).collect())),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(error.into()),
        }
    };
    let sort_columns = columns("ARROWCOL_LAYOUT_SORT_COLUMNS")?;
    if schema.field_with_name("customer_id").is_err() && sort_columns.is_none() {
        return Ok(None);
    }
    let mut policy = crate::layout::LayoutPolicy::default();
    if let Some(columns) = sort_columns {
        policy.sort_columns = columns;
    }
    if let Some(columns) = columns("ARROWCOL_LAYOUT_BLOOM_COLUMNS")? {
        policy.bloom_columns = columns;
    }
    for (name, value) in [
        ("ARROWCOL_LAYOUT_MAX_ROWS", &mut policy.max_rows),
        ("ARROWCOL_LAYOUT_MAX_INPUT_BYTES", &mut policy.max_input_bytes),
        ("ARROWCOL_LAYOUT_ROW_GROUP_ROWS", &mut policy.row_group_rows),
        ("ARROWCOL_LAYOUT_PAGE_ROWS", &mut policy.page_rows),
    ] {
        match std::env::var(name) {
            Ok(setting) => *value = setting.parse().with_context(|| format!("invalid {name}"))?,
            Err(std::env::VarError::NotPresent) => {}
            Err(error) => return Err(error.into()),
        }
    }
    policy.validate(schema)?;
    Ok(Some(policy))
}

fn azure_store(url: &Url) -> Result<object_store::azure::MicrosoftAzure> {
    Ok(object_store::azure::MicrosoftAzureBuilder::from_env()
        .with_url(url.as_str())
        .with_credentials(Arc::new(crate::azure_cli::BoundedAzureCli::default()))
        .with_retry(object_store::RetryConfig {
            max_retries: 0,
            retry_timeout: Duration::from_secs(10),
            ..Default::default()
        })
        .with_client_options(object_store::ClientOptions::new().with_timeout(Duration::from_secs(10)))
        .build()?)
}

/// Shared refreshable storage authentication for readers and checkpoint clients.
/// This does not acquire table ownership or authorize writes past a sandbox reset fence.
///
/// # Errors
/// Rejects unsupported URLs and invalid credential configuration.
pub fn object_store_for_url(url: &Url) -> Result<Arc<dyn ObjectStore>> {
    let store: Arc<dyn ObjectStore> = match url.scheme() {
        "file" => Arc::new(object_store::local::LocalFileSystem::new()),
        "abfs" | "abfss" | "az" => Arc::new(azure_store(url)?),
        _ => bail!("only file and ADLS URLs are supported"),
    };
    Ok(store)
}

/// Captured sandbox generation for a source/checkpoint service.
/// Check immediately before and after each checkpoint mutation; a failure is
/// terminal for that service generation, not permission to retry against a reset.
pub struct StorageAuthority {
    session: Option<Arc<PublisherSession>>,
}

impl StorageAuthority {
    /// Captured cloud reset epoch, to bind durable routing before receipt admission.
    /// Local storage has no cloud reset epoch.
    #[must_use]
    pub fn generation(&self) -> Option<&str> {
        self.session.as_ref().and_then(|session| session.generation())
    }

    /// The process publisher session backing this authority (Azure only).
    #[must_use]
    pub fn session(&self) -> Option<&Arc<PublisherSession>> {
        self.session.as_ref()
    }

    /// Bind an explicitly opened publisher session.
    #[must_use]
    pub const fn from_session(session: Arc<PublisherSession>) -> Self {
        Self { session: Some(session) }
    }

    /// Capture the process-wide registered publisher session for the URL's account.
    /// Must not be called on the shared Delta I/O runtime.
    ///
    /// # Errors
    /// Rejects missing/mismatched container authority and authentication failures.
    pub fn open(url: &Url) -> Result<Self> {
        if url.scheme() == "file" {
            return Ok(Self { session: None });
        }
        Ok(Self {
            session: Some(PublisherSession::shared_for_url(url)?),
        })
    }

    /// Fail closed if reset state, generation or registration differs from the captured authority.
    ///
    /// # Errors
    /// Returns reset fencing, revocation, authentication or storage-read failures.
    pub fn check(&self) -> Result<()> {
        if let Some(session) = &self.session {
            session.check()?;
        }
        Ok(())
    }
}

/// Retained transaction fence preventing expired ingestion replay after PURGE.
pub const PURGE_WATERMARK: &str = "arrowcol.purge.before-ms";
/// UTC ingestion-time Hive partition, independent of the event timestamp.
pub const INGESTION_PARTITION: &str = "__arrowcol_ingestion";

/// Format an ingestion stamp in the explicitly configured UTC partition granularity.
///
/// # Errors
/// Rejects invalid timestamps and unknown granularity settings.
pub fn ingestion_partition_value(stamp: i64) -> Result<String> {
    let timestamp = chrono::DateTime::from_timestamp_millis(stamp).context("invalid ingestion stamp")?;
    let format = match std::env::var("ARROWCOL_PARTITION_GRANULARITY").as_deref() {
        Ok("minute") => "%Y-%m-%d-%H-%M",
        Ok("hour") => "%Y-%m-%d-%H",
        Ok("day") => "%Y-%m-%d",
        other => bail!("partition granularity must be minute, hour or day, got {other:?}"),
    };
    Ok(timestamp.format(format).to_string())
}

fn validate_transaction_id(app_id: &str, version: i64) -> Result<()> {
    if app_id.is_empty() || app_id.len() > 256 || app_id.starts_with("arrowcol.purge") || version < 0 {
        bail!("invalid or reserved publication transaction identity");
    }
    Ok(())
}

pub(crate) fn collision_backoff(attempt: u32) {
    let jitter = u64::from(uuid::Uuid::new_v4().as_bytes()[0]) % 17;
    std::thread::sleep(Duration::from_millis((5_u64 << attempt.min(6)) + jitter));
}

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

/// Verify storage authority and conditional write/delete access without creating a Delta commit.
///
/// # Errors
/// Returns invalid URI, reset fencing, authentication or storage errors.
pub fn verify_writable_uri(root: &str) -> Result<()> {
    let url = match Url::parse(root) {
        Ok(url) => url,
        Err(_) => {
            fs::create_dir_all(root)?;
            Url::from_directory_path(fs::canonicalize(root)?).map_err(|()| anyhow::anyhow!("invalid path"))?
        }
    };
    let table = DeltaTable::open_connection(url, Vec::new(), None, None)?;
    table.check_publication()?;
    let path = table.object_path(&format!(".arrowcol-probe-{}", uuid::Uuid::new_v4()))?;
    let store = Arc::clone(&table.store);
    shared_executor()?.block_on(async move {
        store
            .put_opts(&path, b"arrowcol".to_vec().into(), PutMode::Create.into())
            .await?;
        store.delete(&path).await
    })?;
    Ok(())
}

#[cfg(test)]
mod io_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashSet;

    /// Scenario: a new nested two-column Hive table publishes locally, with injected fsync faults.
    /// Guarantees: root ancestry, leaf/intermediate partitions and the checkpoint hint are fsynced
    /// before success; any fsync failure propagates and publishes nothing.
    #[tokio::test]
    async fn local_publication_syncs_partition_root_and_hint_ancestry() -> Result<()> {
        use crate::sync_hook;
        use arrow::array::{Int32Array, StringArray};
        let base = tempfile::tempdir()?;
        let base_path = fs::canonicalize(base.path())?;
        let parent = base_path.join("nested");
        let root = parent.join("table");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("p1", DataType::Utf8, false),
            Field::new("p2", DataType::Utf8, false),
        ]));
        let columns = || vec!["p1".to_owned(), "p2".to_owned()];
        let url = Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("URL"))?;
        sync_hook::fail_on(&parent);
        assert!(DeltaTable::open_url(url.clone(), Arc::clone(&schema), columns()).is_err());
        sync_hook::reset(&base_path);
        // The root now exists, but its ancestry was never proven durable; reopening must sync it.
        let table = DeltaTable::open_url(url, Arc::clone(&schema), columns())?;
        let synced = sync_hook::synced_under(&base_path);
        for directory in [&root, &parent, &base_path] {
            assert!(
                synced.contains(directory),
                "missing ancestor fsync {}",
                directory.display()
            );
        }
        let version = table.version()?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["b"])),
            ],
        )?;
        let values = BTreeMap::from([
            ("p1".to_owned(), Some("a".to_owned())),
            ("p2".to_owned(), Some("b".to_owned())),
        ]);
        let intermediate = root.join("p1=a");
        let leaf = intermediate.join("p2=b");
        sync_hook::fail_on(&intermediate);
        let mut file = table.new_file_partitioned(Arc::clone(&schema), values.clone())?;
        file.write(&batch).await?;
        assert!(
            file.stage().await.is_err(),
            "intermediate fsync failure must block staging"
        );
        assert_eq!(table.version()?, version);
        sync_hook::reset(&base_path);
        let mut file = table.new_file_partitioned(Arc::clone(&schema), values)?;
        file.write(&batch).await?;
        let staged = file.stage().await?;
        let synced = sync_hook::synced_under(&base_path);
        for directory in [&leaf, &intermediate, &root] {
            assert!(
                synced.contains(directory),
                "missing pre-commit fsync {}",
                directory.display()
            );
        }
        let committed = table.commit_files(&[staged])?;
        assert_eq!(committed, version + 1);
        let hint = root.join("_delta_log").join("_last_checkpoint");
        sync_hook::fail_on(&hint);
        assert!(table.checkpoint().is_err(), "hint fsync failure must propagate");
        sync_hook::reset(&base_path);
        assert_eq!(table.checkpoint()?, committed);
        assert!(sync_hook::synced_under(&base_path).contains(&hint));
        sync_hook::reset(&base_path);
        Ok(())
    }

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
        let inherited = table._lock.as_ref().context("missing writer lock")?.0.try_clone()?;
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
    /// Guarantees: a safe append collision refreshes and retries without overwriting either publication.
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
        let append = file.finish().await?;
        assert_eq!(append.version, 3);
        assert_eq!(table.version()?, 3);
        let mut paths = HashSet::new();
        for version in 1..=3 {
            let log = fs::read_to_string(directory.path().join(format!("_delta_log/{version:020}.json")))?;
            for line in log.lines() {
                let action: serde_json::Value = serde_json::from_str(line)?;
                if let Some(path) = action
                    .get("add")
                    .and_then(|add| add.get("path"))
                    .and_then(serde_json::Value::as_str)
                {
                    paths.insert(path.to_owned());
                }
            }
        }
        assert_eq!(paths.len(), 2);
        assert!(append.file.path.exists());
        Ok(())
    }

    /// Scenario: URL-significant characters occur in a physical Hive directory.
    /// Guarantees: local and ADLS request paths encode segments exactly once.
    #[test]
    fn staged_urls_round_trip_special_partition_literals() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let mut table = DeltaTable::open(directory.path())?;
        let table = Arc::get_mut(&mut table).context("unique table owner")?;
        for root in ["file:///tmp/table/", "abfss://data@example.dfs.core.windows.net/table/"] {
            table.url = Url::parse(root)?;
            let url = table.staged_url("p=space %?#λ/part.parquet")?;
            assert!(url.query().is_none() && url.fragment().is_none());
            let path = object_store::path::Path::from_url_path(url.path())?;
            assert!(path.as_ref().ends_with("p=space %?#λ/part.parquet"));
            let relative = url.as_str().strip_prefix(table.url.as_str()).context("relative path")?;
            assert_eq!(table.relative_url(relative)?, url);
        }
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
    pub(crate) root: PathBuf,
    pub(crate) url: Url,
    pub(crate) engine: LocalEngine,
    _lock: Option<WriterLock>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) store: Arc<dyn ObjectStore>,
    partition_columns: Vec<String>,
    session: Option<Arc<PublisherSession>>,
    bootstrap_only: bool,
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

pub(crate) struct DurableCommitter;

struct GuardedCommitter {
    session: Option<Arc<PublisherSession>>,
}

impl Committer for GuardedCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: DeltaResultIterator<'_, FilteredEngineData>,
        metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        let mut prepared = Vec::new();
        let mut bytes = 0_usize;
        for action in actions {
            let batch = action?.apply_selection_vector()?.try_into_record_batch()?;
            bytes = bytes
                .checked_add(batch.get_array_memory_size())
                .ok_or_else(|| delta_kernel::Error::generic("commit payload size overflow"))?;
            if bytes > 8 * 1024 * 1024 {
                return Err(delta_kernel::Error::generic(
                    "commit action payload exceeds 8 MiB admission bound",
                ));
            }
            prepared.push(Ok(FilteredEngineData::with_all_rows_selected(Box::new(
                ArrowEngineData::new(batch),
            ))));
        }
        if let Some(session) = &self.session {
            let session = Arc::clone(session);
            shared_executor()
                .map_err(|error| delta_kernel::Error::generic(error.to_string()))?
                .block_on(async move { session.check_async().await })
                .map_err(|error| delta_kernel::Error::generic(format!("{error:#}")))?;
        }
        DurableCommitter.commit(engine, Box::new(prepared.into_iter()), metadata)
    }

    fn is_catalog_committer(&self) -> bool {
        false
    }

    fn publish(&self, engine: &dyn Engine, metadata: PublishMetadata) -> DeltaResult<()> {
        DurableCommitter.publish(engine, metadata)
    }
}

impl Committer for DurableCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: DeltaResultIterator<'_, FilteredEngineData>,
        metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        let response = FileSystemCommitter::new().commit(engine, actions, metadata)?;
        if let CommitResponse::Committed { file_meta } = &response {
            if file_meta.location.scheme() != "file" {
                return Ok(response);
            }
            let path = file_meta
                .location
                .to_file_path()
                .map_err(|_| delta_kernel::Error::generic("non-local commit location"))?;
            let durable =
                |result: Result<()>| result.map_err(|error| delta_kernel::Error::generic(format!("{error:#}")));
            durable(sync_file(&path))?;
            let parent = path
                .parent()
                .ok_or_else(|| delta_kernel::Error::generic("missing log parent"))?;
            durable(sync_directory(parent))?;
            if let Some(root) = parent.parent() {
                durable(sync_directory(root))?;
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
    pub(crate) fn committer(&self) -> Box<dyn Committer> {
        Box::new(GuardedCommitter {
            session: self.session.clone(),
        })
    }
    /// Open a local or ADLS table without taking the collector's lifetime ownership lock.
    /// Atomic log creation coordinates independent processes; callers own their WAL fencing.
    ///
    /// # Errors
    /// Rejects unsupported URLs, credentials, incompatible partitions or table metadata.
    pub fn open_url(url: Url, schema: SchemaRef, partition_columns: Vec<String>) -> Result<Arc<Self>> {
        let table = Self::open_connection(url, partition_columns, None, None)?;
        table.ensure_schema(normalize::schema(&schema)?)?;
        Ok(table)
    }

    /// [`Self::open_url`] under an explicitly owned publisher session. Every data,
    /// log and checkpoint mutation goes through the session's guarded store, so the
    /// session can only close cleanly after they all have definite outcomes. The
    /// caller closes the session after dropping its tables; a bootstrap session
    /// opens schema only and never publishes data.
    /// Must not be called on the shared Delta I/O runtime.
    ///
    /// # Errors
    /// Rejects a URL outside the session's storage, plus [`Self::open_url`] errors.
    pub fn open_url_with_session(
        url: Url,
        schema: SchemaRef,
        partition_columns: Vec<String>,
        session: Arc<PublisherSession>,
    ) -> Result<Arc<Self>> {
        let table = Self::open_connection(url, partition_columns, None, Some(session))?;
        table.ensure_schema(normalize::schema(&schema)?)?;
        Ok(table)
    }

    /// Initialize an owned, fenced Azure sandbox without enabling normal data publication.
    /// Existing roots are validated rather than replaced.
    ///
    /// # Errors
    /// Requires matching sandbox container metadata in the fenced state.
    /// Azure admission registers a fresh bootstrap publisher only when the
    /// container is `fenced-v1` at the generation in
    /// [`crate::publisher::EXPECTED_GENERATION_ENV`], with an empty registry and
    /// released occupancy, before any table I/O. The member then drains and
    /// unregisters; it never sets `ready-v1`. An uncertain mutation leaves it
    /// registered so reset cannot proceed over it.
    pub fn initialize_url_for_bootstrap(url: Url, schema: SchemaRef, partition_columns: Vec<String>) -> Result<()> {
        if url.scheme() == "file" {
            let table = Self::open_connection(url, partition_columns, None, None)?;
            table.ensure_schema(normalize::schema(&schema)?)?;
            return Ok(());
        }
        let expected = crate::publisher::expected_generation_from_env()?;
        let session = PublisherSession::open_bootstrap(&url, &expected)?;
        let initialized = Self::open_connection(url, partition_columns, None, Some(Arc::clone(&session)))
            .and_then(|table| table.ensure_schema(normalize::schema(&schema)?));
        let closed = session.close_after_drain();
        match (initialized, closed) {
            (Ok(_), closed) => closed,
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(close)) => Err(error.context(format!("bootstrap registration retained: {close:#}"))),
        }
    }

    /// Open a collector-owned local path or an independently coordinated ADLS URI.
    ///
    /// # Errors
    /// Returns storage, authority or ownership errors.
    pub fn open_uri_with_fence(root: &str, publication_fence: Arc<dyn PublicationFence>) -> Result<Arc<Self>> {
        match Url::parse(root) {
            Ok(url) if url.scheme() != "file" => Self::open_connection(
                url,
                if std::env::var_os("ARROWCOL_PARTITION_GRANULARITY").is_some() {
                    vec![INGESTION_PARTITION.to_owned()]
                } else {
                    Vec::new()
                },
                Some(publication_fence),
                None,
            ),
            Ok(url) => Self::open_with_fence(
                url.to_file_path().map_err(|()| anyhow::anyhow!("invalid local URI"))?,
                publication_fence,
            ),
            Err(_) => Self::open_with_fence(root, publication_fence),
        }
    }

    fn open_connection(
        mut url: Url,
        partition_columns: Vec<String>,
        publication_fence: Option<Arc<dyn PublicationFence>>,
        session: Option<Arc<PublisherSession>>,
    ) -> Result<Arc<Self>> {
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        let session = match (url.scheme(), session) {
            (_, Some(session)) => Some(session),
            ("file", None) => None,
            ("abfs" | "abfss" | "az", None) => Some(PublisherSession::shared_for_url(&url)?),
            _ => bail!("only file and ADLS URLs are supported"),
        };
        let bootstrap_only = session
            .as_ref()
            .is_some_and(|session| session.mode() == SessionMode::Bootstrap);
        if let Some(session) = &session {
            session.check()?;
        }
        let store: Arc<dyn ObjectStore> = match &session {
            Some(session) => session.store_for_url(&url)?,
            None => Arc::new(object_store::local::LocalFileSystem::new()),
        };
        let root = if url.scheme() == "file" {
            let path = url
                .to_file_path()
                .map_err(|()| anyhow::anyhow!("invalid local table URL"))?;
            fs::create_dir_all(&path)?;
            let path = fs::canonicalize(path)?;
            sync_ancestors(&path)?;
            path
        } else {
            // Staging is intentionally retained after ambiguous failures, never
            // automatically removed on drop. VACUUM LITE cannot see these files.
            let path = std::env::temp_dir().join(format!("arrowcol-stage-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&path)?;
            path
        };
        let engine = DefaultEngineBuilder::new(Arc::clone(&store))
            .with_task_executor(shared_executor()?)
            .with_buffer_size(NonZeroUsize::new(READ_AHEAD).context("invalid read ahead")?)
            .with_batch_size(NonZeroUsize::new(BATCH_ROWS).context("invalid batch size")?)
            .build();
        let table = Arc::new(Self {
            root,
            url,
            engine,
            store,
            partition_columns,
            _lock: None,
            commit_lock: Mutex::new(()),
            snapshot_cache: Mutex::new(None),
            reuse_snapshots: false,
            publication_fence,
            session,
            bootstrap_only,
        });
        Ok(table)
    }

    pub(crate) fn check_publication(&self) -> Result<()> {
        if self.bootstrap_only {
            bail!("bootstrap initialization cannot publish data");
        }
        if let Some(session) = &self.session {
            session.check()?;
        }
        Ok(())
    }

    /// Resolve an application transaction from Kernel replay, including checkpoints.
    ///
    /// # Errors
    /// Returns history, retention or metadata errors.
    pub fn lookup_txn(&self, app_id: &str) -> Result<Option<i64>> {
        let snapshot = self.snapshot()?;
        Self::validate_retention(&snapshot)?;
        Ok(snapshot.get_app_id_version(app_id, &self.engine)?)
    }

    /// Write a classic V1 Kernel checkpoint, retaining application transactions.
    ///
    /// # Errors
    /// Returns authority, checkpoint replay, write or synchronization failures.
    pub fn checkpoint(&self) -> Result<u64> {
        self.check_publication()?;
        let snapshot = self.snapshot()?;
        Self::validate_retention(&snapshot)?;
        let writer = Arc::clone(&snapshot).create_checkpoint_writer(&self.engine)?;
        let data = writer.checkpoint_data(&self.engine)?;
        let state = data.state();
        let data = data
            .map(|batch| batch.and_then(|batch| batch.apply_selection_vector()))
            .collect::<DeltaResult<Vec<_>>>()?;
        let location = writer.checkpoint_path()?;
        self.check_publication()?;
        self.engine
            .parquet_handler()
            .write_parquet_file(location.clone(), Box::new(data.into_iter().map(Ok)))?;
        let path = object_store::path::Path::from_url_path(location.path())?;
        let store = Arc::clone(&self.store);
        let size = shared_executor()?
            .block_on(async move { store.head(&path).await })?
            .size;
        if let Ok(path) = location.to_file_path() {
            File::open(path)?.sync_all()?;
        }
        let state = Arc::into_inner(state).context("checkpoint state remains shared")?;
        let statistics = delta_kernel::checkpoint::LastCheckpointHintStats::from_reconciliation_state(state, size, 0)?;
        self.check_publication()?;
        writer.finalize(&self.engine, &statistics)?;
        if self.url.scheme() == "file" {
            // The hint is written by rename without a file fsync; make its contents durable.
            let log = self.root.join("_delta_log");
            let hint = log.join("_last_checkpoint");
            if hint.try_exists()? {
                sync_file(&hint)?;
            }
            sync_directory(&log)?;
        }
        Ok(snapshot.version())
    }

    /// Make a finished local file's directory entries durable before any Add can reference it.
    /// Table-local outputs sync every partition directory up to the table root; remote
    /// staging only needs its own directory.
    pub(crate) fn sync_local_output(&self, file: &Path) -> Result<()> {
        let parent = file.parent().context("local output has no directory")?;
        if self.url.scheme() == "file" {
            sync_directory_chain(parent, &self.root)
        } else {
            sync_directory(parent)
        }
    }

    fn validate_retention(snapshot: &SnapshotRef) -> Result<()> {
        if snapshot
            .metadata_configuration()
            .contains_key("delta.setTransactionRetentionDuration")
        {
            bail!("transaction retention must remain unset to preserve durable replay and fences");
        }
        Ok(())
    }

    pub(crate) fn object_path(&self, relative: &str) -> Result<object_store::path::Path> {
        Ok(object_store::path::Path::from_url_path(
            self.relative_url(relative)?.path(),
        )?)
    }

    pub(crate) fn relative_url(&self, relative: &str) -> Result<Url> {
        if relative.starts_with('/')
            || relative.contains(['?', '#', '\\'])
            || relative.split('/').any(|part| part == ".." || part == ".")
        {
            bail!("unsafe relative Delta path");
        }
        let url = self.url.join(relative)?;
        let path = object_store::path::Path::from_url_path(url.path())?;
        let root = object_store::path::Path::from_url_path(self.url.path())?;
        if url.scheme() != self.url.scheme() || url.host_str() != self.url.host_str() || !path.prefix_matches(&root) {
            bail!("Delta file path escapes its table");
        }
        Ok(url)
    }

    fn staged_url(&self, relative: &str) -> Result<Url> {
        let mut url = self.url.clone();
        url.path_segments_mut()
            .map_err(|()| anyhow::anyhow!("invalid table base URL"))?
            .pop_if_empty()
            .extend(relative.split('/'));
        Ok(url)
    }

    fn upload_file(&self, file: &CompletedFile) -> Result<()> {
        self.check_publication()?;
        if self.url.scheme() == "file" {
            return self.sync_local_output(&file.path);
        }
        if file.bytes > 512 * 1024 * 1024 {
            bail!("staged file exceeds bounded upload size");
        }
        let relative = file
            .path
            .strip_prefix(&self.root)?
            .to_str()
            .context("invalid staged path")?;
        let path = object_store::path::Path::from_url_path(self.staged_url(relative)?.path())?;
        let bytes = fs::read(&file.path)?;
        self.check_publication()?;
        let store = Arc::clone(&self.store);
        shared_executor()?
            .block_on(async move { store.put_opts(&path, bytes.into(), PutMode::Create.into()).await })?;
        Ok(())
    }
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
        sync_ancestors(&root)?;
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
        let engine = DefaultEngineBuilder::new(Arc::clone(&store))
            .with_task_executor(shared_executor()?)
            .with_buffer_size(NonZeroUsize::new(READ_AHEAD).context("invalid Delta read-ahead")?)
            .with_batch_size(NonZeroUsize::new(BATCH_ROWS).context("invalid Delta batch size")?)
            .build();
        Ok(Arc::new(Self {
            root,
            url,
            engine,
            _lock: Some(lock),
            store,
            partition_columns: if std::env::var_os("ARROWCOL_PARTITION_GRANULARITY").is_some() {
                vec![INGESTION_PARTITION.to_owned()]
            } else {
                Vec::new()
            },
            commit_lock: Mutex::new(()),
            snapshot_cache: Mutex::new(None),
            reuse_snapshots,
            publication_fence,
            session: None,
            bootstrap_only: false,
        }))
    }

    pub(crate) fn snapshot(&self) -> Result<SnapshotRef> {
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

    fn ensure_schema(&self, incoming: SchemaRef) -> Result<SnapshotRef> {
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("Delta commit lock poisoned"))?;
        let exists = if self.url.scheme() == "file" {
            self.root.join("_delta_log").exists()
        } else {
            let store = Arc::clone(&self.store);
            let prefix = self.object_path("_delta_log/")?;
            shared_executor()?.block_on(async move {
                use futures::StreamExt;
                let mut entries = store.list(Some(&prefix));
                entries.next().await.transpose().map(|entry| entry.is_some())
            })?
        };
        if !exists {
            for entry in fs::read_dir(&self.root)? {
                let entry = entry?;
                if entry.file_name() != WRITER_LOCK {
                    bail!("refusing to adopt non-Delta directory {}", self.root.display());
                }
            }
            let schema = Arc::new(StructType::try_from_arrow(Arc::clone(&incoming))?);
            let mut builder =
                delta_kernel::transaction::create_table::create_table(self.url.as_str(), schema, "arrow-deltalake");
            if !self.partition_columns.is_empty() {
                builder = builder.with_data_layout(delta_kernel::transaction::data_layout::DataLayout::partitioned(
                    self.partition_columns.clone(),
                ));
            }
            let transaction = builder.build(&self.engine, self.committer())?;
            match transaction.commit(&self.engine)? {
                CommitResult::Committed(_) | CommitResult::Conflicted(_) => {}
                CommitResult::Retryable(retry) => {
                    // A concurrently created table or unknown-success creation
                    // must be loaded and validated, not blindly recreated.
                    self.snapshot()
                        .with_context(|| format!("uncertain table creation: {}", retry.error))?;
                }
            }
        }
        let snapshot = self.snapshot()?;
        Self::validate_retention(&snapshot)?;
        if !self.partition_columns.is_empty()
            && snapshot.table_configuration().metadata().partition_columns() != self.partition_columns
        {
            bail!("incompatible table partition columns");
        }
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
            let transaction = builder.build(&self.engine, self.committer())?;
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
        self.new_file_partitioned_inner(input, BTreeMap::new(), None)
    }

    /// Create a replayable publication file with an immutable admission timestamp.
    ///
    /// # Errors
    /// Returns schema, identity, partition or file creation errors.
    pub fn new_file_with_publication(
        self: &Arc<Self>,
        input: SchemaRef,
        app_id: String,
        version: i64,
        stamp: i64,
    ) -> Result<DeltaFileWriter> {
        let layout = configured_publication_layout(&normalize::schema(&input)?)?;
        self.new_file_with_publication_and_layout(input, app_id, version, stamp, layout)
    }

    /// Create a replayable publication with explicit whole-file sorting controls.
    /// `None` retains streaming encoding; a policy bounds the entire publication.
    ///
    /// # Errors
    /// Rejects invalid identities, partitions, layout policies and file creation failures.
    pub fn new_file_with_publication_and_layout(
        self: &Arc<Self>,
        input: SchemaRef,
        app_id: String,
        version: i64,
        stamp: i64,
        layout: Option<crate::layout::LayoutPolicy>,
    ) -> Result<DeltaFileWriter> {
        let mut values = BTreeMap::new();
        let input = if self.partition_columns == [INGESTION_PARTITION] {
            values.insert(INGESTION_PARTITION.to_owned(), Some(ingestion_partition_value(stamp)?));
            let mut fields = input.fields().to_vec();
            if input.field_with_name(INGESTION_PARTITION).is_err() {
                fields.push(Arc::new(arrow::datatypes::Field::new(
                    INGESTION_PARTITION,
                    arrow::datatypes::DataType::Utf8,
                    false,
                )));
            }
            Arc::new(arrow::datatypes::Schema::new(fields))
        } else {
            input
        };
        let properties = layout
            .as_ref()
            .map(|policy| policy.writer_properties(&normalize::schema(&input)?))
            .transpose()?;
        let mut writer = self.new_file_partitioned_inner(input, values, properties)?;
        if let Some(policy) = layout {
            let physical = Arc::new(writer.context.physical_data_schema().as_ref().try_into_arrow()?);
            writer.sorter = Some(crate::layout::FileSortBuffer::new(physical, policy)?);
        }
        writer.with_transaction(app_id, version)?.with_ingestion_time(stamp)
    }

    /// Bind one file to explicit string-valued Hive partitions.
    ///
    /// # Errors
    /// Rejects incomplete partition values or incompatible schemas.
    pub fn new_file_partitioned(
        self: &Arc<Self>,
        input: SchemaRef,
        values: BTreeMap<String, Option<String>>,
    ) -> Result<DeltaFileWriter> {
        self.new_file_partitioned_inner(input, values, None)
    }

    /// Bind one file with caller-selected Parquet encoding controls.
    ///
    /// # Errors
    /// Rejects invalid partitions, schemas or encoding controls.
    pub fn new_file_partitioned_with_properties(
        self: &Arc<Self>,
        input: SchemaRef,
        values: BTreeMap<String, Option<String>>,
        properties: WriterProperties,
    ) -> Result<DeltaFileWriter> {
        self.new_file_partitioned_inner(input, values, Some(properties))
    }

    fn new_file_partitioned_inner(
        self: &Arc<Self>,
        input: SchemaRef,
        values: BTreeMap<String, Option<String>>,
        properties: Option<WriterProperties>,
    ) -> Result<DeltaFileWriter> {
        let snapshot = self.ensure_schema(normalize::schema(&input)?)?;
        let schema = Arc::new(snapshot.schema().as_ref().try_into_arrow()?);
        let transaction = snapshot.transaction(self.committer(), &self.engine)?;
        let state = transaction.write_state()?;
        let mut builder = state.write_context_builder();
        if !values.is_empty() {
            let mut typed = std::collections::HashMap::new();
            for (name, value) in &values {
                let scalar = value.clone().map_or_else(
                    || delta_kernel::expressions::Scalar::Null(delta_kernel::schema::DataType::STRING),
                    delta_kernel::expressions::Scalar::String,
                );
                typed.insert(name.clone(), scalar);
            }
            builder = builder.with_partition_values(typed);
        }
        let context = builder.build()?;
        let mut directory = self.root.clone();
        for (name, value) in &values {
            let value = value.as_deref().unwrap_or("__HIVE_DEFAULT_PARTITION__");
            if name.contains(['/', '\\', '=']) || value.contains(['/', '\\']) || value == "." || value == ".." {
                bail!("unsafe Hive partition component");
            }
            directory.push(format!("{name}={value}"));
        }
        fs::create_dir_all(&directory)?;
        let path = directory.join(format!("part-{}.parquet", uuid::Uuid::new_v4()));
        let physical = Arc::new(context.physical_data_schema().as_ref().try_into_arrow()?);
        let parquet = ParquetFileWriter::create_with_properties(path, physical, properties)?;
        let stats = FileStatsAccumulator::new(context.stats_columns(), context.physical_data_schema());
        Ok(DeltaFileWriter {
            table: Arc::clone(self),
            schema,
            context,
            parquet,
            stats,
            failed: false,
            publication: None,
            ingested_at_ms: None,
            partition_values: values,
            sorter: None,
            accepted_rows: 0,
            accepted_input_bytes: 0,
        })
    }

    /// Latest committed version, for diagnostics.
    ///
    /// # Errors
    /// Returns snapshot/log errors.
    pub fn version(&self) -> Result<u64> {
        Ok(self.snapshot()?.version())
    }

    /// Re-establish local durability of every replayed log/checkpoint file up to
    /// `version` before acknowledging an identity already visible in the log.
    pub(crate) fn sync_replayed_local_state(&self, version: u64) -> Result<()> {
        if self.url.scheme() != "file" {
            return Ok(());
        }
        let log = self.root.join("_delta_log");
        for entry in fs::read_dir(&log)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let is_replayed = name
                .get(..20)
                .and_then(|prefix| prefix.parse::<u64>().ok())
                .is_some_and(|entry_version| entry_version <= version)
                && (name.ends_with(".json") || name.ends_with(".parquet"));
            if name == "_last_checkpoint" || is_replayed {
                sync_file(&entry.path())?;
            }
        }
        sync_directory(&log)?;
        sync_directory(&self.root)
    }

    pub(crate) fn commit_files(&self, files: &[StagedAppend]) -> Result<u64> {
        if files.is_empty() {
            bail!("cannot publish an empty Delta data commit");
        }
        self.commit_files_inner(files, None)
    }

    /// Publish a batch under one stable application transaction, including empty progress batches.
    ///
    /// # Errors
    /// Rejects stale/fenced identities, changed schemas and exhausted collision budgets.
    pub fn commit_files_with_transaction(&self, files: &[StagedAppend], app_id: &str, version: i64) -> Result<u64> {
        validate_transaction_id(app_id, version)?;
        self.commit_files_inner(files, Some((app_id, version)))
    }

    fn commit_files_inner(&self, files: &[StagedAppend], identity: Option<(&str, i64)>) -> Result<u64> {
        let mut observation = crate::telemetry::PublicationObservation::start();
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("Delta commit lock poisoned"))?;
        let started = Instant::now();
        for attempt in 0..8 {
            observation.event("attempt");
            self.check_publication()?;
            let snapshot = self.snapshot()?;
            Self::validate_retention(&snapshot)?;
            if let Some((app_id, version)) = identity
                && let Some(previous) = snapshot.get_app_id_version(app_id, &self.engine)?
            {
                if previous > version {
                    bail!("publication transaction fenced by newer application version");
                }
                if previous == version {
                    self.sync_replayed_local_state(snapshot.version())?;
                    observation.finish("deduplicated");
                    return Ok(snapshot.version());
                }
            }
            let mut pending = Vec::new();
            let mut identities = std::collections::HashSet::new();
            let purge_before = snapshot.get_app_id_version(PURGE_WATERMARK, &self.engine)?;
            for append in files {
                if let Some((app_id, version)) = &append.publication {
                    if !identities.insert(app_id.as_str()) {
                        bail!("duplicate application identity in commit");
                    }
                    if let Some(previous) = snapshot.get_app_id_version(app_id, &self.engine)? {
                        if previous > *version {
                            bail!("publication transaction fenced by newer application version");
                        }
                        if previous == *version {
                            continue;
                        }
                    }
                }
                if let (Some(stamp), Some(cutoff)) = (append.ingested_at_ms, purge_before)
                    && stamp < cutoff
                {
                    bail!("publication ingestion stamp is beyond the retained recovery horizon (purge fence)");
                }
                pending.push(append);
            }
            if pending.is_empty() && identity.is_none() {
                self.sync_replayed_local_state(snapshot.version())?;
                observation.finish("deduplicated");
                return Ok(snapshot.version());
            }
            let read_version = snapshot.version();
            let proposed_version = read_version.checked_add(1).context("Delta version overflow")?;
            let canonical: arrow::datatypes::Schema = snapshot.schema().as_ref().try_into_arrow()?;
            let mut transaction = snapshot
                .transaction(self.committer(), &self.engine)?
                .with_operation("INSERT".into())
                .with_engine_info("arrow-deltalake")
                .with_data_change(true);
            if !pending.is_empty() {
                transaction = transaction.with_blind_append();
            }
            for append in &pending {
                if append.table.url != self.url || canonical != *append.schema {
                    bail!("table or schema changed while file was open; file remains uncommitted");
                }
                let relative = append
                    .file
                    .path
                    .strip_prefix(&self.root)?
                    .to_str()
                    .context("invalid data file path")?;
                let url = self.staged_url(relative)?;
                let modified = i64::try_from(
                    fs::metadata(&append.file.path)?
                        .modified()?
                        .duration_since(UNIX_EPOCH)?
                        .as_millis(),
                )?;
                let metadata =
                    DataFileMetadata::new(FileMeta::new(url, modified, append.file.bytes), append.stats.clone());
                transaction.add_files(build_add_file_metadata(metadata, &append.context)?);
                if let Some((app_id, version)) = &append.publication {
                    transaction = transaction.with_transaction_id(app_id.clone(), *version);
                }
            }
            if let Some((app_id, version)) = identity {
                transaction = transaction.with_transaction_id(app_id.to_owned(), version);
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
            let identifiable = identity.is_some() || pending.iter().all(|append| append.publication.is_some());
            let reconcile = || -> Result<Option<u64>> {
                let latest = self
                    .snapshot()
                    .context("unknown publication outcome; reconciliation failed")?;
                Self::validate_retention(&latest)?;
                let mut ids = pending
                    .iter()
                    .filter_map(|append| append.publication.as_ref().map(|(id, version)| (id.as_str(), *version)))
                    .collect::<Vec<_>>();
                if let Some(id) = identity {
                    ids.push(id);
                }
                for (id, version) in ids {
                    match latest.get_app_id_version(id, &self.engine)? {
                        Some(previous) if previous > version => {
                            bail!("publication transaction fenced by newer application version")
                        }
                        Some(previous) if previous == version => {}
                        _ => return Ok(None),
                    }
                }
                // Visibility is not proof of local durability after an I/O error.
                self.sync_replayed_local_state(latest.version())?;
                Ok(Some(latest.version()))
            };
            match transaction.commit(&self.engine) {
                Ok(CommitResult::Committed(commit)) => {
                    sync_directory(&self.root)?;
                    observation.finish("committed");
                    return Ok(commit.commit_version());
                }
                Ok(CommitResult::Conflicted(_)) => observation.event("conflict"),
                Ok(CommitResult::Retryable(retry)) => {
                    observation.event("retryable_io");
                    // Refresh/check every stable identity on the next iteration BEFORE
                    // resubmission. If refresh fails, the uncertain outcome stays an error.
                    if !identifiable {
                        bail!("uncertain publication without retry identity: {}", retry.error);
                    }
                    if let Some(version) = reconcile()? {
                        observation.finish("reconciled");
                        return Ok(version);
                    }
                }
                Err(error) => {
                    // Kernel can surface post-publication I/O as Err, not only Retryable.
                    // A proven transaction is success; an unreadable outcome stays an error.
                    if identifiable && let Some(version) = reconcile()? {
                        observation.finish("reconciled");
                        return Ok(version);
                    }
                    return Err(error.into());
                }
            }
            if started.elapsed() > Duration::from_secs(10) {
                break;
            }
            collision_backoff(attempt);
        }
        observation.finish("exhausted");
        bail!("Delta publication collision/retry budget exhausted (8 attempts / 10 seconds)")
    }
}

/// Streaming Delta data file. `finish` closes, syncs, and commits it.
pub struct DeltaFileWriter {
    table: Arc<DeltaTable>,
    pub(crate) schema: SchemaRef,
    pub(crate) context: BoundWriteContext,
    parquet: ParquetFileWriter,
    stats: FileStatsAccumulator,
    failed: bool,
    publication: Option<(String, i64)>,
    ingested_at_ms: Option<i64>,
    partition_values: BTreeMap<String, Option<String>>,
    sorter: Option<crate::layout::FileSortBuffer>,
    accepted_rows: usize,
    accepted_input_bytes: u64,
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
    pub(crate) stats: StructArray,
    pub(crate) publication: Option<(String, i64)>,
    pub(crate) ingested_at_ms: Option<i64>,
}

impl StagedAppend {
    /// Return counters for the durable file awaiting Delta publication.
    #[must_use]
    pub const fn observation(&self) -> CompletedFileObservation {
        self.file.observation()
    }
}

impl DeltaFileWriter {
    /// Assign a stable identity; retries of this publication must reuse it.
    ///
    /// # Errors
    /// Rejects reserved or invalid application identities and versions.
    pub fn with_transaction(mut self, app_id: String, version: i64) -> Result<Self> {
        validate_transaction_id(&app_id, version)?;
        self.publication = Some((app_id, version));
        Ok(self)
    }

    /// Preserve an admission-time stamp through durable replay.
    ///
    /// # Errors
    /// Rejects timestamps before the Unix epoch.
    pub fn with_ingestion_time(mut self, ingested_at_ms: i64) -> Result<Self> {
        if ingested_at_ms < 0 {
            bail!("ingestion stamp is before Unix epoch");
        }
        self.ingested_at_ms = Some(ingested_at_ms);
        Ok(self)
    }
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
        self.accepted_rows
    }

    /// Logical bytes from caller-provided Arrow batches accepted by this file.
    #[must_use]
    pub const fn input_bytes_written(&self) -> u64 {
        self.accepted_input_bytes
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let input_bytes = record_batch_logical_bytes(batch)?;
        let mut batch = normalize::batch(batch)?;
        for (name, expected) in &self.partition_values {
            if let Ok(index) = batch.schema().index_of(name) {
                let column = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .context("partition columns must contain string literals")?;
                for row in 0..batch.num_rows() {
                    let actual = if column.is_null(row) {
                        None
                    } else {
                        Some(column.value(row))
                    };
                    if actual != expected.as_deref() {
                        bail!("batch partition value differs from bound write context");
                    }
                }
            } else {
                let mut fields = batch.schema().fields().to_vec();
                fields.push(Arc::new(arrow::datatypes::Field::new(
                    name,
                    arrow::datatypes::DataType::Utf8,
                    expected.is_none(),
                )));
                let mut columns = batch.columns().to_vec();
                columns.push(Arc::new(arrow::array::StringArray::from(vec![
                    expected.as_deref();
                    batch.num_rows()
                ])));
                batch = RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)?;
            }
        }
        if self.schema.field_with_name(INGESTION_PARTITION).is_ok()
            && batch.schema().field_with_name(INGESTION_PARTITION).is_err()
        {
            let stamp = self
                .ingested_at_ms
                .context("partitioned publication requires stable ingestion stamp")?;
            let value = ingestion_partition_value(stamp)?;
            let mut fields = batch.schema().fields().to_vec();
            fields.push(Arc::new(arrow::datatypes::Field::new(
                INGESTION_PARTITION,
                arrow::datatypes::DataType::Utf8,
                false,
            )));
            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(arrow::array::StringArray::from(vec![
                value.as_str();
                batch.num_rows()
            ])));
            batch = RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)?;
        }
        let normalized = normalize::align(&batch, Arc::clone(&self.schema))?;
        let data_schema: arrow::datatypes::Schema = self.context.logical_data_schema().as_ref().try_into_arrow()?;
        let projection = data_schema
            .fields()
            .iter()
            .map(|field| normalized.schema().index_of(field.name()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let normalized = normalized.project(&projection)?;
        let data = ArrowEngineData::new(normalized);
        let evaluator = self.table.engine.evaluation_handler().new_expression_evaluator(
            Arc::clone(self.context.logical_data_schema()),
            self.context.logical_to_physical(),
            Arc::clone(self.context.physical_data_schema()).into(),
        )?;
        let physical = evaluator.evaluate(&data)?.try_into_record_batch()?;
        let rows = self
            .accepted_rows
            .checked_add(batch.num_rows())
            .context("file row count overflow")?;
        let bytes = self
            .accepted_input_bytes
            .checked_add(input_bytes)
            .context("file input byte count overflow")?;
        self.stats.merge(&physical)?;
        if let Some(sorter) = &mut self.sorter {
            if !sorter.try_push(&physical)? {
                bail!(
                    "publication exceeds sorted-file budget; split upstream into independently identified publications"
                );
            }
        } else {
            self.parquet
                .write_accounted(&physical, batch.num_rows(), input_bytes)
                .await?;
        }
        self.accepted_rows = rows;
        self.accepted_input_bytes = bytes;
        Ok(())
    }

    /// Finalize and synchronize Parquet without publishing it.
    ///
    /// # Errors
    /// Returns close, synchronization, or statistics errors.
    pub async fn stage(mut self) -> Result<StagedAppend> {
        if self.failed {
            bail!("Delta file previously failed; refusing to commit its partial data");
        }
        if let Some(sorter) = self.sorter.take() {
            let sorted = sorter.finish()?;
            self.parquet
                .write_accounted(&sorted, self.accepted_rows, self.accepted_input_bytes)
                .await?;
        }
        let file = self.parquet.finish().await?;
        let stats = self.stats.finish()?.context("cannot commit an empty Delta file")?;
        self.table.upload_file(&file)?;
        Ok(StagedAppend {
            table: self.table,
            schema: self.schema,
            context: self.context,
            file,
            stats,
            publication: self.publication,
            ingested_at_ms: self.ingested_at_ms,
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
