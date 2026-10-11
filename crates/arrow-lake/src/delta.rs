//! Local, append-only Delta tables backed exclusively by Delta Kernel.

use crate::{CompletedFile, ParquetFileWriter, normalize, sync_directory};
use anyhow::{Context, Result, bail};
use arrow::array::{RecordBatch, StructArray};
use arrow::datatypes::SchemaRef;
use delta_kernel::committer::{CommitMetadata, CommitResponse, Committer, FileSystemCommitter, PublishMetadata};
use delta_kernel::engine::arrow_conversion::{TryFromArrow, TryIntoArrow};
use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
use delta_kernel::schema::StructType;
use delta_kernel::transaction::{BoundWriteContext, CommitResult};
use delta_kernel::{DeltaResult, DeltaResultIterator, Engine, FileMeta, FilteredEngineData, Snapshot, SnapshotRef};
use delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use delta_kernel_default_engine::parquet::DataFileMetadata;
use delta_kernel_default_engine::stats::FileStatsAccumulator;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder, build_add_file_metadata};
use fs2::FileExt;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;
use url::Url;

type LocalEngine = DefaultEngine<TokioBackgroundExecutor>;

/// Local table ownership and Kernel engine. Hold one instance per table.
pub struct DeltaTable {
    root: PathBuf,
    url: Url,
    engine: LocalEngine,
    _lock: File,
    commit_lock: Mutex<()>,
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
        Self::open_with_store(
            root,
            Arc::new(delta_kernel::object_store::local::LocalFileSystem::new()),
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
        fs::create_dir_all(root.as_ref())?;
        let root = fs::canonicalize(root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(".arrow-lake-writer.lock"))?;
        lock.try_lock_exclusive()
            .with_context(|| format!("Delta table already has a writer: {}", root.display()))?;
        let url = Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("invalid table path"))?;
        let engine = DefaultEngineBuilder::new(store).build();
        Ok(Arc::new(Self {
            root,
            url,
            engine,
            _lock: lock,
            commit_lock: Mutex::new(()),
        }))
    }

    fn snapshot(&self) -> Result<SnapshotRef> {
        Ok(Snapshot::builder_for(self.url.as_str()).build(&self.engine)?)
    }

    fn ensure_schema(&self, incoming: SchemaRef) -> Result<SchemaRef> {
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("Delta commit lock poisoned"))?;
        if !self.root.join("_delta_log").exists() {
            for entry in fs::read_dir(&self.root)? {
                let entry = entry?;
                if entry.file_name() != ".arrow-lake-writer.lock" {
                    bail!("refusing to adopt non-Delta directory {}", self.root.display());
                }
            }
            let schema = Arc::new(StructType::try_from_arrow(Arc::clone(&incoming))?);
            let transaction =
                delta_kernel::transaction::create_table::create_table(self.url.as_str(), schema, "arrow-lake")
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
        }
        Ok(Arc::new(self.snapshot()?.schema().as_ref().try_into_arrow()?))
    }

    /// Open a streaming file using a raw Arrow schema, evolving nullable top-level columns.
    ///
    /// # Errors
    /// Returns ownership, schema, Kernel, and file creation errors.
    pub fn new_file(self: &Arc<Self>, input: SchemaRef) -> Result<DeltaFileWriter> {
        let schema = self.ensure_schema(normalize::schema(&input)?)?;
        let snapshot = self.snapshot()?;
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
        let canonical: arrow::datatypes::Schema = snapshot.schema().as_ref().try_into_arrow()?;
        let mut transaction = snapshot
            .transaction(Box::new(DurableCommitter), &self.engine)?
            .with_operation("INSERT".into())
            .with_engine_info("arrow-lake")
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

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let normalized = normalize::align(&normalize::batch(batch)?, Arc::clone(&self.schema))?;
        let data = ArrowEngineData::new(normalized);
        let evaluator = self.table.engine.evaluation_handler().new_expression_evaluator(
            Arc::clone(self.context.logical_data_schema()),
            self.context.logical_to_physical(),
            Arc::clone(self.context.physical_data_schema()).into(),
        )?;
        let physical = evaluator.evaluate(&data)?.try_into_record_batch()?;
        self.stats.merge(&physical)?;
        self.parquet.write(&physical).await
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
