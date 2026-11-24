//! Streaming writers for ordinary Arrow batches, independent of telemetry.
//!
//! Parquet mode retains the supplied schema. Delta mode normalizes unsupported
//! logical types losslessly and uses Delta Kernel to create/evolve/commit tables.

use anyhow::{Context, Result};
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

#[cfg(feature = "delta")]
mod azure_cli;
#[cfg(feature = "delta")]
pub use azure_cli::AzureCliTokenProvider;
#[cfg(feature = "delta")]
mod azure_fence;
#[cfg(feature = "delta")]
pub mod catalog;
#[cfg(feature = "delta")]
pub mod commit;
#[cfg(feature = "delta")]
pub mod delta;
#[cfg(feature = "delta")]
mod delta_io;
pub mod delta_stats;
#[cfg(feature = "delta")]
pub use delta_io::{DeltaRuntimeConfig, runtime_config};
pub mod footer;
pub mod layout;
#[cfg(feature = "delta")]
pub mod maintenance;
#[cfg(feature = "delta")]
pub mod normalize;
#[cfg(feature = "delta")]
mod parallel_writer;
#[cfg(feature = "delta")]
mod pipelined_writer;
#[cfg(feature = "delta")]
mod streaming_columns;
#[cfg(feature = "delta")]
mod partition;
#[cfg(feature = "delta")]
mod publication_evidence;
#[cfg(feature = "delta")]
mod publication_lookup;
#[cfg(feature = "delta")]
pub use publication_evidence::{PublicationEvidence, PublicationOutcome, PublicationReason};
#[cfg(feature = "delta")]
pub mod publisher;
#[cfg(feature = "delta")]
pub mod qap_http;
pub mod sizing;
#[cfg(feature = "delta")]
pub mod telemetry;
pub mod writer;

/// Strategy for resolving transaction identities before Delta publication.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TxnLookupMode {
    PerId,
    #[default]
    Batched,
    BatchedCached,
}

impl<'de> serde::Deserialize<'de> for TxnLookupMode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        const EXPECTED: &str = "collector.commit.txn_lookup_mode must be per_id, batched, or batched_cached";
        match String::deserialize(deserializer).as_deref() {
            Ok("per_id") => Ok(Self::PerId),
            Ok("batched") => Ok(Self::Batched),
            Ok("batched_cached") => Ok(Self::BatchedCached),
            _ => Err(serde::de::Error::custom(EXPECTED)),
        }
    }
}

/// Remote Parquet staging strategy; local Delta tables always fsync their data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", try_from = "String")]
pub enum StagingMode {
    #[default]
    LocalFsync,
    Streaming,
}

impl StagingMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalFsync => "local_fsync",
            Self::Streaming => "streaming",
        }
    }
}

impl TryFrom<String> for StagingMode {
    type Error = &'static str;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "local_fsync" => Ok(Self::LocalFsync),
            "streaming" => Ok(Self::Streaming),
            _ => Err("staging_mode must be local_fsync or streaming"),
        }
    }
}

/// Scheduling of Parquet column encoders; one column worker stays sequential.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", try_from = "String")]
pub enum ParquetEncodeMode {
    #[default]
    ColumnWaves,
    ColumnPipeline,
}

impl ParquetEncodeMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ColumnWaves => "column_waves",
            Self::ColumnPipeline => "column_pipeline",
        }
    }
}

impl TryFrom<String> for ParquetEncodeMode {
    type Error = &'static str;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        match raw.as_str() {
            "column_waves" => Ok(Self::ColumnWaves),
            "column_pipeline" => Ok(Self::ColumnPipeline),
            _ => Err("expected column_waves or column_pipeline"),
        }
    }
}

/// Queue discipline for the dedicated CPU workers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", try_from = "String")]
pub enum CpuDispatch {
    #[default]
    SharedFifo,
    BoundedShards,
}

impl CpuDispatch {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SharedFifo => "shared_fifo",
            Self::BoundedShards => "bounded_shards",
        }
    }
}

impl TryFrom<String> for CpuDispatch {
    type Error = &'static str;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        match raw.as_str() {
            "shared_fifo" => Ok(Self::SharedFifo),
            "bounded_shards" => Ok(Self::BoundedShards),
            _ => Err("expected shared_fifo or bounded_shards"),
        }
    }
}

/// Affinity policy for CPU workers, independent of the I/O runtime.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", try_from = "String")]
pub enum CpuPlacement {
    #[default]
    Os,
    CompactAllowed,
    ScatterAllowed,
    PhysicalFirst,
}

impl CpuPlacement {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Os => "os",
            Self::CompactAllowed => "compact_allowed",
            Self::ScatterAllowed => "scatter_allowed",
            Self::PhysicalFirst => "physical_first",
        }
    }
}

impl TryFrom<String> for CpuPlacement {
    type Error = &'static str;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        match raw.as_str() {
            "os" => Ok(Self::Os),
            "compact_allowed" => Ok(Self::CompactAllowed),
            "scatter_allowed" => Ok(Self::ScatterAllowed),
            "physical_first" => Ok(Self::PhysicalFirst),
            _ => Err("expected os, compact_allowed, scatter_allowed or physical_first"),
        }
    }
}

/// How the writer groups rows by their Delta partition values.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", try_from = "String")]
pub enum PartitionGrouping {
    #[default]
    ScalarMap,
    TypedRuns,
}

impl TryFrom<String> for PartitionGrouping {
    type Error = &'static str;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        match raw.as_str() {
            "scalar_map" => Ok(Self::ScalarMap),
            "typed_runs" => Ok(Self::TypedRuns),
            _ => Err("expected scalar_map or typed_runs"),
        }
    }
}

impl PartitionGrouping {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScalarMap => "scalar_map",
            Self::TypedRuns => "typed_runs",
        }
    }
}

/// Writer-side validation of a bound ingestion partition (independent of grouping).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(try_from = "String", rename_all = "snake_case")]
pub enum GeneratedPartitionBinding {
    #[default]
    RevalidateRows,
    ProvenConstant,
}

impl GeneratedPartitionBinding {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RevalidateRows => "revalidate_rows",
            Self::ProvenConstant => "proven_constant",
        }
    }
}

impl TryFrom<String> for GeneratedPartitionBinding {
    type Error = anyhow::Error;

    fn try_from(raw: String) -> Result<Self> {
        match raw.as_str() {
            "revalidate_rows" => Ok(Self::RevalidateRows),
            "proven_constant" => Ok(Self::ProvenConstant),
            _ => {
                anyhow::bail!("generated_partition_binding must be 'revalidate_rows' or 'proven_constant', got {raw:?}")
            }
        }
    }
}

#[cfg(feature = "delta")]
use parallel_writer::ParallelArrowWriter;
#[cfg(feature = "delta")]
use pipelined_writer::PipelinedArrowWriter;
#[cfg(feature = "delta")]
use streaming_columns::StreamingColumnWriter;

/// Stable size and row counters for one completed Parquet file.
///
/// `input_bytes` is the logical size of the Arrow slices accepted by the
/// writer. Shared buffers are counted once per logical column reference, while
/// unused buffer capacity and Rust object overhead are excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompletedFileObservation {
    /// Rows accepted into the completed file.
    pub rows: usize,
    /// Logical bytes in caller-provided Arrow slices.
    pub input_bytes: u64,
    /// Bytes in the synchronized Parquet file.
    pub parquet_bytes: u64,
}

/// A completed, durable Parquet file. It is not necessarily committed to Delta.
#[derive(Debug)]
pub struct CompletedFile {
    /// Table-root-relative identity anchored at the staging root. Streamed remote objects have no local file here.
    pub path: PathBuf,
    pub rows: usize,
    /// Logical Arrow bytes accepted before Parquet encoding.
    pub input_bytes: u64,
    /// Durable Parquet bytes. Kept as `bytes` for API compatibility.
    pub bytes: u64,
}

impl CompletedFile {
    /// Return path-independent counters suitable for metrics and adaptive sizing.
    #[must_use]
    pub const fn observation(&self) -> CompletedFileObservation {
        CompletedFileObservation {
            rows: self.rows,
            input_bytes: self.input_bytes,
            parquet_bytes: self.bytes,
        }
    }
}

/// Streaming Parquet file writer. Dropping it does not finish the file.
pub struct ParquetFileWriter {
    #[cfg(feature = "delta")]
    properties: WriterProperties,
    writer: Option<FileWriter>,
    durable_handle: Option<File>,
    path: PathBuf,
    rows: usize,
    input_bytes: u64,
}

enum FileWriter {
    Sequential(AsyncArrowWriter<tokio::fs::File>),
    #[cfg(feature = "delta")]
    Parallel(ParallelArrowWriter<File>),
    #[cfg(feature = "delta")]
    Pipelined(PipelinedArrowWriter<File>),
    #[cfg(feature = "delta")]
    Streaming(AsyncArrowWriter<delta::streaming_sink::StreamingSink>),
    #[cfg(feature = "delta")]
    StreamingColumns(StreamingColumnWriter),
}

impl ParquetFileWriter {
    /// Create a unique file; never overwrite existing output.
    ///
    /// # Errors
    /// Returns file creation or Parquet schema errors.
    pub fn create(path: impl AsRef<Path>, schema: SchemaRef) -> Result<Self> {
        Self::create_with_properties(path, schema, None)
    }

    /// Create a unique file with explicit row-group, page and encoding controls.
    ///
    /// # Errors
    /// Returns file creation or Parquet schema errors.
    pub fn create_with_properties(
        path: impl AsRef<Path>,
        schema: SchemaRef,
        properties: Option<WriterProperties>,
    ) -> Result<Self> {
        Self::create_inner(path.as_ref(), schema, properties, false)
    }

    /// Opt into a schema-neutral footer declaration for level-bearing codecs (Delta ingestion and its evidence fixture).
    ///
    /// # Errors
    /// Returns file creation, writer policy or Parquet schema errors.
    pub fn create_with_codec_evidence(
        path: impl AsRef<Path>,
        schema: SchemaRef,
        properties: Option<WriterProperties>,
    ) -> Result<Self> {
        Self::create_inner(path.as_ref(), schema, properties, true)
    }

    fn create_inner(
        path: &Path,
        schema: SchemaRef,
        properties: Option<WriterProperties>,
        declare_levels: bool,
    ) -> Result<Self> {
        let properties = Self::effective_properties(&schema, properties, declare_levels)?;
        Self::open_local(path, schema, properties)
    }

    fn effective_properties(
        schema: &SchemaRef,
        properties: Option<WriterProperties>,
        declare_levels: bool,
    ) -> Result<WriterProperties> {
        let properties = writer::process_config()?.writer_properties(
            schema,
            properties.clone(),
            &writer::sorting_from_properties(schema, properties.as_ref())?,
        )?;
        if declare_levels {
            let physical = parquet::arrow::ArrowSchemaConverter::new()
                .with_coerce_types(properties.coerce_types())
                .convert(schema)?;
            footer::stamp(&physical, properties)
        } else {
            Ok(properties)
        }
    }

    fn open_local(path: &Path, schema: SchemaRef, properties: WriterProperties) -> Result<Self> {
        let path = path.to_path_buf();
        let durable_handle = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| format!("create Parquet {}", path.display()))?;
        #[cfg(feature = "delta")]
        let effective = properties.clone();
        #[cfg(feature = "delta")]
        let writer = {
            let config = delta_io::runtime_config()?;
            let column_workers = config.parquet_column_workers;
            if config.parquet_encode_mode == ParquetEncodeMode::ColumnPipeline {
                FileWriter::Pipelined(PipelinedArrowWriter::try_new(
                    durable_handle.try_clone()?,
                    schema,
                    properties,
                    column_workers,
                )?)
            } else if column_workers > 1 {
                FileWriter::Parallel(ParallelArrowWriter::try_new(
                    durable_handle.try_clone()?,
                    schema,
                    properties,
                    column_workers,
                )?)
            } else {
                let output = tokio::fs::File::from_std(durable_handle.try_clone()?);
                FileWriter::Sequential(AsyncArrowWriter::try_new(output, schema, Some(properties))?)
            }
        };
        #[cfg(not(feature = "delta"))]
        let writer = {
            let output = tokio::fs::File::from_std(durable_handle.try_clone()?);
            FileWriter::Sequential(AsyncArrowWriter::try_new(output, schema, Some(properties))?)
        };
        Ok(Self {
            writer: Some(writer),
            #[cfg(feature = "delta")]
            properties: effective,
            durable_handle: Some(durable_handle),
            path,
            rows: 0,
            input_bytes: 0,
        })
    }

    #[cfg(feature = "delta")]
    fn streaming(
        path: PathBuf,
        schema: SchemaRef,
        properties: Option<WriterProperties>,
        sink: delta::streaming_sink::StreamingSink,
    ) -> Result<Self> {
        let properties = Self::effective_properties(&schema, properties, true)?;
        let config = delta_io::runtime_config()?;
        let writer = if config.parquet_encode_mode == ParquetEncodeMode::ColumnPipeline {
            FileWriter::StreamingColumns(StreamingColumnWriter::try_new(
                sink,
                schema,
                properties.clone(),
                config.parquet_column_workers,
            )?)
        } else {
            FileWriter::Streaming(AsyncArrowWriter::try_new(sink, schema, Some(properties.clone()))?)
        };
        Ok(Self {
            properties,
            writer: Some(writer),
            durable_handle: None,
            path,
            rows: 0,
            input_bytes: 0,
        })
    }

    /// Append a batch. Column-pipeline mode retains a bounded window of Arrow slices.
    ///
    /// # Errors
    /// Returns byte-accounting, schema, or file-write errors.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let input_bytes = record_batch_logical_bytes(batch)?;
        self.write_accounted(batch, batch.num_rows(), input_bytes).await
    }

    /// Rows accepted by successful writes to the current file.
    #[must_use]
    pub const fn rows_written(&self) -> usize {
        self.rows
    }

    /// Logical Arrow bytes accepted by successful writes to the current file.
    #[must_use]
    pub const fn input_bytes_written(&self) -> u64 {
        self.input_bytes
    }

    pub(crate) async fn write_accounted(&mut self, batch: &RecordBatch, rows: usize, input_bytes: u64) -> Result<()> {
        let rows = self.rows.checked_add(rows).context("row count overflow")?;
        let input_bytes = self
            .input_bytes
            .checked_add(input_bytes)
            .context("input Arrow byte count overflow")?;
        #[cfg(feature = "delta")]
        {
            let writer = self
                .writer
                .take()
                .context("Parquet writer unavailable after interrupted write")?;
            match writer {
                FileWriter::Sequential(mut writer) => {
                    let batch = batch.clone();
                    let (writer, result) = delta_io::run_cpu_future(async move {
                        let result = writer.write(&batch).await;
                        (writer, result)
                    })
                    .await?;
                    self.writer = Some(FileWriter::Sequential(writer));
                    result?;
                }
                FileWriter::Streaming(mut writer) => {
                    let batch = batch.clone();
                    let (writer, result) = delta_io::run_cpu_future(async move {
                        let result = writer.write(&batch).await;
                        (writer, result)
                    })
                    .await?;
                    self.writer = Some(FileWriter::Streaming(writer));
                    result?;
                }
                FileWriter::Parallel(mut writer) => {
                    let result = writer.write(batch).await;
                    self.writer = Some(FileWriter::Parallel(writer));
                    result?;
                }
                FileWriter::Pipelined(mut writer) => {
                    let result = writer.write(batch).await;
                    self.writer = Some(FileWriter::Pipelined(writer));
                    result?;
                }
                FileWriter::StreamingColumns(mut writer) => {
                    let result = writer.write(batch).await;
                    self.writer = Some(FileWriter::StreamingColumns(writer));
                    result?;
                }
            }
        }
        #[cfg(not(feature = "delta"))]
        match self.writer.take().context("Parquet writer unavailable")? {
            FileWriter::Sequential(mut writer) => {
                writer.write(batch).await?;
                self.writer = Some(FileWriter::Sequential(writer));
            }
        }
        self.rows = rows;
        self.input_bytes = input_bytes;
        Ok(())
    }

    /// Write the footer and synchronize data and its directory before success.
    ///
    /// # Errors
    /// Returns close, metadata, or synchronization errors.
    pub async fn finish(self) -> Result<CompletedFile> {
        Ok(self.finish_with_upload().await?.0)
    }

    async fn finish_with_upload(self) -> Result<(CompletedFile, Option<std::time::Duration>)> {
        let Self {
            writer,
            durable_handle,
            path,
            rows,
            input_bytes,
            ..
        } = self;
        match writer.context("Parquet writer unavailable after interrupted write")? {
            FileWriter::Sequential(writer) => {
                #[cfg(feature = "delta")]
                delta_io::run_cpu_future(async move { writer.close().await }).await??;
                #[cfg(not(feature = "delta"))]
                writer.close().await?;
            }
            #[cfg(feature = "delta")]
            FileWriter::Streaming(mut writer) => {
                let (bytes, duration) = delta_io::run_cpu_future(async move {
                    writer.finish().await?;
                    Ok::<_, anyhow::Error>(writer.into_inner().observation())
                })
                .await??;
                return Ok((
                    CompletedFile {
                        path,
                        rows,
                        input_bytes,
                        bytes,
                    },
                    Some(duration),
                ));
            }
            #[cfg(feature = "delta")]
            FileWriter::Parallel(writer) => {
                drop(writer.finish().await?);
            }
            #[cfg(feature = "delta")]
            FileWriter::Pipelined(writer) => {
                drop(writer.finish().await?);
            }
            #[cfg(feature = "delta")]
            FileWriter::StreamingColumns(writer) => {
                let (bytes, duration) = writer.finish().await?;
                return Ok((CompletedFile { path, rows, input_bytes, bytes }, Some(duration)));
            }
        }
        let durable_handle =
            tokio::fs::File::from_std(durable_handle.context("local writer missing durability handle")?);
        durable_handle.sync_all().await?;
        let bytes = durable_handle.metadata().await?.len();
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        tokio::fs::File::open(parent).await?.sync_all().await?;
        Ok((
            CompletedFile {
                path,
                rows,
                input_bytes,
                bytes,
            },
            None,
        ))
    }
}

/// Return the logical Arrow bytes represented by a record batch.
///
/// The measurement follows each logical column and nested child slice. Shared
/// buffers therefore contribute once per logical reference, while unused
/// allocation capacity and Rust object overhead do not contribute.
///
/// # Errors
/// Returns Arrow layout or checked integer-conversion/addition errors.
pub fn record_batch_logical_bytes(batch: &RecordBatch) -> Result<u64> {
    batch.columns().iter().try_fold(0_u64, |total, column| {
        let bytes = column
            .to_data()
            .get_slice_memory_size()
            .context("measure logical Arrow input bytes")?;
        let bytes = u64::try_from(bytes).context("logical Arrow input byte count exceeds u64")?;
        total
            .checked_add(bytes)
            .context("logical Arrow input byte count overflow")
    })
}

#[cfg(feature = "delta")]
pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(test)]
    sync_hook::observe(path)?;
    File::open(path)
        .with_context(|| format!("open directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("fsync directory {}", path.display()))?;
    Ok(())
}

#[cfg(feature = "delta")]
pub(crate) fn sync_file(path: &Path) -> Result<()> {
    #[cfg(test)]
    sync_hook::observe(path)?;
    File::open(path)
        .with_context(|| format!("open file {}", path.display()))?
        .sync_all()
        .with_context(|| format!("fsync file {}", path.display()))?;
    Ok(())
}

/// Synchronize `leaf` and every directory above it up to and including `root`, so
/// each newly created (possibly multi-level Hive) directory entry is durable.
#[cfg(feature = "delta")]
pub(crate) fn sync_directory_chain(leaf: &Path, root: &Path) -> Result<()> {
    anyhow::ensure!(
        leaf.starts_with(root),
        "{} is outside table root {}",
        leaf.display(),
        root.display()
    );
    for directory in leaf.ancestors() {
        sync_directory(directory)?;
        if directory == root {
            break;
        }
    }
    Ok(())
}

/// Synchronize a canonical directory and all of its ancestors, making a newly
/// created nested table root durable even when existing entries were not yet synced.
#[cfg(feature = "delta")]
pub(crate) fn sync_ancestors(path: &Path) -> Result<()> {
    path.ancestors().try_for_each(sync_directory)
}

/// Test-only fsync observation and fault injection, keyed by exact path so parallel
/// tests using distinct temporary directories never interfere.
#[cfg(all(test, feature = "delta"))]
pub(crate) mod sync_hook {
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, PoisonError};

    static FAIL: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());
    static SYNCED: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

    type SyncGate = (PathBuf, tokio::sync::oneshot::Sender<()>, std::sync::mpsc::Receiver<()>);
    static GATES: Mutex<Vec<SyncGate>> = Mutex::new(Vec::new());

    pub(crate) fn pause_next(path: PathBuf) -> (tokio::sync::oneshot::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        GATES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((path, entered, wait));
        (ready, release)
    }

    pub(crate) fn fail_on(path: &Path) {
        FAIL.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(path.to_owned());
    }

    pub(crate) fn reset(under: &Path) {
        FAIL.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|path| !path.starts_with(under));
        SYNCED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|path| !path.starts_with(under));
    }

    pub(crate) fn synced_under(under: &Path) -> Vec<PathBuf> {
        SYNCED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|path| path.starts_with(under))
            .cloned()
            .collect()
    }

    pub(super) fn observe(path: &Path) -> anyhow::Result<()> {
        let gate = {
            let mut gates = GATES.lock().unwrap_or_else(PoisonError::into_inner);
            gates
                .iter()
                .position(|(p, _, _)| p == path)
                .map(|i| gates.swap_remove(i))
        };
        if let Some((_, entered, wait)) = gate {
            let _ = entered.send(());
            let _ = wait.recv();
        }
        if FAIL
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|failing| failing == path)
        {
            anyhow::bail!("injected fsync failure: {}", path.display());
        }
        SYNCED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(path.to_owned());
        Ok(())
    }
}

/// Timestamp conversion used for raw Delta inputs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampKernel {
    #[default]
    Reference,
    FusedChecked,
}
