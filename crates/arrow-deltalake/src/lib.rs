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
mod azure_fence;
#[cfg(feature = "delta")]
pub mod commit;
#[cfg(feature = "delta")]
pub mod delta;
#[cfg(feature = "delta")]
mod delta_io;
pub mod layout;
#[cfg(feature = "delta")]
pub mod normalize;
pub mod sizing;

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
    writer: AsyncArrowWriter<tokio::fs::File>,
    durable_handle: File,
    path: PathBuf,
    rows: usize,
    input_bytes: u64,
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
        let path = path.as_ref().to_path_buf();
        let durable_handle = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| format!("create Parquet {}", path.display()))?;
        let output = tokio::fs::File::from_std(durable_handle.try_clone()?);
        let writer = AsyncArrowWriter::try_new(output, schema, properties)?;
        Ok(Self {
            writer,
            durable_handle,
            path,
            rows: 0,
            input_bytes: 0,
        })
    }

    /// Append a batch without retaining it in memory.
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
        self.writer.write(batch).await?;
        self.rows = rows;
        self.input_bytes = input_bytes;
        Ok(())
    }

    /// Write the footer and synchronize data and its directory before success.
    ///
    /// # Errors
    /// Returns close, metadata, or synchronization errors.
    pub async fn finish(self) -> Result<CompletedFile> {
        self.writer.close().await?;
        let durable_handle = tokio::fs::File::from_std(self.durable_handle);
        durable_handle.sync_all().await?;
        let bytes = durable_handle.metadata().await?.len();
        let parent = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        tokio::fs::File::open(parent).await?.sync_all().await?;
        Ok(CompletedFile {
            path: self.path,
            rows: self.rows,
            input_bytes: self.input_bytes,
            bytes,
        })
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
    File::open(path)
        .with_context(|| format!("open directory {}", path.display()))?
        .sync_all()?;
    Ok(())
}
