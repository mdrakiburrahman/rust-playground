//! Streaming writers for ordinary Arrow batches, independent of telemetry.
//!
//! Parquet mode retains the supplied schema. Delta mode normalizes unsupported
//! logical types losslessly and uses Delta Kernel to create/evolve/commit tables.

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parquet::arrow::AsyncArrowWriter;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

#[cfg(feature = "delta")]
pub mod commit;
#[cfg(feature = "delta")]
pub mod delta;
#[cfg(feature = "delta")]
pub mod normalize;

/// A completed, durable Parquet file. It is not necessarily committed to Delta.
#[derive(Debug)]
pub struct CompletedFile {
    pub path: PathBuf,
    pub rows: usize,
    pub bytes: u64,
}

/// Streaming Parquet file writer. Dropping it does not finish the file.
pub struct ParquetFileWriter {
    writer: AsyncArrowWriter<tokio::fs::File>,
    durable_handle: File,
    path: PathBuf,
    rows: usize,
}

impl ParquetFileWriter {
    /// Create a unique file; never overwrite existing output.
    ///
    /// # Errors
    /// Returns file creation or Parquet schema errors.
    pub fn create(path: impl AsRef<Path>, schema: SchemaRef) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let durable_handle = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| format!("create Parquet {}", path.display()))?;
        let output = tokio::fs::File::from_std(durable_handle.try_clone()?);
        let writer = AsyncArrowWriter::try_new(output, schema, None)?;
        Ok(Self {
            writer,
            durable_handle,
            path,
            rows: 0,
        })
    }

    /// Append a batch without retaining it in memory.
    ///
    /// # Errors
    /// Returns schema or file-write errors.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch).await?;
        self.rows = self.rows.checked_add(batch.num_rows()).context("row count overflow")?;
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
            bytes,
        })
    }
}

pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {}", path.display()))?
        .sync_all()?;
    Ok(())
}
