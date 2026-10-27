use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_deltalake::commit::{BatchedTable, CommitOptions};
use arrow_deltalake::delta::PublicationFence as DeltaFence;
use arrowcol_buffer::{
    DeltaPublicationSink, PublicationContext, PublicationOutcome, PublicationSession, PublicationSink, SinkError,
};
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::lease::TableLeaseManager;
use crate::metrics::CollectorMetrics;

/// Multi-table Delta sink that opens one fenced coordinator per owned table.
pub struct ProductionDeltaSink {
    root: PathBuf,
    options: CommitOptions,
    leases: Arc<TableLeaseManager>,
    metrics: CollectorMetrics,
}

impl ProductionDeltaSink {
    /// Creates a dynamic multi-table publication sink.
    #[must_use]
    pub fn new(
        root: PathBuf,
        options: CommitOptions,
        leases: Arc<TableLeaseManager>,
        metrics: CollectorMetrics,
    ) -> Self {
        Self {
            root,
            options,
            leases,
            metrics,
        }
    }

    /// Returns the physical table path.
    #[must_use]
    pub fn table_path(&self, table: &arrowcol_protocol::TableIdentity) -> PathBuf {
        self.root
            .join(table.catalog().as_str())
            .join(table.schema().as_str())
            .join(table.table().as_str())
    }
}

#[async_trait]
impl PublicationSink for ProductionDeltaSink {
    async fn current_lease(
        &self,
        table: &arrowcol_protocol::TableIdentity,
        admitted: &arrowcol_protocol::TableLease,
    ) -> Result<arrowcol_protocol::TableLease, SinkError> {
        let handle = self
            .leases
            .handle_for_fence(table, &admitted.publication_fence())
            .await
            .map_err(|error| SinkError::fenced(format!("refresh queued publication lease: {error:#}")))?;
        handle
            .current()
            .map_err(|error| SinkError::fenced(format!("load confirmed publication lease: {error:#}")))
    }

    async fn begin(&self, context: PublicationContext) -> Result<Box<dyn PublicationSession>, SinkError> {
        let handle = self
            .leases
            .handle_for_fence(&context.table, &context.fence)
            .await
            .map_err(|error| SinkError::fenced(format!("validate table publication lease: {error:#}")))?;
        let table = if let Some(table) = handle.delta_table().await {
            table
        } else {
            let fence: Arc<dyn DeltaFence> = handle.delta_fence();
            let root = self.table_path(&context.table);
            let opened = BatchedTable::open_uri_with_fence(&root.to_string_lossy(), self.options.clone(), fence)
                .await
                .map_err(|error| SinkError::retryable(format!("open Delta table coordinator: {error:#}")))?;
            handle.install_delta_table(opened).await
        };
        let session = DeltaPublicationSink::new(
            table,
            context.table.clone(),
            context.schema_fingerprint,
            context.fence.clone(),
        )
        .begin(context)
        .await?;
        Ok(Box::new(CountingSession {
            inner: session,
            rows: 0,
            metrics: self.metrics.clone(),
        }))
    }
}

struct CountingSession {
    inner: Box<dyn PublicationSession>,
    rows: u64,
    metrics: CollectorMetrics,
}

#[async_trait]
impl PublicationSession for CountingSession {
    async fn write(&mut self, batch: &RecordBatch) -> Result<(), SinkError> {
        self.inner.write(batch).await?;
        self.rows = self.rows.saturating_add(batch.num_rows() as u64);
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<PublicationOutcome, SinkError> {
        let Self { inner, rows, metrics } = *self;
        let outcome = inner.finish().await?;
        metrics.exported_rows(rows);
        Ok(outcome)
    }
}

/// Verifies that the configured Delta root can create, synchronize, and remove
/// a file on the target filesystem.
///
/// # Errors
///
/// Returns the failed filesystem operation.
pub async fn verify_writable_root(root: &Path) -> std::io::Result<()> {
    let uri = root.to_string_lossy();
    if uri.contains("://") {
        let uri = uri.into_owned();
        return tokio::task::spawn_blocking(move || arrow_deltalake::delta::verify_writable_uri(&uri))
            .await
            .map_err(std::io::Error::other)?
            .map_err(std::io::Error::other);
    }
    tokio::fs::create_dir_all(root).await?;
    let probe = root.join(format!(".arrowcol-write-probe-{}", Uuid::new_v4()));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .await?;
    file.write_all(b"arrowcol").await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::remove_file(&probe).await?;
    tokio::fs::File::open(root).await?.sync_all().await
}
