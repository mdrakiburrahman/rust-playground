use crate::delta::DeltaTable;
use crate::delta_io::{runtime_config, shared_executor};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures::{FutureExt, future::BoxFuture};
use object_store::path::Path;
use parquet::{arrow::async_writer::AsyncFileWriter, errors::ParquetError};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{io::AsyncWriteExt, sync::oneshot};

/// An upload failure may surface while encoding a row group. Its delivery must remain retryable.
#[derive(Debug)]
pub struct StreamingUploadError(pub(crate) anyhow::Error);

impl std::fmt::Display for StreamingUploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "streaming Parquet upload: {}", self.0)
    }
}
impl std::error::Error for StreamingUploadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

pub(crate) struct StreamingSink {
    table: Arc<DeltaTable>,
    path: Path,
    bytes: u64,
    wait: Duration,
    live: Option<Live>,
}

struct Live {
    pipe: tokio::io::DuplexStream,
    finished: oneshot::Sender<u64>,
    task: tokio::task::JoinHandle<Result<Duration>>,
}

impl StreamingSink {
    pub(crate) fn new(table: Arc<DeltaTable>, path: Path) -> Self {
        Self {
            table,
            path,
            bytes: 0,
            wait: Duration::ZERO,
            live: None,
        }
    }

    pub(crate) fn observation(self) -> (u64, Duration) {
        (self.bytes, self.wait)
    }

    async fn write_bytes(&mut self, bytes: Bytes) -> Result<()> {
        let started = Instant::now();
        let total = self
            .bytes
            .checked_add(bytes.len() as u64)
            .context("encoded size overflow")?;
        ensure!(total <= 512 * 1024 * 1024, "staged file exceeds bounded upload size");
        if self.live.is_none() {
            let permit = shared_executor()?.upload_slot().await?;
            let (pipe, reader) = tokio::io::duplex(64 * 1024);
            let (finished, expected) = oneshot::channel();
            let table = Arc::clone(&self.table);
            let path = self.path.clone();
            let config = runtime_config()?;
            let task = crate::delta::spawn_shared_io(async move {
                let _permit = permit;
                table.check_publication_async().await?;
                super::upload_multipart(
                    Arc::clone(&table.store),
                    path,
                    reader,
                    async { expected.await.context("Parquet stream ended without a finished footer") },
                    config.upload_buffer_capacity_bytes,
                    config.upload_request_concurrency,
                )
                .await
            })?;
            self.live = Some(Live { pipe, finished, task });
        }
        let live = self.live.as_mut().context("streaming upload was not started")?;
        if let Err(io_error) = live.pipe.write_all(&bytes).await {
            let Live { pipe, finished, task } = self.live.take().context("streaming upload was not started")?;
            drop((pipe, finished));
            task.await.context("streaming upload worker failed")??;
            return Err(io_error.into());
        }
        self.wait += started.elapsed();
        self.bytes = total;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        let started = Instant::now();
        let Live { pipe, finished, task } = self.live.take().context("streaming upload has no footer")?;
        // EOF alone is not a completed file: dropping an unfinished writer must abort multipart.
        let signalled = finished.send(self.bytes);
        drop(pipe);
        task.await.context("streaming upload worker failed")??;
        self.wait += started.elapsed();
        ensure!(signalled.is_ok(), "streaming upload stopped before footer confirmation");
        Ok(())
    }
}

impl AsyncFileWriter for StreamingSink {
    fn write(&mut self, bytes: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async {
            self.write_bytes(bytes)
                .await
                .map_err(|e| ParquetError::External(Box::new(StreamingUploadError(e))))
        }
        .boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async {
            self.finish()
                .await
                .map_err(|e| ParquetError::External(Box::new(StreamingUploadError(e))))
        }
        .boxed()
    }
}
