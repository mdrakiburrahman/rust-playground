use crate::delta::streaming_sink::{StreamingSink, StreamingUploadError};
use crate::pipelined_writer::PipelinedArrowWriter;
use anyhow::{Context, Result, bail};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use bytes::Bytes;
use parquet::{arrow::async_writer::AsyncFileWriter, file::properties::WriterProperties};
use std::{io::Write, time::Duration};
use tokio::sync::mpsc;

const CHUNK_BYTES: usize = 64 * 1024;
const QUEUED_CHUNKS: usize = 2;

enum Message {
    Data(Bytes),
    Complete,
}

struct ColumnSink(mpsc::Sender<Message>);

impl Write for ColumnSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        for chunk in bytes.chunks(CHUNK_BYTES) {
            let message = Message::Data(Bytes::copy_from_slice(chunk));
            let result = match self.0.try_send(message) {
                Ok(()) => Ok(()),
                // Construction emits only PAR1; subsequent writes run on the writer's blocking I/O worker.
                Err(mpsc::error::TrySendError::Full(message)) => self.0.blocking_send(message),
                Err(mpsc::error::TrySendError::Closed(message)) => Err(mpsc::error::SendError(message)),
            };
            result.map_err(|_| {
                std::io::Error::other(StreamingUploadError(anyhow::anyhow!(
                    "Parquet column stream consumer stopped"
                )))
            })?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn consume<S: AsyncFileWriter>(mut sink: S, mut messages: mpsc::Receiver<Message>) -> Result<S> {
    while let Some(message) = messages.recv().await {
        match message {
            Message::Data(bytes) => sink.write(bytes).await?,
            Message::Complete => {
                sink.complete().await?;
                return Ok(sink);
            }
        }
    }
    bail!("Parquet column stream ended without a finished footer");
}

pub(crate) struct StreamingColumnWriter {
    writer: PipelinedArrowWriter<ColumnSink>,
    upload: tokio::task::JoinHandle<Result<(u64, Duration)>>,
}

impl StreamingColumnWriter {
    pub(crate) fn try_new(
        sink: StreamingSink,
        schema: SchemaRef,
        properties: WriterProperties,
        column_workers: u8,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(QUEUED_CHUNKS);
        let writer = PipelinedArrowWriter::try_new(ColumnSink(sender), schema, properties, column_workers)?;
        let upload = crate::delta_io::spawn_shared_io(async move {
            Ok(consume(sink, receiver).await?.observation())
        })?;
        Ok(Self { writer, upload })
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch).await
    }

    pub(crate) async fn finish(self) -> Result<(u64, Duration)> {
        let sink = match self.writer.finish().await {
            Ok(sink) => sink,
            Err(error) => {
                // Let dropping the stream abort any multipart upload before returning the encode failure.
                let _upload = self.upload.await;
                return Err(error);
            }
        };
        let signalled = sink.0.send(Message::Complete).await;
        drop(sink);
        let observation = self.upload.await.context("Parquet column upload worker failed")??;
        signalled.context("Parquet column upload stopped before footer confirmation")?;
        Ok(observation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt, future::BoxFuture};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    struct Sink {
        bytes: Vec<u8>,
        completed: Arc<AtomicBool>,
        fail: bool,
    }

    impl AsyncFileWriter for Sink {
        fn write(&mut self, bytes: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
            async move {
                if self.fail {
                    return Err(parquet::errors::ParquetError::General("injected upload failure".into()));
                }
                self.bytes.extend_from_slice(&bytes);
                Ok(())
            }.boxed()
        }

        fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
            async {
                self.completed.store(true, Ordering::SeqCst);
                Ok(())
            }.boxed()
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_bridge_preserves_bytes_and_requires_footer_confirmation() -> Result<()> {
        for complete in [false, true] {
            let completed = Arc::new(AtomicBool::new(false));
            let sink = Sink { bytes: Vec::new(), completed: Arc::clone(&completed), fail: false };
            let (sender, receiver) = mpsc::channel(QUEUED_CHUNKS);
            let mut writer = ColumnSink(sender);
            writer.write_all(b"PAR1")?;
            let producer = tokio::task::spawn_blocking(move || -> Result<_> {
                writer.write_all(&vec![7; CHUNK_BYTES * 8 + 1])?;
                Ok(writer)
            });
            let consumer = tokio::spawn(consume(sink, receiver));
            let writer = producer.await??;
            if complete {
                writer.0.send(Message::Complete).await?;
            }
            drop(writer);
            let result = consumer.await?;
            assert_eq!(completed.load(Ordering::SeqCst), complete);
            if complete {
                let sink = result?;
                assert_eq!(&sink.bytes[..4], b"PAR1");
                assert_eq!(&sink.bytes[4..], &vec![7; CHUNK_BYTES * 8 + 1]);
            } else {
                assert!(result.is_err());
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn upload_failure_releases_blocked_producer_without_completing() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let completed = Arc::new(AtomicBool::new(false));
            let (sender, receiver) = mpsc::channel(QUEUED_CHUNKS);
            let sink = Sink { bytes: Vec::new(), completed: Arc::clone(&completed), fail: true };
            let producer = tokio::task::spawn_blocking(move || {
                ColumnSink(sender).write_all(&vec![0; CHUNK_BYTES * 8])
            });
            assert!(consume(sink, receiver).await.is_err());
            let error = anyhow::Error::new(producer.await?.expect_err("consumer failed"));
            assert!(error.chain().any(|cause| cause.is::<StreamingUploadError>()));
            assert!(!completed.load(Ordering::SeqCst));
            Ok::<_, anyhow::Error>(())
        }).await??;
        Ok(())
    }
}
