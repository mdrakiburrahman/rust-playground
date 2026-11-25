use anyhow::{Context, Result, ensure};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use delta_kernel_default_engine::executor::TaskExecutor;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowLeafColumn, ArrowRowGroupWriterFactory, ArrowWriter, ArrowWriterOptions,
    compute_leaves,
};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use std::io::Write;
use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;

/// Persistent column owners overlap batches without a per-batch or column-wave barrier.
///
/// At most `2 * column_workers` batch slices retain Arrow leaves, and at most
/// `column_workers` CPU jobs are admitted at once. One row group may await assembly
/// while another is being appended. These are queue bounds, not a hard RSS limit:
/// slices retain their backing arrays, and encoders/chunks retain row-group data.
///
/// Construction writes the Parquet header synchronously, like `ArrowWriter`.
/// Call it on a blocking thread when the sink can block. Subsequent sink writes,
/// including the footer, always run on the shared blocking I/O executor.
pub(crate) struct PipelinedArrowWriter<W: Write + Send + 'static> {
    schema: SchemaRef,
    factory: ArrowRowGroupWriterFactory,
    max_row_group_rows: Option<usize>,
    row_group_index: usize,
    row_group_rows: usize,
    lanes: Vec<mpsc::UnboundedSender<LaneMessage>>,
    workers: Vec<JoinHandle<Result<()>>>,
    row_groups: Option<mpsc::Sender<RowGroupJob>>,
    assembler: Option<JoinHandle<Result<SerializedFileWriter<BlockingSink<W>>>>>,
    state: Arc<PipelineState>,
    finished: bool,
}

// Parquet's internal BufWriter also writes from Drop. Suppress that flush when
// an async error/cancellation discards a writer outside the blocking I/O scope.
struct BlockingSink<W> {
    inner: W,
    enabled: bool,
    failed: bool,
    state: Arc<PipelineState>,
}

impl<W> BlockingSink<W> {
    fn preserve_error(&self, error: std::io::Error) -> std::io::Error {
        if error.kind() == std::io::ErrorKind::Interrupted {
            return error;
        }
        let kind = error.kind();
        // Capture before Parquet's footer-flush path turns I/O errors into text.
        let failure = self.state.fail(anyhow::Error::new(SinkFailure(error)));
        std::io::Error::new(kind, failure)
    }
}

impl<W: Write> Write for BlockingSink<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if !self.enabled || self.failed {
            return Err(std::io::Error::other("pipelined Parquet sink is no longer writable"));
        }
        let result = self.inner.write(bytes);
        self.failed = match &result {
            Ok(0) => !bytes.is_empty(),
            Err(error) => error.kind() != std::io::ErrorKind::Interrupted,
            _ => false,
        };
        result.map_err(|error| self.preserve_error(error))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if !self.enabled || self.failed {
            return Err(std::io::Error::other("pipelined Parquet sink is no longer writable"));
        }
        let result = self.inner.flush();
        self.failed = result
            .as_ref()
            .is_err_and(|error| error.kind() != std::io::ErrorKind::Interrupted);
        result.map_err(|error| self.preserve_error(error))
    }
}

#[derive(Debug)]
struct SinkFailure(std::io::Error);

impl std::fmt::Display for SinkFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, formatter)
    }
}

impl std::error::Error for SinkFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // io::Error::source skips the custom inner error itself. Expose it so
        // retry classification can still downcast to the sink's marker type.
        Some(match self.0.get_ref() {
            Some(inner) => inner,
            None => &self.0,
        })
    }
}

#[derive(Debug)]
struct SharedFailure(Arc<anyhow::Error>);

impl std::fmt::Display for SharedFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "pipelined Parquet writer is unusable: {}", self.0)
    }
}

impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}

struct PipelineState {
    failure: OnceLock<Arc<anyhow::Error>>,
    window: Arc<Semaphore>,
    cpu: Arc<Semaphore>,
}

impl PipelineState {
    fn fail(&self, error: anyhow::Error) -> SharedFailure {
        let failure = self.failure.get_or_init(|| Arc::new(error));
        self.window.close();
        self.cpu.close();
        SharedFailure(Arc::clone(failure))
    }

    fn check(&self) -> Result<()> {
        if let Some(failure) = self.failure.get() {
            return Err(SharedFailure(Arc::clone(failure)).into());
        }
        Ok(())
    }
}

// Also fires when an async operation is cancelled or a worker unwinds.
struct StopOnDrop {
    state: Arc<PipelineState>,
    reason: &'static str,
    armed: bool,
}

impl StopOnDrop {
    fn new(state: &Arc<PipelineState>, reason: &'static str) -> Self {
        Self {
            state: Arc::clone(state),
            reason,
            armed: true,
        }
    }

    fn complete<T>(&mut self, result: Result<T>) -> Result<T> {
        self.armed = false;
        result.map_err(|error| self.state.fail(error).into())
    }
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.state.fail(anyhow::anyhow!(self.reason));
        }
    }
}

enum LaneMessage {
    Leaf {
        leaf: ArrowLeafColumn,
        _slot: Arc<OwnedSemaphorePermit>,
    },
    Close {
        next: Option<Box<ArrowColumnWriter>>,
        chunk: oneshot::Sender<ArrowColumnChunk>,
    },
}

struct RowGroupJob {
    chunks: Vec<oneshot::Receiver<ArrowColumnChunk>>,
    rows: usize,
}

impl<W: Write + Send + 'static> PipelinedArrowWriter<W> {
    #[cfg(test)]
    pub(crate) fn try_new(
        writer: W,
        schema: SchemaRef,
        properties: WriterProperties,
        column_workers: u8,
    ) -> Result<Self> {
        Self::try_new_with_options(writer, schema, properties, column_workers, false)
    }

    pub(crate) fn try_new_with_options(
        writer: W,
        schema: SchemaRef,
        properties: WriterProperties,
        column_workers: u8,
        skip_arrow_metadata: bool,
    ) -> Result<Self> {
        ensure!(
            (2..=16).contains(&column_workers),
            "column_pipeline requires collector.runtime.parquet_column_workers between 2 and 16"
        );
        ensure!(
            properties.content_defined_chunking().is_none(),
            "column_pipeline does not support Parquet content-defined chunking"
        );
        ensure!(
            properties.max_row_group_bytes().is_none(),
            "column_pipeline does not support Parquet max_row_group_bytes; use a fixed row-group row limit"
        );
        let max_row_group_rows = properties.max_row_group_row_count();
        ensure!(
            max_row_group_rows.is_none_or(|rows| rows > 0),
            "Parquet max row-group rows must be positive"
        );

        let state = Arc::new(PipelineState {
            failure: OnceLock::new(),
            window: Arc::new(Semaphore::new(2 * usize::from(column_workers))),
            cpu: Arc::new(Semaphore::new(usize::from(column_workers))),
        });
        let sink = BlockingSink {
            inner: writer,
            enabled: true,
            failed: false,
            state: Arc::clone(&state),
        };
        let writer = ArrowWriter::try_new_with_options(
            sink,
            Arc::clone(&schema),
            ArrowWriterOptions::new()
                .with_properties(properties)
                .with_skip_arrow_metadata(skip_arrow_metadata),
        )?;
        let (mut file_writer, factory) = writer.into_serialized_writer()?;
        file_writer.inner_mut().enabled = false;
        let columns = factory.create_column_writers(0)?;
        let (row_groups, receiver) = mpsc::channel(1);
        let mut writer = Self {
            schema,
            factory,
            max_row_group_rows,
            row_group_index: 0,
            row_group_rows: 0,
            lanes: Vec::with_capacity(columns.len()),
            workers: Vec::with_capacity(columns.len()),
            row_groups: Some(row_groups),
            assembler: None,
            state,
            finished: false,
        };
        for column in columns {
            let (sender, receiver) = mpsc::unbounded_channel();
            writer.workers.push(crate::delta_io::spawn_shared_io(run_lane(
                column,
                receiver,
                Arc::clone(&writer.state),
            ))?);
            writer.lanes.push(sender);
        }
        writer.assembler = Some(crate::delta_io::spawn_shared_io(assemble(
            file_writer,
            receiver,
            Arc::clone(&writer.state),
        ))?);
        Ok(writer)
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.state.check()?;
        let mut guard = StopOnDrop::new(&self.state, "write was cancelled before all slices were enqueued");
        let result = self.write_inner(batch).await;
        guard.complete(result)
    }

    async fn write_inner(&mut self, batch: &RecordBatch) -> Result<()> {
        ensure!(
            batch.schema().as_ref() == self.schema.as_ref(),
            "Parquet batch schema does not match the writer schema"
        );
        ensure!(
            batch.num_rows() == 0 || !self.lanes.is_empty(),
            "cannot encode nonempty Parquet batches without physical columns"
        );
        let mut offset = 0;
        while offset < batch.num_rows() {
            self.state.check()?;
            let rows = self.max_row_group_rows.map_or(batch.num_rows() - offset, |limit| {
                (limit - self.row_group_rows).min(batch.num_rows() - offset)
            });
            let slot = Arc::new(
                Arc::clone(&self.state.window)
                    .acquire_owned()
                    .await
                    .context("Parquet batch-slice admission stopped")?,
            );
            self.state.check()?;
            let slice = batch.slice(offset, rows);
            let mut leaves = Vec::with_capacity(self.lanes.len());
            for (field, array) in self.schema.fields().iter().zip(slice.columns()) {
                leaves.extend(compute_leaves(field, array)?);
            }
            ensure!(
                leaves.len() == self.lanes.len(),
                "Parquet leaf count does not match the writer schema"
            );
            let next_rows = self
                .row_group_rows
                .checked_add(rows)
                .context("Parquet row-group row count overflow")?;
            for (lane, leaf) in self.lanes.iter().zip(leaves) {
                lane.send(LaneMessage::Leaf {
                    leaf,
                    _slot: Arc::clone(&slot),
                })
                .map_err(|_| anyhow::anyhow!("Parquet column lane terminated"))?;
            }
            drop(slot);
            self.row_group_rows = next_rows;
            offset += rows;
            if self.max_row_group_rows == Some(self.row_group_rows) {
                self.close_row_group(false).await?;
            }
        }
        self.state.check()
    }

    async fn close_row_group(&mut self, last: bool) -> Result<()> {
        if self.row_group_rows == 0 {
            return Ok(());
        }
        // Reserve before queuing close markers or allocating successor encoders.
        // Thus fast lanes cannot accumulate unbounded completed row groups.
        let reserved = self
            .row_groups
            .as_ref()
            .context("Parquet row-group assembler is unavailable")?
            .reserve()
            .await
            .context("Parquet row-group assembler terminated")?;
        self.state.check()?;
        let next_index = self
            .row_group_index
            .checked_add(1)
            .context("Parquet row-group index overflow")?;
        let next = if last {
            (0..self.lanes.len()).map(|_| None).collect::<Vec<_>>()
        } else {
            let columns = self.factory.create_column_writers(next_index)?;
            ensure!(
                columns.len() == self.lanes.len(),
                "Parquet successor leaf count changed"
            );
            columns.into_iter().map(|column| Some(Box::new(column))).collect()
        };
        let mut chunks = Vec::with_capacity(self.lanes.len());
        for (lane, next) in self.lanes.iter().zip(next) {
            let (chunk, receiver) = oneshot::channel();
            lane.send(LaneMessage::Close { next, chunk })
                .map_err(|_| anyhow::anyhow!("Parquet column lane terminated during close"))?;
            chunks.push(receiver);
        }
        reserved.send(RowGroupJob {
            chunks,
            rows: self.row_group_rows,
        });
        self.row_group_index = next_index;
        self.row_group_rows = 0;
        Ok(())
    }

    pub(crate) async fn finish(mut self) -> Result<W> {
        let result = match self.state.check() {
            Ok(()) => self.close_row_group(true).await,
            error => error,
        };
        if let Err(error) = result {
            self.state.fail(error);
        }
        self.lanes.clear();
        self.row_groups.take();

        // Join even after an error: all admitted CPU jobs must return before
        // finalization reports failure. Never abort a task awaiting CPU work.
        for worker in self.workers.drain(..) {
            match worker.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    self.state.fail(error);
                }
                Err(error) => {
                    self.state
                        .fail(anyhow::Error::new(error).context("Parquet column task failed"));
                }
            }
        }
        let assembled = match self.assembler.take() {
            Some(assembler) => assembler
                .await
                .context("Parquet assembler task failed")
                .and_then(|result| result),
            None => Err(anyhow::anyhow!("Parquet row-group assembler is unavailable")),
        };
        let assembled = assembled.map_err(|error| self.state.fail(error));
        self.state.check()?;
        let mut file_writer = assembled?;
        let state = Arc::clone(&self.state);
        let result = crate::delta_io::shared_executor()?
            .spawn_blocking(move || {
                state.check()?;
                file_writer.inner_mut().enabled = true;
                file_writer
                    .into_inner()
                    .map(|sink| sink.inner)
                    .context("close pipelined Parquet file")
            })
            .await?;
        let result = result.map_err(|error| self.state.fail(error).into());
        self.finished = result.is_ok();
        result
    }
}

impl<W: Write + Send + 'static> Drop for PipelinedArrowWriter<W> {
    fn drop(&mut self) {
        if !self.finished {
            // Closing admission wakes idle workers; their already admitted work
            // checks this flag between messages. A synchronous Write already in
            // progress cannot be interrupted, just as with ArrowWriter.
            self.state
                .fail(anyhow::anyhow!("writer dropped before successful finish"));
        }
    }
}

async fn run_lane(
    column: ArrowColumnWriter,
    mut receiver: mpsc::UnboundedReceiver<LaneMessage>,
    state: Arc<PipelineState>,
) -> Result<()> {
    let mut guard = StopOnDrop::new(&state, "Parquet column task was cancelled or panicked");
    let result = async {
        let mut column = Some(column);
        while let Some(first) = receiver.recv().await {
            state.check()?;
            let mut messages = vec![first];
            while messages.len() < 8 {
                match receiver.try_recv() {
                    Ok(message) => messages.push(message),
                    Err(_) => break,
                }
            }
            let permit = Arc::clone(&state.cpu)
                .acquire_owned()
                .await
                .context("Parquet column CPU admission stopped")?;
            let work_state = Arc::clone(&state);
            column = crate::delta_io::run_cpu_async(move || {
                let _permit = permit;
                let mut guard = StopOnDrop::new(&work_state, "Parquet column encoder panicked");
                let result = encode_messages(column, messages, &work_state);
                guard.complete(result)
            })
            .await??;
        }
        state.check()
    }
    .await;
    guard.complete(result)
}

fn encode_messages(
    mut column: Option<ArrowColumnWriter>,
    messages: Vec<LaneMessage>,
    state: &PipelineState,
) -> Result<Option<ArrowColumnWriter>> {
    for message in messages {
        state.check()?;
        match message {
            LaneMessage::Leaf { leaf, _slot } => {
                column
                    .as_mut()
                    .context("Parquet column is already closed")?
                    .write(&leaf)?;
            }
            LaneMessage::Close { next, chunk } => {
                let encoded = column.take().context("Parquet column is already closed")?.close()?;
                column = next.map(|column| *column);
                chunk
                    .send(encoded)
                    .map_err(|_| anyhow::anyhow!("Parquet column chunk receiver terminated"))?;
            }
        }
    }
    Ok(column)
}

async fn assemble<W: Write + Send + 'static>(
    mut writer: SerializedFileWriter<BlockingSink<W>>,
    mut receiver: mpsc::Receiver<RowGroupJob>,
    state: Arc<PipelineState>,
) -> Result<SerializedFileWriter<BlockingSink<W>>> {
    let mut guard = StopOnDrop::new(&state, "Parquet assembler was cancelled or panicked");
    let result = async {
        while let Some(job) = receiver.recv().await {
            state.check()?;
            let mut chunks = Vec::with_capacity(job.chunks.len());
            for chunk in job.chunks {
                chunks.push(
                    chunk
                        .await
                        .context("Parquet column terminated without a completed chunk")?,
                );
            }
            let io_state = Arc::clone(&state);
            writer = crate::delta_io::shared_executor()?
                .spawn_blocking(move || -> Result<_> {
                    io_state.check()?;
                    writer.inner_mut().enabled = true;
                    let mut row_group = writer.next_row_group()?;
                    for chunk in chunks {
                        io_state.check()?;
                        chunk.append_to_row_group(&mut row_group)?;
                    }
                    let metadata = row_group.close()?;
                    ensure!(
                        metadata.num_rows() > 0 && metadata.num_rows() as u64 == job.rows as u64,
                        "Parquet assembled row-group row count mismatch"
                    );
                    writer.inner_mut().enabled = false;
                    Ok(writer)
                })
                .await??;
        }
        state.check()?;
        Ok(writer)
    }
    .await;
    guard.complete(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Int32Array, Int64Array, ListArray, StringArray, StructArray, UInt8Array,
    };
    use arrow::buffer::NullBuffer;
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Int32Type, Schema, UInt8Type};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::metadata::SortingColumn;
    use parquet::file::properties::{CdcOptions, EnabledStatistics};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    fn batches() -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let fields = vec![
            Arc::new(Field::new("number", DataType::Int32, true)),
            Arc::new(Field::new("label", DataType::Utf8, true)),
        ];
        let lists = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
            Some(vec![Some(3)]),
            Some(vec![None]),
            None,
            Some(vec![Some(6), Some(7)]),
            Some(vec![]),
            Some(vec![Some(8)]),
            None,
            Some(vec![None, Some(10)]),
            Some(vec![Some(11)]),
        ]);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("nullable", DataType::Utf8, true),
                Field::new("nested", DataType::Struct(fields.clone().into()), true),
                Field::new("list", lists.data_type().clone(), true),
                Field::new(
                    "dictionary",
                    DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                    true,
                ),
            ],
            HashMap::from([("source".into(), "pipeline differential".into())]),
        ));
        let nested = StructArray::new(
            fields.into(),
            vec![
                Arc::new(Int32Array::from_iter((0..12).map(|i| (i % 3 != 0).then_some(i)))) as ArrayRef,
                Arc::new(StringArray::from_iter(
                    (0..12).map(|i| (i % 4 != 0).then(|| format!("nested-{i}"))),
                )),
            ],
            Some(NullBuffer::from((0..12).map(|i| i % 5 != 0).collect::<Vec<_>>())),
        );
        let dictionary = DictionaryArray::<UInt8Type>::try_new(
            UInt8Array::from_iter((0..12).map(|i| (i % 4 != 0).then_some((i % 3) as u8))),
            Arc::new(StringArray::from(vec!["x", "a longer dictionary value", "z"])),
        )?;
        let all = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..12)) as ArrayRef,
                Arc::new(StringArray::from_iter(
                    (0..12).map(|i| (i % 3 != 0).then(|| "v".repeat(i + 1))),
                )),
                Arc::new(nested),
                Arc::new(lists),
                Arc::new(dictionary),
            ],
        )?;
        Ok((
            schema,
            vec![
                all.slice(0, 1),
                all.slice(0, 0),
                all.slice(1, 5),
                all.slice(6, 4),
                all.slice(10, 0),
                all.slice(10, 2),
            ],
        ))
    }

    fn properties(rows: Option<usize>) -> WriterProperties {
        WriterProperties::builder()
            .set_max_row_group_row_count(rows)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_data_page_row_count_limit(2)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_bloom_filter_enabled(true)
            .set_sorting_columns(Some(vec![SortingColumn {
                column_idx: 0,
                descending: false,
                nulls_first: false,
            }]))
            .build()
    }

    async fn compare(schema: SchemaRef, batches: &[RecordBatch], rows: Option<usize>, workers: u8) -> Result<()> {
        let props = properties(rows);
        let mut sequential = ArrowWriter::try_new(Vec::new(), Arc::clone(&schema), Some(props.clone()))?;
        let mut pipeline = PipelinedArrowWriter::try_new(Vec::new(), Arc::clone(&schema), props, workers)?;
        for batch in batches {
            sequential.write(batch)?;
            pipeline.write(batch).await?;
        }
        let expected_bytes = sequential.into_inner()?;
        let actual_bytes = tokio::time::timeout(Duration::from_secs(10), pipeline.finish()).await??;
        assert_eq!(actual_bytes, expected_bytes);
        let expected_reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(expected_bytes))?;
        let actual_reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(actual_bytes))?;
        assert_eq!(actual_reader.schema(), &schema);
        assert_eq!(actual_reader.metadata(), expected_reader.metadata());
        let total = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let expected_groups = if total == 0 {
            0
        } else {
            rows.map_or(1, |rows| total.div_ceil(rows))
        };
        assert_eq!(actual_reader.metadata().num_row_groups(), expected_groups);
        assert_eq!(actual_reader.metadata().file_metadata().num_rows(), total as i64);
        assert!(
            actual_reader
                .metadata()
                .row_groups()
                .iter()
                .all(|group| group.num_rows() > 0)
        );
        let decoded = actual_reader.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(concat_batches(&schema, &decoded)?, concat_batches(&schema, batches)?);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn nested_dictionary_nullable_batches_match_sequential_bytes_and_metadata() -> Result<()> {
        let (schema, batches) = batches()?;
        for (rows, workers) in [(Some(3), 2), (Some(5), 4), (Some(1), 16), (None, 2)] {
            compare(Arc::clone(&schema), &batches, rows, workers).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn empty_inputs_and_exact_boundaries_do_not_create_empty_row_groups() -> Result<()> {
        let (schema, batches) = batches()?;
        compare(Arc::clone(&schema), &[], Some(3), 2).await?;
        compare(
            Arc::clone(&schema),
            &[RecordBatch::new_empty(Arc::clone(&schema))],
            Some(3),
            2,
        )
        .await?;
        let all = concat_batches(&schema, &batches)?;
        compare(Arc::clone(&schema), std::slice::from_ref(&all), Some(3), 2).await?;
        compare(schema, &[all.slice(0, 6), all.slice(6, 6)], Some(3), 2).await
    }

    #[test]
    fn rejects_unsupported_options_and_worker_counts() -> Result<()> {
        let (schema, _) = batches()?;
        for workers in [0, 1, 17, u8::MAX] {
            let result = PipelinedArrowWriter::try_new(Vec::new(), Arc::clone(&schema), properties(Some(3)), workers);
            assert!(
                result
                    .err()
                    .context("invalid workers accepted")?
                    .to_string()
                    .contains("between 2 and 16")
            );
        }
        for (props, message) in [
            (
                WriterProperties::builder().set_max_row_group_bytes(Some(1024)).build(),
                "max_row_group_bytes",
            ),
            (
                WriterProperties::builder()
                    .set_content_defined_chunking(Some(CdcOptions::default()))
                    .build(),
                "content-defined chunking",
            ),
        ] {
            let result = PipelinedArrowWriter::try_new(Vec::new(), Arc::clone(&schema), props, 2);
            assert!(
                result
                    .err()
                    .context("unsupported option accepted")?
                    .to_string()
                    .contains(message)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn schema_mismatch_even_on_empty_batches_poisons_and_drains() -> Result<()> {
        let (schema, batches) = batches()?;
        let mut writer = PipelinedArrowWriter::try_new(Vec::new(), schema, properties(Some(100)), 2)?;
        writer.write(&batches[0]).await?;
        let wrong_schema = Arc::new(Schema::new(vec![Field::new("wrong", DataType::Int64, true)]));
        let error = writer
            .write(&RecordBatch::new_empty(wrong_schema))
            .await
            .err()
            .context("schema mismatch accepted")?;
        assert!(error.to_string().contains("schema"));
        assert!(writer.write(&batches[0]).await.is_err());
        assert!(
            tokio::time::timeout(Duration::from_secs(5), writer.finish())
                .await?
                .is_err()
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounded_slice_admission_and_cancelled_write_poison_the_writer() -> Result<()> {
        let (schema, batches) = batches()?;
        let mut writer = PipelinedArrowWriter::try_new(Vec::new(), schema, properties(None), 2)?;
        let state = Arc::clone(&writer.state);
        let held = Arc::clone(&state.cpu).acquire_many_owned(2).await?;
        for _ in 0..4 {
            writer.write(&batches[0]).await?;
        }
        assert_eq!(state.window.available_permits(), 0);
        {
            let pending = writer.write(&batches[0]);
            tokio::pin!(pending);
            assert!(futures::poll!(pending.as_mut()).is_pending());
        }
        assert!(writer.write(&batches[0]).await.is_err());
        drop(held);
        assert!(
            tokio::time::timeout(Duration::from_secs(5), writer.finish())
                .await?
                .is_err()
        );
        assert_eq!(state.window.available_permits(), 4);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn column_failure_wakes_admission_and_finish_drains_workers() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
        let writer = PipelinedArrowWriter::try_new(Vec::new(), schema, properties(None), 2)?;
        let wrong_field = Field::new("wrong", DataType::Utf8, false);
        let wrong_array = Arc::new(StringArray::from(vec!["not an integer"])) as ArrayRef;
        let leaf = compute_leaves(&wrong_field, &wrong_array)?
            .pop()
            .context("missing test leaf")?;
        let slot = Arc::new(Arc::clone(&writer.state.window).acquire_owned().await?);
        writer.lanes[0]
            .send(LaneMessage::Leaf { leaf, _slot: slot })
            .map_err(|_| anyhow::anyhow!("test lane unavailable"))?;
        let state = Arc::clone(&writer.state);
        assert!(
            tokio::time::timeout(Duration::from_secs(5), writer.finish())
                .await?
                .is_err()
        );
        assert!(state.failure.get().is_some());
        assert_eq!(state.window.available_permits(), 4);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failing_sink_does_not_block_the_caller_and_returns_its_error() -> Result<()> {
        struct Sink {
            fail: Arc<AtomicBool>,
            started: Option<oneshot::Sender<()>>,
            release: std::sync::mpsc::Receiver<()>,
        }
        impl Write for Sink {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.fail.load(Ordering::SeqCst) {
                    if let Some(started) = self.started.take() {
                        let _ = started.send(());
                    }
                    self.release
                        .recv_timeout(Duration::from_secs(3))
                        .map_err(|_| std::io::Error::other("async caller did not remain responsive"))?;
                    return Err(std::io::Error::other("injected pipeline sink failure"));
                }
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (schema, batches) = batches()?;
        let fail = Arc::new(AtomicBool::new(false));
        let (started, observed) = oneshot::channel();
        let (release, receiver) = std::sync::mpsc::channel();
        let sink = Sink {
            fail: Arc::clone(&fail),
            started: Some(started),
            release: receiver,
        };
        let mut writer = PipelinedArrowWriter::try_new(sink, schema, properties(None), 2)?;
        writer.write(&batches[0]).await?;
        fail.store(true, Ordering::SeqCst);
        let (result, heartbeat) = tokio::join!(writer.finish(), async {
            tokio::time::timeout(Duration::from_secs(5), observed).await??;
            release.send(()).context("caller could not release the blocking sink")
        });
        heartbeat?;
        let error = result.err().context("failing sink succeeded")?;
        assert!(format!("{error:#}").contains("injected pipeline sink failure"));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn typed_sink_failures_survive_row_group_and_footer_error_paths() -> Result<()> {
        #[derive(Debug)]
        struct RetryableSinkFailure;
        impl std::fmt::Display for RetryableSinkFailure {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("injected retryable sink failure")
            }
        }
        impl std::error::Error for RetryableSinkFailure {}

        struct Sink(Arc<AtomicBool>);
        impl Write for Sink {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.0.load(Ordering::SeqCst) {
                    return Err(std::io::Error::other(RetryableSinkFailure));
                }
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
        // An empty file fails in the footer's BufWriter flush; a large,
        // uncompressed column fails while appending the completed row group.
        for rows in [0, 8192] {
            let fail = Arc::new(AtomicBool::new(false));
            let properties = WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_max_row_group_row_count(Some(8192))
                .build();
            let mut writer =
                PipelinedArrowWriter::try_new(Sink(Arc::clone(&fail)), Arc::clone(&schema), properties, 2)?;
            fail.store(true, Ordering::SeqCst);
            if rows > 0 {
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from_iter_values(0..rows)) as ArrayRef],
                )?;
                if let Err(error) = writer.write(&batch).await {
                    assert!(error.chain().any(|cause| cause.is::<RetryableSinkFailure>()));
                }
            }
            let error = tokio::time::timeout(Duration::from_secs(5), writer.finish())
                .await?
                .err()
                .context("failing sink unexpectedly succeeded")?;
            assert!(
                error.chain().any(|cause| cause.is::<RetryableSinkFailure>()),
                "typed sink cause was lost for {rows} rows: {error:#}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_and_short_sink_writes_preserve_the_file() -> Result<()> {
        struct Sink {
            bytes: Vec<u8>,
            interrupted: bool,
        }
        impl Write for Sink {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if !self.interrupted {
                    self.interrupted = true;
                    return Err(std::io::ErrorKind::Interrupted.into());
                }
                let count = bytes.len().min(17);
                self.bytes.extend_from_slice(&bytes[..count]);
                Ok(count)
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (schema, batches) = batches()?;
        let mut sequential = ArrowWriter::try_new(Vec::new(), Arc::clone(&schema), Some(properties(Some(3))))?;
        let mut pipeline = PipelinedArrowWriter::try_new(
            Sink {
                bytes: Vec::new(),
                interrupted: false,
            },
            schema,
            properties(Some(3)),
            2,
        )?;
        for batch in &batches {
            sequential.write(batch)?;
            pipeline.write(batch).await?;
        }
        assert_eq!(pipeline.finish().await?.bytes, sequential.into_inner()?);
        Ok(())
    }

    #[test]
    fn dropping_after_the_caller_runtime_exits_releases_queued_work_and_sink() -> Result<()> {
        struct Sink {
            dropped: Option<std::sync::mpsc::Sender<()>>,
            writes: Arc<AtomicUsize>,
        }
        impl Write for Sink {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.writes.fetch_add(1, Ordering::SeqCst);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl Drop for Sink {
            fn drop(&mut self) {
                if let Some(sender) = self.dropped.take() {
                    let _ = sender.send(());
                }
            }
        }
        let (schema, batches) = batches()?;
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let (sender, dropped) = std::sync::mpsc::channel();
        let writes = Arc::new(AtomicUsize::new(0));
        let sink = Sink {
            dropped: Some(sender),
            writes: Arc::clone(&writes),
        };
        let mut writer = PipelinedArrowWriter::try_new(sink, schema, properties(Some(1)), 2)?;
        let constructor_writes = writes.load(Ordering::SeqCst);
        let state = Arc::clone(&writer.state);
        let held = runtime.block_on(Arc::clone(&state.cpu).acquire_many_owned(2))?;
        runtime.block_on(writer.write(&batches[0]))?;
        let workers = std::mem::take(&mut writer.workers);
        let assembler = writer.assembler.take().context("missing assembler")?;
        drop(runtime);
        drop(writer);
        drop(held);
        dropped.recv_timeout(Duration::from_secs(5))?;
        assert_eq!(writes.load(Ordering::SeqCst), constructor_writes);
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                for worker in workers {
                    let _ = worker.await;
                }
                let _ = assembler.await;
            })
            .await
        })?;
        assert_eq!(state.window.available_permits(), 4);
        Ok(())
    }
}
