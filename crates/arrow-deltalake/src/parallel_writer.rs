use anyhow::{Context, Result, ensure};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::future::join_all;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowRowGroupWriterFactory, ArrowWriter, compute_leaves,
};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use std::io::Write;
use std::sync::Arc;

pub(crate) struct ParallelArrowWriter<W: Write + Send + 'static> {
    file_writer: Option<SerializedFileWriter<W>>,
    row_group_factory: ArrowRowGroupWriterFactory,
    schema: SchemaRef,
    column_workers: usize,
    max_row_group_rows: Option<usize>,
    max_row_group_bytes: Option<usize>,
    row_group_index: usize,
    row_group_rows: usize,
    columns: Option<Vec<ArrowColumnWriter>>,
    failed: bool,
}

impl<W: Write + Send + 'static> ParallelArrowWriter<W> {
    pub(crate) fn try_new(
        writer: W,
        schema: SchemaRef,
        properties: WriterProperties,
        column_workers: u8,
    ) -> Result<Self> {
        ensure!(
            (1..=16).contains(&column_workers),
            "collector.runtime.parquet_column_workers must be between 1 and 16"
        );
        ensure!(
            properties.content_defined_chunking().is_none(),
            "collector.runtime.parquet_column_workers does not support Parquet content-defined chunking"
        );
        let max_row_group_rows = properties.max_row_group_row_count();
        let max_row_group_bytes = properties.max_row_group_bytes();
        ensure!(
            max_row_group_rows.is_none_or(|rows| rows > 0),
            "Parquet max row-group rows must be positive"
        );
        ensure!(
            max_row_group_bytes.is_none_or(|bytes| bytes > 0),
            "Parquet max row-group bytes must be positive"
        );

        let arrow_writer = ArrowWriter::try_new(writer, Arc::clone(&schema), Some(properties))?;
        let (file_writer, row_group_factory) = arrow_writer.into_serialized_writer()?;
        Ok(Self {
            file_writer: Some(file_writer),
            row_group_factory,
            schema,
            column_workers: usize::from(column_workers),
            max_row_group_rows,
            max_row_group_bytes,
            row_group_index: 0,
            row_group_rows: 0,
            columns: None,
            failed: false,
        })
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        ensure!(
            !self.failed,
            "parallel Parquet writer is unusable after an encode failure"
        );
        let result = self.write_inner(batch).await;
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    async fn write_inner(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut offset = 0;
        while offset < batch.num_rows() {
            let mut rows = batch.num_rows() - offset;
            if let Some(max_rows) = self.max_row_group_rows {
                let remaining_rows = max_rows
                    .checked_sub(self.row_group_rows)
                    .context("Parquet row-group row count overflow")?;
                rows = rows.min(remaining_rows);
            }
            if rows == 0 {
                self.flush().await?;
                continue;
            }

            if let Some(max_bytes) = self.max_row_group_bytes
                && self.row_group_rows > 0
            {
                let current_bytes = self.estimated_total_bytes()?;
                if current_bytes >= max_bytes {
                    self.flush().await?;
                    continue;
                }
                let average_row_bytes = current_bytes / self.row_group_rows;
                if let Some(rows_that_fit) = (max_bytes - current_bytes).checked_div(average_row_bytes) {
                    if rows > rows_that_fit {
                        if rows_that_fit == 0 {
                            self.flush().await?;
                            continue;
                        }
                        rows = rows_that_fit;
                    }
                }
            }

            if self.columns.is_none() {
                self.columns = Some(self.row_group_factory.create_column_writers(self.row_group_index)?);
            }
            let slice = batch.slice(offset, rows);
            self.encode_columns(&slice).await?;
            self.row_group_rows = self
                .row_group_rows
                .checked_add(rows)
                .context("Parquet row-group row count overflow")?;
            offset += rows;

            let reached_row_limit = self
                .max_row_group_rows
                .is_some_and(|max_rows| self.row_group_rows >= max_rows);
            let reached_byte_limit = if let Some(max_bytes) = self.max_row_group_bytes {
                self.estimated_total_bytes()? >= max_bytes
            } else {
                false
            };
            if reached_row_limit || reached_byte_limit {
                self.flush().await?;
            }
        }
        Ok(())
    }

    fn estimated_total_bytes(&self) -> Result<usize> {
        self.columns
            .as_ref()
            .context("Parquet row-group columns are unavailable")?
            .iter()
            .try_fold(0_usize, |total, column| {
                total
                    .checked_add(column.get_estimated_total_bytes())
                    .context("Parquet row-group encoded-size estimate overflow")
            })
    }

    async fn encode_columns(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut leaves = Vec::new();
        for (field, array) in self.schema.fields().iter().zip(batch.columns()) {
            leaves.extend(compute_leaves(field, array)?);
        }

        let columns = self
            .columns
            .take()
            .context("Parquet row-group columns are unavailable")?;
        ensure!(
            columns.len() == leaves.len(),
            "Parquet row-group leaf count does not match the Arrow schema"
        );
        let mut pending = columns.into_iter().zip(leaves);
        let mut encoded = Vec::new();
        loop {
            let work = pending.by_ref().take(self.column_workers).collect::<Vec<_>>();
            if work.is_empty() {
                break;
            }
            let tasks = work.into_iter().map(|(mut column, leaf)| {
                crate::delta_io::run_cpu_async(move || {
                    column.write(&leaf)?;
                    Ok::<_, anyhow::Error>(column)
                })
            });
            for result in join_all(tasks).await {
                encoded.push(result??);
            }
        }
        self.columns = Some(encoded);
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        let Some(columns) = self.columns.take() else {
            return Ok(());
        };
        let mut chunks: Vec<ArrowColumnChunk> = Vec::with_capacity(columns.len());
        let mut pending = columns.into_iter();
        loop {
            let work = pending.by_ref().take(self.column_workers).collect::<Vec<_>>();
            if work.is_empty() {
                break;
            }
            let tasks = work
                .into_iter()
                .map(|column| crate::delta_io::run_cpu_async(move || column.close()));
            for result in join_all(tasks).await {
                chunks.push(result??);
            }
        }

        let file_writer = self
            .file_writer
            .as_mut()
            .context("Parquet file writer is unavailable")?;
        let mut row_group = file_writer.next_row_group()?;
        for chunk in chunks {
            chunk.append_to_row_group(&mut row_group)?;
        }
        row_group.close()?;
        self.row_group_index = self
            .row_group_index
            .checked_add(1)
            .context("Parquet row-group index overflow")?;
        self.row_group_rows = 0;
        Ok(())
    }

    pub(crate) async fn finish(mut self) -> Result<W> {
        ensure!(
            !self.failed,
            "parallel Parquet writer is unusable after an encode failure"
        );
        self.flush().await?;
        let file_writer = self.file_writer.take().context("Parquet file writer is unavailable")?;
        crate::delta_io::run_cpu_async(move || file_writer.into_inner())
            .await?
            .context("close parallel Parquet file")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, DictionaryArray, Int32Array, Int64Array, StringArray, StructArray, UInt8Array};
    use arrow::buffer::NullBuffer;
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema, UInt8Type};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::metadata::SortingColumn;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use std::fs::File;
    use std::sync::Arc;

    fn batches() -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let nested_fields = vec![
            Arc::new(Field::new("number", DataType::Int32, true)),
            Arc::new(Field::new("label", DataType::Utf8, true)),
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("nullable", DataType::Utf8, true),
            Field::new("nested", DataType::Struct(nested_fields.clone().into()), true),
            Field::new(
                "dictionary",
                DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                true,
            ),
        ]));
        let nested = StructArray::new(
            nested_fields.into(),
            vec![
                Arc::new(Int32Array::from(vec![
                    Some(10),
                    Some(11),
                    None,
                    Some(13),
                    Some(14),
                    None,
                    Some(16),
                    Some(17),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("c"),
                    Some("d"),
                    None,
                    Some("f"),
                    Some("g"),
                    Some("h"),
                ])),
            ],
            Some(NullBuffer::from(vec![true, false, true, true, false, true, true, true])),
        );
        let dictionary = DictionaryArray::<UInt8Type>::try_new(
            UInt8Array::from(vec![Some(0), Some(1), None, Some(0), Some(1), None, Some(0), Some(1)]),
            Arc::new(StringArray::from(vec!["x", "y"])),
        )?;
        let all = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(0..8)) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("short"),
                    None,
                    Some("a longer value"),
                    Some("x"),
                    None,
                    Some("medium"),
                    Some("z"),
                    Some("another long value"),
                ])),
                Arc::new(nested),
                Arc::new(dictionary),
            ],
        )?;
        Ok((
            schema,
            vec![
                all.slice(0, 3),
                RecordBatch::new_empty(Arc::clone(&all.schema())),
                all.slice(3, 4),
                all.slice(7, 1),
            ],
        ))
    }

    fn properties() -> WriterProperties {
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .set_max_row_group_bytes(Some(96))
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

    #[tokio::test]
    async fn parallel_columns_match_sequential_footer_and_nested_round_trip() -> Result<()> {
        let (schema, batches) = batches()?;
        let properties = properties();
        let mut sequential_bytes = Vec::new();
        {
            let mut writer =
                ArrowWriter::try_new(&mut sequential_bytes, Arc::clone(&schema), Some(properties.clone()))?;
            for batch in &batches {
                writer.write(batch)?;
            }
            writer.close()?;
        }

        let mut writer = ParallelArrowWriter::try_new(Vec::new(), Arc::clone(&schema), properties, 4)?;
        for batch in &batches {
            writer.write(batch).await?;
        }
        let parallel_bytes = writer.finish().await?;
        assert_eq!(parallel_bytes, sequential_bytes);

        let expected = concat_batches(&schema, &batches)?;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("parallel.parquet");
        std::fs::write(&path, &parallel_bytes)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
        let row_groups = builder.metadata().row_groups();
        assert!(row_groups.len() > 1);
        assert!(row_groups.iter().all(|group| group.num_rows() > 0));
        let decoded = builder.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(concat_batches(&schema, &decoded)?, expected);
        Ok(())
    }

    #[tokio::test]
    async fn failed_partial_row_group_cannot_be_finalized() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
        let properties = WriterProperties::builder().set_max_row_group_row_count(Some(4)).build();
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("failed.parquet");
        let file = File::create(&path)?;
        let mut writer = ParallelArrowWriter::try_new(file, Arc::clone(&schema), properties, 2)?;
        let valid = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )?;
        writer.write(&valid).await?;

        let wrong_schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, false)]));
        let invalid = RecordBatch::try_new(
            wrong_schema,
            vec![Arc::new(StringArray::from(vec!["wrong"])) as ArrayRef],
        )?;
        assert!(writer.write(&invalid).await.is_err());
        assert!(writer.finish().await.is_err());
        assert!(ParquetRecordBatchReaderBuilder::try_new(File::open(path)?).is_err());
        Ok(())
    }
}
