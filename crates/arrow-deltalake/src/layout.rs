//! Bounded, whole-file sorting and Parquet layout controls.
//!
//! A buffer belongs to exactly one output file. Callers must flush it when
//! `try_push` returns false, then retry the batch in a new buffer. Limits bound
//! retained Arrow input; concatenation, permutation, and encoding need additional
//! proportional working memory. Oversized batches must be sliced by the caller.

use anyhow::{Context, Result, ensure};
use arrow::array::RecordBatch;
use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::SchemaRef;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

/// Configurable controls applied at the actual Parquet file boundary.
#[derive(Clone, Debug)]
pub struct LayoutPolicy {
    pub sort_columns: Vec<String>,
    pub max_rows: usize,
    pub max_input_bytes: usize,
    pub row_group_rows: usize,
    pub page_rows: usize,
    pub bloom_columns: Vec<String>,
}

impl Default for LayoutPolicy {
    fn default() -> Self {
        Self {
            sort_columns: vec!["customer_id".into()],
            max_rows: 262_144,
            max_input_bytes: 64 * 1024 * 1024,
            row_group_rows: 16_384,
            page_rows: 2_048,
            bloom_columns: vec!["customer_id".into()],
        }
    }
}

impl LayoutPolicy {
    /// Validate names and nonzero resource limits against the supplied schema.
    ///
    /// # Errors
    /// Rejects missing or duplicate sort columns and invalid limits.
    pub fn validate(&self, schema: &SchemaRef) -> Result<()> {
        ensure!(
            self.max_rows > 0 && self.max_rows <= u32::MAX as usize,
            "file row limit must fit u32"
        );
        ensure!(self.max_input_bytes > 0, "file input memory limit must be positive");
        ensure!(
            self.row_group_rows > 0 && self.page_rows > 0,
            "row group and page limits must be positive"
        );
        ensure!(!self.sort_columns.is_empty(), "at least one sort column is required");
        let mut names = std::collections::BTreeSet::new();
        for column in &self.sort_columns {
            schema
                .index_of(column)
                .with_context(|| format!("unknown sort column {column}"))?;
            ensure!(names.insert(column), "duplicate sort column {column}");
        }
        for column in &self.bloom_columns {
            schema
                .index_of(column)
                .with_context(|| format!("unknown Bloom filter column {column}"))?;
        }
        Ok(())
    }

    /// Enable statistics/page indexes and explicitly selected Bloom filters.
    ///
    /// # Errors
    /// Returns policy/schema validation errors.
    pub fn writer_properties(&self, schema: &SchemaRef) -> Result<WriterProperties> {
        self.validate(schema)?;
        let mut builder = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_max_row_group_size(self.row_group_rows)
            .set_data_page_row_count_limit(self.page_rows)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_offset_index_disabled(false);
        for column in &self.bloom_columns {
            builder = builder
                .set_column_bloom_filter_enabled(ColumnPath::from(column.as_str()), true)
                .set_column_bloom_filter_fpp(ColumnPath::from(column.as_str()), 0.01)
                .set_column_bloom_filter_ndv(ColumnPath::from(column.as_str()), self.row_group_rows as u64);
        }
        Ok(builder.build())
    }
}

/// A backpressured buffer for one sorted output file, not one input batch.
pub struct FileSortBuffer {
    schema: SchemaRef,
    policy: LayoutPolicy,
    batches: Vec<RecordBatch>,
    rows: usize,
    input_bytes: usize,
}

impl FileSortBuffer {
    /// Create an empty, schema-bound file buffer.
    ///
    /// # Errors
    /// Returns policy/schema validation errors.
    pub fn new(schema: SchemaRef, policy: LayoutPolicy) -> Result<Self> {
        policy.validate(&schema)?;
        Ok(Self {
            schema,
            policy,
            batches: Vec::new(),
            rows: 0,
            input_bytes: 0,
        })
    }

    /// Retain the batch, or return false without retaining it if this file is full.
    ///
    /// # Errors
    /// Rejects mismatched schemas or a single batch exceeding the file budget.
    pub fn try_push(&mut self, batch: &RecordBatch) -> Result<bool> {
        ensure!(batch.schema() == self.schema, "file-sort schema changed");
        let bytes = batch.get_array_memory_size();
        ensure!(
            batch.num_rows() <= self.policy.max_rows && bytes <= self.policy.max_input_bytes,
            "batch exceeds sorted-file input budget; slice it before retrying"
        );
        let rows = self
            .rows
            .checked_add(batch.num_rows())
            .context("file row count overflow")?;
        let input_bytes = self
            .input_bytes
            .checked_add(bytes)
            .context("file memory count overflow")?;
        if rows > self.policy.max_rows || input_bytes > self.policy.max_input_bytes {
            return Ok(false);
        }
        if batch.num_rows() > 0 {
            self.batches.push(batch.clone());
            self.rows = rows;
            self.input_bytes = input_bytes;
        }
        Ok(true)
    }

    #[must_use]
    pub const fn rows(&self) -> usize {
        self.rows
    }

    /// Sort every retained row together, including rows from different batches.
    ///
    /// # Errors
    /// Returns Arrow concatenation, ordering, or permutation errors.
    pub fn finish(self) -> Result<RecordBatch> {
        let batch = concat_batches(&self.schema, &self.batches)?;
        if batch.num_rows() == 0 {
            return Ok(batch);
        }
        let columns = self
            .policy
            .sort_columns
            .iter()
            .map(|name| {
                Ok(SortColumn {
                    values: batch.column(self.schema.index_of(name)?).clone(),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let indices = lexsort_to_indices(&columns, None)?;
        let arrays = batch
            .columns()
            .iter()
            .map(|column| take(column, &indices, None))
            .collect::<arrow::error::Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(self.schema, arrays)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::sync::Arc;

    fn batch(customers: Vec<&str>, values: Vec<i32>) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("customer_id", DataType::Utf8, false),
                Field::new("value", DataType::Int32, false),
            ])),
            vec![
                Arc::new(StringArray::from(customers)),
                Arc::new(Int32Array::from(values)),
            ],
        )?)
    }

    #[test]
    fn sorts_across_batches_and_preserves_rows_and_schema() -> Result<()> {
        let a = batch(vec!["z", "a"], vec![4, 2])?;
        let b = batch(vec!["z", "a"], vec![3, 1])?;
        let policy = LayoutPolicy {
            sort_columns: vec!["customer_id".into(), "value".into()],
            ..LayoutPolicy::default()
        };
        let mut buffer = FileSortBuffer::new(a.schema(), policy)?;
        assert!(buffer.try_push(&a)?);
        assert!(buffer.try_push(&b)?);
        let sorted = buffer.finish()?;
        assert_eq!(sorted.schema(), a.schema());
        assert_eq!(sorted.column(0).as_ref(), &StringArray::from(vec!["a", "a", "z", "z"]));
        assert_eq!(sorted.column(1).as_ref(), &Int32Array::from(vec![1, 2, 3, 4]));
        Ok(())
    }

    #[test]
    fn budget_backpressures_without_losing_or_retaining_rejected_rows() -> Result<()> {
        let a = batch(vec!["z", "a"], vec![2, 1])?;
        let policy = LayoutPolicy {
            max_rows: 3,
            ..LayoutPolicy::default()
        };
        let mut buffer = FileSortBuffer::new(a.schema(), policy)?;
        assert!(buffer.try_push(&a)?);
        assert!(!buffer.try_push(&a)?);
        assert_eq!(buffer.rows(), 2);
        assert_eq!(buffer.finish()?.num_rows(), 2);
        let mut buffer = FileSortBuffer::new(
            a.schema(),
            LayoutPolicy {
                max_input_bytes: 1,
                ..LayoutPolicy::default()
            },
        )?;
        assert!(buffer.try_push(&a).is_err());
        Ok(())
    }

    #[test]
    fn writes_real_row_groups_statistics_page_indexes_and_bloom_filters() -> Result<()> {
        let a = batch(vec!["z", "a", "z", "a"], vec![4, 2, 3, 1])?;
        let policy = LayoutPolicy {
            row_group_rows: 2,
            page_rows: 1,
            ..LayoutPolicy::default()
        };
        let properties = policy.writer_properties(&a.schema())?;
        let mut buffer = FileSortBuffer::new(a.schema(), policy)?;
        assert!(buffer.try_push(&a)?);
        let sorted = buffer.finish()?;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("sorted.parquet");
        let mut writer = ArrowWriter::try_new(std::fs::File::create(&path)?, a.schema(), Some(properties))?;
        writer.write(&sorted)?;
        writer.close()?;
        let reader = SerializedFileReader::new(std::fs::File::open(path)?)?;
        let groups = reader.metadata().row_groups();
        assert_eq!(groups.len(), 2);
        for group in groups {
            let column = group.column(0);
            assert!(column.statistics().is_some());
            assert!(column.column_index_offset().is_some());
            assert!(column.offset_index_offset().is_some());
            assert!(column.bloom_filter_offset().is_some());
        }
        let first = groups[0].column(0).statistics().context("missing statistics")?;
        assert_eq!(first.min_bytes_opt(), Some(b"a".as_slice()));
        assert_eq!(first.max_bytes_opt(), Some(b"a".as_slice()));
        let last = groups[1].column(0).statistics().context("missing statistics")?;
        assert_eq!(last.min_bytes_opt(), Some(b"z".as_slice()));
        assert_eq!(last.max_bytes_opt(), Some(b"z".as_slice()));
        Ok(())
    }
}
