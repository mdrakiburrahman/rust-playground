//! Bounded, whole-file sorting and Parquet layout controls.
//!
//! A buffer belongs to exactly one output file. Callers must flush it when
//! `try_push` returns false, then retry the batch in a new buffer. Limits bound
//! retained Arrow input; concatenation, permutation, and encoding need additional
//! proportional working memory. Oversized batches must be sliced by the caller.

use anyhow::{Context, Result, ensure};
use arrow::array::RecordBatch;
use arrow::compute::{
    SortColumn, SortOptions, concat, concat_batches, interleave_record_batch, lexsort_to_indices, take,
};
use arrow::datatypes::{DataType, SchemaRef};
use parquet::basic::Compression;
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
#[cfg(feature = "delta")]
use sha2::Digest;
use std::sync::Arc;

/// Configurable controls applied at the actual Parquet file boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// Construct layout from the shared catalog-property registry.
    /// The caller still validates column names against the table schema.
    ///
    /// # Errors
    /// Returns property type or platform-size conversion errors.
    pub fn from_properties(props: &arrowcol_props::ResolvedProps) -> Result<Self> {
        let mut sort_columns = props.string_list("arrowcol.layout.sortColumns")?.to_vec();
        if sort_columns.len() == 1 && sort_columns[0].is_empty() {
            sort_columns.clear();
        }
        ensure!(
            sort_columns.iter().all(|column| !column.is_empty()),
            "arrowcol.layout.sortColumns cannot contain empty items"
        );
        let mut bloom_columns = props.string_list("arrowcol.layout.bloomColumns")?.to_vec();
        if bloom_columns.len() == 1 && bloom_columns[0].is_empty() {
            bloom_columns.clear();
        }
        ensure!(
            bloom_columns.iter().all(|column| !column.is_empty()),
            "arrowcol.layout.bloomColumns cannot contain empty items"
        );
        let policy = Self {
            sort_columns,
            bloom_columns,
            max_rows: usize::try_from(props.unsigned("arrowcol.layout.maxRows")?)?,
            max_input_bytes: usize::try_from(props.unsigned("arrowcol.layout.maxInputSize")?)?,
            row_group_rows: usize::try_from(props.unsigned("arrowcol.layout.rowGroupRows")?)?,
            page_rows: usize::try_from(props.unsigned("arrowcol.layout.pageRows")?)?,
        };
        policy.validate_limits()?;
        Ok(policy)
    }

    #[cfg(feature = "delta")]
    pub(crate) fn config_json(&self, enabled: Option<bool>) -> serde_json::Value {
        serde_json::json!({
            "enabled": enabled,
            "sort_columns": self.sort_columns,
            "bloom_columns": self.bloom_columns,
            "max_rows": self.max_rows,
            "max_input_bytes": self.max_input_bytes,
            "row_group_rows": self.row_group_rows,
            "page_rows": self.page_rows,
        })
    }

    #[cfg(feature = "delta")]
    pub(crate) fn fingerprint(&self) -> Result<String> {
        let bytes = serde_json::to_vec(&self.config_json(None))?;
        Ok(sha2::Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect())
    }

    /// Validate names and nonzero resource limits against the supplied schema.
    ///
    /// # Errors
    /// Rejects missing or duplicate sort columns and invalid limits.
    pub fn validate(&self, schema: &SchemaRef) -> Result<()> {
        self.validate_limits()?;
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

    fn validate_limits(&self) -> Result<()> {
        ensure!(
            self.max_rows > 0 && self.max_rows <= u32::MAX as usize,
            "file row limit must fit u32"
        );
        ensure!(self.max_input_bytes > 0, "file input memory limit must be positive");
        ensure!(
            self.row_group_rows > 0 && self.page_rows > 0,
            "row group and page limits must be positive"
        );
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
            .set_max_row_group_row_count(Some(self.row_group_rows))
            .set_data_page_row_count_limit(self.page_rows)
            .set_write_batch_size(self.page_rows.min(1_024))
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_offset_index_disabled(false);
        let descriptor = parquet::arrow::ArrowSchemaConverter::new().convert(schema)?;
        let sorting = if self.sort_columns.is_empty() {
            None
        } else {
            Some(
                self.sort_columns
                    .iter()
                    .map(|name| {
                        let index = descriptor
                            .columns()
                            .iter()
                            .position(|column| column.path().parts() == [name.as_str()])
                            .with_context(|| format!("sort column {name} must be a physical scalar column"))?;
                        Ok(SortingColumn {
                            column_idx: i32::try_from(index)?,
                            descending: false,
                            nulls_first: false,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
        };
        builder = builder.set_sorting_columns(sorting);
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

    #[cfg(any(feature = "delta", test))]
    pub(crate) fn at_budget(&self) -> bool {
        self.rows >= self.policy.max_rows || self.input_bytes >= self.policy.max_input_bytes
    }

    /// A rejected batch cannot make this buffer reusable as full.
    #[cfg(any(feature = "delta", test))]
    pub(crate) fn full_for_next(&self, _batch: &RecordBatch) -> bool {
        self.at_budget()
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
        if self.batches.len() > 1
            && !self.policy.sort_columns.is_empty()
            && self
                .schema
                .fields()
                .iter()
                .all(|field| matches!(field.data_type(), DataType::Utf8 | DataType::Int16 | DataType::Int64))
        {
            let columns = self
                .policy
                .sort_columns
                .iter()
                .map(|name| {
                    let index = self.schema.index_of(name)?;
                    let arrays = self
                        .batches
                        .iter()
                        .map(|batch| batch.column(index).as_ref())
                        .collect::<Vec<_>>();
                    Ok(SortColumn {
                        values: concat(&arrays)?,
                        options: Some(SortOptions {
                            descending: false,
                            nulls_first: false,
                        }),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let indices = lexsort_to_indices(&columns, None)?;
            let locations = self
                .batches
                .iter()
                .enumerate()
                .flat_map(|(batch, values)| (0..values.num_rows()).map(move |row| (batch, row)))
                .collect::<Vec<_>>();
            let ordered = indices
                .values()
                .iter()
                .map(|index| locations[*index as usize])
                .collect::<Vec<_>>();
            let batches = self.batches.iter().collect::<Vec<_>>();
            return Ok(interleave_record_batch(&batches, &ordered)?);
        }
        let batch = concat_batches(&self.schema, &self.batches)?;
        if batch.num_rows() == 0 {
            return Ok(batch);
        }
        if self.policy.sort_columns.is_empty() {
            return Ok(batch);
        }
        let columns = self
            .policy
            .sort_columns
            .iter()
            .map(|name| {
                Ok(SortColumn {
                    values: Arc::clone(batch.column(self.schema.index_of(name)?)),
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
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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
    fn empty_sort_policy_keeps_input_order_without_sorting_metadata() -> Result<()> {
        let input = batch(vec!["z", "a"], vec![2, 1])?;
        let policy = LayoutPolicy {
            sort_columns: Vec::new(),
            bloom_columns: Vec::new(),
            ..LayoutPolicy::default()
        };
        let properties = policy.writer_properties(&input.schema())?;
        assert!(properties.sorting_columns().is_none());

        let mut buffer = FileSortBuffer::new(input.schema(), policy)?;
        assert!(buffer.try_push(&input)?);
        let output = buffer.finish()?;
        assert_eq!(output.column(0).as_ref(), input.column(0).as_ref());
        assert_eq!(output.column(1).as_ref(), input.column(1).as_ref());

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("empty-sort.parquet");
        let mut writer = ArrowWriter::try_new(std::fs::File::create(&path)?, input.schema(), Some(properties))?;
        writer.write(&output)?;
        writer.close()?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
        assert!(reader.metadata().row_group(0).sorting_columns().is_none());
        let batches = reader.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
        let readback = concat_batches(&input.schema(), &batches)?;
        assert_eq!(readback.column(0).as_ref(), input.column(0).as_ref());
        assert_eq!(readback.column(1).as_ref(), input.column(1).as_ref());
        Ok(())
    }

    #[test]
    fn accepts_campaign_caps_and_empty_layout_lists_from_properties() -> Result<()> {
        let process = std::collections::BTreeMap::from([
            ("arrowcol.layout.sortColumns".to_owned(), String::new()),
            ("arrowcol.layout.bloomColumns".to_owned(), String::new()),
            ("arrowcol.layout.maxRows".to_owned(), "1000000".to_owned()),
            ("arrowcol.layout.maxInputSize".to_owned(), "1 GiB".to_owned()),
        ]);
        let properties = arrowcol_props::resolve(&std::collections::BTreeMap::new(), &process)?;
        let policy = LayoutPolicy::from_properties(&properties)?;
        assert!(policy.sort_columns.is_empty());
        assert!(policy.bloom_columns.is_empty());
        assert_eq!(policy.max_rows, 1_000_000);
        assert_eq!(policy.max_input_bytes, 1024 * 1024 * 1024);
        policy.validate(&batch(vec!["a"], vec![1])?.schema())?;
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
    fn rejected_batches_do_not_prove_reusable_saturation() -> Result<()> {
        let rows_before_rejection = batch(vec!["x"; 6], (0..6).collect())?;
        let row_rejection = batch(vec!["y"; 5], (0..5).collect())?;
        let mut row_buffer = FileSortBuffer::new(
            rows_before_rejection.schema(),
            LayoutPolicy {
                max_rows: 10,
                max_input_bytes: 1024 * 1024,
                ..LayoutPolicy::default()
            },
        )?;
        assert!(row_buffer.try_push(&rows_before_rejection)?);
        assert!(!row_buffer.try_push(&row_rejection)?);
        let row_marked_full = row_buffer.full_for_next(&row_rejection);

        let large_value = "x".repeat(128);
        let bytes_before_rejection = batch(vec![large_value.as_str(); 2], vec![0, 1])?;
        let byte_rejection = batch(vec!["y"; 2], vec![2, 3])?;
        let large_bytes = bytes_before_rejection.get_array_memory_size();
        let next_bytes = byte_rejection.get_array_memory_size();
        assert!(large_bytes > next_bytes);
        let max_input_bytes = large_bytes + next_bytes - 1;
        let mut byte_buffer = FileSortBuffer::new(
            bytes_before_rejection.schema(),
            LayoutPolicy {
                max_rows: 100,
                max_input_bytes,
                ..LayoutPolicy::default()
            },
        )?;
        assert!(byte_buffer.try_push(&bytes_before_rejection)?);
        assert!(!byte_buffer.try_push(&byte_rejection)?);
        let byte_marked_full = byte_buffer.full_for_next(&byte_rejection);
        assert!(
            !row_marked_full && !byte_marked_full,
            "row_marked_full={row_marked_full}, byte_marked_full={byte_marked_full}"
        );
        Ok(())
    }

    #[test]
    fn writes_real_row_groups_statistics_page_indexes_and_bloom_filters() -> Result<()> {
        let a = batch(vec!["z", "a", "z", "a"], vec![4, 2, 3, 1])?;
        let policy = LayoutPolicy {
            row_group_rows: 2,
            page_rows: 1,
            bloom_columns: vec!["customer_id".into()],
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
