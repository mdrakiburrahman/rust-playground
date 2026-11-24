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
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
use serde::{Deserialize, Serialize};
#[cfg(feature = "delta")]
use sha2::Digest;
use std::sync::Arc;

/// Collector execution strategy; maintenance always uses its existing file sorter.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", try_from = "String")]
pub enum SortExecution {
    #[default]
    Concatenate,
    StreamUnordered,
    VerifyOrder,
}

impl std::str::FromStr for SortExecution {
    type Err = anyhow::Error;
    fn from_str(raw: &str) -> Result<Self> {
        match raw {
            "concatenate" => Ok(Self::Concatenate),
            "stream_unordered" => Ok(Self::StreamUnordered),
            "verify_order" => Ok(Self::VerifyOrder),
            _ => anyhow::bail!("unknown collector layout sort_execution: {raw}"),
        }
    }
}

impl TryFrom<String> for SortExecution {
    type Error = anyhow::Error;
    fn try_from(raw: String) -> Result<Self> {
        raw.parse()
    }
}

impl SortExecution {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Concatenate => "concatenate",
            Self::StreamUnordered => "stream_unordered",
            Self::VerifyOrder => "verify_order",
        }
    }

    /// # Errors
    /// Rejects a streaming strategy incompatible with the resolved file layout.
    pub fn validate(self, enabled: bool, has_keys: bool) -> Result<()> {
        match self {
            Self::Concatenate => (),
            Self::StreamUnordered => ensure!(
                enabled && !has_keys,
                "stream_unordered requires enabled layout with empty sort_columns"
            ),
            Self::VerifyOrder => ensure!(
                enabled && has_keys,
                "verify_order requires enabled layout with non-empty sort_columns"
            ),
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NullPlacement {
    First,
    Last,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SortKeyOptions {
    pub column: String,
    pub direction: SortDirection,
    pub nulls: NullPlacement,
}

impl SortKeyOptions {
    pub fn to_property(&self) -> String {
        let direction = match self.direction {
            SortDirection::Ascending => "ascending",
            SortDirection::Descending => "descending",
        };
        let nulls = match self.nulls {
            NullPlacement::First => "first",
            NullPlacement::Last => "last",
        };
        format!("{}:{direction}:{nulls}", self.column)
    }

    /// Parse one catalog sort option.
    ///
    /// # Errors
    /// Rejects malformed keys, directions, and null placement.
    pub fn from_property(value: &str) -> Result<Self> {
        let parts = value.split(':').collect::<Vec<_>>();
        ensure!(parts.len() == 3 && !parts[0].is_empty(), "invalid sort option {value}");
        let direction = match parts[1] {
            "ascending" => SortDirection::Ascending,
            "descending" => SortDirection::Descending,
            _ => anyhow::bail!("invalid sort direction"),
        };
        let nulls = match parts[2] {
            "first" => NullPlacement::First,
            "last" => NullPlacement::Last,
            _ => anyhow::bail!("invalid null placement"),
        };
        Ok(Self {
            column: parts[0].into(),
            direction,
            nulls,
        })
    }
}

/// Configurable controls applied at the actual Parquet file boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayoutPolicy {
    pub sort_columns: Vec<String>,
    pub sort_options: Option<Vec<SortKeyOptions>>,
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
            sort_options: None,
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
        let options = props.string_list("arrowcol.layout.sortOptions")?;
        let sort_options = if options.is_empty() {
            None
        } else {
            Some(
                options
                    .iter()
                    .map(|value| SortKeyOptions::from_property(value))
                    .collect::<Result<Vec<_>>>()?,
            )
        };
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
            sort_options,
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
        let mut value = serde_json::json!({
            "enabled": enabled,
            "sort_columns": self.sort_columns,
            "bloom_columns": self.bloom_columns,
            "max_rows": self.max_rows,
            "max_input_bytes": self.max_input_bytes,
            "row_group_rows": self.row_group_rows,
            "page_rows": self.page_rows,
        });
        if let Some(options) = &self.sort_options {
            value["sort_options"] = serde_json::json!(options);
        }
        value
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
        if let Some(options) = &self.sort_options {
            ensure!(!options.is_empty(), "sort options cannot be empty");
            ensure!(
                options.len() == self.sort_columns.len()
                    && options
                        .iter()
                        .zip(&self.sort_columns)
                        .all(|(option, column)| option.column == *column),
                "sort options must match sort columns exactly in order"
            );
        }
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
                    .enumerate()
                    .map(|(position, name)| {
                        let index = descriptor
                            .columns()
                            .iter()
                            .position(|column| column.path().parts() == [name.as_str()])
                            .with_context(|| format!("sort column {name} must be a physical scalar column"))?;
                        Ok(SortingColumn {
                            column_idx: i32::try_from(index)?,
                            descending: self
                                .sort_options
                                .as_ref()
                                .is_some_and(|options| options[position].direction == SortDirection::Descending),
                            nulls_first: self
                                .sort_options
                                .as_ref()
                                .is_some_and(|options| options[position].nulls == NullPlacement::First),
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
        if !self.try_admit(batch)? {
            return Ok(false);
        }
        if batch.num_rows() > 0 {
            self.batches.push(batch.clone());
        }
        Ok(true)
    }

    fn try_admit(&mut self, batch: &RecordBatch) -> Result<bool> {
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
        let batch = concat_batches(&self.schema, &self.batches)?;
        if batch.num_rows() == 0 {
            return Ok(batch);
        }
        if self.policy.sort_columns.is_empty() {
            return Ok(batch);
        }
        let columns = self.sort_columns(&batch)?;
        let indices = lexsort_to_indices(&columns, None)?;
        let arrays = batch
            .columns()
            .iter()
            .map(|column| take(column, &indices, None))
            .collect::<arrow::error::Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(self.schema, arrays)?)
    }
    fn sort_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        self.policy
            .sort_columns
            .iter()
            .enumerate()
            .map(|(position, name)| {
                Ok(SortColumn {
                    values: Arc::clone(batch.column(self.schema.index_of(name)?)),
                    options: Some(SortOptions {
                        descending: self
                            .policy
                            .sort_options
                            .as_ref()
                            .is_some_and(|options| options[position].direction == SortDirection::Descending),
                        nulls_first: self
                            .policy
                            .sort_options
                            .as_ref()
                            .is_some_and(|options| options[position].nulls == NullPlacement::First),
                    }),
                })
            })
            .collect()
    }
}

#[cfg(feature = "delta")]
pub(crate) struct PublicationSorter {
    buffer: FileSortBuffer,
    mode: SortExecution,
    last_keys: Vec<arrow::array::ArrayRef>,
}

#[cfg(feature = "delta")]
impl PublicationSorter {
    pub(crate) fn new(schema: SchemaRef, policy: LayoutPolicy, mode: SortExecution) -> Result<Self> {
        mode.validate(true, !policy.sort_columns.is_empty())?;
        Ok(Self {
            buffer: FileSortBuffer::new(schema, policy)?,
            mode,
            last_keys: Vec::new(),
        })
    }

    /// Returns true when the admitted batch should be encoded immediately.
    pub(crate) fn admit(&mut self, batch: &RecordBatch) -> Result<bool> {
        let buffered = self.mode == SortExecution::Concatenate;
        let admitted = if buffered {
            self.buffer.try_push(batch)?
        } else {
            self.buffer.try_admit(batch)?
        };
        ensure!(
            admitted,
            "publication exceeds sorted-file budget; split upstream into independently identified publications"
        );
        if self.mode == SortExecution::VerifyOrder && batch.num_rows() > 0 {
            let keys = self.buffer.sort_columns(batch)?;
            for (left, right) in self.last_keys.iter().zip(&keys) {
                let cmp = arrow::array::make_comparator(
                    left.as_ref(),
                    right.values.as_ref(),
                    right.options.unwrap_or_default(),
                )?;
                match cmp(0, 0) {
                    std::cmp::Ordering::Less => break,
                    std::cmp::Ordering::Equal => (),
                    std::cmp::Ordering::Greater => anyhow::bail!("verify_order: unsorted batch boundary"),
                }
            }
            let cmp = arrow::compute::LexicographicalComparator::try_new(&keys)?;
            ensure!(
                (1..batch.num_rows()).all(|i| !cmp.compare(i - 1, i).is_gt()),
                "verify_order: unsorted rows within batch"
            );
            self.last_keys = keys
                .iter()
                .map(|key| key.values.slice(batch.num_rows() - 1, 1))
                .collect();
        }
        Ok(!buffered)
    }

    pub(crate) fn into_buffer(self) -> Option<FileSortBuffer> {
        (self.mode == SortExecution::Concatenate).then_some(self.buffer)
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
        let defaults = policy.writer_properties(&a.schema())?;
        assert_eq!(
            defaults.sorting_columns(),
            Some(&vec![
                SortingColumn {
                    column_idx: 0,
                    descending: false,
                    nulls_first: false
                },
                SortingColumn {
                    column_idx: 1,
                    descending: false,
                    nulls_first: false
                },
            ])
        );
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
    fn mixed_sort_options_match_parquet_rows_and_each_row_group_footer() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));
        let make = |customers, values| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(customers)),
                    Arc::new(Int32Array::from(values)),
                ],
            )
        };
        let a = make(
            vec![Some("a"), None, Some("b"), Some("a")],
            vec![Some(1), Some(4), None, None],
        )?;
        let b = make(
            vec![Some("b"), Some("a"), None, Some("b")],
            vec![Some(2), Some(3), None, Some(1)],
        )?;
        let policy = LayoutPolicy {
            sort_columns: vec!["customer_id".into(), "value".into()],
            sort_options: Some(vec![
                SortKeyOptions {
                    column: "customer_id".into(),
                    direction: SortDirection::Descending,
                    nulls: NullPlacement::First,
                },
                SortKeyOptions {
                    column: "value".into(),
                    direction: SortDirection::Ascending,
                    nulls: NullPlacement::Last,
                },
            ]),
            bloom_columns: Vec::new(),
            row_group_rows: 3,
            ..LayoutPolicy::default()
        };
        let properties = policy.writer_properties(&schema)?;
        let mut buffer = FileSortBuffer::new(Arc::clone(&schema), policy.clone())?;
        assert!(buffer.try_push(&a)?);
        assert!(buffer.try_push(&b)?);
        let sorted = buffer.finish()?;
        #[cfg(feature = "delta")]
        {
            let verify = || PublicationSorter::new(Arc::clone(&schema), policy.clone(), SortExecution::VerifyOrder);
            let mut verifier = verify()?;
            // The reference sort includes primary/secondary nulls and mixed directions.
            for i in 0..sorted.num_rows() {
                assert!(verifier.admit(&sorted.slice(i, 1))?);
                assert!(verifier.admit(&sorted.slice(i, 0))?);
            }
            assert!(verify()?.admit(&sorted)?);
            let equal = sorted.slice(0, 1);
            let repeated = concat_batches(&schema, [&equal, &equal])?;
            let mut verifier = verify()?;
            assert!(verifier.admit(&repeated)?);
            assert!(verifier.admit(&equal)?);
            assert!(verify()?.admit(&a).is_err());
            for i in 1..sorted.num_rows() {
                let mut verifier = verify()?;
                assert!(verifier.admit(&sorted.slice(i, 1))?);
                assert!(verifier.admit(&sorted.slice(i, 0))?);
                assert!(verifier.admit(&sorted.slice(i - 1, 1)).is_err());
            }
        }
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("mixed.parquet");
        let mut writer = ArrowWriter::try_new(std::fs::File::create(&file)?, Arc::clone(&schema), Some(properties))?;
        writer.write(&sorted)?;
        writer.close()?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(file)?)?;
        assert_eq!(reader.metadata().num_row_groups(), 3);
        let expected = vec![
            SortingColumn {
                column_idx: 0,
                descending: true,
                nulls_first: true,
            },
            SortingColumn {
                column_idx: 1,
                descending: false,
                nulls_first: false,
            },
        ];
        for group in reader.metadata().row_groups() {
            assert_eq!(group.sorting_columns(), Some(&expected));
        }
        let output = concat_batches(&schema, &reader.build()?.collect::<std::result::Result<Vec<_>, _>>()?)?;
        assert_eq!(
            output.column(0).as_ref(),
            &StringArray::from(vec![
                None,
                None,
                Some("b"),
                Some("b"),
                Some("b"),
                Some("a"),
                Some("a"),
                Some("a")
            ])
        );
        assert_eq!(
            output.column(1).as_ref(),
            &Int32Array::from(vec![Some(4), None, Some(1), Some(2), None, Some(1), Some(3), None])
        );
        Ok(())
    }

    #[test]
    fn rejects_invalid_and_mismatched_sort_options() -> Result<()> {
        use std::collections::BTreeMap;
        for value in [
            "customer_id:sideways:last",
            "customer_id:ascending:middle",
            "value:ascending:last",
            "customer_id:ascending:last,value:descending:first",
        ] {
            let process = BTreeMap::from([("arrowcol.layout.sortOptions".to_owned(), value.to_owned())]);
            assert!(arrowcol_props::resolve(&BTreeMap::new(), &process).is_err(), "{value}");
        }
        let table = BTreeMap::from([("arrowcol.layout.sortColumns".to_owned(), "value".to_owned())]);
        let process = BTreeMap::from([(
            "arrowcol.layout.sortOptions".to_owned(),
            "customer_id:descending:first".to_owned(),
        )]);
        assert!(arrowcol_props::resolve(&table, &process).is_err());
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
