//! Strict, schema-checked Parquet policies shared by ingestion and maintenance.

use anyhow::{Context, Result, ensure};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowLeafColumn, ArrowRowGroupWriterFactory, ArrowWriter as SyncArrowWriter,
    compute_leaves,
};
use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, Type, ZstdLevel};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterPropertiesBuilder, WriterVersion};
use parquet::schema::types::ColumnPath;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::{self, JoinHandle};

pub const PARQUET_CONFIG_ENV: &str = "ARROWCOL_PARQUET_CONFIG";
static PROCESS_CONFIG: OnceLock<Result<ParquetWriterConfig, String>> = OnceLock::new();
static LEGACY_LZ4_WARNING: OnceLock<()> = OnceLock::new();

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    Uncompressed,
    Snappy,
    Gzip,
    Brotli,
    Lz4,
    Lz4Raw,
    Zstd,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValueEncoding {
    Plain,
    Rle,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    ByteStreamSplit,
}

impl ValueEncoding {
    fn parquet(self) -> Encoding {
        match self {
            Self::Plain => Encoding::PLAIN,
            Self::Rle => Encoding::RLE,
            Self::DeltaBinaryPacked => Encoding::DELTA_BINARY_PACKED,
            Self::DeltaLengthByteArray => Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Self::DeltaByteArray => Encoding::DELTA_BYTE_ARRAY,
            Self::ByteStreamSplit => Encoding::BYTE_STREAM_SPLIT,
        }
    }

    fn validate(self, physical: Type) -> Result<()> {
        let valid = match self {
            Self::Plain => true,
            Self::Rle => physical == Type::BOOLEAN,
            Self::DeltaBinaryPacked => matches!(physical, Type::INT32 | Type::INT64),
            Self::DeltaLengthByteArray => physical == Type::BYTE_ARRAY,
            Self::DeltaByteArray => matches!(physical, Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY),
            Self::ByteStreamSplit => matches!(
                physical,
                Type::INT32 | Type::INT64 | Type::FLOAT | Type::DOUBLE | Type::FIXED_LEN_BYTE_ARRAY
            ),
        };
        ensure!(valid, "encoding {self:?} is invalid for physical type {physical:?}");
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Statistics {
    None,
    Chunk,
    Page,
}

impl Statistics {
    fn parquet(self) -> EnabledStatistics {
        match self {
            Self::None => EnabledStatistics::None,
            Self::Chunk => EnabledStatistics::Chunk,
            Self::Page => EnabledStatistics::Page,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Version {
    V1,
    V2,
}

/// Missing fields inherit the caller's existing layout/Parquet defaults.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ColumnOptions {
    pub compression: Option<Codec>,
    pub compression_level: Option<i32>,
    pub encoding: Option<ValueEncoding>,
    pub dictionary_enabled: Option<bool>,
    pub dictionary_page_size_limit: Option<usize>,
    pub data_page_size_limit: Option<usize>,
    pub statistics: Option<Statistics>,
    pub write_page_header_statistics: Option<bool>,
    pub bloom_filter_enabled: Option<bool>,
    pub bloom_filter_fpp: Option<f64>,
    pub bloom_filter_ndv: Option<u64>,
    pub data_page_v2_compression_ratio_threshold: Option<f64>,
}

impl ColumnOptions {
    fn compression(&self) -> Result<Option<Compression>> {
        let Some(codec) = self.compression else {
            ensure!(
                self.compression_level.is_none(),
                "compression_level requires compression"
            );
            return Ok(None);
        };
        let level = self.compression_level;
        Ok(Some(match codec {
            Codec::Uncompressed | Codec::Snappy => {
                ensure!(level.is_none(), "{codec:?} does not accept a compression level");
                match codec {
                    Codec::Uncompressed => Compression::UNCOMPRESSED,
                    Codec::Snappy => Compression::SNAPPY,
                    _ => unreachable!(),
                }
            }
            Codec::Lz4 => {
                ensure!(level.is_none(), "{codec:?} does not accept a compression level");
                LEGACY_LZ4_WARNING.get_or_init(|| {
                    tracing::warn!("Parquet codec lz4 uses deprecated Hadoop framing; prefer lz4_raw");
                });
                Compression::LZ4
            }
            Codec::Lz4Raw => {
                ensure!(level.is_none(), "{codec:?} does not accept a compression level");
                Compression::LZ4_RAW
            }
            Codec::Gzip => {
                let level = level.unwrap_or(6);
                ensure!((0..=9).contains(&level), "gzip compression level must be 0..=9");
                Compression::GZIP(GzipLevel::try_new(u32::try_from(level)?)?)
            }
            Codec::Brotli => {
                let level = level.unwrap_or(1);
                ensure!((0..=11).contains(&level), "brotli compression level must be 0..=11");
                Compression::BROTLI(BrotliLevel::try_new(u32::try_from(level)?)?)
            }
            Codec::Zstd => {
                let level = level.unwrap_or(1);
                ensure!((1..=22).contains(&level), "zstd compression level must be 1..=22");
                Compression::ZSTD(ZstdLevel::try_new(level)?)
            }
        }))
    }

    fn validate(&self) -> Result<()> {
        self.compression()?;
        for (name, value) in [
            ("dictionary_page_size_limit", self.dictionary_page_size_limit),
            ("data_page_size_limit", self.data_page_size_limit),
        ] {
            if let Some(value) = value {
                ensure!(value > 0, "{name} must be positive");
            }
        }
        if let Some(fpp) = self.bloom_filter_fpp {
            ensure!(
                fpp.is_finite() && fpp > 0.0 && fpp < 1.0,
                "bloom_filter_fpp must be in (0,1)"
            );
        }
        if let Some(ndv) = self.bloom_filter_ndv {
            ensure!(ndv > 0, "bloom_filter_ndv must be positive");
        }
        if let Some(ratio) = self.data_page_v2_compression_ratio_threshold {
            ensure!(
                ratio.is_finite() && ratio > 0.0,
                "V2 compression ratio threshold must be finite and positive"
            );
        }
        ensure!(
            self.bloom_filter_enabled != Some(false)
                || (self.bloom_filter_fpp.is_none() && self.bloom_filter_ndv.is_none()),
            "disabled Bloom filter cannot specify fpp/ndv (those setters enable it)"
        );
        Ok(())
    }

    fn apply(
        &self,
        mut builder: WriterPropertiesBuilder,
        column: Option<&ColumnPath>,
    ) -> Result<WriterPropertiesBuilder> {
        macro_rules! setting {
            ($value:expr, $global:ident, $per_column:ident) => {
                if let Some(value) = $value {
                    builder = match column {
                        None => builder.$global(value),
                        Some(path) => builder.$per_column(path.clone(), value),
                    };
                }
            };
        }
        setting!(self.compression()?, set_compression, set_column_compression);
        setting!(
            self.encoding.map(ValueEncoding::parquet),
            set_encoding,
            set_column_encoding
        );
        setting!(
            self.dictionary_enabled,
            set_dictionary_enabled,
            set_column_dictionary_enabled
        );
        setting!(
            self.dictionary_page_size_limit,
            set_dictionary_page_size_limit,
            set_column_dictionary_page_size_limit
        );
        setting!(
            self.data_page_size_limit,
            set_data_page_size_limit,
            set_column_data_page_size_limit
        );
        setting!(
            self.statistics.map(Statistics::parquet),
            set_statistics_enabled,
            set_column_statistics_enabled
        );
        setting!(
            self.write_page_header_statistics,
            set_write_page_header_statistics,
            set_column_write_page_header_statistics
        );
        setting!(self.bloom_filter_fpp, set_bloom_filter_fpp, set_column_bloom_filter_fpp);
        setting!(self.bloom_filter_ndv, set_bloom_filter_ndv, set_column_bloom_filter_ndv);
        setting!(
            self.bloom_filter_enabled,
            set_bloom_filter_enabled,
            set_column_bloom_filter_enabled
        );
        setting!(
            self.data_page_v2_compression_ratio_threshold,
            set_data_page_v2_compression_ratio_threshold,
            set_column_data_page_v2_compression_ratio_threshold
        );
        Ok(builder)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SortColumn {
    pub path: String,
    #[serde(default)]
    pub descending: bool,
    #[serde(default)]
    pub nulls_first: bool,
}

/// Startup-only policy. Column paths are physical Parquet leaf paths, joined with dots.
/// Sorting metadata is accepted only when the caller proves that exact ordering.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ParquetWriterConfig {
    pub defaults: ColumnOptions,
    pub columns: BTreeMap<String, ColumnOptions>,
    pub writer_version: Option<Version>,
    pub max_row_group_rows: Option<usize>,
    pub max_row_group_bytes: Option<usize>,
    pub data_page_row_count_limit: Option<usize>,
    pub write_batch_size: Option<usize>,
    pub parallel_column_encoding_threads: usize,
    pub offset_index_enabled: Option<bool>,
    pub column_index_truncate_length: Option<usize>,
    pub statistics_truncate_length: Option<usize>,
    pub sorting_columns: Option<Vec<SortColumn>>,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            defaults: ColumnOptions::default(),
            columns: BTreeMap::new(),
            writer_version: None,
            max_row_group_rows: None,
            max_row_group_bytes: None,
            data_page_row_count_limit: None,
            write_batch_size: None,
            parallel_column_encoding_threads: 1,
            offset_index_enabled: None,
            column_index_truncate_length: None,
            statistics_truncate_length: None,
            sorting_columns: None,
        }
    }
}

impl ParquetWriterConfig {
    /// Validate numeric and codec constraints without a table schema.
    ///
    /// # Errors
    /// Rejects invalid codec levels, page limits and Bloom filter settings.
    pub fn validate(&self) -> Result<()> {
        self.defaults.validate().context("Parquet defaults")?;
        for (path, column) in &self.columns {
            ensure!(!path.is_empty(), "Parquet column path cannot be empty");
            column.validate().with_context(|| format!("Parquet column {path}"))?;
        }
        for (name, value) in [
            ("max_row_group_rows", self.max_row_group_rows),
            ("max_row_group_bytes", self.max_row_group_bytes),
            ("data_page_row_count_limit", self.data_page_row_count_limit),
            ("write_batch_size", self.write_batch_size),
            ("column_index_truncate_length", self.column_index_truncate_length),
            ("statistics_truncate_length", self.statistics_truncate_length),
        ] {
            if let Some(value) = value {
                ensure!(value > 0, "{name} must be positive");
            }
        }
        ensure!(
            (1..=32).contains(&self.parallel_column_encoding_threads),
            "parallel_column_encoding_threads must be between 1 and 32"
        );
        if self.parallel_column_encoding_threads > 1 {
            let max_row_group_bytes = self
                .max_row_group_bytes
                .context("parallel column encoding requires max_row_group_bytes")?;
            ensure!(
                max_row_group_bytes <= 64 * 1024 * 1024,
                "parallel column encoding requires max_row_group_bytes <= 67108864"
            );
        }
        Ok(())
    }

    /// Resolve requested column-encoding concurrency against the physical schema.
    ///
    /// # Errors
    /// Rejects invalid configuration or an unsupported Arrow schema.
    pub fn effective_column_encoding_threads(&self, schema: &SchemaRef) -> Result<usize> {
        self.validate()?;
        let leaf_count = ArrowSchemaConverter::new().convert(schema)?.columns().len();
        Ok(self.parallel_column_encoding_threads.min(leaf_count).max(1))
    }

    /// Resolve validated properties against the actual physical writer schema.
    ///
    /// # Errors
    /// Rejects missing columns, incompatible encodings and unproven sorting metadata.
    pub fn writer_properties(
        &self,
        schema: &SchemaRef,
        base: Option<WriterProperties>,
        verified_sort: &[SortColumn],
    ) -> Result<WriterProperties> {
        self.validate()?;
        let descriptor = ArrowSchemaConverter::new().convert(schema)?;
        let leaves = descriptor.columns();
        let leaf_index = |path: &str| -> Result<usize> {
            let mut matching = leaves
                .iter()
                .enumerate()
                .filter(|(_, leaf)| leaf.path().string() == path);
            let (index, _) = matching
                .next()
                .with_context(|| format!("unknown physical Parquet column {path}"))?;
            ensure!(
                matching.next().is_none(),
                "ambiguous physical Parquet column selector {path}"
            );
            Ok(index)
        };
        for path in self.columns.keys() {
            leaf_index(path)?;
        }
        for leaf in leaves {
            let path = leaf.path().string();
            let encoding = self
                .columns
                .get(&path)
                .and_then(|column| column.encoding)
                .or(self.defaults.encoding);
            if let Some(encoding) = encoding {
                encoding
                    .validate(leaf.physical_type())
                    .with_context(|| format!("Parquet column {path}"))?;
            }
        }
        let mut builder = self.defaults.apply(base.unwrap_or_default().into(), None)?;
        for (path, options) in &self.columns {
            builder = options.apply(builder, Some(leaves[leaf_index(path)?].path()))?;
        }
        if let Some(version) = self.writer_version {
            builder = builder.set_writer_version(match version {
                Version::V1 => WriterVersion::PARQUET_1_0,
                Version::V2 => WriterVersion::PARQUET_2_0,
            });
        }
        if let Some(rows) = self.max_row_group_rows {
            builder = builder.set_max_row_group_row_count(Some(rows));
        }
        if let Some(bytes) = self.max_row_group_bytes {
            builder = builder.set_max_row_group_bytes(Some(bytes));
        }
        if let Some(rows) = self.data_page_row_count_limit {
            builder = builder.set_data_page_row_count_limit(rows);
        }
        if let Some(size) = self.write_batch_size {
            builder = builder.set_write_batch_size(size);
        }
        if let Some(enabled) = self.offset_index_enabled {
            builder = builder.set_offset_index_disabled(!enabled);
        }
        if let Some(length) = self.column_index_truncate_length {
            builder = builder.set_column_index_truncate_length(Some(length));
        }
        if let Some(length) = self.statistics_truncate_length {
            builder = builder.set_statistics_truncate_length(Some(length));
        }
        let sorting = self.sorting_columns.as_deref().unwrap_or(verified_sort);
        ensure!(
            sorting.is_empty() || sorting == verified_sort,
            "sorting metadata must match the actual file sort order"
        );
        if !sorting.is_empty() {
            let columns = sorting
                .iter()
                .map(|sort| {
                    let index = leaf_index(&sort.path)?;
                    Ok(SortingColumn {
                        column_idx: i32::try_from(index)?,
                        descending: sort.descending,
                        nulls_first: sort.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            builder = builder.set_sorting_columns(Some(columns));
        } else {
            builder = builder.set_sorting_columns(None);
        }
        let mut properties = builder.build();
        if self
            .columns
            .values()
            .any(|options| options.bloom_filter_enabled == Some(false))
        {
            let bloom_filters = leaves
                .iter()
                .map(|leaf| {
                    let explicitly_disabled = self
                        .columns
                        .get(&leaf.path().string())
                        .is_some_and(|options| options.bloom_filter_enabled == Some(false));
                    if explicitly_disabled {
                        None
                    } else {
                        properties.bloom_filter_properties(leaf.path()).cloned()
                    }
                })
                .collect::<Vec<_>>();
            let mut builder: WriterPropertiesBuilder = properties.into();
            builder = builder.set_bloom_filter_enabled(false);
            for (leaf, bloom) in leaves.iter().zip(bloom_filters) {
                let path = leaf.path().clone();
                builder = if let Some(bloom) = bloom {
                    builder
                        .set_column_bloom_filter_enabled(path.clone(), true)
                        .set_column_bloom_filter_fpp(path.clone(), bloom.fpp)
                        .set_column_bloom_filter_ndv(path, bloom.ndv)
                } else {
                    builder.set_column_bloom_filter_enabled(path, false)
                };
            }
            properties = builder.build();
        }
        ensure!(
            self.offset_index_enabled != Some(false) || properties.offset_index_disabled(),
            "offset_index_enabled=false was overridden by global page statistics"
        );
        ensure!(
            self.offset_index_enabled != Some(false)
                || leaves
                    .iter()
                    .all(|leaf| properties.statistics_enabled(leaf.path()) != EnabledStatistics::Page),
            "offset_index_enabled=false requires statistics=none or chunk for every column; page statistics force offset indexes"
        );
        Ok(properties)
    }
}

/// Freeze process policy on first use, including invalid input.
///
/// # Errors
/// Rejects invalid UTF-8, unknown JSON fields and invalid policy constraints.
pub fn process_config() -> Result<&'static ParquetWriterConfig> {
    match PROCESS_CONFIG.get_or_init(|| {
        (|| -> Result<ParquetWriterConfig> {
            let config = match std::env::var(PARQUET_CONFIG_ENV) {
                Ok(json) => serde_json::from_str(&json).context(PARQUET_CONFIG_ENV)?,
                Err(std::env::VarError::NotPresent) => ParquetWriterConfig::default(),
                Err(error) => return Err(error.into()),
            };
            config.validate()?;
            Ok(config)
        })()
        .map_err(|error| format!("{error:#}"))
    }) {
        Ok(config) => Ok(config),
        Err(error) => anyhow::bail!("{error}"),
    }
}

/// Return sanitized startup policy for experiment provenance; null fields inherit
/// the documented layout/Parquet defaults and are resolved per physical schema.
///
/// # Errors
/// Returns invalid startup configuration or JSON serialization errors.
pub fn effective_startup_config_json() -> Result<String> {
    let value = serde_json::json!({"parquet_writer": process_config()?});
    #[cfg(feature = "delta")]
    let value = {
        let mut value = value;
        value["layout"] = crate::delta::effective_layout_startup_config()?;
        value["runtime"] = serde_json::to_value(crate::delta_io::runtime_config()?)?;
        value["commit_telemetry"] = serde_json::json!({
            "schema_version": 1,
            "scope": "new_ingestion_addfiles_after_sync",
            "clock": "system_unix_ms"
        });
        value
    };
    Ok(serde_json::to_string(&value)?)
}

pub(crate) fn sorting_from_properties(
    schema: &SchemaRef,
    properties: Option<&WriterProperties>,
) -> Result<Vec<SortColumn>> {
    let descriptor = ArrowSchemaConverter::new().convert(schema)?;
    properties
        .and_then(WriterProperties::sorting_columns)
        .map_or(&[][..], Vec::as_slice)
        .iter()
        .map(|column| {
            let index = usize::try_from(column.column_idx)?;
            let leaf = descriptor
                .columns()
                .get(index)
                .context("sorting column index outside schema")?;
            Ok(SortColumn {
                path: leaf.path().string(),
                descending: column.descending,
                nulls_first: column.nulls_first,
            })
        })
        .collect()
}

enum ColumnWorkerMessage {
    Write(
        Vec<ArrowLeafColumn>,
        SyncSender<std::result::Result<(usize, usize), String>>,
    ),
    Finish,
}

struct ColumnWorker {
    sender: SyncSender<ColumnWorkerMessage>,
    handle: Option<JoinHandle<parquet::errors::Result<Vec<ArrowColumnChunk>>>>,
    columns: usize,
}

impl Drop for ColumnWorker {
    fn drop(&mut self) {
        let _ = self.sender.send(ColumnWorkerMessage::Finish);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct ParallelRowGroup {
    workers: Vec<ColumnWorker>,
    rows: usize,
    input_bytes: u64,
    memory_bytes: usize,
    estimated_bytes: usize,
}

impl ParallelRowGroup {
    fn new(
        factory: &ArrowRowGroupWriterFactory,
        row_group_index: usize,
        requested_threads: usize,
        leaf_count: usize,
    ) -> Result<Self> {
        let writers = factory.create_column_writers(row_group_index)?;
        ensure!(
            writers.len() == leaf_count,
            "Parquet column writer count changed across row groups"
        );
        let worker_count = requested_threads.min(leaf_count);
        ensure!(
            worker_count > 1,
            "parallel row group requires at least two physical columns"
        );
        let mut writer_groups = (0..worker_count).map(|_| Vec::new()).collect::<Vec<_>>();
        for (index, writer) in writers.into_iter().enumerate() {
            writer_groups[index * worker_count / leaf_count].push(writer);
        }
        let mut workers = Vec::with_capacity(worker_count);
        for column_writers in writer_groups {
            let columns = column_writers.len();
            let (sender, receiver) = mpsc::sync_channel(0);
            let handle = thread::Builder::new()
                .name("parquet-column-encoder".into())
                .spawn(move || column_worker(column_writers, receiver))?;
            workers.push(ColumnWorker {
                sender,
                handle: Some(handle),
                columns,
            });
        }
        Ok(Self {
            workers,
            rows: 0,
            input_bytes: 0,
            memory_bytes: 0,
            estimated_bytes: 0,
        })
    }

    fn write(&mut self, leaves: Vec<ArrowLeafColumn>, rows: usize, input_bytes: u64) -> Result<()> {
        let expected = self.workers.iter().map(|worker| worker.columns).sum::<usize>();
        ensure!(
            leaves.len() == expected,
            "Arrow batch does not match Parquet physical leaf count"
        );
        let mut leaves = leaves.into_iter();
        let mut replies = Vec::with_capacity(self.workers.len());
        for worker in &self.workers {
            let columns = (0..worker.columns)
                .map(|_| leaves.next().context("missing Parquet leaf column"))
                .collect::<Result<Vec<_>>>()?;
            let (reply_tx, reply_rx) = mpsc::sync_channel(0);
            worker
                .sender
                .send(ColumnWorkerMessage::Write(columns, reply_tx))
                .map_err(|_| anyhow::anyhow!("Parquet column encoder stopped before write"))?;
            replies.push(reply_rx);
        }
        let mut memory_bytes = 0usize;
        let mut estimated_bytes = 0usize;
        for reply in replies {
            let (memory, estimated) = reply
                .recv()
                .context("Parquet column encoder stopped before acknowledging write")?
                .map_err(|error| anyhow::anyhow!("Parquet column encoding failed: {error}"))?;
            memory_bytes = memory_bytes.saturating_add(memory);
            estimated_bytes = estimated_bytes.saturating_add(estimated);
        }
        self.rows = self
            .rows
            .checked_add(rows)
            .context("Parquet row-group row count overflow")?;
        self.input_bytes = self
            .input_bytes
            .checked_add(input_bytes)
            .context("Parquet row-group input byte count overflow")?;
        self.memory_bytes = memory_bytes;
        self.estimated_bytes = estimated_bytes;
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<ArrowColumnChunk>> {
        for worker in &self.workers {
            let _ = worker.sender.send(ColumnWorkerMessage::Finish);
        }
        let workers = std::mem::take(&mut self.workers);
        let mut chunks = Vec::new();
        let mut first_error = None;
        for mut worker in workers {
            if let Some(handle) = worker.handle.take() {
                match handle.join() {
                    Ok(Ok(mut worker_chunks)) => chunks.append(&mut worker_chunks),
                    Ok(Err(error)) => {
                        first_error.get_or_insert_with(|| anyhow::Error::new(error));
                    }
                    Err(_) => {
                        first_error.get_or_insert_with(|| anyhow::anyhow!("Parquet column encoder panicked"));
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(chunks)
    }
}

fn column_worker(
    mut writers: Vec<ArrowColumnWriter>,
    receiver: Receiver<ColumnWorkerMessage>,
) -> parquet::errors::Result<Vec<ArrowColumnChunk>> {
    while let Ok(message) = receiver.recv() {
        match message {
            ColumnWorkerMessage::Write(leaves, reply) => {
                let result = writers
                    .iter_mut()
                    .zip(leaves)
                    .try_for_each(|(writer, leaf)| writer.write(&leaf));
                let stats = result.as_ref().map(|()| {
                    writers.iter().fold((0usize, 0usize), |(memory, estimate), writer| {
                        (
                            memory.saturating_add(writer.memory_size()),
                            estimate.saturating_add(writer.get_estimated_total_bytes()),
                        )
                    })
                });
                let _ = reply.send(stats.map_err(|error| error.to_string()));
                result?;
            }
            ColumnWorkerMessage::Finish => break,
        }
    }
    writers.into_iter().map(ArrowColumnWriter::close).collect()
}

fn proportional_input_bytes(total_bytes: u64, segment_rows: usize, total_rows: usize) -> Result<u64> {
    ensure!(
        total_rows > 0 && segment_rows <= total_rows,
        "invalid Parquet input byte segment"
    );
    let total_rows = u128::try_from(total_rows)?;
    let segment_rows = u128::try_from(segment_rows)?;
    let weighted = u128::from(total_bytes)
        .checked_mul(segment_rows)
        .context("Parquet segment byte estimate overflow")?;
    let bytes = weighted / total_rows + u128::from(weighted % total_rows != 0);
    u64::try_from(bytes).context("Parquet segment byte estimate exceeds u64")
}

/// Bounded low-level writer used only when column encoding concurrency is requested.
pub(crate) struct ParallelColumnWriter {
    writer: Option<parquet::file::writer::SerializedFileWriter<File>>,
    factory: ArrowRowGroupWriterFactory,
    schema: SchemaRef,
    leaf_count: usize,
    effective_threads: usize,
    max_row_group_rows: usize,
    max_row_group_bytes: usize,
    row_group_index: usize,
    current: Option<ParallelRowGroup>,
}

impl ParallelColumnWriter {
    pub(crate) fn try_new(
        file: File,
        schema: SchemaRef,
        properties: WriterProperties,
        requested_threads: usize,
        max_row_group_rows: usize,
        max_row_group_bytes: usize,
    ) -> Result<Self> {
        ensure!(
            (1..=32).contains(&requested_threads),
            "parallel column encoding threads must be between 1 and 32"
        );
        ensure!(
            max_row_group_rows > 0,
            "parallel column encoding requires a positive row-group row limit"
        );
        ensure!(
            max_row_group_bytes > 0 && max_row_group_bytes <= 64 * 1024 * 1024,
            "parallel column encoding requires a row-group byte bound <= 67108864"
        );
        let leaf_count = ArrowSchemaConverter::new().convert(&schema)?.columns().len();
        let effective_threads = requested_threads.min(leaf_count).max(1);
        ensure!(
            effective_threads > 1,
            "parallel column encoding requires at least two physical Parquet columns"
        );
        let arrow_writer = SyncArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))?;
        let (writer, factory) = arrow_writer.into_serialized_writer()?;
        Ok(Self {
            writer: Some(writer),
            factory,
            schema,
            leaf_count,
            effective_threads,
            max_row_group_rows,
            max_row_group_bytes,
            row_group_index: 0,
            current: None,
        })
    }

    pub(crate) fn write(&mut self, batch: &RecordBatch, rows: usize, input_bytes: u64) -> Result<()> {
        ensure!(
            batch.num_columns() == self.schema.fields().len(),
            "Arrow batch column count does not match Parquet schema"
        );
        ensure!(
            rows == batch.num_rows(),
            "Parquet row accounting does not match Arrow batch"
        );
        if rows == 0 {
            return Ok(());
        }
        let max_group_bytes = u64::try_from(self.max_row_group_bytes)?;
        let mut offset = 0usize;
        while offset < rows {
            let current_rows = self.current.as_ref().map_or(0, |group| group.rows);
            let current_bytes = self.current.as_ref().map_or(0, |group| group.input_bytes);
            if current_rows >= self.max_row_group_rows || current_bytes >= max_group_bytes {
                self.flush_row_group()?;
                continue;
            }
            let candidate_rows = (rows - offset).min(self.max_row_group_rows - current_rows);
            let remaining_bytes = max_group_bytes - current_bytes;
            let candidate_bytes = proportional_input_bytes(input_bytes, candidate_rows, rows)?;
            let (segment_rows, segment_bytes) = if candidate_bytes <= remaining_bytes {
                (candidate_rows, candidate_bytes)
            } else {
                let mut low = 0usize;
                let mut high = candidate_rows;
                while low < high {
                    let middle = low + (high - low).div_ceil(2);
                    let bytes = proportional_input_bytes(input_bytes, middle, rows)?;
                    if bytes <= remaining_bytes {
                        low = middle;
                    } else {
                        high = middle - 1;
                    }
                }
                if low == 0 {
                    if current_rows > 0 {
                        self.flush_row_group()?;
                        continue;
                    }
                    anyhow::bail!("single Arrow row exceeds the parallel Parquet row-group byte bound");
                }
                (low, proportional_input_bytes(input_bytes, low, rows)?)
            };
            let segment = batch.slice(offset, segment_rows);
            self.write_segment(&segment, segment_rows, segment_bytes)?;
            offset += segment_rows;
            let current = self.current.as_ref().context("Parquet row group unavailable")?;
            if current.rows >= self.max_row_group_rows
                || current.memory_bytes >= self.max_row_group_bytes
                || current.estimated_bytes >= self.max_row_group_bytes
                || current.input_bytes >= max_group_bytes
            {
                self.flush_row_group()?;
            }
        }
        Ok(())
    }

    fn write_segment(&mut self, batch: &RecordBatch, rows: usize, input_bytes: u64) -> Result<()> {
        if self.current.is_none() {
            self.current = Some(ParallelRowGroup::new(
                &self.factory,
                self.row_group_index,
                self.effective_threads,
                self.leaf_count,
            )?);
        }
        let mut leaves = Vec::with_capacity(self.leaf_count);
        for (field, column) in self.schema.fields().iter().zip(batch.columns()) {
            leaves.extend(compute_leaves(field, column)?);
        }
        self.current
            .as_mut()
            .context("Parquet row group unavailable")?
            .write(leaves, rows, input_bytes)
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        self.flush_row_group()?;
        self.writer
            .take()
            .context("parallel Parquet writer unavailable")?
            .close()?;
        Ok(())
    }

    fn flush_row_group(&mut self) -> Result<()> {
        let Some(row_group) = self.current.take() else {
            return Ok(());
        };
        let chunks = row_group.finish()?;
        let writer = self.writer.as_mut().context("parallel Parquet writer unavailable")?;
        let mut row_group_writer = writer.next_row_group()?;
        for chunk in chunks {
            chunk.append_to_row_group(&mut row_group_writer)?;
        }
        row_group_writer.close()?;
        self.row_group_index = self
            .row_group_index
            .checked_add(1)
            .context("Parquet row-group index overflow")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn synthetic_availability_batch(start: usize, rows: usize, schema: &SchemaRef) -> Result<RecordBatch> {
        let mut replica_ids = Vec::with_capacity(rows);
        let mut diagnostic_tokens = Vec::with_capacity(rows);
        let mut group_ids = Vec::with_capacity(rows);
        let mut server_names = Vec::with_capacity(rows);
        let mut endpoints = Vec::with_capacity(rows);
        let mut states = Vec::with_capacity(rows);
        let mut errors = Vec::with_capacity(rows);
        let mut details = (0..4).map(|_| Vec::with_capacity(rows)).collect::<Vec<_>>();
        let mut roles = Vec::with_capacity(rows);
        let mut local = Vec::with_capacity(rows);
        let mut updated = Vec::with_capacity(rows);
        for row in start..start + rows {
            let seed = row as u64 + 1;
            replica_ids.push(synthetic_text(seed, 36));
            diagnostic_tokens.push(synthetic_text(seed ^ 0x4f1b_bcdd, 48));
            group_ids.push(synthetic_text(seed / 1024, 36));
            server_names.push(synthetic_text(seed % 64 + 1, 48));
            endpoints.push(synthetic_text(seed % 128 + 1, 190));
            states.push(synthetic_text(seed % 6 + 1, 24));
            errors.push(synthetic_text(seed % 32 + 1, 160));
            for (index, detail) in details.iter_mut().enumerate() {
                detail.push(synthetic_text(seed % 16 + 1 + index as u64 * 32, 175));
            }
            roles.push((row % 3) as i32);
            local.push(row % 2 == 0);
            updated.push(row as i64 * 10_000);
        }
        let mut columns: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(StringArray::from(replica_ids)),
            Arc::new(StringArray::from(diagnostic_tokens)),
            Arc::new(StringArray::from(group_ids)),
            Arc::new(StringArray::from(server_names)),
            Arc::new(StringArray::from(endpoints)),
            Arc::new(StringArray::from(states)),
            Arc::new(StringArray::from(errors)),
        ];
        columns.extend(
            details
                .into_iter()
                .map(|detail| Arc::new(StringArray::from(detail)) as _),
        );
        columns.push(Arc::new(Int32Array::from(roles)));
        columns.push(Arc::new(arrow::array::BooleanArray::from(local)));
        columns.push(Arc::new(arrow::array::Int64Array::from(updated)));
        Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
    }

    fn synthetic_text(mut state: u64, length: usize) -> String {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
        let mut text = String::with_capacity(length);
        for _ in 0..length {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            text.push(ALPHABET[(state as usize) % ALPHABET.len()] as char);
        }
        text
    }

    #[cfg(target_os = "linux")]
    fn process_cpu_seconds() -> Result<f64> {
        let stat = std::fs::read_to_string("/proc/self/stat")?;
        let fields = stat
            .rsplit_once(')')
            .context("parse /proc/self/stat command name")?
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        let user_ticks = fields.get(11).context("read process user CPU ticks")?.parse::<u64>()?;
        let system_ticks = fields
            .get(12)
            .context("read process system CPU ticks")?
            .parse::<u64>()?;
        let clock_ticks = std::process::Command::new("getconf").arg("CLK_TCK").output()?;
        ensure!(clock_ticks.status.success(), "getconf CLK_TCK failed");
        let clock_ticks = String::from_utf8(clock_ticks.stdout)?.trim().parse::<f64>()?;
        Ok(user_ticks.saturating_add(system_ticks) as f64 / clock_ticks)
    }

    #[cfg(target_os = "linux")]
    fn process_peak_rss_bytes() -> Result<u64> {
        let status = std::fs::read_to_string("/proc/self/status")?;
        let line = status
            .lines()
            .find(|line| line.starts_with("VmHWM:"))
            .context("read process peak resident memory")?;
        let kilobytes = line
            .split_whitespace()
            .nth(1)
            .context("parse process peak resident memory")?
            .parse::<u64>()?;
        kilobytes
            .checked_mul(1024)
            .context("process peak resident memory overflow")
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "manual synthetic per-row-varying encode throughput benchmark"]
    async fn synthetic_availability_replica_states_encode_benchmark() -> Result<()> {
        let rows = match std::env::var("W4_BENCH_ROWS") {
            Ok(value) => value.parse::<usize>()?,
            Err(std::env::VarError::NotPresent) => 131_072,
            Err(error) => return Err(error.into()),
        };
        ensure!(rows > 0, "W4_BENCH_ROWS must be positive");
        let schema = Arc::new(Schema::new(vec![
            Field::new("replica_id", DataType::Utf8, false),
            Field::new("diagnostic_token", DataType::Utf8, false),
            Field::new("group_id", DataType::Utf8, false),
            Field::new("server_name", DataType::Utf8, false),
            Field::new("endpoint_url", DataType::Utf8, false),
            Field::new("state_desc", DataType::Utf8, false),
            Field::new("last_connect_error", DataType::Utf8, false),
            Field::new("health_details_0", DataType::Utf8, false),
            Field::new("health_details_1", DataType::Utf8, false),
            Field::new("health_details_2", DataType::Utf8, false),
            Field::new("health_details_3", DataType::Utf8, false),
            Field::new("role", DataType::Int32, false),
            Field::new("is_local", DataType::Boolean, false),
            Field::new("last_update_ms", DataType::Int64, false),
        ]));
        let mut batches = Vec::new();
        for start in (0..rows).step_by(1024) {
            batches.push(synthetic_availability_batch(start, (rows - start).min(1024), &schema)?);
        }
        let input_bytes = batches.iter().try_fold(0u64, |total, batch| {
            total
                .checked_add(crate::record_batch_logical_bytes(batch)?)
                .context("synthetic benchmark input byte count overflow")
        })?;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("availability-replica-states.parquet");
        let mut writer = crate::ParquetFileWriter::create(&path, Arc::clone(&schema))?;
        let requested_threads = writer.requested_encoding_threads();
        let effective_threads = writer.effective_encoding_threads();
        let cpu_started = process_cpu_seconds()?;
        let started = std::time::Instant::now();
        for batch in &batches {
            writer.write(batch).await?;
        }
        writer.finish().await?;
        let elapsed = started.elapsed().as_secs_f64();
        let cpu_seconds = process_cpu_seconds()? - cpu_started;
        let physical_bytes = std::fs::metadata(&path)?.len();
        let peak_rss_bytes = process_peak_rss_bytes()?;
        eprintln!(
            "W4_BENCH profile=synthetic_availability_replica_states_v1 rows={rows} arrow_bytes={input_bytes} physical_bytes={physical_bytes} peak_rss_bytes={peak_rss_bytes} arrow_parquet_ratio={:.2} arrow_bytes_per_row={:.1} wall_seconds={elapsed:.3} cpu_seconds={cpu_seconds:.3} input_mib_s={:.2} mib_s_per_core={:.2} rows_s={:.0} requested_threads={requested_threads} effective_threads={effective_threads}",
            input_bytes as f64 / physical_bytes as f64,
            input_bytes as f64 / rows as f64,
            input_bytes as f64 / elapsed / (1024.0 * 1024.0),
            input_bytes as f64 / cpu_seconds / (1024.0 * 1024.0),
            rows as f64 / elapsed,
        );
        Ok(())
    }

    #[test]
    fn literal_dotted_paths_apply_and_ambiguous_selectors_fail() -> Result<()> {
        let literal = Arc::new(Schema::new(vec![Field::new("a.b", DataType::Int32, false)]));
        let config: ParquetWriterConfig = serde_json::from_str(r#"{"columns":{"a.b":{"compression":"snappy"}}}"#)?;
        let properties = config.writer_properties(&literal, None, &[])?;
        assert_eq!(
            properties.compression(&ColumnPath::from(vec!["a.b".to_owned()])),
            Compression::SNAPPY
        );
        let ambiguous = Arc::new(Schema::new(vec![
            Field::new("a.b", DataType::Int32, false),
            Field::new(
                "a",
                DataType::Struct(vec![Field::new("b", DataType::Int32, false)].into()),
                false,
            ),
        ]));
        assert!(config.writer_properties(&ambiguous, None, &[]).is_err());
        Ok(())
    }

    #[test]
    fn disabling_offset_indexes_rejects_page_statistics_instead_of_being_ignored() -> Result<()> {
        let config: ParquetWriterConfig = serde_json::from_str(r#"{"offset_index_enabled":false}"#)?;
        assert!(config.writer_properties(&schema(), None, &[]).is_err());
        let config: ParquetWriterConfig = serde_json::from_str(
            r#"{"offset_index_enabled":false,"defaults":{"statistics":"chunk"},"columns":{"value":{"statistics":"page"}}}"#,
        )?;
        assert!(config.writer_properties(&schema(), None, &[]).is_err());
        let base = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let config: ParquetWriterConfig = serde_json::from_str(
            r#"{"offset_index_enabled":false,"columns":{"id":{"statistics":"chunk"},"value":{"statistics":"chunk"}}}"#,
        )?;
        assert!(config.writer_properties(&schema(), Some(base), &[]).is_err());
        let config: ParquetWriterConfig =
            serde_json::from_str(r#"{"offset_index_enabled":false,"defaults":{"statistics":"chunk"}}"#)?;
        let schema = schema();
        let properties = config.writer_properties(&schema, None, &[])?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )?;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("no-offset-index.parquet");
        let mut writer = ArrowWriter::try_new(std::fs::File::create(&path)?, schema, Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path)?)?;
        for column in reader.metadata().row_group(0).columns() {
            assert!(column.offset_index_offset().is_none());
        }
        Ok(())
    }

    #[test]
    fn per_column_bloom_false_overrides_global_true_in_written_file() -> Result<()> {
        let schema = schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
            ],
        )?;
        let config: ParquetWriterConfig = serde_json::from_str(
            r#"{"defaults":{"bloom_filter_enabled":true},"columns":{"id":{"bloom_filter_enabled":false}}}"#,
        )?;
        let properties = config.writer_properties(&schema, None, &[])?;
        let id = ColumnPath::from(vec!["id".to_owned()]);
        let value = ColumnPath::from(vec!["value".to_owned()]);
        assert!(properties.bloom_filter_properties(&id).is_none());
        assert!(properties.bloom_filter_properties(&value).is_some());

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("column-bloom-override.parquet");
        let mut writer = ArrowWriter::try_new(std::fs::File::create(&path)?, Arc::clone(&schema), Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
        let group = reader.metadata().row_group(0);
        assert!(group.column(0).bloom_filter_offset().is_none());
        assert!(group.column(1).bloom_filter_offset().is_some());
        Ok(())
    }

    #[test]
    fn explicit_empty_sorting_metadata_clears_the_base() -> Result<()> {
        let base = WriterProperties::builder()
            .set_sorting_columns(Some(vec![SortingColumn {
                column_idx: 0,
                descending: false,
                nulls_first: false,
            }]))
            .build();
        let config = ParquetWriterConfig {
            sorting_columns: Some(vec![]),
            ..Default::default()
        };
        let verified = vec![SortColumn {
            path: "id".into(),
            descending: false,
            nulls_first: false,
        }];
        assert!(
            config
                .writer_properties(&schema(), Some(base), &verified)?
                .sorting_columns()
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn rejects_invalid_levels_unknown_fields_and_type_mismatches() -> Result<()> {
        for json in [
            r#"{"defaults":{"compression":"gzip","compression_level":10}}"#,
            r#"{"defaults":{"compression":"zstd","compression_level":0}}"#,
            r#"{"defaults":{"compression":"snappy","compression_level":1}}"#,
            r#"{"defaults":{"bloom_filter_fpp":1.0}}"#,
            r#"{"max_row_group_rows":0}"#,
        ] {
            assert!(
                serde_json::from_str::<ParquetWriterConfig>(json)?.validate().is_err(),
                "{json}"
            );
        }
        assert!(serde_json::from_str::<ParquetWriterConfig>(r#"{"defaults":{"compression":"unknown"}}"#).is_err());
        assert!(serde_json::from_str::<ParquetWriterConfig>(r#"{"unknown":1}"#).is_err());
        assert!(serde_json::from_str::<ParquetWriterConfig>(r#"{"defaults":{"encoding":"rle_dictionary"}}"#).is_err());
        let config: ParquetWriterConfig =
            serde_json::from_str(r#"{"columns":{"value":{"encoding":"delta_binary_packed"}}}"#)?;
        assert!(config.writer_properties(&schema(), None, &[]).is_err());
        let config: ParquetWriterConfig = serde_json::from_str(r#"{"sorting_columns":[{"path":"id"}]}"#)?;
        assert!(config.writer_properties(&schema(), None, &[]).is_err());
        Ok(())
    }

    #[test]
    fn parallel_encoding_bounds_and_effective_leaf_count_are_validated() -> Result<()> {
        assert_eq!(ParquetWriterConfig::default().parallel_column_encoding_threads, 1);
        for json in [
            r#"{"parallel_column_encoding_threads":0}"#,
            r#"{"parallel_column_encoding_threads":33}"#,
            r#"{"parallel_column_encoding_threads":2}"#,
            r#"{"parallel_column_encoding_threads":2,"max_row_group_bytes":67108865}"#,
        ] {
            assert!(
                serde_json::from_str::<ParquetWriterConfig>(json)?.validate().is_err(),
                "{json}"
            );
        }
        let config: ParquetWriterConfig =
            serde_json::from_str(r#"{"parallel_column_encoding_threads":32,"max_row_group_bytes":67108864}"#)?;
        assert_eq!(config.effective_column_encoding_threads(&schema())?, 2);
        Ok(())
    }

    #[test]
    fn parallel_column_writer_preserves_rows_and_row_group_caps() -> Result<()> {
        let schema = schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "bb", "ccc", "dddd", "eeeee"])),
            ],
        )?;
        let config: ParquetWriterConfig = serde_json::from_str(
            r#"{"parallel_column_encoding_threads":2,"max_row_group_rows":3,"max_row_group_bytes":1024}"#,
        )?;
        let properties = config.writer_properties(&schema, None, &[])?;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("parallel.parquet");
        let mut writer = ParallelColumnWriter::try_new(
            std::fs::File::create(&path)?,
            Arc::clone(&schema),
            properties,
            2,
            3,
            1024,
        )?;
        writer.write(&batch, batch.num_rows(), crate::record_batch_logical_bytes(&batch)?)?;
        writer.finish()?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path)?)?;
        assert_eq!(reader.metadata().num_row_groups(), 2);
        let decoded = reader.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(arrow::compute::concat_batches(&schema, &decoded)?, batch);
        Ok(())
    }

    #[test]
    fn parallel_column_writer_splits_large_input_estimates_by_rows() -> Result<()> {
        let schema = schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "bb", "ccc", "dddd", "eeeee"])),
            ],
        )?;
        let config: ParquetWriterConfig = serde_json::from_str(
            r#"{"parallel_column_encoding_threads":2,"max_row_group_rows":10,"max_row_group_bytes":1024}"#,
        )?;
        let properties = config.writer_properties(&schema, None, &[])?;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("byte-capped.parquet");
        let mut writer = ParallelColumnWriter::try_new(
            std::fs::File::create(&path)?,
            Arc::clone(&schema),
            properties,
            2,
            10,
            1024,
        )?;
        writer.write(&batch, batch.num_rows(), 5_000)?;
        writer.finish()?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path)?)?;
        assert_eq!(reader.metadata().num_row_groups(), 5);
        let decoded = reader.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(arrow::compute::concat_batches(&schema, &decoded)?, batch);
        Ok(())
    }

    #[test]
    fn all_codecs_round_trip_rows_and_schema_with_column_overrides() -> Result<()> {
        let schema = schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["repeat"; 4])),
            ],
        )?;
        for codec in ["uncompressed", "snappy", "gzip", "brotli", "lz4", "lz4_raw", "zstd"] {
            let config: ParquetWriterConfig = serde_json::from_value(serde_json::json!({
                "defaults": {"compression": codec},
                "columns": {"id": {"encoding": "delta_binary_packed", "dictionary_enabled": false}},
                "max_row_group_rows": 2
            }))?;
            let properties = config.writer_properties(&schema, None, &[])?;
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("codec.parquet");
            let mut writer =
                ArrowWriter::try_new(std::fs::File::create(&path)?, Arc::clone(&schema), Some(properties))?;
            writer.write(&batch)?;
            writer.close()?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path)?)?.build()?;
            let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
            let actual = arrow::compute::concat_batches(&schema, &batches)?;
            assert_eq!(actual, batch, "{codec}");
        }
        Ok(())
    }
}
