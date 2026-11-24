//! Strict, schema-checked Parquet policies shared by ingestion and maintenance.

use anyhow::{Context, Result, ensure};
use arrow::datatypes::SchemaRef;
use parquet::arrow::ArrowSchemaConverter;
use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, Type, ZstdLevel};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterPropertiesBuilder, WriterVersion};
use parquet::schema::types::ColumnPath;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::OnceLock;

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
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ParquetWriterConfig {
    pub defaults: ColumnOptions,
    pub columns: BTreeMap<String, ColumnOptions>,
    pub writer_version: Option<Version>,
    pub max_row_group_rows: Option<usize>,
    pub max_row_group_bytes: Option<usize>,
    pub data_page_row_count_limit: Option<usize>,
    pub write_batch_size: Option<usize>,
    pub offset_index_enabled: Option<bool>,
    pub column_index_truncate_length: Option<usize>,
    pub statistics_truncate_length: Option<usize>,
    pub sorting_columns: Option<Vec<SortColumn>>,
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
        Ok(())
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
        let descriptor = ArrowSchemaConverter::new()
            .with_coerce_types(base.as_ref().is_some_and(WriterProperties::coerce_types))
            .convert(schema)?;
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
        let has_base = base.is_some();
        let mut builder = self.defaults.apply(base.unwrap_or_default().into(), None)?;
        // A base can already contain leaf-specific settings (notably layout Bloom filters).
        // Global setters alone cannot override those. Seed configured leaves as well so
        // a partial per-leaf Bloom request inherits the requested default fpp and ndv.
        for leaf in leaves {
            if has_base || self.columns.contains_key(&leaf.path().string()) {
                builder = self.defaults.apply(builder, Some(leaf.path()))?;
            }
        }
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
        self.verify_effective(&descriptor, &properties)?;
        crate::footer::stamp(&descriptor, properties)
    }

    fn verify_effective(
        &self,
        descriptor: &parquet::schema::types::SchemaDescriptor,
        props: &WriterProperties,
    ) -> Result<()> {
        macro_rules! check {
            ($field:ident, $actual:expr) => {
                if let Some(expected) = self.$field {
                    ensure!(
                        Some(expected) == $actual,
                        "requested Parquet {} differs from writer properties",
                        stringify!($field)
                    );
                }
            };
        }
        check!(
            writer_version,
            Some(match props.writer_version() {
                WriterVersion::PARQUET_1_0 => Version::V1,
                WriterVersion::PARQUET_2_0 => Version::V2,
            })
        );
        check!(max_row_group_rows, props.max_row_group_row_count());
        check!(max_row_group_bytes, props.max_row_group_bytes());
        check!(data_page_row_count_limit, Some(props.data_page_row_count_limit()));
        check!(write_batch_size, Some(props.write_batch_size()));
        check!(offset_index_enabled, Some(!props.offset_index_disabled()));
        check!(column_index_truncate_length, props.column_index_truncate_length());
        check!(statistics_truncate_length, props.statistics_truncate_length());
        let empty = ColumnOptions::default();
        if self.columns.is_empty() && self.defaults == empty {
            return Ok(());
        }
        for leaf in descriptor.columns() {
            let path = leaf.path();
            let column = self.columns.get(&path.string()).unwrap_or(&empty);
            macro_rules! requested {
                ($field:ident) => {
                    column.$field.or(self.defaults.$field)
                };
            }
            macro_rules! verify {
                ($field:ident, $expected:expr, $actual:expr) => {
                    if let Some(expected) = $expected {
                        let actual = $actual;
                        ensure!(Some(expected) == actual, "requested Parquet {} for {path} differs from writer properties: expected {expected:?}, found {actual:?}", stringify!($field));
                    }
                };
            }
            let codec = if column.compression.is_some() {
                column
            } else {
                &self.defaults
            };
            verify!(compression, codec.compression()?, Some(props.compression(path)));
            verify!(
                encoding,
                requested!(encoding).map(ValueEncoding::parquet),
                props.encoding(path)
            );
            verify!(
                dictionary_enabled,
                requested!(dictionary_enabled),
                Some(props.dictionary_enabled(path))
            );
            verify!(
                statistics,
                requested!(statistics).map(Statistics::parquet),
                Some(props.statistics_enabled(path))
            );
            verify!(
                write_page_header_statistics,
                requested!(write_page_header_statistics),
                Some(props.write_page_header_statistics(path))
            );
            verify!(
                data_page_size_limit,
                requested!(data_page_size_limit),
                Some(props.column_data_page_size_limit(path))
            );
            verify!(
                dictionary_page_size_limit,
                requested!(dictionary_page_size_limit),
                Some(props.column_dictionary_page_size_limit(path))
            );
            verify!(
                data_page_v2_compression_ratio_threshold,
                requested!(data_page_v2_compression_ratio_threshold),
                Some(props.column_data_page_v2_compression_ratio_threshold(path))
            );
            let bloom = props.bloom_filter_properties(path);
            let enabled = |options: &ColumnOptions| {
                options.bloom_filter_enabled.or_else(|| {
                    (options.bloom_filter_fpp.is_some() || options.bloom_filter_ndv.is_some()).then_some(true)
                })
            };
            let expected = enabled(column).or_else(|| enabled(&self.defaults));
            verify!(bloom_filter_enabled, expected, Some(bloom.is_some()));
            if expected != Some(false) {
                verify!(bloom_filter_fpp, requested!(bloom_filter_fpp), bloom.map(|b| b.fpp));
                verify!(bloom_filter_ndv, requested!(bloom_filter_ndv), bloom.map(|b| b.ndv));
            }
        }
        Ok(())
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

    #[test]
    fn writer_gate_rejects_wrong_level_even_when_codec_matches() -> Result<()> {
        let config: ParquetWriterConfig = serde_json::from_str(
            r#"{"defaults":{"compression":"zstd","compression_level":7},"columns":{"id":{"compression":"snappy"}}}"#,
        )?;
        let schema = schema();
        let descriptor = ArrowSchemaConverter::new().convert(&schema)?;
        let good = config.writer_properties(&schema, None, &[])?;
        let bad = WriterPropertiesBuilder::from(good)
            .set_column_compression(ColumnPath::from("value"), Compression::ZSTD(ZstdLevel::try_new(1)?))
            .build();
        let error = config
            .verify_effective(&descriptor, &bad)
            .err()
            .context("wrong compression level passed verification")?
            .to_string();
        assert!(error.contains("compression") && error.contains("value"), "{error}");
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
