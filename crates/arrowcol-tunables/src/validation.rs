use serde::Serialize;
use crate::*;
use anyhow::{Result, bail};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyBoundary {
    PerCellRuntime,
    ProcessStartup,
    RebuildOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Capability {
    pub name: &'static str,
    pub default: &'static str,
    pub boundary: ApplyBoundary,
}

#[derive(Clone, Copy, Debug, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityClass {
    WalDisabled,
    FsyncPerWrite,
    PeriodicFsync,
    LossyEviction,
}

const CAPABILITIES: &[Capability] = &[];

#[must_use]
pub fn capability_table() -> &'static [Capability] {
    CAPABILITIES
}

impl Tunables {
    /// Validates enum-specific ranges, positivity, cross-field relationships, and explicit memory budgets.
    pub fn validate(&self) -> Result<()> {
        if let Some(client) = &self.client {
            positive("client.clients_per_table", client.clients_per_table)?;
            positive("client.generator_workers", client.generator_workers)?;
            positive("client.rows_per_upload", client.rows_per_upload)?;
            positive("client.request_timeout_ms", client.request_timeout_ms)?;
            positive("client.max_attempts", client.max_attempts)?;
            positive("client.customer_cardinality", client.customer_cardinality)?;
            positive("client.payload_bytes", client.payload_bytes)?;
            if client.precache_max_bytes.zip(client.memory_budget_bytes).is_some_and(|(cache, budget)| cache > budget) {
                bail!("client.precache_max_bytes exceeds client.memory_budget_bytes");
            }
            if client.null_fraction.is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value)) {
                bail!("client.null_fraction must be finite and between 0 and 1");
            }
            if client.key_skew.is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value)) {
                bail!("client.key_skew must be finite and between 0 and 1");
            }
        }
        if let Some(transport) = &self.transport {
            positive("transport.connections", transport.connections)?;
            positive("transport.keepalive_interval_ms", transport.keepalive_interval_ms)?;
            positive("transport.request_timeout_ms", transport.request_timeout_ms)?;
            positive("transport.max_frame_bytes", transport.max_frame_bytes)?;
            positive("transport.stream_window_bytes", transport.stream_window_bytes)?;
            positive("transport.connection_window_bytes", transport.connection_window_bytes)?;
            if transport.stream_window_bytes.zip(transport.connection_window_bytes).is_some_and(|(stream, connection)| stream > connection) {
                bail!("transport.stream_window_bytes must not exceed connection_window_bytes");
            }
        }
        if let Some(router) = &self.router {
            positive("router.worker_threads", router.worker_threads)?;
            positive("router.listener_threads", router.listener_threads)?;
            positive("router.max_downstream_streams", router.max_downstream_streams)?;
            positive("router.max_upstream_streams", router.max_upstream_streams)?;
            positive("router.stream_window_bytes", router.stream_window_bytes)?;
            positive("router.connection_window_bytes", router.connection_window_bytes)?;
            positive("router.request_timeout_ms", router.request_timeout_ms)?;
            positive("router.idle_timeout_ms", router.idle_timeout_ms)?;
            positive("router.connect_timeout_ms", router.connect_timeout_ms)?;
            if router.stream_window_bytes.zip(router.connection_window_bytes).is_some_and(|(stream, connection)| stream > connection) {
                bail!("router.stream_window_bytes must not exceed connection_window_bytes");
            }
        }
        if let Some(collector) = &self.collector {
            for (name, value) in [
                ("collector.dataflow_cores", collector.dataflow_cores),
                ("collector.pdata_channel_capacity", collector.pdata_channel_capacity),
                ("collector.batch_channel_capacity", collector.batch_channel_capacity),
                ("collector.result_channel_capacity", collector.result_channel_capacity),
                ("collector.max_active_streams", collector.max_active_streams),
                ("collector.max_batches", collector.max_batches),
                ("collector.max_rows", collector.max_rows),
                ("collector.per_table_max_batches", collector.per_table_max_batches),
                ("collector.per_table_max_rows", collector.per_table_max_rows),
                ("collector.flight_max_bytes", collector.flight_max_bytes),
                ("collector.rss_limit_bytes", collector.rss_limit_bytes),
                ("collector.buffered_bytes_limit", collector.buffered_bytes_limit),
                ("collector.max_bytes", collector.max_bytes),
                ("collector.per_table_max_bytes", collector.per_table_max_bytes),
                ("collector.admission_timeout_ms", collector.admission_timeout_ms),
                ("collector.stream_timeout_ms", collector.stream_timeout_ms),
            ] { positive(name, value)?; }
            if collector.buffered_bytes_limit.zip(collector.rss_limit_bytes).is_some_and(|(buffered, rss)| buffered > rss) {
                bail!("collector.buffered_bytes_limit must not exceed rss_limit_bytes");
            }
            if collector.per_table_max_bytes.zip(collector.max_bytes).is_some_and(|(table, global)| table > global) {
                bail!("collector.per_table_max_bytes must not exceed max_bytes");
            }
            if collector.per_table_max_rows.zip(collector.max_rows).is_some_and(|(table, global)| table > global) {
                bail!("collector.per_table_max_rows must not exceed max_rows");
            }
            if collector.per_table_max_batches.zip(collector.max_batches).is_some_and(|(table, global)| table > global) {
                bail!("collector.per_table_max_batches must not exceed max_batches");
            }
            validate_flush(collector.flush.as_ref())?;
            validate_commit(collector.commit.as_ref())?;
            validate_runtime(collector.runtime.as_ref())?;
            validate_sort(collector.sort.as_ref())?;
            validate_layout(collector.layout.as_ref())?;
            validate_coalescing(collector.coalescing.as_ref())?;
        }
        if let Some(writer) = &self.parquet_writer {
            if let Some(compression) = &writer.compression { compression.validate("parquet_writer.compression")?; }
            if let Some(columns) = &writer.columns {
                let mut names = std::collections::HashSet::new();
                for column in columns {
                    if column.name.trim().is_empty() || !names.insert(column.name.to_ascii_lowercase()) {
                        bail!("parquet_writer.columns must have nonempty, unique column names");
                    }
                    if let Some(compression) = &column.compression { compression.validate("parquet_writer.columns.compression")?; }
                    if let Some(encoding) = column.encoding { validate_encoding(column.physical_type, encoding)?; }
                }
            }
            positive("parquet_writer.dictionary_page_size_bytes", writer.dictionary_page_size_bytes)?;
            positive("parquet_writer.row_group_rows", writer.row_group_rows)?;
            positive("parquet_writer.row_group_bytes", writer.row_group_bytes)?;
            positive("parquet_writer.data_page_bytes", writer.data_page_bytes)?;
            positive("parquet_writer.data_page_rows", writer.data_page_rows)?;
            if let Some(bloom) = &writer.bloom_filter {
                if bloom.false_positive_probability.is_some_and(|value| !value.is_finite() || !(0.0..1.0).contains(&value)) {
                    bail!("parquet_writer.bloom_filter.false_positive_probability must be finite and greater than 0 and less than 1");
                }
                positive("parquet_writer.bloom_filter.ndv", bloom.ndv)?;
                if bloom.enabled == Some(false) && (bloom.false_positive_probability.is_some() || bloom.ndv.is_some()) {
                    bail!("disabled Bloom filters cannot specify false_positive_probability or ndv");
                }
            }
        }
        if let Some(wal) = &self.wal {
            positive("wal.capacity_bytes", wal.capacity_bytes)?;
            positive("wal.segment_size_bytes", wal.segment_size_bytes)?;
            positive("wal.poll_interval_ms", wal.poll_interval_ms)?;
            positive("wal.retry_min_ms", wal.retry_min_ms)?;
            positive("wal.retry_max_ms", wal.retry_max_ms)?;
            positive("wal.max_retries", wal.max_retries)?;
            if wal.segment_size_bytes.zip(wal.capacity_bytes).is_some_and(|(segment, capacity)| segment > capacity) {
                bail!("wal.segment_size_bytes must not exceed wal.capacity_bytes");
            }
            if wal.retry_min_ms.zip(wal.retry_max_ms).is_some_and(|(minimum, maximum)| minimum > maximum) {
                bail!("wal.retry_min_ms must not exceed retry_max_ms");
            }
        }
        if let Some(maintenance) = &self.maintenance {
            positive("maintenance.interval_ms", maintenance.interval_ms)?;
            positive("maintenance.timeout_ms", maintenance.timeout_ms)?;
            positive("maintenance.target_file_bytes", maintenance.target_file_bytes)?;
            positive("maintenance.small_file_bytes", maintenance.small_file_bytes)?;
            positive("maintenance.files_per_bin", maintenance.files_per_bin)?;
            positive("maintenance.bins_per_commit", maintenance.bins_per_commit)?;
            positive("maintenance.max_decoded_bytes_per_bin", maintenance.max_decoded_bytes_per_bin)?;
            positive("maintenance.max_decoded_batch_bytes", maintenance.max_decoded_batch_bytes)?;
            positive("maintenance.max_row_group_bytes", maintenance.max_row_group_bytes)?;
            positive("maintenance.table_concurrency", maintenance.table_concurrency)?;
            positive("maintenance.max_table_concurrency", maintenance.max_table_concurrency)?;
            positive("maintenance.max_partitions", maintenance.max_partitions)?;
            if maintenance.small_file_bytes.zip(maintenance.target_file_bytes).is_some_and(|(small, target)| small > target) {
                bail!("maintenance.small_file_bytes must not exceed target_file_bytes");
            }
            if maintenance.table_concurrency.zip(maintenance.max_table_concurrency).is_some_and(|(active, maximum)| active > maximum) {
                bail!("maintenance.table_concurrency must not exceed max_table_concurrency");
            }
            if maintenance.max_decoded_batch_bytes.zip(maintenance.max_decoded_bytes_per_bin).is_some_and(|(batch, bin)| batch > bin) {
                bail!("maintenance.max_decoded_batch_bytes must not exceed max_decoded_bytes_per_bin");
            }
        }
        Ok(())
    }

    /// Serializes only explicitly requested settings in stable key order.
    pub fn canonical_json(&self) -> Result<String> {
        let value = serde_json::to_value(self)?;
        Ok(canonicalize(value).to_string())
    }

    /// Labels the configured WAL durability class; full settings remain in the canonical JSON.
    #[must_use]
    pub fn durability_class(&self) -> DurabilityClass {
        let Some(wal) = &self.wal else { return DurabilityClass::FsyncPerWrite; };
        if wal.enabled == Some(false) { return DurabilityClass::WalDisabled; }
        if wal.overflow_policy == Some(WalOverflowPolicy::EvictOldest) { return DurabilityClass::LossyEviction; }
        if wal.fsync_interval_ms.is_some_and(|interval| interval > 0) { return DurabilityClass::PeriodicFsync; }
        DurabilityClass::FsyncPerWrite
    }

    /// Projects configured scalar values to component-prefixed environment variables.
    pub fn environment(&self) -> Result<BTreeMap<String, String>> {
        let value = serde_json::to_value(self)?;
        let mut output = BTreeMap::new();
        let Some(sections) = value.as_object() else { return Ok(output); };
        for (section, section_value) in sections {
            let prefix = match section.as_str() {
                "client" => "ARROWCOL_CLIENT",
                "transport" => "ARROWCOL_CLIENT_TRANSPORT",
                "router" => "ARROWCOL_ROUTER",
                "collector" => "ARROWCOL_COLLECTOR",
                "parquet_writer" => "ARROWCOL_PARQUET",
                "arrow_ipc" => "ARROWCOL_COLLECTOR_IPC",
                "wal" => "ARROWCOL_WAL",
                "maintenance" => "ARROWCOL_MAINT",
                _ => continue,
            };
            flatten_environment(section_value, prefix, &mut output);
        }
        Ok(output)
    }
}

impl Compression {
    fn validate(&self, field: &str) -> Result<()> {
        match (self.codec, self.level) {
            (CompressionCodec::Gzip, Some(level)) if level <= 9 => {}
            (CompressionCodec::Brotli, Some(level)) if level <= 11 => {}
            (CompressionCodec::Zstd, Some(level)) if (1..=22).contains(&level) => {}
            (CompressionCodec::Gzip | CompressionCodec::Brotli | CompressionCodec::Zstd, None) => {}
            (CompressionCodec::Gzip, Some(_)) => bail!("{field}.level must be between 0 and 9 for gzip"),
            (CompressionCodec::Brotli, Some(_)) => bail!("{field}.level must be between 0 and 11 for brotli"),
            (CompressionCodec::Zstd, Some(_)) => bail!("{field}.level must be between 1 and 22 for zstd"),
            (_, Some(_)) => bail!("{field}.level is only supported for gzip, brotli, or zstd"),
        }
        Ok(())
    }
}

fn validate_encoding(physical_type: PhysicalType, encoding: Encoding) -> Result<()> {
    let supported = match encoding {
        Encoding::Plain => true,
        Encoding::Rle => physical_type == PhysicalType::Boolean,
        Encoding::DeltaBinaryPacked => matches!(physical_type, PhysicalType::Int32 | PhysicalType::Int64),
        Encoding::ByteStreamSplit => matches!(physical_type, PhysicalType::Int32 | PhysicalType::Int64 | PhysicalType::Float | PhysicalType::Double | PhysicalType::FixedLenByteArray),
        Encoding::DeltaLengthByteArray | Encoding::DeltaByteArray => physical_type == PhysicalType::ByteArray,
    };
    if !supported { bail!("encoding {encoding:?} is not supported for physical type {physical_type:?}"); }
    Ok(())
}

fn validate_flush(flush: Option<&FlushTunables>) -> Result<()> {
    let Some(flush) = flush else { return Ok(()); };
    positive("collector.flush.max_bytes", flush.max_bytes)?;
    positive("collector.flush.max_rows", flush.max_rows)?;
    positive("collector.flush.max_batches", flush.max_batches)?;
    positive("collector.flush.max_age_ms", flush.max_age_ms)?;
    positive("collector.flush.concurrency", flush.concurrency)
}

fn validate_commit(commit: Option<&CommitTunables>) -> Result<()> {
    let Some(commit) = commit else { return Ok(()); };
    positive("collector.commit.window_ms", commit.window_ms)?;
    positive("collector.commit.max_files_per_commit", commit.max_files_per_commit)?;
    positive("collector.commit.max_pending_files", commit.max_pending_files)?;
    if commit.max_files_per_commit.zip(commit.max_pending_files).is_some_and(|(batch, pending)| batch > pending) {
        bail!("collector.commit.max_files_per_commit must not exceed max_pending_files");
    }
    Ok(())
}

fn validate_runtime(runtime: Option<&RuntimeTunables>) -> Result<()> {
    let Some(runtime) = runtime else { return Ok(()); };
    positive("collector.runtime.async_workers", runtime.async_workers)?;
    positive("collector.runtime.blocking_workers", runtime.blocking_workers)?;
    positive("collector.runtime.upload_concurrency", runtime.upload_concurrency)?;
    positive("collector.runtime.kernel_task_concurrency", runtime.kernel_task_concurrency)?;
    positive("collector.runtime.read_ahead", runtime.read_ahead)?;
    positive("collector.runtime.batch_rows", runtime.batch_rows)
}

fn validate_sort(sort: Option<&SortTunables>) -> Result<()> {
    let Some(sort) = sort else { return Ok(()); };
    positive("collector.sort.max_rows", sort.max_rows)?;
    positive("collector.sort.max_bytes", sort.max_bytes)?;
    positive("collector.sort.concurrency", sort.concurrency)
}

fn validate_layout(layout: Option<&LayoutTunables>) -> Result<()> {
    let Some(layout) = layout else { return Ok(()); };
    positive("collector.layout.row_group_rows", layout.row_group_rows)?;
    positive("collector.layout.page_rows", layout.page_rows)?;
    positive("collector.layout.max_row_group_bytes", layout.max_row_group_bytes)?;
    positive("collector.layout.max_page_bytes", layout.max_page_bytes)
}

fn validate_coalescing(coalescing: Option<&CoalescingTunables>) -> Result<()> {
    let Some(coalescing) = coalescing else { return Ok(()); };
    positive("collector.coalescing.target_rows", coalescing.target_rows)?;
    positive("collector.coalescing.target_bytes", coalescing.target_bytes)?;
    positive("collector.coalescing.max_age_ms", coalescing.max_age_ms)?;
    positive("collector.coalescing.max_pending_batches", coalescing.max_pending_batches)
}

fn positive<T>(field: &str, value: Option<T>) -> Result<()>
where
    T: Copy + PartialOrd + From<u8>,
{
    if value.is_some_and(|number| number < T::from(1)) {
        bail!("{field} must be greater than zero");
    }
    Ok(())
}

fn canonicalize(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        Value::Object(object) => {
            let sorted = object.into_iter().map(|(key, value)| (key, canonicalize(value))).collect::<BTreeMap<_, _>>();
            Value::Object(Map::from_iter(sorted))
        }
        scalar => scalar,
    }
}

fn flatten_environment(value: &Value, prefix: &str, output: &mut BTreeMap<String, String>) {
    let Some(fields) = value.as_object() else { return; };
    for (name, value) in fields {
        if value.is_null() { continue; }
        let key = format!("{prefix}_{}", name.to_ascii_uppercase());
        match value {
            Value::Object(_) => flatten_environment(value, &key, output),
            Value::Array(_) => { output.insert(key, value.to_string()); }
            _ => { output.insert(key, value.as_str().map_or_else(|| value.to_string(), str::to_owned)); }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_enum_parsing_rejects_unknown_codec_and_encoding() {
        let error = serde_json::from_str::<Compression>(r#"{"codec":"brotliish","level":5}"#).unwrap_err();
        assert!(error.to_string().contains("unknown variant"));
        let error = serde_json::from_str::<ColumnOverride>(r#"{"name":"x","physical_type":"int64","encoding":"dictionary","compression":null}"#).unwrap_err();
        assert!(error.to_string().contains("unknown variant"));
    }

    #[test]
    fn validates_compression_bounds_bloom_probability_and_physical_encoding() -> Result<()> {
        let valid = Tunables {
            parquet_writer: Some(ParquetWriterTunables {
                compression: Some(Compression { codec: CompressionCodec::Zstd, level: Some(22) }),
                ..Default::default()
            }),
            ..Default::default()
        };
        valid.validate()?;
        let invalid = Tunables {
            parquet_writer: Some(ParquetWriterTunables {
                compression: Some(Compression { codec: CompressionCodec::Zstd, level: Some(23) }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("zstd"));
        let invalid = Tunables {
            parquet_writer: Some(ParquetWriterTunables {
                bloom_filter: Some(BloomFilterTunables { enabled: Some(true), false_positive_probability: Some(f64::NAN), ndv: None }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("false_positive_probability"));
        let invalid = Tunables {
            parquet_writer: Some(ParquetWriterTunables {
                columns: Some(vec![ColumnOverride { name: "x".to_owned(), physical_type: PhysicalType::Boolean, compression: None, encoding: Some(Encoding::DeltaBinaryPacked) }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("physical type"));
        Ok(())
    }

    #[test]
    fn validates_memory_limits_and_cross_field_bounds() {
        let invalid = Tunables {
            client: Some(ClientTunables { precache_max_bytes: Some(20), memory_budget_bytes: Some(10), ..Default::default() }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("memory_budget"));
        let invalid = Tunables {
            collector: Some(CollectorTunables { buffered_bytes_limit: Some(9), rss_limit_bytes: Some(8), ..Default::default() }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("rss_limit"));
        let invalid = Tunables {
            wal: Some(WalTunables { retry_min_ms: Some(20), retry_max_ms: Some(10), ..Default::default() }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("retry_min_ms"));
    }

    #[test]
    fn canonical_json_environment_and_durability_are_deterministic() -> Result<()> {
        let config: Tunables = serde_json::from_str(r#"{"wal":{"fsync_interval_ms":15},"client":{"rows_per_upload":4}}"#)?;
        assert_eq!(config.canonical_json()?, r#"{"client":{"rows_per_upload":4},"wal":{"fsync_interval_ms":15}}"#);
        assert_eq!(config.durability_class(), DurabilityClass::PeriodicFsync);
        let env = config.environment()?;
        assert_eq!(env["ARROWCOL_CLIENT_ROWS_PER_UPLOAD"], "4");
        assert_eq!(env["ARROWCOL_WAL_FSYNC_INTERVAL_MS"], "15");
        assert!(config.validate().is_ok());
        Ok(())
    }

    #[test]
    fn unknown_fields_are_rejected_in_each_section() {
        let error = serde_json::from_str::<Tunables>(r#"{"client":{"rows_per_upload":1,"compression":"gzip"}}"#).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }
}
