use crate::*;
use anyhow::{Result, bail};
use serde::Serialize;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComponentRole {
    Client,
    Router,
    Collector,
    Maintenance,
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
            let cardinality = client.cardinality.unwrap_or(1_024);
            let memory = client.max_memory_bytes.unwrap_or(536_870_912);
            let connections = client.connections_per_worker.unwrap_or(1);
            let streams = client.streams_per_worker.unwrap_or(0);
            positive("client.cardinality", Some(cardinality))?;
            positive("client.max_memory_bytes", Some(memory))?;
            if client.workers.is_some_and(|workers| workers > 1_024) {
                bail!("client.workers must be 0 (all available cores) or at most 1024");
            }
            if client.string_width.is_some_and(|width| width > 65_536) {
                bail!("client.string_width must be at most 65536");
            }
            if client.corpus_rows.is_some_and(|rows| rows > 0) && client.corpus_bytes.is_some_and(|bytes| bytes > 0) {
                bail!("client.corpus_rows and client.corpus_bytes are mutually exclusive");
            }
            if client.corpus_bytes.is_some_and(|bytes| bytes > memory) {
                bail!("client.corpus_bytes exceeds client.max_memory_bytes");
            }
            if !(1..=256).contains(&connections) {
                bail!("client.connections_per_worker must be between 1 and 256");
            }
            if streams > 1_024 {
                bail!("client.streams_per_worker must be 0 (legacy concurrency) or at most 1024");
            }
            for (field, value) in [
                ("client.null_fraction", client.null_fraction),
                ("client.skew", client.skew),
                ("client.entropy", client.entropy),
            ] {
                if value.is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value)) {
                    bail!("{field} must be finite and between 0 and 1");
                }
            }
        }
        if let Some(transport) = &self.transport {
            let message_bytes = transport.max_message_bytes.unwrap_or(16_777_216);
            let flight_bytes = transport.max_flight_data_bytes.unwrap_or(2_097_152);
            if message_bytes == 0 || message_bytes > i32::MAX as u64 {
                bail!("transport.max_message_bytes must be between 1 and i32::MAX");
            }
            if flight_bytes < 1_024 || flight_bytes > message_bytes {
                bail!("transport.max_flight_data_bytes must be at least 1024 and no greater than max_message_bytes");
            }
            let frame_bytes = transport.max_frame_bytes.unwrap_or(16_384);
            if !(16_384..=16_777_215).contains(&frame_bytes) || u64::from(frame_bytes) > message_bytes {
                bail!("transport.max_frame_bytes must be between 16384 and 16777215 and not exceed max_message_bytes");
            }
            let header_list_bytes = transport.max_header_list_bytes.unwrap_or(16_384);
            positive("transport.max_header_list_bytes", Some(header_list_bytes))?;
            if transport.runtime_threads.is_some_and(|threads| threads > 1_024) {
                bail!("transport.runtime_threads must be 0 (automatic) or at most 1024");
            }
            let stream_window = transport.stream_window_bytes.unwrap_or(1_048_576);
            let connection_window = transport.connection_window_bytes.unwrap_or(16_777_216);
            validate_h2_window("transport.stream_window_bytes", stream_window)?;
            validate_h2_window("transport.connection_window_bytes", connection_window)?;
            if stream_window > connection_window {
                bail!("transport.stream_window_bytes must not exceed connection_window_bytes");
            }
            for (field, value) in [
                (
                    "transport.keepalive_interval_ms",
                    transport.keepalive_interval_ms.unwrap_or(10_000),
                ),
                (
                    "transport.keepalive_timeout_ms",
                    transport.keepalive_timeout_ms.unwrap_or(20_000),
                ),
                (
                    "transport.connect_timeout_ms",
                    transport.connect_timeout_ms.unwrap_or(30_000),
                ),
                (
                    "transport.request_timeout_ms",
                    transport.request_timeout_ms.unwrap_or(30_000),
                ),
                (
                    "transport.tcp_keepalive_ms",
                    transport.tcp_keepalive_ms.unwrap_or(60_000),
                ),
                ("transport.retry_backoff_ms", transport.retry_backoff_ms.unwrap_or(100)),
            ] {
                if value > 86_400_000 || (field.ends_with("timeout_ms") && value == 0) {
                    bail!("{field} must be within the supported 24-hour duration bound");
                }
            }
            for (field, value) in [
                (
                    "transport.endpoint_concurrency_limit",
                    transport.endpoint_concurrency_limit.unwrap_or(256),
                ),
                (
                    "transport.endpoint_buffer_size",
                    transport.endpoint_buffer_size.unwrap_or(1_024),
                ),
            ] {
                positive(field, Some(value))?;
            }
            if transport.connect_retries.is_some_and(|retries| retries > 100) {
                bail!("transport.connect_retries must be between 0 and 100");
            }
            if transport.dictionary_handling.is_some() {
                bail!(
                    "transport.dictionary_handling has no effect for the current DMV workload, which has no dictionary-encoded fields"
                );
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
            let stream_window = router.stream_window_bytes.unwrap_or(1_048_576);
            let connection_window = router.connection_window_bytes.unwrap_or(16_777_216);
            validate_h2_window("router.stream_window_bytes", stream_window)?;
            validate_h2_window("router.connection_window_bytes", connection_window)?;
            if stream_window > connection_window {
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
            ] {
                positive(name, value)?;
            }
            for (name, value) in [
                ("collector.flight_max_bytes", collector.flight_max_bytes),
                ("collector.rss_limit_bytes", collector.rss_limit_bytes),
                ("collector.buffered_bytes_limit", collector.buffered_bytes_limit),
                ("collector.max_bytes", collector.max_bytes),
                ("collector.per_table_max_bytes", collector.per_table_max_bytes),
                ("collector.admission_timeout_ms", collector.admission_timeout_ms),
                ("collector.stream_timeout_ms", collector.stream_timeout_ms),
            ] {
                positive(name, value)?;
            }
            let buffered_limit = collector.buffered_bytes_limit.unwrap_or(2_147_483_648);
            let rss_limit = collector.rss_limit_bytes.unwrap_or(4_294_967_296);
            if buffered_limit > rss_limit {
                bail!("collector.buffered_bytes_limit must not exceed rss_limit_bytes");
            }
            if collector.per_table_max_bytes.unwrap_or(536_870_912) > collector.max_bytes.unwrap_or(2_147_483_648) {
                bail!("collector.per_table_max_bytes must not exceed max_bytes");
            }
            if collector.per_table_max_rows.unwrap_or(2_000_000) > collector.max_rows.unwrap_or(10_000_000) {
                bail!("collector.per_table_max_rows must not exceed max_rows");
            }
            if collector.per_table_max_batches.unwrap_or(1_024) > collector.max_batches.unwrap_or(4_096) {
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
            if let Some(compression) = &writer.compression {
                compression.validate("parquet_writer.compression")?;
            }
            if let Some(columns) = &writer.columns {
                let mut names = std::collections::HashSet::new();
                for column in columns {
                    if column.name.trim().is_empty() || !names.insert(column.name.to_ascii_lowercase()) {
                        bail!("parquet_writer.columns must have nonempty, unique column names");
                    }
                    if let Some(compression) = &column.compression {
                        compression.validate("parquet_writer.columns.compression")?;
                    }
                    if let Some(encoding) = column.encoding {
                        validate_encoding(column.physical_type, encoding)?;
                    }
                }
            }
            positive(
                "parquet_writer.dictionary_page_size_bytes",
                writer.dictionary_page_size_bytes,
            )?;
            positive("parquet_writer.row_group_rows", writer.row_group_rows)?;
            positive("parquet_writer.row_group_bytes", writer.row_group_bytes)?;
            positive("parquet_writer.data_page_bytes", writer.data_page_bytes)?;
            positive("parquet_writer.data_page_rows", writer.data_page_rows)?;
            if let Some(bloom) = &writer.bloom_filter {
                if bloom
                    .false_positive_probability
                    .is_some_and(|value| !value.is_finite() || value <= 0.0 || value >= 1.0)
                {
                    bail!(
                        "parquet_writer.bloom_filter.false_positive_probability must be finite and greater than 0 and less than 1"
                    );
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
            if wal
                .segment_size_bytes
                .zip(wal.capacity_bytes)
                .is_some_and(|(segment, capacity)| segment > capacity)
            {
                bail!("wal.segment_size_bytes must not exceed wal.capacity_bytes");
            }
            if wal.retry_min_ms.unwrap_or(250) > wal.retry_max_ms.unwrap_or(2_000) {
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
            positive(
                "maintenance.max_decoded_bytes_per_bin",
                maintenance.max_decoded_bytes_per_bin,
            )?;
            positive(
                "maintenance.max_decoded_batch_bytes",
                maintenance.max_decoded_batch_bytes,
            )?;
            positive("maintenance.max_row_group_bytes", maintenance.max_row_group_bytes)?;
            positive("maintenance.table_concurrency", maintenance.table_concurrency)?;
            positive("maintenance.max_table_concurrency", maintenance.max_table_concurrency)?;
            positive("maintenance.max_partitions", maintenance.max_partitions)?;
            if maintenance.small_file_bytes.unwrap_or(67_108_864) > maintenance.target_file_bytes.unwrap_or(134_217_728)
            {
                bail!("maintenance.small_file_bytes must not exceed target_file_bytes");
            }
            if maintenance.table_concurrency.unwrap_or(1) > maintenance.max_table_concurrency.unwrap_or(16) {
                bail!("maintenance.table_concurrency must not exceed max_table_concurrency");
            }
            if maintenance.max_decoded_batch_bytes.unwrap_or(67_108_864)
                > maintenance.max_decoded_bytes_per_bin.unwrap_or(1_073_741_824)
            {
                bail!("maintenance.max_decoded_batch_bytes must not exceed max_decoded_bytes_per_bin");
            }
        }
        Ok(())
    }

    /// Checks per-column declarations against the actual normalized schema before writer construction.
    pub fn validate_column_types(&self, actual: &BTreeMap<String, PhysicalType>) -> Result<()> {
        self.validate()?;
        if let Some(columns) = self.parquet_writer.as_ref().and_then(|writer| writer.columns.as_ref()) {
            for column in columns {
                let name = column.name.to_ascii_lowercase();
                let physical_type = actual
                    .get(&name)
                    .ok_or_else(|| anyhow::anyhow!("Parquet override references unknown column {:?}", column.name))?;
                if *physical_type != column.physical_type {
                    bail!(
                        "Parquet override for {:?} declares {:?}, actual normalized physical type is {:?}",
                        column.name,
                        column.physical_type,
                        physical_type
                    );
                }
                if let Some(encoding) = column.encoding {
                    validate_encoding(*physical_type, encoding)?;
                }
            }
        }
        Ok(())
    }

    /// Serializes only explicitly requested settings in stable key order.
    pub fn canonical_json(&self) -> Result<String> {
        let value = serde_json::to_value(self)?;
        Ok(canonicalize(value).to_string())
    }

    /// Merges explicit overrides over component-resolved defaults and serializes the effective JSON canonically.
    /// `resolved_defaults` must be a JSON object returned by the component config/default resolver.
    pub fn canonical_effective_json(&self, resolved_defaults: &Value) -> Result<String> {
        self.validate()?;
        let mut effective = resolved_defaults.clone();
        if !effective.is_object() {
            bail!("resolved component defaults must be a JSON object");
        }
        deep_merge(&mut effective, serde_json::to_value(self)?);
        Ok(canonicalize(effective).to_string())
    }

    /// Returns only environment overrides for the process role receiving them.
    pub fn environment_for(&self, role: ComponentRole) -> Result<BTreeMap<String, String>> {
        let variables = self.environment()?;
        let selected = variables
            .into_iter()
            .filter(|(name, _)| match role {
                ComponentRole::Client => name.starts_with("ARROWCOL_CLIENT_"),
                ComponentRole::Router => name.starts_with("ARROWCOL_ROUTER_"),
                ComponentRole::Collector => {
                    name.starts_with("ARROWCOL_COLLECTOR_")
                        || name.starts_with("ARROWCOL_PARQUET_")
                        || name.starts_with("ARROWCOL_WAL_")
                }
                ComponentRole::Maintenance => name.starts_with("ARROWCOL_MAINT_"),
            })
            .collect();
        Ok(selected)
    }

    /// Labels the configured WAL durability class; full settings remain in the canonical JSON.
    #[must_use]
    pub fn durability_class(&self) -> DurabilityClass {
        let Some(wal) = &self.wal else {
            return DurabilityClass::FsyncPerWrite;
        };
        if wal.enabled == Some(false) {
            return DurabilityClass::WalDisabled;
        }
        if wal.overflow_policy == Some(WalOverflowPolicy::EvictOldest) {
            return DurabilityClass::LossyEviction;
        }
        if wal.fsync_interval_ms.is_some_and(|interval| interval > 0) {
            return DurabilityClass::PeriodicFsync;
        }
        DurabilityClass::FsyncPerWrite
    }

    /// Projects configured scalar values to component-prefixed environment variables.
    pub fn environment(&self) -> Result<BTreeMap<String, String>> {
        let value = serde_json::to_value(self)?;
        let mut output = BTreeMap::new();
        let Some(sections) = value.as_object() else {
            return Ok(output);
        };
        for (section, section_value) in sections {
            let prefix = match section.as_str() {
                "client" => "ARROWCOL_CLIENT",
                "transport" => "ARROWCOL_CLIENT",
                "router" => "ARROWCOL_ROUTER",
                "collector" => "ARROWCOL_COLLECTOR",
                "parquet_writer" => "ARROWCOL_PARQUET",
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
            (
                CompressionCodec::Uncompressed
                | CompressionCodec::Snappy
                | CompressionCodec::Lz4Raw
                | CompressionCodec::Lz4,
                None,
            ) => {}
            (CompressionCodec::Gzip, Some(_)) => bail!("{field}.level must be between 0 and 9 for gzip"),
            (CompressionCodec::Brotli, Some(_)) => bail!("{field}.level must be between 0 and 11 for brotli"),
            (CompressionCodec::Zstd, Some(_)) => bail!("{field}.level must be between 1 and 22 for zstd"),
            (_, Some(_)) => bail!("{field}.level is only supported for gzip, brotli, or zstd"),
        }
        Ok(())
    }
}

fn validate_h2_window(field: &str, window: u32) -> Result<()> {
    if window == 0 || window > 2_147_483_647 {
        bail!("{field} must be between 1 and 2147483647 bytes");
    }
    Ok(())
}

fn validate_encoding(physical_type: PhysicalType, encoding: Encoding) -> Result<()> {
    let supported = match encoding {
        Encoding::Plain => true,
        Encoding::Rle => physical_type == PhysicalType::Boolean,
        Encoding::DeltaBinaryPacked => matches!(physical_type, PhysicalType::Int32 | PhysicalType::Int64),
        Encoding::ByteStreamSplit => matches!(
            physical_type,
            PhysicalType::Int32
                | PhysicalType::Int64
                | PhysicalType::Float
                | PhysicalType::Double
                | PhysicalType::FixedLenByteArray
        ),
        Encoding::DeltaLengthByteArray | Encoding::DeltaByteArray => physical_type == PhysicalType::ByteArray,
    };
    if !supported {
        bail!("encoding {encoding:?} is not supported for physical type {physical_type:?}");
    }
    Ok(())
}

fn validate_flush(flush: Option<&FlushTunables>) -> Result<()> {
    let Some(flush) = flush else {
        return Ok(());
    };
    positive("collector.flush.max_bytes", flush.max_bytes)?;
    positive("collector.flush.max_rows", flush.max_rows)?;
    positive("collector.flush.max_batches", flush.max_batches)?;
    positive("collector.flush.max_age_ms", flush.max_age_ms)?;
    positive("collector.flush.concurrency", flush.concurrency)
}

fn validate_commit(commit: Option<&CommitTunables>) -> Result<()> {
    let Some(commit) = commit else {
        return Ok(());
    };
    positive("collector.commit.window_ms", commit.window_ms)?;
    positive("collector.commit.max_files_per_commit", commit.max_files_per_commit)?;
    positive("collector.commit.max_pending_files", commit.max_pending_files)?;
    if commit.max_files_per_commit.unwrap_or(64) > commit.max_pending_files.unwrap_or(256) {
        bail!("collector.commit.max_files_per_commit must not exceed max_pending_files");
    }
    Ok(())
}

fn validate_runtime(runtime: Option<&RuntimeTunables>) -> Result<()> {
    let Some(runtime) = runtime else {
        return Ok(());
    };
    positive("collector.runtime.async_workers", runtime.async_workers)?;
    positive("collector.runtime.blocking_workers", runtime.blocking_workers)?;
    positive("collector.runtime.upload_concurrency", runtime.upload_concurrency)?;
    positive(
        "collector.runtime.kernel_task_concurrency",
        runtime.kernel_task_concurrency,
    )?;
    positive("collector.runtime.read_ahead", runtime.read_ahead)?;
    positive("collector.runtime.batch_rows", runtime.batch_rows)
}

fn validate_sort(sort: Option<&SortTunables>) -> Result<()> {
    let Some(sort) = sort else {
        return Ok(());
    };
    positive("collector.sort.max_rows", sort.max_rows)?;
    positive("collector.sort.max_bytes", sort.max_bytes)?;
    positive("collector.sort.concurrency", sort.concurrency)
}

fn validate_layout(layout: Option<&LayoutTunables>) -> Result<()> {
    let Some(layout) = layout else {
        return Ok(());
    };
    positive("collector.layout.row_group_rows", layout.row_group_rows)?;
    positive("collector.layout.page_rows", layout.page_rows)?;
    positive("collector.layout.max_row_group_bytes", layout.max_row_group_bytes)?;
    positive("collector.layout.max_page_bytes", layout.max_page_bytes)
}

fn validate_coalescing(coalescing: Option<&CoalescingTunables>) -> Result<()> {
    let Some(coalescing) = coalescing else {
        return Ok(());
    };
    positive("collector.coalescing.target_rows", coalescing.target_rows)?;
    positive("collector.coalescing.target_bytes", coalescing.target_bytes)?;
    positive("collector.coalescing.max_age_ms", coalescing.max_age_ms)?;
    positive(
        "collector.coalescing.max_pending_batches",
        coalescing.max_pending_batches,
    )
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

fn deep_merge(target: &mut Value, overrides: Value) {
    match (target, overrides) {
        (Value::Object(target), Value::Object(overrides)) => {
            for (key, value) in overrides {
                if value.is_null() {
                    continue;
                }
                deep_merge(target.entry(key).or_insert(Value::Null), value);
            }
        }
        (target, value) => *target = value,
    }
}

fn canonicalize(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        Value::Object(object) => {
            let sorted = object
                .into_iter()
                .filter(|(_, value)| !value.is_null())
                .map(|(key, value)| (key, canonicalize(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(Map::from_iter(sorted))
        }
        scalar => scalar,
    }
}

fn flatten_environment(value: &Value, prefix: &str, output: &mut BTreeMap<String, String>) {
    let Some(fields) = value.as_object() else {
        return;
    };
    for (name, value) in fields {
        if value.is_null() {
            continue;
        }
        let key = format!("{prefix}_{}", name.to_ascii_uppercase());
        match value {
            Value::Object(_) => flatten_environment(value, &key, output),
            Value::Array(_) => {
                output.insert(key, value.to_string());
            }
            _ => {
                output.insert(key, value.as_str().map_or_else(|| value.to_string(), str::to_owned));
            }
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
        let error = serde_json::from_str::<ColumnOverride>(
            r#"{"name":"x","physical_type":"int64","encoding":"dictionary","compression":null}"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown variant"));
    }

    #[test]
    fn validates_compression_bounds_bloom_probability_and_physical_encoding() -> Result<()> {
        let valid = Tunables {
            parquet_writer: Some(ParquetWriterTunables {
                compression: Some(Compression {
                    codec: CompressionCodec::Zstd,
                    level: Some(22),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        valid.validate()?;
        let invalid = Tunables {
            parquet_writer: Some(ParquetWriterTunables {
                compression: Some(Compression {
                    codec: CompressionCodec::Zstd,
                    level: Some(23),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("zstd"));
        for probability in [0.0, 1.0, f64::NAN] {
            let invalid = Tunables {
                parquet_writer: Some(ParquetWriterTunables {
                    bloom_filter: Some(BloomFilterTunables {
                        enabled: Some(true),
                        false_positive_probability: Some(probability),
                        ndv: None,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            };
            assert!(
                invalid
                    .validate()
                    .unwrap_err()
                    .to_string()
                    .contains("false_positive_probability")
            );
        }
        let invalid = Tunables {
            parquet_writer: Some(ParquetWriterTunables {
                columns: Some(vec![ColumnOverride {
                    name: "x".to_owned(),
                    physical_type: PhysicalType::Boolean,
                    compression: None,
                    encoding: Some(Encoding::DeltaBinaryPacked),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("physical type"));
        let declared = Tunables {
            parquet_writer: Some(ParquetWriterTunables {
                columns: Some(vec![ColumnOverride {
                    name: "x".to_owned(),
                    physical_type: PhysicalType::Int64,
                    compression: None,
                    encoding: Some(Encoding::DeltaBinaryPacked),
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            declared
                .validate_column_types(&BTreeMap::from([("x".to_owned(), PhysicalType::Int32)]))
                .is_err()
        );
        assert!(
            declared
                .validate_column_types(&BTreeMap::from([("x".to_owned(), PhysicalType::Int64)]))
                .is_ok()
        );
        Ok(())
    }

    #[test]
    fn validates_memory_limits_and_cross_field_bounds() {
        let invalid = Tunables {
            client: Some(ClientTunables {
                corpus_bytes: Some(20),
                max_memory_bytes: Some(10),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("max_memory_bytes"));
        let invalid = Tunables {
            collector: Some(CollectorTunables {
                buffered_bytes_limit: Some(9),
                rss_limit_bytes: Some(8),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("rss_limit"));
        let invalid = Tunables {
            wal: Some(WalTunables {
                retry_min_ms: Some(2_001),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("retry_min_ms"));
        let invalid = Tunables {
            transport: Some(TransportTunables {
                stream_window_bytes: Some(16_777_217),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            invalid
                .validate()
                .unwrap_err()
                .to_string()
                .contains("connection_window_bytes")
        );
        let invalid = Tunables {
            collector: Some(CollectorTunables {
                per_table_max_rows: Some(10_000_001),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("max_rows"));
        let invalid = Tunables {
            maintenance: Some(MaintenanceTunables {
                small_file_bytes: Some(134_217_729),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            invalid
                .validate()
                .unwrap_err()
                .to_string()
                .contains("target_file_bytes")
        );
        let invalid = Tunables {
            collector: Some(CollectorTunables {
                commit: Some(CommitTunables {
                    max_files_per_commit: Some(257),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            invalid
                .validate()
                .unwrap_err()
                .to_string()
                .contains("max_pending_files")
        );
        let auto_workers = Tunables {
            client: Some(ClientTunables {
                workers: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(auto_workers.validate().is_ok());
    }

    #[test]
    fn canonical_json_environment_and_durability_are_deterministic() -> Result<()> {
        let config: Tunables = serde_json::from_str(r#"{"wal":{"fsync_interval_ms":15},"client":{"batch_rows":4}}"#)?;
        assert_eq!(
            config.canonical_json()?,
            r#"{"client":{"batch_rows":4},"wal":{"fsync_interval_ms":15}}"#
        );
        assert_eq!(config.durability_class(), DurabilityClass::PeriodicFsync);
        let effective = config.canonical_effective_json(&serde_json::json!({"client":{"batch_rows":8192,"workers":0},"wal":{"fsync_interval_ms":0,"capacity_bytes":4294967296u64}}))?;
        assert_eq!(
            effective,
            r#"{"client":{"batch_rows":4,"workers":0},"wal":{"capacity_bytes":4294967296,"fsync_interval_ms":15}}"#
        );
        let env = config.environment()?;
        assert_eq!(env["ARROWCOL_CLIENT_BATCH_ROWS"], "4");
        assert_eq!(env["ARROWCOL_WAL_FSYNC_INTERVAL_MS"], "15");
        assert!(config.validate().is_ok());
        assert_eq!(
            config.environment_for(ComponentRole::Client)?["ARROWCOL_CLIENT_BATCH_ROWS"],
            "4"
        );
        assert_eq!(config.environment_for(ComponentRole::Router)?.len(), 0);
        assert_eq!(
            config.environment_for(ComponentRole::Collector)?["ARROWCOL_WAL_FSYNC_INTERVAL_MS"],
            "15"
        );
        Ok(())
    }

    #[test]
    fn validates_client_loadgen_bounds_and_legacy_inheritance() {
        let inherited = Tunables {
            client: Some(ClientTunables {
                workers: Some(0),
                streams_per_worker: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(inherited.validate().is_ok());
        let invalid = Tunables {
            client: Some(ClientTunables {
                corpus_rows: Some(10),
                corpus_bytes: Some(10),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            invalid
                .validate()
                .unwrap_err()
                .to_string()
                .contains("mutually exclusive")
        );
        let invalid = Tunables {
            client: Some(ClientTunables {
                workers: Some(1_025),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("workers"));
        let invalid = Tunables {
            client: Some(ClientTunables {
                entropy: Some(f64::INFINITY),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("entropy"));
    }

    #[test]
    fn unknown_fields_are_rejected_in_each_section() {
        let error =
            serde_json::from_str::<Tunables>(r#"{"client":{"batch_rows":1,"compression":"gzip"}}"#).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }
}
