mod validation;

pub use validation::{ApplyBoundary, Capability, DurabilityClass, capability_table};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Tunables {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<ClientTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<TransportTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub router: Option<RouterTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collector: Option<CollectorTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parquet_writer: Option<ParquetWriterTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrow_ipc: Option<ArrowIpcTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal: Option<WalTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance: Option<MaintenanceTunables>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTunables {
    pub clients_per_table: Option<usize>,
    pub generator_workers: Option<usize>,
    pub rows_per_upload: Option<usize>,
    pub seed: Option<u64>,
    pub send_interval_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
    pub max_attempts: Option<u32>,
    pub precache_max_bytes: Option<u64>,
    pub memory_budget_bytes: Option<u64>,
    pub customer_cardinality: Option<usize>,
    pub null_fraction: Option<f64>,
    pub payload_bytes: Option<usize>,
    pub value_entropy: Option<ValueEntropy>,
    pub key_skew: Option<f64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransportTunables {
    pub connections: Option<usize>,
    pub keepalive_interval_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
    pub max_frame_bytes: Option<u64>,
    pub stream_window_bytes: Option<u32>,
    pub connection_window_bytes: Option<u32>,
    pub grpc_compression: Option<GrpcCompression>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouterTunables {
    pub worker_threads: Option<usize>,
    pub listener_threads: Option<usize>,
    pub max_downstream_streams: Option<usize>,
    pub max_upstream_streams: Option<usize>,
    pub stream_window_bytes: Option<u32>,
    pub connection_window_bytes: Option<u32>,
    pub request_timeout_ms: Option<u64>,
    pub idle_timeout_ms: Option<u64>,
    pub connect_timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CollectorTunables {
    pub dataflow_cores: Option<usize>,
    pub pdata_channel_capacity: Option<usize>,
    pub flight_max_bytes: Option<u64>,
    pub batch_channel_capacity: Option<usize>,
    pub result_channel_capacity: Option<usize>,
    pub max_active_streams: Option<usize>,
    pub admission_timeout_ms: Option<u64>,
    pub stream_timeout_ms: Option<u64>,
    pub rss_limit_bytes: Option<u64>,
    pub buffered_bytes_limit: Option<u64>,
    pub max_batches: Option<usize>,
    pub max_rows: Option<usize>,
    pub max_bytes: Option<u64>,
    pub per_table_max_batches: Option<usize>,
    pub per_table_max_rows: Option<usize>,
    pub per_table_max_bytes: Option<u64>,
    pub flush: Option<FlushTunables>,
    pub commit: Option<CommitTunables>,
    pub runtime: Option<RuntimeTunables>,
    pub sort: Option<SortTunables>,
    pub layout: Option<LayoutTunables>,
    pub coalescing: Option<CoalescingTunables>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlushTunables {
    pub max_bytes: Option<u64>,
    pub max_rows: Option<usize>,
    pub max_batches: Option<usize>,
    pub max_age_ms: Option<u64>,
    pub concurrency: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitTunables {
    pub window_ms: Option<u64>,
    pub max_files_per_commit: Option<usize>,
    pub max_pending_files: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeTunables {
    pub async_workers: Option<usize>,
    pub blocking_workers: Option<usize>,
    pub upload_concurrency: Option<usize>,
    pub kernel_task_concurrency: Option<usize>,
    pub read_ahead: Option<usize>,
    pub batch_rows: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SortTunables {
    pub enabled: Option<bool>,
    pub max_rows: Option<usize>,
    pub max_bytes: Option<u64>,
    pub concurrency: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LayoutTunables {
    pub row_group_rows: Option<usize>,
    pub page_rows: Option<usize>,
    pub max_row_group_bytes: Option<u64>,
    pub max_page_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoalescingTunables {
    pub enabled: Option<bool>,
    pub target_rows: Option<usize>,
    pub target_bytes: Option<u64>,
    pub max_age_ms: Option<u64>,
    pub max_pending_batches: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetWriterTunables {
    pub compression: Option<Compression>,
    pub columns: Option<Vec<ColumnOverride>>,
    pub dictionary_enabled: Option<bool>,
    pub dictionary_page_size_bytes: Option<u64>,
    pub writer_version: Option<WriterVersion>,
    pub statistics: Option<StatisticsLevel>,
    pub page_index_enabled: Option<bool>,
    pub bloom_filter: Option<BloomFilterTunables>,
    pub row_group_rows: Option<usize>,
    pub row_group_bytes: Option<u64>,
    pub data_page_bytes: Option<u64>,
    pub data_page_rows: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Compression {
    pub codec: CompressionCodec,
    pub level: Option<u8>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CompressionCodec {
    #[default]
    Uncompressed,
    Snappy,
    Gzip,
    Brotli,
    Zstd,
    Lz4Raw,
    Lz4,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnOverride {
    pub name: String,
    pub physical_type: PhysicalType,
    pub compression: Option<Compression>,
    pub encoding: Option<Encoding>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    Plain,
    Rle,
    DeltaBinaryPacked,
    ByteStreamSplit,
    DeltaLengthByteArray,
    DeltaByteArray,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BloomFilterTunables {
    pub enabled: Option<bool>,
    pub false_positive_probability: Option<f64>,
    pub ndv: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WriterVersion {
    #[default]
    V1,
    V2,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StatisticsLevel {
    None,
    #[default]
    Chunk,
    Page,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArrowIpcTunables {
    pub compression: Option<ArrowIpcCompression>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ArrowIpcCompression {
    None,
    Lz4Frame,
    Zstd,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WalTunables {
    pub enabled: Option<bool>,
    pub fsync_interval_ms: Option<u64>,
    pub capacity_bytes: Option<u64>,
    pub segment_size_bytes: Option<u64>,
    pub poll_interval_ms: Option<u64>,
    pub retry_min_ms: Option<u64>,
    pub retry_max_ms: Option<u64>,
    pub max_retries: Option<u32>,
    pub overflow_policy: Option<WalOverflowPolicy>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WalOverflowPolicy {
    #[default]
    Backpressure,
    Reject,
    EvictOldest,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceTunables {
    pub interval_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub target_file_bytes: Option<u64>,
    pub small_file_bytes: Option<u64>,
    pub files_per_bin: Option<usize>,
    pub bins_per_commit: Option<usize>,
    pub max_decoded_bytes_per_bin: Option<u64>,
    pub max_decoded_batch_bytes: Option<u64>,
    pub max_row_group_bytes: Option<u64>,
    pub table_concurrency: Option<usize>,
    pub max_table_concurrency: Option<usize>,
    pub max_partitions: Option<usize>,
    pub purge_enabled: Option<bool>,
    pub vacuum_enabled: Option<bool>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ValueEntropy {
    Low,
    Medium,
    High,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum GrpcCompression {
    None,
    Gzip,
    Zstd,
}
