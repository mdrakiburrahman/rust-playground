mod validation;

pub use validation::{ApplyBoundary, Capability, ComponentRole, DurabilityClass, capability_table};

pub use arrow_deltalake::writer::{
    Codec as ParquetCodec, ColumnOptions as ParquetColumnOptions, ParquetWriterConfig, SortColumn as ParquetSortColumn,
    Statistics as ParquetStatistics, ValueEncoding as ParquetValueEncoding, Version as ParquetWriterVersion,
};

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
    pub parquet_writer: Option<ParquetWriterConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal: Option<WalTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance: Option<MaintenanceTunables>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTunables {
    pub corpus_rows: Option<u64>,
    pub corpus_bytes: Option<u64>,
    pub batch_rows: Option<usize>,
    pub workers: Option<usize>,
    pub data_mode: Option<DataMode>,
    pub string_width: Option<usize>,
    pub cardinality: Option<usize>,
    pub null_fraction: Option<f64>,
    pub skew: Option<f64>,
    pub entropy: Option<f64>,
    pub seed: Option<u64>,
    pub max_memory_bytes: Option<u64>,
    pub interval_us: Option<u64>,
    pub connections_per_worker: Option<usize>,
    pub streams_per_worker: Option<usize>,
    pub max_rows: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransportTunables {
    pub ipc_compression: Option<ArrowIpcCompression>,
    pub dictionary_handling: Option<DictionaryHandling>,
    pub max_flight_data_bytes: Option<u64>,
    pub max_message_bytes: Option<u64>,
    pub runtime_threads: Option<usize>,
    pub grpc_send_compression: Option<GrpcCompression>,
    pub grpc_accept_compression: Option<GrpcCompression>,
    pub max_frame_bytes: Option<u32>,
    pub header_table_bytes: Option<u32>,
    pub max_header_list_bytes: Option<u32>,
    pub stream_window_bytes: Option<u32>,
    pub connection_window_bytes: Option<u32>,
    pub adaptive_window: Option<bool>,
    pub keepalive_interval_ms: Option<u64>,
    pub keepalive_timeout_ms: Option<u64>,
    pub keepalive_while_idle: Option<bool>,
    pub connect_timeout_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
    pub tcp_nodelay: Option<bool>,
    pub tcp_keepalive_ms: Option<u64>,
    pub tcp_keepalive_interval_ms: Option<u64>,
    pub tcp_keepalive_retries: Option<u32>,
    pub rate_limit_rps: Option<u64>,
    pub endpoint_concurrency_limit: Option<usize>,
    pub endpoint_buffer_size: Option<usize>,
    pub connect_retries: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
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

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ArrowIpcCompression {
    None,
    Lz4Frame,
    Zstd,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DictionaryHandling {
    Hydrate,
    Resend,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DataMode {
    Shared,
    Independent,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum GrpcCompression {
    None,
    Gzip,
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
