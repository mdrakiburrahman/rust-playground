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
    pub runtime_threads: Option<usize>,
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
#[serde(default, deny_unknown_fields)]
pub struct CollectorTunables {
    pub batching: Option<BatchingTunables>,
    pub commit: Option<CommitTunables>,
    pub limits: Option<LimitsTunables>,
    pub pressure: Option<PressureTunables>,
    pub runtime: Option<RuntimeTunables>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueCapacityTunables {
    pub bytes: Option<u32>,
    pub rows: Option<u32>,
    pub batches: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdaptiveFileTunables {
    pub target_parquet_bytes: Option<u64>,
    pub min_input_bytes: Option<u64>,
    pub max_input_bytes: Option<u64>,
    pub cold_start_parquet_ratio: Option<f64>,
    pub ewma_weight: Option<f64>,
    pub min_parquet_ratio: Option<f64>,
    pub max_parquet_ratio: Option<f64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BatchingTunables {
    pub coalesce_publications: Option<bool>,
    pub global_capacity: Option<QueueCapacityTunables>,
    pub per_table_capacity: Option<QueueCapacityTunables>,
    pub max_arrow_memory_per_flush: Option<u64>,
    pub max_rows_per_flush: Option<u64>,
    pub max_batches_per_flush: Option<u64>,
    pub max_age: Option<String>,
    pub max_concurrent_flushes: Option<usize>,
    pub adaptive_file: Option<AdaptiveFileTunables>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CommitTunables {
    pub batch_window: Option<String>,
    pub max_files_per_commit: Option<usize>,
    pub max_pending_files: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsTunables {
    pub max_metadata_bytes: Option<usize>,
    pub max_schema_bytes: Option<usize>,
    pub max_message_bytes: Option<usize>,
    pub batch_channel_capacity: Option<usize>,
    pub put_result_channel_capacity: Option<usize>,
    pub max_stream_duration: Option<String>,
    pub admission_timeout: Option<String>,
    pub max_ddl_bytes: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PressureTunables {
    pub max_concurrent_streams: Option<usize>,
    pub rss_limit_bytes: Option<u64>,
    pub buffered_bytes_limit: Option<u64>,
    pub buffered_rows_limit: Option<u64>,
    pub buffered_batches_limit: Option<u64>,
    pub pressure_interval: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeTunables {
    pub workers: Option<usize>,
    pub blocking_workers: Option<usize>,
    pub upload_limit: Option<usize>,
    pub task_limit: Option<usize>,
    pub read_ahead: Option<usize>,
    pub batch_rows: Option<usize>,
    pub cpu_workers: Option<usize>,
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
#[serde(default, deny_unknown_fields)]
pub struct MaintenanceTunables {
    pub enabled: Option<bool>,
    pub operations: Option<Vec<MaintenanceOperation>>,
    pub apply: Option<bool>,
    pub interval_secs: Option<u64>,
    pub max_idle_backoff_secs: Option<u64>,
    pub max_cycles: Option<u64>,
    pub max_concurrent_tables: Option<usize>,
    pub operation_timeout_secs: Option<u64>,
    pub granularity: Option<MaintenanceGranularity>,
    pub purge_retention_secs: Option<u64>,
    pub anchor_ms: Option<i64>,
    pub vacuum_retention_secs: Option<u64>,
    pub allow_short_vacuum_retention: Option<bool>,
    pub target_file_mib: Option<u64>,
    pub small_file_bytes: Option<u64>,
    pub min_files: Option<usize>,
    pub vacuum_max_deletes: Option<usize>,
    pub vacuum_max_listed_files: Option<usize>,
    pub layout_enabled: Option<bool>,
    pub layout_sort_columns: Option<Vec<String>>,
    pub layout_bloom_columns: Option<Vec<String>>,
    pub layout_max_rows: Option<usize>,
    pub layout_max_input_bytes: Option<usize>,
    pub layout_row_group_rows: Option<usize>,
    pub layout_page_rows: Option<usize>,
    pub max_files_per_bin: Option<usize>,
    pub max_bins_per_commit: Option<usize>,
    pub max_actions_per_commit: Option<usize>,
    pub max_plan_bytes: Option<usize>,
    pub max_decoded_bytes_per_bin: Option<u64>,
    pub max_row_group_bytes: Option<u64>,
    pub max_batch_bytes: Option<usize>,
    pub batch_rows: Option<usize>,
    pub output_row_group_rows: Option<usize>,
    pub max_output_file_bytes: Option<u64>,
    pub max_active_files: Option<usize>,
    pub latest_partitions: Option<usize>,
    pub sealed_final: Option<bool>,
    pub checkpoint_every_commits: Option<u64>,
    pub log_retention_secs: Option<u64>,
    pub max_prune_files: Option<usize>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceGranularity {
    Minute,
    Hour,
    Day,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceOperation {
    Purge,
    Optimize,
    Vacuum,
    Checkpoint,
    PruneLog,
}
