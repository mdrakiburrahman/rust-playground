mod validation;

pub use validation::{
    ApplyBoundary, Capability, ComponentRole, DurabilityClass, capability_table, parse_tunable_duration,
};

pub use arrow_deltalake::writer::{
    Codec as ParquetCodec, ColumnOptions as ParquetColumnOptions, ParquetWriterConfig, SortColumn as ParquetSortColumn,
    Statistics as ParquetStatistics, ValueEncoding as ParquetValueEncoding, Version as ParquetWriterVersion,
};
pub use arrowcol_routing::AdmissionPressureGauge;

use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Payload customer IDs are bounded printable identifiers, not delivery identities.
#[must_use]
pub fn valid_payload_customer_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Tunables {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<ClientTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<TransportTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrow_ipc: Option<ArrowIpcTunables>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logging: Option<LoggingTunables>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTunables {
    pub payload_customer_id: Option<String>,
    pub corpus_rows: Option<u64>,
    pub corpus_bytes: Option<u64>,
    pub batch_rows: Option<usize>,
    pub workers: Option<usize>,
    pub runtime_threads: Option<usize>,
    pub data_mode: Option<DataMode>,
    pub data_profile: Option<ClientDataProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send_iter: Option<ClientSendIter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_builder: Option<ClientIdentityBuilder>,
    pub string_length_distribution: Option<ClientStringLengthDistribution>,
    pub value_variation: Option<ClientValueVariation>,
    pub guid_uniqueness: Option<ClientGuidUniqueness>,
    pub string_width: Option<usize>,
    pub cardinality: Option<usize>,
    pub null_fraction: Option<f64>,
    pub skew: Option<f64>,
    pub entropy: Option<f64>,
    pub seed: Option<u64>,
    pub max_memory_bytes: Option<u64>,
    pub interval_us: Option<u64>,
    pub connections_per_worker: Option<usize>,
    pub pod_ordinal: Option<u32>,
    pub streams_per_worker: Option<usize>,
    pub max_rows: Option<u64>,
    pub stream_retry_max_attempts: Option<u32>,
    pub stream_retry_base_backoff_ms: Option<u64>,
    pub stream_retry_max_backoff_ms: Option<u64>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_period_ms: Option<u64>,
    pub endpoint_concurrency_limit: Option<usize>,
    pub endpoint_buffer_size: Option<usize>,
    pub connect_retries: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArrowIpcTunables {
    pub compression: Option<ArrowIpcCompression>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alignment: Option<u8>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouterTunables {
    pub workers: Option<usize>,
    pub listener_tasks_per_fd: Option<usize>,
    pub keepalive_pool_size: Option<usize>,
    pub h2_max_concurrent_streams: Option<u32>,
    pub h2_stream_window_bytes: Option<u32>,
    pub h2_connection_window_bytes: Option<u32>,
    pub h2_idle_timeout_seconds: Option<u64>,
    pub sql_connect_timeout_ms: Option<u64>,
    pub sql_command_timeout_ms: Option<u64>,
    pub sql_pool_max_size: Option<usize>,
    pub sql_pool_acquire_timeout_ms: Option<u64>,
    pub refresh_interval_ms: Option<u64>,
    pub max_snapshot_age_ms: Option<u64>,
    pub max_future_skew_ms: Option<u64>,
    pub max_table_entries: Option<usize>,
    pub max_oldest_buffer_age_ms: Option<u64>,
    pub hard_pressure_bps: Option<u16>,
    pub excluded_admission_gauges: Option<Vec<AdmissionPressureGauge>>,
    pub warmup_ms: Option<u64>,
    pub minimum_warmup_bps: Option<u16>,
    pub lease_ttl_ms: Option<u64>,
    pub oat_run_pin_ttl_ms: Option<u64>,
    pub readiness_max_staleness_ms: Option<u64>,
    pub distinct_table_owners: Option<bool>,
    pub edge_replay_max_nonces: Option<usize>,
    pub upstream_max_h2_streams: Option<usize>,
    pub upstream_stream_window_bytes: Option<u32>,
    pub upstream_connection_window_bytes: Option<u32>,
    pub upstream_connection_timeout_ms: Option<u64>,
    pub upstream_total_connection_timeout_ms: Option<u64>,
    pub upstream_idle_timeout_seconds: Option<u64>,
    pub upstream_ping_interval_seconds: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RouterLogLevel {
    Inherit,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl RouterLogLevel {
    fn as_filter(self) -> Option<&'static str> {
        match self {
            Self::Inherit => None,
            Self::Error => Some("error"),
            Self::Warn => Some("warn"),
            Self::Info => Some("info"),
            Self::Debug => Some("debug"),
            Self::Trace => Some("trace"),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingTunables {
    pub router: Option<RouterLogLevel>,
    pub collector: Option<LogLevel>,
    pub maintenance: Option<LogLevel>,
}

pub const DEFAULT_COLLECTOR_SHUTDOWN_DRAIN_TIMEOUT_SECONDS: u32 = 60;
pub const MAX_COLLECTOR_SHUTDOWN_DRAIN_TIMEOUT_SECONDS: u32 = 1_500;

fn deserialize_shutdown_drain_timeout_seconds<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    u32::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CollectorTunables {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_shutdown_drain_timeout_seconds"
    )]
    pub shutdown_drain_timeout_seconds: Option<u32>,
    pub batching: Option<BatchingTunables>,
    pub catalog: Option<CatalogTunables>,
    pub commit: Option<CommitTunables>,
    pub exporter: Option<ExporterTunables>,
    pub layout: Option<LayoutTunables>,
    pub limits: Option<LimitsTunables>,
    pub pressure: Option<PressureTunables>,
    pub routing: Option<RoutingTunables>,
    pub runtime: Option<RuntimeTunables>,
    pub upload: Option<CollectorUploadTunables>,
    pub cpu_pressure: Option<CpuPressureMode>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CollectorUploadTunables {
    pub mode: Option<CollectorUploadMode>,
    pub buffer_capacity_bytes: Option<usize>,
    pub request_concurrency: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CatalogTunables {
    pub maximum_cache_age: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingTunables {
    pub connect_timeout: Option<String>,
    pub command_timeout: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExporterTunables {
    pub authority_timeout: Option<String>,
}

/// Largest exact JSON byte budget that also fits a Tokio semaphore on this target.
pub const MAX_QUEUE_CAPACITY_BYTES: u64 = if ((usize::MAX >> 3) as u64) < 9_007_199_254_740_991 {
    (usize::MAX >> 3) as u64
} else {
    9_007_199_254_740_991
};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueCapacityTunables {
    pub bytes: Option<u64>,
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

/// Collection behavior when the oldest queued batch reaches its age cutoff.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgeFlushPolicy {
    #[default]
    Immediate,
    DrainReady,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BatchingTunables {
    pub coalesce_publications: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_flush_policy: Option<AgeFlushPolicy>,
    pub global_capacity: Option<QueueCapacityTunables>,
    pub per_table_capacity: Option<QueueCapacityTunables>,
    pub max_arrow_memory_per_flush: Option<u64>,
    pub max_rows_per_flush: Option<u64>,
    pub max_batches_per_flush: Option<u64>,
    pub max_age: Option<String>,
    pub max_concurrent_flushes: Option<usize>,
    pub pipeline_depth: Option<usize>,
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
pub struct LayoutTunables {
    pub enabled: Option<bool>,
    pub sort_columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_options: Option<Vec<arrow_deltalake::layout::SortKeyOptions>>,
    pub bloom_columns: Option<Vec<String>>,
    pub max_rows: Option<usize>,
    pub max_input_bytes: Option<usize>,
    pub row_group_rows: Option<usize>,
    pub page_rows: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsTunables {
    pub grpc_accept_compression: Option<GrpcCompression>,
    pub grpc_send_compression: Option<GrpcCompression>,
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
    pub heartbeat_interval: Option<String>,
    pub registration_ttl: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeTunables {
    pub workers: Option<usize>,
    pub blocking_workers: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub blocking_fs_reserve: Option<usize>,
    pub upload_limit: Option<usize>,
    pub task_limit: Option<usize>,
    pub read_ahead: Option<usize>,
    pub batch_rows: Option<usize>,
    pub cpu_workers: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub cpu_queue_jobs: Option<usize>,
    pub generated_partition_fast_path: Option<bool>,
}

fn deserialize_present_usize<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    usize::deserialize(deserializer).map(Some)
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

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClientDataProfile {
    #[default]
    Diagnostic,
    Realistic,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClientSendIter {
    CloneVec,
    ArcCursor,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClientStringLengthDistribution {
    #[default]
    Fixed,
    Uniform,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClientIdentityBuilder {
    FormattedRows,
    DirectUtf8,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClientValueVariation {
    #[default]
    Cardinality,
    PerRow,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClientGuidUniqueness {
    #[default]
    Cardinality,
    PerRow,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CpuPressureMode {
    #[default]
    Off,
    Process,
}

impl CpuPressureMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Process => "process",
        }
    }
}

impl FromStr for CpuPressureMode {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "off" => Ok(Self::Off),
            "process" => Ok(Self::Process),
            _ => Err("expected 'off' or 'process'"),
        }
    }
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
    pub flush_interval_ms: Option<u64>,
    pub retention_size_cap_bytes: Option<u64>,
    pub max_age_secs: Option<u64>,
    pub size_cap_policy: Option<WalSizeCapPolicy>,
    pub max_segment_open_duration_ms: Option<u64>,
    pub poll_interval_ms: Option<u64>,
    pub initial_retry_interval_ms: Option<u64>,
    pub max_retry_interval_ms: Option<u64>,
    pub retry_multiplier: Option<f64>,
    pub max_in_flight: Option<usize>,
    pub otlp_handling: Option<WalOtlpHandling>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WalSizeCapPolicy {
    Backpressure,
    DropOldest,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WalOtlpHandling {
    PassThrough,
    ConvertToArrow,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MaintenanceTunables {
    pub enabled: Option<bool>,
    pub operations: Option<Vec<MaintenanceOperation>>,
    pub apply: Option<bool>,
    pub parquet_writer: Option<ParquetWriterConfig>,
    pub optimize_upload_mode: Option<MaintenanceUploadMode>,
    pub optimize_upload_buffer_bytes: Option<usize>,
    pub interval_secs: Option<u64>,
    pub max_idle_backoff_secs: Option<u64>,
    pub max_cycles: Option<u64>,
    pub max_consecutive_stale_plans: Option<u32>,
    pub max_concurrent_tables: Option<usize>,
    pub max_concurrent_decoded_bytes: Option<u64>,
    pub operation_timeout_secs: Option<u64>,
    pub commit_attempts: Option<u32>,
    pub commit_budget_secs: Option<u64>,
    pub catalog_runtime_threads: Option<usize>,
    pub catalog_rpc_timeout_secs: Option<u64>,
    pub catalog_max_decoding_message_bytes: Option<usize>,
    pub input_read_strategy: Option<MaintenanceInputReadStrategy>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layout_sort_options: Option<Vec<arrow_deltalake::layout::SortKeyOptions>>,
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
    pub sealed_partitions_only: Option<bool>,
    pub checkpoint_every_commits: Option<u64>,
    pub log_retention_secs: Option<u64>,
    pub max_prune_files: Option<usize>,
    pub purge_max_files_per_commit: Option<usize>,
    pub purge_max_plan_bytes: Option<usize>,
    pub vacuum_delete_chunk: Option<usize>,
    pub vacuum_max_plan_bytes: Option<usize>,
    pub replay_max_log_files: Option<usize>,
    pub replay_max_log_file_bytes: Option<u64>,
    pub replay_max_checkpoint_bytes: Option<u64>,
    pub operation_max_active_files: Option<usize>,
    pub discovery_max_log_files: Option<usize>,
    pub discovery_max_log_file_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceInputReadStrategy {
    Memory,
    Spool,
    Range,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceUploadMode {
    PutCreate,
    BufferedMultipart,
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

// PERF69-TUNABLES-BEGIN oat-evidence
// PERF69-TUNABLES-END oat-evidence
// PERF69-TUNABLES-BEGIN oat-campaign
// PERF69-TUNABLES-END oat-campaign
// PERF69-TUNABLES-BEGIN aks-custody
// PERF69-TUNABLES-END aks-custody
// PERF69-TUNABLES-BEGIN file-formation
// PERF69-TUNABLES-END file-formation
// PERF69-TUNABLES-BEGIN commit-pipeline
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CollectorUploadMode {
    #[default]
    PutCreate,
    BufferedMultipart,
}

impl CollectorUploadMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PutCreate => "put_create",
            Self::BufferedMultipart => "buffered_multipart",
        }
    }
}

impl FromStr for CollectorUploadMode {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "put_create" => Ok(Self::PutCreate),
            "buffered_multipart" => Ok(Self::BufferedMultipart),
            _ => Err("expected 'put_create' or 'buffered_multipart'"),
        }
    }
}
// PERF69-TUNABLES-END commit-pipeline
// PERF69-TUNABLES-BEGIN maintenance
// PERF69-TUNABLES-END maintenance
// PERF69-TUNABLES-BEGIN loadgen
// PERF69-TUNABLES-END loadgen
// PERF69-TUNABLES-BEGIN telemetry
// PERF69-TUNABLES-END telemetry
// PERF69-TUNABLES-BEGIN grafana
// PERF69-TUNABLES-END grafana
