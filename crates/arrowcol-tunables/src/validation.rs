use crate::*;
use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

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
    Durable,
    FsyncBatched,
    LossyEviction,
}

macro_rules! capability {
    ($name:literal, $default:literal) => {
        Capability {
            name: $name,
            default: $default,
            boundary: ApplyBoundary::ProcessStartup,
        }
    };
    ($name:literal, $default:literal, $boundary:ident) => {
        Capability {
            name: $name,
            default: $default,
            boundary: ApplyBoundary::$boundary,
        }
    };
}

const CAPABILITIES: &[Capability] = &[
    capability!("client.corpus_rows", "component default"),
    capability!("client.corpus_bytes", "component default"),
    capability!("client.batch_rows", "8192"),
    capability!("client.workers", "0 (available parallelism)"),
    capability!("client.runtime_threads", "0 (automatic)"),
    capability!("client.data_mode", "shared"),
    capability!("client.string_width", "0 (canonical fallback)"),
    capability!("client.cardinality", "1024"),
    capability!("client.null_fraction", "0"),
    capability!("client.skew", "0"),
    capability!("client.entropy", "1"),
    capability!("client.seed", "component default"),
    capability!("client.max_memory_bytes", "536870912"),
    capability!("client.interval_us", "0"),
    capability!("client.connections_per_worker", "1"),
    capability!("client.streams_per_worker", "0 (legacy concurrency)"),
    capability!("client.max_rows", "component default"),
    capability!("transport.ipc_compression", "none (compatibility alias)"),
    capability!("arrow_ipc.compression", "none"),
    capability!(
        "transport.dictionary_handling",
        "hydrate; resend rejected for current DMV schema"
    ),
    capability!("transport.max_flight_data_bytes", "32505856 (31 MiB)"),
    capability!("transport.max_message_bytes", "33554432 (32 MiB)"),
    capability!("transport.grpc_send_compression", "disabled"),
    capability!("transport.grpc_accept_compression", "disabled"),
    capability!("transport.max_frame_bytes", "16384"),
    capability!("transport.header_table_bytes", "component default"),
    capability!("transport.max_header_list_bytes", "16384"),
    capability!("transport.stream_window_bytes", "1048576"),
    capability!("transport.connection_window_bytes", "16777216"),
    capability!("transport.adaptive_window", "false"),
    capability!("transport.keepalive_interval_ms", "10000"),
    capability!("transport.keepalive_timeout_ms", "20000"),
    capability!("transport.keepalive_while_idle", "true"),
    capability!("transport.connect_timeout_ms", "0 (inherit request timeout)"),
    capability!("transport.request_timeout_ms", "component default"),
    capability!("transport.tcp_nodelay", "true"),
    capability!("transport.tcp_keepalive_ms", "component default"),
    capability!("transport.tcp_keepalive_interval_ms", "component default"),
    capability!("transport.tcp_keepalive_retries", "component default"),
    capability!("transport.rate_limit_rps", "disabled"),
    capability!("transport.endpoint_concurrency_limit", "256"),
    capability!("transport.endpoint_buffer_size", "1024"),
    capability!("transport.connect_retries", "component default"),
    capability!("transport.retry_backoff_ms", "100"),
    capability!(
        "transport.receiver_ipc_decode_support",
        "build-gated: IPC codec feature graph",
        RebuildOnly
    ),
    capability!(
        "transport.receiver_grpc_decode_support",
        "build-gated: gRPC codec feature graph",
        RebuildOnly
    ),
    capability!("router.workers", "2"),
    capability!("router.listener_tasks_per_fd", "1"),
    capability!("router.keepalive_pool_size", "1024"),
    capability!("router.h2_max_concurrent_streams", "256"),
    capability!("router.h2_stream_window_bytes", "1048576"),
    capability!("router.h2_connection_window_bytes", "16777216"),
    capability!("router.h2_idle_timeout_seconds", "60"),
    capability!("router.sql_connect_timeout_ms", "5000"),
    capability!("router.sql_command_timeout_ms", "15000"),
    capability!("router.refresh_interval_ms", "1000"),
    capability!("router.max_snapshot_age_ms", "15000"),
    capability!("router.max_future_skew_ms", "2000"),
    capability!("router.max_table_entries", "256"),
    capability!("router.max_oldest_buffer_age_ms", "86400000"),
    capability!("router.hard_pressure_bps", "9500"),
    capability!("router.warmup_ms", "60000"),
    capability!("router.minimum_warmup_bps", "1000"),
    capability!("router.lease_ttl_ms", "30000"),
    capability!("router.oat_run_pin_ttl_ms", "consumer default"),
    capability!("router.readiness_max_staleness_ms", "30000"),
    capability!("router.distinct_table_owners", "false"),
    capability!("router.edge_replay_max_nonces", "consumer default"),
    capability!("router.upstream_max_h2_streams", "128"),
    capability!("router.upstream_stream_window_bytes", "1048576"),
    capability!("router.upstream_connection_window_bytes", "16777216"),
    capability!("router.upstream_connection_timeout_ms", "1000"),
    capability!("router.upstream_total_connection_timeout_ms", "3000"),
    capability!("router.upstream_idle_timeout_seconds", "60"),
    capability!("router.upstream_ping_interval_seconds", "15; 0 disables"),
    capability!("collector.batching.coalesce_publications", "false"),
    capability!("collector.batching.global_capacity.bytes", "2147483648"),
    capability!("collector.batching.global_capacity.rows", "10000000"),
    capability!("collector.batching.global_capacity.batches", "4096"),
    capability!("collector.batching.per_table_capacity.bytes", "536870912"),
    capability!("collector.batching.per_table_capacity.rows", "2000000"),
    capability!("collector.batching.per_table_capacity.batches", "1024"),
    capability!("collector.batching.max_arrow_memory_per_flush", "536870912"),
    capability!("collector.batching.max_rows_per_flush", "1000000"),
    capability!("collector.batching.max_batches_per_flush", "512"),
    capability!("collector.batching.max_age", "30s"),
    capability!("collector.batching.max_concurrent_flushes", "4"),
    capability!("collector.batching.adaptive_file.target_parquet_bytes", "134217728"),
    capability!("collector.batching.adaptive_file.min_input_bytes", "16777216"),
    capability!("collector.batching.adaptive_file.max_input_bytes", "1073741824"),
    capability!("collector.batching.adaptive_file.cold_start_parquet_ratio", "0.25"),
    capability!("collector.batching.adaptive_file.ewma_weight", "0.2"),
    capability!("collector.batching.adaptive_file.min_parquet_ratio", "0.015625"),
    capability!("collector.batching.adaptive_file.max_parquet_ratio", "4"),
    capability!("collector.catalog.maximum_cache_age", "10s; remote catalog only"),
    capability!("collector.routing.connect_timeout", "15s; SQL Server routing only"),
    capability!("collector.routing.command_timeout", "30s; SQL Server routing only"),
    capability!("collector.exporter.authority_timeout", "10s"),
    capability!("collector.commit.batch_window", "10s"),
    capability!("collector.commit.max_files_per_commit", "64"),
    capability!("collector.commit.max_pending_files", "256"),
    capability!("collector.limits.max_metadata_bytes", "16384"),
    capability!("collector.limits.max_schema_bytes", "1048576"),
    capability!("collector.limits.max_message_bytes", "16777216"),
    capability!("collector.limits.batch_channel_capacity", "4"),
    capability!("collector.limits.put_result_channel_capacity", "8"),
    capability!("collector.limits.max_stream_duration", "300s"),
    capability!("collector.limits.admission_timeout", "30s"),
    capability!("collector.limits.max_ddl_bytes", "65536"),
    capability!("collector.pressure.max_concurrent_streams", "128"),
    capability!("collector.pressure.rss_limit_bytes", "2147483648"),
    capability!("collector.pressure.buffered_bytes_limit", "536870912"),
    capability!("collector.pressure.buffered_rows_limit", "10000000"),
    capability!("collector.pressure.buffered_batches_limit", "4096"),
    capability!("collector.pressure.pressure_interval", "2s"),
    capability!("collector.pressure.heartbeat_interval", "5s"),
    capability!(
        "collector.pressure.registration_ttl",
        "20s; must exceed heartbeat_interval"
    ),
    capability!("collector.runtime.workers", "clamp(c/4,2,16)"),
    capability!("collector.runtime.blocking_workers", "clamp(2c,32,128)"),
    capability!("collector.runtime.upload_limit", "clamp(2c,8,32)"),
    capability!("collector.runtime.task_limit", "clamp(4c,64,256)"),
    capability!("collector.runtime.read_ahead", "clamp(c,4,16)"),
    capability!("collector.runtime.batch_rows", "clamp(1024c,4096,32768)"),
    capability!("collector.runtime.cpu_workers", "clamp(c-1,1,32)"),
    capability!("parquet_writer.defaults.compression", "inherit component default"),
    capability!("parquet_writer.defaults.compression_level", "codec default"),
    capability!("parquet_writer.defaults.encoding", "inherit component default"),
    capability!(
        "parquet_writer.defaults.dictionary_enabled",
        "inherit component default"
    ),
    capability!(
        "parquet_writer.defaults.dictionary_page_size_limit",
        "inherit component default"
    ),
    capability!(
        "parquet_writer.defaults.data_page_size_limit",
        "inherit component default"
    ),
    capability!("parquet_writer.defaults.statistics", "inherit component default"),
    capability!(
        "parquet_writer.defaults.write_page_header_statistics",
        "inherit component default"
    ),
    capability!(
        "parquet_writer.defaults.bloom_filter_enabled",
        "inherit component default"
    ),
    capability!("parquet_writer.defaults.bloom_filter_fpp", "inherit component default"),
    capability!("parquet_writer.defaults.bloom_filter_ndv", "inherit component default"),
    capability!(
        "parquet_writer.defaults.data_page_v2_compression_ratio_threshold",
        "inherit component default"
    ),
    capability!("parquet_writer.columns.*.compression", "inherit defaults"),
    capability!("parquet_writer.columns.*.compression_level", "codec default"),
    capability!("parquet_writer.columns.*.encoding", "inherit defaults"),
    capability!("parquet_writer.columns.*.dictionary_enabled", "inherit defaults"),
    capability!(
        "parquet_writer.columns.*.dictionary_page_size_limit",
        "inherit defaults"
    ),
    capability!("parquet_writer.columns.*.data_page_size_limit", "inherit defaults"),
    capability!("parquet_writer.columns.*.statistics", "inherit defaults"),
    capability!(
        "parquet_writer.columns.*.write_page_header_statistics",
        "inherit defaults"
    ),
    capability!("parquet_writer.columns.*.bloom_filter_enabled", "inherit defaults"),
    capability!("parquet_writer.columns.*.bloom_filter_fpp", "inherit defaults"),
    capability!("parquet_writer.columns.*.bloom_filter_ndv", "inherit defaults"),
    capability!(
        "parquet_writer.columns.*.data_page_v2_compression_ratio_threshold",
        "inherit defaults"
    ),
    capability!("parquet_writer.writer_version", "inherit component default"),
    capability!("parquet_writer.max_row_group_rows", "inherit component default"),
    capability!("parquet_writer.max_row_group_bytes", "inherit component default"),
    capability!("parquet_writer.data_page_row_count_limit", "inherit component default"),
    capability!("parquet_writer.write_batch_size", "inherit component default"),
    capability!(
        "parquet_writer.offset_index_enabled",
        "inherit component default; Page statistics require offset index"
    ),
    capability!(
        "parquet_writer.column_index_truncate_length",
        "inherit component default"
    ),
    capability!("parquet_writer.statistics_truncate_length", "inherit component default"),
    capability!("parquet_writer.sorting_columns", "inherit verified table layout"),
    capability!(
        "collector.parquet_codec_support",
        "requires compiled codec features",
        RebuildOnly
    ),
    capability!(
        "collector.allocator",
        "system; mimalloc requires a rebuilt collector image",
        RebuildOnly
    ),
    capability!(
        "collector.multipart_upload",
        "unsupported: pinned backend lacks conditional completion",
        RebuildOnly
    ),
    capability!("wal.flush_interval_ms", "0s"),
    capability!("wal.retention_size_cap_bytes", "4 GiB"),
    capability!("wal.max_age_secs", "unset"),
    capability!("wal.size_cap_policy", "backpressure"),
    capability!("wal.max_segment_open_duration_ms", "1s"),
    capability!("wal.poll_interval_ms", "100ms"),
    capability!("wal.initial_retry_interval_ms", "250ms"),
    capability!("wal.max_retry_interval_ms", "2s"),
    capability!("wal.retry_multiplier", "2.0"),
    capability!("wal.max_in_flight", "1000"),
    capability!("wal.otlp_handling", "pass_through"),
    capability!("maintenance.enabled", "false"),
    capability!("maintenance.operations", "unset"),
    capability!("maintenance.apply", "false"),
    capability!("maintenance.interval_secs", "one-shot"),
    capability!("maintenance.max_idle_backoff_secs", "3600"),
    capability!("maintenance.max_cycles", "unlimited"),
    capability!("maintenance.max_concurrent_tables", "1"),
    capability!("maintenance.max_concurrent_decoded_bytes", "unset"),
    capability!("maintenance.operation_timeout_secs", "900"),
    capability!("maintenance.commit_attempts", "8"),
    capability!("maintenance.commit_budget_secs", "10"),
    capability!("maintenance.catalog_runtime_threads", "2"),
    capability!("maintenance.catalog_rpc_timeout_secs", "30"),
    capability!("maintenance.catalog_max_decoding_message_bytes", "33554432"),
    capability!("maintenance.input_read_strategy", "memory; spool or range"),
    capability!("maintenance.granularity", "catalog default; hour"),
    capability!("maintenance.purge_retention_secs", "catalog default; 2592000"),
    capability!("maintenance.anchor_ms", "current time"),
    capability!("maintenance.vacuum_retention_secs", "604800"),
    capability!("maintenance.allow_short_vacuum_retention", "false"),
    capability!("maintenance.target_file_mib", "128"),
    capability!("maintenance.small_file_bytes", "half target"),
    capability!("maintenance.min_files", "2"),
    capability!("maintenance.vacuum_max_deletes", "10000"),
    capability!("maintenance.vacuum_max_listed_files", "100000"),
    capability!("maintenance.layout_enabled", "automatic"),
    capability!("maintenance.layout_sort_columns", "customer_id"),
    capability!("maintenance.layout_bloom_columns", "customer_id"),
    capability!("maintenance.layout_max_rows", "262144"),
    capability!("maintenance.layout_max_input_bytes", "67108864"),
    capability!("maintenance.layout_row_group_rows", "16384"),
    capability!("maintenance.layout_page_rows", "2048"),
    capability!("maintenance.max_files_per_bin", "64"),
    capability!("maintenance.max_bins_per_commit", "16"),
    capability!("maintenance.max_actions_per_commit", "2048"),
    capability!("maintenance.max_plan_bytes", "1048576"),
    capability!("maintenance.max_decoded_bytes_per_bin", "1073741824"),
    capability!("maintenance.max_row_group_bytes", "134217728"),
    capability!("maintenance.max_batch_bytes", "67108864"),
    capability!("maintenance.batch_rows", "8192"),
    capability!("maintenance.output_row_group_rows", "65536"),
    capability!("maintenance.max_output_file_bytes", "1072693248"),
    capability!("maintenance.max_active_files", "1000000"),
    capability!("maintenance.latest_partitions", "all"),
    capability!("maintenance.sealed_final", "false"),
    capability!("maintenance.checkpoint_every_commits", "100"),
    capability!("maintenance.log_retention_secs", "table policy or 2592000"),
    capability!("maintenance.max_prune_files", "10000"),
    capability!("maintenance.purge_max_files_per_commit", "10000"),
    capability!("maintenance.purge_max_plan_bytes", "4194304"),
    capability!("maintenance.vacuum_delete_chunk", "256"),
    capability!("maintenance.vacuum_max_plan_bytes", "8388608"),
    capability!("maintenance.replay_max_log_files", "100000"),
    capability!("maintenance.replay_max_log_file_bytes", "67108864"),
    capability!("maintenance.replay_max_checkpoint_bytes", "268435456"),
    capability!("maintenance.operation_max_active_files", "1000000"),
    capability!("maintenance.discovery_max_log_files", "100000"),
    capability!("maintenance.discovery_max_log_file_bytes", "67108864"),
];

#[must_use]
pub fn capability_table() -> &'static [Capability] {
    CAPABILITIES
}

impl Tunables {
    /// Validates values and verifies every configured section can be projected to process environments.
    ///
    /// # Errors
    /// Returns an error for invalid values, unsupported settings, or an unprojectable configuration.
    pub fn validate_for_oat(&self) -> Result<()> {
        self.validate()?;
        for role in [
            ComponentRole::Client,
            ComponentRole::Router,
            ComponentRole::Collector,
            ComponentRole::Maintenance,
        ] {
            self.environment_for(role)?;
        }
        Ok(())
    }

    /// Validates enum-specific ranges, positivity, cross-field relationships, and explicit memory budgets.
    ///
    /// # Errors
    /// Returns an error when a setting is out of bounds, unsupported, or inconsistent with another setting.
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
            if client.runtime_threads.is_some_and(|threads| threads > 1_024) {
                bail!("client.runtime_threads must be 0 (automatic) or at most 1024");
            }
            if client.max_memory_bytes.is_some_and(|bytes| bytes > isize::MAX as u64) {
                bail!("client.max_memory_bytes must fit the address space");
            }
            if client.batch_rows.is_some_and(|rows| rows > i32::MAX as usize) {
                bail!("client.batch_rows exceeds Arrow's supported variable-width range");
            }
            if client.interval_us.is_some_and(|interval| interval > 86_400_000_000) {
                bail!("client.interval_us must not exceed 86400000000");
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
        if self
            .arrow_ipc
            .as_ref()
            .is_some_and(|settings| settings.compression.is_some())
            && self
                .transport
                .as_ref()
                .is_some_and(|settings| settings.ipc_compression.is_some())
        {
            bail!("configure Arrow IPC compression in arrow_ipc.compression, not in both sections");
        }
        if let Some(transport) = &self.transport {
            let message_bytes = transport.max_message_bytes.unwrap_or(33_554_432);
            let flight_bytes = transport.max_flight_data_bytes.unwrap_or(32_505_856);
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
                if value > 65_536 {
                    bail!("{field} must not exceed 65536");
                }
            }
            if transport.connect_retries.is_some_and(|retries| retries > 100) {
                bail!("transport.connect_retries must be between 0 and 100");
            }
            if transport.dictionary_handling == Some(DictionaryHandling::Resend) {
                bail!("transport.dictionary_handling=resend is unsupported for the current plain DMV schema");
            }
        }
        validate_router(self.router.as_ref())?;
        if let Some(collector) = &self.collector {
            validate_collector_controls(collector)?;
            validate_batching(collector.batching.as_ref())?;
            validate_commit(collector.commit.as_ref())?;
            validate_limits(collector.limits.as_ref())?;
            validate_pressure(collector.pressure.as_ref())?;
            validate_runtime(collector.runtime.as_ref())?;
        }
        if let Some(writer) = &self.parquet_writer {
            writer.validate()?;
        }
        validate_wal(self.wal.as_ref())?;
        if let Some(maintenance) = &self.maintenance {
            validate_maintenance(maintenance)?;
        }
        Ok(())
    }

    /// Serializes only explicitly requested settings in stable key order.
    ///
    /// # Errors
    /// Returns an error if the configuration cannot be serialized as JSON.
    pub fn canonical_json(&self) -> Result<String> {
        let value = serde_json::to_value(self)?;
        Ok(canonicalize(value).to_string())
    }

    /// Merges explicit overrides over component-resolved defaults and serializes the effective JSON canonically.
    /// `resolved_defaults` must be a JSON object returned by the component config/default resolver.
    ///
    /// # Errors
    /// Returns an error if defaults are not an object, the settings are invalid, or JSON serialization fails.
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
    ///
    /// # Errors
    /// Returns an error if settings cannot be serialized or projected for the role.
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
                ComponentRole::Maintenance => {
                    self.maintenance
                        .as_ref()
                        .is_some_and(|settings| settings.enabled == Some(true))
                        && (name.starts_with("ARROWCOL_MAINT_") || name == "ARROWCOL_PARQUET_CONFIG")
                }
            })
            .collect();
        Ok(selected)
    }

    /// Labels the configured WAL durability class; full settings remain in the canonical JSON.
    #[must_use]
    pub fn durability_class(&self) -> DurabilityClass {
        let Some(wal) = &self.wal else {
            return DurabilityClass::Durable;
        };
        if wal.max_age_secs.is_some() || wal.size_cap_policy == Some(WalSizeCapPolicy::DropOldest) {
            return DurabilityClass::LossyEviction;
        }
        if wal.flush_interval_ms.unwrap_or(0) > 0 {
            return DurabilityClass::FsyncBatched;
        }
        DurabilityClass::Durable
    }

    /// Projects configured values to component environment variables and canonical JSON adapters.
    ///
    /// # Errors
    /// Returns an error if a configuration section cannot be serialized or projected.
    pub fn environment(&self) -> Result<BTreeMap<String, String>> {
        let value = serde_json::to_value(self)?;
        let mut output = BTreeMap::new();
        let Some(sections) = value.as_object() else {
            return Ok(output);
        };
        for (section, section_value) in sections {
            if section == "collector" {
                let Some(collector) = self.collector.as_ref() else {
                    bail!("collector section serialized without typed settings");
                };
                if let Some(value) = &collector.batching {
                    let mut value = value.clone();
                    canonicalize_optional_duration(&mut value.max_age, "collector.batching.max_age")?;
                    insert_json_config("ARROWCOL_COLLECTOR_BATCHING_CONFIG", &value, &mut output)?;
                }
                if let Some(value) = &collector.catalog {
                    let mut value = value.clone();
                    canonicalize_optional_duration(
                        &mut value.maximum_cache_age,
                        "collector.catalog.maximum_cache_age",
                    )?;
                    insert_json_config("ARROWCOL_COLLECTOR_CATALOG_CONFIG", &value, &mut output)?;
                }
                if let Some(value) = &collector.commit {
                    let mut value = value.clone();
                    canonicalize_optional_duration(&mut value.batch_window, "collector.commit.batch_window")?;
                    insert_json_config("ARROWCOL_COLLECTOR_COMMIT_CONFIG", &value, &mut output)?;
                }
                if let Some(value) = &collector.exporter {
                    let mut value = value.clone();
                    canonicalize_optional_duration(
                        &mut value.authority_timeout,
                        "collector.exporter.authority_timeout",
                    )?;
                    insert_json_config("ARROWCOL_COLLECTOR_EXPORTER_CONFIG", &value, &mut output)?;
                }
                if let Some(value) = &collector.limits {
                    let mut value = value.clone();
                    canonicalize_optional_duration(
                        &mut value.max_stream_duration,
                        "collector.limits.max_stream_duration",
                    )?;
                    canonicalize_optional_duration(&mut value.admission_timeout, "collector.limits.admission_timeout")?;
                    insert_json_config("ARROWCOL_COLLECTOR_LIMITS_CONFIG", &value, &mut output)?;
                }
                if let Some(value) = &collector.pressure {
                    let mut value = value.clone();
                    canonicalize_optional_duration(
                        &mut value.pressure_interval,
                        "collector.pressure.pressure_interval",
                    )?;
                    canonicalize_optional_duration(
                        &mut value.heartbeat_interval,
                        "collector.pressure.heartbeat_interval",
                    )?;
                    canonicalize_optional_duration(&mut value.registration_ttl, "collector.pressure.registration_ttl")?;
                    insert_json_config("ARROWCOL_COLLECTOR_PRESSURE_CONFIG", &value, &mut output)?;
                }
                if let Some(value) = &collector.routing {
                    let mut value = value.clone();
                    canonicalize_optional_duration(&mut value.connect_timeout, "collector.routing.connect_timeout")?;
                    canonicalize_optional_duration(&mut value.command_timeout, "collector.routing.command_timeout")?;
                    insert_json_config("ARROWCOL_COLLECTOR_ROUTING_CONFIG", &value, &mut output)?;
                }
                if let Some(runtime) = &collector.runtime {
                    for (field, value) in [
                        ("RUNTIME_WORKERS", runtime.workers),
                        ("BLOCKING_WORKERS", runtime.blocking_workers),
                        ("UPLOAD_LIMIT", runtime.upload_limit),
                        ("TASK_LIMIT", runtime.task_limit),
                        ("READ_AHEAD", runtime.read_ahead),
                        ("BATCH_ROWS", runtime.batch_rows),
                        ("CPU_WORKERS", runtime.cpu_workers),
                    ] {
                        if let Some(value) = value {
                            output.insert(format!("ARROWCOL_COLLECTOR_{field}"), value.to_string());
                        }
                    }
                }
                continue;
            }
            if section == "maintenance" {
                let mut value = section_value.clone();
                if let Some(object) = value.as_object_mut() {
                    object.remove("enabled");
                    object.remove("apply");
                    object.remove("operations");
                }
                flatten_environment(&value, "ARROWCOL_MAINT", &mut output);
                continue;
            }
            if section == "parquet_writer" {
                output.insert(
                    "ARROWCOL_PARQUET_CONFIG".to_owned(),
                    canonicalize(section_value.clone()).to_string(),
                );
                continue;
            }
            if section == "arrow_ipc" {
                if let Some(compression) = self.arrow_ipc.as_ref().and_then(|settings| settings.compression) {
                    let value = match compression {
                        ArrowIpcCompression::None => "none",
                        ArrowIpcCompression::Lz4Frame => "lz4_frame",
                        ArrowIpcCompression::Zstd => "zstd",
                    };
                    output.insert("ARROWCOL_CLIENT_IPC_COMPRESSION".to_owned(), value.to_owned());
                }
                continue;
            }
            if section == "wal" {
                let Some(wal) = self.wal.as_ref() else {
                    bail!("WAL section serialized without typed settings");
                };
                project_wal(wal, &mut output);
                continue;
            }
            let prefix = match section.as_str() {
                "client" => "ARROWCOL_CLIENT",
                "transport" => "ARROWCOL_CLIENT",
                "router" => "ARROWCOL_ROUTER",
                "collector" => "ARROWCOL_COLLECTOR",
                "wal" => "ARROWCOL_WAL",
                "maintenance" => "ARROWCOL_MAINT",
                _ => continue,
            };
            flatten_environment(section_value, prefix, &mut output);
        }
        Ok(output)
    }
}

fn validate_router(router: Option<&RouterTunables>) -> Result<()> {
    let router = router.cloned().unwrap_or_default();
    for (name, value, minimum, maximum) in [
        ("workers", router.workers, 1, 256),
        ("listener_tasks_per_fd", router.listener_tasks_per_fd, 1, 64),
        ("keepalive_pool_size", router.keepalive_pool_size, 1, 65_536),
        (
            "h2_max_concurrent_streams",
            router.h2_max_concurrent_streams.map(|value| value as usize),
            1,
            65_535,
        ),
        ("max_table_entries", router.max_table_entries, 1, 1_048_576),
        ("edge_replay_max_nonces", router.edge_replay_max_nonces, 1, 1_048_576),
        ("upstream_max_h2_streams", router.upstream_max_h2_streams, 1, 65_535),
    ] {
        if value.is_some_and(|value| !(minimum..=maximum).contains(&value)) {
            bail!("router.{name} must be between {minimum} and {maximum}");
        }
    }
    let downstream_stream_window = router.h2_stream_window_bytes.unwrap_or(1_048_576);
    let downstream_connection_window = router.h2_connection_window_bytes.unwrap_or(16_777_216);
    validate_h2_window("router.h2_stream_window_bytes", downstream_stream_window)?;
    validate_h2_window("router.h2_connection_window_bytes", downstream_connection_window)?;
    if downstream_connection_window < downstream_stream_window {
        bail!("router.h2_connection_window_bytes must not be less than h2_stream_window_bytes");
    }
    let upstream_stream_window = router.upstream_stream_window_bytes.unwrap_or(1_048_576);
    let upstream_connection_window = router.upstream_connection_window_bytes.unwrap_or(16_777_216);
    validate_h2_window("router.upstream_stream_window_bytes", upstream_stream_window)?;
    validate_h2_window("router.upstream_connection_window_bytes", upstream_connection_window)?;
    if upstream_connection_window < upstream_stream_window {
        bail!("router.upstream_connection_window_bytes must not be less than upstream_stream_window_bytes");
    }

    for (name, value, minimum, maximum) in [
        ("h2_idle_timeout_seconds", router.h2_idle_timeout_seconds, 1, 86_400),
        ("sql_connect_timeout_ms", router.sql_connect_timeout_ms, 1, 600_000),
        ("sql_command_timeout_ms", router.sql_command_timeout_ms, 1, 600_000),
        ("refresh_interval_ms", router.refresh_interval_ms, 1, 3_600_000),
        (
            "upstream_connection_timeout_ms",
            router.upstream_connection_timeout_ms,
            1,
            600_000,
        ),
        (
            "upstream_total_connection_timeout_ms",
            router.upstream_total_connection_timeout_ms,
            1,
            600_000,
        ),
        (
            "upstream_idle_timeout_seconds",
            router.upstream_idle_timeout_seconds,
            1,
            86_400,
        ),
    ] {
        if value.is_some_and(|value| !(minimum..=maximum).contains(&value)) {
            bail!("router.{name} must be between {minimum} and {maximum}");
        }
    }
    let connect_timeout = router.upstream_connection_timeout_ms.unwrap_or(1_000);
    if router.upstream_total_connection_timeout_ms.unwrap_or(3_000) < connect_timeout {
        bail!("router.upstream_total_connection_timeout_ms must be at least upstream_connection_timeout_ms");
    }
    if router.upstream_ping_interval_seconds.is_some_and(|value| value > 3_600) {
        bail!("router.upstream_ping_interval_seconds must be 0 (disabled) or at most 3600");
    }
    for (name, value) in [
        ("max_snapshot_age_ms", router.max_snapshot_age_ms),
        ("max_future_skew_ms", router.max_future_skew_ms),
        ("max_oldest_buffer_age_ms", router.max_oldest_buffer_age_ms),
        ("warmup_ms", router.warmup_ms),
        ("lease_ttl_ms", router.lease_ttl_ms),
        ("oat_run_pin_ttl_ms", router.oat_run_pin_ttl_ms),
        ("readiness_max_staleness_ms", router.readiness_max_staleness_ms),
    ] {
        if value.is_some_and(|value| value > 86_400_000) {
            bail!("router.{name} must not exceed 86400000 milliseconds");
        }
    }
    for (name, value) in [
        ("hard_pressure_bps", router.hard_pressure_bps),
        ("minimum_warmup_bps", router.minimum_warmup_bps),
    ] {
        if value.is_some_and(|value| value > 10_000) {
            bail!("router.{name} must not exceed 10000 basis points");
        }
    }
    Ok(())
}

fn validate_wal(wal: Option<&WalTunables>) -> Result<()> {
    let Some(wal) = wal else {
        return Ok(());
    };
    if wal
        .flush_interval_ms
        .is_some_and(|value| value > i64::MAX as u64 / 1_000_000)
    {
        bail!("wal.flush_interval_ms exceeds the consumer duration range");
    }
    positive("wal.retention_size_cap_bytes", wal.retention_size_cap_bytes)?;
    if wal
        .retention_size_cap_bytes
        .is_some_and(|value| value > i64::MAX as u64)
    {
        bail!("wal.retention_size_cap_bytes exceeds the consumer byte-size range");
    }
    if wal
        .max_age_secs
        .is_some_and(|value| value == 0 || value > i64::MAX as u64 / 1_000_000_000)
    {
        bail!("wal.max_age_secs must be positive and fit the consumer duration range");
    }
    for (name, value) in [
        ("max_segment_open_duration_ms", wal.max_segment_open_duration_ms),
        ("poll_interval_ms", wal.poll_interval_ms),
        ("initial_retry_interval_ms", wal.initial_retry_interval_ms),
        ("max_retry_interval_ms", wal.max_retry_interval_ms),
    ] {
        if value.is_some_and(|value| value == 0 || value > i64::MAX as u64 / 1_000_000) {
            bail!("wal.{name} must be positive and fit the consumer duration range");
        }
    }
    if wal.initial_retry_interval_ms.unwrap_or(250) > wal.max_retry_interval_ms.unwrap_or(2_000) {
        bail!("wal.initial_retry_interval_ms must not exceed max_retry_interval_ms");
    }
    if wal
        .retry_multiplier
        .is_some_and(|value| !value.is_finite() || value <= 0.0)
    {
        bail!("wal.retry_multiplier must be finite and positive");
    }
    if wal
        .max_in_flight
        .is_some_and(|value| value == 0 || value > (usize::MAX >> 3))
    {
        bail!("wal.max_in_flight must be positive and not exceed Semaphore::MAX_PERMITS");
    }
    Ok(())
}

fn project_wal(wal: &WalTunables, output: &mut BTreeMap<String, String>) {
    let mut insert_duration = |key: &str, value: Option<u64>| {
        if let Some(value) = value {
            let duration = humantime::format_duration(Duration::from_millis(value)).to_string();
            output.insert(key.to_owned(), duration);
        }
    };
    insert_duration("ARROWCOL_WAL_FLUSH_INTERVAL", wal.flush_interval_ms);
    insert_duration(
        "ARROWCOL_WAL_MAX_SEGMENT_OPEN_DURATION",
        wal.max_segment_open_duration_ms,
    );
    insert_duration("ARROWCOL_WAL_POLL_INTERVAL", wal.poll_interval_ms);
    insert_duration("ARROWCOL_WAL_INITIAL_RETRY_INTERVAL", wal.initial_retry_interval_ms);
    insert_duration("ARROWCOL_WAL_MAX_RETRY_INTERVAL", wal.max_retry_interval_ms);
    if let Some(bytes) = wal.retention_size_cap_bytes {
        output.insert("ARROWCOL_WAL_RETENTION_SIZE_CAP".to_owned(), format_byte_size(bytes));
    }
    if let Some(seconds) = wal.max_age_secs {
        output.insert(
            "ARROWCOL_WAL_MAX_AGE".to_owned(),
            humantime::format_duration(Duration::from_secs(seconds)).to_string(),
        );
    }
    if let Some(policy) = wal.size_cap_policy {
        let value = match policy {
            WalSizeCapPolicy::Backpressure => "backpressure",
            WalSizeCapPolicy::DropOldest => "drop_oldest",
        };
        output.insert("ARROWCOL_WAL_SIZE_CAP_POLICY".to_owned(), value.to_owned());
    }
    if let Some(multiplier) = wal.retry_multiplier {
        output.insert("ARROWCOL_WAL_RETRY_MULTIPLIER".to_owned(), multiplier.to_string());
    }
    if let Some(value) = wal.max_in_flight {
        output.insert("ARROWCOL_WAL_MAX_IN_FLIGHT".to_owned(), value.to_string());
    }
    if let Some(handling) = wal.otlp_handling {
        let value = match handling {
            WalOtlpHandling::PassThrough => "pass_through",
            WalOtlpHandling::ConvertToArrow => "convert_to_arrow",
        };
        output.insert("ARROWCOL_WAL_OTLP_HANDLING".to_owned(), value.to_owned());
    }
}

fn format_byte_size(bytes: u64) -> String {
    for (unit, multiplier) in [("GiB", 1_u64 << 30), ("MiB", 1_u64 << 20), ("KiB", 1_u64 << 10)] {
        if bytes.is_multiple_of(multiplier) {
            return format!("{} {unit}", bytes / multiplier);
        }
    }
    format!("{bytes} B")
}

fn validate_h2_window(field: &str, window: u32) -> Result<()> {
    if window == 0 || window > 2_147_483_647 {
        bail!("{field} must be between 1 and 2147483647 bytes");
    }
    Ok(())
}

fn validate_duration(name: &str, value: Option<&str>, default: &str) -> Result<Duration> {
    let value = value.unwrap_or(default);
    let duration =
        humantime::parse_duration(value).with_context(|| format!("{name} must be a valid humantime duration"))?;
    if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
        bail!("{name} must be positive and representable by Instant");
    }
    if duration.as_millis() > u64::MAX as u128 {
        bail!("{name} must fit a u64 millisecond duration");
    }
    Ok(duration)
}

fn validate_millisecond_duration(name: &str, value: Option<&str>, default: &str) -> Result<Duration> {
    let duration = validate_duration(name, value, default)?;
    if duration.as_millis() == 0 {
        bail!("{name} must be at least 1 millisecond");
    }
    Ok(duration)
}

fn canonicalize_optional_duration(value: &mut Option<String>, name: &str) -> Result<()> {
    if let Some(value) = value {
        let duration = validate_duration(name, Some(value), "1s")?;
        *value = humantime::format_duration(duration).to_string();
    }
    Ok(())
}

fn validate_collector_controls(collector: &CollectorTunables) -> Result<()> {
    if let Some(catalog) = &collector.catalog {
        validate_millisecond_duration(
            "collector.catalog.maximum_cache_age",
            catalog.maximum_cache_age.as_deref(),
            "10s",
        )?;
    }
    if let Some(routing) = &collector.routing {
        validate_millisecond_duration(
            "collector.routing.connect_timeout",
            routing.connect_timeout.as_deref(),
            "15s",
        )?;
        validate_millisecond_duration(
            "collector.routing.command_timeout",
            routing.command_timeout.as_deref(),
            "30s",
        )?;
    }
    if let Some(exporter) = &collector.exporter {
        validate_millisecond_duration(
            "collector.exporter.authority_timeout",
            exporter.authority_timeout.as_deref(),
            "10s",
        )?;
    }
    Ok(())
}

fn validate_batching(batching: Option<&BatchingTunables>) -> Result<()> {
    let batching = batching.cloned().unwrap_or_default();
    let global = batching.global_capacity.unwrap_or_default();
    let per_table = batching.per_table_capacity.unwrap_or_default();
    let global_bytes = global.bytes.unwrap_or(2_147_483_648);
    let global_rows = global.rows.unwrap_or(10_000_000);
    let global_batches = global.batches.unwrap_or(4_096);
    let table_bytes = per_table.bytes.unwrap_or(536_870_912);
    let table_rows = per_table.rows.unwrap_or(2_000_000);
    let table_batches = per_table.batches.unwrap_or(1_024);
    for (name, value) in [
        ("collector.batching.global_capacity.bytes", global_bytes),
        ("collector.batching.global_capacity.rows", global_rows),
        ("collector.batching.global_capacity.batches", global_batches),
        ("collector.batching.per_table_capacity.bytes", table_bytes),
        ("collector.batching.per_table_capacity.rows", table_rows),
        ("collector.batching.per_table_capacity.batches", table_batches),
    ] {
        positive(name, Some(value))?;
    }
    if table_bytes > global_bytes || table_rows > global_rows || table_batches > global_batches {
        bail!("collector.batching per_table_capacity must not exceed global_capacity");
    }

    let flush_bytes = batching.max_arrow_memory_per_flush.unwrap_or(536_870_912);
    let flush_rows = batching.max_rows_per_flush.unwrap_or(1_000_000);
    let flush_batches = batching.max_batches_per_flush.unwrap_or(512);
    positive("collector.batching.max_arrow_memory_per_flush", Some(flush_bytes))?;
    positive("collector.batching.max_rows_per_flush", Some(flush_rows))?;
    positive("collector.batching.max_batches_per_flush", Some(flush_batches))?;
    let flushes = batching.max_concurrent_flushes.unwrap_or(4);
    positive("collector.batching.max_concurrent_flushes", Some(flushes))?;
    if flushes > (usize::MAX >> 3) {
        bail!("collector.batching.max_concurrent_flushes exceeds Semaphore::MAX_PERMITS");
    }
    validate_duration("collector.batching.max_age", batching.max_age.as_deref(), "30s")?;
    if flush_bytes > u64::from(table_bytes)
        || flush_rows > u64::from(table_rows)
        || flush_batches > u64::from(table_batches)
    {
        bail!("collector.batching flush limits must not exceed per_table_capacity");
    }

    let adaptive = batching.adaptive_file.unwrap_or_default();
    let target = adaptive.target_parquet_bytes.unwrap_or(134_217_728);
    let minimum = adaptive.min_input_bytes.unwrap_or(16_777_216);
    let maximum = adaptive.max_input_bytes.unwrap_or(1_073_741_824);
    let cold_ratio = adaptive.cold_start_parquet_ratio.unwrap_or(0.25);
    let ewma = adaptive.ewma_weight.unwrap_or(0.2);
    let minimum_ratio = adaptive.min_parquet_ratio.unwrap_or(0.015625);
    let maximum_ratio = adaptive.max_parquet_ratio.unwrap_or(4.0);
    positive("collector.batching.adaptive_file.target_parquet_bytes", Some(target))?;
    positive("collector.batching.adaptive_file.min_input_bytes", Some(minimum))?;
    positive("collector.batching.adaptive_file.max_input_bytes", Some(maximum))?;
    if minimum > maximum {
        bail!("collector.batching.adaptive_file.min_input_bytes must not exceed max_input_bytes");
    }
    for (name, ratio) in [
        ("cold_start_parquet_ratio", cold_ratio),
        ("ewma_weight", ewma),
        ("min_parquet_ratio", minimum_ratio),
        ("max_parquet_ratio", maximum_ratio),
    ] {
        if !ratio.is_finite() || ratio <= 0.0 {
            bail!("collector.batching.adaptive_file.{name} must be finite and positive");
        }
    }
    if ewma > 0.5 {
        bail!("collector.batching.adaptive_file.ewma_weight must be at most 0.5");
    }
    if minimum_ratio > cold_ratio || cold_ratio > maximum_ratio {
        bail!("collector.batching.adaptive_file ratios must satisfy min <= cold_start <= max");
    }
    Ok(())
}

fn validate_commit(commit: Option<&CommitTunables>) -> Result<()> {
    let commit = commit.cloned().unwrap_or_default();
    validate_duration("collector.commit.batch_window", commit.batch_window.as_deref(), "10s")?;
    let files = commit.max_files_per_commit.unwrap_or(64);
    let pending = commit.max_pending_files.unwrap_or(256);
    positive("collector.commit.max_files_per_commit", Some(files))?;
    positive("collector.commit.max_pending_files", Some(pending))?;
    if files > pending || pending > (usize::MAX >> 3) {
        bail!("collector.commit requires 0 < max_files_per_commit <= max_pending_files <= Semaphore::MAX_PERMITS");
    }
    Ok(())
}

fn validate_limits(limits: Option<&LimitsTunables>) -> Result<()> {
    let limits = limits.cloned().unwrap_or_default();
    let metadata = limits.max_metadata_bytes.unwrap_or(16_384);
    let schema = limits.max_schema_bytes.unwrap_or(1_048_576);
    let message = limits.max_message_bytes.unwrap_or(16_777_216);
    for (name, value) in [
        ("max_metadata_bytes", metadata),
        ("max_schema_bytes", schema),
        ("max_message_bytes", message),
        ("batch_channel_capacity", limits.batch_channel_capacity.unwrap_or(4)),
        (
            "put_result_channel_capacity",
            limits.put_result_channel_capacity.unwrap_or(8),
        ),
        ("max_ddl_bytes", limits.max_ddl_bytes.unwrap_or(65_536)),
    ] {
        positive(&format!("collector.limits.{name}"), Some(value))?;
    }
    if schema > message {
        bail!("collector.limits.max_schema_bytes must not exceed max_message_bytes");
    }
    validate_duration(
        "collector.limits.max_stream_duration",
        limits.max_stream_duration.as_deref(),
        "300s",
    )?;
    validate_duration(
        "collector.limits.admission_timeout",
        limits.admission_timeout.as_deref(),
        "30s",
    )?;
    Ok(())
}

fn validate_pressure(pressure: Option<&PressureTunables>) -> Result<()> {
    let pressure = pressure.cloned().unwrap_or_default();
    let rss = pressure.rss_limit_bytes.unwrap_or(2_147_483_648);
    let buffered = pressure.buffered_bytes_limit.unwrap_or(536_870_912);
    let max_streams = pressure.max_concurrent_streams.unwrap_or(128);
    if max_streams > (usize::MAX >> 3) {
        bail!("collector.pressure.max_concurrent_streams exceeds Semaphore::MAX_PERMITS");
    }
    for (name, value) in [
        (
            "max_concurrent_streams",
            pressure.max_concurrent_streams.unwrap_or(128) as u128,
        ),
        ("rss_limit_bytes", u128::from(rss)),
        ("buffered_bytes_limit", u128::from(buffered)),
        (
            "buffered_rows_limit",
            u128::from(pressure.buffered_rows_limit.unwrap_or(10_000_000)),
        ),
        (
            "buffered_batches_limit",
            u128::from(pressure.buffered_batches_limit.unwrap_or(4_096)),
        ),
    ] {
        positive(&format!("collector.pressure.{name}"), Some(value))?;
        if value > i64::MAX as u128 {
            bail!("collector.pressure.{name} must fit a positive i64 measurement");
        }
    }
    if buffered > rss {
        bail!("collector.pressure.buffered_bytes_limit must not exceed rss_limit_bytes");
    }
    validate_duration(
        "collector.pressure.pressure_interval",
        pressure.pressure_interval.as_deref(),
        "2s",
    )?;
    let heartbeat = validate_millisecond_duration(
        "collector.pressure.heartbeat_interval",
        pressure.heartbeat_interval.as_deref(),
        "5s",
    )?;
    let ttl = validate_millisecond_duration(
        "collector.pressure.registration_ttl",
        pressure.registration_ttl.as_deref(),
        "20s",
    )?;
    if ttl <= heartbeat {
        bail!("collector.pressure.registration_ttl must exceed heartbeat_interval");
    }
    let expiry = std::time::SystemTime::now()
        .checked_add(ttl)
        .context("collector.pressure.registration_ttl expiry is not representable")?;
    let expiry_ms = expiry
        .duration_since(std::time::UNIX_EPOCH)
        .context("collector.pressure.registration_ttl expiry precedes Unix epoch")?
        .as_millis();
    if expiry_ms > u64::MAX as u128 {
        bail!("collector.pressure.registration_ttl expiry must fit UnixMillis");
    }
    Ok(())
}

fn validate_runtime(runtime: Option<&RuntimeTunables>) -> Result<()> {
    let Some(runtime) = runtime else {
        return Ok(());
    };
    for (name, value, minimum, maximum) in [
        ("workers", runtime.workers, 1, 64),
        ("blocking_workers", runtime.blocking_workers, 2, 256),
        ("upload_limit", runtime.upload_limit, 1, 256),
        ("task_limit", runtime.task_limit, 2, 1_024),
        ("read_ahead", runtime.read_ahead, 1, 128),
        ("batch_rows", runtime.batch_rows, 1, 262_144),
        ("cpu_workers", runtime.cpu_workers, 1, 128),
    ] {
        if value.is_some_and(|value| !(minimum..=maximum).contains(&value)) {
            bail!("collector.runtime.{name} must be between {minimum} and {maximum}");
        }
    }
    Ok(())
}

fn validate_maintenance(maintenance: &MaintenanceTunables) -> Result<()> {
    let enabled = maintenance.enabled.unwrap_or(false);
    let has_tunables = serde_json::to_value(maintenance)?.as_object().is_some_and(|object| {
        object.iter().any(|(key, value)| match key.as_str() {
            "enabled" | "apply" => value == true,
            "operations" => value.as_array().is_some_and(|values| !values.is_empty()),
            _ => !value.is_null(),
        })
    });
    if !enabled {
        if has_tunables {
            bail!("maintenance settings require maintenance.enabled=true");
        }
        return Ok(());
    }
    let operations = maintenance
        .operations
        .as_deref()
        .context("maintenance.enabled requires a nonempty operations list")?;
    if operations.is_empty() {
        bail!("maintenance.enabled requires a nonempty operations list");
    }
    if operations.iter().collect::<std::collections::HashSet<_>>().len() != operations.len() {
        bail!("maintenance.operations must not contain duplicates");
    }
    let has_operation = |operation| operations.contains(&operation);
    if maintenance.apply == Some(true) && !enabled {
        bail!("maintenance.apply=true requires maintenance.enabled=true");
    }
    let has_optimize = has_operation(MaintenanceOperation::Optimize);
    let has_purge = has_operation(MaintenanceOperation::Purge);
    let has_vacuum = has_operation(MaintenanceOperation::Vacuum);
    let has_checkpoint = has_operation(MaintenanceOperation::Checkpoint);
    let has_prune_log = has_operation(MaintenanceOperation::PruneLog);
    for (name, present, operation) in [
        (
            "purge_retention_secs",
            maintenance.purge_retention_secs.is_some(),
            has_purge,
        ),
        (
            "vacuum_retention_secs",
            maintenance.vacuum_retention_secs.is_some(),
            has_vacuum,
        ),
        (
            "allow_short_vacuum_retention",
            maintenance.allow_short_vacuum_retention.is_some(),
            has_vacuum,
        ),
        (
            "vacuum_max_deletes",
            maintenance.vacuum_max_deletes.is_some(),
            has_vacuum,
        ),
        (
            "vacuum_max_listed_files",
            maintenance.vacuum_max_listed_files.is_some(),
            has_vacuum,
        ),
        (
            "checkpoint_every_commits",
            maintenance.checkpoint_every_commits.is_some(),
            has_checkpoint,
        ),
        (
            "log_retention_secs",
            maintenance.log_retention_secs.is_some(),
            has_prune_log,
        ),
        ("max_prune_files", maintenance.max_prune_files.is_some(), has_prune_log),
        ("target_file_mib", maintenance.target_file_mib.is_some(), has_optimize),
        ("small_file_bytes", maintenance.small_file_bytes.is_some(), has_optimize),
        ("min_files", maintenance.min_files.is_some(), has_optimize),
        ("layout_enabled", maintenance.layout_enabled.is_some(), has_optimize),
        (
            "layout_sort_columns",
            maintenance.layout_sort_columns.is_some(),
            has_optimize,
        ),
        (
            "layout_bloom_columns",
            maintenance.layout_bloom_columns.is_some(),
            has_optimize,
        ),
        ("layout_max_rows", maintenance.layout_max_rows.is_some(), has_optimize),
        (
            "layout_max_input_bytes",
            maintenance.layout_max_input_bytes.is_some(),
            has_optimize,
        ),
        (
            "layout_row_group_rows",
            maintenance.layout_row_group_rows.is_some(),
            has_optimize,
        ),
        ("layout_page_rows", maintenance.layout_page_rows.is_some(), has_optimize),
        (
            "input_read_strategy",
            maintenance.input_read_strategy.is_some(),
            has_optimize,
        ),
        (
            "max_concurrent_decoded_bytes",
            maintenance.max_concurrent_decoded_bytes.is_some(),
            has_optimize,
        ),
        (
            "max_files_per_bin",
            maintenance.max_files_per_bin.is_some(),
            has_optimize,
        ),
        (
            "max_bins_per_commit",
            maintenance.max_bins_per_commit.is_some(),
            has_optimize,
        ),
        (
            "max_actions_per_commit",
            maintenance.max_actions_per_commit.is_some(),
            has_optimize,
        ),
        ("max_plan_bytes", maintenance.max_plan_bytes.is_some(), has_optimize),
        (
            "max_decoded_bytes_per_bin",
            maintenance.max_decoded_bytes_per_bin.is_some(),
            has_optimize,
        ),
        (
            "max_row_group_bytes",
            maintenance.max_row_group_bytes.is_some(),
            has_optimize,
        ),
        ("max_batch_bytes", maintenance.max_batch_bytes.is_some(), has_optimize),
        ("batch_rows", maintenance.batch_rows.is_some(), has_optimize),
        (
            "output_row_group_rows",
            maintenance.output_row_group_rows.is_some(),
            has_optimize,
        ),
        (
            "max_output_file_bytes",
            maintenance.max_output_file_bytes.is_some(),
            has_optimize,
        ),
        ("max_active_files", maintenance.max_active_files.is_some(), has_optimize),
        (
            "latest_partitions",
            maintenance.latest_partitions.is_some(),
            has_optimize,
        ),
        ("sealed_final", maintenance.sealed_final.is_some(), has_optimize),
    ] {
        if present && !operation {
            bail!("maintenance.{name} requires its corresponding operation");
        }
    }

    positive(
        "maintenance.max_idle_backoff_secs",
        maintenance.max_idle_backoff_secs.or(Some(3_600)),
    )?;
    positive("maintenance.max_cycles", maintenance.max_cycles)?;
    let concurrent_tables = maintenance.max_concurrent_tables.unwrap_or(1);
    positive("maintenance.max_concurrent_tables", Some(concurrent_tables))?;
    if concurrent_tables > 16 {
        bail!("maintenance.max_concurrent_tables must be between 1 and 16");
    }
    positive(
        "maintenance.operation_timeout_secs",
        maintenance.operation_timeout_secs.or(Some(900)),
    )?;
    positive(
        "maintenance.max_concurrent_decoded_bytes",
        maintenance.max_concurrent_decoded_bytes,
    )?;
    positive("maintenance.commit_attempts", maintenance.commit_attempts.or(Some(8)))?;
    positive(
        "maintenance.commit_budget_secs",
        maintenance.commit_budget_secs.or(Some(10)),
    )?;
    if maintenance.commit_attempts.is_some_and(|attempts| attempts > 1_000) {
        bail!("maintenance.commit_attempts must not exceed 1000");
    }
    if maintenance.commit_budget_secs.is_some_and(|seconds| seconds > 3_600) {
        bail!("maintenance.commit_budget_secs must not exceed 3600 seconds");
    }
    let idle_backoff = maintenance.max_idle_backoff_secs.unwrap_or(3_600);
    if maintenance
        .interval_secs
        .is_some_and(|interval| interval > idle_backoff)
    {
        bail!("maintenance.max_idle_backoff_secs must be at least interval_secs");
    }
    let catalog_threads = maintenance.catalog_runtime_threads.unwrap_or(2);
    if !(1..=64).contains(&catalog_threads) {
        bail!("maintenance.catalog_runtime_threads must be between 1 and 64");
    }
    let catalog_timeout = maintenance.catalog_rpc_timeout_secs.unwrap_or(30);
    if !(1..=3_600).contains(&catalog_timeout) {
        bail!("maintenance.catalog_rpc_timeout_secs must be between 1 and 3600");
    }
    let catalog_message_bytes = maintenance.catalog_max_decoding_message_bytes.unwrap_or(33_554_432);
    if !(1_024..=1_073_741_824).contains(&catalog_message_bytes) {
        bail!("maintenance.catalog_max_decoding_message_bytes must be between 1024 and 1073741824");
    }
    if let Some(budget) = maintenance.max_concurrent_decoded_bytes {
        let concurrency = u64::try_from(maintenance.max_concurrent_tables.unwrap_or(1))
            .context("maintenance.max_concurrent_tables does not fit u64")?;
        let required = maintenance
            .max_decoded_bytes_per_bin
            .unwrap_or(1_073_741_824)
            .checked_mul(concurrency)
            .context("maintenance concurrent decoded-byte budget overflows u64")?;
        if required > budget {
            bail!(
                "maintenance.max_concurrent_decoded_bytes must cover max_decoded_bytes_per_bin times max_concurrent_tables"
            );
        }
    }
    positive("maintenance.interval_secs", maintenance.interval_secs)?;
    positive("maintenance.purge_retention_secs", maintenance.purge_retention_secs)?;
    positive(
        "maintenance.vacuum_retention_secs",
        maintenance.vacuum_retention_secs.or(Some(604_800)),
    )?;
    if maintenance.min_files.is_some_and(|value| value < 2) {
        bail!("maintenance.min_files must be at least 2");
    }
    positive("maintenance.vacuum_max_deletes", maintenance.vacuum_max_deletes)?;
    positive(
        "maintenance.vacuum_max_listed_files",
        maintenance.vacuum_max_listed_files,
    )?;
    positive("maintenance.layout_max_rows", maintenance.layout_max_rows)?;
    positive("maintenance.layout_max_input_bytes", maintenance.layout_max_input_bytes)?;
    positive("maintenance.layout_row_group_rows", maintenance.layout_row_group_rows)?;
    positive("maintenance.layout_page_rows", maintenance.layout_page_rows)?;
    if maintenance.max_files_per_bin.is_some_and(|value| value < 2) {
        bail!("maintenance.max_files_per_bin must be at least 2");
    }
    positive(
        "maintenance.max_files_per_bin",
        maintenance.max_files_per_bin.or(Some(64)),
    )?;
    positive(
        "maintenance.max_bins_per_commit",
        maintenance.max_bins_per_commit.or(Some(16)),
    )?;
    let max_actions = maintenance.max_actions_per_commit.unwrap_or(2_048);
    if max_actions < 3 {
        bail!("maintenance.max_actions_per_commit must be at least 3");
    }
    positive("maintenance.max_actions_per_commit", Some(max_actions))?;
    let max_plan = maintenance.max_plan_bytes.unwrap_or(1_048_576);
    if max_plan < 1_024 {
        bail!("maintenance.max_plan_bytes must be at least 1024");
    }
    positive("maintenance.max_plan_bytes", Some(max_plan))?;
    positive(
        "maintenance.max_decoded_bytes_per_bin",
        maintenance.max_decoded_bytes_per_bin.or(Some(1_073_741_824)),
    )?;
    positive(
        "maintenance.max_row_group_bytes",
        maintenance.max_row_group_bytes.or(Some(134_217_728)),
    )?;
    positive(
        "maintenance.max_batch_bytes",
        maintenance.max_batch_bytes.or(Some(67_108_864)),
    )?;
    positive("maintenance.batch_rows", maintenance.batch_rows.or(Some(8_192)))?;
    positive(
        "maintenance.output_row_group_rows",
        maintenance.output_row_group_rows.or(Some(65_536)),
    )?;
    positive(
        "maintenance.max_output_file_bytes",
        maintenance.max_output_file_bytes.or(Some(1_072_693_248)),
    )?;
    positive(
        "maintenance.max_active_files",
        maintenance.max_active_files.or(Some(1_000_000)),
    )?;
    positive("maintenance.latest_partitions", maintenance.latest_partitions)?;
    positive(
        "maintenance.checkpoint_every_commits",
        maintenance.checkpoint_every_commits.or(Some(100)),
    )?;
    positive("maintenance.log_retention_secs", maintenance.log_retention_secs)?;
    positive(
        "maintenance.max_prune_files",
        maintenance.max_prune_files.or(Some(10_000)),
    )?;
    for (name, value) in [
        (
            "maintenance.purge_max_files_per_commit",
            maintenance.purge_max_files_per_commit.unwrap_or(10_000),
        ),
        (
            "maintenance.vacuum_delete_chunk",
            maintenance.vacuum_delete_chunk.unwrap_or(256),
        ),
        (
            "maintenance.replay_max_log_files",
            maintenance.replay_max_log_files.unwrap_or(100_000),
        ),
        (
            "maintenance.operation_max_active_files",
            maintenance.operation_max_active_files.unwrap_or(1_000_000),
        ),
        (
            "maintenance.discovery_max_log_files",
            maintenance.discovery_max_log_files.unwrap_or(100_000),
        ),
    ] {
        positive(name, Some(value))?;
    }
    for (name, value) in [
        (
            "maintenance.purge_max_plan_bytes",
            maintenance.purge_max_plan_bytes.unwrap_or(4_194_304),
        ),
        (
            "maintenance.vacuum_max_plan_bytes",
            maintenance.vacuum_max_plan_bytes.unwrap_or(8_388_608),
        ),
    ] {
        if value < 1_024 {
            bail!("{name} must be at least 1024 bytes");
        }
        positive(name, Some(value))?;
    }
    for (name, value) in [
        (
            "maintenance.replay_max_log_file_bytes",
            maintenance.replay_max_log_file_bytes.unwrap_or(67_108_864),
        ),
        (
            "maintenance.replay_max_checkpoint_bytes",
            maintenance.replay_max_checkpoint_bytes.unwrap_or(268_435_456),
        ),
        (
            "maintenance.discovery_max_log_file_bytes",
            maintenance.discovery_max_log_file_bytes.unwrap_or(67_108_864),
        ),
    ] {
        positive(name, Some(value))?;
    }

    let target_mib = maintenance.target_file_mib.unwrap_or(128);
    if !(1..=1_023).contains(&target_mib) {
        bail!("maintenance.target_file_mib must be between 1 and 1023");
    }
    let target_bytes = target_mib
        .checked_mul(1_048_576)
        .context("maintenance.target_file_mib overflows byte conversion")?;
    let small_bytes = maintenance
        .small_file_bytes
        .unwrap_or_else(|| (target_bytes / 2).max(1));
    positive("maintenance.small_file_bytes", Some(small_bytes))?;
    if small_bytes > target_bytes {
        bail!("maintenance.small_file_bytes must not exceed target_file_mib converted to bytes");
    }
    let max_output_bytes = maintenance.max_output_file_bytes.unwrap_or(1_072_693_248);
    if max_output_bytes < target_bytes || max_output_bytes >= 1_073_741_824 {
        bail!("maintenance.max_output_file_bytes must be at least target_file_mib and less than 1073741824 bytes");
    }
    if let Some(columns) = &maintenance.layout_sort_columns {
        validate_column_list("maintenance.layout_sort_columns", columns, true)?;
    }
    if let Some(columns) = &maintenance.layout_bloom_columns {
        validate_column_list("maintenance.layout_bloom_columns", columns, false)?;
    }
    if maintenance.layout_max_rows.is_some_and(|rows| rows > u32::MAX as usize) {
        bail!("maintenance.layout_max_rows must not exceed 4294967295");
    }
    if u128::from(maintenance.max_batch_bytes.unwrap_or(67_108_864) as u64)
        > u128::from(maintenance.max_decoded_bytes_per_bin.unwrap_or(1_073_741_824))
    {
        bail!("maintenance.max_batch_bytes must not exceed max_decoded_bytes_per_bin");
    }
    let vacuum_retention = maintenance.vacuum_retention_secs.unwrap_or(604_800);
    if vacuum_retention < 604_800 && maintenance.allow_short_vacuum_retention != Some(true) {
        bail!("vacuum_retention_secs below seven days requires allow_short_vacuum_retention=true");
    }
    if maintenance.allow_short_vacuum_retention == Some(true) && vacuum_retention >= 604_800 {
        bail!("allow_short_vacuum_retention only applies below seven days");
    }
    Ok(())
}

fn validate_column_list(field: &str, columns: &[String], nonempty: bool) -> Result<()> {
    if nonempty && columns.is_empty() {
        bail!("{field} must not be empty");
    }
    let mut seen = std::collections::BTreeSet::new();
    for column in columns {
        if column.trim().is_empty() || column.trim() != column || column.contains(',') {
            bail!("{field} entries must be nonempty, trimmed column names without commas");
        }
        if !seen.insert(column) {
            bail!("{field} entries must be unique");
        }
    }
    Ok(())
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

fn insert_json_config<T: Serialize>(key: &str, config: &T, output: &mut BTreeMap<String, String>) -> Result<()> {
    let value = canonicalize(serde_json::to_value(config)?);
    if has_nonempty_value(&value) {
        output.insert(key.to_owned(), value.to_string());
    }
    Ok(())
}

fn has_nonempty_value(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.values().any(has_nonempty_value),
        Value::Array(values) => !values.is_empty() && values.iter().any(has_nonempty_value),
        Value::Null => false,
        _ => true,
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
            Value::Array(values) => {
                let joined = values
                    .iter()
                    .filter_map(|value| match value {
                        Value::String(value) => Some(value.clone()),
                        Value::Number(value) => Some(value.to_string()),
                        Value::Bool(value) => Some(value.to_string()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                output.insert(key, joined);
            }
            _ => {
                output.insert(key, value.as_str().map_or_else(|| value.to_string(), str::to_owned));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    fn error_text<T, E: std::fmt::Display>(result: std::result::Result<T, E>) -> String {
        match result {
            Ok(_) => String::new(),
            Err(error) => error.to_string(),
        }
    }

    use super::*;

    #[test]
    fn strict_writer_config_parsing_and_validation() -> Result<()> {
        assert!(
            error_text(serde_json::from_str::<ParquetWriterConfig>(
                r#"{"defaults":{"compression":"brotliish"}}"#
            ))
            .contains("unknown variant")
        );

        let mut valid: ParquetWriterConfig = serde_json::from_str(
            r#"{"defaults":{"compression":"zstd","compression_level":22},"max_row_group_rows":1000}"#,
        )?;
        valid.validate()?;
        valid.defaults.compression_level = Some(23);
        assert!(error_text(valid.validate()).contains("zstd"));

        for probability in [0.0, 1.0, f64::NAN] {
            let mut invalid = ParquetWriterConfig::default();
            invalid.defaults.bloom_filter_fpp = Some(probability);
            assert!(invalid.validate().is_err());
        }
        Ok(())
    }

    #[test]
    fn parquet_environment_uses_authoritative_json_config() -> Result<()> {
        let tunables = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Optimize]),
                apply: Some(false),
                ..Default::default()
            }),
            parquet_writer: Some(ParquetWriterConfig {
                defaults: ParquetColumnOptions {
                    compression: Some(ParquetCodec::Zstd),
                    compression_level: Some(5),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        let env = tunables.environment_for(ComponentRole::Collector)?;
        let config: ParquetWriterConfig = serde_json::from_str(&env["ARROWCOL_PARQUET_CONFIG"])?;
        assert_eq!(config.defaults.compression, Some(ParquetCodec::Zstd));
        assert_eq!(config.defaults.compression_level, Some(5));
        assert_eq!(env.len(), 1);
        let maintenance_env = tunables.environment_for(ComponentRole::Maintenance)?;
        assert_eq!(
            maintenance_env["ARROWCOL_PARQUET_CONFIG"],
            env["ARROWCOL_PARQUET_CONFIG"]
        );
        assert!(!maintenance_env.contains_key("ARROWCOL_MAINT_ENABLED"));
        assert!(!maintenance_env.contains_key("ARROWCOL_MAINT_APPLY"));
        assert!(!maintenance_env.contains_key("ARROWCOL_MAINT_OPERATIONS"));
        let disabled = Tunables {
            maintenance: Some(MaintenanceTunables::default()),
            ..Default::default()
        };
        assert!(disabled.environment_for(ComponentRole::Maintenance)?.is_empty());
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
        assert!(error_text(invalid.validate()).contains("max_memory_bytes"));
        let invalid = Tunables {
            collector: Some(CollectorTunables {
                pressure: Some(PressureTunables {
                    buffered_bytes_limit: Some(9),
                    rss_limit_bytes: Some(8),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("rss_limit"));
        let invalid = Tunables {
            wal: Some(WalTunables {
                initial_retry_interval_ms: Some(2_001),
                max_retry_interval_ms: Some(2_000),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("initial_retry_interval_ms"));
        let invalid = Tunables {
            transport: Some(TransportTunables {
                stream_window_bytes: Some(16_777_217),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("connection_window_bytes"));
        let invalid = Tunables {
            collector: Some(CollectorTunables {
                batching: Some(BatchingTunables {
                    per_table_capacity: Some(QueueCapacityTunables {
                        rows: Some(10_000_001),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("per_table_capacity"));
        let invalid = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Optimize]),
                small_file_bytes: Some(134_217_729),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("target_file_mib"));
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
        assert!(error_text(invalid.validate()).contains("max_pending_files"));
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
        let config: Tunables = serde_json::from_str(r#"{"wal":{"flush_interval_ms":15},"client":{"batch_rows":4}}"#)?;
        assert_eq!(
            config.canonical_json()?,
            r#"{"client":{"batch_rows":4},"wal":{"flush_interval_ms":15}}"#
        );
        assert_eq!(config.durability_class(), DurabilityClass::FsyncBatched);
        let effective = config.canonical_effective_json(&serde_json::json!({"client":{"batch_rows":8192,"workers":0},"wal":{"flush_interval_ms":0,"retention_size_cap_bytes":4294967296u64}}))?;
        assert_eq!(
            effective,
            r#"{"client":{"batch_rows":4,"workers":0},"wal":{"flush_interval_ms":15,"retention_size_cap_bytes":4294967296}}"#
        );
        let env = config.environment()?;
        assert_eq!(env["ARROWCOL_CLIENT_BATCH_ROWS"], "4");
        assert_eq!(env["ARROWCOL_WAL_FLUSH_INTERVAL"], "15ms");
        assert!(config.validate().is_ok());
        assert_eq!(
            config.environment_for(ComponentRole::Client)?["ARROWCOL_CLIENT_BATCH_ROWS"],
            "4"
        );
        assert_eq!(config.environment_for(ComponentRole::Router)?.len(), 0);
        assert_eq!(
            config.environment_for(ComponentRole::Collector)?["ARROWCOL_WAL_FLUSH_INTERVAL"],
            "15ms"
        );
        assert_eq!(Tunables::default().durability_class(), DurabilityClass::Durable);
        Ok(())
    }

    #[test]
    fn arrow_ipc_has_a_canonical_strict_section() -> Result<()> {
        let config: Tunables = serde_json::from_str(r#"{"arrow_ipc":{"compression":"lz4_frame"}}"#)?;
        config.validate_for_oat()?;
        assert_eq!(
            config.environment_for(ComponentRole::Client)?["ARROWCOL_CLIENT_IPC_COMPRESSION"],
            "lz4_frame"
        );
        assert!(serde_json::from_str::<Tunables>(r#"{"arrow_ipc":{"compression":"brotli"}}"#).is_err());

        let duplicate: Tunables =
            serde_json::from_str(r#"{"arrow_ipc":{"compression":"zstd"},"transport":{"ipc_compression":"zstd"}}"#)?;
        assert!(error_text(duplicate.validate()).contains("not in both sections"));
        Ok(())
    }

    #[test]
    fn router_projection_uses_the_consumer_environment_contract() -> Result<()> {
        let config = Tunables {
            router: Some(RouterTunables {
                workers: Some(4),
                listener_tasks_per_fd: Some(2),
                h2_max_concurrent_streams: Some(512),
                h2_stream_window_bytes: Some(2_097_152),
                h2_connection_window_bytes: Some(8_388_608),
                sql_connect_timeout_ms: Some(8_000),
                refresh_interval_ms: Some(2_000),
                upstream_ping_interval_seconds: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };
        config.validate_for_oat()?;
        let env = config.environment_for(ComponentRole::Router)?;
        assert_eq!(env["ARROWCOL_ROUTER_WORKERS"], "4");
        assert_eq!(env["ARROWCOL_ROUTER_LISTENER_TASKS_PER_FD"], "2");
        assert_eq!(env["ARROWCOL_ROUTER_H2_MAX_CONCURRENT_STREAMS"], "512");
        assert_eq!(env["ARROWCOL_ROUTER_H2_STREAM_WINDOW_BYTES"], "2097152");
        assert_eq!(env["ARROWCOL_ROUTER_H2_CONNECTION_WINDOW_BYTES"], "8388608");
        assert_eq!(env["ARROWCOL_ROUTER_SQL_CONNECT_TIMEOUT_MS"], "8000");
        assert_eq!(env["ARROWCOL_ROUTER_REFRESH_INTERVAL_MS"], "2000");
        assert_eq!(env["ARROWCOL_ROUTER_UPSTREAM_PING_INTERVAL_SECONDS"], "0");
        assert!(!env.contains_key("ARROWCOL_ROUTER_WORKER_THREADS"));

        let invalid = Tunables {
            router: Some(RouterTunables {
                h2_stream_window_bytes: Some(2_097_152),
                h2_connection_window_bytes: Some(1_048_576),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("h2_connection_window_bytes"));
        Ok(())
    }

    #[test]
    fn wal_projection_uses_quiver_names_and_durability_semantics() -> Result<()> {
        let config = Tunables {
            wal: Some(WalTunables {
                flush_interval_ms: Some(0),
                retention_size_cap_bytes: Some(4_294_967_296),
                max_age_secs: Some(60),
                size_cap_policy: Some(WalSizeCapPolicy::DropOldest),
                max_segment_open_duration_ms: Some(1_000),
                poll_interval_ms: Some(100),
                initial_retry_interval_ms: Some(250),
                max_retry_interval_ms: Some(2_000),
                retry_multiplier: Some(2.0),
                max_in_flight: Some(1000),
                otlp_handling: Some(WalOtlpHandling::PassThrough),
            }),
            ..Default::default()
        };
        config.validate_for_oat()?;
        assert_eq!(config.durability_class(), DurabilityClass::LossyEviction);
        assert!(serde_json::from_str::<WalSizeCapPolicy>("\"evict_oldest\"").is_err());
        let env = config.environment_for(ComponentRole::Collector)?;
        assert_eq!(env["ARROWCOL_WAL_FLUSH_INTERVAL"], "0s");
        assert_eq!(env["ARROWCOL_WAL_RETENTION_SIZE_CAP"], "4 GiB");
        assert_eq!(env["ARROWCOL_WAL_MAX_AGE"], "1m");
        assert_eq!(env["ARROWCOL_WAL_SIZE_CAP_POLICY"], "drop_oldest");
        assert_eq!(env["ARROWCOL_WAL_MAX_SEGMENT_OPEN_DURATION"], "1s");
        assert_eq!(env["ARROWCOL_WAL_INITIAL_RETRY_INTERVAL"], "250ms");
        assert_eq!(env["ARROWCOL_WAL_OTLP_HANDLING"], "pass_through");
        assert!(!env.contains_key("ARROWCOL_WAL_FSYNC_INTERVAL_MS"));
        Ok(())
    }

    #[test]
    fn maintenance_budget_and_commit_controls_are_projected_and_validated() -> Result<()> {
        let invalid = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Optimize]),
                max_concurrent_tables: Some(2),
                max_decoded_bytes_per_bin: Some(100),
                max_batch_bytes: Some(50),
                max_concurrent_decoded_bytes: Some(199),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("max_concurrent_decoded_bytes"));

        let tunables = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Optimize]),
                max_concurrent_tables: Some(2),
                max_decoded_bytes_per_bin: Some(100),
                max_batch_bytes: Some(50),
                max_concurrent_decoded_bytes: Some(200),
                commit_attempts: Some(12),
                commit_budget_secs: Some(15),
                input_read_strategy: Some(MaintenanceInputReadStrategy::Spool),
                catalog_runtime_threads: Some(4),
                catalog_rpc_timeout_secs: Some(45),
                catalog_max_decoding_message_bytes: Some(65_536),
                ..Default::default()
            }),
            ..Default::default()
        };
        tunables.validate_for_oat()?;
        let env = tunables.environment_for(ComponentRole::Maintenance)?;
        assert_eq!(env["ARROWCOL_MAINT_MAX_CONCURRENT_DECODED_BYTES"], "200");
        assert_eq!(env["ARROWCOL_MAINT_COMMIT_ATTEMPTS"], "12");
        assert_eq!(env["ARROWCOL_MAINT_COMMIT_BUDGET_SECS"], "15");
        assert_eq!(env["ARROWCOL_MAINT_INPUT_READ_STRATEGY"], "spool");
        let range = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Optimize]),
                input_read_strategy: Some(MaintenanceInputReadStrategy::Range),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            range.environment_for(ComponentRole::Maintenance)?["ARROWCOL_MAINT_INPUT_READ_STRATEGY"],
            "range"
        );
        assert_eq!(env["ARROWCOL_MAINT_CATALOG_RUNTIME_THREADS"], "4");
        assert_eq!(env["ARROWCOL_MAINT_CATALOG_RPC_TIMEOUT_SECS"], "45");
        assert_eq!(env["ARROWCOL_MAINT_CATALOG_MAX_DECODING_MESSAGE_BYTES"], "65536");

        let invalid = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Optimize]),
                commit_attempts: Some(1_001),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("must not exceed 1000"));
        Ok(())
    }

    #[test]
    fn maintenance_is_opt_in_and_requires_explicit_supported_operations() -> Result<()> {
        assert!(serde_json::from_str::<MaintenanceOperation>("\"unsupported\"").is_err());
        let disabled = Tunables {
            maintenance: Some(MaintenanceTunables {
                max_batch_bytes: Some(1_000),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(disabled.validate()).contains("enabled=true"));
        let enabled_without_operations = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(enabled_without_operations.validate()).contains("operations"));
        let supported = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Optimize]),
                ..Default::default()
            }),
            ..Default::default()
        };
        supported.validate_for_oat()?;
        let env = supported.environment_for(ComponentRole::Maintenance)?;
        assert!(env.is_empty());
        Ok(())
    }

    #[test]
    fn collector_projection_uses_consumer_config_blobs_and_runtime_names() -> Result<()> {
        let tunables = Tunables {
            collector: Some(CollectorTunables {
                batching: Some(BatchingTunables {
                    max_age: Some("250ms".to_owned()),
                    global_capacity: Some(QueueCapacityTunables {
                        rows: Some(20_000_000),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                commit: Some(CommitTunables {
                    batch_window: Some("1s".to_owned()),
                    ..Default::default()
                }),
                limits: Some(LimitsTunables {
                    admission_timeout: Some("5s".to_owned()),
                    ..Default::default()
                }),
                pressure: Some(PressureTunables {
                    pressure_interval: Some("3s".to_owned()),
                    ..Default::default()
                }),
                runtime: Some(RuntimeTunables {
                    workers: Some(8),
                    batch_rows: Some(16_384),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let env = tunables.environment_for(ComponentRole::Collector)?;
        let batching: serde_json::Value = serde_json::from_str(&env["ARROWCOL_COLLECTOR_BATCHING_CONFIG"])?;
        assert_eq!(batching["max_age"], "250ms");
        assert_eq!(batching["global_capacity"]["rows"], 20_000_000);
        assert_eq!(env["ARROWCOL_COLLECTOR_COMMIT_CONFIG"], r#"{"batch_window":"1s"}"#);
        assert_eq!(env["ARROWCOL_COLLECTOR_LIMITS_CONFIG"], r#"{"admission_timeout":"5s"}"#);
        assert_eq!(
            env["ARROWCOL_COLLECTOR_PRESSURE_CONFIG"],
            r#"{"pressure_interval":"3s"}"#
        );
        assert_eq!(env["ARROWCOL_COLLECTOR_RUNTIME_WORKERS"], "8");
        assert_eq!(env["ARROWCOL_COLLECTOR_BATCH_ROWS"], "16384");
        Ok(())
    }

    #[test]
    fn standalone_transport_defaults_accept_full_default_flight_batch() {
        let tunables = Tunables {
            transport: Some(TransportTunables {
                max_flight_data_bytes: Some(32_505_856),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(tunables.validate().is_ok());
    }

    #[test]
    fn collector_duration_overrides_are_canonicalized_and_validated() -> Result<()> {
        let tunables = Tunables {
            collector: Some(CollectorTunables {
                batching: Some(BatchingTunables {
                    max_age: Some("1000ms".to_owned()),
                    ..Default::default()
                }),
                catalog: Some(CatalogTunables {
                    maximum_cache_age: Some("10000ms".to_owned()),
                }),
                commit: Some(CommitTunables {
                    batch_window: Some("2500ms".to_owned()),
                    ..Default::default()
                }),
                exporter: Some(ExporterTunables {
                    authority_timeout: Some("10000ms".to_owned()),
                }),
                limits: Some(LimitsTunables {
                    max_stream_duration: Some("60000ms".to_owned()),
                    admission_timeout: Some("5000ms".to_owned()),
                    ..Default::default()
                }),
                pressure: Some(PressureTunables {
                    pressure_interval: Some("2000ms".to_owned()),
                    heartbeat_interval: Some("5000ms".to_owned()),
                    registration_ttl: Some("20000ms".to_owned()),
                    ..Default::default()
                }),
                routing: Some(RoutingTunables {
                    connect_timeout: Some("15000ms".to_owned()),
                    command_timeout: Some("30000ms".to_owned()),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        tunables.validate_for_oat()?;
        let env = tunables.environment_for(ComponentRole::Collector)?;
        let batching: Value = serde_json::from_str(&env["ARROWCOL_COLLECTOR_BATCHING_CONFIG"])?;
        let catalog: Value = serde_json::from_str(&env["ARROWCOL_COLLECTOR_CATALOG_CONFIG"])?;
        let commit: Value = serde_json::from_str(&env["ARROWCOL_COLLECTOR_COMMIT_CONFIG"])?;
        let exporter: Value = serde_json::from_str(&env["ARROWCOL_COLLECTOR_EXPORTER_CONFIG"])?;
        let limits: Value = serde_json::from_str(&env["ARROWCOL_COLLECTOR_LIMITS_CONFIG"])?;
        let pressure: Value = serde_json::from_str(&env["ARROWCOL_COLLECTOR_PRESSURE_CONFIG"])?;
        let routing: Value = serde_json::from_str(&env["ARROWCOL_COLLECTOR_ROUTING_CONFIG"])?;
        assert_eq!(batching["max_age"], "1s");
        assert_eq!(catalog["maximum_cache_age"], "10s");
        assert_eq!(commit["batch_window"], "2s 500ms");
        assert_eq!(exporter["authority_timeout"], "10s");
        assert_eq!(limits["max_stream_duration"], "1m");
        assert_eq!(limits["admission_timeout"], "5s");
        assert_eq!(pressure["pressure_interval"], "2s");
        assert_eq!(pressure["heartbeat_interval"], "5s");
        assert_eq!(pressure["registration_ttl"], "20s");
        assert_eq!(routing["connect_timeout"], "15s");
        assert_eq!(routing["command_timeout"], "30s");

        let invalid = Tunables {
            collector: Some(CollectorTunables {
                pressure: Some(PressureTunables {
                    heartbeat_interval: Some("5s".to_owned()),
                    registration_ttl: Some("5s".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("must exceed heartbeat_interval"));
        let invalid = Tunables {
            collector: Some(CollectorTunables {
                pressure: Some(PressureTunables {
                    registration_ttl: Some("500us".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("at least 1 millisecond"));
        Ok(())
    }

    #[test]
    fn maintenance_operation_limits_are_projected_and_bounded() -> Result<()> {
        let tunables = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Vacuum]),
                purge_max_files_per_commit: Some(32),
                discovery_max_log_file_bytes: Some(4_194_304),
                ..Default::default()
            }),
            ..Default::default()
        };
        tunables.validate_for_oat()?;
        let env = tunables.environment_for(ComponentRole::Maintenance)?;
        assert_eq!(env["ARROWCOL_MAINT_PURGE_MAX_FILES_PER_COMMIT"], "32");
        assert_eq!(env["ARROWCOL_MAINT_DISCOVERY_MAX_LOG_FILE_BYTES"], "4194304");

        let invalid = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Purge]),
                purge_max_plan_bytes: Some(1_023),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("at least 1024 bytes"));
        Ok(())
    }

    #[test]
    fn collector_cross_field_bounds_include_component_defaults() {
        let invalid = Tunables {
            collector: Some(CollectorTunables {
                batching: Some(BatchingTunables {
                    per_table_capacity: Some(QueueCapacityTunables {
                        rows: Some(10_000_001),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("per_table_capacity"));
        let invalid = Tunables {
            collector: Some(CollectorTunables {
                batching: Some(BatchingTunables {
                    max_arrow_memory_per_flush: Some(536_870_913),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("flush limits"));
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
        assert!(error_text(invalid.validate()).contains("max_pending_files"));
        let invalid = Tunables {
            collector: Some(CollectorTunables {
                limits: Some(LimitsTunables {
                    max_schema_bytes: Some(16_777_217),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("max_schema_bytes"));
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
        assert!(error_text(invalid.validate()).contains("mutually exclusive"));
        let invalid = Tunables {
            client: Some(ClientTunables {
                workers: Some(1_025),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("workers"));
        let invalid = Tunables {
            client: Some(ClientTunables {
                entropy: Some(f64::INFINITY),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(error_text(invalid.validate()).contains("entropy"));
    }

    #[test]
    fn capability_catalog_is_populated_and_unique() {
        let names = capability_table()
            .iter()
            .map(|capability| capability.name)
            .collect::<std::collections::BTreeSet<_>>();
        assert!(!capability_table().is_empty());
        assert_eq!(names.len(), capability_table().len());
    }

    #[test]
    fn maintenance_input_read_strategy_is_strict_and_supports_range() {
        assert_eq!(
            serde_json::from_str::<MaintenanceInputReadStrategy>("\"range\""),
            Ok(MaintenanceInputReadStrategy::Range)
        );
        assert!(serde_json::from_str::<MaintenanceInputReadStrategy>("\"invalid\"").is_err());
    }

    #[test]
    fn unknown_fields_are_rejected_in_each_section() {
        assert!(
            error_text(serde_json::from_str::<Tunables>(
                r#"{"client":{"batch_rows":1,"compression":"gzip"}}"#
            ))
            .contains("unknown field")
        );
    }
}
