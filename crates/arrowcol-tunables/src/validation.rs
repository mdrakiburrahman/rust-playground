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
    /// Validates values and verifies every configured section can be projected to process environments.
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
            validate_batching(collector.batching.as_ref())?;
            validate_commit(collector.commit.as_ref())?;
            validate_limits(collector.limits.as_ref())?;
            validate_pressure(collector.pressure.as_ref())?;
            validate_runtime(collector.runtime.as_ref())?;
        }
        if let Some(writer) = &self.parquet_writer {
            writer.validate()?;
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
            validate_maintenance(maintenance)?;
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
            if section == "collector" {
                let collector = self.collector.as_ref().expect("serialized section comes from config");
                if let Some(value) = &collector.batching {
                    insert_json_config("ARROWCOL_COLLECTOR_BATCHING_CONFIG", value, &mut output)?;
                }
                if let Some(value) = &collector.commit {
                    insert_json_config("ARROWCOL_COLLECTOR_COMMIT_CONFIG", value, &mut output)?;
                }
                if let Some(value) = &collector.limits {
                    insert_json_config("ARROWCOL_COLLECTOR_LIMITS_CONFIG", value, &mut output)?;
                }
                if let Some(value) = &collector.pressure {
                    insert_json_config("ARROWCOL_COLLECTOR_PRESSURE_CONFIG", value, &mut output)?;
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
    Ok(duration)
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
    positive(
        "maintenance.max_concurrent_tables",
        maintenance.max_concurrent_tables.or(Some(1)),
    )?;
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
    if maintenance.commit_budget_secs.is_some_and(|seconds| seconds > 86_400) {
        bail!("maintenance.commit_budget_secs must not exceed 86400 seconds");
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
    positive("maintenance.min_files", maintenance.min_files)?;
    positive("maintenance.vacuum_max_deletes", maintenance.vacuum_max_deletes)?;
    positive(
        "maintenance.vacuum_max_listed_files",
        maintenance.vacuum_max_listed_files,
    )?;
    positive("maintenance.layout_max_rows", maintenance.layout_max_rows)?;
    positive("maintenance.layout_max_input_bytes", maintenance.layout_max_input_bytes)?;
    positive("maintenance.layout_row_group_rows", maintenance.layout_row_group_rows)?;
    positive("maintenance.layout_page_rows", maintenance.layout_page_rows)?;
    positive(
        "maintenance.max_files_per_bin",
        maintenance.max_files_per_bin.or(Some(64)),
    )?;
    positive(
        "maintenance.max_bins_per_commit",
        maintenance.max_bins_per_commit.or(Some(16)),
    )?;
    positive(
        "maintenance.max_actions_per_commit",
        maintenance.max_actions_per_commit.or(Some(2_048)),
    )?;
    positive(
        "maintenance.max_plan_bytes",
        maintenance.max_plan_bytes.or(Some(1_048_576)),
    )?;
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

    let target_mib = maintenance.target_file_mib.unwrap_or(128);
    positive("maintenance.target_file_mib", Some(target_mib))?;
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
    use super::*;

    #[test]
    fn strict_writer_config_parsing_and_validation() -> Result<()> {
        let error =
            serde_json::from_str::<ParquetWriterConfig>(r#"{"defaults":{"compression":"brotliish"}}"#).unwrap_err();
        assert!(error.to_string().contains("unknown variant"));

        let mut valid: ParquetWriterConfig = serde_json::from_str(
            r#"{"defaults":{"compression":"zstd","compression_level":22},"max_row_group_rows":1000}"#,
        )?;
        valid.validate()?;
        valid.defaults.compression_level = Some(23);
        assert!(format!("{:#}", valid.validate().unwrap_err()).contains("zstd"));

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
        assert!(invalid.validate().unwrap_err().to_string().contains("max_memory_bytes"));
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
        assert!(
            invalid
                .validate()
                .unwrap_err()
                .to_string()
                .contains("per_table_capacity")
        );
        let invalid = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Optimize]),
                small_file_bytes: Some(134_217_729),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().unwrap_err().to_string().contains("target_file_mib"));
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
    fn maintenance_budget_and_commit_controls_are_projected_and_validated() -> Result<()> {
        let invalid = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Optimize]),
                max_concurrent_tables: Some(2),
                max_decoded_bytes_per_bin: Some(100),
                max_concurrent_decoded_bytes: Some(199),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            invalid
                .validate()
                .unwrap_err()
                .to_string()
                .contains("max_concurrent_decoded_bytes")
        );

        let tunables = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Optimize]),
                max_concurrent_tables: Some(2),
                max_decoded_bytes_per_bin: Some(100),
                max_concurrent_decoded_bytes: Some(200),
                commit_attempts: Some(12),
                commit_budget_secs: Some(15),
                ..Default::default()
            }),
            ..Default::default()
        };
        tunables.validate_for_oat()?;
        let env = tunables.environment_for(ComponentRole::Maintenance)?;
        assert_eq!(env["ARROWCOL_MAINT_MAX_CONCURRENT_DECODED_BYTES"], "200");
        assert_eq!(env["ARROWCOL_MAINT_COMMIT_ATTEMPTS"], "12");
        assert_eq!(env["ARROWCOL_MAINT_COMMIT_BUDGET_SECS"], "15");

        let invalid = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                operations: Some(vec![MaintenanceOperation::Optimize]),
                commit_attempts: Some(1_001),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            invalid
                .validate()
                .unwrap_err()
                .to_string()
                .contains("must not exceed 1000")
        );
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
        assert!(disabled.validate().unwrap_err().to_string().contains("enabled=true"));
        let enabled_without_operations = Tunables {
            maintenance: Some(MaintenanceTunables {
                enabled: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            enabled_without_operations
                .validate()
                .unwrap_err()
                .to_string()
                .contains("operations")
        );
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
        assert!(
            invalid
                .validate()
                .unwrap_err()
                .to_string()
                .contains("per_table_capacity")
        );
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
        assert!(invalid.validate().unwrap_err().to_string().contains("flush limits"));
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
        assert!(invalid.validate().unwrap_err().to_string().contains("max_schema_bytes"));
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
