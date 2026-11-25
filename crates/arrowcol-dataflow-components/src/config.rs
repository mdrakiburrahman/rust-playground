use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

use arrowcol_catalog::{CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, MemoryCatalogStore};
use arrowcol_flight::{RuntimeLimits, RuntimeResult};
use arrowcol_protocol::{IsolationLevel, PositiveMeasurement, UnixMillis};
use arrowcol_routing::{MemoryRoutingStore, RoutingStore, SqlServerRoutingConfig, SqlServerRoutingStore};
use arrowcol_tunables::AgeFlushPolicy;
use serde::{Deserialize, Serialize};

static CLOUD_GENERATION: OnceLock<String> = OnceLock::new();

#[cfg(test)]
mod override_tests {
    use super::*;

    #[test]
    fn receiver_isolation_defaults_to_full_path_and_reports_resolved_level() -> Result<(), serde_json::Error> {
        let mut input = serde_json::json!({
            "listening_addr":"127.0.0.1:8815", "admin_listening_addr":"127.0.0.1:8816",
            "catalog":{"type":"memory"}, "routing":{"type":"memory"},
            "registration":{"instance_key":"test", "flight_endpoint":"127.0.0.1:8815"}
        });
        let config: FlightReceiverConfig = serde_json::from_value(input.clone())?;
        assert_eq!(config.effective_config_json()["isolation_level"], "l5");
        input["isolation_level"] = serde_json::json!("l1");
        let config: FlightReceiverConfig = serde_json::from_value(input.clone())?;
        assert_eq!(config.effective_config_json()["isolation_level"], "l1");
        for invalid in [
            serde_json::Value::Null,
            serde_json::json!({"l1": null}),
            serde_json::json!("L1"),
        ] {
            input["isolation_level"] = invalid;
            assert!(serde_json::from_value::<FlightReceiverConfig>(input.clone()).is_err());
        }
        Ok(())
    }

    #[test]
    fn commit_projection_preserves_yaml_and_reaches_table_options() -> Result<(), String> {
        use arrow_deltalake::{
            CheckpointMode, PreflightLookupMode, SnapshotCacheMode, SnapshotListMode, TxnLookupMode,
        };

        let yaml: CommitConfig = serde_json::from_value(serde_json::json!({"snapshot_cache_mode":"checkpoint_aware"}))
            .map_err(|error| error.to_string())?;
        for (name, mode) in [
            ("external", CheckpointMode::External),
            ("inline", CheckpointMode::Inline),
            ("async", CheckpointMode::Async),
        ] {
            let settings = arrowcol_tunables::Tunables::parse_oat_json(serde_json::json!({
                "collector":{"commit":{"checkpoint_mode":name,"checkpoint_interval":17,"txn_lookup_mode":"incremental",
                    "preflight_lookup_mode":"grouped","snapshot_list_mode":"from_checkpoint","stage_snapshot_mode":"per_group","commit_attempts":3,"commit_budget_secs":2}}
            }))
            .map_err(|error| error.to_string())?;
            let environment = settings
                .environment_for(arrowcol_tunables::ComponentRole::Collector)
                .map_err(|error| error.to_string())?;
            let merged = merge_config(&yaml, &environment["ARROWCOL_COLLECTOR_COMMIT_CONFIG"], None)?;
            let options = merged.build()?;
            assert_eq!(options.snapshot_cache_mode, SnapshotCacheMode::CheckpointAware);
            assert_eq!(options.checkpoint_mode, mode);
            assert_eq!(options.checkpoint_interval, 17);
            assert_eq!(options.txn_lookup_mode, TxnLookupMode::Incremental);
            assert_eq!(options.preflight_lookup_mode, PreflightLookupMode::Grouped);
            assert_eq!(options.snapshot_list_mode, SnapshotListMode::FromCheckpoint);
            assert_eq!(options.stage_snapshot_mode, arrow_deltalake::StageSnapshotMode::PerGroup);
            assert_eq!(options.commit_attempts, 3);
            assert_eq!(options.commit_budget, Duration::from_secs(2));
        }
        let defaults = CommitConfig::default().build()?;
        assert_eq!(
            (defaults.commit_attempts, defaults.commit_budget),
            (8, Duration::from_secs(10))
        );
        for raw in [
            serde_json::json!({"checkpoint_interval": 0}),
            serde_json::json!({"checkpoint_interval": 100_001}),
            serde_json::json!({"commit_attempts": 0}),
            serde_json::json!({"commit_attempts": 1_001}),
            serde_json::json!({"commit_budget_secs": 0}),
            serde_json::json!({"commit_budget_secs": 3_601}),
        ] {
            let raw = raw.to_string();
            assert!(merge_config(&yaml, &raw, None)?.build().is_err(), "{raw}");
        }
        Ok(())
    }

    #[test]
    fn receiver_compression_projects_typed_oat_json_into_actual_runtime_limits() -> Result<(), String> {
        let defaults = RuntimeLimitsConfig::default()
            .build()
            .map_err(|error| error.to_string())?;
        assert_eq!(defaults.grpc_accept_compression, None);
        assert_eq!(defaults.grpc_send_compression, None);
        for (name, expected) in [
            ("none", None),
            ("gzip", Some(tonic::codec::CompressionEncoding::Gzip)),
            ("zstd", Some(tonic::codec::CompressionEncoding::Zstd)),
        ] {
            let tunables = arrowcol_tunables::Tunables::parse_oat_json(serde_json::json!({
                "collector":{"limits":{"grpc_accept_compression":name,"grpc_send_compression":name}}
            }))
            .map_err(|error| error.to_string())?;
            let environment = tunables
                .environment_for(arrowcol_tunables::ComponentRole::Collector)
                .map_err(|error| error.to_string())?;
            let config = merge_config(
                &RuntimeLimitsConfig::default(),
                &environment["ARROWCOL_COLLECTOR_LIMITS_CONFIG"],
                None,
            )?;
            let limits = config.build().map_err(|error| error.to_string())?;
            assert_eq!(limits.grpc_accept_compression, expected);
            assert_eq!(limits.grpc_send_compression, expected);
            assert_eq!(limits.max_message_bytes, defaults.max_message_bytes);
            assert_eq!(limits.batch_channel_capacity, defaults.batch_channel_capacity);
        }
        for field in ["grpc_accept_compression", "grpc_send_compression"] {
            for value in ["brotli", "deflate", "Gzip", ""] {
                let invalid = serde_json::json!({field:value}).to_string();
                assert!(merge_config(&RuntimeLimitsConfig::default(), &invalid, None).is_err());
            }
        }
        Ok(())
    }

    #[test]
    fn nested_override_preserves_yaml_values_and_validates_final_budget() -> Result<(), String> {
        let config = FileBatchConfig {
            max_age: Duration::from_secs(5),
            ..Default::default()
        };
        let merged = merge_config(
            &config,
            r#"{"per_table_capacity":{"rows":1234},"max_rows_per_flush":1000}"#,
            None,
        )?;
        assert_eq!(merged.max_age, config.max_age);
        assert_eq!(merged.per_table_capacity.bytes, config.per_table_capacity.bytes);
        assert_eq!(merged.per_table_capacity.rows, 1234);
        merged.validate()?;
        let invalid = merge_config(&config, r#"{"max_rows_per_flush":0}"#, None)?;
        assert!(invalid.validate().is_err());
        assert!(merge_config(&config, r#"{"max_row_per_flush":10}"#, None).is_err());
        assert!(merge_config(&config, r#"{"per_table_capacity":{"extra":1}}"#, None).is_err());
        assert!(merge_config(&config, "[]", None).is_err());
        let merged = merge_config(&config, r#"{"age_flush_policy":"drain_ready"}"#, None)?;
        assert_eq!(merged.age_flush_policy, AgeFlushPolicy::DrainReady);
        assert!(merge_config(&config, r#"{"age_flush_policy":"drain_forever"}"#, None).is_err());
        assert!(merge_config(&config, r#"{"age_flush_policy":true}"#, None).is_err());
        Ok(())
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn large_byte_capacities_round_trip_through_oat_and_runtime_validation() -> Result<(), String> {
        let values = serde_json::json!({
            "global_capacity":{"bytes":6_000_000_000_u64},
            "per_table_capacity":{"bytes":5_000_000_000_u64}
        });
        let tunables = arrowcol_tunables::Tunables::parse_oat_json(serde_json::json!({
            "collector":{"batching":values}
        }))
        .map_err(|error| error.to_string())?;
        let environment = tunables
            .environment_for(arrowcol_tunables::ComponentRole::Collector)
            .map_err(|error| error.to_string())?;
        let config = merge_config(
            &FileBatchConfig::default(),
            &environment["ARROWCOL_COLLECTOR_BATCHING_CONFIG"],
            None,
        )?;
        config.validate()?;
        assert_eq!(config.global_capacity.bytes, 6_000_000_000);
        assert_eq!(config.per_table_capacity.bytes, 5_000_000_000);

        for invalid in [
            serde_json::json!(0),
            serde_json::json!(arrowcol_tunables::MAX_QUEUE_CAPACITY_BYTES + 1),
            serde_json::json!(1.5),
        ] {
            assert!(
                arrowcol_tunables::Tunables::parse_oat_json(serde_json::json!({
                    "collector":{"batching":{"global_capacity":{"bytes":invalid}}}
                }))
                .is_err()
            );
        }

        let oversized = format!(
            r#"{{"global_capacity":{{"bytes":{}}}}}"#,
            arrowcol_tunables::MAX_QUEUE_CAPACITY_BYTES + 1
        );
        assert!(
            merge_config(&FileBatchConfig::default(), &oversized, None)?
                .validate()
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn adaptive_policy_refresh_interval_validates_the_merged_config() {
        let config = FileBatchConfig::default();
        for (value, valid) in [
            ("\"0s\"", true),
            ("\"30s\"", true),
            ("\"1h\"", true),
            ("\"61m\"", false),
            ("\"-1s\"", false),
            ("\"soon\"", false),
            ("30", false),
            ("null", false),
        ] {
            let json = format!(r#"{{"adaptive_policy_refresh_interval":{value}}}"#);
            let result = merge_config(&config, &json, None).and_then(|merged| merged.validate());
            assert_eq!(result.is_ok(), valid, "{json}");
        }
    }

    #[test]
    fn pending_commit_budget_validates_the_merged_config() {
        let config = FileBatchConfig::default();
        assert_eq!(config.max_pending_commit_bytes, 0);
        let limit = arrowcol_tunables::MAX_QUEUE_CAPACITY_BYTES;
        for (value, valid) in [
            (limit.to_string(), true),
            ((limit + 1).to_string(), false),
            ("null".into(), false),
            ("-1".into(), false),
            ("1.5".into(), false),
            ("\"16\"".into(), false),
        ] {
            let json = format!(r#"{{"max_pending_commit_bytes":{value}}}"#);
            let result = merge_config(&config, &json, None).and_then(|merged| merged.validate());
            assert_eq!(result.is_ok(), valid, "{json}");
        }
    }

    #[test]
    fn stage_file_concurrency_validates_the_merged_batch_policy() {
        let config = FileBatchConfig::default();
        assert_eq!(config.stage_file_concurrency, 1);
        let enabled = FileBatchConfig {
            coalesce_publications: true,
            ..config.clone()
        };
        assert!(
            merge_config(&enabled, r#"{"stage_file_concurrency":16}"#, None)
                .and_then(|merged| merged.validate())
                .is_ok()
        );
        for field in ["stage_file_concurrency", "stage_files_per_group"] {
            for (raw, valid) in [
                ("1", true),
                ("16", false),
                ("0", false),
                ("17", false),
                ("-1", false),
                ("1.5", false),
                ("\"2\"", false),
                ("null", false),
            ] {
                let json = format!(r#"{{"{field}":{raw}}}"#);
                let result = merge_config(&config, &json, None).and_then(|merged| merged.validate());
                assert_eq!(result.is_ok(), valid, "{json}");
            }
        }
    }

    #[test]
    fn channels_reject_capacity_that_would_panic_at_startup() {
        let limits = RuntimeLimitsConfig {
            batch_channel_capacity: usize::MAX,
            ..Default::default()
        };
        assert!(limits.build().is_err());
        let batching = FileBatchConfig {
            max_concurrent_flushes: usize::MAX,
            ..Default::default()
        };
        assert!(batching.validate().is_err());
        for depth in [0, 65] {
            assert!(
                FileBatchConfig {
                    pipeline_depth: depth,
                    coalesce_publications: true,
                    ..Default::default()
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            FileBatchConfig {
                pipeline_depth: 2,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            FileBatchConfig {
                pipeline_depth: 64,
                coalesce_publications: true,
                ..Default::default()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn pressure_overrides_cannot_change_identity() {
        assert!(
            merge_config(
                &serde_json::json!({"instance_key": "original"}),
                r#"{"instance_key":"replacement"}"#,
                Some(&["rss_limit_bytes"]),
            )
            .is_err()
        );
    }

    #[test]
    fn control_plane_overrides_reject_wrong_backends_and_identity_changes() -> Result<(), String> {
        assert!(CatalogConfig::Memory.apply_override("{}").is_err());
        assert!(RoutingStoreConfig::Standalone.apply_override("{}").is_err());
        assert!(RoutingStoreConfig::Memory.apply_override("{}").is_err());
        let mut catalog = CatalogConfig::Remote {
            endpoint: "http://localhost:8817".to_owned(),
            maximum_cache_age: Duration::from_secs(10),
        };
        catalog.apply_override(r#"{"maximum_cache_age":"3s"}"#)?;
        catalog.validate()?;
        assert_eq!(catalog.effective_config_json()["maximum_cache_age"], "3s");
        assert!(catalog.apply_override(r#"{"endpoint":"http://localhost:1"}"#).is_err());
        catalog.apply_override(r#"{"maximum_cache_age":"0s"}"#)?;
        assert!(catalog.validate().is_err());

        let mut routing: RoutingStoreConfig = serde_json::from_value(serde_json::json!({
            "type": "sql_server",
            "connection": {
                "host": "localhost", "port": 1433, "database": "test",
                "username": "test-user", "password_file": "test-password-file"
            }
        }))
        .map_err(|error| error.to_string())?;
        routing.apply_override(r#"{"connect_timeout":"2s"}"#)?;
        routing.validate()?;
        let effective = routing.effective_config_json();
        assert_eq!(effective["connect_timeout"], "2s");
        assert_eq!(effective["command_timeout"], "30s");
        assert_eq!(effective["pool_max_size"], 0);
        assert!(!effective.to_string().contains("password"));
        assert!(routing.apply_override(r#"{"host":"replacement"}"#).is_err());
        routing.apply_override(r#"{"pool_max_size":8,"pool_acquire_timeout":"2s"}"#)?;
        routing.validate()?;
        let effective = routing.effective_config_json();
        assert_eq!(effective["pool_max_size"], 8);
        assert_eq!(effective["pool_acquire_timeout"], "2s");
        for invalid in [
            r#"{"pool_max_size":-1}"#,
            r#"{"pool_max_size":1.5}"#,
            r#"{"pool_max_size":"8"}"#,
            r#"{"pool_size":8}"#,
        ] {
            assert!(routing.apply_override(invalid).is_err(), "{invalid}");
        }
        routing.apply_override(r#"{"pool_max_size":1025}"#)?;
        assert!(routing.validate().is_err());
        routing.apply_override(r#"{"pool_max_size":8,"pool_acquire_timeout":"0s"}"#)?;
        assert!(routing.validate().is_err());
        routing.apply_override(r#"{"pool_acquire_timeout":"601s"}"#)?;
        assert!(routing.validate().is_err());
        routing.apply_override(r#"{"pool_max_size":0}"#)?;
        routing.validate()?;
        routing.apply_override(r#"{"command_timeout":"0s"}"#)?;
        assert!(routing.validate().is_err());
        Ok(())
    }

    #[test]
    fn oat_partial_overrides_reach_the_shipped_collector_baseline() -> Result<(), String> {
        let tunables = arrowcol_tunables::Tunables::parse_oat_json(serde_json::json!({
            "collector":{"pressure":{"heartbeat_interval":"25s","buffered_bytes_limit":3_000_000_000_u64},
                         "batching":{"adaptive_file":{"max_input_bytes":8_388_608}}}
        }))
        .map_err(|error| error.to_string())?;
        let environment = tunables
            .environment_for(arrowcol_tunables::ComponentRole::Collector)
            .map_err(|error| error.to_string())?;
        for template in [
            include_str!("../../../projects/arrowcol-dataflow-rust/config.yaml"),
            include_str!("../../../projects/arrowcol-dataflow-rust/config.quiver.yaml"),
        ] {
            let expanded = otel_arrow_dfe_config::env_substitution::substitute_env_vars(template)
                .map_err(|error| error.to_string())?;
            let pipeline = otel_arrow_dfe_config::engine::OtelDataflowSpec::from_yaml(&expanded)
                .map_err(|error| error.to_string())?;
            let pipeline = serde_json::to_value(pipeline).map_err(|error| error.to_string())?;
            let registration: RegistrationConfig = serde_json::from_value(
                pipeline
                    .pointer("/groups/arrowcol/pipelines/ingest/nodes/flight/config/registration")
                    .ok_or("missing receiver registration")?
                    .clone(),
            )
            .map_err(|error| error.to_string())?;
            let effective: RegistrationConfig =
                merge_config(&registration, &environment["ARROWCOL_COLLECTOR_PRESSURE_CONFIG"], None)?;
            effective.validate()?;
            assert_eq!(effective.heartbeat_interval, Duration::from_secs(25));
            assert_eq!(effective.buffered_bytes_limit, 3_000_000_000);
            let batch: FileBatchConfig = serde_json::from_value(
                pipeline
                    .pointer("/groups/arrowcol/pipelines/ingest/nodes/delta/config/batching")
                    .ok_or("missing exporter batching")?
                    .clone(),
            )
            .map_err(|error| error.to_string())?;
            let effective: FileBatchConfig =
                merge_config(&batch, &environment["ARROWCOL_COLLECTOR_BATCHING_CONFIG"], None)?;
            effective.validate()?;
            assert_eq!(effective.adaptive_file.max_input_bytes, 8_388_608);
        }
        Ok(())
    }

    #[test]
    fn lease_overrides_validate_resolved_ttl_and_clock_bounds() -> Result<(), String> {
        let registration: RegistrationConfig = serde_json::from_value(serde_json::json!({
            "instance_key": "test", "flight_endpoint": "127.0.0.1:8815"
        }))
        .map_err(|error| error.to_string())?;
        let invalid = merge_config(&registration, r#"{"registration_ttl":"4s"}"#, None)?;
        assert!(invalid.validate().is_err());
        let valid = merge_config(
            &registration,
            r#"{"heartbeat_interval":"2s","registration_ttl":"4s"}"#,
            None,
        )?;
        valid.validate()?;
        assert!(validate_duration("deadline", Duration::ZERO).is_err());
        assert!(validate_duration("deadline", Duration::MAX).is_err());
        let sub_millisecond = RegistrationConfig {
            heartbeat_interval: Duration::from_nanos(1),
            registration_ttl: Duration::from_nanos(2),
            ..registration
        };
        assert!(sub_millisecond.validate().is_err());
        Ok(())
    }

    #[test]
    fn exporter_authority_override_is_bounded_and_read_back_without_paths() -> Result<(), String> {
        let mut config = DeltaExporterConfig {
            base_path: std::env::temp_dir(),
            routing: RoutingStoreConfig::Standalone,
            commit: CommitConfig::default(),
            batching: FileBatchConfig::default(),
            authority_timeout: Duration::from_secs(10),
        };
        let current = ExporterOverrides {
            authority_timeout: config.authority_timeout,
        };
        let resolved = merge_config(&current, r#"{"authority_timeout":"250ms"}"#, None)?;
        config.authority_timeout = resolved.authority_timeout;
        config.validate()?;
        assert_eq!(config.effective_config_json()["authority_timeout"], "250ms");
        assert_eq!(
            config.effective_config_json()["batching"]["age_flush_policy"],
            "immediate"
        );
        config.batching = merge_config(&config.batching, r#"{"age_flush_policy":"drain_ready"}"#, None)?;
        config.validate()?;
        assert_eq!(
            config.effective_config_json()["batching"]["age_flush_policy"],
            "drain_ready"
        );
        assert!(config.effective_config_json().get("base_path").is_none());
        assert!(merge_config(&current, r#"{"base_path":"replacement"}"#, None).is_err());
        config.authority_timeout = Duration::MAX;
        assert!(config.validate().is_err());
        Ok(())
    }
}

fn apply_json_environment<T: Serialize + serde::de::DeserializeOwned>(
    config: &mut T,
    name: &str,
    allowed: Option<&[&str]>,
) -> Result<(), String> {
    let Some(json) = environment_json(name)? else {
        return Ok(());
    };
    *config = merge_config(config, &json, allowed).map_err(|error| format!("{name}: {error}"))?;
    Ok(())
}

fn environment_json(name: &str) -> Result<Option<String>, String> {
    match std::env::var(name) {
        Ok(json) => Ok(Some(json)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(format!("{name}: {error}")),
    }
}

fn validate_duration(name: &str, value: Duration) -> Result<(), String> {
    if value.is_zero() || std::time::Instant::now().checked_add(value).is_none() {
        return Err(format!("{name} must be positive and fit clock bounds"));
    }
    duration_millis(value).map_err(|error| format!("{name}: {error}"))?;
    Ok(())
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CatalogOverrides {
    #[serde(with = "humantime_serde")]
    maximum_cache_age: Duration,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RoutingOverrides {
    #[serde(with = "humantime_serde")]
    connect_timeout: Duration,
    #[serde(with = "humantime_serde")]
    command_timeout: Duration,
    pool_max_size: usize,
    #[serde(with = "humantime_serde")]
    pool_acquire_timeout: Duration,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExporterOverrides {
    #[serde(with = "humantime_serde")]
    authority_timeout: Duration,
}

fn merge_config<T: Serialize + serde::de::DeserializeOwned>(
    config: &T,
    json: &str,
    allowed: Option<&[&str]>,
) -> Result<T, String> {
    fn merge(base: &mut serde_json::Value, patch: serde_json::Value) {
        match (base, patch) {
            (serde_json::Value::Object(base), serde_json::Value::Object(patch)) => {
                for (key, value) in patch {
                    if let Some(existing) = base.get_mut(&key) {
                        merge(existing, value);
                    } else {
                        base.insert(key, value);
                    }
                }
            }
            (base, patch) => *base = patch,
        }
    }
    let patch: serde_json::Value = serde_json::from_str(json).map_err(|error| error.to_string())?;
    let object = patch.as_object().ok_or("collector override must be a JSON object")?;
    if let Some(allowed) = allowed {
        for key in object.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(format!("unsupported collector override {key}"));
            }
        }
    }
    let mut base = serde_json::to_value(config).map_err(|error| error.to_string())?;
    merge(&mut base, patch);
    serde_json::from_value(base).map_err(|error| error.to_string())
}

/// Freezes the embedding collector's validated cloud epoch before DFE admission.
///
/// # Errors
/// Rejects an attempted process-epoch change or conflicting initialization.
pub fn configure_cloud_generation(generation: &str) -> Result<(), String> {
    if let Some(existing) = CLOUD_GENERATION.get() {
        if existing != generation {
            return Err("DFE process cloud generation cannot change".to_owned());
        }
        return Ok(());
    }
    CLOUD_GENERATION
        .set(generation.to_owned())
        .map_err(|_| "DFE process cloud generation concurrently initialized".to_owned())
}

pub(crate) fn process_cloud_generation() -> Option<&'static str> {
    CLOUD_GENERATION.get().map(String::as_str)
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(15)
}

fn default_command_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_pool_acquire_timeout() -> Duration {
    Duration::from_secs(5)
}

const MAX_ROUTING_POOL_SIZE: usize = 1_024;
const MAX_ROUTING_POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(600);

fn default_heartbeat_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_pressure_interval() -> Duration {
    Duration::from_secs(2)
}

fn default_registration_ttl() -> Duration {
    Duration::from_secs(20)
}

fn default_max_concurrent_streams() -> usize {
    128
}

fn default_rss_limit_bytes() -> u64 {
    2 * 1_024 * 1_024 * 1_024
}

fn default_buffered_bytes_limit() -> u64 {
    512 * 1_024 * 1_024
}

fn default_buffered_rows_limit() -> u64 {
    10_000_000
}

fn default_buffered_batches_limit() -> u64 {
    4_096
}

fn default_authority_timeout() -> Duration {
    Duration::from_secs(10)
}

/// Shared SQL Server connection settings.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SqlServerConnectionConfig {
    /// SQL Server host.
    pub host: String,
    /// SQL Server TCP port.
    pub port: u16,
    /// Database name.
    pub database: String,
    /// SQL-authenticated user.
    pub username: String,
    /// One-line password file.
    pub password_file: PathBuf,
    /// Local-development certificate bypass.
    #[serde(default)]
    pub trust_server_certificate: bool,
    /// TCP/login timeout.
    #[serde(default = "default_connect_timeout", with = "humantime_serde")]
    pub connect_timeout: Duration,
    /// SQL command timeout.
    #[serde(default = "default_command_timeout", with = "humantime_serde")]
    pub command_timeout: Duration,
    /// Maximum pooled routing connections; 0 opens one connection per operation.
    #[serde(default)]
    pub pool_max_size: usize,
    /// Wait bound for a pooled routing connection when `pool_max_size > 0`.
    #[serde(default = "default_pool_acquire_timeout", with = "humantime_serde")]
    pub pool_acquire_timeout: Duration,
}

impl SqlServerConnectionConfig {
    fn validate(&self) -> Result<(), String> {
        if self.host.is_empty()
            || self.port == 0
            || self.database.is_empty()
            || self.username.is_empty()
            || self.password_file.as_os_str().is_empty()
            || self.connect_timeout.is_zero()
            || self.command_timeout.is_zero()
        {
            return Err(
                "SQL Server host, port, database, username, password file, and timeouts must be set".to_owned(),
            );
        }
        validate_duration("connect_timeout", self.connect_timeout)?;
        validate_duration("command_timeout", self.command_timeout)?;
        if self.pool_max_size > MAX_ROUTING_POOL_SIZE {
            return Err(format!("pool_max_size must be at most {MAX_ROUTING_POOL_SIZE}"));
        }
        if self.pool_max_size > 0 {
            validate_duration("pool_acquire_timeout", self.pool_acquire_timeout)?;
            if self.pool_acquire_timeout > MAX_ROUTING_POOL_ACQUIRE_TIMEOUT {
                return Err(format!(
                    "pool_acquire_timeout must be at most {MAX_ROUTING_POOL_ACQUIRE_TIMEOUT:?}"
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn routing_config(&self) -> arrowcol_routing::RoutingResult<SqlServerRoutingConfig> {
        Ok(SqlServerRoutingConfig::with_password_file(
            self.host.clone(),
            self.port,
            self.database.clone(),
            self.username.clone(),
            &self.password_file,
        )?
        .trust_server_certificate(self.trust_server_certificate)
        .with_timeouts(self.connect_timeout, self.command_timeout))
    }
}

/// Catalog implementation used by the Flight receiver.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CatalogConfig {
    /// Standalone Flight SQL catalog with a bounded CES-backed schema cache.
    Remote {
        /// Flight SQL and CES endpoint, for example http://arrowcol-catalog:8817.
        endpoint: String,
        /// Maximum cache age without confirmed CES high-watermark reconciliation.
        #[serde(default = "default_catalog_cache_age", with = "humantime_serde")]
        maximum_cache_age: Duration,
    },
    /// Empty in-memory catalog, intended for focused development.
    Memory,
}

fn default_catalog_cache_age() -> Duration {
    Duration::from_secs(10)
}

impl CatalogConfig {
    fn apply_override(&mut self, json: &str) -> Result<(), String> {
        match self {
            Self::Remote { maximum_cache_age, .. } => {
                let resolved = merge_config(
                    &CatalogOverrides {
                        maximum_cache_age: *maximum_cache_age,
                    },
                    json,
                    None,
                )?;
                *maximum_cache_age = resolved.maximum_cache_age;
                Ok(())
            }
            Self::Memory => Err("catalog override requires a remote catalog".to_owned()),
        }
    }

    fn apply_environment(&mut self) -> Result<(), String> {
        let name = "ARROWCOL_COLLECTOR_CATALOG_CONFIG";
        if let Some(json) = environment_json(name)? {
            self.apply_override(&json).map_err(|error| format!("{name}: {error}"))?;
        }
        Ok(())
    }

    fn effective_config_json(&self) -> serde_json::Value {
        match self {
            Self::Remote { maximum_cache_age, .. } => serde_json::json!({
                "type": "remote",
                "maximum_cache_age": format!("{maximum_cache_age:?}")
            }),
            Self::Memory => serde_json::json!({"type": "memory"}),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        match self {
            Self::Remote {
                endpoint,
                maximum_cache_age,
            } => {
                if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) || maximum_cache_age.is_zero()
                {
                    return Err(
                        "remote catalog endpoint must be HTTP(S) and maximum_cache_age must be positive".to_owned(),
                    );
                }
                validate_duration("maximum_cache_age", *maximum_cache_age)
            }
            Self::Memory => Ok(()),
        }
    }

    pub(crate) async fn start(
        &self,
    ) -> arrowcol_catalog::CatalogResult<(Arc<dyn CatalogStore>, Arc<dyn FlightSqlDdlHandler>)> {
        match self {
            Self::Remote {
                endpoint,
                maximum_cache_age,
            } => {
                let client =
                    arrowcol_catalog_client::RemoteCatalogClient::connect(endpoint, *maximum_cache_age).await?;
                Ok((
                    Arc::clone(&client) as Arc<dyn CatalogStore>,
                    client as Arc<dyn FlightSqlDdlHandler>,
                ))
            }
            Self::Memory => {
                let store: Arc<dyn CatalogStore> = Arc::new(MemoryCatalogStore::new());
                let (control, _) = CatalogControlPlane::start(DdlParser::default(), Arc::clone(&store)).await?;
                Ok((store, Arc::new(control)))
            }
        }
    }
}

static STANDALONE_ROUTING: LazyLock<Arc<MemoryRoutingStore>> = LazyLock::new(Arc::default);

/// Routing-store implementation shared by admission, fencing, and registration.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RoutingStoreConfig {
    /// Empty process-local routing store, intended only for tests.
    Memory,
    /// One routing store shared by every receiver and exporter in this
    /// process, with table leases claimed on first admission. For a single
    /// collector that owns all of its tables without a routing control plane.
    Standalone,
    /// SQL Server routing control plane.
    SqlServer {
        /// SQL Server connection.
        connection: SqlServerConnectionConfig,
    },
}

impl RoutingStoreConfig {
    fn apply_override(&mut self, json: &str) -> Result<(), String> {
        match self {
            Self::SqlServer { connection } => {
                let resolved = merge_config(
                    &RoutingOverrides {
                        connect_timeout: connection.connect_timeout,
                        command_timeout: connection.command_timeout,
                        pool_max_size: connection.pool_max_size,
                        pool_acquire_timeout: connection.pool_acquire_timeout,
                    },
                    json,
                    None,
                )?;
                connection.connect_timeout = resolved.connect_timeout;
                connection.command_timeout = resolved.command_timeout;
                connection.pool_max_size = resolved.pool_max_size;
                connection.pool_acquire_timeout = resolved.pool_acquire_timeout;
                Ok(())
            }
            Self::Memory | Self::Standalone => Err("routing override requires SQL Server routing".to_owned()),
        }
    }

    fn apply_environment(&mut self) -> Result<(), String> {
        let name = "ARROWCOL_COLLECTOR_ROUTING_CONFIG";
        if let Some(json) = environment_json(name)? {
            self.apply_override(&json).map_err(|error| format!("{name}: {error}"))?;
        }
        Ok(())
    }

    fn effective_config_json(&self) -> serde_json::Value {
        match self {
            Self::SqlServer { connection } => serde_json::json!({
                "type": "sql_server",
                "connect_timeout": format!("{:?}", connection.connect_timeout),
                "command_timeout": format!("{:?}", connection.command_timeout),
                "pool_max_size": connection.pool_max_size,
                "pool_acquire_timeout": format!("{:?}", connection.pool_acquire_timeout)
            }),
            Self::Memory => serde_json::json!({"type": "memory"}),
            Self::Standalone => serde_json::json!({"type": "standalone"}),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        match self {
            Self::Memory | Self::Standalone => Ok(()),
            Self::SqlServer { connection } => connection.validate(),
        }
    }

    pub(crate) const fn claims_leases(&self) -> bool {
        matches!(self, Self::Standalone)
    }

    pub(crate) fn build(&self) -> Result<Arc<dyn RoutingStore>, String> {
        match self {
            Self::Memory => Ok(Arc::new(MemoryRoutingStore::default())),
            Self::Standalone => Ok(Arc::clone(&*STANDALONE_ROUTING) as Arc<dyn RoutingStore>),
            Self::SqlServer { connection } => {
                let config = connection.routing_config().map_err(|error| error.to_string())?;
                let store = if connection.pool_max_size == 0 {
                    SqlServerRoutingStore::new(config)
                } else {
                    let pool = config
                        .create_pool(connection.pool_max_size, connection.pool_acquire_timeout)
                        .map_err(|error| error.to_string())?;
                    SqlServerRoutingStore::new(config)
                        .with_pool(pool)
                        .map_err(|error| error.to_string())?
                };
                Ok(Arc::new(store) as Arc<dyn RoutingStore>)
            }
        }
    }
}

/// Declarative Arrow Flight runtime limits.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeLimitsConfig {
    /// Accepted request compression; uncompressed requests remain accepted.
    pub grpc_accept_compression: arrowcol_tunables::GrpcCompression,
    /// Response compression when supported by the requesting client.
    pub grpc_send_compression: arrowcol_tunables::GrpcCompression,
    /// Maximum aggregate gRPC metadata bytes.
    pub max_metadata_bytes: usize,
    /// Maximum encoded Arrow schema bytes.
    pub max_schema_bytes: usize,
    /// Maximum one-message size.
    pub max_message_bytes: usize,
    /// Bounded decoder-to-admission channel capacity.
    pub batch_channel_capacity: usize,
    /// Bounded Flight response channel capacity.
    pub put_result_channel_capacity: usize,
    /// Maximum stream duration.
    #[serde(with = "humantime_serde")]
    pub max_stream_duration: Duration,
    /// Maximum time spent in one DFE admission call.
    #[serde(with = "humantime_serde")]
    pub admission_timeout: Duration,
    /// Maximum Flight SQL command bytes.
    pub max_ddl_bytes: usize,
}

impl Default for RuntimeLimitsConfig {
    fn default() -> Self {
        let limits = RuntimeLimits::default();
        Self {
            max_metadata_bytes: limits.max_metadata_bytes,
            grpc_accept_compression: arrowcol_tunables::GrpcCompression::None,
            grpc_send_compression: arrowcol_tunables::GrpcCompression::None,
            max_schema_bytes: limits.max_schema_bytes,
            max_message_bytes: limits.max_message_bytes,
            batch_channel_capacity: limits.batch_channel_capacity,
            put_result_channel_capacity: limits.put_result_channel_capacity,
            max_stream_duration: limits.max_stream_duration,
            admission_timeout: limits.admission_timeout,
            max_ddl_bytes: limits.max_ddl_bytes,
        }
    }
}

impl RuntimeLimitsConfig {
    pub(crate) fn build(&self) -> RuntimeResult<RuntimeLimits> {
        let limits = RuntimeLimits {
            grpc_accept_compression: grpc_compression_encoding(self.grpc_accept_compression),
            grpc_send_compression: grpc_compression_encoding(self.grpc_send_compression),
            max_metadata_bytes: self.max_metadata_bytes,
            max_schema_bytes: self.max_schema_bytes,
            max_message_bytes: self.max_message_bytes,
            batch_channel_capacity: self.batch_channel_capacity,
            put_result_channel_capacity: self.put_result_channel_capacity,
            max_stream_duration: self.max_stream_duration,
            admission_timeout: self.admission_timeout,
            max_ddl_bytes: self.max_ddl_bytes,
        };
        limits.validate()?;
        if self.batch_channel_capacity > tokio::sync::Semaphore::MAX_PERMITS
            || self.put_result_channel_capacity > tokio::sync::Semaphore::MAX_PERMITS
            || tokio::time::Instant::now()
                .checked_add(self.max_stream_duration)
                .is_none()
            || tokio::time::Instant::now()
                .checked_add(self.admission_timeout)
                .is_none()
        {
            return Err(arrowcol_flight::RuntimeError::new(
                arrowcol_protocol::ErrorCode::Internal,
                tonic::Code::Internal,
                "Flight channel capacity exceeds semaphore bounds or timeout exceeds clock bounds",
            ));
        }
        Ok(limits)
    }
}

fn grpc_compression_encoding(
    compression: arrowcol_tunables::GrpcCompression,
) -> Option<tonic::codec::CompressionEncoding> {
    match compression {
        arrowcol_tunables::GrpcCompression::None => None,
        arrowcol_tunables::GrpcCompression::Gzip => Some(tonic::codec::CompressionEncoding::Gzip),
        arrowcol_tunables::GrpcCompression::Zstd => Some(tonic::codec::CompressionEncoding::Zstd),
    }
}

/// Collector registration and pressure-publication settings.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RegistrationConfig {
    /// Stable deployment key.
    pub instance_key: String,
    /// Router-visible Flight endpoint.
    pub flight_endpoint: SocketAddr,
    /// Advertised concurrent stream limit.
    #[serde(default = "default_max_concurrent_streams")]
    pub max_concurrent_streams: usize,
    /// Registration heartbeat interval.
    #[serde(default = "default_heartbeat_interval", with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    /// Rich pressure publication interval.
    #[serde(default = "default_pressure_interval", with = "humantime_serde")]
    pub pressure_interval: Duration,
    /// Registration lease lifetime.
    #[serde(default = "default_registration_ttl", with = "humantime_serde")]
    pub registration_ttl: Duration,
    /// Hard resident-memory limit.
    #[serde(default = "default_rss_limit_bytes")]
    pub rss_limit_bytes: u64,
    /// End-to-end buffered Arrow byte bound.
    #[serde(default = "default_buffered_bytes_limit")]
    pub buffered_bytes_limit: u64,
    /// End-to-end buffered row bound.
    #[serde(default = "default_buffered_rows_limit")]
    pub buffered_rows_limit: u64,
    /// End-to-end buffered batch bound.
    #[serde(default = "default_buffered_batches_limit")]
    pub buffered_batches_limit: u64,
}

impl RegistrationConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.instance_key.is_empty()
            || self.instance_key.len() > 200
            || self.instance_key.chars().any(char::is_control)
            || self.flight_endpoint.port() == 0
            || self.max_concurrent_streams == 0
            || self.max_concurrent_streams > tokio::sync::Semaphore::MAX_PERMITS
            || self.heartbeat_interval.is_zero()
            || self.pressure_interval.is_zero()
            || self.registration_ttl <= self.heartbeat_interval
            || self.rss_limit_bytes == 0
            || self.buffered_bytes_limit == 0
            || self.buffered_rows_limit == 0
            || self.buffered_batches_limit == 0
        {
            return Err("collector registration and pressure bounds are invalid".to_owned());
        }
        PositiveMeasurement::new(
            u64::try_from(self.max_concurrent_streams).map_err(|_| "max_concurrent_streams exceeds u64".to_owned())?,
        )
        .map_err(|error| error.to_string())?;
        for value in [
            self.rss_limit_bytes,
            self.buffered_bytes_limit,
            self.buffered_rows_limit,
            self.buffered_batches_limit,
        ] {
            PositiveMeasurement::new(value).map_err(|error| error.to_string())?;
        }
        for (name, value) in [
            ("heartbeat_interval", self.heartbeat_interval),
            ("pressure_interval", self.pressure_interval),
            ("registration_ttl", self.registration_ttl),
        ] {
            validate_duration(name, value)?;
        }
        if duration_millis(self.registration_ttl)? == 0 {
            return Err("registration_ttl must be at least one millisecond".to_owned());
        }
        checked_add_millis(now_unix_millis()?, self.registration_ttl)?;
        Ok(())
    }
}

/// Strict configuration for `receiver:arrow_flight`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FlightReceiverConfig {
    /// Arrow Flight bind address.
    pub listening_addr: SocketAddr,
    /// Admin HTTP bind address.
    pub admin_listening_addr: SocketAddr,
    /// Strict catalog control plane.
    pub catalog: CatalogConfig,
    /// Routing registration and lease store.
    pub routing: RoutingStoreConfig,
    /// Registration and bounded-pressure settings.
    pub registration: RegistrationConfig,
    /// Flight resource limits.
    #[serde(default)]
    pub limits: RuntimeLimitsConfig,
    /// Flight listener and accepted-socket settings.
    #[serde(default)]
    pub socket: arrowcol_tunables::CollectorSocketTunables,
    /// Enables durable admission through native Quiver persistence.
    ///
    /// The topology must route this receiver into a `processor:durable_buffer`
    /// (ideally configured with `wal_flush_interval: 0s`). A Flight `DoPut`
    /// then completes with `durable_admission` only after the durable buffer
    /// acknowledged every batch of the stream; a rejection or
    /// `limits.admission_timeout` fails the stream with a retryable error.
    /// When `false`, completion stays `best_effort_admission`.
    #[serde(default)]
    pub native_quiver: bool,
    /// Process CPU sampling for admission pressure; disabled by default.
    #[serde(default)]
    pub cpu_pressure: arrowcol_tunables::CpuPressureMode,
    /// Last collector stage exercised; only the default L5 runs the normal publication path.
    #[serde(default)]
    pub isolation_level: IsolationLevel,
}

impl FlightReceiverConfig {
    pub(crate) fn apply_environment(&mut self) -> Result<(), String> {
        const ISOLATION_ENV: &str = "ARROWCOL_COLLECTOR_ISOLATION_LEVEL";
        if let Some(value) = environment_json(ISOLATION_ENV)? {
            self.isolation_level = value.parse().map_err(|error| format!("{ISOLATION_ENV}: {error}"))?;
        }
        const CPU_PRESSURE_ENV: &str = "ARROWCOL_COLLECTOR_CPU_PRESSURE";
        match std::env::var(CPU_PRESSURE_ENV) {
            Ok(value) => {
                self.cpu_pressure = value.parse().map_err(|error| format!("{CPU_PRESSURE_ENV}: {error}"))?;
            }
            Err(std::env::VarError::NotPresent) => {}
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(format!("{CPU_PRESSURE_ENV} must be UTF-8"));
            }
        }
        apply_json_environment(&mut self.limits, "ARROWCOL_COLLECTOR_LIMITS_CONFIG", None)?;
        apply_json_environment(&mut self.socket, "ARROWCOL_COLLECTOR_SOCKET_CONFIG", None)?;
        apply_json_environment(
            &mut self.registration,
            "ARROWCOL_COLLECTOR_PRESSURE_CONFIG",
            Some(&[
                "max_concurrent_streams",
                "rss_limit_bytes",
                "buffered_bytes_limit",
                "buffered_rows_limit",
                "buffered_batches_limit",
                "pressure_interval",
                "heartbeat_interval",
                "registration_ttl",
            ]),
        )?;
        self.catalog.apply_environment()?;
        self.routing.apply_environment()
    }

    pub(crate) fn effective_config_json(&self) -> serde_json::Value {
        serde_json::json!({
            "isolation_level": self.isolation_level,
            "cpu_pressure": self.cpu_pressure,
            "limits": self.limits,
            "socket": self.socket,
            "pressure": {
                "max_concurrent_streams": self.registration.max_concurrent_streams,
                "rss_limit_bytes": self.registration.rss_limit_bytes,
                "buffered_bytes_limit": self.registration.buffered_bytes_limit,
                "buffered_rows_limit": self.registration.buffered_rows_limit,
                "buffered_batches_limit": self.registration.buffered_batches_limit,
                "pressure_interval": format!("{:?}", self.registration.pressure_interval),
                "heartbeat_interval": format!("{:?}", self.registration.heartbeat_interval),
                "registration_ttl": format!("{:?}", self.registration.registration_ttl)
            },
            "catalog": self.catalog.effective_config_json(),
            "routing": self.routing.effective_config_json()
        })
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.listening_addr.port() == 0
            || self.admin_listening_addr.port() == 0
            || self.listening_addr == self.admin_listening_addr
        {
            return Err("Flight and admin listeners must use distinct nonzero ports".to_owned());
        }
        self.catalog.validate()?;
        self.routing.validate()?;
        self.registration.validate()?;
        self.limits.build().map_err(|error| error.to_string())?;
        self.socket.validate()?;
        if !cfg!(target_os = "linux")
            && (self.socket.recv_buffer_bytes.is_some() || self.socket.send_buffer_bytes.is_some())
        {
            return Err("collector.socket buffer settings are only supported on Linux".to_owned());
        }
        Ok(())
    }
}

/// Bounded Delta publication settings.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, Hash, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct CommitConfig {
    /// Strategy used to resolve prior Delta transactions.
    pub txn_lookup_mode: arrow_deltalake::TxnLookupMode,
    /// When a partially filled commit batch is published: after `batch_window` or as soon as the worker is idle.
    pub trigger: arrow_deltalake::CommitTrigger,
    /// Maximum batching delay.
    #[serde(with = "humantime_serde")]
    pub batch_window: Duration,
    /// Maximum files in one Delta commit.
    pub max_files_per_commit: usize,
    /// Maximum queued and committing files per table.
    pub max_pending_files: usize,
    /// How the table owner reuses its Delta snapshot.
    pub snapshot_cache_mode: arrow_deltalake::SnapshotCacheMode,
    /// How object-store snapshot refreshes list `_delta_log/`.
    pub snapshot_list_mode: arrow_deltalake::SnapshotListMode,
    /// Whether coalesced files bind their write schema separately or as a group.
    pub stage_snapshot_mode: arrow_deltalake::StageSnapshotMode,
    /// How coalesced preparation resolves prior Delta transactions.
    pub preflight_lookup_mode: arrow_deltalake::PreflightLookupMode,
    /// Whether the collector or an external service writes checkpoints.
    pub checkpoint_mode: arrow_deltalake::CheckpointMode,
    /// Commit interval for collector-owned checkpoints.
    pub checkpoint_interval: u64,
    /// Publication attempts per commit cohort, including the first.
    pub commit_attempts: u32,
    /// Elapsed publication retry budget in seconds, checked between attempts only.
    pub commit_budget_secs: u64,
}

impl Default for CommitConfig {
    fn default() -> Self {
        let options = arrow_deltalake::commit::CommitOptions::default();
        Self {
            txn_lookup_mode: options.txn_lookup_mode,
            trigger: options.trigger,
            batch_window: options.batch_window,
            max_files_per_commit: options.max_files_per_commit,
            max_pending_files: options.max_pending_files,
            snapshot_cache_mode: options.snapshot_cache_mode,
            snapshot_list_mode: options.snapshot_list_mode,
            stage_snapshot_mode: options.stage_snapshot_mode,
            preflight_lookup_mode: options.preflight_lookup_mode,
            checkpoint_mode: options.checkpoint_mode,
            checkpoint_interval: options.checkpoint_interval,
            commit_attempts: options.commit_attempts,
            commit_budget_secs: options.commit_budget.as_secs(),
        }
    }
}

impl CommitConfig {
    pub(crate) fn build(&self) -> Result<arrow_deltalake::commit::CommitOptions, String> {
        let options = arrow_deltalake::commit::CommitOptions {
            txn_lookup_mode: self.txn_lookup_mode,
            trigger: self.trigger,
            batch_window: self.batch_window,
            max_files_per_commit: self.max_files_per_commit,
            max_pending_files: self.max_pending_files,
            snapshot_cache_mode: self.snapshot_cache_mode,
            snapshot_list_mode: self.snapshot_list_mode,
            stage_snapshot_mode: self.stage_snapshot_mode,
            preflight_lookup_mode: self.preflight_lookup_mode,
            checkpoint_mode: self.checkpoint_mode,
            checkpoint_interval: self.checkpoint_interval,
            commit_attempts: self.commit_attempts,
            commit_budget: Duration::from_secs(self.commit_budget_secs),
        };
        options.validate().map_err(|error| error.to_string())?;
        Ok(options)
    }
}

/// Strict configuration for `exporter:arrow_deltalake`.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeltaExporterConfig {
    /// Absolute local root or abfss URI for logical Arrowcol tables.
    pub base_path: PathBuf,
    /// Authoritative routing lease store.
    pub routing: RoutingStoreConfig,
    /// Bounded publication settings.
    #[serde(default)]
    pub commit: CommitConfig,
    /// Bounded Arrow-to-Parquet file grouping.
    #[serde(default)]
    pub batching: FileBatchConfig,
    /// Maximum synchronous wait for the commit-boundary authority check.
    #[serde(default = "default_authority_timeout", with = "humantime_serde")]
    pub authority_timeout: Duration,
}

impl DeltaExporterConfig {
    pub(crate) fn apply_environment(&mut self) -> Result<(), String> {
        apply_json_environment(&mut self.batching, "ARROWCOL_COLLECTOR_BATCHING_CONFIG", None)?;
        apply_json_environment(&mut self.commit, "ARROWCOL_COLLECTOR_COMMIT_CONFIG", None)?;
        self.routing.apply_environment()?;
        let mut overrides = ExporterOverrides {
            authority_timeout: self.authority_timeout,
        };
        apply_json_environment(&mut overrides, "ARROWCOL_COLLECTOR_EXPORTER_CONFIG", None)?;
        self.authority_timeout = overrides.authority_timeout;
        Ok(())
    }

    pub(crate) fn effective_config_json(&self) -> serde_json::Value {
        serde_json::json!({
            "batching": self.batching,
            "commit": self.commit,
            "routing": self.routing.effective_config_json(),
            "authority_timeout": format!("{:?}", self.authority_timeout)
        })
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if !self.base_path.is_absolute() && !self.base_path.to_string_lossy().starts_with("abfss://") {
            return Err("Delta base_path must be absolute or an abfss URI".to_owned());
        }
        if self.authority_timeout.is_zero() {
            return Err("Delta authority_timeout must be positive".to_owned());
        }
        validate_duration("authority_timeout", self.authority_timeout)?;
        self.routing.validate()?;
        self.commit.build()?;
        self.batching.validate()?;
        Ok(())
    }
}

/// Admission limits for queued and in-flight Arrow input.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BatchCapacity {
    /// Logical Arrow bytes.
    pub bytes: u64,
    /// Rows.
    pub rows: u32,
    /// Batches.
    pub batches: u32,
}

/// Bounded file grouping, independent of delivery receipt semantics.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct FileBatchConfig {
    /// Combine publications only through replay-safe atomic file groups.
    pub coalesce_publications: bool,
    /// Optionally collect a finite snapshot of ready commands at the age cutoff.
    pub age_flush_policy: AgeFlushPolicy,
    /// Process-wide queued and in-flight capacity.
    pub global_capacity: BatchCapacity,
    /// Per-table queued and in-flight capacity.
    pub per_table_capacity: BatchCapacity,
    /// Maximum logical Arrow bytes retained per file before flush; not compressed Parquet bytes.
    pub max_arrow_memory_per_flush: u64,
    /// Maximum rows in a file.
    pub max_rows_per_flush: u64,
    /// Maximum batches in a file.
    pub max_batches_per_flush: u64,
    /// Maximum age of the first batch in a file.
    #[serde(with = "humantime_serde")]
    pub max_age: Duration,
    /// Minimum age of a table writer-policy readback before it is refreshed again; zero refreshes every batch.
    #[serde(with = "humantime_serde")]
    pub adaptive_policy_refresh_interval: Duration,
    /// Maximum simultaneous file publications.
    pub max_concurrent_flushes: usize,
    /// Maximum sibling files staged concurrently within one coalesced publication.
    pub stage_file_concurrency: usize,
    /// Balanced sibling files per unsorted partition in a coalesced group.
    pub stage_files_per_group: usize,
    /// Per-table publication groups allowed to encode/upload ahead of durable receipts.
    pub pipeline_depth: usize,
    /// Backend-wide logical bytes allowed to release Arrow and flush capacity after queue handoff.
    /// Zero keeps both reservations through the durable receipt.
    pub max_pending_commit_bytes: u64,
    /// Compressed Parquet target and estimator limits.
    pub adaptive_file: AdaptiveFileConfig,
}

impl Default for FileBatchConfig {
    fn default() -> Self {
        Self {
            coalesce_publications: false,
            age_flush_policy: AgeFlushPolicy::Immediate,
            global_capacity: BatchCapacity {
                bytes: 2 * 1024 * 1024 * 1024,
                rows: 10_000_000,
                batches: 4096,
            },
            per_table_capacity: BatchCapacity {
                bytes: 512 * 1024 * 1024,
                rows: 2_000_000,
                batches: 1024,
            },
            max_arrow_memory_per_flush: 512 * 1024 * 1024,
            max_rows_per_flush: 1_000_000,
            max_batches_per_flush: 512,
            max_age: Duration::from_secs(30),
            adaptive_policy_refresh_interval: Duration::ZERO,
            max_concurrent_flushes: 4,
            stage_file_concurrency: 1,
            stage_files_per_group: 1,
            pipeline_depth: 1,
            max_pending_commit_bytes: 0,
            adaptive_file: AdaptiveFileConfig::default(),
        }
    }
}

impl FileBatchConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.max_pending_commit_bytes > arrowcol_tunables::MAX_QUEUE_CAPACITY_BYTES {
            return Err("max_pending_commit_bytes exceeds the exact byte capacity limit".to_owned());
        }
        if self.adaptive_policy_refresh_interval > arrowcol_tunables::MAX_ADAPTIVE_POLICY_REFRESH_INTERVAL {
            return Err("adaptive_policy_refresh_interval must not exceed 1h".to_owned());
        }
        let global = self.global_capacity;
        let table = self.per_table_capacity;
        if !(1..=16).contains(&self.stage_file_concurrency) {
            return Err("stage_file_concurrency must be in 1..=16".to_owned());
        }
        if self.stage_file_concurrency > 1 && !self.coalesce_publications {
            return Err("stage_file_concurrency > 1 requires coalesce_publications=true".to_owned());
        }
        if !(1..=16).contains(&self.stage_files_per_group) {
            return Err("stage_files_per_group must be in 1..=16".to_owned());
        }
        if self.stage_files_per_group > 1 && !self.coalesce_publications {
            return Err("stage_files_per_group > 1 requires coalesce_publications=true".to_owned());
        }
        if [global.rows, global.batches, table.rows, table.batches].contains(&0)
            || global.bytes == 0
            || table.bytes == 0
            || global.bytes > arrowcol_tunables::MAX_QUEUE_CAPACITY_BYTES
            || table.bytes > arrowcol_tunables::MAX_QUEUE_CAPACITY_BYTES
            || [global.rows, global.batches, table.rows, table.batches]
                .into_iter()
                .any(|value| u64::from(value) > tokio::sync::Semaphore::MAX_PERMITS as u64)
            || table.bytes > global.bytes
            || table.rows > global.rows
            || table.batches > global.batches
            || self.max_arrow_memory_per_flush == 0
            || self.max_arrow_memory_per_flush > table.bytes
            || self.max_rows_per_flush == 0
            || self.max_rows_per_flush > u64::from(table.rows)
            || self.max_batches_per_flush == 0
            || self.max_batches_per_flush > u64::from(table.batches)
            || self.max_age.is_zero()
            || tokio::time::Instant::now().checked_add(self.max_age).is_none()
            || self.max_concurrent_flushes == 0
            || self.max_concurrent_flushes > tokio::sync::Semaphore::MAX_PERMITS
            || !(1..=64).contains(&self.pipeline_depth)
            || (self.pipeline_depth > 1 && !self.coalesce_publications)
        {
            return Err("invalid bounded Arrow file batching capacities or flush policy".to_owned());
        }
        self.adaptive_file.build().validate().map_err(|error| error.to_string())
    }
}

/// Configuration for the shared compressed-file size estimator.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct AdaptiveFileConfig {
    /// Desired compressed Parquet bytes.
    pub target_parquet_bytes: u64,
    /// Minimum logical input threshold.
    pub min_input_bytes: u64,
    /// Maximum logical input threshold.
    pub max_input_bytes: u64,
    /// Initial compressed/input ratio.
    pub cold_start_parquet_ratio: f64,
    /// EWMA observation weight.
    pub ewma_weight: f64,
    /// Minimum observed ratio.
    pub min_parquet_ratio: f64,
    /// Maximum observed ratio.
    pub max_parquet_ratio: f64,
}

impl Default for AdaptiveFileConfig {
    fn default() -> Self {
        let config = arrow_deltalake::sizing::AdaptiveFileSizeConfig::default();
        Self {
            target_parquet_bytes: config.target_parquet_bytes,
            min_input_bytes: config.min_input_bytes,
            max_input_bytes: config.max_input_bytes,
            cold_start_parquet_ratio: config.cold_start_parquet_ratio,
            ewma_weight: config.ewma_weight,
            min_parquet_ratio: config.min_parquet_ratio,
            max_parquet_ratio: config.max_parquet_ratio,
        }
    }
}

impl AdaptiveFileConfig {
    pub(crate) const fn build(self) -> arrow_deltalake::sizing::AdaptiveFileSizeConfig {
        arrow_deltalake::sizing::AdaptiveFileSizeConfig {
            target_parquet_bytes: self.target_parquet_bytes,
            min_input_bytes: self.min_input_bytes,
            max_input_bytes: self.max_input_bytes,
            cold_start_parquet_ratio: self.cold_start_parquet_ratio,
            ewma_weight: self.ewma_weight,
            min_parquet_ratio: self.min_parquet_ratio,
            max_parquet_ratio: self.max_parquet_ratio,
        }
    }
}

pub(crate) fn config_error(error: impl Into<String>) -> otel_arrow_dfe_config::error::Error {
    otel_arrow_dfe_config::error::Error::InvalidUserConfig { error: error.into() }
}

pub(crate) fn parse_config<T>(value: &serde_json::Value) -> Result<T, otel_arrow_dfe_config::error::Error>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(value.clone()).map_err(|error| config_error(error.to_string()))
}

pub(crate) fn duration_millis(duration: Duration) -> Result<u64, String> {
    u64::try_from(duration.as_millis()).map_err(|_| "duration exceeds u64 milliseconds".to_owned())
}

pub(crate) fn now_unix_millis() -> Result<UnixMillis, String> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("system clock precedes Unix epoch: {error}"))?;
    let millis = u64::try_from(elapsed.as_millis()).map_err(|_| "Unix timestamp exceeds u64".to_owned())?;
    UnixMillis::new(millis).map_err(|error| error.to_string())
}

pub(crate) fn checked_add_millis(now: UnixMillis, duration: Duration) -> Result<UnixMillis, String> {
    let millis = duration_millis(duration)?;
    let value = now
        .get()
        .checked_add(millis)
        .ok_or_else(|| "Unix timestamp overflow".to_owned())?;
    UnixMillis::new(value).map_err(|error| error.to_string())
}

pub(crate) fn table_path(base: &Path, table: &arrowcol_protocol::TableIdentity) -> PathBuf {
    base.join(table.catalog().as_str())
        .join(table.schema().as_str())
        .join(table.table().as_str())
}
