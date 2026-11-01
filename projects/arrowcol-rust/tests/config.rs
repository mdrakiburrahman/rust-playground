use arrowcol_rust::{CollectorConfig, ConfigError};

/// Scenario: Cloud startup omits its epoch or supplies a malformed UUID.
/// Guarantees: The actual entrypoint refuses before reading SQL credentials or opening WAL/writers.
#[test]
fn cloud_entrypoint_refuses_missing_and_malformed_generation() {
    for generation in [None, Some("not-a-generation-uuid")] {
        let mut command = assert_cmd::Command::new(env!("CARGO_BIN_EXE_arrowcol-rust"));
        command
            .arg("--config")
            .arg(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.docker.yaml"))
            .env(
                "ARROWCOL_DELTA_ROOT",
                "abfss://container@account.dfs.core.windows.net/root",
            )
            .env_remove("ARROWCOL_RESET_GENERATION")
            .env_remove("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
            .env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")
            .env("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
            .env("OTEL_METRIC_EXPORT_INTERVAL", "60000")
            .timeout(std::time::Duration::from_secs(15));
        if let Some(generation) = generation {
            command.env("ARROWCOL_RESET_GENERATION", generation);
        }
        command
            .assert()
            .failure()
            .stderr(predicates::str::contains("ARROWCOL_RESET_GENERATION"));
    }
}

#[test]
fn checked_in_configs_are_strict_and_valid() -> Result<(), ConfigError> {
    CollectorConfig::from_yaml(include_bytes!("../config.yaml"))?;
    CollectorConfig::from_yaml(include_bytes!("../config.docker.yaml"))?;
    Ok(())
}

#[test]
fn unknown_fields_are_rejected() -> anyhow::Result<()> {
    let yaml = String::from_utf8(include_bytes!("../config.yaml").to_vec())?
        .replace("  bind: 127.0.0.1:50051", "  bind: 127.0.0.1:50051\n  surprise: true");
    let Err(error) = CollectorConfig::from_yaml(yaml.as_bytes()) else {
        anyhow::bail!("unknown field was accepted");
    };
    assert!(matches!(error, ConfigError::Yaml(_)));
    Ok(())
}

fn with_quiver(settings: &str) -> anyhow::Result<String> {
    Ok(String::from_utf8(include_bytes!("../config.yaml").to_vec())?.replace(
        "  native_quiver:\n    enabled: false\n    directory: null\n    capacity_bytes: null",
        &format!("  native_quiver:\n{settings}"),
    ))
}

/// Scenario: Native Quiver is enabled with a directory and a 1 GiB disk budget.
/// Guarantees: The config validates, exposes the durable-stage settings with
/// fsync-per-acknowledgement defaults, and registers a receipt recovery window.
#[test]
fn native_quiver_enables_durable_stage() -> anyhow::Result<()> {
    let yaml = with_quiver(
        "    enabled: true\n    directory: projects/arrowcol-rust/.data/quiver\n    capacity_bytes: 1073741824",
    )?;
    let config = CollectorConfig::from_yaml(yaml.as_bytes())?;
    let stage = config
        .buffer
        .native_quiver
        .stage_config()?
        .ok_or_else(|| anyhow::anyhow!("enabled Quiver produced no stage settings"))?;
    assert_eq!(stage.capacity_bytes, 1_073_741_824);
    assert!(stage.wal_flush_interval.is_zero());
    assert_eq!(config.buffer.native_quiver.recovery_window_ms(), Some(600_000));
    Ok(())
}

/// Scenario: Native Quiver is enabled without a directory, with a disk budget
/// below Quiver's minimum, or with an out-of-range recovery window.
/// Guarantees: Each misconfiguration is rejected at validation time rather than
/// failing after the collector starts accepting data.
#[test]
fn invalid_native_quiver_settings_are_rejected() -> anyhow::Result<()> {
    for settings in [
        "    enabled: true\n    directory: null\n    capacity_bytes: 1073741824",
        "    enabled: true\n    directory: projects/arrowcol-rust/.data/quiver\n    capacity_bytes: 1048576",
        "    enabled: true\n    directory: projects/arrowcol-rust/.data/quiver\n    capacity_bytes: 1073741824\n    recovery_window_ms: 0",
        "    enabled: false\n    directory: projects/arrowcol-rust/.data/quiver\n    capacity_bytes: null",
    ] {
        let yaml = with_quiver(settings)?;
        let Err(error) = CollectorConfig::from_yaml(yaml.as_bytes()) else {
            anyhow::bail!("invalid Quiver settings were accepted: {settings}");
        };
        assert!(matches!(error, ConfigError::Invalid(_)), "{error}");
    }
    Ok(())
}

#[test]
fn unsafe_lease_timing_is_rejected() -> anyhow::Result<()> {
    let yaml = String::from_utf8(include_bytes!("../config.yaml").to_vec())?
        .replace("  table_lease_renew_ms: 5000", "  table_lease_renew_ms: 20000");
    let Err(error) = CollectorConfig::from_yaml(yaml.as_bytes()) else {
        anyhow::bail!("unsafe renewal timing was accepted");
    };
    assert!(error.to_string().contains("less than half table_lease_ttl_ms"));
    Ok(())
}

#[test]
fn sql_pool_defaults_preserve_existing_configuration() -> anyhow::Result<()> {
    let yaml = String::from_utf8(include_bytes!("../config.yaml").to_vec())?.replace(
        "  pool:\n    receipts_max_size: 16\n    routing_max_size: 4\n    catalog_max_size: 4\n    acquire_timeout_ms: 5000\n",
        "",
    );
    let config = CollectorConfig::from_yaml(yaml.as_bytes())?;
    assert_eq!(config.sql_server.pool.receipts_max_size, 16);
    assert_eq!(config.sql_server.pool.routing_max_size, 4);
    assert_eq!(config.sql_server.pool.catalog_max_size, 4);
    assert_eq!(config.sql_server.pool.acquire_timeout_ms, 5000);
    Ok(())
}

#[test]
fn invalid_sql_pool_settings_are_rejected() -> anyhow::Result<()> {
    let yaml = String::from_utf8(include_bytes!("../config.yaml").to_vec())?;
    for setting in [
        "receipts_max_size: 16",
        "routing_max_size: 4",
        "catalog_max_size: 4",
        "acquire_timeout_ms: 5000",
    ] {
        let key = setting
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("missing setting value"))?
            .0;
        let invalid_yaml = yaml.replace(setting, &format!("{key}: 0"));
        let error = CollectorConfig::from_yaml(invalid_yaml.as_bytes())
            .err()
            .ok_or_else(|| anyhow::anyhow!("zero {key} was accepted"))?;
        assert!(matches!(error, ConfigError::Invalid(_)));
        assert!(error.to_string().contains(key));
    }
    let unknown = yaml.replace(
        "    routing_max_size: 4",
        "    routing_max_size: 4\n    unknown_pool_setting: 1",
    );
    assert!(matches!(
        CollectorConfig::from_yaml(unknown.as_bytes()),
        Err(ConfigError::Yaml(_))
    ));
    let oversized = yaml.replace("receipts_max_size: 16", &format!("receipts_max_size: {}", usize::MAX));
    assert!(matches!(
        CollectorConfig::from_yaml(oversized.as_bytes()),
        Err(ConfigError::Invalid(_))
    ));
    Ok(())
}
