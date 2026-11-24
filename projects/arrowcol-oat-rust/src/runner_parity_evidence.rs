fn parity_has_override(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Object(fields) => fields.values().any(parity_has_override),
        Value::Array(values) => !values.is_empty(),
        _ => true,
    }
}

fn verify_observer_barrier(value: &Value, run_id: &str, image_id: &str, now: u64) -> Result<()> {
    let container_id = |key| {
        value[key].as_str().is_some_and(|id| {
            id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    };
    if value["version"] != 1
        || value["run_id"] != run_id
        || value["collector_image_id"] != image_id
        || !container_id("collector_container_id")
        || !container_id("observer_container_id")
        || !value["ready_unix_ms"].as_u64().is_some_and(|time| time > 0 && time <= now)
    {
        bail!("invalid observer readiness barrier");
    }
    Ok(())
}

async fn wait_observer_barrier(config: &OatConfig, run_id: &str) -> Result<()> {
    ensure!(config.platform.kind == crate::config::OatMode::Local, "observer barrier requires local OAT");
    let image = config
        .platform
        .local
        .expected_image_ids
        .as_ref()
        .and_then(|images| images.get("collector"))
        .context("observer barrier requires a frozen collector image")?;
    let root = config.platform.compose_state_root.as_deref().context("local state root missing")?;
    let path = Path::new(root).join(run_id).join("observer-ready.json");
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            match tokio::fs::symlink_metadata(&path).await {
                Ok(metadata) => {
                    ensure!(metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() <= 4096,
                        "invalid observer barrier file");
                    let value: Value = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
                    return verify_observer_barrier(&value, run_id, image, unix_time_ms_u64()?);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }).await.context("observer readiness barrier timed out")?
}

async fn wait_observer_completion(config: &OatConfig, run_id: &str, measurement_end: u64) -> Result<()> {
    let root = config.platform.compose_state_root.as_deref().context("local state root missing")?;
    let directory = Path::new(root).join(run_id);
    let ready: Value = serde_json::from_slice(&tokio::fs::read(directory.join("observer-ready.json")).await?)?;
    let path = directory.join("observer-done.json");
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            match tokio::fs::symlink_metadata(&path).await {
                Ok(metadata) => {
                    ensure!(metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() <= 4096,
                        "invalid observer completion barrier file");
                    let done: Value = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
                    let finished = done["finished_unix_ms"].as_u64().context("missing observer completion time")?;
                    ensure!(done["version"] == 1 && done["run_id"] == run_id
                        && done["observer_container_id"] == ready["observer_container_id"]
                        && finished >= measurement_end && finished <= unix_time_ms_u64()?,
                        "invalid observer completion barrier");
                    return Ok(());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }).await.context("observer completion barrier timed out")?
}

fn parity_admission_readback(
    config: &OatConfig,
    logs: &[ProcessLog],
    clients: &[ClientProgress],
    allow_writer_overrides: bool,
) -> Result<Value> {
    if config.platform.kind != crate::config::OatMode::Local
        || config.tables.len() != 1
        || config.clients_per_table != 1
        || clients.len() != 1
    {
        bail!("parity admission requires one local table and client");
    }
    let [collector] = logs else {
        bail!("parity admission requires exactly one collector");
    };
    if !matches!(collector.role, ComponentRole::Collector) || !collector.complete || collector.image_id.is_none() {
        bail!("complete collector identity and startup readback are required before admission");
    }
    let collector_config = process_configurations(&collector.text)?;
    verify_process_configuration(config, ComponentRole::Collector, &collector_config)?;
    let writer = collector_config
        .get("parquet_writer")
        .filter(|value| value.is_object())
        .context("parity collector writer readback is missing")?;
    if parity_has_override(writer) && (!allow_writer_overrides || config.tunables.parquet_writer.is_none()) {
        bail!("parity collector has a Parquet writer override");
    }
    let [client] = clients else {
        bail!("parity admission requires exactly one client");
    };
    if !client.ready || client.completed || client.failure.is_some() {
        bail!("parity client was not ready before admission");
    }
    let client_config = process_configurations(&client.output)?;
    let resolved_config = resolved_fleet_client_config(config, 0)?;
    crate::runner::verify_oat_client_build_profile_readback(&resolved_config, &client_config)?;
    verify_process_configuration(&resolved_config, ComponentRole::Client, &client_config)?;
    Ok(json!({
        "status": "verified_before_schedule",
        "observed_at_unix_ms": unix_time_ms_u64()?,
        "collector_image_id": collector.image_id,
        "collector": collector_config,
        "client": client_config,
    }))
}

#[cfg(test)]
mod parity_admission_tests {
    use super::*;

    #[test]
    fn writer_override_check_distinguishes_empty_policy_from_values() {
        assert!(!parity_has_override(&json!({"defaults":{"compression":null}, "columns":{}})));
        assert!(parity_has_override(&json!({"defaults":{"compression":"zstd"}})));
        assert!(parity_has_override(&json!({"columns":{"field":{"dictionary_enabled":false}}})));
    }

    #[test]
    fn observer_barrier_requires_matching_live_run_and_image() {
        let run = "run";
        let image = format!("sha256:{}", "a".repeat(64));
        let value = json!({ "version": 1, "run_id": run, "collector_image_id": image,
            "collector_container_id": "b".repeat(64), "observer_container_id": "c".repeat(64), "ready_unix_ms": 100 });
        assert!(verify_observer_barrier(&value, run, &image, 100).is_ok());
        assert!(verify_observer_barrier(&value, "different", &image, 100).is_err());
        assert!(verify_observer_barrier(&value, run, &image, 99).is_err());
        assert!(verify_observer_barrier(&value, run, "other", 100).is_err());
    }
}
