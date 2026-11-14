struct ProcessLog {
    role: ComponentRole,
    image: Option<String>,
    image_id: Option<String>,
    source_revision: Option<String>,
    complete: bool,
    text: String,
}

async fn local_client_image_evidence(id: &str) -> Result<Value> {
    let mut command = Command::new("docker");
    command.args(["inspect", id]);
    let values: Vec<Value> =
        serde_json::from_slice(&run_command(command, "read actual client image identity").await?.stdout)?;
    if values.len() != 1 {
        bail!("client container identity is ambiguous");
    }
    let container = &values[0];
    Ok(json!({
        "image": container.pointer("/Config/Image").and_then(Value::as_str),
        "image_id": container["Image"].as_str(),
        "source_revision": container.pointer("/Config/Labels/org.opencontainers.image.revision").and_then(Value::as_str),
        "source": "docker_container_inspect",
    }))
}

async fn aks_client_image_evidence(config: &OatConfig, job_name: &str) -> Result<Value> {
    let selector = format!("job-name={job_name}");
    let pods: Value = serde_json::from_str(
        &kubectl(
            config,
            vec![
                "get",
                "pods",
                "--namespace",
                &config.namespace,
                "--selector",
                &selector,
                "-o",
                "json",
            ],
            None,
        )
        .await?,
    )?;
    let pods = pods["items"].as_array().context("client pod readback has no list")?;
    if pods.len() != 1 {
        bail!("client Job did not resolve to exactly one execution pod");
    }
    let statuses = pods[0]
        .pointer("/status/containerStatuses")
        .and_then(Value::as_array)
        .context("client pod has no executed container status")?;
    let client = statuses
        .iter()
        .find(|status| status["name"] == "client")
        .context("client execution image status is missing")?;
    Ok(
        json!({ "image": client["image"], "image_id": client["imageID"], "source_revision": null, "source": "kubernetes_container_status" }),
    )
}

fn client_image_evidence(output: &str) -> Result<Value> {
    let mut records = output
        .lines()
        .filter_map(|line| line.strip_prefix("OAT_CLIENT_IMAGE_PROVENANCE "));
    let Some(record) = records.next() else {
        return Ok(json!({ "unavailable_reason": "client_execution_image_not_reported" }));
    };
    if records.next().is_some() {
        bail!("duplicate client image provenance");
    }
    let record: Value = serde_json::from_str(record).context("invalid client image provenance")?;
    Ok(json!({
        "image": record["image"].as_str(),
        "image_id": record["image_id"].as_str(),
        "source_revision": record["source_revision"].as_str(),
        "source": match record["source"].as_str() {
            Some("docker_container_inspect") => "docker_container_inspect",
            Some("kubernetes_container_status") => "kubernetes_container_status",
            _ => bail!("invalid client image provenance source"),
        },
    }))
}

fn role_name(role: ComponentRole) -> &'static str {
    match role {
        ComponentRole::Client => "client",
        ComponentRole::Router => "router",
        ComponentRole::Collector => "collector",
        ComponentRole::Maintenance => "maintenance",
    }
}

fn public_tuning_value(value: &Value) -> Option<Value> {
    match value {
        Value::Object(object) => Some(Value::Object(
            object
                .iter()
                .filter_map(|(name, value)| {
                    let lower = name.to_ascii_lowercase();
                    if ["secret", "password", "token", "endpoint", "listen", "address", "auth"]
                        .iter()
                        .any(|private| lower.contains(private))
                    {
                        return None;
                    }
                    public_tuning_value(value).map(|value| (name.clone(), value))
                })
                .collect(),
        )),
        Value::Array(values) => Some(Value::Array(values.iter().filter_map(public_tuning_value).collect())),
        Value::String(value) if value.len() <= 128 && !value.contains(['/', '\\', '@', ':']) => {
            Some(Value::String(value.clone()))
        }
        Value::String(_) => None,
        _ => Some(value.clone()),
    }
}

fn process_configurations(text: &str) -> Result<Value> {
    let mut configuration = json!({});
    for line in text.lines() {
        let value = [
            "arrowcol_effective_config ",
            "arrowcol_router_effective_config ",
            "arrowcol_wal_effective_config ",
            "arrowcol_maintenance_effective_config ",
            "OAT_CLIENT_EFFECTIVE_CONFIG ",
        ]
        .into_iter()
        .find_map(|prefix| line.strip_prefix(prefix));
        if let Some(value) = value {
            let mut value: Value = serde_json::from_str(value).context("invalid process effective configuration")?;
            if !value.is_object() {
                bail!("process effective configuration must be an object");
            }
            if let Some(component) = value.get("component").and_then(Value::as_str) {
                if !matches!(component, "receiver" | "exporter" | "build") {
                    bail!("unknown component effective-config envelope");
                }
                value = value
                    .get("config")
                    .cloned()
                    .context("component readback omitted resolved config")?;
                if !value.is_object() {
                    bail!("component readback config must be an object");
                }
            }
            let value = public_tuning_value(&value).context("process tuning readback was not public")?;
            merge_observed_configuration(&mut configuration, value)?;
        }
    }
    Ok(configuration)
}

fn merge_observed_configuration(target: &mut Value, observed: Value) -> Result<()> {
    let target = target
        .as_object_mut()
        .context("effective config target is not an object")?;
    for (name, value) in observed.as_object().context("effective config must be an object")? {
        match target.get_mut(name) {
            Some(existing) if existing.is_object() && value.is_object() => {
                merge_observed_configuration(existing, value.clone())?
            }
            Some(existing) if *existing != *value => bail!("conflicting process effective configuration for {name}"),
            Some(_) => {}
            None => {
                target.insert(name.clone(), value.clone());
            }
        }
    }
    Ok(())
}

fn contains_requested(observed: &Value, requested: &Value) -> bool {
    match (observed, requested) {
        (Value::Object(observed), Value::Object(requested)) => requested.iter().all(|(name, value)| {
            observed
                .get(name)
                .is_some_and(|observed| contains_requested(observed, value))
        }),
        _ => observed == requested,
    }
}

fn router_readback_path(variable: &str) -> Result<String> {
    let field = variable
        .strip_prefix("ARROWCOL_ROUTER_")
        .context("not a router environment name")?;
    let path = match field {
        "WORKERS" | "LISTENER_TASKS_PER_FD" | "KEEPALIVE_POOL_SIZE" | "EDGE_REPLAY_MAX_NONCES" => {
            format!("/{}", field.to_ascii_lowercase())
        }
        "H2_MAX_CONCURRENT_STREAMS"
        | "H2_STREAM_WINDOW_BYTES"
        | "H2_CONNECTION_WINDOW_BYTES"
        | "H2_IDLE_TIMEOUT_SECONDS" => format!(
            "/downstream_h2/{}",
            field.trim_start_matches("H2_").to_ascii_lowercase()
        ),
        "SQL_CONNECT_TIMEOUT_MS" | "SQL_COMMAND_TIMEOUT_MS" => format!(
            "/sql_timeouts/{}",
            field.trim_start_matches("SQL_").to_ascii_lowercase()
        ),
        "REFRESH_INTERVAL_MS"
        | "MAX_SNAPSHOT_AGE_MS"
        | "MAX_FUTURE_SKEW_MS"
        | "MAX_TABLE_ENTRIES"
        | "MAX_OLDEST_BUFFER_AGE_MS"
        | "HARD_PRESSURE_BPS"
        | "WARMUP_MS"
        | "MINIMUM_WARMUP_BPS"
        | "LEASE_TTL_MS"
        | "OAT_RUN_PIN_TTL_MS"
        | "READINESS_MAX_STALENESS_MS"
        | "DISTINCT_TABLE_OWNERS" => format!("/routing/{}", field.to_ascii_lowercase()),
        "UPSTREAM_MAX_H2_STREAMS"
        | "UPSTREAM_STREAM_WINDOW_BYTES"
        | "UPSTREAM_CONNECTION_WINDOW_BYTES"
        | "UPSTREAM_CONNECTION_TIMEOUT_MS"
        | "UPSTREAM_TOTAL_CONNECTION_TIMEOUT_MS"
        | "UPSTREAM_IDLE_TIMEOUT_SECONDS"
        | "UPSTREAM_PING_INTERVAL_SECONDS" => format!(
            "/upstream/{}",
            field.trim_start_matches("UPSTREAM_").to_ascii_lowercase()
        ),
        _ => bail!("router environment has no checked effective-config adapter: {variable}"),
    };
    Ok(path)
}

fn verify_process_configuration(config: &OatConfig, role: ComponentRole, observed: &Value) -> Result<()> {
    if matches!(role, ComponentRole::Maintenance) && maintenance_enabled(config) {
        verify_maintenance_controls(config, observed)?;
    }
    for (name, expected) in role_environment(config, role)? {
        let mut expected = serde_json::from_str::<Value>(&expected).unwrap_or(Value::String(expected));
        if name == "ARROWCOL_ROUTER_UPSTREAM_PING_INTERVAL_SECONDS" && expected == 0 {
            expected = Value::Null;
        }
        if matches!(role, ComponentRole::Maintenance) && name.starts_with("ARROWCOL_MAINT_") {
            let (actual, expected) = maintenance_readback_value(observed, &name, expected)?;
            if !contains_requested(&actual, &expected) {
                bail!("maintenance startup does not match applied tunable {name}");
            }
            continue;
        }
        let path = match name.as_str() {
            "ARROWCOL_PARQUET_CONFIG" if matches!(role, ComponentRole::Maintenance) => {
                "/parquet_writer_overrides".to_owned()
            }
            "ARROWCOL_PARQUET_CONFIG" => "/parquet_writer".to_owned(),
            "ARROWCOL_COLLECTOR_BATCHING_CONFIG" => "/batching".to_owned(),
            "ARROWCOL_COLLECTOR_COMMIT_CONFIG" => "/commit".to_owned(),
            "ARROWCOL_COLLECTOR_LIMITS_CONFIG" => "/limits".to_owned(),
            "ARROWCOL_COLLECTOR_PRESSURE_CONFIG" => "/pressure".to_owned(),
            "ARROWCOL_COLLECTOR_RUNTIME_WORKERS" => "/runtime/runtime_workers".to_owned(),
            variable if variable.starts_with("ARROWCOL_COLLECTOR_") => format!(
                "/runtime/{}",
                variable.trim_start_matches("ARROWCOL_COLLECTOR_").to_ascii_lowercase()
            ),
            variable if variable.starts_with("ARROWCOL_CLIENT_") => format!(
                "/{}",
                variable.trim_start_matches("ARROWCOL_CLIENT_").to_ascii_lowercase()
            ),
            variable if variable.starts_with("ARROWCOL_ROUTER_") => router_readback_path(variable)?,
            variable if variable.starts_with("ARROWCOL_WAL_") => format!(
                "/wal/{}",
                variable.trim_start_matches("ARROWCOL_WAL_").to_ascii_lowercase()
            ),
            variable if variable.starts_with("ARROWCOL_MAINT_") => format!(
                "/{}",
                variable.trim_start_matches("ARROWCOL_MAINT_").to_ascii_lowercase()
            ),
            _ => bail!("no effective readback adapter for {name}"),
        };
        let actual = observed
            .pointer(&path)
            .with_context(|| format!("{} startup readback omitted applied tunable {name}", role_name(role)))?;
        if !contains_requested(actual, &expected) {
            bail!(
                "{} startup readback does not match applied tunable {name}",
                role_name(role)
            );
        }
    }
    Ok(())
}

fn process_evidence(config: &OatConfig, logs: &[ProcessLog]) -> Result<Value> {
    let mut result = Vec::new();
    for role in [
        ComponentRole::Router,
        ComponentRole::Collector,
        ComponentRole::Maintenance,
    ] {
        let selected = logs
            .iter()
            .filter(|log| role_name(log.role) == role_name(role))
            .collect::<Vec<_>>();
        if (!role_environment(config, role)?.is_empty()
            || (matches!(role, ComponentRole::Maintenance) && maintenance_enabled(config)))
            && selected.is_empty()
        {
            bail!("configured {} has no running consumer/readback", role_name(role));
        }
        for log in selected {
            let resolved = process_configurations(&log.text)?;
            verify_process_configuration(config, role, &resolved)?;
            let observed = !resolved.as_object().is_none_or(serde_json::Map::is_empty);
            let maintenance_tables = if matches!(role, ComponentRole::Maintenance) {
                Some(maintenance_table_evidence(config, &log.text)?)
            } else {
                None
            };
            result.push(json!({
                "component": role_name(role),
                "effective_config": observed.then_some(resolved),
                "effective_config_source": observed.then_some("consumer_startup_readback"),
                "effective_config_unavailable_reason": (!observed).then_some("consumer_did_not_emit_startup_readback"),
                "image": log.image,
                "image_id": log.image_id,
                "source_revision": log.source_revision,
                "log_complete": log.complete,
                "maintenance_table_policies": maintenance_tables,
            }));
        }
    }
    Ok(json!(result))
}

async fn local_process_logs(config: &OatConfig, platform: &LocalPlatform) -> Result<Vec<ProcessLog>> {
    let project = scoped_compose_project(
        config
            .platform
            .compose_project
            .as_deref()
            .context("local project missing")?,
        &platform.run_id,
    );
    let mut list = compose_command(
        config
            .platform
            .compose_file
            .as_deref()
            .context("local compose missing")?,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut list);
    list.args(["ps", "--all", "--quiet"]);
    let ids = String::from_utf8(
        run_command(list, "list owned processes for effective-config evidence")
            .await?
            .stdout,
    )?;
    let ids = ids.split_whitespace().collect::<Vec<_>>();
    if ids.is_empty() {
        bail!("local stack disappeared before evidence readback");
    }
    let mut inspect = Command::new("docker");
    inspect.arg("inspect").args(ids);
    let inspected: Vec<Value> =
        serde_json::from_slice(&run_command(inspect, "read actual local image identities").await?.stdout)?;
    let mut logs = Vec::new();
    for container in inspected {
        let role = match container
            .pointer("/Config/Labels/com.docker.compose.service")
            .and_then(Value::as_str)
        {
            Some("dataflow-1") => ComponentRole::Collector,
            Some("router") => ComponentRole::Router,
            Some("delta-maintenance") => ComponentRole::Maintenance,
            _ => continue,
        };
        if container["RestartCount"].as_u64().unwrap_or(0) > 0 {
            bail!("required measurement process restarted during the cell");
        }
        let id = container["Id"].as_str().context("inspected container has no ID")?;
        let mut command = Command::new("docker");
        command.args(["logs", "--tail", "20000", id]);
        let output = run_command(command, "read owned process tuning and commit evidence").await?;
        let text = format!(
            "{}\n{}",
            String::from_utf8(output.stdout)?,
            String::from_utf8(output.stderr)?
        );
        logs.push(ProcessLog {
            role,
            image: container
                .pointer("/Config/Image")
                .and_then(Value::as_str)
                .map(str::to_owned),
            image_id: container["Image"].as_str().map(str::to_owned),
            source_revision: container
                .pointer("/Config/Labels/org.opencontainers.image.revision")
                .and_then(Value::as_str)
                .map(str::to_owned),
            complete: text.lines().count() < 20000,
            text,
        });
    }
    Ok(logs)
}

async fn aks_process_logs(config: &OatConfig) -> Result<Vec<ProcessLog>> {
    let pods: Value = serde_json::from_str(
        &kubectl(
            config,
            vec!["get", "pods", "--namespace", &config.namespace, "-o", "json"],
            None,
        )
        .await?,
    )?;
    let mut logs = Vec::new();
    for pod in pods["items"]
        .as_array()
        .context("AKS process evidence has no pod list")?
    {
        let pod_name = pod
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .context("AKS pod has no name")?;
        for status in pod
            .pointer("/status/containerStatuses")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let name = status["name"].as_str().context("AKS container status has no name")?;
            let role = if name == config.dfe_container {
                ComponentRole::Collector
            } else if name == "router" {
                ComponentRole::Router
            } else if name == "maintenance" {
                ComponentRole::Maintenance
            } else {
                continue;
            };
            if status["restartCount"].as_u64().unwrap_or(0) > 0 {
                bail!("required AKS measurement process restarted during the cell");
            }
            let text = kubectl(
                config,
                vec![
                    "logs",
                    pod_name,
                    "--namespace",
                    &config.namespace,
                    "--container",
                    name,
                    "--limit-bytes",
                    "16777216",
                ],
                None,
            )
            .await?;
            logs.push(ProcessLog {
                role,
                image: status["image"].as_str().map(str::to_owned),
                image_id: status["imageID"].as_str().map(str::to_owned),
                source_revision: None,
                complete: text.len() < 16777216,
                text,
            });
        }
    }
    Ok(logs)
}

#[cfg(test)]
mod startup_readback_tests {
    use super::*;

    #[test]
    fn requested_nondefault_is_not_effective_without_matching_consumer_readback() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = serde_json::from_value(json!({"client":{"batch_rows":257}}))?;
        assert!(verify_process_configuration(&config, ComponentRole::Client, &json!({})).is_err());
        assert!(verify_process_configuration(&config, ComponentRole::Client, &json!({"batch_rows":256})).is_err());
        let readback =
            process_configurations("unrelated log\nOAT_CLIENT_EFFECTIVE_CONFIG {\"batch_rows\":257,\"workers\":4}\n")?;
        verify_process_configuration(&config, ComponentRole::Client, &readback)?;
        assert_eq!(readback["workers"], 4);
        Ok(())
    }

    #[test]
    fn readback_rejects_conflicting_startups_and_never_republishes_private_fields() -> Result<()> {
        assert!(
            process_configurations(
                "OAT_CLIENT_EFFECTIVE_CONFIG {\"batch_rows\":1}\nOAT_CLIENT_EFFECTIVE_CONFIG {\"batch_rows\":2}\n"
            )
            .is_err()
        );
        let readback = process_configurations(
            "OAT_CLIENT_EFFECTIVE_CONFIG {\"batch_rows\":1,\"secret_file\":\"private\",\"endpoint\":\"https://private\"}\n",
        )?;
        assert_eq!(readback, json!({"batch_rows":1}));
        Ok(())
    }
}
