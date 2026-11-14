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
                    if !matches!(lower.as_str(), "authority_timeout" | "listener_tasks_per_fd")
                        && ["secret", "password", "token", "endpoint", "listen", "address", "auth"]
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
    let mut resolution_seen = false;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("OAT_CLIENT_RESOLUTION ") {
            if resolution_seen {
                bail!("duplicate client resolution source");
            }
            resolution_seen = true;
            let value: Value = serde_json::from_str(value).context("invalid client resolution source")?;
            if !value.is_object() || value["version"] != 1 {
                bail!("unsupported client resolution source");
            }
            configuration["resolution"] =
                public_tuning_value(&value).context("client resolution source was not public")?;
            continue;
        }
        let value = [
            "arrowcol_effective_config ",
            "arrowcol_router_effective_config ",
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
            if value["component"] == "layout" && value["scope"] == "table_effective" {
                continue;
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
    if let Some(wal) = wal_process_configuration(text)? {
        configuration["wal"] = public_tuning_value(&wal).context("WAL readback was not public")?;
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

fn client_sentinel_value(
    config: &OatConfig,
    variable: &str,
    requested: &Value,
    observed: &Value,
) -> Result<Option<Value>> {
    if *requested != 0 {
        return Ok(None);
    }
    let field = match variable {
        "ARROWCOL_CLIENT_WORKERS" => "workers",
        "ARROWCOL_CLIENT_RUNTIME_THREADS" => "runtime_threads",
        "ARROWCOL_CLIENT_BATCH_ROWS" => "batch_rows",
        "ARROWCOL_CLIENT_CORPUS_ROWS" => "corpus_rows",
        "ARROWCOL_CLIENT_STREAMS_PER_WORKER" => "streams_per_worker",
        _ => return Ok(None),
    };
    let proof = observed
        .get("resolution")
        .context("automatic client tuning requires consumer resolution-source evidence")?;
    if proof["version"] != 1
        || proof["requested"][field] != 0
        || proof["legacy"]["rows"] != config.rows_per_table
        || proof["legacy"]["concurrency"] != config.dmv_concurrency
    {
        bail!("client sentinel resolution source disagrees with requested tuning or legacy inputs");
    }
    let resolved = match field {
        "workers" | "runtime_threads" => {
            let cores = proof["available_parallelism"]
                .as_u64()
                .filter(|cores| (1..=1024).contains(cores))
                .context("automatic client tuning has no bounded available-parallelism observation")?;
            json!(cores)
        }
        "batch_rows" => json!(config.rows_per_table),
        "streams_per_worker" => json!(config.dmv_concurrency),
        "corpus_rows" => {
            let bytes = config
                .tunables
                .client
                .as_ref()
                .and_then(|client| client.corpus_bytes)
                .unwrap_or(0);
            if proof["requested"]["corpus_bytes"] != bytes || proof["resolved"]["corpus_bytes"] != bytes {
                bail!("client corpus resolution source disagrees with configured byte budget");
            }
            let (source, rows) = if bytes > 0 {
                ("corpus_bytes_estimate_deferred_until_preparation", 0)
            } else {
                ("legacy_rows", config.rows_per_table)
            };
            if proof["corpus_rows_source"] != source {
                bail!("client corpus resolution source is inconsistent");
            }
            json!(rows)
        }
        _ => bail!("client sentinel adapter is missing"),
    };
    if proof["resolved"][field] != resolved {
        bail!("client resolved tuning contradicts its observed resolution inputs");
    }
    Ok(Some(resolved))
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
    if matches!(role, ComponentRole::Collector) {
        verify_wal_configuration(config, observed)?;
    }
    if matches!(role, ComponentRole::Maintenance) && maintenance_enabled(config) {
        verify_maintenance_controls(config, observed)?;
    }
    for (name, expected) in role_environment(config, role)? {
        if name.starts_with("ARROWCOL_WAL_") {
            continue;
        }
        let mut expected = serde_json::from_str::<Value>(&expected).unwrap_or(Value::String(expected));
        if matches!(role, ComponentRole::Client)
            && let Some(resolved) = client_sentinel_value(config, &name, &expected, observed)?
        {
            expected = resolved;
        }
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
            "ARROWCOL_COLLECTOR_CATALOG_CONFIG" => "/catalog".to_owned(),
            "ARROWCOL_COLLECTOR_ROUTING_CONFIG" => "/routing".to_owned(),
            "ARROWCOL_COLLECTOR_EXPORTER_CONFIG" => String::new(),
            "ARROWCOL_COLLECTOR_LAYOUT_ENABLED" => "/layout/process_policy/enabled".to_owned(),
            "ARROWCOL_COLLECTOR_SORT_COLUMNS" | "ARROWCOL_COLLECTOR_BLOOM_COLUMNS" => {
                expected = json!(
                    expected
                        .as_str()
                        .context("collector columns must be comma separated")?
                        .split(',')
                        .filter(|value| !value.is_empty())
                        .collect::<Vec<_>>()
                );
                format!(
                    "/layout/process_policy/{}",
                    name.trim_start_matches("ARROWCOL_COLLECTOR_").to_ascii_lowercase()
                )
            }
            variable if variable.starts_with("ARROWCOL_COLLECTOR_LAYOUT_") => format!(
                "/layout/process_policy/{}",
                variable
                    .trim_start_matches("ARROWCOL_COLLECTOR_LAYOUT_")
                    .to_ascii_lowercase()
            ),
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

fn collector_layout_records(config: &OatConfig, text: &str) -> Result<Vec<Value>> {
    let tables = config
        .tables
        .iter()
        .map(|table| canonical_dmv_table_name(table))
        .collect::<Result<Vec<_>>>()?;
    let mut records = Vec::new();
    for line in text
        .lines()
        .filter_map(|line| line.strip_prefix("arrowcol_effective_config "))
    {
        let value: Value = serde_json::from_str(line).context("invalid collector layout readback")?;
        if value["component"] != "layout" || value["scope"] != "table_effective" {
            continue;
        }
        let Some(table) = value["table_name"]
            .as_str()
            .filter(|table| tables.iter().any(|allowed| allowed.as_str() == *table))
        else {
            continue;
        };
        let id = value["table_id"]
            .as_str()
            .filter(|id| {
                id.len() == 64
                    && id
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            .context("collector layout omitted canonical table hash")?;
        let enabled = value["config"]["enabled"]
            .as_bool()
            .context("collector layout omitted enabled state")?;
        let mut policy = serde_json::Map::from_iter([("enabled".to_owned(), json!(enabled))]);
        for field in [
            "sort_columns",
            "bloom_columns",
            "max_rows",
            "max_input_bytes",
            "row_group_rows",
            "page_rows",
        ] {
            let actual = value["config"]
                .get(field)
                .context("collector layout omitted resolved field")?;
            if !enabled {
                if !actual.is_null() {
                    bail!("disabled collector layout reported applicable settings");
                }
            } else if matches!(field, "sort_columns" | "bloom_columns") {
                let columns = actual.as_array().context("collector layout columns must be arrays")?;
                if !columns.iter().all(|column| {
                    column.as_str().is_some_and(|name| {
                        !name.is_empty()
                            && name.len() <= 128
                            && name.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                    })
                }) {
                    bail!("collector layout reported invalid column names");
                }
            } else if actual.as_u64().is_none_or(|value| value == 0) {
                bail!("collector layout reported invalid resolved limit");
            }
            policy.insert(field.to_owned(), actual.clone());
        }
        let record = json!({"table_id":id,"table_name":table,"config":policy,
            "scope":"table_effective_at_file_construction_not_full_run_history"});
        if !records.contains(&record) {
            if records.len() >= 256 {
                bail!("collector layout policy history exceeded evidence bound");
            }
            records.push(record);
        }
    }
    Ok(records)
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
            || (matches!(role, ComponentRole::Maintenance) && maintenance_enabled(config))
            || (matches!(role, ComponentRole::Collector) && config.tunables.wal.is_some()))
            && selected.is_empty()
        {
            bail!("configured {} has no running consumer/readback", role_name(role));
        }
        for log in selected {
            if matches!(role, ComponentRole::Maintenance) && !log.complete {
                bail!("maintenance operation log was truncated; safety outcomes cannot be established");
            }
            let resolved = process_configurations(&log.text)?;
            verify_process_configuration(config, role, &resolved)?;
            let observed = !resolved.as_object().is_none_or(serde_json::Map::is_empty);
            let maintenance_tables = if matches!(role, ComponentRole::Maintenance) {
                Some(maintenance_table_evidence(config, &log.text)?)
            } else {
                None
            };
            let layout_tables = if matches!(role, ComponentRole::Collector) {
                Some(collector_layout_records(config, &log.text)?)
            } else {
                None
            };
            let mut effective = resolved;
            let fields = effective
                .as_object_mut()
                .context("consumer startup configuration is not an object")?;
            let writer_policy = fields
                .remove("parquet_writer")
                .or_else(|| fields.remove("parquet_writer_overrides"));
            let wal_storage_scope =
                fields
                    .contains_key("wal")
                    .then_some(if config.platform.kind == crate::config::OatMode::Local {
                        "run_owned_bind_mount_per_container_instance_not_node_failure_durable"
                    } else {
                        "per_pod_empty_dir_not_persistent_across_pod_or_node_loss"
                    });
            result.push(json!({
                "component": role_name(role),
                "effective_config": observed.then_some(effective),
                "writer_policy_overrides": writer_policy,
                "writer_policy_scope": "configured_startup_overrides_not_complete_schema_resolved_or_exercised_file_properties",
                "wal_storage_scope": wal_storage_scope,
                "effective_config_source": observed.then_some("consumer_startup_readback"),
                "effective_config_unavailable_reason": (!observed).then_some("consumer_did_not_emit_startup_readback"),
                "image": log.image,
                "image_id": log.image_id,
                "source_revision": log.source_revision,
                "log_complete": log.complete,
                "maintenance_table_policies": maintenance_tables,
                "layout_table_policies": layout_tables,
                "layout_policy_scope": "startup_process_policy_only_table_properties_and_parquet_overlays_may_override",
            }));
        }
    }
    if role_environment(config, ComponentRole::Collector)?.keys().any(|name| {
        name.starts_with("ARROWCOL_COLLECTOR_LAYOUT_")
            || matches!(
                name.as_str(),
                "ARROWCOL_COLLECTOR_SORT_COLUMNS" | "ARROWCOL_COLLECTOR_BLOOM_COLUMNS"
            )
    }) {
        let mut identities = std::collections::BTreeMap::new();
        for process in &result {
            for record in process["layout_table_policies"].as_array().into_iter().flatten() {
                let table = record["table_name"].as_str().context("layout table identity missing")?;
                let id = record["table_id"].as_str().context("layout table hash missing")?;
                if identities.insert(table, id).is_some_and(|previous| previous != id) {
                    bail!("collector layout table identity is ambiguous across processes");
                }
                if process["log_complete"] != true {
                    bail!("collector layout log is incomplete");
                }
            }
        }
        for table in &config.tables {
            if !identities.contains_key(canonical_dmv_table_name(table)?.as_str()) {
                bail!("configured collector layout has no matching table-effective file-construction evidence");
            }
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
    fn collector_layout_requires_separate_matching_table_proof_and_never_merges_it_into_startup() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let policy = json!({"enabled":true,"sort_columns":["customer_id"],"bloom_columns":[],
            "max_rows":777,"max_input_bytes":1_048_576,"row_group_rows":123,"page_rows":17});
        config.tunables = serde_json::from_value(json!({"collector":{"layout":policy}}))?;
        let startup = format!(
            "arrowcol_effective_config {}\n",
            json!({"layout":{"process_policy":policy,"table_properties_override":true}})
        );
        let readback = process_configurations(&startup)?;
        verify_process_configuration(&config, ComponentRole::Collector, &readback)?;
        let mut log = ProcessLog {
            role: ComponentRole::Collector,
            image: None,
            image_id: None,
            source_revision: None,
            complete: true,
            text: startup.clone(),
        };
        assert!(process_evidence(&config, std::slice::from_ref(&log)).is_err());
        let mut event = json!({"component":"layout","scope":"table_effective","table_id":"a".repeat(64),
            "table_name":canonical_dmv_table_name(&config.tables[0])?,"config":policy,"secret_path":"/private"});
        log.text.push_str(&format!(
            "arrowcol_effective_config {event}\narrowcol_effective_config {event}\n"
        ));
        let readback = process_configurations(&log.text)?;
        assert_eq!(readback["layout"]["process_policy"], policy);
        assert!(readback.get("table_id").is_none());
        let evidence = process_evidence(&config, std::slice::from_ref(&log))?;
        assert_eq!(
            evidence[0]["layout_table_policies"]
                .as_array()
                .context("missing table proof")?
                .len(),
            1
        );
        assert_eq!(evidence[0]["layout_table_policies"][0]["config"], policy);
        assert!(evidence[0]["layout_table_policies"][0].get("secret_path").is_none());
        let valid_log = log.text.clone();
        event["table_name"] = json!("another.catalog.table");
        log.text = format!("{startup}arrowcol_effective_config {event}\n");
        assert!(process_evidence(&config, std::slice::from_ref(&log)).is_err());
        event["table_name"] = json!(canonical_dmv_table_name(&config.tables[0])?);
        event["table_id"] = json!("b".repeat(64));
        log.text = format!("{valid_log}arrowcol_effective_config {event}\n");
        assert!(process_evidence(&config, &[log]).is_err());
        Ok(())
    }

    #[test]
    fn disabled_table_layout_has_only_null_inapplicable_fields() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let mut event = json!({"component":"layout","scope":"table_effective","table_id":"a".repeat(64),
            "table_name":canonical_dmv_table_name(&config.tables[0])?,
            "config":{"enabled":false,"sort_columns":null,"bloom_columns":null,"max_rows":null,
                "max_input_bytes":null,"row_group_rows":null,"page_rows":null}});
        let records = collector_layout_records(&config, &format!("arrowcol_effective_config {event}\n"))?;
        assert_eq!(records[0]["config"]["enabled"], false);
        assert!(records[0]["config"]["row_group_rows"].is_null());
        event["config"]["row_group_rows"] = json!(123);
        assert!(collector_layout_records(&config, &format!("arrowcol_effective_config {event}\n")).is_err());
        Ok(())
    }

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

    #[test]
    fn writer_overrides_are_not_claimed_as_schema_resolved_or_exercised_properties() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let evidence = process_evidence(&config, &[ProcessLog {
            role: ComponentRole::Collector, image: None, image_id: None, source_revision: None, complete: true,
            text: "arrowcol_effective_config {\"runtime\":{\"batch_rows\":8192},\"parquet_writer\":{\"defaults\":{\"compression\":null}}}\n".to_owned(),
        }])?;
        assert_eq!(evidence[0]["effective_config"]["runtime"]["batch_rows"], 8192);
        assert!(evidence[0]["effective_config"].get("parquet_writer").is_none());
        assert_eq!(
            evidence[0]["writer_policy_overrides"],
            json!({"defaults":{"compression":null}})
        );
        assert_eq!(
            public_tuning_value(&json!({"authority_timeout":"13s","auth_secret":"private"})),
            Some(json!({"authority_timeout":"13s"}))
        );
        Ok(())
    }

    #[test]
    fn real_router_and_collector_control_plane_shapes_preserve_public_tunables() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = serde_json::from_value(json!({
            "router":{"workers":3,"listener_tasks_per_fd":7,"sql_connect_timeout_ms":17000},
            "collector":{"catalog":{"maximum_cache_age":"13000ms"},
                "routing":{"connect_timeout":"17000ms","command_timeout":"31000ms"},
                "exporter":{"authority_timeout":"19000ms"},
                "pressure":{"heartbeat_interval":"6000ms","registration_ttl":"24000ms"}}
        }))?;
        let router = process_configurations(concat!(
            "arrowcol_router_effective_config ",
            "{\"workers\":3,\"listener_tasks_per_fd\":7,\"sql_timeouts\":{\"connect_timeout_ms\":17000},\"listen\":\"private\"}\n"
        ))?;
        verify_process_configuration(&config, ComponentRole::Router, &router)?;
        assert!(router.get("listen").is_none());
        let collector = process_configurations(concat!(
            "arrowcol_effective_config {\"component\":\"receiver\",\"config\":{\"catalog\":{\"type\":\"remote\",\"maximum_cache_age\":\"13s\"},\"routing\":{\"type\":\"sql_server\",\"connect_timeout\":\"17s\",\"command_timeout\":\"31s\"},\"pressure\":{\"heartbeat_interval\":\"6s\",\"registration_ttl\":\"24s\"}}}\n",
            "arrowcol_effective_config {\"component\":\"exporter\",\"config\":{\"routing\":{\"type\":\"sql_server\",\"connect_timeout\":\"17s\",\"command_timeout\":\"31s\"},\"authority_timeout\":\"19s\"}}\n"
        ))?;
        verify_process_configuration(&config, ComponentRole::Collector, &collector)?;
        Ok(())
    }

    #[test]
    fn client_zero_sentinels_require_matching_consumer_resolution_inputs() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = serde_json::from_value(json!({"client":{
            "workers":0,"runtime_threads":0,"batch_rows":0,"corpus_rows":0,"streams_per_worker":0
        }}))?;
        let proof = json!({"version":1,"available_parallelism":3,
            "requested":{"workers":0,"runtime_threads":0,"batch_rows":0,"corpus_rows":0,"corpus_bytes":0,"streams_per_worker":0},
            "legacy":{"rows":config.rows_per_table,"concurrency":config.dmv_concurrency},
            "resolved":{"workers":3,"runtime_threads":3,"batch_rows":config.rows_per_table,
                "corpus_rows":config.rows_per_table,"corpus_bytes":0,"streams_per_worker":config.dmv_concurrency},
            "corpus_rows_source":"legacy_rows"});
        let text = format!(
            "OAT_CLIENT_RESOLUTION {proof}\nOAT_CLIENT_EFFECTIVE_CONFIG {}\n",
            proof["resolved"]
        );
        let observed = process_configurations(&text)?;
        verify_process_configuration(&config, ComponentRole::Client, &observed)?;
        let mut invalid = observed.clone();
        invalid
            .as_object_mut()
            .context("missing readback")?
            .remove("resolution");
        assert!(verify_process_configuration(&config, ComponentRole::Client, &invalid).is_err());
        invalid = observed.clone();
        invalid["resolution"]["available_parallelism"] = json!(4);
        assert!(verify_process_configuration(&config, ComponentRole::Client, &invalid).is_err());
        invalid = observed.clone();
        invalid["resolution"]["legacy"]["rows"] = json!(config.rows_per_table + 1);
        assert!(verify_process_configuration(&config, ComponentRole::Client, &invalid).is_err());
        invalid = observed;
        invalid["runtime_threads"] = json!(4);
        assert!(verify_process_configuration(&config, ComponentRole::Client, &invalid).is_err());
        config.tunables = serde_json::from_value(json!({"client":{"corpus_rows":0,"corpus_bytes":4096}}))?;
        let deferred = json!({"corpus_rows":0,"corpus_bytes":4096,"resolution":{
            "version":1,"requested":{"corpus_rows":0,"corpus_bytes":4096},
            "legacy":{"rows":config.rows_per_table,"concurrency":config.dmv_concurrency},
            "resolved":{"corpus_rows":0,"corpus_bytes":4096},
            "corpus_rows_source":"corpus_bytes_estimate_deferred_until_preparation"}});
        verify_process_configuration(&config, ComponentRole::Client, &deferred)?;
        assert!(process_configurations(&format!("{text}OAT_CLIENT_RESOLUTION {proof}\n")).is_err());
        Ok(())
    }

    #[test]
    #[ignore = "requires cargo build --package collector-chain --bin arrowcol-dmv"]
    fn automatic_values_are_accepted_from_actual_client_binary() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = serde_json::from_value(json!({"client":{
            "workers":0,"runtime_threads":0,"batch_rows":0,"corpus_rows":0,"streams_per_worker":0
        }}))?;
        let binary = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("target")
            .join("debug")
            .join(format!("arrowcol-dmv{}", std::env::consts::EXE_SUFFIX));
        let mut command = std::process::Command::new(binary);
        command.args([
            "--endpoint",
            "http://127.0.0.1:0",
            "--table",
            &config.tables[0],
            "--rows",
            &config.rows_per_table.to_string(),
            "--concurrency",
            &config.dmv_concurrency.to_string(),
        ]);
        command.envs(role_environment(&config, ComponentRole::Client)?);
        let output = command
            .output()
            .context("launch actual client binary for startup propagation smoke")?;
        assert!(
            !output.status.success(),
            "unreachable endpoint must not look successful"
        );
        let text = format!(
            "{}\n{}",
            String::from_utf8(output.stdout)?,
            String::from_utf8(output.stderr)?
        );
        let observed = process_configurations(&text)?;
        verify_process_configuration(&config, ComponentRole::Client, &observed)?;
        assert!(observed["workers"].as_u64().is_some_and(|workers| workers > 0));
        assert_eq!(observed["workers"], observed["resolution"]["available_parallelism"]);
        assert_eq!(
            observed["runtime_threads"],
            observed["resolution"]["available_parallelism"]
        );
        Ok(())
    }
}
