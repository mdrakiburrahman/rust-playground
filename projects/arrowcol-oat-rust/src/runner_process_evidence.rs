#[derive(Clone, Debug)]
struct ProcessLog {
    role: ComponentRole,
    instance_id: String,
    image: Option<String>,
    image_id: Option<String>,
    source_revision: Option<String>,
    complete: bool,
    raw_log_bytes: u64,
    raw_log_lines: u64,
    stdout: String,
    stderr: String,
    text: String,
}

const MAX_RETAINED_PROCESS_LOG_BYTES: usize = 64 * 1024 * 1024;

struct FilteredProcessOutput {
    stdout: String,
    stderr: String,
    raw_log_bytes: u64,
    raw_log_lines: u64,
}

fn has_startup_envelope(text: &str, prefix: &str, required_component: Option<&str>) -> bool {
    text.lines().any(|line| {
        let Some(json) = line.strip_prefix(prefix) else {
            return false;
        };
        let Ok(value) = serde_json::from_str::<Value>(json) else {
            return false;
        };
        value.as_object().is_some_and(|object| {
            required_component.is_none_or(|required| {
                object
                    .get("component")
                    .is_some_and(|component| component.as_str() == Some(required))
            })
        })
    })
}

fn process_log_complete(role: &ComponentRole, text: &str) -> bool {
    match role {
        ComponentRole::Client => true,
        ComponentRole::Router => has_startup_envelope(text, "arrowcol_router_effective_config ", None),
        ComponentRole::Collector => has_startup_envelope(text, "arrowcol_effective_config ", Some("receiver")),
        ComponentRole::Maintenance => has_startup_envelope(text, "arrowcol_maintenance_effective_config ", None),
    }
}

fn is_process_evidence_line(line: &str) -> bool {
    [
        "arrowcol_effective_config ",
        "df_engine_effective_config ",
        "arrowcol_router_effective_config ",
        "arrowcol_wal_effective_config ",
        "arrowcol_layout_",
        "arrowcol_data_commit ",
        "arrowcol_maintenance_",
        "delta-maintenance ",
        "OAT_CLIENT_",
        "otel.controller.main::shutdown.signal_received",
        "otel.processor.durable_buffer::durable_buffer.shutdown.",
        "otel-arrow-dfe-quiver::quiver.engine.stop",
    ]
    .iter()
    .any(|prefix| line.contains(prefix))
        || line.trim_start().starts_with('{')
}

fn append_process_evidence_line(
    line: &str,
    output: &mut String,
    retained_bytes: &mut usize,
    action: &str,
) -> Result<()> {
    if !is_process_evidence_line(line) {
        return Ok(());
    }
    let bytes = line.len().saturating_add(1);
    if retained_bytes.saturating_add(bytes) > MAX_RETAINED_PROCESS_LOG_BYTES {
        bail!(
            "{action} exceeded the {} byte retained evidence cap; refusing partial process logs",
            MAX_RETAINED_PROCESS_LOG_BYTES
        );
    }
    output.push_str(line);
    output.push('\n');
    *retained_bytes += bytes;
    Ok(())
}

fn append_process_record(bytes: &[u8], output: &mut String, retained_bytes: &mut usize, action: &str) -> Result<()> {
    let without_lf = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    let line = without_lf.strip_suffix(b"\r").unwrap_or(without_lf);
    let line = std::str::from_utf8(line).context("process log contains a non-UTF-8 line")?;
    let before = output.len();
    append_process_evidence_line(line, output, retained_bytes, action)?;
    if output.len() > before && !bytes.ends_with(b"\n") {
        output.pop();
    }
    Ok(())
}

async fn filtered_process_command_output(mut command: Command, action: &str) -> Result<FilteredProcessOutput> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("{action}: start process"))?;
    let stdout = child.stdout.take().context("process log stdout is missing")?;
    let stderr = child.stderr.take().context("process log stderr is missing")?;
    let mut stdout = BufReader::new(stdout);
    let mut stderr = BufReader::new(stderr);
    let mut stdout_open = true;
    let mut stderr_open = true;
    let mut stdout_line = Vec::new();
    let mut stderr_line = Vec::new();
    let mut retained_bytes = 0;
    let mut output = FilteredProcessOutput {
        stdout: String::new(),
        stderr: String::new(),
        raw_log_bytes: 0,
        raw_log_lines: 0,
    };
    while stdout_open || stderr_open {
        tokio::select! {
            result = stdout.read_until(b'\n', &mut stdout_line), if stdout_open => {
                let bytes_read = result?;
                if bytes_read == 0 {
                    stdout_open = false;
                } else {
                    output.raw_log_bytes = output.raw_log_bytes
                        .checked_add(u64::try_from(bytes_read).context("raw log byte count overflow")?)
                        .context("raw log byte count overflow")?;
                    output.raw_log_lines = output.raw_log_lines
                        .checked_add(1)
                        .context("raw log line count overflow")?;
                    append_process_record(&stdout_line, &mut output.stdout, &mut retained_bytes, action)?;
                    stdout_line.clear();
                }
            },
            result = stderr.read_until(b'\n', &mut stderr_line), if stderr_open => {
                let bytes_read = result?;
                if bytes_read == 0 {
                    stderr_open = false;
                } else {
                    output.raw_log_bytes = output.raw_log_bytes
                        .checked_add(u64::try_from(bytes_read).context("raw log byte count overflow")?)
                        .context("raw log byte count overflow")?;
                    output.raw_log_lines = output.raw_log_lines
                        .checked_add(1)
                        .context("raw log line count overflow")?;
                    append_process_record(&stderr_line, &mut output.stderr, &mut retained_bytes, action)?;
                    stderr_line.clear();
                }
            },
        }
    }
    let status = child
        .wait()
        .await
        .with_context(|| format!("{action}: wait for process"))?;
    if !status.success() {
        bail!("{action} failed with {status}");
    }
    Ok(output)
}

#[cfg(test)]
fn filter_process_evidence_text(text: &str) -> Result<String> {
    let mut filtered = String::new();
    let mut retained_bytes = 0;
    for line in text.split_inclusive('\n') {
        append_process_record(line.as_bytes(), &mut filtered, &mut retained_bytes, "process log filtering")?;
    }
    Ok(filtered)
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
                    let public_endpoint_limit = matches!(
                        lower.as_str(),
                        "endpoint_concurrency_limit" | "endpoint_buffer_size"
                    ) && value.as_u64().is_some();
                    if !public_endpoint_limit
                        && !matches!(lower.as_str(), "authority_timeout" | "listener_tasks_per_fd")
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
            "df_engine_effective_config ",
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

fn matches_collector_pressure(observed: &Value, requested: &Value) -> Result<bool> {
    let (Some(observed), Some(requested)) = (observed.as_object(), requested.as_object()) else {
        return Ok(false);
    };
    for (field, expected) in requested {
        let Some(actual) = observed.get(field) else {
            return Ok(false);
        };
        if matches!(field.as_str(), "pressure_interval" | "heartbeat_interval" | "registration_ttl") {
            let parse = |value: &Value| {
                let value = value.as_str().with_context(|| format!("collector pressure {field} must be a duration string"))?;
                arrowcol_tunables::parse_tunable_duration(field, value)
            };
            if parse(actual)? != parse(expected)? {
                return Ok(false);
            }
        } else if !contains_requested(actual, expected) {
            return Ok(false);
        }
    }
    Ok(true)
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
        "ARROWCOL_CLIENT_STRING_WIDTH" => "string_width",
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
        "string_width" => {
            let profile = config.tunables.client.as_ref().and_then(|client| client.data_profile).unwrap_or_default();
            let profile = serde_json::to_value(profile)?;
            if proof["requested"]["data_profile"] != profile || proof["resolved"]["data_profile"] != profile
                || observed["data_profile"] != profile
            {
                bail!("client string_width resolution disagrees with data_profile");
            }
            if profile == "realistic" { json!(96) } else { json!(0) }
        }
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
        "SQL_POOL_MAX_SIZE" | "SQL_POOL_ACQUIRE_TIMEOUT_MS" => format!(
            "/sql_pool/{}",
            field.trim_start_matches("SQL_POOL_").to_ascii_lowercase()
        ),
        "REFRESH_INTERVAL_MS"
        | "MAX_SNAPSHOT_AGE_MS"
        | "MAX_FUTURE_SKEW_MS"
        | "MAX_TABLE_ENTRIES"
        | "MAX_OLDEST_BUFFER_AGE_MS"
        | "HARD_PRESSURE_BPS"
        | "EXCLUDED_ADMISSION_GAUGES"
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
    verify_process_configuration_recording(config, role, observed, &mut None)
}

fn verify_process_configuration_recording(
    config: &OatConfig,
    role: ComponentRole,
    observed: &Value,
    diff: &mut Option<Value>,
) -> Result<()> {
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
        if name == "ARROWCOL_ROUTER_EXCLUDED_ADMISSION_GAUGES" {
            expected = json!(
                expected
                    .as_str()
                    .context("router excluded admission gauges must be comma separated")?
                    .split(',')
                    .filter(|value| !value.is_empty())
                    .collect::<Vec<_>>()
            );
        }
        if matches!(role, ComponentRole::Maintenance) && name == "ARROWCOL_MAINT_OPERATION_TIMEOUT_SECS" {
            continue;
        }
        if matches!(role, ComponentRole::Maintenance) && name.starts_with("ARROWCOL_MAINT_") {
            let (actual, expected) = maintenance_readback_value(observed, &name, expected)?;
            if !contains_requested(&actual, &expected) {
                *diff = Some(startup_mismatch(role, &name, &expected, Some(&actual)));
                bail!("maintenance startup does not match applied tunable {name}");
            }
            continue;
        }
        let path = match name.as_str() {
            "RUST_LOG" => "/log_filter".to_owned(),
            "ARROWCOL_PARQUET_CONFIG" if matches!(role, ComponentRole::Maintenance) => {
                "/parquet_writer_overrides".to_owned()
            }
            "ARROWCOL_PARQUET_CONFIG" => "/parquet_writer".to_owned(),
            "ARROWCOL_COLLECTOR_BATCHING_CONFIG" => "/batching".to_owned(),
            "ARROWCOL_COLLECTOR_COMMIT_CONFIG" => "/commit".to_owned(),
            "ARROWCOL_COLLECTOR_LIMITS_CONFIG" => "/limits".to_owned(),
            "ARROWCOL_COLLECTOR_PRESSURE_CONFIG" => "/pressure".to_owned(),
            "ARROWCOL_COLLECTOR_CPU_PRESSURE" => "/cpu_pressure".to_owned(),
            "ARROWCOL_COLLECTOR_SHUTDOWN_DRAIN_TIMEOUT_SECONDS" => {
                "/engine/shutdown_drain_timeout_seconds".to_owned()
            }
            "ARROWCOL_COLLECTOR_CATALOG_CONFIG" => "/catalog".to_owned(),
            "ARROWCOL_COLLECTOR_ROUTING_CONFIG" => "/routing".to_owned(),
            "ARROWCOL_COLLECTOR_EXPORTER_CONFIG" => String::new(),
            "ARROWCOL_COLLECTOR_LAYOUT_ENABLED" => "/layout/process_policy/enabled".to_owned(),
            "ARROWCOL_COLLECTOR_SORT_OPTIONS" => {
                expected = serde_json::to_value(
                    expected.as_str().context("collector sort options must be comma separated")?
                        .split(',')
                        .map(arrow_deltalake::layout::SortKeyOptions::from_property)
                        .collect::<Result<Vec<_>>>()?
                )?;
                "/layout/process_policy/sort_options".to_owned()
            }
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
        let Some(actual) = observed.pointer(&path) else {
            *diff = Some(startup_mismatch(role, &name, &expected, None));
            bail!("{} startup readback omitted applied tunable {name}", role_name(role));
        };
        let matches = if name == "ARROWCOL_COLLECTOR_PRESSURE_CONFIG" {
            matches_collector_pressure(actual, &expected)
        } else {
            Ok(contains_requested(actual, &expected))
        };
        if !matches.as_ref().is_ok_and(|equal| *equal) {
            *diff = Some(startup_mismatch(role, &name, &expected, Some(actual)));
        }
        if !matches? {
            bail!(
                "{} startup readback does not match applied tunable {name}",
                role_name(role)
            );
        }
    }
    if matches!(role, ComponentRole::Router)
        && !role_environment(config, role)?.contains_key("RUST_LOG")
    {
        if observed["log_filter_source"].as_str() != Some("config") {
            bail!("router inherited logging must come from its configuration file");
        }
        let expected = inherited_router_log_filter(config)?;
        let actual = observed["log_filter"]
            .as_str()
            .context("router startup readback omitted the inherited config log filter")?;
        if actual != expected {
            bail!("router startup log filter does not match its platform config");
        }
    }
    Ok(())
}

fn inherited_router_log_filter(config: &OatConfig) -> Result<String> {
    let content = match config.platform.kind {
        crate::config::OatMode::Local => {
            let compose_file = config
                .platform
                .compose_file
                .as_deref()
                .context("local router config requires platform.compose_file")?;
            let router_yaml_path = std::path::Path::new(compose_file)
                .parent()
                .map(|parent| parent.join("config/router.yaml"))
                .context("local compose file must have a parent directory")?;
            std::fs::read_to_string(&router_yaml_path)
                .with_context(|| format!("read rendered local router config {}", router_yaml_path.display()))?
        }
        crate::config::OatMode::Aks => {
            include_str!("../../arrowcol-router-rust/config.docker.yaml").to_owned()
        }
    };
    let yaml: Value = serde_yaml::from_str(&content).context("parse inherited router YAML")?;
    yaml.pointer("/telemetry/log_filter")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .context("router YAML omitted telemetry.log_filter")
}

fn parquet_column_readback(value: &Value) -> Option<Value> {
    let columns = value.as_object()?;
    if columns.is_empty() || columns.len() > 256 {
        return None;
    }
    for (name, settings) in columns {
        if name.is_empty() || name.len() > 128
            || !name.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.'))
        {
            return None;
        }
        let settings = settings.as_object()?;
        if settings.len() != 2 {
            return None;
        }
        let level = settings.get("compression_level")?;
        match settings.get("compression")?.as_str()? {
            "uncompressed" | "snappy" | "lz4" | "lz4_raw" | "lzo" if level.is_null() => {}
            "gzip" if level.as_i64().is_some_and(|level| (0..=9).contains(&level)) => {}
            "brotli" if level.as_i64().is_some_and(|level| (0..=11).contains(&level)) => {}
            "zstd" if level.as_i64().is_some_and(|level| (1..=22).contains(&level)) => {}
            _ => return None,
        }
    }
    Some(value.clone())
}

fn parquet_column_options_readback(value: &Value) -> Option<Value> {
    let columns = value.as_object()?;
    if columns.is_empty() || columns.len() > 256 {
        return None;
    }
    for (name, settings) in columns {
        if name.is_empty() || name.len() > 128
            || !name.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.'))
        {
            return None;
        }
        let fields = settings.as_object()?;
        if fields.len() != 10
            || !matches!(fields.get("encoding")?, Value::Null)
                && !matches!(fields.get("encoding")?.as_str(), Some("plain" | "rle" | "delta_binary_packed"
                    | "delta_length_byte_array" | "delta_byte_array" | "byte_stream_split"))
            || !matches!(fields.get("statistics")?.as_str(), Some("none" | "chunk" | "page"))
            || ["dictionary_enabled", "write_page_header_statistics", "bloom_filter_enabled"]
                .iter().any(|name| fields.get(*name).is_none_or(|value| !value.is_boolean()))
            || ["dictionary_page_size_limit", "data_page_size_limit"]
                .iter().any(|name| fields.get(*name).and_then(Value::as_u64).is_none_or(|value| value == 0))
            || fields.get("data_page_v2_compression_ratio_threshold")?.as_f64().is_none_or(|value| !value.is_finite() || value <= 0.0)
        {
            return None;
        }
        let fpp = fields.get("bloom_filter_fpp")?;
        let ndv = fields.get("bloom_filter_ndv")?;
        if fields["bloom_filter_enabled"] == true {
            if fpp.as_f64().is_none_or(|value| !value.is_finite() || !(0.0..1.0).contains(&value) || value == 0.0)
                || ndv.as_u64().is_none_or(|value| value == 0)
            {
                return None;
            }
        } else if !fpp.is_null() || !ndv.is_null() {
            return None;
        }
    }
    Some(value.clone())
}

fn collector_adaptive_records(config: &OatConfig, text: &str) -> Result<Vec<Value>> {
    let tables = config.tables.iter().map(|table| canonical_dmv_table_name(table))
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    let mut records = Vec::new();
    for line in text.lines().filter_map(|line| line.strip_prefix("arrowcol_adaptive_config ")) {
        let value: Value = serde_json::from_str(line).context("invalid adaptive policy readback")?;
        let table = value["table_name"].as_str().context("adaptive policy readback omitted table identity")?;
        if !tables.contains(table) {
            continue;
        }
        if value["scope"] != "installed_estimator_not_full_run_history" {
            bail!("invalid adaptive policy readback scope");
        }
        let fields = value["config"].as_object().context("missing adaptive policy config")?;
        if fields.len() != 7 {
            bail!("invalid adaptive policy fields");
        }
        let integer = |name| fields.get(name).and_then(Value::as_u64).context("invalid adaptive policy integer");
        let ratio = |name| fields.get(name).and_then(Value::as_f64).context("invalid adaptive policy ratio");
        let policy = arrow_deltalake::sizing::AdaptiveFileSizeConfig {
            target_parquet_bytes: integer("target_parquet_bytes")?,
            min_input_bytes: integer("min_input_bytes")?,
            max_input_bytes: integer("max_input_bytes")?,
            cold_start_parquet_ratio: ratio("cold_start_parquet_ratio")?,
            ewma_weight: ratio("ewma_weight")?,
            min_parquet_ratio: ratio("min_parquet_ratio")?,
            max_parquet_ratio: ratio("max_parquet_ratio")?,
        };
        policy.validate().context("invalid adaptive policy readback")?;
        let record = json!({"table_name":table, "scope":"installed_estimator_not_full_run_history",
            "config":value["config"]});
        if !records.contains(&record) {
            if records.len() >= 256 {
                bail!("adaptive policy history exceeded evidence bound");
            }
            records.push(record);
        }
    }
    Ok(records)
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
        let properties = &value["parquet_properties"];
        let valid_properties = properties.as_object().is_some_and(|object| {
            object.len() == 6
                && matches!(properties["writer_version"].as_str(), Some("v1" | "v2"))
                && properties["write_batch_size"].as_u64().is_some_and(|value| value > 0)
                && properties["offset_index_enabled"].is_boolean()
                && ["max_row_group_bytes", "column_index_truncate_length", "statistics_truncate_length"]
                    .iter().all(|field| object.get(*field).is_some_and(|value| {
                        value.is_null() || value.as_u64().is_some_and(|number| number > 0)
                    }))
        });
        let record = json!({"table_id":id,"table_name":table,"config":policy,
            "parquet_columns":parquet_column_readback(&value["parquet_columns"]),
            "parquet_column_options":parquet_column_options_readback(&value["parquet_column_options"]),
            "parquet_properties":valid_properties.then_some(properties),
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

#[cfg(test)]
fn process_evidence(config: &OatConfig, logs: &[ProcessLog]) -> Result<Value> {
    let mut logs = logs.to_vec();
    let router_environment = config.tunables.environment_for(ComponentRole::Router)?;
    let added_router = !logs.iter().any(|log| matches!(log.role, ComponentRole::Router))
        && router_environment.contains_key("RUST_LOG")
        && router_environment.keys().all(|name| name == "RUST_LOG");
    if added_router {
        let log_filter = config.tunables.environment_for(ComponentRole::Router)?["RUST_LOG"].clone();
        logs.push(ProcessLog {
            role: ComponentRole::Router,
            instance_id: "router-test-default".to_owned(),
            image: None,
            image_id: None,
            source_revision: None,
            complete: true,
            raw_log_bytes: 0,
            raw_log_lines: 1,
            stdout: String::new(),
            stderr: String::new(),
            text: format!("arrowcol_router_effective_config {{\"log_filter\":{}}}\n", serde_json::to_string(&log_filter)?),
        });
    }
    let mut evidence = process_evidence_for_cell(config, &logs, &[], &mut None)?;
    if added_router && let Some(entries) = evidence.as_array_mut() {
        entries.retain(|entry| entry["component"] != "router");
    }
    Ok(evidence)
}

fn process_evidence_for_cell(
    config: &OatConfig,
    logs: &[ProcessLog],
    baseline_logs: &[ProcessLog],
    diff: &mut Option<Value>,
) -> Result<Value> {
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
            verify_process_configuration_recording(config, role, &resolved, diff)?;
            let observed = !resolved.as_object().is_none_or(serde_json::Map::is_empty);
            let maintenance_tables = if matches!(role, ComponentRole::Maintenance) {
                Some(maintenance_table_evidence(config, &log.text)?)
            } else {
                None
            };
            let maintenance_outcomes = matches!(role, ComponentRole::Maintenance)
                .then(|| maintenance_cell_outcome_evidence(log, baseline_logs));
            let layout_tables = if matches!(role, ComponentRole::Collector) {
                Some(collector_layout_records(config, &log.text)?)
            } else {
                None
            };
            let adaptive_tables = if matches!(role, ComponentRole::Collector) {
                Some(collector_adaptive_records(config, &log.text)?)
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
                "instance_id": log.instance_id,
                "startup_tunables_verified": true,
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
            "raw_log_bytes": log.raw_log_bytes,
            "raw_log_lines": log.raw_log_lines,
                "maintenance_table_policies": maintenance_tables,
                "maintenance_operation_outcomes": maintenance_outcomes,
                "layout_table_policies": layout_tables,
                "adaptive_table_policies": adaptive_tables,
                "layout_policy_scope": "startup_process_policy_only_table_properties_and_parquet_overlays_may_override",
            }));
        }
    }
    if role_environment(config, ComponentRole::Collector)?.keys().any(|name| {
        name.starts_with("ARROWCOL_COLLECTOR_LAYOUT_")
            || matches!(
                name.as_str(),
                "ARROWCOL_COLLECTOR_SORT_COLUMNS" | "ARROWCOL_COLLECTOR_SORT_OPTIONS" | "ARROWCOL_COLLECTOR_BLOOM_COLUMNS"
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

fn maintenance_cell_outcome_evidence(log: &ProcessLog, baseline_logs: &[ProcessLog]) -> Value {
    let unavailable = |comment| {
        json!({
            "status": "unavailable",
            "cell_scope": "unavailable",
            "operation_count": null,
            "operation_failure": null,
            "retry_kind_counts": null,
            "error_kind_counts": null,
            "operations": null,
            "comment": comment,
        })
    };
    let Some(baseline) = baseline_logs
        .iter()
        .find(|baseline| baseline.role == ComponentRole::Maintenance && baseline.instance_id == log.instance_id)
    else {
        return unavailable("No matching maintenance process baseline was available for this cell.");
    };
    if !baseline.complete || !log.complete {
        return unavailable("Maintenance process logs were truncated, so cell outcomes could not be isolated.");
    }
    if [&baseline.stdout, &baseline.stderr].iter().any(|stream| !stream.is_empty() && !stream.ends_with('\n')) {
        return unavailable("Maintenance baseline ended in a partial record; cell outcomes are unknown.");
    }
    let Some(stdout) = log.stdout.strip_prefix(&baseline.stdout) else {
        return unavailable("Maintenance stdout changed or rotated, so cell outcomes could not be isolated.");
    };
    let Some(stderr) = log.stderr.strip_prefix(&baseline.stderr) else {
        return unavailable("Maintenance stderr changed or rotated, so cell outcomes could not be isolated.");
    };
    let appended = format!("{stdout}\n{stderr}");
    let mut operation_count = 0_u64;
    let mut retry_kind_counts = serde_json::Map::new();
    let mut error_kind_counts = serde_json::Map::new();
    let mut operations = serde_json::Map::new();
    if [stdout, stderr].iter().any(|stream| !stream.is_empty() && !stream.ends_with('\n')) {
        return unavailable("Maintenance log ended in a partial record; cell outcomes are unknown.");
    }
    for line in appended.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            if line.trim_start().starts_with('{') {
                return unavailable("A maintenance JSON record was malformed; cell outcomes are unknown.");
            }
            continue;
        };
        if value.get("operation").is_none() {
            continue;
        }
        let Some(operation) = value["operation"].as_str().filter(|operation| {
            ["optimize", "checkpoint", "purge", "vacuum", "prune_log"].contains(operation)
        }) else {
            return unavailable("A maintenance operation identity was missing or invalid.");
        };
        let record_outcome = maintenance_record_outcome(&value);
        if !value["applied"].is_boolean() || !value["table"].is_string() || record_outcome.is_none() {
            return unavailable("A maintenance operation record was partial; cell outcomes are unknown.");
        }
        operation_count = operation_count.saturating_add(1);
        let operation_counts = operations.entry(operation.to_owned()).or_insert_with(|| json!({
            "operation_count": 0, "retry_kind_counts": {}, "error_kind_counts": {},
        }));
        operation_counts["operation_count"] = json!(operation_counts["operation_count"].as_u64().unwrap_or(0) + 1);
        let error_kind = value.get("error_kind").and_then(Value::as_str);
        let failed = record_outcome != Some(MaintenanceRecordOutcome::Succeeded);
        if failed {
            let kind = sanitized_maintenance_error_kind(error_kind);
            let retry = record_outcome == Some(MaintenanceRecordOutcome::Retry);
            let counts = if retry { &mut retry_kind_counts } else { &mut error_kind_counts };
            let count = counts.entry(kind.clone()).or_insert_with(|| json!(0));
            *count = json!(count.as_u64().unwrap_or(0).saturating_add(1));
            let field = if retry { "retry_kind_counts" } else { "error_kind_counts" };
            operation_counts[field][&kind] = json!(operation_counts[field][&kind].as_u64().unwrap_or(0) + 1);
        }
    }
    if operation_count == 0 {
        return unavailable("No complete maintenance operation records were available for this cell.");
    }
    let comment = if error_kind_counts.is_empty() {
        "No operation failures in the complete cell records; optimistic-concurrency replans are counted separately."
    } else {
        "Cell-scoped maintenance errors are counted by sanitized error_kind; raw error text is omitted."
    };
    json!({
        "status": "observed",
        "cell_scope": "stdout_stderr_append_offsets",
        "operation_count": operation_count,
        "operation_failure": !error_kind_counts.is_empty(),
        "retry_kind_counts": retry_kind_counts,
        "error_kind_counts": error_kind_counts,
        "operations": operations,
        "comment": comment,
    })
}

fn maintenance_exit_classification(log: &ProcessLog, exit: Option<i64>) -> Value {
    if log.text.contains("delta-maintenance stale-plan starvation ")
        || log.text.contains("delta-maintenance pending-table status failed:")
    {
        return json!("unhealthy");
    }
    if exit == Some(1) {
        return json!("nonzero_health_exit");
    }
    let mut baseline = log.clone();
    baseline.stdout.clear();
    baseline.stderr.clear();
    baseline.complete = true;
    let lifetime = maintenance_cell_outcome_evidence(log, &[baseline]);
    let failure = lifetime["operation_failure"].as_bool();
    json!(match (exit, failure) {
        (_, Some(true)) => "operation_failure",
        (Some(0 | 143), Some(false)) => "no_operation_failures",
        _ => "unknown",
    })
}

fn log_counter(text: &str, marker: &str, field: &str) -> Option<u64> {
    text.lines()
        .filter(|line| line.contains(marker))
        .filter_map(|line| line.split_once(field).map(|(_, value)| value))
        .find_map(|value| {
            let value = value.trim_start().strip_prefix('=')?.trim_start();
            let digits = value.chars().take_while(char::is_ascii_digit).collect::<String>();
            (!digits.is_empty()).then(|| digits.parse().ok()).flatten()
        })
}

fn completed_durable_buffer_shutdowns(text: &str) -> Option<u64> {
    let started = text.lines().filter(|line| line.contains("durable_buffer.shutdown.start")).count();
    let completed = text.lines().filter(|line| line.contains("durable_buffer.shutdown.complete")).count();
    (started > 0 && started == completed).then_some(0)
}

fn collector_shutdown_drain_evidence(
    log: &ProcessLog,
    sigterm_at_unix_ms: Option<u64>,
    finished_at: Option<&str>,
    exit_code: Option<i64>,
    wal_enabled: bool,
) -> Value {
    let finished_at_unix_ms = finished_at
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .and_then(|time| u64::try_from(time.timestamp_millis()).ok());
    let sigterm_to_exit_seconds = sigterm_at_unix_ms
        .zip(finished_at_unix_ms)
        .and_then(|(signal, finish)| finish.checked_sub(signal))
        .map(|milliseconds| milliseconds as f64 / 1000.0);
    let mut result = json!({
        "status": "observed",
        "sigterm_sent": sigterm_at_unix_ms.is_some(),
        "sigterm_to_exit_seconds": sigterm_to_exit_seconds,
        "commits_after_sigterm": null,
        "committed_rows_after_sigterm": null,
        "exit_code": exit_code,
        "log_complete": log.complete,
    });
    if exit_code.is_none() {
        result["status"] = json!("unavailable");
        result["unavailable_reason"] = json!("collector_exit_code_unavailable");
    } else if sigterm_at_unix_ms.is_some() && sigterm_to_exit_seconds.is_none() {
        result["status"] = json!("unavailable");
        result["unavailable_reason"] = json!("collector_exit_timestamp_unavailable");
    }
    if !log.complete {
        result["status"] = json!("unavailable");
        result["unavailable_reason"] = json!("collector_commit_log_incomplete");
        result["quiver"] = json!({"status": if wal_enabled { "unavailable" } else { "not_applicable" }, "bundles_drained": null, "force_dropped_bundles": null});
        return result;
    }
    if let Some(signal) = sigterm_at_unix_ms {
        let mut commits = 0_u64;
        let mut rows = 0_u64;
        for line in log.text.lines() {
            let Some(payload) = line.strip_prefix("arrowcol_data_commit ") else {
                continue;
            };
            let event: CommitEvent = match serde_json::from_str(payload) {
                Ok(event) => event,
                Err(_) => {
                    result["status"] = json!("unavailable");
                    result["unavailable_reason"] = json!("malformed_collector_commit_event");
                    result["commits_after_sigterm"] = Value::Null;
                    result["committed_rows_after_sigterm"] = Value::Null;
                    break;
                }
            };
            if event.outcome != "committed" {
                continue;
            }
            let Some(finished) = event.finished_at_unix_ms.and_then(|time| u64::try_from(time).ok()) else {
                result["status"] = json!("unavailable");
                result["unavailable_reason"] = json!("collector_commit_event_missing_finish_time");
                result["commits_after_sigterm"] = Value::Null;
                result["committed_rows_after_sigterm"] = Value::Null;
                break;
            };
            if finished >= signal {
                let Some(event_rows) = event.rows else {
                    result["status"] = json!("unavailable");
                    result["unavailable_reason"] = json!("collector_commit_event_missing_rows");
                    result["commits_after_sigterm"] = Value::Null;
                    result["committed_rows_after_sigterm"] = Value::Null;
                    break;
                };
                let Some(next_commits) = commits.checked_add(1) else {
                    result["status"] = json!("unavailable");
                    result["unavailable_reason"] = json!("collector_commit_count_overflow");
                    break;
                };
                let Some(next_rows) = rows.checked_add(event_rows) else {
                    result["status"] = json!("unavailable");
                    result["unavailable_reason"] = json!("collector_commit_row_count_overflow");
                    break;
                };
                commits = next_commits;
                rows = next_rows;
            }
        }
        if result["status"] == "observed" {
            result["commits_after_sigterm"] = json!(commits);
            result["committed_rows_after_sigterm"] = json!(rows);
        }
    }
    if wal_enabled {
        let drained = log_counter(&log.text, "durable_buffer.shutdown.drained", "bundles_drained")
            .or_else(|| log_counter(&log.text, "durable_buffer.shutdown.drain_deadline", "bundles_drained"));
        let inferred = drained.is_none() && completed_durable_buffer_shutdowns(&log.text).is_some();
        let drained = drained.or_else(|| inferred.then_some(0));
        let force_dropped = log_counter(&log.text, "quiver.engine.stop", "force_dropped_bundles");
        result["quiver"] = json!({
            "status": if drained.is_some() && force_dropped.is_some() { if inferred { "inferred" } else { "observed" } } else { "unavailable" },
            "bundles_drained_source": drained.map(|_| if inferred { "matched_shutdown_start_complete" } else { "explicit_counter" }),
            "bundles_drained": drained,
            "force_dropped_bundles": force_dropped,
        });
        if drained.is_none() || force_dropped.is_none() {
            result["status"] = json!("unavailable");
            result["unavailable_reason"] = json!("quiver_shutdown_log_unavailable");
        }
    } else {
        result["quiver"] = json!({"status": "not_applicable", "bundles_drained": null, "force_dropped_bundles": null});
    }
    result
}

fn parse_compose_duration_seconds(value: &str) -> Option<u64> {
    let mut total = 0_u64;
    let mut number = 0_u64;
    let mut has_number = false;
    for character in value.chars() {
        if let Some(digit) = character.to_digit(10) {
            number = number.checked_mul(10)?.checked_add(u64::from(digit))?;
            has_number = true;
            continue;
        }
        let multiplier = match character {
            'h' => 3_600,
            'm' => 60,
            's' => 1,
            _ => return None,
        };
        if !has_number {
            return None;
        }
        total = total.checked_add(number.checked_mul(multiplier)?)?;
        number = 0;
        has_number = false;
    }
    (!has_number).then_some(total)
}

fn verify_local_collector_stop_grace(compose_config: &Value, expected_seconds: u64) -> Result<()> {
    let actual = compose_config
        .pointer("/services/dataflow-1/stop_grace_period")
        .and_then(Value::as_str)
        .and_then(parse_compose_duration_seconds)
        .context("effective local Compose config omitted the collector stop_grace_period")?;
    if actual != expected_seconds {
        bail!("effective local Compose collector stop grace differs from requested value");
    }
    Ok(())
}

async fn local_process_logs(config: &OatConfig, platform: &LocalPlatform) -> Result<Vec<ProcessLog>> {
    local_process_logs_for_role(config, platform, None).await
}

async fn local_process_logs_for_role(
    config: &OatConfig,
    platform: &LocalPlatform,
    role_filter: Option<ComponentRole>,
) -> Result<Vec<ProcessLog>> {
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
        if role_filter.is_some_and(|filter| filter != role) {
            continue;
        }
        if container["RestartCount"].as_u64().unwrap_or(0) > 0 {
            bail!("required measurement process restarted during the cell");
        }
        let id = container["Id"].as_str().context("inspected container has no ID")?;
        let mut command = Command::new("docker");
        command.args(["logs", "--tail=-1", id]);
        let capture =
            filtered_process_command_output(command, "read full owned process logs for tuning and commit evidence")
                .await?;
        let FilteredProcessOutput {
            stdout,
            stderr,
            raw_log_bytes,
            raw_log_lines,
        } = capture;
        let text = format!("{stdout}\n{stderr}");
        logs.push(ProcessLog {
            role,
            instance_id: id.to_owned(),
            image: container
                .pointer("/Config/Image")
                .and_then(Value::as_str)
                .map(str::to_owned),
            image_id: container["Image"].as_str().map(str::to_owned),
            source_revision: container
                .pointer("/Config/Labels/org.opencontainers.image.revision")
                .and_then(Value::as_str)
                .map(str::to_owned),
            complete: process_log_complete(&role, &text),
            raw_log_bytes,
            raw_log_lines,
            stdout,
            stderr,
            text,
        });
    }
    Ok(logs)
}

async fn aks_process_logs(config: &OatConfig) -> Result<Vec<ProcessLog>> {
    aks_process_logs_for_role(config, None).await
}

async fn aks_process_logs_for_role(config: &OatConfig, role_filter: Option<ComponentRole>) -> Result<Vec<ProcessLog>> {
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
            if role_filter.is_some_and(|filter| filter != role) {
                continue;
            }
            if status["restartCount"].as_u64().unwrap_or(0) > 0 {
                bail!("required AKS measurement process restarted during the cell");
            }
            let mut command = Command::new("kubectl");
            if let Some(context) = &config.kubectl_context {
                command.args(["--context", context]);
            }
            command.args(["logs", pod_name, "--namespace", &config.namespace, "--container", name]);
            let capture =
                filtered_process_command_output(command, &format!("read full AKS process logs for {pod_name}/{name}"))
                    .await?;
            let FilteredProcessOutput {
                stdout,
                stderr,
                raw_log_bytes,
                raw_log_lines,
            } = capture;
            let text = format!("{stdout}\n{stderr}");
            logs.push(ProcessLog {
                role,
                instance_id: format!("{pod_name}/{name}"),
                image: status["image"].as_str().map(str::to_owned),
                image_id: status["imageID"].as_str().map(str::to_owned),
                source_revision: None,
                complete: process_log_complete(&role, &text),
                raw_log_bytes,
                raw_log_lines,
                stdout,
                stderr,
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
    fn adaptive_policy_requires_matching_installed_readback_for_every_table() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tables.push("sqlserver_availability_replica_states".to_owned());
        let policy = json!({
            "target_parquet_bytes": 1000, "min_input_bytes": 100, "max_input_bytes": 4000,
            "cold_start_parquet_ratio": 0.5, "ewma_weight": 0.25,
            "min_parquet_ratio": 0.1, "max_parquet_ratio": 2.0
        });
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector":{"batching":{"adaptive_file":policy}}
        }))?;
        let env = role_environment(&config, ComponentRole::Collector)?;
        let batching: Value = serde_json::from_str(&env["ARROWCOL_COLLECTOR_BATCHING_CONFIG"])?;
        let log = config.tables.iter().map(|table| Ok(format!("arrowcol_adaptive_config {}\n", json!({
            "table_name":canonical_dmv_table_name(table)?,
            "scope":"installed_estimator_not_full_run_history",
            "config":policy
        })))).collect::<Result<String>>()?;
        let records = collector_adaptive_records(&config, &log)?;
        let process = json!({"component":"collector", "instance_id":"one", "log_complete":true,
            "startup_tunables_verified":true, "effective_config_source":"consumer_startup_readback",
            "effective_config":{"batching":batching}, "adaptive_table_policies":records});
        let initial = tunable_leaf_evidence(&config)?;
        let mut covered = initial.clone();
        promote_collector_readback(&config, &mut covered, Some(&json!([process.clone()])), 1)?;
        for field in policy.as_object().context("policy object")?.keys() {
            let path = format!("collector.batching.adaptive_file.{field}");
            assert!(covered["consumer_verified"].as_array().context("missing verified fields")?
                .iter().any(|entry| entry["path"] == path && entry["reason"] == "consumer_table_adaptive_readback"));
            let mut changed = process.clone();
            changed["adaptive_table_policies"][1]["config"][field] = json!("wrong");
            let mut result = initial.clone();
            promote_collector_readback(&config, &mut result, Some(&json!([changed])), 1)?;
            assert!(result["projection_only"].as_array().context("missing projection fields")?
                .iter().any(|entry| entry["path"] == path));
        }
        let mut overridden = process.clone();
        overridden["adaptive_table_policies"][1]["config"]["target_parquet_bytes"] = json!(2000);
        let mut result = initial.clone();
        promote_collector_readback(&config, &mut result, Some(&json!([overridden])), 1)?;
        assert!(result["projection_only"].as_array().context("missing projection fields")?
            .iter().any(|entry| entry["path"] == "collector.batching.adaptive_file.target_parquet_bytes"));
        let mut missing = process.clone();
        missing["adaptive_table_policies"].as_array_mut().context("missing policies")?.pop();
        let mut result = initial.clone();
        promote_collector_readback(&config, &mut result, Some(&json!([missing])), 1)?;
        assert_eq!(result, initial);
        let mut bad = json!({"table_name":canonical_dmv_table_name(&config.tables[0])?,
            "scope":"installed_estimator_not_full_run_history", "config":policy});
        bad["config"]["ewma_weight"] = json!("0.25");
        assert!(collector_adaptive_records(&config, &format!("arrowcol_adaptive_config {bad}\n")).is_err());
        bad.as_object_mut().context("event object")?.remove("table_name");
        assert!(collector_adaptive_records(&config, &format!("arrowcol_adaptive_config {bad}\n")).is_err());
        Ok(())
    }

    #[test]
    fn aks_inherited_router_filter_uses_image_config() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.platform.kind = crate::config::OatMode::Aks;
        config.platform.compose_file = None;

        let actual = inherited_router_log_filter(&config)?;
        let image_config: Value = serde_yaml::from_str(include_str!(
            "../../arrowcol-router-rust/config.docker.yaml"
        ))?;
        assert_eq!(actual, image_config["telemetry"]["log_filter"]);
        Ok(())
    }

    #[test]
    fn local_collector_stop_grace_readback_requires_exact_value() -> Result<()> {
        let compose = json!({"services":{"dataflow-1":{"stop_grace_period":"5m0s"}}});
        verify_local_collector_stop_grace(&compose, 300)?;
        assert!(verify_local_collector_stop_grace(&compose, 301).is_err());
        assert!(verify_local_collector_stop_grace(&json!({"services":{}}), 300).is_err());
        assert_eq!(parse_compose_duration_seconds("30s"), Some(30));
        assert_eq!(parse_compose_duration_seconds("1h0m0s"), Some(3_600));
        assert_eq!(parse_compose_duration_seconds("invalid"), None);
        Ok(())
    }

    #[test]
    fn maintenance_outcomes_use_separate_stream_offsets_and_omit_raw_errors() -> Result<()> {
        let previous = json!({"table":"t","operation":"optimize","applied":true,
            "error_kind":"previous_failure","error":"private previous error"});
        let baseline = ProcessLog {
            role: ComponentRole::Maintenance,
            instance_id: "maintenance-test".to_owned(),
            image: None,
            image_id: None,
            source_revision: None,
            complete: true,
            raw_log_bytes: 0,
            raw_log_lines: 0,
            stdout: format!("{previous}\n"),
            stderr: "prior stderr\n".to_owned(),
            text: String::new(),
        };
        let current_stdout = json!({"table":"t","operation":"optimize","applied":true,
            "error_kind":"commit_budget_exhausted","error":"private stdout error"});
        let current_stderr = json!({"table":"t","operation":"optimize","applied":true,
            "error_kind":"collision","error":"private stderr error"});
        let mut current = baseline.clone();
        current.stdout.push_str(&format!("{current_stdout}\n"));
        current.stderr.push_str(&format!("{current_stderr}\n"));
        let evidence = maintenance_cell_outcome_evidence(&current, std::slice::from_ref(&baseline));
        assert_eq!(evidence["status"], "observed");
        assert_eq!(evidence["operation_count"], 2);
        assert_eq!(
            evidence["error_kind_counts"],
            json!({"commit_budget_exhausted":1,"collision":1})
        );
        assert!(!evidence.to_string().contains("private"));

        let retry = json!({"table":"t","operation":"vacuum","applied":true,
            "error_kind":"stale_plan","error":"private retry error"});
        let mut retry_log = baseline.clone();
        retry_log.stdout.push_str(&format!("{retry}\n"));
        let retried = maintenance_cell_outcome_evidence(&retry_log, std::slice::from_ref(&baseline));
        assert_eq!(retried["operation_failure"], false);
        assert_eq!(retried["error_kind_counts"], json!({}));
        assert_eq!(retried["operations"]["vacuum"]["retry_kind_counts"], json!({"stale_plan":1}));
        assert_eq!(maintenance_exit_classification(&retry_log, Some(0)), "operation_failure",
            "a prior lifetime failure must not be hidden by cell-only retries");
        let mut only_retry = retry_log.clone();
        only_retry.stdout = format!("{retry}\n");
        assert_eq!(maintenance_exit_classification(&only_retry, Some(1)), "nonzero_health_exit");
        let mut unhealthy = only_retry.clone();
        unhealthy.text.push_str("delta-maintenance stale-plan starvation table=t operation=vacuum consecutive=8 limit=8\n");
        assert_eq!(maintenance_exit_classification(&unhealthy, Some(1)), "unhealthy");
        unhealthy.text = "delta-maintenance pending-table status failed: private discovery error\n".to_owned();
        assert_eq!(maintenance_exit_classification(&unhealthy, Some(1)), "unhealthy");
        only_retry.stdout = format!("{}\n", json!({"table":"t","operation":"vacuum","applied":true}));
        assert_eq!(maintenance_exit_classification(&only_retry, Some(0)), "unknown");
        for suffix in ["{\"operation\":", "{\"operation\":\"vacuum\",\"applied\":true}\n"] {
            let mut partial = baseline.clone();
            append_process_record(suffix.as_bytes(), &mut partial.stdout, &mut 0, "partial regression")?;
            let outcome = maintenance_cell_outcome_evidence(&partial, std::slice::from_ref(&baseline));
            assert_eq!(outcome["status"], "unavailable");
            assert!(outcome["operation_failure"].is_null() && outcome["error_kind_counts"].is_null());
        }
        assert!(maintenance_cell_outcome_evidence(&baseline, std::slice::from_ref(&baseline))["operation_count"].is_null());

        current.stderr = "rotated stderr\n".to_owned();
        let unavailable = maintenance_cell_outcome_evidence(&current, std::slice::from_ref(&baseline));
        assert_eq!(unavailable["status"], "unavailable");
        assert!(unavailable["error_kind_counts"].is_null());
        assert!(unavailable["comment"].as_str().is_some());
        Ok(())
    }

    #[test]
    fn maintenance_semantically_incomplete_success_is_unknown() -> Result<()> {
        let producer: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/maintenance-operation-results.json"))?;
        let success = producer["operation_results"].as_array().context("producer operations")?
            .iter().find(|record| record["operation"] == "vacuum").context("producer vacuum")?.clone();
        let baseline = ProcessLog {
            role: ComponentRole::Maintenance, instance_id: "semantic-test".to_owned(),
            image: None, image_id: None, source_revision: None, complete: true,
            raw_log_bytes: 0, raw_log_lines: 0, stdout: String::new(),
            stderr: String::new(), text: String::new(),
        };
        let observe = |record: &Value| {
            let mut log = baseline.clone();
            log.stdout = format!("{record}\n");
            maintenance_cell_outcome_evidence(&log, std::slice::from_ref(&baseline))
        };
        assert_eq!(observe(&success)["operation_failure"], false);
        for field in ["report", "outcome", "interrupted", "error_kind", "error"] {
            let mut malformed = success.clone();
            match field {
                "report" => { malformed.as_object_mut().context("record")?.remove(field); }
                "outcome" | "interrupted" => {
                    malformed["report"].as_object_mut().context("report")?.remove(field);
                }
                _ => malformed[field] = json!(17),
            }
            let evidence = observe(&malformed);
            assert_eq!(evidence["status"], "unavailable", "{field}");
            assert!(evidence["operation_failure"].is_null(), "{field}");
            assert!(evidence["error_kind_counts"].is_null(), "{field}");
        }
        let mut omitted_optional = success.clone();
        for field in ["error", "error_kind"] {
            omitted_optional.as_object_mut().context("record")?.remove(field);
        }
        assert_eq!(observe(&omitted_optional)["operation_failure"], false);
        omitted_optional.as_object_mut().context("record")?.remove("report");
        omitted_optional["error_kind"] = json!("stale_plan");
        assert_eq!(observe(&omitted_optional)["operation_failure"], false);
        assert_eq!(observe(&omitted_optional)["retry_kind_counts"]["stale_plan"], 1);
        Ok(())
    }

    #[test]
    fn maintenance_nonzero_health_does_not_become_retry_success_after_recovery() -> Result<()> {
        let producer: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/maintenance-operation-results.json"))?;
        let operations = producer["operation_results"].as_array().context("producer operations")?;
        let mut stale = operations[0].clone();
        stale.as_object_mut().context("record")?.remove("report");
        stale["error_kind"] = json!("stale_plan");
        for limit in [1, 8] {
            let mut log = ProcessLog {
                role: ComponentRole::Maintenance, instance_id: "health-test".to_owned(),
                image: None, image_id: None, source_revision: None, complete: true,
                raw_log_bytes: 0, raw_log_lines: 0,
                stdout: format!("{}\n{stale}\n",
                    json!({"startup":{"max_consecutive_stale_plans":limit}})),
                stderr: String::new(), text: String::new(),
            };
            for record in operations {
                log.stdout.push_str(&format!("{record}\n"));
            }
            assert_eq!(maintenance_exit_classification(&log, Some(0)), "no_operation_failures");
            assert_eq!(maintenance_exit_classification(&log, Some(1)), "nonzero_health_exit",
                "limit {limit}: recovery or a benign retry cannot prove terminal health");
        }
        Ok(())
    }

    #[test]
    fn collector_cpu_pressure_mode_requires_effective_startup_readback() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables.collector = Some(arrowcol_tunables::CollectorTunables {
            cpu_pressure: Some(arrowcol_tunables::CpuPressureMode::Process),
            ..Default::default()
        });
        verify_process_configuration(
            &config,
            ComponentRole::Collector,
            &json!({"cpu_pressure":"process"}),
        )?;
        assert!(verify_process_configuration(
            &config,
            ComponentRole::Collector,
            &json!({"cpu_pressure":"off"}),
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn collector_sort_options_require_exact_startup_readback() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let options = json!([{"column":"customer_id","direction":"descending","nulls":"first"}]);
        config.tunables = serde_json::from_value(json!({"collector":{"layout":{
            "sort_columns":["customer_id"],"sort_options":options
        }}}))?;
        let observed = json!({"layout":{"process_policy":{"sort_columns":["customer_id"],"sort_options":options}}});
        verify_process_configuration(&config, ComponentRole::Collector, &observed)?;
        let mut wrong = observed.clone();
        wrong["layout"]["process_policy"]["sort_options"][0]["nulls"] = json!("last");
        assert!(verify_process_configuration(&config, ComponentRole::Collector, &wrong).is_err());
        wrong["layout"]["process_policy"].as_object_mut().context("policy")?.remove("sort_options");
        assert!(verify_process_configuration(&config, ComponentRole::Collector, &wrong).is_err());
        Ok(())
    }

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
            instance_id: "collector-test".to_owned(),
            image: None,
            image_id: None,
            source_revision: None,
            complete: true,
            raw_log_bytes: 0,
            raw_log_lines: 0,
            stdout: startup.clone(),
            stderr: String::new(),
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
    fn log_completeness_requires_role_specific_startup_envelopes() {
        assert!(process_log_complete(
            &ComponentRole::Collector,
            "arrowcol_effective_config {\"component\":\"receiver\"}\n"
        ));
        assert!(!process_log_complete(
            &ComponentRole::Collector,
            "arrowcol_effective_config {\"component\":\"layout\",\"table_effective\":{}}\n"
        ));
        assert!(!process_log_complete(
            &ComponentRole::Collector,
            "arrowcol_effective_config {\"table_effective\":{}}\n"
        ));
        assert!(process_log_complete(
            &ComponentRole::Router,
            "arrowcol_router_effective_config {\"workers\":2,\"log_filter\":\"warn,tiberius=error\"}\n"
        ));
        assert!(process_log_complete(
            &ComponentRole::Maintenance,
            "arrowcol_maintenance_effective_config {\"maintenance\":{}}\n"
        ));
    }

    #[test]
    fn process_evidence_records_raw_log_bytes_and_lines() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let log = ProcessLog {
            role: ComponentRole::Router,
            instance_id: "router-test".to_owned(),
            image: None,
            image_id: None,
            source_revision: None,
            complete: true,
            raw_log_bytes: 97,
            raw_log_lines: 4,
            stdout: String::new(),
            stderr: String::new(),
            text: "arrowcol_router_effective_config {\"workers\":2,\"log_filter\":\"warn,tiberius=error\"}\n".to_owned(),
        };
        let evidence = process_evidence(&config, &[log])?;
        assert_eq!(evidence[0]["raw_log_bytes"], 97);
        assert_eq!(evidence[0]["raw_log_lines"], 4);
        Ok(())
    }

    #[test]
    fn disabled_maintenance_logging_does_not_require_a_process() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "logging": {"maintenance": "warn"},
            "maintenance": {"enabled": false}
        }))?;
        assert_eq!(process_evidence(&config, &[])?, json!([]));
        assert_eq!(
            inert_tunable_fields(&config)?,
            vec![json!({"path":"logging.maintenance","reason":"maintenance_disabled"})]
        );
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "logging": {"maintenance": "warn"},
            "maintenance": {"enabled": true, "operations": ["optimize"]}
        }))?;
        assert!(inert_tunable_fields(&config)?.is_empty());
        Ok(())
    }

    #[test]
    fn process_log_filter_fails_closed_when_retained_byte_cap_is_exceeded() {
        let mut output = String::new();
        let mut retained_bytes = MAX_RETAINED_PROCESS_LOG_BYTES;
        assert!(
            append_process_evidence_line("arrowcol_data_commit {}", &mut output, &mut retained_bytes, "test log",)
                .is_err()
        );
        assert!(output.is_empty());
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
            "OAT_CLIENT_EFFECTIVE_CONFIG {\"batch_rows\":1,\"secret_file\":\"private\",\"endpoint\":\"https://private\",\"endpoint_concurrency_limit\":12,\"endpoint_buffer_size\":14}\n",
        )?;
        assert_eq!(readback, json!({"batch_rows":1,"endpoint_concurrency_limit":12,"endpoint_buffer_size":14}));
        assert!(public_tuning_value(&json!({"endpoint_buffer_size":"https://private"}))
            .is_some_and(|value| value == json!({})));
        Ok(())
    }

    #[test]
    fn router_excluded_gauges_require_matching_startup_readback() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "router": {"excluded_admission_gauges": ["cpu_utilization_bps", "active_streams"]}
        }))?;
        let log_filter = role_environment(&config, ComponentRole::Router)?["RUST_LOG"].clone();
        let observed = json!({"log_filter": log_filter, "routing":{"excluded_admission_gauges":["cpu_utilization_bps","active_streams"]}});
        verify_process_configuration(&config, ComponentRole::Router, &observed)?;
        assert!(verify_process_configuration(&config, ComponentRole::Router, &json!({"routing":{}})).is_err());
        assert!(verify_process_configuration(
            &config, ComponentRole::Router, &json!({"log_filter":log_filter,"routing":{"excluded_admission_gauges":[]}})
        ).is_err());
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "router": {"excluded_admission_gauges": []}
        }))?;
        verify_process_configuration(
            &config, ComponentRole::Router, &json!({"log_filter":log_filter,"routing":{"excluded_admission_gauges":[]}})
        )?;
        Ok(())
    }

    #[test]
    #[ignore = "requires cargo build --package arrowcol-router-rust"]
    fn router_excluded_gauges_reach_actual_startup_readback() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "router": {"excluded_admission_gauges": ["cpu_utilization_bps", "active_streams"]}
        }))?;
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
        let binary = root.join("target").join("debug")
            .join(format!("arrowcol-router-rust{}", std::env::consts::EXE_SUFFIX));
        let output = std::process::Command::new(binary)
            .env_clear()
            .envs(role_environment(&config, ComponentRole::Router)?)
            .arg("--config")
            .arg(root.join("projects/arrowcol-router-rust/config.smoke.yaml"))
            .arg("--validate-and-exit")
            .output()
            .context("launch actual router for startup propagation")?;
        anyhow::ensure!(output.status.success(), "router validation failed: {}", String::from_utf8_lossy(&output.stderr));
        let text = format!("{}\n{}", String::from_utf8(output.stdout)?, String::from_utf8(output.stderr)?);
        let observed = process_configurations(&text)?;
        verify_process_configuration(&config, ComponentRole::Router, &observed)?;
        let process = json!({
            "component": "router", "instance_id": "router-1", "log_complete": true,
            "startup_tunables_verified": true, "effective_config_source": "consumer_startup_readback",
            "effective_config": observed,
        });
        let initial = tunable_leaf_evidence(&config)?;
        let mut covered = initial.clone();
        promote_router_readback(&config, &mut covered, Some(&json!([process.clone()])), 1)?;
        assert_eq!(covered["consumer_verified"], json!([{
            "path": "router.excluded_admission_gauges", "reason": "consumer_startup_readback"
        }]));
        let mut wrong = process.clone();
        wrong["effective_config"]["routing"]["excluded_admission_gauges"] = json!([]);
        for processes in [json!([]), json!([wrong]), json!([process.clone(), process])] {
            let mut uncovered = initial.clone();
            let replicas = if processes.as_array().context("process array")?.len() == 2 { 2 } else { 1 };
            promote_router_readback(&config, &mut uncovered, Some(&processes), replicas)?;
            assert_eq!(uncovered, initial);
        }
        Ok(())
    }

    #[test]
    fn parquet_policy_reaches_consumer_startup_configuration() -> Result<()> {
        const CHILD: &str = "OAT_PARQUET_STARTUP_CHILD";
        const PREFIX: &str = "OAT_PARQUET_STARTUP_READBACK ";
        if std::env::var_os(CHILD).is_some() {
            println!("{PREFIX}{}", arrow_deltalake::writer::effective_startup_config_json()?);
            return Ok(());
        }
        let cases = [
            json!({"defaults":{"compression":"uncompressed"}}),
            json!({"defaults":{"compression":"snappy"}}),
            json!({"defaults":{"compression":"gzip","compression_level":6}}),
            json!({"defaults":{"compression":"brotli","compression_level":1}}),
            json!({"defaults":{"compression":"lz4"}}),
            json!({"defaults":{"compression":"lz4_raw"}}),
            json!({"defaults":{"compression":"zstd","compression_level":3},
                "columns":{"customer_id":{"compression":"snappy","dictionary_enabled":true}},
                "writer_version":"v2","max_row_group_rows":16384,"max_row_group_bytes":1048576,
                "data_page_row_count_limit":2048,"write_batch_size":512,
                "offset_index_enabled":true,"column_index_truncate_length":64,
                "statistics_truncate_length":64}),
        ];
        for policy in cases {
            let mut config: OatConfig = serde_json::from_str(include_str!(
                "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
            ))?;
            config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({"parquet_writer":policy}))?;
            let output = std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", "runner::startup_readback_tests::parquet_policy_reaches_consumer_startup_configuration", "--nocapture"])
                .env_clear()
                .env(CHILD, "1")
                .envs(role_environment(&config, ComponentRole::Collector)?)
                .output()?;
            let stdout = String::from_utf8(output.stdout)?;
            anyhow::ensure!(output.status.success(), "writer startup failed: {stdout}\n{}", String::from_utf8_lossy(&output.stderr));
            let observed: Value = serde_json::from_str(stdout.lines()
                .find_map(|line| line.split_once(PREFIX).map(|(_, json)| json))
                .context("writer startup readback missing")?)?;
            verify_process_configuration(&config, ComponentRole::Collector, &observed)?;
            assert!(verify_process_configuration(&config, ComponentRole::Collector, &json!({})).is_err());
        }
        Ok(())
    }

    #[test]
    fn writer_overrides_are_not_claimed_as_schema_resolved_or_exercised_properties() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let evidence = process_evidence(&config, &[ProcessLog {
            role: ComponentRole::Collector, instance_id: "collector-test".to_owned(), image: None, image_id: None, source_revision: None, complete: true,
            raw_log_bytes: 0,
            raw_log_lines: 0,
            stdout: String::new(),
            stderr: String::new(),
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
    fn maintenance_writer_readback_is_independent_of_collector() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "parquet_writer": {"defaults": {"compression": "zstd"}},
            "maintenance": {"enabled": true, "operations": ["optimize"],
                "parquet_writer": {"defaults": {"compression": "snappy"}}}
        }))?;
        let collector: Value = serde_json::from_str(
            &role_environment(&config, ComponentRole::Collector)?["ARROWCOL_PARQUET_CONFIG"],
        )?;
        let maintenance: Value = serde_json::from_str(
            &role_environment(&config, ComponentRole::Maintenance)?["ARROWCOL_PARQUET_CONFIG"],
        )?;
        verify_process_configuration(&config, ComponentRole::Collector, &json!({"parquet_writer": collector}))?;
        let observed = json!({
            "startup": {"apply": false, "operations": ["optimize"], "interval_secs": 60},
            "parquet_writer_overrides": maintenance
        });
        verify_process_configuration(&config, ComponentRole::Maintenance, &observed)?;
        let mut mismatch = observed;
        mismatch["parquet_writer_overrides"]["defaults"]["compression"] = json!("zstd");
        assert!(verify_process_configuration(&config, ComponentRole::Maintenance, &mismatch).is_err());
        Ok(())
    }

    #[test]
    fn real_router_and_collector_control_plane_shapes_preserve_public_tunables() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = serde_json::from_value(json!({
            "router":{"workers":3,"listener_tasks_per_fd":7,"sql_connect_timeout_ms":17000,
                "sql_pool_max_size":32,"sql_pool_acquire_timeout_ms":3000},
            "logging":{"router":"debug"},
            "collector":{"catalog":{"maximum_cache_age":"13000ms"},
                "routing":{"connect_timeout":"17000ms","command_timeout":"31000ms"},
                "exporter":{"authority_timeout":"19000ms"},
                "pressure":{"heartbeat_interval":"6000ms","registration_ttl":"24000ms"}}
        }))?;
        let router = process_configurations(concat!(
            "arrowcol_router_effective_config ",
            "{\"workers\":3,\"listener_tasks_per_fd\":7,\"sql_timeouts\":{\"connect_timeout_ms\":17000},\"sql_pool\":{\"max_size\":32,\"acquire_timeout_ms\":3000},\"log_filter\":\"debug,tiberius=error\",\"listen\":\"private\"}\n"
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
    fn collector_pressure_readback_compares_only_typed_durations_semantically() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = serde_json::from_value(json!({"collector":{"pressure":{
            "registration_ttl":"15m", "heartbeat_interval":"1m 5s", "pressure_interval":"1m",
            "buffered_bytes_limit":2147483648_u64
        }}}))?;
        let pressure = json!({
            "registration_ttl":"900s", "heartbeat_interval":"65s", "pressure_interval":"60s",
            "buffered_bytes_limit":2147483648_u64
        });
        for (field, invalid) in [
            ("registration_ttl", json!("899s")),
            ("heartbeat_interval", json!("unrecognized duration")),
            ("pressure_interval", json!(60)),
            ("buffered_bytes_limit", json!("2147483648")),
        ] {
            let mut observed = json!({"pressure":pressure.clone()});
            observed["pressure"][field] = invalid;
            assert!(verify_process_configuration(&config, ComponentRole::Collector, &observed).is_err());
        }
        let mut missing = pressure.clone();
        missing.as_object_mut().context("test pressure object")?.remove("registration_ttl");
        assert!(verify_process_configuration(&config, ComponentRole::Collector, &json!({"pressure":missing})).is_err());
        verify_process_configuration(&config, ComponentRole::Collector, &json!({"pressure":pressure}))
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

#[cfg(test)]
mod consumer_startup_projection_tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "requires built arrowcol-dataflow-rust"]
    fn commit_and_pressure_reach_actual_collector_startup() -> Result<()> {
        struct TempRoot(PathBuf);
        impl Drop for TempRoot {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        struct Child(std::process::Child);
        impl Child {
            fn stop(&mut self) -> Result<()> {
                if self.0.try_wait()?.is_none() {
                    self.0.kill()?;
                    self.0.wait()?;
                }
                Ok(())
            }
        }
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.stop();
            }
        }

        let root = std::env::temp_dir().join(format!("oat-collector-readback-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root)?;
        let root = TempRoot(root);
        let yaml_path = root.0.join("config.yaml");
        let log_path = root.0.join("stderr.log");
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector": {
                "batching":{"coalesce_publications":true,"age_flush_policy":"drain_ready","pipeline_depth":2,
                    "global_capacity":{"bytes":1073741824,"rows":5000000,"batches":2048},
                    "per_table_capacity":{"bytes":268435456,"rows":1000000,"batches":512},
                    "max_arrow_memory_per_flush":134217728,"max_rows_per_flush":777777,
                    "max_batches_per_flush":333,"max_age":"12s","max_concurrent_flushes":2},
                "commit":{"batch_window":"3s","max_files_per_commit":7,"max_pending_files":13},
                "pressure":{"max_concurrent_streams":23,"rss_limit_bytes":1073741824,
                    "buffered_bytes_limit":134217728,"buffered_rows_limit":100000,
                    "buffered_batches_limit":123,"pressure_interval":"3s",
                    "heartbeat_interval":"4s","registration_ttl":"30s"},
                "runtime":{"workers":3,"blocking_workers":6,"blocking_fs_reserve":2,
                    "task_limit":77,"read_ahead":5,"batch_rows":12000,"cpu_workers":3,"cpu_queue_jobs":4}
            }
        }))?;
        let binary = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/debug/arrowcol-dataflow-rust");
        let mut text = None;
        for attempt in 0..3 {
            let flight = std::net::TcpListener::bind("127.0.0.1:0")?;
            let admin = std::net::TcpListener::bind("127.0.0.1:0")?;
            let flight_port = flight.local_addr()?.port();
            let admin_port = admin.local_addr()?.port();
            let yaml = format!(r#"
version: otel_dataflow/v1
policies:
  resources:
    core_allocation:
      type: core_count
      count: 1
groups:
  isolated:
    pipelines:
      ingest:
        nodes:
          flight:
            type: receiver:arrow_flight
            config:
              listening_addr: "127.0.0.1:{flight_port}"
              admin_listening_addr: "127.0.0.1:{admin_port}"
              catalog:
                type: memory
              routing:
                type: memory
              registration:
                instance_key: isolated-startup-test
                flight_endpoint: "127.0.0.1:{flight_port}"
          delta:
            type: exporter:arrow_deltalake
            config:
              base_path: "{}/data"
              routing:
                type: memory
        connections:
          - from: flight
            to: delta
"#, root.0.display());
            std::fs::write(&yaml_path, yaml)?;
            drop((flight, admin));
            let mut child = Child(std::process::Command::new(&binary)
                .args(["--num-cores", "1", "--config"])
                .arg(&yaml_path)
                .env_clear()
                .envs(role_environment(&config, ComponentRole::Collector)?)
                .stdout(std::process::Stdio::null())
                .stderr(std::fs::File::create(&log_path)?)
                .spawn()?);
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                let output = std::fs::read_to_string(&log_path)?;
                if output.contains("Address already in use") {
                    if attempt < 2 {
                        child.stop().context("stop collector after bind collision")?;
                        break;
                    }
                    bail!("collector bind collision after three attempts: {output}");
                }
                if let Some(status) = child.0.try_wait()? {
                    bail!("collector exited before both readbacks ({status}): {output}");
                }
                let listening = [flight_port, admin_port].into_iter().all(|port| {
                    std::net::TcpStream::connect_timeout(
                        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
                        Duration::from_millis(100),
                    ).is_ok()
                });
                if listening && output.contains(r#""component":"receiver""#) && output.contains(r#""component":"exporter""#) {
                    std::thread::sleep(Duration::from_millis(100));
                    if let Some(status) = child.0.try_wait()? {
                        let output = std::fs::read_to_string(&log_path)?;
                        if output.contains("Address already in use") && attempt < 2 {
                            break;
                        }
                        bail!("collector exited during listener startup ({status}): {output}");
                    }
                    child.stop().context("stop isolated collector")?;
                    text = Some(output);
                    break;
                }
                anyhow::ensure!(std::time::Instant::now() < deadline, "collector readback timed out: {output}");
                std::thread::sleep(Duration::from_millis(100));
            }
            if text.is_some() {
                break;
            }
        }
        let observed = process_configurations(&text.context("collector did not produce readback")?)?;
        verify_process_configuration(&config, ComponentRole::Collector, &observed)?;
        let process = json!({
            "component": "collector", "instance_id": "collector-1", "log_complete": true,
            "startup_tunables_verified": true, "effective_config_source": "consumer_startup_readback",
            "effective_config": observed,
        });
        let initial = tunable_leaf_evidence(&config)?;
        let mut covered = initial.clone();
        promote_collector_readback(&config, &mut covered, Some(&json!([process.clone()])), 1)?;
        assert_eq!(covered["consumer_verified"].as_array().context("consumer_verified array")?.len(), 33);
        assert!(covered["projection_only"].as_array().context("projection_only array")?.is_empty());
        let mut wrong_process = process.clone();
        wrong_process["effective_config"]["commit"]["max_files_per_commit"] = json!(8);
        let mut missing_process = process.clone();
        missing_process["effective_config"]["pressure"].as_object_mut().context("pressure object")?.remove("buffered_rows_limit");
        let mut incomplete_process = process.clone();
        incomplete_process["log_complete"] = json!(false);
        for processes in [
            json!([]), json!([wrong_process]), json!([missing_process]), json!([incomplete_process]),
            json!([process.clone(), process.clone()]),
        ] {
            let mut uncovered = initial.clone();
            let replicas = if processes.as_array().context("process array")?.len() == 2 { 2 } else { 1 };
            promote_collector_readback(&config, &mut uncovered, Some(&processes), replicas)?;
            assert_eq!(uncovered, initial);
        }
        for (group, fields) in [
            ("batching", &["coalesce_publications", "age_flush_policy", "pipeline_depth",
                "max_arrow_memory_per_flush", "max_rows_per_flush", "max_batches_per_flush",
                "max_age", "max_concurrent_flushes"][..]),
            ("commit", &["batch_window", "max_files_per_commit", "max_pending_files"][..]),
            ("pressure", &["max_concurrent_streams", "rss_limit_bytes", "buffered_bytes_limit",
                "buffered_rows_limit", "buffered_batches_limit", "pressure_interval",
                "heartbeat_interval", "registration_ttl"][..]),
            ("runtime", &["runtime_workers", "blocking_workers", "blocking_fs_reserve", "task_limit",
                "read_ahead", "batch_rows", "cpu_workers", "cpu_queue_jobs"][..]),
        ] {
            for field in fields {
                let mut missing = observed.clone();
                missing[group].as_object_mut().context("missing readback group")?
                    .remove(*field).context("consumer omitted requested field")?;
                anyhow::ensure!(verify_process_configuration(&config, ComponentRole::Collector, &missing).is_err(),
                    "verifier accepted missing {group}.{field}");
                let mut wrong = observed.clone();
                wrong[group][*field] = json!("wrong");
                anyhow::ensure!(verify_process_configuration(&config, ComponentRole::Collector, &wrong).is_err(),
                    "verifier accepted wrong {group}.{field}");
            }
        }
        for capacity in ["global_capacity", "per_table_capacity"] {
            for field in ["bytes", "rows", "batches"] {
                let mut missing = observed.clone();
                missing["batching"][capacity].as_object_mut().context("missing capacity readback")?
                    .remove(field).context("consumer omitted requested capacity")?;
                assert!(verify_process_configuration(&config, ComponentRole::Collector, &missing).is_err());
                let mut wrong = observed.clone();
                wrong["batching"][capacity][field] = json!("wrong");
                assert!(verify_process_configuration(&config, ComponentRole::Collector, &wrong).is_err());
            }
        }
        let mut wrong = observed;
        wrong["commit"]["max_files_per_commit"] = json!(8);
        assert!(verify_process_configuration(&config, ComponentRole::Collector, &wrong).is_err());
        Ok(())
    }

    #[test]
    #[ignore = "requires built arrowcol-dataflow-rust"]
    fn parquet_policy_reaches_actual_collector_startup() -> Result<()> {
        fn leaf_paths(value: &Value, path: &mut Vec<String>, leaves: &mut Vec<Vec<String>>) {
            if let Some(fields) = value.as_object() {
                for (key, value) in fields {
                    path.push(key.clone());
                    leaf_paths(value, path, leaves);
                    path.pop();
                }
            } else {
                leaves.push(path.clone());
            }
        }
        let binary = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/debug")
            .join(format!("arrowcol-dataflow-rust{}", std::env::consts::EXE_SUFFIX));
        let cases = [
            json!({"defaults":{"compression":"uncompressed"}}),
            json!({"defaults":{"compression":"snappy"}}),
            json!({"defaults":{"compression":"gzip","compression_level":6}}),
            json!({"defaults":{"compression":"brotli","compression_level":1}}),
            json!({"defaults":{"compression":"lz4"}}),
            json!({"defaults":{"compression":"lz4_raw"}}),
            json!({"defaults":{"compression":"zstd","compression_level":3,"encoding":"plain",
                    "dictionary_enabled":true,"dictionary_page_size_limit":2048,
                    "data_page_size_limit":4096,"statistics":"chunk","write_page_header_statistics":true,
                    "bloom_filter_enabled":true,"bloom_filter_fpp":0.05,"bloom_filter_ndv":1024,
                    "data_page_v2_compression_ratio_threshold":1.5},
                "columns":{"customer_id":{"compression":"gzip","compression_level":5,
                    "encoding":"delta_byte_array","dictionary_enabled":true,"dictionary_page_size_limit":1024,
                    "data_page_size_limit":2048,"statistics":"page","write_page_header_statistics":false,
                    "bloom_filter_enabled":true,"bloom_filter_fpp":0.1,"bloom_filter_ndv":512,
                    "data_page_v2_compression_ratio_threshold":1.2}},
                "writer_version":"v2","max_row_group_rows":16384,"max_row_group_bytes":1048576,
                "data_page_row_count_limit":2048,"write_batch_size":512,
                "offset_index_enabled":true,"column_index_truncate_length":64,
                "statistics_truncate_length":64}),
        ];
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        for policy in cases {
            let mut input = json!({"parquet_writer":policy.clone()});
            if policy.get("max_row_group_rows").is_some() {
                input["collector"] = json!({"layout":{"enabled":true, "sort_columns":["customer_id","value"],
                    "bloom_columns":["value","customer_id"], "max_rows":65536, "max_input_bytes":1048576,
                    "row_group_rows":16384, "page_rows":2048}});
            }
            config.tunables = arrowcol_tunables::Tunables::parse_oat_json(input)?;
            let output = std::process::Command::new(&binary)
                .arg("--help")
                .env_clear()
                .envs(role_environment(&config, ComponentRole::Collector)?)
                .output()
                .context("launch collector for Parquet policy readback")?;
            anyhow::ensure!(output.status.success(), "collector startup failed: {}", String::from_utf8_lossy(&output.stderr));
            let mut observed = process_configurations(&String::from_utf8(output.stderr)?)?;
            verify_process_configuration(&config, ComponentRole::Collector, &observed)?;
            if policy.get("max_row_group_rows").is_some() {
                config.tables.push("sqlserver_availability_replica_states".to_owned());
                let events = config.tables.iter().map(|table| Ok(format!(
                    "arrowcol_effective_config {}\n", json!({
                        "component":"layout", "scope":"table_effective", "table_id":"a".repeat(64),
                        "table_name":canonical_dmv_table_name(table)?,
                        "config":{"enabled":true, "sort_columns":["customer_id","value"], "bloom_columns":["customer_id","value"],
                            "max_rows":65536, "max_input_bytes":1048576,
                            "row_group_rows":policy["max_row_group_rows"], "page_rows":policy["data_page_row_count_limit"]},
                        "parquet_columns":{
                            "value":{"compression":"zstd","compression_level":3},
                            "customer_id":{"compression":"gzip","compression_level":5}
                        },
                        "parquet_column_options":{
                            "value":{"encoding":"plain","dictionary_enabled":true,"dictionary_page_size_limit":2048,
                                "data_page_size_limit":4096,"statistics":"chunk","write_page_header_statistics":true,
                                "bloom_filter_enabled":true,"bloom_filter_fpp":0.05,"bloom_filter_ndv":1024,
                                "data_page_v2_compression_ratio_threshold":1.5},
                            "customer_id":{"encoding":"delta_byte_array","dictionary_enabled":true,"dictionary_page_size_limit":1024,
                                "data_page_size_limit":2048,"statistics":"page","write_page_header_statistics":false,
                                "bloom_filter_enabled":true,"bloom_filter_fpp":0.1,"bloom_filter_ndv":512,
                                "data_page_v2_compression_ratio_threshold":1.2}
                        },
                        "parquet_properties":{"writer_version":"v2", "max_row_group_bytes":1048576,
                            "write_batch_size":512, "offset_index_enabled":true,
                            "column_index_truncate_length":64, "statistics_truncate_length":64}
                    })
                ))).collect::<Result<String>>()?;
                let tables = collector_layout_records(&config, &events)?;
                let mut process = json!({
                    "component":"collector", "instance_id":"collector-1", "log_complete":true,
                    "startup_tunables_verified":true, "effective_config_source":"consumer_startup_readback",
                    "effective_config":observed, "writer_policy_overrides":observed["parquet_writer"],
                    "layout_table_policies":tables,
                });
                let initial = tunable_leaf_evidence(&config)?;
                let mut covered = initial.clone();
                promote_collector_readback(&config, &mut covered, Some(&json!([process.clone()])), 1)?;
                for field in ["enabled", "sort_columns", "bloom_columns", "max_rows", "max_input_bytes", "row_group_rows", "page_rows"] {
                    let path = format!("collector.layout.{field}");
                    assert!(covered["consumer_verified"].as_array().context("missing verified fields")?
                        .iter().any(|entry| entry["path"] == path), "missing table readback for {path}");
                    let mut overridden = process.clone();
                    overridden["layout_table_policies"][1]["config"][field] = json!("wrong");
                    let mut result = initial.clone();
                    promote_collector_readback(&config, &mut result, Some(&json!([overridden])), 1)?;
                    assert!(result["projection_only"].as_array().context("missing projected fields")?
                        .iter().any(|entry| entry["path"] == path), "accepted override for {path}");
                }
                for path in ["parquet_writer.max_row_group_rows", "parquet_writer.data_page_row_count_limit",
                    "parquet_writer.writer_version", "parquet_writer.max_row_group_bytes",
                    "parquet_writer.write_batch_size", "parquet_writer.offset_index_enabled",
                    "parquet_writer.column_index_truncate_length", "parquet_writer.statistics_truncate_length",
                    "parquet_writer.defaults.compression", "parquet_writer.defaults.compression_level",
                    "parquet_writer.columns.customer_id.compression", "parquet_writer.columns.customer_id.compression_level",
                    "parquet_writer.defaults.encoding", "parquet_writer.defaults.dictionary_enabled",
                    "parquet_writer.defaults.dictionary_page_size_limit", "parquet_writer.defaults.data_page_size_limit",
                    "parquet_writer.defaults.statistics",
                    "parquet_writer.defaults.bloom_filter_enabled", "parquet_writer.defaults.bloom_filter_fpp",
                    "parquet_writer.defaults.bloom_filter_ndv", "parquet_writer.defaults.data_page_v2_compression_ratio_threshold",
                    "parquet_writer.columns.customer_id.encoding", "parquet_writer.columns.customer_id.dictionary_enabled",
                    "parquet_writer.columns.customer_id.dictionary_page_size_limit", "parquet_writer.columns.customer_id.data_page_size_limit",
                    "parquet_writer.columns.customer_id.statistics", "parquet_writer.columns.customer_id.write_page_header_statistics",
                    "parquet_writer.columns.customer_id.bloom_filter_enabled", "parquet_writer.columns.customer_id.bloom_filter_fpp",
                    "parquet_writer.columns.customer_id.bloom_filter_ndv", "parquet_writer.columns.customer_id.data_page_v2_compression_ratio_threshold"] {
                    assert!(covered["consumer_verified"].as_array().context("missing verified fields")?
                        .iter().any(|entry| entry["path"] == path && entry["reason"] == "consumer_table_writer_readback"),
                        "missing writer readback for {path}: {covered}");
                }
                let projected = |evidence: &Value, path: &str| {
                    evidence["projection_only"].as_array().is_some_and(|entries| entries.iter().any(|entry| entry["path"] == path))
                };
                for bloom in [
                    json!(["customer_id"]),
                    json!(["customer_id", "value", "extra"]),
                    json!(["customer_id", "value", "value"]),
                    json!(["customer_id", 1]),
                    Value::Null,
                ] {
                    let mut changed = process.clone();
                    changed["layout_table_policies"][1]["config"]["bloom_columns"] = bloom;
                    let mut result = initial.clone();
                    promote_collector_readback(&config, &mut result, Some(&json!([changed])), 1)?;
                    assert!(projected(&result, "collector.layout.bloom_columns"));
                }
                let mut reordered_sort = process.clone();
                reordered_sort["layout_table_policies"][1]["config"]["sort_columns"] =
                    json!(["value", "customer_id"]);
                let mut result = initial.clone();
                promote_collector_readback(&config, &mut result, Some(&json!([reordered_sort])), 1)?;
                assert!(projected(&result, "collector.layout.sort_columns"));
                assert!(projected(&covered, "parquet_writer.defaults.write_page_header_statistics"));
                let mut malformed_options = process["layout_table_policies"][0]["parquet_column_options"].clone();
                malformed_options["value"]["dictionary_enabled"] = json!("true");
                assert!(parquet_column_options_readback(&malformed_options).is_none());
                let mut no_statistics = process.clone();
                for column in ["value", "customer_id"] {
                    no_statistics["layout_table_policies"][0]["parquet_column_options"][column]["statistics"] = json!("none");
                }
                let mut result = initial.clone();
                promote_collector_readback(&config, &mut result, Some(&json!([no_statistics])), 1)?;
                assert!(projected(&result, "parquet_writer.column_index_truncate_length"));
                assert!(projected(&result, "parquet_writer.statistics_truncate_length"));
                let mut missing_properties = process.clone();
                missing_properties["layout_table_policies"][0]["parquet_properties"] = Value::Null;
                let mut result = initial.clone();
                promote_collector_readback(&config, &mut result, Some(&json!([missing_properties])), 1)?;
                assert!(projected(&result, "parquet_writer.write_batch_size"));
                let mut disabled_column = process.clone();
                disabled_column["layout_table_policies"][0]["parquet_column_options"]["customer_id"]["dictionary_enabled"] = json!(false);
                let mut result = initial.clone();
                promote_collector_readback(&config, &mut result, Some(&json!([disabled_column])), 1)?;
                assert!(projected(&result, "parquet_writer.columns.customer_id.dictionary_page_size_limit"));
                let mut wrong_column = process.clone();
                wrong_column["layout_table_policies"][1]["parquet_column_options"]["value"]["encoding"] = json!("rle");
                let mut result = initial.clone();
                promote_collector_readback(&config, &mut result, Some(&json!([wrong_column])), 1)?;
                assert!(projected(&result, "parquet_writer.defaults.encoding"));
                let mut uncompressed = process.clone();
                uncompressed["layout_table_policies"][0]["parquet_columns"]["value"] =
                    json!({"compression":"uncompressed","compression_level":null});
                let mut result = initial.clone();
                promote_collector_readback(&config, &mut result, Some(&json!([uncompressed])), 1)?;
                assert!(projected(&result, "parquet_writer.defaults.data_page_v2_compression_ratio_threshold"));
                let mut conflicting_properties = process.clone();
                conflicting_properties["layout_table_policies"][1]["parquet_properties"]["write_batch_size"] = json!(1);
                let mut result = initial.clone();
                promote_collector_readback(&config, &mut result, Some(&json!([conflicting_properties])), 1)?;
                assert!(projected(&result, "parquet_writer.write_batch_size"));
                assert!(parquet_column_readback(&json!({"value":{"compression":"zstd","compression_level":"3"}})).is_none());
                for columns in [
                    Value::Null,
                    json!({"customer_id":{"compression":"gzip","compression_level":5}}),
                    json!({"value":{"compression":"snappy","compression_level":3},
                        "customer_id":{"compression":"gzip","compression_level":5}}),
                ] {
                    let mut incomplete = process.clone();
                    incomplete["layout_table_policies"][1]["parquet_columns"] = columns;
                    let mut result = initial.clone();
                    promote_collector_readback(&config, &mut result, Some(&json!([incomplete])), 1)?;
                    assert!(projected(&result, "parquet_writer.defaults.compression_level"));
                }
                let mut disabled = process.clone();
                disabled["layout_table_policies"][0]["config"]["enabled"] = json!(false);
                let mut result = initial.clone();
                promote_collector_readback(&config, &mut result, Some(&json!([disabled])), 1)?;
                assert!(projected(&result, "parquet_writer.max_row_group_rows"));
                assert!(projected(&result, "collector.layout.bloom_columns"));
                assert!(projected(&result, "collector.layout.sort_columns"));
                assert!(!projected(&result, "parquet_writer.defaults.compression"));
                process["layout_table_policies"][0]["config"]["row_group_rows"] = json!(1);
                let mut uncovered = initial.clone();
                promote_collector_readback(&config, &mut uncovered, Some(&json!([process.clone()])), 1)?;
                assert!(uncovered["projection_only"].as_array().context("missing projection fields")?
                    .iter().any(|entry| entry["path"] == "parquet_writer.max_row_group_rows"));
                process["layout_table_policies"][0]["parquet_columns"]["value"]["compression"] = json!("snappy");
                let mut wrong_codec = initial.clone();
                promote_collector_readback(&config, &mut wrong_codec, Some(&json!([process.clone()])), 1)?;
                assert!(wrong_codec["projection_only"].as_array().context("missing projection fields")?
                    .iter().any(|entry| entry["path"] == "parquet_writer.defaults.compression"));
                process["layout_table_policies"].as_array_mut().context("missing table policies")?.remove(0);
                let mut missing = initial.clone();
                promote_collector_readback(&config, &mut missing, Some(&json!([process])), 1)?;
                assert_eq!(missing, initial);
                config.tables.pop();
            }
            let mut leaves = Vec::new();
            leaf_paths(&policy, &mut Vec::new(), &mut leaves);
            for path in leaves {
                let mut missing = observed.clone();
                let mut parent = &mut missing["parquet_writer"];
                for part in &path[..path.len() - 1] {
                    parent = parent.get_mut(part).context("missing policy parent")?;
                }
                parent.as_object_mut().context("policy parent must be an object")?
                    .remove(&path[path.len() - 1]).context("consumer omitted requested policy leaf")?;
                anyhow::ensure!(
                    verify_process_configuration(&config, ComponentRole::Collector, &missing).is_err(),
                    "verifier accepted missing Parquet leaf {}", path.join(".")
                );
            }
            let mut wrong = observed.clone();
            wrong["parquet_writer"]["defaults"]["compression"] = json!("invalid-compression");
            assert!(verify_process_configuration(&config, ComponentRole::Collector, &wrong).is_err());
            observed.as_object_mut().context("startup readback must be an object")?.remove("parquet_writer");
            assert!(verify_process_configuration(&config, ComponentRole::Collector, &observed).is_err());
        }
        Ok(())
    }

    fn actual_client_startup(binary: &Path, config: &OatConfig) -> Result<Value> {
        let table = config.tables.first().context("startup fixture needs a table")?;
        let mut command = std::process::Command::new(binary);
        command.env_clear().envs(role_environment(config, ComponentRole::Client)?);
        command.args([
            "--endpoint", "http://127.0.0.1:0", "--table", table,
            "--rows", &config.rows_per_table.to_string(),
            "--concurrency", &config.dmv_concurrency.to_string(),
        ]);
        let output = command.output().context("launch actual client binary")?;
        anyhow::ensure!(!output.status.success(), "unreachable endpoint unexpectedly succeeded");
        let text = format!("{}\n{}", String::from_utf8(output.stdout)?, String::from_utf8(output.stderr)?);
        anyhow::ensure!(text.lines().any(|line| line.starts_with("OAT_CLIENT_EFFECTIVE_CONFIG ")),
            "consumer omitted startup envelope");
        process_configurations(&text)
    }

    // Real binary output, not a fabricated effective-config envelope. Each sample must
    // differ from the same binary's baseline so a dropped env var cannot pass by default.
    #[test]
    #[ignore = "requires built arrowcol-dmv and an unreachable local endpoint"]
    fn client_leaf_adapters_use_actual_consumer_startup() -> Result<()> {
        let cases = [
            (json!({"client":{"payload_customer_id":"synthetic-customer"}}), "payload_customer_id"),
            (json!({"client":{"corpus_rows":127}}), "corpus_rows"),
            (json!({"client":{"corpus_bytes":4096}}), "corpus_bytes"),
            (json!({"client":{"batch_rows":257}}), "batch_rows"),
            (json!({"client":{"workers":7}}), "workers"),
            (json!({"client":{"runtime_threads":5}}), "runtime_threads"),
            (json!({"client":{"data_mode":"independent"}}), "data_mode"),
            (json!({"client":{"data_profile":"realistic"}}), "data_profile"),
            (json!({"client":{"send_iter":"arc_cursor"}}), "send_iter"),
            (json!({"client":{"data_profile":"realistic","identity_builder":"direct_utf8"}}), "identity_builder"),
            (json!({"client":{"string_length_distribution":"uniform"}}), "string_length_distribution"),
            (json!({"client":{"value_variation":"per_row"}}), "value_variation"),
            (json!({"client":{"guid_uniqueness":"per_row"}}), "guid_uniqueness"),
            (json!({"client":{"string_width":31}}), "string_width"),
            (json!({"client":{"cardinality":31}}), "cardinality"),
            (json!({"client":{"null_fraction":0.2}}), "null_fraction"),
            (json!({"client":{"skew":0.3}}), "skew"),
            (json!({"client":{"entropy":0.8}}), "entropy"),
            (json!({"client":{"seed":29}}), "seed"),
            (json!({"client":{"max_memory_bytes":268435456}}), "max_memory_bytes"),
            (json!({"client":{"interval_us":7}}), "interval_us"),
            (json!({"client":{"connections_per_worker":3}}), "connections_per_worker"),
            (json!({"client":{"streams_per_worker":7}}), "streams_per_worker"),
            (json!({"client":{"max_rows":129}}), "max_rows"),
            (json!({"client":{"pod_ordinal":3}}), "pod_ordinal"),
            (json!({"client":{"stream_retry_max_attempts":3}}), "stream_retry_max_attempts"),
            (json!({"client":{"stream_retry_base_backoff_ms":13}}), "stream_retry_base_backoff_ms"),
            (json!({"client":{"stream_retry_max_backoff_ms":300}}), "stream_retry_max_backoff_ms"),
            (json!({"transport":{"max_flight_data_bytes":1048576}}), "max_flight_data_bytes"),
            (json!({"transport":{"max_message_bytes":8388608}}), "max_message_bytes"),
            (json!({"transport":{"max_frame_bytes":32768}}), "max_frame_bytes"),
            (json!({"transport":{"grpc_send_compression":"gzip"}}), "grpc_send_compression"),
            (json!({"transport":{"grpc_accept_compression":"zstd"}}), "grpc_accept_compression"),
            (json!({"transport":{"header_table_bytes":8192}}), "header_table_bytes"),
            (json!({"transport":{"max_header_list_bytes":32768}}), "max_header_list_bytes"),
            (json!({"transport":{"stream_window_bytes":2097152}}), "stream_window_bytes"),
            (json!({"transport":{"connection_window_bytes":33554432}}), "connection_window_bytes"),
            (json!({"transport":{"adaptive_window":true}}), "adaptive_window"),
            (json!({"transport":{"keepalive_interval_ms":2000}}), "keepalive_interval_ms"),
            (json!({"transport":{"keepalive_timeout_ms":3000}}), "keepalive_timeout_ms"),
            (json!({"transport":{"keepalive_while_idle":false}}), "keepalive_while_idle"),
            (json!({"transport":{"connect_timeout_ms":1234}}), "connect_timeout_ms"),
            (json!({"transport":{"request_timeout_ms":1200}}), "request_timeout_ms"),
            (json!({"transport":{"tcp_nodelay":false}}), "tcp_nodelay"),
            (json!({"transport":{"tcp_keepalive_ms":1000}}), "tcp_keepalive_ms"),
            (json!({"transport":{"tcp_keepalive_interval_ms":500}}), "tcp_keepalive_interval_ms"),
            (json!({"transport":{"tcp_keepalive_retries":3}}), "tcp_keepalive_retries"),
            (json!({"transport":{"rate_limit_rps":123}}), "rate_limit_rps"),
            (json!({"transport":{"rate_limit_rps":123,"rate_limit_period_ms":250}}), "rate_limit_period_ms"),
            (json!({"transport":{"connect_retries":3}}), "connect_retries"),
            (json!({"transport":{"connect_retries":1,"retry_backoff_ms":1}}), "retry_backoff_ms"),
            (json!({"transport":{"endpoint_concurrency_limit":23}}), "endpoint_concurrency_limit"),
            (json!({"transport":{"endpoint_buffer_size":37}}), "endpoint_buffer_size"),
            (json!({"transport":{"ipc_compression":"zstd"}}), "ipc_compression"),
            (json!({"arrow_ipc":{"compression":"zstd"}}), "ipc_compression"),
            (json!({"arrow_ipc":{"alignment":8}}), "ipc_alignment"),
        ];
        let binary = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..").join("..").join("target").join("debug")
            .join(format!("arrowcol-dmv{}", std::env::consts::EXE_SUFFIX));
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        // Keep legacy sentinel resolutions deterministic and distinct from the test values.
        config.rows_per_table = 128;
        config.dmv_concurrency = 1;
        config.tunables = arrowcol_tunables::Tunables::default();
        let baseline = actual_client_startup(&binary, &config).context("read baseline client startup")?;
        for (mut input, field) in cases {
            // Automatic worker counts depend on the machine, so avoid accidental collisions.
            if field == "workers" && baseline.get(field) == Some(&json!(7)) {
                input["client"]["workers"] = json!(11);
            }
            if field == "runtime_threads" && baseline.get(field) == Some(&json!(5)) {
                input["client"]["runtime_threads"] = json!(11);
            }
            config.tunables = arrowcol_tunables::Tunables::parse_oat_json(input.clone())
                .with_context(|| format!("invalid startup fixture {input}"))?;
            let observed = actual_client_startup(&binary, &config)
                .with_context(|| format!("read consumer startup for {input}"))?;
            let actual = observed.get(field).with_context(|| format!("consumer omitted {field} for {input}"))?;
            anyhow::ensure!(Some(actual) != baseline.get(field), "{field} did not differ from baseline for {input}");
            verify_process_configuration(&config, ComponentRole::Client, &observed)
                .with_context(|| format!("verify real consumer startup for {input}"))?;
            if matches!(field, "max_message_bytes" | "max_flight_data_bytes" | "max_frame_bytes"
                | "payload_customer_id" | "corpus_rows" | "corpus_bytes"
                | "workers" | "runtime_threads" | "batch_rows" | "connections_per_worker"
                | "streams_per_worker" | "interval_us" | "data_mode" | "data_profile" | "send_iter" | "identity_builder" | "max_memory_bytes"
                | "stream_retry_max_attempts" | "stream_retry_base_backoff_ms" | "stream_retry_max_backoff_ms"
                | "header_table_bytes" | "max_header_list_bytes" | "adaptive_window"
                | "stream_window_bytes" | "connection_window_bytes" | "keepalive_interval_ms"
                | "keepalive_timeout_ms" | "keepalive_while_idle"
                | "connect_timeout_ms" | "request_timeout_ms" | "tcp_nodelay"
                | "tcp_keepalive_ms" | "tcp_keepalive_interval_ms" | "tcp_keepalive_retries"
                | "rate_limit_period_ms" | "rate_limit_rps" | "connect_retries" | "retry_backoff_ms"
                | "grpc_send_compression" | "grpc_accept_compression" | "ipc_compression" | "ipc_alignment"
                | "endpoint_concurrency_limit" | "endpoint_buffer_size") {
                config.clients_per_table = 1;
                let report = json!({"client_id":"client-1", "consumer_effective_config":observed});
                let initial = tunable_leaf_evidence(&config)?;
                let mut covered = initial.clone();
                promote_client_readback(&config, &mut covered, Some(&vec![report.clone()]))?;
                let path = if input.get("arrow_ipc").is_some() {
                    format!("arrow_ipc.{}", if field == "ipc_compression" { "compression" } else { "alignment" })
                } else {
                    format!("{}.{}", if input.get("client").is_some() { "client" } else { "transport" }, field)
                };
                assert!(covered["consumer_verified"].as_array().context("consumer_verified array")?
                    .iter().any(|entry| entry["path"] == path), "missing verified field {path}");
                let mut wrong_report = report.clone();
                wrong_report["consumer_effective_config"][field] = json!("wrong");
                for reports in [vec![], vec![wrong_report], vec![report.clone(), report]] {
                    let mut uncovered = initial.clone();
                    config.clients_per_table = if reports.len() == 2 { 2 } else { 1 };
                    promote_client_readback(&config, &mut uncovered, Some(&reports))?;
                    assert_eq!(uncovered, initial);
                }
            }
            let mut missing = observed.clone();
            missing.as_object_mut().context("consumer startup is not an object")?.remove(field);
            anyhow::ensure!(verify_process_configuration(&config, ComponentRole::Client, &missing).is_err(),
                "adapter accepted missing {field} for {input}");
            let mut wrong = observed;
            wrong[field] = json!("intentional-mismatch");
            anyhow::ensure!(verify_process_configuration(&config, ComponentRole::Client, &wrong).is_err(),
                "adapter accepted wrong {field} for {input}");
        }
        config.clients_per_table = 1;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({"client":{"null_fraction":0.2}}))?;
        let observed = actual_client_startup(&binary, &config)?;
        let table = config.tables.first().context("startup fixture needs a table")?;
        let mut prepared = json!({
            "corpus_rows": 8,
            "generator_applicability": {"version":1,"tables":[{
                "table":table,"schema_fields":2,"rows":8,
                "null_fraction":{"eligible_columns":0,"eligible_rows":0,"null_rows":0}
            }]}
        });
        let preparation = client_preparation(Some(&prepared), ClientDataProfile::Diagnostic)?;
        let initial = tunable_leaf_evidence(&config)?;
        let report = json!({
            "client_id":"client-1", "table":table, "consumer_effective_config":observed,
            "phase_counters":{"preparation":preparation}
        });
        let mut covered = initial.clone();
        promote_client_readback(&config, &mut covered, Some(&vec![report.clone()]))?;
        assert!(covered["inert_fields"].as_array().context("inert_fields array")?
            .iter().any(|entry| entry["path"] == "client.null_fraction" && entry["reason"] == "no_nullable_generated_columns"));
        for case in ["missing", "eligible_columns", "wrong_table"] {
            let mut absent = report.clone();
            match case {
                "missing" => absent["phase_counters"]["preparation"]["generator_applicability"] = Value::Null,
                "eligible_columns" => {
                    absent["phase_counters"]["preparation"]["generator_applicability"]["tables"][0]["null_fraction"]["eligible_columns"] = json!(1);
                    absent["phase_counters"]["preparation"]["generator_applicability"]["tables"][0]["null_fraction"]["eligible_rows"] = json!(8);
                },
                "wrong_table" => absent["table"] = json!("other"),
                _ => unreachable!(),
            }
            let mut unverified = initial.clone();
            promote_client_readback(&config, &mut unverified, Some(&vec![absent]))?;
            assert!(unverified["consumer_verified"].as_array().context("consumer_verified array")?.is_empty());
            assert!(unverified["inert_fields"].as_array().context("inert_fields array")?.is_empty());
        }
        prepared["generator_applicability"]["tables"][0]["null_fraction"]["null_rows"] = json!(9);
        assert!(client_preparation(Some(&prepared), ClientDataProfile::Diagnostic).is_err());
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "client":{"pod_ordinal":3,"guid_uniqueness":"per_row","value_variation":"per_row","string_width":31}
        }))?;
        let observed = actual_client_startup(&binary, &config)?;
        prepared["generator_applicability"]["tables"][0]["null_fraction"]["null_rows"] = json!(0);
        prepared["generator_applicability"]["tables"][0]["pod_ordinal"] =
            json!({"eligible_columns":1,"eligible_rows":8});
        prepared["generator_applicability"]["tables"][0]["guid_uniqueness"] =
            json!({"eligible_columns":1,"eligible_rows":8});
        prepared["generator_applicability"]["tables"][0]["value_variation"] =
            json!({"eligible_columns":1,"eligible_rows":8});
        prepared["generator_applicability"]["tables"][0]["string_width"] =
            json!({"eligible_columns":1,"eligible_rows":8});
        let preparation = client_preparation(Some(&prepared), ClientDataProfile::Diagnostic)?;
        let initial = tunable_leaf_evidence(&config)?;
        let report = json!({
            "client_id":"client-1", "table":table, "consumer_effective_config":observed,
            "phase_counters":{"preparation":preparation}
        });
        let mut covered = initial.clone();
        promote_client_readback(&config, &mut covered, Some(&vec![report.clone()]))?;
        assert!(covered["consumer_verified"].as_array().context("consumer_verified array")?
            .iter().any(|entry| entry["path"] == "client.pod_ordinal"));
        assert!(covered["consumer_verified"].as_array().context("consumer_verified array")?
            .iter().any(|entry| entry["path"] == "client.guid_uniqueness"));
        assert!(covered["consumer_verified"].as_array().context("consumer_verified array")?
            .iter().any(|entry| entry["path"] == "client.value_variation"));
        assert!(covered["consumer_verified"].as_array().context("consumer_verified array")?
            .iter().any(|entry| entry["path"] == "client.string_width"));
        let mut absent = report.clone();
        absent["phase_counters"]["preparation"]["generator_applicability"]["tables"][0]["pod_ordinal"] = Value::Null;
        let mut uncovered = initial.clone();
        promote_client_readback(&config, &mut uncovered, Some(&vec![absent]))?;
        assert!(uncovered["projection_only"].as_array().context("projection_only array")?
            .iter().any(|entry| entry["path"] == "client.pod_ordinal"));
        let mut zero = report;
        zero["phase_counters"]["preparation"]["generator_applicability"]["tables"][0]["pod_ordinal"] =
            json!({"eligible_columns":0,"eligible_rows":0});
        let mut inert = initial;
        promote_client_readback(&config, &mut inert, Some(&vec![zero.clone()]))?;
        assert!(inert["inert_fields"].as_array().context("inert_fields array")?
            .iter().any(|entry| entry["path"] == "client.pod_ordinal" && entry["reason"] == "no_eligible_generated_rows"));
        assert!(inert["consumer_verified"].as_array().context("consumer_verified array")?
            .iter().any(|entry| entry["path"] == "client.guid_uniqueness"));
        for (skew, target, reason) in [(1.0, "inert_fields", Some("all_values_skewed")), (0.5, "projection_only", None)] {
            let mut skewed = zero.clone();
            skewed["consumer_effective_config"]["skew"] = json!(skew);
            let mut classified = tunable_leaf_evidence(&config)?;
            promote_client_readback(&config, &mut classified, Some(&vec![skewed]))?;
            assert!(classified[target].as_array().context("evidence array")?.iter().any(|entry|
                entry["path"] == "client.value_variation" && reason.is_none_or(|reason| entry["reason"] == reason)));
        }
        prepared["generator_applicability"]["tables"][0]["pod_ordinal"]["eligible_rows"] = json!(9);
        assert!(client_preparation(Some(&prepared), ClientDataProfile::Diagnostic).is_err());
        for profile in ["diagnostic", "realistic"] {
            config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
                "client":{"string_width":0,"data_profile":profile}
            }))?;
            let observed = actual_client_startup(&binary, &config)?;
            verify_process_configuration(&config, ComponentRole::Client, &observed)?;
            assert_eq!(observed["string_width"], if profile == "realistic" { 96 } else { 0 });
            let mut incorrect = observed;
            incorrect["resolution"]["requested"]["string_width"] = json!(1);
            assert!(verify_process_configuration(&config, ComponentRole::Client, &incorrect).is_err());
        }
        for (input, inactive, active) in [
            (json!({"corpus_rows":0,"corpus_bytes":4096}), "client.corpus_rows", "client.corpus_bytes"),
            (json!({"corpus_rows":127,"corpus_bytes":0}), "client.corpus_bytes", "client.corpus_rows"),
        ] {
            config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({"client":input}))?;
            let observed = actual_client_startup(&binary, &config)?;
            let mut covered = tunable_leaf_evidence(&config)?;
            promote_client_readback(&config, &mut covered, Some(&vec![json!({
                "client_id":"client-1", "consumer_effective_config":observed
            })]))?;
            assert!(covered["projection_only"].as_array().context("missing projection fields")?
                .iter().any(|entry| entry["path"] == inactive));
            assert!(covered["consumer_verified"].as_array().context("missing verified fields")?
                .iter().any(|entry| entry["path"] == active));
        }
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({"client":{
            "workers":0, "runtime_threads":0, "batch_rows":0, "streams_per_worker":0
        }}))?;
        let auto = actual_client_startup(&binary, &config)?;
        let initial = tunable_leaf_evidence(&config)?;
        let mut covered = initial.clone();
        let mut report = json!({"client_id":"client-1", "consumer_effective_config":auto});
        promote_client_readback(&config, &mut covered, Some(&vec![report.clone()]))?;
        assert_eq!(covered["consumer_verified"].as_array().context("missing verified fields")?.len(), 4);
        report["consumer_effective_config"]["resolution"]["requested"]["workers"] = json!(1);
        let mut unverified = initial;
        promote_client_readback(&config, &mut unverified, Some(&vec![report]))?;
        assert!(unverified["consumer_verified"].as_array().context("missing verified fields")?.is_empty());
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({"transport":{
            "adaptive_window":true, "stream_window_bytes":2097152, "connection_window_bytes":33554432,
            "keepalive_interval_ms":0, "keepalive_timeout_ms":3000, "keepalive_while_idle":false,
            "tcp_keepalive_ms":0, "tcp_keepalive_interval_ms":500, "tcp_keepalive_retries":3,
            "rate_limit_period_ms":250
        }}))?;
        let observed = actual_client_startup(&binary, &config)?;
        let mut coverage = tunable_leaf_evidence(&config)?;
        promote_client_readback(&config, &mut coverage, Some(&vec![json!({
            "client_id":"client-1", "consumer_effective_config":observed
        })]))?;
        for field in ["transport.stream_window_bytes", "transport.connection_window_bytes",
            "transport.keepalive_timeout_ms", "transport.keepalive_while_idle",
            "transport.tcp_keepalive_interval_ms", "transport.tcp_keepalive_retries",
            "transport.rate_limit_period_ms"] {
            assert!(coverage["inert_fields"].as_array().context("missing inert fields")?
                .iter().any(|entry| entry["path"] == field));
            assert!(!coverage["consumer_verified"].as_array().context("missing verified fields")?
                .iter().any(|entry| entry["path"] == field));
        }
        Ok(())
    }
}
