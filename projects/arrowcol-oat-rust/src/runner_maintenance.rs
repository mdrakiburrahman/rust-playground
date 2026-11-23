fn maintenance_enabled(config: &OatConfig) -> bool {
    config
        .tunables
        .maintenance
        .as_ref()
        .is_some_and(|settings| settings.enabled == Some(true))
}

fn maintenance_runtime(config: &OatConfig) -> Result<Option<Value>> {
    if !maintenance_enabled(config) {
        return Ok(None);
    }
    let settings = config
        .tunables
        .maintenance
        .as_ref()
        .context("enabled maintenance settings missing")?;
    let operations = settings
        .operations
        .as_ref()
        .context("maintenance operations must be explicit")?;
    let operations = operations
        .iter()
        .map(|operation| match operation {
            arrowcol_tunables::MaintenanceOperation::Optimize => "optimize",
            arrowcol_tunables::MaintenanceOperation::Checkpoint => "checkpoint",
            arrowcol_tunables::MaintenanceOperation::Purge => "purge",
            arrowcol_tunables::MaintenanceOperation::Vacuum => "vacuum",
            arrowcol_tunables::MaintenanceOperation::PruneLog => "prune-log",
        })
        .collect::<Vec<_>>();
    if operations.is_empty() {
        bail!("maintenance operations must not be empty");
    }
    Ok(Some(json!({
        "tables": config.tables.iter().map(|table| canonical_dmv_table_name(table)).collect::<Result<Vec<_>>>()?,
        "operations": operations,
        "apply": settings.apply.unwrap_or(false),
        "granularity": settings.granularity.unwrap_or(arrowcol_tunables::MaintenanceGranularity::Hour),
    })))
}

fn maintenance_arguments(config: &OatConfig, catalog_endpoint: &str) -> Result<Vec<String>> {
    let policy = maintenance_runtime(config)?.context("maintenance is disabled")?;
    let settings = config.tunables.maintenance.as_ref().context("maintenance settings missing")?;
    let interval = settings.interval_secs.unwrap_or(60);
    let max_idle_backoff = settings.max_idle_backoff_secs.unwrap_or(interval);
    let mut args = match config.platform.kind {
        crate::config::OatMode::Local => vec![
            "--root".to_owned(),
            "file:///tmp/arrowcol-stress/dataflow".to_owned(),
        ],
        crate::config::OatMode::Aks => vec![
            "--catalog-endpoint".to_owned(),
            catalog_endpoint.to_owned(),
        ],
    };
    args.extend([
        "--interval-secs".to_owned(),
        interval.to_string(),
        "--max-idle-backoff-secs".to_owned(),
        max_idle_backoff.to_string(),
        "--listen".to_owned(),
        "0.0.0.0:9467".to_owned(),
    ]);
    for (field, flag) in [("tables", "--table"), ("operations", "--operation")] {
        for value in policy[field].as_array().context("invalid maintenance policy array")? {
            args.extend([
                flag.to_owned(),
                value.as_str().context("invalid maintenance policy value")?.to_owned(),
            ]);
        }
    }
    if policy["apply"] == true {
        args.push("--apply".to_owned());
    }
    Ok(args)
}

fn compose_maintenance(config: &OatConfig, services: &mut serde_json::Map<String, Value>) -> Result<()> {
    if !maintenance_enabled(config) {
        return Ok(());
    }
    let mut worker = json!({
        "image": config.maintenance_image.as_deref().unwrap_or("arrowcol-oat/maintenance:local"),
        "command": maintenance_arguments(config, "http://arrowcol-catalog:8817")?,
        "environment": role_environment(config, ComponentRole::Maintenance)?,
    });
    if config.maintenance_image.is_none() {
        worker["build"] = json!({
            "context": std::env::current_dir()?,
            "dockerfile": "crates/delta-maintenance/Dockerfile",
            "labels": { "org.opencontainers.image.revision": std::env::var("GIT_SHA").unwrap_or_else(|_| "unknown".to_owned()) },
        });
    }
    services.insert("delta-maintenance".to_owned(), worker);
    Ok(())
}

async fn pin_local_maintenance_image(config: &OatConfig, platform: &LocalPlatform) -> Result<()> {
    if !maintenance_enabled(config) {
        return Ok(());
    }
    let mut inspect = Command::new("docker");
    inspect.args([
        "image",
        "inspect",
        "--format",
        "{{.Id}}",
        config
            .maintenance_image
            .as_deref()
            .unwrap_or("arrowcol-oat/maintenance:local"),
    ]);
    let image = String::from_utf8(
        run_command(inspect, "resolve maintenance image content identity")
            .await?
            .stdout,
    )?;
    let image = image.trim();
    if !image
        .strip_prefix("sha256:")
        .is_some_and(|digest| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        bail!("maintenance image did not resolve to an immutable Docker content ID");
    }
    let path = platform.state_dir.join("tunables.compose.json");
    let mut overlay: Value = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
    let worker = overlay
        .pointer_mut("/services/delta-maintenance")
        .and_then(Value::as_object_mut)
        .context("maintenance Compose overlay missing")?;
    worker.insert("image".to_owned(), json!(image));
    worker.remove("build");
    tokio::fs::write(&path, serde_json::to_vec(&overlay)?).await?;
    Ok(())
}

fn local_partition_policy_evidence(
    requested: &str,
    table: &str,
    collector: &Value,
    maintenance_log: &str,
) -> Result<Option<Value>> {
    let environment = collector
        .pointer("/Config/Env")
        .and_then(Value::as_array)
        .context("collector container environment is missing")?;
    let values = environment
        .iter()
        .filter_map(Value::as_str)
        .filter_map(|entry| entry.strip_prefix("ARROWCOL_PARTITION_GRANULARITY="))
        .collect::<Vec<_>>();
    if values.as_slice() != [requested] {
        bail!("collector partition granularity does not match the campaign request");
    }
    let mut observed = false;
    for line in maintenance_log.lines().filter_map(|line| line.strip_prefix("arrowcol_maintenance_table_config ")) {
        let event: Value = serde_json::from_str(line).context("invalid maintenance table policy readback")?;
        if event["table"].as_str() != Some(table) {
            continue;
        }
        if event.pointer("/config/active") != Some(&json!(true)) {
            bail!("maintenance table policy is not active");
        }
        let actual = event.pointer("/config/table_policy/arrowcol.partition.granularity")
            .and_then(Value::as_str)
            .context("maintenance table policy omitted partition granularity")?;
        if actual != requested {
            bail!("effective table partition granularity conflicts with the campaign request");
        }
        observed = true;
    }
    Ok(observed.then(|| json!({
        "table": table,
        "requested": requested,
        "collector_process": requested,
        "maintenance_table_effective": requested,
        "collector_source": "docker_container_environment",
        "maintenance_source": "consumer_table_policy_readback",
        "writer_resolution": "table_properties_over_process_over_registry",
        "scope": "before_client_admission",
    })))
}

fn validate_local_table_partition_property<'a>(
    properties: &'a std::collections::BTreeMap<String, String>,
    requested: &str,
) -> Result<Option<&'a str>> {
    let table_value = properties.get("arrowcol.partition.granularity").map(String::as_str);
    if table_value.is_some_and(|value| value != requested) {
        bail!("authoritative table partition granularity conflicts with the campaign request");
    }
    Ok(table_value)
}

async fn verify_local_partition_policy_before_admission(
    config: &OatConfig,
    platform: &LocalPlatform,
    tables: &[String],
) -> Result<()> {
    if config.campaign_profile != Some(crate::config::CampaignProfile::SingleHotTable)
        && !config.platform.local.partitioned_table_bootstrap
    {
        return Ok(());
    }
    let [table] = tables else {
        bail!("local campaign partition policy requires exactly one table");
    };
    let table = canonical_dmv_table_name(table)?;
    let granularity = config.tunables.maintenance.as_ref()
        .and_then(|settings| settings.granularity)
        .context("local campaign requires explicit maintenance.granularity")?;
    let granularity = serde_json::to_value(granularity)?;
    let requested = granularity.as_str().context("invalid typed partition granularity")?;
    let collectors = local_process_logs_for_role(config, platform, Some(ComponentRole::Collector)).await?;
    let [collector] = collectors.as_slice() else {
        bail!("local campaign requires exactly one collector process");
    };
    let mut inspect = Command::new("docker");
    inspect.args(["inspect", &collector.instance_id]);
    let containers: Vec<Value> = serde_json::from_slice(
        &run_command(inspect, "read actual collector partition configuration").await?.stdout,
    )?;
    let [collector] = containers.as_slice() else {
        bail!("collector process identity is ambiguous");
    };
    if collector.pointer("/State/Running") != Some(&json!(true)) {
        bail!("collector is not running before campaign admission");
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let logs = local_process_logs_for_role(config, platform, Some(ComponentRole::Maintenance)).await?;
        let [log] = logs.as_slice() else {
            bail!("local campaign requires exactly one maintenance process");
        };
        if let Some(mut evidence) = local_partition_policy_evidence(requested, &table, collector, &log.text)? {
            verify_process_configuration(config, ComponentRole::Maintenance, &process_configurations(&log.text)?)?;
            let root = platform.state_dir.join("dataflow/telemetry/sqlserver/sqlserver_availability_replica_states");
            let properties = tokio::task::spawn_blocking(move || -> Result<_> {
                let url = url::Url::from_directory_path(root)
                    .map_err(|()| anyhow::anyhow!("invalid local campaign table path"))?;
                arrow_deltalake::delta::DeltaTable::open_for_maintenance_probe(&url)?
                    .maintenance_properties()
            }).await.context("join local partition metadata read")??;
            let table_value = validate_local_table_partition_property(&properties, requested)?;
            evidence["table_property"] = json!(table_value);
            evidence["table_property_source"] = json!("fresh_checkpoint_aware_delta_metadata_read");
            local_progress(config, &format!("pre-admission partition policy: {evidence}"));
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("maintenance did not emit effective table partition policy before admission");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn start_local_maintenance(config: &OatConfig, platform: &LocalPlatform) -> Result<()> {
    if !maintenance_enabled(config) {
        return Ok(());
    }
    let project = scoped_compose_project(
        config
            .platform
            .compose_project
            .as_deref()
            .context("local project missing")?,
        &platform.run_id,
    );
    let mut command = compose_command(
        config
            .platform
            .compose_file
            .as_deref()
            .context("local compose missing")?,
        config.platform.compose_override_file.as_deref(),
        &project,
        "maintenance",
    );
    platform.add_compose_environment(&mut command);
    command.args([
        "up",
        "--detach",
        "--no-deps",
        "--wait",
        "--wait-timeout",
        "600",
        "delta-maintenance",
    ]);
    run_streaming_command(command, "start allowlisted local maintenance worker").await
}

async fn start_aks_maintenance(config: &OatConfig) -> Result<()> {
    if !maintenance_enabled(config) {
        return Ok(());
    }
    kubectl(
        config,
        vec![
            "scale",
            "--namespace",
            &config.namespace,
            "deployment/delta-maintenance",
            "--replicas=1",
        ],
        None,
    )
    .await
    .context("start allowlisted run-owned maintenance worker")?;
    rollout(config, "delta-maintenance").await
}

fn inject_maintenance_arguments(config: &OatConfig, resource: &mut Value) -> Result<()> {
    if resource.pointer("/metadata/name").and_then(Value::as_str) != Some("delta-maintenance") {
        return Ok(());
    }
    let containers = resource
        .pointer_mut("/spec/template/spec/containers")
        .and_then(Value::as_array_mut)
        .context("maintenance Deployment containers missing")?;
    let worker = containers
        .iter_mut()
        .find(|container| container["name"] == "maintenance")
        .context("maintenance container missing")?;
    worker["args"] = json!(maintenance_arguments(config, "http://arrowcol-catalog:8817")?);
    Ok(())
}

fn maintenance_readback_value(observed: &Value, variable: &str, expected: Value) -> Result<(Value, Value)> {
    let field = variable
        .strip_prefix("ARROWCOL_MAINT_")
        .context("not a maintenance environment name")?;
    let (path, factor, invert) = match field {
        "INTERVAL_SECS"
        | "MAX_IDLE_BACKOFF_SECS"
        | "MAX_CYCLES"
        | "MAX_CONSECUTIVE_STALE_PLANS"
        | "SEALED_PARTITIONS_ONLY"
        | "MAX_CONCURRENT_TABLES"
        | "BIN_CONCURRENCY"
        | "MAX_CONCURRENT_DECODED_BYTES"
        | "COMMIT_ATTEMPTS"
        | "COMMIT_BUDGET_SECS"
        | "OUTPUT_ROW_GROUP_ROWS"
        | "MAX_PRUNE_FILES"
        | "ANCHOR_MS"
        | "INPUT_READ_STRATEGY"
        | "INPUT_PREFETCH_FILES"
        | "CATALOG_RUNTIME_THREADS"
        | "CATALOG_RPC_TIMEOUT_SECS"
        | "CATALOG_MAX_DECODING_MESSAGE_BYTES" => (format!("/startup/{}", field.to_ascii_lowercase()), 1, false),
        "PURGE_MAX_FILES_PER_COMMIT"
        | "PURGE_MAX_PLAN_BYTES"
        | "OPTIMIZE_UPLOAD_MODE"
        | "OPTIMIZE_UPLOAD_BUFFER_BYTES"
        | "OPTIMIZE_UPLOAD_CONCURRENCY"
        | "VACUUM_DELETE_CHUNK"
        | "VACUUM_MAX_PLAN_BYTES"
        | "REPLAY_MAX_LOG_FILES"
        | "REPLAY_MAX_LOG_FILE_BYTES"
        | "REPLAY_MAX_CHECKPOINT_BYTES"
        | "OPERATION_MAX_ACTIVE_FILES"
        | "DISCOVERY_MAX_LOG_FILES"
        | "DISCOVERY_MAX_LOG_FILE_BYTES" => (
            format!("/startup/operation_limits/{}", field.to_ascii_lowercase()),
            1,
            false,
        ),
        "OPERATION_TIMEOUT_SECS" => (
            "/table_policy/arrowcol.maintenance.operationTimeout".to_owned(),
            1000,
            false,
        ),
        "TARGET_FILE_MIB" => (
            "/table_policy/arrowcol.optimize.targetFileSize".to_owned(),
            1024 * 1024,
            false,
        ),
        "PURGE_RETENTION_SECS" => ("/table_policy/arrowcol.purge.retention".to_owned(), 1000, false),
        "VACUUM_RETENTION_SECS" => ("/operation_settings/vacuum_retention_ms".to_owned(), 1000, false),
        "LOG_RETENTION_SECS" => ("/operation_settings/log_retention_ms".to_owned(), 1000, false),
        "ALLOW_SHORT_VACUUM_RETENTION" => ("/operation_settings/allow_short_vacuum_retention".to_owned(), 1, false),
        "GRANULARITY" => ("/table_policy/arrowcol.partition.granularity".to_owned(), 1, false),
        "SMALL_FILE_BYTES" => ("/table_policy/arrowcol.optimize.smallFileSize".to_owned(), 1, false),
        "MIN_FILES" => ("/table_policy/arrowcol.optimize.minFiles".to_owned(), 1, false),
        "VACUUM_MAX_DELETES" => ("/table_policy/arrowcol.vacuum.maxDeletes".to_owned(), 1, false),
        "VACUUM_MAX_LISTED_FILES" => ("/table_policy/arrowcol.vacuum.maxListedFiles".to_owned(), 1, false),
        "LAYOUT_ENABLED" => ("/table_policy/arrowcol.layout.enabled".to_owned(), 1, false),
        "LAYOUT_SORT_COLUMNS" => ("/table_policy/arrowcol.layout.sortColumns".to_owned(), 1, false),
        "LAYOUT_SORT_OPTIONS" => ("/table_policy/arrowcol.layout.sortOptions".to_owned(), 1, false),
        "LAYOUT_BLOOM_COLUMNS" => ("/table_policy/arrowcol.layout.bloomColumns".to_owned(), 1, false),
        "LAYOUT_MAX_ROWS" => ("/table_policy/arrowcol.layout.maxRows".to_owned(), 1, false),
        "LAYOUT_MAX_INPUT_BYTES" => ("/table_policy/arrowcol.layout.maxInputSize".to_owned(), 1, false),
        "LAYOUT_ROW_GROUP_ROWS" => ("/table_policy/arrowcol.layout.rowGroupRows".to_owned(), 1, false),
        "LAYOUT_PAGE_ROWS" => ("/table_policy/arrowcol.layout.pageRows".to_owned(), 1, false),
        "MAX_FILES_PER_BIN" => ("/table_policy/arrowcol.optimize.maxFilesPerBin".to_owned(), 1, false),
        "MAX_BINS_PER_COMMIT" => ("/table_policy/arrowcol.optimize.maxBinsPerCommit".to_owned(), 1, false),
        "MAX_ACTIONS_PER_COMMIT" => (
            "/table_policy/arrowcol.optimize.maxActionsPerCommit".to_owned(),
            1,
            false,
        ),
        "MAX_PLAN_BYTES" => ("/table_policy/arrowcol.optimize.maxPlanSize".to_owned(), 1, false),
        "MAX_DECODED_BYTES_PER_BIN" => (
            "/table_policy/arrowcol.optimize.maxDecodedSizePerBin".to_owned(),
            1,
            false,
        ),
        "MAX_ROW_GROUP_BYTES" => ("/table_policy/arrowcol.optimize.maxRowGroupSize".to_owned(), 1, false),
        "MAX_BATCH_BYTES" => ("/table_policy/arrowcol.optimize.maxBatchSize".to_owned(), 1, false),
        "BATCH_ROWS" => ("/table_policy/arrowcol.optimize.batchRows".to_owned(), 1, false),
        "MAX_OUTPUT_FILE_BYTES" => ("/table_policy/arrowcol.optimize.maxOutputFileSize".to_owned(), 1, false),
        "MAX_ACTIVE_FILES" => ("/table_policy/arrowcol.optimize.maxActiveFiles".to_owned(), 1, false),
        "LATEST_PARTITIONS" => ("/table_policy/arrowcol.optimize.latestPartitions".to_owned(), 1, false),
        "SEALED_FINAL" => (
            "/table_policy/arrowcol.optimize.sealedPartitionsOnly".to_owned(),
            1,
            true,
        ),
        "CHECKPOINT_EVERY_COMMITS" => ("/table_policy/delta.checkpointInterval".to_owned(), 1, false),
        _ => bail!("maintenance environment has no checked effective-policy adapter: {variable}"),
    };
    let expected = if invert {
        json!(!expected.as_bool().context("maintenance inversion requires a boolean")?)
    } else if factor != 1 {
        json!(
            expected
                .as_u64()
                .context("maintenance unit conversion requires an unsigned integer")?
                .checked_mul(factor)
                .context("maintenance unit conversion overflow")?
        )
    } else if matches!(field, "LAYOUT_SORT_COLUMNS" | "LAYOUT_BLOOM_COLUMNS") {
        json!(
            expected
                .as_str()
                .context("maintenance columns must be comma separated")?
                .split(',')
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
        )
    } else {
        expected
    };
    let actual = observed
            .pointer(&path)
            .with_context(|| format!("maintenance startup omitted {variable}"))?
            .clone();
    if field == "LAYOUT_SORT_OPTIONS" {
        let parse = |values: Vec<&str>| -> Result<Value> {
            let options = values.into_iter()
                .map(arrow_deltalake::layout::SortKeyOptions::from_property)
                .collect::<Result<Vec<_>>>()?;
            Ok(serde_json::to_value(options)?)
        };
        let actual: Vec<arrow_deltalake::layout::SortKeyOptions> = serde_json::from_value(actual)
            .context("maintenance sort options readback must be a typed array")?;
        let expected = expected.as_str().context("maintenance sort options must be comma separated")?;
        return Ok((serde_json::to_value(actual)?, parse(expected.split(',').collect())?));
    }
    Ok((actual, expected))
}

fn verify_maintenance_controls(config: &OatConfig, observed: &Value) -> Result<()> {
    let settings = config
        .tunables
        .maintenance
        .as_ref()
        .context("maintenance settings missing")?;
    if observed.pointer("/startup/apply") != Some(&json!(settings.apply.unwrap_or(false)))
        || observed.pointer("/startup/operations") != Some(&serde_json::to_value(&settings.operations)?)
        || observed.pointer("/startup/interval_secs") != Some(&json!(settings.interval_secs.unwrap_or(60)))
    {
        bail!("maintenance process did not confirm explicit operation/apply/schedule controls");
    }
    if let Some(expected) = settings.max_consecutive_stale_plans
        && (observed.pointer("/startup/max_consecutive_stale_plans") != Some(&json!(expected))
            || observed.pointer("/operation_settings/max_consecutive_stale_plans") != Some(&json!(expected)))
    {
        bail!("maintenance did not confirm its effective stale-plan health limit");
    }
    if let Some(requested) = settings.sealed_partitions_only {
        if observed.pointer("/startup/sealed_partitions_only") != Some(&json!(requested)) {
            bail!("maintenance did not confirm its requested sealed-partition preference");
        }
        let effective = observed.pointer("/operation_settings/sealed_partitions_only")
            .filter(|value| value.is_boolean()).context("maintenance effective sealed-partition policy missing")?;
        if observed.pointer("/table_policy/arrowcol.optimize.sealedPartitionsOnly") != Some(effective) {
            bail!("maintenance effective sealed-partition policy disagrees with table policy");
        }
    }
    Ok(())
}

fn sanitized_maintenance_error_kind(kind: Option<&str>) -> String {
    kind.filter(|kind| {
        !kind.is_empty()
            && kind.len() <= 64
            && kind
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    })
    .unwrap_or("unknown")
    .to_owned()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MaintenanceRecordOutcome {
    Succeeded,
    Retry,
    Failed,
}

fn maintenance_record_outcome(value: &Value) -> Option<MaintenanceRecordOutcome> {
    let kind = match value.get("error_kind") {
        None | Some(Value::Null) => None,
        Some(Value::String(kind)) if !kind.is_empty() => Some(kind.as_str()),
        _ => return None,
    };
    let error = match value.get("error") {
        None | Some(Value::Null) => false,
        Some(Value::String(_)) => true,
        _ => return None,
    };
    let failed = kind.is_some() || error;
    match value.get("report") {
        None | Some(Value::Null) if failed => {}
        Some(Value::Object(report)) => {
            if report.get("interrupted")?.as_bool()?
                || !["noop", "planned", "committed", "already_applied", "deleted", "checkpointed", "pruned"]
                    .contains(&report.get("outcome")?.as_str()?)
            {
                return None;
            }
        }
        _ => return None,
    }
    Some(if kind == Some("stale_plan") {
        MaintenanceRecordOutcome::Retry
    } else if failed {
        MaintenanceRecordOutcome::Failed
    } else {
        MaintenanceRecordOutcome::Succeeded
    })
}

#[derive(serde::Deserialize, serde::Serialize)]
struct MaintenanceCompactionDebt {
    observed_at_ms: i64,
    source: MaintenanceCompactionDebtSource,
    eligible_files: u64,
    eligible_bytes: u64,
    backlog_files: u64,
    backlog_bytes: u64,
    oldest_eligible_partition_start_ms: Option<i64>,
    protected_partition_backlog: Option<MaintenanceProtectedPartitionBacklog>,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceProtectedPartitionBacklog {
    files: u64,
    bytes: u64,
    #[serde(deserialize_with = "deserialize_present_due_work_option")]
    oldest_partition_start_ms: Option<i64>,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum MaintenanceCompactionDebtSource {
    SnapshotActiveFiles,
    BoundedPhysicalInventory,
}

fn maintenance_compaction_debt(value: &Value, failed: bool) -> Result<Option<MaintenanceCompactionDebt>> {
    if failed || value["operation"] != "optimize"
        || maintenance_record_outcome(value) != Some(MaintenanceRecordOutcome::Succeeded)
    {
        return Ok(None);
    }
    let Some(debt) = value.pointer("/report/compaction_debt").filter(|debt| !debt.is_null()) else {
        return Ok(None);
    };
    let debt: MaintenanceCompactionDebt =
        serde_json::from_value(debt.clone()).context("invalid maintenance compaction debt receipt")?;
    if debt.observed_at_ms <= 0
        || debt.backlog_files > debt.eligible_files
        || debt.backlog_bytes > debt.eligible_bytes
        || debt.oldest_eligible_partition_start_ms.is_some_and(|time| time > debt.observed_at_ms)
        || debt.protected_partition_backlog.as_ref().is_some_and(|backlog| {
            backlog.oldest_partition_start_ms.is_some_and(|time| time > debt.observed_at_ms)
                || (backlog.files == 0 && (backlog.bytes != 0 || backlog.oldest_partition_start_ms.is_some()))
        })
    {
        bail!("invalid maintenance compaction debt values");
    }
    Ok(Some(debt))
}

fn deserialize_present_due_work_option<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct MaintenanceDueWorkEvidence {
    observed_at_ms: i64,
    source: MaintenanceDueWorkSource,
    completeness: MaintenanceDueWorkCompleteness,
    unit: MaintenanceDueWorkUnit,
#[serde(deserialize_with = "deserialize_present_due_work_option")]
    due: Option<bool>,
#[serde(deserialize_with = "deserialize_present_due_work_option")]
    eligible_count: Option<u64>,
#[serde(deserialize_with = "deserialize_present_due_work_option")]
    eligible_bytes: Option<u64>,
#[serde(deserialize_with = "deserialize_present_due_work_option")]
    backlog_count: Option<u64>,
#[serde(deserialize_with = "deserialize_present_due_work_option")]
    backlog_bytes: Option<u64>,
#[serde(deserialize_with = "deserialize_present_due_work_option")]
    oldest_eligible_timestamp_ms: Option<i64>,
#[serde(deserialize_with = "deserialize_present_due_work_option")]
    oldest_timestamp_basis: Option<MaintenanceOldestTimestampBasis>,
#[serde(deserialize_with = "deserialize_present_due_work_option")]
    incomplete_reason: Option<MaintenanceDueWorkIncompleteReason>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MaintenanceDueWorkSource {
    SnapshotActiveFiles,
    BoundedPhysicalInventory,
    BoundedTombstoneReplay,
    CheckpointLogInventory,
    BoundedLogInventory,
    VerifiedPruneProof,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MaintenanceDueWorkCompleteness {
    Complete,
    Incomplete,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MaintenanceDueWorkUnit {
    ParquetFiles,
    Tombstones,
    Commits,
    LogObjects,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MaintenanceOldestTimestampBasis {
    IngestionPartitionStart,
    DeltaRemoveTimestamp,
    LogObjectLastModified,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MaintenanceDueWorkIncompleteReason {
    ActiveFileLimit,
    PhysicalListingLimit,
    LogListingLimit,
    MalformedPartition,
    UnsupportedLayout,
    MissingCheckpointProof,
    UndatedTombstones,
    StaleIdentity,
    OperationFailed,
    Cancelled,
}

fn maintenance_due_work(value: &Value) -> Result<Option<MaintenanceDueWorkEvidence>> {
    let Some(report) = value.get("report") else {
        return Ok(None);
    };
    if maintenance_record_outcome(value) != Some(MaintenanceRecordOutcome::Succeeded) {
        return Ok(None);
    }
    let Some(raw) = report.get("maintenance_due_work") else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    let evidence: MaintenanceDueWorkEvidence =
        serde_json::from_value(raw.clone()).context("invalid maintenance due-work evidence")?;
    let incomplete = evidence.completeness == MaintenanceDueWorkCompleteness::Incomplete;
    if evidence.observed_at_ms <= 0
        || incomplete != evidence.incomplete_reason.is_some()
        || (incomplete
            && (evidence.due.is_some()
                || evidence.eligible_count.is_some()
                || evidence.eligible_bytes.is_some()
                || evidence.backlog_count.is_some()
                || evidence.backlog_bytes.is_some()))
        || (evidence.eligible_count == Some(0) && evidence.due != Some(false))
        || evidence.backlog_count.zip(evidence.eligible_count).is_some_and(|(backlog, eligible)| backlog > eligible)
        || evidence.backlog_bytes.zip(evidence.eligible_bytes).is_some_and(|(backlog, eligible)| backlog > eligible)
        || evidence.oldest_eligible_timestamp_ms.is_some() != evidence.oldest_timestamp_basis.is_some()
        || evidence.oldest_eligible_timestamp_ms.is_some_and(|time| time > evidence.observed_at_ms)
    {
        bail!("maintenance due-work completeness, counts, or timestamp provenance is inconsistent");
    }
    Ok(Some(evidence))
}

fn verify_effective_maintenance_stop_budget(config: &OatConfig, table_policy: &Value) -> Result<()> {
    let settings = config.tunables.maintenance.as_ref().context("maintenance settings missing")?;
    let operation_count = u64::try_from(settings.operations.as_ref().context("maintenance operations missing")?.len())?;
    if operation_count == 0 {
        bail!("maintenance stop budget requires at least one enabled operation");
    }
    let operation_timeout_ms = table_policy
        .get("arrowcol.maintenance.operationTimeout")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .context("effective per-table maintenance operationTimeout is missing or invalid")?;
    let required_grace_ms = operation_timeout_ms
        .checked_mul(operation_count)
        .and_then(|value| value.checked_add(300_000))
        .context("effective per-table maintenance grace calculation overflow")?;
    let required_stop_timeout_ms = required_grace_ms
        .checked_add(300_000)
        .context("effective per-table maintenance stop timeout calculation overflow")?;
    let budget = config.maintenance_stop_budget()?.validate()?;
    if required_grace_ms > budget.maintenance_grace_seconds.saturating_mul(1_000)
        || required_stop_timeout_ms > budget.maintenance_stop_timeout_ms
    {
        bail!("effective per-table maintenance operationTimeout exceeds the persisted stop budget");
    }
    Ok(())
}

async fn verify_maintenance_stop_budget_before_admission(config: &OatConfig, platform: &dyn Platform) -> Result<()> {
    if !maintenance_enabled(config) {
        return Ok(());
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let logs = platform.process_logs_for_role(config, ComponentRole::Maintenance).await?;
        let [log] = logs.as_slice() else {
            bail!("maintenance stop-budget readback requires exactly one maintenance process");
        };
        if !log.complete {
            bail!("maintenance stop-budget readback log is incomplete");
        }
        let policy_count = log
            .text
            .lines()
            .filter(|line| line.starts_with("arrowcol_maintenance_table_config "))
            .count();
        if policy_count >= config.tables.len() {
            let evidence = maintenance_table_evidence(config, &log.text)?;
            local_progress(config, &format!("pre-admission maintenance stop-budget readback: {}", evidence["tables"]));
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("maintenance omitted effective per-table operationTimeout before load admission");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn maintenance_table_evidence(config: &OatConfig, text: &str) -> Result<Value> {
    let mut tables = serde_json::Map::new();
    for line in text
        .lines()
        .filter_map(|line| line.strip_prefix("arrowcol_maintenance_table_config "))
    {
        let event: Value = serde_json::from_str(line).context("invalid maintenance per-table readback")?;
        let table = event["table"]
            .as_str()
            .context("maintenance table readback has no table")?;
        if !config
            .tables
            .iter()
            .map(|name| canonical_dmv_table_name(name))
            .collect::<Result<Vec<_>>>()?
            .iter()
            .any(|name| name == table)
        {
            bail!("maintenance emitted policy for a table outside the benchmark allowlist");
        }
        let policy = public_tuning_value(event.get("config").context("maintenance table config missing")?)
            .context("maintenance table policy is not public")?;
        if !policy["table_policy"].is_object() || !policy["active"].is_boolean() {
            bail!("maintenance table readback omitted resolved table policy");
        }
        verify_maintenance_controls(config, &policy)?;
        if config.tunables.maintenance.as_ref().and_then(|settings| settings.layout_sort_options.as_ref()).is_some() {
            let source = policy.pointer("/layout_sources/arrowcol.layout.sortOptions")
                .and_then(Value::as_str).context("maintenance sort options source missing")?;
            let expected = config.tunables.environment_for(ComponentRole::Maintenance)?
                .remove("ARROWCOL_MAINT_LAYOUT_SORT_OPTIONS")
                .context("maintenance sort options projection missing")?;
            let (actual, requested) = maintenance_readback_value(&policy, "ARROWCOL_MAINT_LAYOUT_SORT_OPTIONS", json!(expected))?;
            let columns = policy.pointer("/table_policy/arrowcol.layout.sortColumns")
                .and_then(Value::as_array).context("maintenance effective sort columns missing")?;
            let options = actual.as_array().context("maintenance effective sort options missing")?;
            if !options.is_empty() && (options.len() != columns.len() || options.iter().zip(columns)
                .any(|(option, column)| option.get("column") != Some(column)))
            {
                bail!("maintenance effective sort options do not match sort columns");
            }
            match source {
                "process" if actual == requested => {},
                "table" => {},
                _ => bail!("maintenance effective sort options do not match their source"),
            }
        }
        verify_effective_maintenance_stop_budget(config, &policy["table_policy"])?;
        if let Some(previous) = tables.insert(table.to_owned(), policy.clone())
            && previous != policy
        {
            bail!("maintenance table policy changed during the measured cell");
        }
    }
    for table in &config.tables {
        if !tables.contains_key(&canonical_dmv_table_name(table)?) {
            bail!("maintenance omitted effective per-table policy, including negative/no-op probes");
        }
    }
    let mut reports = Vec::new();
    let mut dropped = 0_u64;
    let mut operation_count = 0_u64;
    let mut operation_error_count = 0_u64;
    let mut operation_retry_count = 0_u64;
    let mut retry_kind_counts = serde_json::Map::new();
    let mut error_kind_counts = serde_json::Map::new();
    let mut records_complete = text.is_empty() || text.ends_with('\n');
    for line in text.lines().filter(|line| line.starts_with('{')) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            records_complete = false;
            continue;
        };
        if value.get("operation").is_none() {
            continue;
        }
        if !value["operation"].is_string() || !value["applied"].is_boolean() {
            records_complete = false;
            continue;
        }
        let table = value["table"]
            .as_str()
            .context("maintenance operation report omitted its table")?;
        if !tables.contains_key(table) {
            bail!("maintenance operation report is outside the benchmark allowlist");
        }
        let settings = config
            .tunables
            .maintenance
            .as_ref()
            .context("maintenance settings missing")?;
        let operations = serde_json::to_value(&settings.operations)?;
        if value["applied"] != settings.apply.unwrap_or(false)
            || !operations
                .as_array()
                .is_some_and(|operations| operations.contains(&value["operation"]))
        {
            bail!("maintenance operation receipt disagrees with explicit operation/apply authorization");
        }
        operation_count = operation_count.saturating_add(1);
        let record_outcome = maintenance_record_outcome(&value);
        if record_outcome.is_none() {
            records_complete = false;
        }
        let raw_error_kind = value.get("error_kind").and_then(Value::as_str);
        let operation_retry = record_outcome == Some(MaintenanceRecordOutcome::Retry);
        let operation_failed = record_outcome == Some(MaintenanceRecordOutcome::Failed);
        if operation_retry {
            operation_retry_count = operation_retry_count.saturating_add(1);
            retry_kind_counts.insert("stale_plan".to_owned(), json!(operation_retry_count));
        }
        let error_kind = operation_failed.then(|| sanitized_maintenance_error_kind(raw_error_kind));
        if let Some(kind) = &error_kind {
            operation_error_count = operation_error_count.saturating_add(1);
            let count = error_kind_counts.entry(kind.clone()).or_insert_with(|| json!(0));
            *count = json!(count.as_u64().unwrap_or(0).saturating_add(1));
        }
        let mut counts = serde_json::Map::new();
        for field in [
            "files_added",
            "files_removed",
            "bytes_added",
            "bytes_removed",
            "rows_added",
            "rows_removed",
            "files_deleted",
            "files_already_absent",
            "backlog_files",
            "attempts",
            "collisions",
            "retries",
        ] {
            if let Some(count) = value
                .get("report")
                .and_then(|report| report.get(field))
                .and_then(Value::as_u64)
            {
                counts.insert(field.to_owned(), json!(count));
            }
        }
        if reports.len() == 256 {
            reports.remove(0);
            dropped += 1;
        }
        let due_work = maintenance_due_work(&value)?;
        reports.push(json!({
            "table": table, "operation": value["operation"], "negative_probe": value["negative_probe"].as_bool(),
            "maintenance_due_work": due_work,
            "mutation_mode": if value["applied"] == true { "apply_authorized_not_proof_of_mutation" } else { "dry_run_predicted_not_applied" },
            "operation_failed": record_outcome.map(|_| operation_failed), "error_kind": error_kind,
            "operation_retry": record_outcome.map(|_| operation_retry), "retry_kind": operation_retry.then_some("stale_plan"),
            "compaction_debt": maintenance_compaction_debt(&value, operation_failed || operation_retry)?,
            "consumer_reported_counts": counts, "duration_ms": value["duration_ms"].as_u64(),
            "report_outcome": value.pointer("/report/outcome").and_then(Value::as_str),
        }));
    }
    Ok(json!({
        "source":"validated_consumer_per_table_policy", "precedence":"table_properties_override_process_defaults", "tables":tables,
        "operation_reports": reports, "operation_reports_dropped": dropped,
        "operation_outcomes": {
            "scope": "process_lifetime",
            "operation_count": operation_count,
            "status": if records_complete && operation_count > 0 { "observed" } else { "unavailable" },
            "error_count": (records_complete && operation_count > 0).then_some(operation_error_count),
            "error_kind_counts": (records_complete && operation_count > 0).then_some(error_kind_counts),
            "retry_count": (records_complete && operation_count > 0).then_some(operation_retry_count),
            "retry_kind_counts": (records_complete && operation_count > 0).then_some(retry_kind_counts),
            "comment": if operation_error_count == 0 {
                "No maintenance operation errors were observed in the process log."
            } else {
                "Maintenance failures are counted by sanitized error_kind; raw error text is omitted."
            },
        },
        "rewrite_proof_scope": "consumer_operation_receipts_not_independent_physical_file_validation",
        "operation_report_timing_scope": "process_lifetime_including_warmup_and_drain_not_shared_measurement_window",
    }))
}

#[cfg(test)]
mod maintenance_topology_tests {
    use super::*;

    #[test]
    fn maintenance_is_opt_in_allowlisted_dry_run_and_unit_exact() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.platform.kind = crate::config::OatMode::Local;
        assert!(maintenance_runtime(&config)?.is_none());
        config.tunables = serde_json::from_value(json!({"maintenance":{
            "enabled":true,"operations":["optimize"],"interval_secs":131,"max_idle_backoff_secs":144,"target_file_mib":17
        }}))?;
        let args = maintenance_arguments(&config, "http://arrowcol-catalog:8817")?;
        assert!(!args.iter().any(|arg| arg == "--apply" || arg == "--catalog-endpoint"));
        assert!(args.windows(2).any(|pair| pair == ["--root", "file:///tmp/arrowcol-stress/dataflow"]));
        assert!(args.windows(2).any(|pair| pair == ["--max-idle-backoff-secs", "144"]));
        assert!(args.windows(2).any(|pair| pair == ["--interval-secs", "131"]));
        let table_name = canonical_dmv_table_name(&config.tables[0])?;
        assert!(args.windows(2).any(|pair| pair == ["--table", table_name.as_str()]));
        let compose = compose_tunables(&config)?;
        assert_eq!(compose["services"]["delta-maintenance"]["command"], json!(args));
        assert_eq!(
            compose["services"]["delta-maintenance"]["environment"]["ARROWCOL_MAINT_TARGET_FILE_MIB"],
            "17"
        );
        let mut deployment = json!({"metadata":{"name":"delta-maintenance"},"spec":{"template":{"spec":{"containers":[{"name":"maintenance"}]}}}});
        inject_maintenance_arguments(&config, &mut deployment)?;
        assert_eq!(
            deployment.pointer("/spec/template/spec/containers/0/args"),
            Some(&json!(args))
        );
        config.platform.kind = crate::config::OatMode::Aks;
        let aks_args = maintenance_arguments(&config, "http://arrowcol-catalog:8817")?;
        assert!(aks_args.windows(2).any(|pair| pair == ["--catalog-endpoint", "http://arrowcol-catalog:8817"]));
        assert!(!aks_args.iter().any(|arg| arg == "--root"));
        Ok(())
    }

    #[test]
    fn maintenance_due_work_requires_a_complete_operation_result() -> Result<()> {
        let fixture: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/maintenance-operation-results.json"
        ))?;
        let operation_results = fixture["operation_results"]
            .as_array()
            .context("serde fixture omitted operation_results")?;
        assert_eq!(operation_results.len(), 5);
        for result in operation_results {
            let evidence = maintenance_due_work(result)?
                .context("complete serialized operation result lost due-work evidence")?;
            assert_eq!(evidence.completeness, MaintenanceDueWorkCompleteness::Complete);
            assert_eq!(evidence.due, Some(false));
        }
        let optimize = operation_results.iter().find(|result| result["operation"] == "optimize")
            .context("producer fixture omitted OPTIMIZE")?;
        let debt = maintenance_compaction_debt(optimize, false)?.context("producer omitted compaction debt")?;
        assert_eq!(serde_json::to_value(debt)?["protected_partition_backlog"],
            optimize["report"]["compaction_debt"]["protected_partition_backlog"]);

        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tables = vec!["sqlserver_availability_replica_states".to_owned()];
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "maintenance": {"enabled":true, "apply":true,
                "operations":["optimize","purge","vacuum","checkpoint","prune_log"]}
        }))?;
        let table = canonical_dmv_table_name(&config.tables[0])?;
        let mut text = format!("arrowcol_maintenance_table_config {}\n", json!({
            "table": table, "config": {"active":true, "table_policy":{"arrowcol.maintenance.operationTimeout":300_000},
                "startup":{"apply":true,"operations":["optimize","purge","vacuum","checkpoint","prune_log"],"interval_secs":60}}
        }));
        for result in operation_results {
            let mut bound = result.clone();
            bound["table"] = json!(table);
            text.push_str(&format!("{bound}\n"));
        }
        let current = ProcessLog {
            role: ComponentRole::Maintenance, instance_id:"producer-fixture".to_owned(),
            image:None, image_id:None, source_revision:None, complete:true, raw_log_bytes:0, raw_log_lines:0,
            stdout:text.clone(), stderr:String::new(), text:text.clone(),
        };
        let mut baseline = current.clone();
        baseline.stdout.clear();
        let projection = json!({
            "producer_commit":"dbdfbf2b3c0dcaae2db19104b0796933d7fea520",
            "producer_sha256":"b2717115ca191b392f0e5cf7cfb570b0a27b1a8619f3a65aa426832bedcfbee2",
            "binding":"events fixture table renamed to benchmark allowlist; operation/report payloads unchanged",
            "processes":[{"component":"maintenance","log_complete":true,
                "maintenance_table_policies":maintenance_table_evidence(&config, &text)?,
                "maintenance_operation_outcomes":maintenance_cell_outcome_evidence(&current, &[baseline])}],
        });
        let expected_projection: Value = serde_json::from_str(include_str!(
            "../../arrowcol-oat/campaign-results-maintenance-health-e1-projection.json"
        ))?;
        assert_eq!(projection, expected_projection);
        assert_eq!(crate::local::file_digest(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/maintenance-operation-results.json"),
        )?.1, projection["producer_sha256"].as_str().context("producer hash")?);
        let protected = optimize["report"]["compaction_debt"]["protected_partition_backlog"].clone();
        assert!(!protected.is_null(), "fixture must contain measured protected debt, not an invented zero");

        let cases = fixture["qualification_cases"]
            .as_array()
            .context("serde fixture omitted qualification_cases")?;
        assert_eq!(cases.len(), 5);
        for case in cases {
            let result = case.get("result").context("qualification case omitted result")?;
            match case["expected_eligibility"].as_str() {
                Some("complete") => {
                    assert!(maintenance_due_work(result)?.is_some());
                }
                Some("unknown") => assert!(maintenance_due_work(result)?.is_none()),
                other => bail!("unexpected fixture eligibility: {other:?}"),
            }
        }
        Ok(())
    }

    #[test]
    fn maintenance_sort_options_require_startup_and_effective_readback() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tables = vec!["sqlserver_availability_replica_states".to_owned()];
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "maintenance":{"enabled":true,"operations":["optimize"],
                "layout_sort_columns":["customer_id"],
                "layout_sort_options":[{"column":"customer_id","direction":"descending","nulls":"first"}]}
        }))?;
        let mut observed = json!({
            "startup":{"apply":false,"operations":["optimize"],"interval_secs":60},
            "table_policy":{"arrowcol.maintenance.operationTimeout":300_000,
                "arrowcol.layout.sortColumns":["customer_id"],
                "arrowcol.layout.sortOptions":[{"column":"customer_id","direction":"descending","nulls":"first"}]},
            "layout_sources":{"arrowcol.layout.sortOptions":"process"}, "active":true
        });
        verify_process_configuration(&config, ComponentRole::Maintenance, &observed)?;
        let table = canonical_dmv_table_name(&config.tables[0])?;
        let event = |policy: &Value| format!("arrowcol_maintenance_table_config {}\n", json!({"table":table,"config":policy}));
        maintenance_table_evidence(&config, &event(&observed))?;
        observed["table_policy"]["arrowcol.layout.sortOptions"] =
            json!([{"column":"customer_id","direction":"ascending","nulls":"last"}]);
        assert!(verify_process_configuration(&config, ComponentRole::Maintenance, &observed).is_err());
        assert!(maintenance_table_evidence(&config, &event(&observed)).is_err());
        observed["layout_sources"]["arrowcol.layout.sortOptions"] = json!("table");
        maintenance_table_evidence(&config, &event(&observed))?;
        observed["table_policy"]["arrowcol.layout.sortOptions"] = json!([]);
        maintenance_table_evidence(&config, &event(&observed))?;
        observed["table_policy"].as_object_mut().context("policy")?.remove("arrowcol.layout.sortOptions");
        assert!(verify_process_configuration(&config, ComponentRole::Maintenance, &observed).is_err());
        assert!(maintenance_table_evidence(&config, &event(&observed)).is_err());
        Ok(())
    }

    #[test]
    fn maintenance_readback_converts_units_without_truncation_and_preserves_table_overrides() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = serde_json::from_value(
            json!({"maintenance":{"enabled":true,"operations":["optimize"],"target_file_mib":17}}),
        )?;
        let startup = json!({"startup":{"apply":false,"operations":["optimize"],"interval_secs":60},
            "table_policy":{"arrowcol.optimize.targetFileSize":17_u64*1024*1024,
                "arrowcol.maintenance.operationTimeout":300_000},"active":true});
        verify_process_configuration(&config, ComponentRole::Maintenance, &startup)?;
        let (actual, expected) = maintenance_readback_value(&startup, "ARROWCOL_MAINT_TARGET_FILE_MIB", json!(17))?;
        assert_eq!(actual, expected);
        assert!(maintenance_readback_value(&startup, "ARROWCOL_MAINT_TARGET_FILE_MIB", json!(u64::MAX)).is_err());
        let mut text = String::new();
        for table in &config.tables {
            let mut policy = startup.clone();
            policy["table_policy"]["arrowcol.optimize.targetFileSize"] = json!(32_u64 * 1024 * 1024);
            text.push_str(&format!(
                "arrowcol_maintenance_table_config {}\n",
                json!({
                    "table": canonical_dmv_table_name(table)?, "config": policy,
                })
            ));
        }
        let policies = maintenance_table_evidence(&config, &text)?;
        let table = canonical_dmv_table_name(&config.tables[0])?;
        assert_eq!(
            policies["tables"][&table]["table_policy"]["arrowcol.optimize.targetFileSize"],
            32_u64 * 1024 * 1024
        );
        assert!(maintenance_table_evidence(&config, "").is_err());
        let actual: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/maintenance-open-partition-results.json"
        ))?;
        assert_eq!(crate::local::file_digest(&Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/maintenance-open-partition-results.json"))?.1,
            "873a51a750f374a1c2d73aaf6cc0601591cb1f88e3140b720e41f52eea3eaa90");
        assert!(actual["default_configuration"]["startup"]["sealed_partitions_only"].is_null());
        assert_eq!(actual["default_configuration"]["operation_settings"]["sealed_partitions_only"], true);
        assert_eq!(actual["default_configuration"]["table_policy"]["arrowcol.optimize.sealedPartitionsOnly"], true);
        assert!(!role_environment(&config, ComponentRole::Maintenance)?
            .contains_key("ARROWCOL_MAINT_SEALED_PARTITIONS_ONLY"));
        let mut bounded_config = config.clone();
        bounded_config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "maintenance":{"enabled":true,"apply":true,"operations":["optimize"],"interval_secs":1,
                "max_consecutive_stale_plans":4,"sealed_partitions_only":false}
        }))?;
        let mut overridden = actual["opt_in_configuration"].clone();
        verify_process_configuration(&bounded_config, ComponentRole::Maintenance, &overridden)?;
        assert_eq!(overridden["startup"]["sealed_partitions_only"], false);
        assert_eq!(overridden["operation_settings"]["sealed_partitions_only"], false);
        overridden["operation_settings"]["sealed_partitions_only"] = json!(true);
        overridden["table_policy"]["arrowcol.optimize.sealedPartitionsOnly"] = json!(true);
        verify_process_configuration(&bounded_config, ComponentRole::Maintenance, &overridden)?;
        overridden["startup"]["sealed_partitions_only"] = Value::Null;
        assert!(verify_process_configuration(&bounded_config, ComponentRole::Maintenance, &overridden).is_err(),
            "omitted/null is not an observed requested false");
        assert!(
            maintenance_table_evidence(
                &config,
                &format!(
                    "{text}arrowcol_maintenance_table_config {{\"table\":\"other.schema.table\",\"config\":{{}}}}\n"
                )
            )
            .is_err()
        );
        let report = json!({"table":table,"operation":"optimize","applied":false,"negative_probe":false,
            "report":{"outcome":"noop","interrupted":false,
                "files_added":2,"rows_added":257,"outputs":["private-object-path"]},"duration_ms":12});
        let reports = maintenance_table_evidence(&config, &format!("{text}{report}\n"))?;
        assert!(reports["operation_reports"][0]["compaction_debt"].is_null());
        assert!(reports["operation_reports"][0]["maintenance_due_work"].is_null());
        assert_eq!(
            reports["operation_reports"][0]["consumer_reported_counts"]["rows_added"],
            257
        );
        assert_eq!(
            reports["operation_reports"][0]["mutation_mode"],
            "dry_run_predicted_not_applied"
        );
        assert!(!reports.to_string().contains("private-object-path"));
        let due_work = json!({
            "observed_at_ms": 1700000000000_i64,
            "source": "snapshot_active_files",
            "completeness": "complete",
            "unit": "parquet_files",
            "due": false,
            "eligible_count": 0,
            "eligible_bytes": 0,
            "backlog_count": 0,
            "backlog_bytes": 0,
            "oldest_eligible_timestamp_ms": null,
            "oldest_timestamp_basis": null,
            "incomplete_reason": null
        });
        let mut due_report = report.clone();
        due_report["report"]["maintenance_due_work"] = due_work.clone();
        let due = maintenance_table_evidence(&config, &format!("{text}{due_report}\n"))?;
        assert_eq!(due["operation_reports"][0]["maintenance_due_work"], due_work);
        let mut incomplete = due_work.clone();
        incomplete["completeness"] = json!("incomplete");
        incomplete["due"] = Value::Null;
        incomplete["eligible_count"] = Value::Null;
        incomplete["eligible_bytes"] = Value::Null;
        incomplete["backlog_count"] = Value::Null;
        incomplete["backlog_bytes"] = Value::Null;
        incomplete["incomplete_reason"] = json!("active_file_limit");
        incomplete["oldest_eligible_timestamp_ms"] = json!(1699999999000_i64);
        incomplete["oldest_timestamp_basis"] = json!("ingestion_partition_start");
        due_report["report"]["maintenance_due_work"] = incomplete.clone();
        let due = maintenance_table_evidence(&config, &format!("{text}{due_report}\n"))?;
        assert_eq!(due["operation_reports"][0]["maintenance_due_work"]["eligible_count"], Value::Null);
        assert_eq!(due["operation_reports"][0]["maintenance_due_work"]["oldest_eligible_timestamp_ms"], 1699999999000_i64);
        due_report["report"]["maintenance_due_work"]["incomplete_reason"] = Value::Null;
        assert!(maintenance_table_evidence(&config, &format!("{text}{due_report}\n")).is_err());
        due_report["report"]["maintenance_due_work"] = due_work.clone();
        due_report["report"]["maintenance_due_work"]["due"] = json!(true);
        assert!(maintenance_table_evidence(&config, &format!("{text}{due_report}\n")).is_err());
        due_report["report"]["maintenance_due_work"] = due_work.clone();
        due_report["report"]["maintenance_due_work"]["source"] = json!("unknown_source");
        assert!(maintenance_table_evidence(&config, &format!("{text}{due_report}\n")).is_err());
        due_report["report"]["maintenance_due_work"] = due_work.clone();
        due_report["report"]["maintenance_due_work"].as_object_mut().context("due-work fixture is not an object")?
            .remove("eligible_bytes");
        assert!(maintenance_table_evidence(&config, &format!("{text}{due_report}\n")).is_err());
        let mut debt_report = report.clone();
        debt_report["report"]["compaction_debt"] = json!({
            "observed_at_ms": 1700000000000_i64, "source": "snapshot_active_files",
            "eligible_files": 4, "eligible_bytes": 1000, "backlog_files": 2, "backlog_bytes": 500,
            "oldest_eligible_partition_start_ms": null, "protected_partition_backlog": null
        });
        let debt = maintenance_table_evidence(&config, &format!("{text}{debt_report}\n"))?;
        assert_eq!(debt["operation_reports"][0]["compaction_debt"], debt_report["report"]["compaction_debt"]);
        debt_report["error_kind"] = json!("commit_budget_exhausted");
        let debt = maintenance_table_evidence(&config, &format!("{text}{debt_report}\n"))?;
        assert!(debt["operation_reports"][0]["compaction_debt"].is_null());
        debt_report["error_kind"] = Value::Null;
        debt_report["report"]["compaction_debt"]["backlog_files"] = json!(5);
        assert!(maintenance_table_evidence(&config, &format!("{text}{debt_report}\n")).is_err());
        let mut failed = report.clone();
        failed["error_kind"] = json!("commit_budget_exhausted");
        failed["error"] = json!("private operation error");
        let failure_evidence = maintenance_table_evidence(&config, &format!("{text}{failed}\n"))?;
        assert_eq!(failure_evidence["operation_outcomes"]["error_count"], 1);
        assert!(failure_evidence["operation_reports"][0]["maintenance_due_work"].is_null());
        assert_eq!(
            failure_evidence["operation_outcomes"]["error_kind_counts"]["commit_budget_exhausted"],
            1
        );
        assert!(failure_evidence["operation_outcomes"]["comment"].as_str().is_some());
        assert!(!failure_evidence.to_string().contains("private operation error"));
        let mut stale = failed.clone();
        stale["error_kind"] = json!("stale_plan");
        let retry = maintenance_table_evidence(&config, &format!("{text}{stale}\n"))?;
        assert_eq!(retry["operation_outcomes"]["error_count"], 0);
        assert_eq!(retry["operation_outcomes"]["retry_kind_counts"], json!({"stale_plan":1}));
        assert_eq!(retry["operation_reports"][0]["operation_failed"], false);
        assert_eq!(retry["operation_reports"][0]["retry_kind"], "stale_plan");
        assert!(retry["operation_reports"][0]["maintenance_due_work"].is_null());
        assert!(!retry.to_string().contains("private operation error"));
        failed = report.clone();
        failed["applied"] = json!(true);
        assert!(maintenance_table_evidence(&config, &format!("{text}{failed}\n")).is_err());
        config.platform.kind = crate::config::OatMode::Aks;
        let mut aks_failure = report.clone();
        aks_failure["error_kind"] = json!("commit_budget_exhausted");
        let aks_evidence = maintenance_table_evidence(&config, &format!("{text}{aks_failure}\n"))?;
        assert_eq!(
            aks_evidence["operation_outcomes"]["error_kind_counts"]["commit_budget_exhausted"],
            1
        );
        for (variable, expected) in [
            ("CATALOG_RUNTIME_THREADS", json!(3)),
            ("CATALOG_RPC_TIMEOUT_SECS", json!(17)),
            ("CATALOG_MAX_DECODING_MESSAGE_BYTES", json!(1048576)),
            ("INPUT_READ_STRATEGY", json!("spool")),
        ] {
            let mut observed = json!({"startup":{}});
            observed["startup"][variable.to_ascii_lowercase()] = expected.clone();
            let (actual, requested) =
                maintenance_readback_value(&observed, &format!("ARROWCOL_MAINT_{variable}"), expected)?;
            assert_eq!(actual, requested);
        }
        Ok(())
    }

    #[test]
    fn effective_table_timeout_is_budgeted_before_admission() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "maintenance":{"enabled":true,"apply":true,"operations":["optimize"]}
        }))?;
        let table = canonical_dmv_table_name(&config.tables[0])?;
        let event = |timeout_ms| json!({
            "table": table,
            "config": {"active":true,"table_policy":{"arrowcol.maintenance.operationTimeout":timeout_ms},
                "startup":{"apply":true,"operations":["optimize"],"interval_secs":60}}
        });
        let default_timeout = format!("arrowcol_maintenance_table_config {}\n", event(300_000));
        assert!(maintenance_table_evidence(&config, &default_timeout).is_ok(),
            "the resolved registry default must remain eligible when it fits the persisted deadline");
        let oversized_timeout = format!("arrowcol_maintenance_table_config {}\n", event(1_200_000));
        assert!(maintenance_table_evidence(&config, &oversized_timeout).is_err(),
            "a 1,200-second table override must not outlive the 900-second requested/default budget");

        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "maintenance":{"enabled":true,"apply":true,"operations":["optimize"],"operation_timeout_secs":900}
        }))?;
        let safe_override = event(600_000);
        verify_process_configuration(&config, ComponentRole::Maintenance, &safe_override["config"])?;
        let shorter_override = format!("arrowcol_maintenance_table_config {}\n", safe_override);
        assert!(maintenance_table_evidence(&config, &shorter_override).is_ok(),
            "a safe table override may differ from the requested process default");
        Ok(())
    }

    #[test]
    fn maintenance_upload_readback_requires_actual_configured_values() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "maintenance": {"enabled": true, "operations": ["optimize"],
                "optimize_upload_mode": "buffered_multipart", "optimize_upload_buffer_bytes": 4194304, "optimize_upload_concurrency": 3,
                "bin_concurrency": 2, "max_concurrent_decoded_bytes": 2147483648u64, "input_prefetch_files": 2}
        }))?;
        let mut observed = json!({"startup": {
            "apply": false, "operations": ["optimize"], "interval_secs": 60,
            "bin_concurrency": 2, "max_concurrent_decoded_bytes": 2147483648u64, "input_prefetch_files": 2,
            "operation_limits": {"optimize_upload_mode": "buffered_multipart", "optimize_upload_buffer_bytes": 4194304, "optimize_upload_concurrency": 3}
        }});
        verify_process_configuration(&config, ComponentRole::Maintenance, &observed)?;
        observed["startup"]["operation_limits"]["optimize_upload_buffer_bytes"] = Value::Null;
        assert!(verify_process_configuration(&config, ComponentRole::Maintenance, &observed).is_err());
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires L2 delta-maintenance binary"]
    async fn maintenance_upload_json_reaches_real_consumer_readback() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "parquet_writer": {"defaults": {"compression": "zstd"}},
            "maintenance": {"enabled": true, "operations": ["optimize"],
                "optimize_upload_mode": "buffered_multipart", "optimize_upload_buffer_bytes": 4194304,
                "max_consecutive_stale_plans": 4, "sealed_partitions_only": false,
                "parquet_writer": {"defaults": {"compression": "snappy"}}}
        }))?;
        let binary = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/delta-maintenance");
        let mut command = Command::new(binary);
        command.env_clear().envs(role_environment(&config, ComponentRole::Maintenance)?)
            .args(["--root", "file:///tmp/oat-e1-nonexistent-table-root", "--table", "fixture",
                "--operation", "optimize", "--interval-secs", "60", "--max-cycles", "1",
                "--no-metrics-listener"])
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(30), command.output()).await??;
        let stderr = String::from_utf8(output.stderr)?;
        let observed = process_configurations(&stderr)?;
        verify_process_configuration(&config, ComponentRole::Maintenance, &observed)?;
        assert_eq!(observed["parquet_writer_overrides"]["defaults"]["compression"], "snappy");
        assert_eq!(observed["startup"]["operation_limits"]["optimize_upload_mode"], "buffered_multipart");
        assert_eq!(observed["startup"]["operation_limits"]["optimize_upload_buffer_bytes"], 4194304);
        assert_eq!(observed["startup"]["max_consecutive_stale_plans"], 4);
        assert_eq!(observed["operation_settings"]["max_consecutive_stale_plans"], 4);
        assert_eq!(observed["startup"]["sealed_partitions_only"], false);
        assert_eq!(observed["operation_settings"]["sealed_partitions_only"], false);
        assert_eq!(observed["table_policy"]["arrowcol.optimize.sealedPartitionsOnly"], false);
        Ok(())
    }
}
