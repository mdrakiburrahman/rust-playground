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
    })))
}

fn maintenance_arguments(config: &OatConfig, catalog_endpoint: &str) -> Result<Vec<String>> {
    let policy = maintenance_runtime(config)?.context("maintenance is disabled")?;
    let interval = config
        .tunables
        .maintenance
        .as_ref()
        .and_then(|settings| settings.interval_secs)
        .unwrap_or(60);
    let mut args = vec![
        "--catalog-endpoint".to_owned(),
        catalog_endpoint.to_owned(),
        "--interval-secs".to_owned(),
        interval.to_string(),
        "--listen".to_owned(),
        "0.0.0.0:9467".to_owned(),
    ];
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
        | "MAX_CONCURRENT_TABLES"
        | "MAX_CONCURRENT_DECODED_BYTES"
        | "COMMIT_ATTEMPTS"
        | "COMMIT_BUDGET_SECS"
        | "OUTPUT_ROW_GROUP_ROWS"
        | "MAX_PRUNE_FILES"
        | "ANCHOR_MS" => (format!("/startup/{}", field.to_ascii_lowercase()), 1, false),
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
    Ok((
        observed
            .pointer(&path)
            .with_context(|| format!("maintenance startup omitted {variable}"))?
            .clone(),
        expected,
    ))
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
    Ok(())
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
        if let Some(previous) = tables.insert(table.to_owned(), policy.clone()) {
            if previous != policy {
                bail!("maintenance table policy changed during the measured cell");
            }
        }
    }
    for table in &config.tables {
        if !tables.contains_key(&canonical_dmv_table_name(table)?) {
            bail!("maintenance omitted effective per-table policy, including negative/no-op probes");
        }
    }
    Ok(
        json!({ "source":"validated_consumer_per_table_policy", "precedence":"table_properties_override_process_defaults", "tables":tables }),
    )
}

#[cfg(test)]
mod maintenance_topology_tests {
    use super::*;

    #[test]
    fn maintenance_is_opt_in_allowlisted_dry_run_and_unit_exact() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        assert!(maintenance_runtime(&config)?.is_none());
        config.tunables = serde_json::from_value(json!({"maintenance":{
            "enabled":true,"operations":["optimize"],"interval_secs":131,"target_file_mib":17
        }}))?;
        let args = maintenance_arguments(&config, "http://arrowcol-catalog:8817")?;
        assert!(!args.iter().any(|arg| arg == "--apply" || arg == "--root"));
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
            "table_policy":{"arrowcol.optimize.targetFileSize":17_u64*1024*1024},"active":true});
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
        assert!(
            maintenance_table_evidence(
                &config,
                &format!(
                    "{text}arrowcol_maintenance_table_config {{\"table\":\"other.schema.table\",\"config\":{{}}}}\n"
                )
            )
            .is_err()
        );
        Ok(())
    }
}
