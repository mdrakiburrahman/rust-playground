use arrowcol_tunables::ComponentRole;
use otel_arrow_dfe_config::engine::EngineConfig;

fn capability_evidence() -> Vec<Value> {
    arrowcol_tunables::capability_table()
        .iter()
        .map(|capability| {
            json!({
                "name": capability.name,
                "default": capability.default,
                "boundary": match capability.boundary {
                    arrowcol_tunables::ApplyBoundary::PerCellRuntime => "per_cell_runtime",
                    arrowcol_tunables::ApplyBoundary::ProcessStartup => "process_startup",
                    arrowcol_tunables::ApplyBoundary::RebuildOnly => "rebuild_only",
                },
            })
        })
        .collect()
}

fn role_environment(config: &OatConfig, role: ComponentRole) -> Result<std::collections::BTreeMap<String, String>> {
    config.tunables.validate_for_oat()?;
    if matches!(role, ComponentRole::Collector)
        && config.tunables.logging.as_ref().and_then(|logging| logging.collector).is_some()
    {
        let pipeline = if config.tunables.wal.is_some() {
            include_str!("../../arrowcol-dataflow-rust/config.quiver.yaml")
        } else {
            include_str!("../../arrowcol-dataflow-rust/config.yaml")
        };
        validate_collector_pipeline_log_level(pipeline)?;
    }
    config.tunables.environment_for(role)
}

fn validate_collector_pipeline_log_level(pipeline_yaml: &str) -> Result<()> {
    let pipeline: serde_yaml::Value = serde_yaml::from_str(pipeline_yaml)
        .context("parse rendered collector pipeline for logging precedence")?;
    let engine = pipeline
        .get("engine")
        .cloned()
        .unwrap_or_else(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    let engine: EngineConfig = serde_yaml::from_value(engine)
        .context("parse collector engine settings for logging precedence")?;
    if engine.telemetry.logs.level.is_some() {
        bail!("logging.collector cannot override engine.telemetry.logs.level in the rendered pipeline");
    }
    Ok(())
}

fn compose_tunables(config: &OatConfig) -> Result<Value> {
    let mut services = serde_json::Map::new();
    for (service, role) in [
        ("oat-client", ComponentRole::Client),
        ("router", ComponentRole::Router),
        ("dataflow-1", ComponentRole::Collector),
    ] {
        let mut environment = role_environment(config, role)?;
        if matches!(role, ComponentRole::Collector) {
            if config.platform.kind == crate::config::OatMode::Local
                && config.campaign_profile == Some(crate::config::CampaignProfile::SingleHotTable)
                && let Some(granularity) = config.tunables.maintenance.as_ref().and_then(|settings| settings.granularity)
            {
                let value = serde_json::to_value(granularity)?;
                environment.insert(
                    "ARROWCOL_PARTITION_GRANULARITY".to_owned(),
                    value.as_str().context("invalid typed partition granularity")?.to_owned(),
                );
            }
            for &(legacy, canonical, _) in arrow_deltalake::delta::PROCESS_WRITER_PROPERTY_ENVIRONMENT_ALIASES {
                if let Some(value) = environment.get(canonical).cloned() {
                    environment.insert(legacy.to_owned(), value);
                }
            }
        }
        if !environment.is_empty() {
            services.insert(service.to_owned(), json!({ "environment": environment }));
        }
    }
    compose_maintenance(config, &mut services)?;
    Ok(json!({ "services": services }))
}

fn local_dataflow_overlay(config: &OatConfig, state_dir: &Path, overlay: &mut Value) -> Result<()> {
    let services = overlay["services"]
        .as_object_mut()
        .context("Compose tunable services missing")?;
    let collector = services.entry("dataflow-1").or_insert_with(|| json!({}));
    collector["stop_grace_period"] = json!(format!("{}s", config.collector_stop_grace_seconds));
    collector["ports"] = json!([{
        "host_ip": "127.0.0.1",
        "target": 32011,
        "published": "0",
        "protocol": "tcp"
    }]);
    collector["entrypoint"] = json!(["/bin/sh", "-ec"]);
    collector["command"] = json!([concat!(
        "export ARROWCOL_INSTANCE_KEY=\"oat-$(hostname)\"; ",
        "export ARROWCOL_ADVERTISED_FLIGHT_ENDPOINT=\"$(hostname -i):32010\"; ",
        "export ARROWCOL_QUIVER_PATH=\"/oat-quiver/$(hostname)\"; ",
        "exec /usr/bin/tini -- /usr/local/bin/arrowcol-dataflow-rust --num-cores \"${LOADTEST_CPUS:-2}\" --config /etc/oat/dataflow.yaml"
    )]);
    let mut volumes = vec![json!({"type":"bind", "source":state_dir.join("dataflow.yaml"),
        "target":"/etc/oat/dataflow.yaml", "read_only":true, "bind":{"create_host_path":false}})];
    if config.tunables.wal.is_some() {
        volumes.push(json!({"type":"bind", "source":state_dir.join("quiver"),
            "target":"/oat-quiver", "bind":{"create_host_path":false}}));
    }
    collector["volumes"] = json!(volumes);
    Ok(())
}

fn inject_container_environment(
    container: &mut Value,
    variables: &std::collections::BTreeMap<String, String>,
) -> Result<()> {
    if variables.is_empty() {
        return Ok(());
    }
    let container = container
        .as_object_mut()
        .context("workload container must be an object")?;
    let environment = container
        .entry("env")
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .context("workload container env must be an array")?;
    for (name, value) in variables {
        let mut existing = environment
            .iter_mut()
            .filter(|entry| entry["name"].as_str() == Some(name));
        if let Some(entry) = existing.next() {
            *entry = json!({ "name": name, "value": value });
            if existing.next().is_some() {
                bail!("duplicate tunable environment name {name}");
            }
        } else {
            environment.push(json!({ "name": name, "value": value }));
        }
    }
    Ok(())
}

fn inject_deployment_environment(
    deployment: &mut Value,
    name: &str,
    variables: &std::collections::BTreeMap<String, String>,
) -> Result<()> {
    if variables.is_empty() {
        return Ok(());
    }
    let containers = deployment
        .pointer_mut("/spec/template/spec/containers")
        .and_then(Value::as_array_mut)
        .context("workload Deployment has no containers")?;
    let mut selected = containers
        .iter_mut()
        .filter(|container| container["name"].as_str() == Some(name));
    let container = selected.next().context("tunable target container does not exist")?;
    inject_container_environment(container, variables)?;
    if selected.next().is_some() {
        bail!("tunable target container is ambiguous");
    }
    Ok(())
}

fn inject_workload_environment(config: &OatConfig, resources: &mut [Value]) -> Result<()> {
    for resource in resources {
        if resource["kind"].as_str() != Some("Deployment") {
            continue;
        }
        inject_maintenance_arguments(config, resource)?;
        let role_and_container = match resource.pointer("/metadata/name").and_then(Value::as_str) {
            Some("arrowcol-router") => Some((ComponentRole::Router, "router")),
            Some(name) if name == config.dfe_deployment => {
                Some((ComponentRole::Collector, config.dfe_container.as_str()))
            }
            Some("delta-maintenance") => Some((ComponentRole::Maintenance, "maintenance")),
            _ => None,
        };
        if let Some((role, container)) = role_and_container {
            inject_deployment_environment(resource, container, &role_environment(config, role)?)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tunable_injection_tests {
    use super::*;

    #[test]
    fn local_campaign_granularity_reaches_both_consumers_and_gates_admission() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.campaign_profile = Some(crate::config::CampaignProfile::SingleHotTable);
        config.tunables = serde_json::from_value(json!({
            "maintenance":{"enabled":true,"operations":["purge"],"granularity":"hour"}
        }))?;
        let overlay = compose_tunables(&config)?;
        let collector_env = overlay.pointer("/services/dataflow-1/environment")
            .and_then(Value::as_object).context("collector environment missing")?
            .iter().map(|(key, value)| Ok((key.clone(), value.as_str().context("not a string")?.to_owned())))
            .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
        let writer = arrow_deltalake::delta::resolve_process_writer_properties_from_environment(&collector_env)?;
        assert_eq!(writer.get("arrowcol.partition.granularity").map(String::as_str), Some("hour"));
        assert_eq!(overlay.pointer("/services/delta-maintenance/environment/ARROWCOL_MAINT_GRANULARITY"), Some(&json!("hour")));
        let table = canonical_dmv_table_name(&config.tables[0])?;
        let collector = json!({"Config":{"Env":["ARROWCOL_PARTITION_GRANULARITY=hour"]}});
        let event = |granularity: &str| format!("arrowcol_maintenance_table_config {}\n", json!({
            "table": table,
            "config": {"active":true,"table_policy":{"arrowcol.partition.granularity":granularity}}
        }));
        assert!(local_partition_policy_evidence("hour", &table, &collector, "")?.is_none());
        let evidence = local_partition_policy_evidence("hour", &table, &collector, &event("hour"))?
            .context("missing evidence")?;
        assert_eq!(evidence["maintenance_table_effective"], "hour");
        assert!(local_partition_policy_evidence("hour", &table, &collector, &event("minute")).is_err());
        let mut properties = std::collections::BTreeMap::new();
        assert_eq!(validate_local_table_partition_property(&properties, "hour")?, None);
        properties.insert("arrowcol.partition.granularity".to_owned(), "hour".to_owned());
        assert_eq!(validate_local_table_partition_property(&properties, "hour")?, Some("hour"));
        properties.insert("arrowcol.partition.granularity".to_owned(), "minute".to_owned());
        assert!(validate_local_table_partition_property(&properties, "hour").is_err());
        assert!(local_partition_policy_evidence(
            "hour", &table, &json!({"Config":{"Env":["ARROWCOL_PARTITION_GRANULARITY=minute"]}}), &event("hour")
        ).is_err());
        assert!(serde_json::from_value::<arrowcol_tunables::Tunables>(
            json!({"maintenance":{"granularity":"hours"}})
        ).is_err());
        config.campaign_profile = None;
        let noncampaign = compose_tunables(&config)?;
        assert!(noncampaign.pointer("/services/dataflow-1/environment/ARROWCOL_PARTITION_GRANULARITY").is_none());
        config.campaign_profile = Some(crate::config::CampaignProfile::SingleHotTable);
        config.tunables.maintenance.as_mut().context("maintenance missing")?.granularity = None;
        let unset = compose_tunables(&config)?;
        assert!(unset.pointer("/services/dataflow-1/environment/ARROWCOL_PARTITION_GRANULARITY").is_none());
        Ok(())
    }

    #[test]
    #[ignore = "requires built delta-maintenance binary"]
    fn local_campaign_granularity_reaches_real_maintenance_consumer() -> Result<()> {
        let root = std::env::temp_dir().join(format!("oat-partition-policy-{}", uuid::Uuid::new_v4()));
        struct TempRoot(PathBuf);
        impl Drop for TempRoot {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        std::fs::create_dir(&root)?;
        let root = TempRoot(root);
        let table = "telemetry.sqlserver.sqlserver_availability_replica_states";
        arrowcol_loadgen::initialize::initialize_local(arrowcol_loadgen::cli::InitializeLocalArgs {
            partition_by_ingestion_date: true, table: table.to_owned(), dataflow_root: root.0.clone(),
        })?;
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.campaign_profile = Some(crate::config::CampaignProfile::SingleHotTable);
        config.tables = vec!["sqlserver_availability_replica_states".to_owned()];
        config.tunables = serde_json::from_value(json!({
            "maintenance":{"enabled":true,"operations":["purge"],"granularity":"hour",
                "purge_retention_secs":2592000}
        }))?;
        let overlay = compose_tunables(&config)?;
        let environment = overlay.pointer("/services/dataflow-1/environment")
            .and_then(Value::as_object).context("collector environment missing")?;
        let env = environment.iter().map(|(key, value)|
            Ok(format!("{key}={}", value.as_str().context("environment value is not a string")?)))
            .collect::<Result<Vec<_>>>()?;
        let collector = json!({"Config":{"Env": env}});
        let url = url::Url::from_directory_path(&root.0).map_err(|()| anyhow::anyhow!("invalid root"))?;
        let binary = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/delta-maintenance");
        let output = std::process::Command::new(binary).env_clear()
            .envs(role_environment(&config, ComponentRole::Maintenance)?)
            .args(["--root", url.as_str(), "--table", table, "--operation", "purge",
                "--interval-secs", "60", "--max-cycles", "1", "--no-metrics-listener"])
            .output()?;
        let stderr = String::from_utf8(output.stderr)?;
        if !output.status.success() { bail!("maintenance fixture failed: {stderr}"); }
        verify_process_configuration(&config, ComponentRole::Maintenance, &process_configurations(&stderr)?)?;
        let evidence = local_partition_policy_evidence("hour", table, &collector, &stderr)?
            .context("real maintenance consumer omitted effective table policy")?;
        assert_eq!(evidence["maintenance_table_effective"], "hour");
        let metadata_url = url::Url::from_directory_path(root.0.join(
            "telemetry/sqlserver/sqlserver_availability_replica_states"
        )).map_err(|()| anyhow::anyhow!("invalid table"))?;
        let properties = arrow_deltalake::delta::DeltaTable::open_for_maintenance_probe(&metadata_url)?
            .maintenance_properties()?;
        assert!(!properties.contains_key("arrowcol.partition.granularity"));
        Ok(())
    }

    #[test]
    fn collector_upload_json_reaches_runtime() -> Result<()> {
        if std::env::var_os("OAT_UPLOAD_RUNTIME_CHILD").is_some() {
            let actual = serde_json::to_value(arrow_deltalake::DeltaRuntimeConfig::from_env()?)?;
            let expected: Value = serde_json::from_str(&std::env::var("OAT_UPLOAD_RUNTIME_EXPECTED")?)?;
            for key in ["upload_mode", "upload_buffer_capacity_bytes", "upload_request_concurrency", "upload_limit"] {
                assert_eq!(actual[key], expected[key], "{key}");
            }
            return Ok(());
        }
        for (tunables, expected) in [
            (json!({"collector":{"runtime":{"upload_limit":1},"upload":{"mode":"buffered_multipart","buffer_capacity_bytes":1048576,"request_concurrency":16}}}),
             json!({"upload_mode":"buffered_multipart","upload_buffer_capacity_bytes":1048576,"upload_request_concurrency":16,"upload_limit":1})),
            (json!({"collector":{"runtime":{"upload_limit":256},"upload":{"mode":"put_create"}}}),
             json!({"upload_mode":"put_create","upload_buffer_capacity_bytes":8388608,"upload_request_concurrency":2,"upload_limit":256})),
        ] {
            let settings = arrowcol_tunables::Tunables::parse_oat_json(tunables)?;
            let environment = settings.environment_for(ComponentRole::Collector)?;
            let output = std::process::Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("runner::tunable_injection_tests::collector_upload_json_reaches_runtime")
                .arg("--nocapture")
                .env_clear()
                .env("OAT_UPLOAD_RUNTIME_CHILD", "1")
                .env("OAT_UPLOAD_RUNTIME_EXPECTED", expected.to_string())
                .envs(environment)
                .output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            anyhow::ensure!(
                output.status.success() && stdout.contains("1 passed;"),
                "collector upload runtime rehearsal failed: {stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(())
    }

    fn config() -> Result<OatConfig> {
        Ok(serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?)
    }

    #[test]
    fn collector_logging_rejects_an_explicit_pipeline_engine_level() -> Result<()> {
        validate_collector_pipeline_log_level("engine: {}\n")?;
        let error = validate_collector_pipeline_log_level("engine:\n  telemetry:\n    logs:\n      level: debug\n");
        assert!(error.is_err());
        let mut config = config()?;
        config.tunables.logging = Some(arrowcol_tunables::LoggingTunables {
            collector: Some(arrowcol_tunables::LogLevel::Debug),
            ..Default::default()
        });
        assert!(role_environment(&config, ComponentRole::Collector).is_ok());
        Ok(())
    }

    #[test]
    fn typed_nondefault_client_value_has_identical_compose_and_aks_injection() -> Result<()> {
        let mut config = config()?;
        config.tunables = serde_json::from_value(json!({ "client": { "batch_rows": 257 } }))?;
        let compose = compose_tunables(&config)?;
        assert_eq!(
            compose["services"]["oat-client"]["environment"]["ARROWCOL_CLIENT_BATCH_ROWS"],
            "257"
        );
        assert_eq!(compose["services"]["router"]["environment"]["RUST_LOG"], "warn,tiberius=error");
        let mut client = json!({ "env": [{ "name": "ARROWCOL_OAT_RUN_ID", "value": "owned" }] });
        inject_container_environment(&mut client, &role_environment(&config, ComponentRole::Client)?)?;
        assert_eq!(client["env"][1]["value"], "257");
        assert_eq!(client["env"][0]["value"], "owned");
        Ok(())
    }

    #[test]
    fn dmv_transport_defaults_remain_valid_through_compose_and_aks_projection() -> Result<()> {
        let mut config = config()?;
        config.tunables = serde_json::from_value(json!({"transport":{"max_message_bytes":4_194_304}}))?;
        config.validate()?;
        assert!(config.tunables.validate().is_err());
        let environment = role_environment(&config, ComponentRole::Client)?;
        assert!(environment.values().any(|value| value == "4194304"));
        let compose = compose_tunables(&config)?;
        assert_eq!(compose["services"]["oat-client"]["environment"], json!(environment));
        let mut client = json!({"env":[]});
        inject_container_environment(&mut client, &environment)?;
        let expected = environment
            .iter()
            .map(|(name, value)| json!({"name":name,"value":value}))
            .collect::<Vec<_>>();
        assert_eq!(client["env"], json!(expected));
        Ok(())
    }

    #[test]
    fn local_overlay_resolves_all_oat_layout_aliases_without_conflicts() -> Result<()> {
        let mut config = config()?;
        config.tunables = serde_json::from_value(json!({
            "collector": {
                "layout": {
                    "enabled": true,
                    "sort_columns": ["session_id"],
                    "bloom_columns": ["database_name"],
                    "max_rows": 4096,
                    "max_input_bytes": 16777216,
                    "row_group_rows": 1024,
                    "page_rows": 256
                }
            }
        }))?;
        let mut overlay = compose_tunables(&config)?;
        local_dataflow_overlay(&config, Path::new("/owned/run"), &mut overlay)?;
        assert_eq!(
            overlay["services"]["dataflow-1"]["ports"],
            json!([{"host_ip":"127.0.0.1","target":32011,"published":"0","protocol":"tcp"}])
        );
        let environment = overlay["services"]["dataflow-1"]["environment"]
            .as_object()
            .context("local collector environment must be an object")?
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.clone(),
                    value.as_str()
                        .context("local collector environment values must be strings")?
                        .to_owned(),
                ))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
        let effective =
            arrow_deltalake::delta::resolve_process_writer_properties_from_environment(&environment)?;
        let aliases = arrow_deltalake::delta::PROCESS_WRITER_PROPERTY_ENVIRONMENT_ALIASES;
        assert_eq!(
            aliases.iter().filter(|(_, canonical, _)| environment.contains_key(*canonical)).count(),
            aliases.len(),
            "the test configuration should set every aliased layout knob"
        );
        for &(legacy, canonical, property) in aliases {
            let value = environment.get(canonical).context("missing canonical OAT layout value")?;
            assert_eq!(environment.get(legacy), Some(value));
            assert_eq!(effective.get(property), Some(value));
        }
        assert_eq!(effective["arrowcol.layout.rowGroupRows"], "1024");
        assert_eq!(effective["arrowcol.layout.pageRows"], "256");
        Ok(())
    }

    #[test]
    fn injection_preserves_identity_and_refuses_ambiguous_targets() -> Result<()> {
        let variables = std::collections::BTreeMap::from([("ARROWCOL_CLIENT_BATCH_ROWS".to_owned(), "257".to_owned())]);
        let mut workload = json!({"spec":{"template":{"spec":{"containers":[{"name":"dataflow","env":[
            {"name":"AZURE_CLIENT_ID","valueFrom":{"secretKeyRef":{"name":"identity","key":"client"}}},
            {"name":"ARROWCOL_CLIENT_BATCH_ROWS","value":"1"}
        ]}]}}}});
        inject_deployment_environment(&mut workload, "dataflow", &variables)?;
        assert!(
            workload
                .pointer("/spec/template/spec/containers/0/env/0/valueFrom")
                .is_some()
        );
        assert_eq!(
            workload
                .pointer("/spec/template/spec/containers/0/env/1/value")
                .and_then(Value::as_str),
            Some("257")
        );
        assert!(inject_deployment_environment(&mut workload, "missing", &variables).is_err());
        Ok(())
    }

    #[test]
    fn empty_layout_json_reaches_resolved_policy() -> Result<()> {
        if std::env::var_os("OAT_R1_CHILD").is_some() {
            let config = arrow_deltalake::delta::effective_layout_startup_config()?;
            assert_eq!(config["process_policy"]["enabled"], true);
            assert_eq!(config["process_policy"]["sort_columns"], json!([]));
            assert_eq!(config["process_policy"]["bloom_columns"], json!([]));
            return Ok(());
        }
        let tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector": {"layout": {"enabled": true, "sort_columns": [], "bloom_columns": []}}
        }))?;
        let environment = tunables.environment_for(ComponentRole::Collector)?;
        assert_eq!(environment["ARROWCOL_COLLECTOR_SORT_COLUMNS"], "");
        assert_eq!(environment["ARROWCOL_COLLECTOR_BLOOM_COLUMNS"], "");
        let mut command = std::process::Command::new(std::env::current_exe()?);
        for (legacy, canonical, _) in arrow_deltalake::delta::PROCESS_WRITER_PROPERTY_ENVIRONMENT_ALIASES {
            command.env_remove(legacy).env_remove(canonical);
        }
        let status = command
            .envs(environment)
            .env("OAT_R1_CHILD", "1")
            .args([
                "--exact",
                "runner::tunable_injection_tests::empty_layout_json_reaches_resolved_policy",
                "--nocapture",
            ])
            .status()?;
        assert!(status.success());
        Ok(())
    }

    #[test]
    fn local_quiver_storage_is_run_owned_and_partitioned_by_process_instance() -> Result<()> {
        let mut config = config()?;
        let state = Path::new("/owned/run");
        let mut overlay = compose_tunables(&config)?;
        local_dataflow_overlay(&config, state, &mut overlay)?;
        assert_eq!(
            overlay["services"]["dataflow-1"]["volumes"]
                .as_array()
                .context("missing mounts")?
                .len(),
            1
        );
        config.tunables = serde_json::from_value(json!({"wal":{"flush_interval_ms":7}}))?;
        let mut overlay = compose_tunables(&config)?;
        local_dataflow_overlay(&config, state, &mut overlay)?;
        let collector = &overlay["services"]["dataflow-1"];
        assert_eq!(collector["environment"]["ARROWCOL_WAL_FLUSH_INTERVAL"], "7ms");
        assert_eq!(collector["volumes"][1]["source"], "/owned/run/quiver");
        assert_eq!(collector["volumes"][1]["bind"]["create_host_path"], false);
        assert!(
            collector["command"][0]
                .as_str()
                .context("missing command")?
                .contains("/oat-quiver/$(hostname)")
        );
        Ok(())
    }
}
