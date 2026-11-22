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
    let mut engine = pipeline
        .get("engine")
        .cloned()
        .unwrap_or_else(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    // The DFE expands the drain setting's environment placeholder before loading the profile.
    if let Some(mapping) = engine.as_mapping_mut() {
        mapping.remove("shutdown_drain_timeout_seconds");
    }
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
    if let Some(bytes) = config.platform.local.collector_memory_limit_bytes {
        collector["mem_limit"] = json!(bytes);
    }
    collector["stop_grace_period"] = json!(format!("{}s", config.collector_stop_grace_seconds));
    collector["ports"] = json!([{
        "host_ip": config.platform.local.bind_host.map_or_else(|| "127.0.0.1".to_owned(), |host| host.to_string()),
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
            let variables = role_environment(config, role)?;
            if role == ComponentRole::Collector
                && config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable)
            {
                validate_collector_exporter_override(resource, container, &variables)?;
            }
            inject_deployment_environment(resource, container, &variables)?;
        }
    }
    Ok(())
}

fn validate_collector_exporter_override(
    deployment: &Value,
    container_name: &str,
    variables: &std::collections::BTreeMap<String, String>,
) -> Result<()> {
    let containers = deployment
        .pointer("/spec/template/spec/containers")
        .and_then(Value::as_array)
        .context("collector Deployment has no containers")?;
    let mut selected = containers.iter().filter(|item| item["name"].as_str() == Some(container_name));
    let container = selected.next().context("collector container is missing")?;
    if selected.next().is_some() {
        bail!("collector container is ambiguous");
    }
    let entries = container["env"].as_array().context("collector environment is missing")?;
    let mut entries = entries
        .iter()
        .filter(|entry| entry["name"].as_str() == Some("ARROWCOL_COLLECTOR_EXPORTER_CONFIG"));
    let entry = entries.next();
    if entries.next().is_some() {
        bail!("rendered collector exporter override is duplicated");
    }
    let Some(entry) = entry else {
        return Ok(());
    };
    let value = entry["value"].as_str().context("collector exporter override must have a literal value")?;
    let rendered: arrowcol_tunables::ExporterTunables =
        serde_json::from_str(value).context("invalid rendered collector exporter override")?;
    let name = "ARROWCOL_COLLECTOR_EXPORTER_CONFIG";
    let requested: arrowcol_tunables::ExporterTunables = serde_json::from_str(
        variables.get(name).context("unrequested collector exporter override")?,
    ).context("invalid collector exporter tunable override")?;
    if serde_json::to_value(rendered)? != serde_json::to_value(requested)? {
        bail!("collector exporter override differs from requested tunables");
    }
    Ok(())
}

#[cfg(test)]
mod tunable_injection_tests {
    use super::*;

    #[test]
    fn rust_runtime_exporter_config_matches_collector_parser_without_override() -> Result<()> {
        runtime_exporter_config_matches_collector_parser(None)
    }

    #[test]
    fn rust_runtime_exporter_config_matches_collector_parser_with_override() -> Result<()> {
        runtime_exporter_config_matches_collector_parser(Some("10s"))
    }

    fn runtime_exporter_config_matches_collector_parser(timeout: Option<&str>) -> Result<()> {
        let mut config = config()?;
        config.campaign_profile = Some(crate::config::CampaignProfile::ScaleOutSingleTable);
        config.tables = vec!["sqlserver_availability_replica_states".to_owned()];
        config.dfe_deployment = "arrowcol".to_owned();
        config.dfe_container = "dataflow".to_owned();
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector":timeout.map(|value| json!({"exporter":{"authority_timeout":value}})).unwrap_or_else(|| json!({})),
            "maintenance":{"enabled":true,"apply":true,
                "operations":["purge","optimize","vacuum","checkpoint","prune_log"]}
        }))?;
        let mut runtime = json!({
            "namespace":"arrowcol-oat", "oatRunId":"00000000-0000-0000-0000-000000000003",
            "workloadIdentityClientId":"00000000-0000-0000-0000-000000000001",
            "subscriptionId":"00000000-0000-0000-0000-000000000002", "stamp":"89152",
            "deltaRoot":"abfss://onelake@arrowcollector89152.dfs.core.windows.net/dataflow",
            "effective_stop_budgets":{"maintenance_grace_seconds":4800,"maintenance_stop_timeout_ms":5100000},
            "images": (["sqlserver","catalog","router","dataflow","maintenance"].into_iter()
                .map(|role| (role.to_owned(), format!("arrowcollector89152.azurecr.io/{role}@sha256:{}", "a".repeat(64))))
                .collect::<std::collections::BTreeMap<_,_>>()),
            "quiver":false,
            "maintenance":{"tables":["telemetry.sqlserver.sqlserver_availability_replica_states"],
                "operations":["optimize","checkpoint","purge","vacuum","prune-log"],"apply":true}
        });
        project_scale_out_runtime(&mut runtime, &config, 1)?;
        let script = r#"
import { readFileSync } from 'node:fs';
import { renderRuntime } from './projects/arrowcol-oat/runtime.ts';
const input = JSON.parse(readFileSync(0, 'utf8'));
process.stdout.write(JSON.stringify(await renderRuntime(input, 'workload')));
"#;
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut child = std::process::Command::new("node")
            .current_dir(root)
            .args(["--import", "tsx", "--input-type=module", "-e", script])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("start actual TypeScript runtime renderer")?;
        child.stdin.take().context("renderer stdin missing")?.write_all(&serde_json::to_vec(&runtime)?)?;
        let output = child.wait_with_output().context("wait for runtime renderer")?;
        if !output.status.success() {
            bail!("runtime renderer failed: {}", String::from_utf8_lossy(&output.stderr));
        }
        let mut resources: Vec<Value> = serde_json::from_slice(&output.stdout)?;
        assert_exporter_configs(&resources, None)?;
        let rendered_collector = resources.iter().find(|resource| resource["kind"] == "Deployment"
            && resource["metadata"]["name"] == config.dfe_deployment).context("collector Deployment missing")?.clone();
        inject_workload_environment(&config, &mut resources)?;
        assert_exporter_configs(&resources, timeout)?;
        let collector = resources.iter_mut().find(|resource| resource["kind"] == "Deployment"
            && resource["metadata"]["name"] == config.dfe_deployment).context("collector Deployment missing")?;
        configure_collector_deployment(&config, collector, 1)?;
        assert_exporter_configs(std::slice::from_ref(collector), timeout)?;
        let configured = collector.clone();
        configure_collector_deployment(&config, collector, 1)?;
        assert_eq!(*collector, configured);
        let mut fresh = rendered_collector;
        configure_collector_deployment(&config, &mut fresh, 1)?;
        assert_exporter_configs(std::slice::from_ref(&fresh), timeout)?;
        assert_eq!(fresh, configured);
        for invalid in [
            "{",
            "{}",
            r#"{"startup_table":{}}"#,
            r#"{"authority_timeout":"10s","extra":true}"#,
            r#"{"authority_timeout":"20s"}"#,
            r#"{"authority_timeout":"10s","authority_timeout":"10s"}"#,
        ] {
            let mut invalid_deployment = configured.clone();
            let entries = invalid_deployment.pointer_mut("/spec/template/spec/containers/0/env")
                .and_then(Value::as_array_mut).context("collector env missing")?;
            entries.retain(|entry| entry["name"] != "ARROWCOL_COLLECTOR_EXPORTER_CONFIG");
            entries.push(json!({"name":"ARROWCOL_COLLECTOR_EXPORTER_CONFIG","value":invalid}));
            assert!(configure_collector_deployment(&config, &mut invalid_deployment.clone(), 1).is_err(), "{invalid}");
            assert!(inject_workload_environment(&config, std::slice::from_mut(&mut invalid_deployment)).is_err(), "{invalid}");
        }
        let mut duplicated = configured.clone();
        let entries = duplicated.pointer_mut("/spec/template/spec/containers/0/env")
            .and_then(Value::as_array_mut).context("collector env missing")?;
        let entry = json!({"name":"ARROWCOL_COLLECTOR_EXPORTER_CONFIG","value":r#"{"authority_timeout":"10s"}"#});
        entries.retain(|entry| entry["name"] != "ARROWCOL_COLLECTOR_EXPORTER_CONFIG");
        entries.extend([entry.clone(),entry]);
        assert!(configure_collector_deployment(&config, &mut duplicated.clone(), 1).is_err());
        assert!(inject_workload_environment(&config, std::slice::from_mut(&mut duplicated)).is_err());
        Ok(())
    }

    fn assert_exporter_configs(resources: &[Value], timeout: Option<&str>) -> Result<()> {
        let mut count = 0;
        for resource in resources {
            if let Some(containers) = resource.pointer("/spec/template/spec/containers").and_then(Value::as_array) {
                for container in containers {
                    if let Some(env) = container["env"].as_array() {
                        for entry in env.iter().filter(|entry| entry["name"] == "ARROWCOL_COLLECTOR_EXPORTER_CONFIG") {
                            let parsed: arrowcol_tunables::ExporterTunables = serde_json::from_str(
                                entry["value"].as_str().context("exporter config must be a string")?
                            ).context("actual collector exporter parser rejected emitted config")?;
                            assert_eq!(parsed.authority_timeout.as_deref(), timeout);
                            count += 1;
                        }
                    }
                }
            }
        }
        assert_eq!(count, usize::from(timeout.is_some()));
        Ok(())
    }

    #[test]
    fn local_collector_port_uses_bind_host() -> Result<()> {
        let proof_config = std::env::var_os("OAT_BIND_PROOF_CONFIG");
        let source = match &proof_config {
            Some(path) => std::fs::read_to_string(path)?,
            None => include_str!("../../../docs/experiments/arrowcol-oat/oat-config.quick.json").to_owned(),
        };
        let mut config: OatConfig = serde_json::from_str(&source)?;
        for (name, host, expected) in [
            ("default", None, "127.0.0.1"),
            ("gateway", Some("172.18.0.1".parse()?), "172.18.0.1"),
            ("ipv6", Some("::1".parse()?), "::1"),
        ] {
            config.platform.local.bind_host = host;
            let mut overlay = compose_tunables(&config)?;
            local_dataflow_overlay(&config, Path::new("/owned/run"), &mut overlay)?;
            assert_eq!(
                overlay["services"]["dataflow-1"]["ports"],
                json!([{"host_ip":expected,"target":32011,"published":"0","protocol":"tcp"}])
            );
            if proof_config.is_some() {
                println!("OAT_BIND_OVERLAY_{name}={overlay}");
            }
        }
        Ok(())
    }

    #[test]
    fn overload_local_memory_cap_is_explicit_and_scoped_to_the_collector() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let mut overlay = compose_tunables(&config)?;
        local_dataflow_overlay(&config, Path::new("/owned/run"), &mut overlay)?;
        assert!(overlay["services"]["dataflow-1"].get("mem_limit").is_none());
        config.platform.local.collector_memory_limit_bytes = Some(4 * 1024_u64.pow(3));
        config.validate()?;
        local_dataflow_overlay(&config, Path::new("/owned/run"), &mut overlay)?;
        assert_eq!(overlay["services"]["dataflow-1"]["mem_limit"], 4 * 1024_u64.pow(3));
        for (service, value) in overlay["services"].as_object().context("services missing")? {
            if service != "dataflow-1" { assert!(value.get("mem_limit").is_none()); }
        }
        for invalid in [json!("4GiB"), json!("4096Mi"), json!(-1), json!(1.5)] {
            assert!(serde_json::from_value::<crate::config::LocalOptions>(
                json!({"collector_memory_limit_bytes":invalid})).is_err());
        }
        config.platform.local.collector_memory_limit_bytes = Some(1);
        assert!(config.validate().is_err());
        Ok(())
    }

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
            partition_by_ingestion_date: true, unpartitioned: false, table: table.to_owned(), dataflow_root: root.0.clone(),
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
    fn collector_shutdown_drain_timeout_reaches_dfe_profiles_and_readback() -> Result<()> {
        const CHILD: &str = "OAT_DFE_DRAIN_CHILD";
        const ENV: &str = "ARROWCOL_COLLECTOR_SHUTDOWN_DRAIN_TIMEOUT_SECONDS";
        let mut config = config()?;
        config.collector_stop_grace_seconds = 540;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector": {"shutdown_drain_timeout_seconds": 240}
        }))?;
        config.run_pin_ttl_ms = 86_400_000;
        config.validate()?;
        if std::env::var_os(CHILD).is_some() {
            use otel_arrow_dfe_config::{engine::OtelDataflowSpec, env_substitution::substitute_env_vars};
            for template in [
                include_str!("../../arrowcol-dataflow-rust/config.yaml"),
                include_str!("../../arrowcol-dataflow-rust/config.quiver.yaml"),
            ] {
                let value = substitute_env_vars(template)?;
                let parsed = OtelDataflowSpec::from_yaml(&value)?;
                assert_eq!(
                    arrowcol_tunables::MAX_COLLECTOR_SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
                    otel_arrow_dfe_config::engine::MAX_SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
                );
                let actual = parsed.engine.shutdown_drain_timeout_seconds.get();
                assert_eq!(actual, 240);
                let line = format!("df_engine_effective_config {}", json!({
                    "engine": {"shutdown_drain_timeout_seconds": actual}
                }));
                let text = filter_process_evidence_text(&line)?;
                let observed = process_configurations(&text)?;
                verify_process_configuration(&config, ComponentRole::Collector, &observed)?;
            }
            for observed in [json!({}), json!({"engine":{"shutdown_drain_timeout_seconds": 60}})] {
                assert!(verify_process_configuration(&config, ComponentRole::Collector, &observed).is_err());
            }
            return Ok(());
        }
        let compose = compose_tunables(&config)?;
        let expected = compose["services"]["dataflow-1"]["environment"][ENV].as_str().context("missing Compose tunable")?;
        let mut deployment = json!({"spec":{"template":{"spec":{"containers":[{"name":"dataflow"}]}}}});
        inject_deployment_environment(&mut deployment, "dataflow", &role_environment(&config, ComponentRole::Collector)?)?;
        assert_eq!(deployment["spec"]["template"]["spec"]["containers"][0]["env"], json!([{"name": ENV,"value":expected}]));
        let output = std::process::Command::new(std::env::current_exe()?)
            .env(CHILD, "1")
            .env(ENV, expected)
            .args(["collector_shutdown_drain_timeout_reaches_dfe_profiles_and_readback", "--nocapture"])
            .output()?;
        anyhow::ensure!(
            output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed;"),
            "DFE drain config subprocess failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
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
    fn aged_batching_policy_reaches_compose_aks_and_effective_readback() -> Result<()> {
        let mut config = config()?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector":{"batching":{"age_flush_policy":"drain_ready"}}
        }))?;
        let environment = role_environment(&config, ComponentRole::Collector)?;
        let requested: Value = serde_json::from_str(&environment["ARROWCOL_COLLECTOR_BATCHING_CONFIG"])?;
        assert_eq!(requested, json!({"age_flush_policy":"drain_ready"}));
        let compose = compose_tunables(&config)?;
        assert_eq!(compose["services"]["dataflow-1"]["environment"], json!(environment));
        let mut resources = vec![json!({
            "kind":"Deployment", "metadata":{"name":config.dfe_deployment},
            "spec":{"template":{"spec":{"containers":[{"name":config.dfe_container,"env":[]}]}}}
        })];
        inject_workload_environment(&config, &mut resources)?;
        let injected = resources[0].pointer("/spec/template/spec/containers/0/env")
            .and_then(Value::as_array).context("injected environment")?;
        assert!(injected.iter().any(|entry| entry["name"] == "ARROWCOL_COLLECTOR_BATCHING_CONFIG"
            && entry["value"] == environment["ARROWCOL_COLLECTOR_BATCHING_CONFIG"]));
        let startup = |policy: &str| format!("arrowcol_effective_config {}\n", json!({
            "component":"exporter","config":{"batching":{"age_flush_policy":policy,"max_age":"30s"}}
        }));
        let actual = process_configurations(&startup("drain_ready"))?;
        verify_process_configuration(&config, ComponentRole::Collector, &actual)?;
        assert_eq!(actual["batching"]["age_flush_policy"], requested["age_flush_policy"]);
        for policy in ["immediate", "drain_forever"] {
            assert!(verify_process_configuration(
                &config, ComponentRole::Collector, &process_configurations(&startup(policy))?
            ).is_err());
        }
        assert!(verify_process_configuration(&config, ComponentRole::Collector, &json!({})).is_err());
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
