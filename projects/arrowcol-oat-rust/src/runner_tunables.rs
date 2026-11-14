use arrowcol_tunables::ComponentRole;

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
    config.tunables.environment_for(role)
}

fn compose_tunables(config: &OatConfig) -> Result<Value> {
    let mut services = serde_json::Map::new();
    for (service, role) in [
        ("oat-client", ComponentRole::Client),
        ("router", ComponentRole::Router),
        ("dataflow-1", ComponentRole::Collector),
    ] {
        let environment = role_environment(config, role)?;
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

    fn config() -> Result<OatConfig> {
        Ok(serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?)
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
        assert!(compose["services"].get("router").is_none());
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
