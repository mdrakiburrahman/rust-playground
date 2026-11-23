fn diagnostic_cpu_masks(names: &str, collector: &[u32], client: &str) -> Result<BTreeMap<String, String>> {
    let collector = collector.iter().copied().collect::<BTreeSet<_>>();
    let client = diagnostic_cpu_set(client)?;
    let support = BTreeSet::from([30, 31]);
    ensure!(
        client == (24..30).collect()
            && !collector.is_empty()
            && collector.is_disjoint(&client)
            && collector.is_disjoint(&support),
        "invalid diagnostic CPU role sets"
    );
    let mut masks = BTreeMap::new();
    for name in names.lines() {
        ensure!(
            !name.is_empty() && name.bytes().all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c)),
            "invalid resolved service"
        );
        let cpus = match name {
            "oat-client" => &client,
            "dataflow-1" => &collector,
            _ => &support,
        };
        masks.insert(
            name.to_owned(),
            cpus.iter().map(u32::to_string).collect::<Vec<_>>().join(","),
        );
    }
    ensure!(
        ["router", "dataflow-1", "oat-client"]
            .iter()
            .all(|s| masks.contains_key(*s)),
        "incomplete resolved diagnostic project"
    );
    Ok(masks)
}

async fn apply_diagnostic_cpu_masks(config: &OatConfig, platform: &LocalPlatform, overlay: &mut Value) -> Result<()> {
    let Some(_) = diagnostic_expected_pin(
        config.platform.compose_project.as_deref(),
        std::env::var(DIAGNOSTIC_CORPUS_ENV),
    )?
    else {
        return Ok(());
    };
    let project = scoped_compose_project(
        config
            .platform
            .compose_project
            .as_deref()
            .context("missing diagnostic project")?,
        &platform.run_id,
    );
    let mut command = compose_command(
        config
            .platform
            .compose_file
            .as_deref()
            .context("missing compose file")?,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut command);
    command.args(["--profile", "oat", "--profile", "maintenance", "config", "--services"]);
    let names = diagnostic_command(command, "resolve diagnostic service roster").await?;
    let masks = diagnostic_cpu_masks(
        &names,
        config
            .platform
            .local
            .collector_cpus
            .as_deref()
            .context("missing collector CPUs")?,
        &std::env::var(DIAGNOSTIC_CLIENT_CPUS_ENV)?,
    )?;
    for (service, mask) in &masks {
        overlay["services"][service]["cpuset"] = json!(mask);
    }
    tokio::fs::write(
        platform.state_dir.join("diagnostic-cpus.json"),
        serde_json::to_vec(&masks)?,
    )
    .await?;
    Ok(())
}

fn diagnostic_cpu_tool() -> Command {
    let mut c = Command::new("node");
    c.args(["--import", "tsx", "projects/arrowcol-oat/diagnostic-cpu.ts"])
        .kill_on_drop(true);
    c
}

#[cfg(test)]
#[test]
fn resolved_unknown_service_is_pinned_to_shared_support_not_collector() -> Result<()> {
    let names = "dataflow-1\noat-client\nrouter\nfuture-worker";
    let masks = diagnostic_cpu_masks(names, &[0, 1], "24-29")?;
    assert_eq!(masks["future-worker"], "30,31");
    assert_eq!(masks["router"], masks["future-worker"]);
    assert_eq!(masks["dataflow-1"], "0,1");
    assert!(diagnostic_cpu_masks(names, &[1, 31], "24-29").is_err());
    Ok(())
}
