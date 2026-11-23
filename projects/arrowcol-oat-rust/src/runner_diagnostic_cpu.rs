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

fn diagnostic_worker_count(cpus: &[u32], concurrency: &[u32], workers: Option<usize>) -> Result<usize> {
    let n = cpus.len();
    ensure!(
        matches!(n, 8 | 16 | 24)
            && cpus.windows(2).all(|p| p[0] < p[1])
            && cpus.iter().all(|&cpu| cpu < 24)
            && concurrency == [n as u32]
            && workers == Some(n),
        "diagnostic engine workers/concurrency must equal collector CPU allocation"
    );
    Ok(n)
}

fn configured_diagnostic_workers(config: &OatConfig) -> Result<usize> {
    diagnostic_worker_count(
        config.platform.local.collector_cpus.as_deref().unwrap_or_default(),
        &config.dfe_worker_concurrency,
        config
            .tunables
            .collector
            .as_ref()
            .and_then(|c| c.runtime.as_ref())
            .and_then(|r| r.cpu_workers),
    )
}

fn verify_diagnostic_engine_argv(raw: &str, expected: usize) -> Result<usize> {
    let args: Vec<_> = raw
        .strip_suffix('\0')
        .context("missing live collector argv")?
        .split('\0')
        .collect();
    let exe = "/usr/local/bin/arrowcol-dataflow-rust";
    ensure!(
        args.first() == Some(&exe) && args.iter().filter(|&&a| a == exe).count() == 1,
        "ambiguous collector process"
    );
    let flags: Vec<_> = args
        .iter()
        .enumerate()
        .filter(|(_, a)| **a == "--num-cores" || a.starts_with("--num-cores="))
        .collect();
    ensure!(
        flags.len() == 1
            && *flags[0].1 == "--num-cores"
            && args.get(flags[0].0 + 1).copied() == Some(expected.to_string().as_str()),
        "running collector --num-cores differs from diagnostic CPU allocation"
    );
    Ok(expected)
}

async fn diagnostic_engine_readback(id: &str, config: &OatConfig) -> Result<usize> {
    let expected = configured_diagnostic_workers(config)?;
    let mut cmd = Command::new("docker");
    cmd.args(["exec", id, "/bin/sh", "-ec", r#"for f in /proc/[0-9]*/cmdline; do
        if [ "$(tr '\000' '\n' < "$f" 2>/dev/null | head -n 1)" = /usr/local/bin/arrowcol-dataflow-rust ]; then cat "$f"; fi
    done"#]);
    verify_diagnostic_engine_argv(
        &diagnostic_command(cmd, "read running collector engine arguments").await?,
        expected,
    )
}

#[test]
fn diagnostic_worker_strata_reject_mismatched_config_and_live_engine() -> Result<()> {
    for n in [8, 16, 24] {
        let cpus: Vec<_> = (0..n).collect();
        let expected = diagnostic_worker_count(&cpus, &[n], Some(n as usize))?;
        let argv =
            format!("/usr/local/bin/arrowcol-dataflow-rust\0--num-cores\0{n}\0--config\0/etc/oat/dataflow.yaml\0");
        assert_eq!(verify_diagnostic_engine_argv(&argv, expected)?, cpus.len());
        assert!(diagnostic_worker_count(&cpus, &[32], Some(n as usize)).is_err());
        assert!(diagnostic_worker_count(&cpus, &[n], Some(32)).is_err());
        assert!(verify_diagnostic_engine_argv(&argv, 32).is_err());
        assert!(verify_diagnostic_engine_argv(&format!("{argv}{argv}"), expected).is_err());
    }
    Ok(())
}
