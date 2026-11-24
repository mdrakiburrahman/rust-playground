async fn validate_local_collector_cpu_quota(config: &OatConfig) -> Result<()> {
    let Some(quota) = config.platform.local.collector_cpu_quota else {
        return Ok(());
    };
    let nano_cpus = quota.nano_cpus().map_err(anyhow::Error::msg)?;
    let mut info = Command::new("docker");
    info.args(["info", "--format", "{{.NCPU}}"]);
    let output = run_command(info, "read Docker host logical CPU count").await?;
    let host_cpus: u64 = String::from_utf8(output.stdout)?.trim().parse()?;
    if host_cpus == 0 || nano_cpus > host_cpus.saturating_mul(1_000_000_000) {
        bail!("platform.local.collector_cpu_quota exceeds Docker host logical CPUs ({host_cpus})");
    }
    Ok(())
}

async fn capture_local_cpu_quota(
    config: &OatConfig,
    run_id: &str,
    environment: &HashMap<String, String>,
) -> Result<Value> {
    let project = scoped_compose_project(
        config.platform.compose_project.as_deref().context("local compose project name is missing")?,
        run_id,
    );
    let mut list = compose_command(
        config.platform.compose_file.as_deref().context("local compose file is missing")?,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    list.envs(environment);
    list.args(["ps", "--quiet", "dataflow-1"]);
    let ids = String::from_utf8(run_command(list, "list collector CPU quota targets").await?.stdout)?;
    let mut containers = Vec::new();
    for id in ids.lines().map(str::trim).filter(|id| !id.is_empty()) {
        let mut inspect = Command::new("docker");
        inspect.envs(environment);
        inspect.args(["inspect", "--format", "{{json .HostConfig.NanoCpus}}", id]);
        let nano_cpus = match run_command(inspect, "read collector NanoCpus").await {
            Ok(output) => {
                let raw = String::from_utf8(output.stdout)?;
                match raw.trim() {
                    "" | "null" => None,
                    value => Some(value.parse::<u64>().context("collector NanoCpus readback is invalid")?),
                }
            }
            Err(error) => {
                tracing::warn!("collector NanoCpus readback unavailable; using cgroup cpu.max: {error:#}");
                None
            }
        };
        let mut read = Command::new("docker");
        read.envs(environment);
        read.args(["exec", id, "sh", "-ec",
            "cat /sys/fs/cgroup/cpu.max; cat /sys/fs/cgroup/cpu.stat"]);
        let counters = String::from_utf8(run_command(read, "read collector cgroup CPU counters").await?.stdout)?;
        containers.push(parse_local_cpu_quota(id, nano_cpus, &counters)?);
    }
    if containers.is_empty() {
        bail!("collector CPU quota readback has no running containers");
    }
    Ok(json!({"sampled_at_unix_ms":unix_time_ms_u64()?, "containers":containers}))
}

fn parse_local_cpu_quota(id: &str, nano_cpus: Option<u64>, text: &str) -> Result<Value> {
    let mut lines = text.lines();
    let cpu_max = lines.next().context("collector cpu.max is missing")?;
    let fields = cpu_max.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 2 || fields[1].parse::<u64>()? == 0 {
        bail!("invalid collector cpu.max");
    }
    let limit_millicores = if fields[0] == "max" {
        None
    } else {
        Some(fields[0].parse::<u64>()? as f64 * 1000.0 / fields[1].parse::<u64>()? as f64)
    };
    let mut counters = serde_json::Map::new();
    for line in lines {
        let Some((name, value)) = line.split_once(' ') else {
            bail!("invalid collector cpu.stat row");
        };
        if ["nr_periods", "nr_throttled", "throttled_usec"].contains(&name) {
            counters.insert(name.to_owned(), json!(value.trim().parse::<u64>()?));
        }
    }
    if counters.len() != 3 {
        bail!("collector cpu.stat omitted required throttling counters");
    }
    Ok(json!({"container_id":id, "nano_cpus":nano_cpus, "readback_source":if nano_cpus.is_some() { "docker_nanocpus_and_cgroup_cpu_max" } else { "cgroup_cpu_max" }, "cpu_max":cpu_max,
        "limit_millicores":limit_millicores, "cpu_stat":counters}))
}

fn verify_local_cpu_quota(config: &OatConfig, workers: u32, readback: &Value) -> Result<()> {
    let expected = config.platform.local.collector_cpu_quota
        .map_or_else(|| Ok(u64::from(workers) * 1_000_000_000), |quota| quota.nano_cpus())
        .map_err(anyhow::Error::msg)?;
    for container in readback["containers"].as_array().context("CPU quota containers missing")? {
        let actual = container["nano_cpus"].as_u64();
        let effective = container["limit_millicores"].as_f64();
        if actual.is_some_and(|actual| actual != expected) || (expected == 0) != effective.is_none()
            || effective.is_some_and(|limit| (limit - expected as f64 / 1_000_000.0).abs() > 0.01)
        {
            bail!("platform.local.collector_cpu_quota readback differs: expected {expected} NanoCpus, observed {container}");
        }
    }
    Ok(())
}

async fn local_cpu_quota_observation(config: &OatConfig, run_id: &str, environment: &HashMap<String, String>) -> Value {
    match tokio::time::timeout(Duration::from_secs(5), capture_local_cpu_quota(config, run_id, environment)).await {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => json!({"status":"unavailable", "reason":format!("{error:#}")}),
        Err(_) => json!({"status":"unavailable", "reason":"collector CPU quota snapshot deadline exceeded"}),
    }
}

fn local_cpu_quota_window(config: &OatConfig, start: u64, end: u64, before: Option<&Value>, after: Option<&Value>) -> Value {
    let mut deltas = Vec::new();
    if let Some(containers) = after.and_then(|sample| sample["containers"].as_array()) {
        for container in containers {
            let initial = before.and_then(|sample| sample["containers"].as_array())
                .and_then(|samples| samples.iter().find(|sample| sample["container_id"] == container["container_id"]));
            let mut delta = json!({"container_id":container["container_id"], "status":"measured"});
            for name in ["nr_periods", "nr_throttled", "throttled_usec"] {
                let value = initial.and_then(|sample| sample["cpu_stat"][name].as_u64())
                    .and_then(|first| container["cpu_stat"][name].as_u64()?.checked_sub(first));
                delta[name] = json!(value);
                if value.is_none() {
                    delta["status"] = json!("unavailable");
                    delta["reason"] = json!("initial snapshot missing, container identity changed, or counter reset");
                }
            }
            deltas.push(delta);
        }
    }
    json!({
        "collector": {
            "source": if config.platform.local.collector_cpu_quota.is_some() { "platform.local.collector_cpu_quota" } else { "coupled_to_dfe_worker_concurrency" },
            "requested_quota":config.platform.local.collector_cpu_quota,
            "readback":after.or(before),
        },
        "throttling": {
            "status":if deltas.is_empty() { "unavailable" } else { "sampled" },
            "scope":"measurement_window_boundary_snapshots",
            "window_start_unix_ms":start, "window_end_unix_ms":end,
            "start":before, "end":after, "containers":deltas,
            "reason":if before.is_none() || after.is_none() { Some("measurement interrupted before both boundary snapshots") } else { None },
        },
    })
}

#[cfg(test)]
mod local_cpu_quota_tests {
    use super::*;

    #[test]
    fn invalid_control_values_name_the_exact_oat_field() -> Result<()> {
        let config: Value = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let mut candidate = config.clone();
        candidate["platform"]["local"]["collector_cpu_quota"] = json!("max");
        let error = serde_json::from_value::<OatConfig>(candidate).err().context("invalid quota accepted")?;
        assert!(error.to_string().contains("platform.local.collector_cpu_quota"), "{error}");
        let mut candidate = config;
        candidate["overload_guard"]["container_memory_metric"] = json!("working-set");
        let error = serde_json::from_value::<OatConfig>(candidate).err().context("invalid metric accepted")?;
        assert!(error.to_string().contains("overload_guard.container_memory_metric"), "{error}");
        Ok(())
    }

    #[test]
    fn quota_overlay_is_independent_and_rejects_invalid_input() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        for invalid in [json!(-1), json!(0), json!(0.009), json!(4097), json!(0.0000000001), json!("max"), json!("2"), json!(true), json!({})] {
            let parsed = serde_json::from_value::<arrowcol_tunables::CollectorCpuQuota>(invalid);
            assert!(parsed.is_err() || parsed.is_ok_and(|quota| quota.nano_cpus().is_err()));
        }
        for (requested, expected) in [(json!(0.01), 0.01), (json!(3.5), 3.5), (json!(1.001), 1.001), (json!("unlimited"), 0.0)] {
            config.platform.local.collector_cpu_quota = Some(serde_json::from_value(requested)?);
            config.validate()?;
            let mut overlay = compose_tunables(&config)?;
            local_dataflow_overlay(&config, Path::new("/owned/run"), &mut overlay)?;
            assert_eq!(overlay["services"]["dataflow-1"]["cpus"], expected);
            assert!(overlay["services"]["dataflow-1"]["command"][0].as_str().context("command")?.contains("${LOADTEST_CPUS:-2}"));
        }
        let readback = json!({"containers":[parse_local_cpu_quota("id", Some(0),
            "max 100000\nnr_periods 10\nnr_throttled 2\nthrottled_usec 30\n")?]});
        verify_local_cpu_quota(&config, 2, &readback)?;
        assert!(parse_local_cpu_quota("id", None, "max 100000\nnr_periods 10\n").is_err());
        let fallback = json!({"containers":[parse_local_cpu_quota("id", None,
            "max 100000\nnr_periods 10\nnr_throttled 2\nthrottled_usec 30\n")?]});
        assert_eq!(fallback["containers"][0]["readback_source"], "cgroup_cpu_max");
        verify_local_cpu_quota(&config, 2, &fallback)?;
        config.platform.local.collector_cpu_quota = Some(serde_json::from_value(json!(1.5))?);
        assert!(verify_local_cpu_quota(&config, 2, &fallback).is_err());
        let fallback = json!({"containers":[parse_local_cpu_quota("id", None,
            "150000 100000\nnr_periods 10\nnr_throttled 2\nthrottled_usec 30\n")?]});
        verify_local_cpu_quota(&config, 2, &fallback)?;
        Ok(())
    }

    #[test]
    fn throttling_window_subtracts_only_matching_monotonic_counters() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let before = json!({"containers":[parse_local_cpu_quota("id", Some(2_000_000_000),
            "200000 100000\nnr_periods 10\nnr_throttled 2\nthrottled_usec 30\n")?]});
        let mut after = json!({"containers":[parse_local_cpu_quota("id", Some(2_000_000_000),
            "200000 100000\nnr_periods 20\nnr_throttled 5\nthrottled_usec 80\n")?]});
        let result = local_cpu_quota_window(&config, 1, 2, Some(&before), Some(&after));
        assert_eq!(result["throttling"]["containers"][0]["nr_periods"], 10);
        assert_eq!(result["throttling"]["containers"][0]["nr_throttled"], 3);
        assert_eq!(result["throttling"]["containers"][0]["throttled_usec"], 50);
        after["containers"][0]["container_id"] = json!("replacement");
        let result = local_cpu_quota_window(&config, 1, 2, Some(&before), Some(&after));
        assert_eq!(result["throttling"]["containers"][0]["status"], "unavailable");
        Ok(())
    }
}
