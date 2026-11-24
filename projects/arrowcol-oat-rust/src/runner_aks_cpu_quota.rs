// cAdvisor counters are independent of the older scheduled-load throttling evidence.
// Keep the latter intact while collecting identity-fenced measurement boundaries.
struct AksCpuQuotaWindowTask(JoinHandle<Value>);

struct AksCpuResourceMeasurement {
    throttling_start: Vec<CpuThrottleSample>,
    quota_targets: Vec<AksResourcePodTarget>,
    quota_window: Option<AksCpuQuotaWindow>,
    quota_evidence: Value,
}

impl Drop for AksCpuQuotaWindowTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

enum AksCpuQuotaWindow {
    Background(AksCpuQuotaWindowTask),
    Fixture {
        start: u64,
        end: u64,
        before: Option<Value>,
        after: Option<Value>,
    },
}

impl AksCpuQuotaWindow {
    async fn poll(&mut self, config: &OatConfig, targets: &[AksResourcePodTarget]) {
        if let Self::Fixture {
            start,
            end,
            before,
            after,
        } = self
        {
            for (timestamp, snapshot) in [(*start, before), (*end, after)] {
                if snapshot.is_none() && unix_time_ms_u64().is_ok_and(|now| now >= timestamp) {
                    *snapshot = Some(aks_cpu_quota_observation(config, targets).await);
                }
            }
        }
    }

    async fn finish(mut self, config: &OatConfig, targets: &[AksResourcePodTarget], preflight: &Value) -> Value {
        self.poll(config, targets).await;
        match self {
            Self::Background(mut task) => match (&mut task.0).await {
                Ok(evidence) => evidence,
                Err(error) => json!({
                    "verification_status": "unavailable", "collector": preflight["collector"],
                    "throttling": {"status": "unavailable", "reason": format!("AKS CPU quota task failed: {error}")},
                }),
            },
            Self::Fixture {
                start,
                end,
                before,
                after,
            } => {
                let missing = || json!({"status": "unavailable", "reason": "measurement boundary was not sampled"});
                aks_cpu_quota_window(
                    config,
                    targets,
                    start,
                    end,
                    before.unwrap_or_else(missing),
                    after.unwrap_or_else(missing),
                )
            }
        }
    }
}

fn aks_collector_quota_targets(readbacks: &[Value], replicas: usize) -> Result<Vec<AksResourcePodTarget>> {
    let targets = aks_resource_pod_targets(readbacks)?
        .into_iter()
        .filter(|target| target.role == "collector")
        .collect::<Vec<_>>();
    ensure!(
        replicas > 0 && targets.len() == replicas,
        "AKS collector CPU quota readback omitted or added replicas"
    );
    let mut identities = BTreeSet::new();
    for target in &targets {
        ensure!(
            !target.pod.is_empty()
                && !target.uid.is_empty()
                && !target.node.is_empty()
                && !target.container.is_empty()
                && target.container_id.as_deref().is_some_and(|id| !id.is_empty())
                && identities.insert(target.pod.as_str()),
            "AKS collector CPU quota identity is missing or ambiguous"
        );
    }
    Ok(targets)
}

fn aks_quota_identity(target: &AksResourcePodTarget) -> Value {
    json!({
        "pod": target.pod, "uid": target.uid, "node": target.node,
        "container": target.container, "container_id": target.container_id,
    })
}

fn verify_aks_quota_identities(expected: &[AksResourcePodTarget], actual: &[AksResourcePodTarget]) -> Result<()> {
    ensure!(
        !expected.is_empty() && expected.len() == actual.len(),
        "AKS collector replica set changed"
    );
    for target in expected {
        let matches = actual
            .iter()
            .filter(|sample| sample.pod == target.pod)
            .collect::<Vec<_>>();
        let [sample] = matches.as_slice() else {
            bail!("AKS collector pod identity is missing or ambiguous");
        };
        ensure!(
            aks_quota_identity(target) == aks_quota_identity(sample),
            "AKS collector pod, node, UID or runtime container identity changed"
        );
    }
    Ok(())
}

fn cadvisor_quota_cgroup_matches(id: &str, target: &AksResourcePodTarget) -> bool {
    let Some(container_id) = target
        .container_id
        .as_deref()
        .and_then(|id| id.split_once("://").map(|(_, id)| id))
    else {
        return false;
    };
    let normalized = id.replace('-', "_");
    let pod_uid = format!("pod{}", target.uid.replace('-', "_"));
    !container_id.is_empty()
        && normalized.split(['/', '.', ':']).any(|part| part.ends_with(&pod_uid))
        && id
            .split(|c: char| !c.is_ascii_alphanumeric())
            .any(|part| part == container_id)
}

fn parse_aks_cadvisor_cpu_quota(metrics: &str, namespace: &str, target: &AksResourcePodTarget) -> Result<Value> {
    let mut values = HashMap::<&str, f64>::new();
    let mut cgroup_id = None;
    for line in metrics.lines().filter(|line| !line.starts_with('#')) {
        let Some((name, remainder)) = line.split_once('{') else {
            continue;
        };
        if !matches!(
            name,
            "container_spec_cpu_quota"
                | "container_spec_cpu_period"
                | "container_cpu_cfs_periods_total"
                | "container_cpu_cfs_throttled_periods_total"
                | "container_cpu_cfs_throttled_seconds_total"
        ) {
            continue;
        }
        let (labels, raw) = remainder
            .split_once('}')
            .context("malformed cAdvisor CPU quota metric")?;
        let labels = labels
            .split(',')
            .filter_map(|label| label.split_once('='))
            .map(|(key, value)| (key.trim(), value.trim().trim_matches('"')))
            .collect::<HashMap<_, _>>();
        if labels.get("namespace").copied() != Some(namespace)
            || labels.get("pod").or_else(|| labels.get("pod_name")).copied() != Some(target.pod.as_str())
            || labels.get("container").copied() != Some(target.container.as_str())
        {
            continue;
        }
        let id = labels
            .get("id")
            .copied()
            .context("cAdvisor CPU quota metric omitted cgroup identity")?;
        ensure!(
            cadvisor_quota_cgroup_matches(id, target),
            "cAdvisor CPU quota metric UID or runtime container identity differs"
        );
        ensure!(
            cgroup_id.is_none_or(|previous| previous == id),
            "cAdvisor CPU quota metrics span different cgroups"
        );
        cgroup_id = Some(id);
        let value = raw
            .split_whitespace()
            .next()
            .context("cAdvisor CPU quota metric omitted value")?
            .parse::<f64>()?;
        ensure!(value.is_finite(), "cAdvisor CPU quota metric is not finite");
        ensure!(
            values.insert(name, value).is_none(),
            "ambiguous cAdvisor CPU quota metric"
        );
    }
    let integer = |name: &str| -> Result<u64> {
        let value = *values.get(name).with_context(|| format!("cAdvisor omitted {name}"))?;
        ensure!(
            value >= 0.0 && value.fract() == 0.0 && value <= 9_007_199_254_740_991.0,
            "cAdvisor CPU quota metric is not an exact nonnegative integer"
        );
        Ok(value as u64)
    };
    let quota = *values
        .get("container_spec_cpu_quota")
        .context("cAdvisor omitted effective CPU quota")?;
    // CpuSpec uses zero for disabled CFS quota. Upstream exporters usually omit
    // that metric (and its counters); absence is not proof of an unlimited quota.
    let quota = if quota == -1.0 || quota == 0.0 {
        None
    } else {
        Some(integer("container_spec_cpu_quota")?)
    };
    let period = integer("container_spec_cpu_period")?;
    ensure!(period > 0, "cAdvisor CPU quota period is zero");
    let periods = integer("container_cpu_cfs_periods_total")?;
    let throttled = integer("container_cpu_cfs_throttled_periods_total")?;
    ensure!(throttled <= periods, "cAdvisor throttled periods exceed total periods");
    let seconds = *values
        .get("container_cpu_cfs_throttled_seconds_total")
        .context("cAdvisor omitted throttled time")?;
    let micros = seconds * 1_000_000.0;
    ensure!(
        (0.0..=9_007_199_254_740_991.0).contains(&micros),
        "cAdvisor throttled time is invalid or exceeds exact integer precision"
    );
    let limit = quota.map(|quota| quota as f64 * 1000.0 / period as f64);
    let mut sample = aks_quota_identity(target);
    sample["cgroup_id"] = json!(cgroup_id);
    sample["quota_usec"] = json!(quota);
    sample["period_usec"] = json!(period);
    sample["cpu_max"] = json!(format!(
        "{} {period}",
        quota.map_or_else(|| "max".to_owned(), |q| q.to_string())
    ));
    sample["limit_millicores"] = json!(limit);
    sample["nano_cpus"] = Value::Null;
    sample["cpu_stat"] = json!({
        "nr_periods": periods, "nr_throttled": throttled, "throttled_usec": micros.round() as u64,
    });
    Ok(sample)
}

fn verify_aks_cpu_quota_samples(requested: Option<u64>, samples: &[Value], replicas: usize) -> Result<()> {
    ensure!(
        replicas > 0 && samples.len() == replicas,
        "AKS effective CPU quota evidence omitted replicas"
    );
    let mut identities = BTreeSet::new();
    for sample in samples {
        let id = sample["container_id"]
            .as_str()
            .context("AKS CPU quota omitted runtime identity")?;
        ensure!(
            !id.is_empty() && identities.insert(id),
            "AKS CPU quota runtime identity is ambiguous"
        );
        let period = sample["period_usec"].as_u64().context("AKS CPU quota omitted period")?;
        ensure!(period > 0, "AKS CPU quota period is zero");
        let quota = match sample.get("quota_usec") {
            Some(Value::Null) => None,
            Some(value) => Some(value.as_u64().context("AKS effective CPU quota is invalid")?),
            None => bail!("AKS effective CPU quota is missing"),
        };
        ensure!(
            match (requested, quota) {
                (None, None) => true,
                (Some(requested), Some(quota)) =>
                    u128::from(quota) * 1000 == u128::from(requested) * u128::from(period),
                _ => false,
            },
            "AKS requested versus effective collector CPU quota differs"
        );
        for name in ["nr_periods", "nr_throttled", "throttled_usec"] {
            sample["cpu_stat"][name]
                .as_u64()
                .with_context(|| format!("AKS CPU quota omitted {name}"))?;
        }
    }
    Ok(())
}

async fn capture_aks_collector_cpu_quota(config: &OatConfig, expected: &[AksResourcePodTarget]) -> Result<Value> {
    let started = unix_time_ms_u64()?;
    let capture = async {
        let requested = config.effective_aks_resources().collector;
        let requested_millicores = requested
            .limits
            .cpu
            .as_deref()
            .map(parse_resource_cpu_millicores)
            .transpose()?;
        let live_targets = async {
            let readback = aks_deployment_resource_readback(
                config,
                "collector",
                &config.dfe_deployment,
                &config.dfe_container,
                &requested,
            )
            .await?;
            aks_collector_quota_targets(&[readback], expected.len())
        };
        let before = live_targets.await?;
        verify_aks_quota_identities(expected, &before)?;
        let nodes = expected
            .iter()
            .map(|target| target.node.as_str())
            .collect::<BTreeSet<_>>();
        let scrape = |node| async move {
            let path = format!("/api/v1/nodes/{node}/proxy/metrics/cadvisor");
            let metrics = kubectl(config, vec!["get", "--raw", &path, "--request-timeout=5s"], None).await?;
            Ok::<_, anyhow::Error>((node, metrics))
        };
        let metrics = if FIXTURE_BOUNDARY.try_with(|_| ()).is_ok() {
            let mut metrics = HashMap::new();
            for node in nodes {
                let (node, sample) = scrape(node).await?;
                metrics.insert(node, sample);
            }
            metrics
        } else {
            join_all(nodes.into_iter().map(scrape))
                .await
                .into_iter()
                .collect::<Result<HashMap<_, _>>>()?
        };
        let samples = expected
            .iter()
            .map(|target| {
                parse_aks_cadvisor_cpu_quota(
                    metrics
                        .get(target.node.as_str())
                        .context("collector node metrics missing")?,
                    &config.namespace,
                    target,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let after_readback = aks_deployment_resource_readback(
            config,
            "collector",
            &config.dfe_deployment,
            &config.dfe_container,
            &requested,
        )
        .await?;
        let after = aks_collector_quota_targets(&[after_readback], expected.len())?;
        verify_aks_quota_identities(expected, &after)?;
        verify_aks_cpu_quota_samples(requested_millicores, &samples, expected.len())?;
        Ok::<_, anyhow::Error>(json!({
            "status": "verified", "source": "kubelet cAdvisor container_spec_cpu_quota/period",
            "started_at_unix_ms": started, "completed_at_unix_ms": unix_time_ms_u64()?,
            "containers": samples,
        }))
    };
    tokio::time::timeout(Duration::from_millis(AKS_RESOURCE_COUNTER_SNAPSHOT_TIMEOUT_MS), capture)
        .await
        .context("AKS effective CPU quota snapshot deadline exceeded")?
}

fn aks_cpu_quota_window(
    config: &OatConfig,
    expected: &[AksResourcePodTarget],
    start: u64,
    end: u64,
    before: Value,
    after: Value,
) -> Value {
    let validate = || -> Result<Vec<Value>> {
        ensure!(end > start, "AKS CPU quota measurement window is invalid");
        let requested = config
            .effective_aks_resources()
            .collector
            .limits
            .cpu
            .as_deref()
            .map(parse_resource_cpu_millicores)
            .transpose()?;
        let mut boundaries = Vec::new();
        for (boundary, timestamp) in [(&before, start), (&after, end)] {
            ensure!(
                boundary["status"] == "verified",
                "AKS CPU quota boundary unavailable: {boundary}"
            );
            let sampled_at = boundary["started_at_unix_ms"]
                .as_u64()
                .context("quota snapshot timestamp missing")?;
            let completed_at = boundary["completed_at_unix_ms"]
                .as_u64()
                .context("quota completion timestamp missing")?;
            ensure!(
                sampled_at >= timestamp
                    && completed_at >= sampled_at
                    && completed_at - timestamp <= AKS_RESOURCE_COUNTER_SNAPSHOT_TIMEOUT_MS,
                "AKS CPU quota snapshot missed measurement boundary"
            );
            let samples = boundary["containers"]
                .as_array()
                .context("AKS CPU quota containers missing")?;
            verify_aks_cpu_quota_samples(requested, samples, expected.len())?;
            ensure!(
                expected.iter().all(|target| samples
                    .iter()
                    .filter(|sample| {
                        let identity = aks_quota_identity(target);
                        identity
                            .as_object()
                            .is_some_and(|fields| fields.iter().all(|(key, value)| sample[key] == *value))
                    })
                    .count()
                    == 1),
                "AKS CPU quota measurement identity changed"
            );
            boundaries.push(samples);
        }
        let mut deltas = Vec::new();
        for final_sample in boundaries[1] {
            let initial = boundaries[0]
                .iter()
                .find(|sample| sample["container_id"] == final_sample["container_id"])
                .context("AKS CPU quota initial identity missing")?;
            ensure!(
                initial["cgroup_id"] == final_sample["cgroup_id"]
                    && initial["quota_usec"] == final_sample["quota_usec"]
                    && initial["period_usec"] == final_sample["period_usec"],
                "AKS CPU quota cgroup identity or quota changed during measurement"
            );
            let mut delta = json!({
                "container_id": final_sample["container_id"], "pod": final_sample["pod"],
                "uid": final_sample["uid"], "container": final_sample["container"],
                "node": final_sample["node"], "status": "measured",
            });
            for name in ["nr_periods", "nr_throttled", "throttled_usec"] {
                let first = initial["cpu_stat"][name].as_u64().context("AKS CPU counter missing")?;
                let last = final_sample["cpu_stat"][name]
                    .as_u64()
                    .context("AKS CPU counter missing")?;
                delta[name] = json!(
                    last.checked_sub(first)
                        .context("AKS CPU quota measurement counter reset")?
                );
            }
            deltas.push(delta);
        }
        Ok(deltas)
    };
    let (status, containers, reason) = match validate() {
        Ok(deltas) => ("sampled", deltas, None),
        Err(error) => {
            let reason = format!("{error:#}");
            let unavailable = expected
                .iter()
                .map(|target| {
                    let mut sample = aks_quota_identity(target);
                    sample["status"] = json!("unavailable");
                    sample["reason"] = json!(reason);
                    for name in ["nr_periods", "nr_throttled", "throttled_usec"] {
                        sample[name] = Value::Null;
                    }
                    sample
                })
                .collect();
            ("unavailable", unavailable, Some(reason))
        }
    };
    json!({
        "verification_status": if status == "sampled" { "verified" } else { "unavailable" },
        "collector": {
            "source": "platform.resources.collector.limits.cpu",
            "requested_quota": config.effective_aks_resources().collector.limits.cpu.unwrap_or_else(|| "max".to_owned()),
            "readback": after,
        },
        "throttling": {
            "status": status, "source": "kubelet cAdvisor container CPU CFS counters",
            "scope": "measurement_window_boundary_snapshots",
            "window_start_unix_ms": start, "window_end_unix_ms": end,
            "start": before, "end": after, "containers": containers, "reason": reason,
        },
    })
}

async fn aks_cpu_quota_observation(config: &OatConfig, targets: &[AksResourcePodTarget]) -> Value {
    match capture_aks_collector_cpu_quota(config, targets).await {
        Ok(value) => value,
        Err(error) => json!({"status": "unavailable", "reason": format!("{error:#}")}),
    }
}

fn start_aks_cpu_quota_window(
    config: OatConfig,
    targets: Vec<AksResourcePodTarget>,
    start: u64,
    end: u64,
) -> AksCpuQuotaWindow {
    if FIXTURE_BOUNDARY.try_with(|_| ()).is_ok() {
        return AksCpuQuotaWindow::Fixture {
            start,
            end,
            before: None,
            after: None,
        };
    }
    AksCpuQuotaWindow::Background(AksCpuQuotaWindowTask(tokio::spawn(async move {
        let config_ref = &config;
        let targets_ref = &targets;
        let observe = |timestamp: u64| async move {
            let now = unix_time_ms_u64().unwrap_or(u64::MAX);
            tokio::time::sleep(Duration::from_millis(timestamp.saturating_sub(now))).await;
            aks_cpu_quota_observation(config_ref, targets_ref).await
        };
        let before = observe(start).await;
        let after = observe(end).await;
        aks_cpu_quota_window(&config, &targets, start, end, before, after)
    })))
}

fn merge_aks_cpu_quota_evidence(mut legacy: Value, quota: Option<&Value>) -> Value {
    let missing = json!({
        "verification_status": "unavailable",
        "collector": {
            "source": "platform.resources.collector.limits.cpu",
            "requested_quota": legacy["collector"]["limit_cpu"].as_str().unwrap_or("max"),
            "readback": null,
        },
        "throttling": {"status": "unavailable", "reason": "AKS effective CPU quota measurement evidence missing"},
    });
    let quota = quota.unwrap_or(&missing);
    legacy["verification_status"] = quota["verification_status"].clone();
    for field in ["source", "requested_quota", "readback"] {
        legacy["collector"][field] = quota["collector"][field].clone();
    }
    let mut scheduled_load = legacy["throttling"].take();
    if let Some(fields) = scheduled_load.as_object_mut() {
        fields.remove("cpu_quota");
    }
    legacy["throttling"] = quota["throttling"].clone();
    if legacy["throttling"]["source"].is_null() {
        legacy["throttling"]["source"] = json!("kubelet cAdvisor container CPU CFS counters");
    }
    if legacy["throttling"]["scope"].is_null() {
        legacy["throttling"]["scope"] = json!("measurement_window_boundary_snapshots");
    }
    if legacy["throttling"]["containers"].is_null() {
        legacy["throttling"]["containers"] = json!([]);
    }
    for field in ["window_start_unix_ms", "window_end_unix_ms", "start", "end"] {
        if legacy["throttling"].get(field).is_none() {
            legacy["throttling"][field] = Value::Null;
        }
    }
    legacy["throttling"]["scheduled_load"] = scheduled_load;
    legacy
}

#[cfg(test)]
mod aks_effective_cpu_quota_tests {
    use super::*;

    fn target(pod: &str) -> AksResourcePodTarget {
        AksResourcePodTarget {
            role: "collector".to_owned(),
            pod: pod.to_owned(),
            uid: format!("uid-{pod}"),
            container: "dataflow".to_owned(),
            node: "node-a".to_owned(),
            container_id: Some(format!("containerd://cid{pod}")),
        }
    }

    fn metrics(target: &AksResourcePodTarget, quota: i64) -> String {
        let id = format!("/kubepods/pod{}/cri-containerd-cid{}.scope", target.uid, target.pod);
        [
            ("container_spec_cpu_quota", quota.to_string()),
            ("container_spec_cpu_period", "100000".to_owned()),
            ("container_cpu_cfs_periods_total", "100".to_owned()),
            ("container_cpu_cfs_throttled_periods_total", "10".to_owned()),
            ("container_cpu_cfs_throttled_seconds_total", "0.125".to_owned()),
        ]
        .into_iter()
        .map(|(name, value)| {
            format!(
                "{name}{{namespace=\"oat\",pod=\"{}\",container=\"dataflow\",id=\"{id}\"}} {value}\n",
                target.pod,
            )
        })
        .collect()
    }

    fn config() -> Result<OatConfig> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.platform.kind = crate::config::OatMode::Aks;
        config.platform.resources = None;
        Ok(config)
    }

    #[test]
    fn verifies_all_replicas_and_rejects_mismatch_missing_and_unlimited_confusion() -> Result<()> {
        let first = target("a");
        let second = target("b");
        let samples = vec![
            parse_aks_cadvisor_cpu_quota(&metrics(&first, 200000), "oat", &first)?,
            parse_aks_cadvisor_cpu_quota(&metrics(&second, 200000), "oat", &second)?,
        ];
        verify_aks_cpu_quota_samples(Some(2000), &samples, 2)?;
        let mut mismatched = samples.clone();
        mismatched[1]["quota_usec"] = json!(100000);
        assert!(verify_aks_cpu_quota_samples(Some(2000), &mismatched, 2).is_err());
        assert!(verify_aks_cpu_quota_samples(Some(1000), &samples, 2).is_err());
        assert!(verify_aks_cpu_quota_samples(Some(2000), &samples[..1], 2).is_err());
        assert!(parse_aks_cadvisor_cpu_quota("", "oat", &first).is_err());
        let unlimited = parse_aks_cadvisor_cpu_quota(&metrics(&first, -1), "oat", &first)?;
        verify_aks_cpu_quota_samples(None, std::slice::from_ref(&unlimited), 1)?;
        let zero = parse_aks_cadvisor_cpu_quota(&metrics(&first, 0), "oat", &first)?;
        verify_aks_cpu_quota_samples(None, &[zero], 1)?;
        assert!(verify_aks_cpu_quota_samples(Some(2000), &[unlimited], 1).is_err());
        assert!(verify_aks_cpu_quota_samples(None, &samples, 2).is_err());
        Ok(())
    }

    #[test]
    fn rejects_stale_ambiguous_and_incomplete_cadvisor_metrics() {
        let target = target("a");
        let valid = metrics(&target, 200000);
        for invalid in [
            valid.replace("cida", "oldcontainer"),
            valid.replace("poduid-a", "podold-uid"),
            valid.replace("id=", "missing_id="),
            format!("{valid}{valid}"),
            valid.lines().skip(1).collect::<Vec<_>>().join("\n"),
            valid.replace("0.125", "NaN"),
        ] {
            assert!(parse_aks_cadvisor_cpu_quota(&invalid, "oat", &target).is_err());
        }
        for missing in [
            "container_spec_cpu_period",
            "container_cpu_cfs_periods_total",
            "container_cpu_cfs_throttled_periods_total",
            "container_cpu_cfs_throttled_seconds_total",
        ] {
            let incomplete = valid
                .lines()
                .filter(|line| !line.starts_with(missing))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(parse_aks_cadvisor_cpu_quota(&incomplete, "oat", &target).is_err());
        }
    }

    #[test]
    fn fences_pod_node_container_and_replica_identity() {
        let expected = vec![target("a"), target("b")];
        assert!(verify_aks_quota_identities(&expected, &expected).is_ok());
        assert!(verify_aks_quota_identities(&expected, &expected[..1]).is_err());
        for field in ["uid", "node", "container_id"] {
            let mut changed = vec![target("a"), target("b")];
            match field {
                "uid" => changed[0].uid = "recreated".to_owned(),
                "node" => changed[0].node = "rescheduled".to_owned(),
                _ => changed[0].container_id = Some("containerd://restarted".to_owned()),
            }
            assert!(verify_aks_quota_identities(&expected, &changed).is_err());
        }
    }

    struct QuotaFixtureState {
        quotas: Vec<i64>,
        periods: u64,
        throttled: u64,
        seconds: f64,
        omit_quota: bool,
        omit_replica: bool,
        restart_on_scrape: bool,
        restarted: bool,
        requests: Vec<FixtureCommandRequest>,
    }

    impl Default for QuotaFixtureState {
        fn default() -> Self {
            Self {
                quotas: vec![200000, 200000],
                periods: 100,
                throttled: 10,
                seconds: 0.125,
                omit_quota: false,
                omit_replica: false,
                restart_on_scrape: false,
                restarted: false,
                requests: Vec::new(),
            }
        }
    }

    struct QuotaFixtureTransport {
        targets: Vec<AksResourcePodTarget>,
        resources: Value,
        state: Arc<std::sync::Mutex<QuotaFixtureState>>,
    }

    #[async_trait::async_trait]
    impl FixtureCommandTransport for QuotaFixtureTransport {
        async fn exchange(&self, request: &FixtureCommandRequest) -> Result<FixtureCommandResponse> {
            let mut state = self.state.lock().map_err(|error| anyhow::anyhow!("{error}"))?;
            ensure!(
                request.executable == "kubectl",
                "quota fixture attempted non-kubectl execution"
            );
            ensure!(
                request
                    .argv
                    .windows(2)
                    .any(|pair| pair == ["--server", "https://offline.invalid"]),
                "quota command escaped its fixture authority"
            );
            ensure!(
                request.request_sha256 == request.digest()?,
                "quota fixture request digest differs"
            );
            let stdout = if request.argv.iter().any(|arg| arg == "deployment") {
                json!({"spec":{
                    "selector":{"matchLabels":{"app":"collector"}},
                    "template":{"spec":{"containers":[{"name":"dataflow","resources":self.resources}]}},
                }})
                .to_string()
            } else if request.argv.iter().any(|arg| arg == "pods") {
                let pods = self.targets.iter().enumerate()
                    .filter(|(index, _)| !state.omit_replica || *index == 0)
                    .map(|(index, target)| json!({
                        "metadata":{"name":target.pod,"namespace":"oat","uid":target.uid},
                        "spec":{"nodeName":target.node,"containers":[{"name":"dataflow","resources":self.resources}]},
                        "status":{"containerStatuses":[{"name":"dataflow",
                            "containerID": if state.restarted && index == 1 {
                                Some("containerd://restarted")
                            } else { target.container_id.as_deref() }}]},
                    })).collect::<Vec<_>>();
                json!({"items":pods}).to_string()
            } else if request.argv.iter().any(|arg| arg == "--raw") {
                let path = request
                    .argv
                    .iter()
                    .find(|arg| arg.starts_with("/api/v1/nodes/"))
                    .context("quota fixture omitted node path")?;
                let mut output = String::new();
                for (index, target) in self
                    .targets
                    .iter()
                    .enumerate()
                    .filter(|(_, target)| *path == format!("/api/v1/nodes/{}/proxy/metrics/cadvisor", target.node))
                {
                    let text = metrics(target, state.quotas[index])
                        .replace("} 100\n", &format!("}} {}\n", state.periods))
                        .replace("} 10\n", &format!("}} {}\n", state.throttled))
                        .replace("} 0.125\n", &format!("}} {}\n", state.seconds));
                    if state.omit_quota && index == 1 {
                        output.push_str(
                            &text
                                .lines()
                                .filter(|line| !line.starts_with("container_spec_cpu_quota"))
                                .map(|line| format!("{line}\n"))
                                .collect::<String>(),
                        );
                    } else {
                        output.push_str(&text);
                    }
                }
                state.restarted |= state.restart_on_scrape;
                output
            } else {
                bail!("unexpected quota fixture command: {:?}", request.argv);
            };
            state.requests.push(request.clone());
            Ok(FixtureCommandResponse {
                schema_version: 1,
                kind: "command_response".to_owned(),
                request_id: request.request_id,
                request_sha256: request.request_sha256.clone(),
                exit_code: 0,
                stdout,
                stderr: String::new(),
                truncated: false,
            })
        }
    }

    struct QuotaFixture {
        config: OatConfig,
        targets: Vec<AksResourcePodTarget>,
        state: Arc<std::sync::Mutex<QuotaFixtureState>>,
        boundary: FixtureBoundary,
    }

    fn quota_fixture() -> Result<QuotaFixture> {
        let mut config = config()?;
        config.namespace = "oat".to_owned();
        config.dfe_deployment = "collector".to_owned();
        config.dfe_container = "dataflow".to_owned();
        let mut targets = vec![target("a"), target("b")];
        targets[1].node = "node-b".to_owned();
        let state = Arc::new(std::sync::Mutex::new(QuotaFixtureState::default()));
        let transport = Arc::new(QuotaFixtureTransport {
            targets: targets.clone(),
            resources: serde_json::to_value(config.effective_aks_resources().collector)?,
            state: Arc::clone(&state),
        });
        let boundary = FixtureBoundary {
            authority: FixtureAuthority {
                campaign_id: "quota-fixture".to_owned(),
                cell_run_id: "quota-fixture".to_owned(),
                owner_id: "quota-fixture".to_owned(),
                fencing_token: 1,
                namespace: "oat".to_owned(),
                server: "https://offline.invalid".to_owned(),
                context: "offline".to_owned(),
            },
            deadline_unix_ms: unix_time_ms_u64()? + 30_000,
            sequence: std::sync::atomic::AtomicU64::new(1),
            transport,
            responses: tokio::sync::Mutex::new(Vec::new()),
        };
        Ok(QuotaFixture {
            config,
            targets,
            state,
            boundary,
        })
    }

    #[tokio::test]
    async fn fake_kubelet_capture_fences_every_replica_and_fails_preflight_closed() -> Result<()> {
        let QuotaFixture {
            config,
            targets,
            state,
            boundary,
        } = quota_fixture()?;
        FIXTURE_BOUNDARY
            .scope(boundary, async {
                let sample = capture_aks_collector_cpu_quota(&config, &targets).await?;
                assert_eq!(sample["status"], "verified");
                assert_eq!(sample["containers"].as_array().map(Vec::len), Some(2));
                assert_eq!(sample["containers"][1]["limit_millicores"], 2000.0);
                {
                    let state = state.lock().map_err(|error| anyhow::anyhow!("{error}"))?;
                    assert_eq!(
                        state
                            .requests
                            .iter()
                            .filter(|request| request.argv.iter().any(|arg| arg == "pods"))
                            .count(),
                        2
                    );
                    assert_eq!(
                        state
                            .requests
                            .iter()
                            .filter(|request| request.argv.iter().any(|arg| arg == "--raw"))
                            .count(),
                        2
                    );
                }
                for fault in ["mismatch", "missing_quota", "missing_replica", "restart"] {
                    {
                        let mut state = state.lock().map_err(|error| anyhow::anyhow!("{error}"))?;
                        state.quotas[1] = if fault == "mismatch" { 100000 } else { 200000 };
                        state.omit_quota = fault == "missing_quota";
                        state.omit_replica = fault == "missing_replica";
                        state.restart_on_scrape = fault == "restart";
                    }
                    assert!(
                        capture_aks_collector_cpu_quota(&config, &targets).await.is_err(),
                        "{fault}"
                    );
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
    }

    #[tokio::test]
    async fn inline_fixture_window_retains_fake_authority_and_rechecks_both_boundary_quotas() -> Result<()> {
        for fault in ["none", "start_mismatch", "end_mismatch"] {
            let QuotaFixture {
                config,
                targets,
                state,
                boundary,
            } = quota_fixture()?;
            FIXTURE_BOUNDARY
                .scope(boundary, async {
                    let preflight = capture_aks_collector_cpu_quota(&config, &targets).await?;
                    let start = unix_time_ms_u64()?;
                    let end = start + 25;
                    let mut window = start_aks_cpu_quota_window(config.clone(), targets.clone(), start, end);
                    assert!(matches!(&window, AksCpuQuotaWindow::Fixture { .. }));
                    if fault == "start_mismatch" {
                        state.lock().map_err(|error| anyhow::anyhow!("{error}"))?.quotas[1] = 100000;
                    }
                    window.poll(&config, &targets).await;
                    {
                        let mut state = state.lock().map_err(|error| anyhow::anyhow!("{error}"))?;
                        state.quotas[1] = if fault == "end_mismatch" { 100000 } else { 200000 };
                        state.periods = 200;
                        state.throttled = 20;
                        state.seconds = 0.250;
                    }
                    tokio::time::sleep(Duration::from_millis(end.saturating_sub(unix_time_ms_u64()?))).await;
                    let evidence = window
                        .finish(&config, &targets, &json!({"collector":{"readback":preflight}}))
                        .await;
                    assert_eq!(
                        evidence["verification_status"],
                        if fault == "none" { "verified" } else { "unavailable" }
                    );
                    if fault == "none" {
                        assert_eq!(evidence["throttling"]["containers"][1]["nr_periods"], 100);
                        assert_eq!(evidence["throttling"]["containers"][1]["nr_throttled"], 10);
                        assert_eq!(evidence["throttling"]["containers"][1]["throttled_usec"], 125000);
                        assert_eq!(
                            FIXTURE_BOUNDARY.with(|boundary| boundary
                                .responses
                                .try_lock()
                                .map(|responses| responses.len())
                                .ok()),
                            Some(18)
                        );
                    }
                    Ok::<_, anyhow::Error>(())
                })
                .await?;
        }
        Ok(())
    }

    #[test]
    fn measures_window_deltas_and_fails_closed_on_boundary_loss_or_resets() -> Result<()> {
        let config = config()?;
        let targets = vec![target("a"), target("b")];
        let samples = targets
            .iter()
            .map(|target| parse_aks_cadvisor_cpu_quota(&metrics(target, 200000), "oat", target))
            .collect::<Result<Vec<_>>>()?;
        let before = json!({
            "status": "verified", "started_at_unix_ms": 1000, "completed_at_unix_ms": 1001,
            "containers": samples,
        });
        let mut after = before.clone();
        after["started_at_unix_ms"] = json!(2000);
        after["completed_at_unix_ms"] = json!(2001);
        for index in 0..2 {
            after["containers"][index]["cpu_stat"] = json!({
                "nr_periods": 130, "nr_throttled": 15, "throttled_usec": 175000,
            });
        }
        let evidence = aks_cpu_quota_window(&config, &targets, 1000, 2000, before.clone(), after.clone());
        assert_eq!(evidence["verification_status"], "verified");
        assert_eq!(evidence["throttling"]["containers"].as_array().map(Vec::len), Some(2));
        assert_eq!(evidence["throttling"]["containers"][1]["nr_periods"], 30);
        assert_eq!(evidence["throttling"]["containers"][1]["nr_throttled"], 5);
        assert_eq!(evidence["throttling"]["containers"][1]["throttled_usec"], 50000);
        for field in ["nr_periods", "nr_throttled", "throttled_usec"] {
            let mut reset = after.clone();
            reset["containers"][1]["cpu_stat"][field] = json!(0);
            assert_eq!(
                aks_cpu_quota_window(&config, &targets, 1000, 2000, before.clone(), reset)["verification_status"],
                "unavailable"
            );
        }
        for invalid in [
            json!({"status": "unavailable", "reason": "forbidden"}),
            {
                let mut changed = after.clone();
                changed["containers"][0]["uid"] = json!("new-uid");
                changed
            },
            {
                let mut changed = after.clone();
                changed["containers"][1]["cgroup_id"] = json!("changed-cgroup");
                changed
            },
            {
                let mut changed = after.clone();
                changed["containers"][1]["quota_usec"] = json!(400000);
                changed["containers"][1]["period_usec"] = json!(200000);
                changed
            },
            {
                let mut late = after.clone();
                late["completed_at_unix_ms"] = json!(2001 + AKS_RESOURCE_COUNTER_SNAPSHOT_TIMEOUT_MS);
                late
            },
        ] {
            assert_eq!(
                aks_cpu_quota_window(&config, &targets, 1000, 2000, before.clone(), invalid)["verification_status"],
                "unavailable"
            );
        }
        let legacy = json!({"collector":{"requested_workers":8}, "throttling":{
            "status":"measured", "scope":"scheduled_load_to_client_completion", "pods":[{"periods":50}],
        }});
        let merged = merge_aks_cpu_quota_evidence(legacy.clone(), Some(&evidence));
        assert_eq!(merged["throttling"]["scheduled_load"], legacy["throttling"]);
        assert_eq!(merged["throttling"]["status"], "sampled");
        assert_eq!(merged["throttling"]["scope"], "measurement_window_boundary_snapshots");
        assert_eq!(merged["throttling"]["source"], evidence["throttling"]["source"]);
        assert_eq!(merged["throttling"]["containers"], evidence["throttling"]["containers"]);
        assert_eq!(merged["collector"]["source"], "platform.resources.collector.limits.cpu");
        assert_eq!(
            merged["collector"]["readback"]["containers"][0]["cpu_max"],
            "200000 100000"
        );
        let missing = merge_aks_cpu_quota_evidence(legacy.clone(), None);
        assert_eq!(missing["verification_status"], "unavailable");
        assert_eq!(missing["throttling"]["status"], "unavailable");
        assert_eq!(missing["throttling"]["scope"], "measurement_window_boundary_snapshots");
        assert_eq!(missing["throttling"]["scheduled_load"], legacy["throttling"]);
        Ok(())
    }
}
