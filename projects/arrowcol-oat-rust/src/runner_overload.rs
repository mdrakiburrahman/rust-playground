const GUARD_WINDOW_MS: u64 = 30_000;
const GUARD_MAX_GAP_MS: u64 = 5_000;

#[derive(Clone, Debug, serde::Serialize)]
struct GuardCap {
    metric: &'static str,
    observed_value: u64,
    cap: u64,
    confirmed: bool,
}

struct GuardReading {
    request_start_ms: u64,
    timestamp_ms: u64,
    caps: Vec<GuardCap>,
    commits: Vec<CommitEvent>,
}

struct GuardEngine {
    settings: crate::config::OverloadGuardConfig,
    start: Option<u64>,
    end: Option<u64>,
    last_sample: Option<u64>,
    window_index: u64,
    window_outstanding: bool,
    zero_windows: u32,
    events: BTreeMap<(String, u64), CommitEvent>,
    proof: Value,
    stop: Option<Value>,
}

impl GuardEngine {
    fn new(settings: crate::config::OverloadGuardConfig) -> Self {
        Self {
            proof: json!({"status":"monitoring","settings":settings,"sample_count":0,"peak_cap_fraction":0.0,
                "max_sampling_gap_ms":0,"complete_windows":[],"disk":{"status":"not_applicable","reason":"memory_only"}}),
            settings,
            start: None,
            end: None,
            last_sample: None,
            window_index: 0,
            window_outstanding: true,
            zero_windows: 0,
            events: BTreeMap::new(),
            stop: None,
        }
    }

    fn arm(&mut self, start: u64, end: u64) {
        self.start = Some(start);
        self.end = Some(end);
        self.proof["run_start_ms"] = json!(start);
        self.proof["measurement_end_ms"] = json!(end);
        self.last_sample = None;
        self.proof["coverage_start_ms"] = Value::Null;
        self.proof["coverage_end_ms"] = Value::Null;
    }

    fn observe(&mut self, reading: GuardReading, pending: &PublicationBacklogSample) -> Result<()> {
        if reading.caps.len() != 3 || reading.caps.iter().any(|cap| !cap.confirmed || cap.cap == 0) {
            bail!("required hard-cap protection is unavailable");
        }
        let outstanding = match (
            pending.available,
            pending.publications,
            pending.rows,
            pending.logical_bytes,
        ) {
            (Some(true), Some(publications), Some(rows), Some(bytes)) => publications > 0 || rows > 0 || bytes > 0,
            _ => bail!("publication backlog protection is unavailable"),
        };
        if reading.request_start_ms > reading.timestamp_ms {
            bail!("overload observation clock reversed");
        }
        if self.last_sample.is_none() && self.start.is_some_and(|start| reading.request_start_ms > start) {
            bail!("overload coverage did not begin before warm-up");
        }
        if let Some(previous) = self.last_sample {
            let gap = reading
                .request_start_ms
                .checked_sub(previous)
                .context("overload sample clock reversed")?;
            if self.start.is_some_and(|start| reading.request_start_ms >= start) && gap > GUARD_MAX_GAP_MS {
                bail!("overload protection has a sampling gap");
            }
            self.proof["max_sampling_gap_ms"] = json!(
                self.proof["max_sampling_gap_ms"]
                    .as_u64()
                    .context("guard gap counter missing")?
                    .max(gap)
            );
        }
        self.last_sample = Some(reading.request_start_ms);
        self.proof["sample_count"] = json!(
            self.proof["sample_count"]
                .as_u64()
                .context("guard sample counter missing")?
                + 1
        );
        self.proof["last_caps"] = json!(reading.caps);
        self.proof["last_backlog"] = json!(pending);
        self.proof["coverage_end_ms"] = json!(reading.request_start_ms);
        if self.proof["coverage_start_ms"].is_null() {
            self.proof["coverage_start_ms"] = json!(reading.request_start_ms);
        }
        for cap in &reading.caps {
            let fraction = cap.observed_value as f64 / cap.cap as f64;
            self.proof["peak_cap_fraction"] = json!(
                self.proof["peak_cap_fraction"]
                    .as_f64()
                    .context("guard peak fraction missing")?
                    .max(fraction)
            );
            if fraction >= self.settings.cap_fraction {
                self.stop = Some(json!({
                    "classification":"above_ceiling","reason":"hard_cap_fraction","metric":cap.metric,"observed_value":cap.observed_value,
                    "cap":cap.cap,"fraction":fraction,"threshold_fraction":self.settings.cap_fraction,
                    "window_index":self.window_index,"timestamp_ms":reading.timestamp_ms,
                }));
                self.proof["status"] = json!("triggered");
                return Ok(());
            }
        }
        for event in reading.commits {
            let key = (
                event.table_id.clone().context("guard commit table identity missing")?,
                event.delta_version.context("guard commit version missing")?,
            );
            if let Some(previous) = self.events.insert(key, event.clone())
                && previous != event
            {
                bail!("conflicting durable ingestion commit identity");
            }
        }
        let Some(start) = self.start else {
            return Ok(());
        };
        if reading.request_start_ms < start {
            self.window_outstanding = outstanding;
            return Ok(());
        }
        let through = reading
            .request_start_ms
            .min(self.end.context("guard measurement end missing")?);
        while through >= start + (self.window_index + 1) * GUARD_WINDOW_MS {
            let window_start = start + self.window_index * GUARD_WINDOW_MS;
            let window_end = window_start + GUARD_WINDOW_MS;
            let commits = self
                .events
                .values()
                .filter(|event| {
                    event
                        .finished_at_unix_ms
                        .is_some_and(|finished| finished >= window_start as i64 && finished < window_end as i64)
                })
                .count();
            let covered = self.proof["coverage_start_ms"]
                .as_u64()
                .is_some_and(|first| first <= window_start);
            let outstanding_window = self.window_outstanding && outstanding;
            self.zero_windows = if covered && outstanding_window && commits == 0 {
                self.zero_windows + 1
            } else {
                0
            };
            self.proof["complete_windows"]
                .as_array_mut()
                .context("guard window evidence missing")?
                .push(json!({
                    "index":self.window_index,"start_ms":window_start,"end_ms":window_end,
                    "ingestion_commits":commits,"outstanding":outstanding_window,"covered":covered,
                }));
            if self.zero_windows >= self.settings.zero_progress_windows {
                self.stop = Some(json!({
                    "classification":"above_ceiling","reason":"zero_ingestion_commit_progress","metric":"new_ingestion_commits","observed_value":0,
                    "cap":null,"fraction":null,"window_index":self.window_index,"timestamp_ms":reading.timestamp_ms,
                    "zero_progress_windows":self.zero_windows,
                }));
                self.proof["status"] = json!("triggered");
                return Ok(());
            }
            self.window_index += 1;
            self.window_outstanding = outstanding;
        }
        self.window_outstanding &= outstanding;
        Ok(())
    }

    fn unavailable(&mut self, reason: &str) {
        self.proof["last_confirmed_caps"] = self.proof["last_caps"].take();
        self.proof["last_caps"] = json!(
            [
                "container_memory_bytes",
                "collector_rss_bytes",
                "collector_buffered_bytes"
            ]
            .map(|metric| json!({"metric":metric,"confirmed":false,"observed_value":null,"cap":null}))
        );
        self.proof["status"] = json!("unavailable");
        self.proof["unavailable_reason"] = json!(reason);
    }

    fn record(&mut self, reading: Result<GuardReading>, pending: &PublicationBacklogSample) {
        if let Err(error) = reading.and_then(|reading| self.observe(reading, pending)) {
            tracing::error!(%error, "overload protection unavailable; stopping the cell");
            self.unavailable("live_cap_backlog_or_commit_evidence_unavailable");
        }
    }

    fn failure(&self) -> Option<String> {
        if let Some(stop) = &self.stop {
            Some(format!(
                "above_ceiling: overload_stop {} ({})",
                stop["reason"], stop["metric"]
            ))
        } else if self.proof["status"] == "unavailable" {
            Some("overload_guard protection unavailable; cell cannot qualify".to_owned())
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ScaleOutCollectorIdentity {
    pod: String,
    pod_uid: String,
    replica_set_uid: String,
    container_id: String,
}

fn controlling_owner(value: &Value, kind: &str) -> Result<(String, String)> {
    let owners = value.pointer("/metadata/ownerReferences").and_then(Value::as_array)
        .context("Kubernetes owner references missing")?;
    let controllers = owners.iter().filter(|owner| owner["controller"] == true).collect::<Vec<_>>();
    let [owner] = controllers.as_slice() else {
        bail!("Kubernetes object must have exactly one controlling owner");
    };
    if owner["kind"] != kind {
        bail!("Kubernetes controlling owner kind mismatched");
    }
    let field = |key| owner[key].as_str().filter(|value| !value.is_empty())
        .map(str::to_owned).context("Kubernetes controlling owner identity missing");
    Ok((field("name")?, field("uid")?))
}

fn scale_out_collector_identities(
    namespace: &str,
    container: &str,
    expected_replicas: u32,
    deployment: &Value,
    replica_sets: &Value,
    pods: &Value,
) -> Result<Vec<ScaleOutCollectorIdentity>> {
    if expected_replicas != 1 || !rollout_generation_ready(deployment)
        || deployment.pointer("/spec/replicas").and_then(Value::as_u64) != Some(u64::from(expected_replicas))
        || deployment.pointer("/metadata/namespace").and_then(Value::as_str) != Some(namespace)
    {
        bail!("collector deployment does not match the ready expected topology");
    }
    let deployment_name = deployment.pointer("/metadata/name").and_then(Value::as_str)
        .filter(|value| !value.is_empty()).context("collector deployment name missing")?;
    let deployment_uid = deployment.pointer("/metadata/uid").and_then(Value::as_str)
        .filter(|value| !value.is_empty()).context("collector deployment UID missing")?;
    let labels = deployment.pointer("/spec/selector/matchLabels").and_then(Value::as_object)
        .filter(|value| !value.is_empty()).context("collector deployment selector missing")?;
    let mut owned_sets = BTreeMap::new();
    for replica_set in replica_sets["items"].as_array().context("replica set inventory missing")? {
        if replica_set.pointer("/metadata/namespace").and_then(Value::as_str) != Some(namespace) {
            continue;
        }
        if controlling_owner(replica_set, "Deployment").ok().as_ref()
            != Some(&(deployment_name.to_owned(), deployment_uid.to_owned())) {
            continue;
        }
        let name = replica_set.pointer("/metadata/name").and_then(Value::as_str)
            .filter(|value| !value.is_empty()).context("replica set name missing")?;
        let uid = replica_set.pointer("/metadata/uid").and_then(Value::as_str)
            .filter(|value| !value.is_empty()).context("replica set UID missing")?;
        if owned_sets.insert(uid.to_owned(), name.to_owned()).is_some() {
            bail!("duplicate replica set UID");
        }
    }
    let items = pods["items"].as_array().context("collector pod inventory missing")?;
    if items.len() != expected_replicas as usize {
        bail!("collector pod count differs from the selected cell");
    }
    let mut identities = Vec::with_capacity(items.len());
    for pod in items {
        if pod.pointer("/metadata/namespace").and_then(Value::as_str) != Some(namespace)
            || !pod.pointer("/metadata/deletionTimestamp").is_none_or(Value::is_null)
            || !labels.iter().all(|(key, value)| pod.pointer("/metadata/labels").and_then(Value::as_object)
                .and_then(|pod_labels| pod_labels.get(key)) == Some(value))
        {
            bail!("collector pod namespace, selector or lifecycle mismatched");
        }
        let (owner_name, owner_uid) = controlling_owner(pod, "ReplicaSet")?;
        if owned_sets.get(&owner_uid) != Some(&owner_name) {
            bail!("collector pod is not owned by the selected deployment");
        }
        let name = pod.pointer("/metadata/name").and_then(Value::as_str)
            .filter(|value| !value.is_empty()).context("collector pod name missing")?;
        let uid = pod.pointer("/metadata/uid").and_then(Value::as_str)
            .filter(|value| !value.is_empty()).context("collector pod UID missing")?;
        let statuses = pod.pointer("/status/containerStatuses").and_then(Value::as_array)
            .context("collector container statuses missing")?.iter()
            .filter(|status| status["name"] == container).collect::<Vec<_>>();
        let [status] = statuses.as_slice() else { bail!("collector container status is missing or ambiguous") };
        if status["ready"] != true || status["restartCount"].as_u64() != Some(0)
            || !status.pointer("/state/running").is_some_and(Value::is_object) {
            bail!("collector container is not ready or has restarted");
        }
        let container_id = status["containerID"].as_str().filter(|value| !value.is_empty())
            .context("collector container ID missing")?;
        identities.push(ScaleOutCollectorIdentity {
            pod: name.to_owned(), pod_uid: uid.to_owned(), replica_set_uid: owner_uid, container_id: container_id.to_owned(),
        });
    }
    identities.sort();
    if identities.iter().map(|value| &value.pod_uid).collect::<HashSet<_>>().len() != identities.len()
        || identities.iter().map(|value| &value.pod).collect::<HashSet<_>>().len() != identities.len()
        || identities.iter().map(|value| &value.container_id).collect::<HashSet<_>>().len() != identities.len() {
        bail!("collector identities are not unique");
    }
    Ok(identities)
}

struct GuardProbe {
    instance_id: String,
    rss_cap: u64,
    buffer_cap: u64,
    collector_instance: Option<arrowcol_protocol::CollectorInstanceId>,
    expected_scale_out_identity: Option<Vec<ScaleOutCollectorIdentity>>,
    local: bool,
    since_ms: u64,
}

async fn scale_out_collector_snapshot(config: &OatConfig, replicas: u32) -> Result<Vec<ScaleOutCollectorIdentity>> {
    let deployment = deployment(config, &config.dfe_deployment).await?;
    let selector = deployment_selector(&deployment)?;
    let (sets, pods) = tokio::try_join!(
        kubectl(config, vec!["get", "replicasets", "--namespace", &config.namespace, "-o", "json"], None),
        kubectl(config, vec!["get", "pods", "--namespace", &config.namespace, "--selector", &selector, "-o", "json"], None),
    )?;
    scale_out_collector_identities(
        &config.namespace, &config.dfe_container, replicas, &deployment,
        &serde_json::from_str(&sets)?, &serde_json::from_str(&pods)?,
    )
}

struct PreparedGuard {
    scraper: PublicationBacklogScraper,
    probe: GuardProbe,
    engine: GuardEngine,
}

impl PreparedGuard {
    async fn prepare(config: &OatConfig, platform: &dyn Platform) -> Result<Self> {
        if config.tunables.wal.is_some() {
            bail!("WAL disk-cap accounting is unavailable; this guard supports the memory-only campaign");
        }
        let source = platform
            .resource_sampler_source()
            .context("overload resource sampler source unavailable")?;
        let logs = platform.process_logs_for_role(config, ComponentRole::Collector).await?;
        let [log] = logs.as_slice() else {
            bail!("overload guard requires exactly one collector identity");
        };
        if !log.complete {
            bail!("collector cap readback is incomplete");
        }
        let expected_scale_out_identity =
            if config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
                if platform.kind() != crate::config::OatMode::Aks {
                    bail!("scale-out guard requires an AKS collector");
                }
                let expected = scale_out_collector_snapshot(config, 1).await?;
                let [identity] = expected.as_slice() else {
                    bail!("scale-out guard requires exactly one collector identity");
                };
                if log.instance_id != format!("{}/{}", identity.pod, config.dfe_container) {
                    bail!("collector startup evidence does not match the owned collector");
                }
                Some(expected)
            } else {
                None
            };
        let observed = process_configurations(&log.text)?;
        if observed["commit_telemetry"]
            != json!({"schema_version":1,
            "scope":"new_ingestion_addfiles_after_sync","clock":"system_unix_ms"})
        {
            bail!("ingestion-only producer capability unavailable");
        }
        let mut prepared = Self {
            scraper: PublicationBacklogScraper::start(&source, config).await?,
            probe: GuardProbe {
                instance_id: log.instance_id.clone(),
                rss_cap: observed
                    .pointer("/pressure/rss_limit_bytes")
                    .and_then(Value::as_u64)
                    .filter(|cap| *cap > 0)
                    .context("RSS hard-cap readback missing")?,
                buffer_cap: observed
                    .pointer("/pressure/buffered_bytes_limit")
                    .and_then(Value::as_u64)
                    .filter(|cap| *cap > 0)
                    .context("buffer hard-cap readback missing")?,
                collector_instance: None,
                expected_scale_out_identity,
                local: platform.kind() == crate::config::OatMode::Local,
                since_ms: unix_time_ms_u64()?.saturating_sub(GUARD_MAX_GAP_MS),
            },
            engine: GuardEngine::new(config.overload_guard.clone()),
        };
        prepared.engine.proof["requested_container_memory_limit_bytes"] =
            json!(config.platform.local.collector_memory_limit_bytes);
        prepared.engine.proof["collector_rss_limit_source"] = json!("registration_configuration_not_cgroup");
        prepared.engine.proof["commit_log_sampling"] = json!({"mode":"incremental_since_with_dedupe",
            "overlap_ms":GUARD_MAX_GAP_MS,"capability_source":"initial_complete_startup_readback"});
        let (reading, backlog) = tokio::try_join!(
            prepared.probe.read(config, &prepared.scraper),
            prepared.scraper.scrape(),
        )?;
        prepared
            .engine
            .observe(reading, &publication_backlog_sample(backlog, 0, 0, 0))?;
        Ok(prepared)
    }
}

fn guard_container_memory(text: &str, requested: Option<u64>) -> Result<GuardCap> {
    let lines = text.lines().collect::<Vec<_>>();
    let [namespace, used, limit] = lines.as_slice() else {
        bail!("cgroup memory readback is incomplete");
    };
    if *namespace != "0::/" {
        bail!("cgroup accounting is not scoped to the collector container: {namespace}");
    }
    let observed_value = used.parse::<u64>().context("cgroup memory.current is invalid")?;
    let cap = limit
        .parse::<u64>()
        .context("container memory hard cap is unconfirmed or unlimited")?;
    if cap == 0 || cap > i64::MAX as u64 {
        bail!("container memory hard cap is unavailable");
    }
    if let Some(requested) = requested
        && requested != cap
    {
        bail!("container memory hard cap mismatch: requested {requested}, effective {cap}");
    }
    Ok(GuardCap {
        metric: "container_memory_bytes",
        observed_value,
        cap,
        confirmed: true,
    })
}

fn verify_scale_out_buffering_mode(pressure: &Value) -> Result<()> {
    if pressure.get("resolved_buffering_mode").and_then(Value::as_str) != Some("memory_only") {
        bail!("scale-out collector did not affirmatively resolve memory-only buffering");
    }
    Ok(())
}

impl GuardProbe {
    fn command(&self, config: &OatConfig, logs: bool) -> Result<Command> {
        let since = DateTime::from_timestamp_millis(i64::try_from(self.since_ms)?)
            .context("guard log cursor timestamp is invalid")?
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        if self.local {
            let mut command = Command::new("docker");
            if logs {
                command.args(["logs", &format!("--since={since}"), &self.instance_id]);
            } else {
                command.args([
                    "exec",
                    &self.instance_id,
                    "cat",
                    "/proc/self/cgroup",
                    "/sys/fs/cgroup/memory.current",
                    "/sys/fs/cgroup/memory.max",
                ]);
            }
            Ok(command)
        } else {
            let (pod, container) = self
                .instance_id
                .split_once('/')
                .context("guard pod/container identity missing")?;
            let mut command = Command::new("kubectl");
            if let Some(context) = &config.kubectl_context {
                command.args(["--context", context]);
            }
            command.args(["--namespace", &config.namespace]);
            if logs {
                command.args(["logs", pod, "--container", container, &format!("--since-time={since}")]);
            } else {
                command.args([
                    "exec",
                    pod,
                    "--container",
                    container,
                    "--",
                    "cat",
                    "/proc/self/cgroup",
                    "/sys/fs/cgroup/memory.current",
                    "/sys/fs/cgroup/memory.max",
                ]);
            }
            Ok(command)
        }
    }

    async fn read(&mut self, config: &OatConfig, scraper: &PublicationBacklogScraper) -> Result<GuardReading> {
        let request_start_ms = unix_time_ms_u64()?;
        if let Some(expected) = &self.expected_scale_out_identity
            && scale_out_collector_snapshot(config, 1).await? != *expected
        {
            bail!("collector identity changed before observation");
        }
        let mut pressure_url = Url::parse(&scraper.endpoint)?;
        pressure_url.set_path("/v1/pressure");
        pressure_url.set_query(None);
        let memory = run_command(
            self.command(config, false)?,
            "read collector cgroup memory usage and hard cap",
        );
        let logs = filtered_process_command_output(self.command(config, true)?, "read live ingestion commit evidence");
        let pressure = scraper.read_endpoint(pressure_url.as_str());
        let (memory, logs, pressure) = tokio::try_join!(memory, logs, pressure)?;
        let memory = String::from_utf8(memory.stdout)?;
        let container_cap = guard_container_memory(
            &memory,
            if self.local {
                config.platform.local.collector_memory_limit_bytes
            } else {
                None
            },
        )?;
        let pressure: Value = serde_json::from_str(&pressure)?;
        if config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
            verify_scale_out_buffering_mode(&pressure)?;
        }
        let rich: arrowcol_routing::RichPressureSnapshot = serde_json::from_value(pressure["rich"].clone())?;
        let timestamp_ms = unix_time_ms_u64()?;
        let observed_ms = rich.observed_at_unix_ms.get();
        if observed_ms < request_start_ms
            || observed_ms > timestamp_ms
            || rich.rss_limit_bytes.get() != self.rss_cap
            || rich.buffered_bytes.limit().get() != self.buffer_cap
            || self.collector_instance.is_some_and(|id| id != rich.instance_id)
            || pressure["native_quiver_enabled"] != false
        {
            bail!("collector pressure identity, clock or cap accounting mismatched");
        }
        self.collector_instance = Some(rich.instance_id);
        let rss = pressure["actual_rss_bytes"]
            .as_u64()
            .filter(|value| *value > 0)
            .context("actual collector RSS unavailable")?;
        let text = format!("{}\n{}", logs.stdout, logs.stderr);
        if [&logs.stdout, &logs.stderr]
            .iter()
            .any(|stream| !stream.is_empty() && !stream.ends_with('\n'))
        {
            bail!("incremental collector log ends in an incomplete record");
        }
        let allowed = config
            .tables
            .iter()
            .map(|table| canonical_dmv_table_name(table))
            .collect::<Result<HashSet<_>>>()?;
        let mut commits = Vec::new();
        for line in text
            .lines()
            .filter_map(|line| line.strip_prefix("arrowcol_data_commit "))
        {
            let event: CommitEvent = serde_json::from_str(line)?;
            if event.schema_version != 1
                || event.outcome != "committed"
                || event
                    .table_id
                    .as_ref()
                    .is_none_or(|id| id.len() != 64 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()))
                || event.delta_version.is_none()
                || event.rows.is_none_or(|rows| rows == 0)
                || event
                    .finished_at_unix_ms
                    .is_none_or(|time| time < 0 || time as u64 > timestamp_ms)
            {
                bail!("durable ingestion commit identity or outcome is unproven");
            }
            if event.table_name.as_ref().is_some_and(|table| allowed.contains(table)) {
                commits.push(event);
            } else {
                bail!("live ingestion commit table identity mismatched");
            }
        }
        if let Some(expected) = &self.expected_scale_out_identity
            && scale_out_collector_snapshot(config, 1).await? != *expected
        {
            bail!("collector identity changed during observation");
        }
        self.since_ms = request_start_ms.saturating_sub(GUARD_MAX_GAP_MS);
        Ok(GuardReading {
            request_start_ms,
            timestamp_ms,
            commits,
            caps: vec![
                container_cap,
                GuardCap {
                    metric: "collector_rss_bytes",
                    observed_value: rss,
                    cap: self.rss_cap,
                    confirmed: true,
                },
                GuardCap {
                    metric: "collector_buffered_bytes",
                    observed_value: rich.buffered_bytes.current().get(),
                    cap: self.buffer_cap,
                    confirmed: true,
                },
            ],
        })
    }
}

#[derive(Clone)]
enum GuardSignal {
    Disabled,
    Monitoring,
    Complete,
    Stop(String),
}

#[cfg(test)]
mod overload_guard_tests {
    use super::*;

    #[test]
    fn scale_out_collector_set_requires_complete_owned_unrestarted_pods() -> Result<()> {
        let deployment = json!({
            "metadata":{"name":"collector","namespace":"run","uid":"deployment","generation":1},
            "spec":{"replicas":1,"selector":{"matchLabels":{"app":"collector"}}},
            "status":{"observedGeneration":1,"updatedReplicas":1,"replicas":1,"availableReplicas":1}
        });
        let replica_sets = json!({"items":[{
            "metadata":{"name":"rs","namespace":"run","uid":"rs-uid","ownerReferences":[
                {"kind":"Deployment","name":"collector","uid":"deployment","controller":true}
            ]}
        }]});
        let pod = |index| json!({
            "metadata":{"name":format!("pod-{index}"),"namespace":"run","uid":format!("uid-{index}"),
                "labels":{"app":"collector"},"ownerReferences":[
                    {"kind":"ReplicaSet","name":"rs","uid":"rs-uid","controller":true}
                ]},
            "status":{"containerStatuses":[{"name":"dataflow","ready":true,"restartCount":0,
                "containerID":format!("container-{index}"),"state":{"running":{}}}]}
        });
        let pods = json!({"items":[pod(0)]});
        let identities = scale_out_collector_identities("run", "dataflow", 1, &deployment, &replica_sets, &pods)?;
        assert_eq!(identities.len(), 1);
        assert!(scale_out_collector_identities("run", "dataflow", 2, &deployment, &replica_sets, &pods).is_err());
        assert_eq!(identities[0].pod_uid, "uid-0");
        let mut missing = pods.clone();
        missing["items"].as_array_mut().context("missing pod array")?.pop();
        assert!(scale_out_collector_identities("run", "dataflow", 1, &deployment, &replica_sets, &missing).is_err());
        let mut extra = pods.clone();
        extra["items"].as_array_mut().context("missing pod array")?.push(pod(2));
        assert!(scale_out_collector_identities("run", "dataflow", 1, &deployment, &replica_sets, &extra).is_err());
        for (pointer, value) in [
            ("/items/0/status/containerStatuses/0/restartCount", json!(1)),
            ("/items/0/status/containerStatuses/0/containerID", Value::Null),
            ("/items/0/metadata/ownerReferences/0/uid", json!("foreign")),
            ("/items/0/metadata/labels/app", json!("foreign")),
        ] {
            let mut bad = pods.clone();
            *bad.pointer_mut(pointer).context("missing test field")? = value;
            assert!(scale_out_collector_identities("run", "dataflow", 1, &deployment, &replica_sets, &bad).is_err());
        }
        let mut restarted = pods.clone();
        restarted["items"][0]["status"]["containerStatuses"][0]["containerID"] = json!("replacement");
        assert_ne!(identities, scale_out_collector_identities("run", "dataflow", 1, &deployment, &replica_sets, &restarted)?);
        Ok(())
    }

    #[test]
    fn overload_cgroup_accounting_and_incremental_cursor_refuse_invented_limits() -> Result<()> {
        assert_eq!(guard_container_memory("0::/\n100\n1000\n", Some(1000))?.cap, 1000);
        for text in ["0::/\n100\nmax\n", "0::/host\n100\n1000\n", "0::/\n100\n999\n"] {
            assert!(guard_container_memory(text, Some(1000)).is_err());
        }
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let mut probe = GuardProbe {
            instance_id: "collector".to_owned(),
            rss_cap: 1000,
            buffer_cap: 1000,
            collector_instance: None,
            expected_scale_out_identity: None,
            local: true,
            since_ms: 595_000,
        };
        let args = probe
            .command(&config, true)?
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(args.contains(&"--since=1970-01-01T00:09:55.000Z".to_owned()));
        assert!(!args.iter().any(|arg| arg.contains("--tail")));
        probe.local = false;
        probe.instance_id = "pod/collector".to_owned();
        let args = probe
            .command(&config, true)?
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(args.contains(&"--since-time=1970-01-01T00:09:55.000Z".to_owned()));
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_endpoint_refresh_after_scaling_never_reuses_previous_bindings() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("oat-endpoint-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        fs::create_dir(root.join("bin"))?;
        let result = async {
            let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/endpoint-refresh.ts");
            let docker = root.join("bin/docker");
            fs::write(
                &docker,
                format!("#!/bin/sh\nexec node --import tsx '{}' \"$@\"\n", script.display()),
            )?;
            fs::set_permissions(docker, fs::Permissions::from_mode(0o700))?;
            let before = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let after = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let old = before.local_addr()?.port();
            let new = after.local_addr()?.port();
            let mut state = json!({"scaled":false,"before":{"router":old,"dataflow-1":old,"kusto":old},
                "after":{"router":new,"dataflow-1":new,"kusto":new}});
            fs::write(root.join("state.json"), state.to_string())?;
            let config: OatConfig = serde_json::from_str(include_str!(
                "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
            ))?;
            let mut paths = vec![root.join("bin")];
            paths.extend(std::env::split_paths(
                &std::env::var_os("PATH").context("fixture PATH")?,
            ));
            let mut platform = LocalPlatform {
                run_id: uuid::Uuid::new_v4().to_string(),
                state_dir: root.clone(),
                control_dir: root.clone(),
                environment: HashMap::from([
                    (
                        "PATH".to_owned(),
                        std::env::join_paths(paths)?
                            .into_string()
                            .map_err(|_| anyhow::anyhow!("invalid fixture PATH"))?,
                    ),
                    (
                        "OAT_ENDPOINT_FIXTURE_ROOT".to_owned(),
                        root.to_str().context("fixture path")?.to_owned(),
                    ),
                ]),
                client_progress: std::sync::Mutex::new(HashMap::new()),
                router_metrics_url: None,
                collector_metrics_url: None,
                qap_url: None,
                daemon_endpoint: Some("unix:///var/run/docker.sock".to_owned()),
                shutdown_drain_evidence: None,
                maintenance_cell_baseline: Vec::new(),
            };
            platform.router_metrics_url =
                Some(local_compose_endpoint(&config, &platform, "router", "50050", "/metrics").await?);
            platform.collector_metrics_url =
                Some(local_compose_endpoint(&config, &platform, "dataflow-1", "32011", "/metrics").await?);
            platform.qap_url = Some(local_compose_endpoint(&config, &platform, "kusto", "8080", "").await?);
            let previous = platform.collector_metrics_url.clone();
            platform.scale_arrowcols(&config, 1, 1).await?;
            assert_ne!(
                platform.collector_metrics_url, previous,
                "scale/recreate must rediscover published bindings"
            );
            assert_eq!(
                platform.collector_metrics_url,
                Some(format!("http://127.0.0.1:{new}/metrics"))
            );
            assert_eq!(
                platform.router_metrics_url,
                Some(format!("http://127.0.0.1:{new}/metrics"))
            );
            assert_eq!(platform.qap_url, Some(format!("http://127.0.0.1:{new}")));
            let Some(ResourceSamplerSource::Local {
                collector_metrics_url, ..
            }) = platform.resource_sampler_source()
            else {
                bail!("local sampler source missing");
            };
            assert_eq!(collector_metrics_url, platform.collector_metrics_url);

            state["scaled"] = json!(true);
            state["fail"] = json!("dataflow-1");
            fs::write(root.join("state.json"), state.to_string())?;
            platform.scale_arrowcols(&config, 1, 1).await?;
            assert!(
                platform.collector_metrics_url.is_none(),
                "failed rediscovery must discard the previous collector URL"
            );
            let source = platform.resource_sampler_source().context("sampler source missing")?;
            let error = PublicationBacklogScraper::start(&source, &config)
                .await
                .err()
                .context("missing endpoint admitted")?;
            assert!(format!("{error:#}").contains("metrics endpoint is unavailable"));

            state["fail"] = json!("kusto");
            fs::write(root.join("state.json"), state.to_string())?;
            platform.scale_arrowcols(&config, 1, 1).await?;
            assert!(
                platform.qap_url.is_none(),
                "optional downstream failure must not retain an old URL"
            );
            assert!(platform.collector_metrics_url.is_some());

            state["fail"] = json!("router");
            fs::write(root.join("state.json"), state.to_string())?;
            assert!(
                platform.scale_arrowcols(&config, 1, 1).await.is_err(),
                "required router rediscovery must fail"
            );
            assert!(platform.router_metrics_url.is_none());
            assert!(platform.collector_metrics_url.is_none());
            assert!(platform.qap_url.is_none());
            let commands = fs::read_to_string(root.join("commands"))?;
            assert!(!commands.contains("oat-client"));
            Ok::<(), anyhow::Error>(())
        }
        .await;
        fs::remove_dir_all(&root)?;
        result
    }

    #[tokio::test]
    #[ignore = "requires authorized no-load setup/recreate/pin/maintenance admission with cached component images"]
    async fn local_endpoint_actual_docker_lifecycle_without_clients() -> Result<()> {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize()?;
        if std::env::var_os("OAT_ENDPOINT_LIFECYCLE_CHILD").is_none() {
            let mut command = Command::new(std::env::current_exe()?);
            command
                .current_dir(&workspace)
                .env("OAT_ENDPOINT_LIFECYCLE_CHILD", "1")
                .args([
                    "--exact",
                    "runner::overload_guard_tests::local_endpoint_actual_docker_lifecycle_without_clients",
                    "--ignored",
                    "--nocapture",
                ]);
            tokio::time::timeout(
                Duration::from_secs(900),
                run_streaming_command(command, "bounded no-load local endpoint lifecycle"),
            )
            .await
            .context("no-load lifecycle timed out; inspect its run-owned resources before cleanup")??;
            return Ok(());
        }
        let mut config: OatConfig =
            serde_json::from_slice(&fs::read(std::env::var("OAT_ENDPOINT_LIFECYCLE_CONFIG")?)?)?;
        anyhow::ensure!(
            config.platform.kind == crate::config::OatMode::Local,
            "local config required"
        );
        anyhow::ensure!(
            config.platform.local.collector_memory_limit_bytes == Some(4 * 1024 * 1024 * 1024),
            "this lifecycle proof requires an explicit 4 GiB cap"
        );
        anyhow::ensure!(
            config.platform.local.bind_host.is_some() && config.platform.local.endpoint_host.is_some(),
            "explicit reachable Docker-host binding and endpoint required for the lifecycle proof"
        );
        let run_id = uuid::Uuid::new_v4().to_string();
        let root = workspace
            .join("out/arrowcol-oat")
            .join(format!("endpoint-lifecycle-{run_id}"));
        fs::create_dir(&root)?;
        config.platform.compose_state_root = Some(root.join("state").to_str().context("state path")?.to_owned());
        config.platform.local.snapshot_root = None;
        config.platform.compose_project = Some("oat-endpoint-lifecycle".to_owned());
        config.maintenance_image = Some("arrowcol-oat/maintenance:local".to_owned());
        let mut overlay: serde_yaml::Value = serde_yaml::from_str(&fs::read_to_string(
            config
                .platform
                .compose_override_file
                .as_deref()
                .context("local override missing")?,
        )?)?;
        for service in [
            "sqlserver",
            "database-bootstrap",
            "arrowcol-migrate",
            "arrowcol-catalog",
            "dataflow-1",
            "router",
            "oat-client",
        ] {
            overlay["services"][service]["build"] =
                serde_yaml::Value::Tagged(Box::new(serde_yaml::value::TaggedValue {
                    tag: serde_yaml::value::Tag::new("!reset"),
                    value: serde_yaml::Value::Null,
                }));
        }
        let override_path = root.join("cached-components.yaml");
        fs::write(&override_path, serde_yaml::to_string(&overlay)?)?;
        config.platform.compose_override_file = Some(override_path.to_str().context("override path")?.to_owned());
        let mut platform = LocalPlatform::new(&config, &run_id)?;
        let receipt_path = root.join("receipt.json");
        println!(
            "endpoint_lifecycle_scope {}",
            json!({"run_id":run_id,"receipt":receipt_path,
            "classification":"unscored_no_load_lifecycle","workload_clients":0,"schedule_armed":false})
        );
        let mut receipt = json!({"run_id":run_id,"classification":"unscored_no_load_lifecycle",
            "workload_clients":0,"schedule_armed":false});
        let result = async {
            platform.ensure_stack(&config, &run_id, 1).await?;
            let before = platform
                .process_logs_for_role(&config, ComponentRole::Collector)
                .await?;
            let [before] = before.as_slice() else {
                bail!("initial collector identity ambiguous");
            };
            receipt["before"] = json!({"container_id":before.instance_id,"metrics_url":platform.collector_metrics_url});
            println!("endpoint_lifecycle_before {}", receipt["before"]);
            platform.scale_arrowcols(&config, 1, 1).await?;
            let after = platform
                .process_logs_for_role(&config, ComponentRole::Collector)
                .await?;
            let [after] = after.as_slice() else {
                bail!("scaled collector identity ambiguous");
            };
            receipt["after"] = json!({"container_id":after.instance_id,"metrics_url":platform.collector_metrics_url});
            println!("endpoint_lifecycle_after {}", receipt["after"]);
            anyhow::ensure!(
                before.instance_id != after.instance_id,
                "scale did not recreate collector"
            );
            anyhow::ensure!(
                receipt["before"]["metrics_url"] != receipt["after"]["metrics_url"],
                "mapped port did not change"
            );
            let endpoint = platform
                .collector_metrics_url
                .clone()
                .context("collector endpoint missing after scale")?;
            let status = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()?
                .get(&endpoint)
                .send()
                .await?
                .status();
            anyhow::ensure!(
                status == reqwest::StatusCode::OK,
                "fresh collector metrics returned {status}"
            );
            receipt["metrics_http_status"] = json!(status.as_u16());
            let tables = config.tables.clone();
            let now = unix_time_ms_u64()?;
            platform
                .initialize_run_table_pins(
                    &config,
                    &run_id,
                    &expected_pin_keys_json(&tables)?,
                    now.checked_add(config.run_pin_ttl_ms).context("pin expiry overflow")?,
                    now,
                )
                .await?;
            let baseline = platform.process_logs(&config).await?;
            platform.retain_cell_process_baseline(&baseline);
            let prepared = tokio::time::timeout(Duration::from_secs(5), PreparedGuard::prepare(&config, &platform))
                .await
                .context("guard preparation deadline")??;
            anyhow::ensure!(
                prepared.probe.instance_id == after.instance_id,
                "guard retained stale container identity"
            );
            anyhow::ensure!(prepared.scraper.endpoint == endpoint, "guard retained stale endpoint");
            receipt["guard"] = json!({"status":"prepared","container_id":prepared.probe.instance_id,
                "collector_instance":prepared.probe.collector_instance,"metrics_url":prepared.scraper.endpoint,
                "readback":prepared.engine.proof});
            let mut cgroup = Command::new("docker");
            cgroup.args([
                "exec",
                &after.instance_id,
                "cat",
                "/proc/self/cgroup",
                "/sys/fs/cgroup/memory.current",
                "/sys/fs/cgroup/memory.max",
            ]);
            let cgroup = String::from_utf8(
                run_command(cgroup, "read recreated collector private cgroup")
                    .await?
                    .stdout,
            )?;
            guard_container_memory(&cgroup, Some(4 * 1024 * 1024 * 1024))?;
            receipt["cgroup_readback"] = json!(cgroup);
            platform
                .prepare_campaign_load_admission(&config, &run_id, &tables)
                .await?;
            receipt["policy_preparation"] = json!("passed");
            let maintenance = platform
                .process_logs_for_role(&config, ComponentRole::Maintenance)
                .await?;
            let [maintenance] = maintenance.as_slice() else {
                bail!("maintenance identity ambiguous");
            };
            receipt["maintenance_configuration"] = process_configurations(&maintenance.text)?;
            let policy = maintenance
                .text
                .lines()
                .filter_map(|line| line.strip_prefix("arrowcol_maintenance_table_config "))
                .map(serde_json::from_str::<Value>)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            anyhow::ensure!(!policy.is_empty(), "effective maintenance table readback missing");
            for event in &policy {
                anyhow::ensure!(
                    event.pointer("/config/table_policy/arrowcol.partition.granularity") == Some(&json!("hour")),
                    "effective hour granularity not proved"
                );
                anyhow::ensure!(
                    event.pointer("/config/table_policy/arrowcol.optimize.sealedPartitionsOnly") == Some(&json!(false)),
                    "effective sealed=false not proved"
                );
            }
            receipt["maintenance_table_readback"] = json!(policy);
            fs::write(root.join("maintenance.log"), &maintenance.text)?;
            fs::write(root.join("collector.log"), &after.text)?;
            anyhow::ensure!(
                fs::read_to_string(platform.control_dir.join("schedule"))? == "pending\n",
                "schedule was armed"
            );
            let project =
                scoped_compose_project(config.platform.compose_project.as_deref().context("project")?, &run_id);
            let mut clients = Command::new("docker");
            clients.args(local_oat_client_list_args(&project));
            anyhow::ensure!(
                run_command(clients, "verify no workload clients exist")
                    .await?
                    .stdout
                    .is_empty(),
                "workload clients existed"
            );
            println!("endpoint_lifecycle_prepared {}", receipt);
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = &result {
            receipt["failure"] = json!(format!("{error:#}"));
        }
        fs::write(&receipt_path, serde_json::to_vec_pretty(&receipt)?)?;
        let cleanup = platform.cleanup(&config).await;
        match cleanup {
            Ok(()) => receipt["cleanup"] = platform.shutdown_drain_evidence().context("cleanup evidence missing")?,
            Err(error) => {
                receipt["cleanup_error"] = json!(format!("{error:#}"));
                fs::write(&receipt_path, serde_json::to_vec_pretty(&receipt)?)?;
                return Err(error.context("no-load cleanup failed; retain run-owned custody"));
            }
        }
        fs::write(&receipt_path, serde_json::to_vec_pretty(&receipt)?)?;
        let project = scoped_compose_project(config.platform.compose_project.as_deref().context("project")?, &run_id);
        let mut list = Command::new("docker");
        list.args([
            "ps",
            "-aq",
            "--filter",
            &format!("label=com.docker.compose.project={project}"),
        ]);
        let ids = String::from_utf8(
            run_command(list, "verify run-owned workloads stopped after normal cleanup")
                .await?
                .stdout,
        )?
        .split_whitespace()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        if !ids.is_empty() {
            let mut inspect = Command::new("docker");
            inspect.arg("inspect").args(&ids);
            let remaining: Vec<Value> = serde_json::from_slice(
                &run_command(inspect, "verify stopped test-owned cleanup residue")
                    .await?
                    .stdout,
            )?;
            anyhow::ensure!(
                remaining.len() == ids.len()
                    && remaining
                        .iter()
                        .all(|container| container.pointer("/State/Running") == Some(&json!(false))
                            && container.pointer("/Config/Labels/com.docker.compose.project") == Some(&json!(project))),
                "normal cleanup left running or unowned resources; retain custody"
            );
            receipt["normal_cleanup_stopped_container_residue"] = json!(ids);
            fs::write(&receipt_path, serde_json::to_vec_pretty(&receipt)?)?;
            let mut remove = Command::new("docker");
            remove.arg("rm").args(&ids);
            run_command(remove, "remove only verified stopped test-owned container residue").await?;
        }
        receipt["test_owned_containers_removed"] = json!(true);
        fs::write(&receipt_path, serde_json::to_vec_pretty(&receipt)?)?;
        println!("endpoint_lifecycle_receipt {}", receipt_path.display());
        result
    }

    #[tokio::test]
    #[ignore = "requires an explicitly authorized test-owned no-load Docker collector; see TUNABLES.md"]
    async fn overload_actual_docker_admission_and_idle_sampling() -> Result<()> {
        async fn docker(args: &[&str]) -> Result<std::process::Output> {
            let mut command = Command::new("docker");
            command.args(args);
            tokio::time::timeout(
                Duration::from_secs(15),
                run_command(command, "guard Docker integration"),
            )
            .await
            .context("guard Docker command timed out")?
        }
        let binary = fs::canonicalize(std::env::var("OAT_GUARD_COLLECTOR_BINARY")?)?;
        let binary = binary.to_str().context("collector binary path is not UTF-8")?;
        let runner = std::env::var("OAT_GUARD_DOCKER_RUNNER")?;
        let image = docker(&["inspect", "--format", "{{.Image}}", &runner]).await?;
        let image = String::from_utf8(image.stdout)?.trim().to_owned();
        let root = std::env::temp_dir().join(format!("oat-guard-docker-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        let mut created = Vec::<String>::new();
        let result = tokio::time::timeout(Duration::from_secs(90), async {
            for capped in [true, false] {
                let run_id = uuid::Uuid::new_v4().to_string();
                let cap = 2 * 1024_u64.pow(3);
                let mut config: OatConfig = serde_json::from_str(include_str!(
                    "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
                ))?;
                config.platform.local.collector_memory_limit_bytes = capped.then_some(cap);
                config.tunables = Default::default();
                let project = scoped_compose_project(config.platform.compose_project.as_deref().context("project missing")?, &run_id);
                let compose_path = root.join(format!("{run_id}.compose.json"));
                let override_path = root.join(format!("{run_id}.override.json"));
                fs::write(&compose_path, json!({"services":{"dataflow-1":{"image":image},
                    "router":{"image":image},"oat-client":{"image":image}}}).to_string())?;
                fs::write(&override_path, "{\"services\":{}}")?;
                config.platform.compose_file = Some(compose_path.to_string_lossy().into_owned());
                config.platform.compose_override_file = Some(override_path.to_string_lossy().into_owned());
                let mut overlay = compose_tunables(&config)?;
                local_dataflow_overlay(&config, &root, &mut overlay)?;
                fs::write(root.join("tunables.compose.json"), overlay.to_string())?;
                let mut render = compose_command(compose_path.to_str().context("compose path not UTF-8")?,
                    config.platform.compose_override_file.as_deref(), &project, "oat");
                render.arg("--file").arg(root.join("tunables.compose.json")).args(["config", "--format", "json"]);
                let rendered: Value = serde_json::from_slice(&run_command(render, "verify actual Compose memory projection").await?.stdout)?;
                if capped {
                    let value = &rendered["services"]["dataflow-1"]["mem_limit"];
                    let rendered_cap = match value.as_u64() {
                        Some(bytes) => bytes,
                        None => value.as_str().context("Compose memory limit missing")?.parse::<u64>()
                            .with_context(|| format!("Compose memory limit is not integer bytes: {value}"))?,
                    };
                    anyhow::ensure!(rendered_cap == cap, "Compose cap {rendered_cap} did not match requested {cap}");
                    println!("guard_compose_cap {}", json!({"requested_bytes":cap,"rendered_bytes":rendered_cap}));
                }
                let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
                let admin_port = listener.local_addr()?.port();
                let flight_listener = std::net::TcpListener::bind("127.0.0.1:0")?;
                let flight_port = flight_listener.local_addr()?.port();
                let mut pipeline: Value = serde_yaml::from_str(include_str!("../../arrowcol-dataflow-rust/config.smoke.yaml"))?;
                let receiver = &mut pipeline["groups"]["arrowcol"]["pipelines"]["ingest"]["nodes"]["flight"]["config"];
                receiver["listening_addr"] = json!(format!("127.0.0.1:{flight_port}"));
                receiver["admin_listening_addr"] = json!(format!("127.0.0.1:{admin_port}"));
                receiver["native_quiver"] = json!(false);
                receiver["registration"]["instance_key"] = json!(run_id);
                receiver["registration"]["flight_endpoint"] = json!(format!("127.0.0.1:{flight_port}"));
                pipeline["groups"]["arrowcol"]["pipelines"]["ingest"]["nodes"]["delta"]["config"]["base_path"] = json!("/tmp/oat-guard-delta");
                let config_path = root.join(format!("{run_id}.yaml"));
                fs::write(&config_path, serde_yaml::to_string(&pipeline)?)?;
                let project_label = format!("com.docker.compose.project={project}");
                let network = format!("container:{runner}");
                let mut args = vec!["create", "--network", &network, "--cpus=1",
                    "--label", &project_label, "--label", "com.docker.compose.service=dataflow-1",
                    "--label", "com.docker.compose.config-hash=oat-no-load-guard",
                    "--label", "com.docker.compose.oneoff=False",
                    "--entrypoint", "/tmp/oat-guard-dataflow"];
                let memory = cap.to_string();
                if capped { args.extend(["--memory", &memory, "--memory-swap", &memory]); }
                args.extend([&image, "--num-cores", "1", "--config", "/tmp/oat-guard.yaml"]);
                let id = String::from_utf8(docker(&args).await?.stdout)?.trim().to_owned();
                if id.len() != 64 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) { bail!("invalid test-owned container ID"); }
                created.push(id.clone());
                docker(&["cp", binary, &format!("{id}:/tmp/oat-guard-dataflow")]).await?;
                docker(&["cp", config_path.to_str().context("config path not UTF-8")?, &format!("{id}:/tmp/oat-guard.yaml")]).await?;
                drop(listener);
                drop(flight_listener);
                docker(&["start", &id]).await?;
                let endpoint = format!("http://127.0.0.1:{admin_port}");
                let client = reqwest::Client::builder().timeout(Duration::from_millis(500)).build()?;
                tokio::time::timeout(Duration::from_secs(25), async {
                    loop {
                        if let Ok(response) = client.get(format!("{endpoint}/readyz")).send().await
                            && response.status().is_success() { break; }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }).await.context("actual no-load collector did not become ready")?;
                let platform = LocalPlatform { run_id:run_id.clone(), state_dir:root.clone(), environment:HashMap::new(),
                    control_dir:root.clone(), client_progress:std::sync::Mutex::new(HashMap::new()),
                    router_metrics_url:None, collector_metrics_url:Some(format!("{endpoint}/metrics")),
                    qap_url:None, daemon_endpoint:None, shutdown_drain_evidence:None, maintenance_cell_baseline:Vec::new() };
                let cgroup = String::from_utf8(docker(&["exec", &id, "cat", "/proc/self/cgroup",
                    "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.max"]).await?.stdout)?;
                println!("guard_docker_readback {}", json!({"container_id":id,"requested_cap":config.platform.local.collector_memory_limit_bytes,"cgroup":cgroup}));
                if capped {
                    let mut prepared = PreparedGuard::prepare(&config, &platform).await?;
                    anyhow::ensure!(prepared.engine.proof["last_caps"][0]["cap"] == cap, "actual cgroup cap mismatches request");
                    let mut missing = LocalPlatform { collector_metrics_url:None, ..platform };
                    let error = PreparedGuard::prepare(&config, &missing).await.err().context("missing protection was admitted")?;
                    anyhow::ensure!(format!("{error:#}").contains("metrics endpoint is unavailable"), "unexpected missing-protection cause: {error:#}");
                    println!("guard_docker_missing_protection_refused {error:#}");
                    missing.collector_metrics_url = Some(format!("{endpoint}/metrics"));
                    let start = unix_time_ms_u64()? + 2_000;
                    let end = start + 10_000;
                    prepared.engine.arm(start, end);
                    prepared.engine.proof["run_id"] = json!(run_id);
                    let mut evidence = CellEvidence::default();
                    ResourceSampler::start(missing.resource_sampler_source(), config.clone(), start, end, Some(prepared))
                        .finish(&mut evidence).await?;
                    let proof = evidence.overload_guard.context("idle guard proof missing")?;
                    println!("guard_docker_idle {}", serde_json::to_string(&proof)?);
                    anyhow::ensure!(proof["status"] == "complete", "idle protection was incomplete");
                    anyhow::ensure!(proof["sample_count"].as_u64().context("sample count missing")? >= 13, "idle sampling fell below 1 Hz");
                    anyhow::ensure!(proof["max_sampling_gap_ms"].as_u64().context("sampling gap missing")? <= GUARD_MAX_GAP_MS, "idle sampling gap exceeded limit");
                    anyhow::ensure!(evidence.overload_stop.is_none(), "idle collector triggered overload");
                } else {
                    anyhow::ensure!(cgroup.lines().last() == Some("max"), "uncapped test did not expose unlimited memory.max");
                    let error = PreparedGuard::prepare(&config, &platform).await.err().context("unlimited container was admitted")?;
                    anyhow::ensure!(format!("{error:#}").contains("hard cap is unconfirmed or unlimited"), "unexpected uncapped cause: {error:#}");
                    println!("guard_docker_uncapped_refused {error:#}");
                }
                docker(&["stop", "--time=5", &id]).await?;
            }
            Ok::<(), anyhow::Error>(())
        }).await.context("bounded no-load guard validation timed out").and_then(|result| result);
        let mut cleanup_error = None;
        for id in created {
            if let Err(error) = docker(&["rm", "--force", &id]).await {
                cleanup_error = Some(error);
            }
        }
        fs::remove_dir_all(&root)?;
        if let Some(error) = cleanup_error {
            return Err(error.context("test-owned guard container cleanup failed"));
        }
        result
    }

    fn reading(time: u64, memory: u64) -> GuardReading {
        GuardReading {
            request_start_ms: time,
            timestamp_ms: time,
            commits: Vec::new(),
            caps: vec![
                GuardCap {
                    metric: "container_memory_bytes",
                    observed_value: memory,
                    cap: 1000,
                    confirmed: true,
                },
                GuardCap {
                    metric: "collector_rss_bytes",
                    observed_value: 10,
                    cap: 1000,
                    confirmed: true,
                },
                GuardCap {
                    metric: "collector_buffered_bytes",
                    observed_value: 10,
                    cap: 1000,
                    confirmed: true,
                },
            ],
        }
    }

    fn pending(time: u64, rows: u64) -> PublicationBacklogSample {
        publication_backlog_sample(
            PublicationBacklogMetrics {
                available: true,
                publications: Some(u64::from(rows > 0)),
                rows: Some(rows),
                logical_bytes: Some(rows * 10),
                oldest_age_seconds: Some(0.0),
                status: "available".to_owned(),
            },
            time,
            time,
            3000,
        )
    }

    #[test]
    fn scale_out_buffering_requires_affirmative_memory_mode() {
        assert!(verify_scale_out_buffering_mode(&json!({"resolved_buffering_mode":"memory_only"})).is_ok());
        for pressure in [
            json!({}), json!({"native_quiver_enabled":false}),
            json!({"resolved_buffering_mode":null}), json!({"resolved_buffering_mode":false}),
            json!({"resolved_buffering_mode":"unknown"}), json!({"resolved_buffering_mode":"quiver"}),
            json!({"resolved_buffering_mode":"spill_recovery"}), json!({"resolved_buffering_mode":"mixed"}),
            json!({"resolved_buffering_mode":"future_mode"}),
        ] {
            assert!(verify_scale_out_buffering_mode(&pressure).is_err());
        }
    }

    #[test]
    fn overload_cap_boundary_and_unconfirmed_accounting_are_distinct() {
        let mut engine = GuardEngine::new(Default::default());
        engine.record(Ok(reading(0, 899)), &pending(0, 0));
        assert!(engine.stop.is_none());
        engine.record(Ok(reading(1, 900)), &pending(1, 0));
        assert_eq!(engine.stop.as_ref().map(|stop| &stop["fraction"]), Some(&json!(0.9)));
        assert_eq!(engine.proof["status"], "triggered");
        let mut unknown = GuardEngine::new(Default::default());
        let mut sample = reading(0, 1000);
        sample.caps[0].confirmed = false;
        unknown.record(Ok(sample), &pending(0, 0));
        assert_eq!(unknown.proof["status"], "unavailable");
        assert!(unknown.stop.is_none());
        assert!(unknown.proof["last_caps"][0]["cap"].is_null());
    }

    #[test]
    fn overload_zero_progress_requires_two_complete_outstanding_windows_not_idle_or_replayed_progress() -> Result<()> {
        let mut stalled = GuardEngine::new(Default::default());
        let mut idle = GuardEngine::new(Default::default());
        let mut replayed = GuardEngine::new(Default::default());
        for engine in [&mut stalled, &mut idle, &mut replayed] {
            engine.arm(0, 90_000);
        }
        let event: CommitEvent = serde_json::from_value(json!({
            "schema_version":1,"outcome":"committed","table_id":"a".repeat(64),"delta_version":7,
            "rows":1,"finished_at_unix_ms":21_000,
        }))?;
        for time in (0..=90_000).step_by(1000) {
            if time <= 60_000 {
                stalled.record(Ok(reading(time, 100)), &pending(time, 1));
                idle.record(Ok(reading(time, 100)), &pending(time, 0));
                assert_eq!(stalled.stop.is_some(), time == 60_000, "{time}");
                assert!(idle.stop.is_none());
            }
            let mut sample = reading(time, 100);
            if time >= 21_000 {
                sample.commits = vec![event.clone(), event.clone()];
            }
            replayed.record(Ok(sample), &pending(time, 1));
            assert_eq!(replayed.stop.is_some(), time == 90_000, "{time}");
        }
        assert_eq!(stalled.stop.as_ref().map(|stop| &stop["window_index"]), Some(&json!(1)));
        assert_eq!(replayed.proof["complete_windows"][0]["ingestion_commits"], 1);
        Ok(())
    }

    #[tokio::test]
    async fn overload_stop_preserves_guard_and_resource_evidence_through_sampler_finish() -> Result<()> {
        let mut engine = GuardEngine::new(Default::default());
        engine.record(Ok(reading(0, 900)), &pending(0, 1));
        let stop = engine.stop.clone().context("threshold must trigger")?;
        let terminal = CellEvidence {
            overload_guard: Some(engine.proof),
            overload_stop: Some(stop.clone()),
            resource_samples: vec![ResourceSample {
                sampled_at_unix_ms: 0,
                metrics: ResourceMetrics {
                    cpu_millicores: 1.0,
                    memory_bytes: 900,
                },
            }],
            ..Default::default()
        };
        let (sender, signal) = watch::channel(GuardSignal::Stop("above_ceiling".to_owned()));
        let (stop_sender, _stop_receiver) = watch::channel(false);
        let mut sampler = ResourceSampler {
            task: Some(tokio::spawn(async move { Ok(terminal) })),
            signal,
            stop: stop_sender,
        };
        assert_eq!(sampler.wait_for_overload_stop().await, "above_ceiling");
        let mut evidence = CellEvidence::default();
        sampler.finish_early(&mut evidence).await?;
        drop(sender);
        let public = evidence.for_table("table", None);
        assert_eq!(public["overload_stop"], stop);
        assert_eq!(public["overload_guard"]["status"], "triggered");
        assert_eq!(evidence.resource_samples.len(), 1);
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let run_id = uuid::Uuid::new_v4().to_string();
        let rows = result_rows(
            &config,
            "orchestration",
            &run_id,
            1,
            1,
            CellMeasurements {
                clients: ClientRun {
                    metrics: HashMap::new(),
                    pinning: PinningReport::default(),
                    qap_tables: Vec::new(),
                    failure: Some("above_ceiling".to_owned()),
                    evidence,
                },
                started_at_ms: 0,
                qap: QapMetrics::default(),
                resources: None,
                resource_evidence: None,
            },
        )?;
        let mut outcome = finish_matrix_cell(
            &config,
            "orchestration",
            &run_id,
            1,
            1,
            Ok(rows),
            Err(anyhow::anyhow!("stop_failed")),
        );
        assert!(outcome.error.is_some());
        attach_shutdown_drain_evidence(&mut outcome, json!({"status":"drained"}))?;
        for row in outcome.rows {
            assert_eq!(row.status, "failed");
            let retained: Value = serde_json::from_str(row.evidence_json.as_deref().context("cell evidence missing")?)?;
            assert_eq!(retained["overload_stop"], stop);
            assert_eq!(retained["shutdown_drain"]["status"], "drained");
        }
        Ok(())
    }
}
