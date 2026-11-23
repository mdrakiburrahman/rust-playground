const DIAGNOSTIC_CORPUS_ENV: &str = "OAT_DIAGNOSTIC_CORPUS_SHA256";
const DIAGNOSTIC_CLIENT_CPUS_ENV: &str = "OAT_DIAGNOSTIC_CLIENT_CPUS";
const DIAGNOSTIC_REFERENCE_ENV: &str = "OAT_DIAGNOSTIC_REFERENCE";
const DIAGNOSTIC_LIVE_BATCH_BYTES: u64 = 184_639_488;
const DIAGNOSTIC_CLIENT_MEMORY_BYTES: u64 = 5_368_709_120;
const DIAGNOSTIC_COLLECTOR_MEMORY_BYTES: u64 = 86 * 1024 * 1024 * 1024;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "mode", deny_unknown_fields)]
enum DiagnosticReference {
    #[serde(rename = "first")]
    First,
    #[serde(rename = "matched")]
    Matched {
        kind: String,
        qualification: String,
        profile_sha256: String,
        receipt_sha256: String,
        run_id: String,
        corpus_sha256: String,
        clients: Vec<DiagnosticClientReservation>,
    },
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct DiagnosticClientReservation {
    logical_client_id: String,
    corpus_memory_reservation_bytes: u64,
}

fn validate_diagnostic_reference(raw: &str, proof: &Value, run_id: &str) -> Result<Value> {
    ensure!(raw.len() <= 16_384, "diagnostic comparison profile is oversized");
    let value = serde_json::from_str::<DiagnosticJson>(raw)?.0;
    let reference: DiagnosticReference = serde_json::from_value(value)?;
    if let DiagnosticReference::Matched {
        kind,
        qualification,
        profile_sha256,
        receipt_sha256,
        run_id: reference_run_id,
        corpus_sha256,
        clients,
    } = &reference
    {
        let valid_hash = |hash: &str| {
            hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        };
        let mapping = proof.get("client_mapping").context("missing diagnostic client mapping")?;
        let logical_client_id = format!(
            "{}:{}",
            mapping["table"].as_str().context("missing diagnostic table")?,
            mapping["client_ordinal"].as_u64().context("missing diagnostic client ordinal")?
        );
        ensure!(
            kind == "diagnostic-comparison-profile"
                && qualification == "not-evaluated"
                && valid_hash(profile_sha256)
                && valid_hash(receipt_sha256)
                && valid_hash(corpus_sha256)
                && uuid::Uuid::parse_str(reference_run_id)?.to_string() == *reference_run_id
                && reference_run_id != run_id
                && corpus_sha256 == proof["payload_corpus_sha256"].as_str().unwrap_or_default()
                && clients.len() == 1
                && clients[0].logical_client_id == logical_client_id
                && clients[0].corpus_memory_reservation_bytes > 0
                && proof["corpus_memory_reservation_bytes"].as_u64()
                    == Some(clients[0].corpus_memory_reservation_bytes),
            "diagnostic comparison profile differs from the prepared client"
        );
    }
    Ok(serde_json::to_value(reference)?)
}

fn schedule_after_diagnostic_admission<T>(
    raw_reference: Option<&str>,
    proof: Option<&Value>,
    run_id: &str,
    schedule: impl FnOnce() -> T,
) -> Result<T> {
    match (raw_reference, proof) {
        (Some(raw), Some(proof)) => {
            let reference = validate_diagnostic_reference(raw, proof, run_id)?;
            ensure!(
                proof.get("reference") == Some(&reference),
                "diagnostic schedule proof differs from its frozen comparison profile"
            );
        }
        (None, None) => {}
        _ => anyhow::bail!("diagnostic schedule proof and reference must be present together"),
    }
    Ok(schedule())
}

fn diagnostic_host_memory_admission(meminfo: &str, collector_cap: Option<u64>) -> Result<Value> {
    let read = |name: &str| -> Result<u64> {
        let fields = meminfo
            .lines()
            .filter_map(|line| line.strip_prefix(name))
            .collect::<Vec<_>>();
        ensure!(fields.len() == 1, "host memory field missing or repeated");
        let parts = fields[0].split_whitespace().collect::<Vec<_>>();
        ensure!(
            parts.len() == 2
                && parts[1] == "kB"
                && !parts[0].is_empty()
                && parts[0].bytes().all(|byte| byte.is_ascii_digit()),
            "host memory field malformed"
        );
        parts[0]
            .parse::<u64>()?
            .checked_mul(1024)
            .context("host memory value overflow")
    };
    let total = read("MemTotal:")?;
    let available = read("MemAvailable:")?;
    ensure!(
        total > 0
            && available <= total
            && available > DIAGNOSTIC_CLIENT_MEMORY_BYTES
            && collector_cap == Some(DIAGNOSTIC_COLLECTOR_MEMORY_BYTES),
        "invalid diagnostic host admission inputs"
    );
    let budget = available - DIAGNOSTIC_CLIENT_MEMORY_BYTES;
    let bound = budget.checked_mul(3).context("host admission arithmetic overflow")? / 4;
    ensure!(
        DIAGNOSTIC_COLLECTOR_MEMORY_BYTES <= bound,
        "collector cap exceeds 75% of available memory after client budget"
    );
    Ok(json!({
        "kind": "diagnostic-only", "phase": "immediately_before_schedule",
        "mem_total_bytes": total, "mem_available_bytes": available,
        "client_memory_budget_bytes": DIAGNOSTIC_CLIENT_MEMORY_BYTES,
        "collector_memory_limit_bytes": DIAGNOSTIC_COLLECTOR_MEMORY_BYTES,
        "collector_limit_bound_bytes": bound,
        "observed_at_unix_ms": unix_time_ms_u64()?,
    }))
}

async fn diagnostic_memory_admission_or_cancel(
    config: &OatConfig,
    platform: &mut dyn Platform,
    handles: &[ClientHandle],
) -> Result<()> {
    let result = (|| {
        let Some(_) = diagnostic_expected_pin(
            config.platform.compose_project.as_deref(),
            std::env::var(DIAGNOSTIC_CORPUS_ENV),
        )?
        else {
            return Ok(None);
        };
        let meminfo = fs::read_to_string("/proc/meminfo").context("read host memory before scheduling")?;
        diagnostic_host_memory_admission(&meminfo, config.platform.local.collector_memory_limit_bytes).map(Some)
    })();
    match result {
        Ok(Some(proof)) => {
            println!("[oat] diagnostic host admission: {proof}");
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(error) => match cancel_clients(platform, config, handles).await {
            Ok(()) => Err(error).context("verify diagnostic host memory immediately before scheduling"),
            Err(cleanup_error) => Err(error.context(format!(
                "cancelling clients after host admission failure also failed: {cleanup_error:#}"
            ))),
        },
    }
}

fn diagnostic_cpu_set(raw: &str) -> Result<BTreeSet<u32>> {
    let mut cpus = BTreeSet::new();
    for part in raw.trim().split(',') {
        let parse = |text: &str| -> Result<u32> {
            ensure!(
                !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()),
                "invalid CPU number"
            );
            let value: u32 = text.parse()?;
            ensure!(value <= 4095, "CPU number exceeds limit");
            Ok(value)
        };
        let (first, last) = match part.split_once('-') {
            Some((first, last)) => (parse(first)?, parse(last)?),
            None => {
                let cpu = parse(part)?;
                (cpu, cpu)
            }
        };
        ensure!(first <= last, "invalid CPU range");
        for cpu in first..=last {
            ensure!(cpus.insert(cpu), "duplicate CPU");
        }
    }
    ensure!(!cpus.is_empty() && cpus.len() <= 128, "invalid CPU allocation size");
    Ok(cpus)
}

fn validate_diagnostic_cpu_topology(
    collector: &BTreeSet<u32>,
    client: &BTreeSet<u32>,
    topology: &[(u32, BTreeSet<u32>)],
) -> Result<()> {
    ensure!(
        !collector.is_empty() && !client.is_empty() && collector.is_disjoint(client),
        "collector and client CPUs must be disjoint"
    );
    let requested = collector.union(client).copied().collect::<BTreeSet<_>>();
    ensure!(topology.len() == requested.len(), "CPU sibling topology is incomplete");
    let mut seen = BTreeSet::new();
    for (cpu, siblings) in topology {
        ensure!(
            requested.contains(cpu) && seen.insert(*cpu) && siblings.contains(cpu),
            "invalid CPU sibling topology"
        );
        if collector.contains(cpu) {
            ensure!(
                siblings.is_disjoint(client),
                "collector and client share a physical CPU"
            );
        } else {
            ensure!(
                siblings.is_disjoint(collector),
                "collector and client share a physical CPU"
            );
        }
    }
    Ok(())
}

async fn diagnostic_command(mut command: Command, description: &str) -> Result<String> {
    command.kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(30), run_command(command, description))
        .await
        .context("diagnostic CPU readback timed out")??;
    String::from_utf8(output.stdout).context("diagnostic CPU readback is not UTF-8")
}

async fn diagnostic_container_cpu_readback(id: &str, requested: &BTreeSet<u32>) -> Result<Value> {
    let mut inspect = Command::new("docker");
    inspect.args(["inspect", "--format", "{{json .HostConfig.CpusetCpus}}", id]);
    let configured: String =
        serde_json::from_str(&diagnostic_command(inspect, "inspect diagnostic CPU configuration").await?)?;
    let mut effective = Command::new("docker");
    effective.args(["exec", id, "cat", "/sys/fs/cgroup/cpuset.cpus.effective"]);
    let effective = diagnostic_command(effective, "read diagnostic effective CPU allocation").await?;
    ensure!(
        diagnostic_cpu_set(&configured)? == *requested && diagnostic_cpu_set(&effective)? == *requested,
        "configured or effective diagnostic CPU allocation differs from request"
    );
    Ok(json!({"container_id": id, "requested": requested, "configured": configured, "effective": effective.trim()}))
}

async fn diagnostic_cpu_readback(config: &OatConfig, handle: &ClientHandle, run_id: &str) -> Result<Value> {
    let client = diagnostic_cpu_set(
        &std::env::var(DIAGNOSTIC_CLIENT_CPUS_ENV).context("diagnostic client CPU allocation is missing")?,
    )?;
    let collector = config
        .platform
        .local
        .collector_cpus
        .as_ref()
        .context("diagnostic collector CPU allocation is missing")?
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    ensure!(
        !collector.is_empty() && collector.len() <= 128,
        "invalid collector CPU allocation"
    );
    let ClientProcess::Local { container_id } = &handle.process else {
        bail!("diagnostic requires a local client");
    };
    let project = scoped_compose_project(
        config
            .platform
            .compose_project
            .as_deref()
            .context("diagnostic project missing")?,
        run_id,
    );
    let mut ps = Command::new("docker");
    ps.args([
        "ps",
        "--no-trunc",
        "--filter",
        &format!("label=com.docker.compose.project={project}"),
        "--filter",
        "label=com.docker.compose.service=dataflow-1",
        "--format",
        "{{.ID}}",
    ]);
    let ids = diagnostic_command(ps, "locate diagnostic collector").await?;
    let ids = ids.lines().collect::<Vec<_>>();
    ensure!(
        ids.len() == 1 && ids[0].len() == 64 && ids[0].bytes().all(|b| b.is_ascii_hexdigit()),
        "diagnostic collector identity is missing or ambiguous"
    );
    let mut clients = Command::new("docker");
    clients.args([
        "ps",
        "--no-trunc",
        "--filter",
        &format!("label=com.docker.compose.project={project}"),
        "--filter",
        "label=com.docker.compose.service=oat-client",
        "--format",
        "{{.ID}}",
    ]);
    let client_ids = diagnostic_command(clients, "locate diagnostic clients").await?;
    let client_ids = client_ids.lines().collect::<Vec<_>>();
    ensure!(
        client_ids.len() == 1 && client_ids[0] == container_id,
        "diagnostic run-owned client roster differs from its single handle"
    );
    let collector_proof = diagnostic_container_cpu_readback(ids[0], &collector).await?;
    let client_proof = diagnostic_container_cpu_readback(container_id, &client).await?;
    let cpus = collector.union(&client).copied().collect::<Vec<_>>();
    let mut topology = Command::new("docker");
    topology.args([
        "exec",
        container_id,
        "/bin/sh",
        "-ec",
        "for cpu do cat /sys/devices/system/cpu/cpu${cpu}/topology/thread_siblings_list; done",
        "diagnostic-topology",
    ]);
    for cpu in &cpus {
        topology.arg(cpu.to_string());
    }
    let topology_output = diagnostic_command(topology, "read physical CPU sibling topology").await?;
    let siblings = topology_output
        .lines()
        .map(diagnostic_cpu_set)
        .collect::<Result<Vec<_>>>()?;
    ensure!(siblings.len() == cpus.len(), "CPU sibling readback is incomplete");
    let topology = cpus.into_iter().zip(siblings).collect::<Vec<_>>();
    validate_diagnostic_cpu_topology(&collector, &client, &topology)?;
    let masks = Path::new(config.platform.compose_state_root.as_deref().context("missing diagnostic state root")?)
        .join(run_id).join("diagnostic-cpus.json");
    let mut inspect = diagnostic_cpu_tool();
    inspect.arg("inspect").arg(&project).arg(masks);
    let roster: Value = serde_json::from_str(&diagnostic_command(inspect, "verify complete diagnostic project CPU assignments").await?)?;
    Ok(json!({"collector": collector_proof, "client": client_proof, "thread_siblings": topology, "project_readback": roster}))
}

fn diagnostic_expected_pin(
    project: Option<&str>,
    value: std::result::Result<String, std::env::VarError>,
) -> Result<Option<String>> {
    let diagnostic = project.is_some_and(|name| name.starts_with("diagnostic-"));
    if !diagnostic && matches!(&value, Err(std::env::VarError::NotPresent)) {
        return Ok(None);
    }
    ensure!(diagnostic, "diagnostic corpus pin requires a diagnostic-scoped project");
    Ok(Some(value.context("diagnostic corpus pin is missing or invalid")?))
}

fn require_diagnostic_parity_precheck(value: std::result::Result<String, std::env::VarError>) -> Result<()> {
    ensure!(
        matches!(value.as_deref(), Ok("1")),
        "diagnostic requires the non-qualification parity readback"
    );
    Ok(())
}

struct DiagnosticJson(Value);

impl<'de> serde::Deserialize<'de> for DiagnosticJson {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct UniqueValue;
        impl<'de> serde::de::Visitor<'de> for UniqueValue {
            type Value = DiagnosticJson;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("JSON without duplicate object keys")
            }
            fn visit_bool<E: serde::de::Error>(self, value: bool) -> std::result::Result<Self::Value, E> {
                Ok(DiagnosticJson(json!(value)))
            }
            fn visit_i64<E: serde::de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(DiagnosticJson(json!(value)))
            }
            fn visit_u64<E: serde::de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(DiagnosticJson(json!(value)))
            }
            fn visit_f64<E: serde::de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
                let number = serde_json::Number::from_f64(value).ok_or_else(|| E::custom("invalid number"))?;
                Ok(DiagnosticJson(Value::Number(number)))
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(DiagnosticJson(Value::String(value.to_owned())))
            }
            fn visit_string<E: serde::de::Error>(self, value: String) -> std::result::Result<Self::Value, E> {
                Ok(DiagnosticJson(Value::String(value)))
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(DiagnosticJson(Value::Null))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element::<DiagnosticJson>()? {
                    values.push(value.0);
                }
                Ok(DiagnosticJson(Value::Array(values)))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> std::result::Result<Self::Value, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(serde::de::Error::custom("duplicate diagnostic JSON key"));
                    }
                    values.insert(key, map.next_value::<DiagnosticJson>()?.0);
                }
                Ok(DiagnosticJson(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(UniqueValue)
    }
}

fn validate_diagnostic_prepared_metrics(
    output: &str,
    expected_corpus: &str,
    expected_client_id: &str,
) -> Result<Value> {
    ensure!(
        expected_corpus.len() == 64
            && expected_corpus
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid diagnostic corpus pin"
    );
    let lines = output
        .lines()
        .enumerate()
        .filter_map(|(index, line)| line.strip_prefix("OAT_CLIENT_METRICS ").map(|value| (index, value)))
        .collect::<Vec<_>>();
    let ready = output
        .lines()
        .enumerate()
        .filter_map(|(index, line)| line.strip_prefix("OAT_CLIENT_READY ").map(|value| (index, value)))
        .collect::<Vec<_>>();
    ensure!(
        lines.len() == 1 && lines[0].1.len() <= 1_048_576,
        "prepared client metrics missing or repeated"
    );
    ensure!(
        ready.len() == 1 && lines[0].0 < ready[0].0,
        "client readiness missing, repeated, or before preparation"
    );
    let ids = ready[0]
        .1
        .split_whitespace()
        .filter_map(|field| field.strip_prefix("client_id="))
        .collect::<Vec<_>>();
    ensure!(
        ids.len() == 1 && !expected_client_id.is_empty() && ids[0] == expected_client_id,
        "client readiness does not match the expected client identity"
    );
    let metrics = serde_json::from_str::<DiagnosticJson>(lines[0].1)
        .context("parse prepared client metrics")?
        .0;
    let p = metrics.get("preparation").context("missing client preparation")?;
    let live_batch = p["live_identity_batch_bytes"]
        .as_u64()
        .context("missing live batch reservation")?;
    let live_total = live_batch.checked_mul(28).context("live batch total overflow")?;
    ensure!(
        metrics["version"] == 1
            && metrics["phase"] == "prepared"
            && metrics.get("measurement_window") == Some(&Value::Null)
            && p["data_profile"] == "realistic"
            && p["payload_corpus_hash_version"] == 2
            && p["payload_corpus_hash_scope"] == "arrow-logical-v2-excluding-endpoint_url"
            && p["payload_corpus_sha256"] == expected_corpus
            && p["steady_state_generation"] == true
            && p["configured_live_slots"] == 28
            && p["admitted_live_slots"] == 28
            && live_batch == DIAGNOSTIC_LIVE_BATCH_BYTES
            && p["live_identity_batch_reservation_bytes"] == live_total
            && p["max_memory_bytes"] == DIAGNOSTIC_CLIENT_MEMORY_BYTES,
        "diagnostic client preparation differs from the frozen admission contract"
    );
    let corpus = p["corpus_memory_reservation_bytes"]
        .as_u64()
        .context("missing corpus reservation")?;
    let total = p["memory_reservation_bytes"]
        .as_u64()
        .context("missing total reservation")?;
    ensure!(
        corpus > 0 && corpus.checked_add(live_total) == Some(total) && total <= DIAGNOSTIC_CLIENT_MEMORY_BYTES,
        "diagnostic client reservations do not fit the memory budget"
    );
    let workers = metrics["workers"].as_array().context("missing worker metrics")?;
    ensure!(workers.len() == 14, "diagnostic client must prepare fourteen workers");
    for (index, worker) in workers.iter().enumerate() {
        ensure!(
            worker["worker"] == index
                && worker["attempted_rows"] == 0
                && worker["completed_rows"] == 0
                && worker["retried_rows"] == 0
                && worker["ambiguous_rows"] == 0,
            "diagnostic client sent data before admission"
        );
    }
    Ok(json!({
        "kind": "diagnostic-only",
        "phase": "before_schedule",
        "data_profile": "realistic",
        "payload_corpus_sha256": expected_corpus,
        "configured_live_slots": 28,
        "admitted_live_slots": 28,
        "live_identity_batch_bytes": DIAGNOSTIC_LIVE_BATCH_BYTES,
        "corpus_memory_reservation_bytes": corpus,
        "memory_reservation_bytes": total,
        "expected_client_ids": [expected_client_id],
        "client_mapping": {
            "table": "sqlserver_availability_replica_states",
            "client_ordinal": 1,
            "client_id": expected_client_id,
        },
        "startup_accounting": {
            "schema_version": 1,
            "capture_point": "client_ready_before_arm",
            "clients": [{
                "client_id": ids[0],
                "configured_live_slots": p["configured_live_slots"],
                "admitted_live_slots": p["admitted_live_slots"],
                "live_identity_batch_bytes": p["live_identity_batch_bytes"],
                "live_identity_batch_reservation_bytes": p["live_identity_batch_reservation_bytes"],
                "corpus_memory_reservation_bytes": p["corpus_memory_reservation_bytes"],
                "memory_reservation_bytes": p["memory_reservation_bytes"],
                "max_memory_bytes": p["max_memory_bytes"],
            }],
        },
        "observed_at_unix_ms": unix_time_ms_u64()?,
    }))
}

fn validate_diagnostic_live_fleet(
    config: &OatConfig,
    handles: &[ClientHandle],
    clients: &[ClientProgress],
    run_id: &str,
    expected: &str,
    raw_reference: &str,
) -> Result<Value> {
    ensure!(
        config.platform.kind == crate::config::OatMode::Local
            && config.tables.len() == 1
            && config.tables[0] == "sqlserver_availability_replica_states"
            && config.clients_per_table == 1
            && handles.len() == 1
            && clients.len() == 1,
        "diagnostic requires one local table and client"
    );
    let client = &clients[0];
    ensure!(
        client.ready && !client.completed && !client.schedule_received && client.failure.is_none(),
        "diagnostic client is not exclusively waiting for its schedule"
    );
    ensure!(
        uuid::Uuid::parse_str(run_id)?.to_string() == run_id
            && handles[0].client_id == format!("{run_id}:{}:1", config.tables[0])
            && handles[0].table == config.tables[0]
            && matches!(&handles[0].process, ClientProcess::Local { .. }),
        "diagnostic client handle differs from the active run and table"
    );
    let expected_client_id = format!("oat-{run_id}-{}-c1", config.tables[0]);
    let mut proof = validate_diagnostic_prepared_metrics(&client.output, expected, &expected_client_id)?;
    proof["client_mapping"] = json!({
        "table": config.tables[0],
        "client_ordinal": 1,
        "client_id": expected_client_id,
    });
    proof["reference"] = validate_diagnostic_reference(raw_reference, &proof, run_id)?;
    Ok(proof)
}

async fn diagnostic_admission_or_cancel(
    config: &OatConfig,
    platform: &mut dyn Platform,
    handles: &[ClientHandle],
    clients: &[ClientProgress],
    run_id: &str,
    pre_schedule: bool,
) -> Result<Option<Value>> {
    let result = (|| {
        let Some(expected) = diagnostic_expected_pin(
            config.platform.compose_project.as_deref(),
            std::env::var(DIAGNOSTIC_CORPUS_ENV),
        )?
        else {
            return Ok(None);
        };
        require_diagnostic_parity_precheck(std::env::var("OAT_DARWIN_PARITY_PRECHECK"))?;
        let reference = std::env::var(DIAGNOSTIC_REFERENCE_ENV).context("diagnostic reference is missing")?;
        validate_diagnostic_live_fleet(config, handles, clients, run_id, &expected, &reference)
            .map(|proof| Some((proof, run_id.to_owned())))
    })();
    let result = match result {
        Ok(Some((mut proof, run_id))) => diagnostic_cpu_readback(config, &handles[0], &run_id).await.map(|cpu| {
            proof["cpu_isolation"] = cpu;
            Some(proof)
        }),
        Ok(None) => Ok(None),
        Err(error) => Err(error),
    };
    match result {
        Ok(Some(proof)) => {
            let label = if pre_schedule {
                "diagnostic pre-schedule admission"
            } else {
                "diagnostic admission"
            };
            println!("[oat] {label}: {proof}");
            Ok(Some(proof))
        }
        Ok(None) => Ok(None),
        Err(error) => {
            let cleanup = cancel_clients(platform, config, handles).await;
            match cleanup {
                Ok(()) => Err(error).context("verify diagnostic admission before scheduling"),
                Err(cleanup_error) => Err(error.context(format!(
                    "cancelling clients after diagnostic admission failure also failed: {cleanup_error:#}"
                ))),
            }
        }
    }
}

async fn diagnostic_recheck_or_cancel(
    config: &OatConfig,
    platform: &mut dyn Platform,
    handles: &[ClientHandle],
    run_id: &str,
) -> Result<Option<Value>> {
    let enabled = diagnostic_expected_pin(
        config.platform.compose_project.as_deref(),
        std::env::var(DIAGNOSTIC_CORPUS_ENV),
    );
    match enabled {
        Ok(None) => Ok(None),
        Ok(Some(_)) => match poll_clients(platform, config, handles).await {
            Ok(clients) => diagnostic_admission_or_cancel(config, platform, handles, &clients, run_id, true).await,
            Err(error) => match cancel_clients(platform, config, handles).await {
                Ok(()) => Err(error).context("refresh diagnostic clients before scheduling"),
                Err(cleanup_error) => Err(error.context(format!(
                    "cancelling clients after diagnostic refresh failure also failed: {cleanup_error:#}"
                ))),
            },
        },
        Err(error) => match cancel_clients(platform, config, handles).await {
            Ok(()) => Err(error).context("verify diagnostic configuration before scheduling"),
            Err(cleanup_error) => Err(error.context(format!(
                "cancelling clients after diagnostic configuration failure also failed: {cleanup_error:#}"
            ))),
        },
    }
}

#[cfg(test)]
mod realistic_diagnostic_admission_tests {
    use super::*;

    #[test]
    fn host_admission_uses_current_available_memory_and_fails_closed() -> Result<()> {
        let enough = "MemTotal: 200000000 kB\nMemAvailable: 150000000 kB\n";
        assert!(diagnostic_host_memory_admission(enough, Some(DIAGNOSTIC_COLLECTOR_MEMORY_BYTES)).is_ok());
        assert!(
            diagnostic_host_memory_admission(
                "MemTotal: 200000000 kB\nMemAvailable: 100000000 kB\n",
                Some(DIAGNOSTIC_COLLECTOR_MEMORY_BYTES)
            )
            .is_err()
        );
        assert!(
            diagnostic_host_memory_admission(
                "MemTotal: 200000000 kB\nMemAvailable: 150000000 kB\nMemAvailable: 150000000 kB\n",
                Some(DIAGNOSTIC_COLLECTOR_MEMORY_BYTES)
            )
            .is_err()
        );
        assert!(diagnostic_host_memory_admission(enough, Some(DIAGNOSTIC_COLLECTOR_MEMORY_BYTES - 1)).is_err());
        assert!(
            diagnostic_host_memory_admission("MemTotal: bad kB\n", Some(DIAGNOSTIC_COLLECTOR_MEMORY_BYTES)).is_err()
        );
        Ok(())
    }

    #[test]
    fn cpu_isolation_rejects_shared_siblings_and_bad_cpu_lists() -> Result<()> {
        let collector = diagnostic_cpu_set("0-1")?;
        let client = diagnostic_cpu_set("2,3")?;
        let good = vec![
            (0, diagnostic_cpu_set("0-1")?),
            (1, diagnostic_cpu_set("0-1")?),
            (2, diagnostic_cpu_set("2-3")?),
            (3, diagnostic_cpu_set("2-3")?),
        ];
        assert!(validate_diagnostic_cpu_topology(&collector, &client, &good).is_ok());
        let mut bad = good.clone();
        bad[0].1 = diagnostic_cpu_set("0,2")?;
        assert!(validate_diagnostic_cpu_topology(&collector, &client, &bad).is_err());
        assert!(validate_diagnostic_cpu_topology(&collector, &client, &good[1..]).is_err());
        assert!(diagnostic_cpu_set("1-2,2").is_err());
        assert!(diagnostic_cpu_set("2-1").is_err());
        Ok(())
    }

    fn prepared() -> Value {
        let live_total = DIAGNOSTIC_LIVE_BATCH_BYTES * 28;
        json!({
            "version": 1, "phase": "prepared", "measurement_window": null,
            "preparation": {
                "data_profile": "realistic", "payload_corpus_hash_version": 2,
                "payload_corpus_hash_scope": "arrow-logical-v2-excluding-endpoint_url",
                "payload_corpus_sha256": "a".repeat(64), "steady_state_generation": true,
                "configured_live_slots": 28, "admitted_live_slots": 28,
                "live_identity_batch_bytes": DIAGNOSTIC_LIVE_BATCH_BYTES,
                "live_identity_batch_reservation_bytes": live_total,
                "max_memory_bytes": DIAGNOSTIC_CLIENT_MEMORY_BYTES,
                "corpus_memory_reservation_bytes": 100,
                "memory_reservation_bytes": live_total + 100,
            },
            "workers": (0..14).map(|worker| json!({"worker": worker, "attempted_rows": 0,
                "completed_rows": 0, "retried_rows": 0, "ambiguous_rows": 0})).collect::<Vec<_>>(),
        })
    }

    fn output(value: &Value) -> String {
        format!("OAT_CLIENT_METRICS {value}\nOAT_CLIENT_READY client_id=test\n")
    }

    #[test]
    fn diagnostic_project_requires_a_pin_without_changing_normal_parity() -> Result<()> {
        assert!(diagnostic_expected_pin(Some("diagnostic-test"), Err(std::env::VarError::NotPresent)).is_err());
        assert_eq!(
            diagnostic_expected_pin(Some("parity"), Err(std::env::VarError::NotPresent))?,
            None
        );
        assert!(diagnostic_expected_pin(Some("parity"), Ok("a".repeat(64))).is_err());
        assert_eq!(
            diagnostic_expected_pin(Some("diagnostic-test"), Ok("a".repeat(64)))?,
            Some("a".repeat(64))
        );
        Ok(())
    }

    #[test]
    fn diagnostic_requires_non_qualification_parity_readback() {
        assert!(require_diagnostic_parity_precheck(Ok("1".into())).is_ok());
        for value in ["qualified", "0", ""] {
            assert!(require_diagnostic_parity_precheck(Ok(value.into())).is_err());
        }
        assert!(require_diagnostic_parity_precheck(Err(std::env::VarError::NotPresent)).is_err());
    }

    #[test]
    fn schedule_callback_is_gated_by_each_clients_frozen_reservation() -> Result<()> {
        use std::cell::Cell;

        let run = "00000000-0000-4000-8000-000000000001";
        let reference_run = "00000000-0000-4000-8000-000000000002";
        let corpus = "a".repeat(64);
        let client_id = format!("oat-{run}-sqlserver_availability_replica_states-c1");
        let metrics = output(&prepared()).replace("client_id=test", &format!("client_id={client_id}"));
        let mut proof = validate_diagnostic_prepared_metrics(&metrics, &corpus, &client_id)?;
        proof["client_mapping"] = json!({
            "table": "sqlserver_availability_replica_states",
            "client_ordinal": 1,
            "client_id": client_id,
        });
        let exact_reference = json!({
            "mode": "matched",
            "kind": "diagnostic-comparison-profile",
            "qualification": "not-evaluated",
            "profile_sha256": "b".repeat(64),
            "receipt_sha256": "c".repeat(64),
            "run_id": reference_run,
            "corpus_sha256": corpus,
            "clients": [{
                "logical_client_id": "sqlserver_availability_replica_states:1",
                "corpus_memory_reservation_bytes": 100,
            }],
        });
        let schedule_calls = Cell::new(0);
        let mut mismatched_reference = exact_reference.clone();
        mismatched_reference["clients"][0]["corpus_memory_reservation_bytes"] = json!(101);
        proof["reference"] = mismatched_reference.clone();
        assert!(
            schedule_after_diagnostic_admission(
                Some(&mismatched_reference.to_string()),
                Some(&proof),
                run,
                || schedule_calls.set(schedule_calls.get() + 1),
            )
            .is_err()
        );
        assert_eq!(schedule_calls.get(), 0, "a one-byte mismatch must not enter the schedule callback");

        proof["reference"] = exact_reference.clone();
        assert!(
            schedule_after_diagnostic_admission(None, Some(&proof), run, || {
                schedule_calls.set(schedule_calls.get() + 1)
            })
            .is_err(),
            "a missing reference must not schedule clients"
        );
        assert_eq!(schedule_calls.get(), 0);

        let mut wrong_roster = exact_reference.clone();
        wrong_roster["clients"] = json!([]);
        proof["reference"] = wrong_roster.clone();
        assert!(
            schedule_after_diagnostic_admission(
                Some(&wrong_roster.to_string()),
                Some(&proof),
                run,
                || schedule_calls.set(schedule_calls.get() + 1),
            )
            .is_err(),
            "an incomplete client roster must not schedule clients"
        );
        assert_eq!(schedule_calls.get(), 0);

        let normal_schedule_calls = Cell::new(0);
        schedule_after_diagnostic_admission(None, None, run, || {
            normal_schedule_calls.set(normal_schedule_calls.get() + 1)
        })?;
        assert_eq!(normal_schedule_calls.get(), 1, "non-diagnostic scheduling remains unchanged");

        proof["reference"] = exact_reference.clone();
        schedule_after_diagnostic_admission(
            Some(&exact_reference.to_string()),
            Some(&proof),
            run,
            || schedule_calls.set(schedule_calls.get() + 1),
        )?;
        assert_eq!(schedule_calls.get(), 1, "an exact per-client match must enter the schedule callback");
        Ok(())
    }

    #[test]
    fn live_fleet_requires_exact_run_owned_waiting_client() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let table = "sqlserver_availability_replica_states";
        config.tables = vec![table.into()];
        config.clients_per_table = 1;
        let run = "00000000-0000-4000-8000-000000000001";
        let process_id = format!("oat-{run}-{table}-c1");
        let handle = ClientHandle {
            client_id: format!("{run}:{table}:1"),
            table: table.into(),
            process: ClientProcess::Local {
                container_id: "one".into(),
            },
        };
        let client = ClientProgress {
            ready: true,
            output: output(&prepared()).replace("client_id=test", &format!("client_id={process_id}")),
            ..Default::default()
        };
        let pin = "a".repeat(64);
        assert!(
            validate_diagnostic_live_fleet(
                &config,
                std::slice::from_ref(&handle),
                std::slice::from_ref(&client),
                run,
                &pin,
                r#"{"mode":"first"}"#
            )
            .is_ok()
        );
        assert!(
            validate_diagnostic_live_fleet(
                &config, &[], std::slice::from_ref(&client), run, &pin, r#"{"mode":"first"}"#
            )
            .is_err()
        );
        assert!(
            validate_diagnostic_live_fleet(
                &config,
                &[handle.clone(), handle.clone()],
                std::slice::from_ref(&client),
                run,
                &pin,
                r#"{"mode":"first"}"#
            )
            .is_err()
        );
        assert!(
            validate_diagnostic_live_fleet(
                &config,
                std::slice::from_ref(&handle),
                &[client.clone(), client.clone()],
                run,
                &pin,
                r#"{"mode":"first"}"#
            )
            .is_err()
        );
        assert!(
            validate_diagnostic_live_fleet(
                &config,
                std::slice::from_ref(&handle),
                std::slice::from_ref(&client),
                "00000000-0000-4000-8000-000000000002",
                &pin,
                r#"{"mode":"first"}"#
            )
            .is_err()
        );
        let mut wrong = client.clone();
        wrong.schedule_received = true;
        assert!(
            validate_diagnostic_live_fleet(
                &config, std::slice::from_ref(&handle), &[wrong], run, &pin, r#"{"mode":"first"}"#
            )
            .is_err()
        );
        let mut wrong = client.clone();
        wrong.failure = Some("failed".into());
        assert!(
            validate_diagnostic_live_fleet(
                &config, std::slice::from_ref(&handle), &[wrong], run, &pin, r#"{"mode":"first"}"#
            )
            .is_err()
        );
        let mut wrong = client.clone();
        wrong.output.clear();
        assert!(
            validate_diagnostic_live_fleet(
                &config, &[handle], &[wrong], run, &pin, r#"{"mode":"first"}"#
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn checks_pinned_corpus_and_complete_pre_schedule_reservation() -> Result<()> {
        let value = prepared();
        let pin = "a".repeat(64);
        let proof = validate_diagnostic_prepared_metrics(&output(&value), &pin, "test")?;
        assert_eq!(proof["startup_accounting"]["clients"][0]["client_id"], "test");
        assert_eq!(
            proof["startup_accounting"]["clients"][0]["live_identity_batch_bytes"],
            DIAGNOSTIC_LIVE_BATCH_BYTES
        );
        assert_eq!(proof["corpus_memory_reservation_bytes"], 100);
        assert!(validate_diagnostic_prepared_metrics("", &pin, "test").is_err());
        assert!(validate_diagnostic_prepared_metrics(&output(&value), &"b".repeat(64), "test").is_err());
        assert!(validate_diagnostic_prepared_metrics(&output(&value), &pin, "other").is_err());
        let mut wrong = value.clone();
        wrong["phase"] = json!("measurement");
        assert!(validate_diagnostic_prepared_metrics(&output(&wrong), &pin, "test").is_err());
        wrong = value.clone();
        wrong
            .as_object_mut()
            .context("fixture is not an object")?
            .remove("measurement_window");
        assert!(validate_diagnostic_prepared_metrics(&output(&wrong), &pin, "test").is_err());
        let duplicate = output(&value).replacen(
            "\"admitted_live_slots\":28",
            "\"admitted_live_slots\":27,\"admitted_live_slots\":28",
            1,
        );
        assert!(validate_diagnostic_prepared_metrics(&duplicate, &pin, "test").is_err());
        wrong = value.clone();
        wrong["preparation"]["admitted_live_slots"] = json!(27);
        assert!(validate_diagnostic_prepared_metrics(&output(&wrong), &pin, "test").is_err());
        wrong = value.clone();
        wrong["preparation"]["live_identity_batch_bytes"] = json!(DIAGNOSTIC_LIVE_BATCH_BYTES - 1);
        assert!(validate_diagnostic_prepared_metrics(&output(&wrong), &pin, "test").is_err());
        wrong = value.clone();
        wrong["preparation"]["corpus_memory_reservation_bytes"] = json!(0);
        assert!(validate_diagnostic_prepared_metrics(&output(&wrong), &pin, "test").is_err());
        wrong = value.clone();
        wrong["preparation"]["memory_reservation_bytes"] = json!(DIAGNOSTIC_CLIENT_MEMORY_BYTES + 1);
        assert!(validate_diagnostic_prepared_metrics(&output(&wrong), &pin, "test").is_err());
        wrong = value.clone();
        wrong["workers"][0]["attempted_rows"] = json!(1);
        assert!(validate_diagnostic_prepared_metrics(&output(&wrong), &pin, "test").is_err());
        assert!(
            validate_diagnostic_prepared_metrics(&format!("{}{}", output(&value), output(&value)), &pin, "test")
                .is_err()
        );
        assert!(
            validate_diagnostic_prepared_metrics(
                &format!("OAT_CLIENT_READY client_id=test\nOAT_CLIENT_METRICS {value}\n"),
                &pin,
                "test"
            )
            .is_err()
        );
        assert!(
            validate_diagnostic_prepared_metrics(
                &output(&value).replace("client_id=test", "client_id=test client_id=other"),
                &pin,
                "test"
            )
            .is_err()
        );
        Ok(())
    }
}
