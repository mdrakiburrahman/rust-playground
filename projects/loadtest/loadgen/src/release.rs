//! Positive publisher-release proof, admitted before any post-drain maintenance.
//!
//! The runner first stops every mutator container and records that each exited cleanly. That only
//! proves the processes ended. This command then reads routing state and proves, for the same run,
//! that:
//!
//! - the single Dataflow publisher this run routed to is durably `stopped` in routing under the
//!   same id, key, and pool;
//! - every collector generation has an explicit stopped/failed state or same-run owned SIGKILL and
//!   successor process proof; heartbeat expiry alone is not enough;
//!   each routing lease belongs to a same-run stopped publisher in the matching pool;
//! - each routing lease belongs to the same-run stopped publisher in the matching pool.
//!
//! A row absent from routing is never treated as stopped. Terminated historical generations are
//! allowed. The report is written for both outcomes and never invents a run identity.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use arrowcol_protocol::UnixMillis;
use arrowcol_routing::{RoutingStore, RoutingView, SqlServerRoutingConfig, SqlServerRoutingStore};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::fs;
use uuid::Uuid;

use crate::cli::PublisherReleaseArgs;
use crate::manifest::{ROUTING_EVIDENCE_FILE, RUN_METADATA_FILE};
use crate::transport::now_unix_millis;

/// Atomically replaced proof report, written for both outcomes.
pub const PUBLISHER_RELEASE_FILE: &str = "publisher-release.json";
/// Publication drain receipt written by `drain` while collectors were still running.
pub const PUBLICATION_DRAIN_FILE: &str = "publication-drain.json";
/// Clean stopped-mutator receipt written by the runner after the containers exited.
pub const MUTATOR_DRAIN_FILE: &str = "mutator-drain.json";
/// Exact owned process exits captured around the controlled Quiver crash.
pub const QUIVER_PROCESS_EXITS_FILE: &str = "quiver-process-exits.json";

/// Source publisher pool and the mutator service that hosts it.
const PUBLISHERS: [(&str, &str); 1] = [("dataflow", "dataflow-1")];
/// Mutator services the runner may record as cleanly stopped.
const MUTATORS: [&str; 3] = ["dataflow-1", "tenant-stream", "delta-maintenance"];

/// One routing collector as recorded in the run's `routing.json` or read back from SQL.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoutedCollector {
    pub instance_id: String,
    pub instance_key: String,
    pub implementation_pool: String,
    pub state: String,
    pub terminated: bool,
}

/// One routing table lease and its holder.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoutedLease {
    pub implementation_pool: String,
    pub table: String,
    pub holder_instance_id: String,
    pub state: String,
    #[serde(default)]
    pub expires_at_unix_ms: Option<u64>,
}

/// Complete routing collectors and leases at one instant.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoutingState {
    #[serde(default)]
    pub run_id: Option<String>,
    pub loaded_at_unix_ms: u64,
    pub collectors: Vec<RoutedCollector>,
    pub leases: Vec<RoutedLease>,
}

/// Drain receipts the runner recorded for this run.
#[derive(Clone, Debug)]
pub struct DrainEvidence {
    pub metadata: Value,
    pub publication: Value,
    pub mutators: Value,
    /// Optional positive process proof, never inferred from heartbeat expiry.
    pub crashes: Option<CrashEvidence>,
}

/// Same-run SIGKILL observations bound to restarted publisher generations.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CrashEvidence {
    /// Exact load-generation run identity.
    pub run_id: String,
    /// One observed exit and restart per source pool.
    pub publishers: Vec<CrashedPublisher>,
}

/// Controller-observed Docker process exit and successor identity.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CrashedPublisher {
    /// Killed collector's pool.
    pub implementation_pool: String,
    /// Stable source service key.
    pub instance_key: String,
    /// UUID read from the killed process's pressure endpoint.
    pub instance_id: String,
    /// Owned container observed before and after SIGKILL.
    pub container_id: String,
    /// Original Docker process start time.
    pub started_at_unix_ms: u64,
    /// Docker's observed process finish time.
    pub finished_at_unix_ms: u64,
    /// Must be 137 for the requested SIGKILL, not a clean stop or unknown exit.
    pub exit_code: u16,
    /// New UUID read from the healthy successor's pressure endpoint.
    pub successor_instance_id: String,
    /// Container later positively proved cleanly stopped.
    pub successor_container_id: String,
    /// New Docker process start time after the observed exit.
    pub successor_started_at_unix_ms: u64,
}

/// Public proof report; `passed` is true only when `errors` is empty.
#[derive(Clone, Debug, Serialize)]
pub struct ReleaseReport {
    pub passed: bool,
    pub run_id: Option<String>,
    pub checked_at_unix_ms: u64,
    /// The publishers bound from the run's routing evidence.
    pub publishers: Vec<RoutedCollector>,
    /// Post-drain routing state read from the routing store.
    pub routing: Option<RoutingState>,
    pub errors: Vec<String>,
}

/// Reads this run's drain evidence and post-drain routing state, then writes the proof.
///
/// # Errors
///
/// Fails when the proof report cannot be written or the release is not positively proven.
pub async fn publisher_release(arguments: PublisherReleaseArgs) -> Result<()> {
    let checked_at = now_unix_millis();
    let report = match prove(&arguments, checked_at).await {
        Ok(report) => report,
        Err(error) => {
            let mut errors = vec![format!("{error:#}")];
            let run_id = match read_json::<Value>(&arguments.results.join(RUN_METADATA_FILE)).await {
                Ok(metadata) => metadata["run_id"]
                    .as_str()
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned),
                Err(metadata_error) => {
                    errors.push(format!("run identity unavailable: {metadata_error:#}"));
                    None
                }
            };
            ReleaseReport {
                passed: false,
                run_id,
                checked_at_unix_ms: checked_at,
                publishers: Vec::new(),
                routing: None,
                errors,
            }
        }
    };
    let output = arguments.results.join(PUBLISHER_RELEASE_FILE);
    let temporary = output.with_extension("json.next");
    fs::write(&temporary, serde_json::to_vec_pretty(&report)?).await?;
    fs::rename(&temporary, &output).await?;
    ensure!(
        report.passed,
        "publisher release is not proven: {}",
        report.errors.join("; ")
    );
    Ok(())
}

async fn prove(arguments: &PublisherReleaseArgs, checked_at: u64) -> Result<ReleaseReport> {
    let results = &arguments.results;
    let drain = DrainEvidence {
        metadata: read_json(&results.join(RUN_METADATA_FILE)).await?,
        publication: read_json(&results.join(PUBLICATION_DRAIN_FILE)).await?,
        mutators: read_json(&results.join(MUTATOR_DRAIN_FILE)).await?,
        crashes: match fs::read(results.join(QUIVER_PROCESS_EXITS_FILE)).await {
            Ok(bytes) => Some(serde_json::from_slice(&bytes).context("parse Quiver process-exit proof")?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).context("read Quiver process-exit proof"),
        },
    };
    let routed = canonical_routing(read_json(&results.join(ROUTING_EVIDENCE_FILE)).await?)?;
    let mut admission_errors = Vec::new();
    let run_id = check_drain(&drain, &mut admission_errors);
    check_run_binding(run_id.as_deref(), &routed, &mut admission_errors);
    let publishers = bind_publishers(&routed, &mut admission_errors);
    if !admission_errors.is_empty() {
        return Ok(ReleaseReport {
            passed: false,
            run_id,
            checked_at_unix_ms: checked_at,
            publishers,
            routing: None,
            errors: admission_errors,
        });
    }
    let timeout = arguments.timeout.get();
    ensure!(!timeout.is_zero(), "publisher-release SQL bound must be positive");
    let after = read_routing(arguments, timeout).await?;
    Ok(release_report(&drain, &routed, after, checked_at))
}

async fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = fs::read(path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

async fn read_routing(arguments: &PublisherReleaseArgs, timeout: Duration) -> Result<RoutingState> {
    let config = SqlServerRoutingConfig::with_password_file(
        &arguments.sql_host,
        arguments.sql_port,
        &arguments.sql_database,
        &arguments.sql_username,
        &arguments.sql_password_file,
    )?
    .trust_server_certificate(true)
    .with_timeouts(timeout, timeout);
    let store = SqlServerRoutingStore::new(config);
    let loaded_at = UnixMillis::new(now_unix_millis())?;
    let view = tokio::time::timeout(timeout, store.load_view(loaded_at))
        .await
        .context("post-drain routing read exceeded its bound")??;
    canonical_routing(routing_state(&view))
}

/// Maps every routing collector and lease, including stopped, failed, and expired generations.
#[must_use]
pub fn routing_state(view: &RoutingView) -> RoutingState {
    RoutingState {
        run_id: None,
        loaded_at_unix_ms: view.loaded_at_unix_ms.get(),
        collectors: view
            .collectors
            .iter()
            .map(|(id, collector)| RoutedCollector {
                instance_id: id.to_string(),
                instance_key: collector.registration.instance_key.clone(),
                implementation_pool: collector.registration.implementation_pool.to_string(),
                state: format!("{:?}", collector.state).to_lowercase(),
                terminated: collector.is_terminated_at(view.loaded_at_unix_ms),
            })
            .collect(),
        leases: view
            .leases
            .values()
            .map(|lease| RoutedLease {
                implementation_pool: lease.key.implementation_pool.to_string(),
                table: lease.key.table.to_string(),
                holder_instance_id: lease.holder_instance_id.to_string(),
                state: format!("{:?}", lease.state).to_lowercase(),
                expires_at_unix_ms: Some(lease.expires_at_unix_ms.get()),
            })
            .collect(),
    }
}

fn canonical_id(value: &str) -> Result<String> {
    Ok(Uuid::parse_str(value)
        .with_context(|| format!("malformed collector instance identity {value:?}"))?
        .hyphenated()
        .to_string())
}

fn canonical_routing(mut state: RoutingState) -> Result<RoutingState> {
    for collector in &mut state.collectors {
        collector.instance_id = canonical_id(&collector.instance_id)?;
    }
    for lease in &mut state.leases {
        lease.holder_instance_id = canonical_id(&lease.holder_instance_id)?;
    }
    Ok(state)
}

/// Pure fail-closed release decision over already-read evidence.
#[must_use]
pub fn release_report(
    drain: &DrainEvidence,
    routed: &RoutingState,
    after: RoutingState,
    checked_at_unix_ms: u64,
) -> ReleaseReport {
    let mut errors = Vec::new();
    let run_id = check_drain(drain, &mut errors);
    check_run_binding(run_id.as_deref(), routed, &mut errors);
    let publishers = bind_publishers(routed, &mut errors);
    let killed = check_crashes(
        drain,
        run_id.as_deref(),
        &publishers,
        &after,
        checked_at_unix_ms,
        &mut errors,
    );
    check_routing(&publishers, &after, &killed, &mut errors);
    ReleaseReport {
        passed: errors.is_empty(),
        run_id,
        checked_at_unix_ms,
        publishers,
        routing: Some(after),
        errors,
    }
}

fn check_run_binding(run_id: Option<&str>, routed: &RoutingState, errors: &mut Vec<String>) {
    if run_id.is_none() || routed.run_id.as_deref() != run_id {
        errors.push("routing evidence does not belong to this run".to_owned());
    }
}

fn check_drain(drain: &DrainEvidence, errors: &mut Vec<String>) -> Option<String> {
    let Some(run_id) = drain.metadata["run_id"].as_str().filter(|id| !id.is_empty()) else {
        errors.push("run metadata has no run identity".to_owned());
        return None;
    };
    if drain.publication["run_id"].as_str() != Some(run_id)
        || drain.publication["dataflow_settled"].as_bool() != Some(true)
    {
        errors.push("publication drain evidence is not settled for this run".to_owned());
    }
    if drain.mutators["run_id"].as_str() != Some(run_id) || drain.mutators["clean"].as_bool() != Some(true) {
        errors.push("mutator drain evidence is not clean for this run".to_owned());
    }
    match drain.mutators["services"].as_array() {
        None => errors.push("mutator drain evidence has no stopped services".to_owned()),
        Some(services) => {
            let names = services.iter().map(Value::as_str).collect::<Vec<_>>();
            let unique = names.iter().flatten().copied().collect::<BTreeSet<_>>();
            if names.iter().any(Option::is_none)
                || unique.len() != names.len()
                || unique.iter().any(|service| !MUTATORS.contains(service))
            {
                errors.push("mutator drain services must be unique known mutators".to_owned());
            }
            for (_, service) in PUBLISHERS {
                if !unique.contains(service) {
                    errors.push(format!("publisher {service} has no clean stop evidence"));
                }
            }
        }
    }
    Some(run_id.to_owned())
}

fn check_unique_ids<'a>(ids: impl Iterator<Item = &'a str>, source: &str, errors: &mut Vec<String>) {
    let mut seen = BTreeSet::new();
    for id in ids {
        if !seen.insert(id) {
            errors.push(format!("{source} repeats collector instance {id}"));
        }
    }
}

/// Binds exactly one live generation per source pool from the run's own routing evidence.
fn bind_publishers(routed: &RoutingState, errors: &mut Vec<String>) -> Vec<RoutedCollector> {
    check_unique_ids(
        routed.collectors.iter().map(|collector| collector.instance_id.as_str()),
        "run routing evidence",
        errors,
    );
    let mut publishers = Vec::new();
    for (pool, service) in PUBLISHERS {
        let live = routed
            .collectors
            .iter()
            .filter(|collector| collector.implementation_pool == pool && !collector.terminated)
            .collect::<Vec<_>>();
        match live.as_slice() {
            [publisher] => {
                if publisher.instance_key != format!("loadtest-{service}") {
                    errors.push(format!("run routing evidence has an unexpected {service} instance key"));
                }
                publishers.push((*publisher).clone());
            }
            [] => errors.push(format!("run routing evidence has no live {service} publisher")),
            many => errors.push(format!(
                "run routing evidence has {} live {pool} generations",
                many.len()
            )),
        }
    }
    for collector in &routed.collectors {
        if !collector.terminated
            && !PUBLISHERS
                .iter()
                .any(|(pool, _)| collector.implementation_pool == *pool)
        {
            errors.push(format!(
                "run routing evidence has a live publisher in unknown pool {}",
                collector.implementation_pool
            ));
        }
    }
    publishers
}

fn check_crashes(
    drain: &DrainEvidence,
    run_id: Option<&str>,
    publishers: &[RoutedCollector],
    after: &RoutingState,
    checked_at: u64,
    errors: &mut Vec<String>,
) -> BTreeSet<String> {
    let Some(crashes) = &drain.crashes else {
        return BTreeSet::new();
    };
    let mut killed = BTreeSet::new();
    let mut pools = BTreeSet::new();
    if Some(crashes.run_id.as_str()) != run_id || crashes.publishers.len() != PUBLISHERS.len() {
        errors.push("Quiver process exits are not a complete same-run crash proof".to_owned());
    }
    for crash in &crashes.publishers {
        let valid_id = |id: &str| canonical_id(id).is_ok_and(|canonical| canonical == id);
        let valid_container = |id: &str| {
            id.len() == 64
                && id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        };
        let successor = publishers.iter().find(|publisher| {
            publisher.instance_id == crash.successor_instance_id
                && publisher.instance_key == crash.instance_key
                && publisher.implementation_pool == crash.implementation_pool
        });
        let historical = after.collectors.iter().find(|collector| {
            collector.instance_id == crash.instance_id
                && collector.instance_key == crash.instance_key
                && collector.implementation_pool == crash.implementation_pool
                && collector.terminated
        });
        let clean_successor = drain.mutators["containers"].as_array().is_some_and(|containers| {
            containers
                .iter()
                .filter(|container| {
                    container["id"].as_str() == Some(crash.successor_container_id.as_str())
                        && container["started_at_unix_ms"].as_u64() == Some(crash.successor_started_at_unix_ms)
                        && container["service"].as_str() == Some(format!("{}-1", crash.implementation_pool).as_str())
                })
                .count()
                == 1
        });
        let valid = valid_id(&crash.instance_id)
            && valid_id(&crash.successor_instance_id)
            && crash.instance_id != crash.successor_instance_id
            && valid_container(&crash.container_id)
            && valid_container(&crash.successor_container_id)
            && crash.exit_code == 137
            && crash.started_at_unix_ms > 0
            && crash.started_at_unix_ms <= crash.finished_at_unix_ms
            && crash.finished_at_unix_ms <= crash.successor_started_at_unix_ms
            && crash.successor_started_at_unix_ms <= checked_at
            && PUBLISHERS.iter().any(|(pool, service)| {
                crash.implementation_pool == *pool && crash.instance_key == format!("loadtest-{service}")
            })
            && successor.is_some()
            && historical.is_some()
            && clean_successor
            && pools.insert(crash.implementation_pool.clone())
            && killed.insert(crash.instance_id.clone());
        if !valid {
            errors.push(format!("invalid Quiver process-exit proof for {}", crash.instance_key));
        }
    }
    killed
}

fn check_routing(
    publishers: &[RoutedCollector],
    after: &RoutingState,
    killed: &BTreeSet<String>,
    errors: &mut Vec<String>,
) {
    check_unique_ids(
        after.collectors.iter().map(|collector| collector.instance_id.as_str()),
        "post-drain routing state",
        errors,
    );
    for publisher in publishers {
        let key = &publisher.instance_key;
        match after
            .collectors
            .iter()
            .find(|collector| collector.instance_id == publisher.instance_id)
        {
            None => errors.push(format!(
                "routed publisher {key} is absent from post-drain routing state; absence is not release"
            )),
            Some(collector)
                if collector.instance_key != publisher.instance_key
                    || collector.implementation_pool != publisher.implementation_pool =>
            {
                errors.push(format!("routed publisher {key} changed identity after the drain"));
            }
            Some(collector) if collector.state != "stopped" => errors.push(format!(
                "routed publisher {key} is {}, not cleanly stopped",
                collector.state
            )),
            Some(_) => {}
        }
    }
    for collector in &after.collectors {
        if !matches!(collector.state.as_str(), "stopped" | "failed") && !killed.contains(&collector.instance_id) {
            errors.push(format!(
                "collector {} generation is still live after the drain",
                collector.instance_key
            ));
        }
    }
    // V0008 routing leases remain after a clean stop; they are safe only when held by this
    // run's stopped publisher in the matching pool.
    let mut lease_keys = BTreeSet::new();
    for lease in &after.leases {
        if !lease_keys.insert((&lease.implementation_pool, &lease.table)) {
            errors.push("post-drain routing state repeats a table lease".to_owned());
        }
        let same_run = publishers.iter().any(|publisher| {
            publisher.instance_id == lease.holder_instance_id
                && publisher.implementation_pool == lease.implementation_pool
        });
        let stopped = after
            .collectors
            .iter()
            .any(|collector| collector.instance_id == lease.holder_instance_id && collector.state == "stopped");
        if !same_run || !stopped || !matches!(lease.state.as_str(), "active" | "draining") {
            errors.push(format!(
                "routing lease {}/{} is held by a live or unknown collector, or outside this run",
                lease.implementation_pool, lease.table
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const DATAFLOW: &str = "22222222-2222-4222-8222-222222222222";
    const HISTORICAL: &str = "33333333-3333-4333-8333-333333333333";

    fn collector(id: &str, key: &str, pool: &str, state: &str, terminated: bool) -> RoutedCollector {
        RoutedCollector {
            instance_id: id.to_owned(),
            instance_key: key.to_owned(),
            implementation_pool: pool.to_owned(),
            state: state.to_owned(),
            terminated,
        }
    }

    fn lease(holder: &str) -> RoutedLease {
        RoutedLease {
            implementation_pool: "dataflow".to_owned(),
            table: "telemetry.sqlserver.sqlserver_cpu_utilization".to_owned(),
            holder_instance_id: holder.to_owned(),
            state: "active".to_owned(),
            expires_at_unix_ms: Some(10_000),
        }
    }

    struct Case {
        drain: DrainEvidence,
        routed: RoutingState,
        after: RoutingState,
    }

    fn released() -> Case {
        Case {
            drain: DrainEvidence {
                metadata: json!({"run_id": "run-1"}),
                publication: json!({"run_id": "run-1", "dataflow_settled": true}),
                mutators: json!({"run_id": "run-1", "clean": true,
                    "services": ["dataflow-1", "tenant-stream", "delta-maintenance"]}),
                crashes: None,
            },
            routed: RoutingState {
                run_id: Some("run-1".to_owned()),
                loaded_at_unix_ms: 1,
                collectors: vec![collector(DATAFLOW, "loadtest-dataflow-1", "dataflow", "ready", false)],
                leases: vec![lease(DATAFLOW)],
            },
            after: RoutingState {
                run_id: None,
                loaded_at_unix_ms: 2,
                collectors: vec![collector(DATAFLOW, "loadtest-dataflow-1", "dataflow", "stopped", true)],
                leases: vec![lease(DATAFLOW)],
            },
        }
    }

    fn decide(case: Case) -> ReleaseReport {
        release_report(&case.drain, &case.routed, case.after, 3)
    }

    fn rejects(mutate: impl FnOnce(&mut Case), expected: &str) {
        let mut case = released();
        mutate(&mut case);
        let report = decide(case);
        assert!(!report.passed, "{expected}: must fail closed");
        assert!(
            report.errors.iter().any(|error| error.contains(expected)),
            "{expected} not in {:?}",
            report.errors
        );
    }

    #[test]
    fn exact_same_run_dataflow_release_passes() {
        let report = decide(released());
        assert!(report.passed, "{:?}", report.errors);
        assert_eq!(report.run_id.as_deref(), Some("run-1"));
        assert_eq!(
            report.publishers.iter().map(|publisher| publisher.instance_id.as_str()).collect::<Vec<_>>(),
            [DATAFLOW]
        );
    }

    #[test]
    fn drain_evidence_must_be_clean_and_bound_to_the_run() {
        rejects(|case| case.drain.metadata = json!({}), "no run identity");
        rejects(|case| case.routed.run_id = Some("old-run".to_owned()), "does not belong to this run");
        rejects(
            |case| case.drain.publication["dataflow_settled"] = json!(false),
            "publication drain evidence is not settled",
        );
        rejects(|case| case.drain.mutators["clean"] = json!(false), "mutator drain evidence is not clean");
        rejects(
            |case| case.drain.mutators["services"] = json!(["tenant-stream"]),
            "publisher dataflow-1 has no clean stop evidence",
        );
        rejects(
            |case| case.drain.mutators["services"] = json!(["dataflow-1", "dataflow-1"]),
            "unique known mutators",
        );
    }

    #[test]
    fn routing_evidence_requires_exactly_one_dataflow_publisher() {
        rejects(|case| case.routed.collectors.clear(), "no live dataflow-1 publisher");
        rejects(
            |case| case.routed.collectors.push(collector(HISTORICAL, "loadtest-dataflow-1", "dataflow", "ready", false)),
            "2 live dataflow generations",
        );
        rejects(
            |case| case.routed.collectors.push(collector(HISTORICAL, "other", "tenant", "ready", false)),
            "unknown pool tenant",
        );
        rejects(
            |case| case.routed.collectors.push(collector(DATAFLOW, "duplicate", "dataflow", "ready", false)),
            "repeats collector instance",
        );
    }

    #[test]
    fn post_drain_routing_must_prove_stopped_publisher_and_valid_lease() {
        rejects(|case| case.after.collectors.clear(), "absence is not release");
        rejects(
            |case| case.after.collectors[0].state = "ready".to_owned(),
            "is ready, not cleanly stopped",
        );
        rejects(
            |case| case.after.collectors.push(collector(HISTORICAL, "another", "dataflow", "ready", false)),
            "still live after the drain",
        );
        rejects(|case| case.after.leases = vec![lease(HISTORICAL)], "outside this run");
        rejects(|case| case.after.leases.push(lease(DATAFLOW)), "repeats a table lease");
    }

    fn observed_crash() -> Case {
        let mut case = released();
        case.after.collectors.push(collector(
            HISTORICAL,
            "loadtest-dataflow-1",
            "dataflow",
            "failed",
            true,
        ));
        case.drain.mutators["containers"] = json!([
            {"id": "b".repeat(64), "service": "dataflow-1", "started_at_unix_ms": 3}
        ]);
        case.drain.crashes = Some(CrashEvidence {
            run_id: "run-1".to_owned(),
            publishers: vec![CrashedPublisher {
                implementation_pool: "dataflow".to_owned(),
                instance_key: "loadtest-dataflow-1".to_owned(),
                instance_id: HISTORICAL.to_owned(),
                container_id: "a".repeat(64),
                started_at_unix_ms: 1,
                finished_at_unix_ms: 2,
                exit_code: 137,
                successor_instance_id: DATAFLOW.to_owned(),
                successor_container_id: "b".repeat(64),
                successor_started_at_unix_ms: 3,
            }],
        });
        case
    }

    #[test]
    fn same_run_sigkill_and_clean_successor_proves_historical_process_exit() {
        let report = decide(observed_crash());
        assert!(report.passed, "{:?}", report.errors);
    }

    #[test]
    fn malformed_or_incomplete_crash_evidence_fails_closed() {
        let mutations: [fn(&mut CrashEvidence); 7] = [
            |proof| proof.run_id = "other".to_owned(),
            |proof| proof.publishers.clear(),
            |proof| proof.publishers[0].exit_code = 0,
            |proof| proof.publishers[0].instance_id = "bad-id".to_owned(),
            |proof| proof.publishers[0].container_id = "bad".to_owned(),
            |proof| proof.publishers[0].instance_key = "wrong".to_owned(),
            |proof| proof.publishers[0].successor_started_at_unix_ms = 4,
        ];
        for mutate in mutations {
            let mut case = observed_crash();
            mutate(case.drain.crashes.as_mut().expect("crash evidence"));
            assert!(!decide(case).passed);
        }
        let mut case = observed_crash();
        case.drain.mutators["containers"] = json!([]);
        assert!(!decide(case).passed, "successor needs positive clean-stop evidence");
    }

    #[test]
    fn invalid_collector_id_is_rejected() {
        assert!(canonical_id("not-a-uuid").is_err());
    }
}
