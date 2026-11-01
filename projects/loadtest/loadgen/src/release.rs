//! Positive publisher-release proof, admitted before any post-drain maintenance.
//!
//! The runner first stops every mutator container and records that each exited cleanly. That only
//! proves the processes ended. This command then reads the actual SQL state the publishers used and
//! proves, for the same run, that:
//!
//! - every publisher this run routed to (exactly one live direct and one live dataflow generation in
//!   the run's `routing.json`) is durably `stopped` in routing under the same id, key, and pool;
//! - every collector generation has an explicit stopped/failed state or same-run owned SIGKILL and
//!   successor process proof; heartbeat expiry alone is not enough;
//!   each routing lease belongs to a same-run stopped publisher in the matching pool;
//! - the direct receipt owner finished its owner drain (`stopped` with a stop time and no work), no
//!   receipt owner is still starting, active, or draining, and no table lease or unowned delivery
//!   receipt is still active.
//!
//! A row absent from SQL is never treated as stopped. Terminated historical generations are allowed;
//! outstanding receipts still fail admission, even for terminal historical owners. The report is
//! written for both outcomes and never invents a run identity.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use arrowcol_protocol::UnixMillis;
use arrowcol_routing::{RoutingStore, RoutingView, SqlServerRoutingConfig, SqlServerRoutingStore};
use arrowcol_sql_server::{ConnectionConfig, SqlConnection};
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

/// Source publisher pools and the mutator service that hosts each one.
const PUBLISHERS: [(&str, &str); 2] = [("direct", "direct-1"), ("dataflow", "dataflow-1")];
/// Mutator services the runner may record as cleanly stopped.
const MUTATORS: [&str; 4] = ["direct-1", "dataflow-1", "tenant-stream", "delta-maintenance"];
/// Receipt-owner states that can no longer accept, buffer, or flush work.
const TERMINAL_OWNER_STATES: [&str; 2] = ["stopped", "failed"];

/// Every receipt-owner generation with its outstanding work, then whole-database totals, in one
/// read-only batch. The predicates are the ones the owner drain itself uses.
const RECEIPT_STATE_SQL: &str = "\
SELECT LOWER(CONVERT(char(36), c.instance_id)) AS instance_id, c.instance_key, c.generation, c.state, \
       CAST(CASE WHEN c.stopped_at_utc IS NULL THEN 0 ELSE 1 END AS bit) AS stopped_recorded, \
       (SELECT COUNT_BIG(*) FROM arrowcol.table_leases AS l \
        WHERE l.holder_instance_id = c.instance_id AND l.released_at_utc IS NULL) AS unreleased_leases, \
       (SELECT COUNT_BIG(*) FROM arrowcol.delivery_receipts AS r \
        WHERE r.owner_instance_id = c.instance_id AND r.state IN ('accepted', 'buffered', 'flushing')) AS active_receipts \
FROM arrowcol.collector_instances AS c ORDER BY c.instance_key, c.generation; \
SELECT DB_NAME() AS database_name, \
       (SELECT COUNT_BIG(*) FROM arrowcol.table_leases WHERE released_at_utc IS NULL) AS unreleased_leases, \
       (SELECT COUNT_BIG(*) FROM arrowcol.delivery_receipts \
        WHERE state IN ('accepted', 'buffered', 'flushing')) AS active_receipts;";

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

/// One receipt-owner generation and the work it still holds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReceiptOwner {
    pub instance_id: String,
    pub instance_key: String,
    pub generation: u64,
    pub state: String,
    pub stopped_recorded: bool,
    pub unreleased_leases: u64,
    pub active_receipts: u64,
}

/// Receipt-owner rows plus whole-database outstanding work.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReceiptState {
    pub database: String,
    pub owners: Vec<ReceiptOwner>,
    pub unreleased_leases: u64,
    pub active_receipts: u64,
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
    /// Post-drain routing state read from SQL.
    pub routing: Option<RoutingState>,
    /// Post-drain receipt-owner state read from SQL.
    pub receipts: Option<ReceiptState>,
    /// Active receipts left by terminated historical owners that no live process can write.
    pub historical_active_receipts: u64,
    pub errors: Vec<String>,
}

/// Reads this run's drain receipts and the actual post-drain SQL state, then writes the proof.
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
                receipts: None,
                historical_active_receipts: 0,
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
            receipts: None,
            historical_active_receipts: 0,
            errors: admission_errors,
        });
    }
    let timeout = arguments.timeout.get();
    ensure!(!timeout.is_zero(), "publisher-release SQL bound must be positive");
    let after = read_routing(arguments, timeout).await?;
    let receipts = read_receipts(arguments, timeout).await?;
    Ok(release_report(&drain, &routed, after, receipts, checked_at))
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

async fn read_receipts(arguments: &PublisherReleaseArgs, timeout: Duration) -> Result<ReceiptState> {
    let password = fs::read_to_string(&arguments.sql_password_file)
        .await
        .context("could not read SQL Server password file")?;
    let password = password.trim_end_matches(['\r', '\n']);
    ensure!(
        !password.is_empty() && !password.contains(['\r', '\n']),
        "SQL Server password file must contain one non-empty line"
    );
    let config = ConnectionConfig::new(
        &arguments.sql_host,
        arguments.sql_port,
        &arguments.sql_database,
        &arguments.sql_username,
        password,
    )
    .trust_server_certificate(true)
    .with_timeouts(timeout, timeout);
    let sets = tokio::time::timeout(timeout, async {
        let mut connection = SqlConnection::connect(&config).await?;
        let sets = connection.query(RECEIPT_STATE_SQL, &[]).await?.into_results().await?;
        anyhow::Ok(sets)
    })
    .await
    .context("post-drain receipt-owner read exceeded its bound")??;
    let [owners, totals] = sets.as_slice() else {
        anyhow::bail!("receipt-owner read returned {} result sets, expected 2", sets.len());
    };
    let [totals] = totals.as_slice() else {
        anyhow::bail!("receipt totals returned {} rows, expected 1", totals.len());
    };
    Ok(ReceiptState {
        database: text(totals, "database_name")?,
        owners: owners
            .iter()
            .map(|row| {
                Ok(ReceiptOwner {
                    instance_id: canonical_id(&text(row, "instance_id")?)?,
                    instance_key: text(row, "instance_key")?,
                    generation: count(row, "generation")?,
                    state: text(row, "state")?,
                    stopped_recorded: row
                        .try_get::<bool, _>("stopped_recorded")?
                        .context("stopped_recorded is null")?,
                    unreleased_leases: count(row, "unreleased_leases")?,
                    active_receipts: count(row, "active_receipts")?,
                })
            })
            .collect::<Result<_>>()?,
        unreleased_leases: count(totals, "unreleased_leases")?,
        active_receipts: count(totals, "active_receipts")?,
    })
}

fn text(row: &tiberius::Row, column: &str) -> Result<String> {
    Ok(row
        .try_get::<&str, _>(column)?
        .with_context(|| format!("{column} is null"))?
        .to_owned())
}

fn count(row: &tiberius::Row, column: &str) -> Result<u64> {
    let value = row
        .try_get::<i64, _>(column)?
        .with_context(|| format!("{column} is null"))?;
    u64::try_from(value).with_context(|| format!("{column} is negative"))
}

/// Normalizes a textual instance identity so routing and receipt rows compare exactly.
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
    receipts: ReceiptState,
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
    let historical_active_receipts = check_receipts(&publishers, &receipts, &mut errors);
    ReleaseReport {
        passed: errors.is_empty(),
        run_id,
        checked_at_unix_ms,
        publishers,
        routing: Some(after),
        receipts: Some(receipts),
        historical_active_receipts,
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
    // V0008 and RoutingLease allow only active/draining: clean stop does not delete or release these
    // routing rows. A persistent row is safe here only when its same-run publisher is stopped.
    // Receipt table leases, unlike routing leases, must actually be released below.
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

/// Returns active receipts left by terminated historical owners.
fn check_receipts(publishers: &[RoutedCollector], receipts: &ReceiptState, errors: &mut Vec<String>) -> u64 {
    check_unique_ids(
        receipts.owners.iter().map(|owner| owner.instance_id.as_str()),
        "receipt-owner state",
        errors,
    );
    for publisher in publishers {
        let owner = receipts
            .owners
            .iter()
            .find(|owner| owner.instance_id == publisher.instance_id);
        let key = &publisher.instance_key;
        match owner {
            None if publisher.implementation_pool == "direct" => errors.push(format!(
                "direct publisher {key} has no receipt-owner row; absence is not a finished owner drain"
            )),
            None => {}
            Some(owner) => {
                if owner.instance_key != publisher.instance_key {
                    errors.push(format!("receipt owner {key} has a different instance key"));
                }
                if owner.state != "stopped" || !owner.stopped_recorded {
                    errors.push(format!("receipt owner {key} did not finish its owner drain"));
                }
                if owner.unreleased_leases != 0 || owner.active_receipts != 0 {
                    errors.push(format!("receipt owner {key} still holds leases or active receipts"));
                }
            }
        }
    }
    let mut owned_active = 0_u64;
    let mut historical = 0_u64;
    for owner in &receipts.owners {
        owned_active = owned_active.saturating_add(owner.active_receipts);
        if !TERMINAL_OWNER_STATES.contains(&owner.state.as_str()) {
            errors.push(format!(
                "receipt owner {} generation {} is still {}",
                owner.instance_key, owner.generation, owner.state
            ));
        } else if !publishers
            .iter()
            .any(|publisher| publisher.instance_id == owner.instance_id)
        {
            historical = historical.saturating_add(owner.active_receipts);
        }
    }
    if receipts.unreleased_leases != 0 {
        errors.push(format!(
            "{} receipt table leases remain unreleased",
            receipts.unreleased_leases
        ));
    }
    if receipts.active_receipts != owned_active {
        errors.push("receipt-owner and global active counts disagree".to_owned());
    }
    if receipts.active_receipts > owned_active {
        errors.push(format!(
            "{} active delivery receipts have no registered owner",
            receipts.active_receipts - owned_active
        ));
    }
    if historical != 0 {
        errors.push("historical owners still hold active receipts".to_owned());
    }
    historical
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use arrowcol_protocol::{CollectorInstanceId, ImplementationPool, PositiveMeasurement};
    use arrowcol_routing::{CollectorRegistration, CollectorState, MemoryRoutingStore};
    use serde_json::json;

    use super::*;

    const DIRECT: &str = "11111111-1111-4111-8111-111111111111";
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
            implementation_pool: "direct".to_owned(),
            table: "telemetry.sqlserver.sqlserver_cpu_utilization".to_owned(),
            holder_instance_id: holder.to_owned(),
            state: "active".to_owned(),
            expires_at_unix_ms: Some(10_000),
        }
    }

    fn owner(id: &str, key: &str, generation: u64, state: &str, active_receipts: u64) -> ReceiptOwner {
        ReceiptOwner {
            instance_id: id.to_owned(),
            instance_key: key.to_owned(),
            generation,
            state: state.to_owned(),
            stopped_recorded: state == "stopped",
            unreleased_leases: 0,
            active_receipts,
        }
    }

    struct Case {
        drain: DrainEvidence,
        routed: RoutingState,
        after: RoutingState,
        receipts: ReceiptState,
    }

    /// A crash-restarted run with a terminal historical generation and no unpublished receipts.
    fn released() -> Case {
        Case {
            drain: DrainEvidence {
                metadata: json!({"run_id": "run-1"}),
                publication: json!({"run_id": "run-1", "dataflow_settled": true}),
                mutators: json!({"run_id": "run-1", "clean": true,
                    "services": ["direct-1", "dataflow-1", "tenant-stream", "delta-maintenance"]}),
                crashes: None,
            },
            routed: RoutingState {
                run_id: Some("run-1".to_owned()),
                loaded_at_unix_ms: 1,
                collectors: vec![
                    collector(HISTORICAL, "loadtest-direct-1", "direct", "failed", true),
                    collector(DIRECT, "loadtest-direct-1", "direct", "ready", false),
                    collector(DATAFLOW, "loadtest-dataflow-1", "dataflow", "ready", false),
                ],
                leases: vec![lease(DIRECT)],
            },
            after: RoutingState {
                run_id: None,
                loaded_at_unix_ms: 2,
                collectors: vec![
                    collector(HISTORICAL, "loadtest-direct-1", "direct", "failed", true),
                    collector(DIRECT, "loadtest-direct-1", "direct", "stopped", true),
                    collector(DATAFLOW, "loadtest-dataflow-1", "dataflow", "stopped", true),
                ],
                leases: vec![lease(DIRECT)],
            },
            receipts: ReceiptState {
                database: "arrowcol".to_owned(),
                owners: vec![
                    owner(HISTORICAL, "loadtest-direct-1", 1, "failed", 0),
                    owner(DIRECT, "loadtest-direct-1", 2, "stopped", 0),
                ],
                unreleased_leases: 0,
                active_receipts: 0,
            },
        }
    }

    fn decide(case: Case) -> ReleaseReport {
        release_report(&case.drain, &case.routed, case.after, case.receipts, 3)
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
    fn exact_same_run_release_passes_and_reports_historical_receipts() {
        let report = decide(released());
        assert!(report.passed, "{:?}", report.errors);
        assert_eq!(report.run_id.as_deref(), Some("run-1"));
        assert_eq!(
            report
                .publishers
                .iter()
                .map(|publisher| publisher.instance_id.as_str())
                .collect::<Vec<_>>(),
            [DIRECT, DATAFLOW]
        );
        assert_eq!(report.historical_active_receipts, 0);
    }

    fn observed_crash() -> Case {
        let mut case = released();
        let historical_dataflow = "44444444-4444-4444-8444-444444444444";
        case.after.collectors[0].state = "ready".to_owned();
        case.after.collectors.push(collector(
            historical_dataflow,
            "loadtest-dataflow-1",
            "dataflow",
            "ready",
            true,
        ));
        case.drain.mutators["containers"] = json!([
            {"id": "a".repeat(64), "service": "direct-1", "started_at_unix_ms": 3},
            {"id": "b".repeat(64), "service": "dataflow-1", "started_at_unix_ms": 3}
        ]);
        case.drain.crashes = Some(CrashEvidence {
            run_id: "run-1".to_owned(),
            publishers: [
                ("direct", HISTORICAL, DIRECT, 'a'),
                ("dataflow", historical_dataflow, DATAFLOW, 'b'),
            ]
            .into_iter()
            .map(|(pool, instance, successor, container)| CrashedPublisher {
                implementation_pool: pool.to_owned(),
                instance_key: format!("loadtest-{pool}-1"),
                instance_id: instance.to_owned(),
                container_id: container.to_string().repeat(64),
                started_at_unix_ms: 1,
                finished_at_unix_ms: 2,
                exit_code: 137,
                successor_instance_id: successor.to_owned(),
                successor_container_id: container.to_string().repeat(64),
                successor_started_at_unix_ms: 3,
            })
            .collect(),
        });
        case
    }

    #[test]
    fn observed_sigkill_with_exact_clean_successors_proves_historical_process_exit() {
        let report = decide(observed_crash());
        assert!(report.passed, "{:?}", report.errors);
    }

    #[test]
    fn crash_proof_cannot_substitute_for_current_publisher_or_receipt_release() {
        let mut case = observed_crash();
        case.after.collectors[1].state = "ready".to_owned();
        assert!(!decide(case).passed, "current publisher must still be SQL stopped");
        let mut case = observed_crash();
        case.after.leases = vec![lease(HISTORICAL)];
        assert!(
            !decide(case).passed,
            "historical routing leases are never released by process proof"
        );
        let mut case = observed_crash();
        case.receipts.owners[0].active_receipts = 1;
        case.receipts.active_receipts = 1;
        assert!(!decide(case).passed, "historical active receipts remain unresolved");
    }

    #[test]
    fn malformed_partial_or_cross_run_crash_proof_fails_closed() {
        let mutations: [fn(&mut CrashEvidence); 12] = [
            |proof| proof.run_id = "other".to_owned(),
            |proof| {
                proof.publishers.pop();
            },
            |proof| proof.publishers[0].exit_code = 0,
            |proof| proof.publishers[0].instance_id = DIRECT.to_owned(),
            |proof| proof.publishers[0].successor_instance_id = HISTORICAL.to_owned(),
            |proof| proof.publishers[0].container_id = "bad".to_owned(),
            |proof| proof.publishers[0].successor_container_id = "c".repeat(64),
            |proof| proof.publishers[0].started_at_unix_ms = 0,
            |proof| proof.publishers[0].finished_at_unix_ms = 4,
            |proof| proof.publishers[0].successor_started_at_unix_ms = 4,
            |proof| proof.publishers[0].instance_key = "loadtest-direct-2".to_owned(),
            |proof| proof.publishers[1] = proof.publishers[0].clone(),
        ];
        for mutate in mutations {
            let mut case = observed_crash();
            if let Some(proof) = &mut case.drain.crashes {
                mutate(proof);
            }
            assert!(!decide(case).passed);
        }
        let mut case = observed_crash();
        case.after.collectors[0].terminated = false;
        assert!(!decide(case).passed, "a live historical registration is never release");
        let mut case = observed_crash();
        case.drain.mutators["containers"] = json!([]);
        assert!(
            !decide(case).passed,
            "successor container must have positive clean-stop evidence"
        );
    }

    #[test]
    fn drain_receipts_must_belong_to_this_run_and_be_clean() {
        rejects(|case| case.drain.metadata = json!({}), "no run identity");
        rejects(
            |case| case.routed.run_id = Some("old-run".to_owned()),
            "does not belong to this run",
        );
        rejects(
            |case| case.routed.collectors[1].instance_key = "wrong".to_owned(),
            "unexpected direct-1",
        );
        rejects(
            |case| case.drain.publication["run_id"] = json!("run-0"),
            "publication drain evidence is not settled",
        );
        rejects(
            |case| case.drain.publication["dataflow_settled"] = json!(false),
            "publication drain evidence is not settled",
        );
        rejects(
            |case| case.drain.mutators["clean"] = json!(false),
            "mutator drain evidence is not clean",
        );
        rejects(
            |case| case.drain.mutators["run_id"] = json!("run-0"),
            "mutator drain evidence is not clean",
        );
        rejects(
            |case| case.drain.mutators["services"] = json!(["direct-1"]),
            "publisher dataflow-1 has no clean stop evidence",
        );
        rejects(
            |case| case.drain.mutators["services"] = json!(["direct-1", "dataflow-1", "direct-1"]),
            "unique known mutators",
        );
        rejects(
            |case| case.drain.mutators["services"] = json!(["direct-1", "dataflow-1", "router"]),
            "unique known mutators",
        );
        rejects(
            |case| case.drain.mutators["services"] = json!(null),
            "no stopped services",
        );
    }

    #[test]
    fn publishers_bind_exactly_one_live_generation_per_pool_from_the_run() {
        rejects(
            |case| {
                case.routed
                    .collectors
                    .retain(|collector| collector.instance_id != DIRECT)
            },
            "no live direct-1 publisher",
        );
        rejects(
            |case| {
                case.routed
                    .collectors
                    .push(collector(HISTORICAL, "loadtest-dataflow-1", "dataflow", "ready", false));
            },
            "2 live dataflow generations",
        );
        rejects(
            |case| {
                case.routed
                    .collectors
                    .push(collector(DIRECT, "loadtest-direct-1", "direct", "ready", false))
            },
            "run routing evidence repeats collector instance",
        );
        rejects(
            |case| {
                case.routed
                    .collectors
                    .push(collector(HISTORICAL, "other", "tenant", "ready", false));
            },
            "unknown pool tenant",
        );
    }

    #[test]
    fn missing_or_unclean_post_drain_routing_is_never_release() {
        rejects(
            |case| case.after.leases = vec![lease(DATAFLOW)],
            "held by a live or unknown collector",
        );
        rejects(|case| case.after.leases.push(lease(DIRECT)), "repeats a table lease");
        rejects(|case| case.after.leases = vec![lease(HISTORICAL)], "outside this run");
        rejects(
            |case| {
                case.after.collectors[0].state = "ready".to_owned();
                case.after.collectors[0].terminated = true;
            },
            "still live after the drain",
        );
        rejects(
            |case| {
                case.after.collectors[0].state = "ready".to_owned();
                case.after.collectors[0].terminated = true;
                case.after.leases = vec![lease(HISTORICAL)];
            },
            "held by a live or unknown collector",
        );
        rejects(
            |case| {
                case.after
                    .collectors
                    .retain(|collector| collector.instance_id != DATAFLOW)
            },
            "absence is not release",
        );
        rejects(
            |case| {
                case.after.collectors[2].state = "failed".to_owned();
            },
            "is failed, not cleanly stopped",
        );
        rejects(
            |case| {
                case.after.collectors[1].instance_key = "loadtest-direct-2".to_owned();
            },
            "changed identity",
        );
        rejects(
            |case| {
                case.after.collectors[2].implementation_pool = "direct".to_owned();
            },
            "changed identity",
        );
        rejects(
            |case| {
                case.after.collectors.push(collector(
                    "44444444-4444-4444-8444-444444444444",
                    "loadtest-direct-1",
                    "direct",
                    "ready",
                    false,
                ));
            },
            "still live after the drain",
        );
        rejects(
            |case| case.after.leases = vec![lease("55555555-5555-4555-8555-555555555555")],
            "held by a live or unknown collector",
        );
        rejects(
            |case| case.after.collectors.push(case.after.collectors[0].clone()),
            "post-drain routing state repeats",
        );
    }

    #[test]
    fn direct_owner_drain_must_be_finished_with_no_outstanding_work() {
        rejects(
            |case| case.receipts.owners.retain(|owner| owner.instance_id != DIRECT),
            "absence is not a finished owner drain",
        );
        rejects(
            |case| case.receipts.owners[1].state = "draining".to_owned(),
            "did not finish its owner drain",
        );
        rejects(
            |case| case.receipts.owners[1].stopped_recorded = false,
            "did not finish its owner drain",
        );
        rejects(
            |case| case.receipts.owners[1].instance_key = "other".to_owned(),
            "different instance key",
        );
        rejects(
            |case| {
                case.receipts.owners[1].unreleased_leases = 1;
                case.receipts.unreleased_leases = 1;
            },
            "still holds leases or active receipts",
        );
        rejects(
            |case| {
                case.receipts.owners[1].active_receipts = 1;
                case.receipts.active_receipts = 3;
            },
            "still holds leases or active receipts",
        );
    }

    #[test]
    fn no_live_owner_lease_or_unowned_receipt_may_remain() {
        rejects(
            |case| {
                case.receipts.owners[0].active_receipts = 1;
                case.receipts.active_receipts = 1;
            },
            "historical owners still hold active receipts",
        );
        rejects(
            |case| {
                case.receipts.owners.push(owner(
                    "66666666-6666-4666-8666-666666666666",
                    "loadtest-direct-1",
                    3,
                    "active",
                    0,
                ));
            },
            "generation 3 is still active",
        );
        rejects(
            |case| {
                case.receipts
                    .owners
                    .push(owner("66666666-6666-4666-8666-666666666666", "other", 1, "unknown", 0));
            },
            "is still unknown",
        );
        rejects(
            |case| case.receipts.unreleased_leases = 1,
            "1 receipt table leases remain unreleased",
        );
        rejects(
            |case| case.receipts.active_receipts = 1,
            "1 active delivery receipts have no registered owner",
        );
        rejects(
            |case| {
                case.receipts
                    .owners
                    .push(owner(DATAFLOW, "loadtest-dataflow-1", 1, "failed", 0))
            },
            "receipt owner loadtest-dataflow-1 did not finish",
        );
        rejects(
            |case| case.receipts.owners.push(case.receipts.owners[1].clone()),
            "receipt-owner state repeats",
        );
    }

    #[test]
    fn instance_identities_are_compared_canonically() -> Result<()> {
        let routed = canonical_routing(RoutingState {
            run_id: None,
            loaded_at_unix_ms: 1,
            collectors: vec![collector(
                &DIRECT.to_uppercase().replace('-', ""),
                "loadtest-direct-1",
                "direct",
                "ready",
                false,
            )],
            leases: vec![lease(&DIRECT.to_uppercase())],
        })?;
        assert_eq!(routed.collectors[0].instance_id, DIRECT);
        assert_eq!(routed.leases[0].holder_instance_id, DIRECT);
        assert!(canonical_id("not-a-uuid").is_err());
        Ok(())
    }

    /// The adapter keeps stopped, failed, and lease-expired generations instead of filtering them.
    #[tokio::test]
    async fn routing_adapter_returns_terminated_generations() -> Result<()> {
        let store = MemoryRoutingStore::default();
        let registration = |instance_id, pool, expires| -> Result<CollectorRegistration> {
            Ok(CollectorRegistration {
                instance_id,
                instance_key: format!("loadtest-{pool}"),
                implementation_pool: pool,
                flight_endpoint: "127.0.0.1:50051".parse()?,
                catalog_capable: true,
                max_concurrent_streams: PositiveMeasurement::new(8)?,
                started_at_unix_ms: UnixMillis::new(1_000)?,
                heartbeat_at_unix_ms: UnixMillis::new(1_000)?,
                lease_expires_at_unix_ms: UnixMillis::new(expires)?,
            })
        };
        let (stopped, failed, expired, live) = (
            CollectorInstanceId::new(),
            CollectorInstanceId::new(),
            CollectorInstanceId::new(),
            CollectorInstanceId::new(),
        );
        for (id, pool, expires) in [
            (stopped, ImplementationPool::Direct, 9_000),
            (failed, ImplementationPool::Dataflow, 9_000),
            (expired, ImplementationPool::Direct, 1_500),
            (live, ImplementationPool::Dataflow, 9_000),
        ] {
            store.register_collector(&registration(id, pool, expires)?).await?;
        }
        store
            .set_collector_state(stopped, CollectorState::Stopped, UnixMillis::new(1_100)?)
            .await?;
        store
            .set_collector_state(failed, CollectorState::Failed, UnixMillis::new(1_100)?)
            .await?;
        let state = canonical_routing(routing_state(&store.load_view(UnixMillis::new(2_000)?).await?))?;
        let find = |id: CollectorInstanceId| {
            state
                .collectors
                .iter()
                .find(|collector| collector.instance_id == id.to_string())
                .map(|collector| (collector.state.clone(), collector.terminated))
        };
        assert_eq!(find(stopped), Some(("stopped".to_owned(), true)));
        assert_eq!(find(failed), Some(("failed".to_owned(), true)));
        assert_eq!(find(expired), Some(("ready".to_owned(), true)));
        assert_eq!(find(live), Some(("ready".to_owned(), false)));
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires an explicitly authorized live arrowcol-infra SQL Server"]
    async fn sql_load_view_retains_historical_terminal_rows() -> Result<()> {
        ensure!(
            std::env::var("ARROWCOL_SQLSERVER_TEST").as_deref() == Ok("1"),
            "live SQL test requires explicit ARROWCOL_SQLSERVER_TEST=1"
        );
        let host = std::env::var("ARROWCOL_SQLSERVER_HOST")?;
        let password = std::env::var("ARROWCOL_SQL_PASSWORD_FILE")?;
        let store = SqlServerRoutingStore::new(
            SqlServerRoutingConfig::with_password_file(host, 1_433, "arrowcol", "arrowcol_app", password)?
                .trust_server_certificate(true),
        );
        let now = now_unix_millis();
        let id = CollectorInstanceId::new();
        store
            .register_collector(&CollectorRegistration {
                instance_id: id,
                instance_key: format!("release-query-test-{id}"),
                implementation_pool: ImplementationPool::Direct,
                flight_endpoint: "127.0.0.1:50051".parse()?,
                catalog_capable: true,
                max_concurrent_streams: PositiveMeasurement::new(1)?,
                started_at_unix_ms: UnixMillis::new(now)?,
                heartbeat_at_unix_ms: UnixMillis::new(now)?,
                lease_expires_at_unix_ms: UnixMillis::new(now + 60_000)?,
            })
            .await?;
        store
            .set_collector_state(id, CollectorState::Stopped, UnixMillis::new(now + 1)?)
            .await?;
        let loaded = routing_state(&store.load_view(UnixMillis::new(now + 2)?).await?);
        let row = loaded
            .collectors
            .iter()
            .find(|row| row.instance_id == id.to_string())
            .context("SQL load_view filtered out a stopped historical row")?;
        ensure!(
            row.state == "stopped" && row.terminated,
            "SQL adapter lost terminal state"
        );
        Ok(())
    }
}
