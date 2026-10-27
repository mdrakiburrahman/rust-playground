use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::Path,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use delta_inspect::{SnapshotSummary, TableSummary};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::fs;

use crate::{
    cli::ValidateArgs,
    manifest::{
        AckOutcome, CAPABILITY_EVIDENCE_FILE, RECEIPT_MANIFEST_FILE, ROUTING_EVIDENCE_FILE, ReceiptManifestEntry,
        SCHEMA_CONTRACT_FILE, SEND_MANIFEST_FILE, SchemaContract, SendManifestEntry, VALIDATION_FILE,
    },
};

/// Wait for real publication queues while their admin endpoints are still alive.
///
/// # Errors
/// Rejects missing run metadata, unreachable collectors, and undrained publication.
pub async fn drain(arguments: ValidateArgs) -> Result<()> {
    let metadata: Value = read_json(&arguments.results.join("run-metadata.json")).await?;
    let run_id = metadata["run_id"].as_str().context("missing drain run identity")?;
    crate::kusto::settle_dataflow(
        &arguments.dataflow_admin_endpoints,
        arguments.timeout.get(),
        arguments.quiver,
    )
    .await?;
    fs::write(
        arguments.results.join("publication-drain.json"),
        serde_json::to_vec_pretty(&serde_json::json!({"run_id": run_id, "dataflow_settled": true}))?,
    )
    .await?;
    Ok(())
}

/// A runner deadline further away than this is a unit or clock error, not a validation budget.
const MAX_VALIDATE_BUDGET: Duration = Duration::from_secs(24 * 60 * 60);

/// The validate phase that first failed, so a bounded failure is never reported blank.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum ValidatePhase {
    Deadline,
    Inputs,
    DrainReceipts,
    SettleDataflow,
    SnapshotDirect,
    SnapshotDataflow,
    Kusto,
}

/// One absolute bound for the whole validate command. The runner's wall-clock deadline is
/// converted to a monotonic instant once, so later wall-clock changes cannot extend it.
#[derive(Clone, Debug)]
struct Budget {
    timings: delta_inspect::scheduler::OracleTimings,
    deadline: Option<Instant>,
    deadline_unix_ms: Option<u64>,
}

impl Budget {
    fn new(deadline_unix_ms: Option<u64>, now_unix: Duration, now: Instant) -> Result<Self> {
        let Some(deadline_unix_ms) = deadline_unix_ms else {
            return Ok(Self {
                timings: delta_inspect::scheduler::OracleTimings::default(),
                deadline: None,
                deadline_unix_ms: None,
            });
        };
        let now_ms = u64::try_from(now_unix.as_millis()).context("wall clock overflow")?;
        // A deadline already in the past becomes an expired budget, which fails visibly.
        let remaining = Duration::from_millis(deadline_unix_ms.saturating_sub(now_ms));
        ensure!(
            remaining <= MAX_VALIDATE_BUDGET,
            "validate deadline is more than {MAX_VALIDATE_BUDGET:?} away"
        );
        Ok(Self {
            timings: delta_inspect::scheduler::OracleTimings::default(),
            deadline: Some(now.checked_add(remaining).context("validate deadline overflow")?),
            deadline_unix_ms: Some(deadline_unix_ms),
        })
    }

    fn from_clock(deadline_unix_ms: Option<u64>) -> Result<Self> {
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("wall clock is before the Unix epoch")?;
        Self::new(deadline_unix_ms, now_unix, Instant::now())
    }

    fn expired_at(&self, now: Instant) -> bool {
        self.deadline.is_some_and(|deadline| now >= deadline)
    }

    /// A phase's own budget, shortened to what remains of the absolute bound.
    fn clamp_at(&self, own: Duration, now: Instant) -> Duration {
        self.deadline
            .map_or(own, |deadline| own.min(deadline.saturating_duration_since(now)))
    }

    fn clamp(&self, own: Duration) -> Duration {
        self.clamp_at(own, Instant::now())
    }

    /// Runs one phase under the absolute bound; reaching it drops the phase, cancelling in-flight
    /// requests at their next await. Cancellation is cooperative only: synchronous reader stretches
    /// cannot be preempted and may overrun the bound (and the runner's reserve), so a result that
    /// completes at or after the bound is rejected rather than accepted late. Without a bound the
    /// phase runs exactly as before.
    async fn within<T>(&self, phase: ValidatePhase, work: impl Future<Output = Result<T>>) -> Result<T> {
        let timer = self.timings.measure(format!("phase/{phase:?}"));
        let result = async {
            let Some(deadline) = self.deadline else {
                return work.await;
            };
            ensure!(
                Instant::now() < deadline,
                "validate deadline elapsed before phase {phase:?} started"
            );
            let value = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), work)
                .await
                .map_err(|_| anyhow::anyhow!("phase {phase:?} reached the validate deadline"))??;
            ensure!(
                Instant::now() < deadline,
                "phase {phase:?} completed only after the validate deadline"
            );
            Ok(value)
        }
        .await;
        timer.finish(result.is_ok());
        result
    }

    fn evidence(&self) -> Option<Value> {
        let deadline = self.deadline?;
        Some(serde_json::json!({
            "deadline_unix_ms": self.deadline_unix_ms,
            "remaining_ms": u64::try_from(deadline.saturating_duration_since(Instant::now()).as_millis())
                .unwrap_or(u64::MAX),
        }))
    }
}

fn check_drain_receipts(metadata: &Value, publication: &Value, mutators: &Value) -> Result<()> {
    let run_id = metadata["run_id"].as_str().context("missing validation run identity")?;
    anyhow::ensure!(
        publication["run_id"].as_str() == Some(run_id)
            && publication["dataflow_settled"].as_bool() == Some(true)
            && mutators["run_id"].as_str() == Some(run_id)
            && mutators["clean"].as_bool() == Some(true),
        "publication and mutator drain evidence must match the captured run"
    );
    let services = mutators["services"]
        .as_array()
        .context("missing clean mutator identities")?;
    anyhow::ensure!(
        ["direct-1", "dataflow-1"]
            .iter()
            .all(|required| services.iter().any(|service| service.as_str() == Some(*required))),
        "both source collectors must have clean stopped evidence"
    );
    Ok(())
}

/// Independently validates manifests, direct/dataflow Delta snapshots, routing,
/// delivery guarantees, and paired logical parity.
///
/// # Errors
///
/// Returns malformed evidence/snapshot failures or an aggregate failed validation.
pub async fn validate(arguments: ValidateArgs) -> Result<()> {
    let budget = match Budget::from_clock(arguments.validate_deadline_unix_ms) {
        Ok(budget) => budget,
        Err(error) => return fail_early(&arguments, None, ValidatePhase::Deadline, error).await,
    };
    let inputs = budget
        .within(ValidatePhase::Inputs, async {
            Ok::<_, anyhow::Error>((
                read_json_lines::<SendManifestEntry>(&arguments.results.join(SEND_MANIFEST_FILE)).await?,
                read_json_lines::<ReceiptManifestEntry>(&arguments.results.join(RECEIPT_MANIFEST_FILE)).await?,
                read_json::<Vec<SchemaContract>>(&arguments.results.join(SCHEMA_CONTRACT_FILE)).await?,
                read_json::<RoutingEvidence>(&arguments.results.join(ROUTING_EVIDENCE_FILE)).await?,
                read_json::<Value>(&arguments.results.join(CAPABILITY_EVIDENCE_FILE)).await?,
            ))
        })
        .await;
    let (sends, receipts, contracts, routing, capabilities) = match inputs {
        Ok(inputs) => inputs,
        Err(error) => return fail_early(&arguments, Some(&budget), ValidatePhase::Inputs, error).await,
    };

    let accounting_timer = budget.timings.measure("phase/Accounting");
    let mut failures = Vec::new();
    let mut failed_phase = None;
    if budget.expired_at(Instant::now()) {
        failures.push("validate deadline elapsed before validation started".to_owned());
        failed_phase = Some(ValidatePhase::Deadline);
    }
    let kusto_settle = if arguments.collectors_drained {
        let drained = budget
            .within(ValidatePhase::DrainReceipts, async {
                let metadata = read_json(&arguments.results.join("run-metadata.json")).await?;
                let publication = read_json(&arguments.results.join("publication-drain.json")).await?;
                let mutators = read_json(&arguments.results.join("mutator-drain.json")).await?;
                check_drain_receipts(&metadata, &publication, &mutators)
            })
            .await;
        drained.err().map(|error| {
            failures.push(format!("drain receipts rejected: {error:#}"));
            failed_phase.get_or_insert(ValidatePhase::DrainReceipts);
            error.context("drain receipts rejected")
        })
    } else if arguments.kusto_endpoint.is_some() {
        let settled = budget
            .within(
                ValidatePhase::SettleDataflow,
                crate::kusto::settle_dataflow(
                    &arguments.dataflow_admin_endpoints,
                    budget.clamp(arguments.timeout.get()),
                    arguments.quiver,
                ),
            )
            .await
            .err();
        if settled.is_some() {
            failed_phase.get_or_insert(ValidatePhase::SettleDataflow);
        }
        settled
    } else {
        None
    };
    check(!sends.is_empty(), "send manifest is empty", &mut failures);
    check(
        contracts.len() == 13,
        "schema contract does not contain 13 tables",
        &mut failures,
    );
    let contract_map = contracts
        .iter()
        .map(|contract| (contract.table.clone(), contract))
        .collect::<BTreeMap<_, _>>();
    check(
        contract_map.len() == contracts.len(),
        "schema contract contains duplicate table identities",
        &mut failures,
    );

    let receipt_map = receipts
        .iter()
        .map(|receipt| (receipt.receipt_id, receipt))
        .collect::<BTreeMap<_, _>>();
    check(
        receipt_map.len() == receipts.len(),
        "receipt manifest contains duplicate receipt identifiers",
        &mut failures,
    );

    let mut attempts = BTreeMap::<(String, String), Vec<&SendManifestEntry>>::new();
    let mut paired = BTreeMap::<String, BTreeMap<String, &SendManifestEntry>>::new();
    for send in &sends {
        check(
            contract_map.contains_key(&send.table),
            &format!("send manifest contains unknown table {}", send.table),
            &mut failures,
        );
        if send.outcome == AckOutcome::DurableReceipt {
            check(
                send.pool == "direct" && send.receipt_id.is_some(),
                "durable acknowledgement is not a direct receipt",
                &mut failures,
            );
        }
        if send.outcome == AckOutcome::BestEffortAdmission {
            check(
                send.pool == "dataflow" && send.receipt_id.is_none(),
                "best-effort acknowledgement is not honest dataflow admission",
                &mut failures,
            );
            check(
                !arguments.quiver,
                "dataflow acknowledged without durable admission while Quiver was enabled",
                &mut failures,
            );
        }
        if send.outcome == AckOutcome::DurableAdmission {
            check(
                send.pool == "dataflow" && send.receipt_id.is_none() && arguments.quiver,
                "durable admission is not an honest Quiver-backed dataflow acknowledgement",
                &mut failures,
            );
        }
        attempts
            .entry((send.pool.clone(), send.logical_batch_id.clone()))
            .or_default()
            .push(send);
        if send.scenario == crate::cli::Scenario::Paired && send.outcome != AckOutcome::Error {
            paired
                .entry(send.logical_batch_id.clone())
                .or_default()
                .entry(send.pool.clone())
                .or_insert(send);
        }
    }

    for (logical_id, pools) in &paired {
        let direct = pools.get("direct");
        let dataflow = pools.get("dataflow");
        check(
            direct.is_some() && dataflow.is_some(),
            &format!("paired logical batch {logical_id} did not succeed in both pools"),
            &mut failures,
        );
        if let (Some(direct), Some(dataflow)) = (direct, dataflow) {
            check(
                direct.table == dataflow.table
                    && direct.rows == dataflow.rows
                    && direct.schema_fingerprint == dataflow.schema_fingerprint
                    && direct.payload_sha256 == dataflow.payload_sha256
                    && direct.row_sha256 == dataflow.row_sha256,
                &format!("paired logical batch {logical_id} changed between pools"),
                &mut failures,
            );
        }
    }

    let mut admitted_rows = BTreeMap::<String, BTreeMap<String, u64>>::new();
    let mut required_rows = BTreeMap::<String, BTreeMap<String, u64>>::new();
    let mut expected_tables = BTreeMap::<String, BTreeSet<String>>::new();
    for ((pool, logical_id), values) in &attempts {
        let successes = values
            .iter()
            .copied()
            .filter(|entry| entry.outcome != AckOutcome::Error)
            .collect::<Vec<_>>();
        check(
            !successes.is_empty(),
            &format!("{pool} logical batch {logical_id} exhausted every attempt"),
            &mut failures,
        );
        if successes.is_empty() {
            continue;
        }
        let first = successes[0];
        check(
            successes.iter().all(|entry| {
                entry.table == first.table
                    && entry.rows == first.rows
                    && entry.payload_sha256 == first.payload_sha256
                    && entry.schema_fingerprint == first.schema_fingerprint
            }),
            &format!("{pool} logical batch {logical_id} changed across retries"),
            &mut failures,
        );
        let delivered = if pool == "direct" {
            let mut published = false;
            for success in successes {
                let Some(receipt_id) = success.receipt_id else {
                    continue;
                };
                let terminal = receipt_map.get(&receipt_id);
                check(
                    terminal.is_some(),
                    &format!("direct receipt {receipt_id} lacks a terminal poll result"),
                    &mut failures,
                );
                if let Some(terminal) = terminal {
                    check(
                        matches!(terminal.state.as_str(), "published" | "failed" | "publication_unknown"),
                        &format!("direct receipt {receipt_id} is not terminal"),
                        &mut failures,
                    );
                    published |= terminal.state == "published" && terminal.purge_allowed;
                }
            }
            check(
                published,
                &format!("direct logical batch {logical_id} has no published copy"),
                &mut failures,
            );
            published
        } else {
            true
        };
        if delivered {
            let rows = u64::try_from(first.rows).unwrap_or(u64::MAX);
            *admitted_rows
                .entry(pool.clone())
                .or_default()
                .entry(first.table.clone())
                .or_default() += rows;
            let required = required_rows
                .entry(pool.clone())
                .or_default()
                .entry(first.table.clone())
                .or_insert(0);
            // Direct receipts and Quiver durable admission promise every acknowledged row;
            // best-effort dataflow admission only promises that the table was published.
            if pool == "direct" || arguments.quiver {
                *required += rows;
            } else if *required == 0 || rows < *required {
                *required = rows;
            }
            expected_tables
                .entry(pool.clone())
                .or_default()
                .insert(first.table.clone());
        }
    }

    let all_tables = contract_map.keys().cloned().collect::<BTreeSet<_>>();
    for (pool, tables) in &expected_tables {
        check(
            tables == &all_tables,
            &format!("{pool} traffic did not cover every registered table"),
            &mut failures,
        );
    }

    accounting_timer.finish(true);
    let mut snapshots = BTreeMap::new();
    let snapshot_for = |pool: &'static str, root: &Path, phase| {
        let expected = required_rows.get(pool);
        let required = expected_tables.contains_key(pool);
        let timings = budget.timings.clone();
        let budget = &budget;
        let root = root.to_owned();
        let timeout = arguments.timeout.get();
        async move {
            if !required {
                return (pool, phase, None);
            }
            let result = budget
                .within(phase, async {
                    let expected = expected.with_context(|| format!("{pool} row expectations missing"))?;
                    wait_for_snapshot(&root, expected, budget.clamp(timeout), budget.deadline, timings).await
                })
                .await;
            (pool, phase, Some(result))
        }
    };
    let (direct_snapshot, dataflow_snapshot) = tokio::join!(
        snapshot_for("direct", &arguments.direct_root, ValidatePhase::SnapshotDirect),
        snapshot_for("dataflow", &arguments.dataflow_root, ValidatePhase::SnapshotDataflow),
    );
    for (pool, phase, result) in [direct_snapshot, dataflow_snapshot] {
        match result {
            Some(Ok(snapshot)) => {
                snapshots.insert(pool, snapshot);
            }
            Some(Err(error)) => {
                failures.push(format!("{pool} Delta snapshot did not become visible: {error:#}"));
                failed_phase.get_or_insert(phase);
            }
            None => {}
        }
    }
    let checks_timer = budget.timings.measure("phase/LocalChecks");
    let direct = snapshots.remove("direct");
    let dataflow = snapshots.remove("dataflow");
    if let Some(snapshot) = &direct {
        validate_snapshot(
            "direct",
            snapshot,
            &contract_map,
            required_rows.get("direct"),
            &mut failures,
        );
    }

    async fn wait_for_snapshot(
        root: &Path,
        expected: &BTreeMap<String, u64>,
        timeout: Duration,
        ceiling: Option<Instant>,
        timings: delta_inspect::scheduler::OracleTimings,
    ) -> Result<SnapshotSummary> {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = if root.to_str().is_some_and(|root| root.starts_with("abfss://")) {
                delta_inspect::summarize_cloud_timed(
                    root.to_str().context("non-UTF8 cloud root")?,
                    &expected.keys().cloned().collect::<Vec<_>>(),
                    ceiling,
                    timings.clone(),
                )
                .await
            } else {
                delta_inspect::summarize_timed(root, ceiling, timings.clone()).await
            };
            let error = match snapshot {
                Ok(snapshot)
                    if expected.iter().all(|(table, rows)| {
                        snapshot.tables.get(table).is_some_and(|summary| summary.rows >= *rows)
                    }) =>
                {
                    return Ok(snapshot);
                }
                Ok(snapshot) => anyhow::anyhow!(
                    "snapshot has {} tables and {} files but expected rows are not visible",
                    snapshot.tables.len(),
                    snapshot.files
                ),
                Err(error) => error,
            };
            if Instant::now() >= deadline {
                return Err(error);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    if let Some(snapshot) = &dataflow {
        validate_snapshot(
            "dataflow",
            snapshot,
            &contract_map,
            required_rows.get("dataflow"),
            &mut failures,
        );
    }
    if let (Some(direct), Some(dataflow)) = (&direct, &dataflow) {
        for table in &all_tables {
            if let (Some(direct), Some(dataflow)) = (direct.tables.get(table), dataflow.tables.get(table)) {
                check(
                    direct.schema == dataflow.schema,
                    &format!("{table} Delta schemas differ between pools"),
                    &mut failures,
                );
                check(
                    direct.payload_row_hashes.keys().eq(dataflow.payload_row_hashes.keys()),
                    &format!("{table} deterministic row values differ between pools"),
                    &mut failures,
                );
            }
        }
    }

    validate_routing(
        &routing,
        &all_tables,
        &expected_tables,
        arguments.require_all_collectors,
        &mut failures,
    );
    validate_capabilities(&capabilities, arguments.quiver, &mut failures);

    checks_timer.finish(true);
    let mut diagnostics = Vec::new();
    let kusto = if let Some(endpoint) = &arguments.kusto_endpoint {
        let bound = crate::kusto::KustoBound {
            timings: budget.timings.clone(),
            ceiling: budget.deadline,
            ..crate::kusto::KustoBound::default()
        };
        let validation = if let Some(error) = kusto_settle {
            Err(error)
        } else if let (Some(direct), Some(dataflow)) = (&direct, &dataflow) {
            budget
                .within(ValidatePhase::Kusto, async {
                    tokio::time::timeout(
                        arguments.timeout.get(),
                        crate::kusto::validate(
                            endpoint,
                            &arguments.results,
                            &arguments.direct_root,
                            &arguments.dataflow_root,
                            direct,
                            dataflow,
                            &bound,
                        ),
                    )
                    .await
                    .context("Kusto validation deadline elapsed")
                    .and_then(|report| report)
                })
                .await
        } else {
            Err(anyhow::anyhow!(
                "Kusto validation requires both independent Delta snapshots"
            ))
        };
        match validation {
            Ok(mut report) => {
                report["timings"] = serde_json::json!(budget.timings.evidence());
                crate::kusto::write_validation(&arguments.results, &report)?;
                Some(report)
            }
            Err(error) => {
                failures.push(format!("Kusto validation failed: {error:#}"));
                failed_phase.get_or_insert(ValidatePhase::Kusto);
                record_kusto_failure(&arguments.results, &error, &bound.into_progress(), &mut diagnostics).await
            }
        }
    } else {
        None
    };
    let report = ValidationReport {
        timings: budget.timings.evidence(),
        oracle_workers: delta_inspect::scheduler::oracle_scheduler().workers(),
        passed: failures.is_empty(),
        failures,
        failed_phase,
        deadline: budget.evidence(),
        diagnostics,
        admitted_rows,
        required_rows,
        direct,
        dataflow,
        routing,
        kusto,
    };
    let mut bytes = serde_json::to_vec_pretty(&report)?;
    bytes.push(b'\n');
    write_report(&arguments.results, &bytes).await?;
    if !report.passed {
        bail!(
            "loadtest validation failed with {} issue(s); see {}",
            report.failures.len(),
            arguments.results.join(VALIDATION_FILE).display()
        );
    }
    println!(
        "PASS: 13 schemas, complete rows, paired parity, receipts, routing, capabilities, and {} Delta files",
        report.direct.as_ref().map_or(0, |snapshot| snapshot.files)
            + report.dataflow.as_ref().map_or(0, |snapshot| snapshot.files)
    );
    Ok(())
}

fn validate_snapshot(
    pool: &str,
    snapshot: &SnapshotSummary,
    contracts: &BTreeMap<String, &SchemaContract>,
    expected_rows: Option<&BTreeMap<String, u64>>,
    failures: &mut Vec<String>,
) {
    let Some(expected_rows) = expected_rows else {
        check(false, &format!("{pool} expected-row accounting is missing"), failures);
        return;
    };
    for (table, rows) in expected_rows {
        let actual = snapshot.tables.get(table);
        check(
            actual.is_some(),
            &format!("{pool} Delta snapshot is missing {table}"),
            failures,
        );
        let Some(actual) = actual else {
            continue;
        };
        check(
            actual.rows >= *rows,
            &format!(
                "{pool} {table} has {} rows but at least {rows} logical rows were required",
                actual.rows
            ),
            failures,
        );
        validate_table_storage(pool, table, actual, failures);
        if let Some(contract) = contracts.get(table) {
            validate_delta_schema(pool, table, actual, contract, failures);
        }
    }
    for table in snapshot.tables.keys() {
        check(
            contracts.contains_key(table),
            &format!("{pool} Delta root contains cross-table output {table}"),
            failures,
        );
    }
}

fn validate_table_storage(pool: &str, table: &str, summary: &TableSummary, failures: &mut Vec<String>) {
    check(
        summary.version >= 1,
        &format!("{pool} {table} has no data Delta version"),
        failures,
    );
    check(
        summary.active_files > 0
            && summary.bytes > 0
            && summary.minimum_file_bytes > 0
            && summary.maximum_file_bytes <= 1_073_741_824,
        &format!("{pool} {table} has invalid active file-size evidence"),
        failures,
    );
    check(
        summary.files.iter().map(|file| file.rows).sum::<u64>() == summary.rows,
        &format!("{pool} {table} file row counts do not match snapshot rows"),
        failures,
    );
}

fn validate_delta_schema(
    pool: &str,
    table: &str,
    summary: &TableSummary,
    contract: &SchemaContract,
    failures: &mut Vec<String>,
) {
    let mut fields = summary
        .schema
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    fields.retain(|field| {
        !summary
            .partition_columns
            .iter()
            .any(|column| field["name"].as_str() == Some(column))
    });
    check(
        summary
            .partition_columns
            .iter()
            .all(|column| column == "__arrowcol_ingestion"),
        &format!("{pool} {table} has an unexpected generated partition column"),
        failures,
    );
    let mut expected = Vec::<(String, String, bool)>::new();
    for field in &contract.fields {
        let delta_type = match field.arrow_type.as_str() {
            "Boolean" => "boolean",
            "UInt8" | "Int16" => "short",
            "Int32" => "integer",
            "Int64" => "long",
            "Float64" => "double",
            "Utf8" => "string",
            value if value.starts_with("Timestamp(") => "timestamp",
            other => {
                check(
                    false,
                    &format!("unsupported expected Arrow type {other} for {table}"),
                    failures,
                );
                continue;
            }
        };
        expected.push((field.name.clone(), delta_type.to_owned(), field.nullable));
        if delta_type == "timestamp" {
            expected.push((format!("{}__unix_nanos", field.name), "long".to_owned(), field.nullable));
        }
    }
    check(
        fields.len() == expected.len(),
        &format!(
            "{pool} {table} Delta schema has {} fields; expected {}",
            fields.len(),
            expected.len()
        ),
        failures,
    );
    for (index, (name, data_type, nullable)) in expected.iter().enumerate() {
        let Some(field) = fields.get(index) else {
            break;
        };
        check(
            field.get("name").and_then(Value::as_str) == Some(name.as_str())
                && field.get("type").and_then(Value::as_str) == Some(data_type.as_str())
                && field.get("nullable").and_then(Value::as_bool) == Some(*nullable),
            &format!("{pool} {table} Delta schema mismatch at field {index} ({name})"),
            failures,
        );
    }
}

fn validate_routing(
    routing: &RoutingEvidence,
    all_tables: &BTreeSet<String>,
    expected_pools: &BTreeMap<String, BTreeSet<String>>,
    require_all_collectors: bool,
    failures: &mut Vec<String>,
) {
    let collectors = routing
        .collectors
        .iter()
        .map(|collector| (collector.instance_id.as_str(), collector))
        .collect::<BTreeMap<_, _>>();
    for pool in expected_pools.keys() {
        // Terminated registrations (for example a predecessor killed by the crash demo) remain
        // in the routing tables; only live collectors must own and serve tables.
        let pool_collectors = routing
            .collectors
            .iter()
            .filter(|collector| &collector.implementation_pool == pool && !collector.terminated)
            .collect::<Vec<_>>();
        check(
            !pool_collectors.is_empty(),
            &format!("{pool} did not register a live collector"),
            failures,
        );
        let leases = routing
            .leases
            .iter()
            .filter(|lease| &lease.implementation_pool == pool && all_tables.contains(&lease.table))
            .collect::<Vec<_>>();
        check(
            leases.len() == all_tables.len(),
            &format!("{pool} does not have one routing lease for every table"),
            failures,
        );
        let mut leased_tables = BTreeSet::new();
        let mut holders = BTreeSet::new();
        for lease in leases {
            check(
                leased_tables.insert(lease.table.clone()),
                &format!("{pool} has duplicate routing affinity for {}", lease.table),
                failures,
            );
            let collector = collectors.get(lease.holder_instance_id.as_str());
            check(
                collector.is_some_and(|collector| collector.implementation_pool == *pool),
                &format!("{pool} {} is routed to a collector in the wrong pool", lease.table),
                failures,
            );
            check(
                collector.is_none_or(|collector| !collector.terminated),
                &format!("{pool} {} is still routed to a terminated collector", lease.table),
                failures,
            );
            holders.insert(lease.holder_instance_id.clone());
        }
        if require_all_collectors {
            check(
                holders.len() == pool_collectors.len(),
                &format!("{pool} table distribution did not use every collector"),
                failures,
            );
        }
        for holder in holders {
            if let Some(collector) = collectors.get(holder.as_str()) {
                check(
                    collector.received_batches.unwrap_or(0) > 0,
                    &format!("{pool} lease holder {} received no batches", collector.instance_key),
                    failures,
                );
            }
        }
    }
}

fn validate_capabilities(capabilities: &Value, quiver: bool, failures: &mut Vec<String>) {
    let direct = capabilities
        .get("direct")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let dataflow = capabilities
        .get("dataflow")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    check(!direct.is_empty(), "direct capability evidence is missing", failures);
    check(
        !dataflow.is_empty(),
        "dataflow capability evidence is missing",
        failures,
    );
    for entry in direct {
        let response = &entry["response"];
        let values = response["capabilities"].as_array().cloned().unwrap_or_default();
        check(
            response["implementation_pool"] == "direct" && values.iter().any(|value| value == "receipt_status"),
            "direct collector did not advertise durable receipt status",
            failures,
        );
        check(
            values.iter().any(|value| value == "durable_payload_storage") == quiver,
            if quiver {
                "direct collector did not advertise native Quiver payload storage"
            } else {
                "direct collector advertised native Quiver payload storage without --quiver"
            },
            failures,
        );
    }
    for entry in dataflow {
        let response = &entry["response"];
        let values = response["capabilities"].as_array().cloned().unwrap_or_default();
        let honest_gap = !values.iter().any(|value| value == "receipt_status");
        if quiver {
            check(
                response["delivery_guarantee"] == "durable_admission"
                    && response["completion_mode"] == "durable_admission"
                    && response["at_least_once"] == true
                    && response["native_quiver"]["enabled"] == true
                    && honest_gap,
                "dataflow collector did not report native Quiver durable admission",
                failures,
            );
        } else {
            check(
                response["delivery_guarantee"] == "best_effort"
                    && response["completion_mode"] == "best_effort_admission"
                    && honest_gap,
                "dataflow collector capability gap was not reported honestly",
                failures,
            );
        }
    }
}

fn check(condition: bool, message: &str, failures: &mut Vec<String>) {
    if !condition {
        failures.push(message.to_owned());
    }
}

async fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = fs::read(path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

async fn read_json_lines<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    let text = fs::read_to_string(path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
        .map(|(index, line)| {
            serde_json::from_str(line).with_context(|| format!("parse {} line {}", path.display(), index + 1))
        })
        .collect()
}

/// Replaces the validation report atomically, so a killed command never leaves a partial file.
async fn write_report(results: &Path, bytes: &[u8]) -> Result<()> {
    let output = results.join(VALIDATION_FILE);
    let temporary = output.with_extension("json.next");
    fs::write(&temporary, bytes).await?;
    fs::rename(&temporary, &output).await?;
    Ok(())
}

/// Records a failure that happened before the per-pool checks could run, with the same report keys
/// as a full report, then fails the command.
async fn fail_early(
    arguments: &ValidateArgs,
    budget: Option<&Budget>,
    phase: ValidatePhase,
    error: anyhow::Error,
) -> Result<()> {
    let mut diagnostics = Vec::new();
    let mut progress = crate::kusto::KustoProgress::default();
    progress.timings = budget.map(|budget| budget.timings.evidence()).unwrap_or_default();
    let kusto = if arguments.kusto_endpoint.is_some() {
        record_kusto_failure(&arguments.results, &error, &progress, &mut diagnostics).await
    } else {
        None
    };
    let mut report = serde_json::json!({
        "passed": false,
        "failures": [format!("validation stopped in phase {phase:?}: {error:#}")],
        "failed_phase": phase,
        "deadline": budget.and_then(Budget::evidence),
        "timings": budget.map(|budget| budget.timings.evidence()),
        "admitted_rows": {}, "required_rows": {},
        "direct": null, "dataflow": null, "routing": null,
        "kusto": kusto,
    });
    if !diagnostics.is_empty() {
        report["diagnostics"] = serde_json::json!(diagnostics);
    }
    let mut bytes = serde_json::to_vec_pretty(&report)?;
    bytes.push(b'\n');
    write_report(&arguments.results, &bytes).await?;
    Err(error.context(format!(
        "loadtest validation failed in phase {phase:?}; see {}",
        arguments.results.join(VALIDATION_FILE).display()
    )))
}

/// Writes the failed Kusto report first and then the existing degraded status. Either secondary
/// write failing (including a missing run identity or a corrupt status) becomes an explicit
/// diagnostic instead of preventing the primary report; no run identity or status is invented.
async fn record_kusto_failure(
    results: &Path,
    error: &anyhow::Error,
    progress: &crate::kusto::KustoProgress,
    diagnostics: &mut Vec<String>,
) -> Option<Value> {
    let report = match crate::kusto::validation_failure(results, error, progress) {
        Ok(report) => Some(report),
        Err(write) => {
            diagnostics.push(format!("failed Kusto report not written: {write:#}"));
            None
        }
    };
    let status = async {
        let metadata: Value = read_json(&results.join(crate::manifest::RUN_METADATA_FILE)).await?;
        let run_id = metadata["run_id"].as_str().context("run identity missing")?;
        crate::kusto::degraded(results, run_id, error)
    }
    .await;
    if let Err(status) = status {
        diagnostics.push(format!("degraded Kusto status not written: {status:#}"));
    }
    report
}

#[derive(Deserialize, Serialize)]
struct RoutingEvidence {
    loaded_at_unix_ms: u64,
    collectors: Vec<RoutingCollectorEvidence>,
    leases: Vec<RoutingLeaseEvidence>,
}

#[derive(Deserialize, Serialize)]
struct RoutingCollectorEvidence {
    instance_id: String,
    instance_key: String,
    implementation_pool: String,
    endpoint: String,
    state: String,
    #[serde(default)]
    terminated: bool,
    received_rows: Option<u64>,
    received_batches: Option<u64>,
}

#[derive(Deserialize, Serialize)]
struct RoutingLeaseEvidence {
    implementation_pool: String,
    table: String,
    holder_instance_id: String,
    fencing_epoch: u64,
    lease_version: u64,
    state: String,
}

#[derive(Serialize)]
struct ValidationReport {
    timings: BTreeMap<String, delta_inspect::scheduler::OracleTiming>,
    oracle_workers: usize,
    passed: bool,
    failures: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failed_phase: Option<ValidatePhase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deadline: Option<Value>,
    /// Secondary evidence that could not be written; never hides the primary failure.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    diagnostics: Vec<String>,
    admitted_rows: BTreeMap<String, BTreeMap<String, u64>>,
    required_rows: BTreeMap<String, BTreeMap<String, u64>>,
    direct: Option<SnapshotSummary>,
    dataflow: Option<SnapshotSummary>,
    routing: RoutingEvidence,
    kusto: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW_UNIX: Duration = Duration::from_millis(1_800_000_000_000);

    #[test]
    fn budget_is_derived_once_and_bounds_every_sequential_phase() -> Result<()> {
        let now = Instant::now();
        let unbounded = Budget::new(None, NOW_UNIX, now)?;
        assert!(unbounded.deadline.is_none() && unbounded.evidence().is_none());
        assert!(!unbounded.expired_at(now + Duration::from_secs(3_600)));
        assert_eq!(
            unbounded.clamp_at(Duration::from_secs(480), now),
            Duration::from_secs(480)
        );

        let budget = Budget::new(Some(1_800_000_570_000), NOW_UNIX, now)?;
        assert_eq!(budget.deadline, Some(now + Duration::from_secs(570)));
        // Three sequential phases that each own the 8m drain timeout share the one 570s bound.
        let mut elapsed = Duration::ZERO;
        let mut granted = Vec::new();
        for _ in 0..3 {
            let wait = budget.clamp_at(Duration::from_secs(480), now + elapsed);
            granted.push(wait);
            elapsed += wait;
        }
        assert_eq!(
            granted,
            [Duration::from_secs(480), Duration::from_secs(90), Duration::ZERO]
        );
        assert_eq!(elapsed, Duration::from_secs(570));
        assert!(budget.expired_at(now + elapsed));
        assert!(!budget.expired_at(now + Duration::from_millis(569_999)));

        for past in [1, 1_799_999_999_999, 1_800_000_000_000] {
            let expired = Budget::new(Some(past), NOW_UNIX, now)?;
            assert!(expired.expired_at(now), "{past} is not in the future");
            assert_eq!(expired.clamp_at(Duration::from_secs(480), now), Duration::ZERO);
        }
        assert!(Budget::new(Some(1_800_000_000_000 + 86_400_000), NOW_UNIX, now).is_ok());
        for too_far in [1_800_000_000_000 + 86_400_001, u64::MAX] {
            assert!(
                Budget::new(Some(too_far), NOW_UNIX, now).is_err(),
                "{too_far} must be rejected"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn phases_fail_at_the_deadline_and_late_success_is_rejected() -> Result<()> {
        let started = Instant::now();
        let expired = Budget::new(Some(1), NOW_UNIX, started)?;
        let polled = std::sync::atomic::AtomicBool::new(false);
        let result = expired
            .within(ValidatePhase::SnapshotDirect, async {
                polled.store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            })
            .await;
        assert!(result.is_err(), "an elapsed deadline cannot start a phase");
        assert!(
            !polled.load(std::sync::atomic::Ordering::Relaxed),
            "the phase is never polled"
        );

        let budget = Budget {
            timings: delta_inspect::scheduler::OracleTimings::default(),
            deadline: Some(Instant::now() + Duration::from_millis(200)),
            deadline_unix_ms: Some(1),
        };
        let started = Instant::now();
        let late = budget
            .within(ValidatePhase::Kusto, async {
                tokio::time::sleep(Duration::from_millis(1_500)).await;
                Ok("converged")
            })
            .await;
        let error = late.err().context("a success after the deadline must be rejected")?;
        assert!(format!("{error:#}").contains("Kusto"), "{error:#}");
        assert!(
            started.elapsed() < Duration::from_millis(1_000),
            "the in-flight phase is dropped"
        );

        // A synchronous stretch cannot be preempted; its late success is still rejected.
        let budget = Budget {
            timings: delta_inspect::scheduler::OracleTimings::default(),
            deadline: Some(Instant::now() + Duration::from_millis(100)),
            deadline_unix_ms: Some(1),
        };
        let blocking = budget
            .within(ValidatePhase::SnapshotDataflow, async {
                std::thread::sleep(Duration::from_millis(300));
                Ok("snapshot")
            })
            .await;
        let error = blocking.err().context("a blocking phase that overran must fail")?;
        assert!(format!("{error:#}").contains("completed only after"), "{error:#}");

        let unbounded = Budget::new(None, NOW_UNIX, Instant::now())?;
        let value = unbounded
            .within(ValidatePhase::Kusto, async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(7)
            })
            .await?;
        assert_eq!(value, 7, "without a deadline phases run unchanged");
        Ok(())
    }

    struct Results(std::path::PathBuf);

    impl Drop for Results {
        fn drop(&mut self) {
            if self.0.exists()
                && let Err(error) = std::fs::remove_dir_all(&self.0)
            {
                panic!("remove test directory: {error}");
            }
        }
    }

    fn validate_arguments(results: &Path, deadline: u64, extra: &[&str]) -> Result<ValidateArgs> {
        use clap::Parser;
        let deadline = deadline.to_string();
        let cli = crate::cli::Cli::try_parse_from(
            [
                "arrowcol-loadgen",
                "validate",
                "--results",
                results.to_str().context("results path")?,
                "--validate-deadline-unix-ms",
                &deadline,
            ]
            .iter()
            .copied()
            .chain(extra.iter().copied()),
        )?;
        match cli.command {
            crate::cli::Command::Validate(arguments) => Ok(arguments),
            _ => bail!("expected validate command"),
        }
    }

    fn results_directory(test: &str) -> Result<Results> {
        let results = Results(std::path::PathBuf::from(format!(
            "target/loadgen-validation-tests/{}-{test}",
            std::process::id()
        )));
        if results.0.exists() {
            std::fs::remove_dir_all(&results.0)?;
        }
        std::fs::create_dir_all(&results.0)?;
        Ok(results)
    }

    fn unix_now_ms() -> Result<u64> {
        Ok(u64::try_from(
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        )?)
    }

    #[tokio::test]
    async fn early_failures_write_a_failed_report_before_exiting() -> Result<()> {
        let results = results_directory("early")?;
        let now = unix_now_ms()?;
        for (deadline, phase) in [
            (now + 2 * 86_400_000, "deadline"),
            (now + 570_000, "inputs"),
            (1, "inputs"),
        ] {
            let started = Instant::now();
            let error = validate(validate_arguments(&results.0, deadline, &[])?)
                .await
                .err()
                .context("an early failure must fail the command")?;
            assert!(started.elapsed() < Duration::from_secs(5), "no wait after the failure");
            let report: Value = read_json(&results.0.join(VALIDATION_FILE)).await?;
            assert_eq!(report["passed"], false, "{error:#}");
            assert_eq!(report["failed_phase"], phase);
            assert_eq!(report["failures"].as_array().map(Vec::len), Some(1));
            assert!(
                !results.0.join("validation.json.next").exists(),
                "the report is replaced atomically"
            );
            if phase == "deadline" {
                assert!(
                    report["deadline"].is_null(),
                    "a rejected bound is not reported as a budget"
                );
            } else {
                assert_eq!(report["deadline"]["deadline_unix_ms"], deadline);
            }
        }
        Ok(())
    }

    const UNREACHABLE_KUSTO: [&str; 2] = ["--kusto-endpoint", "http://127.0.0.1:1"];

    /// Missing run identity or a corrupt degraded status must not cost the primary failed reports.
    async fn assert_kusto_diagnostics(results: &Path, phase: &str, corrupt_status: Option<&str>) -> Result<()> {
        let report: Value = read_json(&results.join(VALIDATION_FILE)).await?;
        assert_eq!(report["passed"], false);
        assert_eq!(report["failed_phase"], phase);
        assert_eq!(report["kusto"]["passed"], false, "{report}");
        let kusto: Value = read_json(&results.join("kusto-validation.json")).await?;
        assert_eq!(kusto["passed"], false);
        assert!(kusto["error"].as_str().is_some_and(|error| !error.is_empty()));
        let diagnostics = report["diagnostics"].as_array().context("diagnostics missing")?;
        assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
        assert!(
            diagnostics[0]
                .as_str()
                .is_some_and(|diagnostic| diagnostic.starts_with("degraded Kusto status not written")),
            "{diagnostics:?}"
        );
        let status = results.join("kusto-status.json");
        match corrupt_status {
            None => assert!(!status.exists(), "no status is invented without a run identity"),
            Some(corrupt) => assert_eq!(
                std::fs::read_to_string(&status)?,
                corrupt,
                "corrupt status left as found"
            ),
        }
        Ok(())
    }

    #[tokio::test]
    async fn early_kusto_failure_reports_survive_missing_metadata_and_corrupt_status() -> Result<()> {
        let deadline = unix_now_ms()? + 570_000;
        let results = results_directory("early-kusto-metadata")?;
        let error = validate(validate_arguments(&results.0, deadline, &UNREACHABLE_KUSTO)?).await;
        assert!(error.is_err());
        assert_kusto_diagnostics(&results.0, "inputs", None).await?;

        let results = results_directory("early-kusto-status")?;
        std::fs::write(results.0.join("run-metadata.json"), r#"{"run_id":"current"}"#)?;
        std::fs::write(results.0.join("kusto-status.json"), "{corrupt")?;
        let error = validate(validate_arguments(&results.0, deadline, &UNREACHABLE_KUSTO)?).await;
        assert!(error.is_err());
        assert_kusto_diagnostics(&results.0, "inputs", Some("{corrupt")).await
    }

    #[tokio::test]
    async fn late_kusto_failure_reports_survive_missing_metadata_and_corrupt_status() -> Result<()> {
        let deadline = unix_now_ms()? + 570_000;
        let arguments = [UNREACHABLE_KUSTO[0], UNREACHABLE_KUSTO[1], "--collectors-drained"];
        for (test, corrupt_status) in [("late-kusto-metadata", None), ("late-kusto-status", Some("{corrupt"))] {
            let results = results_directory(test)?;
            for (file, contents) in [
                (SEND_MANIFEST_FILE, ""),
                (RECEIPT_MANIFEST_FILE, ""),
                (SCHEMA_CONTRACT_FILE, "[]"),
                (
                    ROUTING_EVIDENCE_FILE,
                    r#"{"loaded_at_unix_ms":0,"collectors":[],"leases":[]}"#,
                ),
                (CAPABILITY_EVIDENCE_FILE, "{}"),
            ] {
                std::fs::write(results.0.join(file), contents)?;
            }
            if let Some(corrupt) = corrupt_status {
                std::fs::write(results.0.join("run-metadata.json"), r#"{"run_id":"current"}"#)?;
                std::fs::write(results.0.join("kusto-status.json"), corrupt)?;
            }
            let error = validate(validate_arguments(&results.0, deadline, &arguments)?).await;
            assert!(error.is_err());
            assert_kusto_diagnostics(&results.0, "drain-receipts", corrupt_status).await?;
        }
        Ok(())
    }

    #[test]
    fn stopped_validation_rejects_missing_stale_and_partial_drain_receipts() -> Result<()> {
        let metadata = serde_json::json!({"run_id": "current"});
        let publication = serde_json::json!({"run_id": "current", "dataflow_settled": true});
        let mutators = serde_json::json!({"run_id": "current", "clean": true, "services": ["direct-1", "dataflow-1"]});
        check_drain_receipts(&metadata, &publication, &mutators)?;
        for invalid in [
            Value::Null,
            serde_json::json!({"run_id": "old", "clean": true, "services": ["direct-1", "dataflow-1"]}),
            serde_json::json!({"run_id": "current", "clean": false, "services": ["direct-1", "dataflow-1"]}),
            serde_json::json!({"run_id": "current", "clean": true, "services": ["direct-1"]}),
        ] {
            assert!(check_drain_receipts(&metadata, &publication, &invalid).is_err());
        }
        assert!(check_drain_receipts(&metadata, &Value::Null, &mutators).is_err());
        assert!(
            check_drain_receipts(
                &metadata,
                &serde_json::json!({"run_id": "old", "dataflow_settled": true}),
                &mutators
            )
            .is_err()
        );
        Ok(())
    }

    fn collector(instance_id: &str, terminated: bool) -> RoutingCollectorEvidence {
        RoutingCollectorEvidence {
            instance_id: instance_id.to_owned(),
            instance_key: "loadtest-direct-1".to_owned(),
            implementation_pool: "direct".to_owned(),
            endpoint: "10.89.0.21:50051".to_owned(),
            state: "ready".to_owned(),
            terminated,
            received_rows: Some(8),
            received_batches: Some(1),
        }
    }

    fn crash_succession(holder: &str) -> (RoutingEvidence, BTreeSet<String>, BTreeMap<String, BTreeSet<String>>) {
        let tables = BTreeSet::from(["telemetry.prod.logs".to_owned(), "telemetry.prod.traces".to_owned()]);
        let evidence = RoutingEvidence {
            loaded_at_unix_ms: 1,
            collectors: vec![collector("predecessor", true), collector("successor", false)],
            leases: tables
                .iter()
                .map(|table| RoutingLeaseEvidence {
                    implementation_pool: "direct".to_owned(),
                    table: table.clone(),
                    holder_instance_id: holder.to_owned(),
                    fencing_epoch: 2,
                    lease_version: 2,
                    state: "active".to_owned(),
                })
                .collect(),
        };
        let pools = BTreeMap::from([("direct".to_owned(), tables.clone())]);
        (evidence, tables, pools)
    }

    #[test]
    fn crashed_predecessor_does_not_count_as_an_idle_collector() {
        let (evidence, tables, pools) = crash_succession("successor");
        let mut failures = Vec::new();
        validate_routing(&evidence, &tables, &pools, true, &mut failures);
        assert!(failures.is_empty(), "{failures:?}");
    }

    #[test]
    fn tables_must_not_stay_routed_to_a_terminated_collector() {
        let (evidence, tables, pools) = crash_succession("predecessor");
        let mut failures = Vec::new();
        validate_routing(&evidence, &tables, &pools, true, &mut failures);
        assert!(
            failures
                .iter()
                .any(|failure| failure.contains("still routed to a terminated collector")),
            "{failures:?}"
        );
    }

    fn ordinary_capabilities() -> Value {
        serde_json::json!({
            "direct": [{
                "response": {
                    "implementation_pool": "direct",
                    "capabilities": ["receipt_status"]
                }
            }],
            "dataflow": [{
                "response": {
                    "delivery_guarantee": "best_effort",
                    "completion_mode": "best_effort_admission",
                    "capabilities": []
                }
            }]
        })
    }

    #[test]
    fn ordinary_collector_delivery_capabilities_are_valid() {
        let mut failures = Vec::new();
        validate_capabilities(&ordinary_capabilities(), false, &mut failures);
        assert!(failures.is_empty(), "{failures:?}");
    }

    #[test]
    fn capability_evidence_is_required_for_both_pools() {
        let mut failures = Vec::new();
        validate_capabilities(&serde_json::json!({}), false, &mut failures);
        assert_eq!(
            failures,
            [
                "direct capability evidence is missing",
                "dataflow capability evidence is missing",
            ]
        );
    }

    #[test]
    fn capability_validation_preserves_delivery_contracts() {
        let mut capabilities = ordinary_capabilities();
        capabilities["direct"][0]["response"]["capabilities"] = serde_json::json!([]);
        capabilities["dataflow"][0]["response"]["capabilities"] = serde_json::json!(["receipt_status"]);
        let mut failures = Vec::new();
        validate_capabilities(&capabilities, false, &mut failures);
        assert_eq!(
            failures,
            [
                "direct collector did not advertise durable receipt status",
                "dataflow collector capability gap was not reported honestly",
            ]
        );
    }

    fn quiver_capabilities() -> Value {
        serde_json::json!({
            "direct": [{
                "response": {
                    "implementation_pool": "direct",
                    "capabilities": ["durable_payload_storage", "receipt_status"]
                }
            }],
            "dataflow": [{
                "response": {
                    "delivery_guarantee": "durable_admission",
                    "completion_mode": "durable_admission",
                    "at_least_once": true,
                    "native_quiver": {"supported": true, "enabled": true},
                    "capabilities": []
                }
            }]
        })
    }

    #[test]
    fn quiver_collector_delivery_capabilities_are_valid() {
        let mut failures = Vec::new();
        validate_capabilities(&quiver_capabilities(), true, &mut failures);
        assert!(failures.is_empty(), "{failures:?}");
    }

    #[test]
    fn capability_validation_requires_the_requested_quiver_mode() {
        let mut failures = Vec::new();
        validate_capabilities(&ordinary_capabilities(), true, &mut failures);
        assert_eq!(
            failures,
            [
                "direct collector did not advertise native Quiver payload storage",
                "dataflow collector did not report native Quiver durable admission",
            ]
        );
        let mut failures = Vec::new();
        validate_capabilities(&quiver_capabilities(), false, &mut failures);
        assert_eq!(
            failures,
            [
                "direct collector advertised native Quiver payload storage without --quiver",
                "dataflow collector capability gap was not reported honestly",
            ]
        );
    }
}
