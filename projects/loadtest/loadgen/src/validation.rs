use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
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
    let sends: Vec<SendManifestEntry> = read_json_lines(&arguments.results.join(SEND_MANIFEST_FILE)).await?;
    let receipts: Vec<ReceiptManifestEntry> = read_json_lines(&arguments.results.join(RECEIPT_MANIFEST_FILE)).await?;
    let contracts: Vec<SchemaContract> = read_json(&arguments.results.join(SCHEMA_CONTRACT_FILE)).await?;
    let routing: RoutingEvidence = read_json(&arguments.results.join(ROUTING_EVIDENCE_FILE)).await?;
    let capabilities: Value = read_json(&arguments.results.join(CAPABILITY_EVIDENCE_FILE)).await?;

    let mut failures = Vec::new();
    let kusto_settle = if arguments.collectors_drained {
        let metadata = read_json(&arguments.results.join("run-metadata.json")).await?;
        let publication = read_json(&arguments.results.join("publication-drain.json")).await?;
        let mutators = read_json(&arguments.results.join("mutator-drain.json")).await?;
        check_drain_receipts(&metadata, &publication, &mutators)?;
        None
    } else if arguments.kusto_endpoint.is_some() {
        crate::kusto::settle_dataflow(
            &arguments.dataflow_admin_endpoints,
            arguments.timeout.get(),
            arguments.quiver,
        )
        .await
        .err()
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

    let direct = if expected_tables.contains_key("direct") {
        Some(
            wait_for_snapshot(
                &arguments.direct_root,
                required_rows.get("direct").context("direct row expectations missing")?,
                arguments.timeout.get(),
            )
            .await?,
        )
    } else {
        None
    };
    let dataflow = if expected_tables.contains_key("dataflow") {
        Some(
            wait_for_snapshot(
                &arguments.dataflow_root,
                required_rows
                    .get("dataflow")
                    .context("dataflow row expectations missing")?,
                arguments.timeout.get(),
            )
            .await?,
        )
    } else {
        None
    };
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
    ) -> Result<SnapshotSummary> {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = if root.to_str().is_some_and(|root| root.starts_with("abfss://")) {
                delta_inspect::summarize_cloud(
                    root.to_str().context("non-UTF8 cloud root")?,
                    &expected.keys().cloned().collect::<Vec<_>>(),
                )
                .await
            } else {
                delta_inspect::summarize(root).await
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

    let kusto = if let Some(endpoint) = &arguments.kusto_endpoint {
        let validation = if let Some(error) = kusto_settle {
            Err(error)
        } else if let (Some(direct), Some(dataflow)) = (&direct, &dataflow) {
            tokio::time::timeout(
                arguments.timeout.get(),
                crate::kusto::validate(
                    endpoint,
                    &arguments.results,
                    &arguments.direct_root,
                    &arguments.dataflow_root,
                    direct,
                    dataflow,
                ),
            )
            .await
            .context("Kusto validation deadline elapsed")
            .and_then(|report| report)
        } else {
            Err(anyhow::anyhow!(
                "Kusto validation requires both independent Delta snapshots"
            ))
        };
        Some(match validation {
            Ok(report) => {
                crate::kusto::write_validation(&arguments.results, &report)?;
                report
            }
            Err(error) => {
                failures.push(format!("Kusto validation failed: {error:#}"));
                let metadata: Value = read_json(&arguments.results.join(crate::manifest::RUN_METADATA_FILE)).await?;
                crate::kusto::degraded(
                    &arguments.results,
                    metadata["run_id"].as_str().context("run identity missing")?,
                    &error,
                )?;
                crate::kusto::validation_failure(&arguments.results, &error)?
            }
        })
    } else {
        None
    };
    let report = ValidationReport {
        passed: failures.is_empty(),
        failures,
        admitted_rows,
        required_rows,
        direct,
        dataflow,
        routing,
        kusto,
    };
    let mut bytes = serde_json::to_vec_pretty(&report)?;
    bytes.push(b'\n');
    fs::write(arguments.results.join(VALIDATION_FILE), bytes).await?;
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
    passed: bool,
    failures: Vec<String>,
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
