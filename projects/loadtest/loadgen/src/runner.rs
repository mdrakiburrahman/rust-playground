use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use arrowcol_protocol::{ImplementationPool, UnixMillis};
use arrowcol_routing::{RoutingStore, SqlServerRoutingConfig, SqlServerRoutingStore};
use serde::Serialize;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    cli::{RunArgs, Scenario},
    generator::{LogicalBatch, Scheduler, build_logical_batch},
    manifest::{
        AckOutcome, CAPABILITY_EVIDENCE_FILE, ManifestWriter, ROUTING_EVIDENCE_FILE, RUN_METADATA_FILE,
        SendManifestEntry, run_metadata,
    },
    schema::{TABLE_COUNT, schema_contract_sha256},
    sdk::prepare_tables,
    transport::{FlightAcknowledgement, FlightTransport, now_unix_millis, status_fields},
};

/// Executes a production finite or continuous benchmark run.
///
/// # Errors
///
/// Returns invalid configuration, dependency, delivery, receipt, or evidence failures.
pub async fn run(arguments: RunArgs) -> Result<()> {
    validate_arguments(&arguments)?;
    let run_id = format!("run-{}-{}", now_unix_millis(), Uuid::new_v4().simple());
    let manifests = ManifestWriter::create(&arguments.results).await?;
    let schema_digest = schema_contract_sha256()?;
    manifests
        .write_json(RUN_METADATA_FILE, &run_metadata(&run_id, &arguments, &schema_digest))
        .await?;

    let transport = FlightTransport::connect(
        &arguments.router_endpoint,
        &arguments.catalog_endpoint,
        &arguments.edge_secret_file,
        &arguments.internal_secret_file,
        arguments.request_timeout.get(),
    )
    .await?;
    let tables = prepare_tables(&arguments, &run_id, &transport, &manifests).await?;
    if tables.len() != TABLE_COUNT {
        bail!("prepared {} schemas instead of {TABLE_COUNT}", tables.len());
    }
    let requested_tables = arguments.tables.iter().cloned().collect::<BTreeSet<_>>();
    let load_tables = tables
        .into_iter()
        .filter(|table| requested_tables.is_empty() || requested_tables.contains(&table.identity.to_string()))
        .collect::<Vec<_>>();
    if load_tables.is_empty() || (!requested_tables.is_empty() && load_tables.len() != requested_tables.len()) {
        bail!("requested load table filter contains an unknown table");
    }

    let capabilities = serde_json::json!({
        "dataflow": transport.capability_evidence(&arguments.dataflow_admin_endpoints).await?,
    });
    manifests.write_json(CAPABILITY_EVIDENCE_FILE, &capabilities).await?;

    if !arguments.capture_settle.get().is_zero() {
        tokio::time::sleep(arguments.capture_settle.get()).await;
    }

    let scheduled_table_count = load_tables.len();
    let scheduler = Arc::new(Scheduler::new(load_tables, arguments.seed)?);
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancellation.cancel();
        }
    });

    let kusto_sync = crate::kusto::LiveSync::start(&arguments, &run_id);

    let mut run_error = None;
    for scenario in arguments.effective_scenarios() {
        if cancellation.is_cancelled() {
            break;
        }
        if !arguments.warmup.get().is_zero()
            && let Err(error) = run_phase(
                &run_id,
                scenario,
                "warmup",
                Some(arguments.warmup.get()),
                &arguments,
                Arc::clone(&scheduler),
                transport.clone(),
                manifests.clone(),
                cancellation.clone(),
                scheduled_table_count,
            )
            .await
        {
            run_error = Some(error);
            cancellation.cancel();
            break;
        }
        let duration = if arguments.continuous {
            None
        } else {
            Some(arguments.measure.get())
        };
        if let Err(error) = run_phase(
            &run_id,
            scenario,
            "measure",
            duration,
            &arguments,
            Arc::clone(&scheduler),
            transport.clone(),
            manifests.clone(),
            cancellation.clone(),
            scheduled_table_count,
        )
        .await
        {
            run_error = Some(error);
            cancellation.cancel();
            break;
        }
    }
    if let Some(sync) = &kusto_sync {
        sync.finish_generation();
    }
    if let Some(error) = run_error {
        eprintln!("FAIL collector generation: {error:#}");
        if let Err(evidence_error) = manifests
            .write_json(
                "run-error.json",
                &serde_json::json!({"run_id": run_id, "error": format!("{error:#}")}),
            )
            .await
        {
            eprintln!("could not retain generation failure evidence: {evidence_error:#}");
        }
        if let Some(sync) = kusto_sync
            && let Err(cleanup_error) = sync.abort().await
        {
            eprintln!("Kusto cleanup failed after generation error: {cleanup_error:#}");
        }
        if let Err(flush_error) = manifests.flush().await {
            eprintln!("failure evidence flush failed: {flush_error:#}");
        }
        return Err(error);
    }
    manifests.flush().await?;
    if let Some(sync) = kusto_sync
        && let Err(error) = sync.stop().await
    {
        eprintln!("Kusto DEGRADED (collector run retained): {error:#}");
        crate::kusto::degraded(&arguments.results, &run_id, &error)?;
    }
    let routing = capture_routing(&arguments, &run_id).await?;
    manifests.write_json(ROUTING_EVIDENCE_FILE, &routing).await?;
    manifests.flush().await?;

    println!("PASS dataflow run={} logical_batches={}", run_id, scheduler.scheduled());
    Ok(())
}

fn validate_arguments(arguments: &RunArgs) -> Result<()> {
    if arguments.lanes == 0 || arguments.batch_rows == 0 || arguments.max_attempts == 0 {
        bail!("lanes, batch rows, and max attempts must be positive");
    }
    if arguments.seed > i64::MAX as u64 {
        bail!("seed must not exceed {}", i64::MAX);
    }
    if arguments.measure.get().is_zero() || arguments.request_timeout.get().is_zero() {
        bail!("measure and request timeout must be positive");
    }
    let scenarios = arguments.effective_scenarios();
    if scenarios.is_empty() {
        bail!("at least one benchmark scenario is required");
    }
    if !arguments.continuous && scenarios.iter().collect::<BTreeSet<_>>().len() != scenarios.len() {
        bail!("benchmark scenarios must not contain duplicates");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_phase(
    run_id: &str,
    scenario: Scenario,
    phase: &str,
    duration: Option<Duration>,
    arguments: &RunArgs,
    scheduler: Arc<Scheduler>,
    transport: FlightTransport,
    manifests: ManifestWriter,
    cancellation: CancellationToken,
    table_count: usize,
) -> Result<()> {
    let phase_started_at = now_unix_millis();
    let phase_duration_ms = duration
        .map(|duration| u64::try_from(duration.as_millis()))
        .transpose()
        .context("driver phase duration exceeds the evidence bound")?;
    let deadline = duration.map(|duration| Instant::now() + duration);
    let first_ordinal = scheduler.scheduled();
    let mut lanes = JoinSet::new();
    let send_policy_failures = Arc::new(AtomicU64::new(0));
    for lane in 0..arguments.lanes {
        let scheduler = Arc::clone(&scheduler);
        let batch_rows = arguments.batch_rows;
        let seed = arguments.seed;
        let transport = transport.clone();
        let manifests = manifests.clone();
        let cancellation = cancellation.clone();
        let run_id = run_id.to_owned();
        let phase = phase.to_owned();
        let request_timeout = arguments.request_timeout.get();
        let retry = RetryPolicy::from_arguments(arguments);
        let send_policy_failures = Arc::clone(&send_policy_failures);
        lanes.spawn(async move {
            loop {
                if cancellation.is_cancelled() {
                    break;
                }
                let enough_tables = scheduler.scheduled().saturating_sub(first_ordinal) >= table_count as u64;
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) && enough_tables {
                    break;
                }
                let scheduled = scheduler.next();
                let batch_seed = seed
                    .checked_add(scheduled.table_batch_ordinal)
                    .context("logical batch seed overflow")?;
                let batch = Arc::new(build_logical_batch(Arc::clone(&scheduled.table), batch_rows, batch_seed).await?);
                let logical_batch_id = format!("{}:{}:{}:{}", run_id, scenario.as_str(), phase, scheduled.ordinal);
                let sent = send_pool_with_retry(
                    &run_id,
                    scenario,
                    &phase,
                    &logical_batch_id,
                    scheduled.ordinal,
                    scheduled.table_batch_ordinal,
                    lane,
                    ImplementationPool::Dataflow,
                    Arc::clone(&batch),
                    &transport,
                    &manifests,
                    request_timeout,
                    retry,
                )
                .await;
                sent.inspect_err(|_| {
                    send_policy_failures.fetch_add(1, Ordering::Relaxed);
                })?;
            }
            Ok::<(), anyhow::Error>(())
        });
    }
    println!(
        "[loadgen] {phase} ({}) started: concurrency={}, rows/batch={}",
        scenario.as_str(),
        arguments.lanes,
        arguments.batch_rows
    );
    let stop_progress = CancellationToken::new();
    let progress = {
        let scheduler = Arc::clone(&scheduler);
        let phase = phase.to_owned();
        let run_id = run_id.to_owned();
        let manifests = manifests.clone();
        let stop = stop_progress.clone();
        let cancellation = cancellation.clone();
        let batch_rows = u64::try_from(arguments.batch_rows)?;
        let started = Instant::now();
        tokio::spawn(async move {
            let mut sequence = 0_u64;
            loop {
                let scheduled = scheduler.scheduled().saturating_sub(first_ordinal);
                let Some(rows) = scheduled.checked_mul(batch_rows) else {
                    cancellation.cancel();
                    bail!("driver scheduled-row count overflow");
                };
                let observed_at = now_unix_millis();
                let phase_remaining_ms = deadline
                    .map(|deadline| u64::try_from(deadline.saturating_duration_since(Instant::now()).as_millis()))
                    .transpose()
                    .context("remaining driver phase duration exceeds the evidence bound")?;
                let frame = serde_json::json!({
                    "run_id": run_id, "phase": phase, "scenario": scenario.as_str(),
                    "phase_started_at": phase_started_at, "phase_duration_ms": phase_duration_ms,
                    "phase_remaining_ms": phase_remaining_ms,
                    "timestamp": observed_at, "scheduled_rows": rows,
                    "send_policy_failures": send_policy_failures.load(Ordering::Relaxed),
                });
                if let Err(error) = manifests.write_json("driver-progress.json", &frame).await {
                    cancellation.cancel();
                    return Err(error);
                }
                if stop.is_cancelled() {
                    return Ok::<(), anyhow::Error>(());
                }
                if sequence.is_multiple_of(10) {
                    println!(
                        "[loadgen] {phase} {:>4}s: {} batches sent",
                        started.elapsed().as_secs(),
                        scheduled
                    );
                }
                sequence += 1;
                tokio::select! {
                    () = stop.cancelled() => {}
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        })
    };
    let mut outcome = Ok(());
    while let Some(result) = lanes.join_next().await {
        if let Err(error) = result.context("load lane task failed").and_then(|inner| inner) {
            outcome = Err(error);
            break;
        }
    }
    lanes.abort_all();
    while lanes.join_next().await.is_some() {}
    stop_progress.cancel();
    let progress_outcome = progress
        .await
        .context("driver progress task failed")
        .and_then(|result| result);
    println!(
        "[loadgen] {phase} finished: {} batches sent",
        scheduler.scheduled().saturating_sub(first_ordinal)
    );
    outcome.and(progress_outcome)
}

#[allow(clippy::too_many_arguments)]
async fn send_pool_with_retry(
    run_id: &str,
    scenario: Scenario,
    phase: &str,
    logical_batch_id: &str,
    ordinal: u64,
    table_batch_ordinal: u64,
    lane: usize,
    pool: ImplementationPool,
    batch: Arc<LogicalBatch>,
    transport: &FlightTransport,
    manifests: &ManifestWriter,
    request_timeout: Duration,
    retry: RetryPolicy,
) -> Result<()> {
    let started = Instant::now();
    let mut attempt = 0_u32;
    loop {
        attempt = attempt.saturating_add(1);
        let sent_at = now_unix_millis();
        match transport
            .send_batch(&batch.table, pool, Arc::clone(&batch.encoded), request_timeout)
            .await
        {
            Ok(acknowledgement) => {
                let acknowledged_at = now_unix_millis();
                let outcome = match acknowledgement {
                    FlightAcknowledgement::BestEffortAdmission => AckOutcome::BestEffortAdmission,
                    FlightAcknowledgement::DurableAdmission => AckOutcome::DurableAdmission,
                };
                manifests
                    .write_send(&SendManifestEntry {
                        run_id: run_id.to_owned(),
                        scenario,
                        phase: phase.to_owned(),
                        logical_batch_id: logical_batch_id.to_owned(),
                        ordinal,
                        table_batch_ordinal,
                        table: batch.table.identity.to_string(),
                        schema_version: batch.table.schema_version,
                        schema_fingerprint: batch.table.fingerprint.clone(),
                        rows: batch.rows,
                        arrow_bytes: batch.arrow_bytes,
                        payload_sha256: batch.payload_sha256.clone(),
                        row_sha256: batch.row_sha256.clone(),
                        row_hashes: batch.row_hashes.clone(),
                        lane,
                        pool: pool.to_string(),
                        attempt,
                        sent_at_unix_ms: sent_at,
                        acknowledged_at_unix_ms: acknowledged_at,
                        outcome,
                        error_code: None,
                        error: None,
                    })
                    .await?;
                return Ok(());
            }
            Err(error) => {
                let (error_code, message) = status_fields(&error);
                manifests
                    .write_send(&SendManifestEntry {
                        run_id: run_id.to_owned(),
                        scenario,
                        phase: phase.to_owned(),
                        logical_batch_id: logical_batch_id.to_owned(),
                        ordinal,
                        table_batch_ordinal,
                        table: batch.table.identity.to_string(),
                        schema_version: batch.table.schema_version,
                        schema_fingerprint: batch.table.fingerprint.clone(),
                        rows: batch.rows,
                        arrow_bytes: batch.arrow_bytes,
                        payload_sha256: batch.payload_sha256.clone(),
                        row_sha256: batch.row_sha256.clone(),
                        row_hashes: batch.row_hashes.clone(),
                        lane,
                        pool: pool.to_string(),
                        attempt,
                        sent_at_unix_ms: sent_at,
                        acknowledged_at_unix_ms: now_unix_millis(),
                        outcome: AckOutcome::Error,
                        error_code: Some(error_code),
                        error: Some(message),
                    })
                    .await?;
                if !retry.allows_another(attempt, started.elapsed()) {
                    return Err(error.context("delivery attempts exhausted"));
                }
                tokio::time::sleep(RetryPolicy::backoff(attempt)).await;
            }
        }
    }
}

/// Bounds physical delivery attempts for one pool/logical batch.
///
/// `max_attempts` is the floor; an optional window keeps retrying past it so a
/// load run can ride out a deliberate collector outage (the Quiver crash demo).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RetryPolicy {
    max_attempts: u32,
    window: Option<Duration>,
}

impl RetryPolicy {
    fn from_arguments(arguments: &RunArgs) -> Self {
        Self {
            max_attempts: arguments.max_attempts,
            window: arguments.retry_window.map(|window| window.get()),
        }
    }

    fn allows_another(&self, attempts: u32, elapsed: Duration) -> bool {
        attempts < self.max_attempts || self.window.is_some_and(|window| elapsed < window)
    }

    fn backoff(attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1).min(5);
        Duration::from_millis(100_u64.saturating_mul(1_u64 << exponent).min(2_000))
    }
}

async fn capture_routing(arguments: &RunArgs, run_id: &str) -> Result<RoutingEvidence> {
    let config = SqlServerRoutingConfig::with_password_file(
        &arguments.sql_host,
        arguments.sql_port,
        &arguments.sql_database,
        &arguments.sql_username,
        &arguments.sql_password_file,
    )?
    .trust_server_certificate(true)
    .with_timeouts(arguments.request_timeout.get(), arguments.request_timeout.get());
    let store = SqlServerRoutingStore::new(config);
    let loaded_at = UnixMillis::new(now_unix_millis())?;
    let view = store.load_view(loaded_at).await?;
    let collectors = view
        .collectors
        .iter()
        .map(|(id, collector)| RoutingCollectorEvidence {
            instance_id: id.to_string(),
            instance_key: collector.registration.instance_key.clone(),
            implementation_pool: collector.registration.implementation_pool.to_string(),
            endpoint: collector.registration.flight_endpoint.to_string(),
            state: format!("{:?}", collector.state).to_lowercase(),
            terminated: collector.is_terminated_at(loaded_at),
            received_rows: collector.pressure.as_ref().map(|pressure| pressure.received_rows.get()),
            received_batches: collector
                .pressure
                .as_ref()
                .map(|pressure| pressure.received_batches.get()),
        })
        .collect::<Vec<_>>();
    let leases = view
        .leases
        .values()
        .map(|lease| RoutingLeaseEvidence {
            implementation_pool: lease.key.implementation_pool.to_string(),
            table: lease.key.table.to_string(),
            holder_instance_id: lease.holder_instance_id.to_string(),
            fencing_epoch: lease.fencing_epoch.get(),
            lease_version: lease.lease_version,
            state: format!("{:?}", lease.state).to_lowercase(),
        })
        .collect();
    Ok(RoutingEvidence {
        run_id: run_id.to_owned(),
        loaded_at_unix_ms: view.loaded_at_unix_ms.get(),
        collectors,
        leases,
    })
}

#[derive(Serialize)]
struct RoutingEvidence {
    run_id: String,
    loaded_at_unix_ms: u64,
    collectors: Vec<RoutingCollectorEvidence>,
    leases: Vec<RoutingLeaseEvidence>,
}

#[derive(Serialize)]
struct RoutingCollectorEvidence {
    instance_id: String,
    instance_key: String,
    implementation_pool: String,
    endpoint: String,
    state: String,
    /// Registration expired or the process stopped/failed, such as a predecessor killed by the
    /// Quiver crash demo whose leases moved to its restarted successor.
    terminated: bool,
    received_rows: Option<u64>,
    received_batches: Option<u64>,
}

#[derive(Serialize)]
struct RoutingLeaseEvidence {
    implementation_pool: String,
    table: String,
    holder_instance_id: String,
    fencing_epoch: u64,
    lease_version: u64,
    state: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_honors_attempts_without_a_window() {
        let policy = RetryPolicy {
            max_attempts: 3,
            window: None,
        };
        assert!(policy.allows_another(2, Duration::from_secs(3_600)));
        assert!(!policy.allows_another(3, Duration::ZERO));
    }

    #[test]
    fn retry_window_extends_attempts_through_an_outage() {
        let policy = RetryPolicy {
            max_attempts: 3,
            window: Some(Duration::from_secs(120)),
        };
        assert!(policy.allows_another(50, Duration::from_secs(119)));
        assert!(!policy.allows_another(50, Duration::from_secs(120)));
        assert!(
            policy.allows_another(1, Duration::from_secs(600)),
            "attempt floor still applies"
        );
    }

    #[test]
    fn retry_backoff_is_capped() {
        assert_eq!(RetryPolicy::backoff(1), Duration::from_millis(100));
        assert_eq!(RetryPolicy::backoff(5), Duration::from_millis(1_600));
        assert_eq!(RetryPolicy::backoff(40), Duration::from_millis(2_000));
    }
}
