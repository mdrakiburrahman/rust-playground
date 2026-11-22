//! Delta Kernel events and SDK publication observations on the caller's OTel provider.

use delta_kernel::metrics::{CommitFailureReason, MetricEvent, MetricsReporter};
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::{KeyValue, global};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Validate and cache the process-wide opt-in interval at collector startup.
pub fn stage_telemetry_interval() -> anyhow::Result<Option<Duration>> {
    static CONFIG: OnceLock<Result<Option<Duration>, String>> = OnceLock::new();
    CONFIG
        .get_or_init(|| match std::env::var("ARROWCOL_STAGE_TELEMETRY_INTERVAL_MS") {
            Ok(value) => parse_stage_interval(&value)
                .map(Some)
                .map_err(|error| error.to_string()),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(format!("invalid ARROWCOL_STAGE_TELEMETRY_INTERVAL_MS: {error}")),
        })
        .as_ref()
        .copied()
        .map_err(|error| anyhow::anyhow!("{error}"))
}

fn parse_stage_interval(value: &str) -> anyhow::Result<Duration> {
    let ms = value
        .parse::<u64>()
        .ok()
        .filter(|ms| (100..=60_000).contains(ms))
        .ok_or_else(|| anyhow::anyhow!("ARROWCOL_STAGE_TELEMETRY_INTERVAL_MS must be an integer in 100..=60000"))?;
    Ok(Duration::from_millis(ms))
}

#[derive(Default)]
pub(crate) struct CommitStageAttempt {
    pub(crate) log_and_conflict: Option<Duration>,
    pub(crate) local_sync: Option<Duration>,
}

pub(crate) fn claim_commit_sample(interval: Duration) -> bool {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    static NEXT_MS: AtomicU64 = AtomicU64::new(0);
    let now = ORIGIN
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    claim_sample(&NEXT_MS, now, interval.as_millis() as u64)
}

fn claim_sample(next: &AtomicU64, now: u64, interval_ms: u64) -> bool {
    let mut previous = next.load(Ordering::Relaxed);
    loop {
        if previous == u64::MAX || now < previous {
            return false;
        }
        match next.compare_exchange_weak(
            previous,
            now.saturating_add(interval_ms),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(actual) => previous = actual,
        }
    }
}

pub(crate) fn sampled_commit_stages(
    timing: &Mutex<CommitStageAttempt>,
    failed_attempts: u32,
    retry_time: Duration,
    extra_sync: Duration,
) -> serde_json::Value {
    match timing.lock() {
        Ok(timing) => commit_stage_values(Some(&timing), failed_attempts, retry_time, extra_sync, None),
        Err(_) => commit_stage_values(
            None,
            failed_attempts,
            retry_time,
            extra_sync,
            Some("commit_stage_lock_poisoned"),
        ),
    }
}

fn commit_stage_values(
    timing: Option<&CommitStageAttempt>,
    failed_attempts: u32,
    retry_time: Duration,
    extra_sync: Duration,
    lock_error: Option<&'static str>,
) -> serde_json::Value {
    let stage = |value: Option<Duration>, reason: &'static str, scope: &'static str| {
        serde_json::json!({
            "seconds": value.map(|v| v.as_secs_f64()), "unavailable_reason": value.is_none().then_some(reason), "scope": scope,
        })
    };
    let log = timing.and_then(|t| t.log_and_conflict);
    let sync = timing.and_then(|t| t.local_sync).map(|v| v + extra_sync);
    serde_json::json!({
        "sampling_scope": "process_wide_interval_claimed_at_first_attempt_reported_on_definite_success",
        "failed_attempts": failed_attempts,
        "conflict_retry": stage(Some(retry_time), "", "failed_transaction_commit_invocations_and_backoff_only_excludes_reconciliation_and_snapshot_refresh"),
        "log_write": stage(log, lock_error.unwrap_or("committer_timing_unavailable"), "successful_filesystem_committer_invocation_including_opaque_kernel_work_not_pure_log_write"),
        "local_durability_sync": stage(sync, lock_error.unwrap_or("local_sync_not_observed_or_nonlocal_log"), "successful_attempt_local_file_and_directory_fsyncs_only_not_response_ack"),
    })
}

/// Attach this reporter using Kernel's `WithMetricsReporterLayer` to the process's
/// global tracing subscriber. Install the global OTel meter provider first.
/// Operation IDs, correlation IDs, paths and account names are never metric labels.
pub fn kernel_metrics_reporter() -> Arc<dyn MetricsReporter> {
    Arc::new(KernelReporter::new(global::meter("arrow-deltalake")))
}

#[derive(Debug)]
struct KernelReporter {
    events: Counter<u64>,
    duration: Histogram<f64>,
    values: Histogram<u64>,
    bytes: Histogram<u64>,
    version: Gauge<u64>,
}

impl KernelReporter {
    fn new(meter: Meter) -> Self {
        Self {
            events: meter.u64_counter("delta.kernel.events").build(),
            duration: meter.f64_histogram("delta.kernel.duration").with_unit("s").build(),
            values: meter.u64_histogram("delta.kernel.values").with_unit("1").build(),
            bytes: meter.u64_histogram("delta.kernel.bytes").with_unit("By").build(),
            // No unit: Prometheus OTLP translation suffixes unit "1" gauges with `_ratio`,
            // which would rename the required `delta_kernel_version` family.
            version: meter.u64_gauge("delta.kernel.version").build(),
        }
    }

    fn event(&self, event: &'static str, outcome: &'static str) {
        self.events
            .add(1, &[KeyValue::new("event", event), KeyValue::new("outcome", outcome)]);
    }

    fn duration(&self, event: &'static str, phase: &'static str, value: Duration) {
        self.duration.record(
            value.as_secs_f64(),
            &[KeyValue::new("event", event), KeyValue::new("phase", phase)],
        );
    }

    fn value(&self, event: &'static str, field: &'static str, value: u64) {
        self.values
            .record(value, &[KeyValue::new("event", event), KeyValue::new("field", field)]);
    }

    fn bytes(&self, event: &'static str, field: &'static str, value: u64) {
        self.bytes
            .record(value, &[KeyValue::new("event", event), KeyValue::new("field", field)]);
    }

    fn version(&self, event: &'static str, value: u64) {
        self.version.record(value, &[KeyValue::new("event", event)]);
    }
}

impl MetricsReporter for KernelReporter {
    fn report(&self, event: MetricEvent) {
        match event {
            MetricEvent::LogSegmentLoadSuccess(e) => {
                self.event("log_segment", "success");
                self.duration("log_segment", "total", e.duration);
                for (field, value) in [
                    ("commits", e.num_commit_files),
                    ("checkpoints", e.num_checkpoint_files),
                    ("compactions", e.num_compaction_files),
                ] {
                    self.value("log_segment", field, value);
                }
                if let Some(value) = e.crc_versions_behind {
                    self.value("log_segment", "crc_versions_behind", value);
                }
            }
            MetricEvent::LogSegmentLoadFailure(_) => self.event("log_segment", "failure"),
            MetricEvent::ProtocolMetadataLoadSuccess(e) => {
                self.event("protocol_metadata", "success");
                self.duration("protocol_metadata", "total", e.duration);
            }
            MetricEvent::ProtocolMetadataLoadFailure(_) => self.event("protocol_metadata", "failure"),
            MetricEvent::SnapshotBuildSuccess(e) => {
                self.event("snapshot", "success");
                self.duration("snapshot", "total", e.duration);
                self.version("snapshot", e.version);
            }
            MetricEvent::SnapshotBuildFailure(_) => self.event("snapshot", "failure"),
            MetricEvent::TransactionCommitSuccess(e) => {
                self.event("transaction", "success");
                self.version("transaction", e.commit_version);
                for (phase, value) in [
                    ("prepare", e.prepare_duration),
                    ("committer", e.committer_duration),
                    ("total", e.total_duration),
                ] {
                    self.duration("transaction", phase, value);
                }
                for (field, value) in [
                    ("adds", e.num_add_files),
                    ("removes", e.num_remove_files),
                    ("dv_updates", e.num_dv_updates),
                    ("blind_append", u64::from(e.is_blind_append)),
                    ("data_change", u64::from(e.data_change)),
                ] {
                    self.value("transaction", field, value);
                }
                self.bytes("transaction", "adds", e.add_files_bytes);
                self.bytes("transaction", "removes", e.remove_files_bytes);
            }
            MetricEvent::TransactionCommitFailure(e) => self.event(
                "transaction",
                match e.reason {
                    CommitFailureReason::Conflict => "conflict",
                    CommitFailureReason::RetryableIo => "retryable_io",
                    CommitFailureReason::Error => "failure",
                },
            ),
            MetricEvent::DomainMetadataLoadSuccess(e) => {
                self.event("domain_metadata", "success");
                self.duration("domain_metadata", "total", e.duration);
                self.value("domain_metadata", "cached", u64::from(e.from_cache));
                self.value("domain_metadata", "domains", e.num_domains_returned);
            }
            MetricEvent::DomainMetadataLoadFailure => self.event("domain_metadata", "failure"),
            MetricEvent::SetTransactionLoadSuccess(e) => {
                self.event("set_transaction", "success");
                self.duration("set_transaction", "total", e.duration);
                self.value("set_transaction", "cached", u64::from(e.from_cache));
                self.value("set_transaction", "found", u64::from(e.found));
            }
            MetricEvent::SetTransactionLoadFailure => self.event("set_transaction", "failure"),
            MetricEvent::CrcReadSuccess(e) => {
                self.event("crc_read", "success");
                self.duration("crc_read", "total", e.duration);
                self.bytes("crc_read", "read", e.bytes_read);
            }
            MetricEvent::CrcReadFailure => self.event("crc_read", "failure"),
            MetricEvent::JsonReadCompleted(e) => {
                self.event("json_read", "completed");
                self.value("json_read", "files", e.num_files);
                self.bytes("json_read", "read", e.bytes_read);
            }
            MetricEvent::ParquetReadCompleted(e) => {
                self.event("parquet_read", "completed");
                self.value("parquet_read", "files", e.num_files);
                self.bytes("parquet_read", "read", e.bytes_read);
            }
            MetricEvent::ScanMetadataCompleted(e) => {
                self.event("scan_metadata", "completed");
                for (phase, value) in [
                    ("total", e.duration),
                    ("dedup", e.dedup_visitor_time),
                    ("action_transform", e.action_transform_time),
                    ("predicate_eval", e.predicate_eval_time),
                ] {
                    self.duration("scan_metadata", phase, value);
                }
                for (field, value) in [
                    ("adds_seen", e.num_add_files_seen),
                    ("delta_adds_seen", e.num_add_files_seen_from_delta_files),
                    ("selected_adds", e.num_selected_add_files),
                    ("delta_removes_seen", e.num_remove_files_seen_from_delta_files),
                    ("non_file_actions", e.num_non_file_actions),
                    ("predicate_filtered", e.num_predicate_filtered),
                    ("peak_hash_set", e.peak_hash_set_size as u64),
                ] {
                    self.value("scan_metadata", field, value);
                }
                self.bytes("scan_metadata", "selected_adds", e.selected_add_files_bytes);
            }
            MetricEvent::StorageListCompleted(e) => {
                self.event("storage_list", "completed");
                self.duration("storage_list", "total", e.duration);
                self.value("storage_list", "files", e.num_files);
            }
            MetricEvent::StorageReadCompleted(e) => {
                self.event("storage_read", "completed");
                self.duration("storage_read", "total", e.duration);
                self.value("storage_read", "files", e.num_files);
                self.bytes("storage_read", "read", e.bytes_read);
            }
            MetricEvent::StorageCopyCompleted(e) => {
                self.event("storage_copy", "completed");
                self.duration("storage_copy", "total", e.duration);
            }
        }
    }
}

struct PublicationMetrics {
    events: Counter<u64>,
    duration: Histogram<f64>,
    committed_rows: Counter<u64>,
    committed_files: Counter<u64>,
    committed_input_bytes: Counter<u64>,
    committed_bytes: Counter<u64>,
}

fn publications() -> &'static PublicationMetrics {
    static METRICS: OnceLock<PublicationMetrics> = OnceLock::new();
    METRICS.get_or_init(|| {
        let meter = global::meter("arrow-deltalake");
        PublicationMetrics {
            events: meter.u64_counter("delta.publication.events").build(),
            duration: meter.f64_histogram("delta.publication.duration").with_unit("s").build(),
            committed_rows: meter.u64_counter("delta.commit.rows").build(),
            committed_files: meter.u64_counter("delta.commit.files").build(),
            committed_input_bytes: meter.u64_counter("delta.commit.input_bytes").with_unit("By").build(),
            committed_bytes: meter.u64_counter("delta.commit.parquet_bytes").with_unit("By").build(),
        }
    })
}

/// Counts only newly committed AddFiles after the local durability barrier.
/// These process counters are not a per-cell measurement without boundary evidence.
#[allow(clippy::too_many_arguments)]
pub(crate) fn data_commit(
    version: u64,
    rows: u64,
    input_bytes: u64,
    parquet_bytes: u64,
    files: u64,
    min_ingested_at_ms: Option<i64>,
    max_ingested_at_ms: Option<i64>,
    elapsed: Duration,
    table_id: &str,
    table_name: Option<&str>,
    commit_stages: Option<serde_json::Value>,
) {
    let metrics = publications();
    metrics.committed_rows.add(rows, &[]);
    metrics.committed_files.add(files, &[]);
    metrics.committed_input_bytes.add(input_bytes, &[]);
    metrics.committed_bytes.add(parquet_bytes, &[]);
    let mut event = serde_json::json!({
        "schema_version": 1,
        "outcome": "committed",
        "table_id": table_id,
        "table_name": table_name,
        "delta_version": version,
        "rows": rows,
        "files": files,
        "input_bytes": input_bytes,
        "parquet_bytes": parquet_bytes,
        "min_ingested_at_ms": min_ingested_at_ms,
        "max_ingested_at_ms": max_ingested_at_ms,
        "finished_at_unix_ms": chrono::Utc::now().timestamp_millis(),
        "elapsed_seconds": elapsed.as_secs_f64()
    });
    if let Some(stages) = commit_stages {
        event["commit_stages"] = stages;
    }
    eprintln!("arrowcol_data_commit {event}");
    tracing::info!(commit = %event, "arrowcol_data_commit");
}

pub(crate) fn ambiguous_commit_reconciled(table_id: &str, table_name: Option<&str>, elapsed: Duration) {
    publication_event("commit_attribution_uncertain");
    let event = ambiguous_commit_event(table_id, table_name, elapsed);
    eprintln!("arrowcol_data_commit {event}");
    tracing::warn!(
        commit = %event,
        "arrowcol_data_commit"
    );
}

fn ambiguous_commit_event(table_id: &str, table_name: Option<&str>, elapsed: Duration) -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1,
        "outcome": "reconciled",
        "table_id": table_id,
        "table_name": table_name,
        "delta_version": null,
        "rows": null,
        "files": null,
        "input_bytes": null,
        "parquet_bytes": null,
        "min_ingested_at_ms": null,
        "max_ingested_at_ms": null,
        "finished_at_unix_ms": chrono::Utc::now().timestamp_millis(),
        "elapsed_seconds": elapsed.as_secs_f64(),
        "reason": "uncertain_commit_version_attribution"
    })
}

/// Count a publication-side event outside a timed publication attempt.
pub(crate) fn publication_event(outcome: &'static str) {
    publications().events.add(1, &[KeyValue::new("outcome", outcome)]);
}

pub(crate) struct PublicationObservation {
    started: Instant,
    outcome: &'static str,
}

impl PublicationObservation {
    pub(crate) fn start() -> Self {
        publications();
        Self {
            started: Instant::now(),
            outcome: "failure",
        }
    }

    pub(crate) fn event(&self, outcome: &'static str) {
        publications().events.add(1, &[KeyValue::new("outcome", outcome)]);
    }

    pub(crate) fn finish(&mut self, outcome: &'static str) {
        self.outcome = outcome;
    }
}

impl Drop for PublicationObservation {
    fn drop(&mut self) {
        self.event(self.outcome);
        publications().duration.record(
            self.started.elapsed().as_secs_f64(),
            &[KeyValue::new("outcome", self.outcome)],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_interval_and_sampling_boundaries() {
        for value in ["", "99", "60001", "-1", "1.5", "overflow999999999999999999999999"] {
            assert!(parse_stage_interval(value).is_err(), "{value}");
        }
        assert_eq!(parse_stage_interval("100").unwrap(), Duration::from_millis(100));
        assert_eq!(parse_stage_interval("60000").unwrap(), Duration::from_secs(60));
        let next = AtomicU64::new(0);
        assert!(claim_sample(&next, 0, 100));
        assert!(!claim_sample(&next, 99, 100));
        assert!(claim_sample(&next, 100, 100));
        assert!(claim_sample(&next, u64::MAX, 100));
        assert!(!claim_sample(&next, u64::MAX, 100));
    }

    #[test]
    fn concurrent_sampling_claim_is_bounded() {
        let next = AtomicU64::new(0);
        let winners = std::thread::scope(|scope| {
            let handles = (0..16)
                .map(|_| scope.spawn(|| claim_sample(&next, 0, 100)))
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .filter(|won| *won)
                .count()
        });
        assert_eq!(winners, 1);
    }

    #[test]
    fn measured_values_are_scoped_and_poison_is_explicit() {
        let local = CommitStageAttempt {
            log_and_conflict: Some(Duration::from_millis(8)),
            local_sync: Some(Duration::from_millis(3)),
        };
        let values = commit_stage_values(
            Some(&local),
            2,
            Duration::from_millis(7),
            Duration::from_millis(2),
            None,
        );
        assert_eq!(values["conflict_retry"]["seconds"], 0.007);
        assert_eq!(values["log_write"]["seconds"], 0.008);
        assert_eq!(values["local_durability_sync"]["seconds"], 0.005);
        assert!(
            values["log_write"]["scope"]
                .as_str()
                .unwrap()
                .contains("not_pure_log_write")
        );
        let cloud = commit_stage_values(
            Some(&CommitStageAttempt::default()),
            0,
            Duration::ZERO,
            Duration::ZERO,
            None,
        );
        assert!(cloud["local_durability_sync"]["seconds"].is_null());
        assert_eq!(
            cloud["local_durability_sync"]["unavailable_reason"],
            "local_sync_not_observed_or_nonlocal_log"
        );
        let poisoned = Mutex::new(local);
        let _ = std::panic::catch_unwind(|| {
            let _guard = poisoned.lock().unwrap();
            panic!("poison test");
        });
        let values = sampled_commit_stages(&poisoned, 2, Duration::from_millis(7), Duration::ZERO);
        assert_eq!(values["conflict_retry"]["seconds"], 0.007);
        for name in ["log_write", "local_durability_sync"] {
            assert!(values[name]["seconds"].is_null());
            assert_eq!(values[name]["unavailable_reason"], "commit_stage_lock_poisoned");
        }
    }

    #[test]
    fn ambiguous_commit_event_has_table_boundary_and_null_attribution() {
        let event = ambiguous_commit_event("sha256-id", Some("catalog.schema.table"), Duration::from_secs(2));
        assert_eq!(event["table_id"], "sha256-id");
        assert_eq!(event["table_name"], "catalog.schema.table");
        assert_eq!(event["outcome"], "reconciled");
        assert_eq!(event["reason"], "uncertain_commit_version_attribution");
        for field in [
            "delta_version",
            "rows",
            "files",
            "input_bytes",
            "parquet_bytes",
            "min_ingested_at_ms",
            "max_ingested_at_ms",
        ] {
            assert!(event[field].is_null(), "{field}");
        }
        assert!(event["finished_at_unix_ms"].as_i64().is_some());
        assert_eq!(event["elapsed_seconds"], 2.0);
    }
}
