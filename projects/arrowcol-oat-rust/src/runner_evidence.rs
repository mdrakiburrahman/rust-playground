#[derive(Clone, Debug, Default, serde::Serialize)]
struct CellEvidence {
    downstream_comment: Option<&'static str>,
    resource_samples: Vec<ResourceSample>,
    resource_sampling_failures: u64,
    resource_samples_dropped: u64,
    client_reports: HashMap<String, Vec<Value>>,
    processes: Option<Value>,
    measurement_window: Option<(u64, u64)>,
    durable_commits: HashMap<String, Value>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct ResourceSample {
    sampled_at_unix_ms: u64,
    metrics: ResourceMetrics,
}

fn local_downstream_optional(kind: crate::config::OatMode) -> bool {
    kind == crate::config::OatMode::Local
}

#[derive(Debug)]
struct LocalDownstreamUnavailable;

impl std::fmt::Display for LocalDownstreamUnavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("local Kustainer observation is unavailable")
    }
}

impl std::error::Error for LocalDownstreamUnavailable {}

impl CellEvidence {
    fn downstream_unavailable(&mut self, reason: &'static str) {
        self.downstream_comment.get_or_insert(reason);
        tracing::warn!(
            reason,
            "downstream observation unavailable; preserving local collector evidence"
        );
    }

    fn resource_summary(&self) -> Option<ResourceMetrics> {
        if self.resource_samples.is_empty() {
            return None;
        }
        Some(ResourceMetrics {
            cpu_millicores: self
                .resource_samples
                .iter()
                .map(|sample| sample.metrics.cpu_millicores)
                .sum::<f64>()
                / self.resource_samples.len() as f64,
            memory_bytes: self
                .resource_samples
                .iter()
                .map(|sample| sample.metrics.memory_bytes)
                .max()?,
        })
    }

    fn record_client(&mut self, config: &OatConfig, table: &str, output: &str) -> Result<()> {
        let effective = process_configurations(output)?;
        verify_process_configuration(config, ComponentRole::Client, &effective)?;
        let counters = client_phase_evidence(output)?;
        if let Some(window) = counters.get("measurement_window").filter(|value| !value.is_null()) {
            let report = parse_client_metrics(output)?;
            if self.measurement_window != window["start_unix_ms"].as_u64().zip(window["end_unix_ms"].as_u64())
                || window["completed_rows"].as_u64() != Some(report.rows)
            {
                bail!("client phase counters disagree with the armed window or measured-row report");
            }
        }
        self.client_reports.entry(table.to_owned()).or_default().push(json!({
            "phase_counters": counters,
            "consumer_effective_config": effective,
            "execution_image": client_image_evidence(output)?,
        }));
        Ok(())
    }

    fn for_table(&self, table: &str, client: Option<&ClientMetrics>) -> Value {
        let durable = self.durable_commits.get(table);
        json!({
            "version": 1,
            "processes": self.processes,
            "durable_commits": self.durable_commits.get(table),
            "client_phase_reports": self.client_reports.get(table),
            "downstream_status": if self.downstream_comment.is_some() { "unavailable" } else { "observed_or_not_attempted" },
            "downstream_comment": self.downstream_comment,
            "resources": {
                "scope": "collector_fleet_measurement_window_only",
                "cpu_summary": "arithmetic_mean_of_observed_samples",
                "memory_summary": "maximum_observed_sample_not_peak_rss",
                "samples": self.resource_samples,
                "sampling_failures": self.resource_sampling_failures,
                "samples_dropped": self.resource_samples_dropped,
                "retention": "most_recent_4096_samples",
                "cadence_seconds": 1,
                "sample_timeout_seconds": 3,
                "unavailable_reason": self.resource_samples.is_empty().then_some("no_measurement_window_resource_samples"),
            },
            "rows": {
                "attempted": null,
                "sent": null,
                "client_completed": client.map(|metrics| metrics.rows),
                "wal_acked": null,
                "delta_committed": durable.and_then(|value| value["rows"].as_u64()),
                "delta_committed_scope": "observed_new_commits_with_ingress_wholly_in_window_not_all_delivered_rows",
                "unique_delivered": null,
                "unavailable_reason": "no_cell_scoped_commit_or_unique_delivery_evidence",
                "retry_semantics": "upload_completion_does_not_establish_exactly_once_delivery",
            },
            "durable_files": durable.and_then(|value| value["files"].as_u64()),
            "durable_bytes": durable.and_then(|value| value["parquet_bytes"].as_u64()),
            "durable_bytes_per_row": durable.and_then(|value| value["bytes_per_row"].as_f64()),
            "compression_ratio": durable.and_then(|value| value["compression_ratio"].as_f64()),
            "compression_ratio_denominator": "committed_parquet_file_bytes_including_metadata",
            "stage_latency_percentiles": { "commit_seconds": durable.and_then(|value| value.get("commit_latency_seconds")) },
            "backlog": null,
        })
    }
}

enum ResourceSamplerSource {
    Aks,
    Local {
        run_id: String,
        environment: HashMap<String, String>,
    },
}

struct ResourceSampler(Option<JoinHandle<Result<CellEvidence>>>);

impl Drop for ResourceSampler {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

impl ResourceSampler {
    fn start(source: Option<ResourceSamplerSource>, config: OatConfig, start: u64, end: u64) -> Self {
        Self(source.map(|source| {
            tokio::spawn(async move {
                let mut evidence = CellEvidence::default();
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    let now = unix_time_ms_u64()?;
                    if now >= end {
                        break;
                    }
                    if now < start {
                        continue;
                    }
                    let sample = async {
                        match &source {
                            ResourceSamplerSource::Aks => collect_aks_metrics(&config).await,
                            ResourceSamplerSource::Local { run_id, environment } => {
                                collect_local_metrics(&config, run_id, environment).await
                            }
                        }
                    };
                    let timeout_ms = 3_000.min(end - now);
                    match tokio::time::timeout(Duration::from_millis(timeout_ms), sample).await {
                        Ok(Ok(metrics)) => {
                            let sampled_at_unix_ms = unix_time_ms_u64()?;
                            if sampled_at_unix_ms < end {
                                if evidence.resource_samples.len() == 4096 {
                                    evidence.resource_samples.remove(0);
                                    evidence.resource_samples_dropped += 1;
                                }
                                evidence.resource_samples.push(ResourceSample {
                                    sampled_at_unix_ms,
                                    metrics,
                                });
                            } else {
                                evidence.resource_sampling_failures += 1;
                            }
                        }
                        _ => {
                            evidence.resource_sampling_failures += 1;
                            tracing::warn!("resource sample unavailable or exceeded its bounded deadline");
                        }
                    }
                }
                Ok(evidence)
            })
        }))
    }

    async fn finish(mut self, evidence: &mut CellEvidence) -> Result<()> {
        if let Some(task) = self.0.as_mut() {
            let sampled = task.await.context("resource sampler task failed")??;
            self.0.take();
            evidence.resource_samples = sampled.resource_samples;
            evidence.resource_sampling_failures = sampled.resource_sampling_failures;
            evidence.resource_samples_dropped = sampled.resource_samples_dropped;
        }
        Ok(())
    }
}

#[cfg(test)]
mod measurement_evidence_tests {
    use super::*;

    #[test]
    fn downstream_degradation_is_local_only() {
        assert!(local_downstream_optional(crate::config::OatMode::Local));
        assert!(!local_downstream_optional(crate::config::OatMode::Aks));
        let mut evidence = CellEvidence::default();
        evidence.downstream_unavailable("local_qap_initialization_unavailable");
        let public = evidence.for_table("table", None);
        assert_eq!(public["downstream_status"], "unavailable");
        assert!(public["rows"]["delta_committed"].is_null());
        assert!(public["compression_ratio"].is_null());
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct WorkerCounterSnapshot {
    worker: u64,
    attempted_rows: u64,
    attempted_bytes: u64,
    completed_rows: u64,
    completed_bytes: u64,
    error_rows: u64,
    error_bytes: u64,
    cancelled_rows: u64,
    cancelled_bytes: u64,
    best_effort_streams: u64,
    durable_streams: u64,
    send_latency_us: Option<ClientLatency>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct ClientLatency {
    p50: Option<u64>,
    p95: Option<u64>,
    p99: Option<u64>,
    samples: u64,
    precision: String,
}

#[derive(serde::Deserialize)]
struct ClientCounterSnapshot {
    version: u64,
    phase: String,
    scope: String,
    elapsed_seconds: f64,
    workers: Vec<WorkerCounterSnapshot>,
    preparation: Option<Value>,
    measurement_window: Option<ClientMeasurementWindow>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct ClientMeasurementWindow {
    start_unix_ms: u64,
    end_unix_ms: u64,
    completed_rows: u64,
    completed_bytes: u64,
    worker_completed_rows: Vec<u64>,
    pilot_window_rows: Vec<u64>,
    scope: String,
}

fn client_preparation(value: Option<&Value>) -> Result<Value> {
    let Some(value) = value else {
        return Ok(Value::Null);
    };
    let object = value.as_object().context("client preparation must be an object")?;
    let mut result = serde_json::Map::new();
    for key in [
        "corpus_rows",
        "corpus_decoded_bytes",
        "corpus_encoded_bytes",
        "retained_encoded_bytes",
        "memory_reservation_bytes",
    ] {
        if let Some(value) = object.get(key) {
            if value.as_u64().is_none() {
                bail!("client preparation byte/row evidence must be unsigned");
            }
            result.insert(key.to_owned(), value.clone());
        }
    }
    for key in [
        "generation_wall_seconds",
        "generation_cpu_seconds",
        "encoding_wall_seconds",
        "encoding_cpu_seconds",
        "preparation_wall_seconds",
        "preparation_cpu_seconds",
        "generator_rows_per_second",
    ] {
        if let Some(value) = object.get(key) {
            if !value.is_null() && value.as_f64().is_none_or(|value| !value.is_finite() || value < 0.0) {
                bail!("client preparation timing/rate evidence must be finite and nonnegative");
            }
            result.insert(key.to_owned(), value.clone());
        }
    }
    if let Some(value) = object.get("steady_state_generation") {
        if value.as_bool() != Some(false) {
            bail!("unexpected hot-path generation in prepared client");
        }
        result.insert("steady_state_generation".to_owned(), value.clone());
    }
    Ok(Value::Object(result))
}

fn client_phase_evidence(output: &str) -> Result<Value> {
    let snapshots = output
        .lines()
        .filter_map(|line| line.strip_prefix("OAT_CLIENT_METRICS "))
        .map(serde_json::from_str::<ClientCounterSnapshot>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("invalid client counter snapshot")?;
    if snapshots.is_empty() {
        return Ok(json!({ "unavailable_reason": "client_did_not_report_phase_counters" }));
    }
    client_snapshot_delta(&snapshots)
}

#[cfg(test)]
mod downstream_runner_tests {
    use super::*;

    #[derive(Clone, Copy)]
    enum Fault {
        InitUnavailable,
        InitRequired,
        InitTimeout,
        ObservationUnavailable,
        Collector,
        Pinning,
    }

    struct FaultPlatform {
        kind: crate::config::OatMode,
        fault: Fault,
        endpoint: String,
        start: std::sync::atomic::AtomicU64,
        unresolved: bool,
    }

    #[async_trait::async_trait]
    impl Platform for FaultPlatform {
        fn kind(&self) -> crate::config::OatMode {
            self.kind
        }
        fn workload_endpoint(&self, _: &OatConfig) -> String {
            self.endpoint.clone()
        }
        fn router_metrics_endpoint(&self, _: &OatConfig) -> Result<String> {
            Ok(format!("{}/metrics", self.endpoint))
        }
        fn qap_endpoint(&self, _: &OatConfig) -> String {
            self.endpoint.clone()
        }
        async fn ensure_stack(&mut self, _: &OatConfig) -> Result<()> {
            Ok(())
        }
        async fn scale_arrowcols(&mut self, _: &OatConfig, _: u32, _: u32) -> Result<()> {
            Ok(())
        }
        async fn initialize_run_table_pins(&self, _: &OatConfig, _: &str, _: &str, _: u64, _: u64) -> Result<()> {
            Ok(())
        }
        fn qap_observation_mode(&self) -> arrowcol_loadgen::ObservationMode {
            arrowcol_loadgen::ObservationMode::Local
        }
        fn qap_database(&self, _: &OatConfig) -> Result<String> {
            Ok("loadtest".to_owned())
        }
        async fn initialize_qap(&mut self, _: &OatConfig, _: &str, _: &[String]) -> Result<Vec<String>> {
            match self.fault {
                Fault::InitRequired => bail!("required_binding_setup_failed"),
                Fault::InitTimeout => std::future::pending().await,
                Fault::ObservationUnavailable => Ok(vec!["observed".to_owned()]),
                _ => Err(anyhow::anyhow!(LocalDownstreamUnavailable)),
            }
        }
        fn mark_qap_outcome_unresolved(&mut self) {
            self.unresolved = true;
        }
        async fn prepare_client_fleet(&self, _: &OatConfig, _: &str) -> Result<()> {
            Ok(())
        }
        async fn start_client(
            &self,
            _: &OatConfig,
            _: &str,
            table: &str,
            _: usize,
            _: &[String],
        ) -> Result<ClientHandle> {
            Ok(ClientHandle {
                client_id: "client".to_owned(),
                table: table.to_owned(),
                process: ClientProcess::Local {
                    container_id: "fake".to_owned(),
                },
            })
        }
        async fn poll_client(&self, _: &OatConfig, _: &ClientHandle) -> Result<ClientProgress> {
            let armed = self.start.load(std::sync::atomic::Ordering::SeqCst) != 0;
            Ok(ClientProgress {
                ready: true,
                schedule_received: armed,
                completed: armed,
                failure: matches!(self.fault, Fault::Collector).then(|| "collector_failed".to_owned()),
                output: String::new(),
            })
        }
        async fn arm_clients(&self, _: &OatConfig, _: &str, start: u64) -> Result<()> {
            self.start.store(start, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn finish_client(&self, config: &OatConfig, _: ClientHandle) -> Result<String> {
            let start = self.start.load(std::sync::atomic::Ordering::SeqCst) + config.warmup_seconds * 1000;
            let end = start + config.measure_seconds * 1000;
            Ok(format!(
                "measured_rows=100 measured_seconds={} rows_per_second={} measurement_window_start_unix_ms={start} measurement_window_end_unix_ms={end} measurement_started_unix_ms={start}\n",
                config.measure_seconds,
                100.0 / config.measure_seconds as f64
            ))
        }
        async fn cancel_client(&self, _: &OatConfig, _: &ClientHandle) -> Result<()> {
            Ok(())
        }
        async fn cleanup(&mut self, _: &OatConfig) -> Result<()> {
            Ok(())
        }
    }

    async fn run_fault(fault: Fault, kind: crate::config::OatMode) -> Result<(Vec<OatResult>, FaultPlatform)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let metric = if matches!(fault, Fault::Pinning) {
            "arrowcol_router_failures_total{reason=\"pinning_violation\"} 1\n"
        } else {
            "arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"telemetry\",schema=\"sqlserver\",table=\"sqlserver_active_sessions\",collector=\"collector-a\",state=\"active\"} 1\n"
        };
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new()
                    .route("/metrics", axum::routing::get(move || async move { metric }))
                    .fallback(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }),
            )
            .await
        });
        let mut platform = FaultPlatform {
            kind,
            fault,
            endpoint,
            start: std::sync::atomic::AtomicU64::new(0),
            unresolved: false,
        };
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.platform.kind = kind;
        config.qap_timeout_seconds = 1;
        let result = run_matrix_cell(
            &config,
            "orchestration",
            "11111111-1111-4111-8111-111111111111",
            1,
            1,
            &mut platform,
        )
        .await;
        server.abort();
        Ok((result?, platform))
    }

    #[tokio::test]
    async fn unavailable_initialization_and_observation_preserve_local_measurements_only() -> Result<()> {
        for fault in [
            Fault::InitUnavailable,
            Fault::InitTimeout,
            Fault::ObservationUnavailable,
        ] {
            let (rows, platform) = run_fault(fault, crate::config::OatMode::Local).await?;
            assert_eq!(rows[0].status, "succeeded");
            assert_eq!(rows[0].rows, Some(100));
            assert!(rows[0].qap.completion_percent.is_none());
            let evidence: Value = serde_json::from_str(rows[0].evidence_json.as_deref().context("evidence")?)?;
            assert_eq!(evidence["downstream_status"], "unavailable");
            if matches!(fault, Fault::InitTimeout) {
                assert!(platform.unresolved);
            }
            let (rows, _) = run_fault(fault, crate::config::OatMode::Aks).await?;
            assert_eq!(rows[0].status, "failed");
        }
        Ok(())
    }

    #[tokio::test]
    async fn local_degradation_never_hides_required_setup_collector_or_pinning_failures() -> Result<()> {
        for fault in [Fault::InitRequired, Fault::Collector, Fault::Pinning] {
            let (rows, _) = run_fault(fault, crate::config::OatMode::Local).await?;
            assert_ne!(rows[0].status, "succeeded");
            assert!(rows[0].failure.is_some());
        }
        Ok(())
    }
}
fn client_snapshot_delta(snapshots: &[ClientCounterSnapshot]) -> Result<Value> {
    let phase = |name| -> Result<&ClientCounterSnapshot> {
        let mut matches = snapshots.iter().filter(|snapshot| snapshot.phase == name);
        let snapshot = matches.next().context("missing client counter phase")?;
        if matches.next().is_some() {
            bail!("duplicate client counter phase");
        }
        Ok(snapshot)
    };
    for snapshot in snapshots {
        if snapshot.version != 1
            || snapshot.scope != "process_cumulative"
            || !snapshot.elapsed_seconds.is_finite()
            || snapshot.elapsed_seconds < 0.0
        {
            bail!("unsupported client counter contract");
        }
        let mut workers = HashSet::new();
        if snapshot.workers.is_empty() || snapshot.workers.iter().any(|worker| !workers.insert(worker.worker)) {
            bail!("empty or duplicate client counter workers");
        }
    }
    let start = phase("measurement_start")?;
    let end = phase("completed")?;
    if let Some(window) = &end.measurement_window {
        if window.scope != "per_stream_admission_completion_observed_in_half_open_shared_unix_window"
            || window.start_unix_ms >= window.end_unix_ms
            || window.worker_completed_rows.len() != end.workers.len()
            || window
                .worker_completed_rows
                .iter()
                .try_fold(0_u64, |sum, rows| sum.checked_add(*rows))
                != Some(window.completed_rows)
        {
            bail!("invalid exact client measurement-window accounting");
        }
    }
    if end.elapsed_seconds < start.elapsed_seconds || end.workers.len() != start.workers.len() {
        bail!("client counter interval or worker count changed");
    }
    let mut deltas = Vec::new();
    for worker in &end.workers {
        let before = start
            .workers
            .iter()
            .find(|before| before.worker == worker.worker)
            .context("client counter worker identity changed")?;
        let difference = |after: u64, before: u64| after.checked_sub(before).context("client counter reset");
        if let Some(latency) = &worker.send_latency_us {
            if latency.precision != "power_of_two_upper_bound"
                || [latency.p50, latency.p95, latency.p99]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .windows(2)
                    .any(|pair| pair[0] > pair[1])
            {
                bail!("invalid client latency evidence");
            }
        }
        deltas.push(json!({
            "worker": worker.worker,
            "attempted_rows": difference(worker.attempted_rows, before.attempted_rows)?,
            "attempted_bytes": difference(worker.attempted_bytes, before.attempted_bytes)?,
            "client_completed_rows": difference(worker.completed_rows, before.completed_rows)?,
            "client_completed_bytes": difference(worker.completed_bytes, before.completed_bytes)?,
            "error_rows": difference(worker.error_rows, before.error_rows)?,
            "error_bytes": difference(worker.error_bytes, before.error_bytes)?,
            "cancelled_rows": difference(worker.cancelled_rows, before.cancelled_rows)?,
            "cancelled_bytes": difference(worker.cancelled_bytes, before.cancelled_bytes)?,
            "best_effort_streams": difference(worker.best_effort_streams, before.best_effort_streams)?,
            "durable_streams": difference(worker.durable_streams, before.durable_streams)?,
            "process_cumulative_send_latency_us": worker.send_latency_us,
        }));
    }
    Ok(json!({
        "scope": "measurement_start_to_completed_snapshot_delta_not_unique_delivery",
        "elapsed_seconds": end.elapsed_seconds - start.elapsed_seconds,
        "completion_semantics": "validated_admission_response_not_delta_commit",
        "bytes_semantics": "sum_of_protobuf_flight_data_encoded_lengths_excluding_grpc_http2",
        "latency_scope": "process_cumulative_includes_preflight_and_warmup_not_phase_percentiles",
        "workers": deltas,
        "preparation": client_preparation(end.preparation.as_ref())?,
        "measurement_window": end.measurement_window,
    }))
}

#[test]
fn resource_summary_is_observed_mean_and_max_not_an_end_snapshot() {
    let mut evidence = CellEvidence::default();
    assert!(evidence.resource_summary().is_none());
    for (cpu_millicores, memory_bytes) in [(100.0, 200), (300.0, 100)] {
        evidence.resource_samples.push(ResourceSample {
            sampled_at_unix_ms: 1,
            metrics: ResourceMetrics {
                cpu_millicores,
                memory_bytes,
            },
        });
    }
    let metrics = evidence.resource_summary().unwrap();
    assert_eq!(metrics.cpu_millicores, 200.0);
    assert_eq!(metrics.memory_bytes, 200);
}
