#[derive(Clone, Debug, Default, serde::Serialize)]
struct CellEvidence {
    downstream_comment: Option<&'static str>,
    resource_samples: Vec<ResourceSample>,
    resource_sampling_failures: u64,
    resource_samples_dropped: u64,
    backlog_scrape_configured: bool,
    backlog_samples: Vec<PublicationBacklogSample>,
    backlog_sampling_failures: u64,
    backlog_samples_dropped: u64,
    client_reports: HashMap<String, Vec<Value>>,
    processes: Option<Value>,
    measurement_window: Option<(u64, u64)>,
    durable_commits: HashMap<String, Value>,
    router_sql: Option<Value>,
    router_cpu_pressure: Option<Value>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct ResourceSample {
    sampled_at_unix_ms: u64,
    metrics: ResourceMetrics,
}

#[derive(Clone, Debug, serde::Serialize)]
struct PublicationBacklogSample {
    request_start_unix_ms: u64,
    request_end_unix_ms: u64,
    request_timeout_ms: u64,
    available: Option<bool>,
    publications: Option<u64>,
    rows: Option<u64>,
    logical_bytes: Option<u64>,
    oldest_age_seconds: Option<f64>,
    status: Option<String>,
    unavailable_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct PublicationBacklogMetrics {
    available: bool,
    publications: Option<u64>,
    rows: Option<u64>,
    logical_bytes: Option<u64>,
    oldest_age_seconds: Option<f64>,
    status: String,
}

fn parse_publication_backlog_metrics(text: &str) -> Result<PublicationBacklogMetrics> {
    let mut samples = BTreeMap::new();
    let mut status = None;
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty() && !line.starts_with('#')) {
        let mut fields = line.split_whitespace();
        let sample = fields.next().context("publication backlog metric sample is empty")?;
        let metric = sample.split('{').next().unwrap_or(sample);
        if !metric.starts_with("arrowcol_publication_backlog_") {
            continue;
        }
        let value = fields.next().context("publication backlog metric has no value")?;
        if fields.next().is_some() {
            bail!("publication backlog metric has an unsupported timestamp or extra field");
        }
        match metric {
            "arrowcol_publication_backlog_available"
            | "arrowcol_publication_backlog_publications"
            | "arrowcol_publication_backlog_rows"
            | "arrowcol_publication_backlog_bytes"
            | "arrowcol_publication_backlog_oldest_age_seconds" => {
                if sample != metric {
                    bail!("publication backlog gauge has unexpected labels");
                }
                if samples.insert(metric, value).is_some() {
                    bail!("publication backlog exposition contains a duplicate gauge");
                }
            }
            "arrowcol_publication_backlog_status" => {
                let label = sample
                    .strip_prefix("arrowcol_publication_backlog_status{status=\"")
                    .and_then(|value| value.strip_suffix("\"}"))
                    .context("publication backlog status has unexpected labels")?;
                if label.is_empty()
                    || label.len() > 64
                    || !label.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
                    || value != "1"
                {
                    bail!("publication backlog status sample is invalid");
                }
                if status.replace(label).is_some() {
                    bail!("publication backlog exposition contains duplicate status samples");
                }
            }
            _ if metric.starts_with("arrowcol_publication_backlog_") => {
                bail!("publication backlog exposition contains an unsupported metric");
            }
            _ => {}
        }
    }

    let get = |name| {
        samples
            .get(name)
            .copied()
            .with_context(|| format!("publication backlog exposition omitted {name}"))
    };
    let available = match get("arrowcol_publication_backlog_available")? {
        "0" => false,
        "1" => true,
        _ => bail!("publication backlog availability must be 0 or 1"),
    };
    let raw_publications = get("arrowcol_publication_backlog_publications")?;
    let raw_rows = get("arrowcol_publication_backlog_rows")?;
    let raw_bytes = get("arrowcol_publication_backlog_bytes")?;
    let raw_age = get("arrowcol_publication_backlog_oldest_age_seconds")?;
    let status = status.context("publication backlog exposition omitted status")?.to_owned();

    if available {
        if status != "available" {
            bail!("available publication backlog must have available status");
        }
        let publications = raw_publications.parse::<u64>().context("invalid publication backlog count")?;
        let rows = raw_rows.parse::<u64>().context("invalid publication backlog row count")?;
        let logical_bytes = raw_bytes.parse::<u64>().context("invalid publication backlog byte count")?;
        let oldest_age_seconds = raw_age.parse::<f64>().context("invalid publication backlog age")?;
        if !oldest_age_seconds.is_finite() || oldest_age_seconds < 0.0 {
            bail!("available publication backlog age must be finite and non-negative");
        }
        Ok(PublicationBacklogMetrics {
            available,
            publications: Some(publications),
            rows: Some(rows),
            logical_bytes: Some(logical_bytes),
            oldest_age_seconds: Some(oldest_age_seconds),
            status,
        })
    } else {
        if status == "available"
            || [raw_publications, raw_rows, raw_bytes, raw_age]
                .iter()
                .any(|value| !value.eq_ignore_ascii_case("nan"))
        {
            bail!("unavailable publication backlog must use NaN gauges and an unavailable status");
        }
        Ok(PublicationBacklogMetrics {
            available,
            publications: None,
            rows: None,
            logical_bytes: None,
            oldest_age_seconds: None,
            status,
        })
    }
}

fn failed_publication_backlog_sample(
    start: u64,
    end: u64,
    request_timeout_ms: u64,
    reason: &str,
) -> PublicationBacklogSample {
    PublicationBacklogSample {
        request_start_unix_ms: start,
        request_end_unix_ms: end,
        request_timeout_ms,
        available: None,
        publications: None,
        rows: None,
        logical_bytes: None,
        oldest_age_seconds: None,
        status: None,
        unavailable_reason: Some(reason.to_owned()),
    }
}

fn publication_backlog_sample(
    metrics: PublicationBacklogMetrics,
    request_start_unix_ms: u64,
    request_end_unix_ms: u64,
    request_timeout_ms: u64,
) -> PublicationBacklogSample {
    PublicationBacklogSample {
        request_start_unix_ms,
        request_end_unix_ms,
        request_timeout_ms,
        available: Some(metrics.available),
        publications: metrics.publications,
        rows: metrics.rows,
        logical_bytes: metrics.logical_bytes,
        oldest_age_seconds: metrics.oldest_age_seconds,
        unavailable_reason: (!metrics.available).then(|| metrics.status.clone()),
        status: Some(metrics.status),
    }
}

fn valid_backlog_request_interval(start: u64, end: u64, window_start: u64, window_end: u64) -> bool {
    start >= window_start && end <= window_end && end >= start
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

fn classify_local_qap_error(error: anyhow::Error) -> anyhow::Error {
    if error.is::<arrowcol_loadgen::QapUnavailable>() {
        error.context(LocalDownstreamUnavailable)
    } else {
        error
    }
}

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

    fn client_snapshot_total(&self, table: &str, field: &str) -> Option<u64> {
        let reports = self.client_reports.get(table)?;
        if reports.is_empty() {
            return None;
        }
        reports.iter().try_fold(0_u64, |total, report| {
            report
                .pointer("/phase_counters/workers")?
                .as_array()?
                .iter()
                .try_fold(total, |total, worker| total.checked_add(worker.get(field)?.as_u64()?))
        })
    }

    fn client_cumulative_total(&self, table: &str, field: &str) -> Option<u64> {
        let reports = self.client_reports.get(table)?;
        if reports.is_empty() {
            return None;
        }
        reports.iter().try_fold(0_u64, |total, report| {
            total.checked_add(report.pointer(&format!("/phase_counters/process_cumulative/{field}"))?.as_u64()?)
        })
    }

    fn client_window_payload_bytes(&self, table: &str, client: Option<&ClientMetrics>) -> Option<u64> {
        let client = client?;
        let window = self.measurement_window?;
        let reports = self.client_reports.get(table)?;
        if reports.is_empty() {
            return None;
        }
        let (rows, bytes) = reports.iter().try_fold((0_u64, 0_u64), |(rows, bytes), report| {
            let measurement = report.pointer("/phase_counters/measurement_window")?;
            if measurement["start_unix_ms"].as_u64().zip(measurement["end_unix_ms"].as_u64()) != Some(window) {
                return None;
            }
            Some((
                rows.checked_add(measurement["completed_rows"].as_u64()?)?,
                bytes.checked_add(measurement["completed_bytes"].as_u64()?)?,
            ))
        })?;
        (rows == client.rows).then_some(bytes)
    }

    fn reconciliation(&self, table: &str, durable: Option<&Value>) -> Value {
        let delta_evidence = durable.and_then(|value| value.get("run_cumulative_output"));
        let Some(delta_rows) = delta_evidence.and_then(|value| value.get("rows")).and_then(Value::as_u64) else {
            return json!({
                "status": "unavailable",
                "reason": delta_evidence
                    .and_then(|value| value.get("unavailable_reason"))
                    .and_then(Value::as_str)
                    .unwrap_or("cumulative_delta_rows_unavailable"),
            });
        };
        let Some(completed_rows) = self.client_cumulative_total(table, "completed_rows") else {
            return json!({"status": "unavailable", "reason": "client_cumulative_completed_rows_unavailable"});
        };
        let Some(ambiguous_rows) = self.client_cumulative_total(table, "ambiguous_rows") else {
            return json!({"status": "unavailable", "reason": "client_cumulative_ambiguous_rows_unavailable"});
        };
        let Some(maximum_delta_rows) = completed_rows.checked_add(ambiguous_rows) else {
            return json!({"status": "unavailable", "reason": "client_reconciliation_bound_overflow"});
        };
        let lower_bound_met = completed_rows <= delta_rows;
        let upper_bound_met = delta_rows <= maximum_delta_rows;
        let settle = delta_evidence.and_then(|value| value.get("settle_seconds")).and_then(Value::as_f64);
        let settled = delta_evidence.and_then(|value| value.get("settled")).and_then(Value::as_bool).unwrap_or(false);
        let status = if !upper_bound_met || (settled && !lower_bound_met) {
            "outside_bound"
        } else if lower_bound_met {
            "within_bound"
        } else {
            "unsettled"
        };
        json!({
            "status": status,
            "reason": (status == "unsettled").then_some("delta_commit_drain_timeout"),
            "settle_seconds": settle,
            "settle_timeout_seconds": delta_evidence.and_then(|value| value.get("settle_timeout_seconds")),
            "settled": settled,
            "cell_scope": delta_evidence.and_then(|value| value.get("cell_scope")),
            "delta_rows": delta_rows,
            "completed_rows": completed_rows,
            "ambiguous_rows": ambiguous_rows,
            "gap_rows": completed_rows.saturating_sub(delta_rows),
            "minimum_delta_rows": completed_rows,
            "maximum_delta_rows": maximum_delta_rows,
            "lower_bound_met": lower_bound_met,
            "upper_bound_met": upper_bound_met,
            "exact_match_required": ambiguous_rows == 0,
            "exact_match": delta_rows == completed_rows,
            "scope": "process_cumulative_client_rows_vs_this_cells_new_ingestion_commits_including_warmup",
        })
    }

    fn for_table(&self, table: &str, client: Option<&ClientMetrics>) -> Value {
        let durable = self.durable_commits.get(table);
        let output = durable.and_then(|value| value.get("output_throughput"));
        let observed_cores = self.resource_summary().map(|metrics| metrics.cpu_millicores / 1000.0)
            .filter(|cores| cores.is_finite() && *cores > 0.0);
        let committed_rate = output.and_then(|value| value["rows_per_second"].as_f64());
        let input_rate = output.and_then(|value| value["input_bytes_per_second"].as_f64());
        let parquet_rate = output.and_then(|value| value["parquet_bytes_per_second"].as_f64());
        let payload_bytes = self.client_window_payload_bytes(table, client);
        let window_seconds = self.measurement_window
            .and_then(|(start, end)| end.checked_sub(start))
            .filter(|duration| *duration > 0)
            .map(|duration| duration as f64 / 1000.0);
        let payload_rate = payload_bytes.zip(window_seconds).map(|(bytes, seconds)| bytes as f64 / seconds);
        let backlog = json!({
            "schema_version": 1,
            "source": "arrowcol_publication_backlog_*",
            "scope": "unique_table_owner_admissions_not_confirmed_durable_commit",
            "timestamp_basis": "UTC request start/end around direct /metrics GET; each is a scrape-time snapshot",
            "sampling_interval_ms": PUBLICATION_BACKLOG_SAMPLING_INTERVAL.as_millis(),
            "request_timeout_ms": PUBLICATION_BACKLOG_REQUEST_TIMEOUT.as_millis(),
            "measurement_window": self.measurement_window.map(|(start_unix_ms, end_unix_ms)| json!({
                "start_unix_ms": start_unix_ms, "end_unix_ms": end_unix_ms,
            })),
            "scraper_configured": self.backlog_scrape_configured,
            "samples": self.backlog_samples,
            "sampling_failures": self.backlog_sampling_failures,
            "samples_dropped": self.backlog_samples_dropped,
            "retention": "most_recent_4096_samples",
            "cadence_seconds": 1,
            "last_unavailable_reason": self.backlog_samples.last().and_then(|sample| sample.unavailable_reason.as_deref()),
            "unavailable_reason": if !self.backlog_scrape_configured {
                Some("scraper_not_configured")
            } else if self.backlog_samples.is_empty() {
                Some("no_successful_scrape_samples")
            } else {
                None
            },
        });
        json!({
            "version": 1,
            "processes": self.processes,
            "router_sql": self.router_sql,
            "router_cpu_pressure": self.router_cpu_pressure,
            "durable_commits": self.durable_commits.get(table),
            "reconciliation": self.reconciliation(table, durable),
            "commit_window_output_throughput": output,
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
                "attempted": self.client_snapshot_total(table, "attempted_rows"),
                "attempted_scope": "measurement_start_to_completed_process_snapshot_interval_not_exact_window",
                "sent": null,
                "sent_unavailable_reason": "no_transport_write_completion_counter",
                "client_completed": client.map(|metrics| metrics.rows),
                "wal_acked": null,
                "delta_committed": output.and_then(|value| value["rows"].as_u64()),
                "delta_committed_scope": "ingestion_commit_finish_window_includes_warmup_backlog_not_client_cohort",
                "unique_delivered": null,
                "unique_delivery_unavailable_reason": "commit_counts_do_not_prove_unique_client_delivery",
                "wal_ack_unavailable_reason": "admission_response_is_not_a_wal_fsync_receipt",
                "retry_semantics": "upload_completion_does_not_establish_exactly_once_delivery",
            },
            "durable_rows_per_second": output.and_then(|value| value["rows_per_second"].as_f64()),
            "rate_evidence": {
                "collector_input_bytes_per_second": input_rate,
                "committed_parquet_bytes_per_second": parquet_rate,
                "completed_flight_payload_bytes_per_second": payload_rate,
                "payload_scope": "protobuf_flight_data_encoded_lengths_for_admission_completions_excludes_grpc_and_http2_overhead",
                "observed_collector_cores": observed_cores,
                "committed_rows_per_second_per_observed_collector_core": committed_rate.zip(observed_cores).map(|(rate, cores)| rate / cores),
                "collector_input_bytes_per_second_per_observed_collector_core": input_rate.zip(observed_cores).map(|(rate, cores)| rate / cores),
                "committed_parquet_bytes_per_second_per_observed_collector_core": parquet_rate.zip(observed_cores).map(|(rate, cores)| rate / cores),
                "completed_flight_payload_bytes_per_second_per_observed_collector_core": payload_rate.zip(observed_cores).map(|(rate, cores)| rate / cores),
                "core_scope": "arithmetic_mean_of_observed_collector_fleet_cpu_samples_not_time_weighted",
                "allocated_collector_cores": null,
            },
            "durable_files": output.and_then(|value| value["files"].as_u64()),
            "durable_bytes": output.and_then(|value| value["parquet_bytes"].as_u64()),
            "durable_bytes_per_row": output.and_then(|value| value["bytes_per_row"].as_f64()),
            "compression_ratio": output.and_then(|value| value["compression_ratio"].as_f64()),
            "compression_ratio_denominator": "committed_parquet_file_bytes_including_metadata",
            "stage_latency_percentiles": { "commit_seconds": output.and_then(|value| value.get("commit_latency_seconds")) },
            "backlog": backlog,
        })
    }
}

enum ResourceSamplerSource {
    Aks,
    Local {
        run_id: String,
        environment: HashMap<String, String>,
        collector_metrics_url: Option<String>,
    },
}

struct ResourceSampler(Option<JoinHandle<Result<CellEvidence>>>);

const PUBLICATION_BACKLOG_SAMPLING_INTERVAL: Duration = Duration::from_secs(1);
const PUBLICATION_BACKLOG_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

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
                let mut evidence = CellEvidence {
                    backlog_scrape_configured: true,
                    ..Default::default()
                };
                let backlog_scraper = match PublicationBacklogScraper::start(&source, &config).await {
                    Ok(scraper) => Some(scraper),
                    Err(error) => {
                        tracing::warn!(%error, "publication backlog metrics endpoint unavailable");
                        None
                    }
                };
                let mut interval = tokio::time::interval(PUBLICATION_BACKLOG_SAMPLING_INTERVAL);
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
                    let request_start_unix_ms = unix_time_ms_u64()?;
                    let sample = async {
                        match &source {
                            ResourceSamplerSource::Aks => collect_aks_metrics(&config).await,
                            ResourceSamplerSource::Local {
                                run_id, environment, ..
                            } => {
                                collect_local_metrics(&config, run_id, environment).await
                            }
                        }
                    };
                    let timeout_ms = (PUBLICATION_BACKLOG_REQUEST_TIMEOUT.as_millis() as u64).min(end - now);
                    let backlog_sample = async {
                        let result = match &backlog_scraper {
                            Some(scraper) => scraper.scrape().await,
                            None => Err(anyhow::anyhow!("publication backlog metrics endpoint unavailable")),
                        };
                        Ok::<_, anyhow::Error>((unix_time_ms_u64()?, result))
                    };
                    let (resource_result, backlog_result) = tokio::join!(
                        tokio::time::timeout(Duration::from_millis(timeout_ms), sample),
                        tokio::time::timeout(Duration::from_millis(timeout_ms), backlog_sample),
                    );
                    match resource_result {
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
                    let (request_end_unix_ms, backlog_result) = match backlog_result {
                        Ok(Ok((request_end, result))) => (request_end, result),
                        _ => (unix_time_ms_u64()?, Err(anyhow::anyhow!("publication backlog scrape exceeded its bounded deadline"))),
                    };
                    let backlog_sample = match backlog_result {
                        Ok(metrics) => publication_backlog_sample(
                            metrics,
                            request_start_unix_ms,
                            request_end_unix_ms,
                            timeout_ms,
                        ),
                        _ => {
                            evidence.backlog_sampling_failures += 1;
                            failed_publication_backlog_sample(
                                request_start_unix_ms,
                                request_end_unix_ms,
                                timeout_ms,
                                if backlog_scraper.is_some() {
                                    "scrape_failed"
                                } else {
                                    "endpoint_unavailable"
                                },
                            )
                        }
                    };
                    if valid_backlog_request_interval(request_start_unix_ms, request_end_unix_ms, start, end) {
                        if evidence.backlog_samples.len() == 4096 {
                            evidence.backlog_samples.remove(0);
                            evidence.backlog_samples_dropped += 1;
                        }
                        evidence.backlog_samples.push(backlog_sample);
                    } else {
                        evidence.backlog_sampling_failures += 1;
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
            evidence.backlog_scrape_configured = sampled.backlog_scrape_configured;
            evidence.backlog_samples = sampled.backlog_samples;
            evidence.backlog_sampling_failures = sampled.backlog_sampling_failures;
            evidence.backlog_samples_dropped = sampled.backlog_samples_dropped;
        }
        Ok(())
    }
}

#[cfg(test)]
mod measurement_evidence_tests {
    use super::*;

    fn publication_backlog_exposition(available: u8, publications: &str, rows: &str, bytes: &str, age: &str, status: &str) -> String {
        format!(
            "# HELP arrowcol_publication_backlog_available availability\n\
             # TYPE arrowcol_publication_backlog_available gauge\n\
             arrowcol_publication_backlog_available {available}\n\
             arrowcol_publication_backlog_publications {publications}\n\
             arrowcol_publication_backlog_rows {rows}\n\
             arrowcol_publication_backlog_bytes {bytes}\n\
             arrowcol_publication_backlog_oldest_age_seconds {age}\n\
             arrowcol_publication_backlog_status{{status=\"{status}\"}} 1\n"
        )
    }

    #[test]
    fn publication_backlog_parser_preserves_unavailable_and_rejects_bad_gauges() -> Result<()> {
        let exposition = format!(
            "unrelated_metric 123 1700000000\n{}",
            publication_backlog_exposition(1, "2", "7", "256", "1.25", "available")
        );
        let available = parse_publication_backlog_metrics(&exposition)?;
        assert_eq!(
            available,
            PublicationBacklogMetrics {
                available: true,
                publications: Some(2),
                rows: Some(7),
                logical_bytes: Some(256),
                oldest_age_seconds: Some(1.25),
                status: "available".to_owned(),
            }
        );
        let unavailable = parse_publication_backlog_metrics(&publication_backlog_exposition(
            0, "NaN", "NaN", "NaN", "NaN", "unknown_outcome",
        ))?;
        assert_eq!(unavailable.publications, None);
        assert_eq!(unavailable.rows, None);
        assert_eq!(unavailable.logical_bytes, None);
        assert_eq!(unavailable.oldest_age_seconds, None);
        assert_eq!(unavailable.status, "unknown_outcome");
        assert!(
            parse_publication_backlog_metrics(&publication_backlog_exposition(
                0, "0", "0", "0", "0", "unconfigured",
            ))
            .is_err()
        );
        assert!(
            parse_publication_backlog_metrics(&publication_backlog_exposition(
                1, "NaN", "7", "256", "1.25", "available",
            ))
            .is_err()
        );
        assert!(
            parse_publication_backlog_metrics(&publication_backlog_exposition(
                1, "2", "7", "256", "-0.1", "available",
            ))
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn publication_backlog_fixture_covers_one_timestamped_150_second_cell() -> Result<()> {
        let start = 1_700_000_000_000_u64;
        let end = start + 150_000;
        let mut evidence = CellEvidence {
            measurement_window: Some((start, end)),
            backlog_scrape_configured: true,
            ..Default::default()
        };
        for second in 0..150_u64 {
            let offset = second * 1_000;
            let request_start = start + offset;
            let exposition = publication_backlog_exposition(
                1,
                "3",
                &(120 - (second % 30)).to_string(),
                "8192",
                "2.5",
                "available",
            );
            let metrics = parse_publication_backlog_metrics(&exposition)?;
            evidence.backlog_samples.push(publication_backlog_sample(
                metrics,
                request_start,
                request_start + 20,
                3_000.min(end - request_start),
            ));
        }
        let evidence = evidence.for_table("table", None)["backlog"].clone();
        assert_eq!(evidence["schema_version"], 1);
        assert_eq!(evidence["measurement_window"]["start_unix_ms"], start);
        assert_eq!(evidence["measurement_window"]["end_unix_ms"], end);
        assert_eq!(evidence["sampling_interval_ms"], 1_000);
        assert_eq!(evidence["request_timeout_ms"], 3_000);
        assert_eq!(evidence["scope"], "unique_table_owner_admissions_not_confirmed_durable_commit");
        assert_eq!(evidence["samples"].as_array().context("backlog samples")?.len(), 150);
        let mut bucket_counts = [0_u64; 5];
        for sample in evidence["samples"].as_array().context("backlog samples")? {
            let sample_start = sample["request_start_unix_ms"].as_u64().context("sample start")?;
            let sample_end = sample["request_end_unix_ms"].as_u64().context("sample end")?;
            assert!(sample_start >= start && sample_end <= end && sample_end >= sample_start);
            assert_eq!(sample["request_timeout_ms"], 3_000.min(end - sample_start));
            assert_eq!(sample["available"], true);
            let bucket_index = ((sample_start - start) / 30_000).min(4);
            let bucket_start = start + bucket_index * 30_000;
            let bucket_end = bucket_start + 30_000;
            assert!(sample_start >= bucket_start && sample_end <= bucket_end);
            bucket_counts[bucket_index as usize] += 1;
        }
        assert_eq!(bucket_counts, [30, 30, 30, 30, 30]);
        assert!(!valid_backlog_request_interval(start + 2, start + 1, start, end));
        assert_eq!(evidence["sampling_failures"], 0);
        assert_eq!(evidence["samples_dropped"], 0);
        if let Some(path) = std::env::var_os("OAT_BACKLOG_FIXTURE_OUT") {
            std::fs::write(path, serde_json::to_vec_pretty(&evidence)?)?;
        }
        Ok(())
    }

    #[test]
    fn downstream_degradation_is_local_only() {
        assert!(local_downstream_optional(crate::config::OatMode::Local));
        assert!(!local_downstream_optional(crate::config::OatMode::Aks));
        let mut evidence = CellEvidence::default();
        evidence.downstream_unavailable("local_qap_initialization_unavailable");
        let public = evidence.for_table("table", None);
        assert_eq!(public["downstream_status"], "unavailable");
        assert!(public["rows"]["delta_committed"].is_null());
        assert_eq!(public["reconciliation"]["status"], "unavailable");
        assert_eq!(public["reconciliation"]["reason"], "cumulative_delta_rows_unavailable");
        assert!(public["compression_ratio"].is_null());
    }

    #[test]
    fn window_payload_and_sampled_core_rates_preserve_evidence_boundaries() -> Result<()> {
        let mut evidence = CellEvidence {
            measurement_window: Some((1000, 3000)),
            ..Default::default()
        };
        evidence.client_reports.insert("table".to_owned(), vec![json!({
            "phase_counters": {"measurement_window": {
                "start_unix_ms": 1000, "end_unix_ms": 3000, "completed_rows": 10, "completed_bytes": 200
            }}
        })]);
        evidence.durable_commits.insert("table".to_owned(), json!({
            "output_throughput": {"rows_per_second": 8.0, "input_bytes_per_second": 100.0, "parquet_bytes_per_second": 50.0}
        }));
        evidence.resource_samples.push(ResourceSample {
            sampled_at_unix_ms: 2000,
            metrics: ResourceMetrics { cpu_millicores: 2000.0, memory_bytes: 100 },
        });
        let client = ClientMetrics { rows: 10, duration_ms: 2000, rows_per_second: 5.0, completions: Vec::new() };
        let result = evidence.for_table("table", Some(&client));
        assert_eq!(result["rate_evidence"]["completed_flight_payload_bytes_per_second"], 100.0);
        assert_eq!(result["rate_evidence"]["committed_rows_per_second_per_observed_collector_core"], 4.0);
        assert_eq!(result["rate_evidence"]["collector_input_bytes_per_second_per_observed_collector_core"], 50.0);
        assert_eq!(result["rate_evidence"]["committed_parquet_bytes_per_second_per_observed_collector_core"], 25.0);
        assert_eq!(result["rate_evidence"]["completed_flight_payload_bytes_per_second_per_observed_collector_core"], 50.0);
        evidence
            .client_reports
            .get_mut("table")
            .context("measurement client report missing")?[0]["phase_counters"]["measurement_window"]["end_unix_ms"] = json!(4000);
        assert!(evidence.for_table("table", Some(&client))["rate_evidence"]["completed_flight_payload_bytes_per_second"].is_null());
        assert!(evidence.for_table("table", Some(&client))["rate_evidence"]["completed_flight_payload_bytes_per_second_per_observed_collector_core"].is_null());
        evidence.resource_samples.clear();
        assert!(evidence.for_table("table", Some(&client))["rate_evidence"]["committed_rows_per_second_per_observed_collector_core"].is_null());
        assert!(evidence.for_table("table", Some(&client))["rate_evidence"]["collector_input_bytes_per_second_per_observed_collector_core"].is_null());
        Ok(())
    }


    #[test]
    fn legacy_client_snapshots_default_retry_counters_to_zero() -> Result<()> {
        let worker = json!({
            "worker": 0, "attempted_rows": 5, "attempted_bytes": 25,
            "completed_rows": 5, "completed_bytes": 25, "error_rows": 0,
            "error_bytes": 0, "cancelled_rows": 0, "cancelled_bytes": 0,
            "best_effort_streams": 0, "durable_streams": 5, "send_latency_us": null,
        });
        let start = json!({
            "version": 1, "phase": "measurement_start", "scope": "process_cumulative",
            "elapsed_seconds": 0.0, "workers": [worker.clone()],
        });
        let end = json!({
            "version": 1, "phase": "completed", "scope": "process_cumulative",
            "elapsed_seconds": 1.0, "workers": [worker],
            "measurement_window": {
                "start_unix_ms": 10, "end_unix_ms": 20, "completed_rows": 5,
                "completed_bytes": 25, "worker_completed_rows": [5], "pilot_window_rows": [],
                "scope": "per_stream_admission_completion_observed_in_half_open_shared_unix_window",
            },
        });
        let snapshots = [start, end]
            .into_iter()
            .map(serde_json::from_value::<ClientCounterSnapshot>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let output = client_snapshot_delta(&snapshots)?;
        assert_eq!(output["workers"][0]["rejected_rows"], 0);
        assert_eq!(output["workers"][0]["retried_streams"], 0);
        assert_eq!(output["measurement_window"]["ambiguous_rows"], 0);
        assert_eq!(output["process_cumulative"]["ambiguous_rows"], 0);
        Ok(())
    }

    #[test]
    fn reconciliation_uses_cumulative_rows_and_ambiguity_bound() {
        let mut evidence = CellEvidence::default();
        evidence.client_reports.insert("table".to_owned(), vec![json!({
            "phase_counters": {"process_cumulative": {"completed_rows": 10, "ambiguous_rows": 2}}
        })]);
        evidence.durable_commits.insert("table".to_owned(), json!({
            "run_cumulative_output": {"rows": 12, "settled": true, "settle_seconds": 1.0, "settle_timeout_seconds": 120}
        }));
        let reconciliation = evidence.for_table("table", None)["reconciliation"].clone();
        assert_eq!(reconciliation["status"], "within_bound");
        assert_eq!(reconciliation["minimum_delta_rows"], 10);
        assert_eq!(reconciliation["maximum_delta_rows"], 12);
        assert_eq!(reconciliation["exact_match_required"], false);

        evidence.client_reports.insert("table".to_owned(), vec![json!({
            "phase_counters": {"process_cumulative": {"completed_rows": 10, "ambiguous_rows": 0}}
        })]);
        evidence.durable_commits.insert("table".to_owned(), json!({
            "run_cumulative_output": {"rows": 10, "settled": true, "settle_seconds": 1.0, "settle_timeout_seconds": 120}
        }));
        let reconciliation = evidence.for_table("table", None)["reconciliation"].clone();
        assert_eq!(reconciliation["status"], "within_bound");
        assert_eq!(reconciliation["exact_match_required"], true);
        assert_eq!(reconciliation["exact_match"], true);

        evidence.client_reports.insert("table".to_owned(), vec![json!({
            "phase_counters": {"process_cumulative": {"completed_rows": 10, "ambiguous_rows": 2}}
        })]);
        evidence.durable_commits.insert("table".to_owned(), json!({
            "run_cumulative_output": {"rows": 8, "settled": false, "settle_seconds": 120.0, "settle_timeout_seconds": 120}
        }));
        let reconciliation = evidence.for_table("table", None)["reconciliation"].clone();
        assert_eq!(reconciliation["status"], "unsettled");
        assert_eq!(reconciliation["gap_rows"], 2);
        assert_eq!(reconciliation["settle_seconds"], 120.0);

        evidence.durable_commits.insert("table".to_owned(), json!({
            "run_cumulative_output": {"rows": 8, "settled": true, "settle_seconds": 120.0, "settle_timeout_seconds": 120}
        }));
        let reconciliation = evidence.for_table("table", None)["reconciliation"].clone();
        assert_eq!(reconciliation["status"], "outside_bound");
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
    #[serde(default)]
    rejected_streams: u64,
    #[serde(default)]
    rejected_rows: u64,
    #[serde(default)]
    retried_streams: u64,
    #[serde(default)]
    retried_rows: u64,
    #[serde(default)]
    given_up_streams: u64,
    #[serde(default)]
    given_up_rows: u64,
    #[serde(default)]
    ambiguous_streams: u64,
    #[serde(default)]
    ambiguous_rows: u64,
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
    #[serde(default)]
    rejected_streams: u64,
    #[serde(default)]
    rejected_rows: u64,
    #[serde(default)]
    retried_streams: u64,
    #[serde(default)]
    retried_rows: u64,
    #[serde(default)]
    given_up_streams: u64,
    #[serde(default)]
    given_up_rows: u64,
    #[serde(default)]
    ambiguous_streams: u64,
    #[serde(default)]
    ambiguous_rows: u64,
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
        InitBindingRejected,
        ObservationBindingRejected,
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
        async fn ensure_stack(&mut self, _: &OatConfig, _: &str) -> Result<()> {
            Ok(())
        }
        async fn scale_arrowcols(&mut self, _: &OatConfig, _: u32, _: u32) -> Result<()> {
            Ok(())
        }
        async fn initialize_run_table_pins(&mut self, _: &OatConfig, _: &str, _: &str, _: u64, _: u64) -> Result<()> {
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
                Fault::InitUnavailable | Fault::InitBindingRejected => {
                    let results = std::env::temp_dir().join(format!("oat-qap-init-{}", uuid::Uuid::new_v4()));
                    fs::create_dir(&results)?;
                    let outcome = arrowcol_loadgen::initialize_local_qap(
                        &self.endpoint,
                        &results,
                        "loadtest",
                        &[(
                            "observed".to_owned(),
                            "/delta/dataflow/telemetry/sqlserver/sqlserver_active_sessions".to_owned(),
                        )],
                        Duration::from_secs(5),
                    )
                    .await;
                    fs::remove_dir_all(&results)?;
                    outcome.map_err(classify_local_qap_error)?;
                    Ok(vec!["observed".to_owned()])
                }
                Fault::ObservationUnavailable | Fault::ObservationBindingRejected => Ok(vec!["observed".to_owned()]),
                _ => Err(anyhow::anyhow!(LocalDownstreamUnavailable)),
            }
        }
        fn mark_qap_outcome_unresolved(&mut self) {
            self.unresolved = true;
        }
        async fn prepare_client_fleet(&mut self, _: &OatConfig, _: &str) -> Result<()> {
            Ok(())
        }
        async fn start_client(
            &mut self,
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
        async fn arm_clients(&mut self, _: &OatConfig, _: &str, start: u64) -> Result<()> {
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
        async fn process_logs(&self, _: &OatConfig) -> Result<Vec<ProcessLog>> {
            Ok(vec![ProcessLog {
                role: ComponentRole::Router,
                instance_id: "router-fault-test".to_owned(),
                image: None,
                image_id: None,
                source_revision: None,
                complete: true,
                raw_log_bytes: 0,
                raw_log_lines: 1,
                stdout: String::new(),
                stderr: String::new(),
                text: "arrowcol_router_effective_config {\"log_filter\":\"warn,tiberius=error\"}\n".to_owned(),
            }])
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
        let metric = format!("{metric}arrowcol_router_sql_connections_opened_total 0\n");
        let downstream_status = if matches!(fault, Fault::InitBindingRejected | Fault::ObservationBindingRejected) {
            axum::http::StatusCode::BAD_REQUEST
        } else {
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        };
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new()
                    .route("/metrics", axum::routing::get(move || async move { metric }))
                    .fallback(move || async move { downstream_status }),
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
        config.qap_timeout_seconds = if matches!(fault, Fault::InitTimeout) { 1 } else { 10 };
        let run_id = uuid::Uuid::new_v4().to_string();
        let result = run_matrix_cell(
            &config,
            "orchestration",
            &run_id,
            1,
            1,
            &mut platform,
        )
        .await;
        server.abort();
        let results = Path::new("out/arrowcol-oat/qap").join(&run_id);
        if results.exists() {
            fs::remove_dir_all(&results)?;
        }
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
            assert_eq!(rows[0].status, "succeeded", "failure: {:?}", rows[0].failure);
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
    async fn concurrent_downstream_fault_runs_keep_qap_artifacts_isolated() -> Result<()> {
        let (unavailable, rejected) = tokio::join!(
            run_fault(Fault::ObservationUnavailable, crate::config::OatMode::Local),
            run_fault(Fault::ObservationBindingRejected, crate::config::OatMode::Local),
        );
        let (unavailable, _) = unavailable?;
        let (rejected, _) = rejected?;
        assert_ne!(unavailable[0].run_id, rejected[0].run_id);
        assert_eq!(unavailable[0].status, "succeeded", "{:?}", unavailable[0].failure);
        assert_eq!(rejected[0].status, "failed");
        Ok(())
    }

    #[tokio::test]
    async fn local_degradation_never_hides_required_setup_collector_or_pinning_failures() -> Result<()> {
        for fault in [
            Fault::InitRequired,
            Fault::InitBindingRejected,
            Fault::ObservationBindingRejected,
            Fault::Collector,
            Fault::Pinning,
        ] {
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
    if let Some(window) = &end.measurement_window
        && (window.scope != "per_stream_admission_completion_observed_in_half_open_shared_unix_window"
            || window.start_unix_ms >= window.end_unix_ms
            || window.worker_completed_rows.len() != end.workers.len()
            || window
                .worker_completed_rows
                .iter()
                .try_fold(0_u64, |sum, rows| sum.checked_add(*rows))
                != Some(window.completed_rows))
    {
        bail!("invalid exact client measurement-window accounting");
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
        if let Some(latency) = &worker.send_latency_us
            && (latency.precision != "power_of_two_upper_bound"
                || [latency.p50, latency.p95, latency.p99]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .windows(2)
                    .any(|pair| pair[0] > pair[1]))
        {
            bail!("invalid client latency evidence");
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
            "rejected_streams": difference(worker.rejected_streams, before.rejected_streams)?,
            "rejected_rows": difference(worker.rejected_rows, before.rejected_rows)?,
            "retried_streams": difference(worker.retried_streams, before.retried_streams)?,
            "retried_rows": difference(worker.retried_rows, before.retried_rows)?,
            "given_up_streams": difference(worker.given_up_streams, before.given_up_streams)?,
            "given_up_rows": difference(worker.given_up_rows, before.given_up_rows)?,
            "ambiguous_streams": difference(worker.ambiguous_streams, before.ambiguous_streams)?,
            "ambiguous_rows": difference(worker.ambiguous_rows, before.ambiguous_rows)?,
            "process_cumulative_send_latency_us": worker.send_latency_us,
        }));
    }
    let cumulative_total = |field: fn(&WorkerCounterSnapshot) -> u64| -> Result<u64> {
        end.workers
            .iter()
            .try_fold(0_u64, |total, worker| total.checked_add(field(worker)))
            .context("client cumulative counter overflow")
    };
    Ok(json!({
        "scope": "measurement_start_to_completed_snapshot_delta_not_unique_delivery",
        "process_cumulative": {
            "completed_rows": cumulative_total(|worker| worker.completed_rows)?,
            "ambiguous_rows": cumulative_total(|worker| worker.ambiguous_rows)?,
        },
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
fn resource_summary_is_observed_mean_and_max_not_an_end_snapshot() -> Result<()> {
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
    let metrics = evidence
        .resource_summary()
        .context("missing observed resource summary")?;
    assert_eq!(metrics.cpu_millicores, 200.0);
    assert_eq!(metrics.memory_bytes, 200);
    Ok(())
}

#[test]
fn cell_evidence_includes_router_cpu_pressure() {
    let mut evidence = CellEvidence::default();
    let cpu_pressure = json!({"status": "observed", "max_utilization_bps": 731});
    evidence.router_cpu_pressure = Some(cpu_pressure.clone());

    let cell = evidence.for_table("table", None);

    assert_eq!(cell["router_cpu_pressure"], cpu_pressure);
}
