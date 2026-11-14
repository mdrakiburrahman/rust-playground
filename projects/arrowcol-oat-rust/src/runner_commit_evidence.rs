#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq)]
struct CommitEvent {
    schema_version: u32,
    outcome: String,
    table_id: Option<String>,
    table_name: Option<String>,
    delta_version: Option<u64>,
    rows: Option<u64>,
    files: Option<u64>,
    input_bytes: Option<u64>,
    parquet_bytes: Option<u64>,
    min_ingested_at_ms: Option<i64>,
    max_ingested_at_ms: Option<i64>,
    finished_at_unix_ms: Option<i64>,
    elapsed_seconds: Option<f64>,
}

fn unavailable_commits(reason: &str) -> Value {
    json!({
        "scope": "collector_commit_events",
        "rows": null, "files": null, "input_bytes": null, "parquet_bytes": null,
        "bytes_per_row": null, "compression_ratio": null, "commit_latency_seconds": null,
        "complete_delivery_established": false, "unique_delivered_rows": null,
        "unavailable_reason": reason,
    })
}

fn percentile(values: &[f64], percentile: usize) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let index = (values.len() * percentile).div_ceil(100).saturating_sub(1);
    values.get(index).copied()
}

fn canonical_dmv_table_name(table: &str) -> Result<String> {
    let definitions = collector_chain::dmv::tables()?;
    let definition = definitions
        .iter()
        .find(|definition| definition.identity().table().as_str() == table)
        .context("commit evidence table is not a canonical DMV table")?;
    let identity = definition.identity();
    Ok(format!(
        "{}.{}.{}",
        identity.catalog().as_str(),
        identity.schema().as_str(),
        identity.table().as_str()
    ))
}

fn commit_window_output(
    events: &std::collections::BTreeMap<(String, u64), CommitEvent>,
    table: &str,
    start: u64,
    end: u64,
    unavailable: Option<&str>,
) -> Result<Value> {
    let seconds = end
        .checked_sub(start)
        .filter(|duration| *duration > 0)
        .context("invalid shared commit measurement window")? as f64
        / 1000.0;
    let mut result = json!({
        "scope": "new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort",
        "measurement_start_unix_ms": start, "measurement_end_unix_ms": end, "window_seconds": seconds,
        "clock": "collector_system_unix_ms_shared_schedule_clock_skew_not_measured",
        "rows": null, "files": null, "input_bytes": null, "parquet_bytes": null, "rows_per_second": null,
        "bytes_per_row": null, "compression_ratio": null, "commit_latency_seconds": null,
        "unique_delivered_rows": null, "complete_delivery_established": false,
        "warmup_backlog": "included_when_commit_finishes_in_window",
        "rewrite_commits": "excluded_by_ingestion_only_producer_capability",
    });
    if let Some(reason) = unavailable {
        result["unavailable_reason"] = json!(reason);
        return Ok(result);
    }
    let mut rows = 0_u64;
    let mut files = 0_u64;
    let mut input_bytes = 0_u64;
    let mut parquet_bytes = 0_u64;
    let mut latencies = Vec::new();
    for event in events
        .values()
        .filter(|event| event.table_name.as_deref() == Some(table))
    {
        let Some(finished) = event.finished_at_unix_ms.and_then(|time| u64::try_from(time).ok()) else {
            result["unavailable_reason"] = json!("commit_finish_time_unavailable");
            return Ok(result);
        };
        if finished < start || finished >= end {
            continue;
        }
        let (Some(event_rows), Some(event_files), Some(event_input), Some(event_bytes), Some(elapsed)) = (
            event.rows,
            event.files,
            event.input_bytes,
            event.parquet_bytes,
            event.elapsed_seconds,
        ) else {
            bail!("commit-window event omitted row/file/byte/timing evidence");
        };
        if event_rows == 0 || event_files == 0 || event_bytes == 0 || !elapsed.is_finite() || elapsed < 0.0 {
            bail!("invalid commit-window row/file/byte/timing evidence");
        }
        rows = rows.checked_add(event_rows).context("commit output row overflow")?;
        files = files.checked_add(event_files).context("commit output file overflow")?;
        input_bytes = input_bytes
            .checked_add(event_input)
            .context("commit output input-byte overflow")?;
        parquet_bytes = parquet_bytes
            .checked_add(event_bytes)
            .context("commit output byte overflow")?;
        latencies.push(elapsed);
    }
    latencies.sort_by(f64::total_cmp);
    result["rows"] = json!(rows);
    result["files"] = json!(files);
    result["input_bytes"] = json!(input_bytes);
    result["parquet_bytes"] = json!(parquet_bytes);
    result["rows_per_second"] = json!(rows as f64 / seconds);
    result["commits"] = json!(latencies.len());
    result["bytes_per_row"] = json!((rows > 0).then(|| parquet_bytes as f64 / rows as f64));
    result["compression_ratio"] = json!((parquet_bytes > 0).then(|| input_bytes as f64 / parquet_bytes as f64));
    result["compression_ratio_numerator"] = json!("collector_reported_input_bytes");
    result["compression_ratio_denominator"] = json!("committed_parquet_file_bytes_including_metadata");
    result["commit_latency_seconds"] = json!({
        "p50": percentile(&latencies, 50), "p95": percentile(&latencies, 95), "p99": percentile(&latencies, 99),
        "samples": latencies.len(), "scope": "commit_retry_and_sync_not_encoding_or_admission",
    });
    Ok(result)
}

fn durable_commit_evidence(
    config: &OatConfig,
    logs: &[ProcessLog],
    window: Option<(u64, u64)>,
) -> Result<HashMap<String, Value>> {
    let Some((start, end)) = window else {
        return Ok(config
            .tables
            .iter()
            .map(|table| (table.clone(), unavailable_commits("measurement_window_unavailable")))
            .collect());
    };
    let collector_logs = logs
        .iter()
        .filter(|log| matches!(log.role, ComponentRole::Collector))
        .collect::<Vec<_>>();
    let mut unavailable = collector_logs.is_empty().then_some("collector_commit_log_unavailable");
    if collector_logs.iter().any(|log| !log.complete) {
        unavailable = Some("collector_commit_log_incomplete");
    }
    let mut capability_observed = !collector_logs.is_empty();
    for log in &collector_logs {
        let readback = process_configurations(&log.text)?;
        capability_observed &= readback["commit_telemetry"]
            == json!({
                "schema_version":1,"scope":"new_ingestion_addfiles_after_sync","clock":"system_unix_ms",
            });
    }
    let mut events = std::collections::BTreeMap::new();
    for log in collector_logs {
        for line in log
            .text
            .lines()
            .filter_map(|line| line.strip_prefix("arrowcol_data_commit "))
        {
            let event: CommitEvent = serde_json::from_str(line).context("invalid collector commit event")?;
            if event.schema_version != 1 {
                bail!("unsupported collector commit event version");
            }
            if event.outcome != "committed" {
                unavailable = Some("uncertain_commit_version_attribution");
                continue;
            }
            let table_id = event
                .table_id
                .as_deref()
                .filter(|id| id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
                .context("committed event omitted canonical table hash")?;
            let version = event.delta_version.context("committed event omitted Delta version")?;
            let key = (table_id.to_owned(), version);
            if let Some(previous) = events.insert(key, event.clone())
                && previous != event
            {
                bail!("conflicting evidence for the same Delta commit identity");
            }
        }
    }
    if events.values().any(|event| event.table_name.is_none()) {
        unavailable = Some("commit_event_has_no_verified_table_identity");
    }
    let mut result = HashMap::new();
    for table in &config.tables {
        let canonical_table_name = canonical_dmv_table_name(table)?;
        if let Some(reason) = unavailable {
            result.insert(table.clone(), unavailable_commits(reason));
            continue;
        }
        let mut selected = Vec::new();
        let mut reason = None;
        for event in events
            .values()
            .filter(|event| event.table_name.as_deref() == Some(canonical_table_name.as_str()))
        {
            let Some((first, last)) = event.min_ingested_at_ms.zip(event.max_ingested_at_ms) else {
                reason = Some("commit_ingress_boundary_unavailable");
                break;
            };
            if first < 0 || last < first {
                bail!("invalid commit ingress time bounds");
            }
            let first = first as u64;
            let last = last as u64;
            if last < start || first >= end {
                continue;
            }
            if first < start || last >= end {
                reason = Some("commit_overlaps_preflight_warmup_or_measurement_boundary");
                break;
            }
            if event.finished_at_unix_ms.is_none_or(|finished| finished < last as i64)
                || event
                    .elapsed_seconds
                    .is_none_or(|elapsed| !elapsed.is_finite() || elapsed < 0.0)
                || event.rows.is_none_or(|rows| rows == 0)
                || event.files.is_none_or(|files| files == 0)
                || event.parquet_bytes.is_none_or(|bytes| bytes == 0)
                || event.input_bytes.is_none()
            {
                bail!("committed event omitted valid file/row/byte/timing evidence");
            }
            selected.push(event);
        }
        if let Some(reason) = reason {
            result.insert(table.clone(), unavailable_commits(reason));
        } else if selected.is_empty() {
            result.insert(
                table.clone(),
                unavailable_commits("no_provable_window_scoped_commits_observed"),
            );
        } else {
            let sum = |field: fn(&CommitEvent) -> Option<u64>| -> Result<u64> {
                selected
                    .iter()
                    .try_fold(0_u64, |sum, event| sum.checked_add(field(event)?))
                    .context("commit evidence counter overflow")
            };
            let rows = sum(|event| event.rows)?;
            let files = sum(|event| event.files)?;
            let input_bytes = sum(|event| event.input_bytes)?;
            let parquet_bytes = sum(|event| event.parquet_bytes)?;
            let mut latencies = selected
                .iter()
                .filter_map(|event| event.elapsed_seconds)
                .collect::<Vec<_>>();
            latencies.sort_by(f64::total_cmp);
            result.insert(table.clone(), json!({
                "scope": "observed_new_delta_commits_with_ingress_wholly_in_shared_measurement_window",
                "measurement_start_unix_ms": start, "measurement_end_unix_ms": end,
                "rows": rows, "files": files, "input_bytes": input_bytes, "parquet_bytes": parquet_bytes,
                "bytes_per_row": parquet_bytes as f64 / rows as f64,
                "compression_ratio": input_bytes as f64 / parquet_bytes as f64,
                "compression_ratio_numerator": "collector_reported_input_bytes",
                "compression_ratio_denominator": "committed_parquet_file_bytes_including_metadata",
                "commit_latency_seconds": {
                    "p50": percentile(&latencies, 50), "p95": percentile(&latencies, 95), "p99": percentile(&latencies, 99),
                    "samples": latencies.len(), "scope": "commit_retry_and_sync_not_encoding_or_admission",
                },
                "commits": selected.len(),
                "complete_delivery_established": false,
                "unique_delivered_rows": null,
                "delivery_uncertainty": "events_observed_before_readback_only_no_drain_or_retry_deduplication_proof",
            }));
        }
    }
    for (table, evidence) in &mut result {
        evidence["output_throughput"] = commit_window_output(
            &events,
            &canonical_dmv_table_name(table)?,
            start,
            end,
            unavailable.or((!capability_observed).then_some("ingestion_commit_producer_capability_unavailable")),
        )?;
    }
    Ok(result)
}

#[cfg(test)]
mod commit_evidence_tests {
    use super::*;

    fn config() -> Result<OatConfig> {
        Ok(serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?)
    }

    fn event(config: &OatConfig, min: i64, max: i64) -> Result<Value> {
        Ok(
            json!({"schema_version":1,"outcome":"committed","table_id":"a".repeat(64),"table_name":canonical_dmv_table_name(&config.tables[0])?,
            "delta_version":7,"rows":10,"files":1,"input_bytes":100,"parquet_bytes":50,
            "min_ingested_at_ms":min,"max_ingested_at_ms":max,"finished_at_unix_ms":max+1,"elapsed_seconds":0.5}),
        )
    }

    fn log(events: &[Value]) -> ProcessLog {
        ProcessLog {
            role: ComponentRole::Collector,
            image: None,
            image_id: None,
            source_revision: None,
            complete: true,
            text: format!(
                "arrowcol_effective_config {{\"commit_telemetry\":{{\"schema_version\":1,\"scope\":\"new_ingestion_addfiles_after_sync\",\"clock\":\"system_unix_ms\"}}}}\n{}",
                events
                    .iter()
                    .map(|event| format!("arrowcol_data_commit {event}\n"))
                    .collect::<String>()
            ),
        }
    }

    #[test]
    fn commit_identity_deduplicates_replay_without_claiming_unique_delivery() -> Result<()> {
        let config = config()?;
        let commit = event(&config, 100, 199)?;
        let evidence = durable_commit_evidence(&config, &[log(&[commit.clone(), commit])], Some((100, 200)))?;
        let evidence = &evidence[&config.tables[0]];
        assert_eq!(evidence["rows"], 10);
        assert_eq!(evidence["compression_ratio"], 2.0);
        assert!(evidence["unique_delivered_rows"].is_null());
        assert_eq!(evidence["complete_delivery_established"], false);
        Ok(())
    }

    #[test]
    fn mixed_windows_unknown_outcomes_and_conflicting_replays_are_not_counted() -> Result<()> {
        let config = config()?;
        for events in [
            vec![event(&config, 99, 101)?],
            vec![json!({"schema_version":1,"outcome":"reconciled"})],
        ] {
            let evidence = durable_commit_evidence(&config, &[log(&events)], Some((100, 200)))?;
            assert!(evidence[&config.tables[0]]["rows"].is_null());
        }
        let first = event(&config, 100, 101)?;
        let mut conflicting = first.clone();
        conflicting["rows"] = json!(11);
        assert!(durable_commit_evidence(&config, &[log(&[first, conflicting])], Some((100, 200))).is_err());
        Ok(())
    }

    #[test]
    fn output_window_includes_boundary_crossing_backlog_but_not_late_or_rewrite_commits() -> Result<()> {
        let config = config()?;
        let crossing = event(&config, 99, 101)?;
        let mut late = event(&config, 110, 199)?;
        late["delta_version"] = json!(8);
        let mut at_start = event(&config, 98, 99)?;
        at_start["delta_version"] = json!(9);
        let mut before_start = event(&config, 90, 98)?;
        before_start["delta_version"] = json!(10);
        let mut rewrite = log(&[event(&config, 110, 150)?]);
        rewrite.role = ComponentRole::Maintenance;
        let logs = [
            log(&[crossing.clone(), crossing, late, at_start, before_start]),
            rewrite,
        ];
        let evidence = durable_commit_evidence(&config, &logs, Some((100, 200)))?;
        let table = &evidence[&config.tables[0]];
        assert!(table["rows"].is_null());
        assert_eq!(table["output_throughput"]["rows"], 20);
        assert_eq!(table["output_throughput"]["rows_per_second"], 200.0);
        assert!(table["output_throughput"]["unique_delivered_rows"].is_null());
        let zero = durable_commit_evidence(&config, &[log(&[])], Some((100, 200)))?;
        assert_eq!(zero[&config.tables[0]]["output_throughput"]["rows"], 0);
        let mut absent = log(&[]);
        absent.text.clear();
        let absent = durable_commit_evidence(&config, &[absent], Some((100, 200)))?;
        assert!(absent[&config.tables[0]]["output_throughput"]["rows"].is_null());
        Ok(())
    }
}
