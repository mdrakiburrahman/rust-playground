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
        "run_cumulative_output": {
            "scope": "all_new_ingestion_commits_from_this_cell_including_warmup",
            "rows": null, "unavailable_reason": reason,
        },
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
    cell_start_unix_ms: Option<u64>,
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
        "input_bytes_per_second": null, "parquet_bytes_per_second": null,
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
        if let Some(cell_start) = cell_start_unix_ms {
            let (Some(first), Some(last)) = (event.min_ingested_at_ms, event.max_ingested_at_ms) else {
                result["unavailable_reason"] = json!("commit_ingress_boundary_unavailable");
                return Ok(result);
            };
            if last < cell_start as i64 {
                continue;
            }
            if first < cell_start as i64 || last < first {
                result["unavailable_reason"] = json!("commit_crosses_cell_ingress_boundary");
                return Ok(result);
            }
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
    result["input_bytes_per_second"] = json!(input_bytes as f64 / seconds);
    result["parquet_bytes_per_second"] = json!(parquet_bytes as f64 / seconds);
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

fn unavailable_cumulative_output(reason: &str) -> Value {
    json!({
        "scope": "all_new_ingestion_commits_from_this_cell_including_warmup",
        "rows": null,
        "commits_by_version": [],
        "unavailable_reason": reason,
    })
}

fn run_cumulative_output(
    events: &std::collections::BTreeMap<(String, u64), CommitEvent>,
    table: &str,
    unavailable: Option<&str>,
    cell_start_unix_ms: Option<u64>,
) -> Result<Value> {
    if let Some(reason) = unavailable {
        return Ok(unavailable_cumulative_output(reason));
    }
    let mut rows = 0_u64;
    let mut commits = 0_u64;
    let mut commits_by_version = Vec::new();
    for event in events
        .values()
        .filter(|event| event.table_name.as_deref() == Some(table))
    {
        let (
            Some(table_id),
            Some(table_name),
            Some(event_rows),
            Some(event_files),
            Some(event_input_bytes),
            Some(event_parquet_bytes),
            Some(min_ingested_at_ms),
            Some(max_ingested_at_ms),
            Some(elapsed_seconds),
        ) = (
            event.table_id.as_deref(),
            event.table_name.as_deref(),
            event.rows,
            event.files,
            event.input_bytes,
            event.parquet_bytes,
            event.min_ingested_at_ms,
            event.max_ingested_at_ms,
            event.elapsed_seconds,
        ) else {
            return Ok(unavailable_cumulative_output("cumulative_commit_metadata_unavailable"));
        };
        let Some(finished_at_unix_ms) = event.finished_at_unix_ms else {
            return Ok(unavailable_cumulative_output("cumulative_commit_finish_time_unavailable"));
        };
        if max_ingested_at_ms < min_ingested_at_ms || finished_at_unix_ms < 0
            || !elapsed_seconds.is_finite() || elapsed_seconds < 0.0
        {
            return Ok(unavailable_cumulative_output("cumulative_commit_metadata_invalid"));
        }
        if let Some(cell_start) = cell_start_unix_ms {
            if max_ingested_at_ms < cell_start as i64 {
                continue;
            }
            if min_ingested_at_ms < cell_start as i64 {
                return Ok(unavailable_cumulative_output("commit_crosses_cell_ingress_boundary"));
            }
        }
        let Some(delta_version) = event.delta_version else {
            return Ok(unavailable_cumulative_output("cumulative_commit_version_unavailable"));
        };
        rows = rows.checked_add(event_rows).context("cumulative commit row overflow")?;
        commits = commits.checked_add(1).context("cumulative commit count overflow")?;
        commits_by_version.push(json!({
            "table_id": table_id,
            "table_name": table_name,
            "delta_version": delta_version,
            "rows": event_rows,
            "files": event_files,
            "input_bytes": event_input_bytes,
            "parquet_bytes": event_parquet_bytes,
            "min_ingested_at_ms": min_ingested_at_ms,
            "max_ingested_at_ms": max_ingested_at_ms,
            "finished_at_unix_ms": finished_at_unix_ms,
            "elapsed_seconds": elapsed_seconds,
        }));
    }
    Ok(json!({
        "scope": "all_new_ingestion_commits_from_this_cell_including_warmup",
        "rows": rows,
        "commits": commits,
        "complete_delivery_established": false,
        "commits_by_version": commits_by_version,
    }))
}

fn complete_commit_windows(
    events: &std::collections::BTreeMap<(String, u64), CommitEvent>,
    table: &str,
    start: u64,
    end: u64,
    unavailable: Option<&str>,
    cell_start_unix_ms: Option<u64>,
) -> Result<Vec<Value>> {
    let mut windows = Vec::new();
    let mut window_start = start;
    while let Some(window_end) = window_start.checked_add(30_000).filter(|window_end| *window_end <= end) {
        windows.push(commit_window_output(
            events, table, window_start, window_end, unavailable, cell_start_unix_ms,
        )?);
        window_start = window_end;
    }
    Ok(windows)
}

#[cfg(test)]
fn durable_commit_evidence(
    config: &OatConfig,
    logs: &[ProcessLog],
    window: Option<(u64, u64)>,
) -> Result<HashMap<String, Value>> {
    durable_commit_evidence_for_cell(config, logs, logs, window, None)
}

fn cell_scoped_commit_logs(current: &[ProcessLog], baseline: &[ProcessLog]) -> Result<(Vec<ProcessLog>, bool)> {
    let mut baseline_by_id = HashMap::new();
    for log in baseline
        .iter()
        .filter(|log| matches!(log.role, ComponentRole::Collector))
    {
        if baseline_by_id.insert(log.instance_id.as_str(), log).is_some() {
            bail!("collector process identity is duplicated in the cell baseline");
        }
    }
    let mut current_ids = HashSet::new();
    let mut scoped = Vec::new();
    let mut used_log_offsets = true;
    for log in current
        .iter()
        .filter(|log| matches!(log.role, ComponentRole::Collector))
    {
        if !current_ids.insert(log.instance_id.as_str()) {
            bail!("collector process identity is duplicated in the current snapshot");
        }
        let prior = baseline_by_id.get(log.instance_id.as_str());
        let (text, complete) = match prior {
            Some(prior) if prior.complete && log.complete => match log.text.strip_prefix(&prior.text) {
                Some(suffix) => (suffix.to_owned(), true),
                None => {
                    used_log_offsets = false;
                    (log.text.clone(), log.complete)
                }
            },
            Some(_) => {
                used_log_offsets = false;
                (log.text.clone(), log.complete)
            }
            None => {
                used_log_offsets = false;
                (log.text.clone(), log.complete)
            }
        };
        scoped.push(ProcessLog {
            role: log.role,
            instance_id: log.instance_id.clone(),
            image: log.image.clone(),
            image_id: log.image_id.clone(),
            source_revision: log.source_revision.clone(),
            complete,
            raw_log_bytes: log.raw_log_bytes,
            raw_log_lines: log.raw_log_lines,
            stdout: log.stdout.clone(),
            stderr: log.stderr.clone(),
            text,
        });
    }
    Ok((scoped, used_log_offsets))
}

fn durable_commit_evidence_for_cell(
    config: &OatConfig,
    process_logs: &[ProcessLog],
    commit_logs: &[ProcessLog],
    window: Option<(u64, u64)>,
    cell_start_unix_ms: Option<u64>,
) -> Result<HashMap<String, Value>> {
    let Some((start, end)) = window else {
        return Ok(config
            .tables
            .iter()
            .map(|table| (table.clone(), unavailable_commits("measurement_window_unavailable")))
            .collect());
    };
    let collector_logs = process_logs
        .iter()
        .filter(|log| matches!(log.role, ComponentRole::Collector))
        .collect::<Vec<_>>();
    let cell_commit_logs = commit_logs
        .iter()
        .filter(|log| matches!(log.role, ComponentRole::Collector))
        .collect::<Vec<_>>();
    let collector_logs_incomplete =
        collector_logs.iter().any(|log| !log.complete) || cell_commit_logs.iter().any(|log| !log.complete);
    let mut unavailable =
        (collector_logs.is_empty() || cell_commit_logs.is_empty()).then_some("collector_commit_log_unavailable");
    if collector_logs_incomplete {
        unavailable = Some("collector_commit_log_incomplete");
    }
    let mut capability_observed = !collector_logs.is_empty() && !collector_logs_incomplete;
    if !collector_logs_incomplete {
        for log in &collector_logs {
            let readback = process_configurations(&log.text)?;
            capability_observed &= readback["commit_telemetry"]
                == json!({
                    "schema_version":1,"scope":"new_ingestion_addfiles_after_sync","clock":"system_unix_ms",
                });
        }
    }
    let mut events = std::collections::BTreeMap::new();
    if !collector_logs_incomplete {
        for log in cell_commit_logs {
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
    }
    if events.values().any(|event| event.table_name.is_none()) {
        unavailable = Some("commit_event_has_no_verified_table_identity");
    }
    let mut result = HashMap::new();
    for table in &config.tables {
        let canonical_table_name = canonical_dmv_table_name(table)?;
        if let Some(reason) = unavailable {
            let mut evidence = unavailable_commits(reason);
            evidence["output_throughput"] = commit_window_output(
                &events, &canonical_table_name, start, end, Some(reason), cell_start_unix_ms,
            )?;
            evidence["complete_30_second_commit_windows"] = json!(complete_commit_windows(
                &events, &canonical_table_name, start, end, Some(reason), cell_start_unix_ms,
            )?);
            result.insert(table.clone(), evidence);
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
            if event.finished_at_unix_ms.is_none() {
                reason = Some("commit_finish_time_unavailable");
                break;
            }
            if event.finished_at_unix_ms.is_some_and(|finished| finished < last as i64) {
                bail!("commit finish time predates its ingress boundary");
            }
            if event.elapsed_seconds.is_none()
                || event.rows.is_none()
                || event.files.is_none()
                || event.parquet_bytes.is_none()
                || event.input_bytes.is_none()
            {
                reason = Some("committed_event_evidence_unavailable");
                break;
            }
            if event.elapsed_seconds.is_some_and(|elapsed| !elapsed.is_finite() || elapsed < 0.0)
                || event.rows.is_some_and(|rows| rows == 0)
                || event.files.is_some_and(|files| files == 0)
                || event.parquet_bytes.is_some_and(|bytes| bytes == 0)
            {
                bail!("invalid committed file/row/byte/timing evidence");
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
        let canonical_table_name = canonical_dmv_table_name(table)?;
        let cumulative_unavailable =
            unavailable.or((!capability_observed).then_some("ingestion_commit_producer_capability_unavailable"));
        evidence["output_throughput"] = commit_window_output(
            &events,
            &canonical_table_name,
            start,
            end,
            cumulative_unavailable,
            cell_start_unix_ms,
        )?;
        evidence["complete_30_second_commit_windows"] = json!(complete_commit_windows(
            &events, &canonical_table_name, start, end, cumulative_unavailable, cell_start_unix_ms,
        )?);
        evidence["run_cumulative_output"] = run_cumulative_output(
            &events,
            &canonical_table_name,
            cumulative_unavailable,
            cell_start_unix_ms,
        )?;
    }
    Ok(result)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DeltaLogAdd {
    rows: u64,
    bytes: u64,
}

fn parse_delta_log_adds(
    version: u64,
    contents: &[u8],
    adds: &mut BTreeMap<(u64, String), DeltaLogAdd>,
) -> Result<()> {
    for (line_number, line) in contents.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        let action: Value = serde_json::from_slice(line)
            .with_context(|| format!("invalid Delta log JSON at line {}", line_number + 1))?;
        let Some(add) = action.get("add") else {
            continue;
        };
        match add.get("dataChange") {
            None | Some(Value::Bool(true)) => {}
            Some(Value::Bool(false)) => continue,
            Some(_) => bail!("Delta add dataChange must be a boolean"),
        }
        let path = add
            .get("path")
            .and_then(Value::as_str)
            .filter(|path| !path.is_empty())
            .context("Delta add action omitted its path")?;
        let stats = match add.get("stats") {
            Some(Value::String(stats)) => serde_json::from_str::<Value>(stats)
                .context("Delta add action has invalid stats JSON")?,
            Some(Value::Object(_)) => add["stats"].clone(),
            _ => bail!("Delta add action omitted row statistics"),
        };
        let rows = stats
            .get("numRecords")
            .and_then(Value::as_u64)
            .context("Delta add statistics omitted numRecords")?;
        let bytes = add
            .get("size")
            .and_then(Value::as_u64)
            .context("Delta add action omitted file size")?;
        let identity = (version, path.to_owned());
        let record = DeltaLogAdd { rows, bytes };
        if let Some(previous) = adds.insert(identity, record)
            && previous != record
        {
            bail!("conflicting duplicate Delta add action");
        }
    }
    Ok(())
}

fn parse_delta_checkpoint_version(filename: &str) -> Result<Option<u64>> {
    let Some((version_text, suffix)) = filename.split_once(".checkpoint.") else {
        return Ok(None);
    };
    ensure!(
        version_text.len() == 20 && version_text.bytes().all(|byte| byte.is_ascii_digit()),
        "Delta checkpoint filename has a noncanonical version"
    );
    let valid_suffix = suffix == "parquet"
        || suffix.strip_suffix(".parquet").is_some_and(|parts| {
            let segments = parts.split('.').collect::<Vec<_>>();
            segments.len() == 2
                && segments.iter().all(|segment| {
                    segment.len() == 10 && segment.bytes().all(|byte| byte.is_ascii_digit())
                })
        });
    ensure!(valid_suffix, "Delta checkpoint filename is malformed");
    Ok(Some(version_text.parse::<u64>().context("Delta checkpoint version is invalid")?))
}

fn parse_last_checkpoint_version(contents: &[u8]) -> Result<u64> {
    let checkpoint: Value = serde_json::from_slice(contents).context("invalid Delta _last_checkpoint JSON")?;
    checkpoint
        .get("version")
        .and_then(Value::as_u64)
        .context("Delta _last_checkpoint omitted its version")
}

fn validate_delta_log_version_history(versions: &BTreeSet<u64>, checkpoint_tips: &BTreeSet<u64>) -> Result<()> {
    ensure!(!versions.is_empty(), "Delta log history contains no commit JSON files");
    ensure!(versions.first() == Some(&0), "Delta log history does not begin at run-scoped version zero");
    ensure!(
        versions.iter().enumerate().all(|(index, version)| u64::try_from(index).ok() == Some(*version)),
        "Delta log history has a missing commit version"
    );
    let latest = versions.last().copied().context("Delta log history has no latest version")?;
    ensure!(
        checkpoint_tips.iter().all(|tip| *tip <= latest),
        "Delta checkpoint tip is newer than verified JSON history"
    );
    Ok(())
}

async fn read_delta_log_commits(delta_root: &str, table: &str) -> Result<Value> {
    use futures_util::TryStreamExt;
    use object_store::ObjectStoreExt;

    ensure!(
        !table.is_empty() && table.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "Delta evidence table path is not a canonical DMV name"
    );
    let mut url = Url::parse(delta_root).context("parse run-scoped Delta root")?;
    ensure!(url.scheme() == "abfss" && url.query().is_none() && url.fragment().is_none(), "Delta evidence requires a plain run-scoped abfss URL");
    let log_path = format!("{}/telemetry/sqlserver/{table}/_delta_log", url.path().trim_end_matches('/'));
    url.set_path(&log_path);
    let prefix = ObjectPath::from_url_path(url.path())?;
    let store = object_store_for_url(&url)?;
    let mut stream = store.list(Some(&prefix));
    let mut adds = BTreeMap::new();
    let mut log_files = 0_usize;
    let mut versions_seen = BTreeSet::new();
    let mut checkpoint_tips = BTreeSet::new();
    while let Some(meta) = stream.try_next().await? {
        let Some(filename) = meta.location.filename() else {
            continue;
        };
        if filename == "_last_checkpoint" {
            ensure!(meta.size <= 64 * 1024, "Delta _last_checkpoint exceeds the 64 KiB evidence bound");
            let contents = store.get(&meta.location).await?.bytes().await?;
            ensure!(contents.len() <= 64 * 1024, "Delta _last_checkpoint exceeds the 64 KiB evidence bound");
            checkpoint_tips.insert(parse_last_checkpoint_version(&contents)?);
            continue;
        }
        if filename.contains(".checkpoint") {
            let version = parse_delta_checkpoint_version(filename)?
                .context("Delta checkpoint filename is malformed")?;
            checkpoint_tips.insert(version);
            continue;
        }
        let Some(version_text) = filename.strip_suffix(".json") else {
            continue;
        };
        if version_text.len() != 20 || !version_text.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("Delta log JSON filename is not a canonical version");
        }
        let version = version_text.parse::<u64>().context("Delta log version is invalid")?;
        ensure!(versions_seen.insert(version), "duplicate Delta log version file");
        ensure!(meta.size <= 16 * 1024 * 1024, "Delta log JSON exceeds the 16 MiB evidence bound");
        log_files = log_files.checked_add(1).context("Delta log file count overflow")?;
        ensure!(log_files <= 10_000, "Delta log exceeds the 10,000-file evidence bound");
        let contents = store.get(&meta.location).await?.bytes().await?;
        parse_delta_log_adds(version, &contents, &mut adds)?;
    }
    validate_delta_log_version_history(&versions_seen, &checkpoint_tips)?;
    let mut versions: BTreeMap<u64, (u64, u64, u64)> = BTreeMap::new();
    for ((version, _), add) in adds {
        let totals = versions.entry(version).or_default();
        totals.0 = totals.0.checked_add(add.rows).context("Delta log row count overflow")?;
        totals.1 = totals.1.checked_add(1).context("Delta log file count overflow")?;
        totals.2 = totals.2.checked_add(add.bytes).context("Delta log byte count overflow")?;
    }
    let (rows, files, bytes) = versions.values().try_fold((0_u64, 0_u64, 0_u64), |(rows, files, bytes), (version_rows, version_files, version_bytes)| {
        Some((rows.checked_add(*version_rows)?, files.checked_add(*version_files)?, bytes.checked_add(*version_bytes)?))
    }).context("Delta log total overflow")?;
    Ok(json!({
        "scope": "unique_data_change_true_add_actions_in_run_scoped_table_log",
        "rows": rows,
        "files": files,
        "parquet_bytes": bytes,
        "versions": versions.iter().map(|(version, (rows, files, bytes))| json!({
            "delta_version": version, "rows": rows, "files": files, "parquet_bytes": bytes,
        })).collect::<Vec<_>>(),
    }))
}

#[cfg(test)]
mod delta_log_evidence_tests {
    use super::*;

    #[test]
    fn delta_log_add_parser_excludes_rewrites_and_deduplicates_paths() -> Result<()> {
        let mut adds = BTreeMap::new();
        let commit = br#"{"add":{"path":"part-1.parquet","size":20,"dataChange":true,"stats":"{\"numRecords\":5}"}}
{"add":{"path":"part-1.parquet","size":20,"dataChange":true,"stats":"{\"numRecords\":5}"}}
{"add":{"path":"compact.parquet","size":30,"dataChange":false,"stats":"{\"numRecords\":5}"}}"#;
        parse_delta_log_adds(7, commit, &mut adds)?;
        assert_eq!(adds, BTreeMap::from([((7, "part-1.parquet".to_owned()), DeltaLogAdd { rows: 5, bytes: 20 })]));
        Ok(())
    }

    #[test]
    fn exploratory_reconciliation_marks_mismatches_without_hiding_counts() {
        let delta = json!({
            "rows": 5,
            "versions": [{"delta_version": 7, "rows": 5, "files": 1, "parquet_bytes": 20}],
        });
        let events = json!({
            "rows": 5,
            "commits_by_version": [{"delta_version": 7, "rows": 5, "files": 1, "parquet_bytes": 20}],
        });
        let matched = exploratory_delta_reconciliation(Some(&delta), Some(&events), Some(5), None);
        assert_eq!(matched["status"], "matched");
        assert_eq!(matched["delta_log_committed_rows"], 5);
        assert_eq!(matched["client_acknowledged_rows"], 5);
        assert_eq!(matched["qualification"], "not_qualifying");

        let mismatch = exploratory_delta_reconciliation(Some(&delta), Some(&events), Some(6), None);
        assert_eq!(mismatch["status"], "mismatch");
        assert_eq!(mismatch["delta_vs_acknowledged_rows"], -1);
        assert_eq!(mismatch["delta_log_committed_rows"], 5);

        let unavailable = exploratory_delta_reconciliation(None, Some(&events), Some(5), Some("delta_log_read_failed"));
        assert_eq!(unavailable["status"], "unavailable");
        assert!(unavailable["delta_log_committed_rows"].is_null());
    }

    #[test]
    fn exploratory_reconciliation_detects_per_version_commit_divergence() {
        let delta = json!({"rows": 5, "versions": [{"delta_version": 7, "rows": 5, "files": 1, "parquet_bytes": 20}]});
        let events = json!({"rows": 5, "commits_by_version": [{"delta_version": 7, "rows": 4, "files": 1, "parquet_bytes": 20}]});
        let result = exploratory_delta_reconciliation(Some(&delta), Some(&events), Some(5), None);
        assert_eq!(result["status"], "mismatch");
        assert_eq!(result["per_version"][0]["matched"], false);
    }

    #[test]
    fn delta_log_history_accepts_benign_checkpoint_only_with_complete_json() -> Result<()> {
        let complete = BTreeSet::from([0, 1, 2]);
        assert!(validate_delta_log_version_history(&complete, &BTreeSet::new()).is_ok());
        assert!(validate_delta_log_version_history(&complete, &BTreeSet::from([2])).is_ok());
        assert!(validate_delta_log_version_history(&complete, &BTreeSet::from([3])).is_err());
        assert!(validate_delta_log_version_history(&BTreeSet::new(), &BTreeSet::from([0])).is_err());
        assert!(validate_delta_log_version_history(&BTreeSet::from([0, 2]), &BTreeSet::from([1])).is_err());
        assert!(validate_delta_log_version_history(&BTreeSet::from([1]), &BTreeSet::from([0])).is_err());
        Ok(())
    }

    #[test]
    fn delta_checkpoint_markers_require_canonical_metadata() -> Result<()> {
        assert_eq!(parse_last_checkpoint_version(br#"{"version":2,"size":3}"#)?, 2);
        assert!(parse_last_checkpoint_version(br#"{"size":3}"#).is_err());
        assert_eq!(
            parse_delta_checkpoint_version("00000000000000000002.checkpoint.0000000001.0000000002.parquet")?,
            Some(2)
        );
        assert!(parse_delta_checkpoint_version("not-a-checkpoint.parquet").is_ok_and(|tip| tip.is_none()));
        assert!(parse_delta_checkpoint_version("00000000000000000002.checkpoint.bad.parquet").is_err());
        Ok(())
    }

    #[test]
    fn delta_log_add_parser_rejects_missing_row_statistics() {
        let mut adds = BTreeMap::new();
        assert!(parse_delta_log_adds(1, br#"{"add":{"path":"part.parquet","size":20,"dataChange":true}}"#, &mut adds).is_err());
    }
}

fn exploratory_delta_reconciliation(
    delta_log: Option<&Value>,
    commit_output: Option<&Value>,
    client_acknowledged_rows: Option<u64>,
    unavailable_reason: Option<&str>,
) -> Value {
    let delta_rows = delta_log.and_then(|value| value["rows"].as_u64());
    let event_rows = commit_output.and_then(|value| value["rows"].as_u64());
    let delta_vs_event_rows = delta_rows.zip(event_rows).map(|(delta, event)| i128::from(delta) - i128::from(event));
    let delta_vs_acknowledged_rows = delta_rows
        .zip(client_acknowledged_rows)
        .map(|(delta, acknowledged)| i128::from(delta) - i128::from(acknowledged));
    let delta_versions = delta_log.and_then(|value| value["versions"].as_array());
    let event_versions = commit_output.and_then(|value| value["commits_by_version"].as_array());
    let version_comparison = delta_versions.zip(event_versions).map(|(delta, events)| {
        let mut delta_by_version = BTreeMap::new();
        let mut event_by_version = BTreeMap::new();
        let valid_delta = delta.iter().all(|version| {
            let Some(number) = version["delta_version"].as_u64() else { return false; };
            delta_by_version.insert(number, (version["rows"].as_u64(), version["files"].as_u64(), version["parquet_bytes"].as_u64())).is_none()
        });
        let valid_events = events.iter().all(|event| {
            let Some(number) = event["delta_version"].as_u64() else { return false; };
            event_by_version.insert(number, (event["rows"].as_u64(), event["files"].as_u64(), event["parquet_bytes"].as_u64())).is_none()
        });
        if !valid_delta || !valid_events {
            return (None, json!([]));
        }
        let numbers = delta_by_version.keys().chain(event_by_version.keys()).copied().collect::<BTreeSet<_>>();
        let mut comparisons = Vec::new();
        let mut matched = true;
        for number in numbers {
            let delta = delta_by_version.get(&number).copied();
            let event = event_by_version.get(&number).copied();
            let equal = delta.is_some() && delta == event && delta.is_some_and(|(rows, files, bytes)| rows.is_some() && files.is_some() && bytes.is_some());
            matched &= equal;
            comparisons.push(json!({
                "delta_version": number,
                "delta_log": delta.map(|(rows, files, bytes)| json!({"rows": rows, "files": files, "parquet_bytes": bytes})),
                "collector_event": event.map(|(rows, files, bytes)| json!({"rows": rows, "files": files, "parquet_bytes": bytes})),
                "matched": equal,
            }));
        }
        (Some(matched), json!(comparisons))
    });
    let versions_match = version_comparison.as_ref().and_then(|(matched, _)| *matched);
    let ack_matches = delta_vs_acknowledged_rows.map(|delta| delta == 0);
    let status = if versions_match == Some(false) || ack_matches == Some(false) {
        "mismatch"
    } else if delta_rows.is_some() && event_rows.is_some() && client_acknowledged_rows.is_some()
        && versions_match == Some(true) && ack_matches == Some(true)
    {
        "matched"
    } else {
        "unavailable"
    };
    json!({
        "evidence_class": "d53_exploratory",
        "status": status,
        "qualification": "not_qualifying",
        "unavailable_reason": (status == "unavailable").then_some(unavailable_reason.unwrap_or("required_reconciliation_evidence_unavailable")),
        "delta_log_committed_rows": delta_rows,
        "collector_event_committed_rows": event_rows,
        "client_acknowledged_rows": client_acknowledged_rows,
        "delta_vs_collector_event_rows": delta_vs_event_rows,
        "delta_vs_acknowledged_rows": delta_vs_acknowledged_rows,
        "per_version": version_comparison.map(|(_, comparisons)| comparisons),
        "scope": "unique_data_change_true_delta_adds_cross_checked_against_ingestion_only_commit_events_and_whole_run_client_acknowledgments_including_drain",
    })
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
            instance_id: "collector-test".to_owned(),
            image: None,
            image_id: None,
            source_revision: None,
            complete: true,
            raw_log_bytes: 0,
            raw_log_lines: 0,
            stdout: String::new(),
            stderr: String::new(),
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
    fn full_filtered_log_preserves_startup_readback_and_tail_commit() -> Result<()> {
        let mut config = config()?;
        config.tunables = serde_json::from_value(json!({"router":{"hard_pressure_bps":9500}}))?;
        let commit = event(&config, 100, 199)?;
        let mut input = String::from(
            "arrowcol_router_effective_config {\"routing\":{\"hard_pressure_bps\":9500},\"log_filter\":\"warn,tiberius=error\"}\narrowcol_effective_config {\"commit_telemetry\":{\"schema_version\":1,\"scope\":\"new_ingestion_addfiles_after_sync\",\"clock\":\"system_unix_ms\"}}\n",
        );
        for _ in 0..20_001 {
            input.push_str("noise line\n");
        }
        input.push_str(&format!("arrowcol_data_commit {commit}\n"));
        let filtered = filter_process_evidence_text(&input)?;
        let readback = process_configurations(&filtered)?;
        verify_process_configuration(&config, ComponentRole::Router, &readback)?;
        let log = ProcessLog {
            role: ComponentRole::Collector,
            instance_id: "collector-test".to_owned(),
            image: None,
            image_id: None,
            source_revision: None,
            complete: true,
            raw_log_bytes: 0,
            raw_log_lines: 0,
            stdout: filtered.clone(),
            stderr: String::new(),
            text: filtered,
        };
        let evidence = durable_commit_evidence(&config, &[log], Some((100, 200)))?;
        assert_eq!(evidence[&config.tables[0]]["rows"], 10);
        Ok(())
    }

    #[test]
    fn incomplete_collector_logs_never_count_partial_commit_events() -> Result<()> {
        let config = config()?;
        let mut log = log(&[]);
        let commit = event(&config, 100, 199)?;
        log.text = format!(
            "arrowcol_effective_config {{\"component\":\"layout\",\"table_effective\":{{}}}}\narrowcol_data_commit {commit}\n"
        );
        log.complete = process_log_complete(&log.role, &log.text);
        assert!(!log.complete);
        let evidence = durable_commit_evidence(&config, &[log], Some((100, 200)))?;
        let table = &evidence[&config.tables[0]];
        assert!(table["rows"].is_null());
        assert_eq!(table["unavailable_reason"], "collector_commit_log_incomplete");
        assert!(table["run_cumulative_output"]["rows"].is_null());
        assert_eq!(
            table["run_cumulative_output"]["unavailable_reason"],
            "collector_commit_log_incomplete"
        );
        Ok(())
    }

    #[test]
    fn cell_scoped_commit_logs_exclude_the_previous_cell_tail() -> Result<()> {
        let config = config()?;
        let previous = event(&config, 10, 20)?;
        let mut late_previous = event(&config, 100, 150)?;
        late_previous["delta_version"] = json!(8);
        let mut current = event(&config, 210, 220)?;
        current["delta_version"] = json!(9);
        let baseline = log(&[previous]);
        let mut full = baseline.clone();
        full.text.push_str(&format!(
            "arrowcol_data_commit {}\narrowcol_data_commit {}\n",
            late_previous, current
        ));
        let (scoped, used_offsets) = cell_scoped_commit_logs(&[full.clone()], &[baseline])?;
        assert!(used_offsets);
        let evidence = durable_commit_evidence_for_cell(&config, &[full], &scoped, Some((200, 300)), Some(200))?;
        assert_eq!(evidence[&config.tables[0]]["run_cumulative_output"]["rows"], 10);
        assert_eq!(evidence[&config.tables[0]]["rows"], 10);
        Ok(())
    }

    #[test]
    fn cell_scoped_logs_fall_back_when_stdout_and_stderr_grow() -> Result<()> {
        let config = config()?;
        let previous = event(&config, 10, 20)?;
        let mut late_previous = event(&config, 100, 150)?;
        late_previous["delta_version"] = json!(8);
        let mut current = event(&config, 210, 220)?;
        current["delta_version"] = json!(9);
        let baseline = log(&[previous]);
        let mut current_log = baseline.clone();
        current_log.text = format!("stdout1stdout2\nstderr1stderr2\n{}", current_log.text);
        current_log.text.push_str(&format!(
            "arrowcol_data_commit {}\narrowcol_data_commit {}\n",
            late_previous, current
        ));
        let mut baseline_log = baseline;
        baseline_log.text = format!("stdout1\nstderr1\n{}", baseline_log.text);
        let (scoped, used_offsets) = cell_scoped_commit_logs(&[current_log.clone()], &[baseline_log])?;
        assert!(!used_offsets);
        let evidence = durable_commit_evidence_for_cell(&config, &[current_log], &scoped, Some((200, 300)), Some(200))?;
        assert_eq!(evidence[&config.tables[0]]["run_cumulative_output"]["rows"], 10);
        assert_eq!(evidence[&config.tables[0]]["rows"], 10);
        Ok(())
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
        let receipts = evidence["run_cumulative_output"]["commits_by_version"].as_array().context("missing commit receipts")?;
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0]["table_id"], "a".repeat(64));
        assert_eq!(receipts[0]["table_name"], canonical_dmv_table_name(&config.tables[0])?);
        assert_eq!(receipts[0]["delta_version"], 7);
        assert_eq!(receipts[0]["rows"], 10);
        assert_eq!(receipts[0]["files"], 1);
        assert_eq!(receipts[0]["input_bytes"], 100);
        assert_eq!(receipts[0]["parquet_bytes"], 50);
        assert_eq!(receipts[0]["min_ingested_at_ms"], 100);
        assert_eq!(receipts[0]["max_ingested_at_ms"], 199);
        assert_eq!(receipts[0]["finished_at_unix_ms"], 200);
        assert_eq!(receipts[0]["elapsed_seconds"], 0.5);
        Ok(())
    }

    #[test]
    fn stopped_collector_output_includes_drain_and_marks_incomplete_logs() -> Result<()> {
        let config = config()?;
        let commit = event(&config, 100, 199)?;
        let mut drain = event(&config, 210, 220)?;
        drain["delta_version"] = json!(8);
        let mut full = log(&[commit, drain]);
        let table = canonical_dmv_table_name(&config.tables[0])?;
        let stopped = stopped_collector_commit_output(&config, &full)?;
        assert_eq!(stopped["source"], "complete_stopped_collector_logs");
        assert_eq!(stopped["tables"][&table]["commits_by_version"].as_array().unwrap().len(), 2);
        full.complete = false;
        let incomplete = stopped_collector_commit_output(&config, &full)?;
        assert_eq!(incomplete["source"], "incomplete_stopped_collector_logs");
        assert_eq!(incomplete["tables"][&table]["unavailable_reason"], "collector_commit_log_incomplete");
        Ok(())
    }

    #[test]
    fn cumulative_commit_receipts_without_finish_time_are_unavailable() -> Result<()> {
        let config = config()?;
        let mut commit = event(&config, 100, 199)?;
        commit["finished_at_unix_ms"] = Value::Null;
        let evidence = durable_commit_evidence(&config, &[log(&[commit])], Some((100, 200)))?;
        let cumulative = &evidence[&config.tables[0]]["run_cumulative_output"];
        assert!(cumulative["rows"].is_null());
        assert_eq!(cumulative["commits_by_version"], json!([]));
        assert_eq!(cumulative["unavailable_reason"], "cumulative_commit_finish_time_unavailable");
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
        assert_eq!(table["output_throughput"]["input_bytes_per_second"], 2000.0);
        assert_eq!(table["output_throughput"]["parquet_bytes_per_second"], 1000.0);
        assert_eq!(table["run_cumulative_output"]["rows"], 40);
        assert!(table["output_throughput"]["unique_delivered_rows"].is_null());
        let zero = durable_commit_evidence(&config, &[log(&[])], Some((100, 200)))?;
        assert_eq!(zero[&config.tables[0]]["output_throughput"]["rows"], 0);
        let mut absent = log(&[]);
        absent.text.clear();
        let absent = durable_commit_evidence(&config, &[absent], Some((100, 200)))?;
        assert!(absent[&config.tables[0]]["output_throughput"]["rows"].is_null());
        Ok(())
    }

    #[test]
    fn complete_commit_windows_preserve_half_open_boundaries_and_unknowns() -> Result<()> {
        let config = config()?;
        let mut first = event(&config, 900, 999)?;
        first["finished_at_unix_ms"] = json!(1000);
        let mut boundary = event(&config, 1000, 1001)?;
        boundary["delta_version"] = json!(8);
        boundary["finished_at_unix_ms"] = json!(31_000);
        let mut tail = event(&config, 1000, 1001)?;
        tail["delta_version"] = json!(9);
        tail["finished_at_unix_ms"] = json!(61_000);
        let mut at_end = event(&config, 1000, 1001)?;
        at_end["delta_version"] = json!(10);
        at_end["finished_at_unix_ms"] = json!(66_000);
        let evidence = durable_commit_evidence(&config, &[log(&[first, boundary, tail, at_end])], Some((1000, 66_000)))?;
        let windows = &evidence[&config.tables[0]]["complete_30_second_commit_windows"];
        assert_eq!(windows.as_array().context("commit windows are not an array")?.len(), 2);
        assert_eq!(windows[0]["rows"], 10);
        assert_eq!(windows[1]["rows"], 10);
        assert_eq!(evidence[&config.tables[0]]["output_throughput"]["rows"], 30);
        let cumulative = &evidence[&config.tables[0]]["run_cumulative_output"];
        let receipts = cumulative["commits_by_version"].as_array().context("missing cumulative commit receipts")?;
        assert_eq!(receipts.len(), 4);
        assert_eq!(receipts.iter().map(|receipt| receipt["finished_at_unix_ms"].as_i64().unwrap()).collect::<Vec<_>>(),
            vec![1_000, 31_000, 61_000, 66_000]);
        assert_eq!(cumulative["rows"], 40);
        let mut missing = log(&[]);
        missing.complete = false;
        let evidence = durable_commit_evidence(&config, &[missing], Some((1000, 66_000)))?;
        let windows = &evidence[&config.tables[0]]["complete_30_second_commit_windows"];
        assert_eq!(windows.as_array().context("commit windows are not an array")?.len(), 2);
        assert!(windows[0]["rows"].is_null());
        assert_eq!(windows[0]["unavailable_reason"], "collector_commit_log_incomplete");
        Ok(())
    }
}
