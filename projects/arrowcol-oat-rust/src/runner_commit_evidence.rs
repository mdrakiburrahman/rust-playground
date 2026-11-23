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


#[derive(Default)]
struct LocalDeltaCommitMetadata {
    operation: Option<String>,
    operation_mode: Option<String>,
    timestamp_unix_ms: Option<u64>,
}

type LocalDeltaAdds = BTreeMap<(u64, String), DeltaLogAdd>;

fn parse_local_delta_commit(version: u64, contents: &[u8]) -> Result<(LocalDeltaCommitMetadata, LocalDeltaAdds)> {
    let mut metadata = LocalDeltaCommitMetadata::default();
    let mut adds = BTreeMap::new();
    for (line_number, line) in contents.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() { continue; }
        let action: Value = serde_json::from_slice(line)
            .with_context(|| format!("invalid Delta log JSON at line {}", line_number + 1))?;
        if let Some(info) = action.get("commitInfo") {
            if let Some(operation) = info.get("operation").and_then(Value::as_str) {
                metadata.operation = Some(operation.to_owned());
            }
            metadata.operation_mode = info.pointer("/operationParameters/mode")
                .and_then(Value::as_str).map(str::to_owned);
            metadata.timestamp_unix_ms = info.get("timestamp").and_then(Value::as_i64)
                .and_then(|timestamp| u64::try_from(timestamp).ok());
        }
        let Some(add) = action.get("add") else { continue; };
        match add.get("dataChange") {
            None | Some(Value::Bool(true)) => {}
            Some(Value::Bool(false)) => continue,
            Some(_) => bail!("Delta add dataChange must be a boolean"),
        }
        let path = add.get("path").and_then(Value::as_str).filter(|path| !path.is_empty())
            .context("Delta add action omitted its path")?;
        let stats = match add.get("stats") {
            Some(Value::String(stats)) => serde_json::from_str::<Value>(stats)
                .context("Delta add action has invalid stats JSON")?,
            Some(Value::Object(_)) => add["stats"].clone(),
            _ => bail!("Delta add action omitted row statistics"),
        };
        let rows = stats.get("numRecords").and_then(Value::as_u64)
            .context("Delta add statistics omitted numRecords")?;
        let bytes = add.get("size").and_then(Value::as_u64)
            .context("Delta add action omitted file size")?;
        let identity = (version, path.to_owned());
        let record = DeltaLogAdd { rows, bytes };
        if let Some(previous) = adds.insert(identity, record) && previous != record {
            bail!("conflicting duplicate Delta add action");
        }
    }
    Ok((metadata, adds))
}

fn unavailable_local_delta_reconciliation(reason: impl Into<String>) -> Value {
    let reason = reason.into();
    json!({
        "version": 1, "status": "unavailable", "reconciliation_state": "unavailable",
        "unavailable_reason": reason, "csv_reason": format!("reconciliation unavailable: {reason}"),
        "csv": {
            "started_utc": null, "ended_utc": null, "durable_rows_per_s": null,
            "durable_rows": null, "committed_add_actions": null, "bytes_per_row": null,
            "compression_ratio": null, "latency_p50_ms": null, "latency_p99_ms": null,
            "files_written": null, "commits_written": null,
        },
    })
}

#[derive(Clone, Copy)]
struct LocalCommitReceipt {
    version: u64, rows: u64, files: u64, input_bytes: u64, parquet_bytes: u64,
    min_ingested_at_ms: u64, max_ingested_at_ms: u64, finished_at_unix_ms: u64,
}

fn local_commit_receipt(value: &Value) -> Result<LocalCommitReceipt> {
    let number = |field: &str| value.get(field).and_then(Value::as_u64)
        .with_context(|| format!("commit receipt omitted valid {field}"));
    Ok(LocalCommitReceipt {
        version: number("delta_version")?, rows: number("rows")?, files: number("files")?,
        input_bytes: number("input_bytes")?, parquet_bytes: number("parquet_bytes")?,
        min_ingested_at_ms: number("min_ingested_at_ms")?, max_ingested_at_ms: number("max_ingested_at_ms")?,
        finished_at_unix_ms: number("finished_at_unix_ms")?,
    })
}

fn reconcile_local_delta_log(delta_log: &Path, durable: &Value) -> Value {
    match reconcile_local_delta_log_inner(delta_log, durable) {
        Ok(value) => value,
        Err(error) => unavailable_local_delta_reconciliation(format!("{error:#}")),
    }
}

fn reconcile_local_delta_log_inner(delta_log: &Path, durable: &Value) -> Result<Value> {
    let window = durable.get("output_throughput").context("output_throughput absent")?;
    if let Some(reason) = window.get("unavailable_reason").and_then(Value::as_str) {
        bail!("output_throughput unavailable: {reason}");
    }
    let start = window["measurement_start_unix_ms"].as_u64()
        .context("measurement window start missing or invalid")?;
    let end = window["measurement_end_unix_ms"].as_u64()
        .context("measurement window end missing or invalid")?;
    ensure!(end > start, "measurement window is empty or reversed");
    let window_seconds = (end - start) as f64 / 1000.0;
    let receipt_values = durable.pointer("/run_cumulative_output/commits_by_version")
        .and_then(Value::as_array).context("commit receipts (commits_by_version) absent")?;
    if let Some(reason) = durable.pointer("/run_cumulative_output/unavailable_reason").and_then(Value::as_str) {
        bail!("run_cumulative_output unavailable: {reason}");
    }
    let mut receipts_by_version = BTreeMap::new();
    let mut receipt_errors = Vec::new();
    for value in receipt_values {
        match local_commit_receipt(value) {
            Ok(receipt) => {
                if receipts_by_version.insert(receipt.version, receipt).is_some() {
                    return Ok(local_delta_mismatch("duplicate receipts for the same Delta version",
                        json!({"duplicate_receipt_version": receipt.version})));
                }
            }
            Err(error) => receipt_errors.push(format!("{error:#}")),
        }
    }
    if !receipt_errors.is_empty() { bail!("{}", receipt_errors.join("; ")); }
    let selected = receipts_by_version.values()
        .filter(|receipt| receipt.finished_at_unix_ms >= start && receipt.finished_at_unix_ms < end)
        .copied().collect::<Vec<_>>();
    let sum = |field: fn(&LocalCommitReceipt) -> u64| -> Result<u64> {
        selected.iter().try_fold(0_u64, |total, receipt| {
            total.checked_add(field(receipt)).context("in-window receipt totals overflow")
        })
    };
    let receipt_totals = json!({
        "rows": sum(|receipt| receipt.rows)?, "files": sum(|receipt| receipt.files)?,
        "input_bytes": sum(|receipt| receipt.input_bytes)?, "parquet_bytes": sum(|receipt| receipt.parquet_bytes)?,
    });
    for field in ["rows", "files", "input_bytes", "parquet_bytes"] {
        let expected = window[field].as_u64().with_context(|| format!("window {field} missing or invalid"))?;
        if receipt_totals[field].as_u64() != Some(expected) {
            return Ok(local_delta_mismatch(format!("window {field} differs from in-window receipt total"),
                json!({"window_value": expected, "receipt_value": receipt_totals[field]})));
        }
    }

    let mut present_versions = BTreeSet::new();
    for entry in fs::read_dir(delta_log).context("Delta log directory unreadable")? {
        let entry = entry.context("read Delta log directory entry")?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(version_text) = name.strip_suffix(".json") else { continue; };
        if version_text.len() != 20 || !version_text.bytes().all(|byte| byte.is_ascii_digit()) { continue; }
        let metadata = fs::symlink_metadata(entry.path()).context("inspect Delta log JSON file")?;
        ensure!(metadata.file_type().is_file(), "Delta log JSON entry is not a regular file");
        present_versions.insert(version_text.parse::<u64>().context("Delta log filename version invalid")?);
        ensure!(present_versions.len() <= 10_000, "Delta log exceeds the 10,000-file evidence bound");
    }

    let mut observed_rows = 0_u64;
    let mut observed_files = 0_u64;
    let mut observed_bytes = 0_u64;
    let mut operations = BTreeMap::<String, u64>::new();
    let mut oldest_latency_ms = Vec::new();
    let mut newest_latency_ms = Vec::new();
    for receipt in &selected {
        let path = delta_log.join(format!("{:020}.json", receipt.version));
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("Delta version {} missing", receipt.version))?;
        ensure!(metadata.file_type().is_file(), "receipted Delta version is not a regular file");
        ensure!(metadata.len() <= 64 * 1024 * 1024, "Delta version JSON exceeds the 64 MiB evidence bound");
        let contents = fs::read(&path).context("read receipted Delta version JSON")?;
        let (commit, adds) = parse_local_delta_commit(receipt.version, &contents)
            .with_context(|| format!("parse Delta version {}", receipt.version))?;
        let (rows, files, bytes) = adds.values().try_fold((0_u64, 0_u64, 0_u64), |(rows, files, bytes), add| {
            Some((rows.checked_add(add.rows)?, files.checked_add(1)?, bytes.checked_add(add.bytes)?))
        }).context("Delta add totals overflow")?;
        if (rows, files, bytes) != (receipt.rows, receipt.files, receipt.parquet_bytes) {
            return Ok(local_delta_mismatch(format!("Delta version {} Adds differ from its commit receipt", receipt.version),
                json!({"delta_version": receipt.version,
                    "observed": {"rows": rows, "files": files, "parquet_bytes": bytes},
                    "receipt": {"rows": receipt.rows, "files": receipt.files, "parquet_bytes": receipt.parquet_bytes}})));
        }
        observed_rows = observed_rows.checked_add(rows).context("Delta row total overflow")?;
        observed_files = observed_files.checked_add(files).context("Delta file total overflow")?;
        observed_bytes = observed_bytes.checked_add(bytes).context("Delta byte total overflow")?;
        let operation = commit.operation.unwrap_or_else(|| "unknown".to_owned());
        let operation_key = commit
            .operation_mode
            .map_or_else(|| operation.clone(), |mode| format!("{operation}/{mode}"));
        *operations.entry(operation_key).or_default() += 1;
        oldest_latency_ms.push(receipt.finished_at_unix_ms.saturating_sub(receipt.min_ingested_at_ms));
        newest_latency_ms.push(receipt.finished_at_unix_ms.saturating_sub(receipt.max_ingested_at_ms));
    }

    let span = selected.first().zip(selected.last()).map(|(first, last)| (first.version, last.version));
    let mut candidates = present_versions.clone();
    if let Some((first, last)) = span {
        ensure!(last - first <= 10_000, "receipt version span exceeds the 10,000-version evidence bound");
        for version in first..=last {
            let path = delta_log.join(format!("{version:020}.json"));
            let metadata = fs::symlink_metadata(&path)
                .with_context(|| format!("Delta JSON missing inside receipt version span at {version}"))?;
            ensure!(metadata.file_type().is_file(), "Delta JSON inside receipt version span is not a regular file");
            candidates.insert(version);
        }
    }
    let maintenance_operations = ["DELETE", "UPDATE", "MERGE", "RESTORE", "REORG", "OPTIMIZE"];
    let mut interleaved_maintenance = BTreeMap::<String, u64>::new();
    let mut unreceipted_checked = 0_u64;
    for version in candidates {
        if receipts_by_version.contains_key(&version) { continue; }
        let in_span = span.is_some_and(|(first, last)| version >= first && version <= last);
        let path = delta_log.join(format!("{version:020}.json"));
        let contents = match fs::read(&path) {
            Ok(contents) => contents,
            Err(error) if in_span => return Err(error).with_context(|| format!("unreceipted version {version} inside receipt span is missing")),
            Err(_) => continue,
        };
        ensure!(contents.len() <= 64 * 1024 * 1024, "Delta version JSON exceeds the 64 MiB evidence bound");
        let (commit, adds) = match parse_local_delta_commit(version, &contents) {
            Ok(parsed) => parsed,
            Err(_) if !in_span => continue,
            Err(error) => return Err(error).with_context(|| format!("parse unreceipted Delta version {version}")),
        };
        let in_window = commit.timestamp_unix_ms.is_some_and(|timestamp| timestamp >= start && timestamp < end);
        if !in_span && !in_window { continue; }
        unreceipted_checked = unreceipted_checked.checked_add(1).context("unreceipted version count overflow")?;
        let operation = commit.operation.unwrap_or_else(|| "unknown".to_owned());
        if !adds.is_empty() && !maintenance_operations.contains(&operation.as_str()) {
            return Ok(local_delta_mismatch(format!("unreceipted version {version} ({operation}) contains data-changing Adds"),
                json!({"delta_version": version, "operation": operation, "data_change_adds": adds.len()})));
        }
        let key = if adds.is_empty() { operation } else { format!("{operation} (data-changing rewrite, excluded)") };
        *interleaved_maintenance.entry(key).or_default() += 1;
    }

    let percentile = |values: &mut Vec<u64>, p: usize| -> Option<u64> {
        if values.is_empty() { return None; }
        values.sort_unstable();
        Some(values[(values.len() * p).div_ceil(100).saturating_sub(1).min(values.len() - 1)])
    };
    let p50 = percentile(&mut oldest_latency_ms, 50);
    let p99 = percentile(&mut oldest_latency_ms, 99);
    let newest_p50 = percentile(&mut newest_latency_ms, 50);
    let newest_p99 = percentile(&mut newest_latency_ms, 99);
    let start_utc = i64::try_from(start).ok()
        .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
        .map(|value| value.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
    let end_utc = i64::try_from(end).ok()
        .and_then(chrono::DateTime::<chrono::Utc>::from_timestamp_millis)
        .map(|value| value.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
    let bytes_per_row = (observed_rows > 0).then_some(observed_bytes as f64 / observed_rows as f64);
    let input_bytes = receipt_totals["input_bytes"].as_u64().unwrap_or_default();
    let compression_ratio = (observed_bytes > 0).then_some(input_bytes as f64 / observed_bytes as f64);
    let csv = json!({
        "started_utc": start_utc, "ended_utc": end_utc,
        "durable_rows_per_s": observed_rows as f64 / window_seconds, "durable_rows": observed_rows,
        "committed_add_actions": observed_files, "bytes_per_row": bytes_per_row,
        "compression_ratio": compression_ratio, "latency_p50_ms": p50, "latency_p99_ms": p99,
        "files_written": observed_files, "commits_written": selected.len(),
    });
    Ok(json!({
        "version": 1, "status": "matched", "reconciliation_state": "reconciled",
        "scope": "local_stopped_delta_snapshot_receipts_selected_by_finished_at_in_half_open_window",
        "window": {"measurement_start_unix_ms": start, "measurement_end_unix_ms": end, "window_seconds": window_seconds},
        "receipts": {"total": receipt_values.len(), "in_window": selected.len(),
            "version_span": span.map(|(first, last)| json!([first, last]))},
        "receipt_totals": receipt_totals,
        "observed": {
            "durable_rows": observed_rows, "committed_add_actions": observed_files, "parquet_bytes": observed_bytes,
            "durable_rows_per_s": observed_rows as f64 / window_seconds, "bytes_per_row": bytes_per_row,
            "compression_ratio": compression_ratio,
            "ingest_to_commit_oldest_row_ms": {"p50": p50, "p99": p99, "commits": selected.len()},
            "ingest_to_commit_newest_row_ms": {"p50": newest_p50, "p99": newest_p99},
        },
        "collector_operations": operations, "interleaved_maintenance": interleaved_maintenance,
        "unreceipted_versions_checked": unreceipted_checked, "csv": csv, "csv_reason": null,
    }))
}

fn local_delta_mismatch(reason: impl Into<String>, details: Value) -> Value {
    let reason = reason.into();
    let mut result = unavailable_local_delta_reconciliation(format!("reconciliation mismatch: {reason}"));
    result["status"] = json!("mismatch");
    result["reconciliation_state"] = json!("mismatch");
    result["unavailable_reason"] = Value::Null;
    result["mismatch_reason"] = json!(reason);
    result["details"] = details;
    result["csv_reason"] = json!(format!("reconciliation mismatch: {reason}"));
    result
}


#[cfg(test)]
mod local_delta_reconciliation_tests {
    use super::*;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Result<Self> {
            let path = std::env::temp_dir().join(format!("arrowcol-local-delta-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&path)?;
            Ok(Self(path))
        }

        fn path(&self) -> &Path { &self.0 }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
    }

    fn receipt(version: u64, rows: u64, files: u64, bytes: (u64, u64), ingress: (u64, u64), finish: u64) -> Value {
        json!({
            "delta_version": version, "rows": rows, "files": files,
            "input_bytes": bytes.0, "parquet_bytes": bytes.1,
            "min_ingested_at_ms": ingress.0, "max_ingested_at_ms": ingress.1,
            "finished_at_unix_ms": finish, "elapsed_seconds": 0.1,
        })
    }

    fn add(path: &str, rows: u64, bytes: u64, data_change: bool) -> Value {
        json!({"add": {
            "path": path, "size": bytes, "dataChange": data_change,
            "stats": json!({"numRecords": rows}).to_string(),
        }})
    }

    fn write_version(root: &Path, version: u64, operation: &str, timestamp: u64, actions: &[Value]) -> Result<()> {
        let mut lines = vec![serde_json::to_string(&json!({"commitInfo": {
            "timestamp": timestamp, "operation": operation,
        }}))?];
        lines.extend(actions.iter().map(serde_json::to_string).collect::<std::result::Result<Vec<_>, _>>()?);
        fs::write(root.join(format!("{version:020}.json")), lines.join("\n") + "\n")?;
        Ok(())
    }

    fn evidence() -> Value {
        json!({
            "output_throughput": {
                "measurement_start_unix_ms": 1_000, "measurement_end_unix_ms": 109_000,
                "rows": 15, "files": 2, "input_bytes": 900, "parquet_bytes": 400,
            },
            "run_cumulative_output": {"commits_by_version": [
                receipt(0, 99, 1, (100, 50), (900, 950), 999),
                receipt(1, 10, 1, (600, 300), (1_005, 1_008), 1_010),
                receipt(3, 5, 1, (300, 100), (1_040, 1_045), 1_050),
                receipt(4, 77, 1, (100, 50), (108_000, 108_500), 109_000),
            ]},
        })
    }

    fn complete_log(root: &Path, middle: Value) -> Result<()> {
        write_version(root, 1, "WRITE", 1_010, &[add("part-1.parquet", 10, 300, true)])?;
        write_version(root, 2, "OPTIMIZE", 1_020, &[middle])?;
        write_version(root, 3, "WRITE", 1_050, &[add("part-3.parquet", 5, 100, true)])?;
        Ok(())
    }

    #[test]
    fn reconciles_half_open_receipts_and_emits_screen_v1_metrics() -> Result<()> {
        let directory = TestDirectory::new()?;
        complete_log(directory.path(), add("rewrite.parquet", 15, 390, false))?;
        let result = reconcile_local_delta_log(directory.path(), &evidence());
        assert_eq!(result["status"], "matched");
        assert_eq!(result["receipts"]["in_window"], 2);
        assert_eq!(result["observed"]["durable_rows"], 15);
        assert_eq!(result["observed"]["committed_add_actions"], 2);
        assert_eq!(result["csv"]["durable_rows"], 15);
        assert_eq!(result["csv"]["commits_written"], 2);
        assert_eq!(result["csv"]["latency_p50_ms"], 5);
        assert_eq!(result["csv"]["latency_p99_ms"], 10);
        assert_eq!(result["interleaved_maintenance"]["OPTIMIZE"], 1);
        Ok(())
    }

    #[test]
    fn missing_in_span_json_is_unavailable_and_receipt_add_divergence_is_mismatch() -> Result<()> {
        let missing = TestDirectory::new()?;
        write_version(missing.path(), 1, "WRITE", 1_010, &[add("part-1.parquet", 10, 300, true)])?;
        write_version(missing.path(), 3, "WRITE", 1_050, &[add("part-3.parquet", 5, 100, true)])?;
        assert_eq!(reconcile_local_delta_log(missing.path(), &evidence())["status"], "unavailable");

        let mismatch = TestDirectory::new()?;
        write_version(mismatch.path(), 1, "WRITE", 1_010, &[add("part-1.parquet", 9, 300, true)])?;
        write_version(mismatch.path(), 2, "OPTIMIZE", 1_020, &[add("rewrite.parquet", 15, 390, false)])?;
        write_version(mismatch.path(), 3, "WRITE", 1_050, &[add("part-3.parquet", 5, 100, true)])?;
        let result = reconcile_local_delta_log(mismatch.path(), &evidence());
        assert_eq!(result["status"], "mismatch");
        assert!(result["csv"]["durable_rows"].is_null());

        let mut bad_window_totals = evidence();
        bad_window_totals["output_throughput"]["rows"] = json!(16);
        assert_eq!(reconcile_local_delta_log(mismatch.path(), &bad_window_totals)["status"], "mismatch");
        Ok(())
    }

    #[test]
    fn unreceipted_data_adds_fail_except_for_allowed_maintenance_rewrites() -> Result<()> {
        let ordinary = TestDirectory::new()?;
        write_version(ordinary.path(), 1, "WRITE", 1_010, &[add("part-1.parquet", 10, 300, true)])?;
        write_version(ordinary.path(), 2, "WRITE", 1_020, &[add("unexpected.parquet", 1, 20, true)])?;
        write_version(ordinary.path(), 3, "WRITE", 1_050, &[add("part-3.parquet", 5, 100, true)])?;
        assert_eq!(reconcile_local_delta_log(ordinary.path(), &evidence())["status"], "mismatch");

        let maintenance = TestDirectory::new()?;
        complete_log(maintenance.path(), add("rewrite.parquet", 1, 20, true))?;
        let result = reconcile_local_delta_log(maintenance.path(), &evidence());
        assert_eq!(result["status"], "matched");
        assert_eq!(result["interleaved_maintenance"]["OPTIMIZE (data-changing rewrite, excluded)"], 1);
        Ok(())
    }
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
        assert_eq!(stopped["tables"][&table]["commits_by_version"].as_array().context("missing stopped commit receipts")?.len(), 2);
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
        assert_eq!(receipts.iter().map(|receipt| receipt["finished_at_unix_ms"].as_i64().context("missing finish timestamp")).collect::<Result<Vec<_>>>()?,
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
