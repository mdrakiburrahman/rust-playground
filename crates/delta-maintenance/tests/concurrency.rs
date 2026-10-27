//! Process-level concurrency proof: a separate blind-append writer process runs
//! while the standalone service binary applies OPTIMIZE, PURGE and VACUUM LITE.

use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_deltalake::delta::{DeltaTable, INGESTION_PARTITION, ingestion_partition_value};
use arrow_deltalake::maintenance::{DISPOSABLE_TEST_MARKER, MAX_COMMIT_ATTEMPTS, now_ms};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};
use url::Url;

const TEST: &str = "separate_writer_process_with_service_optimize_purge_vacuum";
const ROLE: &str = "DELTA_MAINTENANCE_CONCURRENCY_ROLE";
const ROOT: &str = "DELTA_MAINTENANCE_CONCURRENCY_ROOT";
const BASE: &str = "DELTA_MAINTENANCE_CONCURRENCY_BASE";
const MINUTE: i64 = 60_000;
/// Predeclared bounds: writer publications (each replayed once) and service cycles.
const PUBLICATIONS: i64 = 24;
const CYCLES: usize = 6;
/// Partitions are minute offsets from `base` (30 minutes before start). With
/// purge retention 26.5 minutes and a run shorter than 5 minutes, the purge cutoff
/// stays inside (base+3, base+9]: 0..=2 always expire and 10..=20 never do.
const EXPIRED: [i64; 3] = [0, 1, 2];
const RETAINED: std::ops::RangeInclusive<i64> = 10..=20;
const WRITER: [i64; 4] = [16, 17, 18, 19];
/// Latest 2 sealed partitions below the protected newest (20).
const LATEST: [i64; 2] = [18, 19];
const PURGE_RETENTION_SECS: &str = "1590";

type Row = (i64, String);

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(INGESTION_PARTITION, DataType::Utf8, false),
    ]))
}

fn table_dir(root: &Path) -> PathBuf {
    root.join("events")
}

fn open(root: &Path) -> Result<Arc<DeltaTable>> {
    let url = Url::from_directory_path(fs::canonicalize(table_dir(root))?).map_err(|()| anyhow!("URL"))?;
    DeltaTable::open_url(url, schema(), vec![INGESTION_PARTITION.into()])
}

fn stamp(base: i64, offset: i64) -> i64 {
    base + offset * MINUTE + 1
}

fn partition(base: i64, offset: i64) -> Result<String> {
    ingestion_partition_value(stamp(base, offset))
}

fn publish(table: &Arc<DeltaTable>, base: i64, offset: i64, ids: &[i64], app_id: Option<&str>) -> Result<u64> {
    let value = partition(base, offset)?;
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vec![value.as_str(); ids.len()])),
        ],
    )?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let mut file = table
            .new_file_partitioned(
                schema(),
                BTreeMap::from([(INGESTION_PARTITION.to_owned(), Some(value.clone()))]),
            )?
            .with_ingestion_time(stamp(base, offset))?;
        if let Some(app_id) = app_id {
            file = file.with_transaction(app_id.to_owned(), 0)?;
        }
        file.write(&batch).await?;
        Ok(file.finish().await?.version)
    })
}

fn writer_ids(index: i64) -> Vec<i64> {
    (0..5).map(|row| 100_000 + index * 10 + row).collect()
}

fn writer_offset(index: i64) -> i64 {
    WRITER[usize::try_from(index).unwrap_or(0) % WRITER.len()]
}

/// Child role: independent publications, each followed by a replay of the same identity.
fn writer(root: &Path, base: i64) -> Result<()> {
    let table = open(root)?;
    for index in 0..PUBLICATIONS {
        let app_id = format!("conc.writer.{index}");
        let ids = writer_ids(index);
        let first = publish(&table, base, writer_offset(index), &ids, Some(&app_id))?;
        if index == 0 {
            fs::write(root.join("writer-started"), b"1")?;
        }
        let replay = publish(&table, base, writer_offset(index), &ids, Some(&app_id))?;
        ensure!(first == replay, "{app_id}: replay committed {replay}, original {first}");
    }
    Ok(())
}

fn service(root: &Path, operations: &[&str]) -> Result<Vec<serde_json::Value>> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_delta-maintenance"));
    command.env_remove("OTEL_EXPORTER_OTLP_ENDPOINT").args([
        "--root",
        Url::from_directory_path(root).map_err(|()| anyhow!("URL"))?.as_str(),
        "--table",
        "events",
        "--granularity",
        "minute",
        "--purge-retention-secs",
        PURGE_RETENTION_SECS,
        "--latest-partitions",
        "2",
        "--vacuum-retention-secs",
        "0",
        "--unsafe-short-vacuum-for-tests",
        "--no-metrics-listener",
        "--apply",
    ]);
    for operation in operations {
        command.args(["--operation", operation]);
    }
    let output = command.output()?;
    let reports = std::str::from_utf8(&output.stdout)?
        .lines()
        .map(|line| Ok(serde_json::from_str::<serde_json::Value>(line)?))
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        reports.len() == operations.len(),
        "service exit {:?}: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let failed = reports.iter().any(|report| !report["error_kind"].is_null());
    ensure!(
        output.status.success() != failed,
        "exit status must reflect report errors"
    );
    Ok(reports)
}

fn log_actions(root: &Path) -> Result<Vec<(u64, serde_json::Value)>> {
    let log = table_dir(root).join("_delta_log");
    let mut actions = Vec::new();
    for entry in fs::read_dir(&log)? {
        let path = entry?.path();
        let name = path.file_name().and_then(|name| name.to_str()).unwrap_or_default();
        if let Some(version) = name.strip_suffix(".json") {
            let version: u64 = version.parse()?;
            for line in fs::read_to_string(&path)?.lines() {
                actions.push((version, serde_json::from_str(line)?));
            }
        }
    }
    actions.sort_by_key(|(version, _)| *version);
    Ok(actions)
}

fn active(actions: &[(u64, serde_json::Value)]) -> Result<BTreeMap<String, String>> {
    let mut files = BTreeMap::new();
    for (_, action) in actions {
        if let Some(add) = action.get("add") {
            let value = add["partitionValues"][INGESTION_PARTITION]
                .as_str()
                .context("partition")?;
            files.insert(add["path"].as_str().context("path")?.to_owned(), value.to_owned());
        } else if let Some(remove) = action.get("remove") {
            files.remove(remove["path"].as_str().context("path")?);
        }
    }
    Ok(files)
}

fn physical_parquet(dir: &Path, prefix: &Path, found: &mut BTreeSet<String>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path.file_name().and_then(|name| name.to_str()).unwrap_or_default();
        if name == "_delta_log" {
            continue;
        }
        if path.is_dir() {
            physical_parquet(&path, prefix, found)?;
        } else if name.ends_with(".parquet") {
            found.insert(path.strip_prefix(prefix)?.to_string_lossy().into_owned());
        }
    }
    Ok(())
}

fn decode(relative: &str) -> String {
    relative.replace("%3A", ":").replace("%2D", "-")
}

fn rows(root: &Path, files: &BTreeMap<String, String>) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for (relative, value) in files {
        let path = table_dir(root).join(decode(relative));
        for batch in ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&path)?)?.build()? {
            let batch = batch?;
            let ids = batch
                .column_by_name("id")
                .context("id")?
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("id type")?;
            rows.extend(ids.values().iter().map(|id| (*id, value.clone())));
        }
    }
    rows.sort();
    Ok(rows)
}

fn driver(root: &Path) -> Result<()> {
    let base = (now_ms()? / MINUTE) * MINUTE - 30 * MINUTE;
    let started = Instant::now();
    fs::create_dir_all(table_dir(root))?;
    let table = open(root)?;
    let mut expected = Vec::new();
    for offset in EXPIRED.into_iter().chain(RETAINED) {
        for part in 0..2 {
            let ids = [offset * 100 + part];
            publish(&table, base, offset, &ids, None)?;
            if RETAINED.contains(&offset) {
                expected.push((ids[0], partition(base, offset)?));
            }
        }
    }
    fs::write(table_dir(root).join(DISPOSABLE_TEST_MARKER), b"")?;
    for index in 0..PUBLICATIONS {
        let value = partition(base, writer_offset(index))?;
        expected.extend(writer_ids(index).into_iter().map(|id| (id, value.clone())));
    }
    expected.sort();

    let mut child = Command::new(std::env::current_exe()?)
        .args([TEST, "--exact", "--nocapture", "--test-threads=1"])
        .env(ROLE, "writer")
        .env(ROOT, root)
        .env(BASE, base.to_string())
        .spawn()?;
    while !root.join("writer-started").exists() {
        if started.elapsed() > Duration::from_secs(120) {
            child.kill()?;
            bail!("writer never started");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let operations = ["optimize", "purge", "vacuum"];
    let mut outcomes: BTreeMap<String, usize> = BTreeMap::new();
    let mut max_attempts = 0_u64;
    let mut collisions = 0_u64;
    let mut concurrent_cycles = 0;
    for _ in 0..CYCLES {
        for report in service(root, &operations)? {
            let label = match report["error_kind"].as_str() {
                Some(kind) => {
                    ensure!(
                        matches!(kind, "stale_plan" | "exhausted"),
                        "unexpected concurrent failure: {report}"
                    );
                    format!("{}:{kind}", report["operation"].as_str().unwrap_or_default())
                }
                None => {
                    let inner = &report["report"];
                    max_attempts = max_attempts.max(inner["attempts"].as_u64().unwrap_or(0));
                    collisions += inner["collisions"].as_u64().unwrap_or(0);
                    format!(
                        "{}:{}",
                        report["operation"].as_str().unwrap_or_default(),
                        inner["outcome"].as_str().unwrap_or_default()
                    )
                }
            };
            *outcomes.entry(label).or_default() += 1;
        }
        if child.try_wait()?.is_none() {
            concurrent_cycles += 1;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    ensure!(child.wait()?.success(), "writer process failed");
    ensure!(concurrent_cycles > 0, "no service cycle overlapped the writer");
    ensure!(
        max_attempts <= u64::from(MAX_COMMIT_ATTEMPTS),
        "attempts {max_attempts}"
    );

    std::thread::sleep(Duration::from_millis(20));
    for report in service(root, &operations)? {
        ensure!(report["error_kind"].is_null(), "quiescent apply failed: {report}");
    }
    std::thread::sleep(Duration::from_millis(20));
    let settled = service(root, &["vacuum"])?;
    ensure!(settled[0]["error_kind"].is_null(), "{}", settled[0]);
    let version = log_actions(root)?.last().map(|(version, _)| *version);
    for report in service(root, &operations)? {
        let inner = &report["report"];
        // VACUUM LITE never commits; its repeat must find nothing left to delete.
        let idle = if report["operation"] == "vacuum" {
            inner["files_deleted"] == 0
        } else {
            inner["outcome"] == "noop"
        };
        ensure!(idle, "repeat must be a no-op: {report}");
    }
    ensure!(
        log_actions(root)?.last().map(|(version, _)| *version) == version,
        "no-op repeat changed the version"
    );

    let actions = log_actions(root)?;
    let files = active(&actions)?;
    let mut physical = BTreeSet::new();
    physical_parquet(&table_dir(root), &table_dir(root), &mut physical)?;
    let active_paths: BTreeSet<String> = files.keys().map(|path| decode(path)).collect();
    ensure!(active_paths.is_subset(&physical), "active Add missing on disk");
    // Each new-delivery replay stages its own file; on a deduplicated commit that file
    // stays unreferenced (kept because unknown-success retries may reuse staged paths),
    // and log-only VACUUM LITE never collects it. Model the orphans exactly.
    let mentioned: BTreeSet<String> = actions
        .iter()
        .filter_map(|(_, action)| action.get("add").or_else(|| action.get("remove")))
        .filter_map(|file| file["path"].as_str().map(decode))
        .collect();
    let orphans: BTreeMap<String, String> = physical
        .difference(&active_paths)
        .map(|path| {
            let value = path
                .split('/')
                .next()
                .and_then(|dir| dir.strip_prefix(&format!("{INGESTION_PARTITION}=")))
                .context("orphan partition")?;
            Ok((path.clone(), value.to_owned()))
        })
        .collect::<Result<_>>()?;
    ensure!(
        orphans.keys().all(|path| !mentioned.contains(path)),
        "a logged file survived outside the active set"
    );
    ensure!(
        orphans.len() == usize::try_from(PUBLICATIONS)?,
        "expected one orphan per deduplicated replay, found {}",
        orphans.len()
    );
    let mut replayed: Vec<Row> = Vec::new();
    for index in 0..PUBLICATIONS {
        let value = partition(base, writer_offset(index))?;
        replayed.extend(writer_ids(index).into_iter().map(|id| (id, value.clone())));
    }
    replayed.sort();
    ensure!(
        rows(root, &orphans)? == replayed,
        "orphans must be exactly the replayed rows"
    );
    ensure!(rows(root, &files)? == expected, "row multiset changed");

    let latest: BTreeSet<String> = LATEST
        .iter()
        .map(|offset| partition(base, *offset))
        .collect::<Result<_>>()?;
    let expired: BTreeSet<String> = EXPIRED
        .iter()
        .map(|offset| partition(base, *offset))
        .collect::<Result<_>>()?;
    let mut optimized = BTreeSet::new();
    let mut txns: BTreeMap<String, usize> = BTreeMap::new();
    for (_, action) in &actions {
        if let Some(remove) = action.get("remove") {
            let value = remove["partitionValues"][INGESTION_PARTITION]
                .as_str()
                .context("remove partition")?
                .to_owned();
            if remove["dataChange"] == false {
                ensure!(latest.contains(&value), "OPTIMIZE touched {value} outside latest 2");
                optimized.insert(value);
            } else {
                ensure!(expired.contains(&value), "PURGE removed retained {value}");
            }
        }
        if let Some(txn) = action.get("txn") {
            let app = txn["appId"].as_str().context("appId")?;
            if app.starts_with("conc.writer.") {
                *txns.entry(app.to_owned()).or_default() += 1;
            }
        }
    }
    ensure!(
        optimized == latest,
        "OPTIMIZE never compacted {latest:?}: {optimized:?}"
    );
    ensure!(
        txns.len() == usize::try_from(PUBLICATIONS)? && txns.values().all(|count| *count == 1),
        "each writer publication commits exactly once: {txns:?}"
    );
    ensure!(
        started.elapsed() < Duration::from_secs(300),
        "run exceeded the partition model window"
    );
    println!(
        "concurrency proof: cycles={CYCLES} overlapping={concurrent_cycles} publications={PUBLICATIONS} \
         max_attempts={max_attempts} collisions={collisions} outcomes={outcomes:?} rows={} files={} replay_orphans={}",
        expected.len(),
        files.len(),
        orphans.len()
    );
    Ok(())
}

/// Scenario: a separate OS process makes 24 independent blind-append publications
/// (each replayed with the same identity) into retained minute partitions while the
/// actual service binary runs 6 fixed OPTIMIZE+PURGE+VACUUM LITE apply cycles on a
/// marked disposable root, then one quiescent cycle and a repeat.
/// Guarantees: every writer publication and replay succeeds and commits exactly once;
/// maintenance failures are only bounded stale-plan/exhausted outcomes within
/// `MAX_COMMIT_ATTEMPTS`; OPTIMIZE touches only the latest 2 sealed partitions and
/// PURGE only expired ones; the final row multiset equals retained seed rows plus
/// all writer rows; every active Add exists on disk and the only other Parquet files
/// are the 24 never-logged replay stagings (known limitation); the repeat is a no-op
/// with no new version.
#[test]
fn separate_writer_process_with_service_optimize_purge_vacuum() -> Result<()> {
    match std::env::var(ROLE).as_deref() {
        Ok("writer") => {
            let root = PathBuf::from(std::env::var(ROOT)?);
            writer(&root, std::env::var(BASE)?.parse()?)
        }
        Ok("driver") => driver(&PathBuf::from(std::env::var(ROOT)?)),
        _ => {
            let root = tempfile::tempdir()?;
            let status = Command::new(std::env::current_exe()?)
                .args([TEST, "--exact", "--nocapture", "--test-threads=1"])
                .env(ROLE, "driver")
                .env(ROOT, root.path())
                .env("ARROWCOL_PARTITION_GRANULARITY", "minute")
                .status()?;
            ensure!(status.success(), "driver failed");
            Ok(())
        }
    }
}
