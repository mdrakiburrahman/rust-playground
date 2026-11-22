use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_deltalake::delta::{DeltaTable, INGESTION_PARTITION};
use arrow_deltalake::maintenance::{
    Granularity, MaintenanceControl, MaintenanceErrorKind, NegativeProbeConfig, OptimizeConfig, Outcome,
    PassProbeInventory, ProbeOperation, PurgeConfig, VacuumConfig, error_kind,
};
use async_trait::async_trait;
use delta_kernel::object_store::{
    self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, local::LocalFileSystem,
};
use futures::stream::BoxStream;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;
use url::Url;

const BASE: i64 = 1_704_067_200_000;
const MINUTE: i64 = 60_000;
const RETENTION: i64 = 7 * 24 * 60 * MINUTE;
const ROWS_PER_FILE: i64 = 512;
const PASSES: usize = 30;

type Row = (i64, Option<String>, String);

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(INGESTION_PARTITION, DataType::Utf8, false),
    ]))
}

fn partition(index: i64) -> String {
    format!("2024-01-01-00-{index:02}")
}

fn url(root: &Path) -> Result<Url> {
    Url::from_directory_path(fs::canonicalize(root)?).map_err(|()| anyhow!("invalid local directory URL"))
}

fn create(root: &Path) -> Result<Arc<DeltaTable>> {
    fs::create_dir(root)?;
    DeltaTable::open_url(url(root)?, schema(), vec![INGESTION_PARTITION.into()])
}

fn append(table: &Arc<DeltaTable>, index: i64, ids: Range<i64>) -> Result<()> {
    let value = partition(index);
    let names = ids
        .clone()
        .map(|id| (id % 3 != 0).then(|| format!("n{id}")))
        .collect::<Vec<_>>();
    let rows = names.len();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(vec![value.as_str(); rows])),
        ],
    )?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let mut file = table
                .new_file_partitioned(
                    schema(),
                    BTreeMap::from([(INGESTION_PARTITION.to_owned(), Some(value))]),
                )?
                .with_ingestion_time(BASE + index * MINUTE + 1)?;
            file.write(&batch).await?;
            file.finish().await?;
            Ok::<_, anyhow::Error>(())
        })
}

fn seed(root: &Path) -> Result<()> {
    fs::create_dir(root)?;
    let active = create(&root.join("active"))?;
    for file in 0..34 {
        let part = if file < 16 {
            0
        } else if file < 32 {
            1
        } else {
            2
        };
        append(&active, part, file * ROWS_PER_FILE..(file + 1) * ROWS_PER_FILE)?;
    }
    let _empty = create(&root.join("empty"))?;
    let quiescent = create(&root.join("quiescent"))?;
    append(&quiescent, 0, 0..ROWS_PER_FILE)?;
    let protected = create(&root.join("protected"))?;
    append(&protected, 0, 0..ROWS_PER_FILE)?;
    append(&protected, 0, ROWS_PER_FILE..2 * ROWS_PER_FILE)?;
    let due = create(&root.join("due"))?;
    append(&due, 0, 0..ROWS_PER_FILE)?;
    append(&due, 1, ROWS_PER_FILE..2 * ROWS_PER_FILE)?;
    Ok(())
}

fn active_files(root: &Path) -> Result<BTreeMap<String, String>> {
    let mut logs = fs::read_dir(root.join("_delta_log"))?
        .map(|entry| Ok(entry?.path()))
        .collect::<Result<Vec<_>>>()?;
    logs.retain(|p| p.extension().is_some_and(|ext| ext == "json"));
    logs.sort();
    let mut active = BTreeMap::new();
    for log in logs {
        for line in fs::read_to_string(log)?.lines() {
            let action: Value = serde_json::from_str(line)?;
            if let Some(add) = action.get("add") {
                let path = add["path"].as_str().context("missing add path")?;
                let part = add["partitionValues"][INGESTION_PARTITION]
                    .as_str()
                    .context("missing partition")?;
                active.insert(path.to_owned(), part.to_owned());
            }
            if let Some(remove) = action.get("remove") {
                active.remove(remove["path"].as_str().context("missing remove path")?);
            }
        }
    }
    Ok(active)
}

fn metadata(root: &Path) -> Result<Value> {
    let mut logs = fs::read_dir(root.join("_delta_log"))?
        .map(|entry| Ok(entry?.path()))
        .collect::<Result<Vec<_>>>()?;
    logs.retain(|p| p.extension().is_some_and(|ext| ext == "json"));
    logs.sort();
    let mut metadata = None;
    for log in logs {
        for line in fs::read_to_string(log)?.lines() {
            let action: Value = serde_json::from_str(line)?;
            if let Some(value) = action.get("metaData") {
                metadata = Some(value.clone());
            }
        }
    }
    metadata.context("missing Delta metadata")
}

fn verify_rewrite_actions(root: &Path, first_version: u64, final_version: u64) -> Result<()> {
    for version in first_version + 1..=final_version {
        let path = root.join("_delta_log").join(format!("{version:020}.json"));
        let mut changes = 0;
        for line in fs::read_to_string(path)?.lines() {
            let action: Value = serde_json::from_str(line)?;
            if let Some(change) = action.get("add").or_else(|| action.get("remove")) {
                ensure!(change["dataChange"] == false, "rewrite action was marked as ingestion");
                changes += 1;
            }
        }
        ensure!(changes > 0, "rewrite commit did not contain file actions");
    }
    Ok(())
}

fn data_path(root: &Path, relative: &str) -> Result<PathBuf> {
    url(root)?
        .join(relative)?
        .to_file_path()
        .map_err(|()| anyhow!("invalid data path"))
}

fn rows_and_bytes(root: &Path) -> Result<(Vec<Row>, u64, usize)> {
    let files = active_files(root)?;
    let mut rows = Vec::new();
    let mut bytes = 0;
    for (relative, partition) in &files {
        let path = data_path(root, relative)?;
        bytes += fs::metadata(&path)?.len();
        let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path)?)?.build()?;
        for batch in reader {
            let batch = batch?;
            let ids = batch.column_by_name("id").context("id")?;
            let names = batch.column_by_name("name").context("name")?;
            ensure!(
                ids.data_type() == &DataType::Int64 && names.data_type() == &DataType::Utf8,
                "unexpected physical column schema"
            );
            let ids = ids.as_any().downcast_ref::<Int64Array>().context("id array")?;
            let names = names.as_any().downcast_ref::<StringArray>().context("name array")?;
            for row in 0..batch.num_rows() {
                ensure!(ids.is_valid(row), "required id is null");
                rows.push((
                    ids.value(row),
                    names.is_valid(row).then(|| names.value(row).to_owned()),
                    partition.clone(),
                ));
            }
        }
    }
    rows.sort();
    Ok((rows, bytes, files.len()))
}

fn control() -> MaintenanceControl {
    MaintenanceControl::new(Arc::new(AtomicBool::new(false)), None)
}

fn optimize() -> OptimizeConfig {
    OptimizeConfig {
        granularity: Some(Granularity::Minute),
        now_ms: Some(BASE + 60 * MINUTE),
        max_files_per_bin: 8,
        max_bins_per_commit: 2,
        ..OptimizeConfig::default()
    }
}

fn probe() -> NegativeProbeConfig {
    NegativeProbeConfig {
        now_ms: BASE + 60 * MINUTE,
        granularity: Granularity::Minute,
        purge_retention_ms: RETENTION,
        vacuum_retention_ms: RETENTION,
        log_retention_ms: RETENTION,
        checkpoint_interval: 100,
        optimize_small_file_bytes: 64 * 1024 * 1024,
        optimize_target_file_bytes: 128 * 1024 * 1024,
        optimize_min_files: 2,
        sealed_partitions_only: false,
        max_listed_files: 1000,
    }
}

fn cpu_ticks() -> Result<u64> {
    let stat = fs::read_to_string("/proc/self/stat")?;
    let fields = stat
        .rsplit_once(") ")
        .context("invalid proc stat")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    Ok(fields.get(11).context("missing user ticks")?.parse::<u64>()?
        + fields.get(12).context("missing system ticks")?.parse::<u64>()?)
}

fn tick_rate() -> Result<u64> {
    let output = std::process::Command::new("getconf").arg("CLK_TCK").output()?;
    ensure!(output.status.success(), "getconf CLK_TCK failed");
    Ok(std::str::from_utf8(&output.stdout)?.trim().parse()?)
}

fn active(root: &Path) -> Result<Value> {
    let root = root.join("active");
    let (before, before_bytes, before_files) = rows_and_bytes(&root)?;
    ensure!(
        before.len() == 34 * ROWS_PER_FILE as usize && before_files == 34,
        "unexpected seeded active state"
    );
    for (index, (id, name, part)) in before.iter().enumerate() {
        let expected_id = index as i64;
        let file = expected_id / ROWS_PER_FILE;
        let expected_part = if file < 16 {
            0
        } else if file < 32 {
            1
        } else {
            2
        };
        ensure!(
            *id == expected_id
                && *name == (expected_id % 3 != 0).then(|| format!("n{expected_id}"))
                && *part == partition(expected_part),
            "seed row does not match deterministic input"
        );
    }
    let original_metadata = metadata(&root)?;
    let protected_before = active_files(&root)?
        .into_iter()
        .filter(|(_, part)| part == &partition(2))
        .collect::<BTreeMap<_, _>>();
    let table = DeltaTable::open_for_maintenance(&url(&root)?)?;
    let first_version = table.version()?;
    let config = optimize();
    let mut reports = Vec::new();
    let ticks_per_second = tick_rate()?;
    let cpu_start = cpu_ticks()?;
    let started = Instant::now();
    let final_debt = loop {
        ensure!(reports.len() < 64, "maintenance did not converge");
        let prepared = table.prepare_optimize(&config)?;
        if prepared.plan.bins.is_empty() {
            break prepared.plan.compaction_debt;
        }
        let report = table.apply_prepared_optimize(&prepared, &config, &control())?;
        ensure!(
            report.outcome == Outcome::Committed,
            "non-committed active pass: {:?}",
            report.outcome
        );
        reports.push(report);
    };
    let elapsed = started.elapsed().as_secs_f64();
    let cpu_seconds = (cpu_ticks()? - cpu_start) as f64 / ticks_per_second as f64;
    let (after, after_bytes, after_files) = rows_and_bytes(&root)?;
    ensure!(before == after, "active row multiset changed");
    ensure!(metadata(&root)? == original_metadata, "Delta metadata changed");
    ensure!(
        active_files(&root)?
            .into_iter()
            .filter(|(_, part)| part == &partition(2))
            .collect::<BTreeMap<_, _>>()
            == protected_before,
        "protected files changed"
    );
    let final_version = table.version()?;
    ensure!(
        final_version == first_version + reports.len() as u64,
        "unexpected active commit count"
    );
    verify_rewrite_actions(&root, first_version, final_version)?;
    let noop = table.prepare_optimize(&config)?;
    ensure!(noop.plan.bins.is_empty(), "converged plan has bins");
    ensure!(
        table.apply_prepared_optimize(&noop, &config, &control())?.outcome == Outcome::Noop,
        "converged pass was not a no-op"
    );
    ensure!(table.version()? == final_version, "converged pass committed");
    ensure!(final_debt.eligible_files == 0, "eligible debt remains");
    let protected = final_debt
        .protected_partition_backlog
        .context("missing protected backlog")?;
    ensure!(protected.files == 2, "unexpected protected backlog");
    let removed_files: u64 = reports.iter().map(|r| r.files_removed).sum();
    let added_files: u64 = reports.iter().map(|r| r.files_added).sum();
    let removed_bytes: u64 = reports.iter().map(|r| r.bytes_removed).sum();
    let added_bytes: u64 = reports.iter().map(|r| r.bytes_added).sum();
    ensure!(
        removed_bytes <= before_bytes * 4 && removed_files <= before_files as u64 * 4,
        "rewrite amplification exceeded the fixed 4x fixture bound"
    );
    ensure!(
        before_bytes
            .checked_add(added_bytes)
            .and_then(|b| b.checked_sub(removed_bytes))
            == Some(after_bytes),
        "physical active-byte reconciliation failed: before={before_bytes} removed={removed_bytes} added={added_bytes} after={after_bytes}"
    );
    ensure!(
        (before_files as u64)
            .checked_add(added_files)
            .and_then(|n| n.checked_sub(removed_files))
            == Some(after_files as u64),
        "active-file reconciliation failed"
    );
    Ok(json!({
        "fixture": "maintenance-active", "storage": "local-file", "initial_logical_rows": before.len(),
        "initial_logical_rows_per_second_to_convergence": before.len() as f64 / elapsed,
        "initial_eligible_rows": 32 * ROWS_PER_FILE, "initial_protected_rows": 2 * ROWS_PER_FILE,
        "elapsed_seconds": elapsed, "cpu_seconds": cpu_seconds, "cpu_clock_ticks_per_second": ticks_per_second,
        "initial_active_files": before_files, "initial_active_bytes": before_bytes,
        "final_active_files": after_files, "final_active_bytes": after_bytes,
        "rewrite_input_files": removed_files, "rewrite_input_bytes": removed_bytes,
        "rewrite_output_files": added_files, "rewrite_output_bytes": added_bytes,
        "rewrite_input_byte_amplification": removed_bytes as f64 / before_bytes as f64,
        "rewrite_amplification_limit": 4,
        "protected_files": protected.files, "protected_bytes": protected.bytes,
        "eligible_files_after": final_debt.eligible_files, "commits": reports.len(),
        "row_multiset_equal": true, "metadata_equal": true, "protected_files_unchanged": true,
        "rewrite_actions_data_change_false": true, "converged_noop_commits": 0
    }))
}

#[derive(Debug, Default)]
struct Counts {
    reads: AtomicUsize,
    lists: AtomicUsize,
    writes: AtomicUsize,
}

#[derive(Debug)]
struct CountedStore {
    inner: LocalFileSystem,
    counts: Arc<Counts>,
}

impl std::fmt::Display for CountedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("counted local file store")
    }
}

#[async_trait]
impl ObjectStore for CountedStore {
    async fn put_opts(
        &self,
        path: &object_store::path::Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.counts.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &object_store::path::Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.counts.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &object_store::path::Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.counts.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.counts.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&object_store::path::Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.counts.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&object_store::path::Path>) -> object_store::Result<ListResult> {
        self.counts.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.counts.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.counts.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.rename_opts(from, to, options).await
    }
}

fn counted(root: &Path) -> Result<(Arc<DeltaTable>, Arc<Counts>)> {
    let counts = Arc::new(Counts::default());
    let store = Arc::new(CountedStore {
        inner: LocalFileSystem::new(),
        counts: Arc::clone(&counts),
    });
    let table = DeltaTable::open_with_store(root, store as Arc<dyn ObjectStore>)?;
    counts.reads.store(0, Ordering::SeqCst);
    counts.lists.store(0, Ordering::SeqCst);
    counts.writes.store(0, Ordering::SeqCst);
    Ok((table, counts))
}

fn idle_case(root: &Path, case: &str) -> Result<Value> {
    let (table, counts) = counted(&root.join(case))?;
    let config = probe();
    let version = table.version()?;
    counts.reads.store(0, Ordering::SeqCst);
    counts.lists.store(0, Ordering::SeqCst);
    counts.writes.store(0, Ordering::SeqCst);
    let ticks_per_second = tick_rate()?;
    let cpu_start = cpu_ticks()?;
    let started = Instant::now();
    let mut fallback_count = 0;
    for _ in 0..PASSES {
        let mut inventory = PassProbeInventory::default();
        for operation in [
            ProbeOperation::Purge,
            ProbeOperation::Optimize,
            ProbeOperation::Vacuum,
            ProbeOperation::Checkpoint,
            ProbeOperation::PruneLog,
        ] {
            if table
                .probe_no_work_with_inventory(operation, &config, &control(), &mut inventory)?
                .is_none()
            {
                fallback_count += 1;
                match operation {
                    ProbeOperation::Purge => {
                        let policy = PurgeConfig::new(RETENTION, config.now_ms, Granularity::Minute);
                        let plan = table.plan_purge(&policy)?;
                        ensure!(plan.eligible_files == 0, "idle fixture unexpectedly has purge debt");
                        let report = table.apply_purge(&plan, &policy, &control())?;
                        ensure!(report.outcome == Outcome::Noop, "idle purge wrote");
                    }
                    ProbeOperation::Optimize => {
                        let prepared = table.prepare_optimize(&optimize())?;
                        ensure!(
                            prepared.plan.bins.is_empty(),
                            "idle fixture unexpectedly has eligible bins"
                        );
                        let report = table.apply_prepared_optimize(&prepared, &optimize(), &control())?;
                        ensure!(report.outcome == Outcome::Noop, "idle optimize wrote");
                    }
                    ProbeOperation::Vacuum => {
                        let report =
                            table.vacuum_once(&VacuumConfig::new(RETENTION, config.now_ms), true, &control())?;
                        ensure!(report.outcome == Outcome::Noop, "idle vacuum wrote");
                    }
                    _ => bail!("unexpected due or incomplete idle operation {operation:?} in {case}"),
                }
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let cpu_seconds = (cpu_ticks()? - cpu_start) as f64 / ticks_per_second as f64;
    let reads = counts.reads.load(Ordering::SeqCst);
    let lists = counts.lists.load(Ordering::SeqCst);
    let writes = counts.writes.load(Ordering::SeqCst);
    ensure!(
        table.version()? == version && writes == 0,
        "idle pass changed Delta or wrote to object store"
    );
    Ok(json!({
        "case": case, "passes": PASSES, "probe_invocations": 5 * PASSES,
        "elapsed_seconds": elapsed, "cpu_seconds": cpu_seconds,
        "store_get_calls": reads, "store_list_calls": lists,
        "store_write_calls": writes, "delta_commits": 0, "planner_fallbacks": fallback_count
    }))
}

fn verify_due(root: &Path) -> Result<()> {
    let root = root.join("due");
    let table = DeltaTable::open_for_maintenance(&url(&root)?)?;
    let config = probe();
    let mut retention_inventory = PassProbeInventory::default();
    ensure!(
        table
            .probe_no_work_with_inventory(ProbeOperation::Purge, &config, &control(), &mut retention_inventory)?
            .is_some(),
        "expected initial negative retention probe"
    );
    let retention_due = NegativeProbeConfig {
        now_ms: BASE + 8 * 24 * 60 * MINUTE,
        ..config.clone()
    };
    ensure!(
        table
            .probe_no_work_with_inventory(
                ProbeOperation::Purge,
                &retention_due,
                &control(),
                &mut retention_inventory
            )?
            .is_none(),
        "retention transition hidden by prior negative probe"
    );
    let purge = table.plan_purge(&PurgeConfig::new(RETENTION, retention_due.now_ms, Granularity::Minute))?;
    ensure!(purge.eligible_files > 0, "retention planner missed newly due files");
    let mut inventory = PassProbeInventory::default();
    ensure!(
        table
            .probe_no_work_with_inventory(ProbeOperation::Optimize, &config, &control(), &mut inventory)?
            .is_some(),
        "expected initial negative optimization probe"
    );
    let writer = DeltaTable::open_url(url(&root)?, schema(), vec![INGESTION_PARTITION.into()])?;
    append(&writer, 0, 2 * ROWS_PER_FILE..3 * ROWS_PER_FILE)?;
    let stale = table.probe_no_work_with_inventory(ProbeOperation::Optimize, &config, &control(), &mut inventory);
    match stale {
        Err(error) => ensure!(
            error_kind(&error) == Some(MaintenanceErrorKind::StalePlan),
            "unexpected stale probe error: {error:#}"
        ),
        Ok(None) => {}
        Ok(Some(_)) => bail!("stale negative probe hid newly due work"),
    }
    let mut fresh = PassProbeInventory::default();
    ensure!(
        table
            .probe_no_work_with_inventory(ProbeOperation::Optimize, &config, &control(), &mut fresh)?
            .is_none(),
        "fresh probe missed new compaction debt"
    );
    ensure!(
        !table.plan_optimize(&optimize())?.bins.is_empty(),
        "planner missed new compaction debt"
    );
    Ok(())
}

fn idle(root: &Path) -> Result<Value> {
    let mut cases = Vec::new();
    for case in ["empty", "quiescent", "protected"] {
        cases.push(idle_case(root, case)?);
    }
    verify_due(root)?;
    Ok(json!({
        "fixture": "maintenance-idle", "storage": "local-file", "cadence": "five operations per pass, no sleep",
        "cases": cases, "post_negative_commit_and_retention_due_detected": true,
        "io_scope": "object-store calls during measured passes; excludes direct filesystem sidecars"
    }))
}

fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    ensure!(
        args.len() == 2,
        "usage: darwin_maintenance <seed|active|idle> <fixture-directory>"
    );
    let root = Path::new(&args[1]);
    match args[0].as_str() {
        "seed" => seed(root),
        "active" => {
            println!("{}", active(root)?);
            Ok(())
        }
        "idle" => {
            println!("{}", idle(root)?);
            Ok(())
        }
        _ => bail!("unknown fixture command"),
    }
}
