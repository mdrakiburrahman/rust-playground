#![cfg(feature = "delta")]
//! Maintenance evidence against real Delta Kernel tables and Parquet files.

use anyhow::{Context, Result, anyhow, bail};
use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_deltalake::delta::{
    DeltaTable, INGESTION_PARTITION, PURGE_WATERMARK, PublicationFence, PublicationFenceContext, PublicationFenceError,
};
use arrow_deltalake::maintenance::{
    DISPOSABLE_TEST_MARKER, Granularity, MaintenanceControl, MaintenanceErrorKind, MaintenancePlan, OptimizeConfig,
    Outcome, PurgeConfig, VacuumConfig, error_kind, now_ms,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use url::Url;

const BASE: i64 = 1_704_067_200_000;
const MINUTE: i64 = 60_000;
type Row = (i64, Option<String>, String);

fn minute(index: i64) -> String {
    format!("2024-01-01-00-{index:02}")
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(INGESTION_PARTITION, DataType::Utf8, false),
    ]))
}

fn batch(ids: Range<i64>, partition: &str) -> Result<RecordBatch> {
    let names: Vec<Option<String>> = ids.clone().map(|id| (id % 3 != 0).then(|| format!("n{id}"))).collect();
    let rows = names.len();
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(vec![partition; rows])),
        ],
    )?)
}

fn expected(ids: Range<i64>, partition: &str) -> Vec<Row> {
    ids.map(|id| (id, (id % 3 != 0).then(|| format!("n{id}")), partition.to_owned()))
        .collect()
}

fn run<F: Future>(future: F) -> Result<F::Output> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(future))
}

fn url_of(root: &Path) -> Result<Url> {
    Url::from_directory_path(fs::canonicalize(root)?).map_err(|()| anyhow!("URL"))
}

fn create(root: &Path) -> Result<Arc<DeltaTable>> {
    DeltaTable::open_url(url_of(root)?, schema(), vec![INGESTION_PARTITION.into()])
}

fn append_value(table: &Arc<DeltaTable>, value: &str, stamp: i64, ids: Range<i64>) -> Result<u64> {
    run(async {
        let mut file = table
            .new_file_partitioned(
                schema(),
                BTreeMap::from([(INGESTION_PARTITION.to_owned(), Some(value.to_owned()))]),
            )?
            .with_ingestion_time(stamp)?;
        file.write(&batch(ids, value)?).await?;
        Ok::<_, anyhow::Error>(file.finish().await?.version)
    })?
}

fn append(table: &Arc<DeltaTable>, index: i64, ids: Range<i64>) -> Result<u64> {
    append_value(table, &minute(index), BASE + index * MINUTE + 1, ids)
}

fn control() -> MaintenanceControl {
    MaintenanceControl::new(Arc::new(AtomicBool::new(false)), None)
}

fn kind<T>(result: Result<T>) -> Option<MaintenanceErrorKind> {
    result.err().and_then(|error| error_kind(&error))
}

fn optimize_config() -> OptimizeConfig {
    OptimizeConfig {
        granularity: Some(Granularity::Minute),
        now_ms: Some(BASE + 60 * MINUTE),
        ..OptimizeConfig::default()
    }
}

fn purge_config() -> PurgeConfig {
    // cutoff = floor(00:03:30 - 90s) = 00:02, so minutes 00 and 01 expire.
    PurgeConfig::new(90_000, BASE + 3 * MINUTE + 30_000, Granularity::Minute)
}

fn log_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(root.join("_delta_log"))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<_>>()?;
    files.retain(|path| path.extension().is_some_and(|extension| extension == "json"));
    files.sort();
    Ok(files)
}

fn commit_actions(root: &Path, version: u64) -> Result<Vec<serde_json::Value>> {
    let text = fs::read_to_string(root.join("_delta_log").join(format!("{version:020}.json")))?;
    text.lines().map(|line| Ok(serde_json::from_str(line)?)).collect()
}

/// Active (decoded path -> partition) from a full JSON log replay.
fn active_files(root: &Path) -> Result<BTreeMap<String, String>> {
    let mut active = BTreeMap::new();
    for path in log_files(root)? {
        for line in fs::read_to_string(path)?.lines() {
            let action: serde_json::Value = serde_json::from_str(line)?;
            if let Some(add) = action.get("add") {
                let partition = add["partitionValues"][INGESTION_PARTITION]
                    .as_str()
                    .context("partition")?
                    .to_owned();
                active.insert(add["path"].as_str().context("path")?.to_owned(), partition);
            } else if let Some(remove) = action.get("remove") {
                active.remove(remove["path"].as_str().context("path")?);
            }
        }
    }
    Ok(active)
}

fn data_path(root: &Path, relative: &str) -> Result<PathBuf> {
    url_of(root)?
        .join(relative)?
        .to_file_path()
        .map_err(|()| anyhow!("path"))
}

/// Sorted row multiset read from the actual active Parquet files.
fn rows(root: &Path) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for (relative, partition) in active_files(root)? {
        let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(data_path(root, &relative)?)?)?.build()?;
        for batch in reader {
            let batch = batch?;
            let ids = arrow::compute::cast(batch.column_by_name("id").context("id")?, &DataType::Int64)?;
            let names = arrow::compute::cast(batch.column_by_name("name").context("name")?, &DataType::Utf8)?;
            let ids = ids.as_any().downcast_ref::<Int64Array>().context("ids")?;
            let names = names.as_any().downcast_ref::<StringArray>().context("names")?;
            for row in 0..batch.num_rows() {
                let name = names.is_valid(row).then(|| names.value(row).to_owned());
                rows.push((ids.value(row), name, partition.clone()));
            }
        }
    }
    rows.sort();
    Ok(rows)
}

fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort();
    rows
}

fn files_in(root: &Path, partition: &str) -> Result<usize> {
    Ok(active_files(root)?.values().filter(|value| *value == partition).count())
}

/// Scenario: four minute partitions of two small files each, latest-2 OPTIMIZE.
/// Guarantees: the newest partition is protected, only the latest two closed partitions
/// are rewritten from real Parquet with identical rows/schema in one dataChange=false
/// commit, and repeated or exhausted plans never create bogus commits.
#[test]
fn optimize_rewrites_latest_closed_partitions_preserving_rows() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    let mut all = Vec::new();
    for index in 0..4 {
        for part in 0..2 {
            let ids = index * 100 + part * 10..index * 100 + part * 10 + 7;
            append(&table, index, ids.clone())?;
            all.extend(expected(ids, &minute(index)));
        }
    }
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let config = OptimizeConfig {
        latest_partitions: Some(2),
        ..optimize_config()
    };
    let plan = maintenance.plan_optimize(&config)?;
    let planned: BTreeSet<_> = plan
        .bins
        .iter()
        .map(|bin| bin.partition_values[INGESTION_PARTITION].clone())
        .collect();
    assert_eq!(planned, BTreeSet::from([Some(minute(1)), Some(minute(2))]));
    assert_eq!(plan.protected_partitions, 1);
    let serialized = serde_json::to_string(&MaintenancePlan::Optimize(plan.clone()))?;
    let MaintenancePlan::Optimize(plan) = serde_json::from_str(&serialized)? else {
        bail!("plan kind")
    };
    let before = maintenance.version()?;
    assert_eq!(rows(root.path())?, sorted(all.clone()));

    let report = maintenance.apply_optimize(&plan, &config, &control())?;
    assert_eq!(report.outcome, Outcome::Committed);
    assert_eq!(report.version, before + 1);
    assert_eq!(
        (report.files_removed, report.files_added, report.rows_added),
        (4, 2, 28)
    );
    assert_eq!(rows(root.path())?, sorted(all));
    for (index, files) in [(0, 2), (1, 1), (2, 1), (3, 2)] {
        assert_eq!(files_in(root.path(), &minute(index))?, files, "partition {index}");
    }
    let actions = commit_actions(root.path(), report.version)?;
    let changes: Vec<_> = actions
        .iter()
        .filter_map(|action| action.get("add").or_else(|| action.get("remove")))
        .collect();
    assert_eq!(changes.len(), 6);
    assert!(changes.iter().all(|change| change["dataChange"] == false));
    let replanned = maintenance.plan_optimize(&config)?;
    assert_eq!(
        replanned.header.fingerprint, plan.header.fingerprint,
        "schema and metadata preserved"
    );

    let again = maintenance.apply_optimize(&plan, &config, &control())?;
    assert_eq!(again.outcome, Outcome::AlreadyApplied);
    assert_eq!(maintenance.version()?, before + 1);
    let only_old = maintenance.plan_optimize(&optimize_config())?;
    assert_eq!(only_old.bins.len(), 1, "only minute 00 still has several files");
    maintenance.apply_optimize(&only_old, &optimize_config(), &control())?;
    let version = maintenance.version()?;
    let noop = maintenance.plan_optimize(&optimize_config())?;
    assert!(noop.bins.is_empty());
    assert_eq!(
        maintenance
            .apply_optimize(&noop, &optimize_config(), &control())?
            .outcome,
        Outcome::Noop
    );
    assert_eq!(maintenance.version()?, version, "singleton bins create no commit");
    Ok(())
}

/// Scenario: two plans over the same inputs, an unrelated append, and a schema change.
/// Guarantees: unrelated appends are tolerated, removed inputs make a plan stale,
/// metadata changes are rejected, and no failed apply publishes anything.
#[test]
fn optimize_rejects_stale_and_metadata_changed_plans() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    for index in 0..3 {
        append(&table, index, index * 10..index * 10 + 3)?;
        append(&table, index, index * 10 + 5..index * 10 + 8)?;
    }
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let first = maintenance.plan_optimize(&optimize_config())?;
    let second = maintenance.plan_optimize(&optimize_config())?;
    append(&table, 2, 50..52)?;
    append(&table, 0, 60..62)?;
    let report = maintenance.apply_optimize(&first, &optimize_config(), &control())?;
    assert_eq!(report.outcome, Outcome::Committed);
    let version = maintenance.version()?;
    assert_eq!(
        kind(maintenance.apply_optimize(&second, &optimize_config(), &control())),
        Some(MaintenanceErrorKind::StalePlan)
    );
    assert_eq!(maintenance.version()?, version);

    append(&table, 1, 70..72)?;
    let third = maintenance.plan_optimize(&optimize_config())?;
    assert!(!third.bins.is_empty());
    let mut evolved: Vec<_> = schema().fields().iter().map(Arc::clone).collect();
    evolved.push(Arc::new(Field::new("extra", DataType::Utf8, true)));
    DeltaTable::open_url(
        url_of(root.path())?,
        Arc::new(Schema::new(evolved)),
        vec![INGESTION_PARTITION.into()],
    )?;
    let evolved_version = maintenance.version()?;
    assert_eq!(
        kind(maintenance.apply_optimize(&third, &optimize_config(), &control())),
        Some(MaintenanceErrorKind::MetadataChanged)
    );
    assert_eq!(maintenance.version()?, evolved_version);
    Ok(())
}

/// Scenario: a malformed partition directory coexists with valid ingestion partitions.
/// Guarantees: malformed values never authorize rewrite or deletion.
#[test]
fn malformed_partitions_fail_closed() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    append_value(&table, "bogus", BASE, 0..3)?;
    append_value(&table, "2024-01-01-00-1", BASE, 3..6)?;
    append_value(&table, "bogus", BASE, 6..9)?;
    for index in 0..4 {
        append(&table, index, 100 + index * 10..103 + index * 10)?;
    }
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let plan = maintenance.plan_optimize(&optimize_config())?;
    assert_eq!(plan.skipped_partitions, 2);
    assert!(plan.bins.is_empty());
    let purge = maintenance.plan_purge(&purge_config())?;
    assert_eq!(purge.skipped_partitions, 2);
    let removed: BTreeSet<_> = purge
        .partitions
        .iter()
        .map(|partition| partition.value.clone())
        .collect();
    assert_eq!(removed, BTreeSet::from([minute(0), minute(1)]));
    maintenance.apply_purge(&purge, &purge_config(), &control())?;
    let left: BTreeSet<_> = active_files(root.path())?.into_values().collect();
    assert!(left.contains("bogus") && left.contains("2024-01-01-00-1"));
    Ok(())
}

/// Scenario: PURGE of expired minute partitions with a replayed expired append afterwards.
/// Guarantees: exact expired partitions are removed with dataChange=true and the
/// watermark in the same commit; replay below it is fenced; repeats keep the version.
#[test]
fn purge_removes_expired_partitions_and_fences_replay() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    let mut kept = Vec::new();
    for index in 0..4 {
        append(&table, index, index * 10..index * 10 + 4)?;
        if index >= 2 {
            kept.extend(expected(index * 10..index * 10 + 4, &minute(index)));
        }
    }
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let plan = maintenance.plan_purge(&purge_config())?;
    assert_eq!(plan.cutoff_ms, BASE + 2 * MINUTE);
    assert_eq!(plan.watermark_ms, Some(BASE + 2 * MINUTE));
    let report = maintenance.apply_purge(&plan, &purge_config(), &control())?;
    assert_eq!(
        (report.outcome, report.files_removed, report.rows_removed),
        (Outcome::Committed, 2, 8)
    );
    assert_eq!(rows(root.path())?, sorted(kept.clone()));
    let actions = commit_actions(root.path(), report.version)?;
    let removes: Vec<_> = actions.iter().filter_map(|action| action.get("remove")).collect();
    assert_eq!(removes.len(), 2);
    assert!(removes.iter().all(|remove| remove["dataChange"] == true));
    assert!(actions.iter().all(|action| action.get("add").is_none()));
    assert_eq!(maintenance.lookup_txn(PURGE_WATERMARK)?, Some(BASE + 2 * MINUTE));

    let replay = append(&table, 1, 900..901);
    assert!(format!("{:#}", replay.err().context("expired replay must fail")?).contains("purge fence"));
    append(&table, 3, 910..911)?;
    kept.extend(expected(910..911, &minute(3)));
    let version = maintenance.version()?;
    assert_eq!(
        maintenance.apply_purge(&plan, &purge_config(), &control())?.outcome,
        Outcome::AlreadyApplied
    );
    let empty = maintenance.plan_purge(&purge_config())?;
    assert!(empty.partitions.is_empty() && empty.watermark_ms.is_none());
    assert_eq!(
        maintenance.apply_purge(&empty, &purge_config(), &control())?.outcome,
        Outcome::Noop
    );
    assert_eq!(maintenance.version()?, version, "no watermark-only or bogus commits");
    assert_eq!(rows(root.path())?, sorted(kept));
    Ok(())
}

/// Scenario: an append lands inside the expired predicate after PURGE planned.
/// Guarantees: the stale plan fails instead of silently purging a changed subset;
/// a replan includes the new file; the newest partition is never purged.
#[test]
fn purge_rejects_concurrent_adds_into_expired_partitions() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    append(&table, 0, 0..2)?;
    append(&table, 1, 10..12)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let protected = maintenance.plan_purge(&purge_config())?;
    assert_eq!(
        protected.partitions.len(),
        1,
        "newest partition 01 is protected even though expired"
    );
    assert_eq!(protected.watermark_ms, Some(BASE + MINUTE));
    append(&table, 3, 30..32)?;
    let plan = maintenance.plan_purge(&purge_config())?;
    assert_eq!(plan.partitions.len(), 2);
    append(&table, 0, 40..42)?;
    let version = maintenance.version()?;
    assert_eq!(
        kind(maintenance.apply_purge(&plan, &purge_config(), &control())),
        Some(MaintenanceErrorKind::StalePlan)
    );
    assert_eq!(
        kind(maintenance.apply_purge(&protected, &purge_config(), &control())),
        Some(MaintenanceErrorKind::StalePlan)
    );
    assert_eq!(maintenance.version()?, version);
    let fresh = maintenance.plan_purge(&purge_config())?;
    assert_eq!(
        fresh
            .partitions
            .iter()
            .map(|partition| partition.files.len())
            .sum::<usize>(),
        3
    );
    assert_eq!(
        maintenance
            .apply_purge(&fresh, &purge_config(), &control())?
            .files_removed,
        3
    );
    assert_eq!(rows(root.path())?, expected(30..32, &minute(3)));
    Ok(())
}

struct BarrierFence {
    reached: Mutex<Option<Sender<()>>>,
    release: Mutex<Option<Receiver<()>>>,
    calls: AtomicUsize,
}

impl PublicationFence for BarrierFence {
    fn check_before_commit(
        &self,
        _context: &PublicationFenceContext<'_>,
    ) -> std::result::Result<(), PublicationFenceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let reached = self.reached.lock().ok().and_then(|mut slot| slot.take());
        if let Some(reached) = reached {
            reached
                .send(())
                .map_err(|_| PublicationFenceError::check_failed("barrier closed"))?;
            let release = self.release.lock().ok().and_then(|mut slot| slot.take());
            if let Some(release) = release {
                release
                    .recv_timeout(Duration::from_secs(60))
                    .map_err(|_| PublicationFenceError::check_failed("barrier release timed out"))?;
            }
        }
        Ok(())
    }
}

/// Scenario: an expired append passes its final watermark check and blocks just before
/// Kernel publication while PURGE commits the watermark.
/// Guarantees: the append's single-CAS commit conflicts, its retry refreshes the snapshot
/// and watermark, and the expired replay is rejected rather than resurrecting data.
#[test]
fn purge_fences_append_racing_past_its_watermark_check() -> Result<()> {
    let root = tempfile::tempdir()?;
    let shared = create(root.path())?;
    for index in 0..4 {
        append(&shared, index, index * 10..index * 10 + 2)?;
    }
    drop(shared);
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let fence = Arc::new(BarrierFence {
        reached: Mutex::new(Some(reached_tx)),
        release: Mutex::new(Some(release_rx)),
        calls: AtomicUsize::new(0),
    });
    let collector = DeltaTable::open_with_fence(root.path(), Arc::clone(&fence) as Arc<dyn PublicationFence>)?;
    let append_thread = std::thread::spawn(move || append(&collector, 0, 500..510));
    reached_rx.recv_timeout(Duration::from_secs(60))?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let plan = maintenance.plan_purge(&purge_config())?;
    let report = maintenance.apply_purge(&plan, &purge_config(), &control())?;
    assert_eq!(report.outcome, Outcome::Committed);
    release_tx.send(())?;
    let result = append_thread.join().map_err(|_| anyhow!("append thread panicked"))?;
    let error = result.err().context("racing expired append must be fenced")?;
    assert!(format!("{error:#}").contains("purge fence"), "{error:#}");
    assert_eq!(
        fence.calls.load(Ordering::SeqCst),
        1,
        "retry stopped at the refreshed watermark"
    );
    assert_eq!(maintenance.version()?, report.version);
    let mut kept = expected(20..22, &minute(2));
    kept.extend(expected(30..32, &minute(3)));
    assert_eq!(rows(root.path())?, kept);
    Ok(())
}

const CHILD_ROOT: &str = "ARROWCOL_MAINTENANCE_CHILD_ROOT";

fn child_appends(root: &str) -> Result<()> {
    let table = DeltaTable::open_url(url_of(Path::new(root))?, schema(), vec![INGESTION_PARTITION.into()])?;
    for round in 0..12 {
        append(
            &table,
            if round % 2 == 0 { 1 } else { 3 },
            1000 + round * 10..1003 + round * 10,
        )?;
        if round == 0 {
            fs::write(Path::new(root).join("child-started"), b"1")?;
        }
    }
    Ok(())
}

/// Scenario: a separate OS process appends (including into an optimized partition)
/// while OPTIMIZE commits.
/// Guarantees: collisions are retried against revalidated state and no row is lost or duplicated.
#[test]
fn optimize_concurrent_with_separate_process_appends() -> Result<()> {
    if let Ok(root) = std::env::var(CHILD_ROOT) {
        return child_appends(&root);
    }
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    let mut all = Vec::new();
    for index in 0..4 {
        for part in 0..3 {
            let ids = index * 100 + part * 10..index * 100 + part * 10 + 5;
            append(&table, index, ids.clone())?;
            all.extend(expected(ids, &minute(index)));
        }
    }
    for round in 0..12 {
        let index = if round % 2 == 0 { 1 } else { 3 };
        all.extend(expected(1000 + round * 10..1003 + round * 10, &minute(index)));
    }
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let plan = maintenance.plan_optimize(&optimize_config())?;
    assert_eq!(plan.bins.len(), 3);
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "optimize_concurrent_with_separate_process_appends",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ROOT, root.path())
        .spawn()?;
    let started = Instant::now();
    while !root.path().join("child-started").exists() {
        if started.elapsed() > Duration::from_secs(120) {
            child.kill()?;
            bail!("child never started appending");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let report = maintenance.apply_optimize(&plan, &optimize_config(), &control())?;
    assert!(child.wait()?.success(), "child appends failed");
    assert_eq!(report.outcome, Outcome::Committed);
    assert_eq!(rows(root.path())?, sorted(all));
    Ok(())
}

/// Scenario: tombstones from OPTIMIZE and PURGE, an orphan file and repeated VACUUM LITE.
/// Guarantees: the 7-day floor and disposable-root proof are enforced, only expired
/// tombstones absent from the active set are deleted, unpublished files survive,
/// no Delta commit is written, and repeats treat NotFound as already absent.
#[test]
fn vacuum_lite_deletes_only_expired_tombstones() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    for index in 0..4 {
        append(&table, index, index * 10..index * 10 + 2)?;
        append(&table, index, index * 10 + 5..index * 10 + 7)?;
    }
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let optimize = maintenance.plan_optimize(&OptimizeConfig {
        latest_partitions: Some(1),
        ..optimize_config()
    })?;
    maintenance.apply_optimize(&optimize, &optimize_config(), &control())?;
    let purge = maintenance.plan_purge(&purge_config())?;
    maintenance.apply_purge(&purge, &purge_config(), &control())?;
    let tombstoned: BTreeSet<String> = optimize
        .bins
        .iter()
        .flat_map(|bin| &bin.files)
        .chain(purge.partitions.iter().flat_map(|partition| &partition.files))
        .map(|file| file.path.clone())
        .collect();
    assert_eq!(tombstoned.len(), 6);
    let orphan = root
        .path()
        .join(INGESTION_PARTITION.to_owned() + "=" + &minute(3))
        .join("part-orphan.parquet");
    fs::write(&orphan, b"unpublished")?;
    let rows_before = rows(root.path())?;
    let version = maintenance.version()?;
    let now = now_ms()? + 1_000;

    let safe = maintenance.plan_vacuum(&VacuumConfig::new(7 * 24 * 3_600_000, now))?;
    assert!(safe.candidates.is_empty());
    assert_eq!(safe.retained_tombstones, 6);
    let mut short = VacuumConfig::new(0, now);
    assert_eq!(
        kind(maintenance.plan_vacuum(&short)),
        Some(MaintenanceErrorKind::UnsafeRequest)
    );
    short.unsafe_allow_short_retention_for_tests = true;
    assert_eq!(
        kind(maintenance.plan_vacuum(&short)),
        Some(MaintenanceErrorKind::UnsafeRequest),
        "marker required"
    );
    fs::write(root.path().join(DISPOSABLE_TEST_MARKER), b"")?;
    let plan = maintenance.plan_vacuum(&short)?;
    let candidates: BTreeSet<_> = plan.candidates.iter().map(|candidate| candidate.path.clone()).collect();
    assert_eq!(candidates, tombstoned);
    let serialized = serde_json::to_string(&MaintenancePlan::Vacuum(plan.clone()))?;
    assert!(matches!(serde_json::from_str(&serialized)?, MaintenancePlan::Vacuum(_)));
    let report = maintenance.apply_vacuum(&plan, &short, &control())?;
    assert_eq!(
        (report.outcome, report.files_deleted, report.files_already_absent),
        (Outcome::Deleted, 6, 0)
    );
    for path in &tombstoned {
        assert!(!data_path(root.path(), path)?.exists(), "{path} deleted");
    }
    assert!(orphan.exists(), "unknown files are never deleted");
    assert_eq!(maintenance.version()?, version, "VACUUM writes no Delta commit");
    assert_eq!(rows(root.path())?, rows_before);
    let repeat = maintenance.apply_vacuum(&maintenance.plan_vacuum(&short)?, &short, &control())?;
    assert_eq!((repeat.files_deleted, repeat.files_already_absent), (0, 6));

    let cancel = Arc::new(AtomicBool::new(true));
    let cancelled = maintenance.apply_vacuum(&plan, &short, &MaintenanceControl::new(cancel, None))?;
    assert_eq!((cancelled.outcome, cancelled.interrupted), (Outcome::Partial, true));
    Ok(())
}

/// Scenario: history whose first JSON commit is gone (checkpoint-only replay),
/// and a log containing a traversal tombstone.
/// Guarantees: VACUUM LITE refuses visibly instead of falling back to directory listing.
#[test]
fn vacuum_rejects_incomplete_history_and_unsafe_paths() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    append(&table, 0, 0..2)?;
    append(&table, 1, 2..4)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let config = VacuumConfig::new(7 * 24 * 3_600_000, now_ms()?);
    maintenance.plan_vacuum(&config)?;
    let version = table.version()?;
    let outside = root.path().join("outside.parquet");
    fs::write(&outside, b"x")?;
    fs::write(
        root.path().join("_delta_log").join(format!("{:020}.json", version + 1)),
        "{\"remove\":{\"path\":\"a/../../outside.parquet\",\"deletionTimestamp\":1,\"dataChange\":true}}\n",
    )?;
    assert_eq!(
        kind(maintenance.plan_vacuum(&config)),
        Some(MaintenanceErrorKind::UnsafeRequest)
    );
    assert!(outside.exists());

    let other = tempfile::tempdir()?;
    let table = create(other.path())?;
    append(&table, 0, 0..2)?;
    append(&table, 1, 2..4)?;
    table.checkpoint()?;
    fs::remove_file(other.path().join("_delta_log").join(format!("{:020}.json", 0)))?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(other.path())?)?;
    assert_eq!(
        kind(maintenance.plan_vacuum(&config)),
        Some(MaintenanceErrorKind::Unsupported)
    );
    Ok(())
}
