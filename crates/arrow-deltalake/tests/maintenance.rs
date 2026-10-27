#![cfg(feature = "delta")]
//! Maintenance evidence against real Delta Kernel tables and Parquet files.

use anyhow::{Context, Result, anyhow, bail};
use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_deltalake::delta::{
    DeltaTable, INGESTION_PARTITION, PURGE_WATERMARK, PublicationFence, PublicationFenceContext, PublicationFenceError,
};
use arrow_deltalake::layout::LayoutPolicy;
use arrow_deltalake::maintenance::{
    DISPOSABLE_TEST_MARKER, Granularity, LogFloor, MaintenanceControl, MaintenanceErrorKind, MaintenancePlan,
    OptimizeConfig, Outcome, PruneConfig, PurgeConfig, VacuumConfig, error_kind, now_ms,
};
use async_trait::async_trait;
use delta_kernel::object_store::{
    self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, local::LocalFileSystem,
};
use futures::stream::BoxStream;
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
    rows_of(root, active_files(root)?)
}

/// Sorted row multiset read from the given (path -> partition) Parquet files.
fn rows_of(root: &Path, active: BTreeMap<String, String>) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for (relative, partition) in active {
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

const DAY: i64 = 24 * 3_600_000;

fn parquet_files(root: &Path, partition: &str) -> Result<usize> {
    let directory = root.join(format!("{INGESTION_PARTITION}={partition}"));
    Ok(fs::read_dir(directory)?
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|extension| extension == "parquet"))
        .count())
}

/// Scenario: a PURGE plan applied after retention grew, and forged plan cutoffs/anchors.
/// Guarantees: the cutoff is bound to the plan's recorded anchor/retention and is never
/// later than the apply configuration allows; rejected plans remove nothing and never
/// advance the purge watermark; the honest plan still applies.
#[test]
fn purge_plan_cutoff_is_bound_to_recorded_and_current_policy() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    let mut all = Vec::new();
    for index in 0..4 {
        append(&table, index, index * 10..index * 10 + 3)?;
        all.extend(expected(index * 10..index * 10 + 3, &minute(index)));
    }
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let plan = maintenance.plan_purge(&purge_config())?;
    assert_eq!(plan.cutoff_ms, BASE + 2 * MINUTE);
    let version = maintenance.version()?;
    let unchanged = |label: &str| -> Result<()> {
        assert_eq!(maintenance.version()?, version, "{label}: version");
        assert_eq!(maintenance.lookup_txn(PURGE_WATERMARK)?, None, "{label}: watermark");
        assert_eq!(rows(root.path())?, sorted(all.clone()), "{label}: rows");
        Ok(())
    };

    // Retention grew from 90 s to 150 s: the current cutoff is 00:01, so 00:01 is retained.
    let longer = PurgeConfig::new(150_000, purge_config().anchor_ms, Granularity::Minute);
    assert_eq!(
        kind(maintenance.apply_purge(&plan, &longer, &control())),
        Some(MaintenanceErrorKind::StalePlan)
    );
    unchanged("increased retention")?;

    let mut forged = plan.clone();
    forged.cutoff_ms = BASE + 3 * MINUTE;
    assert_eq!(
        kind(maintenance.apply_purge(&forged, &purge_config(), &control())),
        Some(MaintenanceErrorKind::InvalidPlan)
    );
    unchanged("tampered cutoff")?;

    // Internally consistent but shorter recorded retention: later than the config allows.
    let mut shorter = plan.clone();
    shorter.retention_ms = 30_000;
    shorter.cutoff_ms = BASE + 3 * MINUTE;
    assert_eq!(
        kind(maintenance.apply_purge(&shorter, &purge_config(), &control())),
        Some(MaintenanceErrorKind::StalePlan)
    );
    unchanged("forged shorter retention")?;

    let mut future = plan.clone();
    future.anchor_ms = now_ms()? + DAY;
    future.cutoff_ms = Granularity::Minute.floor_ms(future.anchor_ms - future.retention_ms);
    assert_eq!(
        kind(maintenance.apply_purge(&future, &purge_config(), &control())),
        Some(MaintenanceErrorKind::InvalidPlan)
    );
    let future_config = PurgeConfig::new(90_000, now_ms()? + DAY, Granularity::Minute);
    assert_eq!(
        kind(maintenance.plan_purge(&future_config)),
        Some(MaintenanceErrorKind::InvalidPlan)
    );
    unchanged("future anchor")?;

    let report = maintenance.apply_purge(&plan, &purge_config(), &control())?;
    assert_eq!((report.outcome, report.files_removed), (Outcome::Committed, 2));
    assert_eq!(maintenance.lookup_txn(PURGE_WATERMARK)?, Some(BASE + 2 * MINUTE));
    Ok(())
}

/// Scenario: a short-retention VACUUM plan applied under a longer retention, and forged
/// plan cutoff/retention/anchor values.
/// Guarantees: only tombstones older than BOTH the plan's recorded cutoff and the current
/// configuration's cutoff are deleted; every rejection happens before any delete.
#[test]
fn vacuum_plan_cutoff_is_bound_to_recorded_and_current_policy() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    append(&table, 0, 0..2)?;
    append(&table, 0, 5..7)?;
    append(&table, 1, 10..12)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let optimize = maintenance.plan_optimize(&optimize_config())?;
    maintenance.apply_optimize(&optimize, &optimize_config(), &control())?;
    let tombstones: Vec<PathBuf> = optimize
        .bins
        .iter()
        .flat_map(|bin| &bin.files)
        .map(|file| data_path(root.path(), &file.path))
        .collect::<Result<_>>()?;
    assert_eq!(tombstones.len(), 2);
    fs::write(root.path().join(DISPOSABLE_TEST_MARKER), b"")?;
    let now = now_ms()? + 1_000;
    let mut short = VacuumConfig::new(0, now);
    short.unsafe_allow_short_retention_for_tests = true;
    let plan = maintenance.plan_vacuum(&short)?;
    assert_eq!((plan.candidates.len(), plan.now_ms, plan.cutoff_ms), (2, now, now));
    let intact = |label: &str| {
        for path in &tombstones {
            assert!(path.exists(), "{label}: {} must not be deleted", path.display());
        }
    };

    let longer = VacuumConfig {
        retention_ms: 3_600_000,
        ..short.clone()
    };
    assert_eq!(
        kind(maintenance.apply_vacuum(&plan, &longer, &control())),
        Some(MaintenanceErrorKind::StalePlan)
    );
    intact("changed retention");
    assert_eq!(
        kind(maintenance.apply_vacuum(&plan, &VacuumConfig::new(7 * DAY, now), &control())),
        Some(MaintenanceErrorKind::UnsafeRequest),
        "a short-retention plan needs the unsafe override at apply time too"
    );
    intact("seven-day configuration");

    let mut forged = plan.clone();
    forged.cutoff_ms += DAY;
    assert_eq!(
        kind(maintenance.apply_vacuum(&forged, &short, &control())),
        Some(MaintenanceErrorKind::InvalidPlan)
    );
    let mut forged = plan.clone();
    forged.retention_ms = 7 * DAY;
    assert_eq!(
        kind(maintenance.apply_vacuum(&forged, &short, &control())),
        Some(MaintenanceErrorKind::InvalidPlan)
    );
    let mut forged = plan.clone();
    forged.now_ms = now + 30 * DAY;
    forged.retention_ms = 7 * DAY;
    forged.cutoff_ms = forged.now_ms - forged.retention_ms;
    assert_eq!(
        kind(maintenance.apply_vacuum(&forged, &short, &control())),
        Some(MaintenanceErrorKind::InvalidPlan)
    );
    let mut negative = plan.clone();
    negative.now_ms = -1;
    negative.cutoff_ms = -1;
    assert_eq!(
        kind(maintenance.apply_vacuum(&negative, &short, &control())),
        Some(MaintenanceErrorKind::InvalidPlan)
    );
    assert_eq!(
        kind(maintenance.plan_vacuum(&VacuumConfig::new(7 * DAY, now + 30 * DAY))),
        Some(MaintenanceErrorKind::InvalidPlan)
    );
    intact("forged plan");

    let report = maintenance.apply_vacuum(&plan, &short, &control())?;
    assert_eq!((report.files_deleted, report.cutoff_ms), (2, Some(now)));
    assert!(tombstones.iter().all(|path| !path.exists()));
    Ok(())
}

fn tiny_layout(max_rows: usize) -> LayoutPolicy {
    LayoutPolicy {
        sort_columns: vec!["id".into()],
        max_rows,
        bloom_columns: Vec::new(),
        ..LayoutPolicy::default()
    }
}

/// Scenario: a constrained sorted layout whose actual outputs do not reduce the file count,
/// then one whose actual outputs exceed the per-commit action budget.
/// Guarantees: OPTIMIZE verifies actual outputs before the transaction, removes rejected
/// rewrites, commits nothing when nothing is beneficial (version and rows unchanged) and
/// commits only the bins that fit; sorting is never disabled to reach a reduction.
#[test]
fn optimize_verifies_actual_outputs_before_committing() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    let mut all = Vec::new();
    for index in 0..3 {
        for part in 0..4 {
            let ids = index * 100 + part * 10..index * 100 + part * 10 + 2;
            append(&table, index, ids.clone())?;
            all.extend(expected(ids, &minute(index)));
        }
    }
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let version = maintenance.version()?;

    // max_rows 2 with 2-row inputs: four inputs always become four outputs.
    let nonbeneficial = OptimizeConfig {
        layout: Some(tiny_layout(2)),
        ..optimize_config()
    };
    let plan = maintenance.plan_optimize(&nonbeneficial)?;
    assert_eq!(plan.bins.len(), 2);
    let report = maintenance.apply_optimize(&plan, &nonbeneficial, &control())?;
    assert_eq!((report.outcome, report.nonbeneficial_bins), (Outcome::Noop, 2));
    assert_eq!((report.files_removed, report.files_added), (0, 0));
    assert_eq!(maintenance.version()?, version);
    assert_eq!(rows(root.path())?, sorted(all.clone()));
    for index in 0..3 {
        assert_eq!(
            parquet_files(root.path(), &minute(index))?,
            4,
            "rejected outputs removed"
        );
    }

    // max_rows 5: four 2-row inputs become two outputs (estimated one), six actions per bin.
    let budget = OptimizeConfig {
        layout: Some(tiny_layout(5)),
        max_actions_per_commit: 10,
        ..optimize_config()
    };
    let plan = maintenance.plan_optimize(&budget)?;
    assert_eq!(plan.bins.len(), 2);
    let report = maintenance.apply_optimize(&plan, &budget, &control())?;
    assert_eq!(report.outcome, Outcome::Committed);
    assert_eq!(
        (report.files_removed, report.files_added, report.backlog_files),
        (4, 2, 4)
    );
    assert_eq!(maintenance.version()?, version + 1);
    assert_eq!(rows(root.path())?, sorted(all.clone()));
    let left = (0..2)
        .map(|index| files_in(root.path(), &minute(index)))
        .collect::<Result<Vec<_>>>()?;
    assert!(left == vec![2, 4] || left == vec![4, 2], "{left:?}");

    let single = OptimizeConfig {
        max_actions_per_commit: 5,
        ..budget
    };
    let plan = maintenance.plan_optimize(&single)?;
    assert_eq!(plan.bins.len(), 1);
    let report = maintenance.apply_optimize(&plan, &single, &control())?;
    assert_eq!((report.outcome, report.backlog_files), (Outcome::Noop, 4));
    assert_eq!(maintenance.version()?, version + 1);
    assert_eq!(rows(root.path())?, sorted(all));
    for (index, files) in (0..2).zip(&left) {
        assert_eq!(
            parquet_files(root.path(), &minute(index))?,
            4 + 2 * usize::from(*files == 2),
            "over-budget outputs removed"
        );
    }
    Ok(())
}

fn customer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
        Field::new(INGESTION_PARTITION, DataType::Utf8, false),
    ]))
}

fn append_customers(table: &Arc<DeltaTable>, index: i64, customers: &[&str], first_id: i64) -> Result<()> {
    let partition = minute(index);
    let rows = customers.len();
    let batch = RecordBatch::try_new(
        customer_schema(),
        vec![
            Arc::new(StringArray::from(customers.to_vec())),
            Arc::new(Int64Array::from_iter_values(first_id..first_id + rows as i64)),
            Arc::new(StringArray::from(vec![partition.as_str(); rows])),
        ],
    )?;
    run(async {
        let mut file = table
            .new_file_partitioned(
                customer_schema(),
                BTreeMap::from([(INGESTION_PARTITION.to_owned(), Some(partition.clone()))]),
            )?
            .with_ingestion_time(BASE + index * MINUTE + 1)?;
        file.write(&batch).await?;
        file.finish().await?;
        Ok::<_, anyhow::Error>(())
    })?
}

/// Scenario: a table with a `customer_id` column compacted with the default
/// (unset) OPTIMIZE layout.
/// Guarantees: the shared publication layout is used, so the compacted file is sorted
/// by customer across the whole file rather than concatenated in input order.
#[test]
fn optimize_default_layout_preserves_whole_file_customer_order() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open_url(
        url_of(root.path())?,
        customer_schema(),
        vec![INGESTION_PARTITION.into()],
    )?;
    append_customers(&table, 0, &["c5", "c1", "c9"], 0)?;
    append_customers(&table, 0, &["c3", "c7", "c2"], 10)?;
    append_customers(&table, 0, &["c8", "c0", "c4"], 20)?;
    append_customers(&table, 1, &["c6"], 30)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let config = optimize_config();
    assert!(config.layout.is_none());
    let plan = maintenance.plan_optimize(&config)?;
    assert_eq!(plan.bins.len(), 1);
    let report = maintenance.apply_optimize(&plan, &config, &control())?;
    assert_eq!(
        (report.outcome, report.files_added, report.rows_added),
        (Outcome::Committed, 1, 9)
    );
    let outputs: Vec<String> = active_files(root.path())?
        .into_iter()
        .filter(|(_, partition)| *partition == minute(0))
        .map(|(path, _)| path)
        .collect();
    assert_eq!(outputs.len(), 1);
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(fs::File::open(data_path(root.path(), &outputs[0])?)?)?.build()?;
    let mut pairs = Vec::new();
    for batch in reader {
        let batch = batch?;
        let customers = arrow::compute::cast(
            batch.column_by_name("customer_id").context("customer")?,
            &DataType::Utf8,
        )?;
        let customers = customers.as_any().downcast_ref::<StringArray>().context("customers")?;
        let ids = batch.column_by_name("id").context("id")?;
        let ids = ids.as_any().downcast_ref::<Int64Array>().context("ids")?;
        for row in 0..batch.num_rows() {
            pairs.push((customers.value(row).to_owned(), ids.value(row)));
        }
    }
    let order: Vec<&str> = pairs.iter().map(|(customer, _)| customer.as_str()).collect();
    assert_eq!(order, ["c0", "c1", "c2", "c3", "c4", "c5", "c7", "c8", "c9"]);
    let mut ids: Vec<i64> = pairs.iter().map(|(_, id)| *id).collect();
    ids.sort_unstable();
    assert_eq!(ids, [0, 1, 2, 10, 11, 12, 20, 21, 22]);
    Ok(())
}

/// Commit-log fault injection for maintenance publication.
#[derive(Debug)]
struct UncertainStore {
    inner: LocalFileSystem,
    target: String,
    unreadable: AtomicBool,
    attempts: AtomicUsize,
}

impl std::fmt::Display for UncertainStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "uncertain-commit store")
    }
}

#[async_trait]
impl ObjectStore for UncertainStore {
    async fn put_opts(
        &self,
        path: &object_store::path::Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if path.as_ref().ends_with(&self.target) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            self.inner.put_opts(path, payload, options).await?;
            return Err(object_store::Error::Generic {
                store: "uncertain_store",
                source: Box::new(std::io::Error::other("injected failure after publishing the log")),
            });
        }
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &object_store::path::Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &object_store::path::Path, options: GetOptions) -> object_store::Result<GetResult> {
        if self.unreadable.load(Ordering::SeqCst)
            && self.attempts.load(Ordering::SeqCst) != 0
            && path.as_ref().ends_with(&self.target)
        {
            return Err(object_store::Error::Generic {
                store: "uncertain_store",
                source: Box::new(std::io::Error::other("injected reconciliation read failure")),
            });
        }
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&object_store::path::Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&object_store::path::Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Scenario: the commit-log write succeeds but the store reports an error, once with a
/// readable log (reconcilable) and once with the new commit unreadable during reconciliation.
/// Guarantees: an uncertain OPTIMIZE publication never deletes its rewrite outputs;
/// a visible plan id resolves as applied, an unreadable outcome returns an explicit
/// uncertain error with every output retained, and no row is lost or duplicated.
#[test]
fn optimize_uncertain_commit_never_discards_outputs() -> Result<()> {
    for unreadable in [false, true] {
        let root = tempfile::tempdir()?;
        let table = create(root.path())?;
        let mut all = Vec::new();
        for (index, ids) in [(0, 0..3), (0, 10..13), (1, 20..23)] {
            append(&table, index, ids.clone())?;
            all.extend(expected(ids, &minute(index)));
        }
        let target = format!("{:020}.json", table.version()? + 1);
        drop(table);
        let store = Arc::new(UncertainStore {
            inner: LocalFileSystem::new(),
            target,
            unreadable: AtomicBool::new(unreadable),
            attempts: AtomicUsize::new(0),
        });
        let maintenance = DeltaTable::open_with_store(root.path(), Arc::clone(&store) as Arc<dyn ObjectStore>)?;
        let plan = maintenance.plan_optimize(&optimize_config())?;
        assert_eq!(plan.bins.len(), 1);
        let result = maintenance.apply_optimize(&plan, &optimize_config(), &control());
        assert!(store.attempts.load(Ordering::SeqCst) >= 1);
        if unreadable {
            let error = result.err().context("unreadable outcome must not report success")?;
            assert_eq!(error_kind(&error), Some(MaintenanceErrorKind::Uncertain), "{error:#}");
            assert!(format!("{error:#}").contains("outputs retained"), "{error:#}");
            store.unreadable.store(false, Ordering::SeqCst);
        } else {
            let report = result?;
            assert!(
                matches!(report.outcome, Outcome::Committed | Outcome::AlreadyApplied),
                "{report:?}"
            );
        }
        assert_eq!(
            parquet_files(root.path(), &minute(0))?,
            3,
            "output retained beside inputs"
        );
        let active = active_files(root.path())?;
        assert_eq!(active.values().filter(|partition| **partition == minute(0)).count(), 1);
        for relative in active.keys() {
            assert!(
                data_path(root.path(), relative)?.exists(),
                "{relative} is active and present"
            );
        }
        assert_eq!(rows(root.path())?, sorted(all));
        assert_eq!(
            maintenance
                .apply_optimize(&plan, &optimize_config(), &control())?
                .outcome,
            Outcome::AlreadyApplied
        );
    }
    Ok(())
}

const PUBLICATIONS: [&str; 2] = ["arrowcol.publication.prune-0", "arrowcol.publication.prune-1"];

fn append_published(table: &Arc<DeltaTable>, index: i64, ids: Range<i64>, app_id: &str) -> Result<u64> {
    run(async {
        let mut file = table
            .new_file_partitioned(
                schema(),
                BTreeMap::from([(INGESTION_PARTITION.to_owned(), Some(minute(index)))]),
            )?
            .with_ingestion_time(BASE + index * MINUTE + 1)?
            .with_transaction(app_id.to_owned(), 1)?;
        file.write(&batch(ids, &minute(index))?).await?;
        Ok::<_, anyhow::Error>(file.finish().await?.version)
    })?
}

fn json_path(root: &Path, version: u64) -> PathBuf {
    root.join("_delta_log").join(format!("{version:020}.json"))
}

fn checkpoint_path(root: &Path, version: u64) -> PathBuf {
    root.join("_delta_log")
        .join(format!("{version:020}.checkpoint.parquet"))
}

fn short_vacuum(now: i64) -> VacuumConfig {
    let mut config = VacuumConfig::new(0, now);
    config.unsafe_allow_short_retention_for_tests = true;
    config
}

fn short_prune(now: i64) -> PruneConfig {
    PruneConfig {
        log_retention_ms: Some(0),
        unsafe_allow_short_retention_for_tests: true,
        ..PruneConfig::new(now)
    }
}

/// Txn versions the pruning proof must preserve.
fn txn_versions(table: &DeltaTable) -> Result<Vec<Option<i64>>> {
    PUBLICATIONS
        .iter()
        .chain([&PURGE_WATERMARK])
        .map(|app_id| table.lookup_txn(app_id))
        .collect()
}

/// Active (path -> partition) after pruning: the hinted checkpoint's adds plus later JSON.
fn pruned_active(root: &Path) -> Result<BTreeMap<String, String>> {
    use arrow::array::AsArray;
    let hint: serde_json::Value = serde_json::from_slice(&fs::read(root.join("_delta_log").join("_last_checkpoint"))?)?;
    let checkpoint = hint["version"].as_u64().context("hint version")?;
    let mut active = BTreeMap::new();
    for batch in
        ParquetRecordBatchReaderBuilder::try_new(fs::File::open(checkpoint_path(root, checkpoint))?)?.build()?
    {
        let batch = batch?;
        let add = batch.column_by_name("add").context("add")?.as_struct();
        let paths = add.column_by_name("path").context("add.path")?.as_string::<i32>();
        for row in 0..batch.num_rows() {
            if add.is_valid(row) {
                let path = paths.value(row).to_owned();
                let partition = path
                    .split('/')
                    .next()
                    .and_then(|segment| segment.strip_prefix(&format!("{INGESTION_PARTITION}=")))
                    .context("partition segment")?
                    .to_owned();
                active.insert(path, partition);
            }
        }
    }
    let mut version = checkpoint + 1;
    while json_path(root, version).is_file() {
        for action in commit_actions(root, version)? {
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
        version += 1;
    }
    Ok(active)
}

/// Appends with two publication txns, OPTIMIZE and PURGE (tombstones plus the
/// purge watermark), and a marked disposable root. Returns the maintenance handle.
fn prunable_history(root: &Path) -> Result<(Arc<DeltaTable>, Arc<DeltaTable>)> {
    let table = create(root)?;
    for index in 0..4 {
        append_published(
            &table,
            index,
            index * 10..index * 10 + 2,
            PUBLICATIONS[usize::try_from(index % 2)?],
        )?;
        append(&table, index, index * 10 + 5..index * 10 + 7)?;
    }
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root)?)?;
    let optimize = maintenance.plan_optimize(&OptimizeConfig {
        latest_partitions: Some(1),
        ..optimize_config()
    })?;
    maintenance.apply_optimize(&optimize, &optimize_config(), &control())?;
    let purge = maintenance.plan_purge(&purge_config())?;
    maintenance.apply_purge(&purge, &purge_config(), &control())?;
    fs::write(root.join(DISPOSABLE_TEST_MARKER), b"")?;
    Ok((table, maintenance))
}

/// VACUUM LITE apply (writes progress at the vacuum read version) then a checkpoint there.
fn vacuum_and_checkpoint(maintenance: &DeltaTable) -> Result<u64> {
    let now = now_ms()? + 1_000;
    let plan = maintenance.plan_vacuum(&short_vacuum(now))?;
    let report = maintenance.apply_vacuum(&plan, &short_vacuum(now), &control())?;
    assert!(matches!(report.outcome, Outcome::Deleted | Outcome::Noop));
    let progress = maintenance.vacuum_lite_progress()?.context("progress recorded")?;
    assert_eq!(
        progress.version, plan.header.read_version,
        "progress records the scanned version"
    );
    let checkpoint = maintenance.checkpoint_if_due(1, true)?;
    assert_eq!(checkpoint.outcome, Outcome::Checkpointed);
    Ok(checkpoint.version)
}

/// Scenario: publications, OPTIMIZE and PURGE tombstones, VACUUM LITE with a test
/// retention, a checkpoint, then a dry run, an apply and later maintenance.
/// Guarantees: the dry run deletes nothing; the apply deletes exactly JSON `0..=C`
/// after writing the table-bound floor, keeps checkpoint C and `_last_checkpoint`,
/// preserves every txn (publications and purge watermark), rows and the active set;
/// appends, OPTIMIZE, VACUUM LITE and a second, floor-seeded prune keep working,
/// and repeats are Noop.
#[test]
fn prune_log_deletes_proven_history_and_keeps_table_state() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (table, maintenance) = prunable_history(root.path())?;
    let txns = txn_versions(&maintenance)?;
    assert!(txns.iter().all(Option::is_some), "{txns:?}");
    let checkpoint = vacuum_and_checkpoint(&maintenance)?;
    let active = active_files(root.path())?;
    let rows_before = rows(root.path())?;
    let json_before = log_files(root.path())?;

    let config = short_prune(now_ms()? + 1_000);
    assert_eq!(
        kind(maintenance.plan_prune_log(&PruneConfig {
            unsafe_allow_short_retention_for_tests: false,
            ..config.clone()
        })),
        Some(MaintenanceErrorKind::UnsafeRequest)
    );
    let plan = maintenance.plan_prune_log(&config)?;
    assert_eq!(plan.checkpoint_version, Some(checkpoint));
    let expected: Vec<String> = (0..=checkpoint)
        .map(|version| format!("_delta_log/{version:020}.json"))
        .collect();
    assert_eq!(plan.deletions, expected);
    let dry = plan.report();
    assert_eq!((dry.outcome, dry.files_removed), (Outcome::Planned, checkpoint + 1));
    assert_eq!(log_files(root.path())?, json_before, "dry run deletes nothing");
    assert_eq!(maintenance.log_floor()?, None, "dry run writes no floor");

    let report = maintenance.apply_prune_log(&plan, &config, &control())?;
    assert_eq!(
        (report.outcome, report.files_deleted, report.version),
        (Outcome::Pruned, checkpoint + 1, checkpoint)
    );
    assert!(log_files(root.path())?.is_empty(), "JSON 0..=C pruned");
    assert!(checkpoint_path(root.path(), checkpoint).is_file());
    assert!(root.path().join("_delta_log").join("_last_checkpoint").is_file());
    let floor = maintenance.log_floor()?.context("floor")?;
    assert_eq!(floor.checkpoint_version, checkpoint);
    assert_eq!(maintenance.version()?, checkpoint, "pruning writes no Delta commit");
    assert_eq!(txn_versions(&maintenance)?, txns);
    let reopened = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    assert_eq!(txn_versions(&reopened)?, txns);
    assert_eq!(pruned_active(root.path())?, active);
    assert_eq!(rows_of(root.path(), pruned_active(root.path())?)?, rows_before);
    assert_eq!(
        maintenance
            .plan_prune_log(&short_prune(now_ms()? + 1_000))?
            .report()
            .outcome,
        Outcome::Noop,
        "repeat prune"
    );

    // Later appends, OPTIMIZE and VACUUM LITE (seeded from the floor checkpoint).
    append(&table, 4, 40..42)?;
    append(&table, 4, 45..47)?;
    append(&table, 5, 50..51)?;
    let optimize = reopened.plan_optimize(&OptimizeConfig {
        latest_partitions: Some(1),
        ..optimize_config()
    })?;
    let optimized = reopened.apply_optimize(&optimize, &optimize_config(), &control())?;
    assert_eq!(optimized.outcome, Outcome::Committed);
    let rows_later = rows_of(root.path(), pruned_active(root.path())?)?;
    let second = vacuum_and_checkpoint(&reopened)?;
    assert!(second > checkpoint);
    let again = reopened.plan_prune_log(&short_prune(now_ms()? + 1_000))?;
    assert_eq!(again.checkpoint_version, Some(second));
    let mut expected: Vec<String> = (checkpoint + 1..=second)
        .map(|version| format!("_delta_log/{version:020}.json"))
        .collect();
    expected.insert(0, format!("_delta_log/{checkpoint:020}.checkpoint.parquet"));
    assert_eq!(again.deletions, expected);
    let report = reopened.apply_prune_log(&again, &short_prune(now_ms()? + 1_000), &control())?;
    assert_eq!(report.outcome, Outcome::Pruned);
    assert!(!checkpoint_path(root.path(), checkpoint).exists());
    assert!(checkpoint_path(root.path(), second).is_file());
    assert_eq!(
        reopened.log_floor()?.map(|floor| floor.checkpoint_version),
        Some(second)
    );
    assert_eq!(txn_versions(&reopened)?, txns);
    assert_eq!(rows_of(root.path(), pruned_active(root.path())?)?, rows_later);
    append(&table, 6, 60..62)?;
    reopened.plan_vacuum(&short_vacuum(now_ms()? + 1_000))?;
    Ok(())
}

/// Scenario: a checkpoint whose removes VACUUM LITE has not processed: no progress
/// record, progress older than the checkpoint, or removes newer than the progress cutoff.
/// Guarantees: pruning is Noop and nothing is deleted.
#[test]
fn prune_log_waits_for_vacuum_progress() -> Result<()> {
    let no_progress = tempfile::tempdir()?;
    let (_, maintenance) = prunable_history(no_progress.path())?;
    maintenance.checkpoint_if_due(1, true)?;
    let before = log_files(no_progress.path())?;
    let config = short_prune(now_ms()? + 1_000);
    let plan = maintenance.plan_prune_log(&config)?;
    assert_eq!((plan.checkpoint_version, plan.report().outcome), (None, Outcome::Noop));
    assert_eq!(
        maintenance.apply_prune_log(&plan, &config, &control())?.outcome,
        Outcome::Noop
    );
    assert_eq!(log_files(no_progress.path())?, before);

    let stale = tempfile::tempdir()?;
    let (table, maintenance) = prunable_history(stale.path())?;
    let now = now_ms()? + 1_000;
    maintenance.apply_vacuum(
        &maintenance.plan_vacuum(&short_vacuum(now))?,
        &short_vacuum(now),
        &control(),
    )?;
    append(&table, 3, 30..31)?;
    maintenance.checkpoint_if_due(1, true)?;
    let before = log_files(stale.path())?;
    let plan = maintenance.plan_prune_log(&short_prune(now_ms()? + 1_000))?;
    assert_eq!(plan.report().outcome, Outcome::Noop, "progress V < C");
    assert_eq!(log_files(stale.path())?, before);

    let early = tempfile::tempdir()?;
    let (_, maintenance) = prunable_history(early.path())?;
    // A cutoff before the OPTIMIZE/PURGE removes: nothing is vacuumed, progress T is too old.
    let anchor = now_ms()? - 120_000;
    let plan = maintenance.plan_vacuum(&short_vacuum(anchor))?;
    assert!(plan.candidates.is_empty());
    maintenance.apply_vacuum(&plan, &short_vacuum(anchor), &control())?;
    let progress = maintenance.vacuum_lite_progress()?.context("progress")?;
    assert_eq!(progress.cutoff_ms, anchor);
    maintenance.checkpoint_if_due(1, true)?;
    let before = log_files(early.path())?;
    let plan = maintenance.plan_prune_log(&short_prune(now_ms()? + 1_000))?;
    assert_eq!(plan.report().outcome, Outcome::Noop, "remove stamps >= T");
    assert_eq!(log_files(early.path())?, before);
    Ok(())
}

/// Scenario: a fully provable checkpoint under the default and a one-day log retention.
/// Guarantees: young commits are never pruned.
#[test]
fn prune_log_respects_log_retention() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (_, maintenance) = prunable_history(root.path())?;
    vacuum_and_checkpoint(&maintenance)?;
    let before = log_files(root.path())?;
    let now = now_ms()? + 1_000;
    let default = maintenance.plan_prune_log(&PruneConfig::new(now))?;
    assert_eq!(default.log_retention_ms, 30 * DAY);
    assert_eq!(default.report().outcome, Outcome::Noop);
    let day = maintenance.plan_prune_log(&PruneConfig {
        log_retention_ms: Some(DAY),
        ..short_prune(now)
    })?;
    assert_eq!((day.checkpoint_version, day.report().outcome), (None, Outcome::Noop));
    assert_eq!(log_files(root.path())?, before);
    Ok(())
}

/// Scenario: an apply cancelled before deleting, then a bounded apply that writes the
/// floor and deletes only two JSON commits (a crash mid-prune).
/// Guarantees: cancellation deletes nothing; with partial history VACUUM LITE,
/// OPTIMIZE and checkpointing still work, and a fresh plan completes the prune.
#[test]
fn prune_log_recovers_from_partial_apply() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (table, maintenance) = prunable_history(root.path())?;
    let txns = txn_versions(&maintenance)?;
    let checkpoint = vacuum_and_checkpoint(&maintenance)?;
    let rows_before = rows(root.path())?;
    let config = short_prune(now_ms()? + 1_000);
    let plan = maintenance.plan_prune_log(&config)?;
    let cancelled = maintenance.apply_prune_log(
        &plan,
        &config,
        &MaintenanceControl::new(Arc::new(AtomicBool::new(true)), None),
    )?;
    assert_eq!((cancelled.outcome, cancelled.files_deleted), (Outcome::Partial, 0));
    assert!(json_path(root.path(), 0).is_file());

    let bounded = PruneConfig {
        max_prune_files: 2,
        ..config.clone()
    };
    let partial = maintenance.plan_prune_log(&bounded)?;
    assert_eq!(partial.deletions.len(), 2);
    assert_eq!(partial.backlog_files, checkpoint - 1);
    assert_eq!(
        maintenance
            .apply_prune_log(&partial, &bounded, &control())?
            .files_deleted,
        2
    );
    assert!(!json_path(root.path(), 1).exists());
    assert!(json_path(root.path(), 2).is_file());
    assert_eq!(
        maintenance.log_floor()?,
        Some(LogFloor {
            table_id: partial.header.fingerprint.table_id.clone(),
            checkpoint_version: checkpoint,
        })
    );

    let vacuum = maintenance.plan_vacuum(&short_vacuum(now_ms()? + 1_000))?;
    assert_eq!(vacuum.header.read_version, checkpoint);
    append(&table, 4, 40..42)?;
    assert_eq!(maintenance.checkpoint_if_due(100, false)?.outcome, Outcome::Noop);

    let complete = maintenance.plan_prune_log(&short_prune(now_ms()? + 1_000))?;
    assert_eq!(complete.checkpoint_version, Some(checkpoint));
    assert_eq!(complete.deletions.len(), usize::try_from(checkpoint - 1)?);
    let report = maintenance.apply_prune_log(&complete, &short_prune(now_ms()? + 1_000), &control())?;
    assert_eq!(report.outcome, Outcome::Pruned);
    assert_eq!(log_files(root.path())?, vec![json_path(root.path(), checkpoint + 1)]);
    assert_eq!(txn_versions(&maintenance)?, txns);
    let mut expected = rows_before;
    expected.extend(expected_rows(4, 40..42));
    assert_eq!(rows_of(root.path(), pruned_active(root.path())?)?, sorted(expected));
    assert_eq!(
        maintenance
            .plan_prune_log(&short_prune(now_ms()? + 1_000))?
            .report()
            .outcome,
        Outcome::Noop
    );
    Ok(())
}

fn expected_rows(index: i64, ids: Range<i64>) -> Vec<Row> {
    expected(ids, &minute(index))
}

/// Scenario: JSON missing above the log floor (behind a newer checkpoint Kernel can
/// load from), and pruned history whose floor record was lost.
/// Guarantees: VACUUM LITE refuses visibly with Unsupported and pruning deletes nothing.
#[test]
fn prune_log_unexplained_gaps_are_unsupported() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (table, maintenance) = prunable_history(root.path())?;
    let checkpoint = vacuum_and_checkpoint(&maintenance)?;
    let config = short_prune(now_ms()? + 1_000);
    maintenance.apply_prune_log(&maintenance.plan_prune_log(&config)?, &config, &control())?;
    for index in 4..7 {
        append(&table, index, index * 10..index * 10 + 1)?;
    }
    maintenance.checkpoint_if_due(1, true)?;
    fs::remove_file(json_path(root.path(), checkpoint + 1))?;
    let vacuum = short_vacuum(now_ms()? + 1_000);
    assert_eq!(
        kind(maintenance.plan_vacuum(&vacuum)),
        Some(MaintenanceErrorKind::Unsupported)
    );
    // Progress still ends at the floor, so pruning has nothing provable and deletes nothing.
    let before = log_files(root.path())?;
    assert_eq!(
        maintenance
            .plan_prune_log(&short_prune(now_ms()? + 1_000))?
            .report()
            .outcome,
        Outcome::Noop
    );
    assert_eq!(log_files(root.path())?, before);

    let lost = tempfile::tempdir()?;
    let (_, maintenance) = prunable_history(lost.path())?;
    vacuum_and_checkpoint(&maintenance)?;
    let config = short_prune(now_ms()? + 1_000);
    maintenance.apply_prune_log(&maintenance.plan_prune_log(&config)?, &config, &control())?;
    fs::remove_file(lost.path().join("_arrowcol_maintenance").join("log-floor.json"))?;
    assert_eq!(
        kind(maintenance.plan_vacuum(&vacuum)),
        Some(MaintenanceErrorKind::Unsupported)
    );
    Ok(())
}

/// Scenario: a checkpoint rewritten without one publication txn row.
/// Guarantees: the independent JSON replay disagrees, pruning refuses with
/// Unsupported, and nothing is deleted.
#[test]
fn prune_log_refuses_checkpoint_missing_a_txn() -> Result<()> {
    use arrow::array::{AsArray, BooleanArray};
    let root = tempfile::tempdir()?;
    let (_, maintenance) = prunable_history(root.path())?;
    let checkpoint = vacuum_and_checkpoint(&maintenance)?;
    let path = checkpoint_path(root.path(), checkpoint);
    let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&path)?)?;
    let schema = Arc::clone(reader.schema());
    let mut kept = Vec::new();
    let mut dropped = 0;
    for batch in reader.build()? {
        let batch = batch?;
        let txn = batch.column_by_name("txn").context("txn")?.as_struct();
        let app_ids = txn.column_by_name("appId").context("appId")?.as_string::<i32>();
        let keep: BooleanArray = (0..batch.num_rows())
            .map(|row| Some(!(txn.is_valid(row) && app_ids.value(row) == PUBLICATIONS[0])))
            .collect();
        dropped += keep.false_count();
        kept.push(arrow::compute::filter_record_batch(&batch, &keep)?);
    }
    assert_eq!(dropped, 1);
    let mut writer = parquet::arrow::ArrowWriter::try_new(fs::File::create(&path)?, schema, None)?;
    for batch in &kept {
        writer.write(batch)?;
    }
    writer.close()?;
    let before = log_files(root.path())?;
    assert_eq!(
        kind(maintenance.plan_prune_log(&short_prune(now_ms()? + 1_000))),
        Some(MaintenanceErrorKind::Unsupported)
    );
    assert_eq!(log_files(root.path())?, before);
    assert_eq!(maintenance.log_floor()?, None);
    Ok(())
}

/// Rewrite every UTF-8 value at `path` (struct fields; `value` selects map
/// values) for which `edit` returns a replacement.
fn edit_strings(
    array: &arrow::array::ArrayRef,
    path: &[&str],
    edit: &mut dyn FnMut(&str) -> Option<String>,
) -> Result<arrow::array::ArrayRef> {
    use arrow::array::{AsArray, MapArray, StructArray};
    let Some((head, rest)) = path.split_first() else {
        let strings = array.as_string::<i32>();
        let edited: StringArray = (0..strings.len())
            .map(|row| {
                strings
                    .is_valid(row)
                    .then(|| edit(strings.value(row)).unwrap_or_else(|| strings.value(row).to_owned()))
            })
            .collect();
        return Ok(Arc::new(edited));
    };
    if let DataType::Map(field, ordered) = array.data_type() {
        let map = array.as_map();
        let entries = map.entries();
        let index = usize::from(*head == "value");
        let mut columns = entries.columns().to_vec();
        columns[index] = edit_strings(&columns[index], rest, edit)?;
        let entries = StructArray::try_new(entries.fields().clone(), columns, entries.nulls().cloned())?;
        return Ok(Arc::new(MapArray::try_new(
            Arc::clone(field),
            map.offsets().clone(),
            entries,
            map.nulls().cloned(),
            *ordered,
        )?));
    }
    let parent = array.as_struct();
    let index = parent
        .fields()
        .iter()
        .position(|field| field.name() == head)
        .with_context(|| format!("field {head}"))?;
    let mut columns = parent.columns().to_vec();
    columns[index] = edit_strings(&columns[index], rest, edit)?;
    Ok(Arc::new(StructArray::try_new(
        parent.fields().clone(),
        columns,
        parent.nulls().cloned(),
    )?))
}

/// Add `key -> value` to `metaData.configuration` of every metaData row.
fn add_configuration(array: &arrow::array::ArrayRef, key: &str, value: &str) -> Result<arrow::array::ArrayRef> {
    use arrow::array::{AsArray, MapArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    let metadata = array.as_struct();
    let index = metadata
        .fields()
        .iter()
        .position(|field| field.name() == "configuration")
        .context("configuration")?;
    let column = metadata.column(index);
    let DataType::Map(field, ordered) = column.data_type() else {
        bail!("configuration is not a map");
    };
    let map = column.as_map();
    let (keys, values) = (map.keys().as_string::<i32>(), map.values().as_string::<i32>());
    let (mut new_keys, mut new_values, mut lengths) = (Vec::new(), Vec::new(), Vec::new());
    for row in 0..map.len() {
        let range = usize::try_from(map.value_offsets()[row])?..usize::try_from(map.value_offsets()[row + 1])?;
        for entry in range.clone() {
            new_keys.push(Some(keys.value(entry).to_owned()));
            new_values.push(values.is_valid(entry).then(|| values.value(entry).to_owned()));
        }
        let mut length = range.len();
        if metadata.is_valid(row) {
            new_keys.push(Some(key.to_owned()));
            new_values.push(Some(value.to_owned()));
            length += 1;
        }
        lengths.push(length);
    }
    let entries = map.entries();
    let entries = StructArray::try_new(
        entries.fields().clone(),
        vec![
            Arc::new(StringArray::from(new_keys)),
            Arc::new(StringArray::from(new_values)),
        ],
        None,
    )?;
    let mut columns = metadata.columns().to_vec();
    columns[index] = Arc::new(MapArray::try_new(
        Arc::clone(field),
        OffsetBuffer::from_lengths(lengths),
        entries,
        map.nulls().cloned(),
        *ordered,
    )?);
    Ok(Arc::new(StructArray::try_new(
        metadata.fields().clone(),
        columns,
        metadata.nulls().cloned(),
    )?))
}

/// Rewrite checkpoint `version` in place, replacing the `column` of every batch.
fn rewrite_checkpoint(
    root: &Path,
    version: u64,
    column: &str,
    rewrite: &mut dyn FnMut(&arrow::array::ArrayRef) -> Result<arrow::array::ArrayRef>,
) -> Result<()> {
    let path = checkpoint_path(root, version);
    let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&path)?)?;
    let schema = Arc::clone(reader.schema());
    let index = schema.index_of(column)?;
    let mut batches = Vec::new();
    for batch in reader.build()? {
        let batch = batch?;
        let mut columns = batch.columns().to_vec();
        columns[index] = rewrite(&columns[index])?;
        batches.push(RecordBatch::try_new(Arc::clone(&schema), columns)?);
    }
    let mut writer = parquet::arrow::ArrowWriter::try_new(fs::File::create(&path)?, schema, None)?;
    for batch in &batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}

/// A provable checkpoint whose Parquet file is then corrupted by `corrupt`.
/// Pruning must refuse (Unsupported or MetadataChanged) and delete nothing.
fn assert_corrupt_checkpoint_refused(corrupt: &mut dyn FnMut(&Path, u64) -> Result<()>) -> Result<()> {
    let root = tempfile::tempdir()?;
    let (_, maintenance) = prunable_history(root.path())?;
    let checkpoint = vacuum_and_checkpoint(&maintenance)?;
    let config = short_prune(now_ms()? + 1_000);
    assert_eq!(
        maintenance.plan_prune_log(&config)?.checkpoint_version,
        Some(checkpoint),
        "provable before corruption"
    );
    corrupt(root.path(), checkpoint)?;
    let before = log_files(root.path())?;
    let refused = kind(maintenance.plan_prune_log(&short_prune(now_ms()? + 1_000)));
    assert!(
        matches!(
            refused,
            Some(MaintenanceErrorKind::Unsupported | MaintenanceErrorKind::MetadataChanged)
        ),
        "{refused:?}"
    );
    assert_eq!(log_files(root.path())?, before);
    assert_eq!(maintenance.log_floor()?, None);
    Ok(())
}

/// Scenario: checkpoint C rewritten with the same paths, txns and table id but one
/// add's partitionValue changed.
/// Guarantees: the recovery-state comparison refuses the prune; nothing is deleted.
#[test]
fn prune_log_refuses_checkpoint_with_changed_partition_value() -> Result<()> {
    assert_corrupt_checkpoint_refused(&mut |root, version| {
        let mut changed = 0;
        rewrite_checkpoint(root, version, "add", &mut |array| {
            edit_strings(array, &["partitionValues", "value"], &mut |value| {
                (changed == 0).then(|| {
                    changed += 1;
                    format!("{value}-changed")
                })
            })
        })?;
        assert_eq!(changed, 1);
        Ok(())
    })
}

/// Scenario: checkpoint C rewritten with a different (valid) metaData.schemaString.
/// Guarantees: the recovery-state comparison refuses the prune; nothing is deleted.
#[test]
fn prune_log_refuses_checkpoint_with_changed_schema() -> Result<()> {
    assert_corrupt_checkpoint_refused(&mut |root, version| {
        let mut changed = 0;
        rewrite_checkpoint(root, version, "metaData", &mut |array| {
            edit_strings(array, &["schemaString"], &mut |schema| {
                let mut parsed: serde_json::Value = serde_json::from_str(schema).ok()?;
                parsed["fields"].as_array_mut()?.push(serde_json::json!({
                    "name": "extra",
                    "type": "string",
                    "nullable": true,
                    "metadata": {}
                }));
                changed += 1;
                Some(parsed.to_string())
            })
        })?;
        assert_eq!(changed, 1);
        Ok(())
    })
}

/// Scenario: checkpoint C rewritten with an extra metaData.configuration property.
/// Guarantees: the recovery-state comparison refuses the prune; nothing is deleted.
#[test]
fn prune_log_refuses_checkpoint_with_changed_configuration() -> Result<()> {
    assert_corrupt_checkpoint_refused(&mut |root, version| {
        rewrite_checkpoint(root, version, "metaData", &mut |array| {
            add_configuration(array, "arrowcol.test.injected", "1")
        })
    })
}

fn modified_ms(path: &Path) -> Result<i64> {
    Ok(i64::try_from(
        fs::metadata(path)?
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?)
}

/// Scenario: a durable floor at C, a second proven plan at C2, then a delayed older
/// checkpoint rewrites `_last_checkpoint` below the floor (or it is corrupted).
/// Guarantees: planning and the pending apply neither panic nor delete; the floor is
/// unchanged; a malformed hint is an explicit error for pruning while checkpointing
/// still repairs it, after which pruning resumes.
#[test]
fn prune_log_hint_below_floor_is_refused_without_deletion() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (table, maintenance) = prunable_history(root.path())?;
    let floor = vacuum_and_checkpoint(&maintenance)?;
    let config = short_prune(now_ms()? + 1_000);
    let plan = maintenance.plan_prune_log(&config)?;
    maintenance.apply_prune_log(&plan, &config, &control())?;
    assert_eq!(maintenance.log_floor()?.context("floor")?.checkpoint_version, floor);
    for index in 10..12 {
        append(&table, index, index * 10..index * 10 + 2)?;
    }
    let second = vacuum_and_checkpoint(&maintenance)?;
    let config = short_prune(now_ms()? + 1_000);
    let pending = maintenance.plan_prune_log(&config)?;
    assert_eq!(pending.checkpoint_version, Some(second));

    let hint = root.path().join("_delta_log").join("_last_checkpoint");
    let restore = fs::read(&hint)?;
    fs::write(&hint, format!("{{\"version\":{},\"size\":1}}", floor - 1))?;
    let before = log_files(root.path())?;
    assert!(!before.is_empty());
    let plan = maintenance.plan_prune_log(&config)?;
    assert_eq!((plan.checkpoint_version, plan.report().outcome), (None, Outcome::Noop));
    assert_eq!(
        kind(maintenance.apply_prune_log(&pending, &config, &control())),
        Some(MaintenanceErrorKind::StalePlan)
    );
    assert_eq!(log_files(root.path())?, before, "no deletion below the floor");
    assert_eq!(maintenance.log_floor()?.context("floor")?.checkpoint_version, floor);

    fs::write(&hint, b"{not json")?;
    assert_eq!(
        kind(maintenance.plan_prune_log(&config)),
        Some(MaintenanceErrorKind::Unsupported)
    );
    assert_eq!(
        kind(maintenance.apply_prune_log(&pending, &config, &control())),
        Some(MaintenanceErrorKind::Unsupported)
    );
    assert_eq!(log_files(root.path())?, before);
    assert_eq!(maintenance.checkpoint_if_due(1, false)?.outcome, Outcome::Planned);

    fs::write(&hint, restore)?;
    let report = maintenance.apply_prune_log(&pending, &config, &control())?;
    assert_eq!((report.outcome, report.version), (Outcome::Pruned, second));
    assert_eq!(maintenance.log_floor()?.context("floor")?.checkpoint_version, second);
    Ok(())
}

/// Scenario: a bounded short-retention prune leaves JSON at or below the floor, then
/// the log retention grows so the newest leftover is young again.
/// Guarantees: resuming at the floor applies the fresh-proof age check to every
/// leftover: the plan is Noop and applying the earlier resume plan is StalePlan,
/// with zero additional deletes; once the anchor makes every leftover old enough
/// under the larger retention, the resume completes.
#[test]
fn prune_log_resume_respects_increased_retention() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (_, maintenance) = prunable_history(root.path())?;
    let checkpoint = vacuum_and_checkpoint(&maintenance)?;
    let bounded = PruneConfig {
        max_prune_files: 2,
        ..short_prune(now_ms()? + 1_000)
    };
    let partial = maintenance.plan_prune_log(&bounded)?;
    assert_eq!(
        maintenance
            .apply_prune_log(&partial, &bounded, &control())?
            .files_deleted,
        2
    );
    assert_eq!(
        maintenance.log_floor()?.map(|floor| floor.checkpoint_version),
        Some(checkpoint)
    );
    let resume = maintenance.plan_prune_log(&short_prune(now_ms()? + 1_000))?;
    assert_eq!(resume.checkpoint_version, Some(checkpoint));
    assert_eq!(resume.deletions.len(), usize::try_from(checkpoint - 1)?);
    let leftovers = log_files(root.path())?;
    let newest = leftovers
        .iter()
        .map(|path| modified_ms(path))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max()
        .context("leftover JSON")?;
    while now_ms()? <= newest {
        std::thread::sleep(Duration::from_millis(5));
    }
    // The cutoff lands exactly on the newest leftover, which is therefore too young.
    let anchor = now_ms()?;
    let stricter = PruneConfig {
        log_retention_ms: Some(anchor - newest),
        unsafe_allow_short_retention_for_tests: true,
        ..PruneConfig::new(anchor)
    };
    let plan = maintenance.plan_prune_log(&stricter)?;
    assert_eq!((plan.checkpoint_version, plan.report().outcome), (None, Outcome::Noop));
    assert!(plan.deletions.is_empty());
    assert_eq!(
        kind(maintenance.apply_prune_log(&resume, &stricter, &control())),
        Some(MaintenanceErrorKind::StalePlan)
    );
    assert_eq!(log_files(root.path())?, leftovers, "zero additional deletes");

    while now_ms()? <= anchor + 1 {
        std::thread::sleep(Duration::from_millis(5));
    }
    let later = PruneConfig {
        now_ms: anchor + 1,
        ..stricter
    };
    let plan = maintenance.plan_prune_log(&later)?;
    assert_eq!(plan.checkpoint_version, Some(checkpoint));
    assert_eq!(plan.deletions.len(), leftovers.len());
    let report = maintenance.apply_prune_log(&plan, &later, &control())?;
    assert_eq!(report.outcome, Outcome::Pruned);
    assert!(log_files(root.path())?.is_empty());
    assert!(checkpoint_path(root.path(), checkpoint).is_file());
    Ok(())
}
