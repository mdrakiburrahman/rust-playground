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
    CompactionDebtSource, DISPOSABLE_TEST_MARKER, Granularity, LogFloor, MaintenanceControl,
    MaintenanceDueWorkCompleteness, MaintenanceDueWorkIncompleteReason, MaintenanceErrorKind, MaintenancePlan,
    NegativeProbeConfig, OptimizeConfig, Outcome, PassProbeInventory, ProbeOperation, PruneConfig, PurgeConfig,
    VacuumConfig, error_kind, now_ms,
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
use std::time::{Duration, Instant, SystemTime};
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

#[test]
fn identity_scan_uses_one_active_snapshot_during_compaction() -> Result<()> {
    let root = tempfile::tempdir()?;
    let identity_schema = Arc::new(Schema::new(vec![
        Field::new("endpoint_url", DataType::Utf8, false),
        Field::new(INGESTION_PARTITION, DataType::Utf8, false),
    ]));
    let url = url_of(root.path())?;
    let table = DeltaTable::open_url(
        url.clone(),
        Arc::clone(&identity_schema),
        vec![INGESTION_PARTITION.into()],
    )?;
    for suffix in 0..2 {
        let values = vec![format!("identity-{suffix}-0"), format!("identity-{suffix}-1")];
        let data = RecordBatch::try_new(
            Arc::clone(&identity_schema),
            vec![
                Arc::new(StringArray::from(values)),
                Arc::new(StringArray::from(vec![minute(0); 2])),
            ],
        )?;
        run(async {
            let mut file = table
                .new_file_partitioned(
                    Arc::clone(&identity_schema),
                    BTreeMap::from([(INGESTION_PARTITION.to_owned(), Some(minute(0)))]),
                )?
                .with_ingestion_time(BASE + 1)?;
            file.write(&data).await?;
            file.finish().await?;
            Ok::<_, anyhow::Error>(())
        })??;
    }
    let maintenance = DeltaTable::open_for_maintenance(&url)?;
    let config = OptimizeConfig {
        sealed_final: true,
        ..optimize_config()
    };
    let plan = maintenance.plan_optimize(&config)?;
    assert!(!plan.bins.is_empty());
    let mut first = true;
    let mut observed = Vec::new();
    let before = arrow_deltalake::maintenance::scan_active_endpoint_urls(&url, 100, |values| {
        observed.extend(values.iter().map(|value| value.expect("non-null identity").to_owned()));
        if first {
            first = false;
            maintenance.apply_optimize(&plan, &config, &control())?;
        }
        Ok(())
    })?;
    observed.sort();
    assert_eq!(
        observed,
        ["identity-0-0", "identity-0-1", "identity-1-0", "identity-1-1"]
    );
    assert_eq!(before.files, 2);
    assert_eq!(before.rows, 4);
    let mut after_values = Vec::new();
    let after = arrow_deltalake::maintenance::scan_active_endpoint_urls(&url, 100, |values| {
        after_values.extend(values.iter().map(|value| value.expect("non-null identity").to_owned()));
        Ok(())
    })?;
    after_values.sort();
    assert_eq!(after_values, observed);
    assert!(after.version > before.version);
    assert_eq!(after.rows, 4);
    assert_eq!(after.files, 1);
    assert_eq!(after.table_id, before.table_id);
    assert!(arrow_deltalake::maintenance::scan_active_endpoint_urls(&url, 0, |_| Ok(())).is_err());
    Ok(())
}

#[test]
fn optimize_decode_limit_diagnostic_identifies_failed_bound() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    append(&table, 0, 0..3)?;
    append(&table, 0, 3..6)?;
    append(&table, 1, 6..9)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    for bound in ["max_batch_bytes", "max_decoded_bytes_per_bin"] {
        let config = OptimizeConfig {
            max_batch_bytes: if bound == "max_batch_bytes" { 1 } else { usize::MAX },
            max_decoded_bytes_per_bin: if bound == "max_decoded_bytes_per_bin" {
                1
            } else {
                u64::MAX
            },
            ..optimize_config()
        };
        let plan = maintenance.plan_optimize(&config)?;
        assert!(!plan.bins.is_empty());
        let error = maintenance
            .apply_optimize(&plan, &config, &control())
            .err()
            .context("decoded input must exceed the selected one-byte bound")?;
        assert_eq!(error_kind(&error), Some(MaintenanceErrorKind::ResourceLimit));
        let diagnostic = format!("{error:#}");
        let prefix = format!("decoded Arrow input exceeds {bound}: measured_bytes=");
        let measured = diagnostic
            .split_once(&prefix)
            .context("diagnostic identifies the failed bound")?
            .1
            .split_once(", allowed_bytes=1")
            .context("diagnostic identifies allowed bytes")?
            .0
            .parse::<u64>()?;
        assert!(measured > 1, "diagnostic reports actual decoded bytes");
    }
    Ok(())
}

#[test]
fn compaction_debt_evidence_distinguishes_zero_from_bounded_backlog() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    append(&table, 0, 0..3)?;
    append(&table, 1, 100..103)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let anchor = BASE + 60 * MINUTE;
    let probe = NegativeProbeConfig {
        now_ms: anchor,
        granularity: Granularity::Minute,
        purge_retention_ms: 2_592_000_000,
        vacuum_retention_ms: 604_800_000,
        log_retention_ms: 2_592_000_000,
        checkpoint_interval: 100,
        optimize_small_file_bytes: 64 * 1024 * 1024,
        optimize_target_file_bytes: 128 * 1024 * 1024,
        optimize_min_files: 2,
        sealed_partitions_only: false,
        max_listed_files: 1000,
    };
    let zero = maintenance
        .probe_no_work(ProbeOperation::Optimize, &probe, &control())?
        .context("complete bounded inventory proves no eligible compaction")?;
    let zero_debt = zero.compaction_debt.context("available zero-debt evidence")?;
    assert_eq!(zero_debt.source, CompactionDebtSource::BoundedPhysicalInventory);
    assert_eq!(zero_debt.observed_at_ms, anchor);
    assert_eq!((zero_debt.eligible_files, zero_debt.eligible_bytes), (0, 0));

    for batch in 1..4 {
        append(&table, 0, batch * 10..batch * 10 + 3)?;
    }
    let config = OptimizeConfig {
        max_files_per_bin: 2,
        max_bins_per_commit: 1,
        now_ms: Some(anchor),
        ..optimize_config()
    };
    let plan = maintenance.plan_optimize(&config)?;
    assert_eq!(plan.bins.len(), 1);
    assert_eq!(plan.compaction_debt.source, CompactionDebtSource::SnapshotActiveFiles);
    assert_eq!(plan.compaction_debt.observed_at_ms, anchor);
    assert_eq!(plan.compaction_debt.eligible_files, 4);
    assert!(plan.compaction_debt.eligible_bytes > 0);
    assert_eq!(plan.compaction_debt.backlog_files, 2);
    assert!(plan.compaction_debt.backlog_bytes > 0);
    assert_eq!(plan.compaction_debt.oldest_eligible_partition_start_ms, Some(BASE));
    let report = maintenance.apply_optimize(&plan, &config, &control())?;
    assert_eq!(report.compaction_debt.as_ref(), Some(&plan.compaction_debt));
    let evidence = report
        .maintenance_due_work
        .as_ref()
        .context("OPTIMIZE due-work evidence")?;
    assert_eq!(evidence.due, Some(true));
    assert_eq!(evidence.eligible_count, Some(4));
    assert_eq!(evidence.backlog_count, Some(2));
    Ok(())
}

#[test]
fn optimize_protected_partition_backlog_tracks_subtarget_active_inputs() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    append(&table, 0, 0..3)?;
    append(&table, 0, 3..6)?;
    let active = active_files(root.path())?;
    let bytes = active.keys().try_fold(0_u64, |total, path| {
        Ok::<_, anyhow::Error>(total + fs::metadata(root.path().join(path))?.len())
    })?;
    let path = active.keys().next().context("active input")?;
    fs::copy(
        root.path().join(path),
        root.path()
            .join(format!("{INGESTION_PARTITION}={}/orphan.parquet", minute(0))),
    )?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let anchor = BASE + MINUTE / 2;
    let probe = NegativeProbeConfig {
        now_ms: anchor,
        granularity: Granularity::Minute,
        purge_retention_ms: 2_592_000_000,
        vacuum_retention_ms: 604_800_000,
        log_retention_ms: 2_592_000_000,
        checkpoint_interval: 100,
        optimize_small_file_bytes: 1,
        optimize_target_file_bytes: 128 * 1024 * 1024,
        optimize_min_files: 3,
        sealed_partitions_only: true,
        max_listed_files: 1000,
    };
    assert!(
        maintenance
            .probe_no_work(ProbeOperation::Optimize, &probe, &control())?
            .is_none()
    );
    let config = OptimizeConfig {
        small_file_bytes: probe.optimize_small_file_bytes,
        min_files_per_bin: probe.optimize_min_files,
        now_ms: Some(anchor),
        ..optimize_config()
    };
    let plan = maintenance.plan_optimize(&config)?;
    assert!(plan.bins.is_empty());
    let version = maintenance.version()?;
    let report = maintenance.apply_optimize(&plan, &config, &control())?;
    let evidence = report.maintenance_due_work.context("complete eligibility evidence")?;
    assert_eq!(evidence.completeness, MaintenanceDueWorkCompleteness::Complete);
    assert_eq!(evidence.due, Some(false));
    assert_eq!(evidence.eligible_count, Some(0));
    let debt = report.compaction_debt.context("protected compaction backlog")?;
    assert_eq!(debt.observed_at_ms, anchor);
    assert_eq!(debt.source, CompactionDebtSource::SnapshotActiveFiles);
    let protected = debt
        .protected_partition_backlog
        .context("authoritative protected inputs")?;
    assert_eq!((protected.files, protected.bytes), (2, bytes));
    assert_eq!(protected.oldest_partition_start_ms, Some(BASE));
    assert_eq!(maintenance.version()?, version);
    assert_eq!(active_files(root.path())?, active);
    assert_eq!(rows(root.path())?, sorted(expected(0..6, &minute(0))));
    Ok(())
}

#[test]
fn optimize_probe_does_not_infer_active_frontier_from_newer_orphan() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    append(&table, 0, 0..3)?;
    let active = active_files(root.path())?;
    let path = active.keys().next().context("active input")?;
    let bytes = fs::metadata(root.path().join(path))?.len();
    let target = bytes + 1;
    let orphan_dir = root.path().join(format!("{INGESTION_PARTITION}={}", minute(1)));
    fs::create_dir(&orphan_dir)?;
    fs::File::create(orphan_dir.join("orphan.parquet"))?.set_len(target)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let probe = NegativeProbeConfig {
        now_ms: BASE + 60 * MINUTE,
        granularity: Granularity::Minute,
        purge_retention_ms: 2_592_000_000,
        vacuum_retention_ms: 604_800_000,
        log_retention_ms: 2_592_000_000,
        checkpoint_interval: 100,
        optimize_small_file_bytes: 1,
        optimize_target_file_bytes: target,
        optimize_min_files: 3,
        sealed_partitions_only: true,
        max_listed_files: 1000,
    };
    assert!(
        maintenance
            .probe_no_work(ProbeOperation::Optimize, &probe, &control())?
            .is_none()
    );
    let plan = maintenance.plan_optimize(&OptimizeConfig {
        now_ms: Some(probe.now_ms),
        target_file_bytes: target,
        small_file_bytes: probe.optimize_small_file_bytes,
        min_files_per_bin: probe.optimize_min_files,
        ..optimize_config()
    })?;
    assert!(plan.bins.is_empty());
    let protected = plan
        .compaction_debt
        .protected_partition_backlog
        .context("active protected frontier")?;
    assert_eq!((protected.files, protected.bytes), (1, bytes));
    assert_eq!(protected.oldest_partition_start_ms, Some(BASE));
    assert_eq!(active_files(root.path())?, active);
    Ok(())
}

#[test]
fn optimize_final_debt_fields_are_included_in_plan_size_bound() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    for batch in 0..4 {
        append(&table, 0, batch * 10..batch * 10 + 3)?;
    }
    append(&table, 1, 100..103)?;
    append(&table, 1, 110..113)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let anchor = BASE + 60 * MINUTE;
    let base_config = OptimizeConfig {
        max_files_per_bin: 2,
        max_bins_per_commit: 1,
        now_ms: Some(anchor),
        ..optimize_config()
    };
    let unbounded = maintenance.prepare_optimize(&base_config)?;
    assert_eq!(unbounded.plan.bins.len(), 1);
    assert!(unbounded.plan.compaction_debt.backlog_files > 0);
    assert!(unbounded.plan.compaction_debt.backlog_bytes > 0);

    let mut preliminary = unbounded.plan.clone();
    preliminary.backlog_files = 0;
    preliminary.compaction_debt.backlog_files = 0;
    preliminary.compaction_debt.backlog_bytes = 0;
    let max_plan_bytes = serde_json::to_vec(&preliminary)?.len();
    let config = OptimizeConfig {
        max_plan_bytes,
        ..base_config
    };
    let prepared = maintenance.prepare_optimize(&config)?;
    let encoded = serde_json::to_vec(&prepared.plan)?;
    assert!(encoded.len() <= max_plan_bytes);
    assert_eq!(
        prepared.plan.bins.len(),
        0,
        "final backlog fields require trimming the bin"
    );
    assert_eq!(
        prepared.plan.backlog_files,
        prepared.plan.compaction_debt.eligible_files
    );
    assert_eq!(
        prepared.plan.compaction_debt.backlog_bytes,
        prepared.plan.compaction_debt.eligible_bytes
    );
    let version = maintenance.version()?;
    let report = maintenance.apply_prepared_optimize(&prepared, &config, &control())?;
    assert_eq!(report.outcome, Outcome::Noop);
    assert_eq!(maintenance.version()?, version, "empty bounded plan must not commit");
    Ok(())
}

#[test]
fn pass_inventory_bounds_counted_io_for_all_idle_probes() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    drop(table);

    let counts = Arc::new(StorageCounts::default());
    let store = Arc::new(UncertainStore {
        inner: LocalFileSystem::new(),
        target: "no-uncertain-commit-in-this-test".to_owned(),
        unreadable: AtomicBool::new(false),
        attempts: AtomicUsize::new(0),
        read_barrier: None,
        counts: Some(Arc::clone(&counts)),
    });
    let maintenance = DeltaTable::open_with_store(root.path(), Arc::clone(&store) as Arc<dyn ObjectStore>)?;
    counts.reads.store(0, Ordering::SeqCst);
    counts.lists.store(0, Ordering::SeqCst);
    counts.writes.store(0, Ordering::SeqCst);

    let config = NegativeProbeConfig {
        now_ms: BASE + 60 * MINUTE,
        granularity: Granularity::Minute,
        purge_retention_ms: 2_592_000_000,
        vacuum_retention_ms: 604_800_000,
        log_retention_ms: 2_592_000_000,
        checkpoint_interval: 100,
        optimize_small_file_bytes: 64 * 1024 * 1024,
        optimize_target_file_bytes: 128 * 1024 * 1024,
        optimize_min_files: 2,
        sealed_partitions_only: false,
        max_listed_files: 1000,
    };
    let mut inventory = PassProbeInventory::default();
    for operation in [
        ProbeOperation::Purge,
        ProbeOperation::Optimize,
        ProbeOperation::Vacuum,
        ProbeOperation::Checkpoint,
        ProbeOperation::PruneLog,
    ] {
        maintenance.probe_no_work_with_inventory(operation, &config, &control(), &mut inventory)?;
    }

    assert_eq!(counts.writes.load(Ordering::SeqCst), 0);
    assert!(counts.lists.load(Ordering::SeqCst) <= 7, "bounded pass listings");
    assert!(counts.reads.load(Ordering::SeqCst) <= 12, "bounded pass reads");
    Ok(())
}

#[test]
fn vacuum_probe_defers_to_bounded_tombstone_replay() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    append(&table, 0, 0..3)?;
    drop(table);
    let old_modified = SystemTime::now()
        .checked_sub(Duration::from_secs(8 * 24 * 60 * 60))
        .context("old file timestamp")?;
    let partition_dir = fs::read_dir(root.path())?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("__arrowcol_ingestion="))
        })
        .context("ingestion partition directory")?;
    let file = fs::read_dir(partition_dir)?.next().context("partition file")??;
    fs::File::open(file.path())?.set_modified(old_modified)?;

    let counts = Arc::new(StorageCounts::default());
    let store = Arc::new(UncertainStore {
        inner: LocalFileSystem::new(),
        target: "no-uncertain-commit-in-this-test".to_owned(),
        unreadable: AtomicBool::new(false),
        attempts: AtomicUsize::new(0),
        read_barrier: None,
        counts: Some(Arc::clone(&counts)),
    });
    let maintenance = DeltaTable::open_with_store(root.path(), Arc::clone(&store) as Arc<dyn ObjectStore>)?;
    counts.reads.store(0, Ordering::SeqCst);
    counts.lists.store(0, Ordering::SeqCst);
    counts.writes.store(0, Ordering::SeqCst);

    let now = now_ms()?;
    let anchor = now
        .checked_sub(2 * 24 * 60 * 60 * 1000)
        .context("initial anchor underflow")?;
    let config = NegativeProbeConfig {
        now_ms: anchor,
        granularity: Granularity::Minute,
        purge_retention_ms: 2_592_000_000,
        vacuum_retention_ms: 604_800_000,
        log_retention_ms: 2_592_000_000,
        checkpoint_interval: 100,
        optimize_small_file_bytes: 64 * 1024 * 1024,
        optimize_target_file_bytes: 128 * 1024 * 1024,
        optimize_min_files: 2,
        sealed_partitions_only: false,
        max_listed_files: 1000,
    };
    let mut inventory = PassProbeInventory::default();
    assert!(
        maintenance
            .probe_no_work_with_inventory(ProbeOperation::Vacuum, &config, &control(), &mut inventory)?
            .is_none(),
        "physical timestamps cannot establish VACUUM LITE tombstone eligibility"
    );
    let due_config = NegativeProbeConfig { now_ms: now, ..config };
    assert!(
        maintenance
            .probe_no_work_with_inventory(ProbeOperation::Vacuum, &due_config, &control(), &mut inventory)?
            .is_none(),
        "each anchor must reach the bounded tombstone planner"
    );
    let report = maintenance.vacuum_once(&VacuumConfig::new(604_800_000, now), false, &control())?;
    let evidence = report.maintenance_due_work.context("bounded tombstone evidence")?;
    assert_eq!(evidence.observed_at_ms, now);
    assert_eq!(evidence.completeness, MaintenanceDueWorkCompleteness::Complete);
    assert_eq!(evidence.due, Some(false));
    assert_eq!(evidence.eligible_count, Some(0));
    assert_eq!(counts.writes.load(Ordering::SeqCst), 0);
    Ok(())
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

fn parquet_compression(path: &Path) -> Result<parquet::basic::Compression> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path)?)?;
    let group = reader.metadata().row_groups().first().context("Parquet row group")?;
    let column = group.columns().first().context("Parquet column")?;
    Ok(column.compression())
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

/// Final-compaction policy bypasses only frontier churn protection, preserving
/// exact rows, schema, partitions, publication transactions and no-op semantics.
#[test]
fn sealed_final_optimize_preserves_frontier_data_and_publication_txns() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    for (index, app_id) in PUBLICATIONS.iter().enumerate() {
        let index = i64::try_from(index)?;
        append_published(&table, index, index * 10..index * 10 + 3, app_id)?;
        append(&table, index, index * 10 + 3..index * 10 + 6)?;
    }
    append(&table, 2, 20..23)?;
    append(&table, 2, 23..26)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let before_rows = rows(root.path())?;
    let before_txns = txn_versions(&maintenance)?;
    let closed = maintenance.plan_optimize(&optimize_config())?;
    assert!(!OptimizeConfig::default().sealed_final);
    assert_eq!(closed.protected_partitions, 1, "newest is protected even when closed");
    let default = OptimizeConfig {
        now_ms: Some(BASE + 30_000),
        ..optimize_config()
    };
    let protected = maintenance.plan_optimize(&default)?;
    assert_eq!(protected.protected_partitions, 3, "all open partitions stay protected");
    assert!(protected.bins.is_empty());

    let config = OptimizeConfig {
        sealed_final: true,
        ..default
    };
    let plan = maintenance.plan_optimize(&config)?;
    assert_eq!(plan.protected_partitions, 0);
    assert_eq!(plan.bins.len(), 3);
    let before = maintenance.version()?;
    let report = maintenance.apply_optimize(&plan, &config, &control())?;
    assert_eq!(report.outcome, Outcome::Committed);
    assert_eq!(report.version, before + 1);
    assert_eq!((report.files_removed, report.files_added), (6, 3));
    assert_eq!(rows(root.path())?, before_rows);
    assert_eq!(txn_versions(&maintenance)?, before_txns);
    for index in 0..3 {
        assert_eq!(files_in(root.path(), &minute(index))?, 1);
    }
    let actions = commit_actions(root.path(), report.version)?;
    let changes: Vec<_> = actions
        .iter()
        .filter_map(|action| action.get("add").or_else(|| action.get("remove")))
        .collect();
    assert_eq!(changes.len(), 9);
    assert!(changes.iter().all(|change| change["dataChange"] == false));
    assert_eq!(
        actions.iter().filter(|action| action.get("txn").is_some()).count(),
        1,
        "only the existing maintenance plan-id transaction is added"
    );
    let noop = maintenance.plan_optimize(&config)?;
    assert_eq!(
        noop.header.fingerprint, plan.header.fingerprint,
        "schema and metadata unchanged"
    );
    assert!(noop.bins.is_empty());
    assert_eq!(
        maintenance.apply_optimize(&noop, &config, &control())?.outcome,
        Outcome::Noop
    );
    assert_eq!(maintenance.version()?, report.version);
    assert_eq!(txn_versions(&maintenance)?, before_txns);
    Ok(())
}

/// Within existing byte/row bounds, 65 tiny files need two real passes because
/// a bin admits at most 64 inputs. A third fresh plan is empty.
#[test]
fn sealed_final_optimize_two_passes_with_more_than_64_small_files() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    for id in 0..65 {
        append(&table, 0, id..id + 1)?;
    }
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let config = OptimizeConfig {
        sealed_final: true,
        ..optimize_config()
    };
    let before_rows = rows(root.path())?;
    let before = maintenance.version()?;
    for (pass, expected_files) in [(1, 2), (2, 1)] {
        let plan = maintenance.plan_optimize(&config)?;
        assert_eq!(plan.bins.len(), 1);
        assert!(plan.bins[0].files.len() <= config.max_files_per_bin);
        let report = maintenance.apply_optimize(&plan, &config, &control())?;
        assert_eq!(report.outcome, Outcome::Committed);
        assert_eq!(report.version, before + pass);
        assert_eq!(files_in(root.path(), &minute(0))?, expected_files);
        assert_eq!(rows(root.path())?, before_rows);
    }
    let noop = maintenance.plan_optimize(&config)?;
    assert!(noop.bins.is_empty());
    assert_eq!(noop.backlog_files, 0);
    assert_eq!(
        maintenance.apply_optimize(&noop, &config, &control())?.outcome,
        Outcome::Noop
    );
    assert_eq!(maintenance.version()?, before + 2);
    Ok(())
}

#[test]
fn unsorted_optimize_respects_output_row_group_limit() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    append(&table, 0, 0..7)?;
    append(&table, 0, 7..14)?;
    append(&table, 1, 14..15)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let input_files = active_files(root.path())?;
    let input_compressions = input_files
        .keys()
        .map(|relative| parquet_compression(&data_path(root.path(), relative)?))
        .collect::<Result<Vec<_>>>()?;
    let input_compression = *input_compressions.first().context("input Parquet file")?;
    assert!(
        input_compressions
            .iter()
            .all(|compression| *compression == input_compression)
    );
    let override_codec = if matches!(input_compression, parquet::basic::Compression::ZSTD(_)) {
        "snappy"
    } else {
        "zstd"
    };
    let writer_override: arrow_deltalake::writer::ParquetWriterConfig = serde_json::from_value(serde_json::json!({
        "defaults": {"compression": override_codec}
    }))?;
    let config = OptimizeConfig {
        use_default_layout: false,
        output_row_group_rows: 3,
        ..optimize_config()
    };
    let before = rows(root.path())?;
    let prepared = maintenance.prepare_optimize_with_writer_config(&config, &writer_override)?;
    let report = maintenance.apply_prepared_optimize_with_writer_config(
        &prepared,
        &config,
        &control(),
        None,
        &writer_override,
    )?;
    assert_eq!(report.outcome, Outcome::Committed);
    assert_eq!(rows(root.path())?, before);
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(fs::File::open(data_path(root.path(), &report.outputs[0])?)?)?;
    let groups = reader.metadata().row_groups();
    assert_eq!(groups.len(), 5);
    assert!(groups.iter().all(|group| group.num_rows() <= 3));
    let output_compression = parquet_compression(&data_path(root.path(), &report.outputs[0])?)?;
    if matches!(input_compression, parquet::basic::Compression::ZSTD(_)) {
        assert_eq!(output_compression, parquet::basic::Compression::SNAPPY);
    } else {
        assert!(matches!(output_compression, parquet::basic::Compression::ZSTD(_)));
    }

    let active_after_optimize: BTreeSet<_> = active_files(root.path())?.into_keys().collect();
    append(&table, 2, 15..18)?;
    let new_ingestion_file = active_files(root.path())?
        .into_keys()
        .find(|path| !active_after_optimize.contains(path))
        .context("new ingestion file")?;
    assert_eq!(
        parquet_compression(&data_path(root.path(), &new_ingestion_file)?)?,
        input_compression
    );
    Ok(())
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

#[test]
fn optimize_probe_defers_below_threshold_malformed_partition() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    append_value(&table, "not-a-time", BASE, 0..3)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let config = NegativeProbeConfig {
        now_ms: BASE + 60 * MINUTE,
        granularity: Granularity::Minute,
        purge_retention_ms: 2_592_000_000,
        vacuum_retention_ms: 604_800_000,
        log_retention_ms: 2_592_000_000,
        checkpoint_interval: 100,
        optimize_small_file_bytes: 64 * 1024 * 1024,
        optimize_target_file_bytes: 128 * 1024 * 1024,
        optimize_min_files: 2,
        sealed_partitions_only: true,
        max_listed_files: 1000,
    };
    assert!(
        maintenance
            .probe_no_work(ProbeOperation::Optimize, &config, &control())?
            .is_none()
    );
    let report = MaintenancePlan::Optimize(maintenance.plan_optimize(&optimize_config())?).report();
    let evidence = report.maintenance_due_work.context("malformed partition evidence")?;
    assert_eq!(evidence.completeness, MaintenanceDueWorkCompleteness::Incomplete);
    assert_eq!(
        evidence.incomplete_reason,
        Some(MaintenanceDueWorkIncompleteReason::MalformedPartition)
    );
    assert_eq!(evidence.due, None);
    assert_eq!(evidence.eligible_count, None);
    Ok(())
}

#[test]
fn purge_probe_defers_malformed_or_mismatched_partition() -> Result<()> {
    for (value, granularity) in [
        ("2026-11-16-14-35", Granularity::Hour),
        ("not-a-time", Granularity::Minute),
        ("2026-11-16-14-35-00", Granularity::Minute),
        ("2026-02-30-14-35", Granularity::Minute),
    ] {
        let root = tempfile::tempdir()?;
        let table = create(root.path())?;
        append_value(&table, value, BASE, 0..3)?;
        let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
        let version = maintenance.version()?;
        let files = active_files(root.path())?;
        let probe = NegativeProbeConfig {
            now_ms: now_ms()?,
            granularity,
            purge_retention_ms: 2_592_000_000,
            vacuum_retention_ms: 604_800_000,
            log_retention_ms: 2_592_000_000,
            checkpoint_interval: 100,
            optimize_small_file_bytes: 64 * 1024 * 1024,
            optimize_target_file_bytes: 128 * 1024 * 1024,
            optimize_min_files: 2,
            sealed_partitions_only: true,
            max_listed_files: 1000,
        };
        assert!(
            maintenance
                .probe_no_work(ProbeOperation::Purge, &probe, &control())?
                .is_none(),
            "{value}: probe must defer instead of failing or proving zero debt"
        );
        let config = PurgeConfig::new(probe.purge_retention_ms, probe.now_ms, granularity);
        let plan = maintenance.plan_purge(&config)?;
        assert_eq!(plan.skipped_partitions, 1);
        assert!(plan.partitions.is_empty());
        let report = maintenance.apply_purge(&plan, &config, &control())?;
        let evidence = report.maintenance_due_work.context("skipped partition evidence")?;
        assert_eq!(evidence.completeness, MaintenanceDueWorkCompleteness::Incomplete);
        assert_eq!(
            evidence.incomplete_reason,
            Some(MaintenanceDueWorkIncompleteReason::MalformedPartition)
        );
        assert_eq!(evidence.due, None);
        assert_eq!(evidence.eligible_count, None);
        assert_eq!(maintenance.version()?, version);
        assert_eq!(maintenance.lookup_txn(PURGE_WATERMARK)?, None);
        assert_eq!(active_files(root.path())?, files);
        assert_eq!(rows(root.path())?, sorted(expected(0..3, value)));
    }
    Ok(())
}

#[test]
fn purge_actual_writer_partition_retention_boundary() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    let start = Granularity::Minute.parse_partition("2026-11-16-14-35")?;
    for index in 0..3 {
        let value = Granularity::Minute.format_ms(start + index * MINUTE)?;
        append_value(&table, &value, start + index * MINUTE, index * 10..index * 10 + 3)?;
    }
    append_value(&table, "2026-11-16-14-35", start, 30..33)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let boundary = PurgeConfig::new(2 * MINUTE, start + 3 * MINUTE, Granularity::Minute);
    let before = PurgeConfig::new(boundary.retention_ms, boundary.anchor_ms - 1, Granularity::Minute);
    assert!(maintenance.plan_purge(&before)?.partitions.is_empty());
    let plan = maintenance.plan_purge(&boundary)?;
    assert_eq!(plan.skipped_partitions, 0);
    assert_eq!(plan.cutoff_ms, start + MINUTE);
    assert_eq!(plan.partitions.len(), 1);
    let partition = plan.partitions.first().context("whole expired minute partition")?;
    assert_eq!(partition.value, "2026-11-16-14-35");
    assert_eq!(partition.end_ms, plan.cutoff_ms);
    assert_eq!(partition.files.len(), 2);
    let files = active_files(root.path())?;
    let version = maintenance.version()?;
    assert_eq!(
        kind(maintenance.apply_purge(&plan, &before, &control())),
        Some(MaintenanceErrorKind::StalePlan)
    );
    assert_eq!(active_files(root.path())?, files);
    assert_eq!(maintenance.version()?, version);
    assert_eq!(maintenance.lookup_txn(PURGE_WATERMARK)?, None);
    let report = maintenance.apply_purge(&plan, &boundary, &control())?;
    assert_eq!((report.outcome, report.files_removed), (Outcome::Committed, 2));
    assert_eq!(maintenance.lookup_txn(PURGE_WATERMARK)?, Some(start + MINUTE));
    let kept: BTreeSet<_> = active_files(root.path())?.into_values().collect();
    assert_eq!(
        kept,
        BTreeSet::from(["2026-11-16-14-36".to_owned(), "2026-11-16-14-37".to_owned()])
    );
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
    let evidence = report
        .maintenance_due_work
        .as_ref()
        .context("PURGE due-work evidence")?;
    assert_eq!(evidence.due, Some(true));
    assert_eq!(evidence.eligible_count, Some(2));
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

#[derive(Debug)]
struct BarrierFence {
    reached: Mutex<Option<Sender<()>>>,
    release: Mutex<Option<Receiver<()>>>,
    calls: AtomicUsize,
}

/// An actual blind append commits while sealed OPTIMIZE is inside apply, paused
/// at its input read. Revalidation preserves that unrelated active file.
#[test]
fn sealed_final_optimize_revalidates_inputs_after_concurrent_append() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = create(root.path())?;
    append(&table, 0, 0..3)?;
    append(&table, 0, 3..6)?;
    drop(table);
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let barrier = Arc::new(BarrierFence {
        reached: Mutex::new(Some(reached_tx)),
        release: Mutex::new(Some(release_rx)),
        calls: AtomicUsize::new(0),
    });
    let store = Arc::new(UncertainStore {
        inner: LocalFileSystem::new(),
        target: "no-uncertain-commit-in-this-test".to_owned(),
        unreadable: AtomicBool::new(false),
        attempts: AtomicUsize::new(0),
        read_barrier: Some(barrier),
        counts: None,
    });
    let table = DeltaTable::open_with_store(root.path(), store)?;
    let config = OptimizeConfig {
        sealed_final: true,
        ..optimize_config()
    };
    let plan = table.plan_optimize(&config)?;
    assert_eq!(plan.bins.len(), 1);
    let planned: BTreeSet<_> = plan.bins[0].files.iter().map(|file| file.path.clone()).collect();
    let report = std::thread::scope(|scope| -> Result<_> {
        let apply = scope.spawn(|| table.apply_optimize(&plan, &config, &control()));
        let appended = (|| -> Result<_> {
            reached_rx.recv_timeout(Duration::from_secs(60))?;
            let version = append(&table, 0, 100..103)?;
            let paths = active_files(root.path())?
                .into_keys()
                .filter(|path| !planned.contains(path))
                .collect::<Vec<_>>();
            Ok((version, paths))
        })();
        let released = release_tx.send(());
        let applied = apply.join().map_err(|_| anyhow!("OPTIMIZE thread panicked"))?;
        let (version, paths) = appended?;
        released?;
        let report = applied?;
        assert_eq!(report.version, version + 1);
        let active = active_files(root.path())?;
        assert_eq!(paths.len(), 1);
        assert!(
            active.contains_key(&paths[0]),
            "concurrent append is not a planned removal"
        );
        Ok(report)
    })?;
    assert_eq!(report.outcome, Outcome::Committed);
    assert_eq!((report.files_removed, report.files_added), (2, 1));
    let mut all = expected(0..6, &minute(0));
    all.extend(expected(100..103, &minute(0)));
    assert_eq!(rows(root.path())?, sorted(all));
    Ok(())
}

impl BarrierFence {
    fn wait(&self) -> std::result::Result<(), PublicationFenceError> {
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

impl PublicationFence for BarrierFence {
    fn check_before_commit(
        &self,
        _context: &PublicationFenceContext<'_>,
    ) -> std::result::Result<(), PublicationFenceError> {
        self.wait()
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
/// Guarantees: the default 7-day floor, production acknowledgement and test-root
/// proof are enforced; zero retention works without a disposable marker, only expired
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
    assert_eq!(maintenance.plan_vacuum(&short)?.candidates.len(), 6);
    fs::remove_file(root.path().join(DISPOSABLE_TEST_MARKER))?;
    short.unsafe_allow_short_retention_for_tests = false;
    short.allow_short_vacuum_retention = true;
    let negative = VacuumConfig {
        retention_ms: -1,
        ..short.clone()
    };
    assert_eq!(
        kind(maintenance.plan_vacuum(&negative)),
        Some(MaintenanceErrorKind::UnsafeRequest)
    );
    let plan = maintenance.plan_vacuum(&short)?;
    let due_evidence = MaintenancePlan::Vacuum(plan.clone())
        .report()
        .maintenance_due_work
        .context("VACUUM due-work evidence")?;
    assert_eq!(due_evidence.due, Some(true));
    assert_eq!(due_evidence.eligible_count, Some(6));
    let unacknowledged = VacuumConfig::new(0, now);
    assert_eq!(
        kind(maintenance.apply_vacuum(&plan, &unacknowledged, &control())),
        Some(MaintenanceErrorKind::UnsafeRequest),
        "acknowledgement is required again when applying"
    );
    assert_eq!(
        kind(maintenance.apply_vacuum(&plan, &negative, &control())),
        Some(MaintenanceErrorKind::UnsafeRequest)
    );
    let cancelled = maintenance.apply_vacuum(
        &plan,
        &short,
        &MaintenanceControl::new(Arc::new(AtomicBool::new(true)), None),
    )?;
    assert_eq!((cancelled.outcome, cancelled.files_deleted), (Outcome::Partial, 0));
    for path in &tombstoned {
        assert!(data_path(root.path(), path)?.exists(), "cancelled before delete");
    }
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

#[test]
fn optimize_does_not_rewrite_budget_full_layout_outputs_again() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open_url(
        url_of(root.path())?,
        customer_schema(),
        vec![INGESTION_PARTITION.into()],
    )?;
    for (index, customer) in ["c4", "c1", "c3", "c0"].into_iter().enumerate() {
        append_customers(&table, 0, &[customer], index as i64)?;
    }
    append_customers(&table, 1, &["frontier"], 10)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let layout = LayoutPolicy {
        sort_columns: vec!["customer_id".into()],
        max_rows: 2,
        max_input_bytes: 1024 * 1024,
        row_group_rows: 2,
        page_rows: 1,
        bloom_columns: vec!["customer_id".into()],
    };
    let config = OptimizeConfig {
        layout: Some(layout.clone()),
        sealed_final: true,
        ..optimize_config()
    };

    let first = maintenance.plan_optimize(&config)?;
    assert_eq!(first.bins.iter().map(|bin| bin.files.len()).sum::<usize>(), 4);
    let first_report = maintenance.apply_optimize(&first, &config, &control())?;
    assert_eq!(
        (
            first_report.outcome,
            first_report.files_removed,
            first_report.files_added,
            first_report.rows_removed,
            first_report.rows_added,
        ),
        (Outcome::Committed, 4, 2, 4, 4)
    );

    drop(maintenance);
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let second = maintenance.plan_optimize(&config)?;
    assert!(
        second.bins.is_empty(),
        "budget-full outputs under the same policy must not be reselected"
    );
    let second_report = maintenance.apply_optimize(&second, &config, &control())?;
    assert_eq!(second_report.outcome, Outcome::Noop);

    let changed_policy = OptimizeConfig {
        layout: Some(LayoutPolicy { max_rows: 3, ..layout }),
        ..config
    };
    assert!(
        !maintenance.plan_optimize(&changed_policy)?.bins.is_empty(),
        "a changed layout policy must make prior outputs eligible for re-layout"
    );
    let mut actual_rows = Vec::new();
    for (path, _) in active_files(root.path())? {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(data_path(root.path(), &path)?)?)?.build()?;
        for batch in reader {
            let batch = batch?;
            let customers = arrow::compute::cast(
                batch.column_by_name("customer_id").context("customer")?,
                &DataType::Utf8,
            )?;
            let customers = customers.as_any().downcast_ref::<StringArray>().context("customers")?;
            let ids = arrow::compute::cast(batch.column_by_name("id").context("id")?, &DataType::Int64)?;
            let ids = ids.as_any().downcast_ref::<Int64Array>().context("ids")?;
            for row in 0..batch.num_rows() {
                actual_rows.push((customers.value(row).to_owned(), ids.value(row)));
            }
        }
    }
    actual_rows.sort();
    assert_eq!(
        actual_rows,
        [
            ("c0".into(), 3),
            ("c1".into(), 1),
            ("c3".into(), 2),
            ("c4".into(), 0),
            ("frontier".into(), 10)
        ]
    );
    Ok(())
}

#[test]
fn optimize_reconsiders_partial_outputs_after_reopen_and_new_arrival() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open_url(
        url_of(root.path())?,
        customer_schema(),
        vec![INGESTION_PARTITION.into()],
    )?;
    append_customers(&table, 0, &["c0", "c1", "c2"], 0)?;
    append_customers(&table, 0, &["c3", "c4", "c5"], 3)?;
    append_customers(&table, 0, &["c6", "c7", "c8"], 6)?;
    append_customers(&table, 0, &["c9", "c10", "c11", "c12", "c13"], 9)?;
    let layout = LayoutPolicy {
        max_rows: 10,
        max_input_bytes: 1024 * 1024,
        row_group_rows: 10,
        page_rows: 2,
        ..LayoutPolicy::default()
    };
    let config = OptimizeConfig {
        layout: Some(layout),
        sealed_final: true,
        ..optimize_config()
    };
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let first = maintenance.plan_optimize(&config)?;
    assert_eq!(first.bins.iter().flat_map(|bin| &bin.files).count(), 4);
    let first_report = maintenance.apply_optimize(&first, &config, &control())?;
    assert_eq!(
        (
            first_report.files_removed,
            first_report.files_added,
            first_report.rows_removed,
            first_report.rows_added,
        ),
        (4, 2, 14, 14)
    );
    let first_outputs = active_files(root.path())?;
    assert_eq!(first_outputs.len(), 2);
    assert!(
        first_outputs.keys().all(|path| !path.contains("full-")),
        "partial outputs must not carry a reusable full-capacity marker"
    );

    drop(maintenance);
    drop(table);
    let reopened = DeltaTable::open_url(
        url_of(root.path())?,
        customer_schema(),
        vec![INGESTION_PARTITION.into()],
    )?;
    // A one-row arrival fits either partial output regardless of UUID path order.
    append_customers(&reopened, 0, &["c14"], 14)?;
    drop(reopened);
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let second = maintenance.plan_optimize(&config)?;
    assert_eq!(
        second.bins.iter().flat_map(|bin| &bin.files).count(),
        3,
        "reopened planning must reconsider both partial outputs with the later arrival"
    );
    let second_report = maintenance.apply_optimize(&second, &config, &control())?;
    assert_eq!(
        (
            second_report.outcome,
            second_report.files_removed,
            second_report.files_added,
            second_report.rows_removed,
            second_report.rows_added,
        ),
        (Outcome::Committed, 3, 2, 15, 15)
    );

    let mut actual_rows = Vec::new();
    for path in active_files(root.path())?.keys() {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(data_path(root.path(), path)?)?)?.build()?;
        let mut file_customers = Vec::new();
        for batch in reader {
            let batch = batch?;
            let customers = batch
                .column_by_name("customer_id")
                .context("customer_id")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("customers")?;
            let ids = batch
                .column_by_name("id")
                .context("id")?
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("ids")?;
            for row in 0..batch.num_rows() {
                file_customers.push(customers.value(row).to_owned());
                actual_rows.push((customers.value(row).to_owned(), ids.value(row)));
            }
        }
        assert!(file_customers.windows(2).all(|pair| pair[0] <= pair[1]));
    }
    actual_rows.sort();
    let mut expected_rows: Vec<_> = (0..15).map(|id| (format!("c{id}"), id)).collect();
    expected_rows.sort();
    assert_eq!(actual_rows, expected_rows);
    Ok(())
}

#[test]
fn optimize_d42_residual_batch_rows_change_invalidates_no_benefit_proof() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open_url(
        url_of(root.path())?,
        customer_schema(),
        vec![INGESTION_PARTITION.into()],
    )?;
    append_customers(&table, 0, &["c0", "c1", "c2", "c3", "c4", "c5"], 0)?;
    append_customers(&table, 0, &["c6", "c7", "c8", "c9", "c10", "c11"], 6)?;
    append_customers(&table, 0, &["c12", "c13", "c14", "c15", "c16", "c17"], 12)?;
    let layout = LayoutPolicy {
        max_rows: 10,
        max_input_bytes: 1024 * 1024,
        row_group_rows: 10,
        page_rows: 2,
        ..LayoutPolicy::default()
    };
    let six_row_batches = OptimizeConfig {
        layout: Some(layout),
        sealed_final: true,
        batch_rows: 6,
        ..optimize_config()
    };
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let plan = maintenance.plan_optimize(&six_row_batches)?;
    assert_eq!(plan.bins.iter().flat_map(|bin| &bin.files).count(), 3);
    let first = maintenance.apply_optimize(&plan, &six_row_batches, &control())?;
    assert_eq!(first.outcome, Outcome::Noop);
    assert_eq!(first.nonbeneficial_bins, 1);

    drop(maintenance);
    drop(table);
    let reopened = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let two_row_batches = OptimizeConfig {
        batch_rows: 2,
        ..six_row_batches
    };
    let changed = reopened.plan_optimize(&two_row_batches)?;
    assert_eq!(changed.bins.iter().flat_map(|bin| &bin.files).count(), 3);
    let report = reopened.apply_optimize(&changed, &two_row_batches, &control())?;
    assert_eq!(
        (
            report.outcome,
            report.files_removed,
            report.files_added,
            report.rows_removed,
            report.rows_added,
        ),
        (Outcome::Committed, 3, 2, 18, 18)
    );
    assert_eq!(active_files(root.path())?.len(), 2);
    Ok(())
}

#[test]
fn optimize_d42_residual_proof_write_failure_discards_prior_bin_outputs() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open_url(
        url_of(root.path())?,
        customer_schema(),
        vec![INGESTION_PARTITION.into()],
    )?;
    append_customers(&table, 1, &["c0", "c1", "c2"], 0)?;
    append_customers(&table, 1, &["c3", "c4", "c5"], 3)?;
    append_customers(&table, 0, &["c6", "c7", "c8", "c9", "c10", "c11"], 6)?;
    append_customers(&table, 0, &["c12", "c13", "c14", "c15", "c16", "c17"], 12)?;
    let config = OptimizeConfig {
        layout: Some(LayoutPolicy {
            max_rows: 10,
            max_input_bytes: 1024 * 1024,
            row_group_rows: 10,
            page_rows: 2,
            ..LayoutPolicy::default()
        }),
        sealed_final: true,
        batch_rows: 6,
        ..optimize_config()
    };
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let plan = maintenance.plan_optimize(&config)?;
    assert_eq!(plan.bins.len(), 2);
    let newest = minute(1);
    assert_eq!(
        plan.bins[0]
            .partition_values
            .get(INGESTION_PARTITION)
            .and_then(Option::as_deref),
        Some(newest.as_str())
    );
    let version = maintenance.version()?;
    let record_dir = root.path().join("_arrowcol_maintenance");
    fs::create_dir_all(&record_dir)?;
    fs::create_dir(record_dir.join(".lock"))?;

    let error = maintenance
        .apply_optimize(&plan, &config, &control())
        .err()
        .context("no-benefit proof persistence failure must surface")?;
    assert!(format!("{error:#}").contains("Is a directory"), "{error:#}");
    assert_eq!(
        maintenance.version()?,
        version,
        "the failed proof write must not commit"
    );
    assert_eq!(active_files(root.path())?.len(), 4);
    assert_eq!(
        parquet_files(root.path(), &newest)?,
        2,
        "earlier bin output must be discarded"
    );
    assert_eq!(parquet_files(root.path(), &minute(0))?, 2);
    Ok(())
}

#[test]
fn optimize_byte_limited_partial_outputs_do_not_churn_until_candidates_change() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open_url(
        url_of(root.path())?,
        customer_schema(),
        vec![INGESTION_PARTITION.into()],
    )?;
    let wide_values: Vec<String> = (0..4).map(|index| format!("{index}{}", "x".repeat(299_999))).collect();
    let sample = RecordBatch::try_new(
        customer_schema(),
        vec![
            Arc::new(StringArray::from(vec![wide_values[0].as_str()])),
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(StringArray::from(vec![minute(0)])),
        ],
    )?;
    let row_bytes = sample.get_array_memory_size();
    let input_cap = row_bytes.checked_mul(5).context("byte budget overflow")? / 2;
    assert!(row_bytes < input_cap);
    for (index, customer) in wide_values.iter().enumerate() {
        append_customers(&table, 0, &[customer], index as i64)?;
    }
    append_customers(&table, 1, &["frontier"], 10)?;

    let layout = LayoutPolicy {
        max_rows: 16,
        max_input_bytes: input_cap,
        row_group_rows: 16,
        page_rows: 4,
        ..LayoutPolicy::default()
    };
    let config = OptimizeConfig {
        layout: Some(layout),
        sealed_final: true,
        ..optimize_config()
    };
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let initial = maintenance.plan_optimize(&config)?;
    assert_eq!(initial.bins.iter().flat_map(|bin| &bin.files).count(), 4);
    let initial_report = maintenance.apply_optimize(&initial, &config, &control())?;
    assert_eq!(
        (
            initial_report.outcome,
            initial_report.files_removed,
            initial_report.files_added,
            initial_report.rows_removed,
            initial_report.rows_added,
        ),
        (Outcome::Committed, 4, 2, 4, 4)
    );

    let partial_outputs: Vec<String> = active_files(root.path())?
        .into_iter()
        .filter(|(_, partition)| *partition == minute(0))
        .map(|(path, _)| path)
        .collect();
    assert_eq!(partial_outputs.len(), 2);
    let mut output_batch_bytes = Vec::new();
    for path in &partial_outputs {
        let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(data_path(root.path(), path)?)?)?;
        for batch in reader.build()? {
            output_batch_bytes.push(batch?.get_array_memory_size());
        }
    }
    assert_eq!(output_batch_bytes.len(), 2);
    assert!(
        output_batch_bytes
            .iter()
            .all(|bytes| *bytes > input_cap / 2 && *bytes < input_cap),
        "observed output Arrow bytes {output_batch_bytes:?}, cap {input_cap}"
    );

    let first_repeat = maintenance.plan_optimize(&config)?;
    assert_eq!(first_repeat.bins.iter().flat_map(|bin| &bin.files).count(), 2);
    let first_repeat_report = maintenance.apply_optimize(&first_repeat, &config, &control())?;
    assert_eq!(first_repeat_report.outcome, Outcome::Noop);
    assert_eq!(first_repeat_report.nonbeneficial_bins, 1);
    let partition_dir = data_path(root.path(), &partial_outputs[0])?
        .parent()
        .context("partition directory")?
        .to_path_buf();
    let after_proof = fs::metadata(&partition_dir)?.modified()?;

    let unchanged = maintenance.plan_optimize(&config)?;
    assert_eq!(unchanged.bins.iter().flat_map(|bin| &bin.files).count(), 2);
    let unchanged_report = maintenance.apply_optimize(&unchanged, &config, &control())?;
    assert_eq!(unchanged_report.outcome, Outcome::Noop);
    assert_eq!(unchanged_report.nonbeneficial_bins, 1);
    assert_eq!(
        fs::metadata(&partition_dir)?.modified()?,
        after_proof,
        "a repeated no-benefit plan must not create or delete Parquet outputs"
    );

    drop(maintenance);
    let reopened = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let after_reopen_plan = reopened.plan_optimize(&config)?;
    assert_eq!(after_reopen_plan.bins.iter().flat_map(|bin| &bin.files).count(), 2);
    let after_reopen_report = reopened.apply_optimize(&after_reopen_plan, &config, &control())?;
    assert_eq!(after_reopen_report.outcome, Outcome::Noop);
    assert_eq!(after_reopen_report.nonbeneficial_bins, 1);
    assert_eq!(
        fs::metadata(&partition_dir)?.modified()?,
        after_proof,
        "the no-benefit proof must survive reopening without output churn"
    );

    drop(reopened);
    let writer = DeltaTable::open_url(
        url_of(root.path())?,
        customer_schema(),
        vec![INGESTION_PARTITION.into()],
    )?;
    append_customers(&writer, 0, &["zz"], 11)?;
    drop(writer);
    let changed = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let changed_plan = changed.plan_optimize(&config)?;
    assert_eq!(changed_plan.bins.iter().flat_map(|bin| &bin.files).count(), 3);
    let changed_report = changed.apply_optimize(&changed_plan, &config, &control())?;
    assert_eq!(
        (
            changed_report.outcome,
            changed_report.files_removed,
            changed_report.files_added,
            changed_report.rows_removed,
            changed_report.rows_added,
        ),
        (Outcome::Committed, 3, 2, 5, 5)
    );
    Ok(())
}
#[test]
fn optimize_rejects_writer_config_drift_before_effects() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open_url(
        url_of(root.path())?,
        customer_schema(),
        vec![INGESTION_PARTITION.into()],
    )?;
    append_customers(&table, 0, &["c2", "c1", "c0"], 0)?;
    append_customers(&table, 0, &["c5", "c4", "c3"], 3)?;
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let config = OptimizeConfig {
        layout: Some(LayoutPolicy {
            max_rows: 10,
            max_input_bytes: 1024 * 1024,
            row_group_rows: 10,
            page_rows: 2,
            ..LayoutPolicy::default()
        }),
        sealed_final: true,
        ..optimize_config()
    };
    let prepared = maintenance.prepare_optimize(&config)?;
    let version = maintenance.version()?;
    let original_writer = arrow_deltalake::writer::process_config()?.clone();
    let mut changed_writer = original_writer.clone();
    changed_writer.max_row_group_rows = Some(if original_writer.max_row_group_rows == Some(1) {
        2
    } else {
        1
    });
    let error = match maintenance.apply_prepared_optimize_with_writer_config(
        &prepared,
        &config,
        &control(),
        None,
        &changed_writer,
    ) {
        Err(error) => error,
        Ok(_) => bail!("writer policy drift must be rejected before writing outputs"),
    };
    assert_eq!(error_kind(&error), Some(MaintenanceErrorKind::InvalidPlan));
    assert_eq!(maintenance.version()?, version);
    assert_eq!(active_files(root.path())?.len(), 2);
    Ok(())
}

#[test]
fn optimize_relayouts_marked_files_when_writer_policy_changes_after_reopen() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open_url(
        url_of(root.path())?,
        customer_schema(),
        vec![INGESTION_PARTITION.into()],
    )?;
    append_customers(&table, 0, &["c0", "c1", "c2", "c3", "c4"], 0)?;
    append_customers(&table, 0, &["c5", "c6", "c7", "c8", "c9"], 5)?;
    let config = OptimizeConfig {
        layout: Some(LayoutPolicy {
            max_rows: 10,
            max_input_bytes: 1024 * 1024,
            row_group_rows: 10,
            page_rows: 2,
            ..LayoutPolicy::default()
        }),
        sealed_final: true,
        ..optimize_config()
    };
    let original_writer = arrow_deltalake::writer::ParquetWriterConfig::default();
    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let first = maintenance.prepare_optimize_with_writer_config(&config, &original_writer)?;
    let first_report =
        maintenance.apply_prepared_optimize_with_writer_config(&first, &config, &control(), None, &original_writer)?;
    assert_eq!(
        (
            first_report.files_removed,
            first_report.files_added,
            first_report.rows_removed,
            first_report.rows_added,
        ),
        (2, 1, 10, 10)
    );
    let first_output = active_files(root.path())?;
    assert_eq!(first_output.len(), 1);
    assert!(
        first_output
            .keys()
            .any(|path| path.contains("output-v2-") && path.contains("full-"))
    );

    drop(maintenance);
    drop(table);
    let reopened = DeltaTable::open_url(
        url_of(root.path())?,
        customer_schema(),
        vec![INGESTION_PARTITION.into()],
    )?;
    append_customers(&reopened, 0, &["c10", "c11", "c12"], 10)?;
    append_customers(&reopened, 0, &["c13", "c14", "c15"], 13)?;
    append_customers(&reopened, 0, &["c16", "c17", "c18"], 16)?;
    append_customers(&reopened, 0, &["c19", "c20", "c21"], 19)?;
    drop(reopened);

    let maintenance = DeltaTable::open_for_maintenance(&url_of(root.path())?)?;
    let changed_writer: arrow_deltalake::writer::ParquetWriterConfig = serde_json::from_value(serde_json::json!({
        "defaults": {"compression": "snappy"}
    }))?;
    let stale = maintenance.prepare_optimize_with_writer_config(&config, &original_writer)?;
    let version = maintenance.version()?;
    let error = match maintenance.apply_prepared_optimize_with_writer_config(
        &stale,
        &config,
        &control(),
        None,
        &changed_writer,
    ) {
        Err(error) => error,
        Ok(_) => bail!("writer policy drift must be rejected before writing outputs"),
    };
    assert_eq!(error_kind(&error), Some(MaintenanceErrorKind::InvalidPlan));
    assert_eq!(maintenance.version()?, version);
    assert_eq!(active_files(root.path())?.len(), 5);

    let changed = maintenance.prepare_optimize_with_writer_config(&config, &changed_writer)?;
    assert_eq!(changed.plan.bins.iter().flat_map(|bin| &bin.files).count(), 5);
    let report =
        maintenance.apply_prepared_optimize_with_writer_config(&changed, &config, &control(), None, &changed_writer)?;
    assert_eq!(
        (
            report.outcome,
            report.files_removed,
            report.files_added,
            report.rows_removed,
            report.rows_added,
        ),
        (Outcome::Committed, 5, 3, 22, 22),
        "unexpected OPTIMIZE report: {report:#?}"
    );
    let active = active_files(root.path())?;
    assert_eq!(active.len(), 3);
    let mut actual_rows = Vec::new();
    for path in active.keys() {
        let file_path = data_path(root.path(), path)?;
        assert_eq!(parquet_compression(&file_path)?, parquet::basic::Compression::SNAPPY);
        let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(file_path)?)?.build()?;
        let mut file_customers = Vec::new();
        for batch in reader {
            let batch = batch?;
            let customers = batch
                .column_by_name("customer_id")
                .context("customer_id")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("customers")?;
            let ids = batch
                .column_by_name("id")
                .context("id")?
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("ids")?;
            for row in 0..batch.num_rows() {
                file_customers.push(customers.value(row).to_owned());
                actual_rows.push((customers.value(row).to_owned(), ids.value(row)));
            }
        }
        assert!(file_customers.windows(2).all(|pair| pair[0] <= pair[1]));
    }
    actual_rows.sort();
    let mut expected_rows: Vec<_> = (0..22).map(|id| (format!("c{id}"), id)).collect();
    expected_rows.sort();
    assert_eq!(actual_rows, expected_rows);
    Ok(())
}

/// Commit-log fault injection for maintenance publication.
#[derive(Debug)]
struct UncertainStore {
    inner: LocalFileSystem,
    target: String,
    unreadable: AtomicBool,
    attempts: AtomicUsize,
    read_barrier: Option<Arc<BarrierFence>>,
    counts: Option<Arc<StorageCounts>>,
}

#[derive(Debug, Default)]
struct StorageCounts {
    reads: AtomicUsize,
    lists: AtomicUsize,
    writes: AtomicUsize,
}

impl StorageCounts {
    fn read(&self) {
        self.reads.fetch_add(1, Ordering::SeqCst);
    }

    fn write(&self) {
        self.writes.fetch_add(1, Ordering::SeqCst);
    }
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
        if let Some(counts) = &self.counts {
            counts.write();
        }
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
        if let Some(counts) = &self.counts {
            counts.write();
        }
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &object_store::path::Path, options: GetOptions) -> object_store::Result<GetResult> {
        if let Some(counts) = &self.counts {
            counts.read();
        }
        if path.as_ref().ends_with(".parquet")
            && let Some(barrier) = &self.read_barrier
        {
            barrier.wait().map_err(|error| object_store::Error::Generic {
                store: "input_read_barrier",
                source: Box::new(error),
            })?;
        }
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
        if let Some(counts) = &self.counts {
            counts.write();
        }
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&object_store::path::Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if let Some(counts) = &self.counts {
            counts.read();
            counts.lists.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&object_store::path::Path>) -> object_store::Result<ListResult> {
        if let Some(counts) = &self.counts {
            counts.read();
            counts.lists.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        if let Some(counts) = &self.counts {
            counts.write();
        }
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        if let Some(counts) = &self.counts {
            counts.write();
        }
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
            read_barrier: None,
            counts: None,
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
    let expected: Vec<String> = (0..checkpoint)
        .map(|version| format!("_delta_log/{version:020}.json"))
        .collect();
    assert_eq!(plan.deletions, expected, "JSON strictly below C");
    let dry = plan.report();
    assert_eq!((dry.outcome, dry.files_removed), (Outcome::Planned, checkpoint));
    assert_eq!(log_files(root.path())?, json_before, "dry run deletes nothing");
    assert_eq!(maintenance.log_floor()?, None, "dry run writes no floor");

    let report = maintenance.apply_prune_log(&plan, &config, &control())?;
    assert_eq!(report.maintenance_due_work, dry.maintenance_due_work);
    let evidence = report.maintenance_due_work.as_ref().context("applied prune evidence")?;
    assert_eq!(evidence.completeness, MaintenanceDueWorkCompleteness::Complete);
    assert_eq!(evidence.due, Some(true));
    assert_eq!(evidence.eligible_count, Some(checkpoint));
    assert_eq!(
        (report.outcome, report.files_deleted, report.version),
        (Outcome::Pruned, checkpoint, checkpoint)
    );
    assert_eq!(
        log_files(root.path())?,
        vec![json_path(root.path(), checkpoint)],
        "JSON < C pruned, C.json retained"
    );
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
    let mut expected: Vec<String> = (checkpoint..second)
        .map(|version| format!("_delta_log/{version:020}.json"))
        .collect();
    expected.insert(1, format!("_delta_log/{checkpoint:020}.checkpoint.parquet"));
    assert_eq!(again.deletions, expected, "the old floor's JSON becomes deletable");
    let report = reopened.apply_prune_log(&again, &short_prune(now_ms()? + 1_000), &control())?;
    assert_eq!(report.outcome, Outcome::Pruned);
    assert!(!checkpoint_path(root.path(), checkpoint).exists());
    assert!(checkpoint_path(root.path(), second).is_file());
    assert_eq!(log_files(root.path())?, vec![json_path(root.path(), second)]);
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
    let evidence = plan.report().maintenance_due_work.context("PRUNE_LOG evidence")?;
    assert_eq!(evidence.completeness, MaintenanceDueWorkCompleteness::Incomplete);
    assert_eq!(
        evidence.incomplete_reason,
        Some(MaintenanceDueWorkIncompleteReason::MissingCheckpointProof)
    );
    assert_eq!(evidence.due, None);
    assert_eq!(evidence.eligible_count, None);
    let applied = maintenance.apply_prune_log(&plan, &config, &control())?;
    assert_eq!(applied.outcome, Outcome::Noop);
    assert_eq!(applied.maintenance_due_work, plan.report().maintenance_due_work);
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
    assert_eq!(partial.backlog_files, checkpoint - 2);
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
    assert_eq!(complete.deletions.len(), usize::try_from(checkpoint - 2)?);
    let report = maintenance.apply_prune_log(&complete, &short_prune(now_ms()? + 1_000), &control())?;
    assert_eq!(report.outcome, Outcome::Pruned);
    assert_eq!(
        log_files(root.path())?,
        vec![
            json_path(root.path(), checkpoint),
            json_path(root.path(), checkpoint + 1)
        ]
    );
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

/// The planner's age stamp for a commit: the later of its `commitInfo.timestamp`
/// and its mtime. A coarse filesystem clock can record an mtime a tick earlier than
/// the commit timestamp, so mtime alone can place a cutoff on a still-young commit.
fn age_stamp_ms(path: &Path) -> Result<i64> {
    let committed = fs::read_to_string(path)?
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .filter_map(|action| action.ok()?["commitInfo"]["timestamp"].as_i64())
        .max();
    Ok(committed.map_or(modified_ms(path)?, |stamp| {
        stamp.max(modified_ms(path).unwrap_or(stamp))
    }))
}

fn modified_ms(path: &Path) -> Result<i64> {
    Ok(i64::try_from(
        fs::metadata(path)?
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?)
}

/// Scenario: `C.json` is young (it is retained, so it never blocks pruning), then the
/// floor advances from `F` to `C2` while the retained `F.json` is young.
/// Guarantees: the retained checkpoint's JSON is excluded from the age gate, but the
/// old floor's JSON is age-gated once it becomes deletable: Noop plan, StalePlan
/// apply and zero deletes until it ages, after which the advance deletes it.
#[test]
fn prune_log_retains_checkpoint_json_and_age_gates_old_floor_json() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (table, maintenance) = prunable_history(root.path())?;
    let floor = vacuum_and_checkpoint(&maintenance)?;
    let future = std::time::SystemTime::now() + Duration::from_secs(3_600);
    let set_modified = |path: PathBuf, time: std::time::SystemTime| -> Result<()> {
        fs::File::options().write(true).open(path)?.set_modified(time)?;
        Ok(())
    };
    set_modified(json_path(root.path(), floor), future)?;
    let config = short_prune(now_ms()? + 1_000);
    let plan = maintenance.plan_prune_log(&config)?;
    assert_eq!(plan.checkpoint_version, Some(floor), "young C.json does not block");
    maintenance.apply_prune_log(&plan, &config, &control())?;
    assert_eq!(log_files(root.path())?, vec![json_path(root.path(), floor)]);

    for index in 10..12 {
        append(&table, index, index * 10..index * 10 + 2)?;
    }
    set_modified(
        json_path(root.path(), floor),
        std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1),
    )?;
    let second = vacuum_and_checkpoint(&maintenance)?;
    let config = short_prune(now_ms()? + 1_000);
    let pending = maintenance.plan_prune_log(&config)?;
    assert_eq!(pending.checkpoint_version, Some(second));
    assert_eq!(pending.deletions[0], format!("_delta_log/{floor:020}.json"));

    set_modified(json_path(root.path(), floor), future)?;
    let before = log_files(root.path())?;
    let plan = maintenance.plan_prune_log(&config)?;
    assert_eq!((plan.checkpoint_version, plan.report().outcome), (None, Outcome::Noop));
    assert_eq!(
        kind(maintenance.apply_prune_log(&pending, &config, &control())),
        Some(MaintenanceErrorKind::StalePlan)
    );
    assert_eq!(
        log_files(root.path())?,
        before,
        "young old-floor JSON blocks the advance"
    );
    assert_eq!(maintenance.log_floor()?.context("floor")?.checkpoint_version, floor);

    set_modified(
        json_path(root.path(), floor),
        std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1),
    )?;
    let report = maintenance.apply_prune_log(&pending, &config, &control())?;
    assert_eq!(report.outcome, Outcome::Pruned);
    assert_eq!(log_files(root.path())?, vec![json_path(root.path(), second)]);
    assert_eq!(maintenance.log_floor()?.context("floor")?.checkpoint_version, second);
    Ok(())
}

/// Scenario: floor `F` with old `F.json`, then a newer checkpoint `C2` while the old
/// floor checkpoint `F.checkpoint.parquet` is young (e.g. freshly rewritten).
/// Guarantees: deleting checkpoints below `C2` is age-gated like JSON: Noop plan,
/// StalePlan apply and zero deletes until it ages, after which the advance deletes it.
#[test]
fn prune_log_age_gates_old_floor_checkpoint() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (table, maintenance) = prunable_history(root.path())?;
    let floor = vacuum_and_checkpoint(&maintenance)?;
    let config = short_prune(now_ms()? + 1_000);
    let plan = maintenance.plan_prune_log(&config)?;
    maintenance.apply_prune_log(&plan, &config, &control())?;
    for index in 10..12 {
        append(&table, index, index * 10..index * 10 + 2)?;
    }
    let second = vacuum_and_checkpoint(&maintenance)?;
    let config = short_prune(now_ms()? + 1_000);
    let pending = maintenance.plan_prune_log(&config)?;
    assert_eq!(pending.checkpoint_version, Some(second));
    let old_checkpoint = format!("_delta_log/{floor:020}.checkpoint.parquet");
    assert!(pending.deletions.contains(&old_checkpoint));

    let set_modified = |time: std::time::SystemTime| -> Result<()> {
        fs::File::options()
            .write(true)
            .open(checkpoint_path(root.path(), floor))?
            .set_modified(time)?;
        Ok(())
    };
    set_modified(std::time::SystemTime::now() + Duration::from_secs(3_600))?;
    let before = log_files(root.path())?;
    let plan = maintenance.plan_prune_log(&config)?;
    assert_eq!((plan.checkpoint_version, plan.report().outcome), (None, Outcome::Noop));
    assert_eq!(
        kind(maintenance.apply_prune_log(&pending, &config, &control())),
        Some(MaintenanceErrorKind::StalePlan)
    );
    assert_eq!(log_files(root.path())?, before);
    assert!(checkpoint_path(root.path(), floor).is_file(), "young checkpoint kept");
    assert_eq!(maintenance.log_floor()?.context("floor")?.checkpoint_version, floor);

    set_modified(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1))?;
    let report = maintenance.apply_prune_log(&pending, &config, &control())?;
    assert_eq!(report.outcome, Outcome::Pruned);
    assert!(!checkpoint_path(root.path(), floor).exists());
    assert_eq!(log_files(root.path())?, vec![json_path(root.path(), second)]);
    Ok(())
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
    assert_eq!(resume.deletions.len(), usize::try_from(checkpoint - 2)?);
    let all_files = log_files(root.path())?;
    let retained = json_path(root.path(), checkpoint);
    let leftovers: Vec<PathBuf> = all_files.iter().filter(|path| **path != retained).cloned().collect();
    let newest = leftovers
        .iter()
        .map(|path| age_stamp_ms(path))
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
    assert_eq!(log_files(root.path())?, all_files, "zero additional deletes");

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
    assert_eq!(log_files(root.path())?, vec![retained]);
    assert!(checkpoint_path(root.path(), checkpoint).is_file());
    Ok(())
}
