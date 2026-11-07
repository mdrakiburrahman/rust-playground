use anyhow::{Result, anyhow};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::{DeltaTable, INGESTION_PARTITION};
use arrow_deltalake::maintenance::{
    Granularity, MaintenanceControl, NegativeProbeConfig, OptimizeConfig, Outcome, ProbeOperation, now_ms,
};
use delta_maintenance::{DiscoveredTable, Metrics, Operation, Settings, TableDiscovery, run_cycle_with_discovery};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::Url;

const BASE: i64 = 1_704_067_200_000;

struct CatalogTable(DiscoveredTable);

impl TableDiscovery for CatalogTable {
    fn tables(&self) -> Result<Vec<String>> {
        Ok(vec![self.0.name.clone()])
    }

    fn table(&self, name: &str) -> Result<DiscoveredTable> {
        if name != self.0.name {
            return Err(anyhow!("unknown table"));
        }
        Ok(self.0.clone())
    }
}

fn fixture() -> Result<(tempfile::TempDir, Settings, CatalogTable)> {
    let root = tempfile::tempdir()?;
    let table_path = root.path().join("events");
    std::fs::create_dir(&table_path)?;
    let url = Url::from_directory_path(&table_path).map_err(|()| anyhow!("table URL"))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(INGESTION_PARTITION, DataType::Utf8, false),
    ]));
    let table = DeltaTable::open_url(url.clone(), Arc::clone(&schema), vec![INGESTION_PARTITION.into()])?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    for minute in 0..2 {
        for id in 0..2 {
            let stamp = format!("2024-01-01-00-{minute:02}");
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![id])),
                    Arc::new(StringArray::from(vec![stamp.as_str()])),
                ],
            )?;
            runtime.block_on(async {
                let mut file = table
                    .new_file_partitioned(
                        Arc::clone(&schema),
                        BTreeMap::from([(INGESTION_PARTITION.into(), Some(stamp.clone()))]),
                    )?
                    .with_ingestion_time(BASE + minute * 60_000)?;
                file.write(&batch).await?;
                file.finish().await?;
                Ok::<_, anyhow::Error>(())
            })?;
        }
    }
    let settings = Settings {
        root: Url::from_directory_path(root.path()).map_err(|()| anyhow!("root URL"))?,
        tables: vec!["events".into()],
        operations: vec![
            Operation::Optimize,
            Operation::Vacuum,
            Operation::Checkpoint,
            Operation::PruneLog,
        ],
        apply: true,
        interval: None,
        max_idle_backoff: Duration::from_secs(300),
        max_cycles: None,
        max_concurrent_tables: 1,
        operation_timeout: Duration::from_secs(30),
        granularity: Some(Granularity::Minute),
        purge_retention_ms: None,
        anchor_ms: Some(BASE + 3 * 60_000),
        vacuum_retention_ms: 604_800_000,
        allow_short_vacuum_retention: false,
        unsafe_short_vacuum_for_tests: false,
        optimize: OptimizeConfig::default(),
        checkpoint_every_commits: 100,
        log_retention_ms: None,
        max_prune_files: 100,
    };
    let source = CatalogTable(DiscoveredTable {
        name: "events".into(),
        url,
        log_token: None,
        properties: BTreeMap::from([
            ("arrowcol.optimize.minFiles".into(), "3".into()),
            ("arrowcol.partition.granularity".into(), "minute".into()),
            ("delta.checkpointInterval".into(), "100".into()),
        ]),
    });
    Ok((root, settings, source))
}

type FileInventory = BTreeMap<PathBuf, (Vec<u8>, SystemTime)>;

fn inventory(root: &Path) -> Result<FileInventory> {
    fn walk(root: &Path, path: &Path, result: &mut FileInventory) -> Result<()> {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                walk(root, &entry.path(), result)?;
            } else {
                result.insert(
                    entry.path().strip_prefix(root)?.to_owned(),
                    (std::fs::read(entry.path())?, entry.metadata()?.modified()?),
                );
            }
        }
        Ok(())
    }
    let mut result = BTreeMap::new();
    walk(root, root, &mut result)?;
    Ok(result)
}

#[test]
fn idle_catalog_policy_passes_have_no_log_checkpoint_or_progress_rewrites() -> Result<()> {
    let (root, settings, source) = fixture()?;
    let before = inventory(root.path())?;
    let metrics = Metrics::new();
    for _ in 0..2 {
        let results = run_cycle_with_discovery(&settings, &source, &metrics, &Arc::new(AtomicBool::new(false)))?;
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|r| r.error.is_none() && r.negative_probe));
        assert!(
            results
                .iter()
                .all(|r| r.report.as_ref().is_some_and(|r| r.outcome == Outcome::Noop))
        );
        assert_eq!(before, inventory(root.path())?);
    }
    assert!(metrics.render().contains("delta_maintenance_negative_probes_total"));
    Ok(())
}

#[test]
fn table_policy_overrides_process_minimum_and_only_compacts_sealed_pair() -> Result<()> {
    let (root, mut settings, mut source) = fixture()?;
    settings.operations = vec![Operation::Optimize];
    source
        .0
        .properties
        .insert("arrowcol.optimize.minFiles".into(), "2".into());
    let results = run_cycle_with_discovery(&settings, &source, &Metrics::new(), &Arc::new(AtomicBool::new(false)))?;
    assert_eq!(results.len(), 1);
    assert!(results[0].error.is_none(), "{:?}", results[0].error);
    assert_eq!(results[0].report.as_ref().map(|r| r.files_removed), Some(2));
    assert_eq!(
        std::fs::read_dir(root.path().join("events").join("__arrowcol_ingestion=2024-01-01-00-01"))?.count(),
        2
    );
    Ok(())
}

#[test]
fn invalid_catalog_properties_and_inventory_errors_fail_without_mutation() -> Result<()> {
    let (root, settings, mut source) = fixture()?;
    let before = inventory(root.path())?;
    for (key, value) in [
        ("arrowcol.optimize.minFile", "2"),
        ("arrowcol.vacuum.retention", "6 days"),
        ("arrowcol.vacuum.maxListedFiles", "1"),
    ] {
        source.0.properties.insert(key.into(), value.into());
        let results = run_cycle_with_discovery(&settings, &source, &Metrics::new(), &Arc::new(AtomicBool::new(false)))?;
        assert!(results.iter().all(|r| r.error.is_some() && !r.negative_probe), "{key}");
        assert_eq!(before, inventory(root.path())?);
        source.0.properties.remove(key);
    }
    Ok(())
}

#[test]
fn physical_timestamp_retention_boundary_forces_replay_only_strictly_before_cutoff() -> Result<()> {
    let (root, _, source) = fixture()?;
    let table = DeltaTable::open_for_maintenance_probe(&source.0.url)?;
    let anchor = now_ms()?;
    let cutoff = anchor - 604_800_000;
    let config = NegativeProbeConfig {
        now_ms: anchor,
        granularity: Granularity::Minute,
        purge_retention_ms: 2_592_000_000,
        vacuum_retention_ms: 604_800_000,
        log_retention_ms: 2_592_000_000,
        checkpoint_interval: 100,
        optimize_small_file_bytes: 64 * 1024 * 1024,
        optimize_min_files: 2,
        sealed_partitions_only: true,
        max_listed_files: 1000,
    };
    let data = inventory(root.path())?
        .keys()
        .find(|p| p.extension().is_some_and(|ext| ext == "parquet"))
        .cloned()
        .ok_or_else(|| anyhow!("data file"))?;
    let file = std::fs::File::open(root.path().join(data))?;
    for (stamp, expected_noop) in [(cutoff, true), (cutoff - 1, false)] {
        file.set_times(
            std::fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_millis(u64::try_from(stamp)?)),
        )?;
        let result = table.probe_no_work(ProbeOperation::Vacuum, &config, &MaintenanceControl::default())?;
        assert_eq!(result.is_some(), expected_noop);
    }
    Ok(())
}

#[test]
fn repeated_full_vacuum_noop_does_not_rewrite_existing_progress_record() -> Result<()> {
    let (root, _, source) = fixture()?;
    let table = DeltaTable::open_for_maintenance(&source.0.url)?;
    let config = arrow_deltalake::maintenance::VacuumConfig::new(604_800_000, now_ms()?);
    table.vacuum_once(&config, true, &MaintenanceControl::default())?;
    assert!(
        root.path()
            .join("events/_arrowcol_maintenance/vacuum-lite.json")
            .is_file()
    );
    let before = inventory(root.path())?;
    let later = arrow_deltalake::maintenance::VacuumConfig {
        now_ms: config.now_ms + 1,
        ..config
    };
    let report = table.vacuum_once(&later, true, &MaintenanceControl::default())?;
    assert_eq!(report.outcome, Outcome::Noop);
    assert_eq!(before, inventory(root.path())?);
    Ok(())
}

#[test]
fn prepared_optimize_rejects_concurrent_schema_change_before_any_rewrite() -> Result<()> {
    let (root, _, source) = fixture()?;
    let table = DeltaTable::open_for_maintenance(&source.0.url)?;
    let config = OptimizeConfig {
        granularity: Some(Granularity::Minute),
        now_ms: Some(BASE + 3 * 60_000),
        ..Default::default()
    };
    let prepared = table.prepare_optimize(&config)?;
    assert!(!prepared.plan.bins.is_empty());
    let evolved = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(INGESTION_PARTITION, DataType::Utf8, false),
        Field::new("extra", DataType::Utf8, true),
    ]));
    DeltaTable::open_url(source.0.url, evolved, vec![INGESTION_PARTITION.into()])?;
    let before = inventory(root.path())?;
    let error = match table.apply_prepared_optimize(&prepared, &config, &MaintenanceControl::default()) {
        Err(error) => error,
        Ok(_) => return Err(anyhow!("stale prepared state unexpectedly applied")),
    };
    assert_eq!(
        arrow_deltalake::maintenance::error_kind(&error),
        Some(arrow_deltalake::maintenance::MaintenanceErrorKind::MetadataChanged)
    );
    assert_eq!(before, inventory(root.path())?);
    Ok(())
}
