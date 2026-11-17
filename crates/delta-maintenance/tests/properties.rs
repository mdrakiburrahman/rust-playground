use anyhow::{Context, Result, anyhow};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::{DeltaTable, INGESTION_PARTITION};
use arrow_deltalake::maintenance::test_support::CountingObjectStore;
use arrow_deltalake::maintenance::{
    Granularity, MaintenanceControl, MaintenanceDueWorkCompleteness, MaintenanceError, MaintenanceErrorKind,
    NegativeProbeConfig, OptimizeConfig, Outcome, ProbeOperation, VacuumConfig, now_ms,
};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_maintenance::{
    DiscoveredTable, FilesystemDiscovery, Metrics, Operation, OptimizeUploadMode, Settings, TableDiscovery,
    run_cycle_with_discovery, run_with_discovery, startup_configuration,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::Url;

const BASE: i64 = 1_704_067_200_000;

struct CatalogTable(DiscoveredTable, Option<Arc<DeltaTable>>);

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

    fn open_probe_table(&self, url: &Url, max_log_files: usize) -> Result<Arc<DeltaTable>> {
        self.1.as_ref().map(Arc::clone).map_or_else(
            || DeltaTable::open_for_maintenance_probe_with_limit(url, max_log_files),
            Ok,
        )
    }

    fn open_maintenance_table(&self, url: &Url) -> Result<Arc<DeltaTable>> {
        self.1
            .as_ref()
            .map(Arc::clone)
            .map_or_else(|| DeltaTable::open_for_maintenance(url), Ok)
    }
}

struct StopAtMaintenanceClock {
    source: CatalogTable,
    stop: Arc<AtomicBool>,
}

impl TableDiscovery for StopAtMaintenanceClock {
    fn tables(&self) -> Result<Vec<String>> {
        self.source.tables()
    }

    fn table(&self, name: &str) -> Result<DiscoveredTable> {
        self.source.table(name)
    }

    fn maintenance_now_ms(&self) -> Result<i64> {
        self.stop.store(true, Ordering::SeqCst);
        Ok(BASE + 3 * 60_000)
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
    write_pairs(&table, schema)?;
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
        max_consecutive_stale_plans: 8,
        max_concurrent_tables: 1,
        max_concurrent_decoded_bytes: None,
        operation_timeout: Duration::from_secs(30),
        commit_attempts: 8,
        commit_budget: Duration::from_secs(10),
        catalog_options: Default::default(),
        granularity: Some(Granularity::Minute),
        purge_retention_ms: None,
        anchor_ms: Some(BASE + 3 * 60_000),
        vacuum_retention_ms: 604_800_000,
        allow_short_vacuum_retention: false,
        unsafe_short_vacuum_for_tests: false,
        optimize: OptimizeConfig::default(),
        process_properties: BTreeMap::new(),
        checkpoint_every_commits: 100,
        log_retention_ms: None,
        max_prune_files: 100,
        operation_limits: Default::default(),
    };
    let source = CatalogTable(
        DiscoveredTable {
            name: "events".into(),
            url,
            log_token: None,
            properties: BTreeMap::from([
                ("arrowcol.optimize.minFiles".into(), "3".into()),
                ("arrowcol.purge.enabled".into(), "true".into()),
                (
                    "arrowcol.purge.retention".into(),
                    (30 * 24 * 60 * 60 * 1000_i64).to_string(),
                ),
                ("arrowcol.partition.granularity".into(), "minute".into()),
                ("delta.checkpointInterval".into(), "100".into()),
            ]),
        },
        None,
    );
    persist_policy(&source)?;
    Ok((root, settings, source))
}

struct MissingDiscovery;

impl TableDiscovery for MissingDiscovery {
    fn tables(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn table(&self, _name: &str) -> Result<DiscoveredTable> {
        Err(anyhow!("unexpected table lookup"))
    }
}

#[test]
fn one_shot_rejects_missing_allowlisted_tables_before_reporting_success() -> Result<()> {
    let (_root, settings, _) = fixture()?;
    let error = match run_cycle_with_discovery(
        &settings,
        &MissingDiscovery,
        &Metrics::new(),
        &Arc::new(AtomicBool::new(false)),
    ) {
        Ok(results) => return Err(anyhow!("missing allowlisted table produced results: {results:?}")),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("allowlisted tables were missing or ambiguous")
    );
    Ok(())
}

#[test]
fn graceful_stop_does_not_interrupt_an_admitted_table_operation() -> Result<()> {
    let (_root, mut settings, mut source) = fixture()?;
    settings.operations = vec![Operation::Optimize];
    settings.anchor_ms = None;
    source
        .0
        .properties
        .insert("arrowcol.optimize.minFiles".into(), "2".into());
    persist_policy(&source)?;
    let stop = Arc::new(AtomicBool::new(false));
    let discovery = StopAtMaintenanceClock {
        source,
        stop: Arc::clone(&stop),
    };

    let results = run_cycle_with_discovery(&settings, &discovery, &Metrics::new(), &stop)?;

    assert!(
        stop.load(Ordering::SeqCst),
        "the test must request shutdown after table admission"
    );
    assert_eq!(
        results.len(),
        1,
        "the admitted OPTIMIZE operation must produce a result: {results:?}"
    );
    assert_eq!(results[0].operation, Operation::Optimize);
    assert!(
        results[0].error_kind.is_none(),
        "graceful shutdown interrupted admitted work: {results:?}"
    );
    let report = results[0].report.as_ref().context("admitted OPTIMIZE report")?;
    assert_eq!(report.outcome, Outcome::Committed);
    assert!(report.files_removed > 0 && report.files_added > 0, "{report:?}");
    assert_eq!((report.rows_removed, report.rows_added), (2, 2));
    Ok(())
}

#[test]
fn automatic_layout_uses_table_policy_over_process_and_reports_applied_source() -> Result<()> {
    let (_root, mut settings, mut source) = fixture()?;
    settings.operations = vec![Operation::Optimize];
    settings
        .process_properties
        .insert("arrowcol.optimize.minFiles".into(), "2".into());
    settings
        .process_properties
        .insert("arrowcol.layout.sortColumns".into(), "missing".into());
    settings
        .process_properties
        .insert("arrowcol.layout.maxRows".into(), "3".into());
    settings
        .process_properties
        .insert("arrowcol.layout.bloomColumns".into(), "customer_id".into());
    source
        .0
        .properties
        .insert("arrowcol.optimize.minFiles".into(), "2".into());
    source
        .0
        .properties
        .insert("arrowcol.layout.sortColumns".into(), "id".into());
    source.0.properties.insert("arrowcol.layout.maxRows".into(), "2".into());
    source
        .0
        .properties
        .insert("arrowcol.layout.bloomColumns".into(), "id".into());
    persist_policy(&source)?;

    let results = run_cycle_with_discovery(&settings, &source, &Metrics::new(), &Arc::new(AtomicBool::new(false)))?;
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(results[0].error_kind.is_none(), "{results:?}");
    let evidence = results[0].optimize.as_ref().context("OPTIMIZE evidence")?;
    assert_eq!(evidence.config["layout_mode"], "automatic");
    assert_eq!(evidence.config["layout_applied"], true);
    assert_eq!(evidence.config["layout"]["sort_columns"], serde_json::json!(["id"]));
    assert_eq!(evidence.config["layout"]["max_rows"], 2);
    assert_eq!(evidence.config["layout"]["bloom_columns"], serde_json::json!(["id"]));
    assert_eq!(
        evidence.config["layout_sources"]["arrowcol.layout.bloomColumns"],
        "table"
    );
    assert_eq!(
        evidence.config["layout_sources"]["arrowcol.layout.sortColumns"],
        "table"
    );
    assert_eq!(evidence.config["layout_sources"]["arrowcol.layout.maxRows"], "table");
    assert_eq!(
        evidence.config["layout_sources"]["arrowcol.layout.rowGroupRows"],
        "registry"
    );
    Ok(())
}

#[test]
fn scheduled_local_discovery_reports_pending_allowlisted_tables() -> Result<()> {
    let (_root, mut settings, _) = fixture()?;
    settings.tables = vec!["not-yet-created".into()];
    settings.interval = Some(Duration::from_millis(1));
    settings.max_cycles = Some(2);
    let discovery = FilesystemDiscovery::new(settings.root.clone(), settings.tables.clone());
    let metrics = Metrics::new();
    assert!(run_with_discovery(
        &settings,
        &discovery,
        &metrics,
        &Arc::new(AtomicBool::new(false))
    ));
    assert!(discovery.tables()?.is_empty());
    assert_eq!(discovery.pending_tables()?, vec!["not-yet-created"]);
    assert!(metrics.render().contains("delta_maintenance_pending_tables 1"));
    Ok(())
}

type FileInventory = BTreeMap<PathBuf, (Vec<u8>, SystemTime)>;

#[test]
fn setup_errors_preserve_typed_stale_plan_without_classifying_plain_text() -> Result<()> {
    struct FailedDiscovery(bool);
    impl TableDiscovery for FailedDiscovery {
        fn tables(&self) -> Result<Vec<String>> {
            Ok(vec!["events".into()])
        }
        fn table(&self, _: &str) -> Result<DiscoveredTable> {
            if self.0 {
                Err(anyhow::Error::new(MaintenanceError {
                    kind: MaintenanceErrorKind::StalePlan,
                    message: "table advanced during negative probe; retry".into(),
                    report: None,
                })
                .context("loading table properties"))
            } else {
                Err(anyhow!("stale_plan: table advanced during negative probe; retry"))
            }
        }
    }

    let (root, mut settings, _) = fixture()?;
    settings.operations = vec![Operation::Purge];
    let before = inventory(root.path())?;
    for (typed, expected) in [(true, "stale_plan"), (false, "failure")] {
        let results = run_cycle_with_discovery(
            &settings,
            &FailedDiscovery(typed),
            &Metrics::new(),
            &Arc::new(AtomicBool::new(false)),
        )?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].error_kind, Some(expected));
        assert!(!results[0].negative_probe);
        assert!(results[0].report.is_none());
        assert!(
            results[0]
                .error
                .as_deref()
                .is_some_and(|error| error.contains("table advanced during negative probe"))
        );
        assert_eq!(before, inventory(root.path())?);
    }
    Ok(())
}

fn persist_policy(source: &CatalogTable) -> Result<()> {
    let table = DeltaTable::open_catalog(source.0.url.clone())?;
    let current = table.catalog_snapshot()?.ok_or_else(|| anyhow!("table metadata"))?;
    table.commit_catalog(
        Some(current.version),
        current.schema,
        current.partition_columns,
        source.0.properties.clone(),
        current.comment,
    )?;
    Ok(())
}

fn write_pairs(table: &Arc<DeltaTable>, schema: Arc<Schema>) -> Result<()> {
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
    Ok(())
}

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
    let (root, mut settings, mut source) = fixture()?;
    settings.operations.insert(0, Operation::Purge);
    settings.purge_retention_ms = Some(30 * 24 * 60 * 60 * 1000);
    let store = Arc::new(CountingObjectStore::new(Arc::new(LocalFileSystem::new())));
    let table_path = source.0.url.to_file_path().map_err(|()| anyhow!("table path"))?;
    let counted_store = Arc::clone(&store);
    let counted_store: Arc<dyn delta_kernel::object_store::ObjectStore> = counted_store;
    source.1 = Some(DeltaTable::open_with_store(table_path, counted_store)?);
    let maintenance = source.1.as_ref().context("counted maintenance table")?;
    let anchor = settings.anchor_ms.context("test maintenance anchor")?;
    assert_eq!(
        maintenance.checkpoint_if_due_at(1, true, anchor)?.outcome,
        Outcome::Checkpointed
    );
    assert_eq!(
        maintenance
            .vacuum_once(
                &VacuumConfig::new(settings.vacuum_retention_ms, anchor),
                true,
                &MaintenanceControl::default()
            )?
            .outcome,
        Outcome::Noop
    );
    let before = inventory(root.path())?;
    let metrics = Metrics::new();
    for _ in 0..2 {
        store.reset();
        let results = run_cycle_with_discovery(&settings, &source, &metrics, &Arc::new(AtomicBool::new(false)))?;
        assert_eq!(results.len(), 5);
        assert!(results.iter().all(|r| r.error.is_none()));
        assert_eq!(results.iter().filter(|r| r.negative_probe).count(), 2);
        assert_eq!(results[0].operation, Operation::Purge);
        assert!(results.iter().all(|result| {
            result.probe_duration_ms.saturating_add(result.plan_apply_duration_ms)
                <= result.duration_ms.saturating_add(1)
        }));
        assert!(
            results
                .iter()
                .all(|r| r.report.as_ref().is_some_and(|r| r.outcome == Outcome::Noop))
        );
        for result in &results {
            let evidence = result
                .report
                .as_ref()
                .and_then(|report| report.maintenance_due_work.as_ref())
                .context("each idle operation reports bounded due-work evidence")?;
            assert_eq!(evidence.observed_at_ms, anchor, "{:?} anchor", result.operation);
            assert_eq!(evidence.completeness, MaintenanceDueWorkCompleteness::Complete);
            assert_eq!(evidence.due, Some(false));
            assert_eq!(evidence.eligible_count, Some(0));
        }
        let serde_fixture = serde_json::to_value(&results[1])?;
        assert!(serde_fixture.get("error").is_none());
        assert!(serde_fixture.get("error_kind").is_none());
        assert_eq!(serde_fixture["operation"], "optimize");
        assert_eq!(serde_fixture["report"]["interrupted"], false);
        assert_eq!(serde_fixture["report"]["outcome"], "noop");
        let protected = &serde_fixture["report"]["compaction_debt"]["protected_partition_backlog"];
        assert_eq!(protected["files"], 2);
        assert!(protected["bytes"].as_u64().is_some_and(|bytes| bytes > 0));
        assert_eq!(protected["oldest_partition_start_ms"], BASE + 60_000);
        assert_eq!(
            serde_fixture["report"]["maintenance_due_work"],
            serde_json::json!({
                "observed_at_ms": anchor,
                "source": "snapshot_active_files",
                "completeness": "complete",
                "unit": "parquet_files",
                "due": false,
                "eligible_count": 0,
                "eligible_bytes": 0,
                "backlog_count": 0,
                "backlog_bytes": 0,
                "oldest_eligible_timestamp_ms": null,
                "oldest_timestamp_basis": null,
                "incomplete_reason": null
            })
        );
        let mut failed = results[1].clone();
        failed.error_kind = Some("resource_limit");
        failed.error = Some("fixture failure after planning".into());
        let mut interrupted = results[1].clone();
        interrupted.report.as_mut().context("fixture report")?.interrupted = true;
        let mut partial = results[1].clone();
        partial.report.as_mut().context("fixture report")?.outcome = Outcome::Partial;
        let mut missing = results[1].clone();
        missing.report = None;
        let wrapper_fixtures = serde_json::json!([
            {"case":"complete", "expected_eligibility":"complete", "result":serde_fixture},
            {"case":"error", "expected_eligibility":"unknown", "result":failed},
            {"case":"interrupted", "expected_eligibility":"unknown", "result":interrupted},
            {"case":"partial", "expected_eligibility":"unknown", "result":partial},
            {"case":"missing_report", "expected_eligibility":"unknown", "result":missing}
        ]);
        assert_eq!(wrapper_fixtures[1]["result"]["error_kind"], "resource_limit");
        assert_eq!(wrapper_fixtures[2]["result"]["report"]["interrupted"], true);
        assert_eq!(wrapper_fixtures[3]["result"]["report"]["outcome"], "partial");
        assert!(wrapper_fixtures[4]["result"].get("report").is_none());
        let fixtures = serde_json::json!({
            "operation_results": results,
            "qualification_cases": wrapper_fixtures
        });
        println!("maintenance_due_work_serde_fixtures={fixtures}");
        assert_eq!(before, inventory(root.path())?);
        let (reads, lists, writes) = store.snapshot();
        assert!(reads > 0);
        assert!(
            lists > 0 && lists <= 24,
            "bounded idle pass made {lists} object listings"
        );
        assert!(
            reads <= 80,
            "bounded idle pass with active protected-debt scan made {reads} object reads/listings"
        );
        assert_eq!(writes, 0, "idle pass must not write objects");
    }
    assert!(metrics.render().contains("delta_maintenance_negative_probes_total"));
    Ok(())
}

#[test]
fn later_vacuum_probe_uses_fresh_due_time_after_earlier_operation() -> Result<()> {
    struct AdvancingClock {
        source: CatalogTable,
        anchors: [i64; 2],
        calls: AtomicUsize,
    }

    impl TableDiscovery for AdvancingClock {
        fn tables(&self) -> Result<Vec<String>> {
            self.source.tables()
        }

        fn table(&self, name: &str) -> Result<DiscoveredTable> {
            self.source.table(name)
        }

        fn maintenance_now_ms(&self) -> Result<i64> {
            let index = self.calls.fetch_add(1, Ordering::SeqCst).min(1);
            Ok(self.anchors[index])
        }
    }

    let (root, mut settings, mut source) = fixture()?;
    settings.operations = vec![Operation::Purge, Operation::Vacuum];
    source.0.properties.insert(
        "arrowcol.purge.retention".into(),
        (100_i64 * 365 * 24 * 60 * 60 * 1000).to_string(),
    );
    persist_policy(&source)?;
    settings.anchor_ms = None;
    let first_anchor = now_ms()?;
    let second_anchor = first_anchor + 2_000;
    let retention = settings.vacuum_retention_ms;
    let physical_mtime = first_anchor - retention + 1_000;
    let data_dir = root.path().join("events");
    let parquet_path = std::fs::read_dir(&data_dir)?
        .flat_map(|entry| {
            let path = entry.ok()?.path();
            (path.is_dir()).then(|| std::fs::read_dir(path).ok()).flatten()
        })
        .flatten()
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|extension| extension == "parquet"))
        .ok_or_else(|| anyhow!("active Parquet file"))?;
    let modified = std::time::UNIX_EPOCH + Duration::from_millis(u64::try_from(physical_mtime)?);
    std::fs::File::open(&parquet_path)?.set_times(std::fs::FileTimes::new().set_modified(modified))?;
    let discovery = AdvancingClock {
        source,
        anchors: [first_anchor, second_anchor],
        calls: AtomicUsize::new(0),
    };
    let before = inventory(root.path())?;
    let results = run_cycle_with_discovery(
        &settings,
        &discovery,
        &Metrics::new(),
        &Arc::new(AtomicBool::new(false)),
    )?;
    assert_eq!(discovery.calls.load(Ordering::SeqCst), 2);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].operation, Operation::Purge);
    assert!(results[0].negative_probe, "purge remains not due");
    assert_eq!(results[1].operation, Operation::Vacuum);
    assert!(
        !results[1].negative_probe,
        "vacuum crossed its physical-retention boundary"
    );
    assert!(results[1].error.is_none(), "{:?}", results[1].error);
    assert_eq!(
        results[1].report.as_ref().map(|report| report.outcome),
        Some(Outcome::Noop)
    );
    let after = inventory(root.path())?;
    let durable_objects = |objects: &FileInventory| {
        objects
            .keys()
            .filter(|path| {
                path.extension().is_some_and(|extension| extension == "parquet")
                    || path.to_string_lossy().contains("/_delta_log/")
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        durable_objects(&before),
        durable_objects(&after),
        "due vacuum must not delete data or write Delta log actions when no tombstones exist"
    );
    let report = results[1]
        .report
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("vacuum report"))?;
    assert_eq!(report.version, report.read_version, "no Delta commit");
    assert_eq!(report.files_deleted, 0);
    Ok(())
}

#[test]
fn table_policy_overrides_process_minimum_and_only_compacts_sealed_pair() -> Result<()> {
    let (root, mut settings, mut source) = fixture()?;
    settings.operations = vec![Operation::Optimize];
    settings
        .process_properties
        .insert("arrowcol.optimize.minFiles".into(), "4".into());
    source
        .0
        .properties
        .insert("arrowcol.optimize.minFiles".into(), "2".into());
    persist_policy(&source)?;
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
fn invalid_process_properties_are_rejected_before_a_pass() -> Result<()> {
    let (_, mut settings, _) = fixture()?;
    settings
        .process_properties
        .insert("arrowcol.optimize.minFiles".into(), "1".into());
    assert!(settings.validate().is_err());
    Ok(())
}

#[test]
fn decoded_budget_rejects_table_override_without_mutating_data() -> Result<()> {
    let (root, mut settings, mut source) = fixture()?;
    settings.operations = vec![Operation::Optimize];
    settings.max_concurrent_tables = 2;
    settings.max_concurrent_decoded_bytes = Some(2 * 1024 * 1024 * 1024);
    settings.validate()?;
    source
        .0
        .properties
        .insert("arrowcol.optimize.maxDecodedSizePerBin".into(), "2 GiB".into());
    persist_policy(&source)?;
    let before = inventory(root.path())?;
    let results = run_cycle_with_discovery(&settings, &source, &Metrics::new(), &Arc::new(AtomicBool::new(false)))?;
    assert!(
        results[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("aggregate decode budget"))
    );
    assert_eq!(before, inventory(root.path())?);
    Ok(())
}

#[test]
fn changed_policy_bypasses_old_schedule_without_repeating_an_unchanged_idle_pass() -> Result<()> {
    struct UpdatingDiscovery(Mutex<(usize, CatalogTable, FileInventory, PathBuf)>);

    impl TableDiscovery for UpdatingDiscovery {
        fn tables(&self) -> Result<Vec<String>> {
            let mut state = self.0.lock().map_err(|error| anyhow!("discovery poisoned: {error}"))?;
            state.0 += 1;
            if state.0 == 3 {
                assert_eq!(state.2, inventory(&state.3)?);
                state
                    .1
                    .0
                    .properties
                    .insert("arrowcol.optimize.minFiles".into(), "2".into());
                persist_policy(&state.1)?;
            }
            state.1.tables()
        }

        fn table(&self, name: &str) -> Result<DiscoveredTable> {
            self.0
                .lock()
                .map_err(|error| anyhow!("discovery poisoned: {error}"))?
                .1
                .table(name)
        }
    }

    let (root, mut settings, mut source) = fixture()?;
    settings.operations = vec![Operation::Optimize];
    settings.interval = Some(Duration::from_millis(1));
    settings.max_idle_backoff = Duration::from_millis(1);
    settings.max_cycles = Some(3);
    source
        .0
        .properties
        .insert("arrowcol.maintenance.interval".into(), "60000".into());
    persist_policy(&source)?;
    let initial = DeltaTable::open_catalog(source.0.url.clone())?
        .catalog_snapshot()?
        .ok_or_else(|| anyhow!("initial table metadata missing"))?
        .version;
    let discovery = UpdatingDiscovery(Mutex::new((0, source, inventory(root.path())?, root.path().into())));
    assert!(run_with_discovery(
        &settings,
        &discovery,
        &Metrics::new(),
        &Arc::new(AtomicBool::new(false))
    ));
    let state = discovery
        .0
        .lock()
        .map_err(|error| anyhow!("discovery poisoned: {error}"))?;
    assert_eq!(state.0, 3);
    let snapshot = DeltaTable::open_catalog(state.1.0.url.clone())?
        .catalog_snapshot()?
        .ok_or_else(|| anyhow!("updated table metadata missing"))?;
    assert_eq!(
        snapshot.version,
        initial + 2,
        "one metadata update and one OPTIMIZE, before the old 60s deadline"
    );
    assert_eq!(
        std::fs::read_dir(root.path().join("events").join("__arrowcol_ingestion=2024-01-01-00-00"))?.count(),
        3
    );
    assert_eq!(
        std::fs::read_dir(root.path().join("events").join("__arrowcol_ingestion=2024-01-01-00-01"))?.count(),
        2
    );
    Ok(())
}

#[test]
fn invalid_catalog_properties_and_inventory_errors_fail_without_mutation() -> Result<()> {
    let (root, settings, mut source) = fixture()?;
    let maintenance = DeltaTable::open_for_maintenance(&source.0.url)?;
    let anchor = settings.anchor_ms.context("test maintenance anchor")?;
    maintenance.checkpoint_if_due_at(1, true, anchor)?;
    maintenance.vacuum_once(
        &VacuumConfig::new(settings.vacuum_retention_ms, anchor),
        true,
        &MaintenanceControl::default(),
    )?;
    let before = inventory(root.path())?;
    for (key, value) in [
        ("arrowcol.optimize.minFile", "2"),
        ("arrowcol.vacuum.retention", "6 days"),
        ("arrowcol.vacuum.maxListedFiles", "1"),
    ] {
        source.0.properties.insert(key.into(), value.into());
        let before = if key == "arrowcol.vacuum.maxListedFiles" {
            persist_policy(&source)?;
            let maintenance = DeltaTable::open_for_maintenance(&source.0.url)?;
            let anchor = settings.anchor_ms.context("test maintenance anchor")?;
            maintenance.checkpoint_if_due_at(1, true, anchor)?;
            maintenance.vacuum_once(
                &VacuumConfig::new(settings.vacuum_retention_ms, anchor),
                true,
                &MaintenanceControl::default(),
            )?;
            inventory(root.path())?
        } else {
            before.clone()
        };
        let results = run_cycle_with_discovery(&settings, &source, &Metrics::new(), &Arc::new(AtomicBool::new(false)))?;
        if key == "arrowcol.vacuum.maxListedFiles" {
            assert!(
                results
                    .iter()
                    .filter(|result| result.operation != Operation::Vacuum)
                    .all(|result| result.error.is_some() && !result.negative_probe),
                "{key}"
            );
            let vacuum = results
                .iter()
                .find(|result| result.operation == Operation::Vacuum)
                .context("VACUUM result")?;
            assert!(vacuum.error.is_none());
            assert!(
                vacuum
                    .report
                    .as_ref()
                    .and_then(|report| report.maintenance_due_work.as_ref())
                    .is_some_and(|evidence| evidence.completeness == MaintenanceDueWorkCompleteness::Complete)
            );
        } else {
            assert!(results.iter().all(|r| r.error.is_some() && !r.negative_probe), "{key}");
        }
        assert_eq!(before, inventory(root.path())?);
        source.0.properties.remove(key);
    }
    persist_policy(&source)?;
    let maintenance = DeltaTable::open_for_maintenance(&source.0.url)?;
    maintenance.vacuum_once(
        &VacuumConfig::new(settings.vacuum_retention_ms, anchor),
        true,
        &MaintenanceControl::default(),
    )?;
    let hint = root.path().join("events/_delta_log/_last_checkpoint");
    for content in ["not-json", "{\"version\":99999}"] {
        std::fs::write(&hint, content)?;
        let malformed = inventory(root.path())?;
        let results = run_cycle_with_discovery(&settings, &source, &Metrics::new(), &Arc::new(AtomicBool::new(false)))?;
        assert!(
            results
                .iter()
                .filter(|result| result.operation != Operation::Vacuum)
                .all(|result| result.error.is_some() && !result.negative_probe)
        );
        let vacuum = results
            .iter()
            .find(|result| result.operation == Operation::Vacuum)
            .context("VACUUM result")?;
        if vacuum.error.is_none() {
            assert!(
                vacuum
                    .report
                    .as_ref()
                    .and_then(|report| report.maintenance_due_work.as_ref())
                    .is_some()
            );
        }
        let after = inventory(root.path())?;
        assert_eq!(malformed, after);
    }
    std::fs::remove_file(hint)?;
    std::fs::remove_file(root.path().join("events/_delta_log/00000000000000000001.json"))?;
    let gap = inventory(root.path())?;
    let results = run_cycle_with_discovery(&settings, &source, &Metrics::new(), &Arc::new(AtomicBool::new(false)))?;
    assert!(results.iter().all(|result| !result.negative_probe));
    assert!(results.iter().any(|result| {
        result.operation == Operation::Vacuum
            && result
                .error
                .as_ref()
                .is_some_and(|error| error.contains("complete JSON log history"))
    }));
    assert_eq!(gap, inventory(root.path())?);
    Ok(())
}

#[test]
fn vacuum_probe_does_not_infer_tombstones_from_physical_timestamp() -> Result<()> {
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
        optimize_target_file_bytes: 128 * 1024 * 1024,
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
    for stamp in [cutoff, cutoff - 1] {
        file.set_times(
            std::fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_millis(u64::try_from(stamp)?)),
        )?;
        assert!(
            table
                .probe_no_work(ProbeOperation::Vacuum, &config, &MaintenanceControl::default())?
                .is_none(),
            "physical age is not a tombstone observation"
        );
        let report = table.vacuum_once(
            &VacuumConfig::new(604_800_000, anchor),
            false,
            &MaintenanceControl::default(),
        )?;
        let evidence = report.maintenance_due_work.context("bounded tombstone evidence")?;
        assert_eq!(evidence.due, Some(false));
        assert_eq!(evidence.eligible_count, Some(0));
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

#[test]
fn real_catalog_ddl_discovery_refreshes_policy_without_idle_writes() -> Result<()> {
    use arrow_flight::{flight_service_server::FlightServiceServer, sql::client::FlightSqlServiceClient};
    use arrowcol_catalog_client::{InternalRequestSigner, rpc::schema_catalog_server::SchemaCatalogServer};
    use arrowcol_catalog_rust::{CatalogConfig, CatalogService};
    use delta_maintenance::catalog::CatalogDiscovery;

    let (root, settings, _) = fixture()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let service = runtime.block_on(CatalogService::open(CatalogConfig {
        warehouse_root: root.path().join("warehouse").to_string_lossy().into_owned(),
        state_dir: root.path().join("catalog"),
        ddl_dir: None,
        retained_events: 16,
        ingestion_layout: false,
    }))?;
    let listener = runtime.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = runtime.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(service.clone()))
            .add_service(SchemaCatalogServer::new(service))
            .serve_with_incoming_shutdown(tokio_stream::wrappers::TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            })
            .await
    });
    let channel = runtime.block_on(tonic::transport::Endpoint::from_shared(endpoint.clone())?.connect())?;
    let mut sql = FlightSqlServiceClient::new(channel);
    runtime.block_on(sql.execute_update(
        "CREATE TABLE arrowcol.default.events (id BIGINT NOT NULL, __arrowcol_ingestion STRING NOT NULL) USING DELTA PARTITIONED BY (__arrowcol_ingestion) TBLPROPERTIES ('arrowcol.optimize.minFiles'='3', 'arrowcol.partition.granularity'='minute', 'delta.checkpointInterval'='100')".into(),
        None,
    ))?;
    let source = CatalogDiscovery::connect_with_signer_and_options(
        &endpoint,
        Vec::new(),
        InternalRequestSigner::from_secret(b"catalog-integration-key-at-least-32-bytes".to_vec())?,
        delta_maintenance::catalog::CatalogOptions {
            runtime_threads: 1,
            rpc_timeout: Duration::from_secs(5),
            max_decoding_message_bytes: 1024 * 1024,
        },
    )?;
    let names = source.tables()?;
    assert_eq!(names, ["arrowcol.default.events"]);
    let discovered = source.table(&names[0])?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(INGESTION_PARTITION, DataType::Utf8, false),
    ]));
    let table = DeltaTable::open_url(
        discovered.url.clone(),
        Arc::clone(&schema),
        vec![INGESTION_PARTITION.into()],
    )?;
    write_pairs(&table, schema)?;
    let anchor = settings.anchor_ms.context("test maintenance anchor")?;
    table.checkpoint_if_due_at(1, true, anchor)?;
    table.vacuum_once(
        &VacuumConfig::new(settings.vacuum_retention_ms, anchor),
        true,
        &MaintenanceControl::default(),
    )?;
    let before = inventory(root.path())?;
    let metrics = Metrics::new();
    let cancel = Arc::new(AtomicBool::new(false));
    for _ in 0..2 {
        let results = run_cycle_with_discovery(&settings, &source, &metrics, &cancel)?;
        assert!(results.iter().all(|r| r.error.is_none()), "{results:?}");
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|result| result.error.is_none()), "{results:?}");
        assert!(
            results.iter().all(|result| {
                result.negative_probe != matches!(result.operation, Operation::Vacuum | Operation::Optimize)
            }),
            "{results:?}"
        );
        assert_eq!(before, inventory(root.path())?);
    }
    runtime.block_on(sql.execute_update(
        "ALTER TABLE arrowcol.default.events SET TBLPROPERTIES ('arrowcol.optimize.minFiles'='2')".into(),
        None,
    ))?;
    let stale = CatalogTable(source.table(&names[0])?, None);
    let after_ddl = inventory(root.path())?;
    let rejected = run_cycle_with_discovery(&settings, &stale, &metrics, &cancel)?;
    assert!(rejected.iter().all(|r| r.error.is_some() && !r.negative_probe));
    assert_eq!(after_ddl, inventory(root.path())?);
    let results = run_cycle_with_discovery(&settings, &source, &metrics, &cancel)?;
    let optimize = results
        .iter()
        .find(|r| r.operation == Operation::Optimize)
        .ok_or_else(|| anyhow!("optimize result"))?;
    assert!(optimize.error.is_none(), "{:?}", optimize.error);
    assert_eq!(optimize.report.as_ref().map(|r| r.files_removed), Some(2));
    assert_eq!(source.table(&names[0])?.properties["arrowcol.optimize.minFiles"], "2");
    let path = discovered
        .url
        .to_file_path()
        .map_err(|()| anyhow!("managed table path"))?;
    assert_eq!(
        std::fs::read_dir(path.join("__arrowcol_ingestion=2024-01-01-00-01"))?.count(),
        2
    );
    drop(source);
    drop(sql);
    shutdown
        .send(())
        .map_err(|()| anyhow!("catalog server stopped early"))?;
    runtime.block_on(server)??;
    Ok(())
}

#[test]
fn empty_vacuum_plan_cannot_report_noop_after_a_concurrent_append() -> Result<()> {
    let (root, _, source) = fixture()?;
    let table = DeltaTable::open_for_maintenance(&source.0.url)?;
    let config = arrow_deltalake::maintenance::VacuumConfig::new(604_800_000, now_ms()?);
    let plan = table.plan_vacuum(&config)?;
    assert!(plan.candidates.is_empty());
    let metadata = table.catalog_snapshot()?.ok_or_else(|| anyhow!("table metadata"))?;
    let writer = DeltaTable::open_url(source.0.url, Arc::clone(&metadata.schema), metadata.partition_columns)?;
    write_pairs(&writer, metadata.schema)?;
    let before = inventory(root.path())?;
    let error = match table.apply_vacuum(&plan, &config, &MaintenanceControl::default()) {
        Err(error) => error,
        Ok(_) => return Err(anyhow!("stale empty VACUUM plan was accepted")),
    };
    assert_eq!(
        arrow_deltalake::maintenance::error_kind(&error),
        Some(arrow_deltalake::maintenance::MaintenanceErrorKind::StalePlan)
    );
    assert_eq!(before, inventory(root.path())?);
    Ok(())
}

#[test]
fn optimize_upload_mode_is_strict_and_buffer_is_opt_in() -> Result<()> {
    assert_eq!(
        "put_create".parse::<OptimizeUploadMode>()?,
        OptimizeUploadMode::PutCreate
    );
    assert_eq!(
        "buffered_multipart".parse::<OptimizeUploadMode>()?,
        OptimizeUploadMode::BufferedMultipart
    );
    assert!("stream".parse::<OptimizeUploadMode>().is_err());
    let (_root, mut settings, _) = fixture()?;
    settings.operation_limits.optimize_upload_buffer_bytes = Some(8 * 1024 * 1024);
    assert!(settings.validate().is_err());
    settings.operation_limits.optimize_upload_mode = OptimizeUploadMode::BufferedMultipart;
    for value in [4 * 1024 * 1024, 8 * 1024 * 1024, 64 * 1024 * 1024] {
        settings.operation_limits.optimize_upload_buffer_bytes = Some(value);
        settings.validate()?;
    }
    settings.operation_limits.optimize_upload_buffer_bytes = None;
    settings.validate()?;
    for value in [0, 4 * 1024 * 1024 - 1, 64 * 1024 * 1024 + 1] {
        settings.operation_limits.optimize_upload_buffer_bytes = Some(value);
        assert!(settings.validate().is_err());
    }
    Ok(())
}

#[test]
fn startup_readback_reports_effective_optimize_upload_settings() -> Result<()> {
    let (_root, mut settings, _) = fixture()?;
    let readback = startup_configuration(&settings)?;
    assert_eq!(readback["optimize_upload"]["mode"], "put_create");
    assert!(readback["optimize_upload"]["buffer_bytes_requested"].is_null());
    assert!(readback["optimize_upload"]["buffer_bytes_effective"].is_null());

    settings.operation_limits.optimize_upload_mode = OptimizeUploadMode::BufferedMultipart;
    let readback = startup_configuration(&settings)?;
    assert_eq!(readback["optimize_upload"]["mode"], "buffered_multipart");
    assert!(readback["optimize_upload"]["buffer_bytes_requested"].is_null());
    assert_eq!(
        readback["optimize_upload"]["buffer_bytes_effective"].as_u64(),
        Some(8 * 1024 * 1024)
    );

    settings.operation_limits.optimize_upload_buffer_bytes = Some(16 * 1024 * 1024);
    let readback = startup_configuration(&settings)?;
    assert_eq!(
        readback["optimize_upload"]["buffer_bytes_requested"].as_u64(),
        Some(16 * 1024 * 1024)
    );
    assert_eq!(
        readback["optimize_upload"]["buffer_bytes_effective"].as_u64(),
        Some(16 * 1024 * 1024)
    );
    Ok(())
}
