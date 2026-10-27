//! Opt-in proof over frozen collector output, never a synthetic source replacement.
use anyhow::{Context, Result, ensure};
use arrow::array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow::compute::{concat_batches, filter_record_batch, take_record_batch};
use arrow::json::ArrayWriter;
use arrow_deltalake::layout::LayoutPolicy;
use arrow_tenant_stream::reader::{BatchConsumer, ReadMetrics, TenantQuery, read_file};
use arrow_tenant_stream::source::{BootstrapManifest, DeltaSource, SourceFile};
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use parquet::arrow::ArrowWriter;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    pool: String,
    root: PathBuf,
    frozen: BootstrapManifest,
    customer_id: String,
    row_group_rows: usize,
    page_rows: usize,
}

#[derive(Default)]
struct Rows(Vec<RecordBatch>);

#[async_trait::async_trait]
impl BatchConsumer for Rows {
    async fn consume(&mut self, batch: RecordBatch) -> Result<()> {
        self.0.push(batch);
        Ok(())
    }
}

fn counters(plan: &Arc<dyn ExecutionPlan>, metrics: &mut BTreeMap<String, usize>) {
    if let Some(values) = plan.metrics() {
        for metric in values.iter() {
            let name = metric.value().name().to_owned();
            match metric.value() {
                MetricValue::PruningMetrics { pruning_metrics, .. } => {
                    *metrics.entry(name.replace("_pruned", "_matched")).or_default() += pruning_metrics.matched();
                    *metrics.entry(name).or_default() += pruning_metrics.pruned();
                }
                MetricValue::Ratio { .. } | MetricValue::StartTimestamp(_) | MetricValue::EndTimestamp(_) => {}
                value => *metrics.entry(name).or_default() += value.as_usize(),
            }
        }
    }
    for child in plan.children() {
        counters(child, metrics);
    }
}

fn multiset(batches: &[RecordBatch]) -> Result<BTreeMap<String, usize>> {
    let mut writer = ArrayWriter::new(Vec::new());
    writer.write_batches(&batches.iter().collect::<Vec<_>>())?;
    writer.finish()?;
    let rows: Vec<serde_json::Value> = serde_json::from_slice(&writer.into_inner())?;
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(serde_json::to_string(&row)?).or_default() += 1;
    }
    Ok(counts)
}

async fn prove(fixture: Fixture) -> Result<()> {
    ensure!(
        fixture.row_group_rows == 512 && fixture.page_rows == 128,
        "layout must be predeclared 512/128"
    );
    let root = std::fs::canonicalize(&fixture.root)?;
    let url = url::Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("invalid local fixture"))?;
    let source = DeltaSource::open(url)?;
    let captured = source.capture()?;
    ensure!(
        captured == fixture.frozen,
        "fixture identity/version/active files changed before scan"
    );
    ensure!(
        !captured.files.is_empty(),
        "collector fixture must contain committed source files"
    );
    let mut published = BTreeMap::new();
    for version in 0..=captured.version {
        let text = std::fs::read_to_string(root.join(format!("_delta_log/{version:020}.json")))?;
        let actions = text
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(
            actions.iter().all(|action| action.get("remove").is_none()),
            "source proof requires pristine collector history, not maintenance output"
        );
        let transactions = actions
            .iter()
            .filter_map(|action| action.get("txn"))
            .filter(|txn| {
                txn["appId"]
                    .as_str()
                    .is_some_and(|app| app.starts_with("arrowcol.publication."))
                    && txn["version"].as_i64() == Some(0)
            })
            .count();
        let adds = actions
            .iter()
            .filter_map(|action| action.get("add"))
            .collect::<Vec<_>>();
        ensure!(
            adds.len() == transactions,
            "every collector Add must have a publication transaction"
        );
        for add in adds {
            ensure!(
                add["dataChange"].as_bool() == Some(true),
                "collector Add must change data"
            );
            let path = add["path"].as_str().context("collector Add path")?.to_owned();
            let size = add["size"].as_u64().context("collector Add size")?;
            ensure!(published.insert(path, size).is_none(), "collector data path was reused");
        }
    }
    ensure!(
        published
            == captured
                .files
                .iter()
                .map(|file| (file.path.clone(), file.size))
                .collect(),
        "frozen active files must equal the complete pristine publication history"
    );
    let query = TenantQuery {
        customer_id: fixture.customer_id.clone(),
        projection: Vec::new(),
    };
    let temporary = tempfile::tempdir()?;
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .set_bool("datafusion.execution.parquet.schema_force_view_types", false),
    );
    let mut full = ReadMetrics::default();
    let mut filtered = ReadMetrics::default();
    let mut control = ReadMetrics::default();
    let mut selected = Rows::default();
    let mut expected = Vec::new();
    let mut control_rows = Rows::default();
    let mut total_groups = 0_usize;
    let mut nonempty_files = 0_usize;
    let mut returned_files = 0_usize;
    for (index, file) in captured.files.iter().enumerate() {
        let path = source
            .file_url(&file.path)?
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("non-local source"))?;
        let reader = SerializedFileReader::new(std::fs::File::open(&path)?)?;
        total_groups += reader.num_row_groups();
        for group in reader.metadata().row_groups() {
            ensure!(
                usize::try_from(group.num_rows())? <= 512,
                "source row group exceeds declared 512"
            );
            ensure!(
                group
                    .columns()
                    .iter()
                    .all(|column| column.column_index_offset().is_some() && column.offset_index_offset().is_some()),
                "source lacks written page indexes"
            );
        }
        let dataframe = context
            .read_parquet(source.file_url(&file.path)?.as_str(), ParquetReadOptions::default())
            .await?;
        let plan = dataframe.create_physical_plan().await?;
        let batches = collect(Arc::clone(&plan), context.task_ctx()).await?;
        let physical = concat_batches(&dataframe_schema(&batches)?, &batches)?;
        full.files_considered += 1;
        full.rows_returned += u64::try_from(physical.num_rows())?;
        counters(&plan, &mut full.datafusion);
        let key = physical
            .column_by_name("customer_id")
            .context("source customer column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("source customer type")?;
        let mask = arrow::array::BooleanArray::from_iter(
            (0..key.len()).map(|row| Some(!key.is_null(row) && key.value(row) == fixture.customer_id)),
        );
        let expected_physical = filter_record_batch(&physical, &mask)?;
        nonempty_files += usize::from(expected_physical.num_rows() > 0);
        // Round-robin customer runs to undo the source's physical clustering.
        // The control keeps every full row, encoding settings and file boundary.
        ensure!(key.null_count() == 0, "customer keys must be nonnull");
        let mut buckets: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        for row in 0..physical.num_rows() {
            buckets.entry(key.value(row)).or_default().push(row);
        }
        let longest = buckets.values().map(Vec::len).max().unwrap_or(0);
        let order = (0..longest)
            .flat_map(|offset| buckets.values().filter_map(move |rows| rows.get(offset).copied()))
            .map(u32::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let unsorted = take_record_batch(&physical, &UInt32Array::from(order))?;
        ensure!(
            multiset(std::slice::from_ref(&physical))? == multiset(std::slice::from_ref(&unsorted))?,
            "control must preserve complete source row multiset"
        );
        let name = format!("control-{index}.parquet");
        let control_path = temporary.path().join(&name);
        let policy = LayoutPolicy {
            row_group_rows: 512,
            page_rows: 128,
            ..LayoutPolicy::default()
        };
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&control_path)?,
            physical.schema(),
            Some(policy.writer_properties(&physical.schema())?),
        )?;
        writer.write(&unsorted)?;
        writer.close()?;
        let control_source = DeltaSource::open(
            url::Url::from_directory_path(temporary.path()).map_err(|()| anyhow::anyhow!("invalid control URL"))?,
        )?;
        let control_file = SourceFile {
            path: name,
            size: std::fs::metadata(control_path)?.len(),
            partition_values: file.partition_values.clone(),
        };
        let mut output = Rows::default();
        let metrics = read_file(&source, &captured, file, &query, &mut output).await?;
        returned_files += usize::from(metrics.rows_returned > 0);
        filtered.merge(metrics);
        selected.0.extend(output.0);
        control.merge(read_file(&control_source, &captured, &control_file, &query, &mut control_rows).await?);
        // Independent full-scan filtering must also reconstruct Add partition values.
        let mut logical = expected_physical;
        for name in captured.partition_columns()? {
            let value = file.partition_values.get(&name).context("partition Add value")?;
            let values = StringArray::from(vec![value.as_deref(); logical.num_rows()]);
            let mut fields = logical.schema().fields().to_vec();
            let schema = captured.schema()?;
            fields.push(Arc::clone(&schema.fields()[schema.index_of(&name)?]));
            let mut columns = logical.columns().to_vec();
            columns.push(Arc::new(values));
            logical = RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)?;
        }
        let schema = captured.schema()?;
        expected.push(RecordBatch::try_new(
            Arc::clone(&schema),
            schema
                .fields()
                .iter()
                .map(|field| {
                    logical
                        .column_by_name(field.name())
                        .cloned()
                        .context("logical source field")
                })
                .collect::<Result<Vec<_>>>()?,
        )?);
    }
    ensure!(
        multiset(&selected.0)? == multiset(&expected)?,
        "filtered source must equal full-scan predicate multiset"
    );
    ensure!(
        multiset(&selected.0)? == multiset(&control_rows.0)?,
        "control must return identical full rows"
    );
    ensure!(
        filtered.rows_returned > 0 && filtered.rows_returned < full.rows_returned,
        "nontrivial customer predicate"
    );
    ensure!(
        returned_files == nonempty_files,
        "must consider every committed source file"
    );
    for metrics in [&full, &filtered, &control] {
        ensure!(
            metrics.datafusion.get("files_opened").copied() == Some(captured.files.len()),
            "actual execution must account for every source or corresponding control file"
        );
        ensure!(
            metrics.datafusion.get("file_scan_errors").copied().unwrap_or(0) == 0,
            "source proof cannot tolerate file scan errors"
        );
    }
    let bytes = |metrics: &ReadMetrics| metrics.datafusion.get("bytes_scanned").copied().unwrap_or(0);
    ensure!(
        bytes(&full) > 0 && bytes(&filtered) > 0,
        "actual scan-byte counters required"
    );
    ensure!(
        bytes(&filtered) < bytes(&full),
        "source predicate did not reduce full-scan bytes"
    );
    ensure!(
        bytes(&filtered) < bytes(&control),
        "source layout did not reduce unsorted-control bytes"
    );
    ensure!(
        filtered
            .datafusion
            .get("row_groups_pruned_statistics")
            .copied()
            .unwrap_or(0)
            > 0,
        "actual source execution must prune row groups"
    );
    let matched = |metrics: &ReadMetrics| {
        metrics
            .datafusion
            .get("row_groups_matched_statistics")
            .copied()
            .unwrap_or(0)
    };
    ensure!(
        matched(&full) == total_groups,
        "full execution must visit every footer row group"
    );
    ensure!(
        matched(&filtered) < matched(&full) && matched(&filtered) < matched(&control),
        "actual source must visit fewer row groups than both full and unsorted scans"
    );
    ensure!(
        source.capture()? == captured,
        "source identity/version/files changed during proof"
    );
    println!(
        "COLLECTOR_SOURCE_READ_EFFICIENCY {}",
        serde_json::to_string(&json!({
            "pool": fixture.pool, "verifiedMaximumRowGroupRows":512, "requestedPageRows":128,
            "committedFiles":captured.files.len(), "filesWithMatchingRows":returned_files,
            "footerRowGroups":total_groups,
            "sortedFiltered":filtered, "sortedUnfiltered":full, "identicalUnsortedFiltered":control,
            "completeRowMultisetEqual":true, "frozenSnapshotUnchanged":true,
            "pristinePublicationHistory":true,
        }))?
    );
    Ok(())
}

fn dataframe_schema(batches: &[RecordBatch]) -> Result<arrow::datatypes::SchemaRef> {
    Ok(batches
        .first()
        .context("source full scan returned no batches")?
        .schema())
}

#[tokio::test]
#[ignore = "requires two frozen real collector CPU source snapshots, supplied by the qualification runner"]
async fn actual_collector_cpu_sources_skip_bytes_and_row_groups_in_both_pools() -> Result<()> {
    let manifest = std::env::var_os("ARROWCOL_SOURCE_READ_FIXTURE").context("fixture manifest required")?;
    let fixtures: Vec<Fixture> = serde_json::from_slice(&std::fs::read(manifest)?)?;
    let mut pools = fixtures.iter().map(|fixture| fixture.pool.as_str()).collect::<Vec<_>>();
    pools.sort_unstable();
    ensure!(
        pools == ["dfe", "direct"],
        "one frozen CPU source from each pool is required"
    );
    for fixture in fixtures {
        prove(fixture).await?;
    }
    Ok(())
}

/// Run only after verifying the private transfer receipt and every file digest.
/// Input contains the two roots; output stays private and is the immutable input
/// to the actual query proof, not an automatically refreshed query-time snapshot.
#[test]
#[ignore = "captures relocated fixture manifests after independent transfer-receipt verification"]
fn capture_relocated_collector_source_manifests() -> Result<()> {
    let input = std::env::var_os("ARROWCOL_SOURCE_READ_ROOTS").context("private roots JSON required")?;
    let output = std::env::var_os("ARROWCOL_SOURCE_READ_FIXTURE").context("private output manifest required")?;
    let roots: BTreeMap<String, PathBuf> = serde_json::from_slice(&std::fs::read(input)?)?;
    ensure!(
        roots.keys().map(String::as_str).collect::<Vec<_>>() == ["dfe", "direct"],
        "both pools required"
    );
    let mut fixtures = Vec::new();
    for (pool, root) in roots {
        let root = std::fs::canonicalize(root)?;
        let url = url::Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("invalid local root"))?;
        fixtures.push(Fixture {
            pool,
            frozen: DeltaSource::open(url)?.capture()?,
            root,
            customer_id: "customer-0003".into(),
            row_group_rows: 512,
            page_rows: 128,
        });
    }

    let file = std::fs::OpenOptions::new().write(true).create_new(true).open(output)?;
    serde_json::to_writer_pretty(file, &fixtures)?;
    Ok(())
}

async fn full_logical_scan(source: &DeltaSource, frozen: &BootstrapManifest) -> Result<Vec<RecordBatch>> {
    use datafusion::common::ScalarValue;
    use datafusion::prelude::lit;
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .set_bool("datafusion.execution.parquet.schema_force_view_types", false),
    );
    let schema = frozen.schema()?;
    let mut output = Vec::new();
    for file in &frozen.files {
        let mut frame = context
            .read_parquet(source.file_url(&file.path)?.as_str(), ParquetReadOptions::default())
            .await?;
        for name in frozen.partition_columns()? {
            let value = file.partition_values.get(&name).context("partition value")?;
            let data_type = schema.field_with_name(&name)?.data_type();
            let scalar = match value {
                Some(value) => ScalarValue::try_from_string(value.clone(), data_type)?,
                None => ScalarValue::try_from(data_type)?,
            };
            frame = frame.with_column(&name, lit(scalar))?;
        }
        let names = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        for batch in frame.select_columns(&names)?.collect().await? {
            output.push(RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())?);
        }
    }
    Ok(output)
}

struct Process {
    child: std::process::Child,
    reaped: bool,
}

impl Process {
    fn terminate(&mut self) -> Result<()> {
        use std::time::{Duration, Instant};
        if self.reaped {
            return Ok(());
        }
        if matches!(self.child.try_wait(), Ok(Some(_))) {
            self.reaped = true;
            return Ok(());
        }
        // An unknown wait state is not evidence that the owned child stopped.
        let kill = self.child.kill();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if self.child.try_wait()?.is_some() {
                self.reaped = true;
                kill.context("owned child kill failed")?;
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "owned child cleanup exceeded five seconds; kill={kill:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn finish(&mut self, timeout: std::time::Duration) -> Result<std::process::ExitStatus> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => {
                    self.reaped = true;
                    return Ok(status);
                }
                Ok(None) if std::time::Instant::now() < deadline => {}
                outcome => {
                    let cleanup = self.terminate();
                    cleanup.context("owned child termination/reaping failed")?;
                    anyhow::bail!("owned child did not finish within {timeout:?}: {outcome:?}");
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if !self.reaped
            && let Err(error) = self.terminate()
        {
            eprintln!("owned child fallback cleanup failed: {error:#}");
        }
    }
}

fn run_service(source: &std::path::Path, target: &std::path::Path, checkpoint: &std::path::Path) -> Result<String> {
    use std::process::{Command, Stdio};
    let stdout = tempfile::NamedTempFile::new()?;
    let stderr = tempfile::NamedTempFile::new()?;
    let mut child = Process {
        child: Command::new(env!("CARGO_BIN_EXE_arrow-tenant-stream"))
            .arg("--source")
            .arg(source)
            .arg("--target")
            .arg(target)
            .arg("--checkpoint")
            .arg(checkpoint)
            .args([
                "--customer-id",
                "customer-0003",
                "--once",
                "--max-actions",
                "128",
                "--row-group-rows",
                "512",
                "--metrics-listen",
                "127.0.0.1:0",
            ])
            .stdout(Stdio::from(stdout.as_file().try_clone()?))
            .stderr(Stdio::from(stderr.as_file().try_clone()?))
            .spawn()?,
        reaped: false,
    };
    let status = child.finish(std::time::Duration::from_secs(120))?;
    ensure!(child.reaped, "child custody must finish before PASS");
    ensure!(
        status.success(),
        "actual service failed: {}",
        std::fs::read_to_string(stderr.path())?
    );
    Ok(std::fs::read_to_string(stdout.path())?)
}

#[test]
#[cfg(target_os = "linux")]
fn hung_owned_child_is_killed_and_reaped_at_the_deadline() -> Result<()> {
    let child = std::process::Command::new("sleep").arg("60").spawn()?;
    let pid = child.id();
    let mut process = Process { child, reaped: false };
    let start = std::time::Instant::now();
    let error = process
        .finish(std::time::Duration::from_millis(50))
        .err()
        .context("hung child must time out")?;
    ensure!(
        error.to_string().contains("did not finish"),
        "timeout must remain an error"
    );
    ensure!(process.reaped, "timed-out child must be positively reaped");
    ensure!(process.child.try_wait()?.is_some(), "child status must be known");
    ensure!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "owned PID must be gone, not zombie"
    );
    ensure!(
        start.elapsed() < std::time::Duration::from_secs(6),
        "cleanup must stay bounded"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires the same immutable real collector CPU fixtures as the source-scan proof"]
async fn actual_service_forks_frozen_collector_sources_and_restarts_idle_in_both_pools() -> Result<()> {
    use arrow_tenant_stream::checkpoint::Checkpoint;
    let manifest = std::env::var_os("ARROWCOL_SOURCE_READ_FIXTURE").context("fixture manifest required")?;
    let fixtures: Vec<Fixture> = serde_json::from_slice(&std::fs::read(manifest)?)?;
    let mut pools = fixtures.iter().map(|fixture| fixture.pool.as_str()).collect::<Vec<_>>();
    pools.sort_unstable();
    ensure!(pools == ["dfe", "direct"], "both pools required");
    for fixture in fixtures {
        let root = std::fs::canonicalize(fixture.root)?;
        let url = url::Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("source URL"))?;
        let source = DeltaSource::open(url)?;
        ensure!(
            source.capture()? == fixture.frozen,
            "frozen source changed before service"
        );
        let all_source = full_logical_scan(&source, &fixture.frozen).await?;
        let mut expected = Vec::new();
        for batch in all_source {
            let keys = batch
                .column_by_name("customer_id")
                .context("source customer")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("source key type")?;
            let mask = arrow::array::BooleanArray::from_iter(
                (0..keys.len()).map(|row| Some(!keys.is_null(row) && keys.value(row) == "customer-0003")),
            );
            expected.push(filter_record_batch(&batch, &mask)?);
        }
        ensure!(
            expected.iter().map(RecordBatch::num_rows).sum::<usize>() == 8_128,
            "expected complete source customer rows"
        );
        let temporary = tempfile::tempdir()?;
        let target = temporary.path().join("target");
        let checkpoint = temporary.path().join("checkpoint");
        let first = run_service(&root, &target, &checkpoint)?;
        ensure!(
            first.contains("batch=0") && !first.contains("sink_replay=true"),
            "actual snapshot batch must commit"
        );
        let target_url = url::Url::from_directory_path(&target).map_err(|()| anyhow::anyhow!("target URL"))?;
        let sink = DeltaSource::open(target_url.clone())?;
        let snapshot = sink.capture()?;
        ensure!(
            snapshot.schema()? == fixture.frozen.schema()?,
            "logical schema must be preserved"
        );
        ensure!(
            snapshot.partition_columns()? == fixture.frozen.partition_columns()?,
            "partition schema must be preserved"
        );
        let actual = full_logical_scan(&sink, &snapshot).await?;
        ensure!(
            multiset(&actual)? == multiset(&expected)?,
            "entire target must equal independent full-source predicate"
        );
        ensure!(
            actual.iter().map(RecordBatch::num_rows).sum::<usize>() == 8_128,
            "no extra customer rows"
        );
        for batch in &actual {
            let keys = batch
                .column_by_name("customer_id")
                .context("target customer")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("target customer type")?;
            ensure!(
                keys.null_count() == 0 && keys.iter().all(|key| key == Some("customer-0003")),
                "cross-customer target data"
            );
        }
        let metadata = std::fs::read(checkpoint.join("metadata"))?;
        let identity: serde_json::Value = serde_json::from_slice(&metadata)?;
        let query_id = identity["id"].as_str().context("query UUID")?;
        uuid::Uuid::parse_str(query_id)?;
        ensure!(
            metadata == serde_json::to_vec(&json!({"id":query_id}))?,
            "Spark query metadata exact bytes"
        );
        let mut handle = Checkpoint::open_local(&checkpoint).await?;
        let recovery = handle.recover().await?;
        ensure!(
            recovery.batch_id > 0 && recovery.pending_offset.is_none() && recovery.previous_offset.is_some(),
            "checkpoint must recover only committed progress"
        );
        let committed = recovery.batch_id - 1;
        handle.close().await?;
        for batch in 0..=committed {
            ensure!(
                std::fs::read(checkpoint.join(format!("commits/{batch}")))? == b"v1\n{\"nextBatchWatermarkMs\":0}",
                "exact Spark stateless commit bytes"
            );
            let offset = std::fs::read_to_string(checkpoint.join(format!("offsets/{batch}")))?;
            let lines = offset.split('\n').collect::<Vec<_>>();
            ensure!(lines.len() == 3 && lines[0] == "v1", "Spark offset envelope");
            let metadata: serde_json::Value = serde_json::from_str(lines[1])?;
            ensure!(
                metadata["batchWatermarkMs"] == 0
                    && metadata["conf"] == json!({})
                    && metadata["batchTimestampMs"].is_i64(),
                "Spark stateless offset metadata"
            );
            let _: serde_json::Value = serde_json::from_str(lines[2])?;
        }
        let table = arrow_deltalake::delta::DeltaTable::open_for_maintenance(&target_url)?;
        ensure!(
            table.lookup_txn(query_id)? == Some(committed),
            "sink stable query/batch transaction"
        );
        let restarted = run_service(&root, &target, &checkpoint)?;
        ensure!(!restarted.contains("batch="), "unchanged source restart must be idle");
        ensure!(
            sink.capture()? == snapshot,
            "idle restart must not add target files or versions"
        );
        ensure!(
            std::fs::read(checkpoint.join("metadata"))? == metadata,
            "query identity must survive restart"
        );
        let mut handle = Checkpoint::open_local(&checkpoint).await?;
        ensure!(
            handle.recover().await? == recovery,
            "idle restart must not advance checkpoint"
        );
        handle.close().await?;
        ensure!(
            multiset(&full_logical_scan(&sink, &snapshot).await?)? == multiset(&expected)?,
            "idle restart must not duplicate target rows"
        );
        ensure!(
            source.capture()? == fixture.frozen,
            "frozen source changed during service proof"
        );
        println!(
            "COLLECTOR_SOURCE_SERVICE {}",
            serde_json::to_string(&json!({
                "pool":fixture.pool,"sourceFiles":fixture.frozen.files.len(),"targetRows":8128,
                "targetVersion":snapshot.version,"targetFiles":snapshot.files.len(),
                "fullRowMultisetEqual":true,"partitionSchemaPreserved":true,
                "sparkCheckpointRecovery":true,"restartIdle":true,"ownedProcessesReaped":true,
                "childDeadlineSeconds":120,"liveAppendTailProven":false,
            }))?
        );
    }
    Ok(())
}

fn copy_disposable_tree(source: &std::path::Path, target: &std::path::Path) -> Result<()> {
    std::fs::create_dir(target)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        ensure!(!kind.is_symlink(), "disposable fixture copy cannot follow symlinks");
        if kind.is_dir() {
            copy_disposable_tree(&entry.path(), &target.join(entry.file_name()))?;
        } else {
            ensure!(kind.is_file(), "fixture contains a non-file");
            std::fs::copy(entry.path(), target.join(entry.file_name()))?;
        }
    }
    Ok(())
}

fn local_source(root: &std::path::Path) -> Result<DeltaSource> {
    DeltaSource::open(url::Url::from_directory_path(root).map_err(|()| anyhow::anyhow!("local source URL"))?)
}

fn fixtures() -> Result<Vec<Fixture>> {
    let path = std::env::var_os("ARROWCOL_SOURCE_READ_FIXTURE").context("private fixture manifest required")?;
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn replace_partition(batch: &RecordBatch, partition: &str) -> Result<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    columns[batch.schema().index_of("__arrowcol_ingestion")?] =
        Arc::new(StringArray::from(vec![partition; batch.num_rows()]));
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

async fn template_batch(fixture: &Fixture) -> Result<RecordBatch> {
    let source = local_source(&fixture.root)?;
    let mut one = fixture.frozen.clone();
    one.files.truncate(1);
    let batches = full_logical_scan(&source, &one).await?;
    let batch = concat_batches(&one.schema()?, &batches)?;
    ensure!(
        batch.num_rows() == 1_024,
        "append must retain the actual 1024-row collector batch"
    );
    Ok(batch)
}

#[tokio::test]
#[ignore = "owned child entrypoint for the disposable-source lifecycle test"]
async fn lifecycle_source_append_child() -> Result<()> {
    use arrow_deltalake::normalize::CanonicalSchema;
    let input: serde_json::Value = serde_json::from_str(&std::env::var("ARROWCOL_LIFECYCLE_APPEND")?)?;
    let fixture = fixtures()?
        .into_iter()
        .find(|fixture| input["pool"] == fixture.pool)
        .context("append pool")?;
    let root = PathBuf::from(input["root"].as_str().context("disposable root")?);
    ensure!(
        root.join(arrow_deltalake::maintenance::DISPOSABLE_TEST_MARKER)
            .is_file(),
        "append may mutate only the marked disposable copy"
    );
    let partition = input["partition"].as_str().context("append partition")?;
    let app = input["app"].as_str().context("append publication")?;
    let data = replace_partition(&template_batch(&fixture).await?, partition)?;
    let canonical = CanonicalSchema::try_new(data.schema())?;
    let table = arrow_deltalake::delta::DeltaTable::open_url_canonical(
        local_source(&root)?.url,
        &canonical,
        fixture.frozen.partition_columns()?,
    )?;
    let properties = LayoutPolicy {
        row_group_rows: 512,
        page_rows: 128,
        ..LayoutPolicy::default()
    }
    .writer_properties(&data.schema())?;
    let mut writer = table
        .new_canonical_file_partitioned_with_properties(
            &canonical,
            BTreeMap::from([("__arrowcol_ingestion".into(), Some(partition.into()))]),
            properties,
        )?
        .with_transaction(app.into(), 0)?
        .with_ingestion_time(input["stamp"].as_i64().context("admission stamp")?)?;
    writer.write(&data).await?;
    writer.finish().await?;
    ensure!(
        table.lookup_txn(app)? == Some(0),
        "source append publication transaction"
    );
    Ok(())
}

fn append_owned(root: &std::path::Path, pool: &str, partition: &str, stamp: i64, app: &str) -> Result<()> {
    use std::process::{Command, Stdio};
    let log = tempfile::NamedTempFile::new()?;
    let mut process = Process {
        child: Command::new(std::env::current_exe()?)
            .args(["--exact", "lifecycle_source_append_child", "--ignored", "--nocapture"])
            .env(
                "ARROWCOL_LIFECYCLE_APPEND",
                serde_json::to_string(&json!({
                    "root":root,"pool":pool,"partition":partition,"stamp":stamp,"app":app,
                }))?,
            )
            .stdout(Stdio::from(log.as_file().try_clone()?))
            .stderr(Stdio::from(log.as_file().try_clone()?))
            .spawn()?,
        reaped: false,
    };
    let status = process.finish(std::time::Duration::from_secs(120))?;
    ensure!(
        status.success(),
        "owned append child failed: {}",
        std::fs::read_to_string(log.path())?
    );
    Ok(())
}

fn metric(endpoint: &str, name: &str) -> Result<u64> {
    use std::io::{Read, Write};
    let mut socket = std::net::TcpStream::connect_timeout(&endpoint.parse()?, std::time::Duration::from_secs(2))?;
    socket.set_read_timeout(Some(std::time::Duration::from_secs(2)))?;
    socket.set_write_timeout(Some(std::time::Duration::from_secs(2)))?;
    socket.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    let mut response = String::new();
    socket.read_to_string(&mut response)?;
    ensure!(response.starts_with("HTTP/1.1 200 OK"), "metrics status");
    Ok(response
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .context("required service metric")?
        .parse()?)
}

fn wait_processed(process: &mut Process, endpoint: &str, version: u64) -> Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        ensure!(
            process.child.try_wait()?.is_none(),
            "live service stopped before phase completed"
        );
        if metric(endpoint, "tenant_stream_bootstrap_files_remaining")? == 0
            && metric(endpoint, "tenant_stream_processed_version")? >= version
            && metric(endpoint, "tenant_stream_noop_polls_total")? > 0
        {
            return Ok(());
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "service phase exceeded predeclared 120 seconds"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

fn start_live(
    source: &std::path::Path,
    target: &std::path::Path,
    checkpoint: &std::path::Path,
) -> Result<(Process, tempfile::NamedTempFile, String)> {
    use std::process::{Command, Stdio};
    let log = tempfile::NamedTempFile::new()?;
    let mut process = Process {
        child: Command::new(env!("CARGO_BIN_EXE_arrow-tenant-stream"))
            .arg("--source")
            .arg(source)
            .arg("--target")
            .arg(target)
            .arg("--checkpoint")
            .arg(checkpoint)
            .args([
                "--customer-id",
                "customer-0003",
                "--max-actions",
                "128",
                "--poll-ms",
                "20",
                "--row-group-rows",
                "512",
                "--metrics-listen",
                "127.0.0.1:0",
            ])
            .stdout(Stdio::from(log.as_file().try_clone()?))
            .stderr(Stdio::from(log.as_file().try_clone()?))
            .spawn()?,
        reaped: false,
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        let text = std::fs::read_to_string(log.path())?;
        if let Some(endpoint) = text
            .lines()
            .find_map(|line| line.strip_prefix("tenant stream metrics listening on "))
        {
            return Ok((process, log, endpoint.to_owned()));
        }
        ensure!(process.child.try_wait()?.is_none(), "service startup failed: {text}");
        ensure!(std::time::Instant::now() < deadline, "service startup deadline");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

async fn expected_customer(fixture: &Fixture) -> Result<Vec<RecordBatch>> {
    let all = full_logical_scan(&local_source(&fixture.root)?, &fixture.frozen).await?;
    let mut result = Vec::new();
    for batch in all {
        let keys = batch
            .column_by_name("customer_id")
            .context("customer")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("customer type")?;
        let mask = arrow::array::BooleanArray::from_iter(keys.iter().map(|value| Some(value == Some("customer-0003"))));
        result.push(filter_record_batch(&batch, &mask)?);
    }
    Ok(result)
}

async fn assert_historical_target(
    root: &std::path::Path,
    expected: &[RecordBatch],
    rows: usize,
    original: &BootstrapManifest,
) -> Result<BootstrapManifest> {
    let source = local_source(root)?;
    let frozen = source.capture()?;
    ensure!(
        frozen.schema()? == original.schema()?,
        "complete canonical schema retained"
    );
    ensure!(
        frozen.partition_columns()? == original.partition_columns()?,
        "partition schema retained"
    );
    let actual = full_logical_scan(&source, &frozen).await?;
    ensure!(
        actual.iter().map(RecordBatch::num_rows).sum::<usize>() == rows,
        "historical target row count"
    );
    ensure!(
        multiset(&actual)? == multiset(expected)?,
        "historical all-column multiset must be exact"
    );
    Ok(frozen)
}

#[tokio::test]
#[cfg(target_os = "linux")]
#[ignore = "bounded actual CLI lifecycle using disposable copies of both frozen collector sources"]
async fn actual_service_live_append_optimize_purge_prune_and_restart_on_disposable_copies() -> Result<()> {
    use arrow_deltalake::delta::DeltaTable;
    use arrow_deltalake::maintenance::{
        DISPOSABLE_TEST_MARKER, Granularity, MaintenanceControl, OptimizeConfig, Outcome, PruneConfig, PurgeConfig,
        VacuumConfig, now_ms,
    };
    use arrow_tenant_stream::checkpoint::Checkpoint;
    let fixtures = fixtures()?;
    ensure!(
        fixtures
            .iter()
            .map(|fixture| fixture.pool.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            == std::collections::BTreeSet::from(["dfe", "direct"])
            && fixtures.len() == 2,
        "actual lifecycle requires exactly both collector pools"
    );
    for fixture in fixtures {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("source");
        copy_disposable_tree(&fixture.root, &root)?;
        std::fs::write(root.join(DISPOSABLE_TEST_MARKER), b"")?;
        let target = temporary.path().join("target");
        let checkpoint = temporary.path().join("checkpoint");
        let mut expected = expected_customer(&fixture).await?;
        let (mut service, _log, endpoint) = start_live(&root, &target, &checkpoint)?;
        wait_processed(&mut service, &endpoint, fixture.frozen.version)?;
        assert_historical_target(&target, &expected, 8_128, &fixture.frozen).await?;
        let template = template_batch(&fixture).await?;
        let stamp = now_ms()?;
        let partition = arrow_deltalake::delta::ingestion_partition_value(stamp)?;
        let appended = replace_partition(&template, &partition)?;
        let keys = appended
            .column_by_name("customer_id")
            .context("append customer")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("append customer type")?;
        let selected = filter_record_batch(
            &appended,
            &arrow::array::BooleanArray::from_iter(keys.iter().map(|key| Some(key == Some("customer-0003")))),
        )?;
        ensure!(
            selected.num_rows() == 64,
            "one actual batch contributes exactly64 customer rows"
        );
        append_owned(
            &root,
            &fixture.pool,
            &partition,
            stamp,
            "arrowcol.publication.lifecycle-first",
        )?;
        expected.push(selected.clone());
        let source = local_source(&root)?;
        wait_processed(&mut service, &endpoint, source.capture()?.version)?;
        ensure!(
            metric(&endpoint, "tenant_stream_tail_batches_total")? > 0,
            "must observe live append tail"
        );
        assert_historical_target(&target, &expected, 8_192, &fixture.frozen).await?;
        let m = DeltaTable::open_for_maintenance(&source.url)?;
        let control = MaintenanceControl::new(
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Some(std::time::Instant::now() + std::time::Duration::from_secs(120)),
        );
        let optimize = OptimizeConfig {
            granularity: Some(Granularity::Minute),
            now_ms: Some(stamp),
            layout: Some(LayoutPolicy {
                row_group_rows: 512,
                page_rows: 128,
                ..LayoutPolicy::default()
            }),
            ..OptimizeConfig::default()
        };
        let before_optimize = source.capture()?.files.len();
        let report = m.apply_optimize(&m.plan_optimize(&optimize)?, &optimize, &control)?;
        ensure!(
            report.outcome == Outcome::Committed,
            "actual optimize must rewrite files"
        );
        ensure!(
            source.capture()?.files.len() < before_optimize,
            "OPTIMIZE must reduce actual source file count"
        );
        let optimized = std::fs::read_to_string(root.join(format!("_delta_log/{:020}.json", report.version)))?;
        for line in optimized.lines() {
            let action: serde_json::Value = serde_json::from_str(line)?;
            for name in ["add", "remove"] {
                if let Some(value) = action.get(name) {
                    ensure!(
                        value["dataChange"] == false,
                        "OPTIMIZE rewrites must be dataChange=false"
                    );
                }
            }
        }
        wait_processed(&mut service, &endpoint, report.version)?;
        assert_historical_target(&target, &expected, 8_192, &fixture.frozen).await?;
        let purge = PurgeConfig::new(1, stamp, Granularity::Minute);
        let purge_plan = m.plan_purge(&purge)?;
        ensure!(
            purge_plan.partitions.iter().all(|value| value.value != partition),
            "PURGE may select only expired sealed partitions, not the current admission minute"
        );
        let report = m.apply_purge(&purge_plan, &purge, &control)?;
        ensure!(
            report.outcome == Outcome::Committed,
            "actual expired-partition purge required"
        );
        let consumed_version = report.version;
        wait_processed(&mut service, &endpoint, report.version)?;
        let retained = source.capture()?;
        ensure!(
            retained.files.len() == 1
                && retained.files[0].partition_values["__arrowcol_ingestion"] == Some(partition.clone()),
            "PURGE must retain the protected new admission-minute source file"
        );
        let noop = m.apply_purge(&m.plan_purge(&purge)?, &purge, &control)?;
        ensure!(
            noop.outcome == Outcome::Noop && source.capture()?.version == consumed_version,
            "repeat PURGE noop must not commit a new source version"
        );
        assert_historical_target(&target, &expected, 8_192, &fixture.frozen).await?;
        ensure!(
            std::process::Command::new("kill")
                .args(["-TERM", &service.child.id().to_string()])
                .status()?
                .success(),
            "owned service SIGTERM"
        );
        ensure!(
            service.finish(std::time::Duration::from_secs(120))?.success(),
            "graceful service shutdown"
        );
        let mut barrier = Checkpoint::open_local(&checkpoint).await?;
        let recovered = barrier.recover().await?;
        let offset = recovered
            .previous_offset
            .context("committed pre-vacuum source cursor")?;
        ensure!(
            recovered.pending_offset.is_none()
                && offset["cursor"]["phase"] == "tail"
                && offset["cursor"]["version"]
                    .as_u64()
                    .is_some_and(|version| version > consumed_version),
            "durable next-unread cursor must be beyond every append/OPTIMIZE/PURGE commit before physical cleanup"
        );
        barrier.close().await?;
        let mut vacuum = VacuumConfig::new(0, now_ms()? + 1_000);
        vacuum.unsafe_allow_short_retention_for_tests = true;
        m.apply_vacuum(&m.plan_vacuum(&vacuum)?, &vacuum, &control)?;
        let checkpointed = m.checkpoint_if_due(1, true)?;
        ensure!(
            checkpointed.outcome == Outcome::Checkpointed,
            "real Kernel checkpoint required"
        );
        let floor = checkpointed.version;
        let prune = PruneConfig {
            log_retention_ms: Some(0),
            unsafe_allow_short_retention_for_tests: true,
            ..PruneConfig::new(now_ms()? + 1_000)
        };
        let report = m.apply_prune_log(&m.plan_prune_log(&prune)?, &prune, &control)?;
        ensure!(
            report.outcome == Outcome::Pruned && report.files_deleted > 0,
            "real retained-C log pruning required"
        );
        for version in 0..floor {
            ensure!(
                !root.join(format!("_delta_log/{version:020}.json")).exists(),
                "old JSON must be pruned"
            );
        }
        ensure!(
            root.join(format!("_delta_log/{floor:020}.json")).is_file(),
            "floor JSON retained"
        );
        let last_checkpoint: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("_delta_log/_last_checkpoint"))?)?;
        ensure!(
            last_checkpoint["version"] == floor,
            "retained Kernel checkpoint pointer"
        );
        ensure!(
            root.join(format!("_delta_log/{floor:020}.checkpoint.parquet"))
                .is_file(),
            "retained Kernel checkpoint artifact"
        );
        let before = assert_historical_target(&target, &expected, 8_192, &fixture.frozen).await?;
        ensure!(
            !run_service(&root, &target, &checkpoint)?.contains("batch="),
            "post-prune same-checkpoint restart idle"
        );
        ensure!(
            local_source(&target)?.capture()? == before,
            "no target commit after prune restart"
        );
        let next_stamp = now_ms()?;
        let next_partition = arrow_deltalake::delta::ingestion_partition_value(next_stamp)?;
        append_owned(
            &root,
            &fixture.pool,
            &next_partition,
            next_stamp,
            "arrowcol.publication.lifecycle-after-floor",
        )?;
        expected.push(replace_partition(&selected, &next_partition)?);
        ensure!(
            run_service(&root, &target, &checkpoint)?.contains("batch="),
            "new append after floor must tail"
        );
        let after = assert_historical_target(&target, &expected, 8_256, &fixture.frozen).await?;
        ensure!(
            !run_service(&root, &target, &checkpoint)?.contains("batch="),
            "final restart idle"
        );
        ensure!(
            local_source(&target)?.capture()? == after,
            "final restart no extra target version or files"
        );
        let mut state = Checkpoint::open_local(&checkpoint).await?;
        let recovered = state.recover().await?;
        ensure!(recovered.pending_offset.is_none(), "all lifecycle offsets committed");
        let query = state.query_id().to_owned();
        let batch = recovered.batch_id - 1;
        state.close().await?;
        let sink = DeltaTable::open_for_maintenance(&local_source(&target)?.url)?;
        ensure!(
            sink.lookup_txn(&query)? == Some(batch),
            "stable query/batch transaction after lifecycle"
        );
        ensure!(
            std::fs::read(checkpoint.join(format!("commits/{batch}")))? == b"v1\n{\"nextBatchWatermarkMs\":0}",
            "Spark stateless commit remains exact"
        );
        ensure!(
            local_source(&fixture.root)?.capture()? == fixture.frozen,
            "original frozen source must remain unchanged"
        );
        println!(
            "COLLECTOR_SOURCE_LIFECYCLE {}",
            serde_json::to_string(&json!({
                "pool":fixture.pool,"bootstrapRows":8128,"afterLiveAppendRows":8192,"finalRows":8256,
                "optimizePurgeDoNotReemit":true,"retainedCheckpointFloor":floor,
                "postPruneRestartIdle":true,"postFloorAppendExactlyOnce":true,
                "allColumnMultisetEqual":true,"actualServiceProcessesReaped":true,
                "disposableCopyOnly":true,"phaseDeadlineSeconds":120,
            }))?
        );
    }
    Ok(())
}
