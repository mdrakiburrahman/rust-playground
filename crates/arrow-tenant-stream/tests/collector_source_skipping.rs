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
