//! Released-writer final compaction. All limits are fail-closed, never sampling.
//! The 300s deadline is cooperative; only the outer runner's kill is a hard limit.

use anyhow::{Context, Result, ensure};
use arrow::array::{Array, ArrayRef, RecordBatch, StringArray, new_null_array};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::row::{RowConverter, SortField};
use arrow_deltalake::delta::{DeltaTable, INGESTION_PARTITION, object_store_for_url};
use arrow_deltalake::maintenance::{
    DataFile, Granularity, MaintenanceControl, MaintenanceReport, OptimizeConfig, OptimizePlan, Outcome,
};
use delta_kernel::Snapshot;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::scan::state::ScanFile;
use delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};
use url::Url;

use crate::cli::FinalCompactionArgs;
use crate::schema::{TABLE_COUNT, schema_statements};

const BUDGET: Duration = Duration::from_secs(300);
const MAX_PASSES: usize = 3;
const MAX_FILES: usize = 4096;
const MAX_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_ROW_GROUP_BYTES: i64 = 128 * 1024 * 1024;
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const MAX_DISTINCT_ROWS: usize = 65_536;
const MAX_ROWS: u64 = 10_000_000;
const MAX_MULTISET_BYTES: usize = 64 * 1024 * 1024;
const MAX_TOTAL_MULTISET_BYTES: usize = 256 * 1024 * 1024;
const MAX_JSON_BYTES: u64 = 1024 * 1024;
const ARTIFACT: &str = "final-compaction.json";

fn check(deadline: Instant) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        "final-compaction 300s cooperative budget exhausted"
    );
    Ok(())
}

fn row_group_bound(bytes: i64) -> Result<()> {
    ensure!(
        (0..=MAX_ROW_GROUP_BYTES).contains(&bytes),
        "Parquet row-group decode bound exceeded"
    );
    Ok(())
}

fn batch_bound(bytes: usize) -> Result<()> {
    ensure!(bytes <= MAX_BATCH_BYTES, "decoded batch byte bound exceeded");
    Ok(())
}

fn total_bytes(current: usize, additional: usize) -> Result<usize> {
    let bytes = current
        .checked_add(additional)
        .context("global multiset byte overflow")?;
    ensure!(
        bytes <= MAX_TOTAL_MULTISET_BYTES,
        "global canonical multiset byte bound exceeded"
    );
    Ok(bytes)
}

#[derive(Deserialize)]
struct RunMetadata {
    run_id: String,
}

#[derive(Deserialize)]
struct PublisherRelease {
    run_id: Option<String>,
    passed: bool,
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let file = File::open(path).with_context(|| format!("read {}", path.display()))?;
    ensure!(
        file.metadata()?.len() <= MAX_JSON_BYTES,
        "receipt exceeds JSON size bound"
    );
    Ok(serde_json::from_reader(file)?)
}

fn require_release(results: &Path, run_id: &str) -> Result<()> {
    let release: PublisherRelease = read_json(&results.join("publisher-release.json"))?;
    ensure!(
        release.passed && release.run_id.as_deref() == Some(run_id),
        "publisher release must pass for the same nonnull run_id"
    );
    Ok(())
}

fn cloud_root(path: &Path) -> bool {
    path.to_str()
        .is_some_and(|raw| Url::parse(raw).is_ok_and(|url| url.scheme() != "file"))
}

/// Whether to compact, skip intentionally, or retain Azure's existing default.
fn enabled(args: &FinalCompactionArgs) -> Result<bool> {
    ensure!(
        !(args.final_compaction && args.skip_final_compaction),
        "cannot request and skip final-compaction"
    );
    let azure = cloud_root(&args.direct_root) || cloud_root(&args.dataflow_root);
    ensure!(
        !(azure && args.final_compaction),
        "explicit Azure final-compaction is unsupported"
    );
    Ok(!azure && !args.skip_final_compaction)
}

fn local_root(path: &Path) -> Result<PathBuf> {
    let path = if path.to_str().is_some_and(|raw| raw.starts_with("file:")) {
        Url::parse(path.to_str().context("non-UTF8 root")?)?
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("invalid local root URL"))?
    } else {
        path.to_owned()
    };
    ensure!(
        !fs::symlink_metadata(&path)?.file_type().is_symlink(),
        "root cannot be a symlink"
    );
    Ok(fs::canonicalize(path)?)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct Inventory {
    schema: Value,
    version: u64,
    files: Vec<String>,
}

#[derive(Clone, Debug)]
struct State {
    inventory: Inventory,
    table_id: String,
    partition_columns: Vec<String>,
    multiset: BTreeMap<Vec<u8>, u64>,
    rows: u64,
    retained_bytes: usize,
}

impl State {
    fn digest(&self) -> String {
        let mut digest = Sha256::new();
        for (row, count) in &self.multiset {
            digest.update((row.len() as u64).to_le_bytes());
            digest.update(row);
            digest.update(count.to_le_bytes());
        }
        format!("sha256:{}", hex::encode(digest.finalize()))
    }

    fn add_row(&mut self, row: &[u8]) -> Result<()> {
        ensure!(self.rows < MAX_ROWS, "table exceeds row bound {MAX_ROWS}");
        if !self.multiset.contains_key(row) {
            ensure!(self.multiset.len() < MAX_DISTINCT_ROWS, "distinct-row bound exceeded");
            let bytes = row.len().checked_add(64).context("multiset byte overflow")?;
            let next = self
                .retained_bytes
                .checked_add(bytes)
                .context("multiset byte overflow")?;
            ensure!(
                next <= MAX_MULTISET_BYTES,
                "table canonical multiset byte bound exceeded"
            );
            self.retained_bytes = next;
        }
        let count = self.multiset.entry(row.to_vec()).or_default();
        *count = count.checked_add(1).context("row multiplicity overflow")?;
        self.rows += 1;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
struct Planned {
    table_id: String,
    version: u64,
    plan_id: String,
    bins: usize,
    inputs: Vec<DataFile>,
    backlog_files: u64,
    protected_partitions: u64,
    skipped_partitions: u64,
}

impl Planned {
    fn empty(&self) -> bool {
        self.bins == 0 && self.inputs.is_empty() && self.backlog_files == 0
    }
}

#[derive(Serialize)]
struct PassEvidence {
    number: usize,
    plans: BTreeMap<String, Planned>,
    reports: BTreeMap<String, MaintenanceReport>,
}

trait Backend {
    fn capture(&mut self, deadline: Instant) -> Result<BTreeMap<String, State>>;
    fn plan(&mut self, deadline: Instant) -> Result<BTreeMap<String, Planned>>;
    fn apply(&mut self, key: &str, deadline: Instant) -> Result<MaintenanceReport>;
}

fn exact_keys<T>(values: &BTreeMap<String, T>, expected: &BTreeSet<String>) -> Result<()> {
    ensure!(
        values.keys().cloned().collect::<BTreeSet<_>>() == *expected,
        "final-compaction table set must match all 26 expected tables"
    );
    Ok(())
}

fn finish<B: Backend>(
    backend: &mut B,
    expected: &BTreeSet<String>,
    deadline: Instant,
    passes: &mut Vec<PassEvidence>,
) -> Result<(BTreeMap<String, State>, BTreeMap<String, State>)> {
    check(deadline)?;
    let before = backend.capture(deadline)?;
    exact_keys(&before, expected)?;
    let mut versions: BTreeMap<_, _> = before
        .iter()
        .map(|(key, state)| (key.clone(), state.inventory.version))
        .collect();
    loop {
        check(deadline)?;
        let plans = backend.plan(deadline)?;
        check(deadline)?;
        exact_keys(&plans, expected)?;
        for (key, plan) in &plans {
            ensure!(
                plan.version == versions[key] && plan.table_id == before[key].table_id,
                "{key}: snapshot changed outside final-compaction"
            );
            ensure!(
                plan.protected_partitions == 0 && plan.skipped_partitions == 0,
                "{key}: protected or unsupported partitions prevent complete final-compaction"
            );
            ensure!(plan.bins > 0 || plan.inputs.is_empty(), "{key}: malformed empty plan");
        }
        let empty = plans.values().all(Planned::empty);
        if empty {
            // Preserve the fresh all-table no-op plan as evidence, not a synthetic report.
            passes.push(PassEvidence {
                number: passes.len() + 1,
                plans,
                reports: BTreeMap::new(),
            });
            break;
        }
        ensure!(
            passes.len() < MAX_PASSES,
            "third pass still leaves eligible work; final-compaction failed"
        );
        passes.push(PassEvidence {
            number: passes.len() + 1,
            plans,
            reports: BTreeMap::new(),
        });
        let pass = passes.last_mut().context("missing compaction pass")?;
        let mut progress = false;
        for (key, plan) in &pass.plans {
            if plan.empty() {
                continue;
            }
            ensure!(
                plan.bins > 0 && plan.inputs.len() >= 2,
                "{key}: backlog without an actionable plan"
            );
            check(deadline)?;
            let report = backend
                .apply(key, deadline)
                .with_context(|| format!("{key}: OPTIMIZE operation failed (no retry)"))?;
            // Keep the real result even when a later invariant fails.
            pass.reports.insert(key.clone(), report.clone());
            check(deadline)?;
            ensure!(
                report.outcome == Outcome::Committed
                    && report.read_version == plan.version
                    && report.version == plan.version.checked_add(1).context("version overflow")?
                    && report.files_added > 0
                    && report.files_removed > report.files_added
                    && report.files_removed <= plan.inputs.len() as u64
                    && report.outputs.len() as u64 == report.files_added,
                "{key}: OPTIMIZE did not produce a real reducing commit"
            );
            versions.insert(key.clone(), report.version);
            progress = true;
        }
        ensure!(progress, "compaction pass made no actual progress");
    }
    check(deadline)?;
    let after = backend.capture(deadline)?;
    exact_keys(&after, expected)?;
    for (key, old) in &before {
        let new = &after[key];
        ensure!(
            new.inventory.version == versions[key]
                && new.table_id == old.table_id
                && new.inventory.schema == old.inventory.schema
                && new.partition_columns == old.partition_columns,
            "{key}: final inventory changed identity, schema, partitions or version"
        );
        ensure!(
            new.rows == old.rows && new.multiset == old.multiset,
            "{key}: canonical row multiset mismatch (loss or duplicate)"
        );
    }
    check(deadline)?;
    Ok((before, after))
}

#[derive(Default)]
struct Captured {
    files: Vec<DataFile>,
    bytes: usize,
    error: Option<String>,
}

impl Captured {
    fn add(&mut self, file: DataFile) -> Result<()> {
        ensure!(self.files.len() < MAX_FILES, "active-file bound exceeded");
        ensure!(
            file.size > 0 && file.size <= MAX_FILE_BYTES,
            "active-file size bound exceeded"
        );
        let bytes = file.path.len()
            + file
                .partition_values
                .iter()
                .map(|(key, value)| key.len() + value.as_ref().map_or(0, String::len))
                .sum::<usize>();
        self.bytes = self.bytes.checked_add(bytes).context("manifest size overflow")?;
        ensure!(self.bytes <= MAX_MANIFEST_BYTES, "active manifest byte bound exceeded");
        self.files.push(file);
        Ok(())
    }
}

fn collect_file(context: &mut Captured, file: ScanFile) {
    if context.error.is_some() {
        return;
    }
    let result = (|| -> Result<()> {
        ensure!(!file.dv_info.has_vector(), "deletion vectors are unsupported");
        let size = u64::try_from(file.size).context("negative Add size")?;
        context.add(DataFile {
            path: file.path,
            size,
            rows: None,
            partition_values: file
                .partition_values
                .into_iter()
                .map(|(key, value)| (key, Some(value)))
                .collect(),
        })?;
        Ok(())
    })();
    if let Err(error) = result {
        context.error = Some(format!("{error:#}"));
    }
}

struct Table {
    root: PathBuf,
    url: Url,
    table: Arc<DeltaTable>,
}

struct Local {
    tables: BTreeMap<String, Table>,
    engine: DefaultEngine<TokioBackgroundExecutor>,
    config: OptimizeConfig,
    plans: BTreeMap<String, OptimizePlan>,
}

impl Local {
    fn open(roots: &BTreeMap<String, PathBuf>, names: &[String], deadline: Instant) -> Result<Self> {
        let first = roots.values().next().context("no pool roots")?;
        let first_url = Url::from_directory_path(first).map_err(|()| anyhow::anyhow!("invalid root"))?;
        let engine = DefaultEngineBuilder::new(object_store_for_url(&first_url)?).build();
        let mut tables = BTreeMap::new();
        let mut all_paths = BTreeSet::new();
        for (pool, root) in roots {
            for name in names {
                check(deadline)?;
                let path = root.join("telemetry").join("sqlserver").join(name);
                let canonical = fs::canonicalize(&path)?;
                ensure!(
                    canonical == path && canonical.starts_with(root),
                    "table path is symlinked or escapes pool"
                );
                ensure!(all_paths.insert(canonical.clone()), "pool tables alias each other");
                let url = Url::from_directory_path(&canonical).map_err(|()| anyhow::anyhow!("invalid table URL"))?;
                let table = DeltaTable::open_for_maintenance(&url)?;
                tables.insert(
                    format!("{pool}/telemetry.sqlserver.{name}"),
                    Table {
                        root: canonical,
                        url,
                        table,
                    },
                );
            }
        }
        Ok(Self {
            tables,
            engine,
            config: OptimizeConfig {
                sealed_final: true,
                granularity: Some(Granularity::Minute),
                ..OptimizeConfig::default()
            },
            plans: BTreeMap::new(),
        })
    }

    fn state(&self, table: &Table, deadline: Instant) -> Result<State> {
        check(deadline)?;
        let snapshot = Snapshot::builder_for(table.url.as_str()).build(&self.engine)?;
        let metadata = snapshot.table_configuration().metadata();
        let schema: Schema = snapshot.schema().as_ref().try_into_arrow()?;
        let schema = Arc::new(schema);
        let partitions = metadata.partition_columns().to_vec();
        ensure!(
            partitions == [INGESTION_PARTITION],
            "expected ingestion-minute partition column"
        );
        let schema_json = serde_json::to_value(snapshot.schema())?;
        let mut state = State {
            inventory: Inventory {
                schema: schema_json,
                version: snapshot.version(),
                files: Vec::new(),
            },
            table_id: metadata.id().to_string(),
            partition_columns: partitions,
            multiset: BTreeMap::new(),
            rows: 0,
            retained_bytes: 0,
        };
        let scan = Arc::clone(&snapshot).scan_builder().build()?;
        let mut captured = Captured::default();
        for data in scan.scan_metadata(&self.engine)? {
            check(deadline)?;
            captured = data?.visit_scan_files(captured, collect_file)?;
            ensure!(
                captured.error.is_none(),
                "{}",
                captured.error.as_deref().unwrap_or("scan failed")
            );
        }
        captured.files.sort_by(|left, right| left.path.cmp(&right.path));
        let converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )?;
        for file in captured.files {
            check(deadline)?;
            let url = table.url.join(&file.path)?;
            ensure!(url.as_str().starts_with(table.url.as_str()), "file URL escapes table");
            let path = url
                .to_file_path()
                .map_err(|()| anyhow::anyhow!("non-local active file"))?;
            let path = fs::canonicalize(path)?;
            ensure!(path.starts_with(&table.root), "active file escapes table");
            let relative = path
                .strip_prefix(&table.root)?
                .to_str()
                .context("non-UTF8 active file")?
                .to_owned();
            ensure!(!state.inventory.files.contains(&relative), "duplicate active file");
            let input = File::open(&path)?;
            ensure!(
                input.metadata()?.len() == file.size,
                "active file size differs from Add"
            );
            let builder = ParquetRecordBatchReaderBuilder::try_new(input)?;
            for group in builder.metadata().row_groups() {
                row_group_bound(group.total_byte_size())?;
            }
            let partition = file
                .partition_values
                .get(INGESTION_PARTITION)
                .and_then(Option::as_deref)
                .context("missing nonnull ingestion partition")?;
            for batch in builder.with_batch_size(1024).build()? {
                check(deadline)?;
                let batch = batch?;
                batch_bound(batch.get_array_memory_size())?;
                let logical = logical_batch(&batch, &schema, partition)?;
                let rows = converter.convert_columns(logical.columns())?;
                for row in rows.iter() {
                    state.add_row(row.as_ref())?;
                }
            }
            state.inventory.files.push(relative);
        }
        state.inventory.files.sort();
        ensure!(
            table.table.version()? == state.inventory.version,
            "table changed during canonical row capture"
        );
        check(deadline)?;
        Ok(state)
    }
}

fn logical_batch(batch: &RecordBatch, schema: &SchemaRef, partition: &str) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        let array = if field.name() == INGESTION_PARTITION {
            ensure!(field.data_type() == &DataType::Utf8, "ingestion partition must be UTF8");
            Arc::new(StringArray::from(vec![partition; batch.num_rows()])) as ArrayRef
        } else if let Some(array) = batch.column_by_name(field.name()) {
            arrow::compute::cast(array, field.data_type())?
        } else {
            ensure!(field.is_nullable(), "missing required physical field {}", field.name());
            new_null_array(field.data_type(), batch.num_rows())
        };
        ensure!(
            field.is_nullable() || array.null_count() == 0,
            "required field contains nulls"
        );
        columns.push(array);
    }
    if let Some(physical) = batch.column_by_name(INGESTION_PARTITION) {
        let physical = physical
            .as_any()
            .downcast_ref::<StringArray>()
            .context("physical partition must be UTF8")?;
        ensure!(
            physical.null_count() == 0 && physical.iter().all(|value| value == Some(partition)),
            "physical partition differs from committed Add"
        );
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

impl Backend for Local {
    fn capture(&mut self, deadline: Instant) -> Result<BTreeMap<String, State>> {
        let mut result = BTreeMap::new();
        let mut bytes = 0_usize;
        for (key, table) in &self.tables {
            let state = self.state(table, deadline)?;
            bytes = total_bytes(bytes, state.retained_bytes)?;
            result.insert(key.clone(), state);
        }
        Ok(result)
    }

    fn plan(&mut self, deadline: Instant) -> Result<BTreeMap<String, Planned>> {
        self.plans.clear();
        let mut evidence = BTreeMap::new();
        for (key, table) in &self.tables {
            check(deadline)?;
            let plan = table.table.plan_optimize(&self.config)?;
            evidence.insert(
                key.clone(),
                Planned {
                    table_id: plan.header.fingerprint.table_id.clone(),
                    version: plan.header.read_version,
                    plan_id: plan.header.plan_id.clone(),
                    bins: plan.bins.len(),
                    inputs: plan.bins.iter().flat_map(|bin| bin.files.clone()).collect(),
                    backlog_files: plan.backlog_files,
                    protected_partitions: plan.protected_partitions,
                    skipped_partitions: plan.skipped_partitions,
                },
            );
            self.plans.insert(key.clone(), plan);
        }
        Ok(evidence)
    }

    fn apply(&mut self, key: &str, deadline: Instant) -> Result<MaintenanceReport> {
        check(deadline)?;
        let control = MaintenanceControl::new(Arc::new(AtomicBool::new(false)), Some(deadline));
        self.tables.get(key).context("unknown table")?.table.apply_optimize(
            self.plans.get(key).context("missing fresh plan")?,
            &self.config,
            &control,
        )
    }
}

#[derive(Serialize)]
struct TableEvidence {
    version_before: u64,
    version_after: u64,
    rows: u64,
    canonical_multiset_sha256: String,
    table_id: String,
}

#[derive(Serialize)]
struct PoolEvidence {
    root: PathBuf,
    validation_inventory: ValidationInventory,
    tables: BTreeMap<String, TableEvidence>,
}

#[derive(Default, Serialize)]
struct ValidationInventory {
    tables: BTreeMap<String, Inventory>,
}

#[derive(Serialize)]
struct Receipt {
    run_id: String,
    passed: bool,
    skipped: bool,
    error: Option<String>,
    budget_secs: u64,
    max_progress_passes: usize,
    passes: Vec<PassEvidence>,
    pools: BTreeMap<String, PoolEvidence>,
}

fn atomic_receipt(results: &Path, receipt: &Receipt, deadline: Option<Instant>) -> Result<()> {
    let destination = results.join(ARTIFACT);
    ensure!(
        !destination.exists(),
        "final-compaction receipt already exists; preserve it"
    );
    let temporary = results.join(format!(".final-compaction-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&temporary)?;
        serde_json::to_writer_pretty(&mut file, receipt)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        if let Some(deadline) = deadline {
            check(deadline)?;
        }
        // A hard link atomically publishes without replacing any pre-existing receipt.
        fs::hard_link(&temporary, &destination)?;
        fs::remove_file(&temporary)?;
        File::open(results)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() && temporary.exists() {
        fs::remove_file(&temporary).context("clean failed receipt temporary file")?;
    }
    result
}

/// Run final compaction off the async executor, retaining custody until it finishes.
///
/// # Errors
/// Rejects missing/mismatched release, unsupported Azure requests, changed tables,
/// exhausted bounds/deadline, operation errors, and any row-multiset discrepancy.
pub async fn run(args: FinalCompactionArgs) -> Result<()> {
    let deadline = Instant::now() + BUDGET;
    tokio::task::spawn_blocking(move || run_sync(&args, deadline))
        .await
        .context("final-compaction worker panicked")?
}

fn run_sync(args: &FinalCompactionArgs, deadline: Instant) -> Result<()> {
    check(deadline)?;
    let metadata: RunMetadata = read_json(&args.results.join("run-metadata.json"))?;
    ensure!(!metadata.run_id.is_empty(), "empty run_id");
    ensure!(
        !args.results.join(ARTIFACT).exists(),
        "final-compaction receipt already exists; preserve it"
    );
    let mut receipt = Receipt {
        run_id: metadata.run_id,
        passed: false,
        skipped: false,
        error: None,
        budget_secs: BUDGET.as_secs(),
        max_progress_passes: MAX_PASSES,
        passes: Vec::new(),
        pools: BTreeMap::new(),
    };
    let result = (|| -> Result<()> {
        if !enabled(args)? {
            receipt.skipped = true;
            return Ok(());
        }
        require_release(&args.results, &receipt.run_id)?;
        let roots = BTreeMap::from([
            ("direct".to_owned(), local_root(&args.direct_root)?),
            ("dataflow".to_owned(), local_root(&args.dataflow_root)?),
        ]);
        ensure!(roots["direct"] != roots["dataflow"], "pool roots must not alias");
        let names = schema_statements()?
            .into_iter()
            .map(|statement| statement.identity.table().to_string())
            .collect::<Vec<_>>();
        ensure!(names.len() == TABLE_COUNT, "canonical schema fixture changed");
        let mut local = Local::open(&roots, &names, deadline)?;
        let expected = local.tables.keys().cloned().collect::<BTreeSet<_>>();
        ensure!(expected.len() == 2 * TABLE_COUNT, "expected exactly 26 tables");
        let (before, after) = finish(&mut local, &expected, deadline, &mut receipt.passes)?;
        for (pool, root) in roots {
            let mut evidence = PoolEvidence {
                root,
                validation_inventory: ValidationInventory::default(),
                tables: BTreeMap::new(),
            };
            for (key, state) in after.iter().filter(|(key, _)| key.starts_with(&format!("{pool}/"))) {
                let identity = key
                    .strip_prefix(&format!("{pool}/"))
                    .context("missing pool prefix")?
                    .to_owned();
                evidence.tables.insert(
                    identity.clone(),
                    TableEvidence {
                        version_before: before[key].inventory.version,
                        version_after: state.inventory.version,
                        rows: state.rows,
                        canonical_multiset_sha256: state.digest(),
                        table_id: state.table_id.clone(),
                    },
                );
                evidence
                    .validation_inventory
                    .tables
                    .insert(identity, state.inventory.clone());
            }
            receipt.pools.insert(pool, evidence);
        }
        check(deadline)?;
        receipt.passed = true;
        Ok(())
    })();
    if let Err(error) = &result {
        receipt.error = Some(format!("{error:#}"));
    }
    atomic_receipt(&args.results, &receipt, result.is_ok().then_some(deadline))?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::bail;
    use arrow::array::Int64Array;
    use arrow::datatypes::Field;
    use clap::Parser;
    use std::collections::VecDeque;

    fn args(results: &Path) -> FinalCompactionArgs {
        FinalCompactionArgs {
            results: results.to_owned(),
            direct_root: results.join("direct"),
            dataflow_root: results.join("dataflow"),
            final_compaction: false,
            skip_final_compaction: false,
        }
    }

    fn receipts(results: &Path, release: Value) -> Result<()> {
        fs::write(results.join("run-metadata.json"), br#"{"run_id":"run-1"}"#)?;
        fs::write(results.join("publisher-release.json"), serde_json::to_vec(&release)?)?;
        Ok(())
    }

    fn file(index: usize) -> DataFile {
        DataFile {
            path: format!("file-{index}.parquet"),
            size: 10,
            rows: Some(1),
            partition_values: BTreeMap::new(),
        }
    }

    fn state(version: u64) -> State {
        State {
            inventory: Inventory {
                schema: serde_json::json!({"type":"struct","fields":[]}),
                version,
                files: vec!["a".into()],
            },
            table_id: "table".into(),
            partition_columns: vec![INGESTION_PARTITION.into()],
            multiset: BTreeMap::from([(b"complete-row".to_vec(), 1)]),
            rows: 1,
            retained_bytes: 76,
        }
    }

    struct Fixture {
        keys: BTreeSet<String>,
        versions: BTreeMap<String, u64>,
        work: VecDeque<usize>,
        captures: usize,
        applies: usize,
        operation_error: bool,
        bunk: bool,
        mismatch: bool,
        protected: bool,
        skipped: bool,
        missing: bool,
    }

    impl Fixture {
        fn new(work: &[usize]) -> Self {
            let keys = (0..26)
                .map(|index| format!("table-{index:02}"))
                .collect::<BTreeSet<_>>();
            Self {
                versions: keys.iter().map(|key| (key.clone(), 1)).collect(),
                keys,
                work: work.iter().copied().collect(),
                captures: 0,
                applies: 0,
                operation_error: false,
                bunk: false,
                mismatch: false,
                protected: false,
                skipped: false,
                missing: false,
            }
        }
    }

    impl Backend for Fixture {
        fn capture(&mut self, _deadline: Instant) -> Result<BTreeMap<String, State>> {
            self.captures += 1;
            Ok(self
                .versions
                .iter()
                .map(|(key, version)| {
                    let mut value = state(*version);
                    if self.mismatch && self.captures > 1 {
                        value.multiset.insert(b"duplicate".to_vec(), 1);
                        value.rows += 1;
                    }
                    (key.clone(), value)
                })
                .collect())
        }

        fn plan(&mut self, _deadline: Instant) -> Result<BTreeMap<String, Planned>> {
            let work = self.work.pop_front().context("unexpected retry/plan")?;
            let mut plans = self
                .versions
                .iter()
                .enumerate()
                .map(|(index, (key, version))| {
                    let working = index == 0 && work > 0;
                    (
                        key.clone(),
                        Planned {
                            table_id: "table".into(),
                            version: *version,
                            plan_id: format!("plan-{version}"),
                            bins: usize::from(working),
                            inputs: if working { vec![file(0), file(1)] } else { Vec::new() },
                            backlog_files: 0,
                            protected_partitions: u64::from(self.protected),
                            skipped_partitions: u64::from(self.skipped),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>();
            if self.missing {
                plans.pop_first();
            }
            Ok(plans)
        }

        fn apply(&mut self, key: &str, _deadline: Instant) -> Result<MaintenanceReport> {
            self.applies += 1;
            if self.operation_error {
                bail!("injected operation error");
            }
            let version = self.versions.get_mut(key).context("missing table")?;
            let read_version = *version;
            *version += 1;
            Ok(MaintenanceReport {
                outcome: Outcome::Committed,
                read_version,
                version: *version,
                files_removed: if self.bunk { 0 } else { 2 },
                files_added: if self.bunk { 0 } else { 1 },
                outputs: if self.bunk {
                    Vec::new()
                } else {
                    vec!["real-output".into()]
                },
                ..MaintenanceReport::default()
            })
        }
    }

    fn execute(fixture: &mut Fixture) -> Result<Vec<PassEvidence>> {
        let expected = fixture.keys.clone();
        let mut passes = Vec::new();
        finish(fixture, &expected, Instant::now() + BUDGET, &mut passes)?;
        Ok(passes)
    }

    #[test]
    fn all_26_need_a_fresh_empty_plan_and_only_progress_continues() -> Result<()> {
        let mut fixture = Fixture::new(&[1, 1, 0]);
        let passes = execute(&mut fixture)?;
        assert_eq!(fixture.applies, 2);
        assert_eq!(passes.len(), 3);
        assert_eq!(passes[2].plans.len(), 26);
        assert!(passes[2].plans.values().all(Planned::empty));
        assert!(passes[2].reports.is_empty());
        let mut empty = Fixture::new(&[0]);
        execute(&mut empty)?;
        assert_eq!(empty.applies, 0);
        Ok(())
    }

    #[test]
    fn third_progress_pass_without_final_noop_fails_not_retries() -> Result<()> {
        let mut fixture = Fixture::new(&[1, 1, 1, 1]);
        let error = execute(&mut fixture).err().context("must fail")?;
        assert!(error.to_string().contains("third pass still leaves"));
        assert_eq!(fixture.applies, 3);
        assert!(fixture.work.is_empty());
        Ok(())
    }

    #[test]
    fn operation_error_is_immediate_without_retry() -> Result<()> {
        let mut fixture = Fixture::new(&[1, 0]);
        fixture.operation_error = true;
        let error = execute(&mut fixture).err().context("must fail")?;
        assert!(format!("{error:#}").contains("OPTIMIZE operation failed (no retry)"));
        assert_eq!(fixture.applies, 1);
        assert_eq!(fixture.work.len(), 1, "no re-plan after operation error");
        Ok(())
    }

    #[test]
    fn bunk_commits_and_row_loss_or_duplicates_fail() -> Result<()> {
        let mut fixture = Fixture::new(&[1, 0]);
        fixture.bunk = true;
        assert!(
            execute(&mut fixture)
                .err()
                .context("must fail")?
                .to_string()
                .contains("real reducing commit")
        );
        assert_eq!(fixture.applies, 1);
        let mut changed = Fixture::new(&[1, 0]);
        changed.mismatch = true;
        assert!(
            execute(&mut changed)
                .err()
                .context("must fail")?
                .to_string()
                .contains("canonical row multiset mismatch")
        );
        Ok(())
    }

    #[test]
    fn table_sets_and_unsupported_or_protected_partitions_fail_closed() {
        for mode in 0..3 {
            let mut fixture = Fixture::new(&[0]);
            fixture.missing = mode == 0;
            fixture.protected = mode == 1;
            fixture.skipped = mode == 2;
            assert!(execute(&mut fixture).is_err());
            assert_eq!(fixture.applies, 0);
        }
    }

    #[test]
    fn budget_exhaustion_prevents_any_work() -> Result<()> {
        let mut fixture = Fixture::new(&[1, 0]);
        let expected = fixture.keys.clone();
        let mut passes = Vec::new();
        let result = finish(&mut fixture, &expected, Instant::now(), &mut passes);
        assert!(
            result
                .err()
                .context("must fail")?
                .to_string()
                .contains("cooperative budget exhausted")
        );
        assert_eq!((fixture.captures, fixture.applies), (0, 0));
        Ok(())
    }

    #[test]
    fn row_and_decoding_guards_are_named_failures() -> Result<()> {
        let mut value = state(1);
        value.rows = MAX_ROWS;
        assert!(
            value
                .add_row(b"x")
                .err()
                .context("row bound")?
                .to_string()
                .contains("row bound")
        );
        value.rows = 0;
        value.retained_bytes = MAX_MULTISET_BYTES;
        assert!(
            value
                .add_row(b"x")
                .err()
                .context("byte bound")?
                .to_string()
                .contains("multiset byte bound")
        );
        value.retained_bytes = 0;
        value.multiset = (0..MAX_DISTINCT_ROWS)
            .map(|index| (index.to_le_bytes().to_vec(), 1))
            .collect();
        assert!(
            value
                .add_row(b"new distinct")
                .err()
                .context("distinct bound")?
                .to_string()
                .contains("distinct-row")
        );
        assert!(total_bytes(MAX_TOTAL_MULTISET_BYTES, 1).is_err());
        assert!(row_group_bound(MAX_ROW_GROUP_BYTES + 1).is_err());
        assert!(row_group_bound(-1).is_err());
        assert!(batch_bound(MAX_BATCH_BYTES + 1).is_err());
        let mut captured = Captured {
            files: vec![file(0); MAX_FILES],
            ..Captured::default()
        };
        assert!(
            captured
                .add(file(1))
                .err()
                .context("file count")?
                .to_string()
                .contains("active-file bound")
        );
        let mut captured = Captured::default();
        let mut huge = file(0);
        huge.size = MAX_FILE_BYTES + 1;
        assert!(
            captured
                .add(huge)
                .err()
                .context("file size")?
                .to_string()
                .contains("size bound")
        );
        captured.bytes = MAX_MANIFEST_BYTES;
        assert!(
            captured
                .add(file(0))
                .err()
                .context("manifest bytes")?
                .to_string()
                .contains("manifest byte")
        );
        Ok(())
    }

    #[test]
    fn release_missing_false_null_and_wrong_run_all_fail_before_table_effects() -> Result<()> {
        let root = tempfile::tempdir()?;
        let arguments = args(root.path());
        fs::write(root.path().join("run-metadata.json"), br#"{"run_id":"run-1"}"#)?;
        assert!(run_sync(&arguments, Instant::now() + BUDGET).is_err());
        assert!(!arguments.direct_root.exists());
        for release in [
            serde_json::json!({"run_id":"run-1","passed":false}),
            serde_json::json!({"run_id":null,"passed":true}),
            serde_json::json!({"run_id":"","passed":true}),
            serde_json::json!({"run_id":"wrong","passed":true}),
        ] {
            let case = tempfile::tempdir()?;
            receipts(case.path(), release)?;
            let arguments = args(case.path());
            assert!(run_sync(&arguments, Instant::now() + BUDGET).is_err());
            assert!(!arguments.direct_root.exists());
            let evidence: Value = read_json(&case.path().join(ARTIFACT))?;
            assert_eq!(evidence["passed"], false);
            assert!(
                evidence["error"]
                    .as_str()
                    .context("error")?
                    .contains("publisher release")
            );
        }
        Ok(())
    }

    #[test]
    fn null_missing_and_empty_metadata_run_ids_never_match_a_release() -> Result<()> {
        for metadata in [
            serde_json::json!({"run_id":null}),
            serde_json::json!({}),
            serde_json::json!({"run_id":""}),
        ] {
            let root = tempfile::tempdir()?;
            fs::write(root.path().join("run-metadata.json"), serde_json::to_vec(&metadata)?)?;
            fs::write(
                root.path().join("publisher-release.json"),
                br#"{"run_id":null,"passed":true}"#,
            )?;
            let arguments = args(root.path());
            assert!(run_sync(&arguments, Instant::now() + BUDGET).is_err());
            assert!(!arguments.direct_root.exists());
        }
        Ok(())
    }

    #[test]
    fn azure_explicit_rejected_default_skipped_and_local_optout_explicit() -> Result<()> {
        let root = tempfile::tempdir()?;
        let mut arguments = args(root.path());
        assert!(enabled(&arguments)?);
        arguments.skip_final_compaction = true;
        assert!(!enabled(&arguments)?);
        arguments.direct_root = "abfss://container@account.dfs.core.windows.net/direct".into();
        arguments.skip_final_compaction = false;
        assert!(!enabled(&arguments)?);
        arguments.final_compaction = true;
        assert!(
            enabled(&arguments)
                .err()
                .context("Azure unsupported")?
                .to_string()
                .contains("Azure")
        );
        let cli = crate::Cli::try_parse_from(["arrowcol-loadgen", "final-compaction", "--skip-final-compaction"])?;
        assert!(matches!(cli.command, crate::Command::FinalCompaction(value) if value.skip_final_compaction));
        assert!(
            crate::Cli::try_parse_from([
                "arrowcol-loadgen",
                "final-compaction",
                "--skip-final-compaction",
                "--final-compaction"
            ])
            .is_err()
        );
        let case = tempfile::tempdir()?;
        fs::write(case.path().join("run-metadata.json"), br#"{"run_id":"run-1"}"#)?;
        let mut arguments = args(case.path());
        arguments.skip_final_compaction = true;
        run_sync(&arguments, Instant::now() + BUDGET)?;
        let evidence: Value = read_json(&case.path().join(ARTIFACT))?;
        assert_eq!(evidence["skipped"], true);
        assert_eq!(evidence["passed"], false);
        assert!(!arguments.direct_root.exists());
        assert!(
            run_sync(&arguments, Instant::now() + BUDGET).is_err(),
            "never overwrite prior evidence"
        );
        Ok(())
    }

    #[tokio::test]
    async fn local_26_table_fixture_compacts_and_binds_independent_validation_inventory() -> Result<()> {
        let root = tempfile::tempdir()?;
        let arguments = args(root.path());
        receipts(root.path(), serde_json::json!({"run_id":"run-1","passed":true}))?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(INGESTION_PARTITION, DataType::Utf8, false),
        ]));
        for pool in [&arguments.direct_root, &arguments.dataflow_root] {
            for statement in schema_statements()? {
                let path = pool
                    .join("telemetry/sqlserver")
                    .join(statement.identity.table().as_str());
                fs::create_dir_all(&path)?;
                let url = Url::from_directory_path(&path).map_err(|()| anyhow::anyhow!("fixture URL"))?;
                let table = DeltaTable::open_url(url, Arc::clone(&schema), vec![INGESTION_PARTITION.into()])?;
                for id in 0..2 {
                    let mut writer = table
                        .new_file_partitioned(
                            Arc::clone(&schema),
                            BTreeMap::from([(INGESTION_PARTITION.into(), Some("2024-01-01-00-00".into()))]),
                        )?
                        .with_ingestion_time(1_704_067_200_000)?;
                    let batch = RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from(vec![id, id])),
                            Arc::new(StringArray::from(vec![Some("a|b"), None])),
                            Arc::new(StringArray::from(vec!["2024-01-01-00-00"; 2])),
                        ],
                    )?;
                    writer.write(&batch).await?;
                    writer.finish().await?;
                }
            }
        }
        run(arguments.clone()).await?;
        let receipt: Value = read_json(&root.path().join(ARTIFACT))?;
        assert_eq!(receipt["run_id"], "run-1");
        assert_eq!(receipt["passed"], true);
        assert_eq!(receipt["skipped"], false);
        assert_eq!(receipt["passes"].as_array().context("passes")?.len(), 2);
        assert_eq!(
            receipt["passes"][0]["reports"].as_object().context("reports")?.len(),
            26
        );
        assert!(
            receipt["passes"][1]["reports"]
                .as_object()
                .context("final reports")?
                .is_empty()
        );
        for (pool, path) in [
            ("direct", &arguments.direct_root),
            ("dataflow", &arguments.dataflow_root),
        ] {
            // Existing independent test oracle only; production never calls delta-rs.
            let independent = delta_inspect::inventory(path).await?;
            assert_eq!(
                receipt["pools"][pool]["validation_inventory"],
                serde_json::to_value(independent)?
            );
            let tables = receipt["pools"][pool]["tables"].as_object().context("tables")?;
            assert_eq!(tables.len(), 13);
            for evidence in tables.values() {
                assert_eq!(evidence["rows"], 4);
                assert_eq!(
                    evidence["version_after"].as_u64(),
                    evidence["version_before"].as_u64().map(|version| version + 1)
                );
            }
        }
        Ok(())
    }
}
