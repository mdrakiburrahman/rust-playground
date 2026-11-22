#![cfg(all(feature = "delta", target_os = "linux"))]

use anyhow::{Context, Result, ensure};
use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::commit::{BatchedTable, CommitOptions};
use delta_kernel::Snapshot;
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel_default_engine::DefaultEngineBuilder;
use futures::FutureExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use url::Url;

const GROUPS: usize = 12;
const ROWS: usize = 16_384;

fn input(group: usize) -> Result<RecordBatch> {
    let start = i64::try_from(group * ROWS)?;
    let ids = Int64Array::from_iter_values(start..start + i64::try_from(ROWS)?);
    let payloads = StringArray::from_iter_values((0..ROWS).map(|row| format!("value-{group:04}-{row:08}")));
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ])),
        vec![Arc::new(ids), Arc::new(payloads)],
    )?)
}

fn process_ticks() -> Result<u64> {
    let stat = fs::read_to_string("/proc/self/stat")?;
    let fields = stat
        .rsplit_once(')')
        .context("malformed process stat")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let user = fields.get(11).context("missing user ticks")?.parse::<u64>()?;
    let system = fields.get(12).context("missing system ticks")?.parse::<u64>()?;
    Ok(user + system)
}

fn ticks_per_second() -> Result<u64> {
    let output = std::process::Command::new("getconf").arg("CLK_TCK").output()?;
    ensure!(output.status.success(), "getconf CLK_TCK failed");
    let value = std::str::from_utf8(&output.stdout)?.trim().parse::<u64>()?;
    ensure!(value > 0, "invalid CLK_TCK");
    Ok(value)
}

fn verify_snapshot(root: &Path, expected: &BTreeMap<PathBuf, u64>) -> Result<(usize, u64)> {
    let url = Url::from_directory_path(fs::canonicalize(root)?).map_err(|()| anyhow::anyhow!("table URL"))?;
    let engine = DefaultEngineBuilder::new(Arc::new(object_store::local::LocalFileSystem::new())).build();
    let snapshot = Snapshot::builder_for(url.as_str()).build(&engine)?;
    ensure!(
        snapshot.version() == u64::try_from(expected.len())?,
        "unexpected snapshot version"
    );
    let scan = snapshot.scan_builder().build()?;
    let mut paths = BTreeMap::new();
    for data in scan.scan_metadata(&engine)? {
        let batch = data?.scan_files.apply_selection_vector()?.try_into_record_batch()?;
        let names = batch
            .column_by_name("path")
            .context("scan path")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("scan path type")?;
        let sizes = batch
            .column_by_name("size")
            .context("scan size")?
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("scan size type")?;
        for row in 0..batch.num_rows() {
            ensure!(!names.is_null(row) && !sizes.is_null(row), "null active file metadata");
            let size = u64::try_from(sizes.value(row))?;
            ensure!(
                paths.insert(names.value(row).to_owned(), size).is_none(),
                "duplicate active path"
            );
        }
    }
    ensure!(paths.len() == expected.len(), "wrong active file count");
    let mut ids = BTreeSet::new();
    let mut bytes = 0_u64;
    for (name, size) in paths {
        let path = url
            .join(&name)?
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("active file URL"))?;
        ensure!(expected.get(&path) == Some(&size), "active file differs from receipt");
        ensure!(fs::metadata(&path)?.len() == size, "active file size differs from Add");
        bytes += size;
        let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path)?)?.build()?;
        for batch in reader {
            let batch = batch?;
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("id type")?;
            let payloads = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("payload type")?;
            for row in 0..batch.num_rows() {
                let id = values.value(row);
                let id_usize = usize::try_from(id)?;
                let expected_payload = format!("value-{:04}-{:08}", id_usize / ROWS, id_usize % ROWS);
                ensure!(
                    payloads.value(row) == expected_payload,
                    "durable payload differs from input"
                );
                ensure!(ids.insert(id), "duplicate durable row");
            }
        }
    }
    ensure!(ids.len() == expected.len() * ROWS, "wrong active row count");
    ensure!(
        ids.iter().copied().eq(0..i64::try_from(expected.len() * ROWS)?),
        "wrong durable row identities"
    );
    Ok((ids.len(), bytes))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit local component profile"]
async fn local_commit_pipeline_profile() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = BatchedTable::open(
        root.path(),
        CommitOptions {
            batch_window: Duration::from_secs(60),
            max_files_per_commit: 16,
            max_pending_files: 16,
        },
    )
    .await?;
    let warmup = table
        .stage_coalesced_publications(vec![(input(0)?, "darwin-warmup".into(), 0, 1)])
        .await?;
    let mut barrier_receipt = Box::pin(table.submit_prepared_group(warmup, false).await?.wait());
    ensure!(
        barrier_receipt.as_mut().now_or_never().is_none(),
        "receipt preceded barrier"
    );
    ensure!(
        !root.path().join("_delta_log/00000000000000000001.json").exists(),
        "commit preceded barrier"
    );
    table.flush_pending();
    let warmup_result = barrier_receipt.await?;
    ensure!(warmup_result.version == 1, "barrier did not publish warmup");
    ensure!(warmup_result.files.len() == 1, "wrong warmup receipt shape");
    let mut expected_files = BTreeMap::new();
    expected_files.insert(warmup_result.files[0].path.clone(), warmup_result.files[0].bytes);

    let batches = (1..=GROUPS).map(input).collect::<Result<Vec<_>>>()?;
    let ticks_hz = ticks_per_second()?;
    let cpu_start = process_ticks()?;
    let wall_start = Instant::now();
    let mut stage_wall = Duration::ZERO;
    let mut build = Duration::ZERO;
    let mut sort = Duration::ZERO;
    let mut encode = Duration::ZERO;
    let mut upload = Duration::ZERO;
    let mut upload_samples = 0_usize;
    let mut admission = Duration::ZERO;
    let mut receipt_wait = Duration::ZERO;
    let mut commit = Duration::ZERO;
    let mut commits_timed = 0_usize;
    let mut receipts = BTreeSet::new();
    let mut identities = Vec::new();
    for (index, batch) in batches.into_iter().enumerate() {
        let id = format!("darwin-group-{index:04}");
        identities.push((id.clone(), 0));
        let started = Instant::now();
        let prepared = table
            .stage_coalesced_publications(vec![(batch, id.clone(), 0, 1)])
            .await?;
        stage_wall += started.elapsed();
        build += prepared.preparation_build;
        sort += prepared.preparation_sort;
        ensure!(
            prepared.identities.len() == 1 && prepared.committed_identities.is_empty(),
            "unexpected replay"
        );
        ensure!(prepared.appends.len() == 1, "unexpected file formation");
        for file in &prepared.appends {
            let timing = file.stage_timings();
            encode += timing.encode;
            if let Some(duration) = timing.upload {
                upload += duration;
                upload_samples += 1;
            }
        }
        let started = Instant::now();
        let receipt = table.submit_prepared_group(prepared, true).await?;
        admission += started.elapsed();
        let started = Instant::now();
        let result = receipt.wait().await?;
        receipt_wait += started.elapsed();
        ensure!(
            result.version == u64::try_from(index + 2)?,
            "out-of-order durable receipt"
        );
        ensure!(
            result.files.len() == 1 && result.file_observations.len() == 1,
            "wrong receipt shape"
        );
        ensure!(result.file_observations[0].rows == ROWS, "wrong receipt row count");
        ensure!(
            expected_files
                .insert(result.files[0].path.clone(), result.files[0].bytes)
                .is_none(),
            "duplicate file in receipts"
        );
        ensure!(receipts.insert(id), "duplicate receipt identity");
        if let Some(duration) = result.commit_duration {
            commit += duration;
            commits_timed += 1;
        }
    }
    let wall = wall_start.elapsed();
    let cpu_ticks = process_ticks()?
        .checked_sub(cpu_start)
        .context("process ticks went backwards")?;
    ensure!(receipts.len() == GROUPS, "missing receipt");
    ensure!(
        table
            .lookup_publications(identities)
            .await?
            .into_iter()
            .all(|committed| committed),
        "missing durable transaction"
    );
    drop(table);
    let (active_rows, active_bytes) = verify_snapshot(root.path(), &expected_files)?;
    let root_url =
        Url::from_directory_path(fs::canonicalize(root.path())?).map_err(|()| anyhow::anyhow!("table URL"))?;
    for version in 1..=GROUPS + 1 {
        let log = fs::read_to_string(root.path().join(format!("_delta_log/{version:020}.json")))?;
        let actions = log
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        ensure!(
            actions.iter().filter(|action| action.get("add").is_some()).count() == 1,
            "wrong committed Add count"
        );
        ensure!(
            actions.iter().filter(|action| action.get("txn").is_some()).count() == 1,
            "wrong transaction count"
        );
        let txn = actions
            .iter()
            .find_map(|action| action.get("txn"))
            .context("missing transaction")?;
        let expected_id = if version == 1 {
            "darwin-warmup".to_owned()
        } else {
            format!("darwin-group-{:04}", version - 2)
        };
        ensure!(
            txn["appId"] == expected_id && txn["version"] == 0,
            "wrong transaction identity"
        );
        let add = actions
            .iter()
            .find_map(|action| action.get("add"))
            .context("missing Add")?;
        let path = root_url
            .join(add["path"].as_str().context("missing Add path")?)?
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("Add URL"))?;
        let bytes = add["size"].as_u64().context("missing Add size")?;
        ensure!(
            expected_files.remove(&path) == Some(bytes),
            "Add does not match a unique receipt"
        );
    }
    ensure!(expected_files.is_empty(), "receipt missing from committed Adds");
    println!(
        "backend=local_filesystem timed_groups={GROUPS} timed_durable_rows={} receipt_count={} wall_s={:.6} process_cpu_s={:.6} stage_wall_s={:.6} preparation_build_s={:.6} preparation_sort_s={:.6} encode_s={:.6} upload_samples={} upload_s={:.6} admission_s={:.6} receipt_wait_s={:.6} commit_samples={} commit_s={:.6} active_files={} active_rows={} active_bytes={} warmup_rows={} barrier=passed",
        GROUPS * ROWS,
        receipts.len(),
        wall.as_secs_f64(),
        cpu_ticks as f64 / ticks_hz as f64,
        stage_wall.as_secs_f64(),
        build.as_secs_f64(),
        sort.as_secs_f64(),
        encode.as_secs_f64(),
        upload_samples,
        upload.as_secs_f64(),
        admission.as_secs_f64(),
        receipt_wait.as_secs_f64(),
        commits_timed,
        commit.as_secs_f64(),
        GROUPS + 1,
        active_rows,
        active_bytes,
        ROWS,
    );
    Ok(())
}
