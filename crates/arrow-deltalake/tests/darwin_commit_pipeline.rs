#![cfg(all(feature = "delta", target_os = "linux"))]
//! Local component profile: `cargo test --release -p arrow-deltalake --test
//! darwin_commit_pipeline -- --ignored --nocapture`.
//!
//! The serial control forces every group. The separate window screen stages fixed
//! waves of eight groups, then submits them non-forced and waits for natural
//! cohorts. Input construction, warmup, replay checks, and independent
//! snapshot/Parquet verification are outside each timed interval. These are not
//! DFE acknowledgments, exporter, or Azure network benchmarks. Encode is part of
//! staging, and commit service is part of receipt wait; timings must not be added.
//! Local storage does not exercise upload.

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

fn verify_snapshot(
    root: &Path,
    expected: &BTreeMap<PathBuf, u64>,
    expected_groups: &BTreeMap<PathBuf, usize>,
    expected_version: u64,
) -> Result<(usize, u64)> {
    let url = Url::from_directory_path(fs::canonicalize(root)?).map_err(|()| anyhow::anyhow!("table URL"))?;
    let engine = DefaultEngineBuilder::new(Arc::new(object_store::local::LocalFileSystem::new())).build();
    let snapshot = Snapshot::builder_for(url.as_str()).build(&engine)?;
    ensure!(snapshot.version() == expected_version, "unexpected snapshot version");
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
        let expected_group = *expected_groups.get(&path).context("active file lacks source group")?;
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
                ensure!(
                    id_usize / ROWS == expected_group,
                    "receipt file has another group's rows"
                );
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
    let mut expected_groups = BTreeMap::from([(warmup_result.files[0].path.clone(), 0)]);

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
        expected_groups.insert(result.files[0].path.clone(), index + 1);
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
    let (active_rows, active_bytes) = verify_snapshot(
        root.path(),
        &expected_files,
        &expected_groups,
        u64::try_from(GROUPS + 1)?,
    )?;
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
    let upload_seconds = if upload_samples == 0 {
        "unavailable".to_owned()
    } else {
        format!("{:.6}", upload.as_secs_f64())
    };
    println!(
        "backend=local_filesystem timed_groups={GROUPS} timed_durable_rows={} receipt_count={} durable_rows_per_s={:.3} receipts_per_s={:.3} wall_s={:.6} process_cpu_s={:.6} stage_wall_s={:.6} preparation_build_s={:.6} preparation_sort_s={:.6} encode_s={:.6} upload_samples={} upload_s={} admission_s={:.6} receipt_wait_s={:.6} commit_samples={} commit_s={:.6} active_files={} active_rows={} active_bytes={} warmup_rows={} barrier=passed",
        GROUPS * ROWS,
        receipts.len(),
        (GROUPS * ROWS) as f64 / wall.as_secs_f64(),
        receipts.len() as f64 / wall.as_secs_f64(),
        wall.as_secs_f64(),
        cpu_ticks as f64 / ticks_hz as f64,
        stage_wall.as_secs_f64(),
        build.as_secs_f64(),
        sort.as_secs_f64(),
        encode.as_secs_f64(),
        upload_samples,
        upload_seconds,
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

const DEPTH: usize = 8;
const TIMED_WAVES: usize = 4;

struct Wave {
    stage: Duration,
    build: Duration,
    encode: Duration,
    submit: Duration,
    receipts: Vec<(usize, Duration, arrow_deltalake::commit::GroupAppendResult)>,
}

async fn run_natural_wave(table: &BatchedTable, first: usize, batches: &[RecordBatch]) -> Result<Wave> {
    let mut prepared = Vec::with_capacity(DEPTH);
    let mut stage = Duration::ZERO;
    let mut build = Duration::ZERO;
    let mut encode = Duration::ZERO;
    for (offset, batch) in batches.iter().enumerate() {
        let index = first + offset;
        let started = Instant::now();
        let group = table
            .stage_coalesced_publications(vec![(batch.clone(), format!("wave-{index:04}"), 0, 1)])
            .await?;
        stage += started.elapsed();
        build += group.preparation_build;
        ensure!(
            group.identities.len() == 1 && group.committed_identities.is_empty(),
            "unexpected replay"
        );
        ensure!(group.appends.len() == 1, "unexpected file formation");
        for file in &group.appends {
            let timing = file.stage_timings();
            ensure!(timing.upload.is_none(), "local run unexpectedly reports remote upload");
            encode += timing.encode;
        }
        prepared.push(group);
    }
    let mut pending = Vec::with_capacity(DEPTH);
    let submit_started = Instant::now();
    for (offset, group) in prepared.into_iter().enumerate() {
        let admitted = Instant::now();
        let receipt = table.submit_prepared_group(group, false).await?;
        pending.push((first + offset, admitted, Box::pin(receipt.wait())));
    }
    let submit = submit_started.elapsed();
    for (_, _, receipt) in &mut pending {
        ensure!(
            receipt.as_mut().now_or_never().is_none(),
            "wave did not reach eight outstanding groups before a receipt"
        );
    }
    let receipts = futures::future::join_all(pending.into_iter().map(|(index, admitted, receipt)| async move {
        let result = receipt.await?;
        Ok::<_, anyhow::Error>((index, admitted.elapsed(), result))
    }))
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    Ok(Wave {
        stage,
        build,
        encode,
        submit,
        receipts,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "explicit local non-forced window screen"]
async fn local_natural_cohort_screen() -> Result<()> {
    let window_ms = std::env::var("DARWIN_WINDOW_MS")
        .unwrap_or_else(|_| "2000".into())
        .parse::<u64>()?;
    ensure!(window_ms == 2000 || window_ms == 250, "window must be 2000 or 250 ms");
    let root = tempfile::tempdir()?;
    let table = BatchedTable::open(
        root.path(),
        CommitOptions {
            batch_window: Duration::from_millis(window_ms),
            max_files_per_commit: 128,
            max_pending_files: 512,
        },
    )
    .await?;
    let total_groups = DEPTH * (TIMED_WAVES + 1);
    let batches = (0..total_groups).map(input).collect::<Result<Vec<_>>>()?;
    let warmup = run_natural_wave(&table, 0, &batches[..DEPTH]).await?;
    let ticks_hz = ticks_per_second()?;
    let cpu_start = process_ticks()?;
    let wall_start = Instant::now();
    let mut waves = Vec::new();
    for wave in 1..=TIMED_WAVES {
        let first = wave * DEPTH;
        waves.push(run_natural_wave(&table, first, &batches[first..first + DEPTH]).await?);
    }
    let wall = wall_start.elapsed();
    let cpu = process_ticks()?
        .checked_sub(cpu_start)
        .context("process ticks went backwards")?;
    let stage = waves.iter().map(|wave| wave.stage).sum::<Duration>();
    let build = waves.iter().map(|wave| wave.build).sum::<Duration>();
    let encode = waves.iter().map(|wave| wave.encode).sum::<Duration>();
    let submit = waves.iter().map(|wave| wave.submit).sum::<Duration>();
    let mut expected_files = BTreeMap::new();
    let mut expected_groups = BTreeMap::new();
    let mut cohorts: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    let mut cohort_files: BTreeMap<u64, BTreeMap<PathBuf, u64>> = BTreeMap::new();
    let mut cohort_commits: BTreeMap<u64, Duration> = BTreeMap::new();
    let mut observed = BTreeSet::new();
    let mut receipt_wait = Duration::ZERO;
    let mut maximum_receipt_wait = Duration::ZERO;
    let mut previous_wave_version = 0;
    for wave in std::iter::once(&warmup).chain(&waves) {
        let mut wave_versions = BTreeSet::new();
        for (index, latency, result) in &wave.receipts {
            ensure!(observed.insert(*index), "duplicate receipt identity");
            ensure!(
                result.files.len() == 1 && result.file_observations.len() == 1,
                "wrong receipt shape"
            );
            ensure!(result.file_observations[0].rows == ROWS, "wrong receipt rows");
            ensure!(
                expected_files
                    .insert(result.files[0].path.clone(), result.files[0].bytes)
                    .is_none(),
                "duplicate file receipt"
            );
            expected_groups.insert(result.files[0].path.clone(), *index);
            cohort_files
                .entry(result.version)
                .or_default()
                .insert(result.files[0].path.clone(), result.files[0].bytes);
            cohorts.entry(result.version).or_default().push(*index);
            wave_versions.insert(result.version);
            if let Some(duration) = result.commit_duration {
                ensure!(
                    cohort_commits.insert(result.version, duration).is_none(),
                    "duplicate commit duration"
                );
            }
            if *index >= DEPTH {
                receipt_wait += *latency;
                maximum_receipt_wait = maximum_receipt_wait.max(*latency);
            }
        }
        ensure!(!wave_versions.is_empty(), "no durable cohort");
        ensure!(
            wave_versions.first().copied().unwrap_or_default() > previous_wave_version,
            "waves completed out of order"
        );
        previous_wave_version = wave_versions.last().copied().context("missing wave version")?;
    }
    ensure!(observed.len() == total_groups, "missing receipt");
    let last_version = *cohorts.keys().next_back().context("missing cohort version")?;
    ensure!(
        cohorts.len() >= TIMED_WAVES + 1,
        "insufficient independent natural cohorts"
    );
    ensure!(cohort_commits.len() == cohorts.len(), "missing per-commit timing");
    let identities = (0..total_groups)
        .map(|index| (format!("wave-{index:04}"), 0))
        .collect::<Vec<_>>();
    ensure!(
        table
            .lookup_publications(identities.clone())
            .await?
            .into_iter()
            .all(|committed| committed),
        "missing committed identity"
    );
    let replay = table
        .stage_coalesced_publications(vec![(batches[0].clone(), "wave-0000".into(), 0, 1)])
        .await?;
    ensure!(
        replay.identities.is_empty() && replay.committed_identities.len() == 1 && replay.appends.is_empty(),
        "replay was not deduplicated"
    );
    let replay = table.submit_prepared_group(replay, false).await?.wait().await?;
    ensure!(
        replay.version == last_version && replay.files.is_empty(),
        "replay changed durable files or version"
    );
    drop(table);
    let (active_rows, active_bytes) = verify_snapshot(root.path(), &expected_files, &expected_groups, last_version)?;
    let root_url =
        Url::from_directory_path(fs::canonicalize(root.path())?).map_err(|()| anyhow::anyhow!("table URL"))?;
    let mut remaining = expected_files;
    for version in 1..=last_version {
        let log = fs::read_to_string(root.path().join(format!("_delta_log/{version:020}.json")))?;
        let actions = log
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        let expected = cohorts.get(&version).context("unexpected committed version")?;
        let mut txn_ids = BTreeSet::new();
        let mut adds = 0_usize;
        for action in actions {
            if let Some(txn) = action.get("txn") {
                ensure!(txn["version"] == 0, "wrong transaction version");
                ensure!(
                    txn_ids.insert(txn["appId"].as_str().context("missing appId")?.to_owned()),
                    "duplicate txn"
                );
            }
            if let Some(add) = action.get("add") {
                let path = root_url
                    .join(add["path"].as_str().context("missing Add path")?)?
                    .to_file_path()
                    .map_err(|()| anyhow::anyhow!("Add URL"))?;
                let bytes = add["size"].as_u64().context("missing Add size")?;
                ensure!(
                    cohort_files.get(&version).and_then(|files| files.get(&path)) == Some(&bytes),
                    "Add version differs from receipt"
                );
                ensure!(
                    remaining.remove(&path) == Some(bytes),
                    "Add does not match a unique receipt"
                );
                adds += 1;
            }
        }
        let expected_ids = expected
            .iter()
            .map(|index| format!("wave-{index:04}"))
            .collect::<BTreeSet<_>>();
        ensure!(
            txn_ids == expected_ids && adds == expected.len(),
            "committed cohort differs from receipts"
        );
    }
    ensure!(remaining.is_empty(), "receipt missing from committed Adds");
    let engine = DefaultEngineBuilder::new(Arc::new(object_store::local::LocalFileSystem::new())).build();
    let snapshot = Snapshot::builder_for(root_url.as_str()).build(&engine)?;
    ensure!(snapshot.version() == last_version, "replay changed snapshot version");
    for (id, version) in &identities {
        ensure!(
            snapshot.get_app_id_version(id, &engine)? == Some(*version),
            "snapshot lost transaction identity"
        );
    }
    let cohort_shape = cohorts
        .iter()
        .map(|(version, groups)| format!("{version}:{}", groups.len()))
        .collect::<Vec<_>>()
        .join(",");
    let commit_seconds = cohort_commits
        .iter()
        .filter(|(version, _)| cohorts[version].iter().all(|index| *index >= DEPTH))
        .map(|(_, duration)| duration.as_secs_f64())
        .sum::<f64>();
    println!(
        "backend=local_filesystem window_ms={window_ms} forced=false depth={DEPTH} timed_waves={TIMED_WAVES} timed_groups={} timed_durable_rows={} wall_s={:.6} process_cpu_s={:.6} stage_s={:.6} preparation_build_s={:.6} encode_s={:.6} upload=unavailable submit_s={:.6} receipt_wait_sum_s={:.6} receipt_wait_max_s={:.6} commit_service_sum_s={:.6} all_cohort_sizes={} active_files={} active_rows={} active_bytes={} bytes_per_active_row={:.6} replay=deduplicated",
        DEPTH * TIMED_WAVES,
        DEPTH * TIMED_WAVES * ROWS,
        wall.as_secs_f64(),
        cpu as f64 / ticks_hz as f64,
        stage.as_secs_f64(),
        build.as_secs_f64(),
        encode.as_secs_f64(),
        submit.as_secs_f64(),
        receipt_wait.as_secs_f64(),
        maximum_receipt_wait.as_secs_f64(),
        commit_seconds,
        cohort_shape,
        total_groups,
        active_rows,
        active_bytes,
        active_bytes as f64 / active_rows as f64,
    );
    Ok(())
}
