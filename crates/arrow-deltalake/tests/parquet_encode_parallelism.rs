#![cfg(all(target_os = "linux", feature = "delta"))]

use anyhow::{Context, Result, ensure};
use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_deltalake::ParquetFileWriter;
use futures::future::try_join_all;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;

const STRING_COLUMNS: usize = 17;

fn env_usize(name: &str, default: usize) -> Result<usize> {
    match std::env::var(name) {
        Ok(value) => value.parse().with_context(|| format!("{name} must be an integer")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn schema() -> Arc<Schema> {
    let mut fields: Vec<Field> = (0..STRING_COLUMNS)
        .map(|column| Field::new(format!("s{column}"), DataType::Utf8, true))
        .collect();
    fields.push(Field::new("i0", DataType::Int32, true));
    fields.extend((0..4).map(|column| {
        Field::new(
            format!("t{column}"),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )
    }));
    Arc::new(Schema::new(fields))
}

fn mix(value: u64) -> u64 {
    let mut x = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn text(seed: u64, width: usize) -> String {
    let mut out = String::with_capacity(width + 16);
    let mut state = seed;
    while out.len() < width {
        state = mix(state);
        out.push_str(&format!("{state:016x}"));
    }
    out.truncate(width);
    out
}

/// `random` varies every row; `loadgen` repeats one value per batch like the standalone load generator.
fn batch(schema: &Arc<Schema>, index: usize, rows: usize, width: usize, random: bool) -> Result<RecordBatch> {
    let base = (index as u64) << 32;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for column in 0..STRING_COLUMNS {
        let array = if random {
            let cardinality = match column % 3 {
                0 => 1024,
                1 => u64::MAX,
                _ => 64,
            };
            StringArray::from_iter_values((0..rows).map(|row| {
                let key = (base + row as u64) % cardinality;
                text(key ^ ((column as u64) << 56), width)
            }))
        } else {
            let value = text(base ^ ((column as u64) << 56), width);
            StringArray::from_iter_values(std::iter::repeat_n(value.as_str(), rows))
        };
        columns.push(Arc::new(array));
    }
    columns.push(Arc::new(Int32Array::from_iter_values(
        (0..rows).map(|row| (mix(base + row as u64) % 100_000) as i32),
    )));
    for column in 0..4 {
        columns.push(Arc::new(TimestampMicrosecondArray::from_iter_values((0..rows).map(
            |row| 1_700_000_000_000_000 + ((base as i64 >> 8) + row as i64) * 1_000 + column,
        ))));
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

fn corpus(batches: usize, rows: usize, width: usize, random: bool) -> Result<Vec<RecordBatch>> {
    let schema = schema();
    let threads = std::thread::available_parallelism()?.get().min(batches.max(1));
    let mut slots: Vec<Option<RecordBatch>> = (0..batches).map(|_| None).collect();
    std::thread::scope(|scope| -> Result<()> {
        let handles: Vec<_> = slots
            .chunks_mut(batches.div_ceil(threads))
            .enumerate()
            .map(|(chunk, slots)| {
                let schema = Arc::clone(&schema);
                let first = chunk * batches.div_ceil(threads);
                scope.spawn(move || -> Result<()> {
                    for (offset, slot) in slots.iter_mut().enumerate() {
                        *slot = Some(batch(&schema, first + offset, rows, width, random)?);
                    }
                    Ok(())
                })
            })
            .collect();
        for handle in handles {
            handle.join().map_err(|_| anyhow::anyhow!("corpus thread panicked"))??;
        }
        Ok(())
    })?;
    slots
        .into_iter()
        .map(|slot| slot.context("missing corpus batch"))
        .collect()
}

fn process_usage() -> Result<(f64, f64, u64, u64, u64)> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let fields: Vec<_> = stat
        .rsplit_once(") ")
        .context("invalid /proc/self/stat")?
        .1
        .split_whitespace()
        .collect();
    let user: u64 = fields.get(11).context("missing user ticks")?.parse()?;
    let system: u64 = fields.get(12).context("missing system ticks")?.parse()?;
    let minor_faults: u64 = fields.get(7).context("missing minor faults")?.parse()?;
    let major_faults: u64 = fields.get(9).context("missing major faults")?.parse()?;
    let io = std::fs::read_to_string("/proc/self/io")?;
    let write_bytes = io
        .lines()
        .find_map(|line| line.strip_prefix("write_bytes:"))
        .context("missing write_bytes")?
        .trim()
        .parse()?;
    let result = std::process::Command::new("getconf").arg("CLK_TCK").output()?;
    let ticks: f64 = std::str::from_utf8(&result.stdout)?.trim().parse()?;
    Ok((
        user as f64 / ticks,
        system as f64 / ticks,
        minor_faults,
        major_faults,
        write_bytes,
    ))
}

fn peak_rss_kib() -> Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .context("missing VmHWM")?;
    Ok(value.split_whitespace().next().context("missing peak RSS")?.parse()?)
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual release-mode component benchmark; configure with BENCH_* and ARROWCOL_COLLECTOR_* variables"]
async fn parquet_encode_parallelism() -> Result<()> {
    let rows = env_usize("BENCH_ROWS", 1_048_576)?;
    let batch_rows = env_usize("BENCH_BATCH_ROWS", 8_192)?;
    let row_group_rows = env_usize("BENCH_ROW_GROUP_ROWS", 1_048_576)?;
    let files = env_usize("BENCH_FILES", 1)?;
    let width = env_usize("BENCH_STRING_BYTES", 28)?;
    let zstd_level = env_usize("BENCH_ZSTD_LEVEL", 1)?;
    let random = match std::env::var("BENCH_DATA").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("random") => true,
        Ok("loadgen") => false,
        _ => anyhow::bail!("BENCH_DATA must be random or loadgen"),
    };
    ensure!(
        files > 0
            && rows > 0
            && batch_rows > 0
            && batch_rows
                .checked_mul(files)
                .is_some_and(|divisor| rows.is_multiple_of(divisor)),
        "rows must divide into whole batches per file"
    );

    let batches = corpus(rows / batch_rows, batch_rows, width, random)?;
    let schema = batches[0].schema();
    let per_file = batches.len() / files;
    let dir = match std::env::var_os("BENCH_DIR") {
        Some(base) => tempfile::tempdir_in(base)?,
        None => tempfile::tempdir()?,
    };
    let properties = || -> Result<WriterProperties> {
        Ok(WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(i32::try_from(zstd_level)?)?))
            .set_max_row_group_row_count(Some(row_group_rows))
            .build())
    };

    let cpu_started = process_usage()?;
    let started = Instant::now();
    let tasks = batches.chunks(per_file).enumerate().map(|(index, chunk)| {
        let path = dir.path().join(format!("file-{index}.parquet"));
        let schema = Arc::clone(&schema);
        let properties = properties();
        let chunk = chunk.to_vec();
        async move {
            let mut writer = ParquetFileWriter::create_with_codec_evidence(&path, schema, Some(properties?))?;
            for batch in &chunk {
                writer.write(batch).await?;
            }
            let encoded = started.elapsed();
            let completed = writer.finish().await?;
            Ok::<_, anyhow::Error>((completed, encoded, started.elapsed() - encoded))
        }
    });
    let results = try_join_all(tasks.map(tokio::spawn)).await?;
    let wall = started.elapsed().as_secs_f64();
    let cpu_finished = process_usage()?;
    let user = cpu_finished.0 - cpu_started.0;
    let system = cpu_finished.1 - cpu_started.1;
    let cpu = user + system;

    let mut total_rows = 0_usize;
    let mut input_bytes = 0_u64;
    let mut parquet_bytes = 0_u64;
    let mut finish_s = 0.0_f64;
    let mut encode_s = 0.0_f64;
    for result in results {
        let (completed, encoded, finish) = result?;
        total_rows += completed.rows;
        input_bytes += completed.input_bytes;
        parquet_bytes += completed.bytes;
        finish_s = finish_s.max(finish.as_secs_f64());
        encode_s = encode_s.max(encoded.as_secs_f64());
    }
    ensure!(total_rows == rows, "row count mismatch");
    println!(
        "BW_ENCODE_RESULT={}",
        json!({
            "data": if random { "random" } else { "loadgen" },
            "rows": total_rows,
            "files": files,
            "batch_rows": batch_rows,
            "row_group_rows": row_group_rows,
            "string_bytes": width,
            "zstd_level": zstd_level,
            "column_workers": std::env::var("ARROWCOL_COLLECTOR_PARQUET_COLUMN_WORKERS").ok(),
            "cpu_workers": std::env::var("ARROWCOL_COLLECTOR_CPU_WORKERS").ok(),
            "encode_mode": arrow_deltalake::runtime_config()?.parquet_encode_mode.as_str(),
            "allocator": "system",
            "glibc_tunables": std::env::var("GLIBC_TUNABLES").ok(),
            "wall_s": wall,
            "rows_per_s": total_rows as f64 / wall,
            "encode_s": encode_s,
            "encode_rows_per_s": total_rows as f64 / encode_s,
            "arrow_bytes_per_row": input_bytes as f64 / total_rows as f64,
            "arrow_mb_per_s": input_bytes as f64 / wall / 1e6,
            "parquet_bytes_per_row": parquet_bytes as f64 / total_rows as f64,
            "cpu_s": cpu,
            "user_cpu_s": user,
            "system_cpu_s": system,
            "minor_faults": cpu_finished.2 - cpu_started.2,
            "major_faults": cpu_finished.3 - cpu_started.3,
            "write_bytes": cpu_finished.4 - cpu_started.4,
            "rss_hwm_kib": peak_rss_kib()?,
            "cpu_s_per_mrow": cpu / total_rows as f64 * 1e6,
            "cores_busy": cpu / wall,
            "slowest_finish_s": finish_s,
        })
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual release-mode L5 benchmark; configure with BENCH_* and ARROWCOL_* variables"]
async fn parquet_commit_parallelism() -> Result<()> {
    use arrow_deltalake::{
        commit::{BatchedTable, CommitOptions},
        delta::DeltaTable,
    };
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;

    fn latest(log: &std::path::Path) -> Result<u64> {
        std::fs::read_dir(log)?.try_fold(0_u64, |maximum, entry| {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "json")
                && let Some(stem) = path.file_stem().and_then(|stem| stem.to_str())
                && stem.len() == 20
                && stem.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Ok(maximum.max(stem.parse()?));
            }
            Ok(maximum)
        })
    }

    let rows = env_usize("BENCH_ROWS", 131_072)?;
    let batch_rows = env_usize("BENCH_BATCH_ROWS", 8_192)?;
    let files = env_usize("BENCH_FILES", 1)?;
    let concurrency = env_usize("BENCH_STAGE_CONCURRENCY", 1)?;
    let groups = env_usize("BENCH_GROUPS", 1)?;
    let seconds = env_usize("BENCH_SECONDS", 15)?;
    let width = env_usize("BENCH_STRING_BYTES", 150)?;
    let random = match std::env::var("BENCH_DATA").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("random") => true,
        Ok("loadgen") => false,
        _ => anyhow::bail!("BENCH_DATA must be random or loadgen"),
    };
    ensure!(
        rows > 0 && batch_rows > 0 && rows.is_multiple_of(batch_rows),
        "rows must divide into whole batches"
    );
    ensure!(
        (1..=16).contains(&files) && (1..=16).contains(&concurrency),
        "file widths must be in 1..=16"
    );
    ensure!(
        (1..=16).contains(&groups) && (15..=600).contains(&seconds),
        "groups must be 1..=16 and seconds 15..=600"
    );
    let batches = corpus(rows / batch_rows, batch_rows, width, random)?;
    let schema = batches[0].schema();
    let dir = match std::env::var_os("BENCH_DIR") {
        Some(base) => tempfile::tempdir_in(base)?,
        None => tempfile::tempdir()?,
    };
    let root = dir.path().canonicalize()?;
    let url = url::Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("invalid benchmark path"))?;
    DeltaTable::open_catalog(url)?.commit_catalog(None, schema, Vec::new(), Default::default(), None)?;
    let table = BatchedTable::open(&root, CommitOptions::default()).await?;
    let input_bytes = batches.iter().try_fold(0_u64, |total, batch| {
        Ok::<_, anyhow::Error>(total + arrow_deltalake::record_batch_logical_bytes(batch)?)
    })?;
    let stop = AtomicBool::new(false);
    let offered = AtomicU64::new(0);
    let clients = try_join_all((0..groups).map(|producer| {
        let batches = &batches;
        let table = &table;
        let stop = &stop;
        let offered = &offered;
        async move {
            let mut iteration = 0_u64;
            while !stop.load(Ordering::Relaxed) {
                let entries = batches
                    .iter()
                    .enumerate()
                    .map(|(index, batch)| {
                        (
                            batch.clone(),
                            format!("bench-{producer}-{iteration}-{index}"),
                            0,
                            1_700_000_000_000,
                        )
                    })
                    .collect();
                offered.fetch_add(rows as u64, Ordering::Relaxed);
                let group = table
                    .stage_coalesced_publications_with_file_policy(entries, concurrency, files)
                    .await?;
                let result = table.submit_prepared_group(group, true).await?.wait().await?;
                ensure!(
                    result.files.iter().map(|file| file.rows).sum::<usize>() == rows,
                    "receipt row count mismatch"
                );
                iteration += 1;
            }
            Ok::<_, anyhow::Error>(iteration * rows as u64)
        }
    }));
    tokio::pin!(clients);
    let log_dir = root.join("_delta_log");
    let sample = async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let before = latest(&log_dir)?;
        ensure!(before > 0, "warmup did not produce a durable commit");
        let usage_started = process_usage()?;
        let started = Instant::now();
        tokio::time::sleep(Duration::from_secs(seconds as u64)).await;
        let after = latest(&log_dir)?;
        let wall_s = started.elapsed().as_secs_f64();
        let usage_finished = process_usage()?;
        Ok::<_, anyhow::Error>((before, after, wall_s, usage_started, usage_finished))
    };
    let (before, after, wall_s, usage_started, usage_finished) = tokio::select! {
        outcome = &mut clients => anyhow::bail!("traffic ended during sample: {outcome:?}"),
        result = sample => result?,
    };
    stop.store(true, Ordering::Relaxed);
    let acked_rows: u64 = tokio::time::timeout(Duration::from_secs(120), clients)
        .await??
        .into_iter()
        .sum();
    let final_version = latest(&log_dir)?;
    let mut committed_rows = 0_u64;
    let mut all_rows = 0_u64;
    let mut parquet_bytes = 0_u64;
    let mut committed_files = 0_u64;
    let mut paths = std::collections::HashSet::new();
    for version in 0..=final_version {
        let log = std::fs::read_to_string(log_dir.join(format!("{version:020}.json")))?;
        for line in log.lines() {
            let action: serde_json::Value = serde_json::from_str(line)?;
            ensure!(action.get("remove").is_none(), "measurement table must be append-only");
            if let Some(add) = action.get("add") {
                ensure!(
                    paths.insert(add["path"].as_str().context("missing add path")?.to_owned()),
                    "duplicate add path"
                );
                let stats: serde_json::Value =
                    serde_json::from_str(add["stats"].as_str().context("missing add statistics")?)?;
                let records = stats["numRecords"].as_u64().context("missing committed row count")?;
                all_rows += records;
                if before < version && version <= after {
                    committed_rows += records;
                    parquet_bytes += add["size"].as_u64().context("missing add size")?;
                    committed_files += 1;
                }
            }
        }
    }
    ensure!(
        all_rows == acked_rows && all_rows == offered.load(Ordering::Relaxed),
        "Delta log, offered and drained receipt rows disagree"
    );
    ensure!(committed_rows > 0, "no durable commits in measurement window");
    let user_s = usage_finished.0 - usage_started.0;
    let system_s = usage_finished.1 - usage_started.1;
    println!(
        "BW_COMMIT_RESULT={}",
        json!({
            "level":"L5_local_fsync", "rows":committed_rows, "files":committed_files,
            "stage_file_concurrency":concurrency, "requested_files":files,
            "groups":groups, "rows_per_group":rows, "batch_rows":batch_rows, "data":if random {"random"} else {"loadgen"},
            "string_bytes":width, "warmup_s":5, "wall_s":wall_s,
            "start_version_exclusive":before, "end_version_inclusive":after,
            "all_durable_rows":all_rows,"drained_acked_rows":acked_rows,
            "committed_rows_per_s":committed_rows as f64 / wall_s,
            "arrow_bytes_per_row":input_bytes as f64 / rows as f64,
            "parquet_bytes_per_row":parquet_bytes as f64 / committed_rows as f64,
            "user_cpu_s":user_s, "system_cpu_s":system_s,
            "minor_faults":usage_finished.2-usage_started.2,
            "major_faults":usage_finished.3-usage_started.3,
            "write_bytes":usage_finished.4-usage_started.4,
            "write_mb_per_s":(usage_finished.4-usage_started.4) as f64 / wall_s / 1e6,
            "rss_hwm_kib":peak_rss_kib()?,
            "cpu_s_per_mrow":(user_s+system_s) / committed_rows as f64 * 1e6,
            "cores_busy":(user_s+system_s) / wall_s,
            "allocator":"system", "glibc_tunables":std::env::var("GLIBC_TUNABLES").ok(),
            "runtime":arrow_deltalake::runtime_config()?,
            "parquet_config":std::env::var("ARROWCOL_PARQUET_CONFIG").ok(),
        })
    );
    Ok(())
}
