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
    fields.extend(
        (0..4).map(|column| Field::new(format!("t{column}"), DataType::Timestamp(TimeUnit::Microsecond, None), true)),
    );
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
        columns.push(Arc::new(TimestampMicrosecondArray::from_iter_values(
            (0..rows).map(|row| 1_700_000_000_000_000 + ((base as i64 >> 8) + row as i64) * 1_000 + column),
        )));
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
    slots.into_iter().map(|slot| slot.context("missing corpus batch")).collect()
}

fn process_cpu_seconds() -> Result<f64> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let fields: Vec<_> = stat.rsplit_once(") ").context("invalid /proc/self/stat")?.1.split_whitespace().collect();
    let user: u64 = fields.get(11).context("missing user ticks")?.parse()?;
    let system: u64 = fields.get(12).context("missing system ticks")?.parse()?;
    let result = std::process::Command::new("getconf").arg("CLK_TCK").output()?;
    let ticks: f64 = std::str::from_utf8(&result.stdout)?.trim().parse()?;
    Ok((user + system) as f64 / ticks)
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
    let random = std::env::var("BENCH_DATA").map_or(true, |value| value != "loadgen");
    ensure!(
        files > 0 && rows % (batch_rows * files) == 0,
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

    let cpu_started = process_cpu_seconds()?;
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
    let cpu = process_cpu_seconds()? - cpu_started;

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
            "wall_s": wall,
            "rows_per_s": total_rows as f64 / wall,
            "encode_s": encode_s,
            "encode_rows_per_s": total_rows as f64 / encode_s,
            "arrow_bytes_per_row": input_bytes as f64 / total_rows as f64,
            "arrow_mb_per_s": input_bytes as f64 / wall / 1e6,
            "parquet_bytes_per_row": parquet_bytes as f64 / total_rows as f64,
            "cpu_s": cpu,
            "cpu_s_per_mrow": cpu / total_rows as f64 * 1e6,
            "cores_busy": cpu / wall,
            "slowest_finish_s": finish_s,
        })
    );
    Ok(())
}
