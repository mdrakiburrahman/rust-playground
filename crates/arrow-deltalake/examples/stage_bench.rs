//! Isolated coalesced stage/commit benchmark for the collector write path.
//!
//! Environment: `BENCH_BATCH_ROWS`, `BENCH_BATCHES_PER_GROUP`, `BENCH_GROUPS`,
//! `BENCH_CONCURRENCY`, `BENCH_STRING_WIDTH`, `BENCH_VARY` (row-varying strings),
//! `BENCH_COMMIT` (also commit each group) and `BENCH_DIR` (table root).

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_deltalake::commit::{BatchedTable, CommitOptions};
use arrow_deltalake::delta::{DeltaTable, INGESTION_PARTITION};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use url::Url;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn schema() -> Arc<Schema> {
    let utc = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let text = |name: &str| Field::new(name, DataType::Utf8, false);
    Arc::new(Schema::new(vec![
        text("customer_id"),
        Field::new("sample_time_utc", utc(), false),
        text("sql_server_instance_name"),
        text("machine_name"),
        text("availability_group_id"),
        text("availability_group_name"),
        text("replica_server_name"),
        text("replica_role"),
        text("operational_state_desc"),
        text("recovery_health_desc"),
        text("synchronization_health_desc"),
        text("connected_state_desc"),
        Field::new("last_connect_error_number", DataType::Int32, false),
        Field::new("last_connect_error_timestamp", utc(), false),
        text("endpoint_url"),
        text("availability_mode_desc"),
        text("failover_mode_desc"),
        text("primary_role_allow_connections_desc"),
        text("secondary_role_allow_connections_desc"),
        Field::new("availability_replica_create_date", utc(), false),
        Field::new("availability_replica_modify_date", utc(), false),
        text("seeding_mode_desc"),
        Field::new("is_primary", DataType::Boolean, false),
        Field::new("shard", DataType::Int16, false),
    ]))
}

fn string_column(name: &str, rows: usize, seed: u64, width: usize, vary: bool) -> ArrayRef {
    if name == "customer_id" {
        return Arc::new(StringArray::from_iter_values(
            (0..rows).map(|row| format!("customer-{:04}", row % 4096)),
        ));
    }
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ name
            .bytes()
            .fold(0x1234_5678_u64, |acc, byte| acc.wrapping_mul(131) ^ u64::from(byte));
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    if vary {
        Arc::new(StringArray::from_iter_values((0..rows).map(|_| {
            (0..width)
                .map(|_| char::from(b'a' + (next() % 26) as u8))
                .collect::<String>()
        })))
    } else {
        let value: String = (0..width).map(|_| char::from(b'a' + (next() % 26) as u8)).collect();
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(value.as_str(), rows)))
    }
}

fn batch(schema: &Arc<Schema>, rows: usize, seed: u64, width: usize, vary: bool) -> Result<RecordBatch> {
    let base = i64::try_from(seed)? * 1_000_000 + 1_700_000_000_000_000;
    let columns = schema
        .fields()
        .iter()
        .map(|field| -> ArrayRef {
            match field.data_type() {
                DataType::Utf8 => string_column(field.name(), rows, seed, width, vary),
                DataType::Timestamp(..) => Arc::new(
                    TimestampMicrosecondArray::from_iter_values((0..rows).map(|row| base + row as i64))
                        .with_timezone("UTC"),
                ),
                DataType::Int32 => Arc::new(Int32Array::from_iter_values(
                    (0..rows).map(|row| row as i32 ^ seed as i32),
                )),
                DataType::Int16 => Arc::new(Int16Array::from(vec![(seed % 1_000) as i16; rows])),
                DataType::Boolean => Arc::new(BooleanArray::from(vec![seed.is_multiple_of(2); rows])),
                other => unreachable!("unsupported benchmark type {other}"),
            }
        })
        .collect();
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

/// User seconds, system seconds and minor page faults of this process so far.
fn process_usage() -> Result<(f64, f64, f64)> {
    let stat = std::fs::read_to_string("/proc/self/stat").context("read /proc/self/stat")?;
    let comm_end = stat.rfind(')').context("stat comm")?;
    let fields: Vec<&str> = stat[comm_end + 2..].split_whitespace().collect();
    let field = |index: usize| -> Result<f64> { Ok(fields.get(index).context("stat field")?.parse::<f64>()?) };
    Ok((field(11)? / 100.0, field(12)? / 100.0, field(7)?))
}

#[tokio::main]
async fn main() -> Result<()> {
    let batch_rows = env_usize("BENCH_BATCH_ROWS", 4096);
    let batches = env_usize("BENCH_BATCHES_PER_GROUP", 128);
    let groups = env_usize("BENCH_GROUPS", 8);
    let concurrency = env_usize("BENCH_CONCURRENCY", 1);
    let width = env_usize("BENCH_STRING_WIDTH", 150);
    let vary = env_usize("BENCH_VARY", 0) != 0;
    let commit = env_usize("BENCH_COMMIT", 1) != 0;

    let directory = match std::env::var("BENCH_DIR") {
        Ok(path) => std::path::PathBuf::from(path),
        Err(_) => tempfile::tempdir()?.keep(),
    };
    std::fs::create_dir_all(&directory)?;
    let root = directory.canonicalize()?;
    let url = Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("bad table path"))?;
    let schema = schema();
    let mut table_schema = schema.fields().to_vec();
    table_schema.push(Arc::new(Field::new(INGESTION_PARTITION, DataType::Utf8, false)));
    let partitions = vec![INGESTION_PARTITION.to_owned()];
    let properties = BTreeMap::from([("arrowcol.partition.granularity".to_owned(), "hour".to_owned())]);
    DeltaTable::open_catalog(url)?.commit_catalog(
        None,
        Arc::new(Schema::new(table_schema)),
        partitions,
        properties,
        None,
    )?;
    let table = BatchedTable::open(
        &root,
        CommitOptions {
            batch_window: Duration::from_millis(50),
            max_files_per_commit: 128,
            max_pending_files: 512,
            ..CommitOptions::default()
        },
    )
    .await?;

    let inputs = (0..batches.min(32))
        .map(|index| batch(&schema, batch_rows, index as u64 + 1, width, vary))
        .collect::<Result<Vec<_>>>()?;
    let arrow_bytes: usize = inputs.iter().map(RecordBatch::get_array_memory_size).sum::<usize>() / inputs.len();
    let rows_per_group = batch_rows * batches;
    println!(
        "config rows/batch={batch_rows} batches/group={batches} groups={groups} concurrency={concurrency} \
         width={width} vary={vary} commit={commit} arrow_bytes/row={}",
        arrow_bytes / batch_rows
    );

    let mut stamp_ms = 1_700_000_000_000_i64;
    let mut build = Duration::ZERO;
    let mut encode = Duration::ZERO;
    let mut files = 0_usize;
    let (user_start, sys_start, faults_start) = process_usage()?;
    let started = Instant::now();
    for group in 0..groups {
        let entries = (0..batches)
            .map(|index| {
                (
                    inputs[index % inputs.len()].clone(),
                    format!("bench.{group}.{index}"),
                    0_i64,
                    stamp_ms,
                )
            })
            .collect::<Vec<_>>();
        stamp_ms += 1_000;
        let prepared = table
            .stage_coalesced_publications_with_concurrency(entries, concurrency)
            .await?;
        build += prepared.preparation_build;
        for append in &prepared.appends {
            encode += append.stage_timings().encode;
            files += 1;
        }
        if commit {
            table
                .submit_prepared_group(prepared, true)
                .await?
                .wait()
                .await
                .context("commit group")?;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let (user_end, sys_end, faults_end) = process_usage()?;
    let (user, sys) = (user_end - user_start, sys_end - sys_start);
    let cpu = user + sys;
    let rows = (rows_per_group * groups) as f64;
    println!(
        "rows={rows} files={files} wall={elapsed:.2}s rows/s={:.0} cpu={cpu:.2}s cpu_s/Mrow={:.3} \
         user={user:.2}s sys={sys:.2}s minflt={:.0} plan_build={:.2}s file_encode_sum={:.2}s",
        rows / elapsed,
        cpu / rows * 1e6,
        faults_end - faults_start,
        build.as_secs_f64(),
        encode.as_secs_f64(),
    );
    Ok(())
}
