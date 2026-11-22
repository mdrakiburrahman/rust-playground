use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use arrow::array::{Array, ArrayRef, Int16Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt32Array};
use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, sort_to_indices, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::util::display::array_value_to_string;
use arrow_deltalake::layout::{FileSortBuffer, LayoutPolicy};
use sha2::{Digest, Sha256};

const BATCHES: usize = 32;
const BATCH_ROWS: usize = 8_192;
const MAX_ROWS: usize = 2_000_000;
const MAX_BYTES: usize = 1_073_741_824;

fn schema(stress: bool) -> SchemaRef {
    let fields = vec![
        Field::new("customer_id", DataType::Utf8, stress)
            .with_metadata(HashMap::from([("origin".into(), "synthetic".into())])),
        Field::new(
            "sample_time_utc",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("sql_server_instance_name", DataType::Utf8, false),
        Field::new("machine_name", DataType::Utf8, false),
        Field::new("database_id", DataType::Int16, stress),
        Field::new("database_name", DataType::Utf8, false),
        Field::new("session_id", DataType::Int16, false),
        Field::new("session_status", DataType::Utf8, false),
        Field::new("connection_id", DataType::Utf8, false),
    ];
    Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([("fixture".into(), "public-synthetic-dmv-v1".into())]),
    ))
}

fn corpus(stress: bool) -> Result<Vec<RecordBatch>> {
    let schema = schema(stress);
    (0..BATCHES)
        .map(|batch| {
            let mut customers = Vec::with_capacity(BATCH_ROWS);
            let mut times = Vec::with_capacity(BATCH_ROWS);
            let mut instances = Vec::with_capacity(BATCH_ROWS);
            let mut machines = Vec::with_capacity(BATCH_ROWS);
            let mut databases = Vec::with_capacity(BATCH_ROWS);
            let mut names = Vec::with_capacity(BATCH_ROWS);
            let mut sessions = Vec::with_capacity(BATCH_ROWS);
            let mut statuses = Vec::with_capacity(BATCH_ROWS);
            let mut connections = Vec::with_capacity(BATCH_ROWS);
            for row in 0..BATCH_ROWS {
                let id = batch * BATCH_ROWS + row;
                let key = if stress {
                    (id * 17 + batch) % 13
                } else {
                    (batch * 7) % 11
                };
                customers.push(if stress && id % 19 == 0 {
                    None
                } else {
                    Some(format!("customer-{key:02}"))
                });
                times.push(1_700_000_000_000_i64 + id as i64);
                instances.push(format!("sql-instance-{:02}", id % 16));
                machines.push(format!("machine-{:02}", id % 32));
                databases.push(if stress && id % 23 == 0 {
                    None
                } else {
                    Some((id % 17) as i16)
                });
                names.push(format!("database-{:02}", id % 17));
                sessions.push((id % 1000) as i16);
                statuses.push(if id % 2 == 0 { "running" } else { "sleeping" });
                connections.push(format!("connection-{id:08}"));
            }
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(customers)) as ArrayRef,
                    Arc::new(TimestampMicrosecondArray::from(times)),
                    Arc::new(StringArray::from(instances)),
                    Arc::new(StringArray::from(machines)),
                    Arc::new(Int16Array::from(databases)),
                    Arc::new(StringArray::from(names)),
                    Arc::new(Int16Array::from(sessions)),
                    Arc::new(StringArray::from(statuses)),
                    Arc::new(StringArray::from(connections)),
                ],
            )
            .map_err(Into::into)
        })
        .collect()
}

fn reference(batches: &[RecordBatch], stress: bool) -> Result<RecordBatch> {
    let input = concat_batches(&batches[0].schema(), batches)?;
    let options = Some(SortOptions {
        descending: false,
        nulls_first: false,
    });
    let indices = if stress {
        lexsort_to_indices(
            &[
                SortColumn {
                    values: Arc::clone(input.column(0)),
                    options,
                },
                SortColumn {
                    values: Arc::clone(input.column(4)),
                    options,
                },
            ],
            None,
        )?
    } else {
        sort_to_indices(input.column(0), options, None)?
    };
    let arrays = input
        .columns()
        .iter()
        .map(|column| take(column, &indices, None))
        .collect::<arrow::error::Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(input.schema(), arrays)?)
}

fn null_last<T: Ord>(left: Option<T>, right: Option<T>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(a), Some(b)) => a.cmp(&b),
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn independent_checks(input: &[RecordBatch], output: &RecordBatch, stress: bool) -> Result<()> {
    let original = concat_batches(&input[0].schema(), input)?;
    ensure!(output.schema() == original.schema(), "schema or metadata changed");
    ensure!(output.num_rows() == original.num_rows(), "row count changed");
    let ids = output
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .context("time column type")?;
    let keys = output
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("key type")?;
    let databases = output
        .column(4)
        .as_any()
        .downcast_ref::<Int16Array>()
        .context("database type")?;
    let mut seen = vec![false; output.num_rows()];
    let mut source_indices = Vec::with_capacity(output.num_rows());
    for i in 0..output.num_rows() {
        let id = usize::try_from(ids.value(i) - 1_700_000_000_000)?;
        ensure!(id < seen.len() && !seen[id], "duplicate or invalid source row");
        seen[id] = true;
        source_indices.push(u32::try_from(id)?);
        if i > 0 {
            let k = |n| if keys.is_null(n) { None } else { Some(keys.value(n)) };
            let d = |n| {
                if databases.is_null(n) {
                    None
                } else {
                    Some(databases.value(n))
                }
            };
            let order = null_last(k(i - 1), k(i));
            ensure!(order != std::cmp::Ordering::Greater, "customer order");
            if stress && order == std::cmp::Ordering::Equal {
                ensure!(
                    null_last(d(i - 1), d(i)) != std::cmp::Ordering::Greater,
                    "secondary order"
                );
            }
        }
    }
    let indices = UInt32Array::from(source_indices);
    for (source, actual) in original.columns().iter().zip(output.columns()) {
        ensure!(
            take(source, &indices, None)?.as_ref() == actual.as_ref(),
            "source row contents changed"
        );
    }
    Ok(())
}

fn hash(batch: &RecordBatch) -> Result<String> {
    let mut digest = Sha256::new();
    for row in 0..batch.num_rows() {
        for column in batch.columns() {
            if column.is_null(row) {
                digest.update([0]);
            } else {
                let value = array_value_to_string(column.as_ref(), row)?;
                digest.update([1]);
                digest.update((value.len() as u64).to_le_bytes());
                digest.update(value.as_bytes());
            }
        }
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn cpu_ticks() -> Result<u64> {
    let stat = fs::read_to_string("/proc/self/stat")?;
    let rest = stat.rsplit_once(") ").context("invalid proc stat")?.1;
    let fields: Vec<_> = rest.split_whitespace().collect();
    Ok(fields.get(11).context("missing user ticks")?.parse::<u64>()?
        + fields.get(12).context("missing system ticks")?.parse::<u64>()?)
}

fn high_water_kib() -> Result<u64> {
    let status = fs::read_to_string("/proc/self/status")?;
    let line = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .context("VmHWM unavailable")?;
    Ok(line.split_whitespace().nth(1).context("VmHWM value")?.parse()?)
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        args.len() == 3 && (args[1] == "parity" || args[1] == "nulls" || args[1] == "stress"),
        "usage: layout-sort-fixture parity|nulls|stress iterations"
    );
    let iterations: usize = args[2].parse()?;
    ensure!((1..=100).contains(&iterations), "iterations must be 1..=100");
    let stress = args[1] == "stress";
    let batches = corpus(args[1] != "parity")?;
    let input_bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
    let policy = LayoutPolicy {
        sort_columns: if stress {
            vec!["customer_id".into(), "database_id".into()]
        } else {
            vec!["customer_id".into()]
        },
        max_rows: MAX_ROWS,
        max_input_bytes: MAX_BYTES,
        row_group_rows: 262_144,
        ..LayoutPolicy::default()
    };
    ensure!(
        batches.len() * BATCH_ROWS <= MAX_ROWS && input_bytes <= MAX_BYTES,
        "fixture exceeds retained-input budget"
    );
    let expected = reference(&batches, stress)?;
    independent_checks(&batches, &expected, stress)?;
    let input = concat_batches(&batches[0].schema(), &batches)?;
    let input_hash = hash(&input)?;
    let output_hash = hash(&expected)?;

    let variable = vec![
        input.slice(0, 1),
        input.slice(1, 8_191),
        input.slice(8_192, 16_385),
        input.slice(24_577, input.num_rows() - 24_577),
    ];
    let variable_expected = reference(&variable, stress)?;
    let mut variable_buffer = FileSortBuffer::new(input.schema(), policy.clone())?;
    for batch in &variable {
        ensure!(variable_buffer.try_push(batch)?, "variable-size batch rejected");
    }
    let variable_actual = variable_buffer.finish()?;
    ensure!(
        variable_actual == variable_expected,
        "variable-size sliced batches differ from Arrow reference"
    );
    independent_checks(&variable, &variable_actual, stress)?;

    let mut row_limited = FileSortBuffer::new(
        batches[0].schema(),
        LayoutPolicy {
            max_rows: BATCH_ROWS + 1,
            ..policy.clone()
        },
    )?;
    ensure!(
        row_limited.try_push(&batches[0])? && !row_limited.try_push(&batches[1])?,
        "row admission bound"
    );
    ensure!(row_limited.rows() == BATCH_ROWS, "row rejection retained data");
    let mut byte_limited = FileSortBuffer::new(
        batches[0].schema(),
        LayoutPolicy {
            max_input_bytes: batches[0].get_array_memory_size() + 1,
            ..policy.clone()
        },
    )?;
    ensure!(
        byte_limited.try_push(&batches[0])? && !byte_limited.try_push(&batches[1])?,
        "byte admission bound"
    );
    ensure!(byte_limited.rows() == BATCH_ROWS, "byte rejection retained data");
    let mut oversized = FileSortBuffer::new(
        batches[0].schema(),
        LayoutPolicy {
            max_input_bytes: 1,
            ..policy.clone()
        },
    )?;
    ensure!(oversized.try_push(&batches[0]).is_err(), "oversized batch accepted");

    println!(
        "case={} batches={} rows={} retained_input_bytes={} input_sha256={} output_sha256={}",
        args[1],
        BATCHES,
        input.num_rows(),
        input_bytes,
        input_hash,
        output_hash
    );
    for iteration in 0..iterations {
        let mut buffer = FileSortBuffer::new(batches[0].schema(), policy.clone())?;
        for batch in &batches {
            ensure!(buffer.try_push(batch)?, "unexpected budget rejection");
        }
        let cpu_before = cpu_ticks()?;
        let start = Instant::now();
        let actual = buffer.finish()?;
        let wall_ns = start.elapsed().as_nanos();
        let cpu_after = cpu_ticks()?;
        ensure!(
            actual == expected,
            "output differs from Arrow reference, including tie order"
        );
        independent_checks(&batches, &actual, stress)?;
        println!(
            "iteration={iteration} finish_wall_ns={wall_ns} process_cpu_ticks={} process_peak_rss_kib={}",
            cpu_after - cpu_before,
            high_water_kib()?
        );
    }
    Ok(())
}
