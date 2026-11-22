use super::{fitting_sort_chunk, owned_for_sort};
use anyhow::{Context, Result, ensure};
use arrow::array::{ArrayRef, Int16Array, ListArray, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Instant;

const ROWS: usize = 65_536;

fn input() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Utf8, false),
        Field::new(
            "sample_time_utc",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("sql_server_instance_name", DataType::Utf8, false),
        Field::new("machine_name", DataType::Utf8, false),
        Field::new("database_id", DataType::Int16, false),
        Field::new("database_name", DataType::Utf8, false),
        Field::new("session_id", DataType::Int16, false),
        Field::new("session_status", DataType::Utf8, false),
        Field::new("connection_id", DataType::Utf8, false),
    ]));
    let strings = |f: fn(usize) -> String| -> ArrayRef { Arc::new(StringArray::from_iter_values((0..ROWS).map(f))) };
    Ok(RecordBatch::try_new(
        schema,
        vec![
            strings(|_| "customer-0001".into()),
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                (0..ROWS).map(|i| 1_700_000_000_000_i64 + i as i64),
            )),
            strings(|i| format!("instance-{:02}", i % 16)),
            strings(|i| format!("machine-{:02}", i % 32)),
            Arc::new(Int16Array::from_iter_values((0..ROWS).map(|i| (i % 17) as i16))),
            strings(|i| format!("database-{:02}", i % 17)),
            Arc::new(Int16Array::from_iter_values((0..ROWS).map(|i| (i % 1_000) as i16))),
            strings(|i| {
                if i.is_multiple_of(2) {
                    "running".into()
                } else {
                    "sleeping".into()
                }
            }),
            strings(|i| format!("connection-{i:08}")),
        ],
    )?)
}

fn nested() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new_list("nested", Field::new_list_field(DataType::Int32, true), true),
    ]));
    let keys = StringArray::from_iter((0_i32..1024).map(|i| (i % 11 != 0).then(|| format!("key-{}", i % 31))));
    let lists = ListArray::from_iter_primitive::<Int32Type, _, _>(
        (0_i32..1024).map(|i| (i % 13 != 0).then(|| vec![Some(i), None, Some(i + 1)])),
    );
    Ok(RecordBatch::try_new(schema, vec![Arc::new(keys), Arc::new(lists)])?)
}

fn reference(batch: &RecordBatch, start: usize, max_rows: usize, max_bytes: usize) -> Result<Option<RecordBatch>> {
    let mut low = 0;
    let mut high = max_rows.min(batch.num_rows().saturating_sub(start));
    while low < high {
        let rows = low + (high - low).div_ceil(2);
        if owned_for_sort(batch.slice(start, rows))?.get_array_memory_size() <= max_bytes {
            low = rows;
        } else {
            high = rows - 1;
        }
    }
    if low == 0 {
        Ok(None)
    } else {
        Ok(Some(owned_for_sort(batch.slice(start, low))?))
    }
}

fn assert_equal(batch: &RecordBatch, start: usize, max_rows: usize, max_bytes: usize) -> Result<()> {
    let actual = fitting_sort_chunk(batch, start, max_rows, max_bytes)?;
    let expected = reference(batch, start, max_rows, max_bytes)?;
    ensure!(actual.is_some() == expected.is_some(), "presence differs");
    if let (Some(a), Some(b)) = (actual, expected) {
        ensure!(a == b && a.schema() == b.schema(), "chunk contents or schema differ");
        ensure!(
            a.get_array_memory_size() == b.get_array_memory_size(),
            "actual memory charge differs"
        );
        ensure!(
            a.get_array_memory_size() <= max_bytes && a.num_rows() <= max_rows,
            "cap exceeded"
        );
    }
    Ok(())
}

fn cpu_ticks() -> Result<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let fields = stat
        .rsplit_once(") ")
        .context("invalid proc stat")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    Ok(fields.get(11).context("missing user ticks")?.parse::<u64>()?
        + fields.get(12).context("missing system ticks")?.parse::<u64>()?)
}

#[test]
fn chunk_reference_cap_and_backing_buffers() -> Result<()> {
    let full = input()?;
    let nested = nested()?;
    for batch in [&full, &full.slice(12_345, 3_000), &nested, &nested.slice(19, 157)] {
        for start in [0, batch.num_rows() / 3, batch.num_rows(), batch.num_rows() + 1] {
            for rows in [0, 1, 17, batch.num_rows(), batch.num_rows() + 1] {
                let full_len = rows.min(batch.num_rows().saturating_sub(start));
                let charge = if full_len == 0 {
                    0
                } else {
                    owned_for_sort(batch.slice(start, full_len))?.get_array_memory_size()
                };
                for bytes in [
                    0,
                    1,
                    charge.saturating_sub(1),
                    charge,
                    charge.saturating_add(1),
                    usize::MAX,
                ] {
                    assert_equal(batch, start, rows, bytes)?;
                }
            }
        }
    }
    Ok(())
}

#[test]
#[ignore = "release-mode measurement; run alone"]
fn measure_sort_chunk() -> Result<()> {
    let input = input()?;
    let nested = nested()?;
    let cases = [
        ("full_fit", input.clone(), 0, ROWS, 1_073_741_824),
        ("partial_rows", input.clone(), 0, ROWS / 2, 1_073_741_824),
        ("partial_bytes", input.clone(), 0, ROWS, 1_000_000),
        ("sliced", input.slice(123, 32_000), 71, 20_000, 1_073_741_824),
        ("nested", nested, 7, 900, 1_073_741_824),
    ];
    for (name, batch, start, rows, bytes) in cases {
        assert_equal(&batch, start, rows, bytes)?;
        let expected = reference(&batch, start, rows, bytes)?;
        let result = expected.as_ref().map_or(0, RecordBatch::num_rows);
        let charge = expected.as_ref().map_or(0, RecordBatch::get_array_memory_size);
        let cpu = cpu_ticks()?;
        let before = Instant::now();
        for _ in 0..10 {
            std::hint::black_box(fitting_sort_chunk(std::hint::black_box(&batch), start, rows, bytes)?);
        }
        let elapsed = before.elapsed().as_nanos();
        println!(
            "helper case={name} loops=10 wall_ns={elapsed} cpu_ticks={} input_rows={} input_bytes={} start={start} row_cap={rows} byte_cap={bytes} output_rows={result} output_bytes={charge}",
            cpu_ticks()? - cpu,
            batch.num_rows(),
            batch.get_array_memory_size()
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "release-mode local staging measurement; run alone"]
async fn measure_coalesced_stage() -> Result<()> {
    let input = input()?;
    let normalized = crate::normalize::batch(&input)?;
    let root = tempfile::tempdir()?;
    let table = crate::commit::BatchedTable::open(root.path(), crate::commit::CommitOptions::default()).await?;
    for iteration in 0..3 {
        let entries = (0..4).map(|i| (input.clone(), format!("fixture-{i}"), 0, 1)).collect();
        let cpu = cpu_ticks()?;
        let start = Instant::now();
        let group = table.stage_coalesced_publications(entries).await?;
        let elapsed = start.elapsed().as_nanos();
        let cpu_elapsed = cpu_ticks()? - cpu;
        ensure!(
            group.appends.len() == 1 && group.identities.len() == 4,
            "unexpected physical grouping"
        );
        let file = &group.appends[0].file;
        ensure!(file.rows == ROWS * 4, "stage lost rows");
        let bytes = std::fs::read(&file.path)?;
        let sha = format!("{:x}", Sha256::digest(&bytes));
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&file.path)?)?
                .build()?;
        let output = reader.collect::<arrow::error::Result<Vec<_>>>()?;
        let merged = arrow::compute::concat_batches(&output.first().context("missing output")?.schema(), &output)?;
        ensure!(merged.num_rows() == ROWS * 4, "physical parquet row count");
        for (index, column) in normalized.columns().iter().enumerate() {
            for repeat in 0..4 {
                ensure!(
                    merged.column(index).slice(repeat * ROWS, ROWS).as_ref() == column.as_ref(),
                    "physical parquet differs from reference at column {index}"
                );
            }
        }
        println!(
            "stage iteration={iteration} wall_ns={elapsed} cpu_ticks={cpu_elapsed} prep_sort_ns={} files={} rows={} parquet_bytes={} parquet_sha256={sha}",
            group.preparation_sort.as_nanos(),
            group.appends.len(),
            file.rows,
            bytes.len()
        );
    }
    Ok(())
}
