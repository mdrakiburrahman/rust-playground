use anyhow::{Context, Result, ensure};
use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow_deltalake::ParquetFileWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

const ROWS: usize = 4_194_304;
const BATCH_ROWS: usize = 8_192;
const ROW_GROUP_ROWS: usize = 65_536;

fn corpus() -> Result<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("measurement", DataType::Int64, false),
        Field::new("node", DataType::Utf8, false),
        Field::new("sample", DataType::Utf8, false),
    ]));
    (0..ROWS)
        .step_by(BATCH_ROWS)
        .map(|start| {
            let ids = (start..start + BATCH_ROWS).map(|row| row as i64);
            let measurements = (start..start + BATCH_ROWS).map(|row| ((row * 73) % 1_000_003) as i64);
            let nodes = (start..start + BATCH_ROWS).map(|row| format!("node-{:04}", row % 1024));
            let samples = (start..start + BATCH_ROWS)
                .map(|row| format!("sample-{:016x}-{:08x}", row, row.wrapping_mul(2_654_435_761)));
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(ids)) as ArrayRef,
                    Arc::new(Int64Array::from_iter_values(measurements)),
                    Arc::new(StringArray::from_iter_values(nodes)),
                    Arc::new(StringArray::from_iter_values(samples)),
                ],
            )?)
        })
        .collect()
}

fn process_cpu_ticks() -> Result<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let fields = stat.rsplit_once(") ").context("invalid /proc/self/stat")?.1;
    let fields: Vec<_> = fields.split_whitespace().collect();
    let user: u64 = fields.get(11).context("missing user CPU ticks")?.parse()?;
    let system: u64 = fields.get(12).context("missing system CPU ticks")?.parse()?;
    user.checked_add(system).context("process CPU tick overflow")
}

fn ticks_per_second() -> Result<u64> {
    let result = std::process::Command::new("getconf").arg("CLK_TCK").output()?;
    ensure!(result.status.success(), "getconf CLK_TCK failed");
    let ticks = std::str::from_utf8(&result.stdout)?.trim().parse::<u64>()?;
    ensure!(ticks > 0, "invalid CPU clock frequency");
    Ok(ticks)
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual release-mode component benchmark"]
async fn darwin_parquet_encode() -> Result<()> {
    let clock_ticks_per_second = ticks_per_second()?;
    let batches = corpus()?;
    let schema = batches[0].schema();
    let expected = concat_batches(&schema, &batches)?;
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &schema)?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    let corpus_sha256 = format!("{:x}", Sha256::digest(&ipc));
    drop(ipc);
    let requested_config = match std::env::var("ARROWCOL_PARQUET_CONFIG") {
        Ok(value) => serde_json::from_str::<serde_json::Value>(&value)?,
        Err(std::env::VarError::NotPresent) => serde_json::Value::Null,
        Err(error) => return Err(error.into()),
    };
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?))
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .build();
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("data.parquet");

    let started = Instant::now();
    let cpu_started = process_cpu_ticks()?;
    let mut writer = ParquetFileWriter::create_with_properties(&path, Arc::clone(&schema), Some(properties))?;
    let create_wall_ns = started.elapsed().as_nanos();
    let create_cpu_ticks = process_cpu_ticks()? - cpu_started;

    let started = Instant::now();
    let cpu_started = process_cpu_ticks()?;
    for batch in &batches {
        writer.write(batch).await?;
    }
    let write_wall_ns = started.elapsed().as_nanos();
    let write_cpu_ticks = process_cpu_ticks()? - cpu_started;

    let started = Instant::now();
    let cpu_started = process_cpu_ticks()?;
    let completed = writer.finish().await?;
    let finish_wall_ns = started.elapsed().as_nanos();
    let finish_cpu_ticks = process_cpu_ticks()? - cpu_started;
    ensure!(completed.rows == ROWS, "completed row count mismatch");
    ensure!(
        completed.bytes == std::fs::metadata(&path)?.len(),
        "physical file size mismatch"
    );

    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
    ensure!(builder.schema().as_ref() == schema.as_ref(), "decoded schema mismatch");
    let metadata = builder.metadata();
    let row_groups = metadata.num_row_groups();
    let row_group_rows: Vec<_> = metadata.row_groups().iter().map(|group| group.num_rows()).collect();
    let codecs: Vec<Vec<&str>> = metadata
        .row_groups()
        .iter()
        .map(|group| {
            group
                .columns()
                .iter()
                .map(|column| match column.compression() {
                    Compression::UNCOMPRESSED => "UNCOMPRESSED",
                    Compression::SNAPPY => "SNAPPY",
                    Compression::GZIP(_) => "GZIP",
                    Compression::LZO => "LZO",
                    Compression::BROTLI(_) => "BROTLI",
                    Compression::LZ4 => "LZ4",
                    Compression::ZSTD(_) => "ZSTD",
                    Compression::LZ4_RAW => "LZ4_RAW",
                })
                .collect()
        })
        .collect();
    let dictionary_pages: Vec<Vec<bool>> = metadata
        .row_groups()
        .iter()
        .map(|group| {
            group
                .columns()
                .iter()
                .map(|column| column.dictionary_page_offset().is_some())
                .collect()
        })
        .collect();
    let decoded = builder.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
    let actual = concat_batches(&schema, &decoded)?;
    ensure!(
        actual.num_rows() == ROWS && actual == expected,
        "decoded values or row count differ"
    );

    println!(
        "DARWIN_PARQUET_RESULT={}",
        json!({
            "contract_version": 1,
            "corpus": "local-synthetic-v1-not-historical-dmv",
            "corpus_sha256_arrow_ipc": corpus_sha256,
            "rows": completed.rows,
            "batch_rows": BATCH_ROWS,
            "batches": batches.len(),
            "row_groups": row_groups,
            "row_group_rows": row_group_rows,
            "base_compression": "zstd",
            "base_zstd_level": 1,
            "base_max_row_group_rows": ROW_GROUP_ROWS,
            "requested_process_config": requested_config,
            "footer_codecs_by_row_group": codecs,
            "footer_dictionary_pages_by_row_group": dictionary_pages,
            "arrow_deltalake_input_bytes": completed.input_bytes,
            "parquet_bytes": completed.bytes,
            "parquet_bytes_per_row": completed.bytes as f64 / completed.rows as f64,
            "create_wall_ns": create_wall_ns,
            "create_cpu_ticks": create_cpu_ticks,
            "write_wall_ns": write_wall_ns,
            "write_cpu_ticks": write_cpu_ticks,
            "finish_wall_ns": finish_wall_ns,
            "finish_cpu_ticks": finish_cpu_ticks,
            "clock_ticks_per_second": clock_ticks_per_second,
            "validation": "full_schema_and_ordered_values_equal",
            "scope": "component_not_delta_commit"
        })
    );
    Ok(())
}
