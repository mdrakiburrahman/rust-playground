mod policy;

use anyhow::{Context, Result, ensure};
use arrow::ipc::writer::StreamWriter;
use arrow_deltalake::{ParquetFileWriter, writer::ParquetWriterConfig};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs::File, time::Instant};

const ROWS: usize = 65_536;
const REPEATS: usize = 16;
const CUSTOMER: &str = "public-customer-000001";

fn cpu_ticks() -> Result<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let fields = stat
        .rsplit_once(") ")
        .context("invalid stat")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    Ok(fields.get(11).context("utime")?.parse::<u64>()? + fields.get(12).context("stime")?.parse::<u64>()?)
}

fn family(codec: Compression) -> &'static str {
    match codec {
        Compression::UNCOMPRESSED => "UNCOMPRESSED",
        Compression::SNAPPY => "SNAPPY",
        Compression::GZIP(_) => "GZIP",
        Compression::BROTLI(_) => "BROTLI",
        Compression::LZ4 => "LZ4",
        Compression::LZ4_RAW => "LZ4_RAW",
        Compression::ZSTD(_) => "ZSTD",
        Compression::LZO => "LZO",
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    ensure!(args.len() == 3, "usage: darwin-codec-fixture BASELINE CANDIDATE");
    let baseline: Value = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let candidate: Value = serde_json::from_slice(&std::fs::read(&args[2])?)?;
    let table = collector_chain::dmv::tables()?
        .into_iter()
        .find(|table| table.identity().table().as_str() == "sqlserver_availability_replica_states")
        .context("DMV table absent")?;
    ensure!(CUSTOMER.len() == 22, "customer literal must be 22 bytes");
    let batch = collector_chain::dmv::batch(&table, CUSTOMER, ROWS, 1_700_000_000_000_000)?;
    let schema = batch.schema();
    let selected = policy::check(&baseline, &candidate, &schema)?;
    let requested: ParquetWriterConfig = serde_json::from_value(
        candidate
            .pointer("/tunables/parquet_writer")
            .context("writer missing")?
            .clone(),
    )?;
    ensure!(
        arrow_deltalake::writer::process_config()? == &requested,
        "startup writer policy does not match candidate; set ARROWCOL_PARQUET_CONFIG"
    );
    let readback: Value = serde_json::from_str(&arrow_deltalake::writer::effective_startup_config_json()?)?;

    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    let corpus_hash = format!("{:x}", Sha256::digest(&ipc));
    drop(ipc);
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("data.parquet");
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROWS))
        .build();
    let begin = Instant::now();
    let cpu = cpu_ticks()?;
    let mut writer = ParquetFileWriter::create_with_properties(&path, schema.clone(), Some(properties))?;
    for _ in 0..REPEATS {
        writer.write(&batch).await?;
    }
    let file = writer.finish().await?;
    let wall_ns = begin.elapsed().as_nanos();
    let cpu_ticks = cpu_ticks()? - cpu;

    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?.with_batch_size(ROWS);
    ensure!(builder.schema().as_ref() == schema.as_ref(), "schema mismatch");
    let metadata = builder.metadata();
    let row_groups = metadata.num_row_groups();
    for group in metadata.row_groups() {
        ensure!(group.num_rows() > 0, "empty row group");
        if let Some(limit) = requested.max_row_group_rows {
            ensure!(group.num_rows() as usize <= limit, "row group exceeds requested limit");
        }
        let actual: Vec<_> = group
            .columns()
            .iter()
            .map(|column| family(column.compression()))
            .collect();
        ensure!(
            actual == selected.expected_codecs,
            "footer codec differs from expected physical policy"
        );
    }
    let mut decoded_rows = 0;
    for decoded in builder.build()? {
        let decoded = decoded.context("reader does not support codec or data is corrupt")?;
        let mut offset = 0;
        while offset < decoded.num_rows() {
            let expected_offset = (decoded_rows + offset) % ROWS;
            let count = (decoded.num_rows() - offset).min(ROWS - expected_offset);
            ensure!(
                decoded.slice(offset, count) == batch.slice(expected_offset, count),
                "decoded rows differ from expected corpus"
            );
            offset += count;
        }
        decoded_rows += decoded.num_rows();
    }
    ensure!(
        decoded_rows == ROWS * REPEATS && file.rows == decoded_rows,
        "row count mismatch"
    );
    ensure!(std::fs::metadata(&path)?.len() == file.bytes, "file length mismatch");
    let clock = std::process::Command::new("getconf").arg("CLK_TCK").output()?;
    ensure!(clock.status.success(), "CLK_TCK failed");
    let hz: u64 = std::str::from_utf8(&clock.stdout)?.trim().parse()?;
    println!(
        "DARWIN_CODEC_RESULT={}",
        json!({
            "corpus":"dmv-batch-synthetic-65536-replayed-16",
            "customer":"22-byte-public-literal",
            "corpus_sha256_arrow_ipc":corpus_hash,
            "rows":decoded_rows,"distinct_rows":ROWS,"replays":REPEATS,
            "row_groups":row_groups,"physical_columns":selected.expected_codecs.len(),
            "coordinate":selected.coordinate,"footer_codecs":selected.expected_codecs,
            "startup_readback":readback,
            "parquet_bytes":file.bytes,"parquet_bytes_per_row":file.bytes as f64 / decoded_rows as f64,
            "writer_input_bytes":file.input_bytes,"wall_ns":wall_ns,
            "process_cpu_ticks":cpu_ticks,"clock_ticks_per_second":hz,
            "validation":"schema_and_every_ordered_value_equal",
            "scope":"parquet_component_no_delta_commit"
        })
    );
    Ok(())
}
