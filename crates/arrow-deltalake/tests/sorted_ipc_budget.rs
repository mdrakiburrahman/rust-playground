#![cfg(feature = "delta")]

use anyhow::{Context, Result, anyhow};
use arrow::array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt8Array,
    UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_deltalake::delta::{DeltaTable, INGESTION_PARTITION, ingestion_partition_value};
use arrow_deltalake::layout::{FileSortBuffer, LayoutPolicy};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use url::Url;

const STAMP: i64 = 1_704_067_260_123;
const BUDGET_MESSAGE: &str = "batch exceeds sorted-file input budget; slice it before retrying";

/// Column shape of `telemetry.sqlserver.sqlserver_database_properties` (43 columns).
const COLUMNS: &[(&str, char)] = &[
    ("sample_time_utc", 't'),
    ("collection_time_utc", 't'),
    ("sql_server_instance_name", 's'),
    ("machine_name", 's'),
    ("database_id", 'h'),
    ("database_name", 's'),
    ("is_primary_replica", 'b'),
    ("create_date", 't'),
    ("compatibility_level", 'h'),
    ("collation_name", 's'),
    ("user_access_desc", 's'),
    ("is_read_only", 'b'),
    ("updateability", 's'),
    ("is_auto_shrink_on", 'b'),
    ("state_desc", 's'),
    ("is_in_standby", 'b'),
    ("snapshot_isolation_state", 'u'),
    ("is_read_committed_snapshot_on", 'b'),
    ("recovery_model_desc", 's'),
    ("page_verify_option_desc", 's'),
    ("is_auto_create_stats_on", 'b'),
    ("is_auto_update_stats_on", 'b'),
    ("is_auto_update_stats_async_on", 'b'),
    ("is_parameterization_forced", 'b'),
    ("is_published", 'b'),
    ("is_subscribed", 'b'),
    ("is_merge_published", 'b'),
    ("is_distributor", 'b'),
    ("is_broker_enabled", 'b'),
    ("log_reuse_wait_desc", 's'),
    ("containment_desc", 's'),
    ("is_cdc_enabled", 'b'),
    ("is_encrypted", 'b'),
    ("delayed_durability_desc", 's'),
    ("is_accelerated_database_recovery_on", 'b'),
    ("last_good_checkdb_time", 't'),
    ("notable_db_scoped_configs", 's'),
    ("query_store_actual_state_desc", 's'),
    ("query_store_query_capture_mode_desc", 's'),
    ("force_last_good_plan_actual_state", 's'),
    ("count_suspect_pages", 'i'),
    ("is_ledger_on", 'b'),
    ("is_change_feed_enabled", 'b'),
];

fn schema() -> SchemaRef {
    Arc::new(Schema::new(
        COLUMNS
            .iter()
            .map(|(name, kind)| {
                let data_type = match kind {
                    't' => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    's' => DataType::Utf8,
                    'h' => DataType::Int16,
                    'b' => DataType::Boolean,
                    'u' => DataType::UInt8,
                    _ => DataType::Int32,
                };
                Field::new(*name, data_type, false)
            })
            .collect::<Vec<_>>(),
    ))
}

fn table_schema() -> SchemaRef {
    let mut fields = schema().fields().to_vec();
    fields.push(Arc::new(Field::new(INGESTION_PARTITION, DataType::Utf8, false)));
    Arc::new(Schema::new(fields))
}

/// A 1024-row batch with realistic, long descriptive strings, built with owned buffers.
fn built(rows: usize, offset: usize) -> Result<RecordBatch> {
    let columns = COLUMNS
        .iter()
        .map(|(name, kind)| -> Result<ArrayRef> {
            Ok(match kind {
                't' => Arc::new(
                    TimestampNanosecondArray::from_iter_values(
                        (0..rows).map(|row| 1_704_067_200_000_000_000 + i64::try_from(row + offset).unwrap_or(0)),
                    )
                    .with_timezone("UTC"),
                ),
                's' => Arc::new(StringArray::from_iter_values((0..rows).map(|row| {
                    format!(
                        "{name}:{:08}:{}",
                        row + offset,
                        "SQL_Latin1_General_CP1_CI_AS ".repeat(4)
                    )
                }))),
                'h' => Arc::new(Int16Array::from_iter_values(
                    (0..rows).map(|row| i16::try_from((row + offset) % 32_000).unwrap_or(0)),
                )),
                'b' => Arc::new(BooleanArray::from_iter((0..rows).map(|row| Some(row % 2 == 0)))),
                'u' => Arc::new(UInt8Array::from_iter_values(
                    (0..rows).map(|row| u8::try_from(row % 4).unwrap_or(0)),
                )),
                _ => Arc::new(Int32Array::from_iter_values(
                    (0..rows).map(|row| i32::try_from(row + offset).unwrap_or(0)),
                )),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema(), columns)?)
}

/// The same batch after a Flight/IPC hop: every column references one shared body allocation.
fn decoded(batch: &RecordBatch) -> Result<RecordBatch> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    StreamReader::try_new(std::io::Cursor::new(bytes), None)?
        .next()
        .context("decoded batch")?
        .map_err(Into::into)
}

fn compacted(batch: &RecordBatch) -> Result<RecordBatch> {
    let rows = u32::try_from(batch.num_rows())?;
    Ok(arrow::compute::take_record_batch(
        batch,
        &UInt32Array::from_iter_values(0..rows),
    )?)
}

/// Run `name` in a child test process with minute ingestion partitions, without mutating this
/// process's environment. Returns false in the parent, true in the child that should run the body.
fn in_minute_child(name: &str) -> Result<bool> {
    if std::env::var("ARROWCOL_PARTITION_GRANULARITY").as_deref() == Ok("minute") {
        return Ok(true);
    }
    let status = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env("ARROWCOL_PARTITION_GRANULARITY", "minute")
        .status()?;
    anyhow::ensure!(status.success(), "{name} failed under minute partitions");
    Ok(false)
}

fn policy() -> LayoutPolicy {
    LayoutPolicy {
        sort_columns: vec!["database_name".into(), "sample_time_utc".into()],
        bloom_columns: vec!["database_name".into()],
        ..LayoutPolicy::default()
    }
}

fn adds(root: &Path) -> Result<Vec<serde_json::Value>> {
    let mut active = BTreeMap::new();
    let mut names = fs::read_dir(root.join("_delta_log"))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.retain(|path| path.extension().is_some_and(|extension| extension == "json"));
    names.sort();
    for path in names {
        for line in fs::read_to_string(path)?.lines() {
            let action: serde_json::Value = serde_json::from_str(line)?;
            if let Some(add) = action.get("add") {
                active.insert(add["path"].as_str().context("path")?.to_owned(), add.clone());
            } else if let Some(remove) = action.get("remove") {
                active.remove(remove["path"].as_str().context("path")?);
            }
        }
    }
    Ok(active.into_values().collect())
}

fn txns(root: &Path) -> Result<Vec<(String, i64)>> {
    let mut found = Vec::new();
    for entry in fs::read_dir(root.join("_delta_log"))? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "json") {
            for line in fs::read_to_string(path)?.lines() {
                let action: serde_json::Value = serde_json::from_str(line)?;
                if let Some(txn) = action.get("txn") {
                    found.push((
                        txn["appId"].as_str().context("appId")?.to_owned(),
                        txn["version"].as_i64().context("txn version")?,
                    ));
                }
            }
        }
    }
    found.sort();
    Ok(found)
}

/// Scenario: a real-width 1024-row batch arrives IPC-decoded, as the collector receives it.
/// Guarantees: before the fix the unchanged guard rejects it only because each column charges the
/// shared IPC body; compaction keeps the same rows and charges real memory, while the guard still
/// rejects the identical compacted input under a genuinely smaller budget.
#[test]
fn decoded_wide_batch_overcharges_shared_ipc_body_and_compaction_fits_budget() -> Result<()> {
    let built = built(1024, 0)?;
    let decoded = decoded(&built)?;
    let compact = compacted(&decoded)?;
    let budget = LayoutPolicy::default().max_input_bytes;
    assert!(
        decoded.get_array_memory_size() > budget,
        "decoded charge {} must reproduce the production overcharge",
        decoded.get_array_memory_size()
    );
    assert!(compact.get_array_memory_size() * 8 < budget);
    assert!(compact.get_array_memory_size() <= built.get_array_memory_size() * 2);
    assert_eq!(compact, built);
    assert_eq!(decoded, built);

    let physical = decoded.schema();
    let mut red = FileSortBuffer::new(Arc::clone(&physical), policy())?;
    let error = red
        .try_push(&decoded)
        .err()
        .context("pre-fix decoded batch must be rejected")?;
    assert_eq!(error.to_string(), BUDGET_MESSAGE);
    let mut green = FileSortBuffer::new(Arc::clone(&physical), policy())?;
    assert!(green.try_push(&compact)?);

    let tight = LayoutPolicy {
        max_input_bytes: compact.get_array_memory_size() - 1,
        ..policy()
    };
    let mut oversize = FileSortBuffer::new(physical, tight)?;
    let error = oversize
        .try_push(&compact)
        .err()
        .context("genuinely oversize input must fail")?;
    assert_eq!(error.to_string(), BUDGET_MESSAGE);
    Ok(())
}

/// Scenario: two IPC-decoded 1024-row batches form one publication through the production writer,
/// then the same publication identity is replayed.
/// Guarantees: one sorted file holds exactly the 2048 input rows in the admission partition under
/// one stable txn, and the replay adds no file, row or txn.
#[tokio::test]
async fn decoded_wide_publication_commits_once_in_one_sorted_file() -> Result<()> {
    if !in_minute_child("decoded_wide_publication_commits_once_in_one_sorted_file")? {
        return Ok(());
    }
    let root = tempfile::tempdir()?;
    let url = Url::from_directory_path(fs::canonicalize(root.path())?).map_err(|()| anyhow!("URL"))?;
    let table = DeltaTable::open_url(url, table_schema(), vec![INGESTION_PARTITION.into()])?;
    let first = decoded(&built(1024, 1024)?)?;
    let second = decoded(&built(1024, 0)?)?;
    let partition = ingestion_partition_value(STAMP)?;
    assert_eq!(partition, "2024-01-01-00-01", "admission minute of the stable stamp");
    let mut versions = Vec::new();
    for _ in 0..2 {
        let mut writer = table.new_file_with_publication_and_layout(
            schema(),
            "arrowcol.publication.wide".into(),
            0,
            STAMP,
            Some(policy()),
        )?;
        writer.write(&first).await?;
        writer.write(&second).await?;
        assert_eq!(writer.rows_written(), 2048);
        versions.push(writer.finish().await?.version);
    }
    assert_eq!(versions[0], versions[1], "replay must resolve to the original commit");

    let adds = adds(root.path())?;
    assert_eq!(adds.len(), 1);
    let add = &adds[0];
    assert_eq!(add["partitionValues"][INGESTION_PARTITION], partition.as_str());
    let stats: serde_json::Value = serde_json::from_str(add["stats"].as_str().context("stats")?)?;
    assert_eq!(stats["numRecords"], 2048);
    assert_eq!(txns(root.path())?, vec![("arrowcol.publication.wide".to_owned(), 0)]);

    let relative = add["path"].as_str().context("path")?;
    let file = Url::from_directory_path(fs::canonicalize(root.path())?)
        .map_err(|()| anyhow!("URL"))?
        .join(relative)?
        .to_file_path()
        .map_err(|()| anyhow!("path"))?;
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(fs::File::open(file)?)?.build()?;
    let mut keys = Vec::new();
    for batch in reader {
        let batch = batch?;
        let names = batch
            .column_by_name("database_name")
            .context("database_name")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("database_name strings")?
            .clone();
        keys.extend(names.iter().map(|value| value.map(str::to_owned)));
    }
    let mut expected = keys.clone();
    expected.sort();
    assert_eq!(keys, expected, "whole-file sort spans both input batches");
    let mut unique = keys.clone();
    unique.dedup();
    assert_eq!(unique.len(), 2048, "exact row multiset: no drop or duplicate");
    Ok(())
}

/// Scenario: a single row is larger than the sort budget.
/// Guarantees: explicit deterministic rejection, no split loop, no commit.
#[tokio::test]
async fn single_row_over_byte_budget_fails_without_commit() -> Result<()> {
    if !in_minute_child("single_row_over_byte_budget_fails_without_commit")? {
        return Ok(());
    }
    let root = tempfile::tempdir()?;
    let url = Url::from_directory_path(fs::canonicalize(root.path())?).map_err(|()| anyhow!("URL"))?;
    let table = DeltaTable::open_url(url, table_schema(), vec![INGESTION_PARTITION.into()])?;
    let row = decoded(&built(1, 0)?)?;
    let mut writer = table.new_file_with_publication_and_layout(
        schema(),
        "arrowcol.publication.tiny".into(),
        0,
        STAMP,
        Some(LayoutPolicy {
            max_input_bytes: 64,
            ..policy()
        }),
    )?;
    let error = writer.write(&row).await.err().context("over-budget row must fail")?;
    assert!(format!("{error:#}").contains(BUDGET_MESSAGE), "{error:#}");
    assert_eq!(writer.rows_written(), 0);
    assert!(writer.stage().await.is_err());
    assert_eq!(table.version()?, 0);
    assert!(adds(root.path())?.is_empty());
    Ok(())
}
