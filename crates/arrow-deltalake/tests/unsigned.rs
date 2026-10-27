#![cfg(feature = "delta")]
//! Regression: TINYINT UNSIGNED (Arrow UInt8) through the sorted publication path.

use anyhow::Result;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int16Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_deltalake::delta::{DeltaTable, INGESTION_PARTITION};
use std::sync::Arc;

const STAMP_MS: i64 = 1_704_067_200_001;

fn schema() -> SchemaRef {
    let ts = || DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("sample_time_utc", ts(), false),
        Field::new("database_id", DataType::Int16, false),
        Field::new("is_primary_replica", DataType::Boolean, false),
        Field::new("snapshot_isolation_state", DataType::UInt8, false),
    ]))
}

fn batch(customers: Vec<&str>, values: Vec<u8>) -> Result<RecordBatch> {
    let rows = values.len();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(customers)),
        Arc::new(TimestampNanosecondArray::from(vec![1_704_067_200_000_000_001_i64; rows]).with_timezone("UTC")),
        Arc::new(Int16Array::from(vec![5_i16; rows])),
        Arc::new(BooleanArray::from(vec![true; rows])),
        Arc::new(UInt8Array::from(values)),
    ];
    Ok(RecordBatch::try_new(schema(), columns)?)
}

async fn publish(table: &Arc<DeltaTable>, partitioned: bool, id: &str, input: &RecordBatch) -> Result<u64> {
    let file = if partitioned {
        // An explicit Hive value keeps the test independent of the process-wide
        // ARROWCOL_PARTITION_GRANULARITY setting.
        let value = "2024-01-01T00:00Z";
        let mut fields = input.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(INGESTION_PARTITION, DataType::Utf8, false)));
        let mut columns = input.columns().to_vec();
        columns.push(Arc::new(StringArray::from(vec![value; input.num_rows()])));
        let input = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        let values = std::collections::BTreeMap::from([(INGESTION_PARTITION.to_owned(), Some(value.to_owned()))]);
        let mut file = table.new_file_partitioned(input.schema(), values)?;
        file.write(&input).await?;
        file
    } else {
        let mut file =
            table.new_file_with_publication(input.schema(), format!("arrowcol.publication.{id}"), 0, STAMP_MS)?;
        file.write(input).await?;
        file
    };
    Ok(file.finish().await?.version)
}

/// Scenario: Direct-style publications of a customer-keyed table whose
/// TINYINT UNSIGNED column spans 0 and 255, using the default customer sort.
/// Guarantees: every publication commits, the staged file is sorted by
/// customer and the unsigned values survive as widened Int16.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsigned_tinyint_publishes_through_sorted_layout() -> Result<()> {
    unsigned_tinyint_round_trip(false).await
}

/// Scenario: the same publications into an ingestion-minute partitioned table.
/// Guarantees: an explicit ingestion Hive partition does not disturb unsigned publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsigned_tinyint_publishes_into_ingestion_partitions() -> Result<()> {
    unsigned_tinyint_round_trip(true).await
}

async fn unsigned_tinyint_round_trip(partitioned: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let url = url::Url::from_directory_path(directory.path()).map_err(|()| anyhow::anyhow!("url"))?;
    let (table_schema, parts) = if partitioned {
        let mut fields = schema().fields().to_vec();
        fields.push(Arc::new(Field::new(INGESTION_PARTITION, DataType::Utf8, false)));
        (Arc::new(Schema::new(fields)), vec![INGESTION_PARTITION.to_owned()])
    } else {
        (schema(), Vec::new())
    };
    let table = DeltaTable::open_url(url.clone(), Arc::clone(&table_schema), parts.clone())?;
    let first = batch(
        vec!["customer-b", "customer-a", "customer-b", "customer-a"],
        vec![255, 0, 1, 254],
    )?;
    assert_eq!(publish(&table, partitioned, "first", &first).await?, 1);
    assert_eq!(
        publish(&table, partitioned, "second", &batch(vec!["customer-c"], vec![255])?).await?,
        2
    );
    drop(table);

    let reopened = DeltaTable::open_url(url, table_schema, parts)?;
    assert_eq!(
        publish(&reopened, partitioned, "third", &batch(vec!["customer-a"], vec![0])?).await?,
        3
    );

    let mut published = Vec::new();
    for entry in walk(directory.path())? {
        if entry.extension().is_some_and(|ext| ext == "parquet") {
            let file = std::fs::File::open(&entry)?;
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
            for batch in reader {
                let batch = batch?;
                let customers = batch
                    .column_by_name("customer_id")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned())
                    .ok_or_else(|| anyhow::anyhow!("customer_id"))?;
                let states = batch
                    .column_by_name("snapshot_isolation_state")
                    .and_then(|c| c.as_any().downcast_ref::<Int16Array>().cloned())
                    .ok_or_else(|| anyhow::anyhow!("snapshot_isolation_state is not Int16"))?;
                for row in 0..batch.num_rows() {
                    published.push((customers.value(row).to_owned(), states.value(row)));
                }
            }
        }
    }
    published.sort();
    assert_eq!(
        published,
        vec![
            ("customer-a".to_owned(), 0),
            ("customer-a".to_owned(), 0),
            ("customer-a".to_owned(), 254),
            ("customer-b".to_owned(), 1),
            ("customer-b".to_owned(), 255),
            ("customer-c".to_owned(), 255),
        ]
    );
    Ok(())
}

fn walk(root: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            found.extend(walk(&path)?);
        } else {
            found.push(path);
        }
    }
    Ok(found)
}
