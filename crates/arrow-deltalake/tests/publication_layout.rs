#![cfg(feature = "delta")]

use anyhow::{Context, Result};
use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::DeltaTable;
use arrow_deltalake::layout::LayoutPolicy;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::BTreeSet;
use std::fs::File;
use std::sync::Arc;

fn input(rows: usize) -> Result<RecordBatch> {
    let customers = (0..rows).map(|row| format!("c{:02}", row % 16)).collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("id", DataType::Int32, false),
        ])),
        vec![
            Arc::new(StringArray::from(customers)),
            Arc::new(Int32Array::from((0..i32::try_from(rows)?).collect::<Vec<_>>())),
        ],
    )?)
}

/// Scenario: one real publication spans multiple interleaved customer batches.
/// Guarantees: global file sorting preserves every row and prunes 15 of 16 groups;
/// the same unsorted publication layout cannot prune any group by customer bounds.
#[tokio::test]
async fn publication_scope_sorting_preserves_rows_and_qualifies_source_statistics() -> Result<()> {
    let root = tempfile::tempdir()?;
    let batch = input(8192)?;
    let policy = LayoutPolicy {
        row_group_rows: 512,
        page_rows: 128,
        ..LayoutPolicy::default()
    };
    for (name, expected_groups) in [("sorted", 1), ("unsorted", 16)] {
        let table = DeltaTable::open(root.path().join(name))?;
        let mut writer = if name == "sorted" {
            table.new_file_with_publication_and_layout(
                batch.schema(),
                format!("layout-{name}"),
                0,
                1,
                Some(policy.clone()),
            )?
        } else {
            table
                .new_file_partitioned_with_properties(
                    batch.schema(),
                    Default::default(),
                    policy.writer_properties(&batch.schema())?,
                )?
                .with_transaction(format!("layout-{name}"), 0)?
                .with_ingestion_time(1)?
        };
        writer.write(&batch.slice(0, 4103)).await?;
        writer.write(&batch.slice(4103, 4089)).await?;
        assert_eq!(writer.rows_written(), 8192);
        assert!(writer.input_bytes_written() > 0);
        let result = writer.finish().await?;
        assert_eq!(result.file.rows, 8192);
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&result.file.path)?)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 16);
        let mut matching = 0;
        for group in metadata.row_groups() {
            let statistics = group.column(0).statistics().context("customer statistics")?;
            if statistics.min_bytes_opt().is_some_and(|min| min <= b"c07".as_slice())
                && statistics.max_bytes_opt().is_some_and(|max| max >= b"c07".as_slice())
            {
                matching += 1;
            }
        }
        assert_eq!(matching, expected_groups);
        let mut ids = BTreeSet::new();
        let mut last = String::new();
        for rows in reader.build()? {
            let rows = rows?;
            let customers = rows
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("customers")?;
            let values = rows.column(1).as_any().downcast_ref::<Int32Array>().context("ids")?;
            for row in 0..rows.num_rows() {
                if name == "sorted" {
                    assert!(last.as_str() <= customers.value(row));
                    last = customers.value(row).to_owned();
                }
                assert!(ids.insert(values.value(row)));
            }
        }
        assert_eq!(ids, (0..8192).collect());
    }
    Ok(())
}

/// Scenario: an individual publication cannot fit its configured whole-file sort budget.
/// Guarantees: backpressure is visible and a partially accepted file cannot become a commit.
#[tokio::test]
async fn oversized_sorted_publication_fails_without_partial_commit() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open(root.path())?;
    let batch = input(2)?;
    let policy = LayoutPolicy {
        max_rows: 1,
        ..LayoutPolicy::default()
    };
    let mut writer =
        table.new_file_with_publication_and_layout(batch.schema(), "bounded-layout".into(), 0, 1, Some(policy))?;
    assert!(writer.write(&batch).await.is_err());
    assert_eq!(writer.rows_written(), 0);
    assert!(writer.stage().await.is_err());
    assert_eq!(table.version()?, 0);
    Ok(())
}
