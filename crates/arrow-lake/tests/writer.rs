use anyhow::Result;
#[cfg(feature = "delta")]
use arrow::array::{Array, StringArray, TimestampNanosecondArray, UInt64Array};
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_lake::ParquetFileWriter;
use std::sync::Arc;

fn batch() -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )?)
}

/// Scenario: ordinary Arrow batches are streamed into a standalone Parquet file.
/// Guarantees: finishing preserves schema/rows and refuses overwriting an existing file.
#[tokio::test]
async fn parquet_preserves_schema() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("data.parquet");
    let input = batch()?;
    let mut writer = ParquetFileWriter::create(&path, input.schema())?;
    writer.write(&input).await?;
    writer.write(&input).await?;
    assert_eq!(writer.finish().await?.rows, 4);
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path)?)?;
    assert_eq!(reader.schema().as_ref(), input.schema_ref().as_ref());
    assert!(ParquetFileWriter::create(path, input.schema()).is_err());
    Ok(())
}

/// Scenario: a caller writes a standalone Parquet file using a bare relative filename.
/// Guarantees: completion synchronizes the current directory and succeeds.
#[tokio::test]
async fn parquet_supports_relative_filename() -> Result<()> {
    let path = format!("arrow-lake-relative-{}.parquet", uuid::Uuid::new_v4());
    let _cleanup = tempfile::TempPath::try_from_path(&path)?;
    let input = batch()?;
    let mut writer = ParquetFileWriter::create(&path, input.schema())?;
    writer.write(&input).await?;
    assert_eq!(writer.finish().await?.rows, 2);
    Ok(())
}

/// Scenario: unsupported Delta physical types carry large unsigned and negative timestamp values.
/// Guarantees: decimal widening and timestamp companions retain every input bit.
#[cfg(feature = "delta")]
#[test]
fn lossless_normalization() -> Result<()> {
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("count", DataType::UInt64, false),
            Field::new(
                "time",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
        ])),
        vec![
            Arc::new(UInt64Array::from(vec![u64::MAX, 0])),
            Arc::new(TimestampNanosecondArray::from(vec![Some(-1), None])),
        ],
    )?;
    let output = arrow_lake::normalize::batch(&input)?;
    assert_eq!(output.column(0).data_type(), &DataType::Decimal128(20, 0));
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .ok_or_else(|| anyhow::anyhow!("not decimal"))?
            .value(0),
        i128::from(u64::MAX)
    );
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .ok_or_else(|| anyhow::anyhow!("not timestamp"))?
            .value(0),
        -1
    );
    let nanos = output
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("not nanos"))?;
    assert_eq!(nanos.value(0), -1);
    assert!(nanos.is_null(1));
    Ok(())
}

/// Scenario: dictionaries change key width while preserving logical values.
/// Guarantees: canonical Delta schemas depend on logical types, not transport encodings.
#[cfg(feature = "delta")]
#[test]
fn dictionary_schemas_are_stable() -> Result<()> {
    use arrow::array::{DictionaryArray, Int8Array, Int16Array};
    use arrow::datatypes::{Int8Type, Int16Type};
    let values = Arc::new(StringArray::from(vec!["value"]));
    let first =
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), Arc::clone(&values) as arrow::array::ArrayRef)?;
    let second = DictionaryArray::<Int16Type>::try_new(Int16Array::from(vec![0]), values)?;
    let first = RecordBatch::try_from_iter(vec![("name", Arc::new(first) as arrow::array::ArrayRef)])?;
    let second = RecordBatch::try_from_iter(vec![("name", Arc::new(second) as arrow::array::ArrayRef)])?;
    assert_eq!(
        arrow_lake::normalize::schema(first.schema_ref())?,
        arrow_lake::normalize::schema(second.schema_ref())?
    );
    Ok(())
}

/// Scenario: a nested struct and list contain nullable values and nanosecond timestamps.
/// Guarantees: timestamp companions are recursively added without dropping parent/list null masks.
#[cfg(feature = "delta")]
#[test]
fn nested_values_preserve_nulls() -> Result<()> {
    use arrow::array::{ArrayRef, ListArray, StructArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    let fields: arrow::datatypes::Fields = vec![Field::new(
        "time",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        true,
    )]
    .into();
    let values: ArrayRef = Arc::new(StructArray::try_new(
        fields.clone(),
        vec![Arc::new(TimestampNanosecondArray::from(vec![Some(1_001), None]))],
        Some(NullBuffer::from(vec![true, false])),
    )?);
    let list = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Struct(fields), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
        values,
        Some(NullBuffer::from(vec![true, false])),
    )?;
    let input = RecordBatch::try_from_iter(vec![("items", Arc::new(list) as ArrayRef)])?;
    let output = arrow_lake::normalize::batch(&input)?;
    let list = output
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow::anyhow!("not list"))?;
    assert!(list.is_null(1));
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| anyhow::anyhow!("not struct"))?;
    assert_eq!(values.num_columns(), 2);
    assert!(values.is_null(1));
    Ok(())
}

/// Scenario: callers submit unsupported schema mutations, timestamp overflow, or reserved names.
/// Guarantees: incompatible writes fail explicitly rather than losing data.
#[cfg(feature = "delta")]
#[tokio::test]
async fn rejects_incompatible_writes() -> Result<()> {
    use arrow_lake::delta::DeltaTableWriter;
    let directory = tempfile::tempdir()?;
    let mut writer = DeltaTableWriter::open(directory.path())?;
    writer.write(&batch()?).await?;
    writer.flush().await?;
    let changed = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(StringArray::from(vec!["wrong"])) as arrow::array::ArrayRef,
    )])?;
    assert!(writer.write(&changed).await.is_err());
    let reserved = Schema::new(vec![Field::new("time__unix_nanos", DataType::Int64, true)]);
    assert!(arrow_lake::normalize::schema(&reserved).is_err());
    let overflow = RecordBatch::try_from_iter(vec![(
        "time",
        Arc::new(arrow::array::TimestampSecondArray::from(vec![i64::MAX])) as arrow::array::ArrayRef,
    )])?;
    assert!(arrow_lake::normalize::batch(&overflow).is_err());
    Ok(())
}

/// Scenario: an existing directory has corrupt Delta metadata or only raw Parquet.
/// Guarantees: the writer never replaces history or silently adopts unrelated files.
#[cfg(feature = "delta")]
#[tokio::test]
async fn rejects_corrupt_and_non_delta_tables() -> Result<()> {
    use arrow_lake::delta::DeltaTableWriter;
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("raw.parquet"), b"not delta")?;
    let mut writer = DeltaTableWriter::open(directory.path())?;
    assert!(writer.write(&batch()?).await.is_err());
    drop(writer);
    std::fs::remove_file(directory.path().join("raw.parquet"))?;
    std::fs::create_dir(directory.path().join("_delta_log"))?;
    std::fs::write(
        directory.path().join("_delta_log/00000000000000000000.json"),
        b"bad json",
    )?;
    let mut writer = DeltaTableWriter::open(directory.path())?;
    assert!(writer.write(&batch()?).await.is_err());
    assert_eq!(
        std::fs::read(directory.path().join("_delta_log/00000000000000000000.json"))?,
        b"bad json"
    );
    Ok(())
}

/// Scenario: a generic writer appends, evolves nullable columns, closes, and reopens.
/// Guarantees: Kernel commits readable files and advances versions without overwriting history.
#[cfg(feature = "delta")]
#[tokio::test]
async fn kernel_append_and_evolution() -> Result<()> {
    use arrow_lake::delta::{DeltaTable, DeltaTableWriter};
    let directory = tempfile::tempdir()?;
    let input = batch()?;
    let mut writer = DeltaTableWriter::open(directory.path())?;
    writer.write(&input).await?;
    assert_eq!(
        writer
            .flush()
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing append"))?
            .version,
        1
    );
    assert!(DeltaTable::open(directory.path()).is_err());
    let evolved = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["third"])),
        ],
    )?;
    writer.write(&evolved).await?;
    assert_eq!(
        writer
            .close()
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing append"))?
            .version,
        3
    );
    let mut reopened = DeltaTableWriter::open(directory.path())?;
    reopened.write(&input).await?;
    assert_eq!(
        reopened
            .close()
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing append"))?
            .version,
        4
    );
    Ok(())
}

/// Scenario: a streaming file accepts data and then receives an incompatible batch.
/// Guarantees: failure poisons the file and its previously buffered rows are never committed.
#[cfg(feature = "delta")]
#[tokio::test]
async fn failed_file_cannot_commit_partial_data() -> Result<()> {
    use arrow_lake::delta::DeltaTable;
    let directory = tempfile::tempdir()?;
    let table = DeltaTable::open(directory.path())?;
    let input = batch()?;
    let mut writer = table.new_file(input.schema())?;
    writer.write(&input).await?;
    let wrong = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(StringArray::from(vec!["wrong"])) as arrow::array::ArrayRef,
    )])?;
    assert!(writer.write(&wrong).await.is_err());
    assert!(writer.write(&input).await.is_err());
    assert!(writer.finish().await.is_err());
    assert_eq!(table.version()?, 0);
    Ok(())
}

/// Scenario: input omits a required column while proposing a nullable schema addition.
/// Guarantees: rejection occurs before any metadata evolution or append is committed.
#[cfg(feature = "delta")]
#[tokio::test]
async fn missing_required_columns_do_not_evolve_table() -> Result<()> {
    use arrow_lake::delta::DeltaTable;
    let directory = tempfile::tempdir()?;
    let table = DeltaTable::open(directory.path())?;
    let input = batch()?;
    let mut writer = table.new_file(input.schema())?;
    writer.write(&input).await?;
    writer.finish().await?;
    let incoming = Arc::new(Schema::new(vec![Field::new("new_column", DataType::Utf8, true)]));
    assert!(table.new_file(incoming).is_err());
    assert_eq!(table.version()?, 1);
    Ok(())
}

/// Scenario: a convenience writer sees only empty Arrow batches and flush requests.
/// Guarantees: no Delta log or data files are created and flush reports no append.
#[cfg(feature = "delta")]
#[tokio::test]
async fn empty_batches_do_not_create_tables() -> Result<()> {
    use arrow_lake::delta::DeltaTableWriter;
    let directory = tempfile::tempdir()?;
    let mut writer = DeltaTableWriter::open(directory.path())?;
    writer.write(&RecordBatch::new_empty(batch()?.schema())).await?;
    assert!(writer.flush().await?.is_none());
    assert!(writer.close().await?.is_none());
    assert!(!directory.path().join("_delta_log").exists());
    Ok(())
}

/// Scenario: a map's unsigned values need widening and its Arrow key/value names differ from Delta.
/// Guarantees: canonical map names, null masks, and maximum values survive a real Kernel commit.
#[cfg(feature = "delta")]
#[tokio::test]
async fn maps_round_trip_through_kernel() -> Result<()> {
    use arrow::array::{ArrayRef, Decimal128Array, MapArray, MapBuilder, StringBuilder, UInt64Builder};
    use arrow_lake::delta::DeltaTableWriter;
    let directory = tempfile::tempdir()?;
    let mut map = MapBuilder::new(None, StringBuilder::new(), UInt64Builder::new());
    map.keys().append_value("counter");
    map.values().append_value(u64::MAX);
    map.append(true)?;
    map.append(false)?;
    let input = RecordBatch::try_from_iter(vec![("values", Arc::new(map.finish()) as ArrayRef)])?;
    let mut writer = DeltaTableWriter::open(directory.path())?;
    writer.write(&input).await?;
    let append = writer.close().await?.ok_or_else(|| anyhow::anyhow!("missing append"))?;
    let mut reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(append.file.path)?)?
            .build()?;
    let output = reader.next().ok_or_else(|| anyhow::anyhow!("missing rows"))??;
    let map = output
        .column(0)
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| anyhow::anyhow!("not map"))?;
    assert!(map.is_null(1));
    let values = map
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| anyhow::anyhow!("not decimal"))?;
    assert_eq!(values.value(0), i128::from(u64::MAX));
    Ok(())
}

/// Scenario: a second batch uses a different logical type with the same normalized physical type.
/// Guarantees: logical changes are rejected before partial buffered data can be committed.
#[cfg(feature = "delta")]
#[tokio::test]
async fn same_physical_type_does_not_hide_logical_changes() -> Result<()> {
    use arrow::array::{ArrayRef, UInt16Array};
    use arrow_lake::delta::DeltaTable;
    let directory = tempfile::tempdir()?;
    let input = RecordBatch::try_from_iter(vec![("value", Arc::new(UInt16Array::from(vec![1])) as ArrayRef)])?;
    let changed = RecordBatch::try_from_iter(vec![("value", Arc::new(Int32Array::from(vec![2])) as ArrayRef)])?;
    let table = DeltaTable::open(directory.path())?;
    let mut writer = table.new_file(input.schema())?;
    writer.write(&input).await?;
    assert!(writer.write(&changed).await.is_err());
    assert!(writer.finish().await.is_err());
    assert_eq!(table.version()?, 0);
    Ok(())
}
