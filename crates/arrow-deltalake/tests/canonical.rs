#![cfg(feature = "delta")]

//! Canonical (already normalized) schemas: a table's Kernel schema can feed another
//! table without re-normalization, keeping every timestamp companion exactly.

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, UInt8Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use arrow_deltalake::delta::DeltaTable;
use arrow_deltalake::normalize::{self, CanonicalSchema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use url::Url;

fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread().enable_all().build()?)
}

fn url(path: &Path) -> Result<Url> {
    Url::from_directory_path(path).map_err(|()| anyhow::anyhow!("directory url"))
}

fn utc_micros() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

/// A raw collector-shaped batch: sub-microsecond and negative nanoseconds, a nested
/// struct timestamp, a list of structs with timestamps, and an unsigned column.
fn raw_batch() -> Result<RecordBatch> {
    let nested_fields: Fields = vec![
        Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("n", DataType::UInt8, true),
    ]
    .into();
    let nested = StructArray::try_new(
        nested_fields.clone(),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![5, 6, -7]).with_timezone("UTC")),
            Arc::new(UInt8Array::from(vec![Some(255), None, Some(1)])),
        ],
        Some(NullBuffer::from(vec![true, false, true])),
    )?;
    let item_fields: Fields = vec![Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true)].into();
    let items = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Struct(item_fields.clone()), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2, 3])),
        Arc::new(StructArray::try_new(
            item_fields,
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_999),
                None,
                Some(-1),
            ]))],
            None,
        )?),
        Some(NullBuffer::from(vec![true, false, true])),
    )?;
    Ok(RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "sample_time_utc",
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_793_000_000_123_456_789),
                None,
                Some(-1_001),
            ])),
        ),
        ("nested", Arc::new(nested)),
        ("items", Arc::new(items)),
    ])?)
}

fn read_file(path: &Path, schema: &SchemaRef) -> Result<RecordBatch> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?.build()?;
    let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    let batch = arrow::compute::concat_batches(&batches.first().context("no batch")?.schema(), &batches)?;
    // Re-label with the Kernel schema; this fails if any physical type differs.
    Ok(RecordBatch::try_new(Arc::clone(schema), batch.columns().to_vec())?)
}

/// Scenario: a table written from raw batches is read back with its Kernel schema and
/// republished to a second table through the canonical API (the tenant sink seam).
/// Guarantees: the full schema (companions, original types, nullability) and every
/// value, including sub-microsecond companion nanos, survive unchanged; raw APIs still
/// reject the reserved companion names.
#[test]
fn kernel_schema_round_trips_through_canonical_writer() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let sink_root = tempfile::tempdir()?;
    let raw = raw_batch()?;
    let source = DeltaTable::open_url(url(source_root.path())?, raw.schema(), vec![])?;
    let published = runtime()?.block_on(async {
        let mut file = source.new_file(raw.schema())?;
        file.write(&raw).await?;
        file.finish().await
    })?;
    let kernel = source.arrow_schema()?;
    assert_eq!(kernel.fields(), normalize::schema(raw.schema_ref())?.fields());
    assert!(kernel.field_with_name("sample_time_utc__unix_nanos").is_ok());
    let canonical = CanonicalSchema::try_new(Arc::clone(&kernel))?;
    let frozen = read_file(&published.file.path, &kernel)?;

    let sink = DeltaTable::open_url_canonical(url(sink_root.path())?, &canonical, vec![])?;
    let copied = runtime()?.block_on(async {
        let mut file = sink.new_canonical_file_partitioned_with_properties(
            &canonical,
            BTreeMap::new(),
            WriterProperties::builder().build(),
        )?;
        file.write(&frozen).await?;
        file.finish().await
    })?;
    assert_eq!(sink.arrow_schema()?, kernel, "full sink schema");
    let output = read_file(&copied.file.path, &kernel)?;
    assert_eq!(output.num_rows(), 3);
    for (index, field) in kernel.fields().iter().enumerate() {
        assert_eq!(
            output.column(index).to_data(),
            frozen.column(index).to_data(),
            "column {}",
            field.name()
        );
    }
    let nanos = output
        .column(kernel.index_of("sample_time_utc__unix_nanos")?)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("companion")?;
    assert_eq!(nanos.value(0), 1_793_000_000_123_456_789);
    assert!(nanos.is_null(1));
    assert_eq!(nanos.value(2), -1_001);

    // Raw entry points keep rejecting reserved companion names.
    let raw_open = tempfile::tempdir()?;
    let error = DeltaTable::open_url(url(raw_open.path())?, Arc::clone(&kernel), vec![])
        .err()
        .context("raw open accepted a canonical schema")?;
    assert!(format!("{error:#}").contains("reserved timestamp companion name"));
    assert!(normalize::schema(&kernel).is_err());
    assert!(normalize::batch(&frozen).is_err());
    assert!(sink.new_file(Arc::clone(&kernel)).is_err());
    Ok(())
}

fn ts_pair(name: &str, nullable: bool) -> [Field; 2] {
    [
        Field::new(name, utc_micros(), nullable),
        Field::new(format!("{name}__unix_nanos"), DataType::Int64, nullable),
    ]
}

fn reject(fields: Vec<Field>, reason: &str) {
    let result = CanonicalSchema::try_new(Arc::new(Schema::new(fields)));
    assert!(result.is_err(), "accepted {reason}");
}

/// Scenario: schemas that are not a fixed point of normalization.
/// Guarantees: each is refused, so the canonical path cannot smuggle raw types or
/// unpaired reserved names past normalization.
#[test]
fn non_canonical_schemas_are_rejected() -> Result<()> {
    let [ts, nanos] = ts_pair("t", true);
    CanonicalSchema::try_new(Arc::new(Schema::new(vec![ts.clone(), nanos.clone()])))?;
    reject(vec![nanos.clone()], "orphan companion");
    reject(vec![nanos.clone(), ts.clone()], "companion before timestamp");
    reject(vec![ts.clone()], "timestamp without companion");
    reject(
        vec![ts.clone(), nanos.clone().with_nullable(false)],
        "companion nullability differs",
    );
    reject(
        vec![ts.clone(), Field::new("t__unix_nanos", DataType::Int32, true)],
        "companion type differs",
    );
    reject(
        vec![
            Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            nanos.clone(),
        ],
        "raw nanosecond timestamp",
    );
    reject(
        vec![
            Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            nanos.clone(),
        ],
        "timezone-less timestamp",
    );
    reject(vec![Field::new("u", DataType::UInt8, true)], "unsigned");
    reject(
        vec![Field::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        )],
        "dictionary",
    );
    reject(
        vec![Field::new("s", DataType::Struct(vec![nanos.clone()].into()), true)],
        "nested orphan companion",
    );
    reject(
        vec![Field::new("s", DataType::Struct(vec![ts.clone()].into()), true)],
        "nested timestamp without companion",
    );
    // Raw normalization is unchanged.
    let dictionary = DictionaryArray::<arrow::datatypes::Int8Type>::try_new(
        Int8Array::from(vec![0]),
        Arc::new(StringArray::from(vec!["v"])) as ArrayRef,
    )?;
    assert!(
        normalize::batch(&RecordBatch::try_from_iter(vec![(
            "d",
            Arc::new(dictionary) as ArrayRef
        )])?)
        .is_ok()
    );
    assert!(normalize::schema(&Schema::new(vec![nanos])).is_err());
    Ok(())
}

fn pair_batch(micros: Vec<Option<i64>>, nanos: Vec<Option<i64>>) -> Result<RecordBatch> {
    let [ts, companion] = ts_pair("t", true);
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![ts, companion])),
        vec![
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone("UTC")),
            Arc::new(Int64Array::from(nanos)),
        ],
    )?)
}

/// Scenario: a canonical writer receives batches whose companion disagrees with its
/// timestamp, or whose schema is raw.
/// Guarantees: the write fails, the writer refuses further writes, and nothing commits.
#[test]
fn canonical_writer_rejects_inconsistent_batches() -> Result<()> {
    for (case, batch) in [
        ("value", pair_batch(vec![Some(1)], vec![Some(5_000)])?),
        ("null mask", pair_batch(vec![None], vec![Some(1_000)])?),
        ("negative rounding", pair_batch(vec![Some(0)], vec![Some(-1)])?),
        (
            "raw schema",
            RecordBatch::try_from_iter(vec![(
                "t",
                Arc::new(TimestampNanosecondArray::from(vec![1])) as ArrayRef,
            )])?,
        ),
    ] {
        let root = tempfile::tempdir()?;
        let canonical = CanonicalSchema::try_new(pair_batch(vec![], vec![])?.schema())?;
        let table = DeltaTable::open_url_canonical(url(root.path())?, &canonical, vec![])?;
        let version = table.version()?;
        runtime()?.block_on(async {
            let mut file = table.new_canonical_file_partitioned_with_properties(
                &canonical,
                BTreeMap::new(),
                WriterProperties::builder().build(),
            )?;
            assert!(file.write(&batch).await.is_err(), "accepted inconsistent {case}");
            let good = pair_batch(vec![Some(-1)], vec![Some(-1)])?;
            assert!(file.write(&good).await.is_err(), "writer reused after failure ({case})");
            anyhow::Ok(())
        })?;
        assert_eq!(table.version()?, version, "{case} committed");
    }
    let accepted = normalize::canonical_batch(&pair_batch(vec![Some(-1), None], vec![Some(-1), None])?)?;
    assert_eq!(accepted.num_rows(), 2);
    Ok(())
}
