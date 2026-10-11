use anyhow::{Context, Result, bail};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::Path;

fn field_value(field: &Field) -> Value {
    match field {
        Field::Group(row) => row_value(row),
        Field::ListInternal(list) => Value::Array(list.elements().iter().map(field_value).collect()),
        Field::Null => Value::Null,
        Field::Bool(value) => json!(value),
        Field::Byte(value) => json!(value),
        Field::Short(value) => json!(value),
        Field::Int(value) => json!(value),
        Field::Long(value) if value.unsigned_abs() > 9_007_199_254_740_991 => json!(value.to_string()),
        Field::Long(value) => json!(value),
        Field::UByte(value) => json!(value),
        Field::UShort(value) => json!(value),
        Field::UInt(value) => json!(value),
        Field::ULong(value) => json!(value),
        Field::Float(value) => json!(value),
        Field::Double(value) => json!(value),
        Field::Str(value) => json!(value),
        Field::Bytes(value) => json!(
            value
                .data()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
        other => json!(other.to_string()),
    }
}

fn row_value(row: &parquet::record::Row) -> Value {
    Value::Object(
        row.get_column_iter()
            .map(|(key, field)| (key.clone(), field_value(field)))
            .collect(),
    )
}

/// Read only files selected by delta-rs snapshots, never orphan Parquet files.
///
/// # Errors
/// Rejects absent/corrupt Delta logs, unsupported table layouts, and corrupt data.
pub async fn inspect(directory: &Path) -> Result<Value> {
    let mut directories = fs::read_dir(directory)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    directories.sort();
    let mut tables: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut schemas = Map::new();
    let mut versions = Map::new();
    let mut active_files = Map::new();
    let mut file_namespaces = Map::new();
    let mut files = 0;
    for root in directories.into_iter().filter(|path| path.is_dir()) {
        let table = root
            .file_name()
            .context("missing table name")?
            .to_string_lossy()
            .into_owned();
        let root = fs::canonicalize(root)?;
        let url = url::Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("invalid table path"))?;
        let delta = deltalake_core::open_table(url)
            .await
            .with_context(|| format!("open Delta table {table}"))?;
        let snapshot = delta.snapshot()?;
        if !snapshot.metadata().partition_columns().is_empty() {
            bail!("partitioned inspection is not supported");
        }
        let schema = snapshot.schema();
        schemas.insert(table.clone(), serde_json::to_value(&schema)?);
        versions.insert(table.clone(), json!(snapshot.version()));
        let uris = delta.get_file_uris_by_partitions(&[]).await?;
        active_files.insert(table.clone(), json!(uris.len()));
        let mut rows = Vec::new();
        let mut namespaces_by_file = Map::new();
        for uri in uris {
            let path = if uri.starts_with("file:") {
                url::Url::parse(&uri)?
                    .to_file_path()
                    .map_err(|()| anyhow::anyhow!("non-local data file"))?
            } else if Path::new(&uri).is_absolute() {
                std::path::PathBuf::from(&uri)
            } else {
                bail!("non-local data file URI {uri}");
            };
            let path = fs::canonicalize(path)?;
            if !path.starts_with(&root) {
                bail!("data file escapes table root");
            }
            let reader = SerializedFileReader::new(File::open(&path)?)
                .with_context(|| format!("open Parquet {}", path.display()))?;
            let mut namespaces = BTreeSet::new();
            for row in reader.get_row_iter(None)? {
                let mut value = row_value(&row?);
                let fields = value.as_object_mut().context("expected object row")?;
                if let Some(namespace) = fields.get("_otel_join_namespace").and_then(Value::as_str) {
                    namespaces.insert(namespace.to_string());
                }
                for field in schema.fields() {
                    if !fields.contains_key(field.name()) {
                        if !field.is_nullable() {
                            bail!("required field absent from data: {}", field.name());
                        }
                        fields.insert(field.name().clone(), Value::Null);
                    }
                }
                rows.push(value);
            }
            let relative = path.strip_prefix(&root)?.to_str().context("non-UTF8 data path")?;
            namespaces_by_file.insert(relative.to_string(), json!(namespaces));
            files += 1;
        }
        file_namespaces.insert(table.clone(), Value::Object(namespaces_by_file));
        tables.insert(table, rows);
    }
    if tables.is_empty() {
        bail!("no Delta tables found in {}", directory.display());
    }
    Ok(
        json!({"files": files, "schemas": schemas, "tables": tables, "versions": versions,
            "active_files": active_files, "file_namespaces": file_namespaces}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: binary identifiers contain zero and high-bit bytes.
    /// Guarantees: inspection preserves every byte as hexadecimal.
    #[test]
    fn binary_ids_are_preserved_byte_for_byte() {
        let value = Field::Bytes(parquet::data_type::ByteArray::from(vec![0, 1, 128, 255]));
        assert_eq!(field_value(&value), json!("000180ff"));
    }

    /// Scenario: the requested output directory is absent.
    /// Guarantees: inspection fails rather than returning empty success.
    #[tokio::test]
    async fn missing_directory_is_an_error() {
        let missing = std::env::temp_dir().join(format!("parquet-missing-{}", std::process::id()));
        assert!(inspect(&missing).await.is_err());
    }

    /// Scenario: a directory contains no tables or only raw Parquet without a log.
    /// Guarantees: raw files cannot masquerade as committed Delta data.
    #[tokio::test]
    async fn empty_and_non_delta_are_errors() -> Result<()> {
        let directory = tempfile::tempdir()?;
        assert!(inspect(directory.path()).await.is_err());
        fs::create_dir(directory.path().join("logs"))?;
        fs::write(directory.path().join("logs/broken.parquet"), b"not parquet")?;
        assert!(inspect(directory.path()).await.is_err());
        Ok(())
    }

    /// Scenario: Kernel appends and evolves a table while an uncommitted file is present.
    /// Guarantees: delta-rs selects committed files only and null-fills old rows after evolution.
    #[tokio::test]
    async fn kernel_writer_delta_rs_reader_interoperate() -> Result<()> {
        use arrow::{
            array::{Int32Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use arrow_lake::delta::DeltaTableWriter;
        use std::sync::Arc;
        let directory = tempfile::tempdir()?;
        let root = directory.path().join("generic");
        let mut writer = DeltaTableWriter::open(&root)?;
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )?;
        writer.write(&input).await?;
        writer.flush().await?;
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
        writer.close().await?;
        let active = fs::read_dir(&root)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?
            .into_iter()
            .find(|path| path.extension().is_some_and(|ext| ext == "parquet"))
            .context("missing file")?;
        fs::copy(active, root.join("orphan.parquet"))?;
        let result = inspect(directory.path()).await?;
        assert_eq!(result["files"], 2);
        assert_eq!(result["versions"]["generic"], 3);
        let rows = result["tables"]["generic"].as_array().context("missing rows")?;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows.iter().filter(|row| row["name"].is_null()).count(), 2);
        Ok(())
    }

    /// Scenario: pure Arrow batches contain maximum unsigned values, sub-microsecond epochs, and binary IDs.
    /// Guarantees: independently opened delta-rs snapshots preserve lossless Kernel writer representations.
    #[tokio::test]
    async fn lossless_values_survive_delta_snapshots() -> Result<()> {
        use arrow::{
            array::{ArrayRef, FixedSizeBinaryArray, RecordBatch, TimestampNanosecondArray, UInt64Array},
            datatypes::{DataType, Field, Schema, TimeUnit},
        };
        use arrow_lake::delta::DeltaTableWriter;
        use std::sync::Arc;
        let directory = tempfile::tempdir()?;
        let mut writer = DeltaTableWriter::open(directory.path().join("generic"))?;
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("count", DataType::UInt64, false),
                Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
                Field::new("id", DataType::FixedSizeBinary(2), true),
            ])),
            vec![
                Arc::new(UInt64Array::from(vec![u64::MAX, 0])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![Some(-1), None])),
                Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some([0, 255]), None].into_iter(),
                    2,
                )?),
            ],
        )?;
        writer.write(&input).await?;
        writer.close().await?;
        let result = inspect(directory.path()).await?;
        let rows = result["tables"]["generic"].as_array().context("missing rows")?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["count"], "18446744073709551615.");
        assert_eq!(rows[0]["time"], "1969-12-31 23:59:59.999999 +00:00");
        assert_eq!(rows[0]["time__unix_nanos"], -1);
        assert_eq!(rows[0]["id"], "00ff");
        assert_eq!(rows[1]["time"], Value::Null);
        assert_eq!(rows[1]["time__unix_nanos"], Value::Null);
        assert_eq!(rows[1]["id"], Value::Null);
        Ok(())
    }

    /// Scenario: histogram counts and quantile values use nested Arrow lists.
    /// Guarantees: Delta snapshots retain maximum unsigned counts and nullable list/struct values.
    #[tokio::test]
    async fn nested_metric_values_survive_delta_snapshots() -> Result<()> {
        use arrow::array::{ArrayRef, Float64Array, ListBuilder, RecordBatch, StructArray, UInt64Builder};
        use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use arrow::datatypes::{DataType, Field};
        use arrow_lake::delta::DeltaTableWriter;
        use std::sync::Arc;
        let directory = tempfile::tempdir()?;
        let mut counts = ListBuilder::new(UInt64Builder::new());
        counts.values().append_value(u64::MAX);
        counts.values().append_value(1);
        counts.append(true);
        counts.append(false);
        let quantile_fields: arrow::datatypes::Fields = vec![
            Field::new("quantile", DataType::Float64, false),
            Field::new("value", DataType::Float64, false),
        ]
        .into();
        let values = StructArray::try_new(
            quantile_fields.clone(),
            vec![
                Arc::new(Float64Array::from(vec![0.5])),
                Arc::new(Float64Array::from(vec![42.0])),
            ],
            None,
        )?;
        let quantiles = arrow::array::ListArray::try_new(
            Arc::new(Field::new("item", DataType::Struct(quantile_fields), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 1])),
            Arc::new(values),
            Some(NullBuffer::from(vec![true, false])),
        )?;
        let input = RecordBatch::try_from_iter(vec![
            ("bucket_counts", Arc::new(counts.finish()) as ArrayRef),
            ("quantile_values", Arc::new(quantiles) as ArrayRef),
        ])?;
        let mut writer = DeltaTableWriter::open(directory.path().join("metrics"))?;
        writer.write(&input).await?;
        writer.close().await?;
        let result = inspect(directory.path()).await?;
        let rows = result["tables"]["metrics"].as_array().context("missing rows")?;
        assert_eq!(rows[0]["bucket_counts"][0], "18446744073709551615.");
        assert_eq!(rows[0]["quantile_values"][0]["quantile"], 0.5);
        assert_eq!(rows[0]["quantile_values"][0]["value"], 42.0);
        assert!(rows[1]["bucket_counts"].is_null());
        assert!(rows[1]["quantile_values"].is_null());
        Ok(())
    }
}
