use anyhow::{Context, Result, bail};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

fn parquet_files(directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory).with_context(|| format!("read {}", directory.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            parquet_files(&path, files)?;
        } else if path.extension().is_some_and(|extension| extension == "parquet") {
            files.push(path);
        }
    }
    Ok(())
}

fn field_value(field: &Field) -> Value {
    match field {
        Field::Group(row) => row_value(row),
        Field::ListInternal(list) => Value::Array(list.elements().iter().map(field_value).collect()),
        Field::Null => Value::Null,
        Field::Bool(value) => json!(value),
        Field::Byte(value) => json!(value),
        Field::Short(value) => json!(value),
        Field::Int(value) => json!(value),
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

fn inspect(directory: &Path) -> Result<Value> {
    let mut files = Vec::new();
    parquet_files(directory, &mut files)?;
    files.sort();
    if files.is_empty() {
        bail!("no Parquet files found in {}", directory.display());
    }
    let mut tables: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut schemas = Map::new();
    for path in &files {
        let relative = path.strip_prefix(directory)?;
        let table = relative
            .components()
            .next()
            .context("missing table directory")?
            .as_os_str()
            .to_string_lossy()
            .into_owned();
        let reader =
            SerializedFileReader::new(File::open(path)?).with_context(|| format!("open Parquet {}", path.display()))?;
        schemas.insert(
            table.clone(),
            json!(
                reader
                    .metadata()
                    .file_metadata()
                    .schema_descr()
                    .columns()
                    .iter()
                    .map(|column| column.path().string())
                    .collect::<Vec<_>>()
            ),
        );
        for row in reader.get_row_iter(None)? {
            tables.entry(table.clone()).or_default().push(row_value(&row?));
        }
    }
    Ok(json!({"files": files.len(), "schemas": schemas, "tables": tables}))
}

fn main() -> Result<()> {
    let directory = std::env::args()
        .nth(1)
        .context("usage: parquet-inspect <onelake directory>")?;
    println!("{}", serde_json::to_string(&inspect(Path::new(&directory))?)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_ids_are_preserved_byte_for_byte() {
        let value = Field::Bytes(parquet::data_type::ByteArray::from(vec![0, 1, 128, 255]));
        assert_eq!(field_value(&value), json!("000180ff"));
    }

    #[test]
    fn missing_directory_is_an_error() {
        let missing = std::env::temp_dir().join(format!("parquet-missing-{}", std::process::id()));
        assert!(inspect(&missing).is_err());
    }

    #[test]
    fn empty_and_corrupt_parquet_are_errors() -> Result<()> {
        let directory = std::env::temp_dir().join(format!("parquet-test-{}", std::process::id()));
        fs::create_dir(&directory)?;
        let result = (|| -> Result<()> {
            assert!(inspect(&directory).is_err());
            fs::write(directory.join("broken.parquet"), b"not parquet")?;
            assert!(inspect(&directory).is_err());
            Ok(())
        })();
        fs::remove_file(directory.join("broken.parquet"))?;
        fs::remove_dir(&directory)?;
        result
    }
}
