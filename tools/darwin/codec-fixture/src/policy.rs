use anyhow::{Context, Result, bail, ensure};
use arrow::datatypes::SchemaRef;
use arrowcol_tunables::{ParquetCodec, Tunables};
use parquet::arrow::ArrowSchemaConverter;
use parquet::basic::Compression;
use serde_json::Value;

#[derive(Debug)]
pub struct Selection {
    pub coordinate: String,
    pub expected_codecs: Vec<String>,
}

fn object_at<'a>(value: &'a Value, path: &[&str]) -> Result<&'a serde_json::Map<String, Value>> {
    let mut current = value;
    for key in path {
        current = current.get(*key).with_context(|| format!("missing {key}"))?;
    }
    current.as_object().context("expected object")
}

fn writer_config(value: &Value, schema: &SchemaRef) -> Result<(Tunables, Vec<String>)> {
    let tunables = value.get("tunables").context("missing tunables")?;
    let parsed = Tunables::parse_oat_json(tunables.clone())?;
    let writer = parsed.parquet_writer.as_ref().context("missing parquet_writer")?;
    ensure!(
        writer.defaults.compression != Some(ParquetCodec::Lz4),
        "legacy LZ4 is excluded"
    );
    ensure!(
        writer
            .columns
            .values()
            .all(|column| column.compression != Some(ParquetCodec::Lz4)),
        "legacy LZ4 is excluded"
    );
    let properties = writer.writer_properties(schema, None, &[])?;
    let descriptor = ArrowSchemaConverter::new().convert(schema)?;
    let codecs = descriptor
        .columns()
        .iter()
        .map(|column| {
            let family = match properties.compression(column.path()) {
                Compression::UNCOMPRESSED => "UNCOMPRESSED",
                Compression::SNAPPY => "SNAPPY",
                Compression::GZIP(_) => "GZIP",
                Compression::BROTLI(_) => "BROTLI",
                Compression::LZ4_RAW => "LZ4_RAW",
                Compression::ZSTD(_) => "ZSTD",
                other => bail!("unsupported codec {other:?}"),
            };
            Ok(family.to_owned())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((parsed, codecs))
}

pub fn check(baseline: &Value, candidate: &Value, schema: &SchemaRef) -> Result<Selection> {
    let (_, _) = writer_config(baseline, schema)?;
    let (_, codecs) = writer_config(candidate, schema)?;
    let path = ["tunables", "parquet_writer", "defaults"];
    let before = object_at(baseline, &path)?;
    let after = object_at(candidate, &path)?;
    let mut restored = candidate.clone();
    let defaults = restored
        .pointer_mut("/tunables/parquet_writer/defaults")
        .and_then(Value::as_object_mut)
        .context("invalid defaults")?;
    for key in ["compression", "compression_level"] {
        match before.get(key) {
            Some(value) => {
                defaults.insert(key.to_owned(), value.clone());
            }
            None => {
                defaults.remove(key);
            }
        }

        #[cfg(test)]
        mod tests {
            use super::*;
            use serde_json::json;

            fn schema() -> SchemaRef {
                collector_chain::dmv::tables()
                    .unwrap()
                    .into_iter()
                    .find(|table| table.identity().table().as_str() == "sqlserver_availability_replica_states")
                    .unwrap()
                    .schema()
                    .clone()
            }
            fn baseline() -> Value {
                json!({"tunables":{"parquet_writer":{"defaults":{"compression":"zstd","compression_level":1}}}})
            }
            #[test]
            fn codec_and_dictionary_coordinates_use_registry_and_physical_schema() {
                let base = baseline();
                for (codec, level, family) in [
                    ("snappy", Value::Null, "SNAPPY"),
                    ("lz4_raw", Value::Null, "LZ4_RAW"),
                    ("zstd", json!(6), "ZSTD"),
                    ("gzip", json!(9), "GZIP"),
                    ("brotli", json!(11), "BROTLI"),
                    ("uncompressed", Value::Null, "UNCOMPRESSED"),
                ] {
                    let mut candidate = base.clone();
                    candidate["tunables"]["parquet_writer"]["defaults"] =
                        json!({"compression":codec,"compression_level":level});
                    let selection = check(&base, &candidate, &schema()).unwrap();
                    assert_eq!(selection.coordinate, "codec");
                    assert!(selection.expected_codecs.iter().all(|value| value == family));
                }
                let mut candidate = base.clone();
                candidate["tunables"]["parquet_writer"]["columns"] =
                    json!({"customer_id":{"dictionary_enabled":false}});
                assert_eq!(
                    check(&base, &candidate, &schema()).unwrap().coordinate,
                    "dictionary:customer_id"
                );
            }

            #[test]
            fn rejects_invalid_and_multi_coordinate_candidates() {
                let base = baseline();
                for defaults in [
                    json!({"compression":"zstdd","compression_level":1}),
                    json!({"compression":"zstd","compression_level":23}),
                    json!({"compression":"snappy","compression_level":1}),
                    json!({"compression":"lz4","compression_level":null}),
                    json!({"compression":"gzip","compression_level":10}),
                    json!({"compression":"brotli","compression_level":-1}),
                    json!({"compression":"zstd","compression_level":"1"}),
                    json!({"compression":"zstd","compression_level":1,"misspelled":true}),
                ] {
                    let mut candidate = base.clone();
                    candidate["tunables"]["parquet_writer"]["defaults"] = defaults;
                    assert!(check(&base, &candidate, &schema()).is_err());
                }
                for columns in [
                    json!({"missing":{"dictionary_enabled":false}}),
                    json!({"customer_id":{"dictionary_enabled":"false"}}),
                    json!({"customer_id":{"dictionary_enabled":false,"encoding":"plain"}}),
                    json!({"customer_id":{"dictionary_enabled":false},"endpoint_url":{"dictionary_enabled":false}}),
                    json!([]),
                ] {
                    let mut candidate = base.clone();
                    candidate["tunables"]["parquet_writer"]["columns"] = columns;
                    assert!(check(&base, &candidate, &schema()).is_err());
                }
                let mut candidate = base.clone();
                candidate["tunables"]["parquet_writer"]["defaults"]["compression_level"] = json!(2);
                candidate["tunables"]["parquet_writer"]["columns"] =
                    json!({"customer_id":{"dictionary_enabled":false}});
                assert!(check(&base, &candidate, &schema()).is_err());
                candidate = base.clone();
                candidate["extra"] = json!(true);
                assert!(check(&base, &candidate, &schema()).is_err());
            }
        }
    }
    if restored == *baseline {
        let changed = before.get("compression") != after.get("compression")
            || before.get("compression_level") != after.get("compression_level");
        return Ok(Selection {
            coordinate: if changed { "codec" } else { "control" }.into(),
            expected_codecs: codecs,
        });
    }

    let before_columns = object_at(baseline, &["tunables", "parquet_writer"])?.get("columns");
    let after_columns = object_at(candidate, &["tunables", "parquet_writer"])?.get("columns");
    let empty = serde_json::Map::new();
    let before_columns = match before_columns {
        Some(v) => v.as_object().context("baseline columns must be an object")?,
        None => &empty,
    };
    let after_columns = match after_columns {
        Some(v) => v.as_object().context("candidate columns must be an object")?,
        None => &empty,
    };
    let keys: std::collections::BTreeSet<_> = before_columns.keys().chain(after_columns.keys()).collect();
    let changed: Vec<_> = keys
        .into_iter()
        .filter(|key| before_columns.get(*key) != after_columns.get(*key))
        .collect();
    ensure!(changed.len() == 1, "exactly one dictionary coordinate may change");
    let name = changed[0];
    let descriptor = ArrowSchemaConverter::new().convert(schema)?;
    ensure!(
        descriptor
            .columns()
            .iter()
            .any(|column| column.path().string() == *name),
        "unknown physical column"
    );
    let empty_column = serde_json::Map::new();
    let before_column = match before_columns.get(name) {
        Some(v) => v.as_object().context("baseline column must be an object")?,
        None => &empty_column,
    };
    let after_column = after_columns
        .get(name)
        .and_then(Value::as_object)
        .context("candidate column must be an object")?;
    ensure!(
        matches!(after_column.get("dictionary_enabled"), Some(Value::Bool(_))),
        "dictionary_enabled must be boolean"
    );
    let mut column_restored = after_column.clone();
    match before_column.get("dictionary_enabled") {
        Some(value) => {
            column_restored.insert("dictionary_enabled".into(), value.clone());
        }
        None => {
            column_restored.remove("dictionary_enabled");
        }
    }
    ensure!(&column_restored == before_column, "only dictionary_enabled may change");
    let mut restored = candidate.clone();
    let writer = restored
        .pointer_mut("/tunables/parquet_writer")
        .and_then(Value::as_object_mut)
        .context("invalid writer")?;
    match object_at(baseline, &["tunables", "parquet_writer"])?.get("columns") {
        Some(value) => {
            writer.insert("columns".into(), value.clone());
        }
        None => {
            writer.remove("columns");
        }
    }
    ensure!(restored == *baseline, "changes outside dictionary coordinate");
    Ok(Selection {
        coordinate: format!("dictionary:{name}"),
        expected_codecs: codecs,
    })
}
