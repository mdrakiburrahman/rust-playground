use anyhow::{Context, Result, bail};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

mod cloud;
mod corpus;
pub use cloud::{cloud_sample_corpus, cloud_sample_rows, summarize_cloud};
use corpus::CorpusBuilder;
pub use corpus::{MAX_CHUNK_DISTINCT_ROWS, MAX_CORPUS_DISTINCT_ROWS, ValidationChunk, ValidationCorpus};

type PartitionValues = BTreeMap<String, Option<String>>;

fn row_digest(value: &Value) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        hex::encode(Sha256::digest(serde_json::to_vec(value)?))
    ))
}

fn payload_digest(mut value: Value, partition_columns: &[String]) -> Result<String> {
    let fields = value.as_object_mut().context("expected object row")?;
    for column in partition_columns {
        fields.remove(column);
    }
    row_digest(&value)
}

fn snapshot_partitions(delta: &deltalake_core::DeltaTable) -> Result<BTreeMap<String, PartitionValues>> {
    let mut files = BTreeMap::new();
    let snapshot = delta.snapshot()?;
    for file in snapshot.log_data().iter() {
        if file.deletion_vector_descriptor().is_some() {
            bail!("independent Parquet oracle does not support deletion vectors");
        }
        let partitions: PartitionValues = file.partition_values_map().into_iter().collect();
        validate_partition_columns(&partitions, snapshot.metadata().partition_columns())?;
        files.insert(file.path().into_owned(), partitions);
    }
    Ok(files)
}

fn validate_partition_columns(partitions: &PartitionValues, expected: &[String]) -> Result<()> {
    if partitions.len() != expected.len() || expected.iter().any(|column| !partitions.contains_key(column)) {
        bail!("committed Add partition values do not match Delta metadata");
    }
    Ok(())
}

fn with_partitions(mut row: Value, schema: &Value, partitions: &PartitionValues) -> Result<Value> {
    let fields = row.as_object_mut().context("expected object row")?;
    for (name, raw) in partitions {
        let definition = schema["fields"]
            .as_array()
            .context("missing Delta fields")?
            .iter()
            .find(|field| field["name"].as_str() == Some(name))
            .context("partition column absent from schema")?;
        let value = match raw {
            None => Value::Null,
            Some(raw) => match definition["type"].as_str() {
                Some("string" | "date") => json!(raw),
                Some("boolean") => json!(raw.parse::<bool>()?),
                Some("byte" | "short" | "integer" | "long") => {
                    let number = raw.parse::<i64>()?;
                    if number.unsigned_abs() > 9_007_199_254_740_991 {
                        json!(raw)
                    } else {
                        json!(number)
                    }
                }
                other => bail!("unsupported independent oracle partition type {other:?}"),
            },
        };
        if value.is_null() && definition["nullable"] == false {
            bail!("required partition column is null");
        }
        if let Some(physical) = fields.get(name) {
            if physical != &value {
                bail!("physical partition value differs from committed Add action");
            }
        } else {
            fields.insert(name.clone(), value);
        }
    }
    Ok(row)
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
        let schema = snapshot.schema();
        let schema_json = serde_json::to_value(&schema)?;
        let partitions = snapshot_partitions(&delta)?;
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
            let relative = path.strip_prefix(&root)?.to_str().context("non-UTF8 data path")?;
            let partition_values = partitions
                .get(relative)
                .context("active file lacks committed partition values")?;
            let reader = SerializedFileReader::new(File::open(&path)?)
                .with_context(|| format!("open Parquet {}", path.display()))?;
            let mut namespaces = BTreeSet::new();
            for row in reader.get_row_iter(None)? {
                let mut value = with_partitions(row_value(&row?), &schema_json, partition_values)?;
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

/// Snapshot-selected summary for an independent Delta root.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotSummary {
    pub files: usize,
    pub tables: BTreeMap<String, TableSummary>,
}

/// One Delta table's schema, versions, active files, rows, bytes, and row multiset.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TableSummary {
    pub schema: Value,
    pub version: u64,
    pub active_files: usize,
    pub rows: u64,
    pub bytes: u64,
    pub minimum_file_bytes: u64,
    pub maximum_file_bytes: u64,
    pub row_hashes: BTreeMap<String, u64>,
    #[serde(default)]
    pub payload_row_hashes: BTreeMap<String, u64>,
    pub files: Vec<FileSummary>,
    #[serde(default)]
    pub partition_columns: Vec<String>,
}

/// One active Delta data file.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileSummary {
    pub path: String,
    pub bytes: u64,
    pub rows: u64,
    #[serde(default)]
    pub partition_values: PartitionValues,
}

/// Committed file names and schemas without reading or hashing Parquet rows.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotInventory {
    pub tables: BTreeMap<String, TableInventory>,
}

/// One independent Delta snapshot selected for incremental ingestion.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TableInventory {
    pub schema: Value,
    pub version: u64,
    pub files: Vec<String>,
}

/// Reads one reference row from a file selected by a captured full summary.
///
/// # Errors
///
/// Rejects unsafe paths, missing/corrupt data, and empty snapshots.
pub fn sample_row(directory: &Path, identity: &str, summary: &TableSummary) -> Result<Value> {
    let root = table_root(directory, identity)?;
    let file = summary.files.first().context("snapshot has no data files")?;
    let path = local_active_file(&root, root.join(&file.path).to_str().context("non-UTF8 data path")?)?;
    let reader = SerializedFileReader::new(File::open(path)?)?;
    let row = reader
        .get_row_iter(None)?
        .next()
        .context("snapshot file has no rows")??;
    with_partitions(row_value(&row), &summary.schema, &file.partition_values)
}

/// Reads the complete bounded row multiset from the captured active files.
///
/// # Errors
///
/// Rejects unsafe paths, changed row counts/hashes, and more than 256 distinct
/// rows. This is a validation oracle for the synthetic corpus, not a bulk API.
pub fn sample_rows(directory: &Path, identity: &str, summary: &TableSummary) -> Result<Vec<(Value, u64)>> {
    let root = table_root(directory, identity)?;
    let mut distinct = BTreeMap::<String, (Value, u64)>::new();
    let mut hashes = BTreeMap::<String, u64>::new();
    let mut rows = 0_u64;
    for file in &summary.files {
        let path = local_active_file(&root, root.join(&file.path).to_str().context("non-UTF8 data path")?)?;
        let reader = SerializedFileReader::new(File::open(path)?)?;
        for row in reader.get_row_iter(None)? {
            let value = with_partitions(row_value(&row?), &summary.schema, &file.partition_values)?;
            let encoded = serde_json::to_string(&value)?;
            let digest = format!("sha256:{}", hex::encode(Sha256::digest(encoded.as_bytes())));
            *hashes.entry(digest).or_default() += 1;
            let entry = distinct.entry(encoded).or_insert((value, 0));
            entry.1 = entry.1.checked_add(1).context("row multiplicity overflow")?;
            rows = rows.checked_add(1).context("row count overflow")?;
            if distinct.len() > 256 {
                bail!("validation corpus exceeds 256 distinct rows");
            }
        }
    }
    if rows != summary.rows || hashes != summary.row_hashes {
        bail!("captured validation corpus changed");
    }
    Ok(distinct.into_values().collect())
}

/// Reads the complete exact row multiset of the captured active files as disjoint chunks of whole
/// partitions, each with at most [`MAX_CHUNK_DISTINCT_ROWS`] distinct rows. Nothing is sampled or
/// truncated.
///
/// # Errors
///
/// Rejects unsafe paths, changed row counts/hashes, more than one or a non-string partition
/// column, null partition values, a partition with more than 256 distinct rows, and corpora beyond
/// [`MAX_CORPUS_DISTINCT_ROWS`].
pub fn sample_corpus(directory: &Path, identity: &str, summary: &TableSummary) -> Result<ValidationCorpus> {
    let root = table_root(directory, identity)?;
    let mut builder = CorpusBuilder::new(summary)?;
    for file in &summary.files {
        let path = local_active_file(&root, root.join(&file.path).to_str().context("non-UTF8 data path")?)?;
        let reader = SerializedFileReader::new(File::open(path)?)?;
        for row in reader.get_row_iter(None)? {
            let value = with_partitions(row_value(&row?), &summary.schema, &file.partition_values)?;
            builder.add(value, &file.partition_values)?;
        }
    }
    builder.finish(summary)
}

fn table_root(directory: &Path, identity: &str) -> Result<PathBuf> {
    if identity
        .split('.')
        .any(|part| part.is_empty() || part.contains(['/', '\\']))
    {
        bail!("invalid table identity");
    }
    let directory = fs::canonicalize(directory)?;
    let root = fs::canonicalize(directory.join(identity.replace('.', "/")))?;
    if !root.starts_with(&directory) {
        bail!("table escapes Delta root");
    }
    Ok(root)
}

/// Lists only committed, local active files. An empty root is pending, not a
/// completed validation; corrupt tables and unsafe paths remain errors.
///
/// # Errors
///
/// Rejects missing roots, corrupt logs, unsupported layouts, and unsafe paths.
pub async fn inventory(directory: &Path) -> Result<SnapshotInventory> {
    let directory =
        fs::canonicalize(directory).with_context(|| format!("canonicalize Delta root {}", directory.display()))?;
    let mut tables = BTreeMap::new();
    for root in find_delta_roots(&directory)? {
        let identity = table_identity(&directory, &root)?;
        let url = url::Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("invalid table path"))?;
        let delta = deltalake_core::open_table(url)
            .await
            .with_context(|| format!("open Delta table {identity}"))?;
        let snapshot = delta.snapshot()?;
        snapshot_partitions(&delta)?;
        let mut files = Vec::new();
        for uri in delta.get_file_uris_by_partitions(&[]).await? {
            let file = local_active_file(&root, &uri)?;
            files.push(
                file.strip_prefix(&root)?
                    .to_str()
                    .context("non-UTF8 data path")?
                    .to_owned(),
            );
        }
        files.sort();
        if tables
            .insert(
                identity.clone(),
                TableInventory {
                    schema: serde_json::to_value(snapshot.schema())?,
                    version: snapshot.version(),
                    files,
                },
            )
            .is_some()
        {
            bail!("duplicate Delta table identity {identity}");
        }
    }
    Ok(SnapshotInventory { tables })
}

fn table_identity(directory: &Path, root: &Path) -> Result<String> {
    let identity = root
        .strip_prefix(directory)?
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join(".");
    if identity.is_empty() {
        bail!("Delta table identity is empty");
    }
    Ok(identity)
}

/// Recursively opens every Delta table below a root and summarizes only files
/// selected by each independent delta-rs snapshot.
///
/// # Errors
///
/// Rejects absent tables, symlinks, corrupt logs/data, path escapes, and
/// non-local active files.
pub async fn summarize(directory: &Path) -> Result<SnapshotSummary> {
    let directory =
        fs::canonicalize(directory).with_context(|| format!("canonicalize Delta root {}", directory.display()))?;
    let roots = find_delta_roots(&directory)?;
    if roots.is_empty() {
        bail!("no Delta tables found in {}", directory.display());
    }
    let mut tables = BTreeMap::new();
    let mut total_files = 0_usize;
    for root in roots {
        let identity = table_identity(&directory, &root)?;
        let url = url::Url::from_directory_path(&root).map_err(|()| anyhow::anyhow!("invalid table path"))?;
        let delta = deltalake_core::open_table(url)
            .await
            .with_context(|| format!("open Delta table {identity}"))?;
        let snapshot = delta.snapshot()?;
        let partitions = snapshot_partitions(&delta)?;
        let partition_columns = snapshot.metadata().partition_columns().to_vec();
        let schema = serde_json::to_value(snapshot.schema())?;
        let uris = delta.get_file_uris_by_partitions(&[]).await?;
        let mut files = Vec::with_capacity(uris.len());
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        let mut row_hashes = BTreeMap::<String, u64>::new();
        let mut payload_row_hashes = BTreeMap::<String, u64>::new();
        for uri in uris {
            let path = local_active_file(&root, &uri)?;
            let relative = path
                .strip_prefix(&root)?
                .to_str()
                .context("non-UTF8 data path")?
                .to_owned();
            let partition_values = partitions
                .get(&relative)
                .context("active file lacks committed partition values")?;
            let size = fs::metadata(&path)?.len();
            let reader = SerializedFileReader::new(File::open(&path)?)
                .with_context(|| format!("open Parquet {}", path.display()))?;
            let file_rows = u64::try_from(reader.metadata().file_metadata().num_rows())
                .context("negative or overflowing Parquet row count")?;
            for row in reader.get_row_iter(None)? {
                let value = with_partitions(row_value(&row?), &schema, partition_values)?;
                *payload_row_hashes
                    .entry(payload_digest(value.clone(), &partition_columns)?)
                    .or_default() += 1;
                let digest = format!("sha256:{}", hex::encode(Sha256::digest(serde_json::to_vec(&value)?)));
                *row_hashes.entry(digest).or_default() += 1;
            }
            let relative = path
                .strip_prefix(&root)?
                .to_str()
                .context("non-UTF8 data path")?
                .to_owned();
            files.push(FileSummary {
                path: relative,
                bytes: size,
                rows: file_rows,
                partition_values: partition_values.clone(),
            });
            rows = rows.checked_add(file_rows).context("snapshot row count overflow")?;
            bytes = bytes.checked_add(size).context("snapshot byte count overflow")?;
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let minimum_file_bytes = files.iter().map(|file| file.bytes).min().unwrap_or(0);
        let maximum_file_bytes = files.iter().map(|file| file.bytes).max().unwrap_or(0);
        total_files += files.len();
        if tables
            .insert(
                identity.clone(),
                TableSummary {
                    schema,
                    version: snapshot.version(),
                    active_files: files.len(),
                    rows,
                    bytes,
                    minimum_file_bytes,
                    maximum_file_bytes,
                    row_hashes,
                    payload_row_hashes,
                    files,
                    partition_columns,
                },
            )
            .is_some()
        {
            bail!("duplicate Delta table identity {identity}");
        }
    }
    Ok(SnapshotSummary {
        files: total_files,
        tables,
    })
}

fn find_delta_roots(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut roots = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(current) = pending.pop() {
        let metadata = fs::symlink_metadata(&current)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("Delta snapshot traversal encountered a non-directory or symlink");
        }
        if current.join("_delta_log").is_dir() {
            roots.push(current);
            continue;
        }
        for entry in fs::read_dir(&current)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                bail!("Delta snapshot traversal rejects symlink {}", path.display());
            }
            if metadata.is_dir() {
                pending.push(path);
            }
        }
    }
    roots.sort();
    Ok(roots)
}

fn local_active_file(root: &Path, uri: &str) -> Result<PathBuf> {
    let path = if uri.starts_with("file:") {
        url::Url::parse(uri)?
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("non-local data file"))?
    } else if Path::new(uri).is_absolute() {
        PathBuf::from(uri)
    } else {
        bail!("non-local data file URI {uri}");
    };
    if fs::symlink_metadata(&path)?.file_type().is_symlink() {
        bail!("active data file is a symlink");
    }
    let path = fs::canonicalize(path)?;
    if !path.starts_with(root) {
        bail!("data file escapes table root");
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn partition_values_come_from_committed_actions_not_directory_names() -> Result<()> {
        use arrow::{
            array::{Int32Array, RecordBatch},
            datatypes::{DataType, Field, Schema},
        };
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        let directory = TestDirectory::new("partition-actions")?;
        let root = directory.path().join("telemetry/sqlserver/partitioned");
        fs::create_dir_all(root.join("_delta_log"))?;
        fs::create_dir_all(root.join("opaque"))?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int32Array::from(vec![1, 1, 2]))])?;
        let data = root.join("opaque/file.parquet");
        let mut writer = ArrowWriter::try_new(File::create(&data)?, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        fs::copy(&data, root.join("orphan.parquet"))?;
        let delta_schema = json!({"type":"struct","fields":[
            {"name":"id","type":"integer","nullable":false,"metadata":{}},
            {"name":"__arrowcol_ingestion","type":"string","nullable":false,"metadata":{}}
        ]});
        let actions = [
            json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
            json!({"metaData":{"id":"fixture-partition","format":{"provider":"parquet","options":{}},
                "schemaString":delta_schema.to_string(),"partitionColumns":["__arrowcol_ingestion"],"configuration":{}}}),
            json!({"add":{"path":"opaque/file.parquet","partitionValues":{"__arrowcol_ingestion":"2026-10-27-02-00"},
                "size":fs::metadata(&data)?.len(),"modificationTime":1,"dataChange":true,
                "stats":json!({"numRecords":3}).to_string()}}),
        ];
        fs::write(
            root.join("_delta_log/00000000000000000000.json"),
            actions.iter().map(Value::to_string).collect::<Vec<_>>().join("\n") + "\n",
        )?;
        let snapshot = summarize(directory.path()).await?;
        let summary = &snapshot.tables["telemetry.sqlserver.partitioned"];
        assert_eq!(summary.rows, 3);
        assert_eq!(summary.active_files, 1);
        assert_eq!(summary.partition_columns, ["__arrowcol_ingestion"]);
        let corpus = sample_rows(directory.path(), "telemetry.sqlserver.partitioned", summary)?;
        assert_eq!(
            corpus,
            vec![
                (json!({"id":1,"__arrowcol_ingestion":"2026-10-27-02-00"}), 2),
                (json!({"id":2,"__arrowcol_ingestion":"2026-10-27-02-00"}), 1),
            ]
        );
        let chunked = sample_corpus(directory.path(), "telemetry.sqlserver.partitioned", summary)?;
        assert_eq!(chunked.partition_column.as_deref(), Some("__arrowcol_ingestion"));
        assert_eq!(chunked.expected_rows, 3);
        assert_eq!(
            chunked.partition_rows,
            BTreeMap::from([("2026-10-27-02-00".to_owned(), 3)])
        );
        assert_eq!(chunked.chunks.len(), 1);
        assert_eq!(chunked.chunks[0].partition_values, ["2026-10-27-02-00"]);
        assert_eq!(chunked.chunks[0].rows, corpus);
        assert_eq!(
            inventory(directory.path()).await?.tables["telemetry.sqlserver.partitioned"].files,
            ["opaque/file.parquet"]
        );
        Ok(())
    }

    #[test]
    fn partition_decoder_rejects_mismatches_and_preserves_large_integers() -> Result<()> {
        let schema = json!({"fields":[{"name":"p","type":"long","nullable":false}]});
        let partitions = BTreeMap::from([("p".to_owned(), Some("9223372036854775807".to_owned()))]);
        assert_eq!(
            with_partitions(json!({}), &schema, &partitions)?,
            json!({"p":"9223372036854775807"})
        );
        assert!(with_partitions(json!({"p":1}), &schema, &partitions).is_err());
        assert!(with_partitions(json!({}), &schema, &BTreeMap::from([("p".to_owned(), None)])).is_err());
        Ok(())
    }

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(name: &str) -> Result<Self> {
            let sequence = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = PathBuf::from(format!(
                "target/loadtest-reader-tests/{}-{}-{sequence}",
                std::process::id(),
                name
            ));
            if path.exists() {
                fs::remove_dir_all(&path)?;
            }
            fs::create_dir_all(&path)?;
            Ok(Self(path))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

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
    async fn missing_directory_is_an_error() -> Result<()> {
        let directory = TestDirectory::new("missing")?;
        let missing = directory.path().join("absent");
        assert!(inspect(&missing).await.is_err());
        Ok(())
    }

    /// Scenario: a directory contains no tables or only raw Parquet without a log.
    /// Guarantees: raw files cannot masquerade as committed Delta data.
    #[tokio::test]
    async fn empty_and_non_delta_are_errors() -> Result<()> {
        let directory = TestDirectory::new("invalid")?;
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
        use arrow_deltalake::delta::DeltaTableWriter;
        use std::sync::Arc;
        let directory = TestDirectory::new("interop")?;
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
        let summary = summarize(directory.path()).await?;
        assert_eq!(summary.files, 2);
        assert_eq!(summary.tables["generic"].rows, 3);
        assert_eq!(summary.tables["generic"].active_files, 2);
        assert!(summary.tables["generic"].bytes > 0);
        let selected = inventory(directory.path()).await?;
        assert_eq!(selected.tables["generic"].version, 3);
        assert_eq!(selected.tables["generic"].files.len(), 2);
        assert!(
            !selected.tables["generic"]
                .files
                .iter()
                .any(|file| file == "orphan.parquet")
        );
        assert_eq!(selected, inventory(directory.path()).await?);
        let corpus = sample_rows(directory.path(), "generic", &summary.tables["generic"])?;
        assert_eq!(corpus.len(), 3);
        assert_eq!(corpus.iter().map(|(_, count)| count).sum::<u64>(), 3);
        let mut stale = summary.tables["generic"].clone();
        stale.rows += 1;
        assert!(sample_rows(directory.path(), "generic", &stale).is_err());
        assert!(sample_rows(directory.path(), "../generic", &stale).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn inventories_follow_late_commits_below_nested_table_roots() -> Result<()> {
        use arrow::array::{Int32Array, RecordBatch};
        use arrow_deltalake::delta::DeltaTableWriter;
        use std::sync::Arc;
        let directory = TestDirectory::new("inventory")?;
        assert!(inventory(directory.path()).await?.tables.is_empty());
        assert!(inventory(&directory.path().join("absent")).await.is_err());
        let root = directory.path().join("telemetry/sqlserver/example");
        let mut writer = DeltaTableWriter::open(&root)?;
        let batch = RecordBatch::try_from_iter(vec![("id", Arc::new(Int32Array::from(vec![1])) as _)])?;
        writer.write(&batch).await?;
        writer.flush().await?;
        let first = inventory(directory.path()).await?;
        writer.write(&batch).await?;
        writer.close().await?;
        let second = inventory(directory.path()).await?;
        let table = "telemetry.sqlserver.example";
        let summary = summarize(directory.path()).await?;
        assert_eq!(
            sample_rows(directory.path(), table, &summary.tables[table])?,
            vec![(json!({"id": 1}), 2)]
        );
        assert_eq!(first.tables[table].files.len(), 1);
        assert_eq!(second.tables[table].files.len(), 2);
        assert!(second.tables[table].version > first.tables[table].version);
        assert!(second.tables[table].files.contains(&first.tables[table].files[0]));
        fs::write(root.join("_delta_log/00000000000000000003.json"), b"invalid")?;
        assert!(inventory(directory.path()).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn validation_corpus_is_bounded_without_truncating_successfully() -> Result<()> {
        use arrow::array::{Int32Array, RecordBatch};
        use arrow_deltalake::delta::DeltaTableWriter;
        use std::sync::Arc;
        let directory = TestDirectory::new("bounded-corpus")?;
        let mut writer = DeltaTableWriter::open(directory.path().join("example"))?;
        let batch = RecordBatch::try_from_iter(vec![("id", Arc::new(Int32Array::from_iter_values(0..257)) as _)])?;
        writer.write(&batch).await?;
        writer.close().await?;
        let snapshot = summarize(directory.path()).await?;
        let error = sample_rows(directory.path(), "example", &snapshot.tables["example"])
            .err()
            .context("expected corpus bound error")?;
        assert!(error.to_string().contains("exceeds 256"));
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn inventories_reject_symlink_traversal_and_active_file_escapes() -> Result<()> {
        let directory = TestDirectory::new("inventory-paths")?;
        let outside = TestDirectory::new("inventory-outside")?;
        std::os::unix::fs::symlink(outside.path().canonicalize()?, directory.path().join("linked"))?;
        assert!(inventory(directory.path()).await.is_err());
        fs::write(outside.path().join("data.parquet"), b"not parquet")?;
        assert!(
            local_active_file(
                directory.path(),
                outside
                    .path()
                    .join("data.parquet")
                    .canonicalize()?
                    .to_str()
                    .context("path")?
            )
            .is_err()
        );
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
        use arrow_deltalake::delta::DeltaTableWriter;
        use std::sync::Arc;
        let directory = TestDirectory::new("lossless")?;
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
        use arrow_deltalake::delta::DeltaTableWriter;
        use std::sync::Arc;
        let directory = TestDirectory::new("nested")?;
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
