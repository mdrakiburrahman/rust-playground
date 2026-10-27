//! Kernel-validated snapshots plus strictly contiguous committed JSON actions.

use anyhow::{Context, Result, bail, ensure};
use arrow_deltalake::delta::object_store_for_url;
use delta_kernel::Snapshot;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::scan::state::ScanFile;
use delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, ObjectStoreScheme};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SourceFile {
    pub path: String,
    pub size: u64,
    pub partition_values: BTreeMap<String, Option<String>>,
}

/// Saved before the first offset. Never reconstructed from a newer snapshot.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapManifest {
    pub source_url: String,
    pub table_id: String,
    pub version: u64,
    pub metadata: Value,
    pub protocol: Value,
    pub files: Vec<SourceFile>,
}

impl BootstrapManifest {
    /// Recover the logical Arrow schema without loading current source metadata.
    ///
    /// # Errors
    /// Rejects invalid Delta schemas or unsupported Arrow conversions.
    pub fn schema(&self) -> Result<arrow::datatypes::SchemaRef> {
        let json = self.metadata["schemaString"]
            .as_str()
            .context("missing source schemaString")?;
        let schema: delta_kernel::schema::StructType = serde_json::from_str(json)?;
        Ok(Arc::new((&schema).try_into_arrow()?))
    }

    /// Original source partition column names, in declared order.
    ///
    /// # Errors
    /// Rejects malformed metadata.
    pub fn partition_columns(&self) -> Result<Vec<String>> {
        Ok(serde_json::from_value(self.metadata["partitionColumns"].clone())?)
    }
}

/// The next unread action, not the last emitted file. Bootstrap indexes refer
/// to the frozen manifest; tail indexes refer to physical JSON action lines.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "phase", rename_all = "camelCase")]
pub enum Cursor {
    Bootstrap { index: usize },
    Tail { version: u64, index: usize },
}

#[derive(Debug)]
pub struct Microbatch {
    pub files: Vec<SourceFile>,
    pub end: Cursor,
    pub actions_examined: usize,
}

pub struct DeltaSource {
    pub url: Url,
    pub store: Arc<dyn ObjectStore>,
    prefix: Path,
    engine: DefaultEngine<TokioBackgroundExecutor>,
}

impl DeltaSource {
    /// Open a source URL using the shared local/Azure CLI storage provider.
    ///
    /// # Errors
    /// Returns URL, storage configuration, or Kernel construction errors.
    pub fn open(mut url: Url) -> Result<Self> {
        ensure!(
            url.query().is_none() && url.fragment().is_none(),
            "source URL cannot contain query/fragment"
        );
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        let (_, prefix) = ObjectStoreScheme::parse(&url)?;
        let store = object_store_for_url(&url)?;
        let engine = DefaultEngineBuilder::new(Arc::clone(&store)).build();
        Ok(Self {
            url,
            store,
            prefix,
            engine,
        })
    }

    /// Resolve a Delta relative data path without escaping the source table.
    ///
    /// # Errors
    /// Rejects absolute URLs, traversal, and paths outside this table.
    pub fn file_url(&self, path: &str) -> Result<Url> {
        let result = self.url.join(path)?;
        ensure!(
            !path.starts_with('/')
                && result.as_str().starts_with(self.url.as_str())
                && result.query().is_none()
                && result.fragment().is_none(),
            "Delta data path escapes source table"
        );
        Path::from_url_path(result.path()).context("unsafe encoded Delta data path")?;
        Ok(result)
    }

    /// Resolve the object-store key for a validated, URI-encoded Delta path.
    ///
    /// # Errors
    /// Rejects unsafe paths and invalid URI encoding.
    pub fn file_location(&self, path: &str) -> Result<Path> {
        self.file_url(path)?;
        let relative = Path::from_url_path(path)?;
        Ok(relative
            .parts()
            .fold(self.prefix.clone(), |prefix, part| prefix.join(part)))
    }

    /// Capture one Kernel snapshot, including its validated active Add set.
    ///
    /// # Errors
    /// Rejects unsupported source features and unreadable/missing history.
    pub fn capture(&self) -> Result<BootstrapManifest> {
        let snapshot = Snapshot::builder_for(self.url.as_str()).build(&self.engine)?;
        let configuration = snapshot.table_configuration();
        let metadata = serde_json::to_value(configuration.metadata())?;
        let protocol = serde_json::to_value(configuration.protocol())?;
        ensure!(
            metadata["configuration"]["delta.columnMapping.mode"]
                .as_str()
                .is_none_or(|mode| mode == "none"),
            "column-mapped source tables are not supported"
        );
        ensure!(
            metadata["configuration"]["delta.enableRowTracking"].as_str() != Some("true"),
            "row-tracking source tables are not supported"
        );
        let table_id = metadata["id"]
            .as_str()
            .context("source metadata missing table identity")?
            .to_owned();
        let version = snapshot.version();
        let scan = snapshot.scan_builder().build()?;
        let mut captured = CapturedFiles::default();
        for data in scan.scan_metadata(&self.engine)? {
            captured = data?.visit_scan_files(captured, collect_file)?;
            ensure!(
                captured.error.is_none(),
                "{}",
                captured.error.as_deref().unwrap_or("source capture failed")
            );
        }
        let mut files = captured.files;
        files.sort_by(|a, b| a.path.cmp(&b.path));
        let partitions: Vec<String> = serde_json::from_value(metadata["partitionColumns"].clone())?;
        for file in &mut files {
            self.file_url(&file.path)?;
            for name in &partitions {
                file.partition_values.entry(name.clone()).or_insert(None);
            }
        }
        Ok(BootstrapManifest {
            source_url: self.url.to_string(),
            table_id,
            version,
            metadata,
            protocol,
            files,
        })
    }

    /// Validate source identity/schema/protocol before every tail planning step.
    ///
    /// # Errors
    /// Rejects replaced tables and unsupported schema/protocol evolution.
    pub fn latest_version(&self, frozen: &BootstrapManifest) -> Result<u64> {
        let snapshot = Snapshot::builder_for(self.url.as_str()).build(&self.engine)?;
        let configuration = snapshot.table_configuration();
        ensure!(
            serde_json::to_value(configuration.metadata())? == frozen.metadata,
            "source identity/schema/partition metadata changed; start a new fork explicitly"
        );
        ensure!(
            serde_json::to_value(configuration.protocol())? == frozen.protocol,
            "source protocol changed; explicit migration required"
        );
        ensure!(snapshot.version() >= frozen.version, "source version regressed");
        Ok(snapshot.version())
    }

    /// Plan bounded work. Every traversed tail version must have its JSON log;
    /// a newer checkpoint never licenses skipping unavailable source history.
    ///
    /// # Errors
    /// Rejects gaps, malformed/unknown actions, unsupported row mutations, and
    /// invalid cursors. DELETE/PURGE removes are deliberately not propagated.
    pub async fn plan(&self, frozen: &BootstrapManifest, start: &Cursor, max_actions: usize) -> Result<Microbatch> {
        ensure!(max_actions > 0, "max actions per microbatch must be positive");
        ensure!(frozen.source_url == self.url.as_str(), "checkpoint source URL changed");
        let latest = self.latest_version(frozen)?;
        if let Cursor::Bootstrap { index } = start {
            ensure!(
                *index <= frozen.files.len(),
                "bootstrap cursor is beyond immutable manifest"
            );
            let end = index.saturating_add(max_actions).min(frozen.files.len());
            return Ok(Microbatch {
                files: frozen.files[*index..end].to_vec(),
                end: if end == frozen.files.len() {
                    Cursor::Tail {
                        version: frozen.version.checked_add(1).context("source version overflow")?,
                        index: 0,
                    }
                } else {
                    Cursor::Bootstrap { index: end }
                },
                actions_examined: end - index,
            });
        }
        let Cursor::Tail { version, index } = start else {
            unreachable!()
        };
        ensure!(*version > frozen.version, "tail cursor predates frozen snapshot");
        ensure!(
            *version <= latest.checked_add(1).context("source version overflow")?,
            "tail cursor is ahead of the source"
        );
        ensure!(
            *version <= latest || *index == 0,
            "tail cursor indexes a nonexistent commit"
        );
        let mut version = *version;
        let mut index = *index;
        let mut files = Vec::new();
        let mut examined = 0;
        while version <= latest && examined < max_actions {
            let path = self
                .prefix
                .clone()
                .join("_delta_log")
                .join(format!("{version:020}.json"));
            let object =
                self.store.get(&path).await.with_context(|| {
                    format!("required Delta history {version} unavailable; cannot advance checkpoint")
                })?;
            ensure!(
                object.meta.size <= 64 * 1024 * 1024,
                "Delta commit {version} exceeds the 64 MiB action-log budget"
            );
            let bytes = object.bytes().await?;
            let text = std::str::from_utf8(&bytes).context("non-UTF8 Delta commit")?;
            // Validate the WHOLE transaction before publishing any subset, so an
            // update/merge can't sneak an Add ahead of its paired Remove. Keep
            // only one parsed action rather than a whole-log JSON object graph.
            let action_count = validate_commit(text.lines().map(serde_json::from_str::<Value>), frozen)?;
            ensure!(index < action_count, "tail action cursor beyond commit {version}");
            for line in text.lines().skip(index).take(max_actions - examined) {
                let action: Value = serde_json::from_str(line)?;
                if let Some(add) = action.get("add")
                    && add["dataChange"].as_bool().context("Add missing dataChange")?
                {
                    let file: SourceFile = serde_json::from_value(add.clone())?;
                    self.file_url(&file.path)?;
                    files.push(file);
                }
                index += 1;
                examined += 1;
            }
            if index == action_count {
                version = version.checked_add(1).context("source version overflow")?;
                index = 0;
            }
        }
        Ok(Microbatch {
            files,
            end: Cursor::Tail { version, index },
            actions_examined: examined,
        })
    }
}

#[derive(Default)]
struct CapturedFiles {
    files: Vec<SourceFile>,
    path_bytes: usize,
    error: Option<String>,
}

fn collect_file(context: &mut CapturedFiles, file: ScanFile) {
    if context.error.is_some() {
        return;
    }
    let result: Result<SourceFile> = (|| {
        ensure!(
            !file.dv_info.has_vector(),
            "deletion-vector source files are unsupported"
        );
        let bytes = file.path.len()
            + file
                .partition_values
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>();
        context.path_bytes = context
            .path_bytes
            .checked_add(bytes)
            .context("manifest path size overflow")?;
        ensure!(
            context.files.len() < 262_144 && context.path_bytes <= 64 * 1024 * 1024,
            "bootstrap exceeds 262144 files/64 MiB path budget; split the source table before forking"
        );
        Ok(SourceFile {
            path: file.path,
            size: u64::try_from(file.size).context("negative Add file size")?,
            partition_values: file
                .partition_values
                .into_iter()
                .map(|(key, value)| (key, Some(value)))
                .collect(),
        })
    })();
    match result {
        Ok(file) => context.files.push(file),
        Err(error) => context.error = Some(format!("{error:#}")),
    }
}

fn validate_commit(
    actions: impl Iterator<Item = serde_json::Result<Value>>,
    frozen: &BootstrapManifest,
) -> Result<usize> {
    let mut add_change = false;
    let mut remove_change = false;
    let mut count = 0;
    for action in actions {
        let action = action?;
        count += 1;
        let object = action.as_object().context("Delta action is not an object")?;
        ensure!(object.len() == 1, "Delta action must contain exactly one action key");
        let (kind, body) = object.iter().next().context("empty Delta action")?;
        match kind.as_str() {
            "add" => {
                ensure!(
                    body.get("deletionVector").is_none_or(Value::is_null),
                    "deletion vector Add unsupported"
                );
                add_change |= body["dataChange"].as_bool().context("Add missing dataChange")?;
            }
            "remove" => {
                remove_change |= body["dataChange"].as_bool().context("Remove missing dataChange")?;
            }
            "metaData" => ensure!(body == &frozen.metadata, "source metadata changed in tail"),
            "protocol" => ensure!(body == &frozen.protocol, "source protocol changed in tail"),
            "commitInfo" | "txn" => {}
            "cdc" => bail!("change-data-feed files are unsupported; this fork consumes append-only Adds"),
            _ => bail!("unsupported Delta action {kind}"),
        }
    }
    ensure!(
        !(add_change && remove_change),
        "mixed dataChange Add/Remove mutation is unsupported (UPDATE/MERGE/overwrite)"
    );
    ensure!(count > 0, "empty Delta commit");
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_deltalake::delta::DeltaTable;
    use serde_json::json;

    fn manifest() -> BootstrapManifest {
        BootstrapManifest {
            source_url: "file:///tmp/source/".into(),
            table_id: "table".into(),
            version: 3,
            metadata: json!({"id":"table"}),
            protocol: json!({"minReaderVersion":1}),
            files: Vec::new(),
        }
    }

    fn validate_commit_values(actions: &[Value], frozen: &BootstrapManifest) -> Result<usize> {
        validate_commit(actions.iter().cloned().map(Ok), frozen)
    }

    #[test]
    fn distinguishes_append_optimize_purge_and_unsupported_updates() -> Result<()> {
        let frozen = manifest();
        validate_commit_values(&[json!({"add":{"dataChange":true}})], &frozen)?;
        validate_commit_values(
            &[
                json!({"add":{"dataChange":false}}),
                json!({"remove":{"dataChange":false}}),
            ],
            &frozen,
        )?;
        validate_commit_values(&[json!({"remove":{"dataChange":true}})], &frozen)?;
        assert!(
            validate_commit_values(
                &[
                    json!({"add":{"dataChange":true}}),
                    json!({"remove":{"dataChange":true}})
                ],
                &frozen
            )
            .is_err()
        );
        assert!(validate_commit_values(&[json!({"add":{"dataChange":true,"deletionVector":{}}})], &frozen).is_err());
        assert!(validate_commit_values(&[json!({"unknown":{}})], &frozen).is_err());
        assert!(validate_commit_values(&[json!({"metaData":{"id":"replaced"}})], &frozen).is_err());
        assert!(validate_commit_values(&[], &frozen).is_err());
        Ok(())
    }

    fn batch(customer: &str, value: i32) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("customer_id", DataType::Utf8, false),
                Field::new("value", DataType::Int32, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![customer])),
                Arc::new(Int32Array::from(vec![value])),
            ],
        )?)
    }

    async fn append(table: &Arc<DeltaTable>, value: i32) -> Result<()> {
        let data = batch("customer-0000", value)?;
        let mut writer = table.new_file(data.schema())?;
        writer.write(&data).await?;
        writer.finish().await?;
        Ok(())
    }

    #[tokio::test]
    async fn frozen_snapshot_then_bounded_tail_and_missing_history_fail_closed() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let table = DeltaTable::open(directory.path())?;
        append(&table, 1).await?;
        let source =
            DeltaSource::open(Url::from_directory_path(directory.path()).map_err(|()| anyhow::anyhow!("URL"))?)?;
        let frozen = source.capture()?;
        assert_eq!(frozen.files.len(), 1);
        assert_eq!(frozen.version, 1);
        append(&table, 2).await?;
        let bootstrap = source.plan(&frozen, &Cursor::Bootstrap { index: 0 }, 1).await?;
        assert_eq!(bootstrap.files, frozen.files);
        assert_eq!(bootstrap.end, Cursor::Tail { version: 2, index: 0 });
        let mut cursor = bootstrap.end;
        let mut files = Vec::new();
        for _ in 0..10 {
            let work = source.plan(&frozen, &cursor, 1).await?;
            assert!(work.actions_examined <= 1);
            files.extend(work.files);
            cursor = work.end;
            if cursor == (Cursor::Tail { version: 3, index: 0 }) {
                break;
            }
        }
        assert_eq!(files.len(), 1);
        assert_ne!(files[0].path, frozen.files[0].path);
        assert!(source.plan(&frozen, &cursor, 1).await?.files.is_empty());
        append(&table, 3).await?;
        std::fs::remove_file(directory.path().join("_delta_log/00000000000000000002.json"))?;
        assert!(
            source
                .plan(&frozen, &Cursor::Tail { version: 2, index: 0 }, 10)
                .await
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn rejects_relative_traversal_and_external_data_paths() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let source =
            DeltaSource::open(Url::from_directory_path(directory.path()).map_err(|()| anyhow::anyhow!("URL"))?)?;
        assert!(source.file_url("../other/data.parquet").is_err());
        assert!(source.file_url("file:///etc/passwd").is_err());
        assert!(source.file_url("/absolute.parquet").is_err());
        assert!(source.file_url("%2e%2e/other.parquet").is_err());
        assert!(source.file_url("nested%2F..%2F..%2Fother.parquet").is_err());
        assert!(source.file_url("minute=2026-10-27/part.parquet").is_ok());
        assert!(
            source
                .file_location("minute=2026-10-27/part.parquet")?
                .to_string()
                .ends_with("/minute=2026-10-27/part.parquet")
        );
        Ok(())
    }
}
