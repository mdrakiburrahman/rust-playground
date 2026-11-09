use anyhow::{Context, Result, ensure};
use arrow_deltalake::delta::WRITER_LOCK;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::IpAddr;
use std::path::{Component, Path, PathBuf};
use url::Url;

pub(crate) fn endpoint_host(host: &str) -> Result<String> {
    let host = match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(address)) => format!("[{address}]"),
        _ => host.to_owned(),
    };
    ensure!(
        !host.is_empty() && !host.chars().any(char::is_whitespace) && !host.contains(['/', '\\', '?', '#', '@', '%']),
        "endpoint_host must be a bare DNS name or IP address, without a port"
    );
    let parsed = url::Host::parse(&host).context("invalid local endpoint_host (ports are not allowed)")?;
    match &parsed {
        url::Host::Ipv4(address) => ensure!(
            !address.is_unspecified() && !address.is_multicast(),
            "invalid endpoint address"
        ),
        url::Host::Ipv6(address) => ensure!(
            !address.is_unspecified() && !address.is_multicast(),
            "invalid endpoint address"
        ),
        url::Host::Domain(_) => {}
    }
    Ok(parsed.to_string())
}

pub(crate) fn published_endpoint(binding: &str, host: Option<&str>, daemon: &str, path: &str) -> Result<String> {
    let binding = binding
        .trim()
        .parse::<std::net::SocketAddr>()
        .context("invalid Docker port binding")?;
    let port = binding.port();
    ensure!(port > 0, "Docker returned port zero");
    let bound_host = binding.ip().to_string();
    let daemon = Url::parse(daemon).context("invalid Docker daemon endpoint")?;
    let host = if let Some(host) = host {
        endpoint_host(host)?
    } else {
        let address = binding.ip();
        if matches!(daemon.scheme(), "tcp" | "http" | "https" | "ssh") {
            ensure!(
                !address.is_loopback(),
                "remote Docker loopback is not runner loopback; configure platform.local.bind_host and endpoint_host"
            );
            if address.is_unspecified() {
                endpoint_host(daemon.host_str().context("Docker daemon has no host")?)?
            } else {
                endpoint_host(&bound_host)?
            }
        } else {
            ensure!(
                !address.is_unspecified(),
                "wildcard Docker binding needs platform.local.endpoint_host"
            );
            endpoint_host(&bound_host)?
        }
    };
    Ok(format!("http://{host}:{port}{path}"))
}

pub(crate) fn absolute_directory(path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "local snapshot/state path must be absolute");
    for component in path.components() {
        ensure!(
            !matches!(component, Component::ParentDir | Component::CurDir),
            "noncanonical local path"
        );
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => ensure!(
                metadata.is_dir() && !metadata.file_type().is_symlink(),
                "unsafe directory {}",
                current.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SnapshotFile {
    path: String,
    size: u64,
    sha256: String,
}

fn regular_file(path: &Path) -> Result<File> {
    absolute_directory(path.parent().context("snapshot file has no parent")?)?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "not a regular snapshot file: {}",
        path.display()
    );
    let file = File::open(path)?;
    let after = fs::symlink_metadata(path)?;
    ensure!(
        after.is_file() && !after.file_type().is_symlink(),
        "snapshot file changed while opening"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let opened = file.metadata()?;
        ensure!(
            metadata.dev() == opened.dev()
                && metadata.ino() == opened.ino()
                && after.dev() == opened.dev()
                && after.ino() == opened.ino(),
            "snapshot file identity changed while opening"
        );
    }
    Ok(file)
}

pub(crate) fn file_digest(path: &Path) -> Result<(u64, String)> {
    let mut file = regular_file(path)?;
    let mut hash = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        size = size
            .checked_add(u64::try_from(count)?)
            .context("snapshot file size overflow")?;
    }
    Ok((size, format!("{:x}", hash.finalize())))
}

fn inventory(root: &Path, directory: &Path, files: &mut Vec<SnapshotFile>) -> Result<()> {
    absolute_directory(directory)?;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "snapshot refuses symlinks: {}",
            path.display()
        );
        if metadata.is_dir() {
            inventory(root, &path, files)?;
        } else {
            let relative = path
                .strip_prefix(root)?
                .to_str()
                .context("snapshot path is not UTF-8")?
                .to_owned();
            ensure!(!relative.contains('\\'), "ambiguous snapshot path separator");
            ensure!(
                !relative.contains(".checkpoint.") && !relative.ends_with("_last_checkpoint"),
                "checkpoint-dependent snapshots are not supported"
            );
            let (size, sha256) = file_digest(&path)?;
            files.push(SnapshotFile {
                path: relative,
                size,
                sha256,
            });
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(())
}

fn table_inventory(root: &Path) -> Result<Vec<SnapshotFile>> {
    let mut files = Vec::new();
    inventory(root, root, &mut files)?;
    files.retain(|file| !(file.path == WRITER_LOCK && file.size == 0));
    Ok(files)
}

fn delta_path(raw: &str) -> Result<String> {
    ensure!(
        !raw.is_empty() && !raw.starts_with('/') && !raw.contains(['\\', ':', '?', '#']),
        "unsafe Delta payload path"
    );
    for component in raw.split('/') {
        ensure!(
            !component.is_empty() && !matches!(component, "." | ".."),
            "unsafe Delta path component"
        );
    }
    let url = Url::parse(&format!("file:///{raw}"))?;
    let decoded = url
        .to_file_path()
        .map_err(|()| anyhow::anyhow!("invalid Delta payload path"))?;
    let decoded = decoded
        .strip_prefix(Path::new("/"))?
        .to_str()
        .context("non-UTF8 Delta path")?;
    ensure!(!decoded.contains('\\'), "encoded backslash in Delta path");
    for component in decoded.split('/') {
        ensure!(
            !component.is_empty() && !matches!(component, "." | ".."),
            "encoded traversal in Delta path"
        );
    }
    // URL parsers normalize encoded dot segments; reject them before trusting the decoded path.
    ensure!(
        !raw.to_ascii_lowercase().contains("%2e") && !raw.to_ascii_lowercase().contains("%2f"),
        "encoded traversal/separator in Delta path"
    );
    Ok(decoded.to_owned())
}

fn snapshot_proof(root: &Path, files: &[SnapshotFile]) -> Result<Value> {
    let mut logs = BTreeMap::new();
    for file in files {
        if let Some(name) = file.path.strip_prefix("_delta_log/")
            && let Some(prefix) = name.get(..20)
            && name.len() == 25
            && name.ends_with(".json")
            && prefix.bytes().all(|byte| byte.is_ascii_digit())
        {
            logs.insert(prefix.parse::<u64>()?, root.join(&file.path));
        }
    }
    ensure!(!logs.is_empty(), "snapshot has no JSON Delta history");
    let mut active = BTreeSet::new();
    let mut referenced = BTreeMap::new();
    let mut schema = None;
    for (expected, (version, path)) in logs.iter().enumerate() {
        ensure!(
            *version == u64::try_from(expected)?,
            "snapshot requires contiguous Delta history from version zero"
        );
        let mut contents = String::new();
        regular_file(path)?.read_to_string(&mut contents)?;
        for line in contents.lines() {
            let action: Value = serde_json::from_str(line)?;
            if let Some(protocol) = action.get("protocol") {
                for feature_key in ["readerFeatures", "writerFeatures"] {
                    if let Some(features) = protocol.get(feature_key).and_then(Value::as_array) {
                        ensure!(
                            !features
                                .iter()
                                .any(|feature| feature.as_str() == Some("deletionVectors")),
                            "deletion-vector protocol is unsupported"
                        );
                    }
                }
            }
            if let Some(metadata) = action.get("metaData") {
                let raw = metadata
                    .get("schemaString")
                    .and_then(Value::as_str)
                    .context("Delta schemaString missing")?;
                let parsed: Value = serde_json::from_str(raw)?;
                ensure!(
                    parsed.get("fields").and_then(Value::as_array).is_some(),
                    "invalid Delta schema"
                );
                schema = Some(raw.to_owned());
            }
            if let Some(add) = action.get("add") {
                ensure!(
                    add.get("deletionVector").is_none_or(Value::is_null),
                    "deletion-vector snapshots are unsupported"
                );
                let raw = add
                    .get("path")
                    .and_then(Value::as_str)
                    .context("Delta Add path missing")?;
                let path = delta_path(raw)?;
                let size = add
                    .get("size")
                    .and_then(Value::as_u64)
                    .context("Delta Add size missing")?;
                if let Some(previous) = referenced.insert(path.clone(), size) {
                    ensure!(previous == size, "Delta Add path changed size");
                }
                active.insert(path);
            }
            if let Some(remove) = action.get("remove") {
                ensure!(
                    remove.get("deletionVector").is_none_or(Value::is_null),
                    "deletion-vector snapshots are unsupported"
                );
                active.remove(&delta_path(
                    remove
                        .get("path")
                        .and_then(Value::as_str)
                        .context("Delta Remove path missing")?,
                )?);
            }
        }
    }
    let mut rows = 0_u64;
    for (path, expected_size) in referenced {
        let file = files
            .iter()
            .find(|file| file.path == path)
            .context("snapshot is missing a historical Delta payload")?;
        ensure!(file.size == expected_size, "Delta Add size differs from copied payload");
        let builder = ParquetRecordBatchReaderBuilder::try_new(regular_file(&root.join(&path))?)?;
        let footer_rows = u64::try_from(builder.metadata().file_metadata().num_rows())?;
        let mut decoded_rows = 0_u64;
        for batch in builder.build()? {
            decoded_rows = decoded_rows
                .checked_add(u64::try_from(batch?.num_rows())?)
                .context("Parquet row count overflow")?;
        }
        ensure!(
            footer_rows == decoded_rows,
            "Parquet footer and decoded row counts disagree"
        );
        if active.contains(&path) {
            rows = rows.checked_add(decoded_rows).context("snapshot row count overflow")?;
        }
    }
    ensure!(rows > 0, "snapshot contains no committed active rows");
    Ok(json!({
        "latestVersion": logs.last_key_value().context("missing Delta version")?.0,
        "rowCount": rows,
        "schemaString": schema.context("snapshot has no Delta schema")?,
    }))
}

fn durable_json(path: &Path, value: &Value) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    sync_directory(path.parent().context("manifest has no parent")?)
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn sync_tree_directories(path: &Path) -> Result<()> {
    absolute_directory(path)?;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.file_type()?;
        ensure!(!metadata.is_symlink(), "snapshot destination acquired a symlink");
        if metadata.is_dir() {
            sync_tree_directories(&entry.path())?;
        }
    }
    sync_directory(path)
}

pub(crate) fn export_stopped(
    state: &Path,
    destination: &Path,
    run_id: &str,
    tables: &[String],
    writers: &[Value],
) -> Result<()> {
    absolute_directory(state)?;
    absolute_directory(destination)?;
    ensure!(
        !destination.starts_with(state) && !state.starts_with(destination),
        "snapshot destination must be separate from generated state"
    );
    ensure!(
        uuid::Uuid::parse_str(run_id)?.to_string() == run_id,
        "invalid snapshot run UUID"
    );
    let mut missing = Vec::new();
    for ancestor in destination.ancestors() {
        if ancestor.exists() {
            break;
        }
        missing.push(ancestor);
    }
    fs::create_dir_all(destination)?;
    for created in missing {
        sync_directory(created)?;
        sync_directory(created.parent().context("snapshot root has no parent")?)?;
    }
    let destination = destination.join(run_id);
    fs::create_dir(&destination).context("snapshot run destination already exists; refusing overwrite")?;
    durable_json(
        &destination.join("stopped.json"),
        &json!({
            "schema_version": 1, "run_id": run_id, "writers": writers,
            "semantics": "last committed stopped snapshot; not proof all admitted rows flushed"
        }),
    )?;
    let mut manifests = Vec::new();
    for table in tables {
        ensure!(
            !table.is_empty() && table.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "unsafe snapshot table name"
        );
        let source = state.join("dataflow/telemetry/sqlserver").join(table);
        let table_destination = destination.join(table);
        fs::create_dir(&table_destination)?;
        let copied = table_destination.join("table");
        fs::create_dir(&copied)?;
        let files = table_inventory(&source)?;
        for entry in &files {
            let target = copied.join(&entry.path);
            let parent = target.parent().context("snapshot payload has no parent")?;
            absolute_directory(parent)?;
            fs::create_dir_all(parent)?;
            let mut input = regular_file(&source.join(&entry.path))?;
            let mut output = OpenOptions::new().create_new(true).write(true).open(&target)?;
            std::io::copy(&mut input, &mut output)?;
            output.sync_all()?;
            sync_directory(parent)?;
            ensure!(
                file_digest(&target)? == (entry.size, entry.sha256.clone()),
                "snapshot copy verification failed"
            );
        }
        ensure!(
            table_inventory(&source)? == files,
            "stopped source changed during snapshot"
        );
        let proof = snapshot_proof(&copied, &files)?;
        let mut verified = Vec::new();
        inventory(&copied, &copied, &mut verified)?;
        ensure!(
            verified == files,
            "snapshot inventory changed during independent row proof"
        );
        sync_tree_directories(&copied)?;
        durable_json(
            &table_destination.join("manifest.json"),
            &json!({
                "schema_version": 1, "run_id": run_id, "table": table,
                "latestVersion": proof["latestVersion"], "rowCount": proof["rowCount"],
                "schemaString": proof["schemaString"], "files": files, "writers": writers,
                "semantics": "last committed stopped snapshot; not proof all admitted rows flushed"
            }),
        )?;
        let (size, sha256) = file_digest(&table_destination.join("manifest.json"))?;
        manifests.push(json!({"path": format!("{table}/manifest.json"), "size": size, "sha256": sha256}));
        sync_directory(&table_destination)?;
    }
    durable_json(
        &destination.join("complete.json"),
        &json!({"schema_version": 1, "run_id": run_id, "tables": tables, "manifests": manifests}),
    )?;
    sync_directory(&destination)?;
    sync_directory(destination.parent().context("snapshot run has no parent")?)
}

pub(crate) async fn snapshot_then_cleanup<S, E, C>(stop: S, export: E, cleanup: C) -> Result<()>
where
    S: std::future::Future<Output = Result<Vec<Value>>>,
    E: FnOnce(Vec<Value>) -> Result<()>,
    C: std::future::Future<Output = Result<()>>,
{
    let writers = stop
        .await
        .context("stop outcome uncertain; retaining owned local data")?;
    export(writers).context("snapshot failed; retaining stopped owned local data")?;
    cleanup.await
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::bail;

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Result<Self> {
            let root = fs::canonicalize(std::env::temp_dir())?.join(format!("oat-snapshot-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&root)?;
            Ok(Self(root))
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            if let Err(error) = fs::remove_dir_all(&self.0) {
                panic!("remove owned snapshot fixture: {error}");
            }
        }
    }

    #[test]
    fn snapshot_preserves_history_payload_bytes_and_refuses_overwrite_or_missing_history() -> Result<()> {
        use arrow::array::{Int64Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        let fixture = Fixture::new()?;
        let state = fixture.0.join("state");
        let table = state.join("dataflow/telemetry/sqlserver/sqlserver_active_sessions");
        fs::create_dir_all(table.join("_delta_log"))?;
        File::create(table.join(WRITER_LOCK))?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(vec![1_i64, 2]))])?;
        let mut writer = ArrowWriter::try_new(File::create(table.join("part.parquet"))?, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        let size = fs::metadata(table.join("part.parquet"))?.len();
        let actions = [
            json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
            json!({"metaData": {"schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}]}"}}),
            json!({"add": {"path": "part.parquet", "size": size, "stats": "{\"numRecords\":999}"}}),
        ];
        let log = actions.iter().map(Value::to_string).collect::<Vec<_>>().join("\n");
        fs::write(table.join("_delta_log/00000000000000000000.json"), &log)?;
        fs::copy(table.join("part.parquet"), table.join("next.parquet"))?;
        let append = [
            json!({"remove": {"path": "part.parquet"}}),
            json!({"add": {"path": "next.parquet", "size": size, "stats": "{\"numRecords\":999}"}}),
        ]
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
        fs::write(table.join("_delta_log/00000000000000000001.json"), &append)?;
        let destination = fixture.0.join("snapshots");
        let run = uuid::Uuid::new_v4().to_string();
        let tables = vec!["sqlserver_active_sessions".to_owned()];
        let writers = vec![json!({"id": "owned", "service": "dataflow-1", "exit_code": 0})];
        export_stopped(&state, &destination, &run, &tables, &writers)?;
        let root = destination.join(&run).join(&tables[0]);
        let manifest: Value = serde_json::from_slice(&fs::read(root.join("manifest.json"))?)?;
        assert_eq!(manifest["rowCount"], 2);
        assert_eq!(manifest["latestVersion"], 1);
        assert_eq!(manifest["files"].as_array().context("missing inventory")?.len(), 4);
        assert!(table.join(WRITER_LOCK).exists());
        assert!(!root.join("table").join(WRITER_LOCK).exists());
        assert_eq!(
            fs::read(root.join("table/_delta_log/00000000000000000000.json"))?,
            log.as_bytes()
        );
        assert_eq!(
            file_digest(&table.join("part.parquet"))?,
            file_digest(&root.join("table/part.parquet"))?
        );
        assert!(export_stopped(&state, &destination, &run, &tables, &writers).is_err());
        fs::write(table.join("_delta_log/00000000000000000003.json"), "{}")?;
        let next_run = uuid::Uuid::new_v4().to_string();
        assert!(export_stopped(&state, &destination, &next_run, &tables, &writers).is_err());
        assert!(table.join("part.parquet").exists());
        assert!(!destination.join(next_run).join("complete.json").exists());
        fs::remove_file(table.join("_delta_log/00000000000000000003.json"))?;
        fs::remove_file(table.join("part.parquet"))?;
        let missing_history = uuid::Uuid::new_v4().to_string();
        assert!(export_stopped(&state, &destination, &missing_history, &tables, &writers).is_err());
        assert!(!destination.join(missing_history).join("complete.json").exists());
        assert!(table.join("next.parquet").exists());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_refuses_symlink_inputs_and_nested_destinations() -> Result<()> {
        let fixture = Fixture::new()?;
        let state = fixture.0.join("state");
        let table = state.join("dataflow/telemetry/sqlserver/sqlserver_active_sessions");
        fs::create_dir_all(&table)?;
        fs::write(fixture.0.join("outside"), "not snapshot data")?;
        std::os::unix::fs::symlink(fixture.0.join("outside"), table.join("link"))?;
        let run = uuid::Uuid::new_v4().to_string();
        let tables = vec!["sqlserver_active_sessions".to_owned()];
        assert!(export_stopped(&state, &fixture.0.join("snapshots"), &run, &tables, &[]).is_err());
        assert!(export_stopped(&state, &state.join("nested"), &run, &tables, &[]).is_err());
        assert!(table.join("link").exists());
        Ok(())
    }

    #[test]
    fn endpoints_preserve_binding_ipv6_and_explicit_daemon_host() -> Result<()> {
        assert_eq!(
            published_endpoint("127.0.0.1:1234", None, "unix:///var/run/docker.sock", "/metrics")?,
            "http://127.0.0.1:1234/metrics"
        );
        assert_eq!(
            published_endpoint("[::1]:1234", None, "unix:///var/run/docker.sock", "")?,
            "http://[::1]:1234"
        );
        assert_eq!(
            published_endpoint("10.0.0.1:1234", Some("10.0.0.1"), "unix:///var/run/docker.sock", "")?,
            "http://10.0.0.1:1234"
        );
        assert_eq!(
            published_endpoint("0.0.0.0:1234", None, "tcp://docker.example:2376", "")?,
            "http://docker.example:1234"
        );
        assert_eq!(
            published_endpoint("10.0.0.1:80", None, "tcp://docker.example:2376", "")?,
            "http://10.0.0.1:80"
        );
        assert_eq!(
            published_endpoint("[::]:1234", Some("2001:db8::1"), "ssh://docker.example", "")?,
            "http://[2001:db8::1]:1234"
        );
        assert!(published_endpoint("127.0.0.1:1234", None, "tcp://docker.example:2376", "").is_err());
        assert!(published_endpoint("0.0.0.0:1234", None, "unix:///var/run/docker.sock", "").is_err());
        for host in ["0.0.0.0", "host:80", "host/path", "user@host", "host?x=1"] {
            assert!(endpoint_host(host).is_err());
        }
        assert!(delta_path("../outside").is_err());
        assert!(delta_path("x/%2e%2e/outside").is_err());
        assert!(delta_path("https://outside").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn stop_or_export_failure_never_polls_destructive_cleanup() -> Result<()> {
        let cleaned = std::cell::Cell::new(false);
        let failed_stop = snapshot_then_cleanup(
            async { bail!("uncertain stop") },
            |_| panic!("export after uncertain stop"),
            async {
                cleaned.set(true);
                Ok(())
            },
        )
        .await;
        assert!(failed_stop.is_err());
        assert!(!cleaned.get());
        let failed_export = snapshot_then_cleanup(async { Ok(vec![]) }, |_| bail!("copy failed"), async {
            cleaned.set(true);
            Ok(())
        })
        .await;
        assert!(failed_export.is_err());
        assert!(!cleaned.get());
        snapshot_then_cleanup(async { Ok(vec![]) }, |_| Ok(()), async {
            cleaned.set(true);
            Ok(())
        })
        .await?;
        assert!(cleaned.get());
        Ok(())
    }
}
