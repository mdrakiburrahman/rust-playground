//! Real `initialize_url_for_bootstrap` admission, schema initialization and
//! drain/close over injected control metadata and a disposable local store.
//! Requires the non-default `test-control` feature and `debug_assertions`
//! (absent from release builds); no Azure.
#![cfg(all(feature = "test-control", debug_assertions))]

use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::DeltaTable;
use arrow_deltalake::publisher::{
    CasOutcome, ControlMetadata, MetadataSnapshot, PublisherSession, TestBootstrapControl,
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult,
};
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use url::Url;

const SANDBOX: &str = "0123456789ab";
const ACCOUNT: &str = "acct";
const GENERATION: &str = "00000000-0000-4000-8000-000000000001";
const ROTATED: &str = "00000000-0000-4000-8000-000000000002";

#[derive(Default)]
struct Memory(Mutex<(BTreeMap<String, String>, u64)>);

impl fmt::Debug for Memory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Memory")
    }
}

impl Memory {
    fn new(metadata: BTreeMap<String, String>) -> Arc<Self> {
        Arc::new(Self(Mutex::new((metadata, 0))))
    }

    fn metadata(&self) -> BTreeMap<String, String> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).0.clone()
    }

    fn members(&self) -> Result<usize> {
        let raw = self.metadata().get("publishers").cloned().context("publishers")?;
        Ok(serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&raw)?.len())
    }
}

#[async_trait]
impl ControlMetadata for Memory {
    async fn read(&self) -> Result<MetadataSnapshot> {
        let state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(MetadataSnapshot {
            etag: state.1.to_string(),
            metadata: state.0.clone(),
        })
    }

    async fn compare_and_set(&self, expected: &MetadataSnapshot, metadata: &BTreeMap<String, String>) -> CasOutcome {
        let mut state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if state.0 != expected.metadata {
            return CasOutcome::Conflict;
        }
        state.0 = metadata.clone();
        state.1 += 1;
        CasOutcome::Applied
    }
}

fn authority(state: &str) -> BTreeMap<String, String> {
    [
        ("sandboxid", SANDBOX),
        ("resetstate", state),
        ("resetgeneration", GENERATION),
        ("publisherregistryversion", "1"),
        ("publishers", "{}"),
        ("occupancy", "released"),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value.to_owned()))
    .collect()
}

fn control(metadata: &Arc<Memory>, expected: &str, data: Arc<dyn ObjectStore>) -> TestBootstrapControl {
    TestBootstrapControl {
        metadata: Arc::clone(metadata) as Arc<dyn ControlMetadata>,
        sandbox_id: SANDBOX.into(),
        account: ACCOUNT.into(),
        expected_generation: expected.into(),
        data,
    }
}

fn local(root: &Path) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(LocalFileSystem::new_with_prefix(root)?))
}

fn table_url(account: &str, table: &str) -> Result<Url> {
    Ok(Url::parse(&format!(
        "abfss://tables@{account}.dfs.core.windows.net/{table}"
    ))?)
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, true),
        Field::new("ingested_at", DataType::Utf8, true),
    ]))
}

fn files(root: &Path) -> Result<usize> {
    let mut count = 0;
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            } else {
                count += 1;
            }
        }
    }
    Ok(count)
}

fn initialize(table: &str, metadata: &Arc<Memory>, expected: &str, data: Arc<dyn ObjectStore>) -> Result<()> {
    DeltaTable::initialize_url_for_bootstrap_with_control(
        table_url(ACCOUNT, table)?,
        schema(),
        vec!["ingested_at".into()],
        control(metadata, expected, data),
    )
}

/// Guarantees: the real initializer creates every table log under fenced-v1,
/// drains and unregisters, never sets ready-v1, and is idempotent.
#[test]
fn bootstrap_initializes_tables_and_closes_fenced() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let metadata = Memory::new(authority("fenced-v1"));
    for table in ["cpu", "memory", "cpu"] {
        initialize(table, &metadata, GENERATION, local(dir.path())?)?;
        assert_eq!(metadata.members()?, 0, "{table}");
    }
    for table in ["cpu", "memory"] {
        let log = dir.path().join(table).join("_delta_log");
        assert!(std::fs::read_dir(&log)?.next().is_some(), "{table} log");
    }
    assert_eq!(
        metadata.metadata().get("resetstate").map(String::as_str),
        Some("fenced-v1")
    );
    Ok(())
}

/// Guarantees: refused admission (rotated generation, non-fenced state,
/// stopping occupancy, existing member) writes neither metadata nor blobs.
#[test]
fn refused_admission_writes_nothing() -> Result<()> {
    let mut cases: Vec<(&str, BTreeMap<String, String>, &str)> = vec![("rotated", authority("fenced-v1"), ROTATED)];
    for state in ["cleaning-v1", "ready-v1"] {
        cases.push((state, authority(state), GENERATION));
    }
    let mut stopping = authority("fenced-v1");
    stopping.insert("occupancy".into(), "stopping".into());
    stopping.insert("occupancyid".into(), ROTATED.into());
    cases.push(("stopping", stopping, GENERATION));
    for (name, initial, expected) in cases {
        let dir = tempfile::tempdir()?;
        let metadata = Memory::new(initial.clone());
        assert!(
            initialize("cpu", &metadata, expected, local(dir.path())?).is_err(),
            "{name}"
        );
        assert_eq!(metadata.metadata(), initial, "{name}: metadata unchanged");
        assert_eq!(files(dir.path())?, 0, "{name}: no blobs");
    }

    let dir = tempfile::tempdir()?;
    let metadata = Memory::new(authority("fenced-v1"));
    let live = PublisherSession::open_bootstrap_with_control(
        Arc::clone(&metadata) as Arc<dyn ControlMetadata>,
        SANDBOX,
        ACCOUNT,
        GENERATION,
    )?;
    assert!(
        initialize("cpu", &metadata, GENERATION, local(dir.path())?).is_err(),
        "live member"
    );
    assert_eq!(files(dir.path())?, 0);
    assert_eq!(metadata.members()?, 1);
    live.close_after_drain()?;
    assert_eq!(metadata.members()?, 0);
    Ok(())
}

/// Guarantees: the seam stays bound to ADLS URLs of the configured account.
#[test]
fn seam_rejects_local_and_foreign_urls() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let metadata = Memory::new(authority("fenced-v1"));
    let file = Url::from_directory_path(dir.path()).map_err(|()| anyhow::anyhow!("dir url"))?;
    for url in [file, table_url("other", "cpu")?] {
        assert!(
            DeltaTable::initialize_url_for_bootstrap_with_control(
                url.clone(),
                schema(),
                Vec::new(),
                control(&metadata, GENERATION, local(dir.path())?),
            )
            .is_err(),
            "{url}"
        );
    }
    assert_eq!(files(dir.path())?, 0);
    assert_eq!(metadata.members()?, 0, "rejected before or after drain, never retained");
    Ok(())
}

/// Stores nothing; every put has an unknown outcome (timed out).
#[derive(Debug, Default)]
struct TimedOutPuts(InMemory);

impl fmt::Display for TimedOutPuts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TimedOutPuts")
    }
}

#[async_trait]
impl ObjectStore for TimedOutPuts {
    async fn put_opts(&self, _: &ObjectPath, _: PutPayload, _: PutOptions) -> object_store::Result<PutResult> {
        Err(object_store::Error::Generic {
            store: "TimedOutPuts",
            source: "operation timed out".into(),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> object_store::Result<GetResult> {
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.0.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> object_store::Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &ObjectPath, to: &ObjectPath, options: CopyOptions) -> object_store::Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}

/// Guarantees: an unknown-outcome data write fails the initializer and the
/// bootstrap member stays registered as a reset blocker (never released).
#[test]
fn unknown_data_write_retains_member() -> Result<()> {
    let metadata = Memory::new(authority("fenced-v1"));
    let error = initialize("cpu", &metadata, GENERATION, Arc::new(TimedOutPuts::default()))
        .err()
        .context("unknown write succeeded")?;
    assert!(format!("{error:#}").contains("registration retained"), "{error:#}");
    assert_eq!(metadata.members()?, 1);
    assert_eq!(
        metadata.metadata().get("resetstate").map(String::as_str),
        Some("fenced-v1")
    );
    Ok(())
}
