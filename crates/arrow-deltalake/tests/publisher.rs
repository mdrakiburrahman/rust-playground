//! Offline protocol coverage for persistent publisher sessions. This proves the
//! cooperative registry and guard only; it makes no claim about arbitrary or
//! legacy external writers, which the operator must stop.
#![cfg(feature = "delta")]

use anyhow::{Context, Result};
use arrow_deltalake::publisher::{
    CasOutcome, ControlMetadata, MAX_METADATA_BYTES, MAX_PUBLISHERS, MetadataSnapshot, MutationRetries, Occupancy,
    Phase, PublisherSession, Registry, SessionMode, UnknownTransition,
};
use async_trait::async_trait;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

type Mutate = dyn Fn(&mut BTreeMap<String, String>);
use url::Url;

const SANDBOX: &str = "0123456789ab";
const GENERATION: &str = "00000000-0000-4000-8000-000000000001";
const ROTATED: &str = "00000000-0000-4000-8000-000000000002";

#[derive(Debug, Clone, Copy)]
enum Fault {
    Conflict,
    Rejected,
    Unknown { applied: bool },
}

type Hook = Box<dyn FnOnce(&mut BTreeMap<String, String>) + Send>;

#[derive(Default)]
struct State {
    metadata: BTreeMap<String, String>,
    etag: u64,
    faults: VecDeque<Fault>,
    /// Runs once between a caller's read and its serialized write.
    race: Option<Hook>,
    writes: usize,
}

/// In-memory serialized metadata backend with the lease protocol's outcomes.
#[derive(Default)]
struct Memory(Mutex<State>);

impl fmt::Debug for Memory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Memory")
    }
}

impl Memory {
    fn new(metadata: BTreeMap<String, String>) -> Arc<Self> {
        Arc::new(Self(Mutex::new(State {
            metadata,
            ..State::default()
        })))
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set(&self, name: &str, value: &str) {
        let mut state = self.state();
        state.metadata.insert(name.into(), value.into());
        state.etag += 1;
    }

    fn members(&self) -> Result<serde_json::Map<String, serde_json::Value>> {
        let raw = self.state().metadata.get("publishers").cloned().context("publishers")?;
        Ok(serde_json::from_str(&raw)?)
    }
}

#[async_trait]
impl ControlMetadata for Memory {
    async fn read(&self) -> Result<MetadataSnapshot> {
        let state = self.state();
        Ok(MetadataSnapshot {
            etag: state.etag.to_string(),
            metadata: state.metadata.clone(),
        })
    }

    async fn compare_and_set(&self, expected: &MetadataSnapshot, metadata: &BTreeMap<String, String>) -> CasOutcome {
        let mut state = self.state();
        if let Some(race) = state.race.take() {
            race(&mut state.metadata);
            state.etag += 1;
        }
        match state.faults.pop_front() {
            Some(Fault::Conflict) => return CasOutcome::Conflict,
            Some(Fault::Rejected) => return CasOutcome::Rejected("forbidden".into()),
            Some(Fault::Unknown { applied }) => {
                if applied {
                    state.metadata = metadata.clone();
                    state.etag += 1;
                }
                return CasOutcome::Unknown("connection reset".into());
            }
            None => {}
        }
        if state.metadata != expected.metadata {
            return CasOutcome::Conflict;
        }
        state.metadata = metadata.clone();
        state.etag += 1;
        state.writes += 1;
        CasOutcome::Applied
    }
}

fn authority(state: &str, publishers: &str) -> BTreeMap<String, String> {
    [
        ("sandboxid", SANDBOX),
        ("resetstate", state),
        ("resetgeneration", GENERATION),
        ("publisherregistryversion", "1"),
        ("publishers", publishers),
        ("occupancy", "released"),
        ("operatornote", "keep"),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value.to_owned()))
    .collect()
}

async fn open(control: &Arc<Memory>, mode: SessionMode) -> Result<Arc<PublisherSession>> {
    let expected = (mode == SessionMode::Bootstrap).then_some(GENERATION);
    PublisherSession::open_with_control(
        Arc::clone(control) as Arc<dyn ControlMetadata>,
        SANDBOX,
        "acct",
        mode,
        expected,
    )
    .await
}

fn table_url(container: &str) -> Result<Url> {
    Ok(Url::parse(&format!(
        "abfss://{container}@acct.dfs.core.windows.net/root"
    ))?)
}

fn memory_store(session: &Arc<PublisherSession>, retries: MutationRetries) -> Result<Arc<dyn ObjectStore>> {
    session.bind_store(&table_url("tables")?, Arc::new(InMemory::new()), retries)
}

fn payload() -> PutPayload {
    PutPayload::from_static(b"data")
}

/// Guarantees: a fence that lands between the registrar's read and write
/// wins; the retry re-reads fenced-v1 and admits nothing.
#[tokio::test]
async fn fence_racing_ahead_of_registration_admits_nothing() -> Result<()> {
    let control = Memory::new(authority("ready-v1", "{}"));
    control.state().race = Some(Box::new(|metadata| {
        metadata.insert("resetstate".into(), "fenced-v1".into());
    }));
    let error = open(&control, SessionMode::Publisher).await.err().context("fenced")?;
    assert!(format!("{error:#}").contains("requires ready-v1"));
    assert!(control.members()?.is_empty());
    Ok(())
}

/// Guarantees: a fence after registration stops new mutations while the
/// member may still drain and leave with a same-generation clean close.
#[tokio::test]
async fn fence_after_registration_blocks_writes_and_allows_drain() -> Result<()> {
    let control = Memory::new(authority("ready-v1", "{}"));
    let session = open(&control, SessionMode::Publisher).await?;
    let store = memory_store(&session, MutationRetries::Disabled)?;
    store.put(&Path::from("a"), payload()).await?;
    control.set("resetstate", "fenced-v1");
    assert_eq!(control.members()?.len(), 1, "fenced registry still blocks cleanup");
    assert!(session.check_async().await.is_err());
    assert!(matches!(session.status().phase, Phase::Fenced(_)));
    assert!(store.put(&Path::from("b"), payload()).await.is_err());
    session.close_after_drain_async().await?;
    assert!(control.members()?.is_empty());
    assert_eq!(session.status().phase, Phase::Closed);
    assert_eq!(
        control.state().metadata.get("operatornote").map(String::as_str),
        Some("keep")
    );
    Ok(())
}

/// Guarantees: an unknown registration outcome is a typed error, leaves the
/// possibly written member blocking, and yields no session.
#[tokio::test]
async fn unknown_registration_leaves_blocking_member() -> Result<()> {
    for applied in [true, false] {
        let control = Memory::new(authority("ready-v1", "{}"));
        control.state().faults.push_back(Fault::Unknown { applied });
        let error = open(&control, SessionMode::Publisher).await.err().context("unknown")?;
        let unknown = error.downcast_ref::<UnknownTransition>().context("typed")?;
        assert_eq!(unknown.operation, "registration");
        assert_eq!(control.members()?.contains_key(&unknown.publisher.to_string()), applied);
        assert_eq!(control.state().writes, 0, "no retry after an unknown outcome");
    }
    Ok(())
}

/// Guarantees: a definite rejection and bounded contention write nothing and
/// end with explicit errors.
#[tokio::test]
async fn rejection_and_contention_are_explicit() -> Result<()> {
    let control = Memory::new(authority("ready-v1", "{}"));
    control.state().faults.push_back(Fault::Rejected);
    assert!(
        format!(
            "{:#}",
            open(&control, SessionMode::Publisher).await.err().context("rejected")?
        )
        .contains("rejected")
    );
    control.state().faults.extend([Fault::Conflict; 8]);
    let error = open(&control, SessionMode::Publisher)
        .await
        .err()
        .context("exhausted")?;
    assert!(format!("{error:#}").contains("contention budget exhausted"));
    assert!(control.members()?.is_empty());
    Ok(())
}

fn full_registry(count: usize) -> String {
    let entries: BTreeMap<String, serde_json::Value> = (0..count)
        .map(|index| {
            (
                format!("00000000-0000-4000-8000-{index:012x}"),
                serde_json::json!({"generation": GENERATION, "state": "active"}),
            )
        })
        .collect();
    serde_json::to_string(&entries).unwrap_or_default()
}

/// Guarantees: slot and total metadata budgets fail closed before writing.
#[tokio::test]
async fn slot_and_metadata_budgets_fail_closed() -> Result<()> {
    let control = Memory::new(authority("ready-v1", &full_registry(MAX_PUBLISHERS)));
    let error = open(&control, SessionMode::Publisher).await.err().context("full")?;
    assert!(format!("{error:#}").contains("full"));
    let mut metadata = authority("ready-v1", "{}");
    let used: usize = metadata
        .iter()
        .map(|(name, value)| "x-ms-meta-: \r\n".len() + name.len() + value.len())
        .sum();
    // Leave room for the filler header itself but not for one more member.
    let filler = MAX_METADATA_BYTES - used - "x-ms-meta-filler: \r\n".len() - 10;
    metadata.insert("filler".into(), "x".repeat(filler));
    assert!(
        Registry::parse(&metadata, SANDBOX).is_ok(),
        "unregistered authority fits the budget"
    );
    let control = Memory::new(metadata);
    let error = open(&control, SessionMode::Publisher).await.err().context("budget")?;
    assert!(format!("{error:#}").contains("budget"));
    assert_eq!(control.state().writes, 0);
    Ok(())
}
/// Guarantees: malformed, duplicate, unknown, noncanonical, legacy and
/// generation-mismatched authority is rejected; so binaries that expect
/// unversioned ready/fenced never see a value they accept.
#[test]
fn schema_rejects_invalid_authority() -> Result<()> {
    let member = |generation: &str| {
        format!(r#"{{"00000000-0000-4000-8000-000000000009":{{"generation":"{generation}","state":"active"}}}}"#)
    };
    let valid = authority("ready-v1", &member(GENERATION));
    let parsed = Registry::parse(&valid, SANDBOX)?;
    assert_eq!(parsed.publishers.len(), 1);
    assert_eq!(parsed.occupancy, Occupancy::Released);
    let cases: Vec<(&str, Box<Mutate>)> = vec![
        (
            "legacy ready",
            Box::new(|m| drop(m.insert("resetstate".into(), "ready".into()))),
        ),
        (
            "legacy fenced",
            Box::new(|m| drop(m.insert("resetstate".into(), "fenced".into()))),
        ),
        (
            "sandbox",
            Box::new(|m| drop(m.insert("sandboxid".into(), "ffffffffffff".into()))),
        ),
        (
            "version",
            Box::new(|m| drop(m.insert("publisherregistryversion".into(), "2".into()))),
        ),
        (
            "missing version",
            Box::new(|m| drop(m.remove("publisherregistryversion"))),
        ),
        ("missing registry", Box::new(|m| drop(m.remove("publishers")))),
        ("missing occupancy", Box::new(|m| drop(m.remove("occupancy")))),
        (
            "unknown occupancy",
            Box::new(|m| drop(m.insert("occupancy".into(), "idle".into()))),
        ),
        (
            "testing without owner",
            Box::new(|m| drop(m.insert("occupancy".into(), "testing".into()))),
        ),
        (
            "uppercase generation",
            Box::new(|m| {
                m.insert("publishers".into(), "{}".into());
                m.insert("resetgeneration".into(), "ABCDEF00-0000-4000-8000-000000000001".into());
            }),
        ),
        (
            "noncompact",
            Box::new(|m| drop(m.insert("publishers".into(), "{ }".into()))),
        ),
        ("array", Box::new(|m| drop(m.insert("publishers".into(), "[]".into())))),
        (
            "duplicate",
            Box::new(|m| {
                let entry = r#""00000000-0000-4000-8000-000000000009":{"generation":"00000000-0000-4000-8000-000000000001","state":"active"}"#;
                m.insert("publishers".into(), format!("{{{entry},{entry}}}"));
            }),
        ),
        (
            "unknown field",
            Box::new(|m| {
                m.insert(
                    "publishers".into(),
                    r#"{"00000000-0000-4000-8000-000000000009":{"generation":"00000000-0000-4000-8000-000000000001","lease":1,"state":"active"}}"#.into(),
                );
            }),
        ),
        (
            "unknown state",
            Box::new(|m| {
                m.insert(
                    "publishers".into(),
                    r#"{"00000000-0000-4000-8000-000000000009":{"generation":"00000000-0000-4000-8000-000000000001","state":"gone"}}"#.into(),
                );
            }),
        ),
        (
            "noncanonical member",
            Box::new(|m| {
                m.insert(
                    "publishers".into(),
                    r#"{"00000000000040008000000000000009":{"generation":"00000000-0000-4000-8000-000000000001","state":"active"}}"#.into(),
                );
            }),
        ),
        (
            "generation mismatch",
            Box::new(move |m| drop(m.insert("publishers".into(), member(ROTATED)))),
        ),
        (
            "slots",
            Box::new(|m| drop(m.insert("publishers".into(), full_registry(MAX_PUBLISHERS + 1)))),
        ),
        (
            "bytes",
            Box::new(|m| drop(m.insert("publishers".into(), format!("{{{}}}", " ".repeat(4096))))),
        ),
    ];
    for (name, mutate) in cases {
        let mut metadata = valid.clone();
        mutate(&mut metadata);
        assert!(Registry::parse(&metadata, SANDBOX).is_err(), "{name} must be rejected");
    }
    let mut stopping = valid.clone();
    stopping.insert("occupancy".into(), "stopping".into());
    stopping.insert("occupancyid".into(), ROTATED.into());
    assert!(matches!(
        Registry::parse(&stopping, SANDBOX)?.occupancy,
        Occupancy::Stopping(_)
    ));
    Ok(())
}

/// Guarantees: a drained session removes only its own member, preserves all
/// other metadata, and refuses every later mutation.
#[tokio::test]
async fn clean_drain_deregisters_only_self() -> Result<()> {
    let other = r#"{"00000000-0000-4000-8000-000000000009":{"generation":"00000000-0000-4000-8000-000000000001","state":"uncertain"}}"#;
    let control = Memory::new(authority("ready-v1", other));
    let session = open(&control, SessionMode::Publisher).await?;
    let store = memory_store(&session, MutationRetries::Disabled)?;
    store.put(&Path::from("a"), payload()).await?;
    session.check_async().await?;
    session.close_after_drain_async().await?;
    assert_eq!(
        control.state().metadata.get("publishers").map(String::as_str),
        Some(other)
    );
    assert_eq!(
        control.state().metadata.get("operatornote").map(String::as_str),
        Some("keep")
    );
    let path = Path::from("a");
    assert!(store.put(&Path::from("b"), payload()).await.is_err());
    assert!(store.delete(&path).await.is_err());
    assert!(store.copy(&path, &Path::from("c")).await.is_err());
    assert!(store.rename(&path, &Path::from("d")).await.is_err());
    assert!(store.put_multipart(&Path::from("m")).await.is_err());
    store.get(&path).await?;
    assert_eq!(store.list(None).count().await, 1);
    session.close_after_drain_async().await?;
    Ok(())
}

/// Guarantees: close refuses while a multipart upload is in flight, keeps
/// admissions stopped, and succeeds once the upload is aborted definitively.
#[tokio::test]
async fn inflight_upload_refuses_close_until_settled() -> Result<()> {
    let control = Memory::new(authority("ready-v1", "{}"));
    let session = open(&control, SessionMode::Publisher).await?;
    let store = memory_store(&session, MutationRetries::Disabled)?;
    let mut upload = store.put_multipart(&Path::from("m")).await?;
    upload.put_part(payload()).await?;
    let error = session.close_after_drain_async().await.err().context("inflight")?;
    assert!(format!("{error:#}").contains("in flight"));
    assert_eq!(control.members()?.len(), 1);
    assert!(store.put(&Path::from("late"), payload()).await.is_err());
    assert!(upload.put_part(payload()).await.is_err());
    assert!(upload.complete().await.is_err());
    upload.abort().await?;
    session.close_after_drain_async().await?;
    assert!(control.members()?.is_empty());
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum Flaw {
    Fail,
    Hang,
}

#[derive(Debug)]
struct FlakyStore {
    inner: InMemory,
    flaw: Flaw,
}

impl fmt::Display for FlakyStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FlakyStore")
    }
}

#[async_trait]
impl ObjectStore for FlakyStore {
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> object_store::Result<PutResult> {
        match self.flaw {
            Flaw::Fail => Err(object_store::Error::Generic {
                store: "FlakyStore",
                source: "operation timed out".into(),
            }),
            Flaw::Hang => futures::future::pending().await,
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Fake Azure store whose server-side copy is "accepted" (Ok) like an
/// asynchronous `202` Copy Blob whose `x-ms-copy-status` is still pending.
#[derive(Debug, Default)]
struct AcceptedCopyStore {
    inner: InMemory,
    copies: AtomicUsize,
}

impl fmt::Display for AcceptedCopyStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AcceptedCopyStore")
    }
}

#[async_trait]
impl ObjectStore for AcceptedCopyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> object_store::Result<()> {
        self.copies.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn rename_opts(&self, _: &Path, _: &Path, _: RenameOptions) -> object_store::Result<()> {
        self.copies.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Guarantees: on an Azure-bound guarded store every copy/rename variant
/// fails `NotSupported` before reaching the transport, so an accepted but
/// still-pending server-side copy can never be counted as a drained outcome;
/// close succeeds only because no copy was ever issued.
#[tokio::test]
async fn azure_server_side_copy_is_unreachable() -> Result<()> {
    for retries in [MutationRetries::Disabled, MutationRetries::Opaque] {
        let control = Memory::new(authority("ready-v1", "{}"));
        let session = open(&control, SessionMode::Publisher).await?;
        let fake = Arc::new(AcceptedCopyStore::default());
        let store = session.bind_store(
            &table_url("tables")?,
            Arc::clone(&fake) as Arc<dyn ObjectStore>,
            retries,
        )?;
        let (from, to) = (Path::from("a"), Path::from("b"));
        let results = [
            store.copy(&from, &to).await,
            store.copy_if_not_exists(&from, &to).await,
            store.rename(&from, &to).await,
            store.rename_if_not_exists(&from, &to).await,
        ];
        for result in results {
            assert!(
                matches!(result, Err(object_store::Error::NotSupported { .. })),
                "{result:?}"
            );
        }
        assert_eq!(fake.copies.load(Ordering::SeqCst), 0, "no copy reached the transport");
        let status = session.status();
        assert_eq!(status.inflight, 0);
        assert!(status.uncertain.is_none(), "{:?}", status.uncertain);
        assert_eq!(control.members()?.len(), 1);
        session.close_after_drain_async().await?;
        assert!(control.members()?.is_empty());
    }
    Ok(())
}

/// Guarantees: an ambiguous failure, a cancelled mutation, a dropped upload
/// and opaque retries all retain the member (marked uncertain) and refuse
/// both further writes and clean close.
#[tokio::test]
async fn uncertain_outcomes_retain_member() -> Result<()> {
    for case in 0..4 {
        let control = Memory::new(authority("ready-v1", "{}"));
        let session = open(&control, SessionMode::Publisher).await?;
        let url = table_url("tables")?;
        match case {
            0 => {
                let flaky = Arc::new(FlakyStore {
                    inner: InMemory::new(),
                    flaw: Flaw::Fail,
                });
                let store = session.bind_store(&url, flaky, MutationRetries::Disabled)?;
                assert!(store.put(&Path::from("a"), payload()).await.is_err());
            }
            1 => {
                let flaky = Arc::new(FlakyStore {
                    inner: InMemory::new(),
                    flaw: Flaw::Hang,
                });
                let store = session.bind_store(&url, flaky, MutationRetries::Disabled)?;
                let path = Path::from("a");
                let put = store.put(&path, payload());
                assert!(tokio::time::timeout(Duration::from_millis(20), put).await.is_err());
            }
            2 => {
                let store = memory_store(&session, MutationRetries::Disabled)?;
                drop(store.put_multipart(&Path::from("m")).await?);
            }
            _ => {
                let store = memory_store(&session, MutationRetries::Opaque)?;
                store.put(&Path::from("a"), payload()).await?;
            }
        }
        assert!(session.status().uncertain.is_some(), "case {case}");
        let store = memory_store(&session, MutationRetries::Disabled)?;
        assert!(store.put(&Path::from("z"), payload()).await.is_err());
        assert!(session.close_after_drain_async().await.is_err());
        let members = control.members()?;
        let id = session.publisher_id().context("id")?.to_string();
        assert_eq!(members[&id]["state"], "uncertain", "case {case}");
    }
    Ok(())
}

/// Guarantees: a rotated generation revokes the session: no writes and no
/// cleanup of the new generation's registry.
#[tokio::test]
async fn stale_generation_refuses_write_and_cleanup() -> Result<()> {
    let control = Memory::new(authority("ready-v1", "{}"));
    let session = open(&control, SessionMode::Publisher).await?;
    let store = memory_store(&session, MutationRetries::Disabled)?;
    control.set("resetgeneration", ROTATED);
    control.set("publishers", "{}");
    assert!(session.check_async().await.is_err());
    assert!(matches!(session.status().phase, Phase::Revoked(_)));
    assert!(store.put(&Path::from("a"), payload()).await.is_err());
    let writes = control.state().writes;
    assert!(session.close_after_drain_async().await.is_err());
    assert_eq!(control.state().writes, writes);
    Ok(())
}

/// Guarantees: one session guards many tables and checkpoint stores in its
/// account under one registration and rejects other accounts and schemes.
#[tokio::test]
async fn one_session_binds_many_stores_in_one_account() -> Result<()> {
    let control = Memory::new(authority("ready-v1", "{}"));
    let session = open(&control, SessionMode::Publisher).await?;
    for container in ["tables", "checkpoints", "other"] {
        let store = session.bind_store(
            &table_url(container)?,
            Arc::new(InMemory::new()),
            MutationRetries::Disabled,
        )?;
        store.put(&Path::from("a"), payload()).await?;
    }
    assert_eq!(control.members()?.len(), 1);
    let foreign = Url::parse("abfss://tables@other.dfs.core.windows.net/root")?;
    assert!(
        session
            .bind_store(&foreign, Arc::new(InMemory::new()), MutationRetries::Disabled)
            .is_err()
    );
    let local = Url::parse("file:///tmp/table")?;
    assert!(
        session
            .bind_store(&local, Arc::new(InMemory::new()), MutationRetries::Disabled)
            .is_err()
    );
    session.close_after_drain_async().await?;
    Ok(())
}

/// Guarantees: bootstrap is the exclusive fenced-v1 path at the captured
/// generation with an empty registry and released occupancy; normal admission
/// never uses it and a rotated generation registers nothing.
#[tokio::test]
async fn bootstrap_is_exclusive_and_generation_pinned() -> Result<()> {
    let control = Memory::new(authority("fenced-v1", "{}"));
    assert!(open(&control, SessionMode::Publisher).await.is_err());
    let account = Arc::clone(&control) as Arc<dyn ControlMetadata>;
    let missing =
        PublisherSession::open_with_control(Arc::clone(&account), SANDBOX, "acct", SessionMode::Bootstrap, None);
    assert!(missing.await.is_err());
    let rotated = PublisherSession::open_with_control(
        Arc::clone(&account),
        SANDBOX,
        "acct",
        SessionMode::Bootstrap,
        Some(ROTATED),
    );
    assert!(format!("{:#}", rotated.await.err().context("rotated")?).contains("does not match expected"));
    assert!(control.members()?.is_empty());
    let bootstrap = open(&control, SessionMode::Bootstrap).await?;
    assert_eq!(bootstrap.mode(), SessionMode::Bootstrap);
    bootstrap.check_async().await?;
    assert!(
        open(&control, SessionMode::Bootstrap).await.is_err(),
        "second initializer"
    );
    let store = memory_store(&bootstrap, MutationRetries::Disabled)?;
    store.put(&Path::from("_delta_log/0.json"), payload()).await?;
    bootstrap.close_after_drain_async().await?;
    assert!(control.members()?.is_empty());
    assert_eq!(
        control.state().metadata.get("resetstate").map(String::as_str),
        Some("fenced-v1")
    );
    for (name, value) in [
        ("resetstate", "cleaning-v1"),
        ("resetstate", "ready-v1"),
        ("occupancy", "stopping"),
    ] {
        let mut metadata = authority("fenced-v1", "{}");
        metadata.insert(name.into(), value.into());
        metadata.insert("occupancyid".into(), ROTATED.into());
        let control = Memory::new(metadata);
        assert!(open(&control, SessionMode::Bootstrap).await.is_err(), "{name}={value}");
        assert!(control.members()?.is_empty());
    }
    Ok(())
}

/// Guarantees: `stopping` occupancy is a blocking hold: neither normal nor
/// bootstrap admission registers anything, while `testing` still admits normal
/// publishers (it is not `released`, so it never admits bootstrap).
#[tokio::test]
async fn stopping_occupancy_blocks_all_admission() -> Result<()> {
    for (state, mode) in [
        ("ready-v1", SessionMode::Publisher),
        ("fenced-v1", SessionMode::Bootstrap),
    ] {
        let mut metadata = authority(state, "{}");
        metadata.insert("occupancy".into(), "stopping".into());
        metadata.insert("occupancyid".into(), ROTATED.into());
        let control = Memory::new(metadata.clone());
        let error = open(&control, mode).await.err().context("stopping admitted")?;
        assert!(format!("{error:#}").contains("occupancy"), "{state}: {error:#}");
        assert!(control.members()?.is_empty());
        assert_eq!(control.state().metadata, metadata, "{state}: nothing written");
    }
    let mut testing = authority("ready-v1", "{}");
    testing.insert("occupancy".into(), "testing".into());
    testing.insert("occupancyid".into(), ROTATED.into());
    let control = Memory::new(testing.clone());
    let session = open(&control, SessionMode::Publisher).await?;
    assert_eq!(
        control.state().metadata.get("occupancy").map(String::as_str),
        Some("testing")
    );
    session.close_after_drain_async().await?;
    testing.insert("resetstate".into(), "fenced-v1".into());
    assert!(open(&Memory::new(testing), SessionMode::Bootstrap).await.is_err());
    Ok(())
}

/// Guarantees: an unknown clean-release outcome keeps the session closing
/// and failed; a later close reconciles only by observing its own absence
/// under the same generation, or retries removal if it did not apply.
#[tokio::test]
async fn unknown_release_reconciles_by_reread() -> Result<()> {
    for applied in [true, false] {
        let control = Memory::new(authority("ready-v1", "{}"));
        let session = open(&control, SessionMode::Publisher).await?;
        control.state().faults.push_back(Fault::Unknown { applied });
        let error = session.close_after_drain_async().await.err().context("unknown")?;
        assert!(error.downcast_ref::<UnknownTransition>().is_some());
        assert!(
            memory_store(&session, MutationRetries::Disabled)?
                .put(&Path::from("a"), payload())
                .await
                .is_err()
        );
        session.close_after_drain_async().await?;
        assert!(control.members()?.is_empty());
        assert_eq!(session.status().phase, Phase::Closed);
    }
    Ok(())
}

/// Guarantees: local tables share one process-wide guarded session that is
/// returned from the cache without re-registration until explicit close.
#[test]
fn local_shared_session_is_process_wide() -> Result<()> {
    let first = PublisherSession::shared_for_url(&Url::parse("file:///tmp/a")?)?;
    let second = PublisherSession::shared_for_url(&Url::parse("file:///tmp/b")?)?;
    assert!(Arc::ptr_eq(&first, &second));
    assert!(first.publisher_id().is_none());
    let root = tempfile::tempdir()?;
    let url = Url::from_directory_path(root.path()).map_err(|()| anyhow::anyhow!("path"))?;
    let store = first.store_for_url(&url)?;
    let path = Path::from_absolute_path(root.path().join("x"))?;
    futures::executor::block_on(store.put(&path, payload()))?;
    let copied = Path::from_absolute_path(root.path().join("y"))?;
    let renamed = Path::from_absolute_path(root.path().join("z"))?;
    futures::executor::block_on(store.copy(&path, &copied))?;
    futures::executor::block_on(store.rename(&copied, &renamed))?;
    assert!(root.path().join("z").exists() && !root.path().join("y").exists());
    first.close_after_drain()?;
    assert!(futures::executor::block_on(store.put(&path, payload())).is_err());
    Ok(())
}
