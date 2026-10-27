//! Persistent per-process publisher registration in sandbox control metadata.
//!
//! One [`PublisherSession`] per mutating process registers a fresh UUID in the
//! checkpoint control container's `publishers` metadata map before any data,
//! log or checkpoint mutation. A reset fences admissions and deletes only after
//! the same-generation member set is empty. A member leaves only through an
//! explicit clean close after every guarded mutation has a definite outcome.
//! Crashes, cancellations and ambiguous I/O leave the member registered: there
//! is no TTL, heartbeat or forced removal here, and `Drop` never touches the
//! network.
//!
//! Metadata transitions are serialized by a short owned container lease (Azure
//! `Set Container Metadata` does not honor `If-Match`); see
//! [`crate::azure_fence`]. The guard covers only writers that use it. Old,
//! pre-fence and arbitrary external writers must be stopped by the operator.

use crate::azure_fence::AzureControlContainer;
use crate::delta_io::shared_executor;
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use delta_kernel_default_engine::executor::TaskExecutor;
use futures::stream::{BoxStream, Stream, StreamExt};
use object_store::azure::AzureCredentialProvider;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, RenameOptions, UploadPart,
};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use url::Url;
use uuid::Uuid;

/// Reset state admitting normal publishers.
pub const READY_STATE: &str = "ready-v1";
/// Reset state admitting no normal publisher; existing members may drain.
pub const FENCED_STATE: &str = "fenced-v1";
/// Operator cleanup in progress; admits nothing.
pub const CLEANING_STATE: &str = "cleaning-v1";
/// Supported `publisherregistryversion`.
pub const REGISTRY_VERSION: &str = "1";
/// Maximum registered publishers.
pub const MAX_PUBLISHERS: usize = 16;
/// Maximum UTF-8 bytes of the `publishers` value.
pub const MAX_PUBLISHERS_BYTES: usize = 4096;
/// Maximum total container metadata, counted as `x-ms-meta-{name}: {value}\r\n`.
pub const MAX_METADATA_BYTES: usize = 8192;
/// Bounded metadata transaction attempts under contention.
pub const CAS_ATTEMPTS: u32 = 8;
/// Reset generation the bootstrap operator captured before launching the initializer.
pub const EXPECTED_GENERATION_ENV: &str = "ARROWCOL_EXPECTED_RESET_GENERATION";
/// Reset generation the runner captured for this process. Normal Azure
/// admission is pinned to it; it is never used for bootstrap.
pub const RESET_GENERATION_ENV: &str = "ARROWCOL_RESET_GENERATION";

/// Canonical configured generation from [`RESET_GENERATION_ENV`] for normal
/// publisher admission.
///
/// # Errors
/// Fails closed when the variable is missing, not Unicode or not a canonical
/// UUID. The error never echoes the value.
pub fn reset_generation_from_env() -> Result<String> {
    let Ok(generation) = std::env::var(RESET_GENERATION_ENV) else {
        bail!(
            "{RESET_GENERATION_ENV} is required for Azure publisher admission (the runner's captured reset \
             generation); nothing registered"
        );
    };
    if canonical_uuid(&generation).is_err() {
        bail!("{RESET_GENERATION_ENV} is not a canonical lowercase UUID; nothing registered");
    }
    Ok(generation)
}

/// Canonical expected generation from [`EXPECTED_GENERATION_ENV`].
///
/// # Errors
/// Fails when the variable is missing or not a canonical UUID.
pub fn expected_generation_from_env() -> Result<String> {
    let generation =
        std::env::var(EXPECTED_GENERATION_ENV).with_context(|| format!("{EXPECTED_GENERATION_ENV} is required"))?;
    canonical_uuid(&generation).with_context(|| format!("{EXPECTED_GENERATION_ENV} is invalid"))?;
    Ok(generation)
}

/// Versioned sandbox reset state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetState {
    Ready,
    Fenced,
    Cleaning,
}

/// Registered publisher state. `Uncertain` is never cleanly released.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberState {
    Active,
    Uncertain,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    generation: String,
    state: MemberState,
}

struct Publishers(BTreeMap<String, Member>);

impl<'de> Deserialize<'de> for Publishers {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct UniqueKeys;
        impl<'de> Visitor<'de> for UniqueKeys {
            type Value = Publishers;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a publisher map")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Publishers, A::Error> {
                let mut entries = BTreeMap::new();
                while let Some((key, member)) = map.next_entry::<String, Member>()? {
                    if entries.insert(key, member).is_some() {
                        return Err(de::Error::custom("duplicate publisher"));
                    }
                }
                Ok(Publishers(entries))
            }
        }
        deserializer.deserialize_map(UniqueKeys)
    }
}

/// Parse a canonical lowercase hyphenated UUID.
///
/// # Errors
/// Rejects malformed or noncanonical spellings.
pub fn canonical_uuid(value: &str) -> Result<Uuid> {
    let parsed = Uuid::parse_str(value).context("invalid UUID")?;
    if parsed.hyphenated().to_string() != value {
        bail!("noncanonical UUID");
    }
    Ok(parsed)
}

fn metadata_budget(metadata: &BTreeMap<String, String>) -> Result<()> {
    let size: usize = metadata
        .iter()
        .map(|(name, value)| "x-ms-meta-".len() + name.len() + ": ".len() + value.len() + "\r\n".len())
        .sum();
    if size > MAX_METADATA_BYTES {
        bail!("container metadata budget exhausted ({MAX_METADATA_BYTES} bytes); admission fails closed");
    }
    Ok(())
}

/// Sandbox occupancy recorded by the control plane; `stopping` is a blocking,
/// possibly ambiguous hold that admits neither publishers nor bootstrap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Occupancy {
    Released,
    Testing(Uuid),
    Stopping(Uuid),
}

/// Validated publisher registry and reset authority from container metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registry {
    pub generation: String,
    pub state: ResetState,
    /// Mandatory sandbox occupancy.
    pub occupancy: Occupancy,
    pub publishers: BTreeMap<Uuid, MemberState>,
}

impl Registry {
    /// Validate metadata names (lowercase, without `x-ms-meta-`).
    ///
    /// # Errors
    /// Rejects missing, malformed, duplicate, unknown, noncanonical, over-budget,
    /// unsupported-version, legacy-state or generation-mismatched authority.
    pub fn parse(metadata: &BTreeMap<String, String>, sandbox_id: &str) -> Result<Self> {
        let field = |name: &str| {
            metadata
                .get(name)
                .map(String::as_str)
                .with_context(|| format!("publisher authority metadata {name} missing"))
        };
        if field("sandboxid")? != sandbox_id {
            bail!("publisher authority sandbox ownership mismatch");
        }
        let state = match field("resetstate")? {
            READY_STATE => ResetState::Ready,
            FENCED_STATE => ResetState::Fenced,
            CLEANING_STATE => ResetState::Cleaning,
            _ => bail!("publisher authority requires versioned ready-v1/fenced-v1/cleaning-v1 state"),
        };
        let generation = field("resetgeneration")?;
        canonical_uuid(generation).context("invalid reset generation")?;
        if field("publisherregistryversion")? != REGISTRY_VERSION {
            bail!("publisher registry is missing or has an unsupported version");
        }
        let raw = field("publishers")?;
        if raw.len() > MAX_PUBLISHERS_BYTES {
            bail!("publisher registry exceeds {MAX_PUBLISHERS_BYTES} bytes");
        }
        let Publishers(entries) = serde_json::from_str(raw).context("publisher registry JSON is malformed")?;
        if entries.len() > MAX_PUBLISHERS {
            bail!("publisher registry exceeds {MAX_PUBLISHERS} slots");
        }
        let mut publishers = BTreeMap::new();
        for (publisher, member) in entries {
            let id = canonical_uuid(&publisher).context("publisher registry requires canonical UUIDs")?;
            if member.generation != generation {
                bail!("publisher registry contains a different authority generation");
            }
            publishers.insert(id, member.state);
        }
        let occupancy = match field("occupancy")? {
            "released" => Occupancy::Released,
            kind @ ("testing" | "stopping") => {
                let owner = canonical_uuid(field("occupancyid")?).context("invalid occupancy owner")?;
                if kind == "testing" {
                    Occupancy::Testing(owner)
                } else {
                    Occupancy::Stopping(owner)
                }
            }
            _ => bail!("publisher authority requires explicit occupancy released/testing/stopping"),
        };
        let registry = Self {
            generation: generation.to_owned(),
            state,
            occupancy,
            publishers,
        };
        if registry.encode_publishers()? != raw {
            bail!("publisher registry is not canonical compact JSON");
        }
        metadata_budget(metadata)?;
        Ok(registry)
    }

    fn encode_publishers(&self) -> Result<String> {
        let entries: BTreeMap<String, Member> = self
            .publishers
            .iter()
            .map(|(id, state)| {
                let member = Member {
                    generation: self.generation.clone(),
                    state: *state,
                };
                (id.hyphenated().to_string(), member)
            })
            .collect();
        let encoded = serde_json::to_string(&entries)?;
        if entries.len() > MAX_PUBLISHERS || encoded.len() > MAX_PUBLISHERS_BYTES {
            bail!("publisher registry budget exhausted ({MAX_PUBLISHERS} slots / {MAX_PUBLISHERS_BYTES} bytes)");
        }
        Ok(encoded)
    }

    /// Preserve all other metadata and replace only `publishers`.
    fn apply(&self, metadata: &BTreeMap<String, String>) -> Result<BTreeMap<String, String>> {
        let mut next = metadata.clone();
        next.insert("publishers".into(), self.encode_publishers()?);
        metadata_budget(&next)?;
        Ok(next)
    }
}
/// Container metadata read with its ETag (lowercase names without `x-ms-meta-`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataSnapshot {
    pub etag: String,
    pub metadata: BTreeMap<String, String>,
}

/// Result of one serialized metadata transaction.
#[derive(Debug)]
pub enum CasOutcome {
    /// The server definitively applied the replacement.
    Applied,
    /// Definitely not applied: busy, changed since `expected`, or lease lost.
    Conflict,
    /// Definitely not applied, for a non-retryable reason.
    Rejected(String),
    /// Sent without a definite response; it may have been applied.
    Unknown(String),
}

/// Test-only replacements for the Azure control container and blob endpoint
/// used by `DeltaTable::initialize_url_for_bootstrap_with_control`. Exists only
/// with the non-default `test-control` feature in builds with `debug_assertions`
/// (never release, even with `--all-features`); production bootstrap always
/// derives these from the URL, Azure CLI credential and subscription.
#[cfg(all(feature = "test-control", debug_assertions))]
#[derive(Debug)]
pub struct TestBootstrapControl {
    /// Serialized sandbox metadata (stands in for container metadata).
    pub metadata: Arc<dyn ControlMetadata>,
    /// Sandbox id the metadata must carry.
    pub sandbox_id: String,
    /// Storage account the table URL must name.
    pub account: String,
    /// Operator-captured expected reset generation.
    pub expected_generation: String,
    /// Inner data transport; always wrapped by the session guard.
    pub data: Arc<dyn ObjectStore>,
}

/// Serialized container-metadata transaction backend.
#[async_trait]
pub trait ControlMetadata: Send + Sync + fmt::Debug {
    /// Read current metadata.
    ///
    /// # Errors
    /// Returns transport, authentication or protocol failures.
    async fn read(&self) -> Result<MetadataSnapshot>;

    /// Replace all metadata only if the content still equals `expected`.
    async fn compare_and_set(&self, expected: &MetadataSnapshot, metadata: &BTreeMap<String, String>) -> CasOutcome;
}

/// A metadata transition was sent without a definite outcome. The member, if
/// any, keeps blocking reset; it is never adopted or re-enrolled implicitly.
#[derive(Debug)]
pub struct UnknownTransition {
    pub publisher: Uuid,
    pub operation: &'static str,
    detail: String,
}

impl fmt::Display for UnknownTransition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "publisher {} {} outcome unknown ({}); registration retained as a reset blocker",
            self.publisher, self.operation, self.detail
        )
    }
}

impl std::error::Error for UnknownTransition {}

fn is_unknown(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<UnknownTransition>())
}

async fn backoff(attempt: u32) {
    let base = 20_u64 << attempt.min(4);
    let jitter = u64::from(Uuid::new_v4().as_bytes()[0]) % base;
    tokio::time::sleep(Duration::from_millis(base + jitter)).await;
}

/// `decide` returns `None` when no write is needed.
async fn transition<F>(
    control: &dyn ControlMetadata,
    sandbox_id: &str,
    publisher: Uuid,
    operation: &'static str,
    mut decide: F,
) -> Result<()>
where
    F: FnMut(&Registry) -> Result<Option<Registry>> + Send,
{
    for attempt in 0..CAS_ATTEMPTS {
        let snapshot = control.read().await?;
        let registry = Registry::parse(&snapshot.metadata, sandbox_id)?;
        let Some(next) = decide(&registry)? else {
            return Ok(());
        };
        let metadata = next.apply(&snapshot.metadata)?;
        match control.compare_and_set(&snapshot, &metadata).await {
            CasOutcome::Applied => return Ok(()),
            CasOutcome::Conflict => backoff(attempt).await,
            CasOutcome::Rejected(detail) => bail!("publisher {operation} rejected: {detail}"),
            CasOutcome::Unknown(detail) => {
                return Err(UnknownTransition {
                    publisher,
                    operation,
                    detail,
                }
                .into());
            }
        }
    }
    bail!("publisher {operation} contention budget exhausted ({CAS_ATTEMPTS} attempts)")
}

/// Registration mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionMode {
    /// Normal publisher; admitted only in `ready-v1`.
    Publisher,
    /// Exclusive operator initializer; admitted only in `fenced-v1` with an
    /// empty registry and released occupancy.
    Bootstrap,
}

/// Whether a bound store may transparently resend a mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationRetries {
    /// One request per mutation: a definite response is the only attempt.
    Disabled,
    /// Hidden retries possible: every mutation outcome is uncertain, so the
    /// session can never close cleanly.
    Opaque,
}

/// Admission lifecycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    Open,
    /// Authority is no longer admitting; clean close is still allowed.
    Fenced(String),
    Closing,
    Closed,
    /// Generation changed or membership lost; no writes and no cleanup.
    Revoked(String),
}

/// Point-in-time session state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionStatus {
    pub phase: Phase,
    pub inflight: usize,
    pub uncertain: Option<String>,
}

#[derive(Debug)]
struct Lifecycle {
    phase: Phase,
    inflight: usize,
    uncertain: Option<String>,
    release_unknown: bool,
}

impl Lifecycle {
    fn refusal(&self) -> Option<String> {
        if let Some(reason) = &self.uncertain {
            return Some(format!("publisher session has an uncertain mutation outcome: {reason}"));
        }
        match &self.phase {
            Phase::Open => None,
            Phase::Fenced(reason) => Some(format!("publisher authority is not admitting: {reason}")),
            Phase::Closing | Phase::Closed => Some("publisher session is closing".into()),
            Phase::Revoked(reason) => Some(format!("publisher session revoked: {reason}")),
        }
    }
}

#[derive(Debug)]
struct Shared {
    lifecycle: Mutex<Lifecycle>,
}

impl Shared {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            lifecycle: Mutex::new(Lifecycle {
                phase: Phase::Open,
                inflight: 0,
                uncertain: None,
                release_unknown: false,
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Lifecycle> {
        self.lifecycle.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn admit(self: &Arc<Self>) -> object_store::Result<Permit> {
        let mut lifecycle = self.lock();
        if let Some(reason) = lifecycle.refusal() {
            return Err(refused(reason));
        }
        lifecycle.inflight += 1;
        Ok(Permit {
            shared: Arc::clone(self),
            settled: false,
        })
    }

    fn admitting(&self) -> object_store::Result<()> {
        self.lock().refusal().map_or(Ok(()), |reason| Err(refused(reason)))
    }

    fn revoked(&self) -> Option<String> {
        match &self.lock().phase {
            Phase::Revoked(reason) => Some(reason.clone()),
            _ => None,
        }
    }

    fn settle(&self, uncertain: Option<String>) {
        let mut lifecycle = self.lock();
        lifecycle.inflight = lifecycle.inflight.saturating_sub(1);
        if lifecycle.uncertain.is_none() {
            lifecycle.uncertain = uncertain;
        }
    }
}

fn refused(reason: String) -> object_store::Error {
    object_store::Error::Generic {
        store: "PublisherSession",
        source: reason.into(),
    }
}

/// In-flight mutation; dropping it before a definite outcome is uncertain.
#[derive(Debug)]
struct Permit {
    shared: Arc<Shared>,
    settled: bool,
}

impl Permit {
    fn settle(mut self, uncertain: Option<String>) {
        self.settled = true;
        self.shared.settle(uncertain);
    }

    fn finish<T>(self, result: &object_store::Result<T>, retries: MutationRetries) {
        self.settle(classify(result, retries));
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if !self.settled {
            self.shared
                .settle(Some("mutation dropped or cancelled before a definite outcome".into()));
        }
    }
}

/// `None` only for a definite outcome of a single-attempt mutation.
fn classify<T>(result: &object_store::Result<T>, retries: MutationRetries) -> Option<String> {
    use object_store::Error as E;
    if retries == MutationRetries::Opaque {
        return Some("mutation used opaque transport retries; earlier attempts may be outstanding".into());
    }
    match result {
        Ok(_)
        | Err(
            E::AlreadyExists { .. }
            | E::Precondition { .. }
            | E::NotModified { .. }
            | E::NotFound { .. }
            | E::PermissionDenied { .. }
            | E::Unauthenticated { .. }
            | E::InvalidPath { .. }
            | E::NotSupported { .. }
            | E::NotImplemented { .. }
            | E::UnknownConfigurationKey { .. },
        ) => None,
        Err(error) => Some(format!("ambiguous mutation failure: {error}")),
    }
}
struct Registration {
    control: Arc<dyn ControlMetadata>,
    sandbox_id: String,
    publisher: Uuid,
    generation: String,
}

/// One persistent publisher registration shared by every guarded store of a
/// mutating process in one account and control container.
pub struct PublisherSession {
    shared: Arc<Shared>,
    mode: SessionMode,
    account: Option<String>,
    credentials: Option<AzureCredentialProvider>,
    registration: Option<Registration>,
    close: tokio::sync::Mutex<()>,
}

impl fmt::Debug for PublisherSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublisherSession")
            .field("mode", &self.mode)
            .field("account", &self.account)
            .field("publisher", &self.publisher_id())
            .field("generation", &self.generation())
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

type Settled = Option<std::result::Result<Arc<PublisherSession>, String>>;

/// Per-key shared admission state. A key is recorded as `Admitting` before
/// any registration I/O, so a dropped caller can never cause a second
/// enrollment for the same key.
enum Slot {
    /// An owned admission task is running; it always settles the slot.
    Admitting(tokio::sync::watch::Receiver<Settled>),
    Ready(Arc<PublisherSession>),
    /// Unknown or abandoned admission: permanent, never re-enrolled.
    Poisoned(String),
}

type SessionCache = Mutex<HashMap<String, Slot>>;
static SESSIONS: OnceLock<SessionCache> = OnceLock::new();

fn sessions() -> MutexGuard<'static, HashMap<String, Slot>> {
    SESSIONS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Settles one `Admitting` slot exactly once, even if the admission task is
/// dropped (for example by runtime shutdown) before a definite outcome.
struct Admission {
    key: String,
    sender: tokio::sync::watch::Sender<Settled>,
    settled: bool,
}

impl Admission {
    fn finish(mut self, result: Result<Arc<PublisherSession>>) {
        self.settled = true;
        let mut cache = sessions();
        let settled = match result {
            Ok(session) => {
                cache.insert(self.key.clone(), Slot::Ready(Arc::clone(&session)));
                Ok(session)
            }
            Err(error) => {
                let message = format!("{error:#}");
                if is_unknown(&error) {
                    cache.insert(self.key.clone(), Slot::Poisoned(message.clone()));
                } else {
                    // Definite failure: nothing was written, so nothing is cached.
                    cache.remove(&self.key);
                }
                Err(message)
            }
        };
        self.sender.send_replace(Some(settled));
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let message = "shared publisher admission was abandoned before a definite outcome; any registration is \
                       retained as a reset blocker and never re-enrolled"
            .to_owned();
        sessions().insert(self.key.clone(), Slot::Poisoned(message.clone()));
        self.sender.send_replace(Some(Err(message)));
    }
}

pub(crate) fn azure_account(url: &Url) -> Result<String> {
    if url.scheme() == "az" {
        return std::env::var("AZURE_STORAGE_ACCOUNT")
            .or_else(|_| std::env::var("AZURE_STORAGE_ACCOUNT_NAME"))
            .context("missing Azure account");
    }
    Ok(url
        .host_str()
        .context("missing Azure account")?
        .split('.')
        .next()
        .context("missing Azure account")?
        .to_owned())
}

fn is_azure(url: &Url) -> bool {
    matches!(url.scheme(), "abfs" | "abfss" | "az")
}

fn cache_key(url: &Url) -> Result<String> {
    match url.scheme() {
        "file" => Ok("file".into()),
        _ if is_azure(url) => {
            let container =
                std::env::var("AZURE_STORAGE_CHECKPOINT_CONTAINER").unwrap_or_else(|_| "checkpoints".into());
            Ok(format!("{}/{container}", azure_account(url)?))
        }
        _ => bail!("only file and ADLS URLs are supported"),
    }
}

/// Run on the shared Delta I/O runtime from a thread outside it. Its two
/// workers would deadlock on a nested synchronous wait, so callers already on
/// that runtime are rejected; other Tokio runtimes and plain threads work.
fn block_on_shared<T, F>(future: F) -> Result<T>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    if std::thread::current().name() == Some("delta-io") {
        bail!("synchronous publisher call on the shared Delta I/O runtime would deadlock; use the async API");
    }
    shared_executor()?.block_on(future)
}

impl PublisherSession {
    /// Synchronous [`Self::shared_for_url_async`].
    ///
    /// # Errors
    /// See [`Self::shared_for_url_async`]; also rejects shared-runtime callers.
    pub fn shared_for_url(url: &Url) -> Result<Arc<Self>> {
        let url = url.clone();
        block_on_shared(async move { Self::shared_for_url_async(&url).await })
    }

    /// Process-wide publisher session for the URL's storage: one per Azure
    /// account and control container, one for all local tables. The cache
    /// holds strong references and is never evicted, so tables may open,
    /// close and reopen under one registration until explicit process close;
    /// a cached session is returned without network access.
    ///
    /// The key is recorded as in progress before any registration I/O and the
    /// admission runs as one owned task on the shared Delta I/O runtime that
    /// always settles the key. A dropped or timed-out caller therefore never
    /// causes a second enrollment: later callers join the same admission. An
    /// unknown or abandoned outcome is cached as a permanent error: no
    /// adoption and no re-enrollment. Definite failures wrote nothing and are
    /// not cached. Azure admission is pinned to [`RESET_GENERATION_ENV`].
    ///
    /// # Errors
    /// Returns admission, authority, authentication or unknown-outcome failures.
    pub async fn shared_for_url_async(url: &Url) -> Result<Arc<Self>> {
        let key = cache_key(url)?;
        let url = url.clone();
        Self::shared_admission(
            key,
            async move { Self::open_url(&url, SessionMode::Publisher, None).await },
        )
        .await
    }

    /// [`Self::shared_for_url_async`] cache semantics over an injected
    /// metadata backend, for protocol tests. `key` lives in its own namespace
    /// and never aliases a URL-derived key. Admission is pinned to
    /// `expected_generation`, as normal Azure admission is.
    ///
    /// # Errors
    /// See [`Self::shared_for_url_async`].
    pub async fn shared_with_control(
        key: &str,
        control: Arc<dyn ControlMetadata>,
        sandbox_id: &str,
        account: &str,
        expected_generation: &str,
    ) -> Result<Arc<Self>> {
        let sandbox_id = sandbox_id.to_owned();
        let account = account.to_owned();
        let expected = expected_generation.to_owned();
        Self::shared_admission(format!("control:{key}"), async move {
            Self::register(
                control,
                sandbox_id,
                account,
                None,
                SessionMode::Publisher,
                Some(&expected),
            )
            .await
        })
        .await
    }

    async fn shared_admission<F>(key: String, admit: F) -> Result<Arc<Self>>
    where
        F: Future<Output = Result<Arc<Self>>> + Send + 'static,
    {
        let mut receiver = {
            let mut cache = sessions();
            match cache.get(&key) {
                Some(Slot::Ready(session)) => return Ok(Arc::clone(session)),
                Some(Slot::Poisoned(error)) => bail!("{error}"),
                Some(Slot::Admitting(receiver)) => receiver.clone(),
                None => {
                    let executor = shared_executor()?;
                    let (sender, receiver) = tokio::sync::watch::channel(None);
                    cache.insert(key.clone(), Slot::Admitting(receiver.clone()));
                    let admission = Admission {
                        key,
                        sender,
                        settled: false,
                    };
                    let _runtime = executor.enter();
                    drop(tokio::spawn(async move {
                        let result = admit.await;
                        admission.finish(result);
                    }));
                    receiver
                }
            }
        };
        let settled = receiver
            .wait_for(Option::is_some)
            .await
            .map(|settled| settled.clone())
            .map_err(|_| anyhow::anyhow!("shared publisher admission ended without an outcome"))?;
        match settled {
            Some(Ok(session)) => Ok(session),
            Some(Err(error)) => bail!("{error}"),
            None => bail!("shared publisher admission ended without an outcome"),
        }
    }

    /// Synchronous [`Self::close_shared_async`].
    ///
    /// # Errors
    /// See [`Self::close_shared_async`]; also rejects shared-runtime callers.
    pub fn close_shared() -> Result<()> {
        block_on_shared(Self::close_shared_async())
    }

    /// Process shutdown: close every cached shared session after its guarded
    /// mutations drain. Never registers anything. Refuses without side effects
    /// while any shared admission is still in progress. Otherwise each ready
    /// session is attempted; a failed one stays registered. Any unknown or
    /// abandoned admission makes the result an error: the process is never
    /// reported clean while a possible member is unaccounted for.
    ///
    /// # Errors
    /// Returns in-progress, unknown-admission, close refusal, revocation or
    /// unknown-outcome failures.
    pub async fn close_shared_async() -> Result<()> {
        let (ready, poisoned) = {
            let Some(cache) = SESSIONS.get() else {
                return Ok(());
            };
            let cache = cache.lock().unwrap_or_else(PoisonError::into_inner);
            let pending = cache.values().filter(|slot| matches!(slot, Slot::Admitting(_))).count();
            if pending > 0 {
                bail!(
                    "{pending} shared publisher admissions still in progress with no definite outcome; process is \
                     not clean, close again after they settle"
                );
            }
            let mut ready = Vec::new();
            let mut poisoned = Vec::new();
            for slot in cache.values() {
                match slot {
                    Slot::Ready(session) => ready.push(Arc::clone(session)),
                    Slot::Poisoned(error) => poisoned.push(error.clone()),
                    Slot::Admitting(_) => {}
                }
            }
            (ready, poisoned)
        };
        let mut first = None;
        for session in ready {
            if let Err(error) = session.close_after_drain_async().await {
                first.get_or_insert(error);
            }
        }
        if let Some(error) = first {
            return Err(error);
        }
        if let Some(error) = poisoned.first() {
            bail!(
                "{} shared publisher admissions have unknown outcomes; process is not clean: {error}",
                poisoned.len()
            );
        }
        Ok(())
    }

    /// Synchronous [`Self::open_async`].
    ///
    /// # Errors
    /// See [`Self::open_async`]; also rejects shared-runtime callers.
    pub fn open(url: &Url) -> Result<Arc<Self>> {
        let url = url.clone();
        block_on_shared(async move { Self::open_async(&url).await })
    }

    /// Register a fresh publisher in `ready-v1` (uncached). Azure admission is
    /// pinned to [`RESET_GENERATION_ENV`] inside the leased transition.
    ///
    /// # Errors
    /// Returns admission, authority, authentication or unknown-outcome
    /// failures; fails closed when the configured generation is absent,
    /// invalid or differs from the authority.
    pub async fn open_async(url: &Url) -> Result<Arc<Self>> {
        Self::open_url(url, SessionMode::Publisher, None).await
    }

    /// Synchronous [`Self::open_bootstrap_async`].
    ///
    /// # Errors
    /// See [`Self::open_bootstrap_async`]; also rejects shared-runtime callers.
    pub fn open_bootstrap(url: &Url, expected_generation: &str) -> Result<Arc<Self>> {
        let url = url.clone();
        let expected = expected_generation.to_owned();
        block_on_shared(async move { Self::open_bootstrap_async(&url, &expected).await })
    }

    /// Register the exclusive bootstrap initializer under the generation the
    /// operator captured (uncached). The comparison happens inside the
    /// admission transaction, so a rotated generation writes nothing and is
    /// never adopted. Only drain and close follow; this never sets `ready-v1`.
    ///
    /// # Errors
    /// Fails closed unless `fenced-v1` at `expected_generation`, an empty
    /// registry and occupancy exactly `released`.
    pub async fn open_bootstrap_async(url: &Url, expected_generation: &str) -> Result<Arc<Self>> {
        Self::open_url(url, SessionMode::Bootstrap, Some(expected_generation)).await
    }

    /// Test-only synchronous bootstrap registration through injected control
    /// metadata; see [`TestBootstrapControl`].
    ///
    /// # Errors
    /// See [`Self::open_bootstrap_async`].
    #[cfg(all(feature = "test-control", debug_assertions))]
    pub fn open_bootstrap_with_control(
        control: Arc<dyn ControlMetadata>,
        sandbox_id: &str,
        account: &str,
        expected_generation: &str,
    ) -> Result<Arc<Self>> {
        let (sandbox_id, account, expected) = (
            sandbox_id.to_owned(),
            account.to_owned(),
            expected_generation.to_owned(),
        );
        block_on_shared(async move {
            Self::open_with_control(control, &sandbox_id, &account, SessionMode::Bootstrap, Some(&expected)).await
        })
    }

    async fn open_url(url: &Url, mode: SessionMode, expected_generation: Option<&str>) -> Result<Arc<Self>> {
        if url.scheme() == "file" {
            let mut session = Self::unregistered();
            session.mode = mode;
            return Ok(Arc::new(session));
        }
        if !is_azure(url) {
            bail!("only file and ADLS URLs are supported");
        }
        let configured;
        let expected_generation = match (mode, expected_generation) {
            (_, Some(expected)) => expected,
            (SessionMode::Publisher, None) => {
                configured = reset_generation_from_env()?;
                configured.as_str()
            }
            (SessionMode::Bootstrap, None) => {
                bail!("bootstrap requires the operator's captured expected reset generation")
            }
        };
        let account = azure_account(url)?;
        let credentials: AzureCredentialProvider = Arc::new(crate::azure_cli::BoundedAzureCli::default());
        let control = Arc::new(AzureControlContainer::new(&account, Arc::clone(&credentials))?);
        let sandbox_id = control.sandbox_id().to_owned();
        Self::register(
            control,
            sandbox_id,
            account,
            Some(credentials),
            mode,
            Some(expected_generation),
        )
        .await
    }

    fn unregistered() -> Self {
        Self {
            shared: Shared::new(),
            mode: SessionMode::Publisher,
            account: None,
            credentials: None,
            registration: None,
            close: tokio::sync::Mutex::new(()),
        }
    }

    /// Uncached local storage session: no cloud registry, still admission-guarded.
    #[must_use]
    pub fn local() -> Arc<Self> {
        Arc::new(Self::unregistered())
    }

    /// Register through an injected metadata backend (uncached). Bootstrap
    /// requires `expected_generation`; a publisher may pin one too (the
    /// production Azure paths always do).
    ///
    /// # Errors
    /// Returns admission, authority or unknown-outcome failures.
    pub async fn open_with_control(
        control: Arc<dyn ControlMetadata>,
        sandbox_id: &str,
        account: &str,
        mode: SessionMode,
        expected_generation: Option<&str>,
    ) -> Result<Arc<Self>> {
        Self::register(
            control,
            sandbox_id.to_owned(),
            account.to_owned(),
            None,
            mode,
            expected_generation,
        )
        .await
    }

    async fn register(
        control: Arc<dyn ControlMetadata>,
        sandbox_id: String,
        account: String,
        credentials: Option<AzureCredentialProvider>,
        mode: SessionMode,
        expected_generation: Option<&str>,
    ) -> Result<Arc<Self>> {
        if let Some(expected) = expected_generation {
            canonical_uuid(expected).context("invalid expected reset generation")?;
        } else if mode == SessionMode::Bootstrap {
            bail!("bootstrap requires the operator's captured expected reset generation");
        }
        let publisher = Uuid::new_v4();
        let mut generation = String::new();
        transition(control.as_ref(), &sandbox_id, publisher, "registration", |registry| {
            if let Some(expected) = expected_generation
                && registry.generation != expected
            {
                bail!(
                    "authority generation {} does not match expected {expected}; nothing registered",
                    registry.generation
                );
            }
            match mode {
                SessionMode::Publisher if registry.state != ResetState::Ready => {
                    bail!("publisher admission requires ready-v1 authority")
                }
                SessionMode::Publisher if matches!(registry.occupancy, Occupancy::Stopping(_)) => {
                    bail!("publisher admission is blocked while occupancy is stopping")
                }
                SessionMode::Bootstrap if registry.state != ResetState::Fenced => {
                    bail!("bootstrap admission requires fenced-v1 authority")
                }
                SessionMode::Bootstrap if !registry.publishers.is_empty() => {
                    bail!("bootstrap requires an exclusive empty publisher registry")
                }
                SessionMode::Bootstrap if registry.occupancy != Occupancy::Released => {
                    bail!("bootstrap requires released occupancy")
                }
                _ => {}
            }
            if registry.publishers.contains_key(&publisher) {
                bail!("publisher identity already registered; no implicit adoption");
            }
            if registry.publishers.len() >= MAX_PUBLISHERS {
                bail!("publisher registry is full ({MAX_PUBLISHERS} slots)");
            }
            let mut next = registry.clone();
            next.publishers.insert(publisher, MemberState::Active);
            generation.clone_from(&registry.generation);
            Ok(Some(next))
        })
        .await?;
        Ok(Arc::new(Self {
            shared: Shared::new(),
            mode,
            account: Some(account),
            credentials,
            registration: Some(Registration {
                control,
                sandbox_id,
                publisher,
                generation,
            }),
            close: tokio::sync::Mutex::new(()),
        }))
    }
    #[must_use]
    pub fn publisher_id(&self) -> Option<Uuid> {
        self.registration.as_ref().map(|registration| registration.publisher)
    }

    #[must_use]
    pub fn generation(&self) -> Option<&str> {
        self.registration
            .as_ref()
            .map(|registration| registration.generation.as_str())
    }

    /// Require that a persisted WAL epoch/generation equals the generation
    /// this session was admitted under. Never relabels: a mismatch must not
    /// be replayed into this sandbox. An unregistered local session has no
    /// reset authority and accepts any canonical generation.
    ///
    /// # Errors
    /// Rejects a noncanonical `persisted` value or any mismatch.
    pub fn require_generation(&self, persisted: &str) -> Result<()> {
        if canonical_uuid(persisted).is_err() {
            bail!("persisted WAL generation is not a canonical UUID; refusing replay");
        }
        if let Some(registration) = &self.registration
            && registration.generation != persisted
        {
            bail!(
                "persisted WAL generation {persisted} does not match admitted publisher generation {}; refusing \
                 replay or relabeling",
                registration.generation
            );
        }
        Ok(())
    }

    #[must_use]
    pub fn account(&self) -> Option<&str> {
        self.account.as_deref()
    }

    #[must_use]
    pub const fn mode(&self) -> SessionMode {
        self.mode
    }

    #[must_use]
    pub fn status(&self) -> SessionStatus {
        let lifecycle = self.shared.lock();
        SessionStatus {
            phase: lifecycle.phase.clone(),
            inflight: lifecycle.inflight,
            uncertain: lifecycle.uncertain.clone(),
        }
    }

    fn validate_url(&self, url: &Url) -> Result<()> {
        match (&self.account, url.scheme()) {
            (None, "file") => Ok(()),
            (Some(account), _) if is_azure(url) => {
                if azure_account(url)? != *account {
                    bail!("publisher session is bound to a different storage account");
                }
                Ok(())
            }
            _ => bail!("URL storage does not match the publisher session"),
        }
    }

    /// Guarded store for a table or checkpoint URL in the session's storage.
    /// Azure stores share the session credential and never retry transport.
    ///
    /// # Errors
    /// Rejects mismatched storage and invalid store configuration.
    pub fn store_for_url(self: &Arc<Self>, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        self.validate_url(url)?;
        let store: Arc<dyn ObjectStore> = if url.scheme() == "file" {
            Arc::new(object_store::local::LocalFileSystem::new())
        } else {
            let credentials = self
                .credentials
                .clone()
                .context("publisher session has no Azure credential")?;
            Arc::new(
                object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_url(url.as_str())
                    .with_credentials(credentials)
                    .with_retry(object_store::RetryConfig {
                        max_retries: 0,
                        retry_timeout: Duration::from_secs(10),
                        backoff: object_store::BackoffConfig::default(),
                    })
                    .with_client_options(object_store::ClientOptions::new().with_timeout(Duration::from_secs(10)))
                    .build()?,
            )
        };
        self.bind_store(url, store, MutationRetries::Disabled)
    }

    /// Guard every mutating surface of `store`; reads and lists are forwarded.
    /// Declare [`MutationRetries::Opaque`] unless the store sends each
    /// mutation exactly once. Server-side copy and rename are forwarded only
    /// for `file:` URLs: on Azure, Copy Blob is asynchronous (`202` with a
    /// pending `x-ms-copy-status`) and `object_store` returns `Ok` without
    /// checking completion, so those calls fail `NotSupported` before any I/O.
    ///
    /// # Errors
    /// Rejects a URL outside the session's storage account.
    pub fn bind_store(
        self: &Arc<Self>,
        url: &Url,
        store: Arc<dyn ObjectStore>,
        retries: MutationRetries,
    ) -> Result<Arc<dyn ObjectStore>> {
        self.validate_url(url)?;
        Ok(Arc::new(GuardedStore {
            inner: store,
            shared: Arc::clone(&self.shared),
            retries,
            server_copy: url.scheme() == "file",
        }))
    }

    /// Synchronous [`Self::check_async`].
    ///
    /// # Errors
    /// See [`Self::check_async`]; also rejects shared-runtime callers.
    pub fn check(self: &Arc<Self>) -> Result<()> {
        let session = Arc::clone(self);
        block_on_shared(async move { session.check_async().await })
    }

    /// Verify the same generation, own membership and an admitting state.
    /// Generation change or lost membership revokes the session; a fenced or
    /// cleaning state stops admissions but still allows clean close.
    ///
    /// # Errors
    /// Returns revocation, fencing, local refusal or read failures.
    pub async fn check_async(&self) -> Result<()> {
        if let Some(registration) = &self.registration {
            let snapshot = registration.control.read().await?;
            let registry = Registry::parse(&snapshot.metadata, &registration.sandbox_id)?;
            if registry.generation != registration.generation
                || !registry.publishers.contains_key(&registration.publisher)
            {
                let reason = "authority generation changed or registration missing".to_owned();
                let mut lifecycle = self.shared.lock();
                if lifecycle.phase != Phase::Closed {
                    lifecycle.phase = Phase::Revoked(reason.clone());
                }
                bail!("publisher session revoked: {reason}");
            }
            let expected = match self.mode {
                SessionMode::Publisher => ResetState::Ready,
                SessionMode::Bootstrap => ResetState::Fenced,
            };
            if registry.state != expected {
                let reason = format!("authority state is {:?}", registry.state);
                let mut lifecycle = self.shared.lock();
                if lifecycle.phase == Phase::Open {
                    lifecycle.phase = Phase::Fenced(reason.clone());
                }
                bail!("publisher authority is not admitting: {reason}");
            }
        }
        self.shared.admitting()?;
        Ok(())
    }

    /// Synchronous [`Self::close_after_drain_async`].
    ///
    /// # Errors
    /// See [`Self::close_after_drain_async`]; also rejects shared-runtime callers.
    pub fn close_after_drain(self: &Arc<Self>) -> Result<()> {
        let session = Arc::clone(self);
        block_on_shared(async move { session.close_after_drain_async().await })
    }

    /// Stop admissions, then remove only this publisher with a same-generation
    /// transaction once every guarded mutation has a definite outcome. Allowed
    /// while fenced so reset can drain. Refuses (member retained) while any
    /// mutation is in flight or uncertain; an uncertain member is marked so.
    ///
    /// # Errors
    /// Returns refusal, stale authority, rejection or unknown-outcome failures.
    pub async fn close_after_drain_async(&self) -> Result<()> {
        let _serial = self.close.lock().await;
        let (uncertain, release_unknown) = {
            let mut lifecycle = self.shared.lock();
            match &lifecycle.phase {
                Phase::Closed => return Ok(()),
                Phase::Revoked(reason) => bail!("publisher session revoked ({reason}); registration retained"),
                _ => {}
            }
            lifecycle.phase = Phase::Closing;
            if lifecycle.inflight > 0 {
                bail!(
                    "{} guarded mutations still in flight; join them, then close again",
                    lifecycle.inflight
                );
            }
            (lifecycle.uncertain.clone(), lifecycle.release_unknown)
        };
        let Some(registration) = &self.registration else {
            if let Some(reason) = uncertain {
                bail!("uncertain mutation outcome ({reason}); session cannot close cleanly");
            }
            self.shared.lock().phase = Phase::Closed;
            return Ok(());
        };
        if let Some(reason) = uncertain {
            let marked = transition(
                registration.control.as_ref(),
                &registration.sandbox_id,
                registration.publisher,
                "uncertain marking",
                |registry| {
                    if registry.generation != registration.generation {
                        bail!("authority generation changed");
                    }
                    match registry.publishers.get(&registration.publisher) {
                        None => bail!("registration missing"),
                        Some(MemberState::Uncertain) => Ok(None),
                        Some(MemberState::Active) => {
                            let mut next = registry.clone();
                            next.publishers.insert(registration.publisher, MemberState::Uncertain);
                            Ok(Some(next))
                        }
                    }
                },
            )
            .await;
            let note = marked
                .err()
                .map(|error| format!("; marking failed: {error:#}"))
                .unwrap_or_default();
            bail!("uncertain mutation outcome ({reason}); registration retained{note}");
        }
        let mut stale = false;
        let released = transition(
            registration.control.as_ref(),
            &registration.sandbox_id,
            registration.publisher,
            "clean release",
            |registry| {
                if registry.generation != registration.generation {
                    stale = true;
                    bail!("authority generation changed; cleanup refused");
                }
                match registry.publishers.get(&registration.publisher) {
                    // Read under a later lease: the earlier unknown removal applied.
                    None if release_unknown => Ok(None),
                    None => {
                        stale = true;
                        bail!("registration missing; cleanup refused")
                    }
                    Some(MemberState::Uncertain) => bail!("an uncertain publisher cannot be released"),
                    Some(MemberState::Active) => {
                        let mut next = registry.clone();
                        next.publishers.remove(&registration.publisher);
                        Ok(Some(next))
                    }
                }
            },
        )
        .await;
        let mut lifecycle = self.shared.lock();
        match released {
            Ok(()) => {
                lifecycle.phase = Phase::Closed;
                Ok(())
            }
            Err(error) => {
                if is_unknown(&error) {
                    lifecycle.release_unknown = true;
                } else if stale {
                    lifecycle.phase = Phase::Revoked(format!("{error:#}"));
                }
                Err(error)
            }
        }
    }
}
#[derive(Debug)]
struct GuardedStore {
    inner: Arc<dyn ObjectStore>,
    shared: Arc<Shared>,
    retries: MutationRetries,
    /// Server-side copy/rename has a definite synchronous outcome (local only).
    server_copy: bool,
}

impl GuardedStore {
    fn require_server_copy(&self, operation: &str) -> object_store::Result<()> {
        if self.server_copy {
            return Ok(());
        }
        Err(object_store::Error::NotSupported {
            source: format!(
                "{operation} is disabled for publisher-guarded remote stores: Azure Copy Blob may still be pending \
                 after Ok, so it cannot prove drain"
            )
            .into(),
        })
    }
}

impl fmt::Display for GuardedStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "PublisherGuarded({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for GuardedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let permit = self.shared.admit()?;
        let result = self.inner.put_opts(location, payload, opts).await;
        permit.finish(&result, self.retries);
        result
    }

    /// The upload holds its admission until complete or abort settles it.
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let permit = self.shared.admit()?;
        match self.inner.put_multipart_opts(location, opts).await {
            Ok(upload) => Ok(Box::new(GuardedUpload {
                inner: upload,
                shared: Arc::clone(&self.shared),
                retries: self.retries,
                permit: Some(permit),
            })),
            Err(error) => {
                let result: object_store::Result<Box<dyn MultipartUpload>> = Err(error);
                permit.finish(&result, self.retries);
                result
            }
        }
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    /// One admission covers the whole stream; dropping it early is uncertain.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        match self.shared.admit() {
            Ok(permit) => GuardedDelete {
                inner: self.inner.delete_stream(locations),
                permit: Some(permit),
                uncertain: None,
                retries: self.retries,
            }
            .boxed(),
            Err(error) => futures::stream::once(async move { Err(error) }).boxed(),
        }
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.require_server_copy("copy")?;
        let permit = self.shared.admit()?;
        let result = self.inner.copy_opts(from, to, options).await;
        permit.finish(&result, self.retries);
        result
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.require_server_copy("rename")?;
        let permit = self.shared.admit()?;
        let result = self.inner.rename_opts(from, to, options).await;
        permit.finish(&result, self.retries);
        result
    }
}

struct GuardedDelete {
    inner: BoxStream<'static, object_store::Result<Path>>,
    permit: Option<Permit>,
    uncertain: Option<String>,
    retries: MutationRetries,
}

impl Stream for GuardedDelete {
    type Item = object_store::Result<Path>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        match this.inner.poll_next_unpin(context) {
            Poll::Ready(Some(item)) => {
                if this.uncertain.is_none() {
                    this.uncertain = classify(&item, this.retries);
                }
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                if let Some(permit) = this.permit.take() {
                    permit.settle(this.uncertain.take());
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
struct GuardedUpload {
    inner: Box<dyn MultipartUpload>,
    shared: Arc<Shared>,
    retries: MutationRetries,
    /// Held until complete or abort has a definite outcome.
    permit: Option<Permit>,
}

fn finished() -> object_store::Error {
    refused("multipart upload already finished".into())
}

#[async_trait]
impl MultipartUpload for GuardedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if self.permit.is_none() {
            return Box::pin(futures::future::ready(Err(finished())));
        }
        let permit = match self.shared.admit() {
            Ok(permit) => permit,
            Err(error) => return Box::pin(futures::future::ready(Err(error))),
        };
        let part = self.inner.put_part(data);
        let retries = self.retries;
        Box::pin(async move {
            let result = part.await;
            permit.finish(&result, retries);
            result
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.shared.admitting()?;
        let permit = self.permit.take().ok_or_else(finished)?;
        let result = self.inner.complete().await;
        permit.finish(&result, self.retries);
        result
    }

    /// Allowed while closing or fenced to settle the upload, never once revoked.
    async fn abort(&mut self) -> object_store::Result<()> {
        if let Some(reason) = self.shared.revoked() {
            return Err(refused(format!("publisher session revoked: {reason}")));
        }
        let permit = self.permit.take().ok_or_else(finished)?;
        let result = self.inner.abort().await;
        permit.finish(&result, self.retries);
        result
    }
}
