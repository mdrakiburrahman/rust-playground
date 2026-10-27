//! Checkpoint control-container authority. Container metadata survives a
//! zero-blob sandbox reset and carries the versioned reset state and the
//! publisher registry.
//!
//! `Set Container Metadata` ignores `If-Match`, so every cooperative writer
//! serializes metadata replacement with a short owned container lease: acquire
//! a fresh 15 s lease, re-read and compare content under it, replace ALL
//! metadata with `x-ms-lease-id`, then release. A late write after the lease
//! expired or was lost fails with 412. Writers that omit the lease id can still
//! clobber metadata and must not exist. Leases serialize transitions only;
//! registry membership never expires.

use crate::publisher::{CasOutcome, ControlMetadata, MetadataSnapshot};
#[cfg(test)]
use crate::publisher::{Registry, ResetState};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use object_store::azure::{AzureCredential, AzureCredentialProvider};
use reqwest::{Method, StatusCode};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
#[cfg(test)]
use std::sync::Arc;
use std::time::Duration;

const API_VERSION: &str = "2023-11-03";
const LEASE_SECONDS: &str = "15";
const META_PREFIX: &str = "x-ms-meta-";

/// Lease-serialized metadata transactions on the checkpoint control container.
pub(crate) struct AzureControlContainer {
    credentials: AzureCredentialProvider,
    client: reqwest::Client,
    endpoint: String,
    sandbox_id: String,
}

impl fmt::Debug for AzureControlContainer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AzureControlContainer")
            .field("endpoint", &self.endpoint)
            .field("sandbox_id", &self.sandbox_id)
            .finish_non_exhaustive()
    }
}

enum Exchange {
    Response(reqwest::Response),
    /// Definitely never reached the service.
    NotSent(String),
    /// Possibly reached the service without a response.
    Ambiguous(String),
}

fn valid_metadata(metadata: &BTreeMap<String, String>) -> std::result::Result<(), String> {
    for (name, value) in metadata {
        let identifier = name.bytes().next().is_some_and(|byte| !byte.is_ascii_digit())
            && name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        let printable = value.bytes().all(|byte| (0x20..0x7f).contains(&byte)) && value.trim() == value;
        if !identifier || !printable {
            return Err(format!("metadata {name} cannot round-trip as a lowercase ASCII header"));
        }
    }
    Ok(())
}

impl AzureControlContainer {
    pub(crate) fn new(account: &str, credentials: AzureCredentialProvider) -> Result<Self> {
        let container = std::env::var("AZURE_STORAGE_CHECKPOINT_CONTAINER").unwrap_or_else(|_| "checkpoints".into());
        if account.is_empty()
            || !account.bytes().all(|byte| byte.is_ascii_alphanumeric())
            || container.is_empty()
            || !container
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            bail!("invalid Azure control container configuration");
        }
        let subscription =
            std::env::var("AZURE_SUBSCRIPTION_ID").context("Azure sandbox ownership configuration missing")?;
        let sandbox_id = format!("{:x}", Sha256::digest(format!("{subscription}:rust-playground")));
        Self::with_endpoint(
            format!("https://{account}.blob.core.windows.net/{container}"),
            sandbox_id[..12].to_owned(),
            credentials,
        )
    }

    /// Single-attempt client: reqwest never retries, and no write is replayed.
    pub(crate) fn with_endpoint(
        endpoint: String,
        sandbox_id: String,
        credentials: AzureCredentialProvider,
    ) -> Result<Self> {
        Ok(Self {
            credentials,
            client: reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?,
            endpoint,
            sandbox_id,
        })
    }

    pub(crate) fn sandbox_id(&self) -> &str {
        &self.sandbox_id
    }

    async fn send(&self, method: Method, query: &str, headers: &[(String, String)]) -> Exchange {
        let token = match self.credentials.get_credential().await {
            Ok(credential) => match credential.as_ref() {
                AzureCredential::BearerToken(token) => token.clone(),
                _ => return Exchange::NotSent("Azure control container requires bearer identity".into()),
            },
            Err(_) => return Exchange::NotSent("Azure control container authentication failed".into()),
        };
        let mut request = self
            .client
            .request(method.clone(), format!("{}?{query}", self.endpoint))
            .bearer_auth(token)
            .header("x-ms-version", API_VERSION)
            .header(
                "x-ms-date",
                chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
            );
        for (name, value) in headers {
            request = request.header(name.as_str(), value.as_str());
        }
        if method == Method::PUT {
            request = request.body(Vec::new());
        }
        match request.send().await {
            Ok(response) => Exchange::Response(response),
            Err(error) if error.is_builder() => Exchange::NotSent("invalid Azure control request".into()),
            Err(error) if error.is_connect() => Exchange::NotSent("Azure control connection failed".into()),
            Err(_) => Exchange::Ambiguous("Azure control request has no definite response".into()),
        }
    }

    async fn read_metadata(&self) -> Result<MetadataSnapshot> {
        let response = match self.send(Method::HEAD, "restype=container", &[]).await {
            Exchange::Response(response) => response,
            Exchange::NotSent(detail) | Exchange::Ambiguous(detail) => bail!("{detail}"),
        };
        if !response.status().is_success() {
            bail!("Azure control metadata unavailable (status {})", response.status());
        }
        let headers = response.headers();
        let mut metadata = BTreeMap::new();
        for name in headers.keys() {
            let Some(key) = name.as_str().strip_prefix(META_PREFIX) else {
                continue;
            };
            let mut values = headers.get_all(name).iter();
            let (Some(value), None) = (values.next(), values.next()) else {
                bail!("Azure control metadata {key} is duplicated");
            };
            let value = value
                .to_str()
                .with_context(|| format!("Azure control metadata {key} is not ASCII"))?;
            if metadata.insert(key.to_ascii_lowercase(), value.to_owned()).is_some() {
                bail!("Azure control metadata {key} is duplicated");
            }
        }
        let etag = headers
            .get("etag")
            .context("Azure control ETag missing")?
            .to_str()
            .context("Azure control ETag invalid")?
            .to_owned();
        Ok(MetadataSnapshot { etag, metadata })
    }

    async fn lease(&self, action: &str, lease_id: &str) -> Exchange {
        let mut headers = vec![("x-ms-lease-action".to_owned(), action.to_owned())];
        if action == "acquire" {
            headers.push(("x-ms-lease-duration".into(), LEASE_SECONDS.into()));
            headers.push(("x-ms-proposed-lease-id".into(), lease_id.into()));
        } else {
            headers.push(("x-ms-lease-id".into(), lease_id.into()));
        }
        self.send(Method::PUT, "comp=lease&restype=container", &headers).await
    }

    async fn leased_write(
        &self,
        lease_id: &str,
        expected: &MetadataSnapshot,
        metadata: &BTreeMap<String, String>,
    ) -> CasOutcome {
        match self.read_metadata().await {
            Ok(current) if current.metadata == expected.metadata => {}
            Ok(_) => return CasOutcome::Conflict,
            Err(error) => return CasOutcome::Rejected(format!("{error:#}")),
        }
        let mut headers = vec![("x-ms-lease-id".to_owned(), lease_id.to_owned())];
        headers.extend(
            metadata
                .iter()
                .map(|(name, value)| (format!("{META_PREFIX}{name}"), value.clone())),
        );
        match self
            .send(Method::PUT, "restype=container&comp=metadata", &headers)
            .await
        {
            Exchange::Response(response) if response.status() == StatusCode::OK => CasOutcome::Applied,
            // Lease expired or lost before the write: definitely not applied.
            Exchange::Response(response) if response.status() == StatusCode::PRECONDITION_FAILED => {
                CasOutcome::Conflict
            }
            Exchange::Response(response) => CasOutcome::Unknown(format!("metadata write status {}", response.status())),
            Exchange::NotSent(detail) => CasOutcome::Rejected(detail),
            Exchange::Ambiguous(detail) => CasOutcome::Unknown(detail),
        }
    }
}

#[async_trait]
impl ControlMetadata for AzureControlContainer {
    async fn read(&self) -> Result<MetadataSnapshot> {
        self.read_metadata().await
    }

    async fn compare_and_set(&self, expected: &MetadataSnapshot, metadata: &BTreeMap<String, String>) -> CasOutcome {
        if let Err(detail) = valid_metadata(metadata) {
            return CasOutcome::Rejected(detail);
        }
        let lease_id = uuid::Uuid::new_v4().hyphenated().to_string();
        match self.lease("acquire", &lease_id).await {
            Exchange::Response(response) if response.status() == StatusCode::CREATED => {}
            Exchange::Response(response) if response.status() == StatusCode::CONFLICT => return CasOutcome::Conflict,
            Exchange::Response(response) if !response.status().is_server_error() => {
                return CasOutcome::Rejected(format!("lease acquire status {}", response.status()));
            }
            Exchange::NotSent(detail) => return CasOutcome::Rejected(detail),
            // No metadata write was attempted; a lease we may hold is released
            // here or expires within the lease duration.
            Exchange::Response(_) | Exchange::Ambiguous(_) => {
                let _ = self.lease("release", &lease_id).await;
                return CasOutcome::Conflict;
            }
        }
        let outcome = self.leased_write(&lease_id, expected, metadata).await;
        let _ = self.lease("release", &lease_id).await;
        outcome
    }
}

/// Unregistered generation check, superseded in production by
/// [`crate::publisher::PublisherSession`]; kept to pin registry state parsing.
#[cfg(test)]
pub(crate) struct AzureResetFence {
    control: Arc<AzureControlContainer>,
    generation: String,
}

#[cfg(test)]
impl AzureResetFence {
    pub(crate) fn generation(&self) -> &str {
        &self.generation
    }

    pub(crate) async fn with_control(control: Arc<AzureControlContainer>, bootstrap: bool) -> Result<Self> {
        let generation = Self::authority(&control, bootstrap).await?.generation;
        Ok(Self { control, generation })
    }

    async fn authority(control: &AzureControlContainer, bootstrap: bool) -> Result<Registry> {
        let snapshot = control.read_metadata().await?;
        let registry = Registry::parse(&snapshot.metadata, control.sandbox_id())?;
        let expected = if bootstrap {
            ResetState::Fenced
        } else {
            ResetState::Ready
        };
        if registry.state != expected {
            bail!("Azure sandbox is fenced or not ready for this operation");
        }
        Ok(registry)
    }

    pub(crate) async fn check(&self) -> Result<()> {
        self.check_mode(false).await
    }

    pub(crate) async fn check_mode(&self, bootstrap: bool) -> Result<()> {
        if Self::authority(&self.control, bootstrap).await?.generation != self.generation {
            bail!("Azure reset generation changed; reopen under the new authority");
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::{PublisherSession, SessionMode, UnknownTransition};
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    const SANDBOX: &str = "0123456789ab";
    const GENERATION: &str = "00000000-0000-4000-8000-000000000001";

    #[derive(Debug, Clone)]
    struct Request {
        method: String,
        query: String,
        headers: BTreeMap<String, String>,
    }

    #[derive(Debug, Default)]
    struct Server {
        metadata: BTreeMap<String, String>,
        etag: u64,
        lease: Option<String>,
        /// A foreign holder keeps the lease.
        busy: bool,
        /// Apply the next metadata write, then drop the connection.
        drop_after_apply: bool,
        /// Change content between the caller's read and its leased re-read.
        mutate_on_acquire: u32,
        log: Vec<Request>,
    }

    type Shared = Arc<Mutex<Server>>;

    fn authority(state: &str) -> BTreeMap<String, String> {
        [
            ("sandboxid", SANDBOX),
            ("resetstate", state),
            ("resetgeneration", GENERATION),
            ("publisherregistryversion", "1"),
            ("publishers", "{}"),
            ("occupancy", "released"),
            ("operatornote", "keep"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
    }

    fn lock(server: &Shared) -> std::sync::MutexGuard<'_, Server> {
        server.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    async fn serve(server: Shared, mut stream: TcpStream) {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 4096];
        while !buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(read) => buffer.extend_from_slice(&chunk[..read]),
            }
        }
        let text = String::from_utf8_lossy(&buffer).into_owned();
        let mut lines = text.split("\r\n");
        let mut start = lines.next().unwrap_or_default().split(' ');
        let method = start.next().unwrap_or_default().to_owned();
        let target = start.next().unwrap_or_default();
        let query = target
            .split_once('?')
            .map(|(_, query)| query.to_owned())
            .unwrap_or_default();
        let headers: BTreeMap<String, String> = lines
            .take_while(|line| !line.is_empty())
            .filter_map(|line| line.split_once(": "))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.to_owned()))
            .collect();
        let request = Request {
            method: method.clone(),
            query: query.clone(),
            headers: headers.clone(),
        };
        let (status, extra, drop_connection) = {
            let mut state = lock(&server);
            state.log.push(request);
            let header = |name: &str| headers.get(name).cloned().unwrap_or_default();
            if method == "HEAD" {
                let mut extra = format!("ETag: \"{}\"\r\n", state.etag);
                for (name, value) in &state.metadata {
                    extra.push_str(&format!("x-ms-meta-{name}: {value}\r\n"));
                }
                ("200 OK", extra, false)
            } else if query.contains("comp=lease") {
                match header("x-ms-lease-action").as_str() {
                    "acquire" if state.busy || state.lease.is_some() => ("409 Conflict", String::new(), false),
                    "acquire" => {
                        state.lease = Some(header("x-ms-proposed-lease-id"));
                        if state.mutate_on_acquire > 0 {
                            state.mutate_on_acquire -= 1;
                            state.metadata.insert("operatornote".into(), "changed".into());
                            state.etag += 1;
                        }
                        ("201 Created", String::new(), false)
                    }
                    "release" if state.lease.as_deref() == Some(header("x-ms-lease-id").as_str()) => {
                        state.lease = None;
                        ("200 OK", String::new(), false)
                    }
                    _ => ("409 Conflict", String::new(), false),
                }
            } else if query.contains("comp=metadata") {
                if state.lease.is_none() || state.lease.as_deref() != Some(header("x-ms-lease-id").as_str()) {
                    ("412 Precondition Failed", String::new(), false)
                } else {
                    state.metadata = headers
                        .iter()
                        .filter_map(|(name, value)| {
                            name.strip_prefix("x-ms-meta-")
                                .map(|name| (name.to_owned(), value.clone()))
                        })
                        .collect();
                    state.etag += 1;
                    let drop_connection = std::mem::take(&mut state.drop_after_apply);
                    ("200 OK", String::new(), drop_connection)
                }
            } else {
                ("400 Bad Request", String::new(), false)
            }
        };
        if drop_connection {
            return;
        }
        let response = format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n{extra}\r\n");
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    }

    async fn start(metadata: BTreeMap<String, String>) -> Result<(Shared, Arc<AzureControlContainer>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server: Shared = Arc::new(Mutex::new(Server {
            metadata,
            ..Server::default()
        }));
        let accepted = Arc::clone(&server);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(Arc::clone(&accepted), stream));
            }
        });
        let credentials: AzureCredentialProvider = Arc::new(object_store::StaticCredentialProvider::new(
            AzureCredential::BearerToken("test-token".into()),
        ));
        let control =
            AzureControlContainer::with_endpoint(format!("http://{address}/checkpoints"), SANDBOX.into(), credentials)?;
        Ok((server, Arc::new(control)))
    }

    fn members(server: &Shared) -> Result<serde_json::Value> {
        Ok(serde_json::from_str(
            lock(server).metadata.get("publishers").context("publishers")?,
        )?)
    }

    /// Guarantees: registration acquires a fresh 15 s lease, writes all
    /// metadata with that lease id and no If-Match, releases it, and preserves
    /// foreign metadata; clean close removes only its own member.
    #[tokio::test]
    async fn leased_registration_preserves_metadata_and_releases() -> Result<()> {
        let (server, control) = start(authority("ready-v1")).await?;
        let session =
            PublisherSession::open_with_control(control, SANDBOX, "acct", SessionMode::Publisher, None).await?;
        let id = session.publisher_id().context("publisher id")?.to_string();
        assert_eq!(
            members(&server)?,
            serde_json::json!({ id.clone(): {"generation": GENERATION, "state": "active"} })
        );
        let log = lock(&server).log.clone();
        let acquire = log
            .iter()
            .find(|request| request.headers.get("x-ms-lease-action").map(String::as_str) == Some("acquire"))
            .context("acquire")?;
        assert_eq!(acquire.method, "PUT");
        assert_eq!(
            acquire.headers.get("x-ms-lease-duration").map(String::as_str),
            Some("15")
        );
        let lease = acquire
            .headers
            .get("x-ms-proposed-lease-id")
            .context("proposed")?
            .clone();
        let write = log
            .iter()
            .find(|request| request.query.contains("comp=metadata"))
            .context("write")?;
        assert_eq!(write.headers.get("x-ms-lease-id"), Some(&lease));
        assert!(!write.headers.contains_key("if-match"));
        assert_eq!(
            write.headers.get("authorization").map(String::as_str),
            Some("Bearer test-token")
        );
        assert!(log.iter().any(|request| {
            request.headers.get("x-ms-lease-action").map(String::as_str) == Some("release")
                && request.headers.get("x-ms-lease-id") == Some(&lease)
        }));
        let snapshot = lock(&server).metadata.clone();
        assert_eq!(snapshot.get("operatornote").map(String::as_str), Some("keep"));
        assert_eq!(snapshot.get("occupancy").map(String::as_str), Some("released"));
        assert!(lock(&server).lease.is_none());
        session.close_after_drain_async().await?;
        assert_eq!(members(&server)?, serde_json::json!({}));
        assert_eq!(
            lock(&server).metadata.get("operatornote").map(String::as_str),
            Some("keep")
        );
        Ok(())
    }

    /// Guarantees: content changed between read and leased re-read is a
    /// conflict that writes nothing; the next attempt applies to fresh content.
    #[tokio::test]
    async fn changed_content_under_lease_retries_without_clobbering() -> Result<()> {
        let (server, control) = start(authority("ready-v1")).await?;
        lock(&server).mutate_on_acquire = 1;
        PublisherSession::open_with_control(control, SANDBOX, "acct", SessionMode::Publisher, None).await?;
        let state = lock(&server);
        assert_eq!(state.metadata.get("operatornote").map(String::as_str), Some("changed"));
        assert_eq!(
            state
                .log
                .iter()
                .filter(|request| request.query.contains("comp=metadata"))
                .count(),
            1
        );
        Ok(())
    }

    /// Guarantees: a foreign lease holder exhausts the bounded budget with an
    /// explicit error and no metadata write.
    #[tokio::test]
    async fn busy_lease_exhausts_without_writing() -> Result<()> {
        let (server, control) = start(authority("ready-v1")).await?;
        lock(&server).busy = true;
        let error = PublisherSession::open_with_control(control, SANDBOX, "acct", SessionMode::Publisher, None)
            .await
            .err()
            .context("busy lease must fail")?;
        assert!(format!("{error:#}").contains("contention budget exhausted"));
        let state = lock(&server);
        assert!(!state.log.iter().any(|request| request.query.contains("comp=metadata")));
        assert_eq!(state.metadata.get("publishers").map(String::as_str), Some("{}"));
        Ok(())
    }

    /// Guarantees: an applied write whose response is lost is an unknown
    /// transition; the member stays registered as a blocker and is not adopted.
    #[tokio::test]
    async fn lost_write_response_is_unknown_and_member_blocks() -> Result<()> {
        let (server, control) = start(authority("ready-v1")).await?;
        lock(&server).drop_after_apply = true;
        let error = PublisherSession::open_with_control(control, SANDBOX, "acct", SessionMode::Publisher, None)
            .await
            .err()
            .context("lost response must fail")?;
        let unknown = error.downcast_ref::<UnknownTransition>().context("typed unknown")?;
        let members = members(&server)?;
        assert_eq!(members.as_object().map(serde_json::Map::len), Some(1));
        assert!(members.get(unknown.publisher.to_string()).is_some());
        Ok(())
    }

    /// Guarantees: a write without the held lease id is refused by 412 (lease
    /// lost) and treated as definitely not applied.
    #[tokio::test]
    async fn write_without_matching_lease_is_412_conflict() -> Result<()> {
        let (server, control) = start(authority("ready-v1")).await?;
        let snapshot = control.read().await?;
        let mut next = snapshot.metadata.clone();
        next.insert("operatornote".into(), "late".into());
        let outcome = control
            .leased_write("00000000-0000-4000-8000-0000000000ff", &snapshot, &next)
            .await;
        assert!(matches!(outcome, CasOutcome::Conflict));
        assert_eq!(
            lock(&server).metadata.get("operatornote").map(String::as_str),
            Some("keep")
        );
        Ok(())
    }

    /// Guarantees: the reset fence accepts only versioned states, so binaries
    /// expecting legacy ready/fenced fail closed, and checks the generation.
    #[tokio::test]
    async fn reset_fence_requires_versioned_state_and_same_generation() -> Result<()> {
        for legacy in ["ready", "fenced"] {
            let (_, control) = start(authority(legacy)).await?;
            assert!(
                AzureResetFence::with_control(control, legacy == "fenced")
                    .await
                    .is_err()
            );
        }
        let (_, control) = start(authority("cleaning-v1")).await?;
        assert!(AzureResetFence::with_control(control, true).await.is_err());
        let mut no_occupancy = authority("ready-v1");
        no_occupancy.remove("occupancy");
        let (_, control) = start(no_occupancy).await?;
        assert!(AzureResetFence::with_control(control, false).await.is_err());
        let (server, control) = start(authority("ready-v1")).await?;
        let fence = AzureResetFence::with_control(control, false).await?;
        assert_eq!(fence.generation(), GENERATION);
        fence.check().await?;
        assert!(fence.check_mode(true).await.is_err());
        lock(&server)
            .metadata
            .insert("resetgeneration".into(), "00000000-0000-4000-8000-000000000002".into());
        assert!(fence.check().await.is_err());
        Ok(())
    }
}
