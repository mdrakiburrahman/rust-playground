//! Opt-in attribution of application-owned Azure HTTP traffic.
//!
//! Set both `ARROWCOL_QAP_CLIENT_MARKER=arrowcol-qap/<run-id>/<trial>` and
//! `ARROWCOL_QAP_HTTP_TRACE=<absolute private JSONL path>`. Each transport
//! attempt durably records a request before network I/O and a response before
//! returning its result. A cancelled attempt may leave an unmatched request;
//! consumers must treat unmatched requests as incomplete attribution.
//! Traced clients disable redirects: canonical Azure endpoints are required.
//!
//! [`StorageBridge`] is an explicit, zero-row fixture transport using dedicated
//! sequential NDJSON pipes. Its parent owns authentication and effect admission.
//! It never enables itself from environment configuration, performs direct network
//! fallback, or grants provider execution clearance. Its original deadline includes drain.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use fs2::FileExt;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService, ReqwestConnector,
};
use object_store::{ClientConfigKey, ClientOptions};
use reqwest::header::{HeaderValue, USER_AGENT};
use serde::Serialize;
use url::Url;
use uuid::Uuid;

pub(crate) use storage_bridge::Outcome as StorageBridgeOutcome;
pub use storage_bridge::{StorageBridge, StorageBridgeAuthority};

#[derive(Debug)]
pub(crate) struct DeniedBridgeCredentials;

#[async_trait]
impl object_store::CredentialProvider for DeniedBridgeCredentials {
    type Credential = object_store::azure::AzureCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        Err(store_error(invalid(
            "child authentication is forbidden for the storage bridge",
        )))
    }
}

mod storage_bridge {
    use std::collections::BTreeMap;
    use std::fmt;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use http_body_util::BodyExt;
    use object_store::client::{HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService};
    use serde::{Deserialize, Serialize};
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
    use tokio::sync::Mutex;
    use tokio::time::Instant;
    use url::Url;

    const MAX_BODY: usize = 1_048_576;
    const MAX_FRAME: usize = 4_194_304;
    const JSON_SAFE: u64 = 9_007_199_254_740_991;

    /// Immutable authority for one explicitly bridged zero-row fixture.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct StorageBridgeAuthority {
        pub campaign_id: String,
        pub cell_run_id: String,
        pub owner_id: String,
        pub fencing_token: u64,
        pub storage_account: String,
        pub table_root_url: String,
        pub control_container_url: String,
        pub sandbox_id: String,
        pub reset_generation: String,
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum Code {
        AdmissionRefused,
        DeadlineExceeded,
        InvalidRequest,
        Protocol,
        ResponseLost,
        TransportUnavailable,
        Poisoned,
    }

    impl fmt::Display for Code {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                Self::AdmissionRefused => "admission_refused",
                Self::DeadlineExceeded => "deadline_exceeded",
                Self::InvalidRequest => "invalid_request",
                Self::Protocol => "protocol",
                Self::ResponseLost => "response_lost",
                Self::TransportUnavailable => "transport_unavailable",
                Self::Poisoned => "poisoned",
            })
        }
    }

    pub(crate) enum Outcome {
        Response(HttpResponse),
        NotSent(Code),
        Unknown(Code),
    }

    #[derive(Serialize)]
    struct Request<'a> {
        schema_version: u8,
        kind: &'static str,
        request_id: u64,
        authority: &'a StorageBridgeAuthority,
        original_deadline_unix_ms: u64,
        remaining_timeout_ms: u64,
        target: &'a str,
        method: &'a str,
        url: &'a str,
        headers: Vec<(String, String)>,
        body: String,
        body_sha256: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_sha256: Option<String>,
    }

    #[derive(Deserialize)]
    #[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
    enum Reply {
        Response {
            schema_version: u8,
            kind: String,
            request_id: u64,
            request_sha256: String,
            status: u16,
            headers: Vec<(String, String)>,
            body: String,
            body_sha256: String,
            truncated: bool,
        },
        DefinitelyNotSent {
            schema_version: u8,
            kind: String,
            request_id: u64,
            request_sha256: String,
            error_code: Code,
        },
        Unknown {
            schema_version: u8,
            kind: String,
            request_id: u64,
            request_sha256: String,
            error_code: Code,
        },
    }

    struct State {
        reader: BufReader<Pin<Box<dyn AsyncRead + Send>>>,
        writer: Pin<Box<dyn AsyncWrite + Send>>,
        next_id: u64,
        poisoned: bool,
    }

    /// Dedicated sequential pipes; never performs authentication or direct networking.
    pub struct StorageBridge {
        authority: StorageBridgeAuthority,
        original_deadline_unix_ms: u64,
        deadline: Instant,
        data_endpoint: Url,
        data_prefix: String,
        state: Mutex<State>,
        planned_object_operation_ids: Option<Vec<String>>,
        next_planned_object_operation: AtomicUsize,
    }

    impl fmt::Debug for StorageBridge {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("StorageBridge").finish_non_exhaustive()
        }
    }

    fn invalid() -> anyhow::Error {
        anyhow::anyhow!("invalid storage bridge configuration")
    }

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn canonical(value: &serde_json::Value) -> Result<Vec<u8>, serde_json::Error> {
        fn sorted(value: &serde_json::Value) -> serde_json::Value {
            match value {
                serde_json::Value::Object(map) => {
                    let ordered: BTreeMap<_, _> = map.iter().collect();
                    serde_json::Value::Object(ordered.into_iter().map(|(k, v)| (k.clone(), sorted(v))).collect())
                }
                serde_json::Value::Array(items) => serde_json::Value::Array(items.iter().map(sorted).collect()),
                value => value.clone(),
            }
        }
        serde_json::to_vec(&sorted(value))
    }

    fn header_allowed(name: &str, response: bool) -> bool {
        let name = name.to_ascii_lowercase();
        name.starts_with("x-ms-meta-")
            || matches!(
                name.as_str(),
                "content-length"
                    | "content-type"
                    | "content-md5"
                    | "date"
                    | "x-ms-version"
                    | "x-ms-date"
                    | "x-arrowcol-operation-id"
                    | "x-ms-client-request-id"
                    | "x-ms-request-id"
                    | "x-ms-lease-id"
            )
            || if response {
                matches!(
                    name.as_str(),
                    "etag"
                        | "last-modified"
                        | "server"
                        | "accept-ranges"
                        | "content-range"
                        | "x-ms-error-code"
                        | "x-ms-blob-type"
                        | "x-ms-blob-content-md5"
                        | "x-ms-lease-status"
                        | "x-ms-lease-state"
                        | "x-ms-lease-duration"
                        | "x-ms-creation-time"
                        | "x-ms-server-encrypted"
                        | "transfer-encoding"
                )
            } else {
                matches!(
                    name.as_str(),
                    "if-match"
                        | "if-none-match"
                        | "if-modified-since"
                        | "if-unmodified-since"
                        | "range"
                        | "x-ms-range"
                        | "x-ms-lease-action"
                        | "x-ms-lease-duration"
                        | "x-ms-proposed-lease-id"
                        | "x-ms-blob-type"
                        | "user-agent"
                        | "accept"
                        | "accept-encoding"
                )
            }
    }

    impl StorageBridge {
        /// Bind dedicated protocol pipes to the original absolute deadline.
        ///
        /// # Errors
        /// Rejects invalid authority, credential-bearing endpoints and expired budgets.
        pub fn new<R, W>(
            authority: StorageBridgeAuthority,
            original_deadline_unix_ms: u64,
            reader: R,
            writer: W,
        ) -> anyhow::Result<Arc<Self>>
        where
            R: AsyncRead + Send + 'static,
            W: AsyncWrite + Send + 'static,
        {
            for id in [
                &authority.campaign_id,
                &authority.cell_run_id,
                &authority.owner_id,
                &authority.reset_generation,
            ] {
                crate::publisher::canonical_uuid(id)?;
            }
            if !(1..=JSON_SAFE).contains(&authority.fencing_token)
                || original_deadline_unix_ms > JSON_SAFE
                || authority.sandbox_id.is_empty()
                || authority.sandbox_id.len() > 256
            {
                return Err(invalid());
            }
            let root = Url::parse(&authority.table_root_url).map_err(|_| invalid())?;
            if root.scheme() != "abfss"
                || !root.path().ends_with('/')
                || root.path() == "/"
                || root.query().is_some()
                || root.fragment().is_some()
                || root.password().is_some()
                || !(3..=24).contains(&authority.storage_account.len())
                || !authority
                    .storage_account
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                || root.host_str() != Some(format!("{}.dfs.core.windows.net", authority.storage_account).as_str())
                || root.port().is_some()
                || root.as_str() != authority.table_root_url
                || root.path().contains("//")
                || !root
                    .path()
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_./".contains(&b))
            {
                return Err(invalid());
            }
            let data_path = object_store::path::Path::parse(root.path()).map_err(|_| invalid())?;
            if format!("/{data_path}/") != root.path() {
                return Err(invalid());
            }
            let container = root.username();
            if !(3..=63).contains(&container.len())
                || container.starts_with('-')
                || container.ends_with('-')
                || container.contains("--")
                || !container
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            {
                return Err(invalid());
            }
            let origin = format!("https://{}.blob.core.windows.net", authority.storage_account);
            let control = Url::parse(&authority.control_container_url).map_err(|_| invalid())?;
            if control.origin().ascii_serialization() != origin
                || control.query().is_some()
                || control.fragment().is_some()
                || !control.username().is_empty()
                || control.password().is_some()
                || control.path().trim_matches('/').is_empty()
                || control.path().trim_matches('/').contains('/')
                || control.path().ends_with('/')
            {
                return Err(invalid());
            }
            let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
            let remaining = original_deadline_unix_ms
                .checked_sub(now)
                .filter(|x| *x > 0)
                .ok_or_else(invalid)?;
            let data_endpoint = Url::parse(&format!("{origin}/{container}")).map_err(|_| invalid())?;
            let data_prefix = format!("{data_path}/");
            Ok(Arc::new(Self {
                authority,
                original_deadline_unix_ms,
                deadline: Instant::now() + Duration::from_millis(remaining),
                data_endpoint,
                data_prefix,
                planned_object_operation_ids: None,
                next_planned_object_operation: AtomicUsize::new(0),
                state: Mutex::new(State {
                    reader: BufReader::new(Box::pin(reader)),
                    writer: Box::pin(writer),
                    next_id: 1,
                    poisoned: false,
                }),
            }))
        }
        /// Bind immutable object-store operation identities for a plan-driven initialization.
        ///
        /// # Errors
        /// Rejects invalid operation markers or an invalid bridge configuration.
        pub(crate) fn new_with_planned_object_operation_ids<R, W>(
            authority: StorageBridgeAuthority,
            original_deadline_unix_ms: u64,
            operation_ids: Vec<String>,
            reader: R,
            writer: W,
        ) -> anyhow::Result<Arc<Self>>
        where
            R: AsyncRead + Send + 'static,
            W: AsyncWrite + Send + 'static,
        {
            if operation_ids.is_empty()
                || operation_ids.len() > 16
                || operation_ids.iter().enumerate().any(|(index, id)| {
                    id.is_empty()
                        || id.len() > 64
                        || !id
                            .bytes()
                            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                        || operation_ids[..index].contains(id)
                })
            {
                return Err(invalid());
            }
            let mut bridge = Self::new(authority, original_deadline_unix_ms, reader, writer)?;
            let Some(inner) = Arc::get_mut(&mut bridge) else {
                return Err(invalid());
            };
            inner.planned_object_operation_ids = Some(operation_ids);
            Ok(bridge)
        }

        pub(crate) fn authority(&self) -> &StorageBridgeAuthority {
            &self.authority
        }

        pub(crate) async fn poison(&self) {
            self.state.lock().await.poisoned = true;
        }

        pub(crate) fn validate_root(&self, url: &Url) -> anyhow::Result<()> {
            if url.as_str() != self.authority.table_root_url {
                return Err(invalid());
            }
            Ok(())
        }

        fn validate_request(&self, request: &HttpRequest, target: &str) -> Result<(), Code> {
            let url = Url::parse(&request.uri().to_string()).map_err(|_| Code::InvalidRequest)?;
            if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
                return Err(Code::InvalidRequest);
            }
            let mut query = BTreeMap::new();
            for (key, value) in url.query_pairs() {
                if query.insert(key.into_owned(), value.into_owned()).is_some() {
                    return Err(Code::InvalidRequest);
                }
            }
            let method = request.method().as_str();
            let allowed = match target {
                "control" => {
                    let mut endpoint = url.clone();
                    endpoint.set_query(None);
                    endpoint.as_str() == self.authority.control_container_url
                        && query.get("restype").is_some_and(|x| x == "container")
                        && match (method, query.get("comp").map(String::as_str)) {
                            ("HEAD", None) => query.len() == 1,
                            ("PUT", Some("lease")) => {
                                query.len() == 2
                                    && request
                                        .headers()
                                        .get("x-ms-lease-action")
                                        .is_some_and(|x| x == "acquire" || x == "release")
                            }
                            ("PUT", Some("metadata")) => query.len() == 2,
                            _ => false,
                        }
                        && request.body().content_length() == 0
                }
                "object_store" => {
                    let prefix = format!("{}/{}", self.data_endpoint.path(), self.data_prefix);
                    url.origin() == self.data_endpoint.origin()
                        && match method {
                            "GET" if url.path() == self.data_endpoint.path() => {
                                let start_from_valid = match query.get("startFrom") {
                                    Some(start_from) => {
                                        let expected_prefix = format!("{}_delta_log/", self.data_prefix);
                                        let is_directory_cursor = start_from == expected_prefix.trim_end_matches('/');
                                        let is_version_cursor =
                                            start_from.strip_prefix(&expected_prefix).is_some_and(|version| {
                                                version.len() == 20 && version.bytes().all(|byte| byte.is_ascii_digit())
                                            });
                                        let mut canonical_query = url::form_urlencoded::Serializer::new(String::new());
                                        canonical_query
                                            .append_pair("restype", "container")
                                            .append_pair("comp", "list")
                                            .append_pair("prefix", &expected_prefix)
                                            .append_pair("startFrom", start_from);
                                        let canonical_query = canonical_query.finish();
                                        query.len() == 4
                                            && query.get("prefix") == Some(&expected_prefix)
                                            && (is_directory_cursor || is_version_cursor)
                                            && url.query() == Some(canonical_query.as_str())
                                    }
                                    None => true,
                                };
                                query.len() >= 3
                                    && query.len() <= 5
                                    && query.get("restype").is_some_and(|x| x == "container")
                                    && query.get("comp").is_some_and(|x| x == "list")
                                    && query.get("prefix").is_some_and(|x| x.starts_with(&self.data_prefix))
                                    && start_from_valid
                                    && query.keys().all(|x| {
                                        matches!(
                                            x.as_str(),
                                            "restype" | "comp" | "prefix" | "marker" | "maxresults" | "startFrom"
                                        )
                                    })
                            }
                            "GET" | "HEAD" => {
                                query.is_empty() && url.path().starts_with(&format!("{prefix}_delta_log/"))
                            }
                            "PUT" => {
                                query.is_empty()
                                    && url.path() == format!("{prefix}_delta_log/00000000000000000000.json")
                                    && request.headers().get("if-none-match").is_some_and(|x| x == "*")
                            }
                            _ => false,
                        }
                }
                _ => false,
            };
            if !allowed || request.body().content_length() > MAX_BODY {
                return Err(Code::InvalidRequest);
            }
            for (name, value) in request.headers() {
                if !header_allowed(name.as_str(), false) || value.to_str().is_err() {
                    return Err(Code::InvalidRequest);
                }
            }
            Ok(())
        }

        pub(crate) async fn exchange(&self, mut request: HttpRequest, target: &str) -> Outcome {
            let mut state = self.state.lock().await;
            if state.poisoned {
                return Outcome::Unknown(Code::Poisoned);
            }
            if target == "object_store" && request.headers().contains_key("x-arrowcol-operation-id") {
                return Outcome::NotSent(Code::InvalidRequest);
            }
            if let Err(code) = self.validate_request(&request, target) {
                return Outcome::NotSent(code);
            }
            let lease_release = target == "control"
                && request
                    .headers()
                    .get("x-ms-lease-action")
                    .is_some_and(|value| value == "release");
            let mut bytes = Vec::new();
            while let Some(frame) = request.body_mut().frame().await {
                let Ok(frame) = frame else {
                    return Outcome::NotSent(Code::InvalidRequest);
                };
                let Ok(data) = frame.into_data() else {
                    return Outcome::NotSent(Code::InvalidRequest);
                };
                if data.len() > MAX_BODY - bytes.len() {
                    return Outcome::NotSent(Code::InvalidRequest);
                }
                bytes.extend_from_slice(&data);
            }
            let Ok(body) = String::from_utf8(bytes) else {
                return Outcome::NotSent(Code::InvalidRequest);
            };
            let deadline = self.deadline.min(Instant::now() + Duration::from_secs(10));
            let remaining = deadline.saturating_duration_since(Instant::now());
            if target == "object_store" {
                if let Some(operation_ids) = &self.planned_object_operation_ids {
                    let index = self.next_planned_object_operation.fetch_add(1, Ordering::Relaxed);
                    let Some(operation_id) = operation_ids.get(index) else {
                        return Outcome::NotSent(Code::InvalidRequest);
                    };
                    let Ok(value) = reqwest::header::HeaderValue::from_bytes(operation_id.as_bytes()) else {
                        return Outcome::NotSent(Code::InvalidRequest);
                    };
                    request.headers_mut().insert("x-arrowcol-operation-id", value);
                }
            }
            let Ok(remaining_timeout_ms) = u64::try_from(remaining.as_millis()) else {
                return Outcome::NotSent(Code::DeadlineExceeded);
            };
            if remaining_timeout_ms == 0 || state.next_id > JSON_SAFE {
                return Outcome::NotSent(Code::DeadlineExceeded);
            }
            let headers = request
                .headers()
                .iter()
                .map(|(k, v)| v.to_str().map(|value| (k.to_string(), value.to_owned())))
                .collect::<Result<Vec<_>, _>>();
            let Ok(mut headers) = headers else {
                return Outcome::NotSent(Code::InvalidRequest);
            };
            headers.sort_unstable();
            let url = request.uri().to_string();
            let mut envelope = Request {
                schema_version: 1,
                kind: "storage_http_request",
                request_id: state.next_id,
                authority: &self.authority,
                original_deadline_unix_ms: self.original_deadline_unix_ms,
                remaining_timeout_ms,
                target,
                method: request.method().as_str(),
                url: &url,
                headers,
                body_sha256: digest(body.as_bytes()),
                body,
                request_sha256: None,
            };
            let Ok(value) = serde_json::to_value(&envelope) else {
                return Outcome::NotSent(Code::InvalidRequest);
            };
            let Ok(unsigned) = canonical(&value) else {
                return Outcome::NotSent(Code::InvalidRequest);
            };
            let request_sha256 = digest(&unsigned);
            envelope.request_sha256 = Some(request_sha256.clone());
            let Ok(value) = serde_json::to_value(envelope) else {
                return Outcome::NotSent(Code::InvalidRequest);
            };
            let Ok(mut frame) = canonical(&value) else {
                return Outcome::NotSent(Code::InvalidRequest);
            };
            if frame.len() + 1 > MAX_FRAME {
                return Outcome::NotSent(Code::InvalidRequest);
            }
            frame.push(b'\n');
            // Set before the first write: dropping this future must never permit a second forward.
            state.poisoned = true;
            state.next_id += 1;
            let result = tokio::time::timeout_at(deadline, async {
                state.writer.write_all(&frame).await?;
                state.writer.flush().await?;
                let mut reply = Vec::new();
                loop {
                    let available = state.reader.fill_buf().await?;
                    if available.is_empty() {
                        return Err(std::io::Error::other("bridge EOF"));
                    }
                    let length = available
                        .iter()
                        .position(|b| *b == b'\n')
                        .map_or(available.len(), |n| n + 1);
                    if length > MAX_FRAME - reply.len() {
                        return Err(std::io::Error::other("bridge frame limit"));
                    }
                    let complete = available[length - 1] == b'\n';
                    reply.extend_from_slice(&available[..length]);
                    state.reader.consume(length);
                    if complete {
                        break;
                    }
                }
                Ok::<_, std::io::Error>(reply)
            })
            .await;
            let Ok(Ok(reply)) = result else {
                return Outcome::Unknown(Code::ResponseLost);
            };
            if Instant::now() >= deadline {
                return Outcome::Unknown(Code::DeadlineExceeded);
            }
            let Ok(response) = serde_json::from_slice::<Reply>(&reply) else {
                return Outcome::Unknown(Code::Protocol);
            };
            let matches = |version, kind: &str, id, hash: &str| {
                version == 1 && kind == "storage_http_response" && id == state.next_id - 1 && hash == request_sha256
            };
            match response {
                Reply::Response {
                    schema_version,
                    kind,
                    request_id,
                    request_sha256,
                    status,
                    headers,
                    body,
                    body_sha256,
                    truncated,
                } => {
                    if !matches(schema_version, &kind, request_id, &request_sha256)
                        || truncated
                        || body.len() > MAX_BODY
                        || digest(body.as_bytes()) != body_sha256
                        || !(200..=599).contains(&status)
                        || ((300..=399).contains(&status) && status != 304)
                    {
                        return Outcome::Unknown(Code::Protocol);
                    }
                    let mut response = HttpResponse::new(body.into());
                    let Ok(status) = reqwest::StatusCode::from_u16(status) else {
                        return Outcome::Unknown(Code::Protocol);
                    };
                    *response.status_mut() = status;
                    for (name, value) in headers {
                        if !header_allowed(&name, true) {
                            return Outcome::Unknown(Code::Protocol);
                        }
                        let Ok(name) = reqwest::header::HeaderName::from_bytes(name.as_bytes()) else {
                            return Outcome::Unknown(Code::Protocol);
                        };
                        let Ok(value) = reqwest::header::HeaderValue::from_str(&value) else {
                            return Outcome::Unknown(Code::Protocol);
                        };
                        response.headers_mut().append(name, value);
                    }
                    if !state.reader.buffer().is_empty() {
                        return Outcome::Unknown(Code::Protocol);
                    }
                    state.poisoned = lease_release && !status.is_success();
                    Outcome::Response(response)
                }
                Reply::DefinitelyNotSent {
                    schema_version,
                    kind,
                    request_id,
                    request_sha256,
                    error_code,
                } => {
                    if !matches(schema_version, &kind, request_id, &request_sha256)
                        || !matches!(
                            error_code,
                            Code::AdmissionRefused
                                | Code::DeadlineExceeded
                                | Code::InvalidRequest
                                | Code::TransportUnavailable
                        )
                        || !state.reader.buffer().is_empty()
                    {
                        return Outcome::Unknown(Code::Protocol);
                    }
                    state.poisoned = lease_release;
                    Outcome::NotSent(error_code)
                }
                Reply::Unknown {
                    schema_version,
                    kind,
                    request_id,
                    request_sha256,
                    error_code,
                } => {
                    if !matches(schema_version, &kind, request_id, &request_sha256) {
                        return Outcome::Unknown(Code::Protocol);
                    }
                    Outcome::Unknown(error_code)
                }
            }
        }
    }

    #[derive(Debug)]
    pub(super) struct Service(pub Arc<StorageBridge>);

    #[async_trait]
    impl HttpService for Service {
        async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
            let (kind, code) = match self.0.exchange(request, "object_store").await {
                Outcome::Response(response) => return Ok(response),
                Outcome::NotSent(code) => (HttpErrorKind::Connect, code),
                Outcome::Unknown(code) => (HttpErrorKind::Unknown, code),
            };
            Err(HttpError::new(
                kind,
                std::io::Error::other(format!("storage bridge {code}")),
            ))
        }
    }

    #[cfg(test)]
    mod bridge_tests {
        use super::*;
        use crate::publisher::PublisherSession;
        use futures::TryStreamExt;
        use object_store::{ObjectStoreExt, PutMode, PutOptions};
        use serde_json::{Value, json};
        use tokio::io::{AsyncReadExt, DuplexStream};
        use tokio::sync::Notify;

        const GENERATION: &str = "00000000-0000-0000-0000-000000000003";
        const UNRELATED: &str = "00000000-0000-0000-0000-000000000009";
        const ROOT: &str = "abfss://data@offlineaccount.dfs.core.windows.net/owned-run/table/";
        const KERNEL_TABLE_PATH: &str = "fixture/campaigns/00000000-0000-4000-8000-000000000001/runs/00000000-0000-4000-8000-000000000002/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states/";
        const KERNEL_LOG_PREFIX: &str = "fixture/campaigns/00000000-0000-4000-8000-000000000001/runs/00000000-0000-4000-8000-000000000002/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states/_delta_log/";

        fn authority() -> StorageBridgeAuthority {
            StorageBridgeAuthority {
                campaign_id: "00000000-0000-0000-0000-000000000001".into(),
                cell_run_id: "00000000-0000-0000-0000-000000000004".into(),
                owner_id: "00000000-0000-0000-0000-000000000002".into(),
                fencing_token: 7,
                storage_account: "offlineaccount".into(),
                table_root_url: ROOT.into(),
                control_container_url: "https://offlineaccount.blob.core.windows.net/checkpoints".into(),
                sandbox_id: "offline-sandbox".into(),
                reset_generation: GENERATION.into(),
            }
        }

        fn deadline(milliseconds: u64) -> anyhow::Result<u64> {
            Ok(u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())? + milliseconds)
        }

        fn bridge(milliseconds: u64) -> anyhow::Result<(Arc<StorageBridge>, DuplexStream)> {
            let (child, parent) = tokio::io::duplex(MAX_FRAME);
            let (read, write) = tokio::io::split(child);
            Ok((
                StorageBridge::new(authority(), deadline(milliseconds)?, read, write)?,
                parent,
            ))
        }

        fn head() -> anyhow::Result<HttpRequest> {
            let mut request = HttpRequest::new(object_store::client::HttpRequestBody::empty());
            *request.method_mut() = reqwest::Method::HEAD;
            *request.uri_mut() =
                "https://offlineaccount.blob.core.windows.net/checkpoints?restype=container".parse()?;
            Ok(request)
        }

        fn object_list(uri: &str) -> anyhow::Result<HttpRequest> {
            let mut request = HttpRequest::new(object_store::client::HttpRequestBody::empty());
            *request.method_mut() = reqwest::Method::GET;
            *request.uri_mut() = uri.parse()?;
            Ok(request)
        }

        fn response(request: &Value, status: u16, headers: Vec<(String, String)>, body: &str) -> Value {
            json!({
                "schema_version": 1, "kind": "storage_http_response",
                "request_id": request["request_id"], "request_sha256": request["request_sha256"],
                "outcome": "response", "status": status, "headers": headers,
                "body": body, "body_sha256": digest(body.as_bytes()), "truncated": false,
            })
        }

        async fn read_request(reader: &mut BufReader<tokio::io::ReadHalf<DuplexStream>>) -> anyhow::Result<Value> {
            let mut line = String::new();
            anyhow::ensure!(reader.read_line(&mut line).await? > 0, "offline bridge EOF");
            let mut value: Value = serde_json::from_str(&line)?;
            let actual = value
                .as_object_mut()
                .ok_or_else(invalid)?
                .remove("request_sha256")
                .ok_or_else(invalid)?;
            anyhow::ensure!(
                actual == digest(&canonical(&value)?),
                "canonical request digest mismatch"
            );
            value
                .as_object_mut()
                .ok_or_else(invalid)?
                .insert("request_sha256".into(), actual);
            anyhow::ensure!(value["body_sha256"] == digest(value["body"].as_str().ok_or_else(invalid)?.as_bytes()));
            Ok(value)
        }

        struct Fixture {
            metadata: BTreeMap<String, String>,
            requests: Vec<Value>,
            object: Option<String>,
            responses: Vec<Value>,
            release_fault: Option<(&'static str, usize)>,
        }

        impl Fixture {
            fn ready() -> Self {
                Self {
                    metadata: BTreeMap::from([
                        ("sandboxid".into(), "offline-sandbox".into()),
                        ("resetstate".into(), "ready-v1".into()),
                        ("resetgeneration".into(), GENERATION.into()),
                        ("publisherregistryversion".into(), "1".into()),
                        (
                            "publishers".into(),
                            json!({UNRELATED: {"generation": GENERATION, "state": "active"}}).to_string(),
                        ),
                        ("occupancy".into(), "released".into()),
                        ("unrelated".into(), "preserve-me".into()),
                    ]),
                    requests: Vec::new(),
                    object: None,
                    responses: Vec::new(),
                    release_fault: None,
                }
            }
        }

        async fn serve(
            parent: DuplexStream,
            fixture: Arc<Mutex<Fixture>>,
            hold_put: Option<Arc<Notify>>,
        ) -> anyhow::Result<()> {
            let (read, mut write) = tokio::io::split(parent);
            let mut reader = BufReader::new(read);
            loop {
                let request = match read_request(&mut reader).await {
                    Ok(request) => request,
                    Err(_) => return Ok(()),
                };
                let mut state = fixture.lock().await;
                state.requests.push(request.clone());
                let method = request["method"].as_str().ok_or_else(invalid)?;
                let url = Url::parse(request["url"].as_str().ok_or_else(invalid)?)?;
                let headers: Vec<(String, String)> = serde_json::from_value(request["headers"].clone())?;
                anyhow::ensure!(
                    headers
                        .iter()
                        .all(|(name, _)| !name.eq_ignore_ascii_case("authorization"))
                );
                if headers.iter().any(|(k, v)| k == "x-ms-lease-action" && v == "release")
                    && let Some((fault, remaining)) = &mut state.release_fault
                {
                    *remaining -= 1;
                    if *remaining == 0 {
                        let mut reply = response(&request, 500, Vec::new(), "");
                        match *fault {
                            "unknown" | "not_sent" => {
                                reply = json!({
                                    "schema_version":1,"kind":"storage_http_response",
                                    "request_id":request["request_id"],"request_sha256":request["request_sha256"],
                                    "outcome":if *fault == "unknown" {"unknown"} else {"definitely_not_sent"},
                                    "error_code":if *fault == "unknown" {"response_lost"} else {"admission_refused"}
                                });
                            }
                            "malformed" => {
                                write.write_all(b"{\n").await?;
                                return Ok(());
                            }
                            "eof" => return Ok(()),
                            _ => {}
                        }
                        state.responses.push(reply.clone());
                        drop(state);
                        write.write_all(&serde_json::to_vec(&reply)?).await?;
                        write.write_all(b"\n").await?;
                        write.flush().await?;
                        continue;
                    }
                }
                let mut status = 200;
                let mut output_headers = Vec::new();
                let mut body = String::new();
                if request["target"] == "control" {
                    if method == "HEAD" {
                        output_headers.push(("etag".into(), format!("\"{}\"", state.requests.len())));
                        output_headers.extend(
                            state
                                .metadata
                                .iter()
                                .map(|(k, v)| (format!("x-ms-meta-{k}"), v.clone())),
                        );
                    } else if url
                        .query_pairs()
                        .any(|(key, value)| key == "comp" && value == "metadata")
                    {
                        state.metadata = headers
                            .iter()
                            .filter_map(|(k, v)| k.strip_prefix("x-ms-meta-").map(|k| (k.to_owned(), v.clone())))
                            .collect();
                    } else if headers.iter().any(|(k, v)| k == "x-ms-lease-action" && v == "acquire") {
                        status = 201;
                    }
                } else if method == "PUT" {
                    if let Some(notify) = &hold_put {
                        notify.notify_one();
                        drop(state);
                        std::future::pending::<()>().await;
                        unreachable!();
                    }
                    anyhow::ensure!(headers.iter().any(|(k, v)| k == "if-none-match" && v == "*"));
                    if state.object.is_some() {
                        status = 409;
                        output_headers.push(("x-ms-error-code".into(), "BlobAlreadyExists".into()));
                        body =
                            "<Error><Code>BlobAlreadyExists</Code><Message>offline collision</Message></Error>".into();
                    } else {
                        state.object = Some(request["body"].as_str().ok_or_else(invalid)?.to_owned());
                        status = 201;
                        output_headers.push(("etag".into(), "\"offline-object\"".into()));
                        output_headers.push(("last-modified".into(), "Tue, 17 Nov 2026 03:00:00 GMT".into()));
                    }
                } else if url.query_pairs().any(|(key, value)| key == "comp" && value == "list") {
                    body = "<?xml version=\"1.0\"?><EnumerationResults ServiceEndpoint=\"https://offlineaccount.blob.core.windows.net\" ContainerName=\"data\"><Blobs></Blobs><NextMarker /></EnumerationResults>".into();
                } else if let Some(object) = &state.object {
                    body = object.clone();
                    output_headers.push(("etag".into(), "\"offline-object\"".into()));
                    output_headers.push(("last-modified".into(), "Tue, 17 Nov 2026 03:00:00 GMT".into()));
                    output_headers.push(("content-length".into(), body.len().to_string()));
                } else {
                    status = 404;
                }
                let reply = response(&request, status, output_headers, &body);
                state.responses.push(reply.clone());
                drop(state);
                write.write_all(&serde_json::to_vec(&reply)?).await?;
                write.write_all(b"\n").await?;
                write.flush().await?;
            }
        }

        #[tokio::test]
        async fn production_session_sdk_control_and_drain_use_one_unsigned_bridge() -> anyhow::Result<()> {
            let (bridge, parent) = bridge(30_000)?;
            let fixture = Arc::new(Mutex::new(Fixture::ready()));
            let server = tokio::spawn(serve(parent, Arc::clone(&fixture), None));
            let url = Url::parse(ROOT)?;
            let session = PublisherSession::open_bridged_async(&url, Arc::clone(&bridge)).await?;
            let publisher = session.publisher_id().ok_or_else(invalid)?.to_string();
            let store = session.store_for_url(&url)?;
            let empty = store
                .list(Some(&object_store::path::Path::from("owned-run/table/")))
                .try_collect::<Vec<_>>()
                .await?;
            assert!(empty.is_empty());
            let path = object_store::path::Path::from("owned-run/table/_delta_log/00000000000000000000.json");
            store
                .put_opts(
                    &path,
                    "{\"offline_sdk_transport_witness\":true}\n".into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await?;
            assert!(store.get(&path).await?.bytes().await?.starts_with(b"{"));
            assert!(matches!(
                store
                    .put_opts(
                        &path,
                        "must-not-replace".into(),
                        PutOptions {
                            mode: PutMode::Create,
                            ..Default::default()
                        }
                    )
                    .await,
                Err(object_store::Error::AlreadyExists { .. })
            ));
            session.close_after_drain_async().await?;
            let state = fixture.lock().await;
            assert!(state.metadata["publishers"].contains(UNRELATED));
            assert!(!state.metadata["publishers"].contains(&publisher));
            assert_eq!(state.metadata["unrelated"], "preserve-me");
            assert!(
                state
                    .requests
                    .iter()
                    .any(|v| v["target"] == "object_store" && v["method"] == "PUT")
            );
            assert!(
                state
                    .requests
                    .iter()
                    .any(|v| v["target"] == "control" && v["method"] == "PUT")
            );
            assert!(
                state
                    .requests
                    .windows(2)
                    .all(|pair| pair[0]["request_id"].as_u64() < pair[1]["request_id"].as_u64())
            );
            eprintln!(
                "D47_OFFLINE_GOLDEN={}",
                json!({"offline_protocol_fixture_only":true,"requests":state.requests,"responses":state.responses})
            );
            drop(state);
            server.abort();
            Ok(())
        }

        #[tokio::test]
        async fn azure_kernel_offset_listing_emits_canonical_directory_and_version_cursors() -> anyhow::Result<()> {
            let root = format!("abfss://tables@offlineproof.dfs.core.windows.net/{KERNEL_TABLE_PATH}");
            let mut authority = authority();
            authority.storage_account = "offlineproof".into();
            authority.control_container_url = "https://offlineproof.blob.core.windows.net/checkpoints".into();
            authority.table_root_url = root.clone();
            let (child, parent) = tokio::io::duplex(MAX_FRAME);
            let (read, write) = tokio::io::split(child);
            let bridge = StorageBridge::new(authority, deadline(10_000)?, read, write)?;
            let fixture = Arc::new(Mutex::new(Fixture::ready()));
            let server = tokio::spawn(serve(parent, Arc::clone(&fixture), None));

            let url = Url::parse(&root)?;
            let session = PublisherSession::open_bridged_async(&url, Arc::clone(&bridge)).await?;
            let store = session.store_for_url(&url)?;
            let prefix = object_store::path::Path::parse(KERNEL_LOG_PREFIX)?;
            assert!(
                store
                    .list_with_offset(Some(&prefix), &prefix)
                    .try_collect::<Vec<_>>()
                    .await?
                    .is_empty()
            );
            let version_cursor = object_store::path::Path::parse(format!("{KERNEL_LOG_PREFIX}00000000000000000000"))?;
            assert!(
                store
                    .list_with_offset(Some(&prefix), &version_cursor)
                    .try_collect::<Vec<_>>()
                    .await?
                    .is_empty()
            );

            let requests = fixture.lock().await.requests.clone();
            let lists: Vec<_> = requests
                .iter()
                .filter(|request| request["target"] == "object_store" && request["method"] == "GET")
                .collect();
            assert_eq!(lists.len(), 2);
            let encoded_prefix = KERNEL_LOG_PREFIX.replace('/', "%2F");
            let expected_directory_url = format!(
                "https://offlineproof.blob.core.windows.net/tables?restype=container&comp=list&prefix={encoded_prefix}&startFrom={}",
                KERNEL_LOG_PREFIX.trim_end_matches('/').replace('/', "%2F")
            );
            let encoded_version_cursor = format!("{KERNEL_LOG_PREFIX}00000000000000000000").replace('/', "%2F");
            let expected_version_url = format!(
                "https://offlineproof.blob.core.windows.net/tables?restype=container&comp=list&prefix={encoded_prefix}&startFrom={}",
                encoded_version_cursor
            );
            assert_eq!(lists[0]["url"], expected_directory_url);
            assert_eq!(lists[1]["url"], expected_version_url);
            session.close_after_drain_async().await?;
            server.abort();
            Ok(())
        }

        #[tokio::test]
        async fn azure_list_start_from_rejects_out_of_scope_cursors() -> anyhow::Result<()> {
            let (bridge, mut parent) = bridge(5_000)?;
            let prefix = "owned-run/table/_delta_log/";
            let list_url = |prefix: &str, start_from: &str| {
                let mut query = url::form_urlencoded::Serializer::new(String::new());
                query
                    .append_pair("restype", "container")
                    .append_pair("comp", "list")
                    .append_pair("prefix", prefix)
                    .append_pair("startFrom", start_from);
                format!("https://offlineaccount.blob.core.windows.net/data?{}", query.finish())
            };
            for start_from in [
                "owned-run/other-table/_delta_log",
                "owned-run/table/_delta_log/_last_checkpoint",
                "owned-run/table/_delta_log/0000000000000000000",
                "owned-run/table/_delta_log/000000000000000000000",
                "owned-run/table/_delta_log/0000000000000000000a",
                "owned-run/table/_delta_log/00000000000000000000.json",
                "owned-run/table/_delta_log/+0000000000000000000",
                "owned-run/table/_delta_log/../00000000000000000000",
                "owned-run/table/_delta_log/%2F00000000000000000000",
            ] {
                let url = list_url(prefix, start_from);
                assert!(
                    matches!(
                        bridge.exchange(object_list(&url)?, "object_store").await,
                        Outcome::NotSent(Code::InvalidRequest)
                    ),
                    "{url}",
                    url = url
                );
            }
            assert!(matches!(
                bridge
                    .exchange(
                        object_list(&list_url("owned-run/table/_commits/", "owned-run/table/_commits"))?,
                        "object_store"
                    )
                    .await,
                Outcome::NotSent(Code::InvalidRequest)
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(20), parent.read_u8())
                    .await
                    .is_err()
            );
            Ok(())
        }

        #[tokio::test]
        async fn azure_list_start_from_rejects_noncanonical_query_shape() -> anyhow::Result<()> {
            let (bridge, mut parent) = bridge(5_000)?;
            let prefix = "owned-run/table/_delta_log/";
            let cursor = "owned-run/table/_delta_log/00000000000000000000";
            let encoded_prefix = prefix.replace('/', "%2F");
            let encoded_cursor = cursor.replace('/', "%2F");
            for query in [
                format!("restype=container&comp=list&prefix={encoded_prefix}&startFrom={encoded_cursor}&marker="),
                format!("restype=container&prefix={encoded_prefix}&comp=list&startFrom={encoded_cursor}"),
                format!(
                    "restype=container&comp=list&prefix={encoded_prefix}&startFrom={}",
                    cursor
                ),
                format!(
                    "restype=container&comp=list&prefix={encoded_prefix}&startFrom={}",
                    encoded_cursor.replace("%2F", "%2f")
                ),
                format!(
                    "restype=container&comp=list&prefix={encoded_prefix}&startFrom={}",
                    encoded_cursor.replace("%2F", "%252F")
                ),
                format!(
                    "restype=container&comp=list&prefix={encoded_prefix}&startFrom={encoded_cursor}&startFrom={encoded_cursor}"
                ),
            ] {
                let url = format!("https://offlineaccount.blob.core.windows.net/data?{query}");
                assert!(
                    matches!(
                        bridge.exchange(object_list(&url)?, "object_store").await,
                        Outcome::NotSent(Code::InvalidRequest)
                    ),
                    "{url}"
                );
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(20), parent.read_u8())
                    .await
                    .is_err()
            );
            Ok(())
        }

        #[tokio::test]
        async fn final_release_failure_never_reports_clean_close_or_admission() -> anyhow::Result<()> {
            for fault in ["unknown", "malformed", "eof", "not_sent", "status"] {
                for admission in [false, true] {
                    let (bridge, parent) = bridge(5_000)?;
                    let mut initial = Fixture::ready();
                    initial.release_fault = Some((fault, if admission { 1 } else { 2 }));
                    let fixture = Arc::new(Mutex::new(initial));
                    let server = tokio::spawn(serve(parent, Arc::clone(&fixture), None));
                    let url = Url::parse(ROOT)?;
                    let opened = PublisherSession::open_bridged_async(&url, Arc::clone(&bridge)).await;
                    if admission {
                        assert!(
                            opened.is_err(),
                            "{fault}: failed final registration release admitted a session"
                        );
                    } else {
                        let session = opened?;
                        let member = session.publisher_id().ok_or_else(invalid)?.to_string();
                        assert!(
                            session.close_after_drain_async().await.is_err(),
                            "{fault}: failed final release closed cleanly"
                        );
                        assert_ne!(session.status().phase, crate::publisher::Phase::Closed);
                        assert!(!fixture.lock().await.metadata["publishers"].contains(&member));
                        let count = fixture.lock().await.requests.len();
                        assert!(session.close_after_drain_async().await.is_err());
                        assert_eq!(
                            fixture.lock().await.requests.len(),
                            count,
                            "{fault}: repeated close replayed control"
                        );
                    }
                    let count = fixture.lock().await.requests.len();
                    assert!(PublisherSession::open_bridged_async(&url, bridge).await.is_err());
                    let state = fixture.lock().await;
                    assert_eq!(
                        state.requests.len(),
                        count,
                        "{fault}: failed release permitted re-registration"
                    );
                    assert!(state.metadata["publishers"].contains(UNRELATED));
                    let members: BTreeMap<String, Value> = serde_json::from_str(&state.metadata["publishers"])?;
                    assert_eq!(members.len(), if admission { 2 } else { 1 });
                    assert_eq!(state.metadata["unrelated"], "preserve-me");
                    server.abort();
                }
            }
            Ok(())
        }

        #[tokio::test]
        async fn public_open_rejects_noncanonical_roots_before_any_control_frame() -> anyhow::Result<()> {
            for root in [
                "abfss://data@offlineaccount.attacker.invalid/owned-run/table/",
                "abfss://data@offlineaccount.dfs.core.windows.net:443/owned-run/table/",
                "abfss://data@offlineaccount.dfs.core.windows.net:8443/owned-run/table/",
                "abfss://data@offlineaccount.dfs.core.windows.net/owned%20run/table/",
                "abfss://data@offlineaccount.dfs.core.windows.net/owned%2Frun/table/",
                "abfss://data@offlineaccount.dfs.core.windows.net/owned-run//table/",
            ] {
                let mut authority = authority();
                authority.table_root_url = root.into();
                let (child, mut parent) = tokio::io::duplex(MAX_FRAME);
                let (read, write) = tokio::io::split(child);
                if let Ok(bridge) = StorageBridge::new(authority, deadline(5_000)?, read, write) {
                    assert!(
                        PublisherSession::open_bridged_async(&Url::parse(root)?, bridge)
                            .await
                            .is_err(),
                        "{root}: invalid root admitted"
                    );
                }
                assert_eq!(
                    tokio::time::timeout(Duration::from_millis(20), parent.read(&mut [0_u8; 1])).await??,
                    0,
                    "{root}: invalid root emitted a control frame"
                );
                let (canonical, mut parent) = bridge(5_000)?;
                assert!(
                    PublisherSession::open_bridged_async(&Url::parse(root)?, canonical)
                        .await
                        .is_err()
                );
                assert_eq!(parent.read(&mut [0_u8; 1]).await?, 0);
            }
            Ok(())
        }

        #[tokio::test]
        async fn canonical_root_sdk_list_and_object_paths_agree() -> anyhow::Result<()> {
            for root in [
                ROOT,
                "abfss://data@offlineaccount.dfs.core.windows.net/owned_run-2/table.v1/",
            ] {
                let mut authority = authority();
                authority.table_root_url = root.into();
                let (child, parent) = tokio::io::duplex(MAX_FRAME);
                let (read, write) = tokio::io::split(child);
                let bridge = StorageBridge::new(authority, deadline(5_000)?, read, write)?;
                let fixture = Arc::new(Mutex::new(Fixture::ready()));
                let server = tokio::spawn(serve(parent, Arc::clone(&fixture), None));
                let url = Url::parse(root)?;
                let session = PublisherSession::open_bridged_async(&url, bridge).await?;
                let store = session.store_for_url(&url)?;
                let prefix = object_store::path::Path::parse(url.path())?;
                assert!(store.list(Some(&prefix)).try_collect::<Vec<_>>().await?.is_empty());
                let path = prefix.join("_delta_log").join("00000000000000000000.json");
                store
                    .put_opts(
                        &path,
                        "{}".into(),
                        PutOptions {
                            mode: PutMode::Create,
                            ..Default::default()
                        },
                    )
                    .await?;
                assert_eq!(store.get(&path).await?.bytes().await?.as_ref(), b"{}");
                session.close_after_drain_async().await?;
                server.abort();
            }
            Ok(())
        }

        #[tokio::test]
        async fn cancelled_guarded_put_poisoning_retains_registration_and_blocks_forwarding() -> anyhow::Result<()> {
            let (bridge, parent) = bridge(30_000)?;
            let fixture = Arc::new(Mutex::new(Fixture::ready()));
            let hold = Arc::new(Notify::new());
            let server = tokio::spawn(serve(parent, Arc::clone(&fixture), Some(Arc::clone(&hold))));
            let url = Url::parse(ROOT)?;
            let session = PublisherSession::open_bridged_async(&url, bridge).await?;
            let publisher = session.publisher_id().ok_or_else(invalid)?.to_string();
            let store = session.store_for_url(&url)?;
            let mutation = tokio::spawn({
                let store = Arc::clone(&store);
                async move {
                    store
                        .put_opts(
                            &object_store::path::Path::from("owned-run/table/_delta_log/00000000000000000000.json"),
                            "{}".into(),
                            PutOptions {
                                mode: PutMode::Create,
                                ..Default::default()
                            },
                        )
                        .await
                }
            });
            tokio::time::timeout(Duration::from_secs(5), hold.notified()).await?;
            mutation.abort();
            assert!(mutation.await.is_err());
            let count = fixture.lock().await.requests.len();
            assert!(
                store
                    .get(&object_store::path::Path::from(
                        "owned-run/table/_delta_log/00000000000000000000.json"
                    ))
                    .await
                    .is_err()
            );
            assert!(session.status().uncertain.is_some());
            assert!(session.close_after_drain_async().await.is_err());
            let state = fixture.lock().await;
            assert_eq!(state.requests.len(), count);
            assert!(state.metadata["publishers"].contains(&publisher));
            assert!(state.metadata["publishers"].contains(UNRELATED));
            server.abort();
            Ok(())
        }

        #[tokio::test]
        async fn corrupt_unknown_and_lost_replies_never_allow_a_second_frame() -> anyhow::Result<()> {
            for fault in [
                "unknown",
                "mismatch",
                "body_digest",
                "truncated",
                "duplicate",
                "eof",
                "late",
            ] {
                let (bridge, parent) = bridge(if fault == "late" { 100 } else { 5_000 })?;
                let (read, mut write) = tokio::io::split(parent);
                let server = tokio::spawn(async move {
                    let mut reader = BufReader::new(read);
                    let request = read_request(&mut reader).await?;
                    let mut reply = response(&request, 200, Vec::new(), "");
                    match fault {
                        "unknown" => {
                            reply = json!({"schema_version":1,"kind":"storage_http_response","request_id":request["request_id"],"request_sha256":request["request_sha256"],"outcome":"unknown","error_code":"admission_refused"})
                        }
                        "mismatch" => reply["request_id"] = json!(999),
                        "body_digest" => reply["body_sha256"] = json!("invalid"),
                        "truncated" => reply["truncated"] = json!(true),
                        "late" => tokio::time::sleep(Duration::from_millis(150)).await,
                        "eof" => return Ok::<_, anyhow::Error>(()),
                        _ => {}
                    }
                    let mut bytes = serde_json::to_vec(&reply)?;
                    bytes.push(b'\n');
                    if fault == "duplicate" {
                        bytes.extend_from_within(..);
                    }
                    write.write_all(&bytes).await?;
                    write.flush().await?;
                    Ok(())
                });
                assert!(
                    matches!(bridge.exchange(head()?, "control").await, Outcome::Unknown(_)),
                    "{fault}"
                );
                assert!(
                    matches!(
                        bridge.exchange(head()?, "control").await,
                        Outcome::Unknown(Code::Poisoned)
                    ),
                    "{fault}"
                );
                server.abort();
            }
            Ok(())
        }

        #[tokio::test]
        async fn secrets_wrong_root_and_deadline_refuse_before_framing() -> anyhow::Result<()> {
            let (bridge, mut parent) = bridge(5_000)?;
            for secret in [true, false] {
                let mut request = head()?;
                if secret {
                    request.headers_mut().insert(
                        reqwest::header::AUTHORIZATION,
                        reqwest::header::HeaderValue::from_static("forbidden-offline-value"),
                    );
                } else {
                    *request.uri_mut() =
                        "https://offlineaccount.blob.core.windows.net/checkpoints?restype=container&sig=forbidden"
                            .parse()?;
                }
                assert!(matches!(
                    bridge.exchange(request, "control").await,
                    Outcome::NotSent(Code::InvalidRequest)
                ));
            }
            assert!(
                bridge
                    .validate_root(&Url::parse("abfss://data@offlineaccount.dfs.core.windows.net/other/")?)
                    .is_err()
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(20), parent.read_u8())
                    .await
                    .is_err()
            );
            let (mut expired, mut expired_parent) = self::bridge(5_000)?;
            Arc::get_mut(&mut expired).ok_or_else(invalid)?.deadline = Instant::now();
            assert!(matches!(
                expired.exchange(head()?, "control").await,
                Outcome::NotSent(Code::DeadlineExceeded)
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(20), expired_parent.read_u8())
                    .await
                    .is_err()
            );
            Ok(())
        }

        #[tokio::test]
        async fn positive_not_sent_is_distinct_from_unknown_and_does_not_reset_budget() -> anyhow::Result<()> {
            let (bridge, parent) = bridge(5_000)?;
            let original = bridge.original_deadline_unix_ms;
            let (read, mut write) = tokio::io::split(parent);
            let server = tokio::spawn(async move {
                let mut reader = BufReader::new(read);
                for id in 1..=2 {
                    let request = read_request(&mut reader).await?;
                    anyhow::ensure!(request["request_id"] == id);
                    anyhow::ensure!(request["original_deadline_unix_ms"] == original);
                    let reply = json!({
                        "schema_version":1,"kind":"storage_http_response",
                        "request_id":request["request_id"],"request_sha256":request["request_sha256"],
                        "outcome":"definitely_not_sent","error_code":"admission_refused"
                    });
                    write.write_all(&serde_json::to_vec(&reply)?).await?;
                    write.write_all(b"\n").await?;
                }
                Ok::<_, anyhow::Error>(())
            });
            for _ in 0..2 {
                assert!(matches!(
                    bridge.exchange(head()?, "control").await,
                    Outcome::NotSent(Code::AdmissionRefused)
                ));
            }
            server.await??;
            Ok(())
        }

        #[tokio::test]
        async fn stale_or_fenced_authority_never_enrolls_and_expired_drain_retains_member() -> anyhow::Result<()> {
            for (field, value) in [("resetstate", "fenced-v1"), ("resetgeneration", UNRELATED)] {
                let (bridge, parent) = bridge(5_000)?;
                let mut state = Fixture::ready();
                state.metadata.insert(field.into(), value.into());
                if field == "resetgeneration" {
                    state.metadata.insert("publishers".into(), "{}".into());
                }
                let fixture = Arc::new(Mutex::new(state));
                let server = tokio::spawn(serve(parent, Arc::clone(&fixture), None));
                assert!(
                    PublisherSession::open_bridged_async(&Url::parse(ROOT)?, bridge)
                        .await
                        .is_err()
                );
                assert!(fixture.lock().await.requests.iter().all(|r| r["method"] == "HEAD"));
                server.abort();
            }
            let (bridge, parent) = bridge(1_000)?;
            let end = bridge.deadline;
            let fixture = Arc::new(Mutex::new(Fixture::ready()));
            let server = tokio::spawn(serve(parent, Arc::clone(&fixture), None));
            let session = PublisherSession::open_bridged_async(&Url::parse(ROOT)?, bridge).await?;
            let member = session.publisher_id().ok_or_else(invalid)?.to_string();
            let count = fixture.lock().await.requests.len();
            tokio::time::sleep_until(end + Duration::from_millis(1)).await;
            assert!(session.close_after_drain_async().await.is_err());
            let state = fixture.lock().await;
            assert_eq!(state.requests.len(), count);
            assert!(state.metadata["publishers"].contains(&member));
            server.abort();
            Ok(())
        }
    }
}

/// Build the shared Azure transport, parsing QAP configuration once.
///
/// With neither environment variable set this is the ordinary SDK connector.
/// Configured tracing requires a private, regular, writable file. Do not reuse
/// the trace path for another run or remove it while a connector is alive.
///
/// # Errors
/// Rejects partial/non-Unicode configuration, invalid markers, non-absolute
/// paths, unsafe files, and failures to open/synchronize the trace.
pub fn azure_http_connector() -> object_store::Result<impl HttpConnector> {
    configured_connector()
}

pub(crate) fn bridge_http_connector(bridge: Arc<StorageBridge>) -> object_store::Result<impl HttpConnector> {
    let mut connector = configured_connector()?;
    connector.bridge = Some(bridge);
    Ok(connector)
}

fn configured_connector() -> object_store::Result<Connector> {
    let marker = std::env::var("ARROWCOL_QAP_CLIENT_MARKER");
    let path = std::env::var("ARROWCOL_QAP_HTTP_TRACE");
    let value = |result| match result {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(invalid("QAP HTTP configuration must be Unicode")),
    };
    let marker = value(marker).map_err(store_error)?;
    let path = value(path).map_err(store_error)?;
    Connector::configured(marker.as_deref(), path.as_deref().map(Path::new)).map_err(store_error)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn store_error(source: io::Error) -> object_store::Error {
    object_store::Error::Generic {
        store: "QAP HTTP attribution",
        source: Box::new(source),
    }
}

fn http_error(source: io::Error) -> HttpError {
    HttpError::new(HttpErrorKind::Unknown, source)
}

#[derive(Debug)]
struct Connector {
    trace: Option<Arc<Trace>>,
    bridge: Option<Arc<StorageBridge>>,
}

impl Connector {
    fn configured(marker: Option<&str>, path: Option<&Path>) -> io::Result<Self> {
        match (marker, path) {
            (None, None) => Ok(Self {
                trace: None,
                bridge: None,
            }),
            (Some(marker), Some(path)) => {
                let parts: Vec<_> = marker.split('/').collect();
                let safe_part = |part: &str| {
                    !part.is_empty()
                        && part.len() <= 128
                        && part.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
                };
                if parts.len() != 3 || parts[0] != "arrowcol-qap" || !safe_part(parts[1]) || !safe_part(parts[2]) {
                    return Err(invalid("QAP marker must be arrowcol-qap/<ASCII run-id>/<ASCII trial>"));
                }
                if !path.is_absolute() {
                    return Err(invalid("QAP trace path must be absolute"));
                }
                let marker = HeaderValue::from_str(marker).map_err(|_| invalid("invalid QAP marker"))?;
                let mut options = OpenOptions::new();
                options.append(true).create_new(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                let file = match options.open(path) {
                    Ok(file) => file,
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        let metadata = std::fs::symlink_metadata(path)?;
                        if !metadata.file_type().is_file() {
                            return Err(invalid("QAP trace must be a regular file, not a symlink"));
                        }
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt;
                            if metadata.permissions().mode() & 0o077 != 0 {
                                return Err(invalid("QAP trace permissions must be private"));
                            }
                        }
                        OpenOptions::new().append(true).open(path)?
                    }
                    Err(error) => return Err(error),
                };
                if !file.metadata()?.is_file() {
                    return Err(invalid("QAP trace must be a regular file"));
                }
                file.sync_all()?;
                Ok(Self {
                    bridge: None,
                    trace: Some(Arc::new(Trace {
                        marker,
                        path: path.to_owned(),
                        state: Mutex::new(TraceState { file, failed: false }),
                    })),
                })
            }
            _ => Err(invalid("QAP marker and trace path must both be configured")),
        }
    }
}

impl HttpConnector for Connector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        if let Some(bridge) = &self.bridge {
            let inner = HttpClient::new(storage_bridge::Service(Arc::clone(bridge)));
            return Ok(match &self.trace {
                Some(trace) => HttpClient::new(Service {
                    inner,
                    trace: Arc::clone(trace),
                }),
                None => inner,
            });
        }
        match &self.trace {
            None => ReqwestConnector::default().connect(options),
            Some(trace) => Ok(HttpClient::new(Service {
                inner: no_redirect_client(options, &trace.marker)?,
                trace: Arc::clone(trace),
            })),
        }
    }
}

fn no_redirect_client(options: &ClientOptions, marker: &HeaderValue) -> object_store::Result<HttpClient> {
    use ClientConfigKey::*;
    // object_store 0.13.2 exposes no redirect option and its ReqwestConnector
    // uses reqwest's default follow policy. Use reqwest's supported HttpService
    // implementation instead, with bounded canonical-Azure client options.
    // Never silently discard a requested transport policy.
    let defaults = ClientOptions::new();
    for key in [
        AllowInvalidCertificates,
        Http1Only,
        Http2Only,
        Http2KeepAliveInterval,
        Http2KeepAliveTimeout,
        Http2KeepAliveWhileIdle,
        Http2MaxFrameSize,
        PoolIdleTimeout,
        PoolMaxIdlePerHost,
        ProxyUrl,
        ProxyCaCertificate,
        ProxyExcludes,
    ] {
        if options.get_config_value(&key) != defaults.get_config_value(&key) {
            return Err(store_error(invalid("unsupported traced QAP HTTP transport option")));
        }
    }
    let allow_http = options
        .get_config_value(&AllowHttp)
        .ok_or_else(|| store_error(invalid("missing QAP allow_http option")))?
        .parse::<bool>()
        .map_err(|_| store_error(invalid("invalid QAP allow_http option")))?;
    let mut builder = reqwest::ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none())
        .referer(false)
        .user_agent(marker.clone())
        .https_only(!allow_http)
        .http1_only()
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate();
    if let Some(headers) = options.get_default_headers() {
        builder = builder.default_headers(headers.clone());
    }
    if let Some(value) = options.get_config_value(&Timeout) {
        builder = builder.timeout(client_duration(&value).map_err(store_error)?);
    }
    if let Some(value) = options.get_config_value(&ConnectTimeout) {
        builder = builder.connect_timeout(client_duration(&value).map_err(store_error)?);
    }
    let client = builder.build().map_err(|error| object_store::Error::Generic {
        store: "QAP HTTP attribution",
        source: Box::new(error.without_url()),
    })?;
    Ok(HttpClient::new(client))
}

fn client_duration(value: &str) -> io::Result<std::time::Duration> {
    let mut total = std::time::Duration::ZERO;
    // Public ClientOptions getters format parsed durations as humantime tokens.
    // Accept those canonical integer units without introducing another crate.
    for token in value.split_whitespace() {
        let end = token
            .find(|c: char| !c.is_ascii_digit())
            .ok_or_else(|| invalid("invalid traced QAP timeout"))?;
        let number = token[..end]
            .parse::<u64>()
            .map_err(|_| invalid("invalid traced QAP timeout"))?;
        let nanos = match &token[end..] {
            "ns" => 1,
            "us" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            "days" => 86_400_000_000_000,
            _ => return Err(invalid("unsupported traced QAP timeout unit")),
        };
        let duration = std::time::Duration::from_nanos(
            number
                .checked_mul(nanos)
                .ok_or_else(|| invalid("traced QAP timeout overflow"))?,
        );
        total = total
            .checked_add(duration)
            .ok_or_else(|| invalid("traced QAP timeout overflow"))?;
    }
    if value.is_empty() {
        return Err(invalid("invalid traced QAP timeout"));
    }
    Ok(total)
}

#[derive(Debug)]
struct Trace {
    marker: HeaderValue,
    path: PathBuf,
    state: Mutex<TraceState>,
}

#[derive(Debug)]
struct TraceState {
    file: File,
    failed: bool,
}

impl Trace {
    fn record(&self, receipt: &Receipt<'_>) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("QAP trace lock poisoned"))?;
        if state.failed {
            return Err(io::Error::other(
                "QAP trace previously failed; refusing unattributed I/O",
            ));
        }
        let result = (|| {
            let named = std::fs::symlink_metadata(&self.path)?;
            if !named.file_type().is_file() {
                return Err(invalid("QAP trace is no longer a regular file"));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::{MetadataExt, PermissionsExt};
                let opened = state.file.metadata()?;
                if named.dev() != opened.dev() || named.ino() != opened.ino() {
                    return Err(invalid("QAP trace was replaced"));
                }
                if named.permissions().mode() & 0o077 != 0 {
                    return Err(invalid("QAP trace permissions are no longer private"));
                }
            }
            let mut line = serde_json::to_vec(receipt)?;
            line.push(b'\n');
            // The mutex covers cloned clients; the file lock covers distinct
            // connectors/processes appending to the same run-owned trace.
            state.file.lock_exclusive()?;
            let written = state.file.write_all(&line).and_then(|()| state.file.sync_all());
            let unlocked = FileExt::unlock(&state.file);
            written.and(unlocked)
        })();
        if result.is_err() {
            state.failed = true;
        }
        result
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Receipt<'a> {
    event: &'a str,
    time: String,
    marker: &'a str,
    client_request_id: &'a str,
    method: &'a str,
    url: &'a str,
    status: Option<u16>,
    server_request_id: Option<&'a str>,
}

#[derive(Debug)]
struct Service {
    inner: HttpClient,
    trace: Arc<Trace>,
}

#[async_trait]
impl HttpService for Service {
    async fn call(&self, mut request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let mut url =
            Url::parse(&request.uri().to_string()).map_err(|_| http_error(invalid("QAP request URL is invalid")))?;
        url.set_query(None);
        url.set_fragment(None);
        url.set_username("")
            .map_err(|()| http_error(invalid("QAP request URL cannot be sanitized")))?;
        url.set_password(None)
            .map_err(|()| http_error(invalid("QAP request URL cannot be sanitized")))?;
        let client_request_id = Uuid::new_v4().to_string();
        request.headers_mut().insert(USER_AGENT, self.trace.marker.clone());
        request.headers_mut().insert(
            "x-ms-client-request-id",
            HeaderValue::from_str(&client_request_id).map_err(|_| http_error(invalid("invalid request ID")))?,
        );
        let method = request.method().clone();
        let mut receipt = Receipt {
            event: "request",
            time: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            marker: self
                .trace
                .marker
                .to_str()
                .map_err(|_| http_error(invalid("invalid QAP marker")))?,
            client_request_id: &client_request_id,
            method: method.as_str(),
            url: url.as_str(),
            status: None,
            server_request_id: None,
        };
        self.trace.record(&receipt).map_err(http_error)?;
        let result = self.inner.execute(request).await;
        receipt.event = "response";
        receipt.time = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        if let Ok(response) = &result {
            receipt.status = Some(response.status().as_u16());
            receipt.server_request_id = response
                .headers()
                .get("x-ms-request-id")
                .and_then(|id| id.to_str().ok());
        }
        self.trace.record(&receipt).map_err(http_error)?;
        if result.as_ref().is_ok_and(|response| response.status().is_redirection()) {
            return Err(http_error(invalid(
                "QAP HTTP redirects are disabled; use a canonical Azure endpoint",
            )));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Context, Result, ensure};
    use object_store::client::HttpRequestBody;
    use serde_json::Value;
    use std::io::Read;
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    const MARKER: &str = "arrowcol-qap/run-012345/trial-1";

    fn directory() -> Result<tempfile::TempDir> {
        let parent = std::env::current_dir()?.join("target/qap-http-tests");
        std::fs::create_dir_all(&parent)?;
        Ok(tempfile::tempdir_in(parent)?)
    }

    fn request(url: &str) -> Result<HttpRequest> {
        let mut request = HttpRequest::new(HttpRequestBody::empty());
        *request.uri_mut() = url.parse()?;
        request
            .headers_mut()
            .insert(USER_AGENT, HeaderValue::from_static("wrong-sdk-agent"));
        request
            .headers_mut()
            .insert("x-ms-client-request-id", HeaderValue::from_static("reused-id"));
        request
            .headers_mut()
            .insert("authorization", HeaderValue::from_static("Bearer private-test-token"));
        Ok(request)
    }

    type Server = std::thread::JoinHandle<Result<Vec<String>>>;

    fn server(count: usize, hook: impl Fn(usize) + Send + 'static) -> Result<(String, Server)> {
        server_responses(vec![200; count], hook)
    }

    fn server_responses(statuses: Vec<u16>, hook: impl Fn(usize) + Send + 'static) -> Result<(String, Server)> {
        server_with_headers(statuses, hook, String::new())
    }

    fn server_with_headers(
        statuses: Vec<u16>,
        hook: impl Fn(usize) + Send + 'static,
        headers: String,
    ) -> Result<(String, Server)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let url = format!("http://{}/object?sig=private-query-token", listener.local_addr()?);
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for (index, status) in statuses.into_iter().enumerate() {
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            ensure!(Instant::now() < deadline, "mock request timed out");
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => return Err(error.into()),
                    }
                };
                stream.set_read_timeout(Some(Duration::from_secs(5)))?;
                let mut bytes = Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    stream.read_exact(&mut byte)?;
                    bytes.push(byte[0]);
                    ensure!(bytes.len() < 16_384, "oversized mock request");
                }
                requests.push(String::from_utf8(bytes)?);
                hook(index);
                stream.write_all(
                    format!(
                        "HTTP/1.1 {status} test\r\nConnection: close\r\nContent-Length: 0\r\n\
                     x-ms-request-id: server-id\r\nETag: \"object-v1\"\r\n\
                     Last-Modified: Sat, 07 Nov 2026 11:00:00 GMT\r\n{headers}\r\n"
                    )
                    .as_bytes(),
                )?;
            }
            Ok(requests)
        });
        Ok((url, server))
    }

    #[tokio::test]
    async fn azure_sdk_retries_receive_distinct_markers_and_receipt_pairs() -> Result<()> {
        use object_store::ObjectStoreExt;
        let directory = directory()?;
        let path = directory.path().join("trace.jsonl");
        let connector = Connector::configured(Some(MARKER), Some(&path))?;
        let (url, server) = server_responses(vec![503, 200], |_| {})?;
        let endpoint = Url::parse(&url)?.origin().ascii_serialization();
        let store = object_store::azure::MicrosoftAzureBuilder::new()
            .with_account("offlineaccount")
            .with_container_name("fixture")
            .with_bearer_token_authorization("private-test-token")
            .with_endpoint(endpoint)
            .with_client_options(ClientOptions::new().with_allow_http(true))
            .with_http_connector(connector)
            .with_retry(object_store::RetryConfig {
                max_retries: 1,
                retry_timeout: Duration::from_secs(2),
                ..Default::default()
            })
            .build()?;
        let metadata = store.head(&object_store::path::Path::from("object")).await?;
        assert_eq!(metadata.e_tag.as_deref(), Some("\"object-v1\""));
        let requests = server.join().map_err(|_| anyhow::anyhow!("mock server panicked"))??;
        assert_eq!(requests.len(), 2);
        let lines = receipts(&path)?;
        assert_eq!(lines.len(), 4);
        for index in [0, 2] {
            assert_eq!(lines[index]["event"], "request");
            assert_eq!(lines[index + 1]["event"], "response");
            assert_eq!(lines[index]["clientRequestId"], lines[index + 1]["clientRequestId"]);
            assert_eq!(lines[index]["method"], "HEAD");
            assert!(requests[index / 2].contains(&format!("user-agent: {MARKER}\r\n")));
            assert!(requests[index / 2].contains(&format!(
                "x-ms-client-request-id: {}\r\n",
                lines[index]["clientRequestId"].as_str().context("request ID")?
            )));
        }
        assert_ne!(lines[0]["clientRequestId"], lines[2]["clientRequestId"]);
        assert_eq!(lines[1]["status"], 503);
        assert_eq!(lines[3]["status"], 200);
        Ok(())
    }

    fn receipts(path: &Path) -> Result<Vec<Value>> {
        std::fs::read_to_string(path)?
            .lines()
            .map(|line| Ok(serde_json::from_str(line)?))
            .collect()
    }

    #[tokio::test]
    async fn traced_redirects_fail_with_receipts_and_never_contact_second_hop() -> Result<()> {
        for status in [301, 302, 303, 307, 308] {
            let directory = directory()?;
            let path = directory.path().join("trace.jsonl");
            let connector = Connector::configured(Some(MARKER), Some(&path))?;
            let destination = TcpListener::bind("127.0.0.1:0")?;
            destination.set_nonblocking(true)?;
            let target = format!("http://{}/unrecorded?sig=redirect-secret", destination.local_addr()?);
            let (url, server) = server_with_headers(vec![status], |_| {}, format!("Location: {target}\r\n"))?;
            let client = connector.connect(&ClientOptions::new().with_allow_http(true))?;
            let error = client
                .execute(request(&url)?)
                .await
                .err()
                .context("redirect must fail")?;
            assert_eq!(error.kind(), HttpErrorKind::Unknown);
            assert!(error.to_string().contains("redirects are disabled"));
            let requests = server.join().map_err(|_| anyhow::anyhow!("mock server panicked"))??;
            assert_eq!(requests.len(), 1);
            assert!(requests[0].contains(&format!("user-agent: {MARKER}\r\n")));
            assert_eq!(
                destination.accept().err().context("redirect hop was contacted")?.kind(),
                io::ErrorKind::WouldBlock
            );
            let lines = receipts(&path)?;
            assert_eq!(lines.len(), 2);
            assert_eq!(lines[0]["event"], "request");
            assert_eq!(lines[1]["event"], "response");
            assert_eq!(lines[0]["clientRequestId"], lines[1]["clientRequestId"]);
            assert_eq!(lines[1]["status"], status);
            assert_eq!(lines[1]["serverRequestId"], "server-id");
            assert!(!std::fs::read_to_string(path)?.contains("redirect-secret"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn ordinary_reqwest_connector_still_follows_default_302_policy() -> Result<()> {
        let (target, second) = server(1, |_| {})?;
        let (url, first) = server_with_headers(vec![302], |_| {}, format!("Location: {target}\r\n"))?;
        let client = Connector::configured(None, None)?.connect(&ClientOptions::new().with_allow_http(true))?;
        assert_eq!(client.execute(request(&url)?).await?.status().as_u16(), 200);
        for server in [first, second] {
            assert_eq!(
                server
                    .join()
                    .map_err(|_| anyhow::anyhow!("mock server panicked"))??
                    .len(),
                1
            );
        }
        Ok(())
    }

    #[test]
    fn traced_clients_preserve_bounded_timeouts_and_reject_unsupported_policy() -> Result<()> {
        for (seconds, formatted) in [(5, "5s"), (10, "10s"), (30, "30s"), (60, "1m")] {
            assert_eq!(client_duration(formatted)?, Duration::from_secs(seconds));
        }
        assert_eq!(
            client_duration("1m 2s 3ms 4us 5ns")?,
            Duration::from_nanos(62_003_004_005)
        );
        assert!(client_duration("invalid").is_err());
        let directory = directory()?;
        let path = directory.path().join("trace.jsonl");
        let connector = Connector::configured(Some(MARKER), Some(&path))?;
        assert!(
            connector
                .connect(&ClientOptions::new().with_proxy_url("http://127.0.0.1:1"))
                .is_err()
        );
        assert!(
            connector
                .connect(&ClientOptions::new().with_allow_invalid_certificates(true))
                .is_err()
        );
        assert!(
            connector
                .connect(
                    &ClientOptions::new()
                        .with_timeout(Duration::from_secs(60))
                        .with_connect_timeout(Duration::from_secs(10))
                )
                .is_ok()
        );
        Ok(())
    }

    #[tokio::test]
    async fn disabled_connector_preserves_unmodified_requests() -> Result<()> {
        let connector = Connector::configured(None, None)?;
        let client = connector.connect(&ClientOptions::new().with_allow_http(true))?;
        let (url, server) = server(1, |_| {})?;
        assert_eq!(client.execute(request(&url)?).await?.status().as_u16(), 200);
        let requests = server.join().map_err(|_| anyhow::anyhow!("mock server panicked"))??;
        assert!(requests[0].contains("user-agent: wrong-sdk-agent\r\n"));
        assert!(requests[0].contains("x-ms-client-request-id: reused-id\r\n"));
        assert!(requests[0].contains("?sig=private-query-token "));
        Ok(())
    }

    #[tokio::test]
    async fn configured_requests_are_marked_correlated_sanitized_and_atomic() -> Result<()> {
        let directory = directory()?;
        let path = directory.path().join("trace.jsonl");
        let connector = Connector::configured(Some(MARKER), Some(&path))?;
        let client = connector.connect(&ClientOptions::new().with_allow_http(true))?;
        let second =
            Connector::configured(Some(MARKER), Some(&path))?.connect(&ClientOptions::new().with_allow_http(true))?;
        let (url, server) = server(4, |_| {})?;
        let requests = [request(&url)?, request(&url)?, request(&url)?, request(&url)?];
        let results = futures::future::join_all(
            requests
                .into_iter()
                .enumerate()
                .map(|(index, request)| if index % 2 == 0 { &client } else { &second }.execute(request)),
        )
        .await;
        for result in results {
            assert_eq!(result?.status().as_u16(), 200);
        }
        let requests = server.join().map_err(|_| anyhow::anyhow!("mock server panicked"))??;
        let lines = receipts(&path)?;
        assert_eq!(lines.len(), 8);
        let mut ids = std::collections::HashSet::new();
        for receipt in lines.iter().filter(|r| r["event"] == "request") {
            let id = receipt["clientRequestId"].as_str().context("request ID")?;
            assert!(ids.insert(id));
            Uuid::parse_str(id)?;
            assert_eq!(receipt["marker"], MARKER);
            assert_eq!(receipt["url"], url.split('?').next().context("URL")?);
            assert_eq!(receipt["method"], "GET");
            chrono::DateTime::parse_from_rfc3339(receipt["time"].as_str().context("timestamp")?)?;
            let response: Vec<_> = lines
                .iter()
                .filter(|r| r["event"] == "response" && r["clientRequestId"] == id)
                .collect();
            assert_eq!(response.len(), 1);
            assert_eq!(response[0]["status"], 200);
            assert_eq!(response[0]["serverRequestId"], "server-id");
            assert!(
                requests
                    .iter()
                    .any(|r| r.contains(&format!("x-ms-client-request-id: {id}\r\n")))
            );
        }
        for wire in requests {
            assert!(wire.contains(&format!("user-agent: {MARKER}\r\n")));
            assert!(!wire.contains("wrong-sdk-agent") && !wire.contains("reused-id"));
        }
        let raw = std::fs::read_to_string(&path)?;
        assert!(!raw.contains("private-query-token") && !raw.contains("private-test-token"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&path)?.permissions().mode() & 0o777, 0o600);
        }
        Ok(())
    }

    #[test]
    fn rejects_partial_invalid_and_unwritable_configuration_before_io() -> Result<()> {
        let directory = directory()?;
        let path = directory.path().join("trace.jsonl");
        assert!(Connector::configured(None, None)?.trace.is_none());
        assert!(Connector::configured(Some(MARKER), None).is_err());
        assert!(Connector::configured(None, Some(&path)).is_err());
        for marker in [
            "",
            "sdk",
            "arrowcol-qap//trial",
            "arrowcol-qap/run/",
            "arrowcol-qap/rün/1",
            "arrowcol-qap/run/1\r\nAuthorization: token",
            "arrowcol-qap/run/1 extra",
            "arrowcol-qap/run/1/extra",
        ] {
            assert!(Connector::configured(Some(marker), Some(&path)).is_err());
        }
        assert!(Connector::configured(Some(MARKER), Some(Path::new("relative.jsonl"))).is_err());
        assert!(Connector::configured(Some(MARKER), Some(directory.path())).is_err());
        assert!(Connector::configured(Some(MARKER), Some(&directory.path().join("absent/trace"))).is_err());
        assert!(!path.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::{PermissionsExt, symlink};
            std::fs::write(&path, "")?;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;
            assert!(Connector::configured(Some(MARKER), Some(&path)).is_err());
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            let link = directory.path().join("symlink.jsonl");
            symlink(&path, &link)?;
            assert!(Connector::configured(Some(MARKER), Some(&link)).is_err());
        }
        Ok(())
    }

    fn disable_writes(trace: &Trace, path: &Path) -> Result<()> {
        trace.state.lock().map_err(|_| anyhow::anyhow!("trace lock"))?.file = File::open(path)?;
        Ok(())
    }

    #[tokio::test]
    async fn request_persistence_failure_prevents_network_io() -> Result<()> {
        let directory = directory()?;
        let path = directory.path().join("trace.jsonl");
        let connector = Connector::configured(Some(MARKER), Some(&path))?;
        let trace = connector.trace.as_ref().context("trace")?;
        disable_writes(trace, &path)?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let url = format!("http://{}/object", listener.local_addr()?);
        let client = connector.connect(&ClientOptions::new().with_allow_http(true))?;
        for _ in 0..2 {
            assert_eq!(
                client
                    .execute(request(&url)?)
                    .await
                    .err()
                    .context("expected failure")?
                    .kind(),
                HttpErrorKind::Unknown
            );
            assert_eq!(
                listener.accept().err().context("unexpected network I/O")?.kind(),
                io::ErrorKind::WouldBlock
            );
        }
        assert!(receipts(&path)?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn removed_or_replaced_trace_prevents_network_io() -> Result<()> {
        for replacement in [false, true] {
            let directory = directory()?;
            let path = directory.path().join("trace.jsonl");
            let connector = Connector::configured(Some(MARKER), Some(&path))?;
            std::fs::remove_file(&path)?;
            if replacement {
                std::fs::write(&path, "")?;
            }
            let listener = TcpListener::bind("127.0.0.1:0")?;
            listener.set_nonblocking(true)?;
            let url = format!("http://{}/object", listener.local_addr()?);
            let client = connector.connect(&ClientOptions::new().with_allow_http(true))?;
            assert!(client.execute(request(&url)?).await.is_err());
            assert_eq!(
                listener.accept().err().context("unexpected network I/O")?.kind(),
                io::ErrorKind::WouldBlock
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn response_persistence_failure_fails_response_and_future_reads() -> Result<()> {
        let directory = directory()?;
        let path = directory.path().join("trace.jsonl");
        let connector = Connector::configured(Some(MARKER), Some(&path))?;
        let trace = Arc::clone(connector.trace.as_ref().context("trace")?);
        let trace_path = path.clone();
        let (url, server) = server(1, move |_| {
            if let Err(error) = disable_writes(&trace, &trace_path) {
                panic!("fault injection failed: {error}");
            }
        })?;
        let client = connector.connect(&ClientOptions::new().with_allow_http(true))?;
        assert!(client.execute(request(&url)?).await.is_err());
        assert!(client.execute(request(&url)?).await.is_err());
        assert_eq!(
            server
                .join()
                .map_err(|_| anyhow::anyhow!("mock server panicked"))??
                .len(),
            1
        );
        let lines = receipts(&path)?;
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["event"], "request");
        Ok(())
    }

    #[tokio::test]
    async fn transport_failure_records_correlated_null_status_without_error_details() -> Result<()> {
        let directory = directory()?;
        let path = directory.path().join("trace.jsonl");
        let connector = Connector::configured(Some(MARKER), Some(&path))?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let url = format!("http://{}/object?sig=private-query-token", listener.local_addr()?);
        drop(listener);
        let client = connector.connect(&ClientOptions::new().with_allow_http(true))?;
        assert!(client.execute(request(&url)?).await.is_err());
        let lines = receipts(&path)?;
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1]["event"], "response");
        assert_eq!(lines[0]["clientRequestId"], lines[1]["clientRequestId"]);
        assert!(lines[1]["status"].is_null());
        assert!(lines[1]["serverRequestId"].is_null());
        assert!(!std::fs::read_to_string(path)?.contains("private-query-token"));
        Ok(())
    }
}
