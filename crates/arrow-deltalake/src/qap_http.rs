//! Opt-in attribution of application-owned Azure HTTP traffic.
//!
//! Set both `ARROWCOL_QAP_CLIENT_MARKER=arrowcol-qap/<run-id>/<trial>` and
//! `ARROWCOL_QAP_HTTP_TRACE=<absolute private JSONL path>`. Each transport
//! attempt durably records a request before network I/O and a response before
//! returning its result. A cancelled attempt may leave an unmatched request;
//! consumers must treat unmatched requests as incomplete attribution.
//! Traced clients disable redirects: canonical Azure endpoints are required.

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
}

impl Connector {
    fn configured(marker: Option<&str>, path: Option<&Path>) -> io::Result<Self> {
        match (marker, path) {
            (None, None) => Ok(Self { trace: None }),
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
