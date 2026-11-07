use std::{
    collections::BTreeSet,
    path::Path,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use arrow_flight::{
    FlightData, FlightDescriptor,
    flight_service_client::FlightServiceClient,
    sql::{CommandStatementUpdate, ProstMessageExt},
};
use arrowcol_flight::{DdlCompletionMetadata, DeliveryGuarantee, PutResultMetadata, decode_ddl_completion_metadata};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, ImplementationPool, PrincipalId, PrincipalIssuer, PrincipalScope,
    TableIdentity, UnixMillis,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::stream;
use hmac::{Hmac, Mac};
use http::{HeaderMap, HeaderValue, Method};
use prost::Message;
use reqwest::{Client as HttpClient, Url};
use serde::Serialize;
use sha2::Sha256;
use tonic::{
    Request,
    metadata::MetadataValue,
    transport::{Channel, Endpoint},
};
use uuid::Uuid;

use crate::sdk::GeneratedTable;

const DO_PUT_PATH: &str = "/arrow.flight.protocol.FlightService/DoPut";
const EDGE_TIMESTAMP_HEADER: &str = "x-arrowcol-edge-timestamp";
const EDGE_NONCE_HEADER: &str = "x-arrowcol-edge-nonce";
const EDGE_SIGNATURE_HEADER: &str = "x-arrowcol-edge-signature";
const INTERNAL_AUTH_CONTEXT_HEADER: &str = "x-arrowcol-internal-auth-context";
const INTERNAL_AUTH_EXPIRES_HEADER: &str = "x-arrowcol-internal-auth-expires";
const INTERNAL_AUTH_SIGNATURE_HEADER: &str = "x-arrowcol-internal-auth-signature";

type HmacSha256 = Hmac<Sha256>;

/// Successful pool-specific Flight acknowledgement.
#[derive(Clone, Debug)]
pub enum FlightAcknowledgement {
    BestEffortAdmission,
    DurableAdmission,
}

/// Retained collector capability response.
#[derive(Clone, Debug, Serialize)]
pub struct CapabilityEvidence {
    pub endpoint: String,
    pub response: serde_json::Value,
}

/// Shared Flight/HTTP clients and trusted-edge request signer.
#[derive(Clone)]
pub struct FlightTransport {
    channel: Channel,
    catalog_channel: Channel,
    http: HttpClient,
    signer: EdgeSigner,
    internal_signer: InternalSigner,
}

impl FlightTransport {
    /// Connects to the router and catalog and loads the authentication keys.
    ///
    /// # Errors
    ///
    /// Returns invalid endpoint, key, or connection failures.
    pub async fn connect(
        flight_endpoint: &str,
        catalog_endpoint: &str,
        edge_secret_file: &Path,
        internal_secret_file: &Path,
        timeout: Duration,
    ) -> Result<Self> {
        let endpoint = Endpoint::from_shared(flight_endpoint.to_owned())?
            .connect_timeout(timeout)
            .timeout(timeout)
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_while_idle(true);
        let channel = endpoint.connect().await?;
        let catalog_channel = Endpoint::from_shared(catalog_endpoint.to_owned())?
            .connect_timeout(timeout)
            .timeout(timeout)
            .connect()
            .await?;
        let http = HttpClient::builder()
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()?;
        Ok(Self {
            channel,
            catalog_channel,
            http,
            signer: EdgeSigner::from_file(edge_secret_file)?,
            internal_signer: InternalSigner::from_file(internal_secret_file)?,
        })
    }

    /// Registers one strict `CREATE TABLE` through Flight SQL `DoPut`.
    ///
    /// # Errors
    ///
    /// Returns signing, transport, timeout, server, or completion errors.
    pub async fn register_schema(
        &self,
        ddl: &str,
        _identity: &TableIdentity,
        timeout: Duration,
    ) -> Result<DdlCompletionMetadata> {
        let command = CommandStatementUpdate {
            query: ddl.to_owned(),
            transaction_id: None,
        };
        let descriptor = FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Cmd as i32,
            cmd: command.as_any().encode_to_vec().into(),
            path: Vec::new(),
        };
        let outbound = stream::iter(vec![FlightData {
            flight_descriptor: Some(descriptor),
            ..FlightData::default()
        }]);
        let mut request = Request::new(outbound);
        self.internal_signer.decorate_catalog_request(&mut request)?;
        request.set_timeout(timeout);
        let mut client = FlightServiceClient::new(self.catalog_channel.clone());
        let response = tokio::time::timeout(timeout, client.do_put(request))
            .await
            .context("Flight SQL registration timed out")??;
        let completion = decode_ddl_completion_metadata(response.metadata())?;
        let mut stream = response.into_inner();
        let mut result_count = 0_usize;
        while stream.message().await?.is_some() {
            result_count += 1;
        }
        if result_count != 1 {
            bail!("Flight SQL registration returned {result_count} results instead of one");
        }
        Ok(completion)
    }

    /// Sends one pre-encoded generated batch through the selected pool.
    ///
    /// # Errors
    ///
    /// Returns signing, timeout, Flight, or acknowledgement-contract failures.
    pub async fn send_batch(
        &self,
        table: &GeneratedTable,
        pool: ImplementationPool,
        encoded: Arc<Vec<FlightData>>,
        timeout: Duration,
    ) -> Result<FlightAcknowledgement> {
        let outbound = stream::iter(encoded.as_ref().clone());
        let mut request = Request::new(outbound);
        let pairs = [
            ("x-arrowcol-protocol-version", "1".to_owned()),
            ("x-arrowcol-implementation-pool", pool.to_string()),
            ("x-arrowcol-catalog", table.identity.catalog().to_string()),
            ("x-arrowcol-schema", table.identity.schema().to_string()),
            ("x-arrowcol-table", table.identity.table().to_string()),
            ("x-arrowcol-schema-fingerprint", table.fingerprint.clone()),
        ];
        self.decorate_request(&mut request, DO_PUT_PATH, &pairs)?;
        request.set_timeout(timeout);
        let mut client = FlightServiceClient::new(self.channel.clone());
        let response = tokio::time::timeout(timeout, client.do_put(request))
            .await
            .context("Arrow Flight DoPut timed out before response")??;
        let mut response = response.into_inner();
        let mut acknowledgement = None;
        loop {
            let result = tokio::time::timeout(timeout, response.message())
                .await
                .context("Arrow Flight DoPut response stream timed out")??;
            let Some(result) = result else {
                break;
            };
            match PutResultMetadata::from_put_result(&result)? {
                PutResultMetadata::Progress { .. } => {}
                PutResultMetadata::Receipt { .. } => {
                    bail!("router returned the retired durable-receipt response")
                }
                PutResultMetadata::BestEffortAdmission { delivery_guarantee, .. } => {
                    if pool != ImplementationPool::Dataflow
                        || delivery_guarantee != DeliveryGuarantee::BestEffort
                        || acknowledgement
                            .replace(FlightAcknowledgement::BestEffortAdmission)
                            .is_some()
                    {
                        bail!("dataflow Flight acknowledgement contract was inconsistent");
                    }
                }
                PutResultMetadata::DurableAdmission { delivery_guarantee, .. } => {
                    if pool != ImplementationPool::Dataflow
                        || delivery_guarantee != DeliveryGuarantee::DurableAdmission
                        || acknowledgement
                            .replace(FlightAcknowledgement::DurableAdmission)
                            .is_some()
                    {
                        bail!("dataflow durable admission contract was inconsistent");
                    }
                }
            }
        }
        acknowledgement.context("Flight DoPut completed without an acknowledgement")
    }

    /// Fetches public collector capability endpoints without adding auth claims.
    ///
    /// # Errors
    ///
    /// Returns invalid URL, HTTP, status-code, or JSON failures.
    pub async fn capability_evidence(&self, endpoints: &[String]) -> Result<Vec<CapabilityEvidence>> {
        let mut evidence = Vec::new();
        for endpoint in endpoints {
            let url = Url::parse(endpoint)?.join("/v1/capabilities")?;
            let response = self.http.get(url).send().await?;
            let status = response.status();
            let bytes = response.bytes().await?;
            if !status.is_success() {
                bail!(
                    "capability endpoint {endpoint} failed with {status}: {}",
                    String::from_utf8_lossy(&bytes)
                );
            }
            evidence.push(CapabilityEvidence {
                endpoint: endpoint.clone(),
                response: serde_json::from_slice(&bytes)?,
            });
        }
        Ok(evidence)
    }

    fn decorate_request<T>(
        &self,
        request: &mut Request<T>,
        path: &str,
        pairs: &[(&'static str, String)],
    ) -> Result<()> {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/grpc"));
        for (key, value) in pairs {
            headers.insert(*key, HeaderValue::from_str(value)?);
        }
        self.signer.insert_signature(&mut headers, &Method::POST, path)?;
        for (key, value) in pairs {
            request.metadata_mut().insert(*key, MetadataValue::from_str(value)?);
        }
        for key in [EDGE_TIMESTAMP_HEADER, EDGE_NONCE_HEADER, EDGE_SIGNATURE_HEADER] {
            let value = headers
                .get(key)
                .and_then(|value| value.to_str().ok())
                .context("edge signer omitted a required header")?;
            request.metadata_mut().insert(key, MetadataValue::from_str(value)?);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct EdgeSigner {
    secret: Arc<[u8]>,
    nonce: Arc<AtomicU64>,
}

#[derive(Clone)]
struct InternalSigner {
    secret: Arc<[u8]>,
}

impl InternalSigner {
    fn from_file(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).with_context(|| format!("read internal HMAC secret {}", path.display()))?;
        let bytes = trim_line_end(bytes);
        if bytes.len() < 32 || bytes.contains(&b'\n') || bytes.contains(&b'\r') {
            bail!("internal HMAC secret must be one line with at least 32 bytes");
        }
        Ok(Self { secret: bytes.into() })
    }

    fn decorate_catalog_request<T>(&self, request: &mut Request<T>) -> Result<()> {
        let now = UnixMillis::new(now_unix_millis())?;
        let mut scopes = BTreeSet::new();
        scopes.insert(PrincipalScope::new("arrowcol/catalog")?);
        let principal = AuthenticatedPrincipal::new(
            PrincipalId::new("loadtest-catalog")?,
            PrincipalIssuer::new("arrowcol-loadgen")?,
            scopes,
        )?;
        let context = AuthContext::new(principal, AuthMethod::WorkloadIdentity, now, None)?;
        let encoded_context = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&context)?);
        let expiry = now
            .get()
            .checked_add(30_000)
            .context("internal identity expiry overflow")?;
        let canonical = format!("{encoded_context}\n{expiry}");
        let mut mac = HmacSha256::new_from_slice(&self.secret).context("initialize internal HMAC")?;
        mac.update(canonical.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        for (key, value) in [
            (INTERNAL_AUTH_CONTEXT_HEADER, encoded_context),
            (INTERNAL_AUTH_EXPIRES_HEADER, expiry.to_string()),
            (INTERNAL_AUTH_SIGNATURE_HEADER, signature),
        ] {
            request.metadata_mut().insert(key, MetadataValue::from_str(&value)?);
        }
        Ok(())
    }
}

impl EdgeSigner {
    fn from_file(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).with_context(|| format!("read trusted-edge secret {}", path.display()))?;
        let bytes = trim_line_end(bytes);
        if bytes.len() < 32 || bytes.contains(&b'\n') || bytes.contains(&b'\r') {
            bail!("trusted-edge secret must be one line with at least 32 bytes");
        }
        Ok(Self {
            secret: bytes.into(),
            nonce: Arc::new(AtomicU64::new(1)),
        })
    }

    fn insert_signature(&self, headers: &mut HeaderMap, method: &Method, path: &str) -> Result<()> {
        let timestamp = now_unix_millis();
        let nonce = format!(
            "loadgen-{}-{}",
            self.nonce.fetch_add(1, Ordering::Relaxed),
            Uuid::new_v4()
        );
        let canonical = canonical_edge_request(method, path, headers, timestamp, &nonce)?;
        let mut mac = HmacSha256::new_from_slice(&self.secret).context("initialize trusted-edge HMAC")?;
        mac.update(canonical.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        headers.insert(EDGE_TIMESTAMP_HEADER, HeaderValue::from_str(&timestamp.to_string())?);
        headers.insert(EDGE_NONCE_HEADER, HeaderValue::from_str(&nonce)?);
        headers.insert(EDGE_SIGNATURE_HEADER, HeaderValue::from_str(&signature)?);
        Ok(())
    }
}

fn canonical_edge_request(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    timestamp: u64,
    nonce: &str,
) -> Result<String> {
    const SIGNED_HEADERS: [&str; 8] = [
        "x-arrowcol-protocol-version",
        "x-arrowcol-implementation-pool",
        "x-arrowcol-catalog",
        "x-arrowcol-schema",
        "x-arrowcol-table",
        "x-arrowcol-schema-fingerprint",
        "x-arrowcol-control-operation",
        "content-type",
    ];
    if path.is_empty()
        || path.len() > 2_048
        || path.chars().any(char::is_control)
        || nonce.is_empty()
        || nonce.len() > 64
        || !nonce.is_ascii()
        || nonce.bytes().any(|byte| byte.is_ascii_control())
    {
        bail!("trusted-edge path or nonce is invalid");
    }
    let mut canonical = format!("{timestamp}\n{nonce}\n{method}\n{path}\n");
    for key in SIGNED_HEADERS {
        let value = headers.get(key).map(|value| value.to_str()).transpose()?.unwrap_or("");
        if value.len() > 512 || value.chars().any(char::is_control) {
            bail!("trusted-edge signed header is invalid");
        }
        canonical.push_str(key);
        canonical.push(':');
        canonical.push_str(value);
        canonical.push('\n');
    }
    Ok(canonical)
}

fn trim_line_end(mut bytes: Vec<u8>) -> Vec<u8> {
    while matches!(bytes.last(), Some(b'\r' | b'\n')) {
        bytes.pop();
    }
    bytes
}

/// Current Unix epoch milliseconds, saturating only before the epoch.
#[must_use]
pub fn now_unix_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

/// Converts a tonic status to stable bounded manifest fields.
#[must_use]
pub fn status_fields(error: &anyhow::Error) -> (String, String) {
    for cause in error.chain() {
        if let Some(status) = cause.downcast_ref::<tonic::Status>() {
            return (status.code().to_string(), bounded_error(status.message()));
        }
    }
    ("client_error".to_owned(), bounded_error(&error.to_string()))
}

fn bounded_error(value: &str) -> String {
    sanitize_error_message(value)
}

#[cfg(test)]
mod error_diagnostics_tests {
    use super::*;

    #[test]
    fn receipt_cause_is_retained_without_private_credentials_or_paths() {
        let message = redact_error_message(
            "publication failed: unsupported UInt8 statistic; account private-account \
             https://private.example/path?sig=credential C:\\Users\\private\\table \
             /private/table 12345678-1234-1234-1234-123456789abc \
             Server=hidden;Password=opaque-secret \
             {\"client_secret\":\"another-secret\"} authorization: Bearer opaque-token \
             AccountKey = spaced-secret key = second-secret Password=\"three word credential\" \
             Password = \"combined private credential\" AccountKey : 'joined private words' \
             Authorization:Basic opaque-basic-token Password = \"https://private.example quoted url-secret\" \
             \"https://private.example?sig=CANARYSIG standalone private-url-secret\"",
            vec!["private-account".to_owned()],
        );
        assert!(message.contains("unsupported UInt8 statistic"));
        for secret in [
            "private-account",
            "private.example",
            "credential",
            "C:\\",
            "/private",
            "12345678-1234-1234-1234-123456789abc",
            "opaque-secret",
            "another-secret",
            "opaque-token",
            "spaced-secret",
            "second-secret",
            "three",
            "word",
            "credential",
            "combined",
            "private words",
            "joined",
            "opaque-basic-token",
            "url-secret",
            "quoted",
            "standalone",
            "private-url-secret",
            "CANARYSIG",
        ] {
            assert!(!message.contains(secret), "leaked {secret}: {message}");
        }
    }

    #[test]
    fn receipt_cause_is_bounded_control_free_and_utf8_safe() {
        let message = redact_error_message(&format!("原因 \u{1b} unsupported {}", "é".repeat(2_000)), Vec::new());
        assert!(message.contains("unsupported"));
        assert_eq!(message.chars().count(), 1_024);
        assert!(!message.chars().any(char::is_control));
        assert!(serde_json::to_string(&message).is_ok());
    }
}
