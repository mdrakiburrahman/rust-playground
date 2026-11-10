use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context as _, ensure};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, PrincipalId, PrincipalIssuer, PrincipalScope, UnixMillis,
};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use http::{HeaderMap, HeaderValue, Method};
use sha2::Sha256;
use thiserror::Error;

/// Trusted-edge assertion timestamp header.
pub const EDGE_TIMESTAMP_HEADER: &str = "x-arrowcol-edge-timestamp";
/// Trusted-edge unique nonce header.
pub const EDGE_NONCE_HEADER: &str = "x-arrowcol-edge-nonce";
/// Trusted-edge HMAC signature header.
pub const EDGE_SIGNATURE_HEADER: &str = "x-arrowcol-edge-signature";
/// Signed internal authentication context.
pub const INTERNAL_AUTH_CONTEXT_HEADER: &str = "x-arrowcol-internal-auth-context";
/// Internal authentication context expiry.
pub const INTERNAL_AUTH_EXPIRES_HEADER: &str = "x-arrowcol-internal-auth-expires";
/// Internal authentication context HMAC.
pub const INTERNAL_AUTH_SIGNATURE_HEADER: &str = "x-arrowcol-internal-auth-signature";

const MAX_NONCE_BYTES: usize = 64;
const MAX_REPLAY_NONCES: usize = 65_536;
const MAX_SIGNATURE_BYTES: usize = 128;
const MAX_INTERNAL_CONTEXT_BYTES: usize = 3_072;

type HmacSha256 = Hmac<Sha256>;

/// Request facts exposed to a pluggable authenticator after edge verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalRequest {
    /// HTTP method.
    pub method: Method,
    /// Request path.
    pub path: String,
}

/// Authentication failure category.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AuthFailureKind {
    /// Required trusted-edge proof was absent.
    #[error("trusted edge assertion is missing")]
    Missing,
    /// Assertion data was malformed, stale, replayed, or forged.
    #[error("trusted edge assertion is invalid")]
    Invalid,
    /// External traffic attempted to supply internal identity metadata.
    #[error("forged internal identity metadata was supplied externally")]
    ForgedInternalIdentity,
    /// The authenticated principal was not authorized.
    #[error("authenticated principal is forbidden")]
    Forbidden,
    /// Replay protection cannot safely admit another live assertion.
    #[error("edge replay cache is full")]
    ReplayCapacity,
    /// Signing configuration or principal serialization failed.
    #[error("authentication subsystem failed")]
    Internal,
}

/// Safe authentication failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{kind}: {message}")]
pub struct AuthFailure {
    kind: AuthFailureKind,
    message: String,
}

impl AuthFailure {
    fn new(kind: AuthFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> AuthFailureKind {
        self.kind
    }

    /// Returns safe public detail.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Pluggable external authenticator.
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// Authenticates an edge-verified request without retaining credentials.
    async fn authenticate(&self, request: &ExternalRequest, now: UnixMillis) -> Result<AuthContext, AuthFailure>;
}

/// Development authenticator that accepts every edge-verified request.
#[derive(Clone, Default)]
pub struct AllowAllAuthenticator;

#[async_trait]
impl Authenticator for AllowAllAuthenticator {
    async fn authenticate(&self, _request: &ExternalRequest, now: UnixMillis) -> Result<AuthContext, AuthFailure> {
        let mut scopes = BTreeSet::new();
        for scope in ["arrowcol/ingest", "arrowcol/catalog"] {
            scopes.insert(PrincipalScope::new(scope).map_err(principal_failure)?);
        }
        let principal = AuthenticatedPrincipal::new(
            PrincipalId::new("allow-all").map_err(principal_failure)?,
            PrincipalIssuer::new("arrowcol-router").map_err(principal_failure)?,
            scopes,
        )
        .map_err(principal_failure)?;
        AuthContext::new(principal, AuthMethod::WorkloadIdentity, now, None).map_err(principal_failure)
    }
}

/// Reads a one-line secret file, removing one trailing LF or CRLF line ending.
///
/// Signers and verifiers must both load key files through this function so they HMAC identical bytes.
///
/// # Errors
///
/// Rejects unreadable, empty, or multi-line secret files.
pub fn read_secret_file(path: &Path) -> anyhow::Result<Vec<u8>> {
    let value = std::fs::read(path).with_context(|| format!("read secret file {}", path.display()))?;
    let value = value
        .strip_suffix(b"\n")
        .and_then(|value| value.strip_suffix(b"\r").or(Some(value)))
        .unwrap_or(&value);
    ensure!(!value.is_empty(), "secret file {} is empty", path.display());
    ensure!(
        !value.contains(&b'\n') && !value.contains(&b'\r'),
        "secret file {} must contain one line",
        path.display()
    );
    Ok(value.to_vec())
}

/// HMAC signer used by an external TLS terminator or integration test.
#[derive(Clone)]
pub struct EdgeRequestSigner {
    secret: Arc<[u8]>,
}

impl EdgeRequestSigner {
    /// Creates a signer from at least 32 bytes of secret material.
    ///
    /// # Errors
    ///
    /// Rejects short keys.
    pub fn new(secret: impl Into<Vec<u8>>) -> Result<Self, AuthFailure> {
        let secret = secret.into();
        validate_secret(&secret)?;
        Ok(Self { secret: secret.into() })
    }

    /// Computes the canonical signature value.
    ///
    /// # Errors
    ///
    /// Rejects invalid nonce or HMAC initialization.
    pub fn sign(
        &self,
        method: &Method,
        path: &str,
        headers: &HeaderMap,
        timestamp: UnixMillis,
        nonce: &str,
    ) -> Result<String, AuthFailure> {
        validate_nonce(nonce)?;
        let canonical = canonical_edge_request(method, path, headers, timestamp, nonce)?;
        sign(&self.secret, canonical.as_bytes())
    }
}

/// Trusted-edge verifier with bounded replay memory.
pub struct EdgeVerifier {
    signer: EdgeRequestSigner,
    max_age_ms: u64,
    max_replay_nonces: usize,
    seen_nonces: Mutex<ReplayCache>,
}

#[derive(Default)]
struct ReplayCache {
    nonces: HashMap<String, u64>,
    expiries: BinaryHeap<Reverse<(u64, String)>>,
}

impl ReplayCache {
    fn insert(&mut self, nonce: String, expiry: u64) {
        self.expiries.push(Reverse((expiry, nonce.clone())));
        self.nonces.insert(nonce, expiry);
    }

    fn reclaim_expired(&mut self, now: u64) {
        while self.expiries.peek().is_some_and(|Reverse((expiry, _))| *expiry < now) {
            if let Some(Reverse((_, nonce))) = self.expiries.pop() {
                self.nonces.remove(&nonce);
            }
        }
    }
}

impl EdgeVerifier {
    /// Creates a verifier.
    ///
    /// # Errors
    ///
    /// Rejects a zero age or short secret.
    pub fn new(secret: impl Into<Vec<u8>>, max_age_ms: u64) -> Result<Self, AuthFailure> {
        Self::with_replay_capacity(secret, max_age_ms, MAX_REPLAY_NONCES)
    }

    /// Creates a verifier with an explicitly sized, fail-closed replay bound.
    ///
    /// # Errors
    ///
    /// Rejects zero age or capacity, capacity above 1,048,576, or a short secret.
    pub fn with_replay_capacity(
        secret: impl Into<Vec<u8>>,
        max_age_ms: u64,
        max_replay_nonces: usize,
    ) -> Result<Self, AuthFailure> {
        if max_age_ms == 0 || !(1..=1_048_576).contains(&max_replay_nonces) {
            return Err(AuthFailure::new(
                AuthFailureKind::Internal,
                "edge assertion age must be positive and replay capacity must be between 1 and 1048576",
            ));
        }
        Ok(Self {
            signer: EdgeRequestSigner::new(secret)?,
            max_age_ms,
            max_replay_nonces,
            seen_nonces: Mutex::new(ReplayCache::default()),
        })
    }

    /// Rejects direct traffic, forgery, stale assertions, and nonce replay.
    ///
    /// # Errors
    ///
    /// Returns a bounded authentication failure.
    pub fn verify(&self, method: &Method, path: &str, headers: &HeaderMap, now: UnixMillis) -> Result<(), AuthFailure> {
        reject_internal_identity_headers(headers)?;
        let timestamp_text = header_text(headers, EDGE_TIMESTAMP_HEADER, AuthFailureKind::Missing)?;
        if timestamp_text.is_empty()
            || !timestamp_text.bytes().all(|byte| byte.is_ascii_digit())
            || (timestamp_text.len() > 1 && timestamp_text.starts_with('0'))
        {
            return Err(AuthFailure::new(
                AuthFailureKind::Invalid,
                "edge timestamp is not canonical unsigned decimal",
            ));
        }
        let timestamp_value = timestamp_text.parse::<u64>().map_err(|_| {
            AuthFailure::new(
                AuthFailureKind::Invalid,
                "edge timestamp is outside the supported range",
            )
        })?;
        let timestamp = UnixMillis::new(timestamp_value).map_err(|_| {
            AuthFailure::new(
                AuthFailureKind::Invalid,
                "edge timestamp is outside the supported range",
            )
        })?;
        let age = now.get().saturating_sub(timestamp.get());
        let future = timestamp.get().saturating_sub(now.get());
        if age > self.max_age_ms || future > self.max_age_ms {
            return Err(AuthFailure::new(
                AuthFailureKind::Invalid,
                "edge assertion is stale or too far in the future",
            ));
        }
        let nonce = header_text(headers, EDGE_NONCE_HEADER, AuthFailureKind::Missing)?;
        validate_nonce(nonce)?;
        let signature = header_text(headers, EDGE_SIGNATURE_HEADER, AuthFailureKind::Missing)?;
        if signature.len() > MAX_SIGNATURE_BYTES {
            return Err(AuthFailure::new(
                AuthFailureKind::Invalid,
                "edge signature exceeds its bound",
            ));
        }
        let decoded = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| AuthFailure::new(AuthFailureKind::Invalid, "edge signature is not base64url"))?;
        let canonical = canonical_edge_request(method, path, headers, timestamp, nonce)?;
        let mut mac = HmacSha256::new_from_slice(&self.signer.secret)
            .map_err(|_| AuthFailure::new(AuthFailureKind::Internal, "could not initialize edge HMAC"))?;
        mac.update(canonical.as_bytes());
        mac.verify_slice(&decoded)
            .map_err(|_| AuthFailure::new(AuthFailureKind::Invalid, "edge signature verification failed"))?;

        let mut seen = self
            .seen_nonces
            .lock()
            .map_err(|_| AuthFailure::new(AuthFailureKind::Internal, "edge replay cache is unavailable"))?;
        seen.reclaim_expired(now.get());
        if seen.nonces.contains_key(nonce) {
            return Err(AuthFailure::new(
                AuthFailureKind::Invalid,
                "edge assertion nonce was replayed",
            ));
        }
        if seen.nonces.len() >= self.max_replay_nonces {
            return Err(AuthFailure::new(
                AuthFailureKind::ReplayCapacity,
                "edge replay cache is full; retry after assertions expire",
            ));
        }
        seen.insert(nonce.to_owned(), timestamp.get().saturating_add(self.max_age_ms));
        Ok(())
    }
}

/// Signed bounded metadata propagated to collectors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignedInternalIdentity {
    /// Base64url JSON authentication context.
    pub encoded_context: String,
    /// Exclusive expiry as canonical milliseconds.
    pub expires_at_unix_ms: UnixMillis,
    /// Base64url HMAC over context and expiry.
    pub signature: String,
}

/// Signs authenticated identity for the internal hop.
#[derive(Clone)]
pub struct InternalIdentitySigner {
    secret: Arc<[u8]>,
    ttl_ms: u64,
}

impl InternalIdentitySigner {
    /// Creates an internal signer.
    ///
    /// # Errors
    ///
    /// Rejects short keys or zero TTL.
    pub fn new(secret: impl Into<Vec<u8>>, ttl_ms: u64) -> Result<Self, AuthFailure> {
        let secret = secret.into();
        validate_secret(&secret)?;
        if ttl_ms == 0 {
            return Err(AuthFailure::new(
                AuthFailureKind::Internal,
                "internal identity TTL must be positive",
            ));
        }
        Ok(Self {
            secret: secret.into(),
            ttl_ms,
        })
    }

    /// Creates signed bounded internal metadata.
    ///
    /// # Errors
    ///
    /// Returns internal failure when serialization or timestamp arithmetic fails.
    pub fn sign(&self, context: &AuthContext, now: UnixMillis) -> Result<SignedInternalIdentity, AuthFailure> {
        let json = serde_json::to_vec(context)
            .map_err(|_| AuthFailure::new(AuthFailureKind::Internal, "could not serialize authentication context"))?;
        let encoded_context = URL_SAFE_NO_PAD.encode(json);
        if encoded_context.len() > MAX_INTERNAL_CONTEXT_BYTES {
            return Err(AuthFailure::new(
                AuthFailureKind::Internal,
                "internal authentication context exceeds its bound",
            ));
        }
        let expires_at_unix_ms = UnixMillis::new(
            now.get()
                .checked_add(self.ttl_ms)
                .ok_or_else(|| AuthFailure::new(AuthFailureKind::Internal, "internal identity expiry overflow"))?,
        )
        .map_err(|_| AuthFailure::new(AuthFailureKind::Internal, "internal identity expiry is invalid"))?;
        let canonical = format!("{}\n{}", encoded_context, expires_at_unix_ms.get());
        let signature = sign(&self.secret, canonical.as_bytes())?;
        Ok(SignedInternalIdentity {
            encoded_context,
            expires_at_unix_ms,
            signature,
        })
    }
}

fn canonical_edge_request(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    timestamp: UnixMillis,
    nonce: &str,
) -> Result<String, AuthFailure> {
    const SIGNED_HEADERS: [&str; 9] = [
        "x-arrowcol-protocol-version",
        "x-arrowcol-implementation-pool",
        "x-arrowcol-catalog",
        "x-arrowcol-schema",
        "x-arrowcol-table",
        "x-arrowcol-schema-fingerprint",
        "x-arrowcol-control-operation",
        "x-arrowcol-oat-run-id",
        "content-type",
    ];
    if path.is_empty() || path.len() > 2_048 || path.chars().any(char::is_control) {
        return Err(AuthFailure::new(
            AuthFailureKind::Invalid,
            "request path is invalid for edge signing",
        ));
    }
    let mut canonical = format!("{}\n{}\n{}\n{}\n", timestamp.get(), nonce, method, path);
    for key in SIGNED_HEADERS {
        let value = match headers.get(key) {
            Some(value) => value
                .to_str()
                .map_err(|_| AuthFailure::new(AuthFailureKind::Invalid, "signed header is not visible ASCII"))?,
            None => "",
        };
        if value.len() > 512 || value.chars().any(char::is_control) {
            return Err(AuthFailure::new(
                AuthFailureKind::Invalid,
                "signed header exceeds its bound or contains controls",
            ));
        }
        canonical.push_str(key);
        canonical.push(':');
        canonical.push_str(value);
        canonical.push('\n');
    }
    Ok(canonical)
}

fn reject_internal_identity_headers(headers: &HeaderMap) -> Result<(), AuthFailure> {
    if [
        INTERNAL_AUTH_CONTEXT_HEADER,
        INTERNAL_AUTH_EXPIRES_HEADER,
        INTERNAL_AUTH_SIGNATURE_HEADER,
    ]
    .into_iter()
    .any(|key| headers.contains_key(key))
    {
        return Err(AuthFailure::new(
            AuthFailureKind::ForgedInternalIdentity,
            "external requests must not contain internal identity headers",
        ));
    }
    Ok(())
}

fn header_text<'a>(
    headers: &'a HeaderMap,
    key: &'static str,
    missing_kind: AuthFailureKind,
) -> Result<&'a str, AuthFailure> {
    let value = headers
        .get(key)
        .ok_or_else(|| AuthFailure::new(missing_kind, format!("required header {key} is missing")))?;
    value
        .to_str()
        .map_err(|_| AuthFailure::new(AuthFailureKind::Invalid, format!("header {key} is not visible ASCII")))
}

fn validate_nonce(nonce: &str) -> Result<(), AuthFailure> {
    if nonce.is_empty()
        || nonce.len() > MAX_NONCE_BYTES
        || !nonce
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(AuthFailure::new(
            AuthFailureKind::Invalid,
            "edge nonce must be bounded portable ASCII",
        ));
    }
    Ok(())
}

fn validate_secret(secret: &[u8]) -> Result<(), AuthFailure> {
    if secret.len() < 32 {
        return Err(AuthFailure::new(
            AuthFailureKind::Internal,
            "HMAC secret must contain at least 32 bytes",
        ));
    }
    Ok(())
}

fn sign(secret: &[u8], value: &[u8]) -> Result<String, AuthFailure> {
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|_| AuthFailure::new(AuthFailureKind::Internal, "could not initialize HMAC"))?;
    mac.update(value);
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

fn principal_failure(cause: impl std::fmt::Display) -> AuthFailure {
    AuthFailure::new(
        AuthFailureKind::Internal,
        format!("allow-all principal construction failed: {cause}"),
    )
}

/// Inserts trusted-edge headers into a request map.
///
/// This helper is intended for trusted TLS terminators and integration tests.
///
/// # Errors
///
/// Returns an authentication failure when the nonce, signature, timestamp, or
/// generated HTTP header value violates a bound.
pub fn insert_edge_signature_headers(
    headers: &mut HeaderMap,
    signer: &EdgeRequestSigner,
    method: &Method,
    path: &str,
    timestamp: UnixMillis,
    nonce: &str,
) -> Result<(), AuthFailure> {
    let signature = signer.sign(method, path, headers, timestamp, nonce)?;
    headers.insert(
        EDGE_TIMESTAMP_HEADER,
        HeaderValue::from_str(&timestamp.get().to_string())
            .map_err(|_| AuthFailure::new(AuthFailureKind::Internal, "could not encode edge timestamp"))?,
    );
    headers.insert(
        EDGE_NONCE_HEADER,
        HeaderValue::from_str(nonce)
            .map_err(|_| AuthFailure::new(AuthFailureKind::Internal, "could not encode edge nonce"))?,
    );
    headers.insert(
        EDGE_SIGNATURE_HEADER,
        HeaderValue::from_str(&signature)
            .map_err(|_| AuthFailure::new(AuthFailureKind::Internal, "could not encode edge signature"))?,
    );
    Ok(())
}

#[cfg(test)]
mod replay_tests {
    use super::*;

    #[test]
    fn canonical_edge_request_matches_wire_golden_vector() -> Result<(), AuthFailure> {
        let mut headers = HeaderMap::new();
        for (key, value) in [
            ("x-arrowcol-protocol-version", "1"),
            ("x-arrowcol-implementation-pool", "dataflow"),
            ("x-arrowcol-catalog", "telemetry"),
            ("x-arrowcol-schema", "prod"),
            ("x-arrowcol-table", "traces"),
            ("x-arrowcol-schema-fingerprint", "sha256:0123456789abcdef"),
            ("x-arrowcol-oat-run-id", "00000000-0000-0000-0000-000000000001"),
            ("content-type", "application/grpc"),
        ] {
            headers.insert(key, HeaderValue::from_static(value));
        }
        let expected = concat!(
            "100000\nwire-golden-1\nPOST\n/arrow.flight.protocol.FlightService/DoPut\n",
            "x-arrowcol-protocol-version:1\n",
            "x-arrowcol-implementation-pool:dataflow\n",
            "x-arrowcol-catalog:telemetry\n",
            "x-arrowcol-schema:prod\n",
            "x-arrowcol-table:traces\n",
            "x-arrowcol-schema-fingerprint:sha256:0123456789abcdef\n",
            "x-arrowcol-control-operation:\n",
            "x-arrowcol-oat-run-id:00000000-0000-0000-0000-000000000001\n",
            "content-type:application/grpc\n",
        );
        assert_eq!(
            canonical_edge_request(
                &Method::POST,
                "/arrow.flight.protocol.FlightService/DoPut",
                &headers,
                now(100_000),
                "wire-golden-1",
            )?,
            expected
        );
        Ok(())
    }

    fn now(value: u64) -> UnixMillis {
        UnixMillis::new(value).unwrap_or_else(|error| panic!("invalid test timestamp: {error}"))
    }

    fn headers(timestamp: u64, nonce: &str) -> Result<HeaderMap, AuthFailure> {
        let signer = EdgeRequestSigner::new(vec![7_u8; 32])?;
        let mut headers = HeaderMap::new();
        insert_edge_signature_headers(&mut headers, &signer, &Method::POST, "/flight", now(timestamp), nonce)?;
        Ok(headers)
    }

    #[test]
    fn configured_capacity_applies_only_after_signature_verification() -> Result<(), AuthFailure> {
        assert!(EdgeVerifier::with_replay_capacity(vec![7_u8; 32], 30_000, 0).is_err());
        assert!(EdgeVerifier::with_replay_capacity(vec![7_u8; 32], 30_000, 1_048_577).is_err());
        let verifier = EdgeVerifier::with_replay_capacity(vec![7_u8; 32], 30_000, 1)?;
        for nonce in ["forged-1", "forged-2"] {
            let mut forged = headers(100_000, nonce)?;
            forged.insert(EDGE_SIGNATURE_HEADER, HeaderValue::from_static("AAAA"));
            assert!(
                verifier
                    .verify(&Method::POST, "/flight", &forged, now(100_000))
                    .is_err()
            );
        }
        verifier.verify(&Method::POST, "/flight", &headers(100_000, "valid")?, now(100_000))?;
        let Err(failure) = verifier.verify(&Method::POST, "/flight", &headers(100_000, "overflow")?, now(100_000))
        else {
            panic!("configured one-entry cache must reject a second valid nonce");
        };
        assert_eq!(failure.kind(), AuthFailureKind::ReplayCapacity);
        Ok(())
    }

    #[test]
    fn earlier_expiry_is_reclaimed_behind_an_earlier_arriving_future_nonce() -> Result<(), AuthFailure> {
        let verifier = EdgeVerifier::with_replay_capacity(vec![7_u8; 32], 30_000, 2)?;
        let future = headers(130_000, "future-first")?;
        verifier.verify(&Method::POST, "/flight", &future, now(100_000))?;
        verifier.verify(
            &Method::POST,
            "/flight",
            &headers(100_000, "expires-first")?,
            now(100_000),
        )?;
        verifier.verify(
            &Method::POST,
            "/flight",
            &headers(130_001, "replacement")?,
            now(130_001),
        )?;
        let seen = verifier
            .seen_nonces
            .lock()
            .unwrap_or_else(|error| panic!("lock replay cache: {error}"));
        assert_eq!(seen.nonces.len(), 2);
        assert_eq!(seen.expiries.len(), 2);
        assert!(!seen.nonces.contains_key("expires-first"));
        assert!(seen.nonces.contains_key("future-first"));
        drop(seen);
        let replay = verifier.verify(&Method::POST, "/flight", &future, now(130_001));
        assert!(replay.is_err_and(|failure| failure.message() == "edge assertion nonce was replayed"));
        Ok(())
    }

    #[test]
    fn full_replay_cache_rejects_new_nonce_preserves_live_entries_and_reclaims_expired() -> Result<(), AuthFailure> {
        let verifier = EdgeVerifier::new(vec![7_u8; 32], 30_000)?;
        {
            let mut seen = verifier
                .seen_nonces
                .lock()
                .unwrap_or_else(|error| panic!("lock replay cache: {error}"));
            for index in 0..65_536 {
                seen.insert(format!("live-{index}"), 130_000);
            }
        }
        let Err(failure) = verifier.verify(&Method::POST, "/flight", &headers(100_000, "new")?, now(100_000)) else {
            panic!("full replay cache must reject a new nonce");
        };
        assert_eq!(
            failure.message(),
            "edge replay cache is full; retry after assertions expire"
        );
        assert_eq!(failure.kind(), AuthFailureKind::ReplayCapacity);
        let replay = verifier.verify(&Method::POST, "/flight", &headers(100_000, "live-0")?, now(100_000));
        assert!(replay.is_err_and(|failure| failure.message() == "edge assertion nonce was replayed"));
        {
            let seen = verifier
                .seen_nonces
                .lock()
                .unwrap_or_else(|error| panic!("lock replay cache: {error}"));
            assert_eq!(seen.nonces.len(), 65_536);
            assert!(!seen.nonces.contains_key("new"));
        }
        verifier.verify(&Method::POST, "/flight", &headers(130_001, "new")?, now(130_001))?;
        let seen = verifier
            .seen_nonces
            .lock()
            .unwrap_or_else(|error| panic!("lock replay cache: {error}"));
        assert_eq!(seen.nonces.len(), 1);
        assert!(seen.nonces.contains_key("new"));
        Ok(())
    }

    #[test]
    fn future_dated_nonce_remains_protected_through_its_last_valid_millisecond() -> Result<(), AuthFailure> {
        let verifier = EdgeVerifier::new(vec![7_u8; 32], 30_000)?;
        let signed = headers(130_000, "future")?;
        verifier.verify(&Method::POST, "/flight", &signed, now(100_000))?;
        for timestamp in [130_000, 160_000] {
            let Err(failure) = verifier.verify(&Method::POST, "/flight", &signed, now(timestamp)) else {
                panic!("still-valid assertion must remain protected from replay at {timestamp}");
            };
            assert_eq!(failure.message(), "edge assertion nonce was replayed");
        }
        Ok(())
    }
}
