use std::collections::{BTreeSet, HashMap};
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
    seen_nonces: Mutex<HashMap<String, u64>>,
}

impl EdgeVerifier {
    /// Creates a verifier.
    ///
    /// # Errors
    ///
    /// Rejects a zero age or short secret.
    pub fn new(secret: impl Into<Vec<u8>>, max_age_ms: u64) -> Result<Self, AuthFailure> {
        if max_age_ms == 0 {
            return Err(AuthFailure::new(
                AuthFailureKind::Internal,
                "edge assertion maximum age must be positive",
            ));
        }
        Ok(Self {
            signer: EdgeRequestSigner::new(secret)?,
            max_age_ms,
            seen_nonces: Mutex::new(HashMap::new()),
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
        seen.retain(|_, expiry| *expiry > now.get());
        if seen.contains_key(nonce) {
            return Err(AuthFailure::new(
                AuthFailureKind::Invalid,
                "edge assertion nonce was replayed",
            ));
        }
        seen.insert(nonce.to_owned(), now.get().saturating_add(self.max_age_ms));
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
