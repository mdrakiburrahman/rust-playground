use std::{
    collections::BTreeSet,
    path::Path,
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, bail};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, PrincipalId, PrincipalIssuer, PrincipalScope, UnixMillis,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use prost::Message;
use sha2::{Digest, Sha256};
use tonic::{Request, metadata::MetadataValue};
use uuid::Uuid;

/// Maximum lifetime of an internal signed catalog RPC assertion.
pub const MAX_INTERNAL_AUTH_TTL_MS: u64 = 30_000;
/// Identity context metadata header.
pub const INTERNAL_AUTH_CONTEXT_HEADER: &str = "x-arrowcol-internal-auth-context";
/// Assertion expiry metadata header.
pub const INTERNAL_AUTH_EXPIRES_HEADER: &str = "x-arrowcol-internal-auth-expires";
/// RPC method/path metadata header.
pub const INTERNAL_AUTH_METHOD_HEADER: &str = "x-arrowcol-internal-auth-method";
/// One-time assertion nonce metadata header.
pub const INTERNAL_AUTH_NONCE_HEADER: &str = "x-arrowcol-internal-auth-nonce";
/// Canonical payload digest metadata header.
pub const INTERNAL_AUTH_PAYLOAD_DIGEST_HEADER: &str = "x-arrowcol-internal-auth-payload-digest";
/// HMAC signature metadata header.
pub const INTERNAL_AUTH_SIGNATURE_HEADER: &str = "x-arrowcol-internal-auth-signature";

type HmacSha256 = Hmac<Sha256>;

/// HMAC signer for catalog control-plane RPC requests.
#[derive(Clone)]
pub struct InternalRequestSigner {
    secret: Arc<[u8]>,
}

impl InternalRequestSigner {
    /// Loads a signing key from a mounted secret file.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is unreadable, multi-line, or shorter than
    /// 32 bytes.
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let mut secret =
            std::fs::read(path).with_context(|| format!("read internal HMAC secret {}", path.display()))?;
        while secret.last().is_some_and(|byte| matches!(byte, b'\n' | b'\r')) {
            secret.pop();
        }
        Self::from_secret(secret)
    }

    /// Creates a signer from secret bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is multi-line or shorter than 32 bytes.
    pub fn from_secret(secret: impl Into<Vec<u8>>) -> anyhow::Result<Self> {
        let secret = secret.into();
        if secret.len() < 32 || secret.contains(&b'\n') || secret.contains(&b'\r') {
            bail!("internal HMAC secret must be one line with at least 32 bytes");
        }
        Ok(Self { secret: secret.into() })
    }

    /// Signs an encoded protobuf request for one exact gRPC method path.
    ///
    /// # Errors
    ///
    /// Returns an error when the method path or assertion identity is invalid,
    /// or the request metadata cannot be represented.
    pub fn sign_message<T: Message>(&self, request: &mut Request<T>, method_path: &str) -> anyhow::Result<()> {
        self.sign_payload(request, method_path, &request.get_ref().encode_to_vec())
    }

    /// Signs an arbitrary canonical payload for one exact gRPC method path.
    ///
    /// # Errors
    ///
    /// Returns an error when the method path or assertion identity is invalid,
    /// or the request metadata cannot be represented.
    pub fn sign_payload<T>(&self, request: &mut Request<T>, method_path: &str, payload: &[u8]) -> anyhow::Result<()> {
        if method_path.is_empty() || method_path.contains(['\n', '\r']) {
            bail!("invalid internal RPC method path");
        }
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let now_ms = u64::try_from(now_ms)?;
        let expires_ms = now_ms
            .checked_add(MAX_INTERNAL_AUTH_TTL_MS)
            .context("internal assertion expiry overflow")?;
        let principal = AuthenticatedPrincipal::new(
            PrincipalId::new("arrowcol-internal-client")?,
            PrincipalIssuer::new("arrowcol-internal-hmac")?,
            BTreeSet::from([PrincipalScope::new("catalog:rpc")?]),
        )?;
        let identity = AuthContext::new(
            principal,
            AuthMethod::WorkloadIdentity,
            UnixMillis::new(now_ms)?,
            Some(UnixMillis::new(expires_ms)?),
        )?;
        let encoded_context = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&identity)?);
        let expiry = expires_ms.to_string();
        let nonce = Uuid::new_v4().simple().to_string();
        let digest = payload_digest(payload);
        let canonical = canonical_request(&encoded_context, &expiry, method_path, &digest, &nonce);
        let mut mac = HmacSha256::new_from_slice(&self.secret).context("initialize internal HMAC signer")?;
        mac.update(canonical.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        let metadata = request.metadata_mut();
        metadata.insert(INTERNAL_AUTH_CONTEXT_HEADER, MetadataValue::from_str(&encoded_context)?);
        metadata.insert(INTERNAL_AUTH_EXPIRES_HEADER, MetadataValue::from_str(&expiry)?);
        metadata.insert(INTERNAL_AUTH_METHOD_HEADER, MetadataValue::from_str(method_path)?);
        metadata.insert(INTERNAL_AUTH_NONCE_HEADER, MetadataValue::from_str(&nonce)?);
        metadata.insert(INTERNAL_AUTH_PAYLOAD_DIGEST_HEADER, MetadataValue::from_str(&digest)?);
        metadata.insert(INTERNAL_AUTH_SIGNATURE_HEADER, MetadataValue::from_str(&signature)?);
        Ok(())
    }
}

/// Computes the URL-safe SHA-256 digest used to bind request payload bytes.
#[must_use]
pub fn payload_digest(payload: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(payload))
}

/// Builds the newline-delimited signed representation shared by signer/verifier.
#[must_use]
pub fn canonical_request(context: &str, expiry: &str, method_path: &str, digest: &str, nonce: &str) -> String {
    format!("{context}\n{expiry}\n{method_path}\n{digest}\n{nonce}")
}
