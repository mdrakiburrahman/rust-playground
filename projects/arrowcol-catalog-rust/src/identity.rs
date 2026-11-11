use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, bail};
use arrowcol_catalog_client::{
    INTERNAL_AUTH_CONTEXT_HEADER, INTERNAL_AUTH_EXPIRES_HEADER, INTERNAL_AUTH_METHOD_HEADER,
    INTERNAL_AUTH_NONCE_HEADER, INTERNAL_AUTH_PAYLOAD_DIGEST_HEADER, INTERNAL_AUTH_SIGNATURE_HEADER,
    MAX_INTERNAL_AUTH_TTL_MS, payload_digest,
};
use arrowcol_protocol::{AuthContext, UnixMillis};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tonic::{Status, metadata::MetadataMap};

const MAX_INTERNAL_CONTEXT_BYTES: usize = 3_072;
const MAX_INTERNAL_SIGNATURE_BYTES: usize = 64;
const MAX_REPLAY_ENTRIES: usize = 65_536;

type HmacSha256 = Hmac<Sha256>;

/// Verifies the router/loadgen signed internal-identity metadata used by catalog DDL.
#[derive(Clone)]
pub struct DdlIdentityVerifier {
    secret: Arc<[u8]>,
    replay: Arc<Mutex<ReplayState>>,
}

#[derive(Default)]
struct ReplayState {
    expiries: BTreeMap<u64, Vec<String>>,
    nonces: HashMap<String, u64>,
}

impl DdlIdentityVerifier {
    /// Creates a verifier from the shared internal HMAC secret.
    ///
    /// # Errors
    ///
    /// Returns an error when the secret is shorter than 32 bytes or is not a
    /// single-line value.
    pub fn from_secret(secret: impl Into<Vec<u8>>) -> anyhow::Result<Self> {
        let secret = secret.into();
        if secret.len() < 32 || secret.contains(&b'\n') || secret.contains(&b'\r') {
            bail!("internal HMAC secret must be one line with at least 32 bytes");
        }
        Ok(Self {
            secret: secret.into(),
            replay: Arc::new(Mutex::new(ReplayState::default())),
        })
    }

    /// Loads the shared internal HMAC secret from a mounted file.
    ///
    /// # Errors
    ///
    /// Returns an error for an unreadable or invalid secret file.
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).with_context(|| format!("read internal HMAC secret {}", path.display()))?;
        let bytes = trim_line_end(bytes);
        Self::from_secret(bytes).with_context(|| format!("validate internal HMAC secret {}", path.display()))
    }

    /// Validates one signed assertion bound to an RPC method and payload.
    ///
    /// # Errors
    ///
    /// Returns `Unauthenticated` for missing, expired, malformed, duplicated,
    /// replayed, or incorrectly signed request metadata. Returns
    /// `ResourceExhausted` when the bounded replay cache is full.
    pub fn verify_request(
        &self,
        metadata: &MetadataMap,
        expected_method: &str,
        payload: &[u8],
        now_ms: u64,
    ) -> Result<AuthContext, Status> {
        let context = single_header(metadata, INTERNAL_AUTH_CONTEXT_HEADER)?;
        let expiry = single_header(metadata, INTERNAL_AUTH_EXPIRES_HEADER)?;
        let method = single_header(metadata, INTERNAL_AUTH_METHOD_HEADER)?;
        let nonce = single_header(metadata, INTERNAL_AUTH_NONCE_HEADER)?;
        let digest = single_header(metadata, INTERNAL_AUTH_PAYLOAD_DIGEST_HEADER)?;
        let signature = single_header(metadata, INTERNAL_AUTH_SIGNATURE_HEADER)?;
        let (Some(context), Some(expiry), Some(method), Some(nonce), Some(digest), Some(signature)) =
            (context, expiry, method, nonce, digest, signature)
        else {
            return Err(unauthenticated());
        };
        if context.is_empty()
            || context.len() > MAX_INTERNAL_CONTEXT_BYTES
            || signature.len() > MAX_INTERNAL_SIGNATURE_BYTES
            || method != expected_method
            || nonce.len() != 32
            || !nonce.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(unauthenticated());
        }
        let expiry_ms = expiry.parse::<u64>().map_err(|_| unauthenticated())?;
        if expiry_ms.to_string() != expiry
            || expiry_ms <= now_ms
            || expiry_ms.saturating_sub(now_ms) > MAX_INTERNAL_AUTH_TTL_MS
            || digest != payload_digest(payload)
        {
            return Err(unauthenticated());
        }

        let context_bytes = URL_SAFE_NO_PAD.decode(context).map_err(|_| unauthenticated())?;
        if URL_SAFE_NO_PAD.encode(&context_bytes) != context {
            return Err(unauthenticated());
        }
        let signature_bytes = URL_SAFE_NO_PAD.decode(signature).map_err(|_| unauthenticated())?;
        if URL_SAFE_NO_PAD.encode(&signature_bytes) != signature {
            return Err(unauthenticated());
        }

        let canonical = format!("{context}\n{expiry}\n{method}\n{digest}\n{nonce}");
        let mut mac = HmacSha256::new_from_slice(&self.secret).map_err(|_| unauthenticated())?;
        mac.update(canonical.as_bytes());
        mac.verify_slice(&signature_bytes).map_err(|_| unauthenticated())?;

        let identity: AuthContext = serde_json::from_slice(&context_bytes).map_err(|_| unauthenticated())?;
        if identity.is_expired_at(UnixMillis::new(now_ms).map_err(|_| unauthenticated())?) {
            return Err(unauthenticated());
        }
        let mut replay = self
            .replay
            .lock()
            .map_err(|_| Status::internal("catalog replay protection is unavailable"))?;
        let expired: Vec<u64> = replay.expiries.range(..=now_ms).map(|(expiry, _)| *expiry).collect();
        for expired_at in expired {
            if let Some(nonces) = replay.expiries.remove(&expired_at) {
                for expired_nonce in nonces {
                    replay.nonces.remove(&expired_nonce);
                }
            }
        }
        if replay.nonces.contains_key(nonce) {
            return Err(unauthenticated());
        }
        if replay.nonces.len() >= MAX_REPLAY_ENTRIES {
            return Err(Status::resource_exhausted("catalog replay protection capacity reached"));
        }
        replay.nonces.insert(nonce.to_owned(), expiry_ms);
        replay.expiries.entry(expiry_ms).or_default().push(nonce.to_owned());
        Ok(identity)
    }
}

/// Catalog DDL authentication behavior; the unsigned mode is explicitly development-only.
#[derive(Clone)]
pub enum DdlIdentityPolicy {
    Reject,
    RequireSigned(DdlIdentityVerifier),
    DevOnlyAllowUnsigned,
}

impl DdlIdentityPolicy {
    /// Requires a valid signed identity on every DDL request.
    #[must_use]
    pub const fn require_signed(verifier: DdlIdentityVerifier) -> Self {
        Self::RequireSigned(verifier)
    }

    /// Allows only requests with no identity headers, for focused local development.
    #[must_use]
    pub const fn dev_only_allow_unsigned() -> Self {
        Self::DevOnlyAllowUnsigned
    }

    /// Rejects all DDL requests until an explicit policy is configured.
    #[must_use]
    pub const fn reject() -> Self {
        Self::Reject
    }

    /// Enforces the selected identity policy for a request.
    ///
    /// # Errors
    ///
    /// Returns `Unauthenticated` if a request is not allowed by the policy.
    pub fn verify_request(
        &self,
        metadata: &MetadataMap,
        expected_method: &str,
        payload: &[u8],
        now_ms: u64,
    ) -> Result<(), Status> {
        match self {
            Self::RequireSigned(verifier) => verifier
                .verify_request(metadata, expected_method, payload, now_ms)
                .map(|_| ()),
            Self::DevOnlyAllowUnsigned => {
                if has_any_identity_header(metadata) {
                    Err(unauthenticated())
                } else {
                    Ok(())
                }
            }
            Self::Reject => Err(unauthenticated()),
        }
    }
}

fn single_header<'a>(metadata: &'a MetadataMap, key: &'static str) -> Result<Option<&'a str>, Status> {
    let mut values = metadata.get_all(key).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(unauthenticated());
    }
    value.to_str().map(Some).map_err(|_| unauthenticated())
}

fn has_any_identity_header(metadata: &MetadataMap) -> bool {
    [
        INTERNAL_AUTH_CONTEXT_HEADER,
        INTERNAL_AUTH_EXPIRES_HEADER,
        INTERNAL_AUTH_METHOD_HEADER,
        INTERNAL_AUTH_NONCE_HEADER,
        INTERNAL_AUTH_PAYLOAD_DIGEST_HEADER,
        INTERNAL_AUTH_SIGNATURE_HEADER,
    ]
    .into_iter()
    .any(|key| metadata.contains_key(key))
}

fn unauthenticated() -> Status {
    Status::unauthenticated("catalog DDL requires a valid signed internal identity")
}

fn trim_line_end(mut bytes: Vec<u8>) -> Vec<u8> {
    while matches!(bytes.last(), Some(b'\r' | b'\n')) {
        bytes.pop();
    }
    bytes
}

pub(crate) fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrowcol_protocol::{AuthMethod, AuthenticatedPrincipal, PrincipalId, PrincipalIssuer, PrincipalScope};
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use tonic::metadata::MetadataMap;

    use super::*;

    type HmacSha256 = Hmac<Sha256>;
    const KEY: &[u8] = b"catalog-ddl-test-key-must-be-at-least-32-bytes";
    const METHOD: &str = "/arrowcol.catalog.v1.SchemaCatalog/GetSnapshot";
    const PAYLOAD: &[u8] = b"snapshot-request";

    fn identity(now_ms: u64) -> anyhow::Result<AuthContext> {
        let principal = AuthenticatedPrincipal::new(
            PrincipalId::new("loadtest-catalog")?,
            PrincipalIssuer::new("arrowcol-loadgen")?,
            BTreeSet::from([PrincipalScope::new("arrowcol/catalog")?]),
        )?;
        Ok(AuthContext::new(
            principal,
            AuthMethod::WorkloadIdentity,
            UnixMillis::new(now_ms)?,
            None,
        )?)
    }

    fn signed_metadata(
        context: &AuthContext,
        key: &[u8],
        expiry: u64,
        method: &str,
        payload: &[u8],
        nonce: &str,
    ) -> anyhow::Result<MetadataMap> {
        let encoded_context = URL_SAFE_NO_PAD.encode(serde_json::to_vec(context)?);
        let expiry = expiry.to_string();
        let digest = payload_digest(payload);
        let canonical = format!("{encoded_context}\n{expiry}\n{method}\n{digest}\n{nonce}");
        let mut mac = HmacSha256::new_from_slice(key)?;
        mac.update(canonical.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        let mut metadata = MetadataMap::new();
        metadata.insert(INTERNAL_AUTH_CONTEXT_HEADER, encoded_context.parse()?);
        metadata.insert(INTERNAL_AUTH_EXPIRES_HEADER, expiry.parse()?);
        metadata.insert(INTERNAL_AUTH_METHOD_HEADER, method.parse()?);
        metadata.insert(INTERNAL_AUTH_NONCE_HEADER, nonce.parse()?);
        metadata.insert(INTERNAL_AUTH_PAYLOAD_DIGEST_HEADER, digest.parse()?);
        metadata.insert(INTERNAL_AUTH_SIGNATURE_HEADER, signature.parse()?);
        Ok(metadata)
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    #[test]
    fn accepts_one_signed_method_and_payload_then_rejects_replay() -> anyhow::Result<()> {
        let now = now_ms();
        let expected = identity(now)?;
        let metadata = signed_metadata(
            &expected,
            KEY,
            now + 30_000,
            METHOD,
            PAYLOAD,
            "0123456789abcdef0123456789abcdef",
        )?;
        let verifier = DdlIdentityVerifier::from_secret(KEY.to_vec())?;

        assert_eq!(verifier.verify_request(&metadata, METHOD, PAYLOAD, now)?, expected);
        assert_eq!(
            verifier
                .verify_request(&metadata, METHOD, PAYLOAD, now)
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
        Ok(())
    }

    #[test]
    fn rejects_changed_payload_method_expiry_and_duplicate_identity_metadata() -> anyhow::Result<()> {
        let now = now_ms();
        let expected = identity(now)?;
        let verifier = DdlIdentityVerifier::from_secret(KEY.to_vec())?;
        let metadata = signed_metadata(
            &expected,
            KEY,
            now + 30_000,
            METHOD,
            PAYLOAD,
            "1123456789abcdef0123456789abcdef",
        )?;
        assert!(
            verifier
                .verify_request(&metadata, METHOD, b"substituted-request", now)
                .is_err()
        );
        assert!(
            verifier
                .verify_request(&metadata, "/arrowcol.catalog.v1.SchemaCatalog/GetEvents", PAYLOAD, now)
                .is_err()
        );
        assert!(verifier.verify_request(&metadata, METHOD, PAYLOAD, now + 1).is_ok());

        let too_long = signed_metadata(
            &expected,
            KEY,
            now + MAX_INTERNAL_AUTH_TTL_MS + 1,
            METHOD,
            PAYLOAD,
            "2123456789abcdef0123456789abcdef",
        )?;
        assert!(verifier.verify_request(&too_long, METHOD, PAYLOAD, now).is_err());

        let mut duplicate = signed_metadata(
            &expected,
            KEY,
            now + 30_000,
            METHOD,
            PAYLOAD,
            "3123456789abcdef0123456789abcdef",
        )?;
        duplicate.append(INTERNAL_AUTH_CONTEXT_HEADER, "second".parse()?);
        assert!(verifier.verify_request(&duplicate, METHOD, PAYLOAD, now).is_err());
        Ok(())
    }

    #[test]
    fn reclaims_earlier_expiry_inserted_after_a_future_skewed_nonce() -> anyhow::Result<()> {
        let verifier = DdlIdentityVerifier::from_secret(KEY.to_vec())?;
        let context = identity(1_000)?;
        let future = signed_metadata(
            &context,
            KEY,
            1_200,
            METHOD,
            PAYLOAD,
            "4123456789abcdef0123456789abcdef",
        )?;
        let earlier = signed_metadata(
            &context,
            KEY,
            1_050,
            METHOD,
            PAYLOAD,
            "5123456789abcdef0123456789abcdef",
        )?;
        verifier.verify_request(&future, METHOD, PAYLOAD, 1_000)?;
        verifier.verify_request(&earlier, METHOD, PAYLOAD, 1_000)?;

        let later_request = signed_metadata(
            &context,
            KEY,
            2_000,
            METHOD,
            PAYLOAD,
            "6123456789abcdef0123456789abcdef",
        )?;
        verifier.verify_request(&later_request, METHOD, PAYLOAD, 1_100)?;
        let replay = verifier.replay.lock().expect("test replay mutex");
        assert!(replay.nonces.contains_key("4123456789abcdef0123456789abcdef"));
        assert!(!replay.nonces.contains_key("5123456789abcdef0123456789abcdef"));
        Ok(())
    }

    #[test]
    fn unsigned_ddl_is_allowed_only_by_the_explicit_development_policy() {
        let metadata = MetadataMap::new();
        assert!(
            DdlIdentityPolicy::dev_only_allow_unsigned()
                .verify_request(&metadata, METHOD, PAYLOAD, now_ms())
                .is_ok()
        );
        assert!(
            DdlIdentityPolicy::reject()
                .verify_request(&metadata, METHOD, PAYLOAD, now_ms())
                .is_err()
        );
    }
}
