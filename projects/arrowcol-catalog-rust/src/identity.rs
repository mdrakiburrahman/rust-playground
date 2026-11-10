use std::{
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, bail};
use arrowcol_protocol::{AuthContext, UnixMillis};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tonic::{Status, metadata::MetadataMap};

const INTERNAL_AUTH_CONTEXT_HEADER: &str = "x-arrowcol-internal-auth-context";
const INTERNAL_AUTH_EXPIRES_HEADER: &str = "x-arrowcol-internal-auth-expires";
const INTERNAL_AUTH_SIGNATURE_HEADER: &str = "x-arrowcol-internal-auth-signature";
const MAX_INTERNAL_CONTEXT_BYTES: usize = 3_072;
const MAX_INTERNAL_SIGNATURE_BYTES: usize = 64;

type HmacSha256 = Hmac<Sha256>;

/// Verifies the router/loadgen signed internal-identity metadata used by catalog DDL.
#[derive(Clone)]
pub struct DdlIdentityVerifier {
    secret: Arc<[u8]>,
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
        Ok(Self { secret: secret.into() })
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

    /// Validates one complete signed identity using the existing wire contract.
    ///
    /// # Errors
    ///
    /// Returns `Unauthenticated` for missing, expired, malformed, duplicated,
    /// or incorrectly signed identity metadata.
    pub fn verify(&self, metadata: &MetadataMap, now_ms: u64) -> Result<AuthContext, Status> {
        let context = single_header(metadata, INTERNAL_AUTH_CONTEXT_HEADER)?;
        let expiry = single_header(metadata, INTERNAL_AUTH_EXPIRES_HEADER)?;
        let signature = single_header(metadata, INTERNAL_AUTH_SIGNATURE_HEADER)?;
        let (Some(context), Some(expiry), Some(signature)) = (context, expiry, signature) else {
            return Err(unauthenticated());
        };
        if context.is_empty()
            || context.len() > MAX_INTERNAL_CONTEXT_BYTES
            || signature.len() > MAX_INTERNAL_SIGNATURE_BYTES
        {
            return Err(unauthenticated());
        }
        let expiry_ms = expiry.parse::<u64>().map_err(|_| unauthenticated())?;
        if expiry_ms.to_string() != expiry || expiry_ms <= now_ms {
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

        let canonical = format!("{context}\n{expiry}");
        let mut mac = HmacSha256::new_from_slice(&self.secret).map_err(|_| unauthenticated())?;
        mac.update(canonical.as_bytes());
        mac.verify_slice(&signature_bytes).map_err(|_| unauthenticated())?;

        let identity: AuthContext = serde_json::from_slice(&context_bytes).map_err(|_| unauthenticated())?;
        if identity.is_expired_at(UnixMillis::new(now_ms).map_err(|_| unauthenticated())?) {
            return Err(unauthenticated());
        }
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

    /// Enforces the selected policy for catalog DDL.
    ///
    /// # Errors
    ///
    /// Returns `Unauthenticated` if a request is not allowed by the policy.
    pub fn verify(&self, metadata: &MetadataMap, now_ms: u64) -> Result<(), Status> {
        match self {
            Self::RequireSigned(verifier) => verifier.verify(metadata, now_ms).map(|_| ()),
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

    fn identity(now_ms: u64) -> anyhow::Result<AuthContext> {
        let scopes = BTreeSet::from([PrincipalScope::new("arrowcol/catalog")?]);
        let principal = AuthenticatedPrincipal::new(
            PrincipalId::new("loadtest-catalog")?,
            PrincipalIssuer::new("arrowcol-loadgen")?,
            scopes,
        )?;
        Ok(AuthContext::new(
            principal,
            AuthMethod::WorkloadIdentity,
            UnixMillis::new(now_ms)?,
            None,
        )?)
    }

    fn signed_metadata(context: &AuthContext, key: &[u8], expiry: u64) -> anyhow::Result<MetadataMap> {
        let encoded_context = URL_SAFE_NO_PAD.encode(serde_json::to_vec(context)?);
        let canonical = format!("{encoded_context}\n{expiry}");
        let mut mac = HmacSha256::new_from_slice(key)?;
        mac.update(canonical.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        let mut metadata = MetadataMap::new();
        metadata.insert(INTERNAL_AUTH_CONTEXT_HEADER, encoded_context.parse()?);
        metadata.insert(INTERNAL_AUTH_EXPIRES_HEADER, expiry.to_string().parse()?);
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
    fn verifies_the_existing_loadgen_signed_identity_contract() -> anyhow::Result<()> {
        let now = now_ms();
        let expected = identity(now)?;
        let metadata = signed_metadata(&expected, KEY, now + 30_000)?;
        let verifier = DdlIdentityVerifier::from_secret(KEY.to_vec())?;

        assert_eq!(verifier.verify(&metadata, now)?, expected);
        Ok(())
    }

    #[test]
    fn rejects_tampered_expired_missing_and_duplicate_identity_metadata() -> anyhow::Result<()> {
        let now = now_ms();
        let expected = identity(now)?;
        let verifier = DdlIdentityVerifier::from_secret(KEY.to_vec())?;

        let mut tampered = signed_metadata(&expected, KEY, now + 30_000)?;
        tampered.insert(INTERNAL_AUTH_SIGNATURE_HEADER, "invalid".parse()?);
        assert!(verifier.verify(&tampered, now).is_err());

        let expired = signed_metadata(&expected, KEY, now)?;
        assert!(verifier.verify(&expired, now).is_err());
        assert!(verifier.verify(&MetadataMap::new(), now).is_err());

        let mut duplicate = signed_metadata(&expected, KEY, now + 30_000)?;
        duplicate.append(INTERNAL_AUTH_CONTEXT_HEADER, "second".parse()?);
        assert!(verifier.verify(&duplicate, now).is_err());
        Ok(())
    }

    #[test]
    fn unsigned_ddl_is_allowed_only_by_the_explicit_development_policy() {
        let metadata = MetadataMap::new();
        assert!(
            DdlIdentityPolicy::dev_only_allow_unsigned()
                .verify(&metadata, now_ms())
                .is_ok()
        );
        assert!(DdlIdentityPolicy::reject().verify(&metadata, now_ms()).is_err());
    }
}
