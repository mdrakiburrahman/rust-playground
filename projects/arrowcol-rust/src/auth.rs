use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrowcol_flight::{
    AuthenticationRequest, Authenticator, INTERNAL_IDENTITY_METADATA_KEY, RuntimeError, RuntimeResult,
    TrustedIdentityValidator,
};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, CATALOG_METADATA_KEY, CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY,
    CollectorInstanceId, FencingToken, IMPLEMENTATION_POOL_METADATA_KEY, ImplementationPool, IngestMetadata, LeaseId,
    PROTOCOL_VERSION_METADATA_KEY, PrincipalId, PrincipalIssuer, PrincipalScope, RawGrpcMetadata, SCHEMA_METADATA_KEY,
    TABLE_METADATA_KEY, UnixMillis,
};
use arrowcol_routing::{LeaseKey, LeaseState, RoutingStore};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tonic::Code;
use tonic::metadata::MetadataMap;

use crate::config::AuthMode;

/// Router-signed authentication context header.
pub const INTERNAL_AUTH_CONTEXT_HEADER: &str = "x-arrowcol-internal-auth-context";
/// Router-signed authentication context expiry header.
pub const INTERNAL_AUTH_EXPIRES_HEADER: &str = "x-arrowcol-internal-auth-expires";
/// Router-signed authentication context signature header.
pub const INTERNAL_AUTH_SIGNATURE_HEADER: &str = "x-arrowcol-internal-auth-signature";
/// Router lease lineage header.
pub const ROUTING_LEASE_ID_HEADER: &str = "x-arrowcol-routing-lease-id";
/// Router fencing epoch header.
pub const ROUTING_FENCING_EPOCH_HEADER: &str = "x-arrowcol-routing-fencing-epoch";
/// Router lease compare-and-set version header.
pub const ROUTING_LEASE_VERSION_HEADER: &str = "x-arrowcol-routing-lease-version";
/// Router lease expiry header.
pub const ROUTING_LEASE_EXPIRES_HEADER: &str = "x-arrowcol-routing-lease-expires";

const AUTHORIZATION_HEADER: &str = "authorization";
const MAX_INTERNAL_CONTEXT_BYTES: usize = 3_072;
const MAX_SIGNATURE_BYTES: usize = 128;

type HmacSha256 = Hmac<Sha256>;

/// Verifies the router's HMAC-authenticated principal assertion.
pub struct RouterIdentityValidator {
    secret: Arc<[u8]>,
    maximum_ttl_ms: u64,
}

/// Rejects every internal identity assertion when direct authentication is
/// configured instead of the trusted router hop.
#[derive(Clone, Copy, Debug, Default)]
pub struct RejectInternalIdentityClaims;

#[async_trait]
impl TrustedIdentityValidator for RejectInternalIdentityClaims {
    async fn validate(&self, metadata: &MetadataMap) -> RuntimeResult<Option<AuthContext>> {
        if [
            INTERNAL_IDENTITY_METADATA_KEY,
            INTERNAL_AUTH_CONTEXT_HEADER,
            INTERNAL_AUTH_EXPIRES_HEADER,
            INTERNAL_AUTH_SIGNATURE_HEADER,
        ]
        .into_iter()
        .any(|key| metadata.contains_key(key))
        {
            return Err(authentication_invalid("internal identity assertion is not accepted"));
        }
        Ok(None)
    }
}

impl RouterIdentityValidator {
    /// Creates a validator from at least 32 bytes of secret material.
    ///
    /// # Errors
    ///
    /// Returns a safe configuration error for a short key or zero TTL.
    pub fn new(secret: Vec<u8>, maximum_ttl_ms: u64) -> RuntimeResult<Self> {
        if secret.len() < 32 || maximum_ttl_ms == 0 {
            return Err(RuntimeError::new(
                arrowcol_protocol::ErrorCode::Internal,
                Code::Internal,
                "internal identity trust configuration is invalid",
            ));
        }
        Ok(Self {
            secret: secret.into(),
            maximum_ttl_ms,
        })
    }
}

#[async_trait]
impl TrustedIdentityValidator for RouterIdentityValidator {
    async fn validate(&self, metadata: &MetadataMap) -> RuntimeResult<Option<AuthContext>> {
        if metadata.contains_key(INTERNAL_IDENTITY_METADATA_KEY) {
            return Err(authentication_invalid("unsupported internal identity assertion"));
        }
        let context = metadata_text(metadata, INTERNAL_AUTH_CONTEXT_HEADER);
        let expiry = metadata_text(metadata, INTERNAL_AUTH_EXPIRES_HEADER);
        let signature = metadata_text(metadata, INTERNAL_AUTH_SIGNATURE_HEADER);
        if context.is_none() && expiry.is_none() && signature.is_none() {
            return Ok(None);
        }
        let (context, expiry, signature) = match (context, expiry, signature) {
            (Some(context), Some(expiry), Some(signature)) => (context, expiry, signature),
            _ => return Err(authentication_invalid("internal identity assertion is incomplete")),
        };
        if context.is_empty()
            || context.len() > MAX_INTERNAL_CONTEXT_BYTES
            || signature.is_empty()
            || signature.len() > MAX_SIGNATURE_BYTES
        {
            return Err(authentication_invalid("internal identity assertion exceeds its bounds"));
        }
        let expires_at = parse_canonical_u64(expiry)
            .and_then(|value| UnixMillis::new(value).ok())
            .ok_or_else(|| authentication_invalid("internal identity expiry is malformed"))?;
        let now = now_unix_millis()?;
        if expires_at <= now || expires_at.get().saturating_sub(now.get()) > self.maximum_ttl_ms {
            return Err(authentication_invalid(
                "internal identity assertion is expired or overlong",
            ));
        }
        let decoded_signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| authentication_invalid("internal identity signature is malformed"))?;
        let canonical = format!("{context}\n{}", expires_at.get());
        let mut mac = HmacSha256::new_from_slice(&self.secret).map_err(|_| RuntimeError::internal())?;
        mac.update(canonical.as_bytes());
        mac.verify_slice(&decoded_signature)
            .map_err(|_| authentication_invalid("internal identity signature verification failed"))?;
        let decoded_context = URL_SAFE_NO_PAD
            .decode(context)
            .map_err(|_| authentication_invalid("internal identity context is malformed"))?;
        let auth: AuthContext = serde_json::from_slice(&decoded_context)
            .map_err(|_| authentication_invalid("internal identity context is malformed"))?;
        if auth.is_expired_at(now) {
            return Err(authentication_invalid("internal identity context is expired"));
        }
        Ok(Some(auth))
    }
}

/// Authentication plus authoritative router-lease validation.
pub struct CollectorAuthenticator {
    mode: AuthMode,
    bearer_token: Option<Arc<[u8]>>,
    allow_all: Option<AuthContext>,
    routing: Arc<dyn RoutingStore>,
    instance_id: CollectorInstanceId,
    require_routing_lease: bool,
}

impl CollectorAuthenticator {
    /// Creates the production authentication boundary.
    #[must_use]
    pub fn new(
        mode: AuthMode,
        bearer_token: Option<Vec<u8>>,
        allow_all: Option<AuthContext>,
        routing: Arc<dyn RoutingStore>,
        instance_id: CollectorInstanceId,
        require_routing_lease: bool,
    ) -> Self {
        Self {
            mode,
            bearer_token: bearer_token.map(Into::into),
            allow_all,
            routing,
            instance_id,
            require_routing_lease,
        }
    }

    async fn validate_routing_ownership(
        &self,
        metadata: &MetadataMap,
        ingest: &IngestMetadata,
        now: UnixMillis,
    ) -> RuntimeResult<()> {
        if !self.require_routing_lease {
            reject_partial_routing_headers(metadata)?;
            return Ok(());
        }
        let lease_id = required_metadata(metadata, ROUTING_LEASE_ID_HEADER)?
            .parse::<LeaseId>()
            .map_err(|_| routing_invalid("routing lease identifier is malformed"))?;
        let fencing_epoch = parse_canonical_u64(required_metadata(metadata, ROUTING_FENCING_EPOCH_HEADER)?)
            .and_then(|value| FencingToken::new(value).ok())
            .ok_or_else(|| routing_invalid("routing fencing epoch is malformed"))?;
        let lease_version = parse_canonical_u64(required_metadata(metadata, ROUTING_LEASE_VERSION_HEADER)?)
            .filter(|value| *value > 0)
            .ok_or_else(|| routing_invalid("routing lease version is malformed"))?;
        let expires_at = parse_canonical_u64(required_metadata(metadata, ROUTING_LEASE_EXPIRES_HEADER)?)
            .and_then(|value| UnixMillis::new(value).ok())
            .ok_or_else(|| routing_invalid("routing lease expiry is malformed"))?;
        let view = self.routing.load_view(now).await.map_err(|_| {
            RuntimeError::new(
                arrowcol_protocol::ErrorCode::PoolUnavailable,
                Code::Unavailable,
                "routing ownership could not be verified",
            )
        })?;
        let key = LeaseKey::new(ImplementationPool::Direct, ingest.table().clone());
        let lease = view
            .leases
            .get(&key)
            .ok_or_else(|| lease_fenced("routing lease is missing"))?;
        if lease.state != LeaseState::Active
            || !lease.is_routable_at(now)
            || lease.holder_instance_id != self.instance_id
            || lease.lease_id != lease_id
            || lease.fencing_epoch != fencing_epoch
            || lease.lease_version != lease_version
            || expires_at > lease.expires_at_unix_ms
            || expires_at <= now
        {
            return Err(lease_fenced("routing lease no longer belongs to this collector"));
        }
        Ok(())
    }
}

#[async_trait]
impl Authenticator for CollectorAuthenticator {
    async fn authenticate(&self, request: AuthenticationRequest) -> RuntimeResult<AuthContext> {
        let now = now_unix_millis()?;
        let context = match self.mode {
            AuthMode::RouterHmac => request
                .trusted_identity()
                .cloned()
                .ok_or_else(|| authentication_invalid("trusted router identity is required"))?,
            AuthMode::StaticBearer => {
                if request.trusted_identity().is_some() {
                    return Err(authentication_invalid(
                        "router identity is not accepted in static bearer mode",
                    ));
                }
                let expected = self.bearer_token.as_deref().ok_or_else(RuntimeError::internal)?;
                verify_bearer(request.metadata(), expected)?;
                development_context("static-bearer", now)?
            }
            AuthMode::AllowAll => {
                if request.trusted_identity().is_some() {
                    return Err(authentication_invalid(
                        "router identity is not accepted in allow-all mode",
                    ));
                }
                self.allow_all.clone().ok_or_else(RuntimeError::internal)?
            }
        };

        let ingest = parse_optional_ingest_metadata(request.metadata())?;
        if let Some(ingest) = ingest.as_ref() {
            require_scope(&context, "arrowcol/ingest")?;
            self.validate_routing_ownership(request.metadata(), ingest, now).await?;
        } else {
            require_scope(&context, "arrowcol/catalog")?;
            reject_partial_routing_headers(request.metadata())?;
        }
        Ok(context)
    }
}

/// Builds the explicit development principal used by allow-all mode.
///
/// # Errors
///
/// Returns a safe internal failure when configured principal text is invalid.
pub fn allow_all_context(subject: &str) -> RuntimeResult<AuthContext> {
    development_context(subject, now_unix_millis()?)
}

fn development_context(subject: &str, now: UnixMillis) -> RuntimeResult<AuthContext> {
    let scopes = ["arrowcol/ingest", "arrowcol/receipts", "arrowcol/catalog"]
        .into_iter()
        .map(PrincipalScope::new)
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|_| RuntimeError::internal())?;
    let principal = AuthenticatedPrincipal::new(
        PrincipalId::new(subject).map_err(|_| RuntimeError::internal())?,
        PrincipalIssuer::new("arrowcol-rust").map_err(|_| RuntimeError::internal())?,
        scopes,
    )
    .map_err(|_| RuntimeError::internal())?;
    AuthContext::new(principal, AuthMethod::WorkloadIdentity, now, None).map_err(|_| RuntimeError::internal())
}

fn verify_bearer(metadata: &MetadataMap, expected: &[u8]) -> RuntimeResult<()> {
    let supplied = metadata_text(metadata, AUTHORIZATION_HEADER)
        .ok_or_else(|| authentication_invalid("bearer authorization is missing"))?;
    let supplied = supplied
        .strip_prefix("Bearer ")
        .ok_or_else(|| authentication_invalid("bearer authorization is malformed"))?;
    if supplied.as_bytes().ct_eq(expected).unwrap_u8() != 1 {
        return Err(authentication_invalid("bearer authorization is invalid"));
    }
    Ok(())
}

fn require_scope(context: &AuthContext, required: &str) -> RuntimeResult<()> {
    if context
        .principal()
        .scopes()
        .iter()
        .any(|scope| scope.as_str() == required)
    {
        Ok(())
    } else {
        Err(RuntimeError::new(
            arrowcol_protocol::ErrorCode::AuthenticationForbidden,
            Code::PermissionDenied,
            "authenticated principal is not authorized for this operation",
        ))
    }
}

fn parse_optional_ingest_metadata(metadata: &MetadataMap) -> RuntimeResult<Option<IngestMetadata>> {
    if !metadata.contains_key(PROTOCOL_VERSION_METADATA_KEY) {
        return Ok(None);
    }
    let raw = RawGrpcMetadata {
        protocol_version: metadata_text(metadata, PROTOCOL_VERSION_METADATA_KEY),
        implementation_pool: metadata_text(metadata, IMPLEMENTATION_POOL_METADATA_KEY),
        catalog: metadata_text(metadata, CATALOG_METADATA_KEY),
        schema: metadata_text(metadata, SCHEMA_METADATA_KEY),
        table: metadata_text(metadata, TABLE_METADATA_KEY),
        claimed_schema_fingerprint: metadata_text(metadata, CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY),
    };
    IngestMetadata::parse(raw)
        .map(Some)
        .map_err(|error| RuntimeError::from_metadata(&error))
}

fn reject_partial_routing_headers(metadata: &MetadataMap) -> RuntimeResult<()> {
    let count = [
        ROUTING_LEASE_ID_HEADER,
        ROUTING_FENCING_EPOCH_HEADER,
        ROUTING_LEASE_VERSION_HEADER,
        ROUTING_LEASE_EXPIRES_HEADER,
    ]
    .into_iter()
    .filter(|key| metadata.contains_key(*key))
    .count();
    if count == 0 {
        Ok(())
    } else {
        Err(routing_invalid(
            "routing lease headers were supplied where they are not accepted",
        ))
    }
}

fn required_metadata<'a>(metadata: &'a MetadataMap, key: &'static str) -> RuntimeResult<&'a str> {
    metadata_text(metadata, key).ok_or_else(|| routing_invalid("required trusted metadata is missing"))
}

fn metadata_text<'a>(metadata: &'a MetadataMap, key: &'static str) -> Option<&'a str> {
    metadata.get(key).and_then(|value| value.to_str().ok())
}

fn parse_canonical_u64(value: &str) -> Option<u64> {
    if value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return None;
    }
    value.parse().ok()
}

fn now_unix_millis() -> RuntimeResult<UnixMillis> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RuntimeError::internal())?
        .as_millis();
    let millis = u64::try_from(millis).map_err(|_| RuntimeError::internal())?;
    UnixMillis::new(millis).map_err(|_| RuntimeError::internal())
}

fn authentication_invalid(message: &'static str) -> RuntimeError {
    RuntimeError::new(
        arrowcol_protocol::ErrorCode::AuthenticationInvalid,
        Code::Unauthenticated,
        message,
    )
}

fn routing_invalid(message: &'static str) -> RuntimeError {
    RuntimeError::new(
        arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
        Code::InvalidArgument,
        message,
    )
}

fn lease_fenced(message: &'static str) -> RuntimeError {
    RuntimeError::new(
        arrowcol_protocol::ErrorCode::LeaseFenced,
        Code::FailedPrecondition,
        message,
    )
}
