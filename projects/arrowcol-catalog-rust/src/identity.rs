#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrowcol_protocol::{
        AuthContext, AuthMethod, AuthenticatedPrincipal, PrincipalId, PrincipalIssuer, PrincipalScope, UnixMillis,
    };
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use hmac::{Hmac, Mac};
    use serde_json;
    use sha2::Sha256;
    use tonic::metadata::MetadataMap;

    use super::*;

    type HmacSha256 = Hmac<Sha256>;
    const KEY: &[u8] = b"catalog-ddl-test-key-must-be-at-least-32-bytes";

    fn identity(now_ms: u64) -> AuthContext {
        let scopes = BTreeSet::from([PrincipalScope::new("arrowcol/catalog").unwrap()]);
        let principal = AuthenticatedPrincipal::new(
            PrincipalId::new("loadtest-catalog").unwrap(),
            PrincipalIssuer::new("arrowcol-loadgen").unwrap(),
            scopes,
        )
        .unwrap();
        AuthContext::new(
            principal,
            AuthMethod::WorkloadIdentity,
            UnixMillis::new(now_ms).unwrap(),
            None,
        )
        .unwrap()
    }

    fn signed_metadata(context: &AuthContext, key: &[u8], expiry: u64) -> MetadataMap {
        let encoded_context = URL_SAFE_NO_PAD.encode(serde_json::to_vec(context).unwrap());
        let canonical = format!("{encoded_context}\n{expiry}");
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(canonical.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        let mut metadata = MetadataMap::new();
        metadata.insert(INTERNAL_AUTH_CONTEXT_HEADER, encoded_context.parse().unwrap());
        metadata.insert(INTERNAL_AUTH_EXPIRES_HEADER, expiry.to_string().parse().unwrap());
        metadata.insert(INTERNAL_AUTH_SIGNATURE_HEADER, signature.parse().unwrap());
        metadata
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[test]
    fn verifies_the_existing_loadgen_signed_identity_contract() {
        let now = now_ms();
        let expected = identity(now);
        let metadata = signed_metadata(&expected, KEY, now + 30_000);
        let verifier = DdlIdentityVerifier::from_secret(KEY.to_vec()).unwrap();

        assert_eq!(verifier.verify(&metadata, now).unwrap(), expected);
    }

    #[test]
    fn rejects_tampered_expired_missing_and_duplicate_identity_metadata() {
        let now = now_ms();
        let expected = identity(now);
        let verifier = DdlIdentityVerifier::from_secret(KEY.to_vec()).unwrap();

        let mut tampered = signed_metadata(&expected, KEY, now + 30_000);
        tampered.insert(INTERNAL_AUTH_SIGNATURE_HEADER, "invalid".parse().unwrap());
        assert!(verifier.verify(&tampered, now).is_err());

        let expired = signed_metadata(&expected, KEY, now);
        assert!(verifier.verify(&expired, now).is_err());
        assert!(verifier.verify(&MetadataMap::new(), now).is_err());

        let mut duplicate = signed_metadata(&expected, KEY, now + 30_000);
        duplicate.append(INTERNAL_AUTH_CONTEXT_HEADER, "second".parse().unwrap());
        assert!(verifier.verify(&duplicate, now).is_err());
    }

    #[test]
    fn unsigned_ddl_is_allowed_only_by_the_explicit_development_policy() {
        let metadata = MetadataMap::new();
        assert!(DdlIdentityPolicy::dev_only_allow_unsigned().verify(&metadata, now_ms()).is_ok());
        assert!(DdlIdentityPolicy::reject().verify(&metadata, now_ms()).is_err());
    }
}
