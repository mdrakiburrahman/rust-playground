use arrowcol_protocol::UnixMillis;
use arrowcol_router_rust::{
    AllowAllAuthenticator, AuthFailureKind, Authenticator, EDGE_SIGNATURE_HEADER, EdgeRequestSigner, EdgeVerifier,
    ExternalRequest, INTERNAL_AUTH_CONTEXT_HEADER, InternalIdentitySigner, RouterConfig, insert_edge_signature_headers,
};
use http::{HeaderMap, HeaderValue, Method};

fn now(value: u64) -> UnixMillis {
    UnixMillis::new(value).unwrap_or_else(|error| panic!("invalid test timestamp: {error}"))
}

fn request_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/grpc"));
    headers.insert("x-arrowcol-protocol-version", HeaderValue::from_static("1"));
    headers.insert("x-arrowcol-implementation-pool", HeaderValue::from_static("dataflow"));
    headers.insert("x-arrowcol-catalog", HeaderValue::from_static("telemetry"));
    headers.insert("x-arrowcol-schema", HeaderValue::from_static("prod"));
    headers.insert("x-arrowcol-table", HeaderValue::from_static("traces"));
    headers.insert(
        "x-arrowcol-schema-fingerprint",
        HeaderValue::from_static("sha256:0000000000000000000000000000000000000000000000000000000000000000"),
    );
    headers
}

#[test]
fn trusted_edge_signature_rejects_direct_forged_tampered_and_replayed_requests() {
    let secret = vec![7_u8; 32];
    let signer = EdgeRequestSigner::new(secret.clone()).unwrap_or_else(|error| panic!("create edge signer: {error}"));
    let verifier = EdgeVerifier::new(secret, 30_000).unwrap_or_else(|error| panic!("create edge verifier: {error}"));
    let method = Method::POST;
    let path = "/arrow.flight.protocol.FlightService/DoPut";

    let Err(direct) = verifier.verify(&method, path, &request_headers(), now(100_000)) else {
        panic!("direct traffic without trusted edge proof must fail");
    };
    assert_eq!(direct.kind(), AuthFailureKind::Missing);

    let mut signed = request_headers();
    insert_edge_signature_headers(&mut signed, &signer, &method, path, now(100_000), "nonce-1")
        .unwrap_or_else(|error| panic!("sign edge request: {error}"));
    verifier
        .verify(&method, path, &signed, now(100_001))
        .unwrap_or_else(|error| panic!("valid edge request failed: {error}"));
    let Err(replay) = verifier.verify(&method, path, &signed, now(100_002)) else {
        panic!("nonce replay must fail");
    };
    assert_eq!(replay.kind(), AuthFailureKind::Invalid);

    let mut tampered = request_headers();
    insert_edge_signature_headers(&mut tampered, &signer, &method, path, now(100_010), "nonce-2")
        .unwrap_or_else(|error| panic!("sign edge request: {error}"));
    tampered.insert("x-arrowcol-table", HeaderValue::from_static("metrics"));
    let Err(tamper) = verifier.verify(&method, path, &tampered, now(100_011)) else {
        panic!("routing metadata tampering must fail");
    };
    assert_eq!(tamper.kind(), AuthFailureKind::Invalid);

    let mut forged = request_headers();
    insert_edge_signature_headers(&mut forged, &signer, &method, path, now(100_020), "nonce-3")
        .unwrap_or_else(|error| panic!("sign edge request: {error}"));
    forged.insert(INTERNAL_AUTH_CONTEXT_HEADER, HeaderValue::from_static("forged"));
    let Err(forgery) = verifier.verify(&method, path, &forged, now(100_021)) else {
        panic!("external internal-identity injection must fail");
    };
    assert_eq!(forgery.kind(), AuthFailureKind::ForgedInternalIdentity);

    let mut bad_signature = request_headers();
    insert_edge_signature_headers(&mut bad_signature, &signer, &method, path, now(100_030), "nonce-4")
        .unwrap_or_else(|error| panic!("sign edge request: {error}"));
    bad_signature.insert(EDGE_SIGNATURE_HEADER, HeaderValue::from_static("AAAA"));
    let Err(invalid) = verifier.verify(&method, path, &bad_signature, now(100_031)) else {
        panic!("forged edge HMAC must fail");
    };
    assert_eq!(invalid.kind(), AuthFailureKind::Invalid);
}

#[tokio::test]
async fn allow_all_identity_is_signed_with_a_bounded_expiry() {
    let authenticator = AllowAllAuthenticator;
    let context = authenticator
        .authenticate(
            &ExternalRequest {
                method: Method::GET,
                path: "/arrow.flight.protocol.FlightService/DoPut".to_owned(),
            },
            now(200_000),
        )
        .await
        .unwrap_or_else(|error| panic!("allow-all authentication failed: {error}"));
    let signer = InternalIdentitySigner::new(vec![9_u8; 32], 30_000)
        .unwrap_or_else(|error| panic!("create internal signer: {error}"));
    let signed = signer
        .sign(&context, now(200_000))
        .unwrap_or_else(|error| panic!("sign internal identity: {error}"));
    assert!(signed.encoded_context.len() < 3_072);
    assert_eq!(signed.expires_at_unix_ms.get(), 230_000);
    assert!(!signed.signature.is_empty());
}

#[test]
fn strict_yaml_rejects_unknown_fields_and_accepts_the_checked_in_config() {
    let valid = include_bytes!("../config.yaml");
    for config in [
        valid.as_slice(),
        include_bytes!("../config.docker.yaml").as_slice(),
        include_bytes!("../config.smoke.yaml").as_slice(),
        include_bytes!("../../loadtest/config/router.yaml").as_slice(),
    ] {
        RouterConfig::from_yaml(config).unwrap_or_else(|error| panic!("checked-in config is invalid: {error}"));
    }

    let mut retired =
        String::from_utf8(valid.to_vec()).unwrap_or_else(|error| panic!("checked-in config is not UTF-8: {error}"));
    retired.push_str("\nreceipts:\n  max_batch_size: 256\n");
    assert!(RouterConfig::from_yaml(retired.as_bytes()).is_err());

    let mut unknown =
        String::from_utf8(valid.to_vec()).unwrap_or_else(|error| panic!("checked-in config is not UTF-8: {error}"));
    unknown.push_str("\nunknown_router_option: true\n");
    assert!(RouterConfig::from_yaml(unknown.as_bytes()).is_err());
}

#[test]
fn startup_requires_explicit_development_authentication_mode() {
    let valid = std::str::from_utf8(include_bytes!("../config.yaml"))
        .unwrap_or_else(|error| panic!("checked-in config is not UTF-8: {error}"));
    for mode in ["allow_all", "production", ""] {
        let invalid = valid.replace("authenticator: dev_only_allow_all", &format!("authenticator: {mode}"));
        assert!(
            RouterConfig::from_yaml(invalid.as_bytes()).is_err(),
            "accepted mode {mode:?}"
        );
    }
    let missing = valid.replace("  authenticator: dev_only_allow_all\n", "");
    assert!(RouterConfig::from_yaml(missing.as_bytes()).is_err());
}
