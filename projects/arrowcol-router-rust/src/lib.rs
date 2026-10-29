//! Production Arrowcol routing application components.

#![forbid(unsafe_code)]

mod auth;
mod config;
pub mod delta;
mod metrics;
mod router;

pub use auth::{
    AllowAllAuthenticator, AuthFailure, AuthFailureKind, Authenticator, EDGE_NONCE_HEADER, EDGE_SIGNATURE_HEADER,
    EDGE_TIMESTAMP_HEADER, EdgeRequestSigner, EdgeVerifier, ExternalRequest, INTERNAL_AUTH_CONTEXT_HEADER,
    INTERNAL_AUTH_EXPIRES_HEADER, INTERNAL_AUTH_SIGNATURE_HEADER, InternalIdentitySigner, SignedInternalIdentity,
    insert_edge_signature_headers,
};
pub use config::{
    AuthConfig, Cli, H2Config, ReceiptApiConfig, RouterConfig, RoutingConfig, ServerConfig, SqlServerConfig,
    TelemetryConfig, UpstreamConfig,
};
pub use metrics::RouterMetrics;
pub use router::{
    CONTROL_OPERATION_METADATA_KEY, ProductionRouter, ROUTING_FENCING_EPOCH_HEADER, ROUTING_LEASE_EXPIRES_HEADER,
    ROUTING_LEASE_ID_HEADER, ROUTING_LEASE_VERSION_HEADER, RouterControlLoop, build_server,
};
