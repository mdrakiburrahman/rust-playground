//! Shared Arrow Flight and Flight SQL ingress runtime for Arrow collectors.
//!
//! The crate owns transport validation, authentication hooks, incremental Arrow
//! decoding, catalog schema negotiation, bounded backpressure, stable
//! `PutResult` metadata, Flight SQL DDL dispatch, and graceful drain behavior.
//! Storage, receipt, buffer, and dataflow implementations plug in through the
//! traits re-exported from this crate.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod admin;
mod auth;
mod error;
mod flight_sql;
mod ingest;
mod lifecycle;
mod put_result;
mod service;

pub use admin::{
    AdminHandlers, AdminHttpResponse, CAPABILITIES_PATH, CapabilityReporter, HEALTH_PATH, HealthReporter, HealthStatus,
    PRESSURE_PATH, PressureReporter, READINESS_PATH, ReadinessReporter, ReadinessStatus,
};
pub use auth::{
    AllowAllAuthenticator, AuthenticationRequest, Authenticator, INTERNAL_IDENTITY_METADATA_KEY,
    RejectUntrustedIdentityClaims, TrustedIdentityValidator,
};
pub use error::{
    ERROR_CATEGORY_METADATA_KEY, ERROR_CODE_METADATA_KEY, ERROR_RETRY_AFTER_METADATA_KEY, ERROR_RETRYABLE_METADATA_KEY,
    ERROR_STATUS_METADATA_KEY, GrpcStatusMetadata, RuntimeError, RuntimeResult, decode_status_metadata,
};
pub use flight_sql::{
    DDL_COMPLETION_METADATA_KEY, DdlCompletionMetadata, FlightSqlDdlAdapter, FlightSqlDdlResponse,
    decode_ddl_completion_metadata, encode_ddl_completion_response,
};
pub use ingest::{RuntimeLimits, align_record_batch};
pub use lifecycle::{
    BatchAdmission, DrainController, DrainOutcome, NoopPressureHooks, PressureEvent, PressureHooks, PutResultEmitter,
    ShutdownCoordinator, StreamAbortReason, StreamContext, StreamLifecycle, StreamOutcome, StreamSummary,
};
pub use put_result::{CompletionMode, DeliveryGuarantee, PUT_RESULT_WIRE_VERSION, PutResultMetadata, StreamCompletion};
pub use service::{ArrowColFlightService, FlightResponseStream};
