//! Reusable, versioned contracts shared by Arrow collector implementations.
//!
//! This crate intentionally contains no SQL, Hive, Pingora, Delta engine, or
//! dataflow runtime integration. It defines validated wire and domain types
//! that those components can share without coupling their implementations.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod auth;
mod capability;
mod error;
mod fingerprint;
mod identity;
mod ids;
mod lease;
mod lifecycle;
mod measurement;
mod metadata;
mod pressure;
mod protocol;

pub use auth::{
    AuthContext, AuthContextError, AuthMethod, AuthenticatedPrincipal, PrincipalError, PrincipalId, PrincipalIssuer,
    PrincipalScope,
};
pub use capability::{ARROW_SCHEMA_CONTRACT_VERSION, Capability, CapabilityError, CollectorCapabilities};
pub use error::{ErrorCategory, ErrorCode, ErrorStatus, ErrorStatusError};
pub use fingerprint::{
    ClaimedSchemaFingerprint, FingerprintParseError, SCHEMA_FINGERPRINT_ALGORITHM, SchemaFingerprint,
    fingerprint_schema,
};
pub use identity::{
    CatalogName, IdentityError, ImplementationPool, MAX_IDENTITY_COMPONENT_BYTES, PoolParseError, SchemaName,
    TableIdentity, TableName,
};
pub use ids::{CollectorInstanceId, IdParseError, LeaseId};
pub use lease::{FencingToken, LeaseError, PublicationFence, TableLease, TableLeaseSuccessorError};
pub use lifecycle::{CollectorInstanceStatus, InstanceLifecycleState, InstanceStatusError, InstanceTransitionError};
pub use measurement::{
    BasisPoints, MAX_MEASUREMENT, Measurement, MeasurementError, PositiveMeasurement, PressureGauge, UnixMillis,
};
pub use metadata::{
    CATALOG_METADATA_KEY, CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY, IMPLEMENTATION_POOL_METADATA_KEY, IngestMetadata,
    MAX_CLAIMED_SCHEMA_FINGERPRINT_METADATA_VALUE_BYTES, MAX_GRPC_METADATA_VALUE_BYTES,
    MAX_IDENTITY_METADATA_VALUE_BYTES, MAX_IMPLEMENTATION_POOL_METADATA_VALUE_BYTES,
    MAX_PROTOCOL_VERSION_METADATA_VALUE_BYTES, MetadataParseError, PROTOCOL_VERSION_METADATA_KEY, RawGrpcMetadata,
    SCHEMA_METADATA_KEY, TABLE_METADATA_KEY,
};
pub use pressure::PressureSnapshot;
pub use protocol::{ProtocolVersion, ProtocolVersionError};
