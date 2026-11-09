use std::{fmt, time::Duration};

use arrowcol_catalog::{CatalogError, CatalogErrorCode};
use arrowcol_protocol::{
    ErrorCategory, ErrorCode, ErrorStatus, MAX_MEASUREMENT, MetadataParseError, PositiveMeasurement,
};
use serde::{Deserialize, Serialize};
use tonic::{
    Code, Status,
    metadata::{Binary, MetadataMap, MetadataValue},
};

/// gRPC metadata key carrying the stable Arrowcol error code.
pub const ERROR_CODE_METADATA_KEY: &str = "x-arrowcol-error-code";
/// gRPC metadata key carrying the stable Arrowcol error category.
pub const ERROR_CATEGORY_METADATA_KEY: &str = "x-arrowcol-error-category";
/// gRPC metadata key carrying `true` when a new delivery may be retried.
pub const ERROR_RETRYABLE_METADATA_KEY: &str = "x-arrowcol-error-retryable";
/// gRPC metadata key carrying a suggested retry delay in milliseconds.
pub const ERROR_RETRY_AFTER_METADATA_KEY: &str = "x-arrowcol-retry-after-ms";
/// Binary gRPC metadata key carrying the versioned JSON [`ErrorStatus`].
pub const ERROR_STATUS_METADATA_KEY: &str = "x-arrowcol-error-status-bin";

const MAX_PUBLIC_MESSAGE_BYTES: usize = 1_024;

/// Result type returned by the reusable Flight runtime.
pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// A safe public failure with stable Arrowcol and gRPC mappings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeError {
    code: ErrorCode,
    grpc_code: Code,
    public_message: String,
    retry_after: Option<Duration>,
}

impl RuntimeError {
    /// Creates an error from an Arrowcol code, gRPC code, and public message.
    ///
    /// Control characters are removed and the message is bounded before it can
    /// cross a transport boundary.
    #[must_use]
    pub fn new(code: ErrorCode, grpc_code: Code, public_message: impl AsRef<str>) -> Self {
        Self {
            code,
            grpc_code,
            public_message: sanitize_public_message(public_message.as_ref()),
            retry_after: None,
        }
    }

    /// Creates a generic internal failure without exposing implementation detail.
    #[must_use]
    pub fn internal() -> Self {
        Self::new(ErrorCode::Internal, Code::Internal, "collector operation failed")
    }

    /// Creates a client-cancellation failure.
    #[must_use]
    pub fn cancelled() -> Self {
        Self::new(ErrorCode::CapacityExhausted, Code::Cancelled, "request was cancelled")
    }

    /// Creates a request-deadline failure.
    #[must_use]
    pub fn deadline_exceeded() -> Self {
        Self::new(
            ErrorCode::BackpressureTimeout,
            Code::DeadlineExceeded,
            "request deadline exceeded",
        )
    }

    /// Creates a graceful-drain admission failure.
    #[must_use]
    pub fn draining() -> Self {
        Self::new(
            ErrorCode::PoolUnavailable,
            Code::Unavailable,
            "collector is draining and not accepting new deliveries",
        )
    }

    /// Maps validated metadata failures without reflecting untrusted values.
    #[must_use]
    pub fn from_metadata(error: &MetadataParseError) -> Self {
        let code = if matches!(error, MetadataParseError::UnsupportedVersion { .. }) {
            ErrorCode::ProtocolVersionUnsupported
        } else {
            ErrorCode::RoutingMetadataInvalid
        };
        Self::new(code, Code::InvalidArgument, "invalid collector routing metadata")
    }

    /// Maps catalog validation and DDL failures onto the stable public surface.
    #[must_use]
    pub fn from_catalog(error: &CatalogError) -> Self {
        let grpc_code = match error.code() {
            CatalogErrorCode::TableNotFound => Code::NotFound,
            CatalogErrorCode::SchemaChangeInProgress | CatalogErrorCode::SchemaVersionConflict => Code::Aborted,
            CatalogErrorCode::UnknownSchemaFingerprint
            | CatalogErrorCode::SchemaIncompatible
            | CatalogErrorCode::DdlParse
            | CatalogErrorCode::DdlUnsupportedStatement
            | CatalogErrorCode::DdlDestructiveStatement
            | CatalogErrorCode::DdlBreakingMutation
            | CatalogErrorCode::DdlPartitionMutation
            | CatalogErrorCode::DdlFieldReorder
            | CatalogErrorCode::DdlNonNullableAppend
            | CatalogErrorCode::TypeMappingUnsupported
            | CatalogErrorCode::InvalidIdentity
            | CatalogErrorCode::TableAlreadyExists => Code::InvalidArgument,
            CatalogErrorCode::CorruptCatalogState
            | CatalogErrorCode::ApplicationDatabase
            | CatalogErrorCode::DescriptorGeneration
            | CatalogErrorCode::ReconciliationFailed => Code::Internal,
        };
        let public_message = match error.code() {
            CatalogErrorCode::TableNotFound => "catalog table was not found",
            CatalogErrorCode::UnknownSchemaFingerprint => "received Arrow schema is not registered",
            CatalogErrorCode::SchemaIncompatible => "received Arrow schema is incompatible with the current table",
            CatalogErrorCode::SchemaChangeInProgress | CatalogErrorCode::SchemaVersionConflict => {
                "catalog schema changed concurrently"
            }
            CatalogErrorCode::DdlParse
            | CatalogErrorCode::DdlUnsupportedStatement
            | CatalogErrorCode::DdlDestructiveStatement
            | CatalogErrorCode::DdlBreakingMutation
            | CatalogErrorCode::DdlPartitionMutation
            | CatalogErrorCode::DdlFieldReorder
            | CatalogErrorCode::DdlNonNullableAppend
            | CatalogErrorCode::TypeMappingUnsupported
            | CatalogErrorCode::InvalidIdentity
            | CatalogErrorCode::TableAlreadyExists => "Flight SQL DDL request is not allowed",
            CatalogErrorCode::CorruptCatalogState
            | CatalogErrorCode::ApplicationDatabase
            | CatalogErrorCode::DescriptorGeneration
            | CatalogErrorCode::ReconciliationFailed => "catalog operation failed",
        };
        Self::new(error.protocol_code(), grpc_code, public_message)
    }

    /// Adds a suggested retry delay.
    #[must_use]
    pub const fn with_retry_after(mut self, retry_after: Duration) -> Self {
        if self.code.is_retryable() && !retry_after.is_zero() {
            self.retry_after = Some(retry_after);
        }
        self
    }

    /// Returns the stable protocol code.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns the mapped gRPC code.
    #[must_use]
    pub const fn grpc_code(&self) -> Code {
        self.grpc_code
    }

    /// Returns the bounded public message.
    #[must_use]
    pub fn public_message(&self) -> &str {
        &self.public_message
    }

    /// Returns the optional retry delay.
    #[must_use]
    pub const fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// Converts this error into a tonic status with structured metadata.
    #[must_use]
    pub fn into_status(self) -> Status {
        let mut metadata = MetadataMap::new();
        insert_ascii(&mut metadata, ERROR_CODE_METADATA_KEY, error_code_name(self.code));
        insert_ascii(
            &mut metadata,
            ERROR_CATEGORY_METADATA_KEY,
            error_category_name(self.code.category()),
        );
        insert_ascii(
            &mut metadata,
            ERROR_RETRYABLE_METADATA_KEY,
            if self.code.is_retryable() { "true" } else { "false" },
        );
        if let Some(delay) = self.retry_after {
            insert_ascii(
                &mut metadata,
                ERROR_RETRY_AFTER_METADATA_KEY,
                &retry_after_millis(delay).to_string(),
            );
        }
        if let Some(status) = self.protocol_status()
            && let Ok(encoded) = serde_json::to_vec(&status)
        {
            metadata.insert_bin(ERROR_STATUS_METADATA_KEY, MetadataValue::<Binary>::from_bytes(&encoded));
        }
        Status::with_metadata(self.grpc_code, self.public_message, metadata)
    }

    fn protocol_status(&self) -> Option<ErrorStatus> {
        let mut status = ErrorStatus::new(self.code, self.public_message.clone()).ok()?;
        if let Some(delay) = self.retry_after {
            let positive = PositiveMeasurement::new(retry_after_millis(delay)).ok()?;
            status = status.with_retry_after(positive).ok()?;
        }
        Some(status)
    }
}

fn retry_after_millis(delay: Duration) -> u64 {
    u64::try_from(delay.as_millis())
        .unwrap_or(MAX_MEASUREMENT)
        .clamp(1, MAX_MEASUREMENT)
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.public_message.fmt(formatter)
    }
}

impl std::error::Error for RuntimeError {}

/// Structured fields decoded from a tonic status produced by this crate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GrpcStatusMetadata {
    /// Stable Arrowcol error code text.
    pub code: Option<String>,
    /// Stable Arrowcol category text.
    pub category: Option<String>,
    /// Whether a new delivery may be retried.
    pub retryable: Option<bool>,
    /// Suggested retry delay in milliseconds.
    pub retry_after_ms: Option<u64>,
    /// Full versioned protocol payload, when present.
    pub status: Option<ErrorStatus>,
}

/// Decodes Arrowcol structured metadata from a tonic status.
#[must_use]
pub fn decode_status_metadata(status: &Status) -> GrpcStatusMetadata {
    let metadata = status.metadata();
    GrpcStatusMetadata {
        code: metadata_text(metadata, ERROR_CODE_METADATA_KEY),
        category: metadata_text(metadata, ERROR_CATEGORY_METADATA_KEY),
        retryable: metadata_text(metadata, ERROR_RETRYABLE_METADATA_KEY).and_then(|value| value.parse::<bool>().ok()),
        retry_after_ms: metadata_text(metadata, ERROR_RETRY_AFTER_METADATA_KEY)
            .and_then(|value| value.parse::<u64>().ok()),
        status: metadata
            .get_bin(ERROR_STATUS_METADATA_KEY)
            .and_then(|value| value.to_bytes().ok())
            .and_then(|value| serde_json::from_slice(value.as_ref()).ok()),
    }
}

fn metadata_text(metadata: &MetadataMap, key: &'static str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn insert_ascii(metadata: &mut MetadataMap, key: &'static str, value: &str) {
    if let Ok(value) = MetadataValue::try_from(value) {
        metadata.insert(key, value);
    }
}

fn sanitize_public_message(message: &str) -> String {
    let mut output = String::with_capacity(message.len().min(MAX_PUBLIC_MESSAGE_BYTES));
    for character in message.chars() {
        if output.len() >= MAX_PUBLIC_MESSAGE_BYTES {
            break;
        }
        if !character.is_control() {
            output.push(character);
        }
    }
    if output.is_empty() {
        "collector operation failed".to_owned()
    } else {
        output
    }
}

const fn error_category_name(category: ErrorCategory) -> &'static str {
    match category {
        ErrorCategory::Authentication => "authentication",
        ErrorCategory::Routing => "routing",
        ErrorCategory::CatalogSchema => "catalog_schema",
        ErrorCategory::CapacityBackpressure => "capacity_backpressure",
        ErrorCategory::Publication => "publication",
        ErrorCategory::Internal => "internal",
    }
}

const fn error_code_name(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::AuthenticationMissing => "authentication_missing",
        ErrorCode::AuthenticationInvalid => "authentication_invalid",
        ErrorCode::AuthenticationForbidden => "authentication_forbidden",
        ErrorCode::RoutingMetadataInvalid => "routing_metadata_invalid",
        ErrorCode::ProtocolVersionUnsupported => "protocol_version_unsupported",
        ErrorCode::PoolUnavailable => "pool_unavailable",
        ErrorCode::LeaseFenced => "lease_fenced",
        ErrorCode::CatalogNotFound => "catalog_not_found",
        ErrorCode::SchemaNotFound => "schema_not_found",
        ErrorCode::TableNotFound => "table_not_found",
        ErrorCode::SchemaFingerprintMismatch => "schema_fingerprint_mismatch",
        ErrorCode::SchemaIncompatible => "schema_incompatible",
        ErrorCode::CapacityExhausted => "capacity_exhausted",
        ErrorCode::BackpressureTimeout => "backpressure_timeout",
        ErrorCode::PublicationConflict => "publication_conflict",
        ErrorCode::PublicationFailed => "publication_failed",
        ErrorCode::PublicationOutcomeUnknown => "publication_outcome_unknown",
        ErrorCode::Internal => "internal",
    }
}
