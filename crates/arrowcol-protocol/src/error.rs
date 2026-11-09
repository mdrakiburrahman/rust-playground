use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{PositiveMeasurement, ProtocolVersion};

const MAX_ERROR_MESSAGE_BYTES: usize = 1_024;

/// Stable high-level category for a public collector failure.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// Authentication or authorization failed.
    Authentication,
    /// Request routing or protocol negotiation failed.
    Routing,
    /// Catalog lookup or Arrow schema validation failed.
    CatalogSchema,
    /// Capacity was exhausted or backpressure timed out.
    CapacityBackpressure,
    /// Delta publication failed or has an uncertain outcome.
    Publication,
    /// An unexpected internal failure occurred.
    Internal,
}

/// Stable machine-readable public collector error code.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Required authentication was not supplied.
    AuthenticationMissing,
    /// Authentication material was invalid.
    AuthenticationInvalid,
    /// The authenticated principal lacks permission.
    AuthenticationForbidden,
    /// Required routing metadata was invalid.
    RoutingMetadataInvalid,
    /// The requested protocol version is unsupported.
    ProtocolVersionUnsupported,
    /// No healthy instance is available in the requested pool.
    PoolUnavailable,
    /// The request used a stale or mismatched table lease.
    LeaseFenced,
    /// The catalog does not exist.
    CatalogNotFound,
    /// The schema does not exist.
    SchemaNotFound,
    /// The table does not exist.
    TableNotFound,
    /// The received schema does not match the authoritative fingerprint.
    SchemaFingerprintMismatch,
    /// The received schema is not compatible with the table contract.
    SchemaIncompatible,
    /// A bounded resource has no remaining capacity.
    CapacityExhausted,
    /// Waiting for capacity exceeded the configured deadline.
    BackpressureTimeout,
    /// Delta publication conflicted with another writer.
    PublicationConflict,
    /// Delta publication failed with a known non-commit outcome.
    PublicationFailed,
    /// Delta publication may have committed and requires reconciliation.
    PublicationOutcomeUnknown,
    /// An unexpected collector failure occurred.
    Internal,
}

impl ErrorCode {
    /// Returns this code's stable category.
    #[must_use]
    pub const fn category(self) -> ErrorCategory {
        match self {
            Self::AuthenticationMissing | Self::AuthenticationInvalid | Self::AuthenticationForbidden => {
                ErrorCategory::Authentication
            }
            Self::RoutingMetadataInvalid
            | Self::ProtocolVersionUnsupported
            | Self::PoolUnavailable
            | Self::LeaseFenced => ErrorCategory::Routing,
            Self::CatalogNotFound
            | Self::SchemaNotFound
            | Self::TableNotFound
            | Self::SchemaFingerprintMismatch
            | Self::SchemaIncompatible => ErrorCategory::CatalogSchema,
            Self::CapacityExhausted | Self::BackpressureTimeout => ErrorCategory::CapacityBackpressure,
            Self::PublicationConflict | Self::PublicationFailed | Self::PublicationOutcomeUnknown => {
                ErrorCategory::Publication
            }
            Self::Internal => ErrorCategory::Internal,
        }
    }

    /// Returns whether a later retry may be useful.
    ///
    /// This flag never promises idempotency or deduplication. Unknown
    /// publication outcomes are non-retryable until storage is reconciled.
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::PoolUnavailable
                | Self::LeaseFenced
                | Self::CapacityExhausted
                | Self::BackpressureTimeout
                | Self::PublicationConflict
                | Self::PublicationFailed
                | Self::Internal
        )
    }
}

/// Versioned public error payload returned by collector APIs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorStatus {
    protocol_version: ProtocolVersion,
    code: ErrorCode,
    message: String,
    retry_after_ms: Option<PositiveMeasurement>,
}

impl ErrorStatus {
    /// Creates a validated public error payload.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorStatusError`] for an empty, oversized, or control-bearing
    /// public message.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Result<Self, ErrorStatusError> {
        let message = message.into();
        validate_message(&message)?;
        Ok(Self {
            protocol_version: ProtocolVersion::CURRENT,
            code,
            message,
            retry_after_ms: None,
        })
    }

    /// Adds a retry delay to a retryable failure.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorStatusError::RetryDelayOnNonRetryable`] when the error
    /// code explicitly requires reconciliation or is otherwise non-retryable.
    pub fn with_retry_after(mut self, retry_after_ms: PositiveMeasurement) -> Result<Self, ErrorStatusError> {
        if !self.code.is_retryable() {
            return Err(ErrorStatusError::RetryDelayOnNonRetryable);
        }
        self.retry_after_ms = Some(retry_after_ms);
        Ok(self)
    }

    /// Returns the payload protocol version.
    #[must_use]
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Returns the machine-readable code.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns the stable error category.
    #[must_use]
    pub const fn category(&self) -> ErrorCategory {
        self.code.category()
    }

    /// Returns whether a later new delivery may be useful.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        self.code.is_retryable()
    }

    /// Returns the safe public message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns a suggested retry delay for a new delivery.
    #[must_use]
    pub const fn retry_after_ms(&self) -> Option<PositiveMeasurement> {
        self.retry_after_ms
    }
}

impl Serialize for ErrorStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            protocol_version: ProtocolVersion,
            code: ErrorCode,
            category: ErrorCategory,
            retryable: bool,
            message: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            retry_after_ms: Option<PositiveMeasurement>,
        }

        Wire {
            protocol_version: self.protocol_version,
            code: self.code,
            category: self.category(),
            retryable: self.is_retryable(),
            message: &self.message,
            retry_after_ms: self.retry_after_ms,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ErrorStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            protocol_version: ProtocolVersion,
            code: ErrorCode,
            category: ErrorCategory,
            retryable: bool,
            message: String,
            #[serde(default)]
            retry_after_ms: Option<PositiveMeasurement>,
        }

        let wire = Wire::deserialize(deserializer)?;
        if wire.category != wire.code.category() {
            return Err(de::Error::custom(ErrorStatusError::CategoryMismatch));
        }
        if wire.retryable != wire.code.is_retryable() {
            return Err(de::Error::custom(ErrorStatusError::RetryabilityMismatch));
        }
        let mut status = Self::new(wire.code, wire.message).map_err(de::Error::custom)?;
        status.protocol_version = wire.protocol_version;
        if let Some(delay) = wire.retry_after_ms {
            status = status.with_retry_after(delay).map_err(de::Error::custom)?;
        }
        Ok(status)
    }
}

/// Failure to validate a public error payload.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ErrorStatusError {
    /// The public message was empty.
    #[error("public error message must not be empty")]
    EmptyMessage,
    /// The public message exceeded its wire bound.
    #[error("public error message exceeds 1024 bytes")]
    MessageTooLong,
    /// The public message contained a control character.
    #[error("public error message contains a control character")]
    MessageContainsControl,
    /// The serialized category disagreed with the code.
    #[error("error category does not match error code")]
    CategoryMismatch,
    /// The serialized retryability flag disagreed with the code.
    #[error("error retryability does not match error code")]
    RetryabilityMismatch,
    /// A retry delay was supplied for a non-retryable code.
    #[error("retry delay is not valid for a non-retryable error")]
    RetryDelayOnNonRetryable,
}

fn validate_message(message: &str) -> Result<(), ErrorStatusError> {
    if message.is_empty() {
        return Err(ErrorStatusError::EmptyMessage);
    }
    if message.len() > MAX_ERROR_MESSAGE_BYTES {
        return Err(ErrorStatusError::MessageTooLong);
    }
    if message.chars().any(char::is_control) {
        return Err(ErrorStatusError::MessageContainsControl);
    }
    Ok(())
}
