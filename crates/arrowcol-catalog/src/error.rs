use std::fmt;

use arrowcol_protocol::{ErrorCode, ErrorStatus, ErrorStatusError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Result type returned by catalog operations.
pub type CatalogResult<T> = Result<T, CatalogError>;

/// Stable, catalog-specific failure code.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogErrorCode {
    /// SQL text could not be parsed as one complete statement.
    DdlParse,
    /// The statement kind is outside the catalog DDL surface.
    DdlUnsupportedStatement,
    /// A destructive statement was attempted.
    DdlDestructiveStatement,
    /// An existing field, table, or schema property would be changed.
    DdlBreakingMutation,
    /// An operation attempted to change partition metadata.
    DdlPartitionMutation,
    /// An appended field attempted to choose a non-tail position.
    DdlFieldReorder,
    /// An appended field was not nullable.
    DdlNonNullableAppend,
    /// A SQL, Arrow, or protobuf type mapping was unsupported or lossy.
    TypeMappingUnsupported,
    /// A name or identity was invalid.
    InvalidIdentity,
    /// The table already exists with a different schema.
    TableAlreadyExists,
    /// The requested table does not exist.
    TableNotFound,
    /// Another non-equivalent schema change is active.
    SchemaChangeInProgress,
    /// The requested base schema changed before reservation.
    SchemaVersionConflict,
    /// A claimed or computed fingerprint was unknown.
    UnknownSchemaFingerprint,
    /// A registered schema is not append-only compatible with the current head.
    SchemaIncompatible,
    /// Persisted catalog state violated an invariant.
    CorruptCatalogState,
    /// The application database operation failed.
    ApplicationDatabase,
    /// Deterministic descriptor generation failed.
    DescriptorGeneration,
    /// Saga reconciliation could not complete.
    ReconciliationFailed,
}

impl CatalogErrorCode {
    /// Maps the detailed catalog code onto the stable public protocol surface.
    #[must_use]
    pub const fn protocol_code(self) -> ErrorCode {
        match self {
            Self::TableNotFound => ErrorCode::TableNotFound,
            Self::UnknownSchemaFingerprint => ErrorCode::SchemaFingerprintMismatch,
            Self::DdlParse
            | Self::DdlUnsupportedStatement
            | Self::DdlDestructiveStatement
            | Self::DdlBreakingMutation
            | Self::DdlPartitionMutation
            | Self::DdlFieldReorder
            | Self::DdlNonNullableAppend
            | Self::TypeMappingUnsupported
            | Self::InvalidIdentity
            | Self::TableAlreadyExists
            | Self::SchemaVersionConflict
            | Self::SchemaIncompatible => ErrorCode::SchemaIncompatible,
            Self::SchemaChangeInProgress
            | Self::CorruptCatalogState
            | Self::ApplicationDatabase
            | Self::DescriptorGeneration
            | Self::ReconciliationFailed => ErrorCode::Internal,
        }
    }

    /// Stable SQL storage value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DdlParse => "ddl_parse",
            Self::DdlUnsupportedStatement => "ddl_unsupported_statement",
            Self::DdlDestructiveStatement => "ddl_destructive_statement",
            Self::DdlBreakingMutation => "ddl_breaking_mutation",
            Self::DdlPartitionMutation => "ddl_partition_mutation",
            Self::DdlFieldReorder => "ddl_field_reorder",
            Self::DdlNonNullableAppend => "ddl_non_nullable_append",
            Self::TypeMappingUnsupported => "type_mapping_unsupported",
            Self::InvalidIdentity => "invalid_identity",
            Self::TableAlreadyExists => "table_already_exists",
            Self::TableNotFound => "table_not_found",
            Self::SchemaChangeInProgress => "schema_change_in_progress",
            Self::SchemaVersionConflict => "schema_version_conflict",
            Self::UnknownSchemaFingerprint => "unknown_schema_fingerprint",
            Self::SchemaIncompatible => "schema_incompatible",
            Self::CorruptCatalogState => "corrupt_catalog_state",
            Self::ApplicationDatabase => "application_database",
            Self::DescriptorGeneration => "descriptor_generation",
            Self::ReconciliationFailed => "reconciliation_failed",
        }
    }

    /// Parses a SQL storage value.
    ///
    /// # Errors
    ///
    /// Returns a corrupt-state error for an unknown value.
    pub fn parse(value: &str) -> CatalogResult<Self> {
        match value {
            "ddl_parse" => Ok(Self::DdlParse),
            "ddl_unsupported_statement" => Ok(Self::DdlUnsupportedStatement),
            "ddl_destructive_statement" => Ok(Self::DdlDestructiveStatement),
            "ddl_breaking_mutation" => Ok(Self::DdlBreakingMutation),
            "ddl_partition_mutation" => Ok(Self::DdlPartitionMutation),
            "ddl_field_reorder" => Ok(Self::DdlFieldReorder),
            "ddl_non_nullable_append" => Ok(Self::DdlNonNullableAppend),
            "type_mapping_unsupported" => Ok(Self::TypeMappingUnsupported),
            "invalid_identity" => Ok(Self::InvalidIdentity),
            "table_already_exists" => Ok(Self::TableAlreadyExists),
            "table_not_found" => Ok(Self::TableNotFound),
            "schema_change_in_progress" => Ok(Self::SchemaChangeInProgress),
            "schema_version_conflict" => Ok(Self::SchemaVersionConflict),
            "unknown_schema_fingerprint" => Ok(Self::UnknownSchemaFingerprint),
            "schema_incompatible" => Ok(Self::SchemaIncompatible),
            "corrupt_catalog_state" => Ok(Self::CorruptCatalogState),
            "application_database" => Ok(Self::ApplicationDatabase),
            "descriptor_generation" => Ok(Self::DescriptorGeneration),
            "reconciliation_failed" => Ok(Self::ReconciliationFailed),
            _ => Err(CatalogError::new(
                Self::CorruptCatalogState,
                format!("unknown persisted catalog error code {value:?}"),
            )),
        }
    }
}

/// Origin of a catalog failure, for safe internal diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogErrorOrigin {
    /// Failure constructed by the local catalog service.
    LocalCatalog,
    /// Failure detected by the remote catalog client before receiving a gRPC status.
    RemoteClient,
    /// Failure mapped from a remote catalog gRPC status.
    RemoteGrpc,
}

impl CatalogErrorOrigin {
    /// Stable diagnostic value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalCatalog => "local_catalog",
            Self::RemoteClient => "remote_client",
            Self::RemoteGrpc => "remote_grpc",
        }
    }
}

/// Structured catalog failure with a stable protocol mapping.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{code}: {message}")]
pub struct CatalogError {
    code: CatalogErrorCode,
    message: String,
    origin: CatalogErrorOrigin,
    operation: Option<&'static str>,
    grpc_status: Option<&'static str>,
    failure_id: Option<&'static str>,
}

impl CatalogError {
    /// Creates a catalog error.
    #[must_use]
    pub fn new(code: CatalogErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            origin: CatalogErrorOrigin::LocalCatalog,
            operation: None,
            grpc_status: None,
            failure_id: None,
        }
    }

    /// Sets the safe internal origin for this failure.
    #[must_use]
    pub const fn with_origin(mut self, origin: CatalogErrorOrigin) -> Self {
        self.origin = origin;
        self
    }

    /// Adds safe, static diagnostic context.
    #[must_use]
    pub const fn with_diagnostic_context(
        mut self,
        operation: &'static str,
        grpc_status: Option<&'static str>,
        failure_id: Option<&'static str>,
    ) -> Self {
        self.operation = Some(operation);
        self.grpc_status = grpc_status;
        self.failure_id = failure_id;
        self
    }

    /// Returns the stable detailed catalog code.
    #[must_use]
    pub const fn code(&self) -> CatalogErrorCode {
        self.code
    }

    /// Returns the failure origin.
    #[must_use]
    pub const fn origin(&self) -> CatalogErrorOrigin {
        self.origin
    }

    /// Returns the static operation associated with this failure.
    #[must_use]
    pub const fn operation(&self) -> Option<&'static str> {
        self.operation
    }

    /// Returns the static gRPC status name, when the failure came from gRPC.
    #[must_use]
    pub const fn grpc_status(&self) -> Option<&'static str> {
        self.grpc_status
    }

    /// Returns the static local failure identifier, when available.
    #[must_use]
    pub const fn failure_id(&self) -> Option<&'static str> {
        self.failure_id
    }

    /// Returns the safe human-readable explanation.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the public protocol code for this failure.
    #[must_use]
    pub const fn protocol_code(&self) -> ErrorCode {
        self.code.protocol_code()
    }

    /// Builds the versioned public protocol payload.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorStatusError`] only if the catalog error message cannot be
    /// represented by the protocol's bounded public payload.
    pub fn try_protocol_status(&self) -> Result<ErrorStatus, ErrorStatusError> {
        ErrorStatus::new(self.protocol_code(), sanitize_public_message(&self.message))
    }
}

impl fmt::Display for CatalogErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

fn sanitize_public_message(message: &str) -> String {
    let mut output = String::with_capacity(message.len().min(1_024));
    for character in message.chars() {
        if output.len() >= 1_024 {
            break;
        }
        if !character.is_control() {
            output.push(character);
        }
    }
    if output.is_empty() {
        "catalog operation failed".to_owned()
    } else {
        output
    }
}
