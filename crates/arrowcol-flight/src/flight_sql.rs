use std::sync::Arc;

use arrow_flight::{PutResult, sql::DoPutUpdateResult};
use arrowcol_catalog::{DdlCompletion, FlightSqlDdlHandler, FlightSqlDdlRequest, SchemaVersion};
use arrowcol_protocol::{ProtocolVersion, SchemaFingerprint};
use prost::Message;
use serde::{Deserialize, Serialize};
use tonic::metadata::{Binary, MetadataMap, MetadataValue};

use crate::{RuntimeError, RuntimeLimits, RuntimeResult};

/// Binary response metadata key carrying structured DDL completion JSON.
pub const DDL_COMPLETION_METADATA_KEY: &str = "x-arrowcol-ddl-completion-bin";

/// Stable Flight SQL DDL completion metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DdlCompletionMetadata {
    protocol_version: ProtocolVersion,
    catalog: String,
    schema: String,
    table: String,
    schema_version: SchemaVersion,
    schema_fingerprint: SchemaFingerprint,
    saga_id: Option<String>,
    disposition: String,
}

impl DdlCompletionMetadata {
    fn from_completion(completion: &DdlCompletion) -> Self {
        Self {
            protocol_version: ProtocolVersion::CURRENT,
            catalog: completion.identity().catalog().to_string(),
            schema: completion.identity().schema().to_string(),
            table: completion.identity().table().to_string(),
            schema_version: completion.schema_version(),
            schema_fingerprint: completion.fingerprint(),
            saga_id: completion.saga_id().map(|value| value.to_string()),
            disposition: completion.disposition().as_str().to_owned(),
        }
    }

    /// Returns the affected catalog.
    #[must_use]
    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    /// Returns the affected schema.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the affected table.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns the committed immutable schema version.
    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns the committed canonical Arrow fingerprint.
    #[must_use]
    pub const fn schema_fingerprint(&self) -> SchemaFingerprint {
        self.schema_fingerprint
    }

    /// Returns the optional durable schema saga identifier.
    #[must_use]
    pub fn saga_id(&self) -> Option<&str> {
        self.saga_id.as_deref()
    }

    /// Returns `created`, `altered`, or `already_applied`.
    #[must_use]
    pub fn disposition(&self) -> &str {
        &self.disposition
    }
}

/// Flight SQL DDL result containing standard and Arrowcol metadata.
#[derive(Clone, Debug)]
pub struct FlightSqlDdlResponse {
    put_result: PutResult,
    response_metadata: MetadataMap,
    completion: DdlCompletionMetadata,
}

impl FlightSqlDdlResponse {
    /// Returns the standard Flight SQL update `PutResult`.
    #[must_use]
    pub const fn put_result(&self) -> &PutResult {
        &self.put_result
    }

    /// Returns response metadata with schema version and fingerprint fields.
    #[must_use]
    pub const fn response_metadata(&self) -> &MetadataMap {
        &self.response_metadata
    }

    /// Returns the structured completion.
    #[must_use]
    pub const fn completion(&self) -> &DdlCompletionMetadata {
        &self.completion
    }

    /// Consumes the response into transport parts.
    #[must_use]
    pub fn into_parts(self) -> (PutResult, MetadataMap, DdlCompletionMetadata) {
        (self.put_result, self.response_metadata, self.completion)
    }
}

/// Reusable adapter from Flight SQL statement updates to catalog DDL.
#[derive(Clone)]
pub struct FlightSqlDdlAdapter {
    handler: Arc<dyn FlightSqlDdlHandler>,
    max_command_bytes: usize,
}

impl FlightSqlDdlAdapter {
    /// Creates an adapter with the runtime DDL size bound.
    #[must_use]
    pub fn new(handler: Arc<dyn FlightSqlDdlHandler>, limits: &RuntimeLimits) -> Self {
        Self {
            handler,
            max_command_bytes: limits.max_ddl_bytes,
        }
    }

    /// Executes one strict DDL statement through the catalog handler.
    ///
    /// The returned `PutResult` remains Flight SQL compatible by encoding
    /// `DoPutUpdateResult { record_count: 0 }`. Arrowcol schema-version,
    /// fingerprint, saga, and disposition metadata are attached to the gRPC
    /// response metadata.
    ///
    /// # Errors
    ///
    /// Returns a bounded command, catalog validation, or internal encoding
    /// failure.
    pub async fn execute(&self, command: &str) -> RuntimeResult<FlightSqlDdlResponse> {
        if command.is_empty() || command.len() > self.max_command_bytes {
            return Err(RuntimeError::new(
                arrowcol_protocol::ErrorCode::SchemaIncompatible,
                tonic::Code::InvalidArgument,
                "Flight SQL DDL command is empty or exceeds its size limit",
            ));
        }
        let completion = self
            .handler
            .execute_ddl(FlightSqlDdlRequest::new(command))
            .await
            .map_err(|error| RuntimeError::from_catalog(&error))?;
        encode_ddl_completion_response(&completion)
    }
}

/// Decodes structured DDL completion metadata from a tonic response map.
///
/// # Errors
///
/// Returns an invalid-argument error when the metadata is absent or malformed.
pub fn decode_ddl_completion_metadata(metadata: &MetadataMap) -> RuntimeResult<DdlCompletionMetadata> {
    let encoded = metadata
        .get_bin(DDL_COMPLETION_METADATA_KEY)
        .ok_or_else(|| {
            RuntimeError::new(
                arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
                tonic::Code::InvalidArgument,
                "Flight SQL DDL completion metadata is missing",
            )
        })?
        .to_bytes()
        .map_err(|_| {
            RuntimeError::new(
                arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
                tonic::Code::InvalidArgument,
                "Flight SQL DDL completion metadata is malformed",
            )
        })?;
    serde_json::from_slice(&encoded).map_err(|_| {
        RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            tonic::Code::InvalidArgument,
            "Flight SQL DDL completion metadata is malformed",
        )
    })
}

/// Encodes a committed DDL result with standard Flight SQL and Arrowcol response metadata.
///
/// # Errors
///
/// Returns an internal encoding error if the committed completion cannot be represented.
pub fn encode_ddl_completion_response(completion: &DdlCompletion) -> RuntimeResult<FlightSqlDdlResponse> {
    let structured = DdlCompletionMetadata::from_completion(completion);
    let encoded = serde_json::to_vec(&structured).map_err(|_| RuntimeError::internal())?;
    let mut response_metadata = MetadataMap::new();
    response_metadata.insert_bin(
        DDL_COMPLETION_METADATA_KEY,
        MetadataValue::<Binary>::from_bytes(&encoded),
    );
    for (key, value) in completion.metadata_pairs() {
        let value = MetadataValue::try_from(value.as_str()).map_err(|_| RuntimeError::internal())?;
        response_metadata.insert(key, value);
    }
    let put_result = PutResult {
        app_metadata: DoPutUpdateResult { record_count: 0 }.encode_to_vec().into(),
    };
    Ok(FlightSqlDdlResponse {
        put_result,
        response_metadata,
        completion: structured,
    })
}
