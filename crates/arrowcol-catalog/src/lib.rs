//! Catalog, schema compatibility, and Flight SQL DDL control-plane services.
//!
//! The crate owns the authoritative immutable schema history used by Arrow
//! collectors. It parses an explicit, lossless Spark SQL DDL
//! surface, maintains recoverable schema-change sagas, and validates incoming
//! Arrow schemas exactly. The runtime catalog service owns persistence.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod artifacts;
mod ddl;
mod error;
mod ir;
mod memory;
mod saga;
mod store;
mod validation;

pub use arrowcol_sparksql::{Statement as SparkStatement, TableMetadata};
pub use artifacts::{ArtifactKind, DescriptorArtifacts, generate_descriptor_artifacts};
pub use ddl::{DdlCommand, DdlParser, FieldBlueprint, ParsedAddColumns, ParsedCreateTable};
pub use error::{CatalogError, CatalogErrorCode, CatalogErrorOrigin, CatalogResult};
pub use ir::{
    CatalogField, CatalogSchemaVersion, CatalogType, FieldPath, ListKind, PartitionMetadata, ProtobufType,
    SchemaVersion, TimeUnit, TypeMapping, deterministic_field_id, deterministic_table_id, next_protobuf_field_number,
    protobuf_field_number,
};
pub use memory::{FailurePoint, MemoryCatalogStore};
pub use saga::{
    CatalogControlPlane, DdlCompletion, DdlCompletionDisposition, FlightSqlDdlHandler, FlightSqlDdlRequest,
    ReconciliationReport,
};
pub use store::{
    CatalogStore, ChangeReservation, RequestHash, ReserveOutcome, SagaId, SagaRecord, SagaState, SagaStep,
};
pub use validation::{AlignmentAction, SchemaAlignmentPlan, SchemaValidator, ValidatedIncomingSchema};
