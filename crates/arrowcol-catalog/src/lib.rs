//! Catalog, schema compatibility, and Flight SQL DDL control-plane services.
//!
//! The crate owns the authoritative immutable schema history used by Arrow
//! collectors. It parses a deliberately small DataFusion-compatible DDL
//! surface, applies schema changes through Hive Metastore's supported Thrift
//! API, persists recoverable application-database sagas, and validates incoming
//! Arrow schemas exactly.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod artifacts;
mod ddl;
mod error;
mod hive;
mod ir;
mod memory;
mod saga;
mod sql_server;
mod store;
mod validation;

pub use arrowcol_sparksql::{Statement as SparkStatement, TableMetadata};
pub use artifacts::{ArtifactKind, DescriptorArtifacts, generate_descriptor_artifacts};
pub use ddl::{DdlCommand, DdlParser, FieldBlueprint, ParsedAddColumns, ParsedCreateTable};
pub use error::{CatalogError, CatalogErrorCode, CatalogResult};
pub use hive::{HiveMetastore, HiveMetastoreConfig, HiveTableSnapshot, Metastore};
pub use ir::{
    CatalogField, CatalogSchemaVersion, CatalogType, FieldPath, ListKind, PartitionMetadata, ProtobufType,
    SchemaVersion, TimeUnit, TypeMapping, deterministic_field_id, deterministic_table_id, next_protobuf_field_number,
    protobuf_field_number,
};
pub use memory::{FailurePoint, MemoryCatalogStore, MemoryMetastore};
pub use saga::{
    CatalogControlPlane, DdlCompletion, DdlCompletionDisposition, FlightSqlDdlHandler, FlightSqlDdlRequest,
    ReconciliationReport,
};
pub use sql_server::{SqlServerCatalogStore, SqlServerConfig};
pub use store::{
    CatalogStore, ChangeReservation, RequestHash, ReserveOutcome, SagaId, SagaRecord, SagaState, SagaStep,
};
pub use validation::{AlignmentAction, SchemaAlignmentPlan, SchemaValidator, ValidatedIncomingSchema};
