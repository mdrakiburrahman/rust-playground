//! DFE-native Arrow Flight ingress and arbitrary Arrow Delta export.
//!
//! The receiver validates every schema through `arrowcol-catalog`, moves the
//! resulting Arrow batches into pluggable DFE pdata without serialization or
//! buffer copies, and reports either best-effort admission or, with
//! `native_quiver: true` and a downstream `processor:durable_buffer`, durable
//! admission once every batch was persisted. The exporter consumes only that
//! representation, preserves its table/schema/lease envelope, rebinds
//! durable-buffer replays to the lease this process currently holds, and
//! writes the original batch through `arrow-deltalake`.

#![deny(unsafe_code)]

mod config;
mod envelope;
mod exporter;
mod metrics;
mod receiver;
mod routing;
mod shutdown;

pub use arrow_deltalake::delta::initialize_shared_io;
pub use config::{
    AdaptiveFileConfig, BatchCapacity, CatalogConfig, CommitConfig, DeltaExporterConfig, FileBatchConfig,
    FlightReceiverConfig, HiveMetastoreConnectionConfig, RegistrationConfig, RoutingStoreConfig, RuntimeLimitsConfig,
    SqlServerConnectionConfig, configure_cloud_generation,
};
pub use envelope::{
    ARROWCOL_FLIGHT_REPRESENTATION_ID, ARROWCOL_FLIGHT_REPRESENTATION_VERSION, ArrowcolBatchEnvelope,
    DecodedArrowBatch, EnvelopeError, arrowcol_flight_representation, decode_record_set, encode_record_batch,
};
pub use exporter::{ARROW_DELTALAKE_EXPORTER, ARROW_DELTALAKE_EXPORTER_URN, GenericDeltaExporter};
pub use metrics::{
    DashboardCapabilities, DashboardPressure, NativeQuiverCapability, RuntimeMetrics, TableVersionStatus,
    global_metrics, process_instance_id, process_instance_key,
};
pub use receiver::{ARROW_FLIGHT_RECEIVER, ARROW_FLIGHT_RECEIVER_URN, DfeAdmission, PdataSendError, PdataSender};
pub use routing::{LeaseResolver, StoreLeaseResolver};
pub use shutdown::{RoutingShutdown, finish_routing_shutdown};
