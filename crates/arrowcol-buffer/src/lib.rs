//! Bounded, zero-copy direct-collector buffering.
//!
//! Record batches are retained as Arrow's existing reference-counted arrays.
//! The crate never concatenates batches and releases capacity only after a
//! durable publication or an explicitly reported incomplete outcome.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod config;
mod error;
mod generation;
mod manager;
mod quiver;
mod sink;
mod telemetry;

pub use generation::CloudGeneration;
pub use telemetry::MetricsGuard;

pub use config::{BufferConfig, CapacityLimits};
pub use error::{BufferError, BufferErrorKind, BufferResult};
pub use manager::{
    Admission, BufferManager, BufferPressureSnapshot, DeliveryCompletion, DeliveryOutcome, FlushReason, IncompleteWork,
    ShutdownReport, TablePressureSnapshot, TrackedState,
};
pub use quiver::{
    DELIVERY_MANIFEST_FORMAT, DELIVERY_MANIFEST_VERSION, DELIVERY_SLOT, DELTA_SUBSCRIBER, DeliveryBundle,
    DeliveryManifest, PersistRequest, QuiverStage, QuiverStageConfig, QuiverStageSnapshot, StageLeases,
};
pub use sink::{
    DeltaPublicationSink, PublicationContext, PublicationOutcome, PublicationSession, PublicationSink, SinkError,
    SinkErrorKind,
};
