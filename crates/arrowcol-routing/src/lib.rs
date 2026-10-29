//! Pressure-aware routing and SQL-coordinated ownership for Arrowcol routers.
//!
//! The request path reads an immutable cached view. SQL is consulted only when
//! a lease is absent or expired, while the background control loop refreshes
//! registrations, snapshots, and leases, performs fenced drain transfers, and
//! hands crashed holders' leases to their same-instance-key restart.

#![forbid(unsafe_code)]

mod control;
mod disk;
mod engine;
mod error;
mod memory;
mod migration;
mod model;
mod selection;
mod sql_server;
mod store;

pub use control::{ControlCredential, RemoteRoutingStore, serve_control};
pub use disk::{DiskRoutingStore, PlacementPolicy};
pub use engine::{ControlOperation, ControlRoute, RebalanceReport, RouteAssignment, RouteSource, RoutingEngine};
pub use error::{RoutingError, RoutingErrorKind, RoutingResult};
pub use memory::{MemoryOperationCounts, MemoryRoutingStore};
pub use migration::{MigrationAck, MigrationPhase, TableMigration};
pub use model::{
    CollectorRecord, CollectorRegistration, CollectorState, ContainerMemory, LeaseKey, LeaseState, PressurePolicy,
    RichPressureSnapshot, RoutingLease, RoutingPolicy, RoutingView, SubsystemState, TablePressure,
};
pub use selection::{collector_is_eligible, rank_collectors};
pub use sql_server::{SqlServerRoutingConfig, SqlServerRoutingStore};
pub use store::{LeaseCas, LeaseClaim, LeaseSuccession, LeaseTransfer, RoutingStore};
pub mod delta;
