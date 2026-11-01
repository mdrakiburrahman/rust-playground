//! Sidecar that keeps local Kusto emulator tables equal to committed Delta snapshots.
//!
//! The emulator's native query acceleration accepts policies over local Delta external tables,
//! but this service publishes exact snapshot generations with explicit source-version, lag, and
//! degraded reporting so local functional tests can assert after-removal correctness.

pub mod config;
pub mod server;
pub mod status;
