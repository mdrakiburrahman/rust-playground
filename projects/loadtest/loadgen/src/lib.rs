//! Production many-table Arrow Flight benchmark and independent Delta validation.

#![forbid(unsafe_code)]

pub mod cli;
pub mod final_compaction;
pub mod generator;
pub mod initialize;
mod kusto;
pub mod manifest;
pub mod release;
pub mod runner;
pub mod schema;
pub mod sdk;
pub mod transport;
pub mod validation;

pub use cli::{Cli, Command};
pub use kusto::{ObservationMode, initialize_local_qap, observe_qap, observe_qap_with_mode};
