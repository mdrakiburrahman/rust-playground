//! Production many-table Arrow Flight benchmark and independent Delta validation.

#![forbid(unsafe_code)]

pub mod cli;
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
pub use kusto::observe_qap;
