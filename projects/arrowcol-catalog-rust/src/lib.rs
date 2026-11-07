//! Standalone Spark SQL catalog, durable Delta metadata and CES notifications.
#![forbid(unsafe_code)]

mod service;
pub use service::{CatalogConfig, CatalogService};
