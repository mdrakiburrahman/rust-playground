//! Standalone Spark SQL catalog, durable Delta metadata and CES notifications.
#![forbid(unsafe_code)]

mod flight;
pub use flight::CatalogFlightService;
mod identity;
pub use identity::{DdlIdentityPolicy, DdlIdentityVerifier};
mod service;
pub use service::{CatalogConfig, CatalogService};
