//! Catalog RPC contract and CES-backed schema clients.
#![forbid(unsafe_code)]

/// Generated catalog metadata and change-event RPC types.
pub mod rpc {
    #![allow(missing_docs, clippy::missing_errors_doc, clippy::clone_on_ref_ptr)]
    tonic::include_proto!("arrowcol.catalog.v1");
}

use arrowcol_catalog::{CatalogResult, CatalogSchemaVersion};
use arrowcol_protocol::TableIdentity;
use async_trait::async_trait;

/// Catalog high watermark paired with one immutable table history.
#[derive(Clone, Debug)]
pub struct CachedTable {
    /// Position at which this snapshot was observed.
    pub position: rpc::EventPosition,
    /// HMS-shaped table and committed descriptor artifacts.
    pub snapshot: rpc::TableSnapshot,
    /// Oldest to newest committed schema versions.
    pub versions: Vec<CatalogSchemaVersion>,
}

/// Fetch-through table cache consumed by ingestion and maintenance.
#[async_trait]
pub trait CatalogClient: Send + Sync {
    /// Returns a fresh-enough immutable snapshot; a warm hit performs no RPC.
    async fn table(&self, identity: &TableIdentity) -> CatalogResult<CachedTable>;

    /// Discards locally cached state so the next access must revalidate remotely.
    async fn invalidate(&self, identity: &TableIdentity);
}
