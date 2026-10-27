//! Append-only historical tenant forks over Kernel-validated Delta tables.

pub mod checkpoint;
pub mod metrics;
pub mod reader;
pub mod source;
pub mod stream;

#[async_trait::async_trait]
impl checkpoint::StoreAuthority for arrow_deltalake::delta::StorageAuthority {
    async fn check(&self) -> anyhow::Result<()> {
        arrow_deltalake::delta::StorageAuthority::check(self)
    }
}
pub mod sink;
