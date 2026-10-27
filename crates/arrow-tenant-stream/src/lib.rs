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

#[async_trait::async_trait]
impl checkpoint::StoreAuthority for arrow_deltalake::publisher::PublisherSession {
    async fn check(&self) -> anyhow::Result<()> {
        self.check_async().await
    }
}

#[cfg(test)]
mod publisher_tests {
    use super::checkpoint::{Checkpoint, StoreAuthority};
    use anyhow::Result;
    use arrow_deltalake::publisher::{MutationRetries, Phase, PublisherSession};
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::sync::Arc;
    use url::Url;

    #[tokio::test]
    async fn checkpoint_releases_before_clean_process_session_close() -> Result<()> {
        let session = PublisherSession::local();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let url = Url::parse("file:///checkpoint-session/")?;
        let store = session.bind_store(&url, inner, MutationRetries::Disabled)?;
        let authority: Arc<dyn StoreAuthority> = Arc::<PublisherSession>::clone(&session);
        let mut checkpoint = Checkpoint::open_store_with_authority(store, Path::from("checkpoint"), authority).await?;
        checkpoint.recover().await?;
        checkpoint.prepare(0, &serde_json::json!({"next": 1}), 123).await?;
        checkpoint.commit(0).await?;
        checkpoint.close().await?;
        assert_eq!(session.status().inflight, 0);
        assert!(session.status().uncertain.is_none());
        session.close_after_drain_async().await?;
        assert_eq!(session.status().phase, Phase::Closed);
        assert!(session.check_async().await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn uncertain_checkpoint_mutation_cannot_be_a_clean_process_drain() -> Result<()> {
        let session = PublisherSession::local();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let url = Url::parse("file:///checkpoint-session/")?;
        let store = session.bind_store(&url, inner, MutationRetries::Opaque)?;
        let authority: Arc<dyn StoreAuthority> = Arc::<PublisherSession>::clone(&session);
        let opened = Checkpoint::open_store_with_authority(store, Path::from("checkpoint"), authority).await;
        assert!(opened.is_err(), "post-mutation authority check must expose uncertainty");
        assert!(session.status().uncertain.is_some());
        assert!(session.close_after_drain_async().await.is_err());
        assert_ne!(session.status().phase, Phase::Closed);
        Ok(())
    }
}
