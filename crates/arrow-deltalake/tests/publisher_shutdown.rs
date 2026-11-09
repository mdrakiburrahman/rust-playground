#![cfg(feature = "delta")]

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use arrow_deltalake::publisher::{CasOutcome, ControlMetadata, MetadataSnapshot, PublisherSession};
use async_trait::async_trait;

const SANDBOX: &str = "0123456789ab";
const GENERATION: &str = "00000000-0000-4000-8000-000000000001";

#[derive(Debug)]
struct Authority {
    state: Mutex<(MetadataSnapshot, bool)>,
}

impl Authority {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new((
                MetadataSnapshot {
                    etag: "0".into(),
                    metadata: [
                        ("sandboxid", SANDBOX),
                        ("resetstate", "ready-v1"),
                        ("resetgeneration", GENERATION),
                        ("publisherregistryversion", "1"),
                        ("publishers", "{}"),
                        ("occupancy", "released"),
                        ("operatornote", "preserve"),
                    ]
                    .into_iter()
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .collect(),
                },
                false,
            )),
        })
    }
}

#[async_trait]
impl ControlMetadata for Authority {
    async fn read(&self) -> Result<MetadataSnapshot> {
        Ok(self.state.lock().map_err(|error| anyhow::anyhow!("{error}"))?.0.clone())
    }

    async fn compare_and_set(&self, expected: &MetadataSnapshot, metadata: &BTreeMap<String, String>) -> CasOutcome {
        let Ok(mut state) = self.state.lock() else {
            return CasOutcome::Rejected("test authority lock poisoned".into());
        };
        if state.1 {
            return CasOutcome::Unknown("injected close response loss".into());
        }
        if state.0.etag != expected.etag {
            return CasOutcome::Conflict;
        }
        state.0.metadata = metadata.clone();
        state.0.etag.push('1');
        CasOutcome::Applied
    }
}

#[tokio::test]
async fn process_shared_shutdown_removes_owned_member_and_preserves_unknown_close() -> Result<()> {
    let authority = Authority::new();
    let session = PublisherSession::shared_with_control(
        "shutdown-success",
        Arc::<Authority>::clone(&authority),
        SANDBOX,
        "acct",
        GENERATION,
    )
    .await?;
    assert!(session.publisher_id().is_some());
    PublisherSession::close_shared_async().await?;
    let clean = authority.read().await?;
    assert_eq!(clean.metadata.get("publishers").map(String::as_str), Some("{}"));
    assert_eq!(
        clean.metadata.get("resetgeneration").map(String::as_str),
        Some(GENERATION)
    );
    assert_eq!(clean.metadata.get("operatornote").map(String::as_str), Some("preserve"));

    let uncertain = Authority::new();
    let session = PublisherSession::shared_with_control(
        "shutdown-unknown",
        Arc::<Authority>::clone(&uncertain),
        SANDBOX,
        "acct",
        GENERATION,
    )
    .await?;
    let publisher = session.publisher_id().context("publisher must be registered")?;
    uncertain.state.lock().map_err(|error| anyhow::anyhow!("{error}"))?.1 = true;
    assert!(PublisherSession::close_shared_async().await.is_err());
    let retained = uncertain.read().await?;
    let members: serde_json::Value =
        serde_json::from_str(retained.metadata.get("publishers").context("publisher registry")?)?;
    assert!(
        members.get(publisher.to_string()).is_some(),
        "unknown close must retain owned member"
    );
    assert_eq!(
        retained.metadata.get("resetgeneration").map(String::as_str),
        Some(GENERATION)
    );
    Ok(())
}
