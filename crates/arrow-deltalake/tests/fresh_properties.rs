#![cfg(feature = "delta")]

use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::{
    DeltaTable, FreshTableTransport, InitialDeltaProperties, PublicationFence, PublicationFenceContext,
    PublicationFenceError, object_store_for_url,
};
use arrow_deltalake::publisher::{MutationRetries, PublisherSession};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use url::Url;

struct Fence {
    calls: Arc<AtomicUsize>,
    reject: bool,
}

impl PublicationFence for Fence {
    fn check_before_commit(&self, context: &PublicationFenceContext<'_>) -> Result<(), PublicationFenceError> {
        assert_eq!(context.read_version(), 0);
        assert_eq!(context.proposed_version(), 0);
        assert_eq!(context.file_count(), 0);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.reject {
            return Err(PublicationFenceError::stale("test fence refuses creation"));
        }
        Ok(())
    }
}

#[test]
fn fresh_literal_properties_keep_fencing_and_refuse_existing_objects() -> Result<()> {
    for reject in [false, true] {
        let root = tempfile::tempdir()?;
        let url = Url::from_directory_path(root.path()).map_err(|()| anyhow::anyhow!("table URL"))?;
        let calls = Arc::new(AtomicUsize::new(0));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let initialize = || {
            let session = PublisherSession::local();
            let result = DeltaTable::initialize_fresh_url_with_session_and_properties(
                url.clone(),
                Arc::clone(&schema),
                Vec::new(),
                Arc::clone(&session),
                Arc::new(Fence {
                    calls: Arc::clone(&calls),
                    reject,
                }),
                FreshTableTransport {
                    store: object_store_for_url(&url)?,
                    mutation_retries: MutationRetries::Disabled,
                },
                &InitialDeltaProperties {
                    target_file_size_bytes: Some(67108864),
                    checkpoint_interval: Some(17),
                },
            );
            session.close_after_drain()?;
            result
        };
        let result = initialize();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let log = root.path().join("_delta_log/00000000000000000000.json");
        if reject {
            assert!(result.is_err());
            assert!(!log.exists());
        } else {
            let receipt = result?;
            assert_eq!(receipt.version, 0);
            assert_eq!(receipt.properties["delta.targetFileSize"], "67108864");
            assert_eq!(receipt.properties["delta.checkpointInterval"], "17");
            let original = std::fs::read(&log)?;
            assert!(initialize().is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(std::fs::read(&log)?, original);
        }
    }
    Ok(())
}
