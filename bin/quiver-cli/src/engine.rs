//! Opens Quiver with the durable-buffer settings used by the DFE collector.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use otel_arrow_dfe_quiver::config::{QuiverConfig, RetentionPolicy};
use otel_arrow_dfe_quiver::record_bundle::RecordBundle;
use otel_arrow_dfe_quiver::subscriber::SubscriberId;
use otel_arrow_dfe_quiver::{DiskBudget, QuiverEngine, QuiverEngineBuilder};

use crate::dfe::DfeManifest;

pub(crate) const CAPACITY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub(crate) const DFE_SEGMENT_MAX_OPEN: Duration = Duration::from_secs(1);
pub(crate) const SUBSCRIBER: &str = "durable-buffer";

pub(crate) async fn open(dir: &Path) -> Result<Arc<QuiverEngine>> {
    let mut config = QuiverConfig {
        data_dir: dir.join("core_0"),
        ..QuiverConfig::default()
    };
    config.wal.flush_interval = Duration::ZERO;
    config.segment.max_open_duration = DFE_SEGMENT_MAX_OPEN;
    let budget = DiskBudget::for_config(CAPACITY_BYTES, &config, RetentionPolicy::Backpressure)?;
    let engine = QuiverEngineBuilder::new(config)
        .with_budget(Arc::new(budget))
        .with_wal_item_counter(Arc::new(|bundle: &dyn RecordBundle| {
            bundle
                .metadata()
                .and_then(|bytes| DfeManifest::decode(bytes).ok())
                .map(|manifest| manifest.root_items)
        }))
        .with_wal_byte_counter(Arc::new(|bundle: &dyn RecordBundle| bundle.byte_count()))
        .build()
        .await?;
    let subscriber = SubscriberId::new(SUBSCRIBER)?;
    engine.register_subscriber(subscriber.clone())?;
    engine.activate_subscriber(&subscriber)?;
    Ok(engine)
}
