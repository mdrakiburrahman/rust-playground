use std::sync::Arc;

use arrowcol_dataflow_components::RoutingShutdown;
use arrowcol_protocol::{CollectorInstanceId, ImplementationPool, PositiveMeasurement, ReceiptId, UnixMillis};
use arrowcol_routing::{CollectorRegistration, CollectorState, MemoryRoutingStore, RoutingStore};

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn draining_store() -> Result<(Arc<MemoryRoutingStore>, CollectorInstanceId), Box<dyn std::error::Error>> {
    let store = Arc::new(MemoryRoutingStore::default());
    let id = CollectorInstanceId::new();
    let now = UnixMillis::new(1)?;
    store
        .register_collector(&CollectorRegistration {
            instance_id: id,
            instance_key: "dataflow-shutdown".to_owned(),
            implementation_pool: ImplementationPool::Dataflow,
            flight_endpoint: "127.0.0.1:32010".parse()?,
            catalog_capable: true,
            max_concurrent_streams: PositiveMeasurement::new(4)?,
            started_at_unix_ms: now,
            heartbeat_at_unix_ms: now,
            lease_expires_at_unix_ms: UnixMillis::new(60_000)?,
        })
        .await?;
    store.set_collector_state(id, CollectorState::Draining, now).await?;
    Ok((store, id))
}

/// Scenario: receiver ingress has drained, then the controller and publisher close cleanly.
/// Guarantees: the retained routing row becomes explicitly stopped, not merely expired.
#[tokio::test]
async fn clean_drain_persists_stopped_after_publisher_completion() -> TestResult {
    let (store, id) = draining_store().await?;
    let shutdown = RoutingShutdown::default();
    shutdown.register(Arc::clone(&store) as Arc<dyn RoutingStore>, id)?;
    assert_eq!(
        store.load_view(UnixMillis::new(2)?).await?.collectors[&id].state,
        CollectorState::Draining
    );
    shutdown.finish(true).await?;
    assert_eq!(
        store.load_view(UnixMillis::new(2)?).await?.collectors[&id].state,
        CollectorState::Stopped
    );
    Ok(())
}

/// Scenario: controller drain or publisher close failed after ingress was frozen.
/// Guarantees: every registered collector becomes failed rather than stopped or draining.
#[tokio::test]
async fn incomplete_drain_or_close_persists_failed() -> TestResult {
    let (store, id) = draining_store().await?;
    let shutdown = RoutingShutdown::default();
    shutdown.register(Arc::clone(&store) as Arc<dyn RoutingStore>, id)?;
    shutdown.finish(false).await?;
    assert_eq!(
        store.load_view(UnixMillis::new(2)?).await?.collectors[&id].state,
        CollectorState::Failed
    );
    Ok(())
}

/// Scenario: one terminal write fails but another registered receiver can still be finalized.
/// Guarantees: the error is surfaced and does not prevent the remaining terminal writes.
#[tokio::test]
async fn failed_terminal_write_is_reported_without_abandoning_other_collectors() -> TestResult {
    let (store, id) = draining_store().await?;
    let shutdown = RoutingShutdown::default();
    shutdown.register(Arc::new(MemoryRoutingStore::default()), CollectorInstanceId::new())?;
    shutdown.register(Arc::clone(&store) as Arc<dyn RoutingStore>, id)?;
    assert!(shutdown.finish(true).await.is_err());
    assert_eq!(
        store.load_view(UnixMillis::new(2)?).await?.collectors[&id].state,
        CollectorState::Stopped
    );
    Ok(())
}

/// Scenario: publication NACKed even though the engine and publisher-close callbacks returned success.
/// Guarantees: a publication failure cannot be hidden behind a clean engine exit.
#[tokio::test]
async fn publication_failure_prevents_stopped_after_clean_controller_exit() -> TestResult {
    let (store, id) = draining_store().await?;
    let shutdown = RoutingShutdown::default();
    shutdown.register(Arc::clone(&store) as Arc<dyn RoutingStore>, id)?;
    shutdown.publication_finished(ReceiptId::new(), false)?;
    assert!(shutdown.finish(true).await.is_err());
    assert_eq!(
        store.load_view(UnixMillis::new(2)?).await?.collectors[&id].state,
        CollectorState::Failed
    );
    Ok(())
}

/// Scenario: a transient NACK is redelivered with its original stable publication identity.
/// Guarantees: confirmed publication clears only that identity's incomplete work.
#[tokio::test]
async fn recovered_nack_can_stop_but_different_publication_does_not_clear_it() -> TestResult {
    let (store, id) = draining_store().await?;
    let shutdown = RoutingShutdown::default();
    shutdown.register(Arc::clone(&store) as Arc<dyn RoutingStore>, id)?;
    let publication = ReceiptId::new();
    shutdown.publication_finished(publication, false)?;
    shutdown.publication_finished(ReceiptId::new(), true)?;
    assert!(shutdown.finish(true).await.is_err());
    shutdown.publication_finished(publication, true)?;
    shutdown.finish(true).await?;
    assert_eq!(
        store.load_view(UnixMillis::new(2)?).await?.collectors[&id].state,
        CollectorState::Stopped
    );
    Ok(())
}
