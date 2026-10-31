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
    let publication = ReceiptId::new();
    shutdown.publication_finished(publication, false)?;
    shutdown.publication_finished(ReceiptId::new(), true)?;
    // The unregistered probe proves accounting only; a registered failed finish
    // would persist the sticky `failed` terminal state checked below.
    assert!(shutdown.finish(true).await.is_err());
    shutdown.register(Arc::clone(&store) as Arc<dyn RoutingStore>, id)?;
    shutdown.publication_finished(publication, true)?;
    shutdown.finish(true).await?;
    assert_eq!(
        store.load_view(UnixMillis::new(2)?).await?.collectors[&id].state,
        CollectorState::Stopped
    );
    Ok(())
}

/// Scenario: a registered finish records `failed`, then a later clean finish is attempted.
/// Guarantees: terminal failure evidence is sticky; the clean claim is reported, not applied.
#[tokio::test]
async fn persisted_failure_is_not_overwritten_by_later_clean_finish() -> TestResult {
    let (store, id) = draining_store().await?;
    let shutdown = RoutingShutdown::default();
    shutdown.register(Arc::clone(&store) as Arc<dyn RoutingStore>, id)?;
    let publication = ReceiptId::new();
    shutdown.publication_finished(publication, false)?;
    assert!(shutdown.finish(true).await.is_err());
    shutdown.publication_finished(publication, true)?;
    assert!(shutdown.finish(true).await.is_err());
    assert_eq!(
        store.load_view(UnixMillis::new(2)?).await?.collectors[&id].state,
        CollectorState::Failed
    );
    Ok(())
}

/// Scenario: two receiver cores share the process routing identity. Core 1's
/// registration heartbeat is stamped after core 0 takes its drain timestamp,
/// and lands first; core 0 then records `draining`, core 1 repeats it, and a
/// late core-1 refresh arrives before the terminal hook runs.
/// Guarantees: neither drain conflicts, the late refresh cannot resurrect
/// `ready`, and the clean terminal hook still records `stopped`.
#[tokio::test]
async fn multi_core_drain_does_not_lose_to_sibling_heartbeat() -> TestResult {
    let store = Arc::new(MemoryRoutingStore::default());
    let id = CollectorInstanceId::new();
    let registration = |heartbeat: u64| -> Result<CollectorRegistration, Box<dyn std::error::Error>> {
        Ok(CollectorRegistration {
            instance_id: id,
            instance_key: "dataflow-multi-core".to_owned(),
            implementation_pool: ImplementationPool::Dataflow,
            flight_endpoint: "127.0.0.1:32010".parse()?,
            catalog_capable: true,
            max_concurrent_streams: PositiveMeasurement::new(4)?,
            started_at_unix_ms: UnixMillis::new(1)?,
            heartbeat_at_unix_ms: UnixMillis::new(heartbeat)?,
            lease_expires_at_unix_ms: UnixMillis::new(60_000)?,
        })
    };
    store.register_collector(&registration(1)?).await?;
    store.register_collector(&registration(20)?).await?;
    store
        .set_collector_state(id, CollectorState::Draining, UnixMillis::new(10)?)
        .await?;
    store
        .set_collector_state(id, CollectorState::Draining, UnixMillis::new(15)?)
        .await?;
    assert!(store.register_collector(&registration(30)?).await.is_err());
    assert_eq!(
        store.load_view(UnixMillis::new(40)?).await?.collectors[&id].state,
        CollectorState::Draining
    );
    let shutdown = RoutingShutdown::default();
    shutdown.register(Arc::clone(&store) as Arc<dyn RoutingStore>, id)?;
    shutdown.finish(true).await?;
    assert_eq!(
        store.load_view(UnixMillis::new(40)?).await?.collectors[&id].state,
        CollectorState::Stopped
    );
    Ok(())
}
