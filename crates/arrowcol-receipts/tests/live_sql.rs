use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest, MemoryCatalogStore,
    MemoryMetastore, SchemaValidator,
};
use arrowcol_protocol::{
    ClaimedSchemaFingerprint, CollectorInstanceId, DeltaPublicationResult, ErrorCode, ErrorStatus, ImplementationPool,
    Measurement, ReceiptId, ReceiptState, TableLease, UnixMillis, fingerprint_schema,
};
use arrowcol_receipts::{
    AdoptionCandidate, FailureDisposition, OwnerRegistration, PublicationBatch, ReceiptAdoption, ReceiptErrorKind,
    ReceiptFailure, ReceiptRecord, ReceiptStore, ReceiptVersion, RejectedAdoption, SqlServerReceiptConfig,
    SqlServerReceiptStore, TableLeaseRequest, ValidatedDelivery,
};
use arrowcol_sql_server::{ConnectionConfig, PoolConfig, SqlPool};
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, Row, ToSql};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use uuid::Uuid;

type SqlClient = Client<Compat<TcpStream>>;

static LIVE_RECEIPT_CASE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Recovery window used by owners that must outlive the whole adoption case.
const LONG_RECOVERY_WINDOW_MS: u64 = 60_000;
/// Recovery window short enough for a live test to observe its expiry.
const SHORT_RECOVERY_WINDOW_MS: u64 = 2_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server"]
async fn live_receipts_prove_durability_races_and_hot_queries() -> anyhow::Result<()> {
    live_receipts_case(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server"]
async fn pooled_live_receipts_reuse_after_durability_races_and_errors() -> anyhow::Result<()> {
    live_receipts_case(true).await
}

/// Scenario: a durable-buffer owner (generation 1, recovery window) crashes
/// while holding accepted, buffered, and flushing receipts. A same-key
/// generation-2 successor registers, acquires a newer table lease, adopts the
/// receipts, later rebinds them to an even newer lease of its own, and
/// buffers, flushes, and publishes them.
/// Guarantees: expiry reconciliation skips the dead owner's receipts inside
/// its window and the live successor's past-deadline backlog; a different
/// instance key, a stale expectation, and a released lease cannot adopt;
/// adoption keeps state and revision, rebinds owner, lease, fencing epoch,
/// deadline, and heartbeat, and appends one `adopted` event per receipt; the
/// original receipt IDs reach `published`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server"]
#[allow(clippy::too_many_lines)]
async fn live_durable_successor_adopts_and_publishes_predecessor_receipts() -> anyhow::Result<()> {
    if std::env::var("ARROWCOL_SQLSERVER_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    let _isolation = LIVE_RECEIPT_CASE.lock().await;
    let fixture = fixture().await?;
    seed_catalog(&fixture).await?;
    let store = SqlServerReceiptStore::new(sql_config()?);
    let delivery = ValidatedDelivery::new(&fixture.validation, 3, 1, 24)?;
    let instance_key = format!("live-durable-{}", Uuid::new_v4().simple());

    let predecessor = CollectorInstanceId::new();
    let predecessor_expiry = register_live_owner(
        &store,
        predecessor,
        &instance_key,
        1,
        1_500,
        Some(LONG_RECOVERY_WINDOW_MS),
    )
    .await?;
    let predecessor_lease = acquire_live_lease(&store, &fixture, predecessor, predecessor_expiry).await?;
    let created = store
        .create_flight_batch(
            predecessor,
            &predecessor_lease,
            plus(now()?, 500)?,
            &[delivery.clone(), delivery.clone(), delivery.clone()],
        )
        .await?;
    let buffered = store
        .mark_buffered(predecessor, &versions(&created[1..]), plus(now()?, 500)?)
        .await?;
    let flushing = store
        .mark_flushing(predecessor, &versions(&buffered[1..]), plus(now()?, 500)?)
        .await?;
    let held = [created[0].clone(), buffered[0].clone(), flushing[0].clone()];
    let ids: Vec<_> = held.iter().map(|record| record.receipt_id).collect();
    assert_eq!(
        held.iter()
            .map(|record| (record.state, record.revision))
            .collect::<Vec<_>>(),
        [
            (ReceiptState::Accepted, 0),
            (ReceiptState::Buffered, 1),
            (ReceiptState::Publishing, 2)
        ]
    );

    // The predecessor "crashes": its owner lease, table lease, and receipt
    // deadlines all expire without a heartbeat.
    sleep_until(plus(predecessor_expiry, 100)?).await?;
    let reconciled = reconcile_all(&store).await?;
    assert!(
        reconciled.iter().all(|record| !ids.contains(&record.receipt_id)),
        "receipts inside the owner's recovery window must not be reconciled"
    );
    assert_eq!(
        store.receipts(&ids).await?,
        held.iter().cloned().map(Some).collect::<Vec<_>>()
    );

    let successor = CollectorInstanceId::new();
    let successor_expiry = register_live_owner(
        &store,
        successor,
        &instance_key,
        2,
        120_000,
        Some(LONG_RECOVERY_WINDOW_MS),
    )
    .await?;
    let window = Some(i32::try_from(LONG_RECOVERY_WINDOW_MS)?);
    assert_eq!(owner_row(predecessor).await?, ("failed".to_owned(), true, window));
    assert_eq!(owner_row(successor).await?, ("active".to_owned(), false, window));

    let intruder = CollectorInstanceId::new();
    let intruder_expiry =
        register_live_owner(&store, intruder, &format!("{instance_key}-intruder"), 1, 120_000, None).await?;
    let intruder_lease = acquire_live_lease(&store, &fixture, intruder, intruder_expiry).await?;
    let foreign = store
        .adopt_receipts(&adoption(
            intruder,
            &intruder_lease,
            now()?,
            held.iter().map(AdoptionCandidate::from_record).collect(),
        )?)
        .await?;
    assert!(foreign.adopted.is_empty(), "a different instance key must not adopt");
    assert_eq!(foreign.rejected, rejections(&held));
    store.release_table_lease(&intruder_lease, now()?).await?;

    let first_lease = acquire_live_lease(&store, &fixture, successor, successor_expiry).await?;
    assert!(first_lease.fencing_token() > intruder_lease.fencing_token());
    let adopted_at = now()?;
    let deadline = plus(adopted_at, 300)?;
    let stale = AdoptionCandidate::new(held[1].receipt_id, predecessor, ReceiptState::Accepted, 0);
    let partial = store
        .adopt_receipts(&ReceiptAdoption {
            successor,
            lease: first_lease.clone(),
            lease_deadline: deadline,
            adopted_at,
            receipts: vec![AdoptionCandidate::from_record(&held[0]), stale],
        })
        .await?;
    assert_eq!(partial.rejected, rejections(&held[1..2]));
    let rest = store
        .adopt_receipts(&ReceiptAdoption {
            successor,
            lease: first_lease.clone(),
            lease_deadline: deadline,
            adopted_at,
            receipts: held[1..].iter().map(AdoptionCandidate::from_record).collect(),
        })
        .await?;
    assert!(rest.rejected.is_empty());
    let adopted: Vec<_> = partial.adopted.into_iter().chain(rest.adopted).collect();
    assert_eq!(adopted.len(), held.len());
    for (record, original) in adopted.iter().zip(&held) {
        assert_eq!(record.receipt_id, original.receipt_id);
        assert_eq!((record.state, record.revision), (original.state, original.revision));
        assert_eq!(record.owner_instance_id, successor);
        assert_eq!(record.lease_id, first_lease.lease_id());
        assert_eq!(record.fencing_epoch, first_lease.fencing_token());
        assert_eq!(record.lease_deadline_unix_ms, deadline);
        assert_eq!(record.heartbeat_at_unix_ms, adopted_at);
        assert_eq!(record.updated_at_unix_ms, adopted_at);
        assert_eq!(record.created_at_unix_ms, original.created_at_unix_ms);
    }
    let events = adoption_events(&ids).await?;
    assert_eq!(events.len(), held.len());
    for (event, original) in events.iter().zip(&held) {
        assert_eq!(event.receipt_id, original.receipt_id.as_uuid());
        assert_eq!(event.revision, i64::try_from(original.revision)?);
        assert_eq!(event.from_state, sql_state(original.state));
        assert_eq!(event.actor, successor.as_uuid());
        assert_eq!(event.previous_owner, predecessor.as_uuid());
    }

    // Durable backlog may outlive its receipt deadline while the owner lives.
    sleep_until(plus(deadline, 100)?).await?;
    let reconciled = reconcile_all(&store).await?;
    assert!(
        reconciled.iter().all(|record| !ids.contains(&record.receipt_id)),
        "a live durable owner's past-deadline backlog must not be reconciled"
    );

    store.release_table_lease(&first_lease, now()?).await?;
    let second_lease = acquire_live_lease(&store, &fixture, successor, successor_expiry).await?;
    let candidates: Vec<_> = adopted.iter().map(AdoptionCandidate::from_record).collect();
    let released = store
        .adopt_receipts(&adoption(successor, &first_lease, now()?, candidates.clone())?)
        .await;
    assert_eq!(released.err().map(|error| error.kind()), Some(ReceiptErrorKind::Fenced));
    let rebound = store
        .adopt_receipts(&adoption(successor, &second_lease, now()?, candidates)?)
        .await?;
    assert!(rebound.rejected.is_empty());
    assert!(rebound.adopted.iter().all(|record| {
        record.owner_instance_id == successor
            && record.lease_id == second_lease.lease_id()
            && record.fencing_epoch == second_lease.fencing_token()
    }));
    let events = adoption_events(&ids).await?;
    assert_eq!(events.len(), 2 * held.len());
    assert!(
        events[held.len()..]
            .iter()
            .all(|event| event.previous_owner == successor.as_uuid() && event.actor == successor.as_uuid())
    );

    let rebuffered = store
        .mark_buffered(successor, &versions(&rebound.adopted[..1]), plus(now()?, 30_000)?)
        .await?;
    let reflushing = store
        .mark_flushing(
            successor,
            &[version(&rebuffered[0]), version(&rebound.adopted[1])],
            plus(now()?, 30_000)?,
        )
        .await?;
    let published = store
        .publish_commit(
            successor,
            &PublicationBatch {
                receipts: vec![
                    version(&reflushing[0]),
                    version(&reflushing[1]),
                    version(&rebound.adopted[2]),
                ],
                publication: DeltaPublicationResult::new(
                    Measurement::new(7)?,
                    Measurement::new(1)?,
                    Measurement::new(9)?,
                    Measurement::new(256)?,
                    delivery.schema_fingerprint(),
                    now()?,
                    second_lease.publication_fence(),
                ),
            },
        )
        .await?;
    assert_eq!(
        published.iter().map(|record| record.receipt_id).collect::<Vec<_>>(),
        ids
    );
    assert!(published.iter().all(ReceiptRecord::can_client_purge));

    for owner in [successor, intruder] {
        let drain = store.start_owner_drain(owner).await?;
        assert_eq!(drain.active_receipts, 0);
        store.finish_owner_drain(owner, now()?).await?;
    }
    Ok(())
}

/// Scenario: a durable owner with a short recovery window and an owner
/// without a window both hold receipts past their deadlines; the durable
/// owner then dies and no successor adopts its receipts in time.
/// Guarantees: the window-less owner's expired receipt reconciles at once,
/// as before; the durable owner's receipts survive while it lives and while
/// its window runs after lease expiry, then reconcile with today's retryable
/// or publication-outcome-unknown outcomes; a late same-key successor's
/// adoption is rejected and reports the reconciled (or missing) records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server"]
#[allow(clippy::too_many_lines)]
async fn live_recovery_window_expiry_restores_reconciliation() -> anyhow::Result<()> {
    if std::env::var("ARROWCOL_SQLSERVER_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    let _isolation = LIVE_RECEIPT_CASE.lock().await;
    let durable_fixture = fixture().await?;
    seed_catalog(&durable_fixture).await?;
    let plain_fixture = fixture().await?;
    seed_catalog(&plain_fixture).await?;
    let store = SqlServerReceiptStore::new(sql_config()?);
    let durable_delivery = ValidatedDelivery::new(&durable_fixture.validation, 2, 1, 16)?;
    let plain_delivery = ValidatedDelivery::new(&plain_fixture.validation, 2, 1, 16)?;
    let durable_key = format!("live-window-{}", Uuid::new_v4().simple());

    let durable = CollectorInstanceId::new();
    let durable_expiry =
        register_live_owner(&store, durable, &durable_key, 1, 1_200, Some(SHORT_RECOVERY_WINDOW_MS)).await?;
    let durable_lease = acquire_live_lease(&store, &durable_fixture, durable, durable_expiry).await?;
    let durable_created = store
        .create_flight_batch(
            durable,
            &durable_lease,
            plus(now()?, 300)?,
            &[durable_delivery.clone(), durable_delivery],
        )
        .await?;
    let durable_buffered = store
        .mark_buffered(durable, &versions(&durable_created[1..]), plus(now()?, 300)?)
        .await?;
    let durable_flushing = store
        .mark_flushing(durable, &versions(&durable_buffered), plus(now()?, 300)?)
        .await?;
    let durable_held = [durable_created[0].clone(), durable_flushing[0].clone()];
    let durable_ids: Vec<_> = durable_held.iter().map(|record| record.receipt_id).collect();

    let plain = CollectorInstanceId::new();
    let plain_key = format!("live-plain-{}", Uuid::new_v4().simple());
    let plain_expiry = register_live_owner(&store, plain, &plain_key, 1, 120_000, None).await?;
    let plain_lease = acquire_live_lease(&store, &plain_fixture, plain, plain_expiry).await?;
    let plain_created = store
        .create_flight_batch(
            plain,
            &plain_lease,
            plus(now()?, 300)?,
            std::slice::from_ref(&plain_delivery),
        )
        .await?;

    let last_deadline = durable_held
        .iter()
        .chain(&plain_created)
        .map(|record| record.lease_deadline_unix_ms)
        .max()
        .ok_or_else(|| anyhow::anyhow!("no receipt deadlines"))?;
    sleep_until(plus(last_deadline, 100)?).await?;
    let reconciled = reconcile_all(&store).await?;
    assert!(reconciled.iter().any(|record| {
        record.receipt_id == plain_created[0].receipt_id
            && record.state == ReceiptState::Failed
            && record.failure_disposition == Some(FailureDisposition::Retryable)
    }));
    assert!(
        reconciled
            .iter()
            .all(|record| !durable_ids.contains(&record.receipt_id))
    );

    sleep_until(plus(durable_expiry, 100)?).await?;
    let reconciled = reconcile_all(&store).await?;
    assert!(
        reconciled
            .iter()
            .all(|record| !durable_ids.contains(&record.receipt_id)),
        "a recently failed durable owner's receipts must wait for its window"
    );
    assert_eq!(
        store.receipts(&durable_ids).await?,
        durable_held.iter().cloned().map(Some).collect::<Vec<_>>()
    );

    sleep_until(plus(durable_expiry, SHORT_RECOVERY_WINDOW_MS + 100)?).await?;
    let reconciled = reconcile_all(&store).await?;
    let outcome = |receipt_id: ReceiptId| {
        reconciled
            .iter()
            .find(|record| record.receipt_id == receipt_id)
            .map(|record| (record.state, record.failure_disposition, record.revision))
    };
    assert_eq!(
        outcome(durable_ids[0]),
        Some((ReceiptState::Failed, Some(FailureDisposition::Retryable), 1))
    );
    assert_eq!(
        outcome(durable_ids[1]),
        Some((ReceiptState::PublicationUnknown, Some(FailureDisposition::Permanent), 3))
    );

    let late = CollectorInstanceId::new();
    let late_expiry =
        register_live_owner(&store, late, &durable_key, 2, 120_000, Some(SHORT_RECOVERY_WINDOW_MS)).await?;
    let late_lease = acquire_live_lease(&store, &durable_fixture, late, late_expiry).await?;
    let missing = ReceiptId::new();
    let mut candidates: Vec<_> = durable_held.iter().map(AdoptionCandidate::from_record).collect();
    candidates.push(AdoptionCandidate::new(missing, durable, ReceiptState::Accepted, 0));
    let late_report = store
        .adopt_receipts(&adoption(late, &late_lease, now()?, candidates)?)
        .await?;
    assert!(late_report.adopted.is_empty());
    let current = store.receipts(&durable_ids).await?;
    let mut expected: Vec<_> = durable_ids
        .iter()
        .zip(current)
        .map(|(receipt_id, current)| RejectedAdoption {
            receipt_id: *receipt_id,
            current,
        })
        .collect();
    expected.push(RejectedAdoption {
        receipt_id: missing,
        current: None,
    });
    assert_eq!(late_report.rejected, expected);
    assert!(late_report.rejected[..2].iter().all(|rejected| {
        rejected
            .current
            .as_ref()
            .is_some_and(|record| !record.can_client_purge())
    }));

    for owner in [late, plain] {
        let drain = store.start_owner_drain(owner).await?;
        assert_eq!(drain.active_receipts, 0);
        store.finish_owner_drain(owner, now()?).await?;
    }
    Ok(())
}

/// Scenario: an owner enters drain, keeps heartbeating while it flushes, then
/// finishes drain.
/// Guarantees: a draining owner renews its lease (its receipts and leases stay
/// owned, so a successor cannot adopt them mid-drain); an expired deadline is
/// still fenced while draining; a stopped owner is fenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server"]
async fn live_draining_owner_heartbeats_until_stopped() -> anyhow::Result<()> {
    if std::env::var("ARROWCOL_SQLSERVER_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    let _isolation = LIVE_RECEIPT_CASE.lock().await;
    let store = SqlServerReceiptStore::new(sql_config()?);
    let owner = CollectorInstanceId::new();
    let instance_key = format!("live-drain-heartbeat-{}", Uuid::new_v4().simple());
    register_live_owner(&store, owner, &instance_key, 1, 30_000, None).await?;
    store.heartbeat_owner(owner, plus(now()?, 30_000)?).await?;

    store.start_owner_drain(owner).await?;
    assert_eq!(owner_row(owner).await?.0, "draining");
    store.heartbeat_owner(owner, plus(now()?, 30_000)?).await?;
    let expired = store
        .heartbeat_owner(owner, minus(now()?, 1_000)?)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("an expired deadline must stay fenced while draining"))?;
    assert_eq!(expired.kind(), ReceiptErrorKind::Fenced);
    assert_eq!(owner_row(owner).await?.0, "draining");

    store.finish_owner_drain(owner, now()?).await?;
    let (state, stopped, _) = owner_row(owner).await?;
    assert_eq!(state, "stopped");
    assert!(stopped);
    let stopped_heartbeat = store
        .heartbeat_owner(owner, plus(now()?, 30_000)?)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("a stopped owner must be fenced"))?;
    assert_eq!(stopped_heartbeat.kind(), ReceiptErrorKind::Fenced);
    Ok(())
}

/// Scenario: a durable predecessor crashes after creating accepted receipts
/// that never reached its local buffer, alongside buffered work; a successor
/// of the same instance key drains its persisted backlog.
/// Guarantees: only the predecessor's accepted receipts become retryable
/// failures with an event; buffered work, the successor's own receipts and
/// another instance key's receipts are untouched; a repeat sweep is a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server"]
async fn live_successor_fails_unbuffered_predecessor_receipts() -> anyhow::Result<()> {
    if std::env::var("ARROWCOL_SQLSERVER_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    let _isolation = LIVE_RECEIPT_CASE.lock().await;
    let local = fixture().await?;
    seed_catalog(&local).await?;
    let store = SqlServerReceiptStore::new(sql_config()?);
    let delivery = ValidatedDelivery::new(&local.validation, 3, 1, 24)?;
    let instance_key = format!("live-unbuffered-{}", Uuid::new_v4().simple());

    let predecessor = CollectorInstanceId::new();
    let predecessor_expiry = register_live_owner(
        &store,
        predecessor,
        &instance_key,
        1,
        1_500,
        Some(LONG_RECOVERY_WINDOW_MS),
    )
    .await?;
    let predecessor_lease = acquire_live_lease(&store, &local, predecessor, predecessor_expiry).await?;
    let created = store
        .create_flight_batch(
            predecessor,
            &predecessor_lease,
            plus(now()?, 500)?,
            &[delivery.clone(), delivery.clone(), delivery.clone()],
        )
        .await?;
    let buffered = store
        .mark_buffered(predecessor, &versions(&created[2..]), plus(now()?, 500)?)
        .await?;

    let foreign = CollectorInstanceId::new();
    let foreign_expiry =
        register_live_owner(&store, foreign, &format!("{instance_key}-foreign"), 1, 120_000, None).await?;
    let foreign_fixture = fixture().await?;
    seed_catalog(&foreign_fixture).await?;
    let foreign_delivery = ValidatedDelivery::new(&foreign_fixture.validation, 3, 1, 24)?;
    let foreign_lease = acquire_live_lease(&store, &foreign_fixture, foreign, foreign_expiry).await?;
    let foreign_accepted = store
        .create_flight_batch(
            foreign,
            &foreign_lease,
            plus(now()?, 30_000)?,
            std::slice::from_ref(&foreign_delivery),
        )
        .await?;

    sleep_until(plus(predecessor_expiry, 100)?).await?;
    let successor = CollectorInstanceId::new();
    let successor_expiry = register_live_owner(
        &store,
        successor,
        &instance_key,
        2,
        120_000,
        Some(LONG_RECOVERY_WINDOW_MS),
    )
    .await?;
    assert_eq!(owner_row(predecessor).await?.0, "failed");
    let successor_lease = acquire_live_lease(&store, &local, successor, successor_expiry).await?;
    let own_accepted = store
        .create_flight_batch(
            successor,
            &successor_lease,
            plus(now()?, 30_000)?,
            std::slice::from_ref(&delivery),
        )
        .await?;
    store.start_owner_drain(successor).await?;

    let report = store
        .fail_unbuffered_predecessor_receipts(successor, now()?, 256)
        .await?;
    assert!(!report.limit_reached);
    let mut failed: Vec<_> = report.reconciled.iter().map(|record| record.receipt_id).collect();
    failed.sort();
    let mut expected: Vec<_> = created[..2].iter().map(|record| record.receipt_id).collect();
    expected.sort();
    assert_eq!(failed, expected);
    for record in &report.reconciled {
        assert_eq!(record.state, ReceiptState::Failed);
        assert_eq!(record.revision, 1);
        assert_eq!(record.owner_instance_id, predecessor);
    }
    let untouched = store
        .receipts(&[
            buffered[0].receipt_id,
            foreign_accepted[0].receipt_id,
            own_accepted[0].receipt_id,
        ])
        .await?;
    assert_eq!(
        untouched,
        vec![
            Some(buffered[0].clone()),
            Some(foreign_accepted[0].clone()),
            Some(own_accepted[0].clone())
        ]
    );
    let mut client = connect().await?;
    for receipt in &expected {
        let row = client
            .query(
                "SELECT from_state, to_state, actor_instance_id FROM arrowcol.receipt_events \
                 WHERE receipt_id = @P1 AND revision = 1;",
                &[&receipt.as_uuid()],
            )
            .await?
            .into_row()
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing failure event"))?;
        assert_eq!(required_text(&row, "from_state")?, "accepted");
        assert_eq!(required_text(&row, "to_state")?, "retryable_failure");
        assert_eq!(required_uuid(&row, "actor_instance_id")?, successor.as_uuid());
    }

    let repeat = store
        .fail_unbuffered_predecessor_receipts(successor, now()?, 256)
        .await?;
    assert!(repeat.reconciled.is_empty() && !repeat.limit_reached);
    Ok(())
}

async fn live_receipts_case(pooled: bool) -> anyhow::Result<()> {
    if std::env::var("ARROWCOL_SQLSERVER_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    // Reconciliation scans expired work across the database, not one fixture.
    let _isolation = LIVE_RECEIPT_CASE.lock().await;
    let fixture = fixture().await?;
    let pool = pooled.then(sql_pool).transpose()?;
    let mut store = SqlServerReceiptStore::new(sql_config()?);
    if let Some(pool) = &pool {
        store = store.with_pool(pool.clone())?;
    }
    let store = Arc::new(store.clone());
    seed_catalog(&fixture).await?;
    let owner = CollectorInstanceId::new();
    let start = now()?;
    let owner_deadline = plus(start, 180_000)?;
    store
        .register_owner(OwnerRegistration {
            instance_id: owner,
            instance_key: format!("live-receipts-{}", Uuid::new_v4().simple()),
            generation: 1,
            host_name: Some("live-test".to_owned()),
            build_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
            started_at_unix_ms: start,
            lease_deadline_unix_ms: owner_deadline,
            durable_recovery_window_ms: None,
        })
        .await?;
    let lease = store
        .acquire_table_lease(TableLeaseRequest {
            table_id: fixture.validation.registered().table_id(),
            table: fixture.validation.registered().identity().clone(),
            owner_instance_id: owner,
            implementation_pool: ImplementationPool::Direct,
            issued_at_unix_ms: start,
            expires_at_unix_ms: owner_deadline,
        })
        .await?;
    let delivery = ValidatedDelivery::new(&fixture.validation, 3, 1, 24)?;

    let duplicates = store
        .create_flight_batch(
            owner,
            &lease,
            plus(start, 60_000)?,
            &[delivery.clone(), delivery.clone()],
        )
        .await?;
    assert_eq!(duplicates.len(), 2);
    assert_ne!(duplicates[0].receipt_id, duplicates[1].receipt_id);
    assert_eq!(duplicates[0].state, ReceiptState::Accepted);
    for receipt in &duplicates {
        assert_eq!(receipt.created_at_unix_ms, receipt.updated_at_unix_ms);
        assert_eq!(receipt.created_at_unix_ms, receipt.heartbeat_at_unix_ms);
    }

    let first_version = ReceiptVersion::new(duplicates[0].receipt_id, 0);
    let left = {
        let store = Arc::clone(&store);
        let lease_deadline = plus(start, 70_000)?;
        tokio::spawn(async move { store.mark_buffered(owner, &[first_version], lease_deadline).await })
    };
    let right = {
        let store = Arc::clone(&store);
        let lease_deadline = plus(start, 70_000)?;
        tokio::spawn(async move { store.mark_buffered(owner, &[first_version], lease_deadline).await })
    };
    let raced = [left.await?, right.await?];
    assert_eq!(raced.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        raced
            .iter()
            .filter_map(|result| result.as_ref().err())
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing optimistic race loser"))?
            .kind(),
        ReceiptErrorKind::Conflict
    );

    let second = store
        .mark_buffered(
            owner,
            &[ReceiptVersion::new(duplicates[1].receipt_id, 0)],
            plus(start, 70_000)?,
        )
        .await?;
    assert_eq!(second[0].revision, 1);
    let retryable = ReceiptFailure::new(
        FailureDisposition::Retryable,
        ErrorStatus::new(ErrorCode::PublicationFailed, "live retry race")?,
    )?;
    let first_buffered = store
        .receipt(duplicates[0].receipt_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("first raced receipt missing"))?;
    let first_failure_version = ReceiptVersion::new(first_buffered.receipt_id, first_buffered.revision);
    let failed_at = now()?;
    let left = {
        let store = Arc::clone(&store);
        let retryable = retryable.clone();
        tokio::spawn(async move {
            store
                .fail_batch(owner, &[first_failure_version], &retryable, failed_at)
                .await
        })
    };
    let right = {
        let store = Arc::clone(&store);
        let retryable = retryable.clone();
        tokio::spawn(async move {
            store
                .fail_batch(owner, &[first_failure_version], &retryable, failed_at)
                .await
        })
    };
    let retry_race = [left.await?, right.await?];
    assert_eq!(retry_race.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        retry_race
            .iter()
            .filter_map(|result| result.as_ref().err())
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing retry race loser"))?
            .kind(),
        ReceiptErrorKind::Conflict
    );
    store
        .fail_batch(
            owner,
            &[ReceiptVersion::new(second[0].receipt_id, second[0].revision)],
            &retryable,
            now()?,
        )
        .await?;

    let publication_receipts = store
        .create_flight_batch(
            owner,
            &lease,
            plus(start, 80_000)?,
            &[delivery.clone(), delivery.clone()],
        )
        .await?;
    let accepted_versions: Vec<_> = publication_receipts
        .iter()
        .map(|record| ReceiptVersion::new(record.receipt_id, record.revision))
        .collect();
    let buffered = store
        .mark_buffered(owner, &accepted_versions, plus(start, 90_000)?)
        .await?;
    let buffered_versions: Vec<_> = buffered
        .iter()
        .map(|record| ReceiptVersion::new(record.receipt_id, record.revision))
        .collect();
    let flushing = store
        .mark_flushing(owner, &buffered_versions, plus(start, 100_000)?)
        .await?;
    let flushing_versions: Vec<_> = flushing
        .iter()
        .map(|record| ReceiptVersion::new(record.receipt_id, record.revision))
        .collect();
    let published_at = now()?;
    let published = store
        .publish_commit(
            owner,
            &PublicationBatch {
                receipts: flushing_versions,
                publication: DeltaPublicationResult::new(
                    Measurement::new(42)?,
                    Measurement::new(1)?,
                    Measurement::new(6)?,
                    Measurement::new(128)?,
                    delivery.schema_fingerprint(),
                    published_at,
                    lease.publication_fence(),
                ),
            },
        )
        .await?;
    assert_eq!(published.len(), 2);
    assert!(published.iter().all(|record| record.can_client_purge()));
    assert!(published.iter().all(|record| {
        record
            .publication
            .as_ref()
            .is_some_and(|result| result.delta_version().get() == 42)
    }));

    let expiring = store
        .create_flight_batch(owner, &lease, plus(now()?, 100)?, std::slice::from_ref(&delivery))
        .await?;
    tokio::time::sleep(Duration::from_millis(160)).await;
    let reconciled = store.reconcile_expired(now()?, 16).await?;
    assert!(
        reconciled
            .reconciled
            .iter()
            .any(|record| record.receipt_id == expiring[0].receipt_id
                && record.failure_disposition == Some(arrowcol_receipts::FailureDisposition::Retryable)
                && !record.can_client_purge())
    );

    let uncertain = store
        .create_flight_batch(owner, &lease, plus(now()?, 100)?, std::slice::from_ref(&delivery))
        .await?;
    let uncertain = store
        .mark_buffered(
            owner,
            &[ReceiptVersion::new(uncertain[0].receipt_id, uncertain[0].revision)],
            plus(now()?, 100)?,
        )
        .await?;
    let uncertain = store
        .mark_flushing(
            owner,
            &[ReceiptVersion::new(uncertain[0].receipt_id, uncertain[0].revision)],
            plus(now()?, 100)?,
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(160)).await;
    let reconciled = store.reconcile_expired(now()?, 16).await?;
    assert!(reconciled.reconciled.iter().any(|record| {
        record.receipt_id == uncertain[0].receipt_id
            && record.state == arrowcol_protocol::ReceiptState::PublicationUnknown
            && record.failure_disposition == Some(arrowcol_receipts::FailureDisposition::Permanent)
            && !record.can_client_purge()
    }));

    let statuses = store
        .receipts(&[
            duplicates[0].receipt_id,
            duplicates[1].receipt_id,
            publication_receipts[0].receipt_id,
            publication_receipts[1].receipt_id,
            expiring[0].receipt_id,
            uncertain[0].receipt_id,
        ])
        .await?;
    assert_eq!(statuses.len(), 6);
    assert!(statuses.iter().all(Option::is_some));
    hot_query_and_append_only_checks(owner).await?;

    let drain = store.start_owner_drain(owner).await?;
    assert_eq!(drain.active_receipts, 0);
    assert_eq!(drain.active_table_leases, 1);
    store.finish_owner_drain(owner, now()?).await?;
    if let Some(pool) = pool {
        let stats = pool.stats();
        assert!(stats.reused > 0, "successful receipt operations must reuse sessions");
        assert!(
            stats.discarded > 0,
            "failed receipt compare-and-set must discard sessions"
        );
        assert!(stats.size <= 2);
    }
    Ok(())
}

struct Fixture {
    validation: arrowcol_catalog::ValidatedIncomingSchema,
}

async fn fixture() -> anyhow::Result<Fixture> {
    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("receipt_{}", &suffix[..12]);
    let store = Arc::new(MemoryCatalogStore::new());
    let service = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        Arc::new(MemoryMetastore::new()),
    );
    let completion = service
        .execute_ddl(FlightSqlDdlRequest::new(format!(
            "CREATE TABLE telemetry.receipts.{table_name} (value BIGINT NOT NULL)"
        )))
        .await?;
    let schema = store
        .current_schema(completion.identity())
        .await?
        .ok_or_else(|| anyhow::anyhow!("live catalog schema missing"))?
        .arrow_schema()?;
    let validation = SchemaValidator::new(store)
        .validate(
            completion.identity(),
            ClaimedSchemaFingerprint::new(fingerprint_schema(&schema)),
            &schema,
        )
        .await?;
    Ok(Fixture { validation })
}

async fn seed_catalog(fixture: &Fixture) -> anyhow::Result<()> {
    let schema = fixture.validation.registered();
    let table_id = schema.table_id();
    let saga_id = Uuid::new_v4();
    let request_id = Uuid::new_v4();
    let version = i64::try_from(schema.version().get())?;
    let fingerprint = schema.fingerprint();
    let digest: &[u8] = fingerprint.as_bytes();
    let schema_json = serde_json::to_string(schema)?;
    let request_digest = [7_u8; 32];
    let mut client = connect().await?;
    client.begin_transaction().await?;
    let result: anyhow::Result<()> = async {
        client
            .execute(
                "INSERT INTO arrowcol.catalog_tables (table_id, catalog_name, schema_name, table_name) \
                 VALUES (@P1, @P2, @P3, @P4);",
                &[
                    &table_id,
                    &schema.identity().catalog().to_string(),
                    &schema.identity().schema().to_string(),
                    &schema.identity().table().to_string(),
                ],
            )
            .await?;
        client
            .execute(
                "INSERT INTO arrowcol.schema_change_sagas ( \
                    saga_id, table_id, request_id, initiated_by_instance_id, from_schema_version, \
                    target_schema_version, state, current_step, attempt_count, completed_at_utc, \
                    request_sha256, operation_kind \
                 ) VALUES (@P1, @P2, @P3, NULL, NULL, @P4, 'committed', 4, 0, SYSUTCDATETIME(), @P5, 'create');",
                &[&saga_id, &table_id, &request_id, &version, &&request_digest[..]],
            )
            .await?;
        client
            .execute(
                "INSERT INTO arrowcol.table_schema_versions ( \
                    table_id, schema_version, schema_sha256, compatibility, state, saga_id, \
                    activated_at_utc, canonical_ddl, schema_json \
                 ) VALUES (@P1, @P2, @P3, 'exact', 'active', @P4, SYSUTCDATETIME(), @P5, @P6);",
                &[
                    &table_id,
                    &version,
                    &digest,
                    &saga_id,
                    &schema.canonical_ddl(),
                    &schema_json,
                ],
            )
            .await?;
        client
            .execute(
                "INSERT INTO arrowcol.table_schema_heads (table_id, current_schema_version) VALUES (@P1, @P2);",
                &[&table_id, &version],
            )
            .await?;
        Ok(())
    }
    .await;
    match result {
        Ok(()) => {
            client.commit_transaction().await?;
            Ok(())
        }
        Err(error) => {
            let _rollback = client.rollback_transaction().await;
            Err(error)
        }
    }
}

async fn hot_query_and_append_only_checks(owner: CollectorInstanceId) -> anyhow::Result<()> {
    let mut client = connect().await?;
    let owner = owner.as_uuid();
    client
        .query(
            "SET ANSI_NULLS ON; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; SET ARITHABORT ON; \
             SET CONCAT_NULL_YIELDS_NULL ON; SET QUOTED_IDENTIFIER ON; SET NUMERIC_ROUNDABORT OFF; \
             SELECT TOP (8) receipt_id, state, revision \
             FROM arrowcol.delivery_receipts WITH (INDEX(IX_delivery_receipts_owner_active)) \
             WHERE owner_instance_id = @P1 AND state IN ('accepted', 'buffered', 'flushing') \
             ORDER BY receipt_lease_deadline_utc, receipt_id;",
            &[&owner],
        )
        .await?
        .into_first_result()
        .await?;
    client
        .query(
            "SELECT TOP (8) r.receipt_id, r.owner_instance_id, r.revision, r.state \
             FROM arrowcol.delivery_receipts AS r WITH (INDEX(IX_delivery_receipts_expiry_active)) \
             WHERE r.receipt_lease_deadline_utc <= SYSUTCDATETIME() \
               AND r.state IN ('accepted', 'buffered', 'flushing') \
               AND NOT EXISTS ( \
                   SELECT 1 FROM arrowcol.collector_instances AS durable_owner \
                   WHERE durable_owner.instance_id = r.owner_instance_id \
                     AND durable_owner.durable_recovery_window_ms IS NOT NULL \
                     AND durable_owner.lease_expires_at_utc > \
                         DATEADD(millisecond, -durable_owner.durable_recovery_window_ms, SYSUTCDATETIME()) \
               ) \
             ORDER BY r.receipt_lease_deadline_utc, r.receipt_id;",
            &[],
        )
        .await?
        .into_first_result()
        .await?;
    let event = client
        .query(
            "SELECT TOP (1) receipt_event_id FROM arrowcol.receipt_events ORDER BY receipt_event_id DESC;",
            &[],
        )
        .await?
        .into_row()
        .await?
        .ok_or_else(|| anyhow::anyhow!("receipt event history is empty"))?;
    let event_id = required_i64(&event, "receipt_event_id")?;
    let update = client
        .execute(
            "UPDATE arrowcol.receipt_events SET event_detail = N'forbidden' WHERE receipt_event_id = @P1;",
            &[&event_id],
        )
        .await;
    assert!(
        update.is_err(),
        "append-only receipt event update unexpectedly succeeded"
    );
    Ok(())
}

fn sql_config() -> anyhow::Result<SqlServerReceiptConfig> {
    Ok(SqlServerReceiptConfig::with_password_file(
        std::env::var("ARROWCOL_SQLSERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned()),
        std::env::var("ARROWCOL_SQLSERVER_PORT")
            .ok()
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(14_333),
        "arrowcol",
        "arrowcol_app",
        secret_path(),
    )?
    .trust_server_certificate(true))
}

fn sql_pool() -> anyhow::Result<SqlPool> {
    let host = std::env::var("ARROWCOL_SQLSERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("ARROWCOL_SQLSERVER_PORT")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(14_333);
    let password = std::fs::read_to_string(secret_path())?;
    Ok(SqlPool::new(
        ConnectionConfig::new(
            host,
            port,
            "arrowcol",
            "arrowcol_app",
            password.trim_end_matches(['\r', '\n']),
        )
        .trust_server_certificate(true),
        PoolConfig {
            max_size: 2,
            acquire_timeout: Duration::from_secs(5),
        },
    )?)
}

async fn connect() -> anyhow::Result<SqlClient> {
    let host = std::env::var("ARROWCOL_SQLSERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("ARROWCOL_SQLSERVER_PORT")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(14_333);
    let password_path = secret_path();
    let password = std::fs::read_to_string(password_path)?;
    let password = password.trim_end_matches(['\r', '\n']).to_owned();
    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.database("arrowcol");
    config.authentication(AuthMethod::sql_server("arrowcol_app", password));
    config.encryption(EncryptionLevel::Required);
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Ok(Client::connect(config, tcp.compat_write()).await?)
}

fn secret_path() -> PathBuf {
    std::env::var_os("ARROWCOL_APP_PASSWORD_FILE").map_or_else(
        || {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../projects/arrowcol-infra/secrets/arrowcol-app-password.txt")
        },
        PathBuf::from,
    )
}

fn required_i64(row: &Row, column: &str) -> anyhow::Result<i64> {
    row.try_get::<i64, _>(column)?
        .ok_or_else(|| anyhow::anyhow!("{column} was NULL"))
}

fn required_uuid(row: &Row, column: &str) -> anyhow::Result<Uuid> {
    row.try_get::<Uuid, _>(column)?
        .ok_or_else(|| anyhow::anyhow!("{column} was NULL"))
}

fn required_text(row: &Row, column: &str) -> anyhow::Result<String> {
    Ok(row
        .try_get::<&str, _>(column)?
        .ok_or_else(|| anyhow::anyhow!("{column} was NULL"))?
        .to_owned())
}

/// One persisted `adopted` receipt event.
struct AdoptionEvent {
    receipt_id: Uuid,
    revision: i64,
    from_state: String,
    actor: Uuid,
    previous_owner: Uuid,
}

async fn adoption_events(receipts: &[ReceiptId]) -> anyhow::Result<Vec<AdoptionEvent>> {
    let ids: Vec<Uuid> = receipts.iter().map(|id| id.as_uuid()).collect();
    let placeholders = (1..=ids.len())
        .map(|index| format!("@P{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let parameters: Vec<&dyn ToSql> = ids.iter().map(|id| id as &dyn ToSql).collect();
    let mut client = connect().await?;
    let rows = client
        .query(
            format!(
                "SELECT receipt_id, revision, from_state, actor_instance_id, previous_owner_instance_id \
                 FROM arrowcol.receipt_events \
                 WHERE to_state = 'adopted' AND receipt_id IN ({placeholders}) \
                 ORDER BY receipt_event_id;"
            ),
            &parameters,
        )
        .await?
        .into_first_result()
        .await?;
    rows.iter()
        .map(|row| {
            Ok(AdoptionEvent {
                receipt_id: required_uuid(row, "receipt_id")?,
                revision: required_i64(row, "revision")?,
                from_state: required_text(row, "from_state")?,
                actor: required_uuid(row, "actor_instance_id")?,
                previous_owner: required_uuid(row, "previous_owner_instance_id")?,
            })
        })
        .collect()
}

/// Returns an owner's state, whether it stopped, and its recovery window.
async fn owner_row(instance_id: CollectorInstanceId) -> anyhow::Result<(String, bool, Option<i32>)> {
    let mut client = connect().await?;
    let row = client
        .query(
            "SELECT state, CAST(CASE WHEN stopped_at_utc IS NULL THEN 0 ELSE 1 END AS bit) AS stopped, \
                    durable_recovery_window_ms \
             FROM arrowcol.collector_instances WHERE instance_id = @P1;",
            &[&instance_id.as_uuid()],
        )
        .await?
        .into_row()
        .await?
        .ok_or_else(|| anyhow::anyhow!("collector instance is missing"))?;
    Ok((
        required_text(&row, "state")?,
        row.try_get::<bool, _>("stopped")?
            .ok_or_else(|| anyhow::anyhow!("stopped was NULL"))?,
        row.try_get::<i32, _>("durable_recovery_window_ms")?,
    ))
}

async fn register_live_owner(
    store: &SqlServerReceiptStore,
    instance_id: CollectorInstanceId,
    instance_key: &str,
    generation: u64,
    ttl_ms: u64,
    durable_recovery_window_ms: Option<u64>,
) -> anyhow::Result<UnixMillis> {
    let started = now()?;
    let deadline = plus(started, ttl_ms)?;
    store
        .register_owner(OwnerRegistration {
            instance_id,
            instance_key: instance_key.to_owned(),
            generation,
            host_name: Some("live-durable".to_owned()),
            build_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
            started_at_unix_ms: started,
            lease_deadline_unix_ms: deadline,
            durable_recovery_window_ms,
        })
        .await?;
    Ok(deadline)
}

async fn acquire_live_lease(
    store: &SqlServerReceiptStore,
    fixture: &Fixture,
    owner: CollectorInstanceId,
    expires_at: UnixMillis,
) -> anyhow::Result<TableLease> {
    let registered = fixture.validation.registered();
    Ok(store
        .acquire_table_lease(TableLeaseRequest {
            table_id: registered.table_id(),
            table: registered.identity().clone(),
            owner_instance_id: owner,
            implementation_pool: ImplementationPool::Direct,
            issued_at_unix_ms: now()?,
            expires_at_unix_ms: expires_at,
        })
        .await?)
}

/// Builds an adoption whose deadline is 30 seconds out, capped by the lease.
fn adoption(
    successor: CollectorInstanceId,
    lease: &TableLease,
    adopted_at: UnixMillis,
    receipts: Vec<AdoptionCandidate>,
) -> anyhow::Result<ReceiptAdoption> {
    Ok(ReceiptAdoption {
        successor,
        lease: lease.clone(),
        lease_deadline: plus(adopted_at, 30_000)?.min(lease.expires_at_unix_ms()),
        adopted_at,
        receipts,
    })
}

fn rejections(records: &[ReceiptRecord]) -> Vec<RejectedAdoption> {
    records
        .iter()
        .map(|record| RejectedAdoption {
            receipt_id: record.receipt_id,
            current: Some(record.clone()),
        })
        .collect()
}

fn version(record: &ReceiptRecord) -> ReceiptVersion {
    ReceiptVersion::new(record.receipt_id, record.revision)
}

fn versions(records: &[ReceiptRecord]) -> Vec<ReceiptVersion> {
    records.iter().map(version).collect()
}

const fn sql_state(state: ReceiptState) -> &'static str {
    match state {
        ReceiptState::Accepted => "accepted",
        ReceiptState::Buffered => "buffered",
        ReceiptState::Publishing => "flushing",
        ReceiptState::Published => "published",
        ReceiptState::Failed | ReceiptState::PublicationUnknown => "failed",
    }
}

/// Reconciles expired receipts across the database until a batch is not full.
async fn reconcile_all(store: &SqlServerReceiptStore) -> anyhow::Result<Vec<ReceiptRecord>> {
    let mut reconciled = Vec::new();
    for _ in 0..64 {
        let report = store.reconcile_expired(now()?, 64).await?;
        reconciled.extend(report.reconciled);
        if !report.limit_reached {
            return Ok(reconciled);
        }
    }
    anyhow::bail!("expired receipt reconciliation did not drain")
}

async fn sleep_until(instant: UnixMillis) -> anyhow::Result<()> {
    let remaining = instant.get().saturating_sub(now()?.get());
    tokio::time::sleep(Duration::from_millis(remaining)).await;
    Ok(())
}

fn now() -> anyhow::Result<UnixMillis> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(UnixMillis::new(u64::try_from(millis)?)?)
}

fn minus(value: UnixMillis, millis: u64) -> anyhow::Result<UnixMillis> {
    Ok(UnixMillis::new(
        value
            .get()
            .checked_sub(millis)
            .ok_or_else(|| anyhow::anyhow!("timestamp underflow"))?,
    )?)
}

fn plus(value: UnixMillis, millis: u64) -> anyhow::Result<UnixMillis> {
    Ok(UnixMillis::new(
        value
            .get()
            .checked_add(millis)
            .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))?,
    )?)
}
