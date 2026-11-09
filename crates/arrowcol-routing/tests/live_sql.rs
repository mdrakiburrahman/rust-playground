use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrowcol_protocol::{
    BasisPoints, CatalogName, ClaimedSchemaFingerprint, CollectorInstanceId, ImplementationPool, IngestMetadata,
    Measurement, PositiveMeasurement, PressureGauge, ProtocolVersion, SchemaFingerprint, SchemaName, TableIdentity,
    TableName, UnixMillis,
};
use arrowcol_routing::{
    CollectorRegistration, CollectorState, LeaseCas, LeaseClaim, LeaseKey, LeaseState, LeaseSuccession,
    RichPressureSnapshot, RouteSource, RoutingEngine, RoutingError, RoutingErrorKind, RoutingPolicy, RoutingStore,
    SqlServerRoutingConfig, SqlServerRoutingStore, SubsystemState, TablePressure,
};
use arrowcol_sql_server::{ConnectionConfig, PoolConfig, SqlConnection, SqlPool};
use uuid::Uuid;

static LIVE_ROUTING_CASE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Copy)]
enum LiveOatPinCase {
    RetryAndPermissions,
    MissingLease,
    DifferentValidOwner,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server and migrator credential"]
async fn live_sql_trusted_oat_initializer_preserves_set_expiry_and_app_permissions() -> anyhow::Result<()> {
    live_oat_pin_case(LiveOatPinCase::RetryAndPermissions).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server and migrator credential"]
async fn live_sql_immutable_oat_routing_rejects_missing_bound_lease() -> anyhow::Result<()> {
    live_oat_pin_case(LiveOatPinCase::MissingLease).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server and migrator credential"]
async fn live_sql_immutable_oat_routing_rejects_different_valid_lease_owner() -> anyhow::Result<()> {
    live_oat_pin_case(LiveOatPinCase::DifferentValidOwner).await
}

async fn live_oat_pin_case(case: LiveOatPinCase) -> anyhow::Result<()> {
    if std::env::var("ARROWCOL_SQLSERVER_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    let _isolation = LIVE_ROUTING_CASE.lock().await;
    let store = Arc::new(SqlServerRoutingStore::new(sql_config()?));
    let migrator_config = SqlServerRoutingConfig::with_password_file(
        std::env::var("ARROWCOL_SQLSERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned()),
        std::env::var("ARROWCOL_SQLSERVER_PORT")
            .ok()
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(14_333),
        "arrowcol",
        "arrowcol_migrator",
        migrator_secret_path(),
    )?
    .trust_server_certificate(true)
    .with_timeouts(Duration::from_secs(5), Duration::from_secs(15));
    let migrator: Arc<dyn RoutingStore> = Arc::new(SqlServerRoutingStore::new(migrator_config));
    let started = now()?;
    let expires_at = add(started, 240_000)?;
    let run_id = Uuid::new_v4().to_string();
    let instances = [CollectorInstanceId::new(), CollectorInstanceId::new()];
    let mut keys = ["a", "b"]
        .into_iter()
        .map(|suffix| {
            Ok(LeaseKey::new(
                ImplementationPool::Dataflow,
                TableIdentity::new(
                    CatalogName::new("telemetry")?,
                    SchemaName::new("live")?,
                    TableName::new(format!("oat_{}_{}", Uuid::new_v4().simple(), suffix))?,
                ),
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    keys.sort();
    let policy = RoutingPolicy {
        warmup_ms: 0,
        lease_ttl_ms: 300_000,
        oat_run_id: Some(run_id.clone()),
        ..RoutingPolicy::default()
    };
    let setup = RoutingEngine::new(migrator, policy.clone())?;
    let mut result: anyhow::Result<()> = async {
        for (instance, port) in instances.into_iter().zip([51_011, 51_012]) {
            store
                .register_collector(&live_registration(
                    instance,
                    format!("live-oat-{}", Uuid::new_v4().simple()),
                    port,
                    subtract(started, 1_000)?,
                    add(started, 600_000)?,
                )?)
                .await?;
            store
                .publish_pressure(&snapshot(instance, started, 500, 0, None)?)
                .await?;
        }
        assert_app_pin_permissions().await?;
        let pins = setup
            .initialize_run_table_pins(&run_id, &keys, expires_at, now()?)
            .await?;
        anyhow::ensure!(pins.len() == keys.len(), "trusted setup did not bind the full set");
        let app_store: Arc<dyn RoutingStore> = Arc::<SqlServerRoutingStore>::clone(&store);
        let app = RoutingEngine::new(app_store, policy.clone())?;
        let mut selected = Vec::with_capacity(keys.len());
        for (key, pin) in keys.iter().zip(&pins) {
            anyhow::ensure!(
                pin.key == *key && pin.expires_at_unix_ms == expires_at,
                "setup changed key or expiry"
            );
            anyhow::ensure!(
                instances.contains(&pin.holder_instance_id),
                "setup selected a foreign collector"
            );
            anyhow::ensure!(
                store.get_run_table_pin(&run_id, key, now()?).await?.as_ref() == Some(pin),
                "application credential could not read the immutable pin"
            );
            let route = app
                .route_ingest_for_run(&oat_metadata(key), &run_id, expires_at, now()?)
                .await?;
            let lease = route
                .lease
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("OAT route has no lease"))?;
            anyhow::ensure!(
                route.instance_id == pin.holder_instance_id
                    && lease.lease_id == pin.lease_id
                    && lease.fencing_epoch == pin.fencing_epoch,
                "application routing did not use the trusted immutable binding"
            );
            let revalidated = app.revalidate_run_assignment(&run_id, &route, now()?).await?;
            anyhow::ensure!(
                revalidated.instance_id == route.instance_id && revalidated.flight_endpoint == route.flight_endpoint,
                "forwarding revalidation changed the selected collector"
            );
            selected.push(route);
        }
        let authority = pin_authority_snapshot(&run_id).await?;
        match case {
            LiveOatPinCase::RetryAndPermissions => {
                let retry = setup
                    .initialize_run_table_pins(&run_id, &keys, expires_at, now()?)
                    .await?;
                anyhow::ensure!(retry == pins, "same-set retry changed immutable pins");
                for changed_expiry in [subtract(expires_at, 1)?, add(expires_at, 1)?] {
                    let changed = setup
                        .initialize_run_table_pins(&run_id, &keys, changed_expiry, now()?)
                        .await;
                    anyhow::ensure!(
                        matches!(changed, Err(error) if error.kind() == RoutingErrorKind::Conflict),
                        "trusted retry accepted a different immutable expiry"
                    );
                }
                let changed_set = setup
                    .initialize_run_table_pins(&run_id, &keys[..1], expires_at, now()?)
                    .await;
                anyhow::ensure!(
                    matches!(changed_set, Err(error) if error.kind() == RoutingErrorKind::Conflict),
                    "trusted retry accepted a different immutable table set"
                );
                let mut changed_keys = keys.clone();
                changed_keys[0].table = TableIdentity::new(
                    CatalogName::new("telemetry")?,
                    SchemaName::new("live")?,
                    TableName::new(format!("oat_changed_{}", Uuid::new_v4().simple()))?,
                );
                changed_keys.sort();
                let changed_digest = setup
                    .initialize_run_table_pins(&run_id, &changed_keys, expires_at, now()?)
                    .await;
                anyhow::ensure!(
                    matches!(changed_digest, Err(error) if error.kind() == RoutingErrorKind::Conflict),
                    "trusted retry accepted different identities with the same table count"
                );
                let denied_run = Uuid::new_v4().to_string();
                let denied_store: Arc<dyn RoutingStore> = Arc::<SqlServerRoutingStore>::clone(&store);
                let denied_setup = RoutingEngine::new(
                    denied_store,
                    RoutingPolicy {
                        oat_run_id: Some(denied_run.clone()),
                        ..policy.clone()
                    },
                )?;
                let denied = denied_setup
                    .initialize_run_table_pins(&denied_run, &keys, expires_at, now()?)
                    .await;
                let Err(error) = denied else {
                    anyhow::bail!("application credential initialized a new immutable run");
                };
                anyhow::ensure!(
                    error.kind() == RoutingErrorKind::Database
                        && error.to_string().to_ascii_lowercase().contains("permission"),
                    "application initialization did not fail on SQL permission denial: {error}"
                );
                assert_pin_authority_absent(&denied_run).await?;
                let expired = store.get_run_table_pin(&run_id, &keys[0], expires_at).await;
                anyhow::ensure!(
                    matches!(expired, Err(error) if error.kind() == RoutingErrorKind::Conflict),
                    "application read accepted an expired immutable run"
                );
                let expired_route = app
                    .route_ingest_for_run(&oat_metadata(&keys[0]), &run_id, expires_at, expires_at)
                    .await;
                anyhow::ensure!(
                    matches!(expired_route, Err(error) if error.kind() == RoutingErrorKind::InvalidConfiguration),
                    "application router accepted a request at the immutable expiry"
                );
                let expired_forward = app.revalidate_run_assignment(&run_id, &selected[0], expires_at).await;
                anyhow::ensure!(
                    matches!(expired_forward, Err(error) if error.kind() == RoutingErrorKind::Conflict),
                    "application router forwarded an earlier assignment after immutable expiry"
                );
            }
            LiveOatPinCase::MissingLease | LiveOatPinCase::DifferentValidOwner => {
                let key = &keys[0];
                let pin = &pins[0];
                if matches!(case, LiveOatPinCase::MissingLease) {
                    delete_fixture_lease(&key.table).await?;
                } else {
                    let replacement = if pin.holder_instance_id == instances[0] {
                        instances[1]
                    } else {
                        instances[0]
                    };
                    replace_fixture_lease_owner(&key.table, replacement, pin.lease_id.as_uuid()).await?;
                }
                let route_at = now()?;
                let before_route = store.load_view(route_at).await?;
                if matches!(case, LiveOatPinCase::MissingLease) {
                    anyhow::ensure!(!before_route.leases.contains_key(key), "fixture lease was not deleted");
                } else {
                    let replacement = before_route
                        .leases
                        .get(key)
                        .ok_or_else(|| anyhow::anyhow!("replacement lease is missing"))?;
                    let ranked = arrowcol_routing::rank_collectors(&before_route, key, route_at, &policy, false);
                    anyhow::ensure!(
                        replacement.is_routable_at(route_at)
                            && replacement.holder_instance_id != pin.holder_instance_id
                            && replacement.lease_id != pin.lease_id
                            && replacement.fencing_epoch > pin.fencing_epoch
                            && ranked.contains(&replacement.holder_instance_id),
                        "replacement is not a genuinely valid different owner at routing time"
                    );
                }
                let fresh_store: Arc<dyn RoutingStore> = Arc::<SqlServerRoutingStore>::clone(&store);
                let fresh = RoutingEngine::new(fresh_store, policy.clone())?;
                let rejected = fresh
                    .route_ingest_for_run(&oat_metadata(key), &run_id, expires_at, route_at)
                    .await;
                anyhow::ensure!(
                    matches!(rejected, Err(error) if error.kind() == RoutingErrorKind::Conflict),
                    "fresh application router accepted missing or replaced bound lease"
                );
                let rejected_forward = app.revalidate_run_assignment(&run_id, &selected[0], route_at).await;
                anyhow::ensure!(
                    matches!(rejected_forward, Err(error) if error.kind() == RoutingErrorKind::Conflict),
                    "application router forwarded a cached assignment after lease loss or owner replacement"
                );
                anyhow::ensure!(
                    store.load_view(route_at).await?.leases == before_route.leases,
                    "failed immutable route created or rewrote a lease"
                );
            }
        }
        anyhow::ensure!(
            pin_authority_snapshot(&run_id).await? == authority,
            "retry, rejected route or expiry check changed immutable authority"
        );
        Ok(())
    }
    .await;
    // V0011 authority tombstones deliberately survive ordinary fixture cleanup.
    for key in &keys {
        result = finish_fixture(&store, result, &instances, &key.table, now()?).await;
    }
    result
}

fn oat_metadata(key: &LeaseKey) -> IngestMetadata {
    IngestMetadata::new(
        ProtocolVersion::CURRENT,
        ImplementationPool::Dataflow,
        key.table.clone(),
        ClaimedSchemaFingerprint::new(SchemaFingerprint::from_bytes([19; 32])),
    )
}

async fn assert_app_pin_permissions() -> anyhow::Result<()> {
    let mut connection = SqlConnection::connect(&sql_connection_config()?).await?;
    let row = connection
        .query(
            "SELECT USER_NAME() AS principal, \
         SUM(CONVERT(int, HAS_PERMS_BY_NAME(objects.name, 'OBJECT', actions.name))) AS writes, \
         COUNT(HAS_PERMS_BY_NAME(objects.name, 'OBJECT', actions.name)) AS evaluated \
         FROM (VALUES ('arrowcol.routing_run_pin_sets'), ('arrowcol.routing_run_table_initializations'), \
                      ('arrowcol.routing_run_table_pins')) objects(name) \
         CROSS JOIN (VALUES ('INSERT'), ('UPDATE'), ('DELETE')) actions(name);",
            &[],
        )
        .await?
        .into_row()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing application permission result"))?;
    anyhow::ensure!(
        row.try_get::<&str, _>("principal")? == Some("arrowcol_app"),
        "wrong application SQL identity"
    );
    anyhow::ensure!(
        row.try_get::<i32, _>("evaluated")? == Some(9),
        "SQL did not evaluate all authority write permissions"
    );
    anyhow::ensure!(
        row.try_get::<i32, _>("writes")? == Some(0),
        "application has immutable-authority write privilege"
    );
    connection.finish().await?;
    Ok(())
}

async fn assert_pin_authority_absent(run_id: &str) -> anyhow::Result<()> {
    let mut connection = SqlConnection::connect(&sql_connection_config()?).await?;
    let run_id = Uuid::parse_str(run_id)?;
    let row = connection
        .query(
            "SELECT \
             (SELECT COUNT_BIG(*) FROM arrowcol.routing_run_pin_sets WHERE run_id=@P1) AS sets, \
             (SELECT COUNT_BIG(*) FROM arrowcol.routing_run_table_initializations WHERE run_id=@P1) AS markers, \
             (SELECT COUNT_BIG(*) FROM arrowcol.routing_run_table_pins WHERE run_id=@P1) AS pins;",
            &[&run_id],
        )
        .await?
        .into_row()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing denied-run authority counts"))?;
    for field in ["sets", "markers", "pins"] {
        let count = row
            .try_get::<i64, _>(field)?
            .ok_or_else(|| anyhow::anyhow!("NULL denied-run {field} count"))?;
        anyhow::ensure!(count == 0, "denied setup left {count} authority rows in {field}");
    }
    connection.finish().await?;
    Ok(())
}

async fn pin_authority_snapshot(run_id: &str) -> anyhow::Result<String> {
    let mut connection = SqlConnection::connect(&sql_connection_config()?).await?;
    let run_id = Uuid::parse_str(run_id)?;
    let row = connection
        .query(
            "SELECT (SELECT s.*, \
         JSON_QUERY((SELECT m.* FROM arrowcol.routing_run_table_initializations m WHERE m.run_id=s.run_id \
                     ORDER BY implementation_pool,catalog_name,schema_name,table_name FOR JSON PATH)) AS markers, \
         JSON_QUERY((SELECT p.* FROM arrowcol.routing_run_table_pins p WHERE p.run_id=s.run_id \
                     ORDER BY implementation_pool,catalog_name,schema_name,table_name FOR JSON PATH)) AS pins \
         FROM arrowcol.routing_run_pin_sets s WHERE s.run_id=@P1 FOR JSON PATH) AS authority;",
            &[&run_id],
        )
        .await?
        .into_row()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing immutable authority snapshot"))?;
    let value = row
        .try_get::<&str, _>("authority")?
        .ok_or_else(|| anyhow::anyhow!("NULL immutable authority snapshot"))?
        .to_owned();
    connection.finish().await?;
    Ok(value)
}

async fn replace_fixture_lease_owner(
    table: &TableIdentity,
    replacement: CollectorInstanceId,
    original_lease_id: Uuid,
) -> anyhow::Result<()> {
    // Model out-of-band lease corruption without rewriting immutable authority.
    let mut connection = SqlConnection::connect(&sql_connection_config()?).await?;
    connection
        .execute(
            "UPDATE arrowcol.routing_table_leases SET holder_instance_id=@P4, lease_id=NEWID(), \
         fencing_epoch=fencing_epoch+1, lease_version=lease_version+1 \
         WHERE implementation_pool='dataflow' AND catalog_name=@P1 AND schema_name=@P2 \
           AND table_name=@P3 AND lease_id=@P5;",
            &[
                &table.catalog().as_str(),
                &table.schema().as_str(),
                &table.table().as_str(),
                &replacement.as_uuid(),
                &original_lease_id,
            ],
        )
        .await?;
    connection.finish().await?;
    Ok(())
}

fn migrator_secret_path() -> PathBuf {
    std::env::var_os("ARROWCOL_MIGRATOR_PASSWORD_FILE").map_or_else(
        || {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../projects/arrowcol-infra/secrets/arrowcol-migrator-password.txt")
        },
        PathBuf::from,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server"]
async fn live_sql_coordinates_router_races_and_fenced_drain_transfer() -> anyhow::Result<()> {
    live_routing_case(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server"]
async fn pooled_live_sql_reuses_after_router_races_and_rollback() -> anyhow::Result<()> {
    live_routing_case(true).await
}

async fn live_routing_case(pooled: bool) -> anyhow::Result<()> {
    if std::env::var("ARROWCOL_SQLSERVER_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    // Routing eligibility includes collectors from every fixture in the database.
    let _isolation = LIVE_ROUTING_CASE.lock().await;
    let pool = pooled.then(sql_pool).transpose()?;
    let mut store = SqlServerRoutingStore::new(sql_config()?);
    if let Some(pool) = &pool {
        store = store.with_pool(pool.clone())?;
    }
    let store = Arc::new(store.clone());
    let now = now()?;
    let first = CollectorInstanceId::new();
    let second = CollectorInstanceId::new();
    let table = TableIdentity::new(
        CatalogName::new("telemetry")?,
        SchemaName::new("live")?,
        TableName::new(format!("route_{}", &Uuid::new_v4().simple().to_string()[..12]))?,
    );
    let key = LeaseKey {
        implementation_pool: ImplementationPool::Dataflow,
        table: table.clone(),
    };
    let result: anyhow::Result<()> = async {
        for (instance, port) in [(first, 51_001), (second, 51_002)] {
            store
                .register_collector(&CollectorRegistration {
                    instance_id: instance,
                    instance_key: format!("live-router-{}", Uuid::new_v4().simple()),
                    implementation_pool: ImplementationPool::Dataflow,
                    flight_endpoint: format!("127.0.0.1:{port}").parse()?,
                    catalog_capable: true,
                    max_concurrent_streams: positive(64)?,
                    started_at_unix_ms: subtract(now, 1_000)?,
                    heartbeat_at_unix_ms: now,
                    lease_expires_at_unix_ms: add(now, 180_000)?,
                })
                .await?;
            store.publish_pressure(&snapshot(instance, now, 500, 0, None)?).await?;
        }

        let metadata = IngestMetadata::new(
            ProtocolVersion::CURRENT,
            ImplementationPool::Dataflow,
            table.clone(),
            ClaimedSchemaFingerprint::new(SchemaFingerprint::from_bytes([11; 32])),
        );
        let policy = RoutingPolicy {
            warmup_ms: 0,
            lease_ttl_ms: 60_000,
            ..RoutingPolicy::default()
        };
        let left_store: Arc<dyn RoutingStore> = Arc::<SqlServerRoutingStore>::clone(&store);
        let right_store: Arc<dyn RoutingStore> = Arc::<SqlServerRoutingStore>::clone(&store);
        let left = Arc::new(RoutingEngine::new(left_store, policy.clone())?);
        let right = Arc::new(RoutingEngine::new(right_store, policy)?);
        left.refresh(now).await?;
        right.refresh(now).await?;
        let claim_at = add(now, 1)?;

        let left_route = {
            let left = Arc::clone(&left);
            let metadata = metadata.clone();
            tokio::spawn(async move { left.route_ingest(&metadata, claim_at).await })
        };
        let right_route = {
            let right = Arc::clone(&right);
            let metadata = metadata.clone();
            tokio::spawn(async move { right.route_ingest(&metadata, claim_at).await })
        };
        let left_route = left_route.await??;
        let right_route = right_route.await??;
        anyhow::ensure!(
            left_route.lease == right_route.lease,
            "concurrent routers observed different leases"
        );
        anyhow::ensure!(
            left_route.instance_id == right_route.instance_id,
            "concurrent routers observed different holders"
        );
        let holder = left_route.instance_id;
        anyhow::ensure!(
            [first, second].contains(&holder),
            "lease holder is outside this fixture"
        );
        let initial_lease = left_route
            .lease
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("initial SQL route has no lease"))?;
        let view = store.load_view(claim_at).await?;
        anyhow::ensure!(
            view.leases.get(&key) == Some(initial_lease),
            "persisted winning lease differs from both router results"
        );
        anyhow::ensure!(
            active_lease_count(&table).await? == 1,
            "fixture does not have exactly one active SQL lease"
        );
        let fallback = if holder == first { second } else { first };

        store
            .publish_pressure(&snapshot(holder, add(now, 2)?, 9_900, 1, Some(table.clone()))?)
            .await?;
        left.rebalance(add(now, 3)?).await?;
        let view = store.load_view(add(now, 3)?).await?;
        let draining = view
            .leases
            .get(&key)
            .ok_or_else(|| anyhow::anyhow!("fixture lease disappeared during drain"))?;
        anyhow::ensure!(
            draining.state == LeaseState::Draining
                && draining.holder_instance_id == holder
                && draining.lease_id == initial_lease.lease_id
                && draining.fencing_epoch == initial_lease.fencing_epoch
                && draining.lease_version == initial_lease.lease_version + 1,
            "fixture drain changed lease identity, owner, or fencing incorrectly"
        );
        anyhow::ensure!(
            left.route_ingest(&metadata, add(now, 4)?).await.is_err(),
            "draining fixture accepted new work"
        );

        store
            .publish_pressure(&snapshot(holder, add(now, 5)?, 9_900, 0, Some(table.clone()))?)
            .await?;
        right.rebalance(add(now, 6)?).await?;
        let final_route = right.route_ingest(&metadata, add(now, 7)?).await?;
        anyhow::ensure!(
            final_route.instance_id == fallback,
            "fixture transferred to the wrong collector"
        );
        let view = store.load_view(add(now, 7)?).await?;
        let transferred = view
            .leases
            .get(&key)
            .ok_or_else(|| anyhow::anyhow!("fixture lease disappeared during transfer"))?;
        anyhow::ensure!(
            transferred.state == LeaseState::Active
                && transferred.holder_instance_id == fallback
                && transferred.fencing_epoch > initial_lease.fencing_epoch
                && transferred.lease_id != initial_lease.lease_id
                && final_route.lease.as_ref() == Some(transferred),
            "fixture transfer did not establish a single new fenced owner"
        );
        anyhow::ensure!(
            active_lease_count(&table).await? == 1,
            "transfer created multiple active SQL leases"
        );
        if let Some(pool) = &pool {
            anyhow::ensure!(
                store
                    .publish_pressure(&snapshot(first, now, 500, 0, None)?)
                    .await
                    .is_err(),
                "duplicate pressure publication unexpectedly succeeded"
            );
            let view = store.load_view(add(now, 8)?).await?;
            anyhow::ensure!(
                view.collectors.contains_key(&first),
                "healthy read after SQL error lost fixture collector"
            );
            let stats = pool.stats();
            anyhow::ensure!(stats.reused > 0, "successful routing operations must reuse sessions");
            anyhow::ensure!(
                stats.discarded > 0,
                "failed pressure publication must discard its session"
            );
            anyhow::ensure!(stats.size <= 2, "routing pool exceeded its configured bound");
        }
        Ok(())
    }
    .await;
    finish_fixture(&store, result, &[first, second], &table, add(now, 10)?).await
}

/// Scenario: a SQL-registered holder starts draining, its registration expires
/// without a drain proof, and its process restarts under the same instance key
/// beside healthy collectors whose keys differ only by case or a trailing space.
/// Guarantees: SQL rejects succession while the holder is live or to a near-miss
/// key, rebalance hands the lease to the exact-key restart with a new lease ID
/// and greater epoch, and cached routing follows the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server"]
async fn live_sql_hands_crashed_holder_lease_to_same_key_restart() -> anyhow::Result<()> {
    if std::env::var("ARROWCOL_SQLSERVER_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    // Routing eligibility includes collectors from every fixture in the database.
    let _isolation = LIVE_ROUTING_CASE.lock().await;
    let store = Arc::new(SqlServerRoutingStore::new(sql_config()?));
    let now = now()?;
    let crashed = CollectorInstanceId::new();
    let restart = CollectorInstanceId::new();
    let case_variant = CollectorInstanceId::new();
    let padded_variant = CollectorInstanceId::new();
    let restarted_key = format!("live-succession-{}", Uuid::new_v4().simple());
    let table = TableIdentity::new(
        CatalogName::new("telemetry")?,
        SchemaName::new("live")?,
        TableName::new(format!("succession_{}", &Uuid::new_v4().simple().to_string()[..12]))?,
    );
    let key = LeaseKey::new(ImplementationPool::Dataflow, table.clone());
    let result: anyhow::Result<()> = async {
        // The crashed registration closes before wall-clock now, so later
        // cases can never select it even though it never records a state.
        store
            .register_collector(&live_registration(
                crashed,
                restarted_key.clone(),
                51_011,
                subtract(now, 10_000)?,
                subtract(now, 5_000)?,
            )?)
            .await?;
        store
            .publish_pressure(&snapshot(crashed, subtract(now, 9_500)?, 500, 0, None)?)
            .await?;
        let initial = store
            .claim_lease(&LeaseClaim {
                key: key.clone(),
                candidate_instance_id: crashed,
                now_unix_ms: subtract(now, 9_000)?,
                expires_at_unix_ms: add(now, 60_000)?,
                minimum_snapshot_at_unix_ms: subtract(now, 24_000)?,
                distinct_table_owners: false,
            })
            .await?;
        anyhow::ensure!(
            initial.holder_instance_id == crashed,
            "fixture lease was not claimed by the holder"
        );
        for (instance, instance_key, port) in [
            (restart, restarted_key.clone(), 51_013),
            (case_variant, restarted_key.to_uppercase(), 51_012),
            (padded_variant, format!("{restarted_key} "), 51_014),
        ] {
            store
                .register_collector(&live_registration(
                    instance,
                    instance_key,
                    port,
                    subtract(now, 7_000)?,
                    add(now, 180_000)?,
                )?)
                .await?;
            store
                .publish_pressure(&snapshot(instance, subtract(now, 7_000)?, 500, 0, None)?)
                .await?;
        }
        let succession = |successor: CollectorInstanceId,
                          expected_version: u64,
                          at: UnixMillis|
         -> anyhow::Result<LeaseSuccession> {
            Ok(LeaseSuccession {
                key: key.clone(),
                expected_version,
                predecessor_instance_id: crashed,
                successor_instance_id: successor,
                now_unix_ms: at,
                expires_at_unix_ms: add(at, 60_000)?,
                minimum_snapshot_at_unix_ms: subtract(at, 15_000)?,
                distinct_table_owners: false,
            })
        };

        let live = store
            .transfer_to_successor(&succession(restart, initial.lease_version, subtract(now, 6_000)?)?)
            .await;
        anyhow::ensure!(
            live.as_ref().err().map(RoutingError::kind) == Some(RoutingErrorKind::Conflict),
            "succession from a live holder did not conflict: {live:?}"
        );
        let draining = store
            .begin_lease_drain(&LeaseCas {
                key: key.clone(),
                expected_version: initial.lease_version,
                now_unix_ms: subtract(now, 5_500)?,
            })
            .await?;
        for near_miss in [case_variant, padded_variant] {
            let mismatched = store
                .transfer_to_successor(&succession(near_miss, draining.lease_version, now)?)
                .await;
            anyhow::ensure!(
                mismatched.as_ref().err().map(RoutingError::kind) == Some(RoutingErrorKind::Conflict),
                "succession to a near-miss instance key did not conflict: {mismatched:?}"
            );
        }
        anyhow::ensure!(
            store.load_view(now).await?.leases.get(&key) == Some(&draining),
            "rejected succession mutated the SQL lease"
        );

        let policy = RoutingPolicy {
            warmup_ms: 0,
            lease_ttl_ms: 60_000,
            ..RoutingPolicy::default()
        };
        let engine_store: Arc<dyn RoutingStore> = Arc::<SqlServerRoutingStore>::clone(&store);
        let engine = RoutingEngine::new(engine_store, policy)?;
        let report = engine.rebalance(now).await?;
        anyhow::ensure!(
            report.successions_completed >= 1,
            "rebalance did not complete the fixture succession: {report:?}"
        );
        let succeeded = store
            .load_view(now)
            .await?
            .leases
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("fixture lease disappeared during succession"))?;
        anyhow::ensure!(
            succeeded.holder_instance_id == restart
                && succeeded.state == LeaseState::Active
                && succeeded.drain_started_at_unix_ms.is_none()
                && succeeded.lease_id != initial.lease_id
                && succeeded.fencing_epoch.get() == draining.fencing_epoch.get() + 1
                && succeeded.lease_version == draining.lease_version + 1,
            "SQL succession did not establish a single new fenced owner: {succeeded:?}"
        );
        let replayed = store
            .transfer_to_successor(&succession(restart, draining.lease_version, now)?)
            .await;
        anyhow::ensure!(
            replayed.as_ref().err().map(RoutingError::kind) == Some(RoutingErrorKind::Conflict),
            "replayed succession did not conflict: {replayed:?}"
        );
        anyhow::ensure!(
            active_lease_count(&table).await? == 1,
            "succession created multiple active SQL leases"
        );

        let metadata = IngestMetadata::new(
            ProtocolVersion::CURRENT,
            ImplementationPool::Dataflow,
            table.clone(),
            ClaimedSchemaFingerprint::new(SchemaFingerprint::from_bytes([12; 32])),
        );
        let route = engine.route_ingest(&metadata, add(now, 1_000)?).await?;
        anyhow::ensure!(
            route.instance_id == restart
                && route.source == RouteSource::CachedLease
                && route.lease.as_ref() == Some(&succeeded),
            "cached routing did not follow the same-key restart"
        );
        Ok(())
    }
    .await;
    // A SIGKILLed holder records no terminal state; its registration already
    // expired, so only the live fixture collectors need stopping.
    finish_fixture(
        &store,
        result,
        &[restart, case_variant, padded_variant],
        &table,
        add(now, 10_000)?,
    )
    .await
}

/// Scenario: SQL Server sees the dataflow multi-core shutdown race: several
/// same-instance writers refresh registration and heartbeat with later
/// timestamps while others record `draining` with earlier ones, then shutdown
/// records `stopped` and stale writers arrive.
/// Guarantees: every drain succeeds and the row stays `draining` (no
/// resurrection to `ready`, heartbeat and lease never move backwards); after
/// `stopped`, drains, other terminal states, refreshes and heartbeats are
/// fenced, and a repeated stop is idempotent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the live arrowcol-infra SQL Server"]
async fn live_sql_collector_state_is_monotone_across_same_instance_writers() -> anyhow::Result<()> {
    if std::env::var("ARROWCOL_SQLSERVER_TEST").as_deref() != Ok("1") {
        return Ok(());
    }
    let _isolation = LIVE_ROUTING_CASE.lock().await;
    let store = Arc::new(SqlServerRoutingStore::new(sql_config()?));
    let collector = CollectorInstanceId::new();
    let key = format!("live-monotone-{}", Uuid::new_v4().simple());
    let started = now()?;
    store
        .register_collector(&live_registration(
            collector,
            key.clone(),
            51_101,
            started,
            add(started, 60_000)?,
        )?)
        .await?;
    let late = add(started, 5_000)?;
    let late_lease = add(started, 90_000)?;
    let mut writers = tokio::task::JoinSet::new();
    for core in 0..8_u64 {
        let store = Arc::clone(&store);
        let mut refresh = live_registration(collector, key.clone(), 51_101, started, late_lease)?;
        refresh.heartbeat_at_unix_ms = late;
        writers.spawn(async move {
            if core % 2 == 0 {
                store
                    .set_collector_state(collector, CollectorState::Draining, add(started, 100 + core)?)
                    .await
                    .map_err(anyhow::Error::from)
                    .map(|()| true)
            } else {
                // A refresh may land before or after a drain; afterwards it is fenced.
                match store.register_collector(&refresh).await {
                    Ok(()) => Ok(false),
                    Err(error) if error.kind() == RoutingErrorKind::Conflict => Ok(false),
                    Err(error) => Err(error.into()),
                }
            }
        });
    }
    let mut drains = 0;
    while let Some(result) = writers.join_next().await {
        if result?? {
            drains += 1;
        }
    }
    assert_eq!(drains, 4, "every same-instance drain must succeed");
    store
        .heartbeat_collector(collector, add(started, 1)?, add(started, 30_000)?)
        .await?;
    let refresh = store
        .register_collector(&live_registration(collector, key.clone(), 51_101, started, late_lease)?)
        .await;
    assert_eq!(
        refresh.err().map(|error| error.kind()),
        Some(RoutingErrorKind::Conflict)
    );
    let (state, heartbeat, lease) = live_collector_row(collector).await?;
    assert_eq!(state, "draining");
    assert!(heartbeat >= started.get(), "heartbeat moved backwards");
    assert!(lease >= add(started, 60_000)?.get(), "lease moved backwards");

    store
        .set_collector_state(collector, CollectorState::Stopped, add(started, 200)?)
        .await?;
    for late_state in [CollectorState::Ready, CollectorState::Draining, CollectorState::Failed] {
        let regressed = store
            .set_collector_state(collector, late_state, add(started, 10_000)?)
            .await;
        assert_eq!(
            regressed.err().map(|error| error.kind()),
            Some(RoutingErrorKind::Conflict),
            "stopped must not change to {late_state:?}"
        );
    }
    let heartbeat = store
        .heartbeat_collector(collector, add(started, 10_000)?, add(started, 70_000)?)
        .await;
    assert_eq!(
        heartbeat.err().map(|error| error.kind()),
        Some(RoutingErrorKind::Conflict)
    );
    store
        .set_collector_state(collector, CollectorState::Stopped, add(started, 300)?)
        .await?;
    assert_eq!(live_collector_row(collector).await?.0, "stopped");
    Ok(())
}

/// Returns a routing collector's state, heartbeat and lease expiry (Unix ms).
async fn live_collector_row(instance_id: CollectorInstanceId) -> anyhow::Result<(String, u64, u64)> {
    let mut connection = SqlConnection::connect(&sql_connection_config()?).await?;
    let row = connection
        .query(
            "SELECT state, \
                    DATEDIFF_BIG(millisecond, '1970-01-01', heartbeat_at_utc) AS heartbeat_ms, \
                    DATEDIFF_BIG(millisecond, '1970-01-01', lease_expires_at_utc) AS lease_ms \
             FROM arrowcol.routing_collectors WHERE instance_id = @P1;",
            &[&instance_id.as_uuid()],
        )
        .await?
        .into_row()
        .await?
        .ok_or_else(|| anyhow::anyhow!("routing collector row is missing"))?;
    let state = row
        .try_get::<&str, _>("state")?
        .ok_or_else(|| anyhow::anyhow!("state was NULL"))?
        .to_owned();
    let heartbeat = row
        .try_get::<i64, _>("heartbeat_ms")?
        .ok_or_else(|| anyhow::anyhow!("heartbeat was NULL"))?;
    let lease = row
        .try_get::<i64, _>("lease_ms")?
        .ok_or_else(|| anyhow::anyhow!("lease was NULL"))?;
    Ok((state, u64::try_from(heartbeat)?, u64::try_from(lease)?))
}

fn live_registration(
    instance_id: CollectorInstanceId,
    instance_key: String,
    port: u16,
    started_at: UnixMillis,
    lease_expires_at: UnixMillis,
) -> anyhow::Result<CollectorRegistration> {
    Ok(CollectorRegistration {
        instance_id,
        instance_key,
        implementation_pool: ImplementationPool::Dataflow,
        flight_endpoint: format!("127.0.0.1:{port}").parse()?,
        catalog_capable: true,
        max_concurrent_streams: positive(64)?,
        started_at_unix_ms: started_at,
        heartbeat_at_unix_ms: started_at,
        lease_expires_at_unix_ms: lease_expires_at,
    })
}

/// Stops fixture collectors and deletes the fixture lease, preserving the
/// primary test failure when cleanup also fails.
async fn finish_fixture(
    store: &SqlServerRoutingStore,
    result: anyhow::Result<()>,
    instances: &[CollectorInstanceId],
    table: &TableIdentity,
    cleanup_after: UnixMillis,
) -> anyhow::Result<()> {
    let mut cleanup_errors = Vec::new();
    let cleanup_at = now()?.max(cleanup_after);
    for instance in instances {
        if let Err(error) = store
            .set_collector_state(*instance, CollectorState::Stopped, cleanup_at)
            .await
        {
            cleanup_errors.push(error.to_string());
        }
    }
    if let Err(error) = delete_fixture_lease(table).await {
        cleanup_errors.push(error.to_string());
    }
    match result {
        Err(error) if !cleanup_errors.is_empty() => {
            Err(error.context(format!("fixture cleanup also failed: {}", cleanup_errors.join("; "))))
        }
        Err(error) => Err(error),
        Ok(()) if !cleanup_errors.is_empty() => anyhow::bail!("fixture cleanup failed: {}", cleanup_errors.join("; ")),
        Ok(()) => Ok(()),
    }
}

fn sql_config() -> anyhow::Result<SqlServerRoutingConfig> {
    let host = std::env::var("ARROWCOL_SQLSERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("ARROWCOL_SQLSERVER_PORT")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(14_333);
    Ok(
        SqlServerRoutingConfig::with_password_file(host, port, "arrowcol", "arrowcol_app", secret_path())?
            .trust_server_certificate(true)
            .with_timeouts(Duration::from_secs(5), Duration::from_secs(15)),
    )
}

fn sql_pool() -> anyhow::Result<SqlPool> {
    Ok(SqlPool::new(
        sql_connection_config()?,
        PoolConfig {
            max_size: 2,
            acquire_timeout: Duration::from_secs(5),
        },
    )?)
}

fn sql_connection_config() -> anyhow::Result<ConnectionConfig> {
    let host = std::env::var("ARROWCOL_SQLSERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("ARROWCOL_SQLSERVER_PORT")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(14_333);
    let password = std::fs::read_to_string(secret_path())?;
    Ok(ConnectionConfig::new(
        host,
        port,
        "arrowcol",
        "arrowcol_app",
        password.trim_end_matches(['\r', '\n']),
    )
    .trust_server_certificate(true)
    .with_timeouts(Duration::from_secs(5), Duration::from_secs(15)))
}

async fn active_lease_count(table: &TableIdentity) -> anyhow::Result<i64> {
    let mut client = SqlConnection::connect(&sql_connection_config()?).await?;
    let row = client
        .query(
            "SELECT COUNT_BIG(*) AS active_leases FROM arrowcol.routing_table_leases \
             WHERE implementation_pool = 'dataflow' AND catalog_name = @P1 AND schema_name = @P2 \
               AND table_name = @P3 AND state = 'active';",
            &[
                &table.catalog().as_str(),
                &table.schema().as_str(),
                &table.table().as_str(),
            ],
        )
        .await?
        .into_row()
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing fixture lease count"))?;
    let count = row
        .try_get::<i64, _>("active_leases")?
        .ok_or_else(|| anyhow::anyhow!("NULL fixture lease count"))?;
    client.finish().await?;
    Ok(count)
}

async fn delete_fixture_lease(table: &TableIdentity) -> anyhow::Result<()> {
    let mut client = SqlConnection::connect(&sql_connection_config()?).await?;
    client
        .execute(
            "DELETE FROM arrowcol.routing_table_leases \
             WHERE implementation_pool = 'dataflow' AND catalog_name = @P1 AND schema_name = @P2 \
               AND table_name = @P3;",
            &[
                &table.catalog().as_str(),
                &table.schema().as_str(),
                &table.table().as_str(),
            ],
        )
        .await?;
    client.finish().await?;
    Ok(())
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

fn snapshot(
    instance_id: CollectorInstanceId,
    observed_at: UnixMillis,
    cpu_bps: u16,
    table_streams: u64,
    table: Option<TableIdentity>,
) -> anyhow::Result<RichPressureSnapshot> {
    let drained = table_streams == 0 && table.is_some();
    let buffered_bytes = if drained { 0 } else { 10 };
    let buffered_rows = if drained { 0 } else { 10 };
    let buffered_batches = if drained { 0 } else { 1 };
    let oldest_buffer_age_ms = if drained { 0 } else { 10 };
    Ok(RichPressureSnapshot {
        instance_id,
        observed_at_unix_ms: observed_at,
        cpu_utilization_bps: BasisPoints::new(cpu_bps)?,
        rss_bytes: measurement(100)?,
        rss_limit_bytes: positive(1_000)?,
        active_streams: gauge(table_streams, 64)?,
        received_bytes: measurement(1_000)?,
        received_rows: measurement(100)?,
        received_batches: measurement(10)?,
        buffered_bytes: gauge(buffered_bytes, 1_000)?,
        buffered_rows: gauge(buffered_rows, 1_000)?,
        buffered_batches: gauge(buffered_batches, 100)?,
        available_permits: gauge(64_u64.saturating_sub(table_streams), 64)?,
        pending_commits: gauge(0, 16)?,
        oldest_buffer_age_ms: measurement(oldest_buffer_age_ms)?,
        delta_pressure_bps: BasisPoints::new(100)?,
        quiver_pressure_bps: BasisPoints::new(100)?,
        dfe_pressure_bps: BasisPoints::new(0)?,
        delta_state: SubsystemState::Ready,
        quiver_state: SubsystemState::Ready,
        dfe_state: SubsystemState::Disabled,
        accepting_deliveries: true,
        tables: table
            .map(|table| {
                vec![TablePressure {
                    table,
                    active_streams: measurement(table_streams)
                        .unwrap_or_else(|error| panic!("invalid test table stream count: {error}")),
                    buffered_bytes: measurement(buffered_bytes)
                        .unwrap_or_else(|error| panic!("invalid test buffered bytes: {error}")),
                    buffered_rows: measurement(buffered_rows)
                        .unwrap_or_else(|error| panic!("invalid test buffered rows: {error}")),
                    buffered_batches: measurement(buffered_batches)
                        .unwrap_or_else(|error| panic!("invalid test buffered batches: {error}")),
                    oldest_buffer_age_ms: measurement(oldest_buffer_age_ms)
                        .unwrap_or_else(|error| panic!("invalid test oldest age: {error}")),
                    pending_commits: measurement(0)
                        .unwrap_or_else(|error| panic!("invalid test pending commits: {error}")),
                }]
            })
            .unwrap_or_default(),
    })
}

fn measurement(value: u64) -> anyhow::Result<Measurement> {
    Ok(Measurement::new(value)?)
}

fn positive(value: u64) -> anyhow::Result<PositiveMeasurement> {
    Ok(PositiveMeasurement::new(value)?)
}

fn gauge(current: u64, limit: u64) -> anyhow::Result<PressureGauge> {
    Ok(PressureGauge::new(measurement(current)?, positive(limit)?)?)
}

fn now() -> anyhow::Result<UnixMillis> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(UnixMillis::new(u64::try_from(millis)?)?)
}

fn add(value: UnixMillis, millis: u64) -> anyhow::Result<UnixMillis> {
    Ok(UnixMillis::new(
        value
            .get()
            .checked_add(millis)
            .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))?,
    )?)
}

fn subtract(value: UnixMillis, millis: u64) -> anyhow::Result<UnixMillis> {
    Ok(UnixMillis::new(value.get().saturating_sub(millis))?)
}
