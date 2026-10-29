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
        implementation_pool: ImplementationPool::Direct,
        table: table.clone(),
    };
    let result: anyhow::Result<()> = async {
        for (instance, port) in [(first, 51_001), (second, 51_002)] {
            store
                .register_collector(&CollectorRegistration {
                    instance_id: instance,
                    instance_key: format!("live-router-{}", Uuid::new_v4().simple()),
                    implementation_pool: ImplementationPool::Direct,
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
            ImplementationPool::Direct,
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
    let key = LeaseKey::new(ImplementationPool::Direct, table.clone());
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
            ImplementationPool::Direct,
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
        implementation_pool: ImplementationPool::Direct,
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
             WHERE implementation_pool = 'direct' AND catalog_name = @P1 AND schema_name = @P2 \
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
             WHERE implementation_pool = 'direct' AND catalog_name = @P1 AND schema_name = @P2 \
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
        container_memory: None,
        effective_cpu_millicores: None,
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
