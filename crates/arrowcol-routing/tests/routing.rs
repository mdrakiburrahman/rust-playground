use std::sync::Arc;

use arrowcol_protocol::{
    BasisPoints, CatalogName, ClaimedSchemaFingerprint, CollectorInstanceId, ImplementationPool, IngestMetadata,
    Measurement, PositiveMeasurement, PressureGauge, ProtocolVersion, SchemaFingerprint, SchemaName, TableIdentity,
    TableName, UnixMillis,
};
use arrowcol_routing::{
    CollectorRegistration, CollectorState, LeaseCas, LeaseClaim, LeaseKey, LeaseState, LeaseSuccession,
    MemoryRoutingStore, RebalanceReport, RichPressureSnapshot, RouteSource, RoutingEngine, RoutingErrorKind,
    RoutingLease, RoutingPolicy, RoutingStore, SubsystemState, TablePressure, rank_collectors,
};
use arrowcol_routing::{DiskRoutingStore, MigrationAck, MigrationPhase, PlacementPolicy};

struct LocalAuthorityDirectory(std::path::PathBuf);

impl LocalAuthorityDirectory {
    fn new() -> Self {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/routing-authority-tests")
            .join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&path).unwrap_or_else(|error| panic!("create isolated authority fixture: {error}"));
        Self(path)
    }
}

impl Drop for LocalAuthorityDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn disk(directory: &LocalAuthorityDirectory) -> DiskRoutingStore {
    DiskRoutingStore::open(&directory.0, RoutingPolicy::default(), PlacementPolicy::LeastPressure)
        .unwrap_or_else(|error| panic!("open isolated authority: {error}"))
}

fn new_claim(key: LeaseKey, candidate: CollectorInstanceId, now: u64) -> LeaseClaim {
    LeaseClaim {
        key,
        candidate_instance_id: candidate,
        now_unix_ms: millis(now),
        expires_at_unix_ms: millis(now + 100_000),
        minimum_snapshot_at_unix_ms: millis(now.saturating_sub(3_000)),
    }
}

#[tokio::test]
async fn disk_authority_serializes_cold_claims_and_preserves_sticky_lineage() -> anyhow::Result<()> {
    let directory = LocalAuthorityDirectory::new();
    let store = Arc::new(disk(&directory));
    let low = CollectorInstanceId::new();
    let high = CollectorInstanceId::new();
    for (id, port, cpu) in [(low, 5501, 100), (high, 5502, 8_000)] {
        store.register_collector(&registration(id, port, 1)).await?;
        store.publish_pressure(&drained_snapshot(id, 100_000, cpu)).await?;
    }
    let claim = new_claim(LeaseKey::new(ImplementationPool::Direct, table()), high, 100_001);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let store = Arc::clone(&store);
        let claim = claim.clone();
        tasks.spawn(async move { store.claim_lease(&claim).await });
    }
    let first = store.claim_lease(&claim).await?;
    assert_eq!(
        first.holder_instance_id, low,
        "authority must ignore caller's chosen owner"
    );
    while let Some(task) = tasks.join_next().await {
        assert_eq!(task??, first, "one lineage per table under concurrent claims");
    }
    store.publish_pressure(&drained_snapshot(low, 100_002, 9_000)).await?;
    assert_eq!(
        store.claim_lease(&claim).await?,
        first,
        "fresh existing owner remains sticky"
    );
    drop(tasks);
    drop(store);
    let reopened = disk(&directory);
    let view = reopened.load_view(millis(100_003)).await?;
    assert!(view.collectors.is_empty(), "telemetry must never be restored as fresh");
    assert_eq!(view.leases.get(&claim.key), Some(&first));
    Ok(())
}

#[tokio::test]
async fn disk_authority_rejects_second_writer_corruption_and_missing_snapshot() -> anyhow::Result<()> {
    let directory = LocalAuthorityDirectory::new();
    let store = disk(&directory);
    assert!(DiskRoutingStore::open(&directory.0, RoutingPolicy::default(), PlacementPolicy::LeastPressure).is_err());
    drop(store);
    let snapshot_path = directory.0.join("routing.json");
    let original = std::fs::read(&snapshot_path)?;
    let mut snapshot: serde_json::Value = serde_json::from_slice(&original)?;
    snapshot["version"] = serde_json::json!(u32::MAX);
    std::fs::write(&snapshot_path, serde_json::to_vec(&snapshot)?)?;
    assert!(DiskRoutingStore::open(&directory.0, RoutingPolicy::default(), PlacementPolicy::LeastPressure).is_err());
    snapshot["version"] = serde_json::json!(1);
    snapshot["checksum"] = serde_json::json!("corrupt");
    std::fs::write(&snapshot_path, serde_json::to_vec(&snapshot)?)?;
    assert!(DiskRoutingStore::open(&directory.0, RoutingPolicy::default(), PlacementPolicy::LeastPressure).is_err());
    std::fs::remove_file(&snapshot_path)?;
    assert!(
        DiskRoutingStore::open(&directory.0, RoutingPolicy::default(), PlacementPolicy::LeastPressure).is_err(),
        "missing initialized snapshot must not reset epochs"
    );
    Ok(())
}

#[tokio::test]
async fn disk_authority_fails_closed_after_uncertain_write_without_acknowledging_claim() -> anyhow::Result<()> {
    let directory = LocalAuthorityDirectory::new();
    let store = disk(&directory);
    let id = CollectorInstanceId::new();
    store.register_collector(&registration(id, 5501, 1)).await?;
    store.publish_pressure(&drained_snapshot(id, 100_000, 100)).await?;
    std::fs::create_dir(directory.0.join("routing.pending"))?;
    let claim = new_claim(LeaseKey::new(ImplementationPool::Direct, table()), id, 100_001);
    assert!(store.claim_lease(&claim).await.is_err());
    assert!(
        store.load_view(millis(100_002)).await.is_err(),
        "uncertain in-memory authority must not serve"
    );
    drop(store);
    std::fs::remove_dir(directory.0.join("routing.pending"))?;
    assert!(disk(&directory).load_view(millis(100_003)).await?.leases.is_empty());
    Ok(())
}

#[tokio::test]
async fn disk_authority_rejects_atomic_replace_failure_and_discards_uncommitted_pending_owner() -> anyhow::Result<()> {
    let directory = LocalAuthorityDirectory::new();
    let store = disk(&directory);
    let id = CollectorInstanceId::new();
    store.register_collector(&registration(id, 5501, 1)).await?;
    store.publish_pressure(&drained_snapshot(id, 100_000, 100)).await?;
    let snapshot = directory.0.join("routing.json");
    let previous = directory.0.join("previous-acknowledged.json");
    std::fs::rename(&snapshot, &previous)?;
    std::fs::create_dir(&snapshot)?;
    let claim = new_claim(LeaseKey::new(ImplementationPool::Direct, table()), id, 100_001);
    assert!(store.claim_lease(&claim).await.is_err());
    assert!(store.load_view(millis(100_002)).await.is_err());
    assert!(directory.0.join("routing.pending").is_file());
    drop(store);
    std::fs::remove_dir(&snapshot)?;
    std::fs::rename(previous, snapshot)?;
    assert!(
        disk(&directory).load_view(millis(100_003)).await?.leases.is_empty(),
        "a fully written but uncommitted pending file must never become ownership"
    );
    Ok(())
}

#[tokio::test]
async fn central_reservations_spread_equal_pressure_cold_tables() -> anyhow::Result<()> {
    let directory = LocalAuthorityDirectory::new();
    let store = disk(&directory);
    let ids = [
        CollectorInstanceId::new(),
        CollectorInstanceId::new(),
        CollectorInstanceId::new(),
    ];
    for (index, id) in ids.iter().enumerate() {
        store
            .register_collector(&registration(*id, 5501 + index as u16, 1))
            .await?;
        store.publish_pressure(&drained_snapshot(*id, 100_000, 100)).await?;
    }

    let mut assigned = std::collections::BTreeMap::new();
    for ordinal in 0..12 {
        let identity = TableIdentity::new(
            table().catalog().clone(),
            table().schema().clone(),
            TableName::new(format!("cold_{ordinal}"))?,
        );
        let lease = store
            .claim_lease(&new_claim(
                LeaseKey::new(ImplementationPool::Direct, identity),
                ids[0],
                100_001,
            ))
            .await?;
        *assigned.entry(lease.holder_instance_id).or_insert(0) += 1;
    }
    assert_eq!(assigned.len(), 3);
    assert!(
        assigned.values().all(|count| *count == 4),
        "bounded cold reservations prevent one-owner stampede"
    );
    Ok(())
}

#[tokio::test]
async fn migration_requires_source_drain_proof_and_durable_greater_epoch_before_activation() -> anyhow::Result<()> {
    let directory = LocalAuthorityDirectory::new();
    let store = disk(&directory);
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let source = CollectorInstanceId::new();
    let target = CollectorInstanceId::new();
    for (id, port, cpu) in [(source, 5501, 100), (target, 5502, 2_000)] {
        let mut record = registration(id, port, now - 1);
        record.heartbeat_at_unix_ms = millis(now);
        record.lease_expires_at_unix_ms = millis(now + 60_000);
        store.register_collector(&record).await?;
        store.publish_pressure(&drained_snapshot(id, now - 1, cpu)).await?;
    }
    let key = LeaseKey::new(ImplementationPool::Direct, table());
    let original = store.claim_lease(&new_claim(key.clone(), source, now)).await?;
    store.publish_pressure(&snapshot(source, now, 8_000, 1)).await?;
    let operation = store.request_migration(&key, "focused controlled move", false).await?;
    assert_eq!(operation.target, target);
    store
        .acknowledge_migration(&operation, target, MigrationAck::Prepared)
        .await?;
    let draining = store.migration_work(source).await?.remove(0);
    assert_eq!(draining.phase, MigrationPhase::Draining);
    assert!(
        store
            .acknowledge_migration(&draining, source, MigrationAck::SourceReleased)
            .await
            .is_err()
    );
    let proof_at = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    store
        .publish_pressure(&drained_snapshot(source, proof_at, 8_000))
        .await?;
    store
        .acknowledge_migration(&draining, source, MigrationAck::SourceReleased)
        .await?;
    let transferred = store.load_view(millis(proof_at)).await?.leases[&key].clone();
    assert_eq!(transferred.holder_instance_id, target);
    assert!(transferred.fencing_epoch > original.fencing_epoch);
    assert_eq!(
        transferred.state,
        LeaseState::Draining,
        "new RPCs fenced until target SQL activation"
    );
    let activating = store.migration_work(target).await?.remove(0);
    store
        .acknowledge_migration(&activating, target, MigrationAck::TargetActive)
        .await?;
    store
        .acknowledge_migration(&activating, target, MigrationAck::TargetActive)
        .await?;
    assert_eq!(
        store.load_view(millis(proof_at)).await?.leases[&key].state,
        LeaseState::Active
    );
    assert_eq!(
        store.migration_work(target).await?[0].phase,
        MigrationPhase::Complete,
        "completed target ACK stays reconcilable after a lost HTTP response"
    );
    drop(store);
    let reopened = disk(&directory);
    assert_eq!(
        reopened.load_view(millis(proof_at)).await?.leases[&key].fencing_epoch,
        transferred.fencing_epoch
    );
    tokio::time::sleep(std::time::Duration::from_millis(5_010)).await;
    let later = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    for (id, cpu) in [(source, 8_000), (target, 2_000)] {
        let mut record = registration(id, if id == source { 5501 } else { 5502 }, later - 1);
        record.heartbeat_at_unix_ms = millis(later);
        record.lease_expires_at_unix_ms = millis(later + 60_000);
        reopened.register_collector(&record).await?;
        reopened.publish_pressure(&drained_snapshot(id, later, cpu)).await?;
    }
    let other = LeaseKey::new(
        ImplementationPool::Direct,
        TableIdentity::new(
            table().catalog().clone(),
            table().schema().clone(),
            TableName::new("another_move")?,
        ),
    );
    let second = reopened.claim_lease(&new_claim(other.clone(), target, later)).await?;
    assert_eq!(second.holder_instance_id, target);
    reopened
        .publish_pressure(&drained_snapshot(target, later + 1, 8_500))
        .await?;
    reopened
        .request_migration(&other, "independent controlled move", false)
        .await?;
    assert!(
        reopened
            .migration_work(target)
            .await?
            .iter()
            .any(|record| record.key == key && record.phase == MigrationPhase::Complete),
        "another table's move must not erase lost activation-ACK reconciliation"
    );
    reopened
        .acknowledge_migration(&activating, target, MigrationAck::TargetActive)
        .await?;
    drop(reopened);
    assert_eq!(disk(&directory).migration_work(target).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn controller_restart_requires_fresh_registration_and_preserves_retirement_intent() -> anyhow::Result<()> {
    let directory = LocalAuthorityDirectory::new();
    let store = disk(&directory);
    let id = CollectorInstanceId::new();
    let registered = registration(id, 5501, 1);
    store.register_collector(&registered).await?;
    store
        .set_collector_state(id, CollectorState::Draining, millis(100_000))
        .await?;
    drop(store);
    let reopened = disk(&directory);
    assert!(reopened.load_view(millis(100_001)).await?.collectors.is_empty());
    reopened.register_collector(&registered).await?;
    let view = reopened.load_view(millis(100_001)).await?;
    assert_eq!(view.collectors[&id].state, CollectorState::Draining);
    assert!(view.collectors[&id].pressure.is_none());
    assert!(
        reopened
            .set_collector_state(id, CollectorState::Ready, millis(100_002))
            .await
            .is_err()
    );
    assert!(
        reopened
            .claim_lease(&new_claim(
                LeaseKey::new(ImplementationPool::Direct, table()),
                id,
                100_002
            ))
            .await
            .is_err()
    );
    Ok(())
}

#[test]
fn actual_container_memory_is_not_clamped_and_excludes_over_limit_placement() -> anyhow::Result<()> {
    let mut pressure = drained_snapshot(CollectorInstanceId::new(), 100_000, 100);
    pressure.container_memory = Some(arrowcol_routing::ContainerMemory {
        current_bytes: measurement(2_000),
        limit_bytes: positive(1_000),
    });
    assert_eq!(pressure.container_memory.map(|m| m.current_bytes.get()), Some(2_000));
    assert_eq!(pressure.maximum_pressure_bps(), 10_000);
    pressure.validate_at(millis(100_001), &RoutingPolicy::default().pressure)?;
    Ok(())
}

fn millis(value: u64) -> UnixMillis {
    UnixMillis::new(value).unwrap_or_else(|error| panic!("invalid test timestamp: {error}"))
}

fn measurement(value: u64) -> Measurement {
    Measurement::new(value).unwrap_or_else(|error| panic!("invalid test measurement: {error}"))
}

fn positive(value: u64) -> PositiveMeasurement {
    PositiveMeasurement::new(value).unwrap_or_else(|error| panic!("invalid test positive measurement: {error}"))
}

fn gauge(current: u64, limit: u64) -> PressureGauge {
    PressureGauge::new(measurement(current), positive(limit))
        .unwrap_or_else(|error| panic!("invalid test gauge: {error}"))
}

fn table() -> TableIdentity {
    TableIdentity::new(
        CatalogName::new("telemetry").unwrap_or_else(|error| panic!("invalid catalog: {error}")),
        SchemaName::new("prod").unwrap_or_else(|error| panic!("invalid schema: {error}")),
        TableName::new("traces").unwrap_or_else(|error| panic!("invalid table: {error}")),
    )
}

fn metadata() -> IngestMetadata {
    IngestMetadata::new(
        ProtocolVersion::CURRENT,
        ImplementationPool::Direct,
        table(),
        ClaimedSchemaFingerprint::new(SchemaFingerprint::from_bytes([7; 32])),
    )
}

fn registration(instance_id: CollectorInstanceId, port: u16, started_at: u64) -> CollectorRegistration {
    CollectorRegistration {
        instance_id,
        instance_key: format!("collector-{port}"),
        implementation_pool: ImplementationPool::Direct,
        flight_endpoint: format!("127.0.0.1:{port}")
            .parse()
            .unwrap_or_else(|error| panic!("invalid endpoint: {error}")),
        catalog_capable: true,
        max_concurrent_streams: positive(64),
        started_at_unix_ms: millis(started_at),
        heartbeat_at_unix_ms: millis(100_000),
        lease_expires_at_unix_ms: millis(200_000),
    }
}

#[derive(Clone, Copy)]
struct TableWork {
    active_streams: u64,
    buffered_bytes: u64,
    buffered_rows: u64,
    buffered_batches: u64,
    pending_commits: u64,
}

impl TableWork {
    const DRAINED: Self = Self {
        active_streams: 0,
        buffered_bytes: 0,
        buffered_rows: 0,
        buffered_batches: 0,
        pending_commits: 0,
    };

    const fn queued(active_streams: u64) -> Self {
        Self {
            active_streams,
            buffered_bytes: 10,
            buffered_rows: 10,
            buffered_batches: 1,
            pending_commits: 0,
        }
    }
}

fn snapshot(
    instance_id: CollectorInstanceId,
    observed_at: u64,
    cpu_bps: u16,
    table_streams: u64,
) -> RichPressureSnapshot {
    snapshot_with_work(instance_id, observed_at, cpu_bps, TableWork::queued(table_streams))
}

fn drained_snapshot(instance_id: CollectorInstanceId, observed_at: u64, cpu_bps: u16) -> RichPressureSnapshot {
    snapshot_with_work(instance_id, observed_at, cpu_bps, TableWork::DRAINED)
}

fn global_snapshot_with_work(
    instance_id: CollectorInstanceId,
    observed_at: u64,
    cpu_bps: u16,
    work: TableWork,
) -> RichPressureSnapshot {
    let mut snapshot = snapshot_with_work(instance_id, observed_at, cpu_bps, work);
    snapshot.tables.clear();
    snapshot
}

fn snapshot_with_work(
    instance_id: CollectorInstanceId,
    observed_at: u64,
    cpu_bps: u16,
    work: TableWork,
) -> RichPressureSnapshot {
    let oldest_age = u64::from(work.buffered_batches > 0);
    RichPressureSnapshot {
        container_memory: None,
        effective_cpu_millicores: None,
        instance_id,
        observed_at_unix_ms: millis(observed_at),
        cpu_utilization_bps: BasisPoints::new(cpu_bps).unwrap_or_else(|error| panic!("invalid CPU pressure: {error}")),
        rss_bytes: measurement(100),
        rss_limit_bytes: positive(1_000),
        active_streams: gauge(work.active_streams, 64),
        received_bytes: measurement(1_000),
        received_rows: measurement(100),
        received_batches: measurement(10),
        buffered_bytes: gauge(work.buffered_bytes, 1_000),
        buffered_rows: gauge(work.buffered_rows, 1_000),
        buffered_batches: gauge(work.buffered_batches, 100),
        available_permits: gauge(64_u64.saturating_sub(work.active_streams), 64),
        pending_commits: gauge(work.pending_commits, 16),
        oldest_buffer_age_ms: measurement(oldest_age),
        delta_pressure_bps: BasisPoints::new(100).unwrap_or_else(|error| panic!("invalid Delta pressure: {error}")),
        quiver_pressure_bps: BasisPoints::new(100).unwrap_or_else(|error| panic!("invalid Quiver pressure: {error}")),
        dfe_pressure_bps: BasisPoints::new(0).unwrap_or_else(|error| panic!("invalid DFE pressure: {error}")),
        delta_state: SubsystemState::Ready,
        quiver_state: SubsystemState::Ready,
        dfe_state: SubsystemState::Disabled,
        accepting_deliveries: true,
        tables: vec![TablePressure {
            table: table(),
            active_streams: measurement(work.active_streams),
            buffered_bytes: measurement(work.buffered_bytes),
            buffered_rows: measurement(work.buffered_rows),
            buffered_batches: measurement(work.buffered_batches),
            oldest_buffer_age_ms: measurement(oldest_age),
            pending_commits: measurement(work.pending_commits),
        }],
    }
}

async fn seeded_store() -> (Arc<MemoryRoutingStore>, CollectorInstanceId, CollectorInstanceId) {
    let store = Arc::new(MemoryRoutingStore::default());
    let first = CollectorInstanceId::new();
    let second = CollectorInstanceId::new();
    store
        .register_collector(&registration(first, 5001, 20_000))
        .await
        .unwrap_or_else(|error| panic!("register first collector: {error}"));
    store
        .register_collector(&registration(second, 5002, 20_000))
        .await
        .unwrap_or_else(|error| panic!("register second collector: {error}"));
    store
        .publish_pressure(&snapshot(first, 99_000, 1_000, 0))
        .await
        .unwrap_or_else(|error| panic!("publish first pressure: {error}"));
    store
        .publish_pressure(&snapshot(second, 99_000, 1_000, 0))
        .await
        .unwrap_or_else(|error| panic!("publish second pressure: {error}"));
    (store, first, second)
}

#[tokio::test]
async fn valid_cached_lease_avoids_per_request_store_access() {
    let (store, _, _) = seeded_store().await;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, RoutingPolicy::default())
        .unwrap_or_else(|error| panic!("create routing engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("initial refresh: {error}"));

    let first = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial route: {error}"));
    assert_eq!(first.source, RouteSource::LeaseClaim);
    let after_claim = store.operation_counts();

    let cached = engine
        .route_ingest(&metadata(), millis(100_002))
        .await
        .unwrap_or_else(|error| panic!("cached route: {error}"));
    assert_eq!(cached.source, RouteSource::CachedLease);
    assert_eq!(cached.instance_id, first.instance_id);
    assert_eq!(store.operation_counts(), after_claim);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_router_replicas_converge_on_one_sql_winner() {
    let (store, _, _) = seeded_store().await;
    let left_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let right_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let left = Arc::new(
        RoutingEngine::new(left_store, RoutingPolicy::default())
            .unwrap_or_else(|error| panic!("create left engine: {error}")),
    );
    let right = Arc::new(
        RoutingEngine::new(right_store, RoutingPolicy::default())
            .unwrap_or_else(|error| panic!("create right engine: {error}")),
    );
    left.refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh left: {error}"));
    right
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh right: {error}"));

    let left_task = {
        let left = Arc::clone(&left);
        tokio::spawn(async move { left.route_ingest(&metadata(), millis(100_001)).await })
    };
    let right_task = {
        let right = Arc::clone(&right);
        tokio::spawn(async move { right.route_ingest(&metadata(), millis(100_001)).await })
    };
    let left_route = left_task
        .await
        .unwrap_or_else(|error| panic!("left task failed: {error}"))
        .unwrap_or_else(|error| panic!("left route failed: {error}"));
    let right_route = right_task
        .await
        .unwrap_or_else(|error| panic!("right task failed: {error}"))
        .unwrap_or_else(|error| panic!("right route failed: {error}"));
    assert_eq!(left_route.lease, right_route.lease);
    assert_eq!(left_route.instance_id, right_route.instance_id);
}

#[tokio::test]
async fn valid_lease_winner_does_not_depend_on_a_losing_candidate() {
    let (store, first, second) = seeded_store().await;
    let key = LeaseKey::new(ImplementationPool::Direct, table());
    let winner = store
        .claim_lease(&LeaseClaim {
            key: key.clone(),
            candidate_instance_id: first,
            now_unix_ms: millis(100_001),
            expires_at_unix_ms: millis(130_001),
            minimum_snapshot_at_unix_ms: millis(99_000),
        })
        .await
        .unwrap_or_else(|error| panic!("claim initial lease: {error}"));
    store
        .set_collector_state(second, CollectorState::Draining, millis(100_002))
        .await
        .unwrap_or_else(|error| panic!("drain losing candidate: {error}"));

    let observed = store
        .claim_lease(&LeaseClaim {
            key,
            candidate_instance_id: second,
            now_unix_ms: millis(100_003),
            expires_at_unix_ms: millis(130_003),
            minimum_snapshot_at_unix_ms: millis(99_000),
        })
        .await
        .unwrap_or_else(|error| panic!("read authoritative lease winner: {error}"));

    assert_eq!(observed, winner);
}

#[tokio::test]
async fn eligible_holder_is_not_drained_for_soft_pressure_imbalance() {
    let (store, _, _) = seeded_store().await;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, RoutingPolicy::default())
        .unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let assigned = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    store
        .publish_pressure(&snapshot(assigned.instance_id, 100_002, 9_499, 0))
        .await
        .unwrap_or_else(|error| panic!("publish soft pressure: {error}"));

    let report = engine
        .rebalance(millis(100_003))
        .await
        .unwrap_or_else(|error| panic!("rebalance: {error}"));
    assert_eq!(report.drains_started, 0);
    let routed = engine
        .route_ingest(&metadata(), millis(100_004))
        .await
        .unwrap_or_else(|error| panic!("route stable lease: {error}"));
    assert_eq!(routed.instance_id, assigned.instance_id);
}

#[tokio::test]
async fn hard_pressure_starts_drain_then_fully_drained_snapshot_allows_transfer() {
    let (store, first, second) = seeded_store().await;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, RoutingPolicy::default())
        .unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let assigned = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    let holder = assigned.instance_id;
    let fallback = if holder == first { second } else { first };

    store
        .publish_pressure(&snapshot(holder, 100_002, 9_900, 1))
        .await
        .unwrap_or_else(|error| panic!("publish hard pressure: {error}"));
    let first_pass = engine
        .rebalance(millis(100_003))
        .await
        .unwrap_or_else(|error| panic!("start drain: {error}"));
    assert_eq!(first_pass.drains_started, 1);
    let Err(draining) = engine.route_ingest(&metadata(), millis(100_004)).await else {
        panic!("new stream should be rejected during drain");
    };
    assert_eq!(draining.kind(), RoutingErrorKind::LeaseDraining);

    store
        .publish_pressure(&drained_snapshot(holder, 100_005, 9_900))
        .await
        .unwrap_or_else(|error| panic!("publish drained pressure: {error}"));
    let second_pass = engine
        .rebalance(millis(100_006))
        .await
        .unwrap_or_else(|error| panic!("transfer drained lease: {error}"));
    assert_eq!(second_pass.transfers_completed, 1);
    let transferred = engine
        .route_ingest(&metadata(), millis(100_007))
        .await
        .unwrap_or_else(|error| panic!("route after transfer: {error}"));
    assert_eq!(transferred.instance_id, fallback);
    let old_epoch = assigned
        .lease
        .as_ref()
        .unwrap_or_else(|| panic!("initial route must have lease"))
        .fencing_epoch;
    let new_epoch = transferred
        .lease
        .as_ref()
        .unwrap_or_else(|| panic!("transferred route must have lease"))
        .fencing_epoch;
    assert!(new_epoch > old_epoch);
}

#[tokio::test]
async fn pre_drain_zero_snapshot_cannot_authorize_transfer() {
    let (store, first, second) = seeded_store().await;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, RoutingPolicy::default())
        .unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let assigned = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    let holder = assigned.instance_id;
    let fallback = if holder == first { second } else { first };

    store
        .publish_pressure(&drained_snapshot(holder, 100_002, 9_900))
        .await
        .unwrap_or_else(|error| panic!("publish pre-drain zero pressure: {error}"));
    let draining = engine
        .rebalance(millis(100_003))
        .await
        .unwrap_or_else(|error| panic!("start drain: {error}"));
    assert_eq!(draining.drains_started, 1);

    let premature = engine
        .rebalance(millis(100_004))
        .await
        .unwrap_or_else(|error| panic!("recheck pre-drain snapshot: {error}"));
    assert_eq!(premature.transfers_completed, 0);

    store
        .publish_pressure(&drained_snapshot(holder, 100_005, 9_900))
        .await
        .unwrap_or_else(|error| panic!("publish post-drain zero pressure: {error}"));
    let transferred = engine
        .rebalance(millis(100_006))
        .await
        .unwrap_or_else(|error| panic!("transfer after post-drain snapshot: {error}"));
    assert_eq!(transferred.transfers_completed, 1);
    let routed = engine
        .route_ingest(&metadata(), millis(100_007))
        .await
        .unwrap_or_else(|error| panic!("route after safe transfer: {error}"));
    assert_eq!(routed.instance_id, fallback);
}

#[tokio::test]
async fn zero_streams_with_buffered_or_committing_work_cannot_transfer() {
    let (store, first, second) = seeded_store().await;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, RoutingPolicy::default())
        .unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let assigned = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    let holder = assigned.instance_id;
    let fallback = if holder == first { second } else { first };

    store
        .publish_pressure(&snapshot(holder, 100_002, 9_900, 1))
        .await
        .unwrap_or_else(|error| panic!("publish pressure: {error}"));
    engine
        .rebalance(millis(100_003))
        .await
        .unwrap_or_else(|error| panic!("start drain: {error}"));

    store
        .publish_pressure(&snapshot_with_work(
            holder,
            100_004,
            9_900,
            TableWork {
                active_streams: 0,
                buffered_bytes: 10,
                buffered_rows: 10,
                buffered_batches: 1,
                pending_commits: 1,
            },
        ))
        .await
        .unwrap_or_else(|error| panic!("publish queued drain pressure: {error}"));
    let blocked = engine
        .rebalance(millis(100_005))
        .await
        .unwrap_or_else(|error| panic!("evaluate queued drain pressure: {error}"));
    assert_eq!(blocked.transfers_completed, 0);

    store
        .publish_pressure(&drained_snapshot(holder, 100_006, 9_900))
        .await
        .unwrap_or_else(|error| panic!("publish fully drained pressure: {error}"));
    let transferred = engine
        .rebalance(millis(100_007))
        .await
        .unwrap_or_else(|error| panic!("transfer fully drained lease: {error}"));
    assert_eq!(transferred.transfers_completed, 1);
    let routed = engine
        .route_ingest(&metadata(), millis(100_008))
        .await
        .unwrap_or_else(|error| panic!("route after fully drained transfer: {error}"));
    assert_eq!(routed.instance_id, fallback);
}

#[tokio::test]
async fn global_fallback_requires_every_drain_counter_to_reach_zero() {
    let (store, first, second) = seeded_store().await;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, RoutingPolicy::default())
        .unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let assigned = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    let holder = assigned.instance_id;
    let fallback = if holder == first { second } else { first };

    store
        .publish_pressure(&snapshot(holder, 100_002, 9_900, 1))
        .await
        .unwrap_or_else(|error| panic!("publish pressure: {error}"));
    engine
        .rebalance(millis(100_003))
        .await
        .unwrap_or_else(|error| panic!("start drain: {error}"));

    store
        .publish_pressure(&global_snapshot_with_work(
            holder,
            100_004,
            9_900,
            TableWork {
                active_streams: 0,
                buffered_bytes: 0,
                buffered_rows: 0,
                buffered_batches: 0,
                pending_commits: 1,
            },
        ))
        .await
        .unwrap_or_else(|error| panic!("publish global pending-commit pressure: {error}"));
    let blocked = engine
        .rebalance(millis(100_005))
        .await
        .unwrap_or_else(|error| panic!("evaluate global drain pressure: {error}"));
    assert_eq!(blocked.transfers_completed, 0);

    store
        .publish_pressure(&global_snapshot_with_work(holder, 100_006, 9_900, TableWork::DRAINED))
        .await
        .unwrap_or_else(|error| panic!("publish global fully drained pressure: {error}"));
    let transferred = engine
        .rebalance(millis(100_007))
        .await
        .unwrap_or_else(|error| panic!("transfer globally drained lease: {error}"));
    assert_eq!(transferred.transfers_completed, 1);
    let routed = engine
        .route_ingest(&metadata(), millis(100_008))
        .await
        .unwrap_or_else(|error| panic!("route after global fallback transfer: {error}"));
    assert_eq!(routed.instance_id, fallback);
}

#[tokio::test]
async fn stale_snapshots_and_expired_leases_fail_closed() {
    let (store, _, _) = seeded_store().await;
    let mut policy = RoutingPolicy::default();
    policy.pressure.max_snapshot_age_ms = 100;
    policy.lease_ttl_ms = 50;
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, policy).unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(99_050))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let route = engine
        .route_ingest(&metadata(), millis(99_050))
        .await
        .unwrap_or_else(|error| panic!("route with fresh pressure: {error}"));
    assert!(route.lease.is_some());

    let Err(stale) = engine.route_ingest(&metadata(), millis(99_200)).await else {
        panic!("expired lease and stale pressure must fail closed");
    };
    assert_eq!(stale.kind(), RoutingErrorKind::PoolUnavailable);
}

#[tokio::test]
async fn stale_control_cache_rejects_valid_owner_and_lease_free_catalog_routing() -> anyhow::Result<()> {
    let (store, _, _) = seeded_store().await;
    let engine = RoutingEngine::new(
        Arc::<MemoryRoutingStore>::clone(&store),
        RoutingPolicy {
            readiness_max_staleness_ms: 10,
            ..RoutingPolicy::default()
        },
    )?;
    engine.refresh(millis(100_000)).await?;
    let control = arrowcol_routing::ControlRoute {
        implementation_pool: ImplementationPool::Direct,
        operation: arrowcol_routing::ControlOperation::CatalogRead,
        routing_key: "catalog-discovery".to_owned(),
    };
    assert!(engine.route_control(&control, millis(100_001)).await.is_ok());
    assert!(engine.route_ingest(&metadata(), millis(100_001)).await?.lease.is_some());
    store.set_available(false);
    let Err(ingest) = engine.route_ingest(&metadata(), millis(100_011)).await else {
        panic!("stale cached owner must not admit ingest");
    };
    let Err(catalog) = engine.route_control(&control, millis(100_011)).await else {
        panic!("stale cached discovery must not select a collector");
    };
    assert_eq!(ingest.kind(), RoutingErrorKind::PoolUnavailable);
    assert_eq!(catalog.kind(), RoutingErrorKind::PoolUnavailable);
    assert!(!engine.is_ready_at(millis(100_011)).await);
    Ok(())
}

#[tokio::test]
async fn sql_outage_uses_only_unexpired_cached_lease_then_fails_closed() {
    let (store, _, _) = seeded_store().await;
    let policy = RoutingPolicy {
        lease_ttl_ms: 50,
        ..RoutingPolicy::default()
    };
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(&store);
    let engine = RoutingEngine::new(engine_store, policy).unwrap_or_else(|error| panic!("create engine: {error}"));
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial route: {error}"));

    store.set_available(false);
    let cached = engine
        .route_ingest(&metadata(), millis(100_025))
        .await
        .unwrap_or_else(|error| panic!("valid cached lease should survive SQL outage: {error}"));
    assert_eq!(cached.source, RouteSource::CachedLease);

    let Err(expired) = engine.route_ingest(&metadata(), millis(100_060)).await else {
        panic!("expired cached lease must not invent ownership during SQL outage");
    };
    assert_eq!(expired.kind(), RoutingErrorKind::Database);
}

#[tokio::test]
async fn weighted_rendezvous_is_deterministic_and_excludes_hard_pressure() {
    let (store, first, second) = seeded_store().await;
    store
        .publish_pressure(&snapshot(first, 100_000, 9_900, 0))
        .await
        .unwrap_or_else(|error| panic!("publish hard pressure: {error}"));
    let view = store
        .load_view(millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("load view: {error}"));
    let key = LeaseKey::new(ImplementationPool::Direct, table());
    let left = rank_collectors(&view, &key, millis(100_001), &RoutingPolicy::default(), false);
    let right = rank_collectors(&view, &key, millis(100_001), &RoutingPolicy::default(), false);
    assert_eq!(left, right);
    assert_eq!(left, vec![second]);
}

#[test]
fn pressure_validation_rejects_duplicate_tables_and_future_observations() {
    let instance = CollectorInstanceId::new();
    let mut pressure = snapshot(instance, 100_000, 100, 0);
    pressure.tables.push(pressure.tables[0].clone());
    let Err(duplicate) = pressure.validate_at(millis(100_000), &arrowcol_routing::PressurePolicy::default()) else {
        panic!("duplicate table must fail");
    };
    assert_eq!(duplicate.kind(), RoutingErrorKind::InvalidSnapshot);

    let future = snapshot(instance, 200_000, 100, 0);
    let Err(future_error) = future.validate_at(millis(100_000), &arrowcol_routing::PressurePolicy::default()) else {
        panic!("far-future observation must fail");
    };
    assert_eq!(future_error.kind(), RoutingErrorKind::InvalidSnapshot);
}

/// Instance key reused by a crashed collector and its restarted process.
const RESTARTED_KEY: &str = "loadtest-direct-1";
/// Production-like lease duration that outlives the crashed registration.
const LONG_LEASE_TTL_MS: u64 = 86_400_000;

fn keyed_registration(
    instance_id: CollectorInstanceId,
    instance_key: &str,
    port: u16,
    started_at: u64,
    lease_expires_at: u64,
) -> CollectorRegistration {
    CollectorRegistration {
        instance_key: instance_key.to_owned(),
        heartbeat_at_unix_ms: millis(started_at),
        lease_expires_at_unix_ms: millis(lease_expires_at),
        ..registration(instance_id, port, started_at)
    }
}

async fn register_with_pressure(store: &MemoryRoutingStore, registration: CollectorRegistration, observed_at: u64) {
    let instance_id = registration.instance_id;
    store
        .register_collector(&registration)
        .await
        .unwrap_or_else(|error| panic!("register collector: {error}"));
    store
        .publish_pressure(&snapshot(instance_id, observed_at, 1_000, 0))
        .await
        .unwrap_or_else(|error| panic!("publish collector pressure: {error}"));
}

fn long_lease_engine(store: &Arc<MemoryRoutingStore>) -> RoutingEngine {
    let engine_store: Arc<dyn RoutingStore> = Arc::<MemoryRoutingStore>::clone(store);
    let policy = RoutingPolicy {
        lease_ttl_ms: LONG_LEASE_TTL_MS,
        ..RoutingPolicy::default()
    };
    RoutingEngine::new(engine_store, policy).unwrap_or_else(|error| panic!("create engine: {error}"))
}

/// A holder that crashed after claiming a lease, its same-key restart, and a
/// healthy collector deployed under a different instance key.
struct CrashedHolder {
    store: Arc<MemoryRoutingStore>,
    crashed: CollectorInstanceId,
    restart: CollectorInstanceId,
    stranger: CollectorInstanceId,
    lease: RoutingLease,
}

impl CrashedHolder {
    /// The holder claims at 100_001 and its registration expires at 200_000;
    /// the restart and the stranger publish fresh pressure at 250_000.
    async fn new(lease_ttl_ms: u64) -> Self {
        let store = Arc::new(MemoryRoutingStore::default());
        let crashed = CollectorInstanceId::new();
        let restart = CollectorInstanceId::new();
        let stranger = CollectorInstanceId::new();
        register_with_pressure(
            &store,
            keyed_registration(crashed, RESTARTED_KEY, 5001, 20_000, 200_000),
            99_000,
        )
        .await;
        let lease = store
            .claim_lease(&LeaseClaim {
                key: LeaseKey::new(ImplementationPool::Direct, table()),
                candidate_instance_id: crashed,
                now_unix_ms: millis(100_001),
                expires_at_unix_ms: millis(100_001 + lease_ttl_ms),
                minimum_snapshot_at_unix_ms: millis(99_000),
            })
            .await
            .unwrap_or_else(|error| panic!("claim crashed holder lease: {error}"));
        register_with_pressure(
            &store,
            keyed_registration(restart, RESTARTED_KEY, 5003, 250_000, 400_000),
            250_000,
        )
        .await;
        register_with_pressure(
            &store,
            keyed_registration(stranger, "loadtest-direct-2", 5002, 20_000, 400_000),
            250_000,
        )
        .await;
        Self {
            store,
            crashed,
            restart,
            stranger,
            lease,
        }
    }

    fn succession(&self, successor: CollectorInstanceId, now: u64) -> LeaseSuccession {
        LeaseSuccession {
            key: self.lease.key.clone(),
            expected_version: self.lease.lease_version,
            predecessor_instance_id: self.crashed,
            successor_instance_id: successor,
            now_unix_ms: millis(now),
            expires_at_unix_ms: millis(now + LONG_LEASE_TTL_MS),
            minimum_snapshot_at_unix_ms: millis(now - RoutingPolicy::default().pressure.max_snapshot_age_ms),
        }
    }

    async fn stored_lease(&self) -> RoutingLease {
        self.store
            .load_view(millis(250_000))
            .await
            .unwrap_or_else(|error| panic!("load view: {error}"))
            .leases
            .get(&self.lease.key)
            .cloned()
            .unwrap_or_else(|| panic!("fixture lease disappeared"))
    }

    async fn assert_rejected(&self, request: &LeaseSuccession, kind: RoutingErrorKind) {
        let before = self.stored_lease().await;
        let Err(error) = self.store.transfer_to_successor(request).await else {
            panic!("succession must be rejected");
        };
        assert_eq!(error.kind(), kind);
        assert_eq!(self.stored_lease().await, before);
    }
}

/// Scenario: a SIGKILLed holder's registration expired while its 24-hour lease
/// is valid, and its restart registered under the same instance key.
/// Guarantees: the restart receives a new active lease ID with the next epoch
/// and version, and replaying the consumed request conflicts.
#[tokio::test]
async fn succession_hands_crashed_holder_lease_to_same_key_restart() {
    let fixture = CrashedHolder::new(LONG_LEASE_TTL_MS).await;
    let request = fixture.succession(fixture.restart, 250_001);
    let succeeded = fixture
        .store
        .transfer_to_successor(&request)
        .await
        .unwrap_or_else(|error| panic!("succession: {error}"));

    assert_eq!(succeeded.key, fixture.lease.key);
    assert_eq!(succeeded.holder_instance_id, fixture.restart);
    assert_ne!(succeeded.lease_id, fixture.lease.lease_id);
    assert_eq!(succeeded.fencing_epoch.get(), fixture.lease.fencing_epoch.get() + 1);
    assert_eq!(succeeded.lease_version, fixture.lease.lease_version + 1);
    assert_eq!(succeeded.state, LeaseState::Active);
    assert_eq!(succeeded.issued_at_unix_ms, millis(250_001));
    assert_eq!(succeeded.renewed_at_unix_ms, millis(250_001));
    assert_eq!(succeeded.expires_at_unix_ms, request.expires_at_unix_ms);
    assert_eq!(succeeded.drain_started_at_unix_ms, None);
    assert_eq!(fixture.stored_lease().await, succeeded);

    let Err(replayed) = fixture.store.transfer_to_successor(&request).await else {
        panic!("a consumed succession version must not apply twice");
    };
    assert_eq!(replayed.kind(), RoutingErrorKind::Conflict);
    assert_eq!(fixture.stored_lease().await, succeeded);
}

/// Scenario: the holder's registration is still live and ready when a router
/// proposes succession to a same-key process.
/// Guarantees: succession conflicts and the lease is unchanged; a live holder
/// keeps ownership until it expires or reports a terminal state.
#[tokio::test]
async fn succession_rejects_predecessor_with_live_registration() {
    let fixture = CrashedHolder::new(LONG_LEASE_TTL_MS).await;
    fixture
        .store
        .heartbeat_collector(fixture.crashed, millis(249_000), millis(300_000))
        .await
        .unwrap_or_else(|error| panic!("extend holder registration: {error}"));

    fixture
        .assert_rejected(
            &fixture.succession(fixture.restart, 250_001),
            RoutingErrorKind::Conflict,
        )
        .await;
}

/// Scenario: the holder's registration is unexpired, but its shutdown recorded
/// the terminal failed state before the same-key restart registered.
/// Guarantees: a terminal predecessor counts as dead, so the restart inherits
/// the lease without waiting for registration expiry.
#[tokio::test]
async fn succession_accepts_failed_predecessor_before_registration_expiry() {
    let fixture = CrashedHolder::new(LONG_LEASE_TTL_MS).await;
    fixture
        .store
        .heartbeat_collector(fixture.crashed, millis(249_000), millis(300_000))
        .await
        .unwrap_or_else(|error| panic!("extend holder registration: {error}"));
    fixture
        .store
        .set_collector_state(fixture.crashed, CollectorState::Failed, millis(249_500))
        .await
        .unwrap_or_else(|error| panic!("record failed shutdown: {error}"));

    let succeeded = fixture
        .store
        .transfer_to_successor(&fixture.succession(fixture.restart, 250_001))
        .await
        .unwrap_or_else(|error| panic!("succession from failed holder: {error}"));
    assert_eq!(succeeded.holder_instance_id, fixture.restart);
    assert_eq!(succeeded.fencing_epoch.get(), fixture.lease.fencing_epoch.get() + 1);
}

/// Scenario: the holder crashed, but the proposed successor is a healthy
/// collector deployed under a different instance key.
/// Guarantees: succession conflicts and the lease is unchanged because only a
/// same-key restart replays the predecessor's durable backlog.
#[tokio::test]
async fn succession_rejects_successor_with_different_instance_key() {
    let fixture = CrashedHolder::new(LONG_LEASE_TTL_MS).await;
    fixture
        .assert_rejected(
            &fixture.succession(fixture.stranger, 250_001),
            RoutingErrorKind::Conflict,
        )
        .await;
}

/// Scenario: the same-key restart's pressure is stale, and later it reports
/// draining.
/// Guarantees: succession applies the normal candidate checks and conflicts
/// without changing the lease.
#[tokio::test]
async fn succession_rejects_restart_without_fresh_ready_pressure() {
    let fixture = CrashedHolder::new(LONG_LEASE_TTL_MS).await;
    fixture
        .assert_rejected(
            &fixture.succession(fixture.restart, 270_000),
            RoutingErrorKind::Conflict,
        )
        .await;

    fixture
        .store
        .set_collector_state(fixture.restart, CollectorState::Draining, millis(250_000))
        .await
        .unwrap_or_else(|error| panic!("drain restart: {error}"));
    fixture
        .assert_rejected(
            &fixture.succession(fixture.restart, 250_001),
            RoutingErrorKind::Conflict,
        )
        .await;
}

/// Scenario: a router proposes succession with a stale lease version, or names
/// a dead same-key process that is not the current holder.
/// Guarantees: compare-and-set evidence must match exactly, so both conflict
/// and the lease is unchanged.
#[tokio::test]
async fn succession_rejects_stale_version_or_non_holder_predecessor() {
    let fixture = CrashedHolder::new(LONG_LEASE_TTL_MS).await;
    let mut stale = fixture.succession(fixture.restart, 250_001);
    stale.expected_version += 1;
    fixture.assert_rejected(&stale, RoutingErrorKind::Conflict).await;

    let ghost = CollectorInstanceId::new();
    register_with_pressure(
        &fixture.store,
        keyed_registration(ghost, RESTARTED_KEY, 5004, 20_000, 200_000),
        99_500,
    )
    .await;
    let mut non_holder = fixture.succession(fixture.restart, 250_001);
    non_holder.predecessor_instance_id = ghost;
    fixture.assert_rejected(&non_holder, RoutingErrorKind::Conflict).await;
}

/// Scenario: the crashed holder's lease expired before succession was
/// attempted.
/// Guarantees: succession conflicts, and the expired lease is reassigned only
/// through the normal claim path.
#[tokio::test]
async fn succession_rejects_expired_lease() {
    let fixture = CrashedHolder::new(100_000).await;
    let request = fixture.succession(fixture.restart, 250_001);
    fixture.assert_rejected(&request, RoutingErrorKind::Conflict).await;

    let claimed = fixture
        .store
        .claim_lease(&LeaseClaim {
            key: request.key.clone(),
            candidate_instance_id: fixture.restart,
            now_unix_ms: request.now_unix_ms,
            expires_at_unix_ms: request.expires_at_unix_ms,
            minimum_snapshot_at_unix_ms: request.minimum_snapshot_at_unix_ms,
        })
        .await
        .unwrap_or_else(|error| panic!("claim expired lease: {error}"));
    assert_eq!(claimed.holder_instance_id, fixture.restart);
}

/// Scenario: a hard-pressure drain started before the holder crashed, so its
/// draining lease can never receive a post-drain proof.
/// Guarantees: the same-key restart takes over the draining lease as active
/// with a new lease ID, the next epoch and version, and no drain metadata.
#[tokio::test]
async fn succession_takes_over_draining_lease() {
    let fixture = CrashedHolder::new(LONG_LEASE_TTL_MS).await;
    let draining = fixture
        .store
        .begin_lease_drain(&LeaseCas {
            key: fixture.lease.key.clone(),
            expected_version: fixture.lease.lease_version,
            now_unix_ms: millis(100_002),
        })
        .await
        .unwrap_or_else(|error| panic!("begin drain: {error}"));
    assert_eq!(draining.state, LeaseState::Draining);

    let mut request = fixture.succession(fixture.restart, 250_001);
    request.expected_version = draining.lease_version;
    let succeeded = fixture
        .store
        .transfer_to_successor(&request)
        .await
        .unwrap_or_else(|error| panic!("succession from draining lease: {error}"));
    assert_eq!(succeeded.holder_instance_id, fixture.restart);
    assert_eq!(succeeded.state, LeaseState::Active);
    assert_eq!(succeeded.drain_started_at_unix_ms, None);
    assert_ne!(succeeded.lease_id, draining.lease_id);
    assert_eq!(succeeded.fencing_epoch.get(), draining.fencing_epoch.get() + 1);
    assert_eq!(succeeded.lease_version, draining.lease_version + 1);
    assert_eq!(fixture.stored_lease().await, succeeded);
}

/// Scenario: a caller proposes self-succession or an empty new lease window.
/// Guarantees: both requests fail validation as invalid configuration before
/// any lease mutation.
#[tokio::test]
async fn succession_rejects_self_succession_and_empty_lease_window() {
    let fixture = CrashedHolder::new(LONG_LEASE_TTL_MS).await;
    let self_succession = fixture.succession(fixture.crashed, 250_001);
    assert_eq!(
        self_succession.validate().map_err(|error| error.kind()),
        Err(RoutingErrorKind::InvalidConfiguration)
    );
    fixture
        .assert_rejected(&self_succession, RoutingErrorKind::InvalidConfiguration)
        .await;

    let mut empty_window = fixture.succession(fixture.restart, 250_001);
    empty_window.expires_at_unix_ms = empty_window.now_unix_ms;
    assert_eq!(
        empty_window.validate().map_err(|error| error.kind()),
        Err(RoutingErrorKind::InvalidConfiguration)
    );
    fixture
        .assert_rejected(&empty_window, RoutingErrorKind::InvalidConfiguration)
        .await;
}

/// Scenario: a SIGKILLed holder's registration expired with a valid 24-hour
/// lease while its same-key restart and a different-key collector are both
/// eligible.
/// Guarantees: one rebalance pass hands the lease to the restart without a
/// drain, and new streams route there from the cache without store access.
#[tokio::test]
async fn rebalance_hands_crashed_holder_lease_to_same_key_restart() {
    let fixture = CrashedHolder::new(LONG_LEASE_TTL_MS).await;
    let engine = long_lease_engine(&fixture.store);
    engine
        .refresh(millis(250_001))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let Err(stranded) = engine.route_ingest(&metadata(), millis(250_001)).await else {
        panic!("a crashed holder must not receive new streams");
    };
    assert_eq!(stranded.kind(), RoutingErrorKind::LeaseHolderUnavailable);

    let report = engine
        .rebalance(millis(250_002))
        .await
        .unwrap_or_else(|error| panic!("rebalance: {error}"));
    assert_eq!(
        report,
        RebalanceReport {
            successions_completed: 1,
            ..RebalanceReport::default()
        }
    );

    let before_route = fixture.store.operation_counts();
    let routed = engine
        .route_ingest(&metadata(), millis(250_003))
        .await
        .unwrap_or_else(|error| panic!("route after succession: {error}"));
    assert_eq!(fixture.store.operation_counts(), before_route);
    assert_eq!(routed.source, RouteSource::CachedLease);
    assert_eq!(routed.instance_id, fixture.restart);
    assert_eq!(routed.flight_endpoint.port(), 5003);
    let lease = routed
        .lease
        .unwrap_or_else(|| panic!("succession route must carry a lease"));
    assert_eq!(lease, fixture.stored_lease().await);
    assert_ne!(lease.lease_id, fixture.lease.lease_id);
    assert_eq!(lease.fencing_epoch.get(), fixture.lease.fencing_epoch.get() + 1);
}

/// Scenario: hard pressure started a drain, then the holder crashed before
/// publishing a post-drain snapshot and restarted under the same key.
/// Guarantees: rebalance hands the draining lease to the restart instead of
/// waiting for a drain proof, and new streams route to the restart.
#[tokio::test]
async fn rebalance_hands_crashed_draining_lease_to_same_key_restart() {
    let store = Arc::new(MemoryRoutingStore::default());
    let crashed = CollectorInstanceId::new();
    let restart = CollectorInstanceId::new();
    let stranger = CollectorInstanceId::new();
    register_with_pressure(
        &store,
        keyed_registration(crashed, RESTARTED_KEY, 5001, 20_000, 200_000),
        99_000,
    )
    .await;
    let engine = long_lease_engine(&store);
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let assigned = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    assert_eq!(assigned.instance_id, crashed);
    register_with_pressure(
        &store,
        keyed_registration(stranger, "loadtest-direct-2", 5002, 20_000, 400_000),
        100_002,
    )
    .await;
    store
        .publish_pressure(&snapshot(crashed, 100_003, 9_900, 1))
        .await
        .unwrap_or_else(|error| panic!("publish hard pressure: {error}"));
    let drain = engine
        .rebalance(millis(100_004))
        .await
        .unwrap_or_else(|error| panic!("start drain: {error}"));
    assert_eq!(
        drain,
        RebalanceReport {
            drains_started: 1,
            ..RebalanceReport::default()
        }
    );

    register_with_pressure(
        &store,
        keyed_registration(restart, RESTARTED_KEY, 5003, 250_000, 400_000),
        250_000,
    )
    .await;
    store
        .publish_pressure(&snapshot(stranger, 250_000, 1_000, 0))
        .await
        .unwrap_or_else(|error| panic!("refresh stranger pressure: {error}"));
    let report = engine
        .rebalance(millis(250_001))
        .await
        .unwrap_or_else(|error| panic!("rebalance after crash: {error}"));
    assert_eq!(
        report,
        RebalanceReport {
            successions_completed: 1,
            ..RebalanceReport::default()
        }
    );

    let routed = engine
        .route_ingest(&metadata(), millis(250_002))
        .await
        .unwrap_or_else(|error| panic!("route after succession: {error}"));
    assert_eq!(routed.instance_id, restart);
    let initial = assigned
        .lease
        .unwrap_or_else(|| panic!("initial route must carry a lease"));
    let lease = routed
        .lease
        .unwrap_or_else(|| panic!("succession route must carry a lease"));
    assert_eq!(lease.state, LeaseState::Active);
    assert_eq!(lease.drain_started_at_unix_ms, None);
    assert_ne!(lease.lease_id, initial.lease_id);
    assert_eq!(lease.fencing_epoch.get(), initial.fencing_epoch.get() + 1);
    assert_eq!(lease.lease_version, initial.lease_version + 2);
}

/// Scenario: a crashed holder has no same-key restart, only a healthy
/// collector deployed under a different instance key.
/// Guarantees: rebalance never attempts succession or transfer without a drain
/// proof, so the lease stays draining with its original holder.
#[tokio::test]
async fn rebalance_never_moves_crashed_lease_to_different_key_without_drain_proof() {
    let store = Arc::new(MemoryRoutingStore::default());
    let crashed = CollectorInstanceId::new();
    let stranger = CollectorInstanceId::new();
    register_with_pressure(
        &store,
        keyed_registration(crashed, RESTARTED_KEY, 5001, 20_000, 200_000),
        99_000,
    )
    .await;
    let engine = long_lease_engine(&store);
    engine
        .refresh(millis(100_000))
        .await
        .unwrap_or_else(|error| panic!("refresh: {error}"));
    let assigned = engine
        .route_ingest(&metadata(), millis(100_001))
        .await
        .unwrap_or_else(|error| panic!("initial assignment: {error}"));
    assert_eq!(assigned.instance_id, crashed);
    register_with_pressure(
        &store,
        keyed_registration(stranger, "loadtest-direct-2", 5002, 20_000, 400_000),
        250_000,
    )
    .await;

    let first = engine
        .rebalance(millis(250_001))
        .await
        .unwrap_or_else(|error| panic!("first rebalance: {error}"));
    assert_eq!(
        first,
        RebalanceReport {
            drains_started: 1,
            ..RebalanceReport::default()
        }
    );
    let second = engine
        .rebalance(millis(250_002))
        .await
        .unwrap_or_else(|error| panic!("second rebalance: {error}"));
    assert_eq!(second, RebalanceReport::default());

    let Err(draining) = engine.route_ingest(&metadata(), millis(250_003)).await else {
        panic!("a draining lease must reject new streams");
    };
    assert_eq!(draining.kind(), RoutingErrorKind::LeaseDraining);
    let counts = store.operation_counts();
    assert_eq!(counts.succession, 0);
    assert_eq!(counts.transfer, 0);
    let view = store
        .load_view(millis(250_003))
        .await
        .unwrap_or_else(|error| panic!("load view: {error}"));
    let lease = view
        .leases
        .get(&LeaseKey::new(ImplementationPool::Direct, table()))
        .unwrap_or_else(|| panic!("crashed holder lease disappeared"));
    assert_eq!(lease.holder_instance_id, crashed);
    assert_eq!(lease.state, LeaseState::Draining);
}
