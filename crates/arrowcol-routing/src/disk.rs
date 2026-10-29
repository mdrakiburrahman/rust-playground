use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use arrowcol_protocol::{CollectorInstanceId, UnixMillis};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::{
    CollectorRegistration, CollectorState, LeaseCas, LeaseClaim, LeaseKey, LeaseState, LeaseSuccession, LeaseTransfer,
    MemoryRoutingStore, MigrationAck, MigrationPhase, RichPressureSnapshot, RoutingError, RoutingErrorKind,
    RoutingLease, RoutingPolicy, RoutingResult, RoutingStore, RoutingView, TableMigration, rank_collectors,
};

const MAX_SNAPSHOT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_RESERVATIONS: usize = 4096;
const MAX_MIGRATION_TABLES: usize = 256;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementPolicy {
    #[default]
    LeastPressure,
    WeightedRendezvous,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    version: u32,
    checksum: String,
    payload: String,
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    leases: Vec<RoutingLease>,
    migrations: Vec<TableMigration>,
    #[serde(default)]
    draining_collectors: BTreeSet<CollectorInstanceId>,
}

struct State {
    memory: MemoryRoutingStore,
    uncertain: bool,
    reservations: BTreeMap<LeaseKey, (CollectorInstanceId, UnixMillis)>,
    migrations: Vec<TableMigration>,
    draining_collectors: BTreeSet<CollectorInstanceId>,
}

/// Single-writer local authority. Telemetry is deliberately never restored.
pub struct DiskRoutingStore {
    directory: PathBuf,
    _writer_lock: File,
    state: Mutex<State>,
    policy: RoutingPolicy,
    placement: PlacementPolicy,
}

fn failure(cause: impl std::fmt::Display) -> RoutingError {
    RoutingError::new(RoutingErrorKind::Database, format!("disk routing: {cause}"))
}

impl DiskRoutingStore {
    /// Opens and validates the local authority before serving any traffic.
    ///
    /// # Errors
    ///
    /// Rejects concurrent writers, corruption, unsupported versions and I/O errors.
    pub fn open(directory: impl AsRef<Path>, policy: RoutingPolicy, placement: PlacementPolicy) -> RoutingResult<Self> {
        policy.validate()?;
        let directory = directory.as_ref().to_owned();
        std::fs::create_dir_all(&directory).map_err(failure)?;
        let previously_opened = directory.join("writer.lock").exists();
        let writer = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("writer.lock"))
            .map_err(failure)?;
        writer.try_lock().map_err(failure)?;
        let path = directory.join("routing.json");
        let mut view = RoutingView::empty(UnixMillis::new(0).map_err(failure)?);
        let mut migrations = Vec::new();
        let mut draining_collectors = BTreeSet::new();
        match std::fs::metadata(&path) {
            Ok(metadata) => {
                if metadata.len() > MAX_SNAPSHOT_BYTES {
                    return Err(failure("snapshot exceeds bound"));
                }
                let snapshot: Snapshot =
                    serde_json::from_slice(&std::fs::read(&path).map_err(failure)?).map_err(failure)?;
                if snapshot.version != 1
                    || snapshot.checksum != format!("{:x}", Sha256::digest(snapshot.payload.as_bytes()))
                {
                    return Err(RoutingError::new(
                        RoutingErrorKind::CorruptState,
                        "unsupported or corrupt routing snapshot",
                    ));
                }
                let payload: Payload = serde_json::from_str(&snapshot.payload).map_err(failure)?;
                migrations = payload.migrations;
                draining_collectors = payload.draining_collectors;
                for lease in payload.leases {
                    lease.validate()?;
                    if view.leases.insert(lease.key.clone(), lease).is_some() {
                        return Err(failure("duplicate persisted lease"));
                    }
                }
                if migrations.len() > MAX_MIGRATION_TABLES
                    || migrations
                        .iter()
                        .filter(|m| m.phase != MigrationPhase::Complete)
                        .count()
                        > 1
                {
                    return Err(failure("snapshot exceeds bounded migration progress"));
                }
                let mut migration_keys = BTreeSet::new();
                for migration in &migrations {
                    if !migration_keys.insert(migration.key.clone()) {
                        return Err(failure("duplicate per-table migration progress"));
                    }
                    let lease = view
                        .leases
                        .get(&migration.key)
                        .ok_or_else(|| failure("migration has no lease lineage"))?;
                    if migration.source == migration.target
                        || migration.updated_at < migration.started_at
                        || migration.reason.is_empty()
                        || migration.reason.len() > 256
                    {
                        return Err(failure("invalid persisted migration identity"));
                    }
                    let consistent = match migration.phase {
                        MigrationPhase::Preparing => {
                            lease.holder_instance_id == migration.source
                                && lease.fencing_epoch == migration.source_epoch
                                && lease.state == LeaseState::Active
                        }
                        MigrationPhase::Draining => {
                            lease.holder_instance_id == migration.source
                                && lease.fencing_epoch == migration.source_epoch
                                && lease.state == LeaseState::Draining
                        }
                        MigrationPhase::Activating => {
                            lease.holder_instance_id == migration.target
                                && lease.fencing_epoch > migration.source_epoch
                                && lease.state == LeaseState::Draining
                        }
                        MigrationPhase::Complete => {
                            lease.holder_instance_id == migration.target
                                && lease.fencing_epoch > migration.source_epoch
                                && lease.state == LeaseState::Active
                        }
                    };
                    if !consistent {
                        return Err(failure("persisted migration disagrees with lease lineage"));
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if previously_opened {
                    return Err(failure(
                        "previously opened authority has no snapshot; refusing epoch reset",
                    ));
                }
            }
            Err(error) => return Err(failure(error)),
        }
        persist(
            &directory,
            Payload {
                leases: view.leases.values().cloned().collect(),
                migrations: migrations.clone(),
                draining_collectors: draining_collectors.clone(),
            },
        )?;
        Ok(Self {
            directory,
            _writer_lock: writer,
            state: Mutex::new(State {
                memory: MemoryRoutingStore::from_view(view),
                uncertain: false,
                reservations: BTreeMap::new(),
                migrations,
                draining_collectors,
            }),
            policy,
            placement,
        })
    }

    async fn commit(&self, state: &mut State, staged: MemoryRoutingStore, now: UnixMillis) -> RoutingResult<()> {
        let payload = Payload {
            leases: staged.load_view(now).await?.leases.into_values().collect(),
            migrations: state.migrations.clone(),
            draining_collectors: state.draining_collectors.clone(),
        };
        let directory = self.directory.clone();
        // Holding the serialization lock through durable publication prevents a later CAS
        // from observing an acknowledged-but-not-durable mutation.
        state.uncertain = true;
        tokio::task::spawn_blocking(move || persist(&directory, payload))
            .await
            .map_err(failure)??;
        state.memory = staged;
        state.uncertain = false;
        Ok(())
    }

    fn check(state: &State) -> RoutingResult<()> {
        if state.uncertain {
            return Err(failure(
                "prior commit outcome is uncertain; restart and inspect snapshot",
            ));
        }
        Ok(())
    }

    fn choose(
        &self,
        state: &State,
        view: &RoutingView,
        key: &LeaseKey,
        now: UnixMillis,
    ) -> RoutingResult<CollectorInstanceId> {
        let mut candidates = rank_collectors(view, key, now, &self.policy, false);
        let pending = |id: &CollectorInstanceId| {
            state
                .reservations
                .values()
                .filter(|(candidate, _)| candidate == id)
                .count() as u64
        };
        let score = |id: &CollectorInstanceId| {
            let collector = &view.collectors[id];
            let pressure = collector.pressure.as_ref().map_or(10_000, |p| p.maximum_pressure_bps());
            u64::from(pressure)
                .saturating_add(pending(id) * 10_000 / collector.registration.max_concurrent_streams.get())
        };
        if matches!(self.placement, PlacementPolicy::LeastPressure) {
            candidates.sort_by_key(|id| (score(id), *id));
        }
        let selected = candidates
            .first()
            .copied()
            .ok_or_else(|| RoutingError::new(RoutingErrorKind::PoolUnavailable, "no fresh eligible collector"))?;
        tracing::info!(
            table = %key.table,
            policy = ?self.placement,
            at_unix_ms = now.get(),
            selected = %selected,
            candidates = %serde_json::Value::Array(candidates.iter().map(|id| {
                let collector = &view.collectors[id];
                serde_json::json!({
                    "instance_id": id, "instance_key": collector.registration.instance_key,
                    "pressure": collector.pressure, "pending_reservations": pending(id),
                    "least_pressure_score_bps": score(id),
                })
            }).collect()),
            "routing_placement_decision"
        );
        Ok(selected)
    }
}

fn persist(directory: &Path, payload: Payload) -> RoutingResult<()> {
    let started = std::time::Instant::now();
    let payload = serde_json::to_string(&payload).map_err(failure)?;
    let snapshot = Snapshot {
        version: 1,
        checksum: format!("{:x}", Sha256::digest(payload.as_bytes())),
        payload,
    };
    let bytes = serde_json::to_vec(&snapshot).map_err(failure)?;
    if bytes.len() as u64 > MAX_SNAPSHOT_BYTES {
        return Err(failure("snapshot exceeds bound"));
    }
    let pending = directory.join("routing.pending");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&pending)
        .map_err(failure)?;
    file.write_all(&bytes).map_err(failure)?;
    file.sync_all().map_err(failure)?;
    std::fs::rename(&pending, directory.join("routing.json")).map_err(failure)?;
    File::open(directory)
        .and_then(|parent| parent.sync_all())
        .map_err(failure)?;
    tracing::info!(
        snapshot_bytes = bytes.len(),
        commit_micros = started.elapsed().as_micros() as u64,
        "routing_snapshot_committed"
    );
    Ok(())
}

#[async_trait]
impl RoutingStore for DiskRoutingStore {
    async fn request_migration(
        &self,
        key: &LeaseKey,
        reason: &str,
        force_scale_in: bool,
    ) -> RoutingResult<TableMigration> {
        let now = crate::control::current_time()?;
        let mut state = self.state.lock().await;
        Self::check(&state)?;
        if reason.is_empty() || reason.len() > 256 {
            return Err(failure("migration reason must be 1..=256 bytes"));
        }
        if state.migrations.iter().any(|m| m.phase != MigrationPhase::Complete) {
            return Err(RoutingError::new(
                RoutingErrorKind::Conflict,
                "one migration is already pending; retry/reconcile it",
            ));
        }
        if state.migrations.len() >= MAX_MIGRATION_TABLES && !state.migrations.iter().any(|m| m.key == *key) {
            return Err(failure("per-table migration progress bound reached"));
        }
        if state
            .migrations
            .iter()
            .max_by_key(|m| m.updated_at)
            .is_some_and(|m| now.get().saturating_sub(m.updated_at.get()) < 5_000)
        {
            return Err(RoutingError::new(
                RoutingErrorKind::Conflict,
                "five-second experimental migration cooldown",
            ));
        }
        let mut view = state.memory.load_view(now).await?;
        let source = view
            .leases
            .get(key)
            .filter(|l| l.is_routable_at(now))
            .cloned()
            .ok_or_else(|| failure("source not active"))?;
        let source_pressure = view
            .collectors
            .get(&source.holder_instance_id)
            .and_then(|c| c.pressure.as_ref())
            .filter(|p| p.validate_at(now, &self.policy.pressure).is_ok())
            .ok_or_else(|| failure("source pressure unavailable"))?
            .maximum_pressure_bps();
        if force_scale_in
            && view
                .collectors
                .get(&source.holder_instance_id)
                .is_none_or(|c| c.state != CollectorState::Draining)
        {
            return Err(failure("scale-in override requires source collector marked draining"));
        }
        view.collectors.remove(&source.holder_instance_id);
        let target = self.choose(&state, &view, key, now)?;
        let target_pressure = view.collectors[&target]
            .pressure
            .as_ref()
            .ok_or_else(|| failure("target pressure missing"))?
            .maximum_pressure_bps();
        if !force_scale_in && target_pressure.saturating_add(500) > source_pressure {
            return Err(RoutingError::new(
                RoutingErrorKind::Conflict,
                "target must improve bottleneck pressure by at least 500 basis points",
            ));
        }
        let migration = TableMigration {
            key: key.clone(),
            source: source.holder_instance_id,
            target,
            source_epoch: source.fencing_epoch,
            phase: MigrationPhase::Preparing,
            started_at: now,
            updated_at: now,
            reason: reason.to_owned(),
        };
        // One last operation per table reconciles lost activation responses,
        // including while a different table is subsequently moved.
        state.migrations.retain(|existing| existing.key != *key);
        state.migrations.push(migration.clone());
        let staged = MemoryRoutingStore::from_view(state.memory.load_view(now).await?);
        self.commit(&mut state, staged, now).await?;
        Ok(migration)
    }

    async fn migration_work(&self, instance_id: CollectorInstanceId) -> RoutingResult<Vec<TableMigration>> {
        let state = self.state.lock().await;
        Self::check(&state)?;
        Ok(state
            .migrations
            .iter()
            .filter(|m| m.source == instance_id || m.target == instance_id)
            .cloned()
            .collect())
    }

    async fn acknowledge_migration(
        &self,
        requested: &TableMigration,
        instance_id: CollectorInstanceId,
        ack: MigrationAck,
    ) -> RoutingResult<()> {
        let now = crate::control::current_time()?;
        let mut state = self.state.lock().await;
        Self::check(&state)?;
        let mut migration = state
            .migrations
            .iter()
            .find(|m| {
                m.key == requested.key
                    && m.source == requested.source
                    && m.target == requested.target
                    && m.source_epoch == requested.source_epoch
                    && m.started_at == requested.started_at
            })
            .cloned()
            .ok_or_else(|| failure("migration identity mismatch"))?;
        let view = state.memory.load_view(now).await?;
        let source = view
            .leases
            .get(&migration.key)
            .cloned()
            .ok_or_else(|| failure("migration lease missing"))?;
        let staged = MemoryRoutingStore::from_view(view.clone());
        match (migration.phase, ack) {
            (MigrationPhase::Preparing, MigrationAck::Prepared) if instance_id == migration.target => {
                if source.holder_instance_id != migration.source
                    || source.fencing_epoch != migration.source_epoch
                    || !source.is_routable_at(now)
                {
                    return Err(failure("source ownership changed before drain"));
                }
                staged
                    .begin_lease_drain(&LeaseCas {
                        key: migration.key.clone(),
                        expected_version: source.lease_version,
                        now_unix_ms: now,
                    })
                    .await?;
                migration.phase = MigrationPhase::Draining;
            }
            (MigrationPhase::Draining, MigrationAck::SourceReleased) if instance_id == migration.source => {
                let pressure = view
                    .collectors
                    .get(&instance_id)
                    .filter(|c| c.registration.is_live_at(now))
                    .and_then(|c| c.pressure.as_ref())
                    .ok_or_else(|| failure("missing live source proof"))?;
                if source.holder_instance_id != migration.source
                    || source.fencing_epoch != migration.source_epoch
                    || pressure.validate_at(now, &self.policy.pressure).is_err()
                    || pressure.observed_at_unix_ms
                        < source
                            .drain_started_at_unix_ms
                            .ok_or_else(|| failure("missing drain barrier"))?
                    || pressure.table_is_drained(&migration.key.table) != Some(true)
                {
                    return Err(failure("source drain proof is incomplete"));
                }
                let transferred = staged
                    .transfer_drained_lease(&LeaseTransfer {
                        key: migration.key.clone(),
                        expected_version: source.lease_version,
                        candidate_instance_id: migration.target,
                        now_unix_ms: now,
                        expires_at_unix_ms: UnixMillis::new(
                            now.get()
                                .checked_add(self.policy.lease_ttl_ms)
                                .ok_or_else(|| failure("expiry overflow"))?,
                        )
                        .map_err(failure)?,
                        minimum_snapshot_at_unix_ms: UnixMillis::new(
                            now.get().saturating_sub(self.policy.pressure.max_snapshot_age_ms),
                        )
                        .map_err(failure)?,
                    })
                    .await?;
                // Target owns the greater epoch but cannot admit until SQL authority is acquired.
                staged
                    .begin_lease_drain(&LeaseCas {
                        key: migration.key.clone(),
                        expected_version: transferred.lease_version,
                        now_unix_ms: now,
                    })
                    .await?;
                migration.phase = MigrationPhase::Activating;
            }
            (MigrationPhase::Activating, MigrationAck::TargetActive) if instance_id == migration.target => {
                if source.holder_instance_id != migration.target
                    || source.fencing_epoch <= migration.source_epoch
                    || !source.is_valid_at(now)
                {
                    return Err(failure("target routing authority changed or expired"));
                }
                let mut active = source;
                active.state = LeaseState::Active;
                active.drain_started_at_unix_ms = None;
                active.lease_version = active
                    .lease_version
                    .checked_add(1)
                    .ok_or_else(|| failure("lease version exhausted"))?;
                active.renewed_at_unix_ms = now;
                staged.insert_lease(active).await?;
                migration.phase = MigrationPhase::Complete;
            }
            (MigrationPhase::Complete, MigrationAck::TargetActive) if instance_id == migration.target => return Ok(()),
            _ => {
                return Err(RoutingError::new(
                    RoutingErrorKind::Conflict,
                    "migration phase changed; poll and reconcile actual state",
                ));
            }
        }
        migration.updated_at = now;
        state.migrations.retain(|existing| existing.key != migration.key);
        state.migrations.push(migration);
        self.commit(&mut state, staged, now).await
    }

    async fn register_collector(&self, registration: &CollectorRegistration) -> RoutingResult<()> {
        let state = self.state.lock().await;
        Self::check(&state)?;
        let view = state.memory.load_view(registration.heartbeat_at_unix_ms).await?;
        if let Some(existing) = view.collectors.get(&registration.instance_id) {
            if existing.registration == *registration {
                return Ok(());
            }
            return Err(failure("registered process identity is immutable; use heartbeat"));
        }
        if view.collectors.values().any(|existing| {
            existing.registration.instance_key == registration.instance_key
                && existing.registration.is_live_at(registration.heartbeat_at_unix_ms)
        }) {
            return Err(failure("stable collector key already has a live process"));
        }
        state.memory.register_collector(registration).await?;
        if state.draining_collectors.contains(&registration.instance_id) {
            state
                .memory
                .set_collector_state(
                    registration.instance_id,
                    CollectorState::Draining,
                    registration.heartbeat_at_unix_ms,
                )
                .await?;
        }
        Ok(())
    }

    async fn heartbeat_collector(
        &self,
        instance_id: CollectorInstanceId,
        heartbeat_at: UnixMillis,
        lease_expires_at: UnixMillis,
    ) -> RoutingResult<()> {
        let mut state = self.state.lock().await;
        Self::check(&state)?;
        state
            .memory
            .heartbeat_collector(instance_id, heartbeat_at, lease_expires_at)
            .await?;
        let view = state.memory.load_view(heartbeat_at).await?;
        let eligible = view.collectors.get(&instance_id).is_some_and(|record| {
            record.registration.is_live_at(heartbeat_at)
                && record
                    .pressure
                    .as_ref()
                    .is_some_and(|p| p.validate_at(heartbeat_at, &self.policy.pressure).is_ok())
        });
        if eligible {
            let staged = MemoryRoutingStore::from_view(view.clone());
            let mut changed = false;
            for mut lease in view
                .leases
                .into_values()
                .filter(|lease| lease.holder_instance_id == instance_id)
            {
                // Heartbeat cannot resurrect an expired lease or silently change its epoch.
                if lease.is_valid_at(heartbeat_at)
                    && lease.expires_at_unix_ms.get().saturating_sub(heartbeat_at.get()) < self.policy.lease_ttl_ms / 2
                {
                    lease.renewed_at_unix_ms = heartbeat_at;
                    lease.expires_at_unix_ms = UnixMillis::new(
                        heartbeat_at
                            .get()
                            .checked_add(self.policy.lease_ttl_ms)
                            .ok_or_else(|| failure("expiry overflow"))?,
                    )
                    .map_err(failure)?;
                    staged.insert_lease(lease).await?;
                    changed = true;
                }
            }
            if changed {
                self.commit(&mut state, staged, heartbeat_at).await?;
            }
        }
        Ok(())
    }

    async fn set_collector_state(
        &self,
        instance_id: CollectorInstanceId,
        new_state: CollectorState,
        changed_at: UnixMillis,
    ) -> RoutingResult<()> {
        let mut state = self.state.lock().await;
        Self::check(&state)?;
        if state.draining_collectors.contains(&instance_id) && new_state == CollectorState::Ready {
            return Err(failure("retiring process cannot resume new placement"));
        }
        let staged = MemoryRoutingStore::from_view(state.memory.load_view(changed_at).await?);
        staged.set_collector_state(instance_id, new_state, changed_at).await?;
        if new_state == CollectorState::Draining {
            state.draining_collectors.insert(instance_id);
        }
        self.commit(&mut state, staged, changed_at).await
    }

    async fn publish_pressure(&self, snapshot: &RichPressureSnapshot) -> RoutingResult<()> {
        let state = self.state.lock().await;
        Self::check(&state)?;
        state.memory.publish_pressure(snapshot).await
    }

    async fn load_view(&self, loaded_at: UnixMillis) -> RoutingResult<RoutingView> {
        let state = self.state.lock().await;
        Self::check(&state)?;
        tracing::debug!("routing_authority_read");
        state.memory.load_view(loaded_at).await
    }

    async fn claim_lease(&self, requested: &LeaseClaim) -> RoutingResult<RoutingLease> {
        let mut state = self.state.lock().await;
        Self::check(&state)?;
        let now = requested.now_unix_ms;
        let view = state.memory.load_view(now).await?;
        if let Some(existing) = view.leases.get(&requested.key) {
            if !existing.is_valid_at(now) {
                return Err(RoutingError::new(
                    RoutingErrorKind::LeaseHolderUnavailable,
                    "expired established ownership requires explicit recovery; no automatic failover",
                ));
            }
            return Ok(existing.clone());
        }
        state.reservations.retain(|_, (_, until)| *until > now);
        if state.reservations.len() >= MAX_RESERVATIONS {
            return Err(RoutingError::new(
                RoutingErrorKind::PoolUnavailable,
                "pending placement bound reached",
            ));
        }
        let mut claim = requested.clone();
        claim.expires_at_unix_ms = UnixMillis::new(
            now.get()
                .checked_add(self.policy.lease_ttl_ms)
                .ok_or_else(|| failure("expiry overflow"))?,
        )
        .map_err(failure)?;
        claim.minimum_snapshot_at_unix_ms =
            UnixMillis::new(now.get().saturating_sub(self.policy.pressure.max_snapshot_age_ms)).map_err(failure)?;
        claim.candidate_instance_id = self.choose(&state, &view, &claim.key, now)?;
        let staged = MemoryRoutingStore::from_view(view);
        let lease = staged.claim_lease(&claim).await?;
        self.commit(&mut state, staged, now).await?;
        tracing::info!(table = %lease.key.table, holder = %lease.holder_instance_id, "routing_initial_claim");
        state.reservations.insert(
            claim.key,
            (
                lease.holder_instance_id,
                UnixMillis::new(now.get().saturating_add(self.policy.pressure.max_snapshot_age_ms)).map_err(failure)?,
            ),
        );
        Ok(lease)
    }

    async fn begin_lease_drain(&self, request: &LeaseCas) -> RoutingResult<RoutingLease> {
        let mut state = self.state.lock().await;
        Self::check(&state)?;
        let staged = MemoryRoutingStore::from_view(state.memory.load_view(request.now_unix_ms).await?);
        let lease = staged.begin_lease_drain(request).await?;
        self.commit(&mut state, staged, request.now_unix_ms).await?;
        Ok(lease)
    }

    async fn transfer_drained_lease(&self, request: &LeaseTransfer) -> RoutingResult<RoutingLease> {
        let _ = request;
        Err(failure(
            "transfer requires an exact source SQL-fence-release acknowledgement",
        ))
    }

    async fn transfer_to_successor(&self, _: &LeaseSuccession) -> RoutingResult<RoutingLease> {
        Err(RoutingError::new(
            RoutingErrorKind::Conflict,
            "automatic crash succession is disabled for the local authority",
        ))
    }
}
