use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
};

use arrowcol_protocol::{SchemaFingerprint, TableIdentity};
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{
    CatalogError, CatalogErrorCode, CatalogResult, CatalogSchemaVersion, CatalogStore, ChangeReservation,
    DescriptorArtifacts, HiveTableSnapshot, Metastore, RequestHash, ReserveOutcome, SagaId, SagaRecord, SagaState,
    SagaStep, SchemaVersion,
};

/// Deterministic failure-injection point used by saga tests.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FailurePoint {
    /// Before a change is reserved.
    Reserve,
    /// Before Hive applies or verifies a schema.
    HiveApply,
    /// Before Hive reads a table.
    HiveRead,
    /// Before the application store records successful Hive application.
    MarkHiveApplied,
    /// Before descriptor artifacts are persisted.
    PersistArtifacts,
    /// Before the schema head is finalized.
    Finalize,
}

#[derive(Default)]
struct MemoryState {
    versions: BTreeMap<TableIdentity, BTreeMap<SchemaVersion, CatalogSchemaVersion>>,
    current: BTreeMap<TableIdentity, SchemaVersion>,
    sagas: BTreeMap<SagaId, SagaRecord>,
    saga_order: Vec<SagaId>,
    operations: HashMap<(TableIdentity, RequestHash), SagaId>,
    active: HashMap<TableIdentity, SagaId>,
    artifacts: BTreeMap<(TableIdentity, SchemaVersion), DescriptorArtifacts>,
    failures: VecDeque<FailurePoint>,
}

/// Transactional in-memory implementation used by focused tests and embedders.
#[derive(Clone, Default)]
pub struct MemoryCatalogStore {
    state: Arc<Mutex<MemoryState>>,
}

impl MemoryCatalogStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Hydrates immutable versions read from already committed Delta metadata.
    ///
    /// # Errors
    /// Rejects inconsistent identities, incompatible history or active schema changes.
    pub async fn hydrate_committed(&self, history: Vec<CatalogSchemaVersion>) -> CatalogResult<()> {
        let Some(head) = history.last() else {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                "empty committed history",
            ));
        };
        let identity = head.identity().clone();
        for pair in history.windows(2) {
            if pair[1].version() <= pair[0].version() || !pair[1].is_nullable_append_only_superset_of(&pair[0]) {
                return Err(CatalogError::new(
                    CatalogErrorCode::CorruptCatalogState,
                    "Delta catalog history is not monotonic nullable append-only",
                ));
            }
        }
        let mut artifacts = Vec::with_capacity(history.len());
        for schema in &history {
            if schema.identity() != &identity {
                return Err(CatalogError::new(
                    CatalogErrorCode::CorruptCatalogState,
                    "Delta catalog history contains mismatched identities",
                ));
            }
            artifacts.push(crate::generate_descriptor_artifacts(schema)?);
        }
        let mut state = self.state.lock().await;
        if state.active.contains_key(&identity) {
            return Err(CatalogError::new(
                CatalogErrorCode::SchemaChangeInProgress,
                "cannot hydrate an active schema change",
            ));
        }
        let version = head.version();
        let mut versions = BTreeMap::new();
        for (schema, artifacts) in history.into_iter().zip(artifacts) {
            state.artifacts.insert((identity.clone(), schema.version()), artifacts);
            versions.insert(schema.version(), schema);
        }
        state.versions.insert(identity.clone(), versions);
        state.current.insert(identity, version);
        Ok(())
    }

    /// Removes a table after its Delta tombstone has committed.
    ///
    /// # Errors
    /// Rejects removal during an active schema change.
    pub async fn remove_committed(&self, identity: &TableIdentity) -> CatalogResult<()> {
        let mut state = self.state.lock().await;
        if state.active.contains_key(identity) {
            return Err(CatalogError::new(
                CatalogErrorCode::SchemaChangeInProgress,
                "cannot remove a table during an active schema change",
            ));
        }
        state.current.remove(identity);
        state.versions.remove(identity);
        state.artifacts.retain(|(table, _), _| table != identity);
        Ok(())
    }

    /// Fails the next matching operation.
    pub async fn fail_next(&self, point: FailurePoint) {
        self.state.lock().await.failures.push_back(point);
    }
}

#[async_trait]
impl CatalogStore for MemoryCatalogStore {
    async fn operation(
        &self,
        identity: &TableIdentity,
        request_hash: RequestHash,
    ) -> CatalogResult<Option<SagaRecord>> {
        let state = self.state.lock().await;
        Ok(state
            .operations
            .get(&(identity.clone(), request_hash))
            .and_then(|id| state.sagas.get(id))
            .cloned())
    }

    async fn current_schema(&self, identity: &TableIdentity) -> CatalogResult<Option<CatalogSchemaVersion>> {
        let state = self.state.lock().await;
        Ok(current_from_state(&state, identity))
    }

    async fn schema_by_fingerprint(
        &self,
        identity: &TableIdentity,
        fingerprint: SchemaFingerprint,
    ) -> CatalogResult<Option<CatalogSchemaVersion>> {
        let state = self.state.lock().await;
        Ok(state
            .versions
            .get(identity)
            .and_then(|versions| versions.values().find(|schema| schema.fingerprint() == fingerprint))
            .cloned())
    }

    async fn schema_history(&self, identity: &TableIdentity) -> CatalogResult<Vec<CatalogSchemaVersion>> {
        let state = self.state.lock().await;
        Ok(state
            .versions
            .get(identity)
            .map(|versions| versions.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn reserve_change(&self, reservation: ChangeReservation) -> CatalogResult<ReserveOutcome> {
        let mut state = self.state.lock().await;
        maybe_fail(&mut state, FailurePoint::Reserve)?;
        let identity = reservation.target_schema.identity().clone();
        if let Some(id) = state.operations.get(&(identity.clone(), reservation.request_hash)) {
            let saga = state.sagas.get(id).cloned().ok_or_else(corrupt_saga)?;
            return Ok(ReserveOutcome::Existing(saga));
        }

        let current = current_from_state(&state, &identity);
        match (&reservation.from_schema, &current) {
            (None, Some(current)) if current.fingerprint() == reservation.target_schema.fingerprint() => {
                return Ok(ReserveOutcome::AlreadyCurrent(current.clone()));
            }
            (None, Some(_)) => {
                return Err(CatalogError::new(
                    CatalogErrorCode::TableAlreadyExists,
                    format!("catalog table {identity} already exists with a different schema"),
                ));
            }
            (Some(_), None) => {
                return Err(CatalogError::new(
                    CatalogErrorCode::TableNotFound,
                    format!("catalog table {identity} does not exist"),
                ));
            }
            (Some(expected), Some(current))
                if expected.version() != current.version() || expected.fingerprint() != current.fingerprint() =>
            {
                return Err(CatalogError::new(
                    CatalogErrorCode::SchemaVersionConflict,
                    format!("schema head for {identity} changed before reservation"),
                ));
            }
            _ => {}
        }
        if let Some(current) = &current
            && current.fingerprint() == reservation.target_schema.fingerprint()
        {
            return Ok(ReserveOutcome::AlreadyCurrent(current.clone()));
        }
        if let Some(active_id) = state.active.get(&identity) {
            let active = state.sagas.get(active_id).cloned().ok_or_else(corrupt_saga)?;
            if active.request_hash() == reservation.request_hash {
                return Ok(ReserveOutcome::Existing(active));
            }
            return Err(CatalogError::new(
                CatalogErrorCode::SchemaChangeInProgress,
                format!("another schema change is active for {identity}"),
            ));
        }

        let saga_id = reservation.saga_id();
        let saga = SagaRecord::new(
            saga_id,
            reservation.request_hash,
            reservation.from_schema,
            reservation.target_schema,
        );
        state
            .operations
            .insert((identity.clone(), saga.request_hash()), saga_id);
        state.active.insert(identity, saga_id);
        state.saga_order.push(saga_id);
        state.sagas.insert(saga_id, saga.clone());
        Ok(ReserveOutcome::Reserved(saga))
    }

    async fn saga(&self, saga_id: SagaId) -> CatalogResult<Option<SagaRecord>> {
        Ok(self.state.lock().await.sagas.get(&saga_id).cloned())
    }

    async fn mark_hive_applied(&self, saga_id: SagaId) -> CatalogResult<SagaRecord> {
        let mut state = self.state.lock().await;
        maybe_fail(&mut state, FailurePoint::MarkHiveApplied)?;
        let saga = state.sagas.get_mut(&saga_id).ok_or_else(corrupt_saga)?;
        if saga.step <= SagaStep::ApplyHive {
            saga.state = SagaState::HiveApplied;
            saga.step = SagaStep::PersistArtifacts;
            saga.last_error = None;
        }
        Ok(saga.clone())
    }

    async fn persist_artifacts(&self, saga_id: SagaId, artifacts: &DescriptorArtifacts) -> CatalogResult<SagaRecord> {
        let mut state = self.state.lock().await;
        maybe_fail(&mut state, FailurePoint::PersistArtifacts)?;
        let (identity, version) = {
            let saga = state.sagas.get(&saga_id).ok_or_else(corrupt_saga)?;
            (saga.target_schema().identity().clone(), saga.target_schema().version())
        };
        state.artifacts.insert((identity, version), artifacts.clone());
        let saga = state.sagas.get_mut(&saga_id).ok_or_else(corrupt_saga)?;
        if saga.step <= SagaStep::PersistArtifacts {
            saga.state = SagaState::ArtifactsPersisted;
            saga.step = SagaStep::Finalize;
            saga.last_error = None;
        }
        Ok(saga.clone())
    }

    async fn finalize(&self, saga_id: SagaId) -> CatalogResult<SagaRecord> {
        let mut state = self.state.lock().await;
        maybe_fail(&mut state, FailurePoint::Finalize)?;
        let saga = state.sagas.get(&saga_id).cloned().ok_or_else(corrupt_saga)?;
        if saga.is_committed() {
            return Ok(saga);
        }
        if saga.step != SagaStep::Finalize {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                format!("saga {saga_id} cannot finalize from step {:?}", saga.step()),
            ));
        }
        let identity = saga.target_schema().identity().clone();
        let version = saga.target_schema().version();
        state
            .versions
            .entry(identity.clone())
            .or_default()
            .insert(version, saga.target_schema().clone());
        state.current.insert(identity.clone(), version);
        state.active.remove(&identity);
        let saga = state.sagas.get_mut(&saga_id).ok_or_else(corrupt_saga)?;
        saga.state = SagaState::Committed;
        saga.step = SagaStep::Complete;
        saga.last_error = None;
        Ok(saga.clone())
    }

    async fn record_failure(&self, saga_id: SagaId, step: SagaStep, error: &CatalogError) -> CatalogResult<SagaRecord> {
        let mut state = self.state.lock().await;
        let saga = state.sagas.get_mut(&saga_id).ok_or_else(corrupt_saga)?;
        saga.state = SagaState::Failed;
        saga.step = step;
        saga.attempt_count = saga.attempt_count.saturating_add(1);
        saga.last_error = Some(error.clone());
        Ok(saga.clone())
    }

    async fn incomplete_sagas(&self) -> CatalogResult<Vec<SagaRecord>> {
        let state = self.state.lock().await;
        Ok(state
            .saga_order
            .iter()
            .filter_map(|id| state.sagas.get(id))
            .filter(|saga| !saga.is_committed())
            .cloned()
            .collect())
    }

    async fn artifacts(
        &self,
        identity: &TableIdentity,
        version: SchemaVersion,
    ) -> CatalogResult<Option<DescriptorArtifacts>> {
        Ok(self
            .state
            .lock()
            .await
            .artifacts
            .get(&(identity.clone(), version))
            .cloned())
    }
}

#[derive(Default)]
struct MemoryMetastoreState {
    tables: BTreeMap<TableIdentity, CatalogSchemaVersion>,
    failures: VecDeque<FailurePoint>,
}

/// Idempotent in-memory metastore used by saga tests.
#[derive(Clone, Default)]
pub struct MemoryMetastore {
    state: Arc<Mutex<MemoryMetastoreState>>,
}

impl MemoryMetastore {
    /// Creates an empty metastore.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Fails the next matching metastore operation.
    pub async fn fail_next(&self, point: FailurePoint) {
        self.state.lock().await.failures.push_back(point);
    }
}

#[async_trait]
impl Metastore for MemoryMetastore {
    async fn ensure_schema(
        &self,
        from_schema: Option<&CatalogSchemaVersion>,
        target_schema: &CatalogSchemaVersion,
    ) -> CatalogResult<HiveTableSnapshot> {
        let mut state = self.state.lock().await;
        maybe_fail_metastore(&mut state, FailurePoint::HiveApply)?;
        let identity = target_schema.identity().clone();
        match state.tables.get(&identity) {
            Some(existing) if existing.fingerprint() == target_schema.fingerprint() => {}
            None if from_schema.is_none() => {
                state.tables.insert(identity, target_schema.clone());
            }
            Some(existing)
                if from_schema.is_some_and(|from| {
                    existing.version() == from.version() && existing.fingerprint() == from.fingerprint()
                }) =>
            {
                state.tables.insert(identity, target_schema.clone());
            }
            None => {
                return Err(CatalogError::new(
                    CatalogErrorCode::HiveMetastore,
                    format!("Hive table {} is missing during ALTER", target_schema.identity()),
                ));
            }
            Some(existing) => {
                return Err(CatalogError::new(
                    CatalogErrorCode::HiveMetastore,
                    format!(
                        "Hive table {} drifted at version {} with fingerprint {}",
                        existing.identity(),
                        existing.version(),
                        existing.fingerprint()
                    ),
                ));
            }
        }
        Ok(HiveTableSnapshot::from_schema(target_schema))
    }

    async fn table(&self, identity: &TableIdentity) -> CatalogResult<Option<HiveTableSnapshot>> {
        let mut state = self.state.lock().await;
        maybe_fail_metastore(&mut state, FailurePoint::HiveRead)?;
        Ok(state.tables.get(identity).map(HiveTableSnapshot::from_schema))
    }
}

fn current_from_state(state: &MemoryState, identity: &TableIdentity) -> Option<CatalogSchemaVersion> {
    state
        .current
        .get(identity)
        .and_then(|version| state.versions.get(identity)?.get(version))
        .cloned()
}

fn maybe_fail(state: &mut MemoryState, point: FailurePoint) -> CatalogResult<()> {
    if state.failures.front() == Some(&point) {
        state.failures.pop_front();
        return Err(injected_failure(point));
    }
    Ok(())
}

fn maybe_fail_metastore(state: &mut MemoryMetastoreState, point: FailurePoint) -> CatalogResult<()> {
    if state.failures.front() == Some(&point) {
        state.failures.pop_front();
        return Err(injected_failure(point));
    }
    Ok(())
}

fn injected_failure(point: FailurePoint) -> CatalogError {
    CatalogError::new(
        match point {
            FailurePoint::HiveApply | FailurePoint::HiveRead => CatalogErrorCode::HiveMetastore,
            _ => CatalogErrorCode::ApplicationDatabase,
        },
        format!("injected failure at {point:?}"),
    )
}

fn corrupt_saga() -> CatalogError {
    CatalogError::new(
        CatalogErrorCode::CorruptCatalogState,
        "catalog saga reference is missing",
    )
}
