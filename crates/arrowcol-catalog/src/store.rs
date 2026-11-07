use std::{fmt, str::FromStr};

use arrowcol_protocol::{SchemaFingerprint, TableIdentity};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{CatalogError, CatalogResult, CatalogSchemaVersion, DescriptorArtifacts};

/// SHA-256 idempotency identity of canonical DDL.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestHash([u8; 32]);

impl RequestHash {
    /// Computes a request hash from canonical DDL.
    #[must_use]
    pub fn from_canonical_ddl(canonical_ddl: &str) -> Self {
        Self(Sha256::digest(canonical_ddl.as_bytes()).into())
    }

    /// Creates a request hash from stored bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw digest.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for RequestHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for RequestHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

/// Stable schema-change saga identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SagaId(Uuid);

impl SagaId {
    /// Derives the idempotent saga identifier for a table request.
    #[must_use]
    pub fn for_request(table_id: Uuid, request_hash: RequestHash) -> Self {
        Self(Uuid::new_v5(&table_id, request_hash.as_bytes()))
    }

    /// Creates a saga identifier from a UUID.
    #[must_use]
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the UUID.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for SagaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for SagaId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Durable schema-change saga state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SagaState {
    /// Application state and the pending target version are reserved.
    Reserved,
    /// Deterministic artifacts are persisted.
    ArtifactsPersisted,
    /// The new immutable version is the current head.
    Committed,
    /// The last attempt failed and reconciliation must resume at `step`.
    Failed,
}

impl SagaState {
    /// Stable persisted storage name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::ArtifactsPersisted => "artifacts_persisted",
            Self::Committed => "committed",
            Self::Failed => "failed",
        }
    }

    /// Parses a persisted storage value.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown persisted value.
    pub fn parse(value: &str) -> CatalogResult<Self> {
        match value {
            "reserved" => Ok(Self::Reserved),
            "artifacts_persisted" => Ok(Self::ArtifactsPersisted),
            "committed" => Ok(Self::Committed),
            "failed" => Ok(Self::Failed),
            _ => Err(crate::CatalogError::new(
                crate::CatalogErrorCode::CorruptCatalogState,
                format!("unknown persisted saga state {value:?}"),
            )),
        }
    }
}

/// Next idempotent step for an incomplete saga.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SagaStep {
    /// Generate and persist deterministic descriptor artifacts.
    PersistArtifacts,
    /// Atomically activate the target application schema.
    Finalize,
    /// No work remains.
    Complete,
}

impl SagaStep {
    /// Stable persisted step number.
    #[must_use]
    pub const fn number(self) -> i16 {
        match self {
            Self::PersistArtifacts => 1,
            Self::Finalize => 2,
            Self::Complete => 3,
        }
    }

    /// Parses a persisted step number.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown persisted value.
    pub fn from_number(value: i16) -> CatalogResult<Self> {
        match value {
            1 => Ok(Self::PersistArtifacts),
            2 => Ok(Self::Finalize),
            3 => Ok(Self::Complete),
            _ => Err(crate::CatalogError::new(
                crate::CatalogErrorCode::CorruptCatalogState,
                format!("unknown persisted saga step {value}"),
            )),
        }
    }
}

/// Complete durable record required to retry or reconcile a schema change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SagaRecord {
    pub(crate) id: SagaId,
    pub(crate) request_hash: RequestHash,
    pub(crate) from_schema: Option<CatalogSchemaVersion>,
    pub(crate) target_schema: CatalogSchemaVersion,
    pub(crate) state: SagaState,
    pub(crate) step: SagaStep,
    pub(crate) attempt_count: u32,
    pub(crate) last_error: Option<CatalogError>,
}

impl SagaRecord {
    /// Returns the saga identifier.
    #[must_use]
    pub const fn id(&self) -> SagaId {
        self.id
    }

    /// Returns the canonical-DDL request hash.
    #[must_use]
    pub const fn request_hash(&self) -> RequestHash {
        self.request_hash
    }

    /// Returns the optional source schema.
    #[must_use]
    pub const fn from_schema(&self) -> Option<&CatalogSchemaVersion> {
        self.from_schema.as_ref()
    }

    /// Returns the pending target schema.
    #[must_use]
    pub const fn target_schema(&self) -> &CatalogSchemaVersion {
        &self.target_schema
    }

    /// Returns the durable saga state.
    #[must_use]
    pub const fn state(&self) -> SagaState {
        self.state
    }

    /// Returns the next idempotent step.
    #[must_use]
    pub const fn step(&self) -> SagaStep {
        self.step
    }

    /// Returns the number of recorded failed attempts.
    #[must_use]
    pub const fn attempt_count(&self) -> u32 {
        self.attempt_count
    }

    /// Returns the most recent visible failure.
    #[must_use]
    pub const fn last_error(&self) -> Option<&CatalogError> {
        self.last_error.as_ref()
    }

    /// Returns whether the saga is committed.
    #[must_use]
    pub const fn is_committed(&self) -> bool {
        matches!(self.state, SagaState::Committed)
    }

    pub(crate) fn new(
        id: SagaId,
        request_hash: RequestHash,
        from_schema: Option<CatalogSchemaVersion>,
        target_schema: CatalogSchemaVersion,
    ) -> Self {
        Self {
            id,
            request_hash,
            from_schema,
            target_schema,
            state: SagaState::Reserved,
            step: SagaStep::PersistArtifacts,
            attempt_count: 0,
            last_error: None,
        }
    }

    pub(crate) fn from_persisted(persisted: PersistedSagaRecord) -> Self {
        Self {
            id: persisted.id,
            request_hash: persisted.request_hash,
            from_schema: persisted.from_schema,
            target_schema: persisted.target_schema,
            state: persisted.state,
            step: persisted.step,
            attempt_count: persisted.attempt_count,
            last_error: persisted.last_error,
        }
    }
}

pub(crate) struct PersistedSagaRecord {
    pub(crate) id: SagaId,
    pub(crate) request_hash: RequestHash,
    pub(crate) from_schema: Option<CatalogSchemaVersion>,
    pub(crate) target_schema: CatalogSchemaVersion,
    pub(crate) state: SagaState,
    pub(crate) step: SagaStep,
    pub(crate) attempt_count: u32,
    pub(crate) last_error: Option<CatalogError>,
}

/// New schema change to reserve atomically.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeReservation {
    /// Canonical request hash.
    pub request_hash: RequestHash,
    /// Expected current schema, absent for a create.
    pub from_schema: Option<CatalogSchemaVersion>,
    /// Complete pending target schema.
    pub target_schema: CatalogSchemaVersion,
}

impl ChangeReservation {
    /// Returns the deterministic saga identifier.
    #[must_use]
    pub fn saga_id(&self) -> SagaId {
        SagaId::for_request(self.target_schema.table_id(), self.request_hash)
    }
}

/// Result of an idempotent reservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReserveOutcome {
    /// This caller created the durable reservation.
    Reserved(SagaRecord),
    /// An equivalent request already exists, possibly incomplete.
    Existing(SagaRecord),
    /// The current schema already equals the requested target.
    AlreadyCurrent(CatalogSchemaVersion),
}

/// Persistence contract for immutable catalog history and recoverable sagas.
#[async_trait]
pub trait CatalogStore: Send + Sync {
    /// Finds an equivalent request for idempotent retry handling.
    async fn operation(&self, identity: &TableIdentity, request_hash: RequestHash)
    -> CatalogResult<Option<SagaRecord>>;

    /// Loads the current active schema head.
    async fn current_schema(&self, identity: &TableIdentity) -> CatalogResult<Option<CatalogSchemaVersion>>;

    /// Loads a schema by exact authoritative fingerprint.
    async fn schema_by_fingerprint(
        &self,
        identity: &TableIdentity,
        fingerprint: SchemaFingerprint,
    ) -> CatalogResult<Option<CatalogSchemaVersion>>;

    /// Loads all immutable versions in ascending order.
    async fn schema_history(&self, identity: &TableIdentity) -> CatalogResult<Vec<CatalogSchemaVersion>>;

    /// Atomically reserves a pending version or returns an equivalent request.
    async fn reserve_change(&self, reservation: ChangeReservation) -> CatalogResult<ReserveOutcome>;

    /// Loads a saga by identifier.
    async fn saga(&self, saga_id: SagaId) -> CatalogResult<Option<SagaRecord>>;

    /// Persists deterministic schema artifacts.
    async fn persist_artifacts(&self, saga_id: SagaId, artifacts: &DescriptorArtifacts) -> CatalogResult<SagaRecord>;

    /// Atomically activates the pending version and commits the saga.
    async fn finalize(&self, saga_id: SagaId) -> CatalogResult<SagaRecord>;

    /// Records a visible failure without discarding retry state.
    async fn record_failure(&self, saga_id: SagaId, step: SagaStep, error: &CatalogError) -> CatalogResult<SagaRecord>;

    /// Returns every incomplete saga in deterministic start order.
    async fn incomplete_sagas(&self) -> CatalogResult<Vec<SagaRecord>>;

    /// Loads persisted artifacts for an immutable version.
    async fn artifacts(
        &self,
        identity: &TableIdentity,
        version: crate::SchemaVersion,
    ) -> CatalogResult<Option<DescriptorArtifacts>>;
}
