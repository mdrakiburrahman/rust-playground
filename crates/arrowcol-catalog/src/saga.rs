use std::{collections::BTreeSet, sync::Arc};

use arrowcol_protocol::{SchemaFingerprint, TableIdentity};
use async_trait::async_trait;

use crate::{
    CatalogError, CatalogErrorCode, CatalogResult, CatalogSchemaVersion, CatalogStore, ChangeReservation, DdlCommand,
    DdlParser, RequestHash, ReserveOutcome, SagaId, SagaRecord, SagaStep, SchemaVersion, generate_descriptor_artifacts,
};

/// Flight SQL DDL request accepted by the embeddable handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlightSqlDdlRequest {
    command: String,
}

impl FlightSqlDdlRequest {
    /// Creates a request from a Flight SQL command string.
    #[must_use]
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
        }
    }

    /// Returns the SQL command.
    #[must_use]
    pub fn command(&self) -> &str {
        &self.command
    }
}

/// Completion disposition for a Flight SQL DDL request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DdlCompletionDisposition {
    /// A new table was committed.
    Created,
    /// Nullable fields were appended as a new version.
    Altered,
    /// An equivalent request or target schema was already committed.
    AlreadyApplied,
}

impl DdlCompletionDisposition {
    /// Stable metadata value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Altered => "altered",
            Self::AlreadyApplied => "already_applied",
        }
    }
}

/// Structured Flight SQL completion and schema-version metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlCompletion {
    identity: TableIdentity,
    schema_version: SchemaVersion,
    fingerprint: SchemaFingerprint,
    saga_id: Option<SagaId>,
    disposition: DdlCompletionDisposition,
}

impl DdlCompletion {
    /// Builds a completion only after its schema is committed to Delta.
    #[must_use]
    pub fn from_committed(schema: &CatalogSchemaVersion, disposition: DdlCompletionDisposition) -> Self {
        Self {
            identity: schema.identity().clone(),
            schema_version: schema.version(),
            fingerprint: schema.fingerprint(),
            saga_id: None,
            disposition,
        }
    }

    /// Returns the affected table.
    #[must_use]
    pub const fn identity(&self) -> &TableIdentity {
        &self.identity
    }

    /// Returns the immutable active version.
    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns the canonical Arrow fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> SchemaFingerprint {
        self.fingerprint
    }

    /// Returns the durable saga identifier, when a saga was involved.
    #[must_use]
    pub const fn saga_id(&self) -> Option<SagaId> {
        self.saga_id
    }

    /// Returns whether the request created, altered, or retried existing state.
    #[must_use]
    pub const fn disposition(&self) -> DdlCompletionDisposition {
        self.disposition
    }

    /// Encodes stable completion metadata suitable for Flight SQL response
    /// application metadata.
    #[must_use]
    pub fn metadata_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = vec![
            ("x-arrowcol-catalog", self.identity.catalog().to_string()),
            ("x-arrowcol-schema", self.identity.schema().to_string()),
            ("x-arrowcol-table", self.identity.table().to_string()),
            ("x-arrowcol-schema-version", self.schema_version.to_string()),
            ("x-arrowcol-schema-fingerprint", self.fingerprint.to_string()),
            ("x-arrowcol-ddl-disposition", self.disposition.as_str().to_owned()),
        ];
        if let Some(saga_id) = self.saga_id {
            pairs.push(("x-arrowcol-schema-saga-id", saga_id.to_string()));
        }
        pairs
    }
}

/// Reconciliation summary produced during control-plane startup.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReconciliationReport {
    /// Number of incomplete sagas examined.
    pub examined: usize,
    /// Number completed during this pass.
    pub completed: usize,
    /// Visible failures that remain retryable on a later startup/pass.
    pub failures: Vec<(SagaId, CatalogError)>,
}

/// Embeddable Flight SQL-facing DDL service abstraction.
#[async_trait]
pub trait FlightSqlDdlHandler: Send + Sync {
    /// Executes one strict DDL command through the recoverable schema saga.
    async fn execute_ddl(&self, request: FlightSqlDdlRequest) -> CatalogResult<DdlCompletion>;
}

/// Catalog control plane coordinating recoverable schema reservations and activation.
#[derive(Clone)]
pub struct CatalogControlPlane {
    parser: DdlParser,
    store: Arc<dyn CatalogStore>,
}

impl CatalogControlPlane {
    /// Creates a control plane without performing I/O.
    ///
    /// Production processes should normally use [`Self::start`] so incomplete
    /// operations are reconciled before serving Flight SQL.
    #[must_use]
    pub fn new(parser: DdlParser, store: Arc<dyn CatalogStore>) -> Self {
        Self { parser, store }
    }

    /// Creates the service and reconciles every incomplete durable saga before
    /// returning it to the caller.
    ///
    /// # Errors
    ///
    /// Returns an application-database error if the incomplete-saga scan cannot
    /// run. Individual retry failures are retained in the report and in durable
    /// saga state so startup can remain observable rather than silently losing
    /// work.
    pub async fn start(parser: DdlParser, store: Arc<dyn CatalogStore>) -> CatalogResult<(Self, ReconciliationReport)> {
        let service = Self::new(parser, store);
        let report = service.reconcile_incomplete().await?;
        Ok((service, report))
    }

    /// Returns the authoritative store.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn CatalogStore> {
        &self.store
    }

    /// Reconciles all incomplete schema changes idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error only when the durable scan itself fails. Per-saga
    /// failures are recorded and returned in [`ReconciliationReport`].
    pub async fn reconcile_incomplete(&self) -> CatalogResult<ReconciliationReport> {
        let sagas = self.store.incomplete_sagas().await?;
        let mut report = ReconciliationReport {
            examined: sagas.len(),
            ..ReconciliationReport::default()
        };
        for saga in sagas {
            match self.drive_saga(saga.clone()).await {
                Ok(_) => report.completed += 1,
                Err(error) => report.failures.push((saga.id(), error)),
            }
        }
        Ok(report)
    }

    async fn execute_command(&self, command: DdlCommand) -> CatalogResult<DdlCompletion> {
        let request_hash = RequestHash::from_canonical_ddl(command.canonical_ddl());
        if let Some(existing) = self.store.operation(command.identity(), request_hash).await? {
            let completed = self.drive_saga(existing).await?;
            return Ok(completion(
                completed.target_schema(),
                Some(completed.id()),
                DdlCompletionDisposition::AlreadyApplied,
            ));
        }

        let (reservation, new_disposition) = match command {
            DdlCommand::CreateTable(create) => {
                let (identity, fields, canonical_ddl) = create.into_parts();
                let target = CatalogSchemaVersion::new(
                    crate::deterministic_table_id(&identity),
                    identity,
                    SchemaVersion::FIRST,
                    fields,
                    canonical_ddl,
                )?;
                (
                    ChangeReservation {
                        request_hash,
                        from_schema: None,
                        target_schema: target,
                    },
                    DdlCompletionDisposition::Created,
                )
            }
            DdlCommand::AddColumns(add) => {
                let current = self.store.current_schema(add.identity()).await?.ok_or_else(|| {
                    CatalogError::new(
                        CatalogErrorCode::TableNotFound,
                        format!("catalog table {} does not exist", add.identity()),
                    )
                })?;
                let mut names = current
                    .fields()
                    .iter()
                    .map(|field| field.name().to_owned())
                    .collect::<BTreeSet<_>>();
                let mut fields = current.fields().to_vec();
                let mut position = current.next_position()?;
                let mut field_number = current.next_field_number()?;
                for field in add.fields() {
                    if !names.insert(field.name().to_owned()) {
                        return Err(CatalogError::new(
                            CatalogErrorCode::DdlBreakingMutation,
                            format!("field {} already exists and cannot be redefined", field.name()),
                        ));
                    }
                    fields.push(field.materialize(position, field_number)?);
                    position = position.checked_add(1).ok_or_else(field_counter_exhausted)?;
                    field_number = field_number.checked_add(1).ok_or_else(field_counter_exhausted)?;
                }
                let target = CatalogSchemaVersion::new(
                    current.table_id(),
                    current.identity().clone(),
                    current.version().next()?,
                    fields,
                    add.canonical_ddl(),
                )?;
                (
                    ChangeReservation {
                        request_hash,
                        from_schema: Some(current),
                        target_schema: target,
                    },
                    DdlCompletionDisposition::Altered,
                )
            }
        };

        match self.store.reserve_change(reservation).await? {
            ReserveOutcome::AlreadyCurrent(schema) => {
                Ok(completion(&schema, None, DdlCompletionDisposition::AlreadyApplied))
            }
            ReserveOutcome::Existing(saga) => {
                let saga = self.drive_saga(saga).await?;
                Ok(completion(
                    saga.target_schema(),
                    Some(saga.id()),
                    DdlCompletionDisposition::AlreadyApplied,
                ))
            }
            ReserveOutcome::Reserved(saga) => {
                let saga = self.drive_saga(saga).await?;
                Ok(completion(saga.target_schema(), Some(saga.id()), new_disposition))
            }
        }
    }

    async fn drive_saga(&self, mut saga: SagaRecord) -> CatalogResult<SagaRecord> {
        loop {
            match saga.step() {
                SagaStep::PersistArtifacts => {
                    let artifacts = match generate_descriptor_artifacts(saga.target_schema()) {
                        Ok(artifacts) => artifacts,
                        Err(error) => {
                            self.record_failure(&saga, SagaStep::PersistArtifacts, &error).await;
                            return Err(error);
                        }
                    };
                    saga = match self.store.persist_artifacts(saga.id(), &artifacts).await {
                        Ok(saga) => saga,
                        Err(error) => {
                            self.record_failure(&saga, SagaStep::PersistArtifacts, &error).await;
                            return Err(error);
                        }
                    };
                }
                SagaStep::Finalize => {
                    saga = match self.store.finalize(saga.id()).await {
                        Ok(saga) => saga,
                        Err(error) => {
                            self.record_failure(&saga, SagaStep::Finalize, &error).await;
                            return Err(error);
                        }
                    };
                }
                SagaStep::Complete => return Ok(saga),
            }
        }
    }

    async fn record_failure(&self, saga: &SagaRecord, step: SagaStep, error: &CatalogError) {
        let _ignored = self.store.record_failure(saga.id(), step, error).await;
    }
}

#[async_trait]
impl FlightSqlDdlHandler for CatalogControlPlane {
    async fn execute_ddl(&self, request: FlightSqlDdlRequest) -> CatalogResult<DdlCompletion> {
        self.execute_command(self.parser.parse(request.command())?).await
    }
}

fn completion(
    schema: &CatalogSchemaVersion,
    saga_id: Option<SagaId>,
    disposition: DdlCompletionDisposition,
) -> DdlCompletion {
    DdlCompletion {
        identity: schema.identity().clone(),
        schema_version: schema.version(),
        fingerprint: schema.fingerprint(),
        saga_id,
        disposition,
    }
}

fn field_counter_exhausted() -> CatalogError {
    CatalogError::new(
        CatalogErrorCode::CorruptCatalogState,
        "field position or protobuf number counter is exhausted",
    )
}
