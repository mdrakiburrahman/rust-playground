use std::sync::Arc;

use arrow_schema::Schema;
use arrowcol_protocol::{ClaimedSchemaFingerprint, TableIdentity, fingerprint_schema};

use crate::{
    CatalogError, CatalogErrorCode, CatalogField, CatalogResult, CatalogSchemaVersion, CatalogStore, SchemaVersion,
};

/// One target-column action used to align a historical registered schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlignmentAction {
    /// Copy the exact source column into the same target field.
    Source {
        /// Zero-based source column index.
        source_index: usize,
        /// Zero-based current-schema target index.
        target_index: usize,
    },
    /// Materialize an all-null array for a newly appended nullable field.
    NullFill {
        /// Zero-based current-schema target index.
        target_index: usize,
        /// Appended target field.
        field: CatalogField,
    },
}

/// Exact or nullable-append alignment plan for a registered incoming schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaAlignmentPlan {
    /// Incoming schema is the current registered version.
    Exact {
        /// Current schema version.
        version: SchemaVersion,
    },
    /// Incoming schema is an older registered prefix of the current version.
    NullableAppend {
        /// Registered incoming version.
        source_version: SchemaVersion,
        /// Current target version.
        target_version: SchemaVersion,
        /// Ordered source-copy and null-fill actions.
        actions: Vec<AlignmentAction>,
    },
}

/// Successful exact incoming-schema validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedIncomingSchema {
    registered: CatalogSchemaVersion,
    current: CatalogSchemaVersion,
    alignment: SchemaAlignmentPlan,
}

impl ValidatedIncomingSchema {
    /// Returns the exact historical or current version sent by the client.
    #[must_use]
    pub const fn registered(&self) -> &CatalogSchemaVersion {
        &self.registered
    }

    /// Returns the current authoritative schema head.
    #[must_use]
    pub const fn current(&self) -> &CatalogSchemaVersion {
        &self.current
    }

    /// Returns the explicit exact/copy/null-fill plan.
    #[must_use]
    pub const fn alignment(&self) -> &SchemaAlignmentPlan {
        &self.alignment
    }
}

/// Validates incoming Arrow schemas against immutable catalog history.
#[derive(Clone)]
pub struct SchemaValidator {
    store: Arc<dyn CatalogStore>,
}

impl SchemaValidator {
    /// Creates a validator over an authoritative catalog store.
    #[must_use]
    pub fn new(store: Arc<dyn CatalogStore>) -> Self {
        Self { store }
    }

    /// Validates a claimed and received Arrow schema exactly.
    ///
    /// An arbitrary prefix is rejected even if it could be null-filled. Only a
    /// byte-for-byte registered Arrow schema is accepted. A historical
    /// registered version receives a null-fill plan only when the current
    /// version is a nullable append-only superset.
    ///
    /// # Errors
    ///
    /// Returns a structured fingerprint, table, or compatibility error.
    pub async fn validate(
        &self,
        identity: &TableIdentity,
        claimed: ClaimedSchemaFingerprint,
        incoming: &Schema,
    ) -> CatalogResult<ValidatedIncomingSchema> {
        let actual_fingerprint = fingerprint_schema(incoming);
        if claimed.fingerprint() != actual_fingerprint {
            return Err(CatalogError::new(
                CatalogErrorCode::UnknownSchemaFingerprint,
                "claimed schema fingerprint does not match the received Arrow schema",
            ));
        }

        let history = self.store.schema_history(identity).await?;
        let current = history.last().cloned().ok_or_else(|| {
            CatalogError::new(
                CatalogErrorCode::TableNotFound,
                format!("catalog table {identity} does not exist"),
            )
        })?;
        let registered = history
            .into_iter()
            .find(|schema| schema.fingerprint() == actual_fingerprint)
            .ok_or_else(|| {
                CatalogError::new(
                    CatalogErrorCode::UnknownSchemaFingerprint,
                    format!("schema fingerprint {actual_fingerprint} is not registered for {identity}"),
                )
            })?;
        let registered_arrow = registered.arrow_schema()?;
        if registered_arrow != *incoming {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                "registered fingerprint resolved to a non-identical Arrow schema",
            ));
        }

        let alignment = if registered.version() == current.version() {
            SchemaAlignmentPlan::Exact {
                version: current.version(),
            }
        } else if current.is_nullable_append_only_superset_of(&registered) {
            let actions = current
                .fields()
                .iter()
                .enumerate()
                .map(|(target_index, field)| {
                    if target_index < registered.fields().len() {
                        AlignmentAction::Source {
                            source_index: target_index,
                            target_index,
                        }
                    } else {
                        AlignmentAction::NullFill {
                            target_index,
                            field: field.clone(),
                        }
                    }
                })
                .collect();
            SchemaAlignmentPlan::NullableAppend {
                source_version: registered.version(),
                target_version: current.version(),
                actions,
            }
        } else {
            return Err(CatalogError::new(
                CatalogErrorCode::SchemaIncompatible,
                format!(
                    "registered schema version {} is not a nullable append-only prefix of current version {}",
                    registered.version(),
                    current.version()
                ),
            ));
        };
        Ok(ValidatedIncomingSchema {
            registered,
            current,
            alignment,
        })
    }
}
