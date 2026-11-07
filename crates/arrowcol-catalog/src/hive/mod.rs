use arrowcol_protocol::{SchemaFingerprint, TableIdentity};
use async_trait::async_trait;

use crate::{CatalogResult, CatalogSchemaVersion, SchemaVersion};

/// Schema snapshot used by the schema-control compatibility adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HiveTableSnapshot {
    identity: TableIdentity,
    schema_version: SchemaVersion,
    fingerprint: SchemaFingerprint,
    fields: Vec<(String, String)>,
    partitions: Vec<(String, String)>,
}

impl HiveTableSnapshot {
    /// Returns the table identity.
    #[must_use]
    pub const fn identity(&self) -> &TableIdentity {
        &self.identity
    }

    /// Returns the Arrowcol schema version recorded in Hive parameters.
    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns the Arrowcol schema fingerprint recorded in Hive parameters.
    #[must_use]
    pub const fn fingerprint(&self) -> SchemaFingerprint {
        self.fingerprint
    }

    /// Returns ordered non-partition Hive columns.
    #[must_use]
    pub fn fields(&self) -> &[(String, String)] {
        &self.fields
    }

    /// Returns ordered Hive partition keys.
    #[must_use]
    pub fn partitions(&self) -> &[(String, String)] {
        &self.partitions
    }

    pub(crate) fn from_schema(schema: &CatalogSchemaVersion) -> Self {
        let fields = schema
            .fields()
            .iter()
            .filter(|field| field.partition().is_none())
            .map(|field| (field.name().to_owned(), field.mapping().hive().to_owned()))
            .collect();
        let mut partitions = schema
            .fields()
            .iter()
            .filter_map(|field| {
                field.partition().map(|partition| {
                    (
                        partition.ordinal(),
                        (field.name().to_owned(), field.mapping().hive().to_owned()),
                    )
                })
            })
            .collect::<Vec<_>>();
        partitions.sort_by_key(|(ordinal, _)| *ordinal);
        Self {
            identity: schema.identity().clone(),
            schema_version: schema.version(),
            fingerprint: schema.fingerprint(),
            fields,
            partitions: partitions.into_iter().map(|(_, field)| field).collect(),
        }
    }
}

/// Schema-control compatibility operations.
#[async_trait]
pub trait Metastore: Send + Sync {
    /// Creates or alters the table idempotently and verifies the resulting
    /// Arrowcol version/fingerprint.
    async fn ensure_schema(
        &self,
        from_schema: Option<&CatalogSchemaVersion>,
        target_schema: &CatalogSchemaVersion,
    ) -> CatalogResult<HiveTableSnapshot>;

    /// Reads an Arrowcol-owned table from the compatibility adapter.
    async fn table(&self, identity: &TableIdentity) -> CatalogResult<Option<HiveTableSnapshot>>;
}
