use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use arrow_flight::sql::client::FlightSqlServiceClient;
use arrowcol_catalog::{CatalogSchemaVersion, SchemaVersion};
use arrowcol_catalog_client::{CatalogClient, rpc};
use arrowcol_protocol::TableIdentity;
use tonic::transport::Endpoint;

use crate::{ArtifactBundle, SdkGenError, SdkGenResult, verify_compatibility};

/// Validates committed catalog artifacts and selects an immutable schema version.
///
/// # Errors
///
/// Rejects empty, inconsistent, non-consecutive or incompatible schema history,
/// mismatched table identities/artifacts, and an unregistered requested version.
pub fn bundle_from_snapshot(
    snapshot: &rpc::TableSnapshot,
    identity: &TableIdentity,
    version: Option<SchemaVersion>,
) -> SdkGenResult<ArtifactBundle> {
    let table = snapshot
        .table
        .as_ref()
        .ok_or_else(|| SdkGenError::Artifact("catalog snapshot lacks a table identity".to_owned()))?;
    if table.catalog != identity.catalog().as_str()
        || table.database != identity.schema().as_str()
        || table.table != identity.table().as_str()
    {
        return Err(SdkGenError::Artifact(
            "catalog snapshot addresses a different table".to_owned(),
        ));
    }
    let history: Vec<CatalogSchemaVersion> = serde_json::from_slice(&snapshot.schema_history_json)?;
    let mut bundles: Vec<ArtifactBundle> = Vec::with_capacity(history.len());
    for schema in history {
        if schema.identity() != identity {
            return Err(SdkGenError::Artifact(
                "catalog history addresses a different table".to_owned(),
            ));
        }
        let bundle = ArtifactBundle::from_schema(schema)?;
        if let Some(previous) = bundles.last() {
            let next = previous
                .schema()
                .version()
                .next()
                .map_err(|error| SdkGenError::Artifact(error.to_string()))?;
            if bundle.schema().version() != next {
                return Err(SdkGenError::Artifact(
                    "catalog history has a schema-version gap".to_owned(),
                ));
            }
            verify_compatibility(previous, &bundle)?;
        } else if bundle.schema().version() != SchemaVersion::FIRST {
            return Err(SdkGenError::Artifact(
                "catalog history does not start at version 1".to_owned(),
            ));
        }
        bundles.push(bundle);
    }
    let latest = bundles
        .last()
        .ok_or_else(|| SdkGenError::Artifact("catalog returned no committed schema versions".to_owned()))?;
    if latest.manifest() != snapshot.manifest
        || latest.arrow_schema() != snapshot.arrow_schema_ipc
        || latest.protobuf_descriptor() != snapshot.protobuf_descriptor
    {
        return Err(SdkGenError::Artifact(
            "catalog head artifacts differ from its committed schema".to_owned(),
        ));
    }
    let selected = version.unwrap_or(latest.schema().version());
    bundles
        .into_iter()
        .find(|bundle| bundle.schema().version() == selected)
        .ok_or_else(|| SdkGenError::Artifact(format!("catalog has no registered version {}", selected.get())))
}

/// Reads a schema contract through the shared catalog-client abstraction.
///
/// # Errors
///
/// Returns catalog access or committed-artifact consistency errors.
pub async fn catalog_artifacts(
    client: &impl CatalogClient,
    identity: &TableIdentity,
    version: Option<SchemaVersion>,
) -> SdkGenResult<ArtifactBundle> {
    let cached = client
        .table(identity)
        .await
        .map_err(|error| SdkGenError::Artifact(error.to_string()))?;
    bundle_from_snapshot(&cached.snapshot, identity, version)
}

/// Hydrates ordered Spark SQL files through Flight SQL and fetches committed artifacts.
///
/// Each file is one DDL statement. No schema is fabricated from local DDL:
/// generation uses only the catalog's committed immutable history.
///
/// # Errors
///
/// Returns file, connection, DDL execution, catalog RPC, or artifact-validation errors.
pub async fn hydrate_catalog(
    endpoint: &str,
    ddl: &[PathBuf],
    identity: &TableIdentity,
    version: Option<SchemaVersion>,
) -> SdkGenResult<ArtifactBundle> {
    let statements = ddl
        .iter()
        .map(|path| read_ddl(path))
        .collect::<SdkGenResult<Vec<_>>>()?;
    let channel = Endpoint::from_shared(endpoint.to_owned())
        .map_err(|error| SdkGenError::Artifact(format!("catalog endpoint: {error}")))?
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .map_err(|error| SdkGenError::Artifact(format!("catalog connection failed: {error}")))?;
    let mut flight = FlightSqlServiceClient::new(channel.clone());
    for (path, statement) in ddl.iter().zip(statements) {
        flight
            .execute_update(statement, None)
            .await
            .map_err(|error| SdkGenError::Artifact(format!("catalog DDL {} failed: {error}", path.display())))?;
    }
    let mut client = rpc::schema_catalog_client::SchemaCatalogClient::new(channel);
    let snapshot = client
        .get_table(rpc::TableRequest {
            catalog: identity.catalog().as_str().to_owned(),
            database: identity.schema().as_str().to_owned(),
            table: identity.table().as_str().to_owned(),
        })
        .await
        .map_err(|error| SdkGenError::Artifact(format!("catalog GetTable failed: {error}")))?
        .into_inner();
    bundle_from_snapshot(&snapshot, identity, version)
}

fn read_ddl(path: &Path) -> SdkGenResult<String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| SdkGenError::io(path, error))?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || path.extension().and_then(|extension| extension.to_str()) != Some("sql")
    {
        return Err(SdkGenError::Artifact(format!(
            "DDL must be a regular non-symlink .sql file: {}",
            path.display()
        )));
    }
    fs::read_to_string(path).map_err(|error| SdkGenError::io(path, error))
}
