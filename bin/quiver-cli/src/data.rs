//! Catalog schemas and the CSV fixture used to create example Quiver bundles.

use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_csv::ReaderBuilder;
use arrowcol_catalog::{
    CatalogSchemaVersion, CatalogStore, DdlCommand, DdlParser, MemoryCatalogStore, SchemaVersion,
    deterministic_table_id,
};
use arrowcol_catalog_client::{CatalogClient, RemoteCatalogClient};
use arrowcol_protocol::{CatalogName, SchemaName, TableIdentity, TableName};

const BUILTIN_CSV: &[u8] = include_bytes!("../data/sqlserver_wait_stats.csv");
const CATALOG_SEED_DDL: &str = include_str!("../../../projects/loadtest/schemas/sqlserver.sql");
pub(crate) const BUILTIN_CSV_PATH: &str = "bin/quiver-cli/data/sqlserver_wait_stats.csv";
pub(crate) const DEFAULT_TABLE: &str = "telemetry.sqlserver.sqlserver_wait_stats";
pub(crate) const DEFAULT_CATALOG_ENDPOINT: &str = "http://127.0.0.1:8817";

/// One immutable schema version returned by the Schema Catalog.
#[derive(Clone, Debug)]
pub(crate) struct BenchmarkTable {
    pub(crate) schema: CatalogSchemaVersion,
    pub(crate) arrow: SchemaRef,
}

impl BenchmarkTable {
    pub(crate) async fn load(endpoint: &str, name: &str) -> Result<Self> {
        let identity = parse_identity(name)?;
        let schema = if endpoint == "memory" {
            load_memory_catalog_schema(&identity).await?
        } else {
            let client = RemoteCatalogClient::connect(endpoint, Duration::from_secs(10))
                .await
                .with_context(|| format!("connect to Schema Catalog at {endpoint}"))?;
            client
                .table(&identity)
                .await
                .with_context(|| format!("fetch {identity} from Schema Catalog"))?
                .versions
                .last()
                .cloned()
                .with_context(|| format!("Schema Catalog returned no versions for {identity}"))?
        };
        Self::from_schema(schema)
    }

    pub(crate) fn from_schema(schema: CatalogSchemaVersion) -> Result<Self> {
        let arrow = Arc::new(schema.arrow_schema()?);
        Ok(Self { schema, arrow })
    }
}

async fn load_memory_catalog_schema(identity: &TableIdentity) -> Result<CatalogSchemaVersion> {
    let parser = DdlParser::default();
    let ddl = CATALOG_SEED_DDL
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut schemas = Vec::new();
    for statement in ddl.split(';').map(str::trim).filter(|statement| !statement.is_empty()) {
        let DdlCommand::CreateTable(create) = parser.parse(statement)? else {
            bail!("loadtest catalog seed contains a non-CREATE statement");
        };
        let (table_identity, fields, canonical_ddl) = create.into_parts();
        schemas.push(CatalogSchemaVersion::new(
            deterministic_table_id(&table_identity),
            table_identity,
            SchemaVersion::FIRST,
            fields,
            canonical_ddl,
        )?);
    }
    let store = MemoryCatalogStore::new();
    for schema in schemas {
        store.hydrate_committed(vec![schema]).await?;
    }
    store
        .current_schema(identity)
        .await?
        .with_context(|| format!("{identity} is not in the loadtest Schema Catalog seed"))
}

fn parse_identity(name: &str) -> Result<TableIdentity> {
    let mut parts = name.split('.');
    let (Some(catalog), Some(schema), Some(table), None) = (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        bail!("table must be a fully qualified catalog.schema.table identity");
    };
    Ok(TableIdentity::new(
        catalog.parse::<CatalogName>()?,
        schema.parse::<SchemaName>()?,
        table.parse::<TableName>()?,
    ))
}

/// Reads CSV rows with the exact Arrow schema supplied by the catalog.
pub(crate) fn read_csv(schema: &SchemaRef, csv: Option<&Path>, batch_rows: usize) -> Result<Vec<RecordBatch>> {
    if batch_rows == 0 {
        bail!("--batch-rows must be positive");
    }
    let input: Box<dyn Read> = match csv {
        Some(path) => Box::new(File::open(path).with_context(|| format!("open {}", path.display()))?),
        None => Box::new(Cursor::new(BUILTIN_CSV)),
    };
    let reader = ReaderBuilder::new(Arc::clone(schema))
        .with_header(true)
        .with_batch_size(batch_rows)
        .build(input)?;
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .context("parse CSV rows with the current Schema Catalog schema")?;
    if batches.is_empty() {
        bail!("the CSV has no data rows");
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;

    #[tokio::test]
    async fn built_in_csv_matches_the_catalog_schema_and_batches() -> Result<()> {
        let table = BenchmarkTable::load("memory", DEFAULT_TABLE).await?;
        let header = std::str::from_utf8(BUILTIN_CSV)?.lines().next().context("CSV header")?;
        assert_eq!(
            table
                .arrow
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            header.split(',').collect::<Vec<_>>(),
        );
        let batches = read_csv(&table.arrow, None, 128)?;
        assert!(batches.len() > 1);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3_546);
        assert!(batches.iter().all(|batch| batch.schema() == table.arrow));
        Ok(())
    }

    #[test]
    fn table_identity_requires_three_validated_components() {
        assert!(parse_identity(DEFAULT_TABLE).is_ok());
        assert!(parse_identity("telemetry.sqlserver").is_err());
        assert!(parse_identity("telemetry.sqlserver.bad/name").is_err());
    }
}
