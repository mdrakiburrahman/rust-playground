use std::{fs, path::PathBuf, time::Duration};

use arrowcol_catalog::{
    DdlCommand, DdlParser, SchemaVersion, materialize_protobuf_properties, protobuf_option_definitions,
    protobuf_table_metadata,
};
use arrowcol_catalog_client::rpc;
use arrowcol_protocol::{CatalogName, SchemaName, TableIdentity, TableName};
use arrowcol_sdk_gen::{SafeOutputPath, bundle_from_snapshot, hydrate_catalog, write_artifact_bundle};
use clap::Subcommand;
use prost::Message;
use serde_json::json;
use tonic::transport::{Channel, Endpoint};

#[derive(Debug, Subcommand)]
pub enum CatalogCommand {
    /// List extensible properties and native defaults from compiled option descriptors.
    Properties,
    /// Decode portable table metadata using only an exported descriptor set.
    Metadata {
        #[arg(long)]
        descriptor: PathBuf,
    },
    /// Validate exactly one CREATE TABLE using the catalog's own parser.
    Validate {
        #[arg(long)]
        ddl: PathBuf,
    },
    /// Freeze a candidate at the authority without registering a table.
    Prepare {
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        ddl: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    /// Register the exact previously prepared candidate after offline verification.
    Promote {
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        candidate: PathBuf,
    },
    /// Read and validate all committed tables.
    List {
        #[arg(long)]
        endpoint: String,
    },
    /// Pin a committed version without executing DDL.
    Export {
        #[arg(long)]
        endpoint: String,
        #[arg(long)]
        table: String,
        #[arg(long)]
        version: Option<u64>,
        #[arg(long)]
        out: PathBuf,
    },
}

fn definition(path: &PathBuf) -> Result<(String, String), Box<dyn std::error::Error>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err("definition must be a regular non-symlink SQL file".into());
    }
    let text = fs::read_to_string(path)?;
    let DdlCommand::CreateTable(create) = DdlParser::new("UTC").parse(&text)? else {
        return Err("contract creation requires exactly one CREATE TABLE".into());
    };
    if create.if_not_exists() {
        return Err("IF NOT EXISTS is not allowed: rebuild an explicit committed version instead".into());
    }
    arrowcol_props::validate_table_properties(&create.metadata().properties)?;
    materialize_protobuf_properties(&create.metadata().properties)?;
    if create.metadata().location.is_some() {
        return Err("contract creation uses the selected catalog's managed warehouse, not LOCATION".into());
    }
    Ok((create.identity().to_string(), create.canonical_ddl().to_owned()))
}

async fn channel(endpoint: String) -> Result<Channel, Box<dyn std::error::Error>> {
    Ok(Endpoint::from_shared(endpoint)?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .connect()
        .await?)
}

fn identity(value: &str) -> Result<TableIdentity, Box<dyn std::error::Error>> {
    let parts: Vec<_> = value.split('.').collect();
    let [catalog, schema, table] = parts.as_slice() else {
        return Err("table must be catalog.schema.table".into());
    };
    Ok(TableIdentity::new(
        CatalogName::new(*catalog)?,
        SchemaName::new(*schema)?,
        TableName::new(*table)?,
    ))
}

pub async fn run(command: CatalogCommand) -> Result<(), Box<dyn std::error::Error>> {
    let result = match command {
        CatalogCommand::Properties => serde_json::to_value(protobuf_option_definitions()?)?,
        CatalogCommand::Metadata { descriptor } => {
            let metadata = fs::symlink_metadata(&descriptor)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err("descriptor must be a regular non-symlink file".into());
            }
            serde_json::to_value(protobuf_table_metadata(&fs::read(descriptor)?)?)?
        }
        CatalogCommand::Validate { ddl } => {
            let (table, ddl) = definition(&ddl)?;
            json!({ "table": table, "ddl": ddl })
        }
        CatalogCommand::Prepare { endpoint, ddl, out } => {
            let (table, ddl) = definition(&ddl)?;
            let snapshot = rpc::schema_catalog_client::SchemaCatalogClient::new(channel(endpoint).await?)
                .prepare_table(rpc::PrepareTableRequest { ddl })
                .await?
                .into_inner();
            let bundle = bundle_from_snapshot(&snapshot, &identity(&table)?, Some(SchemaVersion::FIRST))?;
            let output = SafeOutputPath::new(std::env::current_dir()?, out)?;
            write_artifact_bundle(&bundle, &output)?;
            fs::write(output.absolute().join("candidate.pb"), snapshot.encode_to_vec())?;
            json!({
                "table": table,
                "tableId": bundle.schema().table_id().to_string(),
                "version": bundle.schema().version().get(),
                "fingerprint": bundle.schema().fingerprint().to_string()
            })
        }
        CatalogCommand::Promote { endpoint, candidate } => {
            let metadata = fs::symlink_metadata(&candidate)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err("candidate must be a regular non-symlink file".into());
            }
            let candidate = rpc::TableSnapshot::decode(fs::read(candidate)?.as_slice())?;
            let table = candidate.table.as_ref().ok_or("candidate lacks table")?;
            let name = format!("{}.{}.{}", table.catalog, table.database, table.table);
            let identity = identity(&name)?;
            let prepared = bundle_from_snapshot(&candidate, &identity, Some(SchemaVersion::FIRST))?;
            let committed = rpc::schema_catalog_client::SchemaCatalogClient::new(channel(endpoint).await?)
                .promote_table(rpc::PromoteTableRequest {
                    candidate: Some(candidate),
                })
                .await?
                .into_inner();
            let committed = bundle_from_snapshot(&committed, &identity, Some(SchemaVersion::FIRST))?;
            if committed.schema() != prepared.schema() || committed.manifest() != prepared.manifest() {
                return Err("committed version differs from the entire prepared contract".into());
            }
            json!({ "table": name, "registered": true })
        }
        CatalogCommand::List { endpoint } => {
            let snapshot = rpc::schema_catalog_client::SchemaCatalogClient::new(channel(endpoint).await?)
                .get_snapshot(rpc::Empty {})
                .await?
                .into_inner();
            let mut tables = Vec::new();
            for snapshot in snapshot.tables {
                let table = snapshot.table.as_ref().ok_or("snapshot lacks table")?;
                let name = format!("{}.{}.{}", table.catalog, table.database, table.table);
                let bundle = bundle_from_snapshot(&snapshot, &identity(&name)?, None)?;
                tables.push(json!({
                    "table": name,
                    "tableId": bundle.schema().table_id().to_string(),
                    "version": bundle.schema().version().get(),
                    "fingerprint": bundle.schema().fingerprint().to_string(),
                    "schema": bundle.schema(),
                    "properties": snapshot.properties,
                    "location": snapshot.location,
                    "comment": snapshot.comment
                }));
            }
            tables.sort_by(|left, right| left["table"].as_str().cmp(&right["table"].as_str()));
            json!({ "tables": tables })
        }
        CatalogCommand::Export {
            endpoint,
            table,
            version,
            out,
        } => {
            let bundle = hydrate_catalog(
                &endpoint,
                &[],
                &identity(&table)?,
                version.map(SchemaVersion::new).transpose()?,
            )
            .await?;
            let output = SafeOutputPath::new(std::env::current_dir()?, out)?;
            write_artifact_bundle(&bundle, &output)?;
            json!({
                "table": table,
                "tableId": bundle.schema().table_id().to_string(),
                "version": bundle.schema().version().get(),
                "fingerprint": bundle.schema().fingerprint().to_string()
            })
        }
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
