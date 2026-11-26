use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use arrowcol_catalog::{
    DdlCommand, DdlParser, ParsedCreateTable, SchemaVersion, materialize_protobuf_properties,
    protobuf_option_definitions, protobuf_table_metadata,
};
use arrowcol_catalog_client::{InternalRequestSigner, rpc};
use arrowcol_protocol::{CatalogName, SchemaName, TableIdentity, TableName};
use arrowcol_sdk_gen::{
    ArtifactBundle, SafeOutputPath, bundle_from_snapshot, generate_csharp_models, generate_sdk, write_artifact_bundle,
};
use clap::Subcommand;
use prost::Message;
use serde_json::json;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};

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
    /// Register the bundled public DMV definitions or explicit CREATE TABLE files and generate pinned C# models.
    Seed {
        #[arg(long)]
        endpoint: String,
        /// A file containing one managed CREATE TABLE. Repeat for multiple definitions; omit to use the public DMVs.
        #[arg(long)]
        ddl: Vec<PathBuf>,
        /// Workspace-relative output below .temp or target, with a model directory per table.
        #[arg(long)]
        out: PathBuf,
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

fn read_definition(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err("definition must be a regular non-symlink SQL file".into());
    }
    Ok(fs::read_to_string(path)?)
}

fn parse_definition(text: &str) -> Result<ParsedCreateTable, Box<dyn std::error::Error>> {
    let DdlCommand::CreateTable(create) = DdlParser::new("UTC").parse(text)? else {
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
    Ok(create)
}

fn definition(path: &Path) -> Result<(String, String), Box<dyn std::error::Error>> {
    let create = parse_definition(&read_definition(path)?)?;
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

fn catalog_request<T: Message>(message: T, method: &str) -> Result<Request<T>, Box<dyn std::error::Error>> {
    let mut request = Request::new(message);
    if let Some(path) = std::env::var_os("ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE") {
        InternalRequestSigner::from_file(path)?
            .sign_message(&mut request, &format!("/arrowcol.catalog.v1.SchemaCatalog/{method}"))?;
    }
    Ok(request)
}

async fn seed(
    endpoint: String,
    ddl: Vec<PathBuf>,
    out: PathBuf,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let workspace = std::env::current_dir()?;
    SafeOutputPath::new(workspace.clone(), out.clone())?;
    let definitions = if ddl.is_empty() {
        let public_fixture = include_str!("../../../projects/loadtest/schemas/sqlserver.sql");
        let sql = public_fixture
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        sql.split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty())
            .map(parse_definition)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        ddl.iter()
            .map(|path| parse_definition(&read_definition(path)?))
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut identities = BTreeSet::new();
    for create in &definitions {
        if !identities.insert(create.identity().to_string()) {
            return Err(format!("duplicate seed definition for {}", create.identity()).into());
        }
    }

    let mut client = rpc::schema_catalog_client::SchemaCatalogClient::new(channel(endpoint).await?);
    let mut tables = Vec::new();
    for create in definitions {
        let identity = create.identity();
        let table = identity.to_string();
        let (snapshot, created) = match client
            .prepare_table(catalog_request(
                rpc::PrepareTableRequest {
                    ddl: create.canonical_ddl().to_owned(),
                },
                "PrepareTable",
            )?)
            .await
        {
            Ok(response) => (response.into_inner(), true),
            Err(error) if error.code() == Code::AlreadyExists => {
                let result = client
                    .get_table(catalog_request(
                        rpc::TableRequest {
                            catalog: identity.catalog().to_string(),
                            database: identity.schema().to_string(),
                            table: identity.table().to_string(),
                        },
                        "GetTable",
                    )?)
                    .await
                    .map_err(|read| format!("{table}: prepare failed ({error}); read failed ({read})"))?;
                (result.into_inner(), false)
            }
            Err(error) => return Err(format!("{table}: prepare failed: {error}").into()),
        };
        let bundle = bundle_from_snapshot(&snapshot, identity, None)?;
        verify_seed_schema(&create, &bundle)?;
        let path = out.join(&table);
        let sdk = SafeOutputPath::new(workspace.clone(), path.clone())?;
        generate_csharp_models(&bundle, &sdk)?;

        if created {
            let committed = client
                .promote_table(catalog_request(
                    rpc::PromoteTableRequest {
                        candidate: Some(snapshot),
                    },
                    "PromoteTable",
                )?)
                .await
                .map_err(|error| format!("{table}: promotion failed: {error}"))?
                .into_inner();
            let committed = bundle_from_snapshot(&committed, identity, None)?;
            if committed.manifest() != bundle.manifest() {
                return Err(format!("{table}: committed contract differs from the prepared one").into());
            }
        }
        tables.push(json!({
            "table": table,
            "version": bundle.schema().version().get(),
            "fingerprint": bundle.schema().fingerprint().to_string(),
            "created": created,
            "csharpModels": path.join("models/csharp"),
        }));
    }
    Ok(json!({ "tables": tables }))
}

fn verify_seed_schema(create: &ParsedCreateTable, bundle: &ArtifactBundle) -> Result<(), Box<dyn std::error::Error>> {
    let schema = bundle.schema();
    let mut committed_metadata = schema.metadata().clone();
    committed_metadata.location = None;
    let mut expected_metadata = create.metadata().clone();
    expected_metadata.properties = materialize_protobuf_properties(&expected_metadata.properties)?;
    if schema.version() != SchemaVersion::FIRST
        || schema.fields() != create.fields()
        || committed_metadata != expected_metadata
    {
        return Err(format!(
            "{}: catalog contract differs from the requested v1 definition",
            create.identity()
        )
        .into());
    }
    Ok(())
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
        CatalogCommand::Seed { endpoint, ddl, out } => seed(endpoint, ddl, out).await?,
        CatalogCommand::Prepare { endpoint, ddl, out } => {
            let (table, ddl) = definition(&ddl)?;
            let snapshot = rpc::schema_catalog_client::SchemaCatalogClient::new(channel(endpoint).await?)
                .prepare_table(catalog_request(rpc::PrepareTableRequest { ddl }, "PrepareTable")?)
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
                .promote_table(catalog_request(
                    rpc::PromoteTableRequest {
                        candidate: Some(candidate),
                    },
                    "PromoteTable",
                )?)
                .await?
                .into_inner();
            let receipt = if committed.native_receipt_json.is_empty() {
                None
            } else {
                Some(serde_json::from_slice::<serde_json::Value>(
                    &committed.native_receipt_json,
                )?)
            };
            let committed = bundle_from_snapshot(&committed, &identity, Some(SchemaVersion::FIRST))?;
            if committed.schema() != prepared.schema() || committed.manifest() != prepared.manifest() {
                return Err("committed version differs from the entire prepared contract".into());
            }
            json!({ "table": name, "registered": true, "registrationReceipt": receipt })
        }
        CatalogCommand::List { endpoint } => {
            let snapshot = rpc::schema_catalog_client::SchemaCatalogClient::new(channel(endpoint).await?)
                .get_snapshot(catalog_request(rpc::Empty {}, "GetSnapshot")?)
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
                    "comment": snapshot.comment,
                    "registrationReceipt": if snapshot.native_receipt_json.is_empty() {
                        None
                    } else {
                        Some(serde_json::from_slice::<serde_json::Value>(&snapshot.native_receipt_json)?)
                    }
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
            let name = identity(&table)?;
            let snapshot = rpc::schema_catalog_client::SchemaCatalogClient::new(channel(endpoint).await?)
                .get_table(catalog_request(
                    rpc::TableRequest {
                        catalog: name.catalog().to_string(),
                        database: name.schema().to_string(),
                        table: name.table().to_string(),
                    },
                    "GetTable",
                )?)
                .await?
                .into_inner();
            let bundle = bundle_from_snapshot(&snapshot, &name, version.map(SchemaVersion::new).transpose()?)?;
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
