use std::{fs, path::PathBuf};

use arrowcol_catalog::{
    CatalogSchemaVersion, DdlCommand, DdlParser, SchemaVersion, deterministic_table_id, next_protobuf_field_number,
};
use arrowcol_protocol::{CatalogName, SchemaName, TableIdentity, TableName};
use arrowcol_sdk_gen::{
    ArtifactBundle, SafeOutputPath, check_generated_sdk, generate_sdk, hydrate_catalog, package_generated_sdk,
    verify_compatibility, write_artifact_bundle,
};
use clap::{Parser, Subcommand};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(
    name = "arrowcol-sdk-gen",
    about = "Generate deterministic Rust and TypeScript SDKs from immutable catalog artifacts"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Generate and atomically replace both language SDKs.
    Generate {
        /// Directory containing manifest.json, schema.arrow, and descriptor.pb.
        #[arg(long, conflicts_with_all = ["catalog_endpoint", "ddl", "table", "version", "export_artifacts"])]
        artifacts: Option<PathBuf>,
        /// Running catalog's Flight SQL and schema RPC endpoint.
        #[arg(long, required_unless_present = "artifacts")]
        catalog_endpoint: Option<String>,
        /// Ordered Spark SQL files to hydrate before generation; repeat for each file.
        #[arg(long)]
        ddl: Vec<PathBuf>,
        /// Fully qualified catalog.schema.table identity.
        #[arg(long, required_unless_present = "artifacts")]
        table: Option<String>,
        /// Registered historical version; default is the committed head.
        #[arg(long)]
        version: Option<u64>,
        /// Also export the selected canonical artifacts for compatibility checks.
        #[arg(long)]
        export_artifacts: Option<PathBuf>,
        /// Workspace-relative output below .temp or target.
        #[arg(long)]
        out: PathBuf,
    },
    /// Check existing generated output byte-for-byte without modifying it.
    Check {
        /// Directory containing manifest.json, schema.arrow, and descriptor.pb.
        #[arg(long)]
        artifacts: PathBuf,
        /// Workspace-relative generated output below .temp or target.
        #[arg(long)]
        out: PathBuf,
    },
    /// Verify protobuf WIRE_JSON and physical Arrow/Delta compatibility.
    Compatibility {
        /// Previous immutable artifact directory.
        #[arg(long)]
        previous: PathBuf,
        /// Candidate immutable artifact directory.
        #[arg(long)]
        current: PathBuf,
    },
    /// Build canonical catalog artifacts from a CREATE TABLE fixture.
    Fixture {
        /// UTF-8 file containing exactly one supported CREATE TABLE statement.
        #[arg(long)]
        ddl: PathBuf,
        /// Previous immutable artifacts when the fixture is an ALTER TABLE.
        #[arg(long)]
        previous: Option<PathBuf>,
        /// Workspace-relative artifact output below .temp or target.
        #[arg(long)]
        out: PathBuf,
        /// Timezone used for TIMESTAMP WITH TIME ZONE.
        #[arg(long, default_value = "UTC")]
        timezone: String,
    },
    /// Package generated output as a deterministic .tar.gz artifact.
    PackageArtifact {
        /// Workspace-relative generated SDK directory below .temp or target.
        #[arg(long)]
        input: PathBuf,
        /// Workspace-relative archive path below .temp or target.
        #[arg(long)]
        out: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("arrowcol-sdk-gen: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let workspace = std::env::current_dir()?;
    match cli.command {
        Command::Generate {
            artifacts,
            catalog_endpoint,
            ddl,
            table,
            version,
            export_artifacts,
            out,
        } => {
            let output = SafeOutputPath::new(&workspace, out)?;
            let exported = export_artifacts
                .map(|path| SafeOutputPath::new(&workspace, path))
                .transpose()?;
            let bundle = if let Some(artifacts) = artifacts {
                ArtifactBundle::load_directory(artifacts)?
            } else {
                let endpoint = catalog_endpoint.ok_or("catalog endpoint is required")?;
                let table = table.ok_or("catalog table is required")?;
                let parts = table.split('.').collect::<Vec<_>>();
                let [catalog, schema, table] = parts.as_slice() else {
                    return Err("--table must be catalog.schema.table".into());
                };
                let identity = TableIdentity::new(
                    CatalogName::new(*catalog)?,
                    SchemaName::new(*schema)?,
                    TableName::new(*table)?,
                );
                let version = version.map(SchemaVersion::new).transpose()?;
                hydrate_catalog(&endpoint, &ddl, &identity, version).await?
            };
            if let Some(exported) = exported {
                write_artifact_bundle(&bundle, &exported)?;
            }
            print_json(&generate_sdk(&bundle, &output)?)?;
        }
        Command::Check { artifacts, out } => {
            let bundle = ArtifactBundle::load_directory(artifacts)?;
            let output = SafeOutputPath::new(&workspace, out)?;
            print_json(&check_generated_sdk(&bundle, &output)?)?;
        }
        Command::Compatibility { previous, current } => {
            let previous = ArtifactBundle::load_directory(previous)?;
            let current = ArtifactBundle::load_directory(current)?;
            print_json(&verify_compatibility(&previous, &current)?)?;
        }
        Command::Fixture {
            ddl,
            previous,
            out,
            timezone,
        } => {
            let metadata = fs::symlink_metadata(&ddl)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(format!("DDL fixture must be a regular non-symlink file: {}", ddl.display()).into());
            }
            let ddl_text = fs::read_to_string(&ddl)?;
            let command = DdlParser::new(timezone).parse(&ddl_text)?;
            let schema = match (command, previous) {
                (DdlCommand::CreateTable(create), None) => CatalogSchemaVersion::new(
                    deterministic_table_id(create.identity()),
                    create.identity().clone(),
                    SchemaVersion::FIRST,
                    create.fields().to_vec(),
                    create.canonical_ddl(),
                )?
                .with_metadata(create.metadata().clone())?,
                (DdlCommand::AddColumns(add), Some(previous)) => {
                    let previous = ArtifactBundle::load_directory(previous)?;
                    let old = previous.schema();
                    if add.identity() != old.identity() {
                        return Err("ALTER fixture must address the previous table".into());
                    }
                    let mut fields = old.fields().to_vec();
                    let mut position = old.next_position()?;
                    let mut number = old.next_field_number()?;
                    for blueprint in add.fields() {
                        fields.push(blueprint.materialize(position, number)?);
                        position = position.checked_add(1).ok_or("column ordinal overflow")?;
                        number = next_protobuf_field_number(number)?;
                    }
                    let candidate = CatalogSchemaVersion::new(
                        old.table_id(),
                        old.identity().clone(),
                        old.version().next()?,
                        fields,
                        add.canonical_ddl(),
                    )?
                    .with_metadata(old.metadata().clone())?;
                    verify_compatibility(&previous, &ArtifactBundle::from_schema(candidate.clone())?)?;
                    candidate
                }
                _ => return Err("fixture requires CREATE TABLE, or ALTER TABLE with --previous".into()),
            };
            let bundle = ArtifactBundle::from_schema(schema)?;
            let output = SafeOutputPath::new(&workspace, out)?;
            write_artifact_bundle(&bundle, &output)?;
            print_json(&FixtureResult {
                output: output.relative().to_string_lossy().replace('\\', "/"),
                table: bundle.schema().identity().to_string(),
                version: bundle.schema().version().get(),
                fingerprint: bundle.schema().fingerprint().to_string(),
            })?;
        }
        Command::PackageArtifact { input, out } => {
            let input = SafeOutputPath::new(&workspace, input)?;
            let output = SafeOutputPath::new(&workspace, out)?;
            print_json(&package_generated_sdk(&input, &output)?)?;
        }
    }
    Ok(())
}

fn print_json(value: &impl Serialize) -> Result<(), serde_json::Error> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[derive(Serialize)]
struct FixtureResult {
    output: String,
    table: String,
    version: u64,
    fingerprint: String,
}
