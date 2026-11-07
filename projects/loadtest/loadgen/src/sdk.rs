use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow_schema::SchemaRef;
use arrowcol_catalog::SchemaVersion;
use arrowcol_catalog_client::{CatalogClient, RemoteCatalogClient};
use arrowcol_protocol::{CatalogName, SchemaName, TableIdentity, TableName};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::process::Command;

use crate::{
    cli::RunArgs,
    manifest::{ManifestWriter, SchemaContract, SchemaFieldContract},
    schema::schema_statements,
    transport::FlightTransport,
};

/// Generated table SDK metadata and exact Arrow schema consumed by the client.
#[derive(Clone)]
pub struct GeneratedTable {
    pub identity: TableIdentity,
    pub schema_version: u64,
    pub fingerprint: String,
    pub descriptor_path: Vec<String>,
    pub schema: SchemaRef,
    pub generated_root: PathBuf,
}

impl GeneratedTable {
    #[must_use]
    pub fn contract(&self) -> SchemaContract {
        SchemaContract {
            table: self.identity.to_string(),
            schema_version: self.schema_version,
            fingerprint: self.fingerprint.clone(),
            descriptor_path: self.descriptor_path.clone(),
            fields: self
                .schema
                .fields()
                .iter()
                .map(|field| SchemaFieldContract {
                    name: field.name().clone(),
                    arrow_type: field.data_type().to_string(),
                    nullable: field.is_nullable(),
                })
                .collect(),
        }
    }
}

/// Registers all schemas through the running Flight SQL endpoint, exports the
/// catalog's immutable artifacts, invokes `arrowcol-sdk-gen`, and loads only
/// generated SDK manifests/descriptors for data construction.
///
/// # Errors
///
/// Returns schema registration, catalog, generation, artifact, or filesystem failures.
pub async fn prepare_tables(
    arguments: &RunArgs,
    run_id: &str,
    transport: &FlightTransport,
    manifests: &ManifestWriter,
) -> Result<Vec<Arc<GeneratedTable>>> {
    validate_scratch_root(&arguments.scratch_root)?;
    let run_component = safe_component(run_id)?;
    let run_root = arguments.scratch_root.join(run_component);
    let artifact_root = run_root.join("artifacts");
    let generated_root = run_root.join("generated");
    fs::create_dir_all(&artifact_root)?;
    fs::create_dir_all(&generated_root)?;

    let store = RemoteCatalogClient::connect(&arguments.catalog_endpoint, Duration::from_secs(10)).await?;

    let mut tables = Vec::new();
    for statement in schema_statements()? {
        let completion = transport
            .register_schema(&statement.ddl, &statement.identity, arguments.request_timeout.get())
            .await
            .with_context(|| format!("register {}", statement.identity))?;
        if completion.catalog() != statement.identity.catalog().as_str()
            || completion.schema() != statement.identity.schema().as_str()
            || completion.table() != statement.identity.table().as_str()
        {
            bail!("Flight SQL completion identity disagrees with {}", statement.identity);
        }
        let version = completion.schema_version();
        let current = store.table(&statement.identity).await?;
        let head = current
            .versions
            .last()
            .with_context(|| format!("catalog history missing after registration for {}", statement.identity))?;
        if head.version() != version || head.fingerprint() != completion.schema_fingerprint() {
            bail!(
                "catalog head disagrees with Flight SQL completion for {}",
                statement.identity
            );
        }
        let artifacts = current.snapshot;
        let table_artifacts = artifact_root.join(statement.identity.table().as_str());
        replace_directory(&table_artifacts)?;
        fs::write(table_artifacts.join("manifest.json"), artifacts.manifest)?;
        fs::write(table_artifacts.join("schema.arrow"), artifacts.arrow_schema_ipc)?;
        fs::write(table_artifacts.join("descriptor.pb"), artifacts.protobuf_descriptor)?;

        let table_generated = generated_root.join(statement.identity.table().as_str());
        invoke_generator(
            &arguments.sdk_generator,
            "generate",
            &table_artifacts,
            &table_generated,
            arguments.request_timeout.get(),
        )
        .await?;
        invoke_generator(
            &arguments.sdk_generator,
            "check",
            &table_artifacts,
            &table_generated,
            arguments.request_timeout.get(),
        )
        .await?;
        let generated = load_generated_table(&table_generated, &statement.identity, version)?;
        if generated.schema.fields().len() != statement.field_count {
            bail!(
                "generated schema field count differs for {}: {} != {}",
                statement.identity,
                generated.schema.fields().len(),
                statement.field_count
            );
        }
        tables.push(Arc::new(generated));
    }
    tables.sort_by(|left, right| left.identity.cmp(&right.identity));
    let contracts = tables.iter().map(|table| table.contract()).collect::<Vec<_>>();
    manifests
        .write_json(crate::manifest::SCHEMA_CONTRACT_FILE, &contracts)
        .await?;
    Ok(tables)
}

fn validate_scratch_root(path: &Path) -> Result<()> {
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("load-generator scratch root must be workspace-relative without traversal");
    }
    if !path.components().any(
        |component| matches!(component, std::path::Component::Normal(value) if value == ".temp" || value == "target"),
    ) {
        bail!("load-generator scratch root must be below .temp or target");
    }
    Ok(())
}

fn safe_component(value: &str) -> Result<&str> {
    if value.is_empty()
        || value.len() > 128
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("internal correlation ID must be 1..=128 ASCII letters, digits, '.', '_' or '-'");
    }
    Ok(value)
}

fn replace_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            bail!("artifact output is not a real directory: {}", path.display());
        }
        Ok(_) => fs::remove_dir_all(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::create_dir_all(path)?;
    Ok(())
}

async fn invoke_generator(
    executable: &Path,
    operation: &str,
    artifacts: &Path,
    output: &Path,
    timeout: Duration,
) -> Result<()> {
    let child = Command::new(executable)
        .arg(operation)
        .arg("--artifacts")
        .arg(artifacts)
        .arg("--out")
        .arg(output)
        .kill_on_drop(true)
        .output();
    let result = tokio::time::timeout(timeout, child)
        .await
        .with_context(|| format!("arrowcol-sdk-gen {operation} timed out"))??;
    if !result.status.success() {
        bail!(
            "arrowcol-sdk-gen {operation} failed ({}):\n{}\n{}",
            result.status,
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
    }
    Ok(())
}

fn load_generated_table(
    root: &Path,
    expected_identity: &TableIdentity,
    expected_version: SchemaVersion,
) -> Result<GeneratedTable> {
    let manifest_bytes = fs::read(root.join("sdk-manifest.json"))?;
    let manifest: SdkManifest = serde_json::from_slice(&manifest_bytes)?;
    if manifest.manifest_version != 1 || manifest.generator.name != "arrowcol-sdk-gen" {
        bail!("generated SDK manifest has an unsupported identity or version");
    }
    let identity = TableIdentity::new(
        CatalogName::new(&manifest.table.catalog)?,
        SchemaName::new(&manifest.table.schema)?,
        TableName::new(&manifest.table.table)?,
    );
    if &identity != expected_identity || manifest.table.version != expected_version.get() {
        bail!("generated SDK manifest identity/version disagrees with catalog");
    }
    if manifest.flight.routing_metadata.catalog != manifest.table.catalog
        || manifest.flight.routing_metadata.schema != manifest.table.schema
        || manifest.flight.routing_metadata.table != manifest.table.table
        || manifest.flight.routing_metadata.schema_fingerprint != manifest.table.fingerprint
        || manifest.flight.routing_metadata.protocol_version != "1"
        || manifest.flight.descriptor_path
            != [
                manifest.table.catalog.clone(),
                manifest.table.schema.clone(),
                manifest.table.table.clone(),
                format!("v{}", manifest.table.version),
            ]
    {
        bail!("generated SDK Flight manifest is internally inconsistent");
    }
    if manifest.artifacts.arrow_schema.path != "descriptors/schema.arrow" {
        bail!("generated SDK Arrow schema path is non-canonical");
    }
    let schema_bytes = fs::read(root.join(&manifest.artifacts.arrow_schema.path))?;
    let digest = format!("sha256:{}", hex::encode(Sha256::digest(&schema_bytes)));
    if digest != manifest.artifacts.arrow_schema.sha256 {
        bail!("generated SDK Arrow schema digest mismatch");
    }
    let schema = Arc::new(
        try_schema_from_flatbuffer_bytes(&schema_bytes).context("decode generated SDK Arrow schema descriptor")?,
    );
    if schema.fields().iter().any(|field| field.is_nullable()) {
        bail!("generated SQL Server SDK schema unexpectedly contains nullable fields");
    }
    Ok(GeneratedTable {
        identity,
        schema_version: manifest.table.version,
        fingerprint: manifest.table.fingerprint,
        descriptor_path: manifest.flight.descriptor_path,
        schema,
        generated_root: root.to_path_buf(),
    })
}

#[derive(Deserialize)]
struct SdkManifest {
    manifest_version: u32,
    generator: GeneratorManifest,
    table: TableManifest,
    artifacts: ArtifactManifest,
    flight: FlightManifest,
}

#[derive(Deserialize)]
struct GeneratorManifest {
    name: String,
}

#[derive(Deserialize)]
struct TableManifest {
    catalog: String,
    schema: String,
    table: String,
    version: u64,
    fingerprint: String,
}

#[derive(Deserialize)]
struct ArtifactManifest {
    arrow_schema: ArtifactFile,
}

#[derive(Deserialize)]
struct ArtifactFile {
    path: String,
    sha256: String,
}

#[derive(Deserialize)]
struct FlightManifest {
    descriptor_path: Vec<String>,
    routing_metadata: RoutingManifest,
}

#[derive(Deserialize)]
struct RoutingManifest {
    protocol_version: String,
    catalog: String,
    schema: String,
    table: String,
    schema_fingerprint: String,
}
