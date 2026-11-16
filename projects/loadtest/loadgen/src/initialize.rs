//! Fenced Azure table bootstrap using the same registry as the Flight fixtures.

use std::{collections::BTreeSet, env, fs, path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::{DeltaTable, INGESTION_PARTITION};
use arrowcol_catalog::{DdlCommand, DdlParser};
use serde::Serialize;
use url::Url;

use crate::{
    cli::{InitializeAzureArgs, InitializeLocalArgs},
    schema::{SchemaStatement, schema_statements},
};

const INGESTION: &str = INGESTION_PARTITION;

#[derive(Debug, Serialize)]
struct ManifestEntry {
    name: String,
    relative_path: String,
}

fn checked_root(raw: &str, account: &str, container: &str) -> Result<Url> {
    let url = Url::parse(raw).context("invalid ARROWCOL_AZURE_TABLE_ROOT")?;
    if account.is_empty()
        || !account.bytes().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        || container.is_empty()
        || !container
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        || url.scheme() != "abfss"
        || url.username() != container
        || url.host_str() != Some(format!("{account}.dfs.core.windows.net").as_str())
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || raw.contains('%')
        || raw.contains('\\')
        || raw.split('/').any(|part| matches!(part, "." | ".."))
    {
        bail!("Azure bootstrap root must be a credential-free URL inside the configured account/container");
    }
    let mut url = url;
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

fn partitioned_schema(statement: &SchemaStatement) -> Result<Arc<Schema>> {
    let parser = DdlParser::default();
    let DdlCommand::CreateTable(create) = parser.parse(&statement.ddl)? else {
        bail!("canonical fixture contains a non-CREATE statement");
    };
    let mut fields = create
        .fields()
        .iter()
        .map(arrowcol_catalog::CatalogField::to_arrow)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    fields.push(Field::new(INGESTION, DataType::Utf8, false));
    Ok(Arc::new(Schema::new(fields)))
}

fn entries() -> Result<Vec<(ManifestEntry, Arc<Schema>)>> {
    let mut entries = Vec::new();
    for statement in schema_statements()? {
        let schema = partitioned_schema(&statement)?;
        let identity = statement.identity;
        entries.push((
            ManifestEntry {
                name: format!("dataflow_{}", identity.table()),
                relative_path: format!(
                    "dataflow/{}/{}/{}",
                    identity.catalog(),
                    identity.schema(),
                    identity.table()
                ),
            },
            schema,
        ));
    }
    Ok(entries)
}

/// Initialize only selected, fresh local table directories; existing tables are never opened.
pub(crate) fn initialize_fresh_local(root: &Path, selected: &[String], statements: &[SchemaStatement]) -> Result<()> {
    let requested = selected.iter().cloned().collect::<BTreeSet<_>>();
    if requested.is_empty() || requested.len() != selected.len() {
        bail!("partitioned local initialization requires explicit, unique --table selections");
    }
    let targets = statements
        .iter()
        .filter(|statement| requested.contains(&statement.identity.to_string()))
        .collect::<Vec<_>>();
    if targets.len() != requested.len() {
        bail!("partitioned local initialization contains an unknown table");
    }
    let root_metadata = fs::symlink_metadata(root).context("partitioned local Dataflow root must already exist")?;
    if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
        bail!("partitioned local Dataflow root must be a real directory");
    }
    let root = fs::canonicalize(root)?;
    let mut prepared = Vec::new();
    for statement in targets {
        let schema = partitioned_schema(statement)?;
        let mut parent = root.clone();
        for component in [
            statement.identity.catalog().as_str(),
            statement.identity.schema().as_str(),
        ] {
            parent.push(component);
            match fs::create_dir(&parent) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
            let metadata = fs::symlink_metadata(&parent)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                bail!("local table parent must be a real directory: {}", parent.display());
            }
        }
        let path = parent.join(statement.identity.table().as_str());
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => bail!("refusing to initialize existing local table: {}", path.display()),
            Err(error) => return Err(error.into()),
        }
        prepared.push((path, schema));
    }
    for (path, schema) in prepared {
        fs::create_dir(&path).with_context(|| format!("claim fresh local table {}", path.display()))?;
        let url = Url::from_directory_path(&path).map_err(|()| anyhow::anyhow!("invalid local table directory"))?;
        DeltaTable::initialize_url_for_bootstrap(url, schema, vec![INGESTION.to_owned()])
            .with_context(|| format!("initialize fresh local table {}", path.display()))?;
    }
    Ok(())
}

/// Initialize exactly one fresh local table without Flight, catalog registration, or load generation.
///
/// # Errors
///
/// Requires the explicit partition flag, an absolute existing local root, and one known table whose
/// destination does not already exist.
pub fn initialize_local(args: InitializeLocalArgs) -> Result<()> {
    if !args.partition_by_ingestion_date {
        bail!("setup-only local initialization requires --partition-by-ingestion-date");
    }
    if !args.dataflow_root.is_absolute() {
        bail!("setup-only local initialization requires an absolute --dataflow-root");
    }
    initialize_fresh_local(&args.dataflow_root, &[args.table], &schema_statements()?)
}

/// Initializes existing-or-empty roots and publishes a manifest only after every table succeeds.
///
/// # Errors
///
/// Fails closed for missing configuration, unsafe roots, unfenced storage, incompatible existing
/// tables, or manifest write failures. Never emits storage URLs or credentials to stdout.
pub async fn initialize(args: InitializeAzureArgs) -> Result<()> {
    let config = InitializeAzureConfig::from_lookup(|key| env::var(key).ok())?;
    initialize_tables_with(
        args,
        &config.root,
        &config.account,
        &config.container,
        DeltaTable::initialize_url_for_bootstrap,
    )
    .await
}

/// Storage configuration [`initialize`] reads; production looks keys up in the process environment.
#[derive(Clone, Debug)]
pub struct InitializeAzureConfig {
    /// Canonical ADLS table root.
    pub root: String,
    /// Owned storage account.
    pub account: String,
    /// Owned data container.
    pub container: String,
}

impl InitializeAzureConfig {
    /// Reads every required initializer key, in order, from `lookup`.
    ///
    /// # Errors
    ///
    /// Fails with `missing <KEY>` for the first absent key; the checkpoint container is required
    /// even though only its presence is checked here.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let required = |key: &str| lookup(key).with_context(|| format!("missing {key}"));
        let root = required("ARROWCOL_AZURE_TABLE_ROOT")?;
        let account = required("AZURE_STORAGE_ACCOUNT")?;
        let container = required("AZURE_STORAGE_CONTAINER")?;
        required("AZURE_STORAGE_CHECKPOINT_CONTAINER")?;
        Ok(Self {
            root,
            account,
            container,
        })
    }
}

/// Shared body of [`initialize`]: validates the root, initializes every canonical table through
/// `bootstrap` in order, and writes the manifest only after all of them succeed. Production passes
/// [`DeltaTable::initialize_url_for_bootstrap`]; offline proofs pass the injected-control variant.
///
/// # Errors
///
/// Fails closed on an unsafe root before any table I/O, stops at the first failing table, and
/// writes no manifest unless every table succeeded.
pub async fn initialize_tables_with<F>(
    args: InitializeAzureArgs,
    root: &str,
    account: &str,
    container: &str,
    mut bootstrap: F,
) -> Result<()>
where
    F: FnMut(Url, Arc<Schema>, Vec<String>) -> Result<()>,
{
    let root = checked_root(root, account, container)?;
    let entries = entries()?;
    for (entry, schema) in &entries {
        let url = root
            .join(&entry.relative_path)
            .context("invalid canonical table path")?;
        bootstrap(url, Arc::clone(schema), vec![INGESTION.to_owned()])
            .with_context(|| format!("bootstrap canonical table {}", entry.name))?;
    }
    write_manifest(args).await
}

/// Writes canonical names/relative paths only, without contacting storage or claiming readiness.
///
/// # Errors
///
/// Returns fixture validation or manifest filesystem errors.
pub async fn write_manifest(args: InitializeAzureArgs) -> Result<()> {
    let manifest = entries()?.into_iter().map(|(entry, _)| entry).collect::<Vec<_>>();
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    tokio::fs::write(&args.manifest, bytes)
        .await
        .context("write Azure table manifest")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::TABLE_COUNT;
    use arrow_deltalake::maintenance::{Granularity, MaintenanceControl, Outcome, PurgeConfig, now_ms};

    #[test]
    fn fresh_local_partition_is_purgeable_and_existing_table_is_untouched() -> Result<()> {
        let root = tempfile::tempdir()?;
        let fixture = schema_statements()?;
        let table = "telemetry.sqlserver.sqlserver_availability_replica_states".to_owned();
        initialize_fresh_local(root.path(), std::slice::from_ref(&table), &fixture)?;
        let path = root
            .path()
            .join("telemetry/sqlserver/sqlserver_availability_replica_states");
        let log = path.join("_delta_log/00000000000000000000.json");
        let original = fs::read(&log)?;
        let actions = std::str::from_utf8(&original)?
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let metadata = actions
            .iter()
            .find_map(|action| action.get("metaData"))
            .context("missing metadata")?;
        assert_eq!(metadata["partitionColumns"], serde_json::json!([INGESTION]));
        let url = Url::from_directory_path(&path).map_err(|()| anyhow::anyhow!("invalid test directory"))?;
        let maintenance = DeltaTable::open_for_maintenance(&url)?;
        let version = maintenance.version()?;
        let config = PurgeConfig::new(86_400_000, now_ms()?, Granularity::Hour);
        let plan = maintenance.plan_purge(&config)?;
        assert!(plan.partitions.is_empty() && plan.watermark_ms.is_none());
        assert_eq!(
            maintenance
                .apply_purge(&plan, &config, &MaintenanceControl::default())?
                .outcome,
            Outcome::Noop
        );
        assert_eq!(maintenance.version()?, version);
        assert!(initialize_fresh_local(root.path(), &[table], &fixture).is_err());
        assert_eq!(fs::read(log)?, original);
        Ok(())
    }

    #[test]
    fn local_partition_requires_known_explicit_tables() -> Result<()> {
        let root = tempfile::tempdir()?;
        let fixture = schema_statements()?;
        assert!(initialize_fresh_local(root.path(), &[], &fixture).is_err());
        assert!(initialize_fresh_local(root.path(), &["unknown".to_owned()], &fixture).is_err());
        assert_eq!(fs::read_dir(root.path())?.count(), 0);
        Ok(())
    }

    #[test]
    fn actual_bootstrap_preserves_existing_logs_and_normalizes_timestamp_companions_once() -> Result<()> {
        let root = env::temp_dir().join(format!("loadgen-bootstrap-{}", uuid::Uuid::new_v4()));
        for (entry, schema) in entries()? {
            let path = root.join(entry.relative_path);
            std::fs::create_dir_all(&path)?;
            let url = Url::from_directory_path(&path).map_err(|()| anyhow::anyhow!("invalid test directory"))?;
            DeltaTable::initialize_url_for_bootstrap(url.clone(), Arc::clone(&schema), vec![INGESTION.to_owned()])?;
            let log = path.join("_delta_log/00000000000000000000.json");
            let original = std::fs::read_to_string(&log)?;
            DeltaTable::initialize_url_for_bootstrap(url, Arc::clone(&schema), vec![INGESTION.to_owned()])?;
            assert_eq!(std::fs::read_to_string(&log)?, original);
            assert!(!path.join("_delta_log/00000000000000000001.json").exists());
            let actions = original
                .lines()
                .map(serde_json::from_str::<serde_json::Value>)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let metadata = actions
                .iter()
                .find_map(|action| action.get("metaData"))
                .context("missing metadata")?;
            assert_eq!(metadata["partitionColumns"], serde_json::json!([INGESTION]));
            let stored: serde_json::Value =
                serde_json::from_str(metadata["schemaString"].as_str().context("missing schema")?)?;
            let fields = stored["fields"].as_array().context("missing fields")?;
            let timestamps = schema
                .fields()
                .iter()
                .filter(|field| matches!(field.data_type(), DataType::Timestamp(_, _)))
                .count();
            let companions = fields
                .iter()
                .filter(|field| {
                    field["name"]
                        .as_str()
                        .is_some_and(|name| name.ends_with("__unix_nanos"))
                })
                .count();
            assert_eq!(companions, timestamps);
            assert_eq!(fields.len(), schema.fields().len() + timestamps);
        }
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn dataflow_uses_raw_registry_and_required_partition() -> Result<()> {
        let entries = entries()?;
        assert_eq!(entries.len(), TABLE_COUNT);
        let names = entries
            .iter()
            .map(|(entry, _)| &entry.name)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names.len(), TABLE_COUNT);
        for (index, (entry, schema)) in entries.into_iter().enumerate() {
            assert!(entry.relative_path.starts_with("dataflow/telemetry/sqlserver/"));
            assert_eq!(schema.field(0).name(), "customer_id");
            let field = schema.field_with_name(INGESTION)?;
            assert!(!field.is_nullable());
            assert_eq!(field.data_type(), &DataType::Utf8);
            assert_eq!(schema.fields().len(), schema_statements()?[index].field_count + 1);
        }
        Ok(())
    }
    #[test]
    fn root_is_bounded_to_configured_account_and_container() -> Result<()> {
        let base = "abfss://tables@account.dfs.core.windows.net/sandbox";
        assert_eq!(checked_root(base, "account", "tables")?.path(), "/sandbox/");
        for raw in [
            "abfss://other@account.dfs.core.windows.net/sandbox",
            "abfss://tables@other.dfs.core.windows.net/sandbox",
            "abfss://tables@account.dfs.core.windows.net/a/../b",
            "abfss://tables@account.dfs.core.windows.net/%2e%2e",
            "abfss://tables@account.dfs.core.windows.net/sandbox?sig=secret",
            "file:///tmp/table",
        ] {
            assert!(checked_root(raw, "account", "tables").is_err());
        }
        Ok(())
    }
}
