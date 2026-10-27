//! Fenced Azure table bootstrap using the same registry as the Flight fixtures.

use std::{env, sync::Arc};

use anyhow::{Context, Result, bail};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::DeltaTable;
use arrowcol_catalog::{DdlCommand, DdlParser};
use serde::Serialize;
use url::Url;

use crate::{cli::InitializeAzureArgs, schema::schema_statements};

const INGESTION: &str = "__arrowcol_ingestion";

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

fn entries() -> Result<Vec<(ManifestEntry, Arc<Schema>)>> {
    let parser = DdlParser::default();
    let mut entries = Vec::new();
    for pool in ["direct", "dataflow"] {
        for statement in schema_statements()? {
            let DdlCommand::CreateTable(create) = parser.parse(&statement.ddl)? else {
                bail!("canonical fixture contains a non-CREATE statement");
            };
            let mut fields = create
                .fields()
                .iter()
                .map(arrowcol_catalog::CatalogField::to_arrow)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            fields.push(Field::new(INGESTION, DataType::Utf8, false));
            let identity = statement.identity;
            entries.push((
                ManifestEntry {
                    name: format!("{pool}_{}", identity.table()),
                    relative_path: format!(
                        "{pool}/{}/{}/{}",
                        identity.catalog(),
                        identity.schema(),
                        identity.table()
                    ),
                },
                Arc::new(Schema::new(fields)),
            ));
        }
    }
    Ok(entries)
}

/// Initializes existing-or-empty roots and publishes a manifest only after every table succeeds.
///
/// # Errors
///
/// Fails closed for missing configuration, unsafe roots, unfenced storage, incompatible existing
/// tables, or manifest write failures. Never emits storage URLs or credentials to stdout.
pub async fn initialize(args: InitializeAzureArgs) -> Result<()> {
    let root = env::var("ARROWCOL_AZURE_TABLE_ROOT").context("missing ARROWCOL_AZURE_TABLE_ROOT")?;
    let account = env::var("AZURE_STORAGE_ACCOUNT").context("missing AZURE_STORAGE_ACCOUNT")?;
    let container = env::var("AZURE_STORAGE_CONTAINER").context("missing AZURE_STORAGE_CONTAINER")?;
    let _checkpoint =
        env::var("AZURE_STORAGE_CHECKPOINT_CONTAINER").context("missing AZURE_STORAGE_CHECKPOINT_CONTAINER")?;
    let root = checked_root(&root, &account, &container)?;
    let entries = entries()?;
    for (entry, schema) in &entries {
        let url = root
            .join(&entry.relative_path)
            .context("invalid canonical table path")?;
        DeltaTable::initialize_url_for_bootstrap(url, Arc::clone(schema), vec![INGESTION.to_owned()])
            .with_context(|| format!("bootstrap canonical table {}", entry.name))?;
    }
    let manifest = entries.into_iter().map(|(entry, _)| entry).collect::<Vec<_>>();
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    tokio::fs::write(&args.manifest, bytes)
        .await
        .context("write Azure table manifest")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn both_pools_use_raw_registry_and_required_partition() -> Result<()> {
        let entries = entries()?;
        assert_eq!(entries.len(), 26);
        let names = entries
            .iter()
            .map(|(entry, _)| &entry.name)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names.len(), 26);
        for (index, (entry, schema)) in entries.into_iter().enumerate() {
            assert!(
                entry.relative_path.starts_with("direct/telemetry/sqlserver/")
                    || entry.relative_path.starts_with("dataflow/telemetry/sqlserver/")
            );
            assert_eq!(schema.field(0).name(), "customer_id");
            let field = schema.field_with_name(INGESTION)?;
            assert!(!field.is_nullable());
            assert_eq!(field.data_type(), &DataType::Utf8);
            assert_eq!(schema.fields().len(), schema_statements()?[index % 13].field_count + 1);
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
