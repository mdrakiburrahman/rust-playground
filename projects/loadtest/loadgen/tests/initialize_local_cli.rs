use anyhow::{Context, Result};
use arrow_deltalake::{delta::DeltaTable, normalize::CanonicalSchema};
use arrowcol_catalog::CatalogStore;
use arrowcol_catalog_rust::{CatalogConfig, CatalogService};
use serde_json::Value;
use std::process::{Command, Output};
use std::sync::Arc;

const TABLE: &str = "telemetry.sqlserver.sqlserver_availability_replica_states";

fn run_initializer(root: &std::path::Path, partition: bool) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_arrowcol-loadgen"));
    command.env_clear().arg("initialize-local");
    if partition {
        command.arg("--partition-by-ingestion-date");
    }
    let output = command
        .args(["--table", TABLE, "--dataflow-root"])
        .arg(root)
        .output()
        .context("run setup-only local initializer")?;
    Ok(output)
}

#[test]
fn setup_only_cli_creates_one_fresh_partitioned_table_and_refuses_reuse() -> Result<()> {
    let root = tempfile::tempdir()?;
    let first = run_initializer(root.path(), true)?;
    anyhow::ensure!(
        first.status.success(),
        "setup-only initializer failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );

    let table = root
        .path()
        .join("telemetry/sqlserver/sqlserver_availability_replica_states");
    let log = table.join("_delta_log/00000000000000000000.json");
    let original = std::fs::read(&log)?;
    let actions = std::str::from_utf8(&original)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let metadata = actions
        .iter()
        .find_map(|action| action.get("metaData"))
        .context("new table has no Delta metadata")?;
    assert_eq!(
        metadata["partitionColumns"],
        serde_json::json!(["__arrowcol_ingestion"])
    );
    assert_eq!(
        std::fs::read_dir(root.path().join("telemetry/sqlserver"))?.count(),
        1,
        "setup-only command must initialize only the selected table"
    );

    let repeated = run_initializer(root.path(), true)?;
    assert!(!repeated.status.success(), "existing table was accepted");
    assert!(String::from_utf8_lossy(&repeated.stderr).contains("refusing to initialize existing local table"));
    assert_eq!(
        std::fs::read(log)?,
        original,
        "refusal must preserve existing Delta metadata"
    );
    Ok(())
}

#[test]
fn setup_only_cli_requires_explicit_partition_opt_in() -> Result<()> {
    let root = tempfile::tempdir()?;
    let output = run_initializer(root.path(), false)?;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --partition-by-ingestion-date"));
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn initialized_timestamp_table_survives_catalog_startup_and_logical_registration() -> Result<()> {
    let root = tempfile::tempdir()?;
    let warehouse = root.path().join("dataflow");
    std::fs::create_dir(&warehouse)?;
    let initialized = run_initializer(&warehouse, true)?;
    anyhow::ensure!(
        initialized.status.success(),
        "initializer failed: {}",
        String::from_utf8_lossy(&initialized.stderr)
    );
    let statement = arrowcol_loadgen::schema::schema_statements()?
        .into_iter()
        .find(|statement| statement.identity.to_string() == TABLE)
        .context("missing hot-table fixture")?;
    let location = warehouse.join("telemetry/sqlserver/sqlserver_availability_replica_states");
    let url = url::Url::from_directory_path(&location).map_err(|()| anyhow::anyhow!("invalid table directory"))?;
    let table = DeltaTable::open_catalog(url)?;
    let original = table.catalog_snapshot()?.context("initialized Delta snapshot")?;
    CanonicalSchema::try_new(Arc::clone(&original.schema))?;
    assert!(original.schema.field_with_name("sample_time_utc__unix_nanos").is_ok());
    assert_eq!(original.partition_columns, ["__arrowcol_ingestion"]);

    let mut config = CatalogConfig {
        warehouse_root: warehouse.to_string_lossy().into_owned(),
        state_dir: root.path().join("catalog"),
        ddl_dir: None,
        retained_events: 4,
        ingestion_layout: false,
    };
    let failure = CatalogService::open(config.clone())
        .await
        .err()
        .context("generic catalog import must reject physical timestamp companions")?;
    assert!(
        format!("{failure:#}").contains("reserved timestamp companion name sample_time_utc__unix_nanos"),
        "{failure:#}"
    );

    config.ingestion_layout = true;
    let service = CatalogService::open(config.clone()).await?;
    assert!(service.store().current_schema(&statement.identity).await?.is_none());
    assert_eq!(
        table.version()?,
        original.version,
        "discovery must not mutate the bootstrap"
    );

    let seeds = root.path().join("seeds");
    std::fs::create_dir(&seeds)?;
    std::fs::write(seeds.join("hot-table.sql"), &statement.ddl)?;
    let ordinary = arrowcol_loadgen::schema::schema_statements()?
        .into_iter()
        .find(|statement| statement.identity.table().as_str() == "sqlserver_active_sessions")
        .context("missing ordinary unpartitioned fixture")?;
    let ordinary_location = warehouse.join("telemetry/sqlserver/sqlserver_active_sessions");
    assert!(!ordinary_location.exists());
    std::fs::write(seeds.join("ordinary.sql"), &ordinary.ddl)?;
    config.ddl_dir = Some(seeds);
    let service = CatalogService::open(config.clone()).await?;
    let logical = service
        .store()
        .current_schema(&statement.identity)
        .await?
        .context("registered logical schema")?;
    assert_eq!(logical.fields().len(), statement.field_count);
    assert!(
        logical
            .fields()
            .iter()
            .all(|field| !field.name().ends_with("__unix_nanos") && field.name() != "__arrowcol_ingestion")
    );
    let adopted = table.catalog_snapshot()?.context("adopted Delta snapshot")?;
    assert_eq!(
        adopted.schema, original.schema,
        "preserve precision companions and field metadata"
    );
    assert_eq!(adopted.partition_columns, original.partition_columns);
    let ordinary_logical = service
        .store()
        .current_schema(&ordinary.identity)
        .await?
        .context("ordinary CREATE schema")?;
    assert_eq!(ordinary_logical.fields().len(), ordinary.field_count);
    assert!(ordinary_logical.metadata().partition_columns.is_empty());
    let ordinary_url = url::Url::from_directory_path(&ordinary_location)
        .map_err(|()| anyhow::anyhow!("invalid ordinary table directory"))?;
    let ordinary_snapshot = DeltaTable::open_catalog(ordinary_url)?
        .catalog_snapshot()?
        .context("ordinary CREATE physical schema")?;
    CanonicalSchema::try_new(Arc::clone(&ordinary_snapshot.schema))?;
    assert_eq!(ordinary_snapshot.partition_columns, ["__arrowcol_ingestion"]);
    let restarted = CatalogService::open(config).await?;
    assert_eq!(
        restarted
            .store()
            .current_schema(&statement.identity)
            .await?
            .context("restarted logical schema")?,
        logical
    );
    assert_eq!(
        table.version()?,
        adopted.version,
        "restart must not rewrite adopted metadata"
    );
    Ok(())
}
