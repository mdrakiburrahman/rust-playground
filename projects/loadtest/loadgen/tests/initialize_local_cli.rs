use anyhow::{Context, Result};
use serde_json::Value;
use std::process::{Command, Output};

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
