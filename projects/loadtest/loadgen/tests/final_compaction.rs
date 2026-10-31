//! Actual runner argv/environment dispatch, without mocks or live infrastructure.

use anyhow::Result;
use assert_cmd::Command;
use serde_json::{Value, json};
use std::fs;
use std::path::Path;

fn command(results: &Path) -> Result<Command> {
    let mut command = Command::cargo_bin("arrowcol-loadgen")?;
    command
        .arg("final-compaction")
        .env("LOADTEST_RESULTS", results)
        .env("LOADTEST_DIRECT_ROOT", results.join("direct-must-not-open"))
        .env("LOADTEST_DATAFLOW_ROOT", results.join("dataflow-must-not-open"));
    Ok(command)
}

fn metadata(results: &Path) -> Result<()> {
    fs::write(results.join("run-metadata.json"), br#"{"run_id":"dispatch-run"}"#)?;
    Ok(())
}

#[test]
fn enabled_binary_dispatch_rejects_missing_or_failed_release_and_records_failure() -> Result<()> {
    for missing in [true, false] {
        let results = tempfile::tempdir()?;
        metadata(results.path())?;
        if !missing {
            fs::write(
                results.path().join("publisher-release.json"),
                serde_json::to_vec(&json!({"run_id":"dispatch-run","passed":false}))?,
            )?;
        }
        let output = command(results.path())?.output()?;
        assert!(!output.status.success());
        let evidence: Value = serde_json::from_slice(&fs::read(results.path().join("final-compaction.json"))?)?;
        assert_eq!(evidence["run_id"], "dispatch-run");
        assert_eq!(evidence["passed"], false);
        assert_eq!(evidence["skipped"], false);
        assert!(
            evidence["error"]
                .as_str()
                .is_some_and(|error| error.contains("publisher"))
        );
        assert!(!results.path().join("direct-must-not-open").exists());
        assert!(!results.path().join("dataflow-must-not-open").exists());
    }
    Ok(())
}

#[test]
fn invalid_metadata_binary_dispatch_always_records_failure_with_null_run_id() -> Result<()> {
    for metadata in [
        None,
        Some(b"{".as_slice()),
        Some(br#"{"run_id":null}"#.as_slice()),
        Some(br#"{}"#.as_slice()),
        Some(br#"{"run_id":""}"#.as_slice()),
    ] {
        let results = tempfile::tempdir()?;
        if let Some(bytes) = metadata {
            fs::write(results.path().join("run-metadata.json"), bytes)?;
        }
        let output = command(results.path())?.output()?;
        assert!(!output.status.success());
        let evidence: Value = serde_json::from_slice(&fs::read(results.path().join("final-compaction.json"))?)?;
        assert_eq!(evidence["run_id"], Value::Null);
        assert_eq!(evidence["passed"], false);
        assert_eq!(evidence["skipped"], false);
        assert!(
            evidence["error"]
                .as_str()
                .is_some_and(|error| error.contains("run_id unavailable"))
        );
        assert!(!results.path().join("direct-must-not-open").exists());
        assert!(!results.path().join("dataflow-must-not-open").exists());
    }
    Ok(())
}

#[test]
fn opted_out_binary_dispatch_records_skip_without_a_pass_claim() -> Result<()> {
    let results = tempfile::tempdir()?;
    metadata(results.path())?;
    let output = command(results.path())?.arg("--skip-final-compaction").output()?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let evidence: Value = serde_json::from_slice(&fs::read(results.path().join("final-compaction.json"))?)?;
    assert_eq!(evidence["run_id"], "dispatch-run");
    assert_eq!(evidence["passed"], false);
    assert_eq!(evidence["skipped"], true);
    assert_eq!(evidence["error"], Value::Null);
    assert!(!results.path().join("direct-must-not-open").exists());
    assert!(!results.path().join("dataflow-must-not-open").exists());
    Ok(())
}
