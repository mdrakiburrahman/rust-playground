use std::{fs, process::Command};

use anyhow::Result;
use serde_json::{Value, json};

#[test]
fn dispatched_release_rejects_missing_and_stale_evidence_before_sql() -> Result<()> {
    for stale in [false, true] {
        let directory = std::env::temp_dir().join(format!("publisher-release-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory)?;
        let outcome = (|| -> Result<()> {
            if stale {
                for (name, value) in [
                    ("run-metadata.json", json!({"run_id":"this-run"})),
                    (
                        "publication-drain.json",
                        json!({"run_id":"this-run","dataflow_settled":true}),
                    ),
                    (
                        "mutator-drain.json",
                        json!({"run_id":"this-run","clean":true,"services":["direct-1","dataflow-1"]}),
                    ),
                    (
                        "routing.json",
                        json!({"run_id":"old-run","loaded_at_unix_ms":1,"collectors":[],"leases":[]}),
                    ),
                ] {
                    fs::write(directory.join(name), serde_json::to_vec(&value)?)?;
                }
            }
            let output = Command::new(env!("CARGO_BIN_EXE_arrowcol-loadgen"))
                .env_clear()
                .args(["publisher-release", "--results"])
                .arg(&directory)
                .args(["--sql-host", "127.0.0.1", "--sql-port", "9", "--sql-password-file"])
                .arg(directory.join("must-not-read-password"))
                .output()?;
            assert!(!output.status.success());
            let proof: Value = serde_json::from_slice(&fs::read(directory.join("publisher-release.json"))?)?;
            assert_eq!(proof["passed"], false);
            assert!(proof["routing"].is_null());
            assert!(proof["receipts"].is_null());
            assert_eq!(proof["run_id"], if stale { json!("this-run") } else { Value::Null });
            let errors = proof["errors"].to_string();
            assert!(
                !errors.contains("password"),
                "preflight must not reach SQL password read: {errors}"
            );
            if stale {
                assert!(errors.contains("does not belong to this run"));
            } else {
                assert!(errors.contains("run-metadata.json"));
            }
            Ok(())
        })();
        let cleanup = fs::remove_dir_all(&directory);
        outcome?;
        cleanup?;
    }
    Ok(())
}
