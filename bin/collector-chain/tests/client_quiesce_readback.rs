use anyhow::{Context, Result, ensure};
use serde_json::Value;
use std::process::Command;

#[test]
fn running_client_emits_resolved_quiesce_timeout_for_default_and_explicit_values() -> Result<()> {
    for (configured, expected) in [(None, 1_234_u64), (Some(2_345_u64), 2_345)] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_arrowcol-dmv"));
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("ARROWCOL_CLIENT_") {
                command.env_remove(name);
            }
        }
        command.args([
            "--endpoint",
            "http://127.0.0.1:0",
            "--workers",
            "1",
            "--runtime-threads",
            "1",
            "--rows",
            "8",
            "--concurrency",
            "1",
            "--table",
            "sqlserver_availability_replica_states",
            "--request-timeout-ms",
            "1234",
            "--connect-timeout-ms",
            "100",
            "--connect-retries",
            "0",
        ]);
        if let Some(value) = configured {
            command.args(["--quiesce-timeout-ms", &value.to_string()]);
        }
        let output = command.output().context("run client startup")?;
        ensure!(
            !output.status.success(),
            "fixture endpoint must not accept client connections"
        );
        let stdout = std::str::from_utf8(&output.stdout).context("client stdout is not UTF-8")?;
        let mut readbacks = stdout
            .lines()
            .filter_map(|line| line.strip_prefix("OAT_CLIENT_EFFECTIVE_CONFIG "));
        let effective: Value =
            serde_json::from_str(readbacks.next().context("client omitted effective configuration")?)?;
        assert!(
            readbacks.next().is_none(),
            "client emitted duplicate effective configuration"
        );
        assert_eq!(effective["request_timeout_ms"], 1_234);
        assert_eq!(effective["quiesce_timeout_ms"], expected);
    }
    Ok(())
}
