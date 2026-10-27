use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

fn run(generation: Option<&str>, root: Option<&str>, validate: bool) -> TestResult<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_arrowcol-dataflow-rust"));
    command
        .arg("--config")
        .arg(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.smoke-quiver.yaml"))
        .env_remove("ARROWCOL_RESET_GENERATION")
        .env_remove("ARROWCOL_DELTA_ROOT")
        .env_remove("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9")
        .env("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
        .env("OTEL_METRIC_EXPORT_INTERVAL", "60000")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(generation) = generation {
        command.env("ARROWCOL_RESET_GENERATION", generation);
    }
    if let Some(root) = root {
        command.env("ARROWCOL_DELTA_ROOT", root);
    }
    if validate {
        command.arg("--validate-and-exit");
    }
    let mut child = command.spawn()?;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if child.try_wait()?.is_some() {
            return Ok(child.wait_with_output()?);
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            return Err("DFE entrypoint failed to shut down within 15 seconds".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Scenario: A local durable configuration is validated with a one-minute export interval.
/// Guarantees: Real global instrumentation boots and explicit shutdown exits without waiting for the interval.
#[test]
fn validation_exits_through_metrics_shutdown_hook() -> TestResult<()> {
    let output = run(None, None, true)?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    Ok(())
}

/// Scenario: Cloud startup omits its epoch or supplies a malformed epoch UUID.
/// Guarantees: Startup refuses before authentication, SQL, WAL admission, or Delta publication.
#[test]
fn cloud_entrypoint_refuses_missing_and_malformed_generation() -> TestResult<()> {
    for generation in [None, Some("not-a-generation-uuid")] {
        let output = run(
            generation,
            Some("abfss://container@account.dfs.core.windows.net/root"),
            false,
        )?;
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("ARROWCOL_RESET_GENERATION"));
    }
    Ok(())
}
