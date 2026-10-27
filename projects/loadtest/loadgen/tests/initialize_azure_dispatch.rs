#![cfg(unix)]

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::process::Command;

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Result<Self> {
        let root = std::env::temp_dir().join(format!("initialize-azure-dispatch-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root)?;
        Ok(Self(root))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!("initializer dispatch fixture cleanup failed: {error}");
        }
    }
}

#[test]
fn dispatched_initializer_rejects_missing_or_noncanonical_expected_generation_before_io() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    for generation in [
        None,
        Some(""),
        Some("not-a-generation"),
        Some("11111111222243338444555555555555"),
        Some("AAAAAAAA-2222-4333-8444-555555555555"),
        Some(" 11111111-2222-4333-8444-555555555555"),
    ] {
        let fixture = Fixture::new()?;
        let az = fixture.0.join("az");
        let marker = fixture.0.join("unexpected-auth-or-control");
        let manifest = fixture.0.join("manifest.json");
        std::fs::write(
            &az,
            "#!/bin/sh\nprintf '%s\\n' attempted > \"$INITIALIZER_IO_MARKER\"\nexit 97\n",
        )?;
        std::fs::set_permissions(&az, std::fs::Permissions::from_mode(0o700))?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_arrowcol-loadgen"));
        command
            .env_clear()
            .env("PATH", &fixture.0)
            .env("HOME", &fixture.0)
            .env("AZURE_CONFIG_DIR", &fixture.0)
            .env("INITIALIZER_IO_MARKER", &marker)
            .env("AZURE_STORAGE_ACCOUNT", "offlineproof")
            .env("AZURE_STORAGE_CONTAINER", "tables")
            .env("AZURE_STORAGE_CHECKPOINT_CONTAINER", "checkpoints")
            .env("AZURE_SUBSCRIPTION_ID", "11111111-2222-4333-8444-555555555555")
            .env("ARROWCOL_RESET_GENERATION", "11111111-2222-4333-8444-555555555555")
            .env(
                "ARROWCOL_AZURE_TABLE_ROOT",
                "abfss://tables@offlineproof.dfs.core.windows.net/sandbox/",
            )
            // A regression must not contact Azure even if preflight stops failing.
            .env("HTTPS_PROXY", "http://127.0.0.1:9")
            .env("HTTP_PROXY", "http://127.0.0.1:9")
            .env("ALL_PROXY", "http://127.0.0.1:9")
            .env("NO_PROXY", "")
            .args(["initialize-azure", "--manifest"])
            .arg(&manifest);
        if let Some(generation) = generation {
            command.env("ARROWCOL_EXPECTED_RESET_GENERATION", generation);
        }
        let output = command.output().context("dispatch actual initialize-azure binary")?;
        let stderr = String::from_utf8(output.stderr)?;
        assert!(!output.status.success(), "invalid generation was admitted");
        assert!(stderr.contains("ARROWCOL_EXPECTED_RESET_GENERATION"), "{stderr}");
        assert!(!marker.exists(), "preflight reached credential/control I/O");
        assert!(!manifest.exists(), "failed initializer published its success manifest");
    }
    Ok(())
}
