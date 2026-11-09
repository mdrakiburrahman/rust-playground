#![cfg(unix)]

use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use arrowcol_catalog_client::rpc::{self, schema_catalog_client::SchemaCatalogClient};

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

async fn signal_shutdown(signal: &str) -> Result<()> {
    let root = tempfile::tempdir()?;
    let seeds = root.path().join("seeds");
    std::fs::create_dir(&seeds)?;
    std::fs::write(
        seeds.join("table.sql"),
        "CREATE TABLE arrowcol.default.shutdown_probe (id BIGINT) USING DELTA",
    )?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_arrowcol-catalog-rust"))
            .args(["--listen", &address.to_string(), "--warehouse-root"])
            .arg(root.path().join("warehouse"))
            .arg("--state-dir")
            .arg(root.path().join("state"))
            .arg("--ddl-dir")
            .arg(seeds)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    );
    let endpoint = format!("http://{address}");
    let mut client = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(status) = process.0.try_wait()? {
                bail!("catalog exited before readiness: {status}");
            }
            if let Ok(mut client) = SchemaCatalogClient::connect(endpoint.clone()).await
                && client.get_current_event_id(rpc::Empty {}).await.is_ok()
            {
                return Ok::<_, anyhow::Error>(client);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;
    let position = client.get_current_event_id(rpc::Empty {}).await?.into_inner();
    let mut watch = client
        .watch(rpc::EventsRequest { from: Some(position) })
        .await?
        .into_inner();
    let signaled = Command::new("kill")
        .arg(signal)
        .arg(process.0.id().to_string())
        .status()?;
    assert!(signaled.success());
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = process.0.try_wait()? {
                return Ok::<_, std::io::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("catalog failed to drain an active CES watch")??;
    assert!(status.success(), "catalog did not cleanly drain: {status}");
    assert!(watch.message().await?.is_none(), "watch must end at shutdown");
    Ok(())
}

#[tokio::test]
async fn sigterm_drains_seeded_catalog_with_live_ces_watch() -> Result<()> {
    signal_shutdown("-TERM").await
}

#[tokio::test]
async fn sigint_drains_seeded_catalog_with_live_ces_watch() -> Result<()> {
    signal_shutdown("-INT").await
}
