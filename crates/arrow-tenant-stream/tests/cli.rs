use anyhow::{Context, Result};
use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::DeltaTable;
use arrow_tenant_stream::checkpoint::Checkpoint;
use arrow_tenant_stream::source::DeltaSource;
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use url::Url;

fn url(path: &Path) -> Result<Url> {
    Url::from_directory_path(path).map_err(|()| anyhow::anyhow!("invalid test URL"))
}

fn command(source: &Path, target: &Path, checkpoint: &Path, once: bool) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_arrow-tenant-stream"));
    command
        .arg("--source")
        .arg(source)
        .arg("--target")
        .arg(target)
        .arg("--checkpoint")
        .arg(checkpoint)
        .args(["--customer-id", "a", "--metrics-listen", "127.0.0.1:0"]);
    if once {
        command.arg("--once");
    }
    command
}

#[cfg(unix)]
fn graceful_shutdown(source: &Path, target: &Path, checkpoint: &Path) -> Result<()> {
    use std::io::{Read, Write};
    use std::process::{Child, Stdio};
    use std::time::{Duration, Instant};
    struct Process(Child);
    impl Drop for Process {
        fn drop(&mut self) {
            if matches!(self.0.try_wait(), Ok(None))
                && let Err(error) = self.0.kill()
            {
                eprintln!("test process cleanup failed: {error}");
            }
            if let Err(error) = self.0.wait() {
                eprintln!("test process wait failed: {error}");
            }
        }
    }
    let log = tempfile::NamedTempFile::new()?;
    let mut child = Process(
        command(source, target, checkpoint, false)
            .stdout(Stdio::null())
            .stderr(log.as_file().try_clone()?)
            .spawn()?,
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let endpoint = loop {
        let text = std::fs::read_to_string(log.path())?;
        if let Some(endpoint) = text
            .lines()
            .find_map(|line| line.strip_prefix("tenant stream metrics listening on "))
        {
            break endpoint.to_owned();
        }
        assert!(child.0.try_wait()?.is_none(), "{text}");
        assert!(Instant::now() < deadline, "CLI readiness timed out: {text}");
        std::thread::sleep(Duration::from_millis(10));
    };
    loop {
        let mut connection = std::net::TcpStream::connect(&endpoint)?;
        connection.set_read_timeout(Some(Duration::from_secs(2)))?;
        connection.set_write_timeout(Some(Duration::from_secs(2)))?;
        connection.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
        let mut response = String::new();
        connection.read_to_string(&mut response)?;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        if response
            .lines()
            .find_map(|line| line.strip_prefix("tenant_stream_noop_polls_total "))
            .context("missing idle counter")?
            .parse::<u64>()?
            > 0
        {
            break;
        }
        assert!(Instant::now() < deadline, "CLI did not reach idle");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.0.id().to_string()])
            .status()?
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.0.try_wait()? {
            assert!(status.success(), "SIGTERM was not handled gracefully: {status}");
            break;
        }
        assert!(Instant::now() < deadline, "SIGTERM shutdown timed out");
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

#[tokio::test]
async fn standalone_cli_drains_resumes_and_rejects_a_concurrent_checkpoint_owner() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    let checkpoint = tempfile::tempdir()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
        Field::new("ingestion_minute", DataType::Utf8, false),
    ]));
    let data = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "a"])),
            Arc::new(Int32Array::from(vec![2, 1, 0])),
            Arc::new(StringArray::from(vec!["minute"; 3])),
        ],
    )?;
    let source = DeltaTable::open_url(
        url(source_root.path())?,
        Arc::clone(&schema),
        vec!["ingestion_minute".into()],
    )?;
    let mut file = source.new_file_partitioned(
        schema,
        BTreeMap::from([("ingestion_minute".into(), Some("minute".into()))]),
    )?;
    file.write(&data).await?;
    source.commit_files_with_transaction(&[file.stage().await?], "source-publication", 0)?;
    let first = command(source_root.path(), target_root.path(), checkpoint.path(), true).output()?;
    assert!(first.status.success(), "{}", String::from_utf8_lossy(&first.stderr));
    assert!(String::from_utf8_lossy(&first.stdout).contains("batch=0"));
    let manifest = DeltaSource::open(url(target_root.path())?)?.capture()?;
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.files.len(), 1);
    assert_eq!(manifest.partition_columns()?, vec!["ingestion_minute"]);
    let metadata = std::fs::read(checkpoint.path().join("metadata"))?;
    let second = command(source_root.path(), target_root.path(), checkpoint.path(), true).output()?;
    assert!(second.status.success(), "{}", String::from_utf8_lossy(&second.stderr));
    assert!(!String::from_utf8_lossy(&second.stdout).contains("batch="));
    assert_eq!(std::fs::read(checkpoint.path().join("metadata"))?, metadata);
    assert_eq!(DeltaSource::open(url(target_root.path())?)?.capture()?.version, 1);
    assert_eq!(
        std::fs::read_to_string(checkpoint.path().join("commits/0"))?,
        "v1\n{\"nextBatchWatermarkMs\":0}"
    );
    assert!(!checkpoint.path().join("offsets/1").exists());
    let guard = Checkpoint::open_local(checkpoint.path()).await?;
    let blocked = command(source_root.path(), target_root.path(), checkpoint.path(), true).output()?;
    assert!(!blocked.status.success());
    let error = String::from_utf8(blocked.stderr).context("CLI stderr is UTF8")?;
    assert!(error.contains("lock") || error.contains("owner"), "{error}");
    guard.close().await?;
    #[cfg(unix)]
    graceful_shutdown(source_root.path(), target_root.path(), checkpoint.path())?;
    let reopened = Checkpoint::open_local(checkpoint.path()).await?;
    reopened.close().await?;
    let unsafe_checkpoint = target_root.path().join("checkpoint");
    let rejected = command(source_root.path(), target_root.path(), &unsafe_checkpoint, true).output()?;
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("vacuumable target root"));
    assert_eq!(DeltaSource::open(url(target_root.path())?)?.capture()?.version, 1);
    Ok(())
}
