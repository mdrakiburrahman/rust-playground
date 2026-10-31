use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use arrow_deltalake::delta::DeltaTable;
use arrow_deltalake::maintenance::{Granularity, MaintenanceControl, OptimizeConfig, PurgeConfig};
use serde_json::{Value, json};
use url::Url;

struct Fixture {
    root: PathBuf,
    anchor: i64,
}

struct Producer(Option<Child>);

impl Producer {
    fn child(&mut self) -> Result<&mut Child> {
        self.0.as_mut().context("producer already joined")
    }

    fn join(mut self) -> Result<Output> {
        Ok(self.0.take().context("producer already joined")?.wait_with_output()?)
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Fixture {
    fn new() -> Result<Self> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::current_dir()?.join(format!(".data/experiment-tests/{}-{nanos}", std::process::id()));
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            anchor: i64::try_from(nanos / 1_000_000)? / 60_000 * 60_000,
        })
    }

    fn command(&self) -> Command {
        let mut command = Command::new(assert_cmd::cargo::cargo_bin!("arrowcol-experiment"));
        command
            .env("ARROWCOL_PARTITION_GRANULARITY", "minute")
            .args(["--root"])
            .arg(&self.root)
            .args(["--table", "experiment_test", "--allow-table", "experiment_test"]);
        command
    }

    fn seed(&self) -> Result<Value> {
        let output = self
            .command()
            .args([
                "seed",
                "--anchor-ms",
                &self.anchor.to_string(),
                "--rows-per-file",
                "2",
                "--files-per-partition",
                "2",
            ])
            .output()?;
        success(output)
    }

    fn inspect(&self) -> Result<Value> {
        success(self.command().arg("inspect").output()?)
    }

    fn table_url(&self) -> Result<Url> {
        Url::from_directory_path(self.root.join("experiment_test")).map_err(|()| anyhow::anyhow!("invalid test URL"))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn success(output: Output) -> Result<Value> {
    ensure!(
        output.status.success(),
        "CLI failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let text = std::str::from_utf8(&output.stdout)?;
    ensure!(text.lines().count() == 1, "expected exactly one JSON line");
    let value: Value = serde_json::from_str(text)?;
    ensure!(value["ok"] == true, "unsuccessful JSON response");
    Ok(value)
}

#[test]
fn production_seed_append_and_exact_duplicate_multiset() -> Result<()> {
    let fixture = Fixture::new()?;
    let seed = fixture.seed()?;
    assert_eq!(seed["rows"], 12);
    assert_eq!(seed["nextId"], 12);
    assert_eq!(seed["publisherClosed"], true);
    let initial = fixture.inspect()?;
    assert_eq!(initial["files"].as_array().context("files")?.len(), 6);
    assert_eq!(initial["records"], seed["records"]);
    assert_eq!(initial["fingerprint"], seed["fingerprint"]);

    let first = success(
        fixture
            .command()
            .args([
                "append",
                "--start-id",
                "100",
                "--batches",
                "2",
                "--rows-per-batch",
                "2",
                "--partition-ms",
                &fixture.anchor.to_string(),
            ])
            .output()?,
    )?;
    let second = success(
        fixture
            .command()
            .args([
                "append",
                "--start-id",
                "100",
                "--batches",
                "2",
                "--rows-per-batch",
                "2",
                "--partition-ms",
                &fixture.anchor.to_string(),
            ])
            .output()?,
    )?;
    assert_eq!(first["records"], second["records"]);
    let current = fixture.inspect()?;
    assert_eq!(current["rows"], 20);
    assert_eq!(current["files"].as_array().context("files")?.len(), 10);
    let records = current["records"].as_array().context("records")?;
    let duplicates = records.iter().filter(|row| row[0] == 100).count();
    assert_eq!(
        duplicates, 2,
        "independent receipt IDs must preserve duplicate payloads"
    );
    assert!(!serde_json::to_string(&current)?.contains(fixture.root.to_str().context("root")?));
    Ok(())
}

#[test]
fn concurrent_real_append_optimize_and_purge_preserve_exact_survivors_and_control() -> Result<()> {
    let fixture = Fixture::new()?;
    let seed = fixture.seed()?;
    let before = fixture.inspect()?;
    let control = seed["partitions"][2]["partition"].as_str().context("control")?;
    let expired = seed["partitions"][0]["partition"].as_str().context("expired")?;
    let control_files: Vec<_> = before["files"]
        .as_array()
        .context("files")?
        .iter()
        .filter(|file| file["partition"] == control)
        .cloned()
        .collect();
    let mut producer = Producer(Some(
        fixture
            .command()
            .args([
                "append",
                "--start-id",
                "1000",
                "--batches",
                "30",
                "--rows-per-batch",
                "2",
                "--interval-ms",
                "200",
                "--partition-ms",
                &fixture.anchor.to_string(),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    ));
    let mut started = false;
    for _ in 0..100 {
        if producer.child()?.try_wait()?.is_some() {
            break;
        }
        if fixture.inspect()?["rows"].as_u64().unwrap_or_default() > 12 {
            started = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    if !started {
        let output = producer.join()?;
        anyhow::bail!("producer was not active: {}", String::from_utf8_lossy(&output.stdout));
    }
    let maintenance = DeltaTable::open_for_maintenance(&fixture.table_url()?)?;
    let config = OptimizeConfig {
        granularity: Some(Granularity::Minute),
        now_ms: Some(fixture.anchor),
        target_file_bytes: 1024 * 1024,
        small_file_bytes: 1024 * 1024,
        latest_partitions: Some(2),
        ..OptimizeConfig::default()
    };
    let cancellation = MaintenanceControl::new(Arc::new(AtomicBool::new(false)), None);
    let plan = maintenance.plan_optimize(&config)?;
    maintenance.apply_optimize(&plan, &config, &cancellation)?;
    ensure!(
        producer.child()?.try_wait()?.is_none(),
        "append did not overlap maintenance"
    );
    let optimized = fixture.inspect()?;
    let expired_before: Vec<_> = before["files"]
        .as_array()
        .context("files")?
        .iter()
        .filter(|file| file["partition"] == expired)
        .cloned()
        .collect();
    let expired_after: Vec<_> = optimized["files"]
        .as_array()
        .context("files")?
        .iter()
        .filter(|file| file["partition"] == expired)
        .cloned()
        .collect();
    assert_eq!(
        expired_before, expired_after,
        "partial OPTIMIZE must leave expired files untouched"
    );
    let compact = seed["partitions"][1]["partition"].as_str().context("compact")?;
    assert_eq!(
        optimized["files"]
            .as_array()
            .context("files")?
            .iter()
            .filter(|file| file["partition"] == compact)
            .count(),
        1
    );
    let purge_config = PurgeConfig::new(86_400_000, fixture.anchor, Granularity::Minute);
    let purge = maintenance.plan_purge(&purge_config)?;
    maintenance.apply_purge(&purge, &purge_config, &cancellation)?;
    let appended = success(producer.join()?)?;
    let after = fixture.inspect()?;
    let mut expected = seed["records"]
        .as_array()
        .context("seed records")?
        .iter()
        .filter(|row| row[2] != expired)
        .cloned()
        .collect::<Vec<_>>();
    expected.extend(
        appended["records"]
            .as_array()
            .context("append records")?
            .iter()
            .cloned(),
    );
    expected.sort_by_key(|row| row[0].as_i64());
    assert_eq!(after["records"], json!(expected));
    let after_control: Vec<_> = after["files"]
        .as_array()
        .context("files")?
        .iter()
        .filter(|file| file["partition"] == control)
        .cloned()
        .collect();
    assert_eq!(control_files, after_control);
    assert!(
        after["files"]
            .as_array()
            .context("files")?
            .iter()
            .all(|file| file["partition"] != expired)
    );
    Ok(())
}

#[test]
fn rejects_unowned_unsafe_missing_and_reseeded_targets_without_echoing_urls() -> Result<()> {
    let fixture = Fixture::new()?;
    for args in [
        vec![
            "--root",
            "abfss://secret@private.invalid/root?sig=secret",
            "--table",
            "experiment_a",
            "--allow-table",
            "experiment_a",
            "inspect",
        ],
        vec![
            "--root",
            "/absolute",
            "--table",
            "experiment_a",
            "--allow-table",
            "experiment_b",
            "inspect",
        ],
        vec![
            "--root",
            "/absolute",
            "--table",
            "../bad",
            "--allow-table",
            "../bad",
            "inspect",
        ],
    ] {
        let output = Command::new(assert_cmd::cargo::cargo_bin!("arrowcol-experiment"))
            .env("ARROWCOL_PARTITION_GRANULARITY", "minute")
            .args(args)
            .output()?;
        assert!(!output.status.success());
        let body = String::from_utf8(output.stdout)?;
        assert!(!body.contains("secret"));
        assert!(!body.contains("private.invalid"));
        assert_eq!(serde_json::from_str::<Value>(&body)?["publisherClosed"], false);
    }
    assert!(
        !fixture
            .command()
            .args(["append", "--start-id", "0"])
            .output()?
            .status
            .success()
    );
    let cloud = "abfss://experiment@not-a-real-account.dfs.core.windows.net/owned";
    let output = Command::new(assert_cmd::cargo::cargo_bin!("arrowcol-experiment"))
        .env("ARROWCOL_PARTITION_GRANULARITY", "minute")
        .env("ARROWCOL_DELTA_ROOT", cloud)
        .env_remove("ARROWCOL_RESET_GENERATION")
        .args([
            "--root",
            cloud,
            "--table",
            "experiment_test",
            "--allow-table",
            "experiment_test",
            "seed",
            "--anchor-ms",
            &fixture.anchor.to_string(),
        ])
        .output()?;
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("not-a-real-account"));
    fixture.seed()?;
    assert!(
        !fixture
            .command()
            .args(["seed", "--anchor-ms", &fixture.anchor.to_string()])
            .output()?
            .status
            .success()
    );
    assert_eq!(fixture.inspect()?["rows"], 12);
    Ok(())
}

#[test]
fn soft_deadline_stops_submissions_and_closes_only_after_drain() -> Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    let output = fixture
        .command()
        .args([
            "--timeout-seconds",
            "3",
            "append",
            "--start-id",
            "100",
            "--batches",
            "100",
            "--rows-per-batch",
            "1",
            "--interval-ms",
            "100",
        ])
        .output()?;
    assert!(!output.status.success());
    let result: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(result["cancelled"], true);
    assert_eq!(result["publisherClosed"], true);
    let appended = result["rows"].as_u64().context("rows")?;
    assert!(appended > 0 && appended < 100);
    assert_eq!(fixture.inspect()?["rows"], 12 + appended);
    Ok(())
}

#[cfg(unix)]
#[test]
fn sigterm_joins_active_publication_before_reporting_clean_close() -> Result<()> {
    let fixture = Fixture::new()?;
    fixture.seed()?;
    let mut producer = Producer(Some(
        fixture
            .command()
            .args([
                "append",
                "--start-id",
                "100",
                "--batches",
                "100",
                "--rows-per-batch",
                "1",
                "--interval-ms",
                "100",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?,
    ));
    let mut started = false;
    for _ in 0..100 {
        ensure!(
            producer.child()?.try_wait()?.is_none(),
            "producer exited before SIGTERM"
        );
        if fixture.inspect()?["rows"].as_u64().unwrap_or_default() > 12 {
            started = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    ensure!(started, "producer did not start");
    let signal = Command::new("kill")
        .args(["-TERM", &producer.child()?.id().to_string()])
        .status()?;
    ensure!(signal.success(), "signal failed");
    let output = producer.join()?;
    assert!(!output.status.success());
    let result: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(result["cancelled"], true);
    assert_eq!(result["publisherClosed"], true);
    let appended = result["rows"].as_u64().context("rows")?;
    assert!(appended > 0 && appended < 100);
    assert_eq!(fixture.inspect()?["rows"], 12 + appended);
    Ok(())
}
