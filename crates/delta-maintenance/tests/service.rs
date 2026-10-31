//! End-to-end evidence for the standalone service binary.

use anyhow::{Result, anyhow};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_deltalake::delta::{DeltaTable, INGESTION_PARTITION};
use assert_cmd::Command;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use url::Url;

const BASE: i64 = 1_704_067_200_000;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(INGESTION_PARTITION, DataType::Utf8, false),
    ]))
}

fn table(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root)?;
    let url = Url::from_directory_path(std::fs::canonicalize(root)?).map_err(|()| anyhow!("URL"))?;
    let table = DeltaTable::open_url(url, schema(), vec![INGESTION_PARTITION.into()])?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    for minute in 0..4_i64 {
        for part in 0..2_i64 {
            let value = format!("2024-01-01-00-{minute:02}");
            let batch = RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(vec![minute * 10 + part])),
                    Arc::new(StringArray::from(vec![value.as_str()])),
                ],
            )?;
            runtime.block_on(async {
                let mut file = table
                    .new_file_partitioned(
                        schema(),
                        BTreeMap::from([(INGESTION_PARTITION.to_owned(), Some(value.clone()))]),
                    )?
                    .with_ingestion_time(BASE + minute * 60_000)?;
                file.write(&batch).await?;
                file.finish().await.map(|_| ())
            })?;
        }
    }
    Ok(())
}

fn version(root: &Path) -> Result<usize> {
    Ok(std::fs::read_dir(root.join("_delta_log"))?
        .filter(|entry| {
            entry
                .as_ref()
                .is_ok_and(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
        })
        .count())
}

fn lines(output: &[u8]) -> Result<Vec<serde_json::Value>> {
    std::str::from_utf8(output)?
        .lines()
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

fn service(root: &Path) -> Result<Command> {
    let mut command = Command::cargo_bin("delta-maintenance")?;
    command.env_remove("OTEL_EXPORTER_OTLP_ENDPOINT").args([
        "--root",
        Url::from_directory_path(root).map_err(|()| anyhow!("URL"))?.as_str(),
        "--table",
        "events",
        "--granularity",
        "minute",
        "--anchor-ms",
        &(BASE + 3 * 60_000 + 30_000).to_string(),
        "--purge-retention-secs",
        "90",
    ]);
    Ok(command)
}

#[test]
fn ordinary_short_vacuum_requires_explicit_acknowledgement_and_does_not_authorize_pruning() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("events");
    table(&path)?;
    let optimized = service(root.path())?
        .args(["--operation", "optimize", "--sealed-final", "--apply"])
        .output()?;
    assert!(
        optimized.status.success(),
        "{}",
        String::from_utf8_lossy(&optimized.stderr)
    );
    let anchor = (arrow_deltalake::maintenance::now_ms()? + 1_000).to_string();
    let vacuum = |extra: &[&str]| -> Result<std::process::Output> {
        let mut command = Command::cargo_bin("delta-maintenance")?;
        Ok(command
            .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
            .args([
                "--root",
                Url::from_directory_path(root.path())
                    .map_err(|()| anyhow!("URL"))?
                    .as_str(),
                "--table",
                "events",
                "--operation",
                "vacuum",
                "--anchor-ms",
                &anchor,
            ])
            .args(extra)
            .output()?)
    };
    let default = vacuum(&[])?;
    assert!(default.status.success());
    let reports = lines(&default.stdout)?;
    assert_eq!(reports[0]["vacuum"]["retention_ms"], 604_800_000);
    assert_eq!(reports[0]["vacuum"]["allow_short_vacuum_retention"], false);
    assert_eq!(reports[0]["report"]["files_deleted"], 0);
    let unchanged_default = vacuum(&["--allow-short-vacuum-retention"])?;
    assert!(unchanged_default.status.success());
    assert_eq!(
        lines(&unchanged_default.stdout)?[0]["vacuum"]["retention_ms"],
        604_800_000
    );
    for extra in [
        vec!["--vacuum-retention-secs", "0"],
        vec!["--vacuum-retention-secs", "0", "--unsafe-short-vacuum-for-tests"],
    ] {
        let output = vacuum(&extra)?;
        assert!(!output.status.success());
        assert_eq!(lines(&output.stdout)?[0]["error_kind"], "unsafe_request");
    }
    let negative = vacuum(&["--vacuum-retention-secs=-1", "--allow-short-vacuum-retention"])?;
    assert_eq!(negative.status.code(), Some(2), "negative durations fail CLI parsing");
    let acknowledged = vacuum(&[
        "--vacuum-retention-secs",
        "0",
        "--allow-short-vacuum-retention",
        "--apply",
    ])?;
    assert!(
        acknowledged.status.success(),
        "{}",
        String::from_utf8_lossy(&acknowledged.stderr)
    );
    let reports = lines(&acknowledged.stdout)?;
    assert_eq!(reports[0]["vacuum"]["retention_ms"], 0);
    assert_eq!(reports[0]["vacuum"]["allow_short_vacuum_retention"], true);
    assert_eq!(reports[0]["vacuum"]["unsafe_short_vacuum_for_tests"], false);
    assert_eq!(reports[0]["report"]["files_deleted"], 8);
    let prune = service(root.path())?
        .args([
            "--operation",
            "prune-log",
            "--log-retention-secs",
            "0",
            "--allow-short-vacuum-retention",
            "--apply",
        ])
        .output()?;
    assert!(
        !prune.status.success(),
        "VACUUM acknowledgement cannot lower log retention"
    );
    assert_eq!(lines(&prune.stdout)?[0]["error_kind"], "unsafe_request");
    Ok(())
}

/// One-shot final compaction uses the identical planner in dry-run and apply.
/// Fresh empty-plan evidence, not zero output counters alone, establishes no-op.
#[test]
fn sealed_final_dry_run_apply_and_fresh_noop_evidence() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("events");
    table(&path)?;
    let before = version(&path)?;
    let args = ["--operation", "optimize", "--sealed-final"];
    let dry = service(root.path())?.args(args).output()?;
    assert!(dry.status.success(), "{}", String::from_utf8_lossy(&dry.stderr));
    let dry = lines(&dry.stdout)?;
    assert_eq!(dry.len(), 1);
    let evidence = &dry[0];
    assert_eq!(evidence["applied"], false);
    assert_eq!(evidence["optimize"]["config"]["sealed_final"], true);
    assert_eq!(evidence["optimize"]["config"]["max_files_per_bin"], 64);
    assert_eq!(evidence["optimize"]["config"]["granularity"], "minute");
    assert_eq!(evidence["optimize"]["plan"]["bins"].as_array().map(Vec::len), Some(4));
    assert_eq!(evidence["optimize"]["plan"]["protected_partitions"], 0);
    assert_eq!(evidence["report"]["files_removed"], 8);
    assert_eq!(
        evidence["report"]["files_added"], 0,
        "outputs do not exist before rewrite"
    );
    assert_eq!(version(&path)?, before, "dry run changes no Delta version");

    let apply = service(root.path())?.args(args).arg("--apply").output()?;
    assert!(apply.status.success(), "{}", String::from_utf8_lossy(&apply.stderr));
    let apply = lines(&apply.stdout)?;
    assert_eq!(apply.len(), 1);
    assert_eq!(apply[0]["report"]["outcome"], "committed");
    assert_eq!(apply[0]["report"]["files_removed"], 8);
    assert_eq!(apply[0]["report"]["files_added"], 4);
    assert_eq!(apply[0]["report"]["outputs"].as_array().map(Vec::len), Some(4));
    assert_eq!(apply[0]["optimize"]["plan"]["bins"].as_array().map(Vec::len), Some(4));
    assert_eq!(version(&path)?, before + 1);
    for apply in [false, true] {
        let mut command = service(root.path())?;
        command.args(args);
        if apply {
            command.arg("--apply");
        }
        let output = command.output()?;
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        let reports = lines(&output.stdout)?;
        assert_eq!(reports.len(), 1);
        let report = &reports[0];
        assert_eq!(report["table"], "events");
        assert_eq!(report["operation"], "optimize");
        assert!(report.get("error").is_none());
        assert!(report.get("error_kind").is_none());
        assert_eq!(report["report"]["outcome"], if apply { "noop" } else { "planned" });
        assert_eq!(report["optimize"]["plan"]["bins"], serde_json::json!([]));
        assert_eq!(report["optimize"]["plan"]["backlog_files"], 0);
        assert_eq!(report["optimize"]["plan"]["protected_partitions"], 0);
        assert_eq!(report["optimize"]["plan"]["skipped_partitions"], 0);
        assert_eq!(report["report"]["files_removed"], 0);
        assert_eq!(report["report"]["files_added"], 0);
        assert_eq!(report["report"]["read_version"], report["report"]["version"]);
        assert_eq!(
            report["report"]["read_version"],
            report["optimize"]["plan"]["header"]["read_version"]
        );
        assert_eq!(version(&path)?, before + 1);
    }
    Ok(())
}

/// Invalid final-compaction combinations fail configuration before table
/// discovery, publisher/store setup or metrics startup, in dry-run and apply.
#[test]
fn sealed_final_rejects_schedules_and_other_operations_before_effects() -> Result<()> {
    let root = tempfile::tempdir()?;
    let nonexistent = root.path().join("must-not-exist");
    let invalid = [
        vec!["--operation", "optimize", "--interval-secs", "1"],
        vec!["--operation", "optimize", "--max-cycles", "1"],
        vec!["--operation", "purge"],
        vec!["--operation", "vacuum"],
        vec!["--operation", "checkpoint"],
        vec!["--operation", "prune-log"],
        vec!["--operation", "optimize", "--operation", "purge"],
        vec!["--operation", "optimize", "--operation", "vacuum"],
        vec!["--operation", "optimize", "--operation", "checkpoint"],
        vec!["--operation", "optimize", "--operation", "prune-log"],
    ];
    for apply in [false, true] {
        for args in &invalid {
            let mut command = service(&nonexistent)?;
            command.args(args).arg("--sealed-final");
            if apply {
                command.arg("--apply");
            }
            let output = command.output()?;
            assert_eq!(output.status.code(), Some(2), "{args:?}");
            assert!(output.stdout.is_empty(), "invalid configuration must not run tables");
            assert!(String::from_utf8_lossy(&output.stderr).contains("--sealed-final requires one-shot optimize only"));
            assert!(!nonexistent.exists(), "no table/root discovery or filesystem effects");
        }
    }
    Ok(())
}

/// Scenario: one-shot dry run, then `--apply`, then a repeat.
/// Guarantees: dry runs never change the table, apply compacts and purges with
/// JSON reports, and a repeated apply is a no-op without new commits.
#[test]
fn dry_run_then_apply_then_noop() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("events");
    table(&path)?;
    let before = version(&path)?;
    let dry = service(root.path())?
        .args(["--operation", "optimize", "--operation", "purge"])
        .output()?;
    assert!(dry.status.success(), "{}", String::from_utf8_lossy(&dry.stderr));
    let reports = lines(&dry.stdout)?;
    assert_eq!(reports.len(), 2);
    assert!(reports.iter().all(|report| report["report"]["outcome"] == "planned"));
    assert_eq!(version(&path)?, before);

    let applied = service(root.path())?
        .args(["--operation", "optimize", "--operation", "purge", "--apply"])
        .output()?;
    assert!(applied.status.success(), "{}", String::from_utf8_lossy(&applied.stderr));
    let reports = lines(&applied.stdout)?;
    assert_eq!(reports[0]["operation"], "optimize");
    assert_eq!(reports[0]["report"]["outcome"], "committed");
    assert_eq!(reports[1]["report"]["outcome"], "committed");
    assert_eq!(
        reports[1]["report"]["files_removed"], 2,
        "minute 00 was compacted to one file"
    );
    let after = version(&path)?;
    assert_eq!(after, before + 2);

    let repeat = service(root.path())?
        .args(["--operation", "purge", "--apply"])
        .output()?;
    assert!(repeat.status.success());
    assert_eq!(lines(&repeat.stdout)?[0]["report"]["outcome"], "noop");
    assert_eq!(version(&path)?, after);
    Ok(())
}

/// Scenario: dry-run, apply and repeat of the checkpoint operation.
/// Guarantees: a dry run writes nothing, apply writes one checkpoint without a
/// commit or any JSON deletion, a repeat is a no-op, and VACUUM still sees the
/// complete JSON history afterwards.
#[test]
fn checkpoint_bounds_replay_without_deleting_history() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("events");
    table(&path)?;
    let before = version(&path)?;
    let log = path.join("_delta_log");
    let checkpoints = || -> Result<usize> {
        Ok(std::fs::read_dir(&log)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".checkpoint.parquet"))
            .count())
    };
    let args = ["--operation", "checkpoint", "--checkpoint-every-commits", "1"];
    let dry = service(root.path())?.args(args).output()?;
    assert!(dry.status.success(), "{}", String::from_utf8_lossy(&dry.stderr));
    assert_eq!(lines(&dry.stdout)?[0]["report"]["outcome"], "planned");
    assert_eq!(checkpoints()?, 0);

    let applied = service(root.path())?.args(args).arg("--apply").output()?;
    assert!(applied.status.success(), "{}", String::from_utf8_lossy(&applied.stderr));
    assert_eq!(lines(&applied.stdout)?[0]["report"]["outcome"], "checkpointed");
    assert_eq!(checkpoints()?, 1);
    assert!(log.join("_last_checkpoint").is_file());
    assert_eq!(version(&path)?, before, "no commit and no JSON deleted");

    let repeat = service(root.path())?.args(args).arg("--apply").output()?;
    assert!(repeat.status.success());
    assert_eq!(lines(&repeat.stdout)?[0]["report"]["outcome"], "noop");
    assert_eq!(checkpoints()?, 1);

    let vacuum = service(root.path())?.args(["--operation", "vacuum"]).output()?;
    assert!(vacuum.status.success(), "{}", String::from_utf8_lossy(&vacuum.stderr));
    assert_ne!(lines(&vacuum.stdout)?[0]["error_kind"], "unsupported");
    Ok(())
}

/// Service command on the wall clock (no fixed anchor), as VACUUM and pruning require.
fn wall_clock_service(root: &Path) -> Result<Command> {
    let mut command = Command::cargo_bin("delta-maintenance")?;
    command.env_remove("OTEL_EXPORTER_OTLP_ENDPOINT").args([
        "--root",
        Url::from_directory_path(root).map_err(|()| anyhow!("URL"))?.as_str(),
        "--table",
        "events",
        "--granularity",
        "minute",
    ]);
    Ok(command)
}

/// Scenario: OPTIMIZE, short-retention VACUUM LITE (progress record) and a
/// checkpoint on a marked disposable root, then `prune-log` dry run, apply and repeat.
/// Guarantees: short log retention needs the unsafe flag; the dry run deletes
/// nothing; apply deletes JSON `0..=C` without a commit, keeping the checkpoint;
/// a repeat is a no-op and VACUUM LITE still works over the pruned history.
#[test]
fn prune_log_dry_run_then_apply() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("events");
    table(&path)?;
    std::fs::write(path.join(arrow_deltalake::maintenance::DISPOSABLE_TEST_MARKER), b"")?;
    let optimized = wall_clock_service(root.path())?
        .args(["--operation", "optimize", "--apply"])
        .output()?;
    assert!(
        optimized.status.success(),
        "{}",
        String::from_utf8_lossy(&optimized.stderr)
    );
    assert_eq!(lines(&optimized.stdout)?[0]["report"]["outcome"], "committed");
    std::thread::sleep(Duration::from_millis(20));
    let prepared = wall_clock_service(root.path())?
        .args([
            "--operation",
            "vacuum",
            "--operation",
            "checkpoint",
            "--vacuum-retention-secs",
            "0",
            "--unsafe-short-vacuum-for-tests",
            "--checkpoint-every-commits",
            "1",
            "--apply",
        ])
        .output()?;
    assert!(
        prepared.status.success(),
        "{}",
        String::from_utf8_lossy(&prepared.stderr)
    );
    let outcomes: Vec<_> = lines(&prepared.stdout)?
        .iter()
        .map(|report| report["report"]["outcome"].clone())
        .collect();
    assert_eq!(outcomes, ["deleted", "checkpointed"]);
    let json = version(&path)?;
    assert!(json > 1);

    let refused = wall_clock_service(root.path())?
        .args(["--operation", "prune-log", "--log-retention-secs", "0"])
        .output()?;
    assert_eq!(refused.status.code(), Some(1));
    assert_eq!(lines(&refused.stdout)?[0]["error_kind"], "unsafe_request");

    let prune = [
        "--operation",
        "prune-log",
        "--log-retention-secs",
        "0",
        "--unsafe-short-vacuum-for-tests",
    ];
    let dry = wall_clock_service(root.path())?.args(prune).output()?;
    assert!(dry.status.success(), "{}", String::from_utf8_lossy(&dry.stderr));
    let report = &lines(&dry.stdout)?[0];
    assert_eq!(report["operation"], "prune_log");
    assert_eq!(report["report"]["outcome"], "planned");
    assert_eq!(report["report"]["files_removed"], json - 1, "C.json is retained");
    assert_eq!(version(&path)?, json, "dry run deletes nothing");

    let applied = wall_clock_service(root.path())?.args(prune).arg("--apply").output()?;
    assert!(applied.status.success(), "{}", String::from_utf8_lossy(&applied.stderr));
    let report = &lines(&applied.stdout)?[0]["report"];
    assert_eq!(report["outcome"], "pruned");
    assert_eq!(report["files_deleted"], json - 1);
    assert_eq!(version(&path)?, 1, "JSON < C pruned, C.json retained");
    assert!(path.join("_delta_log").join(format!("{:020}.json", json - 1)).is_file());
    let log = path.join("_delta_log");
    assert!(log.join("_last_checkpoint").is_file());
    assert!(log.join(format!("{:020}.checkpoint.parquet", json - 1)).is_file());
    assert!(path.join("_arrowcol_maintenance").join("log-floor.json").is_file());

    let repeat = wall_clock_service(root.path())?.args(prune).arg("--apply").output()?;
    assert!(repeat.status.success(), "{}", String::from_utf8_lossy(&repeat.stderr));
    assert_eq!(lines(&repeat.stdout)?[0]["report"]["outcome"], "noop");
    let vacuum = wall_clock_service(root.path())?
        .args(["--operation", "vacuum"])
        .output()?;
    assert!(vacuum.status.success(), "{}", String::from_utf8_lossy(&vacuum.stderr));
    Ok(())
}

/// Scenario: unsafe or invalid requests.
/// Guarantees: traversal names and short VACUUM retention fail visibly with nonzero exit.
#[test]
fn rejects_traversal_and_unsafe_vacuum() -> Result<()> {
    let root = tempfile::tempdir()?;
    table(&root.path().join("events"))?;
    service(root.path())?
        .args(["--operation", "optimize", "--table", ".."])
        .assert()
        .code(2);
    let output = service(root.path())?
        .args([
            "--operation",
            "vacuum",
            "--vacuum-retention-secs",
            "0",
            "--unsafe-short-vacuum-for-tests",
            "--apply",
        ])
        .output()?;
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(lines(&output.stdout)?[0]["error_kind"], "unsafe_request");
    service(root.path())?
        .args(["--operation", "optimize", "--table", "missing"])
        .assert()
        .code(1);
    Ok(())
}

fn scrape(port: u16) -> Result<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
    let mut body = String::new();
    stream.read_to_string(&mut body)?;
    Ok(body)
}

/// Scenario: scheduled mode with a metrics listener.
/// Guarantees: Prometheus exposes bounded operation/outcome series and a heartbeat.
#[test]
fn scheduled_mode_serves_prometheus_metrics() -> Result<()> {
    let root = tempfile::tempdir()?;
    table(&root.path().join("events"))?;
    let port = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("delta-maintenance"))
        .args(service(root.path())?.get_args())
        .args([
            "--operation",
            "optimize",
            "--interval-secs",
            "1",
            "--max-cycles",
            "3",
            "--apply",
        ])
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .stdout(std::process::Stdio::null())
        .spawn()?;
    let started = Instant::now();
    let body = loop {
        if let Ok(body) = scrape(port)
            && body.contains("delta_maintenance_runs_total{operation=\"optimize\",outcome=\"committed\"} 1")
        {
            break body;
        }
        if started.elapsed() > Duration::from_secs(60) {
            child.kill()?;
            anyhow::bail!("metrics never reported the committed optimize");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(body.contains("delta_maintenance_heartbeat_timestamp_ms"));
    assert!(body.contains("delta_maintenance_files_removed_total{operation=\"optimize\"} 6"));
    assert!(child.wait()?.success());
    assert!(
        body.lines()
            .all(|line| line.starts_with('#') || !line.contains("table")),
        "no table labels"
    );
    Ok(())
}
