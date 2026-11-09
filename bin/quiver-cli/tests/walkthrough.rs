//! Exercises generation, inspection, acknowledgement, and crash recovery.

use std::path::Path;

use assert_cmd::Command;
use predicates::str::contains;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn quiver_cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_quiver-cli"))
}

fn run(args: &[&str], dir: &Path) -> Result<String> {
    let output = quiver_cli()
        .args(args)
        .arg(dir)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    Ok(String::from_utf8(output)?)
}

#[test]
fn dfe_durable_buffer_walkthrough() -> Result {
    let temp = tempfile::tempdir()?;
    let dir = temp.path().join("quiver-demo");
    let generated = run(&["generate", "--catalog-endpoint", "memory"], &dir)?;
    for fact in [
        "telemetry.sqlserver.sqlserver_wait_stats",
        "arrowcol.flight@1.1",
        "3,546",
    ] {
        assert!(generated.contains(fact), "generate output lacks {fact:?}:\n{generated}");
    }

    let tree = run(&["tree"], &dir)?;
    assert!(dir.join("quiver-cli.json").is_file());
    assert!(tree.contains("core_0"), "DFE uses a per-core Quiver directory:\n{tree}");
    assert!(tree.contains("quiver.wal"), "{tree}");

    let wal = run(&["wal"], &dir)?;
    assert!(
        wal.contains("28"),
        "the CSV must produce 28 catalog-typed batches:\n{wal}"
    );
    let segments = run(&["segments"], &dir)?;
    assert!(segments.contains("3,546 rows"), "{segments}");
    assert!(
        segments.contains("telemetry.sqlserver.sqlserver_wait_stats v1"),
        "{segments}"
    );
    let shown = run(&["show", "--columns", "customer_id,wait_type,wait_time_ms"], &dir)?;
    for fact in [
        "customer-0000",
        "ASYNC_IO_COMPLETION",
        "id 0, slot 46",
        "telemetry.sqlserver.sqlserver_wait_stats v1",
    ] {
        assert!(shown.contains(fact), "show output lacks {fact:?}:\n{shown}");
    }
    assert!(
        !shown.contains("table 0"),
        "show decoded the envelope identity:\n{shown}"
    );
    assert!(
        !shown.contains("machine_name"),
        "--columns selects displayed fields:\n{shown}"
    );

    let consumed = run(&["consume", "--ack", "1"], &dir)?;
    assert!(consumed.contains("durable-buffer acknowledged 1 bundles"), "{consumed}");
    assert!(run(&["tree"], &dir)?.contains("quiver.sub.durable-buffer"));

    let crashed = run(&["crash", "--bundles", "21"], &dir)?;
    assert!(
        crashed.contains("bundle 21 fsynced to the WAL and acknowledged"),
        "{crashed}"
    );
    assert!(crashed.contains("child ended by signal 9"), "{crashed}");
    let wal = run(&["wal"], &dir)?;
    assert!(
        wal.contains("21 WAL only"),
        "crash must leave 21 replayable bundles:\n{wal}"
    );
    assert!(wal.contains("telemetry.sqlserver.sqlserver_wait_stats v1"), "{wal}");
    let last_wal = run(&["show", "--wal", "48", "--rows", "2"], &dir)?;
    assert!(
        last_wal.contains("telemetry.sqlserver.sqlserver_wait_stats v1"),
        "{last_wal}"
    );
    assert!(last_wal.contains("128 rows"), "{last_wal}");

    let recovered = run(&["recover"], &dir)?;
    assert!(recovered.contains("21 WAL-only entries"), "{recovered}");
    assert!(recovered.contains("21 bundles, 2,688 rows"), "{recovered}");
    let segments = run(&["segments"], &dir)?;
    assert!(
        segments.contains("49 bundles, 6,234 rows"),
        "all acknowledged and replayed rows remain:\n{segments}"
    );
    let _ = run(&["recover"], &dir)?;
    let segments_after_repeat = run(&["segments"], &dir)?;
    assert!(
        segments_after_repeat.contains("49 bundles, 6,234 rows"),
        "recovery is idempotent:\n{segments_after_repeat}"
    );
    assert!(run(&["wal"], &dir)?.contains("0 WAL only"));

    let regenerated = run(
        &["generate", "--catalog-endpoint", "memory", "--batch-rows", "256"],
        &dir,
    )?;
    assert!(regenerated.contains("generated"), "{regenerated}");
    let replaced = run(&["segments"], &dir)?;
    assert!(
        replaced.contains("14 bundles, 3,546 rows"),
        "old buffers were not replaced:\n{replaced}"
    );
    assert!(
        !replaced.contains("49 bundles"),
        "old recovery data remains:\n{replaced}"
    );
    let marker: serde_json::Value = serde_json::from_slice(&std::fs::read(dir.join("quiver-cli.json"))?)?;
    assert_eq!(marker["generation_status"], "complete");
    assert_eq!(marker["bundles"], 14);
    assert_eq!(marker["rows"], 3_546);
    let regenerated_rows = run(&["show", "--columns", "customer_id"], &dir)?;
    assert!(
        regenerated_rows.contains("customer-0000"),
        "regenerated storage is readable:\n{regenerated_rows}"
    );
    Ok(())
}

#[test]
fn writers_refuse_unmarked_directories_before_catalog_access() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("notes.txt"), b"someone else's files")?;
    quiver_cli()
        .args(["generate", "--catalog-endpoint", "http://127.0.0.1:1"])
        .arg(temp.path())
        .assert()
        .failure()
        .stderr(contains("is not empty and was not created by quiver-cli"));
    for writer in ["consume", "crash", "recover"] {
        quiver_cli()
            .arg(writer)
            .arg(temp.path())
            .assert()
            .failure()
            .stderr(contains("was not created by quiver-cli"));
    }
    quiver_cli()
        .arg("tree")
        .arg(temp.path())
        .assert()
        .success()
        .stdout(contains("notes.txt"));
    assert_eq!(std::fs::read(temp.path().join("notes.txt"))?, b"someone else's files");
    Ok(())
}
