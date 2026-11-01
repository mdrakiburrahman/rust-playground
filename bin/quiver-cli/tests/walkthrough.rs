//! Runs the docs/quiver tutorial end to end against the built binary and
//! checks what each step shows.

use std::path::Path;

use assert_cmd::Command;
use predicates::prelude::*;
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

fn segment(dir: &Path, seq: u64) -> std::path::PathBuf {
    dir.join("segments").join(format!("{seq:016}.qseg"))
}

#[test]
fn tutorial_walkthrough() -> Result {
    let temp = tempfile::tempdir()?;
    let dir = temp.path().join("quiver-demo");

    // 1. Generate: real rows become fsynced WAL entries, then segments.
    let generated = run(&["generate"], &dir)?;
    for fact in [
        "telemetry.sqlserver.sqlserver_wait_stats",
        "sha256:a349410a",
        "arrowcol.direct@1",
        "3,546       28          28         4",
    ] {
        assert!(generated.contains(fact), "generate output lacks {fact:?}:\n{generated}");
    }

    // 2. The directory.
    let tree = run(&["tree"], &dir)?;
    for fact in [
        "quiver-cli.json",
        "quiver.wal ",
        "write-ahead log: 28 entries",
        "every entry is in a segment, nothing to replay",
        "0000000000000003.qseg",
        "4 bundles, 474 rows, 0/4 acknowledged",
    ] {
        assert!(tree.contains(fact), "tree output lacks {fact:?}:\n{tree}");
    }
    assert!(
        !tree.contains("quiver.sub."),
        "no progress file before the first acknowledgement:\n{tree}"
    );

    // 3. The WAL and its cursor.
    let wal = run(&["wal"], &dir)?;
    assert!(wal.contains("28 in segments, 0 WAL only"), "{wal}");

    // 4. Segments and rows.
    let segments = run(&["segments"], &dir)?;
    assert!(segments.contains("4 finalized segments"), "{segments}");
    assert!(segments.contains("(28 bundles, 3,546 rows)"), "{segments}");
    assert!(segments.contains("slot 0, schema sha256:a349410a"), "{segments}");
    let shown = run(&["show", "--columns", "customer_id,wait_type,wait_time_ms"], &dir)?;
    assert!(
        shown.contains("segment 0000000000000000.qseg, bundle 0: 128 rows"),
        "{shown}"
    );
    assert!(
        shown.contains("table id 30bbde33-b0e2-5244-9260-b9a10c36c872"),
        "{shown}"
    );
    for fact in ["| customer_id", "customer-0000", "customer-0004", "ASYNC_IO_COMPLETION"] {
        assert!(shown.contains(fact), "show output lacks {fact:?}:\n{shown}");
    }
    assert!(!shown.contains("machine_name"), "--columns selects columns:\n{shown}");

    // 5. Consume: the fully acknowledged segment is deleted.
    let consumed = run(&["consume", "--ack", "12"], &dir)?;
    assert!(
        consumed.contains("8/8  every bundle acknowledged: segment deleted"),
        "{consumed}"
    );
    assert!(
        consumed.contains("4/8  kept: other bundles are still unacknowledged"),
        "{consumed}"
    );
    assert!(consumed.contains("segments on disk: 4 -> 3"), "{consumed}");
    assert!(!segment(&dir, 0).exists());
    let tree = run(&["tree"], &dir)?;
    assert!(
        tree.contains("progress of subscriber arrowcol-delta: 4 of 20 bundles in segments acknowledged"),
        "{tree}"
    );

    // 6. Crash: five fsynced bundles that no segment holds yet.
    let crashed = run(&["crash", "--bundles", "5"], &dir)?;
    assert!(
        crashed.contains("bundle 5 fsynced to the WAL and acknowledged"),
        "{crashed}"
    );
    assert!(crashed.contains("child ended by signal 9"), "{crashed}");
    assert!(!segment(&dir, 4).exists());
    let wal = run(&["wal"], &dir)?;
    assert!(wal.contains("28 in segments, 5 WAL only"), "{wal}");
    assert!(wal.contains("WAL only: replays on next open"), "{wal}");
    let replayable = run(&["show", "--wal", "32", "--rows", "2"], &dir)?;
    assert!(
        replayable.contains("WAL entry 32 (WAL only: replays on next open): 128 rows"),
        "{replayable}"
    );

    // 7. Recover: the restart replays the WAL into a new segment.
    let recovered = run(&["recover"], &dir)?;
    assert!(
        recovered.contains("before   3 segments, 5 WAL-only entries"),
        "{recovered}"
    );
    assert!(
        recovered.contains("new segment 0000000000000004.qseg (5 bundles, 640 rows)"),
        "{recovered}"
    );
    assert!(
        recovered.contains("after    4 segments, 0 WAL-only entries"),
        "{recovered}"
    );
    assert!(segment(&dir, 4).exists());
    let wal = run(&["wal"], &dir)?;
    assert!(wal.contains("33 in segments, 0 WAL only"), "{wal}");

    // Generating again replaces the directory.
    let _ = run(&["generate"], &dir)?;
    let segments = run(&["segments"], &dir)?;
    assert!(segments.contains("4 finalized segments"), "{segments}");
    Ok(())
}

#[test]
fn writers_refuse_directories_quiver_cli_did_not_create() -> Result {
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("notes.txt"), b"someone else's files")?;
    for writer in [vec!["consume"], vec!["crash"], vec!["recover"]] {
        quiver_cli()
            .args(&writer)
            .arg(temp.path())
            .assert()
            .failure()
            .stderr(contains("was not created by quiver-cli"));
    }
    quiver_cli()
        .arg("generate")
        .arg(temp.path())
        .assert()
        .failure()
        .stderr(contains("is not empty and was not created by quiver-cli"));
    quiver_cli()
        .arg("tree")
        .arg(temp.path())
        .assert()
        .success()
        .stdout(contains("notes.txt").and(contains("not a Quiver file")));
    assert_eq!(std::fs::read(temp.path().join("notes.txt"))?, b"someone else's files");
    Ok(())
}
