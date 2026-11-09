//! Commands that reopen only a directory quiver-cli created.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use arrowcol_catalog::CatalogSchemaVersion;
use clap::Args;
use otel_arrow_dfe_quiver::inspect::inspect_wal;
use otel_arrow_dfe_quiver::segment::SegmentReader;
use otel_arrow_dfe_quiver::subscriber::SubscriberId;
use serde_json::Value;

use crate::data::{self, BUILTIN_CSV_PATH, BenchmarkTable};
use crate::dfe::DfeRecordBundle;
use crate::dir::{DEFAULT_DIR, MARKER_FILE, require_cli_dir, segment_files};
use crate::engine::{self, MAX_CRASH_BUNDLES, SUBSCRIBER};
use crate::format::{self, Align, Table};

#[derive(Args)]
pub(crate) struct ConsumeArgs {
    /// Quiver data directory created by `quiver-cli generate`.
    #[arg(default_value = DEFAULT_DIR)]
    dir: PathBuf,
    /// Bundles to acknowledge, oldest first.
    #[arg(long, default_value_t = 8)]
    ack: usize,
}

#[derive(Args)]
pub(crate) struct CrashArgs {
    /// Quiver data directory created by `quiver-cli generate`.
    #[arg(default_value = DEFAULT_DIR)]
    pub(crate) dir: PathBuf,
    /// Bundles the child persists before it is killed (1-1000; no timed segment seal).
    #[arg(long, default_value_t = 5)]
    pub(crate) bundles: usize,
}

pub(crate) async fn consume(args: &ConsumeArgs) -> Result<()> {
    require_cli_dir(&args.dir)?;
    let core_dir = args.dir.join("core_0");
    let before = segment_bundle_counts(&core_dir)?;
    let engine = engine::open(&args.dir).await?;
    let subscriber = SubscriberId::new(SUBSCRIBER)?;
    let mut acked = BTreeMap::<u64, u32>::new();
    for _ in 0..args.ack {
        let Some(handle) = engine
            .next_bundle(&subscriber, Some(Duration::from_secs(2)), None)
            .await?
        else {
            break;
        };
        *acked.entry(handle.bundle_ref().segment_seq.raw()).or_default() += 1;
        handle.ack();
    }
    let _ = engine.maintain().await?;
    engine.shutdown().await?;
    let after = segment_bundle_counts(&core_dir)?;

    let total = acked.values().sum::<u32>();
    println!(
        "{SUBSCRIBER} acknowledged {total} bundles, oldest first, as the durable buffer does after downstream success"
    );
    println!();
    let mut table = Table::new(&[
        ("segment", Align::Left),
        ("acknowledged", Align::Right),
        ("result", Align::Left),
    ]);
    for (seq, count) in &acked {
        let bundles = before.get(seq).copied().unwrap_or_default();
        let result = if after.contains_key(seq) {
            "kept: other bundles are still unacknowledged".to_owned()
        } else {
            "every bundle acknowledged: segment deleted".to_owned()
        };
        table.row(vec![segment_name(*seq), format!("{count}/{bundles}"), result]);
    }
    println!("{}", table.render());
    println!();
    println!("  segments on disk: {} -> {}", before.len(), after.len());
    Ok(())
}

fn segment_bundle_counts(dir: &Path) -> Result<BTreeMap<u64, u32>> {
    segment_files(dir)?
        .into_iter()
        .map(|file| {
            let reader = SegmentReader::open(&file.path)?;
            Ok((file.seq, u32::try_from(reader.bundle_count())?))
        })
        .collect()
}

fn segment_name(seq: u64) -> String {
    format!("{seq:016}.qseg")
}

pub(crate) fn crash(args: &CrashArgs) -> Result<()> {
    require_cli_dir(&args.dir)?;
    if !(1..=MAX_CRASH_BUNDLES).contains(&args.bundles) {
        bail!("--bundles must be between 1 and {MAX_CRASH_BUNDLES}");
    }
    let mut child = Command::new(std::env::current_exe()?)
        .arg("crash-child")
        .arg(&args.dir)
        .arg("--bundles")
        .arg(args.bundles.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .context("start the child process")?;
    println!(
        "child process {} writes DFE bundles (WAL fsync before each acknowledgement)",
        child.id()
    );
    let stdout = child.stdout.take().context("child stdout")?;
    let mut persisted = 0;
    for line in BufReader::new(stdout).lines() {
        let line = line?;
        if let Some(ordinal) = line.strip_prefix("persisted ") {
            persisted += 1;
            println!("  bundle {ordinal} fsynced to the WAL and acknowledged");
            if persisted == args.bundles {
                break;
            }
        }
    }
    child.kill().context("kill the child process")?;
    let status = child.wait()?;
    match status.signal() {
        Some(signal) => println!("kill -9: child ended by signal {signal} with its open segment never finalized"),
        None => bail!("the child process exited before it could be killed: {status}"),
    }
    if persisted < args.bundles {
        bail!("the child persisted only {persisted} of {} bundles", args.bundles);
    }
    println!();
    println!("next: quiver-cli wal {}", args.dir.display());
    Ok(())
}

pub(crate) async fn crash_child(args: &CrashArgs) -> Result<()> {
    let marker: Value = serde_json::from_slice(&std::fs::read(args.dir.join(MARKER_FILE))?)?;
    let schema: CatalogSchemaVersion =
        serde_json::from_value(marker["schema_snapshot"].clone()).context("marker Schema Catalog snapshot")?;
    let table = BenchmarkTable::from_schema(schema)?;
    let csv = marker["source_csv"]
        .as_str()
        .filter(|source| *source != BUILTIN_CSV_PATH)
        .map(PathBuf::from);
    let rows_per_bundle = marker["rows_per_bundle"]
        .as_u64()
        .and_then(|rows| usize::try_from(rows).ok())
        .context("marker rows_per_bundle")?;
    let batches = data::read_csv(&table.arrow, csv.as_deref(), rows_per_bundle)?;
    let engine = engine::open_for_crash(&args.dir).await?;
    let mut stdout = std::io::stdout();
    for (ordinal, batch) in batches.iter().cycle().take(args.bundles).enumerate() {
        engine
            .ingest(&DfeRecordBundle::new(&table.schema, batch.clone())?)
            .await?;
        writeln!(stdout, "persisted {}", ordinal + 1)?;
        stdout.flush()?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    std::future::pending::<()>().await;
    Ok(())
}

pub(crate) async fn recover(dir: &Path) -> Result<()> {
    require_cli_dir(dir)?;
    let core_dir = dir.join("core_0");
    let wal_before = inspect_wal(&core_dir)?;
    let segments_before = segment_files(&core_dir)?
        .into_iter()
        .map(|file| file.seq)
        .collect::<Vec<_>>();
    let replayable = wal_before.unconsumed().count();

    let engine = engine::open(dir).await?;
    engine.flush().await?;
    let _ = engine.maintain().await?;
    engine.shutdown().await?;

    let wal_after = inspect_wal(&core_dir)?;
    let segments_after = segment_files(&core_dir)?;
    println!("opened {} like a restarted durable buffer", dir.display());
    println!();
    println!(
        "  before   {} segments, {replayable} WAL-only entries{}",
        segments_before.len(),
        if wal_before.stopped_early.is_some() {
            " (plus a torn final entry)"
        } else {
            ""
        }
    );
    for file in segments_after
        .iter()
        .filter(|file| !segments_before.contains(&file.seq))
    {
        let reader = SegmentReader::open(&file.path)?;
        let rows = reader.manifest().iter().map(|entry| entry.item_count()).sum::<u64>();
        println!(
            "  replay   WAL entries -> new segment {} ({} bundles, {} rows)",
            file.name(),
            reader.bundle_count(),
            format::count(rows)
        );
    }
    println!(
        "  after    {} segments, {} WAL-only entries",
        segments_after.len(),
        wal_after.unconsumed().count()
    );
    Ok(())
}
