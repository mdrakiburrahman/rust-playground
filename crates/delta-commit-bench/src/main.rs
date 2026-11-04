//! `dcb`: benchmark of the ArrowCol Delta commit path.

mod arrow_producer;
mod backend;
mod checker;
mod commit_loop;
mod producer;
mod store;
mod table;
mod wal;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use commit_loop::{
    Adopt, CheckpointExec, CommitLoop, CommitRecord, EpochMode, FenceOn, Idempotency, Limits, LoopConfig, Pacing,
};
use deltalake_core::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake_core::kernel::{Action, Add, Remove};
use deltalake_core::protocol::DeltaOperation;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::Serialize;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use store::{Counters, FaultPlan, InstrumentedStore};
use table::now_millis;
use tokio::sync::mpsc;
use url::Url;

#[derive(Parser)]
#[command(name = "dcb", about = "Delta commit benchmark for ArrowCol")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create one or more benchmark tables.
    Create(CreateArgs),
    /// Run commit loops fed by synthetic encoders.
    Run(Box<RunArgs>),
    /// Verify the four guarantees for a table.
    Check(CheckArgs),
    /// Time a fresh reader's snapshot load.
    Read(ReadArgs),
    /// Time a checkpoint write.
    Checkpoint(TableArg),
    /// Commit compaction-style (remove N, add 1) transactions while appends run.
    Optimize(OptimizeArgs),
    /// Add txn-only commits that accumulate distinct appIds (follow-up Q3).
    SeedTxn(SeedTxnArgs),
    /// Print the txn version a fresh snapshot reports for each appId.
    TxnProbe(TxnProbeArgs),
    /// Checkpoint, then delete every log file before the checkpoint regardless of age.
    CleanupLog(TableArg),
    /// Delete every data file removed from the table, with zero retention.
    Vacuum(TableArg),
}

#[derive(Args)]
struct SeedTxnArgs {
    #[arg(long)]
    table_url: Url,
    /// Distinct appIds to add.
    #[arg(long)]
    count: usize,
    #[arg(long, default_value_t = 100)]
    per_commit: usize,
    #[arg(long, default_value = "seed")]
    prefix: String,
}

#[derive(Args)]
struct TxnProbeArgs {
    #[arg(long)]
    table_url: Url,
    #[arg(long)]
    app_id: Vec<String>,
}

#[derive(Args)]
struct TableArg {
    #[arg(long)]
    table_url: Url,
}

#[derive(Args, Clone)]
struct Tables {
    /// Explicit table URL; repeatable.
    #[arg(long)]
    table_url: Vec<Url>,
    /// Base URL for generated tables `t00000`, `t00001`, ...
    #[arg(long)]
    table_base: Option<Url>,
    /// Number of generated tables under `--table-base`.
    #[arg(long, default_value_t = 0)]
    tables: usize,
}

impl Tables {
    fn urls(&self) -> Result<Vec<Url>> {
        let mut urls = self.table_url.clone();
        if let Some(base) = &self.table_base {
            let base = if base.as_str().ends_with('/') {
                base.clone()
            } else {
                Url::parse(&format!("{base}/"))?
            };
            for i in 0..self.tables {
                urls.push(base.join(&format!("t{i:05}"))?);
            }
        }
        if urls.is_empty() {
            bail!("no tables given");
        }
        Ok(urls)
    }
}

#[derive(Args)]
struct CreateArgs {
    #[command(flatten)]
    tables: Tables,
    /// Table property `key=value`; repeatable.
    #[arg(long = "prop")]
    props: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, clap::ValueEnum)]
enum WalSync {
    PerCommit,
    Group,
}

#[derive(Clone, Copy, Debug, Serialize, clap::ValueEnum)]
enum CheckpointMode {
    /// delta-rs post-commit hook (inside the commit call).
    Inline,
    /// After the commit is acknowledged, every `--checkpoint-every` versions.
    Separate,
    Off,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, clap::ValueEnum)]
enum CommitRuntime {
    /// Multi-thread runtime with `--commit-threads` workers.
    Multi,
    /// Current-thread runtime on one dedicated thread. On Azure, kernel log replays with many
    /// concurrent GETs (checkpoints) stall until the 30 s client timeout under this runtime.
    Current,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, clap::ValueEnum)]
enum LogCleanup {
    Default,
    On,
    Off,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, clap::ValueEnum)]
enum ProducerKind {
    /// Tiny Parquet files encoded on tokio workers, paced by `--rate`.
    Small,
    /// Large Parquet files encoded from in-memory Arrow batches on `--encoders` dedicated threads.
    Arrow,
}

#[derive(Args, Serialize)]
struct RunArgs {
    #[command(flatten)]
    #[serde(skip)]
    tables: Tables,
    /// Create missing tables first.
    #[arg(long)]
    create: bool,
    /// Table property for `--create`; repeatable.
    #[arg(long = "prop")]
    props: Vec<String>,
    /// Instance identity; the per-table appId is `<app-id>/<table>`.
    #[arg(long)]
    app_id: String,
    /// Delta library for the commit loops. Table creation (`--create`) always uses delta-rs.
    #[arg(long, value_enum, default_value_t = backend::Lib::Dl1)]
    lib: backend::Lib,
    #[arg(long)]
    wal_dir: PathBuf,
    /// Take over another identity: fence `<adopt-from>/<table>` in the table log, then recommit
    /// its unlanded pending manifests from `--wal-dir` under this loop's appId.
    #[arg(long)]
    adopt_from: Option<String>,
    /// appId ownership model (follow-up Q1 and Q2).
    #[arg(long, value_enum, default_value_t = EpochMode::Off)]
    epochs: EpochMode,
    /// Signaled ownership: on start, fence the current appId of this identity; repeatable.
    #[arg(long)]
    fence_identity: Vec<String>,
    /// Inferred ownership: when a file arrives while not the owner, fence the previous committer.
    #[arg(long)]
    infer_fence: bool,
    /// Commit this identity's pending manifests under a fresh epoch, then exit. No producer.
    #[arg(long)]
    drain_only: bool,
    /// Manifest durability (follow-up Q4): `per-commit` writes a file per manifest with three
    /// fsyncs on the commit task; `group` appends to one process-wide log with one fdatasync per
    /// batch on a dedicated thread.
    #[arg(long, value_enum, default_value_t = WalSync::PerCommit)]
    wal_sync: WalSync,
    /// Group mode: after the first queued append, wait this long for more before syncing.
    #[arg(long, default_value_t = 0)]
    group_sync_ms: u64,
    /// Group mode: synthetic data appended through the same log and syncs, MB/s.
    #[arg(long, default_value_t = 0.0)]
    data_wal_mbps: f64,
    /// Router simulation: produce files only while this file contains `--app-id`.
    #[arg(long)]
    route_file: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = ProducerKind::Small)]
    producer: ProducerKind,
    /// Files per second per table. With `--producer arrow`, 0 encodes continuously.
    #[arg(long, default_value_t = 1.2)]
    rate: f64,
    /// Encoders per table (encoder threads with `--producer arrow`).
    #[arg(long, default_value_t = 4)]
    encoders: usize,
    /// Arrow producer: target Parquet file size in MiB.
    #[arg(long, default_value_t = 1024)]
    file_mb: usize,
    /// Arrow producer: flush a row group once its estimated encoded size reaches this many MiB.
    #[arg(long, default_value_t = 256)]
    row_group_mb: usize,
    /// Arrow producer: multipart upload part size in MiB.
    #[arg(long, default_value_t = 32)]
    part_mb: usize,
    /// Arrow producer: upload parts in flight per encoder.
    #[arg(long, default_value_t = 4)]
    upload_concurrency: usize,
    /// Arrow producer: raw Arrow MiB of pre-generated batches shared by all encoders.
    #[arg(long, default_value_t = 512)]
    pool_mb: u64,
    /// Arrow producer: rows per pre-generated batch.
    #[arg(long, default_value_t = 32_768)]
    batch_rows: usize,
    #[arg(long, default_value_t = 100)]
    rows: usize,
    /// Upload data files as two-part multipart uploads (needed for --fault-crash-mid-upload).
    #[arg(long)]
    multipart: bool,
    /// Seconds of file production.
    #[arg(long, default_value_t = 60)]
    duration: u64,
    /// `group` or an interval such as `1s`, `5s`, `500ms`.
    #[arg(long, default_value = "group")]
    pacing: String,
    #[arg(long, value_enum, default_value_t = Idempotency::Txn)]
    idempotency: Idempotency,
    #[arg(long, default_value_t = 15)]
    library_max_retries: usize,
    #[arg(long, default_value_t = 50)]
    loop_max_attempts: u32,
    #[arg(long, value_enum, default_value_t = CheckpointMode::Separate)]
    checkpoint: CheckpointMode,
    #[arg(long, default_value_t = 100)]
    checkpoint_every: u64,
    #[arg(long, default_value_t = 10_000)]
    max_files_per_commit: usize,
    #[arg(long, default_value_t = 100)]
    scan_window: u64,
    /// Worker threads for commit loops (one committer core = 1).
    #[arg(long, default_value_t = 1)]
    commit_threads: usize,
    #[arg(long, value_enum, default_value_t = CommitRuntime::Multi)]
    commit_runtime: CommitRuntime,
    /// Where the separate checkpoint's kernel write runs (finding F14).
    #[arg(long, value_enum, default_value_t = CheckpointExec::Worker)]
    checkpoint_exec: CheckpointExec,
    /// Process-wide cap on concurrent table opens and commits. 0 = no cap.
    #[arg(long, default_value_t = 0)]
    max_ops: usize,
    /// Process-wide cap on concurrent separate checkpoints. 0 = no cap.
    #[arg(long, default_value_t = 0)]
    max_checkpoints: usize,
    /// Spread producer starts evenly over this many ms so tables reach checkpoint versions at
    /// different times. 0 = all start together.
    #[arg(long, default_value_t = 0)]
    start_jitter_ms: u64,
    /// Expired-log cleanup in the delta-rs post-commit hook. `default` follows the table property
    /// `delta.enableExpiredLogCleanup` (true unless set). The cleanup lists all of `_delta_log` on
    /// every commit, so it is off here; log cleanup belongs to table maintenance.
    #[arg(long, value_enum, default_value_t = LogCleanup::Off)]
    log_cleanup: LogCleanup,
    /// Rebuild the in-memory snapshot from each checkpoint the loop writes. `false` reproduces
    /// commit latency growing with total log length.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    reload_after_checkpoint: bool,
    /// JSONL output.
    #[arg(long)]
    out: PathBuf,
    /// Seconds between CPU and memory samples.
    #[arg(long, default_value_t = 5)]
    sample_secs: u64,
    #[command(flatten)]
    faults: FaultPlan,
}

#[derive(Args)]
struct CheckArgs {
    #[arg(long)]
    table_url: Url,
    /// WAL roots of every committer that wrote the table; repeatable.
    #[arg(long)]
    wal_dir: Vec<PathBuf>,
    /// Sent-file logs; repeatable.
    #[arg(long)]
    sent: Vec<PathBuf>,
    /// Print the full report including path lists.
    #[arg(long)]
    full: bool,
}

#[derive(Args)]
struct ReadArgs {
    #[arg(long)]
    table_url: Url,
    #[arg(long, default_value_t = 5)]
    repeat: usize,
}

#[derive(Args)]
struct OptimizeArgs {
    #[arg(long)]
    table_url: Url,
    /// Files removed per compaction commit.
    #[arg(long, default_value_t = 10)]
    files: usize,
    #[arg(long, default_value_t = 2000)]
    every_ms: u64,
    #[arg(long, default_value_t = 60)]
    duration: u64,
    #[arg(long)]
    out: PathBuf,
}

fn parse_props(props: &[String]) -> Result<Vec<(String, String)>> {
    props
        .iter()
        .map(|p| {
            p.split_once('=')
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .context("prop must be key=value")
        })
        .collect()
}

fn parse_pacing(s: &str) -> Result<Pacing> {
    if s == "group" {
        return Ok(Pacing::Group);
    }
    let duration = if let Some(ms) = s.strip_suffix("ms") {
        Duration::from_millis(ms.parse()?)
    } else if let Some(secs) = s.strip_suffix('s') {
        Duration::from_secs_f64(secs.parse()?)
    } else {
        bail!("pacing must be group, <n>s, or <n>ms")
    };
    Ok(Pacing::Interval(duration))
}

async fn delayed<F: std::future::Future>(delay: Duration, f: F) -> F::Output {
    tokio::time::sleep(delay).await;
    f.await
}

fn table_name(url: &Url) -> String {
    url.path_segments()
        .and_then(|mut s| s.next_back().map(str::to_owned))
        .unwrap_or_default()
}

fn table_root(url: &Url) -> Result<Path> {
    Ok(Path::from_url_path(url.path())?)
}

#[derive(Serialize)]
struct Sample {
    kind: &'static str,
    ts_ms: i64,
    cpu_secs: BTreeMap<String, f64>,
    /// Live threads by the same name groups as `cpu_secs`.
    threads: BTreeMap<String, usize>,
    rss_mb: f64,
    producer_storage: store::CounterSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    producer: Option<arrow_producer::StatsSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wal_sync: Option<WalSample>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wal_disk: Option<wal::DiskStats>,
}

#[derive(Serialize)]
struct WalSample {
    #[serde(flatten)]
    totals: wal::SyncStats,
    /// Sync latency percentiles over the sample period.
    p50_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

fn wal_sample(log: &wal::GroupLog) -> WalSample {
    let totals = log.stats();
    let mut l = totals.latencies.clone();
    l.sort_by(f64::total_cmp);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let pct = |p: f64| {
        l.get(((l.len() as f64 * p) as usize).min(l.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0.0)
    };
    WalSample {
        p50_ms: pct(0.5),
        p99_ms: pct(0.99),
        max_ms: l.last().copied().unwrap_or(0.0),
        totals,
    }
}

/// CPU seconds by thread-name group from /proc (Linux only; empty elsewhere).
fn thread_cpu() -> BTreeMap<String, f64> {
    thread_stats().0
}

/// CPU seconds and live thread count by thread-name group.
fn thread_stats() -> (BTreeMap<String, f64>, BTreeMap<String, usize>) {
    let mut out = BTreeMap::new();
    let mut counts = BTreeMap::new();
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return (out, counts);
    };
    for task in tasks.flatten() {
        let Ok(stat) = std::fs::read_to_string(task.path().join("stat")) else {
            continue;
        };
        let (Some(open), Some(close)) = (stat.find('('), stat.rfind(')')) else {
            continue;
        };
        let comm = &stat[open + 1..close];
        let fields: Vec<&str> = stat[close + 2..].split_whitespace().collect();
        let ticks: f64 = fields.get(11).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0)
            + fields.get(12).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
        let group = if comm.starts_with("encoder") {
            "encoder"
        } else if comm.starts_with("commit-blk") {
            "commit_blocking"
        } else if comm.starts_with("commit") {
            "commit"
        } else if comm.starts_with("prod") || comm.starts_with("tokio") {
            "producer"
        } else {
            "other"
        };
        *out.entry(group.to_owned()).or_insert(0.0) += ticks / 100.0;
        *counts.entry(group.to_owned()).or_insert(0) += 1;
    }
    (out, counts)
}

fn rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse::<f64>().ok()))
        })
        .map_or(0.0, |kb| kb / 1024.0)
}

async fn create_tables(urls: &[Url], props: &[(String, String)], schema: table::TableSchema) -> Result<()> {
    let root = table::root_store(&urls[0])?;
    let results: Vec<Result<()>> = futures::stream::iter(urls.iter().cloned())
        .map(|url| {
            let root = Arc::clone(&root);
            let props = props.to_vec();
            async move {
                match table::create(&url, root, &props, schema).await {
                    Ok(_) => Ok(()),
                    Err(error) if error.to_string().contains("already exists") => Ok(()),
                    Err(error) => Err(error.context(format!("creating {url}"))),
                }
            }
        })
        .buffer_unordered(32)
        .collect()
        .await;
    results.into_iter().collect()
}

async fn run(args: RunArgs) -> Result<()> {
    let urls = args.tables.urls()?;
    let props = parse_props(&args.props)?;
    if args.create {
        let schema = match args.producer {
            ProducerKind::Small => table::TableSchema::Small,
            ProducerKind::Arrow => table::TableSchema::Logs,
        };
        create_tables(&urls, &props, schema).await?;
    }
    let pacing = parse_pacing(&args.pacing)?;
    let root = table::root_store(&urls[0])?;
    let faults = Arc::new(args.faults.clone());
    std::fs::create_dir_all(&args.wal_dir)?;
    let group_log = (args.wal_sync == WalSync::Group)
        .then(|| {
            wal::init_group(
                &args.wal_dir,
                Duration::from_millis(args.group_sync_ms),
                args.data_wal_mbps,
            )
        })
        .transpose()?;
    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let sent_path = args.out.with_extension("sent.jsonl");
    let sent: producer::SentLog = Arc::new(Mutex::new(
        std::fs::OpenOptions::new().create(true).append(true).open(&sent_path)?,
    ));

    let arrow = if args.producer == ProducerKind::Arrow {
        let (pool, info) = arrow_producer::BatchPool::generate(args.pool_mb << 20, args.batch_rows.max(1))?;
        eprintln!("batch pool: {info:?}");
        Some((Arc::new(pool), info, Arc::new(arrow_producer::ProducerStats::default())))
    } else {
        None
    };

    let (record_tx, mut record_rx) = mpsc::unbounded_channel::<CommitRecord>();
    let mut out = std::io::BufWriter::new(std::fs::OpenOptions::new().create(true).append(true).open(&args.out)?);
    let mut run_record = serde_json::json!({"kind": "run", "ts_ms": now_millis(), "tables": urls.len(), "args": &args});
    if let Some((_, info, _)) = &arrow {
        run_record["pool"] = serde_json::to_value(info)?;
    }
    writeln!(out, "{run_record}")?;
    out.flush()?;
    let (sample_tx, mut sample_rx) = mpsc::unbounded_channel::<Sample>();
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(record) = record_rx.recv() => {
                    if let Ok(line) = serde_json::to_string(&record) { let _ = writeln!(out, "{line}"); }
                    let _ = out.flush();
                }
                Some(sample) = sample_rx.recv() => {
                    if let Ok(line) = serde_json::to_string(&sample) { let _ = writeln!(out, "{line}"); }
                    let _ = out.flush();
                }
                else => break,
            }
        }
        let _ = out.flush();
    });

    let commit_rt = if args.commit_runtime == CommitRuntime::Current {
        tokio::runtime::Builder::new_current_thread()
            .thread_name("commit-blk")
            .enable_all()
            .build()?
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(args.commit_threads.max(1))
            .thread_name_fn(|| "commit".to_owned())
            .enable_all()
            .build()?
    };
    let commit_handle = commit_rt.handle().clone();
    // A current-thread runtime only polls while some thread drives it; give it a dedicated
    // thread named `commit` so its CPU is attributable.
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let driver = std::thread::Builder::new().name("commit".to_owned()).spawn(move || {
        commit_rt.block_on(async {
            let _ = stop_rx.await;
        });
        commit_rt.shutdown_background();
    })?;

    let producer_counters = Arc::new(Counters::default());
    let producer_store: Arc<dyn ObjectStore> = Arc::new(InstrumentedStore::new(
        Arc::clone(&root),
        Arc::clone(&producer_counters),
        Arc::new(FaultPlan {
            fault_crash_mid_upload: args.faults.fault_crash_mid_upload,
            ..FaultPlan::default()
        }),
    ));

    if args.adopt_from.is_some() && urls.len() != 1 {
        bail!("--adopt-from needs exactly one table");
    }
    let mut adopt = args.adopt_from.clone();

    let limits = Limits {
        ops: (args.max_ops > 0).then(|| Arc::new(tokio::sync::Semaphore::new(args.max_ops))),
        checkpoints: (args.max_checkpoints > 0).then(|| Arc::new(tokio::sync::Semaphore::new(args.max_checkpoints))),
    };
    let mut loops = Vec::new();
    let mut producers = Vec::new();
    let started = Instant::now();
    for (index, url) in urls.iter().enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        let start_delay = Duration::from_millis(args.start_jitter_ms * index as u64 / urls.len() as u64);
        let name = table_name(url);
        let app_id = format!("{}/{name}", args.app_id);
        let (tx, rx) = mpsc::channel(100_000);
        let cfg = LoopConfig {
            app_id: app_id.clone(),
            table_name: name.clone(),
            pacing,
            idempotency: args.idempotency,
            library_max_retries: args.library_max_retries,
            loop_max_attempts: args.loop_max_attempts,
            inline_checkpoint: matches!(args.checkpoint, CheckpointMode::Inline),
            cleanup_expired_logs: match args.log_cleanup {
                LogCleanup::Default => None,
                LogCleanup::On => Some(true),
                LogCleanup::Off => Some(false),
            },
            reload_after_checkpoint: args.reload_after_checkpoint,
            separate_checkpoint_every: matches!(args.checkpoint, CheckpointMode::Separate)
                .then_some(args.checkpoint_every),
            checkpoint_exec: args.checkpoint_exec,
            limits: limits.clone(),
            max_files_per_commit: args.max_files_per_commit,
            scan_window: args.scan_window,
            epochs: args.epochs,
            fence_on: if args.infer_fence {
                FenceOn::Inferred
            } else if args.fence_identity.is_empty() {
                FenceOn::None
            } else {
                FenceOn::Explicit(args.fence_identity.clone())
            },
            drain_only: args.drain_only,
            crash_before_claim: args.faults.fault_crash_before_claim,
            crash_after_claim: args.faults.fault_crash_after_claim,
        };
        wal::Wal::open(&args.wal_dir, &app_id)?;
        let wal_dir = args.wal_dir.clone();
        let url_c = url.clone();
        let root_c = Arc::clone(&root);
        let faults_c = Arc::clone(&faults);
        let record_tx = record_tx.clone();
        let adopt_app_id = adopt.take().map(|old| format!("{old}/{name}"));
        let lib = args.lib;
        loops.push(commit_handle.spawn(async move {
            let mut delay = Duration::from_millis(100);
            let mut attempt = 0;
            let lp = loop {
                attempt += 1;
                let (permit, _) = commit_loop::acquire(cfg.limits.ops.as_ref()).await;
                let (table, counters) = match lib {
                    backend::Lib::Dl1 => {
                        let bench = table::open_with_retry(&url_c, Arc::clone(&root_c), Arc::clone(&faults_c)).await?;
                        (backend::Backend::Dl1(bench.table), bench.counters)
                    }
                    backend::Lib::Kernel => {
                        backend::open_kernel_with_retry(&url_c, Arc::clone(&root_c), Arc::clone(&faults_c)).await?
                    }
                };
                let wal = wal::Wal::open(&wal_dir, &cfg.app_id)?;
                let old = adopt_app_id
                    .as_ref()
                    .map(|id| {
                        Ok::<_, anyhow::Error>(Adopt {
                            wal: wal::Wal::open(&wal_dir, id)?,
                            app_id: id.clone(),
                        })
                    })
                    .transpose()?;
                let started = CommitLoop::start(table, counters, wal, cfg.clone(), old, record_tx.clone()).await;
                drop(permit);
                match started {
                    Ok(lp) => break lp,
                    Err(error) if attempt < 30 => {
                        tracing::warn!(%error, attempt, "recovery failed; retrying");
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(5));
                    }
                    Err(error) => return Err(error),
                }
            };
            drop(record_tx);
            lp.run(rx).await
        }));
        if let Some((pool, _, stats)) = &arrow {
            let acfg = arrow_producer::ArrowConfig {
                table_name: name,
                encoders: args.encoders,
                rate: args.rate,
                duration: Duration::from_secs(args.duration),
                file_bytes: args.file_mb << 20,
                row_group_bytes: args.row_group_mb << 20,
                part_bytes: (args.part_mb << 20).max(1),
                upload_concurrency: args.upload_concurrency,
            };
            producers.push(tokio::spawn(delayed(
                start_delay,
                arrow_producer::produce(
                    Arc::clone(&producer_store),
                    table_root(url)?,
                    acfg,
                    Arc::clone(pool),
                    tx,
                    Some(Arc::clone(&sent)),
                    Arc::clone(stats),
                ),
            )));
            continue;
        }
        if args.drain_only {
            drop(tx);
            continue;
        }
        let pcfg = producer::ProducerConfig {
            table_name: name,
            encoders: args.encoders,
            rate: args.rate,
            rows: args.rows,
            multipart: args.multipart,
            duration: Duration::from_secs(args.duration),
            route: args.route_file.clone().map(|file| (file, args.app_id.clone())),
        };
        producers.push(tokio::spawn(delayed(
            start_delay,
            producer::produce(
                Arc::clone(&producer_store),
                table_root(url)?,
                pcfg,
                tx,
                Some(Arc::clone(&sent)),
            ),
        )));
    }
    drop(record_tx);

    let producer_stats = arrow.as_ref().map(|(_, _, stats)| Arc::clone(stats));
    let start_stats = producer_stats.as_ref().map(|s| s.snapshot());
    let sampler = {
        let counters = Arc::clone(&producer_counters);
        let producer_stats = producer_stats.clone();
        let group_log = group_log.clone();
        let wal_dir = args.wal_dir.clone();
        let period = Duration::from_secs(args.sample_secs.max(1));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            loop {
                tick.tick().await;
                let (cpu_secs, threads) = thread_stats();
                if sample_tx
                    .send(Sample {
                        kind: "sample",
                        ts_ms: now_millis(),
                        cpu_secs,
                        threads,
                        rss_mb: rss_mb(),
                        producer_storage: counters.snapshot(),
                        producer: producer_stats.as_ref().map(|s| s.snapshot()),
                        wal_sync: group_log.as_deref().map(wal_sample),
                        wal_disk: wal::disk_stats(&wal_dir),
                    })
                    .is_err()
                {
                    break;
                }
            }
        })
    };

    let mut produced = 0;
    for p in producers {
        produced += p.await??;
    }
    let produce_s = started.elapsed().as_secs_f64();
    let mut loop_errors = Vec::new();
    for l in loops {
        if let Err(error) = l.await? {
            loop_errors.push(error.to_string());
        }
    }
    let elapsed = started.elapsed();
    sampler.abort();
    let _ = stop_tx.send(());
    let _ = driver.join();
    let _ = writer.await;
    let mut out = std::fs::OpenOptions::new().append(true).open(&args.out)?;
    let mut end = serde_json::json!({
        "kind": "end", "ts_ms": now_millis(), "elapsed_s": elapsed.as_secs_f64(),
        "files_produced": produced, "loop_errors": loop_errors,
        "cpu_secs": thread_cpu(), "rss_mb": rss_mb(),
    });
    if let Some(stats) = &producer_stats {
        let snap = stats.snapshot();
        end["producer"] = serde_json::to_value(&snap)?;
        end["producer"]["produce_s"] = produce_s.into();
        end["producer"]["encoders"] = args.encoders.into();
        end["producer"]["nic_tx_delta_bytes"] = start_stats
            .and_then(|s| Some(snap.nic_tx_bytes? - s.nic_tx_bytes?))
            .into();
    }
    writeln!(out, "{end}")?;
    eprintln!(
        "done: {produced} files in {:.1}s, loop errors: {}",
        elapsed.as_secs_f64(),
        loop_errors.len()
    );
    if !loop_errors.is_empty() {
        eprintln!("{loop_errors:#?}");
        bail!("{} commit loop(s) failed", loop_errors.len());
    }
    Ok(())
}

async fn read(args: ReadArgs) -> Result<()> {
    for i in 0..args.repeat {
        let root = table::root_store(&args.table_url)?;
        let started = Instant::now();
        let bench = table::open(&args.table_url, root, Arc::new(FaultPlan::default())).await?;
        let load_ms = started.elapsed().as_secs_f64() * 1000.0;
        let files = bench.table.get_file_uris()?.count();
        println!(
            "{}",
            serde_json::json!({
                "kind": "read", "iteration": i, "load_ms": load_ms, "version": bench.table.version(),
                "files": files, "storage": bench.counters.snapshot(),
            })
        );
    }
    Ok(())
}

async fn checkpoint(args: TableArg) -> Result<()> {
    let root = table::root_store(&args.table_url)?;
    let bench = table::open(&args.table_url, Arc::clone(&root), Arc::new(FaultPlan::default())).await?;
    let before = bench.counters.snapshot();
    let started = Instant::now();
    deltalake_core::checkpoints::create_checkpoint(&bench.table, None).await?;
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    let log = Path::from(format!("{}/_delta_log", table_root(&args.table_url)?.as_ref()));
    let version = bench.table.version().unwrap_or(0);
    let name = format!("{version:020}.checkpoint.parquet");
    let size = root
        .head(&Path::from(format!("{}/{name}", log.as_ref())))
        .await
        .map(|m| m.size)
        .ok();
    println!(
        "{}",
        serde_json::json!({
            "kind": "checkpoint", "version": version, "ms": ms, "bytes": size,
            "files": bench.table.get_file_uris()?.count(),
            "storage": bench.counters.snapshot().since(&before),
        })
    );
    Ok(())
}

async fn optimize(args: OptimizeArgs) -> Result<()> {
    let root = table::root_store(&args.table_url)?;
    let mut bench = table::open(&args.table_url, Arc::clone(&root), Arc::new(FaultPlan::default())).await?;
    let prefix = table_root(&args.table_url)?;
    let mut out = std::fs::OpenOptions::new().create(true).append(true).open(&args.out)?;
    let deadline = Instant::now() + Duration::from_secs(args.duration);
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(args.every_ms)).await;
        bench.table.update_incremental(None).await?;
        let picked: Vec<String> = table::active_paths(&bench.table)?
            .into_iter()
            .filter(|p| !p.starts_with("data/optimize-"))
            .take(args.files)
            .collect();
        if picked.len() < 2 {
            continue;
        }
        let bytes = table::small_parquet(-1, 10, "optimize")?;
        let rel = format!("data/optimize-{}.parquet", uuid::Uuid::new_v4());
        let size = i64::try_from(bytes.len())?;
        root.put(
            &Path::from(format!("{}/{rel}", prefix.as_ref())),
            PutPayload::from(bytes),
        )
        .await?;
        let now = now_millis();
        let mut actions: Vec<Action> = picked
            .iter()
            .map(|path| {
                Action::Remove(Remove {
                    path: path.clone(),
                    data_change: false,
                    deletion_timestamp: Some(now),
                    extended_file_metadata: None,
                    partition_values: None,
                    size: None,
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                })
            })
            .collect();
        actions.push(Action::Add(Add {
            path: rel,
            partition_values: Default::default(),
            size,
            modification_time: now,
            data_change: false,
            stats: Some("{\"numRecords\":10}".to_owned()),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        }));
        let started = Instant::now();
        let result = CommitBuilder::from(CommitProperties::default().with_create_checkpoint(false))
            .with_actions(actions)
            .build(
                Some(bench.table.snapshot()?),
                bench.table.log_store(),
                DeltaOperation::Optimize {
                    predicate: None,
                    target_size: 0,
                },
            )
            .await;
        let (outcome, retries, error) = match result {
            Ok(fc) => {
                let r = fc.metrics.num_retries;
                bench.table.state = Some(fc.snapshot());
                ("ok", r, None)
            }
            Err(e) => ("error", 0, Some(e.to_string())),
        };
        writeln!(
            out,
            "{}",
            serde_json::json!({"kind": "optimize", "ts_ms": now_millis(), "removed": picked.len(),
                "ms": started.elapsed().as_secs_f64() * 1000.0, "outcome": outcome, "retries": retries, "error": error})
        )?;
    }
    Ok(())
}

async fn seed_txn(args: SeedTxnArgs) -> Result<()> {
    let root = table::root_store(&args.table_url)?;
    let mut bench = table::open(&args.table_url, root, Arc::new(FaultPlan::default())).await?;
    let started = Instant::now();
    let mut commits = 0;
    for chunk in (0..args.count).collect::<Vec<_>>().chunks(args.per_commit.max(1)) {
        let txns = chunk
            .iter()
            .map(|i| {
                deltalake_core::kernel::Transaction::new_with_last_update(
                    format!("{}-{i}", args.prefix),
                    1,
                    Some(now_millis()),
                )
            })
            .collect();
        let props = CommitProperties::default()
            .with_application_transactions(txns)
            .with_create_checkpoint(false);
        let finalized = CommitBuilder::from(props)
            .with_actions(Vec::new())
            .build(
                Some(bench.table.snapshot()?),
                bench.table.log_store(),
                DeltaOperation::Write {
                    mode: deltalake_core::protocol::SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await?;
        bench.table.state = Some(finalized.snapshot());
        commits += 1;
    }
    println!(
        "{}",
        serde_json::json!({"kind": "seed_txn", "app_ids": args.count, "commits": commits,
            "ms": started.elapsed().as_secs_f64() * 1000.0, "version": bench.table.version()})
    );
    Ok(())
}

async fn txn_probe(args: TxnProbeArgs) -> Result<()> {
    let root = table::root_store(&args.table_url)?;
    let bench = table::open(&args.table_url, root, Arc::new(FaultPlan::default())).await?;
    let state = bench.table.snapshot()?;
    let mut out = serde_json::Map::new();
    for app_id in &args.app_id {
        let v = state
            .transaction_version(bench.table.log_store().as_ref(), app_id)
            .await?;
        out.insert(
            app_id.clone(),
            serde_json::json!({"version": v, "fenced": v.map(|v| v >= commit_loop::FENCE_OFFSET)}),
        );
    }
    println!(
        "{}",
        serde_json::json!({"kind": "txn_probe", "table_version": bench.table.version(), "txns": out,
            "retention": state.metadata().configuration().get("delta.setTransactionRetentionDuration")})
    );
    Ok(())
}

async fn cleanup_log(args: TableArg) -> Result<()> {
    let root = table::root_store(&args.table_url)?;
    let bench = table::open(&args.table_url, root, Arc::new(FaultPlan::default())).await?;
    deltalake_core::checkpoints::create_checkpoint(&bench.table, None).await?;
    let version = bench.table.version().context("table has no version")?;
    let deleted = deltalake_core::checkpoints::cleanup_expired_logs_for(
        version,
        bench.table.log_store().as_ref(),
        now_millis() + 60_000,
        None,
    )
    .await?;
    println!(
        "{}",
        serde_json::json!({"kind": "cleanup_log", "checkpoint_version": version, "deleted": deleted})
    );
    Ok(())
}

async fn vacuum(args: TableArg) -> Result<()> {
    let root = table::root_store(&args.table_url)?;
    let bench = table::open(&args.table_url, root, Arc::new(FaultPlan::default())).await?;
    let (_, metrics) = bench
        .table
        .vacuum()
        .with_retention_period(chrono::Duration::zero())
        .with_enforce_retention_duration(false)
        .await?;
    println!(
        "{}",
        serde_json::json!({"kind": "vacuum", "deleted": metrics.files_deleted.len()})
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
        Command::Create(args) => {
            create_tables(
                &args.tables.urls()?,
                &parse_props(&args.props)?,
                table::TableSchema::Small,
            )
            .await
        }
        Command::Run(args) => run(*args).await,
        Command::Check(args) => {
            let report = checker::check(&args.table_url, &args.wal_dir, &args.sent).await?;
            let mut value = serde_json::to_value(&report)?;
            if !args.full {
                for key in [
                    "missing_accepted",
                    "duplicate_paths",
                    "split_manifests",
                    "dangling_files",
                ] {
                    if let Some(list) = value.get(key).and_then(|v| v.as_array()).map(Vec::len) {
                        value[format!("{key}_count")] = list.into();
                        if let Some(arr) = value.get_mut(key).and_then(|v| v.as_array_mut()) {
                            arr.truncate(5);
                        }
                    }
                }
            }
            println!("{}", serde_json::to_string_pretty(&value)?);
            Ok(())
        }
        Command::Read(args) => read(args).await,
        Command::Checkpoint(args) => checkpoint(args).await,
        Command::Optimize(args) => optimize(args).await,
        Command::SeedTxn(args) => seed_txn(args).await,
        Command::TxnProbe(args) => txn_probe(args).await,
        Command::CleanupLog(args) => cleanup_log(args).await,
        Command::Vacuum(args) => vacuum(args).await,
    }
}
