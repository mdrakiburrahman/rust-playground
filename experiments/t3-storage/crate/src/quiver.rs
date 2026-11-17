use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, ensure};
use arrow_array::{Array, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use otel_arrow_dfe_quiver::record_bundle::{BundleDescriptor, PayloadRef, RecordBundle, SlotDescriptor, SlotId};
use otel_arrow_dfe_quiver::{DiskBudget, QuiverConfig, QuiverEngine, RetentionPolicy, SegmentReadMode, SubscriberId};
use serde_json::json;

use crate::{Oracle, Samples, compare, cpu_seconds, read_receipts};

const SUBSCRIBER: &str = "storage-spike-reader";

struct Bundle {
    descriptor: BundleDescriptor,
    batch: RecordBatch,
}

impl Bundle {
    fn new(id: i64, data: Arc<BinaryArray>) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("payload", DataType::Binary, false),
        ]));
        Ok(Self {
            descriptor: BundleDescriptor::new(vec![SlotDescriptor::new(SlotId::new(0), "Logs")]),
            batch: RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id])), data])?,
        })
    }
}

impl RecordBundle for Bundle {
    fn descriptor(&self) -> &BundleDescriptor {
        &self.descriptor
    }
    fn ingestion_time(&self) -> SystemTime {
        SystemTime::now()
    }
    fn payload(&self, slot: SlotId) -> Option<PayloadRef<'_>> {
        (slot == SlotId::new(0)).then_some(PayloadRef {
            schema_fingerprint: [17; 32],
            batch: &self.batch,
        })
    }
    fn item_count(&self) -> u64 {
        1
    }
    fn byte_count(&self) -> Option<u64> {
        Some(
            self.batch
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()?
                .value(0)
                .len() as u64
                + 8,
        )
    }
}

pub fn config(dir: &Path, interval_ms: u64, mmap: bool) -> QuiverConfig {
    let mut config = QuiverConfig {
        data_dir: dir.into(),
        ..Default::default()
    };
    config.wal.flush_interval = Duration::from_millis(interval_ms);
    config.read_mode = if mmap {
        SegmentReadMode::Mmap
    } else {
        SegmentReadMode::Standard
    };
    config.ownership.heartbeat_interval = Duration::from_secs(1);
    config.ownership.stale_after = Duration::from_secs(5);
    config
}

async fn open(dir: &Path, interval_ms: u64, cap_bytes: u64, mmap: bool, retry: Duration) -> Result<Arc<QuiverEngine>> {
    let start = Instant::now();
    let mut reported_owned = false;
    loop {
        let config = config(dir, interval_ms, mmap);
        let budget = DiskBudget::for_config(cap_bytes, &config, RetentionPolicy::Backpressure)?;
        match QuiverEngine::builder(config)
            .with_budget(Arc::new(budget))
            .build()
            .await
        {
            Ok(engine) => return Ok(engine),
            Err(error)
                if matches!(error, otel_arrow_dfe_quiver::QuiverError::DataDirOwned { .. })
                    && start.elapsed() < retry =>
            {
                if !reported_owned {
                    eprintln!(
                        "{}",
                        json!({"kind":"ownership-refused", "at_unix_ms":SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs_f64() * 1000.0})
                    );
                    reported_owned = true;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn subscribe(engine: &QuiverEngine) -> Result<SubscriberId> {
    let id = SubscriberId::new(SUBSCRIBER)?;
    engine.register_subscriber(id.clone())?;
    engine.activate_subscriber(&id)?;
    Ok(id)
}

fn drain(engine: &Arc<QuiverEngine>, subscriber: &SubscriberId, ids: &mut Vec<i64>) -> Result<()> {
    while let Some(handle) = engine.poll_next_bundle(subscriber)? {
        let batch = handle.data().payload(SlotId::new(0)).context("missing slot")?;
        let values = batch
            .column_by_name("value")
            .context("missing IDs")?
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("invalid ID type")?;
        ensure!(values.null_count() == 0, "null recovered ID");
        let data = batch
            .column_by_name("payload")
            .context("missing payload")?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context("invalid payload type")?;
        ensure!(
            data.null_count() == 0 && data.len() == values.len(),
            "invalid recovered payload count"
        );
        for i in 0..values.len() {
            ensure!(
                !data.value(i).is_empty() && data.value(i).iter().all(|b| *b == 0x5a),
                "corrupt recovered payload"
            );
            ids.push(values.value(i));
        }
        handle.ack();
    }
    Ok(())
}

#[derive(clap::Args)]
pub struct WriteArgs {
    #[arg(long)]
    pub dir: PathBuf,
    #[arg(long)]
    pub run: String,
    #[arg(long, default_value_t = 5)]
    pub interval_ms: u64,
    #[arg(long, default_value_t = 1_048_576)]
    pub bundle_bytes: usize,
    #[arg(long, default_value_t = 30)]
    pub seconds: u64,
    #[arg(long, default_value_t = 137_438_953_472)]
    pub cap_bytes: u64,
    #[arg(long)]
    pub oracle: Option<PathBuf>,
    #[arg(long)]
    pub drain: bool,
    #[arg(long)]
    pub mmap: bool,
    #[arg(long)]
    pub barrier: Option<PathBuf>,
    #[arg(long, default_value_t = 0)]
    pub fence_samples: u64,
    #[arg(long, default_value_t = 0)]
    pub warmup_seconds: u64,
    #[arg(long)]
    pub barrier_before_ingest: bool,
}

async fn wait_at_barrier(path: &Path) -> Result<()> {
    ensure!(
        !path.exists() && !path.with_extension("ready").exists(),
        "barrier paths must be new"
    );
    std::fs::write(path.with_extension("ready"), b"paused\n")?;
    while !path.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

pub async fn write(args: WriteArgs) -> Result<()> {
    ensure!(
        args.bundle_bytes > 8 && args.bundle_bytes <= 16 * 1024 * 1024,
        "bundle size must be 9 bytes to 16 MiB"
    );
    ensure!(
        args.seconds > 0 && args.interval_ms <= 1000,
        "invalid run duration or interval"
    );
    ensure!(!args.run.is_empty(), "run identity required");
    ensure!(!args.dir.exists(), "writer directory must be new");
    let engine = open(&args.dir, args.interval_ms, args.cap_bytes, args.mmap, Duration::ZERO).await?;
    let subscriber = subscribe(&engine)?;
    if args.fence_samples > 0 || args.warmup_seconds > 0 {
        ensure!(args.fence_samples <= 100_000, "too many fence probes");
        ensure!(args.warmup_seconds <= 120, "warmup exceeds two minutes");
        let generation = std::fs::read_dir(&args.dir)?
            .filter_map(|entry| entry.ok())
            .find(|entry| entry.file_name().to_string_lossy().starts_with("quiver.gen."))
            .context("generation file not found")?
            .path();
        let warmup = Instant::now();
        let mut next_maintenance = Instant::now();
        while warmup.elapsed() < Duration::from_secs(args.warmup_seconds) {
            std::fs::symlink_metadata(&generation)?;
            if Instant::now() >= next_maintenance {
                engine.maintain().await?;
                next_maintenance = Instant::now() + Duration::from_millis(100);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut probes = Samples::default();
        let mut maintenance = Instant::now();
        for _ in 0..args.fence_samples {
            let before = Instant::now();
            std::fs::symlink_metadata(&generation)?;
            probes.record(before.elapsed());
            if maintenance.elapsed() >= Duration::from_millis(100) {
                engine.maintain().await?;
                maintenance = Instant::now();
            }
        }
        println!("{}", json!({"kind":"fence-stat-proxy", "latency":probes.summary()}));
    }
    let mut oracle = args
        .oracle
        .as_ref()
        .map(|p| Oracle::create(p, args.run.clone()))
        .transpose()?;
    let bytes = vec![0x5a; args.bundle_bytes - 8];
    let payload = Arc::new(BinaryArray::from_iter_values([bytes.as_slice()]));
    let mut wait = Samples::default();
    let mut end_to_end = Samples::default();
    let mut ingest = Samples::default();
    let mut sync = Samples::default();
    let start = Instant::now();
    let cpu_start = cpu_seconds();
    let mut next_maintenance = start;
    let mut group_start = start;
    let mut pending = Vec::new();
    let mut sequence = 0_i64;
    let mut groups = 0_u64;
    let mut barrier_used = false;
    println!("{}", json!({"kind":"ready", "run":args.run, "pid":std::process::id()}));
    while start.elapsed() < Duration::from_secs(args.seconds) || !pending.is_empty() {
        if groups >= 5
            && pending.is_empty()
            && !barrier_used
            && args.barrier_before_ingest
            && let Some(path) = &args.barrier
        {
            wait_at_barrier(path).await?;
            barrier_used = true;
        }
        if start.elapsed() < Duration::from_secs(args.seconds) {
            let bundle = Bundle::new(sequence, payload.clone())?;
            let before = Instant::now();
            engine.ingest(&bundle).await?;
            let after = Instant::now();
            ingest.record(after - before);
            pending.push((sequence, before, after));
            sequence += 1;
        }
        if groups >= 5
            && !pending.is_empty()
            && !barrier_used
            && !args.barrier_before_ingest
            && let Some(path) = &args.barrier
        {
            wait_at_barrier(path).await?;
            barrier_used = true;
        }
        if !pending.is_empty()
            && (group_start.elapsed() >= Duration::from_millis(args.interval_ms)
                || start.elapsed() >= Duration::from_secs(args.seconds))
        {
            let before = Instant::now();
            engine.sync_wal().await?;
            let after = Instant::now();
            sync.record(after - before);
            if let Some(oracle) = &mut oracle {
                oracle.record(pending.iter().map(|p| p.0).collect())?;
            }
            for (_, begin, appended) in pending.drain(..) {
                wait.record(after - appended);
                end_to_end.record(after - begin);
            }
            group_start = Instant::now();
            groups += 1;
            if args.drain {
                drain(&engine, &subscriber, &mut Vec::new())?;
            }
        }
        if Instant::now() >= next_maintenance {
            engine.maintain().await?;
            next_maintenance = Instant::now() + Duration::from_millis(100);
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let logical = sequence as u64 * args.bundle_bytes as u64;
    let cpu = cpu_seconds().zip(cpu_start).map(|(end, begin)| end - begin);
    let result = json!({
        "kind":"summary", "backend":"quiver", "run":args.run,
        "seconds":elapsed, "bundles":sequence, "logical_bytes":logical,
        "mib_per_second":logical as f64 / 1_048_576.0 / elapsed,
        "bundles_per_second":sequence as f64 / elapsed,
        "cpu_seconds":cpu, "cpu_seconds_per_gib":cpu.map(|c| c / (logical as f64 / 1_073_741_824.0)),
        "read_mode":if args.mmap {"mmap"} else {"standard"},
        "interval_ms":args.interval_ms, "bundle_bytes":args.bundle_bytes,
        "wal_bytes_written":engine.wal_bytes_written(), "segment_bytes_written":engine.segment_bytes_written(),
        "ingest":ingest.summary(), "ack_after_ingest":wait.summary(),
        "ack_end_to_end":end_to_end.summary(), "backend_sync":sync.summary()
    });
    engine.shutdown().await?;
    println!("{result}");
    Ok(())
}

#[derive(clap::Args)]
pub struct RecoverArgs {
    #[arg(long)]
    pub dir: PathBuf,
    #[arg(long)]
    pub run: String,
    #[arg(long)]
    pub oracle: PathBuf,
    #[arg(long)]
    pub observed: PathBuf,
    #[arg(long, default_value_t = 120)]
    pub retry_seconds: u64,
    #[arg(long)]
    pub mmap: bool,
    #[arg(long, default_value_t = 0)]
    pub hold_open_seconds: u64,
    #[arg(long)]
    pub hold_open_until: Option<PathBuf>,
}

pub async fn recover(args: RecoverArgs) -> Result<()> {
    ensure!(args.dir.is_dir(), "recovery directory does not exist");
    let receipts = read_receipts(&std::fs::read(&args.oracle)?)?;
    let start = Instant::now();
    let engine = open(
        &args.dir,
        5,
        137_438_953_472,
        args.mmap,
        Duration::from_secs(args.retry_seconds),
    )
    .await?;
    let claim_ms = start.elapsed().as_secs_f64() * 1000.0;
    println!(
        "{}",
        json!({"kind":"claim", "run":args.run, "open_retry_ms":claim_ms, "at_unix_ms":SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs_f64() * 1000.0})
    );
    let subscriber = subscribe(&engine)?;
    engine.flush().await?;
    let mut ids = Vec::new();
    drain(&engine, &subscriber, &mut ids)?;
    let mut file = OpenOptions::new().create_new(true).write(true).open(&args.observed)?;
    serde_json::to_writer(&mut file, &ids)?;
    file.sync_all()?;
    let mut result = compare(&receipts, &ids, &args.run)?;
    result["claim_ms"] = json!(claim_ms);
    result["read_mode"] = json!(if args.mmap { "mmap" } else { "standard" });
    result["kind"] = json!("recovery");
    println!("{result}");
    ensure!(
        result["passed"] == true,
        "ACKNOWLEDGED LOSS: stop the experiment and preserve evidence"
    );
    engine.flush_progress().await?;
    engine.cleanup_completed_segments()?;
    let hold = Instant::now();
    while hold.elapsed() < Duration::from_secs(args.hold_open_seconds)
        && !args.hold_open_until.as_ref().is_some_and(|path| path.exists())
    {
        engine.maintain().await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    engine.shutdown().await?;
    Ok(())
}
