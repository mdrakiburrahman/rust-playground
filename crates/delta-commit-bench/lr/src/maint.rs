//! Q6: maintenance commits written next to running appends. The commit is written directly with
//! put-if-absent, as any maintenance engine would, so the appender sees a real concurrent commit.

use crate::schema::StatsMode;
use crate::store;
use crate::synth::Rows;
use anyhow::{Result, bail};
use object_store::{PutMode, PutOptions, PutPayload};
use serde_json::json;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum MaintKind {
    /// Partition drop: remove every file of one `date` partition (dataChange=true).
    Drop,
    /// Compaction: remove one partition's files and add 1 file per `--ratio` (dataChange=false).
    Optimize,
}

#[derive(clap::Args, Debug, Clone)]
pub struct MaintArgs {
    #[arg(long)]
    pub table_url: url::Url,
    #[arg(long, value_enum)]
    pub kind: MaintKind,
    /// Generation parameters of the synthetic table (to regenerate exact paths).
    #[arg(long)]
    pub active: u64,
    #[arg(long, default_value_t = 365)]
    pub days: u32,
    #[arg(long, value_enum, default_value_t = StatsMode::Default)]
    pub stats: StatsMode,
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    #[arg(long, default_value_t = 65_536)]
    pub batch: u64,
    /// Partition index to drop (0 = oldest day).
    #[arg(long, default_value_t = 0)]
    pub day: u64,
    #[arg(long, default_value_t = 8)]
    pub ratio: u64,
    /// Sleep before committing, to land in the middle of an append run.
    #[arg(long, default_value_t = 0.0)]
    pub delay_s: f64,
}

pub async fn run(a: MaintArgs) -> Result<()> {
    if a.delay_s > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(a.delay_s)).await;
    }
    let rows = Rows { active: a.active, tombstones: 0, tombstone_mib: 128, days: a.days, stats: a.stats.clone(), seed: a.seed };
    let per_day = (a.active / u64::from(a.days)).max(1);
    let (lo, hi) = (a.day * per_day, ((a.day + 1) * per_day).min(a.active));
    let now = crate::now_ms();
    let drop = matches!(a.kind, MaintKind::Drop);
    let mut body = Vec::new();
    let op = if drop { "DELETE" } else { "OPTIMIZE" };
    serde_json::to_writer(&mut body, &json!({"commitInfo": {"timestamp": now, "operation": op, "operationParameters": {}, "engineInfo": "lrb-maint"}}))?;
    body.push(b'\n');
    let (mut removed, mut added, mut first_stats, mut date) = (0u64, 0u64, None, String::new());
    let mut chunk = lo - lo % a.batch;
    while chunk < hi {
        for (i, r) in (chunk..).zip(rows.adds(chunk, (chunk + a.batch).min(a.active))) {
            if i < lo || i >= hi {
                continue;
            }
            first_stats.get_or_insert(r.stats.clone());
            date.clone_from(&r.date);
            serde_json::to_writer(&mut body, &json!({"remove": {"path": r.path, "deletionTimestamp": now, "dataChange": drop,
                "extendedFileMetadata": true, "partitionValues": {"date": r.date}, "size": r.size}}))?;
            body.push(b'\n');
            removed += 1;
        }
        chunk += a.batch;
    }
    if !drop {
        for k in 0..removed.div_ceil(a.ratio) {
            serde_json::to_writer(&mut body, &json!({"add": {"path": format!("date={date}/optimized-{k:08}-{now}.zstd.parquet"),
                "partitionValues": {"date": date}, "size": 8i64 << 30, "modificationTime": now, "dataChange": false,
                "stats": first_stats.clone().flatten()}}))?;
            body.push(b'\n');
            added += 1;
        }
    }
    let (st, prefix) = store::open(&a.table_url)?;
    let log = prefix.clone().join("_delta_log");
    let mut latest = None;
    for m in futures::TryStreamExt::try_collect::<Vec<_>>(st.list(Some(&log))).await? {
        if let Some(v) = m.location.filename().and_then(|f| f.strip_suffix(".json")).and_then(|v| v.parse::<i64>().ok()) {
            latest = latest.max(Some(v));
        }
    }
    let Some(mut v) = latest else { bail!("no commits under {log}") };
    let bytes = body.len();
    let payload = PutPayload::from(body);
    let t0 = Instant::now();
    let mut attempts = 0;
    loop {
        v += 1;
        attempts += 1;
        let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
        match st.put_opts(&log.clone().join(format!("{v:020}.json")), payload.clone(), opts).await {
            Ok(_) => break,
            Err(object_store::Error::AlreadyExists { .. }) => {
                // Jump to the newest commit before retrying; v+1 alone falls behind an appender on slow puts.
                let offset = log.clone().join(format!("{v:020}.json"));
                for m in futures::TryStreamExt::try_collect::<Vec<_>>(st.list_with_offset(Some(&log), &offset)).await? {
                    if let Some(n) = m.location.filename().and_then(|f| f.strip_suffix(".json")).and_then(|n| n.parse::<i64>().ok()) {
                        v = v.max(n);
                    }
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    println!(
        "{}",
        json!({"kind": "maint", "op": op, "version": v, "removed": removed, "added": added, "bytes": bytes,
            "attempts": attempts, "put_s": t0.elapsed().as_secs_f64(), "partition": date, "at_ms": crate::now_ms()})
    );
    Ok(())
}
