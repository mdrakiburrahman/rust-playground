use anyhow::{Context, Result};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::commit::{BatchedTable, CommitOptions, CommitTrigger};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
}

struct Sample {
    finished: Duration,
    latency: Duration,
    stage: Duration,
    rows: u64,
    version: u64,
    files: usize,
    commit: Option<Duration>,
}

fn pct(values: &mut [f64], q: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    values.sort_by(f64::total_cmp);
    values[((values.len() - 1) as f64 * q).round() as usize]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let root = std::env::args().nth(1).context("usage: commit_bench <table-root>")?;
    let producers = env("BENCH_PRODUCERS", 4_usize);
    let depth = env("BENCH_DEPTH", 8_usize);
    let warmup = Duration::from_secs(env("BENCH_WARMUP", 5_u64));
    let measure = Duration::from_secs(env("BENCH_SECONDS", 20_u64));
    let ids = env("BENCH_IDS_PER_GROUP", 16_usize);
    let rows = env("BENCH_ROWS_PER_ID", 4096_usize);
    let width = env("BENCH_WIDTH", 64_usize);
    let stage_concurrency = env("BENCH_STAGE_CONCURRENCY", 1_usize);
    let options = CommitOptions {
        batch_window: Duration::from_millis(env("BENCH_WINDOW_MS", 2000_u64)),
        max_files_per_commit: env("BENCH_MAX_FILES", 128_usize),
        max_pending_files: env("BENCH_MAX_PENDING", 512_usize),
        trigger: if env("BENCH_EAGER", 0_u8) == 1 { CommitTrigger::Eager } else { CommitTrigger::Window },
        txn_lookup_mode: match std::env::var("BENCH_LOOKUP").as_deref() {
            Ok("per_id") => arrow_deltalake::TxnLookupMode::PerId,
            Ok("batched_cached") => arrow_deltalake::TxnLookupMode::BatchedCached,
            _ => arrow_deltalake::TxnLookupMode::Batched,
        },
    };
    let payload = "x".repeat(width);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
            Arc::new(StringArray::from_iter_values((0..rows).map(|row| format!("{payload}{row}")))),
        ],
    )?;
    let table = BatchedTable::open(&root, options.clone()).await?;
    let started = Instant::now();
    let total = warmup + measure;
    let mut workers = Vec::new();
    for producer in 0..producers {
        let table = Arc::clone(&table);
        let batch = batch.clone();
        workers.push(tokio::spawn(async move {
            let mut samples = Vec::new();
            let mut in_flight = VecDeque::new();
            let mut group = 0_u64;
            while started.elapsed() < total {
                let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
                let entries = (0..ids)
                    .map(|id| (batch.clone(), format!("bench.{producer}.{group}.{id}"), 0, stamp))
                    .collect::<Vec<_>>();
                group += 1;
                let table = Arc::clone(&table);
                in_flight.push_back(tokio::spawn(async move {
                    let begin = Instant::now();
                    let prepared = table
                        .stage_coalesced_publications_with_concurrency(entries, stage_concurrency)
                        .await?;
                    let stage = begin.elapsed();
                    let result = table.submit_prepared_group(prepared, false).await?.wait().await?;
                    anyhow::Ok(Sample {
                        finished: started.elapsed(),
                        latency: begin.elapsed(),
                        stage,
                        rows: result.files.iter().map(|file| file.rows as u64).sum(),
                        version: result.version,
                        files: result.files.len(),
                        commit: result.commit_duration,
                    })
                }));
                if in_flight.len() >= depth {
                    samples.push(in_flight.pop_front().context("in-flight group")?.await??);
                }
            }
            table.flush_pending();
            for task in in_flight {
                samples.push(task.await??);
            }
            anyhow::Ok(samples)
        }));
    }
    let mut samples = Vec::new();
    for worker in workers {
        samples.extend(worker.await??);
    }
    let window = samples
        .iter()
        .filter(|sample| sample.finished >= warmup && sample.finished < total)
        .collect::<Vec<_>>();
    let committed: u64 = window.iter().map(|sample| sample.rows).sum();
    let mut versions = BTreeMap::new();
    for sample in &window {
        let entry = versions.entry(sample.version).or_insert((0_usize, Duration::ZERO));
        entry.0 += sample.files;
        if let Some(commit) = sample.commit {
            entry.1 = entry.1.max(commit);
        }
    }
    let mut latency = window.iter().map(|sample| sample.latency.as_secs_f64()).collect::<Vec<_>>();
    let mut stage = window.iter().map(|sample| sample.stage.as_secs_f64()).collect::<Vec<_>>();
    let mut commit = versions.values().map(|(_, commit)| commit.as_secs_f64()).collect::<Vec<_>>();
    let busy = commit.iter().sum::<f64>() / measure.as_secs_f64();
    let files: usize = versions.values().map(|(files, _)| files).sum();
    println!(
        "{}",
        serde_json::json!({
            "trigger": format!("{:?}", options.trigger),
            "window_ms": options.batch_window.as_millis(),
            "lookup": format!("{:?}", options.txn_lookup_mode),
            "producers": producers, "depth": depth, "ids_per_group": ids, "rows_per_id": rows,
            "rows_per_s": committed as f64 / measure.as_secs_f64(),
            "groups": window.len(),
            "versions": versions.len(),
            "groups_per_version": window.len() as f64 / versions.len().max(1) as f64,
            "files_per_version": files as f64 / versions.len().max(1) as f64,
            "latency_p50_s": pct(&mut latency, 0.5), "latency_p99_s": pct(&mut latency, 0.99),
            "stage_p50_s": pct(&mut stage, 0.5),
            "commit_p50_s": pct(&mut commit, 0.5), "commit_p99_s": pct(&mut commit, 0.99),
            "commit_busy": busy,
            "last_version": versions.keys().last(),
        })
    );
    Ok(())
}
