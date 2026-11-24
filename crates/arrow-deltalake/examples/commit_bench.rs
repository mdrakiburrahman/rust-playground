//! Coalesced commit-path benchmark for one local Delta table.
//!
//! Rows/s counts durable `arrowcol_data_commit` events whose commit finished in the
//! half-open window `[start + warmup, start + warmup + measure)`, the same rule as the
//! OAT shared commit window. Warmup backlog counts when its commit finishes in the window.
//!
//! Usage: `commit_bench <table-root>`. Unset variables take their defaults; any value
//! that does not parse, or a zero count/duration, is an error.

use anyhow::{Context, Result, bail, ensure};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::TxnLookupMode;
use arrow_deltalake::commit::{BatchedTable, CommitOptions, CommitTrigger};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};

fn var(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("{name} is not valid UTF-8")),
    }
}

fn positive<T: FromStr + PartialEq + Default>(name: &str, default: T) -> Result<T>
where
    T::Err: Display + Send + Sync + 'static,
{
    let Some(text) = var(name)? else { return Ok(default) };
    let value = text
        .trim()
        .parse::<T>()
        .map_err(|error| anyhow::anyhow!("{name}={text:?} is invalid: {error}"))?;
    ensure!(value != T::default(), "{name} must be positive");
    Ok(value)
}

fn strict<T: serde::de::DeserializeOwned>(name: &str, default: &str) -> Result<T> {
    let text = var(name)?.unwrap_or_else(|| default.to_owned());
    serde_json::from_value(json!(text)).with_context(|| format!("{name}={text:?} is invalid"))
}

/// Captures durable commit events emitted by `arrow_deltalake::telemetry::data_commit`.
#[derive(Clone, Default)]
struct Commits(Arc<Mutex<Vec<Value>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Commits {
    fn on_event(&self, event: &tracing::Event<'_>, _: LayerContext<'_, S>) {
        struct Field(Option<String>);
        impl tracing::field::Visit for Field {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn Debug) {
                if field.name() == "commit" {
                    self.0 = Some(format!("{value:?}"));
                }
            }
        }
        let mut field = Field(None);
        event.record(&mut field);
        if let Some(text) = field.0
            && let Ok(value) = serde_json::from_str::<Value>(&text)
            && let Ok(mut commits) = self.0.lock()
        {
            commits.push(value);
        }
    }
}

fn pct(values: &mut [f64], q: f64) -> Option<f64> {
    values.sort_by(f64::total_cmp);
    let last = values.len().checked_sub(1)?;
    Some(values[(last as f64 * q).round() as usize])
}

fn unix_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

fn field_u64(event: &Value, name: &str) -> Result<u64> {
    event[name]
        .as_u64()
        .with_context(|| format!("commit event omitted {name}: {event}"))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let commits = Commits::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(commits.clone()))?;
    let mut args = std::env::args().skip(1);
    let root = args.next().context("usage: commit_bench <table-root>")?;
    ensure!(args.next().is_none(), "usage: commit_bench <table-root>");
    if var("BENCH_EAGER")?.is_some() {
        bail!("BENCH_EAGER was replaced by BENCH_TRIGGER=window|eager");
    }
    let producers = positive("BENCH_PRODUCERS", 4_usize)?;
    let depth = positive("BENCH_DEPTH", 8_usize)?;
    let warmup = Duration::from_secs(positive("BENCH_WARMUP", 5_u64)?);
    let measure = Duration::from_secs(positive("BENCH_SECONDS", 20_u64)?);
    let ids = positive("BENCH_IDS_PER_GROUP", 16_usize)?;
    let rows = positive("BENCH_ROWS_PER_ID", 4096_usize)?;
    let width = positive("BENCH_WIDTH", 64_usize)?;
    let stage_concurrency = positive("BENCH_STAGE_CONCURRENCY", 1_usize)?;
    let options = CommitOptions {
        batch_window: Duration::from_millis(positive("BENCH_WINDOW_MS", 2000_u64)?),
        max_files_per_commit: positive("BENCH_MAX_FILES", 128_usize)?,
        max_pending_files: positive("BENCH_MAX_PENDING", 512_usize)?,
        trigger: strict::<CommitTrigger>("BENCH_TRIGGER", "window")?,
        txn_lookup_mode: strict::<TxnLookupMode>("BENCH_LOOKUP", "batched")?,
        ..CommitOptions::default()
    };
    options.validate()?;
    let payload = "x".repeat(width);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..i64::try_from(rows)?)),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|row| format!("{payload}{row}")),
            )),
        ],
    )?;
    let table = BatchedTable::open(&root, options.clone()).await?;
    let start_ms = unix_ms()?;
    let started = Instant::now();
    let total = warmup + measure;
    let window_start = start_ms + u64::try_from(warmup.as_millis())?;
    let window_end = window_start + u64::try_from(measure.as_millis())?;
    let mut workers = Vec::new();
    for producer in 0..producers {
        let table = Arc::clone(&table);
        let batch = batch.clone();
        workers.push(tokio::spawn(async move {
            let mut receipts = Vec::new();
            let mut in_flight = VecDeque::new();
            let mut group = 0_u64;
            while started.elapsed() < total {
                let stamp = i64::try_from(unix_ms()?)?;
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
                    table.submit_prepared_group(prepared, false).await?.wait().await?;
                    anyhow::Ok((unix_ms()?, begin.elapsed(), stage))
                }));
                if in_flight.len() >= depth {
                    receipts.push(in_flight.pop_front().context("in-flight group")?.await??);
                }
            }
            table.flush_pending();
            for task in in_flight {
                receipts.push(task.await??);
            }
            anyhow::Ok(receipts)
        }));
    }
    let mut receipts = Vec::new();
    for worker in workers {
        receipts.extend(worker.await??);
    }
    let events = commits
        .0
        .lock()
        .map_err(|_| anyhow::anyhow!("commit capture poisoned"))?
        .clone();
    let uncertain = events.iter().filter(|event| event["outcome"] != "committed").count();
    ensure!(uncertain == 0, "{uncertain} commit events were not clean commits");
    let (mut committed, mut files, mut parquet_bytes, mut versions) = (0_u64, 0_u64, 0_u64, 0_u64);
    let mut commit_seconds = Vec::new();
    for event in &events {
        let finished = field_u64(event, "finished_at_unix_ms")?;
        if finished < window_start || finished >= window_end {
            continue;
        }
        committed += field_u64(event, "rows")?;
        files += field_u64(event, "files")?;
        parquet_bytes += field_u64(event, "parquet_bytes")?;
        versions += 1;
        commit_seconds.push(
            event["elapsed_seconds"]
                .as_f64()
                .context("commit event omitted elapsed_seconds")?,
        );
    }
    ensure!(versions > 0, "no durable commit finished inside the measurement window");
    let in_window = |finished: &u64| (window_start..window_end).contains(finished);
    let mut latency = receipts
        .iter()
        .filter(|(finished, ..)| in_window(finished))
        .map(|(_, latency, _)| latency.as_secs_f64())
        .collect::<Vec<_>>();
    let mut stage = receipts
        .iter()
        .filter(|(finished, ..)| in_window(finished))
        .map(|(.., stage)| stage.as_secs_f64())
        .collect::<Vec<_>>();
    let seconds = measure.as_secs_f64();
    println!(
        "{}",
        json!({
            "rows_basis": "durable_commit_events_finished_in_half_open_window",
            "trigger": options.trigger,
            "window_ms": options.batch_window.as_millis(),
            "lookup": options.txn_lookup_mode,
            "producers": producers, "depth": depth, "ids_per_group": ids, "rows_per_id": rows,
            "warmup_s": warmup.as_secs(), "measure_s": measure.as_secs(),
            "rows_per_s": committed as f64 / seconds,
            "rows": committed,
            "versions": versions,
            "versions_per_s": versions as f64 / seconds,
            "files_per_version": files as f64 / versions as f64,
            "parquet_bytes_per_s": parquet_bytes as f64 / seconds,
            "commit_p50_s": pct(&mut commit_seconds.clone(), 0.5),
            "commit_p99_s": pct(&mut commit_seconds.clone(), 0.99),
            "commit_busy": commit_seconds.iter().sum::<f64>() / seconds,
            "receipts_in_window": latency.len(),
            "receipt_latency_p50_s": pct(&mut latency, 0.5),
            "receipt_latency_p99_s": pct(&mut latency, 0.99),
            "stage_p50_s": pct(&mut stage, 0.5),
            "events_total": events.len(),
        })
    );
    Ok(())
}
