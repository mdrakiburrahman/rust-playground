use collector_chain::dmv;

use anyhow::{Context, Result};
use clap::Parser;
use std::cell::Cell;
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

const MAX_SCHEDULE_TIMEOUT_SECS: u64 = 2_160;

#[derive(Parser)]
#[command(about = "Generate deterministic DMV Arrow batches and upload them to ArrowCol")]
struct Args {
    #[arg(long)]
    endpoint: String,
    #[arg(long, default_value = "oat-benchmark")]
    client_id: String,
    #[arg(long, default_value_t = 1_000)]
    rows: usize,
    #[arg(long, default_value_t = 4)]
    concurrency: usize,
    #[arg(long)]
    table: Option<String>,
    #[arg(long)]
    edge_secret_file: Option<std::path::PathBuf>,
    /// Send repeated batches for this measured duration; zero keeps one-shot behavior.
    #[arg(long, default_value_t = 0)]
    duration_secs: u64,
    /// Continue sending during warm-up, but exclude this time from the reported rate.
    #[arg(long, default_value_t = 0)]
    warmup_secs: u64,
    /// Shared future Unix-millisecond run start used to align all fleet clients.
    #[arg(long)]
    run_start_unix_ms: Option<u64>,
    /// Run-scoped schedule file armed only after the complete client fleet is ready.
    #[arg(long)]
    schedule_file: Option<std::path::PathBuf>,
    /// Maximum time to wait for the orchestrator to arm this client's schedule.
    #[arg(long, default_value_t = 600)]
    schedule_timeout_secs: u64,
    /// Emit 30-second throughput windows and the earliest <=5% stable horizon.
    #[arg(long, default_value_t = false)]
    pilot: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.duration_secs > 600 || args.warmup_secs > 300 {
        anyhow::bail!("duration_secs must be at most 600 and warmup_secs at most 300");
    }
    validate_schedule_timeout(args.schedule_timeout_secs)?;
    if args.pilot
        && (!(60..=600).contains(&args.duration_secs)
            || args.duration_secs % 30 != 0
            || args.warmup_secs + args.duration_secs > 600)
    {
        anyhow::bail!(
            "pilot mode requires a 30-second-multiple measurement from 60 to 600 seconds and a total warm-up plus measurement of at most 600 seconds"
        );
    }
    let admissions = Cell::new((0_usize, 0_usize));
    let send = || async {
        let report = dmv::send_authenticated_report(
            &args.endpoint,
            &args.client_id,
            args.table.as_deref(),
            args.rows,
            args.concurrency,
            args.edge_secret_file.as_deref(),
        )
        .await?;
        let (best_effort, durable) = admissions.get();
        admissions.set((
            best_effort
                .checked_add(report.best_effort_streams)
                .context("best-effort admission count overflow")?,
            durable
                .checked_add(report.durable_streams)
                .context("durable admission count overflow")?,
        ));
        Ok::<_, anyhow::Error>(report.tables)
    };
    let table_count = if args.duration_secs == 0 {
        send().await?
    } else {
        let preflight_table_count = send().await?;
        let run_start_unix_ms = match (&args.schedule_file, args.run_start_unix_ms) {
            (Some(_), Some(_)) => anyhow::bail!("use either --schedule-file or --run-start-unix-ms, not both"),
            (Some(schedule_file), None) => {
                println!(
                    "OAT_CLIENT_READY client_id={} preflight_tables={preflight_table_count}",
                    args.client_id
                );
                let run_start_unix_ms =
                    wait_for_schedule(schedule_file, args.schedule_timeout_secs, &args.client_id).await?;
                println!("OAT_SCHEDULE_RECEIVED client_id={}", args.client_id);
                run_start_unix_ms
            }
            (None, Some(run_start_unix_ms)) => run_start_unix_ms,
            (None, None) => anyhow::bail!("duration-based clients require --schedule-file or --run-start-unix-ms"),
        };
        let current_unix_ms = unix_time_ms()?;
        if current_unix_ms >= run_start_unix_ms {
            anyhow::bail!("client missed the shared OAT run start time");
        }
        let run_start_delay = Duration::from_millis(run_start_unix_ms - current_unix_ms);
        let warmup = Duration::from_secs(args.warmup_secs);
        let measure = Duration::from_secs(args.duration_secs);
        let run_started = Instant::now()
            .checked_add(run_start_delay)
            .ok_or_else(|| anyhow::anyhow!("shared run start deadline is out of range"))?;
        tokio::time::sleep_until(run_started).await;
        let measured_at = run_started
            .checked_add(warmup)
            .ok_or_else(|| anyhow::anyhow!("warm-up deadline is out of range"))?;
        let deadline = measured_at
            .checked_add(measure)
            .ok_or_else(|| anyhow::anyhow!("measurement deadline is out of range"))?;
        warmup_until(measured_at, &send).await?;
        let measurement_started_unix_ms = unix_time_ms()?;
        let measurement_window_start_unix_ms = run_start_unix_ms
            .checked_add(args.warmup_secs.saturating_mul(1_000))
            .ok_or_else(|| anyhow::anyhow!("shared measurement start timestamp overflow"))?;
        let measurement_window_end_unix_ms = measurement_window_start_unix_ms
            .checked_add(args.duration_secs.saturating_mul(1_000))
            .ok_or_else(|| anyhow::anyhow!("shared measurement end timestamp overflow"))?;
        if measurement_started_unix_ms.abs_diff(measurement_window_start_unix_ms) > 1_000 {
            anyhow::bail!("client did not start within 1 second of the shared measurement interval");
        }
        let mut measured_rows = 0_u64;
        let mut pilot_window_rows = 0_u64;
        let mut pilot_window_index = 0_u64;
        let mut pilot_rates = Vec::new();
        loop {
            let count = send().await?;
            let completed_at = Instant::now();
            let batch_rows = u64::try_from(args.rows)?
                .checked_mul(u64::try_from(count)?)
                .ok_or_else(|| anyhow::anyhow!("measured batch row count overflow"))?;
            if completed_at >= measured_at && completed_at < deadline {
                measured_rows = measured_rows
                    .checked_add(batch_rows)
                    .ok_or_else(|| anyhow::anyhow!("measured row count overflow"))?;
                if args.pilot {
                    let elapsed = completed_at.duration_since(measured_at).as_secs();
                    let completed_window = elapsed / 30;
                    while pilot_window_index < completed_window {
                        record_pilot_window(
                            pilot_window_index,
                            pilot_window_rows,
                            measurement_window_start_unix_ms,
                            &mut pilot_rates,
                        );
                        pilot_window_index += 1;
                        pilot_window_rows = 0;
                    }
                    pilot_window_rows = pilot_window_rows
                        .checked_add(batch_rows)
                        .ok_or_else(|| anyhow::anyhow!("pilot window row count overflow"))?;
                }
            }
            if completed_at >= deadline {
                if args.pilot {
                    let window_count = args.duration_secs / 30;
                    while pilot_window_index < window_count {
                        record_pilot_window(
                            pilot_window_index,
                            pilot_window_rows,
                            measurement_window_start_unix_ms,
                            &mut pilot_rates,
                        );
                        pilot_window_index += 1;
                        pilot_window_rows = 0;
                    }
                    let stable_from_seconds = stable_window_horizon(&pilot_rates)
                        .map_or_else(|| "none".to_owned(), |seconds| seconds.to_string());
                    println!("pilot_stable_from_seconds={stable_from_seconds}");
                }
                println!(
                    "measured_rows={measured_rows} measured_seconds={:.3} rows_per_second={:.3} measurement_window_start_unix_ms={measurement_window_start_unix_ms} measurement_window_end_unix_ms={measurement_window_end_unix_ms} measurement_started_unix_ms={measurement_started_unix_ms}",
                    args.duration_secs as f64,
                    measured_rows as f64 / args.duration_secs as f64,
                );
                break preflight_table_count;
            }
        }
    };
    let (best_effort_streams, durable_streams) = admissions.get();
    println!(
        "OAT_COLLECTOR_COMPLETION {}",
        serde_json::json!({
            "best_effort_streams": best_effort_streams,
            "durable_streams": durable_streams,
            "completed_at_unix_ms": unix_time_ms()?,
        })
    );
    println!(
        "uploaded {table_count} DMV tables with {} rows each using concurrency {}",
        args.rows, args.concurrency
    );
    Ok(())
}

fn record_pilot_window(index: u64, rows: u64, measurement_start_unix_ms: u64, rates: &mut Vec<f64>) {
    let rate = rows as f64 / 30.0;
    rates.push(rate);
    let window_start_unix_ms = measurement_start_unix_ms.saturating_add(index.saturating_mul(30_000));
    println!(
        "pilot_window={} window_seconds=30 rows={rows} rows_per_second={rate:.3} window_start_unix_ms={window_start_unix_ms}",
        index + 1,
    );
}

fn stable_window_horizon(rates: &[f64]) -> Option<usize> {
    if rates.len() < 2 {
        return None;
    }
    let minimum = rates.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = rates.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let mean = rates.iter().sum::<f64>() / rates.len() as f64;
    if mean <= 0.0 || (maximum - minimum) / mean >= 0.05 {
        return None;
    }
    let mut prefix_minimum = f64::INFINITY;
    let mut prefix_maximum = f64::NEG_INFINITY;
    let mut prefix_sum = 0.0;
    for (index, rate) in rates.iter().copied().enumerate() {
        prefix_minimum = prefix_minimum.min(rate);
        prefix_maximum = prefix_maximum.max(rate);
        prefix_sum += rate;
        let prefix_mean = prefix_sum / (index + 1) as f64;
        if index >= 1 && prefix_mean > 0.0 && (prefix_maximum - prefix_minimum) / prefix_mean < 0.05 {
            return Some((index + 1) * 30);
        }
    }
    None
}

async fn wait_for_schedule(path: &std::path::Path, timeout_secs: u64, client_id: &str) -> Result<u64> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        match tokio::fs::read_to_string(path).await {
            Ok(contents) if contents.trim() == "pending" => {}
            Ok(contents) => {
                let run_start_unix_ms = contents
                    .trim()
                    .parse::<u64>()
                    .context("OAT schedule file must contain `pending` or a Unix-millisecond timestamp")?;
                if run_start_unix_ms == 0 {
                    anyhow::bail!("OAT schedule timestamp must be positive");
                }
                println!(
                    "OAT_CLIENT_SCHEDULE_RECEIVED client_id={} run_start_unix_ms={run_start_unix_ms}",
                    client_id
                );
                return Ok(run_start_unix_ms);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).with_context(|| format!("read OAT schedule file {}", path.display())),
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for the orchestrator to arm the OAT schedule");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn validate_schedule_timeout(timeout_secs: u64) -> Result<()> {
    if timeout_secs == 0 || timeout_secs > MAX_SCHEDULE_TIMEOUT_SECS {
        anyhow::bail!("schedule_timeout_secs must be between 1 and {MAX_SCHEDULE_TIMEOUT_SECS}");
    }
    Ok(())
}

fn unix_time_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

async fn warmup_until<F, Fut>(measured_at: Instant, mut send: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<usize>>,
{
    loop {
        if Instant::now() >= measured_at {
            return Ok(());
        }
        tokio::select! {
            biased;
            result = send() => {
                result?;
                if Instant::now() >= measured_at {
                    return Ok(());
                }
            }
            // Dropping this owned DoPut future cancels the streams; no upload is detached.
            _ = tokio::time::sleep_until(measured_at) => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_SCHEDULE_TIMEOUT_SECS, stable_window_horizon, validate_schedule_timeout, warmup_until};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::time::Instant;

    #[test]
    fn later_throughput_collapse_invalidates_the_stable_hint() {
        assert_eq!(stable_window_horizon(&[100.0, 100.0, 10.0, 10.0]), None);
    }

    #[test]
    fn stable_hint_is_reported_only_after_a_complete_stable_observation() {
        assert_eq!(stable_window_horizon(&[100.0, 103.0, 101.0]), Some(60));
        assert_eq!(stable_window_horizon(&[100.0]), None);
    }

    #[test]
    fn schedule_timeout_accepts_the_maximum_composed_startup_budget_only() {
        assert!(validate_schedule_timeout(1).is_ok());
        assert!(validate_schedule_timeout(MAX_SCHEDULE_TIMEOUT_SECS).is_ok());
        assert!(validate_schedule_timeout(0).is_err());
        assert!(validate_schedule_timeout(MAX_SCHEDULE_TIMEOUT_SECS + 1).is_err());
    }

    #[tokio::test]
    async fn warmup_cancels_a_slow_send_at_the_shared_boundary() -> anyhow::Result<()> {
        struct DropFlag(Arc<AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let started = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicBool::new(false));
        let deadline = Instant::now() + Duration::from_millis(40);
        warmup_until(deadline, {
            let started = Arc::clone(&started);
            let cancelled = Arc::clone(&cancelled);
            let completed = Arc::clone(&completed);
            move || {
                let started = Arc::clone(&started);
                let cancelled = Arc::clone(&cancelled);
                let completed = Arc::clone(&completed);
                async move {
                    let _drop_flag = DropFlag(cancelled);
                    started.store(true, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    completed.store(true, Ordering::SeqCst);
                    Ok(1)
                }
            }
        })
        .await?;

        assert!(started.load(Ordering::SeqCst));
        assert!(cancelled.load(Ordering::SeqCst));
        assert!(!completed.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn warmup_runs_completed_sends_until_the_boundary() -> anyhow::Result<()> {
        let sends = Arc::new(AtomicUsize::new(0));
        let deadline = Instant::now() + Duration::from_millis(60);
        warmup_until(deadline, {
            let sends = Arc::clone(&sends);
            move || {
                let sends = Arc::clone(&sends);
                async move {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    sends.fetch_add(1, Ordering::SeqCst);
                    Ok(1)
                }
            }
        })
        .await?;

        assert!(sends.load(Ordering::SeqCst) >= 1);
        Ok(())
    }

    #[tokio::test]
    async fn warmup_propagates_send_errors() -> anyhow::Result<()> {
        let error = match warmup_until(Instant::now() + Duration::from_secs(1), || async {
            anyhow::bail!("warm-up upload failed");
        })
        .await
        {
            Err(error) => error,
            Ok(_) => anyhow::bail!("warm-up send unexpectedly succeeded"),
        };

        assert_eq!(error.to_string(), "warm-up upload failed");
        Ok(())
    }

    #[tokio::test]
    async fn zero_warmup_does_not_start_a_send() -> anyhow::Result<()> {
        let sends = Arc::new(AtomicUsize::new(0));
        warmup_until(Instant::now(), {
            let sends = Arc::clone(&sends);
            move || {
                sends.fetch_add(1, Ordering::SeqCst);
                async { Ok(1) }
            }
        })
        .await?;

        assert_eq!(sends.load(Ordering::SeqCst), 0);
        Ok(())
    }
}
