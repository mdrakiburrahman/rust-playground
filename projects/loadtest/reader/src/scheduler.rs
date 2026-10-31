use std::{
    cell::Cell,
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use serde::Serialize;
use tokio::sync::Semaphore;

thread_local! {
    static DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
    static SCHEDULED: Cell<bool> = const { Cell::new(false) };
}

/// One process-wide CPU budget shared by snapshot and corpus decoding.
pub struct OracleScheduler {
    permits: Arc<Semaphore>,
    workers: usize,
}

impl OracleScheduler {
    fn new(workers: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(workers)),
            workers,
        }
    }

    /// Number of CPU jobs admitted concurrently, including jobs finishing after cancellation.
    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Runs blocking oracle work under the shared cap and rejects late results.
    ///
    /// # Errors
    /// Propagates worker errors/panics and deadline expiration, including while queued.
    pub async fn run<T: Send + 'static>(
        &self,
        deadline: Option<Instant>,
        work: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let run = async {
            let permit = Arc::clone(&self.permits).acquire_owned().await?;
            if let Some(deadline) = deadline {
                ensure!(Instant::now() < deadline, "oracle deadline elapsed while queued");
            }
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let _context = WorkerContext::enter(deadline);
                check_deadline()?;
                let value = work()?;
                check_deadline()?;
                Ok(value)
            })
            .await
            .context("oracle CPU worker failed")?
        };
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline.into(), run)
                .await
                .context("oracle preparation reached the validate deadline")?,
            None => run.await,
        }
    }
}

/// Shared scheduler; nested file decoders run serially inside its CPU jobs.
pub fn oracle_scheduler() -> &'static OracleScheduler {
    static SCHEDULER: OnceLock<OracleScheduler> = OnceLock::new();
    SCHEDULER
        .get_or_init(|| OracleScheduler::new(std::thread::available_parallelism().map_or(1, std::num::NonZero::get)))
}

struct WorkerContext;

impl WorkerContext {
    fn enter(deadline: Option<Instant>) -> Self {
        SCHEDULED.set(true);
        DEADLINE.set(deadline);
        Self
    }
}

impl Drop for WorkerContext {
    fn drop(&mut self) {
        DEADLINE.set(None);
        SCHEDULED.set(false);
    }
}

pub(crate) fn scheduled() -> bool {
    SCHEDULED.get()
}

pub(crate) fn check_deadline() -> Result<()> {
    ensure!(
        DEADLINE.get().is_none_or(|deadline| Instant::now() < deadline),
        "oracle CPU work reached the validate deadline"
    );
    Ok(())
}

/// Wall-clock anchors and elapsed time, retained even when a phase is cancelled.
#[derive(Clone, Debug, Serialize)]
pub struct OracleTiming {
    pub started_unix_ms: u64,
    pub elapsed_ms: u64,
    pub outcome: String,
}

impl OracleTiming {
    pub fn started() -> Self {
        Self {
            started_unix_ms: u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO)
                    .as_millis(),
            )
            .unwrap_or(u64::MAX),
            elapsed_ms: 0,
            outcome: "running".to_owned(),
        }
    }

    pub fn finish(&mut self, started: Instant, outcome: &str) {
        self.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.outcome = outcome.to_owned();
    }
}

/// Shared timing evidence, including work interrupted by cancellation.
#[derive(Clone, Debug, Default)]
pub struct OracleTimings(Arc<Mutex<BTreeMap<String, OracleTiming>>>);

impl OracleTimings {
    pub fn measure(&self, name: impl Into<String>) -> TimingGuard {
        let name = name.into();
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.clone(), OracleTiming::started());
        TimingGuard {
            timings: self.clone(),
            name,
            started: Instant::now(),
            outcome: "cancelled",
        }
    }

    pub fn evidence(&self) -> BTreeMap<String, OracleTiming> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }
}

pub struct TimingGuard {
    timings: OracleTimings,
    name: String,
    started: Instant,
    outcome: &'static str,
}

impl TimingGuard {
    pub fn finish(mut self, success: bool) {
        self.outcome = if success { "passed" } else { "failed" };
    }
}

impl Drop for TimingGuard {
    fn drop(&mut self) {
        if let Some(timing) = self
            .timings
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&self.name)
        {
            timing.finish(self.started, self.outcome);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn cancellation_keeps_the_permit_until_the_cpu_worker_stops() -> Result<()> {
        let scheduler = Arc::new(OracleScheduler::new(1));
        let entered = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let (release, wait) = std::sync::mpsc::channel();
        let (worker, signal, done) = (Arc::clone(&scheduler), Arc::clone(&entered), Arc::clone(&finished));
        let task = tokio::spawn(async move {
            worker
                .run(None, move || {
                    signal.store(1, Ordering::SeqCst);
                    wait.recv_timeout(Duration::from_secs(2))?;
                    done.store(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while entered.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        task.abort();
        assert!(task.await.is_err());
        let queued = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&queued);
        let queued_result = scheduler
            .run(Some(Instant::now() + Duration::from_millis(20)), move || {
                observed.store(1, Ordering::SeqCst);
                Ok(())
            })
            .await;
        release.send(())?;
        assert!(queued_result.is_err());
        assert_eq!(queued.load(Ordering::SeqCst), 0, "queued late work must never start");
        scheduler
            .run(None, move || {
                ensure!(finished.load(Ordering::SeqCst) == 1, "cancelled worker lost its permit");
                Ok(())
            })
            .await?;
        Ok(())
    }

    #[test]
    fn timing_guards_record_success_error_and_cancellation() {
        let timings = OracleTimings::default();
        timings.measure("passed").finish(true);
        timings.measure("failed").finish(false);
        drop(timings.measure("cancelled"));
        let evidence = timings.evidence();
        assert_eq!(evidence["passed"].outcome, "passed");
        assert_eq!(evidence["failed"].outcome, "failed");
        assert_eq!(evidence["cancelled"].outcome, "cancelled");
        assert!(evidence.values().all(|timing| timing.started_unix_ms > 0));
    }

    #[tokio::test]
    async fn concurrent_callers_share_the_cap_and_disable_nested_workers() -> Result<()> {
        let scheduler = Arc::new(OracleScheduler::new(2));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut jobs = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let (scheduler, active, peak) = (Arc::clone(&scheduler), Arc::clone(&active), Arc::clone(&peak));
            jobs.spawn(async move {
                scheduler
                    .run(None, move || {
                        ensure!(scheduled() && crate::decode_workers() == 1, "nested parallelism");
                        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(15));
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
            });
        }
        while let Some(job) = jobs.join_next().await {
            job??;
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn errors_panics_and_late_results_fail_closed() -> Result<()> {
        let scheduler = OracleScheduler::new(1);
        assert!(
            scheduler
                .run::<()>(None, || anyhow::bail!("decode corruption"))
                .await
                .err()
                .context("expected decode failure")?
                .to_string()
                .contains("corruption")
        );
        assert!(scheduler.run::<()>(None, || panic!("worker panic")).await.is_err());
        let deadline = Instant::now() + Duration::from_millis(20);
        assert!(
            scheduler
                .run(Some(deadline), || {
                    std::thread::sleep(Duration::from_millis(60));
                    Ok(())
                })
                .await
                .is_err()
        );
        assert!(scheduler.run(Some(Instant::now()), || Ok(())).await.is_err());
        assert_eq!(scheduler.run(None, || Ok(42)).await?, 42);
        Ok(())
    }
}
