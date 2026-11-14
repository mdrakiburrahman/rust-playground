//! Process-wide Kernel I/O runtime, independent of the caller's Tokio runtime.

use anyhow::{Context, Result};
use delta_kernel::{DeltaResult, Error};
use delta_kernel_default_engine::executor::TaskExecutor;
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, OnceLock, mpsc};
use tokio::runtime::{EnterGuard, Runtime};

/// Process-wide collector runtime settings, resolved once before starting workers.
///
/// Each field is overridden by `ARROWCOL_COLLECTOR_<UPPERCASE_FIELD_NAME>`.
/// Invalid, zero, non-Unicode, and out-of-range values fail initialization rather
/// than being silently clamped. Defaults alone are clamped to machine size.
/// Deserialized settings must pass [`Self::validate`] before use.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaRuntimeConfig {
    /// Async I/O workers: default cores / 4, clamped to 2..=16; allowed 1..=64.
    pub runtime_workers: usize,
    /// Blocking I/O workers: default cores * 2, clamped to 32..=128; allowed 2..=256.
    /// One worker is reserved for Tokio's filesystem handoffs.
    pub blocking_workers: usize,
    /// Concurrent uploads: default cores * 2, clamped to 8..=32; allowed 1..=256.
    pub upload_limit: usize,
    /// Admitted Kernel tasks: default cores * 4, clamped to 64..=256; allowed 2..=1024.
    pub task_limit: usize,
    /// Kernel read-ahead batches: default cores, clamped to 4..=16; allowed 1..=128.
    pub read_ahead: usize,
    /// Rows per Kernel batch: default cores * 1024, clamped to 4096..=32768;
    /// allowed 1..=262144.
    pub batch_rows: usize,
    /// Dedicated CPU workers: default cores - 1, clamped to 1..=32; allowed 1..=128.
    /// The waiting queue holds at most this many additional jobs.
    pub cpu_workers: usize,
}

impl Default for DeltaRuntimeConfig {
    fn default() -> Self {
        Self::for_cores(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get))
    }
}

impl DeltaRuntimeConfig {
    fn for_cores(cores: usize) -> Self {
        Self {
            runtime_workers: (cores / 4).clamp(2, 16),
            blocking_workers: cores.saturating_mul(2).clamp(32, 128),
            upload_limit: cores.saturating_mul(2).clamp(8, 32),
            task_limit: cores.saturating_mul(4).clamp(64, 256),
            read_ahead: cores.clamp(4, 16),
            batch_rows: cores.saturating_mul(1024).clamp(4096, 32768),
            cpu_workers: cores.saturating_sub(1).clamp(1, 32),
        }
    }

    /// Read startup environment overrides and validate all limits.
    ///
    /// # Errors
    /// Rejects malformed or out-of-range values.
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(error).with_context(|| format!("read {name}")),
        })
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Result<Option<String>>) -> Result<Self> {
        let mut config = Self::default();
        for (name, value) in [
            ("RUNTIME_WORKERS", &mut config.runtime_workers),
            ("BLOCKING_WORKERS", &mut config.blocking_workers),
            ("UPLOAD_LIMIT", &mut config.upload_limit),
            ("TASK_LIMIT", &mut config.task_limit),
            ("READ_AHEAD", &mut config.read_ahead),
            ("BATCH_ROWS", &mut config.batch_rows),
            ("CPU_WORKERS", &mut config.cpu_workers),
        ] {
            let name = format!("ARROWCOL_COLLECTOR_{name}");
            if let Some(raw) = lookup(&name)? {
                anyhow::ensure!(
                    !raw.is_empty() && raw.bytes().all(|byte| byte.is_ascii_digit()),
                    "{name} must be an unsigned decimal integer"
                );
                *value = raw.parse().with_context(|| format!("parse {name}"))?;
            }
        }
        config.validate()?;
        Ok(config)
    }

    /// Validate settings read from JSON or built programmatically.
    ///
    /// # Errors
    /// Returns the offending environment knob and its permitted range.
    pub fn validate(&self) -> Result<()> {
        for (name, value, min, max) in [
            ("RUNTIME_WORKERS", self.runtime_workers, 1, 64),
            ("BLOCKING_WORKERS", self.blocking_workers, 2, 256),
            ("UPLOAD_LIMIT", self.upload_limit, 1, 256),
            ("TASK_LIMIT", self.task_limit, 2, 1024),
            ("READ_AHEAD", self.read_ahead, 1, 128),
            ("BATCH_ROWS", self.batch_rows, 1, 262_144),
            ("CPU_WORKERS", self.cpu_workers, 1, 128),
        ] {
            anyhow::ensure!(
                (min..=max).contains(&value),
                "ARROWCOL_COLLECTOR_{name} must be in {min}..={max}, got {value}"
            );
        }
        Ok(())
    }
}

static CONFIG: OnceLock<std::result::Result<DeltaRuntimeConfig, String>> = OnceLock::new();

/// Return the immutable effective startup settings (including environment overrides).
///
/// # Errors
/// Returns invalid startup configuration; initialization errors remain sticky.
pub fn runtime_config() -> Result<&'static DeltaRuntimeConfig> {
    CONFIG
        .get_or_init(|| DeltaRuntimeConfig::from_env().map_err(|error| format!("{error:#}")))
        .as_ref()
        .map_err(|error| anyhow::anyhow!("{error}"))
}

type CpuJob = Box<dyn FnOnce() + Send + 'static>;

struct CpuPool {
    sender: tokio::sync::mpsc::Sender<CpuJob>,
}

thread_local! {
    static CPU_WORKER: Cell<bool> = const { Cell::new(false) };
}

impl CpuPool {
    fn new(workers: usize) -> Result<Self> {
        let (sender, receiver) = tokio::sync::mpsc::channel::<CpuJob>(workers);
        let receiver = Arc::new(Mutex::new(receiver));
        for index in 0..workers {
            let receiver = Arc::clone(&receiver);
            std::thread::Builder::new()
                .name(format!("delta-cpu-{index}"))
                .spawn(move || {
                    CPU_WORKER.set(true);
                    loop {
                        let job = receiver
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .blocking_recv();
                        match job {
                            Some(job) => job(),
                            None => break,
                        }
                    }
                })
                .context("create Delta CPU worker")?;
        }
        Ok(Self { sender })
    }

    #[cfg(test)]
    fn run<F, T>(&self, task: F) -> Result<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        // Recursive offloads run inline: no CPU worker may wait for its own pool.
        if CPU_WORKER.get() {
            return Ok(task());
        }
        futures::executor::block_on(self.run_async(task))
    }

    async fn run_async<F, T>(&self, task: F) -> Result<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        if CPU_WORKER.get() {
            return Ok(task());
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.sender
            .send(Box::new(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task));
                let _ = sender.send(result);
            }))
            .await
            .map_err(|_| anyhow::anyhow!("Delta CPU pool stopped"))?;
        match receiver.await.context("Delta CPU worker stopped")? {
            Ok(output) => Ok(output),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

static CPU_POOL: OnceLock<std::result::Result<CpuPool, String>> = OnceLock::new();

fn cpu_pool() -> Result<&'static CpuPool> {
    let workers = runtime_config()?.cpu_workers;
    match CPU_POOL.get_or_init(|| CpuPool::new(workers).map_err(|error| format!("{error:#}"))) {
        Ok(pool) => Ok(pool),
        Err(error) => anyhow::bail!("{error}"),
    }
}

/// Run owned sorting/encoding work on a bounded, dedicated process-wide CPU pool.
///
/// Blocks the synchronous caller, with at most `cpu_workers` running jobs and
/// `cpu_workers` queued jobs. Call from synchronous code, never directly from an
/// async I/O task. CPU jobs may use the independent shared I/O executor; they do
/// not consume its blocking workers or Kernel task permits. Nested CPU offloads
/// run inline, and task panics propagate to the original caller without killing
/// a worker. For a fallible closure, use `run_cpu(move || work())??`.
///
/// # Errors
/// Returns invalid startup configuration or CPU pool initialization failure.
#[cfg(test)]
pub fn run_cpu<F, T>(task: F) -> Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    cpu_pool()?.run(task)
}

/// Offload synchronous CPU work without blocking an async caller's runtime.
///
/// Admission awaits the bounded queue before submitting; cancelling the caller
/// does not interrupt an already admitted job. For fallible work use
/// `run_cpu_async(move || sorter.finish()).await??`.
///
/// # Errors
/// Returns invalid startup configuration or CPU pool initialization failure.
pub async fn run_cpu_async<F, T>(task: F) -> Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    cpu_pool()?.run_async(task).await
}

/// Poll an owned encoding/I/O future on a CPU worker, with shared Tokio I/O.
///
/// Unlike spawning the future onto Tokio, its synchronous encoding work stays
/// on the CPU pool. I/O drivers run independently, so waiting for uploads cannot
/// exhaust the I/O runtime's blocking pool. A worker remains occupied until the
/// future completes: upload concurrency is also bounded by the CPU worker count.
/// Move the writer and batch into an `async move` block and return the writer
/// to continue writing. For a fallible future use `run_cpu_future(future).await??`.
///
/// # Errors
/// Returns configuration or CPU/I/O runtime initialization failure.
pub async fn run_cpu_future<F, T>(future: F) -> Result<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    run_cpu_async(move || {
        let executor = shared_executor()?;
        let _guard = executor.enter();
        Ok::<_, anyhow::Error>(futures::executor::block_on(future))
    })
    .await?
}

static EXECUTOR: OnceLock<std::result::Result<Arc<SharedExecutor>, String>> = OnceLock::new();

/// Kept alive for the process lifetime: dropping a table never shuts down I/O
/// needed by other tables, or drops a runtime inside an async call context.
pub(crate) fn shared_executor() -> Result<Arc<SharedExecutor>> {
    match EXECUTOR.get_or_init(|| {
        SharedExecutor::new()
            .map(Arc::new)
            .map_err(|error| format!("{error:#}"))
    }) {
        Ok(executor) => Ok(Arc::clone(executor)),
        Err(error) => anyhow::bail!("{error}"),
    }
}

/// Spawn a process-lifetime asynchronous owner, independent of the caller's runtime.
///
/// Tasks must not block or call synchronous `DeltaTable` APIs on the I/O workers.
/// Use the bounded executor's `spawn_blocking` for synchronous work, or an existing
/// asynchronous API. File encoding and sorting must retain their batch/sort budgets.
/// This does not acquire Kernel task admission; callers must bound owner counts.
///
/// # Errors
/// Returns failure to initialize the process-wide I/O runtime.
pub fn spawn_shared_io<F, T>(future: F) -> Result<tokio::task::JoinHandle<T>>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    Ok(shared_executor()?.runtime.spawn(future))
}

#[derive(Debug)]
pub(crate) struct SharedExecutor {
    runtime: Runtime,
    admission: Arc<Admission>,
    blocking_admission: Arc<tokio::sync::Semaphore>,
    upload_admission: Arc<tokio::sync::Semaphore>,
    background_failure: Arc<Mutex<Option<String>>>,
}

#[derive(Debug)]
struct Admission {
    active: Mutex<usize>,
    available: Condvar,
    limit: usize,
}

struct Permit(Arc<Admission>);

impl Admission {
    fn new(limit: usize) -> Self {
        Self {
            active: Mutex::new(0),
            available: Condvar::new(),
            limit,
        }
    }

    fn acquire(self: &Arc<Self>) -> Permit {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|error| panic!("Delta I/O admission poisoned: {error}"));
        while *active >= self.limit {
            active = self
                .available
                .wait(active)
                .unwrap_or_else(|error| panic!("Delta I/O admission poisoned: {error}"));
        }
        *active += 1;
        Permit(Arc::clone(self))
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut active = self
            .0
            .active
            .lock()
            .unwrap_or_else(|error| panic!("Delta I/O admission poisoned: {error}"));
        *active -= 1;
        self.0.available.notify_one();
    }
}

impl SharedExecutor {
    fn new() -> Result<Self> {
        let config = runtime_config()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.runtime_workers)
            .max_blocking_threads(config.blocking_workers)
            .thread_name("delta-io")
            .enable_all()
            .build()
            .context("create shared Delta I/O runtime")?;
        Ok(Self {
            runtime,
            admission: Arc::new(Admission::new(config.task_limit)),
            // Admitted synchronous jobs may wait on tokio::fs, which uses this
            // same blocking pool. Keep one slot free for its filesystem work.
            blocking_admission: Arc::new(tokio::sync::Semaphore::new(config.blocking_workers - 1)),
            upload_admission: Arc::new(tokio::sync::Semaphore::new(config.upload_limit)),
            background_failure: Arc::new(Mutex::new(None)),
        })
    }

    pub(crate) async fn upload_slot(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.upload_admission)
            .acquire_owned()
            .await
            .context("Delta upload admission failed")
    }

    #[cfg(test)]
    pub(crate) fn available_upload_slots(&self) -> usize {
        self.upload_admission.available_permits()
    }

    fn check_background_failure(&self) {
        let failure = self
            .background_failure
            .lock()
            .unwrap_or_else(|error| panic!("Delta I/O failure state poisoned: {error}"));
        if let Some(error) = failure.as_ref() {
            panic!("Delta background I/O task failed: {error}");
        }
    }
}

impl TaskExecutor for SharedExecutor {
    type Guard<'a> = EnterGuard<'a>;

    fn block_on<T>(&self, task: T) -> T::Output
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.check_background_failure();
        // Admission happens on the synchronous caller, not in an unbounded
        // queue of spawned tasks. No nested Runtime::block_on/block_in_place:
        // this also works on current-thread runtimes and commit-worker threads.
        let permit = self.admission.acquire();
        let (sender, receiver) = mpsc::sync_channel(1);
        let task = self.runtime.spawn(task);
        self.runtime.spawn(async move {
            let result = task.await;
            drop(permit);
            let _ = sender.send(result);
        });
        let result = receiver
            .recv()
            .unwrap_or_else(|error| panic!("Delta I/O runtime stopped: {error}"));
        self.check_background_failure();
        match result {
            Ok(output) => output,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => panic!("Delta I/O task failed: {error}"),
        }
    }

    fn spawn<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.check_background_failure();
        let permit = self.admission.acquire();
        let failure = Arc::clone(&self.background_failure);
        let task = self.runtime.spawn(task);
        self.runtime.spawn(async move {
            if let Err(error) = task.await {
                *failure
                    .lock()
                    .unwrap_or_else(|error| panic!("Delta I/O failure state poisoned: {error}")) =
                    Some(error.to_string());
            }
            drop(permit);
        });
    }

    fn spawn_blocking<T, R>(&self, task: T) -> Pin<Box<dyn Future<Output = DeltaResult<R>> + Send + '_>>
    where
        T: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        // Bound admission before submitting to Tokio's blocking pool. Futures
        // awaiting admission stay with their caller rather than being spawned.
        Box::pin(async move {
            let permit = Arc::clone(&self.blocking_admission)
                .acquire_owned()
                .await
                .map_err(|error| Error::generic(format!("Delta blocking I/O admission failed: {error}")))?;
            self.runtime
                .spawn_blocking(move || {
                    let _permit = permit;
                    task()
                })
                .await
                .map_err(Error::join_failure)
        })
    }

    fn enter(&self) -> EnterGuard<'_> {
        self.runtime.enter()
    }
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn assert_worker_bound() -> Result<()> {
    let mut threads = 0;
    for entry in std::fs::read_dir("/proc/self/task")? {
        let entry = entry?;
        // A completed, unrelated test thread may exit during enumeration.
        match std::fs::read_to_string(entry.path().join("comm")) {
            Ok(name) if name.trim() == "delta-io" => threads += 1,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let config = runtime_config()?;
    assert!((config.runtime_workers..=config.runtime_workers + config.blocking_workers).contains(&threads));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn runtime_defaults_scale_with_cpu_bounds() -> Result<()> {
        let small = DeltaRuntimeConfig::for_cores(1);
        assert_eq!(
            (small.runtime_workers, small.cpu_workers, small.batch_rows),
            (2, 1, 4096)
        );
        let large = DeltaRuntimeConfig::for_cores(usize::MAX);
        assert_eq!(
            (large.runtime_workers, large.cpu_workers, large.batch_rows),
            (16, 32, 32768)
        );
        small.validate()?;
        large.validate()?;
        let mut json = serde_json::to_value(small)?;
        json["unknown"] = serde_json::json!(1);
        assert!(serde_json::from_value::<DeltaRuntimeConfig>(json).is_err());
        Ok(())
    }

    #[test]
    fn runtime_overrides_are_strict_and_do_not_mutate_the_environment() -> Result<()> {
        for (knob, minimum, maximum) in [
            ("RUNTIME_WORKERS", 1, 64),
            ("BLOCKING_WORKERS", 2, 256),
            ("UPLOAD_LIMIT", 1, 256),
            ("TASK_LIMIT", 2, 1024),
            ("READ_AHEAD", 1, 128),
            ("BATCH_ROWS", 1, 262_144),
            ("CPU_WORKERS", 1, 128),
        ] {
            let knob = format!("ARROWCOL_COLLECTOR_{knob}");
            for value in [minimum, maximum] {
                let config = DeltaRuntimeConfig::from_lookup(|name| Ok((name == knob).then(|| value.to_string())))?;
                let field = knob.trim_start_matches("ARROWCOL_COLLECTOR_").to_ascii_lowercase();
                assert_eq!(serde_json::to_value(config)?[field], value);
            }
            for value in [
                String::new(),
                " 2".to_owned(),
                "+2".to_owned(),
                "-1".to_owned(),
                "1.5".to_owned(),
                "184467440737095516160".to_owned(),
                (minimum - 1).to_string(),
                (maximum + 1).to_string(),
            ] {
                let error = DeltaRuntimeConfig::from_lookup(|name| Ok((name == knob).then(|| value.clone())))
                    .err()
                    .context("invalid override must fail")?;
                assert!(format!("{error:#}").contains(&knob));
            }
        }
        Ok(())
    }

    #[test]
    fn cpu_jobs_keep_panics_nested_calls_and_io_independent() -> Result<()> {
        let pool = Arc::new(CpuPool::new(1)?);
        let nested_pool = Arc::clone(&pool);
        let caller = std::thread::current().id();
        let (worker, answer) = pool.run(move || {
            let answer = nested_pool.run(|| {
                Ok::<_, anyhow::Error>(shared_executor()?.block_on(async {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    42
                }))
            })?;
            Ok::<_, anyhow::Error>((std::thread::current().id(), answer))
        })??;
        assert_ne!(worker, caller);
        assert_eq!(answer?, 42);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = pool.run(|| std::panic::panic_any(73_u32));
        }))
        .err()
        .context("CPU panic must reach the caller")?;
        assert_eq!(panic.downcast_ref::<u32>(), Some(&73));
        assert_eq!(pool.run(|| 42)?, 42);
        Ok(())
    }

    #[test]
    fn cpu_queue_and_execution_are_bounded() -> Result<()> {
        let pool = Arc::new(CpuPool::new(1)?);
        let (started, wait_started) = mpsc::sync_channel(1);
        let (release, wait_release) = mpsc::sync_channel(1);
        let active_pool = Arc::clone(&pool);
        let caller = std::thread::spawn(move || {
            active_pool.run(move || {
                started.send(()).context("notify CPU test")?;
                wait_release.recv().context("release CPU test")?;
                Ok::<_, anyhow::Error>(())
            })?
        });
        wait_started.recv_timeout(Duration::from_secs(5))?;
        pool.sender
            .try_send(Box::new(|| {}))
            .map_err(|_| anyhow::anyhow!("expected room for one queued CPU job"))?;
        assert!(matches!(
            pool.sender.try_send(Box::new(|| {})),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))
        ));
        release.send(())?;
        caller.join().unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(pool.run(|| 42)?, 42);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cpu_async_admission_does_not_block_the_caller_runtime() -> Result<()> {
        let pool = Arc::new(CpuPool::new(1)?);
        let (started, wait_started) = mpsc::sync_channel(1);
        let (release, wait_release) = mpsc::sync_channel(1);
        let active_pool = Arc::clone(&pool);
        let caller = std::thread::spawn(move || {
            active_pool.run(move || {
                started.send(()).context("notify CPU test")?;
                wait_release.recv().context("release CPU test")?;
                Ok::<_, anyhow::Error>(())
            })?
        });
        wait_started.recv_timeout(Duration::from_secs(5))?;
        pool.sender
            .try_send(Box::new(|| {}))
            .map_err(|_| anyhow::anyhow!("expected room for one queued CPU job"))?;
        let pending = tokio::spawn(async move { pool.run_async(|| 42).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!pending.is_finished());
        release.send(())?;
        assert_eq!(tokio::time::timeout(Duration::from_secs(5), pending).await???, 42);
        caller.join().unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cpu_future_keeps_encoding_on_cpu_threads_and_drives_io() -> Result<()> {
        let caller = std::thread::current().id();
        let answer = run_cpu_future(async move {
            assert!(CPU_WORKER.get());
            let worker = std::thread::current().id();
            assert_ne!(worker, caller);
            tokio::time::sleep(Duration::from_millis(1)).await;
            assert!(tokio::fs::metadata(std::env::current_exe()?).await?.is_file());
            assert_eq!(worker, std::thread::current().id());
            run_cpu(|| 42)
        })
        .await??;
        assert_eq!(answer, 42);
        let panic = tokio::spawn(run_cpu_async(|| std::panic::panic_any(73_u32)))
            .await
            .err()
            .context("CPU panic must reach the async caller")?;
        assert_eq!(panic.into_panic().downcast_ref::<u32>(), Some(&73));
        assert_eq!(run_cpu_async(|| 42).await?, 42);
        Ok(())
    }

    #[test]
    fn spawned_owner_survives_caller_runtime_drop() -> Result<()> {
        let caller = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let (release, task) = caller.block_on(async {
            let (release, wait) = tokio::sync::oneshot::channel();
            let task = spawn_shared_io(async move {
                wait.await?;
                Ok::<_, anyhow::Error>(42)
            })?;
            Ok::<_, anyhow::Error>((release, task))
        })?;
        drop(caller);
        let remaining = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        remaining.block_on(async {
            release
                .send(())
                .map_err(|()| anyhow::anyhow!("owner stopped with caller runtime"))?;
            assert_eq!(tokio::time::timeout(Duration::from_secs(5), task).await???, 42);
            Ok(())
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn spawned_owner_offloads_sync_delta_calls() -> Result<()> {
        const CHILD: &str = "ARROWCOL_TEST_ISOLATED_OFFLOAD";
        if std::env::var_os(CHILD).as_deref() != Some(std::ffi::OsStr::new("1")) {
            let mut child = std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "delta_io::tests::spawned_owner_offloads_sync_delta_calls",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .spawn()
                .context("start isolated shared-executor offload test")?;
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        anyhow::ensure!(
                            status.success(),
                            "isolated shared-executor offload test failed: {status}"
                        );
                        return Ok(());
                    }
                    Ok(None) if std::time::Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    result => {
                        child.kill().context("stop isolated shared-executor offload test")?;
                        child.wait().context("reap isolated shared-executor offload test")?;
                        result.context("poll isolated shared-executor offload test")?;
                        anyhow::bail!("isolated shared-executor offload test exceeded 30s watchdog");
                    }
                }
            }
        }
        let directory = tempfile::tempdir()?;
        let caller_runtime = tokio::runtime::Handle::current().id();
        let task = spawn_shared_io(async move {
            assert_ne!(tokio::runtime::Handle::current().id(), caller_runtime);
            let owner_thread = std::thread::current().id();
            let executor = shared_executor()?;
            executor
                .spawn_blocking(move || {
                    assert_ne!(std::thread::current().id(), owner_thread);
                    use arrow::datatypes::{DataType, Field, Schema};
                    let table = crate::delta::DeltaTable::open(directory.path())?;
                    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
                    let _file = table.new_file(schema)?;
                    assert_eq!(table.lookup_txn("shared-owner-regression")?, None);
                    Ok::<(), anyhow::Error>(())
                })
                .await?
        })?;
        tokio::time::timeout(Duration::from_secs(5), task).await???;
        Ok(())
    }

    /// Scenario: a synchronous caller has no Tokio runtime.
    /// Guarantees: the shared runtime executes timers and survives table-independent use.
    #[test]
    fn synchronous_context() -> Result<()> {
        let executor = shared_executor()?;
        assert_eq!(executor.block_on(async { 42 }), 42);
        Ok(())
    }

    /// Scenario: synchronous task admission reaches its process-wide limit.
    /// Guarantees: excess work waits before spawning and resumes when a permit is released.
    #[test]
    fn task_admission_is_bounded() -> Result<()> {
        let limit = 3;
        let admission = Arc::new(Admission::new(limit));
        let mut permits = (0..limit).map(|_| admission.acquire()).collect::<Vec<_>>();
        let waiting_admission = Arc::clone(&admission);
        let (sender, receiver) = mpsc::sync_channel(1);
        let waiter = std::thread::spawn(move || {
            let permit = waiting_admission.acquire();
            let _ = sender.send(permit);
        });
        assert!(matches!(
            receiver.recv_timeout(Duration::from_millis(10)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(permits.pop());
        let permit = receiver.recv_timeout(Duration::from_secs(5))?;
        assert_eq!(
            *admission.active.lock().map_err(|error| anyhow::anyhow!("{error}"))?,
            limit
        );
        drop(permit);
        drop(permits);
        waiter
            .join()
            .map_err(|_| anyhow::anyhow!("admission waiter panicked"))?;
        Ok(())
    }

    /// Scenario: Kernel runs inside a single-threaded collector/commit runtime.
    /// Guarantees: independent I/O progresses without block_in_place or nested block_on.
    #[tokio::test(flavor = "current_thread")]
    async fn current_thread_context() -> Result<()> {
        exercise_context().await
    }

    /// Scenario: Kernel runs on a multi-threaded caller runtime.
    /// Guarantees: the same shared executor works without growing the caller's blocking pool.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multithread_context() -> Result<()> {
        exercise_context().await
    }

    async fn exercise_context() -> Result<()> {
        let executor = shared_executor()?;
        let id = executor.block_on(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            tokio::runtime::Handle::current().id()
        });
        assert_ne!(id, tokio::runtime::Handle::current().id());
        let (sender, receiver) = mpsc::sync_channel(1);
        executor.spawn(async move {
            let _ = sender.send(42);
        });
        assert_eq!(receiver.recv_timeout(Duration::from_secs(5))?, 42);
        Ok(())
    }

    /// Scenario: thirteen blocking callers hand off nested file I/O to the shared async workers.
    /// Guarantees: this caller count fits both admission and the blocking pool without deadlocking.
    #[tokio::test(flavor = "current_thread")]
    async fn thirteen_blocking_callers_can_handoff_nested_file_io() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("input");
        std::fs::write(&path, b"bounded nested I/O")?;
        let executor = shared_executor()?;
        let io_executor = Arc::clone(&executor);
        executor.block_on(async move {
            let tasks = (0..13).map(|_| {
                let path = path.clone();
                let nested_executor = Arc::clone(&io_executor);
                io_executor.spawn_blocking(move || nested_executor.block_on(async move { tokio::fs::read(path).await }))
            });
            for result in futures::future::join_all(tasks).await {
                assert_eq!(result??, b"bounded nested I/O");
            }
            Ok::<(), anyhow::Error>(())
        })?;
        #[cfg(target_os = "linux")]
        assert_worker_bound()?;
        Ok(())
    }

    /// Scenario: far more blocking requests arrive than the process-wide pool limit.
    /// Guarantees: admission and thread growth stay bounded, and all requests complete.
    #[test]
    fn blocking_concurrency_and_threads_are_bounded() -> Result<()> {
        let executor = shared_executor()?;
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let io_executor = Arc::clone(&executor);
        let observed_peak = Arc::clone(&peak);
        executor.block_on(async move {
            let tasks = (0..96).map(|_| {
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                io_executor.spawn_blocking(move || {
                    let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(count, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(10));
                    active.fetch_sub(1, Ordering::SeqCst);
                })
            });
            for result in futures::future::join_all(tasks).await {
                result?;
            }
            Ok::<(), Error>(())
        })?;
        assert!((1..=runtime_config()?.blocking_workers).contains(&observed_peak.load(Ordering::SeqCst)));
        #[cfg(target_os = "linux")]
        assert_worker_bound()?;
        Ok(())
    }

    /// Scenario: an I/O task panics rather than returning its result.
    /// Guarantees: blocking tasks return JoinError and synchronous calls propagate panics.
    #[test]
    fn task_failures_are_not_silenced() -> Result<()> {
        let executor = shared_executor()?;
        let io_executor = Arc::clone(&executor);
        let result = executor.block_on(async move {
            io_executor
                .spawn_blocking(|| panic!("injected blocking I/O failure"))
                .await
        });
        assert!(result.is_err());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            executor.block_on(async { panic!("injected synchronous I/O failure") });
        }));
        assert!(result.is_err());
        assert_eq!(executor.block_on(async { 42 }), 42);
        Ok(())
    }
}
