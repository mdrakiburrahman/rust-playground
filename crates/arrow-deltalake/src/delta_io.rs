//! Process-wide Kernel I/O runtime, independent of the caller's Tokio runtime.

use anyhow::{Context, Result};
use delta_kernel::{DeltaResult, Error};
use delta_kernel_default_engine::executor::TaskExecutor;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, OnceLock, mpsc};
use tokio::runtime::{EnterGuard, Runtime};

pub(crate) const READ_AHEAD: usize = 8;
pub(crate) const BATCH_ROWS: usize = 256;
const WORKERS: usize = 2;
const BLOCKING_WORKERS: usize = 32;
const TASK_LIMIT: usize = 64;

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
    background_failure: Arc<Mutex<Option<String>>>,
}

#[derive(Debug, Default)]
struct Admission {
    active: Mutex<usize>,
    available: Condvar,
}

struct Permit(Arc<Admission>);

impl Admission {
    fn acquire(self: &Arc<Self>) -> Permit {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|error| panic!("Delta I/O admission poisoned: {error}"));
        while *active == TASK_LIMIT {
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
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKERS)
            .max_blocking_threads(BLOCKING_WORKERS)
            .thread_name("delta-io")
            .enable_all()
            .build()
            .context("create shared Delta I/O runtime")?;
        Ok(Self {
            runtime,
            admission: Arc::new(Admission::default()),
            blocking_admission: Arc::new(tokio::sync::Semaphore::new(BLOCKING_WORKERS)),
            background_failure: Arc::new(Mutex::new(None)),
        })
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
    assert!((WORKERS..=WORKERS + BLOCKING_WORKERS).contains(&threads));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

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
        let directory = tempfile::tempdir()?;
        let task = spawn_shared_io(async move {
            let executor = shared_executor()?;
            executor
                .spawn_blocking(move || {
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
        let admission = Arc::new(Admission::default());
        let mut permits = (0..TASK_LIMIT).map(|_| admission.acquire()).collect::<Vec<_>>();
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
            TASK_LIMIT
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
        assert!((2..=BLOCKING_WORKERS).contains(&observed_peak.load(Ordering::SeqCst)));
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
