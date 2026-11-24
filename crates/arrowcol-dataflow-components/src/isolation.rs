//! Per-batch diagnostics only: no table lookup, sorting, grouping, Delta log or commit receipt.

use std::fs;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use arrow_array::RecordBatch;
use arrowcol_protocol::IsolationLevel;
use cpu_time::ThreadTime;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, path::Path as ObjectPath};
use parquet::arrow::ArrowWriter;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

const WORKERS: usize = 12;
const MIB: usize = 1 << 20;
const UNIT: usize = 64 << 10;
const BUDGET: usize = 512 * MIB;
const MAX_INPUT: usize = 64 * MIB;
const MAX_OUTPUT: usize = 128 * MIB;
const MAX_ROWS: usize = 1 << 20;
const SCRATCH: &str = "ARROWCOL_ISOLATION_SCRATCH_DIR";
const OBJECT_STORE: &str = "ARROWCOL_ISOLATION_OBJECT_STORE";
const ROOT_NAME: &str = "arrowcol-isolation";

struct Registration {
    active: Weak<WorkerPool>,
    metrics: Arc<Metrics>,
}

static REGISTRATION: Mutex<Option<Registration>> = Mutex::new(None);

/// Cumulative physical work for this sink generation, not Delta publication evidence. Successful stage counts may exceed
/// successful accepts if a later stage fails. Wall and thread CPU times include failed attempts.
/// Absent stages and unmeasurable CPU times are null. Async storage CPU is never estimated.
#[must_use]
pub fn isolation_stage_snapshot() -> Option<Value> {
    REGISTRATION.lock().ok()?.as_ref().map(|entry| entry.metrics.snapshot())
}

pub(crate) struct IsolationSink(Arc<WorkerPool>);

impl IsolationSink {
    pub(crate) fn new(level: IsolationLevel) -> Result<Self> {
        ensure!(
            matches!(level, IsolationLevel::L2 | IsolationLevel::L3 | IsolationLevel::L4),
            "not a physical isolation level"
        );
        let mut entry = REGISTRATION
            .lock()
            .map_err(|_| anyhow::anyhow!("isolation registry poisoned"))?;
        if let Some(active) = entry.as_ref().and_then(|entry| entry.active.upgrade()) {
            ensure!(
                active.metrics.level == level,
                "conflicting physical isolation levels in one collector"
            );
            return Ok(Self(active));
        }
        let root = (level != IsolationLevel::L2)
            .then(|| std::env::var_os(SCRATCH).map(PathBuf::from).context(SCRATCH))
            .transpose()?;
        let store = (level == IsolationLevel::L4)
            .then(|| std::env::var(OBJECT_STORE).context(OBJECT_STORE))
            .transpose()?;
        let pool = Arc::new(WorkerPool::configure(level, root.as_deref(), store.as_deref())?);
        *entry = Some(Registration {
            active: Arc::downgrade(&pool),
            metrics: Arc::clone(&pool.metrics),
        });
        Ok(Self(pool))
    }

    pub(crate) async fn accept(&self, batch: RecordBatch) -> Result<()> {
        let pool = &self.0;
        let rows = batch.num_rows();
        let input = usize::try_from(arrow_deltalake::record_batch_logical_bytes(&batch)?)?;
        if input > MAX_INPUT || rows > MAX_ROWS {
            pool.metrics.rejected.fetch_add(1, Relaxed);
            anyhow::bail!("diagnostic batch exceeds the {MAX_INPUT}-byte or {MAX_ROWS}-row limit");
        }
        let initial = (2 * input + MIB).min(MAX_OUTPUT);
        let units = u32::try_from((input + initial).div_ceil(UNIT))?;
        let start = Instant::now();
        // Bound decoded payloads waiting for admission as well as actively encoded bytes.
        // Charging logical slices avoids counting the same backing Flight IPC body per column.
        let queued = pool
            .waiting
            .try_acquire_many(u32::try_from(input.div_ceil(UNIT).max(1))?)
            .map_err(|_| {
                pool.metrics.rejected.fetch_add(1, Relaxed);
                anyhow::anyhow!("diagnostic waiting-input byte limit reached")
            })?;
        let bytes = Arc::clone(&pool.bytes).acquire_many_owned(units).await?;
        drop(queued);
        let slot = Arc::clone(&pool.workers).acquire_owned().await?;
        pool.metrics.admission_wait_ns.fetch_add(ns(start.elapsed()), Relaxed);
        let pool = Arc::clone(pool);
        // The worker (including any acknowledged-create cleanup) retains both permits even if
        // Flight drops its waiter. Never block a Tokio worker or hold a blocking thread across HTTP.
        let upload = tokio::task::spawn_blocking(move || -> Result<_> {
            let payload = pool.prepare(batch, Output::new(bytes, input, MAX_OUTPUT))?;
            Ok(payload.map(|payload| {
                tokio::spawn(async move {
                    let _permits = (payload.credits, slot);
                    let target = pool.target.as_ref().context("missing diagnostic object store")?;
                    target.upload(target.key(), payload.data, rows, &pool.metrics).await
                })
            }))
        })
        .await
        .context("diagnostic encode/fsync worker")??;
        if let Some(upload) = upload {
            upload.await.context("diagnostic upload worker")??;
        }
        Ok(())
    }
}

struct WorkerPool {
    waiting: Semaphore,
    workers: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    scratch: Option<OwnedDir>,
    target: Option<Target>,
    metrics: Arc<Metrics>,
}

impl WorkerPool {
    fn configure(level: IsolationLevel, root: Option<&Path>, store: Option<&str>) -> Result<Self> {
        let (root, backend) = match level {
            IsolationLevel::L2 => (None, None),
            IsolationLevel::L3 => (Some(validate_root(root.context(SCRATCH)?)?), None),
            IsolationLevel::L4 => (
                Some(validate_root(root.context(SCRATCH)?)?),
                Some(store.context(OBJECT_STORE)?),
            ),
            _ => anyhow::bail!("not a physical isolation level"),
        };
        let metrics = Arc::new(Metrics::new(
            level,
            backend.map(|b| {
                if b == "local" {
                    "local_object_store"
                } else {
                    "azure_blob_object_store"
                }
            }),
        ));
        let scratch = root.map(|dir| OwnedDir::create(dir, "stage-", &metrics)).transpose()?;
        let target = root
            .zip(backend)
            .map(|(dir, backend)| Target::new(dir, backend, &metrics))
            .transpose()?;
        Ok(Self {
            waiting: Semaphore::new(BUDGET / UNIT),
            workers: Arc::new(Semaphore::new(WORKERS)),
            bytes: Arc::new(Semaphore::new(BUDGET / UNIT)),
            scratch,
            target,
            metrics,
        })
    }

    fn prepare(&self, batch: RecordBatch, output: Output) -> Result<Option<Output>> {
        let rows = batch.num_rows();
        let mut encoded = self
            .metrics
            .encode
            .measure(rows, || encode(&batch, output), |o| o.data.len())?;
        drop(batch);
        let Some(dir) = &self.scratch else { return Ok(None) };
        let len = encoded.data.len();
        let mut file = self.metrics.fsync.measure(
            rows,
            || {
                let mut file = tempfile::tempfile_in(&dir.path).context("create anonymous diagnostic file")?;
                file.write_all(&encoded.data).context("write diagnostic Parquet")?;
                file.sync_all().context("fsync diagnostic Parquet")?;
                Ok(file)
            },
            |_| len,
        )?;
        if self.target.is_none() {
            return Ok(None);
        }
        self.metrics.readback.measure(
            rows,
            || {
                file.rewind()?;
                file.read_exact(&mut encoded.data)
                    .context("read back diagnostic Parquet")?;
                Ok(())
            },
            |_| len,
        )?;
        Ok(Some(encoded))
    }
}

fn validate_root(path: &Path) -> Result<&Path> {
    ensure!(
        path.is_absolute() && path.file_name().is_some_and(|n| n == ROOT_NAME),
        "{SCRATCH} must be an absolute dedicated {ROOT_NAME} directory"
    );
    ensure!(
        path.canonicalize().context(SCRATCH)? == path && path.is_dir(),
        "{SCRATCH} must be an existing canonical directory, not a link"
    );
    for parent in path.ancestors() {
        ensure!(
            parent.file_name().is_none_or(|n| n != "_delta_log") && !parent.join("_delta_log").try_exists()?,
            "{SCRATCH} must not be in a Delta table or log"
        );
    }
    Ok(path)
}

struct OwnedDir {
    path: PathBuf,
    metrics: Arc<Metrics>,
}

impl OwnedDir {
    fn create(parent: &Path, prefix: &str, metrics: &Arc<Metrics>) -> Result<Self> {
        let mut builder = tempfile::Builder::new();
        builder.prefix(prefix).rand_bytes(16);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(fs::Permissions::from_mode(0o700));
        }
        let path = builder.tempdir_in(parent)?.keep();
        Ok(Self {
            path,
            metrics: Arc::clone(metrics),
        })
    }
}

impl Drop for OwnedDir {
    fn drop(&mut self) {
        // A foreign file, collision or ambiguous PUT must survive shutdown. Never remove_dir_all.
        if let Err(error) = fs::remove_dir(&self.path) {
            self.metrics.cleanup_errors.fetch_add(1, Relaxed);
            tracing::warn!(path = %self.path.display(), %error, "diagnostic scratch directory not removed");
        }
    }
}

struct Target {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    local: Option<OwnedDir>,
}

impl Target {
    fn new(root: &Path, spec: &str, metrics: &Arc<Metrics>) -> Result<Self> {
        if spec == "local" {
            let dir = OwnedDir::create(root, "objects-", metrics)?;
            let store = object_store::local::LocalFileSystem::new_with_prefix(&dir.path)?;
            return Ok(Self {
                store: Arc::new(store),
                prefix: ObjectPath::default(),
                local: Some(dir),
            });
        }
        // Only a dedicated container and fixed scratch prefix; never accept table URLs, credentials,
        // endpoints or arbitrary blob paths. The production Azure client authenticates the PUT.
        let account = spec
            .strip_prefix("abfss://arrowcol-isolation@")
            .and_then(|s| s.strip_suffix(".dfs.core.windows.net/arrowcol-isolation"));
        ensure!(
            account.is_some_and(
                |a| (3..=24).contains(&a.len()) && a.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
            ),
            "{OBJECT_STORE} must be local or a dedicated abfss://arrowcol-isolation@<account>.dfs.core.windows.net/arrowcol-isolation destination"
        );
        let store = arrow_deltalake::delta::object_store_for_url(&spec.parse()?)?;
        Ok(Self {
            store,
            prefix: ObjectPath::from(format!("{ROOT_NAME}/{}", Uuid::new_v4())),
            local: None,
        })
    }

    fn key(&self) -> ObjectPath {
        self.prefix.clone().join(format!("{}.parquet", Uuid::new_v4()))
    }

    async fn upload(&self, path: ObjectPath, data: Vec<u8>, rows: usize, metrics: &Metrics) -> Result<()> {
        let bytes = data.len();
        let start = Instant::now();
        let result = self.store.put_opts(&path, data.into(), PutMode::Create.into()).await;
        metrics
            .upload
            .record(rows, result.is_ok().then_some(bytes), start.elapsed(), None);
        self.finish_create(&path, result, bytes, metrics).await
    }

    async fn finish_create(
        &self,
        path: &ObjectPath,
        result: object_store::Result<object_store::PutResult>,
        bytes: usize,
        metrics: &Metrics,
    ) -> Result<()> {
        if let Err(error) = result {
            if !matches!(error, object_store::Error::AlreadyExists { .. }) {
                metrics.unconfirmed_puts.fetch_add(1, Relaxed);
            }
            return Err(error)
                .with_context(|| format!("diagnostic create was not acknowledged for {path}; no delete attempted"));
        }
        if self.local.is_none() {
            // The generic Azure delete is unconditional. Never race a replaced version; the
            // dedicated container's lifecycle policy owns remote retention, not this process.
            metrics.retained_objects.fetch_add(1, Relaxed);
            metrics.retained_bytes.fetch_add(bytes as u64, Relaxed);
        } else {
            let start = Instant::now();
            let deleted = self.store.delete(path).await;
            metrics.cleanup_wall_ns.fetch_add(ns(start.elapsed()), Relaxed);
            if let Err(error) = deleted {
                metrics.cleanup_errors.fetch_add(1, Relaxed);
                return Err(error).with_context(|| format!("diagnostic object deletion not confirmed for {path}"));
            }
            metrics.deleted_objects.fetch_add(1, Relaxed);
        }
        Ok(())
    }
}

struct Output {
    data: Vec<u8>,
    limit: usize,
    input: usize,
    credits: OwnedSemaphorePermit,
}

impl Output {
    fn new(credits: OwnedSemaphorePermit, input: usize, limit: usize) -> Self {
        Self {
            data: Vec::new(),
            limit,
            input,
            credits,
        }
    }

    // Footers (schema metadata, indexes and Bloom filters) need not be proportional to row bytes.
    // Reserve actual growth before allocating; do not wait while holding another writer's budget.
    fn fund(&mut self, capacity: usize) -> bool {
        let extra = (self.input + capacity)
            .div_ceil(UNIT)
            .saturating_sub(self.credits.num_permits());
        if extra == 0 {
            return true;
        }
        let Ok(credit) = Arc::clone(self.credits.semaphore()).try_acquire_many_owned(extra as u32) else {
            return false;
        };
        self.credits.merge(credit);
        true
    }
}

impl Write for Output {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.data.len().saturating_add(buf.len());
        if len > self.limit {
            return Err(io::Error::other("diagnostic Parquet output exceeds reserved bytes"));
        }
        if len > self.data.capacity() {
            let desired = len.max(self.data.capacity().saturating_mul(2)).min(self.limit);
            let capacity = if self.fund(desired) {
                desired
            } else if self.fund(len) {
                len
            } else {
                return Err(io::Error::other("diagnostic Parquet active byte budget exhausted"));
            };
            self.data
                .try_reserve_exact(capacity - self.data.len())
                .map_err(io::Error::other)?;
        }
        self.data.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode(batch: &RecordBatch, mut output: Output) -> Result<Output> {
    let properties = arrow_deltalake::delta::diagnostic_writer_properties(&batch.schema())?;
    let mut writer = ArrowWriter::try_new(&mut output, batch.schema(), Some(properties))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(output)
}

fn ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Serialize, Default)]
struct Counts {
    batches: u64,
    rows: u64,
    bytes: u64,
    wall_ns: u64,
    cpu_ns: Option<u64>,
    errors: u64,
}

struct Stage(Mutex<Counts>);

impl Stage {
    fn new(thread_cpu: bool) -> Self {
        Self(Mutex::new(Counts {
            cpu_ns: thread_cpu.then_some(0),
            ..Default::default()
        }))
    }

    fn measure<T>(&self, rows: usize, op: impl FnOnce() -> Result<T>, size: impl FnOnce(&T) -> usize) -> Result<T> {
        let wall = Instant::now();
        let cpu = ThreadTime::try_now().ok();
        let result = op();
        let elapsed = cpu.and_then(|start| start.try_elapsed().ok());
        self.record(rows, result.as_ref().ok().map(size), wall.elapsed(), elapsed);
        result
    }

    fn record(&self, rows: usize, bytes: Option<usize>, wall: Duration, cpu: Option<Duration>) {
        let mut stage = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        if let Some(bytes) = bytes {
            stage.batches += 1;
            stage.rows += rows as u64;
            stage.bytes += bytes as u64;
        } else {
            stage.errors += 1;
        }
        stage.wall_ns = stage.wall_ns.saturating_add(ns(wall));
        stage.cpu_ns = stage
            .cpu_ns
            .zip(cpu)
            .map(|(total, elapsed)| total.saturating_add(ns(elapsed)));
    }

    fn snapshot(&self) -> Value {
        json!(*self.0.lock().unwrap_or_else(|poison| poison.into_inner()))
    }
}

struct Metrics {
    generation: Uuid,
    level: IsolationLevel,
    backend: Option<&'static str>,
    encode: Stage,
    fsync: Stage,
    readback: Stage,
    upload: Stage,
    rejected: AtomicU64,
    admission_wait_ns: AtomicU64,
    cleanup_wall_ns: AtomicU64,
    cleanup_errors: AtomicU64,
    deleted_objects: AtomicU64,
    retained_objects: AtomicU64,
    retained_bytes: AtomicU64,
    unconfirmed_puts: AtomicU64,
}

impl Metrics {
    fn new(level: IsolationLevel, backend: Option<&'static str>) -> Self {
        Self {
            generation: Uuid::new_v4(),
            level,
            backend,
            encode: Stage::new(true),
            fsync: Stage::new(true),
            readback: Stage::new(true),
            upload: Stage::new(false),
            rejected: 0.into(),
            admission_wait_ns: 0.into(),
            cleanup_wall_ns: 0.into(),
            cleanup_errors: 0.into(),
            deleted_objects: 0.into(),
            retained_objects: 0.into(),
            retained_bytes: 0.into(),
            unconfirmed_puts: 0.into(),
        }
    }

    fn snapshot(&self) -> Value {
        json!({"generation": self.generation.to_string(), "level": format!("{:?}", self.level), "scope": "per_batch_physical_stage", "object_store": self.backend,
            "encode": self.encode.snapshot(), "fsync": (self.level != IsolationLevel::L2).then(|| self.fsync.snapshot()),
            "readback": (self.level == IsolationLevel::L4).then(|| self.readback.snapshot()), "upload": (self.level == IsolationLevel::L4).then(|| self.upload.snapshot()),
            "admission_wait_ns": self.admission_wait_ns.load(Relaxed), "rejected_batches": self.rejected.load(Relaxed),
            "cleanup_errors": self.cleanup_errors.load(Relaxed), "cleanup_wall_ns": self.cleanup_wall_ns.load(Relaxed),
            "deleted_objects": self.deleted_objects.load(Relaxed), "retained_objects": self.retained_objects.load(Relaxed),
            "retained_bytes": self.retained_bytes.load(Relaxed), "unconfirmed_puts": self.unconfirmed_puts.load(Relaxed)})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array, NullArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn batch() -> Result<RecordBatch> {
        Ok(RecordBatch::try_from_iter([(
            "n",
            Arc::new(Int32Array::from(vec![Some(8), None, Some(3)])) as ArrayRef,
        )])?)
    }

    fn fixture() -> Result<(tempfile::TempDir, PathBuf)> {
        let fixture = tempfile::tempdir_in(".")?;
        let root = fixture.path().canonicalize()?.join(ROOT_NAME);
        fs::create_dir(&root)?;
        Ok((fixture, root))
    }

    #[test]
    fn memory_cut_finishes_readable_parquet_and_propagates_footer_overflow() -> Result<()> {
        let batch = batch()?;
        let output = |limit| -> Result<Output> {
            let budget = Arc::new(Semaphore::new(MIB / UNIT));
            Ok(Output::new(budget.try_acquire_owned()?, 0, limit))
        };
        let bytes = encode(&batch, output(MIB)?)?.data;
        assert!(encode(&batch, output(bytes.len() - 1)?).is_err());
        let mut file = tempfile::tempfile_in(".")?;
        file.write_all(&bytes)?;
        file.rewind()?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
        assert_eq!(reader.metadata().file_metadata().num_rows(), 3);
        assert_eq!(reader.build()?.collect::<Result<Vec<_>, _>>()?, vec![batch]);
        Ok(())
    }

    #[tokio::test]
    async fn configured_cut_counts_only_completed_physical_stages_and_cleans_owned_files() -> Result<()> {
        let (_fixture, root) = fixture()?;
        fs::write(root.join("unrelated"), b"keep")?;
        for level in [IsolationLevel::L2, IsolationLevel::L3, IsolationLevel::L4] {
            let pool = WorkerPool::configure(
                level,
                (level != IsolationLevel::L2).then_some(root.as_path()),
                (level == IsolationLevel::L4).then_some("local"),
            )?;
            let sink = IsolationSink(Arc::new(pool));
            sink.accept(batch()?).await?;
            let s = sink.0.metrics.snapshot();
            let bytes = s["encode"]["bytes"].as_u64().context("missing Parquet length")?;
            assert!(bytes > 0);
            for (name, expected) in [
                ("encode", true),
                ("fsync", level != IsolationLevel::L2),
                ("readback", level == IsolationLevel::L4),
                ("upload", level == IsolationLevel::L4),
            ] {
                if expected {
                    assert_eq!(s[name]["rows"], 3);
                    assert_eq!(s[name]["batches"], 1);
                    assert_eq!(s[name]["errors"], 0);
                    assert_eq!(s[name]["bytes"], bytes);
                } else {
                    assert!(s[name].is_null());
                }
            }
            assert_eq!(s["deleted_objects"], u64::from(level == IsolationLevel::L4));
            assert!(s["upload"]["cpu_ns"].is_null());
            drop(sink);
            assert_eq!(fs::read_dir(&root)?.count(), 1);
        }
        assert_eq!(fs::read(root.join("unrelated"))?, b"keep");
        Ok(())
    }

    #[tokio::test]
    async fn collision_does_not_overwrite_delete_or_claim_cleanup() -> Result<()> {
        let (_fixture, root) = fixture()?;
        let pool = WorkerPool::configure(IsolationLevel::L4, Some(&root), Some("local"))?;
        let target = pool.target.as_ref().context("target")?;
        let path = target.key();
        let collision = target.local.as_ref().context("local scratch")?.path.join(path.as_ref());
        fs::write(&collision, b"another owner")?;
        assert!(target.upload(path, vec![4, 5], 2, &pool.metrics).await.is_err());
        let stats = Arc::clone(&pool.metrics);
        drop(pool);
        assert_eq!(fs::read(collision)?, b"another owner");
        let s = stats.snapshot();
        assert_eq!(s["upload"]["errors"], 1);
        assert_eq!(s["upload"]["batches"], 0);
        assert_eq!(s["deleted_objects"], 0);
        assert_eq!(s["cleanup_errors"], 1); // The nonempty scratch directory was preserved.
        Ok(())
    }

    #[tokio::test]
    async fn ambiguous_put_does_not_authorize_deletion_even_when_object_is_present() -> Result<()> {
        let (_fixture, root) = fixture()?;
        let pool = WorkerPool::configure(IsolationLevel::L4, Some(&root), Some("local"))?;
        let target = pool.target.as_ref().context("target")?;
        let key = target.key();
        target
            .store
            .put_opts(&key, b"orphan".to_vec().into(), PutMode::Create.into())
            .await?;
        let response_lost = object_store::Error::Generic {
            store: "test",
            source: io::Error::new(io::ErrorKind::TimedOut, "create response lost").into(),
        };
        assert!(
            target
                .finish_create(&key, Err(response_lost), 6, &pool.metrics)
                .await
                .is_err()
        );
        assert_eq!(target.store.get(&key).await?.bytes().await?.as_ref(), b"orphan");
        assert_eq!(pool.metrics.unconfirmed_puts.load(Relaxed), 1);
        assert_eq!(pool.metrics.deleted_objects.load(Relaxed), 0);
        Ok(())
    }

    #[tokio::test]
    async fn acknowledged_remote_object_is_retained_not_reported_deleted() -> Result<()> {
        let metrics = Metrics::new(IsolationLevel::L4, Some("azure_blob_object_store"));
        let target = Target {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: ObjectPath::from("arrowcol-isolation/test"),
            local: None,
        };
        let key = target.key();
        target.upload(key.clone(), vec![1, 2, 3], 4, &metrics).await?;
        assert_eq!(target.store.get(&key).await?.bytes().await?.as_ref(), [1, 2, 3]);
        let s = metrics.snapshot();
        assert_eq!(s["retained_objects"], 1);
        assert_eq!(s["retained_bytes"], 3);
        assert_eq!(s["deleted_objects"], 0);
        assert_eq!(s["upload"]["rows"], 4);
        Ok(())
    }

    #[tokio::test]
    async fn fsync_path_failure_never_reaches_upload() -> Result<()> {
        let (_fixture, root) = fixture()?;
        let sink = IsolationSink(Arc::new(WorkerPool::configure(
            IsolationLevel::L4,
            Some(&root),
            Some("local"),
        )?));
        let stage = &sink.0.scratch.as_ref().context("stage")?.path;
        fs::remove_dir(stage)?;
        assert!(sink.accept(batch()?).await.is_err());
        fs::create_dir(stage)?;
        let s = sink.0.metrics.snapshot();
        assert_eq!(s["encode"]["batches"], 1);
        assert_eq!(s["fsync"]["errors"], 1);
        assert_eq!(s["upload"]["batches"], 0);
        assert_eq!(s["upload"]["errors"], 0);
        Ok(())
    }

    #[test]
    fn refuses_table_scratch_ambiguous_destinations_and_nonphysical_levels() -> Result<()> {
        let (_fixture, root) = fixture()?;
        for spec in [
            None,
            Some(""),
            Some("/production"),
            Some("abfss://tables@acct.dfs.core.windows.net/arrowcol-isolation"),
            Some("abfss://arrowcol-isolation@acct.dfs.core.windows.net/_delta_log"),
        ] {
            assert!(WorkerPool::configure(IsolationLevel::L4, Some(&root), spec).is_err());
        }
        assert_eq!(fs::read_dir(&root)?.count(), 0);
        fs::create_dir(root.parent().context("fixture parent")?.join("_delta_log"))?;
        assert!(WorkerPool::configure(IsolationLevel::L3, Some(&root), None).is_err());
        assert!(WorkerPool::configure(IsolationLevel::L2, Some(&root), Some("invalid")).is_ok());
        for level in [IsolationLevel::L0, IsolationLevel::L1, IsolationLevel::L5] {
            assert!(IsolationSink::new(level).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn large_schema_footer_can_borrow_capacity_beyond_small_input_estimate() -> Result<()> {
        let sink = IsolationSink(Arc::new(WorkerPool::configure(IsolationLevel::L2, None, None)?));
        let mut batch = batch()?;
        batch
            .schema_metadata_mut()
            .insert("wide_metadata".into(), "x".repeat(MIB));
        sink.accept(batch).await?;
        assert!(
            sink.0.metrics.snapshot()["encode"]["bytes"]
                .as_u64()
                .context("Parquet length")?
                > MIB as u64
        );
        assert_eq!(sink.0.bytes.available_permits(), BUDGET / UNIT);
        Ok(())
    }

    #[tokio::test]
    async fn shared_backing_allocation_is_not_charged_once_per_column() -> Result<()> {
        let sink = IsolationSink(Arc::new(WorkerPool::configure(IsolationLevel::L2, None, None)?));
        let field: ArrayRef = Arc::new(Int32Array::from(vec![7; MIB]).slice(0, 1));
        let batch = RecordBatch::try_from_iter((0..17).map(|i| (format!("col{i}"), Arc::clone(&field))))?;
        assert!(batch.get_array_memory_size() > MAX_INPUT);
        sink.accept(batch).await?;
        assert_eq!(sink.0.metrics.snapshot()["encode"]["rows"], 1);
        Ok(())
    }

    #[tokio::test]
    async fn oversized_and_cancelled_waiters_do_not_start_encoding_or_leak_permits() -> Result<()> {
        let sink = IsolationSink(Arc::new(WorkerPool::configure(IsolationLevel::L2, None, None)?));
        let many_rows = RecordBatch::try_from_iter([("n", Arc::new(NullArray::new(MAX_ROWS + 1)) as ArrayRef)])?;
        assert!(sink.accept(many_rows).await.is_err());
        let full = sink.0.waiting.acquire_many((BUDGET / UNIT) as u32).await?;
        assert!(sink.accept(batch()?).await.is_err());
        drop(full);
        let slots = sink.0.workers.acquire_many(WORKERS as u32).await?;
        let mut waiting = Box::pin(sink.accept(batch()?));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        assert!(sink.0.bytes.available_permits() < BUDGET / UNIT);
        drop(waiting);
        assert_eq!(sink.0.bytes.available_permits(), BUDGET / UNIT);
        drop(slots);
        assert_eq!(sink.0.metrics.snapshot()["encode"]["batches"], 0);
        sink.accept(batch()?).await
    }
}
