//! Process-wide Kernel I/O runtime, independent of the caller's Tokio runtime.

#[cfg(target_os = "linux")]
mod topology;

use crate::{CpuDispatch, CpuPlacement};
use anyhow::{Context, Result};
use delta_kernel::{DeltaResult, Error};
use delta_kernel_default_engine::executor::TaskExecutor;
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, mpsc};
use tokio::runtime::{EnterGuard, Runtime};

const UPLOAD_BUFFER_MEMORY_BUDGET_BYTES: usize = 1024 * 1024 * 1024;
const UPLOAD_COPY_BUFFER_BYTES: usize = 64 * 1024;
const DEFAULT_UPLOAD_BUFFER_CAPACITY_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_UPLOAD_REQUEST_CONCURRENCY: usize = 2;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadMode {
    #[default]
    PutCreate,
    BufferedMultipart,
}

impl UploadMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PutCreate => "put_create",
            Self::BufferedMultipart => "buffered_multipart",
        }
    }
}

/// Process-wide collector runtime settings, resolved once before starting workers.
///
/// Each field is overridden by `ARROWCOL_COLLECTOR_<UPPERCASE_FIELD_NAME>`.
/// Invalid, zero, non-Unicode, and out-of-range values fail initialization rather
/// than being silently clamped. Defaults alone are clamped to machine size.
/// Deserialized settings must pass [`Self::validate`] before use.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaRuntimeConfig {
    /// Raw timestamp and nanosecond companion conversion.
    #[serde(default)]
    pub timestamp_kernel: crate::TimestampKernel,
    /// Async I/O workers: default cores / 4, clamped to 2..=16; allowed 1..=64.
    pub runtime_workers: usize,
    /// Blocking I/O workers: default cores * 2, clamped to 32..=128; allowed 2..=256.
    pub blocking_workers: usize,
    /// Blocking workers reserved for filesystem handoffs: default 1.
    #[serde(default = "default_blocking_fs_reserve")]
    pub blocking_fs_reserve: usize,
    /// Detach sorted input from its parent allocation above this charged/logical size ratio.
    #[serde(default = "default_detach_ratio")]
    pub layout_detach_ratio: usize,
    /// Concurrent uploads: default 32 independent of CPU count; allowed 1..=256.
    pub upload_limit: usize,
    /// Upload strategy: default `put_create`; `buffered_multipart` is opt-in and uses UUID paths only.
    pub upload_mode: UploadMode,
    /// File-backed staging by default; streaming is for remote multipart output.
    #[serde(default)]
    pub staging_mode: crate::StagingMode,
    /// Per-upload buffer and multipart chunk size: default 8 MiB; allowed 1..=64 MiB when multipart is enabled.
    pub upload_buffer_capacity_bytes: usize,
    /// In-flight multipart requests per upload: default 2; allowed 1..=16.
    pub upload_request_concurrency: usize,
    /// Admitted Kernel tasks: default cores * 4, clamped to 64..=256; allowed 2..=1024.
    pub task_limit: usize,
    /// Kernel read-ahead batches: default cores, clamped to 8..=16; allowed 1..=128.
    pub read_ahead: usize,
    /// Rows per Kernel batch: default cores * 1024, clamped to 4096..=32768;
    /// allowed 1..=262144.
    pub batch_rows: usize,
    /// Dedicated CPU workers: default cores - 1, clamped to 1..=32; allowed 1..=128.
    pub cpu_workers: usize,
    #[serde(default)]
    pub cpu_dispatch: CpuDispatch,
    #[serde(default)]
    pub cpu_placement: CpuPlacement,
    /// Maximum concurrent Parquet leaf-column encoders; 1 preserves AsyncArrowWriter.
    #[serde(default = "default_parquet_column_workers")]
    pub parquet_column_workers: u8,
    /// Column waves by default; the pipeline retains a bounded window of input slices.
    #[serde(default)]
    pub parquet_encode_mode: crate::ParquetEncodeMode,
    /// CPU queue capacity, 1..=128; absent means the resolved CPU worker count.
    #[serde(default)]
    pub cpu_queue_jobs: Option<usize>,
    /// Partition grouping algorithm; the default preserves row-wise grouping.
    #[serde(default)]
    pub partition_grouping: crate::PartitionGrouping,
    #[serde(default)]
    pub event_interval: Option<usize>,
    #[serde(default)]
    pub global_queue_interval: Option<usize>,
    #[serde(default)]
    pub thread_stack_size: Option<usize>,
    #[serde(default)]
    pub thread_keep_alive_ms: Option<usize>,
    #[serde(default)]
    pub cpu_thread_stack_size: Option<usize>,
    /// Skip row-wise grouping for internally generated single ingestion partitions.
    #[serde(default)]
    pub generated_partition_fast_path: bool,
    /// Row binding policy; does not enable the grouping fast path.
    #[serde(default)]
    pub generated_partition_binding: crate::GeneratedPartitionBinding,
    /// Reuse the bound logical projection and evaluator within one file.
    #[serde(default)]
    pub file_mapping_cache: bool,
}

const fn default_detach_ratio() -> usize {
    2
}

fn default_blocking_fs_reserve() -> usize {
    1
}

fn default_parquet_column_workers() -> u8 {
    1
}

impl Default for DeltaRuntimeConfig {
    fn default() -> Self {
        Self::for_cores(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get))
    }
}

impl DeltaRuntimeConfig {
    fn for_cores(cores: usize) -> Self {
        Self {
            timestamp_kernel: crate::TimestampKernel::Reference,
            runtime_workers: (cores / 4).clamp(2, 16),
            blocking_workers: cores.saturating_mul(2).clamp(32, 128),
            blocking_fs_reserve: 1,
            layout_detach_ratio: default_detach_ratio(),
            upload_limit: 32,
            upload_mode: UploadMode::PutCreate,
            staging_mode: Default::default(),
            upload_buffer_capacity_bytes: DEFAULT_UPLOAD_BUFFER_CAPACITY_BYTES,
            upload_request_concurrency: DEFAULT_UPLOAD_REQUEST_CONCURRENCY,
            task_limit: cores.saturating_mul(4).clamp(64, 256),
            read_ahead: cores.clamp(8, 16),
            batch_rows: cores.saturating_mul(1024).clamp(4096, 32768),
            cpu_workers: cores.saturating_sub(1).clamp(1, 32),
            cpu_dispatch: CpuDispatch::default(),
            cpu_placement: CpuPlacement::default(),
            parquet_column_workers: default_parquet_column_workers(),
            parquet_encode_mode: crate::ParquetEncodeMode::default(),
            cpu_queue_jobs: None,
            partition_grouping: crate::PartitionGrouping::default(),
            event_interval: None,
            global_queue_interval: None,
            thread_stack_size: None,
            thread_keep_alive_ms: None,
            cpu_thread_stack_size: None,
            generated_partition_fast_path: false,
            file_mapping_cache: false,
            generated_partition_binding: crate::GeneratedPartitionBinding::RevalidateRows,
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
        let mut upload_buffer_overridden = false;
        let mut upload_request_concurrency_overridden = false;
        for (name, value) in [
            ("RUNTIME_WORKERS", &mut config.runtime_workers),
            ("BLOCKING_WORKERS", &mut config.blocking_workers),
            ("BLOCKING_FS_RESERVE", &mut config.blocking_fs_reserve),
            ("LAYOUT_DETACH_RATIO", &mut config.layout_detach_ratio),
            ("UPLOAD_LIMIT", &mut config.upload_limit),
            ("UPLOAD_BUFFER_CAPACITY_BYTES", &mut config.upload_buffer_capacity_bytes),
            ("UPLOAD_REQUEST_CONCURRENCY", &mut config.upload_request_concurrency),
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
                if name.ends_with("UPLOAD_BUFFER_CAPACITY_BYTES") {
                    upload_buffer_overridden = true;
                }
                if name.ends_with("UPLOAD_REQUEST_CONCURRENCY") {
                    upload_request_concurrency_overridden = true;
                }
                *value = raw.parse().with_context(|| format!("parse {name}"))?;
            }
        }
        let name = "ARROWCOL_COLLECTOR_PARQUET_COLUMN_WORKERS";
        if let Some(raw) = lookup(name)? {
            anyhow::ensure!(
                !raw.is_empty() && raw.bytes().all(|byte| byte.is_ascii_digit()),
                "{name} must be an unsigned decimal integer"
            );
            config.parquet_column_workers = raw.parse().with_context(|| format!("parse {name}"))?;
        }
        let name = "ARROWCOL_COLLECTOR_PARQUET_ENCODE_MODE";
        if let Some(raw) = lookup(name)? {
            config.parquet_encode_mode = raw.try_into().map_err(|why: &str| anyhow::anyhow!("{name}: {why}"))?;
        }
        let name = "ARROWCOL_COLLECTOR_CPU_QUEUE_JOBS";
        if let Some(raw) = lookup(name)? {
            anyhow::ensure!(
                !raw.is_empty() && raw.bytes().all(|byte| byte.is_ascii_digit()),
                "{name} must be an unsigned decimal integer"
            );
            config.cpu_queue_jobs = Some(raw.parse().with_context(|| format!("parse {name}"))?);
        }
        let name = "ARROWCOL_COLLECTOR_TIMESTAMP_KERNEL";
        if let Some(raw) = lookup(name)? {
            config.timestamp_kernel =
                serde_json::from_value(serde_json::Value::String(raw)).with_context(|| format!("parse {name}"))?;
        }
        let name = "ARROWCOL_COLLECTOR_CPU_DISPATCH";
        if let Some(raw) = lookup(name)? {
            config.cpu_dispatch = CpuDispatch::try_from(raw).map_err(|why| anyhow::anyhow!("{name}: {why}"))?;
        }
        let name = "ARROWCOL_COLLECTOR_CPU_PLACEMENT";
        if let Some(raw) = lookup(name)? {
            config.cpu_placement = CpuPlacement::try_from(raw).map_err(|why| anyhow::anyhow!("{name}: {why}"))?;
        }
        for (suffix, value) in [
            ("EVENT_INTERVAL", &mut config.event_interval),
            ("GLOBAL_QUEUE_INTERVAL", &mut config.global_queue_interval),
            ("THREAD_STACK_SIZE", &mut config.thread_stack_size),
            ("THREAD_KEEP_ALIVE_MS", &mut config.thread_keep_alive_ms),
            ("CPU_THREAD_STACK_SIZE", &mut config.cpu_thread_stack_size),
        ] {
            let name = format!("ARROWCOL_COLLECTOR_{suffix}");
            if let Some(raw) = lookup(&name)? {
                anyhow::ensure!(
                    !raw.is_empty() && raw.bytes().all(|byte| byte.is_ascii_digit()),
                    "{name} must be an unsigned decimal integer"
                );
                *value = Some(raw.parse().with_context(|| format!("parse {name}"))?);
            }
        }
        let name = "ARROWCOL_COLLECTOR_UPLOAD_STAGING_MODE";
        if let Some(raw) = lookup(name)? {
            config.staging_mode = raw
                .try_into()
                .map_err(|message: &str| anyhow::anyhow!("{name}: {message}"))?;
        }
        let mode_name = "ARROWCOL_COLLECTOR_UPLOAD_MODE";
        if let Some(raw) = lookup(mode_name)? {
            config.upload_mode = match raw.as_str() {
                "put_create" => UploadMode::PutCreate,
                "buffered_multipart" => UploadMode::BufferedMultipart,
                _ => anyhow::bail!("{mode_name} must be 'put_create' or 'buffered_multipart', got {raw:?}"),
            };
        }
        let name = "ARROWCOL_COLLECTOR_PARTITION_GROUPING";
        if let Some(raw) = lookup(name)? {
            config.partition_grouping = raw
                .try_into()
                .map_err(|message: &str| anyhow::anyhow!("{name}: {message}"))?;
        }
        let name = "ARROWCOL_COLLECTOR_GENERATED_PARTITION_BINDING";
        if let Some(raw) = lookup(name)? {
            config.generated_partition_binding = raw.try_into().context(name)?;
        }
        let name = "ARROWCOL_COLLECTOR_GENERATED_PARTITION_FAST_PATH";
        if let Some(raw) = lookup(name)? {
            config.generated_partition_fast_path = match raw.as_str() {
                "true" => true,
                "false" => false,
                _ => anyhow::bail!("{name} must be 'true' or 'false', got {raw:?}"),
            };
        }
        let name = "ARROWCOL_COLLECTOR_FILE_MAPPING_CACHE";
        if let Some(raw) = lookup(name)? {
            config.file_mapping_cache = raw
                .parse::<bool>()
                .with_context(|| format!("{name} must be true or false"))?;
        }
        if config.upload_mode == UploadMode::PutCreate {
            anyhow::ensure!(
                !upload_buffer_overridden && !upload_request_concurrency_overridden,
                "ARROWCOL_COLLECTOR_UPLOAD_BUFFER_CAPACITY_BYTES and ARROWCOL_COLLECTOR_UPLOAD_REQUEST_CONCURRENCY require ARROWCOL_COLLECTOR_UPLOAD_MODE=buffered_multipart"
            );
        }
        config.cpu_queue_jobs = Some(config.cpu_queue_jobs.unwrap_or(config.cpu_workers));
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
            ("LAYOUT_DETACH_RATIO", self.layout_detach_ratio, 1, 8),
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
        anyhow::ensure!(
            (1..=16).contains(&self.parquet_column_workers),
            "ARROWCOL_COLLECTOR_PARQUET_COLUMN_WORKERS must be in 1..=16, got {}",
            self.parquet_column_workers
        );
        anyhow::ensure!(
            self.blocking_fs_reserve > 0 && self.blocking_fs_reserve < self.blocking_workers,
            "ARROWCOL_COLLECTOR_BLOCKING_FS_RESERVE must be positive and less than BLOCKING_WORKERS"
        );
        anyhow::ensure!(
            self.parquet_encode_mode != crate::ParquetEncodeMode::ColumnPipeline || self.parquet_column_workers > 1,
            "ARROWCOL_COLLECTOR_PARQUET_ENCODE_MODE=column_pipeline requires PARQUET_COLUMN_WORKERS > 1"
        );
        if let Some(queue) = self.cpu_queue_jobs {
            anyhow::ensure!(
                (1..=128).contains(&queue),
                "ARROWCOL_COLLECTOR_CPU_QUEUE_JOBS must be in 1..=128"
            );
        }
        for (name, value, min, max) in [
            ("EVENT_INTERVAL", self.event_interval, 1, 65_535),
            ("GLOBAL_QUEUE_INTERVAL", self.global_queue_interval, 1, 65_535),
            ("THREAD_STACK_SIZE", self.thread_stack_size, 65_536, 67_108_864),
            ("THREAD_KEEP_ALIVE_MS", self.thread_keep_alive_ms, 1, 86_400_000),
            ("CPU_THREAD_STACK_SIZE", self.cpu_thread_stack_size, 65_536, 67_108_864),
        ] {
            if let Some(value) = value {
                anyhow::ensure!(
                    (min..=max).contains(&value),
                    "ARROWCOL_COLLECTOR_{name} must be in {min}..={max}, got {value}"
                );
            }
        }
        if self.staging_mode == crate::StagingMode::Streaming {
            anyhow::ensure!(
                self.upload_mode == UploadMode::BufferedMultipart
                    && (self.parquet_column_workers == 1
                        || self.parquet_encode_mode == crate::ParquetEncodeMode::ColumnPipeline),
                "streaming staging requires buffered_multipart and either parquet_column_workers=1 or parquet_encode_mode=column_pipeline"
            );
        }
        if self.upload_mode == UploadMode::BufferedMultipart {
            anyhow::ensure!(
                (1024 * 1024..=64 * 1024 * 1024).contains(&self.upload_buffer_capacity_bytes),
                "ARROWCOL_COLLECTOR_UPLOAD_BUFFER_CAPACITY_BYTES must be in 1048576..=67108864, got {}",
                self.upload_buffer_capacity_bytes
            );
            anyhow::ensure!(
                (1..=16).contains(&self.upload_request_concurrency),
                "ARROWCOL_COLLECTOR_UPLOAD_REQUEST_CONCURRENCY must be in 1..=16, got {}",
                self.upload_request_concurrency
            );
            let bridge_bytes = if self.staging_mode == crate::StagingMode::Streaming
                && self.parquet_encode_mode == crate::ParquetEncodeMode::ColumnPipeline
            {
                crate::streaming_columns::BRIDGE_BUFFER_BYTES
            } else {
                0
            };
            let in_flight = self
                .upload_buffer_capacity_bytes
                .checked_mul(
                    self.upload_request_concurrency
                        .checked_add(1)
                        .context("upload request concurrency overflow")?,
                )
                .and_then(|bytes| bytes.checked_add(UPLOAD_COPY_BUFFER_BYTES))
                .and_then(|bytes| bytes.checked_add(bridge_bytes))
                .context("upload buffering size overflow")?;
            let total = self
                .upload_limit
                .checked_mul(in_flight)
                .context("aggregate upload buffering size overflow")?;
            anyhow::ensure!(
                total <= UPLOAD_BUFFER_MEMORY_BUDGET_BYTES,
                "aggregate upload buffering must not exceed {} bytes, got {total}",
                UPLOAD_BUFFER_MEMORY_BUDGET_BYTES
            );
        } else {
            anyhow::ensure!(
                self.upload_buffer_capacity_bytes == DEFAULT_UPLOAD_BUFFER_CAPACITY_BYTES
                    && self.upload_request_concurrency == DEFAULT_UPLOAD_REQUEST_CONCURRENCY,
                "buffer capacity and request concurrency are only valid with buffered_multipart mode"
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
        .get_or_init(|| {
            let config = DeltaRuntimeConfig::from_env().map_err(|error| format!("{error:#}"))?;
            if config.cpu_dispatch != CpuDispatch::SharedFifo || config.cpu_placement != CpuPlacement::Os {
                // Do not advertise an opt-in CPU policy until every worker acknowledged startup.
                configured_cpu_pool(&config, "runtime_initialization").map_err(|error| format!("{error:#}"))?;
            }
            Ok(config)
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!("{error}"))
}

type CpuJob = Box<dyn FnOnce() + Send + 'static>;

struct CpuPool {
    queues: CpuQueues,
    worker_topology: Vec<WorkerLocation>,
}

#[derive(Default, Serialize)]
struct WorkerLocation {
    cpu: Option<usize>,
    package: Option<usize>,
    core: Option<usize>,
}

type ShardJob = (tokio::sync::OwnedSemaphorePermit, CpuJob);

enum CpuQueues {
    Shared(tokio::sync::mpsc::Sender<CpuJob>),
    Shards {
        senders: Vec<mpsc::SyncSender<ShardJob>>,
        next: AtomicUsize,
        slots: Arc<tokio::sync::Semaphore>,
    },
}

enum WorkerQueue {
    Shared(Arc<Mutex<tokio::sync::mpsc::Receiver<CpuJob>>>),
    Shard(mpsc::Receiver<ShardJob>),
}

impl WorkerQueue {
    fn receive(&mut self) -> Option<CpuJob> {
        match self {
            Self::Shared(rx) => rx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .blocking_recv(),
            Self::Shard(rx) => rx.recv().ok().map(|(_queued_slot, job)| job),
        }
    }
}

impl CpuQueues {
    fn new(dispatch: CpuDispatch, workers: usize, capacity: usize) -> (Self, Vec<WorkerQueue>) {
        match dispatch {
            CpuDispatch::SharedFifo => {
                let (tx, rx) = tokio::sync::mpsc::channel(capacity);
                let rx = Arc::new(Mutex::new(rx));
                (
                    Self::Shared(tx),
                    (0..workers).map(|_| WorkerQueue::Shared(Arc::clone(&rx))).collect(),
                )
            }
            CpuDispatch::BoundedShards => {
                let (senders, receivers) = (0..workers)
                    .map(|_| {
                        let (tx, rx) = mpsc::sync_channel(capacity);
                        (tx, WorkerQueue::Shard(rx))
                    })
                    .unzip();
                (
                    Self::Shards {
                        senders,
                        next: AtomicUsize::new(0),
                        slots: Arc::new(tokio::sync::Semaphore::new(capacity)),
                    },
                    receivers,
                )
            }
        }
    }

    async fn send(&self, job: CpuJob) -> Result<()> {
        match self {
            Self::Shared(tx) => tx
                .send(job)
                .await
                .map_err(|_| anyhow::anyhow!("Delta CPU pool stopped")),
            Self::Shards { senders, next, slots } => {
                // All shards share one queue budget. Even a hot shard can accept any
                // job holding a permit without waiting or blocking an async caller.
                let permit = Arc::clone(slots).acquire_owned().await?;
                let shard = next.fetch_add(1, AtomicOrdering::Relaxed) % senders.len();
                senders[shard]
                    .try_send((permit, job))
                    .map_err(|_| anyhow::anyhow!("Delta CPU shard unavailable"))
            }
        }
    }

    fn count(&self) -> usize {
        match self {
            Self::Shared(_) => 1,
            Self::Shards { senders, .. } => senders.len(),
        }
    }
}

#[cfg(target_os = "linux")]
fn effective_cpus() -> Result<Vec<usize>> {
    use rustix::thread::{CpuSet, sched_getaffinity};
    let mask = sched_getaffinity(None).context("read effective CPU affinity")?;
    let cpus: Vec<_> = (0..CpuSet::MAX_CPU).filter(|&id| mask.is_set(id)).collect();
    anyhow::ensure!(!cpus.is_empty(), "effective CPU affinity is empty");
    Ok(cpus)
}

fn worker_targets(placement: CpuPlacement, workers: usize) -> Result<Vec<Option<usize>>> {
    if placement == CpuPlacement::Os {
        return Ok(vec![None; workers]);
    }
    #[cfg(target_os = "linux")]
    {
        let allowed = effective_cpus()?;
        let allowed = if placement == CpuPlacement::PhysicalFirst {
            topology::physical_first(topology::CPU_ROOT.as_ref(), &allowed)?
        } else {
            allowed
        };
        Ok((0..workers)
            .map(|i| Some(allowed[placement_index(placement, i, workers, allowed.len())]))
            .collect())
    }
    #[cfg(not(target_os = "linux"))]
    anyhow::bail!("ARROWCOL_COLLECTOR_CPU_PLACEMENT requires Linux for explicit placement")
}

#[cfg(target_os = "linux")]
fn placement_index(policy: CpuPlacement, worker: usize, workers: usize, allowed: usize) -> usize {
    match policy {
        CpuPlacement::ScatterAllowed => (worker % allowed) * allowed / workers.min(allowed),
        CpuPlacement::CompactAllowed | CpuPlacement::PhysicalFirst => worker % allowed,
        CpuPlacement::Os => unreachable!("OS placement does not select CPUs"),
    }
}

fn start_on_cpu(target: Option<usize>, placement: CpuPlacement) -> Result<WorkerLocation> {
    #[cfg(target_os = "linux")]
    {
        use rustix::thread::{CpuSet, sched_getaffinity, sched_setaffinity};
        if let Some(cpu) = target {
            let mut requested = CpuSet::new();
            requested.set(cpu);
            sched_setaffinity(None, &requested).with_context(|| format!("pin CPU worker to {cpu}"))?;
            let observed = sched_getaffinity(None).context("verify CPU worker affinity")?;
            anyhow::ensure!(observed == requested, "CPU worker affinity mismatch for CPU {cpu}");
        }
        let cpu = rustix::thread::sched_getcpu();
        anyhow::ensure!(
            target.is_none_or(|id| id == cpu),
            "CPU worker running on unexpected CPU {cpu}"
        );
        let core = topology::core(topology::CPU_ROOT.as_ref(), cpu);
        let core = if placement == CpuPlacement::PhysicalFirst {
            Some(core?)
        } else {
            core.ok()
        };
        Ok(WorkerLocation {
            cpu: Some(cpu),
            package: core.map(|p| p.0),
            core: core.map(|p| p.1),
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        anyhow::ensure!(
            target.is_none() && placement == CpuPlacement::Os,
            "explicit CPU placement requires Linux"
        );
        Ok(WorkerLocation::default())
    }
}

thread_local! {
    static CPU_WORKER: Cell<bool> = const { Cell::new(false) };
}

impl CpuPool {
    #[cfg(test)]
    fn new(workers: usize, queue_jobs: usize, stack_size: Option<usize>) -> Result<Self> {
        Self::start(
            workers,
            queue_jobs,
            stack_size,
            CpuDispatch::SharedFifo,
            CpuPlacement::Os,
        )
    }

    fn start(
        workers: usize,
        queue_jobs: usize,
        stack_size: Option<usize>,
        dispatch: CpuDispatch,
        placement: CpuPlacement,
    ) -> Result<Self> {
        let targets = worker_targets(placement, workers)?;
        let (queues, receivers) = CpuQueues::new(dispatch, workers, queue_jobs);
        let mut worker_topology = Vec::with_capacity(workers);
        for (index, (mut receiver, target)) in receivers.into_iter().zip(targets).enumerate() {
            let (started, ready) = mpsc::sync_channel(1);
            let mut builder = std::thread::Builder::new().name(format!("delta-cpu-{index}"));
            if let Some(size) = stack_size {
                builder = builder.stack_size(size);
            }
            builder
                .spawn(move || {
                    let result = start_on_cpu(target, placement);
                    let ok = result.is_ok();
                    if started.send(result).is_err() || !ok {
                        return;
                    }
                    CPU_WORKER.set(true);
                    while let Some(job) = receiver.receive() {
                        job();
                    }
                })
                .context("create Delta CPU worker")?;
            worker_topology.push(ready.recv().context("CPU worker stopped during startup")??);
        }
        Ok(Self {
            queues,
            worker_topology,
        })
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
        self.queues
            .send(Box::new(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task));
                let _ = sender.send(result);
            }))
            .await?;
        match receiver.await.context("Delta CPU worker stopped")? {
            Ok(output) => Ok(output),
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

static CPU_POOL: OnceLock<std::result::Result<CpuPool, String>> = OnceLock::new();

fn cpu_pool() -> Result<&'static CpuPool> {
    configured_cpu_pool(runtime_config()?, "lazy_pool_initialization")
}

fn configured_cpu_pool(config: &DeltaRuntimeConfig, scope: &str) -> Result<&'static CpuPool> {
    let workers = config.cpu_workers;
    let queue_jobs = config.cpu_queue_jobs.unwrap_or(workers);
    match CPU_POOL.get_or_init(|| {
        let pool = CpuPool::start(
            workers,
            queue_jobs,
            config.cpu_thread_stack_size,
            config.cpu_dispatch,
            config.cpu_placement,
        )
        .map_err(|error| format!("{error:#}"))?;
        eprintln!(
            "arrowcol_effective_config {}",
            serde_json::json!({"cpu_pool":{
                "thread_stack_size":config.cpu_thread_stack_size, "scope":scope,
                "cpu_dispatch":config.cpu_dispatch, "cpu_placement":config.cpu_placement,
                "shard_count":pool.queues.count(), "queue_jobs":queue_jobs,
                "workers_started":pool.worker_topology.len(), "worker_topology":pool.worker_topology,
                "worker_cpu_ids":pool.worker_topology.iter().map(|worker| worker.cpu).collect::<Vec<_>>(),
            }})
        );
        Ok(pool)
    }) {
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
/// Synchronous encoding runs on the CPU pool. A pending I/O poll returns its
/// worker to the pool; the next poll is admitted only after a wakeup and may
/// run on a different worker. For fallible work use `run_cpu_future(future).await??`.
///
/// # Errors
/// Returns configuration or CPU/I/O runtime initialization failure.
pub async fn run_cpu_future<F, T>(future: F) -> Result<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    struct Wake(tokio::sync::Notify);
    impl futures::task::ArcWake for Wake {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.notify_one();
        }
    }

    let wake = Arc::new(Wake(tokio::sync::Notify::new()));
    let executor = shared_executor()?;
    let mut future = Box::pin(future);
    loop {
        let waker = futures::task::waker(Arc::clone(&wake));
        let io = Arc::clone(&executor);
        let (next, result) = run_cpu_async(move || {
            let _guard = io.enter();
            let result = future.as_mut().poll(&mut std::task::Context::from_waker(&waker));
            (future, result)
        })
        .await?;
        if let std::task::Poll::Ready(output) = result {
            return Ok(output);
        }
        future = next;
        // Notify retains an early wake between poll and registration.
        wake.0.notified().await;
    }
}

static EXECUTOR: OnceLock<std::result::Result<Arc<SharedExecutor>, String>> = OnceLock::new();

/// Kept alive for the process lifetime: dropping a table never shuts down I/O
/// needed by other tables, or drops a runtime inside an async call context.
pub(crate) fn shared_executor() -> Result<Arc<SharedExecutor>> {
    match EXECUTOR.get_or_init(|| {
        runtime_config()
            .and_then(SharedExecutor::new)
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
    #[cfg(test)]
    live_threads: Arc<AtomicUsize>,
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

fn configured_io_builder(config: &DeltaRuntimeConfig) -> tokio::runtime::Builder {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .worker_threads(config.runtime_workers)
        .max_blocking_threads(config.blocking_workers)
        .thread_name("delta-io")
        .enable_all();
    if let Some(value) = config.event_interval {
        builder.event_interval(value as u32);
    }
    if let Some(value) = config.global_queue_interval {
        builder.global_queue_interval(value as u32);
    }
    if let Some(value) = config.thread_stack_size {
        builder.thread_stack_size(value);
    }
    if let Some(value) = config.thread_keep_alive_ms {
        builder.thread_keep_alive(std::time::Duration::from_millis(value as u64));
    }
    builder
}

impl SharedExecutor {
    fn new(config: &DeltaRuntimeConfig) -> Result<Self> {
        let blocking_capacity = config
            .blocking_workers
            .checked_sub(config.blocking_fs_reserve)
            .filter(|&capacity| capacity > 0)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "blocking filesystem reserve must be smaller than the blocking worker count",
                )
            })?;
        let mut builder = configured_io_builder(config);
        #[cfg(test)]
        let live_threads = {
            let live = Arc::new(AtomicUsize::new(0));
            let started = Arc::clone(&live);
            let stopped = Arc::clone(&live);
            builder.on_thread_start(move || {
                started.fetch_add(1, AtomicOrdering::Relaxed);
            });
            builder.on_thread_stop(move || {
                stopped.fetch_sub(1, AtomicOrdering::Relaxed);
            });
            live
        };
        let runtime = builder.build().context("create shared Delta I/O runtime")?;
        eprintln!(
            "ARROWCOL_DELTA_IO_RUNTIME_BUILT {}",
            serde_json::json!({
                "version": 1,
                "runtime": {
                    "event_interval": config.event_interval,
                    "global_queue_interval": config.global_queue_interval,
                    "thread_stack_size": config.thread_stack_size,
                    "thread_keep_alive_ms": config.thread_keep_alive_ms,
                }
            })
        );
        Ok(Self {
            runtime,
            #[cfg(test)]
            live_threads,
            admission: Arc::new(Admission::new(config.task_limit)),
            // Admitted synchronous jobs may wait on tokio::fs, which uses this
            // same blocking pool. Keep the configured slots free for filesystem work.
            blocking_admission: Arc::new(tokio::sync::Semaphore::new(blocking_capacity)),
            upload_admission: Arc::new(tokio::sync::Semaphore::new(config.upload_limit)),
            background_failure: Arc::new(Mutex::new(None)),
        })
    }

    #[cfg(test)]
    fn live_thread_count(&self) -> usize {
        self.live_threads.load(AtomicOrdering::Relaxed)
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

#[cfg(test)]
pub(crate) fn assert_worker_bound() -> Result<()> {
    let threads = shared_executor()?.live_thread_count();
    let config = runtime_config()?;
    let bound = config.runtime_workers..=config.runtime_workers + config.blocking_workers;
    assert!(
        bound.contains(&threads),
        "shared executor owns {threads} threads; expected {bound:?}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn partition_grouping_env_is_strict() -> Result<()> {
        let name = "ARROWCOL_COLLECTOR_PARTITION_GROUPING";
        assert_eq!(
            DeltaRuntimeConfig::from_lookup(|_| Ok(None))?.partition_grouping,
            crate::PartitionGrouping::ScalarMap
        );
        for raw in ["scalar_map", "typed_runs"] {
            assert_eq!(
                DeltaRuntimeConfig::from_lookup(|key| Ok((key == name).then(|| raw.to_owned())))?
                    .partition_grouping
                    .as_str(),
                raw
            );
        }
        for raw in ["", "TYPED_RUNS", "typed_runs ", "auto"] {
            assert!(DeltaRuntimeConfig::from_lookup(|key| Ok((key == name).then(|| raw.to_owned()))).is_err());
        }
        Ok(())
    }

    #[test]
    fn generated_partition_binding_is_strictly_opt_in() -> Result<()> {
        let name = "ARROWCOL_COLLECTOR_GENERATED_PARTITION_BINDING";
        assert_eq!(
            DeltaRuntimeConfig::from_lookup(|_| Ok(None))?
                .generated_partition_binding
                .as_str(),
            "revalidate_rows"
        );
        for raw in ["revalidate_rows", "proven_constant"] {
            let config = DeltaRuntimeConfig::from_lookup(|key| Ok((key == name).then(|| raw.to_owned())))?;
            assert_eq!(config.generated_partition_binding.as_str(), raw);
            assert!(!config.generated_partition_fast_path);
        }
        for raw in ["", "true", "PROVEN_CONSTANT", "proven_constant "] {
            let error = DeltaRuntimeConfig::from_lookup(|key| Ok((key == name).then(|| raw.to_owned())))
                .err()
                .context("invalid binding unexpectedly accepted")?;
            assert!(format!("{error:#}").contains(name));
        }
        Ok(())
    }

    #[test]
    fn column_pipeline_requires_multiple_workers_and_composes_with_streaming() -> Result<()> {
        for mode in ["", "pipeline", "ColumnPipeline", "column_pipeline "] {
            assert!(
                DeltaRuntimeConfig::from_lookup(|name| Ok(
                    (name == "ARROWCOL_COLLECTOR_PARQUET_ENCODE_MODE").then(|| mode.to_owned())
                ))
                .is_err()
            );
        }
        for workers in [1, 2, 16] {
            let config = DeltaRuntimeConfig::from_lookup(|name| {
                Ok(match name {
                    "ARROWCOL_COLLECTOR_PARQUET_ENCODE_MODE" => Some("column_pipeline".into()),
                    "ARROWCOL_COLLECTOR_PARQUET_COLUMN_WORKERS" => Some(workers.to_string()),
                    "ARROWCOL_COLLECTOR_UPLOAD_MODE" => Some("buffered_multipart".into()),
                    "ARROWCOL_COLLECTOR_UPLOAD_STAGING_MODE" => Some("streaming".into()),
                    _ => None,
                })
            });
            assert_eq!(config.is_ok(), workers > 1);
        }
        assert_eq!(
            DeltaRuntimeConfig::from_lookup(|_| Ok(None))?.parquet_encode_mode,
            crate::ParquetEncodeMode::ColumnWaves
        );
        Ok(())
    }

    #[test]
    fn generated_partition_fast_path_is_strictly_opt_in() -> Result<()> {
        let name = "ARROWCOL_COLLECTOR_GENERATED_PARTITION_FAST_PATH";
        assert!(!DeltaRuntimeConfig::from_lookup(|_| Ok(None))?.generated_partition_fast_path);
        for (raw, expected) in [("true", true), ("false", false)] {
            let config = DeltaRuntimeConfig::from_lookup(|key| Ok((key == name).then(|| raw.to_owned())))?;
            assert_eq!(config.generated_partition_fast_path, expected);
        }
        for raw in ["", "1", "TRUE", "true "] {
            assert!(DeltaRuntimeConfig::from_lookup(|key| Ok((key == name).then(|| raw.to_owned()))).is_err());
        }
        Ok(())
    }

    #[test]
    fn runtime_defaults_scale_with_cpu_bounds() -> Result<()> {
        let small = DeltaRuntimeConfig::for_cores(1);
        assert_eq!(
            (
                small.runtime_workers,
                small.blocking_workers,
                small.upload_limit,
                small.upload_buffer_capacity_bytes,
                small.upload_request_concurrency,
                small.task_limit,
                small.read_ahead,
                small.cpu_workers,
                small.batch_rows,
                small.parquet_column_workers
            ),
            (2, 32, 32, 8 * 1024 * 1024, 2, 64, 8, 1, 4096, 1)
        );
        let large = DeltaRuntimeConfig::for_cores(usize::MAX);
        assert_eq!(
            (
                large.runtime_workers,
                large.blocking_workers,
                large.upload_limit,
                large.upload_buffer_capacity_bytes,
                large.upload_request_concurrency,
                large.task_limit,
                large.read_ahead,
                large.cpu_workers,
                large.batch_rows,
                large.parquet_column_workers
            ),
            (16, 128, 32, 8 * 1024 * 1024, 2, 256, 16, 32, 32768, 1)
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
            ("UPLOAD_BUFFER_CAPACITY_BYTES", 1024 * 1024, 64 * 1024 * 1024),
            ("UPLOAD_REQUEST_CONCURRENCY", 1, 16),
            ("TASK_LIMIT", 2, 1024),
            ("READ_AHEAD", 1, 128),
            ("BATCH_ROWS", 1, 262_144),
            ("CPU_WORKERS", 1, 128),
            ("PARQUET_COLUMN_WORKERS", 1, 16),
        ] {
            let knob = format!("ARROWCOL_COLLECTOR_{knob}");
            let companion_value = |name: &str| -> Option<&'static str> {
                match (knob.as_str(), name) {
                    ("ARROWCOL_COLLECTOR_UPLOAD_LIMIT", "ARROWCOL_COLLECTOR_UPLOAD_BUFFER_CAPACITY_BYTES")
                    | (
                        "ARROWCOL_COLLECTOR_UPLOAD_REQUEST_CONCURRENCY",
                        "ARROWCOL_COLLECTOR_UPLOAD_BUFFER_CAPACITY_BYTES",
                    ) => Some("1048576"),
                    ("ARROWCOL_COLLECTOR_UPLOAD_LIMIT", "ARROWCOL_COLLECTOR_UPLOAD_REQUEST_CONCURRENCY")
                    | (
                        "ARROWCOL_COLLECTOR_UPLOAD_BUFFER_CAPACITY_BYTES",
                        "ARROWCOL_COLLECTOR_UPLOAD_REQUEST_CONCURRENCY",
                    ) => Some("1"),
                    ("ARROWCOL_COLLECTOR_UPLOAD_BUFFER_CAPACITY_BYTES", "ARROWCOL_COLLECTOR_UPLOAD_LIMIT")
                    | ("ARROWCOL_COLLECTOR_UPLOAD_REQUEST_CONCURRENCY", "ARROWCOL_COLLECTOR_UPLOAD_LIMIT") => Some("1"),
                    _ => None,
                }
            };
            let upload_knob = matches!(
                knob.as_str(),
                "ARROWCOL_COLLECTOR_UPLOAD_LIMIT"
                    | "ARROWCOL_COLLECTOR_UPLOAD_BUFFER_CAPACITY_BYTES"
                    | "ARROWCOL_COLLECTOR_UPLOAD_REQUEST_CONCURRENCY"
            );
            let read = |name: &str, value: &str| {
                if name == knob {
                    Some(value.to_owned())
                } else if name == "ARROWCOL_COLLECTOR_UPLOAD_MODE" && upload_knob {
                    Some("buffered_multipart".to_owned())
                } else {
                    companion_value(name).map(str::to_owned)
                }
            };
            for value in [minimum, maximum] {
                let config = DeltaRuntimeConfig::from_lookup(|name| Ok(read(name, &value.to_string())))?;
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
                let error = DeltaRuntimeConfig::from_lookup(|name| Ok(read(name, &value)))
                    .err()
                    .context("invalid override must fail")?;
                assert!(format!("{error:#}").contains(&knob));
            }
        }
        Ok(())
    }

    #[test]
    fn cpu_policy_environment_is_strict() -> Result<()> {
        let defaults = DeltaRuntimeConfig::from_lookup(|_| Ok(None))?;
        assert_eq!(
            (defaults.cpu_dispatch, defaults.cpu_placement),
            (CpuDispatch::SharedFifo, CpuPlacement::Os)
        );
        let dispatch = "ARROWCOL_COLLECTOR_CPU_DISPATCH";
        let placement = "ARROWCOL_COLLECTOR_CPU_PLACEMENT";
        let config = DeltaRuntimeConfig::from_lookup(|name| {
            Ok(match name {
                n if n == dispatch => Some("bounded_shards".into()),
                n if n == placement => Some("compact_allowed".into()),
                _ => None,
            })
        })?;
        assert_eq!(config.cpu_dispatch, CpuDispatch::BoundedShards);
        assert_eq!(config.cpu_placement, CpuPlacement::CompactAllowed);
        for (key, raw) in [(dispatch, "shared"), (placement, "scatter_allowed "), (placement, "OS")] {
            let err = DeltaRuntimeConfig::from_lookup(|name| Ok((name == key).then(|| raw.to_owned())))
                .err()
                .context("invalid CPU policy must fail")?;
            assert!(err.to_string().contains(key));
        }
        Ok(())
    }

    #[test]
    fn shard_admission_budget_is_shared_even_when_smaller_than_worker_count() -> Result<()> {
        let (queues, workers) = CpuQueues::new(CpuDispatch::BoundedShards, 3, 1);
        for worker in workers {
            queues
                .send(Box::new(|| {}))
                .now_or_never()
                .context("expected admission")??;
            assert!(queues.send(Box::new(|| {})).now_or_never().is_none());
            let WorkerQueue::Shard(rx) = worker else {
                anyhow::bail!("expected shard")
            };
            drop(rx.try_recv().context("expected round-robin delivery")?);
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sharded_workers_pin_within_inherited_mask_and_reenter_inline() -> Result<()> {
        use rustix::thread::{CpuSet, sched_setaffinity};
        assert_eq!(placement_index(CpuPlacement::CompactAllowed, 1, 2, 8), 1);
        assert_eq!(placement_index(CpuPlacement::ScatterAllowed, 1, 2, 8), 4);
        // Narrow only a disposable thread, never the runner's own affinity.
        std::thread::spawn(|| -> Result<()> {
            let allowed = effective_cpus()?;
            let mut mask = CpuSet::new();
            mask.set(allowed[0]);
            mask.set(*allowed.last().context("empty mask")?);
            sched_setaffinity(None, &mask)?;
            let inherited = effective_cpus()?;
            for placement in [CpuPlacement::CompactAllowed, CpuPlacement::ScatterAllowed] {
                let pool = Arc::new(CpuPool::start(3, 1, None, CpuDispatch::BoundedShards, placement)?);
                for i in 0..3 {
                    let nested = Arc::clone(&pool);
                    let (outer, inner, observed) = pool.run(move || -> Result<_> {
                        let outer = std::thread::current().id();
                        let inner = nested.run(|| std::thread::current().id())?;
                        Ok((outer, inner, effective_cpus()?))
                    })??;
                    let cpu = inherited[i % inherited.len()];
                    assert_eq!(observed, vec![cpu]);
                    assert_eq!(pool.worker_topology[i].cpu, Some(cpu));
                    assert_eq!(inner, outer);
                }
            }
            Ok(())
        })
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }

    #[test]
    fn runtime_scheduler_settings_parse_and_create_consumers() -> Result<()> {
        let config = DeltaRuntimeConfig::from_lookup(|name| {
            Ok(match name {
                "ARROWCOL_COLLECTOR_EVENT_INTERVAL" => Some("7".to_owned()),
                "ARROWCOL_COLLECTOR_GLOBAL_QUEUE_INTERVAL" => Some("13".to_owned()),
                "ARROWCOL_COLLECTOR_THREAD_STACK_SIZE" => Some("2097152".to_owned()),
                "ARROWCOL_COLLECTOR_THREAD_KEEP_ALIVE_MS" => Some("3000".to_owned()),
                "ARROWCOL_COLLECTOR_CPU_THREAD_STACK_SIZE" => Some("2097152".to_owned()),
                _ => None,
            })
        })?;
        let pool = CpuPool::new(1, 1, config.cpu_thread_stack_size)?;
        assert_eq!(
            pool.run(|| std::thread::current().name().map(str::to_owned))?
                .as_deref(),
            Some("delta-cpu-0")
        );
        let mut builder = configured_io_builder(&config);
        assert!(format!("{builder:?}").contains("thread_stack_size: Some(2097152)"));
        let runtime = builder.build()?;
        assert_eq!(runtime.block_on(async { 42 }), 42);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cpu_pool_readback_follows_worker_creation() -> Result<()> {
        if let Ok(mode) = std::env::var("ARROWCOL_TEST_CPU_POOL_CHILD") {
            let _ = runtime_config()?;
            if mode == "lazy" {
                assert_eq!(run_cpu(|| 42)?, 42);
            }
            return Ok(());
        }
        for (mode, dispatch, placement, scope) in [
            ("lazy", "shared_fifo", "os", "lazy_pool_initialization"),
            ("startup", "bounded_shards", "scatter_allowed", "runtime_initialization"),
            ("startup", "bounded_shards", "physical_first", "runtime_initialization"),
        ] {
            let output = std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "delta_io::tests::cpu_pool_readback_follows_worker_creation",
                    "--nocapture",
                ])
                .env_clear()
                .env("ARROWCOL_TEST_CPU_POOL_CHILD", mode)
                .env("ARROWCOL_COLLECTOR_CPU_WORKERS", "2")
                .env("ARROWCOL_COLLECTOR_CPU_QUEUE_JOBS", "1")
                .env("ARROWCOL_COLLECTOR_CPU_DISPATCH", dispatch)
                .env("ARROWCOL_COLLECTOR_CPU_PLACEMENT", placement)
                .env("ARROWCOL_COLLECTOR_CPU_THREAD_STACK_SIZE", "4194304")
                .output()?;
            anyhow::ensure!(
                output.status.success(),
                "CPU pool child failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let stderr = String::from_utf8(output.stderr)?;
            let readback = stderr
                .lines()
                .find_map(|line| line.strip_prefix("arrowcol_effective_config "))
                .context("CPU pool readback missing")?;
            let readback: serde_json::Value = serde_json::from_str(readback)?;
            assert_eq!(readback["cpu_pool"]["thread_stack_size"], 4194304);
            let pool = &readback["cpu_pool"];
            assert_eq!(pool["scope"], scope);
            assert_eq!(pool["cpu_dispatch"], dispatch);
            assert_eq!(pool["cpu_placement"], placement);
            assert_eq!(pool["workers_started"], 2);
            assert_eq!(pool["queue_jobs"], 1);
            assert_eq!(pool["shard_count"], if mode == "lazy" { 1 } else { 2 });
            let ids = pool["worker_cpu_ids"].as_array().context("worker CPU IDs")?;
            assert_eq!(ids.len(), 2);
            if placement == "physical_first" {
                for (index, cpu) in ids.iter().enumerate() {
                    let cpu = cpu.as_u64().context("actual worker CPU")? as usize;
                    let (package, core) = topology::core(topology::CPU_ROOT.as_ref(), cpu)?;
                    assert_eq!(
                        pool["worker_topology"][index],
                        serde_json::json!({"cpu":cpu,"package":package,"core":core})
                    );
                }
            }
            assert!(ids.iter().all(serde_json::Value::is_u64));
        }
        Ok(())
    }

    #[test]
    fn runtime_queue_and_blocking_reserve_resolve_and_validate() -> Result<()> {
        let config = DeltaRuntimeConfig::from_lookup(|name| {
            Ok(match name {
                "ARROWCOL_COLLECTOR_CPU_WORKERS" => Some("4".to_owned()),
                "ARROWCOL_COLLECTOR_BLOCKING_WORKERS" => Some("4".to_owned()),
                _ => None,
            })
        })?;
        assert_eq!(config.cpu_queue_jobs, Some(4));
        assert_eq!(config.blocking_fs_reserve, 1);
        for (knob, value) in [
            ("ARROWCOL_COLLECTOR_CPU_QUEUE_JOBS", "0"),
            ("ARROWCOL_COLLECTOR_CPU_QUEUE_JOBS", "129"),
            ("ARROWCOL_COLLECTOR_CPU_QUEUE_JOBS", "1.5"),
            ("ARROWCOL_COLLECTOR_BLOCKING_FS_RESERVE", "0"),
            ("ARROWCOL_COLLECTOR_BLOCKING_FS_RESERVE", "4"),
        ] {
            assert!(
                DeltaRuntimeConfig::from_lookup(|name| {
                    Ok(if name == knob {
                        Some(value.to_owned())
                    } else if matches!(
                        name,
                        "ARROWCOL_COLLECTOR_CPU_WORKERS" | "ARROWCOL_COLLECTOR_BLOCKING_WORKERS"
                    ) {
                        Some("4".to_owned())
                    } else {
                        None
                    })
                })
                .is_err(),
                "{knob}={value}"
            );
        }
        let tuned = DeltaRuntimeConfig::from_lookup(|name| {
            Ok(match name {
                "ARROWCOL_COLLECTOR_CPU_WORKERS" | "ARROWCOL_COLLECTOR_BLOCKING_WORKERS" => Some("4".to_owned()),
                "ARROWCOL_COLLECTOR_CPU_QUEUE_JOBS" => Some("5".to_owned()),
                "ARROWCOL_COLLECTOR_BLOCKING_FS_RESERVE" => Some("2".to_owned()),
                _ => None,
            })
        })?;
        assert_eq!((tuned.cpu_queue_jobs, tuned.blocking_fs_reserve), (Some(5), 2));
        Ok(())
    }

    #[test]
    fn upload_mode_is_strict_and_buffer_settings_require_multipart() -> Result<()> {
        let default = DeltaRuntimeConfig::for_cores(1);
        assert_eq!(default.upload_mode, UploadMode::PutCreate);

        let multipart = DeltaRuntimeConfig::from_lookup(|name| {
            Ok((name == "ARROWCOL_COLLECTOR_UPLOAD_MODE").then(|| "buffered_multipart".to_owned()))
        })?;
        assert_eq!(multipart.upload_mode, UploadMode::BufferedMultipart);

        for raw in ["", "multipart", "Buffered_Multipart", "put_create "] {
            let Err(error) = DeltaRuntimeConfig::from_lookup(|name| {
                Ok((name == "ARROWCOL_COLLECTOR_UPLOAD_MODE").then(|| raw.to_owned()))
            }) else {
                anyhow::bail!("invalid upload mode must fail");
            };
            assert!(format!("{error:#}").contains("ARROWCOL_COLLECTOR_UPLOAD_MODE"));
        }

        let Err(error) = DeltaRuntimeConfig::from_lookup(|name| {
            Ok((name == "ARROWCOL_COLLECTOR_UPLOAD_BUFFER_CAPACITY_BYTES").then(|| "8388608".to_owned()))
        }) else {
            anyhow::bail!("buffer settings require explicit multipart mode");
        };
        assert!(format!("{error:#}").contains("UPLOAD_MODE=buffered_multipart"));
        Ok(())
    }

    #[test]
    fn aggregate_upload_buffering_is_bounded() -> Result<()> {
        let mut config = DeltaRuntimeConfig::for_cores(1);
        config.upload_mode = UploadMode::BufferedMultipart;
        config.upload_request_concurrency = 16;
        config.upload_limit = 256;
        let error = config
            .validate()
            .err()
            .context("excessive aggregate upload buffering must fail")?;
        assert!(format!("{error:#}").contains("aggregate upload buffering"));

        config.upload_request_concurrency = 2;
        config.upload_limit = 32;
        config.validate()?;
        Ok(())
    }

    #[test]
    fn cpu_jobs_keep_panics_nested_calls_and_io_independent() -> Result<()> {
        let pool = Arc::new(CpuPool::new(1, 1, None)?);
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
        let pool = Arc::new(CpuPool::new(1, 1, None)?);
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
        pool.queues
            .send(Box::new(|| {}))
            .now_or_never()
            .context("expected room in queue")??;
        assert!(pool.queues.send(Box::new(|| {})).now_or_never().is_none());
        release.send(())?;
        caller.join().unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        assert_eq!(pool.run(|| 42)?, 42);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cpu_async_admission_does_not_block_the_caller_runtime() -> Result<()> {
        let pool = Arc::new(CpuPool::new(1, 1, None)?);
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
        pool.queues
            .send(Box::new(|| {}))
            .now_or_never()
            .context("expected room in queue")??;
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
            assert!(CPU_WORKER.get());
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
        assert_worker_bound()?;
        Ok(())
    }

    /// Scenario: far more blocking requests arrive than the process-wide pool limit.
    /// Guarantees: admission and thread growth stay bounded, and all requests complete.
    #[test]
    fn blocking_concurrency_and_threads_are_bounded() -> Result<()> {
        let executor = shared_executor()?;
        let config = runtime_config()?;
        let capacity = config.blocking_workers - config.blocking_fs_reserve;
        // Keep another delta-io worker alive throughout the measurement.
        let mut other_config = config.clone();
        other_config.runtime_workers = 1;
        let other = SharedExecutor::new(&other_config)?;
        other.block_on(async { assert_eq!(std::thread::current().name(), Some("delta-io")) });
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let io_executor = Arc::clone(&executor);
        let observed_peak = Arc::clone(&peak);
        executor.block_on(async move {
            // Rendezvous the first two jobs so a serial executor cannot pass.
            let (left, from_left) = mpsc::channel();
            let (right, from_right) = mpsc::channel();
            let mut peers = [(left, from_right), (right, from_left)]
                .into_iter()
                .take(if capacity > 1 { 2 } else { 0 });
            let tasks = (0..(2 * capacity)).map(|_| {
                let peer = peers.next();
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                io_executor.spawn_blocking(move || {
                    let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(count, Ordering::SeqCst);
                    if let Some((started, peer)) = peer {
                        assert!(started.send(()).is_ok(), "peer must be live");
                        assert!(
                            peer.recv_timeout(Duration::from_secs(10)).is_ok(),
                            "blocking jobs must overlap"
                        );
                    }
                    std::thread::sleep(Duration::from_millis(10));
                    active.fetch_sub(1, Ordering::SeqCst);
                })
            });
            for result in futures::future::join_all(tasks).await {
                result?;
            }
            Ok::<(), Error>(())
        })?;
        assert!((capacity.min(2)..=capacity).contains(&observed_peak.load(Ordering::SeqCst)));
        assert_eq!(other.live_thread_count(), 1);
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
