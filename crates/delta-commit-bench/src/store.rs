//! Object store wrapper that counts storage transactions and injects faults.
//!
//! The wrapper sits under delta-rs and above the Azure client, so counts include every
//! request delta-rs issues. The Azure client's own HTTP retries happen below this layer.

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Which requests a fault applies to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
pub enum FaultScope {
    /// Requests under `_delta_log/`.
    #[default]
    Log,
    /// Requests outside `_delta_log/`.
    Data,
    /// Every request.
    All,
}

/// How an ambiguous log commit reports its result to the caller.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
pub enum AmbiguousKind {
    /// The write lands and the caller sees a transport error (client timeout).
    #[default]
    Timeout,
    /// The write lands and the caller sees AlreadyExists (a 5xx retried into a 409).
    Conflict,
}

/// Faults to inject. All rates are probabilities in [0, 1].
#[derive(Clone, Debug, Default, Serialize, Deserialize, clap::Args)]
pub struct FaultPlan {
    /// Probability that a request fails with an injected 503 before reaching storage.
    #[arg(long, default_value_t = 0.0)]
    pub fault_error_rate: f64,
    /// Requests the injected 503s and delays apply to.
    #[arg(long, value_enum, default_value_t = FaultScope::Log)]
    pub fault_scope: FaultScope,
    /// Added latency in milliseconds per request in scope.
    #[arg(long, default_value_t = 0)]
    pub fault_delay_ms: u64,
    /// Probability that a successful create-only log write reports an error.
    #[arg(long, default_value_t = 0.0)]
    pub fault_ambiguous_rate: f64,
    /// Error reported for an ambiguous log write.
    #[arg(long, value_enum, default_value_t = AmbiguousKind::Timeout)]
    pub fault_ambiguous_kind: AmbiguousKind,
    /// Abort the process right before the Nth create-only log write is sent.
    #[arg(long)]
    pub fault_crash_before_log_put: Option<u64>,
    /// Abort the process right after the Nth successful create-only log write.
    #[arg(long)]
    pub fault_crash_after_log_put: Option<u64>,
    /// Abort the process after the first part of the Nth multipart data upload.
    #[arg(long)]
    pub fault_crash_mid_upload: Option<u64>,
    /// Abort the process right before the Nth epoch claim commit is attempted (commit loop).
    #[arg(long)]
    pub fault_crash_before_claim: Option<u64>,
    /// Abort the process right after the Nth epoch claim commit lands (commit loop).
    #[arg(long)]
    pub fault_crash_after_claim: Option<u64>,
}

impl FaultPlan {
    fn in_scope(&self, is_log: bool) -> bool {
        match self.fault_scope {
            FaultScope::Log => is_log,
            FaultScope::Data => !is_log,
            FaultScope::All => true,
        }
    }
}

/// Per-store request counters.
#[derive(Debug, Default)]
pub struct Counters {
    log_get: AtomicU64,
    log_head: AtomicU64,
    log_list: AtomicU64,
    log_put_create: AtomicU64,
    log_put_create_conflict: AtomicU64,
    log_put_other: AtomicU64,
    data_get: AtomicU64,
    data_head: AtomicU64,
    data_list: AtomicU64,
    data_put: AtomicU64,
    data_put_part: AtomicU64,
    other: AtomicU64,
    injected_errors: AtomicU64,
    injected_ambiguous: AtomicU64,
    log_put_create_ok_total: AtomicU64,
    log_put_create_attempt_total: AtomicU64,
    multipart_started: AtomicU64,
}

/// Point-in-time copy of [`Counters`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CounterSnapshot {
    pub log_get: u64,
    pub log_head: u64,
    pub log_list: u64,
    pub log_put_create: u64,
    pub log_put_create_conflict: u64,
    pub log_put_other: u64,
    pub data_get: u64,
    pub data_head: u64,
    pub data_list: u64,
    pub data_put: u64,
    pub data_put_part: u64,
    pub other: u64,
    pub injected_errors: u64,
    pub injected_ambiguous: u64,
}

impl CounterSnapshot {
    /// Element-wise difference `self - earlier`.
    #[must_use]
    pub const fn since(&self, earlier: &Self) -> Self {
        Self {
            log_get: self.log_get.saturating_sub(earlier.log_get),
            log_head: self.log_head.saturating_sub(earlier.log_head),
            log_list: self.log_list.saturating_sub(earlier.log_list),
            log_put_create: self.log_put_create.saturating_sub(earlier.log_put_create),
            log_put_create_conflict: self
                .log_put_create_conflict
                .saturating_sub(earlier.log_put_create_conflict),
            log_put_other: self.log_put_other.saturating_sub(earlier.log_put_other),
            data_get: self.data_get.saturating_sub(earlier.data_get),
            data_head: self.data_head.saturating_sub(earlier.data_head),
            data_list: self.data_list.saturating_sub(earlier.data_list),
            data_put: self.data_put.saturating_sub(earlier.data_put),
            data_put_part: self.data_put_part.saturating_sub(earlier.data_put_part),
            other: self.other.saturating_sub(earlier.other),
            injected_errors: self.injected_errors.saturating_sub(earlier.injected_errors),
            injected_ambiguous: self.injected_ambiguous.saturating_sub(earlier.injected_ambiguous),
        }
    }
}

impl Counters {
    /// Copy the current counter values.
    pub fn snapshot(&self) -> CounterSnapshot {
        let l = |c: &AtomicU64| c.load(Ordering::Relaxed);
        CounterSnapshot {
            log_get: l(&self.log_get),
            log_head: l(&self.log_head),
            log_list: l(&self.log_list),
            log_put_create: l(&self.log_put_create),
            log_put_create_conflict: l(&self.log_put_create_conflict),
            log_put_other: l(&self.log_put_other),
            data_get: l(&self.data_get),
            data_head: l(&self.data_head),
            data_list: l(&self.data_list),
            data_put: l(&self.data_put),
            data_put_part: l(&self.data_put_part),
            other: l(&self.other),
            injected_errors: l(&self.injected_errors),
            injected_ambiguous: l(&self.injected_ambiguous),
        }
    }
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

fn is_log(path: &Path) -> bool {
    path.as_ref().contains("_delta_log")
}

fn injected(message: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "fault",
        source: message.to_owned().into(),
    }
}

/// Counting and fault-injecting wrapper around an [`ObjectStore`].
pub struct InstrumentedStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<Counters>,
    faults: Arc<FaultPlan>,
}

impl InstrumentedStore {
    /// Wrap `inner`, recording into `counters` and applying `faults`.
    pub fn new(inner: Arc<dyn ObjectStore>, counters: Arc<Counters>, faults: Arc<FaultPlan>) -> Self {
        Self {
            inner,
            counters,
            faults,
        }
    }

    async fn before(&self, is_log: bool) -> Result<()> {
        if !self.faults.in_scope(is_log) {
            return Ok(());
        }
        if self.faults.fault_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.faults.fault_delay_ms)).await;
        }
        if self.faults.fault_error_rate > 0.0 && rand::random::<f64>() < self.faults.fault_error_rate {
            bump(&self.counters.injected_errors);
            return Err(injected("503 ServerBusy (injected)"));
        }
        Ok(())
    }
}

impl Debug for InstrumentedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "InstrumentedStore({})", self.inner)
    }
}

impl Display for InstrumentedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "InstrumentedStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for InstrumentedStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let log = is_log(location);
        let create = matches!(opts.mode, PutMode::Create);
        self.before(log).await?;
        if log && create {
            let n = self
                .counters
                .log_put_create_attempt_total
                .fetch_add(1, Ordering::SeqCst)
                + 1;
            if self.faults.fault_crash_before_log_put == Some(n) {
                eprintln!("fault: aborting before log write {location}");
                std::process::abort();
            }
        }
        match (log, create) {
            (true, true) => bump(&self.counters.log_put_create),
            (true, false) => bump(&self.counters.log_put_other),
            (false, _) => bump(&self.counters.data_put),
        }
        let result = self.inner.put_opts(location, payload, opts).await;
        if log && create {
            match &result {
                Err(object_store::Error::AlreadyExists { .. }) => bump(&self.counters.log_put_create_conflict),
                Ok(_) => {
                    let n = self.counters.log_put_create_ok_total.fetch_add(1, Ordering::SeqCst) + 1;
                    if self.faults.fault_crash_after_log_put == Some(n) {
                        eprintln!("fault: aborting after log write {location}");
                        std::process::abort();
                    }
                    if self.faults.fault_ambiguous_rate > 0.0
                        && rand::random::<f64>() < self.faults.fault_ambiguous_rate
                    {
                        bump(&self.counters.injected_ambiguous);
                        return Err(match self.faults.fault_ambiguous_kind {
                            AmbiguousKind::Timeout => injected("operation timed out (injected after write)"),
                            AmbiguousKind::Conflict => object_store::Error::AlreadyExists {
                                path: location.to_string(),
                                source: "409 after retried 500 (injected after write)".into(),
                            },
                        });
                    }
                }
                Err(_) => {}
            }
        }
        result
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        self.before(is_log(location)).await?;
        let n = self.counters.multipart_started.fetch_add(1, Ordering::SeqCst) + 1;
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CountingUpload {
            inner,
            counters: Arc::clone(&self.counters),
            crash_after_first_part: self.faults.fault_crash_mid_upload == Some(n),
            parts: 0,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let log = is_log(location);
        tracing::trace!(target: "dcb::store", path = %location, head = options.head, "get");
        if std::env::var("DCB_BT_PATH").is_ok_and(|p| location.as_ref().ends_with(&p)) {
            eprintln!("BACKTRACE {location}\n{}", std::backtrace::Backtrace::force_capture());
        }
        self.before(log).await?;
        bump(match (log, options.head) {
            (true, true) => &self.counters.log_head,
            (true, false) => &self.counters.log_get,
            (false, true) => &self.counters.data_head,
            (false, false) => &self.counters.data_get,
        });
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let log = is_log(location);
        tracing::trace!(target: "dcb::store", path = %location, "get_ranges");
        self.before(log).await?;
        bump(if log {
            &self.counters.log_get
        } else {
            &self.counters.data_get
        });
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        bump(&self.counters.other);
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let log = prefix.is_some_and(is_log);
        bump(if log {
            &self.counters.log_list
        } else {
            &self.counters.data_list
        });
        let faults = Arc::clone(&self.faults);
        let counters = Arc::clone(&self.counters);
        let inner = self.inner.list(prefix);
        if faults.in_scope(log) && (faults.fault_error_rate > 0.0 || faults.fault_delay_ms > 0) {
            futures::stream::once(async move {
                if faults.fault_delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(faults.fault_delay_ms)).await;
                }
                if faults.fault_error_rate > 0.0 && rand::random::<f64>() < faults.fault_error_rate {
                    bump(&counters.injected_errors);
                    return futures::stream::once(async { Err(injected("503 ServerBusy (injected)")) }).boxed();
                }
                inner
            })
            .flatten()
            .boxed()
        } else {
            inner
        }
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, Result<ObjectMeta>> {
        let log = prefix.is_some_and(is_log);
        bump(if log {
            &self.counters.log_list
        } else {
            &self.counters.data_list
        });
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let log = prefix.is_some_and(is_log);
        self.before(log).await?;
        bump(if log {
            &self.counters.log_list
        } else {
            &self.counters.data_list
        });
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        bump(&self.counters.other);
        self.inner.copy_opts(from, to, options).await
    }
}

struct CountingUpload {
    inner: Box<dyn MultipartUpload>,
    counters: Arc<Counters>,
    crash_after_first_part: bool,
    parts: u64,
}

impl Debug for CountingUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "CountingUpload({} parts)", self.parts)
    }
}

#[async_trait]
impl MultipartUpload for CountingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        bump(&self.counters.data_put_part);
        self.parts += 1;
        let crash = self.crash_after_first_part && self.parts == 1;
        let fut = self.inner.put_part(data);
        if crash {
            Box::pin(async move {
                let _ = fut.await;
                eprintln!("fault: aborting mid-upload after first part");
                std::process::abort();
            })
        } else {
            fut
        }
    }

    async fn complete(&mut self) -> Result<PutResult> {
        bump(&self.counters.data_put);
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        bump(&self.counters.other);
        self.inner.abort().await
    }
}
