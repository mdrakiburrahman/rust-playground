//! delta_kernel 0.30.1 benches. Kernel calls are synchronous; they run on a blocking thread and
//! the default engine uses the multi-thread executor on the ambient runtime.

use crate::stats::Summary;
use crate::store;
use crate::synth::Rows;
use anyhow::{Context, Result, anyhow};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::json::ReaderBuilder;
use delta_kernel::checkpoint::{CheckpointSpec, V2CheckpointConfig};
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::snapshot::{Snapshot, SnapshotRef};
use delta_kernel::transaction::{CommitResult, UpdateTableOperation};
use delta_kernel::{Engine, EngineData, FileMeta, ParquetHandler};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use delta_kernel_default_engine::DefaultEngine;
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Instant;
use url::Url;

/// Default engine with an optional checkpoint-writer override: `LR_CKPT_COMPRESSION`
/// (none | snappy | zstd) and `LR_CKPT_ROWGROUP` (rows). Reads always go to the default handler.
struct LrEngine {
    inner: Arc<dyn Engine>,
    parquet: Arc<dyn ParquetHandler>,
}

impl Engine for LrEngine {
    fn evaluation_handler(&self) -> Arc<dyn delta_kernel::EvaluationHandler> {
        self.inner.evaluation_handler()
    }
    fn storage_handler(&self) -> Arc<dyn delta_kernel::StorageHandler> {
        self.inner.storage_handler()
    }
    fn json_handler(&self) -> Arc<dyn delta_kernel::JsonHandler> {
        self.inner.json_handler()
    }
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }
}

struct LrParquet {
    inner: Arc<dyn ParquetHandler>,
    store: Arc<dyn object_store::ObjectStore>,
    rt: tokio::runtime::Handle,
    props: WriterProperties,
}

impl ParquetHandler for LrParquet {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: delta_kernel::schema::SchemaRef,
        predicate: Option<delta_kernel::expressions::PredicateRef>,
    ) -> delta_kernel::KernelResult<delta_kernel::FileDataReadResultIterator> {
        self.inner.read_parquet_files(files, physical_schema, predicate)
    }

    fn write_parquet_file(
        &self,
        location: Url,
        mut data: delta_kernel::KernelResultIteratorStatic<Box<dyn EngineData>>,
    ) -> delta_kernel::KernelResult<delta_kernel::FileSize> {
        let (store, props) = (self.store.clone(), self.props.clone());
        let ke = |e: &dyn std::fmt::Display| delta_kernel::KernelError::generic(e.to_string());
        self.rt.block_on(async move {
            let path = object_store::path::Path::from_url_path(location.path()).map_err(|e| ke(&e))?;
            let first = data.next().ok_or_else(|| delta_kernel::KernelError::generic("empty checkpoint data"))??;
            let first: arrow::array::RecordBatch = (*ArrowEngineData::try_from_engine_data(first)?).into();
            let mut w = AsyncArrowWriter::try_new(object_store::buffered::BufWriter::new(store, path), first.schema(), Some(props))
                .map_err(|e| ke(&e))?;
            w.write(&first).await.map_err(|e| ke(&e))?;
            for b in data {
                let b: arrow::array::RecordBatch = (*ArrowEngineData::try_from_engine_data(b?)?).into();
                w.write(&b).await.map_err(|e| ke(&e))?;
            }
            w.finish().await.map_err(|e| ke(&e))?;
            Ok(w.bytes_written() as u64)
        })
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> delta_kernel::KernelResult<delta_kernel::ParquetFooter> {
        self.inner.read_parquet_footer(file)
    }
}

fn engine(url: &Url) -> Result<Arc<dyn Engine>> {
    let (store, _) = store::open(url)?;
    let ckpt_store = store.clone();
    let exec = Arc::new(TokioMultiThreadExecutor::new(tokio::runtime::Handle::current()));
    let env = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<usize>().ok()).and_then(std::num::NonZero::new);
    let mut b = DefaultEngine::builder(store).with_task_executor(exec);
    if let Some(n) = env("LR_KERNEL_BUFFER") {
        b = b.with_buffer_size(n);
    }
    if let Some(n) = env("LR_KERNEL_BATCH") {
        b = b.with_batch_size(n);
    }
    let inner: Arc<dyn Engine> = Arc::new(b.build());
    let compression = std::env::var("LR_CKPT_COMPRESSION").ok();
    let rowgroup = env("LR_CKPT_ROWGROUP");
    if compression.is_none() && rowgroup.is_none() {
        return Ok(inner);
    }
    let mut props = WriterProperties::builder();
    props = props.set_compression(match compression.as_deref() {
        None | Some("none") => Compression::UNCOMPRESSED,
        Some("snappy") => Compression::SNAPPY,
        Some("zstd") => Compression::ZSTD(Default::default()),
        Some(o) => anyhow::bail!("LR_CKPT_COMPRESSION: unknown {o}"),
    });
    if let Some(n) = rowgroup {
        props = props.set_max_row_group_row_count(Some(n.get()));
    }
    let parquet = Arc::new(LrParquet {
        inner: inner.parquet_handler(),
        store: ckpt_store,
        rt: tokio::runtime::Handle::current(),
        props: props.build(),
    });
    Ok(Arc::new(LrEngine { inner, parquet }))
}

fn table_url(url: &Url) -> Url {
    let mut u = url.clone();
    if !u.path().ends_with('/') {
        u.set_path(&format!("{}/", u.path()));
    }
    u
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f).await?
}

/// Arrow schema for `add_files`: the base add-file schema with `stats` swapped for the table's
/// full stats schema, plus `dataChange`.
fn add_schema(snap: &SnapshotRef, engine: &dyn Engine) -> Result<Arc<Schema>> {
    let txn = snap.clone().update_table(UpdateTableOperation::Write, "lrb", true).build(engine, Box::new(FileSystemCommitter::new()))?;
    let base: Schema = txn.add_files_schema().as_ref().try_into_arrow()?;
    let stats: Schema = txn.stats_schema()?.as_ref().try_into_arrow()?;
    let fields: Vec<Field> = base
        .fields()
        .iter()
        .map(|f| {
            if f.name() == "stats" {
                Field::new("stats", DataType::Struct(stats.fields().clone()), true)
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

fn add_batch(schema: &Arc<Schema>, rows: &[crate::synth::AddRow], prefix: &str) -> Result<Box<dyn EngineData>> {
    let mut buf = Vec::new();
    for r in rows {
        let stats: Value = r.stats.as_deref().map(serde_json::from_str).transpose()?.unwrap_or(Value::Null);
        let v = json!({"path": format!("{prefix}-{}", r.path.replace('/', "_")), "partitionValues": {"date": r.date},
            "size": r.size, "modificationTime": r.mtime, "dataChange": true, "stats": stats});
        serde_json::to_writer(&mut buf, &v)?;
        buf.push(b'\n');
    }
    let mut dec = ReaderBuilder::new(schema.clone()).build_decoder()?;
    dec.decode(&buf)?;
    let batch = dec.flush()?.ok_or_else(|| anyhow!("empty add batch"))?;
    Ok(Box::new(ArrowEngineData::new(batch)))
}

/// Incremental refresh with retry. A local-fs listing that races concurrent commit creation can
/// show version N+1 without N, which kernel rejects as a gap.
fn refresh(snap: &SnapshotRef, engine: &dyn Engine, refresh_errors: &mut usize) -> Result<SnapshotRef> {
    let mut tries = 0;
    loop {
        match Snapshot::builder_from(snap.clone()).build(engine) {
            Ok(s) => return Ok(s),
            Err(e) if tries < 20 => {
                tries += 1;
                *refresh_errors += 1;
                tracing::debug!("refresh retry {tries}: {e}");
                std::thread::sleep(std::time::Duration::from_millis(2 * tries));
            }
            Err(e) => return Err(e.into()),
        }
    }
}

struct WriterOut {
    refresh_errors: usize,
    sum: Summary,
    conflicts: usize,
    landed: usize,
    retryable: usize,
    load_s: f64,
    version: u64,
    last_txn: i64,
}

fn writer_loop(engine: Arc<dyn Engine>, url: Url, app_id: String, args: crate::CommitArgs, w: u32) -> Result<WriterOut> {
    let t0 = Instant::now();
    let mut snap = Snapshot::builder_for(url.as_str()).build(engine.as_ref())?;
    let mut v = snap.get_app_id_version(&app_id, engine.as_ref())?.unwrap_or(0);
    let load_s = t0.elapsed().as_secs_f64();
    let schema = add_schema(&snap, engine.as_ref())?;
    let rows = Rows {
        active: u64::from(args.commits * args.files_per_commit).max(1),
        tombstones: 0,
        tombstone_mib: 128,
        days: 1,
        stats: args.stats.clone(),
        seed: args.seed ^ u64::from(w),
    };
    let mut out = WriterOut { refresh_errors: 0, sum: Summary::default(), conflicts: 0, landed: 0, retryable: 0, load_s, version: 0, last_txn: 0 };
    for c in 0..args.commits {
        if c > 0 && args.interval_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(args.interval_ms));
        }
        v += 1;
        let from = u64::from(c * args.files_per_commit);
        let adds = rows.adds(from, from + u64::from(args.files_per_commit));
        let started = Instant::now();
        let (mut conflicts, base) = (0usize, snap.version());
        loop {
            let mut txn = snap
                .clone()
                .update_table(UpdateTableOperation::Write, "lrb", true)
                .with_blind_append()
                .with_transaction_id(app_id.clone(), v)
                .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
            txn.add_files(add_batch(&schema, &adds, &app_id)?);
            let mut res = txn.commit(engine.as_ref())?;
            while let CommitResult::Retryable(r) = res {
                out.retryable += 1;
                res = r.transaction.commit(engine.as_ref())?;
            }
            match res {
                CommitResult::Committed(c) => {
                    snap = match c.post_commit_snapshot() {
                        Some(s) => s.clone(),
                        None => refresh(&snap, engine.as_ref(), &mut out.refresh_errors)?,
                    };
                    break;
                }
                CommitResult::Conflicted(_) => {
                    conflicts += 1;
                    snap = refresh(&snap, engine.as_ref(), &mut out.refresh_errors)?;
                    if snap.get_app_id_version(&app_id, engine.as_ref())?.is_some_and(|x| x >= v) {
                        out.landed += 1;
                        break;
                    }
                }
                CommitResult::Retryable(_) => unreachable!(),
            }
        }
        out.conflicts += conflicts;
        let el = started.elapsed();
        out.sum.record(el, store::rss_mib());
        if args.trace {
            eprintln!(
                "{}",
                json!({"trace": "commit", "w": w, "c": c, "at_ms": crate::now_ms(), "ms": el.as_secs_f64() * 1000.0,
                    "conflicts": conflicts, "base": base, "version": snap.version()})
            );
        }
    }
    out.version = snap.version();
    out.last_txn = v;
    Ok(out)
}

pub async fn commit(args: crate::CommitArgs) -> Result<()> {
    store::rss_watchdog(args.rss_cap_gib, format!("kernel commit {}", args.table_url));
    let engine = engine(&args.table_url)?;
    let url = table_url(&args.table_url);
    let base_rss = store::rss_mib();
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for w in 0..args.writers {
        let app_id = if args.writers == 1 { args.app_id.clone() } else { format!("{}-w{w}", args.app_id) };
        let (engine, url, args) = (engine.clone(), url.clone(), args.clone());
        handles.push(tokio::task::spawn_blocking(move || writer_loop(engine, url, app_id, args, w)));
    }
    let mut outs = Vec::new();
    for h in handles {
        outs.push(h.await??);
    }
    let wall = t0.elapsed().as_secs_f64();
    // Verify: every writer's final txn version is visible at the end.
    let check = {
        let (engine, url, app_id, n) = (engine.clone(), url.clone(), args.app_id.clone(), args.writers);
        let expect: Vec<i64> = outs.iter().map(|o| o.last_txn).collect();
        blocking(move || {
            let snap = Snapshot::builder_for(url.as_str()).build(engine.as_ref())?;
            let mut ok = true;
            for w in 0..n {
                let id = if n == 1 { app_id.clone() } else { format!("{app_id}-w{w}") };
                ok &= snap.get_app_id_version(&id, engine.as_ref())? == Some(expect[w as usize]);
            }
            Ok((ok, snap.version()))
        })
        .await?
    };
    let mut all = Summary::default();
    for o in &outs {
        all.merge(&o.sum);
    }
    println!(
        "{}",
        json!({"kind": "commit", "lib": "kernel", "table": args.table_url.as_str(), "writers": args.writers,
            "load_s": outs.iter().map(|o| o.load_s).fold(0.0, f64::max), "rss_base_mib": base_rss,
            "hwm_mib": store::hwm_mib(), "conflicts": outs.iter().map(|o| o.conflicts).sum::<usize>(),
            "landed": outs.iter().map(|o| o.landed).sum::<usize>(), "retryable": outs.iter().map(|o| o.retryable).sum::<usize>(),
            "refresh_errors": outs.iter().map(|o| o.refresh_errors).sum::<usize>(),
            "commits_per_s": f64::from(args.commits * args.writers) / wall, "wall_s": wall,
            "txn_check_ok": check.0, "final_version": check.1, "summary": all.json()})
    );
    Ok(())
}

pub async fn load(args: crate::LoadArgs) -> Result<()> {
    store::rss_watchdog(args.rss_cap_gib, format!("kernel load {}", args.table_url));
    let engine = engine(&args.table_url)?;
    let url = table_url(&args.table_url);
    blocking(move || {
        let t0 = Instant::now();
        let snap = Snapshot::builder_for(url.as_str()).build(engine.as_ref())?;
        let open_s = t0.elapsed().as_secs_f64();
        let fields = snap.schema().fields().count();
        let t1 = Instant::now();
        let txn = snap.get_app_id_version(&args.app_id, engine.as_ref())?;
        println!(
            "{}",
            json!({"kind": "load", "lib": "kernel", "table": args.table_url.as_str(), "open_s": open_s,
                "txn_s": t1.elapsed().as_secs_f64(), "total_s": t0.elapsed().as_secs_f64(),
                "rss_mib": store::rss_mib(), "hwm_mib": store::hwm_mib(), "version": snap.version(),
                "fields": fields, "txn": txn})
        );
        Ok(())
    })
    .await
}

pub async fn checkpoint(args: crate::LoadArgs, spec: Option<String>) -> Result<()> {
    store::rss_watchdog(args.rss_cap_gib, format!("kernel checkpoint {}", args.table_url));
    let engine = engine(&args.table_url)?;
    let url = table_url(&args.table_url);
    blocking(move || {
        let t0 = Instant::now();
        let snap = Snapshot::builder_for(url.as_str()).build(engine.as_ref())?;
        let open_s = t0.elapsed().as_secs_f64();
        let spec = match spec.as_deref() {
            None => None,
            Some("v1") => Some(CheckpointSpec::V1),
            Some("v2") => Some(CheckpointSpec::V2(V2CheckpointConfig::NoSidecar)),
            Some(s) => {
                let n: usize = s.strip_prefix("v2-sidecar-").context("spec: v1 | v2 | v2-sidecar-N")?.parse()?;
                Some(CheckpointSpec::V2(V2CheckpointConfig::WithSidecar { file_actions_per_sidecar_hint: Some(n) }))
            }
        };
        let t1 = Instant::now();
        let (res, _) = snap.checkpoint(engine.as_ref(), spec.as_ref())?;
        println!(
            "{}",
            json!({"kind": "checkpoint", "lib": "kernel", "table": args.table_url.as_str(), "open_s": open_s,
                "write_s": t1.elapsed().as_secs_f64(), "hwm_mib": store::hwm_mib(), "version": snap.version(),
                "result": format!("{res:?}")})
        );
        Ok(())
    })
    .await
}

/// One blind-append commit with txn, conflict retry and the landed check. Returns conflicts seen.
fn commit_once(
    engine: &dyn Engine,
    snap: &mut SnapshotRef,
    schema: &Arc<Schema>,
    adds: &[crate::synth::AddRow],
    app_id: &str,
    v: i64,
    refresh_errors: &mut usize,
) -> Result<usize> {
    let mut conflicts = 0;
    loop {
        let mut txn = snap
            .clone()
            .update_table(UpdateTableOperation::Write, "lrb", true)
            .with_blind_append()
            .with_transaction_id(app_id.to_string(), v)
            .build(engine, Box::new(FileSystemCommitter::new()))?;
        txn.add_files(add_batch(schema, adds, app_id)?);
        let mut res = txn.commit(engine)?;
        while let CommitResult::Retryable(r) = res {
            res = r.transaction.commit(engine)?;
        }
        match res {
            CommitResult::Committed(c) => {
                *snap = match c.post_commit_snapshot() {
                    Some(s) => s.clone(),
                    None => refresh(snap, engine, refresh_errors)?,
                };
                return Ok(conflicts);
            }
            CommitResult::Conflicted(_) => {
                conflicts += 1;
                *snap = refresh(snap, engine, refresh_errors)?;
                if snap.get_app_id_version(app_id, engine)?.is_some_and(|x| x >= v) {
                    return Ok(conflicts);
                }
            }
            CommitResult::Retryable(_) => unreachable!(),
        }
    }
}

#[derive(Default)]
struct MultiOut {
    commits: Summary,
    reloads: Summary,
    ckpt_s: Vec<f64>,
    ckpt_wait_s: Vec<f64>,
    conflicts: usize,
    refresh_errors: usize,
    commits_n: u64,
    behind_ms: f64,
    txn_ok: bool,
}

struct MultiTable {
    idx: usize,
    hot: bool,
    url: Url,
    interval: std::time::Duration,
    ckpt_every: u64,
    ckpt_offset: u64,
}

async fn table_loop(
    t: MultiTable,
    args: crate::MultiArgs,
    deadline: Instant,
    commit_sem: Arc<tokio::sync::Semaphore>,
    ckpt_sem: Arc<tokio::sync::Semaphore>,
) -> Result<MultiOut> {
    let engine = engine(&t.url)?;
    let app_id = format!("inst-multi/t{}#e1", t.idx);
    let (mut snap, schema, mut v) = {
        let (engine, url, app_id) = (engine.clone(), t.url.clone(), app_id.clone());
        let _p = commit_sem.clone().acquire_owned().await?;
        blocking(move || {
            let snap = Snapshot::builder_for(url.as_str()).build(engine.as_ref())?;
            let v = snap.get_app_id_version(&app_id, engine.as_ref())?.unwrap_or(0);
            let schema = add_schema(&snap, engine.as_ref())?;
            Ok((snap, schema, v))
        })
        .await?
    };
    let rows = Arc::new(Rows { active: 1 << 40, tombstones: 0, tombstone_mib: 128, days: 1, stats: args.stats.clone(), seed: t.idx as u64 });
    let mut out = MultiOut { txn_ok: true, ..Default::default() };
    let reload = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ckpt_busy = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ckpt_log = Arc::new(std::sync::Mutex::new((Vec::<f64>::new(), Vec::<f64>::new())));
    // Stagger the first commit across the interval.
    let start = Instant::now() + t.interval.mul_f64((t.idx as f64 * 0.618_034) % 1.0);
    let mut c: u64 = 0;
    loop {
        let due = start + t.interval * u32::try_from(c)?;
        if due >= deadline {
            break;
        }
        tokio::time::sleep_until(due.into()).await;
        if reload.swap(false, std::sync::atomic::Ordering::AcqRel) {
            let (engine, url) = (engine.clone(), t.url.clone());
            let _p = commit_sem.clone().acquire_owned().await?;
            let t0 = Instant::now();
            snap = blocking(move || Ok(Snapshot::builder_for(url.as_str()).build(engine.as_ref())?)).await?;
            out.reloads.record(t0.elapsed(), store::rss_mib());
        }
        v += 1;
        let adds = rows.adds(c, c + 1);
        let permit = commit_sem.clone().acquire_owned().await?;
        let t0 = Instant::now();
        let (e2, s2, a2, sc) = (engine.clone(), snap.clone(), app_id.clone(), schema.clone());
        let (s2, conf, rerr) = blocking(move || {
            let mut s = s2;
            let mut rerr = 0;
            let conf = commit_once(e2.as_ref(), &mut s, &sc, &adds, &a2, v, &mut rerr)?;
            Ok((s, conf, rerr))
        })
        .await?;
        drop(permit);
        snap = s2;
        out.commits.record(t0.elapsed(), store::rss_mib());
        out.behind_ms = out.behind_ms.max(Instant::now().saturating_duration_since(due).as_secs_f64() * 1000.0);
        out.conflicts += conf;
        out.refresh_errors += rerr;
        out.commits_n += 1;
        c += 1;
        if t.ckpt_every > 0 && c % t.ckpt_every == t.ckpt_offset && !ckpt_busy.swap(true, std::sync::atomic::Ordering::AcqRel) {
            let (engine, s, sem, reload, busy, log) =
                (engine.clone(), snap.clone(), ckpt_sem.clone(), reload.clone(), ckpt_busy.clone(), ckpt_log.clone());
            let (hot, idx) = (t.hot, t.idx);
            tokio::spawn(async move {
                let q = Instant::now();
                let Ok(_p) = sem.acquire_owned().await else { return };
                let wait = q.elapsed().as_secs_f64();
                let t0 = Instant::now();
                let r = blocking(move || Ok(s.checkpoint(engine.as_ref(), None)?)).await;
                let el = t0.elapsed().as_secs_f64();
                match r {
                    Ok(_) => {
                        eprintln!("{}", json!({"trace": "ckpt", "hot": hot, "t": idx, "wait_s": wait, "s": el, "rss_mib": store::rss_mib()}));
                        let mut l = log.lock().unwrap();
                        l.0.push(el);
                        l.1.push(wait);
                    }
                    Err(e) => eprintln!("{}", json!({"trace": "ckpt_err", "t": idx, "err": e.to_string()})),
                }
                reload.store(true, std::sync::atomic::Ordering::Release);
                busy.store(false, std::sync::atomic::Ordering::Release);
            });
        }
    }
    // Let an in-flight checkpoint finish before reporting.
    while ckpt_busy.load(std::sync::atomic::Ordering::Acquire) {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    {
        let l = ckpt_log.lock().unwrap();
        out.ckpt_s = l.0.clone();
        out.ckpt_wait_s = l.1.clone();
    }
    let (engine, url, a) = (engine.clone(), t.url.clone(), app_id.clone());
    let got = blocking(move || Ok(Snapshot::builder_for(url.as_str()).build(engine.as_ref())?.get_app_id_version(&a, engine.as_ref())?)).await?;
    out.txn_ok = got == Some(v);
    Ok(out)
}

/// Q2: one hot table plus N warm tables committing in one process, with caps and staggering.
pub async fn multi(args: crate::MultiArgs) -> Result<()> {
    store::rss_watchdog(args.rss_cap_gib, "kernel multi".into());
    let commit_sem = Arc::new(tokio::sync::Semaphore::new(args.max_commits));
    let ckpt_sem = Arc::new(tokio::sync::Semaphore::new(args.max_ckpts));
    let base_rss = store::rss_mib();
    let deadline = Instant::now() + std::time::Duration::from_secs(args.duration_s);
    let mut tables = Vec::new();
    if let Some(h) = &args.hot {
        tables.push(MultiTable {
            idx: 0,
            hot: true,
            url: table_url(h),
            interval: std::time::Duration::from_millis(args.hot_interval_ms),
            ckpt_every: args.hot_ckpt_every,
            ckpt_offset: 0,
        });
    }
    for i in 0..args.warm_count {
        let u = Url::parse(&format!("{}{i:03}", args.warm_prefix.as_deref().context("--warm-prefix")?))?;
        let every = args.warm_ckpt_every;
        tables.push(MultiTable {
            idx: i + 1,
            hot: false,
            url: table_url(&u),
            interval: std::time::Duration::from_millis(args.warm_interval_ms),
            ckpt_every: every,
            ckpt_offset: if every > 0 { (i as u64 * every / args.warm_count.max(1) as u64) % every } else { 0 },
        });
    }
    let sampler = {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s2 = stop.clone();
        let t0 = Instant::now();
        let h = std::thread::spawn(move || {
            let mut max = 0f64;
            while !s2.load(std::sync::atomic::Ordering::Acquire) {
                let r = store::rss_mib();
                max = max.max(r);
                eprintln!("{}", json!({"trace": "rss", "t_s": t0.elapsed().as_secs_f64(), "rss_mib": r}));
                std::thread::sleep(std::time::Duration::from_secs(5));
            }
            max
        });
        (stop, h)
    };
    let mut handles = Vec::new();
    for t in tables {
        let hot = t.hot;
        handles.push((hot, tokio::spawn(table_loop(t, args.clone(), deadline, commit_sem.clone(), ckpt_sem.clone()))));
    }
    let (mut hot, mut warm) = (MultiOut { txn_ok: true, ..Default::default() }, MultiOut { txn_ok: true, ..Default::default() });
    for (is_hot, h) in handles {
        let o = h.await??;
        let d = if is_hot { &mut hot } else { &mut warm };
        d.commits.merge(&o.commits);
        d.reloads.merge(&o.reloads);
        d.ckpt_s.extend(o.ckpt_s);
        d.ckpt_wait_s.extend(o.ckpt_wait_s);
        d.conflicts += o.conflicts;
        d.refresh_errors += o.refresh_errors;
        d.commits_n += o.commits_n;
        d.behind_ms = d.behind_ms.max(o.behind_ms);
        d.txn_ok &= o.txn_ok;
    }
    sampler.0.store(true, std::sync::atomic::Ordering::Release);
    let rss_max = sampler.1.join().unwrap_or(0.0);
    let pct = |v: &[f64], p: f64| -> f64 {
        if v.is_empty() {
            return 0.0;
        }
        let mut s = v.to_vec();
        s.sort_by(f64::total_cmp);
        s[((s.len() - 1) as f64 * p).round() as usize]
    };
    let cls = |o: &MultiOut| {
        json!({"commits": o.commits_n, "conflicts": o.conflicts, "refresh_errors": o.refresh_errors, "txn_ok": o.txn_ok,
            "max_behind_ms": o.behind_ms, "commit": o.commits.json(), "reload": o.reloads.json(),
            "ckpts": o.ckpt_s.len(), "ckpt_p50_s": pct(&o.ckpt_s, 0.5), "ckpt_max_s": pct(&o.ckpt_s, 1.0),
            "ckpt_wait_max_s": pct(&o.ckpt_wait_s, 1.0)})
    };
    println!(
        "{}",
        json!({"kind": "multi", "lib": "kernel", "warm_count": args.warm_count, "hot": args.hot.as_ref().map(Url::as_str),
            "duration_s": args.duration_s, "max_commits": args.max_commits, "max_ckpts": args.max_ckpts,
            "rss_base_mib": base_rss, "rss_max_mib": rss_max, "rss_end_mib": store::rss_mib(), "hwm_mib": store::hwm_mib(),
            "hot_out": cls(&hot), "warm_out": cls(&warm)})
    );
    Ok(())
}
