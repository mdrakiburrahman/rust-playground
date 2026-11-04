//! The Delta operations the commit loop uses, over delta-rs 1.1.1 (`dl1`) or upstream
//! delta_kernel 0.30.1 with its default engine (`kernel`).
//!
//! Both backends sit on the same [`InstrumentedStore`], so every injected fault applies to both.
//!
//! The central semantic difference: delta-rs rebases a conflicting commit onto the newer table
//! version after its logical conflict checker accepts it (up to `--library-max-retries`), while
//! kernel has no conflict checker and no rebase. A kernel commit is a put-if-absent of exactly
//! `snapshot.version() + 1`; `Conflicted` only means that file already exists. This backend
//! reports `Conflicted` as an error, so the loop's refresh + landed check decides what happened.

use crate::commit_loop::{CheckpointExec, KIND_KEY};
use crate::store::{Counters, FaultPlan, InstrumentedStore};
use crate::table::now_millis;
use crate::wal::FileReady;
use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use deltalake_core::DeltaTable;
use deltalake_core::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake_core::kernel::{Action, Add, Transaction};
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use kernel030::arrow::array::{RecordBatch, StringArray};
use kernel030::arrow::datatypes::{DataType, Field, Schema};
use kernel030::arrow::json::ReaderBuilder;
use kernel030::committer::FileSystemCommitter;
use kernel030::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use kernel030::engine::arrow_data::ArrowEngineData;
use kernel030::snapshot::{Snapshot, SnapshotRef};
use kernel030::transaction::{CommitResult, UpdateTableOperation};
use kernel030::{Engine, EngineData};
use kernel030_engine::DefaultEngine;
use kernel030_engine::executor::tokio::TokioMultiThreadExecutor;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// Delta library behind `dcb run`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, clap::ValueEnum)]
pub enum Lib {
    /// deltalake-core 1.1.1 (`CommitBuilder`, conflict checker, library retries).
    #[default]
    Dl1,
    /// delta_kernel 0.30.1 + default engine: put-if-absent at snapshot + 1, no conflict checker.
    Kernel,
}

/// `engineInfo` of kernel commits.
const ENGINE_INFO: &str = "dcb";

/// Settings of a data commit. Ownership commits (`None`) use the library defaults.
#[derive(Clone, Copy, Debug)]
pub struct DataOpts {
    pub inline_checkpoint: bool,
    pub cleanup_expired_logs: Option<bool>,
    /// delta-rs: internal VersionAlreadyExists retries. Kernel: re-commits of a `Retryable`
    /// result (same version).
    pub max_retries: usize,
}

/// One commit: `txn` actions, add actions and the `dcbKind` commitInfo marker.
pub struct CommitReq<'a> {
    pub txns: Vec<(String, i64)>,
    pub files: &'a [FileReady],
    pub kind: &'a str,
    pub data: Option<DataOpts>,
}

/// Kernel snapshot plus what is needed to commit and read the log.
pub struct KernelTable {
    engine: Arc<dyn Engine>,
    /// Table root with a trailing slash.
    url: Url,
    store: Arc<dyn ObjectStore>,
    /// `<table path>/_delta_log` in `store` (rooted like `url`).
    log_prefix: Path,
    snap: SnapshotRef,
    /// Arrow schema for `add_files`, built on the first data commit.
    add_schema: Option<Arc<Schema>>,
}

/// The table under a commit loop.
pub enum Backend {
    Dl1(DeltaTable),
    Kernel(Box<KernelTable>),
}

fn add_action(file: &FileReady) -> Action {
    Action::Add(Add {
        path: file.path.clone(),
        partition_values: Default::default(),
        size: file.size,
        modification_time: file.upload_done_ms,
        data_change: true,
        stats: Some(format!("{{\"numRecords\":{}}}", file.rows)),
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    })
}

const fn write_op() -> DeltaOperation {
    DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    }
}

impl Backend {
    pub fn version(&self) -> Option<u64> {
        match self {
            Self::Dl1(table) => table.version(),
            Self::Kernel(k) => Some(k.snap.version()),
        }
    }

    /// One incremental refresh attempt. The loop retries.
    pub async fn refresh(&mut self) -> Result<()> {
        match self {
            Self::Dl1(table) => Ok(table.update_incremental(None).await?),
            Self::Kernel(k) => {
                let (engine, snap) = (Arc::clone(&k.engine), Arc::clone(&k.snap));
                // A local-fs listing that races a concurrent commit's creation can show N+1
                // without N ("Table version N is missing"); the loop's refresh retries.
                k.snap = blocking(move || Ok(Snapshot::builder_from(snap).build(engine.as_ref())?)).await?;
                Ok(())
            }
        }
    }

    pub async fn txn_version_of(&self, app_id: &str) -> Result<Option<i64>> {
        match self {
            Self::Dl1(table) => {
                let Ok(state) = table.snapshot() else {
                    return Ok(None);
                };
                Ok(state.transaction_version(table.log_store().as_ref(), app_id).await?)
            }
            Self::Kernel(k) => {
                let (engine, snap, app) = (Arc::clone(&k.engine), Arc::clone(&k.snap), app_id.to_owned());
                blocking(move || Ok(snap.get_app_id_version(&app, engine.as_ref())?)).await
            }
        }
    }

    /// Commit `req` on the current snapshot. Returns the library retries. On success the
    /// snapshot is the post-commit snapshot.
    pub async fn commit(&mut self, req: CommitReq<'_>) -> Result<u64> {
        match self {
            Self::Dl1(table) => {
                let txns = req
                    .txns
                    .into_iter()
                    .map(|(app, v)| Transaction::new_with_last_update(app, v, Some(now_millis())))
                    .collect();
                let mut props = match req.data {
                    Some(opts) => CommitProperties::default()
                        .with_create_checkpoint(opts.inline_checkpoint)
                        .with_cleanup_expired_logs(opts.cleanup_expired_logs)
                        .with_max_retries(opts.max_retries),
                    None => CommitProperties::default(),
                };
                props = props
                    .with_application_transactions(txns)
                    .with_metadata([(KIND_KEY.to_owned(), serde_json::Value::from(req.kind))]);
                let state = table.snapshot()?;
                let finalized = CommitBuilder::from(props)
                    .with_actions(req.files.iter().map(add_action).collect())
                    .build(Some(state), table.log_store(), write_op())
                    .await?;
                let retries = finalized.metrics.num_retries;
                table.state = Some(finalized.snapshot());
                Ok(retries)
            }
            Self::Kernel(k) => {
                let engine = Arc::clone(&k.engine);
                let snap = Arc::clone(&k.snap);
                let schema = k.add_schema.clone();
                let files = req.files.to_vec();
                let kind = req.kind.to_owned();
                let txns = req.txns;
                let data = req.data;
                let (snap, schema, retries) = blocking(move || {
                    let schema = match (schema, files.is_empty()) {
                        (Some(schema), _) => Some(schema),
                        (None, false) => Some(add_schema(&snap, engine.as_ref())?),
                        (None, true) => None,
                    };
                    let (snap, retries) =
                        kernel_commit(engine.as_ref(), &snap, schema.as_ref(), &txns, &files, &kind, data)?;
                    Ok((snap, schema, retries))
                })
                .await?;
                k.snap = snap;
                if k.add_schema.is_none() {
                    k.add_schema = schema;
                }
                Ok(retries)
            }
        }
    }

    /// The bytes of commit `version`'s log entry, `None` if absent.
    pub async fn read_commit_entry(&self, version: u64) -> Result<Option<Bytes>> {
        match self {
            Self::Dl1(table) => Ok(table.log_store().read_commit_entry(version).await?),
            Self::Kernel(k) => {
                let path = Path::from(format!("{}/{version:020}.json", k.log_prefix.as_ref()));
                match k.store.get(&path).await {
                    Ok(result) => Ok(Some(result.bytes().await?)),
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(error) => Err(error.into()),
                }
            }
        }
    }

    /// Write a checkpoint at `version` from a fresh snapshot. Kernel always runs it on a
    /// blocking thread (`exec` applies to delta-rs only).
    pub async fn write_checkpoint(&self, version: u64, exec: CheckpointExec) -> Result<()> {
        match self {
            Self::Dl1(table) => match exec {
                CheckpointExec::Worker => Ok(deltalake_core::checkpoints::create_checkpoint(table, None).await?),
                CheckpointExec::Blocking => {
                    let log_store = table.log_store();
                    let engine = log_store.engine(None);
                    let root = log_store.transaction_url(None)?;
                    tokio::task::spawn_blocking(move || -> Result<()> {
                        let snapshot = delta_kernel::Snapshot::builder_for(root)
                            .at_version(version)
                            .build(engine.as_ref())?;
                        snapshot.checkpoint(engine.as_ref(), None)?;
                        Ok(())
                    })
                    .await
                    .context("checkpoint task")?
                }
            },
            Self::Kernel(k) => {
                let (engine, url) = (Arc::clone(&k.engine), k.url.clone());
                blocking(move || {
                    let snapshot = Snapshot::builder_for(url.as_str())
                        .at_version(version)
                        .build(engine.as_ref())?;
                    snapshot.checkpoint(engine.as_ref(), None)?;
                    Ok(())
                })
                .await
            }
        }
    }

    /// Replace the snapshot with a fresh load at `version` (from its checkpoint). The old
    /// snapshot stays on error.
    pub async fn reload_at(&mut self, version: u64) -> Result<()> {
        match self {
            Self::Dl1(table) => {
                let previous = table.state.take();
                match table.update_incremental(Some(version)).await {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        table.state = previous;
                        Err(error.into())
                    }
                }
            }
            Self::Kernel(k) => {
                let (engine, url) = (Arc::clone(&k.engine), k.url.clone());
                k.snap = blocking(move || {
                    Ok(Snapshot::builder_for(url.as_str())
                        .at_version(version)
                        .build(engine.as_ref())?)
                })
                .await?;
                Ok(())
            }
        }
    }
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f).await.context("kernel task")?
}

/// Default engine over `store`. Kernel calls block; the multi-thread executor runs the engine's
/// I/O on the ambient runtime while the caller waits on a blocking thread. The current-thread
/// commit runtime cannot host it, so that case gets the engine's own background runtime.
fn engine(store: Arc<dyn ObjectStore>) -> Arc<dyn Engine> {
    let handle = tokio::runtime::Handle::current();
    let builder = DefaultEngine::builder(store);
    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
        Arc::new(
            builder
                .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(handle)))
                .build(),
        )
    } else {
        Arc::new(builder.build())
    }
}

fn table_url(url: &Url) -> Url {
    let mut u = url.clone();
    if !u.path().ends_with('/') {
        u.set_path(&format!("{}/", u.path()));
    }
    u
}

/// Open the kernel snapshot of the table at `url` over the container- or filesystem-rooted
/// `root` (the same store `table::open` gives delta-rs), wrapped with counters and `faults`.
/// Kernel resolves `url.path()` against the store, so the root store and the URL agree on
/// paths for `file://` and for `abfss://container@account/...` alike.
pub async fn open_kernel(url: &Url, root: Arc<dyn ObjectStore>, faults: Arc<FaultPlan>) -> Result<(Backend, Arc<Counters>)> {
    let counters = Arc::new(Counters::default());
    let store: Arc<dyn ObjectStore> = Arc::new(InstrumentedStore::new(root, Arc::clone(&counters), faults));
    let engine = engine(Arc::clone(&store));
    let url = table_url(url);
    let log_prefix = Path::from(format!("{}/_delta_log", Path::from_url_path(url.path())?.as_ref()));
    let snap = {
        let (engine, url) = (Arc::clone(&engine), url.clone());
        blocking(move || Ok(Snapshot::builder_for(url.as_str()).build(engine.as_ref())?)).await?
    };
    let table = KernelTable {
        engine,
        url,
        store,
        log_prefix,
        snap,
        add_schema: None,
    };
    Ok((Backend::Kernel(Box::new(table)), counters))
}

/// [`open_kernel`] with retries, like `table::open_with_retry`.
pub async fn open_kernel_with_retry(
    url: &Url,
    root: Arc<dyn ObjectStore>,
    faults: Arc<FaultPlan>,
) -> Result<(Backend, Arc<Counters>)> {
    let mut delay = Duration::from_millis(100);
    let mut attempt = 0;
    loop {
        attempt += 1;
        match open_kernel(url, Arc::clone(&root), Arc::clone(&faults)).await {
            Ok(opened) => return Ok(opened),
            Err(error) if attempt < 30 => {
                tracing::warn!(%error, attempt, "kernel open failed; retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            Err(error) => return Err(error),
        }
    }
}

/// Arrow schema for `add_files`: the base add-file schema with `stats` swapped for the table's
/// stats schema.
fn add_schema(snap: &SnapshotRef, engine: &dyn Engine) -> Result<Arc<Schema>> {
    let txn = Arc::clone(snap)
        .update_table(UpdateTableOperation::Write, ENGINE_INFO, true)
        .build(engine, Box::new(FileSystemCommitter::new()))?;
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

fn add_batch(schema: &Arc<Schema>, files: &[FileReady]) -> Result<Box<dyn EngineData>> {
    let mut buf = Vec::new();
    for f in files {
        let v = serde_json::json!({"path": f.path, "partitionValues": {}, "size": f.size,
            "modificationTime": f.upload_done_ms, "dataChange": true, "stats": {"numRecords": f.rows}});
        serde_json::to_writer(&mut buf, &v)?;
        buf.push(b'\n');
    }
    let mut decoder = ReaderBuilder::new(Arc::clone(schema)).build_decoder()?;
    decoder.decode(&buf)?;
    let batch = decoder.flush()?.ok_or_else(|| anyhow!("empty add batch"))?;
    Ok(Box::new(ArrowEngineData::new(batch)))
}

/// One-row engine commitInfo carrying the `dcbKind` marker at the top level of `commitInfo`,
/// where delta-rs `with_metadata` puts it (kernel passes unknown engine fields through).
fn kind_commit_info(kind: &str) -> Result<(Box<dyn EngineData>, kernel030::schema::SchemaRef)> {
    let schema = Arc::new(Schema::new(vec![Field::new(KIND_KEY, DataType::Utf8, true)]));
    let kernel_schema: kernel030::schema::Schema = schema.as_ref().try_into_kernel()?;
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![kind]))])?;
    Ok((Box::new(ArrowEngineData::new(batch)), Arc::new(kernel_schema)))
}

/// Commit at exactly `snap.version() + 1`. A `Retryable` result (I/O error from the put) is
/// re-committed as the same transaction, so at the same version: if the first put landed, the
/// re-commit reports `Conflicted`. `Conflicted` is returned as an error; kernel never moves a
/// transaction to a later version.
fn kernel_commit(
    engine: &dyn Engine,
    snap: &SnapshotRef,
    schema: Option<&Arc<Schema>>,
    txns: &[(String, i64)],
    files: &[FileReady],
    kind: &str,
    data: Option<DataOpts>,
) -> Result<(SnapshotRef, u64)> {
    let has_files = !files.is_empty();
    // A txn-only commit is a metadata-only write: no blind append (which requires adds) and
    // dataChange false.
    let mut builder = Arc::clone(snap).update_table(UpdateTableOperation::Write, ENGINE_INFO, has_files);
    if has_files {
        builder = builder.with_blind_append();
    }
    for (app, v) in txns {
        builder = builder.with_transaction_id(app.clone(), *v);
    }
    let (info, info_schema) = kind_commit_info(kind)?;
    builder = builder.with_commit_info(info, info_schema);
    let mut txn = builder.build(engine, Box::new(FileSystemCommitter::new()))?;
    if has_files {
        let schema = schema.context("add schema")?;
        txn.add_files(add_batch(schema, files)?);
    }
    let max_retries = data.map_or(15, |d| d.max_retries) as u64;
    let mut retries = 0u64;
    let mut result = txn.commit(engine)?;
    let committed = loop {
        match result {
            CommitResult::Committed(c) => break c,
            CommitResult::Conflicted(c) => {
                bail!("kernel: version {} already exists (Conflicted)", c.conflict_version())
            }
            CommitResult::Retryable(r) if retries < max_retries => {
                retries += 1;
                tracing::warn!(error = %r.error, retries, "kernel commit retryable; re-committing same version");
                result = r.transaction.commit(engine)?;
            }
            CommitResult::Retryable(r) => {
                bail!("kernel: retryable commit error after {retries} re-commits: {}", r.error)
            }
        }
    };
    let version = committed.commit_version();
    let post = match committed.post_commit_snapshot() {
        Some(s) => Arc::clone(s),
        None => Snapshot::builder_from(Arc::clone(snap))
            .at_version(version)
            .build(engine)?,
    };
    if data.is_some_and(|d| d.inline_checkpoint) {
        let interval = post.table_properties().checkpoint_interval.map_or(100, std::num::NonZero::get);
        if version > 0 && version % interval == 0
            && let Err(error) = post.checkpoint(engine, None)
        {
            tracing::warn!(%error, version, "kernel inline checkpoint failed");
        }
    }
    Ok((post, retries))
}
