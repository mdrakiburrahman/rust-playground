#![cfg(feature = "delta")]

//! Reclamation of staged files whose publication was deduplicated before any commit
//! attempt carried them, and retention of every file that an attempt may have published.

use anyhow::{Context, Result};
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_deltalake::delta::{
    DeltaTable, PublicationFence, PublicationFenceContext, PublicationFenceError, StagedAppend,
};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, OnceLock};

fn metrics() -> &'static (SdkMeterProvider, InMemoryMetricExporter) {
    static METRICS: OnceLock<(SdkMeterProvider, InMemoryMetricExporter)> = OnceLock::new();
    METRICS.get_or_init(|| {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        opentelemetry::global::set_meter_provider(provider.clone());
        (provider, exporter)
    })
}

fn publication_events(outcome: &str) -> Result<u64> {
    let (provider, exporter) = metrics();
    provider.force_flush()?;
    let mut total = 0;
    for resource in exporter.get_finished_metrics()? {
        for metric in resource.scope_metrics().flat_map(|scope| scope.metrics()) {
            if metric.name() != "delta.publication.events" {
                continue;
            }
            if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
                total = total.max(
                    sum.data_points()
                        .filter(|point| {
                            point
                                .attributes()
                                .any(|kv| kv.key.as_str() == "outcome" && kv.value.as_str() == outcome)
                        })
                        .map(|point| point.value())
                        .sum(),
                );
            }
        }
    }
    Ok(total)
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

fn batch(id: i32) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int32Array::from(vec![id]))],
    )?)
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread().enable_all().build()?)
}

fn stage(table: &Arc<DeltaTable>, id: i32, publication: Option<&str>) -> Result<StagedAppend> {
    runtime()?.block_on(async {
        let input = batch(id)?;
        let mut file = table.new_file(input.schema())?;
        if let Some(app_id) = publication {
            file = file.with_transaction(app_id.to_owned(), 0)?;
        }
        file.write(&input).await?;
        file.stage().await
    })
}

/// Create the table with one published file at version 1.
fn seed(table: &Arc<DeltaTable>) -> Result<()> {
    let seeded = table.commit_files_with_transaction(&[stage(table, 0, Some("seed"))?], "seed-outer", 0)?;
    anyhow::ensure!(seeded == 1, "seed committed {seeded}");
    Ok(())
}

fn parquet_files(root: &Path) -> Result<usize> {
    let mut count = 0;
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "parquet") {
            count += 1;
        }
    }
    Ok(count)
}

fn adds_in(root: &Path, version: u64) -> Result<usize> {
    let log = fs::read_to_string(root.join(format!("_delta_log/{version:020}.json")))?;
    Ok(log.lines().filter(|line| line.contains("\"add\":")).count())
}

/// Scenario: a publication is committed, then the same identity arrives again as a new
/// delivery with its own freshly staged file.
/// Guarantees: the replay returns the original version, its never-submitted stage is
/// deleted, and only the published file remains on disk.
#[test]
fn new_delivery_replay_reclaims_unsubmitted_stage() -> Result<()> {
    metrics();
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open(root.path())?;
    let deliver = || {
        runtime()?.block_on(async {
            let input = batch(1)?;
            let mut file = table.new_file(input.schema())?.with_transaction("replayed".into(), 0)?;
            file.write(&input).await?;
            file.finish().await
        })
    };
    let published = deliver()?;
    let replay = deliver()?;
    assert_eq!((published.version, replay.version), (1, 1));
    assert!(published.file.path.exists());
    assert!(
        !replay.file.path.exists(),
        "deduplicated new-delivery stage must be reclaimed"
    );
    assert_eq!(parquet_files(root.path())?, 1);
    assert_eq!(table.version()?, 1);
    assert!(publication_events("orphan_reclaimed")? >= 1);
    Ok(())
}

/// Scenario: one commit call carries an already-published file and a new file.
/// Guarantees: the new file commits, the deduplicated never-submitted file is
/// reclaimed, and the published original is untouched.
#[test]
fn mixed_pending_and_deduplicated_files_reclaim_only_the_duplicate() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open(root.path())?;
    seed(&table)?;
    let duplicate = stage(&table, 0, Some("seed"))?;
    let fresh = stage(&table, 2, Some("fresh"))?;
    assert_eq!(parquet_files(root.path())?, 3);
    assert_eq!(
        table.commit_files_with_transaction(&[duplicate, fresh], "second", 0)?,
        2
    );
    assert_eq!(adds_in(root.path(), 2)?, 1);
    assert_eq!(parquet_files(root.path())?, 2);
    assert_eq!(
        (table.lookup_txn("seed")?, table.lookup_txn("fresh")?),
        (Some(0), Some(0))
    );
    Ok(())
}

/// Scenario: a staged file is reclaimed by a deduplicated call and later offered to
/// a different publication.
/// Guarantees: the later commit is refused before any log write instead of
/// publishing an Add for a deleted file.
#[test]
fn reclaimed_stage_cannot_be_committed_later() -> Result<()> {
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open(root.path())?;
    seed(&table)?;
    assert_eq!(table.commit_files_with_transaction(&[], "outer", 0)?, 2);
    let shared = stage(&table, 1, None)?;
    assert_eq!(
        table.commit_files_with_transaction(std::slice::from_ref(&shared), "outer", 0)?,
        2
    );
    assert_eq!(parquet_files(root.path())?, 1);
    let error = table
        .commit_files_with_transaction(std::slice::from_ref(&shared), "other", 0)
        .err()
        .context("reclaimed stage must be refused")?;
    assert!(format!("{error:#}").contains("reclaimed"), "{error:#}");
    assert_eq!(table.version()?, 2);
    assert_eq!(table.lookup_txn("other")?, None);
    Ok(())
}

#[derive(Clone, Default)]
struct Logs(Arc<std::sync::Mutex<Vec<u8>>>);

impl Logs {
    fn text(&self) -> Result<String> {
        let bytes = self.0.lock().map_err(|_| anyhow::anyhow!("log buffer poisoned"))?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
}

impl Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("log buffer poisoned"))?
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

struct PauseFence {
    armed: AtomicBool,
    entered: Barrier,
    release: Barrier,
}

impl PublicationFence for PauseFence {
    fn check_before_commit(&self, _: &PublicationFenceContext<'_>) -> Result<(), PublicationFenceError> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.wait();
            self.release.wait();
        }
        Ok(())
    }
}

/// Scenario: two lock-free table handles borrow the same staged file. One commit
/// attempt has claimed it and is paused before its log write while the other call
/// deduplicates.
/// Guarantees: the deduplicating call cannot delete the in-flight file; the paused
/// attempt then publishes it with the file present on disk.
#[test]
fn concurrent_submission_wins_over_reclaim_of_shared_stage() -> Result<()> {
    let root = tempfile::tempdir()?;
    let url =
        url::Url::from_directory_path(fs::canonicalize(root.path())?).map_err(|()| anyhow::anyhow!("table URL"))?;
    let deduper = DeltaTable::open_url(url.clone(), schema(), Vec::new())?;
    seed(&deduper)?;
    let fence = Arc::new(PauseFence {
        armed: AtomicBool::new(false),
        entered: Barrier::new(2),
        release: Barrier::new(2),
    });
    let submitter = DeltaTable::open_uri_with_fence(url.as_str(), Arc::<PauseFence>::clone(&fence))?;
    assert_eq!(deduper.commit_files_with_transaction(&[], "old", 0)?, 2);
    let shared = stage(&deduper, 7, None)?;
    fence.armed.store(true, Ordering::SeqCst);
    let (deduped, retained, committed) = std::thread::scope(|scope| -> Result<(u64, usize, u64)> {
        let submission =
            scope.spawn(|| submitter.commit_files_with_transaction(std::slice::from_ref(&shared), "new", 0));
        fence.entered.wait();
        let deduped = deduper.commit_files_with_transaction(std::slice::from_ref(&shared), "old", 0);
        let retained = parquet_files(root.path());
        fence.release.wait();
        let committed = submission.join().map_err(|_| anyhow::anyhow!("submitter panicked"))??;
        Ok((deduped?, retained?, committed))
    })?;
    assert_eq!(retained, 2, "in-flight stage must not be reclaimed");
    assert_eq!((deduped, committed), (2, 3));
    assert_eq!(adds_in(root.path(), 3)?, 1);
    assert_eq!(parquet_files(root.path())?, 2);
    Ok(())
}

struct ExternalTxnFence {
    armed: AtomicBool,
}

impl PublicationFence for ExternalTxnFence {
    fn check_before_commit(&self, context: &PublicationFenceContext<'_>) -> Result<(), PublicationFenceError> {
        if self.armed.swap(false, Ordering::SeqCst) {
            let publish = || -> std::io::Result<()> {
                let mut file = fs::OpenOptions::new().write(true).create_new(true).open(
                    context
                        .table_path()
                        .join("_delta_log")
                        .join(format!("{:020}.json", context.proposed_version())),
                )?;
                file.write_all(b"{\"txn\":{\"appId\":\"raced\",\"version\":0,\"lastUpdated\":1}}\n")?;
                file.sync_all()
            };
            publish().map_err(|error| PublicationFenceError::check_failed(error.to_string()))?;
        }
        Ok(())
    }
}

/// Scenario: a file's first commit attempt collides with another delivery that
/// publishes the same identity; the bounded retry then observes the identity.
/// Guarantees: the call deduplicates without a second Add, and the file, which was
/// already offered to an attempt, is conservatively retained rather than deleted.
#[test]
fn collision_retry_retains_submitted_stage() -> Result<()> {
    let root = tempfile::tempdir()?;
    let fence = Arc::new(ExternalTxnFence {
        armed: AtomicBool::new(false),
    });
    let table = DeltaTable::open_with_fence(root.path(), Arc::<ExternalTxnFence>::clone(&fence))?;
    seed(&table)?;
    let raced = stage(&table, 3, Some("raced"))?;
    fence.armed.store(true, Ordering::SeqCst);
    assert_eq!(table.commit_files_with_transaction(&[raced], "outer", 0)?, 3);
    assert_eq!(adds_in(root.path(), 2)?, 0);
    assert_eq!(adds_in(root.path(), 3)?, 0);
    assert_eq!(table.lookup_txn("raced")?, Some(0));
    assert_eq!(parquet_files(root.path())?, 2, "submitted stage must be retained");
    Ok(())
}

/// Scenario: a call whose outer identity is already committed carries a fresh stage
/// of a different table nested under this table's root.
/// Guarantees: the dedupe does not reclaim the foreign stage, which keeps its file
/// and state and then commits normally to its own table.
#[test]
fn deduplicated_call_never_reclaims_foreign_nested_stage() -> Result<()> {
    let root = tempfile::tempdir()?;
    let outer = DeltaTable::open(root.path())?;
    seed(&outer)?;
    assert_eq!(outer.commit_files_with_transaction(&[], "outer", 0)?, 2);
    let nested_root = root.path().join("nested");
    fs::create_dir(&nested_root)?;
    let nested = DeltaTable::open(&nested_root)?;
    let foreign = stage(&nested, 5, None)?;
    assert_eq!(
        outer.commit_files_with_transaction(std::slice::from_ref(&foreign), "outer", 0)?,
        2
    );
    assert_eq!(parquet_files(&nested_root)?, 1, "foreign stage must survive");
    assert_eq!(nested.commit_files_with_transaction(&[foreign], "nested", 0)?, 1);
    assert_eq!(adds_in(&nested_root, 1)?, 1);
    Ok(())
}

/// Scenario: a deduplicated replay's stage cannot be deleted (read-only directory).
/// Guarantees: the publication result is still success, the file stays on disk, and
/// the failure is counted as `orphan_cleanup_failed`.
#[cfg(unix)]
#[test]
fn reclaim_failure_is_visible_and_never_fails_publication() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    metrics();
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open(root.path())?;
    seed(&table)?;
    assert_eq!(table.commit_files_with_transaction(&[], "outer", 0)?, 2);
    let replay = stage(&table, 1, None)?;
    let staged: Vec<String> = fs::read_dir(root.path())?
        .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_>>()?;
    let logs = Logs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        // Scope the canary to this crate's events; Kernel's own spans are not ours.
        .with_env_filter(tracing_subscriber::EnvFilter::new("arrow_deltalake=trace"))
        .with_ansi(false)
        .finish();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o555))?;
    let result = {
        let _guard = tracing_subscriber::util::SubscriberInitExt::set_default(subscriber);
        table.commit_files_with_transaction(&[replay], "outer", 0)
    };
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o755))?;
    let text = logs.text()?;
    assert!(
        text.contains("cleanup failed") && text.contains("io_permission_denied"),
        "{text}"
    );
    // Privacy canary: no path component of the table or a staged file is exported.
    let dir_name = root.path().file_name().and_then(|name| name.to_str()).context("dir")?;
    assert!(!text.contains(dir_name), "{text}");
    for name in staged.iter().filter(|name| name.ends_with(".parquet")) {
        assert!(!text.contains(name.as_str()), "{text}");
    }
    assert_eq!(result?, 2);
    assert_eq!(parquet_files(root.path())?, 2);
    assert!(publication_events("orphan_cleanup_failed")? >= 1);
    Ok(())
}
