#![cfg(feature = "delta")]

use anyhow::Result;
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::DeltaTable;
use async_trait::async_trait;
use delta_kernel::object_store::{
    self, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, local::LocalFileSystem, path::Path,
};
use futures::stream::BoxStream;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug, Clone, Copy)]
enum Fault {
    Conflict,
    Uncertain,
    UncertainUnreadable,
    ObserveReads,
}

#[derive(Debug)]
struct FaultStore {
    inner: LocalFileSystem,
    fault: Fault,
    attempts: AtomicUsize,
    reads: AtomicUsize,
    active_reads: AtomicUsize,
    peak_reads: AtomicUsize,
}

impl std::fmt::Display for FaultStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "fault-injecting local store")
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(&self, path: &Path, payload: PutPayload, options: PutOptions) -> object_store::Result<PutResult> {
        if path.as_ref().ends_with("00000000000000000001.json") {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            match self.fault {
                Fault::Conflict => {
                    return Err(object_store::Error::AlreadyExists {
                        path: path.to_string(),
                        source: Box::new(std::io::Error::other("injected external commit conflict")),
                    });
                }
                Fault::Uncertain | Fault::UncertainUnreadable => {
                    self.inner.put_opts(path, payload, options).await?;
                    return Err(object_store::Error::Generic {
                        store: "fault_store",
                        source: Box::new(std::io::Error::other("injected failure after publishing the log")),
                    });
                }
                Fault::ObserveReads => {}
            }
        }
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if matches!(self.fault, Fault::UncertainUnreadable)
            && self.attempts.load(Ordering::SeqCst) != 0
            && path.as_ref().ends_with("00000000000000000001.json")
        {
            return Err(object_store::Error::Generic {
                store: "fault_store",
                source: Box::new(std::io::Error::other("injected reconciliation read failure")),
            });
        }
        if matches!(self.fault, Fault::ObserveReads) && !options.head {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let active = self.active_reads.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_reads.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            let result = self.inner.get_opts(path, options).await;
            self.active_reads.fetch_sub(1, Ordering::SeqCst);
            return result;
        }
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

async fn failed_append(fault: Fault) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(FaultStore {
        inner: LocalFileSystem::new(),
        fault,
        attempts: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
        active_reads: AtomicUsize::new(0),
        peak_reads: AtomicUsize::new(0),
    });
    let table = DeltaTable::open_with_store(directory.path(), Arc::<FaultStore>::clone(&store))?;
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )?;
    let mut file = table.new_file(input.schema())?;
    file.write(&input).await?;
    let result = file.finish().await;
    assert!(result.is_err());
    assert_eq!(
        store.attempts.load(Ordering::Relaxed),
        if matches!(fault, Fault::Conflict) { 8 } else { 1 }
    );
    assert!(!directory.path().join("_delta_log/00000000000000000002.json").exists());
    assert_eq!(table.version()?, if matches!(fault, Fault::Uncertain) { 1 } else { 0 });
    Ok(())
}

/// Scenario: an external writer wins the versioned log's atomic creation.
/// Guarantees: eight CAS attempts exhaust visibly without overwriting an external publication.
#[tokio::test]
async fn commit_conflict_exhausts_bounded_retry_budget() -> Result<()> {
    failed_append(Fault::Conflict).await
}

/// Scenario: storage reports an error after the versioned transaction log was created.
/// Guarantees: an uncertain outcome is returned as failure without duplicating the visible append.
#[tokio::test]
async fn uncertain_published_commit_is_not_replayed() -> Result<()> {
    failed_append(Fault::Uncertain).await
}

/// Scenario: storage publishes a stable transaction but loses its success response.
/// Guarantees: replay discovers that identity and never submits a duplicate log version.
#[tokio::test]
async fn identified_unknown_success_reconciles_before_retry() -> Result<()> {
    identified_unknown_success(Fault::Uncertain, true).await
}

/// Scenario: publication may have succeeded but its transaction cannot be read back.
/// Guarantees: reconciliation failure stops publication instead of guessing or retrying.
#[tokio::test]
async fn unidentified_outcome_after_reconciliation_failure_stops() -> Result<()> {
    identified_unknown_success(Fault::UncertainUnreadable, false).await
}

async fn identified_unknown_success(fault: Fault, succeeds: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(FaultStore {
        inner: LocalFileSystem::new(),
        fault,
        attempts: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
        active_reads: AtomicUsize::new(0),
        peak_reads: AtomicUsize::new(0),
    });
    let table = DeltaTable::open_with_store(directory.path(), Arc::<FaultStore>::clone(&store))?;
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )?;
    let mut file = table
        .new_file(input.schema())?
        .with_transaction("unknown-success".into(), 0)?;
    file.write(&input).await?;
    let result = file.finish().await;
    assert_eq!(result.is_ok(), succeeds, "{result:?}");
    assert_eq!(store.attempts.load(Ordering::SeqCst), 1);
    assert!(!directory.path().join("_delta_log/00000000000000000002.json").exists());
    let published = std::fs::read_to_string(directory.path().join("_delta_log/00000000000000000001.json"))?;
    assert_eq!(published.lines().filter(|line| line.contains("\"add\":")).count(), 1);
    if succeeds {
        assert_eq!(table.lookup_txn("unknown-success")?, Some(0));
        assert_eq!(table.version()?, 1);
    }
    Ok(())
}

/// Scenario: reopening a table must replay many commits before reaching its original metadata.
/// Guarantees: read-ahead stays at eight, replay remains fresh, and historic corruption is visible.
#[tokio::test(flavor = "current_thread")]
async fn history_replay_has_bounded_readahead_and_detects_corruption() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let table = DeltaTable::open(directory.path())?;
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )?;
    let mut file = table.new_file(input.schema())?;
    file.write(&input).await?;
    file.finish().await?;
    // External commits without metadata force replay back to the initial schema.
    let append = std::fs::read(directory.path().join("_delta_log/00000000000000000001.json"))?;
    for version in 2..40 {
        std::fs::write(directory.path().join(format!("_delta_log/{version:020}.json")), &append)?;
    }
    drop(table);
    let store = Arc::new(FaultStore {
        inner: LocalFileSystem::new(),
        fault: Fault::ObserveReads,
        attempts: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
        active_reads: AtomicUsize::new(0),
        peak_reads: AtomicUsize::new(0),
    });
    let table = DeltaTable::open_with_store(directory.path(), Arc::<FaultStore>::clone(&store))?;
    assert_eq!(table.version()?, 39);
    assert!(store.reads.load(Ordering::SeqCst) >= 40);
    assert!((2..=8).contains(&store.peak_reads.load(Ordering::SeqCst)));
    let prepared = table.new_file(input.schema())?;
    std::fs::write(directory.path().join("_delta_log/00000000000000000000.json"), "corrupt")?;
    assert!(table.version().is_err());
    assert!(table.new_file(input.schema()).is_err());
    drop(prepared);
    Ok(())
}
