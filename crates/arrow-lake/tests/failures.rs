#![cfg(feature = "delta")]

use anyhow::Result;
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_lake::delta::DeltaTable;
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
}

#[derive(Debug)]
struct FaultStore {
    inner: LocalFileSystem,
    fault: Fault,
    attempts: AtomicUsize,
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
                Fault::Uncertain => {
                    self.inner.put_opts(path, payload, options).await?;
                    return Err(object_store::Error::Generic {
                        store: "fault_store",
                        source: Box::new(std::io::Error::other("injected failure after publishing the log")),
                    });
                }
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
    assert_eq!(store.attempts.load(Ordering::Relaxed), 1);
    assert!(!directory.path().join("_delta_log/00000000000000000002.json").exists());
    assert_eq!(table.version()?, if matches!(fault, Fault::Uncertain) { 1 } else { 0 });
    Ok(())
}

/// Scenario: an external writer wins the versioned log's atomic creation.
/// Guarantees: Kernel conflict is surfaced and no append is silently replayed.
#[tokio::test]
async fn commit_conflict_is_not_retried() -> Result<()> {
    failed_append(Fault::Conflict).await
}

/// Scenario: storage reports an error after the versioned transaction log was created.
/// Guarantees: an uncertain outcome is returned as failure without duplicating the visible append.
#[tokio::test]
async fn uncertain_published_commit_is_not_replayed() -> Result<()> {
    failed_append(Fault::Uncertain).await
}
