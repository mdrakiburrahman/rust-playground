//! Write-ahead microbatch orchestration with idempotent sink recovery.

use crate::checkpoint::Checkpoint;
use crate::metrics::StreamMetrics;
use crate::reader::{BatchConsumer, TenantQuery, read_file};
use crate::source::{BootstrapManifest, Cursor, DeltaSource, SourceFile};
use anyhow::{Context, Result, ensure};
use arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[async_trait::async_trait]
pub trait TenantSink: Send {
    /// Persistent Delta txn version, not an in-memory replay cache.
    ///
    /// # Errors
    /// Returns transaction lookup/storage failures.
    async fn lookup_transaction(&self, query_id: &str) -> Result<Option<i64>>;
    /// # Errors
    /// Returns invalid batch identity or staging setup failures.
    async fn start_batch(&mut self, query_id: &str, batch: i64) -> Result<()>;
    /// # Errors
    /// Returns encoding, backpressure, schema, or storage failures.
    async fn write(&mut self, batch: RecordBatch, partition_values: &BTreeMap<String, Option<String>>) -> Result<()>;
    /// Atomically publish all staged files plus the stable query/batch txn.
    /// Empty-output progress still records the txn.
    ///
    /// # Errors
    /// Returns transaction publication or identity conflicts.
    async fn commit_batch(&mut self, query_id: &str, batch: i64) -> Result<()>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryIdentity {
    pub source: String,
    pub target: String,
    pub query: TenantQuery,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct SourceOffset {
    table_id: String,
    cursor: Cursor,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct BatchPlan {
    start: Cursor,
    end: Cursor,
    files: Vec<SourceFile>,
    actions_examined: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    Idle,
    Committed { batch: i64, files: usize, replay: bool },
}

pub struct TenantStream {
    source: DeltaSource,
    checkpoint: Checkpoint,
    frozen: BootstrapManifest,
    query: TenantQuery,
    max_actions: usize,
    metrics: Arc<StreamMetrics>,
}

impl TenantStream {
    /// Bind one checkpoint to its source, sink, predicate, and projection.
    ///
    /// # Errors
    /// Rejects inconsistent recovery/configuration and missing frozen manifests.
    pub async fn open(
        source: DeltaSource,
        mut checkpoint: Checkpoint,
        target: String,
        query: TenantQuery,
        max_actions: usize,
        metrics: Arc<StreamMetrics>,
    ) -> Result<Self> {
        let initialized: Result<BootstrapManifest> = async {
            ensure!(max_actions > 0, "max actions must be positive");
            let recovery = checkpoint.recover().await?;
            let frozen = match checkpoint.read_immutable("bootstrap.json").await? {
                Some(bytes) => serde_json::from_slice::<BootstrapManifest>(&bytes)?,
                None => {
                    ensure!(
                        recovery.batch_id == 0 && recovery.pending_offset.is_none(),
                        "checkpoint lost immutable bootstrap manifest"
                    );
                    let frozen = source.capture()?;
                    checkpoint
                        .write_immutable("bootstrap.json", &serde_json::to_vec(&frozen)?)
                        .await?;
                    frozen
                }
            };
            ensure!(
                frozen.source_url == source.url.as_str(),
                "checkpoint source URL mismatch"
            );
            query.columns(&frozen)?;
            let identity = QueryIdentity {
                source: source.url.to_string(),
                target,
                query: query.clone(),
            };
            let identity_bytes = serde_json::to_vec(&identity)?;
            match checkpoint.read_immutable("query.json").await? {
                Some(bytes) => ensure!(
                    bytes == identity_bytes,
                    "checkpoint predicate/projection/target configuration changed"
                ),
                None => {
                    ensure!(
                        recovery.batch_id == 0 && recovery.pending_offset.is_none(),
                        "checkpoint lost query configuration"
                    );
                    checkpoint.write_immutable("query.json", &identity_bytes).await?;
                }
            }
            metrics.source_version.store(frozen.version, Ordering::Relaxed);
            let cursor = match recovery.previous_offset {
                Some(value) => {
                    let offset: SourceOffset = serde_json::from_value(value)?;
                    ensure!(
                        offset.table_id == frozen.table_id,
                        "offset source table identity changed"
                    );
                    offset.cursor
                }
                None => Cursor::Bootstrap { index: 0 },
            };
            match cursor {
                Cursor::Bootstrap { index } => {
                    ensure!(
                        index <= frozen.files.len(),
                        "bootstrap offset exceeds immutable manifest"
                    );
                    metrics
                        .bootstrap_files_remaining
                        .store((frozen.files.len() - index) as u64, Ordering::Relaxed);
                }
                Cursor::Tail { version, .. } => metrics
                    .processed_version
                    .store(version.saturating_sub(1), Ordering::Relaxed),
            }
            Ok(frozen)
        }
        .await;
        let frozen = match initialized {
            Ok(frozen) => frozen,
            Err(error) => {
                return match checkpoint.close().await {
                    Ok(()) => Err(error),
                    Err(close) => Err(anyhow::anyhow!(
                        "stream startup failed: {error:#}; checkpoint release also failed: {close:#}"
                    )),
                };
            }
        };
        Ok(Self {
            source,
            checkpoint,
            frozen,
            query,
            max_actions,
            metrics,
        })
    }

    #[must_use]
    pub fn manifest(&self) -> &BootstrapManifest {
        &self.frozen
    }

    /// Release checkpoint ownership after all sink work has stopped.
    ///
    /// # Errors
    /// Returns checkpoint ownership/release failures.
    pub async fn close(self) -> Result<()> {
        self.checkpoint.close().await
    }

    /// Execute at most one microbatch. The sink must provide durable txn dedupe.
    ///
    /// # Errors
    /// Returns storage, source, DataFusion, sink, or checkpoint errors. No error
    /// advances the commit marker. Retrying reuses the exact persisted plan.
    pub async fn step(&mut self, sink: &mut dyn TenantSink) -> Result<Step> {
        let result = self.step_inner(sink).await;
        if result.is_err() {
            self.metrics.failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn step_inner(&mut self, sink: &mut dyn TenantSink) -> Result<Step> {
        let recovery = self.checkpoint.recover().await?;
        let start = match &recovery.previous_offset {
            Some(value) => self.decode_offset(value)?.cursor,
            None if self.frozen.files.is_empty() => Cursor::Tail {
                version: self.frozen.version.checked_add(1).context("source version overflow")?,
                index: 0,
            },
            None => Cursor::Bootstrap { index: 0 },
        };
        let name = format!("batches/{}.json", recovery.batch_id);
        let plan = match self.checkpoint.read_immutable(&name).await? {
            Some(bytes) => serde_json::from_slice::<BatchPlan>(&bytes)?,
            None => {
                ensure!(
                    recovery.pending_offset.is_none(),
                    "pending offset lost its immutable batch plan"
                );
                let latest = self.source.latest_version(&self.frozen)?;
                self.metrics.source_version.store(latest, Ordering::Relaxed);
                let work = self.source.plan(&self.frozen, &start, self.max_actions).await?;
                if work.end == start {
                    self.metrics.noops.fetch_add(1, Ordering::Relaxed);
                    return Ok(Step::Idle);
                }
                let plan = BatchPlan {
                    start: start.clone(),
                    end: work.end,
                    files: work.files,
                    actions_examined: work.actions_examined,
                };
                self.checkpoint
                    .write_immutable(&name, &serde_json::to_vec(&plan)?)
                    .await?;
                plan
            }
        };
        ensure!(
            plan.start == start,
            "persisted batch plan has inconsistent start cursor"
        );
        let offset = serde_json::to_value(SourceOffset {
            table_id: self.frozen.table_id.clone(),
            cursor: plan.end.clone(),
        })?;
        if let Some(pending) = &recovery.pending_offset {
            ensure!(*pending == offset, "pending offset differs from frozen batch plan");
        } else {
            let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
            self.checkpoint
                .prepare(recovery.batch_id, &offset, i64::try_from(timestamp.as_millis())?)
                .await?;
            self.metrics.offsets_prepared.fetch_add(1, Ordering::Relaxed);
        }
        let query_id = self.checkpoint.query_id().to_owned();
        let transaction = sink.lookup_transaction(&query_id).await?;
        ensure!(
            transaction.is_none_or(|version| version <= recovery.batch_id),
            "sink transaction is ahead of checkpoint; checkpoint history may have been lost"
        );
        let replay = transaction == Some(recovery.batch_id);
        if replay {
            self.metrics.replays.fetch_add(1, Ordering::Relaxed);
        } else {
            // Validate current identity before touching the captured data paths.
            self.source.latest_version(&self.frozen)?;
            sink.start_batch(&query_id, recovery.batch_id).await?;
            let mut rows = 0;
            for file in &plan.files {
                let mut consumer = SinkConsumer {
                    sink,
                    partitions: &file.partition_values,
                };
                let reads = read_file(&self.source, &self.frozen, file, &self.query, &mut consumer).await?;
                rows += reads.rows_returned;
                self.metrics.record_read(reads)?;
            }
            sink.commit_batch(&query_id, recovery.batch_id).await?;
            self.metrics.sink_commits.fetch_add(1, Ordering::Relaxed);
            if rows == 0 {
                self.metrics.txn_only_batches.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.checkpoint.commit(recovery.batch_id).await?;
        self.metrics.checkpoints.fetch_add(1, Ordering::Relaxed);
        self.metrics.batches.fetch_add(1, Ordering::Relaxed);
        match plan.start {
            Cursor::Bootstrap { .. } => {
                self.metrics.snapshot_batches.fetch_add(1, Ordering::Relaxed);
            }
            Cursor::Tail { .. } => {
                self.metrics.tail_batches.fetch_add(1, Ordering::Relaxed);
            }
        }
        let processed = match plan.end {
            Cursor::Bootstrap { index } => {
                self.metrics
                    .bootstrap_files_remaining
                    .store((self.frozen.files.len() - index) as u64, Ordering::Relaxed);
                0
            }
            Cursor::Tail { version, .. } => {
                self.metrics.bootstrap_files_remaining.store(0, Ordering::Relaxed);
                version.saturating_sub(1)
            }
        };
        self.metrics.processed_version.store(processed, Ordering::Relaxed);
        Ok(Step::Committed {
            batch: recovery.batch_id,
            files: plan.files.len(),
            replay,
        })
    }

    fn decode_offset(&self, value: &serde_json::Value) -> Result<SourceOffset> {
        let offset: SourceOffset = serde_json::from_value(value.clone()).context("invalid tenant source offset")?;
        ensure!(
            offset.table_id == self.frozen.table_id,
            "offset source table identity changed"
        );
        Ok(offset)
    }
}

struct SinkConsumer<'a> {
    sink: &'a mut dyn TenantSink,
    partitions: &'a BTreeMap<String, Option<String>>,
}

#[async_trait::async_trait]
impl BatchConsumer for SinkConsumer<'_> {
    async fn consume(&mut self, batch: RecordBatch) -> Result<()> {
        self.sink.write(batch, self.partitions).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_deltalake::delta::DeltaTable;
    use std::sync::Mutex;

    #[derive(Default)]
    struct SinkState {
        transaction: Option<i64>,
        rows: Vec<i32>,
    }

    #[derive(Default)]
    struct TestSink {
        state: Arc<Mutex<SinkState>>,
        pending: Vec<i32>,
        fail_after_commit: bool,
    }

    #[async_trait::async_trait]
    impl TenantSink for TestSink {
        async fn lookup_transaction(&self, _: &str) -> Result<Option<i64>> {
            Ok(self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("sink poisoned"))?
                .transaction)
        }

        async fn start_batch(&mut self, _: &str, _: i64) -> Result<()> {
            self.pending.clear();
            Ok(())
        }

        async fn write(&mut self, batch: RecordBatch, _: &BTreeMap<String, Option<String>>) -> Result<()> {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("key type")?;
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("value type")?;
            for row in 0..batch.num_rows() {
                ensure!(keys.value(row) == "a", "cross-customer row escaped filter");
                self.pending.push(values.value(row));
            }
            Ok(())
        }

        async fn commit_batch(&mut self, _: &str, batch: i64) -> Result<()> {
            let mut state = self.state.lock().map_err(|_| anyhow::anyhow!("sink poisoned"))?;
            if state.transaction != Some(batch) {
                state.rows.append(&mut self.pending);
                state.transaction = Some(batch);
            }
            if self.fail_after_commit {
                self.fail_after_commit = false;
                anyhow::bail!("injected crash after sink commit before checkpoint commit");
            }
            Ok(())
        }
    }

    async fn append(table: &Arc<DeltaTable>, base: i32) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let data = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
                Arc::new(Int32Array::from(vec![base, base + 1, base + 2, base + 3])),
            ],
        )?;
        let mut writer = table.new_file(data.schema())?;
        writer.write(&data).await?;
        writer.finish().await?;
        Ok(())
    }

    async fn open_stream(source: &std::path::Path, checkpoint: &std::path::Path) -> Result<TenantStream> {
        TenantStream::open(
            DeltaSource::open(url::Url::from_directory_path(source).map_err(|()| anyhow::anyhow!("URL"))?)?,
            Checkpoint::open_local(checkpoint).await?,
            "file:///target/".into(),
            TenantQuery {
                customer_id: "a".into(),
                projection: Vec::new(),
            },
            128,
            Arc::new(StreamMetrics::default()),
        )
        .await
    }

    #[tokio::test]
    async fn initially_empty_snapshot_is_idle_across_restarts_then_tails_without_a_phantom_transaction() -> Result<()> {
        let source_root = tempfile::tempdir()?;
        let checkpoint_root = tempfile::tempdir()?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let source = DeltaTable::open_url(
            url::Url::from_directory_path(source_root.path()).map_err(|()| anyhow::anyhow!("URL"))?,
            schema,
            Vec::new(),
        )?;
        let mut sink = TestSink::default();
        let mut stream = open_stream(source_root.path(), checkpoint_root.path()).await?;
        assert_eq!(stream.step(&mut sink).await?, Step::Idle);
        assert!(!checkpoint_root.path().join("offsets/0").exists());
        assert!(
            sink.state
                .lock()
                .map_err(|_| anyhow::anyhow!("sink poisoned"))?
                .transaction
                .is_none()
        );
        stream.close().await?;
        let mut stream = open_stream(source_root.path(), checkpoint_root.path()).await?;
        assert_eq!(stream.step(&mut sink).await?, Step::Idle);
        append(&source, 0).await?;
        assert_eq!(
            stream.step(&mut sink).await?,
            Step::Committed {
                batch: 0,
                files: 1,
                replay: false
            }
        );
        assert_eq!(
            sink.state.lock().map_err(|_| anyhow::anyhow!("sink poisoned"))?.rows,
            vec![0, 2]
        );
        assert_eq!(stream.step(&mut sink).await?, Step::Idle);
        stream.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_tail_restarts_and_dedupes_sink_commit_before_checkpoint() -> Result<()> {
        let source_root = tempfile::tempdir()?;
        let checkpoint_root = tempfile::tempdir()?;
        let source = DeltaTable::open(source_root.path())?;
        append(&source, 0).await?;
        let mut stream = open_stream(source_root.path(), checkpoint_root.path()).await?;
        append(&source, 10).await?;
        let mut sink = TestSink {
            fail_after_commit: true,
            ..TestSink::default()
        };
        assert!(stream.step(&mut sink).await.is_err());
        assert_eq!(
            sink.state.lock().map_err(|_| anyhow::anyhow!("sink poisoned"))?.rows,
            vec![0, 2]
        );
        let frozen_files = stream.manifest().files.clone();
        drop(stream);
        // A replay of an already committed sink batch must not reread vacuumed
        // bootstrap data. The manifest and transaction are sufficient.
        for file in &frozen_files {
            std::fs::remove_file(source_root.path().join(&file.path))?;
        }
        let mut stream = open_stream(source_root.path(), checkpoint_root.path()).await?;
        assert_eq!(
            stream.step(&mut sink).await?,
            Step::Committed {
                batch: 0,
                files: 1,
                replay: true
            }
        );
        assert_eq!(
            stream.step(&mut sink).await?,
            Step::Committed {
                batch: 1,
                files: 1,
                replay: false
            }
        );
        assert_eq!(stream.step(&mut sink).await?, Step::Idle);
        assert_eq!(
            sink.state.lock().map_err(|_| anyhow::anyhow!("sink poisoned"))?.rows,
            vec![0, 2, 10, 12]
        );
        drop(stream);
        let mut stream = open_stream(source_root.path(), checkpoint_root.path()).await?;
        assert_eq!(stream.step(&mut sink).await?, Step::Idle);
        assert_eq!(
            sink.state
                .lock()
                .map_err(|_| anyhow::anyhow!("sink poisoned"))?
                .rows
                .len(),
            4
        );
        Ok(())
    }

    #[tokio::test]
    async fn missing_source_data_never_advances_commit_and_changed_predicate_is_rejected() -> Result<()> {
        let source_root = tempfile::tempdir()?;
        let checkpoint_root = tempfile::tempdir()?;
        let source = DeltaTable::open(source_root.path())?;
        append(&source, 0).await?;
        let mut stream = open_stream(source_root.path(), checkpoint_root.path()).await?;
        let file = stream
            .manifest()
            .files
            .first()
            .context("missing source file")?
            .path
            .clone();
        std::fs::remove_file(source_root.path().join(file))?;
        assert!(stream.step(&mut TestSink::default()).await.is_err());
        assert!(!checkpoint_root.path().join("commits/0").exists());
        drop(stream);
        let checkpoint = Checkpoint::open_local(checkpoint_root.path()).await?;
        let changed = TenantStream::open(
            DeltaSource::open(url::Url::from_directory_path(source_root.path()).map_err(|()| anyhow::anyhow!("URL"))?)?,
            checkpoint,
            "file:///target/".into(),
            TenantQuery {
                customer_id: "b".into(),
                projection: Vec::new(),
            },
            128,
            Arc::new(StreamMetrics::default()),
        )
        .await;
        assert!(changed.is_err());
        Ok(())
    }

    #[test]
    fn literal_rust_source_offsets_distinguish_bootstrap_and_next_unread_action() -> Result<()> {
        let payload = |cursor| -> Result<String> {
            Ok(serde_json::to_string(&serde_json::to_value(SourceOffset {
                table_id: "table-identity".into(),
                cursor,
            })?)?)
        };
        assert_eq!(
            payload(Cursor::Bootstrap { index: 0 })?,
            r#"{"cursor":{"index":0,"phase":"bootstrap"},"tableId":"table-identity"}"#
        );
        assert_eq!(
            payload(Cursor::Tail { version: 8, index: 0 })?,
            r#"{"cursor":{"index":0,"phase":"tail","version":8},"tableId":"table-identity"}"#
        );
        assert_eq!(
            payload(Cursor::Tail { version: 8, index: 3 })?,
            r#"{"cursor":{"index":3,"phase":"tail","version":8},"tableId":"table-identity"}"#
        );
        Ok(())
    }

    #[tokio::test]
    async fn filtered_out_microbatch_records_only_txn_progress_and_idle_creates_nothing() -> Result<()> {
        let source_root = tempfile::tempdir()?;
        let checkpoint_root = tempfile::tempdir()?;
        let source = DeltaTable::open(source_root.path())?;
        append(&source, 0).await?;
        let mut stream = TenantStream::open(
            DeltaSource::open(url::Url::from_directory_path(source_root.path()).map_err(|()| anyhow::anyhow!("URL"))?)?,
            Checkpoint::open_local(checkpoint_root.path()).await?,
            "file:///target/".into(),
            TenantQuery {
                customer_id: "absent".into(),
                projection: Vec::new(),
            },
            128,
            Arc::new(StreamMetrics::default()),
        )
        .await?;
        let mut sink = TestSink::default();
        assert_eq!(
            stream.step(&mut sink).await?,
            Step::Committed {
                batch: 0,
                files: 1,
                replay: false
            }
        );
        assert!(
            sink.state
                .lock()
                .map_err(|_| anyhow::anyhow!("sink poisoned"))?
                .rows
                .is_empty()
        );
        assert_eq!(
            sink.state
                .lock()
                .map_err(|_| anyhow::anyhow!("sink poisoned"))?
                .transaction,
            Some(0)
        );
        assert_eq!(stream.step(&mut sink).await?, Step::Idle);
        assert!(!checkpoint_root.path().join("offsets/1").exists());
        assert!(!checkpoint_root.path().join("commits/1").exists());
        Ok(())
    }

    #[tokio::test]
    async fn failed_startup_releases_cloud_checkpoint_ownership() -> Result<()> {
        let source_root = tempfile::tempdir()?;
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let prefix = object_store::path::Path::from("failed-startup");
        let checkpoint = Checkpoint::open_store(Arc::clone(&store), prefix.clone()).await?;
        let opened = TenantStream::open(
            DeltaSource::open(url::Url::from_directory_path(source_root.path()).map_err(|()| anyhow::anyhow!("URL"))?)?,
            checkpoint,
            "file:///target/".into(),
            TenantQuery {
                customer_id: "a".into(),
                projection: Vec::new(),
            },
            128,
            Arc::new(StreamMetrics::default()),
        )
        .await;
        assert!(opened.is_err());
        let reopened = Checkpoint::open_store(store, prefix).await?;
        reopened.close().await?;
        Ok(())
    }
}
