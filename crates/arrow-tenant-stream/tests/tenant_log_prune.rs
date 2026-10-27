use anyhow::{Context, Result};
use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::DeltaTable;
use arrow_deltalake::layout::LayoutPolicy;
use arrow_deltalake::maintenance::{
    DISPOSABLE_TEST_MARKER, MaintenanceControl, Outcome, PruneConfig, VacuumConfig, now_ms,
};
use arrow_tenant_stream::checkpoint::Checkpoint;
use arrow_tenant_stream::metrics::StreamMetrics;
use arrow_tenant_stream::reader::{BatchConsumer, TenantQuery, read_file};
use arrow_tenant_stream::sink::DeltaSink;
use arrow_tenant_stream::source::DeltaSource;
use arrow_tenant_stream::stream::{Step, TenantSink, TenantStream};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, atomic::AtomicBool};
use url::Url;

fn url(root: &Path) -> Result<Url> {
    Url::from_directory_path(root).map_err(|()| anyhow::anyhow!("invalid test URL"))
}

fn data(partition: &str, base: i32) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
            Field::new("ingestion_minute", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
            Arc::new(Int32Array::from(vec![base + 2, base + 3, base, base + 1])),
            Arc::new(StringArray::from(vec![partition; 4])),
        ],
    )?)
}

async fn append(table: &Arc<DeltaTable>, partition: &str, base: i32, version: i64) -> Result<()> {
    let batch = data(partition, base)?;
    let mut writer = table.new_file_partitioned(
        batch.schema(),
        BTreeMap::from([("ingestion_minute".into(), Some(partition.into()))]),
    )?;
    writer.write(&batch).await?;
    table.commit_files_with_transaction(&[writer.stage().await?], "prune-source-publication", version)?;
    Ok(())
}

async fn stream(source: &Path, target: &Path, checkpoint: &Path) -> Result<TenantStream> {
    TenantStream::open(
        DeltaSource::open(url(source)?)?,
        Checkpoint::open_local(checkpoint).await?,
        url(target)?.to_string(),
        TenantQuery {
            customer_id: "a".into(),
            projection: Vec::new(),
        },
        128,
        Arc::new(StreamMetrics::default()),
    )
    .await
}

fn sink(stream: &TenantStream, root: &Path) -> Result<(Arc<DeltaTable>, DeltaSink)> {
    let schema = stream.manifest().schema()?;
    let table = DeltaTable::open_url(url(root)?, Arc::clone(&schema), stream.manifest().partition_columns()?)?;
    let sink = DeltaSink::new(Arc::clone(&table), schema, LayoutPolicy::default())?;
    Ok((table, sink))
}

struct FailAfterCommit(DeltaSink);

#[async_trait::async_trait]
impl TenantSink for FailAfterCommit {
    async fn lookup_transaction(&self, query: &str) -> Result<Option<i64>> {
        self.0.lookup_transaction(query).await
    }

    async fn start_batch(&mut self, query: &str, batch: i64) -> Result<()> {
        self.0.start_batch(query, batch).await
    }

    async fn write(&mut self, batch: RecordBatch, partitions: &BTreeMap<String, Option<String>>) -> Result<()> {
        self.0.write(batch, partitions).await
    }

    async fn commit_batch(&mut self, query: &str, batch: i64) -> Result<()> {
        self.0.commit_batch(query, batch).await?;
        anyhow::bail!("injected crash after sink commit before Spark commit log")
    }
}

fn prune(root: &Path) -> Result<u64> {
    std::fs::write(root.join(DISPOSABLE_TEST_MARKER), b"")?;
    let maintenance = DeltaTable::open_for_maintenance(&url(root)?)?;
    let control = MaintenanceControl::new(Arc::new(AtomicBool::new(false)), None);
    let mut vacuum = VacuumConfig::new(0, now_ms()? + 1_000);
    vacuum.unsafe_allow_short_retention_for_tests = true;
    let vacuum_plan = maintenance.plan_vacuum(&vacuum)?;
    maintenance.apply_vacuum(&vacuum_plan, &vacuum, &control)?;
    assert_eq!(
        maintenance
            .vacuum_lite_progress()?
            .context("durable VACUUM progress")?
            .version,
        vacuum_plan.header.read_version,
    );
    let checkpoint = maintenance.checkpoint_if_due(1, true)?;
    assert_eq!(checkpoint.outcome, Outcome::Checkpointed);
    let version = checkpoint.version;
    let config = PruneConfig {
        log_retention_ms: Some(0),
        unsafe_allow_short_retention_for_tests: true,
        ..PruneConfig::new(now_ms()? + 1_000)
    };
    let plan = maintenance.plan_prune_log(&config)?;
    assert_eq!(plan.checkpoint_version, Some(version));
    assert!(!plan.deletions.is_empty(), "fixture must exercise actual log deletion");
    let report = maintenance.apply_prune_log(&plan, &config, &control)?;
    assert_eq!(report.outcome, Outcome::Pruned);
    assert!(report.files_deleted > 0);
    assert_eq!(
        maintenance.log_floor()?.context("durable floor")?.checkpoint_version,
        version
    );
    for old in 0..=version {
        assert!(!root.join(format!("_delta_log/{old:020}.json")).exists());
    }
    assert!(
        root.join(format!("_delta_log/{version:020}.checkpoint.parquet"))
            .is_file()
    );
    assert_eq!(maintenance.plan_prune_log(&config)?.report().outcome, Outcome::Noop);
    Ok(version)
}

#[derive(Default)]
struct Rows(Vec<(String, i32, String)>);

#[async_trait::async_trait]
impl BatchConsumer for Rows {
    async fn consume(&mut self, batch: RecordBatch) -> Result<()> {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("customer type")?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("value type")?;
        let partitions = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("partition type")?;
        for row in 0..batch.num_rows() {
            self.0
                .push((keys.value(row).into(), values.value(row), partitions.value(row).into()));
        }
        Ok(())
    }
}

async fn rows(root: &Path) -> Result<Vec<(String, i32, String)>> {
    let source = DeltaSource::open(url(root)?)?;
    let manifest = source.capture()?;
    assert_eq!(manifest.partition_columns()?, vec!["ingestion_minute"]);
    let query = TenantQuery {
        customer_id: "a".into(),
        projection: Vec::new(),
    };
    let mut rows = Rows::default();
    for file in &manifest.files {
        read_file(&source, &manifest, file, &query, &mut rows).await?;
    }
    rows.0.sort();
    Ok(rows.0)
}

#[tokio::test]
async fn tenant_snapshot_tail_and_sink_replay_survive_proven_kernel_log_pruning() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    let checkpoint_root = tempfile::tempdir()?;
    let stale_target = tempfile::tempdir()?;
    let stale_checkpoint = tempfile::tempdir()?;
    let source = DeltaTable::open_url(
        url(source_root.path())?,
        data("minute-0", 0)?.schema(),
        vec!["ingestion_minute".into()],
    )?;
    append(&source, "minute-0", 0, 0).await?;
    let mut current = stream(source_root.path(), target_root.path(), checkpoint_root.path()).await?;
    let (target, mut writer) = sink(&current, target_root.path())?;
    assert_eq!(
        current.step(&mut writer).await?,
        Step::Committed {
            batch: 0,
            files: 1,
            replay: false
        }
    );
    let mut stale = stream(source_root.path(), stale_target.path(), stale_checkpoint.path()).await?;
    let (stale_table, mut stale_writer) = sink(&stale, stale_target.path())?;
    assert_eq!(
        stale.step(&mut stale_writer).await?,
        Step::Committed {
            batch: 0,
            files: 1,
            replay: false
        }
    );
    stale.close().await?;
    let metadata = std::fs::read(checkpoint_root.path().join("metadata"))?;
    let query_id = serde_json::from_slice::<serde_json::Value>(&metadata)?["id"]
        .as_str()
        .context("Spark query id")?
        .to_owned();

    append(&source, "minute-1", 10, 1).await?;
    let mut crashing = FailAfterCommit(writer);
    let error = current.step(&mut crashing).await.err().context("injected crash")?;
    assert!(format!("{error:#}").contains("injected crash after sink commit"));
    current.close().await?;
    assert!(!checkpoint_root.path().join("commits/1").exists());
    let pending = std::fs::read(checkpoint_root.path().join("offsets/1"))?;
    assert!(pending.starts_with(b"v1\n"));
    assert_eq!(target.lookup_txn(&query_id)?, Some(1));
    let target_version = target.version()?;

    let floor = prune(source_root.path())?;
    assert_eq!(floor, source.version()?);
    let pending_text = std::str::from_utf8(&pending)?;
    let pending_cursor: serde_json::Value =
        serde_json::from_str(pending_text.lines().nth(2).context("Spark single-source offset line")?)?;
    assert_eq!(
        pending_cursor["cursor"],
        serde_json::json!({
            "phase": "tail", "version": floor + 1, "index": 0,
        })
    );
    assert_eq!(source.lookup_txn("prune-source-publication")?, Some(1));
    assert_eq!(prune(target_root.path())?, target_version);
    assert_eq!(target.lookup_txn(&query_id)?, Some(1));
    let mut current = stream(source_root.path(), target_root.path(), checkpoint_root.path()).await?;
    let (_, mut writer) = sink(&current, target_root.path())?;
    assert_eq!(
        current.step(&mut writer).await?,
        Step::Committed {
            batch: 1,
            files: 1,
            replay: true
        }
    );
    assert_eq!(target.version()?, target_version);
    assert_eq!(std::fs::read(checkpoint_root.path().join("offsets/1"))?, pending);
    assert_eq!(
        std::fs::read(checkpoint_root.path().join("commits/1"))?,
        b"v1\n{\"nextBatchWatermarkMs\":0}"
    );
    current.close().await?;

    let mut stale = stream(source_root.path(), stale_target.path(), stale_checkpoint.path()).await?;
    let before = stale_table.version()?;
    let failure = stale
        .step(&mut stale_writer)
        .await
        .err()
        .context("pruned unread history must fail")?;
    let message = format!("{failure:#}");
    assert!(
        message.contains("required Delta history 2 unavailable; cannot advance checkpoint"),
        "{message}"
    );
    assert!(!stale_checkpoint.path().join("offsets/1").exists());
    assert!(!stale_checkpoint.path().join("commits/1").exists());
    assert_eq!(stale_table.version()?, before);
    stale.close().await?;

    let mut current = stream(source_root.path(), target_root.path(), checkpoint_root.path()).await?;
    assert_eq!(current.step(&mut writer).await?, Step::Idle);
    append(&source, "minute-2", 20, 2).await?;
    assert_eq!(
        current.step(&mut writer).await?,
        Step::Committed {
            batch: 2,
            files: 1,
            replay: false
        }
    );
    assert_eq!(current.step(&mut writer).await?, Step::Idle);
    current.close().await?;
    let mut restarted = stream(source_root.path(), target_root.path(), checkpoint_root.path()).await?;
    assert_eq!(restarted.step(&mut writer).await?, Step::Idle);
    restarted.close().await?;
    assert_eq!(std::fs::read(checkpoint_root.path().join("metadata"))?, metadata);
    assert_eq!(target.lookup_txn(&query_id)?, Some(2));
    assert_eq!(target.version()?, target_version + 1);
    assert!(!checkpoint_root.path().join("offsets/3").exists());
    let expected = vec![
        ("a".into(), 0, "minute-0".into()),
        ("a".into(), 2, "minute-0".into()),
        ("a".into(), 10, "minute-1".into()),
        ("a".into(), 12, "minute-1".into()),
        ("a".into(), 20, "minute-2".into()),
        ("a".into(), 22, "minute-2".into()),
    ];
    assert_eq!(rows(target_root.path()).await?, expected);
    Ok(())
}
