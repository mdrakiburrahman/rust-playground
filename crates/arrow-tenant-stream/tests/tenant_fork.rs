use anyhow::{Context, Result, ensure};
use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::DeltaTable;
use arrow_deltalake::layout::LayoutPolicy;
use arrow_tenant_stream::checkpoint::Checkpoint;
use arrow_tenant_stream::metrics::StreamMetrics;
use arrow_tenant_stream::reader::{BatchConsumer, TenantQuery, read_file};
use arrow_tenant_stream::sink::DeltaSink;
use arrow_tenant_stream::source::DeltaSource;
use arrow_tenant_stream::stream::{Step, TenantSink, TenantStream};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use url::Url;

fn url(path: &Path) -> Result<Url> {
    Url::from_directory_path(path).map_err(|()| anyhow::anyhow!("invalid test URL"))
}

fn data_path(root: &Path, relative: &str) -> Result<std::path::PathBuf> {
    url(root)?
        .join(relative)?
        .to_file_path()
        .map_err(|()| anyhow::anyhow!("invalid data URL"))
}

fn batch(partition: &str, base: i32) -> Result<RecordBatch> {
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

async fn append(table: &Arc<DeltaTable>, partition: &str, base: i32, publication: &str) -> Result<()> {
    let data = batch(partition, base)?;
    let mut file = table.new_file_partitioned(
        data.schema(),
        BTreeMap::from([("ingestion_minute".into(), Some(partition.into()))]),
    )?;
    file.write(&data).await?;
    table.commit_files_with_transaction(&[file.stage().await?], publication, 0)?;
    Ok(())
}

struct CrashAfterSinkCommit {
    sink: DeltaSink,
    fail: bool,
}

#[async_trait::async_trait]
impl TenantSink for CrashAfterSinkCommit {
    async fn lookup_transaction(&self, query: &str) -> Result<Option<i64>> {
        self.sink.lookup_transaction(query).await
    }

    async fn start_batch(&mut self, query: &str, batch: i64) -> Result<()> {
        self.sink.start_batch(query, batch).await
    }

    async fn write(&mut self, batch: RecordBatch, partitions: &BTreeMap<String, Option<String>>) -> Result<()> {
        self.sink.write(batch, partitions).await
    }

    async fn commit_batch(&mut self, query: &str, batch: i64) -> Result<()> {
        self.sink.commit_batch(query, batch).await?;
        if self.fail {
            self.fail = false;
            anyhow::bail!("injected process death after durable sink transaction");
        }
        Ok(())
    }
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
            .context("key type")?;
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

async fn open_stream(source: &Path, target: &Path, checkpoint: &Path, customer: &str) -> Result<TenantStream> {
    TenantStream::open(
        DeltaSource::open(url(source)?)?,
        Checkpoint::open_local(checkpoint).await?,
        url(target)?.to_string(),
        TenantQuery {
            customer_id: customer.into(),
            projection: Vec::new(),
        },
        128,
        Arc::new(StreamMetrics::default()),
    )
    .await
}

fn add_action(root: &Path, version: u64) -> Result<Value> {
    let text = std::fs::read_to_string(root.join("_delta_log").join(format!("{version:020}.json")))?;
    for line in text.lines() {
        let action: Value = serde_json::from_str(line)?;
        if let Some(add) = action.get("add") {
            return Ok(add.clone());
        }
    }
    anyhow::bail!("test commit has no Add")
}

fn external_test_commit(root: &Path, version: u64, actions: &[Value]) -> Result<()> {
    use std::io::Write;
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("_delta_log").join(format!("{version:020}.json")))?;
    for action in actions {
        writeln!(output, "{}", serde_json::to_string(action)?)?;
    }
    output.sync_all()?;
    Ok(())
}

/// Real Kernel target transactions, independently encoded partitioned files,
/// crash replay, historical PURGE behavior, and ignored OPTIMIZE rewrites.
#[tokio::test]
async fn real_partitioned_fork_survives_source_vacuum_and_sink_commit_crash() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    let checkpoint = tempfile::tempdir()?;
    let input = batch("2026-10-27T02:25Z", 0)?;
    let source = DeltaTable::open_url(
        url(source_root.path())?,
        input.schema(),
        vec!["ingestion_minute".into()],
    )?;
    append(&source, "2026-10-27T02:25Z", 0, "source-first").await?;
    let first = add_action(source_root.path(), 1)?;
    let mut stream = open_stream(source_root.path(), target_root.path(), checkpoint.path(), "a").await?;
    let schema = stream.manifest().schema()?;
    let target = DeltaTable::open_url(
        url(target_root.path())?,
        Arc::clone(&schema),
        stream.manifest().partition_columns()?,
    )?;
    let policy = LayoutPolicy {
        row_group_rows: 2,
        page_rows: 1,
        ..LayoutPolicy::default()
    };
    let mut sink = CrashAfterSinkCommit {
        sink: DeltaSink::new(Arc::clone(&target), schema, policy)?,
        fail: true,
    };
    append(&source, "2026-10-27T02:26Z", 10, "source-second").await?;
    let second = add_action(source_root.path(), 2)?;
    let crash = stream
        .step(&mut sink)
        .await
        .err()
        .context("expected injected sink crash")?;
    assert!(format!("{crash:#}").contains("injected process death"), "{crash:#}");
    assert_eq!(target.version()?, 1);
    assert!(!checkpoint.path().join("commits/0").exists());
    stream.close().await?;
    let first_path = first["path"].as_str().context("first Add path")?;
    std::fs::remove_file(data_path(source_root.path(), first_path)?)?;
    let mut stream = open_stream(source_root.path(), target_root.path(), checkpoint.path(), "a").await?;
    assert_eq!(
        stream.step(&mut sink).await?,
        Step::Committed {
            batch: 0,
            files: 1,
            replay: true
        }
    );
    assert_eq!(target.version()?, 1);
    assert_eq!(
        stream.step(&mut sink).await?,
        Step::Committed {
            batch: 1,
            files: 1,
            replay: false
        }
    );
    assert_eq!(target.version()?, 2);

    external_test_commit(
        source_root.path(),
        3,
        &[json!({"remove":{
            "path":first_path,"dataChange":true,"deletionTimestamp":1793067960000_i64
        }})],
    )?;
    assert_eq!(
        stream.step(&mut sink).await?,
        Step::Committed {
            batch: 2,
            files: 0,
            replay: false
        }
    );
    let second_path = second["path"].as_str().context("second Add path")?;
    let optimized_path = "optimized-independent.parquet";
    std::fs::copy(
        data_path(source_root.path(), second_path)?,
        source_root.path().join(optimized_path),
    )?;
    let mut replacement = second.clone();
    replacement["path"] = json!(optimized_path);
    replacement["dataChange"] = json!(false);
    external_test_commit(
        source_root.path(),
        4,
        &[
            json!({"remove":{"path":second_path,"dataChange":false,"deletionTimestamp":1793067960000_i64}}),
            json!({"add":replacement}),
        ],
    )?;
    assert_eq!(
        stream.step(&mut sink).await?,
        Step::Committed {
            batch: 3,
            files: 0,
            replay: false
        }
    );
    assert_eq!(stream.step(&mut sink).await?, Step::Idle);
    std::fs::remove_file(data_path(source_root.path(), second_path)?)?;
    let version = target.version()?;
    assert_eq!(stream.step(&mut sink).await?, Step::Idle);
    assert_eq!(target.version()?, version);
    let mut mutation = second.clone();
    mutation["path"] = json!(optimized_path);
    mutation["dataChange"] = json!(true);
    external_test_commit(
        source_root.path(),
        5,
        &[
            json!({"add":mutation}),
            json!({"remove":{"path":optimized_path,"dataChange":true,"deletionTimestamp":1793067960000_i64}}),
        ],
    )?;
    let error = stream
        .step(&mut sink)
        .await
        .err()
        .context("mixed mutation must fail before emission")?;
    assert!(format!("{error:#}").contains("mixed dataChange"), "{error:#}");
    assert!(!checkpoint.path().join("offsets/4").exists());
    assert_eq!(target.version()?, version);
    stream.close().await?;

    let target_source = DeltaSource::open(url(target_root.path())?)?;
    let manifest = target_source.capture()?;
    assert_eq!(manifest.partition_columns()?, vec!["ingestion_minute"]);
    assert_eq!(manifest.files.len(), 2);
    let query = TenantQuery {
        customer_id: "a".into(),
        projection: Vec::new(),
    };
    let mut rows = Rows::default();
    for file in &manifest.files {
        assert!(data_path(target_root.path(), &file.path)?.exists());
        read_file(&target_source, &manifest, file, &query, &mut rows).await?;
    }
    rows.0.sort();
    assert_eq!(
        rows.0,
        vec![
            ("a".into(), 0, "2026-10-27T02:25Z".into()),
            ("a".into(), 2, "2026-10-27T02:25Z".into()),
            ("a".into(), 10, "2026-10-27T02:26Z".into()),
            ("a".into(), 12, "2026-10-27T02:26Z".into()),
        ]
    );
    let physical = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(
        data_path(target_root.path(), &manifest.files[0].path)?,
    )?)?;
    ensure!(
        physical.schema().field_with_name("ingestion_minute").is_err(),
        "partition column must be reconstructed"
    );
    Ok(())
}

#[tokio::test]
async fn real_filtered_batch_commits_transaction_without_data_files_and_idle_is_noop() -> Result<()> {
    let source_root = tempfile::tempdir()?;
    let target_root = tempfile::tempdir()?;
    let checkpoint = tempfile::tempdir()?;
    let input = batch("minute", 0)?;
    let source = DeltaTable::open_url(
        url(source_root.path())?,
        input.schema(),
        vec!["ingestion_minute".into()],
    )?;
    append(&source, "minute", 0, "source").await?;
    let mut stream = open_stream(source_root.path(), target_root.path(), checkpoint.path(), "absent").await?;
    let schema = stream.manifest().schema()?;
    let target = DeltaTable::open_url(
        url(target_root.path())?,
        Arc::clone(&schema),
        stream.manifest().partition_columns()?,
    )?;
    let mut sink = DeltaSink::new(Arc::clone(&target), schema, LayoutPolicy::default())?;
    assert_eq!(
        stream.step(&mut sink).await?,
        Step::Committed {
            batch: 0,
            files: 1,
            replay: false
        }
    );
    assert_eq!(target.version()?, 1);
    assert!(DeltaSource::open(url(target_root.path())?)?.capture()?.files.is_empty());
    assert_eq!(stream.step(&mut sink).await?, Step::Idle);
    assert_eq!(target.version()?, 1);
    stream.close().await?;
    Ok(())
}
