#![cfg(feature = "delta")]

use anyhow::Result;
use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::DeltaTable;
use std::collections::BTreeMap;
use std::sync::Arc;
use url::Url;

fn batch() -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("p", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "a"])),
        ],
    )?)
}

/// Scenario: retries reencode the same publication, but independent submissions have different IDs.
/// Guarantees: only retry identity dedupes, and an empty stream batch records durable progress once.
#[tokio::test]
async fn stable_transactions_and_empty_progress() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = batch()?;
    let table = DeltaTable::open_url(
        Url::from_directory_path(root.path()).map_err(|()| anyhow::anyhow!("URL"))?,
        input.schema(),
        vec!["p".into()],
    )?;
    for (app, expected) in [("query-a", 1), ("query-a", 1), ("query-b", 2)] {
        let mut file = table.new_file_partitioned(input.schema(), BTreeMap::from([("p".into(), Some("a".into()))]))?;
        file.write(&input).await?;
        assert_eq!(
            table.commit_files_with_transaction(&[file.stage().await?], app, 0)?,
            expected
        );
    }
    assert_eq!(table.lookup_txn("query-a")?, Some(0));
    assert_eq!(table.commit_files_with_transaction(&[], "query-a", 1)?, 3);
    assert_eq!(table.commit_files_with_transaction(&[], "query-a", 1)?, 3);
    assert!(table.commit_files_with_transaction(&[], "query-a", 0).is_err());
    assert!(
        table
            .commit_files_with_transaction(&[], "arrowcol.purge.before-ms", 1)
            .is_err()
    );
    Ok(())
}

/// Scenario: independent processes open the same table while a collector retains its ownership lock.
/// Guarantees: transaction coordination is independent of the collector's lifetime WAL ownership.
#[tokio::test]
async fn shared_table_can_coexist_with_collector_owner() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![42]))],
    )?;
    let collector = DeltaTable::open(root.path())?;
    collector.new_file(input.schema())?;
    let shared = DeltaTable::open_url(
        Url::from_directory_path(root.path()).map_err(|()| anyhow::anyhow!("URL"))?,
        input.schema(),
        vec![],
    )?;
    let mut left = collector.new_file(input.schema())?;
    let mut right = shared.new_file(input.schema())?;
    left.write(&input).await?;
    right.write(&input).await?;
    assert_eq!(left.finish().await?.version, 1);
    assert_eq!(right.finish().await?.version, 2);
    Ok(())
}

/// Scenario: a caller binds p=a but supplies rows from p=b.
/// Guarantees: physical partition elision never silently changes logical rows.
#[tokio::test]
async fn partition_literal_mismatch_is_rejected() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = batch()?;
    let table = DeltaTable::open_url(
        Url::from_directory_path(root.path()).map_err(|()| anyhow::anyhow!("URL"))?,
        input.schema(),
        vec!["p".into()],
    )?;
    let mut file = table.new_file_partitioned(input.schema(), BTreeMap::from([("p".into(), Some("b".into()))]))?;
    assert!(file.write(&input).await.is_err());
    assert_eq!(table.version()?, 0);
    assert!(file.stage().await.is_err());
    Ok(())
}

/// Scenario: a crash replay resumes after transaction JSON history has been removed.
/// Guarantees: checkpoints retain publication identities and higher-version fencing.
#[tokio::test]
async fn transaction_fences_survive_checkpoint_and_log_cleanup() -> Result<()> {
    let root = tempfile::tempdir()?;
    let input = batch()?;
    let table = DeltaTable::open_url(
        Url::from_directory_path(root.path()).map_err(|()| anyhow::anyhow!("URL"))?,
        input.schema(),
        vec!["p".into()],
    )?;
    let mut file = table.new_file_partitioned(input.schema(), BTreeMap::from([("p".into(), Some("a".into()))]))?;
    file.write(&input).await?;
    let staged = file.stage().await?;
    assert_eq!(table.commit_files_with_transaction(&[staged], "stable", 0)?, 1);
    assert_eq!(table.commit_files_with_transaction(&[], "fenced", 4)?, 2);
    assert_eq!(table.checkpoint()?, 2);
    for version in 0..=2 {
        std::fs::remove_file(root.path().join(format!("_delta_log/{version:020}.json")))?;
    }
    assert_eq!(table.lookup_txn("stable")?, Some(0));
    assert_eq!(table.lookup_txn("fenced")?, Some(4));
    assert_eq!(table.commit_files_with_transaction(&[], "stable", 0)?, 2);
    assert!(table.commit_files_with_transaction(&[], "fenced", 3).is_err());
    Ok(())
}

/// Scenario: a process owns an explicit publisher session, publishes, then shuts down.
/// Guarantees: mutations run through the session's guarded store, close succeeds only
/// after they drain, and a closed session admits no further publication.
#[tokio::test(flavor = "multi_thread")]
async fn explicit_session_publishes_then_closes_after_drain() -> Result<()> {
    use arrow_deltalake::publisher::{Phase, PublisherSession};
    let root = tempfile::tempdir()?;
    let input = batch()?;
    let session = PublisherSession::local();
    let url = Url::from_directory_path(root.path()).map_err(|()| anyhow::anyhow!("URL"))?;
    let table = DeltaTable::open_url_with_session(url.clone(), input.schema(), vec!["p".into()], Arc::clone(&session))?;
    let mut file = table.new_file_partitioned(input.schema(), BTreeMap::from([("p".into(), Some("a".into()))]))?;
    file.write(&input).await?;
    assert_eq!(
        table.commit_files_with_transaction(&[file.stage().await?], "query-s", 0)?,
        1
    );
    assert_eq!(session.status().inflight, 0);
    assert!(session.status().uncertain.is_none());
    session.close_after_drain_async().await?;
    assert_eq!(session.status().phase, Phase::Closed);
    assert!(table.commit_files_with_transaction(&[], "query-s", 1).is_err());
    assert_eq!(table.lookup_txn("query-s")?, Some(0));
    assert!(DeltaTable::open_url_with_session(url, input.schema(), vec!["p".into()], session).is_err());
    Ok(())
}
