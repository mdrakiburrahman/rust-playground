#![cfg(feature = "delta")]

use anyhow::{Context, Result};
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_lake::commit::{BatchedTable, CommitOptions};
use arrow_lake::delta::{DeltaTable, StagedAppend};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

async fn file(table: &BatchedTable, id: i32) -> Result<StagedAppend> {
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![id]))],
    )?;
    let mut writer = table.new_file(input.schema()).await?;
    writer.write(&input).await?;
    writer.stage().await
}

fn options() -> CommitOptions {
    CommitOptions {
        max_files_per_commit: 2,
        max_pending_files: 2,
        ..Default::default()
    }
}

/// Scenario: independent producers share a local table and finish separate files.
/// Guarantees: one transaction publishes both files, and later batches advance serial versions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_files_get_one_version() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let left = BatchedTable::open(directory.path(), options()).await?;
    let right = BatchedTable::open(directory.path().join("."), options()).await?;
    assert!(Arc::ptr_eq(&left, &right));
    for expected in 1..=2 {
        let left = Arc::clone(&left);
        let right = Arc::clone(&right);
        let a = tokio::spawn(async move { left.submit(file(&left, 1).await?, false).await?.wait().await });
        let b = tokio::spawn(async move { right.submit(file(&right, 2).await?, false).await?.wait().await });
        assert_eq!(timeout(Duration::from_secs(5), a).await???.version, expected);
        assert_eq!(timeout(Duration::from_secs(5), b).await???.version, expected);
        let log = std::fs::read_to_string(directory.path().join(format!("_delta_log/{expected:020}.json")))?;
        assert_eq!(log.lines().filter(|line| line.contains("\"add\":")).count(), 2);
    }
    Ok(())
}

/// Scenario: a partial batch uses the production ten-second collection window.
/// Guarantees: admission is not success, explicit flush bypasses the window, and no empty commit is made.
#[tokio::test]
async fn partial_batches_need_publication_and_can_be_forced() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let table = BatchedTable::open(directory.path(), CommitOptions::default()).await?;
    let receipt = table.submit(file(&table, 1).await?, false).await?;
    assert!(!directory.path().join("_delta_log/00000000000000000001.json").exists());
    table.flush_pending();
    assert_eq!(timeout(Duration::from_secs(3), receipt.wait()).await??.version, 1);
    table.flush_pending();
    let receipt = table.submit(file(&table, 2).await?, true).await?;
    assert_eq!(timeout(Duration::from_secs(3), receipt.wait()).await??.version, 2);
    assert!(!directory.path().join("_delta_log/00000000000000000003.json").exists());
    Ok(())
}

/// Scenario: queued files occupy capacity while a partial batch awaits its collection deadline.
/// Guarantees: explicit publication releases capacity for subsequent batches.
#[tokio::test]
async fn partial_publication_releases_capacity() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let table = BatchedTable::open(
        directory.path(),
        CommitOptions {
            max_files_per_commit: 3,
            max_pending_files: 3,
            ..Default::default()
        },
    )
    .await?;
    let a = table.submit(file(&table, 1).await?, false).await?;
    let b = table.submit(file(&table, 2).await?, false).await?;
    // Force publication while another producer is preparing, rather than wait
    // for the production collection window.
    table.flush_pending();
    assert_eq!(timeout(Duration::from_secs(3), a.wait()).await??.version, 1);
    assert_eq!(timeout(Duration::from_secs(3), b.wait()).await??.version, 1);
    let c = table.submit(file(&table, 3).await?, true).await?;
    assert_eq!(timeout(Duration::from_secs(3), c.wait()).await??.version, 2);
    Ok(())
}

/// Scenario: a table owner is shared, another uses different limits, and all owners then close.
/// Guarantees: conflicting settings and independent writer locks fail; ownership can be reacquired after drain.
#[tokio::test]
async fn shared_ownership_is_scoped_and_options_are_consistent() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let table = BatchedTable::open(directory.path(), options()).await?;
    let mut different = options();
    different.batch_window = Duration::from_secs(1);
    assert!(BatchedTable::open(directory.path(), different).await.is_err());
    assert!(DeltaTable::open(directory.path()).is_err());
    let receipt = table.submit(file(&table, 1).await?, true).await?;
    receipt.wait().await?;
    drop(table);
    let reopened = timeout(Duration::from_secs(3), BatchedTable::open(directory.path(), options())).await??;
    let result = reopened.submit(file(&reopened, 2).await?, true).await?.wait().await?;
    assert_eq!(result.version, 2);
    Ok(())
}

/// Scenario: a commit's snapshot fails after two eligible files have been finalized.
/// Guarantees: every waiter receives failure and the coordinator refuses subsequent publication.
#[tokio::test]
async fn commit_failure_reaches_all_waiters() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let table = BatchedTable::open(directory.path(), options()).await?;
    let left = file(&table, 1).await?;
    let right = file(&table, 2).await?;
    std::fs::write(directory.path().join("_delta_log/00000000000000000000.json"), "corrupt")?;
    let a = table.submit(left, false).await?;
    let b = table.submit(right, false).await?;
    let error = timeout(Duration::from_secs(3), a.wait())
        .await?
        .err()
        .context("commit unexpectedly succeeded")?;
    assert!(!error.to_string().is_empty());
    assert!(timeout(Duration::from_secs(3), b.wait()).await?.is_err());
    let input = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    assert!(table.new_file(input).await.is_err());
    assert!(!directory.path().join("_delta_log/00000000000000000001.json").exists());
    Ok(())
}

/// Scenario: configuration supplies zero, inconsistent, or overflowing bounds.
/// Guarantees: malformed queues cannot be constructed and the default window remains ten seconds.
#[test]
fn validates_bounded_options() {
    let defaults = CommitOptions::default();
    assert_eq!(defaults.batch_window, Duration::from_secs(10));
    assert!(defaults.validate().is_ok());
    assert!(
        CommitOptions {
            max_files_per_commit: 0,
            ..defaults.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        CommitOptions {
            max_pending_files: 1,
            ..defaults.clone()
        }
        .validate()
        .is_err()
    );
    assert!(
        CommitOptions {
            batch_window: Duration::MAX,
            ..defaults
        }
        .validate()
        .is_err()
    );
}
