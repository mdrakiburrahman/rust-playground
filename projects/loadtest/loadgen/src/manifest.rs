use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use arrowcol_protocol::ReceiptId;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File},
    io::{AsyncWriteExt, BufWriter},
    sync::Mutex,
};

use crate::cli::{RunArgs, Scenario};

pub const SEND_MANIFEST_FILE: &str = "sent-batches.jsonl";
pub const RECEIPT_MANIFEST_FILE: &str = "receipts.jsonl";
pub const RUN_METADATA_FILE: &str = "run-metadata.json";
pub const SCHEMA_CONTRACT_FILE: &str = "schema-contracts.json";
pub const ROUTING_EVIDENCE_FILE: &str = "routing.json";
pub const CAPABILITY_EVIDENCE_FILE: &str = "capabilities.json";
pub const VALIDATION_FILE: &str = "validation.json";

/// Pool-specific acknowledgement outcome.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AckOutcome {
    DurableReceipt,
    BestEffortAdmission,
    /// Dataflow persisted the batch to its Quiver durable buffer before completing.
    DurableAdmission,
    Error,
}

/// One physical delivery attempt.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SendManifestEntry {
    pub run_id: String,
    pub scenario: Scenario,
    pub phase: String,
    pub logical_batch_id: String,
    pub ordinal: u64,
    pub table_batch_ordinal: u64,
    pub table: String,
    pub schema_version: u64,
    pub schema_fingerprint: String,
    pub rows: usize,
    pub arrow_bytes: u64,
    pub payload_sha256: String,
    pub row_sha256: String,
    pub lane: usize,
    pub pool: String,
    pub attempt: u32,
    pub sent_at_unix_ms: u64,
    pub acknowledged_at_unix_ms: u64,
    pub outcome: AckOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt_id: Option<ReceiptId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Terminal receipt evidence emitted by the background direct poller.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReceiptManifestEntry {
    pub run_id: String,
    pub logical_batch_id: String,
    pub table: String,
    pub receipt_id: ReceiptId,
    pub state: String,
    pub purge_allowed: bool,
    pub terminal_at_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_published: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_published: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// Ordered generated-schema contract retained with the run.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SchemaContract {
    pub table: String,
    pub schema_version: u64,
    pub fingerprint: String,
    pub descriptor_path: Vec<String>,
    pub fields: Vec<SchemaFieldContract>,
}

/// One ordered generated schema field.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SchemaFieldContract {
    pub name: String,
    pub arrow_type: String,
    pub nullable: bool,
}

/// Concurrent append-only result manifests.
#[derive(Clone)]
pub struct ManifestWriter {
    root: Arc<PathBuf>,
    sends: Arc<Mutex<BufWriter<File>>>,
    receipts: Arc<Mutex<BufWriter<File>>>,
}

impl ManifestWriter {
    /// Creates/truncates run manifests below a retained result root.
    ///
    /// # Errors
    ///
    /// Returns filesystem errors.
    pub async fn create(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).await?;
        let sends = BufWriter::new(File::create(root.join(SEND_MANIFEST_FILE)).await?);
        let receipts = BufWriter::new(File::create(root.join(RECEIPT_MANIFEST_FILE)).await?);
        Ok(Self {
            root: Arc::new(root),
            sends: Arc::new(Mutex::new(sends)),
            receipts: Arc::new(Mutex::new(receipts)),
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    /// Appends one delivery attempt.
    ///
    /// # Errors
    ///
    /// Returns JSON serialization or file-write failures.
    pub async fn write_send(&self, entry: &SendManifestEntry) -> Result<()> {
        write_json_line(&self.sends, entry).await
    }

    /// Appends one terminal receipt observation.
    ///
    /// # Errors
    ///
    /// Returns JSON serialization or file-write failures.
    pub async fn write_receipt(&self, entry: &ReceiptManifestEntry) -> Result<()> {
        write_json_line(&self.receipts, entry).await
    }

    /// Atomically replaces one small JSON result artifact.
    ///
    /// # Errors
    ///
    /// Returns JSON serialization or file-write failures.
    pub async fn write_json<T: Serialize>(&self, name: &str, value: &T) -> Result<()> {
        let mut bytes = serde_json::to_vec_pretty(value)?;
        bytes.push(b'\n');
        let next = self.root.join(format!("{name}.next"));
        fs::write(&next, bytes)
            .await
            .with_context(|| format!("write result artifact {name}"))?;
        fs::rename(next, self.root.join(name))
            .await
            .with_context(|| format!("publish result artifact {name}"))
    }

    /// Flushes append-only manifest buffers.
    ///
    /// # Errors
    ///
    /// Returns file flush failures.
    pub async fn flush(&self) -> Result<()> {
        self.sends.lock().await.flush().await?;
        self.receipts.lock().await.flush().await?;
        Ok(())
    }
}

async fn write_json_line<T: Serialize>(writer: &Mutex<BufWriter<File>>, value: &T) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    let mut writer = writer.lock().await;
    writer.write_all(&bytes).await?;
    Ok(())
}

/// Builds stable, non-secret run metadata.
#[must_use]
pub fn run_metadata(run_id: &str, arguments: &RunArgs, schema_contract_sha256: &str) -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "run_id": run_id,
        "started_at_unix_ms": crate::transport::now_unix_millis(),
        "schema_contract_sha256": schema_contract_sha256,
        "settings": {
            "warmup_seconds": arguments.warmup.get().as_secs(),
            "measure_seconds": arguments.measure.get().as_secs(),
            "capture_settle_seconds": arguments.capture_settle.get().as_secs(),
            "drain_timeout_seconds": arguments.drain_timeout.get().as_secs(),
            "request_timeout_seconds": arguments.request_timeout.get().as_secs(),
            "continuous": arguments.continuous,
            "keep_stack": arguments.keep_stack,
            "scenarios": arguments.effective_scenarios().iter().map(|value| value.as_str()).collect::<Vec<_>>(),
            "tables": &arguments.tables,
            "lanes": arguments.lanes,
            "batch_rows": arguments.batch_rows,
            "seed": arguments.seed,
            "max_attempts": arguments.max_attempts,
            "retry_window_seconds": arguments.retry_window.map(|window| window.get().as_secs()),
            "quiver": arguments.quiver,
            "scheduler": "single global atomic ordinal; seed-rotated equal round-robin across all 13 tables",
            "paired_semantics": "one encoded logical RecordBatch is reused concurrently for direct and dataflow",
            "direct_delivery": if arguments.quiver {
                "server-generated receipt; batch persisted to Quiver before acknowledgement; receipt polled to a terminal state"
            } else {
                "server-generated receipt polled to a terminal state"
            },
            "dataflow_delivery": if arguments.quiver {
                "durable admission: Quiver WAL fsync before completion; replayed after a crash (at least once); no receipt"
            } else {
                "best-effort admission; no durable receipt claim"
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;
    use crate::cli::Scenario;

    #[tokio::test]
    async fn progress_replacement_never_exposes_partial_json() -> Result<()> {
        let root = std::env::temp_dir().join(format!("loadgen-atomic-progress-{}", uuid::Uuid::new_v4()));
        let writer = ManifestWriter::create(&root).await?;
        writer
            .write_json(
                "driver-progress.json",
                &serde_json::json!({"sequence":0,"body":"x".repeat(8192)}),
            )
            .await?;
        let updating = writer.clone();
        let task = tokio::spawn(async move {
            for sequence in 1..=100 {
                updating
                    .write_json(
                        "driver-progress.json",
                        &serde_json::json!({"sequence":sequence,"body":"x".repeat(8192)}),
                    )
                    .await?;
            }
            Ok::<(), anyhow::Error>(())
        });
        let mut observations = 0;
        while !task.is_finished() {
            let value: serde_json::Value = serde_json::from_slice(&fs::read(root.join("driver-progress.json")).await?)?;
            assert!(value["sequence"].as_u64().is_some_and(|sequence| sequence <= 100));
            assert_eq!(value["body"].as_str().map(str::len), Some(8192));
            observations += 1;
        }
        task.await??;
        assert!(observations > 0);
        drop(writer);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test]
    async fn append_only_manifests_round_trip_without_partial_lines() -> Result<()> {
        let root = PathBuf::from(format!("target/loadgen-manifest-test-{}", std::process::id()));
        if root.exists() {
            std::fs::remove_dir_all(&root)?;
        }
        let writer = ManifestWriter::create(&root).await?;
        writer
            .write_send(&SendManifestEntry {
                run_id: "run".to_owned(),
                scenario: Scenario::Paired,
                phase: "measure".to_owned(),
                logical_batch_id: "run:paired:measure:0".to_owned(),
                ordinal: 0,
                table_batch_ordinal: 0,
                table: "telemetry.sqlserver.sqlserver_wait_stats".to_owned(),
                schema_version: 1,
                schema_fingerprint: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                rows: 8,
                arrow_bytes: 128,
                payload_sha256: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                row_sha256: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
                lane: 0,
                pool: "dataflow".to_owned(),
                attempt: 1,
                sent_at_unix_ms: 1,
                acknowledged_at_unix_ms: 2,
                outcome: AckOutcome::BestEffortAdmission,
                receipt_id: None,
                error_code: None,
                error: None,
            })
            .await?;
        writer.flush().await?;
        let text = fs::read_to_string(root.join(SEND_MANIFEST_FILE)).await?;
        let decoded: SendManifestEntry = serde_json::from_str(text.trim_end())?;
        assert_eq!(decoded.logical_batch_id, "run:paired:measure:0");
        assert_eq!(decoded.outcome, AckOutcome::BestEffortAdmission);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
