use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_deltalake::CompletedFileObservation;
use arrow_deltalake::commit::{BatchedTable, CommitReceiptError};
use arrow_deltalake::delta::{DeltaFileWriter, PublicationFenceErrorKind};
use arrowcol_catalog::SchemaVersion;
use arrowcol_protocol::{
    DeltaPublicationResult, ErrorCode, Measurement, PublicationFence, ReceiptId, SchemaFingerprint, TableIdentity,
    TableLease, UnixMillis,
};
use async_trait::async_trait;
use thiserror::Error;

use crate::manager::FlushReason;

/// Immutable metadata for one receipt's table/schema/fence publication.
#[derive(Clone)]
pub struct PublicationContext {
    /// Stable server-generated identity of this independent publication.
    pub receipt_id: ReceiptId,
    /// UTC admission time, preserved unchanged through durable replay.
    pub ingested_at_ms: UnixMillis,
    /// Portable table identity.
    pub table: TableIdentity,
    /// Exact registered incoming schema version.
    pub schema_version: SchemaVersion,
    /// Exact Arrow schema fingerprint.
    pub schema_fingerprint: SchemaFingerprint,
    /// Shared Arrow schema for every queued batch.
    pub arrow_schema: SchemaRef,
    /// SQL-backed publication fence.
    pub fence: PublicationFence,
    /// Trigger that formed this flush group.
    pub flush_reason: FlushReason,
}

/// Durable sink result used for receipt completion and adaptive sizing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationOutcome {
    /// Shared protocol publication result.
    pub publication: DeltaPublicationResult,
    /// Path-independent Parquet input/output observation.
    pub file_observation: CompletedFileObservation,
}

/// Machine-readable publication sink failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SinkErrorKind {
    /// No durable publication occurred and a new delivery may be useful.
    Retryable,
    /// Ownership was fenced before commit.
    Fenced,
    /// Publication may have committed and must not be retried automatically.
    PublicationUnknown,
    /// The request is permanently invalid.
    Permanent,
    /// Cooperative cancellation interrupted publication before a known commit.
    Cancelled,
}

/// Structured publication failure suitable for receipt completion.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{kind:?}: {message}")]
pub struct SinkError {
    kind: SinkErrorKind,
    code: ErrorCode,
    message: String,
}

impl SinkError {
    /// Creates a retryable known non-publication failure.
    #[must_use]
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            kind: SinkErrorKind::Retryable,
            code: ErrorCode::PublicationFailed,
            message: message.into(),
        }
    }

    /// Creates a stale-lease failure.
    #[must_use]
    pub fn fenced(message: impl Into<String>) -> Self {
        Self {
            kind: SinkErrorKind::Fenced,
            code: ErrorCode::LeaseFenced,
            message: message.into(),
        }
    }

    /// Creates an uncertain commit outcome.
    #[must_use]
    pub fn publication_unknown(message: impl Into<String>) -> Self {
        Self {
            kind: SinkErrorKind::PublicationUnknown,
            code: ErrorCode::PublicationOutcomeUnknown,
            message: message.into(),
        }
    }

    /// Creates a permanent schema/contract failure.
    #[must_use]
    pub fn permanent(code: ErrorCode, message: impl Into<String>) -> Self {
        debug_assert!(!code.is_retryable());
        Self {
            kind: SinkErrorKind::Permanent,
            code,
            message: message.into(),
        }
    }

    /// Creates a cooperative cancellation failure.
    #[must_use]
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self {
            kind: SinkErrorKind::Cancelled,
            code: ErrorCode::PublicationFailed,
            message: message.into(),
        }
    }

    /// Returns the machine-readable sink category.
    #[must_use]
    pub const fn kind(&self) -> SinkErrorKind {
        self.kind
    }

    /// Returns the shared receipt error code.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns safe diagnostic detail.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// One streaming publication attempt.
#[async_trait]
pub trait PublicationSession: Send {
    /// Streams one retained record batch without concatenation.
    async fn write(&mut self, batch: &RecordBatch) -> Result<(), SinkError>;

    /// Finalizes durable storage and returns its commit observation.
    async fn finish(self: Box<Self>) -> Result<PublicationOutcome, SinkError>;
}

/// Storage-neutral factory for one flush session.
#[async_trait]
pub trait PublicationSink: Send + Sync {
    /// Returns the current confirmed lease for a queued publication.
    ///
    /// Sinks without lease renewal retain the admitted lease. Renewing sinks
    /// must preserve its owner, identity, and fencing generation.
    async fn current_lease(&self, _table: &TableIdentity, admitted: &TableLease) -> Result<TableLease, SinkError> {
        Ok(admitted.clone())
    }

    /// Starts a table/schema/fence-specific streaming attempt.
    async fn begin(&self, context: PublicationContext) -> Result<Box<dyn PublicationSession>, SinkError>;
}

/// Arrow Delta Lake publication sink bound to one fenced table owner.
pub struct DeltaPublicationSink {
    table: Arc<BatchedTable>,
    expected_table: TableIdentity,
    expected_fingerprint: SchemaFingerprint,
    expected_fence: PublicationFence,
}

impl DeltaPublicationSink {
    /// Binds a shared Delta commit coordinator to validated protocol identity.
    #[must_use]
    pub fn new(
        table: Arc<BatchedTable>,
        expected_table: TableIdentity,
        expected_fingerprint: SchemaFingerprint,
        expected_fence: PublicationFence,
    ) -> Self {
        Self {
            table,
            expected_table,
            expected_fingerprint,
            expected_fence,
        }
    }
}

#[async_trait]
impl PublicationSink for DeltaPublicationSink {
    async fn begin(&self, context: PublicationContext) -> Result<Box<dyn PublicationSession>, SinkError> {
        if context.table != self.expected_table
            || context.schema_fingerprint != self.expected_fingerprint
            || context.fence != self.expected_fence
        {
            return Err(SinkError::fenced(
                "buffer publication context does not match the configured Delta table fence",
            ));
        }
        let writer = self
            .table
            .new_file_with_publication(
                context.arrow_schema,
                format!("arrowcol.publication.{}", context.receipt_id),
                0,
                i64::try_from(context.ingested_at_ms.get())
                    .map_err(|_| SinkError::retryable("ingestion timestamp exceeds Delta range"))?,
            )
            .await
            .map_err(|cause| SinkError::retryable(format!("create Delta data file: {cause:#}")))?;
        Ok(Box::new(DeltaSession {
            table: Arc::clone(&self.table),
            writer,
            fingerprint: context.schema_fingerprint,
            fence: context.fence,
        }))
    }
}

struct DeltaSession {
    table: Arc<BatchedTable>,
    writer: DeltaFileWriter,
    fingerprint: SchemaFingerprint,
    fence: PublicationFence,
}

#[async_trait]
impl PublicationSession for DeltaSession {
    async fn write(&mut self, batch: &RecordBatch) -> Result<(), SinkError> {
        self.writer
            .write(batch)
            .await
            .map_err(|cause| SinkError::retryable(format!("write Delta data file: {cause:#}")))
    }

    async fn finish(self: Box<Self>) -> Result<PublicationOutcome, SinkError> {
        let staged = self
            .writer
            .stage()
            .await
            .map_err(|cause| SinkError::retryable(format!("stage Delta data file: {cause:#}")))?;
        let observation = staged.observation();
        let receipt = self
            .table
            .submit(staged, true)
            .await
            .map_err(|cause| SinkError::retryable(format!("submit Delta data file: {cause:#}")))?;
        let append = receipt.wait_typed().await.map_err(delta_commit_error)?;
        let version = Measurement::new(append.version)
            .map_err(|cause| SinkError::publication_unknown(format!("Delta version is invalid: {cause}")))?;
        let rows = Measurement::new(
            u64::try_from(observation.rows)
                .map_err(|_| SinkError::publication_unknown("published row count exceeds protocol range"))?,
        )
        .map_err(|cause| SinkError::publication_unknown(format!("published row count is invalid: {cause}")))?;
        let files = Measurement::new(1)
            .map_err(|cause| SinkError::publication_unknown(format!("published file count is invalid: {cause}")))?;
        let bytes = Measurement::new(observation.parquet_bytes)
            .map_err(|cause| SinkError::publication_unknown(format!("published byte count is invalid: {cause}")))?;
        Ok(PublicationOutcome {
            publication: DeltaPublicationResult::new(
                version,
                files,
                rows,
                bytes,
                self.fingerprint,
                unix_millis()?,
                self.fence,
            ),
            file_observation: observation,
        })
    }
}

fn delta_commit_error(error: CommitReceiptError) -> SinkError {
    match error {
        CommitReceiptError::PublicationFenced(error) => match error.kind() {
            PublicationFenceErrorKind::StaleOwnership => SinkError::fenced(error.message()),
            PublicationFenceErrorKind::CheckFailed => SinkError::retryable(error.message()),
            _ => SinkError::retryable(error.message()),
        },
        CommitReceiptError::CommitFailed { message } => SinkError::publication_unknown(message),
        CommitReceiptError::WorkerStopped => {
            SinkError::publication_unknown("Delta commit worker stopped before reporting the commit outcome")
        }
        _ => SinkError::publication_unknown("unknown Delta commit worker failure"),
    }
}

fn unix_millis() -> Result<UnixMillis, SinkError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SinkError::publication_unknown("system clock is before Unix epoch"))?
        .as_millis();
    let millis =
        u64::try_from(millis).map_err(|_| SinkError::publication_unknown("system time exceeds protocol range"))?;
    UnixMillis::new(millis).map_err(|cause| SinkError::publication_unknown(cause.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: A Delta worker disappears before delivering a submitted file's result.
    /// Guarantees: The direct receipt is marked outcome-unknown rather than inviting a blind retry.
    #[test]
    fn stopped_worker_is_an_uncertain_commit() {
        let error = delta_commit_error(CommitReceiptError::WorkerStopped);
        assert_eq!(error.kind(), SinkErrorKind::PublicationUnknown);
        assert_eq!(error.code(), ErrorCode::PublicationOutcomeUnknown);
    }
}
