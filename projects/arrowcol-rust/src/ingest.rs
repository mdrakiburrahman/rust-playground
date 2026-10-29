use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow_deltalake::record_batch_logical_bytes;
use arrowcol_buffer::{Admission, BufferError, BufferErrorKind, BufferManager, PersistRequest, QuiverStage};
use arrowcol_catalog::{CatalogStore, SchemaValidator, ValidatedIncomingSchema};
use arrowcol_flight::{
    BatchAdmission, PutResultEmitter, PutResultMetadata, RuntimeError, RuntimeResult, StreamAbortReason,
    StreamCompletion, StreamContext, StreamLifecycle, StreamSummary,
};
use arrowcol_protocol::{CollectorInstanceId, ErrorCode, ErrorStatus, ReceiptId};
use arrowcol_receipts::{
    FailureDisposition, ReceiptError, ReceiptErrorKind, ReceiptFailure, ReceiptStore, ReceiptVersion, ValidatedDelivery,
};
use async_trait::async_trait;
use tonic::Code;
use tracing::warn;

use crate::control::{ActiveTableStream, TableActivity};
use crate::lease::{TableLeaseManager, now_unix_millis, receipt_deadline};

/// Direct per-batch durable receipt admission.
pub struct DirectAdmission {
    catalog: Arc<dyn CatalogStore>,
    receipts: Arc<dyn ReceiptStore>,
    buffer: Arc<BufferManager>,
    quiver: Option<Arc<QuiverStage>>,
    leases: Arc<TableLeaseManager>,
    activity: Arc<TableActivity>,
    owner: CollectorInstanceId,
    receipt_lease_duration: Duration,
    admission_timeout: Duration,
}

impl DirectAdmission {
    /// Creates the direct Flight admission adapter.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog: Arc<dyn CatalogStore>,
        receipts: Arc<dyn ReceiptStore>,
        buffer: Arc<BufferManager>,
        leases: Arc<TableLeaseManager>,
        activity: Arc<TableActivity>,
        owner: CollectorInstanceId,
        receipt_lease_duration: Duration,
        admission_timeout: Duration,
    ) -> Self {
        Self {
            catalog,
            receipts,
            buffer,
            quiver: None,
            leases,
            activity,
            owner,
            receipt_lease_duration,
            admission_timeout,
        }
    }
}

impl DirectAdmission {
    /// Persists every accepted batch to the Quiver durable stage before its
    /// receipt is acknowledged; the stage publishes it from disk.
    #[must_use]
    pub fn with_quiver(mut self, quiver: Arc<QuiverStage>) -> Self {
        self.quiver = Some(quiver);
        self
    }
}

#[async_trait]
impl BatchAdmission for DirectAdmission {
    async fn begin(
        &self,
        context: StreamContext,
        emitter: Arc<dyn PutResultEmitter>,
    ) -> RuntimeResult<Box<dyn StreamLifecycle>> {
        let validation = SchemaValidator::new(Arc::clone(&self.catalog))
            .validate(
                context.metadata().table(),
                context.metadata().claimed_schema_fingerprint(),
                context.received_schema().as_ref(),
            )
            .await
            .map_err(|error| RuntimeError::from_catalog(&error))?;
        if validation.registered() != context.registered_schema()
            || validation.current() != context.current_schema()
            || validation.alignment() != context.alignment()
        {
            return Err(RuntimeError::internal());
        }
        let activity = self
            .activity
            .enter(context.metadata().table().clone())
            .map_err(|_| RuntimeError::internal())?;
        let handle = self
            .leases
            .lease_for(validation.registered().table_id(), context.metadata().table())
            .await
            .map_err(|_| lease_runtime_error())?;
        Ok(Box::new(DirectStream {
            receipts: Arc::clone(&self.receipts),
            buffer: Arc::clone(&self.buffer),
            quiver: self.quiver.clone(),
            owner: self.owner,
            validation,
            receipt_lease_duration: self.receipt_lease_duration,
            admission_timeout: self.admission_timeout,
            emitter,
            emitted_receipts: 0,
            pending_receipt: None,
            _activity: activity,
            publication_handle: handle,
        }))
    }
}

struct DirectStream {
    receipts: Arc<dyn ReceiptStore>,
    buffer: Arc<BufferManager>,
    quiver: Option<Arc<QuiverStage>>,
    owner: CollectorInstanceId,
    validation: ValidatedIncomingSchema,
    receipt_lease_duration: Duration,
    admission_timeout: Duration,
    emitter: Arc<dyn PutResultEmitter>,
    emitted_receipts: u64,
    pending_receipt: Option<ReceiptVersion>,
    _activity: ActiveTableStream,
    publication_handle: Arc<crate::lease::TableLeaseHandle>,
}

#[async_trait]
impl StreamLifecycle for DirectStream {
    async fn push_batch(&mut self, batch: RecordBatch) -> RuntimeResult<()> {
        if batch.num_rows() == 0 {
            return Err(RuntimeError::new(
                ErrorCode::SchemaIncompatible,
                Code::InvalidArgument,
                "empty record batches are not accepted",
            ));
        }
        let rows = u64::try_from(batch.num_rows()).map_err(|_| RuntimeError::internal())?;
        let bytes = record_batch_logical_bytes(&batch).map_err(|_| RuntimeError::internal())?;
        let delivery = ValidatedDelivery::new(&self.validation, rows, 1, bytes)
            .map_err(|error| receipt_runtime_error(&error, None))?;
        if self.quiver.is_some() {
            // A persisted batch is acknowledged before it reaches the buffer, so
            // reject batches the buffer could never admit while the client can
            // still be told.
            self.buffer
                .check_admissible(delivery.table_id(), rows, bytes, 1)
                .map_err(|error| buffer_runtime_error(&error))?;
        }
        let handle = &self.publication_handle;
        let lease = handle.current().map_err(|_| lease_runtime_error())?;
        if lease.holder_instance_id() != self.owner {
            return Err(lease_runtime_error());
        }
        let deadline = receipt_deadline(&lease, self.receipt_lease_duration).map_err(|_| lease_runtime_error())?;
        let mut created = self
            .receipts
            .create_flight_batch(self.owner, &lease, deadline, std::slice::from_ref(&delivery))
            .await
            .map_err(|error| receipt_runtime_error(&error, None))?;
        let accepted = created.pop().ok_or_else(RuntimeError::internal)?;
        if !created.is_empty() {
            return Err(RuntimeError::internal());
        }
        let accepted_version = ReceiptVersion::new(accepted.receipt_id, accepted.revision);
        self.pending_receipt = Some(accepted_version);
        let receipt_id = accepted.receipt_id;
        if let Some(quiver) = &self.quiver {
            let request = PersistRequest {
                receipt: accepted_version,
                owner: self.owner,
                delivery: &delivery,
                lease: &lease,
                batch,
            };
            if let Err(error) = quiver.persist(request, self.admission_timeout).await {
                self.fail_pending("batch could not be persisted to local durable storage")
                    .await;
                return Err(buffer_runtime_error(&error).with_receipt_id(receipt_id));
            }
        } else {
            let admission = Admission {
                receipt: accepted_version,
                delivery,
                lease,
                batches: vec![batch],
            };
            if let Err(error) = self.buffer.admit_with_timeout(admission, self.admission_timeout).await {
                self.fail_pending("batch could not enter the bounded publication buffer")
                    .await;
                return Err(buffer_runtime_error(&error).with_receipt_id(receipt_id));
            }
        }
        self.pending_receipt = None;
        let summary = StreamSummary::new(1, rows, bytes);
        let metadata = PutResultMetadata::completion(
            StreamCompletion::receipt(receipt_id),
            self.validation.registered().version(),
            self.validation.current().version(),
            self.validation.registered().fingerprint(),
            summary,
        )
        .ok_or_else(RuntimeError::internal)?;
        self.emitter.emit(metadata).await?;
        self.emitted_receipts = self.emitted_receipts.saturating_add(1);
        Ok(())
    }

    async fn complete(&mut self, _summary: StreamSummary) -> RuntimeResult<StreamCompletion> {
        if self.emitted_receipts == 0 {
            return Err(RuntimeError::new(
                ErrorCode::SchemaIncompatible,
                Code::InvalidArgument,
                "DoPut stream contained no record batches",
            ));
        }
        Ok(StreamCompletion::results_already_emitted())
    }

    async fn abort(mut self: Box<Self>, _reason: StreamAbortReason) {
        self.fail_pending("batch admission was interrupted before buffering")
            .await;
    }
}

impl DirectStream {
    async fn fail_pending(&mut self, message: &'static str) {
        let Some(receipt) = self.pending_receipt.take() else {
            return;
        };
        let now = match now_unix_millis() {
            Ok(now) => now,
            Err(error) => {
                warn!(%error, receipt_id = %receipt.receipt_id, "could not timestamp interrupted receipt");
                return;
            }
        };
        let error = match ErrorStatus::new(ErrorCode::PublicationFailed, message) {
            Ok(error) => error.with_receipt_id(receipt.receipt_id),
            Err(error) => {
                warn!(%error, receipt_id = %receipt.receipt_id, "could not construct interrupted receipt failure");
                return;
            }
        };
        let failure = match ReceiptFailure::new(FailureDisposition::Retryable, error) {
            Ok(failure) => failure,
            Err(error) => {
                warn!(%error, receipt_id = %receipt.receipt_id, "could not validate interrupted receipt failure");
                return;
            }
        };
        if let Err(error) = self.receipts.fail_batch(self.owner, &[receipt], &failure, now).await {
            warn!(%error, receipt_id = %receipt.receipt_id, "could not persist interrupted receipt failure");
        }
    }
}

fn buffer_runtime_error(error: &BufferError) -> RuntimeError {
    match error.kind() {
        BufferErrorKind::CapacityExceeded => RuntimeError::new(
            ErrorCode::CapacityExhausted,
            Code::ResourceExhausted,
            "record batch exceeds configured collector capacity",
        ),
        BufferErrorKind::BackpressureTimeout => RuntimeError::new(
            ErrorCode::BackpressureTimeout,
            Code::ResourceExhausted,
            "timed out waiting for bounded collector capacity",
        ),
        BufferErrorKind::AdmissionClosed => RuntimeError::draining(),
        BufferErrorKind::TablePoisoned => lease_runtime_error(),
        BufferErrorKind::ReceiptStore => RuntimeError::new(
            ErrorCode::PublicationFailed,
            Code::Unavailable,
            "receipt persistence is unavailable",
        ),
        BufferErrorKind::Publication => RuntimeError::new(
            ErrorCode::PublicationFailed,
            Code::Unavailable,
            "Delta publication is unavailable",
        ),
        BufferErrorKind::DurableStorage => RuntimeError::new(
            ErrorCode::PublicationFailed,
            Code::Unavailable,
            "local durable storage is unavailable",
        ),
        BufferErrorKind::InvalidConfiguration
        | BufferErrorKind::UnsupportedCapability
        | BufferErrorKind::Synchronization => RuntimeError::internal(),
        _ => RuntimeError::internal(),
    }
}

fn receipt_runtime_error(error: &ReceiptError, receipt_id: Option<ReceiptId>) -> RuntimeError {
    let runtime = match error.kind() {
        ReceiptErrorKind::Fenced => lease_runtime_error(),
        ReceiptErrorKind::Draining => RuntimeError::draining(),
        ReceiptErrorKind::Conflict => RuntimeError::new(
            ErrorCode::ReceiptTransitionInvalid,
            Code::Aborted,
            "receipt state changed concurrently",
        ),
        ReceiptErrorKind::Database => RuntimeError::new(
            ErrorCode::PublicationFailed,
            Code::Unavailable,
            "receipt persistence is unavailable",
        ),
        ReceiptErrorKind::RequestTooLarge
        | ReceiptErrorKind::InvalidConfiguration
        | ReceiptErrorKind::NotFound
        | ReceiptErrorKind::CorruptState => RuntimeError::internal(),
        _ => RuntimeError::internal(),
    };
    receipt_id.map_or(runtime.clone(), |id| runtime.with_receipt_id(id))
}

fn lease_runtime_error() -> RuntimeError {
    RuntimeError::new(
        ErrorCode::LeaseFenced,
        Code::FailedPrecondition,
        "collector no longer owns the table publication lease",
    )
}
