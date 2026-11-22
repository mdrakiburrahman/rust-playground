use std::{sync::Arc, time::Duration};

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, new_null_array};
use arrow_schema::SchemaRef;
use arrowcol_catalog::{AlignmentAction, SchemaAlignmentPlan};
use arrowcol_protocol::ErrorCode;
use tonic::Code;

use crate::{RuntimeError, RuntimeResult};

/// Resource and timeout limits enforced by the shared runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeLimits {
    /// Optional accepted request compression; identity is always accepted.
    pub grpc_accept_compression: Option<tonic::codec::CompressionEncoding>,
    /// Optional response compression, used only when advertised by the client.
    pub grpc_send_compression: Option<tonic::codec::CompressionEncoding>,
    /// Maximum aggregate gRPC metadata bytes inspected before the body.
    pub max_metadata_bytes: usize,
    /// Maximum encoded Arrow schema header and body bytes.
    pub max_schema_bytes: usize,
    /// Maximum encoded protobuf bytes for one `FlightData` message.
    pub max_message_bytes: usize,
    /// Number of decoded record batches permitted in the bounded channel.
    pub batch_channel_capacity: usize,
    /// Number of pending `PutResult` items permitted per response.
    pub put_result_channel_capacity: usize,
    /// Maximum lifetime when the caller supplies no earlier deadline.
    pub max_stream_duration: Duration,
    /// Maximum time one implementation admission call may block.
    pub admission_timeout: Duration,
    /// Maximum Flight SQL DDL command length.
    pub max_ddl_bytes: usize,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            grpc_accept_compression: None,
            grpc_send_compression: None,
            max_metadata_bytes: 16 * 1_024,
            max_schema_bytes: 1_024 * 1_024,
            max_message_bytes: 16 * 1_024 * 1_024,
            batch_channel_capacity: 4,
            put_result_channel_capacity: 8,
            max_stream_duration: Duration::from_secs(5 * 60),
            admission_timeout: Duration::from_secs(30),
            max_ddl_bytes: 64 * 1_024,
        }
    }
}

impl RuntimeLimits {
    /// Validates that every bound is positive and schema messages fit messages.
    ///
    /// # Errors
    ///
    /// Returns an internal configuration error for an unusable bound.
    pub fn validate(&self) -> RuntimeResult<()> {
        if self.max_metadata_bytes == 0
            || self.max_schema_bytes == 0
            || self.max_message_bytes == 0
            || self.batch_channel_capacity == 0
            || self.put_result_channel_capacity == 0
            || self.max_stream_duration.is_zero()
            || self.admission_timeout.is_zero()
            || self.max_ddl_bytes == 0
            || self.max_schema_bytes > self.max_message_bytes
        {
            return Err(RuntimeError::new(
                ErrorCode::Internal,
                Code::Internal,
                "invalid Flight runtime limits",
            ));
        }
        Ok(())
    }
}

/// Applies a catalog exact/null-fill plan without concatenating or copying
/// existing Arrow arrays.
///
/// Exact batches are returned unchanged. Historical nullable-append batches
/// receive a new `RecordBatch` container whose source columns reuse the same
/// `Arc<dyn Array>` allocations and whose appended columns are all-null arrays.
///
/// # Errors
///
/// Returns a schema incompatibility error when the plan and schemas disagree,
/// or an internal error if Arrow rejects the constructed batch.
pub fn align_record_batch(
    batch: RecordBatch,
    alignment: &SchemaAlignmentPlan,
    current_schema: SchemaRef,
) -> RuntimeResult<RecordBatch> {
    match alignment {
        SchemaAlignmentPlan::Exact { .. } => {
            if batch.schema().as_ref() != current_schema.as_ref() {
                return Err(schema_alignment_error());
            }
            Ok(batch)
        }
        SchemaAlignmentPlan::NullableAppend { actions, .. } => {
            if actions.len() != current_schema.fields().len() {
                return Err(schema_alignment_error());
            }
            let row_count = batch.num_rows();
            let batch_schema = batch.schema();
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(actions.len());
            for (expected_target, action) in actions.iter().enumerate() {
                match action {
                    AlignmentAction::Source {
                        source_index,
                        target_index,
                    } if *target_index == expected_target => {
                        let source = batch.columns().get(*source_index).ok_or_else(schema_alignment_error)?;
                        let target_field = current_schema
                            .fields()
                            .get(*target_index)
                            .ok_or_else(schema_alignment_error)?;
                        let source_field = batch_schema
                            .fields()
                            .get(*source_index)
                            .ok_or_else(schema_alignment_error)?;
                        if source_field.as_ref() != target_field.as_ref()
                            || source.data_type() != target_field.data_type()
                        {
                            return Err(schema_alignment_error());
                        }
                        columns.push(Arc::clone(source));
                    }
                    AlignmentAction::NullFill { target_index, field }
                        if *target_index == expected_target && field.is_nullable() =>
                    {
                        let target_field = current_schema
                            .fields()
                            .get(*target_index)
                            .ok_or_else(schema_alignment_error)?;
                        let catalog_field = field.to_arrow().map_err(|_| RuntimeError::internal())?;
                        if &catalog_field != target_field.as_ref() {
                            return Err(schema_alignment_error());
                        }
                        columns.push(new_null_array(target_field.data_type(), row_count));
                    }
                    AlignmentAction::Source { .. } | AlignmentAction::NullFill { .. } => {
                        return Err(schema_alignment_error());
                    }
                }
            }
            RecordBatch::try_new_with_options(
                current_schema,
                columns,
                &RecordBatchOptions::new().with_row_count(Some(row_count)),
            )
            .map_err(|_| RuntimeError::internal())
        }
    }
}

fn schema_alignment_error() -> RuntimeError {
    RuntimeError::new(
        ErrorCode::SchemaIncompatible,
        Code::InvalidArgument,
        "catalog schema alignment plan did not match the decoded batch",
    )
}
