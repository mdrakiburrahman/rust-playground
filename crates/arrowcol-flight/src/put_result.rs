use arrow_flight::PutResult;
use arrowcol_catalog::SchemaVersion;
use arrowcol_protocol::{ProtocolVersion, SchemaFingerprint};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{RuntimeError, RuntimeResult, StreamSummary};

/// Current stable JSON application-metadata wire version.
pub const PUT_RESULT_WIRE_VERSION: u32 = 1;

/// Delivery guarantee explicitly reported by a completion.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryGuarantee {
    /// Every batch was persisted to local durable storage before completion;
    /// the collector owns delivery from there without a client-queryable delivery receipt.
    DurableAdmission,
    /// The runtime admitted the batches without promising durable buffering.
    BestEffort,
}

/// Completion mode selected by a dataflow admission implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionMode {
    /// Dataflow mode admitted the stream on a clearly labeled best-effort basis.
    BestEffortAdmission,
    /// Dataflow mode persisted every batch to a local durable buffer before
    /// completing the stream.
    DurableAdmission,
    /// The implementation emitted per-batch progress acknowledgements, so no
    /// synthetic final result is added.
    ResultsAlreadyEmitted,
}

/// Completion returned by a [`crate::StreamLifecycle`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamCompletion {
    mode: CompletionMode,
}

impl StreamCompletion {
    /// Creates a clearly labeled best-effort dataflow completion.
    #[must_use]
    pub const fn best_effort() -> Self {
        Self {
            mode: CompletionMode::BestEffortAdmission,
        }
    }

    /// Creates a durable-admission completion: every batch was persisted to a
    /// local durable buffer before the stream completed.
    #[must_use]
    pub const fn durable_admission() -> Self {
        Self {
            mode: CompletionMode::DurableAdmission,
        }
    }

    /// Completes a stream whose per-batch progress acknowledgements were already emitted.
    #[must_use]
    pub const fn results_already_emitted() -> Self {
        Self {
            mode: CompletionMode::ResultsAlreadyEmitted,
        }
    }

    /// Returns the implementation-selected completion mode.
    #[must_use]
    pub const fn mode(self) -> CompletionMode {
        self.mode
    }
}

/// Stable application metadata carried by an Arrow Flight [`PutResult`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PutResultMetadata {
    /// Optional progress acknowledgement emitted during admission.
    Progress {
        /// Stable application-metadata wire version.
        wire_version: u32,
        /// Arrowcol protocol version.
        protocol_version: ProtocolVersion,
        /// Monotonic progress sequence chosen by the implementation.
        sequence: u64,
        /// Batches admitted so far.
        batches: u64,
        /// Rows admitted so far.
        rows: u64,
        /// Approximate Arrow array bytes admitted so far.
        bytes: u64,
    },
    /// Best-effort dataflow completion.
    BestEffortAdmission {
        /// Stable application-metadata wire version.
        wire_version: u32,
        /// Arrowcol protocol version.
        protocol_version: ProtocolVersion,
        /// Explicit delivery guarantee.
        delivery_guarantee: DeliveryGuarantee,
        /// Registered schema version sent by the client.
        source_schema_version: SchemaVersion,
        /// Current authoritative target schema version.
        target_schema_version: SchemaVersion,
        /// Fingerprint computed from the received Arrow schema.
        schema_fingerprint: SchemaFingerprint,
        /// Final admitted stream summary.
        summary: StreamSummary,
    },
    /// Dataflow completion after every batch was persisted to a local durable
    /// buffer; the client receives no queryable delivery receipt.
    DurableAdmission {
        /// Stable application-metadata wire version.
        wire_version: u32,
        /// Arrowcol protocol version.
        protocol_version: ProtocolVersion,
        /// Explicit delivery guarantee.
        delivery_guarantee: DeliveryGuarantee,
        /// Registered schema version sent by the client.
        source_schema_version: SchemaVersion,
        /// Current authoritative target schema version.
        target_schema_version: SchemaVersion,
        /// Fingerprint computed from the received Arrow schema.
        schema_fingerprint: SchemaFingerprint,
        /// Final admitted stream summary.
        summary: StreamSummary,
    },
}

impl PutResultMetadata {
    /// Creates a progress acknowledgement.
    #[must_use]
    pub const fn progress(sequence: u64, summary: StreamSummary) -> Self {
        Self::Progress {
            wire_version: PUT_RESULT_WIRE_VERSION,
            protocol_version: ProtocolVersion::CURRENT,
            sequence,
            batches: summary.batches,
            rows: summary.rows,
            bytes: summary.bytes,
        }
    }

    /// Creates final metadata from an implementation completion.
    #[must_use]
    pub const fn completion(
        completion: StreamCompletion,
        source_schema_version: SchemaVersion,
        target_schema_version: SchemaVersion,
        schema_fingerprint: SchemaFingerprint,
        summary: StreamSummary,
    ) -> Option<Self> {
        match completion.mode {
            CompletionMode::BestEffortAdmission => Some(Self::BestEffortAdmission {
                wire_version: PUT_RESULT_WIRE_VERSION,
                protocol_version: ProtocolVersion::CURRENT,
                delivery_guarantee: DeliveryGuarantee::BestEffort,
                source_schema_version,
                target_schema_version,
                schema_fingerprint,
                summary,
            }),
            CompletionMode::DurableAdmission => Some(Self::DurableAdmission {
                wire_version: PUT_RESULT_WIRE_VERSION,
                protocol_version: ProtocolVersion::CURRENT,
                delivery_guarantee: DeliveryGuarantee::DurableAdmission,
                source_schema_version,
                target_schema_version,
                schema_fingerprint,
                summary,
            }),
            CompletionMode::ResultsAlreadyEmitted => None,
        }
    }

    /// Encodes canonical compact JSON application metadata.
    ///
    /// # Errors
    ///
    /// Returns an internal error if serialization unexpectedly fails.
    pub fn encode(&self) -> RuntimeResult<Bytes> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|_| RuntimeError::internal())
    }

    /// Decodes and validates application metadata.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for malformed or unsupported metadata.
    pub fn decode(encoded: &[u8]) -> RuntimeResult<Self> {
        let metadata: Self = serde_json::from_slice(encoded).map_err(|_| {
            RuntimeError::new(
                arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
                tonic::Code::InvalidArgument,
                "invalid PutResult application metadata",
            )
        })?;
        if metadata.wire_version() != PUT_RESULT_WIRE_VERSION {
            return Err(RuntimeError::new(
                arrowcol_protocol::ErrorCode::ProtocolVersionUnsupported,
                tonic::Code::InvalidArgument,
                "unsupported PutResult application metadata version",
            ));
        }
        let valid = match &metadata {
            Self::Progress { .. } => true,
            Self::BestEffortAdmission {
                delivery_guarantee,
                source_schema_version,
                target_schema_version,
                ..
            } => *delivery_guarantee == DeliveryGuarantee::BestEffort && source_schema_version <= target_schema_version,
            Self::DurableAdmission {
                delivery_guarantee,
                source_schema_version,
                target_schema_version,
                ..
            } => {
                *delivery_guarantee == DeliveryGuarantee::DurableAdmission
                    && source_schema_version <= target_schema_version
            }
        };
        if !valid {
            return Err(RuntimeError::new(
                arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
                tonic::Code::InvalidArgument,
                "inconsistent PutResult application metadata",
            ));
        }
        Ok(metadata)
    }

    /// Encodes this metadata as an Arrow Flight `PutResult`.
    ///
    /// # Errors
    ///
    /// Returns an internal error if serialization unexpectedly fails.
    pub fn into_put_result(self) -> RuntimeResult<PutResult> {
        Ok(PutResult {
            app_metadata: self.encode()?,
        })
    }

    /// Decodes metadata from an Arrow Flight `PutResult`.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for malformed or unsupported metadata.
    pub fn from_put_result(result: &PutResult) -> RuntimeResult<Self> {
        Self::decode(&result.app_metadata)
    }

    /// Returns the stable wire version.
    #[must_use]
    pub const fn wire_version(&self) -> u32 {
        match self {
            Self::Progress { wire_version, .. }
            | Self::BestEffortAdmission { wire_version, .. }
            | Self::DurableAdmission { wire_version, .. } => *wire_version,
        }
    }

    /// Returns the explicitly labeled delivery guarantee for completions.
    #[must_use]
    pub const fn delivery_guarantee(&self) -> Option<DeliveryGuarantee> {
        match self {
            Self::Progress { .. } => None,
            Self::BestEffortAdmission { delivery_guarantee, .. }
            | Self::DurableAdmission { delivery_guarantee, .. } => Some(*delivery_guarantee),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::Schema;
    use arrowcol_catalog::SchemaVersion;
    use arrowcol_protocol::{SchemaFingerprint, fingerprint_schema};

    use super::{DeliveryGuarantee, PutResultMetadata, StreamCompletion};
    use crate::StreamSummary;

    /// Scenario: A dataflow stream completes after durable-buffer persistence.
    /// Guarantees: The final PutResult is labeled `durable_admission` and survives
    /// an encode/decode round trip.
    #[test]
    fn durable_admission_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let fingerprint: SchemaFingerprint = fingerprint_schema(&Schema::empty());
        let metadata = PutResultMetadata::completion(
            StreamCompletion::durable_admission(),
            SchemaVersion::FIRST,
            SchemaVersion::FIRST,
            fingerprint,
            StreamSummary::new(3, 30, 300),
        )
        .ok_or("durable admission must produce a completion")?;
        let encoded = metadata.encode()?;
        let json: serde_json::Value = serde_json::from_slice(&encoded)?;
        assert_eq!(json["kind"], "durable_admission");
        assert_eq!(json["delivery_guarantee"], "durable_admission");
        assert!(json.get("receipt_id").is_none());

        let decoded = PutResultMetadata::decode(&encoded)?;
        assert_eq!(decoded, metadata);
        assert_eq!(decoded.delivery_guarantee(), Some(DeliveryGuarantee::DurableAdmission));
        Ok(())
    }

    /// Scenario: A durable-admission result claims a different delivery guarantee.
    /// Guarantees: Decoding rejects the inconsistent label instead of overstating durability.
    #[test]
    fn durable_admission_rejects_mismatched_guarantee() -> Result<(), Box<dyn std::error::Error>> {
        let fingerprint: SchemaFingerprint = fingerprint_schema(&Schema::empty());
        let metadata = PutResultMetadata::completion(
            StreamCompletion::durable_admission(),
            SchemaVersion::FIRST,
            SchemaVersion::FIRST,
            fingerprint,
            StreamSummary::new(1, 1, 1),
        )
        .ok_or("durable admission must produce a completion")?;
        let mut json: serde_json::Value = serde_json::from_slice(&metadata.encode()?)?;
        json["delivery_guarantee"] = serde_json::json!("best_effort");
        let tampered = serde_json::to_vec(&json)?;
        assert!(PutResultMetadata::decode(&tampered).is_err());
        Ok(())
    }

    #[test]
    fn already_emitted_progress_does_not_create_a_synthetic_result() {
        let fingerprint: SchemaFingerprint = fingerprint_schema(&Schema::empty());
        let metadata = PutResultMetadata::completion(
            StreamCompletion::results_already_emitted(),
            SchemaVersion::FIRST,
            SchemaVersion::FIRST,
            fingerprint,
            StreamSummary::new(2, 4, 8),
        );
        assert!(metadata.is_none());
    }
}
