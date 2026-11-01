use arrow_array::RecordBatch;
use arrowcol_catalog::SchemaVersion;
use arrowcol_protocol::{
    CollectorInstanceId, FencingToken, LeaseId, ReceiptId, SchemaFingerprint as ProtocolFingerprint, TableIdentity,
    UnixMillis, fingerprint_schema,
};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::{
    PdataArrowRecordSet, PdataArrowTable, PdataRepresentation, PdataRepresentationId, PdataSchemaId,
    RepresentationVersion, SchemaFingerprint,
};
use serde::{Deserialize, Serialize};

/// Stable representation ID emitted by the Arrow Flight receiver.
pub const ARROWCOL_FLIGHT_REPRESENTATION_ID: &str = "arrowcol.flight";
/// Current representation contract version (`arrowcol.flight@1.1`).
///
/// Version 1.1 added [`ArrowcolBatchEnvelope::instance_key`], which lets a
/// restarted collector recognize durable-buffer replays admitted by its
/// predecessor. Other versions are rejected rather than reinterpreted.
pub const ARROWCOL_FLIGHT_REPRESENTATION_VERSION: RepresentationVersion = RepresentationVersion::new(1, 1);

const SCHEMA_ID_PREFIX: &str = "arrowcol.flight.v1:";
const ROOT_TABLE_ID: u32 = 0;
const MAX_INSTANCE_KEY_BYTES: usize = 200;

/// Arrowcol control metadata carried inside the representation-local schema ID.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArrowcolBatchEnvelope {
    /// Stable server-generated publication identity; absent on legacy durable pdata.
    #[serde(default)]
    pub publication_id: Option<ReceiptId>,
    /// Original admission UTC milliseconds, preserved in durable pdata.
    #[serde(default)]
    pub ingested_at_ms: Option<UnixMillis>,
    /// Logical catalog table.
    pub table: TableIdentity,
    /// Historical schema sent by the client.
    pub source_schema_version: SchemaVersion,
    /// Current catalog schema after null-fill alignment.
    pub target_schema_version: SchemaVersion,
    /// Routing lease lineage.
    pub lease_id: LeaseId,
    /// Collector process lifetime that admitted the stream.
    pub holder_instance_id: CollectorInstanceId,
    /// Monotonic routing fencing token.
    pub fencing_token: FencingToken,
    /// Stable deployment key (`registration.instance_key`) of the admitting collector.
    ///
    /// Unlike `holder_instance_id`, the key survives a process restart, so a
    /// successor that replays the same durable buffer can prove the batch was
    /// admitted by its own deployment slot.
    pub instance_key: String,
}

impl ArrowcolBatchEnvelope {
    /// Validates relationships that are independent of the routing store.
    ///
    /// # Errors
    ///
    /// Returns an invalid version-range error when the source is newer than
    /// the aligned target, or an invalid instance-key error when the key is
    /// empty, longer than 200 bytes, or contains control characters.
    pub fn validate(&self) -> Result<(), EnvelopeError> {
        if self.source_schema_version > self.target_schema_version {
            return Err(EnvelopeError::InvalidSchemaVersionRange);
        }
        if self.instance_key.is_empty()
            || self.instance_key.len() > MAX_INSTANCE_KEY_BYTES
            || self.instance_key.chars().any(char::is_control)
        {
            return Err(EnvelopeError::InvalidInstanceKey);
        }
        Ok(())
    }
}

/// One decoded generic Arrow batch and its preserved control metadata.
#[derive(Debug)]
pub struct DecodedArrowBatch {
    /// Representation-local table number.
    pub table_id: u32,
    /// Preserved Arrowcol envelope.
    pub envelope: ArrowcolBatchEnvelope,
    /// Owned Arrow batch.
    pub batch: RecordBatch,
}

/// Builds the fixed representation identity.
#[must_use]
pub fn arrowcol_flight_representation() -> PdataRepresentation {
    PdataRepresentation::new(
        PdataRepresentationId::new(ARROWCOL_FLIGHT_REPRESENTATION_ID),
        ARROWCOL_FLIGHT_REPRESENTATION_VERSION,
    )
}

/// Moves one validated Arrow batch into owned pluggable pdata.
///
/// The `RecordBatch` and its `Arc<dyn Array>` columns are moved directly. No
/// IPC/protobuf encoding, array rebuild, or value-buffer copy occurs.
///
/// # Errors
///
/// Returns invalid envelope or DFE pdata construction errors.
pub fn encode_record_batch(
    envelope: ArrowcolBatchEnvelope,
    batch: RecordBatch,
) -> Result<PdataArrowRecordSet, EnvelopeError> {
    envelope.validate()?;
    let rows = batch.num_rows();
    let fingerprint = fingerprint_schema(batch.schema_ref().as_ref());
    let table = PdataArrowTable::new(
        ROOT_TABLE_ID,
        encode_schema_id(&envelope)?.into(),
        SchemaFingerprint::new(*fingerprint.as_bytes()),
        batch,
    )
    .map_err(|error| EnvelopeError::Pdata(error.to_string()))?;
    PdataArrowRecordSet::new(
        arrowcol_flight_representation(),
        // The current DFE contract still requires an OTLP signal discriminator.
        // The Arrowcol representation does not interpret this as an OTAP layout.
        SignalType::Logs,
        rows,
        [table],
    )
    .map_err(|error| EnvelopeError::Pdata(error.to_string()))
}

/// Consumes and validates an Arrowcol pluggable record set.
///
/// # Errors
///
/// Returns representation, envelope, or schema-fingerprint validation errors.
pub fn decode_record_set(records: PdataArrowRecordSet) -> Result<Vec<DecodedArrowBatch>, EnvelopeError> {
    if records.representation() != &arrowcol_flight_representation() {
        return Err(EnvelopeError::UnsupportedRepresentation(
            records.representation().to_string(),
        ));
    }
    let (_, _, _, tables) = records.into_parts();
    if tables.is_empty() {
        return Err(EnvelopeError::EmptyRecordSet);
    }
    tables
        .into_iter()
        .map(|table| {
            let (table_id, schema_id, expected_fingerprint, batch) = table.into_parts();
            let envelope = decode_schema_id(&schema_id)?;
            envelope.validate()?;
            let actual = fingerprint_schema(batch.schema_ref().as_ref());
            if expected_fingerprint.as_bytes() != actual.as_bytes() {
                return Err(EnvelopeError::SchemaFingerprintMismatch {
                    expected: protocol_fingerprint(expected_fingerprint),
                    actual,
                });
            }
            Ok(DecodedArrowBatch {
                table_id,
                envelope,
                batch,
            })
        })
        .collect()
}

fn encode_schema_id(envelope: &ArrowcolBatchEnvelope) -> Result<String, EnvelopeError> {
    let encoded =
        serde_json::to_vec(envelope).map_err(|error| EnvelopeError::InvalidSchemaIdentity(error.to_string()))?;
    Ok(format!("{SCHEMA_ID_PREFIX}{}", URL_SAFE_NO_PAD.encode(encoded)))
}

fn decode_schema_id(schema_id: &PdataSchemaId) -> Result<ArrowcolBatchEnvelope, EnvelopeError> {
    let encoded = schema_id
        .as_str()
        .strip_prefix(SCHEMA_ID_PREFIX)
        .ok_or(EnvelopeError::InvalidSchemaIdentity(
            "missing Arrowcol schema identity prefix".to_owned(),
        ))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|error| EnvelopeError::InvalidSchemaIdentity(error.to_string()))?;
    serde_json::from_slice(&bytes).map_err(|error| EnvelopeError::InvalidSchemaIdentity(error.to_string()))
}

fn protocol_fingerprint(fingerprint: SchemaFingerprint) -> ProtocolFingerprint {
    ProtocolFingerprint::from_bytes(*fingerprint.as_bytes())
}

/// Invalid or unsupported Arrowcol pdata envelope.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EnvelopeError {
    /// A different representation reached the generic Delta exporter.
    #[error("unsupported pdata representation `{0}`")]
    UnsupportedRepresentation(String),
    /// A representation had no tables.
    #[error("Arrowcol pdata record set contains no tables")]
    EmptyRecordSet,
    /// The schema identity could not be decoded.
    #[error("invalid Arrowcol pdata schema identity: {0}")]
    InvalidSchemaIdentity(String),
    /// Source schema cannot be newer than the aligned target.
    #[error("Arrowcol pdata source schema version exceeds target schema version")]
    InvalidSchemaVersionRange,
    /// The admitting collector's instance key is empty, oversized, or contains control characters.
    #[error("Arrowcol pdata instance key must be 1-200 bytes without control characters")]
    InvalidInstanceKey,
    /// The Arrow arrays no longer match the admitted schema fingerprint.
    #[error("Arrowcol pdata schema fingerprint mismatch: expected {expected}, got {actual}")]
    SchemaFingerprintMismatch {
        /// Representation-supplied fingerprint.
        expected: ProtocolFingerprint,
        /// Fingerprint recomputed from the owned batch.
        actual: ProtocolFingerprint,
    },
    /// DFE pdata rejected construction.
    #[error("could not construct DFE pdata: {0}")]
    Pdata(String),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::sync::Arc;

    use arrow_array::{ArrayRef, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use arrowcol_protocol::{CatalogName, CollectorInstanceId, FencingToken, LeaseId, SchemaName, TableName};
    use base64::Engine as _;

    use super::*;

    fn envelope() -> ArrowcolBatchEnvelope {
        ArrowcolBatchEnvelope {
            publication_id: Some(ReceiptId::new()),
            ingested_at_ms: Some(UnixMillis::new(1_700_000_012_345).expect("stamp")),
            table: TableIdentity::new(
                CatalogName::new("demo").expect("catalog"),
                SchemaName::new("generated").expect("schema"),
                TableName::new("events").expect("table"),
            ),
            source_schema_version: SchemaVersion::FIRST,
            target_schema_version: SchemaVersion::FIRST,
            lease_id: LeaseId::new(),
            holder_instance_id: CollectorInstanceId::new(),
            fencing_token: FencingToken::new(7).expect("fence"),
            instance_key: "arrowcol-dataflow-0".to_owned(),
        }
    }

    fn records_with_version(version: RepresentationVersion) -> PdataArrowRecordSet {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(UInt64Array::from(vec![1]))]).expect("batch");
        let fingerprint = fingerprint_schema(schema.as_ref());
        let table = PdataArrowTable::new(
            ROOT_TABLE_ID,
            encode_schema_id(&envelope()).expect("schema id").into(),
            SchemaFingerprint::new(*fingerprint.as_bytes()),
            batch,
        )
        .expect("table");
        PdataArrowRecordSet::new(
            PdataRepresentation::new(PdataRepresentationId::new(ARROWCOL_FLIGHT_REPRESENTATION_ID), version),
            SignalType::Logs,
            1,
            [table],
        )
        .expect("records")
    }

    /// Scenario: The receiver labels pdata with the current Arrowcol representation identity.
    /// Guarantees: The exact wire label is `arrowcol.flight@1.1`, the version that carries the instance key.
    #[test]
    fn representation_is_version_one_one() {
        assert_eq!(arrowcol_flight_representation().to_string(), "arrowcol.flight@1.1");
    }

    /// Scenario: An arbitrary generated schema is moved into and out of Arrowcol pdata.
    /// Guarantees: Table/schema/lease metadata, the admitting instance key, and Arrow value-buffer
    /// ownership are preserved exactly.
    #[test]
    fn arbitrary_schema_round_trip_is_zero_copy() {
        let ids = Arc::new(UInt64Array::from(vec![1, 2, 3])) as ArrayRef;
        let names = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::clone(&ids), Arc::clone(&names)]).expect("batch");
        let id_values = ids
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("u64")
            .values()
            .as_ptr();
        let expected = envelope();
        let records = encode_record_batch(expected.clone(), batch).expect("encode");
        let decoded = decode_record_set(records).expect("decode");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].envelope, expected);
        assert_eq!(decoded[0].envelope.instance_key, "arrowcol-dataflow-0");
        assert!(Arc::ptr_eq(decoded[0].batch.column(0), &ids));
        let decoded_values = decoded[0]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("u64")
            .values()
            .as_ptr();
        assert_eq!(decoded_values, id_values);
    }

    /// Scenario: A batch schema changes after an adapter-supplied fingerprint is created.
    /// Guarantees: The exporter rejects the mismatch before any Delta side effect.
    #[test]
    fn rejects_fingerprint_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(UInt64Array::from(vec![1]))]).expect("batch");
        let table = PdataArrowTable::new(
            ROOT_TABLE_ID,
            encode_schema_id(&envelope()).expect("schema id").into(),
            SchemaFingerprint::new([0xAA; 32]),
            batch,
        )
        .expect("table");
        let records =
            PdataArrowRecordSet::new(arrowcol_flight_representation(), SignalType::Logs, 1, [table]).expect("records");

        assert!(matches!(
            decode_record_set(records),
            Err(EnvelopeError::SchemaFingerprintMismatch { .. })
        ));
    }

    /// Scenario: A record set labeled with the previous `arrowcol.flight@1.0` contract reaches the exporter.
    /// Guarantees: Decoding rejects the unsupported version by name instead of guessing a missing instance key.
    #[test]
    fn rejects_previous_representation_version() {
        let error = decode_record_set(records_with_version(RepresentationVersion::new(1, 0))).expect_err("1.0");
        assert_eq!(
            error,
            EnvelopeError::UnsupportedRepresentation("arrowcol.flight@1.0".to_owned())
        );
        assert_eq!(
            decode_record_set(records_with_version(ARROWCOL_FLIGHT_REPRESENTATION_VERSION))
                .expect("1.1")
                .len(),
            1
        );
    }

    /// Scenario: A schema identity omits the v1.1 instance key or adds an unknown field.
    /// Guarantees: Strict decoding rejects both shapes, and encoding refuses an empty instance key.
    #[test]
    fn envelope_fields_are_strict() {
        let mut value = serde_json::to_value(envelope()).expect("envelope JSON");
        let object = value.as_object_mut().expect("object");
        object.remove("instance_key");
        let missing = format!(
            "{SCHEMA_ID_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&value).expect("JSON"))
        );
        assert!(matches!(
            decode_schema_id(&missing.into()),
            Err(EnvelopeError::InvalidSchemaIdentity(_))
        ));

        let mut value = serde_json::to_value(envelope()).expect("envelope JSON");
        value["unexpected"] = serde_json::json!(true);
        let unknown = format!(
            "{SCHEMA_ID_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&value).expect("JSON"))
        );
        assert!(matches!(
            decode_schema_id(&unknown.into()),
            Err(EnvelopeError::InvalidSchemaIdentity(_))
        ));

        let mut empty = envelope();
        empty.instance_key.clear();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)])),
            vec![Arc::new(UInt64Array::from(vec![1]))],
        )
        .expect("batch");
        assert!(matches!(
            encode_record_batch(empty, batch),
            Err(EnvelopeError::InvalidInstanceKey)
        ));
    }
}
