use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow, bail};
use arrow_array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray, UInt8Array, types::Int8Type,
};
use arrow_flight::{
    FlightData, FlightDescriptor, encode::FlightDataEncoderBuilder, error::FlightError,
    flight_descriptor::DescriptorType,
};
use arrow_schema::{DataType, Schema, TimeUnit};
use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlCommand, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest,
    MemoryCatalogStore,
};
use arrowcol_dataflow_components::{ArrowcolBatchEnvelope, decode_record_set, encode_record_batch};
use arrowcol_flight::{
    AllowAllAuthenticator, ArrowColFlightService, BatchAdmission, PutResultEmitter, RuntimeError, RuntimeLimits,
    RuntimeResult, StreamCompletion, StreamContext, StreamLifecycle, StreamSummary,
};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, CatalogName, ClaimedSchemaFingerprint, CollectorInstanceId,
    FencingToken, ImplementationPool, IngestMetadata, LeaseId, PrincipalId, PrincipalIssuer, ProtocolVersion,
    SchemaName, TableIdentity, TableName, UnixMillis, fingerprint_schema,
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::{TryStreamExt, stream};
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::{
    PdataArrowRecordSet, PdataArrowTable, PdataRepresentation, PdataRepresentationId, RepresentationVersion,
    SchemaFingerprint as PdataSchemaFingerprint,
};
use prost::Message as _;
use serde_json::json;
use tonic::{Request, Status, metadata::MetadataValue};

const DMV_DDL: &str = include_str!("../../../projects/loadtest/schemas/sqlserver.sql");
const HISTORICAL_CASE: &str = "Diagnostic / IPC None / sqlserver_active_sessions / 65,536 rows";
const SCHEMA_ID_PREFIX: &str = "arrowcol.flight.v1:";

#[derive(Default)]
struct Captured {
    batches: Mutex<Vec<(RecordBatch, ArrowcolBatchEnvelope)>>,
    encode_ns: AtomicU64,
    decode_ns: AtomicU64,
}

struct EnvelopeAdmission(Arc<Captured>);

#[async_trait]
impl BatchAdmission for EnvelopeAdmission {
    async fn begin(
        &self,
        context: StreamContext,
        _emitter: Arc<dyn PutResultEmitter>,
    ) -> RuntimeResult<Box<dyn StreamLifecycle>> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| RuntimeError::internal())?
            .as_millis()
            .try_into()
            .map_err(|_| RuntimeError::internal())?;
        let timestamp = UnixMillis::new(now_ms).map_err(|_| RuntimeError::internal())?;
        let envelope = ArrowcolBatchEnvelope {
            publication_id: Some(arrowcol_protocol::PublicationId::new()),
            ingested_at_ms: Some(timestamp),
            table: context.metadata().table().clone(),
            source_schema_version: context.registered_schema().version(),
            target_schema_version: context.current_schema().version(),
            lease_id: LeaseId::new(),
            holder_instance_id: CollectorInstanceId::new(),
            fencing_token: FencingToken::new(1).map_err(|_| RuntimeError::internal())?,
            instance_key: "flight-envelope-fixture".to_owned(),
        };
        Ok(Box::new(EnvelopeLifecycle {
            capture: Arc::clone(&self.0),
            envelope,
        }))
    }
}

struct EnvelopeLifecycle {
    capture: Arc<Captured>,
    envelope: ArrowcolBatchEnvelope,
}

#[async_trait]
impl StreamLifecycle for EnvelopeLifecycle {
    async fn push_batch(&mut self, batch: RecordBatch) -> RuntimeResult<()> {
        let started = Instant::now();
        let records = encode_record_batch(self.envelope.clone(), batch).map_err(|_| RuntimeError::internal())?;
        self.capture.encode_ns.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let started = Instant::now();
        let mut decoded = decode_record_set(records).map_err(|_| RuntimeError::internal())?;
        self.capture.decode_ns.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if decoded.len() != 1 {
            return Err(RuntimeError::internal());
        }
        let value = decoded.pop().ok_or_else(RuntimeError::internal)?;
        let mut batches = self.capture.batches.lock().map_err(|_| RuntimeError::internal())?;
        batches.push((value.batch, value.envelope));
        Ok(())
    }

    async fn complete(&mut self, _summary: StreamSummary) -> RuntimeResult<StreamCompletion> {
        Ok(StreamCompletion::best_effort())
    }

    async fn abort(self: Box<Self>, _reason: arrowcol_flight::StreamAbortReason) {}
}

fn test_auth() -> Result<AuthContext> {
    let principal = AuthenticatedPrincipal::new(
        PrincipalId::new("flight-envelope-fixture")?,
        PrincipalIssuer::new("local-fixture")?,
        Default::default(),
    )?;
    Ok(AuthContext::new(
        principal,
        AuthMethod::WorkloadIdentity,
        UnixMillis::new(1)?,
        None,
    )?)
}

fn active_sessions_ddl() -> Result<(String, TableIdentity)> {
    let start = DMV_DDL
        .find("CREATE TABLE telemetry.sqlserver.sqlserver_active_sessions")
        .ok_or_else(|| anyhow!("public sqlserver_active_sessions fixture was not found"))?;
    let remaining = &DMV_DDL[start..];
    let end = remaining
        .find(';')
        .ok_or_else(|| anyhow!("public sqlserver_active_sessions DDL is unterminated"))?;
    let statement = remaining[..end].trim();
    let DdlCommand::CreateTable(create) = DdlParser::default().parse(statement)? else {
        bail!("public DMV fixture did not parse as CREATE TABLE");
    };
    Ok((statement.to_owned(), create.identity().clone()))
}

async fn setup() -> Result<(
    ArrowColFlightService,
    Arc<Captured>,
    TableIdentity,
    Arc<Schema>,
    arrowcol_catalog::SchemaVersion,
)> {
    let (ddl, identity) = active_sessions_ddl()?;
    let catalog = Arc::new(MemoryCatalogStore::new());
    let control = CatalogControlPlane::new(DdlParser::default(), Arc::clone(&catalog) as Arc<dyn CatalogStore>);
    control.execute_ddl(FlightSqlDdlRequest::new(ddl.as_str())).await?;
    let current = catalog
        .current_schema(&identity)
        .await?
        .ok_or_else(|| anyhow!("registered DMV schema is missing"))?;
    let schema = Arc::new(current.arrow_schema()?);
    let captured = Arc::new(Captured::default());
    let version = current.version();
    let service = ArrowColFlightService::new(
        ImplementationPool::Dataflow,
        catalog,
        Arc::new(AllowAllAuthenticator::new(test_auth()?)),
        Arc::new(EnvelopeAdmission(Arc::clone(&captured))),
        RuntimeLimits::default(),
    )?;
    Ok((service, captured, identity, schema, version))
}

fn public_dmv_batch(schema: Arc<Schema>, rows: usize) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| -> Result<ArrayRef> {
            let name = field.name();
            Ok(match field.data_type() {
                DataType::Utf8 if name == "customer_id" => Arc::new(StringArray::from_iter_values(
                    std::iter::repeat_n("synthetic-customer", rows),
                )),
                DataType::Utf8 => Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|row| format!("{name}-{row}")),
                )),
                DataType::Boolean => Arc::new(BooleanArray::from_iter((0..rows).map(|row| Some(row % 2 == 0)))),
                DataType::UInt8 => Arc::new(UInt8Array::from_iter_values((0..rows).map(|row| (row % 255) as u8))),
                DataType::Int16 => Arc::new(Int16Array::from_iter_values((0..rows).map(|row| (row % 32767) as i16))),
                DataType::Int32 => Arc::new(Int32Array::from_iter_values((0..rows).map(|row| row as i32))),
                DataType::Int64 => Arc::new(Int64Array::from_iter_values((0..rows).map(|row| row as i64))),
                DataType::Float64 => Arc::new(Float64Array::from_iter_values((0..rows).map(|row| row as f64 / 10.0))),
                DataType::Timestamp(TimeUnit::Microsecond, timezone) => Arc::new(
                    TimestampMicrosecondArray::from_iter_values(
                        (0..rows).map(|row| 1_700_000_000_000_000_i64 + row as i64),
                    )
                    .with_timezone_opt(timezone.clone()),
                ),
                other => bail!("unsupported public DMV type {other} on {name}"),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn flight_request(
    messages: &[FlightData],
    schema: &Schema,
    identity: &TableIdentity,
) -> Result<Request<impl futures::Stream<Item = Result<FlightData, Status>> + Send + 'static>> {
    let owned_messages = messages.to_vec();
    let mut request = Request::new(stream::iter(owned_messages.into_iter().map(Ok)));
    let metadata = IngestMetadata::new(
        ProtocolVersion::CURRENT,
        ImplementationPool::Dataflow,
        identity.clone(),
        ClaimedSchemaFingerprint::new(fingerprint_schema(schema)),
    );
    for (key, value) in metadata.to_ascii_pairs() {
        request
            .metadata_mut()
            .insert(key, MetadataValue::try_from(value.as_str())?);
    }
    Ok(request)
}

async fn encode_flight(
    schema: Arc<Schema>,
    identity: &TableIdentity,
    version: arrowcol_catalog::SchemaVersion,
    batch: RecordBatch,
) -> Result<Vec<FlightData>> {
    let descriptor = FlightDescriptor {
        r#type: DescriptorType::Path as i32,
        cmd: Default::default(),
        path: vec![
            identity.catalog().to_string(),
            identity.schema().to_string(),
            identity.table().to_string(),
            format!("v{version}"),
        ],
    };
    Ok(FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .with_flight_descriptor(Some(descriptor))
        .build(stream::iter([Ok::<RecordBatch, FlightError>(batch)]))
        .try_collect::<Vec<_>>()
        .await?)
}

async fn receive_once(
    service: &ArrowColFlightService,
    messages: &[FlightData],
    schema: &Schema,
    identity: &TableIdentity,
) -> Result<()> {
    let request = flight_request(messages, schema, identity)?;
    let response = service.handle_do_put(request).await?;
    let results = response.into_inner().try_collect::<Vec<_>>().await?;
    if results.is_empty() {
        bail!("Flight receive ended without an admission result");
    }
    Ok(())
}

fn ipc_footprint(messages: &[FlightData]) -> (usize, usize) {
    let ipc = messages
        .iter()
        .map(|message| message.data_header.len() + message.data_body.len())
        .sum();
    let protobuf = messages.iter().map(|message| message.encoded_len()).sum();
    (ipc, protobuf)
}

fn envelope_fixture() -> Result<ArrowcolBatchEnvelope> {
    Ok(ArrowcolBatchEnvelope {
        publication_id: Some(arrowcol_protocol::PublicationId::new()),
        ingested_at_ms: Some(UnixMillis::new(1_700_000_000_000)?),
        table: TableIdentity::new(
            CatalogName::new("telemetry")?,
            SchemaName::new("fixture")?,
            TableName::new("dictionary_batch")?,
        ),
        source_schema_version: arrowcol_catalog::SchemaVersion::FIRST,
        target_schema_version: arrowcol_catalog::SchemaVersion::FIRST,
        lease_id: LeaseId::new(),
        holder_instance_id: CollectorInstanceId::new(),
        fencing_token: FencingToken::new(1)?,
        instance_key: "flight-envelope-fixture".to_owned(),
    })
}

fn nullable_dictionary_batch() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        arrow_schema::Field::new("nullable_text", DataType::Utf8, true),
        arrow_schema::Field::new(
            "dictionary_text",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        ),
    ]));
    let nullable = StringArray::from(vec![Some("present"), None, Some("again")]);
    let keys = Int8Array::from(vec![Some(0), Some(1), Some(0)]);
    let values = Arc::new(StringArray::from(vec!["first", "second"])) as ArrayRef;
    let dictionary = DictionaryArray::<Int8Type>::try_new(keys, values)?;
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(nullable), Arc::new(dictionary)],
    )?)
}

fn legacy_record_set(envelope: ArrowcolBatchEnvelope, batch: RecordBatch) -> Result<PdataArrowRecordSet> {
    let json = serde_json::to_vec(&envelope)?;
    let schema_id = format!("{SCHEMA_ID_PREFIX}{}", URL_SAFE_NO_PAD.encode(json));
    let fingerprint = fingerprint_schema(batch.schema_ref().as_ref());
    let table = PdataArrowTable::new(
        0,
        schema_id.into(),
        PdataSchemaFingerprint::new(*fingerprint.as_bytes()),
        batch,
    )
    .map_err(|error| anyhow!(error.to_string()))?;
    PdataArrowRecordSet::new(
        PdataRepresentation::new(
            PdataRepresentationId::new("arrowcol.flight"),
            RepresentationVersion::new(1, 1),
        ),
        SignalType::Logs,
        3,
        [table],
    )
    .map_err(|error| anyhow!(error.to_string()))
}

#[test]
fn envelope_round_trips_nullable_dictionary_batches_in_v1_1_and_v1_2() -> Result<()> {
    let batch = nullable_dictionary_batch()?;
    let envelope = envelope_fixture()?;
    let current = decode_record_set(encode_record_batch(envelope.clone(), batch.clone())?)?;
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].envelope, envelope);
    assert_eq!(current[0].batch.schema(), batch.schema());
    for column in 0..batch.num_columns() {
        assert_eq!(
            current[0].batch.column(column).to_data(),
            batch.column(column).to_data()
        );
    }

    let legacy_envelope = envelope_fixture()?;
    let legacy = decode_record_set(legacy_record_set(legacy_envelope.clone(), batch.clone())?)?;
    assert_eq!(legacy.len(), 1);
    assert_eq!(legacy[0].envelope, legacy_envelope);
    assert_eq!(legacy[0].batch.schema(), batch.schema());
    for column in 0..batch.num_columns() {
        assert_eq!(legacy[0].batch.column(column).to_data(), batch.column(column).to_data());
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "release-only historical Flight receive and pdata envelope measurement"]
async fn historical_diagnostic_ipc_none_sqlserver_active_sessions_65536() -> Result<()> {
    let (service, capture, identity, schema, current) = setup().await?;
    let batch = public_dmv_batch(Arc::clone(&schema), 65_536)?;
    let logical = arrow_deltalake::record_batch_logical_bytes(&batch)?;
    let array_bytes = batch.get_array_memory_size();
    let messages = encode_flight(Arc::clone(&schema), &identity, current, batch.clone()).await?;
    let (ipc_bytes, protobuf_bytes) = ipc_footprint(&messages);
    let mut samples_ns = Vec::new();

    for _ in 0..2 {
        capture
            .batches
            .lock()
            .map_err(|_| anyhow!("capture lock poisoned"))?
            .clear();
        receive_once(&service, &messages, &schema, &identity).await?;
    }
    capture.encode_ns.store(0, Ordering::Relaxed);
    capture.decode_ns.store(0, Ordering::Relaxed);

    for _ in 0..5 {
        capture
            .batches
            .lock()
            .map_err(|_| anyhow!("capture lock poisoned"))?
            .clear();
        let start = Instant::now();
        receive_once(&service, &messages, &schema, &identity).await?;
        samples_ns.push(start.elapsed().as_nanos());
    }
    let observed = capture
        .batches
        .lock()
        .map_err(|_| anyhow!("capture lock poisoned"))?
        .clone();
    let mut offset = 0;
    for (decoded, _) in &observed {
        if decoded.schema() != batch.schema() {
            bail!("decoded pdata schema differs from prepared public DMV schema");
        }
        let expected = batch.slice(offset, decoded.num_rows());
        for column in 0..batch.num_columns() {
            if decoded.column(column).to_data() != expected.column(column).to_data() {
                bail!("decoded pdata column {column} differs from prepared public DMV input");
            }
        }
        offset += decoded.num_rows();
    }
    if offset != batch.num_rows() {
        bail!("decoded pdata rows {offset} differ from prepared {}", batch.num_rows());
    }
    let encode_ns = capture.encode_ns.load(Ordering::Relaxed);
    let decode_ns = capture.decode_ns.load(Ordering::Relaxed);
    let rows = u64::try_from(batch.num_rows())? * u64::try_from(samples_ns.len())?;
    println!(
        "flight_envelope_release {}",
        json!({
            "case": HISTORICAL_CASE,
            "build": "release",
            "calls": samples_ns.len(),
            "rows_per_call": batch.num_rows(),
            "rows_measured": rows,
            "flight_data_messages": messages.len(),
            "pdata_batches_per_call": observed.len(),
            "logical_bytes_per_call": logical,
            "arrow_array_memory_bytes_per_call": array_bytes,
            "ipc_header_plus_body_bytes_per_call": ipc_bytes,
            "flight_data_protobuf_bytes_per_call": protobuf_bytes,
            "ipc_compression": "none (FlightDataEncoderBuilder default)",
            "wall_ns_samples": samples_ns,
            "pdata_encode_ns_total": encode_ns,
            "pdata_decode_ns_total": decode_ns,
            "pdata_transfers_measured": observed.len() * samples_ns.len(),
            "per_call_wall_ns_mean": samples_ns.iter().sum::<u128>() / samples_ns.len() as u128,
        })
    );
    Ok(())
}
