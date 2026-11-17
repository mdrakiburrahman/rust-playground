use std::{
    collections::{BTreeSet, HashMap},
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow, bail};
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_flight::{
    FlightData, FlightDescriptor, PutResult,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_descriptor::DescriptorType,
    flight_service_client::FlightServiceClient,
    sql::{CommandStatementUpdate, DoPutUpdateResult, ProstMessageExt},
};
use arrow_schema::{DataType, Field, Schema};
use arrowcol_catalog::{
    CatalogControlPlane, CatalogError, CatalogField, CatalogResult, CatalogSchemaVersion, CatalogStore, CatalogType,
    ChangeReservation, DdlParser, DescriptorArtifacts, FlightSqlDdlHandler, MemoryCatalogStore, RequestHash,
    ReserveOutcome, SagaId, SagaRecord, SagaStep, SchemaValidator, SchemaVersion, TypeMapping, deterministic_field_id,
    deterministic_table_id, generate_descriptor_artifacts,
};
use arrowcol_flight::{
    ArrowColFlightService, AuthenticationRequest, Authenticator, BatchAdmission, DeliveryGuarantee, DrainController,
    PressureEvent, PressureHooks, PutResultEmitter, PutResultMetadata, RuntimeError, RuntimeLimits, RuntimeResult,
    StreamAbortReason, StreamCompletion, StreamContext, StreamLifecycle, StreamSummary, align_record_batch,
    decode_ddl_completion_metadata, decode_status_metadata,
};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, CatalogName, ClaimedSchemaFingerprint, ImplementationPool,
    IngestMetadata, NOT_ADMITTED_METADATA_KEY, PrincipalId, PrincipalIssuer, ProtocolVersion, SchemaName,
    TableIdentity, TableName, UnixMillis, fingerprint_schema,
};
use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use prost::Message;
use tokio::sync::{Mutex, Semaphore};
use tonic::{
    Code, Request, Status,
    metadata::{MetadataMap, MetadataValue},
};
use tonic::{
    codec::CompressionEncoding,
    transport::{Channel, Server},
};
use uuid::Uuid;

struct LoopbackFlight {
    client: FlightServiceClient<Channel>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl LoopbackFlight {
    async fn start(service: ArrowColFlightService) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let incoming = stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let task = tokio::spawn(
            Server::builder()
                .add_service(service.into_tonic_service())
                .serve_with_incoming(incoming),
        );
        let channel = Channel::from_shared(format!("http://{address}"))?
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .connect()
            .await?;
        Ok(Self {
            client: FlightServiceClient::new(channel),
            task,
        })
    }
}

impl Drop for LoopbackFlight {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn wire_ingest_request(
    schema: &CatalogSchemaVersion,
) -> Result<Request<impl Stream<Item = FlightData> + Send + 'static>> {
    let messages = encode_ingest(schema, vec![v1_batch(schema, 0)?]).await?;
    let mut request = Request::new(stream::iter(messages));
    insert_ingest_metadata(
        request.metadata_mut(),
        schema,
        ImplementationPool::Dataflow,
        schema.fingerprint(),
    )?;
    Ok(request)
}

#[tokio::test]
async fn loopback_compression_negotiates_requests_and_responses_independently() -> Result<()> {
    for (request_encoding, response_encoding, advertise_response) in [
        (None, None, false),
        (Some(CompressionEncoding::Gzip), Some(CompressionEncoding::Gzip), true),
        (Some(CompressionEncoding::Zstd), Some(CompressionEncoding::Zstd), true),
        (Some(CompressionEncoding::Gzip), Some(CompressionEncoding::Zstd), true),
        (None, Some(CompressionEncoding::Gzip), false),
    ] {
        let fixture = Fixture::with_current(false).await?;
        let admission = MockAdmission::new(TestCompletion::BestEffort, None);
        let limits = RuntimeLimits {
            grpc_accept_compression: request_encoding,
            grpc_send_compression: response_encoding,
            ..Default::default()
        };
        let service = fixture.service_with_limits(ImplementationPool::Dataflow, admission.clone(), limits)?;
        let server = LoopbackFlight::start(service).await?;
        let mut client = server.client.clone();
        if let Some(encoding) = request_encoding {
            client = client.send_compressed(encoding);
        }
        if advertise_response && let Some(encoding) = response_encoding {
            client = client.accept_compressed(encoding);
        }
        let response = client.do_put(wire_ingest_request(&fixture.v1).await?).await?;
        let expected = if advertise_response { response_encoding } else { None };
        assert_eq!(
            response
                .metadata()
                .get("grpc-encoding")
                .and_then(|value| value.to_str().ok()),
            expected.map(|encoding| match encoding {
                CompressionEncoding::Gzip => "gzip",
                CompressionEncoding::Zstd => "zstd",
                _ => "unsupported",
            }),
        );
        let results = response.into_inner().try_collect::<Vec<_>>().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(admission.begin_count(), 1);
        assert_eq!(admission.batches().await, vec![v1_batch(&fixture.v1, 0)?]);
    }
    Ok(())
}

#[tokio::test]
async fn loopback_compression_does_not_bypass_disabled_codec_or_decompressed_limits() -> Result<()> {
    for encoding in [CompressionEncoding::Gzip, CompressionEncoding::Zstd] {
        for limit in ["disabled", "schema", "message", "metadata"] {
            let fixture = Fixture::with_current(false).await?;
            let admission = MockAdmission::new(TestCompletion::BestEffort, None);
            let mut limits = RuntimeLimits::default();
            if limit != "disabled" {
                limits.grpc_accept_compression = Some(encoding);
            }
            match limit {
                "schema" => limits.max_schema_bytes = 16,
                "message" => {
                    limits.max_schema_bytes = 32;
                    limits.max_message_bytes = 32;
                }
                "metadata" => limits.max_metadata_bytes = 1,
                _ => {}
            }
            let service = fixture.service_with_limits(ImplementationPool::Dataflow, admission.clone(), limits)?;
            let server = LoopbackFlight::start(service).await?;
            let result = server
                .client
                .clone()
                .send_compressed(encoding)
                .do_put(wire_ingest_request(&fixture.v1).await?)
                .await;
            let error = match result {
                Err(error) => error,
                Ok(response) => response
                    .into_inner()
                    .try_collect::<Vec<_>>()
                    .await
                    .err()
                    .ok_or_else(|| anyhow!("compressed {limit} request unexpectedly succeeded"))?,
            };
            if limit == "disabled" {
                assert_eq!(error.code(), Code::Unimplemented);
            } else if limit == "message" {
                assert_eq!(error.code(), Code::InvalidArgument);
                assert_eq!(error.message(), "invalid Arrow Flight stream");
            } else {
                assert_eq!(error.code(), Code::ResourceExhausted, "{limit}: {error}");
            }
            assert_eq!(admission.begin_count(), 0, "{encoding:?}/{limit}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn loopback_compression_retains_admission_wait_and_identity_rejection() -> Result<()> {
    for encoding in [CompressionEncoding::Gzip, CompressionEncoding::Zstd] {
        let fixture = Fixture::with_current(false).await?;
        let gate = Arc::new(BatchGate::new());
        let admission = MockAdmission::new(TestCompletion::BestEffort, Some(Arc::clone(&gate)));
        let service = fixture.service_with_limits(
            ImplementationPool::Dataflow,
            admission.clone(),
            RuntimeLimits {
                grpc_accept_compression: Some(encoding),
                grpc_send_compression: Some(encoding),
                batch_channel_capacity: 1,
                ..Default::default()
            },
        )?;
        let server = LoopbackFlight::start(service).await?;
        let mut client = server
            .client
            .clone()
            .send_compressed(encoding)
            .accept_compressed(encoding);
        let mut forged = wire_ingest_request(&fixture.v1).await?;
        forged.metadata_mut().insert_bin(
            arrowcol_flight::INTERNAL_IDENTITY_METADATA_KEY,
            MetadataValue::from_bytes(b"forged"),
        );
        let error = client
            .do_put(forged)
            .await
            .err()
            .ok_or_else(|| anyhow!("forged identity accepted"))?;
        assert_eq!(error.code(), Code::Unauthenticated);
        assert_eq!(admission.begin_count(), 0);

        let response = client.do_put(wire_ingest_request(&fixture.v1).await?).await?;
        let mut pending = Box::pin(response.into_inner().try_collect::<Vec<_>>());
        gate.wait_entered().await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut pending)
                .await
                .is_err()
        );
        gate.release();
        assert_eq!(pending.await?.len(), 1);
        assert_eq!(admission.begin_count(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn exact_schema_multi_batch_returns_durable_admission_without_receipt() -> Result<()> {
    let fixture = Fixture::with_current(false).await?;
    let admission = MockAdmission::new(TestCompletion::DurableAdmission, None);
    let pressure = Arc::new(RecordingPressure::default());
    let pressure_hooks: Arc<dyn PressureHooks> = Arc::<RecordingPressure>::clone(&pressure);
    let service = fixture
        .service(ImplementationPool::Dataflow, admission.clone())?
        .with_pressure_hooks(pressure_hooks);
    let batches = vec![v1_batch(&fixture.v1, 0)?, v1_batch(&fixture.v1, 10)?];
    let request = ingest_request(
        &fixture.v1,
        batches,
        ImplementationPool::Dataflow,
        fixture.v1.fingerprint(),
    )
    .await?;

    let results = service
        .handle_do_put(request)
        .await?
        .into_inner()
        .try_collect::<Vec<_>>()
        .await?;

    assert_eq!(admission.begin_count(), 1);
    assert_eq!(admission.batches().await.len(), 2);
    assert_eq!(results.len(), 1);
    let metadata = PutResultMetadata::from_put_result(&results[0])?;
    assert_eq!(metadata.delivery_guarantee(), Some(DeliveryGuarantee::DurableAdmission));
    assert!(matches!(&metadata, PutResultMetadata::DurableAdmission { .. }));
    let json: serde_json::Value = serde_json::from_slice(&results[0].app_metadata)?;
    assert!(json.get("receipt_id").is_none());
    let events = pressure.events();
    assert!(events.contains(&PressureEvent::StreamStarted));
    assert!(events.iter().any(|event| matches!(
        event,
        PressureEvent::StreamFinished {
            outcome: arrowcol_flight::StreamOutcome::Completed,
            ..
        }
    )));
    Ok(())
}

#[tokio::test]
async fn registered_older_schema_is_null_filled_to_current() -> Result<()> {
    let fixture = Fixture::with_current(true).await?;
    let admission = MockAdmission::new(TestCompletion::BestEffort, None);
    let service = fixture.service(ImplementationPool::Dataflow, admission.clone())?;
    let request = ingest_request(
        &fixture.v1,
        vec![v1_batch(&fixture.v1, 0)?],
        ImplementationPool::Dataflow,
        fixture.v1.fingerprint(),
    )
    .await?;

    let results = service
        .handle_do_put(request)
        .await?
        .into_inner()
        .try_collect::<Vec<_>>()
        .await?;
    let batches = admission.batches().await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].schema().as_ref(), &fixture.v2.arrow_schema()?);
    assert_eq!(batches[0].num_columns(), 3);
    assert_eq!(batches[0].column(2).null_count(), batches[0].num_rows());
    let metadata = PutResultMetadata::from_put_result(&results[0])?;
    assert_eq!(metadata.delivery_guarantee(), Some(DeliveryGuarantee::BestEffort));
    assert!(matches!(
        metadata,
        PutResultMetadata::BestEffortAdmission {
            source_schema_version,
            target_schema_version,
            ..
        } if source_schema_version == SchemaVersion::FIRST
            && target_schema_version == SchemaVersion::new(2)?
    ));
    Ok(())
}

#[tokio::test]
async fn every_unregistered_breaking_mutation_is_rejected_before_admission() -> Result<()> {
    let fixture = Fixture::with_current(false).await?;
    let admission = MockAdmission::new(TestCompletion::BestEffort, None);
    let service = fixture.service(ImplementationPool::Dataflow, admission.clone())?;
    let schemas = breaking_schemas();

    for (name, schema) in schemas {
        let request = schema_only_request(
            &schema,
            SchemaVersion::FIRST,
            ImplementationPool::Dataflow,
            fingerprint_schema(&schema),
        )
        .await?;
        let status = service
            .handle_do_put(request)
            .await
            .err()
            .ok_or_else(|| anyhow!("{name} unexpectedly reached admission"))?;
        assert_eq!(status.code(), Code::InvalidArgument, "{name}");
        let metadata = decode_status_metadata(&status);
        assert_eq!(metadata.code.as_deref(), Some("schema_fingerprint_mismatch"), "{name}");
    }

    assert_eq!(admission.begin_count(), 0);
    Ok(())
}

#[tokio::test]
async fn known_historical_breaking_schema_is_rejected_before_admission() -> Result<()> {
    let identity = test_identity()?;
    let v1 = make_version(&identity, 1, base_fields(&identity)?, "v1")?;
    let mut fields = base_fields(&identity)?;
    fields[1] = field(
        deterministic_table_id(&identity),
        "body",
        1,
        2,
        false,
        CatalogType::Utf8,
    )?;
    let v2 = make_version(&identity, 2, fields, "breaking-nullability")?;
    let catalog = MockCatalogStore::new();
    install_schema(&catalog, None, &v1).await?;
    install_schema(&catalog, Some(v1.clone()), &v2).await?;
    let admission = MockAdmission::new(TestCompletion::BestEffort, None);
    let service = build_service(
        ImplementationPool::Dataflow,
        Arc::new(catalog),
        admission.clone(),
        RuntimeLimits::default(),
    )?;
    let request = ingest_request(
        &v1,
        vec![v1_batch(&v1, 0)?],
        ImplementationPool::Dataflow,
        v1.fingerprint(),
    )
    .await?;

    let status = service
        .handle_do_put(request)
        .await
        .err()
        .ok_or_else(|| anyhow!("breaking historical version was admitted"))?;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        decode_status_metadata(&status).code.as_deref(),
        Some("schema_incompatible")
    );
    assert_eq!(admission.begin_count(), 0);
    Ok(())
}

#[tokio::test]
async fn malformed_metadata_is_rejected_before_body_or_authentication() -> Result<()> {
    let fixture = Fixture::with_current(false).await?;
    let admission = MockAdmission::new(TestCompletion::BestEffort, None);
    let auth = Arc::new(MockAuthenticator::new(test_auth_context()?));
    let auth_provider: Arc<dyn Authenticator> = Arc::<MockAuthenticator>::clone(&auth);
    let service = ArrowColFlightService::new(
        ImplementationPool::Dataflow,
        Arc::new(fixture.catalog.clone()),
        auth_provider,
        Arc::new(admission.clone()),
        RuntimeLimits::default(),
    )?;
    let never_polled = stream::poll_fn(|_| -> Poll<Option<Result<FlightData, Status>>> {
        panic!("body must not be polled before metadata validation")
    });
    let mut request = Request::new(Box::pin(never_polled) as TestFlightStream);
    request
        .metadata_mut()
        .insert("x-arrowcol-protocol-version", MetadataValue::try_from("1")?);

    let status = service
        .handle_do_put(request)
        .await
        .err()
        .ok_or_else(|| anyhow!("malformed metadata was accepted"))?;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        decode_status_metadata(&status).code.as_deref(),
        Some("routing_metadata_invalid")
    );
    assert_eq!(auth.calls(), 0);
    assert_eq!(admission.begin_count(), 0);
    Ok(())
}

#[tokio::test]
async fn untrusted_claimed_fingerprint_is_rejected_before_admission() -> Result<()> {
    let fixture = Fixture::with_current(false).await?;
    let admission = MockAdmission::new(TestCompletion::BestEffort, None);
    let service = fixture.service(ImplementationPool::Dataflow, admission.clone())?;
    let wrong_schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let request = ingest_request(
        &fixture.v1,
        vec![v1_batch(&fixture.v1, 0)?],
        ImplementationPool::Dataflow,
        fingerprint_schema(&wrong_schema),
    )
    .await?;

    let status = service
        .handle_do_put(request)
        .await
        .err()
        .ok_or_else(|| anyhow!("untrusted fingerprint was accepted"))?;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        decode_status_metadata(&status).code.as_deref(),
        Some("schema_fingerprint_mismatch")
    );
    assert_eq!(admission.begin_count(), 0);
    Ok(())
}

#[tokio::test]
async fn unvalidated_internal_identity_is_rejected_before_body() -> Result<()> {
    let fixture = Fixture::with_current(false).await?;
    let admission = MockAdmission::new(TestCompletion::BestEffort, None);
    let auth = Arc::new(MockAuthenticator::new(test_auth_context()?));
    let auth_provider: Arc<dyn Authenticator> = Arc::<MockAuthenticator>::clone(&auth);
    let service = ArrowColFlightService::new(
        ImplementationPool::Dataflow,
        Arc::new(fixture.catalog.clone()),
        auth_provider,
        Arc::new(admission.clone()),
        RuntimeLimits::default(),
    )?;
    let never_polled = stream::poll_fn(|_| -> Poll<Option<Result<FlightData, Status>>> {
        panic!("body must not be polled before trusted identity validation")
    });
    let mut request = Request::new(Box::pin(never_polled) as TestFlightStream);
    insert_ingest_metadata(
        request.metadata_mut(),
        &fixture.v1,
        ImplementationPool::Dataflow,
        fixture.v1.fingerprint(),
    )?;
    request.metadata_mut().insert_bin(
        arrowcol_flight::INTERNAL_IDENTITY_METADATA_KEY,
        tonic::metadata::MetadataValue::from_bytes(b"untrusted"),
    );

    let status = service
        .handle_do_put(request)
        .await
        .err()
        .ok_or_else(|| anyhow!("unvalidated identity was accepted"))?;
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(auth.calls(), 0);
    assert_eq!(admission.begin_count(), 0);
    Ok(())
}

#[tokio::test]
async fn a_second_schema_message_aborts_the_admitted_stream() -> Result<()> {
    let fixture = Fixture::with_current(false).await?;
    let admission = MockAdmission::new(TestCompletion::BestEffort, None);
    let service = fixture.service(ImplementationPool::Dataflow, admission.clone())?;
    let mut messages = encode_ingest(&fixture.v1, Vec::new()).await?;
    let mut second = messages
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("schema encoder returned no message"))?;
    second.flight_descriptor = None;
    messages.push(second);
    let mut request = Request::new(Box::pin(stream::iter(messages.into_iter().map(Ok))) as TestFlightStream);
    insert_ingest_metadata(
        request.metadata_mut(),
        &fixture.v1,
        ImplementationPool::Dataflow,
        fixture.v1.fingerprint(),
    )?;

    let status = service
        .handle_do_put(request)
        .await?
        .into_inner()
        .next()
        .await
        .ok_or_else(|| anyhow!("second schema did not produce a status"))?
        .err()
        .ok_or_else(|| anyhow!("second schema unexpectedly completed"))?;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(admission.begin_count(), 1);
    assert_eq!(admission.wait_aborted().await?, StreamAbortReason::DecodeFailure);
    Ok(())
}

#[tokio::test]
async fn force_stop_before_first_batch_marks_not_admitted() -> Result<()> {
    let fixture = Fixture::with_current(false).await?;
    let admission = MockAdmission::new(TestCompletion::BestEffort, None);
    let drain = DrainController::new();
    let service = fixture
        .service(ImplementationPool::Dataflow, admission.clone())?
        .with_drain_controller(drain.clone());
    let messages = encode_ingest(&fixture.v1, Vec::new()).await?;
    let input = stream::iter(messages.into_iter().map(Ok)).chain(stream::pending());
    let mut request = Request::new(Box::pin(input) as TestFlightStream);
    insert_ingest_metadata(
        request.metadata_mut(),
        &fixture.v1,
        ImplementationPool::Dataflow,
        fixture.v1.fingerprint(),
    )?;
    let response = service.handle_do_put(request).await?;
    drain.force_stop();
    let result = response.into_inner().try_collect::<Vec<PutResult>>().await;
    let Err(status) = result else {
        return Err(anyhow::anyhow!("force stop returned success"));
    };
    assert_eq!(
        status
            .metadata()
            .get(NOT_ADMITTED_METADATA_KEY)
            .and_then(|value| value.to_str().ok()),
        Some("true")
    );
    assert!(admission.batches().await.is_empty(), "no batch may have been pushed");
    Ok(())
}
#[tokio::test]
async fn force_stop_after_pushed_batch_is_not_retryable() -> Result<()> {
    let fixture = Fixture::with_current(false).await?;
    let gate = Arc::new(BatchGate::new());
    let admission = MockAdmission::new_gated_at(TestCompletion::BestEffort, Some(Arc::clone(&gate)), 2);
    let drain = DrainController::new();
    let service = fixture
        .service(ImplementationPool::Dataflow, admission.clone())?
        .with_drain_controller(drain.clone());
    let batches = vec![v1_batch(&fixture.v1, 0)?, v1_batch(&fixture.v1, 10)?];
    let request = ingest_request(
        &fixture.v1,
        batches,
        ImplementationPool::Dataflow,
        fixture.v1.fingerprint(),
    )
    .await?;
    let response = service.handle_do_put(request).await?;
    let task = tokio::spawn(async move { response.into_inner().try_collect::<Vec<PutResult>>().await });
    gate.wait_entered().await?;
    assert_eq!(
        admission.batches().await.len(),
        1,
        "first batch must be pushed before shutdown"
    );
    drain.force_stop();
    let result = task.await.context("join force-stopped response")?;
    let Err(status) = result else {
        return Err(anyhow::anyhow!("force stop returned success"));
    };
    assert!(
        status.metadata().get(NOT_ADMITTED_METADATA_KEY).is_none(),
        "post-admission shutdown must not be retried"
    );
    assert_eq!(admission.batches().await.len(), 1);
    Ok(())
}

#[tokio::test]
async fn bounded_channel_applies_backpressure_to_the_input_stream() -> Result<()> {
    let fixture = Fixture::with_current(false).await?;
    let gate = Arc::new(BatchGate::new());
    let admission = MockAdmission::new(TestCompletion::BestEffort, Some(Arc::clone(&gate)));
    let limits = RuntimeLimits {
        batch_channel_capacity: 1,
        ..RuntimeLimits::default()
    };
    let service = fixture.service_with_limits(ImplementationPool::Dataflow, admission, limits)?;
    let batches = (0..6)
        .map(|index| v1_batch(&fixture.v1, index * 10))
        .collect::<Result<Vec<_>>>()?;
    let messages = encode_ingest(&fixture.v1, batches).await?;
    let total_messages = messages.len();
    let polls = Arc::new(AtomicUsize::new(0));
    let counted = CountingStream {
        inner: stream::iter(messages.into_iter().map(Ok)),
        polls: Arc::clone(&polls),
    };
    let mut request = Request::new(counted);
    insert_ingest_metadata(
        request.metadata_mut(),
        &fixture.v1,
        ImplementationPool::Dataflow,
        fixture.v1.fingerprint(),
    )?;
    let response = service.handle_do_put(request).await?;
    gate.wait_entered().await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        polls.load(Ordering::Acquire) < total_messages,
        "bounded decoder drained all {total_messages} messages while admission was blocked"
    );
    gate.release();
    let results = response.into_inner().try_collect::<Vec<_>>().await?;
    assert_eq!(results.len(), 1);
    Ok(())
}

#[tokio::test]
async fn dropping_the_response_cancels_and_aborts_admission() -> Result<()> {
    let fixture = Fixture::with_current(false).await?;
    let gate = Arc::new(BatchGate::new());
    let admission = MockAdmission::new(TestCompletion::BestEffort, Some(Arc::clone(&gate)));
    let service = fixture.service(ImplementationPool::Dataflow, admission.clone())?;
    let request = ingest_request(
        &fixture.v1,
        vec![v1_batch(&fixture.v1, 0)?],
        ImplementationPool::Dataflow,
        fixture.v1.fingerprint(),
    )
    .await?;
    let response = service.handle_do_put(request).await?;
    gate.wait_entered().await?;
    drop(response);
    let reason = admission.wait_aborted().await?;
    assert_eq!(reason, StreamAbortReason::ClientCancelled);
    Ok(())
}

#[tokio::test]
async fn grpc_deadline_aborts_a_blocked_admission() -> Result<()> {
    let fixture = Fixture::with_current(false).await?;
    let gate = Arc::new(BatchGate::new());
    let admission = MockAdmission::new(TestCompletion::BestEffort, Some(Arc::clone(&gate)));
    let service = fixture.service(ImplementationPool::Dataflow, admission.clone())?;
    let mut request = ingest_request(
        &fixture.v1,
        vec![v1_batch(&fixture.v1, 0)?],
        ImplementationPool::Dataflow,
        fixture.v1.fingerprint(),
    )
    .await?;
    request
        .metadata_mut()
        .insert("grpc-timeout", MetadataValue::try_from("50m")?);
    let mut response = service.handle_do_put(request).await?.into_inner();
    gate.wait_entered().await?;
    let status = response
        .next()
        .await
        .ok_or_else(|| anyhow!("deadline did not produce a status"))?
        .err()
        .ok_or_else(|| anyhow!("deadline unexpectedly produced a PutResult"))?;
    assert_eq!(status.code(), Code::DeadlineExceeded);
    assert_eq!(admission.wait_aborted().await?, StreamAbortReason::DeadlineExceeded);
    Ok(())
}

#[tokio::test]
async fn schema_size_limit_rejects_before_admission() -> Result<()> {
    let fixture = Fixture::with_current(false).await?;
    let admission = MockAdmission::new(TestCompletion::BestEffort, None);
    let limits = RuntimeLimits {
        max_schema_bytes: 16,
        ..RuntimeLimits::default()
    };
    let service = fixture.service_with_limits(ImplementationPool::Dataflow, admission.clone(), limits)?;
    let request = schema_only_request(
        &fixture.v1.arrow_schema()?,
        fixture.v1.version(),
        ImplementationPool::Dataflow,
        fixture.v1.fingerprint(),
    )
    .await?;

    let status = service
        .handle_do_put(request)
        .await
        .err()
        .ok_or_else(|| anyhow!("oversized schema was accepted"))?;
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(admission.begin_count(), 0);
    Ok(())
}

#[tokio::test]
async fn durable_and_best_effort_dataflow_completions_are_distinct() -> Result<()> {
    let durable_fixture = Fixture::with_current(false).await?;
    let durable_admission = MockAdmission::new(TestCompletion::DurableAdmission, None);
    let durable = durable_fixture.service(ImplementationPool::Dataflow, durable_admission)?;
    let durable_request = ingest_request(
        &durable_fixture.v1,
        vec![v1_batch(&durable_fixture.v1, 0)?],
        ImplementationPool::Dataflow,
        durable_fixture.v1.fingerprint(),
    )
    .await?;
    let durable_result = one_result(durable.handle_do_put(durable_request).await?).await?;
    assert!(matches!(
        PutResultMetadata::from_put_result(&durable_result)?,
        PutResultMetadata::DurableAdmission {
            delivery_guarantee: DeliveryGuarantee::DurableAdmission,
            ..
        }
    ));

    let best_effort_fixture = Fixture::with_current(false).await?;
    let best_effort_admission = MockAdmission::new(TestCompletion::BestEffort, None);
    let best_effort = best_effort_fixture.service(ImplementationPool::Dataflow, best_effort_admission)?;
    let best_effort_request = ingest_request(
        &best_effort_fixture.v1,
        vec![v1_batch(&best_effort_fixture.v1, 0)?],
        ImplementationPool::Dataflow,
        best_effort_fixture.v1.fingerprint(),
    )
    .await?;
    let best_effort_result = one_result(best_effort.handle_do_put(best_effort_request).await?).await?;
    assert!(matches!(
        PutResultMetadata::from_put_result(&best_effort_result)?,
        PutResultMetadata::BestEffortAdmission {
            delivery_guarantee: DeliveryGuarantee::BestEffort,
            ..
        }
    ));
    Ok(())
}

#[tokio::test]
async fn flight_sql_ddl_returns_standard_and_structured_completion() -> Result<()> {
    let catalog = MockCatalogStore::new();
    let (control_plane, report) = CatalogControlPlane::start(DdlParser::default(), Arc::new(catalog.clone())).await?;
    assert!(report.failures.is_empty());
    let admission = MockAdmission::new(TestCompletion::BestEffort, None);
    let service = build_service(
        ImplementationPool::Dataflow,
        Arc::new(catalog),
        admission.clone(),
        RuntimeLimits::default(),
    )?
    .with_ddl_handler(Arc::new(control_plane) as Arc<dyn FlightSqlDdlHandler>);
    let command = CommandStatementUpdate {
        query: "CREATE TABLE telemetry.prod.events (id BIGINT NOT NULL, body STRING) USING DELTA".to_owned(),
        transaction_id: None,
    };
    let descriptor = FlightDescriptor {
        r#type: DescriptorType::Cmd as i32,
        cmd: command.as_any().encode_to_vec().into(),
        path: Vec::new(),
    };
    let request = Request::new(stream::iter(vec![Ok(FlightData {
        flight_descriptor: Some(descriptor),
        ..FlightData::default()
    })]));

    let response = service.handle_do_put(request).await?;
    let completion = decode_ddl_completion_metadata(response.metadata())?;
    assert_eq!(completion.catalog(), "telemetry");
    assert_eq!(completion.schema(), "prod");
    assert_eq!(completion.table(), "events");
    assert_eq!(completion.schema_version(), SchemaVersion::FIRST);
    assert_eq!(completion.disposition(), "created");
    let result = one_result(response).await?;
    let update = DoPutUpdateResult::decode(result.app_metadata)?;
    assert_eq!(update.record_count, 0);
    assert_eq!(admission.begin_count(), 0);
    Ok(())
}

#[tokio::test]
async fn routed_flight_sql_ddl_checks_identity_before_catalog_mutation() -> Result<()> {
    let catalog = MockCatalogStore::new();
    let (control_plane, report) = CatalogControlPlane::start(DdlParser::default(), Arc::new(catalog.clone())).await?;
    assert!(report.failures.is_empty());
    let admission = MockAdmission::new(TestCompletion::BestEffort, None);
    let service = build_service(
        ImplementationPool::Dataflow,
        Arc::new(catalog),
        admission.clone(),
        RuntimeLimits::default(),
    )?
    .with_ddl_handler(Arc::new(control_plane) as Arc<dyn FlightSqlDdlHandler>);
    let ddl = "CREATE TABLE telemetry.prod.events (id BIGINT NOT NULL, body STRING) USING DELTA";
    let identity = DdlParser::default().parse(ddl)?.identity().clone();
    let other = DdlParser::default()
        .parse("CREATE TABLE telemetry.prod.other (id BIGINT NOT NULL) USING DELTA")?
        .identity()
        .clone();
    for (target, valid) in [(other, false), (identity, true)] {
        let command = CommandStatementUpdate {
            query: ddl.to_owned(),
            transaction_id: None,
        };
        let mut request = Request::new(stream::iter(vec![Ok(FlightData {
            flight_descriptor: Some(FlightDescriptor {
                r#type: DescriptorType::Cmd as i32,
                cmd: command.as_any().encode_to_vec().into(),
                path: Vec::new(),
            }),
            ..FlightData::default()
        })]));
        insert_metadata_pairs(
            request.metadata_mut(),
            &IngestMetadata::new(
                ProtocolVersion::CURRENT,
                ImplementationPool::Dataflow,
                target,
                ClaimedSchemaFingerprint::new(fingerprint_schema(&Schema::empty())),
            ),
        )?;
        let result = service.handle_do_put(request).await;
        if valid {
            let response = result?;
            let completion = decode_ddl_completion_metadata(response.metadata())?;
            assert_eq!(completion.disposition(), "created");
            assert_eq!(completion.table(), "events");
            assert_eq!(
                DoPutUpdateResult::decode(one_result(response).await?.app_metadata)?.record_count,
                0
            );
        } else {
            let status = result.err().context("mismatched DDL identity must fail")?;
            assert_eq!(status.code(), Code::InvalidArgument);
            assert!(status.message().contains("target does not match"));
        }
    }
    assert_eq!(admission.begin_count(), 0);
    Ok(())
}

#[tokio::test]
async fn null_fill_alignment_reuses_every_existing_array_allocation() -> Result<()> {
    let fixture = Fixture::with_current(true).await?;
    let validator = SchemaValidator::new(Arc::new(fixture.catalog.clone()));
    let source_schema = fixture.v1.arrow_schema()?;
    let validated = validator
        .validate(
            fixture.v1.identity(),
            ClaimedSchemaFingerprint::new(fixture.v1.fingerprint()),
            &source_schema,
        )
        .await?;
    let batch = v1_batch(&fixture.v1, 0)?;
    let first = Arc::clone(batch.column(0));
    let second = Arc::clone(batch.column(1));
    let aligned = align_record_batch(batch, validated.alignment(), Arc::new(fixture.v2.arrow_schema()?))?;

    assert!(Arc::ptr_eq(&first, aligned.column(0)));
    assert!(Arc::ptr_eq(&second, aligned.column(1)));
    assert_eq!(aligned.column(2).null_count(), aligned.num_rows());
    Ok(())
}

type TestFlightStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

#[derive(Clone)]
struct Fixture {
    catalog: MockCatalogStore,
    v1: CatalogSchemaVersion,
    v2: CatalogSchemaVersion,
}

impl Fixture {
    async fn with_current(include_v2: bool) -> Result<Self> {
        let identity = test_identity()?;
        let v1 = make_version(&identity, 1, base_fields(&identity)?, "v1")?;
        let mut v2_fields = v1.fields().to_vec();
        v2_fields.push(field(v1.table_id(), "severity", 2, 3, true, CatalogType::Int32)?);
        let v2 = make_version(&identity, 2, v2_fields, "v2")?;
        let catalog = MockCatalogStore::new();
        install_schema(&catalog, None, &v1).await?;
        if include_v2 {
            install_schema(&catalog, Some(v1.clone()), &v2).await?;
        }
        Ok(Self { catalog, v1, v2 })
    }

    fn service(&self, pool: ImplementationPool, admission: MockAdmission) -> Result<ArrowColFlightService> {
        self.service_with_limits(pool, admission, RuntimeLimits::default())
    }

    fn service_with_limits(
        &self,
        pool: ImplementationPool,
        admission: MockAdmission,
        limits: RuntimeLimits,
    ) -> Result<ArrowColFlightService> {
        build_service(pool, Arc::new(self.catalog.clone()), admission, limits)
    }
}

fn build_service(
    pool: ImplementationPool,
    catalog: Arc<dyn CatalogStore>,
    admission: MockAdmission,
    limits: RuntimeLimits,
) -> Result<ArrowColFlightService> {
    Ok(ArrowColFlightService::new(
        pool,
        catalog,
        Arc::new(MockAuthenticator::new(test_auth_context()?)),
        Arc::new(admission),
        limits,
    )?)
}

#[derive(Clone)]
struct MockAuthenticator {
    context: AuthContext,
    calls: Arc<AtomicUsize>,
}

impl MockAuthenticator {
    fn new(context: AuthContext) -> Self {
        Self {
            context,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }
}

#[async_trait]
impl Authenticator for MockAuthenticator {
    async fn authenticate(&self, _request: AuthenticationRequest) -> RuntimeResult<AuthContext> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(self.context.clone())
    }
}

#[derive(Clone, Copy)]
enum TestCompletion {
    DurableAdmission,
    BestEffort,
}

struct MockAdmissionState {
    begin_count: AtomicUsize,
    push_count: AtomicUsize,
    gate_at_batch: usize,
    batches: Mutex<Vec<RecordBatch>>,
    aborted: Mutex<Vec<StreamAbortReason>>,
    aborted_signal: Semaphore,
    completion: TestCompletion,
    gate: Option<Arc<BatchGate>>,
}

#[derive(Clone)]
struct MockAdmission {
    state: Arc<MockAdmissionState>,
}

impl MockAdmission {
    fn new(completion: TestCompletion, gate: Option<Arc<BatchGate>>) -> Self {
        Self::new_gated_at(completion, gate, 1)
    }

    fn new_gated_at(completion: TestCompletion, gate: Option<Arc<BatchGate>>, gate_at_batch: usize) -> Self {
        Self {
            state: Arc::new(MockAdmissionState {
                begin_count: AtomicUsize::new(0),
                push_count: AtomicUsize::new(0),
                gate_at_batch,
                batches: Mutex::new(Vec::new()),
                aborted: Mutex::new(Vec::new()),
                aborted_signal: Semaphore::new(0),
                completion,
                gate,
            }),
        }
    }

    fn begin_count(&self) -> usize {
        self.state.begin_count.load(Ordering::Acquire)
    }

    async fn batches(&self) -> Vec<RecordBatch> {
        self.state.batches.lock().await.clone()
    }

    async fn wait_aborted(&self) -> Result<StreamAbortReason> {
        let permit = tokio::time::timeout(Duration::from_secs(2), self.state.aborted_signal.acquire())
            .await
            .context("timed out waiting for abort hook")?
            .map_err(|_| anyhow!("abort semaphore closed"))?;
        permit.forget();
        self.state
            .aborted
            .lock()
            .await
            .last()
            .copied()
            .ok_or_else(|| anyhow!("abort signal had no reason"))
    }
}

#[async_trait]
impl BatchAdmission for MockAdmission {
    async fn begin(
        &self,
        _context: StreamContext,
        _emitter: Arc<dyn PutResultEmitter>,
    ) -> RuntimeResult<Box<dyn StreamLifecycle>> {
        self.state.begin_count.fetch_add(1, Ordering::AcqRel);
        Ok(Box::new(MockLifecycle {
            state: Arc::clone(&self.state),
        }))
    }
}

struct MockLifecycle {
    state: Arc<MockAdmissionState>,
}

#[async_trait]
impl StreamLifecycle for MockLifecycle {
    async fn push_batch(&mut self, batch: RecordBatch) -> RuntimeResult<()> {
        let push_number = self.state.push_count.fetch_add(1, Ordering::AcqRel) + 1;
        if push_number == self.state.gate_at_batch
            && let Some(gate) = &self.state.gate
        {
            gate.entered.add_permits(1);
            let permit = gate.release.acquire().await.map_err(|_| RuntimeError::internal())?;
            permit.forget();
        }
        self.state.batches.lock().await.push(batch);
        Ok(())
    }

    async fn complete(&mut self, _summary: StreamSummary) -> RuntimeResult<StreamCompletion> {
        Ok(match self.state.completion {
            TestCompletion::DurableAdmission => StreamCompletion::durable_admission(),
            TestCompletion::BestEffort => StreamCompletion::best_effort(),
        })
    }

    async fn abort(self: Box<Self>, reason: StreamAbortReason) {
        self.state.aborted.lock().await.push(reason);
        self.state.aborted_signal.add_permits(1);
    }
}

struct BatchGate {
    entered: Semaphore,
    release: Semaphore,
}

impl BatchGate {
    fn new() -> Self {
        Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }

    async fn wait_entered(&self) -> Result<()> {
        let permit = tokio::time::timeout(Duration::from_secs(2), self.entered.acquire())
            .await
            .context("timed out waiting for blocked admission")?
            .map_err(|_| anyhow!("entry semaphore closed"))?;
        permit.forget();
        Ok(())
    }

    fn release(&self) {
        self.release.add_permits(1);
    }
}

struct CountingStream<S> {
    inner: S,
    polls: Arc<AtomicUsize>,
}

impl<S> Stream for CountingStream<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.polls.fetch_add(1, Ordering::AcqRel);
        Pin::new(&mut this.inner).poll_next(context)
    }
}

#[derive(Default)]
struct RecordingPressure {
    events: StdMutex<Vec<PressureEvent>>,
}

impl RecordingPressure {
    fn events(&self) -> Vec<PressureEvent> {
        match self.events.lock() {
            Ok(events) => events.clone(),
            Err(_) => Vec::new(),
        }
    }
}

impl PressureHooks for RecordingPressure {
    fn observe(&self, event: PressureEvent) {
        if let Ok(mut events) = self.events.lock() {
            events.push(event);
        }
    }
}

#[derive(Clone)]
struct MockCatalogStore {
    inner: MemoryCatalogStore,
    fingerprint_lookups: Arc<AtomicUsize>,
}

impl MockCatalogStore {
    fn new() -> Self {
        Self {
            inner: MemoryCatalogStore::new(),
            fingerprint_lookups: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl CatalogStore for MockCatalogStore {
    async fn operation(
        &self,
        identity: &TableIdentity,
        request_hash: RequestHash,
    ) -> CatalogResult<Option<SagaRecord>> {
        self.inner.operation(identity, request_hash).await
    }

    async fn current_schema(&self, identity: &TableIdentity) -> CatalogResult<Option<CatalogSchemaVersion>> {
        self.inner.current_schema(identity).await
    }

    async fn schema_by_fingerprint(
        &self,
        identity: &TableIdentity,
        fingerprint: arrowcol_protocol::SchemaFingerprint,
    ) -> CatalogResult<Option<CatalogSchemaVersion>> {
        self.fingerprint_lookups.fetch_add(1, Ordering::AcqRel);
        self.inner.schema_by_fingerprint(identity, fingerprint).await
    }

    async fn schema_history(&self, identity: &TableIdentity) -> CatalogResult<Vec<CatalogSchemaVersion>> {
        self.inner.schema_history(identity).await
    }

    async fn reserve_change(&self, reservation: ChangeReservation) -> CatalogResult<ReserveOutcome> {
        self.inner.reserve_change(reservation).await
    }

    async fn saga(&self, saga_id: SagaId) -> CatalogResult<Option<SagaRecord>> {
        self.inner.saga(saga_id).await
    }

    async fn persist_artifacts(&self, saga_id: SagaId, artifacts: &DescriptorArtifacts) -> CatalogResult<SagaRecord> {
        self.inner.persist_artifacts(saga_id, artifacts).await
    }

    async fn finalize(&self, saga_id: SagaId) -> CatalogResult<SagaRecord> {
        self.inner.finalize(saga_id).await
    }

    async fn record_failure(&self, saga_id: SagaId, step: SagaStep, error: &CatalogError) -> CatalogResult<SagaRecord> {
        self.inner.record_failure(saga_id, step, error).await
    }

    async fn incomplete_sagas(&self) -> CatalogResult<Vec<SagaRecord>> {
        self.inner.incomplete_sagas().await
    }

    async fn artifacts(
        &self,
        identity: &TableIdentity,
        version: SchemaVersion,
    ) -> CatalogResult<Option<DescriptorArtifacts>> {
        self.inner.artifacts(identity, version).await
    }
}

async fn install_schema(
    store: &MockCatalogStore,
    from_schema: Option<CatalogSchemaVersion>,
    target_schema: &CatalogSchemaVersion,
) -> Result<()> {
    let reservation = ChangeReservation {
        request_hash: RequestHash::from_canonical_ddl(target_schema.canonical_ddl()),
        from_schema,
        target_schema: target_schema.clone(),
    };
    let saga = match store.reserve_change(reservation).await? {
        ReserveOutcome::Reserved(saga) | ReserveOutcome::Existing(saga) => saga,
        ReserveOutcome::AlreadyCurrent(_) => return Ok(()),
    };
    let artifacts = generate_descriptor_artifacts(target_schema)?;
    store.persist_artifacts(saga.id(), &artifacts).await?;
    store.finalize(saga.id()).await?;
    Ok(())
}

fn test_identity() -> Result<TableIdentity> {
    Ok(TableIdentity::new(
        CatalogName::new("telemetry")?,
        SchemaName::new("prod")?,
        TableName::new("events")?,
    ))
}

fn base_fields(identity: &TableIdentity) -> Result<Vec<CatalogField>> {
    let table_id = deterministic_table_id(identity);
    Ok(vec![
        field(table_id, "id", 0, 1, false, CatalogType::Int64)?,
        field(table_id, "body", 1, 2, true, CatalogType::Utf8)?,
    ])
}

fn field(
    table_id: Uuid,
    name: &str,
    position: u32,
    number: u32,
    nullable: bool,
    data_type: CatalogType,
) -> Result<CatalogField> {
    CatalogField::new(
        deterministic_field_id(table_id, &arrowcol_catalog::FieldPath::root(name)),
        name,
        position,
        number,
        nullable,
        None,
        TypeMapping::new(data_type),
    )
    .map_err(Into::into)
}

fn make_version(
    identity: &TableIdentity,
    version: u64,
    fields: Vec<CatalogField>,
    ddl: &str,
) -> Result<CatalogSchemaVersion> {
    CatalogSchemaVersion::new(
        deterministic_table_id(identity),
        identity.clone(),
        SchemaVersion::new(version)?,
        fields,
        ddl,
    )
    .map_err(Into::into)
}

fn v1_batch(schema: &CatalogSchemaVersion, offset: i64) -> Result<RecordBatch> {
    let ids = Int64Array::from(vec![offset + 1, offset + 2]);
    let bodies = StringArray::from(vec![Some("one"), None]);
    Ok(RecordBatch::try_new(
        Arc::new(schema.arrow_schema()?),
        vec![Arc::new(ids) as ArrayRef, Arc::new(bodies) as ArrayRef],
    )?)
}

async fn ingest_request(
    schema: &CatalogSchemaVersion,
    batches: Vec<RecordBatch>,
    pool: ImplementationPool,
    claimed: arrowcol_protocol::SchemaFingerprint,
) -> Result<Request<TestFlightStream>> {
    let messages = encode_ingest(schema, batches).await?;
    let mut request = Request::new(Box::pin(stream::iter(messages.into_iter().map(Ok))) as TestFlightStream);
    insert_ingest_metadata(request.metadata_mut(), schema, pool, claimed)?;
    Ok(request)
}

async fn schema_only_request(
    schema: &Schema,
    version: SchemaVersion,
    pool: ImplementationPool,
    claimed: arrowcol_protocol::SchemaFingerprint,
) -> Result<Request<TestFlightStream>> {
    let identity = test_identity()?;
    let descriptor = descriptor(&identity, version);
    let messages = FlightDataEncoderBuilder::new()
        .with_schema(Arc::new(schema.clone()))
        .with_flight_descriptor(Some(descriptor))
        .build(stream::iter(Vec::<Result<RecordBatch, FlightError>>::new()))
        .try_collect::<Vec<_>>()
        .await?;
    let mut request = Request::new(Box::pin(stream::iter(messages.into_iter().map(Ok))) as TestFlightStream);
    let metadata = IngestMetadata::new(
        ProtocolVersion::CURRENT,
        pool,
        identity,
        ClaimedSchemaFingerprint::new(claimed),
    );
    insert_metadata_pairs(request.metadata_mut(), &metadata)?;
    Ok(request)
}

async fn encode_ingest(schema: &CatalogSchemaVersion, batches: Vec<RecordBatch>) -> Result<Vec<FlightData>> {
    let input = batches.into_iter().map(Ok::<RecordBatch, FlightError>);
    Ok(FlightDataEncoderBuilder::new()
        .with_schema(Arc::new(schema.arrow_schema()?))
        .with_flight_descriptor(Some(descriptor(schema.identity(), schema.version())))
        .build(stream::iter(input))
        .try_collect::<Vec<_>>()
        .await?)
}

fn descriptor(identity: &TableIdentity, version: SchemaVersion) -> FlightDescriptor {
    FlightDescriptor {
        r#type: DescriptorType::Path as i32,
        cmd: Default::default(),
        path: vec![
            identity.catalog().to_string(),
            identity.schema().to_string(),
            identity.table().to_string(),
            format!("v{version}"),
        ],
    }
}

fn insert_ingest_metadata(
    map: &mut MetadataMap,
    schema: &CatalogSchemaVersion,
    pool: ImplementationPool,
    claimed: arrowcol_protocol::SchemaFingerprint,
) -> Result<()> {
    let metadata = IngestMetadata::new(
        ProtocolVersion::CURRENT,
        pool,
        schema.identity().clone(),
        ClaimedSchemaFingerprint::new(claimed),
    );
    insert_metadata_pairs(map, &metadata)
}

fn insert_metadata_pairs(map: &mut MetadataMap, metadata: &IngestMetadata) -> Result<()> {
    for (key, value) in metadata.to_ascii_pairs() {
        map.insert(key, MetadataValue::try_from(value.as_str())?);
    }
    Ok(())
}

fn breaking_schemas() -> Vec<(&'static str, Schema)> {
    let id = Field::new("id", DataType::Int64, false);
    let body = Field::new("body", DataType::Utf8, true);
    vec![
        (
            "rename",
            Schema::new(vec![Field::new("event_id", DataType::Int64, false), body.clone()]),
        ),
        (
            "type_change",
            Schema::new(vec![Field::new("id", DataType::Int32, false), body.clone()]),
        ),
        (
            "nullability_change",
            Schema::new(vec![Field::new("id", DataType::Int64, true), body.clone()]),
        ),
        ("reorder", Schema::new(vec![body.clone(), id.clone()])),
        ("drop", Schema::new(vec![id.clone()])),
        (
            "middle_insert",
            Schema::new(vec![
                id.clone(),
                Field::new("extra", DataType::Int32, true),
                body.clone(),
            ]),
        ),
        (
            "non_nullable_append",
            Schema::new(vec![
                id.clone(),
                body.clone(),
                Field::new("extra", DataType::Int32, false),
            ]),
        ),
        (
            "unregistered_nullable_append",
            Schema::new(vec![
                id.clone(),
                body.clone(),
                Field::new("extra", DataType::Int32, true),
            ]),
        ),
        (
            "field_metadata_change",
            Schema::new(vec![
                id.with_metadata(HashMap::from([("logical".to_owned(), "changed".to_owned())])),
                body,
            ]),
        ),
    ]
}

fn test_auth_context() -> Result<AuthContext> {
    let principal = AuthenticatedPrincipal::new(
        PrincipalId::new("test-subject")?,
        PrincipalIssuer::new("test-issuer")?,
        BTreeSet::new(),
    )?;
    Ok(AuthContext::new(
        principal,
        AuthMethod::WorkloadIdentity,
        UnixMillis::new(1)?,
        None,
    )?)
}

async fn one_result(response: tonic::Response<arrowcol_flight::FlightResponseStream<PutResult>>) -> Result<PutResult> {
    let results = response.into_inner().try_collect::<Vec<_>>().await?;
    if results.len() != 1 {
        bail!("expected one PutResult, received {}", results.len());
    }
    results.into_iter().next().ok_or_else(|| anyhow!("missing PutResult"))
}
