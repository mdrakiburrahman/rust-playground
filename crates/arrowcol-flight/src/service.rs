use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PollInfo, PutResult, SchemaResult, Ticket,
    decode::{DecodedPayload, FlightDataDecoder},
    error::FlightError,
    flight_descriptor::DescriptorType,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{Any, Command, CommandStatementUpdate},
};
use arrow_ipc::MessageHeader;
use arrow_schema::{Schema, SchemaRef};
use arrowcol_catalog::{CatalogStore, DdlParser, FlightSqlDdlHandler, SchemaValidator, SchemaVersion};
use arrowcol_protocol::{
    CATALOG_METADATA_KEY, CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY, IMPLEMENTATION_POOL_METADATA_KEY,
    ImplementationPool, IngestMetadata, PROTOCOL_VERSION_METADATA_KEY, RawGrpcMetadata, SCHEMA_METADATA_KEY,
    TABLE_METADATA_KEY, fingerprint_schema,
};
use async_trait::async_trait;
use futures::{Stream, StreamExt, stream};
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{
    Code, Request, Response, Status,
    metadata::{KeyAndValueRef, MetadataMap},
};

use crate::{
    AuthenticationRequest, Authenticator, BatchAdmission, DrainController, FlightSqlDdlAdapter, NoopPressureHooks,
    PressureEvent, PressureHooks, PutResultEmitter, PutResultMetadata, RejectUntrustedIdentityClaims, RuntimeError,
    RuntimeLimits, RuntimeResult, StreamAbortReason, StreamContext, StreamLifecycle, StreamOutcome, StreamSummary,
    TrustedIdentityValidator, align_record_batch, lifecycle::DrainPermit,
};

/// Boxed tonic response stream used by the reusable Flight service.
pub type FlightResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

type InboundFlightStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

/// Shared Arrow Flight service for direct and dataflow collector binaries.
#[derive(Clone)]
pub struct ArrowColFlightService {
    implementation_pool: ImplementationPool,
    schema_validator: SchemaValidator,
    authenticator: Arc<dyn Authenticator>,
    trusted_identity_validator: Arc<dyn TrustedIdentityValidator>,
    admission: Arc<dyn BatchAdmission>,
    ddl: Option<FlightSqlDdlAdapter>,
    pressure: Arc<dyn PressureHooks>,
    drain: DrainController,
    limits: RuntimeLimits,
}

impl ArrowColFlightService {
    /// Creates a runtime for one implementation pool.
    ///
    /// # Errors
    ///
    /// Returns an internal configuration error when limits are invalid.
    pub fn new(
        implementation_pool: ImplementationPool,
        catalog: Arc<dyn CatalogStore>,
        authenticator: Arc<dyn Authenticator>,
        admission: Arc<dyn BatchAdmission>,
        limits: RuntimeLimits,
    ) -> RuntimeResult<Self> {
        limits.validate()?;
        Ok(Self {
            implementation_pool,
            schema_validator: SchemaValidator::new(catalog),
            authenticator,
            trusted_identity_validator: Arc::new(RejectUntrustedIdentityClaims),
            admission,
            ddl: None,
            pressure: Arc::new(NoopPressureHooks),
            drain: DrainController::new(),
            limits,
        })
    }

    /// Installs the internal trusted-identity validation hook.
    #[must_use]
    pub fn with_trusted_identity_validator(mut self, validator: Arc<dyn TrustedIdentityValidator>) -> Self {
        self.trusted_identity_validator = validator;
        self
    }

    /// Installs the strict Flight SQL DDL handler.
    #[must_use]
    pub fn with_ddl_handler(mut self, handler: Arc<dyn FlightSqlDdlHandler>) -> Self {
        self.ddl = Some(FlightSqlDdlAdapter::new(handler, &self.limits));
        self
    }

    /// Installs non-blocking runtime pressure hooks.
    #[must_use]
    pub fn with_pressure_hooks(mut self, pressure: Arc<dyn PressureHooks>) -> Self {
        self.pressure = pressure;
        self
    }

    /// Reuses a process-wide drain controller.
    #[must_use]
    pub fn with_drain_controller(mut self, drain: DrainController) -> Self {
        self.drain = drain;
        self
    }

    /// Returns the shared drain controller.
    #[must_use]
    pub const fn drain_controller(&self) -> &DrainController {
        &self.drain
    }

    /// Builds a tonic service with the runtime message-size limit.
    #[must_use]
    pub fn into_tonic_service(self) -> FlightServiceServer<Self> {
        let maximum = self.limits.max_message_bytes;
        FlightServiceServer::new(self)
            .max_decoding_message_size(maximum)
            .max_encoding_message_size(maximum)
    }

    /// Handles a generic `FlightData` stream, enabling focused tests without a
    /// bound socket.
    ///
    /// # Errors
    ///
    /// Returns a structured tonic status for authentication, metadata, schema,
    /// sequencing, capacity, deadline, cancellation, or implementation failure.
    pub async fn handle_do_put<S>(
        &self,
        request: Request<S>,
    ) -> Result<Response<FlightResponseStream<PutResult>>, Status>
    where
        S: Stream<Item = Result<FlightData, Status>> + Send + 'static,
    {
        self.handle_do_put_inner(request)
            .await
            .map_err(RuntimeError::into_status)
    }

    async fn handle_do_put_inner<S>(
        &self,
        request: Request<S>,
    ) -> RuntimeResult<Response<FlightResponseStream<PutResult>>>
    where
        S: Stream<Item = Result<FlightData, Status>> + Send + 'static,
    {
        validate_metadata_size(request.metadata(), self.limits.max_metadata_bytes)?;
        let ingest_metadata = parse_optional_ingest_metadata(request.metadata())?;
        if ingest_metadata
            .as_ref()
            .is_some_and(|metadata| metadata.implementation_pool() != self.implementation_pool)
        {
            return Err(RuntimeError::new(
                arrowcol_protocol::ErrorCode::PoolUnavailable,
                Code::FailedPrecondition,
                "request selected a different collector implementation pool",
            ));
        }

        let permit = self.drain.try_acquire().ok_or_else(RuntimeError::draining)?;
        let deadline = effective_deadline(grpc_deadline(request.metadata())?, self.limits.max_stream_duration);
        let metadata = request.metadata().clone();
        let trusted_identity = run_until(deadline, self.trusted_identity_validator.validate(&metadata)).await?;
        let authentication = run_until(
            deadline,
            self.authenticator
                .authenticate(AuthenticationRequest::new(metadata, trusted_identity)),
        )
        .await?;

        let mut stream: InboundFlightStream = Box::pin(request.into_inner());
        let first = next_message(&mut stream, deadline).await?;
        validate_message_size(&first, self.limits.max_message_bytes)?;

        if first
            .flight_descriptor
            .as_ref()
            .is_some_and(|descriptor| descriptor.r#type() == DescriptorType::Cmd)
        {
            return self
                .execute_ddl(first, stream, deadline, permit, ingest_metadata.as_ref())
                .await;
        }
        match ingest_metadata {
            Some(ingest_metadata) => {
                self.start_ingest(ingest_metadata, authentication, first, stream, deadline, permit)
                    .await
            }
            None => self.execute_ddl(first, stream, deadline, permit, None).await,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_ingest(
        &self,
        metadata: IngestMetadata,
        authentication: arrowcol_protocol::AuthContext,
        first: FlightData,
        stream: InboundFlightStream,
        deadline: Instant,
        permit: DrainPermit,
    ) -> RuntimeResult<Response<FlightResponseStream<PutResult>>> {
        let descriptor_version = validate_ingest_descriptor(first.flight_descriptor.as_ref(), &metadata)?;
        let received_schema = decode_first_schema(&first, self.limits.max_schema_bytes)?;
        let actual_fingerprint = fingerprint_schema(received_schema.as_ref());
        let validated = run_until(deadline, async {
            self.schema_validator
                .validate(
                    metadata.table(),
                    metadata.claimed_schema_fingerprint(),
                    received_schema.as_ref(),
                )
                .await
                .map_err(|error| RuntimeError::from_catalog(&error))
        })
        .await?;
        if descriptor_version != validated.registered().version() {
            return Err(RuntimeError::new(
                arrowcol_protocol::ErrorCode::SchemaFingerprintMismatch,
                Code::InvalidArgument,
                "Flight descriptor schema version does not match the received schema",
            ));
        }

        let current_arrow = Arc::new(
            validated
                .current()
                .arrow_schema()
                .map_err(|_| RuntimeError::internal())?,
        );
        let context = StreamContext::new(
            metadata,
            authentication,
            Arc::clone(&received_schema),
            validated.registered().clone(),
            validated.current().clone(),
            validated.alignment().clone(),
            actual_fingerprint,
            Some(deadline),
        );
        let (result_sender, result_receiver) = mpsc::channel(self.limits.put_result_channel_capacity);
        let force_stop = self.drain.force_stop_token();
        let emitter: Arc<dyn PutResultEmitter> = Arc::new(ChannelPutResultEmitter {
            sender: result_sender.clone(),
            deadline,
            force_stop: force_stop.clone(),
        });
        let begin_deadline = admission_deadline(deadline, self.limits.admission_timeout);
        let lifecycle = run_until(
            begin_deadline,
            self.admission.begin(context.clone(), Arc::clone(&emitter)),
        )
        .await?;
        self.pressure.observe(PressureEvent::StreamStarted);

        let pressure = Arc::clone(&self.pressure);
        let limits = self.limits.clone();
        tokio::spawn(process_ingest(
            first,
            stream,
            received_schema,
            current_arrow,
            context,
            lifecycle,
            emitter,
            result_sender,
            pressure,
            force_stop,
            limits,
            deadline,
            permit,
        ));

        Ok(Response::new(
            Box::pin(ReceiverStream::new(result_receiver)) as FlightResponseStream<PutResult>
        ))
    }

    async fn execute_ddl(
        &self,
        first: FlightData,
        mut stream: InboundFlightStream,
        deadline: Instant,
        _permit: DrainPermit,
        routing: Option<&IngestMetadata>,
    ) -> RuntimeResult<Response<FlightResponseStream<PutResult>>> {
        let adapter = self.ddl.as_ref().ok_or_else(|| {
            RuntimeError::new(
                arrowcol_protocol::ErrorCode::SchemaIncompatible,
                Code::Unimplemented,
                "Flight SQL DDL is not enabled",
            )
        })?;
        let command = decode_ddl_command(&first)?;
        if let Some(routing) = routing {
            let parsed = DdlParser::default()
                .parse(&command.query)
                .map_err(|error| RuntimeError::from_catalog(&error))?;
            if parsed.identity() != routing.table() {
                return Err(RuntimeError::new(
                    arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
                    Code::InvalidArgument,
                    "Flight SQL target does not match collector routing metadata",
                ));
            }
        }
        match next_optional_message(&mut stream, deadline).await? {
            None => {}
            Some(data) => {
                validate_message_size(&data, self.limits.max_message_bytes)?;
                return Err(RuntimeError::new(
                    arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
                    Code::InvalidArgument,
                    "Flight SQL DDL stream contained unexpected body messages",
                ));
            }
        }
        let response = run_until(deadline, adapter.execute(&command.query)).await?;
        let (put_result, response_metadata, _completion) = response.into_parts();
        let output = stream::once(async move { Ok(put_result) });
        let mut response = Response::new(Box::pin(output) as FlightResponseStream<PutResult>);
        *response.metadata_mut() = response_metadata;
        Ok(response)
    }
}

#[tonic::async_trait]
impl FlightService for ArrowColFlightService {
    type HandshakeStream = FlightResponseStream<HandshakeResponse>;
    type ListFlightsStream = FlightResponseStream<FlightInfo>;
    type DoGetStream = FlightResponseStream<FlightData>;
    type DoPutStream = FlightResponseStream<PutResult>;
    type DoExchangeStream = FlightResponseStream<FlightData>;
    type DoActionStream = FlightResponseStream<arrow_flight::Result>;
    type ListActionsStream = FlightResponseStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Flight handshake is not enabled"))
    }

    async fn list_flights(&self, _request: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Flight listing is not enabled"))
    }

    async fn get_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Flight queries are not enabled"))
    }

    async fn poll_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Flight polling is not enabled"))
    }

    async fn get_schema(&self, _request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Flight schema lookup is not enabled"))
    }

    async fn do_get(&self, _request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("Flight downloads are not enabled"))
    }

    async fn do_put(
        &self,
        request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.handle_do_put(request).await
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Flight exchange is not enabled"))
    }

    async fn do_action(&self, _request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Flight actions are not enabled"))
    }

    async fn list_actions(&self, _request: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(Box::pin(stream::empty())))
    }
}

struct ChannelPutResultEmitter {
    sender: mpsc::Sender<Result<PutResult, Status>>,
    deadline: Instant,
    force_stop: CancellationToken,
}

#[async_trait]
impl PutResultEmitter for ChannelPutResultEmitter {
    async fn emit(&self, metadata: PutResultMetadata) -> RuntimeResult<()> {
        let result = metadata.into_put_result()?;
        let send = self.sender.send(Ok(result));
        tokio::pin!(send);
        let timeout = tokio::time::sleep_until(tokio::time::Instant::from_std(self.deadline));
        tokio::pin!(timeout);
        tokio::select! {
            _ = self.force_stop.cancelled() => Err(RuntimeError::draining()),
            _ = &mut timeout => Err(RuntimeError::deadline_exceeded()),
            sent = &mut send => sent.map_err(|_| RuntimeError::cancelled()),
        }
    }
}

struct BatchEnvelope {
    batch: arrow_array::RecordBatch,
    bytes: u64,
}

#[allow(clippy::too_many_arguments)]
async fn process_ingest(
    first: FlightData,
    stream: InboundFlightStream,
    received_schema: SchemaRef,
    current_schema: SchemaRef,
    context: StreamContext,
    lifecycle: Box<dyn StreamLifecycle>,
    emitter: Arc<dyn PutResultEmitter>,
    result_sender: mpsc::Sender<Result<PutResult, Status>>,
    pressure: Arc<dyn PressureHooks>,
    force_stop: CancellationToken,
    limits: RuntimeLimits,
    deadline: Instant,
    _permit: DrainPermit,
) {
    let cancellation = CancellationToken::new();
    let (batch_sender, mut batch_receiver) = mpsc::channel::<BatchEnvelope>(limits.batch_channel_capacity);
    let decoder_cancellation = cancellation.clone();
    let decoder_pressure = Arc::clone(&pressure);
    let decoder = tokio::spawn(decode_batches(
        first,
        stream,
        Arc::clone(&received_schema),
        batch_sender,
        decoder_cancellation,
        decoder_pressure,
        limits.max_message_bytes,
    ));
    let mut lifecycle = Some(lifecycle);
    let mut summary = StreamSummary::default();

    loop {
        let deadline_sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
        tokio::pin!(deadline_sleep);
        let next = tokio::select! {
            _ = result_sender.closed() => {
                cancellation.cancel();
                abort_lifecycle(&mut lifecycle, StreamAbortReason::ClientCancelled).await;
                pressure.observe(PressureEvent::StreamFinished {
                    summary,
                    outcome: StreamOutcome::Cancelled,
                });
                return;
            }
            _ = force_stop.cancelled() => {
                cancellation.cancel();
                abort_lifecycle(&mut lifecycle, StreamAbortReason::Shutdown).await;
                send_error(&result_sender, RuntimeError::draining()).await;
                pressure.observe(PressureEvent::StreamFinished {
                    summary,
                    outcome: StreamOutcome::Shutdown,
                });
                return;
            }
            _ = &mut deadline_sleep => {
                cancellation.cancel();
                abort_lifecycle(&mut lifecycle, StreamAbortReason::DeadlineExceeded).await;
                send_error(&result_sender, RuntimeError::deadline_exceeded()).await;
                pressure.observe(PressureEvent::StreamFinished {
                    summary,
                    outcome: StreamOutcome::DeadlineExceeded,
                });
                return;
            }
            next = batch_receiver.recv() => next,
        };
        let Some(envelope) = next else {
            break;
        };
        pressure.observe(PressureEvent::BatchDequeued { bytes: envelope.bytes });
        let aligned = match align_record_batch(envelope.batch, context.alignment(), Arc::clone(&current_schema)) {
            Ok(batch) => batch,
            Err(error) => {
                cancellation.cancel();
                abort_lifecycle(&mut lifecycle, StreamAbortReason::DecodeFailure).await;
                send_error(&result_sender, error).await;
                pressure.observe(PressureEvent::StreamFinished {
                    summary,
                    outcome: StreamOutcome::DecodeFailed,
                });
                return;
            }
        };
        let Some(active_lifecycle) = lifecycle.as_mut() else {
            send_error(&result_sender, RuntimeError::internal()).await;
            return;
        };
        let call_deadline = admission_deadline(deadline, limits.admission_timeout);
        let push_result = {
            let push = active_lifecycle.push_batch(aligned.clone());
            tokio::pin!(push);
            let timeout = tokio::time::sleep_until(tokio::time::Instant::from_std(call_deadline));
            tokio::pin!(timeout);
            tokio::select! {
                _ = result_sender.closed() => {
                    Err(AdmissionInterruption::ClientCancelled)
                }
                _ = force_stop.cancelled() => {
                    Err(AdmissionInterruption::Shutdown)
                }
                _ = &mut timeout => {
                    Err(AdmissionInterruption::DeadlineExceeded)
                }
                result = &mut push => {
                    result.map_err(AdmissionInterruption::Implementation)
                }
            }
        };
        match push_result {
            Ok(()) => summary.record(&aligned),
            Err(AdmissionInterruption::ClientCancelled) => {
                cancellation.cancel();
                abort_lifecycle(&mut lifecycle, StreamAbortReason::ClientCancelled).await;
                pressure.observe(PressureEvent::StreamFinished {
                    summary,
                    outcome: StreamOutcome::Cancelled,
                });
                return;
            }
            Err(AdmissionInterruption::Shutdown) => {
                cancellation.cancel();
                abort_lifecycle(&mut lifecycle, StreamAbortReason::Shutdown).await;
                send_error(&result_sender, RuntimeError::draining()).await;
                pressure.observe(PressureEvent::StreamFinished {
                    summary,
                    outcome: StreamOutcome::Shutdown,
                });
                return;
            }
            Err(AdmissionInterruption::DeadlineExceeded) => {
                cancellation.cancel();
                abort_lifecycle(&mut lifecycle, StreamAbortReason::DeadlineExceeded).await;
                send_error(&result_sender, RuntimeError::deadline_exceeded()).await;
                pressure.observe(PressureEvent::StreamFinished {
                    summary,
                    outcome: StreamOutcome::DeadlineExceeded,
                });
                return;
            }
            Err(AdmissionInterruption::Implementation(error)) => {
                cancellation.cancel();
                abort_lifecycle(&mut lifecycle, StreamAbortReason::AdmissionFailure).await;
                send_error(&result_sender, error).await;
                pressure.observe(PressureEvent::StreamFinished {
                    summary,
                    outcome: StreamOutcome::AdmissionFailed,
                });
                return;
            }
        }
    }

    let decoder_result = match decoder.await {
        Ok(result) => result,
        Err(_) => Err(RuntimeError::internal()),
    };
    if let Err(error) = decoder_result {
        abort_lifecycle(&mut lifecycle, StreamAbortReason::DecodeFailure).await;
        send_error(&result_sender, error).await;
        pressure.observe(PressureEvent::StreamFinished {
            summary,
            outcome: StreamOutcome::DecodeFailed,
        });
        return;
    }

    let Some(active_lifecycle) = lifecycle.as_mut() else {
        send_error(&result_sender, RuntimeError::internal()).await;
        return;
    };
    let call_deadline = admission_deadline(deadline, limits.admission_timeout);
    let completion_result = {
        let complete = active_lifecycle.complete(summary);
        tokio::pin!(complete);
        let timeout = tokio::time::sleep_until(tokio::time::Instant::from_std(call_deadline));
        tokio::pin!(timeout);
        tokio::select! {
            _ = result_sender.closed() => {
                Err(AdmissionInterruption::ClientCancelled)
            }
            _ = force_stop.cancelled() => {
                Err(AdmissionInterruption::Shutdown)
            }
            _ = &mut timeout => {
                Err(AdmissionInterruption::DeadlineExceeded)
            }
            result = &mut complete => {
                result.map_err(AdmissionInterruption::Implementation)
            }
        }
    };
    let completion = match completion_result {
        Ok(completion) => {
            drop(lifecycle.take());
            completion
        }
        Err(AdmissionInterruption::ClientCancelled) => {
            abort_lifecycle(&mut lifecycle, StreamAbortReason::ClientCancelled).await;
            pressure.observe(PressureEvent::StreamFinished {
                summary,
                outcome: StreamOutcome::Cancelled,
            });
            return;
        }
        Err(AdmissionInterruption::Shutdown) => {
            abort_lifecycle(&mut lifecycle, StreamAbortReason::Shutdown).await;
            send_error(&result_sender, RuntimeError::draining()).await;
            pressure.observe(PressureEvent::StreamFinished {
                summary,
                outcome: StreamOutcome::Shutdown,
            });
            return;
        }
        Err(AdmissionInterruption::DeadlineExceeded) => {
            abort_lifecycle(&mut lifecycle, StreamAbortReason::DeadlineExceeded).await;
            send_error(&result_sender, RuntimeError::deadline_exceeded()).await;
            pressure.observe(PressureEvent::StreamFinished {
                summary,
                outcome: StreamOutcome::DeadlineExceeded,
            });
            return;
        }
        Err(AdmissionInterruption::Implementation(error)) => {
            abort_lifecycle(&mut lifecycle, StreamAbortReason::AdmissionFailure).await;
            send_error(&result_sender, error).await;
            pressure.observe(PressureEvent::StreamFinished {
                summary,
                outcome: StreamOutcome::AdmissionFailed,
            });
            return;
        }
    };
    let metadata = PutResultMetadata::completion(
        completion,
        context.registered_schema().version(),
        context.current_schema().version(),
        context.actual_fingerprint(),
        summary,
    );
    if let Some(metadata) = metadata
        && emitter.emit(metadata).await.is_err()
    {
        pressure.observe(PressureEvent::StreamFinished {
            summary,
            outcome: StreamOutcome::Cancelled,
        });
        return;
    }
    pressure.observe(PressureEvent::StreamFinished {
        summary,
        outcome: StreamOutcome::Completed,
    });
}

enum AdmissionInterruption {
    ClientCancelled,
    DeadlineExceeded,
    Shutdown,
    Implementation(RuntimeError),
}

async fn decode_batches(
    first: FlightData,
    stream: InboundFlightStream,
    expected_schema: SchemaRef,
    sender: mpsc::Sender<BatchEnvelope>,
    cancellation: CancellationToken,
    pressure: Arc<dyn PressureHooks>,
    max_message_bytes: usize,
) -> RuntimeResult<()> {
    let checked = stream.map(move |item| match item {
        Ok(data) => validate_followup_message(&data, max_message_bytes).map(|()| data),
        Err(status) => Err(status_to_runtime(status)),
    });
    let input = stream::once(async move { Ok(first) })
        .chain(checked)
        .map(|item| item.map_err(|error| FlightError::from_external_error(Box::new(error))));
    let mut decoder = FlightDataDecoder::new(input);
    let mut saw_schema = false;
    loop {
        let item = tokio::select! {
            _ = cancellation.cancelled() => return Err(RuntimeError::cancelled()),
            item = decoder.next() => item,
        };
        let Some(item) = item else {
            break;
        };
        let decoded = item.map_err(flight_error_to_runtime)?;
        match decoded.payload {
            DecodedPayload::Schema(schema) if !saw_schema => {
                if schema.as_ref() != expected_schema.as_ref() {
                    return Err(invalid_flight_stream());
                }
                saw_schema = true;
            }
            DecodedPayload::Schema(_) => return Err(invalid_flight_stream()),
            DecodedPayload::RecordBatch(batch) if saw_schema => {
                let batch_bytes = record_batch_logical_bytes(&batch)?;
                if batch_bytes > max_message_bytes {
                    return Err(RuntimeError::new(
                        arrowcol_protocol::ErrorCode::CapacityExhausted,
                        Code::ResourceExhausted,
                        "decoded Arrow record batch exceeds the configured limit",
                    ));
                }
                let bytes = u64::try_from(batch_bytes).unwrap_or(u64::MAX);
                pressure.observe(PressureEvent::BatchQueued { bytes });
                let envelope = BatchEnvelope { batch, bytes };
                tokio::select! {
                    _ = cancellation.cancelled() => return Err(RuntimeError::cancelled()),
                    sent = sender.send(envelope) => {
                        if sent.is_err() {
                            return Err(RuntimeError::cancelled());
                        }
                    }
                }
            }
            DecodedPayload::RecordBatch(_) | DecodedPayload::None => {
                return Err(invalid_flight_stream());
            }
        }
    }
    if !saw_schema {
        return Err(invalid_flight_stream());
    }
    Ok(())
}

async fn abort_lifecycle(lifecycle: &mut Option<Box<dyn StreamLifecycle>>, reason: StreamAbortReason) {
    if let Some(lifecycle) = lifecycle.take() {
        lifecycle.abort(reason).await;
    }
}

async fn send_error(sender: &mpsc::Sender<Result<PutResult, Status>>, error: RuntimeError) {
    let _ignored = sender.send(Err(error.into_status())).await;
}

fn decode_first_schema(data: &FlightData, max_schema_bytes: usize) -> RuntimeResult<SchemaRef> {
    let schema_bytes = data.data_header.len().saturating_add(data.data_body.len());
    if schema_bytes > max_schema_bytes {
        return Err(RuntimeError::new(
            arrowcol_protocol::ErrorCode::CapacityExhausted,
            Code::ResourceExhausted,
            "Arrow schema message exceeds the configured limit",
        ));
    }
    let message = arrow_ipc::root_as_message(&data.data_header).map_err(|_| invalid_flight_stream())?;
    if message.header_type() != MessageHeader::Schema {
        return Err(RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "first FlightData message must contain the Arrow schema",
        ));
    }
    Schema::try_from(data)
        .map(Arc::new)
        .map_err(|_| invalid_flight_stream())
}

fn validate_followup_message(data: &FlightData, max_message_bytes: usize) -> RuntimeResult<()> {
    validate_message_size(data, max_message_bytes)?;
    if data.flight_descriptor.is_some() {
        return Err(RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "Flight descriptor is only valid on the first message",
        ));
    }
    let message = arrow_ipc::root_as_message(&data.data_header).map_err(|_| invalid_flight_stream())?;
    if message.header_type() == MessageHeader::Schema {
        return Err(RuntimeError::new(
            arrowcol_protocol::ErrorCode::SchemaIncompatible,
            Code::InvalidArgument,
            "multiple Arrow schema messages are not allowed",
        ));
    }
    Ok(())
}

fn validate_ingest_descriptor(
    descriptor: Option<&FlightDescriptor>,
    metadata: &IngestMetadata,
) -> RuntimeResult<SchemaVersion> {
    let descriptor = descriptor.ok_or_else(|| {
        RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "first FlightData message is missing its descriptor",
        )
    })?;
    if descriptor.r#type() != DescriptorType::Path
        || !descriptor.cmd.is_empty()
        || descriptor.path.len() != 4
        || descriptor.path[0] != metadata.table().catalog().as_str()
        || descriptor.path[1] != metadata.table().schema().as_str()
        || descriptor.path[2] != metadata.table().table().as_str()
    {
        return Err(RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "Flight descriptor does not match collector routing metadata",
        ));
    }
    parse_descriptor_version(&descriptor.path[3])
}

fn parse_descriptor_version(value: &str) -> RuntimeResult<SchemaVersion> {
    let number = value.strip_prefix('v').ok_or_else(|| {
        RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "Flight descriptor schema version is malformed",
        )
    })?;
    if number.is_empty()
        || (number.len() > 1 && number.starts_with('0'))
        || !number.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "Flight descriptor schema version is malformed",
        ));
    }
    let value = number.parse::<u64>().map_err(|_| {
        RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "Flight descriptor schema version is malformed",
        )
    })?;
    SchemaVersion::new(value).map_err(|_| {
        RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "Flight descriptor schema version is malformed",
        )
    })
}

fn decode_ddl_command(data: &FlightData) -> RuntimeResult<CommandStatementUpdate> {
    let descriptor = data.flight_descriptor.as_ref().ok_or_else(|| {
        RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "Flight SQL DDL request is missing its command descriptor",
        )
    })?;
    if descriptor.r#type() != DescriptorType::Cmd
        || descriptor.cmd.is_empty()
        || !descriptor.path.is_empty()
        || !data.data_header.is_empty()
        || !data.data_body.is_empty()
        || !data.app_metadata.is_empty()
    {
        return Err(RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "Flight SQL DDL request has an invalid stream shape",
        ));
    }
    let any = Any::decode(descriptor.cmd.as_ref()).map_err(|_| {
        RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "Flight SQL command descriptor is malformed",
        )
    })?;
    let command = Command::try_from(any).map_err(|_| {
        RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "Flight SQL command descriptor is malformed",
        )
    })?;
    let Command::CommandStatementUpdate(command) = command else {
        return Err(RuntimeError::new(
            arrowcol_protocol::ErrorCode::SchemaIncompatible,
            Code::Unimplemented,
            "only strict Flight SQL DDL statement updates are enabled",
        ));
    };
    if command
        .transaction_id
        .as_ref()
        .is_some_and(|transaction_id| !transaction_id.is_empty())
    {
        return Err(RuntimeError::new(
            arrowcol_protocol::ErrorCode::SchemaIncompatible,
            Code::InvalidArgument,
            "Flight SQL DDL transactions are not enabled",
        ));
    }
    Ok(command)
}

fn parse_optional_ingest_metadata(metadata: &MetadataMap) -> RuntimeResult<Option<IngestMetadata>> {
    let keys = [
        PROTOCOL_VERSION_METADATA_KEY,
        IMPLEMENTATION_POOL_METADATA_KEY,
        CATALOG_METADATA_KEY,
        SCHEMA_METADATA_KEY,
        TABLE_METADATA_KEY,
        CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY,
    ];
    if !keys.iter().any(|key| metadata.contains_key(*key)) {
        return Ok(None);
    }
    let raw = RawGrpcMetadata {
        protocol_version: ascii_metadata(metadata, PROTOCOL_VERSION_METADATA_KEY)?,
        implementation_pool: ascii_metadata(metadata, IMPLEMENTATION_POOL_METADATA_KEY)?,
        catalog: ascii_metadata(metadata, CATALOG_METADATA_KEY)?,
        schema: ascii_metadata(metadata, SCHEMA_METADATA_KEY)?,
        table: ascii_metadata(metadata, TABLE_METADATA_KEY)?,
        claimed_schema_fingerprint: ascii_metadata(metadata, CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY)?,
    };
    IngestMetadata::parse(raw)
        .map(Some)
        .map_err(|error| RuntimeError::from_metadata(&error))
}

fn ascii_metadata<'a>(metadata: &'a MetadataMap, key: &'static str) -> RuntimeResult<Option<&'a str>> {
    metadata
        .get(key)
        .map(|value| {
            value.to_str().map_err(|_| {
                RuntimeError::new(
                    arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
                    Code::InvalidArgument,
                    "collector routing metadata must be visible ASCII",
                )
            })
        })
        .transpose()
}

fn validate_metadata_size(metadata: &MetadataMap, maximum: usize) -> RuntimeResult<()> {
    let mut total = 0_usize;
    for entry in metadata.iter() {
        let (key_bytes, value_bytes) = match entry {
            KeyAndValueRef::Ascii(key, value) => (key.as_str().len(), value.as_encoded_bytes().len()),
            KeyAndValueRef::Binary(key, value) => (key.as_str().len(), value.as_encoded_bytes().len()),
        };
        total = total.saturating_add(key_bytes).saturating_add(value_bytes);
        if total > maximum {
            return Err(RuntimeError::new(
                arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
                Code::ResourceExhausted,
                "gRPC metadata exceeds the configured limit",
            ));
        }
    }
    Ok(())
}

fn validate_message_size(data: &FlightData, maximum: usize) -> RuntimeResult<()> {
    if data.encoded_len() > maximum {
        return Err(RuntimeError::new(
            arrowcol_protocol::ErrorCode::CapacityExhausted,
            Code::ResourceExhausted,
            "FlightData message exceeds the configured limit",
        ));
    }
    Ok(())
}

fn record_batch_logical_bytes(batch: &arrow_array::RecordBatch) -> RuntimeResult<usize> {
    batch.columns().iter().try_fold(0_usize, |total, column| {
        let bytes = column
            .to_data()
            .get_slice_memory_size()
            .map_err(|_| RuntimeError::internal())?;
        total.checked_add(bytes).ok_or_else(RuntimeError::internal)
    })
}

async fn next_message(stream: &mut InboundFlightStream, deadline: Instant) -> RuntimeResult<FlightData> {
    next_optional_message(stream, deadline).await?.ok_or_else(|| {
        RuntimeError::new(
            arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
            Code::InvalidArgument,
            "DoPut stream ended before its first message",
        )
    })
}

async fn next_optional_message(
    stream: &mut InboundFlightStream,
    deadline: Instant,
) -> RuntimeResult<Option<FlightData>> {
    run_until(deadline, async {
        match stream.next().await {
            Some(Ok(data)) => Ok(Some(data)),
            Some(Err(status)) => Err(status_to_runtime(status)),
            None => Ok(None),
        }
    })
    .await
}

fn effective_deadline(request: Option<Instant>, maximum: Duration) -> Instant {
    let configured = Instant::now().checked_add(maximum).unwrap_or_else(Instant::now);
    request.map_or(configured, |request| request.min(configured))
}

fn grpc_deadline(metadata: &MetadataMap) -> RuntimeResult<Option<Instant>> {
    let Some(value) = metadata.get("grpc-timeout") else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| invalid_grpc_timeout())?;
    if !(2..=9).contains(&value.len()) {
        return Err(invalid_grpc_timeout());
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if digits.is_empty() || digits.len() > 8 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_grpc_timeout());
    }
    let amount = digits.parse::<u64>().map_err(|_| invalid_grpc_timeout())?;
    let duration = match unit {
        "H" => Duration::from_secs(amount.saturating_mul(60 * 60)),
        "M" => Duration::from_secs(amount.saturating_mul(60)),
        "S" => Duration::from_secs(amount),
        "m" => Duration::from_millis(amount),
        "u" => Duration::from_micros(amount),
        "n" => Duration::from_nanos(amount),
        _ => return Err(invalid_grpc_timeout()),
    };
    Instant::now()
        .checked_add(duration)
        .map(Some)
        .ok_or_else(invalid_grpc_timeout)
}

fn admission_deadline(request: Instant, timeout: Duration) -> Instant {
    Instant::now()
        .checked_add(timeout)
        .map_or(request, |admission| admission.min(request))
}

async fn run_until<T, F>(deadline: Instant, future: F) -> RuntimeResult<T>
where
    F: Future<Output = RuntimeResult<T>>,
{
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
        .await
        .map_err(|_| RuntimeError::deadline_exceeded())?
}

fn status_to_runtime(status: Status) -> RuntimeError {
    match status.code() {
        Code::Cancelled => RuntimeError::cancelled(),
        Code::DeadlineExceeded => RuntimeError::deadline_exceeded(),
        Code::ResourceExhausted => RuntimeError::new(
            arrowcol_protocol::ErrorCode::CapacityExhausted,
            Code::ResourceExhausted,
            "transport resource limit was exceeded",
        ),
        _ => invalid_flight_stream(),
    }
}

fn flight_error_to_runtime(error: FlightError) -> RuntimeError {
    match error {
        FlightError::ExternalError(error) => match error.downcast::<RuntimeError>() {
            Ok(error) => *error,
            Err(_) => invalid_flight_stream(),
        },
        FlightError::Tonic(status) => status_to_runtime(*status),
        FlightError::Arrow(_)
        | FlightError::NotYetImplemented(_)
        | FlightError::ProtocolError(_)
        | FlightError::DecodeError(_) => invalid_flight_stream(),
    }
}

fn invalid_flight_stream() -> RuntimeError {
    RuntimeError::new(
        arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
        Code::InvalidArgument,
        "invalid Arrow Flight stream",
    )
}

fn invalid_grpc_timeout() -> RuntimeError {
    RuntimeError::new(
        arrowcol_protocol::ErrorCode::RoutingMetadataInvalid,
        Code::InvalidArgument,
        "gRPC deadline metadata is malformed",
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use arrow_array::{ArrayRef, RecordBatch, StringArray};
    use arrow_flight::{decode::FlightRecordBatchStream, encode::FlightDataEncoderBuilder};
    use arrow_schema::{DataType, Field, Schema};
    use futures::{TryStreamExt, stream};

    use super::*;

    /// Scenario: IPC decoding retains many column slices from one shared FlightData body.
    /// Guarantees: Admission limits use logical Arrow bytes rather than multiplying the shared body allocation.
    #[tokio::test]
    async fn decoded_shared_body_uses_logical_batch_size() {
        const LIMIT: usize = 32 * 1024 * 1024;
        let fields = (0..48)
            .map(|index| Field::new(format!("field_{index}"), DataType::Utf8, false))
            .collect::<Vec<_>>();
        let columns = (0..48)
            .map(|index| {
                Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                    format!("field-{index}-value-with-enough-bytes"),
                    8_192,
                ))) as ArrayRef
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("batch");
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(batch.schema())
            .with_max_flight_data_size(31 * 1024 * 1024)
            .build(stream::iter([Ok::<_, FlightError>(batch)]));
        let decoded = FlightRecordBatchStream::new_from_flight_data(encoded)
            .try_collect::<Vec<_>>()
            .await
            .expect("decode");

        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].get_array_memory_size() > LIMIT);
        assert!(record_batch_logical_bytes(&decoded[0]).expect("logical bytes") <= LIMIT);
    }
}
