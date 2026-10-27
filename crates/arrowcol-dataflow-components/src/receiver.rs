use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrowcol_catalog::FlightSqlDdlHandler;
use arrowcol_flight::{
    AllowAllAuthenticator, ArrowColFlightService, BatchAdmission, DrainController, DrainOutcome, PutResultEmitter,
    RuntimeError, RuntimeResult, StreamAbortReason, StreamCompletion, StreamContext, StreamLifecycle, StreamSummary,
};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, CollectorInstanceId, ErrorCode, ImplementationPool,
    PositiveMeasurement, PrincipalId, PrincipalIssuer,
};
use arrowcol_routing::{CollectorRegistration, CollectorState, RoutingStore};
use async_trait::async_trait;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ReceiverConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::{CallData, NodeControlMsg};
use otel_arrow_dfe_engine::error::{Error, ReceiverErrorKind, TypedError};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_engine::shared::receiver;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::{
    Interests, MessageSourceSharedEffectHandlerExtension, ProducerEffectHandlerExtension, ReceiverFactory,
};
use otel_arrow_dfe_otap::OTAP_RECEIVER_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::OtapPayload;
use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::FlightReceiverConfig;
use crate::config::{checked_add_millis, config_error, now_unix_millis, parse_config};
use crate::envelope::{ArrowcolBatchEnvelope, encode_record_batch};
use crate::metrics::{DurableStream, PendingTracker, RuntimeMetrics, global_metrics, process_instance_id};
use crate::routing::{LeaseResolver, StoreLeaseResolver};

/// DFE URN for arbitrary Arrow Flight ingress.
pub const ARROW_FLIGHT_RECEIVER_URN: &str = "urn:otel:receiver:arrow_flight";

/// Abstracts DFE message admission for focused zero-copy and backpressure tests.
#[async_trait]
pub trait PdataSender: Send + Sync {
    /// Sends one owned pdata message through a bounded downstream route.
    async fn send(&self, correlation_id: u64, pdata: OtapPdata) -> Result<(), PdataSendError>;
}

/// DFE send failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PdataSendError {
    /// The selected route is closed.
    #[error("DFE downstream route is closed")]
    Closed,
    /// The selected route remained unavailable.
    #[error("DFE downstream route is unavailable")]
    Unavailable,
}

struct EnginePdataSender {
    effect_handler: receiver::EffectHandler<OtapPdata>,
}

#[async_trait]
impl PdataSender for EnginePdataSender {
    async fn send(&self, correlation_id: u64, mut pdata: OtapPdata) -> Result<(), PdataSendError> {
        let mut call_data = CallData::new();
        call_data.push(correlation_id.into());
        self.effect_handler
            .subscribe_to(Interests::ACKS | Interests::NACKS, call_data, &mut pdata);
        self.effect_handler
            .send_message_with_source_node(pdata)
            .await
            .map_err(map_send_error)
    }
}

fn map_send_error(error: TypedError<OtapPdata>) -> PdataSendError {
    match error {
        TypedError::ChannelSendError(otel_arrow_dfe_channel::error::SendError::Closed(_))
        | TypedError::RuntimeMsgError(otel_arrow_dfe_channel::error::SendError::Closed(_))
        | TypedError::NodeControlMsgSendError {
            error: otel_arrow_dfe_channel::error::SendError::Closed(_),
            ..
        } => PdataSendError::Closed,
        TypedError::ChannelSendError(otel_arrow_dfe_channel::error::SendError::Full(_))
        | TypedError::RuntimeMsgError(otel_arrow_dfe_channel::error::SendError::Full(_))
        | TypedError::NodeControlMsgSendError {
            error: otel_arrow_dfe_channel::error::SendError::Full(_),
            ..
        }
        | TypedError::Error(_) => PdataSendError::Unavailable,
    }
}

/// Arrow Flight admission into DFE channels.
///
/// By default a stream completes with `best_effort_admission` once every batch
/// entered its bounded DFE route. With [`DfeAdmission::with_durable_admission`]
/// the stream completes with `durable_admission` only after the downstream
/// durable buffer acknowledged every batch.
pub struct DfeAdmission {
    sender: Arc<dyn PdataSender>,
    leases: Arc<dyn LeaseResolver>,
    instance_id: CollectorInstanceId,
    instance_key: String,
    permits: Arc<Semaphore>,
    pending: Arc<PendingTracker>,
    metrics: Arc<RuntimeMetrics>,
    durable_timeout: Option<Duration>,
}

impl DfeAdmission {
    /// Creates best-effort admission with a hard concurrent-stream bound.
    ///
    /// `instance_key` is the collector's stable `registration.instance_key`;
    /// it is stamped into every pdata envelope so a restarted collector can
    /// recognize durable-buffer replays from its predecessor.
    #[must_use]
    pub fn new(
        sender: Arc<dyn PdataSender>,
        leases: Arc<dyn LeaseResolver>,
        instance_id: CollectorInstanceId,
        instance_key: impl Into<String>,
        max_concurrent_streams: usize,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self::with_permits(
            sender,
            leases,
            instance_id,
            instance_key.into(),
            Arc::new(Semaphore::new(max_concurrent_streams)),
            metrics,
        )
    }

    /// Completes streams only after the downstream durable buffer acknowledged
    /// every batch, waiting at most `timeout` once the stream ends.
    ///
    /// The topology must route this admission into `processor:durable_buffer`:
    /// its acknowledgement follows the write-ahead-log append. A rejected
    /// batch, a failed send, or the timeout fails the stream with a retryable
    /// error, so the client resends; batches persisted before the failure are
    /// still delivered, which can produce duplicates.
    #[must_use]
    pub fn with_durable_admission(mut self, timeout: Duration) -> Self {
        self.durable_timeout = Some(timeout);
        self
    }

    fn with_permits(
        sender: Arc<dyn PdataSender>,
        leases: Arc<dyn LeaseResolver>,
        instance_id: CollectorInstanceId,
        instance_key: String,
        permits: Arc<Semaphore>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self {
            sender,
            leases,
            instance_id,
            instance_key,
            permits,
            pending: Arc::new(PendingTracker::new(Arc::clone(&metrics))),
            metrics,
            durable_timeout: None,
        }
    }

    fn resolve_feedback(&self, correlation_id: u64, ack: bool) {
        self.pending.resolve(correlation_id, ack);
    }
}

#[async_trait]
impl BatchAdmission for DfeAdmission {
    async fn begin(
        &self,
        context: StreamContext,
        _emitter: Arc<dyn PutResultEmitter>,
    ) -> RuntimeResult<Box<dyn StreamLifecycle>> {
        let lease = self.leases.resolve(context.metadata().table()).await?;
        if lease.holder_instance_id != self.instance_id {
            return Err(RuntimeError::new(
                ErrorCode::LeaseFenced,
                tonic::Code::FailedPrecondition,
                "collector does not own the table lease",
            ));
        }
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| RuntimeError::draining())?;
        let envelope = ArrowcolBatchEnvelope {
            publication_id: None,
            ingested_at_ms: None,
            table: context.metadata().table().clone(),
            source_schema_version: context.registered_schema().version(),
            target_schema_version: context.current_schema().version(),
            lease_id: lease.lease_id,
            holder_instance_id: lease.holder_instance_id,
            fencing_token: lease.fencing_epoch,
            instance_key: self.instance_key.clone(),
        };
        self.metrics.stream_started(&envelope.table);
        Ok(Box::new(DfeStreamLifecycle {
            sender: Arc::clone(&self.sender),
            pending: Arc::clone(&self.pending),
            metrics: Arc::clone(&self.metrics),
            envelope,
            permit: Some(permit),
            finished: false,
            durable: self.durable_timeout.map(DurableAdmission::new),
        }))
    }
}

/// Durable-admission state of one stream.
struct DurableAdmission {
    stream: Arc<DurableStream>,
    timeout: Duration,
}

impl DurableAdmission {
    fn new(timeout: Duration) -> Self {
        Self {
            stream: Arc::new(DurableStream::new()),
            timeout,
        }
    }

    /// Ends slightly before the Flight service's identical admission deadline so
    /// the client receives the explicit durable-admission failure rather than a
    /// generic deadline error.
    fn wait_budget(&self) -> Duration {
        self.timeout
            .saturating_sub((self.timeout / 10).min(DURABLE_DEADLINE_MARGIN))
    }

    fn ensure_not_rejected(&self) -> RuntimeResult<()> {
        if self.stream.progress().rejected > 0 {
            return Err(durable_rejection_error());
        }
        Ok(())
    }

    async fn wait(&self) -> RuntimeResult<()> {
        match self.stream.settled(self.wait_budget()).await {
            Some(progress) if progress.rejected == 0 => Ok(()),
            Some(_) => Err(durable_rejection_error()),
            None => Err(RuntimeError::new(
                ErrorCode::BackpressureTimeout,
                tonic::Code::Unavailable,
                "durable buffer did not acknowledge every batch before the admission timeout; \
                 the stream was not durably admitted and must be retried",
            )),
        }
    }
}

const DURABLE_DEADLINE_MARGIN: Duration = Duration::from_millis(100);

fn durable_rejection_error() -> RuntimeError {
    RuntimeError::new(
        ErrorCode::CapacityExhausted,
        tonic::Code::Unavailable,
        "durable buffer rejected a batch before persisting it; the stream was not durably admitted and must be retried",
    )
}

struct DfeStreamLifecycle {
    sender: Arc<dyn PdataSender>,
    pending: Arc<PendingTracker>,
    metrics: Arc<RuntimeMetrics>,
    envelope: ArrowcolBatchEnvelope,
    permit: Option<OwnedSemaphorePermit>,
    finished: bool,
    durable: Option<DurableAdmission>,
}

struct PendingSendGuard {
    pending: Arc<PendingTracker>,
    correlation_id: u64,
    armed: bool,
}

impl PendingSendGuard {
    fn new(pending: Arc<PendingTracker>, correlation_id: u64) -> Self {
        Self {
            pending,
            correlation_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingSendGuard {
    fn drop(&mut self) {
        if self.armed {
            self.pending.resolve(self.correlation_id, false);
        }
    }
}

impl DfeStreamLifecycle {
    fn finish(&mut self, completed: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        if completed {
            self.metrics.stream_completed(&self.envelope.table);
        } else {
            self.metrics.stream_aborted(&self.envelope.table);
        }
        let _ = self.permit.take();
    }
}

impl Drop for DfeStreamLifecycle {
    fn drop(&mut self) {
        self.finish(false);
    }
}

#[async_trait]
impl StreamLifecycle for DfeStreamLifecycle {
    async fn push_batch(&mut self, batch: RecordBatch) -> RuntimeResult<()> {
        if let Some(durable) = &self.durable {
            // A rejected batch already fails the stream; stop accepting more of it.
            durable.ensure_not_rejected()?;
        }
        let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
        let bytes = arrow_deltalake::record_batch_logical_bytes(&batch).map_err(|_| RuntimeError::internal())?;
        self.metrics.batch_received(rows, bytes);
        let mut envelope = self.envelope.clone();
        let publication_id = arrowcol_protocol::ReceiptId::new();
        envelope.publication_id = Some(publication_id);
        envelope.ingested_at_ms = Some(now_unix_millis().map_err(|_| RuntimeError::internal())?);
        let records = encode_record_batch(envelope, batch).map_err(|_| RuntimeError::internal())?;
        let correlation_id = self
            .pending
            .insert_for_stream(
                self.envelope.table.clone(),
                rows,
                bytes,
                self.envelope.source_schema_version,
                self.envelope.target_schema_version,
                self.durable.as_ref().map(|durable| Arc::clone(&durable.stream)),
            )
            .map_err(|_| RuntimeError::internal())?;
        let mut pending_send = PendingSendGuard::new(Arc::clone(&self.pending), correlation_id);
        let pdata = OtapPdata::new_todo_context(OtapPayload::from_pdata_arrow(records));
        crate::shutdown::PROCESS_ROUTING_SHUTDOWN
            .publication_finished(publication_id, false)
            .map_err(|_| RuntimeError::internal())?;
        match self.sender.send(correlation_id, pdata).await {
            Ok(()) => {
                pending_send.disarm();
                Ok(())
            }
            Err(error) => Err(match error {
                PdataSendError::Closed => RuntimeError::draining(),
                PdataSendError::Unavailable => RuntimeError::new(
                    ErrorCode::CapacityExhausted,
                    tonic::Code::Unavailable,
                    "DFE downstream admission is unavailable",
                ),
            }),
        }
    }

    async fn complete(&mut self, _summary: StreamSummary) -> RuntimeResult<StreamCompletion> {
        let Some(durable) = &self.durable else {
            self.finish(true);
            return Ok(StreamCompletion::best_effort());
        };
        if let Err(error) = durable.wait().await {
            // The service aborts the lifecycle next, which releases the stream as aborted.
            self.metrics.durable_admission_failed();
            return Err(error);
        }
        self.finish(true);
        Ok(StreamCompletion::durable_admission())
    }

    async fn abort(mut self: Box<Self>, _reason: StreamAbortReason) {
        self.finish(false);
    }
}

struct ArrowFlightReceiver {
    config: FlightReceiverConfig,
    routing: Arc<dyn RoutingStore>,
    metrics: Arc<RuntimeMetrics>,
    permits: Arc<Semaphore>,
}

fn validate_receiver_config(value: &serde_json::Value) -> Result<(), otel_arrow_dfe_config::error::Error> {
    let config: FlightReceiverConfig = parse_config(value)?;
    config.validate().map_err(config_error)
}

fn create_receiver(
    _pipeline: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    receiver_config: &ReceiverConfig,
    _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities,
) -> Result<ReceiverWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    let mut config: FlightReceiverConfig = parse_config(&node_config.config)?;
    if let Some(generation) = crate::config::process_cloud_generation() {
        config.registration.instance_key = format!("{}|cloud={generation}", config.registration.instance_key);
    }
    config.validate().map_err(config_error)?;
    let routing = config.routing.build().map_err(config_error)?;
    let metrics = global_metrics();
    metrics
        .configure_admission(&config.registration.instance_key, config.native_quiver)
        .map_err(config_error)?;
    let permits = metrics
        .stream_permits(config.registration.max_concurrent_streams)
        .map_err(config_error)?;
    metrics.configure_receiver(
        config.registration.max_concurrent_streams,
        config.registration.rss_limit_bytes,
        config.registration.buffered_bytes_limit,
        config.registration.buffered_rows_limit,
        config.registration.buffered_batches_limit,
        receiver_config.output_pdata_channel.capacity,
    );
    Ok(ReceiverWrapper::shared(
        ArrowFlightReceiver {
            config,
            routing,
            metrics,
            permits,
        },
        node,
        node_config,
        receiver_config,
    ))
}

/// External DFE receiver registration.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Receiver)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static ARROW_FLIGHT_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: ARROW_FLIGHT_RECEIVER_URN,
    create: create_receiver,
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED
        .with_pdata(otel_arrow_dfe_engine::wiring_contract::PdataWiringContract::PLUGGABLE_ARROW_SOURCE),
    validate_config: validate_receiver_config,
};

#[async_trait]
impl receiver::Receiver<OtapPdata> for ArrowFlightReceiver {
    async fn start(
        self: Box<Self>,
        mut control: receiver::ControlChannel<OtapPdata>,
        effect_handler: receiver::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let instance_id = process_instance_id();
        let limits =
            self.config.limits.build().map_err(|error| {
                receiver_error(&effect_handler, ReceiverErrorKind::Configuration, error.to_string())
            })?;
        let (catalog, ddl) = self
            .config
            .catalog
            .start()
            .await
            .map_err(|error| receiver_error(&effect_handler, ReceiverErrorKind::Connect, error.to_string()))?;
        self.metrics.set_catalog_ready(true);
        let leases: Arc<dyn LeaseResolver> = Arc::new(StoreLeaseResolver::new(Arc::clone(&self.routing), instance_id));
        let mut admission = DfeAdmission::with_permits(
            Arc::new(EnginePdataSender {
                effect_handler: effect_handler.clone(),
            }),
            leases,
            instance_id,
            self.config.registration.instance_key.clone(),
            Arc::clone(&self.permits),
            Arc::clone(&self.metrics),
        );
        if self.config.native_quiver {
            admission = admission.with_durable_admission(limits.admission_timeout);
        }
        let admission = Arc::new(admission);
        let admission_trait: Arc<dyn BatchAdmission> = Arc::clone(&admission) as Arc<dyn BatchAdmission>;
        let drain = DrainController::new();
        let ddl_handler: Arc<dyn FlightSqlDdlHandler> = ddl;
        let service = ArrowColFlightService::new(
            ImplementationPool::Dataflow,
            catalog,
            Arc::new(
                allow_all_authenticator()
                    .map_err(|error| receiver_error(&effect_handler, ReceiverErrorKind::Configuration, error))?,
            ),
            admission_trait,
            limits.clone(),
        )
        .map_err(|error| receiver_error(&effect_handler, ReceiverErrorKind::Configuration, error.to_string()))?
        .with_ddl_handler(ddl_handler)
        .with_drain_controller(drain.clone());

        register_collector(&self.routing, &self.config, instance_id, &self.metrics)
            .await
            .map_err(|error| receiver_error(&effect_handler, ReceiverErrorKind::Connect, error))?;

        let flight_listener = effect_handler.tcp_listener(self.config.listening_addr)?;
        let admin_listener = effect_handler.tcp_listener(self.config.admin_listening_addr)?;
        let shutdown = CancellationToken::new();
        let registration_shutdown = shutdown.child_token();
        let registration = tokio::spawn(registration_loop(
            Arc::clone(&self.routing),
            self.config.clone(),
            instance_id,
            Arc::clone(&self.metrics),
            registration_shutdown,
        ));
        let flight_shutdown = shutdown.child_token();
        let mut flight = tokio::spawn(async move {
            Server::builder()
                .add_service(service.into_tonic_service())
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(flight_listener),
                    flight_shutdown.cancelled_owned(),
                )
                .await
        });
        let admin_shutdown = shutdown.child_token();
        let admin_state = AdminState {
            metrics: Arc::clone(&self.metrics),
            max_request_bytes: u64::try_from(limits.max_message_bytes).unwrap_or(u64::MAX),
        };
        let mut admin = tokio::spawn(async move {
            axum::serve(admin_listener, admin_router(admin_state))
                .with_graceful_shutdown(admin_shutdown.cancelled_owned())
                .await
        });

        loop {
            tokio::select! {
                control_message = control.recv() => {
                    match control_message {
                        Ok(NodeControlMsg::Ack(ack)) => {
                            if let Some(id) = correlation_id(&ack.unwind.route.calldata) {
                                admission.resolve_feedback(id, true);
                            }
                        }
                        Ok(NodeControlMsg::Nack(nack)) => {
                            if let Some(id) = correlation_id(&nack.unwind.route.calldata) {
                                admission.resolve_feedback(id, false);
                            }
                        }
                        Ok(NodeControlMsg::DrainIngress { deadline, .. }) => {
                            self.metrics.set_accepting(false);
                            shutdown.cancel();
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            drain_streams(&drain, remaining, &mut control, &admission).await;
                            finalize_registration(
                                &self.routing,
                                instance_id,
                                CollectorState::Draining,
                                &self.metrics,
                            ).await;
                            let _ = effect_handler.notify_receiver_drained().await;
                            await_tasks(flight, admin, registration, remaining).await;
                            return Ok(TerminalState::default());
                        }
                        Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                            self.metrics.set_accepting(false);
                            shutdown.cancel();
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            drain_streams(&drain, remaining, &mut control, &admission).await;
                            finalize_registration(
                                &self.routing,
                                instance_id,
                                CollectorState::Draining,
                                &self.metrics,
                            ).await;
                            await_tasks(flight, admin, registration, remaining).await;
                            return Ok(TerminalState::default());
                        }
                        Ok(_) => {}
                        Err(error) => {
                            shutdown.cancel();
                            self.metrics.set_healthy(false);
                            return Err(Error::ChannelRecvError(error));
                        }
                    }
                }
                result = &mut flight => {
                    shutdown.cancel();
                    self.metrics.set_healthy(false);
                    return Err(receiver_error(
                        &effect_handler,
                        ReceiverErrorKind::Transport,
                        task_result_message("Flight server", result),
                    ));
                }
                result = &mut admin => {
                    shutdown.cancel();
                    self.metrics.set_healthy(false);
                    return Err(receiver_error(
                        &effect_handler,
                        ReceiverErrorKind::Transport,
                        task_result_message("admin server", result),
                    ));
                }
            }
        }
    }
}

fn correlation_id(call_data: &CallData) -> Option<u64> {
    call_data.first().copied().map(u64::from)
}

/// Waits for admitted Flight streams to finish while still resolving DFE feedback.
///
/// Durable-admission streams complete only after their acknowledgements arrive
/// on this control channel, so draining keeps consuming Ack/Nack messages
/// instead of blocking the control loop until the drain deadline.
async fn drain_streams(
    drain: &DrainController,
    timeout: Duration,
    control: &mut receiver::ControlChannel<OtapPdata>,
    admission: &DfeAdmission,
) {
    let stop = drain.graceful_stop(timeout);
    tokio::pin!(stop);
    loop {
        tokio::select! {
            outcome = &mut stop => {
                record_drain_outcome(outcome);
                return;
            },
            message = control.recv() => match message {
                Ok(NodeControlMsg::Ack(ack)) => {
                    if let Some(id) = correlation_id(&ack.unwind.route.calldata) {
                        admission.resolve_feedback(id, true);
                    }
                }
                Ok(NodeControlMsg::Nack(nack)) => {
                    if let Some(id) = correlation_id(&nack.unwind.route.calldata) {
                        admission.resolve_feedback(id, false);
                    }
                }
                Ok(_) => {}
                Err(_) => {
                    record_drain_outcome((&mut stop).await);
                    return;
                }
            },
        }
    }
}

fn record_drain_outcome(outcome: DrainOutcome) {
    if !matches!(outcome, DrainOutcome::Drained) {
        crate::shutdown::PROCESS_ROUTING_SHUTDOWN.record_failure();
        tracing::error!(?outcome, "dataflow ingress did not drain cleanly");
    }
}

fn allow_all_authenticator() -> Result<AllowAllAuthenticator, String> {
    let principal = AuthenticatedPrincipal::new(
        PrincipalId::new("local-development").map_err(|error| error.to_string())?,
        PrincipalIssuer::new("arrowcol-dataflow").map_err(|error| error.to_string())?,
        BTreeSet::new(),
    )
    .map_err(|error| error.to_string())?;
    let authenticated_at = now_unix_millis()?;
    let context = AuthContext::new(principal, AuthMethod::WorkloadIdentity, authenticated_at, None)
        .map_err(|error| error.to_string())?;
    Ok(AllowAllAuthenticator::new(context))
}

fn receiver_error(
    effect_handler: &receiver::EffectHandler<OtapPdata>,
    kind: ReceiverErrorKind,
    error: impl Into<String>,
) -> Error {
    Error::ReceiverError {
        receiver: effect_handler.receiver_id(),
        kind,
        error: error.into(),
        source_detail: String::new(),
    }
}

fn task_result_message<E>(name: &str, result: Result<Result<(), E>, tokio::task::JoinError>) -> String
where
    E: std::fmt::Display,
{
    match result {
        Ok(Ok(())) => format!("{name} stopped before receiver shutdown"),
        Ok(Err(error)) => format!("{name} failed: {error}"),
        Err(error) => format!("{name} task failed: {error}"),
    }
}

async fn await_tasks(
    mut flight: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    mut admin: tokio::task::JoinHandle<Result<(), std::io::Error>>,
    mut registration: tokio::task::JoinHandle<()>,
    timeout: Duration,
) {
    let wait = async {
        let flight = (&mut flight).await;
        let admin = (&mut admin).await;
        let registration = (&mut registration).await;
        if !matches!(flight, Ok(Ok(()))) || !matches!(admin, Ok(Ok(()))) || registration.is_err() {
            crate::shutdown::PROCESS_ROUTING_SHUTDOWN.record_failure();
            tracing::error!("dataflow receiver shutdown task failed");
        }
    };
    if tokio::time::timeout(timeout, wait).await.is_err() {
        crate::shutdown::PROCESS_ROUTING_SHUTDOWN.record_failure();
        tracing::error!("dataflow receiver shutdown tasks timed out");
        flight.abort();
        admin.abort();
        registration.abort();
    }
}

async fn register_collector(
    store: &Arc<dyn RoutingStore>,
    config: &FlightReceiverConfig,
    instance_id: CollectorInstanceId,
    metrics: &Arc<RuntimeMetrics>,
) -> Result<(), String> {
    let now = now_unix_millis()?;
    let expiry = checked_add_millis(now, config.registration.registration_ttl)?;
    store
        .register_collector(&CollectorRegistration {
            instance_id,
            instance_key: config.registration.instance_key.clone(),
            implementation_pool: ImplementationPool::Dataflow,
            flight_endpoint: config.registration.flight_endpoint,
            catalog_capable: true,
            max_concurrent_streams: PositiveMeasurement::new(
                u64::try_from(config.registration.max_concurrent_streams)
                    .map_err(|_| "max concurrent streams exceeds u64".to_owned())?,
            )
            .map_err(|error| error.to_string())?,
            started_at_unix_ms: now,
            heartbeat_at_unix_ms: now,
            lease_expires_at_unix_ms: expiry,
        })
        .await
        .map_err(|error| error.to_string())?;
    crate::shutdown::PROCESS_ROUTING_SHUTDOWN
        .register(Arc::clone(store), instance_id)
        .map_err(|error| error.to_string())?;
    metrics.publish_pressure(store.as_ref()).await?;
    metrics.set_registered(true);
    Ok(())
}

async fn registration_loop(
    store: Arc<dyn RoutingStore>,
    config: FlightReceiverConfig,
    instance_id: CollectorInstanceId,
    metrics: Arc<RuntimeMetrics>,
    shutdown: CancellationToken,
) {
    let started = now_unix_millis().ok();
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + config.registration.heartbeat_interval,
        config.registration.heartbeat_interval,
    );
    let mut pressure = tokio::time::interval_at(
        tokio::time::Instant::now() + config.registration.pressure_interval,
        config.registration.pressure_interval,
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    pressure.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = heartbeat.tick() => {
                let Some(started) = started else {
                    metrics.set_registered(false);
                    continue;
                };
                let Ok(now) = now_unix_millis() else {
                    metrics.set_registered(false);
                    continue;
                };
                let Ok(expiry) = checked_add_millis(now, config.registration.registration_ttl) else {
                    metrics.set_registered(false);
                    continue;
                };
                let registration = CollectorRegistration {
                    instance_id,
                    instance_key: config.registration.instance_key.clone(),
                    implementation_pool: ImplementationPool::Dataflow,
                    flight_endpoint: config.registration.flight_endpoint,
                    catalog_capable: true,
                    max_concurrent_streams: match PositiveMeasurement::new(
                        u64::try_from(config.registration.max_concurrent_streams).unwrap_or(u64::MAX)
                    ) {
                        Ok(value) => value,
                        Err(_) => {
                            metrics.set_registered(false);
                            continue;
                        }
                    },
                    started_at_unix_ms: started,
                    heartbeat_at_unix_ms: now,
                    lease_expires_at_unix_ms: expiry,
                };
                metrics.set_registered(store.register_collector(&registration).await.is_ok());
            }
            _ = pressure.tick() => {
                if metrics.publish_pressure(store.as_ref()).await.is_err() {
                    metrics.set_registered(false);
                }
            }
        }
    }
}

async fn finalize_registration(
    store: &Arc<dyn RoutingStore>,
    instance_id: CollectorInstanceId,
    state: CollectorState,
    metrics: &Arc<RuntimeMetrics>,
) {
    let result = match now_unix_millis() {
        Ok(now) => store
            .set_collector_state(instance_id, state, now)
            .await
            .map_err(|error| error.to_string()),
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        crate::shutdown::PROCESS_ROUTING_SHUTDOWN.record_failure();
        tracing::error!(%error, "dataflow could not persist ingress drain state");
    }
    if metrics.publish_pressure(store.as_ref()).await.is_err() {
        metrics.set_registered(false);
    }
}

#[derive(Clone)]
struct AdminState {
    metrics: Arc<RuntimeMetrics>,
    max_request_bytes: u64,
}

fn admin_router(state: AdminState) -> Router {
    Router::new()
        .route("/healthz", get(admin_health))
        .route("/readyz", get(admin_readiness))
        .route("/v1/capabilities", get(admin_capabilities))
        .route("/v1/pressure", get(admin_pressure))
        .route("/metrics", get(admin_metrics))
        .with_state(state)
}

#[derive(Serialize)]
struct HealthDocument {
    healthy: bool,
}

async fn admin_health(State(state): State<AdminState>) -> impl IntoResponse {
    let healthy = state.metrics.healthy();
    (
        if healthy {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(HealthDocument { healthy }),
    )
}

#[derive(Serialize)]
struct ReadinessDocument {
    ready: bool,
    accepting_deliveries: bool,
    delivery_guarantee: &'static str,
    receipt_polling: bool,
}

async fn admin_readiness(State(state): State<AdminState>) -> impl IntoResponse {
    let ready = state.metrics.ready();
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(ReadinessDocument {
            ready,
            accepting_deliveries: ready,
            delivery_guarantee: state.metrics.delivery_guarantee(),
            receipt_polling: false,
        }),
    )
}

async fn admin_capabilities(State(state): State<AdminState>) -> impl IntoResponse {
    Json(state.metrics.capabilities(state.max_request_bytes))
}

async fn admin_pressure(State(state): State<AdminState>) -> Response {
    match state.metrics.pressure() {
        Ok(pressure) => Json(pressure).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn admin_metrics(State(state): State<AdminState>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.prometheus(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::sync::Mutex;

    use anyhow::Result;
    use arrow_array::{ArrayRef, Int32Array, Int64Array, StringArray, UInt64Array};
    use arrow_flight::encode::FlightDataEncoderBuilder;
    use arrow_flight::error::FlightError;
    use arrow_flight::flight_descriptor::DescriptorType;
    use arrow_flight::{FlightData, FlightDescriptor};
    use arrow_schema::{DataType, Field, Schema};
    use arrowcol_catalog::{
        CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, MemoryCatalogStore, MemoryMetastore,
    };
    use arrowcol_flight::{DeliveryGuarantee, PutResultMetadata};
    use arrowcol_protocol::{
        BasisPoints, CatalogName, ClaimedSchemaFingerprint, IngestMetadata, Measurement, PressureGauge,
        ProtocolVersion, SchemaName, TableName, UnixMillis, fingerprint_schema,
    };
    use arrowcol_routing::{
        CollectorRegistration, LeaseClaim, LeaseKey, MemoryRoutingStore, RichPressureSnapshot, RoutingStore,
        SubsystemState,
    };
    use futures::{FutureExt, TryStreamExt, stream};
    use tonic::Request;
    use tonic::metadata::MetadataValue;

    use super::*;

    struct CaptureSender {
        sent: Mutex<Vec<(u64, OtapPdata)>>,
        gate: Option<Arc<Semaphore>>,
    }

    #[async_trait]
    impl PdataSender for CaptureSender {
        async fn send(&self, correlation_id: u64, pdata: OtapPdata) -> Result<(), PdataSendError> {
            let _permit = match &self.gate {
                Some(gate) => Some(
                    Arc::clone(gate)
                        .acquire_owned()
                        .await
                        .map_err(|_| PdataSendError::Closed)?,
                ),
                None => None,
            };
            self.sent.lock().expect("capture lock").push((correlation_id, pdata));
            Ok(())
        }
    }

    struct FailingSender;

    #[async_trait]
    impl PdataSender for FailingSender {
        async fn send(&self, _correlation_id: u64, _pdata: OtapPdata) -> Result<(), PdataSendError> {
            Err(PdataSendError::Unavailable)
        }
    }

    fn envelope() -> ArrowcolBatchEnvelope {
        ArrowcolBatchEnvelope {
            publication_id: Some(arrowcol_protocol::ReceiptId::new()),
            ingested_at_ms: Some(arrowcol_protocol::UnixMillis::new(1_700_000_012_345).expect("stamp")),
            table: arrowcol_protocol::TableIdentity::new(
                CatalogName::new("demo").expect("catalog"),
                SchemaName::new("generated").expect("schema"),
                TableName::new("events").expect("table"),
            ),
            source_schema_version: arrowcol_catalog::SchemaVersion::FIRST,
            target_schema_version: arrowcol_catalog::SchemaVersion::FIRST,
            lease_id: arrowcol_protocol::LeaseId::new(),
            holder_instance_id: CollectorInstanceId::new(),
            fencing_token: arrowcol_protocol::FencingToken::new(1).expect("fence"),
            instance_key: "dfe-flight-test".to_owned(),
        }
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![1])) as ArrayRef]).expect("batch")
    }

    async fn flight_request(
        identity: &arrowcol_protocol::TableIdentity,
        version: arrowcol_catalog::SchemaVersion,
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> Result<Request<impl futures::Stream<Item = Result<FlightData, tonic::Status>> + Send + 'static>> {
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
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(Arc::clone(&schema))
            .with_flight_descriptor(Some(descriptor))
            .build(stream::iter(batches.into_iter().map(Ok::<RecordBatch, FlightError>)))
            .try_collect::<Vec<_>>()
            .await?;
        let mut request = Request::new(stream::iter(encoded.into_iter().map(Ok)));
        let metadata = IngestMetadata::new(
            ProtocolVersion::CURRENT,
            ImplementationPool::Dataflow,
            identity.clone(),
            ClaimedSchemaFingerprint::new(fingerprint_schema(schema.as_ref())),
        );
        for (key, value) in metadata.to_ascii_pairs() {
            request
                .metadata_mut()
                .insert(key, MetadataValue::try_from(value.as_str())?);
        }
        Ok(request)
    }

    async fn routing_lease(
        store: &Arc<MemoryRoutingStore>,
        instance_id: CollectorInstanceId,
        table: &arrowcol_protocol::TableIdentity,
    ) -> Result<()> {
        let now = now_unix_millis().map_err(anyhow::Error::msg)?;
        store
            .register_collector(&CollectorRegistration {
                instance_id,
                instance_key: "dfe-flight-test".to_owned(),
                implementation_pool: ImplementationPool::Dataflow,
                flight_endpoint: "127.0.0.1:32010".parse()?,
                catalog_capable: true,
                max_concurrent_streams: PositiveMeasurement::new(2)?,
                started_at_unix_ms: now,
                heartbeat_at_unix_ms: now,
                lease_expires_at_unix_ms: UnixMillis::new(now.get() + 60_000)?,
            })
            .await?;
        let zero = PressureGauge::new(Measurement::ZERO, PositiveMeasurement::new(2)?)?;
        store
            .publish_pressure(&RichPressureSnapshot {
                instance_id,
                observed_at_unix_ms: now,
                cpu_utilization_bps: BasisPoints::new(0)?,
                rss_bytes: Measurement::ZERO,
                rss_limit_bytes: PositiveMeasurement::new(1)?,
                active_streams: zero,
                received_bytes: Measurement::ZERO,
                received_rows: Measurement::ZERO,
                received_batches: Measurement::ZERO,
                buffered_bytes: zero,
                buffered_rows: zero,
                buffered_batches: zero,
                available_permits: PressureGauge::new(Measurement::new(2)?, PositiveMeasurement::new(2)?)?,
                pending_commits: zero,
                oldest_buffer_age_ms: Measurement::ZERO,
                delta_pressure_bps: BasisPoints::new(0)?,
                quiver_pressure_bps: BasisPoints::new(0)?,
                dfe_pressure_bps: BasisPoints::new(0)?,
                delta_state: SubsystemState::Ready,
                quiver_state: SubsystemState::Disabled,
                dfe_state: SubsystemState::Ready,
                accepting_deliveries: true,
                tables: Vec::new(),
            })
            .await?;
        store
            .claim_lease(&LeaseClaim {
                key: LeaseKey::new(ImplementationPool::Dataflow, table.clone()),
                candidate_instance_id: instance_id,
                now_unix_ms: now,
                expires_at_unix_ms: UnixMillis::new(now.get() + 60_000)?,
                minimum_snapshot_at_unix_ms: now,
            })
            .await?;
        Ok(())
    }

    /// Scenario: A bounded DFE sender is saturated.
    /// Guarantees: The send future remains backpressured rather than dropping or bypassing capacity.
    #[tokio::test]
    async fn sender_backpressure_is_awaited() {
        let gate = Arc::new(Semaphore::new(0));
        let sender = Arc::new(CaptureSender {
            sent: Mutex::new(Vec::new()),
            gate: Some(Arc::clone(&gate)),
        });
        let records = crate::encode_record_batch(envelope(), batch()).expect("records");
        let pdata = OtapPdata::new_todo_context(OtapPayload::from_pdata_arrow(records));
        let send = sender.send(1, pdata);
        assert!(tokio::time::timeout(Duration::from_millis(20), send).await.is_err());
        gate.add_permits(1);
    }

    #[tokio::test]
    async fn concurrent_core_registration_and_pressure_writes_are_ordered() {
        let holder = CollectorInstanceId::new();
        let metrics = Arc::new(RuntimeMetrics::new(holder));
        metrics.configure_receiver(4, 1_000_000_000, 1_000_000, 1_000, 10, 10);
        let config: FlightReceiverConfig = serde_json::from_value(serde_json::json!({
            "listening_addr": "127.0.0.1:32010",
            "admin_listening_addr": "127.0.0.1:32011",
            "catalog": {"type": "memory"},
            "routing": {"type": "memory"},
            "registration": {
                "instance_key": "concurrent-cores",
                "flight_endpoint": "127.0.0.1:32010"
            }
        }))
        .expect("config");
        let store: Arc<dyn RoutingStore> = Arc::new(MemoryRoutingStore::default());
        let (first, second) = tokio::join!(
            register_collector(&store, &config, holder, &metrics),
            register_collector(&store, &config, holder, &metrics)
        );
        first.expect("first core");
        second.expect("second core");
        for result in futures::future::join_all((0..64).map(|_| metrics.publish_pressure(store.as_ref()))).await {
            result.expect("strictly advancing publication");
        }
        assert!(metrics.healthy());
    }

    /// Scenario: A downstream DFE route fails after a Flight batch was decoded.
    /// Guarantees: The Flight lifecycle returns a capacity failure and removes the pressure entry.
    #[tokio::test]
    async fn downstream_failure_propagates_without_false_admission() {
        let metrics = Arc::new(RuntimeMetrics::new(CollectorInstanceId::new()));
        metrics.configure_receiver(1, 1_000_000, 1_000_000, 100, 10, 1);
        metrics.configure_exporter(1);
        let pending = Arc::new(PendingTracker::new(Arc::clone(&metrics)));
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&semaphore).acquire_owned().await.expect("permit");
        let envelope = envelope();
        metrics.stream_started(&envelope.table);
        let mut lifecycle = DfeStreamLifecycle {
            sender: Arc::new(FailingSender),
            pending,
            metrics: Arc::clone(&metrics),
            envelope,
            permit: Some(permit),
            finished: false,
            durable: None,
        };

        let error = lifecycle.push_batch(batch()).await.expect_err("send failure");
        assert_eq!(error.code(), ErrorCode::CapacityExhausted);
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.buffered_batches.current().get(), 0);
    }

    /// Scenario: Flight admission is cancelled while a bounded DFE route is backpressured.
    /// Guarantees: The cancelled send removes its pending pressure entry instead of blocking drain forever.
    #[tokio::test]
    async fn cancelled_backpressured_send_releases_pending_pressure() {
        let gate = Arc::new(Semaphore::new(0));
        let metrics = Arc::new(RuntimeMetrics::new(CollectorInstanceId::new()));
        metrics.configure_receiver(1, 1_000_000, 1_000_000, 100, 10, 1);
        metrics.configure_exporter(1);
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&semaphore).acquire_owned().await.expect("permit");
        let envelope = envelope();
        metrics.stream_started(&envelope.table);
        let mut lifecycle = DfeStreamLifecycle {
            sender: Arc::new(CaptureSender {
                sent: Mutex::new(Vec::new()),
                gate: Some(gate),
            }),
            pending: Arc::new(PendingTracker::new(Arc::clone(&metrics))),
            metrics: Arc::clone(&metrics),
            envelope,
            permit: Some(permit),
            finished: false,
            durable: None,
        };

        let send = lifecycle.push_batch(batch());
        assert!(tokio::time::timeout(Duration::from_millis(20), send).await.is_err());
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.buffered_batches.current().get(), 0);
        Box::new(lifecycle).abort(StreamAbortReason::ClientCancelled).await;
    }

    /// Scenario: Flight cancellation aborts an admitted stream before completion.
    /// Guarantees: The stream permit and active-stream pressure are released exactly once.
    #[tokio::test]
    async fn cancellation_releases_stream_capacity() {
        let metrics = Arc::new(RuntimeMetrics::new(CollectorInstanceId::new()));
        metrics.configure_receiver(1, 1_000_000, 1_000_000, 100, 10, 1);
        metrics.configure_exporter(1);
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&semaphore).acquire_owned().await.expect("permit");
        let envelope = envelope();
        metrics.stream_started(&envelope.table);
        let lifecycle = DfeStreamLifecycle {
            sender: Arc::new(CaptureSender {
                sent: Mutex::new(Vec::new()),
                gate: None,
            }),
            pending: Arc::new(PendingTracker::new(Arc::clone(&metrics))),
            metrics: Arc::clone(&metrics),
            envelope,
            permit: Some(permit),
            finished: false,
            durable: None,
        };

        Box::new(lifecycle).abort(StreamAbortReason::ClientCancelled).await;
        assert_eq!(semaphore.available_permits(), 1);
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.active_streams.current().get(), 0);
    }

    /// Scenario: Flight receives an exact arbitrary Arrow schema on an actively leased table.
    /// Guarantees: The batch enters Arrowcol pdata and the final PutResult is explicitly best-effort without a receipt.
    #[tokio::test]
    async fn exact_flight_ingest_returns_best_effort_pdata() -> Result<()> {
        let catalog = Arc::new(MemoryCatalogStore::new());
        let metastore = Arc::new(MemoryMetastore::new());
        let control = CatalogControlPlane::new(
            DdlParser::default(),
            Arc::clone(&catalog) as Arc<dyn CatalogStore>,
            metastore,
        );
        let ddl = control
            .execute_ddl(arrowcol_catalog::FlightSqlDdlRequest::new(
                "CREATE TABLE demo.generated.events (id BIGINT NOT NULL, body VARCHAR)",
            ))
            .await?;
        let current = catalog
            .current_schema(ddl.identity())
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing schema"))?;
        let schema = Arc::new(current.arrow_schema()?);
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("one"), None])) as ArrayRef,
            ],
        )?;
        let routing = Arc::new(MemoryRoutingStore::default());
        let instance_id = CollectorInstanceId::new();
        routing_lease(&routing, instance_id, ddl.identity()).await?;
        let sender = Arc::new(CaptureSender {
            sent: Mutex::new(Vec::new()),
            gate: None,
        });
        let metrics = Arc::new(RuntimeMetrics::new(instance_id));
        metrics.configure_receiver(2, 1_000_000_000, 1_000_000, 10_000, 16, 2);
        metrics.configure_exporter(2);
        let admission = Arc::new(DfeAdmission::new(
            Arc::clone(&sender) as Arc<dyn PdataSender>,
            Arc::new(StoreLeaseResolver::new(
                Arc::clone(&routing) as Arc<dyn RoutingStore>,
                instance_id,
            )),
            instance_id,
            "dfe-flight-test",
            2,
            metrics,
        ));
        let service = ArrowColFlightService::new(
            ImplementationPool::Dataflow,
            catalog as Arc<dyn CatalogStore>,
            Arc::new(allow_all_authenticator().map_err(anyhow::Error::msg)?),
            admission as Arc<dyn BatchAdmission>,
            arrowcol_flight::RuntimeLimits::default(),
        )?;
        let results = service
            .handle_do_put(flight_request(ddl.identity(), current.version(), Arc::clone(&schema), vec![input]).await?)
            .await?
            .into_inner()
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(results.len(), 1);
        let completion = PutResultMetadata::from_put_result(&results[0])?;
        assert_eq!(completion.delivery_guarantee(), Some(DeliveryGuarantee::BestEffort));
        assert_eq!(completion.receipt_id(), None);
        let captured = sender.sent.lock().expect("capture lock");
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].1.signal_format(),
            otel_arrow_dfe_config::SignalFormat::PluggableArrowRecords
        );
        Ok(())
    }

    /// Scenario: Flight receives an unregistered breaking Arrow schema.
    /// Guarantees: Exact catalog validation rejects it before any DFE sender side effect.
    #[tokio::test]
    async fn breaking_flight_schema_is_rejected_before_dfe() -> Result<()> {
        let catalog = Arc::new(MemoryCatalogStore::new());
        let metastore = Arc::new(MemoryMetastore::new());
        let control = CatalogControlPlane::new(
            DdlParser::default(),
            Arc::clone(&catalog) as Arc<dyn CatalogStore>,
            metastore,
        );
        let ddl = control
            .execute_ddl(arrowcol_catalog::FlightSqlDdlRequest::new(
                "CREATE TABLE demo.generated.events (id BIGINT NOT NULL)",
            ))
            .await?;
        let current = catalog
            .current_schema(ddl.identity())
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing schema"))?;
        let routing = Arc::new(MemoryRoutingStore::default());
        let instance_id = CollectorInstanceId::new();
        routing_lease(&routing, instance_id, ddl.identity()).await?;
        let sender = Arc::new(CaptureSender {
            sent: Mutex::new(Vec::new()),
            gate: None,
        });
        let metrics = Arc::new(RuntimeMetrics::new(instance_id));
        metrics.configure_receiver(2, 1_000_000_000, 1_000_000, 10_000, 16, 2);
        let admission = Arc::new(DfeAdmission::new(
            Arc::clone(&sender) as Arc<dyn PdataSender>,
            Arc::new(StoreLeaseResolver::new(
                Arc::clone(&routing) as Arc<dyn RoutingStore>,
                instance_id,
            )),
            instance_id,
            "dfe-flight-test",
            2,
            metrics,
        ));
        let service = ArrowColFlightService::new(
            ImplementationPool::Dataflow,
            catalog as Arc<dyn CatalogStore>,
            Arc::new(allow_all_authenticator().map_err(anyhow::Error::msg)?),
            admission as Arc<dyn BatchAdmission>,
            arrowcol_flight::RuntimeLimits::default(),
        )?;
        let bad_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let bad_batch = RecordBatch::try_new(
            Arc::clone(&bad_schema),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )?;
        let status = match service
            .handle_do_put(flight_request(ddl.identity(), current.version(), bad_schema, vec![bad_batch]).await?)
            .await
        {
            Ok(_) => return Err(anyhow::anyhow!("schema unexpectedly reached DFE")),
            Err(status) => status,
        };
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(sender.sent.lock().expect("capture lock").is_empty());
        Ok(())
    }

    /// Scenario: `native_quiver: true` is configured on `receiver:arrow_flight`.
    /// Guarantees: Strict validation accepts durable admission now that the durable buffer persists
    /// Arrowcol pdata, while unknown receiver fields are still rejected.
    #[test]
    fn native_quiver_config_is_accepted() {
        let mut value = serde_json::json!({
            "listening_addr": "127.0.0.1:32010",
            "admin_listening_addr": "127.0.0.1:32011",
            "catalog": {"type": "memory"},
            "routing": {"type": "memory"},
            "registration": {
                "instance_key": "test",
                "flight_endpoint": "127.0.0.1:32010"
            },
            "native_quiver": true
        });
        validate_receiver_config(&value).expect("durable admission is supported");
        value["native_quiver_gap"] = serde_json::json!(true);
        assert!(validate_receiver_config(&value).is_err());
    }

    fn durable_metrics(native_quiver: bool) -> Arc<RuntimeMetrics> {
        let metrics = Arc::new(RuntimeMetrics::new(CollectorInstanceId::new()));
        metrics.configure_receiver(4, u64::MAX / 4, 1_000_000, 1_000, 10, 4);
        metrics.configure_exporter(4);
        metrics
            .configure_admission("dfe-flight-test", native_quiver)
            .expect("admission mode");
        metrics
    }

    async fn durable_lifecycle(
        metrics: &Arc<RuntimeMetrics>,
        sender: Arc<dyn PdataSender>,
        timeout: Option<Duration>,
    ) -> (DfeStreamLifecycle, Arc<PendingTracker>, Arc<Semaphore>) {
        let pending = Arc::new(PendingTracker::new(Arc::clone(metrics)));
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&semaphore).acquire_owned().await.expect("permit");
        let envelope = envelope();
        metrics.stream_started(&envelope.table);
        let lifecycle = DfeStreamLifecycle {
            sender,
            pending: Arc::clone(&pending),
            metrics: Arc::clone(metrics),
            envelope,
            permit: Some(permit),
            finished: false,
            durable: timeout.map(DurableAdmission::new),
        };
        (lifecycle, pending, semaphore)
    }

    fn capture_sender() -> Arc<CaptureSender> {
        Arc::new(CaptureSender {
            sent: Mutex::new(Vec::new()),
            gate: None,
        })
    }

    fn sent_ids(sender: &CaptureSender) -> Vec<u64> {
        sender
            .sent
            .lock()
            .expect("capture lock")
            .iter()
            .map(|(id, _)| *id)
            .collect()
    }

    fn summary() -> StreamSummary {
        StreamSummary::new(2, 2, 16)
    }

    /// Scenario: A durable-admission stream pushed two batches and ends before the durable buffer
    /// acknowledged them.
    /// Guarantees: Completion stays pending until every correlated ACK arrives, then reports
    /// `durable_admission`, and pressure moves both batches from in-memory DFE work into the durable backlog.
    #[tokio::test]
    async fn durable_completion_waits_for_every_ack() {
        let metrics = durable_metrics(true);
        let sender = capture_sender();
        let (mut lifecycle, pending, semaphore) = durable_lifecycle(
            &metrics,
            Arc::clone(&sender) as Arc<dyn PdataSender>,
            Some(Duration::from_secs(5)),
        )
        .await;
        lifecycle.push_batch(batch()).await.expect("first batch");
        lifecycle.push_batch(batch()).await.expect("second batch");
        let ids = sent_ids(&sender);
        assert_eq!(ids.len(), 2);
        let envelopes = sender
            .sent
            .lock()
            .expect("capture lock")
            .drain(..)
            .map(|(_, pdata)| {
                let (_, payload) = pdata.into_parts();
                let otel_arrow_dfe_pdata::PayloadData::PdataArrowRecords(records) = payload.into_data() else {
                    panic!("expected pluggable Arrow records");
                };
                crate::decode_record_set(*records)
                    .expect("decode")
                    .pop()
                    .expect("one batch")
                    .envelope
            })
            .collect::<Vec<_>>();
        assert!(envelopes.iter().all(|envelope| envelope.ingested_at_ms.is_some()));
        assert!(envelopes.iter().all(|envelope| envelope.publication_id.is_some()));
        assert_ne!(envelopes[0].publication_id, envelopes[1].publication_id);
        {
            let mut completion = lifecycle.complete(summary());
            assert!((&mut completion).now_or_never().is_none());
            pending.resolve(ids[0], true);
            assert!((&mut completion).now_or_never().is_none());
            pending.resolve(ids[1], true);
            let completion = tokio::time::timeout(Duration::from_secs(1), completion)
                .await
                .expect("bounded completion")
                .expect("durable admission");
            assert_eq!(completion, StreamCompletion::durable_admission());
        }
        drop(lifecycle);
        assert_eq!(semaphore.available_permits(), 1);

        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.buffered_batches.current().get(), 2);
        assert_eq!(pressure.rich.dfe_pressure_bps.get(), 0);
        assert_eq!(pressure.rich.quiver_pressure_bps.get(), 2_000);
        assert_eq!(pressure.rich.quiver_state, SubsystemState::Ready);
        assert_eq!(pressure.rich.tables[0].buffered_batches.get(), 2);
        let encoded = serde_json::to_value(&pressure).expect("pressure JSON");
        assert_eq!(encoded["dfe_channel_occupancy"], 0);
        assert_eq!(
            encoded["dfe_channel_occupancy_source"],
            "ack_tracked_until_durable_admission"
        );
        assert_eq!(encoded["native_quiver_buffered_batches"], 2);
        assert!(
            metrics
                .prometheus()
                .contains("arrowcol_dataflow_durable_admitted_batches_total 2\n")
        );
    }

    /// Scenario: The durable buffer acknowledges one batch of a stream and rejects the other.
    /// Guarantees: Completion fails with a retryable UNAVAILABLE error, later pushes fail fast without
    /// reaching DFE, and only the acknowledged batch enters the durable backlog.
    #[tokio::test]
    async fn durable_nack_fails_stream_retryably() {
        let metrics = durable_metrics(true);
        let sender = capture_sender();
        let (mut lifecycle, pending, semaphore) = durable_lifecycle(
            &metrics,
            Arc::clone(&sender) as Arc<dyn PdataSender>,
            Some(Duration::from_secs(5)),
        )
        .await;
        lifecycle.push_batch(batch()).await.expect("first batch");
        lifecycle.push_batch(batch()).await.expect("second batch");
        let ids = sent_ids(&sender);
        pending.resolve(ids[0], true);
        pending.resolve(ids[1], false);

        let error = lifecycle.complete(summary()).await.expect_err("rejected batch");
        assert_eq!(error.code(), ErrorCode::CapacityExhausted);
        assert_eq!(error.grpc_code(), tonic::Code::Unavailable);
        assert!(error.code().is_retryable());
        assert!(error.public_message().contains("durable buffer rejected"));
        let error = lifecycle.push_batch(batch()).await.expect_err("fail fast");
        assert_eq!(error.code(), ErrorCode::CapacityExhausted);
        assert_eq!(sent_ids(&sender).len(), 2);

        Box::new(lifecycle).abort(StreamAbortReason::AdmissionFailure).await;
        assert_eq!(semaphore.available_permits(), 1);
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.active_streams.current().get(), 0);
        assert_eq!(pressure.rich.buffered_batches.current().get(), 1);
        assert!(
            metrics
                .prometheus()
                .contains("arrowcol_dataflow_durable_admission_failures_total 1\n")
        );
    }

    /// Scenario: The durable buffer never acknowledges a pushed batch before the admission timeout.
    /// Guarantees: Completion fails with a retryable UNAVAILABLE timeout, the batch stays in-flight DFE
    /// work until feedback arrives, and a late ACK still moves it into the durable backlog.
    #[tokio::test]
    async fn durable_timeout_fails_stream_retryably() {
        let metrics = durable_metrics(true);
        let sender = capture_sender();
        let (mut lifecycle, pending, _semaphore) = durable_lifecycle(
            &metrics,
            Arc::clone(&sender) as Arc<dyn PdataSender>,
            Some(Duration::from_millis(100)),
        )
        .await;
        lifecycle.push_batch(batch()).await.expect("batch");
        let error = tokio::time::timeout(Duration::from_secs(2), lifecycle.complete(summary()))
            .await
            .expect("bounded by the admission timeout")
            .expect_err("timeout");
        assert_eq!(error.code(), ErrorCode::BackpressureTimeout);
        assert_eq!(error.grpc_code(), tonic::Code::Unavailable);
        assert!(error.code().is_retryable());
        Box::new(lifecycle).abort(StreamAbortReason::AdmissionFailure).await;
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.dfe_pressure_bps.get(), 1_000);
        assert_eq!(pressure.rich.quiver_pressure_bps.get(), 0);

        pending.resolve(sent_ids(&sender)[0], true);
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.dfe_pressure_bps.get(), 0);
        assert_eq!(pressure.rich.buffered_batches.current().get(), 1);
        assert_eq!(pressure.rich.quiver_pressure_bps.get(), 1_000);
    }

    /// Scenario: Native Quiver is disabled and DFE feedback is still outstanding when a stream ends.
    /// Guarantees: Completion returns `best_effort_admission` immediately, exactly as before durable
    /// admission existed, and a later ACK never creates a durable backlog.
    #[tokio::test]
    async fn best_effort_completion_is_unchanged() {
        let metrics = durable_metrics(false);
        let sender = capture_sender();
        let (mut lifecycle, pending, semaphore) =
            durable_lifecycle(&metrics, Arc::clone(&sender) as Arc<dyn PdataSender>, None).await;
        lifecycle.push_batch(batch()).await.expect("batch");
        let completion = lifecycle
            .complete(summary())
            .now_or_never()
            .expect("immediate completion")
            .expect("best effort");
        assert_eq!(completion, StreamCompletion::best_effort());
        drop(lifecycle);
        assert_eq!(semaphore.available_permits(), 1);
        assert_eq!(
            metrics
                .pressure()
                .expect("pressure")
                .rich
                .buffered_batches
                .current()
                .get(),
            1
        );

        pending.resolve(sent_ids(&sender)[0], true);
        let pressure = metrics.pressure().expect("pressure");
        assert_eq!(pressure.rich.buffered_batches.current().get(), 0);
        assert_eq!(pressure.rich.quiver_state, SubsystemState::Disabled);
        assert!(
            metrics
                .prometheus()
                .contains("arrowcol_dataflow_durable_backlog_batches 0\n")
        );
    }

    /// Scenario: Durable-admission streams are aborted while one batch awaits durable-buffer feedback
    /// and another send is still backpressured.
    /// Guarantees: The cancelled send releases its pending entry immediately, the late ACK releases the
    /// other, and neither leaves a per-stream waiter or pending entry behind.
    #[tokio::test]
    async fn aborted_durable_streams_release_waiters() {
        let metrics = durable_metrics(true);
        let sender = capture_sender();
        let (mut lifecycle, pending, semaphore) = durable_lifecycle(
            &metrics,
            Arc::clone(&sender) as Arc<dyn PdataSender>,
            Some(Duration::from_secs(5)),
        )
        .await;
        lifecycle.push_batch(batch()).await.expect("batch");
        let stream = Arc::clone(&lifecycle.durable.as_ref().expect("durable").stream);
        assert_eq!(Arc::strong_count(&stream), 3);
        Box::new(lifecycle).abort(StreamAbortReason::ClientCancelled).await;
        assert_eq!(semaphore.available_permits(), 1);
        assert_eq!(Arc::strong_count(&stream), 2);
        pending.resolve(sent_ids(&sender)[0], true);
        assert_eq!(Arc::strong_count(&stream), 1);
        assert!(pending.pending_ids().is_empty());

        let gate = Arc::new(Semaphore::new(0));
        let (mut blocked, pending, _semaphore) = durable_lifecycle(
            &metrics,
            Arc::new(CaptureSender {
                sent: Mutex::new(Vec::new()),
                gate: Some(gate),
            }),
            Some(Duration::from_secs(5)),
        )
        .await;
        let stream = Arc::clone(&blocked.durable.as_ref().expect("durable").stream);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), blocked.push_batch(batch()))
                .await
                .is_err()
        );
        assert!(pending.pending_ids().is_empty());
        assert_eq!(stream.progress().rejected, 1);
        Box::new(blocked).abort(StreamAbortReason::ClientCancelled).await;
        assert_eq!(Arc::strong_count(&stream), 1);
    }

    struct AckingSender {
        sent: Mutex<Vec<ArrowcolBatchEnvelope>>,
        acks: tokio::sync::mpsc::UnboundedSender<u64>,
    }

    #[async_trait]
    impl PdataSender for AckingSender {
        async fn send(&self, correlation_id: u64, pdata: OtapPdata) -> Result<(), PdataSendError> {
            let (_, payload) = pdata.into_parts();
            let otel_arrow_dfe_pdata::PayloadData::PdataArrowRecords(records) = payload.into_data() else {
                return Err(PdataSendError::Unavailable);
            };
            let decoded = crate::decode_record_set(*records).map_err(|_| PdataSendError::Unavailable)?;
            self.sent
                .lock()
                .expect("capture lock")
                .extend(decoded.into_iter().map(|batch| batch.envelope));
            self.acks.send(correlation_id).map_err(|_| PdataSendError::Closed)
        }
    }

    /// Scenario: Flight ingests an exact schema with durable admission enabled while a fake durable
    /// buffer acknowledges each batch asynchronously through DFE feedback.
    /// Guarantees: The final PutResult is labeled `durable_admission` without a receipt, and every
    /// emitted envelope carries the receiver's instance key.
    #[tokio::test]
    async fn exact_flight_ingest_returns_durable_admission() -> Result<()> {
        let catalog = Arc::new(MemoryCatalogStore::new());
        let control = CatalogControlPlane::new(
            DdlParser::default(),
            Arc::clone(&catalog) as Arc<dyn CatalogStore>,
            Arc::new(MemoryMetastore::new()),
        );
        let ddl = control
            .execute_ddl(arrowcol_catalog::FlightSqlDdlRequest::new(
                "CREATE TABLE demo.generated.events (id BIGINT NOT NULL)",
            ))
            .await?;
        let current = catalog
            .current_schema(ddl.identity())
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing schema"))?;
        let schema = Arc::new(current.arrow_schema()?);
        let batches = (0..3_i64)
            .map(|value| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let routing = Arc::new(MemoryRoutingStore::default());
        let instance_id = CollectorInstanceId::new();
        routing_lease(&routing, instance_id, ddl.identity()).await?;
        let (acks, mut feedback) = tokio::sync::mpsc::unbounded_channel();
        let sender = Arc::new(AckingSender {
            sent: Mutex::new(Vec::new()),
            acks,
        });
        let metrics = Arc::new(RuntimeMetrics::new(instance_id));
        metrics.configure_receiver(2, u64::MAX / 4, 1_000_000, 10_000, 16, 2);
        metrics
            .configure_admission("dfe-flight-test", true)
            .map_err(anyhow::Error::msg)?;
        let admission = Arc::new(
            DfeAdmission::new(
                Arc::clone(&sender) as Arc<dyn PdataSender>,
                Arc::new(StoreLeaseResolver::new(
                    Arc::clone(&routing) as Arc<dyn RoutingStore>,
                    instance_id,
                )),
                instance_id,
                "dfe-flight-test",
                2,
                Arc::clone(&metrics),
            )
            .with_durable_admission(Duration::from_secs(5)),
        );
        let durable_buffer = {
            let admission = Arc::clone(&admission);
            tokio::spawn(async move {
                while let Some(correlation_id) = feedback.recv().await {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    admission.resolve_feedback(correlation_id, true);
                }
            })
        };
        let service = ArrowColFlightService::new(
            ImplementationPool::Dataflow,
            catalog as Arc<dyn CatalogStore>,
            Arc::new(allow_all_authenticator().map_err(anyhow::Error::msg)?),
            Arc::clone(&admission) as Arc<dyn BatchAdmission>,
            arrowcol_flight::RuntimeLimits::default(),
        )?;
        let results = service
            .handle_do_put(flight_request(ddl.identity(), current.version(), schema, batches).await?)
            .await?
            .into_inner()
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(results.len(), 1);
        let completion = PutResultMetadata::from_put_result(&results[0])?;
        assert_eq!(
            completion.delivery_guarantee(),
            Some(DeliveryGuarantee::DurableAdmission)
        );
        assert_eq!(completion.receipt_id(), None);
        let sent = sender.sent.lock().expect("capture lock").clone();
        assert_eq!(sent.len(), 3);
        assert!(sent.iter().all(|envelope| envelope.instance_key == "dfe-flight-test"));
        assert!(sent.iter().all(|envelope| envelope.holder_instance_id == instance_id));
        let pressure = metrics.pressure().map_err(anyhow::Error::msg)?;
        assert_eq!(pressure.rich.buffered_batches.current().get(), 3);
        assert_eq!(pressure.rich.quiver_state, SubsystemState::Ready);
        drop(service);
        durable_buffer.abort();
        Ok(())
    }
}
