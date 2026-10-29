use std::os::fd::RawFd;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrowcol_protocol::{
    CATALOG_METADATA_KEY, CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY, ErrorCode, ErrorStatus,
    IMPLEMENTATION_POOL_METADATA_KEY, ImplementationPool, IngestMetadata, PROTOCOL_VERSION_METADATA_KEY,
    RawGrpcMetadata, ReceiptId, ReceiptStatus, SCHEMA_METADATA_KEY, TABLE_METADATA_KEY, UnixMillis,
};
use arrowcol_receipts::{ReceiptErrorKind, ReceiptRecord, ReceiptStore};
use arrowcol_routing::{ControlOperation, ControlRoute, RouteAssignment, RouteSource, RoutingEngine, RoutingErrorKind};
use async_trait::async_trait;
use bytes::Bytes;
use http::{Method, StatusCode};
use pingora::apps::HttpServerOptions;
use pingora::prelude::*;
use pingora::protocols::Digest;
use pingora::protocols::http::v2::server::default_h2_options;
use pingora::proxy::FailToProxy;
use pingora::server::ShutdownWatch;
use pingora::server::configuration::ServerConf;
use pingora::services::background::{BackgroundService, background_service};
use pingora::upstreams::peer::ALPN;
use serde::Serialize;
use tracing::{error, info, warn};

use crate::{
    AuthFailure, AuthFailureKind, Authenticator, EDGE_NONCE_HEADER, EDGE_SIGNATURE_HEADER, EDGE_TIMESTAMP_HEADER,
    EdgeVerifier, ExternalRequest, INTERNAL_AUTH_CONTEXT_HEADER, INTERNAL_AUTH_EXPIRES_HEADER,
    INTERNAL_AUTH_SIGNATURE_HEADER, InternalIdentitySigner, ReceiptApiConfig, RouterConfig, RouterMetrics,
    SignedInternalIdentity, UpstreamConfig,
};

/// Exact Arrow Flight `DoPut` RPC path.
pub const DO_PUT_PATH: &str = "/arrow.flight.protocol.FlightService/DoPut";
/// Explicit metadata classifying Flight SQL control operations.
pub const CONTROL_OPERATION_METADATA_KEY: &str = "x-arrowcol-control-operation";
/// Internal routing lease lineage.
pub const ROUTING_LEASE_ID_HEADER: &str = "x-arrowcol-routing-lease-id";
/// Internal routing fencing epoch.
pub const ROUTING_FENCING_EPOCH_HEADER: &str = "x-arrowcol-routing-fencing-epoch";
/// Internal routing compare-and-set version.
pub const ROUTING_LEASE_VERSION_HEADER: &str = "x-arrowcol-routing-lease-version";
/// Internal routing lease expiry.
pub const ROUTING_LEASE_EXPIRES_HEADER: &str = "x-arrowcol-routing-lease-expires";

const FLIGHT_SERVICE_PREFIX: &str = "/arrow.flight.protocol.FlightService/";

/// Production Pingora proxy application.
#[derive(Clone)]
pub struct ProductionRouter {
    routing: Arc<RoutingEngine>,
    receipts: Arc<dyn ReceiptStore>,
    authenticator: Arc<dyn Authenticator>,
    edge_verifier: Arc<EdgeVerifier>,
    internal_signer: Arc<InternalIdentitySigner>,
    upstream: UpstreamConfig,
    receipt_config: ReceiptApiConfig,
    metrics: RouterMetrics,
}

impl ProductionRouter {
    /// Creates the proxy application from initialized shared services.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        routing: Arc<RoutingEngine>,
        receipts: Arc<dyn ReceiptStore>,
        authenticator: Arc<dyn Authenticator>,
        edge_verifier: Arc<EdgeVerifier>,
        internal_signer: Arc<InternalIdentitySigner>,
        upstream: UpstreamConfig,
        receipt_config: ReceiptApiConfig,
        metrics: RouterMetrics,
    ) -> Self {
        Self {
            routing,
            receipts,
            authenticator,
            edge_verifier,
            internal_signer,
            upstream,
            receipt_config,
            metrics,
        }
    }

    async fn authenticate_and_sign(
        &self,
        session: &Session,
        now: UnixMillis,
    ) -> Result<SignedInternalIdentity, AuthFailure> {
        let request = session.req_header();
        self.edge_verifier
            .verify(&request.method, request.uri.path(), &request.headers, now)?;
        let context = self
            .authenticator
            .authenticate(
                &ExternalRequest {
                    method: request.method.clone(),
                    path: request.uri.path().to_owned(),
                },
                now,
            )
            .await?;
        self.internal_signer.sign(&context, now)
    }

    async fn route_proxy_request(
        &self,
        session: &mut Session,
        context: &mut RequestContext,
        now: UnixMillis,
    ) -> pingora::Result<bool> {
        let request = session.req_header();
        if request.method != Method::POST {
            write_grpc_error(session, 12, "Flight RPCs require POST").await?;
            return Ok(true);
        }
        if !request
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/grpc"))
        {
            write_grpc_error(session, 3, "content-type must be application/grpc").await?;
            return Ok(true);
        }

        let signed_identity = match self.authenticate_and_sign(session, now).await {
            Ok(identity) => identity,
            Err(failure) => {
                self.metrics.failure("auth", "unknown", auth_reason(failure.kind()));
                write_grpc_error(session, auth_grpc_status(failure.kind()), failure.message()).await?;
                return Ok(true);
            }
        };
        context.identity = Some(signed_identity);
        context.is_grpc = true;

        if request.uri.path() == DO_PUT_PATH {
            let metadata = match parse_ingest_metadata(&request.headers) {
                Ok(metadata) => metadata,
                Err(message) => {
                    self.metrics.failure("doput", "unknown", "invalid_metadata");
                    write_grpc_error(session, 3, &message).await?;
                    return Ok(true);
                }
            };
            let pool = metadata.implementation_pool().to_string();
            match self.routing.route_ingest(&metadata, now).await {
                Ok(assignment) => {
                    self.record_assignment(context, assignment, "doput", pool);
                    Ok(false)
                }
                Err(error) => {
                    self.metrics.failure("doput", &pool, routing_reason(error.kind()));
                    write_grpc_error(session, routing_grpc_status(error.kind()), error.message()).await?;
                    Ok(true)
                }
            }
        } else if request.uri.path().starts_with(FLIGHT_SERVICE_PREFIX) {
            let control = match parse_control_route(&request.headers, request.uri.path()) {
                Ok(control) => control,
                Err(message) => {
                    self.metrics.failure("control", "unknown", "invalid_metadata");
                    write_grpc_error(session, 3, &message).await?;
                    return Ok(true);
                }
            };
            let pool = control.implementation_pool.to_string();
            match self.routing.route_control(&control, now).await {
                Ok(assignment) => {
                    self.record_assignment(context, assignment, "control", pool);
                    Ok(false)
                }
                Err(error) => {
                    self.metrics.failure("control", &pool, routing_reason(error.kind()));
                    write_grpc_error(session, routing_grpc_status(error.kind()), error.message()).await?;
                    Ok(true)
                }
            }
        } else {
            write_grpc_error(session, 12, "only Arrow Flight service RPCs are routed").await?;
            Ok(true)
        }
    }

    fn record_assignment(
        &self,
        context: &mut RequestContext,
        assignment: RouteAssignment,
        operation: &'static str,
        pool: String,
    ) {
        let source = match assignment.source {
            RouteSource::CachedLease => "cache",
            RouteSource::LeaseClaim => "sql_claim",
            RouteSource::ControlSelection => "control_cache",
        };
        self.metrics.decision(operation, &pool, source);
        self.metrics.stream_started(&pool);
        context.assignment = Some(assignment);
        context.operation = operation;
        context.pool = Some(pool);
        context.stream_counted = true;
    }

    async fn handle_http_api(
        &self,
        session: &mut Session,
        path: &str,
        now: UnixMillis,
    ) -> pingora::Result<Option<bool>> {
        match path {
            "/healthz" => {
                write_json(
                    session,
                    StatusCode::OK,
                    &HealthResponse {
                        status: "healthy",
                        unix_ms: now.get(),
                    },
                )
                .await?;
                Ok(Some(true))
            }
            "/readyz" => {
                let ready = self.routing.is_ready_at(now).await;
                write_json(
                    session,
                    if ready {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    },
                    &ReadinessResponse {
                        status: if ready { "ready" } else { "not_ready" },
                    },
                )
                .await?;
                Ok(Some(true))
            }
            "/v1/capabilities" => {
                write_json(
                    session,
                    StatusCode::OK,
                    &CapabilitiesResponse {
                        protocol_version: 1,
                        transparent_flight_data: true,
                        receipt_status: true,
                        bounded_receipt_batch: self.receipt_config.max_batch_size,
                        flight_sql_control_metadata: CONTROL_OPERATION_METADATA_KEY,
                        identity_propagation: "hmac_sha256",
                    },
                )
                .await?;
                Ok(Some(true))
            }
            "/metrics" => {
                let body = self
                    .metrics
                    .encode()
                    .map_err(|cause| Error::because(ErrorType::InternalError, "encode Prometheus metrics", cause))?;
                write_bytes(session, StatusCode::OK, "text/plain; version=0.0.4", body).await?;
                Ok(Some(true))
            }
            _ if path.starts_with("/v1/receipts") => {
                if session.req_header().method != Method::GET {
                    write_api_error(
                        session,
                        StatusCode::METHOD_NOT_ALLOWED,
                        ErrorCode::ReceiptTransitionInvalid,
                        "receipt status APIs require GET",
                    )
                    .await?;
                    return Ok(Some(true));
                }
                if let Err(failure) = self.authenticate_and_sign(session, now).await {
                    write_api_error(
                        session,
                        auth_http_status(failure.kind()),
                        auth_error_code(failure.kind()),
                        failure.message(),
                    )
                    .await?;
                    return Ok(Some(true));
                }
                self.handle_receipts(session, path).await?;
                Ok(Some(true))
            }
            _ => Ok(None),
        }
    }

    async fn handle_receipts(&self, session: &mut Session, path: &str) -> pingora::Result<()> {
        if let Some(id) = path.strip_prefix("/v1/receipts/") {
            if id.is_empty() || id.contains('/') {
                return write_api_error(
                    session,
                    StatusCode::BAD_REQUEST,
                    ErrorCode::ReceiptTransitionInvalid,
                    "receipt path must contain one UUID",
                )
                .await;
            }
            let receipt_id = match ReceiptId::from_str(id) {
                Ok(receipt_id) => receipt_id,
                Err(_) => {
                    return write_api_error(
                        session,
                        StatusCode::BAD_REQUEST,
                        ErrorCode::ReceiptTransitionInvalid,
                        "receipt identifier is invalid",
                    )
                    .await;
                }
            };
            match self.receipts.receipt(receipt_id).await {
                Ok(Some(record)) => write_receipt_record(session, record).await,
                Ok(None) => {
                    write_api_error(
                        session,
                        StatusCode::NOT_FOUND,
                        ErrorCode::ReceiptNotFound,
                        "receipt was not found",
                    )
                    .await
                }
                Err(error) => write_receipt_store_error(session, error.kind(), error.message()).await,
            }
        } else if path == "/v1/receipts" {
            let query = session.req_header().uri.query().unwrap_or("");
            let ids = match parse_receipt_query(query, self.receipt_config.max_batch_size) {
                Ok(ids) => ids,
                Err(message) => {
                    return write_api_error(
                        session,
                        StatusCode::BAD_REQUEST,
                        ErrorCode::ReceiptTransitionInvalid,
                        &message,
                    )
                    .await;
                }
            };
            match self.receipts.receipts(&ids).await {
                Ok(records) => {
                    let entries = ids
                        .into_iter()
                        .zip(records)
                        .map(|(receipt_id, record)| match record {
                            Some(record) => ReceiptBatchEntry::from_record(receipt_id, record),
                            None => ReceiptBatchEntry {
                                receipt_id,
                                found: false,
                                purge_allowed: false,
                                status: None,
                                error: Some(public_error(ErrorCode::ReceiptNotFound, "receipt was not found")),
                            },
                        })
                        .collect();
                    write_json(session, StatusCode::OK, &ReceiptBatchResponse { receipts: entries }).await
                }
                Err(error) => write_receipt_store_error(session, error.kind(), error.message()).await,
            }
        } else {
            write_api_error(
                session,
                StatusCode::NOT_FOUND,
                ErrorCode::ReceiptNotFound,
                "receipt API path was not found",
            )
            .await
        }
    }
}

#[derive(Default)]
pub struct RequestContext {
    assignment: Option<RouteAssignment>,
    identity: Option<SignedInternalIdentity>,
    operation: &'static str,
    pool: Option<String>,
    is_grpc: bool,
    stream_counted: bool,
}

#[async_trait]
impl ProxyHttp for ProductionRouter {
    type CTX = RequestContext;

    fn new_ctx(&self) -> Self::CTX {
        RequestContext::default()
    }

    async fn request_filter(&self, session: &mut Session, context: &mut Self::CTX) -> pingora::Result<bool> {
        let now = now_unix_millis()
            .map_err(|cause| Error::because(ErrorType::InternalError, "read current timestamp", cause))?;
        let path = session.req_header().uri.path().to_owned();
        if let Some(handled) = self.handle_http_api(session, &path, now).await? {
            return Ok(handled);
        }
        self.route_proxy_request(session, context, now).await
    }

    async fn upstream_peer(&self, _session: &mut Session, context: &mut Self::CTX) -> pingora::Result<Box<HttpPeer>> {
        let assignment = context.assignment.as_ref().ok_or_else(|| {
            Error::explain(
                ErrorType::InternalError,
                "request reached upstream selection without a route",
            )
        })?;
        let mut peer = HttpPeer::new(
            assignment.flight_endpoint,
            self.upstream.tls,
            if self.upstream.tls {
                self.upstream.sni.clone()
            } else {
                String::new()
            },
        );
        peer.group_key = u64::from_be_bytes(
            assignment.instance_id.as_uuid().as_bytes()[..8]
                .try_into()
                .map_err(|_| Error::explain(ErrorType::InternalError, "collector UUID was not 16 bytes"))?,
        );
        peer.options.alpn = ALPN::H2;
        peer.options.max_h2_streams = self.upstream.max_h2_streams;
        peer.options.h2_stream_window_size = Some(self.upstream.stream_window_bytes);
        peer.options.h2_connection_window_size = Some(self.upstream.connection_window_bytes);
        peer.options.connection_timeout = Some(Duration::from_millis(self.upstream.connection_timeout_ms));
        peer.options.total_connection_timeout = Some(Duration::from_millis(self.upstream.total_connection_timeout_ms));
        peer.options.idle_timeout = Some(Duration::from_secs(self.upstream.idle_timeout_seconds));
        peer.options.h2_ping_interval = self.upstream.ping_interval_seconds.map(Duration::from_secs);
        Ok(Box::new(peer))
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        context: &mut Self::CTX,
    ) -> pingora::Result<()> {
        for header in [
            EDGE_TIMESTAMP_HEADER,
            EDGE_NONCE_HEADER,
            EDGE_SIGNATURE_HEADER,
            INTERNAL_AUTH_CONTEXT_HEADER,
            INTERNAL_AUTH_EXPIRES_HEADER,
            INTERNAL_AUTH_SIGNATURE_HEADER,
            ROUTING_LEASE_ID_HEADER,
            ROUTING_FENCING_EPOCH_HEADER,
            ROUTING_LEASE_VERSION_HEADER,
            ROUTING_LEASE_EXPIRES_HEADER,
        ] {
            upstream_request.remove_header(header);
        }
        let identity = context.identity.as_ref().ok_or_else(|| {
            Error::explain(
                ErrorType::InternalError,
                "authenticated identity is missing from request context",
            )
        })?;
        upstream_request
            .insert_header(INTERNAL_AUTH_CONTEXT_HEADER, &identity.encoded_context)
            .map_err(|error| error.more_context("insert internal auth context"))?;
        upstream_request
            .insert_header(
                INTERNAL_AUTH_EXPIRES_HEADER,
                identity.expires_at_unix_ms.get().to_string(),
            )
            .map_err(|error| error.more_context("insert internal auth expiry"))?;
        upstream_request
            .insert_header(INTERNAL_AUTH_SIGNATURE_HEADER, &identity.signature)
            .map_err(|error| error.more_context("insert internal auth signature"))?;
        if let Some(lease) = context
            .assignment
            .as_ref()
            .and_then(|assignment| assignment.lease.as_ref())
        {
            upstream_request
                .insert_header(ROUTING_LEASE_ID_HEADER, lease.lease_id.to_string())
                .map_err(|error| error.more_context("insert routing lease id"))?;
            upstream_request
                .insert_header(ROUTING_FENCING_EPOCH_HEADER, lease.fencing_epoch.get().to_string())
                .map_err(|error| error.more_context("insert routing fencing epoch"))?;
            upstream_request
                .insert_header(ROUTING_LEASE_VERSION_HEADER, lease.lease_version.to_string())
                .map_err(|error| error.more_context("insert routing lease version"))?;
            upstream_request
                .insert_header(ROUTING_LEASE_EXPIRES_HEADER, lease.expires_at_unix_ms.get().to_string())
                .map_err(|error| error.more_context("insert routing lease expiry"))?;
        }
        Ok(())
    }

    fn fail_to_connect(
        &self,
        _session: &mut Session,
        _peer: &HttpPeer,
        _context: &mut Self::CTX,
        mut error: Box<Error>,
    ) -> Box<Error> {
        error.set_retry(false);
        error
    }

    fn error_while_proxy(
        &self,
        peer: &HttpPeer,
        _session: &mut Session,
        mut error: Box<Error>,
        _context: &mut Self::CTX,
        _client_reused: bool,
    ) -> Box<Error> {
        error.set_retry(false);
        error.more_context(format!("non-retryable streaming request to {peer}"))
    }

    async fn fail_to_proxy(&self, session: &mut Session, error: &Error, context: &mut Self::CTX) -> FailToProxy {
        let code = if context.is_grpc && session.response_written().is_none() {
            if let Err(write_error) = write_grpc_error(session, 14, "upstream collector is unavailable").await {
                warn!(error = %write_error, "failed to write canonical gRPC proxy error");
            }
            200
        } else {
            let status = match error.esource() {
                ErrorSource::Upstream => 502,
                ErrorSource::Downstream => 400,
                ErrorSource::Internal | ErrorSource::Unset => 500,
            };
            if session.response_written().is_none() {
                let _ = session.respond_error(status).await;
            }
            status
        };
        FailToProxy {
            error_code: code,
            can_reuse_downstream: false,
        }
    }

    async fn connected_to_upstream(
        &self,
        _session: &mut Session,
        reused: bool,
        _peer: &HttpPeer,
        _fd: RawFd,
        _digest: Option<&Digest>,
        _context: &mut Self::CTX,
    ) -> pingora::Result<()> {
        self.metrics.upstream_connection(reused);
        Ok(())
    }

    async fn logging(&self, _session: &mut Session, error: Option<&Error>, context: &mut Self::CTX) {
        if context.stream_counted {
            if let Some(pool) = context.pool.as_deref() {
                self.metrics.stream_finished(pool);
            }
            context.stream_counted = false;
        }
        if let Some(error) = error {
            warn!(
                operation = context.operation,
                pool = context.pool.as_deref().unwrap_or("unknown"),
                error = %error,
                "proxied stream completed with error"
            );
        }
    }
}

/// Background SQL refresh and two-phase rebalance service.
#[derive(Clone)]
pub struct RouterControlLoop {
    routing: Arc<RoutingEngine>,
    metrics: RouterMetrics,
    interval: Duration,
    refresh_only: bool,
}

impl RouterControlLoop {
    /// Creates a routing control loop.
    #[must_use]
    pub const fn new(routing: Arc<RoutingEngine>, metrics: RouterMetrics, interval: Duration) -> Self {
        Self {
            routing,
            metrics,
            interval,
            refresh_only: false,
        }
    }

    /// Disables autonomous movement for controller-owned local benchmarks.
    #[must_use]
    pub const fn with_refresh_only(mut self, enabled: bool) -> Self {
        self.refresh_only = enabled;
        self
    }
}

#[async_trait]
impl BackgroundService for RouterControlLoop {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        loop {
            match now_unix_millis() {
                Ok(now) => match if self.refresh_only {
                    self.routing
                        .refresh(now)
                        .await
                        .map(|_| arrowcol_routing::RebalanceReport::default())
                } else {
                    self.routing.rebalance(now).await
                } {
                    Ok(report) => {
                        self.metrics.refresh(true);
                        self.metrics.lease_event("drain_started", report.drains_started);
                        self.metrics
                            .lease_event("transfer_completed", report.transfers_completed);
                        self.metrics
                            .lease_event("succession_completed", report.successions_completed);
                        self.metrics.lease_event("cas_conflict", report.conflicts);
                        if let Some(view) = self.routing.cached_view().await {
                            self.metrics.update_cache_gauges(&view);
                        }
                    }
                    Err(cause) => {
                        self.metrics.refresh(false);
                        warn!(error = %cause, "routing control loop refresh/rebalance failed");
                    }
                },
                Err(cause) => error!(error = %cause, "routing control loop could not read the clock"),
            }
            tokio::select! {
                () = tokio::time::sleep(self.interval) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        info!("routing control loop observed graceful shutdown");
                        break;
                    }
                }
            }
        }
    }
}

/// Builds the Pingora server with h2c, explicit flow control, connection reuse,
/// zero streaming retries, and a graceful GOAWAY/drain lifecycle.
///
/// # Errors
///
/// Returns an error when state directories or service configuration fail.
pub fn build_server(
    config: &RouterConfig,
    router: ProductionRouter,
    control_loop: RouterControlLoop,
) -> anyhow::Result<Server> {
    std::fs::create_dir_all(&config.server.state_directory)?;
    let server_conf = ServerConf {
        threads: config.server.threads,
        listener_tasks_per_fd: config.server.listener_tasks_per_fd,
        grace_period_seconds: Some(config.server.grace_period_seconds),
        graceful_shutdown_timeout_seconds: Some(config.server.graceful_shutdown_timeout_seconds),
        // Pingora counts the first upstream attempt in this value. One means
        // exactly one attempt; the error hooks above also force retry=false.
        max_retries: 1,
        upstream_keepalive_pool_size: 1_024,
        pid_file: config
            .server
            .state_directory
            .join("arrowcol-router.pid")
            .display()
            .to_string(),
        upgrade_sock: config
            .server
            .state_directory
            .join("arrowcol-router-upgrade.sock")
            .display()
            .to_string(),
        ..ServerConf::default()
    };
    let mut server = Server::new_with_opt_and_conf(None, server_conf);
    server.bootstrap();

    let mut service = http_proxy_service(&server.configuration, router);
    let proxy = service
        .app_logic_mut()
        .ok_or_else(|| anyhow::anyhow!("Pingora proxy service is missing application logic"))?;
    let mut server_options = HttpServerOptions::default();
    server_options.h2c = true;
    server_options.h2_idle_timeout = Some(Duration::from_secs(config.server.h2.idle_timeout_seconds));
    proxy.server_options = Some(server_options);
    let mut h2_options = default_h2_options();
    h2_options.max_concurrent_streams(config.server.h2.max_concurrent_streams);
    h2_options.initial_window_size(config.server.h2.stream_window_bytes);
    h2_options.initial_connection_window_size(config.server.h2.connection_window_bytes);
    proxy.h2_options = Some(h2_options);
    service.add_tcp(&config.server.listen.to_string());
    server.add_service(service);
    server.add_service(background_service("arrowcol-routing-control", control_loop));
    Ok(server)
}

fn parse_ingest_metadata(headers: &http::HeaderMap) -> Result<IngestMetadata, String> {
    let raw = RawGrpcMetadata {
        protocol_version: ascii_header(headers, PROTOCOL_VERSION_METADATA_KEY)?,
        implementation_pool: ascii_header(headers, IMPLEMENTATION_POOL_METADATA_KEY)?,
        catalog: ascii_header(headers, CATALOG_METADATA_KEY)?,
        schema: ascii_header(headers, SCHEMA_METADATA_KEY)?,
        table: ascii_header(headers, TABLE_METADATA_KEY)?,
        claimed_schema_fingerprint: ascii_header(headers, CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY)?,
    };
    IngestMetadata::parse(raw).map_err(|cause| cause.to_string())
}

fn parse_control_route(headers: &http::HeaderMap, path: &str) -> Result<ControlRoute, String> {
    let pool = ascii_header(headers, IMPLEMENTATION_POOL_METADATA_KEY)?
        .ok_or_else(|| format!("required gRPC metadata key {IMPLEMENTATION_POOL_METADATA_KEY} is missing"))?
        .parse::<ImplementationPool>()
        .map_err(|cause| cause.to_string())?;
    let operation = match ascii_header(headers, CONTROL_OPERATION_METADATA_KEY)?
        .ok_or_else(|| format!("required gRPC metadata key {CONTROL_OPERATION_METADATA_KEY} is missing"))?
    {
        "catalog_read" => ControlOperation::CatalogRead,
        "catalog_write" => ControlOperation::CatalogWrite,
        "new_table" => ControlOperation::NewTable,
        _ => return Err("control operation must be catalog_read, catalog_write, or new_table".to_owned()),
    };
    let mut routing_key = path.to_owned();
    for key in [CATALOG_METADATA_KEY, SCHEMA_METADATA_KEY, TABLE_METADATA_KEY] {
        routing_key.push('|');
        routing_key.push_str(ascii_header(headers, key)?.unwrap_or(""));
    }
    if routing_key.len() > 512 {
        return Err("control routing metadata exceeds 512 bytes".to_owned());
    }
    Ok(ControlRoute {
        implementation_pool: pool,
        operation,
        routing_key,
    })
}

fn ascii_header<'a>(headers: &'a http::HeaderMap, key: &str) -> Result<Option<&'a str>, String> {
    headers
        .get(key)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| format!("gRPC metadata {key} is not visible ASCII"))
        })
        .transpose()
}

fn parse_receipt_query(query: &str, maximum: usize) -> Result<Vec<ReceiptId>, String> {
    let value = query
        .strip_prefix("ids=")
        .ok_or_else(|| "batch receipt query must use ?ids=<uuid>,<uuid>".to_owned())?;
    if value.is_empty() {
        return Err("batch receipt query must contain at least one identifier".to_owned());
    }
    let parts: Vec<_> = value.split(',').collect();
    if parts.len() > maximum {
        return Err(format!(
            "batch receipt query contains {} identifiers; maximum is {maximum}",
            parts.len()
        ));
    }
    parts
        .into_iter()
        .map(|value| ReceiptId::from_str(value).map_err(|_| format!("invalid receipt identifier {value:?}")))
        .collect()
}

async fn write_receipt_record(session: &mut Session, record: ReceiptRecord) -> pingora::Result<()> {
    let response = ReceiptResponse::from_record(record).map_err(|cause| {
        Error::explain(
            ErrorType::HTTPStatus(500),
            format!("persisted receipt could not be rendered: {cause}"),
        )
    })?;
    write_json(session, StatusCode::OK, &response).await
}

async fn write_receipt_store_error(
    session: &mut Session,
    kind: ReceiptErrorKind,
    message: &str,
) -> pingora::Result<()> {
    let (status, code) = match kind {
        ReceiptErrorKind::RequestTooLarge | ReceiptErrorKind::InvalidConfiguration => {
            (StatusCode::BAD_REQUEST, ErrorCode::ReceiptTransitionInvalid)
        }
        ReceiptErrorKind::NotFound => (StatusCode::NOT_FOUND, ErrorCode::ReceiptNotFound),
        ReceiptErrorKind::Conflict | ReceiptErrorKind::Fenced | ReceiptErrorKind::Draining => {
            (StatusCode::CONFLICT, ErrorCode::ReceiptTransitionInvalid)
        }
        ReceiptErrorKind::Database => (StatusCode::SERVICE_UNAVAILABLE, ErrorCode::Internal),
        ReceiptErrorKind::CorruptState => (StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal),
    };
    write_api_error(session, status, code, message).await
}

async fn write_api_error(
    session: &mut Session,
    status: StatusCode,
    code: ErrorCode,
    message: &str,
) -> pingora::Result<()> {
    write_json(
        session,
        status,
        &ApiErrorResponse {
            error: public_error(code, message),
        },
    )
    .await
}

fn public_error(code: ErrorCode, message: &str) -> ErrorStatus {
    match ErrorStatus::new(code, sanitize_public_message(message)) {
        Ok(error) => error,
        Err(_) => match ErrorStatus::new(ErrorCode::Internal, "internal router error") {
            Ok(error) => error,
            Err(_) => unreachable!("static public error is valid"),
        },
    }
}

fn sanitize_public_message(message: &str) -> String {
    let mut output: String = message
        .chars()
        .map(|character| if character.is_control() { ' ' } else { character })
        .take(1_024)
        .collect();
    if output.is_empty() {
        output.push_str("router request failed");
    }
    output
}

async fn write_grpc_error(session: &mut Session, grpc_status: u16, message: &str) -> pingora::Result<()> {
    let mut response = ResponseHeader::build(200, Some(5))?;
    response.insert_header("content-type", "application/grpc")?;
    response.insert_header("grpc-status", grpc_status.to_string())?;
    response.insert_header("grpc-message", grpc_message(message))?;
    response.insert_header("cache-control", "no-store")?;
    response.insert_header("x-content-type-options", "nosniff")?;
    session.write_response_header(Box::new(response), true).await
}

async fn write_json<T: Serialize>(session: &mut Session, status: StatusCode, value: &T) -> pingora::Result<()> {
    let body = serde_json::to_vec(value)
        .map_err(|cause| Error::because(ErrorType::InternalError, "serialize JSON response", cause))?;
    write_bytes(session, status, "application/json", body).await
}

async fn write_bytes(
    session: &mut Session,
    status: StatusCode,
    content_type: &str,
    body: Vec<u8>,
) -> pingora::Result<()> {
    let mut response = ResponseHeader::build(status.as_u16(), Some(5))?;
    response.insert_header("content-type", content_type)?;
    response.insert_header("content-length", body.len().to_string())?;
    response.insert_header("cache-control", "no-store")?;
    response.insert_header("x-content-type-options", "nosniff")?;
    session
        .write_response_header(Box::new(response), body.is_empty())
        .await?;
    if !body.is_empty() {
        session.write_response_body(Some(Bytes::from(body)), true).await?;
    }
    Ok(())
}

fn grpc_message(message: &str) -> String {
    let mut encoded = String::new();
    for byte in sanitize_public_message(message).bytes() {
        if matches!(byte, b' '..=b'~') && byte != b'%' {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn routing_reason(kind: RoutingErrorKind) -> &'static str {
    match kind {
        RoutingErrorKind::InvalidConfiguration => "invalid_configuration",
        RoutingErrorKind::InvalidSnapshot => "invalid_snapshot",
        RoutingErrorKind::PoolUnavailable => "pool_unavailable",
        RoutingErrorKind::LeaseDraining => "lease_draining",
        RoutingErrorKind::LeaseHolderUnavailable => "lease_holder_unavailable",
        RoutingErrorKind::Conflict => "conflict",
        RoutingErrorKind::CorruptState => "corrupt_state",
        RoutingErrorKind::Database => "database",
        _ => "other",
    }
}

fn routing_grpc_status(kind: RoutingErrorKind) -> u16 {
    match kind {
        RoutingErrorKind::InvalidConfiguration | RoutingErrorKind::InvalidSnapshot => 3,
        RoutingErrorKind::LeaseDraining => 8,
        RoutingErrorKind::PoolUnavailable
        | RoutingErrorKind::LeaseHolderUnavailable
        | RoutingErrorKind::Conflict
        | RoutingErrorKind::Database => 14,
        RoutingErrorKind::CorruptState => 13,
        _ => 13,
    }
}

fn auth_reason(kind: AuthFailureKind) -> &'static str {
    match kind {
        AuthFailureKind::Missing => "missing",
        AuthFailureKind::Invalid => "invalid",
        AuthFailureKind::ForgedInternalIdentity => "forged_internal_identity",
        AuthFailureKind::Forbidden => "forbidden",
        AuthFailureKind::Internal => "internal",
    }
}

fn auth_grpc_status(kind: AuthFailureKind) -> u16 {
    match kind {
        AuthFailureKind::Forbidden => 7,
        AuthFailureKind::Internal => 13,
        AuthFailureKind::Missing | AuthFailureKind::Invalid | AuthFailureKind::ForgedInternalIdentity => 16,
    }
}

fn auth_http_status(kind: AuthFailureKind) -> StatusCode {
    match kind {
        AuthFailureKind::Forbidden => StatusCode::FORBIDDEN,
        AuthFailureKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        AuthFailureKind::Missing | AuthFailureKind::Invalid | AuthFailureKind::ForgedInternalIdentity => {
            StatusCode::UNAUTHORIZED
        }
    }
}

fn auth_error_code(kind: AuthFailureKind) -> ErrorCode {
    match kind {
        AuthFailureKind::Missing => ErrorCode::AuthenticationMissing,
        AuthFailureKind::Forbidden => ErrorCode::AuthenticationForbidden,
        AuthFailureKind::Invalid | AuthFailureKind::ForgedInternalIdentity => ErrorCode::AuthenticationInvalid,
        AuthFailureKind::Internal => ErrorCode::Internal,
    }
}

fn now_unix_millis() -> anyhow::Result<UnixMillis> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(UnixMillis::new(u64::try_from(millis)?)?)
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    unix_ms: u64,
}

#[derive(Serialize)]
struct ReadinessResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct CapabilitiesResponse {
    protocol_version: u16,
    transparent_flight_data: bool,
    receipt_status: bool,
    bounded_receipt_batch: usize,
    flight_sql_control_metadata: &'static str,
    identity_propagation: &'static str,
}

#[derive(Serialize)]
struct ApiErrorResponse {
    error: ErrorStatus,
}

#[derive(Serialize)]
struct ReceiptResponse {
    status: ReceiptStatus,
    purge_allowed: bool,
}

impl ReceiptResponse {
    fn from_record(record: ReceiptRecord) -> arrowcol_receipts::ReceiptResult<Self> {
        let purge_allowed = record.can_client_purge();
        Ok(Self {
            status: record.protocol_status()?,
            purge_allowed,
        })
    }
}

#[derive(Serialize)]
struct ReceiptBatchResponse {
    receipts: Vec<ReceiptBatchEntry>,
}

#[derive(Serialize)]
struct ReceiptBatchEntry {
    receipt_id: ReceiptId,
    found: bool,
    purge_allowed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<ReceiptStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorStatus>,
}

impl ReceiptBatchEntry {
    fn from_record(receipt_id: ReceiptId, record: ReceiptRecord) -> Self {
        let purge_allowed = record.can_client_purge();
        match record.protocol_status() {
            Ok(status) => Self {
                receipt_id,
                found: true,
                purge_allowed,
                status: Some(status),
                error: None,
            },
            Err(cause) => Self {
                receipt_id,
                found: true,
                purge_allowed: false,
                status: None,
                error: Some(public_error(ErrorCode::Internal, cause.message())),
            },
        }
    }
}
