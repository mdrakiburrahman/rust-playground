use std::io;
use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PollInfo, PutResult, SchemaResult, Ticket,
};
use arrowcol_protocol::{
    BasisPoints, CollectorInstanceId, ImplementationPool, Measurement, PositiveMeasurement, PressureGauge, UnixMillis,
};
use arrowcol_receipts::{ReceiptStore, SqlServerReceiptConfig, SqlServerReceiptStore};
use arrowcol_router_rust::{
    AllowAllAuthenticator, AuthConfig, Authenticator, EdgeRequestSigner, EdgeVerifier, H2Config,
    InternalIdentitySigner, ProductionRouter, ReceiptApiConfig, RouterConfig, RouterControlLoop, RouterMetrics,
    RoutingConfig, ServerConfig, SqlServerConfig, TelemetryConfig, UpstreamConfig, build_server,
    insert_edge_signature_headers,
};
use arrowcol_routing::{
    CollectorRegistration, MemoryRoutingStore, RichPressureSnapshot, RoutingEngine, RoutingPolicy, RoutingStore,
    SubsystemState,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use http::{HeaderMap, HeaderValue, Method};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Request, Response, Status};

const CHILD_ENV: &str = "ARROWCOL_ROUTER_PINGORA_CHILD";
const LISTEN_ENV: &str = "ARROWCOL_ROUTER_PINGORA_LISTEN";
const BACKEND_ENV: &str = "ARROWCOL_ROUTER_PINGORA_BACKEND";
const STATE_ENV: &str = "ARROWCOL_ROUTER_PINGORA_STATE";
const BACKEND_HEADER: &str = "x-test-backend";
const FAIL_AFTER_HEADER: &str = "x-test-fail-after";
const INTERNAL_CONTEXT_HEADER: &str = "x-arrowcol-internal-auth-context";
const EDGE_SIGNATURE_HEADER: &str = "x-arrowcol-edge-signature";

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
struct TestFlight;

#[async_trait]
impl FlightService for TestFlight {
    type HandshakeStream = ResponseStream<HandshakeResponse>;
    type ListFlightsStream = ResponseStream<FlightInfo>;
    type DoGetStream = ResponseStream<FlightData>;
    type DoPutStream = ResponseStream<PutResult>;
    type DoExchangeStream = ResponseStream<FlightData>;
    type DoActionStream = ResponseStream<arrow_flight::Result>;
    type ListActionsStream = ResponseStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(&self, _request: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(&self, _request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(&self, _request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get"))
    }

    async fn do_put(
        &self,
        request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        validate_internal_metadata(request.metadata())?;
        let fail_after = request
            .metadata()
            .get(FAIL_AFTER_HEADER)
            .and_then(|value| value.to_str().ok())
            .map(str::parse::<usize>)
            .transpose()
            .map_err(|_| Status::invalid_argument("invalid fail-after metadata"))?;
        let mut inbound = request.into_inner();
        let (sender, receiver) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut messages = 0_usize;
            while let Some(message) = inbound.next().await {
                match message {
                    Ok(_) => {
                        messages = messages.saturating_add(1);
                        if fail_after == Some(messages) {
                            let mut metadata = MetadataMap::new();
                            metadata.insert(BACKEND_HEADER, MetadataValue::from_static("catalog-a"));
                            let _ = sender
                                .send(Err(Status::with_metadata(
                                    Code::Aborted,
                                    "injected backend stream failure",
                                    metadata,
                                )))
                                .await;
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            }
            let _ = sender
                .send(Ok(PutResult {
                    app_metadata: Bytes::from(format!("backend=catalog-a;messages={messages}")),
                }))
                .await;
        });
        let mut response = Response::new(Box::pin(ReceiverStream::new(receiver)) as Self::DoPutStream);
        response
            .metadata_mut()
            .insert(BACKEND_HEADER, MetadataValue::from_static("catalog-a"));
        Ok(response)
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(&self, request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        validate_internal_metadata(request.metadata())?;
        Ok(Response::new(Box::pin(stream::iter(vec![Ok(arrow_flight::Result {
            body: Bytes::from_static(b"catalog-control-ok"),
        })]))))
    }

    async fn list_actions(&self, _request: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

fn validate_internal_metadata(metadata: &MetadataMap) -> Result<(), Status> {
    if metadata.get(INTERNAL_CONTEXT_HEADER).is_none() {
        return Err(Status::unauthenticated("signed internal identity is missing"));
    }
    if metadata.get(EDGE_SIGNATURE_HEADER).is_some() {
        return Err(Status::permission_denied("trusted-edge metadata leaked to backend"));
    }
    Ok(())
}

#[test]
#[ignore = "child process used by the Pingora integration test"]
fn pingora_child_process() {
    if std::env::var(CHILD_ENV).as_deref() != Ok("1") {
        return;
    }
    run_child().unwrap_or_else(|error| panic!("Pingora child failed: {error}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "multiprocess Pingora HTTP/2 qualification"]
async fn production_pingora_preserves_streaming_errors_control_auth_reuse_and_bounded_rss() -> anyhow::Result<()> {
    if !cfg!(target_os = "linux") {
        return Ok(());
    }
    let proxy = reserve_address()?;
    let backend = reserve_address()?;
    let state = format!(
        "target/pingora-integration-state-{}-{}",
        std::process::id(),
        now()?.get()
    );
    let executable = std::env::current_exe()?;
    let child = Command::new(executable)
        .args(["--ignored", "--exact", "pingora_child_process", "--nocapture"])
        .env(CHILD_ENV, "1")
        .env(LISTEN_ENV, proxy.to_string())
        .env(BACKEND_ENV, backend.to_string())
        .env(STATE_ENV, &state)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;
    let mut child = ChildGuard {
        child: Some(child),
        state: state.clone(),
    };
    wait_for_port(proxy, Duration::from_secs(15)).await?;
    wait_for_port(backend, Duration::from_secs(15)).await?;

    let channel = Endpoint::from_shared(format!("http://{proxy}"))?
        .connect_timeout(Duration::from_secs(5))
        .http2_keep_alive_interval(Duration::from_secs(5))
        .connect()
        .await?;
    let signer = EdgeRequestSigner::new(vec![1_u8; 32])?;
    let nonce = Arc::new(AtomicU64::new(1));

    let direct_result = upload(
        FlightServiceClient::new(channel.clone()),
        signer.clone(),
        Arc::clone(&nonce),
        1,
        16,
        false,
        false,
    )
    .await;
    let Err(direct) = direct_result else {
        panic!("unsigned direct traffic must be rejected");
    };
    assert_ne!(direct.code(), Code::Ok);

    if let Err(error) = upload(
        FlightServiceClient::new(channel.clone()),
        signer.clone(),
        Arc::clone(&nonce),
        0,
        16,
        true,
        false,
    )
    .await
    {
        eprintln!(
            "metrics after signed smoke failure:\n{}",
            http_get(proxy, "/metrics").await?
        );
        return Err(anyhow::anyhow!("signed smoke upload failed: {error}"));
    }

    let mut concurrent = Vec::new();
    for index in 0..8 {
        concurrent.push((
            index,
            tokio::spawn(upload(
                FlightServiceClient::new(channel.clone()),
                signer.clone(),
                Arc::clone(&nonce),
                32,
                32 * 1024,
                true,
                false,
            )),
        ));
    }
    for (index, upload) in concurrent {
        upload
            .await?
            .map_err(|error| anyhow::anyhow!("concurrent upload {index} failed: {error}"))?;
    }

    let failure_result = upload(
        FlightServiceClient::new(channel.clone()),
        signer.clone(),
        Arc::clone(&nonce),
        16,
        16 * 1024,
        true,
        true,
    )
    .await;
    let Err(failure) = failure_result else {
        panic!("backend failure should propagate");
    };
    assert_eq!(failure.code(), Code::Aborted);
    assert_eq!(
        failure
            .metadata()
            .get(BACKEND_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("catalog-a")
    );

    control_request(FlightServiceClient::new(channel.clone()), &signer, Arc::clone(&nonce)).await?;

    let cancellation = tokio::spawn(slow_upload(
        FlightServiceClient::new(channel.clone()),
        signer.clone(),
        Arc::clone(&nonce),
        1_024,
    ));
    tokio::time::sleep(Duration::from_millis(75)).await;
    cancellation.abort();
    assert!(
        cancellation.await.is_err(),
        "cancelled upload task unexpectedly completed"
    );
    upload(
        FlightServiceClient::new(channel.clone()),
        signer.clone(),
        Arc::clone(&nonce),
        4,
        8 * 1024,
        true,
        false,
    )
    .await
    .map_err(|error| anyhow::anyhow!("router did not recover after downstream cancellation: {error}"))?;

    let pid = child.id();
    let baseline = rss_bytes(pid)?;
    let large = tokio::spawn(upload(
        FlightServiceClient::new(channel.clone()),
        signer.clone(),
        Arc::clone(&nonce),
        256,
        256 * 1024,
        true,
        false,
    ));
    let mut peak = baseline;
    while !large.is_finished() {
        peak = peak.max(rss_bytes(pid)?);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    large.await??;
    peak = peak.max(rss_bytes(pid)?);
    assert!(
        peak.saturating_sub(baseline) < 128 * 1024 * 1024,
        "large stream grew router/backend child RSS by {} MiB",
        peak.saturating_sub(baseline) / (1024 * 1024)
    );

    let metrics = http_get(proxy, "/metrics").await?;
    assert!(
        metrics.contains("arrowcol_router_upstream_connections_total{reused=\"true\"}"),
        "upstream H2 connection reuse metric was absent:\n{metrics}"
    );
    let health = http_get(proxy, "/healthz").await?;
    assert!(health.contains("\"status\":\"healthy\""));
    let readiness = http_get(proxy, "/readyz").await?;
    assert!(readiness.contains("\"status\":\"ready\""));
    let capabilities = http_get(proxy, "/v1/capabilities").await?;
    assert!(capabilities.contains("\"transparent_flight_data\":true"));

    let graceful_drain = tokio::spawn(slow_upload(
        FlightServiceClient::new(channel),
        signer,
        Arc::clone(&nonce),
        128,
    ));
    tokio::time::sleep(Duration::from_millis(75)).await;
    child.terminate()?;
    graceful_drain
        .await?
        .map_err(|error| anyhow::anyhow!("active stream did not drain after GOAWAY: {error}"))?;
    child.wait(Duration::from_secs(20))?;
    Ok(())
}

fn run_child() -> anyhow::Result<()> {
    let listen: SocketAddr = std::env::var(LISTEN_ENV)?.parse()?;
    let backend: SocketAddr = std::env::var(BACKEND_ENV)?.parse()?;
    let state = std::env::var(STATE_ENV)?;
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|error| panic!("create backend runtime: {error}"));
        runtime.block_on(async move {
            Server::builder()
                .add_service(
                    FlightServiceServer::new(TestFlight)
                        .max_decoding_message_size(2 * 1024 * 1024)
                        .max_encoding_message_size(2 * 1024 * 1024),
                )
                .serve(backend)
                .await
                .unwrap_or_else(|error| panic!("serve integration backend: {error}"));
        });
    });

    let runtime = tokio::runtime::Runtime::new()?;
    let memory = Arc::new(MemoryRoutingStore::default());
    let instance = CollectorInstanceId::new();
    let current = now()?;
    runtime.block_on(async {
        memory
            .register_collector(&CollectorRegistration {
                instance_id: instance,
                instance_key: "pingora-integration".to_owned(),
                implementation_pool: ImplementationPool::Direct,
                flight_endpoint: backend,
                catalog_capable: true,
                max_concurrent_streams: positive(256)?,
                started_at_unix_ms: subtract(current, 120_000)?,
                heartbeat_at_unix_ms: current,
                lease_expires_at_unix_ms: add(current, 3_600_000)?,
            })
            .await?;
        memory.publish_pressure(&snapshot(instance, current)?).await?;
        anyhow::Ok(())
    })?;
    let store: Arc<dyn RoutingStore> = memory;
    let policy = RoutingPolicy {
        warmup_ms: 0,
        lease_ttl_ms: 300_000,
        readiness_max_staleness_ms: 300_000,
        ..RoutingPolicy::default()
    };
    let routing = Arc::new(RoutingEngine::new(store, policy)?);
    runtime.block_on(routing.refresh(current))?;
    drop(runtime);

    let receipt_config =
        SqlServerReceiptConfig::new("127.0.0.1", 9, "unused", "unused", "unused").trust_server_certificate(true);
    let receipts: Arc<dyn ReceiptStore> = Arc::new(SqlServerReceiptStore::new(receipt_config));
    let metrics = RouterMetrics::new()?;
    let authenticator: Arc<dyn Authenticator> = Arc::new(AllowAllAuthenticator);
    let router = ProductionRouter::new(
        Arc::clone(&routing),
        receipts,
        authenticator,
        Arc::new(EdgeVerifier::new(vec![1_u8; 32], 30_000)?),
        Arc::new(InternalIdentitySigner::new(vec![2_u8; 32], 30_000)?),
        upstream_config(),
        ReceiptApiConfig { max_batch_size: 16 },
        metrics.clone(),
    );
    let config = test_config(listen, state);
    let control = RouterControlLoop::new(routing, metrics, Duration::from_secs(1));
    let server = build_server(&config, router, control)?;
    server.run_forever();
}

async fn upload(
    mut client: FlightServiceClient<Channel>,
    signer: EdgeRequestSigner,
    nonce: Arc<AtomicU64>,
    chunks: usize,
    bytes_per_chunk: usize,
    signed: bool,
    fail: bool,
) -> Result<(), Status> {
    let payload = Bytes::from(vec![42_u8; bytes_per_chunk]);
    let outbound = stream::iter((0..chunks).map(move |index| FlightData {
        data_body: payload.clone(),
        app_metadata: Bytes::from(index.to_string()),
        ..FlightData::default()
    }));
    let mut request = Request::new(outbound);
    add_ingest_metadata(&mut request, &signer, nonce, signed).map_err(|error| Status::internal(error.to_string()))?;
    if fail {
        request
            .metadata_mut()
            .insert(FAIL_AFTER_HEADER, MetadataValue::from_static("3"));
    }
    let response = client.do_put(request).await?;
    if response
        .metadata()
        .get(BACKEND_HEADER)
        .and_then(|value| value.to_str().ok())
        != Some("catalog-a")
    {
        return Err(Status::internal("initial backend metadata was not preserved"));
    }
    let mut stream = response.into_inner();
    while stream.message().await?.is_some() {}
    let _trailers = stream.trailers().await?;
    Ok(())
}

async fn control_request(
    mut client: FlightServiceClient<Channel>,
    signer: &EdgeRequestSigner,
    nonce: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    let mut request = Request::new(Action {
        r#type: "create-table".to_owned(),
        body: Bytes::from_static(b"CREATE TABLE telemetry.prod.new_table (value BIGINT)"),
    });
    let path = "/arrow.flight.protocol.FlightService/DoAction";
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/grpc"));
    headers.insert("x-arrowcol-implementation-pool", HeaderValue::from_static("direct"));
    headers.insert("x-arrowcol-control-operation", HeaderValue::from_static("new_table"));
    headers.insert("x-arrowcol-catalog", HeaderValue::from_static("telemetry"));
    headers.insert("x-arrowcol-schema", HeaderValue::from_static("prod"));
    headers.insert("x-arrowcol-table", HeaderValue::from_static("new_table"));
    sign_and_copy(request.metadata_mut(), signer, &Method::POST, path, headers, nonce)?;
    request
        .metadata_mut()
        .insert("x-arrowcol-implementation-pool", MetadataValue::from_static("direct"));
    request
        .metadata_mut()
        .insert("x-arrowcol-control-operation", MetadataValue::from_static("new_table"));
    request
        .metadata_mut()
        .insert("x-arrowcol-catalog", MetadataValue::from_static("telemetry"));
    request
        .metadata_mut()
        .insert("x-arrowcol-schema", MetadataValue::from_static("prod"));
    request
        .metadata_mut()
        .insert("x-arrowcol-table", MetadataValue::from_static("new_table"));
    let response = client.do_action(request).await?;
    let results: Vec<_> = response.into_inner().collect::<Vec<_>>().await;
    assert_eq!(results.len(), 1);
    let result = results
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("control response was empty"))??;
    assert_eq!(result.body, Bytes::from_static(b"catalog-control-ok"));
    Ok(())
}

async fn slow_upload(
    mut client: FlightServiceClient<Channel>,
    signer: EdgeRequestSigner,
    nonce: Arc<AtomicU64>,
    chunks: usize,
) -> Result<(), Status> {
    let payload = Bytes::from(vec![17_u8; 64 * 1024]);
    let outbound = stream::unfold((0_usize, payload), move |(index, payload)| async move {
        if index >= chunks {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        Some((
            FlightData {
                data_body: payload.clone(),
                app_metadata: Bytes::from(index.to_string()),
                ..FlightData::default()
            },
            (index + 1, payload),
        ))
    });
    let mut request = Request::new(outbound);
    add_ingest_metadata(&mut request, &signer, nonce, true).map_err(|error| Status::internal(error.to_string()))?;
    let mut response = client.do_put(request).await?.into_inner();
    while response.message().await?.is_some() {}
    Ok(())
}

fn add_ingest_metadata<S>(
    request: &mut Request<S>,
    signer: &EdgeRequestSigner,
    nonce: Arc<AtomicU64>,
    signed: bool,
) -> anyhow::Result<()> {
    let path = "/arrow.flight.protocol.FlightService/DoPut";
    let mut headers = HeaderMap::new();
    headers.insert("content-type", HeaderValue::from_static("application/grpc"));
    headers.insert("x-arrowcol-protocol-version", HeaderValue::from_static("1"));
    headers.insert("x-arrowcol-implementation-pool", HeaderValue::from_static("direct"));
    headers.insert("x-arrowcol-catalog", HeaderValue::from_static("telemetry"));
    headers.insert("x-arrowcol-schema", HeaderValue::from_static("prod"));
    headers.insert("x-arrowcol-table", HeaderValue::from_static("traces"));
    headers.insert(
        "x-arrowcol-schema-fingerprint",
        HeaderValue::from_static("sha256:0000000000000000000000000000000000000000000000000000000000000000"),
    );
    if signed {
        sign_and_copy(request.metadata_mut(), signer, &Method::POST, path, headers, nonce)?;
    }
    for (key, value) in [
        ("x-arrowcol-protocol-version", "1"),
        ("x-arrowcol-implementation-pool", "direct"),
        ("x-arrowcol-catalog", "telemetry"),
        ("x-arrowcol-schema", "prod"),
        ("x-arrowcol-table", "traces"),
        (
            "x-arrowcol-schema-fingerprint",
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        ),
    ] {
        request.metadata_mut().insert(
            key,
            MetadataValue::from_str(value).map_err(|error| anyhow::anyhow!("invalid test metadata {key}: {error}"))?,
        );
    }
    Ok(())
}

fn sign_and_copy(
    metadata: &mut MetadataMap,
    signer: &EdgeRequestSigner,
    method: &Method,
    path: &str,
    mut headers: HeaderMap,
    nonce: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    let nonce = format!("integration-{}", nonce.fetch_add(1, Ordering::Relaxed));
    let timestamp = now()?;
    insert_edge_signature_headers(&mut headers, signer, method, path, timestamp, &nonce)?;
    for key in [
        "x-arrowcol-edge-timestamp",
        "x-arrowcol-edge-nonce",
        "x-arrowcol-edge-signature",
    ] {
        let value = headers
            .get(key)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| anyhow::anyhow!("signed test header {key} is missing"))?;
        metadata.insert(
            key,
            MetadataValue::from_str(value)
                .map_err(|error| anyhow::anyhow!("invalid signed metadata {key}: {error}"))?,
        );
    }
    Ok(())
}

async fn http_get(address: SocketAddr, path: &str) -> anyhow::Result<String> {
    let mut stream = tokio::net::TcpStream::connect(address).await?;
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes())
        .await?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    Ok(String::from_utf8(bytes)?)
}

fn test_config(listen: SocketAddr, state_directory: String) -> RouterConfig {
    RouterConfig {
        server: ServerConfig {
            listen,
            threads: 2,
            listener_tasks_per_fd: 1,
            grace_period_seconds: 1,
            graceful_shutdown_timeout_seconds: 15,
            state_directory: state_directory.into(),
            h2: H2Config {
                max_concurrent_streams: 256,
                stream_window_bytes: 1024 * 1024,
                connection_window_bytes: 16 * 1024 * 1024,
                idle_timeout_seconds: 30,
            },
        },
        sql_server: SqlServerConfig {
            host: "unused".to_owned(),
            port: 9,
            database: "unused".to_owned(),
            username: "unused".to_owned(),
            password_file: "unused".into(),
            trust_server_certificate: true,
            connect_timeout_ms: 100,
            command_timeout_ms: 100,
        },
        routing: RoutingConfig {
            refresh_interval_ms: 1_000,
            max_snapshot_age_ms: 300_000,
            max_future_skew_ms: 2_000,
            max_table_entries: 256,
            max_oldest_buffer_age_ms: 86_400_000,
            hard_pressure_bps: 9_500,
            warmup_ms: 0,
            minimum_warmup_bps: 1_000,
            lease_ttl_ms: 300_000,
            readiness_max_staleness_ms: 300_000,
        },
        auth: AuthConfig {
            authenticator: "allow_all".to_owned(),
            edge_secret_file: "unused-edge".into(),
            internal_signing_secret_file: "unused-internal".into(),
            edge_signature_max_age_ms: 30_000,
            internal_identity_ttl_ms: 30_000,
        },
        upstream: upstream_config(),
        receipts: ReceiptApiConfig { max_batch_size: 16 },
        telemetry: TelemetryConfig {
            log_filter: "info".to_owned(),
            json_logs: false,
        },
    }
}

fn upstream_config() -> UpstreamConfig {
    UpstreamConfig {
        tls: false,
        sni: String::new(),
        max_h2_streams: 128,
        stream_window_bytes: 1024 * 1024,
        connection_window_bytes: 16 * 1024 * 1024,
        connection_timeout_ms: 1_000,
        total_connection_timeout_ms: 3_000,
        idle_timeout_seconds: 60,
        ping_interval_seconds: Some(15),
    }
}

fn snapshot(instance_id: CollectorInstanceId, observed_at: UnixMillis) -> anyhow::Result<RichPressureSnapshot> {
    Ok(RichPressureSnapshot {
        container_memory: None,
        effective_cpu_millicores: None,
        instance_id,
        observed_at_unix_ms: observed_at,
        cpu_utilization_bps: BasisPoints::new(100)?,
        rss_bytes: measurement(100)?,
        rss_limit_bytes: positive(1_000)?,
        active_streams: gauge(0, 256)?,
        received_bytes: measurement(0)?,
        received_rows: measurement(0)?,
        received_batches: measurement(0)?,
        buffered_bytes: gauge(0, 1_000)?,
        buffered_rows: gauge(0, 1_000)?,
        buffered_batches: gauge(0, 100)?,
        available_permits: gauge(256, 256)?,
        pending_commits: gauge(0, 16)?,
        oldest_buffer_age_ms: measurement(0)?,
        delta_pressure_bps: BasisPoints::new(0)?,
        quiver_pressure_bps: BasisPoints::new(0)?,
        dfe_pressure_bps: BasisPoints::new(0)?,
        delta_state: SubsystemState::Ready,
        quiver_state: SubsystemState::Ready,
        dfe_state: SubsystemState::Disabled,
        accepting_deliveries: true,
        tables: Vec::new(),
    })
}

fn reserve_address() -> io::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.local_addr()
}

async fn wait_for_port(address: SocketAddr, timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("port {address} did not become ready within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn rss_bytes(pid: u32) -> anyhow::Result<usize> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    let line = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .ok_or_else(|| anyhow::anyhow!("VmRSS was absent for process {pid}"))?;
    let kib = line
        .split_ascii_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("VmRSS value was malformed"))?
        .parse::<usize>()?;
    Ok(kib.saturating_mul(1024))
}

fn now() -> anyhow::Result<UnixMillis> {
    let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    Ok(UnixMillis::new(u64::try_from(millis)?)?)
}

fn add(value: UnixMillis, millis: u64) -> anyhow::Result<UnixMillis> {
    Ok(UnixMillis::new(
        value
            .get()
            .checked_add(millis)
            .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))?,
    )?)
}

fn subtract(value: UnixMillis, millis: u64) -> anyhow::Result<UnixMillis> {
    Ok(UnixMillis::new(value.get().saturating_sub(millis))?)
}

fn measurement(value: u64) -> anyhow::Result<Measurement> {
    Ok(Measurement::new(value)?)
}

fn positive(value: u64) -> anyhow::Result<PositiveMeasurement> {
    Ok(PositiveMeasurement::new(value)?)
}

fn gauge(current: u64, limit: u64) -> anyhow::Result<PressureGauge> {
    Ok(PressureGauge::new(measurement(current)?, positive(limit)?)?)
}

struct ChildGuard {
    child: Option<Child>,
    state: String,
}

impl ChildGuard {
    fn id(&self) -> u32 {
        self.child.as_ref().map_or(0, Child::id)
    }

    fn terminate(&mut self) -> anyhow::Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        let status = Command::new("/bin/kill")
            .arg("-TERM")
            .arg(child.id().to_string())
            .status()?;
        if !status.success() {
            anyhow::bail!("SIGTERM failed for Pingora child {}", child.id());
        }
        Ok(())
    }

    fn wait(&mut self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        loop {
            if let Some(status) = child.try_wait()? {
                if !status.success() {
                    anyhow::bail!("Pingora child exited with {status}");
                }
                self.child = None;
                let _ = std::fs::remove_dir_all(&self.state);
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!("Pingora child did not exit within {timeout:?}");
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.state);
    }
}
