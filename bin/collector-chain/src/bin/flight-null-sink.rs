use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrowcol_catalog::{CatalogControlPlane, CatalogStore, DdlParser, MemoryCatalogStore};
use arrowcol_flight::{
    AllowAllAuthenticator, ArrowColFlightService, BatchAdmission, PutResultEmitter, RuntimeLimits, RuntimeResult,
    StreamAbortReason, StreamCompletion, StreamContext, StreamLifecycle, StreamSummary,
};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, ImplementationPool, PrincipalId, PrincipalIssuer, UnixMillis,
};
use clap::Parser;
use futures::stream;
use serde_json::json;

#[derive(Parser)]
#[command(about = "Bounded loopback-only Flight decode/discard sink; acknowledgements are never durable")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:0")]
    bind: SocketAddr,
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=1024))]
    runtime_threads: u16,
    #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..=1800))]
    duration_secs: u64,
    #[arg(long, default_value_t = 16_777_216, value_parser = clap::value_parser!(u32).range(1_048_576..=1_073_741_824))]
    max_message_bytes: u32,
    #[arg(long, default_value_t = 16_777_216, value_parser = clap::value_parser!(u32).range(1..=2_147_483_647))]
    stream_window_bytes: u32,
    #[arg(long, default_value_t = 67_108_864, value_parser = clap::value_parser!(u32).range(1..=2_147_483_647))]
    connection_window_bytes: u32,
    #[arg(long)]
    ready_file: std::path::PathBuf,
}

struct Sink(Arc<AtomicU64>);
struct DiscardStream {
    total: Arc<AtomicU64>,
    rows: u64,
}

#[async_trait::async_trait]
impl BatchAdmission for Sink {
    async fn begin(&self, _: StreamContext, _: Arc<dyn PutResultEmitter>) -> RuntimeResult<Box<dyn StreamLifecycle>> {
        Ok(Box::new(DiscardStream {
            total: Arc::clone(&self.0),
            rows: 0,
        }))
    }
}

#[async_trait::async_trait]
impl StreamLifecycle for DiscardStream {
    async fn push_batch(&mut self, batch: RecordBatch) -> RuntimeResult<()> {
        self.rows += batch.num_rows() as u64;
        Ok(())
    }

    async fn complete(&mut self, _: StreamSummary) -> RuntimeResult<StreamCompletion> {
        self.total.fetch_add(self.rows, Ordering::Relaxed);
        Ok(StreamCompletion::best_effort())
    }

    async fn abort(self: Box<Self>, _: StreamAbortReason) {}
}

fn main() -> Result<()> {
    let args = Args::parse();
    anyhow::ensure!(args.bind.ip().is_loopback(), "null sink must bind a loopback address");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(usize::from(args.runtime_threads))
        .enable_all()
        .build()?
        .block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    let rows = Arc::new(AtomicU64::new(0));
    let store: Arc<dyn CatalogStore> = Arc::new(MemoryCatalogStore::new());
    let principal = AuthenticatedPrincipal::new(
        PrincipalId::new("synthetic-null-sink")?,
        PrincipalIssuer::new("synthetic-null-sink")?,
        Default::default(),
    )?;
    let auth = AuthContext::new(principal, AuthMethod::WorkloadIdentity, UnixMillis::new(1)?, None)?;
    let service = ArrowColFlightService::new(
        ImplementationPool::Dataflow,
        Arc::clone(&store),
        Arc::new(AllowAllAuthenticator::new(auth)),
        Arc::new(Sink(Arc::clone(&rows))),
        RuntimeLimits {
            max_message_bytes: args.max_message_bytes as usize,
            ..RuntimeLimits::default()
        },
    )?
    .with_ddl_handler(Arc::new(CatalogControlPlane::new(DdlParser::default(), store)))
    .into_tonic_service();
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let ready = json!({
        "endpoint": endpoint, "pid": std::process::id(), "runtime_threads": args.runtime_threads,
        "ready_unix_ms": SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        "scope": "synthetic_Flight_decode_and_discard_no_Delta_no_durability",
    });
    let mut ready_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args.ready_file)
        .context("create run-owned null sink ready file")?;
    std::io::Write::write_all(&mut ready_file, format!("{ready}\n").as_bytes())?;
    println!("FLIGHT_NULL_SINK_READY {ready}");
    let incoming = stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    tonic::transport::Server::builder()
        .initial_stream_window_size(args.stream_window_bytes)
        .initial_connection_window_size(args.connection_window_bytes)
        .add_service(service)
        .serve_with_incoming_shutdown(incoming, tokio::time::sleep(Duration::from_secs(args.duration_secs)))
        .await?;
    println!(
        "FLIGHT_NULL_SINK_COMPLETED {}",
        json!({
            "decoded_completed_rows": rows.load(Ordering::Relaxed),
            "scope": "sink_lifetime_stream_completion_rows_including_client_warmup_and_drain_not_durable",
        })
    );
    Ok(())
}
