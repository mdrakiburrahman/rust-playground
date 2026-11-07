use std::{net::SocketAddr, path::PathBuf};

use anyhow::Result;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrowcol_catalog_client::rpc::{self, schema_catalog_client::SchemaCatalogClient};
use arrowcol_catalog_rust::{CatalogConfig, CatalogService};
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "ARROWCOL_CATALOG_LISTEN", default_value = "0.0.0.0:8817")]
    listen: SocketAddr,
    #[arg(long, env = "ARROWCOL_CATALOG_WAREHOUSE_ROOT", default_value = "/data/warehouse")]
    warehouse_root: String,
    #[arg(long, env = "ARROWCOL_CATALOG_STATE_DIR", default_value = "/data/catalog")]
    state_dir: PathBuf,
    #[arg(long, env = "ARROWCOL_CATALOG_DDL_DIR")]
    ddl_dir: Option<PathBuf>,
    #[arg(long)]
    healthcheck: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.healthcheck {
        let endpoint = format!("http://127.0.0.1:{}", args.listen.port());
        let check = async {
            SchemaCatalogClient::connect(endpoint)
                .await?
                .get_current_event_id(rpc::Empty {})
                .await?;
            Ok::<_, anyhow::Error>(())
        };
        return tokio::time::timeout(std::time::Duration::from_secs(3), check).await?;
    }
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let service = CatalogService::open(CatalogConfig {
        warehouse_root: args.warehouse_root,
        state_dir: args.state_dir,
        ddl_dir: args.ddl_dir,
        retained_events: 4096,
    })
    .await?;
    tracing::info!(listen = %args.listen, "catalog Flight SQL and CES ready");
    let shutdown_service = service.clone();
    let (signal_sender, signal_receiver) = tokio::sync::oneshot::channel();
    tonic::transport::Server::builder()
        .add_service(
            FlightServiceServer::new(service.clone())
                .max_decoding_message_size(8 * 1024 * 1024)
                .max_encoding_message_size(8 * 1024 * 1024),
        )
        .add_service(
            rpc::schema_catalog_server::SchemaCatalogServer::new(service)
                .max_decoding_message_size(8 * 1024 * 1024)
                .max_encoding_message_size(8 * 1024 * 1024),
        )
        .serve_with_shutdown(args.listen, async move {
            #[cfg(unix)]
            let signal = tokio::select! {
                result = tokio::signal::ctrl_c() => result,
                _ = terminate.recv() => Ok(()),
            };
            #[cfg(not(unix))]
            let signal = tokio::signal::ctrl_c().await;
            tracing::info!("catalog draining Flight SQL and CES");
            shutdown_service.shutdown_watch_streams();
            let _ = signal_sender.send(signal);
        })
        .await?;
    signal_receiver.await??;
    arrow_deltalake::publisher::PublisherSession::close_shared_async().await?;
    tracing::info!("catalog publisher sessions drained and closed");
    Ok(())
}
