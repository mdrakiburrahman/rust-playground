use std::{net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result};
use arrow_flight::flight_service_server::FlightServiceServer;
use arrowcol_catalog_client::rpc::schema_catalog_client::SchemaCatalogClient;
use arrowcol_catalog_client::{InternalRequestSigner, rpc};
use arrowcol_catalog_rust::{CatalogCesService, CatalogConfig, CatalogService, DdlIdentityPolicy, DdlIdentityVerifier};
use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "snake_case")]
enum DdlAuthMode {
    SignedIdentity,
    DevOnlyAllowUnsigned,
}

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
    #[arg(
        long,
        env = "ARROWCOL_CATALOG_DDL_AUTH_MODE",
        value_enum,
        default_value = "signed_identity"
    )]
    ddl_auth_mode: DdlAuthMode,
    #[arg(long, env = "ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE")]
    ddl_identity_secret_file: Option<PathBuf>,
    #[arg(long)]
    ingestion_layout: bool,
    #[arg(long)]
    healthcheck: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.healthcheck {
        let endpoint = format!("http://127.0.0.1:{}", args.listen.port());
        let check = async {
            let mut client = SchemaCatalogClient::connect(endpoint).await?;
            let mut request = tonic::Request::new(rpc::Empty {});
            if args.ddl_auth_mode == DdlAuthMode::SignedIdentity {
                let path = args
                    .ddl_identity_secret_file
                    .as_deref()
                    .context("ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE or --ddl-identity-secret-file is required")?;
                InternalRequestSigner::from_file(path)?
                    .sign_message(&mut request, "/arrowcol.catalog.v1.SchemaCatalog/GetCurrentEventId")?;
            }
            client.get_current_event_id(request).await?;
            Ok::<_, anyhow::Error>(())
        };
        return tokio::time::timeout(std::time::Duration::from_secs(3), check).await?;
    }
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let ddl_identity_policy = match args.ddl_auth_mode {
        DdlAuthMode::SignedIdentity => {
            let path = args
                .ddl_identity_secret_file
                .as_deref()
                .context("ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE or --ddl-identity-secret-file is required")?;
            DdlIdentityPolicy::require_signed(DdlIdentityVerifier::from_file(path)?)
        }
        DdlAuthMode::DevOnlyAllowUnsigned => {
            tracing::warn!("dev_only_allow_unsigned bypasses catalog DDL identity verification; not for production");
            DdlIdentityPolicy::dev_only_allow_unsigned()
        }
    };
    let service = CatalogService::open(CatalogConfig {
        warehouse_root: args.warehouse_root,
        state_dir: args.state_dir,
        ddl_dir: args.ddl_dir,
        retained_events: 4096,
        ingestion_layout: args.ingestion_layout,
    })
    .await?;
    tracing::info!(listen = %args.listen, "catalog Flight SQL and CES ready");
    let shutdown_service = service.clone();
    let (signal_sender, signal_receiver) = tokio::sync::oneshot::channel();
    tonic::transport::Server::builder()
        .add_service(
            FlightServiceServer::new(service.flight_service_with_ddl_identity(ddl_identity_policy.clone()))
                .max_decoding_message_size(8 * 1024 * 1024)
                .max_encoding_message_size(8 * 1024 * 1024),
        )
        .add_service(
            rpc::schema_catalog_server::SchemaCatalogServer::new(CatalogCesService::new(service, ddl_identity_policy))
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
