use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use arrowcol_receipts::{ReceiptStore, SqlServerReceiptConfig, SqlServerReceiptStore};
use arrowcol_router_rust::{
    AllowAllAuthenticator, Authenticator, Cli, EdgeVerifier, InternalIdentitySigner, ProductionRouter, RouterConfig,
    RouterControlLoop, RouterMetrics, build_server,
};
use arrowcol_routing::{
    ControlCredential, DiskRoutingStore, PlacementPolicy, RemoteRoutingStore, RoutingEngine, RoutingStore,
    SqlServerRoutingConfig, SqlServerRoutingStore, serve_control,
};
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    if let Ok(config) = std::env::var("ARROWCOL_DELTA_CONFIG") {
        return arrowcol_router_rust::delta::run(&config);
    }
    let cli = Cli::parse();
    let config = RouterConfig::load(&cli.config)?;
    initialize_tracing(&config)?;
    if cli.validate_and_exit {
        info!(config = %cli.config.display(), "router configuration is valid");
        return Ok(());
    }

    let edge_secret = read_secret(&config.auth.edge_secret_file)?;
    let internal_secret = read_secret(&config.auth.internal_signing_secret_file)?;
    let routing_sql = SqlServerRoutingConfig::with_password_file(
        &config.sql_server.host,
        config.sql_server.port,
        &config.sql_server.database,
        &config.sql_server.username,
        &config.sql_server.password_file,
    )?
    .trust_server_certificate(config.sql_server.trust_server_certificate)
    .with_timeouts(
        Duration::from_millis(config.sql_server.connect_timeout_ms),
        Duration::from_millis(config.sql_server.command_timeout_ms),
    );
    let receipt_sql = SqlServerReceiptConfig::with_password_file(
        &config.sql_server.host,
        config.sql_server.port,
        &config.sql_server.database,
        &config.sql_server.username,
        &config.sql_server.password_file,
    )?
    .trust_server_certificate(config.sql_server.trust_server_certificate)
    .with_timeouts(
        Duration::from_millis(config.sql_server.connect_timeout_ms),
        Duration::from_millis(config.sql_server.command_timeout_ms),
    )
    .with_batch_limits(256, config.receipts.max_batch_size)?;

    let mode = std::env::var("ARROWCOL_ROUTING_MODE").unwrap_or_else(|_| "sql".to_owned());
    let routing_store: Arc<dyn RoutingStore> = match mode.as_str() {
        "sql" => Arc::new(SqlServerRoutingStore::new(routing_sql)),
        "controller" => {
            let directory = std::env::var("ARROWCOL_ROUTING_DIRECTORY")
                .context("controller requires ARROWCOL_ROUTING_DIRECTORY")?;
            let placement = match std::env::var("ARROWCOL_PLACEMENT_POLICY")
                .as_deref()
                .unwrap_or("least_pressure")
            {
                "least_pressure" => PlacementPolicy::LeastPressure,
                "weighted_rendezvous" => PlacementPolicy::WeightedRendezvous,
                _ => anyhow::bail!("unsupported placement policy"),
            };
            let store: Arc<dyn RoutingStore> =
                Arc::new(DiskRoutingStore::open(directory, config.routing.policy()?, placement)?);
            let listen = std::env::var("ARROWCOL_CONTROL_LISTEN")
                .context("controller requires private ARROWCOL_CONTROL_LISTEN")?
                .parse()?;
            let credentials_file =
                std::env::var("ARROWCOL_CONTROL_CREDENTIALS_FILE").context("controller requires credentials file")?;
            #[derive(serde::Deserialize)]
            #[serde(deny_unknown_fields)]
            struct CredentialFile {
                role: String,
                instance_key: Option<String>,
                token_file: std::path::PathBuf,
            }
            let entries: Vec<CredentialFile> = serde_json::from_slice(&std::fs::read(credentials_file)?)?;
            let credentials = entries
                .into_iter()
                .map(|entry| {
                    Ok(ControlCredential {
                        token: String::from_utf8(read_secret(&entry.token_file)?)?,
                        role: entry.role,
                        instance_key: entry.instance_key,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let authority = Arc::clone(&store);
            // This listener is independent of ingestion readiness and starts before Pingora.
            std::thread::Builder::new()
                .name("routing-controller".to_owned())
                .spawn(move || {
                    let result = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .build()
                        .map_err(anyhow::Error::from)
                        .and_then(|runtime| {
                            runtime
                                .block_on(serve_control(listen, authority, credentials))
                                .map_err(anyhow::Error::from)
                        });
                    if let Err(error) = result {
                        tracing::error!(%error, "private routing controller stopped");
                        std::process::exit(1);
                    }
                })?;
            store
        }
        "data_only" => {
            ensure!(
                std::env::var_os("ARROWCOL_ROUTING_DIRECTORY").is_none(),
                "data-only router must not configure a disk directory"
            );
            let endpoint = std::env::var("ARROWCOL_CONTROLLER_ENDPOINT")
                .context("data-only router requires controller endpoint")?;
            let token_file =
                std::env::var("ARROWCOL_CONTROL_TOKEN_FILE").context("data-only router requires control token file")?;
            Arc::new(RemoteRoutingStore::new(
                &endpoint,
                String::from_utf8(read_secret(std::path::Path::new(&token_file))?)?,
            )?)
        }
        _ => anyhow::bail!("ARROWCOL_ROUTING_MODE must be sql, controller or data_only"),
    };
    let receipt_store: Arc<dyn ReceiptStore> = Arc::new(SqlServerReceiptStore::new(receipt_sql));
    let routing = Arc::new(RoutingEngine::new(routing_store, config.routing.policy()?)?);
    let metrics = RouterMetrics::new()?;
    let authenticator: Arc<dyn Authenticator> = Arc::new(AllowAllAuthenticator);
    let edge_verifier = Arc::new(EdgeVerifier::new(edge_secret, config.auth.edge_signature_max_age_ms)?);
    let internal_signer = Arc::new(InternalIdentitySigner::new(
        internal_secret,
        config.auth.internal_identity_ttl_ms,
    )?);
    let router = ProductionRouter::new(
        Arc::clone(&routing),
        receipt_store,
        authenticator,
        edge_verifier,
        internal_signer,
        config.upstream.clone(),
        config.receipts.clone(),
        metrics.clone(),
    );
    let control_loop = RouterControlLoop::new(
        routing,
        metrics,
        Duration::from_millis(config.routing.refresh_interval_ms),
    )
    .with_refresh_only(mode != "sql");
    let server = build_server(&config, router, control_loop)?;
    info!(listen = %config.server.listen, "starting Arrowcol production router behind external TLS termination");
    server.run_forever();
}

fn initialize_tracing(config: &RouterConfig) -> Result<()> {
    let filter = EnvFilter::try_new(&config.telemetry.log_filter)
        .with_context(|| format!("parse telemetry.log_filter {:?}", config.telemetry.log_filter))?;
    if config.telemetry.json_logs {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .try_init()
            .map_err(|error| anyhow::anyhow!("initialize JSON tracing: {error}"))?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .map_err(|error| anyhow::anyhow!("initialize tracing: {error}"))?;
    }
    Ok(())
}

fn read_secret(path: &std::path::Path) -> Result<Vec<u8>> {
    let value = std::fs::read(path).with_context(|| format!("read secret file {}", path.display()))?;
    let value = value
        .strip_suffix(b"\n")
        .and_then(|value| value.strip_suffix(b"\r").or(Some(value)))
        .unwrap_or(&value);
    ensure!(!value.is_empty(), "secret file {} is empty", path.display());
    ensure!(
        !value.contains(&b'\n') && !value.contains(&b'\r'),
        "secret file {} must contain one line",
        path.display()
    );
    Ok(value.to_vec())
}
