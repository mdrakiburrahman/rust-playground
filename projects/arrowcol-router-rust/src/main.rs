use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use arrowcol_router_rust::{
    AllowAllAuthenticator, Authenticator, Cli, EdgeVerifier, InternalIdentitySigner, ProductionRouter, RouterConfig,
    RouterControlLoop, RouterMetrics, build_server,
};
use arrowcol_routing::{RoutingEngine, RoutingStore, SqlServerRoutingConfig, SqlServerRoutingStore};
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
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
    let routing_store: Arc<dyn RoutingStore> = Arc::new(SqlServerRoutingStore::new(routing_sql));
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
        authenticator,
        edge_verifier,
        internal_signer,
        config.upstream.clone(),
        metrics.clone(),
    );
    let control_loop = RouterControlLoop::new(
        routing,
        metrics,
        Duration::from_millis(config.routing.refresh_interval_ms),
    );
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
