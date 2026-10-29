//! Explicit Pingora + router-hosted placement arm. Never forwards placement to
//! the ADR controller; only the typed authority grants ownership.
use anyhow::{Context, Result, ensure};
use arrowcol_authority::{
    delta_runtime::{Control, PrivateConfig},
    delta_store::Snapshot,
    *,
};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use pingora::{apps::HttpServerOptions, prelude::*, server::configuration::ServerConf, upstreams::peer::ALPN};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

fn select(state: &Snapshot, excluded: Option<&str>) -> Result<Writer> {
    let mut candidates = state
        .members
        .values()
        .filter(|m| {
            m.member.eligible
                && m.expires > 0
                && m.member.pressure < 0.95
                && Some(m.member.writer.collector.as_str()) != excluded
        })
        .collect::<Vec<_>>();
    let score = |m: &arrowcol_authority::delta_store::Session| {
        let reservations = state
            .tables
            .values()
            .filter(|t| {
                t.record.lifecycle != Lifecycle::Active
                    && t.reservation
                        .as_ref()
                        .is_some_and(|r| r.target.collector == m.member.writer.collector)
            })
            .count();
        arrowcol_routing::delta::reserved_pressure(m.member.pressure, reservations, 16)
    };
    candidates.sort_by(|a, b| {
        score(a)
            .total_cmp(&score(b))
            .then(a.member.writer.collector.cmp(&b.member.writer.collector))
    });
    Ok(candidates
        .first()
        .context("no fresh eligible capacity")?
        .member
        .writer
        .clone())
}

struct DeltaRouter {
    config: Config,
    control: ControlPlane,
    edge: crate::EdgeVerifier,
    cache: Mutex<BTreeMap<String, (Instant, Assignment)>>,
}

enum ControlPlane {
    Local(Arc<Control>),
    Remote(Client),
}

impl ControlPlane {
    async fn resolve(&self, key: String) -> Result<Assignment> {
        let operation = Operation::Resolve { key };
        let value = match self {
            Self::Local(control) => control.apply("gateway", operation).await?,
            Self::Remote(client) => client.call(&operation).await?,
        };
        Ok(serde_json::from_value(value)?)
    }
}

fn validate_data_only(config: &Config, private: bool, listener: bool) -> Result<()> {
    ensure!(
        !private && !listener,
        "data-only router cannot configure private authority or control listener"
    );
    ensure!(
        config.etcd.is_empty()
            && config.etcd_user.is_empty()
            && config.etcd_password.is_empty()
            && config.operator_token.is_empty()
            && config.collectors.is_empty(),
        "data-only router must not receive authority/operator/member credentials"
    );
    ensure!(
        !config.gateway_token.is_empty(),
        "data-only router requires gateway control credential"
    );
    ensure!(
        config.controller.starts_with("http://") || config.controller.starts_with("https://"),
        "data-only router requires primary router control endpoint"
    );
    Ok(())
}

#[derive(Default)]
struct ContextData {
    endpoint: String,
    headers: Vec<(&'static str, String)>,
}

fn error(e: impl std::fmt::Display) -> Box<Error> {
    Error::explain(ErrorType::HTTPStatus(503), e.to_string())
}

#[async_trait]
impl ProxyHttp for DeltaRouter {
    type CTX = ContextData;
    fn new_ctx(&self) -> Self::CTX {
        ContextData::default()
    }

    async fn request_filter(&self, session: &mut Session, context: &mut Self::CTX) -> pingora::Result<bool> {
        let result: Result<()> = async {
            let request = session.req_header();
            ensure!(
                request.method == http::Method::POST
                    && request.uri.path() == "/arrow.flight.protocol.FlightService/DoPut",
                "only native DoPut"
            );
            for h in ["x-adr-route", "x-adr-assertion", "x-adr-signature"] {
                ensure!(!request.headers.contains_key(h), "external route assertion");
            }
            self.edge
                .verify(
                    &request.method,
                    request.uri.path(),
                    &request.headers,
                    arrowcol_protocol::UnixMillis::new(now_ms())?,
                )
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let header = |key| request.headers.get(key).and_then(|v| v.to_str().ok()).unwrap_or("");
            let table = self
                .config
                .tables
                .iter()
                .find(|t| {
                    t.path
                        .iter()
                        .zip(["x-arrowcol-catalog", "x-arrowcol-schema", "x-arrowcol-table"])
                        .all(|(value, key)| value == header(key))
                })
                .context("table unauthorized")?;
            let mut cache = self.cache.lock().await;
            let mut cache_hit = false;
            let assignment = if let Some((at, a)) = cache.get(&table.key)
                && at.elapsed() < Duration::from_millis(300)
                && a.record.lifecycle == Lifecycle::Active
            {
                cache_hit = true;
                a.clone()
            } else {
                let a = self.control.resolve(table.key.clone()).await?;
                cache.insert(table.key.clone(), (Instant::now(), a.clone()));
                a
            };
            ensure!(assignment.record.lifecycle == Lifecycle::Active, "owner recovering");
            eprintln!(
                "{}",
                serde_json::json!({
                    "event":"delta_route","observed_ms":now_ms(),"key":table.key,
                    "cache_hit":cache_hit,"epoch":assignment.record.epoch,
                    "data_only":matches!(self.control, ControlPlane::Remote(_))
                })
            );
            let route = STANDARD.encode(serde_json::to_vec(&assignment)?);
            let assertion = format!("{}\n{}\n{}", now_ms(), header("x-arrowcol-edge-signature"), route);
            context.headers = vec![
                ("x-adr-route", route),
                ("x-adr-assertion", STANDARD.encode(&assertion)),
                (
                    "x-adr-signature",
                    flight::sign(&self.config.internal_secret, &assertion)?,
                ),
            ];
            context.endpoint = assignment.record.endpoint;
            Ok(())
        }
        .await;
        if let Err(e) = result {
            let mut response = ResponseHeader::build(200, Some(3))?;
            response.insert_header("content-type", "application/grpc")?;
            response.insert_header("grpc-status", "14")?;
            response.insert_header("grpc-message", e.to_string().replace('\n', " "))?;
            session.write_response_header(Box::new(response), true).await?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn upstream_peer(&self, _: &mut Session, context: &mut Self::CTX) -> pingora::Result<Box<HttpPeer>> {
        let address = context
            .endpoint
            .strip_prefix("http://")
            .ok_or_else(|| error("invalid endpoint"))?;
        let address = tokio::net::lookup_host(address)
            .await
            .map_err(error)?
            .next()
            .ok_or_else(|| error("endpoint DNS"))?;
        let mut peer = HttpPeer::new(address, false, String::new());
        peer.options.alpn = ALPN::H2;
        peer.options.connection_timeout = Some(Duration::from_secs(2));
        peer.options.total_connection_timeout = Some(Duration::from_secs(3));
        peer.options.read_timeout = Some(Duration::from_secs(30));
        peer.options.max_h2_streams = 16;
        Ok(Box::new(peer))
    }

    async fn upstream_request_filter(
        &self,
        _: &mut Session,
        request: &mut RequestHeader,
        context: &mut Self::CTX,
    ) -> pingora::Result<()> {
        for (key, value) in &context.headers {
            request.insert_header(*key, value)?;
        }
        Ok(())
    }

    fn fail_to_connect(&self, _: &mut Session, _: &HttpPeer, _: &mut Self::CTX, mut e: Box<Error>) -> Box<Error> {
        e.set_retry(false);
        e
    }
    fn error_while_proxy(
        &self,
        _: &HttpPeer,
        _: &mut Session,
        mut e: Box<Error>,
        _: &mut Self::CTX,
        _: bool,
    ) -> Box<Error> {
        e.set_retry(false);
        e
    }
}

/// # Errors
/// Rejects invalid explicit configuration, live controller conflicts and bind errors.
pub fn run(config_path: &str) -> Result<()> {
    ensure!(
        std::env::var("ARROWCOL_PLACEMENT_POLICY").unwrap_or_else(|_| "least_pressure".into()) == "least_pressure",
        "Delta arm currently supports least_pressure only"
    );
    let config = Config::load(config_path)?;
    ensure!(
        config.delta.is_some(),
        "Delta router requires explicit Delta configuration"
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let control = match std::env::var("ARROWCOL_DELTA_MODE").as_deref().unwrap_or("controller") {
        "controller" => {
            let private = PrivateConfig::load(&std::env::var("ARROWCOL_DELTA_PRIVATE")?)?;
            let control = runtime.block_on(async {
                let store = private.store(&config).await?;
                Control::start(config.clone(), store, select).await
            })?;
            let listener = std::env::var("ARROWCOL_CONTROL_LISTEN").unwrap_or_else(|_| "0.0.0.0:50060".into());
            let background = Arc::clone(&control);
            runtime.spawn(async move {
                if let Err(e) = arrowcol_authority::delta_runtime::serve_control(background, &listener).await {
                    eprintln!("router-owned control listener failed: {e}");
                    std::process::exit(1);
                }
            });
            ControlPlane::Local(control)
        }
        "data_only" => {
            validate_data_only(
                &config,
                std::env::var_os("ARROWCOL_DELTA_PRIVATE").is_some(),
                std::env::var_os("ARROWCOL_CONTROL_LISTEN").is_some(),
            )?;
            ControlPlane::Remote(Client::new(config.controller.clone(), config.gateway_token.clone())?)
        }
        _ => anyhow::bail!("ARROWCOL_DELTA_MODE must be controller or data_only"),
    };
    let router = DeltaRouter {
        edge: crate::EdgeVerifier::new(config.edge_secret.as_bytes().to_vec(), 30_000)?,
        config,
        control,
        cache: Mutex::new(BTreeMap::new()),
    };
    let mut server = Server::new_with_opt_and_conf(
        None,
        ServerConf {
            threads: 2,
            max_retries: 1,
            grace_period_seconds: Some(1),
            graceful_shutdown_timeout_seconds: Some(3),
            pid_file: "/runtime/router.pid".into(),
            upgrade_sock: "/runtime/upgrade.sock".into(),
            ..ServerConf::default()
        },
    );
    server.bootstrap();
    let mut proxy = http_proxy_service(&server.configuration, router);
    let mut options = HttpServerOptions::default();
    options.h2c = true;
    proxy.app_logic_mut().context("proxy")?.server_options = Some(options);
    proxy.add_tcp(&std::env::var("ARROWCOL_DELTA_LISTEN").unwrap_or_else(|_| "0.0.0.0:50051".into()));
    server.add_service(proxy);
    server.run_forever();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_only() -> serde_json::Result<Config> {
        serde_json::from_value(serde_json::json!({
            "delta":{"publisher":"http://publisher:50061"},"tenant":"tenant","catalog_id":"catalog","tables":[],
            "storage":".","controller":"http://primary:50060","operator_token":"","gateway_token":"gateway",
            "edge_secret":"edge","internal_secret":"internal","collectors":{},
            "etcd":[],"etcd_user":"","etcd_password":""
        }))
    }

    #[test]
    fn data_only_requires_remote_gateway_access_without_authority_credentials() -> Result<()> {
        let mut config = data_only()?;
        assert!(validate_data_only(&config, false, false).is_ok());
        assert!(validate_data_only(&config, true, false).is_err());
        assert!(validate_data_only(&config, false, true).is_err());
        config.etcd.push("http://etcd:2379".into());
        assert!(validate_data_only(&config, false, false).is_err());
        config = data_only()?;
        config.collectors.insert("member".into(), "secret".into());
        assert!(validate_data_only(&config, false, false).is_err());
        config = data_only()?;
        config.operator_token = "operator".into();
        assert!(validate_data_only(&config, false, false).is_err());
        config = data_only()?;
        config.etcd_password = "authority".into();
        assert!(validate_data_only(&config, false, false).is_err());
        config = data_only()?;
        config.gateway_token.clear();
        assert!(validate_data_only(&config, false, false).is_err());
        config = data_only()?;
        config.controller = "not-an-endpoint".into();
        assert!(validate_data_only(&config, false, false).is_err());
        Ok(())
    }
}
