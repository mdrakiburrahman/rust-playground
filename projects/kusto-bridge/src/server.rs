use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{StatusCode, header},
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::status::Status;

/// Handle used by `/refresh` to request an immediate reconcile pass.
#[derive(Clone)]
pub struct AppState {
    pub status: Status,
    pub trigger: mpsc::Sender<()>,
}

#[derive(Deserialize)]
struct RefreshQuery {
    /// Seconds to wait for the triggered pass to finish (default 60, max 600).
    wait: Option<u64>,
}

/// Build the HTTP router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .route("/refresh", post(refresh))
        .with_state(state)
}

/// Serve until the shutdown future resolves.
///
/// # Errors
/// Fails when the listener cannot bind or the server stops unexpectedly.
pub async fn serve(
    listen: SocketAddr,
    state: AppState,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding {listen}"))?;
    tracing::info!(%listen, "kusto-bridge listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await
        .context("serving HTTP")
}

async fn healthz() -> &'static str {
    "ok"
}

async fn status(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.status.snapshot())
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.status.prometheus(),
    )
}

async fn refresh(State(state): State<AppState>, Query(query): Query<RefreshQuery>) -> impl IntoResponse {
    let wait = Duration::from_secs(query.wait.unwrap_or(60).min(600));
    let before = state.status.pass();
    // A full channel already holds a pending trigger, which covers this request too.
    let _ = state.trigger.try_send(());
    let finished = tokio::time::timeout(wait, async {
        // The pass running when the request arrived may predate the trigger, so wait for two.
        while state.status.pass() < before + 2 {
            state.status.changed().await;
        }
    })
    .await
    .is_ok();
    let code = if finished { StatusCode::OK } else { StatusCode::ACCEPTED };
    (code, Json(state.status.snapshot()))
}

/// Probe `/healthz` for the container healthcheck; the runtime image has no curl.
///
/// # Errors
/// Fails on connection errors, timeouts, or non-success responses.
pub async fn healthcheck(url: &str) -> Result<()> {
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?
        .get(url)
        .send()
        .await
        .with_context(|| format!("requesting {url}"))?;
    anyhow::ensure!(response.status().is_success(), "{url} returned {}", response.status());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn endpoints_and_healthcheck_work() -> Result<()> {
        let status = Status::default();
        let (trigger, mut triggered) = mpsc::channel(1);
        let state = AppState {
            status: status.clone(),
            trigger,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move { axum::serve(listener, router(state)).await });
        let worker = status.clone();
        tokio::spawn(async move {
            while triggered.recv().await.is_some() {
                worker.finish_pass();
                worker.finish_pass();
            }
        });

        healthcheck(&format!("http://{addr}/healthz")).await?;
        let client = reqwest::Client::new();
        let refreshed = client.post(format!("http://{addr}/refresh?wait=10")).send().await?;
        assert_eq!(refreshed.status(), StatusCode::OK);
        let body: serde_json::Value = refreshed.json().await?;
        assert_eq!(body["ready"], true);
        let metrics = client
            .get(format!("http://{addr}/metrics"))
            .send()
            .await?
            .text()
            .await?;
        assert!(metrics.contains("kusto_bridge_up{pool=\"all\"} 1"));
        assert!(healthcheck(&format!("http://{addr}/missing")).await.is_err());
        Ok(())
    }
}
