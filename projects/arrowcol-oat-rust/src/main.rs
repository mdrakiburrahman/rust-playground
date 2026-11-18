use anyhow::{Context, Result, ensure};
use arrowcol_oat_rust::config::{OatConfig, OatMode};
use arrowcol_oat_rust::model::{AppState, CancelRunError, RunState, RunStatus, unix_time_ms};
use axum::extract::{FromRequest, Path, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use futures_util::TryStreamExt;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;

#[derive(Debug, Parser)]
#[command(about = "Run ArrowCol OAT benchmark matrices")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8088")]
    listen: SocketAddr,
    /// Expose the unauthenticated control API outside loopback for trusted development networks only.
    #[arg(long)]
    allow_insecure_non_loopback: bool,
    #[arg(long)]
    validate_config: Option<PathBuf>,
    /// Fixture-only zero-row Kubernetes setup. Requires the per-command NDJSON bridge.
    #[arg(long, conflicts_with = "validate_config")]
    fixture_setup: Option<PathBuf>,
    #[arg(long, requires = "fixture_setup")]
    fixture_bridge_stdio: bool,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Serialize)]
struct ApiError {
    error: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if let Some(path) = args.fixture_setup {
        let result = arrowcol_oat_rust::runner::run_fixture_setup(&path, args.fixture_bridge_stdio).await;
        if let Err(error) = &result {
            eprintln!("{error:#}");
        }
        // Tokio's stdin reader may still block after a bridge timeout; this dedicated CLI owns no background work.
        std::process::exit(if result.is_ok() { 0 } else { 1 });
    }
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    if let Some(path) = args.validate_config {
        let source = fs::read_to_string(&path)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        let config: OatConfig = serde_json::from_str(&source).context("parse OAT JSON")?;
        config.validate()?;
        println!("valid OAT config: {}", path.display());
        return Ok(());
    }

    ensure!(
        args.listen.ip().is_loopback() || args.allow_insecure_non_loopback,
        "non-loopback OAT listeners require --allow-insecure-non-loopback; the control API has no authentication"
    );
    if !args.listen.ip().is_loopback() {
        tracing::warn!(address = %args.listen, "explicitly exposing unauthenticated OAT control API outside loopback");
    }

    let state = Arc::new(AppState::default());
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/runs", post(start_run))
        .route("/runs/{id}", get(get_run))
        .route("/runs/{id}/cancel", post(cancel_run))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(address = %listener.local_addr()?, "ArrowCol OAT server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

async fn start_run(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Result<(StatusCode, Json<RunStatus>), Response> {
    let hasher = Arc::new(tokio::sync::Mutex::new(Sha256::new()));
    let body_hasher = Arc::clone(&hasher);
    let request = request.map(|body| {
        axum::body::Body::from_stream(body.into_data_stream().and_then(move |chunk| {
            let body_hasher = Arc::clone(&body_hasher);
            async move {
                body_hasher.lock().await.update(&chunk);
                Ok(chunk)
            }
        }))
    });
    let Json(config) = Json::<OatConfig>::from_request(request, &state)
        .await
        .map_err(IntoResponse::into_response)?;
    let config_sha256 = format!("{:x}", hasher.lock().await.clone().finalize());
    config.validate().map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError {
                error: format!("{error:#}"),
            }),
        )
            .into_response()
    })?;
    let permit = state.try_start_run().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            Json(ApiError {
                error: "an OAT run is already active".to_owned(),
            }),
        )
            .into_response()
    })?;
    let id = uuid::Uuid::new_v4().to_string();
    let status = RunStatus::queued(id.clone(), config_sha256);
    state.runs.lock().await.insert(id.clone(), status.clone());

    let local = config.platform.kind == OatMode::Local;
    let cancellation = state.register_run_cancel(&id, local).await;
    let task_state = Arc::clone(&state);
    tokio::spawn(async move {
        let _permit = permit;
        let result = arrowcol_oat_rust::runner::execute(config, &id, Arc::clone(&task_state), cancellation).await;
        match result {
            Ok(outcome) if outcome.cancelled => {
                task_state
                    .update(&id, |status| {
                        status.state = RunState::Cancelled;
                        status.progress = "local resources cleaned after cancellation".to_owned();
                        status.error = Some("run cancelled by request".to_owned());
                        status.finished_at_unix_ms = Some(unix_time_ms());
                    })
                    .await;
            }
            Ok(outcome) => {
                task_state
                    .update(&id, |status| {
                        status.state = RunState::Succeeded;
                        status.progress = "published Parquet results".to_owned();
                        status.result_count = outcome.result_count;
                        status.finished_at_unix_ms = Some(unix_time_ms());
                    })
                    .await;
            }
            Err(error) => {
                tracing::error!(run_id = %id, error = %error, "OAT run failed");
                task_state.finish_failed_run(&id, format!("{error:#}")).await;
            }
        }
    });
    Ok((StatusCode::ACCEPTED, Json(status)))
}

async fn cancel_run(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ApiError>)> {
    state
        .request_local_cancel(&id)
        .await
        .map(|()| StatusCode::ACCEPTED)
        .map_err(|error| {
            let (status, message) = match error {
                CancelRunError::NotFound => (StatusCode::NOT_FOUND, "unknown OAT run"),
                CancelRunError::NotLocal => (StatusCode::FORBIDDEN, "only local OAT runs can be cancelled"),
                CancelRunError::NotActive => (StatusCode::CONFLICT, "OAT run is not active"),
            };
            (
                status,
                Json(ApiError {
                    error: message.to_owned(),
                }),
            )
        })
}

async fn get_run(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<RunStatus>, (StatusCode, Json<ApiError>)> {
    state.runs.lock().await.get(&id).cloned().map(Json).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ApiError {
                error: format!("unknown run {id}"),
            }),
        )
    })
}

#[cfg(test)]
mod config_hash_tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use sha2::{Digest, Sha256};

    #[tokio::test(flavor = "current_thread")]
    async fn post_and_get_attest_raw_bytes_before_the_runner_is_scheduled() -> Result<()> {
        let raw = include_bytes!("../../../docs/experiments/arrowcol-oat/oat-config.quick.json");
        let expected = format!("{:x}", Sha256::digest(raw));
        let state = Arc::new(AppState::default());
        let request = Request::builder()
            .header("content-type", "application/json")
            .body(Body::from(raw.as_slice()))?;
        // All awaits are immediately ready on this thread; the spawned workload is never polled.
        let (code, Json(status)) = start_run(State(Arc::clone(&state)), request)
            .await
            .map_err(|response| anyhow::anyhow!("run rejected: {}", response.status()))?;
        assert_eq!(code, StatusCode::ACCEPTED);
        assert_eq!(status.config_sha256, expected);
        let Json(observed) = get_run(State(state), Path(status.id.clone()))
            .await
            .map_err(|(code, _)| anyhow::anyhow!("missing accepted run: {code}"))?;
        assert_eq!(observed.id, status.id);
        assert_eq!(observed.config_sha256, expected);
        let parsed: serde_json::Value = serde_json::from_slice(raw)?;
        assert_ne!(expected, format!("{:x}", Sha256::digest(serde_json::to_vec(&parsed)?)));
        Ok(())
    }

    #[tokio::test]
    async fn raw_capture_preserves_json_extractor_rejections() -> Result<()> {
        for (content_type, body) in [
            ("text/plain", "{}".to_owned()),
            ("application/json", "{".to_owned()),
            ("application/json", r#"{"platform":42}"#.to_owned()),
            ("application/json", " ".repeat(2 * 1024 * 1024 + 1)),
        ] {
            let request = || {
                Request::builder()
                    .header("content-type", content_type)
                    .body(Body::from(body.clone()))
            };
            let state = Arc::new(AppState::default());
            let Err(expected) = Json::<OatConfig>::from_request(request()?, &state).await else {
                anyhow::bail!("JSON extractor unexpectedly accepted the request");
            };
            let expected = expected.into_response();
            let Err(actual) = start_run(State(state), request()?).await else {
                anyhow::bail!("run handler unexpectedly accepted the request");
            };
            assert_eq!(actual.status(), expected.status());
            assert_eq!(actual.headers(), expected.headers());
            assert_eq!(
                to_bytes(actual.into_body(), usize::MAX).await?,
                to_bytes(expected.into_body(), usize::MAX).await?
            );
        }
        Ok(())
    }
}
