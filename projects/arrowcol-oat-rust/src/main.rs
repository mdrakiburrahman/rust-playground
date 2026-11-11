use anyhow::{Context, Result, ensure};
use arrowcol_oat_rust::config::{OatConfig, OatMode};
use arrowcol_oat_rust::model::{AppState, CancelRunError, RunState, RunStatus, unix_time_ms};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use serde::Serialize;
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
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
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
    Json(config): Json<OatConfig>,
) -> Result<(StatusCode, Json<RunStatus>), (StatusCode, Json<ApiError>)> {
    config.validate().map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError {
                error: format!("{error:#}"),
            }),
        )
    })?;
    let permit = state.try_start_run().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            Json(ApiError {
                error: "an OAT run is already active".to_owned(),
            }),
        )
    })?;
    let id = uuid::Uuid::new_v4().to_string();
    let status = RunStatus {
        id: id.clone(),
        state: RunState::Queued,
        progress: "queued".to_owned(),
        result_count: 0,
        error: None,
        started_at_unix_ms: unix_time_ms(),
        finished_at_unix_ms: None,
    };
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
                task_state
                    .update(&id, |status| {
                        status.state = RunState::Failed;
                        status.progress = "failed".to_owned();
                        status.error = Some(format!("{error:#}"));
                        status.finished_at_unix_ms = Some(unix_time_ms());
                    })
                    .await;
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
