use anyhow::{Context, Result, bail, ensure};
use arrow_deltalake::delta::DeltaTable;
use arrow_deltalake::layout::LayoutPolicy;
use arrow_deltalake::publisher::PublisherSession;
use arrow_tenant_stream::checkpoint::Checkpoint;
use arrow_tenant_stream::metrics::{StreamMetrics, serve};
use arrow_tenant_stream::reader::TenantQuery;
use arrow_tenant_stream::sink::DeltaSink;
use arrow_tenant_stream::source::DeltaSource;
use arrow_tenant_stream::stream::{Step, TenantStream};
use clap::Parser;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

#[derive(Parser)]
#[command(about = "Checkpointed append-only historical Delta tenant fork")]
struct Options {
    #[arg(long)]
    source: String,
    #[arg(long)]
    target: String,
    #[arg(long)]
    checkpoint: String,
    #[arg(long)]
    customer_id: String,
    #[arg(long, value_delimiter = ',')]
    projection: Vec<String>,
    #[arg(long, default_value_t = 128)]
    max_actions: usize,
    #[arg(long, default_value_t = 1_000)]
    poll_ms: u64,
    /// Drain available microbatches until the first idle poll, then exit.
    #[arg(long)]
    once: bool,
    #[arg(long, default_value = "0.0.0.0:9094")]
    metrics_listen: std::net::SocketAddr,
    #[arg(long, default_value_t = 262_144)]
    file_rows: usize,
    #[arg(long, default_value_t = 67_108_864)]
    file_input_bytes: usize,
    #[arg(long, default_value_t = 16_384)]
    row_group_rows: usize,
    /// Operator assertion, not a timeout/automatic lease takeover.
    #[arg(long, requires = "confirm_previous_writer_stopped")]
    recover_stale_checkpoint_lock: bool,
    #[arg(long, requires = "recover_stale_checkpoint_lock")]
    confirm_previous_writer_stopped: bool,
}

fn directory_url(value: &str, create: bool) -> Result<Url> {
    let mut url = match Url::parse(value) {
        Ok(url) => url,
        Err(_) => {
            let path = std::env::current_dir()?.join(value);
            Url::from_directory_path(path).map_err(|()| anyhow::anyhow!("invalid directory path"))?
        }
    };
    ensure!(
        url.query().is_none() && url.fragment().is_none(),
        "storage URLs cannot have query/fragment"
    );
    if url.scheme() == "file" {
        let path = url
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("invalid local directory URL"))?;
        if create {
            std::fs::create_dir_all(&path)?;
        }
        url = Url::from_directory_path(std::fs::canonicalize(path)?)
            .map_err(|()| anyhow::anyhow!("invalid canonical directory URL"))?;
    }
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    let options = Options::parse();
    ensure!(
        options.poll_ms > 0 && options.max_actions > 0,
        "poll-ms/max-actions must be positive"
    );
    let source_url = directory_url(&options.source, false)?;
    let target_url = directory_url(&options.target, true)?;
    let checkpoint_url = directory_url(&options.checkpoint, true)?;
    ensure!(
        !target_url.as_str().starts_with(source_url.as_str()) && !source_url.as_str().starts_with(target_url.as_str()),
        "source and target must be separate, non-nested Delta roots"
    );
    ensure!(
        !checkpoint_url.as_str().starts_with(source_url.as_str()),
        "checkpoint must not be stored inside the vacuumable source root"
    );
    ensure!(
        !checkpoint_url.as_str().starts_with(target_url.as_str()),
        "checkpoint must not be stored inside the vacuumable target root"
    );
    let session = PublisherSession::shared_for_url_async(&target_url).await?;
    let run_result = run(&options, source_url, target_url, checkpoint_url, Arc::clone(&session)).await;
    let close_result = session.close_after_drain_async().await;
    match (run_result, close_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(run), Err(close)) => bail!("tenant stream failed: {run:#}; publisher registration retained: {close:#}"),
    }
}

async fn run(
    options: &Options,
    source_url: Url,
    target_url: Url,
    checkpoint_url: Url,
    session: Arc<PublisherSession>,
) -> Result<()> {
    let source = DeltaSource::open(source_url)?;
    let checkpoint = if checkpoint_url.scheme() == "file" {
        ensure!(
            !options.recover_stale_checkpoint_lock,
            "local locks release automatically; never remove a live flock"
        );
        Checkpoint::open_local(
            checkpoint_url
                .to_file_path()
                .map_err(|()| anyhow::anyhow!("invalid checkpoint file URL"))?,
        )
        .await?
    } else {
        let (_, prefix) = object_store::ObjectStoreScheme::parse(&checkpoint_url)?;
        let store = session.store_for_url(&checkpoint_url)?;
        let authority: Arc<dyn arrow_tenant_stream::checkpoint::StoreAuthority> =
            Arc::<PublisherSession>::clone(&session);
        if options.recover_stale_checkpoint_lock {
            Checkpoint::clear_store_lock_with_authority(Arc::clone(&store), prefix.clone(), Arc::clone(&authority))
                .await?;
        }
        Checkpoint::open_store_with_authority(store, prefix, authority).await?
    };
    let metrics = Arc::new(StreamMetrics::default());
    let query = TenantQuery {
        customer_id: options.customer_id.clone(),
        projection: options.projection.clone(),
    };
    let mut stream = TenantStream::open(
        source,
        checkpoint,
        target_url.to_string(),
        query.clone(),
        options.max_actions,
        Arc::clone(&metrics),
    )
    .await?;
    let run_result = drive(&mut stream, options, target_url, &query, metrics, session).await;
    let close_result = stream.close().await;
    match (run_result, close_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(run), Err(close)) => bail!("tenant stream failed: {run:#}; checkpoint release also failed: {close:#}"),
    }
}

async fn drive(
    stream: &mut TenantStream,
    options: &Options,
    target_url: Url,
    query: &TenantQuery,
    metrics: Arc<StreamMetrics>,
    session: Arc<PublisherSession>,
) -> Result<()> {
    let source_schema = stream.manifest().schema()?;
    let columns = query.columns(stream.manifest())?;
    let schema = Arc::new(
        source_schema.project(
            &columns
                .iter()
                .map(|name| source_schema.index_of(name))
                .collect::<arrow::error::Result<Vec<_>>>()?,
        )?,
    );
    let partition_columns = stream.manifest().partition_columns()?;
    let mut policy = LayoutPolicy {
        max_rows: options.file_rows,
        max_input_bytes: options.file_input_bytes,
        row_group_rows: options.row_group_rows,
        ..LayoutPolicy::default()
    };
    if schema.field_with_name("sample_time_utc").is_ok() {
        policy.sort_columns.push("sample_time_utc".into());
    }
    let table = DeltaTable::open_url_with_session(target_url.clone(), Arc::clone(&schema), partition_columns, session)?;
    ensure!(
        DeltaSource::open(target_url)?.capture()?.table_id != stream.manifest().table_id,
        "source and target resolve to the same Delta table identity"
    );
    let mut sink = DeltaSink::new(table, schema, policy)?;
    let listener = tokio::net::TcpListener::bind(options.metrics_listen)
        .await
        .context("bind tenant stream metrics")?;
    eprintln!("tenant stream metrics listening on {}", listener.local_addr()?);
    let mut metrics_task = tokio::spawn(serve(listener, metrics));
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let run_result: Result<()> = async {
        loop {
            let (outcome, stopping) = {
                let step = drain_step_on_shutdown(stream.step(&mut sink), shutdown.as_mut());
                tokio::pin!(step);
                tokio::select! {
                    result = &mut metrics_task => {
                        step.await.context("drain microbatch after metrics failure")?;
                        result.context("tenant metrics task panicked")??;
                        bail!("tenant metrics server exited unexpectedly");
                    }
                    result = &mut step => result?,
                }
            };
            match outcome {
                Step::Idle if stopping => break,
                Step::Idle if options.once => break,
                Step::Idle => {
                    tokio::select! {
                        signal = &mut shutdown => { signal?; break; }
                        result = &mut metrics_task => {
                            result.context("tenant metrics task panicked")??;
                            bail!("tenant metrics server exited unexpectedly");
                        }
                        () = tokio::time::sleep(Duration::from_millis(options.poll_ms)) => {}
                    }
                }
                Step::Committed { batch, files, replay } => {
                    println!("batch={batch} source_files={files} sink_replay={replay}");
                    if stopping {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
    .await;
    metrics_task.abort();
    run_result
}

async fn drain_step_on_shutdown(
    step: impl std::future::Future<Output = Result<Step>>,
    shutdown: impl std::future::Future<Output = Result<()>>,
) -> Result<(Step, bool)> {
    tokio::pin!(step);
    tokio::select! {
        result = &mut step => Ok((result?, false)),
        signal = shutdown => {
            let result = step.await?;
            signal?;
            Ok((result, true))
        }
    }
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            signal = tokio::signal::ctrl_c() => signal?,
            signal = terminate.recv() => ensure!(signal.is_some(), "SIGTERM listener closed unexpectedly"),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_drains_inflight_microbatch_and_preserves_its_failure() -> Result<()> {
        for fail in [false, true] {
            let (started, started_rx) = tokio::sync::oneshot::channel();
            let (finish, finish_rx) = tokio::sync::oneshot::channel();
            let (stop, stop_rx) = tokio::sync::oneshot::channel();
            let (observed, observed_rx) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(drain_step_on_shutdown(
                async move {
                    started
                        .send(())
                        .map_err(|()| anyhow::anyhow!("start receiver closed"))?;
                    finish_rx.await.context("finish signal")?;
                    ensure!(!fail, "injected ambiguous sink failure");
                    Ok(Step::Idle)
                },
                async move {
                    stop_rx.await.context("stop signal")?;
                    observed
                        .send(())
                        .map_err(|()| anyhow::anyhow!("shutdown observer closed"))?;
                    Ok(())
                },
            ));
            started_rx.await?;
            stop.send(())
                .map_err(|()| anyhow::anyhow!("shutdown receiver closed"))?;
            observed_rx.await?;
            tokio::task::yield_now().await;
            assert!(!task.is_finished(), "shutdown discarded an outstanding mutation");
            finish
                .send(())
                .map_err(|()| anyhow::anyhow!("microbatch was cancelled"))?;
            let result = task.await?;
            if fail {
                let error = result.err().context("failure must not become a clean drain")?;
                assert!(format!("{error:#}").contains("injected ambiguous sink failure"));
            } else {
                assert_eq!(result?, (Step::Idle, true));
            }
        }
        Ok(())
    }
}
