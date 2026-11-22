use anyhow::{Context, Result, ensure};
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_deltalake::delta::DeltaTable;
use arrow_deltalake::normalize;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrowcol_catalog::{CatalogControlPlane, DdlParser, MemoryCatalogStore};
use arrowcol_flight::{
    AllowAllAuthenticator, ArrowColFlightService, BatchAdmission, PutResultEmitter, RuntimeLimits, RuntimeResult,
    StreamAbortReason, StreamCompletion, StreamContext, StreamLifecycle, StreamSummary,
    canonical_record_batch_logical_bytes,
};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, ErrorCode, ImplementationPool, PrincipalId, PrincipalIssuer,
    UnixMillis,
};
use async_trait::async_trait;
use cpu_time::ProcessTime;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeSet, HashSet};
use std::fs::{self, File};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const TABLE: &str = "sqlserver_availability_replica_states";
const CLIENT: &str = "direct-e64-confirmwide";
const ROWS: usize = 65_536;

struct Capture(Arc<Mutex<Vec<RecordBatch>>>);

#[async_trait]
impl BatchAdmission for Capture {
    async fn begin(
        &self,
        _context: StreamContext,
        _emitter: Arc<dyn PutResultEmitter>,
    ) -> RuntimeResult<Box<dyn StreamLifecycle>> {
        Ok(Box::new(Capture(Arc::clone(&self.0))))
    }
}

#[async_trait]
impl StreamLifecycle for Capture {
    async fn push_batch(&mut self, batch: RecordBatch) -> RuntimeResult<()> {
        self.0.lock().await.push(batch);
        Ok(())
    }

    async fn complete(&mut self, _summary: StreamSummary) -> RuntimeResult<StreamCompletion> {
        Ok(StreamCompletion::best_effort())
    }

    async fn abort(self: Box<Self>, _reason: StreamAbortReason) {}
}

fn auth() -> Result<AuthContext> {
    let principal = AuthenticatedPrincipal::new(
        PrincipalId::new("local-fixture")?,
        PrincipalIssuer::new("local-test")?,
        BTreeSet::new(),
    )?;
    Ok(AuthContext::new(
        principal,
        AuthMethod::WorkloadIdentity,
        UnixMillis::new(1)?,
    )?)
}

async fn capture() -> Result<Vec<RecordBatch>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let batches = Arc::new(Mutex::new(Vec::new()));
    let catalog: Arc<dyn arrowcol_catalog::CatalogStore> = Arc::new(MemoryCatalogStore::new());
    let (ddl, reconciliation) = CatalogControlPlane::start(DdlParser::default(), Arc::clone(&catalog)).await?;
    ensure!(
        reconciliation.failures.is_empty(),
        "local catalog reconciliation failed"
    );
    let service = ArrowColFlightService::new(
        ImplementationPool::Dataflow,
        catalog,
        Arc::new(AllowAllAuthenticator::new(auth()?)),
        Arc::new(Capture(Arc::clone(&batches))),
        RuntimeLimits::default(),
    )?
    .with_ddl_handler(Arc::new(ddl));
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let binary = std::env::var_os("DARWIN_HISTORICAL_CLIENT").context("set DARWIN_HISTORICAL_CLIENT")?;
    let result = tokio::task::spawn_blocking(move || {
        Command::new(binary)
            .env_clear()
            .args([
                "--endpoint",
                &endpoint,
                "--client-id",
                CLIENT,
                "--table",
                TABLE,
                "--rows",
                "65536",
                "--concurrency",
                "1",
                "--workers",
                "1",
                "--runtime-threads",
                "2",
                "--streams-per-worker",
                "1",
                "--batch-rows",
                "65536",
                "--corpus-rows",
                "65536",
                "--data-profile",
                "diagnostic",
                "--string-width",
                "0",
                "--seed",
                "20260923",
                "--max-flight-data-bytes",
                "67108864",
                "--max-message-bytes",
                "134217728",
                "--max-memory-bytes",
                "1073741824",
                "--stream-retry-max-attempts",
                "1",
            ])
            .output()
    })
    .await??;
    let _ = shutdown_tx.send(());
    server.await??;
    ensure!(
        result.status.success(),
        "historical client failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    for line in String::from_utf8_lossy(&result.stdout).lines() {
        if line.starts_with("OAT_CLIENT_EFFECTIVE_CONFIG ") {
            println!("{line}");
        }
    }
    let captured = std::mem::take(&mut *batches.lock().await);
    ensure!(
        captured.iter().map(RecordBatch::num_rows).sum::<usize>() == ROWS,
        "unexpected captured row count"
    );
    Ok(captured)
}

fn find_parquet(root: &Path, result: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            find_parquet(&path, result)?;
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            result.push(path);
        }
    }
    Ok(())
}

fn check_rows(expected: &[RecordBatch], actual: &[RecordBatch]) -> Result<()> {
    let (mut ei, mut ai, mut eo, mut ao) = (0, 0, 0, 0);
    while ei < expected.len() && ai < actual.len() {
        let count = (expected[ei].num_rows() - eo).min(actual[ai].num_rows() - ao);
        ensure!(count > 0, "empty batch in decoded output");
        let left = normalize::align(&normalize::batch(&expected[ei])?, actual[ai].schema())?.slice(eo, count);
        let right = actual[ai].slice(ao, count);
        ensure!(
            left == right,
            "decoded schema or ordered values differ at input batch {ei}, offset {eo}"
        );
        eo += count;
        ao += count;
        if eo == expected[ei].num_rows() {
            ei += 1;
            eo = 0;
        }
        if ao == actual[ai].num_rows() {
            ai += 1;
            ao = 0;
        }
    }
    ensure!(ei == expected.len() && ai == actual.len(), "decoded row count differs");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn historical_delta_writer() -> Result<()> {
    let batches = capture().await?;
    let mut hasher = DefaultHasher::new();
    format!("{:?}", batches[0].schema()).hash(&mut hasher);
    let mut logical_bytes = 0_u64;
    for batch in &batches {
        logical_bytes += canonical_record_batch_logical_bytes(batch)?;
        for column in batch.columns() {
            let data = column.to_data();
            ensure!(
                data.child_data().is_empty(),
                "nested data requires explicit digest support"
            );
            data.len().hash(&mut hasher);
            data.offset().hash(&mut hasher);
            for buffer in data.buffers() {
                buffer.as_slice().hash(&mut hasher);
            }
            if let Some(nulls) = data.nulls() {
                nulls.buffer().as_slice().hash(&mut hasher);
            }
        }
    }
    for (index, field) in batches[0].schema().fields().iter().enumerate() {
        if let Some(first) = batches[0].column(index).as_any().downcast_ref::<StringArray>() {
            let mut values = HashSet::new();
            for batch in &batches {
                let array = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("string column changed type")?;
                for row in 0..array.len() {
                    if !array.is_null(row) {
                        values.insert(array.value(row).to_owned());
                    }
                }
            }
            println!(
                "HISTORICAL_CARDINALITY column={} distinct={} rows={} nulls_first_batch={}",
                field.name(),
                values.len(),
                ROWS,
                first.null_count()
            );
        }
    }
    println!(
        "HISTORICAL_CORPUS rows={ROWS} batches={} logical_bytes={logical_bytes} hash64={:016x}",
        batches.len(),
        hasher.finish()
    );

    let temp = TempDir::new()?;
    let table = DeltaTable::open(temp.path())?;
    let mut writer = table.new_file_with_publication(batches[0].schema(), "fixture".into(), 1, 1_790_000_000_000)?;
    let started = Instant::now();
    let cpu = ProcessTime::now();
    for batch in &batches {
        writer.write(batch).await?;
    }
    let stage = writer.stage().await?;
    let wall_s = started.elapsed().as_secs_f64();
    let cpu_s = cpu.elapsed().as_secs_f64();
    let timings = stage.stage_timings();
    let observation = stage.observation();
    ensure!(observation.rows == ROWS, "staged row count differs");
    let mut paths = Vec::new();
    find_parquet(temp.path(), &mut paths)?;
    ensure!(paths.len() == 1, "expected exactly one staged Parquet file");
    ensure!(
        fs::metadata(&paths[0])?.len() == observation.parquet_bytes,
        "physical bytes differ"
    );
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&paths[0])?)?;
    let metadata = builder.metadata();
    let mut codecs = BTreeSet::new();
    for group in metadata.row_groups() {
        for column in group.columns() {
            codecs.insert(format!("{:?}", column.compression()));
            println!(
                "HISTORICAL_FOOTER column={} encodings={:?}",
                column.column_path().string(),
                column.encodings()
            );
        }
    }
    let row_groups = metadata.num_row_groups();
    let actual = builder.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
    check_rows(&batches, &actual)?;
    println!(
        "HISTORICAL_RESULT rows={ROWS} logical_bytes={logical_bytes} physical_bytes={} row_groups={row_groups} codecs={codecs:?} wall_s={wall_s:.9} cpu_s={cpu_s:.9} build_s={:.9} sort_s={:.9} encode_s={:.9} decoded_ordered_values=true",
        observation.parquet_bytes,
        timings.build.as_secs_f64(),
        timings.sort.unwrap_or_default().as_secs_f64(),
        timings.encode.as_secs_f64()
    );
    Ok(())
}
