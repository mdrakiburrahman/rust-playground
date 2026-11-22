use anyhow::{Context, Result, ensure};
use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_deltalake::commit::{BatchedTable, CommitOptions};
use arrow_deltalake::normalize;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrowcol_catalog::{CatalogControlPlane, DdlParser, MemoryCatalogStore};
use arrowcol_flight::{
    AllowAllAuthenticator, ArrowColFlightService, BatchAdmission, PutResultEmitter, RuntimeLimits, RuntimeResult,
    StreamAbortReason, StreamCompletion, StreamContext, StreamLifecycle, StreamSummary,
};
use arrowcol_protocol::{
    AuthContext, AuthMethod, AuthenticatedPrincipal, ImplementationPool, PrincipalId, PrincipalIssuer, UnixMillis,
};
use async_trait::async_trait;
use cpu_time::ProcessTime;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::basic::{Compression, Encoding, PageType};
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
const CANDIDATE: &str = r#"{"columns":{"last_connect_error_number":{"dictionary_enabled":false}}}"#;

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
        None,
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
        RuntimeLimits {
            max_message_bytes: 134_217_728,
            ..RuntimeLimits::default()
        },
    )?
    .with_ddl_handler(Arc::new(ddl));
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service).max_decoding_message_size(134_217_728))
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
    let candidate = match std::env::var("ARROWCOL_PARQUET_CONFIG") {
        Err(std::env::VarError::NotPresent) => false,
        Ok(config) if config == CANDIDATE => true,
        _ => anyhow::bail!("only the predeclared dictionary candidate is supported"),
    };
    println!(
        "HISTORICAL_LAYOUT_STARTUP {}",
        arrow_deltalake::delta::effective_layout_startup_config()?
    );
    println!(
        "HISTORICAL_PARQUET_STARTUP {}",
        serde_json::to_string(arrow_deltalake::writer::process_config()?)?
    );
    let batches = capture().await?;
    let mut hasher = DefaultHasher::new();
    format!("{:?}", batches[0].schema()).hash(&mut hasher);
    let mut logical_bytes = 0_u64;
    for batch in &batches {
        logical_bytes += arrow_deltalake::record_batch_logical_bytes(batch)?;
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
        } else if batches[0].column(index).as_any().is::<Int32Array>() {
            let mut values = HashSet::new();
            for batch in &batches {
                let array = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .context("integer column changed type")?;
                for row in 0..array.len() {
                    if !array.is_null(row) {
                        values.insert(array.value(row));
                    }
                }
            }
            println!(
                "HISTORICAL_CARDINALITY column={} distinct={} rows={}",
                field.name(),
                values.len(),
                ROWS
            );
        }
    }
    println!(
        "HISTORICAL_CORPUS rows={ROWS} batches={} logical_bytes={logical_bytes} hash64={:016x}",
        batches.len(),
        hasher.finish()
    );

    let temp = TempDir::new()?;
    let table = BatchedTable::open(temp.path(), CommitOptions::default()).await?;
    let entries = || {
        batches
            .iter()
            .enumerate()
            .map(|(index, batch)| (batch.clone(), format!("fixture-{index}"), 1, 1_790_000_000_000))
            .collect()
    };
    let warmup = table.stage_coalesced_publications(entries()).await?;
    ensure!(warmup.appends.len() == 1, "warmup did not stage one file");
    drop(warmup);
    let mut warmup_paths = Vec::new();
    find_parquet(temp.path(), &mut warmup_paths)?;
    ensure!(warmup_paths.len() == 1, "expected one warmup file");
    fs::remove_file(&warmup_paths[0])?;
    let started = Instant::now();
    let cpu = ProcessTime::now();
    let group = table.stage_coalesced_publications(entries()).await?;
    let wall_s = started.elapsed().as_secs_f64();
    let cpu_s = cpu.elapsed().as_secs_f64();
    ensure!(group.appends.len() == 1, "expected one staged file");
    let stage = &group.appends[0];
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
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        File::open(&paths[0])?,
        ArrowReaderOptions::new().with_encoding_stats_as_mask(false),
    )?;
    let metadata = builder.metadata();
    let mut codecs = BTreeSet::new();
    let mut selected_seen = false;
    for group in metadata.row_groups() {
        println!("HISTORICAL_LAYOUT sorting_columns={:?}", group.sorting_columns());
        for column in group.columns() {
            ensure!(
                column.compression() == Compression::UNCOMPRESSED,
                "unexpected physical codec"
            );
            codecs.insert(format!("{:?}", column.compression()));
            if column.column_path().string() == "last_connect_error_number" {
                selected_seen = true;
                let expected = if candidate {
                    Encoding::PLAIN
                } else {
                    Encoding::RLE_DICTIONARY
                };
                let stats = column
                    .page_encoding_stats()
                    .context("missing physical page encoding statistics")?;
                let data_pages = stats
                    .iter()
                    .filter(|stat| matches!(stat.page_type, PageType::DATA_PAGE | PageType::DATA_PAGE_V2))
                    .collect::<Vec<_>>();
                ensure!(
                    !data_pages.is_empty()
                        && data_pages
                            .iter()
                            .all(|stat| stat.encoding == expected && stat.count > 0),
                    "selected column did not use {expected:?} for all data pages"
                );
            }
            println!(
                "HISTORICAL_FOOTER column={} codec={:?} encodings={:?} page_encoding_stats={:?}",
                column.column_path().string(),
                column.compression(),
                column.encodings().collect::<Vec<_>>(),
                column.page_encoding_stats()
            );
        }
    }
    let row_groups = metadata.num_row_groups();
    ensure!(
        selected_seen && row_groups == 1,
        "unexpected selected column or row group count"
    );
    let actual = builder.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
    let expected_schema = normalize::schema(batches[0].schema_ref())?;
    ensure!(
        expected_schema.fields().len() == actual[0].schema().fields().len(),
        "field count differs"
    );
    for (expected, actual) in expected_schema.fields().iter().zip(actual[0].schema().fields()) {
        ensure!(
            expected.name() == actual.name()
                && expected.data_type() == actual.data_type()
                && expected.is_nullable() == actual.is_nullable(),
            "physical field differs: expected {expected:?}, found {actual:?}"
        );
    }
    check_rows(&batches, &actual)?;
    println!(
        "HISTORICAL_RESULT rows={ROWS} logical_bytes={logical_bytes} physical_bytes={} row_groups={row_groups} codecs={codecs:?} wall_s={wall_s:.9} cpu_s={cpu_s:.9} preparation_build_s={:.9} preparation_sort_s={:.9} build_s={:.9} sort_s={:.9} encode_s={:.9} decoded_ordered_values=true",
        observation.parquet_bytes,
        group.preparation_build.as_secs_f64(),
        group.preparation_sort.as_secs_f64(),
        timings.build.as_secs_f64(),
        timings.sort.unwrap_or_default().as_secs_f64(),
        timings.encode.as_secs_f64()
    );
    Ok(())
}
