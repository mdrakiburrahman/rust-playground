//! Bounded, startup-prepared DMV workload and persistent Flight senders.

use super::*;
use arrow_flight::encode::DictionaryHandling;
use arrow_ipc::writer::IpcWriteOptions;
use clap::{Args, ValueEnum};
use serde::Serialize;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;
use tokio::task::JoinSet;

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum DataMode {
    #[default]
    Shared,
    Independent,
}

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum IpcCompression {
    #[default]
    None,
    #[value(name = "lz4_frame")]
    Lz4Frame,
    Zstd,
}

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Dictionaries {
    #[default]
    Hydrate,
    Resend,
}

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum GrpcCompression {
    #[default]
    None,
    Gzip,
    Zstd,
}

/// All settings are resolved and validated before allocation, registration, or connection.
#[derive(Args, Clone, Debug, Serialize)]
pub struct Options {
    #[arg(long, env = "ARROWCOL_CLIENT_CORPUS_ROWS", default_value_t = 0)]
    pub corpus_rows: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_CORPUS_BYTES", default_value_t = 0)]
    pub corpus_bytes: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_BATCH_ROWS", default_value_t = 0)]
    pub batch_rows: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_WORKERS", default_value_t = 0)]
    pub workers: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_RUNTIME_THREADS", default_value_t = 0)]
    pub runtime_threads: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_DATA_MODE", value_enum, default_value_t = DataMode::Shared)]
    pub data_mode: DataMode,
    #[arg(long, env = "ARROWCOL_CLIENT_STRING_WIDTH", default_value_t = 0)]
    pub string_width: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_CARDINALITY", default_value_t = 1024)]
    pub cardinality: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_NULL_FRACTION", default_value_t = 0.0)]
    pub null_fraction: f64,
    #[arg(long, env = "ARROWCOL_CLIENT_SKEW", default_value_t = 0.0)]
    pub skew: f64,
    #[arg(long, env = "ARROWCOL_CLIENT_ENTROPY", default_value_t = 1.0)]
    pub entropy: f64,
    #[arg(long, env = "ARROWCOL_CLIENT_SEED", default_value_t = 20260923)]
    pub seed: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_MAX_MEMORY_BYTES", default_value_t = 536_870_912)]
    pub max_memory_bytes: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_INTERVAL_US", default_value_t = 0)]
    pub interval_us: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_CONNECTIONS_PER_WORKER", default_value_t = 1)]
    pub connections_per_worker: usize,
    /// Zero inherits the legacy --concurrency setting.
    #[arg(long, env = "ARROWCOL_CLIENT_STREAMS_PER_WORKER", default_value_t = 0)]
    pub streams_per_worker: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_MAX_ROWS", default_value_t = 0)]
    pub max_rows: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_IPC_COMPRESSION", value_enum, default_value_t = IpcCompression::None)]
    pub ipc_compression: IpcCompression,
    #[arg(long, env = "ARROWCOL_CLIENT_DICTIONARY_HANDLING", value_enum, default_value_t = Dictionaries::Hydrate)]
    pub dictionary_handling: Dictionaries,
    #[arg(long, env = "ARROWCOL_CLIENT_GRPC_SEND_COMPRESSION", value_enum, default_value_t = GrpcCompression::None)]
    pub grpc_send_compression: GrpcCompression,
    #[arg(long, env = "ARROWCOL_CLIENT_GRPC_ACCEPT_COMPRESSION", value_enum, default_value_t = GrpcCompression::None)]
    pub grpc_accept_compression: GrpcCompression,
    #[arg(long, env = "ARROWCOL_CLIENT_MAX_FLIGHT_DATA_BYTES", default_value_t = 2_097_152)]
    pub max_flight_data_bytes: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_MAX_MESSAGE_BYTES", default_value_t = 16_777_216)]
    pub max_message_bytes: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_STREAM_WINDOW_BYTES", default_value_t = 1_048_576)]
    pub stream_window_bytes: u32,
    #[arg(long, env = "ARROWCOL_CLIENT_CONNECTION_WINDOW_BYTES", default_value_t = 16_777_216)]
    pub connection_window_bytes: u32,
    #[arg(long, env = "ARROWCOL_CLIENT_MAX_FRAME_BYTES", default_value_t = 16384)]
    pub max_frame_bytes: u32,
    #[arg(long, env = "ARROWCOL_CLIENT_HEADER_TABLE_BYTES", default_value_t = 4096)]
    pub header_table_bytes: u32,
    #[arg(long, env = "ARROWCOL_CLIENT_MAX_HEADER_LIST_BYTES", default_value_t = 16384)]
    pub max_header_list_bytes: u32,
    #[arg(long, env = "ARROWCOL_CLIENT_ADAPTIVE_WINDOW", default_value_t = false, action = clap::ArgAction::Set)]
    pub adaptive_window: bool,
    #[arg(long, env = "ARROWCOL_CLIENT_KEEPALIVE_INTERVAL_MS", default_value_t = 10_000)]
    pub keepalive_interval_ms: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_KEEPALIVE_TIMEOUT_MS", default_value_t = 20_000)]
    pub keepalive_timeout_ms: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_KEEPALIVE_WHILE_IDLE", default_value_t = true, action = clap::ArgAction::Set)]
    pub keepalive_while_idle: bool,
    #[arg(long, env = "ARROWCOL_CLIENT_CONNECT_TIMEOUT_MS", default_value_t = 30_000)]
    pub connect_timeout_ms: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_REQUEST_TIMEOUT_MS", default_value_t = 30_000)]
    pub request_timeout_ms: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_TCP_NODELAY", default_value_t = true, action = clap::ArgAction::Set)]
    pub tcp_nodelay: bool,
    #[arg(long, env = "ARROWCOL_CLIENT_TCP_KEEPALIVE_MS", default_value_t = 60_000)]
    pub tcp_keepalive_ms: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_TCP_KEEPALIVE_INTERVAL_MS", default_value_t = 0)]
    pub tcp_keepalive_interval_ms: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_TCP_KEEPALIVE_RETRIES", default_value_t = 0)]
    pub tcp_keepalive_retries: u32,
    #[arg(long, env = "ARROWCOL_CLIENT_ENDPOINT_CONCURRENCY_LIMIT", default_value_t = 256)]
    pub endpoint_concurrency_limit: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_ENDPOINT_BUFFER_SIZE", default_value_t = 1024)]
    pub endpoint_buffer_size: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_CONNECT_RETRIES", default_value_t = 0)]
    pub connect_retries: u32,
    #[arg(long, env = "ARROWCOL_CLIENT_RETRY_BACKOFF_MS", default_value_t = 100)]
    pub retry_backoff_ms: u64,
    /// Zero disables the per-connection token-bucket request limit.
    #[arg(long, env = "ARROWCOL_CLIENT_RATE_LIMIT_RPS", default_value_t = 0)]
    pub rate_limit_rps: u64,
}

impl Options {
    /// Resolve legacy aliases and reject invalid or unbounded resource requests.
    ///
    /// # Errors
    ///
    /// Rejects invalid bounds, floating-point values, combinations, and overflow.
    pub fn resolve(mut self, rows: usize, concurrency: usize) -> Result<Self> {
        anyhow::ensure!(rows > 0 && concurrency > 0, "rows and concurrency must be positive");
        anyhow::ensure!(
            self.corpus_rows == 0 || self.corpus_bytes == 0,
            "use corpus-rows or corpus-bytes, not both"
        );
        if self.batch_rows == 0 {
            self.batch_rows = rows;
        }
        if self.corpus_rows == 0 && self.corpus_bytes == 0 {
            self.corpus_rows = rows;
        }
        if self.workers == 0 {
            self.workers = std::thread::available_parallelism()?.get();
        }
        if self.runtime_threads == 0 {
            self.runtime_threads = std::thread::available_parallelism()?.get();
        }
        if self.streams_per_worker == 0 {
            self.streams_per_worker = concurrency;
        }
        anyhow::ensure!((1..=1024).contains(&self.workers), "workers must resolve to 1..=1024");
        anyhow::ensure!(
            (1..=1024).contains(&self.runtime_threads),
            "runtime-threads must resolve to 1..=1024"
        );
        anyhow::ensure!(
            (1..=256).contains(&self.connections_per_worker)
                && (1..=1024).contains(&self.streams_per_worker)
                && self.workers.saturating_mul(self.connections_per_worker) <= 4096
                && self.workers.saturating_mul(self.streams_per_worker) <= 65536,
            "connections-per-worker must be 1..=256, streams-per-worker 1..=1024; totals <=4096 connections and <=65536 streams"
        );
        anyhow::ensure!(self.string_width <= 65536, "string-width must be 0..=65536");
        anyhow::ensure!(self.cardinality > 0, "cardinality must be positive");
        for (name, value) in [
            ("null-fraction", self.null_fraction),
            ("skew", self.skew),
            ("entropy", self.entropy),
        ] {
            anyhow::ensure!(
                value.is_finite() && (0.0..=1.0).contains(&value),
                "{name} must be finite and in 0..=1"
            );
        }
        anyhow::ensure!(
            self.max_memory_bytes > 0 && self.max_memory_bytes <= isize::MAX as u64,
            "max-memory-bytes must be positive and fit the address space"
        );
        anyhow::ensure!(
            self.batch_rows <= i32::MAX as usize,
            "batch-rows exceeds Arrow's supported variable-width range"
        );
        anyhow::ensure!(
            self.max_flight_data_bytes >= 1024
                && self.max_flight_data_bytes <= self.max_message_bytes
                && self.max_message_bytes <= i32::MAX as usize,
            "require 1024 <= max-flight-data-bytes <= max-message-bytes <= 2147483647"
        );
        anyhow::ensure!(
            (1..=i32::MAX as u32).contains(&self.stream_window_bytes)
                && (1..=i32::MAX as u32).contains(&self.connection_window_bytes),
            "HTTP/2 windows must be 1..=2147483647 bytes"
        );
        anyhow::ensure!(
            (16384..=16777215).contains(&self.max_frame_bytes) && self.max_header_list_bytes > 0,
            "max-frame-bytes must be 16384..=16777215; max-header-list-bytes must be positive"
        );
        anyhow::ensure!(
            self.connect_timeout_ms > 0
                && self.request_timeout_ms > 0
                && self.keepalive_timeout_ms > 0
                && self.endpoint_concurrency_limit > 0
                && self.endpoint_buffer_size > 0
                && self.connect_retries <= 100,
            "timeouts and endpoint capacities must be positive; connect-retries <=100"
        );
        for value in [
            self.interval_us / 1000,
            self.connect_timeout_ms,
            self.request_timeout_ms,
            self.keepalive_interval_ms,
            self.keepalive_timeout_ms,
            self.tcp_keepalive_ms,
            self.tcp_keepalive_interval_ms,
            self.retry_backoff_ms,
        ] {
            anyhow::ensure!(value <= 86_400_000, "transport durations must not exceed one day");
        }
        Ok(self)
    }

    fn endpoint(&self, address: &str) -> Result<Endpoint> {
        let mut endpoint = Endpoint::from_shared(address.to_owned())?
            .connect_timeout(Duration::from_millis(self.connect_timeout_ms))
            .timeout(Duration::from_millis(self.request_timeout_ms))
            .initial_stream_window_size(self.stream_window_bytes)
            .initial_connection_window_size(self.connection_window_bytes)
            .max_frame_size(self.max_frame_bytes)
            .http2_header_table_size(self.header_table_bytes)
            .http2_max_header_list_size(self.max_header_list_bytes)
            .http2_adaptive_window(self.adaptive_window)
            .keep_alive_timeout(Duration::from_millis(self.keepalive_timeout_ms))
            .keep_alive_while_idle(self.keepalive_while_idle)
            .tcp_nodelay(self.tcp_nodelay)
            .tcp_keepalive((self.tcp_keepalive_ms > 0).then(|| Duration::from_millis(self.tcp_keepalive_ms)))
            .tcp_keepalive_interval(
                (self.tcp_keepalive_interval_ms > 0).then(|| Duration::from_millis(self.tcp_keepalive_interval_ms)),
            )
            .tcp_keepalive_retries((self.tcp_keepalive_retries > 0).then_some(self.tcp_keepalive_retries))
            .concurrency_limit(self.endpoint_concurrency_limit)
            .buffer_size(self.endpoint_buffer_size);
        if self.keepalive_interval_ms > 0 {
            endpoint = endpoint.http2_keep_alive_interval(Duration::from_millis(self.keepalive_interval_ms));
        }
        if self.rate_limit_rps > 0 {
            endpoint = endpoint.rate_limit(self.rate_limit_rps, Duration::from_secs(1));
        }
        Ok(endpoint)
    }

    async fn connect(&self, address: &str) -> Result<Channel> {
        let endpoint = self.endpoint(address)?;
        for attempt in 0..=self.connect_retries {
            match endpoint.connect().await {
                Ok(channel) => return Ok(channel),
                Err(error) if attempt == self.connect_retries => return Err(error).context("connect Flight endpoint"),
                Err(error) => {
                    eprintln!("Flight connection attempt {} failed: {error}", attempt + 1);
                    tokio::time::sleep(Duration::from_millis(self.retry_backoff_ms)).await;
                }
            }
        }
        unreachable!("bounded connection loop always returns")
    }

    fn client(&self, channel: Channel) -> FlightServiceClient<Channel> {
        let mut client = FlightServiceClient::new(channel)
            .max_encoding_message_size(self.max_message_bytes)
            .max_decoding_message_size(self.max_message_bytes);
        for (send, compression) in [
            (true, self.grpc_send_compression),
            (false, self.grpc_accept_compression),
        ] {
            let encoding = match compression {
                GrpcCompression::None => continue,
                GrpcCompression::Gzip => tonic::codec::CompressionEncoding::Gzip,
                GrpcCompression::Zstd => tonic::codec::CompressionEncoding::Zstd,
            };
            client = if send {
                client.send_compressed(encoding)
            } else {
                client.accept_compressed(encoding)
            };
        }
        client
    }
}

struct EncodedBatch {
    table: Arc<DmvTable>,
    data: Arc<Vec<FlightData>>,
    rows: u64,
    encoded_bytes: u64,
}

struct Counters {
    attempted_rows: AtomicU64,
    attempted_bytes: AtomicU64,
    completed_rows: AtomicU64,
    completed_bytes: AtomicU64,
    error_rows: AtomicU64,
    error_bytes: AtomicU64,
    cancelled_rows: AtomicU64,
    cancelled_bytes: AtomicU64,
    best_effort_streams: AtomicU64,
    durable_streams: AtomicU64,
    latency: Mutex<[u64; 64]>,
}

impl Default for Counters {
    fn default() -> Self {
        Self {
            attempted_rows: AtomicU64::new(0),
            attempted_bytes: AtomicU64::new(0),
            completed_rows: AtomicU64::new(0),
            completed_bytes: AtomicU64::new(0),
            error_rows: AtomicU64::new(0),
            error_bytes: AtomicU64::new(0),
            cancelled_rows: AtomicU64::new(0),
            cancelled_bytes: AtomicU64::new(0),
            best_effort_streams: AtomicU64::new(0),
            durable_streams: AtomicU64::new(0),
            latency: Mutex::new([0; 64]),
        }
    }
}

impl Counters {
    fn snapshot(&self, elapsed: f64) -> serde_json::Value {
        let latency = self.latency.lock().unwrap_or_else(|error| {
            eprintln!("latency histogram was poisoned; retaining recorded samples");
            error.into_inner()
        });
        let count = latency.iter().sum::<u64>();
        let percentile = |numerator: u64| {
            if count == 0 {
                return None;
            }
            let target = count.saturating_mul(numerator).div_ceil(100);
            let mut observed = 0;
            for (bucket, samples) in latency.iter().enumerate() {
                observed += samples;
                if observed >= target {
                    return Some((1_u64 << bucket).saturating_sub(1));
                }
            }
            Some(u64::MAX)
        };
        serde_json::json!({
            "attempted_rows": self.attempted_rows.load(Ordering::Relaxed),
            "attempted_bytes": self.attempted_bytes.load(Ordering::Relaxed),
            "completed_rows": self.completed_rows.load(Ordering::Relaxed),
            "completed_bytes": self.completed_bytes.load(Ordering::Relaxed),
            "error_rows": self.error_rows.load(Ordering::Relaxed),
            "error_bytes": self.error_bytes.load(Ordering::Relaxed),
            "cancelled_rows": self.cancelled_rows.load(Ordering::Relaxed),
            "cancelled_bytes": self.cancelled_bytes.load(Ordering::Relaxed),
            "best_effort_streams": self.best_effort_streams.load(Ordering::Relaxed),
            "durable_streams": self.durable_streams.load(Ordering::Relaxed),
            "rows_per_second": self.completed_rows.load(Ordering::Relaxed) as f64 / elapsed.max(f64::MIN_POSITIVE),
            "send_latency_us": {"p50": percentile(50), "p95": percentile(95), "p99": percentile(99), "samples": count, "precision": "power_of_two_upper_bound"}
        })
    }
}

struct Attempt<'a> {
    counters: &'a Counters,
    batch: &'a EncodedBatch,
    started: Instant,
    finished: bool,
}

impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.counters
                .cancelled_rows
                .fetch_add(self.batch.rows, Ordering::Relaxed);
            self.counters
                .cancelled_bytes
                .fetch_add(self.batch.encoded_bytes, Ordering::Relaxed);
        }
    }
}

/// Immutable corpus reused by all measured sends. No DDL, encoding or connection creation in the hot path.
pub struct PreparedClient {
    pub options: Options,
    corpus: Vec<Arc<Vec<EncodedBatch>>>,
    channels: Vec<Vec<Channel>>,
    signer: Option<EdgeRequestSigner>,
    counters: Vec<Counters>,
    next: AtomicU64,
    reserved_rows: AtomicU64,
    pub table_count: usize,
    preparation: serde_json::Value,
    started: Instant,
}

impl PreparedClient {
    /// Build and encode the complete bounded corpus, register once, then establish persistent channels.
    ///
    /// # Errors
    ///
    /// Rejects allocation budgets before generation and propagates generator, codec, DDL and connection errors.
    pub async fn prepare(
        endpoint: &str,
        client_id: &str,
        table_name: Option<&str>,
        edge_secret_file: Option<&std::path::Path>,
        options: Options,
    ) -> Result<Arc<Self>> {
        anyhow::ensure!(
            options.batch_rows > 0
                && options.workers > 0
                && options.streams_per_worker > 0
                && (options.corpus_rows > 0 || options.corpus_bytes > 0),
            "prepare requires resolved, positive workload sizes"
        );
        let started = Instant::now();
        let cpu_started = thread_cpu_seconds()?;
        let mut tables = match table_name {
            Some(name) => vec![table_named(name)?],
            None => tables()?,
        };
        anyhow::ensure!(!tables.is_empty(), "corpus requires at least one table");
        let row_bytes: u64 = tables
            .iter()
            .map(|table| estimated_row_bytes(table, client_id, &options))
            .sum();
        let rows = if options.corpus_bytes > 0 {
            usize::try_from(options.corpus_bytes.div_ceil(row_bytes)).context("corpus byte target row count")?
        } else {
            options.corpus_rows
        };
        let copies = match options.data_mode {
            DataMode::Shared => 1,
            DataMode::Independent => options.workers,
        };
        let estimate = memory_estimate(&tables, rows, row_bytes, copies, &options)?;
        anyhow::ensure!(
            estimate <= options.max_memory_bytes,
            "corpus needs a conservative {estimate} bytes including encoding scratch, exceeding max-memory-bytes={}",
            options.max_memory_bytes
        );
        let signer = edge_secret_file.map(edge_signer_from_file).transpose()?;
        let mut registration = options.client(options.connect(endpoint).await?);
        let mut versions = Vec::new();
        for table in &tables {
            versions.push(
                tokio::time::timeout(
                    Duration::from_millis(options.request_timeout_ms),
                    register(&mut registration, table, signer.as_ref()),
                )
                .await
                .context("DMV registration timeout")??,
            );
        }
        let table_count = tables.len();
        let tables = tables.drain(..).map(Arc::new).collect::<Vec<_>>();
        let mut corpus = Vec::new();
        let mut decoded_bytes = 0_u64;
        let mut encoded_bytes = 0_u64;
        let mut generation_seconds = 0.0;
        let mut encoding_seconds = 0.0;
        let mut generation_cpu_seconds = cpu_started.map(|_| 0.0);
        let mut encoding_cpu_seconds = cpu_started.map(|_| 0.0);
        for copy in 0..copies {
            let mut batches = Vec::new();
            for (table, version) in tables.iter().zip(&versions) {
                for offset in (0..rows).step_by(options.batch_rows) {
                    let length = options.batch_rows.min(rows - offset);
                    let generation_start = Instant::now();
                    let cpu_start = thread_cpu_seconds()?;
                    let batch = synthetic_batch(table, client_id, length, offset, copy, &options)?;
                    generation_seconds += generation_start.elapsed().as_secs_f64();
                    if let Some((total, (end, start))) = generation_cpu_seconds
                        .as_mut()
                        .zip(thread_cpu_seconds()?.zip(cpu_start))
                    {
                        *total += (end - start).max(0.0);
                    }
                    let bytes = batch.get_array_memory_size() as u64;
                    decoded_bytes = decoded_bytes
                        .checked_add(bytes)
                        .context("decoded corpus size overflow")?;
                    let encoding_start = Instant::now();
                    let cpu_start = thread_cpu_seconds()?;
                    let data = encode(table, version, batch, &options).await?;
                    encoding_seconds += encoding_start.elapsed().as_secs_f64();
                    if let Some((total, (end, start))) =
                        encoding_cpu_seconds.as_mut().zip(thread_cpu_seconds()?.zip(cpu_start))
                    {
                        *total += (end - start).max(0.0);
                    }
                    let wire_bytes = data.iter().map(|message| message.encoded_len() as u64).sum::<u64>();
                    encoded_bytes = encoded_bytes
                        .checked_add(wire_bytes)
                        .context("encoded corpus size overflow")?;
                    anyhow::ensure!(
                        encoded_bytes <= options.max_memory_bytes,
                        "encoded corpus exceeded max-memory-bytes"
                    );
                    batches.push(EncodedBatch {
                        table: Arc::clone(table),
                        data: Arc::new(data),
                        rows: length as u64,
                        encoded_bytes: wire_bytes,
                    });
                }
            }
            corpus.push(Arc::new(batches));
        }
        let mut channels = Vec::new();
        for _ in 0..options.workers {
            let mut worker = Vec::new();
            for _ in 0..options.connections_per_worker {
                worker.push(options.connect(endpoint).await?);
            }
            channels.push(worker);
        }
        let preparation = serde_json::json!({
            "corpus_rows": rows as u64 * table_count as u64 * copies as u64,
            "corpus_decoded_bytes": decoded_bytes,
            "corpus_encoded_bytes": encoded_bytes,
            "retained_encoded_bytes": encoded_bytes,
            "memory_reservation_bytes": estimate,
            "generation_wall_seconds": generation_seconds,
            "generation_cpu_seconds": generation_cpu_seconds,
            "encoding_cpu_seconds": encoding_cpu_seconds,
            "preparation_cpu_seconds": thread_cpu_seconds()?.zip(cpu_started).map(|(end, start)| (end - start).max(0.0)),
            "generation_cpu_scope": "caller_thread_linux_schedstat_null_on_other_platforms",
            "encoding_wall_seconds": encoding_seconds,
            "preparation_wall_seconds": started.elapsed().as_secs_f64(),
            "generator_rows_per_second": rows as f64 * table_count as f64 * copies as f64 / generation_seconds.max(f64::MIN_POSITIVE),
            "steady_state_generation": false,
        });
        Ok(Arc::new(Self {
            counters: (0..options.workers).map(|_| Counters::default()).collect(),
            options,
            corpus,
            channels,
            signer,
            next: AtomicU64::new(0),
            reserved_rows: AtomicU64::new(0),
            table_count,
            preparation,
            started: Instant::now(),
        }))
    }

    /// Send independently scheduled streams on all workers; dropping this future aborts every owned task.
    ///
    /// # Errors
    ///
    /// Propagates any terminal stream failure without retrying an ambiguous upload.
    pub async fn send_round(self: &Arc<Self>) -> Result<UploadReport> {
        self.send_work(false).await
    }

    /// Upload the complete corpus once, without changing legacy one-shot row counts.
    ///
    /// # Errors
    ///
    /// Propagates terminal send failures without retrying an ambiguous upload.
    pub async fn send_once(self: &Arc<Self>) -> Result<UploadReport> {
        self.send_work(true).await
    }

    async fn send_work(self: &Arc<Self>, complete_corpus: bool) -> Result<UploadReport> {
        let mut tasks = JoinSet::new();
        for worker in 0..self.options.workers {
            let this = Arc::clone(self);
            tasks.spawn(async move {
                let corpus = &this.corpus[match this.options.data_mode {
                    DataMode::Shared => 0,
                    DataMode::Independent => worker,
                }];
                let indices: Vec<_> = if complete_corpus {
                    match this.options.data_mode {
                        DataMode::Shared => (worker..corpus.len()).step_by(this.options.workers).collect(),
                        DataMode::Independent => (0..corpus.len()).collect(),
                    }
                } else {
                    (0..this.options.streams_per_worker)
                        .map(|_| this.next.fetch_add(1, Ordering::Relaxed) as usize % corpus.len())
                        .collect()
                };
                let streams = stream::iter(indices.into_iter().enumerate()).map(|(stream_index, index)| {
                    let batch = &corpus[index];
                    let channel = this.channels[worker][stream_index % this.channels[worker].len()].clone();
                    this.send_one(worker, channel, batch)
                });
                streams
                    .buffer_unordered(this.options.streams_per_worker)
                    .try_collect::<Vec<_>>()
                    .await
            });
        }
        let mut report = UploadReport {
            tables: self.table_count,
            ..UploadReport::default()
        };
        while let Some(result) = tasks.join_next().await {
            for admission in result.context("client worker failed")??.into_iter().flatten() {
                let (guarantee, rows) = admission;
                report.completed_rows += rows;
                match guarantee {
                    DeliveryGuarantee::BestEffort => report.best_effort_streams += 1,
                    DeliveryGuarantee::DurableAdmission => report.durable_streams += 1,
                }
            }
        }
        Ok(report)
    }

    async fn send_one(
        &self,
        worker: usize,
        channel: Channel,
        batch: &EncodedBatch,
    ) -> Result<Option<(DeliveryGuarantee, u64)>> {
        if self.options.max_rows > 0
            && self
                .reserved_rows
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |rows| {
                    rows.checked_add(batch.rows)
                        .filter(|next| *next <= self.options.max_rows)
                })
                .is_err()
        {
            return Ok(None);
        }
        let counters = &self.counters[worker];
        counters.attempted_rows.fetch_add(batch.rows, Ordering::Relaxed);
        counters
            .attempted_bytes
            .fetch_add(batch.encoded_bytes, Ordering::Relaxed);
        let mut attempt = Attempt {
            counters,
            batch,
            started: Instant::now(),
            finished: false,
        };
        let mut request = Request::new(stream::iter(batch.data.as_ref().clone()));
        let timeout = Duration::from_millis(self.options.request_timeout_ms);
        request.set_timeout(timeout);
        let result = tokio::time::timeout(timeout, async {
            request_metadata(&mut request, &batch.table, self.signer.as_ref())?;
            completed_admission(self.options.client(channel).do_put(request).await?.into_inner()).await
        })
        .await
        .context("DMV upload timeout")
        .and_then(std::convert::identity);
        attempt.finished = true;
        let micros = u64::try_from(attempt.started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let bucket = (64 - micros.leading_zeros()) as usize;
        counters
            .latency
            .lock()
            .map_err(|_| anyhow::anyhow!("latency histogram poisoned"))?[bucket.min(63)] += 1;
        match result {
            Ok(guarantee) => {
                counters.completed_rows.fetch_add(batch.rows, Ordering::Relaxed);
                counters
                    .completed_bytes
                    .fetch_add(batch.encoded_bytes, Ordering::Relaxed);
                match guarantee {
                    DeliveryGuarantee::BestEffort => &counters.best_effort_streams,
                    DeliveryGuarantee::DurableAdmission => &counters.durable_streams,
                }
                .fetch_add(1, Ordering::Relaxed);
                if self.options.interval_us > 0 {
                    tokio::time::sleep(Duration::from_micros(self.options.interval_us)).await;
                }
                Ok(Some((guarantee, batch.rows)))
            }
            Err(error) => {
                counters.error_rows.fetch_add(batch.rows, Ordering::Relaxed);
                counters.error_bytes.fetch_add(batch.encoded_bytes, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    #[must_use]
    pub fn metrics(&self, phase: &str) -> serde_json::Value {
        serde_json::json!({
            "version": 1, "phase": phase, "scope": "process_cumulative",
            "completion_semantics": "validated_admission_response_not_delta_commit",
            "bytes_semantics": "sum_of_protobuf_flight_data_encoded_lengths_excluding_grpc_http2",
            "elapsed_seconds": self.started.elapsed().as_secs_f64(),
            "preparation": self.preparation,
            "workers": self.counters.iter().enumerate().map(|(id, counters)| {
                let mut value = counters.snapshot(self.started.elapsed().as_secs_f64());
                value["worker"] = serde_json::json!(id);
                value
            }).collect::<Vec<_>>(),
        })
    }
}

fn estimated_row_bytes(table: &DmvTable, client_id: &str, options: &Options) -> u64 {
    table
        .schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Utf8 if field.name() == "customer_id" => client_id.len() as u64 + 5,
            DataType::Utf8 => {
                (if options.string_width == 0 {
                    field.name().len() + 32
                } else {
                    options.string_width
                }) as u64
                    + 5
            }
            _ => 9,
        })
        .sum::<u64>()
        .max(1)
}

fn memory_estimate(tables: &[DmvTable], rows: usize, row_bytes: u64, copies: usize, options: &Options) -> Result<u64> {
    let fields = tables
        .iter()
        .map(|table| table.schema.fields().len() as u64)
        .sum::<u64>();
    let batches = (rows as u64).div_ceil(options.batch_rows as u64);
    // Reserve all decoded and encoded data plus a second encoding copy and per-array allocator slack.
    (rows as u64)
        .checked_mul(row_bytes)
        .and_then(|bytes| bytes.checked_mul(4))
        .and_then(|bytes| batches.checked_mul(fields)?.checked_mul(4096)?.checked_add(bytes))
        .and_then(|bytes| bytes.checked_mul(copies as u64))
        .context("corpus memory estimate overflow")
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn unit(value: u64) -> f64 {
    (value >> 11) as f64 / (1_u64 << 53) as f64
}

fn synthetic_batch(
    table: &DmvTable,
    client_id: &str,
    rows: usize,
    offset: usize,
    worker: usize,
    options: &Options,
) -> Result<RecordBatch> {
    let columns = table
        .schema
        .fields()
        .iter()
        .enumerate()
        .map(|(column, field)| -> Result<ArrayRef> {
            let random =
                |row: usize| mix(options.seed ^ mix((offset + row) as u64) ^ mix(column as u64) ^ mix(worker as u64));
            let key = |row: usize| {
                let value = random(row);
                if unit(mix(value)) < options.skew {
                    0
                } else {
                    value % options.cardinality as u64
                }
            };
            let valid = |row: usize| {
                !field.is_nullable()
                    || field.name() == "customer_id"
                    || unit(mix(random(row) ^ 41)) >= options.null_fraction
            };
            macro_rules! primitive {
                ($array:ty, $value:expr) => {
                    Arc::new(<$array>::from_iter(
                        (0..rows).map(|row| valid(row).then(|| $value(row))),
                    )) as ArrayRef
                };
            }
            Ok(match field.data_type() {
                DataType::Utf8 if field.name() == "customer_id" => {
                    Arc::new(StringArray::from_iter_values(std::iter::repeat_n(client_id, rows)))
                }
                DataType::Utf8 => Arc::new(StringArray::from_iter((0..rows).map(|row| {
                    valid(row).then(|| {
                        if options.string_width == 0 {
                            format!("{}-{}", field.name(), key(row))
                        } else {
                            let mut state = mix(options.seed ^ key(row) ^ mix(column as u64) ^ mix(worker as u64));
                            (0..options.string_width)
                                .map(|index| {
                                    state = mix(state);
                                    if index as f64 / (options.string_width as f64) < options.entropy {
                                        (b'a' + (state % 26) as u8) as char
                                    } else {
                                        'a'
                                    }
                                })
                                .collect::<String>()
                        }
                    })
                }))),
                DataType::Boolean => primitive!(BooleanArray, |row| key(row) % 2 == 0),
                DataType::UInt8 => primitive!(UInt8Array, |row| (key(row) % 255) as u8),
                DataType::Int16 => primitive!(Int16Array, |row| (key(row) % 32767) as i16),
                DataType::Int32 => primitive!(Int32Array, |row| (key(row) % i32::MAX as u64) as i32),
                DataType::Int64 => primitive!(Int64Array, |row| (key(row) & i64::MAX as u64) as i64),
                DataType::Float64 => primitive!(Float64Array, |row| key(row) as f64 / 10.0),
                DataType::Timestamp(TimeUnit::Microsecond, timezone) => Arc::new(
                    TimestampMicrosecondArray::from_iter(
                        (0..rows)
                            .map(|row| valid(row).then(|| 1_790_000_000_000_000_i64 + (key(row) % 1_000_000) as i64)),
                    )
                    .with_timezone_opt(timezone.clone()),
                ),
                other => bail!("unsupported synthetic DMV type {other}"),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::clone(&table.schema), columns)?)
}

async fn encode(table: &DmvTable, version: &str, batch: RecordBatch, options: &Options) -> Result<Vec<FlightData>> {
    let compression = match options.ipc_compression {
        IpcCompression::None => None,
        IpcCompression::Lz4Frame => Some(arrow_ipc::CompressionType::LZ4_FRAME),
        IpcCompression::Zstd => Some(arrow_ipc::CompressionType::ZSTD),
    };
    let descriptor = FlightDescriptor {
        r#type: DescriptorType::Path as i32,
        cmd: Default::default(),
        path: vec![
            table.identity.catalog().to_string(),
            table.identity.schema().to_string(),
            table.identity.table().to_string(),
            version.to_owned(),
        ],
    };
    let data = FlightDataEncoderBuilder::new()
        .with_schema(Arc::clone(&table.schema))
        .with_flight_descriptor(Some(descriptor))
        .with_options(IpcWriteOptions::default().try_with_compression(compression)?)
        .with_dictionary_handling(match options.dictionary_handling {
            Dictionaries::Hydrate => DictionaryHandling::Hydrate,
            Dictionaries::Resend => DictionaryHandling::Resend,
        })
        .with_max_flight_data_size(options.max_flight_data_bytes)
        .build(stream::iter([Ok::<RecordBatch, FlightError>(batch)]))
        .try_collect::<Vec<_>>()
        .await?;
    anyhow::ensure!(
        data.iter()
            .all(|message| message.encoded_len() <= options.max_message_bytes),
        "encoded FlightData exceeds max-message-bytes"
    );
    Ok(data)
}

fn thread_cpu_seconds() -> Result<Option<f64>> {
    #[cfg(target_os = "linux")]
    {
        let contents =
            std::fs::read_to_string("/proc/thread-self/schedstat").context("read generator thread CPU accounting")?;
        let nanos: u64 = contents
            .split_whitespace()
            .next()
            .context("missing scheduler CPU accounting")?
            .parse()?;
        Ok(Some(nanos as f64 / 1e9))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        options: Options,
    }

    fn options() -> Result<Options> {
        Ok(Cli::try_parse_from(["test", "--workers", "2"])?
            .options
            .resolve(100, 1)?)
    }

    #[test]
    fn rejects_invalid_enums_and_resource_bounds() -> Result<()> {
        for args in [
            vec!["--ipc-compression", "gzip"],
            vec!["--data-mode", "replicated"],
            vec!["--dictionary-handling", "silent"],
        ] {
            assert!(Cli::try_parse_from(std::iter::once("test").chain(args)).is_err());
        }
        for args in [
            vec!["--null-fraction", "NaN"],
            vec!["--entropy", "1.01"],
            vec!["--skew", "-0.1"],
            vec!["--connections-per-worker", "0"],
            vec!["--corpus-rows", "100", "--corpus-bytes", "100"],
            vec!["--max-memory-bytes", "0"],
            vec!["--stream-window-bytes", "2147483648"],
        ] {
            let parsed = Cli::try_parse_from(std::iter::once("test").chain(args));
            assert!(parsed.map(|cli| cli.options.resolve(10, 1).is_err()).unwrap_or(true));
        }
        Ok(())
    }

    #[tokio::test]
    async fn codecs_round_trip_deterministic_nullable_corpus() -> Result<()> {
        use arrow_flight::decode::FlightRecordBatchStream;
        let table = table_named("sqlserver_wait_stats")?;
        let mut options = options()?;
        options.string_width = 37;
        options.null_fraction = 0.5;
        let batch = synthetic_batch(&table, "client", 100, 0, 0, &options)?;
        assert_eq!(batch, synthetic_batch(&table, "client", 100, 0, 0, &options)?);
        assert_ne!(batch, synthetic_batch(&table, "client", 100, 0, 1, &options)?);
        for codec in [IpcCompression::None, IpcCompression::Lz4Frame, IpcCompression::Zstd] {
            options.ipc_compression = codec;
            let encoded = encode(&table, "v1", batch.clone(), &options).await?;
            let decoded = FlightRecordBatchStream::new_from_flight_data(stream::iter(encoded.into_iter().map(Ok)))
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(decoded, vec![batch.clone()]);
        }
        Ok(())
    }

    #[tokio::test]
    async fn memory_cap_is_rejected_before_connection() -> Result<()> {
        let mut options = options()?;
        options.corpus_rows = usize::MAX;
        options.max_memory_bytes = 1;
        let error = PreparedClient::prepare("not-an-endpoint", "client", None, None, options)
            .await
            .err()
            .context("expected memory error")?;
        assert!(error.to_string().contains("memory"));
        Ok(())
    }
}
