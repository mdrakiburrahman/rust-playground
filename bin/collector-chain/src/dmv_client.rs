//! Bounded, startup-prepared DMV workload and persistent Flight senders.

use super::*;
use arrow_flight::encode::DictionaryHandling;
use arrow_ipc::writer::IpcWriteOptions;
use arrowcol_protocol::NOT_ADMITTED_METADATA_KEY;
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
    /// One-shot total-row limit; duration-based runs ignore this cap and reuse the configured corpus.
    #[arg(long, env = "ARROWCOL_CLIENT_MAX_ROWS", default_value_t = 0)]
    pub max_rows: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_STREAM_RETRY_MAX_ATTEMPTS", default_value_t = 4)]
    pub stream_retry_max_attempts: u32,
    #[arg(long, env = "ARROWCOL_CLIENT_STREAM_RETRY_BASE_BACKOFF_MS", default_value_t = 10)]
    pub stream_retry_base_backoff_ms: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_STREAM_RETRY_MAX_BACKOFF_MS", default_value_t = 250)]
    pub stream_retry_max_backoff_ms: u64,
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
    pub fn resolve(self, rows: usize, concurrency: usize) -> Result<Self> {
        Ok(self.resolve_with_evidence(rows, concurrency)?.0)
    }

    /// Resolves from a single CPU-availability observation while retaining the exact requested sentinels.
    ///
    /// # Errors
    ///
    /// Returns the same validation failures as `resolve`, including unavailable CPU discovery.
    pub fn resolve_with_evidence(mut self, rows: usize, concurrency: usize) -> Result<(Self, serde_json::Value)> {
        let requested = serde_json::json!({
            "workers": self.workers, "runtime_threads": self.runtime_threads,
            "batch_rows": self.batch_rows, "corpus_rows": self.corpus_rows,
            "corpus_bytes": self.corpus_bytes, "streams_per_worker": self.streams_per_worker,
            "stream_retry_max_attempts": self.stream_retry_max_attempts,
            "stream_retry_base_backoff_ms": self.stream_retry_base_backoff_ms,
            "stream_retry_max_backoff_ms": self.stream_retry_max_backoff_ms,
        });
        let corpus_rows_source = if self.corpus_bytes > 0 {
            "corpus_bytes_estimate_deferred_until_preparation"
        } else if self.corpus_rows == 0 {
            "legacy_rows"
        } else {
            "explicit_rows"
        };
        let available_parallelism = std::thread::available_parallelism()?.get();
        anyhow::ensure!(
            matches!(self.dictionary_handling, Dictionaries::Hydrate),
            "dictionary-handling=resend is unsupported for the plain DMV synthetic schemas; no dictionary-bearing workload is generated"
        );
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
            self.workers = available_parallelism;
        }
        if self.runtime_threads == 0 {
            self.runtime_threads = available_parallelism;
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
                && (1..=65536).contains(&self.endpoint_concurrency_limit)
                && (1..=65536).contains(&self.endpoint_buffer_size)
                && self.connect_retries <= 100,
            "timeouts must be positive; endpoint capacities must be 1..=65536; connect-retries <=100"
        );
        anyhow::ensure!(
            (1..=16).contains(&self.stream_retry_max_attempts),
            "stream-retry-max-attempts must be between 1 and 16"
        );
        anyhow::ensure!(
            (1..=60_000).contains(&self.stream_retry_base_backoff_ms)
                && self.stream_retry_max_backoff_ms >= self.stream_retry_base_backoff_ms
                && self.stream_retry_max_backoff_ms <= 60_000,
            "stream retry backoff must satisfy 1 <= base <= max <= 60000 milliseconds"
        );
        anyhow::ensure!(
            self.interval_us <= 86_400_000_000,
            "interval-us must not exceed one day"
        );
        for value in [
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
        let evidence = serde_json::json!({
            "version": 1, "available_parallelism": available_parallelism,
            "requested": requested, "legacy": {"rows": rows, "concurrency": concurrency},
            "resolved": {
                "workers": self.workers, "runtime_threads": self.runtime_threads,
                "batch_rows": self.batch_rows, "corpus_rows": self.corpus_rows,
                "corpus_bytes": self.corpus_bytes, "streams_per_worker": self.streams_per_worker,
            "stream_retry_max_attempts": self.stream_retry_max_attempts,
            "stream_retry_base_backoff_ms": self.stream_retry_base_backoff_ms,
            "stream_retry_max_backoff_ms": self.stream_retry_max_backoff_ms,
            },
            "corpus_rows_source": corpus_rows_source,
        });
        Ok((self, evidence))
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
    raw_arrow_bytes: u64,
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
    rejected_streams: AtomicU64,
    rejected_rows: AtomicU64,
    retried_streams: AtomicU64,
    retried_rows: AtomicU64,
    given_up_streams: AtomicU64,
    given_up_rows: AtomicU64,
    ambiguous_streams: AtomicU64,
    ambiguous_rows: AtomicU64,
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
            rejected_streams: AtomicU64::new(0),
            rejected_rows: AtomicU64::new(0),
            retried_streams: AtomicU64::new(0),
            retried_rows: AtomicU64::new(0),
            given_up_streams: AtomicU64::new(0),
            given_up_rows: AtomicU64::new(0),
            ambiguous_streams: AtomicU64::new(0),
            ambiguous_rows: AtomicU64::new(0),
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
            "rejected_streams": self.rejected_streams.load(Ordering::Relaxed),
            "rejected_rows": self.rejected_rows.load(Ordering::Relaxed),
            "retried_streams": self.retried_streams.load(Ordering::Relaxed),
            "retried_rows": self.retried_rows.load(Ordering::Relaxed),
            "given_up_streams": self.given_up_streams.load(Ordering::Relaxed),
            "given_up_rows": self.given_up_rows.load(Ordering::Relaxed),
            "ambiguous_streams": self.ambiguous_streams.load(Ordering::Relaxed),
            "ambiguous_rows": self.ambiguous_rows.load(Ordering::Relaxed),
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

struct Measurement {
    start_unix_ms: u64,
    end_unix_ms: u64,
    rows: AtomicU64,
    raw_arrow_bytes: AtomicU64,
    bytes: AtomicU64,
    worker_rows: Vec<AtomicU64>,
    pilot_rows: Vec<AtomicU64>,
    raw_window_bytes: Vec<AtomicU64>,
    wire_window_bytes: Vec<AtomicU64>,
    window_latency: Vec<Mutex<[u64; 64]>>,
    resource_start: Mutex<Option<ResourceSample>>,
    resource_end: Mutex<Option<ResourceSample>>,
    rejected_streams: AtomicU64,
    rejected_rows: AtomicU64,
    retried_streams: AtomicU64,
    retried_rows: AtomicU64,
    given_up_streams: AtomicU64,
    given_up_rows: AtomicU64,
    ambiguous_streams: AtomicU64,
    ambiguous_rows: AtomicU64,
}

fn latency_percentiles(latency: &[u64; 64]) -> serde_json::Value {
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
    serde_json::json!({ "p50": percentile(50), "p95": percentile(95), "p99": percentile(99), "samples": count, "precision": "power_of_two_upper_bound" })
}
impl Measurement {
    fn new(start_unix_ms: u64, end_unix_ms: u64, workers: usize) -> Result<Self> {
        anyhow::ensure!(end_unix_ms > start_unix_ms, "measurement window must be positive");
        Ok(Self {
            start_unix_ms,
            end_unix_ms,
            rows: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            raw_arrow_bytes: AtomicU64::new(0),
            worker_rows: (0..workers).map(|_| AtomicU64::new(0)).collect(),
            pilot_rows: (0..(end_unix_ms - start_unix_ms).div_ceil(30_000))
                .map(|_| AtomicU64::new(0))
                .collect(),
            raw_window_bytes: (0..(end_unix_ms - start_unix_ms).div_ceil(30_000))
                .map(|_| AtomicU64::new(0))
                .collect(),
            wire_window_bytes: (0..(end_unix_ms - start_unix_ms).div_ceil(30_000))
                .map(|_| AtomicU64::new(0))
                .collect(),
            window_latency: (0..(end_unix_ms - start_unix_ms).div_ceil(30_000))
                .map(|_| Mutex::new([0; 64]))
                .collect(),
            resource_start: Mutex::new(None),
            resource_end: Mutex::new(None),
            rejected_streams: AtomicU64::new(0),
            rejected_rows: AtomicU64::new(0),
            retried_streams: AtomicU64::new(0),
            retried_rows: AtomicU64::new(0),
            given_up_streams: AtomicU64::new(0),
            given_up_rows: AtomicU64::new(0),
            ambiguous_streams: AtomicU64::new(0),
            ambiguous_rows: AtomicU64::new(0),
        })
    }

    fn record_completion(
        &self,
        worker: usize,
        rows: u64,
        raw_arrow_bytes: u64,
        wire_bytes: u64,
        observed_unix_ms: u64,
    ) {
        if observed_unix_ms >= self.start_unix_ms && observed_unix_ms < self.end_unix_ms {
            self.rows.fetch_add(rows, Ordering::Relaxed);
            self.bytes.fetch_add(wire_bytes, Ordering::Relaxed);
            self.raw_arrow_bytes.fetch_add(raw_arrow_bytes, Ordering::Relaxed);
            self.worker_rows[worker].fetch_add(rows, Ordering::Relaxed);
            let index = ((observed_unix_ms - self.start_unix_ms) / 30_000) as usize;
            self.pilot_rows[index].fetch_add(rows, Ordering::Relaxed);
            self.raw_window_bytes[index].fetch_add(raw_arrow_bytes, Ordering::Relaxed);
            self.wire_window_bytes[index].fetch_add(wire_bytes, Ordering::Relaxed);
        }
    }

    fn record_send_latency(&self, micros: u64, send_unix_ns: u64) -> Result<()> {
        let send_unix_ms = send_unix_ns / 1_000_000;
        if send_unix_ms >= self.start_unix_ms && send_unix_ms < self.end_unix_ms {
            let index = ((send_unix_ms - self.start_unix_ms) / 30_000) as usize;
            let bucket = (64 - micros.leading_zeros()) as usize;
            self.window_latency[index]
                .lock()
                .map_err(|_| anyhow::anyhow!("measurement latency histogram poisoned"))?[bucket.min(63)] += 1;
        }
        Ok(())
    }

    fn record_resource_sample(&self, sample: ResourceSample, is_start: bool) -> Result<()> {
        let target = if is_start {
            &self.resource_start
        } else {
            &self.resource_end
        };
        let mut target = target
            .lock()
            .map_err(|_| anyhow::anyhow!("resource sample state poisoned"))?;
        anyhow::ensure!(target.is_none(), "resource sample already recorded");
        *target = Some(sample);
        Ok(())
    }

    fn resource_metrics(&self) -> serde_json::Value {
        let start = self
            .resource_start
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let end = self
            .resource_end
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        resource_report(start, end)
    }

    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "start_unix_ms": self.start_unix_ms, "end_unix_ms": self.end_unix_ms,
            "completed_rows": self.rows.load(Ordering::Relaxed),
            "completed_bytes": self.bytes.load(Ordering::Relaxed),
            "completed_wire_bytes": self.bytes.load(Ordering::Relaxed),
            "completed_raw_arrow_bytes": self.raw_arrow_bytes.load(Ordering::Relaxed),
            "worker_completed_rows": self.worker_rows.iter().map(|value| value.load(Ordering::Relaxed)).collect::<Vec<_>>(),
            "pilot_window_rows": self.pilot_rows.iter().map(|value| value.load(Ordering::Relaxed)).collect::<Vec<_>>(),
            "windows": (0..self.pilot_rows.len()).map(|index| {
                let start_unix_ms = self.start_unix_ms + index as u64 * 30_000;
                let end_unix_ms = self.end_unix_ms.min(start_unix_ms.saturating_add(30_000));
                let latency = self.window_latency[index].lock().unwrap_or_else(|error| error.into_inner());
                serde_json::json!({
                    "start_unix_ms": start_unix_ms, "end_unix_ms": end_unix_ms,
                    "completed_rows": self.pilot_rows[index].load(Ordering::Relaxed),
                    "completed_raw_arrow_bytes": self.raw_window_bytes[index].load(Ordering::Relaxed),
                    "completed_wire_bytes": self.wire_window_bytes[index].load(Ordering::Relaxed),
                    "send_latency_us": latency_percentiles(&latency),
                    "send_latency_scope": "attempts_grouped_by_fresh_send_timestamp_in_half_open_window"
                })
            }).collect::<Vec<_>>(),
            "rejected_streams": self.rejected_streams.load(Ordering::Relaxed),
            "rejected_rows": self.rejected_rows.load(Ordering::Relaxed),
            "retried_streams": self.retried_streams.load(Ordering::Relaxed),
            "retried_rows": self.retried_rows.load(Ordering::Relaxed),
            "given_up_streams": self.given_up_streams.load(Ordering::Relaxed),
            "given_up_rows": self.given_up_rows.load(Ordering::Relaxed),
            "ambiguous_streams": self.ambiguous_streams.load(Ordering::Relaxed),
            "ambiguous_rows": self.ambiguous_rows.load(Ordering::Relaxed),
            "scope": "per_stream_admission_completion_observed_in_half_open_shared_unix_window",
            "client_resources": self.resource_metrics(),
        })
    }

    fn record_rejection(&self, rows: u64, observed_unix_ms: u64) {
        if observed_unix_ms >= self.start_unix_ms && observed_unix_ms < self.end_unix_ms {
            self.rejected_streams.fetch_add(1, Ordering::Relaxed);
            self.rejected_rows.fetch_add(rows, Ordering::Relaxed);
        }
    }

    fn record_retry(&self, rows: u64, observed_unix_ms: u64) {
        if observed_unix_ms >= self.start_unix_ms && observed_unix_ms < self.end_unix_ms {
            self.retried_streams.fetch_add(1, Ordering::Relaxed);
            self.retried_rows.fetch_add(rows, Ordering::Relaxed);
        }
    }

    fn record_given_up(&self, rows: u64, observed_unix_ms: u64) {
        if observed_unix_ms >= self.start_unix_ms && observed_unix_ms < self.end_unix_ms {
            self.given_up_streams.fetch_add(1, Ordering::Relaxed);
            self.given_up_rows.fetch_add(rows, Ordering::Relaxed);
        }
    }

    fn record_ambiguous(&self, rows: u64, observed_unix_ms: u64) {
        if observed_unix_ms >= self.start_unix_ms && observed_unix_ms < self.end_unix_ms {
            self.ambiguous_streams.fetch_add(1, Ordering::Relaxed);
            self.ambiguous_rows.fetch_add(rows, Ordering::Relaxed);
        }
    }
}

/// Exact completion-window counters after persistent workers have stopped and their requests have drained.
pub struct ContinuousReport {
    pub completed_rows: u64,
    pub measurement_started_unix_ms: u64,
    pub pilot_window_rows: Vec<u64>,
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
    worker_next: Vec<AtomicU64>,
    reserved_rows: AtomicU64,
    measurement: Mutex<Option<Arc<Measurement>>>,
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
        anyhow::ensure!(
            options.max_rows == 0 || options.max_rows >= rows.min(options.batch_rows) as u64,
            "max-rows must fit at least one complete corpus batch"
        );
        let estimate = memory_estimate(&tables, rows, row_bytes, copies, &options)?;
        anyhow::ensure!(
            estimate <= options.max_memory_bytes,
            "corpus needs a conservative {estimate} bytes including encoding scratch, exceeding max-memory-bytes={}",
            options.max_memory_bytes
        );
        for table in &tables {
            for field in table.schema.fields() {
                if field.data_type() == &DataType::Utf8 {
                    let width = if field.name() == "customer_id" {
                        client_id.len()
                    } else if options.string_width == 0 {
                        field.name().len() + 32
                    } else {
                        options.string_width
                    };
                    anyhow::ensure!(
                        rows.min(options.batch_rows)
                            .checked_mul(width)
                            .is_some_and(|bytes| bytes <= i32::MAX as usize),
                        "batch-rows and string-width exceed Arrow Utf8's 2147483647-byte column limit"
                    );
                }
            }
        }
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
                        raw_arrow_bytes: bytes,
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
            worker_next: (0..options.workers).map(|_| AtomicU64::new(0)).collect(),
            options,
            corpus,
            channels,
            signer,
            next: AtomicU64::new(0),
            reserved_rows: AtomicU64::new(0),
            measurement: Mutex::new(None),
            table_count,
            preparation,
            started: Instant::now(),
        }))
    }

    fn next_batch(&self, worker: usize) -> &EncodedBatch {
        let (corpus, cursor) = match self.options.data_mode {
            DataMode::Shared => (&self.corpus[0], &self.next),
            DataMode::Independent => (&self.corpus[worker], &self.worker_next[worker]),
        };
        if corpus.len() == 1 {
            &corpus[0]
        } else {
            &corpus[cursor.fetch_add(1, Ordering::Relaxed) as usize % corpus.len()]
        }
    }

    /// Continuously replenish each worker's independent stream slots through warm-up and measurement.
    ///
    /// In-flight uploads are not cancelled at the warm-up or measurement boundary. Their actual observed
    /// completion time determines attribution; after the end, bounded requests drain before returning.
    ///
    /// # Errors
    ///
    /// Propagates clock, send, worker, or schedule failures after aborting and joining owned tasks.
    pub async fn run_continuous(
        self: &Arc<Self>,
        scheduled_start_unix_ms: u64,
        measurement_start_unix_ms: u64,
        measurement_end_unix_ms: u64,
    ) -> Result<ContinuousReport> {
        let measurement = Arc::new(Measurement::new(
            measurement_start_unix_ms,
            measurement_end_unix_ms,
            self.options.workers,
        )?);
        *self
            .measurement
            .lock()
            .map_err(|_| anyhow::anyhow!("measurement state poisoned"))? = Some(Arc::clone(&measurement));
        while unix_ms()? < scheduled_start_unix_ms {
            tokio::time::sleep(Duration::from_millis(
                scheduled_start_unix_ms.saturating_sub(unix_ms()?).min(10),
            ))
            .await;
        }
        let scheduled_start_observed_unix_ms = unix_ms()?;
        anyhow::ensure!(
            scheduled_start_observed_unix_ms.abs_diff(scheduled_start_unix_ms) <= 250,
            "client did not start within 250ms of the shared scheduled interval"
        );
        let mut tasks = JoinSet::new();
        for worker in 0..self.options.workers {
            for slot in 0..self.options.streams_per_worker {
                let this = Arc::clone(self);
                let measurement = Arc::clone(&measurement);
                tasks.spawn(async move {
                    let channel = this.channels[worker][slot % this.channels[worker].len()].clone();
                    replenish_until(measurement.end_unix_ms, this.options.interval_us, || async {
                        this.send_continuous_one(worker, channel.clone(), this.next_batch(worker), &measurement)
                            .await
                    })
                    .await
                });
            }
        }
        let phase = async {
            while unix_ms()? < measurement_start_unix_ms {
                tokio::time::sleep(Duration::from_millis(
                    measurement_start_unix_ms.saturating_sub(unix_ms()?).min(10),
                ))
                .await;
            }
            let observed = unix_ms()?;
            anyhow::ensure!(
                observed.abs_diff(measurement_start_unix_ms) <= 250,
                "client did not start within 250ms of the shared measurement interval"
            );
            measurement.record_resource_sample(ResourceSample::capture()?, true)?;
            println!("OAT_CLIENT_METRICS {}", self.metrics("measurement_start"));
            Ok::<u64, anyhow::Error>(observed)
        };
        tokio::pin!(phase);
        let mut measurement_started = None;
        let resource_end = async {
            while unix_ms()? < measurement_end_unix_ms {
                tokio::time::sleep(Duration::from_millis(
                    measurement_end_unix_ms.saturating_sub(unix_ms()?).min(10),
                ))
                .await;
            }
            measurement.record_resource_sample(ResourceSample::capture()?, false)
        };
        tokio::pin!(resource_end);
        let mut resource_end_recorded = false;
        loop {
            tokio::select! {
                result = &mut phase, if measurement_started.is_none() => {
                    match result {
                        Ok(started) => measurement_started = Some(started),
                        Err(error) => {
                            tasks.abort_all();
                            while tasks.join_next().await.is_some() {}
                            return Err(error);
                        }
                    }
                }
                result = &mut resource_end, if !resource_end_recorded => {
                    match result {
                        Ok(()) => resource_end_recorded = true,
                        Err(error) => {
                            tasks.abort_all();
                            while tasks.join_next().await.is_some() {}
                            return Err(error);
                        }
                    }
                }
                result = tasks.join_next(), if !tasks.is_empty() => {
                    match result {
                        Some(Ok(Ok(()))) => {}
                        Some(result) => {
                            tasks.abort_all();
                            while tasks.join_next().await.is_some() {}
                            return match result {
                                Ok(Err(error)) => Err(error),
                                Err(error) => Err(error).context("persistent client worker failed"),
                                Ok(Ok(())) => unreachable!(),
                            };
                        }
                        None => {}
                    }
                }
                else => break,
            }
        }
        Ok(ContinuousReport {
            completed_rows: measurement.rows.load(Ordering::Relaxed),
            measurement_started_unix_ms: measurement_started.context("measurement phase never started")?,
            pilot_window_rows: measurement
                .pilot_rows
                .iter()
                .map(|value| value.load(Ordering::Relaxed))
                .collect(),
        })
    }

    #[must_use]
    pub fn admission_counts(&self) -> (u64, u64) {
        self.counters.iter().fold((0, 0), |(best_effort, durable), counters| {
            (
                best_effort + counters.best_effort_streams.load(Ordering::Relaxed),
                durable + counters.durable_streams.load(Ordering::Relaxed),
            )
        })
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
                        .map(|_| {
                            match this.options.data_mode {
                                DataMode::Shared => &this.next,
                                DataMode::Independent => &this.worker_next[worker],
                            }
                            .fetch_add(1, Ordering::Relaxed) as usize
                                % corpus.len()
                        })
                        .collect()
                };
                let streams = stream::iter(indices.into_iter().enumerate()).map(|(stream_index, index)| {
                    let batch = &corpus[index];
                    let channel = this.channels[worker][stream_index % this.channels[worker].len()].clone();
                    this.send_one(worker, channel, batch, None)
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
            let admissions = match result.context("client worker failed").and_then(std::convert::identity) {
                Ok(admissions) => admissions,
                Err(error) => {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    return Err(error);
                }
            };
            for admission in admissions.into_iter().flatten() {
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
        measurement: Option<&Measurement>,
    ) -> Result<Option<(DeliveryGuarantee, u64)>> {
        if measurement.is_none()
            && self.options.max_rows > 0
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
        let observation = SendObservation::fresh()?;
        let mut request = Request::new(stream::iter(batch.data.as_ref().clone()));
        let timeout = Duration::from_millis(self.options.request_timeout_ms);
        let result = tokio::time::timeout(timeout, async {
            request_metadata(&mut request, &batch.table, self.signer.as_ref(), Some(&observation))?;
            request.set_timeout(timeout);
            completed_admission(self.options.client(channel).do_put(request).await?.into_inner()).await
        })
        .await
        .context("DMV upload timeout")
        .and_then(std::convert::identity);
        let completed_unix_ms = if result.is_ok() && measurement.is_some() {
            Some(unix_ms()?)
        } else {
            None
        };
        attempt.finished = true;
        let micros = u64::try_from(attempt.started.elapsed().as_micros()).unwrap_or(u64::MAX);
        if let Some(measurement) = measurement {
            measurement.record_send_latency(micros, observation.send_unix_ns)?;
        }
        let bucket = (64 - micros.leading_zeros()) as usize;
        counters
            .latency
            .lock()
            .map_err(|_| anyhow::anyhow!("latency histogram poisoned"))?[bucket.min(63)] += 1;
        match result {
            Ok(guarantee) => {
                if let Some((measurement, completed_unix_ms)) = measurement.zip(completed_unix_ms) {
                    measurement.record_completion(
                        worker,
                        batch.rows,
                        batch.raw_arrow_bytes,
                        batch.encoded_bytes,
                        completed_unix_ms,
                    );
                }
                counters.completed_rows.fetch_add(batch.rows, Ordering::Relaxed);
                counters
                    .completed_bytes
                    .fetch_add(batch.encoded_bytes, Ordering::Relaxed);
                match guarantee {
                    DeliveryGuarantee::BestEffort => &counters.best_effort_streams,
                    DeliveryGuarantee::DurableAdmission => &counters.durable_streams,
                }
                .fetch_add(1, Ordering::Relaxed);
                Ok(Some((guarantee, batch.rows)))
            }
            Err(error) => {
                counters.error_rows.fetch_add(batch.rows, Ordering::Relaxed);
                counters.error_bytes.fetch_add(batch.encoded_bytes, Ordering::Relaxed);
                if is_not_admitted(&error) {
                    counters.rejected_streams.fetch_add(1, Ordering::Relaxed);
                    counters.rejected_rows.fetch_add(batch.rows, Ordering::Relaxed);
                    if let Some(measurement) = measurement {
                        measurement.record_rejection(batch.rows, unix_ms()?);
                    }
                }
                Err(error)
            }
        }
    }

    async fn send_continuous_one(
        &self,
        worker: usize,
        channel: Channel,
        batch: &EncodedBatch,
        measurement: &Measurement,
    ) -> Result<bool> {
        for attempt in 1..=self.options.stream_retry_max_attempts {
            match self.send_one(worker, channel.clone(), batch, Some(measurement)).await {
                Ok(Some(_)) => return Ok(true),
                Ok(None) => return Ok(false),
                Err(error) if is_not_admitted(&error) => {
                    if attempt == self.options.stream_retry_max_attempts {
                        let counters = &self.counters[worker];
                        counters.given_up_streams.fetch_add(1, Ordering::Relaxed);
                        counters.given_up_rows.fetch_add(batch.rows, Ordering::Relaxed);
                        measurement.record_given_up(batch.rows, unix_ms()?);
                        return Ok(true);
                    }
                    let counters = &self.counters[worker];
                    counters.retried_streams.fetch_add(1, Ordering::Relaxed);
                    counters.retried_rows.fetch_add(batch.rows, Ordering::Relaxed);
                    measurement.record_retry(batch.rows, unix_ms()?);
                    tokio::time::sleep(stream_retry_backoff(
                        self.options.stream_retry_base_backoff_ms,
                        self.options.stream_retry_max_backoff_ms,
                        attempt,
                    ))
                    .await;
                }
                Err(error) if is_ambiguous_continuous_error(&error) => {
                    let counters = &self.counters[worker];
                    counters.ambiguous_streams.fetch_add(1, Ordering::Relaxed);
                    counters.ambiguous_rows.fetch_add(batch.rows, Ordering::Relaxed);
                    measurement.record_ambiguous(batch.rows, unix_ms()?);
                    return Ok(true);
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("validated stream retry attempt count is positive")
    }

    #[must_use]
    pub fn metrics(&self, phase: &str) -> serde_json::Value {
        let measurement = self.measurement.lock().unwrap_or_else(|error| {
            eprintln!("measurement state was poisoned; retaining recorded counters");
            error.into_inner()
        });
        serde_json::json!({
            "version": 1, "phase": phase, "scope": "process_cumulative",
            "completion_semantics": "validated_admission_response_not_delta_commit",
            "send_latency_scope": "request_metadata_protobuf_grpc_compression_network_and_acknowledgement_excluding_interval",
            "bytes_semantics": "sum_of_protobuf_flight_data_encoded_lengths_excluding_grpc_http2",
            "send_observation": {
                "id_header": CLIENT_SEND_OBSERVATION_ID_HEADER,
                "timestamp_header": CLIENT_SEND_UNIX_NS_HEADER,
                "timestamp_unit": "unix_nanoseconds",
                "identity_scope": "unique_per_do_put_attempt_including_retries",
                "clock_uncertainty_ns": null
            },
            "elapsed_seconds": self.started.elapsed().as_secs_f64(),
            "preparation": self.preparation,
            "measurement_window": measurement.as_ref().map(|value| value.snapshot()),
            "workers": self.counters.iter().enumerate().map(|(id, counters)| {
                let mut value = counters.snapshot(self.started.elapsed().as_secs_f64());
                value["worker"] = serde_json::json!(id);
                value
            }).collect::<Vec<_>>(),
        })
    }
}

fn is_ambiguous_continuous_error(error: &anyhow::Error) -> bool {
    let Some(status) = error.chain().find_map(|cause| cause.downcast_ref::<tonic::Status>()) else {
        return true;
    };
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::ResourceExhausted
            | tonic::Code::Aborted
            | tonic::Code::Internal
            | tonic::Code::Unknown
            | tonic::Code::DataLoss
    )
}

fn is_not_admitted(error: &anyhow::Error) -> bool {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<tonic::Status>())
        .is_some_and(|status| {
            status
                .metadata()
                .get(NOT_ADMITTED_METADATA_KEY)
                .and_then(|value| value.to_str().ok())
                == Some("true")
        })
}

static STREAM_RETRY_JITTER: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);

fn stream_retry_backoff(base_ms: u64, max_ms: u64, failed_attempt: u32) -> Duration {
    let shift = failed_attempt.saturating_sub(1).min(63);
    let multiplier = 1_u64.checked_shl(shift).unwrap_or(u64::MAX);
    let cap_ms = base_ms.saturating_mul(multiplier).min(max_ms);
    let lower_ms = cap_ms / 2;
    let range = cap_ms.saturating_sub(lower_ms).saturating_add(1);
    let entropy = mix(STREAM_RETRY_JITTER.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed));
    Duration::from_millis(lower_ms.saturating_add(entropy % range))
}

fn unix_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

async fn replenish_until<F, Fut>(end_unix_ms: u64, interval_us: u64, mut send: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    while unix_ms()? < end_unix_ms {
        if !send().await? {
            break;
        }
        if interval_us > 0 {
            let remaining_ms = end_unix_ms.saturating_sub(unix_ms()?);
            tokio::time::sleep(Duration::from_micros(
                interval_us.min(remaining_ms.saturating_mul(1000)),
            ))
            .await;
        }
    }
    Ok(())
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
                DataType::Boolean => primitive!(BooleanArray, |row| key(row).is_multiple_of(2)),
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

#[derive(Clone, Debug)]
struct ResourceSample {
    sampled_unix_ms: u64,
    process_cpu_ns: Option<u64>,
    network_rx_bytes: Option<u64>,
    network_tx_bytes: Option<u64>,
    network_counters_unavailable_reason: Option<String>,
    network_advertised_link_speed_bps: Option<u64>,
    network_interface_speed_unavailable_reasons: Vec<String>,
    available_parallelism: Option<usize>,
}
impl ResourceSample {
    fn capture() -> Result<Self> {
        let process_cpu_ns = process_cpu_ns()?;
        let network = network_snapshot()?;
        Ok(Self {
            sampled_unix_ms: unix_ms()?,
            process_cpu_ns,
            network_rx_bytes: network.0,
            network_tx_bytes: network.1,
            network_counters_unavailable_reason: network.2,
            network_advertised_link_speed_bps: network.3,
            network_interface_speed_unavailable_reasons: network.4,
            available_parallelism: std::thread::available_parallelism().ok().map(|value| value.get()),
        })
    }
}

#[cfg(target_os = "linux")]
fn process_cpu_ns() -> Result<Option<u64>> {
    let contents = match std::fs::read_to_string("/proc/self/stat") {
        Ok(contents) => contents,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error).context("read process CPU accounting"),
    };
    let command_end = contents
        .rfind(')')
        .context("process stat is missing its command terminator")?;
    let fields = contents[command_end + 1..].split_whitespace().collect::<Vec<_>>();
    let user_ticks: u64 = fields
        .get(11)
        .context("process stat is missing user CPU accounting")?
        .parse()?;
    let system_ticks: u64 = fields
        .get(12)
        .context("process stat is missing system CPU accounting")?
        .parse()?;
    let Some(ticks_per_second) = clock_ticks_per_second()? else {
        return Ok(None);
    };
    let total_ticks = user_ticks
        .checked_add(system_ticks)
        .context("process CPU tick counter overflow")?;
    let total_ns = total_ticks
        .checked_mul(1_000_000_000)
        .context("process CPU nanosecond conversion overflow")?
        / ticks_per_second;
    Ok(Some(total_ns))
}

#[cfg(target_os = "linux")]
fn clock_ticks_per_second() -> Result<Option<u64>> {
    const AT_CLKTCK: usize = 17;
    let contents = match std::fs::read("/proc/self/auxv") {
        Ok(contents) => contents,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error).context("read process clock tick rate"),
    };
    let word_size = std::mem::size_of::<usize>();
    let entry_size = word_size * 2;
    anyhow::ensure!(
        contents.len() % entry_size == 0,
        "process auxiliary vector has an incomplete entry"
    );
    for entry in contents.chunks_exact(entry_size) {
        let kind = usize::from_ne_bytes(entry[..word_size].try_into()?);
        let value = usize::from_ne_bytes(entry[word_size..].try_into()?);
        if kind == AT_CLKTCK {
            return Ok(u64::try_from(value).ok().filter(|value| *value > 0));
        }
        if kind == 0 {
            break;
        }
    }
    Ok(None)
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_ns() -> Result<Option<u64>> {
    Ok(None)
}

fn read_advertised_link_speed(contents: std::io::Result<String>) -> Result<(Option<u64>, Option<String>)> {
    let contents = match contents {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {
            return Ok((None, Some("unsupported_interface".to_owned())));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((None, Some("interface_disappeared".to_owned())));
        }
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            return Ok((None, Some("permission_denied".to_owned())));
        }
        Err(error) => return Err(error).context("read interface link speed"),
    };
    let speed_mbps = contents.trim().parse::<i64>().ok().filter(|speed| *speed > 0);
    let Some(speed_mbps) = speed_mbps else {
        return Ok((None, Some("speed_not_reported".to_owned())));
    };
    let speed_bps = (speed_mbps as u64)
        .checked_mul(1_000_000)
        .context("advertised link speed overflow")?;
    Ok((Some(speed_bps), None))
}

fn network_snapshot() -> Result<(Option<u64>, Option<u64>, Option<String>, Option<u64>, Vec<String>)> {
    let contents = match std::fs::read_to_string("/proc/net/dev") {
        Ok(contents) => contents,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok((
                None,
                None,
                Some("network_counters_unavailable".to_owned()),
                None,
                Vec::new(),
            ));
        }
        Err(error) => return Err(error).context("read client network counters"),
    };
    let mut rx_bytes = 0_u64;
    let mut tx_bytes = 0_u64;
    let mut advertised_speed_bps = 0_u64;
    let mut all_speeds_known = true;
    let mut speed_unavailable_reasons = Vec::new();
    let mut interfaces = 0_usize;
    for line in contents.lines().skip(2) {
        let Some((name, values)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        if name == "lo" {
            continue;
        }
        let values = values
            .split_whitespace()
            .map(str::parse::<u64>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        anyhow::ensure!(
            values.len() >= 9,
            "network counters for {name} have fewer than nine fields"
        );
        rx_bytes = rx_bytes
            .checked_add(values[0])
            .context("network receive counter overflow")?;
        tx_bytes = tx_bytes
            .checked_add(values[8])
            .context("network transmit counter overflow")?;
        let speed_path = std::path::Path::new("/sys/class/net").join(name).join("speed");
        let (speed_bps, unavailable_reason) = read_advertised_link_speed(std::fs::read_to_string(&speed_path))
            .with_context(|| format!("read {}", speed_path.display()))?;
        if let Some(speed_bps) = speed_bps {
            advertised_speed_bps = advertised_speed_bps
                .checked_add(speed_bps)
                .context("advertised link speed total overflow")?;
        } else {
            all_speeds_known = false;
            if let Some(reason) = unavailable_reason {
                speed_unavailable_reasons.push(reason);
            }
        }
        interfaces += 1;
    }
    if interfaces == 0 {
        return Ok((
            None,
            None,
            Some("no_non_loopback_interfaces".to_owned()),
            None,
            Vec::new(),
        ));
    }
    Ok((
        Some(rx_bytes),
        Some(tx_bytes),
        None,
        all_speeds_known.then_some(advertised_speed_bps),
        speed_unavailable_reasons,
    ))
}

fn resource_report(start: Option<ResourceSample>, end: Option<ResourceSample>) -> serde_json::Value {
    let elapsed_ns = start
        .as_ref()
        .zip(end.as_ref())
        .and_then(|(start, end)| end.sampled_unix_ms.checked_sub(start.sampled_unix_ms))
        .and_then(|millis| millis.checked_mul(1_000_000));
    let cpu_ns = start
        .as_ref()
        .zip(end.as_ref())
        .and_then(|(start, end)| start.process_cpu_ns.zip(end.process_cpu_ns))
        .and_then(|(start, end)| end.checked_sub(start));
    let cpu_seconds = cpu_ns.map(|nanos| nanos as f64 / 1e9);
    let elapsed_seconds = elapsed_ns
        .map(|nanos| nanos as f64 / 1e9)
        .filter(|seconds| *seconds > 0.0);
    let available_cores = start
        .as_ref()
        .zip(end.as_ref())
        .and_then(|(start, end)| start.available_parallelism.zip(end.available_parallelism))
        .map(|(start, end)| start.min(end));
    let cpu_core_equivalents = cpu_seconds.zip(elapsed_seconds).map(|(cpu, elapsed)| cpu / elapsed);
    let cpu_utilization_fraction = cpu_core_equivalents
        .zip(available_cores)
        .map(|(used, total)| used / total as f64);
    let rx_bytes = start
        .as_ref()
        .zip(end.as_ref())
        .and_then(|(start, end)| start.network_rx_bytes.zip(end.network_rx_bytes))
        .and_then(|(start, end)| end.checked_sub(start));
    let tx_bytes = start
        .as_ref()
        .zip(end.as_ref())
        .and_then(|(start, end)| start.network_tx_bytes.zip(end.network_tx_bytes))
        .and_then(|(start, end)| end.checked_sub(start));
    let tx_bytes_per_second = tx_bytes
        .map(|bytes| bytes as f64)
        .zip(elapsed_seconds)
        .map(|(bytes, elapsed)| bytes / elapsed);
    serde_json::json!({
        "status": if elapsed_ns.is_some() { "sampled" } else { "incomplete" },
        "start_sample_unix_ms": start.as_ref().map(|sample| sample.sampled_unix_ms),
        "end_sample_unix_ms": end.as_ref().map(|sample| sample.sampled_unix_ms),
        "sample_interval_ns": elapsed_ns,
        "process_cpu_ns": cpu_ns,
        "process_cpu_seconds": cpu_seconds,
        "available_parallelism": available_cores,
        "cpu_core_equivalents": cpu_core_equivalents,
        "cpu_utilization_fraction": cpu_utilization_fraction,
        "network_rx_bytes": rx_bytes,
        "network_tx_bytes": tx_bytes,
        "network_tx_bytes_per_second": tx_bytes_per_second,
        "network_counters_start_unavailable_reason": start.as_ref().and_then(|sample| sample.network_counters_unavailable_reason.as_deref()),
        "network_counters_end_unavailable_reason": end.as_ref().and_then(|sample| sample.network_counters_unavailable_reason.as_deref()),
        "network_advertised_link_speed_start_bps": start.as_ref().and_then(|sample| sample.network_advertised_link_speed_bps),
        "network_advertised_link_speed_end_bps": end.as_ref().and_then(|sample| sample.network_advertised_link_speed_bps),
        "network_interface_speed_start_unavailable_reasons": start.as_ref().map(|sample| &sample.network_interface_speed_unavailable_reasons),
        "network_interface_speed_end_unavailable_reasons": end.as_ref().map(|sample| &sample.network_interface_speed_unavailable_reasons),
        "network_tx_capacity_bps": null,
        "network_tx_capacity_fraction": null,
        "network_tx_capacity_unavailable_reason": "verified_container_or_node_capacity_not_measured",
        "cpu_scope": "linux_process_stat_user_system_ticks",
        "network_scope": "container_non_loopback_interfaces_all_traffic_not_per_flight_attribution",
        "clock_uncertainty_ns": null
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrowcol_flight::{
        BatchAdmission, PutResultEmitter, RuntimeError, RuntimeResult, StreamAbortReason, StreamCompletion,
        StreamContext, StreamLifecycle, StreamSummary,
    };
    use clap::Parser;
    use std::sync::atomic::AtomicBool;

    #[derive(Default)]
    struct SmokeState {
        slow_next: AtomicBool,
        rejections_remaining: AtomicU64,
        ambiguous_remaining: AtomicU64,
        fast_completions: AtomicU64,
        rows: AtomicU64,
    }

    struct SmokeAdmission(Arc<SmokeState>);
    struct SmokeStream {
        state: Arc<SmokeState>,
        slow: bool,
        rows: u64,
    }

    #[async_trait::async_trait]
    impl BatchAdmission for SmokeAdmission {
        async fn begin(
            &self,
            _: StreamContext,
            _: Arc<dyn PutResultEmitter>,
        ) -> RuntimeResult<Box<dyn StreamLifecycle>> {
            if self
                .0
                .rejections_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
                .is_ok()
            {
                return Err(RuntimeError::draining());
            }
            Ok(Box::new(SmokeStream {
                state: Arc::clone(&self.0),
                slow: self.0.slow_next.swap(false, Ordering::SeqCst),
                rows: 0,
            }))
        }
    }

    #[async_trait::async_trait]
    impl StreamLifecycle for SmokeStream {
        async fn push_batch(&mut self, batch: RecordBatch) -> RuntimeResult<()> {
            self.rows += batch.num_rows() as u64;
            Ok(())
        }
        async fn complete(&mut self, _: StreamSummary) -> RuntimeResult<StreamCompletion> {
            if self.slow {
                tokio::time::sleep(Duration::from_millis(350)).await;
            } else {
                self.state.fast_completions.fetch_add(1, Ordering::Relaxed);
            }
            self.state.rows.fetch_add(self.rows, Ordering::Relaxed);
            if self
                .state
                .ambiguous_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
                .is_ok()
            {
                return Err(RuntimeError::deadline_exceeded());
            }
            Ok(StreamCompletion::best_effort())
        }
        async fn abort(self: Box<Self>, _: StreamAbortReason) {}
    }

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        options: Options,
    }

    fn options() -> Result<Options> {
        Cli::try_parse_from(["test", "--workers", "2"])?.options.resolve(100, 1)
    }

    #[test]
    fn only_explicit_not_admitted_status_is_retryable() {
        let marked = anyhow::Error::new(RuntimeError::draining().into_status());
        let ambiguous = anyhow::Error::new(tonic::Status::unavailable("upstream failure"));
        assert!(is_not_admitted(&marked));
        assert!(!is_not_admitted(&ambiguous));
    }

    #[test]
    fn continuous_errors_keep_definitive_statuses_terminal_and_count_only_ambiguous_outcomes() {
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Cancelled,
            tonic::Code::ResourceExhausted,
            tonic::Code::Aborted,
            tonic::Code::Internal,
            tonic::Code::Unknown,
            tonic::Code::DataLoss,
        ] {
            let error = anyhow::Error::new(tonic::Status::new(code, "ambiguous delivery"));
            assert!(is_ambiguous_continuous_error(&error), "{code:?} must be ambiguous");
        }
        for code in [
            tonic::Code::FailedPrecondition,
            tonic::Code::InvalidArgument,
            tonic::Code::Unauthenticated,
            tonic::Code::PermissionDenied,
            tonic::Code::NotFound,
            tonic::Code::Unimplemented,
            tonic::Code::AlreadyExists,
            tonic::Code::OutOfRange,
        ] {
            let error = anyhow::Error::new(tonic::Status::new(code, "definitive rejection"));
            assert!(!is_ambiguous_continuous_error(&error), "{code:?} must remain terminal");
        }
        let transport = anyhow::anyhow!("connection reset without a gRPC status");
        assert!(is_ambiguous_continuous_error(&transport));
    }

    #[test]
    fn stream_retry_backoff_is_jittered_within_exponential_caps() {
        for (attempt, cap) in [(1, 10), (2, 20), (3, 40), (4, 50)] {
            let delay = stream_retry_backoff(10, 50, attempt).as_millis() as u64;
            assert!((cap / 2..=cap).contains(&delay));
        }
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
            vec!["--dictionary-handling", "resend"],
            vec!["--stream-retry-max-attempts", "0"],
            vec![
                "--stream-retry-base-backoff-ms",
                "100",
                "--stream-retry-max-backoff-ms",
                "99",
            ],
        ] {
            let parsed = Cli::try_parse_from(std::iter::once("test").chain(args));
            assert!(parsed.map(|cli| cli.options.resolve(10, 1).is_err()).unwrap_or(true));
        }
        Ok(())
    }

    #[test]
    fn resolution_retains_auto_and_legacy_sources_without_guessing_byte_target_rows() -> Result<()> {
        let requested_options = Cli::try_parse_from(["test", "--workers", "0", "--runtime-threads", "0"])?.options;
        let (resolved, evidence) = requested_options.resolve_with_evidence(123, 7)?;
        assert_eq!(evidence["requested"]["workers"], 0);
        assert_eq!(evidence["requested"]["runtime_threads"], 0);
        assert_eq!(evidence["requested"]["batch_rows"], 0);
        assert_eq!(evidence["requested"]["corpus_rows"], 0);
        assert_eq!(evidence["requested"]["streams_per_worker"], 0);
        assert_eq!(evidence["available_parallelism"], resolved.workers);
        assert_eq!(resolved.workers, resolved.runtime_threads);
        assert_eq!(evidence["legacy"]["rows"], 123);
        assert_eq!(evidence["legacy"]["concurrency"], 7);
        assert_eq!(evidence["resolved"]["batch_rows"], 123);
        assert_eq!(evidence["resolved"]["corpus_rows"], 123);
        assert_eq!(evidence["resolved"]["streams_per_worker"], 7);
        assert_eq!(evidence["corpus_rows_source"], "legacy_rows");

        let mut options = options()?;
        options.corpus_rows = 0;
        options.corpus_bytes = 10000;
        let (resolved, evidence) = options.resolve_with_evidence(123, 7)?;
        assert_eq!(resolved.corpus_rows, 0);
        assert_eq!(evidence["resolved"]["corpus_rows"], 0);
        assert_eq!(
            evidence["corpus_rows_source"],
            "corpus_bytes_estimate_deferred_until_preparation"
        );
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

    #[test]
    fn nullability_width_skew_and_entropy_are_applied_without_changing_customer_identity() -> Result<()> {
        use arrow_schema::Field;
        let mut table = table_named("sqlserver_wait_stats")?;
        table.schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]));
        let mut options = options()?;
        options.string_width = 23;
        options.null_fraction = 1.0;
        let nullable = synthetic_batch(&table, "client", 7, 0, 0, &options)?;
        assert_eq!(nullable.column(1).null_count(), 7);
        assert_eq!(nullable.column(2).null_count(), 0);
        let ids = nullable
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("customer array")?;
        assert!(ids.iter().all(|value| value == Some("client")));
        options.null_fraction = 0.0;
        options.entropy = 0.0;
        options.skew = 1.0;
        let repeated = synthetic_batch(&table, "client", 7, 0, 0, &options)?;
        let strings = repeated
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("string array")?;
        assert!(strings.iter().all(|value| value == Some("aaaaaaaaaaaaaaaaaaaaaaa")));
        let values = repeated
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("value array")?;
        assert!(values.iter().all(|value| value == Some(0)));
        Ok(())
    }

    #[tokio::test]
    async fn terminal_send_failure_is_not_retried_and_row_budget_counts_attempts() -> Result<()> {
        let mut options = options()?;
        options.workers = 1;
        options.streams_per_worker = 1;
        options.max_rows = 10;
        options.request_timeout_ms = 100;
        let table = Arc::new(table_named("sqlserver_wait_stats")?);
        let record = synthetic_batch(&table, "client", 8, 0, 0, &options)?;
        let raw_arrow_bytes = record.get_array_memory_size() as u64;
        let data = encode(&table, "v1", record, &options).await?;
        let encoded_bytes = data.iter().map(|message| message.encoded_len() as u64).sum();
        let channel = options.endpoint("http://127.0.0.1:0")?.connect_lazy();
        let client = Arc::new(PreparedClient {
            corpus: vec![Arc::new(vec![EncodedBatch {
                table,
                data: Arc::new(data),
                rows: 8,
                raw_arrow_bytes,
                encoded_bytes,
            }])],
            channels: vec![vec![channel]],
            signer: None,
            counters: vec![Counters::default()],
            next: AtomicU64::new(0),
            worker_next: vec![AtomicU64::new(0)],
            reserved_rows: AtomicU64::new(0),
            measurement: Mutex::new(None),
            table_count: 1,
            preparation: serde_json::json!({}),
            started: Instant::now(),
            options,
        });
        assert!(client.send_round().await.is_err());
        assert_eq!(client.send_round().await?.completed_rows, 0);
        let metrics = client.metrics("failed");
        assert_eq!(metrics["workers"][0]["attempted_rows"], 8);
        assert_eq!(metrics["workers"][0]["error_rows"], 8);
        assert_eq!(metrics["workers"][0]["completed_rows"], 0);
        Ok(())
    }

    #[tokio::test]
    async fn fast_slot_replenishes_while_another_worker_is_still_waiting() -> Result<()> {
        let sends = AtomicU64::new(0);
        let release = tokio::sync::Semaphore::new(0);
        let deadline = unix_ms()? + 2_000;
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::try_join!(
                replenish_until(deadline, 0, || async {
                    let sent = sends.fetch_add(1, Ordering::SeqCst) + 1;
                    if sent == 10 {
                        release.add_permits(1);
                    }
                    tokio::task::yield_now().await;
                    Ok(sent < 12)
                }),
                replenish_until(deadline, 0, || async {
                    let _permit = release.acquire().await?;
                    assert!(sends.load(Ordering::SeqCst) >= 10);
                    Ok(false)
                }),
            )
        })
        .await
        .context("fast worker was blocked behind the slow worker")??;
        assert_eq!(sends.load(Ordering::SeqCst), 12);
        Ok(())
    }

    #[test]
    fn unavailable_resource_readings_remain_unknown() {
        let start = ResourceSample {
            sampled_unix_ms: 1_000,
            process_cpu_ns: None,
            network_rx_bytes: None,
            network_tx_bytes: None,
            network_counters_unavailable_reason: Some("not_sampled".to_owned()),
            network_advertised_link_speed_bps: None,
            network_interface_speed_unavailable_reasons: vec!["unsupported_interface".to_owned()],
            available_parallelism: None,
        };
        let end = ResourceSample {
            sampled_unix_ms: 2_000,
            ..start.clone()
        };
        let report = resource_report(Some(start), Some(end));
        for field in [
            "process_cpu_ns",
            "network_rx_bytes",
            "network_tx_bytes",
            "network_advertised_link_speed_start_bps",
            "network_tx_capacity_bps",
            "network_tx_capacity_fraction",
            "available_parallelism",
        ] {
            assert!(report[field].is_null(), "{field} must remain unknown");
        }
        assert_eq!(
            report["network_tx_capacity_unavailable_reason"],
            "verified_container_or_node_capacity_not_measured"
        );
        assert_eq!(
            report["network_interface_speed_start_unavailable_reasons"],
            serde_json::json!(["unsupported_interface"])
        );
    }

    #[test]
    fn unsupported_and_disappeared_link_speeds_are_unknown_with_reasons() -> Result<()> {
        assert_eq!(
            read_advertised_link_speed(Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)))?,
            (None, Some("unsupported_interface".to_owned()))
        );
        assert_eq!(
            read_advertised_link_speed(Err(std::io::Error::from(std::io::ErrorKind::NotFound)))?,
            (None, Some("interface_disappeared".to_owned()))
        );
        assert_eq!(
            read_advertised_link_speed(Ok("1000\n".to_owned()))?,
            (Some(1_000_000_000), None)
        );
        assert!(
            read_advertised_link_speed(Err(std::io::Error::new(std::io::ErrorKind::Other, "unexpected",))).is_err()
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_cpu_includes_a_worker_thread_joined_between_samples() -> Result<()> {
        let process_start = process_cpu_ns()?.context("process CPU clock unavailable")?;
        let worker_cpu_ns = std::thread::spawn(|| -> Result<u64> {
            let thread_start = thread_cpu_seconds()?.context("thread CPU clock unavailable")?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(250);
            let mut work = 0_u64;
            while std::time::Instant::now() < deadline {
                work = work.wrapping_add(1);
                std::hint::black_box(work);
            }
            let thread_end = thread_cpu_seconds()?.context("thread CPU clock unavailable")?;
            Ok(((thread_end - thread_start) * 1e9) as u64)
        })
        .join()
        .map_err(|_| anyhow::anyhow!("CPU sampling worker panicked"))??;
        let process_end = process_cpu_ns()?.context("process CPU clock unavailable")?;
        let process_delta = process_end
            .checked_sub(process_start)
            .context("process CPU clock moved backwards")?;
        assert!(
            process_delta >= worker_cpu_ns.saturating_mul(3) / 4,
            "process CPU delta {process_delta} did not include joined worker CPU {worker_cpu_ns}"
        );
        Ok(())
    }

    #[test]
    fn individual_stream_completions_are_attributed_even_when_a_peer_spans_the_window() -> Result<()> {
        let measurement = Measurement::new(10_000, 70_000, 2)?;
        measurement.record_send_latency(10, 9_999_000_000)?;
        measurement.record_send_latency(50, 10_000_000_000)?;
        measurement.record_send_latency(100, 39_999_000_000)?;
        measurement.record_send_latency(200, 40_000_000_000)?;
        measurement.record_send_latency(300, 69_999_000_000)?;
        measurement.record_send_latency(400, 70_000_000_000)?;
        measurement.record_completion(0, 100, 900, 1000, 9_999);
        measurement.record_completion(0, 200, 1800, 2000, 10_000);
        measurement.record_completion(0, 300, 2700, 3000, 69_999);
        measurement.record_completion(1, 400, 3600, 4000, 70_000);
        let snapshot = measurement.snapshot();
        assert_eq!(snapshot["completed_rows"], 500);
        assert_eq!(snapshot["completed_bytes"], 5000);
        assert_eq!(snapshot["completed_raw_arrow_bytes"], 4500);
        assert_eq!(snapshot["worker_completed_rows"], serde_json::json!([500, 0]));
        assert_eq!(snapshot["pilot_window_rows"], serde_json::json!([200, 300]));
        assert_eq!(snapshot["windows"][0]["completed_raw_arrow_bytes"], 1800);
        assert_eq!(snapshot["windows"][0]["send_latency_us"]["samples"], 2);
        assert_eq!(snapshot["windows"][1]["send_latency_us"]["samples"], 2);
        Ok(())
    }

    #[test]
    fn independent_worker_cursors_visit_their_complete_corpus() -> Result<()> {
        let mut options = options()?;
        options.data_mode = DataMode::Independent;
        let table = Arc::new(table_named("sqlserver_wait_stats")?);
        let corpus = || {
            Arc::new(
                (1..=4)
                    .map(|rows| EncodedBatch {
                        table: Arc::clone(&table),
                        data: Arc::new(Vec::new()),
                        rows,
                        raw_arrow_bytes: 0,
                        encoded_bytes: 0,
                    })
                    .collect::<Vec<_>>(),
            )
        };
        let client = PreparedClient {
            corpus: vec![corpus(), corpus()],
            channels: vec![],
            signer: None,
            counters: vec![],
            next: AtomicU64::new(0),
            worker_next: vec![AtomicU64::new(0), AtomicU64::new(0)],
            reserved_rows: AtomicU64::new(0),
            measurement: Mutex::new(None),
            table_count: 1,
            preparation: serde_json::json!({}),
            started: Instant::now(),
            options,
        };
        let mut observed = [Vec::new(), Vec::new()];
        for _ in 0..4 {
            for (worker, rows) in observed.iter_mut().enumerate() {
                rows.push(client.next_batch(worker).rows);
            }
        }
        assert_eq!(observed, [vec![1, 2, 3, 4], vec![1, 2, 3, 4]]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_flight_smoke_keeps_fast_worker_busy_and_drains_late_acknowledgement() -> Result<()> {
        use arrowcol_catalog::{CatalogControlPlane, CatalogStore, MemoryCatalogStore};
        use arrowcol_flight::{AllowAllAuthenticator, ArrowColFlightService, RuntimeLimits};
        use arrowcol_protocol::{AuthContext, AuthMethod, AuthenticatedPrincipal, PrincipalId, PrincipalIssuer};
        let store: Arc<dyn CatalogStore> = Arc::new(MemoryCatalogStore::new());
        let principal = AuthenticatedPrincipal::new(
            PrincipalId::new("smoke")?,
            PrincipalIssuer::new("smoke")?,
            Default::default(),
        )?;
        let auth = AuthContext::new(principal, AuthMethod::WorkloadIdentity, UnixMillis::new(1)?, None)?;
        let state = Arc::new(SmokeState::default());
        let service = ArrowColFlightService::new(
            ImplementationPool::Dataflow,
            Arc::clone(&store),
            Arc::new(AllowAllAuthenticator::new(auth)),
            Arc::new(SmokeAdmission(Arc::clone(&state))),
            RuntimeLimits::default(),
        )?
        .with_ddl_handler(Arc::new(CatalogControlPlane::new(DdlParser::default(), store)))
        .into_tonic_service()
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .send_compressed(tonic::codec::CompressionEncoding::Gzip);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let incoming = stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        let mut options = options()?;
        options.corpus_rows = 8;
        options.batch_rows = 8;
        options.streams_per_worker = 1;
        options.max_rows = 8;
        options.ipc_compression = IpcCompression::Zstd;
        options.grpc_send_compression = GrpcCompression::Gzip;
        options.grpc_accept_compression = GrpcCompression::Gzip;
        let client = PreparedClient::prepare(&endpoint, "smoke", Some("sqlserver_wait_stats"), None, options).await?;
        assert_eq!(client.send_once().await?.completed_rows, 8);
        assert_eq!(state.rows.load(Ordering::Relaxed), 8);
        state.fast_completions.store(0, Ordering::Relaxed);
        state.slow_next.store(true, Ordering::SeqCst);
        state.rejections_remaining.store(2, Ordering::SeqCst);
        state.ambiguous_remaining.store(1, Ordering::SeqCst);
        let scheduled_start = unix_ms()? + 100;
        let measurement_start = scheduled_start + 50;
        let scheduled_client = Arc::clone(&client);
        let run = tokio::spawn(async move {
            scheduled_client
                .run_continuous(scheduled_start, measurement_start, measurement_start + 175)
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            state.rows.load(Ordering::Relaxed),
            8,
            "scheduled client sent before the shared start"
        );
        let report = run.await??;
        assert!(
            state.fast_completions.load(Ordering::Relaxed) >= 3,
            "a slow peer stalled the fast worker"
        );
        assert!(report.completed_rows > 8, "fast worker did not replenish its slot");
        let metrics = client.metrics("completed");
        let retry_counts = |name: &str| -> Result<u64> {
            metrics["workers"]
                .as_array()
                .context("missing worker metrics")?
                .iter()
                .map(|worker| worker[name].as_u64().context("missing retry counter"))
                .sum()
        };
        assert!(retry_counts("rejected_streams")? >= 2);
        assert!(retry_counts("rejected_rows")? >= 16);
        assert!(retry_counts("retried_streams")? >= 2);
        assert!(retry_counts("retried_rows")? >= 16);
        assert_eq!(retry_counts("given_up_streams")?, 0);
        assert_eq!(retry_counts("given_up_rows")?, 0);
        assert!(retry_counts("ambiguous_streams")? >= 1);
        assert!(retry_counts("ambiguous_rows")? >= 8);
        assert!(metrics["measurement_window"]["ambiguous_streams"].as_u64().unwrap_or(0) >= 1);
        let total = metrics["workers"]
            .as_array()
            .context("missing workers")?
            .iter()
            .map(|worker| worker["completed_rows"].as_u64().context("missing completed rows"))
            .sum::<Result<u64>>()?;
        let committed_rows = state.rows.load(Ordering::Relaxed);
        let ambiguous_rows = retry_counts("ambiguous_rows")?;
        assert!(
            total <= committed_rows,
            "acknowledged sends cannot exceed committed rows"
        );
        assert!(
            committed_rows <= total + ambiguous_rows,
            "committed rows must be bounded by ambiguous sends"
        );
        assert!(
            total >= report.completed_rows + 16,
            "late slow acknowledgement must be drained but excluded from measurement"
        );
        drop(client);
        shutdown
            .send(())
            .map_err(|()| anyhow::anyhow!("smoke server stopped early"))?;
        tokio::time::timeout(Duration::from_secs(2), server).await???;
        Ok(())
    }
}
