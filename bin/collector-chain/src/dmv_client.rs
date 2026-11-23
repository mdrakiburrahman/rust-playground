//! Bounded, startup-prepared DMV workload and persistent Flight senders.

use super::*;
use arrow_array::Array;
use arrow_flight::encode::DictionaryHandling;
use arrow_ipc::writer::IpcWriteOptions;
use arrowcol_protocol::NOT_ADMITTED_METADATA_KEY;
use clap::{Args, ValueEnum};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Instant;
use tokio::task::JoinSet;

const MAX_LOGICAL_BATCH_RECORDS: usize = 1_000_000;
const MAX_LOGICAL_MANIFEST_BYTES: usize = 1_048_576;
const MAX_OAT_CLIENT_METRICS_LINE_BYTES: usize = 1_048_576;
const PAYLOAD_HASH_VERSION: u64 = 2;
const PAYLOAD_HASH_DOMAIN: &[u8] = b"arrowcol.payload_corpus.logical.v2\0";
const HASH_FIELD: u8 = 0x11;
const HASH_ROW: u8 = 0x21;

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum DataMode {
    #[default]
    Shared,
    Independent,
}

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum SendIter {
    #[default]
    #[value(name = "clone_vec")]
    CloneVec,
    #[value(name = "arc_cursor")]
    ArcCursor,
}

enum SendFrames {
    Owned(std::vec::IntoIter<FlightData>),
    Shared { frames: Arc<Vec<FlightData>>, index: usize },
}

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IdentityBuilder {
    #[default]
    #[value(name = "formatted_rows")]
    FormattedRows,
    #[value(name = "direct_utf8")]
    DirectUtf8,
}

impl Iterator for SendFrames {
    type Item = FlightData;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Owned(frames) => frames.next(),
            Self::Shared { frames, index } => {
                let frame = frames.get(*index)?.clone();
                *index += 1;
                Some(frame)
            }
        }
    }
}

fn send_frames(frames: &Arc<Vec<FlightData>>, mode: SendIter) -> SendFrames {
    match mode {
        SendIter::CloneVec => SendFrames::Owned(frames.as_ref().clone().into_iter()),
        SendIter::ArcCursor => SendFrames::Shared {
            frames: Arc::clone(frames),
            index: 0,
        },
    }
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

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataProfile {
    #[default]
    Diagnostic,
    Realistic,
}

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StringLengthDistribution {
    #[default]
    Fixed,
    Uniform,
}

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValueVariation {
    #[default]
    Cardinality,
    #[value(name = "per_row")]
    PerRow,
}

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GuidUniqueness {
    #[default]
    Cardinality,
    #[value(name = "per_row")]
    PerRow,
}

/// All settings are resolved and validated before allocation, registration, or connection.
#[derive(Args, Clone, Debug, Serialize)]
pub struct Options {
    #[arg(long, env = "ARROWCOL_CLIENT_PAYLOAD_CUSTOMER_ID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_customer_id: Option<String>,
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
    #[arg(long, env = "ARROWCOL_CLIENT_DATA_PROFILE", value_enum, default_value_t = DataProfile::Diagnostic)]
    pub data_profile: DataProfile,
    #[arg(long, env = "ARROWCOL_CLIENT_SEND_ITER", value_enum, default_value_t = SendIter::CloneVec)]
    pub send_iter: SendIter,
    #[arg(long, env = "ARROWCOL_CLIENT_IDENTITY_BUILDER", value_enum, default_value_t = IdentityBuilder::FormattedRows)]
    pub identity_builder: IdentityBuilder,
    #[arg(long, env = "ARROWCOL_CLIENT_STRING_LENGTH_DISTRIBUTION", value_enum)]
    pub string_length_distribution: Option<StringLengthDistribution>,
    #[arg(long, env = "ARROWCOL_CLIENT_VALUE_VARIATION", value_enum)]
    pub value_variation: Option<ValueVariation>,
    #[arg(long, env = "ARROWCOL_CLIENT_GUID_UNIQUENESS", value_enum)]
    pub guid_uniqueness: Option<GuidUniqueness>,
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
    #[arg(long, env = "ARROWCOL_CLIENT_POD_ORDINAL", default_value_t = 0)]
    pub pod_ordinal: u32,
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
    #[arg(long, env = "ARROWCOL_CLIENT_IPC_ALIGNMENT", default_value_t = 64)]
    pub ipc_alignment: usize,
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
    /// Zero disables the per-connection request limit; otherwise this is the number per period.
    #[arg(long, env = "ARROWCOL_CLIENT_RATE_LIMIT_RPS", default_value_t = 0)]
    pub rate_limit_rps: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_RATE_LIMIT_PERIOD_MS", default_value_t = 1000)]
    pub rate_limit_period_ms: u64,
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
        anyhow::ensure!(
            self.identity_builder != IdentityBuilder::DirectUtf8 || self.data_profile == DataProfile::Realistic,
            "identity-builder=direct_utf8 requires data-profile=realistic"
        );
        anyhow::ensure!(
            matches!(self.ipc_alignment, 8 | 16 | 32 | 64),
            "ipc-alignment must be 8, 16, 32, or 64"
        );
        let requested = serde_json::json!({
            "workers": self.workers, "runtime_threads": self.runtime_threads,
            "batch_rows": self.batch_rows, "corpus_rows": self.corpus_rows,
            "corpus_bytes": self.corpus_bytes, "streams_per_worker": self.streams_per_worker,
            "data_profile": self.data_profile, "string_width": self.string_width,
            "string_length_distribution": self.string_length_distribution,
            "value_variation": self.value_variation, "guid_uniqueness": self.guid_uniqueness,
            "pod_ordinal": self.pod_ordinal,
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
        self.string_length_distribution.get_or_insert(match self.data_profile {
            DataProfile::Diagnostic => StringLengthDistribution::Fixed,
            DataProfile::Realistic => StringLengthDistribution::Uniform,
        });
        self.value_variation.get_or_insert(match self.data_profile {
            DataProfile::Diagnostic => ValueVariation::Cardinality,
            DataProfile::Realistic => ValueVariation::PerRow,
        });
        self.guid_uniqueness.get_or_insert(match self.data_profile {
            DataProfile::Diagnostic => GuidUniqueness::Cardinality,
            DataProfile::Realistic => GuidUniqueness::PerRow,
        });
        if self.data_profile == DataProfile::Realistic && self.string_width == 0 {
            self.string_width = 96;
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
        anyhow::ensure!(
            self.payload_customer_id
                .as_deref()
                .is_none_or(arrowcol_tunables::valid_payload_customer_id),
            "payload-customer-id must be 1..=128 ASCII letters, digits, '.', '_' or '-'"
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
        anyhow::ensure!(
            (1..=86_400_000).contains(&self.rate_limit_period_ms),
            "rate-limit-period-ms must be between 1 and 86400000"
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
                "data_profile": self.data_profile, "string_width": self.string_width,
                "string_length_distribution": self.string_length_distribution,
                "value_variation": self.value_variation, "guid_uniqueness": self.guid_uniqueness,
                "pod_ordinal": self.pod_ordinal,
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
            endpoint = endpoint.rate_limit(self.rate_limit_rps, Duration::from_millis(self.rate_limit_period_ms));
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

#[derive(Clone)]
struct EncodedBatch {
    table: Arc<DmvTable>,
    version: String,
    template: RecordBatch,
    data: Arc<Vec<FlightData>>,
    rows: u64,
    raw_arrow_bytes: u64,
    encoded_bytes: u64,
}

fn null_fraction_corpus_evidence(corpus: &[Arc<Vec<EncodedBatch>>], profile: DataProfile) -> Result<serde_json::Value> {
    #[derive(Default)]
    struct Counts {
        rows: u64,
        fields: usize,
        eligible: std::collections::BTreeSet<String>,
        eligible_rows: u64,
        null_rows: u64,
    }
    let mut tables = std::collections::BTreeMap::<String, Counts>::new();
    for batches in corpus {
        for batch in batches.iter() {
            let counts = tables
                .entry(batch.table.identity.table().as_str().to_owned())
                .or_default();
            counts.fields = batch.template.num_columns();
            counts.rows = counts
                .rows
                .checked_add(batch.rows)
                .context("corpus table row count overflow")?;
            for (field, array) in batch.template.schema().fields().iter().zip(batch.template.columns()) {
                if !field.is_nullable()
                    || field.name() == "customer_id"
                    || (profile == DataProfile::Realistic && field.name() == "endpoint_url")
                {
                    continue;
                }
                counts.eligible.insert(field.name().to_owned());
                counts.eligible_rows = counts
                    .eligible_rows
                    .checked_add(u64::try_from(array.len())?)
                    .context("corpus eligible row count overflow")?;
                counts.null_rows = counts
                    .null_rows
                    .checked_add(u64::try_from(array.null_count())?)
                    .context("corpus null count overflow")?;
            }
        }
    }
    Ok(serde_json::json!({
        "version": 1,
        "tables": tables.into_iter().map(|(table, counts)| serde_json::json!({
            "table": table,
            "schema_fields": counts.fields,
            "rows": counts.rows,
            "null_fraction": {
                "eligible_columns": counts.eligible.len(),
                "eligible_rows": counts.eligible_rows,
                "null_rows": counts.null_rows,
            },
        })).collect::<Vec<_>>(),
    }))
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ManifestCohort {
    Unscheduled,
    Warmup,
    Measurement,
    PostWindow,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ManifestOutcome {
    Pending,
    Acknowledged,
    NotAdmitted,
    Ambiguous,
    Failed,
}

#[derive(Clone, Debug)]
struct LogicalBatchRecord {
    table: String,
    worker: u16,
    sequence: u64,
    row_count: u32,
    cohort: ManifestCohort,
    attempts: u16,
    outcome: ManifestOutcome,
    first_send_unix_ns: Option<u64>,
    last_outcome_unix_ms: Option<u64>,
}

#[derive(Serialize)]
struct LogicalBatchRange {
    table: String,
    worker: u16,
    first_sequence: u64,
    last_sequence: u64,
    batch_count: u64,
    rows_per_batch: u32,
    cohort: ManifestCohort,
    attempts: u16,
    outcome: ManifestOutcome,
    first_send_unix_ns: Option<u64>,
    last_outcome_unix_ms: Option<u64>,
}

struct LogicalBatch {
    batch: EncodedBatch,
    manifest_index: Option<usize>,
    _live_batch_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

struct ContinuousAdmission<P, E, W> {
    acquire_permit: P,
    window_end: E,
    window_open: W,
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
    manifest: Option<(&'a PreparedClient, usize)>,
    send_started: bool,
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
            "completed_flight_data_bytes_configured_ipc_compression": self.bytes.load(Ordering::Relaxed),
            "completed_flight_data_bytes_uncompressed": self.raw_arrow_bytes.load(Ordering::Relaxed),
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
                    "completed_flight_data_bytes_uncompressed": self.raw_window_bytes[index].load(Ordering::Relaxed),
                    "completed_flight_data_bytes_configured_ipc_compression": self.wire_window_bytes[index].load(Ordering::Relaxed),
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
            if self.send_started
                && let Some((client, index)) = self.manifest
            {
                let mut records = client
                    .logical_batches
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(record) = records.get_mut(index) {
                    record.outcome = ManifestOutcome::Ambiguous;
                }
            }
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
    live_batch_slots: Arc<tokio::sync::Semaphore>,
    signer: Option<EdgeRequestSigner>,
    counters: Vec<Counters>,
    next: AtomicU64,
    worker_next: Vec<AtomicU64>,
    worker_batch_sequence: Vec<AtomicU64>,
    reserved_rows: AtomicU64,
    measurement: Mutex<Option<Arc<Measurement>>>,
    run_id: uuid::Uuid,
    incarnation_id: uuid::Uuid,
    logical_batches: Mutex<Vec<LogicalBatchRecord>>,
    manifest_capacity_reached: AtomicBool,
    terminal_failure_reason: Mutex<Option<&'static str>>,
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
        let memory = memory_budget(&tables, client_id, rows, row_bytes, copies, &options)?;
        for table in &tables {
            for field in table.schema.fields() {
                if field.data_type() == &DataType::Utf8 {
                    let width = if field.name() == "customer_id" {
                        options.payload_customer_id.as_deref().unwrap_or(client_id).len()
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
        let mut raw_arrow_bytes_total = 0_u64;
        let mut encoded_bytes = 0_u64;
        let mut generation_seconds = 0.0;
        let mut encoding_seconds = 0.0;
        let mut generation_cpu_seconds = cpu_started.map(|_| 0.0);
        let mut encoding_cpu_seconds = cpu_started.map(|_| 0.0);
        let mut payload_hasher = new_payload_hasher(options.data_profile, copies, table_count)?;
        for copy in 0..copies {
            let mut batches = Vec::new();
            for (table, version) in tables.iter().zip(&versions) {
                for offset in (0..rows).step_by(options.batch_rows) {
                    let length = options.batch_rows.min(rows - offset);
                    let generation_start = Instant::now();
                    let cpu_start = thread_cpu_seconds()?;
                    let batch = synthetic_batch(table, client_id, length, offset, copy, &options)?;
                    hash_payload_batch(
                        &mut payload_hasher,
                        table,
                        &batch,
                        copy,
                        offset,
                        rows,
                        options.data_profile,
                    )?;
                    generation_seconds += generation_start.elapsed().as_secs_f64();
                    if let Some((total, (end, start))) = generation_cpu_seconds
                        .as_mut()
                        .zip(thread_cpu_seconds()?.zip(cpu_start))
                    {
                        *total += (end - start).max(0.0);
                    }
                    let encoding_start = Instant::now();
                    let cpu_start = thread_cpu_seconds()?;
                    let (data, raw_arrow_bytes, wire_bytes) =
                        encode_with_sizes(table, version, batch.clone(), &options).await?;
                    encoding_seconds += encoding_start.elapsed().as_secs_f64();
                    if let Some((total, (end, start))) =
                        encoding_cpu_seconds.as_mut().zip(thread_cpu_seconds()?.zip(cpu_start))
                    {
                        *total += (end - start).max(0.0);
                    }
                    raw_arrow_bytes_total = raw_arrow_bytes_total
                        .checked_add(raw_arrow_bytes)
                        .context("raw Arrow IPC corpus size overflow")?;
                    encoded_bytes = encoded_bytes
                        .checked_add(wire_bytes)
                        .context("encoded corpus size overflow")?;
                    anyhow::ensure!(
                        encoded_bytes <= memory.corpus_bytes,
                        "encoded corpus exceeded its reserved corpus-memory budget"
                    );
                    batches.push(EncodedBatch {
                        table: Arc::clone(table),
                        version: version.clone(),
                        template: batch,
                        data: Arc::new(data),
                        rows: length as u64,
                        raw_arrow_bytes,
                        encoded_bytes: wire_bytes,
                    });
                }
            }
            corpus.push(Arc::new(batches));
        }
        let generator_applicability = null_fraction_corpus_evidence(&corpus, options.data_profile)?;
        let mut channels = Vec::new();
        for _ in 0..options.workers {
            let mut worker = Vec::new();
            for _ in 0..options.connections_per_worker {
                worker.push(options.connect(endpoint).await?);
            }
            channels.push(worker);
        }
        let mut preparation = serde_json::json!({
            "data_profile": options.data_profile,
            "generator_applicability": generator_applicability,
            "payload_corpus_hash_version": PAYLOAD_HASH_VERSION,
            "payload_corpus_hash_scope": payload_hash_scope(options.data_profile),
            "corpus_rows": rows as u64 * table_count as u64 * copies as u64,
            "corpus_uncompressed_flight_data_bytes": raw_arrow_bytes_total,
            "corpus_configured_ipc_compression_flight_data_bytes": encoded_bytes,
            "corpus_uncompressed_ipc_bytes": raw_arrow_bytes_total,
            "corpus_encoded_bytes": encoded_bytes,
            "retained_encoded_bytes": encoded_bytes,
            "memory_reservation_bytes": memory.total_bytes,
            "corpus_memory_reservation_bytes": memory.corpus_bytes,
            "live_identity_batch_bytes": memory.per_live_batch_bytes,
            "live_identity_batch_reservation_bytes": memory.live_batch_bytes,
            "configured_live_slots": memory.configured_live_slots,
            "admitted_live_slots": memory.admitted_live_slots,
            "max_memory_bytes": options.max_memory_bytes,
            "generation_wall_seconds": generation_seconds,
            "generation_cpu_seconds": generation_cpu_seconds,
            "encoding_cpu_seconds": encoding_cpu_seconds,
            "preparation_cpu_seconds": thread_cpu_seconds()?.zip(cpu_started).map(|(end, start)| (end - start).max(0.0)),
            "generation_cpu_scope": "caller_thread_linux_schedstat_null_on_other_platforms",
            "encoding_wall_seconds": encoding_seconds,
            "preparation_wall_seconds": started.elapsed().as_secs_f64(),
            "generator_rows_per_second": rows as f64 * table_count as f64 * copies as f64 / generation_seconds.max(f64::MIN_POSITIVE),
            "steady_state_generation": options.data_profile == DataProfile::Realistic,
        });
        preparation["payload_corpus_sha256"] = serde_json::json!(finish_payload_hasher(payload_hasher));
        Ok(Arc::new(Self {
            counters: (0..options.workers).map(|_| Counters::default()).collect(),
            worker_next: (0..options.workers).map(|_| AtomicU64::new(0)).collect(),
            worker_batch_sequence: (0..options.workers).map(|_| AtomicU64::new(0)).collect(),
            options,
            corpus,
            channels,
            live_batch_slots: Arc::new(tokio::sync::Semaphore::new(memory.admitted_live_slots)),
            signer,
            next: AtomicU64::new(0),
            reserved_rows: AtomicU64::new(0),
            measurement: Mutex::new(None),
            run_id: uuid::Uuid::new_v4(),
            incarnation_id: uuid::Uuid::new_v4(),
            logical_batches: Mutex::new(Vec::new()),
            manifest_capacity_reached: AtomicBool::new(false),
            terminal_failure_reason: Mutex::new(None),
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

    fn next_logical_batch_sequence(&self, worker: usize) -> Result<u64> {
        let sequence = self
            .worker_batch_sequence
            .get(worker)
            .context("logical batch worker id is out of range")?
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| value.checked_add(1))
            .map_err(|_| anyhow::anyhow!("logical batch sequence overflow for worker {worker}"))?;
        Ok(sequence)
    }

    fn reserve_manifest_record(&self, batch: &EncodedBatch, worker: usize, sequence: u64) -> Result<usize> {
        let mut records = self
            .logical_batches
            .lock()
            .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?;
        if records.len() >= MAX_LOGICAL_BATCH_RECORDS {
            self.manifest_capacity_reached.store(true, Ordering::Relaxed);
            anyhow::bail!(
                "logical batch manifest reached its {MAX_LOGICAL_BATCH_RECORDS}-record cap before starting another send"
            );
        }
        records
            .try_reserve(1)
            .context("reserve logical batch manifest record")?;
        let index = records.len();
        records.push(LogicalBatchRecord {
            table: batch.table.identity.table().as_str().to_owned(),
            worker: u16::try_from(worker).context("logical batch worker id exceeds u16")?,
            sequence,
            row_count: u32::try_from(batch.rows).context("logical batch row count exceeds u32")?,
            cohort: ManifestCohort::Unscheduled,
            attempts: 0,
            outcome: ManifestOutcome::Pending,
            first_send_unix_ns: None,
            last_outcome_unix_ms: None,
        });
        Ok(index)
    }

    fn record_manifest_attempt(
        &self,
        index: usize,
        send_unix_ns: u64,
        measurement: Option<&Measurement>,
    ) -> Result<()> {
        let mut records = self
            .logical_batches
            .lock()
            .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?;
        let record = records
            .get_mut(index)
            .context("logical batch manifest index is out of range")?;
        if record.attempts == 0 {
            record.first_send_unix_ns = Some(send_unix_ns);
            record.cohort = measurement.map_or(ManifestCohort::Unscheduled, |window| {
                let send_unix_ms = send_unix_ns / 1_000_000;
                if send_unix_ms < window.start_unix_ms {
                    ManifestCohort::Warmup
                } else if send_unix_ms < window.end_unix_ms {
                    ManifestCohort::Measurement
                } else {
                    ManifestCohort::PostWindow
                }
            });
        }
        record.attempts = record
            .attempts
            .checked_add(1)
            .context("logical batch attempt count overflow")?;
        Ok(())
    }

    fn record_manifest_outcome(
        &self,
        index: usize,
        outcome: ManifestOutcome,
        observed_unix_ms: Option<u64>,
    ) -> Result<()> {
        let mut records = self
            .logical_batches
            .lock()
            .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?;
        let record = records
            .get_mut(index)
            .context("logical batch manifest index is out of range")?;
        record.outcome = outcome;
        if observed_unix_ms.is_some() {
            record.last_outcome_unix_ms = observed_unix_ms;
        }
        Ok(())
    }

    fn remember_first_terminal_failure(&self, reason: &'static str) -> Result<&'static str> {
        let mut first = self
            .terminal_failure_reason
            .lock()
            .map_err(|_| anyhow::anyhow!("terminal failure reason state poisoned"))?;
        Ok(*first.get_or_insert(reason))
    }

    fn first_terminal_failure(&self) -> Result<Option<&'static str>> {
        self.terminal_failure_reason
            .lock()
            .map(|first| *first)
            .map_err(|_| anyhow::anyhow!("terminal failure reason state poisoned"))
    }

    fn logical_manifest(&self, phase: &str) -> Result<serde_json::Value> {
        let records = self
            .logical_batches
            .lock()
            .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?
            .clone();
        let mut ordered = records.clone();
        ordered.sort_by_key(|record| (record.worker, record.sequence));
        let mut ranges: Vec<LogicalBatchRange> = Vec::new();
        for record in ordered {
            let merge = ranges.last().is_some_and(|range| {
                range.table == record.table
                    && range.worker == record.worker
                    && range.last_sequence.checked_add(1) == Some(record.sequence)
                    && range.rows_per_batch == record.row_count
                    && range.cohort == record.cohort
                    && range.attempts == record.attempts
                    && range.outcome == record.outcome
            });
            if merge {
                let range = ranges.last_mut().context("manifest range disappeared")?;
                range.last_sequence = record.sequence;
                range.batch_count = range
                    .batch_count
                    .checked_add(1)
                    .context("logical batch range count overflow")?;
                range.first_send_unix_ns = match (range.first_send_unix_ns, record.first_send_unix_ns) {
                    (Some(left), Some(right)) => Some(left.min(right)),
                    (left, right) => left.or(right),
                };
                range.last_outcome_unix_ms = match (range.last_outcome_unix_ms, record.last_outcome_unix_ms) {
                    (Some(left), Some(right)) => Some(left.max(right)),
                    (left, right) => left.or(right),
                };
            } else {
                ranges.push(LogicalBatchRange {
                    table: record.table,
                    worker: record.worker,
                    first_sequence: record.sequence,
                    last_sequence: record.sequence,
                    batch_count: 1,
                    rows_per_batch: record.row_count,
                    cohort: record.cohort,
                    attempts: record.attempts,
                    outcome: record.outcome,
                    first_send_unix_ns: record.first_send_unix_ns,
                    last_outcome_unix_ms: record.last_outcome_unix_ms,
                });
            }
        }
        let counts = records.iter().fold([0_u64; 5], |mut counts, record| {
            let index = match record.outcome {
                ManifestOutcome::Pending => 0,
                ManifestOutcome::Acknowledged => 1,
                ManifestOutcome::NotAdmitted => 2,
                ManifestOutcome::Ambiguous => 3,
                ManifestOutcome::Failed => 4,
            };
            counts[index] += 1;
            counts
        });
        let measurement_bounds = self
            .measurement
            .lock()
            .map_err(|_| anyhow::anyhow!("measurement state poisoned"))?
            .as_ref()
            .map(|window| {
                serde_json::json!({
                    "start_unix_ms_inclusive": window.start_unix_ms,
                    "end_unix_ms_exclusive": window.end_unix_ms,
                })
            });
        let complete =
            phase == "completed" && !self.manifest_capacity_reached.load(Ordering::Relaxed) && counts[0] == 0;
        let mut manifest = if self.options.data_profile == DataProfile::Diagnostic {
            serde_json::json!({
                "schema": "arrowcol.logical_batch_manifest.v1",
                "data_profile": self.options.data_profile,
                "identity_status": "diagnostic_only",
                "eligible_for_identity_reconciliation": false,
                "complete": false,
                "truncated": false,
                "reason": "diagnostic profile replays identity-free corpus frames",
                "measurement_bounds": measurement_bounds,
                "records": [],
            })
        } else {
            serde_json::json!({
                "schema": "arrowcol.logical_batch_manifest.v1",
                "data_profile": self.options.data_profile,
                "version": 1,
                "identity_status": "identity_bearing",
                "eligible_for_identity_reconciliation": true,
                "complete": complete,
                "truncated": false,
                "record_count": records.len(),
                "record_cap": MAX_LOGICAL_BATCH_RECORDS,
                "record_capacity_reached": self.manifest_capacity_reached.load(Ordering::Relaxed),
                "measurement_bounds": measurement_bounds,
                "cohort_rule": "first_send_unix_ms < start => warmup; start <= first_send_unix_ms < end => measurement; otherwise post_window; bounds are [start,end)",
                "identity_column": "endpoint_url",
                "identity_format": "https://x.invalid/v1/{run_uuid_simple32}/{incarnation_uuid_simple32}/{worker_decimal}/{logical_batch_sequence_base36}/{row_ordinal_decimal}",
                "run_uuid": self.run_id.simple().to_string(),
                "run_uuid_source": "client_generated",
                "oat_run_uuid": null,
                "incarnation_uuid": self.incarnation_id.simple().to_string(),
                "incarnation_uuid_source": "client_generated",
                "prepared_logical_batches": records.len(),
                "attempted_logical_batches": records.iter().filter(|record| record.attempts > 0).count(),
                "acknowledged_logical_batches": counts[1],
                "not_admitted_logical_batches": counts[2],
                "ambiguous_logical_batches": counts[3],
                "failed_logical_batches": counts[4],
                "pending_logical_batches": counts[0],
                "retry_attempts": records.iter().map(|record| u64::from(record.attempts.saturating_sub(1))).sum::<u64>(),
                "ranges": ranges,
            })
        };
        if let Some(hash) = self.preparation.get("payload_corpus_sha256") {
            manifest["payload_corpus_sha256"] = hash.clone();
            manifest["payload_corpus_hash_version"] = serde_json::json!(PAYLOAD_HASH_VERSION);
            manifest["payload_corpus_hash_scope"] = serde_json::json!(payload_hash_scope(self.options.data_profile));
        }
        if self.manifest_capacity_reached.load(Ordering::Relaxed) {
            let reason = self.remember_first_terminal_failure("record_capacity_reached")?;
            manifest["terminal_failure"] = serde_json::json!(reason);
        }
        let serialized_bytes = serde_json::to_vec(&manifest)?.len();
        if serialized_bytes > MAX_LOGICAL_MANIFEST_BYTES {
            self.manifest_capacity_reached.store(true, Ordering::Relaxed);
            let reason = self.remember_first_terminal_failure("serialized_manifest_exceeds_limit")?;
            return Ok(serde_json::json!({
                "schema": "arrowcol.logical_batch_manifest.v1",
                "version": 1,
                "identity_status": "unavailable",
                "eligible_for_identity_reconciliation": false,
                "complete": false,
                "truncated": true,
                "terminal_failure": reason,
                "serialized_bytes": serialized_bytes,
                "serialized_byte_cap": MAX_LOGICAL_MANIFEST_BYTES,
                "record_count": records.len(),
                "record_cap": MAX_LOGICAL_BATCH_RECORDS,
                "measurement_bounds": measurement_bounds,
                "run_uuid": self.run_id.simple().to_string(),
                "run_uuid_source": "client_generated",
                "oat_run_uuid": null,
                "incarnation_uuid": self.incarnation_id.simple().to_string(),
                "incarnation_uuid_source": "client_generated",
            }));
        }
        Ok(manifest)
    }

    /// Builds completed-phase metrics and rejects an incomplete terminal manifest.
    ///
    /// # Errors
    ///
    /// Propagates metrics serialization errors or an incomplete-manifest error.
    pub fn completed_terminal_metrics(&self) -> Result<serde_json::Value> {
        let metrics = self.metrics("completed")?;
        self.ensure_terminal_manifest_complete()?;
        Ok(metrics)
    }

    /// Builds failed-phase metrics while retaining any earlier terminal failure reason.
    ///
    /// # Errors
    ///
    /// Propagates metrics serialization errors.
    pub fn failed_terminal_metrics(&self) -> Result<serde_json::Value> {
        self.metrics("failed")
    }

    /// Rejects terminal success when the logical batch manifest is incomplete or over capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when the evidence exceeded a cap or contains a pending logical batch.
    pub fn ensure_terminal_manifest_complete(&self) -> Result<()> {
        anyhow::ensure!(
            !self.manifest_capacity_reached.load(Ordering::Relaxed),
            "logical batch manifest exceeded a hard capacity; refusing successful completion"
        );
        let records = self
            .logical_batches
            .lock()
            .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?;
        anyhow::ensure!(
            records.iter().all(|record| record.outcome != ManifestOutcome::Pending),
            "logical batch manifest contains pending outcomes; refusing successful completion"
        );
        Ok(())
    }

    async fn prepare_logical_batch(&self, worker: usize, template: &EncodedBatch) -> Result<LogicalBatch> {
        let live_batch_permit = if self.options.data_profile == DataProfile::Diagnostic {
            None
        } else {
            Some(
                Arc::clone(&self.live_batch_slots)
                    .acquire_owned()
                    .await
                    .context("acquire realistic live-batch memory reservation")?,
            )
        };
        self.prepare_logical_batch_with_permit(worker, template, live_batch_permit)
            .await
    }

    async fn prepare_continuous_logical_batch<P, E, W>(
        &self,
        worker: usize,
        template: &EncodedBatch,
        admission: ContinuousAdmission<P, E, W>,
    ) -> Result<Option<LogicalBatch>>
    where
        P: std::future::Future<Output = Result<tokio::sync::OwnedSemaphorePermit>>,
        E: std::future::Future<Output = Result<()>>,
        W: FnMut() -> Result<bool>,
    {
        let ContinuousAdmission {
            acquire_permit,
            window_end,
            mut window_open,
        } = admission;
        if !window_open()? {
            return Ok(None);
        }
        let live_batch_permit = if self.options.data_profile == DataProfile::Diagnostic {
            None
        } else {
            let Some(permit) = acquire_owned_permit_before_end(acquire_permit, window_end).await? else {
                return Ok(None);
            };
            if !window_open()? {
                return Ok(None);
            }
            Some(permit)
        };
        Ok(Some(
            self.prepare_logical_batch_with_permit(worker, template, live_batch_permit)
                .await?,
        ))
    }

    async fn prepare_logical_batch_with_permit(
        &self,
        worker: usize,
        template: &EncodedBatch,
        live_batch_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<LogicalBatch> {
        if self.options.data_profile == DataProfile::Diagnostic {
            return Ok(LogicalBatch {
                batch: template.clone(),
                manifest_index: None,
                _live_batch_permit: None,
            });
        }
        let live_batch_permit = live_batch_permit.context("realistic logical batch requires a memory reservation")?;
        let sequence = self.next_logical_batch_sequence(worker)?;
        let batch = batch_with_logical_identity(
            &template.template,
            self.run_id,
            self.incarnation_id,
            worker,
            sequence,
            self.options.identity_builder,
        )?;
        let (data, raw_arrow_bytes, encoded_bytes) =
            encode_with_sizes(&template.table, &template.version, batch.clone(), &self.options).await?;
        anyhow::ensure!(
            data.iter()
                .all(|message| message.encoded_len() <= self.options.max_message_bytes),
            "identity-bearing Arrow IPC message exceeded max-message-bytes"
        );
        let prepared = EncodedBatch {
            table: Arc::clone(&template.table),
            version: template.version.clone(),
            template: batch,
            data: Arc::new(data),
            rows: template.rows,
            raw_arrow_bytes,
            encoded_bytes,
        };
        let manifest_index = self.reserve_manifest_record(&prepared, worker, sequence)?;
        Ok(LogicalBatch {
            batch: prepared,
            manifest_index: Some(manifest_index),
            _live_batch_permit: Some(live_batch_permit),
        })
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
            println!("OAT_CLIENT_METRICS {}", self.metrics("measurement_start")?);
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
                let stream_limit = this.options.streams_per_worker;
                let streams = stream::iter(indices.into_iter().enumerate()).map(|(stream_index, index)| {
                    let client = Arc::clone(&this);
                    let batch = &corpus[index];
                    let channel = this.channels[worker][stream_index % this.channels[worker].len()].clone();
                    async move { client.send_fresh_one(worker, channel, batch, None).await }
                });
                streams.buffer_unordered(stream_limit).try_collect::<Vec<_>>().await
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

    async fn send_fresh_one(
        &self,
        worker: usize,
        channel: Channel,
        template: &EncodedBatch,
        measurement: Option<&Measurement>,
    ) -> Result<Option<(DeliveryGuarantee, u64)>> {
        if measurement.is_none()
            && self.options.max_rows > 0
            && self
                .reserved_rows
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |rows| {
                    rows.checked_add(template.rows)
                        .filter(|next| *next <= self.options.max_rows)
                })
                .is_err()
        {
            return Ok(None);
        }
        let logical = self.prepare_logical_batch(worker, template).await?;
        self.send_encoded_one(worker, channel, &logical, measurement).await
    }

    async fn send_encoded_one(
        &self,
        worker: usize,
        channel: Channel,
        logical: &LogicalBatch,
        measurement: Option<&Measurement>,
    ) -> Result<Option<(DeliveryGuarantee, u64)>> {
        let batch = &logical.batch;
        let counters = &self.counters[worker];
        counters.attempted_rows.fetch_add(batch.rows, Ordering::Relaxed);
        counters
            .attempted_bytes
            .fetch_add(batch.encoded_bytes, Ordering::Relaxed);
        let mut attempt = Attempt {
            counters,
            batch,
            manifest: logical.manifest_index.map(|index| (self, index)),
            send_started: false,
            started: Instant::now(),
            finished: false,
        };
        let observation = SendObservation::fresh()?;
        let mut request = Request::new(stream::iter(send_frames(&batch.data, self.options.send_iter)));
        let timeout = Duration::from_millis(self.options.request_timeout_ms);
        let result = tokio::time::timeout(timeout, async {
            request_metadata(&mut request, &batch.table, self.signer.as_ref(), Some(&observation))?;
            request.set_timeout(timeout);
            if let Some(index) = logical.manifest_index {
                self.record_manifest_attempt(index, observation.send_unix_ns, measurement)?;
                attempt.send_started = true;
            }
            completed_admission(self.options.client(channel).do_put(request).await?.into_inner()).await
        })
        .await
        .context("DMV upload timeout")
        .and_then(std::convert::identity);
        if let Some(index) = logical.manifest_index {
            let outcome = match &result {
                Ok(_) => ManifestOutcome::Acknowledged,
                Err(error) if is_not_admitted(error) => ManifestOutcome::NotAdmitted,
                Err(error) if is_ambiguous_continuous_error(error) => ManifestOutcome::Ambiguous,
                Err(_) => ManifestOutcome::Failed,
            };
            self.record_manifest_outcome(index, outcome, Some(unix_ms()?))?;
        }
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
        let end_unix_ms = measurement.end_unix_ms;
        let remaining_ms = end_unix_ms.saturating_sub(unix_ms()?);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(remaining_ms);
        let live_batch_slots = Arc::clone(&self.live_batch_slots);
        let acquire_permit = async move {
            live_batch_slots
                .acquire_owned()
                .await
                .context("acquire realistic live-batch memory reservation")
        };
        let window_end = async move {
            tokio::time::sleep_until(deadline).await;
            Ok::<(), anyhow::Error>(())
        };
        self.send_continuous_one_with_admission(
            worker,
            channel,
            batch,
            measurement,
            ContinuousAdmission {
                acquire_permit,
                window_end,
                window_open: || Ok::<bool, anyhow::Error>(unix_ms()? < end_unix_ms),
            },
        )
        .await
    }

    async fn send_continuous_one_with_admission<P, E, W>(
        &self,
        worker: usize,
        channel: Channel,
        batch: &EncodedBatch,
        measurement: &Measurement,
        admission: ContinuousAdmission<P, E, W>,
    ) -> Result<bool>
    where
        P: std::future::Future<Output = Result<tokio::sync::OwnedSemaphorePermit>>,
        E: std::future::Future<Output = Result<()>>,
        W: FnMut() -> Result<bool>,
    {
        let Some(logical) = self.prepare_continuous_logical_batch(worker, batch, admission).await? else {
            return Ok(false);
        };
        for attempt in 1..=self.options.stream_retry_max_attempts {
            match self
                .send_encoded_one(worker, channel.clone(), &logical, Some(measurement))
                .await
            {
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

    /// Returns a versioned metrics snapshot for one lifecycle phase.
    ///
    /// # Errors
    ///
    /// Returns an error if telemetry state is poisoned or terminal receipt sizing overflows.
    #[must_use = "metrics snapshots must be emitted to preserve OAT evidence"]
    pub fn metrics(&self, phase: &str) -> Result<serde_json::Value> {
        let measurement_window = {
            let measurement = self.measurement.lock().unwrap_or_else(|error| {
                eprintln!("measurement state was poisoned; retaining recorded counters");
                error.into_inner()
            });
            measurement.as_ref().map(|value| value.snapshot())
        };
        let mut metrics = serde_json::json!({
            "version": 1, "phase": phase, "scope": "process_cumulative",
            "completion_semantics": "validated_admission_response_not_delta_commit",
            "send_latency_scope": "request_metadata_protobuf_grpc_compression_network_and_acknowledgement_excluding_interval",
            "bytes_semantics": {
                "uncompressed_flight_data_bytes": "sum_of_FlightData_protobuf_encoded_lengths_before_Arrow_IPC_compression",
                "configured_ipc_compression_flight_data_bytes": "sum_of_FlightData_protobuf_encoded_lengths_after_configured_Arrow_IPC_compression",
                "legacy_completed_raw_arrow_bytes": "deprecated_alias_for_uncompressed_flight_data_bytes",
                "legacy_completed_wire_bytes": "alias_for_configured_ipc_compression_flight_data_bytes",
                "exclusions": "these_counts_are_not_logical_Arrow_buffer_bytes_and_exclude_gRPC_and_HTTP/2_framing"
            },
            "send_observation": {
                "id_header": CLIENT_SEND_OBSERVATION_ID_HEADER,
                "timestamp_header": CLIENT_SEND_UNIX_NS_HEADER,
                "timestamp_unit": "unix_nanoseconds",
                "identity_scope": "unique_per_do_put_attempt_including_retries",
                "clock_uncertainty_ns": null
            },
            "elapsed_seconds": self.started.elapsed().as_secs_f64(),
            "preparation": self.preparation,
            "measurement_window": measurement_window,
            "workers": self.counters.iter().enumerate().map(|(id, counters)| {
                let mut value = counters.snapshot(self.started.elapsed().as_secs_f64());
                value["worker"] = serde_json::json!(id);
                value
            }).collect::<Vec<_>>(),
        });
        if matches!(phase, "completed" | "failed") {
            metrics["logical_batch_manifest"] = self.logical_manifest(phase)?;
            if self.options.data_profile != DataProfile::Diagnostic
                && (!metrics["logical_batch_manifest"]["complete"].as_bool().unwrap_or(false)
                    || self.manifest_capacity_reached.load(Ordering::Relaxed))
            {
                metrics["phase"] = serde_json::json!("failed");
            }
            if let Some(reason) = self.first_terminal_failure()? {
                metrics["terminal_failure"] = serde_json::json!(reason);
            }
            let receipt_bytes = oat_client_metrics_line_bytes(&metrics)?;
            if receipt_bytes > MAX_OAT_CLIENT_METRICS_LINE_BYTES {
                self.manifest_capacity_reached.store(true, Ordering::Relaxed);
                let first_terminal_failure =
                    self.remember_first_terminal_failure("serialized_terminal_receipt_exceeds_limit")?;
                metrics["terminal_failure"] = serde_json::json!(first_terminal_failure);
                metrics["phase"] = serde_json::json!("failed");
                metrics["logical_batch_manifest"] = serde_json::json!({
                    "schema": "arrowcol.logical_batch_manifest.v1",
                    "version": 1,
                    "identity_status": "unavailable",
                    "eligible_for_identity_reconciliation": false,
                    "complete": false,
                    "truncated": true,
                    "terminal_failure": first_terminal_failure,
                    "serialized_bytes": receipt_bytes,
                    "serialized_byte_cap": MAX_OAT_CLIENT_METRICS_LINE_BYTES,
                });
                let receipt_bytes_after_manifest = oat_client_metrics_line_bytes(&metrics)?;
                if receipt_bytes_after_manifest > MAX_OAT_CLIENT_METRICS_LINE_BYTES {
                    metrics = serde_json::json!({
                        "version": 1,
                        "phase": "failed",
                        "scope": "process_cumulative",
                        "terminal_failure": first_terminal_failure,
                        "serialized_bytes": receipt_bytes_after_manifest,
                        "serialized_byte_cap": MAX_OAT_CLIENT_METRICS_LINE_BYTES,
                    });
                    anyhow::ensure!(
                        oat_client_metrics_line_bytes(&metrics)? <= MAX_OAT_CLIENT_METRICS_LINE_BYTES,
                        "minimal terminal OAT metrics receipt exceeds its line-size cap"
                    );
                }
            }
        }
        Ok(metrics)
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

async fn acquire_owned_permit_before_end<P, E>(
    acquire_permit: P,
    window_end: E,
) -> Result<Option<tokio::sync::OwnedSemaphorePermit>>
where
    P: std::future::Future<Output = Result<tokio::sync::OwnedSemaphorePermit>>,
    E: std::future::Future<Output = Result<()>>,
{
    tokio::select! {
        permit = acquire_permit => Ok(Some(permit?)),
        result = window_end => {
            result?;
            Ok(None)
        }
    }
}

fn estimated_row_bytes(table: &DmvTable, client_id: &str, options: &Options) -> u64 {
    table
        .schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Utf8 if field.name() == "customer_id" => {
                options.payload_customer_id.as_deref().unwrap_or(client_id).len() as u64 + 5
            }
            DataType::Utf8 => {
                let category_width = if options.data_profile == DataProfile::Realistic {
                    realistic_category_max_width(field.name())
                } else {
                    None
                };
                if let Some(width) = category_width {
                    width as u64 + 5
                } else {
                    (if options.string_width == 0 {
                        field.name().len() + 32
                    } else {
                        options.string_width
                    }) as u64
                        + 5
                }
            }
            _ => 9,
        })
        .sum::<u64>()
        .max(1)
}

#[derive(Clone, Copy, Debug)]
struct MemoryBudget {
    corpus_bytes: u64,
    per_live_batch_bytes: u64,
    live_batch_bytes: u64,
    configured_live_slots: usize,
    admitted_live_slots: usize,
    total_bytes: u64,
}

fn memory_budget(
    tables: &[DmvTable],
    client_id: &str,
    rows: usize,
    row_bytes: u64,
    copies: usize,
    options: &Options,
) -> Result<MemoryBudget> {
    anyhow::ensure!(
        rows > 0 && options.batch_rows > 0 && options.workers > 0 && options.streams_per_worker > 0,
        "memory budgeting requires positive corpus, batch, worker, and stream counts"
    );
    let corpus_bytes = memory_estimate(tables, rows, row_bytes, copies, options)?;
    let configured_live_slots = options
        .workers
        .checked_mul(options.streams_per_worker)
        .context("configured live stream count overflow")?;
    let mut budget = MemoryBudget {
        corpus_bytes,
        per_live_batch_bytes: 0,
        live_batch_bytes: 0,
        configured_live_slots,
        admitted_live_slots: 0,
        total_bytes: corpus_bytes,
    };
    if options.data_profile == DataProfile::Realistic {
        let identity_row_bytes = tables
            .iter()
            .map(|table| estimated_identity_row_bytes(table, client_id, options))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max()
            .context("realistic profile has no tables for live-batch budgeting")?;
        let rows_per_batch = rows.min(options.batch_rows);
        budget.per_live_batch_bytes = memory_estimate(tables, rows_per_batch, identity_row_bytes, 1, options)?;
        let available = options
            .max_memory_bytes
            .checked_sub(corpus_bytes)
            .context("corpus reservation exceeds max-memory-bytes")?;
        anyhow::ensure!(
            available >= budget.per_live_batch_bytes,
            "one realistic live batch needs {} reserved bytes beyond the {}-byte corpus reservation, exceeding max-memory-bytes={}",
            budget.per_live_batch_bytes,
            corpus_bytes,
            options.max_memory_bytes
        );
        budget.admitted_live_slots =
            usize::try_from((available / budget.per_live_batch_bytes).min(configured_live_slots as u64))
                .context("admitted live-batch slot count exceeds address space")?;
        anyhow::ensure!(
            budget.admitted_live_slots > 0,
            "no realistic live-batch slot fits max-memory-bytes"
        );
        budget.live_batch_bytes = budget
            .per_live_batch_bytes
            .checked_mul(budget.admitted_live_slots as u64)
            .context("live-batch reservation overflow")?;
        budget.total_bytes = corpus_bytes
            .checked_add(budget.live_batch_bytes)
            .context("total memory reservation overflow")?;
    }
    anyhow::ensure!(
        budget.total_bytes <= options.max_memory_bytes,
        "conservative workload reservation needs {} bytes, exceeding max-memory-bytes={}",
        budget.total_bytes,
        options.max_memory_bytes
    );
    Ok(budget)
}

fn estimated_identity_row_bytes(table: &DmvTable, client_id: &str, options: &Options) -> Result<u64> {
    let identity_width = logical_identity_max_width(options)?;
    let endpoint = table
        .schema
        .fields()
        .iter()
        .find(|field| field.name() == "endpoint_url")
        .context("realistic logical identity requires the public endpoint_url column")?;
    anyhow::ensure!(
        endpoint.data_type() == &DataType::Utf8,
        "logical identity endpoint_url column must be a nonnullable string"
    );
    let previous_width = match endpoint.name().as_str() {
        "customer_id" => options.payload_customer_id.as_deref().unwrap_or(client_id).len(),
        name => realistic_category_max_width(name).unwrap_or_else(|| {
            if options.string_width == 0 {
                name.len() + 32
            } else {
                options.string_width
            }
        }),
    };
    estimated_row_bytes(table, client_id, options)
        .checked_sub(previous_width as u64 + 5)
        .and_then(|bytes| bytes.checked_add(identity_width as u64 + 5))
        .context("identity-bearing row memory estimate overflow")
}

fn logical_identity_max_width(options: &Options) -> Result<usize> {
    anyhow::ensure!(
        options.workers > 0 && options.batch_rows > 0,
        "identity memory sizing requires positive workers and batch-rows"
    );
    logical_identity_url(
        uuid::Uuid::nil(),
        uuid::Uuid::nil(),
        options.workers - 1,
        u64::MAX,
        options.batch_rows - 1,
    )
    .map(|identity| identity.len())
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

fn oat_client_metrics_line_bytes(metrics: &serde_json::Value) -> Result<usize> {
    serde_json::to_vec(metrics)?
        .len()
        .checked_add(b"OAT_CLIENT_METRICS ".len() + 1)
        .context("terminal OAT metrics line size overflow")
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

fn is_guid_field(name: &str) -> bool {
    name.ends_with("_id") && name != "customer_id"
}

fn realistic_category_values(field_name: &str) -> Option<&'static [&'static str]> {
    match field_name {
        "replica_role" => Some(&["RESOLVING", "PRIMARY", "SECONDARY"]),
        "operational_state_desc" => Some(&[
            "PENDING_FAILOVER",
            "PENDING",
            "ONLINE",
            "OFFLINE",
            "FAILED",
            "FAILED_NO_QUORUM",
        ]),
        "recovery_health_desc" => Some(&["ONLINE_IN_PROGRESS", "ONLINE"]),
        "synchronization_health_desc" => Some(&["NOT_HEALTHY", "PARTIALLY_HEALTHY", "HEALTHY"]),
        "connected_state_desc" => Some(&["DISCONNECTED", "CONNECTED"]),
        "availability_mode_desc" => Some(&["ASYNCHRONOUS_COMMIT", "SYNCHRONOUS_COMMIT", "CONFIGURATION_ONLY"]),
        "failover_mode_desc" => Some(&["MANUAL", "AUTOMATIC"]),
        "primary_role_allow_connections_desc" => Some(&["ALL", "READ_WRITE"]),
        "secondary_role_allow_connections_desc" => Some(&["NO", "READ_ONLY", "ALL"]),
        "seeding_mode_desc" => Some(&["AUTOMATIC", "MANUAL"]),
        _ => None,
    }
}

fn realistic_category_max_width(field_name: &str) -> Option<usize> {
    realistic_category_values(field_name)?
        .iter()
        .map(|value| value.len())
        .max()
}

fn realistic_name_parts(field_name: &str, key: u64) -> Option<(String, &'static str)> {
    match field_name {
        "sql_server_instance_name" => Some((format!("sql-{key:04}-"), ".example.invalid")),
        "machine_name" => Some((format!("machine-{key:04}-"), ".example.invalid")),
        "availability_group_name" => Some((format!("ag_{key:04}_"), "")),
        "replica_server_name" => Some((format!("replica-{key:04}-"), ".example.invalid")),
        _ => None,
    }
}

fn synthetic_guid(seed: u64, pod_ordinal: u32, worker: usize, column: usize, row: usize) -> String {
    let identity = (u64::from(pod_ordinal) << 32) | ((worker as u64) << 16) | column as u64;
    let high = mix(seed ^ identity) as u128;
    let value = (high << 64) | row as u128;
    uuid::Uuid::from_u128(value).to_string()
}

fn payload_hash_scope(profile: DataProfile) -> &'static str {
    match profile {
        DataProfile::Diagnostic => "arrow-logical-v2-all-columns",
        DataProfile::Realistic => "arrow-logical-v2-excluding-endpoint_url",
    }
}

fn payload_profile_name(profile: DataProfile) -> &'static str {
    match profile {
        DataProfile::Diagnostic => "diagnostic",
        DataProfile::Realistic => "realistic",
    }
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) -> Result<()> {
    hasher.update(u64::try_from(bytes.len())?.to_le_bytes());
    hasher.update(bytes);
    Ok(())
}

fn hash_u64(hasher: &mut Sha256, value: usize) -> Result<()> {
    hasher.update(u64::try_from(value)?.to_le_bytes());
    Ok(())
}

fn new_payload_hasher(profile: DataProfile, copies: usize, tables: usize) -> Result<Sha256> {
    let mut hasher = Sha256::new();
    hasher.update(PAYLOAD_HASH_DOMAIN);
    hasher.update(PAYLOAD_HASH_VERSION.to_le_bytes());
    hash_bytes(&mut hasher, payload_profile_name(profile).as_bytes())?;
    hash_u64(&mut hasher, copies)?;
    hash_u64(&mut hasher, tables)?;
    Ok(hasher)
}

fn hash_data_type(hasher: &mut Sha256, data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Utf8 => hash_bytes(hasher, b"utf8")?,
        DataType::Boolean => hash_bytes(hasher, b"bool")?,
        DataType::UInt8 => hash_bytes(hasher, b"u8")?,
        DataType::Int16 => hash_bytes(hasher, b"i16")?,
        DataType::Int32 => hash_bytes(hasher, b"i32")?,
        DataType::Int64 => hash_bytes(hasher, b"i64")?,
        DataType::Float64 => hash_bytes(hasher, b"f64")?,
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            hash_bytes(hasher, b"timestamp_us")?;
            if let Some(timezone) = timezone {
                hasher.update([1]);
                hash_bytes(hasher, timezone.as_bytes())?;
            } else {
                hasher.update([0]);
            }
        }
        other => bail!("unsupported synthetic payload hash type {other}"),
    }
    Ok(())
}

fn hash_array_value(hasher: &mut Sha256, array: &dyn Array, data_type: &DataType, row: usize) -> Result<()> {
    if array.is_null(row) {
        hasher.update([0]);
        return Ok(());
    }
    hasher.update([1]);
    match data_type {
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .context("payload string column type mismatch")?;
            hash_bytes(hasher, values.value(row).as_bytes())?;
        }
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("payload boolean column type mismatch")?;
            hasher.update([u8::from(values.value(row))]);
        }
        DataType::UInt8 => {
            let values = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .context("payload u8 column type mismatch")?;
            hasher.update([values.value(row)]);
        }
        DataType::Int16 => {
            let values = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .context("payload i16 column type mismatch")?;
            hasher.update(values.value(row).to_le_bytes());
        }
        DataType::Int32 => {
            let values = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("payload i32 column type mismatch")?;
            hasher.update(values.value(row).to_le_bytes());
        }
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("payload i64 column type mismatch")?;
            hasher.update(values.value(row).to_le_bytes());
        }
        DataType::Float64 => {
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("payload f64 column type mismatch")?;
            hasher.update(values.value(row).to_bits().to_le_bytes());
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .context("payload timestamp column type mismatch")?;
            hasher.update(values.value(row).to_le_bytes());
        }
        other => bail!("unsupported synthetic payload hash type {other}"),
    }
    Ok(())
}

fn hash_payload_batch(
    hasher: &mut Sha256,
    table: &DmvTable,
    batch: &RecordBatch,
    copy: usize,
    offset: usize,
    total_rows: usize,
    profile: DataProfile,
) -> Result<()> {
    anyhow::ensure!(
        batch.schema().as_ref() == table.schema.as_ref(),
        "payload batch schema changed"
    );
    anyhow::ensure!(
        offset
            .checked_add(batch.num_rows())
            .is_some_and(|end| end <= total_rows),
        "payload batch exceeds table row count"
    );
    let schema = batch.schema();
    let fields = schema.fields();
    let include_field =
        |field: &arrow_schema::Field| profile != DataProfile::Realistic || field.name() != "endpoint_url";
    let field_count = fields.iter().filter(|field| include_field(field)).count();
    if offset == 0 {
        hasher.update([0x10]);
        hash_u64(hasher, copy)?;
        hash_bytes(hasher, table.identity.catalog().to_string().as_bytes())?;
        hash_bytes(hasher, table.identity.schema().to_string().as_bytes())?;
        hash_bytes(hasher, table.identity.table().to_string().as_bytes())?;
        hash_u64(hasher, total_rows)?;
        hash_u64(hasher, field_count)?;
        for (index, field) in fields.iter().enumerate().filter(|(_, field)| include_field(field)) {
            hasher.update([HASH_FIELD]);
            hash_u64(hasher, index)?;
            hash_bytes(hasher, field.name().as_bytes())?;
            hash_data_type(hasher, field.data_type())?;
            hasher.update([u8::from(field.is_nullable())]);
        }
    }
    for row in 0..batch.num_rows() {
        hasher.update([HASH_ROW]);
        hash_u64(hasher, offset + row)?;
        for (index, field) in fields.iter().enumerate().filter(|(_, field)| include_field(field)) {
            hash_array_value(hasher, batch.column(index).as_ref(), field.data_type(), row)?;
        }
    }
    if offset + batch.num_rows() == total_rows {
        hasher.update([0x12]);
    }
    Ok(())
}

fn finish_payload_hasher(mut hasher: Sha256) -> String {
    hasher.update([0xff]);
    format!("{:x}", hasher.finalize())
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
            let string_key = |row: usize| {
                let value = random(row);
                if unit(mix(value)) < options.skew {
                    0
                } else {
                    value % options.cardinality as u64
                }
            };
            let value_key = |row: usize| {
                let value = random(row);
                if unit(mix(value)) < options.skew {
                    0
                } else {
                    match options.value_variation.unwrap_or(ValueVariation::Cardinality) {
                        ValueVariation::Cardinality => value % options.cardinality as u64,
                        ValueVariation::PerRow => value,
                    }
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
                DataType::Utf8 if field.name() == "customer_id" => Arc::new(StringArray::from_iter_values(
                    std::iter::repeat_n(options.payload_customer_id.as_deref().unwrap_or(client_id), rows),
                )),
                DataType::Utf8 => Arc::new(StringArray::from_iter((0..rows).map(|row| {
                    valid(row).then(|| {
                        if is_guid_field(field.name())
                            && options.guid_uniqueness.unwrap_or(GuidUniqueness::Cardinality) == GuidUniqueness::PerRow
                        {
                            synthetic_guid(options.seed, options.pod_ordinal, worker, column, offset + row)
                        } else if let Some(value) = (options.data_profile == DataProfile::Realistic)
                            .then(|| realistic_category_values(field.name()))
                            .flatten()
                            .map(|values| values[(string_key(row) % values.len() as u64) as usize])
                        {
                            value.to_owned()
                        } else if options.string_width == 0
                            && options.data_profile == DataProfile::Diagnostic
                            && options.string_length_distribution == Some(StringLengthDistribution::Fixed)
                        {
                            format!("{}-{}", field.name(), string_key(row))
                        } else {
                            let key = string_key(row);
                            let mut state = mix(options.seed ^ key ^ mix(column as u64) ^ mix(worker as u64));
                            let maximum_width = if options.string_width == 0 {
                                field.name().len() + 32
                            } else {
                                options.string_width
                            };
                            let width = match options
                                .string_length_distribution
                                .unwrap_or(StringLengthDistribution::Fixed)
                            {
                                StringLengthDistribution::Fixed => maximum_width,
                                StringLengthDistribution::Uniform => {
                                    let minimum = maximum_width.div_ceil(2);
                                    minimum + (random(row) % (maximum_width - minimum + 1) as u64) as usize
                                }
                            };
                            let (prefix, suffix) = if options.data_profile == DataProfile::Realistic {
                                realistic_name_parts(field.name(), key).unwrap_or_default()
                            } else {
                                (String::new(), "")
                            };
                            let prefix_len = prefix.len().min(width);
                            let suffix = if prefix_len == prefix.len() && prefix.len() + suffix.len() <= width {
                                suffix
                            } else {
                                ""
                            };
                            let mut value = String::with_capacity(width);
                            value.push_str(&prefix[..prefix_len]);
                            let body_width = width - prefix_len - suffix.len();
                            for index in 0..body_width {
                                state = mix(state);
                                if index as f64 / (maximum_width as f64) < options.entropy {
                                    value.push((b'a' + (state % 26) as u8) as char);
                                } else {
                                    value.push('a');
                                }
                            }
                            value.push_str(suffix);
                            value
                        }
                    })
                }))),
                DataType::Boolean => primitive!(BooleanArray, |row| value_key(row).is_multiple_of(2)),
                DataType::UInt8 => primitive!(UInt8Array, |row| (value_key(row) % 255) as u8),
                DataType::Int16 => primitive!(Int16Array, |row| (value_key(row) % 32767) as i16),
                DataType::Int32 => primitive!(Int32Array, |row| (value_key(row) % i32::MAX as u64) as i32),
                DataType::Int64 => primitive!(Int64Array, |row| (value_key(row) & i64::MAX as u64) as i64),
                DataType::Float64 => primitive!(Float64Array, |row| value_key(row) as f64 / 10.0),
                DataType::Timestamp(TimeUnit::Microsecond, timezone) => Arc::new(
                    TimestampMicrosecondArray::from_iter((0..rows).map(|row| {
                        valid(row).then(|| 1_790_000_000_000_000_i64 + (value_key(row) % 1_000_000) as i64)
                    }))
                    .with_timezone_opt(timezone.clone()),
                ),
                other => bail!("unsupported synthetic DMV type {other}"),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::clone(&table.schema), columns)?)
}

fn base36(mut value: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if value == 0 {
        return "0".to_owned();
    }
    let mut reversed = String::new();
    while value > 0 {
        reversed.push(DIGITS[(value % 36) as usize] as char);
        value /= 36;
    }
    reversed.chars().rev().collect()
}

fn logical_identity_url(
    run_id: uuid::Uuid,
    incarnation_id: uuid::Uuid,
    worker: usize,
    sequence: u64,
    row: usize,
) -> Result<String> {
    anyhow::ensure!(worker < 1_024, "logical identity worker id exceeds 1023");
    let row = u32::try_from(row).context("logical identity row ordinal exceeds u32")?;
    Ok(format!(
        "https://x.invalid/v1/{}/{}/{}/{}/{row}",
        run_id.simple(),
        incarnation_id.simple(),
        worker,
        base36(sequence),
    ))
}

fn batch_with_logical_identity(
    batch: &RecordBatch,
    run_id: uuid::Uuid,
    incarnation_id: uuid::Uuid,
    worker: usize,
    sequence: u64,
    builder: IdentityBuilder,
) -> Result<RecordBatch> {
    let identity_column = batch
        .schema()
        .index_of("endpoint_url")
        .context("realistic logical identity requires the public endpoint_url column")?;
    anyhow::ensure!(
        batch.schema().field(identity_column).data_type() == &DataType::Utf8,
        "logical identity endpoint_url column must be a nonnullable string"
    );
    let identity: ArrayRef = match builder {
        IdentityBuilder::FormattedRows => {
            let values = (0..batch.num_rows())
                .map(|row| logical_identity_url(run_id, incarnation_id, worker, sequence, row))
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StringArray::from_iter_values(values.iter().map(String::as_str)))
        }
        IdentityBuilder::DirectUtf8 => {
            use std::fmt::Write as _;
            anyhow::ensure!(worker < 1_024, "logical identity worker id exceeds 1023");
            let last_row = batch.num_rows().saturating_sub(1);
            u32::try_from(last_row).context("logical identity row ordinal exceeds u32")?;
            let prefix = format!(
                "https://x.invalid/v1/{}/{}/{}/{}/",
                run_id.simple(),
                incarnation_id.simple(),
                worker,
                base36(sequence)
            );
            let width = prefix
                .len()
                .checked_add(last_row.to_string().len())
                .context("identity width overflow")?;
            let capacity = width
                .checked_mul(batch.num_rows())
                .context("identity values capacity overflow")?;
            anyhow::ensure!(
                capacity <= i32::MAX as usize,
                "identity values exceed Arrow Utf8 capacity"
            );
            let mut array = arrow_array::builder::StringBuilder::with_capacity(batch.num_rows(), capacity);
            let mut value = String::with_capacity(width);
            for row in 0..batch.num_rows() {
                value.clear();
                write!(&mut value, "{prefix}{row}")?;
                array.append_value(&value);
            }
            Arc::new(array.finish())
        }
    };
    let columns = batch
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            if index == identity_column {
                Arc::clone(&identity)
            } else {
                Arc::clone(column)
            }
        })
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
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
        .with_options(
            IpcWriteOptions::try_new(options.ipc_alignment, false, arrow_ipc::MetadataVersion::V5)?
                .try_with_compression(compression)?,
        )
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

fn flight_data_bytes(messages: &[FlightData]) -> Result<u64> {
    messages.iter().try_fold(0_u64, |total, message| {
        total
            .checked_add(u64::try_from(message.encoded_len())?)
            .context("encoded Arrow IPC size overflow")
    })
}

async fn encode_with_sizes(
    table: &DmvTable,
    version: &str,
    batch: RecordBatch,
    options: &Options,
) -> Result<(Vec<FlightData>, u64, u64)> {
    let data = encode(table, version, batch.clone(), options).await?;
    let encoded_bytes = flight_data_bytes(&data)?;
    let raw_arrow_bytes = if matches!(options.ipc_compression, IpcCompression::None) {
        encoded_bytes
    } else {
        let mut uncompressed = options.clone();
        uncompressed.ipc_compression = IpcCompression::None;
        let raw = encode(table, version, batch, &uncompressed).await?;
        flight_data_bytes(&raw)?
    };
    Ok((data, raw_arrow_bytes, encoded_bytes))
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
            network_rx_bytes: network.rx_bytes,
            network_tx_bytes: network.tx_bytes,
            network_counters_unavailable_reason: network.counters_unavailable_reason,
            network_advertised_link_speed_bps: network.advertised_link_speed_bps,
            network_interface_speed_unavailable_reasons: network.interface_speed_unavailable_reasons,
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

struct NetworkSnapshot {
    rx_bytes: Option<u64>,
    tx_bytes: Option<u64>,
    counters_unavailable_reason: Option<String>,
    advertised_link_speed_bps: Option<u64>,
    interface_speed_unavailable_reasons: Vec<String>,
}

fn network_snapshot() -> Result<NetworkSnapshot> {
    let contents = match std::fs::read_to_string("/proc/net/dev") {
        Ok(contents) => contents,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok(NetworkSnapshot {
                rx_bytes: None,
                tx_bytes: None,
                counters_unavailable_reason: Some("network_counters_unavailable".to_owned()),
                advertised_link_speed_bps: None,
                interface_speed_unavailable_reasons: Vec::new(),
            });
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
        return Ok(NetworkSnapshot {
            rx_bytes: None,
            tx_bytes: None,
            counters_unavailable_reason: Some("no_non_loopback_interfaces".to_owned()),
            advertised_link_speed_bps: None,
            interface_speed_unavailable_reasons: Vec::new(),
        });
    }
    Ok(NetworkSnapshot {
        rx_bytes: Some(rx_bytes),
        tx_bytes: Some(tx_bytes),
        counters_unavailable_reason: None,
        advertised_link_speed_bps: all_speeds_known.then_some(advertised_speed_bps),
        interface_speed_unavailable_reasons: speed_unavailable_reasons,
    })
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
        capture_identities: AtomicBool,
        identities: Mutex<Vec<String>>,
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
            if self.state.capture_identities.load(Ordering::Relaxed)
                && let Ok(index) = batch.schema().index_of("endpoint_url")
                && let Some(values) = batch.column(index).as_any().downcast_ref::<StringArray>()
            {
                self.state
                    .identities
                    .lock()
                    .map_err(|_| RuntimeError::internal())?
                    .extend(values.iter().flatten().map(str::to_owned));
            }
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
        Cli::try_parse_from(["test", "--workers", "2", "--data-profile", "diagnostic"])?
            .options
            .resolve(100, 1)
    }

    #[tokio::test]
    async fn direct_identity_matches_formatted_rows_and_encoded_frames() -> Result<()> {
        use prost::Message;
        let mut options = options()?;
        options.data_profile = DataProfile::Realistic;
        let table = table_named("sqlserver_availability_replica_states")?;
        let batch = synthetic_batch(&table, "identity-test", 37, 0, 0, &options)?;
        for (worker, sequence) in [(0, 0), (1023, u64::MAX)] {
            let args = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
            let formatted =
                batch_with_logical_identity(&batch, args.0, args.1, worker, sequence, IdentityBuilder::FormattedRows)?;
            let direct =
                batch_with_logical_identity(&batch, args.0, args.1, worker, sequence, IdentityBuilder::DirectUtf8)?;
            assert_eq!(formatted, direct);
            let frames = encode(&table, "v1", formatted, &options).await?;
            let direct_frames = encode(&table, "v1", direct, &options).await?;
            assert_eq!(
                frames.iter().map(Message::encode_to_vec).collect::<Vec<_>>(),
                direct_frames.iter().map(Message::encode_to_vec).collect::<Vec<_>>()
            );
        }
        Ok(())
    }

    #[test]
    fn send_cursor_replays_identical_frames_without_eager_cloning() -> Result<()> {
        use prost::Message;
        let frames = Arc::new(vec![
            FlightData {
                data_header: vec![1, 2].into(),
                ..Default::default()
            },
            FlightData {
                data_body: vec![3, 4, 5].into(),
                ..Default::default()
            },
        ]);
        let expected = send_frames(&frames, SendIter::CloneVec)
            .map(|frame| frame.encode_to_vec())
            .collect::<Vec<_>>();
        let mut cursor = send_frames(&frames, SendIter::ArcCursor);
        assert_eq!(Arc::strong_count(&frames), 2);
        assert_eq!(
            cursor.next().context("missing first frame")?.encode_to_vec(),
            expected[0]
        );
        assert_eq!(
            cursor.next().context("missing second frame")?.encode_to_vec(),
            expected[1]
        );
        assert!(cursor.next().is_none());
        let retry = send_frames(&frames, SendIter::ArcCursor)
            .map(|frame| frame.encode_to_vec())
            .collect::<Vec<_>>();
        assert_eq!(retry, expected);
        Ok(())
    }

    fn hash_test_batches(
        table: &DmvTable,
        profile: DataProfile,
        copy: usize,
        batches: &[RecordBatch],
    ) -> Result<String> {
        let total_rows = batches.iter().map(RecordBatch::num_rows).sum();
        let mut hasher = new_payload_hasher(profile, copy + 1, 1)?;
        let mut offset = 0;
        for batch in batches {
            hash_payload_batch(&mut hasher, table, batch, copy, offset, total_rows, profile)?;
            offset += batch.num_rows();
        }
        Ok(finish_payload_hasher(hasher))
    }

    #[test]
    fn independently_generated_corpus_hash_ignores_send_batch_size() -> Result<()> {
        let table = table_named("sqlserver_availability_replica_states")?;
        let total_rows = 65_537;
        for profile in [DataProfile::Diagnostic, DataProfile::Realistic] {
            let hash_for = |batch_rows: usize| -> Result<String> {
                let mut options = options()?;
                options.data_profile = profile;
                options.batch_rows = batch_rows;
                options.null_fraction = 0.2;
                let mut hasher = new_payload_hasher(profile, 1, 1)?;
                for offset in (0..total_rows).step_by(batch_rows) {
                    let rows = batch_rows.min(total_rows - offset);
                    let batch = synthetic_batch(&table, "logical-hash", rows, offset, 0, &options)?;
                    hash_payload_batch(&mut hasher, &table, &batch, 0, offset, total_rows, profile)?;
                }
                Ok(finish_payload_hasher(hasher))
            };
            assert_eq!(hash_for(4096)?, hash_for(65_536)?, "{profile:?}");
        }
        Ok(())
    }

    #[test]
    fn logical_corpus_hash_ignores_batch_splits_and_detects_order_or_value_changes() -> Result<()> {
        let options = options()?;
        let table = table_named("sqlserver_availability_replica_states")?;
        let batch = synthetic_batch(&table, "logical-hash", 7, 0, 0, &options)?;
        let whole = hash_test_batches(&table, DataProfile::Diagnostic, 0, std::slice::from_ref(&batch))?;
        let ragged = [batch.slice(0, 2), batch.slice(2, 1), batch.slice(3, 4)];
        assert_eq!(whole, hash_test_batches(&table, DataProfile::Diagnostic, 0, &ragged)?);
        let reordered = [batch.slice(3, 4), batch.slice(0, 3)];
        assert_ne!(
            whole,
            hash_test_batches(&table, DataProfile::Diagnostic, 0, &reordered)?
        );
        let column = table.schema.index_of("replica_server_name")?;
        let values = batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("replica_server_name is not a string")?;
        let changed_values = StringArray::from_iter((0..batch.num_rows()).map(|row| {
            if row == 3 {
                Some("changed-middle-value")
            } else if values.is_null(row) {
                None
            } else {
                Some(values.value(row))
            }
        }));
        let mut columns = batch.columns().to_vec();
        columns[column] = Arc::new(changed_values);
        let changed = RecordBatch::try_new(batch.schema(), columns)?;
        assert_ne!(
            whole,
            hash_test_batches(&table, DataProfile::Diagnostic, 0, &[changed])?
        );
        assert_ne!(whole, hash_test_batches(&table, DataProfile::Diagnostic, 1, &[batch])?);
        Ok(())
    }

    #[test]
    fn logical_corpus_hash_covers_all_public_generator_schemas() -> Result<()> {
        let options = options()?;
        for table in tables()? {
            let batch = synthetic_batch(&table, "all-public-schemas", 3, 0, 0, &options)?;
            hash_test_batches(&table, DataProfile::Diagnostic, 0, &[batch])?;
        }
        Ok(())
    }

    #[test]
    fn fixed_business_payload_retains_distinct_delivery_identities() -> Result<()> {
        let mut options = options()?;
        options.data_profile = DataProfile::Realistic;
        options.payload_customer_id = Some("direct-e64-confirmwide".to_owned());
        let table = table_named("sqlserver_availability_replica_states")?;
        options.data_profile = DataProfile::Diagnostic;
        assert_eq!(
            synthetic_batch(&table, "delivery-run-one", 8, 0, 0, &options)?,
            synthetic_batch(&table, "delivery-run-two", 8, 0, 0, &options)?
        );
        options.data_profile = DataProfile::Realistic;
        let first = synthetic_batch(&table, "delivery-run-one", 8, 0, 0, &options)?;
        let second = synthetic_batch(&table, "delivery-run-two", 8, 0, 0, &options)?;
        assert_eq!(first, second);
        let customer = first
            .column_by_name("customer_id")
            .context("customer_id missing")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("customer_id is not a string")?;
        assert!(customer.iter().all(|value| value == Some("direct-e64-confirmwide")));
        let a = batch_with_logical_identity(
            &first,
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(2),
            0,
            0,
            IdentityBuilder::FormattedRows,
        )?;
        let b = batch_with_logical_identity(
            &second,
            uuid::Uuid::from_u128(3),
            uuid::Uuid::from_u128(4),
            0,
            0,
            IdentityBuilder::FormattedRows,
        )?;
        assert_ne!(a.column_by_name("endpoint_url"), b.column_by_name("endpoint_url"));
        assert_eq!(
            hash_test_batches(&table, DataProfile::Realistic, 0, std::slice::from_ref(&a))?,
            hash_test_batches(&table, DataProfile::Realistic, 0, std::slice::from_ref(&b))?
        );
        assert_ne!(
            hash_test_batches(&table, DataProfile::Diagnostic, 0, &[a])?,
            hash_test_batches(&table, DataProfile::Diagnostic, 0, &[b])?
        );
        options.payload_customer_id = None;
        assert_ne!(
            synthetic_batch(&table, "delivery-run-one", 8, 0, 0, &options)?,
            synthetic_batch(&table, "delivery-run-two", 8, 0, 0, &options)?,
        );
        options.payload_customer_id = Some("bad identity!".to_owned());
        assert!(options.resolve(100, 1).is_err());
        Ok(())
    }

    fn manifest_test_client() -> Result<PreparedClient> {
        let mut options = options()?;
        options.data_profile = DataProfile::Realistic;
        let workers = options.workers;
        Ok(PreparedClient {
            corpus: Vec::new(),
            channels: Vec::new(),
            live_batch_slots: Arc::new(tokio::sync::Semaphore::new(1)),
            signer: None,
            counters: (0..workers).map(|_| Counters::default()).collect(),
            next: AtomicU64::new(0),
            worker_next: (0..workers).map(|_| AtomicU64::new(0)).collect(),
            worker_batch_sequence: (0..workers).map(|_| AtomicU64::new(0)).collect(),
            reserved_rows: AtomicU64::new(0),
            measurement: Mutex::new(None),
            run_id: uuid::Uuid::new_v4(),
            incarnation_id: uuid::Uuid::new_v4(),
            logical_batches: Mutex::new(Vec::new()),
            manifest_capacity_reached: AtomicBool::new(false),
            terminal_failure_reason: Mutex::new(None),
            table_count: 0,
            preparation: serde_json::json!({}),
            started: Instant::now(),
            options,
        })
    }

    #[test]
    fn logical_identity_namespaces_are_injective_and_checked() -> Result<()> {
        let run = uuid::Uuid::from_u128(1);
        let incarnation = uuid::Uuid::from_u128(2);
        let identities = [
            logical_identity_url(run, incarnation, 0, 0, 0)?,
            logical_identity_url(run, incarnation, 0, 0, 1)?,
            logical_identity_url(run, incarnation, 0, 1, 0)?,
            logical_identity_url(run, incarnation, 1, 0, 0)?,
            logical_identity_url(run, uuid::Uuid::from_u128(3), 0, 0, 0)?,
            logical_identity_url(uuid::Uuid::from_u128(4), incarnation, 0, 0, 0)?,
        ];
        assert_eq!(
            identities.iter().collect::<std::collections::HashSet<_>>().len(),
            identities.len()
        );
        assert!(logical_identity_url(run, incarnation, 1_024, 0, 0).is_err());
        assert!(logical_identity_url(run, incarnation, 0, 0, u32::MAX as usize + 1).is_err());

        let client = manifest_test_client()?;
        client.worker_batch_sequence[0].store(u64::MAX, Ordering::Relaxed);
        assert!(client.next_logical_batch_sequence(0).is_err());
        assert!(client.next_logical_batch_sequence(usize::MAX).is_err());
        Ok(())
    }

    #[test]
    fn manifest_ranges_preserve_rows_needed_to_detect_equal_count_substitution() -> Result<()> {
        let client = manifest_test_client()?;
        let table = "sqlserver_availability_replica_states".to_owned();
        let mut records = client
            .logical_batches
            .lock()
            .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?;
        for sequence in 0..2 {
            records.push(LogicalBatchRecord {
                table: table.clone(),
                worker: 0,
                sequence,
                row_count: 2,
                cohort: ManifestCohort::Measurement,
                attempts: 2,
                outcome: ManifestOutcome::Acknowledged,
                first_send_unix_ns: Some(1_000_000_000),
                last_outcome_unix_ms: Some(1_000),
            });
        }
        drop(records);

        let manifest = client.logical_manifest("completed")?;
        assert_eq!(manifest["run_uuid_source"], "client_generated");
        assert_eq!(manifest["oat_run_uuid"], serde_json::Value::Null);
        assert_eq!(manifest["incarnation_uuid_source"], "client_generated");
        let metrics = client.metrics("failed")?;
        assert_eq!(
            metrics["bytes_semantics"]["uncompressed_flight_data_bytes"],
            "sum_of_FlightData_protobuf_encoded_lengths_before_Arrow_IPC_compression"
        );
        assert_eq!(
            metrics["bytes_semantics"]["exclusions"],
            "these_counts_are_not_logical_Arrow_buffer_bytes_and_exclude_gRPC_and_HTTP/2_framing"
        );
        assert_eq!(manifest["record_count"], 2);
        assert_eq!(manifest["retry_attempts"], 2);
        let range = manifest["ranges"][0].as_object().context("manifest range")?;
        assert_eq!(range["first_sequence"], 0);
        assert_eq!(range["last_sequence"], 1);
        assert_eq!(range["batch_count"], 2);
        assert_eq!(range["rows_per_batch"], 2);

        let expected_ids = (0..=1)
            .flat_map(|sequence| {
                (0..2).map(move |row| logical_identity_url(client.run_id, client.incarnation_id, 0, sequence, row))
            })
            .collect::<Result<Vec<_>>>()?;
        let expected = expected_ids
            .into_iter()
            .fold(std::collections::BTreeMap::new(), |mut counts, id| {
                *counts.entry(id).or_insert(0_u64) += 1;
                counts
            });
        let mut observed = expected.clone();
        let duplicate = logical_identity_url(client.run_id, client.incarnation_id, 0, 0, 0)?;
        let missing = logical_identity_url(client.run_id, client.incarnation_id, 0, 1, 1)?;
        *observed.get_mut(&missing).context("expected identity missing")? -= 1;
        *observed.entry(duplicate).or_default() += 1;
        assert_eq!(observed.values().sum::<u64>(), expected.values().sum::<u64>());
        assert_ne!(observed, expected);
        Ok(())
    }

    #[test]
    fn oversized_terminal_manifest_fails_closed_within_oat_line_bound() -> Result<()> {
        let client = manifest_test_client()?;
        let mut records = client
            .logical_batches
            .lock()
            .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?;
        for index in 0..10_000_u64 {
            records.push(LogicalBatchRecord {
                table: "sqlserver_availability_replica_states".to_owned(),
                worker: 0,
                sequence: index * 2,
                row_count: 100,
                cohort: ManifestCohort::Measurement,
                attempts: 1,
                outcome: ManifestOutcome::Acknowledged,
                first_send_unix_ns: Some(1_000_000_000 + index),
                last_outcome_unix_ms: Some(1_000),
            });
        }
        drop(records);

        let receipt = client.metrics("completed")?;
        assert_eq!(receipt["phase"], "failed");
        assert_eq!(receipt["logical_batch_manifest"]["truncated"], true);
        assert_eq!(
            receipt["logical_batch_manifest"]["terminal_failure"],
            "serialized_manifest_exceeds_limit"
        );
        let line = format!("OAT_CLIENT_METRICS {receipt}\n");
        assert!(line.len() <= MAX_OAT_CLIENT_METRICS_LINE_BYTES);
        eprintln!(
            "W2_TERMINAL_MANIFEST receipt_line_bytes={} cap_bytes={}",
            line.len(),
            MAX_OAT_CLIENT_METRICS_LINE_BYTES
        );
        let payload = line.strip_prefix("OAT_CLIENT_METRICS ").context("metrics prefix")?;
        serde_json::from_str::<serde_json::Value>(payload.trim_end())?;
        assert!(client.ensure_terminal_manifest_complete().is_err());
        Ok(())
    }

    #[test]
    fn non_manifest_terminal_overflow_emits_a_bounded_failure_receipt() -> Result<()> {
        let mut client = manifest_test_client()?;
        client.preparation = serde_json::json!({
            "non_manifest_witness": "x".repeat(MAX_OAT_CLIENT_METRICS_LINE_BYTES)
        });

        let receipt = client.metrics("completed")?;
        assert_eq!(receipt["phase"], "failed");
        assert_eq!(receipt["terminal_failure"], "serialized_terminal_receipt_exceeds_limit");
        let line_bytes = oat_client_metrics_line_bytes(&receipt)?;
        assert!(line_bytes <= MAX_OAT_CLIENT_METRICS_LINE_BYTES);
        eprintln!(
            "W2_TERMINAL_NON_MANIFEST_OVERFLOW receipt_line_bytes={line_bytes} cap_bytes={MAX_OAT_CLIENT_METRICS_LINE_BYTES} failure={}",
            receipt["terminal_failure"]
        );
        Ok(())
    }

    #[test]
    fn first_terminal_overflow_reason_survives_completed_error_failed_path() -> Result<()> {
        let mut client = manifest_test_client()?;
        client.preparation = serde_json::json!({
            "non_manifest_witness": "x".repeat(MAX_OAT_CLIENT_METRICS_LINE_BYTES)
        });
        let mut records = client
            .logical_batches
            .lock()
            .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?;
        for index in 0..10_000_u64 {
            records.push(LogicalBatchRecord {
                table: "sqlserver_availability_replica_states".to_owned(),
                worker: 0,
                sequence: index * 2,
                row_count: 100,
                cohort: ManifestCohort::Measurement,
                attempts: 1,
                outcome: ManifestOutcome::Acknowledged,
                first_send_unix_ns: Some(1_000_000_000 + index),
                last_outcome_unix_ms: Some(1_000),
            });
        }
        drop(records);

        let completed = client.completed_terminal_metrics();
        assert!(completed.is_err(), "oversized terminal manifest was accepted");
        let failed = client.failed_terminal_metrics()?;
        assert_eq!(failed["phase"], "failed");
        assert_eq!(failed["terminal_failure"], "serialized_manifest_exceeds_limit");
        assert_eq!(failed["serialized_byte_cap"], MAX_OAT_CLIENT_METRICS_LINE_BYTES);
        let line_bytes = oat_client_metrics_line_bytes(&failed)?;
        assert!(line_bytes <= MAX_OAT_CLIENT_METRICS_LINE_BYTES);
        eprintln!(
            "E1_FIRST_TERMINAL_FAILURE completed=error failed_reason={} receipt_line_bytes={line_bytes} cap_bytes={MAX_OAT_CLIENT_METRICS_LINE_BYTES}",
            failed["terminal_failure"]
        );
        Ok(())
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
            vec!["--data-profile", "random"],
            vec!["--string-length-distribution", "exponential"],
            vec!["--value-variation", "batch"],
            vec!["--guid-uniqueness", "never"],
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
            vec!["--rate-limit-period-ms", "0"],
            vec!["--rate-limit-period-ms", "86400001"],
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
    fn profile_defaults_preserve_diagnostic_and_expand_realistic_row_variation() -> Result<()> {
        let diagnostic = Cli::try_parse_from(["test", "--workers", "2"])?
            .options
            .resolve(1024, 1)?;
        assert_eq!(diagnostic.data_profile, DataProfile::Diagnostic);
        assert_eq!(
            diagnostic.string_length_distribution,
            Some(StringLengthDistribution::Fixed)
        );
        assert_eq!(diagnostic.value_variation, Some(ValueVariation::Cardinality));
        assert_eq!(diagnostic.guid_uniqueness, Some(GuidUniqueness::Cardinality));
        assert_eq!(diagnostic.string_width, 0);

        let realistic = Cli::try_parse_from(["test", "--data-profile", "realistic"])?
            .options
            .resolve(1024, 1)?;
        assert_eq!(realistic.string_width, 96);
        assert_eq!(
            realistic.string_length_distribution,
            Some(StringLengthDistribution::Uniform)
        );
        assert_eq!(realistic.value_variation, Some(ValueVariation::PerRow));
        assert_eq!(realistic.guid_uniqueness, Some(GuidUniqueness::PerRow));
        Ok(())
    }

    #[tokio::test]
    async fn realistic_profile_uses_documented_categories_and_varies_ids_integers_and_timestamps() -> Result<()> {
        let options = Cli::try_parse_from(["test", "--data-profile", "realistic"])?
            .options
            .resolve(1024, 1)?;
        let table = table_named("sqlserver_availability_replica_states")?;
        let batch = synthetic_batch(&table, "client", 1024, 0, 0, &options)?;
        let base_ipc_data = encode(&table, "v1", batch.clone(), &options).await?;
        let base_ipc_bytes_per_row =
            base_ipc_data.iter().map(|message| message.encoded_len()).sum::<usize>() / batch.num_rows();
        let identity_batch = batch_with_logical_identity(
            &batch,
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            0,
            0,
            IdentityBuilder::FormattedRows,
        )?;
        let identity_ipc_data = encode(&table, "v1", identity_batch.clone(), &options).await?;
        let identity_ipc_bytes_per_row = identity_ipc_data
            .iter()
            .map(|message| message.encoded_len())
            .sum::<usize>()
            / batch.num_rows();
        let string_bytes_per_row = batch
            .columns()
            .iter()
            .filter(|column| column.data_type() == &DataType::Utf8)
            .map(|column| column.get_array_memory_size() / batch.num_rows())
            .collect::<Vec<_>>();
        let names = batch
            .column_by_name("sql_server_instance_name")
            .context("sql_server_instance_name missing")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("sql_server_instance_name is not a string array")?;
        let name_lengths = names.iter().flatten().map(str::len).collect::<Vec<_>>();
        eprintln!(
            "realistic base_ipc_bytes_per_row={base_ipc_bytes_per_row} identity_ipc_bytes_per_row={identity_ipc_bytes_per_row} array_bytes_per_row={} string_columns={string_bytes_per_row:?} name_length={}..{}",
            batch.get_array_memory_size() / batch.num_rows(),
            name_lengths.iter().min().context("missing string lengths")?,
            name_lengths.iter().max().context("missing string lengths")?,
        );
        assert!(
            (500..=800).contains(&identity_ipc_bytes_per_row),
            "identity-bearing realistic profile produced {identity_ipc_bytes_per_row} uncompressed Arrow IPC bytes per row"
        );
        assert!(identity_ipc_bytes_per_row >= base_ipc_bytes_per_row);

        for field_name in [
            "replica_role",
            "operational_state_desc",
            "recovery_health_desc",
            "synchronization_health_desc",
            "connected_state_desc",
            "availability_mode_desc",
            "failover_mode_desc",
            "primary_role_allow_connections_desc",
            "secondary_role_allow_connections_desc",
            "seeding_mode_desc",
        ] {
            let allowed = realistic_category_values(field_name).context("category values missing")?;
            let values = batch
                .column_by_name(field_name)
                .context("categorical string field missing")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("categorical field is not a string array")?;
            let observed = values.iter().flatten().collect::<std::collections::BTreeSet<_>>();
            assert!(observed.len() > 1, "{field_name} did not vary across rows");
            assert!(
                observed.iter().all(|value| allowed.contains(value)),
                "{field_name} emitted a value outside its documented category set"
            );
        }

        let identity_urls = identity_batch
            .column_by_name("endpoint_url")
            .context("endpoint_url missing")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("endpoint_url is not a string array")?;
        assert_eq!(
            identity_urls.iter().collect::<std::collections::BTreeSet<_>>().len(),
            identity_batch.num_rows()
        );

        let guid = batch
            .column_by_name("availability_group_id")
            .context("availability_group_id missing")?
            .as_any()
            .downcast_ref::<StringArray>()
            .context("availability_group_id is not a string array")?;
        let unique_guids = guid.iter().collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique_guids.len(), batch.num_rows());

        assert!(names.iter().all(|value| {
            value.is_some_and(|value| (48..=96).contains(&value.len()) && value.ends_with(".example.invalid"))
        }));
        assert!(names.iter().flatten().collect::<std::collections::BTreeSet<_>>().len() > 1);

        for (field_name, prefix) in [
            ("machine_name", "machine-"),
            ("availability_group_name", "ag_"),
            ("replica_server_name", "replica-"),
        ] {
            let values = batch
                .column_by_name(field_name)
                .context("structured name field missing")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("structured name is not a string array")?;
            assert!(values.iter().flatten().all(|value| value.starts_with(prefix)));
            assert!(values.iter().flatten().collect::<std::collections::BTreeSet<_>>().len() > 1);
        }

        let errors = batch
            .column_by_name("last_connect_error_number")
            .context("last_connect_error_number missing")?
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("last_connect_error_number is not an int32 array")?;
        assert!(errors.iter().collect::<std::collections::BTreeSet<_>>().len() > 1_000);

        let timestamps = batch
            .column_by_name("sample_time_utc")
            .context("sample_time_utc missing")?
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .context("sample_time_utc is not a timestamp array")?;
        assert!(timestamps.iter().collect::<std::collections::BTreeSet<_>>().len() > 990);
        Ok(())
    }

    #[test]
    fn rejects_parked_client_shard_selector_options() {
        for args in [
            vec!["--shard-index-mode", "fixed"],
            vec!["--fixed-shard-index", "0"],
            vec!["--shard-count", "1"],
        ] {
            assert!(Cli::try_parse_from(std::iter::once("test").chain(args)).is_err());
        }
    }

    #[test]
    fn resolution_retains_auto_and_legacy_sources_without_guessing_byte_target_rows() -> Result<()> {
        let requested_options =
            Cli::try_parse_from(["test", "--workers", "0", "--runtime-threads", "0", "--pod-ordinal", "3"])?.options;
        assert_eq!(requested_options.pod_ordinal, 3);
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
            let mut observed_codecs = Vec::new();
            for data in &encoded {
                let message = arrow_ipc::root_as_message(&data.data_header)
                    .map_err(|error| anyhow::anyhow!("invalid IPC header: {error:?}"))?;
                if let Some(record_batch) = message.header_as_record_batch() {
                    observed_codecs.push(record_batch.compression().map(|compression| compression.codec()));
                }
            }
            let expected = match codec {
                IpcCompression::None => None,
                IpcCompression::Lz4Frame => Some(arrow_ipc::CompressionType::LZ4_FRAME),
                IpcCompression::Zstd => Some(arrow_ipc::CompressionType::ZSTD),
            };
            assert!(!observed_codecs.is_empty());
            assert!(observed_codecs.iter().all(|observed| *observed == expected));
            let decoded = FlightRecordBatchStream::new_from_flight_data(stream::iter(encoded.into_iter().map(Ok)))
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(decoded, vec![batch.clone()]);
        }
        Ok(())
    }

    #[tokio::test]
    async fn ipc_alignment_changes_padding_without_changing_rows() -> Result<()> {
        use arrow_flight::decode::FlightRecordBatchStream;
        let table = table_named("sqlserver_wait_stats")?;
        let mut options = options()?;
        let batch = synthetic_batch(&table, "client", 3, 0, 0, &options)?;
        let mut sizes = Vec::new();
        for alignment in [8, 64] {
            options.ipc_alignment = alignment;
            let encoded = encode(&table, "v1", batch.clone(), &options).await?;
            sizes.push(encoded.iter().map(|message| message.data_body.len()).sum::<usize>());
            let decoded = FlightRecordBatchStream::new_from_flight_data(stream::iter(encoded.into_iter().map(Ok)))
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(decoded, vec![batch.clone()]);
        }
        assert!(sizes[0] < sizes[1]);
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
    fn realistic_memory_budget_admits_only_live_batches_that_fit() -> Result<()> {
        let table = table_named("sqlserver_availability_replica_states")?;
        let tables = vec![table];
        let mut options = options()?;
        options.data_profile = DataProfile::Realistic;
        options.workers = 32;
        options.streams_per_worker = 32;
        options.batch_rows = 32_768;
        options.corpus_rows = 32_768;
        options.string_width = 96;
        options.max_memory_bytes = 512 * 1_024 * 1_024;
        let row_bytes = estimated_row_bytes(&tables[0], "client", &options);
        let budget = memory_budget(&tables, "client", options.corpus_rows, row_bytes, 1, &options)?;
        assert_eq!(budget.configured_live_slots, 1_024);
        assert!(budget.admitted_live_slots > 0);
        assert!(budget.admitted_live_slots < budget.configured_live_slots);
        assert_eq!(budget.total_bytes, budget.corpus_bytes + budget.live_batch_bytes);
        assert!(budget.total_bytes <= options.max_memory_bytes);
        eprintln!(
            "W2_MEMORY_BUDGET cap_bytes={} corpus_bytes={} per_live_batch_bytes={} configured_slots={} admitted_slots={} live_reserved_bytes={} total_reserved_bytes={}",
            options.max_memory_bytes,
            budget.corpus_bytes,
            budget.per_live_batch_bytes,
            budget.configured_live_slots,
            budget.admitted_live_slots,
            budget.live_batch_bytes,
            budget.total_bytes
        );

        options.max_memory_bytes = budget.corpus_bytes + budget.per_live_batch_bytes - 1;
        assert!(memory_budget(&tables, "client", options.corpus_rows, row_bytes, 1, &options).is_err());
        eprintln!(
            "W2_MEMORY_SINGLE_BATCH_REJECT cap_bytes={} corpus_bytes={} required_one_live_batch_bytes={}",
            options.max_memory_bytes, budget.corpus_bytes, budget.per_live_batch_bytes
        );
        Ok(())
    }

    #[tokio::test]
    async fn realistic_live_batch_reservation_is_held_and_released_by_raii() -> Result<()> {
        let mut client = manifest_test_client()?;
        let table = Arc::new(table_named("sqlserver_availability_replica_states")?);
        let template_batch = synthetic_batch(&table, "client", 8, 0, 0, &client.options)?;
        let version = "v1".to_owned();
        let (data, raw_arrow_bytes, encoded_bytes) =
            encode_with_sizes(&table, &version, template_batch.clone(), &client.options).await?;
        let template = EncodedBatch {
            table,
            version,
            template: template_batch,
            data: Arc::new(data),
            rows: 8,
            raw_arrow_bytes,
            encoded_bytes,
        };
        let max_message_bytes = client.options.max_message_bytes;
        client.options.max_message_bytes = 1;
        assert!(client.prepare_logical_batch(0, &template).await.is_err());
        assert_eq!(client.live_batch_slots.available_permits(), 1);

        client.options.max_message_bytes = max_message_bytes;
        let logical = client.prepare_logical_batch(0, &template).await?;
        assert_eq!(client.live_batch_slots.available_permits(), 0);
        drop(logical);
        assert_eq!(client.live_batch_slots.available_permits(), 1);
        Ok(())
    }

    async fn realistic_test_batch(client: &PreparedClient) -> Result<EncodedBatch> {
        let table = Arc::new(table_named("sqlserver_availability_replica_states")?);
        let template = synthetic_batch(&table, "client", 8, 0, 0, &client.options)?;
        let (data, raw_arrow_bytes, encoded_bytes) =
            encode_with_sizes(&table, "v1", template.clone(), &client.options).await?;
        Ok(EncodedBatch {
            table,
            version: "v1".to_owned(),
            template,
            data: Arc::new(data),
            rows: 8,
            raw_arrow_bytes,
            encoded_bytes,
        })
    }

    #[tokio::test]
    async fn expired_continuous_window_does_not_start_a_queued_realistic_batch() -> Result<()> {
        let client = Arc::new(manifest_test_client()?);
        let table = Arc::new(table_named("sqlserver_availability_replica_states")?);
        let template_batch = synthetic_batch(&table, "client", 8, 0, 0, &client.options)?;
        let (data, raw_arrow_bytes, encoded_bytes) =
            encode_with_sizes(&table, "v1", template_batch.clone(), &client.options).await?;
        let template = EncodedBatch {
            table,
            version: "v1".to_owned(),
            template: template_batch,
            data: Arc::new(data),
            rows: 8,
            raw_arrow_bytes,
            encoded_bytes,
        };
        let channel = client.options.endpoint("http://127.0.0.1:0")?.connect_lazy();
        let end_unix_ms = unix_ms()?.saturating_sub(1);
        let measurement = Measurement::new(end_unix_ms.saturating_sub(1), end_unix_ms, 2)?;
        let held = Arc::clone(&client.live_batch_slots).acquire_owned().await?;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = {
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                let _ = started_tx.send(());
                client.send_continuous_one(0, channel, &template, &measurement).await
            })
        };
        started_rx.await?;
        drop(held);
        let result = task.await?;
        assert!(
            matches!(result, Ok(false)),
            "expired measurement started a new batch instead of skipping: {result:?}"
        );
        assert_eq!(client.worker_batch_sequence[0].load(Ordering::Relaxed), 0);
        assert!(
            client
                .logical_batches
                .lock()
                .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?
                .is_empty()
        );
        assert_eq!(client.counters[0].attempted_rows.load(Ordering::Relaxed), 0);
        assert_eq!(client.live_batch_slots.available_permits(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn queued_realistic_work_stops_at_window_end_without_identity_or_send() -> Result<()> {
        let mut prepared = manifest_test_client()?;
        let workers = 64;
        let streams_per_worker = 16;
        let admitted_slots = 3;
        let configured_slots = workers * streams_per_worker;
        assert_eq!(configured_slots, 1_024);
        prepared.options.workers = workers;
        prepared.options.streams_per_worker = streams_per_worker;
        prepared.live_batch_slots = Arc::new(tokio::sync::Semaphore::new(admitted_slots));
        prepared.counters = (0..workers).map(|_| Counters::default()).collect();
        prepared.worker_next = (0..workers).map(|_| AtomicU64::new(0)).collect();
        prepared.worker_batch_sequence = (0..workers).map(|_| AtomicU64::new(0)).collect();
        let client = Arc::new(prepared);
        let template = Arc::new(realistic_test_batch(&client).await?);
        let channel = client.options.endpoint("http://127.0.0.1:0")?.connect_lazy();
        let start = unix_ms()?;
        let measurement = Arc::new(Measurement::new(start, start + 60_000, workers)?);
        let held = vec![
            Arc::clone(&client.live_batch_slots).acquire_owned().await?,
            Arc::clone(&client.live_batch_slots).acquire_owned().await?,
            Arc::clone(&client.live_batch_slots).acquire_owned().await?,
        ];
        let waiter_count = configured_slots - admitted_slots;
        let (queued_tx, mut queued_rx) = tokio::sync::mpsc::unbounded_channel();
        let (end_tx, end_rx) = tokio::sync::watch::channel(false);
        let open = Arc::new(AtomicBool::new(true));
        let mut waiters = tokio::task::JoinSet::new();
        for index in 0..waiter_count {
            let client = Arc::clone(&client);
            let template = Arc::clone(&template);
            let measurement = Arc::clone(&measurement);
            let slots = Arc::clone(&client.live_batch_slots);
            let queued_tx = queued_tx.clone();
            let mut end_rx = end_rx.clone();
            let open = Arc::clone(&open);
            let worker = index / streams_per_worker;
            let channel = channel.clone();
            let acquire_permit = async move {
                let waiting = slots.acquire_owned();
                queued_tx
                    .send(())
                    .map_err(|_| anyhow::anyhow!("permit waiter observation was dropped"))?;
                waiting.await.context("acquire realistic live-batch memory reservation")
            };
            let window_end = async move {
                end_rx.changed().await.context("window-end signal was dropped")?;
                Ok::<(), anyhow::Error>(())
            };
            let window_open = move || Ok::<bool, anyhow::Error>(open.load(Ordering::SeqCst));
            waiters.spawn(async move {
                client
                    .send_continuous_one_with_admission(
                        worker,
                        channel.clone(),
                        &template,
                        &measurement,
                        ContinuousAdmission {
                            acquire_permit,
                            window_end,
                            window_open,
                        },
                    )
                    .await
            });
        }
        drop(queued_tx);
        for _ in 0..waiter_count {
            queued_rx
                .recv()
                .await
                .context("not all realistic waiters reached semaphore admission")?;
        }
        open.store(false, Ordering::SeqCst);
        end_tx
            .send(true)
            .context("queued realistic waiters were not subscribed to window end")?;
        while let Some(result) = waiters.join_next().await {
            assert!(!result??);
        }
        drop(held);
        assert_eq!(client.live_batch_slots.available_permits(), admitted_slots);
        assert_eq!(
            client
                .worker_batch_sequence
                .iter()
                .map(|value| value.load(Ordering::Relaxed))
                .sum::<u64>(),
            0
        );
        assert!(
            client
                .logical_batches
                .lock()
                .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?
                .is_empty()
        );
        assert_eq!(
            client
                .counters
                .iter()
                .map(|counter| counter.attempted_rows.load(Ordering::Relaxed))
                .sum::<u64>(),
            0
        );
        assert_eq!(
            client
                .counters
                .iter()
                .map(|counter| counter.attempted_bytes.load(Ordering::Relaxed))
                .sum::<u64>(),
            0
        );
        eprintln!(
            "W2_END_AWARE configured_slots={configured_slots} admitted_slots={admitted_slots} queued_waiters={waiter_count} post_end_starts=0"
        );
        Ok(())
    }

    #[tokio::test]
    async fn permit_ready_at_window_end_is_released_before_identity_allocation() -> Result<()> {
        let client = Arc::new(manifest_test_client()?);
        let template = realistic_test_batch(&client).await?;
        let channel = client.options.endpoint("http://127.0.0.1:0")?.connect_lazy();
        let start = unix_ms()?;
        let measurement = Measurement::new(start, start + 60_000, 2)?;
        let slots = Arc::clone(&client.live_batch_slots);
        let acquire_permit = async move {
            slots
                .acquire_owned()
                .await
                .context("acquire realistic live-batch memory reservation")
        };
        let window_end = std::future::pending::<Result<()>>();
        let checks = Arc::new(AtomicU64::new(0));
        let window_open = {
            let checks = Arc::clone(&checks);
            move || Ok::<bool, anyhow::Error>(checks.fetch_add(1, Ordering::SeqCst) == 0)
        };
        assert!(
            !client
                .send_continuous_one_with_admission(
                    0,
                    channel,
                    &template,
                    &measurement,
                    ContinuousAdmission {
                        acquire_permit,
                        window_end,
                        window_open,
                    },
                )
                .await?
        );
        assert_eq!(checks.load(Ordering::SeqCst), 2);
        assert_eq!(client.live_batch_slots.available_permits(), 1);
        assert_eq!(client.worker_batch_sequence[0].load(Ordering::Relaxed), 0);
        assert!(
            client
                .logical_batches
                .lock()
                .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?
                .is_empty()
        );
        assert_eq!(client.counters[0].attempted_rows.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[tokio::test]
    async fn expired_window_skips_before_polling_permit_acquisition() -> Result<()> {
        let client = Arc::new(manifest_test_client()?);
        let template = realistic_test_batch(&client).await?;
        let channel = client.options.endpoint("http://127.0.0.1:0")?.connect_lazy();
        let start = unix_ms()?;
        let measurement = Measurement::new(start, start + 60_000, 2)?;
        let polled = Arc::new(AtomicBool::new(false));
        let acquire_permit = {
            let polled = Arc::clone(&polled);
            async move {
                polled.store(true, Ordering::SeqCst);
                Err(anyhow::anyhow!("expired window polled permit acquisition"))
            }
        };
        let window_end = std::future::pending::<Result<()>>();
        assert!(
            !client
                .send_continuous_one_with_admission(
                    0,
                    channel,
                    &template,
                    &measurement,
                    ContinuousAdmission {
                        acquire_permit,
                        window_end,
                        window_open: || Ok(false),
                    },
                )
                .await?
        );
        assert!(!polled.load(Ordering::SeqCst));
        assert_eq!(client.live_batch_slots.available_permits(), 1);
        assert_eq!(client.worker_batch_sequence[0].load(Ordering::Relaxed), 0);
        assert!(
            client
                .logical_batches
                .lock()
                .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?
                .is_empty()
        );
        assert_eq!(client.counters[0].attempted_rows.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_releases_a_prepared_realistic_batch_permit() -> Result<()> {
        let client = Arc::new(manifest_test_client()?);
        let template = realistic_test_batch(&client).await?;
        let (prepared_tx, prepared_rx) = tokio::sync::oneshot::channel();
        let task = {
            let client = Arc::clone(&client);
            tokio::spawn(async move {
                let logical = client.prepare_logical_batch(0, &template).await?;
                prepared_tx
                    .send(())
                    .map_err(|_| anyhow::anyhow!("prepared batch observer was dropped"))?;
                std::future::pending::<()>().await;
                drop(logical);
                Ok::<(), anyhow::Error>(())
            })
        };
        prepared_rx.await.context("logical batch was not prepared")?;
        assert_eq!(client.live_batch_slots.available_permits(), 0);
        task.abort();
        let join_error = match task.await {
            Ok(result) => anyhow::bail!("cancelled prepared task completed normally: {result:?}"),
            Err(error) => error,
        };
        assert!(join_error.is_cancelled());
        assert_eq!(client.live_batch_slots.available_permits(), 1);
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
        let version = "v1".to_owned();
        let (data, raw_arrow_bytes, encoded_bytes) =
            encode_with_sizes(&table, &version, record.clone(), &options).await?;
        let channel = options.endpoint("http://127.0.0.1:0")?.connect_lazy();
        let client = Arc::new(PreparedClient {
            corpus: vec![Arc::new(vec![EncodedBatch {
                table: Arc::clone(&table),
                version,
                template: record,
                data: Arc::new(data),
                rows: 8,
                raw_arrow_bytes,
                encoded_bytes,
            }])],
            channels: vec![vec![channel]],
            live_batch_slots: Arc::new(tokio::sync::Semaphore::new(0)),
            signer: None,
            counters: vec![Counters::default()],
            next: AtomicU64::new(0),
            worker_next: vec![AtomicU64::new(0)],
            worker_batch_sequence: vec![AtomicU64::new(0)],
            reserved_rows: AtomicU64::new(0),
            measurement: Mutex::new(None),
            run_id: uuid::Uuid::new_v4(),
            incarnation_id: uuid::Uuid::new_v4(),
            logical_batches: Mutex::new(Vec::new()),
            manifest_capacity_reached: AtomicBool::new(false),
            terminal_failure_reason: Mutex::new(None),
            table_count: 1,
            preparation: serde_json::json!({}),
            started: Instant::now(),
            options,
        });
        assert!(client.send_round().await.is_err());
        assert_eq!(client.send_round().await?.completed_rows, 0);
        let metrics = client.metrics("failed")?;
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
        assert!(read_advertised_link_speed(Err(std::io::Error::other("unexpected"))).is_err());
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
        assert_eq!(snapshot["completed_flight_data_bytes_uncompressed"], 4500);
        assert_eq!(snapshot["completed_flight_data_bytes_configured_ipc_compression"], 5000);
        assert_eq!(snapshot["worker_completed_rows"], serde_json::json!([500, 0]));
        assert_eq!(snapshot["pilot_window_rows"], serde_json::json!([200, 300]));
        assert_eq!(snapshot["windows"][0]["completed_raw_arrow_bytes"], 1800);
        assert_eq!(snapshot["windows"][0]["completed_flight_data_bytes_uncompressed"], 1800);
        assert_eq!(
            snapshot["windows"][0]["completed_flight_data_bytes_configured_ipc_compression"],
            2000
        );
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
                        version: String::new(),
                        template: RecordBatch::new_empty(Arc::clone(table.schema())),
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
            live_batch_slots: Arc::new(tokio::sync::Semaphore::new(0)),
            signer: None,
            counters: vec![],
            next: AtomicU64::new(0),
            worker_next: vec![AtomicU64::new(0), AtomicU64::new(0)],
            worker_batch_sequence: vec![AtomicU64::new(0), AtomicU64::new(0)],
            reserved_rows: AtomicU64::new(0),
            measurement: Mutex::new(None),
            run_id: uuid::Uuid::new_v4(),
            incarnation_id: uuid::Uuid::new_v4(),
            logical_batches: Mutex::new(Vec::new()),
            manifest_capacity_reached: AtomicBool::new(false),
            terminal_failure_reason: Mutex::new(None),
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
            RuntimeLimits {
                grpc_accept_compression: Some(tonic::codec::CompressionEncoding::Gzip),
                grpc_send_compression: Some(tonic::codec::CompressionEncoding::Gzip),
                ..RuntimeLimits::default()
            },
        )?
        .with_ddl_handler(Arc::new(CatalogControlPlane::new(DdlParser::default(), store)))
        .into_tonic_service();
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
        options.data_profile = DataProfile::Realistic;
        options.null_fraction = 1.0;
        options.send_iter = SendIter::ArcCursor;
        options.identity_builder = IdentityBuilder::DirectUtf8;
        let client = PreparedClient::prepare(
            &endpoint,
            "smoke",
            Some("sqlserver_availability_replica_states"),
            None,
            options,
        )
        .await?;
        assert_eq!(client.send_once().await?.completed_rows, 8);
        assert_eq!(state.rows.load(Ordering::Relaxed), 8);
        state
            .identities
            .lock()
            .map_err(|_| anyhow::anyhow!("smoke identity lock poisoned"))?
            .clear();
        state.capture_identities.store(true, Ordering::Relaxed);
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
        let metrics = client.metrics("completed")?;
        let applicability = &metrics["preparation"]["generator_applicability"]["tables"][0];
        assert_eq!(applicability["table"], "sqlserver_availability_replica_states");
        assert_eq!(applicability["rows"], 8);
        let eligible_columns = applicability["null_fraction"]["eligible_columns"]
            .as_u64()
            .context("eligible columns")?;
        assert_eq!(eligible_columns, 0);
        assert_eq!(applicability["null_fraction"]["eligible_rows"], eligible_columns * 8);
        assert_eq!(applicability["null_fraction"]["null_rows"], eligible_columns * 8);
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
        let (retried_worker, retried_sequence) = {
            let records = client
                .logical_batches
                .lock()
                .map_err(|_| anyhow::anyhow!("logical batch manifest state poisoned"))?;
            let record = records
                .iter()
                .find(|record| record.attempts > 1 && record.outcome == ManifestOutcome::Acknowledged)
                .context("missing acknowledged retry record")?;
            (usize::from(record.worker), record.sequence)
        };
        let retried_row_identity = logical_identity_url(
            client.run_id,
            client.incarnation_id,
            retried_worker,
            retried_sequence,
            0,
        )?;
        let retried_identity_count = {
            let observed_identities = state
                .identities
                .lock()
                .map_err(|_| anyhow::anyhow!("smoke identity lock poisoned"))?;
            observed_identities
                .iter()
                .filter(|identity| identity.as_str() == retried_row_identity.as_str())
                .count()
        };
        assert_eq!(
            retried_identity_count, 1,
            "the acknowledged retry must contain the original logical batch identity exactly once"
        );
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
        let mut limited_options = Cli::try_parse_from(["test", "--workers", "1"])?
            .options
            .resolve(100, 1)?;
        limited_options.workers = 1;
        limited_options.corpus_rows = 16;
        limited_options.batch_rows = 8;
        limited_options.streams_per_worker = 1;
        limited_options.max_rows = 16;
        limited_options.rate_limit_rps = 1;
        limited_options.rate_limit_period_ms = 3000;
        let limited = PreparedClient::prepare(
            &endpoint,
            "rate-limited",
            Some("sqlserver_availability_replica_states"),
            None,
            limited_options,
        )
        .await?;
        let rows_before_limit = state.rows.load(Ordering::Relaxed);
        assert!(
            tokio::time::timeout(Duration::from_millis(1500), limited.send_once())
                .await
                .is_err(),
            "two requests completed before the configured rate-limit period"
        );
        assert_eq!(state.rows.load(Ordering::Relaxed) - rows_before_limit, 8);
        drop(limited);
        shutdown
            .send(())
            .map_err(|()| anyhow::anyhow!("smoke server stopped early"))?;
        tokio::time::timeout(Duration::from_secs(2), server).await???;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "explicit client-ceiling measurement against an in-memory Flight sink"]
    async fn null_flight_sink_client_ceiling_across_worker_counts() -> Result<()> {
        use arrowcol_catalog::{CatalogControlPlane, CatalogStore, MemoryCatalogStore};
        use arrowcol_flight::{AllowAllAuthenticator, ArrowColFlightService, RuntimeLimits};
        use arrowcol_protocol::{AuthContext, AuthMethod, AuthenticatedPrincipal, PrincipalId, PrincipalIssuer};
        let store: Arc<dyn CatalogStore> = Arc::new(MemoryCatalogStore::new());
        let principal = AuthenticatedPrincipal::new(
            PrincipalId::new("ceiling")?,
            PrincipalIssuer::new("ceiling")?,
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
        .into_tonic_service();
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

        for workers in [1, 2, 4, 8, 16, 32] {
            let mut options = options()?;
            options.data_profile = DataProfile::Realistic;
            options.workers = workers;
            options.corpus_rows = 4_096;
            options.batch_rows = 4_096;
            options.streams_per_worker = 2;
            options.interval_us = 0;
            let client = PreparedClient::prepare(
                &endpoint,
                "ceiling",
                Some("sqlserver_availability_replica_states"),
                None,
                options,
            )
            .await?;
            let scheduled_start = unix_ms()? + 100;
            let measurement_start = scheduled_start + 50;
            let measurement_end = measurement_start + 1_000;
            client
                .run_continuous(scheduled_start, measurement_start, measurement_end)
                .await?;
            let snapshot = client.metrics("completed")?;
            client.ensure_terminal_manifest_complete()?;
            let window = &snapshot["measurement_window"];
            let rows = window["completed_rows"].as_u64().context("missing completed rows")?;
            let uncompressed_bytes = window["completed_flight_data_bytes_uncompressed"]
                .as_u64()
                .context("missing uncompressed FlightData bytes")?;
            let configured_bytes = window["completed_flight_data_bytes_configured_ipc_compression"]
                .as_u64()
                .context("missing configured-compression FlightData bytes")?;
            let measurement_seconds = (measurement_end - measurement_start) as f64 / 1000.0;
            eprintln!(
                "W2_CLIENT_SINK_PROBE tokio_runtime_threads=8 workers={workers} aggregate_rows_per_second={:.3} uncompressed_flight_data_GB_per_second={:.6} configured_ipc_compression_flight_data_GB_per_second={:.6}",
                rows as f64 / measurement_seconds,
                uncompressed_bytes as f64 / 1_000_000_000.0 / measurement_seconds,
                configured_bytes as f64 / 1_000_000_000.0 / measurement_seconds,
            );
            assert!(rows > 0, "null Flight sink received no rows with {workers} workers");
        }

        shutdown
            .send(())
            .map_err(|()| anyhow::anyhow!("null sink stopped early"))?;
        tokio::time::timeout(Duration::from_secs(2), server).await???;
        Ok(())
    }
}
