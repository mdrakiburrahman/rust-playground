use std::time::Duration;

use anyhow::{Result, ensure};
use clap::{Args, ValueEnum};
use serde::Serialize;
use tonic::transport::Endpoint;

/// IPC compression supported by this client build; receiver support is required.
#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum IpcCompression {
    #[default]
    None,
    #[value(name = "lz4_frame")]
    Lz4Frame,
    Zstd,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        tuning: ClientTuning,
    }

    #[test]
    fn rejects_bad_enums_nonfinite_entropy_and_unsafe_capacity() {
        for args in [
            vec!["--ipc-compression", "gzip"],
            vec!["--data-mode", "replicated"],
            vec!["--entropy", "NaN"],
            vec!["--precompute-batches-per-table", "17"],
            vec!["--connections-per-worker", "0"],
            vec!["--max-memory-bytes", "0"],
            vec!["--max-flight-data-bytes", "33554433"],
            vec!["--stream-window-bytes", "2147483648"],
            vec!["--retry-initial-backoff-ms", "2001"],
        ] {
            assert!(
                Cli::try_parse_from(std::iter::once("test").chain(args))
                    .map(|cli| cli.tuning.validate(1, 8192).is_err())
                    .unwrap_or(true)
            );
        }
        assert!(
            ClientTuning {
                string_width: 65536,
                ..ClientTuning::default()
            }
            .validate(1, 65536)
            .is_err()
        );
    }
}

/// Allocation ownership; independent copies retain identical proof-compatible values.
#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum DataMode {
    #[default]
    Shared,
    Independent,
}

/// Startup-only knobs for the proof-producing standalone workload.
#[derive(Clone, Debug, Args, Serialize)]
pub struct ClientTuning {
    /// Cache this many variants of the existing 16-seed cycle per table; zero keeps on-demand generation.
    #[arg(long, env = "ARROWCOL_CLIENT_PRECOMPUTE_BATCHES_PER_TABLE", default_value_t = 0)]
    pub precompute_batches_per_table: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_MAX_MEMORY_BYTES", default_value_t = 536_870_912)]
    pub max_memory_bytes: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_DATA_MODE", value_enum, default_value_t = DataMode::Shared)]
    pub data_mode: DataMode,
    #[arg(long, env = "ARROWCOL_CLIENT_INTERVAL_US", default_value_t = 0)]
    pub interval_us: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_RUNTIME_THREADS", default_value_t = 0)]
    pub runtime_threads: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_CONNECTIONS_PER_WORKER", default_value_t = 1)]
    pub connections_per_worker: usize,
    /// Non-GUID, non-customer string width; zero preserves the canonical fallback representation.
    #[arg(long, env = "ARROWCOL_CLIENT_STRING_WIDTH", default_value_t = 0)]
    pub string_width: usize,
    /// Fraction of fixed-width string positions varying across seed variants, never across rows.
    #[arg(long, env = "ARROWCOL_CLIENT_ENTROPY", default_value_t = 1.0)]
    pub entropy: f64,
    #[arg(long, env = "ARROWCOL_CLIENT_IPC_COMPRESSION", value_enum, default_value_t = IpcCompression::None)]
    pub ipc_compression: IpcCompression,
    #[arg(long, env = "ARROWCOL_CLIENT_MAX_FLIGHT_DATA_BYTES", default_value_t = 32_505_856)]
    pub max_flight_data_bytes: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_MAX_MESSAGE_BYTES", default_value_t = 33_554_432)]
    pub max_message_bytes: usize,
    /// Zero inherits the existing --request-timeout.
    #[arg(long, env = "ARROWCOL_CLIENT_CONNECT_TIMEOUT_MS", default_value_t = 0)]
    pub connect_timeout_ms: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_STREAM_WINDOW_BYTES", default_value_t = 1_048_576)]
    pub stream_window_bytes: u32,
    #[arg(long, env = "ARROWCOL_CLIENT_CONNECTION_WINDOW_BYTES", default_value_t = 16_777_216)]
    pub connection_window_bytes: u32,
    #[arg(long, env = "ARROWCOL_CLIENT_ADAPTIVE_WINDOW", default_value_t = false, action = clap::ArgAction::Set)]
    pub adaptive_window: bool,
    #[arg(long, env = "ARROWCOL_CLIENT_KEEPALIVE_INTERVAL_MS", default_value_t = 10_000)]
    pub keepalive_interval_ms: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_KEEPALIVE_TIMEOUT_MS", default_value_t = 20_000)]
    pub keepalive_timeout_ms: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_KEEPALIVE_WHILE_IDLE", default_value_t = true, action = clap::ArgAction::Set)]
    pub keepalive_while_idle: bool,
    #[arg(long, env = "ARROWCOL_CLIENT_TCP_NODELAY", default_value_t = true, action = clap::ArgAction::Set)]
    pub tcp_nodelay: bool,
    #[arg(long, env = "ARROWCOL_CLIENT_ENDPOINT_CONCURRENCY_LIMIT", default_value_t = 256)]
    pub endpoint_concurrency_limit: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_ENDPOINT_BUFFER_SIZE", default_value_t = 1024)]
    pub endpoint_buffer_size: usize,
    #[arg(long, env = "ARROWCOL_CLIENT_RETRY_INITIAL_BACKOFF_MS", default_value_t = 100)]
    pub retry_initial_backoff_ms: u64,
    #[arg(long, env = "ARROWCOL_CLIENT_RETRY_MAX_BACKOFF_MS", default_value_t = 2000)]
    pub retry_max_backoff_ms: u64,
}

impl Default for ClientTuning {
    fn default() -> Self {
        Self {
            precompute_batches_per_table: 0,
            max_memory_bytes: 536_870_912,
            data_mode: DataMode::Shared,
            interval_us: 0,
            runtime_threads: 0,
            connections_per_worker: 1,
            string_width: 0,
            entropy: 1.0,
            ipc_compression: IpcCompression::None,
            max_flight_data_bytes: 32_505_856,
            max_message_bytes: 33_554_432,
            connect_timeout_ms: 0,
            stream_window_bytes: 1_048_576,
            connection_window_bytes: 16_777_216,
            adaptive_window: false,
            keepalive_interval_ms: 10_000,
            keepalive_timeout_ms: 20_000,
            keepalive_while_idle: true,
            tcp_nodelay: true,
            endpoint_concurrency_limit: 256,
            endpoint_buffer_size: 1024,
            retry_initial_backoff_ms: 100,
            retry_max_backoff_ms: 2000,
        }
    }
}

impl ClientTuning {
    /// Rejects invalid codec/resource settings before the run creates artifacts or connects.
    ///
    /// # Errors
    ///
    /// Returns an explicit range or incompatible resource-budget error.
    pub fn validate(&self, lanes: usize, rows: usize) -> Result<()> {
        let variants = delta_inspect::MAX_CHUNK_DISTINCT_ROWS / crate::generator::CUSTOMER_COUNT;
        ensure!(
            self.precompute_batches_per_table <= variants,
            "precompute-batches-per-table must be 0..={variants}"
        );
        ensure!(
            (1..=1024).contains(&lanes) && self.runtime_threads <= 1024,
            "lanes must resolve to 1..=1024; runtime-threads must be 0..=1024"
        );
        ensure!(
            (1..=256).contains(&self.connections_per_worker)
                && lanes.saturating_mul(self.connections_per_worker) <= 4096,
            "connections-per-worker must be 1..=256 and total connections <=4096"
        );
        ensure!(
            rows > 0 && rows <= i32::MAX as usize,
            "batch-rows must be 1..=2147483647"
        );
        ensure!(
            self.max_memory_bytes > 0 && self.max_memory_bytes <= isize::MAX as u64,
            "max-memory-bytes must be positive and fit the address space"
        );
        ensure!(
            self.string_width <= 65536 && self.entropy.is_finite() && (0.0..=1.0).contains(&self.entropy),
            "string-width must be 0..=65536 and entropy finite in 0..=1"
        );
        ensure!(
            rows.checked_mul(self.string_width)
                .is_some_and(|bytes| bytes <= i32::MAX as usize),
            "batch-rows * string-width exceeds Arrow Utf8's 2147483647-byte column limit"
        );
        ensure!(
            self.max_flight_data_bytes >= 1024
                && self.max_flight_data_bytes <= self.max_message_bytes
                && self.max_message_bytes <= i32::MAX as usize,
            "require 1024 <= max-flight-data-bytes <= max-message-bytes <=2147483647"
        );
        ensure!(
            (1..=i32::MAX as u32).contains(&self.stream_window_bytes)
                && (1..=i32::MAX as u32).contains(&self.connection_window_bytes),
            "HTTP/2 windows must be 1..=2147483647"
        );
        ensure!(
            (1..=65536).contains(&self.endpoint_buffer_size) && (1..=65536).contains(&self.endpoint_concurrency_limit),
            "endpoint buffer and concurrency limits must be 1..=65536"
        );
        ensure!(
            self.keepalive_timeout_ms > 0 && self.retry_initial_backoff_ms <= self.retry_max_backoff_ms,
            "keepalive timeout must be positive; retry initial backoff must not exceed its cap"
        );
        ensure!(
            self.interval_us <= 86_400_000_000,
            "interval-us must not exceed one day"
        );
        for value in [
            self.connect_timeout_ms,
            self.keepalive_interval_ms,
            self.keepalive_timeout_ms,
            self.retry_max_backoff_ms,
        ] {
            ensure!(
                value <= 86_400_000,
                "client millisecond durations must not exceed one day"
            );
        }
        Ok(())
    }

    pub(crate) fn endpoint(&self, address: &str, timeout: Duration) -> Result<Endpoint> {
        let mut endpoint = Endpoint::from_shared(address.to_owned())?
            .connect_timeout(if self.connect_timeout_ms == 0 {
                timeout
            } else {
                Duration::from_millis(self.connect_timeout_ms)
            })
            .timeout(timeout)
            .initial_stream_window_size(self.stream_window_bytes)
            .initial_connection_window_size(self.connection_window_bytes)
            .http2_adaptive_window(self.adaptive_window)
            .keep_alive_timeout(Duration::from_millis(self.keepalive_timeout_ms))
            .keep_alive_while_idle(self.keepalive_while_idle)
            .tcp_nodelay(self.tcp_nodelay)
            .concurrency_limit(self.endpoint_concurrency_limit)
            .buffer_size(self.endpoint_buffer_size);
        if self.keepalive_interval_ms > 0 {
            endpoint = endpoint.http2_keep_alive_interval(Duration::from_millis(self.keepalive_interval_ms));
        }
        Ok(endpoint)
    }
}
