use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use arrow_deltalake::commit::CommitOptions;
use arrow_deltalake::sizing::AdaptiveFileSizeConfig;
use arrowcol_buffer::{BufferConfig, CapacityLimits, QuiverStageConfig};
use arrowcol_flight::RuntimeLimits;
use arrowcol_protocol::{CollectorInstanceId, ImplementationPool};
use clap::Parser;
use serde::Deserialize;
use thiserror::Error;

/// Command-line options for the direct collector.
#[derive(Clone, Debug, Parser)]
#[command(about = "Production direct Arrow Flight to Delta collector")]
pub struct Cli {
    /// Strict YAML configuration file.
    #[arg(long, default_value = "projects/arrowcol-rust/config.yaml")]
    pub config: PathBuf,
    /// Parse and validate configuration without opening dependencies or listeners.
    #[arg(long)]
    pub validate_and_exit: bool,
}

/// Configuration loading or capability failure.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The configuration file could not be read.
    #[error("read configuration {path}: {source}")]
    Read {
        /// Configuration path.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// Strict YAML parsing failed.
    #[error("parse strict YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// A configured value or relationship is invalid.
    #[error("invalid configuration: {0}")]
    Invalid(String),
    /// A requested optional runtime capability is unavailable.
    #[error("unsupported capability {capability}: {reason}")]
    UnsupportedCapability {
        /// Stable capability name.
        capability: &'static str,
        /// Safe explanation.
        reason: String,
    },
}

/// Strict production collector configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectorConfig {
    /// Flight listener and transport limits.
    pub flight: FlightConfig,
    /// HTTP administration listener.
    pub admin: AdminConfig,
    /// Process identity and implementation pool.
    pub identity: IdentityConfig,
    /// SQL Server application database.
    pub sql_server: SqlServerConfig,
    /// Hive Metastore control-plane endpoint.
    pub hive: HiveConfig,
    /// Local durable Delta publication.
    pub delta: DeltaConfig,
    /// Bounded Arrow buffers and adaptive flush policy.
    pub buffer: BufferSettings,
    /// Router registration, pressure, and internal trust.
    pub routing: RoutingConfig,
    /// Owner, receipt, and publication lease policy.
    pub receipts: ReceiptConfig,
    /// Request authentication.
    pub auth: AuthConfig,
    /// Graceful shutdown ceilings.
    pub shutdown: ShutdownConfig,
    /// Logging configuration.
    pub telemetry: TelemetryConfig,
}

impl CollectorConfig {
    /// Loads and validates a strict YAML document.
    ///
    /// # Errors
    ///
    /// Returns a read, YAML, bounds, or capability error.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let mut config: Self = serde_yaml::from_slice(&bytes)?;
        config
            .delta
            .override_root(std::env::var_os("ARROWCOL_DELTA_ROOT").map(PathBuf::from));
        config.validate()?;
        Ok(config)
    }

    /// Parses and validates strict YAML bytes.
    ///
    /// # Errors
    ///
    /// Returns a YAML, bounds, or capability error.
    pub fn from_yaml(bytes: &[u8]) -> Result<Self, ConfigError> {
        let config: Self = serde_yaml::from_slice(bytes)?;
        config.validate()?;
        Ok(config)
    }

    /// Validates every local and cross-field invariant.
    ///
    /// # Errors
    ///
    /// Returns the first invalid bound or unsupported capability.
    pub fn validate(&self) -> Result<(), ConfigError> {
        positive(self.flight.max_concurrent_streams, "flight.max_concurrent_streams")?;
        positive(self.flight.max_metadata_bytes, "flight.max_metadata_bytes")?;
        positive(self.flight.max_schema_bytes, "flight.max_schema_bytes")?;
        positive(self.flight.max_message_bytes, "flight.max_message_bytes")?;
        positive(self.flight.batch_channel_capacity, "flight.batch_channel_capacity")?;
        positive(
            self.flight.put_result_channel_capacity,
            "flight.put_result_channel_capacity",
        )?;
        positive(self.flight.max_stream_duration_ms, "flight.max_stream_duration_ms")?;
        positive(self.flight.admission_timeout_ms, "flight.admission_timeout_ms")?;
        positive(self.flight.max_ddl_bytes, "flight.max_ddl_bytes")?;
        if self.flight.max_schema_bytes > self.flight.max_message_bytes {
            return Err(invalid("flight.max_schema_bytes must not exceed max_message_bytes"));
        }
        self.flight
            .runtime_limits()?
            .validate()
            .map_err(|error| invalid(format!("flight runtime limits are invalid: {}", error.public_message())))?;
        self.flight.tls.validate()?;

        positive(self.admin.max_receipt_ids, "admin.max_receipt_ids")?;
        if self.admin.max_receipt_ids > 512 {
            return Err(invalid("admin.max_receipt_ids must be in 1..=512"));
        }

        if self.identity.implementation_pool != ImplementationPool::Direct {
            return Err(invalid("identity.implementation_pool must be direct"));
        }
        nonempty(&self.identity.instance_key, "identity.instance_key")?;
        if self.identity.instance_key.len() > 200 || self.identity.instance_key.chars().any(char::is_control) {
            return Err(invalid(
                "identity.instance_key must be 1..=200 bytes without control characters",
            ));
        }
        positive(self.identity.generation, "identity.generation")?;

        self.sql_server.validate()?;
        self.hive.validate()?;
        self.delta.validate()?;
        self.buffer
            .buffer_config()?
            .validate()
            .map_err(|error| invalid(format!("buffer settings are invalid: {}", error.message())))?;
        self.buffer.native_quiver.validate()?;
        self.routing.validate(self.flight.max_concurrent_streams)?;
        self.receipts.validate()?;
        self.auth.validate(&self.routing, &self.flight.tls)?;
        self.shutdown.validate()?;
        nonempty(&self.telemetry.log_filter, "telemetry.log_filter")?;
        Ok(())
    }

    /// Returns the configured or newly generated process-lifetime identifier.
    #[must_use]
    pub fn instance_id(&self) -> CollectorInstanceId {
        self.identity.instance_id.unwrap_or_else(CollectorInstanceId::new)
    }
}

/// Flight listener and reusable runtime limits.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlightConfig {
    /// Flight bind address.
    pub bind: SocketAddr,
    /// Endpoint advertised to routers.
    pub advertised_endpoint: SocketAddr,
    /// Maximum concurrent inbound streams.
    pub max_concurrent_streams: u64,
    /// Aggregate inspected gRPC metadata bound.
    pub max_metadata_bytes: usize,
    /// Encoded Arrow schema bound.
    pub max_schema_bytes: usize,
    /// Per-message protobuf decoding bound.
    pub max_message_bytes: usize,
    /// Bounded decoded batch channel capacity.
    pub batch_channel_capacity: usize,
    /// Bounded response `PutResult` channel capacity.
    pub put_result_channel_capacity: usize,
    /// Maximum stream lifetime.
    pub max_stream_duration_ms: u64,
    /// Maximum one-batch admission wait.
    pub admission_timeout_ms: u64,
    /// Maximum Flight SQL DDL command bytes.
    pub max_ddl_bytes: usize,
    /// TLS listener configuration.
    pub tls: TlsConfig,
}

impl FlightConfig {
    /// Converts YAML values to shared Flight limits.
    ///
    /// # Errors
    ///
    /// Rejects values that cannot be represented by `Duration`.
    pub fn runtime_limits(&self) -> Result<RuntimeLimits, ConfigError> {
        Ok(RuntimeLimits {
            max_metadata_bytes: self.max_metadata_bytes,
            max_schema_bytes: self.max_schema_bytes,
            max_message_bytes: self.max_message_bytes,
            batch_channel_capacity: self.batch_channel_capacity,
            put_result_channel_capacity: self.put_result_channel_capacity,
            max_stream_duration: duration_ms(self.max_stream_duration_ms, "flight.max_stream_duration_ms")?,
            admission_timeout: duration_ms(self.admission_timeout_ms, "flight.admission_timeout_ms")?,
            max_ddl_bytes: self.max_ddl_bytes,
        })
    }
}

/// Optional Flight TLS and client-certificate enforcement.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Enable TLS on the Flight listener.
    pub enabled: bool,
    /// PEM certificate chain.
    pub certificate_file: Option<PathBuf>,
    /// PEM private key.
    pub private_key_file: Option<PathBuf>,
    /// Optional PEM client CA; when present, client certificates are required.
    pub client_ca_file: Option<PathBuf>,
}

impl TlsConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.enabled {
            required_path(&self.certificate_file, "flight.tls.certificate_file")?;
            required_path(&self.private_key_file, "flight.tls.private_key_file")?;
        } else if self.certificate_file.is_some() || self.private_key_file.is_some() || self.client_ca_file.is_some() {
            return Err(invalid("Flight TLS files require flight.tls.enabled=true"));
        }
        Ok(())
    }
}

/// HTTP administration settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfig {
    /// Admin HTTP bind address.
    pub bind: SocketAddr,
    /// Maximum IDs accepted by one receipt query.
    pub max_receipt_ids: usize,
}

/// Process identity.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityConfig {
    /// Optional fixed process UUID; omitted generates a fresh UUID at startup.
    pub instance_id: Option<CollectorInstanceId>,
    /// Stable deployment/replica key.
    pub instance_key: String,
    /// Positive deployment generation.
    pub generation: u64,
    /// Must be `direct`.
    pub implementation_pool: ImplementationPool,
    /// Optional operator host label.
    pub host_name: Option<String>,
    /// Optional build label.
    pub build_version: Option<String>,
}

/// Shared SQL Server endpoint.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlServerConfig {
    /// SQL Server host.
    pub host: String,
    /// SQL Server port.
    pub port: u16,
    /// Arrowcol application database.
    pub database: String,
    /// Runtime SQL principal.
    pub username: String,
    /// One-line password secret file.
    pub password_file: PathBuf,
    /// Explicit local-development certificate bypass.
    pub trust_server_certificate: bool,
    /// TCP/login timeout.
    pub connect_timeout_ms: u64,
    /// SQL command timeout.
    pub command_timeout_ms: u64,
    /// Separate bounded connection pools for SQL operation roles.
    #[serde(default)]
    pub pool: SqlPoolSettings,
}

impl SqlServerConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        nonempty(&self.host, "sql_server.host")?;
        nonempty(&self.database, "sql_server.database")?;
        nonempty(&self.username, "sql_server.username")?;
        nonempty_path(&self.password_file, "sql_server.password_file")?;
        positive(self.connect_timeout_ms, "sql_server.connect_timeout_ms")?;
        positive(self.command_timeout_ms, "sql_server.command_timeout_ms")?;
        self.pool.validate()
    }
}

/// Per-role SQL connection budgets and checkout deadline.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SqlPoolSettings {
    /// Maximum receipt connections, shared by admission and receipt control.
    pub receipts_max_size: usize,
    /// Maximum routing connections.
    pub routing_max_size: usize,
    /// Maximum catalog connections.
    pub catalog_max_size: usize,
    /// Maximum wait for a connection checkout.
    pub acquire_timeout_ms: u64,
}

impl Default for SqlPoolSettings {
    fn default() -> Self {
        Self {
            receipts_max_size: 16,
            routing_max_size: 4,
            catalog_max_size: 4,
            acquire_timeout_ms: 5000,
        }
    }
}

impl SqlPoolSettings {
    fn validate(&self) -> Result<(), ConfigError> {
        for (size, name) in [
            (self.receipts_max_size, "sql_server.pool.receipts_max_size"),
            (self.routing_max_size, "sql_server.pool.routing_max_size"),
            (self.catalog_max_size, "sql_server.pool.catalog_max_size"),
        ] {
            positive(size, name)?;
            if size > tokio::sync::Semaphore::MAX_PERMITS {
                return Err(invalid(format!("{name} exceeds the supported connection limit")));
            }
        }
        positive(self.acquire_timeout_ms, "sql_server.pool.acquire_timeout_ms")?;
        if std::time::Instant::now()
            .checked_add(Duration::from_millis(self.acquire_timeout_ms))
            .is_none()
        {
            return Err(invalid(
                "sql_server.pool.acquire_timeout_ms exceeds the supported deadline",
            ));
        }
        Ok(())
    }
}

/// Hive Metastore endpoint.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HiveConfig {
    /// Thrift host.
    pub host: String,
    /// Thrift port.
    pub port: u16,
    /// External-table warehouse URI.
    pub warehouse_root: String,
    /// Hive table owner.
    pub owner: String,
    /// TCP/read/write timeout.
    pub connect_timeout_ms: u64,
}

impl HiveConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        nonempty(&self.host, "hive.host")?;
        nonempty(&self.warehouse_root, "hive.warehouse_root")?;
        nonempty(&self.owner, "hive.owner")?;
        positive(self.connect_timeout_ms, "hive.connect_timeout_ms")
    }
}

/// Local Delta table root and commit batching.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaConfig {
    /// Root containing one Delta directory per catalog/schema/table.
    pub root: PathBuf,
    /// Maximum collection window before a partial commit.
    pub commit_batch_window_ms: u64,
    /// Maximum data files in one Delta transaction.
    pub max_files_per_commit: usize,
    /// Maximum queued and committing files per table.
    pub max_pending_files: usize,
}

impl DeltaConfig {
    fn override_root(&mut self, root: Option<PathBuf>) {
        if let Some(root) = root {
            self.root = root;
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        nonempty_path(&self.root, "delta.root")?;
        let options = self.commit_options()?;
        options
            .validate()
            .map_err(|error| invalid(format!("Delta commit settings are invalid: {error}")))
    }

    /// Converts settings to the Delta commit coordinator options.
    ///
    /// # Errors
    ///
    /// Rejects invalid duration representation.
    pub fn commit_options(&self) -> Result<CommitOptions, ConfigError> {
        Ok(CommitOptions {
            batch_window: duration_ms(self.commit_batch_window_ms, "delta.commit_batch_window_ms")?,
            max_files_per_commit: self.max_files_per_commit,
            max_pending_files: self.max_pending_files,
        })
    }
}

/// Bounded capacities, flush policy, and optional native persistence.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferSettings {
    /// Process-wide retained/in-flight capacity.
    pub global_capacity: CapacityConfig,
    /// Per-table retained/in-flight capacity.
    pub per_table_capacity: CapacityConfig,
    /// Maximum logical Arrow bytes per flush group.
    pub max_arrow_memory_per_flush: u64,
    /// Maximum rows per flush group.
    pub max_rows_per_flush: u64,
    /// Maximum batches per flush group.
    pub max_batches_per_flush: u64,
    /// Maximum oldest queued age.
    pub max_age_ms: u64,
    /// Age-trigger scan interval.
    pub age_check_interval_ms: u64,
    /// Maximum concurrent table publication sessions.
    pub max_concurrent_flushes: usize,
    /// Receipt heartbeat horizon during buffer/flush transitions.
    pub receipt_lease_duration_ms: u64,
    /// Adaptive compressed-file sizing.
    pub adaptive_file: AdaptiveFileConfig,
    /// Optional native Quiver settings.
    pub native_quiver: NativeQuiverConfig,
}

impl BufferSettings {
    /// Converts strict YAML into the shared buffer configuration.
    ///
    /// # Errors
    ///
    /// Rejects invalid capacity or duration representation.
    pub fn buffer_config(&self) -> Result<BufferConfig, ConfigError> {
        Ok(BufferConfig {
            global_capacity: self.global_capacity.capacity("buffer.global_capacity")?,
            per_table_capacity: self.per_table_capacity.capacity("buffer.per_table_capacity")?,
            max_arrow_memory_per_flush: self.max_arrow_memory_per_flush,
            max_rows_per_flush: self.max_rows_per_flush,
            max_batches_per_flush: self.max_batches_per_flush,
            max_age: duration_ms(self.max_age_ms, "buffer.max_age_ms")?,
            age_check_interval: duration_ms(self.age_check_interval_ms, "buffer.age_check_interval_ms")?,
            max_concurrent_flushes: self.max_concurrent_flushes,
            receipt_lease_duration: duration_ms(self.receipt_lease_duration_ms, "buffer.receipt_lease_duration_ms")?,
            adaptive_size: self.adaptive_file.adaptive_config(),
        })
    }
}

/// Byte/row/batch capacity.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityConfig {
    /// Logical Arrow bytes.
    pub bytes: u64,
    /// Rows.
    pub rows: u64,
    /// Record batches.
    pub batches: u64,
}

impl CapacityConfig {
    fn capacity(self, name: &str) -> Result<CapacityLimits, ConfigError> {
        CapacityLimits::new(self.bytes, self.rows, self.batches)
            .map_err(|error| invalid(format!("{name} is invalid: {}", error.message())))
    }
}

/// Adaptive compressed-file target.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveFileConfig {
    /// Desired compressed Parquet bytes; 134,217,728 is 128 MiB.
    pub target_parquet_bytes: u64,
    /// Minimum logical Arrow input threshold.
    pub min_input_bytes: u64,
    /// Maximum logical Arrow input threshold.
    pub max_input_bytes: u64,
    /// Cold-start compressed/input ratio.
    pub cold_start_parquet_ratio: f64,
    /// EWMA weight.
    pub ewma_weight: f64,
    /// Minimum accepted ratio.
    pub min_parquet_ratio: f64,
    /// Maximum accepted ratio.
    pub max_parquet_ratio: f64,
}

impl AdaptiveFileConfig {
    const fn adaptive_config(self) -> AdaptiveFileSizeConfig {
        AdaptiveFileSizeConfig {
            target_parquet_bytes: self.target_parquet_bytes,
            min_input_bytes: self.min_input_bytes,
            max_input_bytes: self.max_input_bytes,
            cold_start_parquet_ratio: self.cold_start_parquet_ratio,
            ewma_weight: self.ewma_weight,
            min_parquet_ratio: self.min_parquet_ratio,
            max_parquet_ratio: self.max_parquet_ratio,
        }
    }
}

/// Native Quiver persistence: accepted batches are written to a local
/// write-ahead log before they are acknowledged and published from disk.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeQuiverConfig {
    /// Persist accepted batches to Quiver before acknowledging them.
    pub enabled: bool,
    /// Directory for the write-ahead log, segments, and progress files.
    pub directory: Option<PathBuf>,
    /// Hard cap on disk space used by Quiver.
    pub capacity_bytes: Option<u64>,
    /// Maximum time between WAL fsyncs; 0 fsyncs before every acknowledgement.
    #[serde(default)]
    pub wal_flush_interval_ms: u64,
    /// Maximum time a segment stays open before it is published from disk.
    #[serde(default = "default_quiver_segment_max_open_ms")]
    pub segment_max_open_ms: u64,
    /// Maximum persisted batches admitted from disk but not yet resolved.
    #[serde(default = "default_quiver_max_in_flight")]
    pub max_in_flight: usize,
    /// First retry delay for a batch whose publication outcome was unknown.
    #[serde(default = "default_quiver_retry_initial_ms")]
    pub retry_initial_ms: u64,
    /// Maximum retry delay for one persisted batch.
    #[serde(default = "default_quiver_retry_max_ms")]
    pub retry_max_ms: u64,
    /// How long receipts of a crashed instance stay reserved for its restarted
    /// successor before ordinary expiry reconciliation may fail them.
    #[serde(default = "default_quiver_recovery_window_ms")]
    pub recovery_window_ms: u64,
}

const fn default_quiver_segment_max_open_ms() -> u64 {
    500
}

const fn default_quiver_max_in_flight() -> usize {
    512
}

const fn default_quiver_retry_initial_ms() -> u64 {
    250
}

const fn default_quiver_retry_max_ms() -> u64 {
    5_000
}

const fn default_quiver_recovery_window_ms() -> u64 {
    600_000
}

/// Largest recovery window the receipt store accepts (24 hours).
const MAX_QUIVER_RECOVERY_WINDOW_MS: u64 = 86_400_000;

impl NativeQuiverConfig {
    /// Returns the durable stage settings when Quiver is enabled.
    ///
    /// # Errors
    ///
    /// Rejects a missing directory or capacity and out-of-range durations.
    pub fn stage_config(&self) -> Result<Option<QuiverStageConfig>, ConfigError> {
        if !self.enabled {
            return Ok(None);
        }
        let directory = self
            .directory
            .clone()
            .ok_or_else(|| invalid("buffer.native_quiver.directory is required when enabled"))?;
        let capacity_bytes = self
            .capacity_bytes
            .ok_or_else(|| invalid("buffer.native_quiver.capacity_bytes is required when enabled"))?;
        Ok(Some(QuiverStageConfig {
            directory,
            capacity_bytes,
            wal_flush_interval: Duration::from_millis(self.wal_flush_interval_ms),
            segment_max_open: duration_ms(self.segment_max_open_ms, "buffer.native_quiver.segment_max_open_ms")?,
            max_in_flight: self.max_in_flight,
            retry_initial: duration_ms(self.retry_initial_ms, "buffer.native_quiver.retry_initial_ms")?,
            retry_max: duration_ms(self.retry_max_ms, "buffer.native_quiver.retry_max_ms")?,
        }))
    }

    /// Receipt recovery window registered with the receipt owner when enabled.
    #[must_use]
    pub const fn recovery_window_ms(&self) -> Option<u64> {
        if self.enabled {
            Some(self.recovery_window_ms)
        } else {
            None
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let Some(stage) = self.stage_config()? else {
            if self.directory.is_some() || self.capacity_bytes.is_some() {
                return Err(invalid("buffer.native_quiver directory/capacity require enabled=true"));
            }
            return Ok(());
        };
        if self.recovery_window_ms == 0 || self.recovery_window_ms > MAX_QUIVER_RECOVERY_WINDOW_MS {
            return Err(invalid(
                "buffer.native_quiver.recovery_window_ms must be between 1 and 86400000",
            ));
        }
        stage
            .validate()
            .map_err(|error| invalid(format!("buffer.native_quiver is invalid: {}", error.message())))
    }
}

/// Router registration, pressure, and trusted-hop policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfig {
    /// Require router-signed routing lease headers for ingest.
    pub require_lease_headers: bool,
    /// Registration/heartbeat lease duration.
    pub registration_ttl_ms: u64,
    /// Registration heartbeat interval.
    pub registration_heartbeat_ms: u64,
    /// Rich pressure publication interval.
    pub pressure_interval_ms: u64,
    /// Dependency/readiness probe interval.
    pub dependency_probe_interval_ms: u64,
    /// Declared resident-memory hard bound.
    pub rss_limit_bytes: u64,
    /// Maximum table entries in rich pressure.
    pub max_table_entries: usize,
    /// Maximum accepted router snapshot age.
    pub max_snapshot_age_ms: u64,
    /// Maximum accepted future router clock skew.
    pub max_future_skew_ms: u64,
}

impl RoutingConfig {
    fn validate(&self, max_streams: u64) -> Result<(), ConfigError> {
        positive(self.registration_ttl_ms, "routing.registration_ttl_ms")?;
        positive(self.registration_heartbeat_ms, "routing.registration_heartbeat_ms")?;
        positive(self.pressure_interval_ms, "routing.pressure_interval_ms")?;
        positive(
            self.dependency_probe_interval_ms,
            "routing.dependency_probe_interval_ms",
        )?;
        positive(self.rss_limit_bytes, "routing.rss_limit_bytes")?;
        positive(self.max_table_entries, "routing.max_table_entries")?;
        positive(self.max_snapshot_age_ms, "routing.max_snapshot_age_ms")?;
        if self.registration_heartbeat_ms.saturating_mul(2) >= self.registration_ttl_ms {
            return Err(invalid(
                "routing.registration_heartbeat_ms must be less than half registration_ttl_ms",
            ));
        }
        if self.pressure_interval_ms >= self.max_snapshot_age_ms {
            return Err(invalid(
                "routing.pressure_interval_ms must be less than max_snapshot_age_ms",
            ));
        }
        positive(max_streams, "flight.max_concurrent_streams")
    }
}

/// Receipt owner and table lease policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptConfig {
    /// Owner lease duration.
    pub owner_lease_ttl_ms: u64,
    /// Owner heartbeat interval.
    pub owner_heartbeat_ms: u64,
    /// Table publication lease duration.
    pub table_lease_ttl_ms: u64,
    /// Table publication lease renewal interval.
    pub table_lease_renew_ms: u64,
    /// Accepted receipt heartbeat horizon.
    pub receipt_lease_duration_ms: u64,
    /// Backpressure wait before rejecting one batch.
    pub admission_timeout_ms: u64,
    /// SQL mutation batch bound.
    pub mutation_batch_size: usize,
    /// SQL status batch bound.
    pub status_batch_size: usize,
    /// Startup expired-receipt reconciliation batch.
    pub reconciliation_batch_size: usize,
}

impl ReceiptConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        for (value, name) in [
            (self.owner_lease_ttl_ms, "receipts.owner_lease_ttl_ms"),
            (self.owner_heartbeat_ms, "receipts.owner_heartbeat_ms"),
            (self.table_lease_ttl_ms, "receipts.table_lease_ttl_ms"),
            (self.table_lease_renew_ms, "receipts.table_lease_renew_ms"),
            (self.receipt_lease_duration_ms, "receipts.receipt_lease_duration_ms"),
            (self.admission_timeout_ms, "receipts.admission_timeout_ms"),
        ] {
            positive(value, name)?;
        }
        if self.owner_heartbeat_ms.saturating_mul(2) >= self.owner_lease_ttl_ms {
            return Err(invalid(
                "receipts.owner_heartbeat_ms must be less than half owner_lease_ttl_ms",
            ));
        }
        if self.table_lease_renew_ms.saturating_mul(2) >= self.table_lease_ttl_ms {
            return Err(invalid(
                "receipts.table_lease_renew_ms must be less than half table_lease_ttl_ms",
            ));
        }
        for (value, name) in [
            (self.mutation_batch_size, "receipts.mutation_batch_size"),
            (self.status_batch_size, "receipts.status_batch_size"),
            (self.reconciliation_batch_size, "receipts.reconciliation_batch_size"),
        ] {
            if value == 0 || value > 512 {
                return Err(invalid(format!("{name} must be in 1..=512")));
            }
        }
        Ok(())
    }
}

/// Authentication mode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// Require router-signed internal authentication context.
    RouterHmac,
    /// Accept one configured bearer token for direct access.
    StaticBearer,
    /// Explicit local-development allow-all mode.
    AllowAll,
}

/// Request authentication settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// Authentication mode.
    pub mode: AuthMode,
    /// Router internal HMAC secret.
    pub internal_hmac_secret_file: Option<PathBuf>,
    /// Maximum accepted router assertion lifetime.
    pub internal_identity_max_ttl_ms: u64,
    /// Static bearer token secret.
    pub bearer_token_file: Option<PathBuf>,
    /// Development principal subject.
    pub allow_all_subject: Option<String>,
}

impl AuthConfig {
    fn validate(&self, routing: &RoutingConfig, tls: &TlsConfig) -> Result<(), ConfigError> {
        positive(self.internal_identity_max_ttl_ms, "auth.internal_identity_max_ttl_ms")?;
        match self.mode {
            AuthMode::RouterHmac => {
                required_path(&self.internal_hmac_secret_file, "auth.internal_hmac_secret_file")?;
                if self.bearer_token_file.is_some() || self.allow_all_subject.is_some() {
                    return Err(invalid("router_hmac auth cannot configure bearer or allow-all values"));
                }
                if !routing.require_lease_headers {
                    return Err(invalid("router_hmac auth requires routing.require_lease_headers=true"));
                }
            }
            AuthMode::StaticBearer => {
                required_path(&self.bearer_token_file, "auth.bearer_token_file")?;
                if self.internal_hmac_secret_file.is_some() || self.allow_all_subject.is_some() {
                    return Err(invalid(
                        "static_bearer auth cannot configure router HMAC or allow-all values",
                    ));
                }
                if routing.require_lease_headers {
                    return Err(invalid("static_bearer auth cannot require router lease headers"));
                }
                if !tls.enabled {
                    return Err(invalid("static_bearer auth requires Flight TLS"));
                }
            }
            AuthMode::AllowAll => {
                let subject = self
                    .allow_all_subject
                    .as_deref()
                    .ok_or_else(|| invalid("allow_all auth requires auth.allow_all_subject"))?;
                nonempty(subject, "auth.allow_all_subject")?;
                if self.internal_hmac_secret_file.is_some() || self.bearer_token_file.is_some() {
                    return Err(invalid("allow_all auth cannot configure router HMAC or bearer values"));
                }
                if routing.require_lease_headers {
                    return Err(invalid("allow_all auth cannot require router lease headers"));
                }
            }
        }
        Ok(())
    }
}

/// Graceful shutdown settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShutdownConfig {
    /// Time for Flight streams to complete before forced cancellation.
    pub stream_drain_timeout_ms: u64,
    /// Total time for the Quiver drain, buffer, Delta, and receipt completion.
    ///
    /// Stream plus publication drain must stay below the supervisor's stop
    /// budget so a bounded drain failure is recorded instead of killed.
    pub publication_drain_timeout_ms: u64,
}

impl ShutdownConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        positive(self.stream_drain_timeout_ms, "shutdown.stream_drain_timeout_ms")?;
        positive(
            self.publication_drain_timeout_ms,
            "shutdown.publication_drain_timeout_ms",
        )
    }
}

/// Logging settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    /// Tracing filter directive.
    pub log_filter: String,
    /// Emit JSON logs.
    pub json_logs: bool,
}

fn invalid(message: impl Into<String>) -> ConfigError {
    ConfigError::Invalid(message.into())
}

fn positive<T>(value: T, name: &str) -> Result<(), ConfigError>
where
    T: PartialEq + From<u8>,
{
    if value == T::from(0) {
        Err(invalid(format!("{name} must be positive")))
    } else {
        Ok(())
    }
}

fn nonempty(value: &str, name: &str) -> Result<(), ConfigError> {
    if value.is_empty() {
        Err(invalid(format!("{name} must not be empty")))
    } else {
        Ok(())
    }
}

fn nonempty_path(path: &Path, name: &str) -> Result<(), ConfigError> {
    if path.as_os_str().is_empty() {
        Err(invalid(format!("{name} must not be empty")))
    } else {
        Ok(())
    }
}

fn required_path(value: &Option<PathBuf>, name: &str) -> Result<(), ConfigError> {
    value
        .as_deref()
        .ok_or_else(|| invalid(format!("{name} is required")))
        .and_then(|path| nonempty_path(path, name))
}

fn duration_ms(value: u64, name: &str) -> Result<Duration, ConfigError> {
    positive(value, name)?;
    Ok(Duration::from_millis(value))
}

#[cfg(test)]
mod delta_root_tests {
    use super::*;

    #[test]
    fn root_override_preserves_local_default_and_adls_uri() -> Result<(), ConfigError> {
        let mut config = CollectorConfig::from_yaml(include_bytes!("../config.yaml"))?;
        let default = config.delta.root.clone();
        config.delta.override_root(None);
        assert_eq!(config.delta.root, default);
        let uri = "abfss://container@account.dfs.core.windows.net/prefix";
        config.delta.override_root(Some(PathBuf::from(uri)));
        assert_eq!(config.delta.root.to_string_lossy(), uri);
        config.delta.validate()?;
        config.delta.override_root(Some(PathBuf::new()));
        assert!(config.delta.validate().is_err());
        Ok(())
    }
}
