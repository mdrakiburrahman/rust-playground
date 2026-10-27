use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlParser, HiveMetastore, HiveMetastoreConfig, MemoryCatalogStore,
    MemoryMetastore, SqlServerCatalogStore, SqlServerConfig,
};
use arrowcol_flight::{RuntimeLimits, RuntimeResult};
use arrowcol_protocol::{PositiveMeasurement, UnixMillis};
use arrowcol_routing::{MemoryRoutingStore, RoutingStore, SqlServerRoutingConfig, SqlServerRoutingStore};
use serde::Deserialize;

fn default_connect_timeout() -> Duration {
    Duration::from_secs(15)
}

fn default_command_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_hive_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_pressure_interval() -> Duration {
    Duration::from_secs(2)
}

fn default_registration_ttl() -> Duration {
    Duration::from_secs(20)
}

fn default_max_concurrent_streams() -> usize {
    128
}

fn default_rss_limit_bytes() -> u64 {
    2 * 1_024 * 1_024 * 1_024
}

fn default_buffered_bytes_limit() -> u64 {
    512 * 1_024 * 1_024
}

fn default_buffered_rows_limit() -> u64 {
    10_000_000
}

fn default_buffered_batches_limit() -> u64 {
    4_096
}

fn default_authority_timeout() -> Duration {
    Duration::from_secs(10)
}

/// Shared SQL Server connection settings.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SqlServerConnectionConfig {
    /// SQL Server host.
    pub host: String,
    /// SQL Server TCP port.
    pub port: u16,
    /// Database name.
    pub database: String,
    /// SQL-authenticated user.
    pub username: String,
    /// One-line password file.
    pub password_file: PathBuf,
    /// Local-development certificate bypass.
    #[serde(default)]
    pub trust_server_certificate: bool,
    /// TCP/login timeout.
    #[serde(default = "default_connect_timeout", with = "humantime_serde")]
    pub connect_timeout: Duration,
    /// SQL command timeout.
    #[serde(default = "default_command_timeout", with = "humantime_serde")]
    pub command_timeout: Duration,
}

impl SqlServerConnectionConfig {
    fn validate(&self) -> Result<(), String> {
        if self.host.is_empty()
            || self.port == 0
            || self.database.is_empty()
            || self.username.is_empty()
            || self.password_file.as_os_str().is_empty()
            || self.connect_timeout.is_zero()
            || self.command_timeout.is_zero()
        {
            return Err(
                "SQL Server host, port, database, username, password file, and timeouts must be set".to_owned(),
            );
        }
        Ok(())
    }

    pub(crate) fn catalog_config(&self) -> arrowcol_catalog::CatalogResult<SqlServerConfig> {
        Ok(SqlServerConfig::with_password_file(
            self.host.clone(),
            self.port,
            self.database.clone(),
            self.username.clone(),
            &self.password_file,
        )?
        .trust_server_certificate(self.trust_server_certificate)
        .with_timeouts(self.connect_timeout, self.command_timeout))
    }

    pub(crate) fn routing_config(&self) -> arrowcol_routing::RoutingResult<SqlServerRoutingConfig> {
        Ok(SqlServerRoutingConfig::with_password_file(
            self.host.clone(),
            self.port,
            self.database.clone(),
            self.username.clone(),
            &self.password_file,
        )?
        .trust_server_certificate(self.trust_server_certificate)
        .with_timeouts(self.connect_timeout, self.command_timeout))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HiveMetastoreConnectionConfig {
    /// Hive Metastore host.
    pub host: String,
    /// Hive Metastore Thrift port.
    pub port: u16,
    /// Warehouse URI used for new external tables.
    pub warehouse_root: String,
    /// Hive table owner.
    pub owner: String,
    /// TCP/read/write timeout.
    #[serde(default = "default_hive_timeout", with = "humantime_serde")]
    pub connect_timeout: Duration,
}

impl HiveMetastoreConnectionConfig {
    fn validate(&self) -> Result<(), String> {
        if self.host.is_empty()
            || self.port == 0
            || self.warehouse_root.is_empty()
            || self.owner.is_empty()
            || self.connect_timeout.is_zero()
        {
            return Err("Hive host, port, warehouse root, owner, and connect timeout must be set".to_owned());
        }
        Ok(())
    }

    fn build(&self) -> HiveMetastoreConfig {
        HiveMetastoreConfig::new(self.host.clone(), self.port)
            .with_warehouse_root(self.warehouse_root.clone())
            .with_owner(self.owner.clone())
            .with_connect_timeout(self.connect_timeout)
    }
}

/// Catalog implementation used by the Flight receiver.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CatalogConfig {
    /// Empty in-memory catalog and metastore, intended for focused development.
    Memory,
    /// Production application database plus Hive Metastore.
    SqlHive {
        /// Arrowcol application database.
        application_database: Box<SqlServerConnectionConfig>,
        /// Hive Metastore Thrift connection.
        hive_metastore: Box<HiveMetastoreConnectionConfig>,
        /// Time zone used for strict timestamp DDL mapping.
        #[serde(default = "default_timezone")]
        timestamp_timezone: String,
    },
}

fn default_timezone() -> String {
    "UTC".to_owned()
}

impl CatalogConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        match self {
            Self::Memory => Ok(()),
            Self::SqlHive {
                application_database,
                hive_metastore,
                timestamp_timezone,
            } => {
                application_database.validate()?;
                hive_metastore.validate()?;
                if timestamp_timezone.trim().is_empty() {
                    return Err("catalog timestamp timezone must not be empty".to_owned());
                }
                Ok(())
            }
        }
    }

    pub(crate) async fn start(
        &self,
    ) -> arrowcol_catalog::CatalogResult<(Arc<dyn CatalogStore>, Arc<CatalogControlPlane>)> {
        match self {
            Self::Memory => {
                let store: Arc<dyn CatalogStore> = Arc::new(MemoryCatalogStore::new());
                let metastore = Arc::new(MemoryMetastore::new());
                let (control, _) =
                    CatalogControlPlane::start(DdlParser::default(), Arc::clone(&store), metastore).await?;
                Ok((store, Arc::new(control)))
            }
            Self::SqlHive {
                application_database,
                hive_metastore,
                timestamp_timezone,
            } => {
                let store: Arc<dyn CatalogStore> =
                    Arc::new(SqlServerCatalogStore::new(application_database.catalog_config()?));
                let metastore = Arc::new(HiveMetastore::new(hive_metastore.build()));
                let (control, _) = CatalogControlPlane::start(
                    DdlParser::new(timestamp_timezone.clone()),
                    Arc::clone(&store),
                    metastore,
                )
                .await?;
                Ok((store, Arc::new(control)))
            }
        }
    }
}

/// Routing-store implementation shared by admission, fencing, and registration.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RoutingStoreConfig {
    /// Empty process-local routing store, intended only for tests.
    Memory,
    /// SQL Server routing control plane.
    SqlServer {
        /// SQL Server connection.
        connection: SqlServerConnectionConfig,
    },
}

impl RoutingStoreConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        match self {
            Self::Memory => Ok(()),
            Self::SqlServer { connection } => connection.validate(),
        }
    }

    pub(crate) fn build(&self) -> Result<Arc<dyn RoutingStore>, String> {
        match self {
            Self::Memory => Ok(Arc::new(MemoryRoutingStore::default())),
            Self::SqlServer { connection } => connection
                .routing_config()
                .map(|config| Arc::new(SqlServerRoutingStore::new(config)) as Arc<dyn RoutingStore>)
                .map_err(|error| error.to_string()),
        }
    }
}

/// Declarative Arrow Flight runtime limits.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeLimitsConfig {
    /// Maximum aggregate gRPC metadata bytes.
    pub max_metadata_bytes: usize,
    /// Maximum encoded Arrow schema bytes.
    pub max_schema_bytes: usize,
    /// Maximum one-message size.
    pub max_message_bytes: usize,
    /// Bounded decoder-to-admission channel capacity.
    pub batch_channel_capacity: usize,
    /// Bounded Flight response channel capacity.
    pub put_result_channel_capacity: usize,
    /// Maximum stream duration.
    #[serde(with = "humantime_serde")]
    pub max_stream_duration: Duration,
    /// Maximum time spent in one DFE admission call.
    #[serde(with = "humantime_serde")]
    pub admission_timeout: Duration,
    /// Maximum Flight SQL command bytes.
    pub max_ddl_bytes: usize,
}

impl Default for RuntimeLimitsConfig {
    fn default() -> Self {
        let limits = RuntimeLimits::default();
        Self {
            max_metadata_bytes: limits.max_metadata_bytes,
            max_schema_bytes: limits.max_schema_bytes,
            max_message_bytes: limits.max_message_bytes,
            batch_channel_capacity: limits.batch_channel_capacity,
            put_result_channel_capacity: limits.put_result_channel_capacity,
            max_stream_duration: limits.max_stream_duration,
            admission_timeout: limits.admission_timeout,
            max_ddl_bytes: limits.max_ddl_bytes,
        }
    }
}

impl RuntimeLimitsConfig {
    pub(crate) fn build(&self) -> RuntimeResult<RuntimeLimits> {
        let limits = RuntimeLimits {
            max_metadata_bytes: self.max_metadata_bytes,
            max_schema_bytes: self.max_schema_bytes,
            max_message_bytes: self.max_message_bytes,
            batch_channel_capacity: self.batch_channel_capacity,
            put_result_channel_capacity: self.put_result_channel_capacity,
            max_stream_duration: self.max_stream_duration,
            admission_timeout: self.admission_timeout,
            max_ddl_bytes: self.max_ddl_bytes,
        };
        limits.validate()?;
        Ok(limits)
    }
}

/// Collector registration and pressure-publication settings.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RegistrationConfig {
    /// Stable deployment key.
    pub instance_key: String,
    /// Router-visible Flight endpoint.
    pub flight_endpoint: SocketAddr,
    /// Advertised concurrent stream limit.
    #[serde(default = "default_max_concurrent_streams")]
    pub max_concurrent_streams: usize,
    /// Registration heartbeat interval.
    #[serde(default = "default_heartbeat_interval", with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    /// Rich pressure publication interval.
    #[serde(default = "default_pressure_interval", with = "humantime_serde")]
    pub pressure_interval: Duration,
    /// Registration lease lifetime.
    #[serde(default = "default_registration_ttl", with = "humantime_serde")]
    pub registration_ttl: Duration,
    /// Hard resident-memory limit.
    #[serde(default = "default_rss_limit_bytes")]
    pub rss_limit_bytes: u64,
    /// End-to-end buffered Arrow byte bound.
    #[serde(default = "default_buffered_bytes_limit")]
    pub buffered_bytes_limit: u64,
    /// End-to-end buffered row bound.
    #[serde(default = "default_buffered_rows_limit")]
    pub buffered_rows_limit: u64,
    /// End-to-end buffered batch bound.
    #[serde(default = "default_buffered_batches_limit")]
    pub buffered_batches_limit: u64,
}

impl RegistrationConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.instance_key.is_empty()
            || self.instance_key.len() > 200
            || self.instance_key.chars().any(char::is_control)
            || self.flight_endpoint.port() == 0
            || self.max_concurrent_streams == 0
            || self.heartbeat_interval.is_zero()
            || self.pressure_interval.is_zero()
            || self.registration_ttl <= self.heartbeat_interval
            || self.rss_limit_bytes == 0
            || self.buffered_bytes_limit == 0
            || self.buffered_rows_limit == 0
            || self.buffered_batches_limit == 0
        {
            return Err("collector registration and pressure bounds are invalid".to_owned());
        }
        PositiveMeasurement::new(
            u64::try_from(self.max_concurrent_streams).map_err(|_| "max_concurrent_streams exceeds u64".to_owned())?,
        )
        .map_err(|error| error.to_string())?;
        for value in [
            self.rss_limit_bytes,
            self.buffered_bytes_limit,
            self.buffered_rows_limit,
            self.buffered_batches_limit,
        ] {
            PositiveMeasurement::new(value).map_err(|error| error.to_string())?;
        }
        duration_millis(self.registration_ttl)?;
        Ok(())
    }
}

/// Strict configuration for `receiver:arrow_flight`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FlightReceiverConfig {
    /// Arrow Flight bind address.
    pub listening_addr: SocketAddr,
    /// Admin HTTP bind address.
    pub admin_listening_addr: SocketAddr,
    /// Strict catalog control plane.
    pub catalog: CatalogConfig,
    /// Routing registration and lease store.
    pub routing: RoutingStoreConfig,
    /// Registration and bounded-pressure settings.
    pub registration: RegistrationConfig,
    /// Flight resource limits.
    #[serde(default)]
    pub limits: RuntimeLimitsConfig,
    /// Enables durable admission through native Quiver persistence.
    ///
    /// The topology must route this receiver into a `processor:durable_buffer`
    /// (ideally configured with `wal_flush_interval: 0s`). A Flight `DoPut`
    /// then completes with `durable_admission` only after the durable buffer
    /// acknowledged every batch of the stream; a rejection or
    /// `limits.admission_timeout` fails the stream with a retryable error.
    /// When `false`, completion stays `best_effort_admission`.
    #[serde(default)]
    pub native_quiver: bool,
}

impl FlightReceiverConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.listening_addr.port() == 0
            || self.admin_listening_addr.port() == 0
            || self.listening_addr == self.admin_listening_addr
        {
            return Err("Flight and admin listeners must use distinct nonzero ports".to_owned());
        }
        self.catalog.validate()?;
        self.routing.validate()?;
        self.registration.validate()?;
        self.limits.build().map_err(|error| error.to_string())?;
        Ok(())
    }
}

/// Bounded Delta publication settings.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct CommitConfig {
    /// Maximum batching delay.
    #[serde(with = "humantime_serde")]
    pub batch_window: Duration,
    /// Maximum files in one Delta commit.
    pub max_files_per_commit: usize,
    /// Maximum queued and committing files per table.
    pub max_pending_files: usize,
}

impl Default for CommitConfig {
    fn default() -> Self {
        let options = arrow_deltalake::commit::CommitOptions::default();
        Self {
            batch_window: options.batch_window,
            max_files_per_commit: options.max_files_per_commit,
            max_pending_files: options.max_pending_files,
        }
    }
}

impl CommitConfig {
    pub(crate) fn build(&self) -> Result<arrow_deltalake::commit::CommitOptions, String> {
        let options = arrow_deltalake::commit::CommitOptions {
            batch_window: self.batch_window,
            max_files_per_commit: self.max_files_per_commit,
            max_pending_files: self.max_pending_files,
        };
        options.validate().map_err(|error| error.to_string())?;
        Ok(options)
    }
}

/// Strict configuration for `exporter:arrow_deltalake`.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeltaExporterConfig {
    /// Absolute local root or abfss URI for logical Arrowcol tables.
    pub base_path: PathBuf,
    /// Authoritative routing lease store.
    pub routing: RoutingStoreConfig,
    /// Bounded publication settings.
    #[serde(default)]
    pub commit: CommitConfig,
    /// Bounded Arrow-to-Parquet file grouping.
    #[serde(default)]
    pub batching: FileBatchConfig,
    /// Maximum synchronous wait for the commit-boundary authority check.
    #[serde(default = "default_authority_timeout", with = "humantime_serde")]
    pub authority_timeout: Duration,
}

impl DeltaExporterConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if !self.base_path.is_absolute() && !self.base_path.to_string_lossy().starts_with("abfss://") {
            return Err("Delta base_path must be absolute or an abfss URI".to_owned());
        }
        if self.authority_timeout.is_zero() {
            return Err("Delta authority_timeout must be positive".to_owned());
        }
        self.routing.validate()?;
        self.commit.build()?;
        self.batching.validate()?;
        Ok(())
    }
}

/// Logical Arrow capacity retained until publication finishes.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BatchCapacity {
    /// Logical Arrow bytes.
    pub bytes: u32,
    /// Rows.
    pub rows: u32,
    /// Batches.
    pub batches: u32,
}

/// Bounded file grouping, independent of delivery receipt semantics.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct FileBatchConfig {
    /// Process-wide queued and in-flight capacity.
    pub global_capacity: BatchCapacity,
    /// Per-table queued and in-flight capacity.
    pub per_table_capacity: BatchCapacity,
    /// Maximum logical Arrow bytes in a file.
    pub max_arrow_memory_per_flush: u64,
    /// Maximum rows in a file.
    pub max_rows_per_flush: u64,
    /// Maximum batches in a file.
    pub max_batches_per_flush: u64,
    /// Maximum age of the first batch in a file.
    #[serde(with = "humantime_serde")]
    pub max_age: Duration,
    /// Maximum simultaneous file publications.
    pub max_concurrent_flushes: usize,
    /// Compressed Parquet target and estimator limits.
    pub adaptive_file: AdaptiveFileConfig,
}

impl Default for FileBatchConfig {
    fn default() -> Self {
        Self {
            global_capacity: BatchCapacity {
                bytes: 2 * 1024 * 1024 * 1024,
                rows: 10_000_000,
                batches: 4096,
            },
            per_table_capacity: BatchCapacity {
                bytes: 512 * 1024 * 1024,
                rows: 2_000_000,
                batches: 1024,
            },
            max_arrow_memory_per_flush: 512 * 1024 * 1024,
            max_rows_per_flush: 1_000_000,
            max_batches_per_flush: 512,
            max_age: Duration::from_secs(30),
            max_concurrent_flushes: 4,
            adaptive_file: AdaptiveFileConfig::default(),
        }
    }
}

impl FileBatchConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        let global = self.global_capacity;
        let table = self.per_table_capacity;
        if [
            global.bytes,
            global.rows,
            global.batches,
            table.bytes,
            table.rows,
            table.batches,
        ]
        .contains(&0)
            || table.bytes > global.bytes
            || table.rows > global.rows
            || table.batches > global.batches
            || self.max_arrow_memory_per_flush == 0
            || self.max_arrow_memory_per_flush > u64::from(table.bytes)
            || self.max_rows_per_flush == 0
            || self.max_rows_per_flush > u64::from(table.rows)
            || self.max_batches_per_flush == 0
            || self.max_batches_per_flush > u64::from(table.batches)
            || self.max_age.is_zero()
            || tokio::time::Instant::now().checked_add(self.max_age).is_none()
            || self.max_concurrent_flushes == 0
        {
            return Err("invalid bounded Arrow file batching capacities or flush policy".to_owned());
        }
        self.adaptive_file.build().validate().map_err(|error| error.to_string())
    }
}

/// Configuration for the shared compressed-file size estimator.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct AdaptiveFileConfig {
    /// Desired compressed Parquet bytes.
    pub target_parquet_bytes: u64,
    /// Minimum logical input threshold.
    pub min_input_bytes: u64,
    /// Maximum logical input threshold.
    pub max_input_bytes: u64,
    /// Initial compressed/input ratio.
    pub cold_start_parquet_ratio: f64,
    /// EWMA observation weight.
    pub ewma_weight: f64,
    /// Minimum observed ratio.
    pub min_parquet_ratio: f64,
    /// Maximum observed ratio.
    pub max_parquet_ratio: f64,
}

impl Default for AdaptiveFileConfig {
    fn default() -> Self {
        let config = arrow_deltalake::sizing::AdaptiveFileSizeConfig::default();
        Self {
            target_parquet_bytes: config.target_parquet_bytes,
            min_input_bytes: config.min_input_bytes,
            max_input_bytes: config.max_input_bytes,
            cold_start_parquet_ratio: config.cold_start_parquet_ratio,
            ewma_weight: config.ewma_weight,
            min_parquet_ratio: config.min_parquet_ratio,
            max_parquet_ratio: config.max_parquet_ratio,
        }
    }
}

impl AdaptiveFileConfig {
    pub(crate) const fn build(self) -> arrow_deltalake::sizing::AdaptiveFileSizeConfig {
        arrow_deltalake::sizing::AdaptiveFileSizeConfig {
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

pub(crate) fn config_error(error: impl Into<String>) -> otel_arrow_dfe_config::error::Error {
    otel_arrow_dfe_config::error::Error::InvalidUserConfig { error: error.into() }
}

pub(crate) fn parse_config<T>(value: &serde_json::Value) -> Result<T, otel_arrow_dfe_config::error::Error>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(value.clone()).map_err(|error| config_error(error.to_string()))
}

pub(crate) fn duration_millis(duration: Duration) -> Result<u64, String> {
    u64::try_from(duration.as_millis()).map_err(|_| "duration exceeds u64 milliseconds".to_owned())
}

pub(crate) fn now_unix_millis() -> Result<UnixMillis, String> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("system clock precedes Unix epoch: {error}"))?;
    let millis = u64::try_from(elapsed.as_millis()).map_err(|_| "Unix timestamp exceeds u64".to_owned())?;
    UnixMillis::new(millis).map_err(|error| error.to_string())
}

pub(crate) fn checked_add_millis(now: UnixMillis, duration: Duration) -> Result<UnixMillis, String> {
    let millis = duration_millis(duration)?;
    let value = now
        .get()
        .checked_add(millis)
        .ok_or_else(|| "Unix timestamp overflow".to_owned())?;
    UnixMillis::new(value).map_err(|error| error.to_string())
}

pub(crate) fn table_path(base: &Path, table: &arrowcol_protocol::TableIdentity) -> PathBuf {
    base.join(table.catalog().as_str())
        .join(table.schema().as_str())
        .join(table.table().as_str())
}
