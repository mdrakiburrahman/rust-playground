use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use arrowcol_routing::{PressurePolicy, RoutingPolicy};
use clap::Parser;
use serde::Deserialize;

/// Command-line options for the router process.
#[derive(Clone, Debug, Parser)]
#[command(about = "Production Arrowcol Pingora router")]
pub struct Cli {
    /// Strict YAML configuration file.
    #[arg(long, default_value = "projects/arrowcol-router-rust/config.yaml")]
    pub config: PathBuf,
    /// Parse and validate configuration without starting listeners.
    #[arg(long)]
    pub validate_and_exit: bool,
    /// Runtime tuning overrides; command-line values take precedence over environment and YAML.
    #[command(flatten)]
    pub router: RouterOverrides,
}

/// Supported startup-only router performance overrides.
#[derive(Clone, Debug, Default, clap::Args)]
pub struct RouterOverrides {
    #[arg(long)]
    pub workers: Option<usize>,
    #[arg(long)]
    pub listener_tasks_per_fd: Option<usize>,
    #[arg(long)]
    pub keepalive_pool_size: Option<usize>,
    #[arg(long)]
    pub h2_max_concurrent_streams: Option<u32>,
    #[arg(long)]
    pub h2_stream_window_bytes: Option<u32>,
    #[arg(long)]
    pub h2_connection_window_bytes: Option<u32>,
    #[arg(long)]
    pub h2_idle_timeout_seconds: Option<u64>,
    #[arg(long)]
    pub sql_connect_timeout_ms: Option<u64>,
    #[arg(long)]
    pub sql_command_timeout_ms: Option<u64>,
    #[arg(long)]
    pub refresh_interval_ms: Option<u64>,
    #[arg(long)]
    pub max_snapshot_age_ms: Option<u64>,
    #[arg(long)]
    pub max_future_skew_ms: Option<u64>,
    #[arg(long)]
    pub max_table_entries: Option<usize>,
    #[arg(long)]
    pub max_oldest_buffer_age_ms: Option<u64>,
    #[arg(long)]
    pub hard_pressure_bps: Option<u16>,
    #[arg(long)]
    pub warmup_ms: Option<u64>,
    #[arg(long)]
    pub minimum_warmup_bps: Option<u16>,
    #[arg(long)]
    pub lease_ttl_ms: Option<u64>,
    #[arg(long)]
    pub oat_run_pin_ttl_ms: Option<u64>,
    #[arg(long)]
    pub readiness_max_staleness_ms: Option<u64>,
    #[arg(long, action = clap::ArgAction::Set)]
    pub distinct_table_owners: Option<bool>,
    #[arg(long)]
    pub edge_replay_max_nonces: Option<usize>,
    #[arg(long)]
    pub upstream_max_h2_streams: Option<usize>,
    #[arg(long)]
    pub upstream_stream_window_bytes: Option<u32>,
    #[arg(long)]
    pub upstream_connection_window_bytes: Option<u32>,
    #[arg(long)]
    pub upstream_connection_timeout_ms: Option<u64>,
    #[arg(long)]
    pub upstream_total_connection_timeout_ms: Option<u64>,
    #[arg(long)]
    pub upstream_idle_timeout_seconds: Option<u64>,
    /// Zero disables upstream HTTP/2 keepalive pings.
    #[arg(long)]
    pub upstream_ping_interval_seconds: Option<u64>,
}

/// Strict production router configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterConfig {
    /// Listener and process lifecycle settings.
    pub server: ServerConfig,
    /// SQL Server database for routing registrations, pressure, and fenced leases.
    pub sql_server: SqlServerConfig,
    /// Routing and pressure policy.
    pub routing: RoutingConfig,
    /// Trusted edge and internal identity settings.
    pub auth: AuthConfig,
    /// Upstream HTTP/2 transport settings.
    pub upstream: UpstreamConfig,
    /// Logging settings.
    pub telemetry: TelemetryConfig,
}

impl RouterConfig {
    /// Loads strict YAML and validates every configured bound.
    ///
    /// # Errors
    ///
    /// Returns an error for unreadable YAML, unknown fields, or invalid bounds.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        Self::load_with_cli(path, &RouterOverrides::default())
    }

    /// Loads YAML, applies environment and CLI overrides, then validates the effective config.
    ///
    /// # Errors
    ///
    /// Returns an error for unreadable YAML, unknown fields, invalid overrides, or invalid bounds.
    pub fn load_with_cli(path: impl AsRef<Path>, overrides: &RouterOverrides) -> Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).with_context(|| format!("read router config {}", path.display()))?;
        let mut config: Self =
            serde_yaml::from_slice(&bytes).with_context(|| format!("parse strict YAML {}", path.display()))?;
        apply_environment_overrides(&mut config, |name| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(anyhow::anyhow!("environment variable {name} must be valid UTF-8"))
            }
        })?;
        apply_cli_overrides(&mut config, overrides);
        config.validate()?;
        Ok(config)
    }

    /// Parses strict YAML bytes and validates every configured bound.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown fields or invalid bounds.
    pub fn from_yaml(bytes: &[u8]) -> Result<Self> {
        let config: Self = serde_yaml::from_slice(bytes)?;
        config.validate()?;
        Ok(config)
    }

    /// Validates cross-field and protocol bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when a configured value is unsafe or inconsistent.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=256).contains(&self.server.threads),
            "server.threads must be between 1 and 256"
        );
        ensure!(
            (1..=64).contains(&self.server.listener_tasks_per_fd),
            "server.listener_tasks_per_fd must be between 1 and 64"
        );
        ensure!(
            (1..=65_536).contains(&self.server.upstream_keepalive_pool_size),
            "server.upstream_keepalive_pool_size must be between 1 and 65536"
        );
        ensure!(
            self.server.grace_period_seconds > 0 && self.server.graceful_shutdown_timeout_seconds > 0,
            "graceful shutdown durations must be positive"
        );
        ensure!(
            !self.server.state_directory.as_os_str().is_empty(),
            "server.state_directory must not be empty"
        );
        ensure!(!self.sql_server.host.is_empty(), "sql_server.host must not be empty");
        ensure!(
            !self.sql_server.database.is_empty(),
            "sql_server.database must not be empty"
        );
        ensure!(
            !self.sql_server.username.is_empty(),
            "sql_server.username must not be empty"
        );
        ensure!(
            !self.sql_server.password_file.as_os_str().is_empty(),
            "sql_server.password_file must not be empty"
        );
        ensure!(
            self.routing.refresh_interval_ms > 0,
            "routing.refresh_interval_ms must be positive"
        );
        self.routing.policy()?.validate().map_err(anyhow::Error::msg)?;
        ensure!(
            self.auth.edge_signature_max_age_ms > 0 && self.auth.internal_identity_ttl_ms > 0,
            "authentication time bounds must be positive"
        );
        ensure!(
            (1..=1_048_576).contains(&self.auth.edge_replay_max_nonces),
            "auth.edge_replay_max_nonces must be between 1 and 1048576"
        );
        ensure!(
            self.auth.edge_secret_file != self.auth.internal_signing_secret_file,
            "edge and internal signing secrets must use different files"
        );
        ensure!(
            (1..=65_535).contains(&self.upstream.max_h2_streams)
                && (1..=2_147_483_647).contains(&self.upstream.stream_window_bytes)
                && self.upstream.connection_window_bytes >= self.upstream.stream_window_bytes
                && self.upstream.connection_window_bytes <= 2_147_483_647,
            "upstream H2 streams and windows are outside supported bounds"
        );
        ensure!(
            (1..=65_535).contains(&self.server.h2.max_concurrent_streams)
                && (1..=2_147_483_647).contains(&self.server.h2.stream_window_bytes)
                && self.server.h2.connection_window_bytes >= self.server.h2.stream_window_bytes
                && self.server.h2.connection_window_bytes <= 2_147_483_647,
            "downstream H2 streams and windows are outside supported bounds"
        );
        ensure!(
            (1..=86_400).contains(&self.server.h2.idle_timeout_seconds)
                && (1..=600_000).contains(&self.upstream.connection_timeout_ms)
                && self.upstream.total_connection_timeout_ms >= self.upstream.connection_timeout_ms
                && self.upstream.total_connection_timeout_ms <= 600_000
                && (1..=86_400).contains(&self.upstream.idle_timeout_seconds)
                && self
                    .upstream
                    .ping_interval_seconds
                    .is_none_or(|seconds| (1..=3_600).contains(&seconds)),
            "HTTP/2 idle, connection, and ping timeouts are outside supported bounds"
        );
        ensure!(
            (1..=600_000).contains(&self.sql_server.connect_timeout_ms)
                && (1..=600_000).contains(&self.sql_server.command_timeout_ms),
            "SQL Server timeouts must be between 1 and 600000 milliseconds"
        );
        ensure!(
            (1..=3_600_000).contains(&self.routing.refresh_interval_ms),
            "routing.refresh_interval_ms must be between 1 and 3600000"
        );
        ensure!(
            self.routing.max_snapshot_age_ms <= 86_400_000
                && self.routing.max_future_skew_ms <= 86_400_000
                && self.routing.max_oldest_buffer_age_ms <= 86_400_000
                && self.routing.warmup_ms <= 86_400_000
                && self.routing.lease_ttl_ms <= 86_400_000
                && self.routing.readiness_max_staleness_ms <= 86_400_000,
            "routing time bounds must not exceed 86400000 milliseconds"
        );
        Ok(())
    }
}

fn apply_environment_overrides(
    config: &mut RouterConfig,
    mut get: impl FnMut(&str) -> Result<Option<String>>,
) -> Result<()> {
    macro_rules! set_from_env {
        ($field:expr, $name:literal, $kind:ty) => {
            if let Some(value) = get($name)? {
                *$field = value
                    .parse::<$kind>()
                    .with_context(|| format!("parse environment variable {} as {}", $name, stringify!($kind)))?;
            }
        };
    }

    set_from_env!(&mut config.server.threads, "ARROWCOL_ROUTER_WORKERS", usize);
    set_from_env!(
        &mut config.server.listener_tasks_per_fd,
        "ARROWCOL_ROUTER_LISTENER_TASKS_PER_FD",
        usize
    );
    set_from_env!(
        &mut config.server.upstream_keepalive_pool_size,
        "ARROWCOL_ROUTER_KEEPALIVE_POOL_SIZE",
        usize
    );
    set_from_env!(
        &mut config.server.h2.max_concurrent_streams,
        "ARROWCOL_ROUTER_H2_MAX_CONCURRENT_STREAMS",
        u32
    );
    set_from_env!(
        &mut config.server.h2.stream_window_bytes,
        "ARROWCOL_ROUTER_H2_STREAM_WINDOW_BYTES",
        u32
    );
    set_from_env!(
        &mut config.server.h2.connection_window_bytes,
        "ARROWCOL_ROUTER_H2_CONNECTION_WINDOW_BYTES",
        u32
    );
    set_from_env!(
        &mut config.server.h2.idle_timeout_seconds,
        "ARROWCOL_ROUTER_H2_IDLE_TIMEOUT_SECONDS",
        u64
    );
    set_from_env!(
        &mut config.sql_server.connect_timeout_ms,
        "ARROWCOL_ROUTER_SQL_CONNECT_TIMEOUT_MS",
        u64
    );
    set_from_env!(
        &mut config.sql_server.command_timeout_ms,
        "ARROWCOL_ROUTER_SQL_COMMAND_TIMEOUT_MS",
        u64
    );
    set_from_env!(
        &mut config.routing.refresh_interval_ms,
        "ARROWCOL_ROUTER_REFRESH_INTERVAL_MS",
        u64
    );
    set_from_env!(
        &mut config.routing.max_snapshot_age_ms,
        "ARROWCOL_ROUTER_MAX_SNAPSHOT_AGE_MS",
        u64
    );
    set_from_env!(
        &mut config.routing.max_future_skew_ms,
        "ARROWCOL_ROUTER_MAX_FUTURE_SKEW_MS",
        u64
    );
    set_from_env!(
        &mut config.routing.max_table_entries,
        "ARROWCOL_ROUTER_MAX_TABLE_ENTRIES",
        usize
    );
    set_from_env!(
        &mut config.routing.max_oldest_buffer_age_ms,
        "ARROWCOL_ROUTER_MAX_OLDEST_BUFFER_AGE_MS",
        u64
    );
    set_from_env!(
        &mut config.routing.hard_pressure_bps,
        "ARROWCOL_ROUTER_HARD_PRESSURE_BPS",
        u16
    );
    set_from_env!(&mut config.routing.warmup_ms, "ARROWCOL_ROUTER_WARMUP_MS", u64);
    set_from_env!(
        &mut config.routing.minimum_warmup_bps,
        "ARROWCOL_ROUTER_MINIMUM_WARMUP_BPS",
        u16
    );
    set_from_env!(&mut config.routing.lease_ttl_ms, "ARROWCOL_ROUTER_LEASE_TTL_MS", u64);
    set_from_env!(
        &mut config.routing.oat_run_pin_ttl_ms,
        "ARROWCOL_ROUTER_OAT_RUN_PIN_TTL_MS",
        u64
    );
    set_from_env!(
        &mut config.routing.readiness_max_staleness_ms,
        "ARROWCOL_ROUTER_READINESS_MAX_STALENESS_MS",
        u64
    );
    set_from_env!(
        &mut config.routing.distinct_table_owners,
        "ARROWCOL_ROUTER_DISTINCT_TABLE_OWNERS",
        bool
    );
    set_from_env!(
        &mut config.auth.edge_replay_max_nonces,
        "ARROWCOL_ROUTER_EDGE_REPLAY_MAX_NONCES",
        usize
    );
    set_from_env!(
        &mut config.upstream.max_h2_streams,
        "ARROWCOL_ROUTER_UPSTREAM_MAX_H2_STREAMS",
        usize
    );
    set_from_env!(
        &mut config.upstream.stream_window_bytes,
        "ARROWCOL_ROUTER_UPSTREAM_STREAM_WINDOW_BYTES",
        u32
    );
    set_from_env!(
        &mut config.upstream.connection_window_bytes,
        "ARROWCOL_ROUTER_UPSTREAM_CONNECTION_WINDOW_BYTES",
        u32
    );
    set_from_env!(
        &mut config.upstream.connection_timeout_ms,
        "ARROWCOL_ROUTER_UPSTREAM_CONNECTION_TIMEOUT_MS",
        u64
    );
    set_from_env!(
        &mut config.upstream.total_connection_timeout_ms,
        "ARROWCOL_ROUTER_UPSTREAM_TOTAL_CONNECTION_TIMEOUT_MS",
        u64
    );
    set_from_env!(
        &mut config.upstream.idle_timeout_seconds,
        "ARROWCOL_ROUTER_UPSTREAM_IDLE_TIMEOUT_SECONDS",
        u64
    );
    if let Some(value) = get("ARROWCOL_ROUTER_UPSTREAM_PING_INTERVAL_SECONDS")? {
        let seconds = value
            .parse::<u64>()
            .context("parse environment variable ARROWCOL_ROUTER_UPSTREAM_PING_INTERVAL_SECONDS as u64")?;
        config.upstream.ping_interval_seconds = (seconds != 0).then_some(seconds);
    }

    Ok(())
}

fn apply_cli_overrides(config: &mut RouterConfig, overrides: &RouterOverrides) {
    macro_rules! set_from_cli {
        ($source:ident, $target:expr) => {
            if let Some(value) = overrides.$source {
                $target = value;
            }
        };
    }

    set_from_cli!(workers, config.server.threads);
    set_from_cli!(listener_tasks_per_fd, config.server.listener_tasks_per_fd);
    set_from_cli!(keepalive_pool_size, config.server.upstream_keepalive_pool_size);
    set_from_cli!(h2_max_concurrent_streams, config.server.h2.max_concurrent_streams);
    set_from_cli!(h2_stream_window_bytes, config.server.h2.stream_window_bytes);
    set_from_cli!(h2_connection_window_bytes, config.server.h2.connection_window_bytes);
    set_from_cli!(h2_idle_timeout_seconds, config.server.h2.idle_timeout_seconds);
    set_from_cli!(sql_connect_timeout_ms, config.sql_server.connect_timeout_ms);
    set_from_cli!(sql_command_timeout_ms, config.sql_server.command_timeout_ms);
    set_from_cli!(refresh_interval_ms, config.routing.refresh_interval_ms);
    set_from_cli!(max_snapshot_age_ms, config.routing.max_snapshot_age_ms);
    set_from_cli!(max_future_skew_ms, config.routing.max_future_skew_ms);
    set_from_cli!(max_table_entries, config.routing.max_table_entries);
    set_from_cli!(max_oldest_buffer_age_ms, config.routing.max_oldest_buffer_age_ms);
    set_from_cli!(hard_pressure_bps, config.routing.hard_pressure_bps);
    set_from_cli!(warmup_ms, config.routing.warmup_ms);
    set_from_cli!(minimum_warmup_bps, config.routing.minimum_warmup_bps);
    set_from_cli!(lease_ttl_ms, config.routing.lease_ttl_ms);
    set_from_cli!(oat_run_pin_ttl_ms, config.routing.oat_run_pin_ttl_ms);
    set_from_cli!(readiness_max_staleness_ms, config.routing.readiness_max_staleness_ms);
    set_from_cli!(distinct_table_owners, config.routing.distinct_table_owners);
    set_from_cli!(edge_replay_max_nonces, config.auth.edge_replay_max_nonces);
    set_from_cli!(upstream_max_h2_streams, config.upstream.max_h2_streams);
    set_from_cli!(upstream_stream_window_bytes, config.upstream.stream_window_bytes);
    set_from_cli!(
        upstream_connection_window_bytes,
        config.upstream.connection_window_bytes
    );
    set_from_cli!(upstream_connection_timeout_ms, config.upstream.connection_timeout_ms);
    set_from_cli!(
        upstream_total_connection_timeout_ms,
        config.upstream.total_connection_timeout_ms
    );
    set_from_cli!(upstream_idle_timeout_seconds, config.upstream.idle_timeout_seconds);
    if let Some(seconds) = overrides.upstream_ping_interval_seconds {
        config.upstream.ping_interval_seconds = (seconds != 0).then_some(seconds);
    }
}

/// Pingora listener and lifecycle configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Cleartext listener behind an external TLS terminator.
    pub listen: SocketAddr,
    /// Pingora worker threads.
    pub threads: usize,
    /// Listener tasks per file descriptor.
    pub listener_tasks_per_fd: usize,
    /// Maximum idle upstream connections retained by Pingora.
    #[serde(default = "default_upstream_keepalive_pool_size")]
    pub upstream_keepalive_pool_size: usize,
    /// Delay before runtime shutdown begins.
    pub grace_period_seconds: u64,
    /// Final runtime drain ceiling.
    pub graceful_shutdown_timeout_seconds: u64,
    /// PID and upgrade-socket directory.
    pub state_directory: PathBuf,
    /// Downstream HTTP/2 settings.
    pub h2: H2Config,
}

fn default_upstream_keepalive_pool_size() -> usize {
    1_024
}

/// Downstream HTTP/2 flow-control settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct H2Config {
    /// Maximum concurrent streams per client connection.
    pub max_concurrent_streams: u32,
    /// Initial per-stream receive window.
    pub stream_window_bytes: u32,
    /// Initial connection receive window.
    pub connection_window_bytes: u32,
    /// Idle connection timeout.
    pub idle_timeout_seconds: u64,
}

/// SQL Server application database configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlServerConfig {
    /// SQL Server host.
    pub host: String,
    /// SQL Server port.
    pub port: u16,
    /// Application database.
    pub database: String,
    /// SQL login.
    pub username: String,
    /// One-line Docker/Kubernetes secret file.
    pub password_file: PathBuf,
    /// Local-development certificate bypass.
    pub trust_server_certificate: bool,
    /// TCP/login timeout.
    pub connect_timeout_ms: u64,
    /// SQL command timeout.
    pub command_timeout_ms: u64,
}

/// Cache, pressure, selection, and lease settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfig {
    /// Background refresh/rebalance period.
    pub refresh_interval_ms: u64,
    /// Maximum pressure observation age.
    pub max_snapshot_age_ms: u64,
    /// Accepted future clock skew.
    pub max_future_skew_ms: u64,
    /// Maximum table pressure entries.
    pub max_table_entries: usize,
    /// Maximum oldest-buffer observation.
    pub max_oldest_buffer_age_ms: u64,
    /// Hard pressure exclusion threshold.
    pub hard_pressure_bps: u16,
    /// Warm-up duration.
    pub warmup_ms: u64,
    /// Minimum warm-up weight.
    pub minimum_warmup_bps: u16,
    /// Routing lease duration.
    pub lease_ttl_ms: u64,
    /// Maximum lifetime of an immutable OAT run/table owner binding.
    #[serde(default = "default_oat_run_pin_ttl_ms")]
    pub oat_run_pin_ttl_ms: u64,
    /// Exact run ID authorized to create OAT pins in this run-scoped router.
    #[serde(default)]
    pub oat_run_id: Option<String>,
    /// Maximum cache age reported ready.
    pub readiness_max_staleness_ms: u64,
    /// Require each active table lease in a pool to have a distinct collector.
    #[serde(default)]
    pub distinct_table_owners: bool,
}

impl RoutingConfig {
    /// Converts YAML values into the shared routing policy.
    ///
    /// # Errors
    ///
    /// Returns an error when a numeric bound cannot be represented.
    pub fn policy(&self) -> Result<RoutingPolicy> {
        Ok(RoutingPolicy {
            pressure: PressurePolicy {
                max_snapshot_age_ms: self.max_snapshot_age_ms,
                max_future_skew_ms: self.max_future_skew_ms,
                max_table_entries: self.max_table_entries,
                max_oldest_buffer_age_ms: self.max_oldest_buffer_age_ms,
            },
            hard_pressure_bps: self.hard_pressure_bps,
            warmup_ms: self.warmup_ms,
            minimum_warmup_bps: self.minimum_warmup_bps,
            lease_ttl_ms: self.lease_ttl_ms,
            oat_run_pin_ttl_ms: self.oat_run_pin_ttl_ms,
            readiness_max_staleness_ms: self.readiness_max_staleness_ms,
            distinct_table_owners: self.distinct_table_owners,
            oat_run_id: self.oat_run_id.clone(),
        })
    }
}

fn default_oat_run_pin_ttl_ms() -> u64 {
    3_600_000
}

/// Explicit authentication mode; no production identity provider is implemented.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthenticatorMode {
    /// Grants a shared principal after edge verification, for development only.
    DevOnlyAllowAll,
}

/// Authentication and signed internal identity configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// Pluggable authenticator name.
    pub authenticator: AuthenticatorMode,
    /// Secret used by the trusted external TLS terminator.
    pub edge_secret_file: PathBuf,
    /// Separate secret used to sign internal principal metadata.
    pub internal_signing_secret_file: PathBuf,
    /// Maximum edge assertion age.
    pub edge_signature_max_age_ms: u64,
    /// Maximum live verified edge nonces retained per router process.
    #[serde(default = "default_edge_replay_max_nonces")]
    pub edge_replay_max_nonces: usize,
    /// Internal identity validity interval.
    pub internal_identity_ttl_ms: u64,
}

fn default_edge_replay_max_nonces() -> usize {
    65_536
}

/// Upstream HTTP/2 transport and connection reuse configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamConfig {
    /// Use TLS for the internal hop.
    pub tls: bool,
    /// TLS SNI when internal TLS is enabled.
    pub sni: String,
    /// Maximum multiplexed streams per upstream connection.
    pub max_h2_streams: usize,
    /// Initial per-stream receive window.
    pub stream_window_bytes: u32,
    /// Initial connection receive window.
    pub connection_window_bytes: u32,
    /// TCP/TLS connection timeout.
    pub connection_timeout_ms: u64,
    /// Total connection establishment timeout.
    pub total_connection_timeout_ms: u64,
    /// Reusable connection idle timeout.
    pub idle_timeout_seconds: u64,
    /// Optional H2 ping interval.
    pub ping_interval_seconds: Option<u64>,
}

/// Router logging configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    /// Tracing filter directive.
    pub log_filter: String,
    /// Emit JSON log records.
    pub json_logs: bool,
}

#[cfg(test)]
mod routing_policy_tests {
    use super::{Cli, RouterConfig, RoutingConfig, apply_cli_overrides, apply_environment_overrides};
    use clap::Parser;

    #[test]
    fn distinct_table_owners_is_opt_in_and_disabled_for_single_collector_loadtest() {
        let defaults = RouterConfig::from_yaml(include_bytes!("../config.yaml"))
            .unwrap_or_else(|error| panic!("parse default router configuration: {error}"));
        assert!(!defaults.routing.distinct_table_owners);

        let loadtest: serde_yaml::Value = serde_yaml::from_slice(include_bytes!("../../loadtest/config/router.yaml"))
            .unwrap_or_else(|error| panic!("parse loadtest router configuration: {error}"));
        let routing: RoutingConfig = serde_yaml::from_value(loadtest["routing"].clone())
            .unwrap_or_else(|error| panic!("parse loadtest routing policy: {error}"));
        assert!(!routing.distinct_table_owners);
    }

    #[test]
    fn router_cli_overrides_are_typed_and_disable_zero_ping_interval() {
        let overrides = Cli::try_parse_from([
            "arrowcol-router-rust",
            "--workers",
            "8",
            "--upstream-ping-interval-seconds",
            "0",
        ])
        .unwrap_or_else(|error| panic!("parse router CLI overrides: {error}"));
        let mut config = RouterConfig::from_yaml(include_bytes!("../config.yaml"))
            .unwrap_or_else(|error| panic!("parse default router configuration: {error}"));
        apply_cli_overrides(&mut config, &overrides.router);
        config
            .validate()
            .unwrap_or_else(|error| panic!("validate router CLI overrides: {error}"));
        assert_eq!(config.server.threads, 8);
        assert_eq!(config.upstream.ping_interval_seconds, None);
    }

    #[test]
    fn router_environment_overrides_are_typed_and_validated() {
        use std::collections::HashMap;

        let mut config = RouterConfig::from_yaml(include_bytes!("../config.yaml"))
            .unwrap_or_else(|error| panic!("parse default router configuration: {error}"));
        let overrides = HashMap::from([
            ("ARROWCOL_ROUTER_WORKERS", "8"),
            ("ARROWCOL_ROUTER_H2_MAX_CONCURRENT_STREAMS", "512"),
            ("ARROWCOL_ROUTER_UPSTREAM_PING_INTERVAL_SECONDS", "0"),
            ("ARROWCOL_ROUTER_DISTINCT_TABLE_OWNERS", "true"),
        ]);
        apply_environment_overrides(&mut config, |name| {
            Ok(overrides.get(name).map(|value| (*value).to_owned()))
        })
        .unwrap_or_else(|error| panic!("apply typed router overrides: {error}"));
        config
            .validate()
            .unwrap_or_else(|error| panic!("validate router overrides: {error}"));

        assert_eq!(config.server.threads, 8);
        assert_eq!(config.server.h2.max_concurrent_streams, 512);
        assert_eq!(config.upstream.ping_interval_seconds, None);
        assert!(config.routing.distinct_table_owners);
    }

    #[test]
    fn router_environment_override_errors_name_the_variable_and_enforce_cross_bounds() {
        use std::collections::HashMap;

        let mut config = RouterConfig::from_yaml(include_bytes!("../config.yaml"))
            .unwrap_or_else(|error| panic!("parse default router configuration: {error}"));
        let malformed = HashMap::from([("ARROWCOL_ROUTER_WORKERS", "many")]);
        let error = match apply_environment_overrides(&mut config, |name| {
            Ok(malformed.get(name).map(|value| (*value).to_owned()))
        }) {
            Ok(()) => panic!("non-numeric worker count must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("ARROWCOL_ROUTER_WORKERS"));

        let incompatible = HashMap::from([
            ("ARROWCOL_ROUTER_H2_STREAM_WINDOW_BYTES", "1048576"),
            ("ARROWCOL_ROUTER_H2_CONNECTION_WINDOW_BYTES", "65535"),
        ]);
        apply_environment_overrides(&mut config, |name| {
            Ok(incompatible.get(name).map(|value| (*value).to_owned()))
        })
        .unwrap_or_else(|error| panic!("parse numeric H2 overrides: {error}"));
        assert!(config.validate().is_err());
    }
}
