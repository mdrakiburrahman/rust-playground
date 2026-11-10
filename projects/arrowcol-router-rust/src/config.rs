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
        let path = path.as_ref();
        let bytes = std::fs::read(path).with_context(|| format!("read router config {}", path.display()))?;
        Self::from_yaml(&bytes).with_context(|| format!("parse strict YAML {}", path.display()))
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
        ensure!(self.server.threads > 0, "server.threads must be positive");
        ensure!(
            self.server.listener_tasks_per_fd > 0,
            "server.listener_tasks_per_fd must be positive"
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
            self.upstream.max_h2_streams > 0
                && self.upstream.stream_window_bytes > 0
                && self.upstream.connection_window_bytes >= self.upstream.stream_window_bytes,
            "upstream H2 stream/window bounds are invalid"
        );
        ensure!(
            self.server.h2.max_concurrent_streams > 0
                && self.server.h2.stream_window_bytes > 0
                && self.server.h2.connection_window_bytes >= self.server.h2.stream_window_bytes,
            "downstream H2 stream/window bounds are invalid"
        );
        Ok(())
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
    /// Delay before runtime shutdown begins.
    pub grace_period_seconds: u64,
    /// Final runtime drain ceiling.
    pub graceful_shutdown_timeout_seconds: u64,
    /// PID and upgrade-socket directory.
    pub state_directory: PathBuf,
    /// Downstream HTTP/2 settings.
    pub h2: H2Config,
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
    use super::{RouterConfig, RoutingConfig};

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
}
