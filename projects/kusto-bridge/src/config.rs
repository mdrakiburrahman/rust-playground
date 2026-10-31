use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result, ensure};
use clap::Args;
use url::Url;

/// One Delta source pool mounted at the same path in the bridge and Kusto containers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    pub pool: String,
    pub root: PathBuf,
}

/// Runtime configuration, read from flags or the `KUSTO_BRIDGE_*` environment.
#[derive(Args, Clone, Debug)]
pub struct Config {
    /// Kusto emulator origin, for example `http://10.89.0.50:8080`.
    #[arg(long, env = "KUSTO_BRIDGE_KUSTO_ENDPOINT")]
    pub kusto_endpoint: Url,
    /// Kusto database that receives the simulated accelerated tables.
    #[arg(long, env = "KUSTO_BRIDGE_DATABASE", default_value = "loadtest")]
    pub database: String,
    /// Comma-separated `pool=/path` Delta source roots. Every direct child that holds
    /// `_delta_log` (recursively, through dotted table namespaces) becomes one Kusto table.
    #[arg(long, env = "KUSTO_BRIDGE_SOURCES", value_parser = parse_sources)]
    pub sources: Sources,
    /// Directory for the restart checkpoint (`checkpoint.json`).
    #[arg(long, env = "KUSTO_BRIDGE_STATE_DIR", default_value = "/state")]
    pub state_dir: PathBuf,
    /// Delay between Delta log polls.
    #[arg(long, env = "KUSTO_BRIDGE_POLL_INTERVAL", default_value = "2s", value_parser = parse_duration)]
    pub poll_interval: Duration,
    /// HTTP listen address for `/healthz`, `/status`, `/refresh`, and `/metrics`.
    #[arg(long, env = "KUSTO_BRIDGE_LISTEN", default_value = "0.0.0.0:9464")]
    pub listen: SocketAddr,
    /// Per-request Kusto timeout.
    #[arg(long, env = "KUSTO_BRIDGE_REQUEST_TIMEOUT", default_value = "120s", value_parser = parse_duration)]
    pub request_timeout: Duration,
    /// Accept a changed Delta table identity or schema by dropping and rebuilding the Kusto table.
    /// Without it, such tables stay degraded until an operator resets state.
    #[arg(long, env = "KUSTO_BRIDGE_RESYNC_ON_IDENTITY_CHANGE", default_value_t = false)]
    pub resync_on_identity_change: bool,
}

/// Parsed `KUSTO_BRIDGE_SOURCES`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sources(pub Vec<Source>);

/// Parse `pool=/path[,pool=/path]`.
///
/// # Errors
/// Rejects empty lists, duplicate pools, non-identifier pool names, and relative paths.
pub fn parse_sources(value: &str) -> Result<Sources> {
    let mut sources = Vec::new();
    for entry in value.split(',').map(str::trim).filter(|entry| !entry.is_empty()) {
        let (pool, root) = entry.split_once('=').context("source must be pool=/absolute/path")?;
        ensure!(
            !pool.is_empty()
                && pool
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                && !pool.starts_with(|c: char| c.is_ascii_digit()),
            "invalid source pool {pool:?}"
        );
        let root = PathBuf::from(root);
        ensure!(root.is_absolute(), "source root for {pool} must be absolute");
        ensure!(
            sources.iter().all(|source: &Source| source.pool != pool),
            "duplicate source pool {pool}"
        );
        sources.push(Source {
            pool: pool.to_owned(),
            root,
        });
    }
    ensure!(!sources.is_empty(), "at least one source is required");
    Ok(Sources(sources))
}

/// Parse `500ms`, `2s`, or `1m`.
///
/// # Errors
/// Rejects unknown units, zero, and non-numeric values.
pub fn parse_duration(value: &str) -> Result<Duration> {
    let value = value.trim();
    let (number, unit) = value
        .find(|c: char| !c.is_ascii_digit())
        .map_or((value, "s"), |index| value.split_at(index));
    let number: u64 = number.parse().context("duration must start with an integer")?;
    let duration = match unit {
        "ms" => Duration::from_millis(number),
        "s" => Duration::from_secs(number),
        "m" => Duration::from_secs(number * 60),
        other => anyhow::bail!("unsupported duration unit {other:?}"),
    };
    ensure!(!duration.is_zero(), "duration must be positive");
    Ok(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sources_and_durations_parse_strictly() -> Result<()> {
        let sources = parse_sources("direct=/delta/direct, dataflow=/delta/dataflow")?;
        assert_eq!(sources.0.len(), 2);
        assert_eq!(sources.0[1].pool, "dataflow");
        assert!(parse_sources("").is_err());
        assert!(parse_sources("direct=relative").is_err());
        assert!(parse_sources("direct=/a,direct=/b").is_err());
        assert!(parse_sources("Bad-Pool=/a").is_err());
        assert_eq!(parse_duration("250ms")?, Duration::from_millis(250));
        assert_eq!(parse_duration("2s")?, Duration::from_secs(2));
        assert_eq!(parse_duration("3")?, Duration::from_secs(3));
        assert!(parse_duration("0s").is_err());
        assert!(parse_duration("1h").is_err());
        Ok(())
    }
}
