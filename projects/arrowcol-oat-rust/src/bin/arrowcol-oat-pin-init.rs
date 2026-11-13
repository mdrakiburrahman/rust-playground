use anyhow::{Context, Result, bail};
use arrowcol_protocol::{CatalogName, ImplementationPool, SchemaName, TableIdentity, TableName, UnixMillis};
use arrowcol_routing::{
    LeaseKey, RoutingEngine, RoutingPolicy, SqlServerRoutingConfig, SqlServerRoutingStore, run_table_pin_set_digest,
};
use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const DATABASE: &str = "arrowcol";
const MIGRATOR_USERNAME: &str = "arrowcol_migrator";

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    migrator_password_file: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PinInitConfig {
    schema_version: u32,
    run_id: String,
    expires_at_unix_ms: u64,
    expected_keys: Vec<ExpectedPinKey>,
    expected_digest: String,
    routing_policy: RoutingPolicy,
    sql_server: SqlServerConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedPinKey {
    implementation_pool: String,
    catalog_name: String,
    schema_name: String,
    table_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SqlServerConfig {
    host: String,
    port: u16,
    trust_server_certificate: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let bytes = std::fs::read(&cli.config)
        .with_context(|| format!("read OAT pin initialization config {}", cli.config.display()))?;
    let config: PinInitConfig = serde_json::from_slice(&bytes).context("parse OAT pin initialization config")?;
    let expected = validate_config(&config)?;
    let actual_digest = config.expected_digest.as_str();

    let now = unix_time_ms()?;
    let expires = validate_expiry(&config, now)?;

    let sql_config = SqlServerRoutingConfig::with_password_file(
        &config.sql_server.host,
        config.sql_server.port,
        DATABASE,
        MIGRATOR_USERNAME,
        &cli.migrator_password_file,
    )?
    .trust_server_certificate(config.sql_server.trust_server_certificate)
    .with_timeouts(Duration::from_secs(15), Duration::from_secs(30));
    let store = SqlServerRoutingStore::new(sql_config);
    let engine = RoutingEngine::new(Arc::new(store), config.routing_policy)
        .map_err(anyhow::Error::msg)
        .context("construct migrator-authenticated OAT routing engine")?;
    engine
        .initialize_run_table_pins(&config.run_id, &expected, expires, now)
        .await
        .map_err(anyhow::Error::msg)
        .context("initialize and bind complete immutable OAT run-table pin set")?;

    println!(
        "initialized OAT pin set run_id={} tables={} digest={}",
        config.run_id,
        expected.len(),
        actual_digest
    );
    Ok(())
}

fn validate_config(config: &PinInitConfig) -> Result<Vec<LeaseKey>> {
    if config.schema_version != 1 {
        bail!("unsupported OAT pin initialization config schema_version");
    }
    let run_id = Uuid::parse_str(&config.run_id).context("OAT pin initialization requires a UUID run_id")?;
    if run_id.to_string() != config.run_id {
        bail!("OAT pin initialization run_id must be canonical lowercase UUID");
    }
    if config.expected_keys.is_empty() {
        bail!("OAT pin initialization requires at least one expected table key");
    }
    if config.routing_policy.oat_run_id.as_deref() != Some(config.run_id.as_str()) {
        bail!("OAT pin initialization routing policy must authorize this exact run_id");
    }
    config
        .routing_policy
        .validate()
        .map_err(anyhow::Error::msg)
        .context("validate OAT pin initialization routing policy")?;
    if config.expected_digest.len() != 64
        || !config
            .expected_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("OAT pin initialization expected_digest must be lowercase SHA-256 hex");
    }
    if config.sql_server.host.is_empty()
        || config.sql_server.host.len() > 255
        || config.sql_server.host.chars().any(char::is_control)
        || config.sql_server.port == 0
    {
        bail!("OAT pin initialization SQL endpoint is invalid");
    }

    let keys = config
        .expected_keys
        .iter()
        .map(|key| {
            let pool = key
                .implementation_pool
                .parse::<ImplementationPool>()
                .context("parse OAT pin implementation_pool")?;
            let table = TableIdentity::new(
                CatalogName::new(key.catalog_name.clone())?,
                SchemaName::new(key.schema_name.clone())?,
                TableName::new(key.table_name.clone())?,
            );
            Ok(LeaseKey::new(pool, table))
        })
        .collect::<Result<Vec<_>>>()?;
    if keys.len() > 32
        || keys.windows(2).any(|pair| pair[0] >= pair[1])
        || keys
            .iter()
            .any(|key| key.implementation_pool != ImplementationPool::Dataflow)
    {
        bail!("OAT pin initialization keys must be 1..=32 strictly sorted, unique dataflow identities");
    }
    let actual_digest = run_table_pin_set_digest(&keys)?;
    if actual_digest != config.expected_digest {
        bail!("OAT pin initialization table-set digest does not match expected_digest");
    }
    Ok(keys)
}

fn validate_expiry(config: &PinInitConfig, now: UnixMillis) -> Result<UnixMillis> {
    let expires = UnixMillis::new(config.expires_at_unix_ms)?;
    if expires <= now {
        bail!("OAT pin initialization run pin set is already expired");
    }
    if expires.get().saturating_sub(now.get()) > config.routing_policy.oat_run_pin_ttl_ms {
        bail!("OAT pin initialization expiry exceeds the configured router pin lifetime");
    }
    Ok(expires)
}

fn unix_time_ms() -> Result<UnixMillis> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock precedes UNIX epoch")?;
    let millis = u64::try_from(duration.as_millis()).context("system time exceeds routing timestamp range")?;
    UnixMillis::new(millis).context("system time is outside routing timestamp bounds")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Result<PinInitConfig> {
        let key = LeaseKey::new(
            ImplementationPool::Dataflow,
            TableIdentity::new(
                CatalogName::new("sqlserver")?,
                SchemaName::new("dbo")?,
                TableName::new("wait_stats")?,
            ),
        );
        let expected_digest = run_table_pin_set_digest(&[key])?;
        Ok(PinInitConfig {
            schema_version: 1,
            run_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            expires_at_unix_ms: 10_000,
            expected_keys: vec![ExpectedPinKey {
                implementation_pool: "dataflow".to_owned(),
                catalog_name: "sqlserver".to_owned(),
                schema_name: "dbo".to_owned(),
                table_name: "wait_stats".to_owned(),
            }],
            expected_digest,
            routing_policy: RoutingPolicy {
                oat_run_id: Some("00000000-0000-0000-0000-000000000001".to_owned()),
                ..RoutingPolicy::default()
            },
            sql_server: SqlServerConfig {
                host: "sqlserver".to_owned(),
                port: 1433,
                trust_server_certificate: true,
            },
        })
    }

    #[test]
    fn pin_init_config_requires_matching_digest_and_validated_table_identities() -> Result<()> {
        let valid = config()?;
        assert_eq!(validate_config(&valid)?.len(), 1);
        assert_eq!(
            validate_expiry(&valid, UnixMillis::new(9_000)?)?,
            UnixMillis::new(10_000)?
        );

        let mut too_long_expiry = config()?;
        too_long_expiry.routing_policy.oat_run_pin_ttl_ms = 500;
        assert!(validate_expiry(&too_long_expiry, UnixMillis::new(9_000)?).is_err());

        let mut changed_key = config()?;
        changed_key.expected_keys[0].table_name = "other_table".to_owned();
        assert!(validate_config(&changed_key).is_err());

        let mut unsupported_pool = config()?;
        unsupported_pool.expected_keys[0].implementation_pool = "unknown".to_owned();
        assert!(validate_config(&unsupported_pool).is_err());
        Ok(())
    }

    #[test]
    fn pin_init_config_rejects_unexpected_credential_fields() {
        let error = serde_json::from_str::<PinInitConfig>(
            r#"{
                "schema_version": 1,
                "run_id": "00000000-0000-0000-0000-000000000001",
                "expires_at_unix_ms": 10000,
                "expected_keys": [],
                "expected_digest": "",
                "routing_policy": {
                    "pressure": {
                        "max_snapshot_age_ms": 15000,
                        "max_future_skew_ms": 2000,
                        "max_table_entries": 256,
                        "max_oldest_buffer_age_ms": 86400000
                    },
                    "hard_pressure_bps": 9500,
                    "warmup_ms": 1000,
                    "minimum_warmup_bps": 1000,
                    "lease_ttl_ms": 3600000,
                    "oat_run_pin_ttl_ms": 3600000,
                    "readiness_max_staleness_ms": 30000,
                    "distinct_table_owners": false,
                    "oat_run_id": "00000000-0000-0000-0000-000000000001"
                },
                "sql_server": {
                    "host": "sqlserver",
                    "port": 1433,
                    "trust_server_certificate": true
                },
                "password": null
            }"#,
        );
        assert!(error.is_err());
    }
}
