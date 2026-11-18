use anyhow::{Context, Result, bail};
use arrow_deltalake::delta::{
    DeltaTable, FreshTableReceipt, FreshTableTransport, INGESTION_PARTITION, PublicationFence, PublicationFenceContext,
    PublicationFenceError, object_store_for_url,
};
use arrow_deltalake::publisher::{MutationRetries, PublisherSession};
use arrowcol_protocol::{CatalogName, ImplementationPool, SchemaName, TableIdentity, TableName, UnixMillis};
use arrowcol_routing::{
    LeaseKey, RoutingEngine, RoutingPolicy, SqlServerRoutingConfig, SqlServerRoutingStore, run_table_pin_set_digest,
};
use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use url::Url;
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
    #[serde(default)]
    run_table: Option<RunTableInitialization>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunTableInitialization {
    delta_root: String,
    table: String,
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
    if let Some(run_table) = config.run_table.as_ref() {
        let receipt = initialize_fresh_run_table(run_table).await?;
        println!(
            "{}",
            serde_json::json!({
                "kind": "oat_run_table_initialized",
                "run_id": config.run_id,
                "table": run_table.table,
                "publisher_drained": true,
                "delta": receipt,
            })
        );
    }
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
    validate_run_table_initialization(config.run_table.as_ref(), &config.run_id)?;
    Ok(keys)
}

const HOT_RUN_TABLE: &str = "telemetry.sqlserver.sqlserver_availability_replica_states";

fn validate_run_table_initialization(run_table: Option<&RunTableInitialization>, run_id: &str) -> Result<()> {
    let Some(run_table) = run_table else {
        return Ok(());
    };
    let root = Url::parse(&run_table.delta_root).context("parse run-scoped table initialization root")?;
    let expected_suffix = format!("/runs/{run_id}/dataflow");
    if root.scheme() != "abfss"
        || root.username().is_empty()
        || root.password().is_some()
        || root.query().is_some()
        || root.fragment().is_some()
        || !root.path().ends_with(&expected_suffix)
        || root.path().contains('%')
        || root.path().contains('\\')
        || root
            .path()
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        bail!("run-table initialization root must be the exact credential-free run Dataflow root");
    }
    if run_table.table != HOT_RUN_TABLE {
        bail!("run-table initialization only permits the canonical OAT hot table");
    }
    Ok(())
}

struct RunTableInitializationFence {
    session: Arc<PublisherSession>,
}

impl PublicationFence for RunTableInitializationFence {
    fn check_before_commit(&self, context: &PublicationFenceContext<'_>) -> Result<(), PublicationFenceError> {
        if context.read_version() != 0 || context.proposed_version() != 0 || context.file_count() != 0 {
            return Err(PublicationFenceError::stale(
                "OAT run-table initialization permits only an empty version0 schema commit",
            ));
        }
        self.session.check().map_err(|error| {
            PublicationFenceError::check_failed(format!("OAT run-table publisher authority check failed: {error:#}"))
        })
    }
}

async fn initialize_fresh_run_table(run_table: &RunTableInitialization) -> Result<FreshTableReceipt> {
    let root = Url::parse(&run_table.delta_root)?;
    let (url, schema) = arrowcol_loadgen::initialize::canonical_table_target(&root, &run_table.table)?;
    let store = object_store_for_url(&url)?;
    let session = PublisherSession::open_async(&url).await?;
    let fence = Arc::new(RunTableInitializationFence {
        session: Arc::clone(&session),
    });
    let init_url = url.clone();
    let init_session = Arc::clone(&session);
    let initialization = tokio::task::spawn_blocking(move || {
        DeltaTable::initialize_fresh_url_with_session(
            init_url,
            schema,
            vec![INGESTION_PARTITION.to_owned()],
            init_session,
            fence,
            FreshTableTransport {
                store,
                mutation_retries: MutationRetries::Disabled,
            },
        )
    })
    .await
    .map_err(|error| anyhow::anyhow!("join run-table schema initialization: {error}"))?;
    let drained = session.close_after_drain_async().await;
    match (initialization, drained) {
        (Ok(receipt), Ok(())) => Ok(receipt),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error.context("drain run-table initializer publisher")),
        (Err(error), Err(drain_error)) => {
            Err(error.context(format!("run-table initializer publisher drain failed: {drain_error:#}")))
        }
    }
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
            run_table: None,
        })
    }

    #[test]
    fn pin_init_config_requires_matching_digest_and_validated_table_identities() -> Result<()> {
        let valid = config()?;
        assert_eq!(validate_config(&valid)?.len(), 1);
        let run_id = "00000000-0000-0000-0000-000000000001";
        let valid_run_table = RunTableInitialization {
            delta_root: format!(
                "abfss://tables@arrowcollector1.dfs.core.windows.net/oat-prefix/runs/{run_id}/dataflow"
            ),
            table: HOT_RUN_TABLE.to_owned(),
        };
        validate_run_table_initialization(Some(&valid_run_table), run_id)?;
        let mut wrong_table = valid_run_table;
        wrong_table.table = "telemetry.sqlserver.sqlserver_wait_stats".to_owned();
        assert!(validate_run_table_initialization(Some(&wrong_table), run_id).is_err());
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
