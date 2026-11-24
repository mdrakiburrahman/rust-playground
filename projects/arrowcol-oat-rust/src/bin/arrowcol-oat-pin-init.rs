use anyhow::{Context, Result, bail};
use arrow_deltalake::delta::{
    DeltaTable, FreshTableReceipt, FreshTableTransport, INGESTION_PARTITION, InitialDeltaProperties, PublicationFence,
    PublicationFenceContext, PublicationFenceError, object_store_for_url,
};
use arrow_deltalake::publisher::{MutationRetries, PublisherSession};
use arrowcol_protocol::{CatalogName, ImplementationPool, SchemaName, TableIdentity, TableName, UnixMillis};
use arrowcol_routing::{
    LeaseKey, RoutingEngine, RoutingPolicy, SqlServerRoutingConfig, SqlServerRoutingStore, run_table_pin_set_digest,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use url::Url;
use uuid::Uuid;

const DATABASE: &str = "arrowcol";
const MIGRATOR_USERNAME: &str = "arrowcol_migrator";

#[derive(Serialize)]
struct RegisteredRunTableReceipt<'a, P: Serialize> {
    kind: &'static str,
    run_id: &'a str,
    table: String,
    location: &'a str,
    provider: &'a str,
    schema_version: u64,
    schema_fingerprint: String,
    catalog_table_count: usize,
    properties: &'a P,
}

#[derive(Debug, Parser)]
#[command(group(clap::ArgGroup::new("registration").args(["register_only_config", "register_maintenance_only_config"])))]
struct Cli {
    #[arg(
        long,
        required_unless_present = "registration",
        conflicts_with = "registration"
    )]
    config: Option<PathBuf>,
    #[arg(
        long,
        required_unless_present = "registration",
        conflicts_with = "registration"
    )]
    migrator_password_file: Option<PathBuf>,
    #[arg(long, requires_all = ["edge_secret_file", "internal_secret_file"])]
    register_only_config: Option<PathBuf>,
    #[arg(long, requires_all = ["edge_secret_file", "internal_secret_file"])]
    register_maintenance_only_config: Option<PathBuf>,
    #[arg(long, requires = "registration")]
    edge_secret_file: Option<PathBuf>,
    #[arg(long, requires = "registration")]
    internal_secret_file: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterOnlyConfig {
    schema_version: u32,
    run_id: String,
    table: String,
    delta_root: String,
    endpoint: String,
    catalog_endpoint: String,
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
    #[serde(default)]
    delta_properties: InitialDeltaProperties,
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
    let maintenance_registration = cli.register_maintenance_only_config.is_some();
    if let Some(path) = cli.register_only_config.or(cli.register_maintenance_only_config) {
        let config: RegisterOnlyConfig = serde_json::from_slice(
            &std::fs::read(&path).with_context(|| format!("read catalog registration config {}", path.display()))?,
        )
        .context("parse catalog registration config")?;
        let receipt = register_run_table(
            &config,
            maintenance_registration,
            &cli.edge_secret_file
                .context("catalog registration requires the client edge signing key")?,
            &cli.internal_secret_file
                .context("catalog readback requires the client internal signing key")?,
        )
        .await?;
        println!("{receipt}");
        return Ok(());
    }
    let path = cli.config.context("pin initialization config is required")?;
    let bytes =
        std::fs::read(&path).with_context(|| format!("read OAT pin initialization config {}", path.display()))?;
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
        &cli.migrator_password_file
            .context("pin initialization migrator password file is required")?,
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

fn validate_register_only_config(config: &RegisterOnlyConfig, maintenance_registration: bool) -> Result<Url> {
    let run_id = Uuid::parse_str(&config.run_id).context("registration requires a UUID run_id")?;
    let root = Url::parse(&config.delta_root).context("parse registration Delta root")?;
    if config.schema_version != 1
        || run_id.to_string() != config.run_id
        || (!maintenance_registration && config.table != HOT_RUN_TABLE)
        || !matches!(root.scheme(), "abfss" | "file")
        || root.password().is_some()
        || root.query().is_some()
        || root.fragment().is_some()
        || !root.path().ends_with(&format!("/runs/{run_id}/dataflow"))
    {
        bail!("catalog registration requires the selected canonical table contract and run-scoped Dataflow root");
    }
    for endpoint in [&config.endpoint, &config.catalog_endpoint] {
        let endpoint = Url::parse(endpoint).context("parse registration service endpoint")?;
        if endpoint.scheme() != "http"
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint.path() != "/"
        {
            bail!("catalog registration endpoints must be plain credential-free HTTP service URLs");
        }
    }
    Ok(arrowcol_loadgen::initialize::canonical_table_target(&root, &config.table)?.0)
}

async fn register_run_table(
    config: &RegisterOnlyConfig,
    maintenance_registration: bool,
    edge_secret_file: &std::path::Path,
    internal_secret_file: &std::path::Path,
) -> Result<serde_json::Value> {
    let expected_location = validate_register_only_config(config, maintenance_registration)?;
    let completion =
        collector_chain::dmv::register_table_authenticated(&config.endpoint, &config.table, edge_secret_file)
            .await
            .context("register the initialized run table through the DMV client Flight SQL route")?;
    let snapshot = arrowcol_loadgen::initialize::catalog_snapshot(&config.catalog_endpoint, internal_secret_file)
        .await
        .context("read back catalog registration through signed GetSnapshot")?;
    let [cached] = snapshot.as_slice() else {
        bail!("catalog registration snapshot must contain exactly the canonical run table");
    };
    let table = &cached.snapshot;
    let head = cached
        .versions
        .last()
        .context("catalog registration snapshot has no schema history")?;
    let actual_location = if std::path::Path::new(&table.location).is_absolute() {
        Url::from_directory_path(&table.location).map_err(|()| anyhow::anyhow!("invalid catalog table directory"))?
    } else {
        Url::parse(&table.location).context("invalid catalog registration location URL")?
    };
    if head.identity().to_string() != config.table
        || table.provider != "delta"
        || actual_location.as_str().trim_end_matches('/') != expected_location.as_str().trim_end_matches('/')
        || head.version() != completion.schema_version()
        || head.fingerprint() != completion.schema_fingerprint()
        || head.metadata().provider != table.provider
        || head.metadata().location.as_deref() != Some(table.location.as_str())
        || head
            .metadata()
            .properties
            .iter()
            .any(|(key, value)| table.properties.get(key) != Some(value))
        || head.metadata().properties.len() != table.properties.len()
    {
        bail!("catalog registration readback does not prove the exact canonical schema at the run-scoped location");
    }
    Ok(serde_json::to_value(RegisteredRunTableReceipt {
        kind: "oat_run_table_registered",
        run_id: &config.run_id,
        table: head.identity().to_string(),
        location: &table.location,
        provider: &table.provider,
        schema_version: completion.schema_version().get(),
        schema_fingerprint: completion.schema_fingerprint().to_string(),
        catalog_table_count: snapshot.len(),
        properties: &table.properties,
    })?)
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
    run_table.delta_properties.properties()?;
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
    let properties = run_table.delta_properties.clone();
    let initialization = tokio::task::spawn_blocking(move || {
        DeltaTable::initialize_fresh_url_with_session_and_properties(
            init_url,
            schema,
            vec![INGESTION_PARTITION.to_owned()],
            init_session,
            fence,
            FreshTableTransport {
                store,
                mutation_retries: MutationRetries::Disabled,
            },
            &properties,
        )
    })
    .await
    .map_err(|error| anyhow::anyhow!("join run-table schema initialization: {error}"))
    .and_then(|result| result);
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

    #[test]
    fn registration_receipt_preserves_actual_snapshot_properties_including_unrequested_keys() -> Result<()> {
        let mut actual = InitialDeltaProperties {
            target_file_size_bytes: Some(67108864),
            checkpoint_interval: Some(17),
        }
        .properties()?;
        actual.insert("delta.dataSkippingStatsColumns".to_owned(), "sample".to_owned());
        for properties in [actual, Default::default()] {
            let receipt = serde_json::to_value(RegisteredRunTableReceipt {
                kind: "oat_run_table_registered",
                run_id: "00000000-0000-0000-0000-000000000001",
                table: HOT_RUN_TABLE.to_owned(),
                location: "file:///runs/00000000-0000-0000-0000-000000000001/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states",
                provider: "delta",
                schema_version: 1,
                schema_fingerprint: "fixture".to_owned(),
                catalog_table_count: 1,
                properties: &properties,
            })?;
            assert_eq!(receipt["properties"], serde_json::to_value(properties)?);
        }
        Ok(())
    }

    #[test]
    fn readme_maintenance_registration_has_a_separate_authenticated_mode() -> Result<()> {
        let args = [
            "pin-init",
            "--register-maintenance-only-config",
            "register.json",
            "--edge-secret-file",
            "edge",
            "--internal-secret-file",
            "internal",
        ];
        assert!(Cli::try_parse_from(args).is_ok());
        assert!(Cli::try_parse_from(&args[..5]).is_err());
        let mut conflicting = args.to_vec();
        conflicting.extend(["--register-only-config", "hot-table.json"]);
        assert!(Cli::try_parse_from(conflicting).is_err());
        Ok(())
    }

    #[test]
    fn register_only_mode_is_run_bound_and_does_not_require_migrator_credentials() -> Result<()> {
        let run_id = "00000000-0000-0000-0000-000000000001";
        let mut config = RegisterOnlyConfig {
            schema_version: 1,
            run_id: run_id.to_owned(),
            table: HOT_RUN_TABLE.to_owned(),
            delta_root: format!("file:///tmp/oat/runs/{run_id}/dataflow"),
            endpoint: "http://router:50050".to_owned(),
            catalog_endpoint: "http://catalog:8817".to_owned(),
        };
        validate_register_only_config(&config, false)?;
        config.endpoint = "http://user:password@router:50050".to_owned();
        assert!(validate_register_only_config(&config, false).is_err());
        config.endpoint = "http://router:50050".to_owned();
        config.run_id = "00000000-0000-0000-0000-000000000002".to_owned();
        assert!(validate_register_only_config(&config, false).is_err());
        config.run_id = run_id.to_owned();
        config.table = "telemetry.sqlserver.sqlserver_wait_stats".to_owned();
        assert!(validate_register_only_config(&config, false).is_err());
        validate_register_only_config(&config, true)?;
        config.table = "telemetry.sqlserver.sqlserver_active_sessions".to_owned();
        let location = validate_register_only_config(&config, true)?;
        assert!(location.path().trim_end_matches('/').ends_with("/telemetry/sqlserver/sqlserver_active_sessions"));
        for table in ["sqlserver_active_sessions", "telemetry.sqlserver.unknown_table"] {
            config.table = table.to_owned();
            assert!(validate_register_only_config(&config, true).is_err());
        }
        config.table = "telemetry.sqlserver.sqlserver_active_sessions".to_owned();
        config.delta_root = "file:///tmp/shared/dataflow".to_owned();
        assert!(validate_register_only_config(&config, true).is_err());
        config.delta_root = format!("file:///tmp/oat/runs/{run_id}/dataflow");
        config.catalog_endpoint = "http://user:password@catalog:8817".to_owned();
        assert!(validate_register_only_config(&config, true).is_err());
        let args = [
            "pin-init",
            "--register-only-config",
            "register.json",
            "--edge-secret-file",
            "edge",
            "--internal-secret-file",
            "internal",
        ];
        assert!(Cli::try_parse_from(args)?.migrator_password_file.is_none());
        assert!(Cli::try_parse_from(&args[..5]).is_err());
        assert!(Cli::try_parse_from(["pin-init", "--config", "pins.json"]).is_err());
        assert!(
            Cli::try_parse_from([
                "pin-init",
                "--config",
                "pins.json",
                "--migrator-password-file",
                "password"
            ])
            .is_ok()
        );
        Ok(())
    }

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
    fn pin_init_literal_properties_are_optional_strict_and_validated_before_admission() -> Result<()> {
        let run_id = "00000000-0000-0000-0000-000000000001";
        let base = serde_json::json!({
            "delta_root": format!("abfss://tables@arrowcollector1.dfs.core.windows.net/oat-prefix/runs/{run_id}/dataflow"),
            "table": HOT_RUN_TABLE
        });
        let default: RunTableInitialization = serde_json::from_value(base.clone())?;
        assert!(default.delta_properties.properties()?.is_empty());
        let mut configured = base.clone();
        configured["delta_properties"] =
            serde_json::json!({"target_file_size_bytes":67108864,"checkpoint_interval":17});
        let parsed: RunTableInitialization = serde_json::from_value(configured.clone())?;
        validate_run_table_initialization(Some(&parsed), run_id)?;
        let properties = parsed.delta_properties.properties()?;
        assert_eq!(properties["delta.targetFileSize"], "67108864");
        assert_eq!(properties["delta.checkpointInterval"], "17");
        for field in ["target_file_size_bytes", "checkpoint_interval"] {
            let mut invalid = configured.clone();
            invalid["delta_properties"][field] = serde_json::json!(0);
            let parsed: RunTableInitialization = serde_json::from_value(invalid)?;
            assert!(validate_run_table_initialization(Some(&parsed), run_id).is_err());
        }
        configured["delta_properties"]["unsupported"] = serde_json::json!(1);
        assert!(serde_json::from_value::<RunTableInitialization>(configured).is_err());
        Ok(())
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
            delta_properties: InitialDeltaProperties::default(),
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
