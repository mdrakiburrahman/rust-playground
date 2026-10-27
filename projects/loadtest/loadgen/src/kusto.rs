//! Kusto validation over native Delta external tables with query acceleration (QAP).
//!
//! Locally the emulator reads the committed Delta snapshots directly through `kind=delta`
//! external tables with a one-minute `MaxAge` acceleration policy; no rows are copied. In Azure
//! mode the provisioned external tables are only queried. Correctness is checked twice per table:
//! once through the non-accelerated fallback (`MaxAge` override of one second) and once through the
//! accelerated cache after it reports complete, fresh, and eligible state for the captured commit.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use arrow_deltalake::AzureCliTokenProvider;
use delta_inspect::{SnapshotInventory, SnapshotSummary, TableInventory, TableSummary};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{cli::RunArgs, transport::now_unix_millis};

const DATABASE: &str = "loadtest";
const STATUS: &str = "kusto-status.json";
const BACKEND: &str = "native-qap";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const CACHE_POLL_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_CACHE_TIMEOUT: Duration = Duration::from_secs(300);
const TOKEN_TIMEOUT: Duration = Duration::from_secs(60);
/// Shortest policy the service accepts; bounds cache staleness after each Delta commit.
const LOCAL_MAX_AGE: &str = "00:01:00";
const LOCAL_HOT_WINDOW: &str = "1.00:00:00";
const TABLES_PER_POOL: usize = 13;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Mode {
    Local,
    Azure,
}

impl Mode {
    fn from_env() -> Result<Self> {
        match std::env::var("ARROWCOL_MODE").ok().as_deref().map(str::trim) {
            None | Some("" | "local") => Ok(Self::Local),
            Some("azure") => Ok(Self::Azure),
            Some(other) => bail!("unsupported ARROWCOL_MODE {other:?}"),
        }
    }
}

#[derive(Clone)]
struct KustoClient {
    http: Client,
    endpoint: Url,
    database: String,
    mode: Mode,
    results: PathBuf,
    /// Shared bounded Azure CLI provider scoped to this cluster; absent in local mode.
    token: Option<Arc<AzureCliTokenProvider>>,
}

impl KustoClient {
    fn new(endpoint: &str, results: &Path) -> Result<Self> {
        let mode = Mode::from_env()?;
        let database = match mode {
            Mode::Local => DATABASE.to_owned(),
            Mode::Azure => {
                ensure!(
                    !std::env::var("LOADTEST_KUSTO_BRIDGE_ENDPOINT").is_ok_and(|value| !value.trim().is_empty()),
                    "the local Kusto bridge cannot be used in Azure mode"
                );
                std::env::var("AZURE_KUSTO_DATABASE").context("AZURE_KUSTO_DATABASE is required in Azure mode")?
            }
        };
        Self::with_mode(endpoint, results, mode, &database)
    }

    fn with_mode(endpoint: &str, results: &Path, mode: Mode, database: &str) -> Result<Self> {
        let endpoint = Url::parse(endpoint).context("invalid Kusto endpoint")?;
        let scheme = match mode {
            Mode::Local => "http",
            Mode::Azure => "https",
        };
        ensure!(
            endpoint.scheme() == scheme
                && endpoint.host_str().is_some()
                && endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none()
                && endpoint.path() == "/",
            "Kusto endpoint must be a plain {scheme} origin without credentials"
        );
        ensure!(
            !database.is_empty()
                && database.len() <= 128
                && database
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-')),
            "invalid Kusto database name"
        );
        let token = match mode {
            Mode::Local => None,
            Mode::Azure => Some(Arc::new(AzureCliTokenProvider::new(
                &format!("https://{}/.default", endpoint.host_str().unwrap_or_default()),
                TOKEN_TIMEOUT,
            )?)),
        };
        Ok(Self {
            http: Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .connect_timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            endpoint,
            database: database.to_owned(),
            mode,
            results: results.to_owned(),
            token,
        })
    }

    /// Azure tokens come from the ambient Azure CLI login and are never logged or persisted.
    async fn token(&self) -> Result<Option<String>> {
        match &self.token {
            None => Ok(None),
            Some(provider) => provider.token().await.map(Some),
        }
    }

    async fn execute(&self, command: &str, management: bool, phase: &str) -> Result<Vec<BTreeMap<String, Value>>> {
        let route = if management { "v1/rest/mgmt" } else { "v2/rest/query" };
        let mut request = json!({
            "csl": command,
            "properties": "{\"Options\":{\"notruncation\":true}}"
        });
        if phase != "bootstrap" {
            request["db"] = json!(self.database);
        }
        let mut builder = self.http.post(self.endpoint.join(route)?).json(&request);
        if let Some(token) = self.token().await? {
            builder = builder.bearer_auth(token);
        }
        let response = builder
            .send()
            .await
            .map_err(|error| anyhow::anyhow!("Kusto HTTP request failed: {}", redact_text(&error.to_string())))?;
        let status = response.status();
        let text = response.text().await.context("Kusto response body failed")?;
        let body: Value = serde_json::from_str(&text).unwrap_or_else(|_| json!({"error": text}));
        let public = sanitize(&body);
        append(
            &self.results.join("kusto-queries.jsonl"),
            &json!({
                "at_unix_ms": now_unix_millis(), "phase": phase,
                "management": management, "csl": redact_text(command),
                "http_status": status.as_u16(), "response": public
            }),
        )?;
        ensure!(status.is_success(), "Kusto HTTP {status}: {public}");
        decode_response(&body).map_err(|error| anyhow::anyhow!(redact_text(&format!("{error:#}"))))
    }

    async fn bootstrap(&self) -> Result<()> {
        if self.mode == Mode::Azure {
            return Ok(());
        }
        let rows = self.execute(".show databases", true, "bootstrap").await?;
        if rows
            .iter()
            .any(|row| row.get("DatabaseName").and_then(Value::as_str) == Some(DATABASE))
        {
            return Ok(());
        }
        let marker = self.results.join("kusto-database.json");
        let command = if marker.exists() {
            ".attach database loadtest from @\"/kustodata/dbs/loadtest/md\""
        } else {
            ".create database loadtest persist (@\"/kustodata/dbs/loadtest/md\", @\"/kustodata/dbs/loadtest/data\")"
        };
        self.execute(command, true, "bootstrap").await?;
        write_json(&marker, &json!({"database": DATABASE}))?;
        Ok(())
    }

    /// Defines (or redefines, picking up the current Delta schema) one accelerated external table.
    async fn expose(&self, name: &str, location: &str) -> Result<()> {
        ensure!(
            self.mode == Mode::Local,
            "Azure external tables are provisioned by infrastructure"
        );
        let table = identifier(name)?;
        self.execute(
            &format!(".create-or-alter external table {table} kind=delta (h@'{location}')"),
            true,
            "schema",
        )
        .await?;
        self.execute(
            &format!(
                ".alter external table {table} policy query_acceleration {}",
                literal(&local_policy().to_string())?
            ),
            true,
            "schema",
        )
        .await?;
        Ok(())
    }

    async fn schema(&self, name: &str) -> Result<Vec<(String, String)>> {
        self.execute(
            &format!(
                "{} | getschema | project ColumnName, ColumnType",
                fallback_source(name)?
            ),
            false,
            "schema",
        )
        .await?
        .into_iter()
        .map(|row| {
            Ok((
                row.get("ColumnName")
                    .and_then(Value::as_str)
                    .context("Kusto schema column name missing")?
                    .to_owned(),
                row.get("ColumnType")
                    .and_then(Value::as_str)
                    .context("Kusto schema column type missing")?
                    .to_owned(),
            ))
        })
        .collect()
    }

    async fn count(&self, source: &str, phase: &str) -> Result<u64> {
        self.execute(&format!("{source} | count"), false, phase)
            .await?
            .first()
            .and_then(|row| row.get("Count"))
            .and_then(number)
            .context("Kusto count missing")
    }

    async fn max_age(&self, name: &str) -> Result<(Value, String)> {
        let rows = self
            .execute(
                &format!(".show external table {} policy query_acceleration", identifier(name)?),
                true,
                "qap",
            )
            .await?;
        let policy = rows
            .first()
            .and_then(|row| row.get("Policy"))
            .map(json_value)
            .context("query acceleration policy missing")?;
        ensure!(
            policy["IsEnabled"] == true,
            "query acceleration is not enabled for {name}"
        );
        let max_age = policy["MaxAge"]
            .as_str()
            .context("query acceleration MaxAge missing")?
            .to_owned();
        timespan(&max_age)?;
        Ok((sanitize(&policy), max_age))
    }

    async fn acceleration(&self, name: &str, max_age: &str, commit_ms: Option<u64>) -> Result<Acceleration> {
        Ok(self
            .acceleration_row(name, max_age, commit_ms)
            .await?
            .unwrap_or_default())
    }

    async fn acceleration_row(
        &self,
        name: &str,
        max_age: &str,
        commit_ms: Option<u64>,
    ) -> Result<Option<Acceleration>> {
        let rows = self
            .execute(&acceleration_command(name, max_age, commit_ms)?, true, "qap")
            .await?;
        ensure!(rows.len() <= 1, "duplicate acceleration statistics for {name}");
        Ok(rows.first().map(Acceleration::from_row))
    }

    /// Best-effort sanitized `QueryAccelerationState`; never fails validation on its own.
    async fn details(&self, name: &str) -> Value {
        let Ok(table) = identifier(name) else {
            return json!({"error": "invalid external table name"});
        };
        match self
            .execute(&format!(".show external table {table} details"), true, "qap")
            .await
        {
            Ok(rows) => rows.first().map_or(Value::Null, |row| {
                json!({
                    "QueryAccelerationPolicy": row.get("QueryAccelerationPolicy").map(|value| sanitize(&json_value(value))),
                    "QueryAccelerationState": row.get("QueryAccelerationState").map(|value| sanitize(&json_value(value))),
                })
            }),
            Err(error) => json!({"error": short_error(&error)}),
        }
    }

    async fn capacity(&self) -> Value {
        // Local emulator capacity is functional evidence only, never Azure performance evidence.
        match self.execute(".show capacity", true, "qap").await {
            Ok(rows) => json!({"available": true, "rows": sanitize(&json!(rows))}),
            Err(error) => json!({"available": false, "error": short_error(&error)}),
        }
    }

    async fn multiset(&self, source: &str, schema: &Value, samples: &[(Value, u64)]) -> Result<Multiset> {
        let rows = self
            .execute(&multiset_query(source, schema, samples)?, false, "validation")
            .await?;
        let mut buckets = BTreeMap::new();
        for row in rows {
            let sample = row.get("Sample").and_then(number).context("multiset bucket missing")?;
            let count = row.get("Rows").and_then(number).context("multiset count missing")?;
            let hash = row
                .get("Hash")
                .and_then(Value::as_i64)
                .context("multiset hash missing")?;
            ensure!(
                buckets.insert(sample, (count, hash)).is_none(),
                "duplicate multiset bucket {sample}"
            );
        }
        Ok(Multiset { buckets })
    }
}

fn decode_response(body: &Value) -> Result<Vec<BTreeMap<String, Value>>> {
    if body.get("error").is_some() || body.get("Error").is_some() {
        bail!("Kusto command failed: {body}");
    }
    // v1 reports partial query failures inside an HTTP 200 body.
    ensure!(
        body.get("Exceptions")
            .is_none_or(|errors| errors.is_null() || errors.as_array().is_some_and(Vec::is_empty)),
        "Kusto partial query failure: {}",
        body["Exceptions"]
    );
    let tables = if let Some(frames) = body.as_array() {
        ensure!(
            frames.iter().any(|frame| frame["FrameType"] == "DataSetCompletion"),
            "Kusto query response has no completion frame"
        );
        for frame in frames {
            ensure!(
                frame["HasErrors"] != true
                    && frame
                        .get("OneApiErrors")
                        .is_none_or(|errors| errors.is_null() || errors.as_array().is_some_and(Vec::is_empty))
                    && frame["FrameType"] != "DataSetError"
                    && frame["FrameType"] != "TableFragment",
                "Kusto partial/progressive query failure: {frame}"
            );
        }
        frames
            .iter()
            .filter(|frame| frame["FrameType"] == "DataTable")
            .collect::<Vec<_>>()
    } else {
        body.get("Tables")
            .and_then(Value::as_array)
            .context("Kusto response has no result tables")?
            .iter()
            .collect()
    };
    let mut result = Vec::new();
    let mut found_primary = false;
    for table in tables {
        let columns = table["Columns"].as_array().context("Kusto result columns missing")?;
        let names = columns
            .iter()
            .map(|column| {
                column["ColumnName"]
                    .as_str()
                    .context("Kusto result column name missing")
            })
            .collect::<Result<Vec<_>>>()?;
        let rows = table["Rows"].as_array().context("Kusto result rows missing")?;
        let primary = table["TableKind"] == "PrimaryResult" || (table.get("TableKind").is_none() && !found_primary);
        for row in rows {
            let values = row.as_array().context("Kusto result row is not an array")?;
            ensure!(names.len() == values.len(), "Kusto result row width mismatch");
            if let Some(index) = names.iter().position(|name| *name == "Severity") {
                ensure!(
                    values[index].as_u64().is_some_and(|severity| severity > 2),
                    "Kusto in-band query failure: {row}"
                );
            }
            if primary {
                result.push(
                    names
                        .iter()
                        .zip(values)
                        .map(|(name, value)| ((*name).to_owned(), value.clone()))
                        .collect(),
                );
            }
        }
        found_primary |= primary;
    }
    ensure!(found_primary, "Kusto response has no primary result");
    Ok(result)
}

fn literal(value: &str) -> Result<String> {
    Ok(serde_json::to_string(value)?)
}

fn identifier(value: &str) -> Result<String> {
    ensure!(
        !value.is_empty()
            && value
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
            && !value.starts_with(|character: char| character.is_ascii_digit()),
        "unsupported Kusto identifier {value:?}"
    );
    Ok(format!("['{value}']"))
}

fn external_name(pool: &str, identity: &str) -> Result<String> {
    ensure!(matches!(pool, "direct" | "dataflow"), "unknown Kusto source pool");
    let name = identity
        .strip_prefix("telemetry.sqlserver.")
        .context("unexpected loadtest table namespace")?;
    ensure!(
        name.starts_with("sqlserver_") && !name.contains('.'),
        "unexpected loadtest table identity"
    );
    let name = format!("{pool}_{name}");
    identifier(&name)?;
    Ok(name)
}

/// Committed Delta table root inside the Kusto container (mounted read-only at `/delta/<pool>`).
fn table_location(pool: &str, identity: &str) -> Result<String> {
    external_name(pool, identity)?;
    Ok(format!("/delta/{pool}/{}", identity.replace('.', "/")))
}

fn columns(schema: &Value) -> Result<Vec<(String, String)>> {
    schema["fields"]
        .as_array()
        .context("Delta schema fields missing")?
        .iter()
        .map(|field| {
            let name = field["name"].as_str().context("Delta column name missing")?;
            identifier(name)?;
            let data_type = match field["type"].as_str().context("unsupported nested Delta field")? {
                "boolean" => "bool",
                "byte" | "short" | "integer" => "int",
                "long" => "long",
                "float" | "double" => "real",
                "string" => "string",
                "timestamp" => "datetime",
                other => bail!("unsupported Delta type {other} for {name}"),
            };
            Ok((name.to_owned(), data_type.to_owned()))
        })
        .collect()
}

fn append(file: &Path, value: &Value) -> Result<()> {
    let mut output = OpenOptions::new().create(true).append(true).open(file)?;
    serde_json::to_writer(&mut output, value)?;
    output.write_all(b"\n")?;
    Ok(())
}

fn write_json(file: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = file.with_extension("json.next");
    fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    fs::rename(temporary, file)?;
    Ok(())
}

/// `files_ingested` keeps its historical name for the runner; natively it counts committed active
/// files exposed through external tables (no rows are copied).
#[derive(Default, Deserialize, Serialize)]
struct ExposureStatus {
    state: String,
    run_id: String,
    #[serde(default)]
    backend: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    live_observed: bool,
    live_tables: BTreeSet<String>,
    files_ingested: usize,
    tables: BTreeMap<String, TableProgress>,
}

#[derive(Deserialize, Serialize)]
struct TableProgress {
    version: u64,
    active_files: usize,
    #[serde(default)]
    ingested_files: usize,
}

struct Exposure {
    client: KustoClient,
    status: ExposureStatus,
    schemas: BTreeMap<String, Value>,
}

impl Exposure {
    fn new(endpoint: &str, results: &Path, run_id: &str) -> Result<Self> {
        Ok(Self {
            client: KustoClient::new(endpoint, results)?,
            status: ExposureStatus {
                state: "healthy".to_owned(),
                run_id: run_id.to_owned(),
                backend: BACKEND.to_owned(),
                ..ExposureStatus::default()
            },
            schemas: BTreeMap::new(),
        })
    }

    fn persist(&mut self, error: Option<&anyhow::Error>) -> Result<()> {
        if let Some(error) = error {
            let message = short_error(error);
            if self.status.error.as_ref() != Some(&message) {
                eprintln!("Kusto DEGRADED (collector load continues): {message}");
            }
            self.status.state = "degraded".to_owned();
            self.status.error = Some(message);
        } else {
            self.status.state = "healthy".to_owned();
            self.status.error = None;
        }
        self.status.live_observed = self.status.live_tables.len() == 2 * TABLES_PER_POOL;
        self.status.files_ingested = self.status.tables.values().map(|table| table.active_files).sum();
        write_json(&self.client.results.join(STATUS), &self.status)
    }

    /// Exposes every committed table once; source removals and schema changes are explicit errors
    /// because the external table schema is fixed when it is defined.
    async fn sync(&mut self, pool: &str, snapshot: &SnapshotInventory, live: &AtomicBool) -> Result<()> {
        for name in self.schemas.keys().filter(|name| name.starts_with(&format!("{pool}_"))) {
            ensure!(
                snapshot
                    .tables
                    .keys()
                    .any(|table| external_name(pool, table).is_ok_and(|candidate| candidate == *name)),
                "Delta table disappeared for {name}"
            );
        }
        for (table, inventory) in &snapshot.tables {
            let name = external_name(pool, table)?;
            columns(&inventory.schema)?;
            if let Some(previous) = self.schemas.get(&name) {
                ensure!(
                    previous == &inventory.schema,
                    "Delta schema changed for {name}; the external table must be redefined"
                );
            } else {
                if self.client.mode == Mode::Local {
                    self.client.expose(&name, &table_location(pool, table)?).await?;
                }
                self.schemas.insert(name.clone(), inventory.schema.clone());
            }
            if live.load(Ordering::Acquire)
                && !self.status.live_tables.contains(&name)
                && !inventory.files.is_empty()
                && self.client.count(&external_source(&name)?, "live-observation").await? > 0
                && live.load(Ordering::Acquire)
            {
                self.status.live_tables.insert(name.clone());
            }
            self.status.tables.insert(
                name,
                TableProgress {
                    version: inventory.version,
                    active_files: inventory.files.len(),
                    ingested_files: inventory.files.len(),
                },
            );
        }
        Ok(())
    }

    /// Azure external tables are provisioned and rebound by infrastructure, possibly after load
    /// starts: absent or not-yet-readable tables are pending, never a sticky degradation.
    async fn observe_provisioned(&mut self, live: &AtomicBool) -> Result<()> {
        let rows = self
            .client
            .execute(".show external tables | project TableName", true, "schema")
            .await?;
        for name in rows
            .iter()
            .filter_map(|row| row.get("TableName").and_then(Value::as_str))
            .filter(|name| name.starts_with("direct_sqlserver_") || name.starts_with("dataflow_sqlserver_"))
        {
            if !live.load(Ordering::Acquire) || self.status.live_tables.contains(name) {
                continue;
            }
            match self.client.count(&external_source(name)?, "live-observation").await {
                Ok(rows) if rows > 0 && live.load(Ordering::Acquire) => {
                    self.status.live_tables.insert(name.to_owned());
                }
                Ok(_) => {}
                // Infrastructure may still be rebinding the table or the run root may not have its
                // first commit yet; anything else (auth, partial failure, malformed) is degraded.
                Err(error) if not_yet_readable(&error) => {}
                Err(error) => return Err(error.context(format!("live observation of {name}"))),
            }
        }
        Ok(())
    }

    async fn sync_roots(&mut self, direct: &Path, dataflow: &Path, live: &AtomicBool) -> Result<()> {
        if self.client.mode == Mode::Azure {
            return self.observe_provisioned(live).await;
        }
        self.client.bootstrap().await?;
        for (pool, root) in [("direct", direct), ("dataflow", dataflow)] {
            let snapshot = delta_inspect::inventory(root).await?;
            self.sync(pool, &snapshot, live).await?;
        }
        Ok(())
    }
}

pub(crate) struct LiveSync {
    stop: CancellationToken,
    generating: Arc<AtomicBool>,
    task: JoinHandle<Result<()>>,
}

impl LiveSync {
    pub(crate) fn start(arguments: &RunArgs, run_id: &str) -> Option<Self> {
        let endpoint = arguments.kusto_endpoint.clone()?;
        let arguments = arguments.clone();
        let run_id = run_id.to_owned();
        let stop = CancellationToken::new();
        let generating = Arc::new(AtomicBool::new(true));
        let stopped = stop.clone();
        let active = Arc::clone(&generating);
        let task = tokio::spawn(async move {
            let result = async {
                let mut exposure = Exposure::new(&endpoint, &arguments.results, &run_id)?;
                while !stopped.is_cancelled() {
                    let result = exposure
                        .sync_roots(&arguments.direct_root, &arguments.dataflow_root, &active)
                        .await;
                    exposure.persist(result.as_ref().err())?;
                    tokio::select! {
                        () = stopped.cancelled() => break,
                        () = tokio::time::sleep(POLL_INTERVAL) => {}
                    }
                }
                let final_sync = tokio::time::timeout(arguments.drain_timeout.get(), async {
                    settle_dataflow(
                        &arguments.dataflow_admin_endpoints,
                        arguments.drain_timeout.get(),
                        arguments.quiver,
                    )
                    .await?;
                    exposure
                        .sync_roots(&arguments.direct_root, &arguments.dataflow_root, &active)
                        .await
                })
                .await
                .context("Kusto final catch-up timed out")?;
                exposure.persist(final_sync.as_ref().err())?;
                final_sync
            }
            .await;
            if let Err(error) = &result {
                degraded(&arguments.results, &run_id, error)?;
            }
            result
        });
        Some(Self { stop, generating, task })
    }

    pub(crate) fn finish_generation(&self) {
        self.generating.store(false, Ordering::Release);
    }

    pub(crate) async fn stop(self) -> Result<()> {
        self.finish_generation();
        self.stop.cancel();
        self.task.await.context("Kusto synchronization task failed")?
    }
}

/// One table's native `query_acceleration statistics` row; missing measurements stay `null`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
struct Acceleration {
    hot_size: Option<u64>,
    #[serde(rename = "LastUpdatedDateTime")]
    last_updated: Option<String>,
    #[serde(rename = "AccelerationPendingDataFilesCount")]
    pending_files: Option<u64>,
    #[serde(rename = "AccelerationPendingDataFilesSize")]
    pending_bytes: Option<u64>,
    #[serde(rename = "AccelerationCompletePercentage")]
    complete_percent: Option<f64>,
    /// Last refresh is newer than `now() - MaxAge`, so queries may use the cache.
    eligible: bool,
    /// Last refresh is not older than the captured Delta commit (or no reference exists).
    fresh: bool,
}

/// v1 management responses encode `bool` columns as 0/1 while v2 queries use JSON booleans.
fn flag(value: &Value) -> bool {
    value.as_bool().unwrap_or_else(|| value.as_i64() == Some(1))
}

impl Acceleration {
    fn from_row(row: &BTreeMap<String, Value>) -> Self {
        Self {
            hot_size: row.get("HotSize").and_then(number),
            last_updated: row
                .get("LastUpdatedDateTime")
                .and_then(Value::as_str)
                .map(str::to_owned),
            pending_files: row.get("PendingFiles").and_then(number),
            pending_bytes: row.get("PendingBytes").and_then(number),
            complete_percent: row.get("Complete").and_then(real),
            eligible: row.get("Eligible").is_some_and(flag),
            fresh: row.get("Fresh").is_some_and(flag),
        }
    }

    /// Cache readiness referenced to the tested source generation, not to observation time: an
    /// idle, already-current cache may legitimately keep a refresh time from before capture.
    fn ready(&self) -> bool {
        self.complete_percent.is_some_and(|percent| percent >= 100.0)
            && self.pending_files == Some(0)
            && self.eligible
            && self.fresh
    }
}

/// Health and source generation reported in `QueryAccelerationState`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
struct CacheState {
    table_version: Option<u64>,
    is_healthy: Option<bool>,
    row_count: Option<u64>,
    accelerated_row_count: Option<u64>,
    not_healthy_reason: Option<String>,
}

impl CacheState {
    fn from_details(details: &Value) -> Self {
        let state = &details["QueryAccelerationState"];
        Self {
            table_version: state.get("TableVersion").and_then(Value::as_u64),
            is_healthy: state.get("IsHealthy").and_then(Value::as_bool),
            row_count: state.get("RowCount").and_then(Value::as_u64),
            accelerated_row_count: state.get("AcceleratedRowCount").and_then(Value::as_u64),
            not_healthy_reason: state
                .get("NotHealthyReason")
                .and_then(Value::as_str)
                .filter(|reason| !reason.is_empty())
                .map(str::to_owned),
        }
    }

    /// The cache has accelerated exactly the captured Delta version and all of its rows.
    fn current(&self, version: u64, rows: u64) -> bool {
        self.table_version == Some(version) && self.is_healthy == Some(true) && self.accelerated_row_count == Some(rows)
    }
}

/// Row-count and order-independent hash per oracle sample bucket (bucket 0 = unmatched rows).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Multiset {
    buckets: BTreeMap<u64, (u64, i64)>,
}

impl Multiset {
    fn rows(&self) -> u64 {
        self.buckets.values().map(|(count, _)| count).sum()
    }

    fn unmatched(&self) -> u64 {
        self.buckets.get(&0).map_or(0, |(count, _)| *count)
    }

    fn hash(&self) -> i64 {
        self.buckets
            .values()
            .fold(0_i64, |total, (_, hash)| total.wrapping_add(*hash))
    }

    /// Compares bucket counts against the independent Delta oracle.
    fn check(&self, expected_rows: u64, samples: &[(Value, u64)]) -> Result<()> {
        ensure!(
            self.rows() == expected_rows,
            "Kusto returned {} rows, Delta snapshot has {expected_rows}",
            self.rows()
        );
        ensure!(
            self.unmatched() == 0,
            "{} Kusto rows match no Delta row",
            self.unmatched()
        );
        for (index, (_, expected)) in samples.iter().enumerate() {
            let actual = self.buckets.get(&(index as u64 + 1)).map_or(0, |(count, _)| *count);
            ensure!(
                actual == *expected,
                "Delta row {index} appears {actual} times in Kusto, expected {expected}"
            );
        }
        Ok(())
    }

    fn evidence(&self) -> Value {
        json!({"rows": self.rows(), "unmatched": self.unmatched(), "multiset_hash": self.hash()})
    }
}

fn local_policy() -> Value {
    json!({"IsEnabled": true, "Hot": LOCAL_HOT_WINDOW, "MaxAge": LOCAL_MAX_AGE})
}

fn acceleration_command(name: &str, max_age: &str, commit_ms: Option<u64>) -> Result<String> {
    identifier(name)?;
    timespan(max_age)?;
    let fresh = commit_ms.map_or_else(
        || "true".to_owned(),
        |millis| format!("LastUpdatedDateTime >= unixtime_milliseconds_todatetime({millis})"),
    );
    Ok(format!(
        ".show external tables operations query_acceleration statistics \
         | where ExternalTableName == {} \
         | extend Eligible = LastUpdatedDateTime > now() - totimespan({}), Fresh = {fresh} \
         | project HotSize = tolong(HotSize), LastUpdatedDateTime, \
           PendingFiles = tolong(AccelerationPendingDataFilesCount), \
           PendingBytes = tolong(column_ifexists('AccelerationPendingDataFilesSize', long(null))), \
           Complete = todouble(AccelerationCompletePercentage), Eligible, Fresh",
        literal(name)?,
        literal(max_age)?
    ))
}

fn timespan(value: &str) -> Result<Duration> {
    let (days, clock) = match value.split_once('.') {
        Some((days, clock)) if clock.contains(':') => (days.parse::<u64>()?, clock),
        _ => (0, value),
    };
    let parts = clock.split(':').collect::<Vec<_>>();
    ensure!(parts.len() == 3, "invalid Kusto timespan {value:?}");
    let seconds = parts[2].split('.').next().unwrap_or_default();
    let total =
        days * 86_400 + parts[0].parse::<u64>()? * 3_600 + parts[1].parse::<u64>()? * 60 + seconds.parse::<u64>()?;
    Ok(Duration::from_secs(total))
}

fn external_source(name: &str) -> Result<String> {
    identifier(name)?;
    Ok(format!("external_table({})", literal(name)?))
}

fn fallback_source(name: &str) -> Result<String> {
    identifier(name)?;
    Ok(format!("external_table({}, timespan(1s))", literal(name)?))
}

fn multiset_query(source: &str, schema: &Value, samples: &[(Value, u64)]) -> Result<String> {
    ensure!(!samples.is_empty(), "Delta oracle produced no rows");
    let cases = samples
        .iter()
        .enumerate()
        .map(|(index, (row, _))| {
            Ok(format!(
                "coalesce(({}), false), {}",
                payload_predicate(schema, row)?,
                index + 1
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    Ok(format!(
        "{source} | extend Sample = tolong(case({cases}, 0)) \
         | summarize Rows = count(), Hash = sum(hash(tostring(pack_all())) % 1000000007) by Sample"
    ))
}

fn number(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| {
            value
                .as_f64()
                .filter(|value| value.fract() == 0.0 && *value >= 0.0)
                .map(|value| value as u64)
        })
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn real(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn json_value(value: &Value) -> Value {
    value
        .as_str()
        .and_then(|text| serde_json::from_str(text).ok())
        .unwrap_or_else(|| value.clone())
}

/// Drops locations, connection strings, and credentials from QAP observations.
fn sanitize(value: &Value) -> Value {
    const SENSITIVE: [&str; 11] = [
        "path",
        "uri",
        "url",
        "connection",
        "location",
        "secret",
        "token",
        "signature",
        "password",
        "cachedtable",
        "catalogtable",
    ];
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .filter(|(key, _)| {
                    let key = key.to_ascii_lowercase();
                    !SENSITIVE.iter().any(|word| key.contains(word))
                })
                .map(|(key, value)| (key.clone(), sanitize(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(sanitize).collect()),
        Value::String(text) => {
            let parsed = json_value(value);
            if parsed.is_object() || parsed.is_array() {
                sanitize(&parsed)
            } else {
                Value::String(redact_text(text))
            }
        }
        other => other.clone(),
    }
}

fn short_error(error: &anyhow::Error) -> String {
    redact_text(&format!("{error:#}")).chars().take(512).collect()
}

/// Replaces location-, identity-, and credential-shaped tokens (URIs, absolute paths, Azure
/// resource IDs, GUIDs such as subscription or managed identity IDs, UPNs, SAS signatures, and
/// JWTs) while keeping the surrounding diagnostic text.
fn redact_text(text: &str) -> String {
    fn sensitive(token: &str) -> bool {
        let lower = token.to_ascii_lowercase();
        lower.contains("://")
            || token.starts_with('/')
            || token.starts_with('\\')
            || lower.contains("sig=")
            || lower.contains("/subscriptions/")
            || (lower.starts_with("eyj") && token.len() >= 32)
            || (token.contains('@') && token.contains('.'))
            || token.as_bytes().windows(36).any(|window| {
                window.iter().enumerate().all(|(index, byte)| {
                    if matches!(index, 8 | 13 | 18 | 23) {
                        *byte == b'-'
                    } else {
                        byte.is_ascii_hexdigit()
                    }
                })
            })
    }
    let mut output = String::with_capacity(text.len());
    let mut token = String::new();
    let flush = |token: &mut String, output: &mut String| {
        output.push_str(if sensitive(token) { "[redacted]" } else { token });
        token.clear();
    };
    for character in text.chars() {
        if character.is_whitespace() || "\"'`()[]{},;<>".contains(character) {
            flush(&mut token, &mut output);
            output.push(character);
        } else {
            token.push(character);
        }
    }
    flush(&mut token, &mut output);
    output
}

fn cache_timeout() -> Result<Duration> {
    std::env::var("LOADTEST_KUSTO_CACHE_TIMEOUT").map_or(Ok(DEFAULT_CACHE_TIMEOUT), |value| {
        value
            .parse::<crate::cli::DurationArg>()
            .map(crate::cli::DurationArg::get)
            .map_err(|_| anyhow::anyhow!("invalid LOADTEST_KUSTO_CACHE_TIMEOUT"))
    })
}

/// Commit time of the captured Delta version: the generation the cache must have refreshed past.
fn commit_millis(root: &Path, identity: &str, version: u64) -> Option<u64> {
    // Cloud commits are bounded by the cache's exact TableVersion instead.
    cloud_root(root).is_none().then_some(())?;
    let file = root
        .join(identity.replace('.', "/"))
        .join("_delta_log")
        .join(format!("{version:020}.json"));
    let text = fs::read_to_string(&file).ok()?;
    text.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|action| action["commitInfo"]["timestamp"].as_u64())
        .or_else(|| {
            let modified = fs::metadata(&file).ok()?.modified().ok()?;
            u64::try_from(modified.duration_since(UNIX_EPOCH).ok()?.as_millis()).ok()
        })
}

pub(crate) async fn settle_dataflow(endpoints: &[String], timeout: Duration, quiver: bool) -> Result<()> {
    ensure!(!endpoints.is_empty(), "Dataflow publication endpoints are missing");
    let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
    let deadline = Instant::now() + timeout;
    let mut replay = ReplayQuiescence::default();
    loop {
        let mut settled = true;
        let mut replay_counters = Vec::new();
        for endpoint in endpoints {
            let response = client
                .get(format!("{}/metrics", endpoint.trim_end_matches('/')))
                .send()
                .await?;
            let text = response.error_for_status()?.text().await?;
            settled &= publication_idle(&text)?;
            if quiver {
                settled &= metric_value(&text, "arrowcol_dataflow_durable_backlog_batches")? == 0;
                for name in [
                    "arrowcol_dataflow_replayed_batches_total",
                    "arrowcol_dataflow_replay_deferred_total",
                ] {
                    replay_counters.push(metric_value(&text, name)?);
                }
            }
        }
        if quiver {
            settled = replay.observe(settled, replay_counters, Instant::now());
        }
        if settled {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "Dataflow publication did not settle before Kusto validation"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn publication_idle(metrics: &str) -> Result<bool> {
    let mut idle = true;
    for name in [
        "arrowcol_flight_active_streams",
        "arrowcol_dfe_buffered_batches",
        "arrowcol_delta_pending_commits",
    ] {
        idle &= metric_value(metrics, name)? == 0;
    }
    Ok(idle)
}

fn metric_value(metrics: &str, name: &str) -> Result<u64> {
    Ok(metrics
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .with_context(|| format!("missing publication metric {name}"))?
        .parse::<u64>()?)
}

/// How long Quiver replay counters must stay unchanged before durable Dataflow output is frozen.
const REPLAY_QUIET_PERIOD: Duration = Duration::from_secs(5);

/// Batches replayed from a predecessor's Quiver WAL bypass the in-memory publication gauges, so a
/// durable run additionally waits until replay activity stops before snapshots are captured.
#[derive(Default)]
struct ReplayQuiescence {
    last: Option<Vec<u64>>,
    quiet_since: Option<Instant>,
}

impl ReplayQuiescence {
    fn observe(&mut self, idle: bool, counters: Vec<u64>, now: Instant) -> bool {
        let unchanged = self.last.as_ref() == Some(&counters);
        self.last = Some(counters);
        if !idle || !unchanged {
            self.quiet_since = None;
            return false;
        }
        now.duration_since(*self.quiet_since.get_or_insert(now)) >= REPLAY_QUIET_PERIOD
    }
}

/// Kusto's answer for an external table whose definition or Delta log does not exist yet.
fn not_yet_readable(error: &anyhow::Error) -> bool {
    // Only a listed table whose Delta log is not committed yet is pending. Kusto reports that as a
    // storage blob-not-found partial failure; generic bad requests (syntax, schema, credentials)
    // and every other failure must surface as degraded.
    let message = format!("{error:#}");
    message.contains("E_XSTORE_BLOB_NOT_FOUND") && message.contains("Delta table does not exist")
}

/// Azure oracle roots are credential-free `abfss://` URLs read by the independent cloud reader.
fn cloud_root(root: &Path) -> Option<&str> {
    root.to_str().filter(|root| root.starts_with("abfss://"))
}

async fn frozen(root: &Path, snapshot: &SnapshotSummary) -> Result<bool> {
    let current = if let Some(cloud) = cloud_root(root) {
        let identities: Vec<String> = snapshot.tables.keys().cloned().collect();
        captured_inventory(&delta_inspect::summarize_cloud(cloud, &identities).await?)
    } else {
        delta_inspect::inventory(root).await?
    };
    Ok(current == captured_inventory(snapshot))
}

async fn corpus(root: &Path, table: &str, summary: &TableSummary) -> Result<Vec<(Value, u64)>> {
    if let Some(cloud) = cloud_root(root) {
        delta_inspect::cloud_sample_rows(cloud, table, summary).await
    } else {
        delta_inspect::sample_rows(root, table, summary)
    }
}

fn captured_inventory(snapshot: &SnapshotSummary) -> SnapshotInventory {
    SnapshotInventory {
        tables: snapshot
            .tables
            .iter()
            .map(|(name, table)| {
                (
                    name.clone(),
                    TableInventory {
                        schema: table.schema.clone(),
                        version: table.version,
                        files: table.files.iter().map(|file| file.path.clone()).collect(),
                    },
                )
            })
            .collect(),
    }
}

fn payload_predicate(schema: &Value, sample: &Value) -> Result<String> {
    columns(schema)?
        .iter()
        .map(|(name, kind)| {
            let column = identifier(name)?;
            let value = sample.get(name).with_context(|| format!("reference value missing for {name}"))?;
            if value.is_null() {
                return Ok(if kind == "string" { format!("isempty({column})") } else { format!("isnull({column})") });
            }
            let expected = match kind.as_str() {
                "string" => literal(value.as_str().context("reference string type mismatch")?)?,
                "datetime" => format!("todatetime({})", literal(value.as_str().context("reference timestamp type mismatch")?)?),
                "long" | "int" => {
                    let number = if let Some(text) = value.as_str() { text.parse::<i64>()? } else { value.as_i64().context("reference integer type mismatch")? };
                    format!("{kind}({number})")
                }
                "real" => {
                    let value = value.as_f64().context("reference real type mismatch")?;
                    ensure!(value.is_finite(), "non-finite reference real value");
                    return Ok(format!("isnotnull({column}) and abs({column} - real({value})) <= max_of(1e-9, abs(real({value})) * 1e-12)"));
                }
                "bool" => value.as_bool().context("reference boolean type mismatch")?.to_string(),
                _ => bail!("unsupported Kusto reference type"),
            };
            Ok(format!("{column} == {expected}"))
        })
        .collect::<Result<Vec<_>>>()
        .map(|values| values.join(" and "))
}

/// Per-table oracle and evidence prepared before the bounded cache wait.
struct TableCheck {
    name: String,
    table: String,
    pool: &'static str,
    summary: TableSummary,
    samples: Vec<(Value, u64)>,
    commit_ms: Option<u64>,
    max_age: String,
    policy: Value,
    fallback: Multiset,
}

pub(crate) async fn validate(
    endpoint: &str,
    results: &Path,
    direct_root: &Path,
    dataflow_root: &Path,
    direct: &SnapshotSummary,
    dataflow: &SnapshotSummary,
) -> Result<Value> {
    let mut status: ExposureStatus = serde_json::from_slice(&fs::read(results.join(STATUS))?)?;
    let metadata: Value = serde_json::from_slice(&fs::read(results.join("run-metadata.json"))?)?;
    ensure!(
        metadata["run_id"].as_str() == Some(&status.run_id),
        "Kusto evidence belongs to another run"
    );
    let client = KustoClient::new(endpoint, results)?;
    client.bootstrap().await?;
    let mut checks = Vec::new();
    for (pool, root, snapshot) in [("direct", direct_root, direct), ("dataflow", dataflow_root, dataflow)] {
        ensure!(
            snapshot.tables.len() == TABLES_PER_POOL,
            "{pool} Kusto validation requires all {TABLES_PER_POOL} tables"
        );
        ensure!(
            frozen(root, snapshot).await?,
            "{pool} changed after the validation snapshot was captured"
        );
        for (table, summary) in &snapshot.tables {
            let name = external_name(pool, table)?;
            ensure!(summary.rows > 0, "expected a nonempty Delta snapshot for {name}");
            ensure!(
                client.schema(&name).await? == columns(&summary.schema)?,
                "Kusto external table schema differs from Delta for {name}"
            );
            let samples = corpus(root, table, summary).await?;
            let fallback = client
                .multiset(&fallback_source(&name)?, &summary.schema, &samples)
                .await?;
            fallback
                .check(summary.rows, &samples)
                .with_context(|| format!("non-accelerated fallback differs from Delta for {name}"))?;
            let (policy, max_age) = client.max_age(&name).await?;
            checks.push(TableCheck {
                name,
                table: table.clone(),
                pool,
                summary: summary.clone(),
                samples,
                commit_ms: commit_millis(root, table, summary.version),
                max_age,
                policy,
                fallback,
            });
        }
    }
    let cached = converge(&client, &checks, cache_timeout()?).await?;
    let mut evidence = BTreeMap::new();
    for check in &checks {
        let statistics = client
            .acceleration(&check.name, &check.max_age, check.commit_ms)
            .await?;
        evidence.insert(
            identifier(&check.name)?,
            json!({
                "pool": check.pool, "table": check.table,
                "delta_version": check.summary.version, "active_files": check.summary.active_files,
                "partition_columns": check.summary.partition_columns,
                "commit_unix_ms": check.commit_ms,
                "expected_rows": check.summary.rows, "distinct_rows": check.samples.len(),
                "actual": {"Rows": check.fallback.rows(), "Invalid": check.fallback.unmatched()},
                "fallback": {"query": fallback_source(&check.name)?, "result": check.fallback.evidence()},
                "cached": cached[&check.name],
                "qap": {
                    "policy": check.policy,
                    "statistics": statistics,
                    "details": client.details(&check.name).await,
                },
            }),
        );
    }
    ensure!(
        status.live_observed && status.live_tables.len() == 2 * TABLES_PER_POOL,
        "all {} Kusto tables were not observed while generation was active",
        2 * TABLES_PER_POOL
    );
    status.state = "healthy".to_owned();
    status.backend = BACKEND.to_owned();
    status.error = None;
    write_json(&results.join(STATUS), &status)?;
    Ok(json!({
        "passed": true, "run_id": status.run_id, "backend": BACKEND, "mode": client.mode,
        "live_observed": true, "capacity": client.capacity().await, "tables": evidence
    }))
}

const OBSERVATIONS_PER_TABLE: usize = 32;

/// Waits, under one fixed deadline covering every request, sleep and token refresh, until each
/// table's accelerated cache reports the captured version and returns the oracle multiset. A
/// success observed at or after the deadline is not accepted.
async fn converge(client: &KustoClient, checks: &[TableCheck], timeout: Duration) -> Result<BTreeMap<String, Value>> {
    let started = Instant::now();
    let deadline = started + timeout;
    let elapsed = || u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let mut cached: BTreeMap<String, Value> = BTreeMap::new();
    let mut attempts: BTreeMap<String, u64> = BTreeMap::new();
    let mut observations: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut last_failure: BTreeMap<String, String> = BTreeMap::new();
    let observe = |observations: &mut BTreeMap<String, Vec<Value>>, name: &str, observation: Value| {
        let history = observations.entry(name.to_owned()).or_default();
        if history.len() == OBSERVATIONS_PER_TABLE {
            history.remove(0);
        }
        history.push(observation);
    };
    let polling = async {
        loop {
            for check in checks {
                if cached.contains_key(&check.name) {
                    continue;
                }
                *attempts.entry(check.name.clone()).or_default() += 1;
                let before = client
                    .acceleration(&check.name, &check.max_age, check.commit_ms)
                    .await?;
                let state = CacheState::from_details(&client.details(&check.name).await);
                if !before.ready() || !state.current(check.summary.version, check.summary.rows) {
                    let reason = format!("cache not ready: {before:?} {state:?}");
                    observe(
                        &mut observations,
                        &check.name,
                        json!({
                            "elapsed_ms": elapsed(), "outcome": "not-ready", "statistics": before, "state": state
                        }),
                    );
                    last_failure.insert(check.name.clone(), reason);
                    continue;
                }
                let result = client
                    .multiset(&external_source(&check.name)?, &check.summary.schema, &check.samples)
                    .await?;
                let after = client
                    .acceleration(&check.name, &check.max_age, check.commit_ms)
                    .await?;
                // A stale-but-ready-looking cache is retried until it converges or the deadline elapses.
                let verdict = result.check(check.summary.rows, &check.samples).and_then(|()| {
                    ensure!(result == check.fallback, "cached multiset differs from fallback");
                    ensure!(after.ready(), "cache left ready state during the query: {after:?}");
                    ensure!(
                        Instant::now() < deadline,
                        "cache verified only after the fixed deadline"
                    );
                    Ok(())
                });
                let outcome = match &verdict {
                    Ok(()) => "converged".to_owned(),
                    Err(error) => short_error(error),
                };
                observe(
                    &mut observations,
                    &check.name,
                    json!({
                        "elapsed_ms": elapsed(), "outcome": outcome, "result": result.evidence(),
                        "statistics": before, "state": state
                    }),
                );
                match verdict {
                    Ok(()) => {
                        cached.insert(
                            check.name.clone(),
                            json!({
                                "query": external_source(&check.name)?,
                                "result": result.evidence(),
                                "attempts": attempts[&check.name],
                                "converged_after_ms": elapsed(),
                                "deadline_ms": u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
                                "before": before, "after": after, "state": state,
                            }),
                        );
                    }
                    Err(error) => {
                        last_failure.insert(check.name.clone(), short_error(&error));
                    }
                }
            }
            if cached.len() == checks.len() || Instant::now() >= deadline {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep_until(tokio::time::Instant::from_std(
                (Instant::now() + CACHE_POLL_INTERVAL).min(deadline),
            ))
            .await;
        }
    };
    if let Ok(outcome) = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), polling).await {
        outcome?;
    }
    ensure!(
        cached.len() == checks.len(),
        "accelerated cache did not converge to the captured Delta snapshot within {timeout:?}: {:?}",
        checks
            .iter()
            .filter(|check| !cached.contains_key(&check.name))
            .map(|check| (
                check.name.as_str(),
                last_failure
                    .get(&check.name)
                    .map_or("no observation before the deadline", String::as_str)
            ))
            .collect::<BTreeMap<_, _>>()
    );
    for (name, evidence) in &mut cached {
        evidence["observations"] = json!(observations.remove(name).unwrap_or_default());
    }
    Ok(cached)
}

/// Read-only, phase-aligned QAP observation for live telemetry while load is running. It never
/// creates or alters tables, policies, or databases and makes no correctness claim: `Fresh` is
/// `null` because no captured source generation is referenced. Missing measurements stay `null`;
/// absent or not-yet-provisioned tables are reported per table as unavailable.
///
/// # Errors
///
/// Returns an error when the endpoint or table names are invalid, the deadline elapses, or
/// authentication or the capacity/table listing requests fail. Per-table query failures are
/// reported inside the returned JSON instead of failing the whole observation.
#[allow(dead_code, reason = "exported for the runner's live LGTM observation CLI")]
pub async fn observe_qap(
    endpoint: &str,
    results: &Path,
    database: Option<&str>,
    tables: &[String],
    deadline: Duration,
) -> Result<Value> {
    let client = match database {
        Some(database) => KustoClient::with_mode(endpoint, results, Mode::from_env()?, database)?,
        None => KustoClient::new(endpoint, results)?,
    };
    tokio::time::timeout(deadline, async {
        let mut observed = BTreeMap::new();
        for name in tables {
            observed.insert(name.clone(), observe_table(&client, name).await);
        }
        json!({
            "observed_unix_ms": now_unix_millis(), "backend": BACKEND, "mode": client.mode,
            "capacity": client.capacity().await, "tables": observed
        })
    })
    .await
    .map_err(|_| anyhow::anyhow!("QAP observation exceeded its {deadline:?} deadline"))
}

async fn observe_table(client: &KustoClient, name: &str) -> Value {
    let observed = async {
        let (policy, max_age) = client.max_age(name).await?;
        let statistics = match client.acceleration_row(name, &max_age, None).await? {
            Some(row) => {
                let mut statistics = serde_json::to_value(row)?;
                statistics["Fresh"] = Value::Null;
                statistics
            }
            None => Value::Null,
        };
        let details = client.details(name).await;
        let state = CacheState::from_details(&details);
        Ok::<_, anyhow::Error>(json!({
            "available": true, "policy": policy, "statistics": statistics, "state": state, "details": details
        }))
    }
    .await;
    observed.unwrap_or_else(|error| json!({"available": false, "error": short_error(&error)}))
}

pub(crate) fn validation_failure(results: &Path, error: &anyhow::Error) -> Result<Value> {
    let report = json!({"passed":false, "backend": BACKEND, "error": short_error(error)});
    write_json(&results.join("kusto-validation.json"), &report)?;
    Ok(report)
}

pub(crate) fn degraded(results: &Path, run_id: &str, error: &anyhow::Error) -> Result<()> {
    let path = results.join(STATUS);
    let mut status: ExposureStatus = if path.exists() {
        serde_json::from_slice(&fs::read(&path)?)?
    } else {
        ExposureStatus {
            run_id: run_id.to_owned(),
            backend: BACKEND.to_owned(),
            ..ExposureStatus::default()
        }
    };
    status.state = "degraded".to_owned();
    status.error = Some(short_error(error));
    write_json(&path, &status)
}

pub(crate) fn write_validation(results: &Path, report: &Value) -> Result<()> {
    write_json(&results.join("kusto-validation.json"), report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Read,
        net::{TcpListener, TcpStream},
        sync::atomic::{AtomicU64, AtomicUsize},
        thread,
    };

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct Directory(PathBuf);

    impl Directory {
        fn new() -> Result<Self> {
            let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let root = PathBuf::from(format!("target/loadgen-kusto-tests/{}-{sequence}", std::process::id()));
            fs::create_dir_all(&root)?;
            Ok(Self(root))
        }
    }

    impl Drop for Directory {
        fn drop(&mut self) {
            if self.0.exists()
                && let Err(error) = fs::remove_dir_all(&self.0)
            {
                panic!("remove test directory: {error}");
            }
        }
    }

    fn response(names: &[&str], rows: Vec<Value>) -> Value {
        json!({"Tables":[{
            "Columns":names.iter().map(|name|json!({"ColumnName":name})).collect::<Vec<_>>(),
            "Rows":rows
        }]})
    }

    struct Server {
        endpoint: String,
        stop: Arc<AtomicBool>,
        task: Option<thread::JoinHandle<Result<()>>>,
    }

    impl Server {
        fn start(mut handler: impl FnMut(Value) -> Result<Option<Value>> + Send + 'static) -> Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let endpoint = format!("http://{}", listener.local_addr()?);
            let stop = Arc::new(AtomicBool::new(false));
            let stopped = Arc::clone(&stop);
            let task = thread::spawn(move || -> Result<()> {
                while !stopped.load(Ordering::Acquire) {
                    let (mut stream, _) = listener.accept()?;
                    if stopped.load(Ordering::Acquire) {
                        break;
                    }
                    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
                    let mut header = Vec::new();
                    while !header.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        stream.read_exact(&mut byte)?;
                        header.push(byte[0]);
                    }
                    let header = String::from_utf8(header)?;
                    let length = header
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(str::to_owned)
                        })
                        .context("content length missing")?
                        .trim()
                        .parse::<usize>()?;
                    let mut bytes = vec![0; length];
                    stream.read_exact(&mut bytes)?;
                    if let Some(body) = handler(serde_json::from_slice(&bytes)?)? {
                        let body = body.to_string();
                        write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )?;
                    }
                }
                Ok(())
            });
            Ok(Self {
                endpoint,
                stop,
                task: Some(task),
            })
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _connection = TcpStream::connect(self.endpoint.trim_start_matches("http://"));
            if let Some(task) = self.task.take() {
                match task.join() {
                    Ok(Ok(())) => {}
                    error => panic!("test server failed: {error:?}"),
                }
            }
        }
    }

    #[test]
    fn response_parser_rejects_http_success_shaped_query_errors() -> Result<()> {
        assert!(decode_response(&json!({"error":{"code":"BadRequest"}})).is_err());
        assert!(decode_response(&json!({})).is_err());
        assert!(
            decode_response(&json!([
                {"FrameType":"DataTable","TableKind":"PrimaryResult","Columns":[{"ColumnName":"Rows"}],"Rows":[[7]]},
                {"FrameType":"DataSetCompletion","HasErrors":true,"OneApiErrors":[{"code":"PartialQueryFailure"}]}
            ]))
            .is_err()
        );
        assert!(
            decode_response(&json!([
                {"FrameType":"DataTable","TableKind":"PrimaryResult","Columns":[{"ColumnName":"Rows"}],"Rows":[[7]]},
                {"FrameType":"DataSetCompletion","HasErrors":false,"OneApiErrors":[{"code":"LimitsExceeded"}]}
            ]))
            .is_err()
        );
        let mut partial = response(&["Rows"], vec![json!([7])]);
        partial["Exceptions"] = json!(["Query execution has exceeded the allowed limits"]);
        assert!(
            decode_response(&partial).is_err(),
            "v1 HTTP 200 exceptions must fail closed"
        );
        assert!(decode_response(&response(&["Severity"], vec![json!([2])])).is_err());
        assert!(decode_response(&response(&["Rows"], vec![json!([1, 2])])).is_err());
        let parsed = decode_response(&json!([
            {"FrameType":"DataTable","TableKind":"PrimaryResult","Columns":[{"ColumnName":"Rows"}],"Rows":[[9007199254740993_u64]]},
            {"FrameType":"DataSetCompletion","HasErrors":false}
        ]))?;
        assert_eq!(parsed[0]["Rows"].as_u64(), Some(9_007_199_254_740_993));
        Ok(())
    }

    #[test]
    fn schemas_and_predicates_preserve_typed_and_nanosecond_values() -> Result<()> {
        let schema = json!({"fields":[
            {"name":"flag","type":"boolean"},{"name":"small","type":"short"},
            {"name":"count","type":"long"},{"name":"value","type":"double"},
            {"name":"time","type":"timestamp"},{"name":"time__unix_nanos","type":"long"},
            {"name":"text","type":"string"}
        ]});
        assert_eq!(
            columns(&schema)?
                .iter()
                .map(|(_, kind)| kind.as_str())
                .collect::<Vec<_>>(),
            ["bool", "int", "long", "real", "datetime", "long", "string"]
        );
        let predicate = payload_predicate(
            &schema,
            &json!({
                "flag":true,"small":7,"count":"9007199254740993","value":0.5,
                "time":"1970-01-01 00:00:00 +00:00","time__unix_nanos":-1,"text":"a\"b\\c"
            }),
        )?;
        assert!(predicate.contains("long(9007199254740993)"));
        assert!(predicate.contains("long(-1)"));
        assert!(predicate.contains("todatetime("));
        assert!(predicate.contains("a\\\"b\\\\c"));
        assert!(columns(&json!({"fields":[{"name":"bad","type":"binary"}]})).is_err());
        assert!(identifier("x'); .drop database loadtest").is_err());
        assert!(external_name("direct", "unexpected.sqlserver.sqlserver_wait_stats").is_err());
        assert!(table_location("direct", "telemetry.sqlserver.sqlserver_wait_stats/../x").is_err());
        assert_eq!(
            table_location("dataflow", "telemetry.sqlserver.sqlserver_wait_stats")?,
            "/delta/dataflow/telemetry/sqlserver/sqlserver_wait_stats"
        );
        assert_eq!(
            fallback_source("direct_sqlserver_wait_stats")?,
            "external_table(\"direct_sqlserver_wait_stats\", timespan(1s))"
        );
        assert!(external_source("x\") | take 1; //").is_err());
        Ok(())
    }

    #[test]
    fn multiset_requires_exact_oracle_counts_and_no_unmatched_rows() -> Result<()> {
        let samples = vec![(json!({"id":1}), 2), (json!({"id":2}), 1)];
        let exact = Multiset {
            buckets: BTreeMap::from([(1, (2, 10)), (2, (1, -3))]),
        };
        exact.check(3, &samples)?;
        assert_eq!(exact.hash(), 7);
        let stale = Multiset {
            buckets: BTreeMap::from([(1, (2, 10))]),
        };
        assert!(stale.check(3, &samples).is_err(), "removed rows must fail");
        let extra = Multiset {
            buckets: BTreeMap::from([(0, (1, 5)), (1, (2, 10)), (2, (1, -3))]),
        };
        assert!(extra.check(3, &samples).is_err(), "stale extra rows must fail");
        assert!(extra.check(4, &samples).is_err());
        let duplicated = Multiset {
            buckets: BTreeMap::from([(1, (3, 10)), (2, (1, -3))]),
        };
        assert!(
            duplicated.check(3, &samples).is_err(),
            "reingested duplicates must fail"
        );
        let query = multiset_query(
            "external_table(\"direct_x\", timespan(1s))",
            &json!({"fields":[{"name":"id","type":"long"}]}),
            &samples,
        )?;
        assert!(
            query.contains("case(coalesce((['id'] == long(1)), false), 1, coalesce((['id'] == long(2)), false), 2, 0)")
        );
        assert!(multiset_query("t", &json!({"fields":[]}), &[]).is_err());
        Ok(())
    }

    #[test]
    fn cache_readiness_is_referenced_to_the_captured_commit() -> Result<()> {
        let ready = Acceleration {
            hot_size: Some(10),
            last_updated: Some("2026-01-01T00:00:00Z".to_owned()),
            pending_files: Some(0),
            pending_bytes: Some(0),
            complete_percent: Some(100.0),
            eligible: true,
            fresh: true,
        };
        assert!(ready.ready());
        for not_ready in [
            Acceleration {
                pending_files: Some(1),
                ..ready.clone()
            },
            Acceleration {
                complete_percent: Some(66.67),
                ..ready.clone()
            },
            Acceleration {
                eligible: false,
                ..ready.clone()
            },
            Acceleration {
                fresh: false,
                ..ready.clone()
            },
            Acceleration::default(),
        ] {
            assert!(!not_ready.ready(), "{not_ready:?}");
        }
        // Freshness compares the refresh time with the Delta commit, so an idle table whose cache
        // refreshed after its last commit but before snapshot capture remains ready.
        let command = acceleration_command("direct_x", "00:01:00", Some(1_700_000_000_000))?;
        assert!(command.contains("LastUpdatedDateTime >= unixtime_milliseconds_todatetime(1700000000000)"));
        assert!(command.contains("LastUpdatedDateTime > now() - totimespan(\"00:01:00\")"));
        assert!(acceleration_command("direct_x", "1m; .drop", None).is_err());
        let details = json!({"QueryAccelerationState": {
            "TableVersion": 4, "IsHealthy": true, "RowCount": 3, "AcceleratedRowCount": 3, "NotHealthyReason": ""
        }});
        let state = CacheState::from_details(&details);
        assert!(state.current(4, 3));
        assert!(flag(&json!(1)) && flag(&json!(true)) && !flag(&json!(0)) && !flag(&Value::Null));
        assert!(!state.current(5, 3), "a cache of an older Delta version is not current");
        assert!(!state.current(4, 4), "partially accelerated rows are not current");
        assert!(!CacheState::from_details(&json!({})).current(4, 3));
        assert_eq!(timespan("00:01:00")?, Duration::from_secs(60));
        assert_eq!(timespan("1.00:00:00")?, Duration::from_secs(86_400));
        assert_eq!(timespan("00:05:00.5000000")?, Duration::from_secs(300));
        assert!(timespan("5m").is_err());
        Ok(())
    }

    #[test]
    fn qap_observations_are_sanitized() {
        let observed = sanitize(&json!({
            "IsEnabled": true, "MaxAge": "00:01:00", "CachedTableName": "$_cached_x",
            "ConnectionStrings": ["abfss://c@a.dfs.core.windows.net/x;impersonate"],
            "State": "{\"CompletePercentage\":100,\"DataPath\":\"/delta/direct/x\",\"Note\":\"h@'/delta'\"}",
            "Nested": [{"Uri": "https://a?sig=secret", "HotSize": 7}]
        }));
        let text = observed.to_string();
        assert!(
            !text.contains("abfss") && !text.contains("/delta") && !text.contains("sig=") && !text.contains("$_cached"),
            "{text}"
        );
        assert_eq!(observed["State"]["CompletePercentage"], 100);
        assert_eq!(observed["Nested"][0]["HotSize"], 7);
        assert_eq!(observed["MaxAge"], "00:01:00");
    }

    #[test]
    fn azure_mode_requires_https_and_never_defines_tables() -> Result<()> {
        let directory = Directory::new()?;
        assert!(KustoClient::with_mode("http://cluster.kusto.windows.net", &directory.0, Mode::Azure, "db").is_err());
        assert!(
            KustoClient::with_mode(
                "https://user:secret@cluster.kusto.windows.net",
                &directory.0,
                Mode::Azure,
                "db"
            )
            .is_err()
        );
        assert!(
            KustoClient::with_mode("https://cluster.kusto.windows.net", &directory.0, Mode::Azure, "bad db").is_err()
        );
        assert!(KustoClient::with_mode("https://localhost:8080", &directory.0, Mode::Local, DATABASE).is_err());
        let client = KustoClient::with_mode("https://cluster.kusto.windows.net", &directory.0, Mode::Azure, "db")?;
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        assert!(runtime.block_on(client.expose("direct_x", "/delta/direct/x")).is_err());
        assert!(runtime.block_on(client.bootstrap()).is_ok());
        Ok(())
    }

    #[test]
    fn publication_barrier_requires_all_observable_work_to_finish() -> Result<()> {
        let idle =
            "arrowcol_flight_active_streams 0\narrowcol_dfe_buffered_batches 0\narrowcol_delta_pending_commits 0\n";
        assert!(publication_idle(idle)?);
        assert!(!publication_idle(
            &idle.replace("buffered_batches 0", "buffered_batches 1")
        )?);
        assert!(!publication_idle(
            &idle.replace("pending_commits 0", "pending_commits 1")
        )?);
        assert!(!publication_idle(
            &idle.replace("active_streams 0", "active_streams 1")
        )?);
        assert!(publication_idle("").is_err());
        Ok(())
    }

    #[test]
    fn durable_settle_waits_for_quiver_replay_to_stop() {
        let start = Instant::now();
        let at = |seconds| start + Duration::from_secs(seconds);
        let mut replay = ReplayQuiescence::default();
        assert!(
            !replay.observe(true, vec![1, 0], at(0)),
            "first observation has no baseline"
        );
        assert!(!replay.observe(true, vec![1, 0], at(1)));
        assert!(
            !replay.observe(true, vec![2, 0], at(5)),
            "a replay restarts the quiet period"
        );
        assert!(!replay.observe(true, vec![2, 0], at(6)));
        assert!(
            !replay.observe(false, vec![2, 0], at(12)),
            "in-memory work restarts the quiet period"
        );
        assert!(!replay.observe(true, vec![2, 0], at(13)));
        assert!(!replay.observe(true, vec![2, 0], at(17)));
        assert!(replay.observe(true, vec![2, 0], at(18)));
    }

    #[tokio::test]
    async fn committed_tables_are_exposed_once_and_source_changes_fail_explicitly() -> Result<()> {
        let directory = Directory::new()?;
        let table = "telemetry.sqlserver.sqlserver_wait_stats";
        let definitions = Arc::new(AtomicUsize::new(0));
        let policies = Arc::new(AtomicUsize::new(0));
        let (defined, altered) = (Arc::clone(&definitions), Arc::clone(&policies));
        let server = Server::start(move |request| {
            let command = request["csl"].as_str().context("command missing")?;
            if command.starts_with(".create-or-alter external table") {
                ensure!(command.contains("kind=delta (h@'/delta/direct/telemetry/sqlserver/sqlserver_wait_stats')"));
                defined.fetch_add(1, Ordering::Relaxed);
            } else if command.starts_with(".alter external table") {
                ensure!(command.contains("query_acceleration") && command.contains("00:01:00"));
                altered.fetch_add(1, Ordering::Relaxed);
            } else if command.ends_with("| count") {
                ensure!(command.starts_with("external_table(\"direct_sqlserver_wait_stats\")"));
                return Ok(Some(response(&["Count"], vec![json!([3])])));
            }
            Ok(Some(response(&[], vec![])))
        })?;
        let client = KustoClient::with_mode(&server.endpoint, &directory.0, Mode::Local, DATABASE)?;
        let mut exposure = Exposure {
            client,
            status: ExposureStatus::default(),
            schemas: BTreeMap::new(),
        };
        let snapshot = SnapshotInventory {
            tables: BTreeMap::from([(
                table.to_owned(),
                TableInventory {
                    schema: json!({"fields":[{"name":"id","type":"long"}]}),
                    version: 1,
                    files: vec!["part.parquet".to_owned()],
                },
            )]),
        };
        let live = AtomicBool::new(true);
        exposure.sync("direct", &snapshot, &live).await?;
        let mut optimized = snapshot.clone();
        optimized.tables.get_mut(table).context("table")?.files = vec!["compacted.parquet".to_owned()];
        optimized.tables.get_mut(table).context("table")?.version = 2;
        exposure.sync("direct", &optimized, &live).await?;
        assert_eq!(
            definitions.load(Ordering::Relaxed),
            1,
            "file replacement needs no redefinition"
        );
        assert_eq!(policies.load(Ordering::Relaxed), 1);
        assert!(exposure.status.live_tables.contains("direct_sqlserver_wait_stats"));
        assert_eq!(exposure.status.tables["direct_sqlserver_wait_stats"].version, 2);
        let mut changed = optimized.clone();
        changed.tables.get_mut(table).context("table")?.schema["fields"][0]["type"] = json!("string");
        assert!(exposure.sync("direct", &changed, &live).await.is_err());
        let removed = SnapshotInventory {
            tables: BTreeMap::new(),
        };
        assert!(exposure.sync("direct", &removed, &live).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_emulator_is_recorded_as_degraded_without_a_load_cancellation() -> Result<()> {
        use clap::Parser;
        let directory = Directory::new()?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        drop(listener);
        let cli = crate::cli::Cli::try_parse_from([
            "arrowcol-loadgen",
            "run",
            "--kusto-endpoint",
            &endpoint,
            "--results",
            directory.0.to_str().context("directory")?,
            "--drain-timeout",
            "1s",
        ])?;
        let crate::cli::Command::Run(arguments) = cli.command else {
            bail!("run arguments")
        };
        let sync = LiveSync::start(&arguments, "degraded-run").context("configured synchronization")?;
        assert!(sync.stop().await.is_err());
        let status: ExposureStatus = serde_json::from_slice(&fs::read(directory.0.join(STATUS))?)?;
        assert_eq!(status.state, "degraded");
        assert_eq!(status.backend, BACKEND);
        assert!(status.error.is_some());
        assert!(!status.live_observed);
        assert!(KustoClient::with_mode("http://user:secret@localhost/", &directory.0, Mode::Local, DATABASE).is_err());
        Ok(())
    }

    const CANARIES: [&str; 6] = [
        "CANARYSIG",
        "acct.dfs",
        "11111111-2222",
        "99999999-aaaa",
        "eyJhbGciOi",
        "/delta/private",
    ];

    fn assert_public(text: &str) {
        for canary in CANARIES {
            assert!(!text.contains(canary), "{canary} leaked: {text}");
        }
    }

    #[tokio::test]
    async fn private_identifiers_never_reach_logs_status_or_reports() -> Result<()> {
        let directory = Directory::new()?;
        let message = "Partial query failure: abfss://private@acct.dfs.core.windows.net/x?sig=CANARYSIG \
            on /subscriptions/11111111-2222-3333-4444-555555555555/resourceGroups/rg identity \
            99999999-aaaa-bbbb-cccc-dddddddddddd bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.CANARY";
        let server = Server::start(move |_| {
            Ok(Some(json!([
                {"FrameType":"DataTable","TableKind":"PrimaryResult","Columns":[{"ColumnName":"Rows"}],"Rows":[[7]]},
                {"FrameType":"DataSetCompletion","HasErrors":true,"OneApiErrors":[{"error":{
                    "message": message,
                    "@context": format!("{{\"details\":\"{}\"}}", message.replace('"', "'")),
                }}]}
            ])))
        })?;
        let client = KustoClient::with_mode(&server.endpoint, &directory.0, Mode::Local, DATABASE)?;
        let error = client
            .execute(
                "external_table(\"x\") | where p == h@'/delta/private/x'",
                false,
                "validation",
            )
            .await
            .err()
            .context("HTTP 200 partial failure must fail closed")?;
        assert_public(&format!("{error:#}"));
        assert!(
            format!("{error:#}").contains("Partial query failure"),
            "diagnostics are kept"
        );
        degraded(&directory.0, "run", &error)?;
        validation_failure(&directory.0, &error)?;
        for file in ["kusto-queries.jsonl", STATUS, "kusto-validation.json"] {
            assert_public(&fs::read_to_string(directory.0.join(file))?);
        }
        assert_eq!(
            redact_text("TableVersion 4 at 2026-10-27T03:41:46Z"),
            "TableVersion 4 at 2026-10-27T03:41:46Z"
        );
        Ok(())
    }

    fn deadline_check() -> TableCheck {
        TableCheck {
            name: "direct_x".to_owned(),
            table: "telemetry.sqlserver.x".to_owned(),
            pool: "direct",
            summary: TableSummary {
                schema: json!({"fields":[{"name":"id","type":"long"}]}),
                version: 4,
                active_files: 1,
                rows: 1,
                bytes: 1,
                minimum_file_bytes: 1,
                maximum_file_bytes: 1,
                row_hashes: BTreeMap::new(),
                payload_row_hashes: BTreeMap::new(),
                files: Vec::new(),
                partition_columns: Vec::new(),
            },
            samples: vec![(json!({"id":1}), 1)],
            commit_ms: None,
            max_age: "00:01:00".to_owned(),
            policy: Value::Null,
            fallback: Multiset {
                buckets: BTreeMap::from([(1, (1, 5))]),
            },
        }
    }

    fn ready_cache(delay: Arc<AtomicU64>) -> Result<Server> {
        Server::start(move |request| {
            let command = request["csl"].as_str().context("command missing")?;
            if command.contains("query_acceleration statistics") {
                return Ok(Some(response(
                    &[
                        "HotSize",
                        "LastUpdatedDateTime",
                        "PendingFiles",
                        "PendingBytes",
                        "Complete",
                        "Eligible",
                        "Fresh",
                    ],
                    vec![json!([10, "2026-10-27T00:00:00Z", 0, 0, 100.0, 1, 1])],
                )));
            }
            if command.ends_with(" details") {
                return Ok(Some(response(
                    &["QueryAccelerationState"],
                    vec![json!([
                        "{\"TableVersion\":4,\"IsHealthy\":true,\"RowCount\":1,\"AcceleratedRowCount\":1}"
                    ])],
                )));
            }
            ensure!(
                command.starts_with("external_table(\"direct_x\")"),
                "unexpected {command}"
            );
            thread::sleep(Duration::from_millis(delay.load(Ordering::Relaxed)));
            Ok(Some(response(&["Sample", "Rows", "Hash"], vec![json!([1, 1, 5])])))
        })
    }

    #[tokio::test]
    async fn cache_success_after_the_fixed_deadline_is_rejected() -> Result<()> {
        let directory = Directory::new()?;
        let delay = Arc::new(AtomicU64::new(0));
        let server = ready_cache(Arc::clone(&delay))?;
        let client = KustoClient::with_mode(&server.endpoint, &directory.0, Mode::Local, DATABASE)?;
        let checks = [deadline_check()];
        let converged = converge(&client, &checks, Duration::from_secs(10)).await?;
        let observations = converged["direct_x"]["observations"]
            .as_array()
            .context("observations recorded")?;
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0]["outcome"], "converged");
        // Every table is ready, but its only cached query returns after the deadline.
        delay.store(1_500, Ordering::Relaxed);
        let started = Instant::now();
        let late = converge(&client, &checks, Duration::from_millis(500)).await;
        assert!(late.is_err(), "a cached result after the deadline must not pass");
        assert!(
            started.elapsed() < Duration::from_millis(1_400),
            "the deadline covers in-flight requests"
        );
        delay.store(0, Ordering::Relaxed);
        Ok(())
    }

    #[tokio::test]
    async fn live_observation_is_read_only_and_never_fabricates_measurements() -> Result<()> {
        let directory = Directory::new()?;
        let commands = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let seen = Arc::clone(&commands);
        let server = Server::start(move |request| {
            let command = request["csl"].as_str().context("command missing")?.to_owned();
            seen.lock()
                .map_err(|_| anyhow::anyhow!("poisoned"))?
                .push(command.clone());
            if command.contains("direct_absent") {
                let mut failed = response(&["Rows"], vec![]);
                failed["Exceptions"] = json!(["'direct_absent' was not found"]);
                return Ok(Some(failed));
            }
            if command.ends_with("policy query_acceleration") {
                return Ok(Some(response(
                    &["Policy"],
                    vec![json!([
                        "{\"IsEnabled\":true,\"Hot\":\"1.00:00:00\",\"MaxAge\":\"00:01:00\"}"
                    ])],
                )));
            }
            if command.contains("query_acceleration statistics") && command.contains("direct_ready") {
                return Ok(Some(response(
                    &[
                        "HotSize",
                        "LastUpdatedDateTime",
                        "PendingFiles",
                        "PendingBytes",
                        "Complete",
                        "Eligible",
                        "Fresh",
                    ],
                    vec![json!([10, "2026-10-27T00:00:00Z", 2, null, 50.0, 1, 1])],
                )));
            }
            if command.ends_with(" details") {
                return Ok(Some(response(
                    &["QueryAccelerationState"],
                    vec![json!(["{\"TableVersion\":3,\"IsHealthy\":true}"])],
                )));
            }
            if command == ".show capacity" {
                return Ok(Some(response(&["Resource", "Total"], vec![json!(["Ingestions", 2])])));
            }
            Ok(Some(response(&[], vec![])))
        })?;
        let tables = ["direct_ready", "direct_empty", "direct_absent"].map(str::to_owned);
        let observed = observe_qap(
            &server.endpoint,
            &directory.0,
            Some(DATABASE),
            &tables,
            Duration::from_secs(10),
        )
        .await?;
        let ready = &observed["tables"]["direct_ready"];
        assert_eq!(ready["available"], true);
        assert_eq!(ready["statistics"]["AccelerationPendingDataFilesCount"], 2);
        assert_eq!(ready["statistics"]["AccelerationPendingDataFilesSize"], Value::Null);
        assert_eq!(ready["statistics"]["AccelerationCompletePercentage"], 50.0);
        assert_eq!(ready["statistics"]["Eligible"], true);
        assert_eq!(ready["statistics"]["Fresh"], Value::Null, "no correctness claim");
        assert_eq!(ready["state"]["TableVersion"], 3);
        assert_eq!(ready["state"]["AcceleratedRowCount"], Value::Null);
        assert_eq!(observed["tables"]["direct_empty"]["statistics"], Value::Null);
        assert_eq!(observed["tables"]["direct_absent"]["available"], false);
        assert_eq!(observed["capacity"]["available"], true);
        let commands = commands.lock().map_err(|_| anyhow::anyhow!("poisoned"))?;
        assert!(
            commands
                .iter()
                .all(|command| command.starts_with(".show") || command.starts_with("external_table")),
            "observation must not change state: {commands:?}"
        );
        Ok(())
    }

    #[test]
    fn only_recognized_absence_is_pending_during_azure_startup() {
        // Emulator-observed shape for an external table whose run root has no committed Delta log.
        assert!(not_yet_readable(
            &anyhow::anyhow!(
                "Kusto partial/progressive query failure: {{\"OneApiErrors\":[{{\"error\":{{\"code\":\"LimitsExceeded\",\
                 \"message\":\"Partial query failure: The requested blob was not found (E_XSTORE_BLOB_NOT_FOUND). \
                 (message: 'Delta table does not exist: ')\"}}}}]}}"
            )
            .context("live observation")
        ));
        for surfaced in [
            "Kusto HTTP 400 Bad Request: {\"error\":{\"code\":\"General_BadRequest\"}}",
            "Kusto HTTP 400 Bad Request: {\"error\":{\"code\":\"General_BadRequest\",\"message\":\"Syntax error\"}}",
            "Kusto HTTP 400 Bad Request: General_BadRequest: invalid schema; column not found",
            "Kusto HTTP 400 Bad Request: General_BadRequest: external table credentials are forbidden",
            "Kusto HTTP 400 Bad Request: EntityNotFound: _delta_log does not exist",
            "Kusto HTTP 401 Unauthorized: {}",
            "Kusto HTTP 403 Forbidden: E_XSTORE_BLOB_NOT_FOUND",
            "Kusto HTTP 500 Internal Server Error: {}",
            "Kusto HTTP request failed: timed out",
            "Kusto partial/progressive query failure: E_XSTORE_BLOB_NOT_FOUND (message: 'Access denied')",
            "Kusto count missing",
        ] {
            assert!(!not_yet_readable(&anyhow::anyhow!("{surfaced}")), "{surfaced}");
        }
    }

    /// Real-emulator check of every native validation step against a pool root mounted at
    /// `/delta/direct`. `loadgen/scripts/kusto-qap-it.sh` writes, mutates and VACUUMs the fixture.
    #[tokio::test]
    #[ignore = "requires KUSTO_IT_ENDPOINT and KUSTO_IT_ROOT (emulator mounting the root at /delta/direct)"]
    async fn native_qap_converges_on_emulator() -> Result<()> {
        let endpoint = std::env::var("KUSTO_IT_ENDPOINT")?;
        let root = PathBuf::from(std::env::var("KUSTO_IT_ROOT")?);
        let results = std::env::temp_dir().join(format!("kusto-it-{}", std::process::id()));
        fs::create_dir_all(&results)?;
        let client = KustoClient::new(&endpoint, &results)?;
        client.bootstrap().await?;
        let summary = delta_inspect::summarize(&root).await?;
        ensure!(!summary.tables.is_empty(), "fixture has no Delta tables");
        for (table, table_summary) in &summary.tables {
            let name = external_name("direct", table)?;
            if std::env::var_os("KUSTO_IT_EXPOSE").is_some() {
                client.expose(&name, &table_location("direct", table)?).await?;
            }
            assert_eq!(client.schema(&name).await?, columns(&table_summary.schema)?);
            let samples = delta_inspect::sample_rows(&root, table, table_summary)?;
            let fallback = client
                .multiset(&fallback_source(&name)?, &table_summary.schema, &samples)
                .await?;
            fallback.check(table_summary.rows, &samples)?;
            let (_, max_age) = client.max_age(&name).await?;
            let commit_ms = commit_millis(&root, table, table_summary.version);
            let started = Instant::now();
            loop {
                let before = client.acceleration(&name, &max_age, commit_ms).await?;
                let state = CacheState::from_details(&client.details(&name).await);
                if before.ready() && state.current(table_summary.version, table_summary.rows) {
                    let cached = client
                        .multiset(&external_source(&name)?, &table_summary.schema, &samples)
                        .await?;
                    let verdict = cached.check(table_summary.rows, &samples);
                    eprintln!(
                        "{name} v{} rows {} +{}s ready {before:?} {state:?} cached={verdict:?} equal={}",
                        table_summary.version,
                        table_summary.rows,
                        started.elapsed().as_secs(),
                        cached == fallback
                    );
                    if verdict.is_ok() && cached == fallback {
                        break;
                    }
                }
                ensure!(
                    started.elapsed() < Duration::from_secs(420),
                    "{name} cache did not converge: {before:?} {state:?}"
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
        fs::remove_dir_all(&results)?;
        Ok(())
    }
}
