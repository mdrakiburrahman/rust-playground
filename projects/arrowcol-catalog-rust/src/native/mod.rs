//! Native authorities for adapter-owned, immutable v1 contracts.
mod hive;
#[allow(clippy::all, clippy::expect_used, clippy::unwrap_used)]
mod hms;

use std::{collections::BTreeMap, fs::File, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use arrowcol_catalog::CatalogSchemaVersion;
use reqwest::{Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;

/// Deployment-only native catalogue selection; never sent to an SDK.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeConfig {
    /// `unity` or `hive`.
    pub provider: NativeProvider,
    /// Direct local REST or Thrift endpoint, not the CES bridge.
    pub endpoint: String,
}

/// Supported local native metadata services.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NativeProvider {
    /// Unity Catalog REST API.
    Unity,
    /// Hive 4 Metastore Thrift API.
    Hive,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Column {
    name: String,
    data_type: String,
    nullable: bool,
    comment: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Definition {
    catalog: String,
    database: String,
    table: String,
    columns: Vec<Column>,
    location: String,
    properties: BTreeMap<String, String>,
    comment: Option<String>,
}

impl Definition {
    fn from_schema(schema: &CatalogSchemaVersion) -> Result<Self> {
        ensure!(
            schema.version().get() == 1,
            "native adapters currently accept immutable v1 CREATE contracts only"
        );
        ensure!(
            schema.metadata().partition_columns.is_empty(),
            "native contracts do not yet support partition columns"
        );
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let data_type = field.mapping().spark()?.to_ascii_lowercase();
                primitive_type(&data_type)?;
                Ok(Column {
                    name: field.name().to_owned(),
                    data_type,
                    nullable: field.is_nullable(),
                    comment: field.comment().filter(|value| !value.is_empty()).map(str::to_owned),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let definition = Self {
            catalog: schema.identity().catalog().to_string(),
            database: schema.identity().schema().to_string(),
            table: schema.identity().table().to_string(),
            columns,
            location: normalized_location(schema.metadata().location.as_deref().context("missing location")?)?,
            properties: schema.metadata().properties.clone(),
            comment: schema.metadata().comment.clone().filter(|value| !value.is_empty()),
        };
        for name in [&definition.catalog, &definition.database, &definition.table]
            .into_iter()
            .chain(definition.columns.iter().map(|column| &column.name))
        {
            ensure!(
                !name.is_empty()
                    && name.len() <= 128
                    && name
                        .bytes()
                        .enumerate()
                        .all(|(index, ch)| ch == b'_' || ch.is_ascii_lowercase() || index > 0 && ch.is_ascii_digit()),
                "native contracts require lowercase simple identifiers: {name}"
            );
        }
        Ok(definition)
    }

    fn name(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.database, self.table)
    }
}

fn primitive_type(value: &str) -> Result<(&'static str, String)> {
    let (unity, spark) = match value {
        "boolean" => ("BOOLEAN", "boolean"),
        "tinyint" => ("BYTE", "byte"),
        "smallint" => ("SHORT", "short"),
        "int" => ("INT", "integer"),
        "bigint" => ("LONG", "long"),
        "float" => ("FLOAT", "float"),
        "double" => ("DOUBLE", "double"),
        "string" => ("STRING", "string"),
        "binary" => ("BINARY", "binary"),
        "date" => ("DATE", "date"),
        "timestamp" => ("TIMESTAMP", "timestamp"),
        "timestamp_ntz" => ("TIMESTAMP_NTZ", "timestamp_ntz"),
        value if value.starts_with("decimal(") && value.ends_with(')') => ("DECIMAL", value),
        _ => bail!("native contract type is not yet supported losslessly: {value}"),
    };
    Ok((unity, spark.to_owned()))
}

fn normalized_location(value: &str) -> Result<String> {
    let url = if value.contains(':') {
        Url::parse(value)?
    } else {
        Url::from_directory_path(value).map_err(|()| anyhow::anyhow!("expected an absolute native warehouse path"))?
    };
    ensure!(
        url.scheme() == "file" && url.host_str().is_none(),
        "local native demo requires a shared local file warehouse"
    );
    Ok(url.to_string().trim_end_matches('/').to_owned())
}

#[derive(Debug)]
struct Observed {
    id: String,
    definition: Definition,
}

/// Durable evidence outside native table properties and frozen SDK bytes.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NativeReceipt {
    provider: NativeProvider,
    endpoint: String,
    table: String,
    native_table_id: String,
    contract_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Record {
    contract_sha256: String,
    definition: Definition,
    attempted: bool,
    receipt: Option<NativeReceipt>,
}

pub(crate) struct NativeCatalog {
    config: NativeConfig,
    root: PathBuf,
    client: Client,
    _lease: File,
}

impl NativeCatalog {
    pub(crate) fn open(config: NativeConfig, root: PathBuf) -> Result<Self> {
        let url = Url::parse(&config.endpoint)?;
        ensure!(
            url.username().is_empty() && url.password().is_none() && url.query().is_none() && url.fragment().is_none(),
            "native endpoints cannot contain credentials, query parameters or fragments"
        );
        let host = url.host_str().context("native endpoint needs a host")?;
        let local = host == "localhost"
            || host.starts_with("arrowcol-native-")
            || host.parse::<std::net::IpAddr>().is_ok_and(|ip| match ip {
                std::net::IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
                std::net::IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
            });
        ensure!(
            local,
            "unauthenticated native adapters are restricted to local development endpoints"
        );
        match config.provider {
            NativeProvider::Unity => ensure!(
                url.scheme() == "http" && url.path() == "/api/2.1/unity-catalog",
                "expected local Unity REST API origin and /api/2.1/unity-catalog path"
            ),
            NativeProvider::Hive => ensure!(
                url.scheme() == "thrift" && url.port().is_some() && url.path().is_empty(),
                "expected thrift://host:port"
            ),
        }
        std::fs::create_dir_all(&root)?;
        let lease = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("owner.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lease).context("another bridge owns this native contract ledger")?;
        let config_path = root.join("authority.json");
        let bytes = serde_json::to_vec(&config)?;
        if config_path.exists() {
            ensure!(
                std::fs::read(config_path)? == bytes,
                "native authority endpoint/provider changed; refusing ledger reuse"
            );
        } else {
            super::service::write_durable(&config_path, &bytes)?;
        }
        Ok(Self {
            config,
            root,
            _lease: lease,
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(10))
                .build()?,
        })
    }

    fn record_path(&self, schema: &CatalogSchemaVersion) -> PathBuf {
        self.root.join(format!("{}.json", schema.table_id()))
    }

    fn record(&self, schema: &CatalogSchemaVersion) -> Result<Option<Record>> {
        let path = self.record_path(schema);
        let mut bytes = std::fs::read(&path);
        for _ in 0..5 {
            if !matches!(&bytes, Err(error) if error.kind() == std::io::ErrorKind::NotFound) {
                break;
            }
            // WSL/Windows mounts can briefly hide a destination during replacement.
            // Retry only the local read, never a native mutation.
            std::thread::sleep(Duration::from_millis(20));
            bytes = std::fs::read(&path);
        }
        match bytes {
            Ok(bytes) => {
                let record: Record = serde_json::from_slice(&bytes)?;
                ensure!(
                    record.contract_sha256 == contract_digest(schema)?
                        && record.definition == Definition::from_schema(schema)?,
                    "native receipt belongs to a different contract"
                );
                if let Some(receipt) = &record.receipt {
                    ensure!(
                        record.attempted
                            && receipt.provider == self.config.provider
                            && receipt.endpoint == self.config.endpoint
                            && receipt.table == record.definition.name()
                            && receipt.contract_sha256 == record.contract_sha256
                            && !receipt.native_table_id.is_empty(),
                        "native receipt provenance is inconsistent"
                    );
                }
                Ok(Some(record))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn save(&self, schema: &CatalogSchemaVersion, record: &Record) -> Result<()> {
        super::service::write_durable(&self.record_path(schema), &serde_json::to_vec(record)?)
    }

    pub(crate) async fn preflight(&self, schema: &CatalogSchemaVersion) -> Result<()> {
        let definition = Definition::from_schema(schema)?;
        if self.config.provider == NativeProvider::Hive {
            ensure!(
                definition.catalog == "hive",
                "local Hive contracts require the hive catalog"
            );
            for key in [
                "EXTERNAL",
                "spark.sql.sources.provider",
                "comment",
                "transient_lastDdlTime",
                "numFiles",
                "numFilesErasureCoded",
                "totalSize",
                "COLUMN_STATS_ACCURATE",
                "numRows",
                "rawDataSize",
            ] {
                ensure!(
                    !definition.properties.contains_key(key),
                    "{key} is owned by the Hive metadata service"
                );
            }
        }
        ensure!(
            self.record(schema)?.is_none(),
            "native contract already has a registration intent; resume its original build"
        );
        ensure!(
            self.read(&definition).await?.is_none(),
            "native table already exists; foreign tables cannot be adopted"
        );
        Ok(())
    }

    pub(crate) fn has_intent(&self, schema: &CatalogSchemaVersion) -> Result<bool> {
        Ok(self.record(schema)?.is_some())
    }

    /// Returns true only for an existing, identical local provisioning intent.
    pub(crate) async fn begin(&self, schema: &CatalogSchemaVersion) -> Result<bool> {
        if let Some(record) = self.record(schema)? {
            ensure!(
                !record.attempted || record.receipt.is_some(),
                "native CREATE outcome is uncertain; retained intent must be reconciled, not replayed"
            );
            if record.receipt.is_some() {
                self.verify(schema).await?;
            } else {
                ensure!(
                    self.read(&record.definition).await?.is_none(),
                    "native table appeared before registration; refusing adoption"
                );
            }
            return Ok(true);
        }
        self.preflight(schema).await?;
        self.save(
            schema,
            &Record {
                contract_sha256: contract_digest(schema)?,
                definition: Definition::from_schema(schema)?,
                attempted: false,
                receipt: None,
            },
        )?;
        Ok(false)
    }

    pub(crate) async fn register(&self, schema: &CatalogSchemaVersion) -> Result<NativeReceipt> {
        let mut record = self.record(schema)?.context("missing native provisioning intent")?;
        if record.receipt.is_some() {
            return self.verify(schema).await;
        }
        ensure!(!record.attempted, "native CREATE outcome is uncertain; refusing replay");
        self.namespaces(&record.definition).await?;
        ensure!(
            self.read(&record.definition).await?.is_none(),
            "native table appeared before CREATE; refusing adoption"
        );
        record.attempted = true;
        self.save(schema, &record)?;
        let observed = self.create(&record.definition).await?;
        let receipt = NativeReceipt {
            provider: self.config.provider,
            endpoint: self.config.endpoint.clone(),
            table: record.definition.name(),
            native_table_id: observed.id,
            contract_sha256: record.contract_sha256.clone(),
        };
        record.receipt = Some(receipt.clone());
        self.save(schema, &record)?;
        ensure!(
            observed.definition == record.definition,
            "native CREATE read-back differs from the frozen contract"
        );
        self.verify(schema).await?;
        Ok(receipt)
    }

    pub(crate) async fn verify(&self, schema: &CatalogSchemaVersion) -> Result<NativeReceipt> {
        let record = self
            .record(schema)?
            .context("native contract has no local receipt; refusing import")?;
        let receipt = record.receipt.context("native registration is not confirmed")?;
        let observed = self
            .read(&record.definition)
            .await?
            .context("registered native table was removed")?;
        ensure!(
            observed.id == receipt.native_table_id,
            "native table identity changed; refusing replacement"
        );
        ensure!(
            observed.definition == record.definition,
            "native metadata drifted from the immutable contract"
        );
        Ok(receipt)
    }

    pub(crate) fn receipt_json(&self, schema: &CatalogSchemaVersion) -> Result<Vec<u8>> {
        serde_json::to_vec(
            &self
                .record(schema)?
                .and_then(|record| record.receipt)
                .context("missing native receipt")?,
        )
        .map_err(Into::into)
    }

    pub(crate) async fn probe(&self) -> Result<()> {
        match self.config.provider {
            NativeProvider::Unity => {
                self.unity(Method::GET, "catalogs", None)
                    .await?
                    .context("Unity catalogue API is unavailable")?;
                Ok(())
            }
            NativeProvider::Hive => hive::probe(self.config.endpoint.clone()).await,
        }
    }

    async fn unity(&self, method: Method, path: &str, body: Option<Value>) -> Result<Option<Value>> {
        let mut request = self
            .client
            .request(method.clone(), format!("{}/{path}", self.config.endpoint));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await?;
        if method == Method::GET && response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = response.error_for_status()?;
        Ok(Some(response.json().await?))
    }

    async fn read(&self, definition: &Definition) -> Result<Option<Observed>> {
        match self.config.provider {
            NativeProvider::Unity => self
                .unity(Method::GET, &format!("tables/{}", definition.name()), None)
                .await?
                .map(unity_observed)
                .transpose(),
            NativeProvider::Hive => hive::read(self.config.endpoint.clone(), definition.clone()).await,
        }
    }

    async fn namespaces(&self, definition: &Definition) -> Result<()> {
        match self.config.provider {
            NativeProvider::Unity => {
                for (lookup, collection, body) in [
                    (
                        format!("catalogs/{}", definition.catalog),
                        "catalogs",
                        json!({"name": definition.catalog}),
                    ),
                    (
                        format!("schemas/{}.{}", definition.catalog, definition.database),
                        "schemas",
                        json!({"name": definition.database, "catalog_name": definition.catalog}),
                    ),
                ] {
                    if self.unity(Method::GET, &lookup, None).await?.is_none() {
                        // Only table CREATE is non-replayable; a competing namespace CREATE is
                        // accepted solely after the exact namespace can be read successfully.
                        if let Err(error) = self.unity(Method::POST, collection, Some(body)).await
                            && self.unity(Method::GET, &lookup, None).await?.is_none()
                        {
                            return Err(error);
                        }
                    }
                }
                Ok(())
            }
            NativeProvider::Hive => hive::namespaces(self.config.endpoint.clone(), definition.clone()).await,
        }
    }

    async fn create(&self, definition: &Definition) -> Result<Observed> {
        match self.config.provider {
            NativeProvider::Unity => {
                let columns = definition.columns.iter().enumerate().map(|(index, column)| {
                    let (native_type, spark_type) = primitive_type(&column.data_type)?;
                    Ok(json!({
                        "name": column.name, "position": index, "nullable": column.nullable,
                        "type_name": native_type, "type_text": column.data_type,
                        "type_json": json!({"name": column.name, "type": spark_type, "nullable": column.nullable, "metadata": {}}).to_string(),
                        "comment": column.comment,
                    }))
                }).collect::<Result<Vec<_>>>()?;
                let created = self
                    .unity(
                        Method::POST,
                        "tables",
                        Some(json!({
                            "name": definition.table, "catalog_name": definition.catalog,
                            "schema_name": definition.database, "table_type": "EXTERNAL",
                            "data_source_format": "DELTA", "storage_location": definition.location,
                            "columns": columns, "properties": definition.properties, "comment": definition.comment,
                        })),
                    )
                    .await?
                    .context("missing Unity CREATE response")?;
                unity_observed(created)
            }
            NativeProvider::Hive => hive::create(self.config.endpoint.clone(), definition.clone()).await,
        }
    }
}

/// Read-only structured Hive metadata, including NOT NULL constraints.
///
/// # Errors
/// Rejects invalid table names and propagates native lookup/transport failures.
pub async fn inspect_hive_table(endpoint: String, table: &str) -> Result<Value> {
    let parts: Vec<_> = table.split('.').collect();
    let [catalog, database, name] = parts.as_slice() else {
        bail!("expected catalog.database.table");
    };
    let identity = arrowcol_protocol::TableIdentity::new(catalog.parse()?, database.parse()?, name.parse()?);
    hive::inspect(
        endpoint,
        identity.catalog().to_string(),
        identity.schema().to_string(),
        identity.table().to_string(),
    )
    .await
}

fn contract_digest(schema: &CatalogSchemaVersion) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(schema)?)))
}

fn unity_observed(value: Value) -> Result<Observed> {
    #[derive(Deserialize)]
    struct UnityColumn {
        name: String,
        type_text: String,
        type_name: String,
        type_json: String,
        position: usize,
        nullable: bool,
        comment: Option<String>,
        partition_index: Option<u32>,
    }
    #[derive(Deserialize)]
    struct UnityTable {
        table_id: String,
        catalog_name: String,
        schema_name: String,
        name: String,
        table_type: String,
        data_source_format: String,
        storage_location: String,
        columns: Vec<UnityColumn>,
        properties: BTreeMap<String, String>,
        comment: Option<String>,
    }
    let table: UnityTable = serde_json::from_value(value)?;
    ensure!(
        !table.table_id.is_empty() && table.table_type == "EXTERNAL" && table.data_source_format == "DELTA",
        "expected identified external Delta table in Unity"
    );
    let columns = table.columns.into_iter().enumerate().map(|(index, column)| {
        let data_type = column.type_text.to_ascii_lowercase();
        let (native_type, spark_type) = primitive_type(&data_type)?;
        let type_json: Value = serde_json::from_str(&column.type_json)?;
        ensure!(column.position == index && column.partition_index.is_none() && column.type_name == native_type &&
            type_json == json!({"name": column.name, "type": spark_type, "nullable": column.nullable, "metadata": {}}),
            "Unity column type, order, nullability or metadata is inconsistent");
        Ok(Column { name: column.name, data_type, nullable: column.nullable, comment: column.comment.filter(|value| !value.is_empty()) })
    }).collect::<Result<_>>()?;
    Ok(Observed {
        id: table.table_id,
        definition: Definition {
            catalog: table.catalog_name,
            database: table.schema_name,
            table: table.name,
            columns,
            location: normalized_location(&table.storage_location)?,
            properties: table.properties,
            comment: table.comment.filter(|value| !value.is_empty()),
        },
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrowcol_catalog::{DdlCommand, DdlParser, SchemaVersion, deterministic_table_id};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct MockUnity {
        config: NativeConfig,
        table: Arc<Mutex<Option<Value>>>,
        lose_create_response: Arc<AtomicBool>,
        creates: Arc<AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for MockUnity {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn mock_unity() -> MockUnity {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = NativeConfig {
            provider: NativeProvider::Unity,
            endpoint: format!("http://{}/api/2.1/unity-catalog", listener.local_addr().unwrap()),
        };
        let table = Arc::new(Mutex::new(None));
        let lose_create_response = Arc::new(AtomicBool::new(false));
        let creates = Arc::new(AtomicUsize::new(0));
        let (stored, lost, count) = (
            Arc::clone(&table),
            Arc::clone(&lose_create_response),
            Arc::clone(&creates),
        );
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (stored, lost, count) = (Arc::clone(&stored), Arc::clone(&lost), Arc::clone(&count));
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    let mut buffer = [0; 4096];
                    let end = loop {
                        let read = stream.read(&mut buffer).await.unwrap();
                        if read == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&buffer[..read]);
                        assert!(bytes.len() < 1024 * 1024);
                        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                            break end + 4;
                        }
                    };
                    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|value| value.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    while bytes.len() < end + length {
                        let read = stream.read(&mut buffer).await.unwrap();
                        assert_ne!(read, 0);
                        bytes.extend_from_slice(&buffer[..read]);
                    }
                    let (status, body) = if headers.starts_with("POST ") {
                        count.fetch_add(1, Ordering::SeqCst);
                        let mut value: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
                        value["table_id"] = json!("native-owned-id");
                        *stored.lock().unwrap() = Some(value.clone());
                        if lost.load(Ordering::SeqCst) {
                            return;
                        }
                        (200, value)
                    } else if headers.lines().next().unwrap().contains("/tables/") {
                        stored
                            .lock()
                            .unwrap()
                            .clone()
                            .map_or((404, json!({})), |value| (200, value))
                    } else {
                        (200, json!({}))
                    };
                    let body = body.to_string();
                    stream.write_all(format!(
                        "HTTP/1.1 {status} Result\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()).as_bytes()).await.unwrap();
                });
            }
        });
        MockUnity {
            config,
            table,
            lose_create_response,
            creates,
            task,
        }
    }

    fn schema(root: &std::path::Path, ddl: &str) -> CatalogSchemaVersion {
        let DdlCommand::CreateTable(create) = DdlParser::default().parse(ddl).unwrap() else {
            panic!("CREATE expected");
        };
        let mut metadata = create.metadata().clone();
        metadata.location = Some(root.join("warehouse").to_str().unwrap().to_owned());
        CatalogSchemaVersion::new(
            deterministic_table_id(create.identity()),
            create.identity().clone(),
            SchemaVersion::FIRST,
            create.fields().to_vec(),
            create.canonical_ddl(),
        )
        .unwrap()
        .with_metadata(metadata)
        .unwrap()
    }

    fn config() -> NativeConfig {
        NativeConfig {
            provider: NativeProvider::Unity,
            endpoint: "http://127.0.0.1:1/api/2.1/unity-catalog".to_owned(),
        }
    }

    #[tokio::test]
    async fn uncertain_native_create_cannot_be_replayed_or_reinterpreted() {
        let root = tempfile::tempdir().unwrap();
        let native = NativeCatalog::open(config(), root.path().join("receipts")).unwrap();
        let candidate = schema(
            root.path(),
            "CREATE TABLE demo.prod.events (id BIGINT NOT NULL) USING DELTA",
        );
        native
            .save(
                &candidate,
                &Record {
                    contract_sha256: contract_digest(&candidate).unwrap(),
                    definition: Definition::from_schema(&candidate).unwrap(),
                    attempted: true,
                    receipt: None,
                },
            )
            .unwrap();
        assert!(
            native
                .begin(&candidate)
                .await
                .unwrap_err()
                .to_string()
                .contains("uncertain")
        );
        assert!(
            native
                .register(&candidate)
                .await
                .unwrap_err()
                .to_string()
                .contains("uncertain")
        );
        let altered = schema(
            root.path(),
            "CREATE TABLE demo.prod.events (id BIGINT NOT NULL, payload STRING) USING DELTA",
        );
        assert!(
            native
                .has_intent(&altered)
                .unwrap_err()
                .to_string()
                .contains("different contract")
        );
        assert!(
            native
                .receipt_json(&candidate)
                .unwrap_err()
                .to_string()
                .contains("missing native receipt")
        );
    }

    #[tokio::test]
    async fn native_commit_with_lost_response_retains_unknown_intent_without_retry() {
        let server = mock_unity().await;
        server.lose_create_response.store(true, Ordering::SeqCst);
        let root = tempfile::tempdir().unwrap();
        let native = NativeCatalog::open(server.config.clone(), root.path().join("receipts")).unwrap();
        let candidate = schema(
            root.path(),
            "CREATE TABLE demo.prod.events (id BIGINT NOT NULL) USING DELTA",
        );
        native.begin(&candidate).await.unwrap();
        assert!(native.register(&candidate).await.is_err());
        assert!(
            server.table.lock().unwrap().is_some(),
            "native CREATE committed before transport failure"
        );
        assert!(
            native
                .register(&candidate)
                .await
                .unwrap_err()
                .to_string()
                .contains("uncertain")
        );
        assert_eq!(server.creates.load(Ordering::SeqCst), 1);
        assert!(native.record(&candidate).unwrap().unwrap().receipt.is_none());
    }

    #[tokio::test]
    async fn native_receipt_rejects_drift_replacement_and_deletion() {
        let server = mock_unity().await;
        let root = tempfile::tempdir().unwrap();
        let native = NativeCatalog::open(server.config.clone(), root.path().join("receipts")).unwrap();
        let candidate = schema(
            root.path(),
            "CREATE TABLE demo.prod.events (id BIGINT NOT NULL) USING DELTA",
        );
        native.begin(&candidate).await.unwrap();
        native.register(&candidate).await.unwrap();
        let original = server.table.lock().unwrap().clone().unwrap();
        server.table.lock().unwrap().as_mut().unwrap()["properties"]["foreign"] = json!("changed");
        assert!(
            native
                .verify(&candidate)
                .await
                .unwrap_err()
                .to_string()
                .contains("drifted")
        );
        *server.table.lock().unwrap() = Some(original.clone());
        server.table.lock().unwrap().as_mut().unwrap()["table_id"] = json!("replacement-id");
        assert!(
            native
                .verify(&candidate)
                .await
                .unwrap_err()
                .to_string()
                .contains("identity changed")
        );
        *server.table.lock().unwrap() = Some(original);
        assert!(native.begin(&candidate).await.unwrap());
        native.register(&candidate).await.unwrap();
        assert_eq!(
            server.creates.load(Ordering::SeqCst),
            1,
            "same receipt resumes without CREATE"
        );
        *server.table.lock().unwrap() = None;
        assert!(
            native
                .verify(&candidate)
                .await
                .unwrap_err()
                .to_string()
                .contains("removed")
        );
    }

    #[tokio::test]
    async fn concurrent_promotions_commit_once_and_native_drift_blocks_ces() {
        use arrowcol_catalog_client::rpc::{self, schema_catalog_server::SchemaCatalog};
        use tonic::Request;
        let server = mock_unity().await;
        let root = tempfile::tempdir().unwrap();
        let service = crate::CatalogService::open_native(
            crate::CatalogConfig {
                warehouse_root: root.path().join("warehouse").to_str().unwrap().to_owned(),
                state_dir: root.path().join("state"),
                ddl_dir: None,
                retained_events: 8,
                ingestion_layout: false,
            },
            server.config.clone(),
        )
        .await
        .unwrap();
        let ddl = "CREATE TABLE demo.prod.events (id BIGINT NOT NULL) USING DELTA";
        let first = service
            .prepare_table(Request::new(rpc::PrepareTableRequest { ddl: ddl.to_owned() }))
            .await
            .unwrap()
            .into_inner();
        let second = service
            .prepare_table(Request::new(rpc::PrepareTableRequest { ddl: ddl.to_owned() }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first, second);
        assert_eq!(server.creates.load(Ordering::SeqCst), 0);
        let (first, second) = tokio::join!(
            service.promote_table(Request::new(rpc::PromoteTableRequest { candidate: Some(first) })),
            service.promote_table(Request::new(rpc::PromoteTableRequest {
                candidate: Some(second)
            }))
        );
        assert_eq!(first.unwrap().into_inner(), second.unwrap().into_inner());
        assert_eq!(server.creates.load(Ordering::SeqCst), 1);
        assert_eq!(
            service
                .get_current_event_id(Request::new(rpc::Empty {}))
                .await
                .unwrap()
                .into_inner()
                .event_id,
            1
        );
        server.table.lock().unwrap().as_mut().unwrap()["properties"]["foreign"] = json!("changed");
        assert!(service.get_current_event_id(Request::new(rpc::Empty {})).await.is_err());
        assert!(service.get_snapshot(Request::new(rpc::Empty {})).await.is_err());
    }

    #[tokio::test]
    async fn ledger_lease_and_authority_binding_survive_reopen() {
        let root = tempfile::tempdir().unwrap();
        let native = NativeCatalog::open(config(), root.path().to_owned()).unwrap();
        assert!(NativeCatalog::open(config(), root.path().to_owned()).is_err());
        drop(native);
        drop(NativeCatalog::open(config(), root.path().to_owned()).unwrap());
        let mut changed = config();
        changed.endpoint = "http://127.0.0.1:2/api/2.1/unity-catalog".to_owned();
        assert!(NativeCatalog::open(changed, root.path().to_owned()).is_err());
    }

    #[test]
    fn native_projection_rejects_lossy_nested_types_and_partitions() {
        let root = tempfile::tempdir().unwrap();
        for ddl in [
            "CREATE TABLE demo.prod.events (id BIGINT, payload ARRAY<STRING>) USING DELTA",
            "CREATE TABLE demo.prod.events (id BIGINT) USING DELTA PARTITIONED BY (id)",
        ] {
            assert!(Definition::from_schema(&schema(root.path(), ddl)).is_err());
        }
    }
}
