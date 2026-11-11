use std::{
    collections::{BTreeMap, VecDeque},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use arrow_deltalake::{catalog::CatalogDeltaSnapshot, delta::DeltaTable};
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    encode::FlightDataEncoderBuilder,
    sql::{
        CommandStatementQuery, CommandStatementUpdate, ProstMessageExt, SqlInfo, TicketStatementQuery,
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};
use arrowcol_catalog::{
    CatalogError, CatalogErrorCode, CatalogResult, CatalogSchemaVersion, DdlCommand, DdlCompletion,
    DdlCompletionDisposition, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest, MemoryCatalogStore, SchemaVersion,
    SparkStatement, deterministic_table_id, generate_descriptor_artifacts, next_protobuf_field_number,
};
use arrowcol_catalog_client::{identity_from_request, rpc, table_request};
use arrowcol_protocol::TableIdentity;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast, mpsc};
use tonic::{Request, Response, Status};
use url::Url;

const HISTORY_KEY: &str = "catalog.schemaHistory";
const DROPPED_KEY: &str = "catalog.dropped";
const INGESTION: &str = "__arrowcol_ingestion";

/// Standalone service configuration; all state paths are outside source control.
#[derive(Clone, Debug)]
pub struct CatalogConfig {
    /// Local path/file URL or abfss warehouse root for table default locations.
    pub warehouse_root: String,
    /// Durable table-location index; schemas themselves live in Delta metadata.
    pub state_dir: PathBuf,
    /// Optional directory of single-statement Spark SQL seed files.
    pub ddl_dir: Option<PathBuf>,
    /// Maximum retained notifications in this process epoch.
    pub retained_events: usize,
    /// Keep ingress schemas logical while storing the DFE ingestion partition physically.
    pub ingestion_layout: bool,
}

#[derive(Clone)]
struct Table {
    history: Vec<CatalogSchemaVersion>,
    location: String,
    delta_version: u64,
}

struct State {
    epoch: String,
    event_id: u64,
    tables: BTreeMap<TableIdentity, Table>,
    events: VecDeque<rpc::ChangeEvent>,
    namespaces: BTreeMap<String, arrowcol_sparksql::CreateNamespace>,
}

#[derive(Serialize, Deserialize)]
struct IndexEntry {
    identity: TableIdentity,
    location: String,
}

#[derive(Serialize, Deserialize)]
struct QueryTicket {
    query: String,
    epoch: String,
    event_id: u64,
}

/// Flight SQL DDL and gRPC CES served over one tonic endpoint.
#[derive(Clone)]
pub struct CatalogService {
    config: Arc<CatalogConfig>,
    state: Arc<Mutex<State>>,
    ddl_lock: Arc<Mutex<()>>,
    events: broadcast::Sender<rpc::ChangeEvent>,
    store: Arc<MemoryCatalogStore>,
    parser: DdlParser,
    shutdown: tokio::sync::watch::Sender<bool>,
    #[cfg(test)]
    fail_after_commit: Arc<std::sync::atomic::AtomicBool>,
}

impl CatalogService {
    /// Ends long-lived CES streams so tonic can drain in-flight DDL before publisher closure.
    pub fn shutdown_watch_streams(&self) {
        self.shutdown.send_replace(true);
    }

    /// Rehydrates committed Delta metadata before applying missing SQL seeds.
    ///
    /// # Errors
    /// Rejects corrupt state/logs, unsupported schemas, invalid seeds and storage failures.
    pub async fn open(config: CatalogConfig) -> Result<Self> {
        if config.retained_events == 0 {
            bail!("retained_events must be positive");
        }
        std::fs::create_dir_all(&config.state_dir)?;
        if !config.warehouse_root.contains("://") {
            std::fs::create_dir_all(&config.warehouse_root)?;
        }
        let (events, _) = broadcast::channel(config.retained_events.min(4096));
        let namespaces_path = config.state_dir.join("namespaces.json");
        let namespaces = if namespaces_path.exists() {
            serde_json::from_slice(&std::fs::read(namespaces_path)?)?
        } else {
            BTreeMap::new()
        };
        let service = Self {
            config: Arc::new(config),
            state: Arc::new(Mutex::new(State {
                epoch: uuid::Uuid::new_v4().to_string(),
                event_id: 0,
                tables: BTreeMap::new(),
                events: VecDeque::new(),
                namespaces,
            })),
            ddl_lock: Arc::new(Mutex::new(())),
            events,
            store: Arc::new(MemoryCatalogStore::new()),
            parser: DdlParser::default(),
            shutdown: tokio::sync::watch::channel(false).0,
            #[cfg(test)]
            fail_after_commit: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let mut indexes = Vec::new();
        for entry in std::fs::read_dir(&service.config.state_dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|extension| extension == "json")
                && path.file_name().is_some_and(|name| name != "namespaces.json")
            {
                indexes.push(serde_json::from_slice::<IndexEntry>(&std::fs::read(path)?)?);
            }
        }
        // Discover managed table directories even if an index was lost.
        if let Ok(root) = warehouse_file_path(&service.config.warehouse_root) {
            discover_tables(&root, &root, &mut indexes)?;
        }
        for entry in indexes {
            if service.state.lock().await.tables.contains_key(&entry.identity) {
                continue;
            }
            if let Some(snapshot) = read_delta(&entry.location).await? {
                if service.config.ingestion_layout && !snapshot.properties.contains_key(HISTORY_KEY) {
                    // A supported bootstrap creates physical tables, not client schemas. Only
                    // a matching CREATE may adopt them; never expose physical columns to Flight.
                    continue;
                }
                if snapshot
                    .properties
                    .get(DROPPED_KEY)
                    .is_some_and(|value| value == "true")
                {
                    continue;
                }
                let table = service.recover_table(entry.identity.clone(), entry.location, snapshot)?;
                service.store.hydrate_committed(table.history.clone()).await?;
                service.state.lock().await.tables.insert(entry.identity, table);
            }
        }
        if let Some(dir) = &service.config.ddl_dir {
            let mut files = std::fs::read_dir(dir)?
                .map(|entry| entry.map(|entry| entry.path()))
                .collect::<std::io::Result<Vec<_>>>()?;
            files.retain(|path| path.extension().is_some_and(|extension| extension == "sql"));
            files.sort();
            for file in files {
                let sql = std::fs::read_to_string(&file)?;
                let statement = service.parser.parse_statement(&sql)?;
                if let SparkStatement::CreateTable(create) = &statement {
                    let identity = name_identity(&create.name)?;
                    if service.state.lock().await.tables.contains_key(&identity) {
                        continue;
                    }
                    let location = create
                        .metadata
                        .location
                        .clone()
                        .unwrap_or_else(|| service.default_location(&identity));
                    if let Some(snapshot) = read_delta(&location).await?
                        && snapshot.properties.get(DROPPED_KEY).is_none_or(|value| value != "true")
                        && (!service.config.ingestion_layout || snapshot.properties.contains_key(HISTORY_KEY))
                    {
                        let table = service.recover_table(identity.clone(), location.clone(), snapshot)?;
                        service.persist_index(&identity, &location)?;
                        service.store.hydrate_committed(table.history.clone()).await?;
                        service.state.lock().await.tables.insert(identity, table);
                        continue;
                    }
                }
                service
                    .apply_sql(&sql)
                    .await
                    .with_context(|| format!("seed {}", file.display()))?;
            }
        }
        Ok(service)
    }

    /// Returns the immutable in-memory store used by local schema validators.
    #[must_use]
    pub fn store(&self) -> Arc<MemoryCatalogStore> {
        Arc::clone(&self.store)
    }

    async fn query(
        &self,
        sql: &str,
        expected: Option<rpc::EventPosition>,
    ) -> Result<(arrow_array::RecordBatch, rpc::EventPosition)> {
        let statement = self.parser.parse_statement(sql)?;
        let state = self.state.lock().await;
        if expected.is_some_and(|expected| expected != position(&state)) {
            bail!("catalog changed since query preparation; prepare a fresh Flight SQL query");
        }
        let batch = match &statement {
            SparkStatement::ShowTables { namespace, pattern } => {
                let mut rows = Vec::new();
                for (identity, table) in &state.tables {
                    if let Some(namespace) = namespace {
                        let matches = match namespace.as_slice() {
                            [database] => identity.schema().as_str() == database,
                            [catalog, database] => {
                                identity.catalog().as_str() == catalog && identity.schema().as_str() == database
                            }
                            _ => bail!("SHOW TABLES namespace must be database or catalog.database"),
                        };
                        if !matches {
                            continue;
                        }
                    }
                    if pattern
                        .as_ref()
                        .is_some_and(|pattern| !matches_pattern(pattern, identity.table().as_str()))
                    {
                        continue;
                    }
                    let head = table.history.last().context("empty catalog history")?;
                    rows.push(vec![
                        head.identity().schema().to_string(),
                        identity.table().to_string(),
                        "false".to_owned(),
                    ]);
                }
                string_batch(&["namespace", "tableName", "isTemporary"], rows)
            }
            SparkStatement::DescribeTable { .. } => {
                let identity = statement_identity(&statement)?;
                let head = state
                    .tables
                    .get(&identity)
                    .and_then(|table| table.history.last())
                    .context(format!("table {identity} not found"))?;
                let rows = head
                    .fields()
                    .iter()
                    .map(|field| {
                        Ok(vec![
                            field.name().to_owned(),
                            field.mapping().spark()?.to_owned(),
                            field.comment().unwrap_or_default().to_owned(),
                        ])
                    })
                    .collect::<Result<Vec<_>>>()?;
                string_batch(&["col_name", "data_type", "comment"], rows)
            }
            SparkStatement::ShowTableProperties { key, .. } => {
                let identity = statement_identity(&statement)?;
                let head = state
                    .tables
                    .get(&identity)
                    .and_then(|table| table.history.last())
                    .context(format!("table {identity} not found"))?;
                let rows = if let Some(key) = key {
                    vec![vec![
                        key.clone(),
                        head.metadata()
                            .properties
                            .get(key)
                            .context(format!("table property {key} not found"))?
                            .clone(),
                    ]]
                } else {
                    head.metadata()
                        .properties
                        .iter()
                        .map(|(key, value)| vec![key.clone(), value.clone()])
                        .collect()
                };
                string_batch(&["key", "value"], rows)
            }
            _ => bail!("catalog statement queries support only SHOW TABLES, SHOW TBLPROPERTIES and DESCRIBE TABLE"),
        }?;
        Ok((batch, position(&state)))
    }

    fn default_location(&self, identity: &TableIdentity) -> String {
        format!(
            "{}/{}/{}/{}",
            self.config.warehouse_root.trim_end_matches('/'),
            identity.catalog(),
            identity.schema(),
            identity.table()
        )
    }

    fn persist_index(&self, identity: &TableIdentity, location: &str) -> Result<()> {
        write_durable(
            &self
                .config
                .state_dir
                .join(format!("{}.json", deterministic_table_id(identity))),
            &serde_json::to_vec(&IndexEntry {
                identity: identity.clone(),
                location: location.to_owned(),
            })?,
        )
    }

    fn recover_table(
        &self,
        identity: TableIdentity,
        location: String,
        snapshot: CatalogDeltaSnapshot,
    ) -> Result<Table> {
        let history: Vec<CatalogSchemaVersion> = if let Some(history) = snapshot.properties.get(HISTORY_KEY) {
            serde_json::from_str(history).context("invalid committed catalog history")?
        } else {
            let sql = import_sql(&identity, &snapshot)?;
            let DdlCommand::CreateTable(create) = self.parser.parse(&sql)? else {
                bail!("Delta schema import did not produce CREATE");
            };
            let mut metadata = create.metadata().clone();
            metadata.location = Some(location.clone());
            metadata.properties = public_properties(&snapshot.properties);
            metadata.comment = snapshot.comment.clone();
            vec![
                CatalogSchemaVersion::new(
                    deterministic_table_id(&identity),
                    identity.clone(),
                    SchemaVersion::FIRST,
                    create.fields().to_vec(),
                    create.canonical_ddl(),
                )?
                .with_metadata(metadata)?,
            ]
        };
        let head = history.last().context("empty committed catalog history")?;
        if head.identity() != &identity {
            bail!("committed history identity disagrees with table index");
        }
        let schema = arrow_deltalake::normalize::schema(self.storage_schema(&history)?.as_ref())?;
        if *schema != *snapshot.schema
            || self.storage_partitions(head)? != snapshot.partition_columns
            || head.metadata().properties != public_properties(&snapshot.properties)
            || head.metadata().comment != snapshot.comment
        {
            bail!("committed catalog history does not match Delta schema/properties/comment");
        }
        Ok(Table {
            history,
            location,
            delta_version: snapshot.version,
        })
    }

    fn storage_schema(&self, history: &[CatalogSchemaVersion]) -> Result<Arc<arrow_schema::Schema>> {
        let head = history.last().context("empty catalog history")?;
        let schema = head.arrow_schema()?;
        if !self.config.ingestion_layout {
            return Ok(Arc::new(schema));
        }
        if schema.field_with_name(INGESTION).is_ok() {
            bail!("ingestion partition is reserved for the DFE writer");
        }
        let mut fields = schema.fields().to_vec();
        // Keep the original physical prefix intact when nullable logical fields are appended.
        let initial_fields = history.first().context("empty catalog history")?.fields().len();
        fields.insert(
            initial_fields,
            Arc::new(arrow_schema::Field::new(INGESTION, arrow_schema::DataType::Utf8, false)),
        );
        Ok(Arc::new(arrow_schema::Schema::new(fields)))
    }

    fn storage_partitions(&self, head: &CatalogSchemaVersion) -> Result<Vec<String>> {
        if self.config.ingestion_layout {
            if !head.metadata().partition_columns.is_empty() {
                bail!("ingestion-layout catalogs do not accept user partition columns");
            }
            Ok(vec![INGESTION.to_owned()])
        } else {
            Ok(head.metadata().partition_columns.clone())
        }
    }

    async fn apply_sql(&self, sql: &str) -> Result<Option<DdlCompletion>> {
        let statement = self.parser.parse_statement(sql)?;
        let _ddl = self.ddl_lock.lock().await;
        if let SparkStatement::CreateNamespace(namespace) = &statement {
            if namespace.name.len() > 2 || namespace.name.is_empty() {
                bail!("namespace must contain one database or catalog.database");
            }
            for part in &namespace.name {
                let _: arrowcol_protocol::SchemaName = part.parse()?;
            }
            let key = namespace.name.join(".");
            let mut state = self.state.lock().await;
            if state.namespaces.contains_key(&key) {
                if namespace.if_not_exists {
                    return Ok(None);
                }
                bail!("namespace {key} already exists");
            }
            let mut namespaces = state.namespaces.clone();
            namespaces.insert(key, namespace.clone());
            write_durable(
                &self.config.state_dir.join("namespaces.json"),
                &serde_json::to_vec(&namespaces)?,
            )?;
            state.namespaces = namespaces;
            return Ok(None);
        }
        let identity = statement_identity(&statement)?;
        for _ in 0..5 {
            let current = self.state.lock().await.tables.get(&identity).cloned();
            let mut bootstrap_version = None;
            let (mut history, location, kind, disposition, dropped) = match &statement {
                SparkStatement::CreateTable(create_ast) => {
                    reject_internal(create_ast.metadata.properties.keys())?;
                    arrowcol_props::validate_table_properties(&create_ast.metadata.properties)?;
                    if let Some(current) = &current {
                        let schema = current.history.last().context("empty catalog history")?;
                        let DdlCommand::CreateTable(create) = self.parser.parse(sql)? else {
                            bail!("invalid CREATE");
                        };
                        let mut metadata = create.metadata().clone();
                        metadata.location = Some(
                            metadata
                                .location
                                .clone()
                                .unwrap_or_else(|| self.default_location(&identity)),
                        );
                        if create_ast.if_not_exists
                            || (create.fields() == schema.fields() && metadata == *schema.metadata())
                        {
                            return Ok(Some(DdlCompletion::from_committed(
                                schema,
                                DdlCompletionDisposition::AlreadyApplied,
                            )));
                        }
                        bail!("table {identity} already exists");
                    }
                    let DdlCommand::CreateTable(create) = self.parser.parse(sql)? else {
                        bail!("invalid CREATE");
                    };
                    let mut metadata = create.metadata().clone();
                    let location = metadata
                        .location
                        .clone()
                        .unwrap_or_else(|| self.default_location(&identity));
                    metadata.location = Some(location.clone());
                    let mut schema = CatalogSchemaVersion::new(
                        deterministic_table_id(&identity),
                        identity.clone(),
                        SchemaVersion::FIRST,
                        create.fields().to_vec(),
                        create.canonical_ddl(),
                    )?
                    .with_metadata(metadata)?;
                    if self.config.ingestion_layout
                        && let Some(snapshot) = read_delta(&location).await?
                    {
                        if snapshot.properties.contains_key(HISTORY_KEY) {
                            let recovered = self.recover_table(identity.clone(), location, snapshot)?;
                            self.store.hydrate_committed(recovered.history.clone()).await?;
                            self.state.lock().await.tables.insert(identity.clone(), recovered);
                            continue;
                        }
                        if snapshot.properties.contains_key(DROPPED_KEY)
                            || *arrow_deltalake::normalize::schema(
                                self.storage_schema(std::slice::from_ref(&schema))?.as_ref(),
                            )? != *snapshot.schema
                            || self.storage_partitions(&schema)? != snapshot.partition_columns
                        {
                            bail!("bootstrap table does not match the requested logical ingress schema and layout");
                        }
                        let mut metadata = schema.metadata().clone();
                        metadata.properties = public_properties(&snapshot.properties);
                        metadata.comment = snapshot.comment;
                        schema = schema.with_metadata(metadata)?;
                        bootstrap_version = Some(snapshot.version);
                    }
                    (
                        vec![schema],
                        location,
                        rpc::EventType::CreateTable,
                        DdlCompletionDisposition::Created,
                        false,
                    )
                }
                SparkStatement::AddTableColumns { .. } => {
                    let current = current.as_ref().context(format!("table {identity} not found"))?;
                    let head = current.history.last().context("empty catalog history")?;
                    let DdlCommand::AddColumns(add) = self.parser.parse(sql)? else {
                        bail!("invalid ADD");
                    };
                    if add.fields().iter().all(|addition| {
                        head.fields().iter().any(|field| {
                            field.name() == addition.name()
                                && field.mapping() == addition.mapping()
                                && field.is_nullable() == addition.is_nullable()
                                && field.comment() == addition.comment()
                        })
                    }) {
                        return Ok(Some(DdlCompletion::from_committed(
                            head,
                            DdlCompletionDisposition::AlreadyApplied,
                        )));
                    }
                    let mut fields = head.fields().to_vec();
                    let mut position = head.next_position()?;
                    let mut number = head.next_field_number()?;
                    for field in add.fields() {
                        if fields.iter().any(|existing| existing.name() == field.name()) {
                            bail!(
                                "field {} already exists; additions cannot redefine fields",
                                field.name()
                            );
                        }
                        if !field.is_nullable() {
                            bail!("new column {} must be nullable", field.name());
                        }
                        fields.push(field.materialize(position, number)?);
                        position = position.checked_add(1).context("field position exhausted")?;
                        number = next_protobuf_field_number(number)?;
                    }
                    let target = CatalogSchemaVersion::new(
                        head.table_id(),
                        identity.clone(),
                        head.version().next()?,
                        fields,
                        add.canonical_ddl(),
                    )?
                    .with_metadata(head.metadata().clone())?;
                    let mut history = current.history.clone();
                    history.push(target);
                    (
                        history,
                        current.location.clone(),
                        rpc::EventType::AlterTable,
                        DdlCompletionDisposition::Altered,
                        false,
                    )
                }
                SparkStatement::SetTableProperties { properties, .. } => {
                    reject_internal(properties.keys())?;
                    let current = current.as_ref().context(format!("table {identity} not found"))?;
                    let mut history = current.history.clone();
                    let head = history.last().context("empty catalog history")?;
                    let mut metadata = head.metadata().clone();
                    metadata.properties.extend(properties.clone());
                    arrowcol_props::validate_table_properties(&metadata.properties)?;
                    if metadata == *head.metadata() {
                        return Ok(Some(DdlCompletion::from_committed(
                            head,
                            DdlCompletionDisposition::AlreadyApplied,
                        )));
                    }
                    history.push(
                        CatalogSchemaVersion::new(
                            head.table_id(),
                            identity.clone(),
                            head.version().next()?,
                            head.fields().to_vec(),
                            sql,
                        )?
                        .with_metadata(metadata)?,
                    );
                    (
                        history,
                        current.location.clone(),
                        rpc::EventType::SetProps,
                        DdlCompletionDisposition::Altered,
                        false,
                    )
                }
                SparkStatement::UnsetTableProperties { keys, if_exists, .. } => {
                    reject_internal(keys.iter())?;
                    let current = current.as_ref().context(format!("table {identity} not found"))?;
                    let mut history = current.history.clone();
                    let head = history.last().context("empty catalog history")?;
                    let mut metadata = head.metadata().clone();
                    for key in keys {
                        if metadata.properties.remove(key).is_none() && !if_exists {
                            bail!("table property {key} does not exist");
                        }
                    }
                    arrowcol_props::validate_table_properties(&metadata.properties)?;
                    if metadata == *head.metadata() {
                        return Ok(Some(DdlCompletion::from_committed(
                            head,
                            DdlCompletionDisposition::AlreadyApplied,
                        )));
                    }
                    history.push(
                        CatalogSchemaVersion::new(
                            head.table_id(),
                            identity.clone(),
                            head.version().next()?,
                            head.fields().to_vec(),
                            sql,
                        )?
                        .with_metadata(metadata)?,
                    );
                    (
                        history,
                        current.location.clone(),
                        rpc::EventType::SetProps,
                        DdlCompletionDisposition::Altered,
                        false,
                    )
                }
                SparkStatement::DropTable { if_exists, .. } => {
                    let Some(current) = &current else {
                        if *if_exists {
                            return Ok(None);
                        }
                        bail!("table {identity} not found");
                    };
                    (
                        current.history.clone(),
                        current.location.clone(),
                        rpc::EventType::DropTable,
                        DdlCompletionDisposition::Altered,
                        true,
                    )
                }
                _ => bail!("use Flight SQL statement query for SHOW/DESCRIBE"),
            };
            let head = history.last().context("empty proposed history")?;
            let schema = self.storage_schema(&history)?;
            let mut properties = head.metadata().properties.clone();
            properties.insert(HISTORY_KEY.to_owned(), serde_json::to_string(&history)?);
            if dropped {
                properties.insert(DROPPED_KEY.to_owned(), "true".to_owned());
            }
            let partitions = self.storage_partitions(head)?;
            let comment = head.metadata().comment.clone();
            self.persist_index(&identity, &location)?;
            let location_for_commit = location.clone();
            let expected = current.as_ref().map(|table| table.delta_version).or(bootstrap_version);
            let committed_schema = arrow_deltalake::normalize::schema(&schema)?;
            let committed_properties = properties.clone();
            let committed_partitions = partitions.clone();
            let committed_comment = comment.clone();
            let result = tokio::task::spawn_blocking(move || {
                DeltaTable::open_catalog(storage_url(&location_for_commit)?)?
                    .commit_catalog(expected, schema, partitions, properties, comment)
            })
            .await
            .context("catalog Delta task failed")?;
            let snapshot = match result {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    // An unknown-success commit must be loaded, not blindly replayed.
                    let observed = read_delta(&location).await?;
                    if let Some(observed) = observed {
                        if observed.schema == committed_schema
                            && observed.properties == committed_properties
                            && observed.partition_columns == committed_partitions
                            && observed.comment == committed_comment
                        {
                            observed
                        } else if Some(observed.version) != expected {
                            if observed
                                .properties
                                .get(DROPPED_KEY)
                                .is_some_and(|value| value == "true")
                            {
                                self.store.remove_committed(&identity).await?;
                                self.state.lock().await.tables.remove(&identity);
                                return Err(error.context("table dropped by concurrent Delta writer"));
                            }
                            let recovered = self.recover_table(identity.clone(), location, observed)?;
                            self.store.hydrate_committed(recovered.history.clone()).await?;
                            self.state.lock().await.tables.insert(identity.clone(), recovered);
                            continue;
                        } else {
                            return Err(error);
                        }
                    } else {
                        return Err(error);
                    }
                }
            };
            #[cfg(test)]
            if self.fail_after_commit.swap(false, std::sync::atomic::Ordering::SeqCst) {
                bail!("injected failure after Delta commit, before catalog/event publication");
            }
            let delta_version = snapshot.version;
            if !dropped {
                let recovered = self.recover_table(identity.clone(), location.clone(), snapshot)?;
                history = recovered.history;
                self.store.hydrate_committed(history.clone()).await?;
            } else {
                self.store.remove_committed(&identity).await?;
            }
            let head = history.last().context("empty committed history")?;
            let completion = DdlCompletion::from_committed(head, disposition);
            let mut state = self.state.lock().await;
            if current
                .as_ref()
                .is_some_and(|table| table.delta_version == delta_version)
            {
                return Ok(Some(DdlCompletion::from_committed(
                    head,
                    DdlCompletionDisposition::AlreadyApplied,
                )));
            }
            state.event_id = state.event_id.checked_add(1).context("catalog event ID exhausted")?;
            let event = rpc::ChangeEvent {
                position: Some(position(&state)),
                event_type: kind as i32,
                table: Some(table_request(&identity)),
                schema_version: head.version().get(),
            };
            if dropped {
                state.tables.remove(&identity);
            } else {
                state.tables.insert(
                    identity,
                    Table {
                        history,
                        location,
                        delta_version,
                    },
                );
            }
            state.events.push_back(event.clone());
            while state.events.len() > self.config.retained_events {
                state.events.pop_front();
            }
            // No listener is not an error; replay is retained under the same lock.
            let _ = self.events.send(event);
            return Ok(Some(completion));
        }
        bail!("catalog changed repeatedly; retry DDL after concurrent writers settle")
    }
}

#[async_trait]
impl FlightSqlDdlHandler for CatalogService {
    async fn execute_ddl(&self, request: FlightSqlDdlRequest) -> CatalogResult<DdlCompletion> {
        self.apply_sql(request.command())
            .await
            .map_err(|error| CatalogError::new(CatalogErrorCode::ReconciliationFailed, format!("{error:#}")))?
            .ok_or_else(|| {
                CatalogError::new(
                    CatalogErrorCode::DdlUnsupportedStatement,
                    "statement has no table schema completion",
                )
            })
    }
}

impl CatalogService {
    pub(crate) async fn apply_statement_update<S>(
        &self,
        query: CommandStatementUpdate,
        request: Request<S>,
    ) -> Result<Option<DdlCompletion>, Status>
    where
        S: Stream<Item = Result<FlightData, Status>> + Send + Unpin,
    {
        if query.transaction_id.is_some() {
            return Err(Status::invalid_argument(
                "catalog DDL does not support Flight SQL transactions",
            ));
        }
        let mut stream = request.into_inner();
        let consume = async {
            let mut frames = 0;
            while let Some(frame) = stream.next().await {
                let frame = frame?;
                frames += 1;
                if frames > 1 || !frame.data_header.is_empty() || !frame.data_body.is_empty() {
                    return Err(Status::invalid_argument(
                        "catalog DDL must contain one descriptor and no Arrow body",
                    ));
                }
            }
            Ok::<_, Status>(())
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), consume)
            .await
            .map_err(|_| Status::deadline_exceeded("catalog DDL input did not close"))??;
        self.apply_sql(&query.query)
            .await
            .map_err(|error| Status::failed_precondition(format!("{error:#}")))
    }
}

#[async_trait]
impl FlightSqlService for CatalogService {
    type FlightService = Self;

    async fn do_put_statement_update(
        &self,
        query: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        self.apply_statement_update(query, request).await?;
        Ok(0)
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        if query.transaction_id.is_some() {
            return Err(Status::invalid_argument("catalog queries do not support transactions"));
        }
        let (batch, position) = self
            .query(&query.query, None)
            .await
            .map_err(|error| Status::failed_precondition(format!("{error:#}")))?;
        let ticket = TicketStatementQuery {
            statement_handle: serde_json::to_vec(&QueryTicket {
                query: query.query,
                epoch: position.epoch,
                event_id: position.event_id,
            })
            .map_err(|error| Status::internal(error.to_string()))?
            .into(),
        };
        let info = FlightInfo::new()
            .try_with_schema(batch.schema().as_ref())
            .map_err(|error| Status::internal(error.to_string()))?
            .with_descriptor(request.into_inner())
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket.as_any().encode_to_vec())))
            .with_total_records(
                i64::try_from(batch.num_rows()).map_err(|_| Status::internal("catalog query row count overflow"))?,
            );
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _: Request<Ticket>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status> {
        let ticket: QueryTicket = serde_json::from_slice(&ticket.statement_handle)
            .map_err(|_| Status::invalid_argument("invalid catalog query ticket"))?;
        let (batch, _) = self
            .query(
                &ticket.query,
                Some(rpc::EventPosition {
                    epoch: ticket.epoch,
                    event_id: ticket.event_id,
                }),
            )
            .await
            .map_err(|error| Status::failed_precondition(format!("{error:#}")))?;
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(batch.schema())
            .build(futures::stream::iter(vec![Ok(batch)]))
            .map(|frame| frame.map_err(|error| Status::internal(error.to_string())));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn register_sql_info(&self, _: i32, _: &SqlInfo) {}
}

type EventStream = Pin<Box<dyn Stream<Item = Result<rpc::ChangeEvent, Status>> + Send + 'static>>;

#[async_trait]
impl rpc::schema_catalog_server::SchemaCatalog for CatalogService {
    type WatchStream = EventStream;

    async fn get_current_event_id(&self, _: Request<rpc::Empty>) -> Result<Response<rpc::EventPosition>, Status> {
        Ok(Response::new(position(&*self.state.lock().await)))
    }
    async fn get_events(&self, request: Request<rpc::EventsRequest>) -> Result<Response<rpc::EventsResponse>, Status> {
        let state = self.state.lock().await;
        let from = checked_from(&state, request.into_inner().from)?;
        Ok(Response::new(rpc::EventsResponse {
            position: Some(position(&state)),
            events: state
                .events
                .iter()
                .filter(|event| event.position.as_ref().is_some_and(|position| position.event_id > from))
                .cloned()
                .collect(),
        }))
    }
    async fn watch(&self, request: Request<rpc::EventsRequest>) -> Result<Response<Self::WatchStream>, Status> {
        let state = self.state.lock().await;
        let mut shutdown = self.shutdown.subscribe();
        if *shutdown.borrow() {
            return Err(Status::unavailable("catalog is shutting down"));
        }
        let from = checked_from(&state, request.into_inner().from)?;
        let replay: Vec<_> = state
            .events
            .iter()
            .filter(|event| event.position.as_ref().is_some_and(|position| position.event_id > from))
            .cloned()
            .collect();
        let mut subscription = self.events.subscribe();
        drop(state);
        let (sender, receiver) = mpsc::channel(32);
        tokio::spawn(async move {
            for event in replay {
                if sender.send(Ok(event)).await.is_err() {
                    return;
                }
            }
            loop {
                tokio::select! {
                    _ = sender.closed() => return,
                    event = subscription.recv() => match event {
                        Ok(event) => if sender.send(Ok(event)).await.is_err() { return; },
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            let _ = sender.send(Err(Status::out_of_range("CES retained-event gap; fetch a fresh snapshot"))).await;
                            return;
                        }
                        Err(broadcast::error::RecvError::Closed) => return,
                    }
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(receiver).take_until(async move {
            if !*shutdown.borrow_and_update() {
                let _ = shutdown.changed().await;
            }
        });
        Ok(Response::new(Box::pin(stream)))
    }
    async fn get_table(&self, request: Request<rpc::TableRequest>) -> Result<Response<rpc::TableSnapshot>, Status> {
        let identity = identity_from_request(&request.into_inner())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let state = self.state.lock().await;
        let table = state
            .tables
            .get(&identity)
            .ok_or_else(|| Status::not_found(format!("catalog table {identity} does not exist")))?;
        Ok(Response::new(table_snapshot(table, &position(&state))?))
    }
    async fn get_snapshot(&self, _: Request<rpc::Empty>) -> Result<Response<rpc::CatalogSnapshot>, Status> {
        let state = self.state.lock().await;
        let position = position(&state);
        let tables = state
            .tables
            .values()
            .map(|table| table_snapshot(table, &position))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(rpc::CatalogSnapshot {
            position: Some(position),
            tables,
        }))
    }
}

/// CES endpoint that requires signed identity metadata for every read.
#[derive(Clone)]
pub struct CatalogCesService {
    inner: CatalogService,
    identity_policy: crate::DdlIdentityPolicy,
}

impl CatalogCesService {
    /// Creates an authenticated CES service over the catalog state.
    #[must_use]
    pub fn new(inner: CatalogService, identity_policy: crate::DdlIdentityPolicy) -> Self {
        Self { inner, identity_policy }
    }

    fn authorize<T: Message>(&self, request: Request<T>, method: &str) -> Result<Request<T>, Status> {
        self.identity_policy.verify_request(
            request.metadata(),
            method,
            &request.get_ref().encode_to_vec(),
            crate::identity::now_unix_millis(),
        )?;
        Ok(request)
    }
}

#[async_trait]
impl rpc::schema_catalog_server::SchemaCatalog for CatalogCesService {
    type WatchStream = EventStream;

    async fn get_current_event_id(&self, request: Request<rpc::Empty>) -> Result<Response<rpc::EventPosition>, Status> {
        let request = self.authorize(request, "/arrowcol.catalog.v1.SchemaCatalog/GetCurrentEventId")?;
        rpc::schema_catalog_server::SchemaCatalog::get_current_event_id(&self.inner, request).await
    }

    async fn get_events(&self, request: Request<rpc::EventsRequest>) -> Result<Response<rpc::EventsResponse>, Status> {
        let request = self.authorize(request, "/arrowcol.catalog.v1.SchemaCatalog/GetEvents")?;
        rpc::schema_catalog_server::SchemaCatalog::get_events(&self.inner, request).await
    }

    async fn watch(&self, request: Request<rpc::EventsRequest>) -> Result<Response<Self::WatchStream>, Status> {
        let request = self.authorize(request, "/arrowcol.catalog.v1.SchemaCatalog/Watch")?;
        rpc::schema_catalog_server::SchemaCatalog::watch(&self.inner, request).await
    }

    async fn get_table(&self, request: Request<rpc::TableRequest>) -> Result<Response<rpc::TableSnapshot>, Status> {
        let request = self.authorize(request, "/arrowcol.catalog.v1.SchemaCatalog/GetTable")?;
        rpc::schema_catalog_server::SchemaCatalog::get_table(&self.inner, request).await
    }

    async fn get_snapshot(&self, request: Request<rpc::Empty>) -> Result<Response<rpc::CatalogSnapshot>, Status> {
        let request = self.authorize(request, "/arrowcol.catalog.v1.SchemaCatalog/GetSnapshot")?;
        rpc::schema_catalog_server::SchemaCatalog::get_snapshot(&self.inner, request).await
    }
}

fn checked_from(state: &State, from: Option<rpc::EventPosition>) -> Result<u64, Status> {
    let from = from.ok_or_else(|| Status::invalid_argument("CES requires an epoch/event_id snapshot position"))?;
    let oldest = state
        .events
        .front()
        .and_then(|event| event.position.as_ref())
        .map_or(state.event_id, |position| position.event_id.saturating_sub(1));
    if from.epoch != state.epoch || from.event_id < oldest || from.event_id > state.event_id {
        return Err(Status::out_of_range(
            "CES epoch changed or retained-event gap; fetch a fresh snapshot",
        ));
    }
    Ok(from.event_id)
}

fn position(state: &State) -> rpc::EventPosition {
    rpc::EventPosition {
        epoch: state.epoch.clone(),
        event_id: state.event_id,
    }
}

fn table_snapshot(table: &Table, position: &rpc::EventPosition) -> Result<rpc::TableSnapshot, Status> {
    let head = table
        .history
        .last()
        .ok_or_else(|| Status::internal("empty catalog history"))?;
    let artifacts = generate_descriptor_artifacts(head).map_err(|error| Status::internal(error.to_string()))?;
    Ok(rpc::TableSnapshot {
        table: Some(table_request(head.identity())),
        position: Some(position.clone()),
        schema_history_json: serde_json::to_vec(&table.history).map_err(|error| Status::internal(error.to_string()))?,
        provider: head.metadata().provider.clone(),
        location: table.location.clone(),
        partition_columns: head.metadata().partition_columns.clone(),
        properties: head
            .metadata()
            .properties
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        comment: head.metadata().comment.clone().unwrap_or_default(),
        arrow_schema_ipc: artifacts.arrow_schema().to_vec(),
        manifest: artifacts.manifest().to_vec(),
        protobuf_descriptor: artifacts.protobuf_descriptor().to_vec(),
    })
}

fn name_identity(name: &[String]) -> Result<TableIdentity> {
    let (catalog, database, table) = match name {
        [table] => ("arrowcol", "default", table.as_str()),
        [database, table] => ("arrowcol", database.as_str(), table.as_str()),
        [catalog, database, table] => (catalog.as_str(), database.as_str(), table.as_str()),
        _ => bail!("table name must have one, two or three components"),
    };
    Ok(TableIdentity::new(catalog.parse()?, database.parse()?, table.parse()?))
}

fn statement_identity(statement: &SparkStatement) -> Result<TableIdentity> {
    let name = match statement {
        SparkStatement::CreateTable(create) => &create.name,
        SparkStatement::AddTableColumns { name, .. }
        | SparkStatement::SetTableProperties { name, .. }
        | SparkStatement::UnsetTableProperties { name, .. }
        | SparkStatement::DropTable { name, .. }
        | SparkStatement::ShowTableProperties { name, .. }
        | SparkStatement::DescribeTable { name } => name,
        _ => bail!("statement does not target a table"),
    };
    name_identity(name)
}

fn reject_internal<'a>(keys: impl IntoIterator<Item = &'a String>) -> Result<()> {
    for key in keys {
        if key == HISTORY_KEY || key == DROPPED_KEY {
            bail!("{key} is reserved internal catalog state");
        }
    }
    Ok(())
}

fn public_properties(properties: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    properties
        .iter()
        .filter(|(key, _)| key.as_str() != HISTORY_KEY && key.as_str() != DROPPED_KEY)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn storage_url(location: &str) -> Result<Url> {
    if location.contains("://") {
        Ok(Url::parse(location)?)
    } else {
        std::fs::create_dir_all(location)?;
        Url::from_directory_path(std::fs::canonicalize(location)?)
            .map_err(|()| anyhow::anyhow!("invalid catalog table path"))
    }
}

async fn read_delta(location: &str) -> Result<Option<CatalogDeltaSnapshot>> {
    let location = location.to_owned();
    tokio::task::spawn_blocking(move || DeltaTable::open_catalog(storage_url(&location)?)?.catalog_snapshot())
        .await
        .context("catalog read task failed")?
}

fn warehouse_file_path(warehouse: &str) -> Result<PathBuf> {
    if warehouse.contains("://") {
        Url::parse(warehouse)?
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("warehouse is not local"))
    } else {
        Ok(PathBuf::from(warehouse))
    }
}

fn discover_tables(root: &Path, path: &Path, indexes: &mut Vec<IndexEntry>) -> Result<()> {
    if path.join("_delta_log").is_dir() {
        let parts = path
            .strip_prefix(root)?
            .iter()
            .map(|part| part.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        if parts.len() == 3 {
            indexes.push(IndexEntry {
                identity: name_identity(&parts)?,
                location: path.to_string_lossy().into_owned(),
            });
        }
        return Ok(());
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            discover_tables(root, &entry.path(), indexes)?;
        }
    }
    Ok(())
}

fn write_durable(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    std::fs::File::open(path.parent().context("index has no parent")?)?.sync_all()?;
    Ok(())
}

fn import_sql(identity: &TableIdentity, snapshot: &CatalogDeltaSnapshot) -> Result<String> {
    let mut columns = Vec::new();
    for field in snapshot.schema.fields() {
        let name = arrowcol_sparksql::quote_identifier(field.name());
        let data_type = import_type(field.data_type())?;
        columns.push(format!(
            "{name} {data_type}{}",
            if field.is_nullable() { "" } else { " NOT NULL" }
        ));
    }
    let mut sql = format!("CREATE TABLE {} ({}) USING DELTA", identity, columns.join(", "));
    if !snapshot.partition_columns.is_empty() {
        sql.push_str(&format!(
            " PARTITIONED BY ({})",
            snapshot
                .partition_columns
                .iter()
                .map(|name| arrowcol_sparksql::quote_identifier(name))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(sql)
}

fn import_type(data_type: &arrow_schema::DataType) -> Result<String> {
    use arrow_schema::{DataType, TimeUnit};
    Ok(match data_type {
        DataType::Boolean => "BOOLEAN".to_owned(),
        DataType::Int8 => "TINYINT".to_owned(),
        DataType::Int16 => "SMALLINT".to_owned(),
        DataType::Int32 => "INT".to_owned(),
        DataType::Int64 => "BIGINT".to_owned(),
        DataType::Float32 => "FLOAT".to_owned(),
        DataType::Float64 => "DOUBLE".to_owned(),
        DataType::Utf8 => "STRING".to_owned(),
        DataType::Binary => "BINARY".to_owned(),
        DataType::Date32 => "DATE".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP_NTZ".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == "UTC" => "TIMESTAMP".to_owned(),
        DataType::Decimal128(precision, scale) if *scale >= 0 => format!("DECIMAL({precision},{scale})"),
        DataType::Struct(fields) => format!(
            "STRUCT<{}>",
            fields
                .iter()
                .map(|field| Ok(format!(
                    "{}: {}{}",
                    arrowcol_sparksql::quote_identifier(field.name()),
                    import_type(field.data_type())?,
                    if field.is_nullable() { "" } else { " NOT NULL" }
                )))
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        ),
        DataType::List(field) if field.is_nullable() => format!("ARRAY<{}>", import_type(field.data_type())?),
        DataType::Map(field, false) => {
            let DataType::Struct(fields) = field.data_type() else {
                bail!("invalid Delta map");
            };
            if fields.len() != 2 || fields[0].is_nullable() || !fields[1].is_nullable() {
                bail!("unsupported Delta map nullability");
            }
            format!(
                "MAP<{}, {}>",
                import_type(fields[0].data_type())?,
                import_type(fields[1].data_type())?
            )
        }

        _ => bail!("existing Delta type {data_type:?} cannot be imported losslessly"),
    })
}

#[cfg(test)]
mod tests;

fn string_batch(names: &[&str], rows: Vec<Vec<String>>) -> Result<arrow_array::RecordBatch> {
    let schema = Arc::new(arrow_schema::Schema::new(
        names
            .iter()
            .map(|name| arrow_schema::Field::new(*name, arrow_schema::DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let columns = (0..names.len())
        .map(|column| {
            Arc::new(arrow_array::StringArray::from(
                rows.iter().map(|row| row[column].as_str()).collect::<Vec<_>>(),
            )) as arrow_array::ArrayRef
        })
        .collect();
    Ok(arrow_array::RecordBatch::try_new(schema, columns)?)
}

fn matches_pattern(pattern: &str, value: &str) -> bool {
    pattern.split('|').any(|pattern| {
        let pattern = pattern.to_lowercase();
        let value = value.to_lowercase();
        let parts: Vec<_> = pattern.split('*').collect();
        let mut offset = 0;
        for (index, part) in parts.iter().enumerate() {
            let Some(found) = value[offset..].find(part) else {
                return false;
            };
            if index == 0 && !pattern.starts_with('*') && found != 0 {
                return false;
            }
            offset += found + part.len();
        }
        pattern.ends_with('*') || offset == value.len()
    })
}
