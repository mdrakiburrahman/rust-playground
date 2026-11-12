use std::{
    collections::BTreeMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use arrow_flight::{
    FlightData, FlightDescriptor,
    flight_descriptor::DescriptorType,
    flight_service_client::FlightServiceClient,
    sql::{CommandStatementUpdate, ProstMessageExt},
};
use arrowcol_catalog::{
    CatalogError, CatalogErrorCode, CatalogErrorOrigin, CatalogResult, CatalogSchemaVersion, CatalogStore,
    ChangeReservation, DdlCompletion, DdlCompletionDisposition, DdlParser, DescriptorArtifacts, FlightSqlDdlHandler,
    FlightSqlDdlRequest, RequestHash, ReserveOutcome, SagaId, SagaRecord, SagaStep, SchemaVersion,
};
use arrowcol_protocol::{SchemaFingerprint, TableIdentity};
use async_trait::async_trait;
use futures::stream;
use prost::Message;
use tokio::sync::Mutex;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

use crate::{CachedTable, CatalogClient, InternalRequestSigner, rpc};

const CES_CURRENT_PATH: &str = "/arrowcol.catalog.v1.SchemaCatalog/GetCurrentEventId";
const CES_WATCH_PATH: &str = "/arrowcol.catalog.v1.SchemaCatalog/Watch";
const CES_TABLE_PATH: &str = "/arrowcol.catalog.v1.SchemaCatalog/GetTable";
const CES_SNAPSHOT_PATH: &str = "/arrowcol.catalog.v1.SchemaCatalog/GetSnapshot";
const FLIGHT_DO_PUT_PATH: &str = "/arrow.flight.protocol.FlightService/DoPut";

type RpcClient = rpc::schema_catalog_client::SchemaCatalogClient<Channel>;

#[derive(Default)]
struct Cache {
    tables: BTreeMap<TableIdentity, CachedTable>,
    position: Option<rpc::EventPosition>,
    healthy_at: Option<Instant>,
    generation: u64,
}

/// Actual catalog calls and local cache operations, independent of latency.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheMetrics {
    /// Started RPCs, including failed calls and stream subscriptions.
    pub catalog_calls: u64,
    /// Local table lookups served without an RPC.
    pub hits: u64,
    /// Fetch-through table lookups.
    pub misses: u64,
    /// Tables invalidated by CES.
    pub invalidations: u64,
}

#[derive(Default)]
struct Counters {
    calls: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    invalidations: AtomicU64,
}

/// Remote catalog cache with replay, restart detection and bounded outage age.
pub struct RemoteCatalogClient {
    channel: Channel,
    rpc: RpcClient,
    cache: Mutex<Cache>,
    maximum_age: Duration,
    signer: InternalRequestSigner,
    counters: Counters,
}

impl RemoteCatalogClient {
    /// Connects, takes an atomic snapshot, and starts CES/reconcile in the background.
    ///
    /// # Errors
    /// Rejects an invalid endpoint, zero maximum age, unavailable catalog or corrupt snapshot.
    pub async fn connect(endpoint: &str, maximum_age: Duration) -> CatalogResult<Arc<Self>> {
        let secret_file = std::env::var_os("ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE")
            .or_else(|| std::env::var_os("LOADTEST_INTERNAL_SECRET_FILE"))
            .ok_or_else(|| failure("catalog internal identity secret file is not configured"))?;
        let signer = InternalRequestSigner::from_file(secret_file)
            .map_err(|error| failure(format!("load catalog internal identity signer: {error}")))?;
        Self::connect_with_signer(endpoint, maximum_age, signer).await
    }

    /// Connects with an explicitly provided internal request signer.
    ///
    /// # Errors
    /// Rejects an invalid endpoint, zero maximum age, unavailable catalog or corrupt snapshot.
    pub async fn connect_with_signer(
        endpoint: &str,
        maximum_age: Duration,
        signer: InternalRequestSigner,
    ) -> CatalogResult<Arc<Self>> {
        if maximum_age.is_zero() {
            return Err(failure("catalog maximum cache age must be positive"));
        }
        let channel = Endpoint::from_shared(endpoint.to_owned())
            .map_err(|error| failure(format!("invalid catalog endpoint: {error}")))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .connect()
            .await
            .map_err(|error| failure(format!("catalog connection failed: {error}")))?;
        let client = Arc::new(Self {
            rpc: RpcClient::new(channel.clone()),
            channel,
            cache: Mutex::new(Cache::default()),
            maximum_age,
            signer,
            counters: Counters::default(),
        });
        client.reconcile().await?;
        tokio::spawn(Self::watch_loop(Arc::downgrade(&client)));
        Ok(client)
    }

    /// Returns counters that count actual RPC starts, not estimated request latency.
    #[must_use]
    pub fn metrics(&self) -> CacheMetrics {
        CacheMetrics {
            catalog_calls: self.counters.calls.load(Ordering::Relaxed),
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            invalidations: self.counters.invalidations.load(Ordering::Relaxed),
        }
    }

    fn rpc_call(&self) -> RpcClient {
        self.counters.calls.fetch_add(1, Ordering::Relaxed);
        self.rpc.clone()
    }

    /// Replaces the cache from an atomic catalog snapshot and its CES watermark.
    ///
    /// # Errors
    /// Returns transport, epoch or invalid committed-history errors.
    pub async fn reconcile(&self) -> CatalogResult<()> {
        let generation = self.cache.lock().await.generation;
        let mut request = Request::new(rpc::Empty {});
        self.signer
            .sign_message(&mut request, CES_SNAPSHOT_PATH)
            .map_err(|error| failure(format!("catalog request signing failed: {error}")))?;
        let snapshot = self
            .rpc_call()
            .get_snapshot(request)
            .await
            .map_err(rpc_error)?
            .into_inner();
        let position = snapshot
            .position
            .ok_or_else(|| failure("catalog snapshot has no position"))?;
        if position.epoch.is_empty() {
            return Err(failure("catalog snapshot has no epoch"));
        }
        let mut tables = BTreeMap::new();
        for snapshot in snapshot.tables {
            let table = decode_table(snapshot)?;
            if table.position != position {
                return Err(failure("catalog snapshot tables have inconsistent watermarks"));
            }
            let identity = table.versions[0].identity().clone();
            if tables.insert(identity, table).is_some() {
                return Err(failure("catalog snapshot contains duplicate tables"));
            }
        }
        let mut cache = self.cache.lock().await;
        if cache.generation != generation {
            return Err(failure(
                "catalog changed during reconciliation; retry from a fresh snapshot",
            ));
        }
        cache.tables = tables;
        cache.position = Some(position);
        cache.healthy_at = Some(Instant::now());
        cache.generation = cache.generation.wrapping_add(1);
        Ok(())
    }

    async fn watch_loop(weak: Weak<Self>) {
        loop {
            let Some(client) = weak.upgrade() else {
                return;
            };
            if let Err(error) = client.watch_once().await {
                tracing::warn!(%error, "catalog CES disconnected; reconciling before reconnect");
                let mut cache = client.cache.lock().await;
                cache.tables.clear();
                cache.healthy_at = None;
                cache.generation = cache.generation.wrapping_add(1);
                drop(cache);
            }
            drop(client);
            tokio::time::sleep(Duration::from_secs(1)).await;
            let Some(client) = weak.upgrade() else {
                return;
            };
            if let Err(error) = client.reconcile().await {
                tracing::warn!(%error, "catalog reconciliation failed");
            }
        }
    }

    async fn watch_once(&self) -> CatalogResult<()> {
        let from = self.cache.lock().await.position.clone();
        let mut request = Request::new(rpc::EventsRequest { from });
        self.signer
            .sign_message(&mut request, CES_WATCH_PATH)
            .map_err(|error| failure(format!("catalog request signing failed: {error}")))?;
        let mut stream = self.rpc_call().watch(request).await.map_err(rpc_error)?.into_inner();
        let mut reconcile_tick = tokio::time::interval(Duration::from_secs(5));
        let lifetime = tokio::time::sleep(Duration::from_secs(30));
        tokio::pin!(lifetime);
        loop {
            tokio::select! {
                _ = &mut lifetime => return Ok(()),
                event = stream.message() => {
                    let event = event.map_err(rpc_error)?.ok_or_else(|| failure("catalog CES ended"))?;
                    let position = event.position.ok_or_else(|| failure("CES event has no position"))?;
                    let identity = identity_from_request(&event.table.ok_or_else(|| failure("CES event has no table"))?)?;
                    let mut cache = self.cache.lock().await;
                    let previous = cache.position.as_ref().ok_or_else(|| failure("CES cache has no position"))?;
                    if position.epoch != previous.epoch || position.event_id != previous.event_id.checked_add(1).ok_or_else(|| failure("CES event ID exhausted"))? {
                        return Err(failure("CES epoch changed or retained event gap detected"));
                    }
                    cache.tables.remove(&identity);
                    cache.position = Some(position);
                    cache.generation = cache.generation.wrapping_add(1);
                    self.counters.invalidations.fetch_add(1, Ordering::Relaxed);
                }
                _ = reconcile_tick.tick() => {
                    let mut request = Request::new(rpc::Empty {});
                    self.signer.sign_message(&mut request, CES_CURRENT_PATH).map_err(|error| failure(format!("catalog request signing failed: {error}")))?;
                    let remote = self.rpc_call().get_current_event_id(request).await.map_err(rpc_error)?.into_inner();
                    let mut cache = self.cache.lock().await;
                    let local = cache.position.as_ref().ok_or_else(|| failure("CES cache has no position"))?;
                    if remote.epoch != local.epoch || remote.event_id < local.event_id {
                        return Err(failure("catalog restart detected during reconciliation"));
                    }
                    if remote == *local {
                        cache.healthy_at = Some(Instant::now());
                    } else if cache.healthy_at.is_none_or(|time| time.elapsed() > self.maximum_age) {
                        return Err(failure("CES failed to catch up within maximum cache age"));
                    }
                }
            }
        }
    }
}

#[async_trait]
impl FlightSqlDdlHandler for RemoteCatalogClient {
    async fn execute_ddl(&self, request: FlightSqlDdlRequest) -> CatalogResult<DdlCompletion> {
        let command = DdlParser::default().parse(request.command())?;
        let identity = command.identity().clone();
        let command = CommandStatementUpdate {
            query: request.command().to_owned(),
            transaction_id: None,
        };
        let payload = command.as_any().encode_to_vec();
        let mut flight_request = Request::new(stream::iter([FlightData {
            flight_descriptor: Some(FlightDescriptor {
                r#type: DescriptorType::Cmd as i32,
                cmd: payload.clone().into(),
                path: Vec::new(),
            }),
            ..FlightData::default()
        }]));
        self.signer
            .sign_payload(&mut flight_request, FLIGHT_DO_PUT_PATH, &payload)
            .map_err(|error| failure(format!("catalog request signing failed: {error}")))?;
        self.counters.calls.fetch_add(1, Ordering::Relaxed);
        let response = FlightServiceClient::new(self.channel.clone())
            .do_put(flight_request)
            .await
            .map_err(|error| failure(format!("catalog Flight SQL failed: {error}")))?;
        let mut completion = response.into_inner();
        completion
            .message()
            .await
            .map_err(|error| failure(format!("catalog Flight SQL completion failed: {error}")))?;
        self.invalidate(&identity).await;
        let table = self.table(&identity).await?;
        let schema = table
            .versions
            .last()
            .ok_or_else(|| failure("DDL completed without a committed schema"))?;
        Ok(DdlCompletion::from_committed(
            schema,
            DdlCompletionDisposition::AlreadyApplied,
        ))
    }
}

/// Converts validated portable identities to the HMS-shaped RPC key.
#[must_use]
pub fn table_request(identity: &TableIdentity) -> rpc::TableRequest {
    rpc::TableRequest {
        catalog: identity.catalog().to_string(),
        database: identity.schema().to_string(),
        table: identity.table().to_string(),
    }
}

/// Validates an HMS-shaped RPC table identity.
///
/// # Errors
/// Rejects invalid portable catalog, database or table components.
pub fn identity_from_request(request: &rpc::TableRequest) -> CatalogResult<TableIdentity> {
    Ok(TableIdentity::new(
        request
            .catalog
            .parse()
            .map_err(|error| failure(format!("invalid catalog: {error}")))?,
        request
            .database
            .parse()
            .map_err(|error| failure(format!("invalid database: {error}")))?,
        request
            .table
            .parse()
            .map_err(|error| failure(format!("invalid table: {error}")))?,
    ))
}

/// Decodes and verifies a committed table history.
///
/// # Errors
/// Rejects mismatched identities, non-monotonic history and missing positions.
pub fn decode_table(snapshot: rpc::TableSnapshot) -> CatalogResult<CachedTable> {
    let position = snapshot
        .position
        .clone()
        .ok_or_else(|| failure("table snapshot has no position"))?;
    let identity = identity_from_request(
        snapshot
            .table
            .as_ref()
            .ok_or_else(|| failure("table snapshot has no identity"))?,
    )?;
    let versions: Vec<CatalogSchemaVersion> = serde_json::from_slice(&snapshot.schema_history_json)
        .map_err(|error| failure(format!("invalid schema history: {error}")))?;
    if versions.is_empty() || versions.iter().any(|schema| schema.identity() != &identity) {
        return Err(failure("table snapshot has empty or mismatched history"));
    }
    for pair in versions.windows(2) {
        if pair[1].version() <= pair[0].version() || !pair[1].is_nullable_append_only_superset_of(&pair[0]) {
            return Err(failure("catalog history is not monotonic nullable append-only"));
        }
    }
    Ok(CachedTable {
        position,
        snapshot,
        versions,
    })
}

#[async_trait]
impl CatalogClient for RemoteCatalogClient {
    async fn table(&self, identity: &TableIdentity) -> CatalogResult<CachedTable> {
        for _ in 0..3 {
            let generation = {
                let cache = self.cache.lock().await;
                if cache.healthy_at.is_some_and(|time| time.elapsed() <= self.maximum_age)
                    && let Some(table) = cache.tables.get(identity)
                {
                    self.counters.hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(table.clone());
                }
                cache.generation
            };
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            let mut request = Request::new(table_request(identity));
            self.signer
                .sign_message(&mut request, CES_TABLE_PATH)
                .map_err(|error| failure(format!("catalog request signing failed: {error}")))?;
            let response = self
                .rpc_call()
                .get_table(request)
                .await
                .map_err(rpc_error)?
                .into_inner();
            let table = decode_table(response)?;
            if table.versions[0].identity() != identity {
                return Err(failure("catalog returned a different table"));
            }
            let mut cache = self.cache.lock().await;
            if cache.generation != generation {
                continue;
            }
            if cache
                .position
                .as_ref()
                .is_some_and(|position| position.epoch != table.position.epoch)
            {
                cache.tables.clear();
                cache.healthy_at = None;
                return Err(failure("catalog restarted; reconcile before serving cached schemas"));
            }
            cache.tables.insert(identity.clone(), table.clone());
            // A fresh table RPC validates this table, not all older cached tables.
            return Ok(table);
        }
        Err(failure("catalog changed repeatedly during table fetch"))
    }

    async fn invalidate(&self, identity: &TableIdentity) {
        let mut cache = self.cache.lock().await;
        cache.tables.remove(identity);
        cache.generation = cache.generation.wrapping_add(1);
    }
}

fn failure(message: impl Into<String>) -> CatalogError {
    CatalogError::new(CatalogErrorCode::ReconciliationFailed, message).with_origin(CatalogErrorOrigin::RemoteClient)
}

fn rpc_error(status: tonic::Status) -> CatalogError {
    CatalogError::new(
        if status.code() == tonic::Code::NotFound {
            CatalogErrorCode::TableNotFound
        } else {
            CatalogErrorCode::ReconciliationFailed
        },
        format!("catalog RPC {}: {}", status.code(), status.message()),
    )
    .with_origin(CatalogErrorOrigin::RemoteGrpc)
}

fn read_only() -> CatalogError {
    CatalogError::new(
        CatalogErrorCode::DdlUnsupportedStatement,
        "remote ingestion cache is read-only; submit Spark SQL to the catalog Flight endpoint",
    )
}

#[async_trait]
impl CatalogStore for RemoteCatalogClient {
    async fn current_schema(&self, identity: &TableIdentity) -> CatalogResult<Option<CatalogSchemaVersion>> {
        Ok(self.table(identity).await?.versions.last().cloned())
    }
    async fn schema_by_fingerprint(
        &self,
        identity: &TableIdentity,
        fingerprint: SchemaFingerprint,
    ) -> CatalogResult<Option<CatalogSchemaVersion>> {
        Ok(self
            .table(identity)
            .await?
            .versions
            .into_iter()
            .find(|schema| schema.fingerprint() == fingerprint))
    }
    async fn schema_history(&self, identity: &TableIdentity) -> CatalogResult<Vec<CatalogSchemaVersion>> {
        Ok(self.table(identity).await?.versions)
    }
    async fn artifacts(
        &self,
        identity: &TableIdentity,
        version: SchemaVersion,
    ) -> CatalogResult<Option<DescriptorArtifacts>> {
        let table = self.table(identity).await?;
        let schema = table.versions.iter().find(|schema| schema.version() == version);
        schema.map(arrowcol_catalog::generate_descriptor_artifacts).transpose()
    }
    async fn operation(&self, _: &TableIdentity, _: RequestHash) -> CatalogResult<Option<SagaRecord>> {
        Err(read_only())
    }
    async fn reserve_change(&self, _: ChangeReservation) -> CatalogResult<ReserveOutcome> {
        Err(read_only())
    }
    async fn saga(&self, _: SagaId) -> CatalogResult<Option<SagaRecord>> {
        Err(read_only())
    }
    async fn persist_artifacts(&self, _: SagaId, _: &DescriptorArtifacts) -> CatalogResult<SagaRecord> {
        Err(read_only())
    }
    async fn finalize(&self, _: SagaId) -> CatalogResult<SagaRecord> {
        Err(read_only())
    }
    async fn record_failure(&self, _: SagaId, _: SagaStep, _: &CatalogError) -> CatalogResult<SagaRecord> {
        Err(read_only())
    }
    async fn incomplete_sagas(&self) -> CatalogResult<Vec<SagaRecord>> {
        Err(read_only())
    }
}

#[cfg(test)]
mod tests {
    use super::{failure, rpc_error};
    use arrowcol_catalog::{CatalogErrorCode, CatalogErrorOrigin};
    use tonic::Status;

    #[test]
    fn remote_catalog_errors_preserve_their_origin_and_code() {
        let client_error = failure("local client failure");
        assert_eq!(client_error.code(), CatalogErrorCode::ReconciliationFailed);
        assert_eq!(client_error.origin(), CatalogErrorOrigin::RemoteClient);

        let grpc_error = rpc_error(Status::internal("remote service failure"));
        assert_eq!(grpc_error.code(), CatalogErrorCode::ReconciliationFailed);
        assert_eq!(grpc_error.origin(), CatalogErrorOrigin::RemoteGrpc);

        let missing_table = rpc_error(Status::not_found("missing table"));
        assert_eq!(missing_table.code(), CatalogErrorCode::TableNotFound);
        assert_eq!(missing_table.origin(), CatalogErrorOrigin::RemoteGrpc);
    }
}
