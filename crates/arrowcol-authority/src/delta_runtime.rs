//! Authenticated opt-in shared Delta control and trusted publication services.
use crate::{
    delta_store::*,
    sql_publication::{NoHooks, Publisher},
    *,
};
use anyhow::{Context, Result, bail, ensure};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Mutex, Semaphore};

struct RuntimeHooks;
impl sql_publication::PublicationHooks for RuntimeHooks {
    fn at(&self, phase: &str) -> Result<()> {
        let Some(root) = std::env::var_os("ARROWCOL_DELTA_FAULTS") else {
            return Ok(());
        };
        let root = PathBuf::from(root);
        let marker = root.join(format!("{phase}.pause"));
        if marker.exists() {
            std::fs::write(root.join(format!("{phase}.reached")), phase)?;
            let deadline = std::time::Instant::now() + Duration::from_secs(90);
            while marker.exists() {
                ensure!(std::time::Instant::now() < deadline, "bounded runtime fault barrier");
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaConfig {
    pub publisher: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrivateConfig {
    pub backend: String,
    pub sql_host: String,
    pub sql_port: u16,
    pub sql_database: String,
    pub sql_user: String,
    pub sql_password_file: PathBuf,
    pub root: PathBuf,
    pub staging: PathBuf,
}

impl PrivateConfig {
    /// # Errors
    /// Rejects unsupported backend combinations and invalid private configuration.
    pub fn load(path: &str) -> Result<Self> {
        let value: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        ensure!(
            matches!(value.backend.as_str(), "sql" | "etcd"),
            "backend must be sql or etcd; no fallback"
        );
        Ok(value)
    }

    /// # Errors
    /// Returns unreadable credentials, not logged or sent to collectors.
    pub fn sql(&self) -> Result<arrowcol_sql_server::ConnectionConfig> {
        Ok(arrowcol_sql_server::ConnectionConfig::new(
            &self.sql_host,
            self.sql_port,
            &self.sql_database,
            &self.sql_user,
            std::fs::read_to_string(&self.sql_password_file)?.trim(),
        )
        .trust_server_certificate(true)
        .with_timeouts(Duration::from_secs(3), Duration::from_secs(8)))
    }

    /// # Errors
    /// Returns bootstrap/connectivity errors without switching authority.
    pub async fn store(&self, config: &Config) -> Result<Arc<dyn Store>> {
        match self.backend.as_str() {
            "sql" => Ok(Arc::new(delta_backends::SqlStore::bootstrap(self.sql()?).await?)),
            "etcd" => Ok(Arc::new(delta_backends::EtcdStore::new(config).await?)),
            _ => bail!("unsupported backend"),
        }
    }
}

pub type Select = fn(&Snapshot, &str, Option<&str>) -> Result<Writer>;

/// Both primary gateways use the same whole-operation deadline and retry cadence.
///
/// # Errors
/// Preserves resolution failures and returns a bounded recovery deadline error.
pub async fn resolve_active<F, Fut>(mut resolve: F) -> Result<Assignment>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Assignment>>,
{
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let assignment = resolve().await?;
            if assignment.record.lifecycle == Lifecycle::Active {
                return Ok(assignment);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("owner recovery/drain deadline")?
}

/// Shared resource-normalized policy; the two arms differ in hosting, not policy.
///
/// # Errors
/// Rejects absent fresh capacity.
pub fn adr_select(state: &Snapshot, key: &str, excluded: Option<&str>) -> Result<Writer> {
    crate::placement::select(state, key, excluded)
}

pub struct Control {
    pub config: Config,
    pub store: Arc<dyn Store>,
    pub controller: Controller,
    select: Select,
    serial: Mutex<()>,
    slots: Semaphore,
}

impl Control {
    /// # Errors
    /// Rejects a competing live controller; startup never resets authority.
    pub async fn start(config: Config, store: Arc<dyn Store>, select: Select) -> Result<Arc<Self>> {
        ensure!(config.delta.is_some(), "Delta control requires explicit sink mode");
        let id = uuid::Uuid::new_v4().to_string();
        let snapshot = store.execute(Command::Start { id }).await?;
        let controller = snapshot.controller.context("controller")?;
        for table in &config.tables {
            if snapshot.table_policies.contains_key(&table.key) {
                continue;
            }
            store
                .execute(Command::Catalog {
                    controller: controller.clone(),
                    key: table.key.clone(),
                    policy: placement::TablePolicy {
                        revision: 1,
                        fingerprints: table.fingerprints.clone(),
                        demand: placement::Demand::default(),
                    },
                })
                .await?;
        }
        Ok(Arc::new(Self {
            config,
            store,
            controller,
            select,
            serial: Mutex::new(()),
            slots: Semaphore::new(64),
        }))
    }

    /// # Errors
    /// Returns authentication-independent domain operation failures.
    pub async fn apply(&self, principal: &str, operation: Operation) -> Result<Value> {
        let context = timing::current().unwrap_or_else(|| timing::Context::new("", "authority"));
        context.scope(self.apply_inner(principal, operation)).await
    }

    async fn apply_inner(&self, principal: &str, operation: Operation) -> Result<Value> {
        let _slot = self.slots.try_acquire().context("control capacity")?;
        let mut queue = timing::Span::new("control-queue", "queue");
        let _serial = self.serial.lock().await;
        queue.completed();
        drop(queue);
        match operation {
            Operation::CatalogPolicy {
                key,
                revision,
                fingerprints,
                demand,
            } => {
                ensure!(principal == "operator", "catalog policy forbidden");
                let allowed = self.config.table(&key)?;
                ensure!(
                    fingerprints.iter().all(|f| allowed.fingerprints.contains(f)),
                    "schema not registered"
                );
                let current = self.store.snapshot().await?;
                ensure!(
                    current.table_policies.get(&key).is_some_and(|p| p.revision == revision),
                    "catalog policy revision conflict"
                );
                self.store
                    .execute(Command::Catalog {
                        controller: self.controller.clone(),
                        key,
                        policy: placement::TablePolicy {
                            revision: revision + 1,
                            fingerprints,
                            demand,
                        },
                    })
                    .await?;
                Ok(json!({"applied_revision":revision+1}))
            }
            Operation::Policy {
                collector,
                writer,
                revision,
                operation,
                excluded,
                block_streams,
                retiring,
            } => {
                ensure!(principal == "operator", "policy forbidden");
                let state = self
                    .store
                    .execute(Command::Policy {
                        controller: self.controller.clone(),
                        collector: collector.clone(),
                        writer,
                        revision,
                        operation,
                        excluded,
                        block_streams,
                        retiring,
                    })
                    .await?;
                Ok(serde_json::to_value(&state.policies[&collector])?)
            }
            Operation::Observe {
                writer,
                observation,
                applied_revision,
            } => {
                ensure!(principal == writer.collector, "observation impersonation");
                self.store
                    .execute(Command::Observe {
                        writer,
                        observation,
                        applied_revision,
                    })
                    .await?;
                Ok(json!({"observed":true}))
            }
            Operation::OpenStream {
                key,
                writer,
                epoch,
                stream,
                fingerprint,
            } => {
                ensure!(principal == writer.collector, "stream impersonation");
                self.store
                    .execute(Command::OpenStream {
                        key,
                        writer,
                        epoch,
                        stream,
                        fingerprint,
                    })
                    .await?;
                Ok(json!({"admitted":true}))
            }
            Operation::CloseStream { key, writer, stream } => {
                ensure!(principal == writer.collector, "stream impersonation");
                self.store.execute(Command::CloseStream { key, writer, stream }).await?;
                Ok(json!({"closed":true}))
            }
            Operation::AdmitStream {
                key,
                writer,
                epoch,
                stream,
                unit,
            } => {
                ensure!(principal == writer.collector, "admission impersonation");
                let state = self
                    .store
                    .execute(Command::AdmitStream {
                        key: key.clone(),
                        writer,
                        epoch,
                        stream,
                        unit,
                    })
                    .await?;
                Ok(serde_json::to_value(Assignment {
                    revision: state.revision,
                    record: state.tables[&key].record.clone(),
                })?)
            }
            Operation::View | Operation::Watch { .. } => {
                let state = self.store.snapshot().await?;
                // Every delivery is a complete consistent revisioned snapshot.
                // There is no lossy incremental cursor to silently skip a gap.
                Ok(
                    json!({"revision":state.revision,"snapshot":true,"assignments":state.assignments(),
                    "members":state.members.values().map(|m| &m.member).collect::<Vec<_>>(),
                    "policies":state.policies,"table_policies":state.table_policies,"optimization":state.optimization,
                    "observations":state.observations,
                    "operations":state.tables.iter().map(|(key,t)|(key,&t.reservation)).collect::<BTreeMap<_,_>>()}),
                )
            }
            Operation::Register { member } => {
                ensure!(
                    principal == member.writer.collector && self.config.collectors.contains_key(principal),
                    "registration impersonation"
                );
                ensure!(
                    safe_id(&member.writer.process) && member.endpoint == format!("http://{principal}:50051"),
                    "invalid process/endpoint"
                );
                let state = self
                    .store
                    .execute(Command::Register {
                        controller: self.controller.clone(),
                        member,
                    })
                    .await?;
                Ok(json!({"writer":state.members[principal].member.writer,"revision":state.revision}))
            }
            Operation::ReplaceSession { writer } => {
                ensure!(principal == writer.collector, "replacement impersonation");
                let state = self
                    .store
                    .execute(Command::Replace {
                        controller: self.controller.clone(),
                        writer,
                    })
                    .await?;
                Ok(json!({"writer":state.members[principal].member.writer,"revision":state.revision}))
            }
            Operation::Resolve { key } => {
                ensure!(principal == "gateway" || principal == "operator", "resolve forbidden");
                self.config.table(&key)?;
                let mut state = self.store.snapshot().await?;
                if !state.tables.contains_key(&key) {
                    let mut span = timing::Span::new("placement-select", "processing");
                    let target = (self.select)(&state, &key, None)?;
                    span.completed();
                    drop(span);
                    state = self
                        .store
                        .execute(Command::Claim {
                            controller: self.controller.clone(),
                            key: key.clone(),
                            target,
                            operation: uuid::Uuid::new_v4().to_string(),
                        })
                        .await?;
                }
                Ok(serde_json::to_value(Assignment {
                    revision: state.revision,
                    record: state.tables[&key].record.clone(),
                })?)
            }
            Operation::Admit {
                key,
                writer,
                epoch,
                unit,
            } => {
                ensure!(
                    principal == writer.collector && safe_id(&unit),
                    "admission impersonation/operation"
                );
                self.config.table(&key)?;
                let state = self
                    .store
                    .execute(Command::Admit {
                        key: key.clone(),
                        writer,
                        epoch,
                        unit,
                    })
                    .await?;
                Ok(serde_json::to_value(Assignment {
                    revision: state.revision,
                    record: state.tables[&key].record.clone(),
                })?)
            }
            Operation::Migrate { key } => {
                ensure!(principal == "operator", "migration forbidden");
                self.migrate(&key).await?;
                Ok(json!({"reserved":true}))
            }
            Operation::Revoke { collector } => {
                ensure!(principal == "operator", "revoke forbidden");
                self.store
                    .execute(Command::Revoke {
                        controller: self.controller.clone(),
                        collector,
                    })
                    .await?;
                Ok(json!({"revoked":true}))
            }
            _ => bail!("unsupported operation for Delta control role"),
        }
    }

    async fn migrate(&self, key: &str) -> Result<()> {
        self.config.table(key)?;
        let state = self.store.snapshot().await?;
        let source = &state.tables.get(key).context("unplaced")?.record.writer;
        let target = (self.select)(&state, key, Some(&source.collector))?;
        self.store
            .execute(Command::Reserve {
                controller: self.controller.clone(),
                key: key.into(),
                target,
                operation: uuid::Uuid::new_v4().to_string(),
            })
            .await?;
        Ok(())
    }

    async fn reconcile(&self) -> Result<()> {
        let _serial = self.serial.lock().await;
        self.store
            .execute(Command::Renew {
                controller: self.controller.clone(),
            })
            .await?;
        self.store
            .execute(Command::Optimize {
                controller: self.controller.clone(),
            })
            .await?;
        let state = self.store.snapshot().await?;
        for (key, t) in &state.tables {
            let result: Result<()> = async {
                if t.record.lifecycle == Lifecycle::Draining {
                    if t.reservation.as_ref().is_some_and(|r| {
                        state
                            .members
                            .get(&r.target.collector)
                            .is_none_or(|m| m.expires == 0 || m.member.writer != r.target)
                            || state.policy_for(&r.target).is_some_and(|p| p.excluded || p.retiring)
                    }) {
                        return self.migrate(key).await;
                    }
                    self.store
                        .execute(Command::Transfer {
                            controller: self.controller.clone(),
                            key: key.clone(),
                            operation: t.reservation.as_ref().context("reservation")?.operation.clone(),
                            force: true,
                        })
                        .await?;
                } else if state.policy_for(&t.record.writer).is_some_and(|p| p.retiring) {
                    self.migrate(key).await?;
                } else if state.table_policies.get(key).is_some_and(|policy| {
                    state
                        .observations
                        .get(&t.record.writer.collector)
                        .is_some_and(|o| policy.fingerprints.iter().any(|f| !o.fingerprints.contains(f)))
                }) {
                    self.migrate(key).await?;
                } else if state
                    .members
                    .get(&t.record.writer.collector)
                    .is_none_or(|m| m.expires == 0 || m.member.writer != t.record.writer)
                {
                    self.migrate(key).await?;
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                // One draining table must not block independent failover.
                eprintln!("Delta table reconciliation {key}: {error}");
            }
        }
        let state = self.store.snapshot().await?;
        for (collector, policy) in &state.policies {
            let Some(writer) = policy.incarnation.as_ref() else {
                continue;
            };
            if state.members.get(collector).is_some_and(|m| m.member.writer == *writer)
                && policy.retiring
                && !policy.approved
                && policy.applied_revision == policy.revision
                && policy.recovery_obligations.is_empty()
                && !state.tables.values().any(|t| {
                    t.record.writer.collector == *collector
                        || t.reservation.as_ref().is_some_and(|r| r.target.collector == *collector)
                })
            {
                self.store
                    .execute(Command::ApproveRetirement {
                        controller: self.controller.clone(),
                        writer: writer.clone(),
                        revision: policy.revision,
                        operation: policy.operation.clone(),
                    })
                    .await?;
            }
        }
        Ok(())
    }
}

fn principal<'a>(config: &'a Config, headers: &HeaderMap) -> Result<&'a str> {
    let token = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .unwrap_or("");
    ensure!(!token.is_empty(), "authentication required");
    if token == config.operator_token {
        return Ok("operator");
    }
    if token == config.gateway_token {
        return Ok("gateway");
    }
    config
        .collectors
        .iter()
        .find(|(_, s)| s.as_str() == token)
        .map(|(id, _)| id.as_str())
        .context("authentication required")
}

fn response(result: Result<Value>) -> (StatusCode, Json<Value>) {
    match result {
        Ok(v) => (StatusCode::OK, Json(v)),
        Err(e) => (StatusCode::CONFLICT, Json(json!({"error":e.to_string()}))),
    }
}

async fn control_request(
    State(control): State<Arc<Control>>,
    headers: HeaderMap,
    Json(op): Json<Operation>,
) -> (StatusCode, Json<Value>) {
    let trace = headers.get("x-adr-trace").and_then(|v| v.to_str().ok()).unwrap_or("");
    let parent = headers.get("x-adr-parent").and_then(|v| v.to_str().ok()).unwrap_or("");
    response(
        timing::Context::from_parent(trace, "authority", parent)
            .scope(timing::measure("control-request", "lifecycle", async {
                control.apply(principal(&control.config, &headers)?, op).await
            }))
            .await,
    )
}

/// # Errors
/// Returns listener/server failures.
pub async fn serve_control(control: Arc<Control>, listen: &str) -> Result<()> {
    let background = Arc::clone(&control);
    tokio::spawn(async move {
        loop {
            if let Err(e) = background.reconcile().await {
                eprintln!("Delta reconciliation: {e}");
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    });
    let app = Router::new()
        .route("/v1/authority", post(control_request))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
        .with_state(control);
    axum::serve(tokio::net::TcpListener::bind(listen).await?, app).await?;
    Ok(())
}

pub struct Publication {
    config: Config,
    private: PrivateConfig,
    store: Arc<dyn Store>,
    publisher: Publisher,
    slots: Semaphore,
    mailboxes: BTreeMap<String, Mutex<()>>,
}

impl Publication {
    async fn recover_accepted_prefix(&self) -> Result<()> {
        let state = self.store.snapshot().await?;
        for (key, table) in state.tables {
            if table.published < table.decisions.len() as i64 - 1 {
                let Some(mailbox) = self.mailboxes.get(&key) else {
                    continue;
                };
                let Ok(_guard) = mailbox.try_lock() else { continue };
                if let Err(error) = Box::pin(self.publisher.materialize(&key, &NoHooks)).await {
                    eprintln!("Delta accepted-prefix recovery {key}: {error}");
                }
            }
        }
        Ok(())
    }

    /// # Errors
    /// Returns runtime authority/storage initialization errors.
    pub async fn new(config: Config, private: PrivateConfig) -> Result<Arc<Self>> {
        let store = private.store(&config).await?;
        delta_backends::SqlStore::bootstrap(private.sql()?).await?;
        let publisher = Publisher::new(&private.root, private.sql()?)?.with_authority(Arc::clone(&store));
        let mailboxes = config.tables.iter().map(|t| (t.key.clone(), Mutex::new(()))).collect();
        Ok(Arc::new(Self {
            config,
            private,
            store,
            publisher,
            slots: Semaphore::new(16),
            mailboxes,
        }))
    }

    async fn apply(&self, principal: &str, op: Operation) -> Result<Value> {
        let _slot = self.slots.try_acquire().context("publisher capacity")?;
        let (key, writer, epoch) = match &op {
            Operation::PublishDelta { key, writer, epoch, .. } | Operation::RecoverDelta { key, writer, epoch } => {
                (key, writer, *epoch)
            }
            Operation::DeltaReceipt { key, unit } => {
                self.config.table(key)?;
                ensure!(
                    principal == "operator" || principal == "gateway" || self.config.collectors.contains_key(principal),
                    "receipt forbidden"
                );
                let history = self.publisher.decisions(key).await?;
                let p = history
                    .iter()
                    .find(|p| p.operation == *unit)
                    .context("unknown operation")?;
                let final_sql = delta_backends::SqlStore {
                    connection: self.private.sql()?,
                }
                .has_receipt(p)
                .await?;
                return Ok(json!({"operation":unit,"version":p.version,"digest":p.digest()?,"final":final_sql}));
            }
            _ => bail!("unsupported publication role operation"),
        };
        ensure!(principal == writer.collector, "publication impersonation");
        self.config.table(key)?;
        let mut wait = timing::Span::new("publisher-mailbox", "queue");
        let _mailbox = self.mailboxes.get(key).context("unknown table")?.lock().await;
        wait.completed();
        drop(wait);
        let state = self.store.snapshot().await?;
        let identity = state.identity(key, writer, epoch)?;
        if !self.publisher.decisions(key).await?.is_empty() {
            timing::measure(
                "prefix-materialize",
                "lifecycle",
                Box::pin(self.publisher.materialize(key, &NoHooks)),
            )
            .await?;
        }
        match &op {
            Operation::RecoverDelta { .. } => {
                Box::pin(self.publisher.activate(key, &identity)).await?;
                Ok(json!({"activated":true}))
            }
            Operation::PublishDelta { unit, sha256, .. } => {
                ensure!(safe_id(unit) && sha256.len() == 64, "invalid staged operation");
                self.store
                    .execute(Command::Bind {
                        key: key.clone(),
                        writer: writer.clone(),
                        epoch,
                        unit: unit.clone(),
                        sha256: sha256.clone(),
                    })
                    .await?;
                let stage = self.private.staging.join(principal).join(format!("{unit}.parquet"));
                // The collector only names its own file. No submitted SQL/actions,
                // arbitrary path, schema, identity or final destination is trusted.
                let history = self.publisher.decisions(key).await?;
                if let Some(p) = history.iter().find(|p| p.operation == *unit) {
                    return Ok(
                        json!({"kind":"DELTA_COMMITTED","operation":unit,"version":p.version,"digest":p.digest()?}),
                    );
                }
                let bytes = sql_publication::bounded_read(&stage, 8 * 1024 * 1024)?;
                use sha2::Digest;
                ensure!(
                    format!("{:x}", sha2::Sha256::digest(&bytes)) == *sha256,
                    "stage digest mismatch"
                );
                let builder =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))?;
                let schema = Arc::clone(builder.schema());
                let expected = state
                    .tables
                    .get(key)
                    .and_then(|t| t.input_policy.get(unit))
                    .context("publication requires schema-bound admission")?;
                ensure!(
                    arrowcol_protocol::fingerprint_schema(schema.as_ref()).to_string() == expected.1,
                    "staged schema differs from admitted schema"
                );
                let schema_operation = format!("{unit}-schema");
                // Native Delta preparation has a large async state; keep it off
                // the worker's bounded stack rather than nesting it in dispatch.
                match Box::pin(self.publisher.prepare(
                    key,
                    &schema_operation,
                    identity.clone(),
                    arrow_deltalake::normalize::schema(&schema)?,
                    None,
                ))
                .await
                {
                    Ok(p) => {
                        Box::pin(self.publisher.accept(&p, &NoHooks, false)).await?;
                        Box::pin(self.publisher.materialize(key, &NoHooks)).await?;
                    }
                    Err(e) if e.to_string() == "schema proposal must change exactly one version" => {}
                    Err(e) => return Err(e),
                }
                let proposal = timing::measure(
                    "data-prepare",
                    "lifecycle",
                    Box::pin(
                        self.publisher
                            .prepare(key, unit, identity, schema, Some((&stage, sha256))),
                    ),
                )
                .await?;
                timing::measure(
                    "data-accept",
                    "downstream_wait",
                    Box::pin(self.publisher.accept(&proposal, &RuntimeHooks, false)),
                )
                .await?;
                timing::measure(
                    "data-materialize",
                    "lifecycle",
                    Box::pin(self.publisher.materialize(key, &RuntimeHooks)),
                )
                .await?;
                Ok(
                    json!({"kind":"DELTA_COMMITTED","operation":unit,"version":proposal.version,"digest":proposal.digest()?}),
                )
            }
            _ => bail!("invalid publication request"),
        }
    }
}

async fn publication_request(
    State(p): State<Arc<Publication>>,
    headers: HeaderMap,
    Json(op): Json<Operation>,
) -> (StatusCode, Json<Value>) {
    response(
        async {
            let principal = principal(&p.config, &headers)?.to_owned();
            let context = timing::Context::from_parent(
                headers.get("x-adr-trace").and_then(|v| v.to_str().ok()).unwrap_or(""),
                "publisher",
                headers.get("x-adr-parent").and_then(|v| v.to_str().ok()).unwrap_or(""),
            );
            // A collector's HTTP deadline is not an abort of a submitted
            // publication. Bounded trusted work must outlive that connection.
            tokio::spawn(
                context.scope(async move {
                    timing::measure("publisher-request", "lifecycle", p.apply(&principal, op)).await
                }),
            )
            .await?
        }
        .await,
    )
}

/// # Errors
/// Returns bind/server errors.
pub async fn serve_publication(p: Arc<Publication>, listen: &str) -> Result<()> {
    let recovery = Arc::clone(&p);
    tokio::spawn(async move {
        loop {
            if let Err(error) = recovery.recover_accepted_prefix().await {
                eprintln!("Delta accepted-prefix snapshot: {error}");
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    });
    let app = Router::new()
        .route("/v1/authority", post(publication_request))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
        .with_state(p);
    axum::serve(tokio::net::TcpListener::bind(listen).await?, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_dispatch_future_has_bounded_stack_storage() {
        fn size<F: std::future::Future>(_: impl FnOnce() -> F) -> usize {
            std::mem::size_of::<F>()
        }
        let publication: Option<&Publication> = None;
        let bytes = size(|| publication.unwrap().apply("collector1", Operation::View));
        assert!(bytes <= 64 * 1024, "publication future requires {bytes} bytes");
        let recovery_bytes = size(|| publication.unwrap().recover_accepted_prefix());
        assert!(
            recovery_bytes <= 64 * 1024,
            "background recovery future requires {recovery_bytes} bytes"
        );
    }

    struct MemoryStore(Mutex<Snapshot>);
    #[async_trait::async_trait]
    impl Store for MemoryStore {
        async fn snapshot(&self) -> Result<Snapshot> {
            Ok(self.0.lock().await.clone())
        }
        async fn execute(&self, command: Command) -> Result<Snapshot> {
            let mut state = self.0.lock().await;
            let mut next = state.clone();
            delta_store::apply(
                &mut next,
                &command,
                &Clock {
                    now: 1,
                    live: None,
                    new_lease: 0,
                },
            )?;
            *state = next.clone();
            Ok(next)
        }
    }

    fn test_config() -> Result<Config> {
        Ok(serde_json::from_value(json!({
            "delta":{"publisher":"http://publisher"},"tenant":"t","catalog_id":"c",
            "tables":[{"key":"table","path":["c","s","table"],"fingerprints":["fp"]}],
            "storage":".","controller":"http://control","operator_token":"o","gateway_token":"g",
            "edge_secret":"e","internal_secret":"i","collectors":{},"etcd":[],"etcd_user":"","etcd_password":""
        }))?)
    }

    #[tokio::test]
    async fn restart_preserves_updated_catalog_policy() -> Result<()> {
        let store = Arc::new(MemoryStore(Mutex::new(Snapshot::default())));
        let control = Control::start(test_config()?, Arc::<MemoryStore>::clone(&store), adr_select).await?;
        store
            .execute(Command::Catalog {
                controller: control.controller.clone(),
                key: "table".into(),
                policy: placement::TablePolicy {
                    revision: 2,
                    fingerprints: vec!["fp".into()],
                    demand: placement::Demand::default(),
                },
            })
            .await?;
        store.0.lock().await.controller_expires = 0;
        let _successor = Control::start(test_config()?, Arc::<MemoryStore>::clone(&store), adr_select).await?;
        assert_eq!(store.snapshot().await?.table_policies["table"].revision, 2);
        Ok(())
    }

    #[tokio::test]
    async fn unassigned_collector_can_retire_without_a_table_loop() -> Result<()> {
        let store = Arc::new(MemoryStore(Mutex::new(Snapshot::default())));
        let control = Control::start(test_config()?, Arc::<MemoryStore>::clone(&store), adr_select).await?;
        let state = store
            .execute(Command::Register {
                controller: control.controller.clone(),
                member: Member {
                    writer: Writer {
                        collector: "a".into(),
                        process: "process-a".into(),
                        generation: String::new(),
                    },
                    endpoint: "http://a:50051".into(),
                    pressure: 0.1,
                    rss_bytes: 1024,
                    active: 0,
                    eligible: true,
                },
            })
            .await?;
        let writer = state.members["a"].member.writer.clone();
        store
            .execute(Command::Policy {
                controller: control.controller.clone(),
                collector: "a".into(),
                writer: writer.clone(),
                revision: 0,
                operation: "retire-a".into(),
                excluded: true,
                block_streams: false,
                retiring: true,
            })
            .await?;
        store
            .execute(Command::Observe {
                writer,
                applied_revision: 1,
                observation: placement::Observation {
                    demand: placement::Demand::default(),
                    capacity: placement::Demand {
                        cpu: 1.0,
                        memory: 1.0,
                        throughput: 1.0,
                    },
                    sequence: 1,
                    allocation_revision: 0,
                    tables: BTreeMap::new(),
                    fingerprints: ["fp".to_owned()].into(),
                },
            })
            .await?;
        control.reconcile().await?;
        assert!(store.snapshot().await?.policies["a"].approved);
        Ok(())
    }
    #[tokio::test]
    async fn blocked_drain_does_not_starve_an_independent_failed_source() -> Result<()> {
        let store = Arc::new(MemoryStore(Mutex::new(Snapshot::default())));
        let config = serde_json::from_value(json!({
            "delta":{"publisher":"http://publisher"},"tenant":"t","catalog_id":"c",
            "tables":[{"key":"blocked","path":["c","s","blocked"],"fingerprints":["fingerprint"]},
                      {"key":"recover","path":["c","s","recover"],"fingerprints":["fingerprint"]}],
            "storage":".","controller":"http://control","operator_token":"o","gateway_token":"g",
            "edge_secret":"e","internal_secret":"i","collectors":{},"etcd":[],"etcd_user":"","etcd_password":""
        }))?;
        let control = Control::start(config, Arc::<MemoryStore>::clone(&store), adr_select).await?;
        for collector in ["a", "b"] {
            store
                .execute(Command::Register {
                    controller: control.controller.clone(),
                    member: Member {
                        writer: Writer {
                            collector: collector.into(),
                            process: format!("process-{collector}"),
                            generation: String::new(),
                        },
                        endpoint: format!("http://{collector}:50051"),
                        pressure: 0.1,
                        rss_bytes: 1,
                        active: 0,
                        eligible: true,
                    },
                })
                .await?;
            let state = store.snapshot().await?;
            store
                .execute(Command::Observe {
                    writer: state.members[collector].member.writer.clone(),
                    applied_revision: 0,
                    observation: placement::Observation {
                        demand: placement::Demand::default(),
                        capacity: placement::Demand {
                            cpu: 1.0,
                            memory: 1.0,
                            throughput: 1.0,
                        },
                        sequence: 1,
                        allocation_revision: 0,
                        tables: BTreeMap::new(),
                        fingerprints: ["fingerprint".to_owned()].into(),
                    },
                })
                .await?;
        }
        let state = store.snapshot().await?;
        let a = state.members["a"].member.writer.clone();
        let b = state.members["b"].member.writer.clone();
        for (key, target) in [("blocked", b.clone()), ("recover", a.clone())] {
            store
                .execute(Command::Claim {
                    controller: control.controller.clone(),
                    key: key.into(),
                    target,
                    operation: format!("claim-{key}"),
                })
                .await?;
            let state = store.snapshot().await?;
            store
                .execute(Command::Activate {
                    table: key.into(),
                    identity: state.tables[key].identity.clone(),
                    version: -1,
                })
                .await?;
        }
        store
            .execute(Command::Admit {
                key: "blocked".into(),
                writer: b,
                epoch: 1,
                unit: "pending".into(),
            })
            .await?;
        store
            .execute(Command::Reserve {
                controller: control.controller.clone(),
                key: "blocked".into(),
                target: a,
                operation: "move".into(),
            })
            .await?;
        store
            .execute(Command::Revoke {
                controller: control.controller.clone(),
                collector: "a".into(),
            })
            .await?;
        control.reconcile().await?;
        control.reconcile().await?;
        let state = store.snapshot().await?;
        assert_eq!(state.tables["blocked"].record.pending, ["pending"]);
        assert_eq!(state.tables["recover"].record.writer.collector, "b");
        assert_eq!(state.tables["recover"].record.lifecycle, Lifecycle::Recovering);
        assert_eq!(state.tables["recover"].record.epoch, 2);
        Ok(())
    }
}
