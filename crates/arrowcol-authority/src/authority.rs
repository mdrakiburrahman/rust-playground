//! Placement is advisory. The ownership record, live identities and head are fenced in one etcd transaction.
use crate::{
    etcd::{Etcd, Kv, absent, compare, put},
    *,
};
use anyhow::{Result, bail, ensure};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

const CONTROL: &str = "/adr/control";
const GLOBAL_MIGRATIONS: usize = 4;
const TARGET_MIGRATIONS: usize = 2;

/// Reconstructed from durable operations on every selection, including after restart.
#[derive(Default)]
pub struct Placement {
    pub assignments: BTreeMap<String, usize>,
    pub reservations: BTreeMap<String, usize>,
    pub migrations: usize,
}
impl Placement {
    pub fn from_records(records: &[Record]) -> Self {
        let mut pressure = Self::default();
        for record in records {
            *pressure.assignments.entry(record.writer.collector.clone()).or_default() += 1;
            if let Some(operation) = &record.migration
                && record.lifecycle != Lifecycle::Active
                && matches!(operation.phase.as_str(), "draining" | "recovering")
            {
                pressure.migrations += 1;
                *pressure
                    .reservations
                    .entry(operation.target.collector.clone())
                    .or_default() += 1;
                if operation.target != record.writer {
                    *pressure
                        .assignments
                        .entry(operation.target.collector.clone())
                        .or_default() += 1;
                }
            }
        }
        pressure
    }
    pub fn can_reserve(&self, target: &str) -> bool {
        self.migrations < GLOBAL_MIGRATIONS && self.reservations.get(target).copied().unwrap_or(0) < TARGET_MIGRATIONS
    }
}
fn table_key(key: &str) -> String {
    format!("/adr/tables/{key}")
}
fn session_key(writer: &Writer) -> String {
    format!("/adr/members/{}", writer.collector)
}

pub struct Authority {
    pub config: Config,
    pub etcd: Etcd,
    control: Kv,
    lease: i64,
    leases: Mutex<BTreeMap<String, (Writer, i64)>>,
    serial: Mutex<()>,
}
impl Authority {
    /// # Errors
    /// Rejects unavailable consensus, authentication failures or an existing live controller.
    pub async fn start(config: Config) -> Result<Arc<Self>> {
        let etcd = Etcd::new(&config).await?;
        let lease = etcd.grant(10).await?;
        let value = uuid::Uuid::new_v4().to_string();
        let (ok, revision) = etcd
            .txn(vec![absent(CONTROL)], vec![put(CONTROL, &value, Some(lease))])
            .await?;
        ensure!(
            ok,
            "another live controller exists; wait for its lease, never reset authority"
        );
        Ok(Arc::new(Self {
            config,
            etcd,
            control: Kv {
                key: CONTROL.into(),
                value,
                revision,
            },
            lease,
            leases: Mutex::new(BTreeMap::new()),
            serial: Mutex::new(()),
        }))
    }
    /// # Errors
    /// Rejects unknown tables, unavailable consensus or corrupt records.
    pub async fn record(&self, key: &str) -> Result<(Kv, Record)> {
        self.config.table(key)?;
        let kv = self
            .etcd
            .get(&table_key(key))
            .await?
            .ok_or_else(|| anyhow::anyhow!("unplaced table"))?;
        let record = serde_json::from_str(&kv.value)?;
        Ok((kv, record))
    }
    async fn members(&self) -> Result<Vec<(Kv, Member)>> {
        self.etcd
            .range("/adr/members/", true)
            .await?
            .1
            .into_iter()
            .map(|kv| Ok((kv.clone(), serde_json::from_str(&kv.value)?)))
            .collect()
    }
    async fn live(&self, writer: &Writer) -> Result<Kv> {
        let kv = self
            .etcd
            .get(&session_key(writer))
            .await?
            .ok_or_else(|| anyhow::anyhow!("NOT_OWNER expired process"))?;
        let member: Member = serde_json::from_str(&kv.value)?;
        ensure!(member.writer == *writer, "NOT_OWNER process/generation mismatch");
        Ok(kv)
    }
    async fn save(&self, old: Option<&Kv>, record: &Record, live: Option<&Kv>) -> Result<i64> {
        let key = table_key(&record.key);
        let mut comparisons = compare(&self.control);
        comparisons.extend(old.map(compare).unwrap_or_else(|| vec![absent(&key)]));
        if let Some(live) = live {
            comparisons.extend(compare(live));
        }
        let (ok, rev) = self
            .etcd
            .txn(comparisons, vec![put(&key, &serde_json::to_string(record)?, None)])
            .await?;
        ensure!(ok, "NOT_OWNER control/owner/head CAS rejected");
        Ok(rev)
    }
    async fn choose(&self, excluded: Option<&str>, reservation: bool, replacing: Option<&str>) -> Result<(Kv, Member)> {
        let records = self
            .etcd
            .range("/adr/tables/", true)
            .await?
            .1
            .into_iter()
            .map(|kv| serde_json::from_str::<Record>(&kv.value))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let records = records
            .into_iter()
            .filter(|r| Some(r.key.as_str()) != replacing)
            .collect::<Vec<_>>();
        let pressure = Placement::from_records(&records);
        let mut members = self
            .members()
            .await?
            .into_iter()
            .filter(|(_, m)| {
                m.eligible
                    && Some(m.writer.collector.as_str()) != excluded
                    && m.pressure.is_finite()
                    && m.pressure < 0.95
                    && (!reservation || pressure.can_reserve(&m.writer.collector))
            })
            .collect::<Vec<_>>();
        members.sort_by(|a, b| {
            let score =
                |m: &Member| m.pressure + 0.05 * (*pressure.assignments.get(&m.writer.collector).unwrap_or(&0) as f64);
            score(&a.1)
                .total_cmp(&score(&b.1))
                .then(a.1.writer.collector.cmp(&b.1.writer.collector))
        });
        members
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("no fresh eligible capacity"))
    }
    /// # Errors
    /// Rejects unauthorized, stale, oversized or unavailable operations without granting success.
    pub async fn apply(&self, principal: &str, op: Operation) -> Result<Value> {
        // Serializes placement reservations/admission barriers, not data encoding or forwarding.
        let _guard = self.serial.lock().await;
        match op {
            Operation::PublishDelta { .. }
            | Operation::RecoverDelta { .. }
            | Operation::DeltaReceipt { .. }
            | Operation::Policy { .. }
            | Operation::Observe { .. }
            | Operation::OpenStream { .. }
            | Operation::CloseStream { .. }
            | Operation::AdmitStream { .. }
            | Operation::CatalogPolicy { .. } => {
                bail!("Delta operation requires explicit Delta runtime")
            }
            Operation::Watch { .. } => bail!("watch must use nonblocking directory handler"),
            Operation::View => {
                ensure!(
                    principal == "operator" || principal == "gateway" || self.config.collectors.contains_key(principal),
                    "forbidden view"
                );
                let (revision, kvs) = self.etcd.range("/adr/tables/", true).await?;
                let assignments = kvs
                    .into_iter()
                    .map(|kv| {
                        Ok(Assignment {
                            revision: kv.revision,
                            record: serde_json::from_str(&kv.value)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(
                    json!({"revision":revision,"assignments":assignments,"members":self.members().await?.into_iter().map(|(_,m)|m).collect::<Vec<_>>()}),
                )
            }
            Operation::ReplaceSession { writer } => {
                ensure!(principal == writer.collector, "replacement impersonation");
                let live = self.live(&writer).await?;
                let mut member: Member = serde_json::from_str(&live.value)?;
                member.writer.generation = uuid::Uuid::new_v4().to_string();
                let records = self.etcd.range("/adr/tables/", true).await?.1;
                let mut comparisons = compare(&self.control);
                comparisons.extend(compare(&live));
                let lease = self.etcd.grant(30).await?;
                let mut writes = vec![put(&live.key, &serde_json::to_string(&member)?, Some(lease))];
                for kv in records {
                    let mut record: Record = serde_json::from_str(&kv.value)?;
                    if record.writer != writer {
                        continue;
                    }
                    ensure!(writes.len() <= 32, "local replacement table bound");
                    record.writer = member.writer.clone();
                    record.epoch = record
                        .epoch
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("epoch overflow"))?;
                    record.lifecycle = Lifecycle::Recovering;
                    record.pending.clear();
                    if let Some(migration) = &mut record.migration {
                        migration.phase = "local_replaced".into();
                        migration.activated_ms = Some(now_ms());
                    }
                    comparisons.extend(compare(&kv));
                    writes.push(put(&kv.key, &serde_json::to_string(&record)?, None));
                }
                let (ok, revision) = self.etcd.txn(comparisons, writes).await?;
                ensure!(ok, "NOT_OWNER replacement CAS rejected");
                self.leases
                    .lock()
                    .await
                    .insert(principal.into(), (member.writer.clone(), lease));
                Ok(json!({"writer":member.writer,"revision":revision}))
            }
            Operation::Register { mut member } => {
                ensure!(
                    principal == member.writer.collector && self.config.collectors.contains_key(principal),
                    "process impersonation"
                );
                ensure!(
                    safe_id(&member.writer.process)
                        && (member.writer.generation.is_empty() || safe_id(&member.writer.generation)),
                    "invalid session"
                );
                let expected = format!("http://{}:50051", member.writer.collector);
                ensure!(
                    member.endpoint == expected && member.rss_bytes > 0 && member.pressure.is_finite(),
                    "invalid endpoint/pressure"
                );
                if self
                    .etcd
                    .get(&format!("/adr/draining/{}", member.writer.collector))
                    .await?
                    .is_some()
                {
                    member.eligible = false;
                }
                let key = session_key(&member.writer);
                let mut leases = self.leases.lock().await;
                if leases.contains_key(principal) && self.etcd.get(&key).await?.is_none() {
                    leases.remove(principal);
                }
                if let Some((writer, lease)) = leases.get(principal) {
                    if member.writer.process == writer.process && member.writer.generation.is_empty() {
                        member.writer.generation = writer.generation.clone();
                    }
                    ensure!(*writer == member.writer, "session cannot silently change generation");
                    self.etcd.keepalive(*lease).await?;
                    let live = self.live(writer).await?;
                    let mut comparisons = compare(&live);
                    comparisons.extend(compare(&self.control));
                    let (ok, _) = self
                        .etcd
                        .txn(
                            comparisons,
                            vec![put(&key, &serde_json::to_string(&member)?, Some(*lease))],
                        )
                        .await?;
                    ensure!(ok, "session heartbeat CAS");
                } else {
                    if let Some(live) = self.etcd.get(&key).await? {
                        let previous: Member = serde_json::from_str(&live.value)?;
                        if member.writer.process == previous.writer.process && member.writer.generation.is_empty() {
                            member.writer.generation = previous.writer.generation.clone();
                        }
                        // A controller restart may adopt the exact existing process, but never its stale lease.
                        ensure!(previous.writer == member.writer, "old process session still live");
                        let lease = self.etcd.grant(30).await?;
                        let mut comparisons = compare(&live);
                        comparisons.extend(compare(&self.control));
                        let (ok, _) = self
                            .etcd
                            .txn(
                                comparisons,
                                vec![put(&key, &serde_json::to_string(&member)?, Some(lease))],
                            )
                            .await?;
                        ensure!(ok, "session adoption CAS");
                        leases.insert(principal.to_owned(), (member.writer.clone(), lease));
                    } else {
                        // A process incarnation can be granted once. Liveness expiry never revives its generation.
                        ensure!(
                            member.writer.generation.is_empty(),
                            "expired generation cannot re-register"
                        );
                        member.writer.generation = uuid::Uuid::new_v4().to_string();
                        let process_key =
                            format!("/adr/processes/{}/{}", member.writer.collector, member.writer.process);
                        let lease = self.etcd.grant(30).await?;
                        let mut comparisons = compare(&self.control);
                        comparisons.push(absent(&key));
                        comparisons.push(absent(&process_key));
                        let (ok, _) = self
                            .etcd
                            .txn(
                                comparisons,
                                vec![
                                    put(&key, &serde_json::to_string(&member)?, Some(lease)),
                                    put(&process_key, &member.writer.generation, None),
                                ],
                            )
                            .await?;
                        ensure!(ok, "session registration CAS");
                        leases.insert(principal.to_owned(), (member.writer.clone(), lease));
                    }
                }
                Ok(json!({"registered":true,"writer":member.writer}))
            }
            Operation::Resolve { key } => {
                ensure!(principal == "gateway" || principal == "operator", "resolve forbidden");
                self.config.table(&key)?;
                if let Ok((kv, record)) = self.record(&key).await {
                    return Ok(json!(Assignment {
                        revision: kv.revision,
                        record
                    }));
                }
                let (live, member) = self.choose(None, false, None).await?;
                let record = Record {
                    key,
                    epoch: 1,
                    writer: member.writer,
                    endpoint: member.endpoint,
                    lifecycle: Lifecycle::Recovering,
                    head: None,
                    pending: vec![],
                    migration: None,
                };
                let revision = self.save(None, &record, Some(&live)).await?;
                Ok(json!(Assignment { revision, record }))
            }
            Operation::Admit {
                key,
                writer,
                epoch,
                unit,
            } => {
                ensure!(principal == writer.collector && safe_id(&unit), "admission identity");
                let live = self.live(&writer).await?;
                let (kv, mut record) = self.record(&key).await?;
                ensure!(
                    record.writer == writer && record.epoch == epoch && record.lifecycle == Lifecycle::Active,
                    "NOT_OWNER admission closed"
                );
                ensure!(
                    record.pending.len() < 64 && !record.pending.contains(&unit),
                    "pending bound/duplicate unit"
                );
                record.pending.push(unit);
                let revision = self.save(Some(&kv), &record, Some(&live)).await?;
                Ok(json!(Assignment { revision, record }))
            }
            Operation::Commit {
                key,
                writer,
                epoch,
                head,
            } => {
                ensure!(principal == writer.collector, "commit impersonation");
                let (kv, mut record) = self.record(&key).await?;
                let config = self.config.clone();
                let proposed = head.clone();
                let manifest = tokio::task::spawn_blocking(move || storage::validate(&config, &proposed)).await??;
                ensure!(
                    manifest.table == key && manifest.writer == writer && manifest.epoch == epoch,
                    "manifest authority identity"
                );
                if storage::contains(&self.config, record.head.clone(), &head)? {
                    return Ok(json!({"kind":"TABLE_COMMITTED","head":head,"reconciled":true}));
                }
                let live = self.live(&writer).await?;
                ensure!(
                    record.writer == writer && record.epoch == epoch && record.lifecycle != Lifecycle::Recovering,
                    "NOT_OWNER stale generation"
                );
                ensure!(
                    record.head == manifest.parent && record.pending.contains(&manifest.unit),
                    "head/admission mismatch"
                );
                record.pending.retain(|u| *u != manifest.unit);
                record.head = Some(head.clone());
                // This is the reader-visible boundary: exact live member, control identity, record value/revision,
                // lifecycle, epoch, admitted unit and prior head are all part of ONE transaction.
                match self.save(Some(&kv), &record, Some(&live)).await {
                    Ok(revision) => {
                        Ok(json!({"kind":"TABLE_COMMITTED","head":head,"revision":revision,"rows":manifest.rows}))
                    }
                    Err(error) => {
                        if let Ok((_, current)) = self.record(&key).await
                            && storage::contains(&self.config, current.head, &head)?
                        {
                            return Ok(json!({"kind":"TABLE_COMMITTED","head":head,"reconciled":true}));
                        }
                        Err(error)
                    }
                }
            }
            Operation::Activate {
                key,
                writer,
                epoch,
                observed,
            } => {
                ensure!(principal == writer.collector, "activation impersonation");
                let live = self.live(&writer).await?;
                let (kv, mut record) = self.record(&key).await?;
                ensure!(
                    record.writer == writer
                        && record.epoch == epoch
                        && record.head == observed
                        && record.lifecycle == Lifecycle::Recovering,
                    "recovery head changed"
                );
                record.lifecycle = Lifecycle::Active;
                if let Some(migration) = &mut record.migration {
                    migration.phase = "active".into();
                    migration.activated_ms = Some(now_ms());
                }
                let revision = self.save(Some(&kv), &record, Some(&live)).await?;
                Ok(json!(Assignment { revision, record }))
            }
            Operation::Migrate { key } => {
                ensure!(principal == "operator", "migration forbidden");
                let (kv, mut record) = self.record(&key).await?;
                ensure!(record.lifecycle == Lifecycle::Active, "migration already in progress");
                if let Some(m) = &record.migration {
                    ensure!(
                        now_ms().saturating_sub(m.activated_ms.unwrap_or(m.started_ms)) >= 500,
                        "migration cooldown"
                    );
                }
                let (live, target) = self.choose(Some(&record.writer.collector), true, None).await?;
                record.migration = Some(Migration {
                    id: uuid::Uuid::new_v4().to_string(),
                    source: record.writer.clone(),
                    target: target.writer,
                    started_ms: now_ms(),
                    deadline_ms: now_ms() + 2000,
                    phase: "draining".into(),
                    cutover_ms: None,
                    activated_ms: None,
                });
                record.lifecycle = Lifecycle::Draining;
                let revision = self.save(Some(&kv), &record, Some(&live)).await?;
                Ok(json!(Assignment { revision, record }))
            }
            Operation::Revoke { collector } => {
                ensure!(principal == "operator", "revoke forbidden");
                if let Some((_, lease)) = self.leases.lock().await.remove(&collector) {
                    self.etcd.revoke(lease).await?;
                }
                Ok(json!({"revoked":collector}))
            }
            Operation::DrainCollector { collector } => {
                ensure!(
                    principal == "operator" && self.config.collectors.contains_key(&collector),
                    "drain forbidden"
                );
                let (ok, _) = self
                    .etcd
                    .txn(
                        compare(&self.control),
                        vec![put(&format!("/adr/draining/{collector}"), "draining", None)],
                    )
                    .await?;
                ensure!(ok, "controller changed");
                Ok(json!({"draining":collector}))
            }
        }
    }
    /// # Errors
    /// Returns failed liveness, capacity, storage or conditional-transition checks.
    pub async fn reconcile(&self) -> Result<()> {
        self.etcd.keepalive(self.lease).await?;
        let _guard = self.serial.lock().await;
        let (revision, records) = self.etcd.range("/adr/tables/", true).await?;
        let parsed = records
            .iter()
            .map(|kv| serde_json::from_str::<Record>(&kv.value))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        eprintln!(
            "{}",
            json!({"event":"authority_observation","observed_ms":now_ms(),
            "directory_revision":revision,"control_revision":self.control.revision,
            "tables":parsed.len(),"pending_units":parsed.iter().map(|r|r.pending.len()).sum::<usize>(),
            "draining":parsed.iter().filter(|r|r.lifecycle==Lifecycle::Draining).count(),
            "recovering":parsed.iter().filter(|r|r.lifecycle==Lifecycle::Recovering).count()})
        );
        for (kv, mut record) in records.into_iter().zip(parsed) {
            let live = self.live(&record.writer).await;
            if record.lifecycle == Lifecycle::Draining
                && let Some(migration) = &record.migration
                && self.live(&migration.target).await.is_err()
                && let Ok(source_live) = &live
            {
                record.lifecycle = Lifecycle::Active;
                if let Some(migration) = &mut record.migration {
                    migration.phase = "target_failed".into();
                    migration.activated_ms = Some(now_ms());
                }
                self.save(Some(&kv), &record, Some(source_live)).await?;
                continue;
            }
            let cutover = record.lifecycle == Lifecycle::Draining
                && (record.pending.is_empty() || record.migration.as_ref().is_some_and(|m| now_ms() >= m.deadline_ms));
            if live.is_err() || cutover {
                let target = if let Some(migration) = &record.migration
                    && record.lifecycle == Lifecycle::Draining
                {
                    self.live(&migration.target)
                        .await
                        .ok()
                        .and_then(|kv| serde_json::from_str::<Member>(&kv.value).ok().map(|m| (kv, m)))
                } else {
                    None
                };
                let (target_live, target) = match target {
                    Some(v) => v,
                    None => {
                        self.choose(
                            Some(&record.writer.collector),
                            record
                                .migration
                                .as_ref()
                                .is_some_and(|m| matches!(m.phase.as_str(), "draining" | "recovering")),
                            Some(&record.key),
                        )
                        .await?
                    }
                };
                record.epoch = record
                    .epoch
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("epoch overflow"))?;
                record.writer = target.writer;
                record.endpoint = target.endpoint;
                record.pending.clear();
                record.lifecycle = Lifecycle::Recovering;
                if let Some(migration) = &mut record.migration
                    && matches!(migration.phase.as_str(), "draining" | "recovering")
                {
                    migration.phase = "recovering".into();
                    migration.cutover_ms = Some(now_ms());
                    migration.target = record.writer.clone();
                }
                self.save(Some(&kv), &record, Some(&target_live)).await?;
            }
        }
        Ok(())
    }
}

async fn request(
    State(authority): State<Arc<Authority>>,
    headers: HeaderMap,
    Json(op): Json<Operation>,
) -> (StatusCode, Json<Value>) {
    let token = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .unwrap_or("");
    let config = &authority.config;
    let principal = if token == config.operator_token {
        "operator"
    } else if token == config.gateway_token {
        "gateway"
    } else {
        config
            .collectors
            .iter()
            .find(|(_, secret)| secret.as_str() == token)
            .map(|(id, _)| id.as_str())
            .unwrap_or("")
    };
    if principal.is_empty() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"authentication required"})),
        );
    }
    let result = if let Operation::Watch { after } = op {
        if principal != "gateway" {
            Err(anyhow::anyhow!("watch forbidden"))
        } else {
            authority.etcd.watch(after).await
        }
    } else {
        authority.apply(principal, op).await
    };
    match result {
        Ok(value) => (StatusCode::OK, Json(value)),
        Err(error) => (StatusCode::CONFLICT, Json(json!({"error":error.to_string()}))),
    }
}
/// # Errors
/// Returns listener or server errors.
pub async fn serve(authority: Arc<Authority>, listen: &str) -> Result<()> {
    let background = Arc::clone(&authority);
    tokio::spawn(async move {
        loop {
            if let Err(error) = background.reconcile().await {
                eprintln!("{}", json!({"event":"reconcile_error","error":error.to_string()}));
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
    });
    let app = Router::new()
        .route("/v1/authority", post(request))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
        .with_state(authority);
    axum::serve(tokio::net::TcpListener::bind(listen).await?, app).await?;
    bail!("controller server stopped")
}
