//! Real SQL/etcd implementations of the typed bounded authority.
use crate::{
    Config,
    delta_store::*,
    etcd::{Etcd, absent, compare, put},
};
use anyhow::{Context, Result, bail, ensure};
use arrowcol_sql_server::{ConnectionConfig, SqlConnection};
use async_trait::async_trait;
use serde_json::json;
use std::collections::BTreeSet;

const STATE: &str = "/delta/state";

pub struct SqlStore {
    pub connection: ConnectionConfig,
}

impl SqlStore {
    /// # Errors
    /// Returns schema/connection errors; never resets existing authority.
    pub async fn bootstrap(connection: ConnectionConfig) -> Result<Self> {
        let mut sql = SqlConnection::connect(&connection).await?;
        sql.simple_query(include_str!("delta_authority.sql"))
            .await?
            .into_results()
            .await?;
        sql.finish().await?;
        Ok(Self { connection })
    }

    /// SQL completion projection is deliberately independent of etcd authority.
    /// # Errors
    /// Rejects exact-operation collisions and unavailable SQL.
    pub async fn final_receipt(&self, proposal: &crate::sql_publication::Proposal) -> Result<()> {
        let mut sql = SqlConnection::connect(&self.connection).await?;
        let digest = proposal.digest()?;
        sql.query(
            "SET XACT_ABORT ON; BEGIN TRAN;
             IF EXISTS(SELECT 1 FROM delta_authority.receipts WITH(UPDLOCK,HOLDLOCK)
               WHERE table_id=@P1 AND operation_id=@P2)
             BEGIN
               IF NOT EXISTS(SELECT 1 FROM delta_authority.receipts
                 WHERE table_id=@P1 AND operation_id=@P2 AND version=@P3 AND digest=@P4)
                 THROW 51000,'final receipt collision',1;
             END ELSE INSERT delta_authority.receipts(table_id,operation_id,version,digest)
               VALUES(@P1,@P2,@P3,@P4);
             COMMIT;",
            &[&proposal.table, &proposal.operation, &proposal.version, &digest],
        )
        .await?
        .into_results()
        .await?;
        sql.finish().await?;
        Ok(())
    }

    /// # Errors
    /// Returns SQL failures rather than interpreting them as absent completion.
    pub async fn has_receipt(&self, proposal: &crate::sql_publication::Proposal) -> Result<bool> {
        let mut sql = SqlConnection::connect(&self.connection).await?;
        let digest = proposal.digest()?;
        let row = sql
            .query(
                "SELECT digest FROM delta_authority.receipts WHERE table_id=@P1 AND operation_id=@P2",
                &[&proposal.table, &proposal.operation],
            )
            .await?
            .into_row()
            .await?;
        let found = if let Some(row) = row {
            ensure!(
                row.get::<&str, _>(0) == Some(digest.as_str()),
                "receipt digest mismatch"
            );
            true
        } else {
            false
        };
        sql.finish().await?;
        Ok(found)
    }
}

#[async_trait]
impl Store for SqlStore {
    async fn snapshot(&self) -> Result<Snapshot> {
        let mut sql = SqlConnection::connect(&self.connection).await?;
        let row = sql.query(
            "SELECT payload,DATEDIFF_BIG(millisecond,'19700101',SYSUTCDATETIME()) FROM delta_authority.state WHERE id=1",
            &[]).await?.into_row().await?.context("missing authority schema")?;
        let mut state: Snapshot = serde_json::from_str(row.get::<&str, _>(0).context("state payload")?)?;
        let now = row.get::<i64, _>(1).context("DB time")?;
        for m in state.members.values_mut() {
            if m.expires <= now {
                m.member.eligible = false;
                m.expires = 0;
            }
        }
        sql.finish().await?;
        Ok(state)
    }

    async fn execute(&self, command: Command) -> Result<Snapshot> {
        let mut sql = SqlConnection::connect(&self.connection).await?;
        sql.simple_query("SET XACT_ABORT ON; BEGIN TRAN;")
            .await?
            .into_results()
            .await?;
        // SQL's row lock serializes controller/member changes, transfer AND
        // decision authorization. Revision increments are commit-ordered here,
        // unlike IDENTITY/sequence allocation outside the serialization lock.
        let row = sql
            .query(
                "SELECT payload,DATEDIFF_BIG(millisecond,'19700101',SYSUTCDATETIME())
             FROM delta_authority.state WITH(UPDLOCK,HOLDLOCK) WHERE id=1",
                &[],
            )
            .await?
            .into_row()
            .await?
            .context("missing authority state")?;
        let mut state: Snapshot = serde_json::from_str(row.get::<&str, _>(0).context("payload")?)?;
        apply(
            &mut state,
            &command,
            &Clock {
                now: row.get::<i64, _>(1).context("DB time")?,
                live: None,
                new_lease: 0,
            },
        )?;
        let payload = serde_json::to_string(&state)?;
        sql.query("UPDATE delta_authority.state SET payload=@P1 WHERE id=1;", &[&payload])
            .await?
            .into_results()
            .await?;
        sql.simple_query("COMMIT;").await?.into_results().await?;
        // An unknown COMMIT outcome is returned as an error. Operations carry
        // identities and callers reconcile; never blind-retry unknown commits.
        sql.finish().await?;
        Ok(state)
    }
}

pub struct EtcdStore {
    etcd: Etcd,
}
impl EtcdStore {
    /// # Errors
    /// Rejects unavailable/authentication-failed etcd.
    pub async fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            etcd: Etcd::new(config).await?,
        })
    }
}

#[async_trait]
impl Store for EtcdStore {
    async fn snapshot(&self) -> Result<Snapshot> {
        let (_, kvs) = self.etcd.range("/delta/", true).await?;
        let mut state: Snapshot = kvs
            .iter()
            .find(|k| k.key == STATE)
            .map(|k| serde_json::from_str(&k.value))
            .transpose()?
            .unwrap_or_default();
        for (id, m) in &mut state.members {
            if !kvs.iter().any(|k| k.key == member_key(id)) {
                m.member.eligible = false;
                m.expires = 0;
            }
        }
        Ok(state)
    }

    async fn execute(&self, command: Command) -> Result<Snapshot> {
        for _ in 0..8 {
            let (_, kvs) = self.etcd.range("/delta/", true).await?;
            let current = kvs.iter().find(|k| k.key == STATE);
            let mut state: Snapshot = current
                .map(|k| serde_json::from_str(&k.value))
                .transpose()?
                .unwrap_or_default();
            let live = kvs
                .iter()
                .filter(|k| k.key.starts_with("/delta/live/"))
                .map(|k| k.key.clone())
                .collect::<BTreeSet<_>>();
            // Validate identity before lease RPC side effects: a stale caller
            // must not keep the current process/controller alive.
            apply(
                &mut state.clone(),
                &command,
                &Clock {
                    now: 0,
                    live: Some(live.clone()),
                    new_lease: 0,
                },
            )?;
            let mut comparisons = current.map(compare).unwrap_or_else(|| vec![absent(STATE)]);
            let mut keys = state.members.keys().map(|id| member_key(id)).collect::<BTreeSet<_>>();
            keys.insert(controller_key());
            if let Command::Register { member, .. } = &command {
                keys.insert(member_key(&member.writer.collector));
            }
            for key in keys {
                comparisons.extend(
                    kvs.iter()
                        .find(|k| k.key == key)
                        .map(compare)
                        .unwrap_or_else(|| vec![absent(&key)]),
                );
            }
            let mut writes = vec![];
            let mut new_lease = 0;
            match &command {
                Command::Start { id } if !live.contains(&controller_key()) => {
                    new_lease = self.etcd.grant(TTL_SECONDS).await?;
                    writes.push(put(&controller_key(), id, Some(new_lease)));
                }
                Command::Renew { .. } => self.etcd.keepalive(state.controller_lease).await?,
                Command::Register { member, .. } => {
                    let key = member_key(&member.writer.collector);
                    if live.contains(&key) {
                        let old = state.members.get(&member.writer.collector).context("session")?;
                        // Do not renew a different process's lease.
                        ensure!(
                            member.writer.process == old.member.writer.process
                                && (member.writer.generation.is_empty()
                                    || member.writer.generation == old.member.writer.generation),
                            "NOT_OWNER register identity"
                        );
                        self.etcd.keepalive(old.lease).await?;
                    } else {
                        new_lease = self.etcd.grant(TTL_SECONDS).await?;
                        writes.push(put(&key, &member.writer.process, Some(new_lease)));
                    }
                }
                Command::Revoke { collector, .. } => {
                    writes.push(json!({"request_delete_range":{"key":crate::etcd::b64(member_key(collector))}}))
                }
                _ => {}
            }
            apply(
                &mut state,
                &command,
                &Clock {
                    now: 0,
                    live: Some(live),
                    new_lease,
                },
            )?;
            writes.push(put(STATE, &serde_json::to_string(&state)?, None));
            let (ok, _) = self.etcd.txn(comparisons, writes).await?;
            if ok {
                return Ok(state);
            }
            // Only a known failed CAS can retry. RPC uncertainty exits above.
        }
        bail!("authority CAS contention; retry by operation identity")
    }
}
