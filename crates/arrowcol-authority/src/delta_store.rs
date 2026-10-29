//! Bounded, typed Delta authority. One serialized deployment state is intentional:
//! ownership and acceptance cannot race across independently updated records.
use crate::{
    Assignment, Lifecycle, Member, Record, Writer,
    sql_publication::{Identity, Proposal},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const STATE_LIMIT: usize = 768 * 1024;
pub const TTL_SECONDS: u64 = 12;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Controller {
    pub id: String,
    pub generation: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub member: Member,
    pub expires: i64,
    pub lease: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reservation {
    pub operation: String,
    pub target: Writer,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableState {
    pub record: Record,
    pub identity: Identity,
    pub decisions: Vec<Proposal>,
    pub published: i64,
    pub reservation: Option<Reservation>,
    #[serde(default)]
    pub inputs: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Snapshot {
    pub revision: i64,
    pub controller: Option<Controller>,
    pub controller_expires: i64,
    pub controller_lease: i64,
    pub members: BTreeMap<String, Session>,
    pub retired: BTreeSet<String>,
    pub tables: BTreeMap<String, TableState>,
}

impl Snapshot {
    pub fn assignments(&self) -> Vec<Assignment> {
        self.tables
            .values()
            .map(|t| Assignment {
                revision: self.revision,
                record: t.record.clone(),
            })
            .collect()
    }

    /// # Errors
    /// Rejects unplaced tables and stale process/local-generation/epoch tuples.
    pub fn identity(&self, key: &str, writer: &Writer, epoch: u64) -> Result<Identity> {
        let t = self.tables.get(key).context("unplaced table")?;
        ensure!(
            t.record.writer == *writer && t.record.epoch == epoch,
            "NOT_OWNER identity"
        );
        Ok(t.identity.clone())
    }
}

#[derive(Clone, Debug)]
pub enum Command {
    Start {
        id: String,
    },
    Renew {
        controller: Controller,
    },
    Register {
        controller: Controller,
        member: Member,
    },
    Replace {
        controller: Controller,
        writer: Writer,
    },
    Claim {
        controller: Controller,
        key: String,
        target: Writer,
        operation: String,
    },
    Reserve {
        controller: Controller,
        key: String,
        target: Writer,
        operation: String,
    },
    Transfer {
        controller: Controller,
        key: String,
        operation: String,
        force: bool,
    },
    Admit {
        key: String,
        writer: Writer,
        epoch: u64,
        unit: String,
    },
    Bind {
        key: String,
        writer: Writer,
        epoch: u64,
        unit: String,
        sha256: String,
    },
    Accept(Proposal),
    Final {
        table: String,
        version: i64,
        digest: String,
    },
    Activate {
        table: String,
        identity: Identity,
        version: i64,
    },
    Revoke {
        controller: Controller,
        collector: String,
    },
}

/// Backend supplies DB time or the keys observed in a single etcd revision.
pub struct Clock {
    pub now: i64,
    pub live: Option<BTreeSet<String>>,
    pub new_lease: i64,
}

pub fn controller_key() -> String {
    "/delta/live/controller".into()
}
pub fn member_key(id: &str) -> String {
    format!("/delta/live/member/{id}")
}

fn live(clock: &Clock, key: &str, expires: i64) -> bool {
    clock.live.as_ref().map_or(expires > clock.now, |l| l.contains(key))
}

fn control(state: &Snapshot, expected: &Controller, clock: &Clock) -> Result<()> {
    ensure!(
        state.controller.as_ref() == Some(expected) && live(clock, &controller_key(), state.controller_expires),
        "NOT_OWNER expired controller"
    );
    Ok(())
}

fn member(state: &Snapshot, writer: &Writer, clock: &Clock) -> Result<()> {
    let m = state
        .members
        .get(&writer.collector)
        .context("NOT_OWNER missing member")?;
    ensure!(
        m.member.writer == *writer && live(clock, &member_key(&writer.collector), m.expires),
        "NOT_OWNER expired process/local generation"
    );
    Ok(())
}

fn identity(c: &Controller, w: &Writer, epoch: u64) -> Result<Identity> {
    Ok(Identity {
        controller: c.id.clone(),
        controller_generation: c.generation,
        membership: w.collector.clone(),
        process: w.process.clone(),
        local_generation: w.generation.parse()?,
        epoch: i64::try_from(epoch)?,
    })
}

fn authorize(state: &Snapshot, table: &str, proposed: &Identity, clock: &Clock) -> Result<()> {
    let t = state.tables.get(table).context("unknown table")?;
    let c = state.controller.as_ref().context("missing controller")?;
    control(state, c, clock)?;
    member(state, &t.record.writer, clock)?;
    ensure!(
        &t.identity == proposed && proposed.controller == c.id && proposed.controller_generation == c.generation,
        "NOT_OWNER tuple"
    );
    Ok(())
}

/// Pure transition function shared by real SQL and etcd adapters; no key/value emulator.
///
/// # Errors
/// Rejects stale identities, expired sessions, conflicting operations and bounds.
pub fn apply(state: &mut Snapshot, command: &Command, clock: &Clock) -> Result<()> {
    let expires = clock.now + TTL_SECONDS as i64 * 1000;
    match command {
        Command::Start { id } => {
            if let Some(c) = &state.controller {
                if c.id == *id && live(clock, &controller_key(), state.controller_expires) {
                    return Ok(());
                }
                ensure!(
                    !live(clock, &controller_key(), state.controller_expires),
                    "controller already live"
                );
            }
            let c = Controller {
                id: id.clone(),
                generation: state.controller.as_ref().map_or(1, |c| c.generation + 1),
            };
            for t in state.tables.values_mut() {
                t.record.epoch += 1;
                t.identity = identity(&c, &t.record.writer, t.record.epoch)?;
                t.record.lifecycle = Lifecycle::Recovering;
                t.record.pending.clear();
            }
            state.controller = Some(c);
            state.controller_expires = expires;
            state.controller_lease = clock.new_lease;
        }
        Command::Renew { controller } => {
            control(state, controller, clock)?;
            state.controller_expires = expires;
        }
        Command::Register {
            controller,
            member: requested,
        } => {
            control(state, controller, clock)?;
            ensure!(
                requested.pressure.is_finite() && requested.rss_bytes > 0,
                "invalid pressure"
            );
            let mut requested = requested.clone();
            let id = requested.writer.collector.clone();
            let process = format!("{id}/{}", requested.writer.process);
            match state.members.get(&id) {
                Some(old) if live(clock, &member_key(&id), old.expires) => {
                    if requested.writer.generation.is_empty() && requested.writer.process == old.member.writer.process {
                        requested.writer.generation = old.member.writer.generation.clone();
                    }
                    ensure!(requested.writer == old.member.writer, "live process cannot be replaced");
                    state.members.insert(
                        id,
                        Session {
                            member: requested,
                            expires,
                            lease: old.lease,
                        },
                    );
                }
                old => {
                    ensure!(
                        requested.writer.generation.is_empty() && !state.retired.contains(&process),
                        "expired incarnation cannot revive"
                    );
                    ensure!(state.retired.len() < 1024, "session history bound");
                    requested.writer.generation = old
                        .map_or(1, |m| m.member.writer.generation.parse::<i64>().unwrap_or(0) + 1)
                        .to_string();
                    state.retired.insert(process);
                    state.members.insert(
                        id,
                        Session {
                            member: requested,
                            expires,
                            lease: clock.new_lease,
                        },
                    );
                }
            }
        }
        Command::Replace { controller, writer } => {
            control(state, controller, clock)?;
            member(state, writer, clock)?;
            let m = state.members.get_mut(&writer.collector).context("member")?;
            m.member.writer.generation = (writer.generation.parse::<i64>()? + 1).to_string();
            let successor = m.member.writer.clone();
            for t in state.tables.values_mut().filter(|t| t.record.writer == *writer) {
                t.record.writer = successor.clone();
                t.record.epoch += 1;
                t.record.lifecycle = Lifecycle::Recovering;
                t.record.pending.clear();
                t.identity = identity(controller, &successor, t.record.epoch)?;
            }
        }
        Command::Claim {
            controller,
            key,
            target,
            operation,
        } => {
            control(state, controller, clock)?;
            member(state, target, clock)?;
            if let Some(t) = state.tables.get(key) {
                ensure!(
                    t.record.writer == *target && t.reservation.as_ref().is_some_and(|r| r.operation == *operation),
                    "claim conflict"
                );
                return Ok(());
            }
            ensure!(state.tables.len() < 32, "table bound");
            let endpoint = state.members[&target.collector].member.endpoint.clone();
            state.tables.insert(
                key.clone(),
                TableState {
                    record: Record {
                        key: key.clone(),
                        epoch: 1,
                        writer: target.clone(),
                        endpoint,
                        lifecycle: Lifecycle::Active,
                        head: None,
                        pending: vec![],
                        migration: None,
                    },
                    identity: identity(controller, target, 1)?,
                    decisions: vec![],
                    published: -1,
                    reservation: Some(Reservation {
                        operation: operation.clone(),
                        target: target.clone(),
                    }),
                    inputs: BTreeMap::new(),
                },
            );
        }
        Command::Reserve {
            controller,
            key,
            target,
            operation,
        } => {
            control(state, controller, clock)?;
            member(state, target, clock)?;
            let reservations = state
                .tables
                .iter()
                .filter(|(k, t)| *k != key && t.record.lifecycle == Lifecycle::Draining)
                .map(|(_, t)| t)
                .collect::<Vec<_>>();
            ensure!(
                reservations.len() < 4
                    && reservations
                        .iter()
                        .filter(|t| t
                            .reservation
                            .as_ref()
                            .is_some_and(|r| r.target.collector == target.collector))
                        .count()
                        < 2,
                "reservation capacity"
            );
            let t = state.tables.get(key).context("unknown table")?;
            if t.reservation
                .as_ref()
                .is_some_and(|r| r.operation == *operation && r.target == *target)
            {
                return Ok(());
            }
            ensure!(
                t.record.lifecycle != Lifecycle::Draining
                    || t.reservation
                        .as_ref()
                        .is_some_and(|r| member(state, &r.target, clock).is_err()),
                "migration already reserved"
            );
            let t = state.tables.get_mut(key).context("unknown table")?;
            ensure!(t.record.writer != *target, "migration target is source");
            t.reservation = Some(Reservation {
                operation: operation.clone(),
                target: target.clone(),
            });
            t.record.lifecycle = Lifecycle::Draining;
        }
        Command::Transfer {
            controller,
            key,
            operation,
            force,
        } => {
            control(state, controller, clock)?;
            let t = state.tables.get(key).context("unknown table")?;
            let reservation = t.reservation.as_ref().context("missing reservation")?.clone();
            ensure!(reservation.operation == *operation, "migration operation mismatch");
            member(state, &reservation.target, clock)?;
            if t.record.writer == reservation.target && t.record.lifecycle != Lifecycle::Draining {
                return Ok(());
            }
            ensure!(
                t.record.pending.is_empty() || (*force && member(state, &t.record.writer, clock).is_err()),
                "drain awaits admitted work or authoritative source expiry"
            );
            let endpoint = state.members[&reservation.target.collector].member.endpoint.clone();
            let t = state.tables.get_mut(key).context("table")?;
            t.record.writer = reservation.target;
            t.record.endpoint = endpoint;
            t.record.epoch += 1;
            t.record.lifecycle = Lifecycle::Recovering;
            t.record.pending.clear();
            t.identity = identity(controller, &t.record.writer, t.record.epoch)?;
        }
        Command::Admit {
            key,
            writer,
            epoch,
            unit,
        } => {
            let i = state.identity(key, writer, *epoch)?;
            authorize(state, key, &i, clock)?;
            let t = state.tables.get_mut(key).context("table")?;
            ensure!(t.record.lifecycle == Lifecycle::Active, "NOT_OWNER not active");
            ensure!(
                t.decisions.len() < 128 && t.record.pending.len() < 16,
                "admission backpressure"
            );
            if !t.record.pending.contains(unit) {
                t.record.pending.push(unit.clone());
            }
        }
        Command::Bind {
            key,
            writer,
            epoch,
            unit,
            sha256,
        } => {
            let i = state.identity(key, writer, *epoch)?;
            authorize(state, key, &i, clock)?;
            let t = state.tables.get_mut(key).context("table")?;
            if let Some(old) = t.inputs.get(unit) {
                ensure!(old == sha256, "staged operation identity collision");
            } else {
                ensure!(t.record.pending.contains(unit), "input not admitted");
                ensure!(t.inputs.len() < 128, "input history bound");
                t.inputs.insert(unit.clone(), sha256.clone());
            }
        }
        Command::Accept(proposal) => {
            proposal.digest()?;
            let t = state.tables.get(&proposal.table).context("table")?;
            if let Some(old) = t.decisions.iter().find(|p| p.operation == proposal.operation) {
                ensure!(old == proposal, "operation collision");
                return Ok(());
            }
            authorize(state, &proposal.table, &proposal.identity, clock)?;
            let t = state.tables.get_mut(&proposal.table).context("table")?;
            ensure!(t.record.lifecycle != Lifecycle::Recovering, "NOT_OWNER recovering");
            let unit = proposal
                .operation
                .strip_suffix("-schema")
                .unwrap_or(&proposal.operation);
            ensure!(t.record.pending.iter().any(|u| u == unit), "operation not admitted");
            ensure!(
                proposal.version == t.decisions.len() as i64
                    && t.decisions.len() < 128
                    && proposal.version - t.published <= 16,
                "expected version/backpressure"
            );
            t.decisions.push(proposal.clone());
        }
        Command::Final { table, version, digest } => {
            let t = state.tables.get_mut(table).context("table")?;
            let p = t
                .decisions
                .get(usize::try_from(*version)?)
                .context("missing decision")?;
            ensure!(
                p.digest()? == *digest && *version <= t.published + 1,
                "receipt identity/prefix mismatch"
            );
            t.published = t.published.max(*version);
            if !p.operation.ends_with("-schema") {
                t.record.pending.retain(|u| *u != p.operation);
            }
        }
        Command::Activate {
            table,
            identity,
            version,
        } => {
            authorize(state, table, identity, clock)?;
            let t = state.tables.get_mut(table).context("table")?;
            ensure!(
                *version == t.decisions.len() as i64 - 1 && t.published == *version,
                "recovery prefix/receipt barrier"
            );
            t.record.lifecycle = Lifecycle::Active;
        }
        Command::Revoke { controller, collector } => {
            control(state, controller, clock)?;
            if let Some(m) = state.members.get_mut(collector) {
                m.expires = 0;
            }
        }
    }
    state.revision += 1;
    ensure!(
        serde_json::to_vec(state)?.len() <= STATE_LIMIT,
        "authority state byte backpressure"
    );
    Ok(())
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn snapshot(&self) -> Result<Snapshot>;
    async fn execute(&self, command: Command) -> Result<Snapshot>;
}
