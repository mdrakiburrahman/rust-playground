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
    #[serde(default)]
    pub streams: BTreeMap<String, crate::placement::Stream>,
    #[serde(default)]
    pub activated_at: i64,
    #[serde(default)]
    pub input_policy: BTreeMap<String, (i64, String)>,
    #[serde(default)]
    pub claim_operation: String,
    #[serde(default)]
    pub activation_revision: i64,
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
    #[serde(default)]
    pub policies: BTreeMap<String, crate::placement::Policy>,
    #[serde(default)]
    pub observations: BTreeMap<String, crate::placement::Observation>,
    #[serde(default)]
    pub observed_at: BTreeMap<String, i64>,
    #[serde(default)]
    pub table_policies: BTreeMap<String, crate::placement::TablePolicy>,
    #[serde(default)]
    pub optimization: crate::placement::Optimization,
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
    Optimize {
        controller: Controller,
    },
    Catalog {
        controller: Controller,
        key: String,
        policy: crate::placement::TablePolicy,
    },
    Policy {
        controller: Controller,
        collector: String,
        writer: Writer,
        revision: i64,
        operation: String,
        excluded: bool,
        block_streams: bool,
        retiring: bool,
    },
    Observe {
        writer: Writer,
        observation: crate::placement::Observation,
        applied_revision: i64,
    },
    OpenStream {
        key: String,
        writer: Writer,
        epoch: u64,
        stream: String,
        fingerprint: String,
    },
    CloseStream {
        key: String,
        writer: Writer,
        stream: String,
    },
    AdmitStream {
        key: String,
        writer: Writer,
        epoch: u64,
        stream: String,
        unit: String,
    },
    ApproveRetirement {
        controller: Controller,
        writer: Writer,
        revision: i64,
        operation: String,
    },
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

fn rebind_policy(state: &mut Snapshot, writer: &Writer) -> Result<()> {
    if let Some(policy) = state.policies.get_mut(&writer.collector) {
        ensure!(!policy.approved, "execution resource awaits confirmed removal");
        policy.revision = policy.revision.checked_add(1).context("policy revision exhausted")?;
        policy.incarnation = Some(writer.clone());
        policy.applied_revision = 0;
    }
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
        Command::Optimize { controller } => {
            control(state, controller, clock)?;
            if clock.now - state.optimization.sampled_at < 30_000 {
                return Ok(());
            }
            state.optimization.sampled_at = clock.now;
            if state
                .members
                .iter()
                .any(|(id, m)| m.expires > 0 && !state.observed_at.get(id).is_some_and(|at| clock.now - at <= 3000))
            {
                state.optimization.sustained = 0;
                state.optimization.constraint = "stale-capacity".into();
                return Ok(());
            }
            let pressures = state
                .members
                .keys()
                .filter_map(|id| state.projected(id, "").ok().map(|p| p.pressure()))
                .collect::<Vec<_>>();
            let max = pressures.iter().copied().fold(0.0, f64::max);
            let min = pressures.iter().copied().fold(1.0, f64::min);
            if max <= 0.70 && max - min <= 0.15 {
                state.optimization.sustained = 0;
            } else if max > 0.80 || max - min > 0.25 {
                state.optimization.sustained += 1;
            } else {
                state.optimization.sustained = 0;
            }
            if state.optimization.sustained >= 3 {
                state.optimization.constraint = "no-beneficial-safe-move".into();
                for _ in 0..2 {
                    let Some((key, target)) = crate::placement::optimization_candidate(state, clock.now) else {
                        break;
                    };
                    apply(
                        state,
                        &Command::Reserve {
                            controller: controller.clone(),
                            key,
                            target,
                            operation: uuid::Uuid::new_v4().to_string(),
                        },
                        clock,
                    )?;
                    state.optimization.constraint = "reserved".into();
                }
            }
        }
        Command::Catalog {
            controller,
            key,
            policy,
        } => {
            control(state, controller, clock)?;
            policy.demand.validate()?;
            ensure!(
                !policy.fingerprints.is_empty() && policy.fingerprints.len() <= 32,
                "schema policy bound"
            );
            ensure!(
                state.table_policies.len() < 32 || state.table_policies.contains_key(key),
                "catalog bound"
            );
            if let Some(old) = state.table_policies.get(key) {
                ensure!(
                    policy.revision > old.revision
                        || (policy.revision == old.revision
                            && policy.fingerprints == old.fingerprints
                            && policy.demand == old.demand),
                    "stale schema policy"
                );
            }
            state.table_policies.insert(key.clone(), policy.clone());
        }
        Command::Policy {
            controller,
            collector,
            writer,
            revision,
            operation,
            excluded,
            block_streams,
            retiring,
        } => {
            control(state, controller, clock)?;
            member(state, writer, clock)?;
            ensure!(
                writer.collector == *collector && crate::safe_id(operation),
                "policy identity"
            );
            let old = state.policies.get(collector).cloned().unwrap_or_default();
            if old.operation == *operation {
                ensure!(
                    old.incarnation.as_ref() == Some(writer)
                        && old.excluded == (*excluded || *retiring)
                        && old.block_streams == *block_streams
                        && old.retiring == *retiring,
                    "policy operation collision"
                );
                return Ok(());
            }
            ensure!(
                old.revision == *revision
                    && !old.approved
                    && (old.incarnation.as_ref() != Some(writer) || !old.retiring || *retiring),
                "policy revision/retirement conflict"
            );
            state.policies.insert(
                collector.clone(),
                crate::placement::Policy {
                    revision: revision + 1,
                    excluded: *excluded || *retiring,
                    block_streams: *block_streams,
                    retiring: *retiring,
                    approved: false,
                    applied_revision: if old.incarnation.as_ref() == Some(writer) {
                        old.applied_revision
                    } else {
                        0
                    },
                    incarnation: Some(writer.clone()),
                    operation: operation.clone(),
                    recovery_obligations: if *retiring {
                        old.recovery_obligations
                            .into_iter()
                            .chain(
                                state
                                    .tables
                                    .iter()
                                    .filter(|(_, t)| t.record.writer == *writer)
                                    .map(|(key, _)| key.clone()),
                            )
                            .collect()
                    } else {
                        BTreeSet::new()
                    },
                },
            );
        }
        Command::Observe {
            writer,
            observation,
            applied_revision,
        } => {
            member(state, writer, clock)?;
            observation.demand.validate()?;
            observation.capacity.validate()?;
            ensure!(
                observation.allocation_revision >= 0 && observation.allocation_revision <= state.revision,
                "invalid allocation watermark"
            );
            ensure!(observation.tables.len() <= 32, "table observation bound");
            ensure!(
                observation.fingerprints.len() <= 128 && observation.fingerprints.iter().all(|f| f.len() <= 128),
                "capability bound"
            );
            for (key, demand) in &observation.tables {
                demand.validate()?;
                ensure!(
                    state.tables.get(key).is_some_and(|t| t.record.writer == *writer),
                    "foreign table observation"
                );
            }
            ensure!(
                state
                    .observations
                    .get(&writer.collector)
                    .is_none_or(|o| observation.sequence > o.sequence),
                "stale observation"
            );
            if let Some(policy) = state
                .policies
                .get_mut(&writer.collector)
                .filter(|p| p.incarnation.as_ref() == Some(writer))
            {
                ensure!(*applied_revision == policy.revision, "policy not applied");
                policy.applied_revision = *applied_revision;
            } else {
                ensure!(*applied_revision == 0, "foreign policy acknowledgement");
            }
            state.observations.insert(writer.collector.clone(), observation.clone());
            state.observed_at.insert(writer.collector.clone(), clock.now);
        }
        Command::OpenStream {
            key,
            writer,
            epoch,
            stream,
            fingerprint,
        } => {
            let i = state.identity(key, writer, *epoch)?;
            authorize(state, key, &i, clock)?;
            ensure!(crate::safe_id(stream), "invalid stream");
            let p = state.policy_for(writer).cloned().unwrap_or_default();
            ensure!(
                !state.teardown_pending(&writer.collector) && !p.block_streams && !p.retiring,
                "new stream admission blocked"
            );
            let schema = state.table_policies.get(key).context("missing schema policy")?;
            ensure!(schema.fingerprints.contains(fingerprint), "schema policy rejected");
            ensure!(
                state
                    .observations
                    .get(&writer.collector)
                    .is_some_and(|o| o.fingerprints.contains(fingerprint)),
                "writer lacks schema capability"
            );
            let t = state.tables.get_mut(key).context("table")?;
            if let Some(existing) = t.streams.get(stream) {
                ensure!(
                    existing.writer == *writer
                        && existing.epoch == *epoch
                        && existing.fingerprint == *fingerprint
                        && existing.policy_revision == schema.revision,
                    "stream identity collision"
                );
                return Ok(());
            }
            ensure!(
                t.record.lifecycle == Lifecycle::Active && t.streams.len() < 16,
                "stream backpressure/not active"
            );
            t.streams.insert(
                stream.clone(),
                crate::placement::Stream {
                    writer: writer.clone(),
                    epoch: *epoch,
                    fingerprint: fingerprint.clone(),
                    policy_revision: schema.revision,
                },
            );
        }
        Command::CloseStream { key, writer, stream } => {
            if let Some(t) = state.tables.get_mut(key) {
                if t.streams.get(stream).is_some_and(|s| s.writer == *writer) {
                    t.streams.remove(stream);
                }
            }
        }
        Command::AdmitStream {
            key,
            writer,
            epoch,
            stream,
            unit,
        } => {
            let t = state.tables.get(key).context("table")?;
            let s = t.streams.get(stream).context("stream not admitted")?;
            ensure!(s.writer == *writer && s.epoch == *epoch, "stream lineage");
            let p = state.table_policies.get(key).context("schema policy")?;
            ensure!(
                p.revision == s.policy_revision && p.fingerprints.contains(&s.fingerprint),
                "schema policy changed"
            );
            let policy = (p.revision, s.fingerprint.clone());
            apply(
                state,
                &Command::Admit {
                    key: key.clone(),
                    writer: writer.clone(),
                    epoch: *epoch,
                    unit: unit.clone(),
                },
                clock,
            )?;
            state
                .tables
                .get_mut(key)
                .context("table")?
                .input_policy
                .insert(unit.clone(), policy);
        }
        Command::ApproveRetirement {
            controller,
            writer,
            revision,
            operation,
        } => {
            control(state, controller, clock)?;
            let collector = &writer.collector;
            ensure!(
                state.members.get(collector).is_some_and(|m| m.member.writer == *writer),
                "retirement incarnation changed"
            );
            ensure!(
                !state.tables.values().any(|t| t.record.writer.collector == *collector
                    || t.reservation.as_ref().is_some_and(|r| r.target.collector == *collector)),
                "retirement still owns work"
            );
            let p = state.policies.get_mut(collector).context("retirement missing")?;
            ensure!(
                p.incarnation.as_ref() == Some(writer)
                    && p.revision == *revision
                    && p.operation == *operation
                    && p.retiring
                    && p.applied_revision == p.revision
                    && p.recovery_obligations.is_empty(),
                "retirement awaits applied policy/recovered successors"
            );
            p.approved = true;
        }
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
                t.streams.clear();
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
                        !state.teardown_pending(&id),
                        "execution resource awaits confirmed removal"
                    );
                    state.observations.remove(&id);
                    state.observed_at.remove(&id);
                    ensure!(
                        requested.writer.generation.is_empty() && !state.retired.contains(&process),
                        "expired incarnation cannot revive"
                    );
                    ensure!(state.retired.len() < 1024, "session history bound");
                    requested.writer.generation = old
                        .map_or(1, |m| m.member.writer.generation.parse::<i64>().unwrap_or(0) + 1)
                        .to_string();
                    rebind_policy(state, &requested.writer)?;
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
            ensure!(
                !state.teardown_pending(&writer.collector),
                "execution resource awaits confirmed removal"
            );
            let m = state.members.get_mut(&writer.collector).context("member")?;
            m.member.writer.generation = (writer.generation.parse::<i64>()? + 1).to_string();
            let successor = m.member.writer.clone();
            rebind_policy(state, &successor)?;
            state.observations.remove(&writer.collector);
            state.observed_at.remove(&writer.collector);
            for t in state.tables.values_mut().filter(|t| t.record.writer == *writer) {
                t.record.writer = successor.clone();
                t.record.epoch += 1;
                t.record.lifecycle = Lifecycle::Recovering;
                t.record.pending.clear();
                t.streams.clear();
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
                    t.record.writer == *target && t.claim_operation == *operation,
                    "claim conflict"
                );
                return Ok(());
            }
            ensure!(
                state
                    .observed_at
                    .get(&target.collector)
                    .is_some_and(|at| clock.now - at <= 3000),
                "stale capacity"
            );
            state.eligible(&target.collector, key)?;
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
                        lifecycle: Lifecycle::Recovering,
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
                    streams: BTreeMap::new(),
                    activated_at: 0,
                    input_policy: BTreeMap::new(),
                    claim_operation: operation.clone(),
                    activation_revision: 0,
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
            ensure!(
                state
                    .observed_at
                    .get(&target.collector)
                    .is_some_and(|at| clock.now - at <= 3000),
                "stale capacity"
            );
            state.eligible(&target.collector, key)?;
            let reservations = state
                .tables
                .iter()
                .filter(|(k, t)| *k != key && t.reservation.is_some())
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
                        .is_some_and(|r| member(state, &r.target, clock).is_err()
                            || state.policy_for(&r.target).is_some_and(|p| p.excluded || p.retiring)),
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
            state.eligible(&reservation.target.collector, key)?;
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
            t.streams.clear();
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
            let (revision, fingerprint) = t
                .input_policy
                .get(unit)
                .context("publication requires revisioned stream admission")?;
            let policy = state
                .table_policies
                .get(&proposal.table)
                .context("publication schema policy")?;
            ensure!(
                policy.revision == *revision && policy.fingerprints.contains(fingerprint),
                "publication policy changed"
            );
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
            let t = state.tables.get(table).context("table")?;
            ensure!(
                *version == t.decisions.len() as i64 - 1 && t.published == *version,
                "recovery prefix/receipt barrier"
            );
            if t.record.lifecycle == Lifecycle::Active {
                return Ok(());
            }
            ensure!(
                t.record.lifecycle == Lifecycle::Recovering,
                "activation requires recovery"
            );
            let collector = &t.record.writer.collector;
            ensure!(
                state
                    .observed_at
                    .get(collector)
                    .is_some_and(|at| clock.now - at <= 3000),
                "recovery requires fresh capability observation"
            );
            let schema = state
                .table_policies
                .get(table)
                .context("recovery schema policy missing")?;
            let observation = state
                .observations
                .get(collector)
                .context("recovery capabilities missing")?;
            ensure!(
                schema
                    .fingerprints
                    .iter()
                    .all(|fp| observation.fingerprints.contains(fp)),
                "recovery schema capability unavailable"
            );
            ensure!(
                !state.teardown_pending(collector) && !state.policy_for(&t.record.writer).is_some_and(|p| p.retiring),
                "retiring successor cannot activate"
            );
            let t = state.tables.get_mut(table).context("table")?;
            t.record.lifecycle = Lifecycle::Active;
            t.reservation = None;
            t.activated_at = clock.now;
            t.activation_revision = state.revision + 1;
            for policy in state.policies.values_mut() {
                policy.recovery_obligations.remove(table);
            }
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
