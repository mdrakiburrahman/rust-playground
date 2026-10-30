//! Shared finite placement policy; observations propose, transactions authorize.
use crate::{Writer, delta_store::Snapshot};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct Demand {
    pub cpu: f64,
    pub memory: f64,
    pub throughput: f64,
}

impl Default for Demand {
    fn default() -> Self {
        Self {
            cpu: 0.05,
            memory: 0.04,
            throughput: 0.05,
        }
    }
}

impl Demand {
    /// # Errors
    /// Rejects nonfinite or negative demand; observed overload remains observable.
    pub fn validate(self) -> Result<()> {
        ensure!(
            [self.cpu, self.memory, self.throughput]
                .iter()
                .all(|v| v.is_finite() && *v >= 0.0),
            "invalid normalized resource demand"
        );
        Ok(())
    }
    pub fn plus(self, other: Self) -> Self {
        Self {
            cpu: self.cpu + other.cpu,
            memory: self.memory + other.memory,
            throughput: self.throughput + other.throughput,
        }
    }
    pub fn pressure(self) -> f64 {
        self.cpu.max(self.memory).max(self.throughput)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Policy {
    pub revision: i64,
    pub excluded: bool,
    pub block_streams: bool,
    pub retiring: bool,
    pub approved: bool,
    pub applied_revision: i64,
    pub incarnation: Option<Writer>,
    pub operation: String,
    #[serde(default)]
    pub recovery_obligations: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation {
    pub demand: Demand,
    pub capacity: Demand,
    pub sequence: u64,
    #[serde(default)]
    pub allocation_revision: i64,
    pub tables: BTreeMap<String, Demand>,
    #[serde(default)]
    pub fingerprints: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stream {
    pub writer: Writer,
    pub epoch: u64,
    pub fingerprint: String,
    pub policy_revision: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TablePolicy {
    pub revision: i64,
    pub fingerprints: Vec<String>,
    pub demand: Demand,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Optimization {
    pub sampled_at: i64,
    pub sustained: u32,
    pub constraint: String,
}

pub fn optimization_candidate(state: &Snapshot, now: i64) -> Option<(String, Writer)> {
    let pressures = state
        .members
        .iter()
        .filter(|(_, member)| {
            member.expires > now
                && member.member.eligible
                && !state
                    .policy_for(&member.member.writer)
                    .is_some_and(|p| p.retiring || p.approved)
        })
        .filter_map(|(id, _)| state.projected(id, "").ok().map(|p| (id.clone(), p)))
        .collect::<BTreeMap<_, _>>();
    if pressures.len() < 2 {
        return None;
    }
    let variance = |values: Vec<f64>| {
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64
    };
    let before = variance(pressures.values().map(|p| p.pressure()).collect());
    let mut best: Option<(f64, String, Writer)> = None;
    for (key, table) in &state.tables {
        if table.record.lifecycle != crate::Lifecycle::Active || now - table.activated_at < 600_000 {
            continue;
        }
        let source = &table.record.writer.collector;
        if state.tables.values().any(|t| {
            t.reservation
                .as_ref()
                .is_some_and(|r| t.record.writer.collector == *source || r.target.collector == *source)
        }) {
            continue;
        }
        let demand = state.table_policies.get(key).map_or_else(Demand::default, |p| p.demand);
        for (target, member) in &state.members {
            if target == source || state.eligible(target, key).is_err() {
                continue;
            }
            if state.tables.values().any(|t| {
                t.reservation
                    .as_ref()
                    .is_some_and(|r| t.record.writer.collector == *target || r.target.collector == *target)
            }) {
                continue;
            }
            let Ok(destination) = state.projected(target, key) else {
                continue;
            };
            let mut values = pressures.clone();
            let Some(old) = values.get(source) else { continue };
            let Some(observation) = state.observations.get(source) else {
                continue;
            };
            let capacity = observation.capacity;
            values.insert(
                source.clone(),
                Demand {
                    cpu: (old.cpu - demand.cpu / capacity.cpu).max(0.0),
                    memory: (old.memory - demand.memory / capacity.memory).max(0.0),
                    throughput: (old.throughput - demand.throughput / capacity.throughput).max(0.0),
                },
            );
            values.insert(target.clone(), destination);
            let improvement = before - variance(values.values().map(|v| v.pressure()).collect());
            if improvement >= 0.01 && best.as_ref().is_none_or(|(benefit, _, _)| improvement > *benefit) {
                best = Some((improvement, key.clone(), member.member.writer.clone()));
            }
        }
    }
    best.map(|(_, key, writer)| (key, writer))
}

impl Snapshot {
    pub fn teardown_pending(&self, collector: &str) -> bool {
        self.policies.get(collector).is_some_and(|policy| policy.approved)
    }

    pub fn policy_for(&self, writer: &Writer) -> Option<&Policy> {
        self.policies
            .get(&writer.collector)
            .filter(|policy| policy.incarnation.as_ref() == Some(writer))
    }

    /// # Errors
    /// Rejects missing observations or unknown capacity.
    pub fn projected(&self, collector: &str, key: &str) -> Result<Demand> {
        let observation = self.observations.get(collector).context("capacity not observed")?;
        let mut demand = observation.demand;
        for (table_key, table) in &self.tables {
            let charged = table.record.writer.collector == collector
                || table
                    .reservation
                    .as_ref()
                    .is_some_and(|r| r.target.collector == collector);
            if charged
                && !(observation.tables.contains_key(table_key)
                    && table.record.writer.collector == collector
                    && table.record.lifecycle == crate::Lifecycle::Active
                    && observation.allocation_revision >= table.activation_revision)
            {
                demand = demand.plus(
                    self.table_policies
                        .get(table_key)
                        .map_or_else(Demand::default, |p| p.demand),
                );
            }
        }
        if !key.is_empty()
            && !self
                .tables
                .get(key)
                .is_some_and(|t| t.record.writer.collector == collector)
            && !self
                .tables
                .get(key)
                .and_then(|t| t.reservation.as_ref())
                .is_some_and(|r| r.target.collector == collector)
        {
            demand = demand.plus(self.table_policies.get(key).map_or_else(Demand::default, |p| p.demand));
        }
        let c = observation.capacity;
        ensure!(c.cpu > 0.0 && c.memory > 0.0 && c.throughput > 0.0, "unknown capacity");
        let projected = Demand {
            cpu: demand.cpu / c.cpu,
            memory: demand.memory / c.memory,
            throughput: demand.throughput / c.throughput,
        };
        projected.validate()?;
        Ok(projected)
    }

    /// # Errors
    /// Rejects unavailable, excluded, incompatible or over-budget destinations.
    pub fn eligible(&self, collector: &str, key: &str) -> Result<()> {
        let member = self.members.get(collector).context("missing member")?;
        ensure!(member.expires > 0 && member.member.eligible, "member unavailable");
        ensure!(member.member.active < 16, "writer stream slots exhausted");
        ensure!(
            !self.teardown_pending(collector)
                && !self
                    .policy_for(&member.member.writer)
                    .is_some_and(|p| p.excluded || p.retiring || p.approved),
            "placement excluded"
        );
        if let Some(policy) = self.table_policies.get(key) {
            let observation = self.observations.get(collector).context("capabilities unknown")?;
            ensure!(
                policy.fingerprints.iter().all(|f| observation.fingerprints.contains(f)),
                "schema capability unavailable"
            );
        }
        ensure!(
            self.projected(collector, key)?.pressure() <= 0.80,
            "insufficient projected capacity"
        );
        Ok(())
    }
}

/// Stable per-table tie breaking avoids sending every cold table to one ID.
///
/// # Errors
/// Returns backpressure when no eligible destination has safe capacity.
pub fn select(state: &Snapshot, key: &str, excluded: Option<&str>) -> Result<Writer> {
    use sha2::{Digest, Sha256};
    let mut candidates = state
        .members
        .values()
        .filter(|m| {
            Some(m.member.writer.collector.as_str()) != excluded
                && state.eligible(&m.member.writer.collector, key).is_ok()
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| {
        let score = |id: &str| state.projected(id, key).map(|p| p.pressure()).unwrap_or(f64::INFINITY);
        score(&a.member.writer.collector)
            .total_cmp(&score(&b.member.writer.collector))
            .then_with(|| {
                let hash = |id: &str| Sha256::digest(format!("{key}\n{id}").as_bytes());
                hash(&a.member.writer.collector).cmp(&hash(&b.member.writer.collector))
            })
    });
    Ok(candidates
        .first()
        .context("no fresh eligible capacity")?
        .member
        .writer
        .clone())
}
