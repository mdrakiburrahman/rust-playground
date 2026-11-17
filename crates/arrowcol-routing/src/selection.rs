use std::cmp::Ordering;

use arrowcol_protocol::{BasisPoints, CollectorInstanceId, UnixMillis};
use sha2::{Digest, Sha256};

use crate::{CollectorRecord, CollectorState, LeaseKey, RoutingPolicy, RoutingView};

/// Returns whether a collector is eligible for a new route.
#[must_use]
pub fn collector_is_eligible(
    collector: &CollectorRecord,
    now: UnixMillis,
    policy: &RoutingPolicy,
    require_catalog: bool,
) -> bool {
    if collector.state != CollectorState::Ready
        || !collector.registration.is_live_at(now)
        || (require_catalog && !collector.registration.catalog_capable)
    {
        return false;
    }
    let Some(pressure) = collector.pressure.as_ref() else {
        return false;
    };
    pressure.instance_id == collector.registration.instance_id
        && pressure.validate_at(now, &policy.pressure).is_ok()
        && pressure.accepting_deliveries
        && !pressure.subsystem_blocked()
        && pressure.maximum_pressure_bps_excluding(&policy.excluded_admission_gauges) < policy.hard_pressure_bps
}

/// Returns whether a fresh, registered holder is temporarily rejecting new work.
#[must_use]
pub fn collector_is_overloaded(collector: &CollectorRecord, now: UnixMillis, policy: &RoutingPolicy) -> bool {
    if collector.state != CollectorState::Ready || !collector.registration.is_live_at(now) {
        return false;
    }
    let Some(pressure) = collector.pressure.as_ref() else {
        return false;
    };
    pressure.instance_id == collector.registration.instance_id
        && pressure.validate_at(now, &policy.pressure).is_ok()
        && (!pressure.accepting_deliveries
            || pressure.subsystem_blocked()
            || pressure.maximum_pressure_bps_excluding(&policy.excluded_admission_gauges) >= policy.hard_pressure_bps)
}

/// Ranks eligible collectors using weighted rendezvous hashing.
///
/// The complete deterministic ordering provides stable fallback when the first
/// candidate races, drains, or becomes unavailable.
#[must_use]
pub fn rank_collectors(
    view: &RoutingView,
    key: &LeaseKey,
    now: UnixMillis,
    policy: &RoutingPolicy,
    require_catalog: bool,
) -> Vec<CollectorInstanceId> {
    let mut ranked: Vec<_> = view
        .collectors
        .values()
        .filter(|collector| collector.registration.implementation_pool == key.implementation_pool)
        .filter(|collector| collector_is_eligible(collector, now, policy, require_catalog))
        .map(|collector| {
            (
                rendezvous_score(key, collector, now, policy),
                collector.registration.instance_id,
            )
        })
        .collect();
    ranked.sort_by(|left, right| {
        left.0
            .partial_cmp(&right.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.1.cmp(&right.1))
    });
    ranked.into_iter().map(|(_, instance)| instance).collect()
}

fn rendezvous_score(key: &LeaseKey, collector: &CollectorRecord, now: UnixMillis, policy: &RoutingPolicy) -> f64 {
    let mut hasher = Sha256::new();
    hasher.update(key.implementation_pool.to_string().as_bytes());
    hasher.update([0]);
    hasher.update(key.table.to_string().as_bytes());
    hasher.update([0]);
    hasher.update([key.shard_index]);
    hasher.update([0]);
    hasher.update(collector.registration.instance_id.as_uuid().as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    let hash = u64::from_be_bytes(bytes);
    let uniform = (hash as f64 + 1.0) / (u64::MAX as f64 + 1.0);

    let pressure = collector.pressure.as_ref().map_or(BasisPoints::FULL_SCALE, |snapshot| {
        snapshot.maximum_pressure_bps_excluding(&policy.excluded_admission_gauges)
    });
    let pressure_weight =
        f64::from(BasisPoints::FULL_SCALE.saturating_sub(pressure).max(1)) / f64::from(BasisPoints::FULL_SCALE);
    let warmup_weight = warmup_bps(collector, now, policy) as f64 / f64::from(BasisPoints::FULL_SCALE);
    let capacity = collector.registration.max_concurrent_streams.get() as f64;
    let weight = (capacity * pressure_weight * warmup_weight).max(f64::EPSILON);
    -uniform.ln() / weight
}

fn warmup_bps(collector: &CollectorRecord, now: UnixMillis, policy: &RoutingPolicy) -> u16 {
    if policy.warmup_ms == 0 {
        return BasisPoints::FULL_SCALE;
    }
    let elapsed = now
        .get()
        .saturating_sub(collector.registration.started_at_unix_ms.get());
    let scaled = u128::from(elapsed.min(policy.warmup_ms)) * u128::from(BasisPoints::FULL_SCALE);
    let value = scaled / u128::from(policy.warmup_ms);
    u16::try_from(value)
        .unwrap_or(BasisPoints::FULL_SCALE)
        .max(policy.minimum_warmup_bps)
        .min(BasisPoints::FULL_SCALE)
}
