//! Opt-in shared-authority adaptation of router-hosted least-pressure placement.
//! Disk intents are no longer ownership grants in this mode.

/// Keep reservation pressure in the same units as observed collector pressure.
#[must_use]
pub fn reserved_pressure(pressure: f64, reservations: usize, capacity: usize) -> f64 {
    pressure + reservations as f64 / capacity.max(1) as f64
}
