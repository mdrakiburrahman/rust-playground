//! Observational ownership accounting. These counters never authorize work.
use serde::Serialize;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Debug, Default)]
pub struct Gauge(Arc<Counts>);
#[derive(Debug, Default)]
struct Counts {
    current: AtomicUsize,
    high_water: AtomicUsize,
}
#[derive(Debug, Serialize)]
pub struct Sample {
    pub current: usize,
    pub lifetime_high_water: usize,
}
#[derive(Debug)]
pub struct Held {
    gauge: Gauge,
    amount: usize,
}
impl Gauge {
    pub fn hold(&self, amount: usize) -> Held {
        let current = self.0.current.fetch_add(amount, Ordering::Relaxed) + amount;
        self.0.high_water.fetch_max(current, Ordering::Relaxed);
        Held {
            gauge: self.clone(),
            amount,
        }
    }
    pub fn sample(&self) -> Sample {
        Sample {
            current: self.0.current.load(Ordering::Relaxed),
            lifetime_high_water: self.0.high_water.load(Ordering::Relaxed),
        }
    }
}
impl Drop for Held {
    fn drop(&mut self) {
        self.gauge.0.current.fetch_sub(self.amount, Ordering::Relaxed);
    }
}
#[derive(Default)]
pub struct Counters {
    pub gateway_input_bytes: Gauge,
    pub gateway_result_bytes: Gauge,
    pub mailbox_waiters: Gauge,
    pub writer_streams: Gauge,
    pub decoded_batch_bytes: Gauge,
}
