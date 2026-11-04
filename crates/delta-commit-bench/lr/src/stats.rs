//! Latency and RSS summaries.

use hdrhistogram::Histogram;
use serde_json::{Value, json};
use std::time::Duration;

pub struct Summary {
    hist: Histogram<u64>,
    rss_first: Option<f64>,
    rss_last: f64,
    rss_max: f64,
}

impl Default for Summary {
    fn default() -> Self {
        Self { hist: Histogram::new(3).expect("histogram"), rss_first: None, rss_last: 0.0, rss_max: 0.0 }
    }
}

impl Summary {
    pub fn record(&mut self, d: Duration, rss_mib: f64) {
        let _ = self.hist.record(d.as_micros().max(1) as u64);
        self.rss_first.get_or_insert(rss_mib);
        self.rss_last = rss_mib;
        self.rss_max = self.rss_max.max(rss_mib);
    }

    pub fn merge(&mut self, o: &Summary) {
        let _ = self.hist.add(&o.hist);
        if self.rss_first.is_none() {
            self.rss_first = o.rss_first;
        }
        self.rss_last = self.rss_last.max(o.rss_last);
        self.rss_max = self.rss_max.max(o.rss_max);
    }

    pub fn json(&self) -> Value {
        let ms = |q: f64| self.hist.value_at_quantile(q) as f64 / 1000.0;
        json!({"n": self.hist.len(), "p50_ms": ms(0.5), "p99_ms": ms(0.99), "max_ms": self.hist.max() as f64 / 1000.0,
            "mean_ms": self.hist.mean() / 1000.0, "rss_first_mib": self.rss_first, "rss_last_mib": self.rss_last,
            "rss_max_mib": self.rss_max})
    }
}
