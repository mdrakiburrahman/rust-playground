use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use tokio::sync::Notify;

/// Per-table convergence state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Not yet reconciled since process start.
    #[default]
    Starting,
    /// Kusto holds exactly the latest observed committed snapshot.
    Healthy,
    /// A newer committed snapshot is being applied.
    CatchingUp,
    /// The bridge cannot keep this table coherent; see `reason`.
    Degraded,
}

/// One bridged table as reported by `/status`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct TableStatus {
    pub pool: String,
    pub table: String,
    pub kusto_table: String,
    pub state: State,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Latest committed Delta version observed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_version: Option<u64>,
    /// Delta version whose active-file set Kusto currently holds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_version: Option<u64>,
    pub lag_versions: u64,
    /// Seconds since the oldest committed-but-not-visible version was first observed.
    pub visibility_lag_seconds: f64,
    pub active_files: u64,
    pub pending_files: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_at_unix_ms: Option<u64>,
    #[serde(skip)]
    pub behind_since: Option<SystemTime>,
}

impl TableStatus {
    fn refresh_lag(&mut self, now: SystemTime) {
        self.lag_versions = match (self.source_version, self.applied_version) {
            (Some(source), Some(applied)) => source.saturating_sub(applied),
            (Some(source), None) => source + 1,
            _ => 0,
        };
        if self.lag_versions == 0 && self.state != State::Degraded {
            self.behind_since = None;
        } else if self.behind_since.is_none() {
            self.behind_since = Some(now);
        }
        self.visibility_lag_seconds = self
            .behind_since
            .and_then(|since| now.duration_since(since).ok())
            .map_or(0.0, |lag| lag.as_secs_f64());
    }
}

/// Bounded label values for counters.
pub mod labels {
    pub const OUTCOMES: [&str; 3] = ["applied", "noop", "failed"];
    pub const REMOVALS: [&str; 2] = ["replaced", "deleted"];
    pub const RECONCILED: [&str; 3] = ["already_present", "staging_discarded", "unknown_resolved"];
    pub const OPERATIONS: [&str; 5] = ["log_read", "ingest", "swap", "checkpoint", "schema"];
}

const BUCKETS: [f64; 10] = [0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 120.0];

#[derive(Default)]
struct Histogram {
    counts: [u64; BUCKETS.len()],
    count: u64,
    sum: f64,
}

#[derive(Default)]
struct Registry {
    tables: BTreeMap<(String, String), TableStatus>,
    counters: BTreeMap<(&'static str, String, &'static str), u64>,
    refresh: BTreeMap<String, Histogram>,
    pass: u64,
    ready: bool,
}

/// Shared state between the reconciler and the HTTP server.
#[derive(Clone, Default)]
pub struct Status {
    inner: Arc<Mutex<Registry>>,
    changed: Arc<Notify>,
}

impl Status {
    fn lock(&self) -> MutexGuard<'_, Registry> {
        // A panic while holding the lock leaves only metrics behind; keep serving them.
        self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Insert or replace one table and recompute its lag.
    pub fn update(&self, mut table: TableStatus) {
        let now = SystemTime::now();
        let mut registry = self.lock();
        if let Some(previous) = registry.tables.get(&(table.pool.clone(), table.table.clone())) {
            table.behind_since = table.behind_since.or(previous.behind_since);
        }
        table.refresh_lag(now);
        registry.tables.insert((table.pool.clone(), table.table.clone()), table);
    }

    /// Remove tables whose Delta root disappeared from a source.
    pub fn retain(&self, keep: impl Fn(&str, &str) -> bool) {
        self.lock().tables.retain(|(pool, table), _| keep(pool, table));
    }

    /// Current state of one table.
    #[must_use]
    pub fn table(&self, pool: &str, table: &str) -> Option<TableStatus> {
        self.lock().tables.get(&(pool.to_owned(), table.to_owned())).cloned()
    }

    /// Add to a bounded counter.
    pub fn count(&self, metric: &'static str, pool: &str, label: &'static str, by: u64) {
        if by > 0 {
            *self
                .lock()
                .counters
                .entry((metric, pool.to_owned(), label))
                .or_default() += by;
        }
    }

    /// Record one reconcile pass for a pool.
    pub fn observe_refresh(&self, pool: &str, outcome: &'static str, duration: Duration) {
        let mut registry = self.lock();
        *registry
            .counters
            .entry(("refresh_total", pool.to_owned(), outcome))
            .or_default() += 1;
        let histogram = registry.refresh.entry(pool.to_owned()).or_default();
        let seconds = duration.as_secs_f64();
        for (bucket, limit) in histogram.counts.iter_mut().zip(BUCKETS) {
            if seconds <= limit {
                *bucket += 1;
            }
        }
        histogram.count += 1;
        histogram.sum += seconds;
    }

    /// Mark a whole reconcile pass complete and wake `/refresh` waiters.
    pub fn finish_pass(&self) {
        {
            let mut registry = self.lock();
            registry.pass += 1;
            registry.ready = true;
            let now = SystemTime::now();
            for table in registry.tables.values_mut() {
                table.refresh_lag(now);
            }
        }
        self.changed.notify_waiters();
    }

    /// Completed reconcile passes.
    #[must_use]
    pub fn pass(&self) -> u64 {
        self.lock().pass
    }

    /// Whether at least one pass completed.
    #[must_use]
    pub fn ready(&self) -> bool {
        self.lock().ready
    }

    /// Wait for the next completed pass.
    pub async fn changed(&self) {
        self.changed.notified().await;
    }

    /// JSON body for `/status`.
    #[must_use]
    pub fn snapshot(&self) -> serde_json::Value {
        let registry = self.lock();
        let tables: Vec<_> = registry.tables.values().cloned().collect();
        let degraded = tables.iter().filter(|table| table.state == State::Degraded).count();
        let converged = registry.ready
            && tables
                .iter()
                .all(|table| table.state == State::Healthy && table.lag_versions == 0);
        serde_json::json!({
            "ready": registry.ready,
            "converged": converged,
            "passes": registry.pass,
            "degraded_tables": degraded,
            "at_unix_ms": unix_ms(SystemTime::now()),
            "tables": tables,
        })
    }

    /// Prometheus text exposition for `/metrics`. Labels are limited to pool, outcome, reason,
    /// and operation; table names stay in `/status` so cardinality is bounded by the source count.
    #[must_use]
    pub fn prometheus(&self) -> String {
        let registry = self.lock();
        let mut pools: BTreeMap<&str, Vec<&TableStatus>> = BTreeMap::new();
        for table in registry.tables.values() {
            pools.entry(table.pool.as_str()).or_default().push(table);
        }
        let mut out = String::new();
        let gauge = |out: &mut String, name: &str, help: &str, values: Vec<(&str, f64)>| {
            let _ = writeln!(
                out,
                "# HELP kusto_bridge_{name} {help}\n# TYPE kusto_bridge_{name} gauge"
            );
            for (pool, value) in values {
                let _ = writeln!(out, "kusto_bridge_{name}{{pool=\"{pool}\"}} {value}");
            }
        };
        let per_pool = |f: &dyn Fn(&[&TableStatus]) -> f64| {
            pools
                .iter()
                .map(|(pool, tables)| (*pool, f(tables)))
                .collect::<Vec<_>>()
        };
        #[allow(clippy::cast_precision_loss)]
        let as_f64 = |value: u64| value as f64;
        gauge(
            &mut out,
            "up",
            "1 after the first completed reconcile pass.",
            vec![("all", if registry.ready { 1.0 } else { 0.0 })],
        );
        gauge(
            &mut out,
            "tables",
            "Bridged Delta tables.",
            per_pool(&|tables| as_f64(tables.len() as u64)),
        );
        gauge(
            &mut out,
            "source_version",
            "Highest committed Delta version observed across the pool's tables.",
            per_pool(&|tables| as_f64(tables.iter().filter_map(|t| t.source_version).max().unwrap_or(0))),
        );
        gauge(
            &mut out,
            "applied_version",
            "Lowest Delta version visible in Kusto across the pool's tables (-1 when none).",
            per_pool(&|tables| {
                tables
                    .iter()
                    .map(|t| t.applied_version.map_or(-1.0, as_f64))
                    .fold(f64::INFINITY, f64::min)
            }),
        );
        gauge(
            &mut out,
            "lag_versions",
            "Committed Delta versions not yet visible in Kusto, summed over tables.",
            per_pool(&|tables| as_f64(tables.iter().map(|t| t.lag_versions).sum())),
        );
        gauge(
            &mut out,
            "visibility_lag_seconds",
            "Oldest age of a committed Delta version that Kusto does not show yet.",
            per_pool(&|tables| tables.iter().map(|t| t.visibility_lag_seconds).fold(0.0, f64::max)),
        );
        gauge(
            &mut out,
            "active_files",
            "Active Delta files in the observed snapshots.",
            per_pool(&|tables| as_f64(tables.iter().map(|t| t.active_files).sum())),
        );
        gauge(
            &mut out,
            "pending_files",
            "Active Delta files not yet visible in Kusto.",
            per_pool(&|tables| as_f64(tables.iter().map(|t| t.pending_files).sum())),
        );
        gauge(
            &mut out,
            "degraded_tables",
            "Tables the bridge cannot keep coherent.",
            per_pool(&|tables| as_f64(tables.iter().filter(|t| t.state == State::Degraded).count() as u64)),
        );
        let counters: [(&str, &str, &str, &[&str]); 7] = [
            (
                "refresh_total",
                "Reconcile passes by outcome.",
                "outcome",
                &labels::OUTCOMES,
            ),
            ("files_ingested_total", "Delta files ingested into staging.", "", &[""]),
            ("rows_ingested_total", "Rows ingested into staging.", "", &[""]),
            (
                "generations_published_total",
                "Atomic snapshot swaps published.",
                "",
                &[""],
            ),
            (
                "files_removed_total",
                "Delta files dropped from Kusto.",
                "reason",
                &labels::REMOVALS,
            ),
            (
                "reconciled_total",
                "Recovery decisions after restarts or unknown outcomes.",
                "outcome",
                &labels::RECONCILED,
            ),
            ("failures_total", "Failed operations.", "operation", &labels::OPERATIONS),
        ];
        let mut known_pools: Vec<String> = pools.keys().map(|pool| (*pool).to_owned()).collect();
        for (_, pool, _) in registry.counters.keys() {
            if !known_pools.contains(pool) {
                known_pools.push(pool.clone());
            }
        }
        for (name, help, label, values) in counters {
            let _ = writeln!(
                out,
                "# HELP kusto_bridge_{name} {help}\n# TYPE kusto_bridge_{name} counter"
            );
            for pool in &known_pools {
                for value in values {
                    let count = registry
                        .counters
                        .iter()
                        .find(|((metric, p, l), _)| *metric == name && p == pool && l == value)
                        .map_or(0, |(_, count)| *count);
                    if label.is_empty() {
                        let _ = writeln!(out, "kusto_bridge_{name}{{pool=\"{pool}\"}} {count}");
                    } else {
                        let _ = writeln!(
                            out,
                            "kusto_bridge_{name}{{pool=\"{pool}\",{label}=\"{value}\"}} {count}"
                        );
                    }
                }
            }
        }
        let _ = writeln!(
            out,
            "# HELP kusto_bridge_refresh_duration_seconds Reconcile pass duration.\n# TYPE kusto_bridge_refresh_duration_seconds histogram"
        );
        for (pool, histogram) in &registry.refresh {
            for (count, limit) in histogram.counts.iter().zip(BUCKETS) {
                let _ = writeln!(
                    out,
                    "kusto_bridge_refresh_duration_seconds_bucket{{pool=\"{pool}\",le=\"{limit}\"}} {count}"
                );
            }
            let _ = writeln!(
                out,
                "kusto_bridge_refresh_duration_seconds_bucket{{pool=\"{pool}\",le=\"+Inf\"}} {}\nkusto_bridge_refresh_duration_seconds_sum{{pool=\"{pool}\"}} {}\nkusto_bridge_refresh_duration_seconds_count{{pool=\"{pool}\"}} {}",
                histogram.count, histogram.sum, histogram.count
            );
        }
        out
    }
}

/// Milliseconds since the Unix epoch.
#[must_use]
pub fn unix_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_are_bounded_and_lag_is_tracked() {
        let status = Status::default();
        status.update(TableStatus {
            pool: "direct".into(),
            table: "t".into(),
            kusto_table: "direct_t".into(),
            state: State::CatchingUp,
            source_version: Some(5),
            applied_version: Some(3),
            active_files: 4,
            pending_files: 1,
            ..TableStatus::default()
        });
        status.count("files_ingested_total", "direct", "", 2);
        status.count("files_removed_total", "direct", "replaced", 3);
        status.observe_refresh("direct", "applied", Duration::from_millis(300));
        status.finish_pass();
        let text = status.prometheus();
        assert!(text.contains("kusto_bridge_lag_versions{pool=\"direct\"} 2"));
        assert!(text.contains("kusto_bridge_files_removed_total{pool=\"direct\",reason=\"replaced\"} 3"));
        assert!(text.contains("kusto_bridge_files_removed_total{pool=\"direct\",reason=\"deleted\"} 0"));
        assert!(text.contains("kusto_bridge_refresh_duration_seconds_bucket{pool=\"direct\",le=\"0.5\"} 1"));
        assert!(text.contains("kusto_bridge_up{pool=\"all\"} 1"));
        assert!(!text.contains("direct_t"), "table names must not become labels");
        let snapshot = status.snapshot();
        assert_eq!(snapshot["converged"], false);
        assert_eq!(snapshot["tables"][0]["lag_versions"], 2);

        let mut table = status.table("direct", "t").unwrap_or_default();
        table.state = State::Healthy;
        table.applied_version = Some(5);
        status.update(table);
        status.finish_pass();
        assert_eq!(status.snapshot()["converged"], true);
        assert_eq!(status.table("direct", "t").map(|t| t.visibility_lag_seconds), Some(0.0));
    }
}
