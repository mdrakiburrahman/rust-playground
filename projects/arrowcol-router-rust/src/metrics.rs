use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrowcol_routing::{AdmissionPressureGauge, SqlOperationObserver};
use arrowcol_sql_server::SqlPool;
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder, register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry,
};

/// Low-cardinality router metrics.
#[derive(Clone)]
pub struct RouterMetrics {
    registry: Arc<Registry>,
    decisions: IntCounterVec,
    failures: IntCounterVec,
    oat_ineligibility: IntCounterVec,
    refreshes: IntCounterVec,
    lease_events: IntCounterVec,
    upstream_connections: IntCounterVec,
    active_streams: IntGaugeVec,
    cached_leases: IntGaugeVec,
    table_owners: IntGaugeVec,
    distinct_table_owners: IntGauge,
    maximum_pressure_bps: IntGaugeVec,
    pressure_gauge_bps: IntGaugeVec,
    sql_connections_opened: IntCounter,
    sql_operations: IntCounterVec,
    sql_operation_seconds: HistogramVec,
    request_seconds: Histogram,
    sql_pool: Option<SqlPool>,
    pool_created_seen: Arc<AtomicU64>,
}

impl RouterMetrics {
    /// Creates an isolated Prometheus registry.
    ///
    /// # Errors
    ///
    /// Returns a Prometheus registration error.
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Arc::new(Registry::new());
        let decisions = register_int_counter_vec_with_registry!(
            Opts::new(
                "arrowcol_router_decisions_total",
                "Routing decisions by operation, pool, and source"
            ),
            &["operation", "pool", "source"],
            registry.as_ref()
        )?;
        let failures = register_int_counter_vec_with_registry!(
            Opts::new("arrowcol_router_failures_total", "Structured routing failures"),
            &["operation", "pool", "reason"],
            registry.as_ref()
        )?;
        let oat_ineligibility = register_int_counter_vec_with_registry!(
            Opts::new(
                "arrowcol_router_oat_ineligibility_total",
                "OAT holder rejection by routing phase and bounded ineligibility reason"
            ),
            &["phase", "reason"],
            registry.as_ref()
        )?;
        let refreshes = register_int_counter_vec_with_registry!(
            Opts::new("arrowcol_router_cache_refresh_total", "Routing cache refresh outcomes"),
            &["outcome"],
            registry.as_ref()
        )?;
        let lease_events = register_int_counter_vec_with_registry!(
            Opts::new(
                "arrowcol_router_lease_events_total",
                "Lease drain, transfer, and conflict events"
            ),
            &["event"],
            registry.as_ref()
        )?;
        let upstream_connections = register_int_counter_vec_with_registry!(
            Opts::new(
                "arrowcol_router_upstream_connections_total",
                "Upstream H2 connection acquisition by reuse status"
            ),
            &["reused"],
            registry.as_ref()
        )?;
        let active_streams = register_int_gauge_vec_with_registry!(
            Opts::new("arrowcol_router_active_streams", "Currently proxied streams by pool"),
            &["pool"],
            registry.as_ref()
        )?;
        let cached_leases = register_int_gauge_vec_with_registry!(
            Opts::new(
                "arrowcol_router_cached_leases",
                "Cached routing leases by pool and state"
            ),
            &["pool", "state"],
            registry.as_ref()
        )?;
        let table_owners = register_int_gauge_vec_with_registry!(
            Opts::new(
                "arrowcol_router_table_owners",
                "Current collector assignment for each cached table lease",
            ),
            &["pool", "catalog", "schema", "table", "collector", "state"],
            registry.as_ref()
        )?;
        let distinct_table_owners = register_int_gauge_with_registry!(
            Opts::new(
                "arrowcol_router_distinct_table_owners",
                "Whether active table leases require distinct collectors",
            ),
            registry.as_ref()
        )?;
        let maximum_pressure_bps = register_int_gauge_vec_with_registry!(
            Opts::new(
                "arrowcol_router_maximum_collector_pressure_bps",
                "Maximum fresh collector pressure by pool"
            ),
            &["pool"],
            registry.as_ref()
        )?;
        let pressure_gauge_bps = register_int_gauge_vec_with_registry!(
            Opts::new(
                "arrowcol_router_collector_pressure_gauge_bps",
                "Fresh collector pressure by pool and admission gauge"
            ),
            &["pool", "gauge"],
            registry.as_ref()
        )?;
        let sql_connections_opened = register_int_counter_with_registry!(
            Opts::new(
                "arrowcol_router_sql_connections_opened_total",
                "Successful physical SQL connections opened"
            ),
            registry.as_ref()
        )?;
        let sql_operations = register_int_counter_vec_with_registry!(
            Opts::new(
                "arrowcol_router_sql_operations_total",
                "SQL routing operations by kind and outcome"
            ),
            &["operation", "outcome"],
            registry.as_ref()
        )?;
        let sql_operation_seconds = register_histogram_vec_with_registry!(
            HistogramOpts::new(
                "arrowcol_router_sql_operation_seconds",
                "SQL routing operation latency by kind"
            )
            .buckets(vec![
                0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0
            ]),
            &["operation"],
            registry.as_ref()
        )?;
        let request_seconds = register_histogram_with_registry!(
            HistogramOpts::new(
                "arrowcol_router_request_seconds",
                "Request-filter to upstream response-header latency"
            )
            .buckets(vec![
                0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0
            ]),
            registry.as_ref()
        )?;
        Ok(Self {
            registry,
            decisions,
            failures,
            oat_ineligibility,
            refreshes,
            lease_events,
            upstream_connections,
            active_streams,
            cached_leases,
            table_owners,
            distinct_table_owners,
            maximum_pressure_bps,
            pressure_gauge_bps,
            sql_connections_opened,
            sql_operations,
            sql_operation_seconds,
            request_seconds,
            sql_pool: None,
            pool_created_seen: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn observe_request_duration(&self, elapsed: Duration) {
        self.request_seconds.observe(elapsed.as_secs_f64());
    }

    /// Tracks pool-created connections alongside direct unpooled opens.
    pub fn with_sql_pool(mut self, pool: SqlPool) -> Self {
        self.sql_pool = Some(pool);
        self
    }

    fn sync_pool_connections(&self) {
        if let Some(pool) = &self.sql_pool {
            let created = pool.stats().created;
            let previous = self.pool_created_seen.fetch_max(created, Ordering::Relaxed);
            if created > previous {
                self.sql_connections_opened.inc_by(created - previous);
            }
        }
    }

    /// Records a routing decision.
    pub fn decision(&self, operation: &str, pool: &str, source: &str) {
        self.decisions.with_label_values(&[operation, pool, source]).inc();
    }

    /// Records a structured routing failure.
    pub fn failure(&self, operation: &str, pool: &str, reason: &str) {
        self.failures.with_label_values(&[operation, pool, reason]).inc();
    }

    pub fn oat_ineligibility(&self, phase: &'static str, reason: &'static str) {
        self.oat_ineligibility.with_label_values(&[phase, reason]).inc();
    }

    /// Records a cache refresh.
    pub fn refresh(&self, success: bool) {
        self.refreshes
            .with_label_values(&[if success { "success" } else { "failure" }])
            .inc();
    }

    /// Records one lease control event.
    pub fn lease_event(&self, event: &str, count: u64) {
        self.lease_events.with_label_values(&[event]).inc_by(count);
    }

    /// Records upstream H2 connection reuse.
    pub fn upstream_connection(&self, reused: bool) {
        self.upstream_connections
            .with_label_values(&[if reused { "true" } else { "false" }])
            .inc();
    }

    /// Increments proxied streams for a pool.
    pub fn stream_started(&self, pool: &str) {
        self.active_streams.with_label_values(&[pool]).inc();
    }

    /// Decrements proxied streams for a pool.
    pub fn stream_finished(&self, pool: &str) {
        self.active_streams.with_label_values(&[pool]).dec();
    }

    /// Records whether table ownership requires unique collectors.
    pub fn set_distinct_table_owners(&self, enabled: bool) {
        self.distinct_table_owners.set(i64::from(enabled));
    }

    /// Updates cache and table-owner gauges.
    pub fn update_cache_gauges(&self, view: &arrowcol_routing::RoutingView) {
        self.table_owners.reset();
        self.pressure_gauge_bps.reset();
        for pool in ["dataflow"] {
            for gauge in AdmissionPressureGauge::ALL {
                self.pressure_gauge_bps
                    .with_label_values(&[pool, gauge.as_str()])
                    .set(0);
            }
            for state in ["active", "draining"] {
                self.cached_leases.with_label_values(&[pool, state]).set(0);
            }
            self.maximum_pressure_bps.with_label_values(&[pool]).set(0);
        }
        for lease in view.leases.values() {
            let state = match lease.state {
                arrowcol_routing::LeaseState::Active => "active",
                arrowcol_routing::LeaseState::Draining => "draining",
            };
            self.cached_leases
                .with_label_values(&[&lease.key.implementation_pool.to_string(), state])
                .inc();
            self.table_owners
                .with_label_values(&[
                    &lease.key.implementation_pool.to_string(),
                    lease.key.table.catalog().as_str(),
                    lease.key.table.schema().as_str(),
                    lease.key.table.table().as_str(),
                    &lease.holder_instance_id.to_string(),
                    state,
                ])
                .set(1);
        }
        for collector in view.collectors.values() {
            if let Some(pressure) = collector.pressure.as_ref() {
                let pool = collector.registration.implementation_pool.to_string();
                let maximum = self.maximum_pressure_bps.with_label_values(&[&pool]);
                maximum.set(maximum.get().max(i64::from(pressure.maximum_pressure_bps())));
                for (name, value) in pressure.pressure_gauges_bps() {
                    let gauge = self.pressure_gauge_bps.with_label_values(&[&pool, name.as_str()]);
                    gauge.set(gauge.get().max(i64::from(value)));
                }
            }
        }
    }

    /// Encodes the registry in Prometheus text format.
    ///
    /// # Errors
    ///
    /// Returns an encoding error.
    pub fn encode(&self) -> Result<Vec<u8>, prometheus::Error> {
        self.sync_pool_connections();
        let families = self.registry.gather();
        let mut output = Vec::new();
        TextEncoder::new().encode(&families, &mut output)?;
        Ok(output)
    }
}

impl SqlOperationObserver for RouterMetrics {
    fn connection_opened(&self) {
        self.sql_connections_opened.inc();
    }

    fn operation(&self, operation: &'static str, elapsed: Duration, success: bool) {
        self.sql_operations
            .with_label_values(&[operation, if success { "success" } else { "failure" }])
            .inc();
        self.sql_operation_seconds
            .with_label_values(&[operation])
            .observe(elapsed.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::RouterMetrics;
    use arrowcol_protocol::UnixMillis;
    use arrowcol_routing::RoutingView;
    use arrowcol_routing::SqlOperationObserver;
    use std::time::Duration;

    #[test]
    fn exports_sql_connection_operation_and_latency_metrics() -> Result<(), Box<dyn std::error::Error>> {
        let metrics = RouterMetrics::new()?;
        metrics.oat_ineligibility("initial", "pressure_future");
        metrics.connection_opened();
        metrics.operation("get_run_table_pin", Duration::from_millis(12), true);
        metrics.observe_request_duration(Duration::from_millis(25));
        let output = String::from_utf8(metrics.encode()?)?;
        assert!(
            output.contains("arrowcol_router_oat_ineligibility_total{phase=\"initial\",reason=\"pressure_future\"} 1")
        );
        assert!(output.contains("arrowcol_router_sql_connections_opened_total 1"));
        assert!(
            output.contains(
                "arrowcol_router_sql_operations_total{operation=\"get_run_table_pin\",outcome=\"success\"} 1"
            )
        );
        assert!(
            output.contains(
                "arrowcol_router_sql_operation_seconds_bucket{operation=\"get_run_table_pin\",le=\"0.025\"} 1"
            )
        );
        assert!(output.contains("arrowcol_router_request_seconds_bucket{le=\"0.025\"} 1"));
        Ok(())
    }

    #[test]
    fn exports_fixed_pressure_gauge_labels_for_diagnosis() -> Result<(), Box<dyn std::error::Error>> {
        let metrics = RouterMetrics::new()?;
        metrics.update_cache_gauges(&RoutingView::empty(UnixMillis::new(1)?));
        let output = String::from_utf8(metrics.encode()?)?;
        assert!(output.contains("arrowcol_router_collector_pressure_gauge_bps"));
        assert!(output.contains("gauge=\"cpu_utilization_bps\""));
        assert!(output.contains("gauge=\"quiver_pressure_bps\""));
        Ok(())
    }
}
