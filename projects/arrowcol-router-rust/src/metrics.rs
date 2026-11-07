use std::sync::Arc;

use prometheus::{
    Encoder, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder, register_int_counter_vec_with_registry,
    register_int_gauge_vec_with_registry,
};

/// Low-cardinality router metrics.
#[derive(Clone)]
pub struct RouterMetrics {
    registry: Arc<Registry>,
    decisions: IntCounterVec,
    failures: IntCounterVec,
    refreshes: IntCounterVec,
    lease_events: IntCounterVec,
    upstream_connections: IntCounterVec,
    active_streams: IntGaugeVec,
    cached_leases: IntGaugeVec,
    maximum_pressure_bps: IntGaugeVec,
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
        let maximum_pressure_bps = register_int_gauge_vec_with_registry!(
            Opts::new(
                "arrowcol_router_maximum_collector_pressure_bps",
                "Maximum fresh collector pressure by pool"
            ),
            &["pool"],
            registry.as_ref()
        )?;
        Ok(Self {
            registry,
            decisions,
            failures,
            refreshes,
            lease_events,
            upstream_connections,
            active_streams,
            cached_leases,
            maximum_pressure_bps,
        })
    }

    /// Records a routing decision.
    pub fn decision(&self, operation: &str, pool: &str, source: &str) {
        self.decisions.with_label_values(&[operation, pool, source]).inc();
    }

    /// Records a structured routing failure.
    pub fn failure(&self, operation: &str, pool: &str, reason: &str) {
        self.failures.with_label_values(&[operation, pool, reason]).inc();
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

    /// Updates aggregate cache gauges.
    pub fn update_cache_gauges(&self, view: &arrowcol_routing::RoutingView) {
        for pool in ["dataflow"] {
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
        }
        for collector in view.collectors.values() {
            if let Some(pressure) = collector.pressure.as_ref() {
                let pool = collector.registration.implementation_pool.to_string();
                let gauge = self.maximum_pressure_bps.with_label_values(&[&pool]);
                gauge.set(gauge.get().max(i64::from(pressure.maximum_pressure_bps())));
            }
        }
    }

    /// Encodes the registry in Prometheus text format.
    ///
    /// # Errors
    ///
    /// Returns an encoding error.
    pub fn encode(&self) -> Result<Vec<u8>, prometheus::Error> {
        let families = self.registry.gather();
        let mut output = Vec::new();
        TextEncoder::new().encode(&families, &mut output)?;
        Ok(output)
    }
}
