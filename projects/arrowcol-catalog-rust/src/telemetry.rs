use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{KeyValue, global};

struct CatalogMetrics {
    attributes: Vec<KeyValue>,
    operations: Counter<u64>,
    duration: Histogram<f64>,
}

impl CatalogMetrics {
    fn new() -> Self {
        let meter = global::meter("arrowcol-catalog-rust");
        Self {
            attributes: vec![
                KeyValue::new(
                    "run",
                    std::env::var("ARROWCOL_OAT_RUN_ID").unwrap_or_else(|_| "unknown".to_owned()),
                ),
                KeyValue::new(
                    "variant",
                    std::env::var("ARROWCOL_OAT_VARIANT").unwrap_or_else(|_| "unknown".to_owned()),
                ),
                KeyValue::new("component", "arrowcol-catalog-rust"),
            ],
            operations: meter
                .u64_counter("arrowcol.catalog.ddl.operations")
                .with_unit("1")
                .build(),
            duration: meter
                .f64_histogram("arrowcol.catalog.ddl.duration")
                .with_unit("s")
                .build(),
        }
    }
}

pub(crate) fn record_ddl(elapsed: Duration, success: bool) {
    static METRICS: OnceLock<CatalogMetrics> = OnceLock::new();
    let metrics = METRICS.get_or_init(CatalogMetrics::new);
    let outcome = if success { "success" } else { "failure" };
    let attributes = [
        metrics.attributes[0].clone(),
        metrics.attributes[1].clone(),
        metrics.attributes[2].clone(),
        KeyValue::new("operation", "ddl"),
        KeyValue::new("outcome", outcome),
    ];
    metrics.operations.add(1, &attributes);
    metrics.duration.record(elapsed.as_secs_f64(), &attributes);
}
