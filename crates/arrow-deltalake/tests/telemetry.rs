#![cfg(feature = "delta")]

use anyhow::{Context, Result};
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::DeltaTable;
use delta_kernel::metrics::WithMetricsReporterLayer;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use std::collections::BTreeSet;
use std::sync::Arc;
use tracing_subscriber::util::SubscriberInitExt;

/// Scenario: real Kernel work and replay run with the process telemetry subscriber.
/// Guarantees: every exported family has actual observations and publication dedupe is measured.
#[tokio::test]
async fn real_kernel_and_publication_events_reach_meter_provider() -> Result<()> {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());
    tracing_subscriber::registry()
        .with_metrics_reporter_layer(arrow_deltalake::telemetry::kernel_metrics_reporter())
        .try_init()?;
    let root = tempfile::tempdir()?;
    let table = DeltaTable::open(root.path())?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )?;
    for _ in 0..2 {
        let mut file = table
            .new_file(batch.schema())?
            .with_transaction("telemetry-publication".into(), 0)?;
        file.write(&batch).await?;
        assert_eq!(file.finish().await?.version, 1);
    }
    assert_eq!(table.lookup_txn("telemetry-publication")?, Some(0));
    table.checkpoint()?;
    provider.force_flush()?;
    let finished = exporter.get_finished_metrics()?;
    let metrics = finished
        .iter()
        .flat_map(|resource| resource.scope_metrics())
        .flat_map(|scope| scope.metrics())
        .collect::<Vec<_>>();
    let names = metrics.iter().map(|metric| metric.name()).collect::<BTreeSet<_>>();
    for expected in [
        "delta.kernel.events",
        "delta.kernel.duration",
        "delta.kernel.values",
        "delta.kernel.bytes",
        "delta.kernel.version",
        "delta.publication.events",
        "delta.publication.duration",
    ] {
        assert!(names.contains(expected), "missing {expected}: {names:?}");
    }
    let events = metrics
        .iter()
        .find(|metric| metric.name() == "delta.publication.events")
        .context("publication events")?;
    let AggregatedMetrics::U64(MetricData::Sum(events)) = events.data() else {
        anyhow::bail!("publication event counter did not export a sum");
    };
    for (outcome, count) in [("attempt", 2), ("committed", 1), ("deduplicated", 1)] {
        assert!(
            events.data_points().any(|point| point.value() == count
                && point
                    .attributes()
                    .any(|attribute| attribute.key.as_str() == "outcome" && attribute.value.as_str() == outcome)),
            "missing actual {outcome}={count} observation"
        );
    }
    provider.shutdown()?;
    Ok(())
}
