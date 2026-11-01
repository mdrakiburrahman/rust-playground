use std::time::Duration;

use anyhow::{Context, Result, ensure};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};

/// Process-wide OTLP metrics lifecycle; install before creating Delta instruments.
pub struct MetricsGuard(SdkMeterProvider);

impl MetricsGuard {
    /// Installs the global meter provider, honoring standard OTLP environment variables.
    ///
    /// # Errors
    /// Rejects an invalid export interval or OTLP exporter configuration.
    pub fn install(service: &'static str) -> Result<Self> {
        let interval = export_interval(std::env::var("OTEL_METRIC_EXPORT_INTERVAL").ok().as_deref())?;
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .build()
            .context("initialize OTLP HTTP metrics exporter")?;
        let reader = PeriodicReader::builder(exporter).with_interval(interval).build();
        let provider = SdkMeterProvider::builder()
            .with_resource(
                Resource::builder()
                    .with_service_name(std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| service.to_owned()))
                    .build(),
            )
            .with_reader(reader)
            .build();
        opentelemetry::global::set_meter_provider(provider.clone());
        Ok(Self(provider))
    }
}

impl Drop for MetricsGuard {
    fn drop(&mut self) {
        if let Err(error) = self.0.force_flush() {
            eprintln!("metric force flush failed: {error}");
        }
        if let Err(error) = self.0.shutdown() {
            eprintln!("metric shutdown failed: {error}");
        }
    }
}

fn export_interval(value: Option<&str>) -> Result<Duration> {
    let millis = value
        .unwrap_or("2000")
        .parse::<u64>()
        .context("OTEL_METRIC_EXPORT_INTERVAL must be positive milliseconds")?;
    ensure!(millis > 0, "OTEL_METRIC_EXPORT_INTERVAL must be positive");
    Ok(Duration::from_millis(millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_interval_honors_environment_and_has_short_default() -> Result<()> {
        assert_eq!(export_interval(None)?, Duration::from_secs(2));
        assert_eq!(export_interval(Some("500"))?, Duration::from_millis(500));
        assert!(export_interval(Some("0")).is_err());
        assert!(export_interval(Some("invalid")).is_err());
        Ok(())
    }
}
