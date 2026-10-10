use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::{Result, anyhow};
use opentelemetry::{
    KeyValue,
    logs::{LogRecord, Logger, LoggerProvider, Severity},
    metrics::{Counter, MeterProvider},
    trace::{Span, Tracer, TracerProvider},
};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    logs::{LogBatch, LogExporter, SdkLogger, SdkLoggerProvider},
    metrics::{SdkMeterProvider, Temporality, data::ResourceMetrics, exporter::PushMetricExporter},
    trace::{SdkTracer, SdkTracerProvider, SpanData, SpanExporter},
};

pub struct Telemetry {
    logs: SdkLoggerProvider,
    metrics: SdkMeterProvider,
    traces: SdkTracerProvider,
    logger: SdkLogger,
    counter: Counter<u64>,
    tracer: SdkTracer,
    failures: Arc<Mutex<Option<String>>>,
}

impl Telemetry {
    pub fn new() -> Result<Self> {
        let failures = Arc::new(Mutex::new(None));
        let resource = Resource::builder().with_service_name("hello-world").build();
        let log_exporter = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build()?;
        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build()?;
        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .build()?;
        let logs = SdkLoggerProvider::builder()
            .with_resource(resource.clone())
            .with_batch_exporter(CheckedExporter::new(log_exporter, &failures))
            .build();
        let metrics = SdkMeterProvider::builder()
            .with_resource(resource.clone())
            .with_periodic_exporter(CheckedExporter::new(metric_exporter, &failures))
            .build();
        let traces = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(CheckedExporter::new(span_exporter, &failures))
            .build();
        Ok(Self {
            logger: logs.logger("hello-world"),
            counter: metrics
                .meter("hello-world")
                .u64_counter("hello_world.greetings")
                .build(),
            tracer: traces.tracer("hello-world"),
            logs,
            metrics,
            traces,
            failures,
        })
    }

    pub fn greeting(&self, name: &str, run_id: &str) -> Result<()> {
        let attributes = [
            KeyValue::new("name", name.to_owned()),
            KeyValue::new("run.id", run_id.to_owned()),
        ];
        let mut span = self.tracer.start("greeting");
        span.set_attributes(attributes.clone());
        let context = span.span_context();
        let mut record = self.logger.create_log_record();
        record.set_timestamp(SystemTime::now());
        record.set_severity_number(Severity::Info);
        record.set_severity_text("INFO");
        record.set_body("greeting generated".into());
        record.add_attribute("name", name.to_owned());
        record.add_attribute("run.id", run_id.to_owned());
        record.set_trace_context(context.trace_id(), context.span_id(), Some(context.trace_flags()));
        self.logger.emit(record);
        self.counter.add(1, &attributes);
        span.end();
        self.flush()
    }

    fn flush(&self) -> Result<()> {
        let results = [
            self.logs.force_flush(),
            self.metrics.force_flush(),
            self.traces.force_flush(),
        ];
        self.check(results)
    }

    pub fn shutdown(self) -> Result<()> {
        let flush = self.flush();
        let results = [self.logs.shutdown(), self.metrics.shutdown(), self.traces.shutdown()];
        flush.and(self.check(results))
    }

    fn check(&self, results: [OTelSdkResult; 3]) -> Result<()> {
        for result in results {
            result.map_err(|error| anyhow!("telemetry export failed: {error}"))?;
        }
        if let Some(error) = &*self
            .failures
            .lock()
            .map_err(|_| anyhow!("telemetry failure lock poisoned"))?
        {
            return Err(anyhow!("telemetry export failed: {error}"));
        }
        Ok(())
    }
}

// SDK background exports can report errors only to diagnostics. Retain the first
// failure so even an export racing force_flush makes the CLI exit unsuccessfully.
#[derive(Debug)]
struct CheckedExporter<E> {
    inner: E,
    failures: Arc<Mutex<Option<String>>>,
}

impl<E> CheckedExporter<E> {
    fn new(inner: E, failures: &Arc<Mutex<Option<String>>>) -> Self {
        Self {
            inner,
            failures: Arc::clone(failures),
        }
    }

    fn checked(&self, result: OTelSdkResult) -> OTelSdkResult {
        if let Err(error) = &result
            && let Ok(mut failure) = self.failures.lock()
            && failure.is_none()
        {
            *failure = Some(error.to_string());
        }
        result
    }
}

impl<E: LogExporter> LogExporter for CheckedExporter<E> {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        self.checked(self.inner.export(batch).await)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.checked(self.inner.shutdown_with_timeout(timeout))
    }
}

impl<E: SpanExporter> SpanExporter for CheckedExporter<E> {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let result = self.inner.export(batch).await;
        self.checked(result)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }

    fn shutdown_with_timeout(&mut self, timeout: Duration) -> OTelSdkResult {
        let result = self.inner.shutdown_with_timeout(timeout);
        self.checked(result)
    }

    fn force_flush(&mut self) -> OTelSdkResult {
        let result = self.inner.force_flush();
        self.checked(result)
    }
}

impl<E: PushMetricExporter> PushMetricExporter for CheckedExporter<E> {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        self.checked(self.inner.export(metrics).await)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.checked(self.inner.force_flush())
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.checked(self.inner.shutdown_with_timeout(timeout))
    }

    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_sdk::error::OTelSdkError;

    #[test]
    fn retains_background_export_failure_after_later_success() -> Result<()> {
        let failures = Arc::new(Mutex::new(None));
        let exporter = CheckedExporter::new((), &failures);
        assert!(
            exporter
                .checked(Err(OTelSdkError::InternalFailure("first failure".into())))
                .is_err()
        );
        assert!(exporter.checked(Ok(())).is_ok());
        assert!(
            exporter
                .checked(Err(OTelSdkError::InternalFailure("second failure".into())))
                .is_err()
        );
        let failure = failures.lock().map_err(|_| anyhow!("failure lock poisoned"))?;
        assert!(failure.as_deref().is_some_and(|error| error.contains("first failure")));
        Ok(())
    }
}
