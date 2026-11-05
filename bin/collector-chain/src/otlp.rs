use std::time::SystemTime;

use anyhow::{Result, anyhow};
use opentelemetry::{
    KeyValue,
    logs::{LogRecord, Logger, LoggerProvider, Severity},
    metrics::MeterProvider,
    trace::{Span, Tracer, TracerProvider},
};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{Resource, logs::SdkLoggerProvider, metrics::SdkMeterProvider, trace::SdkTracerProvider};

const SCOPE: &str = "collector-chain";

pub fn send(endpoint: &str, client_id: &str, count: usize) -> Result<()> {
    let base = endpoint.trim_end_matches('/');
    let resource = Resource::builder()
        .with_service_name(SCOPE)
        .with_attribute(KeyValue::new("client.id", client_id.to_owned()))
        .build();
    let logs = SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(
            opentelemetry_otlp::LogExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(format!("{base}/v1/logs"))
                .build()?,
        )
        .build();
    let metrics = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_periodic_exporter(
            opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(format!("{base}/v1/metrics"))
                .build()?,
        )
        .build();
    let traces = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(
            opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(format!("{base}/v1/traces"))
                .build()?,
        )
        .build();

    let logger = logs.logger(SCOPE);
    let counter = metrics.meter(SCOPE).u64_counter("collector_chain.events").build();
    let tracer = traces.tracer(SCOPE);
    for index in 0..count {
        let attributes = [KeyValue::new("event.index", index as i64)];
        let mut span = tracer.start("collector-chain.event");
        span.set_attributes(attributes.clone());
        let context = span.span_context().clone();
        let mut record = logger.create_log_record();
        record.set_timestamp(SystemTime::now());
        record.set_severity_number(Severity::Info);
        record.set_body(format!("collector-chain event {index}").into());
        record.add_attribute("event.index", index as i64);
        record.set_trace_context(context.trace_id(), context.span_id(), Some(context.trace_flags()));
        logger.emit(record);
        counter.add(1, &attributes);
        span.end();
    }

    let results = [
        logs.force_flush(),
        metrics.force_flush(),
        traces.force_flush(),
        logs.shutdown(),
        metrics.shutdown(),
        traces.shutdown(),
    ];
    for result in results {
        result.map_err(|error| anyhow!("OTLP export failed: {error}"))?;
    }
    Ok(())
}
