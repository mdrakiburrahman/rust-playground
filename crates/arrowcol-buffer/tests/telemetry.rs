use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use arrowcol_buffer::MetricsGuard;
use delta_kernel::metrics::WithMetricsReporterLayer;
use tracing_subscriber::prelude::*;

#[test]
fn export_probe() -> Result<()> {
    if std::env::var_os("ARROWCOL_OTLP_PROBE").is_none() {
        return Ok(());
    }
    let metrics = MetricsGuard::install("collector-telemetry-regression")?;
    tracing_subscriber::registry()
        .with_metrics_reporter_layer(arrow_deltalake::telemetry::kernel_metrics_reporter())
        .with(tracing_subscriber::fmt::layer().with_filter(tracing_subscriber::EnvFilter::new("off")))
        .try_init()
        .map_err(|error| anyhow::anyhow!(error))?;
    delta_kernel::metrics::emit_parquet_read_completed(1, 123);
    drop(metrics);
    Ok(())
}

#[test]
fn kernel_metrics_export_with_logs_off_and_flush_before_exit() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let mut child = Command::new(std::env::current_exe()?)
        .args(["--exact", "export_probe", "--nocapture"])
        .env("ARROWCOL_OTLP_PROBE", "1")
        .env(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            format!("http://{}", listener.local_addr()?),
        )
        .env("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
        .env("OTEL_METRIC_EXPORT_INTERVAL", "60000")
        .env("OTEL_EXPORTER_OTLP_TIMEOUT", "2000")
        .env("OTEL_SERVICE_NAME", "collector-service-override")
        .env(
            "OTEL_RESOURCE_ATTRIBUTES",
            "service.namespace=arrowcol-test,deployment.environment.name=regression",
        )
        .env_remove("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .stdout(Stdio::null())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut requests = Vec::new();
    loop {
        match listener.accept() {
            Ok((stream, _)) => requests.push(receive_export(stream)?),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error.into()),
        }
        if let Some(status) = child.try_wait()? {
            ensure!(status.success(), "OTLP probe failed: {status}");
            break;
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            anyhow::bail!("OTLP probe did not flush and exit within 20 seconds");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    ensure!(!requests.is_empty(), "no metrics flushed before child exit");
    let payload: Vec<u8> = requests.into_iter().flatten().collect();
    for expected in [
        "POST /v1/metrics",
        "application/x-protobuf",
        "delta.kernel.events",
        "delta.kernel.bytes",
        "service.name",
        "collector-service-override",
        "service.namespace",
        "arrowcol-test",
        "deployment.environment.name",
        "regression",
        "telemetry.sdk.language",
        "rust",
        "telemetry.sdk.name",
        "opentelemetry",
        "telemetry.sdk.version",
        "0.31.0",
    ] {
        ensure!(
            payload
                .windows(expected.len())
                .any(|window| window == expected.as_bytes()),
            "OTLP export missing {expected}"
        );
    }
    Ok(())
}

fn receive_export(mut stream: TcpStream) -> Result<Vec<u8>> {
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    let mut request = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        let read = stream.read(&mut buffer)?;
        ensure!(read > 0, "OTLP request ended early");
        request.extend_from_slice(&buffer[..read]);
        if let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&request[..header_end])?;
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length").then_some(value.trim())
                })
                .context("OTLP exporter did not send Content-Length")?
                .parse::<usize>()?;
            if request.len() >= header_end + 4 + length {
                break;
            }
        }
        ensure!(request.len() < 1024 * 1024, "unexpectedly large OTLP test export");
    }
    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
    Ok(request)
}
