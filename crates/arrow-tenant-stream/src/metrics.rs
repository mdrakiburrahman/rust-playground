//! Low-cardinality service counters; no tenant, path, or query labels.

use crate::reader::ReadMetrics;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Default)]
pub struct StreamMetrics {
    pub batches: AtomicU64,
    pub checkpoints: AtomicU64,
    pub replays: AtomicU64,
    pub noops: AtomicU64,
    pub txn_only_batches: AtomicU64,
    pub snapshot_batches: AtomicU64,
    pub tail_batches: AtomicU64,
    pub offsets_prepared: AtomicU64,
    pub sink_commits: AtomicU64,
    pub bootstrap_files_remaining: AtomicU64,
    pub failures: AtomicU64,
    pub source_version: AtomicU64,
    pub processed_version: AtomicU64,
    reads: Mutex<ReadMetrics>,
}

impl StreamMetrics {
    /// Accumulate counters from an executed DataFusion plan.
    ///
    /// # Errors
    /// Returns a poisoned metrics lock error.
    pub fn record_read(&self, read: ReadMetrics) -> anyhow::Result<()> {
        self.reads
            .lock()
            .map_err(|_| anyhow::anyhow!("stream metric lock poisoned"))?
            .merge(read);
        Ok(())
    }

    /// Render bounded, identity-free Prometheus text.
    ///
    /// # Errors
    /// Returns a poisoned metrics lock error.
    pub fn render(&self) -> anyhow::Result<String> {
        let mut output = String::new();
        for (name, value, kind) in [
            ("batches_total", self.batches.load(Ordering::Relaxed), "counter"),
            (
                "checkpoint_commits_total",
                self.checkpoints.load(Ordering::Relaxed),
                "counter",
            ),
            ("sink_replays_total", self.replays.load(Ordering::Relaxed), "counter"),
            ("noop_polls_total", self.noops.load(Ordering::Relaxed), "counter"),
            (
                "txn_only_batches_total",
                self.txn_only_batches.load(Ordering::Relaxed),
                "counter",
            ),
            (
                "snapshot_batches_total",
                self.snapshot_batches.load(Ordering::Relaxed),
                "counter",
            ),
            (
                "tail_batches_total",
                self.tail_batches.load(Ordering::Relaxed),
                "counter",
            ),
            (
                "offsets_prepared_total",
                self.offsets_prepared.load(Ordering::Relaxed),
                "counter",
            ),
            (
                "sink_commits_total",
                self.sink_commits.load(Ordering::Relaxed),
                "counter",
            ),
            (
                "bootstrap_files_remaining",
                self.bootstrap_files_remaining.load(Ordering::Relaxed),
                "gauge",
            ),
            ("failures_total", self.failures.load(Ordering::Relaxed), "counter"),
            ("source_version", self.source_version.load(Ordering::Relaxed), "gauge"),
            (
                "processed_version",
                self.processed_version.load(Ordering::Relaxed),
                "gauge",
            ),
            (
                "version_lag",
                self.source_version
                    .load(Ordering::Relaxed)
                    .saturating_sub(self.processed_version.load(Ordering::Relaxed)),
                "gauge",
            ),
        ] {
            output.push_str(&format!(
                "# TYPE tenant_stream_{name} {kind}\ntenant_stream_{name} {value}\n"
            ));
        }
        let reads = self
            .reads
            .lock()
            .map_err(|_| anyhow::anyhow!("stream metric lock poisoned"))?;
        output.push_str(&format!(
            "# TYPE tenant_stream_files_considered_total counter\ntenant_stream_files_considered_total {}\n\
             # TYPE tenant_stream_rows_returned_total counter\ntenant_stream_rows_returned_total {}\n",
            reads.files_considered, reads.rows_returned
        ));
        for name in [
            "bytes_scanned",
            "row_groups_pruned_statistics",
            "row_groups_matched_statistics",
            "row_groups_pruned_bloom_filter",
            "row_groups_matched_bloom_filter",
            "page_index_rows_pruned",
            "page_index_rows_matched",
            "pushdown_rows_pruned",
            "pushdown_rows_matched",
            "files_ranges_pruned_statistics",
            "files_ranges_matched_statistics",
        ] {
            let value = reads.datafusion.get(name).copied().unwrap_or(0);
            output.push_str(&format!(
                "# TYPE tenant_stream_datafusion_{name}_total counter\n\
                 tenant_stream_datafusion_{name}_total {value}\n"
            ));
        }
        Ok(output)
    }
}

/// Serve Prometheus text until cancelled; bind failures are returned to startup.
///
/// # Errors
/// Returns bind, connection, or metric synchronization failures.
pub async fn serve(listener: tokio::net::TcpListener, metrics: std::sync::Arc<StreamMetrics>) -> anyhow::Result<()> {
    loop {
        let (mut socket, _) = listener.accept().await?;
        if let Err(error) = tokio::time::timeout(std::time::Duration::from_secs(5), respond(&mut socket, &metrics))
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| result)
        {
            eprintln!("tenant metrics connection failed: {error:#}");
        }
    }
}

async fn respond(socket: &mut tokio::net::TcpStream, metrics: &StreamMetrics) -> anyhow::Result<()> {
    let mut request = [0; 4_096];
    let mut count = 0;
    while count < request.len() && !request[..count].windows(4).any(|window| window == b"\r\n\r\n") {
        let received = socket.read(&mut request[count..]).await?;
        if received == 0 {
            anyhow::bail!("metrics client closed an incomplete request");
        }
        count += received;
    }
    let (status, body) = if !request[..count].windows(4).any(|window| window == b"\r\n\r\n") {
        ("431 Request Header Fields Too Large", "request too large\n".to_owned())
    } else if request[..count].starts_with(b"GET /metrics ") {
        ("200 OK", metrics.render()?)
    } else {
        ("404 Not Found", "not found\n".to_owned())
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(response.as_bytes()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn prometheus_endpoint_handles_fragmented_requests_without_identity_labels() -> anyhow::Result<()> {
        let metrics = Arc::new(StreamMetrics::default());
        metrics.batches.store(3, Ordering::Relaxed);
        metrics.source_version.store(10, Ordering::Relaxed);
        metrics.processed_version.store(7, Ordering::Relaxed);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let task = tokio::spawn(serve(listener, metrics));
        let mut socket = tokio::net::TcpStream::connect(address).await?;
        socket.write_all(b"GET /met").await?;
        tokio::task::yield_now().await;
        socket.write_all(b"rics HTTP/1.1\r\nHost: localhost\r\n\r\n").await?;
        let mut response = String::new();
        socket.read_to_string(&mut response).await?;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("tenant_stream_batches_total 3\n"));
        assert!(response.contains("tenant_stream_version_lag 3\n"));
        assert!(!response.contains("customer="));
        assert!(!response.contains("filename="));
        task.abort();
        assert!(task.await.is_err_and(|error| error.is_cancelled()));
        Ok(())
    }
}
