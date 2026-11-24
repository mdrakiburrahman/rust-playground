use super::*;
use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Default)]
struct Ingress {
    observed: AtomicBool,
    rows: AtomicU64,
    bytes: AtomicU64,
    completed: AtomicU64,
    started: AtomicU64,
    wall_ns: AtomicU64,
    cpu_ns: AtomicU64,
    missing_cpu: AtomicBool,
}

static INGRESS: std::sync::LazyLock<[Ingress; 5]> = std::sync::LazyLock::new(Default::default);

/// Process-cumulative transport polling costs for diagnostic runs. Async idle time is excluded.
pub fn isolation_ingress_snapshot() -> Option<serde_json::Value> {
    let i = INGRESS.iter().find(|i| i.observed.load(Ordering::Relaxed))?;
    Some(
        serde_json::json!({"scope":"transport_poll_excluding_async_wait", "rows":i.rows.load(Ordering::Relaxed),
        "ipc_body_bytes":i.bytes.load(Ordering::Relaxed),"started":i.started.load(Ordering::Relaxed),"completed":i.completed.load(Ordering::Relaxed),
        "poll_wall_ns":i.wall_ns.load(Ordering::Relaxed),"poll_thread_cpu_ns":(!i.missing_cpu.load(Ordering::Relaxed)).then(||i.cpu_ns.load(Ordering::Relaxed))}),
    )
}

async fn measured_next<S: Stream + Unpin>(stream: &mut S, i: &Ingress) -> Option<S::Item> {
    futures::future::poll_fn(|cx| {
        let wall = Instant::now();
        let cpu = cpu_time::ThreadTime::try_now().ok();
        let result = stream.poll_next_unpin(cx);
        let cpu = cpu.and_then(|t| t.try_elapsed().ok());
        i.wall_ns.fetch_add(wall.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if let Some(cpu) = cpu {
            i.cpu_ns.fetch_add(cpu.as_nanos() as u64, Ordering::Relaxed);
        } else {
            i.missing_cpu.store(true, Ordering::Relaxed);
        }
        result
    })
    .await
}

/// Physical-stage endpoint, never used for production admission.
#[async_trait]
pub trait DiagnosticBatchSink: Send + Sync {
    /// Performs the configured physical work, returning only once it finishes.
    async fn accept(&self, batch: RecordBatch) -> RuntimeResult<()>;
}

/// Observation of discarded diagnostic input; not a `PutResultMetadata` admission.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticReceipt {
    /// Matched level of the non-production sink.
    pub diagnostic_discard: IsolationLevel,
    /// Number of record batch frames consumed.
    pub batches: u64,
    /// Total rows in those frames.
    pub rows: u64,
    /// IPC body bytes including dictionaries, excluding gRPC and IPC headers.
    pub ipc_body_bytes: u64,
}

pub(super) async fn consume(
    level: IsolationLevel,
    sink: Option<&dyn DiagnosticBatchSink>,
    first: FlightData,
    stream: InboundFlightStream,
    target_schema: SchemaRef,
    context: &StreamContext,
    limit: usize,
) -> RuntimeResult<DiagnosticReceipt> {
    let mut receipt = DiagnosticReceipt {
        diagnostic_discard: level,
        batches: 0,
        rows: 0,
        ipc_body_bytes: 0,
    };
    let i = INGRESS.get(level as usize).ok_or_else(RuntimeError::internal)?;
    i.observed.store(true, Ordering::Relaxed);
    i.started.fetch_add(1, Ordering::Relaxed);
    let body_bytes = Arc::new(AtomicU64::new(0));
    let body_counter = Arc::clone(&body_bytes);
    let mut checked = stream.map(move |item| {
        let data = item.map_err(status_to_runtime)?;
        validate_followup_message(&data, limit)?;
        let header = arrow_ipc::root_as_message(&data.data_header).map_err(|_| invalid_flight_stream())?;
        if usize::try_from(header.bodyLength()).ok() != Some(data.data_body.len()) {
            return Err(invalid_flight_stream());
        }
        if !matches!(
            header.header_type(),
            MessageHeader::RecordBatch | MessageHeader::DictionaryBatch
        ) {
            return Err(invalid_flight_stream());
        }
        body_counter.fetch_add(data.data_body.len() as u64, Ordering::Relaxed);
        Ok(data)
    });
    if level == IsolationLevel::L0 {
        while let Some(frame) = measured_next(&mut checked, i).await {
            let frame = frame?;
            let header = arrow_ipc::root_as_message(&frame.data_header).map_err(|_| invalid_flight_stream())?;
            if let Some(batch) = header.header_as_record_batch() {
                let rows = u64::try_from(batch.length()).map_err(|_| invalid_flight_stream())?;
                receipt.rows = receipt.rows.checked_add(rows).ok_or_else(invalid_flight_stream)?;
                receipt.batches += 1;
            }
        }
    } else {
        let frames = stream::once(async move { Ok(first) })
            .chain(checked)
            .map(|item| item.map_err(|error| FlightError::from_external_error(Box::new(error))));
        let mut decoder = FlightDataDecoder::new(frames);
        while let Some(frame) = measured_next(&mut decoder, i).await {
            match frame.map_err(flight_error_to_runtime)?.payload {
                DecodedPayload::Schema(_) => {}
                DecodedPayload::RecordBatch(batch) => {
                    if record_batch_logical_bytes(&batch)? > limit {
                        return Err(RuntimeError::new(
                            ErrorCode::CapacityExhausted,
                            Code::ResourceExhausted,
                            "diagnostic batch exceeds configured limit",
                        ));
                    }
                    receipt.rows = receipt
                        .rows
                        .checked_add(batch.num_rows() as u64)
                        .ok_or_else(invalid_flight_stream)?;
                    receipt.batches += 1;
                    let aligned = align_record_batch(batch, context.alignment(), Arc::clone(&target_schema))?;
                    if let Some(sink) = sink {
                        sink.accept(aligned).await?;
                    }
                }
                DecodedPayload::None => return Err(invalid_flight_stream()),
            }
        }
    }
    receipt.ipc_body_bytes = body_bytes.load(Ordering::Relaxed);
    i.rows.fetch_add(receipt.rows, Ordering::Relaxed);
    i.bytes.fetch_add(receipt.ipc_body_bytes, Ordering::Relaxed);
    i.completed.fetch_add(1, Ordering::Relaxed);
    Ok(receipt)
}
