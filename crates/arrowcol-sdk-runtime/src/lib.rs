//! Shared, bounded Arrow Flight producer. Generated contracts contain no transport implementation.
use std::{marker::PhantomData, sync::Arc, time::Duration};

use arrow_array::RecordBatch;
use arrow_flight::{FlightDescriptor, encode::FlightDataEncoderBuilder, flight_service_client::FlightServiceClient};
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tonic::{
    Request,
    transport::{Channel, ClientTlsConfig, Endpoint},
};

/// Failure surfaced by push, flush or close. Unknown outcomes must not be blindly replayed.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{kind}: {message}")]
pub struct Error {
    pub kind: &'static str,
    pub message: String,
}
impl Error {
    pub fn invalid(message: impl ToString) -> Self {
        Self {
            kind: "invalid_input",
            message: message.to_string(),
        }
    }
    fn closed() -> Self {
        Self {
            kind: "closed",
            message: "stream is closed; explicitly flush and close before releasing it".into(),
        }
    }
    fn unknown(message: impl ToString) -> Self {
        Self {
            kind: "outcome_unknown",
            message: message.to_string(),
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;

/// Converts a decimal's numeric text to the exact Arrow unscaled integer without floating point or rounding.
pub fn decimal128(text: &str, precision: u8, scale: i8) -> Result<i128> {
    let invalid = || Error::invalid("decimal must fit its declared precision/scale without rounding");
    if precision == 0 || precision > 38 || text.len() > 128 {
        return Err(invalid());
    }
    let (negative, text) = if let Some(value) = text.strip_prefix('-') {
        (true, value)
    } else {
        (false, text.strip_prefix('+').unwrap_or(text))
    };
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if (whole.is_empty() && fraction.is_empty()) || !whole.bytes().chain(fraction.bytes()).all(|ch| ch.is_ascii_digit())
    {
        return Err(invalid());
    }
    let fraction = fraction.trim_end_matches('0');
    let mut unscaled = whole
        .bytes()
        .chain(fraction.bytes())
        .try_fold(0i128, |value, ch| {
            value.checked_mul(10)?.checked_add(i128::from(ch - b'0'))
        })
        .ok_or_else(invalid)?;
    let shift = i32::from(scale) - i32::try_from(fraction.len()).map_err(|_| invalid())?;
    if unscaled != 0 {
        let power = 10i128.checked_pow(shift.unsigned_abs()).ok_or_else(invalid)?;
        if shift >= 0 {
            unscaled = unscaled.checked_mul(power).ok_or_else(invalid)?;
        } else {
            if unscaled % power != 0 {
                return Err(invalid());
            }
            unscaled /= power;
        }
    }
    if unscaled >= 10i128.pow(u32::from(precision)) {
        return Err(invalid());
    }
    Ok(if negative { -unscaled } else { unscaled })
}

/// Implemented by the Arrowcol plugin, not by applications.
pub trait Contract: Send + 'static {
    type Event: Send;
    fn schema() -> SchemaRef;
    fn record_batch(events: &[Self::Event]) -> Result<RecordBatch>;
    fn descriptor() -> Vec<String>;
    fn metadata() -> Vec<(&'static str, String)>;
    fn fingerprint() -> &'static str;
    fn version() -> u64;
}

/// Client buffering only; these options do not configure collector or storage policies.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct StreamOptions {
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub buffer_bytes: usize,
    pub flush_interval_ms: u64,
    pub timeout_ms: u64,
}
impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            batch_rows: 1024,
            batch_bytes: 1024 * 1024,
            buffer_bytes: 32 * 1024 * 1024,
            flush_interval_ms: 1000,
            timeout_ms: 30_000,
        }
    }
}
impl StreamOptions {
    fn validate(&self) -> Result<()> {
        if self.batch_rows == 0
            || self.batch_rows > 1_000_000
            || self.batch_bytes == 0
            || self.batch_bytes > 16 * 1024 * 1024
            || self.buffer_bytes < self.batch_bytes
            || self.buffer_bytes > 256 * 1024 * 1024
            || self.flush_interval_ms == 0
            || self.flush_interval_ms > 3_600_000
            || self.timeout_ms == 0
            || self.timeout_ms > 3_600_000
        {
            return Err(Error::invalid(
                "positive limits required: batch <= 16 MiB / 1M rows, batch <= buffer <= 256 MiB, timeouts <= 1 hour",
            ));
        }
        Ok(())
    }
}

struct Batch {
    data: RecordBatch,
    _memory: OwnedSemaphorePermit,
}
enum Command {
    Push(Batch),
    Flush(oneshot::Sender<Result<()>>),
}

/// One table-specific producer. Use one mutable writer; create another stream for another producer.
/// A successful flush means validated collector admission, never proof of a downstream sink commit.
pub struct ArrowcolStream<C: Contract> {
    sender: Option<mpsc::Sender<Command>>,
    memory: Arc<Semaphore>,
    failure: watch::Receiver<Option<Error>>,
    worker: Option<tokio::task::JoinHandle<()>>,
    options: StreamOptions,
    terminal_error: Option<Error>,
    _contract: PhantomData<C>,
}

impl<C: Contract> ArrowcolStream<C> {
    pub async fn connect(endpoint: &str, options: StreamOptions) -> Result<Self> {
        options.validate()?;
        let timeout = Duration::from_millis(options.timeout_ms);
        let mut endpoint_config = Endpoint::from_shared(endpoint.to_owned())
            .map_err(Error::invalid)?
            .connect_timeout(timeout)
            .timeout(timeout);
        if endpoint.starts_with("https://") {
            endpoint_config = endpoint_config
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .map_err(Error::invalid)?;
        }
        let channel = endpoint_config.connect().await.map_err(|error| Error {
            kind: "connect_failed",
            message: error.to_string(),
        })?;
        let (sender, receiver) = mpsc::channel(1024);
        let (failure_tx, failure) = watch::channel(None);
        let memory = Arc::new(Semaphore::new(options.buffer_bytes));
        let worker_memory = Arc::clone(&memory);
        let worker_options = options.clone();
        let worker = tokio::spawn(async move {
            let mut receiver = receiver;
            if let Err(error) = worker::<C>(channel, &mut receiver, worker_options).await {
                failure_tx.send_replace(Some(error));
            }
            worker_memory.close();
        });
        Ok(Self {
            sender: Some(sender),
            memory,
            failure,
            worker: Some(worker),
            options,
            terminal_error: None,
            _contract: PhantomData,
        })
    }

    fn failure_or_closed(&self) -> Error {
        self.failure.borrow().clone().unwrap_or_else(Error::closed)
    }
    fn check(&self) -> Result<()> {
        if let Some(error) = &self.terminal_error {
            return Err(error.clone());
        }
        if let Some(error) = self.failure.borrow().clone() {
            return Err(error);
        }
        if self.sender.is_none() {
            return Err(Error::closed());
        }
        Ok(())
    }

    pub async fn push(&mut self, event: C::Event) -> Result<()> {
        self.push_batch(std::slice::from_ref(&event)).await
    }

    /// Validates the entire input before enqueueing. Oversized calls must be split by the caller.
    /// The byte budget bounds retained Arrow buffers, not total process RSS or transient encoding.
    pub async fn push_batch(&mut self, events: &[C::Event]) -> Result<()> {
        self.check()?;
        if events.is_empty() {
            return Ok(());
        }
        if events.len() > self.options.batch_rows {
            return Err(Error::invalid("push_batch exceeds batch_rows; split the input batch"));
        }
        let data = C::record_batch(events)?;
        if data.schema() != C::schema() || data.num_rows() != events.len() {
            return Err(Error::invalid(
                "generated contract returned a different schema or row count",
            ));
        }
        let bytes = data.get_array_memory_size().max(1);
        if bytes > self.options.batch_bytes {
            return Err(Error::invalid("event/batch exceeds batch_bytes; no data was enqueued"));
        }
        let permit = tokio::time::timeout(
            Duration::from_millis(self.options.timeout_ms),
            Arc::clone(&self.memory).acquire_many_owned(bytes as u32),
        )
        .await
        .map_err(|_| Error {
            kind: "backpressure_timeout",
            message: "no data from this call was enqueued".into(),
        })?
        .map_err(|_| self.failure_or_closed())?;
        self.check()?;
        let sender = self.sender.as_ref().ok_or_else(Error::closed)?;
        tokio::time::timeout(
            Duration::from_millis(self.options.timeout_ms),
            sender.send(Command::Push(Batch { data, _memory: permit })),
        )
        .await
        .map_err(|_| Error {
            kind: "backpressure_timeout",
            message: "no data from this call was enqueued".into(),
        })?
        .map_err(|_| self.failure_or_closed())
    }

    /// Barrier for preceding pushes; drains the complete response and checks the admission metadata.
    pub async fn flush(&mut self) -> Result<()> {
        self.check()?;
        let barrier = async {
            let (reply, result) = oneshot::channel();
            self.sender
                .as_ref()
                .ok_or_else(Error::closed)?
                .send(Command::Flush(reply))
                .await
                .map_err(|_| self.failure_or_closed())?;
            result.await.map_err(|_| self.failure_or_closed())?
        };
        match tokio::time::timeout(Duration::from_millis(self.options.timeout_ms), barrier).await {
            Ok(result) => {
                if let Err(error) = &result {
                    self.terminal_error = Some(error.clone());
                    self.sender.take();
                }
                result
            }
            Err(_) => {
                let error = Error::unknown("flush deadline elapsed; queued or in-flight delivery must be reconciled");
                self.terminal_error = Some(error.clone());
                self.sender.take();
                self.memory.close();
                if let Some(worker) = &self.worker {
                    worker.abort();
                }
                Err(error)
            }
        }
    }

    /// Flushes and releases the worker even if flushing fails. Dropping alone does not flush.
    pub async fn close(&mut self) -> Result<()> {
        let result = self.flush().await;
        self.sender.take();
        if let Some(worker) = self.worker.take()
            && let Err(error) = worker.await
            && (!error.is_cancelled() || self.terminal_error.is_none())
        {
            return Err(Error::unknown(error));
        }
        result
    }
}

impl<C: Contract> Drop for ArrowcolStream<C> {
    fn drop(&mut self) {
        if let Some(worker) = &self.worker {
            worker.abort();
        }
        self.memory.close();
    }
}

async fn worker<C: Contract>(
    channel: Channel,
    receiver: &mut mpsc::Receiver<Command>,
    options: StreamOptions,
) -> Result<()> {
    let mut client = FlightServiceClient::new(channel).max_encoding_message_size(32 * 1024 * 1024);
    let mut pending = Vec::<Batch>::new();
    let mut rows = 0;
    let mut bytes = 0;
    let interval = Duration::from_millis(options.flush_interval_ms);
    let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let barrier = tokio::select! {
            message = receiver.recv() => match message {
                None => return Ok(()),
                Some(Command::Push(batch)) => {
                    if !pending.is_empty() && (rows + batch.data.num_rows() > options.batch_rows ||
                        bytes + batch.data.get_array_memory_size() > options.batch_bytes) {
                        send::<C>(&mut client, &pending, options.timeout_ms).await?;
                        pending.clear(); rows = 0; bytes = 0;
                    }
                    rows += batch.data.num_rows();
                    bytes += batch.data.get_array_memory_size();
                    pending.push(batch);
                    if rows < options.batch_rows && bytes < options.batch_bytes { continue; }
                    None
                },
                Some(Command::Flush(reply)) => Some(reply),
            },
            _ = tick.tick() => None,
        };
        let result = if pending.is_empty() {
            Ok(())
        } else {
            send::<C>(&mut client, &pending, options.timeout_ms).await
        };
        if let Some(reply) = barrier {
            let _ = reply.send(result.clone());
        }
        result?;
        pending.clear();
        rows = 0;
        bytes = 0;
    }
}

async fn send<C: Contract>(
    client: &mut FlightServiceClient<Channel>,
    batches: &[Batch],
    timeout_ms: u64,
) -> Result<()> {
    let batch = arrow_select::concat::concat_batches(&C::schema(), batches.iter().map(|batch| &batch.data))
        .map_err(Error::invalid)?;
    let expected_rows = batch.num_rows();
    let encoded = FlightDataEncoderBuilder::new()
        .with_schema(C::schema())
        .with_flight_descriptor(Some(FlightDescriptor::new_path(C::descriptor())))
        .build(futures::stream::iter([Ok(batch)]))
        .try_collect::<Vec<_>>()
        .await
        .map_err(Error::invalid)?;
    let mut request = Request::new(futures::stream::iter(encoded));
    request.set_timeout(Duration::from_millis(timeout_ms));
    for (key, value) in C::metadata() {
        request.metadata_mut().insert(
            key.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>()
                .map_err(Error::invalid)?,
            value.parse().map_err(Error::invalid)?,
        );
    }
    tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        let mut response = client.do_put(request).await.map_err(Error::unknown)?.into_inner();
        let mut completed = false;
        while let Some(result) = response.message().await.map_err(Error::unknown)? {
            let value: serde_json::Value = serde_json::from_slice(&result.app_metadata).map_err(Error::unknown)?;
            if completed {
                return Err(Error::unknown("unexpected result after admission completion"));
            }
            if value["wire_version"] != 1 || value["protocol_version"] != 1 {
                return Err(Error::unknown("unsupported admission protocol"));
            }
            if value["kind"] == "progress" {
                continue;
            }
            let guarantee = match value["kind"].as_str() {
                Some("best_effort_admission") => "best_effort",
                Some("durable_admission") => "durable_admission",
                _ => return Err(Error::unknown("missing recognized admission completion")),
            };
            if value["delivery_guarantee"] != guarantee
                || value["source_schema_version"] != C::version()
                || value["target_schema_version"]
                    .as_u64()
                    .is_none_or(|version| version < C::version())
                || value["schema_fingerprint"] != C::fingerprint()
                || value["summary"]["rows"].as_u64() != Some(expected_rows as u64)
            {
                return Err(Error::unknown(
                    "admission completion does not match the submitted contract/rows",
                ));
            }
            completed = true;
        }
        if !completed {
            return Err(Error::unknown("Flight ended without admission completion"));
        }
        Ok(())
    })
    .await
    .map_err(|_| Error::unknown("Flight deadline elapsed; data may have been admitted"))?
}
