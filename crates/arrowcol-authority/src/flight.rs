//! Native Flight forwarding and a deliberately explicit manifest collector adapter.
use crate::*;
use anyhow::{Result, ensure};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PollInfo, PutResult, SchemaResult, Ticket,
    decode::FlightRecordBatchStream,
    flight_service_client::FlightServiceClient,
    flight_service_server::{FlightService, FlightServiceServer},
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use hmac::{Hmac, Mac};
use prost::Message;
use sha2::Sha256;
use std::{
    collections::{BTreeMap, HashMap},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, metadata::MetadataMap};

type ResponseStream<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;
const MAX_MESSAGE: usize = 2 * 1024 * 1024;
const HEADERS: [&str; 8] = [
    "x-arrowcol-protocol-version",
    "x-arrowcol-implementation-pool",
    "x-arrowcol-catalog",
    "x-arrowcol-schema",
    "x-arrowcol-table",
    "x-arrowcol-schema-fingerprint",
    "x-arrowcol-control-operation",
    "content-type",
];
fn metadata<'a>(map: &'a MetadataMap, key: &str) -> &'a str {
    map.get(key).and_then(|v| v.to_str().ok()).unwrap_or("")
}
fn status(error: impl std::fmt::Display) -> Status {
    let message = error.to_string();
    if message.contains("NOT_OWNER") || message.contains("unavailable") || message.contains("timed out") {
        Status::unavailable(message)
    } else {
        Status::failed_precondition(message)
    }
}
/// # Errors
/// Returns invalid signing-key errors.
pub fn sign(secret: &str, value: &str) -> Result<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(value.as_bytes());
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}
fn verify(secret: &str, value: &str, signature: &str) -> Result<()> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
    mac.update(value.as_bytes());
    mac.verify_slice(&URL_SAFE_NO_PAD.decode(signature)?)?;
    Ok(())
}
#[derive(Default)]
pub struct Directory {
    pub entries: BTreeMap<String, Assignment>,
    pub revision: i64,
    pub last_snapshot: Option<Instant>,
    pub snapshots: usize,
    pub resets: usize,
    pub lookup_refreshes: usize,
    pub stale_invalidations: usize,
    pub route_error_invalidations: usize,
}
impl Directory {
    /// # Errors
    /// Rejects snapshots exceeding the bounded catalog.
    pub fn snapshot(&mut self, revision: i64, assignments: Vec<Assignment>) -> Result<()> {
        ensure!(assignments.len() <= 1024, "directory bound");
        if revision >= self.revision {
            self.snapshots = self.snapshots.saturating_add(1);
            for assignment in &assignments {
                Self::applied("snapshot", assignment);
            }
            self.entries = assignments.into_iter().map(|a| (a.record.key.clone(), a)).collect();
            self.revision = revision;
            self.last_snapshot = Some(Instant::now());
        }
        Ok(())
    }
    /// # Errors
    /// Rejects regressed revisions or cache overflow, requiring a resnapshot.
    pub fn updates(&mut self, revision: i64, assignments: Vec<Assignment>) -> Result<()> {
        ensure!(
            revision >= self.revision,
            "watch revision regressed; resnapshot required"
        );
        for assignment in assignments {
            if self
                .entries
                .get(&assignment.record.key)
                .is_none_or(|old| old.revision < assignment.revision)
            {
                Self::applied("watch", &assignment);
                self.entries.insert(assignment.record.key.clone(), assignment);
            }
        }
        ensure!(self.entries.len() <= 1024, "directory bound");
        self.revision = revision;
        self.last_snapshot = Some(Instant::now());
        Ok(())
    }
    fn applied(kind: &str, assignment: &Assignment) {
        eprintln!(
            "{}",
            serde_json::json!({"event":"directory_applied","kind":kind,"observed_ms":now_ms(),
                "key":assignment.record.key,"assignment_revision":assignment.revision,
                "epoch":assignment.record.epoch,"lifecycle":assignment.record.lifecycle})
        );
    }
    pub fn invalidate(&mut self, key: &str, error: &Status) {
        self.entries.remove(key);
        self.route_error_invalidations = self.route_error_invalidations.saturating_add(1);
        if error.message().contains("NOT_OWNER") {
            self.stale_invalidations = self.stale_invalidations.saturating_add(1);
        }
    }
}
#[derive(Clone)]
enum Role {
    Gateway,
    Collector(Arc<Mutex<Writer>>),
}
#[derive(Clone)]
pub struct Service {
    config: Arc<Config>,
    client: Client,
    role: Role,
    directory: Arc<Mutex<Directory>>,
    nonces: Arc<Mutex<HashMap<String, u64>>>,
    slots: Arc<Semaphore>,
    writers: Arc<BTreeMap<String, Arc<Mutex<()>>>>,
    active: Arc<AtomicUsize>,
    observations: Arc<observation::Counters>,
    runtime: local::RuntimeGate,
}
impl Service {
    fn publisher(&self) -> Result<Client> {
        Client::new(
            self.config
                .delta
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Delta mode disabled"))?
                .publisher
                .clone(),
            self.client.token.clone(),
        )
    }
    /// # Errors
    /// Returns control-client configuration errors.
    pub fn gateway(config: Config) -> Result<Self> {
        let client = Client::new(config.controller.clone(), config.gateway_token.clone())?;
        Ok(Self::new(config, client, Role::Gateway))
    }
    /// # Errors
    /// Rejects unregistered collectors or invalid control-client configuration.
    pub fn collector(config: Config, id: &str) -> Result<Self> {
        let token = config
            .collectors
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("unregistered collector"))?
            .clone();
        let client = Client::new(config.controller.clone(), token)?;
        let writer = Writer {
            collector: id.into(),
            process: uuid::Uuid::new_v4().to_string(),
            generation: String::new(),
        };
        Ok(Self::new(config, client, Role::Collector(Arc::new(Mutex::new(writer)))))
    }
    fn new(config: Config, client: Client, role: Role) -> Self {
        let writers = config
            .tables
            .iter()
            .map(|t| (t.key.clone(), Arc::new(Mutex::new(()))))
            .collect();
        Self {
            config: Arc::new(config),
            client,
            role,
            directory: Arc::new(Mutex::new(Directory::default())),
            nonces: Arc::new(Mutex::new(HashMap::new())),
            slots: Arc::new(Semaphore::new(16)),
            writers: Arc::new(writers),
            active: Arc::new(AtomicUsize::new(0)),
            observations: Arc::new(observation::Counters::default()),
            runtime: local::RuntimeGate::default(),
        }
    }
    /// Drain admitted streams/encoding, permanently retire this runtime, and reuse
    /// the shared per-table mailboxes in its successor. Controller rotates the
    /// exact session generation and owned-table epochs atomically at the head fence.
    ///
    /// # Errors
    /// Rejects replacement of a retired runtime.
    pub async fn replace_runtime(&self) -> Result<Self> {
        let mut successor = self.clone();
        successor.runtime = self.runtime.replace().await?;
        let Role::Collector(writer) = &self.role else {
            anyhow::bail!("only collector runtimes have writer generations");
        };
        let previous = writer.lock().await.clone();
        let response = self
            .client
            .call(&Operation::ReplaceSession { writer: previous })
            .await?;
        let writer = serde_json::from_value(response["writer"].clone())?;
        successor.role = Role::Collector(Arc::new(Mutex::new(writer)));
        Ok(successor)
    }
    async fn authenticate(&self, headers: &MetadataMap, first: &FlightData) -> Result<Table> {
        ensure!(
            headers.iter().map(|v| format!("{v:?}").len()).sum::<usize>() <= 16 * 1024,
            "metadata bound"
        );
        ensure!(
            metadata(headers, "x-arrowcol-internal-auth-context").is_empty()
                && metadata(headers, "x-arrowcol-internal-identity-bin").is_empty(),
            "foreign identity assertion"
        );
        let timestamp = metadata(headers, "x-arrowcol-edge-timestamp");
        let time: u64 = timestamp.parse()?;
        ensure!(now_ms().abs_diff(time) <= 30_000, "expired edge signature");
        let nonce = metadata(headers, "x-arrowcol-edge-nonce");
        ensure!(!nonce.is_empty() && nonce.len() <= 128, "nonce bound");
        let mut canonical = format!("{timestamp}\n{nonce}\nPOST\n/arrow.flight.protocol.FlightService/DoPut\n");
        for key in HEADERS {
            canonical.push_str(&format!("{key}:{}\n", metadata(headers, key)));
        }
        verify(
            &self.config.edge_secret,
            &canonical,
            metadata(headers, "x-arrowcol-edge-signature"),
        )?;
        let mut nonces = self.nonces.lock().await;
        nonces.retain(|_, expiry| *expiry > now_ms());
        ensure!(
            nonces.len() < 4096 && !nonces.contains_key(nonce),
            "replayed/overloaded edge auth"
        );
        nonces.insert(nonce.to_owned(), now_ms() + 30_000);
        drop(nonces);
        ensure!(
            metadata(headers, "x-arrowcol-protocol-version") == "1"
                && metadata(headers, "x-arrowcol-implementation-pool") == "direct",
            "protocol mismatch"
        );
        let path = first
            .flight_descriptor
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing first descriptor"))?;
        ensure!(
            path.r#type == 1 && path.cmd.is_empty() && path.path.iter().map(String::len).sum::<usize>() <= 1024,
            "descriptor bound/type"
        );
        ensure!(path.path.len() == 4, "versioned descriptor required");
        let table = self
            .config
            .tables
            .iter()
            .find(|t| t.path == path.path[..3])
            .ok_or_else(|| anyhow::anyhow!("unauthorized table identity"))?;
        let version = path.path[3]
            .strip_prefix('v')
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or_else(|| anyhow::anyhow!("descriptor schema version"))?;
        ensure!(
            version > 0
                && table
                    .fingerprints
                    .get(version - 1)
                    .is_some_and(|fp| fp == metadata(headers, "x-arrowcol-schema-fingerprint")),
            "descriptor schema version binding"
        );
        ensure!(
            table
                .path
                .iter()
                .zip(["x-arrowcol-catalog", "x-arrowcol-schema", "x-arrowcol-table"])
                .all(|(v, k)| v == metadata(headers, k)),
            "descriptor/header mismatch"
        );
        ensure!(
            table
                .fingerprints
                .contains(&metadata(headers, "x-arrowcol-schema-fingerprint").to_owned()),
            "unauthorized schema version"
        );
        Ok(table.clone())
    }
    async fn resolve(&self, key: &str) -> Result<Assignment> {
        // Holding this bounded lock coalesces concurrent misses; requests cannot fan out lookup storms.
        let mut directory = self.directory.lock().await;
        if directory
            .last_snapshot
            .is_some_and(|at| at.elapsed() < Duration::from_millis(300))
            && let Some(assignment) = directory.entries.get(key)
            && assignment.record.lifecycle == Lifecycle::Active
        {
            return Ok(assignment.clone());
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        directory.lookup_refreshes = directory.lookup_refreshes.saturating_add(1);
        let assignment = loop {
            let value = self.client.call(&Operation::Resolve { key: key.to_owned() }).await?;
            let assignment: Assignment = serde_json::from_value(value)?;
            if assignment.record.lifecycle == Lifecycle::Active {
                break assignment;
            }
            ensure!(Instant::now() < deadline, "owner recovery/drain deadline");
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        if directory
            .entries
            .get(key)
            .is_none_or(|a| a.revision <= assignment.revision)
        {
            Directory::applied("resolve", &assignment);
            directory.entries.insert(key.into(), assignment.clone());
        }
        directory.last_snapshot = Some(Instant::now());
        Ok(assignment)
    }
    pub async fn background(&self) {
        let mut snapshotted = false;
        let mut cpu_previous: Option<(Instant, u64)> = None;
        loop {
            let Ok(_runtime) = self.runtime.enter().await else {
                return;
            };
            let result: Result<()> = async {
                if matches!(self.role, Role::Gateway) && snapshotted {
                    let after = self.directory.lock().await.revision;
                    let value = self.client.call(&Operation::Watch { after }).await?;
                    if value["reset"] == true {
                        eprintln!(
                            "{}",
                            serde_json::json!({"event":"directory_watch_reset",
                            "observed_ms":now_ms(),"after":after,"compact_revision":value["compact_revision"]})
                        );
                    }
                    ensure!(value["reset"] != true, "watch compacted/gapped; resnapshot required");
                    let mut directory = self.directory.lock().await;
                    let revision = value["revision"].as_i64().unwrap_or(0);
                    let assignments = serde_json::from_value(value["assignments"].clone())?;
                    if value["snapshot"] == true {
                        directory.snapshot(revision, assignments)?;
                    } else {
                        directory.updates(revision, assignments)?;
                    }
                    return Ok(());
                }
                if let Role::Collector(writer) = &self.role {
                    let mut writer_guard = writer.lock().await;
                    let writer = writer_guard.clone();
                    let rss = std::fs::read_to_string("/proc/self/status")?
                        .lines()
                        .find(|line| line.starts_with("VmRSS:"))
                        .and_then(|line| line.split_whitespace().nth(1))
                        .and_then(|n| n.parse::<u64>().ok())
                        .unwrap_or(1)
                        * 1024;
                    let active = self.active.load(Ordering::Relaxed);
                    let cpu = std::fs::read_to_string("/sys/fs/cgroup/cpu.stat")
                        .ok()
                        .and_then(|raw| {
                            raw.lines()
                                .find_map(|line| line.strip_prefix("usage_usec ").and_then(|n| n.parse::<u64>().ok()))
                        })
                        .unwrap_or(0);
                    let sampled = Instant::now();
                    let cpu_pressure = cpu_previous.map_or(0.0, |(at, previous)| {
                        cpu.saturating_sub(previous) as f64 / (sampled.duration_since(at).as_secs_f64() * 1_000_000.)
                    });
                    cpu_previous = Some((sampled, cpu));
                    let registration = self
                        .client
                        .call(&Operation::Register {
                            member: Member {
                                endpoint: format!("http://{}:50051", writer.collector),
                                writer: writer.clone(),
                                pressure: (rss as f64 / (1024. * 1024. * 1024.))
                                    .max(active as f64 / 32.)
                                    .max(cpu_pressure),
                                rss_bytes: rss,
                                active,
                                eligible: true,
                            },
                        })
                        .await?;
                    *writer_guard = serde_json::from_value(registration["writer"].clone())?;
                }
                let view = self.client.call(&Operation::View).await?;
                let assignments: Vec<Assignment> = serde_json::from_value(view["assignments"].clone())?;
                if let Role::Collector(writer) = &self.role {
                    let writer = writer.lock().await.clone();
                    for assignment in &assignments {
                        let record = &assignment.record;
                        if record.writer == writer && record.lifecycle == Lifecycle::Recovering {
                            if self.config.delta.is_some() {
                                self.publisher()?
                                    .call(&Operation::RecoverDelta {
                                        key: record.key.clone(),
                                        writer: writer.clone(),
                                        epoch: record.epoch,
                                    })
                                    .await?;
                                continue;
                            }
                            // Recover final post-cutover committed head before conditional activation.
                            if let Some(head) = &record.head {
                                storage::validate(&self.config, head)?;
                            }
                            self.client
                                .call(&Operation::Activate {
                                    key: record.key.clone(),
                                    writer: writer.clone(),
                                    epoch: record.epoch,
                                    observed: record.head.clone(),
                                })
                                .await?;
                        }
                    }
                }
                self.directory
                    .lock()
                    .await
                    .snapshot(view["revision"].as_i64().unwrap_or(0), assignments)?;
                snapshotted = true;
                Ok(())
            }
            .await;
            if let Err(error) = result {
                if snapshotted && matches!(self.role, Role::Gateway) {
                    let mut directory = self.directory.lock().await;
                    directory.resets = directory.resets.saturating_add(1);
                }
                snapshotted = false;
                eprintln!(
                    "{}",
                    serde_json::json!({"event":"directory_error","error":error.to_string()})
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    async fn observe(&self) {
        loop {
            let directory = self.directory.try_lock().ok();
            let cache = directory.as_ref().map(|d| {
                serde_json::json!({
                    "applied_directory_revision":d.revision,"entries":d.entries.len(),
                    "snapshots":d.snapshots,"resets":d.resets,"lookup_refreshes":d.lookup_refreshes,
                    "stale_invalidations":d.stale_invalidations,
                    "route_error_invalidations":d.route_error_invalidations
                })
            });
            let role = if matches!(self.role, Role::Gateway) {
                "gateway"
            } else {
                "collector"
            };
            eprintln!(
                "{}",
                serde_json::json!({"event":"flight_observation","observed_ms":now_ms(),"role":role,
                    "cache":cache,"cache_sample_deferred":directory.is_none(),
                    "gateway_input_bytes":self.observations.gateway_input_bytes.sample(),
                    "gateway_result_bytes":self.observations.gateway_result_bytes.sample(),
                    "mailbox_waiters":self.observations.mailbox_waiters.sample(),
                    "writer_streams":self.observations.writer_streams.sample(),
                    "decoded_batch_bytes":self.observations.decoded_batch_bytes.sample()})
            );
            drop(directory);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    async fn put(
        &self,
        request: Request<tonic::Streaming<FlightData>>,
    ) -> std::result::Result<Response<ResponseStream<PutResult>>, Status> {
        let permit = Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("stream capacity"))?;
        let headers = request.metadata().clone();
        let mut inbound = request.into_inner();
        let first = tokio::time::timeout(Duration::from_secs(5), inbound.message())
            .await
            .map_err(|_| Status::deadline_exceeded("initial envelope"))??
            .ok_or_else(|| Status::invalid_argument("empty stream"))?;
        if first.data_header.len() + first.data_body.len() > 1024 * 1024 {
            return Err(Status::resource_exhausted("schema bound"));
        }
        let table = self
            .authenticate(&headers, &first)
            .await
            .map_err(|_| Status::unauthenticated("identity/descriptor/schema binding rejected"))?;
        match &self.role {
            Role::Gateway => {
                let first_held = self.observations.gateway_input_bytes.hold(first.encoded_len());
                if !metadata(&headers, "x-adr-route").is_empty() {
                    return Err(Status::unauthenticated("external internal-route assertion"));
                }
                let assignment = self.resolve(&table.key).await.map_err(status)?;
                let route = STANDARD.encode(serde_json::to_vec(&assignment).map_err(status)?);
                let assertion = format!(
                    "{}\n{}\n{}",
                    now_ms(),
                    metadata(&headers, "x-arrowcol-edge-signature"),
                    route
                );
                let signature = sign(&self.config.internal_secret, &assertion).map_err(status)?;
                let endpoint = tonic::transport::Endpoint::from_shared(assignment.record.endpoint.clone())
                    .map_err(status)?
                    .connect_timeout(Duration::from_secs(2))
                    .timeout(Duration::from_secs(30));
                let mut client = FlightServiceClient::new(
                    endpoint
                        .connect()
                        .await
                        .map_err(|e| Status::unavailable(e.to_string()))?,
                )
                .max_decoding_message_size(MAX_MESSAGE)
                .max_encoding_message_size(MAX_MESSAGE);
                let (tx, rx) = mpsc::channel(4);
                tx.send((first, first_held)).await.map_err(status)?;
                let input_bytes = self.observations.gateway_input_bytes.clone();
                let (input_error_tx, mut input_error_rx) = tokio::sync::oneshot::channel();
                let forward = tokio::spawn(async move {
                    let result = async {
                        while let Some(message) = inbound.message().await? {
                            let held = input_bytes.hold(message.encoded_len());
                            if tx.send((message, held)).await.is_err() {
                                return Ok::<(), Status>(());
                            }
                        }
                        Ok(())
                    }
                    .await;
                    drop(tx);
                    if let Err(error) = result {
                        let _ = input_error_tx.send(error);
                    }
                });
                let mut outgoing = Request::new(ReceiverStream::new(rx).map(|(message, _held)| message));
                *outgoing.metadata_mut() = headers;
                outgoing
                    .metadata_mut()
                    .insert("x-adr-route", route.parse().map_err(status)?);
                outgoing
                    .metadata_mut()
                    .insert("x-adr-assertion", STANDARD.encode(&assertion).parse().map_err(status)?);
                outgoing
                    .metadata_mut()
                    .insert("x-adr-signature", signature.parse().map_err(status)?);
                outgoing.set_timeout(Duration::from_secs(30));
                let response = match client.do_put(outgoing).await {
                    Ok(r) => r,
                    Err(error) => {
                        forward.abort();
                        self.directory.lock().await.invalidate(&table.key, &error);
                        return Err(error);
                    }
                };
                let (parts, mut response, extensions) = response.into_parts();
                let (tx, rx) = mpsc::channel(4);
                let cache = Arc::clone(&self.directory);
                let key = table.key;
                let result_bytes = self.observations.gateway_result_bytes.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let deadline = tokio::time::sleep(Duration::from_secs(30));
                    tokio::pin!(deadline);
                    loop {
                        tokio::select! {
                            _=tx.closed()=>break,
                            _=&mut deadline=>{let _=tx.send((Err(Status::deadline_exceeded("gateway stream lifetime")),None)).await;break;},
                            result=&mut input_error_rx, if !input_error_rx.is_terminated()=>{if let Ok(error)=result {let _=tx.send((Err(error),None)).await;break;}},
                            item=response.message()=>{
                                match item {
                                    Ok(Some(value))=>{
                                        let held=result_bytes.hold(value.encoded_len());
                                        if tx.send((Ok(value),Some(held))).await.is_err(){break;}
                                    },
                                    Ok(None)=>break,
                                    Err(error)=>{cache.lock().await.invalidate(&key,&error);let _=tx.send((Err(error),None)).await;break;}
                                }
                            }
                        }
                    }
                    forward.abort();
                });
                Ok(Response::from_parts(
                    parts,
                    Box::pin(ReceiverStream::new(rx).map(|(value, _held)| value)),
                    extensions,
                ))
            }
            Role::Collector(writer) => {
                let route = metadata(&headers, "x-adr-route");
                let assertion =
                    String::from_utf8(STANDARD.decode(metadata(&headers, "x-adr-assertion")).map_err(status)?)
                        .map_err(status)?;
                let fields = assertion.split('\n').collect::<Vec<_>>();
                if fields.len() != 3
                    || fields[1] != metadata(&headers, "x-arrowcol-edge-signature")
                    || fields[2] != route
                {
                    return Err(Status::unauthenticated("invalid internal binding"));
                }
                let time = fields[0].parse::<u64>().map_err(status)?;
                if now_ms().abs_diff(time) > 30_000 {
                    return Err(Status::unauthenticated("expired internal assertion"));
                }
                verify(
                    &self.config.internal_secret,
                    &assertion,
                    metadata(&headers, "x-adr-signature"),
                )
                .map_err(|_| Status::unauthenticated("internal signature"))?;
                let assignment: Assignment =
                    serde_json::from_slice(&STANDARD.decode(route).map_err(status)?).map_err(status)?;
                let writer = writer.lock().await.clone();
                if assignment.record.key != table.key || assignment.record.writer != writer {
                    return Err(Status::unavailable("NOT_OWNER"));
                }
                let mailbox = Arc::clone(self.writers.get(&table.key).ok_or_else(|| status("missing mailbox"))?);
                let service = self.clone();
                let expected_fingerprint = metadata(&headers, "x-arrowcol-schema-fingerprint").to_owned();
                let descriptor = first.flight_descriptor.clone();
                let (tx, rx) = mpsc::channel(4);
                self.active.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let _permit = permit;
                    let result = tokio::time::timeout(Duration::from_secs(30), async {
                        let _legacy_mailbox = if service.config.delta.is_none() {
                            let waiting = service.observations.mailbox_waiters.hold(1);
                            let guard = Arc::clone(&mailbox).lock_owned().await;
                            drop(waiting);
                            Some((guard, service.observations.writer_streams.hold(1)))
                        } else {
                            None
                        };
                        let runtime = service.runtime.enter().await.map_err(status)?;
                        let inbound = inbound.map(move |message| {
                            message.and_then(|message| {
                                if message.flight_descriptor.is_some() && message.flight_descriptor != descriptor {
                                    Err(Status::invalid_argument("table/descriptor changed within stream"))
                                } else {
                                    Ok(message)
                                }
                            })
                        });
                        let input = stream::once(async { Ok(first) }).chain(inbound);
                        let input = input.map_err(arrow_flight::error::FlightError::from);
                        let mut batches = FlightRecordBatchStream::new_from_flight_data(input);
                        let mut total = 0usize;
                        while let Some(batch) = batches.try_next().await.map_err(status)? {
                            let fingerprint =
                                arrowcol_protocol::fingerprint_schema(batch.schema().as_ref()).to_string();
                            if fingerprint != expected_fingerprint || !table.fingerprints.contains(&fingerprint) {
                                return Err(status("decoded schema not registered"));
                            }
                            let bytes = batch.get_array_memory_size();
                            total = total.saturating_add(bytes);
                            if bytes > MAX_MESSAGE || total > 8 * 1024 * 1024 {
                                return Err(Status::resource_exhausted("unit/stream retained memory bound"));
                            }
                            let held_batch = service.observations.decoded_batch_bytes.hold(bytes);
                            // Waiting streams must drain a bounded batch before taking the
                            // publication mailbox: otherwise their unread DATA can consume
                            // the shared H2 connection window and deadlock its active writer.
                            let _delta_mailbox = if service.config.delta.is_some() {
                                let waiting = service.observations.mailbox_waiters.hold(1);
                                let guard = Arc::clone(&mailbox).lock_owned().await;
                                drop(waiting);
                                Some((guard, service.observations.writer_streams.hold(1)))
                            } else {
                                None
                            };
                            if tx.is_closed() {
                                return Err(Status::cancelled("client disconnected"));
                            }
                            let unit = uuid::Uuid::new_v4().to_string();
                            let admitted = service
                                .client
                                .call(&Operation::Admit {
                                    key: table.key.clone(),
                                    writer: writer.clone(),
                                    epoch: assignment.record.epoch,
                                    unit: unit.clone(),
                                })
                                .await
                                .map_err(status)?;
                            let admitted: Assignment = serde_json::from_value(admitted).map_err(status)?;
                            if service.config.delta.is_some() {
                                tx.send(Ok(PutResult {
                                    app_metadata: serde_json::to_vec(&serde_json::json!({
                                        "kind":"DELTA_OPERATION","operation":unit,"final":false
                                    }))
                                    .map_err(status)?
                                    .into(),
                                }))
                                .await
                                .map_err(|_| Status::cancelled("operation identity client closed"))?;
                                let path = service.config.storage.join(format!("{unit}.parquet"));
                                let sha256 = crate::sql_publication::stage_batch(&path, &batch)
                                    .await
                                    .map_err(status)?;
                                drop(held_batch);
                                let receipt = service
                                    .publisher()
                                    .map_err(status)?
                                    .call(&Operation::PublishDelta {
                                        key: table.key.clone(),
                                        writer: writer.clone(),
                                        epoch: assignment.record.epoch,
                                        unit,
                                        sha256,
                                    })
                                    .await
                                    .map_err(status)?;
                                tx.send(Ok(PutResult {
                                    app_metadata: serde_json::to_vec(&receipt).map_err(status)?.into(),
                                }))
                                .await
                                .map_err(|_| Status::cancelled("receipt client closed"))?;
                                continue;
                            }
                            let manifest = Manifest {
                                table: table.key.clone(),
                                writer: writer.clone(),
                                epoch: assignment.record.epoch,
                                commit: uuid::Uuid::new_v4().to_string(),
                                unit,
                                parent: admitted.record.head,
                                parquet: String::new(),
                                parquet_sha256: String::new(),
                                rows: batch.num_rows(),
                                arrow_bytes: bytes,
                                fingerprint,
                            };
                            let config = Arc::clone(&service.config);
                            // A canceled async stream must still drain blocking encoding before replacement.
                            let encoding_runtime = Arc::clone(&runtime);
                            let head = tokio::task::spawn_blocking(move || {
                                let _runtime = encoding_runtime;
                                let _held_batch = held_batch;
                                storage::stage(&config, manifest, &batch)
                            })
                            .await
                            .map_err(status)?
                            .map_err(status)?;
                            let receipt = service
                                .client
                                .call(&Operation::Commit {
                                    key: table.key.clone(),
                                    writer: writer.clone(),
                                    epoch: assignment.record.epoch,
                                    head,
                                })
                                .await
                                .map_err(status)?;
                            tx.send(Ok(PutResult {
                                app_metadata: serde_json::to_vec(&receipt).map_err(status)?.into(),
                            }))
                            .await
                            .map_err(|_| Status::cancelled("receipt client closed"))?;
                        }
                        Ok::<(), Status>(())
                    })
                    .await
                    .unwrap_or_else(|_| Err(Status::deadline_exceeded("collector lifetime")));
                    if let Err(error) = result {
                        let _ = tx.send(Err(error)).await;
                    }
                    service.active.fetch_sub(1, Ordering::Relaxed);
                });
                Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
            }
        }
    }
}
#[tonic::async_trait]
impl FlightService for Service {
    type HandshakeStream = ResponseStream<HandshakeResponse>;
    type ListFlightsStream = ResponseStream<FlightInfo>;
    type DoGetStream = ResponseStream<FlightData>;
    type DoPutStream = ResponseStream<PutResult>;
    type DoExchangeStream = ResponseStream<FlightData>;
    type DoActionStream = ResponseStream<arrow_flight::Result>;
    type ListActionsStream = ResponseStream<ActionType>;
    async fn handshake(
        &self,
        _: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list"))
    }
    async fn get_flight_info(&self, _: Request<FlightDescriptor>) -> std::result::Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("info"))
    }
    async fn poll_flight_info(&self, _: Request<FlightDescriptor>) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll"))
    }
    async fn get_schema(&self, _: Request<FlightDescriptor>) -> std::result::Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("schema"))
    }
    async fn do_get(&self, _: Request<Ticket>) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("get"))
    }
    async fn do_put(
        &self,
        request: Request<tonic::Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        self.put(request).await
    }
    async fn do_exchange(
        &self,
        _: Request<tonic::Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("exchange"))
    }
    async fn do_action(&self, _: Request<Action>) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("action"))
    }
    async fn list_actions(&self, _: Request<Empty>) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("actions"))
    }
}
/// # Errors
/// Returns invalid bind addresses or Flight transport errors.
pub async fn serve(service: Service, listen: &str) -> Result<()> {
    let background = service.clone();
    tokio::spawn(async move { background.background().await });
    let observation = service.clone();
    tokio::spawn(async move { observation.observe().await });
    tonic::transport::Server::builder()
        .add_service(
            FlightServiceServer::new(service)
                .max_decoding_message_size(MAX_MESSAGE)
                .max_encoding_message_size(MAX_MESSAGE),
        )
        .serve(listen.parse()?)
        .await?;
    Ok(())
}
