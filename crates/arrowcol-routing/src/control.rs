use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrowcol_protocol::{CollectorInstanceId, UnixMillis};
use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use serde::{Deserialize, Serialize};

use crate::{
    CollectorRecord, CollectorRegistration, CollectorState, LeaseCas, LeaseClaim, LeaseKey, LeaseSuccession,
    LeaseTransfer, MigrationAck, RichPressureSnapshot, RoutingError, RoutingErrorKind, RoutingLease, RoutingResult,
    RoutingStore, RoutingView, TableMigration,
};

/// A separately provisioned secret for one participant. Collector keys bind registration identity.
#[derive(Clone)]
pub struct ControlCredential {
    pub token: String,
    pub role: String,
    pub instance_key: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "operation", content = "request", rename_all = "snake_case")]
enum Request {
    View,
    Register(CollectorRegistration),
    Heartbeat(CollectorInstanceId, UnixMillis, UnixMillis),
    State(CollectorInstanceId, CollectorState, UnixMillis),
    Pressure(RichPressureSnapshot),
    Claim(LeaseClaim),
    MigrationWork(CollectorInstanceId),
    MigrationAck(TableMigration, CollectorInstanceId, MigrationAck),
    Migrate(LeaseKey, String, bool),
    DrainCollector(CollectorInstanceId),
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
enum Response {
    Done,
    View {
        collectors: Vec<CollectorRecord>,
        leases: Vec<RoutingLease>,
        loaded_at: UnixMillis,
    },
    Lease(RoutingLease),
    Migrations(Vec<TableMigration>),
    Migration(TableMigration),
    Error(RoutingError),
}

struct Authority {
    store: Arc<dyn RoutingStore>,
    credentials: Vec<ControlCredential>,
}

fn unavailable(message: impl std::fmt::Display) -> RoutingError {
    RoutingError::new(RoutingErrorKind::Database, format!("private controller: {message}"))
}

pub(crate) fn current_time() -> RoutingResult<UnixMillis> {
    UnixMillis::new(
        u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(unavailable)?
                .as_millis(),
        )
        .map_err(unavailable)?,
    )
    .map_err(unavailable)
}

/// Serves a private bounded API. Bind only on the task's private container network.
///
/// # Errors
///
/// Returns listener or service failures; never starts another authority.
pub async fn serve_control(
    address: SocketAddr,
    store: Arc<dyn RoutingStore>,
    credentials: Vec<ControlCredential>,
) -> RoutingResult<()> {
    let app = control_app(store, credentials)?;
    let listener = tokio::net::TcpListener::bind(address).await.map_err(unavailable)?;
    axum::serve(listener, app).await.map_err(unavailable)
}

fn control_app(store: Arc<dyn RoutingStore>, credentials: Vec<ControlCredential>) -> RoutingResult<Router> {
    if !valid_credentials(&credentials) {
        return Err(unavailable("invalid control credentials"));
    }
    Ok(Router::new()
        .route("/v1/routing", post(handle))
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(Arc::new(Authority { store, credentials })))
}

fn valid_credentials(credentials: &[ControlCredential]) -> bool {
    !credentials.is_empty()
        && credentials.iter().enumerate().all(|(index, credential)| {
            credential.token.len() >= 32
                && credential.token.bytes().all(|byte| byte.is_ascii_graphic())
                && matches!(credential.role.as_str(), "collector" | "data_router" | "operator")
                && (credential.role == "collector") == credential.instance_key.is_some()
                && credentials[..index].iter().all(|other| other.token != credential.token)
        })
}

async fn handle(
    State(authority): State<Arc<Authority>>,
    headers: HeaderMap,
    Json(request): Json<Request>,
) -> Result<Json<Response>, StatusCode> {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let credential = authority
        .credentials
        .iter()
        .find(|c| constant_eq(c.token.as_bytes(), token.as_bytes()))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let operation = match &request {
        Request::View => "view",
        Request::Register(_) => "register",
        Request::Heartbeat(..) => "heartbeat",
        Request::State(..) => "state",
        Request::Pressure(_) => "pressure",
        Request::Claim(_) => "claim",
        Request::MigrationWork(_) => "migration_work",
        Request::MigrationAck(..) => "migration_ack",
        Request::Migrate(..) => "migrate",
        Request::DrainCollector(_) => "drain_collector",
    };
    let started = std::time::Instant::now();
    let result = execute(&authority, credential, request).await;
    tracing::info!(
        operation,
        role = %credential.role,
        elapsed_micros = started.elapsed().as_micros() as u64,
        succeeded = result.is_ok(),
        "routing_control_operation"
    );
    Ok(Json(result.unwrap_or_else(|error| {
        tracing::warn!(role = %credential.role, error = %error, "routing_control_request_rejected");
        Response::Error(error)
    })))
}

fn constant_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0_u8, |diff, (a, b)| diff | (a ^ b)) == 0
}

async fn execute(authority: &Authority, credential: &ControlCredential, request: Request) -> RoutingResult<Response> {
    let time = current_time()?;
    let store = &authority.store;
    let collector_id = match &request {
        Request::Register(registration) => {
            if credential.role != "collector" || credential.instance_key.as_ref() != Some(&registration.instance_key) {
                return Err(unavailable("registration identity is not authorized"));
            }
            if registration.heartbeat_at_unix_ms.get().abs_diff(time.get()) > 3_000 {
                return Err(unavailable("registration clock is stale"));
            }
            store.register_collector(registration).await?;
            return Ok(Response::Done);
        }
        Request::Heartbeat(id, _, _) | Request::State(id, _, _) => Some(*id),
        Request::MigrationWork(id) | Request::MigrationAck(_, id, _) => Some(*id),
        Request::Pressure(snapshot) => Some(snapshot.instance_id),
        Request::Claim(_) if credential.role != "data_router" => {
            return Err(unavailable("collector cannot choose owners"));
        }
        Request::Migrate(_, _, _) | Request::DrainCollector(_) if credential.role != "operator" => {
            return Err(unavailable("operator role required"));
        }
        _ => None,
    };
    if let Some(id) = collector_id {
        let view = store.load_view(time).await?;
        let record = view
            .collectors
            .get(&id)
            .ok_or_else(|| unavailable("unknown process identity"))?;
        if credential.role != "collector" || credential.instance_key.as_ref() != Some(&record.registration.instance_key)
        {
            return Err(unavailable("process identity is not authorized"));
        }
    }
    match request {
        Request::View => {
            let view = store.load_view(time).await?;
            Ok(Response::View {
                collectors: view.collectors.into_values().collect(),
                leases: view.leases.into_values().collect(),
                loaded_at: view.loaded_at_unix_ms,
            })
        }
        Request::Heartbeat(id, heartbeat, expiry) => {
            if heartbeat.get().abs_diff(time.get()) > 3_000 {
                return Err(unavailable("stale heartbeat"));
            }
            store.heartbeat_collector(id, heartbeat, expiry).await?;
            Ok(Response::Done)
        }
        Request::State(id, state, _) => {
            store.set_collector_state(id, state, time).await?;
            Ok(Response::Done)
        }
        Request::Pressure(snapshot) => {
            snapshot.validate_at(
                time,
                &crate::PressurePolicy {
                    max_snapshot_age_ms: 3_000,
                    ..Default::default()
                },
            )?;
            store.publish_pressure(&snapshot).await?;
            Ok(Response::Done)
        }
        Request::Claim(mut claim) => {
            if claim.now_unix_ms.get().abs_diff(time.get()) > 3_000 {
                return Err(unavailable("stale claim"));
            }
            claim.now_unix_ms = time;
            Ok(Response::Lease(store.claim_lease(&claim).await?))
        }
        Request::MigrationWork(id) => Ok(Response::Migrations(store.migration_work(id).await?)),
        Request::MigrationAck(migration, id, ack) => {
            store.acknowledge_migration(&migration, id, ack).await?;
            Ok(Response::Done)
        }
        Request::Migrate(key, reason, scale_in) => Ok(Response::Migration(
            store.request_migration(&key, &reason, scale_in).await?,
        )),
        Request::DrainCollector(id) => {
            store.set_collector_state(id, CollectorState::Draining, time).await?;
            Ok(Response::Done)
        }
        Request::Register(_) => Err(unavailable("invalid registration operation")),
    }
}

/// Data-only participants never open a routing snapshot or select an owner.
pub struct RemoteRoutingStore {
    endpoint: String,
    token: String,
    client: reqwest::Client,
}

impl RemoteRoutingStore {
    /// # Errors
    ///
    /// Rejects invalid endpoints/secrets and client configuration errors.
    pub fn new(endpoint: &str, token: String) -> RoutingResult<Self> {
        let url = reqwest::Url::parse(endpoint).map_err(unavailable)?;
        if !matches!(url.scheme(), "http" | "https")
            || token.trim().len() < 32
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(unavailable("invalid endpoint or secret"));
        }
        Ok(Self {
            endpoint: format!("{}/v1/routing", endpoint.trim_end_matches('/')),
            token: token.trim().to_owned(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .connect_timeout(Duration::from_secs(1))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(unavailable)?,
        })
    }

    async fn call(&self, request: Request) -> RoutingResult<Response> {
        let mut response = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.token)
            .json(&request)
            .send()
            .await
            .map_err(|_| unavailable("request failed"))?;
        if !response.status().is_success() || response.content_length().is_some_and(|n| n > 16 * 1024 * 1024) {
            return Err(unavailable("request rejected or response too large"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| unavailable("response read failed"))?
        {
            if bytes.len().saturating_add(chunk.len()) > 16 * 1024 * 1024 {
                return Err(unavailable("response too large"));
            }
            bytes.extend_from_slice(&chunk);
        }
        let response: Response = serde_json::from_slice(&bytes).map_err(|_| unavailable("invalid response"))?;
        match response {
            Response::Error(error) => Err(error),
            response => Ok(response),
        }
    }

    async fn done(&self, request: Request) -> RoutingResult<()> {
        match self.call(request).await? {
            Response::Done => Ok(()),
            _ => Err(unavailable("unexpected response")),
        }
    }
}

#[async_trait]
impl RoutingStore for RemoteRoutingStore {
    fn is_remote_authority(&self) -> bool {
        true
    }

    async fn migration_work(&self, id: CollectorInstanceId) -> RoutingResult<Vec<TableMigration>> {
        match self.call(Request::MigrationWork(id)).await? {
            Response::Migrations(migrations) => Ok(migrations),
            _ => Err(unavailable("unexpected migration work response")),
        }
    }
    async fn acknowledge_migration(
        &self,
        migration: &TableMigration,
        id: CollectorInstanceId,
        ack: MigrationAck,
    ) -> RoutingResult<()> {
        self.done(Request::MigrationAck(migration.clone(), id, ack)).await
    }
    async fn request_migration(&self, key: &LeaseKey, reason: &str, scale_in: bool) -> RoutingResult<TableMigration> {
        match self
            .call(Request::Migrate(key.clone(), reason.to_owned(), scale_in))
            .await?
        {
            Response::Migration(migration) => Ok(migration),
            _ => Err(unavailable("unexpected migration response")),
        }
    }
    async fn register_collector(&self, registration: &CollectorRegistration) -> RoutingResult<()> {
        self.done(Request::Register(registration.clone())).await
    }
    async fn heartbeat_collector(
        &self,
        id: CollectorInstanceId,
        heartbeat: UnixMillis,
        expiry: UnixMillis,
    ) -> RoutingResult<()> {
        self.done(Request::Heartbeat(id, heartbeat, expiry)).await
    }
    async fn set_collector_state(
        &self,
        id: CollectorInstanceId,
        state: CollectorState,
        time: UnixMillis,
    ) -> RoutingResult<()> {
        self.done(Request::State(id, state, time)).await
    }
    async fn publish_pressure(&self, snapshot: &RichPressureSnapshot) -> RoutingResult<()> {
        self.done(Request::Pressure(snapshot.clone())).await
    }
    async fn load_view(&self, _: UnixMillis) -> RoutingResult<RoutingView> {
        match self.call(Request::View).await? {
            Response::View {
                collectors,
                leases,
                loaded_at,
            } => Ok(RoutingView {
                loaded_at_unix_ms: loaded_at,
                collectors: collectors
                    .into_iter()
                    .map(|c| (c.registration.instance_id, c))
                    .collect(),
                leases: leases.into_iter().map(|l| (l.key.clone(), l)).collect(),
            }),
            _ => Err(unavailable("unexpected view response")),
        }
    }
    async fn claim_lease(&self, request: &LeaseClaim) -> RoutingResult<RoutingLease> {
        match self.call(Request::Claim(request.clone())).await? {
            Response::Lease(lease) => Ok(lease),
            _ => Err(unavailable("unexpected lease response")),
        }
    }
    async fn begin_lease_drain(&self, _: &LeaseCas) -> RoutingResult<RoutingLease> {
        Err(unavailable("only controller may migrate"))
    }
    async fn transfer_drained_lease(&self, _: &LeaseTransfer) -> RoutingResult<RoutingLease> {
        Err(unavailable("only controller may migrate"))
    }
    async fn transfer_to_successor(&self, _: &LeaseSuccession) -> RoutingResult<RoutingLease> {
        Err(unavailable("automatic succession disabled"))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;
    use crate::MemoryRoutingStore;
    use arrowcol_protocol::{ImplementationPool, PositiveMeasurement};

    fn credential(role: &str, key: Option<&str>, token: char) -> ControlCredential {
        ControlCredential {
            role: role.to_owned(),
            instance_key: key.map(str::to_owned),
            token: token.to_string().repeat(64),
        }
    }

    #[test]
    fn control_credentials_cannot_alias_roles_or_leave_collector_identity_unbound() {
        let operator = credential("operator", None, 'o');
        assert!(valid_credentials(std::slice::from_ref(&operator)));
        assert!(!valid_credentials(&[operator.clone(), operator]));
        assert!(!valid_credentials(&[credential("collector", None, 'c')]));
        assert!(!valid_credentials(&[credential("data_router", Some("collector"), 'd')]));
        assert!(!valid_credentials(&[]));
    }

    #[tokio::test]
    async fn private_api_enforces_bearer_roles_and_exact_process_identity() -> anyhow::Result<()> {
        let memory = Arc::new(MemoryRoutingStore::default());
        let operator = credential("operator", None, 'o');
        let collector = credential("collector", Some("bound-collector"), 'c');
        let follower = credential("data_router", None, 'd');
        let authority = Authority {
            store: Arc::<MemoryRoutingStore>::clone(&memory),
            credentials: vec![operator.clone(), collector.clone(), follower.clone()],
        };
        let now = current_time()?;
        let id = CollectorInstanceId::new();
        let mut registration = CollectorRegistration {
            instance_id: id,
            instance_key: "wrong-collector".to_owned(),
            implementation_pool: ImplementationPool::Direct,
            flight_endpoint: "127.0.0.1:50051".parse()?,
            catalog_capable: true,
            max_concurrent_streams: PositiveMeasurement::new(8)?,
            started_at_unix_ms: now,
            heartbeat_at_unix_ms: now,
            lease_expires_at_unix_ms: UnixMillis::new(now.get() + 10_000)?,
        };
        assert!(
            execute(&authority, &collector, Request::Register(registration.clone()))
                .await
                .is_err()
        );
        registration.instance_key = "bound-collector".to_owned();
        execute(&authority, &collector, Request::Register(registration)).await?;
        assert!(
            execute(&authority, &follower, Request::DrainCollector(id))
                .await
                .is_err()
        );
        assert!(
            execute(
                &authority,
                &collector,
                Request::Heartbeat(CollectorInstanceId::new(), now, now)
            )
            .await
            .is_err()
        );
        execute(&authority, &operator, Request::DrainCollector(id)).await?;
        assert_eq!(
            memory.load_view(now).await?.collectors[&id].state,
            CollectorState::Draining
        );

        let app = control_app(memory, vec![operator, collector, follower.clone()])?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/v1/routing", listener.local_addr()?);
        let task = tokio::spawn(async move { axum::serve(listener, app).await });
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(2))
            .build()?;
        for token in [None, Some("incorrect-bearer-token")] {
            let mut request = client.post(&endpoint).json(&Request::View);
            if let Some(token) = token {
                request = request.bearer_auth(token);
            }
            assert_eq!(request.send().await?.status(), StatusCode::UNAUTHORIZED);
        }
        let remote = RemoteRoutingStore::new(endpoint.trim_end_matches("/v1/routing"), follower.token)?;
        assert!(remote.is_remote_authority());
        assert_eq!(
            remote.load_view(now).await?.collectors[&id].state,
            CollectorState::Draining
        );
        task.abort();
        Ok(())
    }
}
