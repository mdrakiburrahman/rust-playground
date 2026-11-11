use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use arrowcol_protocol::{
    BasisPoints, CatalogName, CollectorInstanceId, FencingToken, ImplementationPool, LeaseId, Measurement,
    PositiveMeasurement, PressureGauge, SchemaName, TableIdentity, TableName, UnixMillis,
};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use sha2::{Digest, Sha256};
use tiberius::{IsolationLevel, Query, Row};
use uuid::Uuid;

use crate::{
    CollectorRecord, CollectorRegistration, CollectorState, LeaseCas, LeaseClaim, LeaseKey, LeaseState,
    LeaseSuccession, LeaseTransfer, RichPressureSnapshot, RoutingError, RoutingErrorKind, RoutingLease, RoutingResult,
    RoutingStore, RoutingView, RunTablePin, SubsystemState,
};

use arrowcol_sql_server::{ConnectionConfig, SqlClient, SqlConnection, SqlPool};

const LEASE_COLUMNS: &str = "\
    implementation_pool, catalog_name, schema_name, table_name, lease_id, holder_instance_id, \
    fencing_epoch, lease_version, state, issued_at_utc, renewed_at_utc, expires_at_utc, drain_started_at_utc";

/// SQL Server connection configuration for the routing control plane.
#[derive(Clone)]
pub struct SqlServerRoutingConfig {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: String,
    trust_server_certificate: bool,
    connect_timeout: Duration,
    command_timeout: Duration,
}

impl SqlServerRoutingConfig {
    /// Creates SQL-authenticated configuration.
    #[must_use]
    pub fn new(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            database: database.into(),
            username: username.into(),
            password: password.into(),
            trust_server_certificate: false,
            connect_timeout: Duration::from_secs(15),
            command_timeout: Duration::from_secs(30),
        }
    }

    /// Reads a one-line password from a secret file.
    ///
    /// # Errors
    ///
    /// Returns invalid configuration for unreadable or multiline secret data.
    pub fn with_password_file(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        username: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> RoutingResult<Self> {
        let password = std::fs::read_to_string(path.as_ref()).map_err(|cause| {
            RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                format!("could not read SQL Server password file: {cause}"),
            )
        })?;
        let password = password.trim_end_matches(['\r', '\n']);
        if password.is_empty() || password.contains(['\r', '\n']) {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "SQL Server password file must contain one non-empty line",
            ));
        }
        Ok(Self::new(host, port, database, username, password.to_owned()))
    }

    /// Enables certificate trust bypass for explicitly local infrastructure.
    #[must_use]
    pub const fn trust_server_certificate(mut self, trust: bool) -> Self {
        self.trust_server_certificate = trust;
        self
    }

    /// Sets connection and command timeouts.
    #[must_use]
    pub const fn with_timeouts(mut self, connect: Duration, command: Duration) -> Self {
        self.connect_timeout = connect;
        self.command_timeout = command;
        self
    }

    fn connection_config(&self) -> ConnectionConfig {
        ConnectionConfig::new(
            self.host.clone(),
            self.port,
            self.database.clone(),
            self.username.clone(),
            self.password.clone(),
        )
        .trust_server_certificate(self.trust_server_certificate)
        .with_timeouts(self.connect_timeout, self.command_timeout)
    }
}

/// SQL Server-backed multi-router coordination store.
///
/// Transactions chosen as deadlock victims (SQL error 1205) are rolled back by
/// the server and retried a few times before the error is surfaced.
#[derive(Clone)]
pub struct SqlServerRoutingStore {
    inner: RawSqlStore,
}

impl SqlServerRoutingStore {
    /// Creates a SQL routing store.
    #[must_use]
    pub const fn new(config: SqlServerRoutingConfig) -> Self {
        Self {
            inner: RawSqlStore { config, pool: None },
        }
    }

    /// Uses a shared pool with the same credentials, TLS policy, and timeouts.
    ///
    /// Clones share this pool. Failed or cancelled operations discard their
    /// connections. Cleanup failure after confirmed success is logged without
    /// changing the operation's result.
    ///
    /// # Errors
    ///
    /// Rejects a pool whose connection configuration differs from this store.
    pub fn with_pool(mut self, pool: SqlPool) -> RoutingResult<Self> {
        if !pool.matches_config(&self.inner.config.connection_config()) {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "SQL Server pool configuration does not match store configuration",
            ));
        }
        self.inner.pool = Some(pool);
        Ok(self)
    }
}

const DEADLOCK_ATTEMPTS: u32 = 6;

/// Computes the canonical digest for an immutable run-table pin set.
///
/// The digest is stable across input order and encodes each UTF-8 identity
/// component before joining rows, matching the SQL Server authority function.
///
/// # Errors
///
/// Returns an error if the pin set is empty or contains duplicate table keys.
pub fn run_table_pin_set_digest(expected: &[LeaseKey]) -> RoutingResult<String> {
    if expected.is_empty() {
        return Err(RoutingError::new(
            RoutingErrorKind::InvalidConfiguration,
            "OAT run pin set must not be empty",
        ));
    }
    let mut rows = expected
        .iter()
        .map(|key| {
            [
                encode_hex(pool_name(key.implementation_pool).as_bytes()),
                encode_hex(key.table.catalog().as_str().as_bytes()),
                encode_hex(key.table.schema().as_str().as_bytes()),
                encode_hex(key.table.table().as_str().as_bytes()),
            ]
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    if rows.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(RoutingError::new(
            RoutingErrorKind::InvalidConfiguration,
            "OAT run pin set contains duplicate table keys",
        ));
    }
    let mut preimage = b"arrowcol-oat-pin-set-v1\n".to_vec();
    for row in rows {
        preimage.extend_from_slice(row.join(":").as_bytes());
        preimage.push(b'\n');
    }
    Ok(format!("{:x}", Sha256::digest(preimage)))
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

async fn retry_deadlock<T, F, Fut>(mut operation: F) -> RoutingResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = RoutingResult<T>>,
{
    let mut attempt = 1;
    loop {
        match operation().await {
            Err(error) if attempt < DEADLOCK_ATTEMPTS && error.to_string().contains("deadlock victim") => {
                tokio::time::sleep(Duration::from_millis(25 * u64::from(attempt))).await;
                attempt += 1;
            }
            other => return other,
        }
    }
}

#[async_trait]
impl RoutingStore for SqlServerRoutingStore {
    async fn register_collector(&self, registration: &CollectorRegistration) -> RoutingResult<()> {
        retry_deadlock(|| self.inner.register_collector(registration)).await
    }

    async fn heartbeat_collector(
        &self,
        instance_id: CollectorInstanceId,
        heartbeat_at: UnixMillis,
        lease_expires_at: UnixMillis,
    ) -> RoutingResult<()> {
        retry_deadlock(|| {
            self.inner
                .heartbeat_collector(instance_id, heartbeat_at, lease_expires_at)
        })
        .await
    }

    async fn set_collector_state(
        &self,
        instance_id: CollectorInstanceId,
        state: CollectorState,
        changed_at: UnixMillis,
    ) -> RoutingResult<()> {
        retry_deadlock(|| self.inner.set_collector_state(instance_id, state, changed_at)).await
    }

    async fn publish_pressure(&self, snapshot: &RichPressureSnapshot) -> RoutingResult<()> {
        retry_deadlock(|| self.inner.publish_pressure(snapshot)).await
    }

    async fn load_view(&self, loaded_at: UnixMillis) -> RoutingResult<RoutingView> {
        retry_deadlock(|| self.inner.load_view(loaded_at)).await
    }

    async fn claim_lease(&self, claim: &LeaseClaim) -> RoutingResult<RoutingLease> {
        retry_deadlock(|| self.inner.claim_lease(claim)).await
    }

    async fn initialize_run_table_set(
        &self,
        run_id: &str,
        expected: &[LeaseKey],
        expires_at: UnixMillis,
        now: UnixMillis,
    ) -> RoutingResult<()> {
        retry_deadlock(|| self.inner.initialize_run_table_set(run_id, expected, expires_at, now)).await
    }

    async fn get_run_table_pin(
        &self,
        run_id: &str,
        key: &LeaseKey,
        now: UnixMillis,
    ) -> RoutingResult<Option<RunTablePin>> {
        retry_deadlock(|| self.inner.get_run_table_pin(run_id, key, now)).await
    }

    async fn bind_initial_run_table_pin(&self, pin: &RunTablePin, now: UnixMillis) -> RoutingResult<RunTablePin> {
        retry_deadlock(|| self.inner.bind_initial_run_table_pin(pin, now)).await
    }

    async fn has_active_run_table_pin(&self, key: &LeaseKey, now: UnixMillis) -> RoutingResult<bool> {
        retry_deadlock(|| self.inner.has_active_run_table_pin(key, now)).await
    }

    async fn begin_lease_drain(&self, request: &LeaseCas) -> RoutingResult<RoutingLease> {
        retry_deadlock(|| self.inner.begin_lease_drain(request)).await
    }

    async fn transfer_drained_lease(&self, request: &LeaseTransfer) -> RoutingResult<RoutingLease> {
        retry_deadlock(|| self.inner.transfer_drained_lease(request)).await
    }

    async fn transfer_to_successor(&self, request: &LeaseSuccession) -> RoutingResult<RoutingLease> {
        retry_deadlock(|| self.inner.transfer_to_successor(request)).await
    }
}

#[derive(Clone)]
struct RawSqlStore {
    config: SqlServerRoutingConfig,
    pool: Option<SqlPool>,
}

impl RawSqlStore {
    async fn connect(&self) -> RoutingResult<SqlConnection> {
        match &self.pool {
            Some(pool) => pool.acquire().await,
            None => SqlConnection::connect(&self.config.connection_config()).await,
        }
        .map_err(|error| database_error("acquire SQL Server connection", error))
    }
}

#[async_trait]
impl RoutingStore for RawSqlStore {
    async fn register_collector(&self, registration: &CollectorRegistration) -> RoutingResult<()> {
        registration.validate()?;
        let instance_id = registration.instance_id.as_uuid();
        let max_streams = to_i64(registration.max_concurrent_streams.get(), "max concurrent streams")?;
        let started = datetime(registration.started_at_unix_ms)?;
        let heartbeat = datetime(registration.heartbeat_at_unix_ms)?;
        let expiry = datetime(registration.lease_expires_at_unix_ms)?;
        let mut client = self.connect().await?;
        let result: RoutingResult<_> = async {
            // A registration refresh never resurrects a draining or terminal
            // row, and never moves its heartbeat or lease backwards: other
            // writers for the same instance may already have stamped later.
            let row = client
                .query(
                    "MERGE arrowcol.routing_collectors WITH (HOLDLOCK) AS target \
                 USING (SELECT @P1 AS instance_id) AS source \
                 ON target.instance_id = source.instance_id \
                 WHEN MATCHED AND target.state = 'ready' THEN UPDATE SET \
                    instance_key = @P2, implementation_pool = @P3, flight_endpoint = @P4, \
                    catalog_capable = @P5, max_concurrent_streams = @P6, \
                    started_at_utc = @P7, \
                    heartbeat_at_utc = CASE WHEN target.heartbeat_at_utc > @P8 \
                        THEN target.heartbeat_at_utc ELSE @P8 END, \
                    lease_expires_at_utc = CASE WHEN target.lease_expires_at_utc > @P9 \
                        THEN target.lease_expires_at_utc ELSE @P9 END \
                 WHEN NOT MATCHED THEN INSERT ( \
                    instance_id, instance_key, implementation_pool, flight_endpoint, catalog_capable, \
                    max_concurrent_streams, state, started_at_utc, heartbeat_at_utc, lease_expires_at_utc \
                 ) VALUES (@P1, @P2, @P3, @P4, @P5, @P6, 'ready', @P7, @P8, @P9) \
                 OUTPUT $action AS merge_action;",
                    &[
                        &instance_id,
                        &registration.instance_key,
                        &registration.implementation_pool.to_string(),
                        &registration.flight_endpoint.to_string(),
                        &registration.catalog_capable,
                        &max_streams,
                        &started,
                        &heartbeat,
                        &expiry,
                    ],
                )
                .await
                .map_err(|cause| database_error("register routing collector", cause))?
                .into_row()
                .await
                .map_err(|cause| database_error("read routing registration result", cause))?;
            if row.is_none() {
                return Err(RoutingError::new(
                    RoutingErrorKind::Conflict,
                    "collector registration was fenced: the instance already left the ready state",
                ));
            }
            Ok(())
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "register_collector",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn heartbeat_collector(
        &self,
        instance_id: CollectorInstanceId,
        heartbeat_at: UnixMillis,
        lease_expires_at: UnixMillis,
    ) -> RoutingResult<()> {
        if lease_expires_at <= heartbeat_at {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "collector lease expiry must be later than heartbeat",
            ));
        }
        let id = instance_id.as_uuid();
        let heartbeat = datetime(heartbeat_at)?;
        let expiry = datetime(lease_expires_at)?;
        let mut client = self.connect().await?;
        let result: RoutingResult<_> = async {
            let row = client
                .query(
                    "UPDATE arrowcol.routing_collectors \
                 SET heartbeat_at_utc = CASE WHEN heartbeat_at_utc > @P2 \
                        THEN heartbeat_at_utc ELSE @P2 END, \
                     lease_expires_at_utc = CASE WHEN lease_expires_at_utc > @P3 \
                        THEN lease_expires_at_utc ELSE @P3 END \
                 OUTPUT INSERTED.instance_id \
                 WHERE instance_id = @P1 AND state IN ('ready', 'draining') AND @P3 > @P2;",
                    &[&id, &heartbeat, &expiry],
                )
                .await
                .map_err(|cause| database_error("heartbeat routing collector", cause))?
                .into_row()
                .await
                .map_err(|cause| database_error("read routing heartbeat result", cause))?;
            if row.is_none() {
                return Err(RoutingError::new(
                    RoutingErrorKind::Conflict,
                    "collector heartbeat was fenced: the instance is missing or terminal",
                ));
            }
            Ok(())
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "heartbeat_collector",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn set_collector_state(
        &self,
        instance_id: CollectorInstanceId,
        state: CollectorState,
        changed_at: UnixMillis,
    ) -> RoutingResult<()> {
        let id = instance_id.as_uuid();
        let changed = datetime(changed_at)?;
        let state = collector_state_text(state);
        let mut client = self.connect().await?;
        let result: RoutingResult<_> = async {
            let row = client
                .query(
                    // Mirrors `CollectorState::may_transition_to`: the
                    // instance's own concurrent writers cannot lose on clock
                    // order, and nothing regresses or leaves a terminal state.
                    "UPDATE arrowcol.routing_collectors \
                 SET state = @P2, heartbeat_at_utc = CASE WHEN heartbeat_at_utc > @P3 \
                        THEN heartbeat_at_utc ELSE @P3 END \
                 OUTPUT INSERTED.instance_id \
                 WHERE instance_id = @P1 \
                   AND (state = @P2 OR state = 'ready' \
                        OR (state = 'draining' AND @P2 IN ('stopped', 'failed')));",
                    &[&id, &state, &changed],
                )
                .await
                .map_err(|cause| database_error("set routing collector state", cause))?
                .into_row()
                .await
                .map_err(|cause| database_error("read routing collector state result", cause))?;
            if row.is_none() {
                return Err(RoutingError::new(
                    RoutingErrorKind::Conflict,
                    "collector state change was fenced: the instance is missing or the transition would regress",
                ));
            }
            Ok(())
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "set_collector_state",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn publish_pressure(&self, snapshot: &RichPressureSnapshot) -> RoutingResult<()> {
        snapshot.validate_at(snapshot.observed_at_unix_ms, &crate::PressurePolicy::default())?;
        let instance_id = snapshot.instance_id.as_uuid();
        let observed = datetime(snapshot.observed_at_unix_ms)?;
        let table_json = serde_json::to_string(&snapshot.tables).map_err(|cause| {
            RoutingError::new(
                RoutingErrorKind::InvalidSnapshot,
                format!("could not serialize table pressure: {cause}"),
            )
        })?;
        if table_json.len() > 1_048_576 {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidSnapshot,
                "serialized table pressure exceeds 1 MiB",
            ));
        }
        let values = SnapshotSqlValues::try_from(snapshot)?;
        let mut client = self.connect().await?;
        let result: RoutingResult<_> = async {
        client
            .begin_transaction_with_isolation(IsolationLevel::ReadCommitted)
            .await
            .map_err(|cause| database_error("begin pressure publication", cause))?;
        let result = async {
            client
                .execute(
                    "IF NOT EXISTS (SELECT 1 FROM arrowcol.routing_collectors WHERE instance_id = @P1) \
                         THROW 51101, 'routing collector registration is missing', 1; \
                     IF EXISTS (SELECT 1 FROM arrowcol.routing_pressure_latest WITH (UPDLOCK, HOLDLOCK) \
                                WHERE collector_instance_id = @P1 AND observed_at_utc >= @P2) \
                         THROW 51102, 'routing pressure timestamp did not advance', 1; \
                     UPDATE arrowcol.routing_pressure_latest SET \
                        observed_at_utc=@P2, cpu_utilization_bps=@P3, rss_bytes=@P4, rss_limit_bytes=@P5, \
                        active_streams=@P6, active_stream_limit=@P7, received_bytes=@P8, received_rows=@P9, \
                        received_batches=@P10, buffered_bytes=@P11, buffered_bytes_limit=@P12, \
                        buffered_rows=@P13, buffered_rows_limit=@P14, buffered_batches=@P15, \
                        buffered_batches_limit=@P16, available_permits=@P17, permit_limit=@P18, \
                        pending_commits=@P19, pending_commit_limit=@P20, oldest_buffer_age_ms=@P21, \
                        delta_pressure_bps=@P22, quiver_pressure_bps=@P23, dfe_pressure_bps=@P24, \
                        delta_state=@P25, quiver_state=@P26, dfe_state=@P27, accepting_deliveries=@P28, \
                        table_pressure_json=@P29 \
                     WHERE collector_instance_id=@P1; \
                     IF @@ROWCOUNT = 0 INSERT INTO arrowcol.routing_pressure_latest ( \
                        collector_instance_id, observed_at_utc, cpu_utilization_bps, rss_bytes, rss_limit_bytes, \
                        active_streams, active_stream_limit, received_bytes, received_rows, received_batches, \
                        buffered_bytes, buffered_bytes_limit, buffered_rows, buffered_rows_limit, buffered_batches, \
                        buffered_batches_limit, available_permits, permit_limit, pending_commits, pending_commit_limit, \
                        oldest_buffer_age_ms, delta_pressure_bps, quiver_pressure_bps, dfe_pressure_bps, \
                        delta_state, quiver_state, dfe_state, accepting_deliveries, table_pressure_json \
                     ) VALUES ( \
                        @P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8,@P9,@P10,@P11,@P12,@P13,@P14,@P15,@P16, \
                        @P17,@P18,@P19,@P20,@P21,@P22,@P23,@P24,@P25,@P26,@P27,@P28,@P29); \
                     INSERT INTO arrowcol.routing_pressure_history ( \
                        collector_instance_id, observed_at_utc, cpu_utilization_bps, rss_bytes, rss_limit_bytes, \
                        active_streams, active_stream_limit, received_bytes, received_rows, received_batches, \
                        buffered_bytes, buffered_bytes_limit, buffered_rows, buffered_rows_limit, buffered_batches, \
                        buffered_batches_limit, available_permits, permit_limit, pending_commits, pending_commit_limit, \
                        oldest_buffer_age_ms, delta_pressure_bps, quiver_pressure_bps, dfe_pressure_bps, \
                        delta_state, quiver_state, dfe_state, accepting_deliveries, table_pressure_json \
                     ) VALUES ( \
                        @P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8,@P9,@P10,@P11,@P12,@P13,@P14,@P15,@P16, \
                        @P17,@P18,@P19,@P20,@P21,@P22,@P23,@P24,@P25,@P26,@P27,@P28,@P29);",
                    &[
                        &instance_id,
                        &observed,
                        &values.cpu,
                        &values.rss,
                        &values.rss_limit,
                        &values.active,
                        &values.active_limit,
                        &values.received_bytes,
                        &values.received_rows,
                        &values.received_batches,
                        &values.buffered_bytes,
                        &values.buffered_bytes_limit,
                        &values.buffered_rows,
                        &values.buffered_rows_limit,
                        &values.buffered_batches,
                        &values.buffered_batches_limit,
                        &values.available_permits,
                        &values.permit_limit,
                        &values.pending_commits,
                        &values.pending_commit_limit,
                        &values.oldest,
                        &values.delta_pressure,
                        &values.quiver_pressure,
                        &values.dfe_pressure,
                        &values.delta_state,
                        &values.quiver_state,
                        &values.dfe_state,
                        &snapshot.accepting_deliveries,
                        &table_json,
                    ],
                )
                .await
                .map_err(|cause| database_error("publish routing pressure", cause))?;
            Ok(())
        }
        .await;
        finish_transaction(&mut client, result, "publish routing pressure").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "publish_pressure",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn load_view(&self, loaded_at: UnixMillis) -> RoutingResult<RoutingView> {
        let mut client = self.connect().await?;
        let result: RoutingResult<_> = async {
            let rows = client
                .query(
                    "SELECT c.instance_id, c.instance_key, c.implementation_pool, c.flight_endpoint, \
                        c.catalog_capable, c.max_concurrent_streams, c.state, c.started_at_utc, \
                        c.heartbeat_at_utc, c.lease_expires_at_utc, \
                        p.observed_at_utc, p.cpu_utilization_bps, p.rss_bytes, p.rss_limit_bytes, \
                        p.active_streams, p.active_stream_limit, p.received_bytes, p.received_rows, \
                        p.received_batches, p.buffered_bytes, p.buffered_bytes_limit, p.buffered_rows, \
                        p.buffered_rows_limit, p.buffered_batches, p.buffered_batches_limit, \
                        p.available_permits, p.permit_limit, p.pending_commits, p.pending_commit_limit, \
                        p.oldest_buffer_age_ms, p.delta_pressure_bps, p.quiver_pressure_bps, \
                        p.dfe_pressure_bps, p.delta_state, p.quiver_state, p.dfe_state, \
                        p.accepting_deliveries, p.table_pressure_json \
                 FROM arrowcol.routing_collectors AS c \
                 LEFT JOIN arrowcol.routing_pressure_latest AS p \
                   ON p.collector_instance_id = c.instance_id;",
                    &[],
                )
                .await
                .map_err(|cause| database_error("load routing collectors", cause))?
                .into_first_result()
                .await
                .map_err(|cause| database_error("read routing collectors", cause))?;
            let mut collectors = std::collections::BTreeMap::new();
            for row in rows {
                let record = collector_from_row(&row)?;
                collectors.insert(record.registration.instance_id, record);
            }
            let rows = client
                .query(
                    &format!("SELECT {LEASE_COLUMNS} FROM arrowcol.routing_table_leases;"),
                    &[],
                )
                .await
                .map_err(|cause| database_error("load routing leases", cause))?
                .into_first_result()
                .await
                .map_err(|cause| database_error("read routing leases", cause))?;
            let mut leases = std::collections::BTreeMap::new();
            for row in rows {
                let lease = lease_from_row(&row)?;
                leases.insert(lease.key.clone(), lease);
            }
            Ok(RoutingView {
                loaded_at_unix_ms: loaded_at,
                collectors,
                leases,
            })
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "load_view",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn claim_lease(&self, claim: &LeaseClaim) -> RoutingResult<RoutingLease> {
        validate_lease_window(claim.now_unix_ms, claim.expires_at_unix_ms)?;
        let mut client = self.connect().await?;
        let result: RoutingResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin lease claim", cause))?;
            let result = async {
                if claim.distinct_table_owners {
                    lock_distinct_table_owner_pool(&mut client, claim.key.implementation_pool).await?;
                }
                if let Some(existing) = select_locked_lease(&mut client, &claim.key).await? {
                    if existing.is_valid_at(claim.now_unix_ms) {
                        if claim.distinct_table_owners {
                            verify_distinct_table_owner(
                                &mut client,
                                &claim.key,
                                existing.holder_instance_id,
                                claim.now_unix_ms,
                            )
                            .await?;
                        }
                        return Ok(existing);
                    }
                    if active_run_pin_exists(&mut client, &claim.key, claim.now_unix_ms).await? {
                        return Err(RoutingError::new(
                            RoutingErrorKind::Conflict,
                            "expired table lease is protected by an active immutable OAT run pin",
                        ));
                    }
                    verify_candidate(
                        &mut client,
                        claim.candidate_instance_id,
                        claim.key.implementation_pool,
                        claim.minimum_snapshot_at_unix_ms,
                        claim.now_unix_ms,
                    )
                    .await?;
                    if claim.distinct_table_owners {
                        verify_distinct_table_owner(
                            &mut client,
                            &claim.key,
                            claim.candidate_instance_id,
                            claim.now_unix_ms,
                        )
                        .await?;
                    }
                    let epoch = existing.fencing_epoch.next().map_err(|cause| {
                        RoutingError::new(
                            RoutingErrorKind::CorruptState,
                            format!("fencing epoch exhausted: {cause}"),
                        )
                    })?;
                    let version = existing.lease_version.checked_add(1).ok_or_else(|| {
                        RoutingError::new(RoutingErrorKind::CorruptState, "routing lease version exhausted")
                    })?;
                    update_lease_owner(
                        &mut client,
                        &claim.key,
                        existing.lease_version,
                        claim.candidate_instance_id,
                        epoch,
                        version,
                        claim.now_unix_ms,
                        claim.expires_at_unix_ms,
                    )
                    .await
                } else {
                    if active_run_pin_exists(&mut client, &claim.key, claim.now_unix_ms).await? {
                        return Err(RoutingError::new(
                            RoutingErrorKind::Conflict,
                            "missing table lease is protected by an active immutable OAT run pin",
                        ));
                    }
                    verify_candidate(
                        &mut client,
                        claim.candidate_instance_id,
                        claim.key.implementation_pool,
                        claim.minimum_snapshot_at_unix_ms,
                        claim.now_unix_ms,
                    )
                    .await?;
                    if claim.distinct_table_owners {
                        verify_distinct_table_owner(
                            &mut client,
                            &claim.key,
                            claim.candidate_instance_id,
                            claim.now_unix_ms,
                        )
                        .await?;
                    }
                    insert_lease(&mut client, claim).await
                }
            }
            .await;
            finish_transaction(&mut client, result, "claim routing lease").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "claim_lease",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn initialize_run_table_set(
        &self,
        run_id: &str,
        expected: &[LeaseKey],
        expires_at: UnixMillis,
        now: UnixMillis,
    ) -> RoutingResult<()> {
        let run_id = parse_run_id(run_id)?;
        if expected.is_empty()
            || expected.len() > 32
            || expected.windows(2).any(|pair| pair[0] >= pair[1])
            || expires_at <= now
        {
            return Err(RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "OAT run pin set must be live and contain 1..=32 strictly sorted, unique table keys",
            ));
        }
        let digest = run_table_pin_set_digest(expected)?;
        let expected_count = i32::try_from(expected.len()).map_err(|_| {
            RoutingError::new(
                RoutingErrorKind::InvalidConfiguration,
                "OAT run table count exceeds SQL Server limits",
            )
        })?;
        let now_sql = datetime(now)?;
        let expires_sql = datetime(expires_at)?;
        let mut client = self.connect().await?;
        let result: RoutingResult<()> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin OAT run pin-set initialization", cause))?;
            let result = async {
                let rows = client
                    .query(
                        "SELECT CONVERT(bigint, expected_table_count) AS expected_table_count, \
                                expected_table_set_sha256, expires_at_utc \
                         FROM arrowcol.routing_run_pin_sets WITH (UPDLOCK, HOLDLOCK) WHERE run_id=@P1;",
                        &[&run_id],
                    )
                    .await
                    .map_err(|cause| database_error("lock OAT run pin set", cause))?
                    .into_first_result()
                    .await
                    .map_err(|cause| database_error("read OAT run pin set", cause))?;
                if let Some(row) = rows.first() {
                    if required_i64(row, "expected_table_count")? != i64::from(expected_count)
                        || required_str(row, "expected_table_set_sha256")? != digest
                        || unix_millis(required_datetime(row, "expires_at_utc")?)? != expires_at
                    {
                        return Err(RoutingError::new(
                            RoutingErrorKind::Conflict,
                            "immutable OAT run pin set already exists with a different count, digest, or expiry",
                        ));
                    }
                    verify_run_pin_set(&mut client, run_id, expected_count, &digest, expires_at, now).await?;
                    return Ok(());
                }

                client
                    .execute(
                        "INSERT INTO arrowcol.routing_run_pin_sets \
                         (run_id, expected_table_count, expected_table_set_sha256, expires_at_utc, created_at_utc) \
                         VALUES (@P1,@P2,@P3,@P4,@P5);",
                        &[&run_id, &expected_count, &digest, &expires_sql, &now_sql],
                    )
                    .await
                    .map_err(|cause| database_error("insert OAT run pin set", cause))?;

                let mut values = Vec::with_capacity(expected.len());
                for index in 0..expected.len() {
                    let first = index * 5 + 1;
                    values.push(format!(
                        "(@P{first},@P{},@P{},@P{},@P{})",
                        first + 1,
                        first + 2,
                        first + 3,
                        first + 4
                    ));
                }
                let sql = format!(
                    "INSERT INTO arrowcol.routing_run_table_initializations \
                     (run_id, implementation_pool, catalog_name, schema_name, table_name) VALUES {};",
                    values.join(",")
                );
                let mut query = Query::new(sql);
                for key in expected {
                    query.bind(run_id);
                    query.bind(pool_name(key.implementation_pool).to_owned());
                    query.bind(key.table.catalog().as_str().to_owned());
                    query.bind(key.table.schema().as_str().to_owned());
                    query.bind(key.table.table().as_str().to_owned());
                }
                query
                    .execute(&mut client)
                    .await
                    .map_err(|cause| database_error("insert complete OAT run table set", cause))?;
                verify_run_pin_set(&mut client, run_id, expected_count, &digest, expires_at, now).await
            }
            .await;
            finish_transaction(&mut client, result, "initialize OAT run pin set").await
        }
        .await;
        result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(operation = "initialize_run_table_set", error = %error, "SQL Server session cleanup failed");
        }
        Ok(())
    }

    async fn get_run_table_pin(
        &self,
        run_id: &str,
        key: &LeaseKey,
        now: UnixMillis,
    ) -> RoutingResult<Option<RunTablePin>> {
        let run_id = parse_run_id(run_id)?;
        let mut client = self.connect().await?;
        client
            .begin_transaction_with_isolation(IsolationLevel::Serializable)
            .await
            .map_err(|cause| database_error("begin OAT run-table pin validation", cause))?;
        let result: RoutingResult<_> = async {
            validate_live_run_pin_set(&mut client, run_id, now).await?;
            let markers = client
                .query(
                    "SELECT state FROM arrowcol.routing_run_table_initializations \
                     WHERE run_id=@P1 AND implementation_pool=@P2 AND catalog_name=@P3 \
                       AND schema_name=@P4 AND table_name=@P5;",
                    &[
                        &run_id,
                        &pool_name(key.implementation_pool),
                        &key.table.catalog().as_str(),
                        &key.table.schema().as_str(),
                        &key.table.table().as_str(),
                    ],
                )
                .await
                .map_err(|cause| database_error("load OAT run table initializer", cause))?
                .into_first_result()
                .await
                .map_err(|cause| database_error("read OAT run table initializer", cause))?;
            let marker = markers.first().ok_or_else(|| {
                RoutingError::new(RoutingErrorKind::Conflict, "table is not in the immutable OAT run set")
            })?;
            let marker_state = required_str(marker, "state")?;
            let rows = client
                .query(
                    "SELECT run_id, implementation_pool, catalog_name, schema_name, table_name, \
                            holder_instance_id, lease_id, fencing_epoch, expires_at_utc \
                     FROM arrowcol.routing_run_table_pins \
                     WHERE run_id=@P1 AND implementation_pool=@P2 AND catalog_name=@P3 \
                       AND schema_name=@P4 AND table_name=@P5;",
                    &[
                        &run_id,
                        &pool_name(key.implementation_pool),
                        &key.table.catalog().as_str(),
                        &key.table.schema().as_str(),
                        &key.table.table().as_str(),
                    ],
                )
                .await
                .map_err(|cause| database_error("load OAT run-table pin", cause))?
                .into_first_result()
                .await
                .map_err(|cause| database_error("read OAT run-table pin", cause))?;
            let Some(row) = rows.first() else {
                return match marker_state {
                    "pending" => Ok(None),
                    "bound" => Err(RoutingError::new(
                        RoutingErrorKind::Conflict,
                        "bound OAT run-table marker is missing its immutable owner row",
                    )),
                    _ => Err(RoutingError::new(
                        RoutingErrorKind::CorruptState,
                        "OAT run marker has an invalid state",
                    )),
                };
            };
            if marker_state != "bound" {
                return Err(RoutingError::new(
                    RoutingErrorKind::CorruptState,
                    "OAT run-table owner exists before its marker is bound",
                ));
            }
            let pin = run_table_pin_from_row(row)?;
            validate_sql_run_pin(&mut client, &pin, now).await?;
            Ok(Some(pin))
        }
        .await;
        let value = finish_transaction(&mut client, result, "validate OAT run-table pin").await?;
        if let Err(error) = client.finish().await {
            tracing::warn!(operation = "get_run_table_pin", error = %error, "SQL Server session cleanup failed");
        }
        Ok(value)
    }

    async fn bind_initial_run_table_pin(&self, pin: &RunTablePin, now: UnixMillis) -> RoutingResult<RunTablePin> {
        pin.validate(now)?;
        let run_id = parse_run_id(&pin.run_id)?;
        let mut client = self.connect().await?;
        let result: RoutingResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin OAT run-table pin", cause))?;
            let result = async {
                validate_live_run_pin_set(&mut client, run_id, now).await?;
                let marker_rows = client
                    .query(
                        "SELECT i.state, s.expires_at_utc \
                         FROM arrowcol.routing_run_table_initializations i \
                         JOIN arrowcol.routing_run_pin_sets s ON s.run_id=i.run_id \
                         WHERE i.run_id=@P1 AND i.implementation_pool=@P2 AND i.catalog_name=@P3 \
                           AND i.schema_name=@P4 AND i.table_name=@P5;",
                        &[
                            &run_id,
                            &pool_name(pin.key.implementation_pool),
                            &pin.key.table.catalog().as_str(),
                            &pin.key.table.schema().as_str(),
                            &pin.key.table.table().as_str(),
                        ],
                    )
                    .await
                    .map_err(|cause| database_error("lock OAT run-table initialization marker", cause))?
                    .into_first_result()
                    .await
                    .map_err(|cause| database_error("read OAT run-table initialization marker", cause))?;
                let marker = marker_rows.first().ok_or_else(|| {
                    RoutingError::new(RoutingErrorKind::Conflict, "table is not in the immutable OAT run set")
                })?;
                let marker_state = required_str(marker, "state")?;
                if unix_millis(required_datetime(marker, "expires_at_utc")?)? != pin.expires_at_unix_ms
                    || marker_state == "pending" && pin.expires_at_unix_ms <= now
                {
                    return Err(RoutingError::new(
                        RoutingErrorKind::Conflict,
                        "OAT table marker expiry differs from the requested immutable pin",
                    ));
                }
                let rows = client
                    .query(
                        "SELECT run_id, implementation_pool, catalog_name, schema_name, table_name, \
                                holder_instance_id, lease_id, fencing_epoch, expires_at_utc \
                         FROM arrowcol.routing_run_table_pins WITH (UPDLOCK, HOLDLOCK) \
                         WHERE run_id=@P1 AND implementation_pool=@P2 AND catalog_name=@P3 \
                           AND schema_name=@P4 AND table_name=@P5;",
                        &[
                            &run_id,
                            &pool_name(pin.key.implementation_pool),
                            &pin.key.table.catalog().as_str(),
                            &pin.key.table.schema().as_str(),
                            &pin.key.table.table().as_str(),
                        ],
                    )
                    .await
                    .map_err(|cause| database_error("lock OAT run-table pin", cause))?
                    .into_first_result()
                    .await
                    .map_err(|cause| database_error("read OAT run-table pin", cause))?;
                if let Some(row) = rows.first() {
                    let existing = run_table_pin_from_row(row)?;
                    if marker_state != "bound"
                        || existing.expires_at_unix_ms <= now
                        || !same_run_table_pin(&existing, pin)
                    {
                        return Err(RoutingError::new(
                            RoutingErrorKind::Conflict,
                            "immutable OAT run-table pin already exists with a different or expired owner",
                        ));
                    }
                    validate_sql_run_pin(&mut client, &existing, now).await?;
                    return Ok(existing);
                }
                if marker_state != "pending" {
                    return Err(RoutingError::new(
                        RoutingErrorKind::Conflict,
                        "bound OAT run-table marker is missing its immutable owner row",
                    ));
                }
                validate_sql_run_pin(&mut client, pin, now).await?;
                client
                    .execute(
                        "INSERT INTO arrowcol.routing_run_table_pins \
                         (run_id, implementation_pool, catalog_name, schema_name, table_name, \
                          holder_instance_id, lease_id, fencing_epoch, expires_at_utc) \
                         VALUES (@P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8,@P9);",
                        &[
                            &run_id,
                            &pool_name(pin.key.implementation_pool),
                            &pin.key.table.catalog().as_str(),
                            &pin.key.table.schema().as_str(),
                            &pin.key.table.table().as_str(),
                            &pin.holder_instance_id.as_uuid(),
                            &pin.lease_id.as_uuid(),
                            &to_i64(pin.fencing_epoch.get(), "fencing epoch")?,
                            &datetime(pin.expires_at_unix_ms)?,
                        ],
                    )
                    .await
                    .map_err(|cause| database_error("insert OAT run-table pin", cause))?;
                Ok(pin.clone())
            }
            .await;
            finish_transaction(&mut client, result, "create OAT run-table pin").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(operation = "bind_initial_run_table_pin", error = %error, "SQL Server session cleanup failed");
        }
        Ok(value)
    }

    async fn has_active_run_table_pin(&self, key: &LeaseKey, now: UnixMillis) -> RoutingResult<bool> {
        let mut client = self.connect().await?;
        let result = active_run_pin_exists(&mut client, key, now).await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(operation = "has_active_run_table_pin", error = %error, "SQL Server session cleanup failed");
        }
        Ok(value)
    }

    async fn begin_lease_drain(&self, request: &LeaseCas) -> RoutingResult<RoutingLease> {
        let version = request
            .expected_version
            .checked_add(1)
            .ok_or_else(|| RoutingError::new(RoutingErrorKind::CorruptState, "routing lease version exhausted"))?;
        let now = datetime(request.now_unix_ms)?;
        let mut client = self.connect().await?;
        let result: RoutingResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin routing lease drain", cause))?;
            let result = async {
                if active_run_pin_exists(&mut client, &request.key, request.now_unix_ms).await? {
                    return Err(RoutingError::new(
                        RoutingErrorKind::Conflict,
                        "table lease is protected by an active immutable OAT run pin",
                    ));
                }
                let rows = client
                    .query(
                        &format!(
                            "UPDATE arrowcol.routing_table_leases SET \
                            state='draining', lease_version=@P5, renewed_at_utc=@P6, drain_started_at_utc=@P6 \
                         OUTPUT INSERTED.{columns} \
                         WHERE implementation_pool=@P1 AND catalog_name=@P2 AND schema_name=@P3 AND table_name=@P4 \
                           AND lease_version=@P7 AND state='active';",
                            columns = LEASE_COLUMNS.replace(", ", ", INSERTED.")
                        ),
                        &[
                            &request.key.implementation_pool.to_string(),
                            &request.key.table.catalog().as_str(),
                            &request.key.table.schema().as_str(),
                            &request.key.table.table().as_str(),
                            &to_i64(version, "lease version")?,
                            &now,
                            &to_i64(request.expected_version, "expected lease version")?,
                        ],
                    )
                    .await
                    .map_err(|cause| database_error("begin routing lease drain", cause))?
                    .into_first_result()
                    .await
                    .map_err(|cause| database_error("read routing lease drain result", cause))?;
                let row = rows.into_iter().next().ok_or_else(|| {
                    RoutingError::new(
                        RoutingErrorKind::Conflict,
                        "routing lease changed before drain compare-and-set",
                    )
                })?;
                lease_from_row(&row)
            }
            .await;
            finish_transaction(&mut client, result, "begin routing lease drain").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "begin_lease_drain",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn transfer_drained_lease(&self, request: &LeaseTransfer) -> RoutingResult<RoutingLease> {
        validate_lease_window(request.now_unix_ms, request.expires_at_unix_ms)?;
        let mut client = self.connect().await?;
        let result: RoutingResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin routing lease transfer", cause))?;
            let result = async {
                if request.distinct_table_owners {
                    lock_distinct_table_owner_pool(&mut client, request.key.implementation_pool).await?;
                }
                let existing = select_locked_lease(&mut client, &request.key)
                    .await?
                    .ok_or_else(|| RoutingError::new(RoutingErrorKind::Conflict, "routing lease was not found"))?;
                if active_run_pin_exists(&mut client, &request.key, request.now_unix_ms).await? {
                    return Err(RoutingError::new(
                        RoutingErrorKind::Conflict,
                        "table lease is protected by an active immutable OAT run pin",
                    ));
                }
                if existing.state != LeaseState::Draining || existing.lease_version != request.expected_version {
                    return Err(RoutingError::new(
                        RoutingErrorKind::Conflict,
                        "routing lease changed before transfer compare-and-set",
                    ));
                }
                verify_candidate(
                    &mut client,
                    request.candidate_instance_id,
                    request.key.implementation_pool,
                    request.minimum_snapshot_at_unix_ms,
                    request.now_unix_ms,
                )
                .await?;
                if request.distinct_table_owners {
                    verify_distinct_table_owner(
                        &mut client,
                        &request.key,
                        request.candidate_instance_id,
                        request.now_unix_ms,
                    )
                    .await?;
                }
                let epoch = existing.fencing_epoch.next().map_err(|cause| {
                    RoutingError::new(
                        RoutingErrorKind::CorruptState,
                        format!("fencing epoch exhausted: {cause}"),
                    )
                })?;
                let version = existing.lease_version.checked_add(1).ok_or_else(|| {
                    RoutingError::new(RoutingErrorKind::CorruptState, "routing lease version exhausted")
                })?;
                update_lease_owner(
                    &mut client,
                    &request.key,
                    existing.lease_version,
                    request.candidate_instance_id,
                    epoch,
                    version,
                    request.now_unix_ms,
                    request.expires_at_unix_ms,
                )
                .await
            }
            .await;
            finish_transaction(&mut client, result, "transfer routing lease").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "transfer_drained_lease",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn transfer_to_successor(&self, request: &LeaseSuccession) -> RoutingResult<RoutingLease> {
        request.validate()?;
        let mut client = self.connect().await?;
        let result: RoutingResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin routing lease succession", cause))?;
            let result = async {
                if request.distinct_table_owners {
                    lock_distinct_table_owner_pool(&mut client, request.key.implementation_pool).await?;
                }
                let existing = select_locked_lease(&mut client, &request.key)
                    .await?
                    .ok_or_else(|| RoutingError::new(RoutingErrorKind::Conflict, "routing lease was not found"))?;
                if active_run_pin_exists(&mut client, &request.key, request.now_unix_ms).await? {
                    return Err(RoutingError::new(
                        RoutingErrorKind::Conflict,
                        "table lease is protected by an active immutable OAT run pin",
                    ));
                }
                if existing.lease_version != request.expected_version
                    || existing.holder_instance_id != request.predecessor_instance_id
                    || !existing.is_valid_at(request.now_unix_ms)
                {
                    return Err(RoutingError::new(
                        RoutingErrorKind::Conflict,
                        "routing lease changed or expired before succession compare-and-set",
                    ));
                }
                verify_successor(&mut client, request).await?;
                verify_candidate(
                    &mut client,
                    request.successor_instance_id,
                    request.key.implementation_pool,
                    request.minimum_snapshot_at_unix_ms,
                    request.now_unix_ms,
                )
                .await
                .map_err(|error| {
                    if error.kind() == RoutingErrorKind::PoolUnavailable {
                        RoutingError::new(
                            RoutingErrorKind::Conflict,
                            "routing lease successor is no longer ready with fresh accepting pressure",
                        )
                    } else {
                        error
                    }
                })?;
                if request.distinct_table_owners {
                    verify_distinct_table_owner(
                        &mut client,
                        &request.key,
                        request.successor_instance_id,
                        request.now_unix_ms,
                    )
                    .await?;
                }
                let epoch = existing.fencing_epoch.next().map_err(|cause| {
                    RoutingError::new(
                        RoutingErrorKind::CorruptState,
                        format!("fencing epoch exhausted: {cause}"),
                    )
                })?;
                let version = existing.lease_version.checked_add(1).ok_or_else(|| {
                    RoutingError::new(RoutingErrorKind::CorruptState, "routing lease version exhausted")
                })?;
                update_lease_owner(
                    &mut client,
                    &request.key,
                    existing.lease_version,
                    request.successor_instance_id,
                    epoch,
                    version,
                    request.now_unix_ms,
                    request.expires_at_unix_ms,
                )
                .await
            }
            .await;
            finish_transaction(&mut client, result, "transfer routing lease to successor").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "transfer_to_successor",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }
}

struct SnapshotSqlValues {
    cpu: i16,
    rss: i64,
    rss_limit: i64,
    active: i64,
    active_limit: i64,
    received_bytes: i64,
    received_rows: i64,
    received_batches: i64,
    buffered_bytes: i64,
    buffered_bytes_limit: i64,
    buffered_rows: i64,
    buffered_rows_limit: i64,
    buffered_batches: i64,
    buffered_batches_limit: i64,
    available_permits: i64,
    permit_limit: i64,
    pending_commits: i64,
    pending_commit_limit: i64,
    oldest: i64,
    delta_pressure: i16,
    quiver_pressure: i16,
    dfe_pressure: i16,
    delta_state: String,
    quiver_state: String,
    dfe_state: String,
}

impl TryFrom<&RichPressureSnapshot> for SnapshotSqlValues {
    type Error = RoutingError;

    fn try_from(value: &RichPressureSnapshot) -> Result<Self, Self::Error> {
        Ok(Self {
            cpu: to_i16(value.cpu_utilization_bps.get(), "CPU basis points")?,
            rss: to_i64(value.rss_bytes.get(), "RSS bytes")?,
            rss_limit: to_i64(value.rss_limit_bytes.get(), "RSS limit")?,
            active: to_i64(value.active_streams.current().get(), "active streams")?,
            active_limit: to_i64(value.active_streams.limit().get(), "active stream limit")?,
            received_bytes: to_i64(value.received_bytes.get(), "received bytes")?,
            received_rows: to_i64(value.received_rows.get(), "received rows")?,
            received_batches: to_i64(value.received_batches.get(), "received batches")?,
            buffered_bytes: to_i64(value.buffered_bytes.current().get(), "buffered bytes")?,
            buffered_bytes_limit: to_i64(value.buffered_bytes.limit().get(), "buffered byte limit")?,
            buffered_rows: to_i64(value.buffered_rows.current().get(), "buffered rows")?,
            buffered_rows_limit: to_i64(value.buffered_rows.limit().get(), "buffered row limit")?,
            buffered_batches: to_i64(value.buffered_batches.current().get(), "buffered batches")?,
            buffered_batches_limit: to_i64(value.buffered_batches.limit().get(), "buffered batch limit")?,
            available_permits: to_i64(value.available_permits.current().get(), "available permits")?,
            permit_limit: to_i64(value.available_permits.limit().get(), "permit limit")?,
            pending_commits: to_i64(value.pending_commits.current().get(), "pending commits")?,
            pending_commit_limit: to_i64(value.pending_commits.limit().get(), "pending commit limit")?,
            oldest: to_i64(value.oldest_buffer_age_ms.get(), "oldest buffer age")?,
            delta_pressure: to_i16(value.delta_pressure_bps.get(), "Delta pressure")?,
            quiver_pressure: to_i16(value.quiver_pressure_bps.get(), "Quiver pressure")?,
            dfe_pressure: to_i16(value.dfe_pressure_bps.get(), "DFE pressure")?,
            delta_state: subsystem_state_text(value.delta_state).to_owned(),
            quiver_state: subsystem_state_text(value.quiver_state).to_owned(),
            dfe_state: subsystem_state_text(value.dfe_state).to_owned(),
        })
    }
}

async fn verify_candidate(
    client: &mut SqlClient,
    candidate: CollectorInstanceId,
    implementation_pool: ImplementationPool,
    minimum_snapshot_at: UnixMillis,
    now: UnixMillis,
) -> RoutingResult<()> {
    let candidate = candidate.as_uuid();
    let minimum_snapshot_at = datetime(minimum_snapshot_at)?;
    let now = datetime(now)?;
    let row = client
        .query(
            "SELECT c.instance_id FROM arrowcol.routing_collectors AS c WITH (UPDLOCK, HOLDLOCK) \
             INNER JOIN arrowcol.routing_pressure_latest AS p WITH (UPDLOCK, HOLDLOCK) \
               ON p.collector_instance_id = c.instance_id \
             WHERE c.instance_id=@P1 AND c.state='ready' AND c.lease_expires_at_utc>@P2 \
               AND p.observed_at_utc>=@P3 AND p.accepting_deliveries=1 \
               AND c.implementation_pool=@P4;",
            &[&candidate, &now, &minimum_snapshot_at, &implementation_pool.to_string()],
        )
        .await
        .map_err(|cause| database_error("verify routing lease candidate", cause))?
        .into_row()
        .await
        .map_err(|cause| database_error("read routing lease candidate", cause))?;
    if row.is_none() {
        return Err(RoutingError::new(
            RoutingErrorKind::PoolUnavailable,
            "selected collector is no longer ready with fresh accepting pressure",
        ));
    }
    Ok(())
}

async fn verify_successor(client: &mut SqlClient, request: &LeaseSuccession) -> RoutingResult<()> {
    let predecessor = request.predecessor_instance_id.as_uuid();
    let successor = request.successor_instance_id.as_uuid();
    let now = datetime(request.now_unix_ms)?;
    // Binary collation plus byte length keeps the instance-key match exact,
    // including case and trailing spaces, like the in-memory comparison.
    let row = client
        .query(
            "SELECT successor.instance_id \
             FROM arrowcol.routing_collectors AS predecessor WITH (UPDLOCK, HOLDLOCK) \
             INNER JOIN arrowcol.routing_collectors AS successor WITH (UPDLOCK, HOLDLOCK) \
               ON successor.instance_key COLLATE Latin1_General_100_BIN2 = predecessor.instance_key \
              AND DATALENGTH(successor.instance_key) = DATALENGTH(predecessor.instance_key) \
              AND successor.instance_id <> predecessor.instance_id \
             WHERE predecessor.instance_id=@P1 AND successor.instance_id=@P2 \
               AND predecessor.implementation_pool=@P3 AND successor.implementation_pool=@P3 \
               AND (predecessor.lease_expires_at_utc<=@P4 OR predecessor.state IN ('stopped', 'failed'));",
            &[
                &predecessor,
                &successor,
                &request.key.implementation_pool.to_string(),
                &now,
            ],
        )
        .await
        .map_err(|cause| database_error("verify routing lease successor", cause))?
        .into_row()
        .await
        .map_err(|cause| database_error("read routing lease successor", cause))?;
    if row.is_none() {
        return Err(RoutingError::new(
            RoutingErrorKind::Conflict,
            "routing lease predecessor is still live or the successor is not its same-key restart",
        ));
    }
    Ok(())
}

async fn select_locked_lease(client: &mut SqlClient, key: &LeaseKey) -> RoutingResult<Option<RoutingLease>> {
    let row = client
        .query(
            &format!(
                "SELECT {LEASE_COLUMNS} FROM arrowcol.routing_table_leases WITH (UPDLOCK, HOLDLOCK) \
                 WHERE implementation_pool=@P1 AND catalog_name=@P2 AND schema_name=@P3 AND table_name=@P4;"
            ),
            &[
                &key.implementation_pool.to_string(),
                &key.table.catalog().as_str(),
                &key.table.schema().as_str(),
                &key.table.table().as_str(),
            ],
        )
        .await
        .map_err(|cause| database_error("lock routing lease", cause))?
        .into_row()
        .await
        .map_err(|cause| database_error("read locked routing lease", cause))?;
    row.as_ref().map(lease_from_row).transpose()
}

async fn lock_distinct_table_owner_pool(
    client: &mut SqlClient,
    implementation_pool: arrowcol_protocol::ImplementationPool,
) -> RoutingResult<()> {
    let resource = format!("arrowcol:distinct-table-owner:{implementation_pool}");
    let row = client
        .query(
            "DECLARE @lock_result int; EXEC @lock_result = sys.sp_getapplock @Resource=@P1, @LockMode='Exclusive', @LockOwner='Transaction', @LockTimeout=5000; SELECT @lock_result AS lock_result;",
            &[&resource],
        )
        .await
        .map_err(|cause| database_error("lock distinct table-owner pool", cause))?
        .into_row()
        .await
        .map_err(|cause| database_error("read distinct table-owner pool lock", cause))?;
    if row.and_then(|row| row.get::<i32, _>(0)).is_none_or(|result| result < 0) {
        return Err(RoutingError::new(
            RoutingErrorKind::PoolUnavailable,
            "could not acquire the distinct table-owner pool lock",
        ));
    }
    Ok(())
}

async fn verify_distinct_table_owner(
    client: &mut SqlClient,
    key: &LeaseKey,
    candidate: CollectorInstanceId,
    now: UnixMillis,
) -> RoutingResult<()> {
    let holder = candidate.as_uuid();
    let now = datetime(now)?;
    let row = client
        .query(
            "SELECT TOP (1) 1 FROM arrowcol.routing_table_leases WITH (UPDLOCK, HOLDLOCK) WHERE implementation_pool=@P1 AND holder_instance_id=@P2 AND state='active' AND expires_at_utc>@P3 AND NOT (catalog_name=@P4 AND schema_name=@P5 AND table_name=@P6);",
            &[
                &key.implementation_pool.to_string(),
                &holder,
                &now,
                &key.table.catalog().as_str(),
                &key.table.schema().as_str(),
                &key.table.table().as_str(),
            ],
        )
        .await
        .map_err(|cause| database_error("check distinct table-owner assignment", cause))?
        .into_row()
        .await
        .map_err(|cause| database_error("read distinct table-owner assignment", cause))?;
    if row.is_some() {
        return Err(RoutingError::new(
            RoutingErrorKind::Conflict,
            "collector already owns another active table lease in distinct table-owner mode",
        ));
    }
    Ok(())
}

async fn insert_lease(client: &mut SqlClient, claim: &LeaseClaim) -> RoutingResult<RoutingLease> {
    let lease_id = LeaseId::new();
    let lease_uuid = lease_id.as_uuid();
    let holder = claim.candidate_instance_id.as_uuid();
    let issued = datetime(claim.now_unix_ms)?;
    let expires = datetime(claim.expires_at_unix_ms)?;
    client
        .execute(
            "INSERT INTO arrowcol.routing_table_leases ( \
                implementation_pool, catalog_name, schema_name, table_name, lease_id, holder_instance_id, \
                fencing_epoch, lease_version, state, issued_at_utc, renewed_at_utc, expires_at_utc, drain_started_at_utc \
             ) VALUES (@P1,@P2,@P3,@P4,@P5,@P6,1,1,'active',@P7,@P7,@P8,NULL);",
            &[
                &claim.key.implementation_pool.to_string(),
                &claim.key.table.catalog().as_str(),
                &claim.key.table.schema().as_str(),
                &claim.key.table.table().as_str(),
                &lease_uuid,
                &holder,
                &issued,
                &expires,
            ],
        )
        .await
        .map_err(|cause| database_error("insert routing lease", cause))?;
    Ok(RoutingLease {
        key: claim.key.clone(),
        lease_id,
        holder_instance_id: claim.candidate_instance_id,
        fencing_epoch: FencingToken::new(1).map_err(|cause| {
            RoutingError::new(
                RoutingErrorKind::CorruptState,
                format!("invalid initial fencing epoch: {cause}"),
            )
        })?,
        lease_version: 1,
        state: LeaseState::Active,
        issued_at_unix_ms: claim.now_unix_ms,
        renewed_at_unix_ms: claim.now_unix_ms,
        expires_at_unix_ms: claim.expires_at_unix_ms,
        drain_started_at_unix_ms: None,
    })
}

#[allow(clippy::too_many_arguments)]
async fn update_lease_owner(
    client: &mut SqlClient,
    key: &LeaseKey,
    expected_version: u64,
    holder: CollectorInstanceId,
    epoch: FencingToken,
    version: u64,
    now: UnixMillis,
    expires: UnixMillis,
) -> RoutingResult<RoutingLease> {
    let lease_id = LeaseId::new();
    let lease_uuid = lease_id.as_uuid();
    let holder_uuid = holder.as_uuid();
    let epoch_i64 = to_i64(epoch.get(), "fencing epoch")?;
    let version_i64 = to_i64(version, "lease version")?;
    let expected_i64 = to_i64(expected_version, "expected lease version")?;
    let now_sql = datetime(now)?;
    let expires_sql = datetime(expires)?;
    let affected = client
        .execute(
            "UPDATE arrowcol.routing_table_leases SET \
                lease_id=@P5, holder_instance_id=@P6, fencing_epoch=@P7, lease_version=@P8, \
                state='active', issued_at_utc=@P9, renewed_at_utc=@P9, expires_at_utc=@P10, \
                drain_started_at_utc=NULL \
             WHERE implementation_pool=@P1 AND catalog_name=@P2 AND schema_name=@P3 AND table_name=@P4 \
               AND lease_version=@P11;",
            &[
                &key.implementation_pool.to_string(),
                &key.table.catalog().as_str(),
                &key.table.schema().as_str(),
                &key.table.table().as_str(),
                &lease_uuid,
                &holder_uuid,
                &epoch_i64,
                &version_i64,
                &now_sql,
                &expires_sql,
                &expected_i64,
            ],
        )
        .await
        .map_err(|cause| database_error("update routing lease owner", cause))?
        .total();
    if affected != 1 {
        return Err(RoutingError::new(
            RoutingErrorKind::Conflict,
            "routing lease changed before owner compare-and-set",
        ));
    }
    Ok(RoutingLease {
        key: key.clone(),
        lease_id,
        holder_instance_id: holder,
        fencing_epoch: epoch,
        lease_version: version,
        state: LeaseState::Active,
        issued_at_unix_ms: now,
        renewed_at_unix_ms: now,
        expires_at_unix_ms: expires,
        drain_started_at_unix_ms: None,
    })
}

fn collector_from_row(row: &Row) -> RoutingResult<CollectorRecord> {
    let instance_id = collector_id(required_uuid(row, "instance_id")?)?;
    let registration = CollectorRegistration {
        instance_id,
        instance_key: required_str(row, "instance_key")?.to_owned(),
        implementation_pool: parse_pool(required_str(row, "implementation_pool")?)?,
        flight_endpoint: required_str(row, "flight_endpoint")?.parse().map_err(|cause| {
            RoutingError::new(
                RoutingErrorKind::CorruptState,
                format!("persisted Flight endpoint is invalid: {cause}"),
            )
        })?,
        catalog_capable: required_bool(row, "catalog_capable")?,
        max_concurrent_streams: positive(required_i64(row, "max_concurrent_streams")?, "max concurrent streams")?,
        started_at_unix_ms: unix_millis(required_datetime(row, "started_at_utc")?)?,
        heartbeat_at_unix_ms: unix_millis(required_datetime(row, "heartbeat_at_utc")?)?,
        lease_expires_at_unix_ms: unix_millis(required_datetime(row, "lease_expires_at_utc")?)?,
    };
    registration.validate()?;
    let pressure = row
        .try_get::<NaiveDateTime, _>("observed_at_utc")
        .map_err(|cause| database_error("read observed_at_utc", cause))?
        .map(|observed| pressure_from_row(row, instance_id, observed))
        .transpose()?;
    Ok(CollectorRecord {
        registration,
        state: parse_collector_state(required_str(row, "state")?)?,
        pressure,
    })
}

fn pressure_from_row(
    row: &Row,
    instance_id: CollectorInstanceId,
    observed: NaiveDateTime,
) -> RoutingResult<RichPressureSnapshot> {
    let table_json = required_str(row, "table_pressure_json")?;
    let tables = serde_json::from_str(table_json).map_err(|cause| {
        RoutingError::new(
            RoutingErrorKind::CorruptState,
            format!("persisted table pressure JSON is invalid: {cause}"),
        )
    })?;
    Ok(RichPressureSnapshot {
        instance_id,
        observed_at_unix_ms: unix_millis(observed)?,
        cpu_utilization_bps: basis_points(required_i16(row, "cpu_utilization_bps")?)?,
        rss_bytes: measurement(required_i64(row, "rss_bytes")?, "RSS bytes")?,
        rss_limit_bytes: positive(required_i64(row, "rss_limit_bytes")?, "RSS limit")?,
        active_streams: gauge(row, "active_streams", "active_stream_limit")?,
        received_bytes: measurement(required_i64(row, "received_bytes")?, "received bytes")?,
        received_rows: measurement(required_i64(row, "received_rows")?, "received rows")?,
        received_batches: measurement(required_i64(row, "received_batches")?, "received batches")?,
        buffered_bytes: gauge(row, "buffered_bytes", "buffered_bytes_limit")?,
        buffered_rows: gauge(row, "buffered_rows", "buffered_rows_limit")?,
        buffered_batches: gauge(row, "buffered_batches", "buffered_batches_limit")?,
        available_permits: gauge(row, "available_permits", "permit_limit")?,
        pending_commits: gauge(row, "pending_commits", "pending_commit_limit")?,
        oldest_buffer_age_ms: measurement(required_i64(row, "oldest_buffer_age_ms")?, "oldest buffer age")?,
        delta_pressure_bps: basis_points(required_i16(row, "delta_pressure_bps")?)?,
        quiver_pressure_bps: basis_points(required_i16(row, "quiver_pressure_bps")?)?,
        dfe_pressure_bps: basis_points(required_i16(row, "dfe_pressure_bps")?)?,
        delta_state: parse_subsystem_state(required_str(row, "delta_state")?)?,
        quiver_state: parse_subsystem_state(required_str(row, "quiver_state")?)?,
        dfe_state: parse_subsystem_state(required_str(row, "dfe_state")?)?,
        accepting_deliveries: required_bool(row, "accepting_deliveries")?,
        tables,
    })
}

fn lease_from_row(row: &Row) -> RoutingResult<RoutingLease> {
    let lease = RoutingLease {
        key: LeaseKey::new(
            parse_pool(required_str(row, "implementation_pool")?)?,
            TableIdentity::new(
                CatalogName::new(required_str(row, "catalog_name")?).map_err(corrupt)?,
                SchemaName::new(required_str(row, "schema_name")?).map_err(corrupt)?,
                TableName::new(required_str(row, "table_name")?).map_err(corrupt)?,
            ),
        ),
        lease_id: LeaseId::from_str(&required_uuid(row, "lease_id")?.to_string()).map_err(corrupt)?,
        holder_instance_id: collector_id(required_uuid(row, "holder_instance_id")?)?,
        fencing_epoch: FencingToken::new(to_u64(required_i64(row, "fencing_epoch")?, "fencing epoch")?)
            .map_err(corrupt)?,
        lease_version: to_u64(required_i64(row, "lease_version")?, "lease version")?,
        state: parse_lease_state(required_str(row, "state")?)?,
        issued_at_unix_ms: unix_millis(required_datetime(row, "issued_at_utc")?)?,
        renewed_at_unix_ms: unix_millis(required_datetime(row, "renewed_at_utc")?)?,
        expires_at_unix_ms: unix_millis(required_datetime(row, "expires_at_utc")?)?,
        drain_started_at_unix_ms: row
            .try_get::<NaiveDateTime, _>("drain_started_at_utc")
            .map_err(|cause| database_error("read drain_started_at_utc", cause))?
            .map(unix_millis)
            .transpose()?,
    };
    lease.validate()?;
    Ok(lease)
}

fn run_table_pin_from_row(row: &Row) -> RoutingResult<RunTablePin> {
    let pin = RunTablePin {
        run_id: required_uuid(row, "run_id")?.to_string(),
        key: LeaseKey::new(
            parse_pool(required_str(row, "implementation_pool")?)?,
            TableIdentity::new(
                CatalogName::new(required_str(row, "catalog_name")?).map_err(corrupt)?,
                SchemaName::new(required_str(row, "schema_name")?).map_err(corrupt)?,
                TableName::new(required_str(row, "table_name")?).map_err(corrupt)?,
            ),
        ),
        holder_instance_id: collector_id(required_uuid(row, "holder_instance_id")?)?,
        lease_id: LeaseId::from_str(&required_uuid(row, "lease_id")?.to_string()).map_err(corrupt)?,
        fencing_epoch: FencingToken::new(to_u64(required_i64(row, "fencing_epoch")?, "fencing epoch")?)
            .map_err(corrupt)?,
        expires_at_unix_ms: unix_millis(required_datetime(row, "expires_at_utc")?)?,
    };
    if uuid::Uuid::parse_str(&pin.run_id).is_err() {
        return Err(RoutingError::new(
            RoutingErrorKind::CorruptState,
            "persisted OAT run-table pin has an invalid UUID run ID",
        ));
    }
    Ok(pin)
}

async fn verify_run_pin_set(
    client: &mut SqlClient,
    run_id: Uuid,
    expected_count: i32,
    expected_digest: &str,
    expires_at: UnixMillis,
    now: UnixMillis,
) -> RoutingResult<()> {
    let rows = client
        .query(
            "SELECT CONVERT(bigint, expected_table_count) AS expected_table_count, \
                    expected_table_set_sha256, expires_at_utc, \
                    (SELECT COUNT_BIG(*) FROM arrowcol.routing_run_table_initializations m \
                     WHERE m.run_id=s.run_id) AS actual_table_count, \
                    arrowcol.routing_run_pin_set_digest(s.run_id) AS actual_table_set_sha256 \
             FROM arrowcol.routing_run_pin_sets s WITH (UPDLOCK, HOLDLOCK) WHERE s.run_id=@P1;",
            &[&run_id],
        )
        .await
        .map_err(|cause| database_error("verify OAT run pin set", cause))?
        .into_first_result()
        .await
        .map_err(|cause| database_error("read OAT run pin set verification", cause))?;
    let row = rows
        .first()
        .ok_or_else(|| RoutingError::new(RoutingErrorKind::Conflict, "immutable OAT run pin set is missing"))?;
    if required_i64(row, "expected_table_count")? != i64::from(expected_count)
        || required_i64(row, "actual_table_count")? != i64::from(expected_count)
        || required_str(row, "expected_table_set_sha256")? != expected_digest
        || required_str(row, "actual_table_set_sha256")? != expected_digest
        || unix_millis(required_datetime(row, "expires_at_utc")?)? != expires_at
        || expires_at <= now
    {
        return Err(RoutingError::new(
            RoutingErrorKind::Conflict,
            "OAT run pin set is incomplete, expired, or differs from its immutable digest and expiry",
        ));
    }
    Ok(())
}

async fn validate_live_run_pin_set(client: &mut SqlClient, run_id: Uuid, now: UnixMillis) -> RoutingResult<()> {
    let rows = client
        .query(
            "SELECT CONVERT(bigint, expected_table_count) AS expected_table_count, \
                    expected_table_set_sha256, expires_at_utc, \
                    (SELECT COUNT_BIG(*) FROM arrowcol.routing_run_table_initializations m \
                     WHERE m.run_id=s.run_id) AS actual_table_count, \
                    arrowcol.routing_run_pin_set_digest(s.run_id) AS actual_table_set_sha256 \
             FROM arrowcol.routing_run_pin_sets s WHERE s.run_id=@P1;",
            &[&run_id],
        )
        .await
        .map_err(|cause| database_error("validate OAT run pin set", cause))?
        .into_first_result()
        .await
        .map_err(|cause| database_error("read OAT run pin set validation", cause))?;
    let row = rows
        .first()
        .ok_or_else(|| RoutingError::new(RoutingErrorKind::Conflict, "immutable OAT run pin set is missing"))?;
    if required_i64(row, "expected_table_count")? != required_i64(row, "actual_table_count")?
        || required_str(row, "expected_table_set_sha256")? != required_str(row, "actual_table_set_sha256")?
        || unix_millis(required_datetime(row, "expires_at_utc")?)? <= now
    {
        return Err(RoutingError::new(
            RoutingErrorKind::Conflict,
            "OAT run pin set is incomplete, expired, or has a changed digest",
        ));
    }
    Ok(())
}

fn parse_run_id(run_id: &str) -> RoutingResult<uuid::Uuid> {
    let parsed = uuid::Uuid::parse_str(run_id)
        .map_err(|_| RoutingError::new(RoutingErrorKind::InvalidConfiguration, "OAT run ID must be a UUID"))?;
    if parsed.to_string() != run_id {
        return Err(RoutingError::new(
            RoutingErrorKind::InvalidConfiguration,
            "OAT run ID must be a canonical lowercase UUID",
        ));
    }
    Ok(parsed)
}

async fn active_run_pin_exists(client: &mut SqlClient, key: &LeaseKey, now: UnixMillis) -> RoutingResult<bool> {
    let now = datetime(now)?;
    let rows = client
        .query(
            "SELECT TOP (1) run_id FROM arrowcol.routing_run_table_pins WITH (UPDLOCK, HOLDLOCK) \
             WHERE implementation_pool=@P1 AND catalog_name=@P2 AND schema_name=@P3 AND table_name=@P4 \
               AND expires_at_utc>@P5;",
            &[
                &pool_name(key.implementation_pool),
                &key.table.catalog().as_str(),
                &key.table.schema().as_str(),
                &key.table.table().as_str(),
                &now,
            ],
        )
        .await
        .map_err(|cause| database_error("check active OAT run-table pin", cause))?
        .into_first_result()
        .await
        .map_err(|cause| database_error("read active OAT run-table pin", cause))?;
    Ok(!rows.is_empty())
}

async fn validate_sql_run_pin(client: &mut SqlClient, pin: &RunTablePin, now: UnixMillis) -> RoutingResult<()> {
    pin.validate(now)?;
    let lease = select_locked_lease(client, &pin.key)
        .await?
        .ok_or_else(|| RoutingError::new(RoutingErrorKind::Conflict, "pinned routing lease is missing"))?;
    if !lease.is_routable_at(now)
        || lease.holder_instance_id != pin.holder_instance_id
        || lease.lease_id != pin.lease_id
        || lease.fencing_epoch != pin.fencing_epoch
    {
        return Err(RoutingError::new(
            RoutingErrorKind::Conflict,
            "pinned table owner or lease epoch changed or expired",
        ));
    }
    let holder = pin.holder_instance_id.as_uuid();
    let rows = client
        .query(
            "SELECT implementation_pool, state, lease_expires_at_utc \
             FROM arrowcol.routing_collectors WHERE instance_id=@P1;",
            &[&holder],
        )
        .await
        .map_err(|cause| database_error("validate pinned collector process", cause))?
        .into_first_result()
        .await
        .map_err(|cause| database_error("read pinned collector process", cause))?;
    let row = rows.first().ok_or_else(|| {
        RoutingError::new(
            RoutingErrorKind::Conflict,
            "pinned collector process registration is missing",
        )
    })?;
    if parse_pool(required_str(row, "implementation_pool")?)? != pin.key.implementation_pool
        || parse_collector_state(required_str(row, "state")?)? != CollectorState::Ready
        || unix_millis(required_datetime(row, "lease_expires_at_utc")?)? <= now
    {
        return Err(RoutingError::new(
            RoutingErrorKind::Conflict,
            "pinned collector process lifetime has expired or is not ready",
        ));
    }
    Ok(())
}

fn same_run_table_pin(existing: &RunTablePin, requested: &RunTablePin) -> bool {
    existing.run_id == requested.run_id
        && existing.key == requested.key
        && existing.holder_instance_id == requested.holder_instance_id
        && existing.lease_id == requested.lease_id
        && existing.fencing_epoch == requested.fencing_epoch
}

fn pool_name(pool: ImplementationPool) -> &'static str {
    match pool {
        ImplementationPool::Dataflow => "dataflow",
    }
}

async fn finish_transaction<T>(
    client: &mut SqlClient,
    result: RoutingResult<T>,
    operation: &'static str,
) -> RoutingResult<T> {
    match result {
        Ok(value) => {
            client
                .commit_transaction()
                .await
                .map_err(|cause| database_error(&format!("commit {operation}"), cause))?;
            Ok(value)
        }
        Err(error) => {
            if let Err(rollback_error) = client.rollback_transaction().await {
                tracing::warn!(
                    operation,
                    error = %rollback_error,
                    "SQL Server rollback failed; preserving primary operation error and discarding connection"
                );
            }
            Err(error)
        }
    }
}

fn validate_lease_window(now: UnixMillis, expires: UnixMillis) -> RoutingResult<()> {
    if expires <= now {
        return Err(RoutingError::new(
            RoutingErrorKind::InvalidConfiguration,
            "routing lease expiry must be later than issuance",
        ));
    }
    Ok(())
}

fn datetime(value: UnixMillis) -> RoutingResult<NaiveDateTime> {
    let seconds = i64::try_from(value.get() / 1_000).map_err(corrupt)?;
    let nanos = u32::try_from((value.get() % 1_000) * 1_000_000).map_err(corrupt)?;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .map(|value| value.naive_utc())
        .ok_or_else(|| RoutingError::new(RoutingErrorKind::InvalidConfiguration, "timestamp is outside SQL range"))
}

fn unix_millis(value: NaiveDateTime) -> RoutingResult<UnixMillis> {
    let utc = value.and_utc();
    let millis = utc.timestamp_millis();
    let millis = u64::try_from(millis).map_err(corrupt)?;
    UnixMillis::new(millis).map_err(corrupt)
}

fn collector_id(value: Uuid) -> RoutingResult<CollectorInstanceId> {
    CollectorInstanceId::from_str(&value.to_string()).map_err(corrupt)
}

fn parse_pool(value: &str) -> RoutingResult<ImplementationPool> {
    value.parse().map_err(corrupt)
}

fn parse_collector_state(value: &str) -> RoutingResult<CollectorState> {
    match value {
        "ready" => Ok(CollectorState::Ready),
        "draining" => Ok(CollectorState::Draining),
        "stopped" => Ok(CollectorState::Stopped),
        "failed" => Ok(CollectorState::Failed),
        _ => Err(corrupt(format!("unknown collector state {value:?}"))),
    }
}

fn parse_lease_state(value: &str) -> RoutingResult<LeaseState> {
    match value {
        "active" => Ok(LeaseState::Active),
        "draining" => Ok(LeaseState::Draining),
        _ => Err(corrupt(format!("unknown lease state {value:?}"))),
    }
}

fn parse_subsystem_state(value: &str) -> RoutingResult<SubsystemState> {
    match value {
        "ready" => Ok(SubsystemState::Ready),
        "degraded" => Ok(SubsystemState::Degraded),
        "blocked" => Ok(SubsystemState::Blocked),
        "disabled" => Ok(SubsystemState::Disabled),
        _ => Err(corrupt(format!("unknown subsystem state {value:?}"))),
    }
}

const fn collector_state_text(value: CollectorState) -> &'static str {
    match value {
        CollectorState::Ready => "ready",
        CollectorState::Draining => "draining",
        CollectorState::Stopped => "stopped",
        CollectorState::Failed => "failed",
    }
}

const fn subsystem_state_text(value: SubsystemState) -> &'static str {
    match value {
        SubsystemState::Ready => "ready",
        SubsystemState::Degraded => "degraded",
        SubsystemState::Blocked => "blocked",
        SubsystemState::Disabled => "disabled",
    }
}

fn measurement(value: i64, label: &str) -> RoutingResult<Measurement> {
    Measurement::new(to_u64(value, label)?).map_err(corrupt)
}

fn positive(value: i64, label: &str) -> RoutingResult<PositiveMeasurement> {
    PositiveMeasurement::new(to_u64(value, label)?).map_err(corrupt)
}

fn basis_points(value: i16) -> RoutingResult<BasisPoints> {
    BasisPoints::new(u16::try_from(value).map_err(corrupt)?).map_err(corrupt)
}

fn gauge(row: &Row, current: &str, limit: &str) -> RoutingResult<PressureGauge> {
    PressureGauge::new(
        measurement(required_i64(row, current)?, current)?,
        positive(required_i64(row, limit)?, limit)?,
    )
    .map_err(corrupt)
}

fn required_uuid(row: &Row, column: &str) -> RoutingResult<Uuid> {
    row.try_get::<Uuid, _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))?
        .ok_or_else(|| corrupt(format!("{column} was NULL")))
}

fn required_str<'a>(row: &'a Row, column: &str) -> RoutingResult<&'a str> {
    row.try_get::<&str, _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))?
        .ok_or_else(|| corrupt(format!("{column} was NULL")))
}

fn required_i64(row: &Row, column: &str) -> RoutingResult<i64> {
    row.try_get::<i64, _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))?
        .ok_or_else(|| corrupt(format!("{column} was NULL")))
}

fn required_i16(row: &Row, column: &str) -> RoutingResult<i16> {
    row.try_get::<i16, _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))?
        .ok_or_else(|| corrupt(format!("{column} was NULL")))
}

fn required_bool(row: &Row, column: &str) -> RoutingResult<bool> {
    row.try_get::<bool, _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))?
        .ok_or_else(|| corrupt(format!("{column} was NULL")))
}

fn required_datetime(row: &Row, column: &str) -> RoutingResult<NaiveDateTime> {
    row.try_get::<NaiveDateTime, _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))?
        .ok_or_else(|| corrupt(format!("{column} was NULL")))
}

fn to_i64(value: u64, label: &str) -> RoutingResult<i64> {
    i64::try_from(value).map_err(|_| {
        RoutingError::new(
            RoutingErrorKind::InvalidConfiguration,
            format!("{label} exceeds SQL bigint"),
        )
    })
}

fn to_i16(value: u16, label: &str) -> RoutingResult<i16> {
    i16::try_from(value).map_err(|_| {
        RoutingError::new(
            RoutingErrorKind::InvalidConfiguration,
            format!("{label} exceeds SQL smallint"),
        )
    })
}

fn to_u64(value: i64, label: &str) -> RoutingResult<u64> {
    u64::try_from(value).map_err(|_| corrupt(format!("{label} is negative")))
}

fn corrupt(cause: impl std::fmt::Display) -> RoutingError {
    RoutingError::new(RoutingErrorKind::CorruptState, cause.to_string())
}

fn database_error(operation: &str, cause: impl std::fmt::Display) -> RoutingError {
    RoutingError::new(
        RoutingErrorKind::Database,
        format!("SQL Server {operation} failed: {cause}"),
    )
}

#[cfg(test)]
mod pooling_tests {
    use super::*;
    use arrowcol_sql_server::PoolConfig;

    #[test]
    fn pooling_requires_equivalent_connection_configuration() -> anyhow::Result<()> {
        let config = SqlServerRoutingConfig::new("localhost", 1433, "catalog", "pool-user", "pool-secret")
            .trust_server_certificate(true)
            .with_timeouts(Duration::from_secs(3), Duration::from_secs(7));
        let pool = SqlPool::new(
            config.connection_config(),
            PoolConfig {
                max_size: 2,
                acquire_timeout: Duration::from_secs(1),
            },
        )?;
        let store = SqlServerRoutingStore::new(config.clone()).with_pool(pool.clone())?;
        assert!(store.clone().inner.pool.is_some());
        assert!(SqlServerRoutingStore::new(config.clone()).inner.pool.is_none());

        for field in 0..8 {
            let mut mismatch = config.clone();
            match field {
                0 => mismatch.host = "other-host".to_owned(),
                1 => mismatch.port = 1434,
                2 => mismatch.database = "other-database".to_owned(),
                3 => mismatch.username = "other-user".to_owned(),
                4 => mismatch.password = "other-secret".to_owned(),
                5 => mismatch.trust_server_certificate = false,
                6 => mismatch.connect_timeout = Duration::from_secs(4),
                _ => mismatch.command_timeout = Duration::from_secs(8),
            }
            let Err(error) = SqlServerRoutingStore::new(mismatch).with_pool(pool.clone()) else {
                anyhow::bail!("store accepted a mismatched pool");
            };
            assert_eq!(error.kind(), RoutingErrorKind::InvalidConfiguration);
            assert!(!error.to_string().contains("pool-user"));
            assert!(!error.to_string().contains("pool-secret"));
            assert!(!error.to_string().contains("other-secret"));
        }
        assert_eq!(pool.stats().created, 0);
        pool.close();
        assert_eq!(
            store.clone().inner.pool.as_ref().map(|pool| pool.stats().max_size),
            Some(0)
        );
        Ok(())
    }
}
