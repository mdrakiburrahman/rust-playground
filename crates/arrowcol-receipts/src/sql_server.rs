use std::{collections::HashMap, path::Path, str::FromStr, time::Duration};

use arrowcol_catalog::SchemaVersion;
use arrowcol_protocol::{
    CatalogName, CollectorInstanceId, DeltaPublicationResult, ErrorCode, ErrorStatus, FencingToken, ImplementationPool,
    LeaseId, Measurement, PositiveMeasurement, ReceiptId, ReceiptState, SchemaFingerprint, SchemaName, TableIdentity,
    TableLease, TableName, UnixMillis,
};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use tiberius::{IsolationLevel, Row, ToSql};
use uuid::Uuid;

use crate::{
    FailureDisposition, MAX_DURABLE_RECOVERY_WINDOW_MS, OwnerDrain, OwnerRegistration, PublicationBatch,
    ReceiptAdoption, ReceiptAdoptionReport, ReceiptError, ReceiptErrorKind, ReceiptFailure, ReceiptRecord,
    ReceiptResult, ReceiptStore, ReceiptVersion, ReconciliationReport, RejectedAdoption, TableLeaseRequest,
    ValidatedDelivery,
};

const RECEIPT_COLUMNS: &str = "\
    r.receipt_id, r.table_id, r.owner_instance_id, r.implementation_pool, \
    r.schema_version, r.schema_sha256, r.lease_id, r.fencing_epoch, \
    r.state, r.revision, r.row_count, r.batch_count, r.arrow_memory_bytes, \
    r.receipt_lease_deadline_utc, r.heartbeat_at_utc, r.created_at_utc, r.updated_at_utc, \
    r.failure_disposition, r.error_code, r.error_message, r.retry_after_ms, \
    r.delta_version, r.data_files_added, r.rows_published, r.bytes_published, r.published_at_utc, \
    t.catalog_name, t.schema_name, t.table_name, \
    l.acquired_at_utc AS table_lease_acquired_at_utc, l.expires_at_utc AS table_lease_expires_at_utc";

const RECEIPT_FROM: &str = "\
    FROM arrowcol.delivery_receipts AS r \
    INNER JOIN arrowcol.catalog_tables AS t ON t.table_id = r.table_id \
    INNER JOIN arrowcol.table_leases AS l \
      ON l.table_id = r.table_id \
     AND l.fencing_epoch = r.fencing_epoch \
     AND l.lease_id = r.lease_id \
     AND l.holder_instance_id = r.owner_instance_id";

use arrowcol_sql_server::{ConnectionConfig, SqlClient, SqlConnection, SqlPool};

/// SQL Server application-database connection and API bounds.
#[derive(Clone)]
pub struct SqlServerReceiptConfig {
    host: String,
    port: u16,
    database: String,
    username: String,
    password: String,
    trust_server_certificate: bool,
    connect_timeout: Duration,
    command_timeout: Duration,
    max_batch_size: usize,
    max_status_batch_size: usize,
}

impl SqlServerReceiptConfig {
    /// Creates SQL-authenticated application database configuration.
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
            max_batch_size: 256,
            max_status_batch_size: 256,
        }
    }

    /// Reads a one-line password from a secret file.
    ///
    /// # Errors
    ///
    /// Returns an invalid-configuration error for unreadable, empty, or
    /// multiline secret material.
    pub fn with_password_file(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        username: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> ReceiptResult<Self> {
        let password = std::fs::read_to_string(path.as_ref()).map_err(|cause| {
            ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                format!("could not read SQL Server password file: {cause}"),
            )
        })?;
        let password = password.trim_end_matches(['\r', '\n']);
        if password.is_empty() || password.contains(['\r', '\n']) {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
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

    /// Sets transactional mutation and status-query bounds.
    ///
    /// # Errors
    ///
    /// Rejects zero bounds and SQL Server's 2,100-parameter ceiling.
    pub fn with_batch_limits(mut self, mutations: usize, statuses: usize) -> ReceiptResult<Self> {
        if mutations == 0 || statuses == 0 || mutations > 512 || statuses > 512 {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "receipt batch limits must be in 1..=512",
            ));
        }
        self.max_batch_size = mutations;
        self.max_status_batch_size = statuses;
        Ok(self)
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

/// SQL Server-backed receipt store over the Arrowcol application database.
///
/// Transactions chosen as deadlock victims (SQL error 1205) are rolled back by
/// the server and retried a few times before the error is surfaced. Every
/// operation runs at most one transaction, so a retry never repeats committed
/// work.
#[derive(Clone)]
pub struct SqlServerReceiptStore {
    inner: RawSqlReceiptStore,
}

impl SqlServerReceiptStore {
    /// Creates a receipt store.
    #[must_use]
    pub const fn new(config: SqlServerReceiptConfig) -> Self {
        Self {
            inner: RawSqlReceiptStore { config, pool: None },
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
    pub fn with_pool(mut self, pool: SqlPool) -> ReceiptResult<Self> {
        if !pool.matches_config(&self.inner.config.connection_config()) {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "SQL Server pool configuration does not match store configuration",
            ));
        }
        self.inner.pool = Some(pool);
        Ok(self)
    }
}

const DEADLOCK_ATTEMPTS: u32 = 6;

async fn retry_deadlock<T, F, Fut>(mut operation: F) -> ReceiptResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ReceiptResult<T>>,
{
    let mut attempt = 1;
    loop {
        match operation().await {
            Err(error) if attempt < DEADLOCK_ATTEMPTS && is_deadlock_victim(&error) => {
                tokio::time::sleep(Duration::from_millis(25 * u64::from(attempt))).await;
                attempt += 1;
            }
            other => return other,
        }
    }
}

fn is_deadlock_victim(error: &ReceiptError) -> bool {
    error.kind() == ReceiptErrorKind::Database && error.to_string().contains("deadlock victim")
}

#[async_trait]
impl ReceiptStore for SqlServerReceiptStore {
    async fn register_owner(&self, registration: OwnerRegistration) -> ReceiptResult<()> {
        retry_deadlock(|| self.inner.register_owner(registration.clone())).await
    }

    async fn heartbeat_owner(&self, owner: CollectorInstanceId, lease_deadline: UnixMillis) -> ReceiptResult<()> {
        retry_deadlock(|| self.inner.heartbeat_owner(owner, lease_deadline)).await
    }

    async fn start_owner_drain(&self, owner: CollectorInstanceId) -> ReceiptResult<OwnerDrain> {
        retry_deadlock(|| self.inner.start_owner_drain(owner)).await
    }

    async fn finish_owner_drain(&self, owner: CollectorInstanceId, stopped_at: UnixMillis) -> ReceiptResult<()> {
        retry_deadlock(|| self.inner.finish_owner_drain(owner, stopped_at)).await
    }

    async fn acquire_table_lease(&self, request: TableLeaseRequest) -> ReceiptResult<TableLease> {
        retry_deadlock(|| self.inner.acquire_table_lease(request.clone())).await
    }

    async fn renew_table_lease(&self, lease: &TableLease, expires_at: UnixMillis) -> ReceiptResult<TableLease> {
        retry_deadlock(|| self.inner.renew_table_lease(lease, expires_at)).await
    }

    async fn release_table_lease(&self, lease: &TableLease, released_at: UnixMillis) -> ReceiptResult<()> {
        retry_deadlock(|| self.inner.release_table_lease(lease, released_at)).await
    }

    async fn create_flight_batch(
        &self,
        owner: CollectorInstanceId,
        lease: &TableLease,
        lease_deadline: UnixMillis,
        deliveries: &[ValidatedDelivery],
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        retry_deadlock(|| self.inner.create_flight_batch(owner, lease, lease_deadline, deliveries)).await
    }

    async fn mark_buffered(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        retry_deadlock(|| self.inner.mark_buffered(owner, receipts, lease_deadline)).await
    }

    async fn mark_flushing(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        retry_deadlock(|| self.inner.mark_flushing(owner, receipts, lease_deadline)).await
    }

    async fn heartbeat_receipts(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<()> {
        retry_deadlock(|| self.inner.heartbeat_receipts(owner, receipts, lease_deadline)).await
    }

    async fn publish_commit(
        &self,
        owner: CollectorInstanceId,
        batch: &PublicationBatch,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        retry_deadlock(|| self.inner.publish_commit(owner, batch)).await
    }

    async fn fail_batch(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        failure: &ReceiptFailure,
        failed_at: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        retry_deadlock(|| self.inner.fail_batch(owner, receipts, failure, failed_at)).await
    }

    async fn receipt(&self, receipt_id: ReceiptId) -> ReceiptResult<Option<ReceiptRecord>> {
        retry_deadlock(|| self.inner.receipt(receipt_id)).await
    }

    async fn receipts(&self, receipt_ids: &[ReceiptId]) -> ReceiptResult<Vec<Option<ReceiptRecord>>> {
        retry_deadlock(|| self.inner.receipts(receipt_ids)).await
    }

    async fn reconcile_expired(&self, now: UnixMillis, limit: usize) -> ReceiptResult<ReconciliationReport> {
        retry_deadlock(|| self.inner.reconcile_expired(now, limit)).await
    }

    async fn adopt_receipts(&self, request: &ReceiptAdoption) -> ReceiptResult<ReceiptAdoptionReport> {
        retry_deadlock(|| self.inner.adopt_receipts(request)).await
    }

    async fn fail_unbuffered_predecessor_receipts(
        &self,
        successor: CollectorInstanceId,
        failed_at: UnixMillis,
        limit: usize,
    ) -> ReceiptResult<ReconciliationReport> {
        retry_deadlock(|| {
            self.inner
                .fail_unbuffered_predecessor_receipts(successor, failed_at, limit)
        })
        .await
    }
}

#[derive(Clone)]
struct RawSqlReceiptStore {
    config: SqlServerReceiptConfig,
    pool: Option<SqlPool>,
}

impl RawSqlReceiptStore {
    async fn connect(&self) -> ReceiptResult<SqlConnection> {
        match &self.pool {
            Some(pool) => pool.acquire().await,
            None => SqlConnection::connect(&self.config.connection_config()).await,
        }
        .map_err(|error| database_error("acquire SQL Server connection", error))
    }

    fn mutation_bound(&self, count: usize) -> ReceiptResult<()> {
        if count > self.config.max_batch_size {
            return Err(ReceiptError::new(
                ReceiptErrorKind::RequestTooLarge,
                format!(
                    "receipt mutation batch contains {count} entries; maximum is {}",
                    self.config.max_batch_size
                ),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ReceiptStore for RawSqlReceiptStore {
    async fn register_owner(&self, registration: OwnerRegistration) -> ReceiptResult<()> {
        if registration.generation == 0 || registration.instance_key.is_empty() || registration.instance_key.len() > 200
        {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "owner key must be 1..=200 bytes and generation must be positive",
            ));
        }
        let generation = i64::try_from(registration.generation).map_err(|_| {
            ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "owner generation exceeds SQL bigint",
            )
        })?;
        let started = datetime(registration.started_at_unix_ms)?;
        let deadline = datetime(registration.lease_deadline_unix_ms)?;
        if deadline <= started {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "owner lease deadline must be later than registration",
            ));
        }
        let recovery_window = registration
            .durable_recovery_window_ms
            .map(recovery_window_i32)
            .transpose()?;
        let instance = registration.instance_id.as_uuid();
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin receipt owner registration", cause))?;
            let result = async {
                let max_generation = client
                    .query(
                        "SELECT MAX(generation) AS max_generation \
                     FROM arrowcol.collector_instances WITH (UPDLOCK, HOLDLOCK) \
                     WHERE instance_key = @P1;",
                        &[&registration.instance_key],
                    )
                    .await
                    .map_err(|cause| database_error("lock receipt owner generation", cause))?
                    .into_row()
                    .await
                    .map_err(|cause| database_error("read receipt owner generation", cause))?
                    .and_then(|row| row.get::<i64, _>("max_generation"));
                if max_generation.is_some_and(|current| generation <= current) {
                    return Err(ReceiptError::new(
                        ReceiptErrorKind::Conflict,
                        "owner generation must advance beyond every prior registration for the instance key",
                    ));
                }

                let active = client
                    .query(
                        "SELECT TOP (1) instance_id, lease_expires_at_utc \
                     FROM arrowcol.collector_instances WITH (UPDLOCK, HOLDLOCK) \
                     WHERE instance_key = @P1 AND stopped_at_utc IS NULL;",
                        &[&registration.instance_key],
                    )
                    .await
                    .map_err(|cause| database_error("lock active receipt owner", cause))?
                    .into_row()
                    .await
                    .map_err(|cause| database_error("read active receipt owner", cause))?;
                if let Some(active) = active {
                    let prior = required_uuid(&active, "instance_id")?;
                    let prior_expiry = required_datetime(&active, "lease_expires_at_utc")?;
                    if prior_expiry > started {
                        return Err(ReceiptError::new(
                            ReceiptErrorKind::Conflict,
                            "a live owner already holds the configured instance key",
                        ));
                    }
                    client
                        .execute(
                            "UPDATE arrowcol.table_leases \
                         SET released_at_utc = @P2, release_reason = 'expired' \
                         WHERE holder_instance_id = @P1 AND released_at_utc IS NULL; \
                         UPDATE arrowcol.collector_instances \
                         SET state = 'failed', stopped_at_utc = @P2 \
                         WHERE instance_id = @P1 AND stopped_at_utc IS NULL;",
                            &[&prior, &started],
                        )
                        .await
                        .map_err(|cause| database_error("fence expired receipt owner", cause))?;
                }

                client
                    .execute(
                        "INSERT INTO arrowcol.collector_instances ( \
                        instance_id, instance_key, generation, state, host_name, build_version, \
                        started_at_utc, heartbeat_at_utc, lease_expires_at_utc, durable_recovery_window_ms \
                     ) VALUES (@P1, @P2, @P3, 'active', @P4, @P5, @P6, @P6, @P7, @P8);",
                        &[
                            &instance,
                            &registration.instance_key,
                            &generation,
                            &registration.host_name,
                            &registration.build_version,
                            &started,
                            &deadline,
                            &recovery_window,
                        ],
                    )
                    .await
                    .map_err(|cause| database_error("register receipt owner", cause))?;
                Ok(())
            }
            .await;
            finish_transaction(&mut client, result, "receipt owner registration").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "register_owner",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn heartbeat_owner(&self, owner: CollectorInstanceId, lease_deadline: UnixMillis) -> ReceiptResult<()> {
        let owner = owner.as_uuid();
        let deadline = datetime(lease_deadline)?;
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            let row = client
                .query(
                    "UPDATE arrowcol.collector_instances \
                 SET heartbeat_at_utc = SYSUTCDATETIME(), lease_expires_at_utc = @P2 \
                 OUTPUT INSERTED.instance_id \
                 WHERE instance_id = @P1 AND state IN ('active', 'draining') \
                   AND stopped_at_utc IS NULL AND @P2 > SYSUTCDATETIME();",
                    &[&owner, &deadline],
                )
                .await
                .map_err(|cause| database_error("heartbeat receipt owner", cause))?
                .into_row()
                .await
                .map_err(|cause| database_error("read owner heartbeat result", cause))?;
            if row.is_none() {
                return Err(ReceiptError::new(
                    ReceiptErrorKind::Fenced,
                    "owner is not active or draining, or the requested deadline has expired",
                ));
            }
            Ok(())
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "heartbeat_owner",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn start_owner_drain(&self, owner: CollectorInstanceId) -> ReceiptResult<OwnerDrain> {
        let owner_uuid = owner.as_uuid();
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin owner drain", cause))?;
            let result = async {
                let row = client
                    .query(
                        "UPDATE arrowcol.collector_instances \
                     SET state = 'draining', heartbeat_at_utc = SYSUTCDATETIME() \
                     OUTPUT INSERTED.instance_id \
                     WHERE instance_id = @P1 AND stopped_at_utc IS NULL \
                       AND state IN ('active', 'draining');",
                        &[&owner_uuid],
                    )
                    .await
                    .map_err(|cause| database_error("start owner drain", cause))?
                    .into_row()
                    .await
                    .map_err(|cause| database_error("read owner drain result", cause))?;
                if row.is_none() {
                    return Err(ReceiptError::new(
                        ReceiptErrorKind::NotFound,
                        "active receipt owner was not found",
                    ));
                }
                owner_drain_counts(&mut client, owner).await
            }
            .await;
            finish_transaction(&mut client, result, "start owner drain").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "start_owner_drain",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn finish_owner_drain(&self, owner: CollectorInstanceId, stopped_at: UnixMillis) -> ReceiptResult<()> {
        let owner_uuid = owner.as_uuid();
        let stopped = datetime(stopped_at)?;
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin owner drain completion", cause))?;
            let result = async {
                let outstanding = owner_drain_counts(&mut client, owner).await?;
                if outstanding.active_receipts != 0 {
                    return Err(ReceiptError::new(
                        ReceiptErrorKind::Conflict,
                        format!(
                            "owner still has {} accepted, buffered, or flushing receipts",
                            outstanding.active_receipts
                        ),
                    ));
                }
                client
                    .execute(
                        "UPDATE arrowcol.table_leases \
                     SET released_at_utc = @P2, release_reason = 'instance_stopped' \
                     WHERE holder_instance_id = @P1 AND released_at_utc IS NULL;",
                        &[&owner_uuid, &stopped],
                    )
                    .await
                    .map_err(|cause| database_error("release drained owner table leases", cause))?;
                let row = client
                    .query(
                        "UPDATE arrowcol.collector_instances \
                     SET state = 'stopped', stopped_at_utc = @P2, heartbeat_at_utc = @P2, lease_expires_at_utc = @P2 \
                     OUTPUT INSERTED.instance_id \
                     WHERE instance_id = @P1 AND state = 'draining' AND stopped_at_utc IS NULL;",
                        &[&owner_uuid, &stopped],
                    )
                    .await
                    .map_err(|cause| database_error("finish owner drain", cause))?
                    .into_row()
                    .await
                    .map_err(|cause| database_error("read owner drain completion", cause))?;
                if row.is_none() {
                    return Err(ReceiptError::new(
                        ReceiptErrorKind::Conflict,
                        "owner was not in the draining state",
                    ));
                }
                Ok(())
            }
            .await;
            finish_transaction(&mut client, result, "finish owner drain").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "finish_owner_drain",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn acquire_table_lease(&self, request: TableLeaseRequest) -> ReceiptResult<TableLease> {
        if request.expires_at_unix_ms <= request.issued_at_unix_ms {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "table lease expiry must follow issuance",
            ));
        }
        let owner = request.owner_instance_id.as_uuid();
        let issued = datetime(request.issued_at_unix_ms)?;
        let expires = datetime(request.expires_at_unix_ms)?;
        let lease_id = LeaseId::new();
        let lease_uuid = lease_id.as_uuid();
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin table lease acquisition", cause))?;
            let result = async {
                validate_catalog_table(&mut client, request.table_id, &request.table).await?;
                validate_active_owner(&mut client, request.owner_instance_id, request.issued_at_unix_ms).await?;
                client
                    .execute(
                        "UPDATE arrowcol.table_leases \
                     SET released_at_utc = @P2, release_reason = 'expired' \
                     WHERE table_id = @P1 AND released_at_utc IS NULL AND expires_at_utc <= @P2;",
                        &[&request.table_id, &issued],
                    )
                    .await
                    .map_err(|cause| database_error("expire predecessor table lease", cause))?;
                let active = client
                    .query(
                        "SELECT lease_id FROM arrowcol.table_leases WITH (UPDLOCK, HOLDLOCK) \
                     WHERE table_id = @P1 AND released_at_utc IS NULL;",
                        &[&request.table_id],
                    )
                    .await
                    .map_err(|cause| database_error("check active table lease", cause))?
                    .into_row()
                    .await
                    .map_err(|cause| database_error("read active table lease", cause))?;
                if active.is_some() {
                    return Err(ReceiptError::new(
                        ReceiptErrorKind::Fenced,
                        "another active table lease owns the table",
                    ));
                }
                let epoch = next_fencing_epoch(&mut client, request.table_id).await?;
                let epoch_i64 = i64::try_from(epoch.get()).map_err(|_| {
                    ReceiptError::new(ReceiptErrorKind::CorruptState, "fencing epoch exceeds SQL bigint")
                })?;
                client
                    .execute(
                        "INSERT INTO arrowcol.table_leases ( \
                        table_id, fencing_epoch, lease_id, holder_instance_id, \
                        acquired_at_utc, renewed_at_utc, expires_at_utc \
                     ) VALUES (@P1, @P2, @P3, @P4, @P5, @P5, @P6);",
                        &[&request.table_id, &epoch_i64, &lease_uuid, &owner, &issued, &expires],
                    )
                    .await
                    .map_err(|cause| database_error("insert table lease", cause))?;
                TableLease::new(
                    lease_id,
                    request.table,
                    request.owner_instance_id,
                    request.implementation_pool,
                    epoch,
                    request.issued_at_unix_ms,
                    request.expires_at_unix_ms,
                )
                .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))
            }
            .await;
            finish_transaction(&mut client, result, "acquire table lease").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "acquire_table_lease",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn renew_table_lease(&self, lease: &TableLease, expires_at: UnixMillis) -> ReceiptResult<TableLease> {
        if expires_at <= lease.expires_at_unix_ms() {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "table lease renewal must extend expiry",
            ));
        }
        let owner = lease.holder_instance_id().as_uuid();
        let lease_uuid = lease.lease_id().as_uuid();
        let epoch = i64::try_from(lease.fencing_token().get()).map_err(|_| {
            ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "fencing epoch exceeds SQL bigint",
            )
        })?;
        let expires = datetime(expires_at)?;
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            let row = client
                .query(
                    "UPDATE l \
                 SET renewed_at_utc = SYSUTCDATETIME(), expires_at_utc = @P4 \
                 OUTPUT INSERTED.renewed_at_utc \
                 FROM arrowcol.table_leases AS l \
                 INNER JOIN arrowcol.collector_instances AS i ON i.instance_id = l.holder_instance_id \
                 WHERE l.lease_id = @P1 AND l.holder_instance_id = @P2 AND l.fencing_epoch = @P3 \
                   AND l.released_at_utc IS NULL AND l.expires_at_utc > SYSUTCDATETIME() \
                   AND i.state IN ('active', 'draining') AND i.lease_expires_at_utc > SYSUTCDATETIME() \
                   AND @P4 > l.expires_at_utc;",
                    &[&lease_uuid, &owner, &epoch, &expires],
                )
                .await
                .map_err(|cause| database_error("renew table lease", cause))?
                .into_row()
                .await
                .map_err(|cause| database_error("read table lease renewal", cause))?;
            let row = row.ok_or_else(|| {
                ReceiptError::new(ReceiptErrorKind::Fenced, "table lease or owner expired before renewal")
            })?;
            let issued = unix_millis(required_datetime(&row, "renewed_at_utc")?)?;
            TableLease::new(
                lease.lease_id(),
                lease.table().clone(),
                lease.holder_instance_id(),
                lease.implementation_pool(),
                lease.fencing_token(),
                issued,
                expires_at,
            )
            .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "renew_table_lease",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn release_table_lease(&self, lease: &TableLease, released_at: UnixMillis) -> ReceiptResult<()> {
        let lease_uuid = lease.lease_id().as_uuid();
        let owner = lease.holder_instance_id().as_uuid();
        let released = datetime(released_at)?;
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            let row = client
                .query(
                    "UPDATE arrowcol.table_leases \
                 SET released_at_utc = @P3, release_reason = 'released' \
                 OUTPUT INSERTED.lease_id \
                 WHERE lease_id = @P1 AND holder_instance_id = @P2 AND released_at_utc IS NULL;",
                    &[&lease_uuid, &owner, &released],
                )
                .await
                .map_err(|cause| database_error("release table lease", cause))?
                .into_row()
                .await
                .map_err(|cause| database_error("read table lease release", cause))?;
            if row.is_none() {
                return Err(ReceiptError::new(
                    ReceiptErrorKind::Fenced,
                    "table lease was already released or ownership changed",
                ));
            }
            Ok(())
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "release_table_lease",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn create_flight_batch(
        &self,
        owner: CollectorInstanceId,
        lease: &TableLease,
        lease_deadline: UnixMillis,
        deliveries: &[ValidatedDelivery],
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.mutation_bound(deliveries.len())?;
        if deliveries.is_empty() {
            return Ok(Vec::new());
        }
        if lease.holder_instance_id() != owner || deliveries.iter().any(|delivery| delivery.table() != lease.table()) {
            return Err(ReceiptError::new(
                ReceiptErrorKind::Fenced,
                "validated deliveries do not belong to the supplied owner/table lease",
            ));
        }
        if lease_deadline > lease.expires_at_unix_ms() {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "receipt lease cannot outlive its table lease",
            ));
        }
        let deadline = datetime(lease_deadline)?;
        let owner_uuid = owner.as_uuid();
        let lease_uuid = lease.lease_id().as_uuid();
        let epoch = i64::try_from(lease.fencing_token().get()).map_err(|_| {
            ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "fencing epoch exceeds SQL bigint",
            )
        })?;
        let pool = lease.implementation_pool().to_string();
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
        client
            .begin_transaction_with_isolation(IsolationLevel::Serializable)
            .await
            .map_err(|cause| database_error("begin receipt creation", cause))?;
        let result = async {
            let mut ids = Vec::with_capacity(deliveries.len());
            for delivery in deliveries {
                let schema_version = i64::try_from(delivery.schema_version().get()).map_err(|_| {
                    ReceiptError::new(ReceiptErrorKind::InvalidConfiguration, "schema version exceeds SQL bigint")
                })?;
                let rows = measurement_i64(delivery.rows(), "row count")?;
                let batches = measurement_i64(delivery.batches(), "batch count")?;
                let bytes = measurement_i64(delivery.arrow_memory_bytes(), "Arrow memory bytes")?;
                let fingerprint = delivery.schema_fingerprint();
                let digest: &[u8] = fingerprint.as_bytes();
                let table_id = delivery.table_id();
                let row = client
                    .query(
                        "DECLARE @receipt_now datetime2(3) = SYSUTCDATETIME(); \
                         INSERT INTO arrowcol.delivery_receipts ( \
                            table_id, owner_instance_id, implementation_pool, schema_version, schema_sha256, \
                            lease_id, fencing_epoch, state, revision, row_count, batch_count, arrow_memory_bytes, \
                            receipt_lease_deadline_utc, heartbeat_at_utc, created_at_utc, updated_at_utc \
                         ) \
                         OUTPUT INSERTED.receipt_id \
                         SELECT @P1, @P2, @P3, @P4, @P5, @P6, @P7, 'accepted', 0, @P8, @P9, @P10, @P11, \
                                @receipt_now, @receipt_now, @receipt_now \
                         FROM arrowcol.table_schema_versions AS v \
                         INNER JOIN arrowcol.table_leases AS l \
                           ON l.table_id = v.table_id AND l.fencing_epoch = @P7 AND l.lease_id = @P6 \
                         INNER JOIN arrowcol.collector_instances AS i ON i.instance_id = @P2 \
                         WHERE v.table_id = @P1 AND v.schema_version = @P4 AND v.schema_sha256 = @P5 \
                           AND v.state IN ('active', 'superseded') \
                           AND l.holder_instance_id = @P2 AND l.released_at_utc IS NULL \
                           AND l.expires_at_utc > SYSUTCDATETIME() AND @P11 >= @receipt_now AND @P11 <= l.expires_at_utc \
                           AND i.state = 'active' AND i.stopped_at_utc IS NULL \
                           AND i.lease_expires_at_utc > SYSUTCDATETIME() AND @P11 <= i.lease_expires_at_utc;",
                        &[
                            &table_id,
                            &owner_uuid,
                            &pool,
                            &schema_version,
                            &digest,
                            &lease_uuid,
                            &epoch,
                            &rows,
                            &batches,
                            &bytes,
                            &deadline,
                        ],
                    )
                    .await
                    .map_err(|cause| database_error("insert delivery receipt", cause))?
                    .into_row()
                    .await
                    .map_err(|cause| database_error("read created receipt id", cause))?;
                let row = row.ok_or_else(|| {
                    ReceiptError::new(
                        ReceiptErrorKind::Fenced,
                        "owner, table lease, or validated schema is no longer active",
                    )
                })?;
                let receipt_uuid = required_uuid(&row, "receipt_id")?;
                insert_event(
                    &mut client,
                    receipt_uuid,
                    0,
                    None,
                    "accepted",
                    owner_uuid,
                    None,
                    None,
                )
                .await?;
                ids.push(receipt_id(receipt_uuid)?);
            }
            receipts_with_client(&mut client, &ids).await
        }
        .await;
        finish_transaction(&mut client, result, "create receipt batch").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "create_flight_batch",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn mark_buffered(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.mutation_bound(receipts.len())?;
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            transition_batch(
                &mut client,
                owner,
                receipts,
                "accepted",
                "buffered",
                Some(lease_deadline),
            )
            .await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "mark_buffered",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn mark_flushing(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.mutation_bound(receipts.len())?;
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            transition_batch(
                &mut client,
                owner,
                receipts,
                "buffered",
                "flushing",
                Some(lease_deadline),
            )
            .await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "mark_flushing",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn heartbeat_receipts(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        lease_deadline: UnixMillis,
    ) -> ReceiptResult<()> {
        self.mutation_bound(receipts.len())?;
        if receipts.is_empty() {
            return Ok(());
        }
        let owner_uuid = owner.as_uuid();
        let deadline = datetime(lease_deadline)?;
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin receipt heartbeat", cause))?;
            let result = async {
                for receipt in receipts {
                    let id = receipt.receipt_id.as_uuid();
                    let revision = revision_i64(receipt.revision)?;
                    let row = client
                        .query(
                            "UPDATE r \
                         SET heartbeat_at_utc = SYSUTCDATETIME(), receipt_lease_deadline_utc = @P4 \
                         OUTPUT INSERTED.receipt_id \
                         FROM arrowcol.delivery_receipts AS r \
                         INNER JOIN arrowcol.table_leases AS l \
                           ON l.table_id = r.table_id AND l.fencing_epoch = r.fencing_epoch \
                          AND l.lease_id = r.lease_id AND l.holder_instance_id = r.owner_instance_id \
                         INNER JOIN arrowcol.collector_instances AS i ON i.instance_id = r.owner_instance_id \
                         WHERE r.receipt_id = @P1 AND r.owner_instance_id = @P2 AND r.revision = @P3 \
                           AND r.state IN ('accepted', 'buffered', 'flushing') \
                           AND l.released_at_utc IS NULL AND l.expires_at_utc > SYSUTCDATETIME() \
                           AND @P4 <= l.expires_at_utc \
                           AND i.state IN ('active', 'draining') AND i.lease_expires_at_utc > SYSUTCDATETIME() \
                           AND @P4 <= i.lease_expires_at_utc;",
                            &[&id, &owner_uuid, &revision, &deadline],
                        )
                        .await
                        .map_err(|cause| database_error("heartbeat receipt lease", cause))?
                        .into_row()
                        .await
                        .map_err(|cause| database_error("read receipt heartbeat result", cause))?;
                    if row.is_none() {
                        return Err(transition_conflict(receipt.receipt_id));
                    }
                }
                Ok(())
            }
            .await;
            finish_transaction(&mut client, result, "heartbeat receipts").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "heartbeat_receipts",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn publish_commit(
        &self,
        owner: CollectorInstanceId,
        batch: &PublicationBatch,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.mutation_bound(batch.receipts.len())?;
        if batch.receipts.is_empty() {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "a Delta publication must complete at least one receipt",
            ));
        }
        let fence = batch.publication.fence();
        if fence.holder_instance_id() != owner {
            return Err(ReceiptError::new(
                ReceiptErrorKind::Fenced,
                "publication fence owner does not match receipt owner",
            ));
        }
        let owner_uuid = owner.as_uuid();
        let lease_uuid = fence.lease_id().as_uuid();
        let epoch = i64::try_from(fence.fencing_token().get()).map_err(|_| {
            ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "fencing epoch exceeds SQL bigint",
            )
        })?;
        let delta_version = measurement_i64(batch.publication.delta_version(), "Delta version")?;
        let data_files = measurement_i64(batch.publication.data_files_added(), "data file count")?;
        let rows = measurement_i64(batch.publication.rows_published(), "published rows")?;
        let bytes = measurement_i64(batch.publication.bytes_published(), "published bytes")?;
        let published = datetime(batch.publication.published_at_unix_ms())?;
        let table = fence.table();
        let catalog = table.catalog().to_string();
        let schema = table.schema().to_string();
        let table_name = table.table().to_string();
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin receipt publication", cause))?;
            let result = async {
                for receipt in &batch.receipts {
                    let id = receipt.receipt_id.as_uuid();
                    let revision = revision_i64(receipt.revision)?;
                    let row = client
                        .query(
                            "UPDATE r \
                         SET state = 'published', revision = revision + 1, updated_at_utc = @P8, \
                             heartbeat_at_utc = @P8, delta_version = @P9, data_files_added = @P10, \
                             rows_published = @P11, bytes_published = @P12, published_at_utc = @P8 \
                         OUTPUT INSERTED.receipt_id \
                         FROM arrowcol.delivery_receipts AS r \
                         INNER JOIN arrowcol.catalog_tables AS t ON t.table_id = r.table_id \
                         INNER JOIN arrowcol.table_leases AS l \
                           ON l.table_id = r.table_id AND l.fencing_epoch = r.fencing_epoch \
                          AND l.lease_id = r.lease_id AND l.holder_instance_id = r.owner_instance_id \
                         WHERE r.receipt_id = @P1 AND r.owner_instance_id = @P2 AND r.revision = @P3 \
                           AND r.state = 'flushing' AND r.lease_id = @P4 AND r.fencing_epoch = @P5 \
                           AND t.catalog_name = @P6 AND t.schema_name = @P7 AND t.table_name = @P13 \
                           AND l.released_at_utc IS NULL AND l.expires_at_utc > @P8;",
                            &[
                                &id,
                                &owner_uuid,
                                &revision,
                                &lease_uuid,
                                &epoch,
                                &catalog,
                                &schema,
                                &published,
                                &delta_version,
                                &data_files,
                                &rows,
                                &bytes,
                                &table_name,
                            ],
                        )
                        .await
                        .map_err(|cause| database_error("publish delivery receipt", cause))?
                        .into_row()
                        .await
                        .map_err(|cause| database_error("read published receipt", cause))?;
                    if row.is_none() {
                        return Err(transition_conflict(receipt.receipt_id));
                    }
                    insert_event(
                        &mut client,
                        id,
                        revision + 1,
                        Some("flushing"),
                        "published",
                        owner_uuid,
                        None,
                        Some("durable Delta commit observed"),
                    )
                    .await?;
                }
                let ids: Vec<_> = batch.receipts.iter().map(|receipt| receipt.receipt_id).collect();
                receipts_with_client(&mut client, &ids).await
            }
            .await;
            finish_transaction(&mut client, result, "publish receipt batch").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "publish_commit",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn fail_batch(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        failure: &ReceiptFailure,
        failed_at: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.mutation_bound(receipts.len())?;
        if receipts.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(associated) = failure.error().receipt_id()
            && (receipts.len() != 1 || receipts[0].receipt_id != associated)
        {
            return Err(ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "a receipt-associated error cannot be applied to a different batch",
            ));
        }
        let target = match failure.disposition() {
            FailureDisposition::Retryable => "retryable_failure",
            FailureDisposition::Permanent => "permanent_failure",
        };
        let disposition = match failure.disposition() {
            FailureDisposition::Retryable => "retryable",
            FailureDisposition::Permanent => "permanent",
        };
        let error_code = error_code_name(failure.error().code())?;
        let error_message = failure.error().message();
        let retry_after = failure
            .error()
            .retry_after_ms()
            .map(|delay| i64::try_from(delay.get()))
            .transpose()
            .map_err(|_| ReceiptError::new(ReceiptErrorKind::InvalidConfiguration, "retry delay exceeds SQL bigint"))?;
        let failed = datetime(failed_at)?;
        let owner_uuid = owner.as_uuid();
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin receipt failure", cause))?;
            let result = async {
                for receipt in receipts {
                    let id = receipt.receipt_id.as_uuid();
                    let revision = revision_i64(receipt.revision)?;
                    let row = client
                        .query(
                            "UPDATE arrowcol.delivery_receipts \
                         SET state = @P4, revision = revision + 1, updated_at_utc = @P5, heartbeat_at_utc = @P5, \
                             failure_disposition = @P6, error_code = @P7, error_message = @P8, retry_after_ms = @P9 \
                         OUTPUT INSERTED.receipt_id \
                         WHERE receipt_id = @P1 AND owner_instance_id = @P2 AND revision = @P3 \
                           AND state IN ('accepted', 'buffered', 'flushing');",
                            &[
                                &id,
                                &owner_uuid,
                                &revision,
                                &target,
                                &failed,
                                &disposition,
                                &error_code,
                                &error_message,
                                &retry_after,
                            ],
                        )
                        .await
                        .map_err(|cause| database_error("fail delivery receipt", cause))?
                        .into_row()
                        .await
                        .map_err(|cause| database_error("read failed receipt", cause))?;
                    if row.is_none() {
                        return Err(transition_conflict(receipt.receipt_id));
                    }
                    let from = receipt_state_for_revision(receipt.revision)?;
                    insert_event(
                        &mut client,
                        id,
                        revision + 1,
                        Some(from),
                        target,
                        owner_uuid,
                        Some(disposition),
                        Some(error_message),
                    )
                    .await?;
                }
                let ids: Vec<_> = receipts.iter().map(|receipt| receipt.receipt_id).collect();
                receipts_with_client(&mut client, &ids).await
            }
            .await;
            finish_transaction(&mut client, result, "fail receipt batch").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "fail_batch",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn receipt(&self, receipt_id: ReceiptId) -> ReceiptResult<Option<ReceiptRecord>> {
        let mut statuses = self.receipts(&[receipt_id]).await?;
        statuses.pop().ok_or_else(|| {
            ReceiptError::new(
                ReceiptErrorKind::CorruptState,
                "single receipt query returned no result slot",
            )
        })
    }

    async fn receipts(&self, receipt_ids: &[ReceiptId]) -> ReceiptResult<Vec<Option<ReceiptRecord>>> {
        if receipt_ids.len() > self.config.max_status_batch_size {
            return Err(ReceiptError::new(
                ReceiptErrorKind::RequestTooLarge,
                format!(
                    "receipt status batch contains {} entries; maximum is {}",
                    receipt_ids.len(),
                    self.config.max_status_batch_size
                ),
            ));
        }
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async { receipts_optional_with_client(&mut client, receipt_ids).await }.await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "receipts",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn reconcile_expired(&self, now: UnixMillis, limit: usize) -> ReceiptResult<ReconciliationReport> {
        if limit == 0 || limit > self.config.max_batch_size {
            return Err(ReceiptError::new(
                ReceiptErrorKind::RequestTooLarge,
                format!("reconciliation limit must be in 1..={}", self.config.max_batch_size),
            ));
        }
        let now = datetime(now)?;
        let limit_i32 = i32::try_from(limit).map_err(|_| {
            ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "reconciliation limit exceeds SQL int",
            )
        })?;
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::ReadCommitted)
                .await
                .map_err(|cause| database_error("begin receipt reconciliation", cause))?;
            let result = async {
                let rows = client
                    .query(
                        format!(
                            "SELECT TOP (@P1) r.receipt_id, r.owner_instance_id, r.revision, r.state \
                     FROM arrowcol.delivery_receipts AS r WITH ( \
                         INDEX(IX_delivery_receipts_expiry_active), UPDLOCK, READPAST, ROWLOCK \
                     ) \
                     WHERE r.receipt_lease_deadline_utc <= @P2 \
                       AND r.state IN ('accepted', 'buffered', 'flushing') \
                       AND {} \
                     ORDER BY r.receipt_lease_deadline_utc, r.receipt_id;",
                            outside_durable_recovery_window("r", "@P2")
                        ),
                        &[&limit_i32, &now],
                    )
                    .await
                    .map_err(|cause| database_error("select expired receipts", cause))?
                    .into_first_result()
                    .await
                    .map_err(|cause| database_error("read expired receipts", cause))?;
                let mut ids = Vec::with_capacity(rows.len());
                let reconcile = format!(
                    "UPDATE r \
                     SET state = @P6, revision = r.revision + 1, updated_at_utc = @P4, \
                         heartbeat_at_utc = @P4, failure_disposition = @P7, \
                         error_code = @P8, error_message = @P9, \
                         retry_after_ms = NULL \
                     OUTPUT INSERTED.receipt_id \
                     FROM arrowcol.delivery_receipts AS r \
                     WHERE r.receipt_id = @P1 AND r.owner_instance_id = @P2 AND r.revision = @P3 \
                       AND r.state = @P5 AND r.receipt_lease_deadline_utc <= @P4 \
                       AND {};",
                    outside_durable_recovery_window("r", "@P4")
                );
                for row in rows {
                    let id = required_uuid(&row, "receipt_id")?;
                    let owner = required_uuid(&row, "owner_instance_id")?;
                    let revision = required_i64(&row, "revision")?;
                    let state = required_str(&row, "state")?.to_owned();
                    let (target, disposition, error_code, error_message, event_message) = if state == "flushing" {
                        (
                            "permanent_failure",
                            "permanent",
                            "publication_outcome_unknown",
                            "receipt ownership expired while Delta publication outcome was uncertain",
                            "expired flushing work requires explicit Delta reconciliation",
                        )
                    } else {
                        (
                            "retryable_failure",
                            "retryable",
                            "publication_failed",
                            "receipt ownership expired before recoverable publication completed",
                            "expired recoverable in-process work",
                        )
                    };
                    let updated = client
                        .query(
                            reconcile.as_str(),
                            &[
                                &id,
                                &owner,
                                &revision,
                                &now,
                                &state,
                                &target,
                                &disposition,
                                &error_code,
                                &error_message,
                            ],
                        )
                        .await
                        .map_err(|cause| database_error("reconcile expired receipt", cause))?
                        .into_row()
                        .await
                        .map_err(|cause| database_error("read reconciled receipt", cause))?;
                    if updated.is_some() {
                        insert_event(
                            &mut client,
                            id,
                            revision + 1,
                            Some(&state),
                            target,
                            owner,
                            Some(disposition),
                            Some(event_message),
                        )
                        .await?;
                        ids.push(receipt_id(id)?);
                    }
                }
                let reconciled = receipts_with_client(&mut client, &ids).await?;
                Ok(ReconciliationReport {
                    limit_reached: reconciled.len() == limit,
                    reconciled,
                })
            }
            .await;
            finish_transaction(&mut client, result, "reconcile expired receipts").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "reconcile_expired",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    #[allow(clippy::too_many_lines)]
    async fn adopt_receipts(&self, request: &ReceiptAdoption) -> ReceiptResult<ReceiptAdoptionReport> {
        self.mutation_bound(request.receipts.len())?;
        request.validate()?;
        if request.receipts.is_empty() {
            return Ok(ReceiptAdoptionReport::default());
        }
        let successor = request.successor.as_uuid();
        let lease_uuid = request.lease.lease_id().as_uuid();
        let epoch = i64::try_from(request.lease.fencing_token().get()).map_err(|_| {
            ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "fencing epoch exceeds SQL bigint",
            )
        })?;
        let deadline = datetime(request.lease_deadline)?;
        let adopted_at = datetime(request.adopted_at)?;
        let pool = request.lease.implementation_pool().to_string();
        let table = request.lease.table();
        let catalog = table.catalog().to_string();
        let schema = table.schema().to_string();
        let table_name = table.table().to_string();
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::Serializable)
                .await
                .map_err(|cause| database_error("begin receipt adoption", cause))?;
            let result = async {
                let authority = client
                    .query(
                        "SELECT l.lease_id \
                     FROM arrowcol.table_leases AS l WITH (UPDLOCK, HOLDLOCK) \
                     INNER JOIN arrowcol.collector_instances AS i WITH (UPDLOCK, HOLDLOCK) \
                       ON i.instance_id = l.holder_instance_id \
                     INNER JOIN arrowcol.catalog_tables AS t ON t.table_id = l.table_id \
                     WHERE l.lease_id = @P1 AND l.holder_instance_id = @P2 AND l.fencing_epoch = @P3 \
                       AND l.released_at_utc IS NULL AND l.expires_at_utc > SYSUTCDATETIME() \
                       AND @P4 <= l.expires_at_utc \
                       AND i.state = 'active' AND i.stopped_at_utc IS NULL \
                       AND i.lease_expires_at_utc > SYSUTCDATETIME() AND @P4 <= i.lease_expires_at_utc \
                       AND t.catalog_name = @P5 AND t.schema_name = @P6 AND t.table_name = @P7;",
                        &[
                            &lease_uuid,
                            &successor,
                            &epoch,
                            &deadline,
                            &catalog,
                            &schema,
                            &table_name,
                        ],
                    )
                    .await
                    .map_err(|cause| database_error("lock receipt adoption lease", cause))?
                    .into_row()
                    .await
                    .map_err(|cause| database_error("read receipt adoption lease", cause))?;
                if authority.is_none() {
                    return Err(ReceiptError::new(
                        ReceiptErrorKind::Fenced,
                        "successor owner or table lease is not active for the adoption deadline",
                    ));
                }
                let mut adopted_ids = Vec::with_capacity(request.receipts.len());
                let mut rejected_ids = Vec::new();
                for candidate in &request.receipts {
                    let id = candidate.receipt_id.as_uuid();
                    let expected_owner = candidate.expected_owner.as_uuid();
                    let state = active_state_name(candidate.expected_state)?;
                    let revision = revision_i64(candidate.expected_revision)?;
                    let row = client
                        .query(
                            "UPDATE r \
                         SET owner_instance_id = @P2, lease_id = @P6, fencing_epoch = @P7, \
                             receipt_lease_deadline_utc = @P8, heartbeat_at_utc = @P9, updated_at_utc = @P9 \
                         OUTPUT DELETED.fencing_epoch AS previous_fencing_epoch \
                         FROM arrowcol.delivery_receipts AS r \
                         INNER JOIN arrowcol.collector_instances AS predecessor \
                           ON predecessor.instance_id = r.owner_instance_id \
                         INNER JOIN arrowcol.collector_instances AS successor ON successor.instance_id = @P2 \
                         INNER JOIN arrowcol.table_leases AS l \
                           ON l.table_id = r.table_id AND l.fencing_epoch = @P7 \
                          AND l.lease_id = @P6 AND l.holder_instance_id = @P2 \
                         WHERE r.receipt_id = @P1 AND r.owner_instance_id = @P3 \
                           AND r.state = @P4 AND r.revision = @P5 \
                           AND r.state IN ('accepted', 'buffered', 'flushing') \
                           AND r.implementation_pool = @P10 AND r.created_at_utc <= @P9 \
                           AND successor.state = 'active' AND successor.stopped_at_utc IS NULL \
                           AND successor.lease_expires_at_utc > SYSUTCDATETIME() \
                           AND @P8 <= successor.lease_expires_at_utc \
                           AND l.released_at_utc IS NULL AND l.expires_at_utc > SYSUTCDATETIME() \
                           AND @P8 <= l.expires_at_utc \
                           AND ( \
                               r.owner_instance_id = @P2 \
                               OR ( \
                                   predecessor.instance_key = successor.instance_key \
                                   AND predecessor.generation < successor.generation \
                                   AND (predecessor.state = 'failed' OR predecessor.stopped_at_utc IS NOT NULL) \
                               ) \
                           );",
                            &[
                                &id,
                                &successor,
                                &expected_owner,
                                &state,
                                &revision,
                                &lease_uuid,
                                &epoch,
                                &deadline,
                                &adopted_at,
                                &pool,
                            ],
                        )
                        .await
                        .map_err(|cause| database_error("adopt delivery receipt", cause))?
                        .into_row()
                        .await
                        .map_err(|cause| database_error("read adopted receipt", cause))?;
                    let Some(row) = row else {
                        rejected_ids.push(candidate.receipt_id);
                        continue;
                    };
                    let previous_epoch = required_i64(&row, "previous_fencing_epoch")?;
                    let detail = if expected_owner == successor {
                        format!("owner rebound receipt from fencing epoch {previous_epoch} to {epoch}")
                    } else {
                        format!(
                            "successor generation adopted receipt from a failed or stopped predecessor; \
                             fencing epoch {previous_epoch} to {epoch}"
                        )
                    };
                    insert_adoption_event(&mut client, id, revision, state, successor, expected_owner, &detail).await?;
                    adopted_ids.push(candidate.receipt_id);
                }
                let adopted = receipts_with_client(&mut client, &adopted_ids).await?;
                let current = receipts_optional_with_client(&mut client, &rejected_ids).await?;
                let rejected = rejected_ids
                    .into_iter()
                    .zip(current)
                    .map(|(receipt_id, current)| RejectedAdoption { receipt_id, current })
                    .collect();
                Ok(ReceiptAdoptionReport { adopted, rejected })
            }
            .await;
            finish_transaction(&mut client, result, "adopt receipt batch").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "adopt_receipts",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }

    async fn fail_unbuffered_predecessor_receipts(
        &self,
        successor: CollectorInstanceId,
        failed_at: UnixMillis,
        limit: usize,
    ) -> ReceiptResult<ReconciliationReport> {
        if limit == 0 || limit > self.config.max_batch_size {
            return Err(ReceiptError::new(
                ReceiptErrorKind::RequestTooLarge,
                format!("unbuffered receipt limit must be in 1..={}", self.config.max_batch_size),
            ));
        }
        let limit_i32 = i32::try_from(limit).map_err(|_| {
            ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "unbuffered receipt limit exceeds SQL int",
            )
        })?;
        let failed = datetime(failed_at)?;
        let successor = successor.as_uuid();
        let mut client = self.connect().await?;
        let result: ReceiptResult<_> = async {
            client
                .begin_transaction_with_isolation(IsolationLevel::ReadCommitted)
                .await
                .map_err(|cause| database_error("begin unbuffered receipt failure", cause))?;
            let result = async {
                let rows = client
                    .query(
                        "SELECT TOP (@P1) r.receipt_id, r.owner_instance_id, r.revision \
                     FROM arrowcol.delivery_receipts AS r WITH (UPDLOCK, READPAST, ROWLOCK) \
                     INNER JOIN arrowcol.collector_instances AS predecessor \
                       ON predecessor.instance_id = r.owner_instance_id \
                     INNER JOIN arrowcol.collector_instances AS successor ON successor.instance_id = @P2 \
                     WHERE r.state = 'accepted' \
                       AND successor.state IN ('active', 'draining') AND successor.stopped_at_utc IS NULL \
                       AND predecessor.instance_key = successor.instance_key \
                       AND predecessor.generation < successor.generation \
                       AND (predecessor.state = 'failed' OR predecessor.stopped_at_utc IS NOT NULL) \
                     ORDER BY r.receipt_id;",
                        &[&limit_i32, &successor],
                    )
                    .await
                    .map_err(|cause| database_error("select unbuffered predecessor receipts", cause))?
                    .into_first_result()
                    .await
                    .map_err(|cause| database_error("read unbuffered predecessor receipts", cause))?;
                let mut ids = Vec::with_capacity(rows.len());
                for row in rows {
                    let id = required_uuid(&row, "receipt_id")?;
                    let owner = required_uuid(&row, "owner_instance_id")?;
                    let revision = required_i64(&row, "revision")?;
                    let updated = client
                        .query(
                            "UPDATE arrowcol.delivery_receipts \
                         SET state = 'retryable_failure', revision = revision + 1, updated_at_utc = @P4, \
                             heartbeat_at_utc = @P4, failure_disposition = 'retryable', \
                             error_code = 'publication_failed', \
                             error_message = 'collector stopped before the batch was durably buffered', \
                             retry_after_ms = NULL \
                         OUTPUT INSERTED.receipt_id \
                         WHERE receipt_id = @P1 AND owner_instance_id = @P2 AND revision = @P3 \
                           AND state = 'accepted';",
                            &[&id, &owner, &revision, &failed],
                        )
                        .await
                        .map_err(|cause| database_error("fail unbuffered predecessor receipt", cause))?
                        .into_row()
                        .await
                        .map_err(|cause| database_error("read failed unbuffered receipt", cause))?;
                    if updated.is_some() {
                        insert_event(
                            &mut client,
                            id,
                            revision + 1,
                            Some("accepted"),
                            "retryable_failure",
                            successor,
                            Some("retryable"),
                            Some("predecessor stopped before durably buffering the batch; successor drained every persisted bundle"),
                        )
                        .await?;
                        ids.push(receipt_id(id)?);
                    }
                }
                let reconciled = receipts_with_client(&mut client, &ids).await?;
                Ok(ReconciliationReport {
                    limit_reached: reconciled.len() == limit,
                    reconciled,
                })
            }
            .await;
            finish_transaction(&mut client, result, "fail unbuffered predecessor receipts").await
        }
        .await;
        let value = result?;
        if let Err(error) = client.finish().await {
            tracing::warn!(
                operation = "fail_unbuffered_predecessor_receipts",
                error = %error,
                "SQL Server session cleanup failed after confirmed success; discarding connection"
            );
        }
        Ok(value)
    }
}

async fn transition_batch(
    client: &mut SqlClient,
    owner: CollectorInstanceId,
    receipts: &[ReceiptVersion],
    from: &'static str,
    to: &'static str,
    lease_deadline: Option<UnixMillis>,
) -> ReceiptResult<Vec<ReceiptRecord>> {
    if receipts.is_empty() {
        return Ok(Vec::new());
    }
    let deadline = lease_deadline.map(datetime).transpose()?;
    let owner_uuid = owner.as_uuid();
    client
        .begin_transaction_with_isolation(IsolationLevel::Serializable)
        .await
        .map_err(|cause| database_error("begin receipt transition", cause))?;
    let result = async {
        for receipt in receipts {
            let id = receipt.receipt_id.as_uuid();
            let revision = revision_i64(receipt.revision)?;
            let row = client
                .query(
                    "UPDATE r \
                     SET state = @P5, revision = revision + 1, updated_at_utc = SYSUTCDATETIME(), \
                         heartbeat_at_utc = SYSUTCDATETIME(), \
                         receipt_lease_deadline_utc = COALESCE(@P6, r.receipt_lease_deadline_utc) \
                     OUTPUT INSERTED.receipt_id \
                     FROM arrowcol.delivery_receipts AS r \
                     INNER JOIN arrowcol.table_leases AS l \
                       ON l.table_id = r.table_id AND l.fencing_epoch = r.fencing_epoch \
                      AND l.lease_id = r.lease_id AND l.holder_instance_id = r.owner_instance_id \
                     INNER JOIN arrowcol.collector_instances AS i ON i.instance_id = r.owner_instance_id \
                     WHERE r.receipt_id = @P1 AND r.owner_instance_id = @P2 AND r.revision = @P3 \
                       AND r.state = @P4 AND l.released_at_utc IS NULL \
                       AND l.expires_at_utc > SYSUTCDATETIME() \
                       AND (@P6 IS NULL OR @P6 <= l.expires_at_utc) \
                       AND i.state IN ('active', 'draining') AND i.lease_expires_at_utc > SYSUTCDATETIME() \
                       AND (@P6 IS NULL OR @P6 <= i.lease_expires_at_utc);",
                    &[&id, &owner_uuid, &revision, &from, &to, &deadline],
                )
                .await
                .map_err(|cause| database_error("transition delivery receipt", cause))?
                .into_row()
                .await
                .map_err(|cause| database_error("read transitioned receipt", cause))?;
            if row.is_none() {
                return Err(transition_conflict(receipt.receipt_id));
            }
            insert_event(client, id, revision + 1, Some(from), to, owner_uuid, None, None).await?;
        }
        let ids: Vec<_> = receipts.iter().map(|receipt| receipt.receipt_id).collect();
        receipts_with_client(client, &ids).await
    }
    .await;
    finish_transaction(client, result, "transition receipt batch").await
}

#[allow(clippy::too_many_arguments)]
async fn insert_event(
    client: &mut SqlClient,
    receipt_id: Uuid,
    revision: i64,
    from: Option<&str>,
    to: &str,
    actor: Uuid,
    failure_disposition: Option<&str>,
    detail: Option<&str>,
) -> ReceiptResult<()> {
    client
        .execute(
            "INSERT INTO arrowcol.receipt_events ( \
                receipt_id, revision, from_state, to_state, actor_instance_id, failure_disposition, event_detail \
             ) VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7);",
            &[
                &receipt_id,
                &revision,
                &from,
                &to,
                &actor,
                &failure_disposition,
                &detail,
            ],
        )
        .await
        .map_err(|cause| database_error("append receipt event", cause))?;
    Ok(())
}

async fn insert_adoption_event(
    client: &mut SqlClient,
    receipt_id: Uuid,
    revision: i64,
    state: &str,
    successor: Uuid,
    previous_owner: Uuid,
    detail: &str,
) -> ReceiptResult<()> {
    client
        .execute(
            "INSERT INTO arrowcol.receipt_events ( \
                receipt_id, revision, from_state, to_state, actor_instance_id, previous_owner_instance_id, \
                event_detail \
             ) VALUES (@P1, @P2, @P3, 'adopted', @P4, @P5, @P6);",
            &[&receipt_id, &revision, &state, &successor, &previous_owner, &detail],
        )
        .await
        .map_err(|cause| database_error("append receipt adoption event", cause))?;
    Ok(())
}

/// SQL predicate excluding receipts whose owner is a live or recently failed
/// durable-buffer owner from expiry reconciliation.
///
/// It is `lease_expires_at_utc + window > now`, rearranged to subtract the
/// bounded window from `now` so a far-future owner lease cannot overflow
/// `datetime2`.
fn outside_durable_recovery_window(receipt: &str, now: &str) -> String {
    format!(
        "NOT EXISTS ( \
             SELECT 1 FROM arrowcol.collector_instances AS durable_owner \
             WHERE durable_owner.instance_id = {receipt}.owner_instance_id \
               AND durable_owner.durable_recovery_window_ms IS NOT NULL \
               AND durable_owner.lease_expires_at_utc > \
                   DATEADD(millisecond, -durable_owner.durable_recovery_window_ms, {now}) \
         )"
    )
}

fn recovery_window_i32(window: u64) -> ReceiptResult<i32> {
    if window > MAX_DURABLE_RECOVERY_WINDOW_MS {
        return Err(ReceiptError::new(
            ReceiptErrorKind::InvalidConfiguration,
            format!("durable recovery window must be at most {MAX_DURABLE_RECOVERY_WINDOW_MS} ms"),
        ));
    }
    i32::try_from(window).map_err(|_| {
        ReceiptError::new(
            ReceiptErrorKind::InvalidConfiguration,
            "durable recovery window exceeds SQL int",
        )
    })
}

fn active_state_name(state: ReceiptState) -> ReceiptResult<&'static str> {
    match state {
        ReceiptState::Accepted => Ok("accepted"),
        ReceiptState::Buffered => Ok("buffered"),
        ReceiptState::Publishing => Ok("flushing"),
        ReceiptState::Published | ReceiptState::Failed | ReceiptState::PublicationUnknown => Err(ReceiptError::new(
            ReceiptErrorKind::InvalidConfiguration,
            "only accepted, buffered, or publishing receipts can be adopted",
        )),
    }
}

async fn owner_drain_counts(client: &mut SqlClient, owner: CollectorInstanceId) -> ReceiptResult<OwnerDrain> {
    let owner = owner.as_uuid();
    let row = client
        .query(
            "SELECT \
                (SELECT COUNT_BIG(*) FROM arrowcol.delivery_receipts WITH (INDEX(IX_delivery_receipts_owner_active)) \
                 WHERE owner_instance_id = @P1 AND state IN ('accepted', 'buffered', 'flushing')) AS active_receipts, \
                (SELECT COUNT_BIG(*) FROM arrowcol.table_leases WITH (INDEX(IX_table_leases_holder_active)) \
                 WHERE holder_instance_id = @P1 AND released_at_utc IS NULL) AS active_table_leases;",
            &[&owner],
        )
        .await
        .map_err(|cause| database_error("count owner drain work", cause))?
        .into_row()
        .await
        .map_err(|cause| database_error("read owner drain counts", cause))?
        .ok_or_else(|| ReceiptError::new(ReceiptErrorKind::CorruptState, "owner drain count returned no row"))?;
    Ok(OwnerDrain {
        active_receipts: nonnegative_u64(required_i64(&row, "active_receipts")?, "active receipt count")?,
        active_table_leases: nonnegative_u64(required_i64(&row, "active_table_leases")?, "active table lease count")?,
    })
}

async fn validate_catalog_table(client: &mut SqlClient, table_id: Uuid, identity: &TableIdentity) -> ReceiptResult<()> {
    let catalog = identity.catalog().to_string();
    let schema = identity.schema().to_string();
    let table = identity.table().to_string();
    let row = client
        .query(
            "SELECT table_id FROM arrowcol.catalog_tables WITH (UPDLOCK, HOLDLOCK) \
             WHERE table_id = @P1 AND catalog_name = @P2 AND schema_name = @P3 AND table_name = @P4;",
            &[&table_id, &catalog, &schema, &table],
        )
        .await
        .map_err(|cause| database_error("validate catalog table for lease", cause))?
        .into_row()
        .await
        .map_err(|cause| database_error("read catalog table validation", cause))?;
    if row.is_none() {
        return Err(ReceiptError::new(
            ReceiptErrorKind::NotFound,
            "catalog table identity was not found",
        ));
    }
    Ok(())
}

async fn validate_active_owner(
    client: &mut SqlClient,
    owner: CollectorInstanceId,
    now: UnixMillis,
) -> ReceiptResult<()> {
    let owner = owner.as_uuid();
    let now = datetime(now)?;
    let row = client
        .query(
            "SELECT instance_id FROM arrowcol.collector_instances WITH (UPDLOCK, HOLDLOCK) \
             WHERE instance_id = @P1 AND state = 'active' AND stopped_at_utc IS NULL \
               AND lease_expires_at_utc > @P2;",
            &[&owner, &now],
        )
        .await
        .map_err(|cause| database_error("validate active receipt owner", cause))?
        .into_row()
        .await
        .map_err(|cause| database_error("read active owner validation", cause))?;
    if row.is_none() {
        return Err(ReceiptError::new(
            ReceiptErrorKind::Draining,
            "receipt owner is not active",
        ));
    }
    Ok(())
}

async fn next_fencing_epoch(client: &mut SqlClient, table_id: Uuid) -> ReceiptResult<FencingToken> {
    let row = client
        .query(
            "UPDATE arrowcol.table_fencing_epochs WITH (UPDLOCK, HOLDLOCK) \
             SET last_fencing_epoch = last_fencing_epoch + 1, updated_at_utc = SYSUTCDATETIME() \
             OUTPUT INSERTED.last_fencing_epoch \
             WHERE table_id = @P1;",
            &[&table_id],
        )
        .await
        .map_err(|cause| database_error("advance table fencing epoch", cause))?
        .into_row()
        .await
        .map_err(|cause| database_error("read advanced fencing epoch", cause))?;
    let epoch = if let Some(row) = row {
        required_i64(&row, "last_fencing_epoch")?
    } else {
        client
            .execute(
                "INSERT INTO arrowcol.table_fencing_epochs (table_id, last_fencing_epoch) VALUES (@P1, 1);",
                &[&table_id],
            )
            .await
            .map_err(|cause| database_error("initialize table fencing epoch", cause))?;
        1
    };
    FencingToken::new(nonnegative_u64(epoch, "fencing epoch")?)
        .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))
}

async fn receipts_with_client(client: &mut SqlClient, ids: &[ReceiptId]) -> ReceiptResult<Vec<ReceiptRecord>> {
    let rows = receipts_optional_with_client(client, ids).await?;
    rows.into_iter()
        .zip(ids)
        .map(|(record, id)| {
            record.ok_or_else(|| {
                ReceiptError::new(
                    ReceiptErrorKind::CorruptState,
                    format!("receipt {id} disappeared inside its transaction"),
                )
            })
        })
        .collect()
}

async fn receipts_optional_with_client(
    client: &mut SqlClient,
    ids: &[ReceiptId],
) -> ReceiptResult<Vec<Option<ReceiptRecord>>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let uuids: Vec<_> = ids.iter().map(|id| id.as_uuid()).collect();
    let placeholders = (1..=uuids.len())
        .map(|index| format!("@P{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT {RECEIPT_COLUMNS} {RECEIPT_FROM} \
         WHERE r.receipt_id IN ({placeholders}) OPTION (RECOMPILE);"
    );
    let parameters: Vec<&dyn ToSql> = uuids.iter().map(|id| id as &dyn ToSql).collect();
    let rows = client
        .query(query, &parameters)
        .await
        .map_err(|cause| database_error("query receipt statuses", cause))?
        .into_first_result()
        .await
        .map_err(|cause| database_error("read receipt statuses", cause))?;
    let mut by_id = HashMap::with_capacity(rows.len());
    for row in rows {
        let record = receipt_from_row(&row)?;
        by_id.insert(record.receipt_id, record);
    }
    Ok(ids.iter().map(|id| by_id.remove(id)).collect())
}

#[allow(clippy::too_many_lines)]
fn receipt_from_row(row: &Row) -> ReceiptResult<ReceiptRecord> {
    let receipt_id = receipt_id(required_uuid(row, "receipt_id")?)?;
    let owner_instance_id = collector_id(required_uuid(row, "owner_instance_id")?)?;
    let lease_id = lease_id(required_uuid(row, "lease_id")?)?;
    let implementation_pool = ImplementationPool::from_str(required_str(row, "implementation_pool")?)
        .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))?;
    let table = TableIdentity::new(
        CatalogName::new(required_str(row, "catalog_name")?)
            .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))?,
        SchemaName::new(required_str(row, "schema_name")?)
            .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))?,
        TableName::new(required_str(row, "table_name")?)
            .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))?,
    );
    let fencing_epoch = FencingToken::new(nonnegative_u64(required_i64(row, "fencing_epoch")?, "fencing epoch")?)
        .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))?;
    let schema_version = SchemaVersion::new(nonnegative_u64(required_i64(row, "schema_version")?, "schema version")?)
        .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))?;
    let schema_fingerprint =
        SchemaFingerprint::from_bytes(bytes32(required_bytes(row, "schema_sha256")?, "schema fingerprint")?);
    let state_text = required_str(row, "state")?;
    let error = error_status_from_row(row, receipt_id)?;
    let state = match state_text {
        "accepted" => ReceiptState::Accepted,
        "buffered" => ReceiptState::Buffered,
        "flushing" => ReceiptState::Publishing,
        "published" => ReceiptState::Published,
        "retryable_failure" | "permanent_failure" => {
            if error
                .as_ref()
                .is_some_and(|error| error.code() == ErrorCode::PublicationOutcomeUnknown)
            {
                ReceiptState::PublicationUnknown
            } else {
                ReceiptState::Failed
            }
        }
        unexpected => {
            return Err(ReceiptError::new(
                ReceiptErrorKind::CorruptState,
                format!("unsupported persisted receipt state {unexpected:?}"),
            ));
        }
    };
    let failure_disposition = optional_str(row, "failure_disposition")?
        .map(|value| match value {
            "retryable" => Ok(FailureDisposition::Retryable),
            "permanent" => Ok(FailureDisposition::Permanent),
            unexpected => Err(ReceiptError::new(
                ReceiptErrorKind::CorruptState,
                format!("unsupported failure disposition {unexpected:?}"),
            )),
        })
        .transpose()?;
    let created_at_unix_ms = unix_millis(required_datetime(row, "created_at_utc")?)?;
    let lease_deadline_unix_ms = unix_millis(required_datetime(row, "receipt_lease_deadline_utc")?)?;
    let table_lease_acquired = unix_millis(required_datetime(row, "table_lease_acquired_at_utc")?)?;
    let table_lease_expires = unix_millis(required_datetime(row, "table_lease_expires_at_utc")?)?;
    let publication = if state == ReceiptState::Published {
        let lease = TableLease::new(
            lease_id,
            table.clone(),
            owner_instance_id,
            implementation_pool,
            fencing_epoch,
            table_lease_acquired,
            table_lease_expires,
        )
        .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))?;
        Some(DeltaPublicationResult::new(
            measurement_from_i64(required_i64(row, "delta_version")?, "Delta version")?,
            measurement_from_i64(required_i64(row, "data_files_added")?, "data file count")?,
            measurement_from_i64(required_i64(row, "rows_published")?, "published rows")?,
            measurement_from_i64(required_i64(row, "bytes_published")?, "published bytes")?,
            schema_fingerprint,
            unix_millis(required_datetime(row, "published_at_utc")?)?,
            lease.publication_fence(),
        ))
    } else {
        None
    };
    let record = ReceiptRecord {
        receipt_id,
        table_id: required_uuid(row, "table_id")?,
        table,
        implementation_pool,
        owner_instance_id,
        lease_id,
        fencing_epoch,
        schema_version,
        schema_fingerprint,
        state,
        failure_disposition,
        revision: nonnegative_u64(required_i64(row, "revision")?, "receipt revision")?,
        rows: measurement_from_i64(required_i64(row, "row_count")?, "row count")?,
        batches: measurement_from_i64(required_i64(row, "batch_count")?, "batch count")?,
        arrow_memory_bytes: measurement_from_i64(required_i64(row, "arrow_memory_bytes")?, "Arrow memory bytes")?,
        lease_deadline_unix_ms,
        heartbeat_at_unix_ms: unix_millis(required_datetime(row, "heartbeat_at_utc")?)?,
        created_at_unix_ms,
        updated_at_unix_ms: unix_millis(required_datetime(row, "updated_at_utc")?)?,
        publication,
        error,
    };
    record.protocol_status()?;
    Ok(record)
}

fn error_status_from_row(row: &Row, receipt_id: ReceiptId) -> ReceiptResult<Option<ErrorStatus>> {
    let Some(code) = optional_str(row, "error_code")? else {
        return Ok(None);
    };
    let code: ErrorCode = serde_json::from_value(serde_json::Value::String(code.to_owned())).map_err(|cause| {
        ReceiptError::new(
            ReceiptErrorKind::CorruptState,
            format!("persisted receipt error code is invalid: {cause}"),
        )
    })?;
    let message = required_str(row, "error_message")?;
    let mut error = ErrorStatus::new(code, message.to_owned())
        .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))?
        .with_receipt_id(receipt_id);
    if let Some(delay) = optional_i64(row, "retry_after_ms")? {
        let delay = PositiveMeasurement::new(nonnegative_u64(delay, "retry delay")?)
            .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))?;
        error = error
            .with_retry_after(delay)
            .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))?;
    }
    Ok(Some(error))
}

async fn finish_transaction<T>(client: &mut SqlClient, result: ReceiptResult<T>, context: &str) -> ReceiptResult<T> {
    match result {
        Ok(value) => {
            client
                .commit_transaction()
                .await
                .map_err(|cause| database_error(&format!("commit {context}"), cause))?;
            Ok(value)
        }
        Err(error) => {
            if let Err(rollback_error) = client.rollback_transaction().await {
                tracing::warn!(
                    operation = context,
                    error = %rollback_error,
                    "SQL Server rollback failed; preserving primary operation error and discarding connection"
                );
            }
            Err(error)
        }
    }
}

fn receipt_state_for_revision(revision: u64) -> ReceiptResult<&'static str> {
    match revision {
        0 => Ok("accepted"),
        1 => Ok("buffered"),
        2 => Ok("flushing"),
        _ => Err(ReceiptError::new(
            ReceiptErrorKind::Conflict,
            "only active receipt revisions may fail",
        )),
    }
}

fn transition_conflict(receipt_id: ReceiptId) -> ReceiptError {
    ReceiptError::new(
        ReceiptErrorKind::Conflict,
        format!("receipt {receipt_id} changed state, revision, owner, or lease"),
    )
}

fn error_code_name(code: ErrorCode) -> ReceiptResult<String> {
    serde_json::to_value(code)
        .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            ReceiptError::new(
                ReceiptErrorKind::CorruptState,
                "protocol error code did not serialize as text",
            )
        })
}

fn datetime(value: UnixMillis) -> ReceiptResult<NaiveDateTime> {
    let millis = i64::try_from(value.get()).map_err(|_| {
        ReceiptError::new(
            ReceiptErrorKind::InvalidConfiguration,
            "Unix timestamp exceeds SQL datetime range",
        )
    })?;
    DateTime::<Utc>::from_timestamp_millis(millis)
        .map(|value| value.naive_utc())
        .ok_or_else(|| {
            ReceiptError::new(
                ReceiptErrorKind::InvalidConfiguration,
                "Unix timestamp is outside SQL datetime2 range",
            )
        })
}

fn unix_millis(value: NaiveDateTime) -> ReceiptResult<UnixMillis> {
    let millis = value.and_utc().timestamp_millis();
    let millis = u64::try_from(millis).map_err(|_| {
        ReceiptError::new(
            ReceiptErrorKind::CorruptState,
            "persisted timestamp is before the Unix epoch",
        )
    })?;
    UnixMillis::new(millis).map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))
}

fn revision_i64(revision: u64) -> ReceiptResult<i64> {
    i64::try_from(revision).map_err(|_| {
        ReceiptError::new(
            ReceiptErrorKind::InvalidConfiguration,
            "receipt revision exceeds SQL bigint",
        )
    })
}

fn measurement_i64(value: Measurement, label: &str) -> ReceiptResult<i64> {
    i64::try_from(value.get()).map_err(|_| {
        ReceiptError::new(
            ReceiptErrorKind::InvalidConfiguration,
            format!("{label} exceeds SQL bigint"),
        )
    })
}

fn measurement_from_i64(value: i64, label: &str) -> ReceiptResult<Measurement> {
    Measurement::new(nonnegative_u64(value, label)?)
        .map_err(|cause| ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string()))
}

fn nonnegative_u64(value: i64, label: &str) -> ReceiptResult<u64> {
    u64::try_from(value)
        .map_err(|_| ReceiptError::new(ReceiptErrorKind::CorruptState, format!("persisted {label} is negative")))
}

fn receipt_id(value: Uuid) -> ReceiptResult<ReceiptId> {
    value
        .to_string()
        .parse()
        .map_err(|cause: arrowcol_protocol::IdParseError| {
            ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string())
        })
}

fn collector_id(value: Uuid) -> ReceiptResult<CollectorInstanceId> {
    value
        .to_string()
        .parse()
        .map_err(|cause: arrowcol_protocol::IdParseError| {
            ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string())
        })
}

fn lease_id(value: Uuid) -> ReceiptResult<LeaseId> {
    value
        .to_string()
        .parse()
        .map_err(|cause: arrowcol_protocol::IdParseError| {
            ReceiptError::new(ReceiptErrorKind::CorruptState, cause.to_string())
        })
}

fn bytes32(bytes: &[u8], label: &str) -> ReceiptResult<[u8; 32]> {
    bytes.try_into().map_err(|_| {
        ReceiptError::new(
            ReceiptErrorKind::CorruptState,
            format!("persisted {label} is not 32 bytes"),
        )
    })
}

fn required_uuid(row: &Row, column: &str) -> ReceiptResult<Uuid> {
    row.try_get::<Uuid, _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))?
        .ok_or_else(|| missing_column(column))
}

fn required_i64(row: &Row, column: &str) -> ReceiptResult<i64> {
    row.try_get::<i64, _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))?
        .ok_or_else(|| missing_column(column))
}

fn optional_i64(row: &Row, column: &str) -> ReceiptResult<Option<i64>> {
    row.try_get::<i64, _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))
}

fn required_str<'a>(row: &'a Row, column: &str) -> ReceiptResult<&'a str> {
    row.try_get::<&str, _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))?
        .ok_or_else(|| missing_column(column))
}

fn optional_str<'a>(row: &'a Row, column: &str) -> ReceiptResult<Option<&'a str>> {
    row.try_get::<&str, _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))
}

fn required_bytes<'a>(row: &'a Row, column: &str) -> ReceiptResult<&'a [u8]> {
    row.try_get::<&[u8], _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))?
        .ok_or_else(|| missing_column(column))
}

fn required_datetime(row: &Row, column: &str) -> ReceiptResult<NaiveDateTime> {
    row.try_get::<NaiveDateTime, _>(column)
        .map_err(|cause| database_error(&format!("read {column}"), cause))?
        .ok_or_else(|| missing_column(column))
}

fn missing_column(column: &str) -> ReceiptError {
    ReceiptError::new(
        ReceiptErrorKind::CorruptState,
        format!("required SQL column {column} is NULL"),
    )
}

fn database_error(context: &str, cause: impl std::fmt::Display) -> ReceiptError {
    ReceiptError::new(ReceiptErrorKind::Database, format!("{context}: {cause}"))
}

#[cfg(test)]
mod pooling_tests {
    use super::*;
    use arrowcol_sql_server::PoolConfig;

    #[test]
    fn pooling_requires_equivalent_connection_configuration() -> anyhow::Result<()> {
        let config = SqlServerReceiptConfig::new("localhost", 1433, "catalog", "pool-user", "pool-secret")
            .trust_server_certificate(true)
            .with_timeouts(Duration::from_secs(3), Duration::from_secs(7));
        let pool = SqlPool::new(
            config.connection_config(),
            PoolConfig {
                max_size: 2,
                acquire_timeout: Duration::from_secs(1),
            },
        )?;
        let bounded = config.clone().with_batch_limits(1, 2)?;
        let store = SqlServerReceiptStore::new(bounded).with_pool(pool.clone())?;
        assert!(store.clone().inner.pool.is_some());
        assert!(SqlServerReceiptStore::new(config.clone()).inner.pool.is_none());

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
            let Err(error) = SqlServerReceiptStore::new(mismatch).with_pool(pool.clone()) else {
                anyhow::bail!("store accepted a mismatched pool");
            };
            assert_eq!(error.kind(), ReceiptErrorKind::InvalidConfiguration);
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

#[cfg(test)]
mod adoption_validation_tests {
    use super::*;
    use crate::AdoptionCandidate;

    /// Scenario: an oversized or malformed adoption request, an empty
    /// adoption, and an out-of-range durable recovery window reach a store
    /// whose SQL Server endpoint is unreachable.
    /// Guarantees: request bounds and validation fail with request-too-large,
    /// fenced, or invalid-configuration errors before any connection attempt
    /// (which would surface as a database error), and an empty adoption is a
    /// no-op that never connects.
    #[tokio::test]
    async fn adoption_and_recovery_window_bounds_fail_before_connecting() -> anyhow::Result<()> {
        let config = SqlServerReceiptConfig::new("127.0.0.1", 9, "arrowcol", "bounds-user", "bounds-secret")
            .with_timeouts(Duration::from_millis(250), Duration::from_millis(250))
            .with_batch_limits(1, 1)?;
        let store = SqlServerReceiptStore::new(config);
        let successor = CollectorInstanceId::new();
        let table = TableIdentity::new(
            CatalogName::new("telemetry")?,
            SchemaName::new("receipts")?,
            TableName::new("bounds")?,
        );
        let lease = TableLease::new(
            LeaseId::new(),
            table,
            successor,
            ImplementationPool::Direct,
            FencingToken::new(2)?,
            UnixMillis::new(1_000)?,
            UnixMillis::new(10_000)?,
        )?;
        let candidate = |state, revision| AdoptionCandidate::new(ReceiptId::new(), successor, state, revision);
        let request = ReceiptAdoption {
            successor,
            lease: lease.clone(),
            lease_deadline: UnixMillis::new(9_000)?,
            adopted_at: UnixMillis::new(2_000)?,
            receipts: Vec::new(),
        };
        assert_eq!(store.adopt_receipts(&request).await?, ReceiptAdoptionReport::default());

        let cases = [
            (
                ReceiptAdoption {
                    receipts: vec![
                        candidate(ReceiptState::Accepted, 0),
                        candidate(ReceiptState::Buffered, 1),
                    ],
                    ..request.clone()
                },
                ReceiptErrorKind::RequestTooLarge,
            ),
            (
                ReceiptAdoption {
                    successor: CollectorInstanceId::new(),
                    receipts: vec![candidate(ReceiptState::Accepted, 0)],
                    ..request.clone()
                },
                ReceiptErrorKind::Fenced,
            ),
            (
                ReceiptAdoption {
                    receipts: vec![candidate(ReceiptState::Published, 3)],
                    ..request.clone()
                },
                ReceiptErrorKind::InvalidConfiguration,
            ),
            (
                ReceiptAdoption {
                    lease_deadline: UnixMillis::new(10_001)?,
                    receipts: vec![candidate(ReceiptState::Publishing, 2)],
                    ..request
                },
                ReceiptErrorKind::InvalidConfiguration,
            ),
        ];
        for (request, expected) in cases {
            let Err(error) = store.adopt_receipts(&request).await else {
                anyhow::bail!("invalid adoption request was accepted");
            };
            assert_eq!(error.kind(), expected, "{error}");
        }

        let started = UnixMillis::new(1_000)?;
        for window in [MAX_DURABLE_RECOVERY_WINDOW_MS + 1, u64::MAX] {
            let Err(error) = store
                .register_owner(OwnerRegistration {
                    instance_id: successor,
                    instance_key: "bounds".to_owned(),
                    generation: 1,
                    host_name: None,
                    build_version: None,
                    started_at_unix_ms: started,
                    lease_deadline_unix_ms: UnixMillis::new(5_000)?,
                    durable_recovery_window_ms: Some(window),
                })
                .await
            else {
                anyhow::bail!("out-of-range recovery window was accepted");
            };
            assert_eq!(error.kind(), ReceiptErrorKind::InvalidConfiguration, "{error}");
        }
        assert_eq!(recovery_window_i32(MAX_DURABLE_RECOVERY_WINDOW_MS)?, 86_400_000);
        assert_eq!(recovery_window_i32(0)?, 0);
        Ok(())
    }

    /// Scenario: the expiry reconciliation predicate is rendered for the
    /// receipt alias and timestamp parameter used by its SELECT and UPDATE.
    /// Guarantees: both statements exclude receipts of owners with a durable
    /// recovery window whose lease plus window is still in the future, using
    /// the overflow-safe `lease_expires > now - window` form.
    #[test]
    fn recovery_window_predicate_targets_the_requested_alias_and_instant() {
        let predicate = outside_durable_recovery_window("r", "@P4");
        assert!(predicate.starts_with("NOT EXISTS ("));
        assert!(predicate.contains("durable_owner.instance_id = r.owner_instance_id"));
        assert!(predicate.contains("durable_owner.durable_recovery_window_ms IS NOT NULL"));
        assert!(predicate.contains("DATEADD(millisecond, -durable_owner.durable_recovery_window_ms, @P4)"));
    }
}

#[cfg(test)]
mod deadlock_tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    /// Scenario: SQL Server picks a receipt transaction as a deadlock victim
    /// twice in a row, as happens when a restarted collector adopts receipts
    /// while its heartbeat and flushes contend for the same rows.
    /// Guarantees: The store retries the rolled-back transaction until it
    /// commits instead of surfacing a transient lock conflict that would fail
    /// the collector closed.
    #[tokio::test]
    async fn deadlock_victims_are_retried_until_they_commit() -> anyhow::Result<()> {
        let attempts = AtomicU32::new(0);
        let value = retry_deadlock(|| async {
            if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(database_error(
                    "heartbeat receipt owner",
                    "Transaction (Process ID 85) was deadlocked on lock resources with another process \
                     and has been chosen as the deadlock victim. Rerun the transaction.",
                ))
            } else {
                Ok(7)
            }
        })
        .await?;
        assert_eq!(value, 7);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        Ok(())
    }

    /// Scenario: A receipt operation fails for a reason other than a deadlock.
    /// Guarantees: The error surfaces after one attempt; only transactions the
    /// server rolled back as deadlock victims are retried.
    #[tokio::test]
    async fn other_failures_are_not_retried() {
        let attempts = AtomicU32::new(0);
        let result: ReceiptResult<()> = retry_deadlock(|| async {
            let _ = attempts.fetch_add(1, Ordering::SeqCst);
            Err(ReceiptError::new(ReceiptErrorKind::Fenced, "table lease is invalid"))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
