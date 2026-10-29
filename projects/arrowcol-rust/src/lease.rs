use std::collections::HashMap;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use arrow_deltalake::commit::BatchedTable;
use arrowcol_buffer::StageLeases;
use arrowcol_protocol::{
    CollectorInstanceId, ImplementationPool, PublicationFence, TableIdentity, TableLease, UnixMillis,
};
use arrowcol_receipts::{LeasePublicationFence, ReceiptStore, TableLeaseRequest};
use arrowcol_routing::{LeaseKey, LeaseState, RoutingStore};
use async_trait::async_trait;
use tokio::sync::{Mutex, RwLock as AsyncRwLock};
use uuid::Uuid;

/// One process-owned table lease and its synchronous Delta commit fence.
pub struct TableLeaseHandle {
    table_id: Uuid,
    lease: RwLock<TableLease>,
    fence: Arc<LeasePublicationFence>,
    valid: AtomicBool,
    delta_table: Mutex<Option<Arc<BatchedTable>>>,
    routing_lease_id: arrowcol_protocol::LeaseId,
    routing_epoch: arrowcol_protocol::FencingToken,
}

impl TableLeaseHandle {
    /// Returns the deterministic catalog table identifier.
    #[must_use]
    pub const fn table_id(&self) -> Uuid {
        self.table_id
    }

    /// Returns the latest successfully renewed lease.
    ///
    /// # Errors
    ///
    /// Fails closed after routing/SQL ownership loss or lock poisoning.
    pub fn current(&self) -> Result<TableLease> {
        if !self.valid.load(Ordering::Acquire) {
            bail!("table lease is invalid");
        }
        let lease = self
            .lease
            .read()
            .map_err(|_| anyhow!("table lease cache lock poisoned"))?
            .clone();
        if !lease.is_active_at(now_unix_millis()?) {
            bail!("table lease expired");
        }
        Ok(lease)
    }

    /// Returns the publication fence expected by buffer jobs.
    ///
    /// # Errors
    ///
    /// Fails closed if the lease is invalid.
    pub fn publication_fence(&self) -> Result<PublicationFence> {
        Ok(self.current()?.publication_fence())
    }

    /// Returns the synchronous fence shared with the Delta coordinator.
    #[must_use]
    pub fn delta_fence(&self) -> Arc<LeasePublicationFence> {
        Arc::clone(&self.fence)
    }

    /// Returns an already opened table coordinator.
    pub async fn delta_table(&self) -> Option<Arc<BatchedTable>> {
        self.delta_table.lock().await.clone()
    }

    /// Installs the table coordinator, preserving the first successful opener.
    pub async fn install_delta_table(&self, table: Arc<BatchedTable>) -> Arc<BatchedTable> {
        let mut guard = self.delta_table.lock().await;
        Arc::clone(guard.get_or_insert(table))
    }

    /// Disables commit collection delays during graceful drain.
    pub async fn start_delta_draining(&self) {
        if let Some(table) = self.delta_table().await {
            table.start_draining();
        }
    }

    fn update(&self, lease: TableLease) -> Result<()> {
        self.fence
            .update(lease.clone())
            .map_err(|error| anyhow!(error.to_string()))?;
        *self
            .lease
            .write()
            .map_err(|_| anyhow!("table lease cache lock poisoned"))? = lease;
        Ok(())
    }

    fn invalidate(&self, reason: impl Into<String>) {
        self.valid.store(false, Ordering::Release);
        self.fence.invalidate(reason);
    }
}

/// Acquires, renews, and releases one receipt/Delta publication lease per table.
pub struct TableLeaseManager {
    receipts: Arc<dyn ReceiptStore>,
    routing: Arc<dyn RoutingStore>,
    instance_id: CollectorInstanceId,
    lease_ttl: Duration,
    entries: AsyncRwLock<HashMap<TableIdentity, Arc<TableLeaseHandle>>>,
    acquisition_gates: Mutex<HashMap<TableIdentity, Arc<Mutex<()>>>>,
    draining: AtomicBool,
    released_tables: AsyncRwLock<HashMap<TableIdentity, Uuid>>,
}

impl TableLeaseManager {
    /// Creates a lease manager.
    #[must_use]
    pub fn new(
        receipts: Arc<dyn ReceiptStore>,
        routing: Arc<dyn RoutingStore>,
        instance_id: CollectorInstanceId,
        lease_ttl: Duration,
    ) -> Self {
        Self {
            receipts,
            routing,
            instance_id,
            lease_ttl,
            entries: AsyncRwLock::new(HashMap::new()),
            acquisition_gates: Mutex::new(HashMap::new()),
            draining: AtomicBool::new(false),
            released_tables: AsyncRwLock::new(HashMap::new()),
        }
    }

    /// Returns or acquires the active publication lease for a routed table.
    ///
    /// # Errors
    ///
    /// Fails closed when routing ownership is absent, stale, draining, or held
    /// by another collector, or when SQL lease acquisition fails.
    pub async fn lease_for(&self, table_id: Uuid, table: &TableIdentity) -> Result<Arc<TableLeaseHandle>> {
        self.acquire(table_id, table, false).await
    }

    /// Acquires target publication authority while new-RPC admission remains fenced.
    ///
    /// # Errors
    ///
    /// Returns an error for stale routing authority, invalid cached lineage,
    /// collector drain, or failed SQL lease acquisition.
    pub async fn activate_table(&self, table_id: Uuid, table: &TableIdentity) -> Result<()> {
        self.acquire(table_id, table, true).await?;
        Ok(())
    }

    async fn acquire(&self, table_id: Uuid, table: &TableIdentity, activating: bool) -> Result<Arc<TableLeaseHandle>> {
        if self.draining.load(Ordering::Acquire) {
            bail!("collector is draining");
        }
        let route = self.verify_routing_owner(table, activating).await?;
        if let Some(handle) = self.entries.read().await.get(table).cloned()
            && handle.current().is_ok()
        {
            if handle.routing_lease_id != route.lease_id || handle.routing_epoch != route.fencing_epoch {
                bail!("cached publication handle belongs to a stale routing epoch");
            }
            return Ok(handle);
        }
        let gate = {
            let mut gates = self.acquisition_gates.lock().await;
            Arc::clone(gates.entry(table.clone()).or_insert_with(|| Arc::new(Mutex::new(()))))
        };
        let _guard = gate.lock().await;
        let route = self.verify_routing_owner(table, activating).await?;
        if let Some(handle) = self.entries.read().await.get(table).cloned()
            && handle.current().is_ok()
        {
            if handle.routing_lease_id != route.lease_id || handle.routing_epoch != route.fencing_epoch {
                bail!("cached publication handle belongs to a stale routing epoch");
            }
            return Ok(handle);
        }
        let issued_at = now_unix_millis()?;
        let expires_at = add_duration(issued_at, self.lease_ttl)?;
        let lease = self
            .receipts
            .acquire_table_lease(TableLeaseRequest {
                table_id,
                table: table.clone(),
                owner_instance_id: self.instance_id,
                implementation_pool: ImplementationPool::Direct,
                issued_at_unix_ms: issued_at,
                expires_at_unix_ms: expires_at,
            })
            .await
            .context("acquire SQL publication lease")?;
        let handle = Arc::new(TableLeaseHandle {
            table_id,
            fence: Arc::new(LeasePublicationFence::new(lease.clone())),
            lease: RwLock::new(lease),
            valid: AtomicBool::new(true),
            delta_table: Mutex::new(None),
            routing_lease_id: route.lease_id,
            routing_epoch: route.fencing_epoch,
        });
        self.entries.write().await.insert(table.clone(), Arc::clone(&handle));
        self.released_tables.write().await.remove(table);
        Ok(handle)
    }

    /// Returns a lease handle matching an exact publication fence.
    ///
    /// # Errors
    ///
    /// Rejects stale or foreign buffer publication contexts.
    pub async fn handle_for_fence(
        &self,
        table: &TableIdentity,
        fence: &PublicationFence,
    ) -> Result<Arc<TableLeaseHandle>> {
        let handle = self
            .entries
            .read()
            .await
            .get(table)
            .cloned()
            .ok_or_else(|| anyhow!("table publication lease is not registered"))?;
        let lease = handle.current()?;
        if !fence.matches_lease(&lease) {
            bail!("buffer publication fence is stale");
        }
        Ok(handle)
    }

    /// Renews every active table lease after confirming current routing
    /// ownership in one SQL view.
    ///
    /// # Errors
    ///
    /// Invalidates the affected fence and returns an error on any ownership or
    /// SQL renewal failure.
    pub async fn renew_all(&self) -> Result<()> {
        let now = now_unix_millis()?;
        let routing = self
            .routing
            .load_view(now)
            .await
            .context("load routing ownership for lease renewal")?;
        let handles: Vec<_> = self
            .entries
            .read()
            .await
            .iter()
            .map(|(table, handle)| (table.clone(), Arc::clone(handle)))
            .collect();
        for (table, handle) in handles {
            let gate = {
                let mut gates = self.acquisition_gates.lock().await;
                Arc::clone(gates.entry(table.clone()).or_insert_with(|| Arc::new(Mutex::new(()))))
            };
            let _guard = gate.lock().await;
            if !self
                .entries
                .read()
                .await
                .get(&table)
                .is_some_and(|current| Arc::ptr_eq(current, &handle))
            {
                continue;
            }
            let key = LeaseKey::new(ImplementationPool::Direct, table.clone());
            let owned = routing.leases.get(&key).is_some_and(|lease| {
                matches!(lease.state, LeaseState::Active | LeaseState::Draining)
                    && lease.holder_instance_id == self.instance_id
                    && lease.lease_id == handle.routing_lease_id
                    && lease.fencing_epoch == handle.routing_epoch
                    && lease.is_valid_at(now)
            });
            if !owned {
                handle.invalidate("routing ownership was lost before publication");
                bail!("routing ownership for {table} was lost");
            }
            let current = handle.current()?;
            let expires_at = add_duration(now, self.lease_ttl)?;
            match self.receipts.renew_table_lease(&current, expires_at).await {
                Ok(renewed) => {
                    if let Err(error) = handle.update(renewed) {
                        handle.invalidate(format!("invalid table lease renewal: {error}"));
                        return Err(error).context("update publication fence after SQL renewal");
                    }
                }
                Err(error) => {
                    handle.invalidate(format!("SQL table lease renewal failed: {error}"));
                    return Err(error).context("renew SQL publication lease");
                }
            }
        }
        Ok(())
    }

    /// Starts local Delta drain for every opened table.
    pub async fn start_delta_draining(&self) {
        self.draining.store(true, Ordering::Release);
        let handles: Vec<_> = self.entries.read().await.values().cloned().collect();
        for handle in handles {
            handle.start_delta_draining().await;
        }
    }

    /// Releases every SQL table lease after publication drain.
    ///
    /// # Errors
    ///
    /// Returns the first release failure after invalidating all local fences.
    pub async fn release_all(&self) -> Result<()> {
        self.draining.store(true, Ordering::Release);
        let released_at = now_unix_millis()?;
        let handles: Vec<_> = self.entries.read().await.values().cloned().collect();
        let mut first_error = None;
        for handle in handles {
            let lease = handle.lease.read().map(|lease| lease.clone());
            handle.invalidate("collector released table ownership");
            match lease {
                Ok(lease) => {
                    if let Err(error) = self.receipts.release_table_lease(&lease, released_at).await
                        && first_error.is_none()
                    {
                        first_error = Some(anyhow!(error).context("release SQL publication lease"));
                    }
                }
                Err(_) if first_error.is_none() => {
                    first_error = Some(anyhow!("table lease cache lock poisoned during release"));
                }
                Err(_) => {}
            }
        }
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    /// Invalidates every commit fence immediately.
    pub async fn invalidate_all(&self, reason: &str) {
        self.draining.store(true, Ordering::Release);
        let handles: Vec<_> = self.entries.read().await.values().cloned().collect();
        for handle in handles {
            handle.invalidate(reason.to_owned());
        }
    }

    /// Returns table identities and deterministic IDs for pressure reporting.
    pub async fn known_tables(&self) -> Vec<(Uuid, TableIdentity)> {
        let mut known: Vec<_> = self
            .entries
            .read()
            .await
            .iter()
            .map(|(table, handle)| (handle.table_id(), table.clone()))
            .collect();
        known.extend(
            self.released_tables
                .read()
                .await
                .iter()
                .map(|(table, id)| (*id, table.clone())),
        );
        known
    }

    /// Releases one exact source SQL fence only after its admission/backlog barrier.
    ///
    /// # Errors
    ///
    /// Returns an error if the source handle is missing/invalid or SQL release
    /// cannot be confirmed. Unknown release outcomes remain fenced.
    pub async fn release_table(&self, table: &TableIdentity) -> Result<()> {
        let gate = {
            let mut gates = self.acquisition_gates.lock().await;
            Arc::clone(gates.entry(table.clone()).or_insert_with(|| Arc::new(Mutex::new(()))))
        };
        let _guard = gate.lock().await;
        if self.released_tables.read().await.contains_key(table) {
            return Ok(());
        }
        let handle = self
            .entries
            .read()
            .await
            .get(table)
            .cloned()
            .context("source has no known publication handle; cannot prove release")?;
        let lease = handle.current()?;
        self.receipts
            .release_table_lease(&lease, now_unix_millis()?)
            .await
            .context("release source SQL publication fence")?;
        handle.invalidate("source publication authority released for controlled migration");
        self.released_tables
            .write()
            .await
            .insert(table.clone(), handle.table_id);
        self.entries.write().await.remove(table);
        Ok(())
    }

    pub async fn start_table_draining(&self, table: &TableIdentity) {
        if let Some(handle) = self.entries.read().await.get(table).cloned() {
            handle.start_delta_draining().await;
        }
    }

    async fn verify_routing_owner(
        &self,
        table: &TableIdentity,
        activating: bool,
    ) -> Result<arrowcol_routing::RoutingLease> {
        let now = now_unix_millis()?;
        let view = self
            .routing
            .load_view(now)
            .await
            .context("load routing ownership before table lease acquisition")?;
        let key = LeaseKey::new(ImplementationPool::Direct, table.clone());
        let route = view
            .leases
            .get(&key)
            .ok_or_else(|| anyhow!("routing lease for {table} is missing"))?;
        if (!activating && route.state != LeaseState::Active)
            || route.holder_instance_id != self.instance_id
            || !route.is_valid_at(now)
        {
            bail!("routing lease for {table} is not active on this collector");
        }
        Ok(route.clone())
    }
}

/// Creates a receipt ownership deadline bounded by the active table lease.
///
/// # Errors
///
/// Rejects expired leases or unrepresentable timestamps.
pub fn receipt_deadline(lease: &TableLease, duration: Duration) -> Result<UnixMillis> {
    let now = now_unix_millis()?;
    if !lease.is_active_at(now) {
        bail!("table lease expired before receipt creation");
    }
    let desired = add_duration(now, duration)?;
    UnixMillis::new(desired.get().min(lease.expires_at_unix_ms().get())).map_err(|error| anyhow!(error.to_string()))
}

/// Current bounded Unix milliseconds.
///
/// # Errors
///
/// Returns an error for an invalid system clock or protocol overflow.
pub fn now_unix_millis() -> Result<UnixMillis> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis();
    let millis = u64::try_from(millis).context("system time exceeds protocol range")?;
    UnixMillis::new(millis).map_err(|error| anyhow!(error.to_string()))
}

/// Adds a duration to a protocol timestamp.
///
/// # Errors
///
/// Returns an error on arithmetic or protocol overflow.
pub fn add_duration(value: UnixMillis, duration: Duration) -> Result<UnixMillis> {
    let delta = u64::try_from(duration.as_millis()).context("duration exceeds protocol range")?;
    let value = value.get().checked_add(delta).context("timestamp overflow")?;
    UnixMillis::new(value).map_err(|error| anyhow!(error.to_string()))
}

#[async_trait]
impl StageLeases for TableLeaseManager {
    async fn current_lease(&self, table_id: Uuid, table: &TableIdentity) -> Result<TableLease, String> {
        // Persisted work already admitted under this exact handle must finish during drain.
        // This path does not create a new RPC or acquire a different owner.
        let handle = match self.entries.read().await.get(table).cloned() {
            Some(handle) => handle,
            None => self
                .lease_for(table_id, table)
                .await
                .map_err(|error| format!("acquire table lease: {error:#}"))?,
        };
        let lease = handle
            .current()
            .map_err(|error| format!("load table lease: {error:#}"))?;
        if lease.holder_instance_id() != self.instance_id {
            return Err("table lease is held by another collector".to_owned());
        }
        Ok(lease)
    }
}
