//! Deliberately process-local receipt bookkeeping; not a durable collector service.

use std::collections::HashMap;

use arrowcol_protocol::{CollectorInstanceId, ReceiptId, ReceiptState, TableLease, UnixMillis};
use arrowcol_receipts::{
    OwnerDrain, OwnerRegistration, PublicationBatch, ReceiptAdoption, ReceiptAdoptionReport, ReceiptError,
    ReceiptErrorKind, ReceiptFailure, ReceiptRecord, ReceiptResult, ReceiptStore, ReceiptVersion, ReconciliationReport,
    TableLeaseRequest, ValidatedDelivery,
};
use async_trait::async_trait;
use tokio::sync::Mutex;

#[derive(Default)]
pub struct MemoryReceipts {
    records: Mutex<HashMap<ReceiptId, ReceiptRecord>>,
}

fn unsupported<T>() -> ReceiptResult<T> {
    Err(ReceiptError::new(
        ReceiptErrorKind::InvalidConfiguration,
        "operation is outside the experiment process-local receipt lifecycle",
    ))
}

impl MemoryReceipts {
    async fn transition(
        &self,
        owner: CollectorInstanceId,
        expected: &[ReceiptVersion],
        from: ReceiptState,
        to: ReceiptState,
        deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        let mut records = self.records.lock().await;
        validate(&records, owner, expected, Some(from))?;
        Ok(expected
            .iter()
            .filter_map(|version| {
                let record = records.get_mut(&version.receipt_id)?;
                record.state = to;
                record.revision += 1;
                record.lease_deadline_unix_ms = deadline;
                Some(record.clone())
            })
            .collect())
    }
}

fn validate(
    records: &HashMap<ReceiptId, ReceiptRecord>,
    owner: CollectorInstanceId,
    expected: &[ReceiptVersion],
    state: Option<ReceiptState>,
) -> ReceiptResult<()> {
    for version in expected {
        let record = records
            .get(&version.receipt_id)
            .ok_or_else(|| ReceiptError::new(ReceiptErrorKind::NotFound, "receipt missing"))?;
        if record.owner_instance_id != owner
            || record.revision != version.revision
            || state.is_some_and(|state| record.state != state)
        {
            return Err(ReceiptError::new(
                ReceiptErrorKind::Conflict,
                "receipt transition conflict",
            ));
        }
    }
    Ok(())
}

#[async_trait]
impl ReceiptStore for MemoryReceipts {
    async fn register_owner(&self, _: OwnerRegistration) -> ReceiptResult<()> {
        unsupported()
    }
    async fn heartbeat_owner(&self, _: CollectorInstanceId, _: UnixMillis) -> ReceiptResult<()> {
        unsupported()
    }
    async fn start_owner_drain(&self, _: CollectorInstanceId) -> ReceiptResult<OwnerDrain> {
        unsupported()
    }
    async fn finish_owner_drain(&self, _: CollectorInstanceId, _: UnixMillis) -> ReceiptResult<()> {
        unsupported()
    }
    async fn acquire_table_lease(&self, _: TableLeaseRequest) -> ReceiptResult<TableLease> {
        unsupported()
    }
    async fn renew_table_lease(&self, _: &TableLease, _: UnixMillis) -> ReceiptResult<TableLease> {
        unsupported()
    }
    async fn release_table_lease(&self, _: &TableLease, _: UnixMillis) -> ReceiptResult<()> {
        unsupported()
    }

    async fn create_flight_batch(
        &self,
        owner: CollectorInstanceId,
        lease: &TableLease,
        deadline: UnixMillis,
        deliveries: &[ValidatedDelivery],
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        let now = crate::now()?;
        if owner != lease.holder_instance_id() || !lease.is_active_at(now) {
            return Err(ReceiptError::new(ReceiptErrorKind::Fenced, "inactive experiment lease"));
        }
        let mut records = self.records.lock().await;
        let mut result = Vec::new();
        for delivery in deliveries {
            if delivery.table() != lease.table() {
                return Err(ReceiptError::new(ReceiptErrorKind::Fenced, "table lease mismatch"));
            }
            let record = ReceiptRecord {
                receipt_id: ReceiptId::new(),
                table_id: delivery.table_id(),
                table: delivery.table().clone(),
                implementation_pool: lease.implementation_pool(),
                owner_instance_id: owner,
                lease_id: lease.lease_id(),
                fencing_epoch: lease.fencing_token(),
                schema_version: delivery.schema_version(),
                schema_fingerprint: delivery.schema_fingerprint(),
                state: ReceiptState::Accepted,
                failure_disposition: None,
                revision: 0,
                rows: delivery.rows(),
                batches: delivery.batches(),
                arrow_memory_bytes: delivery.arrow_memory_bytes(),
                lease_deadline_unix_ms: deadline,
                heartbeat_at_unix_ms: now,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
                publication: None,
                error: None,
            };
            records.insert(record.receipt_id, record.clone());
            result.push(record);
        }
        Ok(result)
    }

    async fn mark_buffered(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.transition(
            owner,
            receipts,
            ReceiptState::Accepted,
            ReceiptState::Buffered,
            deadline,
        )
        .await
    }
    async fn mark_flushing(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        deadline: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        self.transition(
            owner,
            receipts,
            ReceiptState::Buffered,
            ReceiptState::Publishing,
            deadline,
        )
        .await
    }
    async fn heartbeat_receipts(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        _: UnixMillis,
    ) -> ReceiptResult<()> {
        let records = self.records.lock().await;
        validate(&records, owner, receipts, None)
    }
    async fn publish_commit(
        &self,
        owner: CollectorInstanceId,
        batch: &PublicationBatch,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        let mut records = self.records.lock().await;
        validate(&records, owner, &batch.receipts, Some(ReceiptState::Publishing))?;
        Ok(batch
            .receipts
            .iter()
            .filter_map(|version| {
                let record = records.get_mut(&version.receipt_id)?;
                record.state = ReceiptState::Published;
                record.revision += 1;
                record.publication = Some(batch.publication.clone());
                record.updated_at_unix_ms = batch.publication.published_at_unix_ms();
                Some(record.clone())
            })
            .collect())
    }
    async fn fail_batch(
        &self,
        owner: CollectorInstanceId,
        receipts: &[ReceiptVersion],
        failure: &ReceiptFailure,
        failed_at: UnixMillis,
    ) -> ReceiptResult<Vec<ReceiptRecord>> {
        let mut records = self.records.lock().await;
        validate(&records, owner, receipts, None)?;
        Ok(receipts
            .iter()
            .filter_map(|version| {
                let record = records.get_mut(&version.receipt_id)?;
                record.state = if failure.error().code() == arrowcol_protocol::ErrorCode::PublicationOutcomeUnknown {
                    ReceiptState::PublicationUnknown
                } else {
                    ReceiptState::Failed
                };
                record.failure_disposition = Some(failure.disposition());
                record.error = Some(failure.error().clone());
                record.revision += 1;
                record.updated_at_unix_ms = failed_at;
                Some(record.clone())
            })
            .collect())
    }
    async fn receipt(&self, id: ReceiptId) -> ReceiptResult<Option<ReceiptRecord>> {
        Ok(self.records.lock().await.get(&id).cloned())
    }
    async fn receipts(&self, ids: &[ReceiptId]) -> ReceiptResult<Vec<Option<ReceiptRecord>>> {
        let records = self.records.lock().await;
        Ok(ids.iter().map(|id| records.get(id).cloned()).collect())
    }
    async fn reconcile_expired(&self, _: UnixMillis, _: usize) -> ReceiptResult<ReconciliationReport> {
        unsupported()
    }
    async fn adopt_receipts(&self, _: &ReceiptAdoption) -> ReceiptResult<ReceiptAdoptionReport> {
        unsupported()
    }
    async fn fail_unbuffered_predecessor_receipts(
        &self,
        _: CollectorInstanceId,
        _: UnixMillis,
        _: usize,
    ) -> ReceiptResult<ReconciliationReport> {
        unsupported()
    }
}
