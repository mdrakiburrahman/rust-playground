//! Independent target files, bounded file sorting, and Kernel transaction dedupe.

use crate::stream::TenantSink;
use anyhow::{Context, Result, ensure};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_deltalake::delta::{DeltaTable, StagedAppend};
use arrow_deltalake::layout::{FileSortBuffer, LayoutPolicy};
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct DeltaSink {
    table: Arc<DeltaTable>,
    schema: SchemaRef,
    policy: LayoutPolicy,
    partitions: Option<BTreeMap<String, Option<String>>>,
    current: Option<FileSortBuffer>,
    staged: Vec<StagedAppend>,
    active_batch: Option<(String, i64)>,
}

impl DeltaSink {
    /// Wrap an independently owned target whose schema/partitioning is validated.
    ///
    /// # Errors
    /// Returns layout validation errors.
    pub fn new(table: Arc<DeltaTable>, schema: SchemaRef, policy: LayoutPolicy) -> Result<Self> {
        policy.validate(&schema)?;
        Ok(Self {
            table,
            schema,
            policy,
            partitions: None,
            current: None,
            staged: Vec::new(),
            active_batch: None,
        })
    }

    async fn flush_file(&mut self) -> Result<()> {
        if let Some(buffer) = self.current.take() {
            if buffer.rows() == 0 {
                return Ok(());
            }
            let sorted = buffer.finish()?;
            let values = self.partitions.as_ref().context("missing output partition values")?;
            let physical_schema = Arc::new(arrow::datatypes::Schema::new(
                self.schema
                    .fields()
                    .iter()
                    .filter(|field| !values.contains_key(field.name()))
                    .cloned()
                    .collect::<Vec<_>>(),
            ));
            let mut physical_policy = self.policy.clone();
            physical_policy.sort_columns.retain(|name| !values.contains_key(name));
            physical_policy.bloom_columns.retain(|name| !values.contains_key(name));
            // A tenant key may itself be partitioned and omitted from Parquet.
            // Sorting an additional physical column still gives a valid policy.
            if physical_policy.sort_columns.is_empty()
                && let Some(field) = physical_schema.fields().first()
            {
                physical_policy.sort_columns.push(field.name().clone());
            }
            let properties = physical_policy.writer_properties(&physical_schema)?;
            ensure!(
                self.staged.len() < 4_096,
                "microbatch exceeds 4096 staged-file budget; reduce source file sizes"
            );
            let mut writer = self.table.new_file_partitioned_with_properties(
                Arc::clone(&self.schema),
                values.clone(),
                properties,
            )?;
            writer.write(&sorted).await?;
            self.staged.push(writer.stage().await?);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl TenantSink for DeltaSink {
    async fn lookup_transaction(&self, query_id: &str) -> Result<Option<i64>> {
        self.table.lookup_txn(query_id)
    }

    async fn start_batch(&mut self, query_id: &str, batch: i64) -> Result<()> {
        ensure!(
            batch >= 0 && !query_id.starts_with("arrowcol."),
            "invalid tenant sink transaction identity"
        );
        // Previously staged failures remain uncommitted; never reuse them as a
        // different batch. VACUUM LITE cannot discover never-logged orphans;
        // those require separate, quiescent operator reconciliation.
        self.current = None;
        self.partitions = None;
        self.staged.clear();
        self.active_batch = Some((query_id.to_owned(), batch));
        Ok(())
    }

    async fn write(&mut self, batch: RecordBatch, partition_values: &BTreeMap<String, Option<String>>) -> Result<()> {
        ensure!(self.active_batch.is_some(), "sink batch was not started");
        ensure!(batch.schema() == self.schema, "tenant output schema changed");
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self
            .partitions
            .as_ref()
            .is_some_and(|values| values != partition_values)
        {
            self.flush_file().await?;
        }
        self.partitions = Some(partition_values.clone());
        if self.current.is_none() {
            self.current = Some(FileSortBuffer::new(Arc::clone(&self.schema), self.policy.clone())?);
        }
        if !self.current.as_mut().context("missing file buffer")?.try_push(&batch)? {
            self.flush_file().await?;
            let mut next = FileSortBuffer::new(Arc::clone(&self.schema), self.policy.clone())?;
            ensure!(next.try_push(&batch)?, "batch cannot fit an empty file buffer");
            self.current = Some(next);
        }
        Ok(())
    }

    async fn commit_batch(&mut self, query_id: &str, batch: i64) -> Result<()> {
        ensure!(
            self.active_batch
                .as_ref()
                .is_some_and(|identity| identity.0 == query_id && identity.1 == batch),
            "sink commit identity differs from active batch"
        );
        self.flush_file().await?;
        self.table
            .commit_files_with_transaction(&self.staged, query_id, batch)?;
        self.staged.clear();
        self.partitions = None;
        self.active_batch = None;
        Ok(())
    }
}
