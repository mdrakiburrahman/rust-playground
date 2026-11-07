//! Atomic catalog metadata DDL through Delta Kernel's optimistic transactions.

use std::{collections::BTreeMap, io::Cursor, sync::Arc};

use anyhow::{Context, Result, bail};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use delta_kernel::{
    DeltaResult, DeltaResultIterator, Engine, FilteredEngineData,
    actions::Metadata,
    committer::{CommitMetadata, CommitResponse, Committer, PublishMetadata},
    engine::{
        arrow_conversion::{TryFromArrow, TryIntoArrow},
        arrow_data::{ArrowEngineData, EngineDataArrowExt},
    },
    schema::{StructField, StructType},
    table_configuration::TableConfiguration,
    transaction::CommitResult,
};
use delta_kernel_default_engine::executor::TaskExecutor;
use futures::StreamExt;
use url::Url;

use crate::{delta::DeltaTable, delta_io::shared_executor, normalize};

/// Committed Delta schema and HMS-shaped table metadata.
#[derive(Clone, Debug)]
pub struct CatalogDeltaSnapshot {
    /// Optimistic Delta log version.
    pub version: u64,
    /// Canonical physical Delta schema.
    pub schema: SchemaRef,
    /// Complete Delta metadata configuration, including internal catalog state.
    pub properties: BTreeMap<String, String>,
    /// Ordered partition column names.
    pub partition_columns: Vec<String>,
    /// Optional table comment.
    pub comment: Option<String>,
}

impl DeltaTable {
    /// Opens storage for catalog DDL without creating or evolving the table.
    ///
    /// # Errors
    /// Returns credential, sandbox-fence, invalid URL or filesystem errors.
    pub fn open_catalog(url: Url) -> Result<Arc<Self>> {
        Self::open_connection(url, Vec::new(), None, None)
    }

    /// Reads an existing Delta head without initialization.
    ///
    /// # Errors
    /// Returns corrupt-log or storage errors, never treats them as an absent table.
    pub fn catalog_snapshot(&self) -> Result<Option<CatalogDeltaSnapshot>> {
        let store = Arc::clone(&self.store);
        let prefix = self.object_path("_delta_log/")?;
        let exists = shared_executor()?.block_on(async move {
            store
                .list(Some(&prefix))
                .next()
                .await
                .transpose()
                .map(|entry| entry.is_some())
        })?;
        if !exists {
            return Ok(None);
        }
        let snapshot = self.snapshot()?;
        let metadata = snapshot.table_configuration().metadata();
        Ok(Some(CatalogDeltaSnapshot {
            version: snapshot.version(),
            schema: Arc::new(snapshot.schema().as_ref().try_into_arrow()?),
            properties: metadata
                .configuration()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            partition_columns: metadata.partition_columns().to_vec(),
            comment: metadata.description().map(str::to_owned),
        }))
    }

    /// Commits schema, table properties, comment and catalog history atomically.
    ///
    /// The pinned Kernel has no ALTER property setter. A supported Committer
    /// extension augments its generated metadata before publication and validates
    /// the final metadata/protocol pair; no committed log file is rewritten.
    ///
    /// # Errors
    /// Rejects stale versions, breaking evolution, invalid metadata, uncertain I/O,
    /// and concurrent commits. Callers must reread before retrying a conflict.
    pub fn commit_catalog(
        &self,
        expected_version: Option<u64>,
        schema: SchemaRef,
        partition_columns: Vec<String>,
        properties: BTreeMap<String, String>,
        comment: Option<String>,
    ) -> Result<CatalogDeltaSnapshot> {
        let _guard = self
            .commit_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("catalog commit lock poisoned"))?;
        let current = self.catalog_snapshot()?;
        if current.as_ref().map(|snapshot| snapshot.version) != expected_version {
            bail!("catalog Delta version conflict; reread before retry");
        }
        let canonical = normalize::schema(&schema)?;
        let committer = Box::new(MetadataCommitter {
            inner: self.committer(),
            properties: properties.clone(),
            comment: comment.clone(),
        });
        if let Some(current) = &current {
            if current.partition_columns != partition_columns {
                bail!("catalog partition columns cannot evolve");
            }
            if canonical.fields().len() < current.schema.fields().len()
                || canonical.fields()[..current.schema.fields().len()] != current.schema.fields()[..]
                || canonical.fields()[current.schema.fields().len()..]
                    .iter()
                    .any(|field| !field.is_nullable())
            {
                bail!("catalog evolution must preserve all fields and append nullable columns");
            }
            if *canonical == *current.schema && properties == current.properties && comment == current.comment {
                return Ok(current.clone());
            }
            let snapshot = self.snapshot()?;
            let additions = canonical.fields()[current.schema.fields().len()..]
                .iter()
                .map(|field| StructField::try_from_arrow(field.as_ref()).map_err(delta_kernel::Error::from))
                .collect::<DeltaResult<Vec<_>>>()?;
            if additions.is_empty() {
                let transaction = snapshot.transaction(committer, &self.engine)?;
                commit_result(transaction.commit(&self.engine)?)?;
            } else {
                let mut additions = additions.into_iter();
                let mut builder = snapshot
                    .alter_table()
                    .add_column(additions.next().context("missing addition")?);
                for field in additions {
                    builder = builder.add_column(field);
                }
                commit_result(builder.build(&self.engine, committer)?.commit(&self.engine)?)?;
            }
        } else {
            if self.url.scheme() == "file" {
                for entry in std::fs::read_dir(&self.root)? {
                    let entry = entry?;
                    if entry.file_name() != ".arrow-deltalake-writer.lock" {
                        bail!("refusing to adopt a non-Delta directory {}", self.root.display());
                    }
                }
            }
            let schema = Arc::new(StructType::try_from_arrow(canonical)?);
            let mut builder =
                delta_kernel::transaction::create_table::create_table(self.url.as_str(), schema, "arrowcol-catalog")
                    .with_table_properties(properties);
            if !partition_columns.is_empty() {
                builder = builder.with_data_layout(delta_kernel::transaction::data_layout::DataLayout::partitioned(
                    partition_columns,
                ));
            }
            commit_result(builder.build(&self.engine, committer)?.commit(&self.engine)?)?;
        }
        self.catalog_snapshot()?
            .context("catalog commit did not create a readable Delta head")
    }
}

fn commit_result<S>(result: CommitResult<S>) -> Result<()> {
    match result {
        CommitResult::Committed(_) => Ok(()),
        CommitResult::Conflicted(_) => bail!("catalog Delta commit conflict; rebuild from fresh metadata"),
        CommitResult::Retryable(retry) => bail!("catalog Delta commit uncertain; reread before retry: {}", retry.error),
    }
}

struct MetadataCommitter {
    inner: Box<dyn Committer>,
    properties: BTreeMap<String, String>,
    comment: Option<String>,
}

impl Committer for MetadataCommitter {
    fn commit(
        &self,
        engine: &dyn Engine,
        actions: DeltaResultIterator<'_, FilteredEngineData>,
        metadata: CommitMetadata,
    ) -> DeltaResult<CommitResponse> {
        let original = metadata.effective_metadata()?;
        let final_metadata = Metadata::from_parts(
            original.id().to_owned(),
            original.name().map(str::to_owned),
            self.comment.clone(),
            original.format_provider().to_owned(),
            original.format_options().clone(),
            original.schema_string().clone(),
            original.partition_columns().to_vec(),
            original.created_time(),
            self.properties
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        );
        let validated = TableConfiguration::try_new(
            final_metadata.clone(),
            metadata.effective_protocol()?.clone(),
            metadata.table_root().clone(),
            metadata.version(),
        )?;
        validated.ensure_operation_supported(delta_kernel::table_features::Operation::Scan)?;
        validated.ensure_operation_supported(delta_kernel::table_features::Operation::Write)?;
        let serialized = serde_json::to_value(&final_metadata)?;
        let mut prepared = Vec::new();
        let mut found_metadata = false;
        let mut bytes = 0_usize;
        for action in actions {
            let batch = action?.apply_selection_vector()?.try_into_record_batch()?;
            let mut writer = arrow_json::LineDelimitedWriter::new(Vec::new());
            writer.write(&batch).map_err(delta_kernel::Error::from)?;
            writer.finish().map_err(delta_kernel::Error::from)?;
            let json = writer.into_inner();
            let mut lines = Vec::new();
            for row in json.split(|byte| *byte == b'\n').filter(|row| !row.is_empty()) {
                let mut row: serde_json::Value = serde_json::from_slice(row)?;
                if row.get("metaData").is_some_and(|value| !value.is_null()) {
                    if found_metadata {
                        return Err(delta_kernel::Error::generic("multiple catalog metadata actions"));
                    }
                    row["metaData"] = serialized.clone();
                    found_metadata = true;
                }
                serde_json::to_writer(&mut lines, &row)?;
                lines.push(b'\n');
            }
            bytes = bytes
                .checked_add(lines.len())
                .ok_or_else(|| delta_kernel::Error::generic("catalog action size overflow"))?;
            prepared.extend(decode_actions(batch.schema(), lines)?);
        }
        if !found_metadata {
            let mut line = serde_json::to_vec(&serde_json::json!({"metaData": serialized}))?;
            line.push(b'\n');
            bytes = bytes
                .checked_add(line.len())
                .ok_or_else(|| delta_kernel::Error::generic("catalog action size overflow"))?;
            let schema = Arc::new(
                delta_kernel::actions::get_all_actions_schema()
                    .as_ref()
                    .try_into_arrow()?,
            );
            prepared.extend(decode_actions(schema, line)?);
        }
        if bytes > 8 * 1024 * 1024 {
            return Err(delta_kernel::Error::generic("catalog metadata exceeds 8 MiB limit"));
        }
        self.inner.commit(engine, Box::new(prepared.into_iter()), metadata)
    }

    fn is_catalog_committer(&self) -> bool {
        self.inner.is_catalog_committer()
    }

    fn publish(&self, engine: &dyn Engine, metadata: PublishMetadata) -> DeltaResult<()> {
        self.inner.publish(engine, metadata)
    }
}

fn decode_actions(schema: SchemaRef, json: Vec<u8>) -> DeltaResult<Vec<DeltaResult<FilteredEngineData>>> {
    let reader = arrow_json::ReaderBuilder::new(schema).build(Cursor::new(json))?;
    reader
        .map(|batch| {
            let batch: RecordBatch = batch?;
            Ok(Ok(FilteredEngineData::with_all_rows_selected(Box::new(
                ArrowEngineData::new(batch),
            ))))
        })
        .collect()
}
