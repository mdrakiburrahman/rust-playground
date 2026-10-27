//! DataFusion predicate/projection pushdown over explicitly committed files.

use crate::source::{BootstrapManifest, DeltaSource, SourceFile};
use anyhow::{Context, Result, ensure};
use arrow::datatypes::Schema;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext, col, lit};
use futures::StreamExt;
use object_store::ObjectStoreExt;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct ReadMetrics {
    pub files_considered: u64,
    pub rows_returned: u64,
    /// Actual execution-plan counters, not estimates from file size/row count.
    pub datafusion: BTreeMap<String, usize>,
}

impl ReadMetrics {
    pub fn merge(&mut self, other: Self) {
        self.files_considered += other.files_considered;
        self.rows_returned += other.rows_returned;
        for (name, value) in other.datafusion {
            *self.datafusion.entry(name).or_default() += value;
        }
    }

    fn collect_plan(&mut self, plan: &Arc<dyn ExecutionPlan>) {
        if let Some(metrics) = plan.metrics() {
            for metric in metrics.iter() {
                let name = metric.value().name().to_string();
                match metric.value() {
                    MetricValue::PruningMetrics { pruning_metrics, .. } => {
                        *self.datafusion.entry(name.replace("_pruned", "_matched")).or_default() +=
                            pruning_metrics.matched();
                        *self.datafusion.entry(name).or_default() += pruning_metrics.pruned();
                    }
                    MetricValue::Ratio { .. } | MetricValue::StartTimestamp(_) | MetricValue::EndTimestamp(_) => {}
                    value => *self.datafusion.entry(name).or_default() += value.as_usize(),
                }
            }
        }
        for child in plan.children() {
            self.collect_plan(child);
        }
    }
}

/// One-customer filter plus optional stateless projection.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TenantQuery {
    pub customer_id: String,
    pub projection: Vec<String>,
}

impl TenantQuery {
    /// Resolve projection, retaining partition columns and the customer key.
    ///
    /// # Errors
    /// Rejects missing/mistyped customer keys, duplicate/unknown columns, and
    /// projections dropping the key or a source partition column.
    pub fn columns(&self, frozen: &BootstrapManifest) -> Result<Vec<String>> {
        ensure!(!self.customer_id.is_empty(), "customer predicate cannot be empty");
        let schema = frozen.schema()?;
        let key = schema.field_with_name("customer_id")?;
        ensure!(
            key.data_type() == &arrow::datatypes::DataType::Utf8,
            "customer_id must be Utf8"
        );
        let columns = if self.projection.is_empty() {
            schema.fields().iter().map(|field| field.name().to_owned()).collect()
        } else {
            self.projection.clone()
        };
        let mut unique = BTreeSet::new();
        for name in &columns {
            schema
                .index_of(name)
                .with_context(|| format!("unknown projection column {name}"))?;
            ensure!(unique.insert(name.as_str()), "duplicate projection column {name}");
        }
        ensure!(unique.contains("customer_id"), "projection must retain customer_id");
        for name in frozen.partition_columns()? {
            ensure!(
                unique.contains(name.as_str()),
                "projection must retain source partition column {name}"
            );
        }
        Ok(columns)
    }
}

/// Backpressured batch destination; called sequentially.
#[async_trait::async_trait]
pub trait BatchConsumer: Send {
    /// # Errors
    /// Returns downstream write/backpressure failures.
    async fn consume(&mut self, batch: arrow::array::RecordBatch) -> Result<()>;
}

/// Stream selected Arrow batches without collecting a source file in memory.
///
/// # Errors
/// Returns missing source files, unsupported partition types, DataFusion query,
/// or downstream backpressure/write errors. No output checkpoint is advanced.
pub async fn read_file(
    source: &DeltaSource,
    frozen: &BootstrapManifest,
    file: &SourceFile,
    query: &TenantQuery,
    consume: &mut dyn BatchConsumer,
) -> Result<ReadMetrics> {
    let location = source.file_location(&file.path)?;
    let object = source
        .store
        .head(&location)
        .await
        .context("required committed source data is missing; refusing to advance checkpoint")?;
    ensure!(object.size == file.size, "committed source file size changed");
    let columns = query.columns(frozen)?;
    let schema = frozen.schema()?;
    let projected_schema = Arc::new(
        schema.project(
            &columns
                .iter()
                .map(|name| schema.index_of(name))
                .collect::<arrow::error::Result<Vec<_>>>()?,
        )?,
    );
    let partition_columns = frozen.partition_columns()?;
    ensure!(
        file.partition_values.len() == partition_columns.len(),
        "Add partition values do not match source schema"
    );
    let physical = Schema::new(
        schema
            .fields()
            .iter()
            .filter(|field| !partition_columns.contains(field.name()))
            .cloned()
            .collect::<Vec<_>>(),
    );
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(8_192)
        .set_bool("datafusion.execution.parquet.pruning", true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.reorder_filters", true)
        .set_bool("datafusion.execution.parquet.enable_page_index", true)
        .set_bool("datafusion.execution.parquet.bloom_filter_on_read", true)
        .set_bool("datafusion.execution.parquet.schema_force_view_types", false);
    let context = SessionContext::new_with_config(config);
    context.register_object_store(&source.url, Arc::clone(&source.store));
    let url = source.file_url(&file.path)?;
    let mut dataframe = context
        .read_parquet(url.as_str(), ParquetReadOptions::default().schema(&physical))
        .await
        .with_context(|| {
            format!(
                "read committed source file {}; required data may have been vacuumed",
                file.path
            )
        })?;
    for name in &partition_columns {
        let value = file
            .partition_values
            .get(name)
            .context("missing declared partition value")?;
        let data_type = schema.field_with_name(name)?.data_type();
        let scalar = match value {
            Some(value) => ScalarValue::try_from_string(value.to_owned(), data_type)?,
            None => ScalarValue::try_from(data_type)?,
        };
        dataframe = dataframe.with_column(name, lit(scalar))?;
    }
    let refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
    let dataframe = dataframe
        .filter(col("customer_id").eq(lit(query.customer_id.clone())))?
        .select_columns(&refs)?;
    let plan = dataframe.create_physical_plan().await?;
    let mut stream = execute_stream(Arc::clone(&plan), context.task_ctx())?;
    let mut metrics = ReadMetrics {
        files_considered: 1,
        ..ReadMetrics::default()
    };
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let batch = arrow::array::RecordBatch::try_new(Arc::clone(&projected_schema), batch.columns().to_vec())?;
        metrics.rows_returned += batch.num_rows() as u64;
        consume.consume(batch).await?;
    }
    let object = source
        .store
        .head(&location)
        .await
        .context("committed source data disappeared during the scan; refusing to advance checkpoint")?;
    ensure!(
        object.size == file.size,
        "committed source file size changed during the scan"
    );
    metrics.collect_plan(&plan);
    Ok(metrics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::BootstrapManifest;
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field};
    use arrow_deltalake::layout::{FileSortBuffer, LayoutPolicy};
    use parquet::arrow::ArrowWriter;
    use serde_json::json;

    #[derive(Default)]
    struct Collected(Vec<RecordBatch>);

    #[async_trait::async_trait]
    impl BatchConsumer for Collected {
        async fn consume(&mut self, batch: RecordBatch) -> Result<()> {
            self.0.push(batch);
            Ok(())
        }
    }

    #[test]
    fn projection_cannot_drop_customer_or_partition_identity() -> Result<()> {
        let frozen = BootstrapManifest {
            source_url: "file:///source/".into(),
            table_id: "id".into(),
            version: 0,
            metadata: json!({"schemaString":r#"{"type":"struct","fields":[{"name":"customer_id","type":"string","nullable":false,"metadata":{}},{"name":"minute","type":"string","nullable":false,"metadata":{}}]}"#,"partitionColumns":["minute"]}),
            protocol: json!({}),
            files: Vec::new(),
        };
        assert_eq!(
            TenantQuery {
                customer_id: "a".into(),
                projection: vec![]
            }
            .columns(&frozen)?,
            vec!["customer_id", "minute"]
        );
        assert!(
            TenantQuery {
                customer_id: "a".into(),
                projection: vec!["customer_id".into()]
            }
            .columns(&frozen)
            .is_err()
        );
        assert!(
            TenantQuery {
                customer_id: "a".into(),
                projection: vec!["minute".into()]
            }
            .columns(&frozen)
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn measured_sorted_predicate_skips_row_groups_and_bytes_without_cross_customer_rows() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let rows = 65_536;
        let customers = (0..rows)
            .map(|row| format!("customer-{:04}", row % 16))
            .collect::<Vec<_>>();
        let payloads = (0..rows)
            .map(|row| format!("payload-{row:08}-{}", "x".repeat(80)))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(customers)),
                Arc::new(Int32Array::from((0..rows).collect::<Vec<_>>())),
                Arc::new(StringArray::from(payloads)),
            ],
        )?;
        let policy = LayoutPolicy {
            sort_columns: vec!["customer_id".into(), "value".into()],
            max_input_bytes: 128 * 1024 * 1024,
            row_group_rows: 1_024,
            page_rows: 256,
            ..LayoutPolicy::default()
        };
        let properties = policy.writer_properties(&schema)?;
        let mut buffer = FileSortBuffer::new(Arc::clone(&schema), policy)?;
        for offset in (0..rows as usize).step_by(8_192) {
            assert!(buffer.try_push(&batch.slice(offset, 8_192))?);
        }
        let sorted = buffer.finish()?;
        for (name, data) in [("sorted.parquet", &sorted), ("unsorted.parquet", &batch)] {
            let mut writer = ArrowWriter::try_new(
                std::fs::File::create(directory.path().join(name))?,
                Arc::clone(&schema),
                Some(properties.clone()),
            )?;
            writer.write(data)?;
            writer.close()?;
        }
        let page_policy = LayoutPolicy {
            row_group_rows: rows as usize,
            page_rows: 1_024,
            ..LayoutPolicy::default()
        };
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(directory.path().join("page-sorted.parquet"))?,
            Arc::clone(&schema),
            Some(page_policy.writer_properties(&schema)?),
        )?;
        writer.write(&sorted)?;
        writer.close()?;
        let url = url::Url::from_directory_path(directory.path()).map_err(|()| anyhow::anyhow!("invalid test URL"))?;
        let source = DeltaSource::open(url)?;
        let frozen = BootstrapManifest {
            source_url: source.url.to_string(),
            table_id: "fixture".into(),
            version: 0,
            metadata: json!({"schemaString":r#"{"type":"struct","fields":[{"name":"customer_id","type":"string","nullable":false,"metadata":{}},{"name":"value","type":"integer","nullable":false,"metadata":{}},{"name":"payload","type":"string","nullable":false,"metadata":{}}]}"#,"partitionColumns":[]}),
            protocol: json!({}),
            files: Vec::new(),
        };
        let file = |name: &str| -> Result<SourceFile> {
            Ok(SourceFile {
                path: name.into(),
                size: std::fs::metadata(directory.path().join(name))?.len(),
                partition_values: BTreeMap::new(),
            })
        };
        let query = TenantQuery {
            customer_id: "customer-0003".into(),
            projection: vec!["customer_id".into(), "value".into()],
        };
        let mut output = Collected::default();
        let sorted_metrics = read_file(&source, &frozen, &file("sorted.parquet")?, &query, &mut output).await?;
        assert_eq!(sorted_metrics.rows_returned, 4_096);
        let mut actual = Vec::new();
        for result in &output.0 {
            assert_eq!(result.num_columns(), 2);
            let keys = result
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("key type")?;
            let values = result
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("value type")?;
            for row in 0..result.num_rows() {
                assert_eq!(keys.value(row), query.customer_id);
                actual.push(values.value(row));
            }
        }
        actual.sort_unstable();
        assert_eq!(actual, (0..rows).filter(|row| row % 16 == 3).collect::<Vec<_>>());
        let unsorted_metrics = read_file(
            &source,
            &frozen,
            &file("unsorted.parquet")?,
            &query,
            &mut Collected::default(),
        )
        .await?;
        assert_eq!(unsorted_metrics.rows_returned, sorted_metrics.rows_returned);
        let missing = TenantQuery {
            customer_id: "absent-customer".into(),
            projection: query.projection.clone(),
        };
        let missing_metrics = read_file(
            &source,
            &frozen,
            &file("sorted.parquet")?,
            &missing,
            &mut Collected::default(),
        )
        .await?;
        assert_eq!(missing_metrics.rows_returned, 0);
        let bloom_missing = TenantQuery {
            customer_id: "customer-0003-absent".into(),
            projection: query.projection.clone(),
        };
        let bloom_metrics = read_file(
            &source,
            &frozen,
            &file("unsorted.parquet")?,
            &bloom_missing,
            &mut Collected::default(),
        )
        .await?;
        assert_eq!(bloom_metrics.rows_returned, 0);
        assert!(
            bloom_metrics
                .datafusion
                .get("row_groups_pruned_bloom_filter")
                .copied()
                .unwrap_or(0)
                > 0,
            "Bloom filter must actually prune an in-range absent key: {bloom_metrics:?}"
        );
        let page_metrics = read_file(
            &source,
            &frozen,
            &file("page-sorted.parquet")?,
            &query,
            &mut Collected::default(),
        )
        .await?;
        assert_eq!(page_metrics.rows_returned, 4_096);
        assert!(
            page_metrics
                .datafusion
                .get("page_index_rows_pruned")
                .copied()
                .unwrap_or(0)
                > 0,
            "page indexes must actually prune rows within one large row group: {page_metrics:?}"
        );

        let context = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let dataframe = context
            .read_parquet(
                source.file_url("sorted.parquet")?.as_str(),
                ParquetReadOptions::default(),
            )
            .await?
            .select_columns(&["customer_id", "value"])?;
        let plan = dataframe.create_physical_plan().await?;
        let mut stream = execute_stream(Arc::clone(&plan), context.task_ctx())?;
        let mut full_rows = 0;
        while let Some(batch) = stream.next().await {
            full_rows += batch?.num_rows();
        }
        assert_eq!(full_rows, rows as usize);
        let mut full_metrics = ReadMetrics {
            files_considered: 1,
            rows_returned: full_rows as u64,
            ..ReadMetrics::default()
        };
        full_metrics.collect_plan(&plan);
        let bytes = |metrics: &ReadMetrics| metrics.datafusion.get("bytes_scanned").copied().unwrap_or(0);
        assert!(bytes(&full_metrics) > 0, "DataFusion bytes counter must be present");
        assert!(
            bytes(&sorted_metrics) < bytes(&unsorted_metrics),
            "sorted={sorted_metrics:?}; unsorted={unsorted_metrics:?}"
        );
        assert!(
            bytes(&sorted_metrics) < bytes(&full_metrics),
            "sorted={sorted_metrics:?}; full={full_metrics:?}"
        );
        assert!(
            sorted_metrics
                .datafusion
                .get("row_groups_pruned_statistics")
                .copied()
                .unwrap_or(0)
                > 0,
            "must observe actual row-group pruning: {sorted_metrics:?}"
        );
        println!(
            "READ_EFFICIENCY {}",
            serde_json::to_string(&json!({
                "rows":rows,"customers":16,"rowGroupRows":1024,
                "sortedFiltered":sorted_metrics,"unsortedFiltered":unsorted_metrics,
                "sortedUnfiltered":full_metrics,"negativePredicate":missing_metrics,
                "bloomInRangeAbsent":bloom_metrics,"singleRowGroupPageIndex":page_metrics,
            }))?
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconstructs_partitions_from_add_values_and_preserves_logical_schema() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let physical = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let data = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )?;
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(directory.path().join("no-directory-partition.parquet"))?,
            physical,
            None,
        )?;
        writer.write(&data)?;
        writer.close()?;
        let source =
            DeltaSource::open(url::Url::from_directory_path(directory.path()).map_err(|()| anyhow::anyhow!("URL"))?)?;
        let frozen = BootstrapManifest {
            source_url: source.url.to_string(),
            table_id: "fixture".into(),
            version: 0,
            metadata: json!({"schemaString":r#"{"type":"struct","fields":[{"name":"customer_id","type":"string","nullable":false,"metadata":{}},{"name":"value","type":"integer","nullable":false,"metadata":{}},{"name":"ingestion_minute","type":"string","nullable":true,"metadata":{}}]}"#,"partitionColumns":["ingestion_minute"]}),
            protocol: json!({}),
            files: Vec::new(),
        };
        let query = TenantQuery {
            customer_id: "a".into(),
            projection: Vec::new(),
        };
        for value in [Some("2026-10-27T02:25Z".to_owned()), None] {
            let file = SourceFile {
                path: "no-directory-partition.parquet".into(),
                size: std::fs::metadata(directory.path().join("no-directory-partition.parquet"))?.len(),
                partition_values: BTreeMap::from([("ingestion_minute".into(), value.clone())]),
            };
            let mut output = Collected::default();
            let metrics = read_file(&source, &frozen, &file, &query, &mut output).await?;
            assert_eq!(metrics.rows_returned, 1);
            assert_eq!(output.0[0].schema(), frozen.schema()?);
            let partition = output.0[0]
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("partition type")?;
            if let Some(value) = &value {
                assert_eq!(partition.value(0), value);
            } else {
                use arrow::array::Array;
                assert!(partition.is_null(0));
            }
        }
        Ok(())
    }
}
