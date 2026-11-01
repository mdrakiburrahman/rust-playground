use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use arrow_deltalake::delta::{INGESTION_PARTITION, object_store_for_url};
use delta_kernel::Snapshot;
use delta_kernel::scan::state::ScanFile;
use delta_kernel_default_engine::DefaultEngineBuilder;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;

use crate::bulk::{self, MAX_FILES, MAX_TOTAL_BYTES, MIB};
use crate::oracle::ActiveFile;

const MAX_FILE_BYTES: u64 = 512 * MIB;
const MAX_METADATA_BYTES: u32 = 1024 * 1024;
const MAX_PROJECTED_BYTES: u64 = 128 * MIB;
const MAX_ROWS: u64 = 1_000_000;

#[derive(Default)]
struct Capture {
    files: Vec<ActiveFile>,
    bytes: u64,
    error: Option<anyhow::Error>,
}

fn collect(mut capture: Capture, file: ScanFile) -> Capture {
    if capture.error.is_some() {
        return capture;
    }
    let result = (|| {
        ensure!(
            !file.dv_info.has_vector(),
            "scale fixtures do not support deletion vectors"
        );
        ensure!(
            capture.files.len() < MAX_FILES && file.path.len() <= 1024,
            "inventory file bound"
        );
        let bytes = u64::try_from(file.size)?;
        ensure!((8..=MAX_FILE_BYTES).contains(&bytes), "QAP file byte bound");
        capture.bytes = capture.bytes.checked_add(bytes).context("inventory byte overflow")?;
        ensure!(capture.bytes <= MAX_TOTAL_BYTES, "inventory total byte bound");
        let partition = file
            .partition_values
            .get(INGESTION_PARTITION)
            .context("partition missing")?
            .clone();
        ensure!(partition.len() == 16, "expected minute partition");
        capture.files.push(ActiveFile {
            path: file.path,
            bytes,
            partition,
        });
        Ok(())
    })();
    if let Err(error) = result {
        capture.error = Some(error);
    }
    capture
}

pub async fn inventory(url: Url, store: Arc<dyn ObjectStore>) -> Result<(u64, Vec<ActiveFile>)> {
    tokio::task::spawn_blocking(move || {
        let engine = DefaultEngineBuilder::new(store).build();
        let snapshot = Snapshot::builder_for(url.as_str()).build(&engine)?;
        ensure!(
            snapshot.table_configuration().metadata().partition_columns() == [INGESTION_PARTITION],
            "scale fixture partition schema mismatch"
        );
        let version = snapshot.version();
        let scan = snapshot.scan_builder().build()?;
        let mut captured = Capture::default();
        for metadata in scan.scan_metadata(&engine)? {
            captured = metadata?.visit_scan_files(captured, |capture, file| {
                let old = std::mem::take(capture);
                *capture = collect(old, file);
            })?;
            if let Some(error) = captured.error.take() {
                return Err(error);
            }
        }
        captured.files.sort_by(|left, right| left.path.cmp(&right.path));
        for file in &captured.files {
            bulk::location(&url, &file.path)?;
        }
        Ok((version, captured.files))
    })
    .await?
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Group {
    partition: String,
    id: i64,
    value: i64,
    payload_sha256: String,
    multiplicity: u64,
}

pub async fn inspect(url: Url, table: String) -> Result<Value> {
    let store = object_store_for_url(&url)?;
    let manifest = bulk::read_manifest(&url, &store).await?;
    ensure!(manifest.table == table, "foreign manifest");
    let (version, files) = inventory(url.clone(), Arc::clone(&store)).await?;
    let partitions = manifest.options.partitions()?;
    let expected_partitions = partitions
        .iter()
        .map(|partition| partition.partition.as_str())
        .collect::<Vec<_>>();
    let mut groups = BTreeMap::<(String, i64, i64), u64>::new();
    let mut rows = 0_u64;
    let mut projected_bytes = 0_u64;
    for file in &files {
        ensure!(
            expected_partitions.contains(&file.partition.as_str()),
            "foreign partition"
        );
        let path = bulk::location(&url, &file.path)?;
        let before = store.head(&path).await?;
        ensure!(
            before.size == file.bytes && before.e_tag.is_some(),
            "Add/object mismatch"
        );
        let footer = store.get_range(&path, file.bytes - 8..file.bytes).await?;
        ensure!(&footer[4..] == b"PAR1", "invalid Parquet footer");
        let length = u32::from_le_bytes(footer[..4].try_into()?);
        ensure!(
            length <= MAX_METADATA_BYTES && u64::from(length) + 8 <= file.bytes,
            "Parquet metadata bound"
        );
        let reader = ParquetObjectReader::new(Arc::clone(&store), path.clone()).with_file_size(file.bytes);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        ensure!(
            builder.schema().fields().len() == 3
                && builder.schema().field(0).name() == "id"
                && builder.schema().field(0).data_type() == &DataType::Int64
                && builder.schema().field(1).name() == "value"
                && builder.schema().field(1).data_type() == &DataType::Int64
                && builder.schema().field(2).name() == "payload"
                && builder.schema().field(2).data_type() == &DataType::Utf8,
            "scale physical schema mismatch (partition columns must not be physically copied)"
        );
        let file_rows = u64::try_from(builder.metadata().file_metadata().num_rows())?;
        ensure!(
            rows.checked_add(file_rows).is_some_and(|count| count <= MAX_ROWS),
            "scale row bound"
        );
        for row_group in builder.metadata().row_groups() {
            for index in 0..2 {
                projected_bytes = projected_bytes
                    .checked_add(u64::try_from(row_group.column(index).compressed_size())?)
                    .context("projected byte overflow")?;
            }
        }
        ensure!(projected_bytes <= MAX_PROJECTED_BYTES, "projected key byte budget");
        let projection = ProjectionMask::roots(builder.parquet_schema(), [0, 1]);
        let mut stream = builder.with_projection(projection).with_batch_size(1024).build()?;
        let mut observed = 0_u64;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("id type")?;
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("value type")?;
            ensure!(ids.null_count() == 0 && values.null_count() == 0, "null fixture keys");
            for index in 0..batch.num_rows() {
                let id = ids.value(index);
                let value = values.value(index);
                ensure!(
                    id >= 0 && id < manifest.seed_rows as i64 && value == id * 10 + 7,
                    "unexpected seed key/value"
                );
                let count = groups.entry((file.partition.clone(), id, value)).or_default();
                *count = count.checked_add(1).context("multiplicity overflow")?;
            }
            observed += batch.num_rows() as u64;
        }
        ensure!(observed == file_rows, "Parquet row count mismatch");
        rows += observed;
        let after = store.head(&path).await?;
        ensure!(
            before.e_tag == after.e_tag && before.size == after.size,
            "file changed during inspection"
        );
    }
    let current_version = inventory(url.clone(), Arc::clone(&store)).await?.0;
    ensure!(current_version == version, "snapshot changed during inspection");
    let expected_groups = groups
        .into_iter()
        .map(|((partition, id, value), multiplicity)| Group {
            partition,
            id,
            value,
            payload_sha256: manifest.payload_sha256[id as usize].clone(),
            multiplicity,
        })
        .collect::<Vec<_>>();
    let fingerprint = format!("{:x}", Sha256::digest(serde_json::to_vec(&expected_groups)?));
    Ok(json!({
        "action":"inspect-scale","table":table,"version":version,"rows":rows,
        "fingerprint":fingerprint,"totalBytes":files.iter().map(|file| file.bytes).sum::<u64>(),
        "files":files,"expectedGroups":expected_groups,"seedRows":manifest.seed_rows,
        "payloadBytes":bulk::PAYLOAD_BYTES,"projectedKeyBytes":projected_bytes,
        "seed":{"path":manifest.source,"bytes":manifest.source_bytes,"etag":manifest.source_etag,
            "rowGroups":manifest.seed_row_groups,"maxRowGroupBytes":manifest.seed_max_row_group_bytes,
            "rowGroupRows":32,"compression":"UNCOMPRESSED"},
        "supportedShape":"Deterministic seed copies; arbitrary OPTIMIZE and whole-partition PURGE. Keys are read exactly; payload digests derive from verified immutable seed truth and must be checked in Kusto.",
    }))
}
