use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use arrow_deltalake::delta::{INGESTION_PARTITION, object_store_for_url};
use delta_kernel::Snapshot;
use delta_kernel::scan::state::ScanFile;
use delta_kernel_default_engine::DefaultEngineBuilder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, ObjectStoreScheme};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use sha2::{Digest, Sha256};
use url::Url;

pub const MAX_ROWS: usize = 100_000;
const MAX_FILES: usize = 10_000;
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;

pub type Record = (i64, i64, String);

#[derive(Clone, Debug, Serialize)]
pub struct ActiveFile {
    pub path: String,
    pub bytes: u64,
    pub partition: String,
}

#[derive(Default)]
struct Capture {
    files: Vec<ActiveFile>,
    bytes: u64,
    error: Option<anyhow::Error>,
}

fn collect_file(capture: &mut Capture, file: ScanFile) {
    if capture.error.is_some() {
        return;
    }
    let result = (|| {
        ensure!(
            !file.dv_info.has_vector(),
            "deletion vectors are not experiment fixtures"
        );
        ensure!(capture.files.len() < MAX_FILES, "file budget exceeded");
        ensure!(file.path.len() <= 1024, "file path budget exceeded");
        let bytes = u64::try_from(file.size)?;
        ensure!(bytes <= MAX_FILE_BYTES, "file byte budget exceeded");
        capture.bytes = capture.bytes.checked_add(bytes).context("byte count overflow")?;
        ensure!(capture.bytes <= MAX_TOTAL_BYTES, "total byte budget exceeded");
        let partition = file
            .partition_values
            .get(INGESTION_PARTITION)
            .context("missing minute Hive partition")?
            .clone();
        ensure!(partition.len() == 16, "expected minute partition label");
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
}

pub fn fingerprint(records: &[Record]) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(records)?)))
}

fn capture(url: &Url, store: Arc<dyn ObjectStore>) -> Result<(u64, Vec<ActiveFile>)> {
    let engine = DefaultEngineBuilder::new(store).build();
    let snapshot = Snapshot::builder_for(url.as_str()).build(&engine)?;
    ensure!(
        snapshot.table_configuration().metadata().partition_columns() == [INGESTION_PARTITION],
        "only ingestion-partitioned experiment tables are supported"
    );
    let version = snapshot.version();
    let scan = snapshot.scan_builder().build()?;
    let mut captured = Capture::default();
    for metadata in scan.scan_metadata(&engine)? {
        captured = metadata?.visit_scan_files(captured, collect_file)?;
        if let Some(error) = captured.error.take() {
            return Err(error);
        }
    }
    captured.files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok((version, captured.files))
}

#[derive(Serialize)]
pub struct Inspection {
    pub action: &'static str,
    pub table: String,
    pub version: u64,
    pub files: Vec<ActiveFile>,
    pub rows: usize,
    pub fingerprint: String,
    pub records: Vec<Record>,
}

pub async fn inspect(url: Url, table: String) -> Result<Inspection> {
    let store = object_store_for_url(&url)?;
    let capture_url = url.clone();
    let capture_store = Arc::clone(&store);
    let (version, files) = tokio::task::spawn_blocking(move || capture(&capture_url, capture_store)).await??;
    let (_, prefix) = ObjectStoreScheme::parse(&url)?;
    let mut records = Vec::new();
    for file in &files {
        let joined = url.join(&file.path)?;
        ensure!(
            !file.path.starts_with('/')
                && !file.path.contains('\\')
                && joined.as_str().starts_with(url.as_str())
                && joined.query().is_none()
                && joined.fragment().is_none(),
            "active data path escapes table"
        );
        let relative = Path::from_url_path(&file.path)?;
        let location = relative.parts().fold(prefix.clone(), |prefix, part| prefix.join(part));
        let object = store.get(&location).await?;
        ensure!(object.meta.size == file.bytes, "active file size differs from Add");
        let bytes = object.bytes().await?;
        let partition = file.partition.clone();
        let remaining = MAX_ROWS.saturating_sub(records.len());
        let mut batch_records = tokio::task::spawn_blocking(move || -> Result<Vec<Record>> {
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
            ensure!(
                builder.schema().fields().len() == 2
                    && builder.schema().field(0).name() == "id"
                    && builder.schema().field(1).name() == "value"
                    && builder
                        .schema()
                        .fields()
                        .iter()
                        .all(|field| field.data_type() == &DataType::Int64),
                "physical schema is not an experiment fixture"
            );
            ensure!(
                usize::try_from(builder.metadata().file_metadata().num_rows())? <= remaining,
                "oracle row budget exceeded before decoding"
            );
            let reader = builder.with_batch_size(1024).build()?;
            let mut rows = Vec::new();
            for batch in reader {
                let batch = batch?;
                let ids = batch
                    .column_by_name("id")
                    .context("missing id")?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .context("id must be int64")?;
                let values = batch
                    .column_by_name("value")
                    .context("missing value")?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .context("value must be int64")?;
                ensure!(ids.null_count() == 0 && values.null_count() == 0, "null fixture data");
                ensure!(rows.len() + batch.num_rows() <= remaining, "oracle row budget exceeded");
                for index in 0..batch.num_rows() {
                    rows.push((ids.value(index), values.value(index), partition.clone()));
                }
            }
            Ok(rows)
        })
        .await??;
        records.append(&mut batch_records);
    }
    records.sort();
    Ok(Inspection {
        action: "inspect",
        table,
        version,
        files,
        rows: records.len(),
        fingerprint: fingerprint(&records)?,
        records,
    })
}
