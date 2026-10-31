use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::{DeltaTable, INGESTION_PARTITION, ingestion_partition_value};
use arrow_deltalake::maintenance::{Granularity, MaintenanceControl, Outcome, PurgeConfig};
use arrow_deltalake::publisher::{MutationRetries, PublisherSession};
use arrowcol_buffer::CloudGeneration;
use clap::Args;
use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::Scalar;
use delta_kernel::transaction::CommitResult;
use delta_kernel::{FileMeta, Snapshot};
use delta_kernel_default_engine::parquet::DataFileMetadata;
use delta_kernel_default_engine::{DefaultEngineBuilder, build_add_file_metadata};
use futures::stream::{BoxStream, StreamExt};
use object_store::azure::{AzureAuthorizer, AzureCredential, AzureCredentialProvider, MicrosoftAzureBuilder};
use object_store::client::{HttpClient, HttpConnector, HttpRequest, HttpRequestBody, HttpResponse, ReqwestConnector};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt,
    ObjectStoreScheme, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;

use crate::{Cli, Stop, scale_oracle};

pub const MIB: u64 = 1024 * 1024;
pub const PAYLOAD_BYTES: usize = 65_536;
pub const MANIFEST: &str = "_scale/manifest.json";
pub const MAX_TOTAL_BYTES: u64 = 8 * 1024 * MIB;
pub const MAX_FILES: usize = 1024;
/// Tombstone PURGE retention: removes the expired (anchor - 3d) partition and keeps the rest.
const TOMBSTONE_RETENTION_MS: i64 = 86_400_000;

#[derive(Args, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SeedOptions {
    #[arg(long)]
    pub anchor_ms: i64,
    #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u32).range(1..=32))]
    pub seed_mib: u32,
    #[arg(long, default_value_t = 31, value_parser = clap::value_parser!(u32).range(2..=511))]
    pub files_per_partition: u32,
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u32).range(1..=8))]
    pub copy_concurrency: u32,
    #[arg(long, default_value_t = 2147483648, value_parser = clap::value_parser!(u64).range(1..=8589934592))]
    pub max_total_bytes: u64,
    /// PURGE the expired partition right after seeding, leaving its files as Delta
    /// tombstones so a later VACUUM only deletes files that are already unreferenced.
    #[arg(long)]
    #[serde(default)]
    pub tombstones: bool,
}

impl SeedOptions {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.anchor_ms >= 3 * 86_400_000 && self.anchor_ms % 60_000 == 0,
            "invalid anchor"
        );
        ensure!((1..=32).contains(&self.seed_mib), "seed memory bound");
        ensure!((2..=511).contains(&self.files_per_partition), "file count bound");
        ensure!((1..=8).contains(&self.copy_concurrency), "copy concurrency bound");
        ensure!(
            (1..=MAX_TOTAL_BYTES).contains(&self.max_total_bytes),
            "total byte bound"
        );
        let minimum = u64::from(self.seed_mib) * MIB * (2 * u64::from(self.files_per_partition) + 2);
        ensure!(
            minimum <= self.max_total_bytes,
            "requested payload exceeds total byte budget"
        );
        Ok(())
    }

    pub fn rows(&self) -> usize {
        self.seed_mib as usize * MIB as usize / PAYLOAD_BYTES
    }

    pub fn partitions(&self) -> Result<Vec<ScalePartition>> {
        [
            ("expired", self.anchor_ms - 3 * 86_400_000, self.files_per_partition),
            ("compact", self.anchor_ms - 2 * 3_600_000, self.files_per_partition),
            ("control", self.anchor_ms - 3_600_000, 1),
            ("live", self.anchor_ms, 1),
        ]
        .into_iter()
        .map(|(role, timestamp_ms, files)| {
            Ok(ScalePartition {
                role: role.to_owned(),
                partition: ingestion_partition_value(timestamp_ms)?,
                timestamp_ms,
                files,
            })
        })
        .collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScalePartition {
    pub role: String,
    pub partition: String,
    pub timestamp_ms: i64,
    pub files: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CopyFile {
    pub path: String,
    pub partition: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub format: u32,
    pub options: SeedOptions,
    pub table: String,
    pub source: String,
    pub source_etag: String,
    pub source_bytes: u64,
    pub source_sha256: String,
    pub seed_rows: usize,
    pub seed_row_groups: usize,
    pub seed_max_row_group_bytes: u64,
    pub payload_sha256: Vec<String>,
    pub partitions: Vec<ScalePartition>,
    pub copies: Vec<CopyFile>,
}

pub fn payload(id: usize) -> String {
    let mut output = String::with_capacity(PAYLOAD_BYTES);
    for chunk in 0..PAYLOAD_BYTES / 64 {
        let digest = Sha256::digest(format!("qap-r3-v1:{id}:{chunk}").as_bytes());
        for byte in digest {
            output.push(b"0123456789abcdef"[(byte >> 4) as usize] as char);
            output.push(b"0123456789abcdef"[(byte & 15) as usize] as char);
        }
    }
    output
}

pub fn batch(options: &SeedOptions) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let ids = (0..options.rows() as i64).collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(Int64Array::from(ids.iter().map(|id| id * 10 + 7).collect::<Vec<_>>())),
            Arc::new(StringArray::from((0..options.rows()).map(payload).collect::<Vec<_>>())),
        ],
    )?)
}

pub fn location(url: &Url, relative: &str) -> Result<Path> {
    let joined = url.join(relative)?;
    ensure!(
        !relative.is_empty()
            && !relative.starts_with('/')
            && !relative.contains(['\\', '?', '#', '%'])
            && !relative.split('/').any(|part| matches!(part, "." | ".." | ""))
            && joined.as_str().starts_with(url.as_str()),
        "unsafe fixture object path"
    );
    let (_, prefix) = ObjectStoreScheme::parse(url)?;
    Ok(Path::from(relative)
        .parts()
        .fold(prefix, |prefix, part| prefix.join(part)))
}

pub async fn read_manifest(url: &Url, store: &Arc<dyn ObjectStore>) -> Result<Manifest> {
    let path = location(url, MANIFEST)?;
    let meta = store.head(&path).await?;
    ensure!(meta.size <= 2 * MIB, "manifest byte budget");
    let manifest: Manifest = serde_json::from_slice(&store.get(&path).await?.bytes().await?)?;
    manifest.options.validate()?;
    ensure!(manifest.format == 1, "unsupported scale manifest");
    ensure!(manifest.seed_rows == manifest.options.rows(), "manifest row mismatch");
    ensure!(
        manifest.payload_sha256.len() == manifest.seed_rows,
        "manifest digest count"
    );
    ensure!(
        manifest.partitions == manifest.options.partitions()?,
        "manifest partition mismatch"
    );
    ensure!(manifest.copies.len() < MAX_FILES, "manifest copy count");
    ensure!(
        manifest.source_bytes <= 34 * MIB && manifest.source_bytes >= MIB,
        "manifest seed byte bound"
    );
    ensure!(
        manifest
            .source_bytes
            .checked_mul(manifest.copies.len() as u64 + 1)
            .is_some_and(|bytes| bytes <= manifest.options.max_total_bytes),
        "manifest physical byte bound"
    );
    location(url, &manifest.source)?;
    ensure!(
        manifest.source.starts_with(&format!(
            "{INGESTION_PARTITION}={}/",
            ingestion_partition_value(manifest.options.anchor_ms)?
        )),
        "seed partition mismatch"
    );
    ensure!(
        manifest
            .payload_sha256
            .iter()
            .enumerate()
            .all(|(id, digest)| { *digest == format!("{:x}", Sha256::digest(payload(id).as_bytes())) }),
        "seed truth digest mismatch"
    );
    ensure!(
        manifest.copies == expected_copies(&manifest.partitions),
        "manifest copy inventory mismatch"
    );
    for copy in &manifest.copies {
        location(url, &copy.path)?;
        ensure!(
            copy.path
                .starts_with(&format!("{INGESTION_PARTITION}={}/", copy.partition)),
            "copy partition mismatch"
        );
    }
    Ok(manifest)
}

#[derive(Debug)]
pub struct PartialFailure {
    pub inventory: Value,
}

impl fmt::Display for PartialFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("scale publication incomplete; immutable candidate paths retained")
    }
}

impl std::error::Error for PartialFailure {}

pub async fn seed(cli: &Cli, url: Url, options: &SeedOptions, stop: &Stop) -> Result<Value> {
    options.validate()?;
    ensure!(!stop.requested(), "seed cancelled before admission");
    if url.scheme() != "file" {
        ensure!(
            std::env::temp_dir().starts_with(std::env::current_dir()?),
            "TMPDIR must point inside the workspace for production Parquet staging"
        );
    }
    let generation_root = cli.root.clone();
    tokio::task::spawn_blocking(move || CloudGeneration::capture(&generation_root)).await??;
    let session_url = url.clone();
    let session = tokio::task::spawn_blocking(move || PublisherSession::shared_for_url(&session_url)).await??;
    let store = session.store_for_url(&url)?;
    let manifest_path = location(&url, MANIFEST)?;
    let manifest = match store.head(&manifest_path).await {
        Ok(_) => {
            let manifest = read_manifest(&url, &store).await?;
            ensure!(
                manifest.table == cli.table && manifest.options == *options,
                "resume configuration mismatch"
            );
            manifest
        }
        Err(object_store::Error::NotFound { .. }) => {
            session.check_async().await?;
            create_seed(cli, &url, options, &store, &session).await?
        }
        Err(error) => return Err(error.into()),
    };
    let candidates = manifest.copies.iter().map(|copy| copy.path.clone()).collect::<Vec<_>>();
    let work = publish_copies(&url, &manifest, &store, &session, stop).await;
    match work {
        Ok(copies) => {
            let tombstones = if options.tombstones {
                ensure!(!stop.requested(), "tombstone seeding cancelled");
                Some(tombstone_expired(&url, &manifest).await?)
            } else {
                None
            };
            let mut inspection = scale_oracle::inspect(url, cli.table.clone()).await?;
            drop(store);
            drop(session);
            PublisherSession::close_shared_async().await?;
            inspection["action"] = json!("seed-scale");
            inspection["partitions"] = serde_json::to_value(&manifest.partitions)?;
            inspection["copies"] = copies;
            inspection["copyReceiptsPrefix"] = json!("_scale/copies/");
            inspection["tombstones"] = json!(tombstones);
            inspection["publisherClosed"] = json!(true);
            inspection["cancelled"] = json!(stop.requested());
            Ok(inspection)
        }
        Err(error) => {
            if std::env::var_os("ARROWCOL_EXPERIMENT_DEBUG_ERRORS").is_some() {
                eprintln!("{error:?}");
            }
            Err(PartialFailure {
            inventory: json!({
                "manifest": MANIFEST,
                "copyReceiptsPrefix": "_scale/copies/",
                "source": manifest.source,
                "candidatePaths": candidates,
                "classification": "Candidates may be active, completed-uncommitted, pending or absent; inspect before deleting. No automatic cleanup after uncertainty."
            }),
        }
        .into())
        }
    }
}

async fn create_seed(
    cli: &Cli,
    url: &Url,
    options: &SeedOptions,
    store: &Arc<dyn ObjectStore>,
    session: &Arc<PublisherSession>,
) -> Result<Manifest> {
    crate::require_empty(url).await?;
    let physical = batch(options)?;
    let partition = ingestion_partition_value(options.anchor_ms)?;
    let mut fields = physical.schema().fields().to_vec();
    fields.push(Arc::new(Field::new(INGESTION_PARTITION, DataType::Utf8, false)));
    let mut columns = physical.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![partition.clone(); options.rows()])));
    let seed = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    let seed_url = url.clone();
    let schema = seed.schema();
    let table = tokio::task::spawn_blocking(move || {
        DeltaTable::open_url(seed_url, schema, vec![INGESTION_PARTITION.to_owned()])
    })
    .await??;
    let mut writer = table.new_file_partitioned_with_properties(
        seed.schema(),
        BTreeMap::from([(INGESTION_PARTITION.to_owned(), Some(partition))]),
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_max_row_group_row_count(Some(32))
            .build(),
    )?;
    writer.write(&seed).await?;
    writer.finish().await?;
    drop(table);
    let (_, files) = scale_oracle::inventory(url.clone(), Arc::clone(store)).await?;
    ensure!(files.len() == 1, "seed inventory is not isolated");
    let source = &files[0];
    let meta = store.head(&location(url, &source.path)?).await?;
    let source_bytes = store
        .get_opts(
            &meta.location,
            GetOptions {
                if_match: meta.e_tag.clone(),
                ..GetOptions::default()
            },
        )
        .await?
        .bytes()
        .await?;
    ensure!(source_bytes.len() as u64 == meta.size, "seed changed");
    ensure!(
        meta.size <= 34 * MIB && meta.size >= u64::from(options.seed_mib) * MIB,
        "seed byte budget"
    );
    let file_count = 2 * u64::from(options.files_per_partition) + 2;
    ensure!(
        meta.size
            .checked_mul(file_count)
            .is_some_and(|bytes| bytes <= options.max_total_bytes),
        "actual Parquet bytes exceed total byte budget"
    );
    let partitions = options.partitions()?;
    let payloads = physical
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("seed payload type")?;
    let payload_sha256 = (0..options.rows())
        .map(|id| format!("{:x}", Sha256::digest(payloads.value(id).as_bytes())))
        .collect::<Vec<_>>();
    let reader = ParquetRecordBatchReaderBuilder::try_new(source_bytes.clone())?;
    let seed_row_groups = reader.metadata().num_row_groups();
    let seed_max_row_group_bytes = reader
        .metadata()
        .row_groups()
        .iter()
        .map(|group| u64::try_from(group.total_byte_size()))
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .context("missing seed row group")?;
    let mut verified_rows = 0;
    for batch in reader.with_batch_size(32).build()? {
        let batch = batch?;
        ensure!(
            batch.num_columns() == 3 && batch.columns().iter().all(|column| column.null_count() == 0),
            "invalid seed physical data"
        );
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("seed id")?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("seed value")?;
        let strings = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("seed payload")?;
        for index in 0..batch.num_rows() {
            ensure!(
                verified_rows < options.rows()
                    && ids.value(index) == verified_rows as i64
                    && values.value(index) == verified_rows as i64 * 10 + 7
                    && strings.value(index) == payloads.value(verified_rows),
                "seed truth differs from encoded Parquet"
            );
            verified_rows += 1;
        }
    }
    ensure!(verified_rows == options.rows(), "seed row count mismatch");
    let copies = expected_copies(&partitions);
    let manifest = Manifest {
        format: 1,
        options: options.clone(),
        table: cli.table.clone(),
        source: source.path.clone(),
        source_etag: meta.e_tag.context("seed ETag missing")?,
        source_bytes: meta.size,
        source_sha256: format!("{:x}", Sha256::digest(&source_bytes)),
        seed_rows: options.rows(),
        seed_row_groups,
        seed_max_row_group_bytes,
        payload_sha256,
        partitions,
        copies,
    };
    session.check_async().await?;
    store
        .put_opts(
            &location(url, MANIFEST)?,
            serde_json::to_vec(&manifest)?.into(),
            PutMode::Create.into(),
        )
        .await?;
    session.check_async().await?;
    Ok(manifest)
}

fn expected_copies(partitions: &[ScalePartition]) -> Vec<CopyFile> {
    partitions
        .iter()
        .flat_map(|partition| {
            (0..partition.files).filter_map(|index| {
                if partition.role == "live" {
                    None
                } else {
                    Some(CopyFile {
                        path: format!(
                            "{INGESTION_PARTITION}={}/qap-r3-{index:04}.parquet",
                            partition.partition
                        ),
                        partition: partition.partition.clone(),
                    })
                }
            })
        })
        .collect()
}

async fn publish_copies(
    url: &Url,
    manifest: &Manifest,
    store: &Arc<dyn ObjectStore>,
    session: &Arc<PublisherSession>,
    stop: &Stop,
) -> Result<Value> {
    let source = store.head(&location(url, &manifest.source)?).await?;
    ensure!(
        source.e_tag.as_deref() == Some(&manifest.source_etag),
        "source ETag changed"
    );
    ensure!(source.size == manifest.source_bytes, "source size changed");
    let (read_version, active) = scale_oracle::inventory(url.clone(), Arc::clone(store)).await?;
    let expected = manifest
        .copies
        .iter()
        .map(|copy| copy.path.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    ensure!(
        active
            .iter()
            .all(|file| file.path == manifest.source || expected.contains(file.path.as_str())),
        "resume after maintenance or foreign append is unsupported"
    );
    ensure!(
        active.iter().any(|file| file.path == manifest.source),
        "active seed missing"
    );
    ensure!(
        active.iter().all(|file| file.bytes == manifest.source_bytes),
        "active copied file size changed"
    );
    if active.len() == manifest.copies.len() + 1 {
        return Ok(json!([]));
    }
    let expired = expired_copies(manifest)?;
    if manifest.options.tombstones
        && active.len() + expired.len() == manifest.copies.len() + 1
        && active.iter().all(|file| !expired.contains(file.path.as_str()))
    {
        // Resume after the tombstone PURGE already committed.
        return Ok(json!([]));
    }
    ensure!(active.len() == 1, "partial external commit is unsupported");
    let transport: Arc<dyn ObjectStore> = if url.scheme() == "file" {
        Arc::new(LocalCopyStore {
            inner: Arc::clone(store),
            source: source.clone(),
        })
    } else {
        Arc::new(NativeCopyStore::new(url, manifest).await?)
    };
    // A native copy is a create-only PUT transport, not object_store's asynchronous Copy Blob.
    // The production guard tracks it as one mutation through completion, including uncertain exits.
    let transport = session.bind_store(url, transport, MutationRetries::Disabled)?;
    let failed = AtomicBool::new(false);
    let mut pending = futures::stream::iter(manifest.copies.iter().map(|copy| async {
        if stop.requested() || failed.load(Ordering::Acquire) {
            bail!("copy cancelled");
        }
        session.check_async().await?;
        let path = location(url, &copy.path)?;
        let result = transport
            .put_opts(&path, PutPayload::default(), PutMode::Create.into())
            .await?;
        session.check_async().await?;
        let report = json!({
            "path":copy.path,"bytes":manifest.source_bytes,"etag":result.e_tag,
            "requestId":result.version,"completed":true
        });
        let report = record_copy(url, store, report).await?;
        session.check_async().await?;
        Ok::<_, anyhow::Error>(report)
    }))
    .buffer_unordered(manifest.options.copy_concurrency as usize);
    let mut reports = Vec::new();
    let mut failure = None;
    while let Some(result) = pending.next().await {
        match result {
            Ok(report) => reports.push(report),
            Err(error) => {
                failed.store(true, Ordering::Release);
                failure = Some(error);
                break;
            }
        }
    }
    // Join started requests before reporting; never drop a pending native mutation on soft cancellation.
    while let Some(result) = pending.next().await {
        if let Ok(report) = result {
            reports.push(report);
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    ensure!(!stop.requested(), "publication cancelled");
    let source_after = store.head(&source.location).await?;
    ensure!(
        source_after.e_tag == source.e_tag && source_after.size == source.size,
        "source mutation"
    );
    let url = url.clone();
    let manifest = manifest.clone();
    let store = Arc::clone(store);
    let session = Arc::clone(session);
    tokio::task::spawn_blocking(move || commit_copies(&url, &manifest, store, read_version, session)).await??;
    Ok(json!(reports))
}

fn expired_copies(manifest: &Manifest) -> Result<std::collections::BTreeSet<&str>> {
    let expired = &manifest
        .partitions
        .iter()
        .find(|partition| partition.role == "expired")
        .context("expired partition missing")?
        .partition;
    Ok(manifest
        .copies
        .iter()
        .filter(|copy| &copy.partition == expired)
        .map(|copy| copy.path.as_str())
        .collect())
}

/// Production PURGE of the expired partition; returns how many files are now tombstones.
/// Idempotent: a resumed seed finds nothing left to purge.
async fn tombstone_expired(url: &Url, manifest: &Manifest) -> Result<usize> {
    let expired = manifest
        .partitions
        .iter()
        .find(|partition| partition.role == "expired")
        .context("expired partition missing")?
        .partition
        .clone();
    let tombstones = expired_copies(manifest)?.len();
    let url = url.clone();
    let anchor_ms = manifest.options.anchor_ms;
    tokio::task::spawn_blocking(move || {
        let table = DeltaTable::open_for_maintenance(&url)?;
        let config = PurgeConfig::new(TOMBSTONE_RETENTION_MS, anchor_ms, Granularity::Minute);
        let plan = table.plan_purge(&config)?;
        if plan.partitions.is_empty() {
            return Ok(tombstones);
        }
        ensure!(
            plan.partitions.len() == 1
                && plan.partitions[0].value == expired
                && plan.partitions[0].files.len() == tombstones,
            "tombstone PURGE must remove exactly the expired partition"
        );
        let control = MaintenanceControl::new(Arc::new(AtomicBool::new(false)), None);
        let report = table.apply_purge(&plan, &config, &control)?;
        ensure!(
            matches!(report.outcome, Outcome::Committed | Outcome::AlreadyApplied),
            "tombstone PURGE did not commit"
        );
        Ok(tombstones)
    })
    .await?
}

async fn record_copy(url: &Url, store: &Arc<dyn ObjectStore>, report: Value) -> Result<Value> {
    let path = report["path"].as_str().context("copy report path")?;
    let receipt = location(
        url,
        &format!("_scale/copies/{:x}.json", Sha256::digest(path.as_bytes())),
    )?;
    match store
        .put_opts(&receipt, serde_json::to_vec(&report)?.into(), PutMode::Create.into())
        .await
    {
        Ok(_) => Ok(report),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let meta = store.head(&receipt).await?;
            ensure!(meta.size <= 16_384, "copy receipt byte bound");
            let existing: Value = serde_json::from_slice(&store.get(&receipt).await?.bytes().await?)?;
            ensure!(
                ["path", "bytes", "etag", "completed"]
                    .iter()
                    .all(|key| existing[*key] == report[*key]),
                "copy receipt binding changed"
            );
            Ok(existing)
        }
        Err(error) => Err(error.into()),
    }
}

fn commit_copies(
    url: &Url,
    manifest: &Manifest,
    store: Arc<dyn ObjectStore>,
    read_version: u64,
    session: Arc<PublisherSession>,
) -> Result<()> {
    let engine = DefaultEngineBuilder::new(store).build();
    let snapshot = Snapshot::builder_for(url.as_str()).build(&engine)?;
    ensure!(
        snapshot.version() == read_version,
        "snapshot changed before publication"
    );
    let mut txn = snapshot
        .transaction(Box::new(FileSystemCommitter::new()), &engine)?
        .with_operation("INSERT".into())
        .with_engine_info("arrowcol-experiment-server-copy")
        .with_data_change(true)
        .with_blind_append()
        .with_transaction_id("qap-r3-copy-v1".to_owned(), 0);
    let seed = batch(&manifest.options)?;
    let state = txn.write_state()?;
    for copy in &manifest.copies {
        let context = state
            .write_context_builder()
            .with_partition_values(HashMap::from([(
                INGESTION_PARTITION.to_owned(),
                Scalar::String(copy.partition.clone()),
            )]))
            .build()?;
        let stats = delta_kernel_default_engine::stats::collect_stats(
            &seed,
            context.stats_columns(),
            context.physical_data_schema(),
        )?;
        txn.add_files(build_add_file_metadata(
            DataFileMetadata::new(
                FileMeta::new(url.join(&copy.path)?, manifest.options.anchor_ms, manifest.source_bytes),
                stats,
            ),
            &context,
        )?);
    }
    session.check()?;
    match txn.commit(&engine)? {
        CommitResult::Committed(_) => Ok(()),
        _ => bail!("copy commit conflicted or uncertain; retain all immutable candidates"),
    }
}

#[derive(Debug)]
struct LocalCopyStore {
    inner: Arc<dyn ObjectStore>,
    source: ObjectMeta,
}

impl fmt::Display for LocalCopyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("local fixture copy")
    }
}

#[derive(Debug)]
struct NativeCopyStore {
    http: HttpClient,
    credentials: AzureCredentialProvider,
    account: String,
    container: String,
    source: Url,
    source_etag: String,
    source_bytes: u64,
    owned_prefix: Path,
    source_sha256: String,
    endpoint: Url,
}

impl fmt::Display for NativeCopyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("synchronous native Azure fixture copy")
    }
}

impl NativeCopyStore {
    async fn new(url: &Url, manifest: &Manifest) -> Result<Self> {
        let account = url
            .host_str()
            .context("Azure account missing")?
            .split('.')
            .next()
            .context("account")?
            .to_owned();
        let container = url.username().to_owned();
        ensure!(
            !account.is_empty() && !container.is_empty(),
            "expected abfss container/account URL"
        );
        let azure = MicrosoftAzureBuilder::new()
            .with_url(url.as_str())
            .with_use_azure_cli(true)
            .build()?;
        let (_, owned_prefix) = ObjectStoreScheme::parse(url)?;
        let endpoint = Url::parse(&format!("https://{account}.blob.core.windows.net/"))?;
        let mut source = endpoint.clone();
        source.set_path(&format!("{container}/{}", location(url, &manifest.source)?));
        Ok(Self {
            http: ReqwestConnector::default().connect(
                &object_store::ClientOptions::new()
                    .with_timeout(Duration::from_secs(60))
                    .with_connect_timeout(Duration::from_secs(10)),
            )?,
            credentials: Arc::clone(azure.credentials()),
            account,
            container,
            source,
            source_etag: manifest.source_etag.clone(),
            source_bytes: manifest.source_bytes,
            owned_prefix,
            source_sha256: manifest.source_sha256.clone(),
            endpoint,
        })
    }

    async fn request(&self, method: &str, path: &Path) -> Result<HttpResponse> {
        ensure!(
            path.prefix_match(&self.owned_prefix).is_some(),
            "copy destination outside owned table"
        );
        let credential = tokio::time::timeout(Duration::from_secs(20), self.credentials.get_credential()).await??;
        let mut destination = self.endpoint.clone();
        destination.set_path(&format!("{}/{path}", self.container));
        let mut request = HttpRequest::new(HttpRequestBody::empty());
        *request.method_mut() = method.parse()?;
        *request.uri_mut() = destination.as_str().parse()?;
        if method == "PUT" {
            let mut source = self.source.clone();
            if let AzureCredential::SASToken(pairs) = credential.as_ref() {
                source.query_pairs_mut().extend_pairs(pairs);
            }
            for (name, value) in [
                ("content-length", "0"),
                ("if-none-match", "*"),
                ("x-ms-blob-type", "BlockBlob"),
                ("x-ms-copy-source", source.as_str()),
                ("x-ms-source-if-match", self.source_etag.as_str()),
                ("x-ms-meta-qap_source_etag", self.source_etag.as_str()),
                ("x-ms-meta-qap_source_sha256", self.source_sha256.as_str()),
            ] {
                request.headers_mut().insert(name, value.parse()?);
            }
            if let AzureCredential::BearerToken(token) = credential.as_ref() {
                request
                    .headers_mut()
                    .insert("x-ms-copy-source-authorization", format!("Bearer {token}").parse()?);
            }
        }
        AzureAuthorizer::new(credential.as_ref(), &self.account).authorize(&mut request);
        // The SDK authorizer's signed 2023-11-03 version supports Put Blob From URL.
        Ok(self.http.execute(request).await?)
    }

    async fn create(&self, path: &Path) -> Result<PutResult> {
        let response = self.request("PUT", path).await?;
        let status = response.status().as_u16();
        let code = response
            .headers()
            .get("x-ms-error-code")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("none")
            .to_owned();
        ensure!(
            status == 201 || status == 409 || status == 412,
            "native copy is not definitively complete (status {status}, code {code})"
        );
        let request_id = response
            .headers()
            .get("x-ms-request-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let response_etag = response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let head = self.request("HEAD", path).await?;
        ensure!(head.status().as_u16() == 200, "copy HEAD failed");
        let header = |name| {
            head.headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        ensure!(
            header("content-length").context("copy size")?.parse::<u64>()? == self.source_bytes,
            "copy size differs"
        );
        ensure!(
            header("x-ms-meta-qap_source_etag").as_deref() == Some(&self.source_etag),
            "foreign destination"
        );
        ensure!(
            header("x-ms-meta-qap_source_sha256").as_deref() == Some(&self.source_sha256),
            "foreign seed binding"
        );
        ensure!(
            header("x-ms-copy-status").is_none_or(|status| status == "success"),
            "pending or failed copy is not publishable"
        );
        let etag = header("etag").context("copy ETag missing")?;
        ensure!(
            status != 201 || response_etag.as_deref() == Some(&etag),
            "destination changed after native PUT"
        );
        Ok(PutResult {
            e_tag: Some(etag),
            version: request_id,
        })
    }
}

fn unsupported() -> object_store::Error {
    object_store::Error::NotSupported {
        source: "fixture copy transport only supports create-only PUT".into(),
    }
}

macro_rules! copy_transport {
    ($name:ty, $this:ident, $path:ident, $body:expr) => {
        #[async_trait::async_trait]
        impl ObjectStore for $name {
            async fn put_opts(&$this, $path: &Path, payload: PutPayload, options: PutOptions) -> object_store::Result<PutResult> {
                if !matches!(options.mode, PutMode::Create) || payload.content_length() != 0 { return Err(unsupported()); }
                $body
            }
            async fn put_multipart_opts(&self, _: &Path, _: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> { Err(unsupported()) }
            async fn get_opts(&self, _: &Path, _: GetOptions) -> object_store::Result<GetResult> { Err(unsupported()) }
            fn delete_stream(&self, _: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
                futures::stream::once(async { Err(unsupported()) }).boxed()
            }
            fn list(&self, _: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
                futures::stream::once(async { Err(unsupported()) }).boxed()
            }
            async fn list_with_delimiter(&self, _: Option<&Path>) -> object_store::Result<ListResult> { Err(unsupported()) }
            async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> object_store::Result<()> { Err(unsupported()) }
        }
    };
}

copy_transport!(NativeCopyStore, self, location, {
    self.create(location)
        .await
        .map_err(|error| object_store::Error::Generic {
            store: "native fixture copy",
            source: error.into(),
        })
});

copy_transport!(LocalCopyStore, self, location, {
    match self.inner.copy_if_not_exists(&self.source.location, location).await {
        Ok(()) => {}
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = self.inner.get(location).await?.bytes().await?;
            let source = self.inner.get(&self.source.location).await?.bytes().await?;
            if existing != source {
                return Err(unsupported());
            }
        }
        Err(error) => return Err(error),
    }
    let meta = self.inner.head(location).await?;
    if meta.size != self.source.size {
        return Err(unsupported());
    }
    Ok(PutResult {
        e_tag: meta.e_tag,
        version: None,
    })
});

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    type MockServer = std::thread::JoinHandle<Result<Vec<String>>>;

    fn join(server: MockServer) -> Result<Vec<String>> {
        server.join().map_err(|_| anyhow::anyhow!("mock server panicked"))?
    }

    fn mock_native(responses: Vec<String>) -> Result<(NativeCopyStore, MockServer)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = Url::parse(&format!("http://{}/", listener.local_addr()?))?;
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            ensure!(std::time::Instant::now() < deadline, "mock request timed out");
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => return Err(error.into()),
                    }
                };
                stream.set_read_timeout(Some(Duration::from_secs(5)))?;
                let mut bytes = Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    stream.read_exact(&mut byte)?;
                    bytes.push(byte[0]);
                    assert!(bytes.len() <= 16_384);
                }
                requests.push(String::from_utf8(bytes)?);
                stream.write_all(response.as_bytes())?;
            }
            Ok(requests)
        });
        Ok((
            NativeCopyStore {
                http: ReqwestConnector::default().connect(&object_store::ClientOptions::new().with_allow_http(true))?,
                credentials: Arc::new(object_store::client::StaticCredentialProvider::new(
                    AzureCredential::BearerToken("offline-test-token".to_owned()),
                )),
                account: "offlineaccount".to_owned(),
                container: "fixture".to_owned(),
                source: Url::parse("https://offlineaccount.blob.core.windows.net/fixture/owned/seed.parquet")?,
                source_etag: "\"seed-v1\"".to_owned(),
                source_bytes: 1234,
                owned_prefix: Path::from("owned"),
                source_sha256: "fixture-sha256".to_owned(),
                endpoint,
            },
            server,
        ))
    }

    fn response(status: u16, headers: &str) -> String {
        format!("HTTP/1.1 {status} test\r\nConnection: close\r\nContent-Length: 0\r\n{headers}\r\n")
    }

    fn head(copy_status: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 1234\r\nETag: \"dest-v1\"\r\n\
             x-ms-meta-qap_source_etag: \"seed-v1\"\r\nx-ms-meta-qap_source_sha256: fixture-sha256\r\n\
             {copy_status}\r\n"
        )
    }

    #[tokio::test]
    async fn native_copy_is_zero_body_conditional_authenticated_and_completed() -> Result<()> {
        let (store, server) = mock_native(vec![
            response(201, "ETag: \"dest-v1\"\r\nx-ms-request-id: copy-request\r\n"),
            head(""),
        ])?;
        let result = store.create(&Path::from("owned/copy.parquet")).await?;
        assert_eq!(result.e_tag.as_deref(), Some("\"dest-v1\""));
        assert_eq!(result.version.as_deref(), Some("copy-request"));
        let requests = join(server)?;
        let put = requests[0].to_ascii_lowercase();
        assert!(put.starts_with("put /fixture/owned/copy.parquet "));
        for expected in [
            "content-length: 0",
            "if-none-match: *",
            "x-ms-blob-type: blockblob",
            "x-ms-source-if-match: \"seed-v1\"",
            "x-ms-copy-source-authorization: bearer offline-test-token",
            "authorization: bearer offline-test-token",
            "x-ms-version: 2023-11-03",
        ] {
            assert!(put.contains(expected), "missing conditional authenticated copy header");
        }
        assert!(requests[1].starts_with("HEAD "));
        Ok(())
    }

    #[tokio::test]
    async fn pending_failed_or_foreign_copy_cannot_be_published() -> Result<()> {
        for status in [202, 500] {
            let (store, server) = mock_native(vec![response(status, "")])?;
            assert!(store.create(&Path::from("owned/copy.parquet")).await.is_err());
            assert_eq!(join(server)?.len(), 1);
        }
        for state in ["pending", "failed", "aborted"] {
            let (store, server) = mock_native(vec![
                response(201, "ETag: \"dest-v1\"\r\n"),
                head(&format!("x-ms-copy-status: {state}\r\n")),
            ])?;
            assert!(store.create(&Path::from("owned/copy.parquet")).await.is_err());
            join(server)?;
        }
        let (store, server) = mock_native(vec![
            response(412, ""),
            head("").replace("fixture-sha256", "foreign-seed"),
        ])?;
        assert!(store.create(&Path::from("owned/copy.parquet")).await.is_err());
        join(server)?;
        Ok(())
    }

    #[tokio::test]
    async fn immutable_completed_copy_is_reused_and_source_change_fails_closed() -> Result<()> {
        let (store, server) = mock_native(vec![response(412, ""), head("")])?;
        assert!(store.create(&Path::from("owned/copy.parquet")).await.is_ok());
        join(server)?;
        let (store, server) = mock_native(vec![response(412, ""), response(404, "")])?;
        assert!(store.create(&Path::from("owned/copy.parquet")).await.is_err());
        join(server)?;
        Ok(())
    }

    #[test]
    fn path_partition_and_checked_bounds_are_strict() -> Result<()> {
        let url = Url::parse("file:///owned/experiment_test/")?;
        for path in [
            "../foreign",
            "/absolute",
            "a\\b",
            "a/%2e%2e/b",
            "a?secret",
            "a#fragment",
        ] {
            assert!(location(&url, path).is_err());
        }
        let options = SeedOptions {
            anchor_ms: 1793356800000,
            seed_mib: 32,
            files_per_partition: 511,
            copy_concurrency: 8,
            max_total_bytes: MAX_TOTAL_BYTES,
            tombstones: false,
        };
        assert!(options.validate().is_err());
        assert_eq!(payload(0).len(), PAYLOAD_BYTES);
        assert_ne!(payload(0), payload(1));
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_and_closed_authority_cannot_publish_copies() -> Result<()> {
        if std::env::var("ARROWCOL_PARTITION_GRANULARITY").as_deref() != Ok("minute") {
            let status = std::process::Command::new(std::env::current_exe()?)
                .env("ARROWCOL_PARTITION_GRANULARITY", "minute")
                .args([
                    "--exact",
                    "bulk::tests::cancellation_and_closed_authority_cannot_publish_copies",
                ])
                .status()?;
            ensure!(status.success(), "isolated minute-partition test failed");
            return Ok(());
        }
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let root = std::env::current_dir()?.join(format!(".data/bulk-unit-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&root)?;
        struct OwnedRoot(std::path::PathBuf);
        impl Drop for OwnedRoot {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _owned = OwnedRoot(root.clone());
        let options = SeedOptions {
            anchor_ms: 1793356800000,
            seed_mib: 1,
            files_per_partition: 2,
            copy_concurrency: 2,
            max_total_bytes: MAX_TOTAL_BYTES,
            tombstones: false,
        };
        let cli = Cli {
            root: root.to_string_lossy().into_owned(),
            allow_table: "experiment_unit".to_owned(),
            table: "experiment_unit".to_owned(),
            timeout_seconds: 900,
            action: crate::Action::SeedScale(options.clone()),
        };
        let url =
            Url::from_directory_path(root.join("experiment_unit")).map_err(|()| anyhow::anyhow!("local test URL"))?;
        let session = PublisherSession::local();
        let store = session.store_for_url(&url)?;
        let manifest = create_seed(&cli, &url, &options, &store, &session).await?;
        let stop = Stop {
            requested: AtomicBool::new(true),
            notify: tokio::sync::Notify::new(),
        };
        assert!(publish_copies(&url, &manifest, &store, &session, &stop).await.is_err());
        let (version, files) = scale_oracle::inventory(url.clone(), Arc::clone(&store)).await?;
        assert_eq!(files.len(), 1);
        session.close_after_drain_async().await?;
        stop.requested.store(false, Ordering::Release);
        assert!(publish_copies(&url, &manifest, &store, &session, &stop).await.is_err());
        assert_eq!(
            scale_oracle::inventory(url.clone(), Arc::clone(&store)).await?.0,
            version
        );
        for copy in &manifest.copies {
            assert!(matches!(
                store.head(&location(&url, &copy.path)?).await,
                Err(object_store::Error::NotFound { .. })
            ));
        }
        Ok(())
    }
}
