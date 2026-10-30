mod oracle;
mod receipts;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use arrow::array::{Int64Array, RecordBatch};
use arrow_deltalake::commit::{BatchedTable, CommitOptions};
use arrow_deltalake::delta::{ingestion_partition_value, object_store_for_url};
use arrow_deltalake::publisher::PublisherSession;
use arrow_deltalake::record_batch_logical_bytes;
use arrowcol_buffer::{
    Admission, BufferConfig, BufferManager, CloudGeneration, DeliveryOutcome, DeltaPublicationSink, TrackedState,
};
use arrowcol_catalog::{
    CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest, MemoryCatalogStore,
    MemoryMetastore, SchemaValidator, ValidatedIncomingSchema,
};
use arrowcol_protocol::{
    CatalogName, ClaimedSchemaFingerprint, CollectorInstanceId, FencingToken, ImplementationPool, LeaseId,
    ReceiptState, SchemaName, TableIdentity, TableLease, TableName, UnixMillis, fingerprint_schema,
};
use arrowcol_receipts::{
    LeasePublicationFence, ReceiptError, ReceiptErrorKind, ReceiptResult, ReceiptStore, ReceiptVersion,
    ValidatedDelivery,
};
use clap::{Parser, Subcommand};
use futures::StreamExt;
use object_store::ObjectStoreScheme;
use oracle::Record;
use receipts::MemoryReceipts;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::Notify;
use url::Url;

#[derive(Parser)]
#[command(about = "Bounded production ArrowCol fixture publisher (JSON stdout)")]
struct Cli {
    #[arg(long)]
    root: String,
    /// Explicit table allowlist; must equal --table.
    #[arg(long)]
    allow_table: String,
    #[arg(long)]
    table: String,
    /// Hard action deadline; interrupted writes are never reported as cleanly closed.
    #[arg(long, default_value_t = 900, value_parser = clap::value_parser!(u64).range(1..=3600))]
    timeout_seconds: u64,
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    Seed {
        #[arg(long)]
        anchor_ms: i64,
        #[arg(long, default_value_t = 0)]
        start_id: i64,
        #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u32).range(1..=10000))]
        rows_per_file: u32,
        #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(2..=100))]
        files_per_partition: u32,
    },
    Append {
        #[arg(long)]
        start_id: i64,
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..=10000))]
        batches: u32,
        #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u32).range(1..=10000))]
        rows_per_batch: u32,
        #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u64).range(0..=60000))]
        interval_ms: u64,
        /// Omit for actual admission-time frontier; explicit values must be minute aligned.
        #[arg(long)]
        partition_ms: Option<i64>,
    },
    Inspect,
}

#[derive(Clone, Serialize)]
struct Partition {
    role: &'static str,
    partition: String,
    timestamp_ms: i64,
    files: u32,
}

struct Plan {
    start_id: i64,
    rows_per_batch: u32,
    interval_ms: u64,
    stamps: Vec<Option<i64>>,
    partitions: Vec<Partition>,
}

impl Cli {
    fn table_url(&self) -> Result<Url> {
        ensure!(self.allow_table == self.table, "table is not allowlisted");
        ensure!(
            self.table.starts_with("experiment_")
                && self.table.len() <= 63
                && self
                    .table
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
            "table must be a lowercase experiment_ identifier"
        );
        ensure!(
            std::env::var("ARROWCOL_PARTITION_GRANULARITY").as_deref() == Ok("minute"),
            "ARROWCOL_PARTITION_GRANULARITY=minute is required"
        );
        // A cloud root is admitted only when it exactly matches the generated private environment.
        let mut url = if self.root.contains("://") {
            let url = Url::parse(&self.root)?;
            ensure!(matches!(url.scheme(), "abfss" | "file"), "unsupported root scheme");
            if url.scheme() == "abfss" {
                ensure!(
                    std::env::var("ARROWCOL_DELTA_ROOT").as_deref() == Ok(self.root.as_str()),
                    "cloud root does not match ARROWCOL_DELTA_ROOT allowlist"
                );
            }
            url
        } else {
            let root = PathBuf::from(&self.root);
            ensure!(root.is_absolute(), "local root must be absolute");
            Url::from_directory_path(root).map_err(|()| anyhow::anyhow!("invalid local root"))?
        };
        ensure!(
            url.password().is_none() && url.query().is_none() && url.fragment().is_none(),
            "root contains secret URL components"
        );
        ensure!(
            url.scheme() != "file" || url.host_str().is_none(),
            "file root must be local"
        );
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        Ok(url.join(&format!("experiment/fixtures/{}/", self.table))?)
    }

    fn plan(&self) -> Result<Option<Plan>> {
        let plan = match self.action {
            Action::Inspect => return Ok(None),
            Action::Seed {
                anchor_ms,
                start_id,
                rows_per_file,
                files_per_partition,
            } => {
                ensure!(
                    anchor_ms >= 3 * 86_400_000 && anchor_ms % 60_000 == 0,
                    "anchor must be minute aligned"
                );
                let specs = [
                    ("expired", anchor_ms - 3 * 86_400_000, files_per_partition),
                    ("compact", anchor_ms - 2 * 3_600_000, files_per_partition),
                    ("control", anchor_ms - 3_600_000, 1),
                    ("live", anchor_ms, 1),
                ];
                let mut stamps = Vec::new();
                let mut partitions = Vec::new();
                for (role, stamp, files) in specs {
                    partitions.push(Partition {
                        role,
                        partition: ingestion_partition_value(stamp)?,
                        timestamp_ms: stamp,
                        files,
                    });
                    stamps.extend(std::iter::repeat_n(Some(stamp), files as usize));
                }
                Plan {
                    start_id,
                    rows_per_batch: rows_per_file,
                    interval_ms: 0,
                    stamps,
                    partitions,
                }
            }
            Action::Append {
                start_id,
                batches,
                rows_per_batch,
                interval_ms,
                partition_ms,
            } => {
                if let Some(stamp) = partition_ms {
                    ensure!(
                        stamp >= 0 && stamp % 60_000 == 0,
                        "partition timestamp must be minute aligned"
                    );
                    ingestion_partition_value(stamp)?;
                }
                Plan {
                    start_id,
                    rows_per_batch,
                    interval_ms,
                    stamps: vec![partition_ms; batches as usize],
                    partitions: Vec::new(),
                }
            }
        };
        let rows = plan
            .stamps
            .len()
            .checked_mul(plan.rows_per_batch as usize)
            .context("row bound overflow")?;
        ensure!(rows <= oracle::MAX_ROWS, "invocation row budget exceeded");
        ensure!(
            !matches!(self.action, Action::Seed { .. }) || rows <= 24_000,
            "seed row budget exceeded"
        );
        ensure!(
            plan.start_id >= 0
                && plan
                    .start_id
                    .checked_add(rows as i64)
                    .is_some_and(|last| last <= 1_000_000_000_000),
            "id range exceeds deterministic JSON-safe fixture range"
        );
        Ok(Some(plan))
    }
}

pub(crate) fn now() -> ReceiptResult<UnixMillis> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ReceiptError::new(ReceiptErrorKind::CorruptState, "clock before epoch"))?
        .as_millis();
    UnixMillis::new(
        u64::try_from(millis).map_err(|_| ReceiptError::new(ReceiptErrorKind::CorruptState, "clock overflow"))?,
    )
    .map_err(|_| ReceiptError::new(ReceiptErrorKind::CorruptState, "clock outside protocol range"))
}

async fn validation(table: &str) -> Result<ValidatedIncomingSchema> {
    let store = Arc::new(MemoryCatalogStore::new());
    let control = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        Arc::new(MemoryMetastore::new()),
    );
    control
        .execute_ddl(FlightSqlDdlRequest::new(format!(
            "CREATE TABLE experiment.fixtures.{table} (id BIGINT NOT NULL, value BIGINT NOT NULL)"
        )))
        .await?;
    let identity = TableIdentity::new(
        CatalogName::new("experiment")?,
        SchemaName::new("fixtures")?,
        TableName::new(table)?,
    );
    let registered = store.current_schema(&identity).await?.context("schema missing")?;
    let schema = registered.arrow_schema()?;
    Ok(SchemaValidator::new(store)
        .validate(
            &identity,
            ClaimedSchemaFingerprint::new(fingerprint_schema(&schema)),
            &schema,
        )
        .await?)
}

async fn require_empty(url: &Url) -> Result<()> {
    let store = object_store_for_url(url)?;
    let (_, prefix) = ObjectStoreScheme::parse(url)?;
    let log = prefix.join("_delta_log");
    let mut entries = store.list(Some(&log));
    if let Some(entry) = entries.next().await {
        entry?;
        anyhow::bail!("seed requires a fresh experiment table");
    }
    Ok(())
}

struct Stop {
    requested: AtomicBool,
    notify: Notify,
}

impl Stop {
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        self.notify.notify_one();
    }
    fn requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

async fn mutate(cli: &Cli, url: &Url, plan: Plan, stop: &Stop) -> Result<Value> {
    let generation_root = cli.root.clone();
    tokio::task::spawn_blocking(move || CloudGeneration::capture(&generation_root))
        .await?
        .context("capture_generation")?;
    if matches!(cli.action, Action::Seed { .. }) {
        require_empty(url).await.context("seed_target")?;
    } else {
        // Never allow append to silently recreate a missing/pruned-away table.
        let existing = oracle::inspect(url.clone(), cli.table.clone())
            .await
            .context("capture_oracle")?;
        ensure!(
            existing.rows + plan.stamps.len() * plan.rows_per_batch as usize <= oracle::MAX_ROWS,
            "table row budget exceeded"
        );
    }
    let validation = validation(&cli.table).await?;
    let owner = CollectorInstanceId::new();
    let admitted = now()?;
    let lease = TableLease::new(
        LeaseId::new(),
        validation.registered().identity().clone(),
        owner,
        ImplementationPool::Direct,
        FencingToken::new(1)?,
        admitted,
        UnixMillis::new(admitted.get() + 7_200_000)?,
    )?;
    let fence = Arc::new(LeasePublicationFence::new(lease.clone()));
    let table = BatchedTable::open_uri_with_fence(
        url.as_str(),
        CommitOptions {
            batch_window: Duration::from_millis(1),
            max_files_per_commit: 1,
            max_pending_files: 2,
        },
        fence,
    )
    .await?;
    let receipts = Arc::new(MemoryReceipts::default());
    let sink = Arc::new(DeltaPublicationSink::new(
        Arc::clone(&table),
        lease.table().clone(),
        validation.registered().fingerprint(),
        lease.publication_fence(),
    ));
    let manager = BufferManager::new(
        BufferConfig {
            max_batches_per_flush: 1,
            max_concurrent_flushes: 1,
            ..BufferConfig::default()
        },
        Arc::<MemoryReceipts>::clone(&receipts),
        sink,
    )?;
    let mut records = Vec::<Record>::new();
    let mut next_id = plan.start_id;
    let mut version = None;
    let mut batches = 0;
    let publication = async {
        for stamp in &plan.stamps {
            if stop.requested() {
                break;
            }
            let stamp = stamp.unwrap_or(i64::try_from(now()?.get())?);
            let partition = ingestion_partition_value(stamp)?;
            let end = next_id + i64::from(plan.rows_per_batch);
            let ids: Vec<_> = (next_id..end).collect();
            let values: Vec<_> = ids.iter().map(|id| id * 10 + 7).collect();
            let batch = RecordBatch::try_new(
                Arc::new(validation.registered().arrow_schema()?),
                vec![
                    Arc::new(Int64Array::from(ids.clone())),
                    Arc::new(Int64Array::from(values.clone())),
                ],
            )?;
            let delivery = ValidatedDelivery::new(
                &validation,
                u64::from(plan.rows_per_batch),
                1,
                record_batch_logical_bytes(&batch)?,
            )?;
            let accepted = receipts
                .create_flight_batch(
                    owner,
                    &lease,
                    lease.expires_at_unix_ms(),
                    std::slice::from_ref(&delivery),
                )
                .await?;
            let accepted = accepted.first().context("missing accepted receipt")?;
            let (_, completion) = manager
                .admit_tracked_at(
                    Admission {
                        receipt: ReceiptVersion::new(accepted.receipt_id, accepted.revision),
                        delivery,
                        lease: lease.clone(),
                        batches: vec![batch],
                    },
                    TrackedState::Accepted,
                    UnixMillis::new(u64::try_from(stamp)?)?,
                )
                .await?;
            ensure!(
                completion.outcome().await == DeliveryOutcome::Published,
                "publication did not complete definitively"
            );
            let published = receipts
                .receipt(accepted.receipt_id)
                .await?
                .context("published receipt missing")?;
            ensure!(published.state == ReceiptState::Published, "receipt not published");
            version = Some(
                published
                    .publication
                    .context("publication missing")?
                    .delta_version()
                    .get(),
            );
            records.extend(
                ids.into_iter()
                    .zip(values)
                    .map(|(id, value)| (id, value, partition.clone())),
            );
            next_id = end;
            batches += 1;
            if batches < plan.stamps.len() && plan.interval_ms > 0 && !stop.requested() {
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_millis(plan.interval_ms)) => {}
                    () = stop.notify.notified() => {}
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    table.start_draining();
    let report = manager.shutdown().await.context("drain")?;
    ensure!(
        report.incomplete.is_empty(),
        "buffer drain incomplete; publisher retained"
    );
    publication.context("publish")?;
    // Both manager shutdown and all tracked completions joined before cloud membership is released.
    drop(manager);
    drop(table);
    PublisherSession::close_shared_async()
        .await
        .context("close_publisher")?;
    records.sort();
    Ok(json!({
        "action": if matches!(cli.action, Action::Seed { .. }) { "seed" } else { "append" },
        "table": cli.table,
        "version": version,
        "batches": batches,
        "rows": records.len(),
        "startId": plan.start_id,
        "nextId": next_id,
        "cancelled": stop.requested(),
        "publisherClosed": true,
        "partitions": plan.partitions,
        "fingerprint": oracle::fingerprint(&records)?,
        "records": records,
    }))
}

async fn run(cli: &Cli, stop: &Stop) -> Result<Value> {
    let url = cli.table_url().context("validate_configuration")?;
    if let Some(plan) = cli.plan().context("validate_configuration")? {
        mutate(cli, &url, plan, stop).await
    } else {
        Ok(serde_json::to_value(
            oracle::inspect(url, cli.table.clone())
                .await
                .context("capture_oracle")?,
        )?)
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) {
                let _ = error.print();
                return;
            }
            println!(
                "{}",
                json!({"ok":false,"error":"invalid_arguments","publisherClosed":false})
            );
            std::process::exit(2);
        }
    };
    let stop = Arc::new(Stop {
        requested: AtomicBool::new(false),
        notify: Notify::new(),
    });
    let started = Instant::now();
    let watcher_stop = Arc::clone(&stop);
    let soft_deadline = Duration::from_secs(cli.timeout_seconds.saturating_sub(30).max(1));
    let watcher = tokio::spawn(async move {
        tokio::select! {
            _ = shutdown_signal() => {}
            () = tokio::time::sleep(soft_deadline) => {}
        }
        watcher_stop.request();
    });
    let action = run(&cli, &stop);
    tokio::pin!(action);
    let result = tokio::select! {
        result = &mut action => result,
        () = tokio::time::sleep(Duration::from_secs(cli.timeout_seconds)) => {
            // Do not cancel a storage future and then claim a drained publisher.
            println!("{}", json!({"ok":false,"error":"action_deadline","publisherClosed":false}));
            std::process::exit(1);
        }
    };
    watcher.abort();
    let _ = watcher.await;
    match result {
        Ok(mut output) => {
            output["ok"] = json!(true);
            output["elapsedMs"] = json!(started.elapsed().as_millis());
            let cancelled = output["cancelled"] == true;
            println!("{output}");
            if cancelled {
                std::process::exit(130);
            }
        }

        Err(error) => {
            // Error chains may contain signed URLs, credentials or private storage endpoints.
            println!(
                "{}",
                json!({"ok":false,"error":"experiment_action_failed","stage":failure_stage(&error),"publisherClosed":false})
            );
            std::process::exit(1);
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

fn failure_stage(error: &anyhow::Error) -> &'static str {
    for cause in error.chain() {
        let message = cause.to_string();
        for stage in [
            "validate_configuration",
            "capture_generation",
            "seed_target",
            "capture_oracle",
            "drain",
            "publish",
            "close_publisher",
        ] {
            if message == stage {
                return stage;
            }
        }
    }
    "setup"
}
