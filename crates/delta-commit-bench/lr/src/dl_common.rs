// Shared delta-rs bench body. Included by `dl.rs` once per library version with `dlc` bound to
// that version's `deltalake_core`.

use crate::store;
use crate::synth::{AddRow, Rows};
use crate::schema::StatsMode;
use crate::stats::Summary;
use anyhow::Result;
use dlc::kernel::transaction::{CommitBuilder, CommitProperties};
use dlc::kernel::{Action, Add, Transaction};
use dlc::logstore::{LogStore, LogStoreFactory, StorageConfig, default_logstore, logstore_factories};
use dlc::protocol::{DeltaOperation, SaveMode};
use dlc::{DeltaResult, DeltaTable, DeltaTableBuilder, ObjectStore};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Once};
use std::time::Instant;
use url::Url;

struct Factory;

impl LogStoreFactory for Factory {
    fn with_options(
        &self,
        prefixed_store: Arc<dyn ObjectStore>,
        root_store: Arc<dyn ObjectStore>,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(prefixed_store, root_store, location, options))
    }
}

fn register() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        for scheme in ["abfss://", "abfs://", "az://", "azure://"] {
            if let Ok(url) = Url::parse(scheme) {
                logstore_factories().insert(url, Arc::new(Factory));
            }
        }
    });
}

pub async fn open(url: &Url) -> Result<DeltaTable> {
    register();
    let (root, _) = store::open(url)?;
    let mut table = DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(root, url.clone()).build()?;
    table.load().await?;
    Ok(table)
}

fn add_action(r: AddRow) -> Action {
    Action::Add(Add {
        path: r.path,
        partition_values: HashMap::from([("date".to_owned(), Some(r.date))]),
        size: r.size,
        modification_time: r.mtime,
        data_change: true,
        stats: r.stats,
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    })
}

pub async fn txn_version(table: &DeltaTable, app_id: &str) -> Result<Option<i64>> {
    Ok(table.snapshot()?.transaction_version(table.log_store().as_ref(), app_id).await?)
}

pub async fn commit(args: &crate::CommitArgs, lib: &str) -> Result<()> {
    store::rss_watchdog(args.rss_cap_gib, format!("{lib} commit {}", args.table_url));
    let base_rss = store::rss_mib();
    let t0 = Instant::now();
    let mut table = open(&args.table_url).await?;
    let load_s = t0.elapsed().as_secs_f64();
    let rss_load = store::rss_mib();
    let mut v = txn_version(&table, &args.app_id).await?.unwrap_or(0);
    let rows = Rows {
        active: u64::from(args.commits * args.files_per_commit).max(1),
        tombstones: 0,
        tombstone_mib: 128,
        days: 1,
        stats: StatsMode::Default,
        seed: args.seed,
    };
    let mut sum = Summary::default();
    let mut retries = 0usize;
    for c in 0..args.commits {
        v += 1;
        let from = u64::from(c * args.files_per_commit);
        let adds: Vec<Action> = rows
            .adds(from, from + u64::from(args.files_per_commit))
            .into_iter()
            .map(|mut a| {
                a.path = format!("{}-{}", args.app_id, a.path.replace('/', "_"));
                add_action(a)
            })
            .collect();
        let props = CommitProperties::default()
            .with_create_checkpoint(args.dl_checkpoint)
            .with_application_transaction(Transaction::new_with_last_update(&args.app_id, v, Some(crate::now_ms())));
        let started = Instant::now();
        let fc = CommitBuilder::from(props)
            .with_actions(adds)
            .build(
                Some(table.snapshot()?),
                table.log_store(),
                DeltaOperation::Write { mode: SaveMode::Append, partition_by: None, predicate: None },
            )
            .await?;
        retries += fc.metrics.num_retries as usize;
        table.state = Some(fc.snapshot());
        sum.record(started.elapsed(), store::rss_mib());
        if args.trace {
            eprintln!(
                "{}",
                json!({"kind": "trace", "version": table.version(),
                    "ms": started.elapsed().as_secs_f64() * 1e3, "rss_mib": store::rss_mib()})
            );
        }
    }
    println!(
        "{}",
        json!({"kind": "commit", "lib": lib, "table": args.table_url.as_str(), "load_s": load_s,
            "rss_base_mib": base_rss, "rss_after_load_mib": rss_load, "hwm_mib": store::hwm_mib(),
            "retries": retries, "version": table.version(), "summary": sum.json()})
    );
    Ok(())
}

pub async fn load(args: &crate::LoadArgs, lib: &str) -> Result<()> {
    store::rss_watchdog(args.rss_cap_gib, format!("{lib} load {}", args.table_url));
    let t0 = Instant::now();
    let table = open(&args.table_url).await?;
    let open_s = t0.elapsed().as_secs_f64();
    let snap = table.snapshot()?;
    let (r, w) = (snap.protocol().min_reader_version(), snap.protocol().min_writer_version());
    let fields = snap.schema().fields().count();
    let t1 = Instant::now();
    let txn = txn_version(&table, &args.app_id).await?;
    println!(
        "{}",
        json!({"kind": "load", "lib": lib, "table": args.table_url.as_str(), "open_s": open_s,
            "txn_s": t1.elapsed().as_secs_f64(), "total_s": t0.elapsed().as_secs_f64(),
            "rss_mib": store::rss_mib(), "hwm_mib": store::hwm_mib(), "version": table.version(),
            "protocol": [r, w], "fields": fields, "txn": txn})
    );
    Ok(())
}

pub async fn checkpoint(args: &crate::LoadArgs, lib: &str) -> Result<()> {
    store::rss_watchdog(args.rss_cap_gib, format!("{lib} checkpoint {}", args.table_url));
    let t0 = Instant::now();
    let table = open(&args.table_url).await?;
    let open_s = t0.elapsed().as_secs_f64();
    let t1 = Instant::now();
    create_checkpoint(&table).await?;
    println!(
        "{}",
        json!({"kind": "checkpoint", "lib": lib, "table": args.table_url.as_str(), "open_s": open_s,
            "write_s": t1.elapsed().as_secs_f64(), "hwm_mib": store::hwm_mib(), "version": table.version()})
    );
    Ok(())
}
