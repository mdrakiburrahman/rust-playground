//! Synthetic large tables: a checkpoint with N `add` (and optional `remove`) actions, plus a JSON tail.
//!
//! Every batch holds one action type. Its column is filled and the other action columns are null,
//! which matches how Spark and delta-rs lay out checkpoints.

use crate::schema::{self, FileStatsGen, StatsMode};
use crate::store;
use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, ListBuilder, MapBuilder, MapFieldNames, RecordBatch,
    StringArray, StringBuilder, StructArray, new_null_array,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use object_store::{ObjectStore, ObjectStoreExt};
use object_store::path::Path;
use parquet::arrow::AsyncArrowWriter;
use object_store::buffered::BufWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Debug, clap::ValueEnum, PartialEq, Eq)]
pub enum Format {
    Classic,
    Multipart,
    V2,
}

#[derive(clap::Args, Debug, Clone)]
pub struct GenArgs {
    #[arg(long)]
    pub table_url: url::Url,
    /// Active files in the checkpoint.
    #[arg(long)]
    pub active: u64,
    /// Remove tombstones in the checkpoint (deleted within the last 7 days).
    #[arg(long, default_value_t = 0)]
    pub tombstones: u64,
    /// Mean size of a tombstoned (pre-compaction) file in MiB.
    #[arg(long, default_value_t = 128)]
    pub tombstone_mib: u64,
    /// Days the active files span (one `date` partition per day).
    #[arg(long, default_value_t = 365)]
    pub days: u32,
    #[arg(long, value_enum, default_value_t = Format::Classic)]
    pub format: Format,
    /// Parts (multipart) or sidecars (v2).
    #[arg(long, default_value_t = 1)]
    pub parts: u32,
    #[arg(long, value_enum, default_value_t = StatsMode::Default)]
    pub stats: StatsMode,
    /// JSON commits after the checkpoint.
    #[arg(long, default_value_t = 10)]
    pub tail: u32,
    #[arg(long, default_value_t = 10)]
    pub tail_files: u32,
    /// `txn` appIds in the checkpoint (instance x table x epoch).
    #[arg(long, default_value_t = 1000)]
    pub appids: u32,
    /// Checkpoint version.
    #[arg(long, default_value_t = 100_000)]
    pub version: i64,
    #[arg(long, default_value_t = 100_000)]
    pub row_group: usize,
    #[arg(long, default_value_t = 65_536)]
    pub batch: usize,
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// Generator threads.
    #[arg(long, default_value_t = 8)]
    pub threads: usize,
}

const END_US: i64 = 1_793_404_800_000_000; // 2026-11-01T00:00:00Z
const DAY_US: i64 = 86_400_000_000;

fn map_builder() -> MapBuilder<StringBuilder, StringBuilder> {
    MapBuilder::new(
        Some(MapFieldNames { entry: "key_value".into(), key: "key".into(), value: "value".into() }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
}

fn map_of(rows: impl Iterator<Item = Vec<(String, String)>>) -> ArrayRef {
    let mut b = map_builder();
    for row in rows {
        for (k, v) in row {
            b.keys().append_value(k);
            b.values().append_value(v);
        }
        b.append(true).expect("map append");
    }
    Arc::new(b.finish())
}

fn null_map(n: usize) -> ArrayRef {
    let mut b = map_builder();
    for _ in 0..n {
        b.append(false).expect("map append");
    }
    Arc::new(b.finish())
}

fn str_list(rows: impl Iterator<Item = Vec<String>>) -> ArrayRef {
    let mut b = ListBuilder::new(StringBuilder::new());
    for row in rows {
        for s in row {
            b.values().append_value(s);
        }
        b.append(true);
    }
    Arc::new(b.finish())
}

fn dv_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("storageType", DataType::Utf8, true),
        Field::new("pathOrInlineDv", DataType::Utf8, true),
        Field::new("offset", DataType::Int32, true),
        Field::new("sizeInBytes", DataType::Int32, true),
        Field::new("cardinality", DataType::Int64, true),
    ]))
}

fn strukt(cols: Vec<(&str, ArrayRef)>) -> ArrayRef {
    let fields: Vec<Field> = cols.iter().map(|(n, a)| Field::new(*n, a.data_type().clone(), true)).collect();
    let arrays: Vec<ArrayRef> = cols.into_iter().map(|(_, a)| a).collect();
    Arc::new(StructArray::new(Fields::from(fields), arrays, None))
}

pub struct AddRow {
    pub path: String,
    pub date: String,
    pub size: i64,
    pub mtime: i64,
    pub stats: Option<String>,
}

pub struct RemoveRow {
    pub path: String,
    pub date: String,
    pub size: i64,
    pub deleted: i64,
}

pub fn add_array(rows: &[AddRow]) -> ArrayRef {
    let n = rows.len();
    strukt(vec![
        ("path", Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.path.as_str())))),
        ("partitionValues", map_of(rows.iter().map(|r| vec![("date".to_owned(), r.date.clone())]))),
        ("size", Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.size)))),
        ("modificationTime", Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.mtime)))),
        ("dataChange", Arc::new(BooleanArray::from(vec![false; n]))),
        ("stats", Arc::new(StringArray::from_iter(rows.iter().map(|r| r.stats.as_deref())))),
        ("tags", null_map(n)),
        ("deletionVector", new_null_array(&dv_type(), n)),
        ("baseRowId", new_null_array(&DataType::Int64, n)),
        ("defaultRowCommitVersion", new_null_array(&DataType::Int64, n)),
        ("clusteringProvider", new_null_array(&DataType::Utf8, n)),
    ])
}

pub fn remove_array(rows: &[RemoveRow]) -> ArrayRef {
    let n = rows.len();
    strukt(vec![
        ("path", Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.path.as_str())))),
        ("deletionTimestamp", Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.deleted)))),
        ("dataChange", Arc::new(BooleanArray::from(vec![false; n]))),
        ("extendedFileMetadata", Arc::new(BooleanArray::from(vec![true; n]))),
        ("partitionValues", map_of(rows.iter().map(|r| vec![("date".to_owned(), r.date.clone())]))),
        ("size", Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.size)))),
        ("tags", null_map(n)),
        ("deletionVector", new_null_array(&dv_type(), n)),
        ("baseRowId", new_null_array(&DataType::Int64, n)),
        ("defaultRowCommitVersion", new_null_array(&DataType::Int64, n)),
    ])
}

pub fn txn_array(rows: &[(String, i64, i64)]) -> ArrayRef {
    strukt(vec![
        ("appId", Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0.as_str())))),
        ("version", Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1)))),
        ("lastUpdated", Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2)))),
    ])
}

fn protocol_array(v2: bool) -> ArrayRef {
    let feats = || if v2 { vec!["v2Checkpoint".to_owned()] } else { vec![] };
    let (r, w) = if v2 { (3, 7) } else { (1, 2) };
    let list = |v: Vec<String>| -> ArrayRef {
        if v2 { str_list(std::iter::once(v)) } else { new_null_array(&str_list(std::iter::empty()).data_type().clone(), 1) }
    };
    strukt(vec![
        ("minReaderVersion", Arc::new(Int32Array::from(vec![r]))),
        ("minWriterVersion", Arc::new(Int32Array::from(vec![w]))),
        ("readerFeatures", list(feats())),
        ("writerFeatures", list(feats())),
    ])
}

pub fn table_config(stats: &StatsMode, v2: bool) -> Vec<(String, String)> {
    let mut c: Vec<(String, String)> =
        stats.table_config().into_iter().map(|(k, v)| (k.to_owned(), v.to_owned())).collect();
    if v2 {
        c.push(("delta.checkpointPolicy".into(), "v2".into()));
    }
    c
}

fn metadata_array(stats: &StatsMode, v2: bool) -> ArrayRef {
    strukt(vec![
        ("id", Arc::new(StringArray::from(vec!["5f0c6c8e-1b0a-4c43-9d2e-6c1f1c1a0b01"]))),
        ("name", new_null_array(&DataType::Utf8, 1)),
        ("description", new_null_array(&DataType::Utf8, 1)),
        (
            "format",
            strukt(vec![
                ("provider", Arc::new(StringArray::from(vec!["parquet"]))),
                ("options", map_of(std::iter::once(vec![]))),
            ]),
        ),
        ("schemaString", Arc::new(StringArray::from(vec![schema::schema_string()]))),
        ("partitionColumns", str_list(std::iter::once(vec!["date".to_owned()]))),
        ("configuration", map_of(std::iter::once(table_config(stats, v2)))),
        ("createdTime", Arc::new(Int64Array::from(vec![END_US / 1000 - 365 * DAY_US / 1000]))),
    ])
}

fn checkpoint_metadata_array(version: i64) -> ArrayRef {
    strukt(vec![("version", Arc::new(Int64Array::from(vec![version]))), ("tags", null_map(1))])
}

fn sidecar_array(rows: &[(String, i64, i64)]) -> ArrayRef {
    strukt(vec![
        ("path", Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0.as_str())))),
        ("sizeInBytes", Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1)))),
        ("modificationTime", Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2)))),
        ("tags", null_map(rows.len())),
    ])
}

/// A file schema plus one batch per action type.
struct Layout {
    schema: SchemaRef,
}

impl Layout {
    fn new(cols: &[(&str, DataType)]) -> Self {
        let fields: Vec<Field> = cols.iter().map(|(n, t)| Field::new(*n, t.clone(), true)).collect();
        Self { schema: Arc::new(Schema::new(fields)) }
    }

    fn batch(&self, name: &str, array: ArrayRef) -> Result<RecordBatch> {
        let n = array.len();
        let cols: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|f| if f.name() == name { Arc::clone(&array) } else { new_null_array(f.data_type(), n) })
            .collect();
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), cols)?)
    }
}

fn types(stats: &StatsMode, v2: bool) -> Vec<(&'static str, DataType)> {
    let one_add = add_array(&[AddRow { path: String::new(), date: String::new(), size: 0, mtime: 0, stats: None }]);
    let one_rm = remove_array(&[RemoveRow { path: String::new(), date: String::new(), size: 0, deleted: 0 }]);
    vec![
        ("txn", txn_array(&[]).data_type().clone()),
        ("add", one_add.data_type().clone()),
        ("remove", one_rm.data_type().clone()),
        ("metaData", metadata_array(stats, v2).data_type().clone()),
        ("protocol", protocol_array(v2).data_type().clone()),
        ("checkpointMetadata", checkpoint_metadata_array(0).data_type().clone()),
        ("sidecar", sidecar_array(&[]).data_type().clone()),
    ]
}

fn pick(all: &[(&'static str, DataType)], names: &[&str]) -> Vec<(&'static str, DataType)> {
    names.iter().filter_map(|n| all.iter().find(|(m, _)| m == n).cloned()).collect()
}

/// Deterministic row generators, indexable so threads can split the work.
#[derive(Clone)]
pub struct Rows {
    pub active: u64,
    pub tombstones: u64,
    pub tombstone_mib: u64,
    pub days: u32,
    pub stats: StatsMode,
    pub seed: u64,
}

impl Rows {
    pub fn adds(&self, from: u64, to: u64) -> Vec<AddRow> {
        let leaves = self.stats.leaves();
        let g = FileStatsGen { leaves: &leaves };
        let mut rng = SmallRng::seed_from_u64(self.seed ^ from.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let per_day = (self.active / u64::from(self.days)).max(1);
        let start_day = END_US / DAY_US - i64::from(self.days);
        (from..to)
            .map(|i| {
                let day = start_day + (i / per_day).min(u64::from(self.days) - 1) as i64;
                let within = (i % per_day) as i64 * (DAY_US / per_day as i64);
                let start_us = day * DAY_US + within;
                let date = schema::date_str(day);
                let size = (1i64 << 30) - rng.random_range(0..(1i64 << 26));
                let stats = (self.stats != StatsMode::None)
                    .then(|| {
                        let recs = 8_000_000 + rng.random_range(0..2_000_000);
                        g.stats(&mut rng, start_us, DAY_US / per_day as i64, recs)
                    });
                AddRow {
                    path: format!("date={date}/part-{i:010}-{:016x}.c000.snappy.parquet", rng.random::<u64>()),
                    date,
                    size,
                    mtime: start_us / 1000 + 60_000,
                    stats,
                }
            })
            .collect()
    }

    pub fn removes(&self, from: u64, to: u64) -> Vec<RemoveRow> {
        let mut rng = SmallRng::seed_from_u64(self.seed ^ 0xDEAD ^ from.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        (from..to)
            .map(|i| {
                let age = rng.random_range(0..7 * DAY_US);
                let day = (END_US - age) / DAY_US;
                let date = schema::date_str(day);
                let mib = self.tombstone_mib as i64;
                RemoveRow {
                    path: format!("date={date}/part-{i:010}-{:016x}.small.c000.snappy.parquet", rng.random::<u64>()),
                    date,
                    size: (mib << 20) - rng.random_range(0..(mib << 17).max(1)),
                    deleted: (END_US - age) / 1000,
                }
            })
            .collect()
    }
}

pub fn txn_rows(appids: u32) -> Vec<(String, i64, i64)> {
    let mut rng = SmallRng::seed_from_u64(7);
    let mut rows = Vec::new();
    for i in 0..appids {
        let inst = i / 10;
        let epoch = i % 10;
        // Older epochs are invalidated: 2^62 + v + 1.
        let v: i64 = rng.random_range(1_000..1_000_000);
        let version = if epoch < 9 { (1i64 << 62) + v + 1 } else { v };
        rows.push((format!("inst-{inst:04}/t#e{epoch}"), version, END_US / 1000 - rng.random_range(0..DAY_US / 1000)));
    }
    rows.push(("t#epoch".to_owned(), i64::from(appids / 10), END_US / 1000));
    rows
}

fn props(row_group: usize) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(row_group))
        .build()
}

#[derive(Clone, Copy)]
enum Kind {
    Add,
    Remove,
}

/// Write `[from, to)` rows of `kind` into `writer`, generating batches on `threads` threads.
async fn write_range(
    writer: &mut AsyncArrowWriter<BufWriter>,
    layout: &Layout,
    rows: &Rows,
    kind: Kind,
    from: u64,
    to: u64,
    batch: usize,
    threads: usize,
) -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ArrayRef>(threads * 2);
    let chunks: Vec<(u64, u64)> =
        (from..to).step_by(batch).map(|s| (s, (s + batch as u64).min(to))).collect();
    let chunks = Arc::new(std::sync::Mutex::new(chunks.into_iter()));
    let mut handles = Vec::new();
    for _ in 0..threads {
        let tx = tx.clone();
        let chunks = Arc::clone(&chunks);
        let rows = rows.clone();
        handles.push(std::thread::spawn(move || {
            loop {
                let next = chunks.lock().map(|mut c| c.next()).unwrap_or(None);
                let Some((a, b)) = next else { break };
                let arr = match kind {
                    Kind::Add => add_array(&rows.adds(a, b)),
                    Kind::Remove => remove_array(&rows.removes(a, b)),
                };
                if tx.blocking_send(arr).is_err() {
                    break;
                }
            }
        }));
    }
    drop(tx);
    let name = match kind {
        Kind::Add => "add",
        Kind::Remove => "remove",
    };
    while let Some(arr) = rx.recv().await {
        writer.write(&layout.batch(name, arr)?).await?;
    }
    for h in handles {
        h.join().map_err(|_| anyhow::anyhow!("generator thread panicked"))?;
    }
    Ok(())
}

fn log_path(prefix: &Path, name: &str) -> Path {
    Path::from(format!("{}/_delta_log/{name}", prefix.as_ref()))
}

async fn size_of(store: &Arc<dyn ObjectStore>, p: &Path) -> Result<i64> {
    Ok(i64::try_from(store.head(p).await?.size)?)
}

pub async fn run(args: GenArgs) -> Result<()> {
    let started = Instant::now();
    let (store, prefix) = store::open(&args.table_url)?;
    let v2 = args.format == Format::V2;
    let all = types(&args.stats, v2);
    let rows = Rows {
        active: args.active,
        tombstones: args.tombstones,
        tombstone_mib: args.tombstone_mib,
        days: args.days,
        stats: args.stats.clone(),
        seed: args.seed,
    };
    let txns = txn_rows(args.appids);
    let total_rows = args.active + args.tombstones + txns.len() as u64 + 2;
    let parts = u64::from(args.parts.max(1));
    let mut checkpoint_bytes = 0i64;
    let mut files = Vec::new();

    let split = |n: u64, k: u64, p: u64| (n * k / p, n * (k + 1) / p);
    match args.format {
        Format::Classic | Format::Multipart => {
            let layout = Layout::new(&pick(&all, &["txn", "add", "remove", "metaData", "protocol"]));
            let parts = if args.format == Format::Classic { 1 } else { parts };
            for k in 0..parts {
                let name = if parts == 1 {
                    format!("{:020}.checkpoint.parquet", args.version)
                } else {
                    format!("{:020}.checkpoint.{:010}.{:010}.parquet", args.version, k + 1, parts)
                };
                let path = log_path(&prefix, &name);
                let mut w = AsyncArrowWriter::try_new(
                    BufWriter::new(Arc::clone(&store), path.clone()),
                    Arc::clone(&layout.schema),
                    Some(props(args.row_group)),
                )?;
                if k == 0 {
                    w.write(&layout.batch("protocol", protocol_array(false))?).await?;
                    w.write(&layout.batch("metaData", metadata_array(&args.stats, false))?).await?;
                    w.write(&layout.batch("txn", txn_array(&txns))?).await?;
                }
                let (a, b) = split(args.active, k, parts);
                write_range(&mut w, &layout, &rows, Kind::Add, a, b, args.batch, args.threads).await?;
                let (a, b) = split(args.tombstones, k, parts);
                write_range(&mut w, &layout, &rows, Kind::Remove, a, b, args.batch, args.threads).await?;
                w.close().await?;
                checkpoint_bytes += size_of(&store, &path).await?;
                files.push(name);
            }
        }
        Format::V2 => {
            let side = Layout::new(&pick(&all, &["add", "remove"]));
            let mut sidecars = Vec::new();
            for k in 0..parts {
                let name = format!("{}.parquet", uuid::Uuid::new_v4());
                let path = log_path(&prefix, &format!("_sidecars/{name}"));
                let mut w = AsyncArrowWriter::try_new(
                    BufWriter::new(Arc::clone(&store), path.clone()),
                    Arc::clone(&side.schema),
                    Some(props(args.row_group)),
                )?;
                let (a, b) = split(args.active, k, parts);
                write_range(&mut w, &side, &rows, Kind::Add, a, b, args.batch, args.threads).await?;
                let (a, b) = split(args.tombstones, k, parts);
                write_range(&mut w, &side, &rows, Kind::Remove, a, b, args.batch, args.threads).await?;
                w.close().await?;
                let size = size_of(&store, &path).await?;
                checkpoint_bytes += size;
                sidecars.push((name, size, END_US / 1000));
            }
            let top = Layout::new(&pick(&all, &["checkpointMetadata", "txn", "metaData", "protocol", "sidecar"]));
            let name = format!("{:020}.checkpoint.{}.parquet", args.version, uuid::Uuid::new_v4());
            let path = log_path(&prefix, &name);
            let mut w = AsyncArrowWriter::try_new(
                BufWriter::new(Arc::clone(&store), path.clone()),
                Arc::clone(&top.schema),
                Some(props(args.row_group)),
            )?;
            w.write(&top.batch("checkpointMetadata", checkpoint_metadata_array(args.version))?).await?;
            w.write(&top.batch("protocol", protocol_array(true))?).await?;
            w.write(&top.batch("metaData", metadata_array(&args.stats, true))?).await?;
            w.write(&top.batch("txn", txn_array(&txns))?).await?;
            w.write(&top.batch("sidecar", sidecar_array(&sidecars))?).await?;
            w.close().await?;
            checkpoint_bytes += size_of(&store, &path).await?;
            files.push(name);
        }
    }

    let mut last = json!({"version": args.version, "size": total_rows, "sizeInBytes": checkpoint_bytes,
        "numOfAddFiles": args.active});
    if args.format == Format::Multipart {
        last["parts"] = json!(parts);
    }
    store
        .put(&log_path(&prefix, "_last_checkpoint"), last.to_string().into())
        .await
        .context("writing _last_checkpoint")?;

    // JSON tail: small blind appends by one writer.
    let tail_rows = Rows { active: u64::from(args.tail * args.tail_files).max(1), days: 1, ..rows.clone() };
    for t in 0..args.tail {
        let v = args.version + 1 + i64::from(t);
        let adds = tail_rows.adds(u64::from(t * args.tail_files), u64::from((t + 1) * args.tail_files));
        let mut body = String::new();
        let ts = END_US / 1000 + i64::from(t) * 100;
        body.push_str(&json!({"commitInfo": {"timestamp": ts, "operation": "WRITE",
            "operationParameters": {"mode": "Append"}, "isBlindAppend": true}}).to_string());
        body.push('\n');
        body.push_str(&json!({"txn": {"appId": "tail-writer", "version": i64::from(t) + 1, "lastUpdated": ts}}).to_string());
        body.push('\n');
        for a in adds {
            body.push_str(&json!({"add": {"path": format!("tail-{t}-{}", a.path.rsplit('/').next().unwrap_or("x")),
                "partitionValues": {"date": schema::date_str(END_US / DAY_US)}, "size": a.size,
                "modificationTime": ts, "dataChange": true, "stats": a.stats}}).to_string());
            body.push('\n');
        }
        store.put(&log_path(&prefix, &format!("{v:020}.json")), body.into()).await?;
    }

    println!(
        "{}",
        json!({"kind": "gen", "table": args.table_url.as_str(), "format": format!("{:?}", args.format),
            "active": args.active, "tombstones": args.tombstones, "stats": format!("{:?}", args.stats),
            "parts": parts, "checkpoint_bytes": checkpoint_bytes, "bytes_per_row": checkpoint_bytes as f64 / total_rows as f64,
            "files": files, "secs": started.elapsed().as_secs_f64()})
    );
    Ok(())
}
