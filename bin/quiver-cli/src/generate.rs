//! `generate`: turn CSV rows into Quiver storage exactly as the Direct
//! collector persists deliveries.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use arrow::record_batch::RecordBatch;
use arrowcol_buffer::{DELIVERY_MANIFEST_FORMAT, DELIVERY_MANIFEST_VERSION, DeliveryBundle, DeliveryManifest};
use arrowcol_protocol::{CollectorInstanceId, LeaseId, ReceiptId};
use clap::Args;

use crate::data::{BUILTIN_CSV_PATH, BenchmarkTable, DEFAULT_TABLE, read_csv};
use crate::dir::{DEFAULT_DIR, MARKER_FILE};
use crate::engine::{self, MANUAL_SEGMENTS};
use crate::format::{self, Align, Table};

#[derive(Args)]
pub(crate) struct GenerateArgs {
    /// Directory to write; replaced if quiver-cli generated it before.
    #[arg(default_value = DEFAULT_DIR)]
    dir: PathBuf,
    /// CSV with a header row [default: the committed real SQL Server wait stats].
    #[arg(long)]
    csv: Option<PathBuf>,
    /// Loadtest table the rows belong to; its DDL gives the Arrow schema.
    #[arg(long, default_value = DEFAULT_TABLE)]
    table: String,
    /// Rows per bundle: one Arrow record batch, like one Flight delivery.
    #[arg(long, default_value_t = 128)]
    batch_rows: usize,
    /// Bundles per segment. The collector finalizes a segment every 500 ms
    /// instead; a fixed count keeps the output repeatable.
    #[arg(long, default_value_t = 8)]
    segment_bundles: usize,
}

pub(crate) async fn run(args: GenerateArgs) -> Result<()> {
    if args.segment_bundles == 0 {
        bail!("--segment-bundles must be positive");
    }
    let table = BenchmarkTable::load(&args.table)?;
    let batches = read_csv(&table, args.csv.as_deref(), args.batch_rows)?;
    prepare(&args.dir)?;

    let engine = engine::open(&args.dir, MANUAL_SEGMENTS).await?;
    let owner = CollectorInstanceId::new();
    let lease = LeaseId::new();
    let mut rows = 0_u64;
    let mut segments = 0_u64;
    let bundle_count = batches.len();
    for (index, batch) in batches.into_iter().enumerate() {
        let batch_rows = u64::try_from(batch.num_rows())?;
        // With a zero flush interval, ingest returns only after the WAL fsync,
        // which is when the Direct collector acknowledges a delivery.
        engine.ingest(&delivery_bundle(&table, owner, lease, batch)?).await?;
        rows += batch_rows;
        if (index + 1) % args.segment_bundles == 0 || index + 1 == bundle_count {
            engine.flush().await?;
            segments += 1;
        }
    }
    let _ = engine.maintain().await?;
    engine.shutdown().await?;

    let source = args
        .csv
        .as_ref()
        .map_or_else(|| BUILTIN_CSV_PATH.to_owned(), |path| path.display().to_string());
    let fingerprint = format::fingerprint(table.schema.fingerprint().as_bytes());
    let marker = serde_json::json!({
        "created_by": "quiver-cli generate",
        "format": format!("{DELIVERY_MANIFEST_FORMAT}@{DELIVERY_MANIFEST_VERSION}, the Direct collector's Quiver bundle format"),
        "source_csv": source,
        "table": table.schema.identity().to_string(),
        "table_id": table.schema.table_id(),
        "schema_version": table.schema.version().get(),
        "schema_fingerprint": table.schema.fingerprint().to_string(),
        "rows": rows,
        "bundles": bundle_count,
        "rows_per_bundle": args.batch_rows,
        "bundles_per_segment": args.segment_bundles,
        "synthetic": "receipt, lease, and owner ids are generated locally; no SQL Server is involved",
    });
    std::fs::write(args.dir.join(MARKER_FILE), serde_json::to_vec_pretty(&marker)?)?;

    println!("generated {} from {source}", args.dir.display());
    println!();
    println!(
        "  table   {}  (schema v{}, {fingerprint}, as registered by the benchmark)",
        table.schema.identity(),
        table.schema.version().get()
    );
    println!(
        "  format  {DELIVERY_MANIFEST_FORMAT}@{DELIVERY_MANIFEST_VERSION} bundles; WAL fsync before each bundle is acknowledged"
    );
    println!();
    let mut summary = Table::new(&[
        ("rows", Align::Right),
        ("bundles", Align::Right),
        ("WAL fsyncs", Align::Right),
        ("segments", Align::Right),
        ("on disk", Align::Right),
    ]);
    let bundles = u64::try_from(bundle_count)?;
    summary.row(vec![
        format::count(rows),
        format::count(bundles),
        format::count(bundles),
        format::count(segments),
        format::bytes(directory_size(&args.dir)),
    ]);
    println!("{}", summary.render());
    println!();
    println!("next: quiver-cli tree {}", args.dir.display());
    Ok(())
}

/// The bundle the Direct collector persists for one delivery of `batch`, with
/// a fresh receipt id; receipt, lease, and owner ids are synthetic.
pub(crate) fn delivery_bundle(
    table: &BenchmarkTable,
    owner: CollectorInstanceId,
    lease: LeaseId,
    batch: RecordBatch,
) -> Result<DeliveryBundle> {
    let manifest = DeliveryManifest {
        format: DELIVERY_MANIFEST_FORMAT.to_owned(),
        version: DELIVERY_MANIFEST_VERSION,
        receipt_id: ReceiptId::new(),
        ingested_at_ms: Some(arrowcol_protocol::UnixMillis::new(u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
        )?)?),
        receipt_revision: 0,
        owner_instance_id: owner,
        lease_id: lease,
        fencing_token: 1,
        table_id: table.schema.table_id(),
        table: table.schema.identity().clone(),
        schema_version: table.schema.version(),
        schema_fingerprint: table.schema.fingerprint(),
        rows: u64::try_from(batch.num_rows())?,
        batches: 1,
        arrow_memory_bytes: u64::try_from(batch.get_array_memory_size())?,
    };
    Ok(DeliveryBundle::new(&manifest, batch)?)
}

/// Creates the directory, replacing a previous quiver-cli directory and
/// refusing anything else that is not empty.
fn prepare(dir: &Path) -> Result<()> {
    if dir.join(MARKER_FILE).is_file() {
        std::fs::remove_dir_all(dir).with_context(|| format!("replace {}", dir.display()))?;
    } else if dir.is_dir() && std::fs::read_dir(dir)?.next().is_some() {
        bail!(
            "{} is not empty and was not created by quiver-cli; choose another directory",
            dir.display()
        );
    }
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))
}

fn directory_size(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                directory_size(&path)
            } else {
                crate::dir::file_size(&path)
            }
        })
        .sum()
}
