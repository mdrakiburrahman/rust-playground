//! `generate`: persist catalog-typed CSV rows as DFE pluggable-Arrow bundles.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Args;
use otel_arrow_dfe_quiver::record_bundle::RecordBundle;

use crate::data::{self, BUILTIN_CSV_PATH, DEFAULT_CATALOG_ENDPOINT, DEFAULT_TABLE};
use crate::dfe::DfeRecordBundle;
use crate::dir::{DEFAULT_DIR, MARKER_FILE};
use crate::engine;
use crate::format::{self, Align, Table};

#[derive(Args)]
pub(crate) struct GenerateArgs {
    /// Directory to write; replaced only when it carries quiver-cli's marker.
    #[arg(default_value = DEFAULT_DIR)]
    dir: PathBuf,
    /// Schema Catalog endpoint, or `memory` for the committed loadtest schema seed.
    #[arg(long, default_value = DEFAULT_CATALOG_ENDPOINT)]
    catalog_endpoint: String,
    /// CSV with a header row [default: the committed SQL Server wait stats].
    #[arg(long)]
    csv: Option<PathBuf>,
    /// Fully qualified catalog.schema.table identity.
    #[arg(long, default_value = DEFAULT_TABLE)]
    table: String,
    /// Rows per Quiver bundle.
    #[arg(long, default_value_t = 128)]
    batch_rows: usize,
    /// Bundles per explicit segment flush.
    #[arg(long, default_value_t = 8)]
    segment_bundles: usize,
}

pub(crate) async fn run(args: GenerateArgs) -> Result<()> {
    validate_destination(&args.dir)?;
    if args.segment_bundles == 0 {
        bail!("--segment-bundles must be positive");
    }
    let table = data::BenchmarkTable::load(&args.catalog_endpoint, &args.table).await?;
    let batches = data::read_csv(&table.arrow, args.csv.as_deref(), args.batch_rows)?;
    prepare(&args.dir)?;

    let engine = engine::open(&args.dir).await?;
    let mut rows = 0_u64;
    let mut segments = 0_u64;
    let bundle_count = batches.len();
    for (index, batch) in batches.into_iter().enumerate() {
        let bundle = DfeRecordBundle::new(&table.schema, batch)?;
        rows += bundle.item_count();
        engine.ingest(&bundle).await?;
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
    let marker = serde_json::json!({
        "created_by": "quiver-cli generate",
        "format": "DFE durable_buffer Quiver; QPA pluggable-Arrow bundle manifest",
        "catalog_endpoint": args.catalog_endpoint,
        "source_csv": source,
        "schema_snapshot": table.schema,
        "rows": rows,
        "bundles": bundle_count,
        "rows_per_bundle": args.batch_rows,
        "bundles_per_segment": args.segment_bundles
    });
    std::fs::write(args.dir.join(MARKER_FILE), serde_json::to_vec_pretty(&marker)?)?;

    println!("generated {} from {source}", args.dir.display());
    println!();
    println!(
        "  table   {}  (schema v{}, {})",
        table.schema.identity(),
        table.schema.version().get(),
        format::fingerprint(table.schema.fingerprint().as_bytes())
    );
    println!("  format  arrowcol.flight@1.1 QPA bundles; WAL fsync before each durable-buffer acknowledgement");
    println!();
    let mut summary = Table::new(&[
        ("rows", Align::Right),
        ("bundles", Align::Right),
        ("WAL fsyncs", Align::Right),
        ("segments", Align::Right),
        ("on disk", Align::Right),
    ]);
    let bundle_count = u64::try_from(bundle_count)?;
    summary.row(vec![
        format::count(rows),
        format::count(bundle_count),
        format::count(bundle_count),
        format::count(segments),
        format::bytes(directory_size(&args.dir)),
    ]);
    println!("{}", summary.render());
    println!();
    println!("next: quiver-cli tree {}", args.dir.display());
    Ok(())
}

/// Replaces only a marked CLI directory or an empty destination.
fn prepare(dir: &Path) -> Result<()> {
    validate_destination(dir)?;
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

fn validate_destination(dir: &Path) -> Result<()> {
    if dir.join(MARKER_FILE).is_file() {
        return Ok(());
    }
    if dir.is_dir() && std::fs::read_dir(dir)?.next().is_some() {
        bail!(
            "{} is not empty and was not created by quiver-cli; choose another directory",
            dir.display()
        );
    }
    Ok(())
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
