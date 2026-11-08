//! `generate`: persist catalog-typed CSV rows as DFE pluggable-Arrow bundles.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use arrowcol_catalog::CatalogSchemaVersion;
use clap::Args;
use otel_arrow_dfe_quiver::record_bundle::RecordBundle;
use serde_json::{Value, json};

use crate::data::{self, BUILTIN_CSV_PATH, DEFAULT_CATALOG_ENDPOINT, DEFAULT_TABLE};
use crate::dfe::DfeRecordBundle;
use crate::dir::{DEFAULT_DIR, MARKER_FILE, has_cli_marker};
use crate::engine;
use crate::format::{self, Align, Table};

#[derive(Args)]
pub(crate) struct GenerateArgs {
    /// Directory to write; only marked CLI directories can be replaced.
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
    let source = args
        .csv
        .as_ref()
        .map_or_else(|| BUILTIN_CSV_PATH.to_owned(), |path| path.display().to_string());
    let mut marker = initial_marker(&args, &table.schema, &source);
    prepare(&args.dir, &marker)?;

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

    marker["generation_status"] = json!("complete");
    marker["rows"] = json!(rows);
    marker["bundles"] = json!(bundle_count);
    write_completion_marker(&args.dir, &marker)?;

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

fn initial_marker(args: &GenerateArgs, schema: &CatalogSchemaVersion, source: &str) -> Value {
    json!({
        "created_by": "quiver-cli generate",
        "format": "DFE durable_buffer Quiver; QPA pluggable-Arrow bundle manifest",
        "generation_status": "in_progress",
        "catalog_endpoint": args.catalog_endpoint,
        "source_csv": source,
        "schema_snapshot": schema,
        "rows": 0,
        "bundles": 0,
        "rows_per_bundle": args.batch_rows,
        "bundles_per_segment": args.segment_bundles
    })
}

fn prepare(dir: &Path, marker: &Value) -> Result<()> {
    validate_destination(dir)?;
    if has_cli_marker(dir) {
        fs::remove_dir_all(dir).with_context(|| format!("replace {}", dir.display()))?;
    } else if !dir.exists() {
        let parent = dir
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent).with_context(|| format!("create parent {}", parent.display()))?;
        fs::create_dir(dir).with_context(|| format!("claim {}", dir.display()))?;
    }

    let marker_path = dir.join(MARKER_FILE);
    let bytes = serde_json::to_vec_pretty(marker)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker_path)
        .with_context(|| format!("claim {} with {MARKER_FILE}", dir.display()))?;
    file.write_all(&bytes)
        .with_context(|| format!("write {}", marker_path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync {}", marker_path.display()))?;
    OpenOptions::new()
        .read(true)
        .open(dir)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("sync directory {}", dir.display()))?;

    let unexpected = fs::read_dir(dir)?
        .filter_map(std::io::Result::ok)
        .any(|entry| entry.file_name() != MARKER_FILE);
    if unexpected {
        fs::remove_file(&marker_path).with_context(|| format!("release {}", marker_path.display()))?;
        bail!(
            "{} changed while quiver-cli was claiming it; refusing to write",
            dir.display()
        );
    }
    Ok(())
}

fn write_completion_marker(dir: &Path, marker: &Value) -> Result<()> {
    if !has_cli_marker(dir) {
        bail!(
            "{} lost its quiver-cli ownership marker during generation",
            dir.display()
        );
    }
    let marker_path = dir.join(MARKER_FILE);
    let temporary = dir.join(format!(".{MARKER_FILE}.{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec_pretty(marker)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .with_context(|| format!("create {}", temporary.display()))?;
    file.write_all(&bytes)
        .with_context(|| format!("write {}", temporary.display()))?;
    file.sync_all()
        .with_context(|| format!("sync {}", temporary.display()))?;
    if !has_cli_marker(dir) {
        fs::remove_file(&temporary).with_context(|| format!("remove {}", temporary.display()))?;
        bail!(
            "{} lost its quiver-cli ownership marker during generation",
            dir.display()
        );
    }
    fs::rename(&temporary, &marker_path).with_context(|| format!("update {}", marker_path.display()))?;
    OpenOptions::new()
        .read(true)
        .open(dir)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("sync directory {}", dir.display()))?;
    Ok(())
}

fn validate_destination(dir: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("inspect {}", dir.display())),
    };
    if !metadata.file_type().is_dir() {
        bail!("{} is not a real directory; choose another path", dir.display());
    }
    if !has_cli_marker(dir) && fs::read_dir(dir)?.next().is_some() {
        bail!(
            "{} is not empty and was not created by quiver-cli; choose another directory",
            dir.display()
        );
    }
    Ok(())
}

fn directory_size(dir: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(dir) else {
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

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use otel_arrow_dfe_quiver::inspect::inspect_wal;
    use otel_arrow_dfe_quiver::segment::SegmentReader;

    use super::*;
    use crate::data::{BenchmarkTable, DEFAULT_TABLE};

    #[tokio::test]
    async fn incomplete_generation_marker_keeps_durable_rows_recoverable() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let dir = temp.path().join("interrupted");
        let args = GenerateArgs {
            dir: dir.clone(),
            catalog_endpoint: "memory".to_owned(),
            csv: None,
            table: DEFAULT_TABLE.to_owned(),
            batch_rows: 128,
            segment_bundles: 8,
        };
        let table = BenchmarkTable::load("memory", DEFAULT_TABLE).await?;
        let mut batches = data::read_csv(&table.arrow, None, args.batch_rows)?;
        let marker = initial_marker(&args, &table.schema, BUILTIN_CSV_PATH);
        prepare(&dir, &marker)?;
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(dir.join(MARKER_FILE))?)?["generation_status"],
            "in_progress"
        );

        let engine = engine::open_for_crash(&dir).await?;
        engine
            .ingest(&DfeRecordBundle::new(&table.schema, batches.remove(0))?)
            .await?;
        drop(engine);

        let before = inspect_wal(&dir.join("core_0"))?;
        assert_eq!(before.unconsumed().count(), 1);
        crate::lifecycle::recover(&dir).await?;
        let after = inspect_wal(&dir.join("core_0"))?;
        assert_eq!(after.unconsumed().count(), 0);
        let rows = crate::dir::segment_files(&dir.join("core_0"))?
            .iter()
            .map(|file| {
                SegmentReader::open(&file.path)
                    .map(|reader| reader.manifest().iter().map(|entry| entry.item_count()).sum::<u64>())
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<u64>();
        assert_eq!(rows, 128);
        let marker: Value = serde_json::from_slice(&fs::read(dir.join(MARKER_FILE))?)?;
        assert_eq!(marker["generation_status"], "in_progress");
        assert_eq!(marker["rows"], 0);
        Ok(())
    }
}
