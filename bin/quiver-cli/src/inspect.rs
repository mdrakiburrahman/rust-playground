//! Read-only commands: `tree`, `segments`, `show`, and `wal`.
//!
//! They never write, truncate, or lock anything, so they also work on a live
//! collector's directory, such as the loadtest's `direct-state-1/quiver`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use clap::Args;
use otel_arrow_dfe_quiver::inspect::{WalEntry, WalInspection, inspect_wal, wal_entry_batches};
use otel_arrow_dfe_quiver::segment::SegmentReader;

use crate::dfe::{DfeManifest, schema_description};
use crate::dir::{DEFAULT_DIR, MARKER_FILE, SegmentFile, Subscriber, file_name, file_size, segment_files, subscribers};
use crate::format::{self, Align, Table};

#[derive(Args)]
pub(crate) struct DirArgs {
    /// Quiver data directory.
    #[arg(default_value = DEFAULT_DIR)]
    pub(crate) dir: PathBuf,
}

#[derive(Args)]
pub(crate) struct ShowArgs {
    /// Quiver data directory.
    #[arg(default_value = DEFAULT_DIR)]
    dir: PathBuf,
    /// Segment sequence number [default: the oldest segment].
    #[arg(long, conflicts_with = "wal")]
    segment: Option<u64>,
    /// Bundle index within the segment.
    #[arg(long, default_value_t = 0, conflicts_with = "wal")]
    bundle: u32,
    /// Show a WAL entry by sequence instead of a segment bundle.
    #[arg(long)]
    wal: Option<u64>,
    /// Rows to print.
    #[arg(long, default_value_t = 5)]
    rows: usize,
    /// Comma-separated columns to print [default: all].
    #[arg(long, value_delimiter = ',')]
    columns: Vec<String>,
}

#[derive(Args)]
pub(crate) struct WalArgs {
    /// Quiver data directory.
    #[arg(default_value = DEFAULT_DIR)]
    dir: PathBuf,
    /// Newest entries to list; 0 lists every entry.
    #[arg(long, default_value_t = 12)]
    last: usize,
}

/// What one finalized segment holds.
struct SegmentSummary {
    file: SegmentFile,
    bundles: u32,
    rows: u64,
    streams: Vec<(u16, [u8; 32])>,
    content: String,
}

fn summarize(file: SegmentFile) -> Result<SegmentSummary> {
    let reader = SegmentReader::open(&file.path).with_context(|| format!("read {}", file.path.display()))?;
    let manifest = reader.manifest();
    Ok(SegmentSummary {
        bundles: u32::try_from(reader.bundle_count())?,
        rows: manifest.iter().map(|entry| entry.item_count()).sum(),
        streams: reader
            .streams()
            .iter()
            .map(|stream| (stream.slot_id.0, stream.schema_fingerprint))
            .collect(),
        content: manifest
            .first()
            .and_then(|entry| entry.metadata())
            .map_or_else(|| "-".to_owned(), describe_metadata),
        file,
    })
}

/// Segments that can still be read; a live collector may delete one between
/// listing and reading it.
fn summaries(dir: &Path) -> Result<Vec<SegmentSummary>> {
    Ok(segment_files(dir)?
        .into_iter()
        .filter_map(|file| summarize(file).ok())
        .collect())
}

/// Short description of DFE bundle metadata or an opaque manifest.
fn describe_metadata(bytes: &[u8]) -> String {
    match DfeManifest::decode(bytes) {
        Ok(manifest) => format!(
            "{}@{}.{} DFE bundle: {}",
            manifest.representation,
            manifest.major,
            manifest.minor,
            manifest.table_summary()
        ),
        Err(_) if bytes.starts_with(b"QPA") => "DFE pluggable-Arrow manifest".to_owned(),
        Err(_) => format!("{} bytes of opaque metadata", bytes.len()),
    }
}

fn require_dir(dir: &Path) -> Result<()> {
    if dir.is_dir() {
        Ok(())
    } else {
        bail!("{} does not exist; run `quiver-cli generate` first", dir.display())
    }
}

/// Per-core directories under a Dataflow collector's Quiver path.
fn core_dirs(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut cores = entries
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("core_"))
        .collect::<Vec<_>>();
    cores.sort();
    cores
}

pub(crate) fn tree(args: &DirArgs) -> Result<()> {
    let dir = &args.dir;
    require_dir(dir)?;
    let cores = core_dirs(dir);
    if !dir.join("wal").exists() && !cores.is_empty() {
        println!("{} holds one Quiver directory per Dataflow core:", dir.display());
        for core in &cores {
            println!("  {core}/");
            tree(&DirArgs { dir: dir.join(core) })?;
        }
        return Ok(());
    }
    let wal = inspect_wal(dir)?;
    let segments = summaries(dir)?;
    let subscribers = subscribers(dir)?;
    let segment_bundles = segments.iter().map(|segment| segment.bundles).sum::<u32>();

    let mut lines: Vec<(String, u64, String)> = Vec::new();
    let mut known: BTreeSet<String> = BTreeSet::from(["wal".to_owned(), "segments".to_owned()]);
    if dir.join(MARKER_FILE).is_file() {
        lines.push((
            format!("├── {MARKER_FILE}"),
            file_size(&dir.join(MARKER_FILE)),
            "how quiver-cli generated this directory".to_owned(),
        ));
        let _ = known.insert(MARKER_FILE.to_owned());
    }
    for subscriber in &subscribers {
        let acked = segments
            .iter()
            .map(|segment| subscriber.acked(segment.file.seq, segment.bundles))
            .sum::<u32>();
        lines.push((
            format!("├── {}", file_name(&subscriber.path)),
            file_size(&subscriber.path),
            if segment_bundles == 0 {
                format!(
                    "progress of subscriber {}: no readable segment bundles",
                    subscriber.name
                )
            } else {
                format!(
                    "progress of subscriber {}: {acked} of {segment_bundles} bundles in segments acknowledged",
                    subscriber.name
                )
            },
        ));
        let _ = known.insert(file_name(&subscriber.path));
    }
    for other in unknown_entries(dir, &known) {
        lines.push((
            format!("├── {other}"),
            file_size(&dir.join(&other)),
            "not a Quiver file".to_owned(),
        ));
    }

    lines.push(("├── wal/".to_owned(), 0, String::new()));
    for (index, file) in wal.files.iter().enumerate() {
        let entries = wal
            .entries
            .iter()
            .filter(|entry| entry.position >= file.start_position && entry.next_position <= file.end_position)
            .count();
        let meaning = if file.rotation_id.is_some() {
            format!("rotated write-ahead log: {entries} entries, deleted once the cursor passes them")
        } else {
            format!("write-ahead log: {entries} entries, each fsynced before its bundle was acknowledged")
        };
        let branch = if index + 1 == wal.files.len() && wal.cursor_position.is_none() {
            "└──"
        } else {
            "├──"
        };
        lines.push((format!("│   {branch} {}", file_name(&file.path)), file.bytes, meaning));
    }
    if let Some(cursor) = wal.cursor_position {
        let cursor_file = dir.join("wal").join("quiver.wal.cursor");
        let replayable = wal.unconsumed().count();
        let meaning = if replayable == 0 {
            format!(
                "cursor at {}: every entry is checkpointed, nothing to replay",
                format::count(cursor)
            )
        } else {
            format!(
                "cursor at {}: {replayable} uncheckpointed entries replay on next open",
                format::count(cursor)
            )
        };
        lines.push(("│   └── quiver.wal.cursor".to_owned(), file_size(&cursor_file), meaning));
    }
    lines.push(("└── segments/".to_owned(), 0, String::new()));
    for (index, segment) in segments.iter().enumerate() {
        let branch = if index + 1 == segments.len() {
            "└──"
        } else {
            "├──"
        };
        lines.push((
            format!("    {branch} {}", segment.file.name()),
            segment.file.bytes,
            format!(
                "{} bundles, {} rows, {} acknowledged",
                segment.bundles,
                format::count(segment.rows),
                acknowledgements(&subscribers, segment)
            ),
        ));
    }

    let total = lines.iter().map(|(_, bytes, _)| bytes).sum::<u64>();
    println!("{}  ({} in Quiver files)", dir.display(), format::bytes(total));
    let name_width = lines.iter().map(|(name, _, _)| name.chars().count()).max().unwrap_or(0);
    let size_width = lines
        .iter()
        .filter(|(_, _, meaning)| !meaning.is_empty())
        .map(|(_, bytes, _)| format::bytes(*bytes).len())
        .max()
        .unwrap_or(0);
    for (name, bytes, meaning) in lines {
        if meaning.is_empty() {
            println!("{name}");
        } else {
            let pad = name_width - name.chars().count();
            println!(
                "{name}{}  {:>size_width$}  {meaning}",
                " ".repeat(pad),
                format::bytes(bytes)
            );
        }
    }
    Ok(())
}

fn unknown_entries(dir: &Path, known: &BTreeSet<String>) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names = entries
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| !known.contains(name))
        .collect::<Vec<_>>();
    names.sort();
    names
}

pub(crate) fn segments(args: &DirArgs) -> Result<()> {
    let dir = crate::dir::resolve_core_dir(&args.dir)?;
    require_dir(&dir)?;
    let segments = summaries(&dir)?;
    let subscribers = subscribers(&dir)?;
    let bundles = segments.iter().map(|segment| u64::from(segment.bundles)).sum::<u64>();
    let rows = segments.iter().map(|segment| segment.rows).sum::<u64>();
    println!(
        "{} finalized segments in {}  ({} bundles, {} rows)",
        segments.len(),
        dir.display(),
        format::count(bundles),
        format::count(rows)
    );
    if segments.is_empty() {
        println!("  no readable finalized segments found; inspect WAL for replay status");
        return Ok(());
    }
    println!();
    let mut table = Table::new(&[
        ("segment", Align::Left),
        ("size", Align::Right),
        ("bundles", Align::Right),
        ("rows", Align::Right),
        ("acknowledged", Align::Right),
        ("contents", Align::Left),
    ]);
    for segment in &segments {
        table.row(vec![
            segment.file.name(),
            format::bytes(segment.file.bytes),
            format::count(u64::from(segment.bundles)),
            format::count(segment.rows),
            acknowledgements(&subscribers, segment),
            segment.content.clone(),
        ]);
    }
    println!("{}", table.render());
    println!();
    let streams = segments
        .iter()
        .flat_map(|segment| segment.streams.iter().copied())
        .collect::<BTreeSet<_>>();
    for (slot, fingerprint) in streams {
        println!(
            "  stream: slot {slot}, schema {}; one Arrow IPC stream per segment holds this slot's rows",
            format::fingerprint(&fingerprint)
        );
    }
    for subscriber in &subscribers {
        println!(
            "  {}: subscriber whose acknowledgements free a segment for deletion",
            subscriber.name
        );
    }
    Ok(())
}

/// `acked/bundles` per subscriber; a subscriber writes its progress file on
/// its first acknowledgement, so no file means nothing is acknowledged yet.
fn acknowledgements(subscribers: &[Subscriber], segment: &SegmentSummary) -> String {
    if subscribers.is_empty() {
        return format!("0/{}", segment.bundles);
    }
    subscribers
        .iter()
        .map(|subscriber| {
            format!(
                "{}/{}",
                subscriber.acked(segment.file.seq, segment.bundles),
                segment.bundles
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn show(args: &ShowArgs) -> Result<()> {
    let dir = crate::dir::resolve_core_dir(&args.dir)?;
    require_dir(&dir)?;
    let (title, metadata, batches) = match args.wal {
        Some(sequence) => wal_bundle(&dir, sequence)?,
        None => segment_bundle(&dir, args.segment, args.bundle)?,
    };
    let rows = batches.iter().map(|(_, batch)| batch.num_rows()).sum::<usize>();
    println!("{title}: {rows} rows");
    println!();
    print_metadata(metadata.as_deref());
    for (slot, batch) in batches {
        let batch = project(&batch, &args.columns)?;
        let shown = args.rows.min(batch.num_rows());
        println!();
        println!(
            "first {shown} of {} rows (slot {slot}, decoded from Arrow IPC)",
            format::count(u64::try_from(batch.num_rows())?)
        );
        println!("{}", pretty_format_batches(&[batch.slice(0, shown)])?);
    }
    Ok(())
}

type Bundle = (String, Option<Vec<u8>>, Vec<(u16, RecordBatch)>);

fn segment_bundle(dir: &Path, segment: Option<u64>, bundle: u32) -> Result<Bundle> {
    let files = segment_files(dir)?;
    let file = match segment {
        Some(seq) => files.into_iter().find(|file| file.seq == seq),
        None => files.into_iter().next(),
    }
    .with_context(|| "no such segment; list them with `quiver-cli segments`")?;
    let reader = SegmentReader::open(&file.path).with_context(|| format!("read {}", file.path.display()))?;
    let entry = reader
        .manifest()
        .iter()
        .find(|entry| entry.bundle_index == bundle)
        .with_context(|| format!("segment has {} bundles", reader.bundle_count()))?;
    let reconstructed = reader.read_bundle(entry)?;
    let metadata = reconstructed.metadata().map(<[u8]>::to_vec);
    let mut batches = reconstructed
        .into_payloads()
        .into_iter()
        .map(|(slot, batch)| (slot.0, batch))
        .collect::<Vec<_>>();
    batches.sort_by_key(|(slot, _)| *slot);
    Ok((format!("segment {}, bundle {bundle}", file.name()), metadata, batches))
}

fn wal_bundle(dir: &Path, sequence: u64) -> Result<Bundle> {
    let wal = inspect_wal(dir)?;
    let entry = wal
        .entries
        .iter()
        .find(|entry| entry.sequence == sequence)
        .with_context(|| "no such WAL entry; list them with `quiver-cli wal`")?;
    let batches = wal_entry_batches(dir, sequence)?
        .with_context(|| "the WAL entry disappeared while reading")?
        .into_iter()
        .map(|(slot, batch)| (slot.0, batch))
        .collect();
    Ok((
        format!("WAL entry {sequence} ({})", state(entry)),
        entry.metadata.clone(),
        batches,
    ))
}

fn print_metadata(metadata: Option<&[u8]>) {
    let Some(bytes) = metadata else {
        println!("no bundle metadata");
        return;
    };
    let Ok(manifest) = DfeManifest::decode(bytes) else {
        println!("bundle metadata: {}", describe_metadata(bytes));
        return;
    };
    println!(
        "manifest ({}@{}.{}, DFE durable_buffer pluggable Arrow)",
        manifest.representation, manifest.major, manifest.minor
    );
    let signal = match manifest.signal {
        0 => "logs",
        1 => "metrics",
        2 => "traces",
        _ => "unknown",
    };
    println!(
        "  signal    {signal}; {} root items",
        format::count(manifest.root_items)
    );
    for table in &manifest.tables {
        let schema = schema_description(&table.schema_id)
            .map(|(identity, version)| format!("{identity} v{version}"))
            .unwrap_or_else(|| table.schema_id.clone());
        println!("  table     {} (id {}, slot {})", schema, table.table_id, table.slot);
        println!("  schema    {}", format::fingerprint(&table.fingerprint));
    }
}

fn project(batch: &RecordBatch, columns: &[String]) -> Result<RecordBatch> {
    if columns.is_empty() {
        return Ok(batch.clone());
    }
    let schema = batch.schema();
    let indices = columns
        .iter()
        .map(|name| schema.index_of(name).with_context(|| format!("unknown column {name}")))
        .collect::<Result<Vec<_>>>()?;
    Ok(batch.project(&indices)?)
}

fn state(entry: &WalEntry) -> &'static str {
    if entry.consumed {
        "checkpointed: not replayed"
    } else {
        "WAL only: replays on next open"
    }
}

pub(crate) fn wal(args: &WalArgs) -> Result<()> {
    let dir = crate::dir::resolve_core_dir(&args.dir)?;
    require_dir(&dir)?;
    let wal = inspect_wal(&dir)?;
    print_wal(&dir, &wal, args.last)
}

fn print_wal(dir: &Path, wal: &WalInspection, last: usize) -> Result<()> {
    println!(
        "write-ahead log of {}: {} file(s), {} entries",
        dir.display(),
        wal.files.len(),
        wal.entries.len()
    );
    if wal.files.is_empty() {
        return Ok(());
    }
    println!();
    let mut files = Table::new(&[
        ("file", Align::Left),
        ("size", Align::Right),
        ("WAL positions", Align::Left),
    ]);
    for file in &wal.files {
        files.row(vec![
            file_name(&file.path),
            format::bytes(file.bytes),
            format!(
                "{} .. {}",
                format::count(file.start_position),
                format::count(file.end_position)
            ),
        ]);
    }
    println!("{}", files.render());
    println!();
    match wal.cursor_position {
        Some(cursor) => println!(
            "  cursor at {}: entries ending at or before it are checkpointed and skipped during replay; later entries replay on next open",
            format::count(cursor)
        ),
        None => println!("  no cursor: every entry replays on next open"),
    }
    let replayable = wal.unconsumed().count();
    println!(
        "  {} checkpointed, {replayable} WAL only",
        wal.entries.len() - replayable
    );
    println!();
    let skip = if last == 0 {
        0
    } else {
        wal.entries.len().saturating_sub(last)
    };
    if skip > 0 {
        println!("  newest {} of {} entries", wal.entries.len() - skip, wal.entries.len());
    }
    let mut table = Table::new(&[
        ("seq", Align::Right),
        ("position", Align::Right),
        ("bytes", Align::Right),
        ("rows", Align::Right),
        ("table", Align::Left),
        ("state", Align::Left),
    ]);
    for entry in &wal.entries[skip..] {
        let rows = entry.slots.iter().map(|slot| u64::from(slot.rows)).sum::<u64>();
        table.row(vec![
            entry.sequence.to_string(),
            format::count(entry.position),
            format::count(entry.next_position - entry.position),
            format::count(rows),
            entry
                .metadata
                .as_deref()
                .map_or_else(|| "-".to_owned(), describe_metadata),
            state(entry).to_owned(),
        ]);
    }
    println!("{}", table.render());
    if let Some(reason) = &wal.stopped_early {
        println!();
        println!("  stopped early: {reason} (a torn final entry is expected after a crash; replay drops it)");
    }
    Ok(())
}
