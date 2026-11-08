//! Read-only view of the files in a Quiver data directory.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use otel_arrow_dfe_quiver::subscriber::{
    SegmentProgressEntry, SubscriberId, progress_file_path, read_progress_file, scan_progress_files,
};

/// Directory quiver-cli writes to by default (gitignored).
pub(crate) const DEFAULT_DIR: &str = "onelake/quiver-demo";
/// File quiver-cli writes into every directory it generates.
pub(crate) const MARKER_FILE: &str = "quiver-cli.json";

/// One finalized segment file.
pub(crate) struct SegmentFile {
    pub(crate) seq: u64,
    pub(crate) path: PathBuf,
    pub(crate) bytes: u64,
}

impl SegmentFile {
    pub(crate) fn name(&self) -> String {
        file_name(&self.path)
    }
}

/// Finalized segments in sequence order; empty when there are none.
pub(crate) fn segment_files(dir: &Path) -> Result<Vec<SegmentFile>> {
    let segments = dir.join("segments");
    if !segments.is_dir() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&segments).with_context(|| format!("list {}", segments.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("qseg") {
            continue;
        }
        let Some(seq) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| stem.parse().ok())
        else {
            continue;
        };
        files.push(SegmentFile {
            seq,
            bytes: file_size(&path),
            path,
        });
    }
    files.sort_by_key(|file| file.seq);
    Ok(files)
}

/// Resolves a Quiver data directory or a parent containing one core.
pub(crate) fn resolve_core_dir(dir: &Path) -> Result<PathBuf> {
    if dir.join("wal").is_dir() || dir.join("segments").is_dir() {
        return Ok(dir.to_path_buf());
    }
    let mut cores = std::fs::read_dir(dir)
        .with_context(|| format!("list {}", dir.display()))?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("core_"))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    cores.sort();
    match cores.as_slice() {
        [core] => Ok(core.clone()),
        [] => bail!("{} has no Quiver core directory", dir.display()),
        _ => bail!(
            "{} has multiple Quiver cores; pass one core_N directory: {}",
            dir.display(),
            cores.iter().map(|core| file_name(core)).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// Acknowledgement progress of one subscriber.
pub(crate) struct Subscriber {
    pub(crate) name: String,
    pub(crate) path: PathBuf,
    oldest_incomplete: u64,
    entries: Vec<SegmentProgressEntry>,
}

impl Subscriber {
    /// Bundles of segment `seq` (holding `bundles`) this subscriber acknowledged.
    pub(crate) fn acked(&self, seq: u64, bundles: u32) -> u32 {
        if seq < self.oldest_incomplete {
            return bundles;
        }
        self.entries
            .iter()
            .find(|entry| entry.seg_seq.raw() == seq)
            .map_or(0, SegmentProgressEntry::acked_count)
    }
}

/// Every subscriber with a progress file in the directory.
pub(crate) fn subscribers(dir: &Path) -> Result<Vec<Subscriber>> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut subscribers = scan_progress_files(dir)?
        .into_iter()
        .map(|id| subscriber(dir, &id))
        .collect::<Result<Vec<_>>>()?;
    subscribers.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(subscribers)
}

fn subscriber(dir: &Path, id: &SubscriberId) -> Result<Subscriber> {
    let path = progress_file_path(dir, id);
    let (oldest_incomplete, entries) = read_progress_file(&path).with_context(|| format!("read {}", path.display()))?;
    Ok(Subscriber {
        name: id.as_str().to_owned(),
        path,
        oldest_incomplete: oldest_incomplete.raw(),
        entries,
    })
}

/// Returns whether a real directory contains a regular CLI ownership marker.
pub(crate) fn has_cli_marker(dir: &Path) -> bool {
    std::fs::symlink_metadata(dir).is_ok_and(|metadata| metadata.is_dir())
        && std::fs::symlink_metadata(dir.join(MARKER_FILE)).is_ok_and(|metadata| metadata.is_file())
}

/// Refuses to write to a directory quiver-cli did not create, so a writer
/// never opens a live collector's directory next to the collector.
pub(crate) fn require_cli_dir(dir: &Path) -> Result<()> {
    if has_cli_marker(dir) {
        return Ok(());
    }
    if !dir.exists() {
        bail!("{} does not exist; run `quiver-cli generate` first", dir.display());
    }
    bail!(
        "{} was not created by quiver-cli; only the read-only commands (tree, segments, show, wal) may open it",
        dir.display()
    )
}

/// Size of a file, or 0 when it cannot be read.
pub(crate) fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |metadata| metadata.len())
}

/// Last path component as a string.
pub(crate) fn file_name(path: &Path) -> String {
    path.file_name().map_or_else(
        || path.display().to_string(),
        |name| name.to_string_lossy().into_owned(),
    )
}
