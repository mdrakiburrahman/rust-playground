//! Immutable, synchronized objects. Only authority-head traversal determines visibility.
use crate::{Config, Head, Manifest, Writer, safe_id};
use anyhow::{Result, ensure};
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
/// # Errors
/// Rejects unconfined table, writer and commit identities.
pub fn namespace(table: &str, writer: &Writer, commit: &str) -> Result<String> {
    ensure!(
        [table, &writer.collector, &writer.process, &writer.generation, commit]
            .iter()
            .all(|s| safe_id(s)),
        "unsafe object identity"
    );
    Ok(format!(
        "{table}/{}/{}/{}/{commit}",
        writer.collector, writer.process, writer.generation
    ))
}
fn write_once(root: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let path = root.join(name);
    let parent = path.parent().ok_or_else(|| anyhow::anyhow!("missing parent"))?;
    fs::create_dir_all(parent)?;
    let mut current = parent.to_path_buf();
    loop {
        File::open(&current)?.sync_all()?;
        if current == root {
            break;
        }
        current = current
            .parent()
            .ok_or_else(|| anyhow::anyhow!("invalid storage ancestry"))?
            .to_path_buf();
    }
    let mut file = OpenOptions::new().write(true).create_new(true).open(&path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
/// # Errors
/// Returns encoding, immutable-create, or synchronization errors. Staging never grants visibility.
pub fn stage(config: &Config, mut manifest: Manifest, batch: &RecordBatch) -> Result<Head> {
    let prefix = namespace(&manifest.table, &manifest.writer, &manifest.commit)?;
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
    }
    manifest.parquet = format!("{prefix}.parquet");
    manifest.parquet_sha256 = digest(&bytes);
    write_once(&config.storage, &manifest.parquet, &bytes)?;
    let bytes = serde_json::to_vec(&manifest)?;
    let name = format!("{prefix}.json");
    write_once(&config.storage, &name, &bytes)?;
    Ok(Head {
        commit: manifest.commit,
        manifest: name,
        sha256: digest(&bytes),
        sequence: manifest.parent.map_or(1, |h| h.sequence + 1),
    })
}
fn confined(root: &Path, name: &str) -> Result<PathBuf> {
    ensure!(
        !name.is_empty()
            && Path::new(name)
                .components()
                .all(|c| matches!(c, std::path::Component::Normal(_))),
        "unconfined manifest reference"
    );
    let path = root.join(name);
    ensure!(
        path.canonicalize()?.starts_with(root.canonicalize()?),
        "object escaped storage root"
    );
    Ok(path)
}
/// # Errors
/// Rejects corrupt, oversized or incorrectly bound manifest references.
pub fn load(config: &Config, head: &Head) -> Result<Manifest> {
    let path = confined(&config.storage, &head.manifest)?;
    ensure!(fs::metadata(&path)?.len() <= 64 * 1024, "manifest size bound");
    let bytes = fs::read(path)?;
    ensure!(digest(&bytes) == head.sha256, "manifest digest mismatch");
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    let prefix = namespace(&manifest.table, &manifest.writer, &manifest.commit)?;
    ensure!(
        head.commit == manifest.commit && head.manifest == format!("{prefix}.json"),
        "manifest identity mismatch"
    );
    ensure!(
        manifest.parquet == format!("{prefix}.parquet"),
        "writer namespace mismatch"
    );
    ensure!(
        head.sequence == manifest.parent.as_ref().map_or(1, |p| p.sequence + 1),
        "manifest sequence mismatch"
    );
    Ok(manifest)
}
/// # Errors
/// Rejects corrupt objects, unregistered schemas and inconsistent Parquet metadata.
pub fn validate(config: &Config, head: &Head) -> Result<Manifest> {
    let manifest = load(config, head)?;
    ensure!(
        config
            .table(&manifest.table)?
            .fingerprints
            .contains(&manifest.fingerprint),
        "unregistered schema"
    );
    let path = confined(&config.storage, &manifest.parquet)?;
    ensure!(fs::metadata(&path)?.len() <= 32 * 1024 * 1024, "parquet size bound");
    let bytes = fs::read(&path)?;
    ensure!(digest(&bytes) == manifest.parquet_sha256, "parquet digest mismatch");
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    ensure!(
        reader.metadata().file_metadata().num_rows() == manifest.rows as i64,
        "row count mismatch"
    );
    ensure!(
        arrowcol_protocol::fingerprint_schema(reader.schema()).to_string() == manifest.fingerprint,
        "parquet schema mismatch"
    );
    Ok(manifest)
}
/// # Errors
/// Returns corrupt-history or explicit recovery-depth-limit errors.
pub fn contains(config: &Config, mut head: Option<Head>, commit: &Head) -> Result<bool> {
    for _ in 0..100_000 {
        let Some(current) = head else { return Ok(false) };
        if current == *commit {
            return Ok(true);
        }
        if current.sequence <= commit.sequence {
            return Ok(false);
        }
        head = load(config, &current)?.parent;
    }
    anyhow::bail!("manifest recovery depth exceeded; checkpoint required")
}
