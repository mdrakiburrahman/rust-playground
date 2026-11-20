use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use flate2::{Compression, GzBuilder};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tar::{Builder, Header};

use crate::{SafeOutputPath, SdkGenError, SdkGenResult};

/// Result of a deterministic generated-SDK package operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PackageResult {
    /// Workspace-relative archive path.
    pub archive: String,
    /// SHA-256 digest of the archive.
    pub sha256: String,
    /// Archive byte length.
    pub bytes: u64,
    /// Number of packaged regular files.
    pub file_count: usize,
}

/// Creates a deterministic gzip-compressed tar archive from generated output.
///
/// File paths are sorted, symlinks and non-regular files are rejected, and tar
/// ownership/timestamps/modes plus the gzip timestamp are normalized.
///
/// # Errors
///
/// Returns an error for unsafe paths, missing input, symlinks, archive paths
/// nested below the input directory, or filesystem/archive failures.
pub fn package_generated_sdk(input: &SafeOutputPath, archive: &SafeOutputPath) -> SdkGenResult<PackageResult> {
    let input_path = input.absolute();
    let archive_path = archive.absolute();
    if archive_path.starts_with(input_path) {
        return Err(SdkGenError::UnsafeOutputPath {
            path: archive.relative().to_path_buf(),
            reason: "archive must not be written inside the directory being packaged".to_owned(),
        });
    }
    let metadata = fs::symlink_metadata(input_path).map_err(|error| SdkGenError::io(input_path, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(SdkGenError::Artifact(format!(
            "package input must be a regular directory: {}",
            input_path.display()
        )));
    }
    let files = collect_regular_files(input_path)?;
    let parent = archive_path.parent().ok_or_else(|| SdkGenError::UnsafeOutputPath {
        path: archive.relative().to_path_buf(),
        reason: "archive has no parent".to_owned(),
    })?;
    fs::create_dir_all(parent).map_err(|error| SdkGenError::io(parent, error))?;
    let staging = archive_path.with_extension("arrowcol-sdk-gen-stage");
    remove_file_if_present(&staging)?;

    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)
        .map_err(|error| SdkGenError::io(&staging, error))?;
    let encoder = GzBuilder::new().mtime(0).write(file, Compression::best());
    let mut tar = Builder::new(encoder);
    tar.mode(tar::HeaderMode::Deterministic);
    for relative in &files {
        let source = input_path.join(relative);
        let bytes = fs::read(&source).map_err(|error| SdkGenError::io(&source, error))?;
        let mut header = Header::new_gnu();
        header.set_size(
            u64::try_from(bytes.len())
                .map_err(|_| SdkGenError::Artifact(format!("file is too large to package: {}", source.display())))?,
        );
        header.set_mode(0o644);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_cksum();
        tar.append_data(&mut header, relative, bytes.as_slice())
            .map_err(|error| SdkGenError::io(&staging, error))?;
    }
    let encoder = tar.into_inner().map_err(|error| SdkGenError::io(&staging, error))?;
    let mut file = encoder.finish().map_err(|error| SdkGenError::io(&staging, error))?;
    file.flush().map_err(|error| SdkGenError::io(&staging, error))?;
    file.sync_all().map_err(|error| SdkGenError::io(&staging, error))?;
    drop(file);

    if fs::symlink_metadata(archive_path).is_ok() {
        let archive_metadata =
            fs::symlink_metadata(archive_path).map_err(|error| SdkGenError::io(archive_path, error))?;
        if archive_metadata.file_type().is_symlink() || !archive_metadata.is_file() {
            let _ = remove_file_if_present(&staging);
            return Err(SdkGenError::UnsafeOutputPath {
                path: archive.relative().to_path_buf(),
                reason: "existing archive path must be a regular file".to_owned(),
            });
        }
        fs::remove_file(archive_path).map_err(|error| SdkGenError::io(archive_path, error))?;
    }
    fs::rename(&staging, archive_path).map_err(|error| SdkGenError::io(archive_path, error))?;
    let archive_bytes = fs::read(archive_path).map_err(|error| SdkGenError::io(archive_path, error))?;
    Ok(PackageResult {
        archive: archive.relative().to_string_lossy().replace('\\', "/"),
        sha256: format!("sha256:{}", hex::encode(Sha256::digest(&archive_bytes))),
        bytes: u64::try_from(archive_bytes.len())
            .map_err(|_| SdkGenError::Artifact("archive size exceeds u64".to_owned()))?,
        file_count: files.len(),
    })
}

fn collect_regular_files(root: &Path) -> SdkGenResult<Vec<PathBuf>> {
    let mut output = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory).map_err(|error| SdkGenError::io(&directory, error))?;
        for entry in entries {
            let entry = entry.map_err(|error| SdkGenError::io(&directory, error))?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|error| SdkGenError::io(&path, error))?;
            if metadata.file_type().is_symlink() {
                return Err(SdkGenError::Artifact(format!(
                    "generated package input contains a symlink: {}",
                    path.display()
                )));
            }
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() {
                output.push(
                    path.strip_prefix(root)
                        .map_err(|_| SdkGenError::InvalidGeneratedPath(path.clone()))?
                        .to_path_buf(),
                );
            } else {
                return Err(SdkGenError::Artifact(format!(
                    "generated package input contains a non-regular entry: {}",
                    path.display()
                )));
            }
        }
    }
    output.sort();
    Ok(output)
}

fn remove_file_if_present(path: &Path) -> SdkGenResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() || metadata.file_type().is_symlink() => {
            fs::remove_file(path).map_err(|error| SdkGenError::io(path, error))
        }
        Ok(_) => Err(SdkGenError::UnsafeOutputPath {
            path: path.to_path_buf(),
            reason: "staging archive path is not a regular file".to_owned(),
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(SdkGenError::io(path, error)),
    }
}
