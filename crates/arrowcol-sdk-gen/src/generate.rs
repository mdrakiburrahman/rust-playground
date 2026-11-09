use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use serde::Serialize;

use crate::{
    ArtifactBundle, SafeOutputPath, SdkGenError, SdkGenResult,
    path::validate_relative_generated_path,
    render::{RenderedSdk, render_sdk},
};

/// Result of deterministic SDK generation or checking.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GenerationResult {
    /// Workspace-relative generated output directory.
    pub output: String,
    /// Workspace-relative machine-readable manifest path.
    pub manifest: String,
    /// Generated Rust crate package name.
    pub rust_package: String,
    /// Generated TypeScript package name.
    pub typescript_package: String,
    /// Number of files in the generated directory.
    pub file_count: usize,
}

/// Generates both SDKs from one validated artifact bundle and atomically replaces the output.
///
/// # Errors
///
/// Returns an error for unsupported/lossy types, unsafe paths, identifier
/// collisions, rendering failures, or filesystem failures.
pub fn generate_sdk(bundle: &ArtifactBundle, output: &SafeOutputPath) -> SdkGenResult<GenerationResult> {
    let rendered = render_sdk(bundle)?;
    replace_directory(output, &rendered.files)?;
    Ok(result(output, &rendered))
}

/// Verifies byte-for-byte deterministic regeneration without mutating output.
///
/// # Errors
///
/// Returns an error when generated output is missing, stale, contains extra
/// files, or differs byte-for-byte from fresh rendering.
pub fn check_generated_sdk(bundle: &ArtifactBundle, output: &SafeOutputPath) -> SdkGenResult<GenerationResult> {
    let rendered = render_sdk(bundle)?;
    check_directory(output.absolute(), &rendered.files)?;
    Ok(result(output, &rendered))
}

/// Writes a canonical immutable artifact fixture with atomic directory replacement.
///
/// This is intended for tests and examples that first create a catalog schema;
/// production generation should consume artifacts exported by the catalog.
///
/// # Errors
///
/// Returns an error for unsafe paths or filesystem failures.
pub fn write_artifact_bundle(bundle: &ArtifactBundle, output: &SafeOutputPath) -> SdkGenResult<()> {
    let files = BTreeMap::from([
        (PathBuf::from("manifest.json"), bundle.manifest().to_vec()),
        (PathBuf::from("schema.arrow"), bundle.arrow_schema().to_vec()),
        (PathBuf::from("descriptor.pb"), bundle.protobuf_descriptor().to_vec()),
        (PathBuf::from("schema.proto"), bundle.protobuf_source().to_vec()),
    ]);
    replace_directory(output, &files)
}

pub(crate) fn replace_directory(output: &SafeOutputPath, files: &BTreeMap<PathBuf, Vec<u8>>) -> SdkGenResult<()> {
    for path in files.keys() {
        validate_relative_generated_path(path)?;
    }
    let destination = output.absolute();
    let parent = destination.parent().ok_or_else(|| SdkGenError::UnsafeOutputPath {
        path: output.relative().to_path_buf(),
        reason: "output has no parent".to_owned(),
    })?;
    create_directories_without_symlinks(output.workspace(), parent)?;
    let leaf = destination
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| SdkGenError::UnsafeOutputPath {
            path: output.relative().to_path_buf(),
            reason: "output leaf must be UTF-8".to_owned(),
        })?;
    let stage = parent.join(format!(".{leaf}.arrowcol-sdk-gen-stage"));
    let backup = parent.join(format!(".{leaf}.arrowcol-sdk-gen-backup"));
    remove_entry(&stage)?;
    remove_entry(&backup)?;
    fs::create_dir(&stage).map_err(|error| SdkGenError::io(&stage, error))?;

    let write_result = (|| {
        for (relative, bytes) in files {
            let path = stage.join(relative);
            let parent = path
                .parent()
                .ok_or_else(|| SdkGenError::InvalidGeneratedPath(relative.clone()))?;
            fs::create_dir_all(parent).map_err(|error| SdkGenError::io(parent, error))?;
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|error| SdkGenError::io(&path, error))?;
            file.write_all(bytes).map_err(|error| SdkGenError::io(&path, error))?;
            file.sync_all().map_err(|error| SdkGenError::io(&path, error))?;
        }
        sync_directory(&stage)?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = remove_entry(&stage);
        return Err(error);
    }

    let destination_exists = fs::symlink_metadata(destination).is_ok();
    if destination_exists {
        ensure_directory_not_symlink(destination)?;
        fs::rename(destination, &backup).map_err(|error| SdkGenError::io(destination, error))?;
    }
    if let Err(error) = fs::rename(&stage, destination) {
        if destination_exists {
            let _ = fs::rename(&backup, destination);
        }
        let _ = remove_entry(&stage);
        return Err(SdkGenError::io(destination, error));
    }
    sync_directory(parent)?;
    if destination_exists {
        remove_entry(&backup)?;
    }
    Ok(())
}

fn result(output: &SafeOutputPath, rendered: &RenderedSdk) -> GenerationResult {
    GenerationResult {
        output: output.relative().to_string_lossy().replace('\\', "/"),
        manifest: output
            .relative()
            .join("sdk-manifest.json")
            .to_string_lossy()
            .replace('\\', "/"),
        rust_package: rendered.rust_package.clone(),
        typescript_package: rendered.typescript_package.clone(),
        file_count: rendered.files.len(),
    }
}

fn check_directory(directory: &Path, expected: &BTreeMap<PathBuf, Vec<u8>>) -> SdkGenResult<()> {
    ensure_directory_not_symlink(directory)?;
    let actual_paths = collect_files(directory)?;
    let expected_paths = expected.keys().cloned().collect::<BTreeSet<_>>();
    if actual_paths != expected_paths {
        let missing = expected_paths.difference(&actual_paths).next();
        let extra = actual_paths.difference(&expected_paths).next();
        let reason = match (missing, extra) {
            (Some(path), _) => format!("missing generated file {}", path.display()),
            (_, Some(path)) => format!("unexpected stale file {}", path.display()),
            (None, None) => "generated file set differs".to_owned(),
        };
        return Err(SdkGenError::OutputMismatch {
            path: directory.to_path_buf(),
            reason,
        });
    }
    for (relative, expected_bytes) in expected {
        let path = directory.join(relative);
        let actual = fs::read(&path).map_err(|error| SdkGenError::io(&path, error))?;
        if actual != *expected_bytes {
            return Err(SdkGenError::OutputMismatch {
                path,
                reason: "file bytes differ from deterministic regeneration".to_owned(),
            });
        }
    }
    Ok(())
}

fn collect_files(directory: &Path) -> SdkGenResult<BTreeSet<PathBuf>> {
    let mut files = BTreeSet::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(current) = pending.pop() {
        let entries = fs::read_dir(&current).map_err(|error| SdkGenError::io(&current, error))?;
        for entry in entries {
            let entry = entry.map_err(|error| SdkGenError::io(&current, error))?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|error| SdkGenError::io(&path, error))?;
            if metadata.file_type().is_symlink() {
                return Err(SdkGenError::OutputMismatch {
                    path,
                    reason: "generated output must not contain symlinks".to_owned(),
                });
            }
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() {
                let relative = path
                    .strip_prefix(directory)
                    .map_err(|_| SdkGenError::InvalidGeneratedPath(path.clone()))?
                    .to_path_buf();
                files.insert(relative);
            } else {
                return Err(SdkGenError::OutputMismatch {
                    path,
                    reason: "generated output contains a non-regular entry".to_owned(),
                });
            }
        }
    }
    Ok(files)
}

fn create_directories_without_symlinks(workspace: &Path, directory: &Path) -> SdkGenResult<()> {
    let relative = directory
        .strip_prefix(workspace)
        .map_err(|_| SdkGenError::UnsafeOutputPath {
            path: directory.to_path_buf(),
            reason: "directory escaped the workspace".to_owned(),
        })?;
    let mut current = workspace.to_path_buf();
    for component in relative.components() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(SdkGenError::UnsafeOutputPath {
                    path: current,
                    reason: "output parent components must be real directories".to_owned(),
                });
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current).map_err(|source| SdkGenError::io(&current, source))?;
            }
            Err(error) => return Err(SdkGenError::io(&current, error)),
        }
    }
    Ok(())
}

fn ensure_directory_not_symlink(path: &Path) -> SdkGenResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| SdkGenError::io(path, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(SdkGenError::OutputMismatch {
            path: path.to_path_buf(),
            reason: "expected a real generated directory".to_owned(),
        });
    }
    Ok(())
}

fn remove_entry(path: &Path) -> SdkGenResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            fs::remove_dir_all(path).map_err(|error| SdkGenError::io(path, error))
        }
        Ok(_) => fs::remove_file(path).map_err(|error| SdkGenError::io(path, error)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(SdkGenError::io(path, error)),
    }
}

fn sync_directory(path: &Path) -> SdkGenResult<()> {
    let directory = fs::File::open(path).map_err(|error| SdkGenError::io(path, error))?;
    directory.sync_all().map_err(|error| SdkGenError::io(path, error))
}
