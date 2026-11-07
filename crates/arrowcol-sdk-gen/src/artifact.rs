use std::{
    fs,
    path::{Path, PathBuf},
};

use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use arrowcol_catalog::{CatalogSchemaVersion, generate_descriptor_artifacts};
use prost::Message;
use prost_types::FileDescriptorSet;
use serde::Deserialize;

use crate::{SdkGenError, SdkGenResult};

/// Conventional immutable catalog artifact file paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactPaths {
    /// Canonical JSON catalog manifest.
    pub manifest: PathBuf,
    /// Arrow IPC schema message.
    pub arrow_schema: PathBuf,
    /// Encoded protobuf file descriptor set.
    pub protobuf_descriptor: PathBuf,
    /// Proto2 schema contract source.
    pub protobuf_source: PathBuf,
}

impl ArtifactPaths {
    /// Resolves conventional artifact names below a directory.
    #[must_use]
    pub fn in_directory(directory: impl AsRef<Path>) -> Self {
        let directory = directory.as_ref();
        Self {
            manifest: directory.join("manifest.json"),
            arrow_schema: directory.join("schema.arrow"),
            protobuf_descriptor: directory.join("descriptor.pb"),
            protobuf_source: directory.join("schema.proto"),
        }
    }
}

/// Fully validated immutable catalog artifacts consumed by the generator.
#[derive(Clone, Debug)]
pub struct ArtifactBundle {
    schema: CatalogSchemaVersion,
    manifest: Vec<u8>,
    arrow_schema: Vec<u8>,
    protobuf_descriptor: Vec<u8>,
    protobuf_source: Vec<u8>,
    descriptors: FileDescriptorSet,
}

impl ArtifactBundle {
    /// Loads and cross-validates a conventional artifact directory.
    ///
    /// # Errors
    ///
    /// Returns an error when any file is missing, symlinked, malformed, or
    /// differs from deterministic regeneration of its normalized schema.
    pub fn load_directory(directory: impl AsRef<Path>) -> SdkGenResult<Self> {
        Self::load(&ArtifactPaths::in_directory(directory))
    }

    /// Loads and cross-validates explicit artifact paths.
    ///
    /// # Errors
    ///
    /// Returns an error when any file is missing, symlinked, malformed, or
    /// differs from deterministic regeneration of its normalized schema.
    pub fn load(paths: &ArtifactPaths) -> SdkGenResult<Self> {
        let manifest = read_regular_file(&paths.manifest)?;
        let arrow_schema = read_regular_file(&paths.arrow_schema)?;
        let protobuf_descriptor = read_regular_file(&paths.protobuf_descriptor)?;
        let protobuf_source = read_regular_file(&paths.protobuf_source)?;
        let decoded: ArtifactManifest = serde_json::from_slice(&manifest)?;
        if decoded.artifact_manifest_version != 1 {
            return Err(SdkGenError::Artifact(format!(
                "unsupported artifact manifest version {}",
                decoded.artifact_manifest_version
            )));
        }

        let rebuilt_schema = CatalogSchemaVersion::new(
            decoded.schema.table_id(),
            decoded.schema.identity().clone(),
            decoded.schema.version(),
            decoded.schema.fields().to_vec(),
            decoded.schema.canonical_ddl(),
        )
        .map_err(|error| SdkGenError::Artifact(error.to_string()))?;
        if rebuilt_schema != decoded.schema {
            return Err(SdkGenError::Artifact(
                "manifest schema does not match its recomputed fingerprint or invariants".to_owned(),
            ));
        }

        let expected =
            generate_descriptor_artifacts(&decoded.schema).map_err(|error| SdkGenError::Artifact(error.to_string()))?;
        if expected.manifest() != manifest {
            return Err(SdkGenError::Artifact(
                "manifest bytes are not the canonical catalog artifact".to_owned(),
            ));
        }
        if expected.arrow_schema() != arrow_schema {
            return Err(SdkGenError::Artifact(
                "Arrow schema bytes are not the canonical catalog artifact".to_owned(),
            ));
        }
        if expected.protobuf_descriptor() != protobuf_descriptor {
            return Err(SdkGenError::Artifact(
                "protobuf descriptor bytes are not the canonical catalog artifact".to_owned(),
            ));
        }
        if expected
            .protobuf_source()
            .map_err(|error| SdkGenError::Artifact(error.to_string()))?
            .as_bytes()
            != protobuf_source
        {
            return Err(SdkGenError::Artifact(
                "protobuf source bytes are not the canonical catalog artifact".to_owned(),
            ));
        }

        let decoded_arrow = try_schema_from_flatbuffer_bytes(&arrow_schema)
            .map_err(|error| SdkGenError::Artifact(format!("Arrow schema artifact did not decode: {error}")))?;
        let expected_arrow = decoded
            .schema
            .arrow_schema()
            .map_err(|error| SdkGenError::Artifact(error.to_string()))?;
        if decoded_arrow != expected_arrow {
            return Err(SdkGenError::Artifact(
                "Arrow schema artifact differs from the normalized catalog schema".to_owned(),
            ));
        }
        let descriptors = FileDescriptorSet::decode(protobuf_descriptor.as_slice())?;

        Ok(Self {
            schema: decoded.schema,
            manifest,
            arrow_schema,
            protobuf_descriptor,
            protobuf_source,
            descriptors,
        })
    }

    /// Builds canonical artifacts from a validated catalog schema.
    ///
    /// # Errors
    ///
    /// Returns an error when catalog descriptor generation fails.
    pub fn from_schema(schema: CatalogSchemaVersion) -> SdkGenResult<Self> {
        let generated =
            generate_descriptor_artifacts(&schema).map_err(|error| SdkGenError::Artifact(error.to_string()))?;
        let descriptors = FileDescriptorSet::decode(generated.protobuf_descriptor())?;
        Ok(Self {
            schema,
            manifest: generated.manifest().to_vec(),
            arrow_schema: generated.arrow_schema().to_vec(),
            protobuf_descriptor: generated.protobuf_descriptor().to_vec(),
            protobuf_source: generated
                .protobuf_source()
                .map_err(|error| SdkGenError::Artifact(error.to_string()))?
                .into_bytes(),
            descriptors,
        })
    }

    /// Returns the immutable normalized schema.
    #[must_use]
    pub const fn schema(&self) -> &CatalogSchemaVersion {
        &self.schema
    }

    /// Returns canonical manifest bytes.
    #[must_use]
    pub fn manifest(&self) -> &[u8] {
        &self.manifest
    }

    /// Returns canonical Arrow IPC schema bytes.
    #[must_use]
    pub fn arrow_schema(&self) -> &[u8] {
        &self.arrow_schema
    }

    /// Returns canonical protobuf descriptor bytes.
    #[must_use]
    pub fn protobuf_descriptor(&self) -> &[u8] {
        &self.protobuf_descriptor
    }

    /// Returns canonical proto2 schema contract source.
    #[must_use]
    pub fn protobuf_source(&self) -> &[u8] {
        &self.protobuf_source
    }

    /// Returns the decoded protobuf descriptor set.
    #[must_use]
    pub const fn descriptors(&self) -> &FileDescriptorSet {
        &self.descriptors
    }
}

#[derive(Deserialize)]
struct ArtifactManifest {
    artifact_manifest_version: u32,
    schema: CatalogSchemaVersion,
}

fn read_regular_file(path: &Path) -> SdkGenResult<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).map_err(|error| SdkGenError::io(path, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(SdkGenError::Artifact(format!(
            "artifact must be a regular non-symlink file: {}",
            path.display()
        )));
    }
    fs::read(path).map_err(|error| SdkGenError::io(path, error))
}
