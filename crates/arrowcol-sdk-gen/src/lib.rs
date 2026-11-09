//! Deterministic Rust and TypeScript SDK generation from immutable catalog artifacts.

#![forbid(unsafe_code)]

mod artifact;
mod catalog;
mod compatibility;
mod error;
mod generate;
mod package;
mod path;
mod render;

pub use artifact::{ArtifactBundle, ArtifactPaths};
pub use catalog::{bundle_from_snapshot, catalog_artifacts, hydrate_catalog};
pub use compatibility::{CompatibilityReport, verify_compatibility};
pub use error::{SdkGenError, SdkGenResult};
pub use generate::{GenerationResult, check_generated_sdk, generate_sdk, write_artifact_bundle};
pub use package::{PackageResult, package_generated_sdk};
pub use path::SafeOutputPath;
