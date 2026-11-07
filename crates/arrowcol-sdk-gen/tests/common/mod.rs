#![allow(dead_code)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use arrowcol_catalog::{
    CatalogField, CatalogSchemaVersion, DdlCommand, DdlParser, SchemaVersion, deterministic_table_id,
};
use arrowcol_sdk_gen::{ArtifactBundle, SafeOutputPath};

pub fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| panic!("crate must be two levels below workspace root"))
}

pub fn fixture_schema() -> CatalogSchemaVersion {
    schema_from_ddl(include_str!("../../fixtures/telemetry-events.sql"))
}

pub fn schema_from_ddl(ddl: &str) -> CatalogSchemaVersion {
    let DdlCommand::CreateTable(create) = DdlParser::default()
        .parse(ddl)
        .unwrap_or_else(|error| panic!("fixture DDL must parse: {error}"))
    else {
        panic!("fixture must be CREATE TABLE");
    };
    CatalogSchemaVersion::new(
        deterministic_table_id(create.identity()),
        create.identity().clone(),
        SchemaVersion::FIRST,
        create.fields().to_vec(),
        create.canonical_ddl(),
    )
    .and_then(|schema| schema.with_metadata(create.metadata().clone()))
    .unwrap_or_else(|error| panic!("fixture schema must be valid: {error}"))
}

pub fn bundle() -> ArtifactBundle {
    ArtifactBundle::from_schema(fixture_schema())
        .unwrap_or_else(|error| panic!("fixture artifacts must generate: {error}"))
}

pub fn safe(path: &str) -> SafeOutputPath {
    SafeOutputPath::new(workspace(), path).unwrap_or_else(|error| panic!("test output path must be safe: {error}"))
}

pub fn reset(path: &SafeOutputPath) {
    match fs::remove_dir_all(path.absolute()) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("failed to reset {}: {error}", path.absolute().display()),
    }
}

pub fn clone_field(field: &CatalogField, name: &str, position: u32, field_number: u32, nullable: bool) -> CatalogField {
    CatalogField::new(
        field.id(),
        name,
        position,
        field_number,
        nullable,
        field.partition(),
        field.mapping().clone(),
    )
    .unwrap_or_else(|error| panic!("mutated test field must construct: {error}"))
}

pub fn assert_success(output: Output, command: &str) {
    assert!(
        output.status.success(),
        "{command} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub fn command(program: &str) -> Command {
    let mut command = Command::new(program);
    command.current_dir(workspace());
    command
}
