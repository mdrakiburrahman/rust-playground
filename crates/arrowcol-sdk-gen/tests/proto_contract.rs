mod common;

use std::fs;

use arrowcol_catalog::{materialize_protobuf_properties, protobuf_option_definitions, protobuf_table_metadata};
use arrowcol_sdk_gen::{ArtifactBundle, generate_sdk, write_artifact_bundle};
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use sha2::{Digest, Sha256};

#[test]
fn schema_contract_is_optional_and_retains_logical_arrow_validation() {
    let bundle = common::bundle();
    let source = std::str::from_utf8(bundle.protobuf_source()).unwrap_or_else(|error| panic!("UTF-8 source: {error}"));
    assert!(source.starts_with("syntax = \"proto2\";"));
    assert!(source.contains("package arrowcol.telemetry.prod;"));
    assert!(source.contains("19000-19999 are never allocated"));
    assert!(!source.contains("required "));
    let root = bundle
        .descriptors()
        .file
        .iter()
        .find(|file| file.name() == "schema.proto")
        .unwrap_or_else(|| panic!("schema contract"))
        .message_type
        .iter()
        .find(|message| message.name.as_deref() == Some("SdkEvents"))
        .unwrap_or_else(|| panic!("root contract"));
    for field in &root.field {
        assert_ne!(field.label, Some(Label::Required as i32));
    }
    let timestamp = root
        .field
        .iter()
        .find(|field| field.name.as_deref() == Some("observed_at"))
        .unwrap_or_else(|| panic!("timestamp contract"));
    assert_eq!(timestamp.r#type, Some(Type::Int64 as i32));
    assert_eq!(timestamp.label, Some(Label::Optional as i32));
    let decimal = root
        .field
        .iter()
        .find(|field| field.name.as_deref() == Some("amount"))
        .unwrap_or_else(|| panic!("decimal contract"));
    assert_eq!(decimal.r#type, Some(Type::String as i32));
    assert!(!bundle.schema().fields()[0].is_nullable());
}

#[test]
fn persisted_artifacts_regenerate_source_and_field_identity_exactly() {
    let original = common::bundle();
    let path = common::safe("target/arrowcol-sdk-gen-tests/proto-persisted");
    common::reset(&path);
    write_artifact_bundle(&original, &path).unwrap_or_else(|error| panic!("persist: {error}"));
    let restored = ArtifactBundle::load_directory(path.absolute()).unwrap_or_else(|error| panic!("restore: {error}"));
    assert_eq!(restored.protobuf_source(), original.protobuf_source());
    assert_eq!(restored.descriptors(), original.descriptors());
    assert_eq!(restored.schema().fields(), original.schema().fields());
    assert_eq!(restored.protobuf_imports(), original.protobuf_imports());
}

fn metadata_bundle() -> ArtifactBundle {
    let schema = common::fixture_schema();
    let mut metadata = schema.metadata().clone();
    metadata.location = Some("abfss://lake/路径\\\"".to_owned());
    metadata.comment = Some(String::new());
    metadata
        .properties
        .insert("arrowcol.maintenance.enabled".to_owned(), "false".to_owned());
    metadata
        .properties
        .insert("delta.appendOnly".to_owned(), "true".to_owned());
    metadata
        .properties
        .insert("team.owner".to_owned(), "遥测\n\"\\\r\t\0\u{0001}7😀".to_owned());
    metadata.properties.insert("arbitrary.empty".to_owned(), String::new());
    metadata
        .properties
        .insert("arbitrary.\0\"\\😀".to_owned(), "inert".to_owned());
    metadata.properties.insert(
        "protobuf.arrowcol.metadata.v1.ingest_tier".to_owned(),
        "INGEST_TIER_PRIORITY".to_owned(),
    );
    ArtifactBundle::from_schema(
        schema
            .with_metadata(metadata)
            .unwrap_or_else(|error| panic!("metadata: {error}")),
    )
    .unwrap_or_else(|error| panic!("metadata bundle: {error}"))
}

#[test]
fn compiled_definitions_drive_native_defaults_and_validation() {
    let definitions = protobuf_option_definitions().unwrap_or_else(|error| panic!("definitions: {error}"));
    let tier = definitions
        .options
        .iter()
        .find(|option| option.option_name == "arrowcol.metadata.v1.ingest_tier")
        .unwrap_or_else(|| panic!("compiled ingest tier option"));
    assert_eq!(tier.key, "protobuf.arrowcol.metadata.v1.ingest_tier");
    assert!(tier.optional);
    assert_eq!(tier.kind, "enum");
    assert_eq!(
        tier.enum_values,
        ["INGEST_TIER_STANDARD", "INGEST_TIER_BULK", "INGEST_TIER_PRIORITY"]
    );
    assert_eq!(tier.declared_default.as_deref(), Some("INGEST_TIER_STANDARD"));
    assert!(definitions.definitions_sha256.starts_with("sha256:"));
    assert_eq!(definitions.definitions_sha256.len(), 71);
    let original = common::fixture_schema();
    let mut metadata = original.metadata().clone();
    assert!(!metadata.properties.contains_key(&tier.key));
    metadata.properties = materialize_protobuf_properties(&metadata.properties)
        .unwrap_or_else(|error| panic!("native defaults: {error}"));
    assert_eq!(metadata.properties.get(&tier.key), tier.declared_default.as_ref());
    let defaults = ArtifactBundle::from_schema(
        original
            .clone()
            .with_metadata(metadata.clone())
            .unwrap_or_else(|error| panic!("metadata: {error}")),
    )
    .unwrap_or_else(|error| panic!("default bundle: {error}"));
    let source = std::str::from_utf8(defaults.protobuf_source()).unwrap_or_else(|error| panic!("source: {error}"));
    assert!(source.contains("option (arrowcol.metadata.v1.ingest_tier) = INGEST_TIER_STANDARD;"));
    assert_eq!(defaults.schema().fingerprint(), original.fingerprint());
    assert_eq!(
        protobuf_table_metadata(defaults.protobuf_descriptor()).unwrap_or_else(|error| panic!("metadata: {error}")),
        metadata
    );
    let absent = common::bundle();
    let mut dependencies = absent.descriptors().clone();
    dependencies.file.retain(|file| file.name() != "schema.proto");
    assert_eq!(
        definitions.definitions_sha256,
        format!("sha256:{}", hex::encode(Sha256::digest(dependencies.encode_to_vec())))
    );
    let native_default = absent
        .descriptors()
        .file
        .iter()
        .flat_map(|file| &file.extension)
        .find(|field| field.name() == "ingest_tier")
        .and_then(|field| field.default_value.as_deref());
    assert_eq!(native_default, tier.declared_default.as_deref());
    let mut invalid = metadata.properties.clone();
    invalid.insert(tier.key.clone(), "INGEST_TIER_NOT_REAL".to_owned());
    assert!(materialize_protobuf_properties(&invalid).is_err());
    invalid.insert(tier.key.clone(), "2".to_owned());
    assert!(materialize_protobuf_properties(&invalid).is_err());
    invalid.remove(&tier.key);
    invalid.insert("protobuf.arrowcol.metadata.v1.typo".to_owned(), "x".to_owned());
    assert!(materialize_protobuf_properties(&invalid).is_err());
}

#[test]
fn metadata_options_preserve_values_without_changing_physical_identity() {
    let original = common::bundle();
    let changed = metadata_bundle();
    assert_eq!(original.schema().fingerprint(), changed.schema().fingerprint());
    assert_eq!(original.arrow_schema(), changed.arrow_schema());
    assert_ne!(original.protobuf_descriptor(), changed.protobuf_descriptor());
    assert_ne!(original.protobuf_source(), changed.protobuf_source());
    assert_eq!(
        protobuf_table_metadata(changed.protobuf_descriptor()).unwrap_or_else(|error| panic!("decode: {error}")),
        *changed.schema().metadata()
    );
    assert_eq!(
        protobuf_table_metadata(original.protobuf_descriptor()).unwrap_or_else(|error| panic!("decode: {error}")),
        *original.schema().metadata()
    );
}

#[test]
fn portable_source_compiles_to_the_exact_descriptor_with_protoc_and_buf() {
    let schema = common::fixture_schema();
    let mut metadata = schema.metadata().clone();
    metadata.properties =
        materialize_protobuf_properties(&metadata.properties).unwrap_or_else(|error| panic!("defaults: {error}"));
    let default_bundle = ArtifactBundle::from_schema(
        schema
            .with_metadata(metadata)
            .unwrap_or_else(|error| panic!("metadata: {error}")),
    )
    .unwrap_or_else(|error| panic!("bundle: {error}"));
    for (name, bundle) in [
        ("explicit", metadata_bundle()),
        ("defaults", default_bundle),
        ("absent", common::bundle()),
    ] {
        assert_compiler_roundtrip(name, &bundle);
    }
}

fn assert_compiler_roundtrip(name: &str, bundle: &ArtifactBundle) {
    let directory = common::safe(&format!("target/arrowcol-sdk-gen-tests/portable-proto-{name}"));
    common::reset(&directory);
    write_artifact_bundle(bundle, &directory).unwrap_or_else(|error| panic!("write: {error}"));
    // Neither compiler nor a descriptor consumer needs a JSON manifest.
    fs::remove_file(directory.absolute().join("manifest.json")).unwrap_or_else(|error| panic!("remove: {error}"));
    let protoc_output = directory.absolute().join("protoc.pb");
    let output = common::command("protoc")
        .arg("--proto_path")
        .arg(directory.absolute())
        .arg("--include_imports")
        .arg("--descriptor_set_out")
        .arg(&protoc_output)
        .arg(directory.absolute().join("schema.proto"))
        .output()
        .unwrap_or_else(|error| panic!("protoc: {error}"));
    common::assert_success(output, "standalone protoc compilation");
    let buf_output = directory.absolute().join("buf.pb");
    let output = common::command("npx")
        .current_dir(directory.absolute())
        .args(["--no-install", "buf", "build", "."])
        .args(["--config", r#"{"version":"v2","modules":[{"path":"."}]}"#])
        .args(["--as-file-descriptor-set", "--exclude-source-info", "--output"])
        .arg(&buf_output)
        .output()
        .unwrap_or_else(|error| panic!("buf: {error}"));
    common::assert_success(output, "standalone pinned Buf compilation");
    for path in [protoc_output, buf_output] {
        let compiled = fs::read(&path).unwrap_or_else(|error| panic!("compiled descriptor: {error}"));
        assert_eq!(
            protobuf_table_metadata(&compiled).unwrap_or_else(|error| panic!("compiled metadata: {error}")),
            *bundle.schema().metadata()
        );
        assert!(
            compiled == bundle.protobuf_descriptor(),
            "{} must round-trip byte-for-byte",
            path.display()
        );
    }
}

#[test]
fn generated_sdk_carries_all_protobuf_sources_and_raw_options() {
    let bundle = metadata_bundle();
    let output = common::safe("target/arrowcol-sdk-gen-tests/portable-sdk");
    common::reset(&output);
    generate_sdk(&bundle, &output).unwrap_or_else(|error| panic!("generate: {error}"));
    let restored = ArtifactBundle::load_directory(output.absolute().join("descriptors"))
        .unwrap_or_else(|error| panic!("generated descriptors: {error}"));
    assert_eq!(restored.protobuf_descriptor(), bundle.protobuf_descriptor());
    assert_eq!(restored.protobuf_imports(), bundle.protobuf_imports());
    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(output.absolute().join("sdk-manifest.json")).unwrap_or_else(|error| panic!("manifest: {error}")),
    )
    .unwrap_or_else(|error| panic!("manifest JSON: {error}"));
    let text = manifest.to_string();
    assert!(text.contains("descriptors/options.proto"));
    assert!(text.contains("descriptors/google/protobuf/descriptor.proto"));
}

#[test]
fn imports_are_validated_and_reserved_properties_use_the_central_registry() {
    for relative in ["options.proto", "google/protobuf/descriptor.proto"] {
        let output = common::safe(&format!(
            "target/arrowcol-sdk-gen-tests/tampered-{}",
            relative.replace('/', "-")
        ));
        common::reset(&output);
        write_artifact_bundle(&metadata_bundle(), &output).unwrap_or_else(|error| panic!("write: {error}"));
        fs::write(output.absolute().join(relative), b"tampered").unwrap_or_else(|error| panic!("tamper: {error}"));
        assert!(ArtifactBundle::load_directory(output.absolute()).is_err());
        fs::remove_file(output.absolute().join(relative)).unwrap_or_else(|error| panic!("remove: {error}"));
        assert!(ArtifactBundle::load_directory(output.absolute()).is_err());
    }
    for (key, value) in [
        ("arrowcol.unknown", "true"),
        ("delta.appendOnly", "not-a-bool"),
        ("protobuf.unknown", "anything"),
        ("protobuf.arrowcol.metadata.v1.ingest_tier", "INVALID"),
    ] {
        let schema = common::fixture_schema();
        let mut metadata = schema.metadata().clone();
        metadata.properties.insert(key.to_owned(), value.to_owned());
        let schema = schema
            .with_metadata(metadata)
            .unwrap_or_else(|error| panic!("schema: {error}"));
        assert!(ArtifactBundle::from_schema(schema).is_err());
    }
}
