mod common;

use arrowcol_sdk_gen::{ArtifactBundle, write_artifact_bundle};
use prost_types::field_descriptor_proto::{Label, Type};

#[test]
fn schema_contract_is_optional_and_retains_logical_arrow_validation() {
    let bundle = common::bundle();
    let source = std::str::from_utf8(bundle.protobuf_source()).unwrap_or_else(|error| panic!("UTF-8 source: {error}"));
    assert!(source.starts_with("syntax = \"proto2\";"));
    assert!(source.contains("package arrowcol.telemetry.prod;"));
    assert!(source.contains("19000-19999 are never allocated"));
    assert!(!source.contains("required "));
    let root = bundle.descriptors().file[0]
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
}
