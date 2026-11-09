use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use arrowcol_catalog::{
    CatalogSchemaVersion, DdlCommand, DdlParser, SchemaVersion, deterministic_table_id, generate_descriptor_artifacts,
};
use prost::Message;
use prost_types::FileDescriptorSet;

#[test]
fn descriptor_artifacts_are_deterministic_and_round_trip() -> anyhow::Result<()> {
    let DdlCommand::CreateTable(create) = DdlParser::default().parse(
        "CREATE TABLE telemetry.prod.descriptors ( \
            id BIGINT NOT NULL, \
            spans ARRAY<STRUCT<trace_id: BINARY, duration: BIGINT>>, \
            labels MAP<STRING, STRING> \
         ) USING DELTA",
    )?
    else {
        anyhow::bail!("expected create");
    };
    let schema = CatalogSchemaVersion::new(
        deterministic_table_id(create.identity()),
        create.identity().clone(),
        SchemaVersion::FIRST,
        create.fields().to_vec(),
        create.canonical_ddl(),
    )?;
    let first = generate_descriptor_artifacts(&schema)?;
    let second = generate_descriptor_artifacts(&schema)?;
    assert_eq!(first, second);
    assert_eq!(
        try_schema_from_flatbuffer_bytes(first.arrow_schema())?,
        schema.arrow_schema()?
    );
    let descriptors = FileDescriptorSet::decode(first.protobuf_descriptor())?;
    assert_eq!(descriptors.file.len(), 1);
    assert!(descriptors.file[0].message_type.len() >= 3);
    let manifest: serde_json::Value = serde_json::from_slice(first.manifest())?;
    assert_eq!(manifest["artifact_manifest_version"], 1);
    assert_eq!(manifest["schema"]["version"], 1);
    Ok(())
}
