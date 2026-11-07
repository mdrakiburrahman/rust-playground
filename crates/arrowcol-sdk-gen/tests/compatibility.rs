mod common;

use arrowcol_catalog::{
    CatalogField, CatalogSchemaVersion, CatalogType, DdlCommand, DdlParser, FieldPath, PartitionMetadata, TypeMapping,
    deterministic_field_id,
};
use arrowcol_sdk_gen::{ArtifactBundle, verify_compatibility};

use common::{clone_field, fixture_schema};

#[test]
fn nullable_tail_append_is_wire_json_and_layout_compatible() {
    let previous = fixture_schema();
    let DdlCommand::AddColumns(add) = DdlParser::default()
        .parse("ALTER TABLE telemetry.prod.sdk_events ADD COLUMNS (status STRING)")
        .unwrap_or_else(|error| panic!("append DDL must parse: {error}"))
    else {
        panic!("expected add columns");
    };
    let blueprint = &add.fields()[0];
    let appended = CatalogField::new(
        blueprint.id(),
        blueprint.name(),
        previous
            .next_position()
            .unwrap_or_else(|error| panic!("position: {error}")),
        previous
            .next_field_number()
            .unwrap_or_else(|error| panic!("field number: {error}")),
        blueprint.is_nullable(),
        None,
        blueprint.mapping().clone(),
    )
    .unwrap_or_else(|error| panic!("appended field: {error}"));
    let mut fields = previous.fields().to_vec();
    fields.push(appended);
    let current = CatalogSchemaVersion::new(
        previous.table_id(),
        previous.identity().clone(),
        previous
            .version()
            .next()
            .unwrap_or_else(|error| panic!("next version: {error}")),
        fields,
        add.canonical_ddl(),
    )
    .and_then(|schema| schema.with_metadata(previous.metadata().clone()))
    .unwrap_or_else(|error| panic!("candidate schema: {error}"));
    let report = verify_compatibility(&bundle(previous), &bundle(current))
        .unwrap_or_else(|error| panic!("append must be compatible: {error}"));
    assert_eq!(report.appended_fields, 1);
    assert!(report.protobuf_messages_checked >= 1);
}

#[test]
fn breaking_mutations_are_rejected() {
    let previous = fixture_schema();
    let first = &previous.fields()[0];
    let next = previous
        .version()
        .next()
        .unwrap_or_else(|error| panic!("next version: {error}"));

    let mut rename = previous.fields().to_vec();
    rename[0] = clone_field(
        first,
        "renamed_event_id",
        first.position(),
        first.field_number(),
        first.is_nullable(),
    );

    let mut renumber = previous.fields().to_vec();
    renumber[0] = clone_field(
        first,
        first.name(),
        first.position(),
        first.field_number() + 100,
        first.is_nullable(),
    );

    let mut type_change = previous.fields().to_vec();
    type_change[0] = CatalogField::new(
        first.id(),
        first.name(),
        first.position(),
        first.field_number(),
        first.is_nullable(),
        first.partition(),
        TypeMapping::new(CatalogType::Utf8),
    )
    .unwrap_or_else(|error| panic!("type mutation: {error}"));

    let mut partition_change = previous.fields().to_vec();
    partition_change[0] = CatalogField::new(
        first.id(),
        first.name(),
        first.position(),
        first.field_number(),
        first.is_nullable(),
        Some(PartitionMetadata::new(99)),
        first.mapping().clone(),
    )
    .unwrap_or_else(|error| panic!("partition mutation: {error}"));

    let mut reorder = previous.fields().to_vec();
    reorder.swap(0, 1);

    let appended = CatalogField::new(
        deterministic_field_id(previous.table_id(), &FieldPath::root("required_append")),
        "required_append",
        previous
            .next_position()
            .unwrap_or_else(|error| panic!("position: {error}")),
        previous
            .next_field_number()
            .unwrap_or_else(|error| panic!("field number: {error}")),
        false,
        None,
        TypeMapping::new(CatalogType::Utf8),
    )
    .unwrap_or_else(|error| panic!("required append: {error}"));
    let mut required_append = previous.fields().to_vec();
    required_append.push(appended);

    for (label, fields) in [
        ("rename", rename),
        ("renumber", renumber),
        ("type", type_change),
        ("partition", partition_change),
        ("reorder", reorder),
        ("required append", required_append),
    ] {
        let candidate = CatalogSchemaVersion::new(
            previous.table_id(),
            previous.identity().clone(),
            next,
            fields,
            format!("mutation {label}"),
        )
        .unwrap_or_else(|error| panic!("{label} schema must construct: {error}"));
        let Err(error) = verify_compatibility(&bundle(previous.clone()), &bundle(candidate)) else {
            panic!("breaking mutation must fail");
        };
        assert!(
            error.to_string().contains("incompatible schema evolution"),
            "{label}: {error}"
        );
    }
}

fn bundle(schema: CatalogSchemaVersion) -> ArtifactBundle {
    ArtifactBundle::from_schema(schema).unwrap_or_else(|error| panic!("test artifacts must generate: {error}"))
}
