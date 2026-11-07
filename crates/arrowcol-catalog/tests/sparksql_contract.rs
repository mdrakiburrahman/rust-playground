use arrowcol_catalog::{
    CatalogField, CatalogSchemaVersion, CatalogType, DdlCommand, DdlParser, ListKind, ProtobufType, SchemaVersion,
    TimeUnit, TypeMapping, deterministic_table_id, generate_descriptor_artifacts, next_protobuf_field_number,
    protobuf_field_number,
};
use prost::Message;
use prost_types::{FileDescriptorSet, field_descriptor_proto::Type};
use uuid::Uuid;

#[test]
fn every_spark_decimal_mapping_agrees_with_manifest_and_real_proto_contract() -> anyhow::Result<()> {
    let columns = (1_u8..=38)
        .flat_map(|precision| {
            (0..=precision).map(move |scale| format!("f_{precision}_{scale} DECIMAL({precision},{scale})"))
        })
        .collect::<Vec<_>>();
    let sql = format!("CREATE TABLE c.d.decimals ({}) USING DELTA", columns.join(", "));
    let DdlCommand::CreateTable(create) = DdlParser::default().parse(&sql)? else {
        anyhow::bail!("expected create");
    };
    let schema = CatalogSchemaVersion::new(
        deterministic_table_id(create.identity()),
        create.identity().clone(),
        SchemaVersion::FIRST,
        create.fields().to_vec(),
        create.canonical_ddl(),
    )?;
    let artifacts = generate_descriptor_artifacts(&schema)?;
    let descriptor = FileDescriptorSet::decode(artifacts.protobuf_descriptor())?;
    let manifest: serde_json::Value = serde_json::from_slice(artifacts.manifest())?;
    let source = artifacts.protobuf_source()?;
    let fields = &descriptor.file[0].message_type[0].field;
    assert_eq!(fields.len(), columns.len());
    for (index, field) in schema.fields().iter().enumerate() {
        assert_eq!(field.mapping().protobuf(), ProtobufType::String, "{}", field.name());
        assert_eq!(fields[index].r#type(), Type::String, "{}", field.name());
        assert_eq!(
            manifest["schema"]["fields"][index]["mapping"]["protobuf"],
            "string",
            "{}",
            field.name()
        );
        assert!(source.contains(&format!("optional string {} = ", field.name())));
    }
    Ok(())
}

#[test]
fn registered_schema_round_trips_with_metadata_and_comments() -> anyhow::Result<()> {
    let parser = DdlParser::default();
    let DdlCommand::CreateTable(create) = parser.parse(
        "CREATE TABLE c.d.t (id BIGINT NOT NULL COMMENT 'identity', payload STRUCT<label: STRING, values: ARRAY<MAP<STRING,DECIMAL(20,4)>>>, day DATE, ts TIMESTAMP_NTZ) USING DELTA PARTITIONED BY (day,id) LOCATION '/tmp/table' COMMENT 'table' TBLPROPERTIES ('a'='1')"
    )? else { anyhow::bail!("expected create"); };
    let original = CatalogSchemaVersion::new(
        deterministic_table_id(create.identity()),
        create.identity().clone(),
        SchemaVersion::FIRST,
        create.fields().to_vec(),
        create.canonical_ddl(),
    )?
    .with_metadata(create.metadata().clone())?;
    let DdlCommand::CreateTable(reparsed) = parser.parse(&original.to_spark_sql()?)? else {
        anyhow::bail!("expected create");
    };
    assert_eq!(create.fields(), reparsed.fields());
    assert_eq!(create.metadata(), reparsed.metadata());
    assert_eq!(original.fields()[0].comment(), Some("identity"));
    assert_eq!(original.fields()[0].partition().map(|p| p.ordinal()), Some(1));
    assert_eq!(original.fields()[2].partition().map(|p| p.ordinal()), Some(0));
    Ok(())
}

#[test]
fn arrow_only_representations_are_not_silently_coerced() -> anyhow::Result<()> {
    let element = CatalogField::new(
        Uuid::nil(),
        "item",
        0,
        1,
        true,
        None,
        TypeMapping::new(CatalogType::Int32),
    )?;
    for ty in [
        CatalogType::UInt64,
        CatalogType::LargeUtf8,
        CatalogType::BinaryView,
        CatalogType::FixedSizeBinary { bytes: 16 },
        CatalogType::Timestamp {
            unit: TimeUnit::Nanosecond,
            timezone: Some("UTC".to_owned()),
        },
        CatalogType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: Some("Europe/Paris".to_owned()),
        },
        CatalogType::List {
            list_kind: ListKind::LargeList,
            element: Box::new(element),
        },
    ] {
        assert!(ty.spark_name().is_err());
        assert!(TypeMapping::new(ty).spark().is_err());
    }
    assert_eq!(
        CatalogType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None
        }
        .spark_name()?,
        "TIMESTAMP_NTZ"
    );
    Ok(())
}

#[test]
fn protobuf_allocation_skips_reserved_numbers_and_rejects_exhaustion() -> anyhow::Result<()> {
    assert_eq!(protobuf_field_number(18_998)?, 18_999);
    assert_eq!(protobuf_field_number(18_999)?, 20_000);
    assert_eq!(protobuf_field_number(19_000)?, 20_001);
    assert_eq!(next_protobuf_field_number(18_999)?, 20_000);
    assert_eq!(next_protobuf_field_number(20_000)?, 20_001);
    assert!(next_protobuf_field_number(19_000).is_err());
    assert!(next_protobuf_field_number(536_870_911).is_err());
    assert!(protobuf_field_number(u32::MAX).is_err());
    for number in [0, 19_000, 19_999, 536_870_912] {
        assert!(
            CatalogField::new(
                Uuid::nil(),
                "a",
                0,
                number,
                true,
                None,
                TypeMapping::new(CatalogType::Int32)
            )
            .is_err()
        );
    }
    Ok(())
}
