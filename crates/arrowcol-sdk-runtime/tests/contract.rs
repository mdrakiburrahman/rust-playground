#![allow(clippy::expect_used)]
use arrowcol_sdk_runtime::ArrowcolContract;

const MANIFEST: &[u8] = include_bytes!("fixtures/events/manifest.json");
const SCHEMA: &[u8] = include_bytes!("fixtures/events/schema.arrow");
const DESCRIPTOR: &[u8] = include_bytes!("fixtures/events/descriptor.pb");

#[test]
fn canonical_catalog_bundle_loads_and_preserves_annotations_without_enforcing_policy() {
    let contract =
        ArrowcolContract::load(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/events"))
            .expect("canonical bundle");
    assert_eq!(contract.identity(), &["demo", "public", "events"]);
    assert_eq!(contract.descriptor().full_name(), "arrowcol.demo.public.Events");
    assert_eq!(
        contract.fingerprint(),
        "sha256:75c465746aa29c29b38b9edf9d639fd3f160cb81bda38e2cf20dd1f458498e6c"
    );
    assert_eq!(
        contract.manifest()["schema"]["metadata"]["properties"]["protobuf.arrowcol.metadata.v1.ingest_tier"],
        "INGEST_TIER_STANDARD"
    );
    assert_ne!(contract.fingerprint(), contract.bundle_digest());
    contract.validate_model(&contract.descriptor()).expect("model matches");
}

#[test]
fn descriptor_annotations_must_agree_with_manifest() {
    let mut manifest: serde_json::Value = serde_json::from_slice(MANIFEST).expect("manifest");
    manifest["schema"]["metadata"]["properties"]["protobuf.arrowcol.metadata.v1.ingest_tier"] =
        serde_json::json!("INGEST_TIER_EXPEDITED");
    let error = ArrowcolContract::from_artifacts(&serde_json::to_vec(&manifest).expect("manifest"), SCHEMA, DESCRIPTOR)
        .expect_err("annotation mismatch");
    assert!(error.message.contains("annotations"));
}

#[test]
fn catalogue_numeric_fixture_preserves_all_scalar_wire_kinds_and_values() {
    use arrow_array::*;
    use prost::Message;
    use prost_reflect::{DynamicMessage, Value};
    use std::sync::Arc;

    // These checked-in artifacts come from generate_descriptor_artifacts on a
    // CatalogSchemaVersion containing every numeric CatalogType, not a local descriptor builder.
    let contract =
        ArrowcolContract::load(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/numeric"))
            .expect("catalogue numeric contract");
    macro_rules! scalar {
        ($name:literal, $value:expr, $array:ident, $native:expr) => {
            (
                $name,
                $value,
                Arc::new($array::from(vec![Some($native)])) as ArrayRef,
            )
        };
    }
    let cases = vec![
        scalar!("boolean", Value::Bool(false), BooleanArray, false),
        scalar!("signed8", Value::I32(-128), Int8Array, i8::MIN),
        scalar!("signed16", Value::I32(-32768), Int16Array, i16::MIN),
        scalar!("signed32", Value::I32(i32::MIN), Int32Array, i32::MIN),
        scalar!("signed64", Value::I64(i64::MIN), Int64Array, i64::MIN),
        scalar!("unsigned8", Value::U32(255), UInt8Array, u8::MAX),
        scalar!("unsigned16", Value::U32(65535), UInt16Array, u16::MAX),
        scalar!("unsigned32", Value::U32(u32::MAX), UInt32Array, u32::MAX),
        scalar!("unsigned64", Value::U64(u64::MAX), UInt64Array, u64::MAX),
        scalar!("float32", Value::F32(-1.25), Float32Array, -1.25),
        scalar!("float64", Value::F64(2.5), Float64Array, 2.5),
        (
            "decimal",
            Value::String("-123.45".into()),
            Arc::new(
                Decimal128Array::from(vec![Some(-12345)])
                    .with_precision_and_scale(5, 2)
                    .expect("decimal"),
            ) as ArrayRef,
        ),
        scalar!("date32", Value::I32(-1), Date32Array, -1),
        scalar!("date64", Value::I64(-86_400_000), Date64Array, -86_400_000),
        scalar!("time_seconds", Value::I32(86_399), Time32SecondArray, 86_399),
        scalar!(
            "time_millis",
            Value::I32(86_399_999),
            Time32MillisecondArray,
            86_399_999
        ),
        scalar!(
            "time_micros",
            Value::I64(86_399_999_999),
            Time64MicrosecondArray,
            86_399_999_999
        ),
        scalar!(
            "time_nanos",
            Value::I64(86_399_999_999_999),
            Time64NanosecondArray,
            86_399_999_999_999
        ),
        scalar!("timestamp_seconds", Value::I64(-1), TimestampSecondArray, -1),
        scalar!("timestamp_millis", Value::I64(-1), TimestampMillisecondArray, -1),
        (
            "timestamp_micros",
            Value::I64(-1),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(-1)]).with_timezone("UTC")) as ArrayRef,
        ),
        scalar!("timestamp_nanos", Value::I64(-1), TimestampNanosecondArray, -1),
        scalar!("duration_seconds", Value::I64(-1), DurationSecondArray, -1),
        scalar!("duration_millis", Value::I64(-1), DurationMillisecondArray, -1),
        scalar!("duration_micros", Value::I64(-1), DurationMicrosecondArray, -1),
        scalar!("duration_nanos", Value::I64(-1), DurationNanosecondArray, -1),
    ];
    let mut event = DynamicMessage::new(contract.descriptor());
    for (name, value, _) in &cases {
        event.set_field_by_name(name, value.clone());
    }
    let decoded = contract.decode(&event.encode_to_vec()).expect("wire values");
    let batch = contract.record_batch(&[decoded]).expect("numeric batch");
    for (index, (name, _, expected)) in cases.iter().enumerate() {
        assert_eq!(batch.column(index).to_data(), expected.to_data(), "{name}");
    }
    let nulls = contract
        .record_batch(&[contract.decode(&[]).expect("nullable message")])
        .expect("null batch");
    assert!(nulls.columns().iter().all(|column| column.null_count() == 1));
    for (name, value) in [
        ("signed8", Value::I32(-129)),
        ("signed8", Value::I32(128)),
        ("signed16", Value::I32(-32769)),
        ("signed16", Value::I32(32768)),
        ("unsigned8", Value::U32(256)),
        ("unsigned16", Value::U32(65536)),
    ] {
        let mut invalid = event.clone();
        invalid.set_field_by_name(name, value);
        assert!(contract.decode(&invalid.encode_to_vec()).is_err(), "{name}");
    }
    // sint32 must fit an unsigned 32-bit zigzag varint before prost can truncate it.
    assert!(contract.decode(&[0x20, 0x80, 0x80, 0x80, 0x80, 0x10]).is_err());
}

#[test]
fn catalogue_sint32_binding_rejects_plain_int32_even_though_both_use_varints() {
    use prost::Message;
    use prost_reflect::DescriptorPool;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/numeric");
    let contract = ArrowcolContract::load(&path).expect("numeric contract");
    let mut descriptors = prost_types::FileDescriptorSet::decode(
        std::fs::read(path.join("descriptor.pb"))
            .expect("descriptor")
            .as_slice(),
    )
    .expect("descriptor set");
    let table = descriptors
        .file
        .iter_mut()
        .find(|file| file.name() == "schema.proto")
        .expect("table file");
    let signed = table.message_type[0]
        .field
        .iter_mut()
        .find(|field| field.name() == "signed32")
        .expect("signed32");
    assert_eq!(signed.r#type(), prost_types::field_descriptor_proto::Type::Sint32);
    signed.set_type(prost_types::field_descriptor_proto::Type::Int32);
    let pool = DescriptorPool::decode(descriptors.encode_to_vec().as_slice()).expect("mutated model");
    assert!(
        contract
            .validate_model(
                &pool
                    .get_message_by_name("arrowcol.demo.public.NumericScalars")
                    .expect("model")
            )
            .is_err()
    );
}
