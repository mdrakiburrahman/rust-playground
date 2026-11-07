use std::error::Error;

use arrowcol_protocol::{
    CATALOG_METADATA_KEY, CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY, IMPLEMENTATION_POOL_METADATA_KEY,
    ImplementationPool, IngestMetadata, MetadataParseError, PROTOCOL_VERSION_METADATA_KEY, RawGrpcMetadata,
    SCHEMA_METADATA_KEY, TABLE_METADATA_KEY,
};

const FINGERPRINT: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

fn valid_raw() -> RawGrpcMetadata<'static> {
    RawGrpcMetadata {
        protocol_version: Some("1"),
        implementation_pool: Some("dataflow"),
        catalog: Some("telemetry"),
        schema: Some("prod"),
        table: Some("traces"),
        claimed_schema_fingerprint: Some(FINGERPRINT),
    }
}

#[test]
fn parses_and_reencodes_canonical_metadata() -> Result<(), Box<dyn Error>> {
    let parsed = IngestMetadata::parse(valid_raw())?;
    assert_eq!(parsed.implementation_pool(), ImplementationPool::Dataflow);
    assert_eq!(parsed.table().to_string(), "telemetry.prod.traces");

    let pairs = parsed.to_ascii_pairs();
    assert_eq!(pairs[0], (PROTOCOL_VERSION_METADATA_KEY, "1".to_owned()));
    assert_eq!(pairs[1], (IMPLEMENTATION_POOL_METADATA_KEY, "dataflow".to_owned()));
    assert_eq!(pairs[2], (CATALOG_METADATA_KEY, "telemetry".to_owned()));
    assert_eq!(pairs[3], (SCHEMA_METADATA_KEY, "prod".to_owned()));
    assert_eq!(pairs[4], (TABLE_METADATA_KEY, "traces".to_owned()));
    assert_eq!(
        pairs[5],
        (CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY, FINGERPRINT.to_owned())
    );
    Ok(())
}

#[test]
fn rejects_every_required_metadata_failure_class() {
    let missing = IngestMetadata::parse(RawGrpcMetadata::default());
    assert!(matches!(
        missing,
        Err(MetadataParseError::Missing {
            key: PROTOCOL_VERSION_METADATA_KEY
        })
    ));

    let mut malformed = valid_raw();
    malformed.implementation_pool = Some("fast");
    assert!(matches!(
        IngestMetadata::parse(malformed),
        Err(MetadataParseError::Malformed {
            key: IMPLEMENTATION_POOL_METADATA_KEY,
            ..
        })
    ));

    let mut retired = valid_raw();
    retired.implementation_pool = Some("direct");
    assert!(matches!(
        IngestMetadata::parse(retired),
        Err(MetadataParseError::Malformed {
            key: IMPLEMENTATION_POOL_METADATA_KEY,
            ..
        })
    ));
    assert!(serde_json::from_str::<ImplementationPool>("\"direct\"").is_err());

    let oversized_catalog = "x".repeat(129);
    let mut oversized = valid_raw();
    oversized.catalog = Some(&oversized_catalog);
    assert!(matches!(
        IngestMetadata::parse(oversized),
        Err(MetadataParseError::Oversized {
            key: CATALOG_METADATA_KEY,
            actual: 129,
            maximum: 128
        })
    ));

    let mut control = valid_raw();
    control.table = Some("traces\n");
    assert!(matches!(
        IngestMetadata::parse(control),
        Err(MetadataParseError::ControlCharacter {
            key: TABLE_METADATA_KEY,
            ..
        })
    ));

    let mut unsupported = valid_raw();
    unsupported.protocol_version = Some("2");
    assert_eq!(
        IngestMetadata::parse(unsupported),
        Err(MetadataParseError::UnsupportedVersion { found: "2".to_owned() })
    );

    let mut noncanonical = valid_raw();
    noncanonical.protocol_version = Some("01");
    assert!(matches!(
        IngestMetadata::parse(noncanonical),
        Err(MetadataParseError::Malformed {
            key: PROTOCOL_VERSION_METADATA_KEY,
            ..
        })
    ));
}

#[test]
fn claimed_fingerprint_is_not_implicit_schema_proof() -> Result<(), Box<dyn Error>> {
    let parsed = IngestMetadata::parse(valid_raw())?;
    let actual = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "different",
        arrow_schema::DataType::Int64,
        false,
    )]);
    assert!(!parsed.claimed_schema_fingerprint().matches_received_schema(&actual));
    Ok(())
}
