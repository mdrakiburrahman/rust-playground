use crate::{
    CatalogName, ClaimedSchemaFingerprint, ImplementationPool, MAX_IDENTITY_COMPONENT_BYTES, ProtocolVersion,
    SchemaName, TableIdentity, TableName,
};
use thiserror::Error;

/// gRPC metadata key carrying the Arrow collector protocol version.
pub const PROTOCOL_VERSION_METADATA_KEY: &str = "x-arrowcol-protocol-version";
/// gRPC metadata key selecting the `dataflow` pool.
pub const IMPLEMENTATION_POOL_METADATA_KEY: &str = "x-arrowcol-implementation-pool";
/// gRPC metadata key carrying the catalog component.
pub const CATALOG_METADATA_KEY: &str = "x-arrowcol-catalog";
/// gRPC metadata key carrying the schema component.
pub const SCHEMA_METADATA_KEY: &str = "x-arrowcol-schema";
/// gRPC metadata key carrying the table component.
pub const TABLE_METADATA_KEY: &str = "x-arrowcol-table";
/// gRPC metadata key carrying the client's untrusted schema fingerprint claim.
pub const CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY: &str = "x-arrowcol-schema-fingerprint";
/// Absolute bound applied before parsing any Arrow collector metadata value.
pub const MAX_GRPC_METADATA_VALUE_BYTES: usize = 256;
/// Maximum protocol-version metadata value length.
pub const MAX_PROTOCOL_VERSION_METADATA_VALUE_BYTES: usize = 16;
/// Maximum implementation-pool metadata value length.
pub const MAX_IMPLEMENTATION_POOL_METADATA_VALUE_BYTES: usize = 16;
/// Maximum catalog, schema, or table metadata value length.
pub const MAX_IDENTITY_METADATA_VALUE_BYTES: usize = MAX_IDENTITY_COMPONENT_BYTES;
/// Maximum canonical claimed-fingerprint metadata value length.
pub const MAX_CLAIMED_SCHEMA_FINGERPRINT_METADATA_VALUE_BYTES: usize = 71;

/// Borrowed ASCII values extracted from a gRPC request's metadata map.
///
/// Transport adapters should populate this structure without interpreting the
/// values, then call [`IngestMetadata::parse`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RawGrpcMetadata<'a> {
    /// Raw protocol version metadata.
    pub protocol_version: Option<&'a str>,
    /// Raw implementation pool metadata.
    pub implementation_pool: Option<&'a str>,
    /// Raw catalog metadata.
    pub catalog: Option<&'a str>,
    /// Raw schema metadata.
    pub schema: Option<&'a str>,
    /// Raw table metadata.
    pub table: Option<&'a str>,
    /// Raw, untrusted schema fingerprint claim.
    pub claimed_schema_fingerprint: Option<&'a str>,
}

/// Validated routing metadata for one incoming Arrow delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestMetadata {
    protocol_version: ProtocolVersion,
    implementation_pool: ImplementationPool,
    table: TableIdentity,
    claimed_schema_fingerprint: ClaimedSchemaFingerprint,
}

impl IngestMetadata {
    /// Creates metadata from already validated contract values.
    #[must_use]
    pub const fn new(
        protocol_version: ProtocolVersion,
        implementation_pool: ImplementationPool,
        table: TableIdentity,
        claimed_schema_fingerprint: ClaimedSchemaFingerprint,
    ) -> Self {
        Self {
            protocol_version,
            implementation_pool,
            table,
            claimed_schema_fingerprint,
        }
    }

    /// Parses all required Arrow collector gRPC metadata.
    ///
    /// Values are bounded and checked for ASCII/control characters before
    /// domain parsing. The claimed fingerprint remains explicitly untrusted.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataParseError`] for a missing, oversized, control-bearing,
    /// malformed, or unsupported-version value.
    pub fn parse(raw: RawGrpcMetadata<'_>) -> Result<Self, MetadataParseError> {
        let version = required(
            PROTOCOL_VERSION_METADATA_KEY,
            raw.protocol_version,
            MAX_PROTOCOL_VERSION_METADATA_VALUE_BYTES,
        )?;
        let protocol_version = parse_protocol_version(version)?;

        let pool = required(
            IMPLEMENTATION_POOL_METADATA_KEY,
            raw.implementation_pool,
            MAX_IMPLEMENTATION_POOL_METADATA_VALUE_BYTES,
        )?
        .parse()
        .map_err(|_| MetadataParseError::Malformed {
            key: IMPLEMENTATION_POOL_METADATA_KEY,
            reason: "expected dataflow",
        })?;

        let catalog = CatalogName::new(required(
            CATALOG_METADATA_KEY,
            raw.catalog,
            MAX_IDENTITY_METADATA_VALUE_BYTES,
        )?)
        .map_err(|_| MetadataParseError::Malformed {
            key: CATALOG_METADATA_KEY,
            reason: "invalid catalog identity",
        })?;
        let schema = SchemaName::new(required(
            SCHEMA_METADATA_KEY,
            raw.schema,
            MAX_IDENTITY_METADATA_VALUE_BYTES,
        )?)
        .map_err(|_| MetadataParseError::Malformed {
            key: SCHEMA_METADATA_KEY,
            reason: "invalid schema identity",
        })?;
        let table = TableName::new(required(
            TABLE_METADATA_KEY,
            raw.table,
            MAX_IDENTITY_METADATA_VALUE_BYTES,
        )?)
        .map_err(|_| MetadataParseError::Malformed {
            key: TABLE_METADATA_KEY,
            reason: "invalid table identity",
        })?;

        let claimed_schema_fingerprint = required(
            CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY,
            raw.claimed_schema_fingerprint,
            MAX_CLAIMED_SCHEMA_FINGERPRINT_METADATA_VALUE_BYTES,
        )?
        .parse()
        .map_err(|_| MetadataParseError::Malformed {
            key: CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY,
            reason: "invalid canonical schema fingerprint",
        })?;

        Ok(Self {
            protocol_version,
            implementation_pool: pool,
            table: TableIdentity::new(catalog, schema, table),
            claimed_schema_fingerprint,
        })
    }

    /// Returns the negotiated protocol version.
    #[must_use]
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Returns the requested implementation pool.
    #[must_use]
    pub const fn implementation_pool(&self) -> ImplementationPool {
        self.implementation_pool
    }

    /// Returns the fully qualified target table.
    #[must_use]
    pub const fn table(&self) -> &TableIdentity {
        &self.table
    }

    /// Returns the client's untrusted schema fingerprint claim.
    #[must_use]
    pub const fn claimed_schema_fingerprint(&self) -> ClaimedSchemaFingerprint {
        self.claimed_schema_fingerprint
    }

    /// Encodes this validated contract as canonical ASCII gRPC metadata pairs.
    #[must_use]
    pub fn to_ascii_pairs(&self) -> [(&'static str, String); 6] {
        [
            (PROTOCOL_VERSION_METADATA_KEY, self.protocol_version.to_string()),
            (IMPLEMENTATION_POOL_METADATA_KEY, self.implementation_pool.to_string()),
            (CATALOG_METADATA_KEY, self.table.catalog().to_string()),
            (SCHEMA_METADATA_KEY, self.table.schema().to_string()),
            (TABLE_METADATA_KEY, self.table.table().to_string()),
            (
                CLAIMED_SCHEMA_FINGERPRINT_METADATA_KEY,
                self.claimed_schema_fingerprint.to_string(),
            ),
        ]
    }
}

/// Failure to parse required Arrow collector gRPC metadata.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MetadataParseError {
    /// A required key was absent.
    #[error("required gRPC metadata key {key} is missing")]
    Missing {
        /// Missing metadata key.
        key: &'static str,
    },
    /// A value exceeded its field-specific bound.
    #[error("gRPC metadata {key} is {actual} bytes; maximum is {maximum}")]
    Oversized {
        /// Metadata key.
        key: &'static str,
        /// Actual byte length.
        actual: usize,
        /// Maximum accepted byte length.
        maximum: usize,
    },
    /// A value contained an ASCII control character.
    #[error("gRPC metadata {key} contains a control character at byte {index}")]
    ControlCharacter {
        /// Metadata key.
        key: &'static str,
        /// Byte offset of the rejected character.
        index: usize,
    },
    /// A value did not match its canonical grammar.
    #[error("gRPC metadata {key} is malformed: {reason}")]
    Malformed {
        /// Metadata key.
        key: &'static str,
        /// Stable, non-sensitive explanation.
        reason: &'static str,
    },
    /// A syntactically valid protocol version is not supported.
    #[error("unsupported protocol version {found}")]
    UnsupportedVersion {
        /// Canonical decimal version supplied by the caller.
        found: String,
    },
}

fn required<'a>(key: &'static str, value: Option<&'a str>, maximum: usize) -> Result<&'a str, MetadataParseError> {
    let value = value.ok_or(MetadataParseError::Missing { key })?;
    let maximum = maximum.min(MAX_GRPC_METADATA_VALUE_BYTES);
    if value.len() > maximum {
        return Err(MetadataParseError::Oversized {
            key,
            actual: value.len(),
            maximum,
        });
    }
    if let Some(index) = value.bytes().position(|byte| byte.is_ascii_control()) {
        return Err(MetadataParseError::ControlCharacter { key, index });
    }
    if !value.is_ascii() {
        return Err(MetadataParseError::Malformed {
            key,
            reason: "gRPC ASCII metadata must contain only ASCII",
        });
    }
    Ok(value)
}

fn parse_protocol_version(value: &str) -> Result<ProtocolVersion, MetadataParseError> {
    if value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return Err(MetadataParseError::Malformed {
            key: PROTOCOL_VERSION_METADATA_KEY,
            reason: "expected canonical unsigned decimal",
        });
    }
    if value == ProtocolVersion::CURRENT.to_string() {
        return Ok(ProtocolVersion::CURRENT);
    }
    Err(MetadataParseError::UnsupportedVersion {
        found: value.to_owned(),
    })
}
