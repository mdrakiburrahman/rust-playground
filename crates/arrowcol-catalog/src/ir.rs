use std::{fmt, sync::Arc};

use arrow_schema::{DataType, Field, Fields, Schema};
use arrowcol_protocol::{SchemaFingerprint, TableIdentity, fingerprint_schema};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{CatalogError, CatalogErrorCode, CatalogResult};

const TABLE_NAMESPACE: Uuid = Uuid::from_bytes([
    0xa2, 0x68, 0x79, 0x2a, 0x31, 0x6b, 0x58, 0xa1, 0xa6, 0x10, 0xcc, 0x3d, 0xc1, 0x9f, 0x68, 0xe8,
]);

/// Immutable, positive schema version.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SchemaVersion(u64);

impl SchemaVersion {
    /// First version assigned to a newly created table.
    pub const FIRST: Self = Self(1);

    /// Creates a validated schema version.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogErrorCode::CorruptCatalogState`] for zero.
    pub fn new(value: u64) -> CatalogResult<Self> {
        if value == 0 {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                "schema version must be positive",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the numeric version.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next immutable version.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogErrorCode::CorruptCatalogState`] if the counter is
    /// exhausted.
    pub fn next(self) -> CatalogResult<Self> {
        self.0.checked_add(1).map(Self).ok_or_else(|| {
            CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                "schema version counter is exhausted",
            )
        })
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Canonical path used to derive a stable field identity.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct FieldPath(String);

impl FieldPath {
    /// Creates a root field path.
    #[must_use]
    pub fn root(name: &str) -> Self {
        Self(escape_path_component(name))
    }

    /// Returns a child path.
    #[must_use]
    pub fn child(&self, name: &str) -> Self {
        Self(format!("{}/{}", self.0, escape_path_component(name)))
    }

    /// Returns the canonical path text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Arrow time unit represented independently of a specific Arrow crate API.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnit {
    /// Seconds.
    Second,
    /// Milliseconds.
    Millisecond,
    /// Microseconds.
    Microsecond,
    /// Nanoseconds.
    Nanosecond,
}

impl TimeUnit {
    const fn arrow(self) -> arrow_schema::TimeUnit {
        match self {
            Self::Second => arrow_schema::TimeUnit::Second,
            Self::Millisecond => arrow_schema::TimeUnit::Millisecond,
            Self::Microsecond => arrow_schema::TimeUnit::Microsecond,
            Self::Nanosecond => arrow_schema::TimeUnit::Nanosecond,
        }
    }

    const fn sql_precision(self) -> u8 {
        match self {
            Self::Second => 0,
            Self::Millisecond => 3,
            Self::Microsecond => 6,
            Self::Nanosecond => 9,
        }
    }
}

/// Arrow list representation retained by the catalog.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ListKind {
    /// Standard 32-bit-offset list.
    List,
    /// Standard 64-bit-offset list.
    LargeList,
    /// Fixed-size list with the declared element count.
    FixedSize(i32),
    /// Arrow list-view representation.
    ListView,
    /// Arrow large-list-view representation.
    LargeListView,
}

/// Protobuf wire representation selected for a catalog field.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtobufType {
    /// `bool`.
    Bool,
    /// Signed or unsigned 32-bit integer wire type.
    Int32,
    /// Signed or unsigned 64-bit integer wire type.
    Int64,
    /// Unsigned 32-bit integer.
    Uint32,
    /// Unsigned 64-bit integer.
    Uint64,
    /// 32-bit floating point.
    Float,
    /// 64-bit floating point.
    Double,
    /// UTF-8 string.
    String,
    /// Byte string or a logical value encoded as bytes.
    Bytes,
    /// Generated nested message.
    Message,
    /// Repeated scalar or message.
    Repeated,
}

impl ProtobufType {
    /// Stable SQL storage value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::Uint32 => "uint32",
            Self::Uint64 => "uint64",
            Self::Float => "float",
            Self::Double => "double",
            Self::String => "string",
            Self::Bytes => "bytes",
            Self::Message => "message",
            Self::Repeated => "repeated",
        }
    }
}

/// Supported normalized Arrow type.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CatalogType {
    /// Boolean.
    Boolean,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    /// Unsigned 8-bit integer.
    UInt8,
    /// Unsigned 16-bit integer.
    UInt16,
    /// Unsigned 32-bit integer.
    UInt32,
    /// Unsigned 64-bit integer.
    UInt64,
    /// 32-bit floating point.
    Float32,
    /// 64-bit floating point.
    Float64,
    /// Timestamp with explicit Arrow unit and optional timezone.
    Timestamp {
        /// Physical unit.
        unit: TimeUnit,
        /// Arrow timezone text.
        timezone: Option<String>,
    },
    /// Arrow date32.
    Date32,
    /// Arrow date64.
    Date64,
    /// Arrow time32.
    Time32 {
        /// Physical unit.
        unit: TimeUnit,
    },
    /// Arrow time64.
    Time64 {
        /// Physical unit.
        unit: TimeUnit,
    },
    /// Arrow duration.
    Duration {
        /// Physical unit.
        unit: TimeUnit,
    },
    /// Variable-size binary.
    Binary,
    /// Fixed-size binary.
    FixedSizeBinary {
        /// Required byte width.
        bytes: i32,
    },
    /// Large binary.
    LargeBinary,
    /// Binary view.
    BinaryView,
    /// UTF-8 string.
    Utf8,
    /// Large UTF-8 string.
    LargeUtf8,
    /// UTF-8 view.
    Utf8View,
    /// Decimal128.
    Decimal128 {
        /// Decimal precision.
        precision: u8,
        /// Decimal scale.
        scale: i8,
    },
    /// Ordered struct fields.
    Struct {
        /// Nested fields.
        fields: Vec<CatalogField>,
    },
    /// List-like type with a cataloged element field.
    List {
        /// Arrow list representation.
        list_kind: ListKind,
        /// Element field.
        element: Box<CatalogField>,
    },
    /// Arrow map represented by cataloged key and value fields.
    Map {
        /// Key field.
        key: Box<CatalogField>,
        /// Value field.
        value: Box<CatalogField>,
        /// Whether keys are declared sorted.
        keys_sorted: bool,
    },
    /// Dictionary-encoded value.
    Dictionary {
        /// Integer dictionary key type.
        key: Box<CatalogType>,
        /// Logical dictionary value type.
        value: Box<CatalogType>,
        /// Whether dictionary values are ordered.
        ordered: bool,
    },
}

impl CatalogType {
    /// Returns a lossless Spark SQL type spelling.
    ///
    /// # Errors
    ///
    /// Rejects Arrow physical representations not expressible by Spark DDL.
    pub fn spark_name(&self) -> CatalogResult<String> {
        let name = match self {
            Self::Boolean => "BOOLEAN".to_owned(),
            Self::Int8 => "TINYINT".to_owned(),
            Self::Int16 => "SMALLINT".to_owned(),
            Self::Int32 => "INT".to_owned(),
            Self::Int64 => "BIGINT".to_owned(),
            Self::Float32 => "FLOAT".to_owned(),
            Self::Float64 => "DOUBLE".to_owned(),
            Self::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            } => "TIMESTAMP_NTZ".to_owned(),
            Self::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some(zone),
            } if zone == "UTC" => "TIMESTAMP".to_owned(),
            Self::Date32 => "DATE".to_owned(),
            Self::Binary => "BINARY".to_owned(),
            Self::Utf8 => "STRING".to_owned(),
            Self::Decimal128 { precision, scale }
                if *precision > 0 && *precision <= 38 && *scale >= 0 && *scale <= *precision as i8 =>
            {
                format!("DECIMAL({precision},{scale})")
            }
            Self::Struct { fields } => format!(
                "STRUCT<{}>",
                fields
                    .iter()
                    .map(|field| {
                        let mut sql = format!(
                            "{}: {}",
                            arrowcol_sparksql::quote_identifier(field.name()),
                            field.mapping().spark()?
                        );
                        if !field.is_nullable() {
                            sql.push_str(" NOT NULL");
                        }
                        if let Some(comment) = field.comment() {
                            sql.push_str(&format!(" COMMENT {}", arrowcol_sparksql::quote_string(comment)));
                        }
                        Ok(sql)
                    })
                    .collect::<CatalogResult<Vec<_>>>()?
                    .join(", ")
            ),
            Self::List {
                list_kind: ListKind::List,
                element,
            } if element.is_nullable() && element.name() == "item" && element.comment().is_none() => {
                format!("ARRAY<{}>", element.mapping().spark()?)
            }
            Self::Map {
                key,
                value,
                keys_sorted: false,
            } if !key.is_nullable()
                && value.is_nullable()
                && key.name() == "key"
                && value.name() == "value"
                && key.comment().is_none()
                && value.comment().is_none() =>
            {
                format!("MAP<{}, {}>", key.mapping().spark()?, value.mapping().spark()?)
            }
            _ => {
                return Err(CatalogError::new(
                    CatalogErrorCode::TypeMappingUnsupported,
                    format!("Arrow type {self:?} has no lossless Spark SQL representation"),
                ));
            }
        };
        Ok(name)
    }

    /// Converts the normalized type to Arrow.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogErrorCode::CorruptCatalogState`] if persisted nested
    /// map/list invariants are invalid.
    pub fn to_arrow(&self) -> CatalogResult<DataType> {
        let data_type = match self {
            Self::Boolean => DataType::Boolean,
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::UInt8 => DataType::UInt8,
            Self::UInt16 => DataType::UInt16,
            Self::UInt32 => DataType::UInt32,
            Self::UInt64 => DataType::UInt64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Timestamp { unit, timezone } => DataType::Timestamp(unit.arrow(), timezone.clone().map(Into::into)),
            Self::Date32 => DataType::Date32,
            Self::Date64 => DataType::Date64,
            Self::Time32 { unit } => DataType::Time32(unit.arrow()),
            Self::Time64 { unit } => DataType::Time64(unit.arrow()),
            Self::Duration { unit } => DataType::Duration(unit.arrow()),
            Self::Binary => DataType::Binary,
            Self::FixedSizeBinary { bytes } => DataType::FixedSizeBinary(*bytes),
            Self::LargeBinary => DataType::LargeBinary,
            Self::BinaryView => DataType::BinaryView,
            Self::Utf8 => DataType::Utf8,
            Self::LargeUtf8 => DataType::LargeUtf8,
            Self::Utf8View => DataType::Utf8View,
            Self::Decimal128 { precision, scale } => DataType::Decimal128(*precision, *scale),
            Self::Struct { fields } => {
                let fields = fields
                    .iter()
                    .map(CatalogField::to_arrow)
                    .collect::<CatalogResult<Vec<_>>>()?;
                DataType::Struct(Fields::from(fields))
            }
            Self::List { list_kind, element } => {
                let element = Arc::new(element.to_arrow()?);
                match list_kind {
                    ListKind::List => DataType::List(element),
                    ListKind::LargeList => DataType::LargeList(element),
                    ListKind::FixedSize(size) => DataType::FixedSizeList(element, *size),
                    ListKind::ListView => DataType::ListView(element),
                    ListKind::LargeListView => DataType::LargeListView(element),
                }
            }
            Self::Map {
                key,
                value,
                keys_sorted,
            } => {
                if key.is_nullable() {
                    return Err(CatalogError::new(
                        CatalogErrorCode::CorruptCatalogState,
                        "Arrow map keys must be non-nullable",
                    ));
                }
                let entries = Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![key.to_arrow()?, value.to_arrow()?])),
                    false,
                );
                DataType::Map(Arc::new(entries), *keys_sorted)
            }
            Self::Dictionary { key, value, .. } => {
                DataType::Dictionary(Box::new(key.to_arrow()?), Box::new(value.to_arrow()?))
            }
        };
        Ok(data_type)
    }

    fn datafusion_name(&self) -> String {
        match self {
            Self::Boolean => "BOOLEAN".to_owned(),
            Self::Int8 => "TINYINT".to_owned(),
            Self::Int16 => "SMALLINT".to_owned(),
            Self::Int32 => "INTEGER".to_owned(),
            Self::Int64 => "BIGINT".to_owned(),
            Self::UInt8 => "TINYINT UNSIGNED".to_owned(),
            Self::UInt16 => "SMALLINT UNSIGNED".to_owned(),
            Self::UInt32 => "INTEGER UNSIGNED".to_owned(),
            Self::UInt64 => "BIGINT UNSIGNED".to_owned(),
            Self::Float32 => "REAL".to_owned(),
            Self::Float64 => "DOUBLE".to_owned(),
            Self::Timestamp { unit, timezone } => {
                let zone = if timezone.is_some() { " WITH TIME ZONE" } else { "" };
                format!("TIMESTAMP({}){zone}", unit.sql_precision())
            }
            Self::Date32 => "DATE".to_owned(),
            Self::Date64 => "DATE64".to_owned(),
            Self::Time32 { unit } => format!("TIME32({})", unit.sql_precision()),
            Self::Time64 { unit } => format!("TIME({})", unit.sql_precision()),
            Self::Duration { unit } => format!("DURATION_{}", format!("{unit:?}").to_ascii_uppercase()),
            Self::Binary => "BYTEA".to_owned(),
            Self::FixedSizeBinary { bytes } => format!("FIXED_SIZE_BINARY({bytes})"),
            Self::LargeBinary => "LARGE_BINARY".to_owned(),
            Self::BinaryView => "BINARY_VIEW".to_owned(),
            Self::Utf8 => "VARCHAR".to_owned(),
            Self::LargeUtf8 => "LARGE_STRING".to_owned(),
            Self::Utf8View => "STRING_VIEW".to_owned(),
            Self::Decimal128 { precision, scale } => format!("DECIMAL({precision},{scale})"),
            Self::Struct { fields } => format!(
                "STRUCT<{}>",
                fields
                    .iter()
                    .map(|field| format!("{} {}", field.name, field.mapping.datafusion))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::List { list_kind, element } => match list_kind {
                ListKind::List => format!("ARRAY<{}>", element.mapping.datafusion),
                ListKind::LargeList => format!("LARGE_LIST<{}>", element.mapping.datafusion),
                ListKind::FixedSize(size) => format!("{}[{size}]", element.mapping.datafusion),
                ListKind::ListView => format!("LIST_VIEW<{}>", element.mapping.datafusion),
                ListKind::LargeListView => format!("LARGE_LIST_VIEW<{}>", element.mapping.datafusion),
            },
            Self::Map { key, value, .. } => {
                format!("MAP({}, {})", key.mapping.datafusion, value.mapping.datafusion)
            }
            Self::Dictionary { key, value, ordered } => format!(
                "DICTIONARY<{}, {}>{}",
                key.datafusion_name(),
                value.datafusion_name(),
                if *ordered { " ORDERED" } else { "" }
            ),
        }
    }

    fn hive_name(&self) -> String {
        match self {
            Self::Boolean => "boolean".to_owned(),
            Self::Int8 => "tinyint".to_owned(),
            Self::Int16 | Self::UInt8 => "smallint".to_owned(),
            Self::Int32 | Self::UInt16 => "int".to_owned(),
            Self::Int64 | Self::UInt32 => "bigint".to_owned(),
            Self::UInt64 => "decimal(20,0)".to_owned(),
            Self::Float32 => "float".to_owned(),
            Self::Float64 => "double".to_owned(),
            Self::Timestamp { .. } => "timestamp".to_owned(),
            Self::Date32 | Self::Date64 => "date".to_owned(),
            Self::Time32 { .. } | Self::Time64 { .. } | Self::Duration { .. } => "bigint".to_owned(),
            Self::Binary | Self::FixedSizeBinary { .. } | Self::LargeBinary | Self::BinaryView => "binary".to_owned(),
            Self::Utf8 | Self::LargeUtf8 | Self::Utf8View => "string".to_owned(),
            Self::Decimal128 { precision, scale } => format!("decimal({precision},{scale})"),
            Self::Struct { fields } => format!(
                "struct<{}>",
                fields
                    .iter()
                    .map(|field| format!("{}:{}", field.name, field.mapping.hive))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::List { element, .. } => format!("array<{}>", element.mapping.hive),
            Self::Map { key, value, .. } => format!("map<{},{}>", key.mapping.hive, value.mapping.hive),
            Self::Dictionary { value, .. } => value.hive_name(),
        }
    }

    pub(crate) const fn protobuf_type(&self) -> ProtobufType {
        match self {
            Self::Boolean => ProtobufType::Bool,
            Self::Int8 | Self::Int16 | Self::Int32 | Self::Date32 | Self::Time32 { .. } => ProtobufType::Int32,
            Self::Int64 | Self::Timestamp { .. } | Self::Date64 | Self::Time64 { .. } | Self::Duration { .. } => {
                ProtobufType::Int64
            }
            Self::UInt8 | Self::UInt16 | Self::UInt32 => ProtobufType::Uint32,
            Self::UInt64 => ProtobufType::Uint64,
            Self::Float32 => ProtobufType::Float,
            Self::Float64 => ProtobufType::Double,
            Self::Utf8 | Self::LargeUtf8 | Self::Utf8View => ProtobufType::String,
            Self::Binary
            | Self::FixedSizeBinary { .. }
            | Self::LargeBinary
            | Self::BinaryView
            | Self::Decimal128 { .. } => ProtobufType::Bytes,
            Self::Struct { .. } => ProtobufType::Message,
            Self::List { .. } | Self::Map { .. } => ProtobufType::Repeated,
            Self::Dictionary { value, .. } => value.protobuf_type(),
        }
    }
}

/// Exact cross-system type mapping retained for one field.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TypeMapping {
    arrow: CatalogType,
    spark: Option<String>,
    datafusion: String,
    hive: String,
    protobuf: ProtobufType,
}

impl TypeMapping {
    /// Creates a mapping from a supported normalized Arrow type.
    #[must_use]
    pub fn new(arrow: CatalogType) -> Self {
        let spark = arrow.spark_name().ok();
        let datafusion = arrow.datafusion_name();
        let hive = arrow.hive_name();
        let protobuf = arrow.protobuf_type();
        Self {
            arrow,
            spark,
            datafusion,
            hive,
            protobuf,
        }
    }

    /// Returns the normalized Arrow type.
    #[must_use]
    pub const fn arrow(&self) -> &CatalogType {
        &self.arrow
    }

    /// Returns the canonical lossless Spark SQL type.
    ///
    /// # Errors
    ///
    /// Returns `TypeMappingUnsupported` for Arrow-only representations.
    pub fn spark(&self) -> CatalogResult<&str> {
        self.spark.as_deref().ok_or_else(|| {
            CatalogError::new(
                CatalogErrorCode::TypeMappingUnsupported,
                format!("Arrow type {:?} has no lossless Spark SQL representation", self.arrow),
            )
        })
    }

    /// Returns the canonical DataFusion SQL type.
    #[must_use]
    pub fn datafusion(&self) -> &str {
        &self.datafusion
    }

    /// Returns the canonical Hive type.
    #[must_use]
    pub fn hive(&self) -> &str {
        &self.hive
    }

    /// Returns the protobuf representation.
    #[must_use]
    pub const fn protobuf(&self) -> ProtobufType {
        self.protobuf
    }

    /// Revalidates redundant persisted mapping strings.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogErrorCode::CorruptCatalogState`] if persisted values
    /// disagree with the normalized Arrow type.
    pub fn validate(&self) -> CatalogResult<()> {
        let expected = Self::new(self.arrow.clone());
        if *self == expected {
            Ok(())
        } else {
            Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                "persisted type mapping is internally inconsistent",
            ))
        }
    }
}

/// Partition role for a top-level field.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PartitionMetadata {
    ordinal: u32,
}

impl PartitionMetadata {
    /// Creates partition metadata.
    #[must_use]
    pub const fn new(ordinal: u32) -> Self {
        Self { ordinal }
    }

    /// Returns the field's zero-based partition-key ordinal.
    #[must_use]
    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }
}

/// One stable ordered field in a catalog schema.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogField {
    id: Uuid,
    name: String,
    position: u32,
    field_number: u32,
    nullable: bool,
    partition: Option<PartitionMetadata>,
    mapping: TypeMapping,
    #[serde(default)]
    comment: Option<String>,
}

impl CatalogField {
    /// Creates a catalog field.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogErrorCode::CorruptCatalogState`] for field number zero
    /// or an empty name.
    pub fn new(
        id: Uuid,
        name: impl Into<String>,
        position: u32,
        field_number: u32,
        nullable: bool,
        partition: Option<PartitionMetadata>,
        mapping: TypeMapping,
    ) -> CatalogResult<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                "catalog field name must not be empty",
            ));
        }
        if field_number == 0 || (19_000..=19_999).contains(&field_number) || field_number > 536_870_911 {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                "protobuf field number must be 1..=536870911 excluding 19000..=19999",
            ));
        }
        Ok(Self {
            id,
            name,
            position,
            field_number,
            nullable,
            partition,
            mapping,
            comment: None,
        })
    }

    /// Returns the stable field identifier.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the immutable zero-based sibling position.
    #[must_use]
    pub const fn position(&self) -> u32 {
        self.position
    }

    /// Returns the immutable protobuf field number.
    #[must_use]
    pub const fn field_number(&self) -> u32 {
        self.field_number
    }

    /// Returns whether the field is nullable.
    #[must_use]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Returns partition metadata for a partition key.
    #[must_use]
    pub const fn partition(&self) -> Option<PartitionMetadata> {
        self.partition
    }

    /// Returns the cross-system type mapping.
    #[must_use]
    pub const fn mapping(&self) -> &TypeMapping {
        &self.mapping
    }

    /// Returns the optional Spark field comment.
    #[must_use]
    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    /// Attaches the optional Spark field comment.
    #[must_use]
    pub fn with_comment(mut self, comment: Option<String>) -> Self {
        self.comment = comment;
        self
    }

    /// Converts this field to Arrow.
    ///
    /// # Errors
    ///
    /// Returns an error if nested persisted invariants are invalid.
    pub fn to_arrow(&self) -> CatalogResult<Field> {
        let field = Field::new(&self.name, self.mapping.arrow.to_arrow()?, self.nullable);
        if let CatalogType::Dictionary { ordered, .. } = self.mapping.arrow() {
            Ok(field.with_dict_is_ordered(*ordered))
        } else {
            Ok(field)
        }
    }
}

/// One immutable registered schema version.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogSchemaVersion {
    table_id: Uuid,
    identity: TableIdentity,
    version: SchemaVersion,
    fields: Vec<CatalogField>,
    fingerprint: SchemaFingerprint,
    canonical_ddl: String,
    #[serde(default)]
    metadata: arrowcol_sparksql::TableMetadata,
}

impl CatalogSchemaVersion {
    /// Creates and fingerprints an immutable schema version.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate positions, field numbers, names, or
    /// inconsistent nested mappings.
    pub fn new(
        table_id: Uuid,
        identity: TableIdentity,
        version: SchemaVersion,
        fields: Vec<CatalogField>,
        canonical_ddl: impl Into<String>,
    ) -> CatalogResult<Self> {
        validate_sibling_fields(&fields, true)?;
        let schema = arrow_schema_from_fields(&fields)?;
        let fingerprint = fingerprint_schema(&schema);
        let mut partitions = fields
            .iter()
            .filter_map(|field| field.partition().map(|role| (role.ordinal(), field.name().to_owned())))
            .collect::<Vec<_>>();
        partitions.sort_by_key(|(ordinal, _)| *ordinal);
        let metadata = arrowcol_sparksql::TableMetadata {
            partition_columns: partitions.into_iter().map(|(_, name)| name).collect(),
            ..Default::default()
        };
        Ok(Self {
            table_id,
            identity,
            version,
            fields,
            fingerprint,
            canonical_ddl: canonical_ddl.into(),
            metadata,
        })
    }

    /// Returns the deterministic table identifier.
    #[must_use]
    pub const fn table_id(&self) -> Uuid {
        self.table_id
    }

    /// Returns the fully qualified table identity.
    #[must_use]
    pub const fn identity(&self) -> &TableIdentity {
        &self.identity
    }

    /// Returns this immutable version number.
    #[must_use]
    pub const fn version(&self) -> SchemaVersion {
        self.version
    }

    /// Returns ordered top-level fields.
    #[must_use]
    pub fn fields(&self) -> &[CatalogField] {
        &self.fields
    }

    /// Returns the canonical Arrow schema fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> SchemaFingerprint {
        self.fingerprint
    }

    /// Returns canonical normalized DDL.
    #[must_use]
    pub fn canonical_ddl(&self) -> &str {
        &self.canonical_ddl
    }

    /// Returns table properties, provider, location, comment and partition names.
    #[must_use]
    pub const fn metadata(&self) -> &arrowcol_sparksql::TableMetadata {
        &self.metadata
    }

    /// Sets metadata while enforcing its agreement with the partition fields.
    ///
    /// # Errors
    ///
    /// Rejects non-Delta providers or inconsistent partition declarations.
    pub fn with_metadata(mut self, metadata: arrowcol_sparksql::TableMetadata) -> CatalogResult<Self> {
        if metadata.provider != "delta" || metadata.partition_columns != self.metadata.partition_columns {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                "metadata provider or partition columns disagree with the schema",
            ));
        }
        self.metadata = metadata;
        Ok(self)
    }

    /// Renders the full registered table as lossless Spark SQL.
    ///
    /// # Errors
    ///
    /// Rejects Arrow-only field representations.
    pub fn to_spark_sql(&self) -> CatalogResult<String> {
        let columns = self
            .fields
            .iter()
            .map(|field| {
                let mut sql = format!(
                    "{} {}",
                    arrowcol_sparksql::quote_identifier(field.name()),
                    field.mapping().spark()?
                );
                if !field.is_nullable() {
                    sql.push_str(" NOT NULL");
                }
                if let Some(comment) = field.comment() {
                    sql.push_str(&format!(" COMMENT {}", arrowcol_sparksql::quote_string(comment)));
                }
                Ok(sql)
            })
            .collect::<CatalogResult<Vec<_>>>()?;
        let name = self
            .identity
            .to_string()
            .split('.')
            .map(arrowcol_sparksql::quote_identifier)
            .collect::<Vec<_>>()
            .join(".");
        let mut sql = format!("CREATE TABLE {name} ({}) USING DELTA", columns.join(", "));
        if !self.metadata.partition_columns.is_empty() {
            sql.push_str(&format!(
                " PARTITIONED BY ({})",
                self.metadata
                    .partition_columns
                    .iter()
                    .map(|name| arrowcol_sparksql::quote_identifier(name))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(location) = &self.metadata.location {
            sql.push_str(&format!(" LOCATION {}", arrowcol_sparksql::quote_string(location)));
        }
        if let Some(comment) = &self.metadata.comment {
            sql.push_str(&format!(" COMMENT {}", arrowcol_sparksql::quote_string(comment)));
        }
        if !self.metadata.properties.is_empty() {
            sql.push_str(&format!(
                " TBLPROPERTIES ({})",
                self.metadata
                    .properties
                    .iter()
                    .map(|(key, value)| format!(
                        "{}={}",
                        arrowcol_sparksql::quote_string(key),
                        arrowcol_sparksql::quote_string(value)
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        Ok(sql)
    }

    /// Reconstructs the exact registered Arrow schema.
    ///
    /// # Errors
    ///
    /// Returns an error if nested persisted invariants are invalid.
    pub fn arrow_schema(&self) -> CatalogResult<Schema> {
        arrow_schema_from_fields(&self.fields)
    }

    /// Returns the next unused top-level position.
    ///
    /// # Errors
    ///
    /// Returns a corrupt-state error when the position space is exhausted.
    pub fn next_position(&self) -> CatalogResult<u32> {
        self.fields
            .iter()
            .map(CatalogField::position)
            .max()
            .map_or(Ok(0), |position| {
                position.checked_add(1).ok_or_else(field_counter_exhausted)
            })
    }

    /// Returns the next unused top-level protobuf field number.
    ///
    /// # Errors
    ///
    /// Returns a corrupt-state error when the protobuf number space is
    /// exhausted.
    pub fn next_field_number(&self) -> CatalogResult<u32> {
        self.fields
            .iter()
            .map(CatalogField::field_number)
            .max()
            .map_or(Ok(1), next_protobuf_field_number)
    }
}

/// Returns the next legal protobuf field number, skipping its reserved range.
///
/// # Errors
///
/// Returns a corrupt-state error when the protobuf field-number space is exhausted.
pub fn next_protobuf_field_number(current: u32) -> CatalogResult<u32> {
    let next = current.checked_add(1).ok_or_else(field_counter_exhausted)?;
    let next = if (19_000..=19_999).contains(&next) {
        20_000
    } else {
        next
    };
    if next > 536_870_911 {
        return Err(field_counter_exhausted());
    }
    Ok(next)
}

/// Maps a zero-based sibling ordinal to a legal deterministic protobuf number.
///
/// # Errors
///
/// Returns a corrupt-state error when the field-number space is exhausted.
pub fn protobuf_field_number(position: u32) -> CatalogResult<u32> {
    let offset = if position >= 18_999 { 1_001 } else { 1 };
    let number = position.checked_add(offset).ok_or_else(field_counter_exhausted)?;
    if number > 536_870_911 {
        return Err(field_counter_exhausted());
    }
    Ok(number)
}

impl CatalogSchemaVersion {
    /// Returns whether this version is a nullable append-only superset.
    #[must_use]
    pub fn is_nullable_append_only_superset_of(&self, older: &Self) -> bool {
        self.identity == older.identity
            && self.version >= older.version
            && self.fields.len() >= older.fields.len()
            && self.fields[..older.fields.len()] == older.fields
            && self.fields[older.fields.len()..].iter().all(CatalogField::is_nullable)
    }
}

/// Derives a stable table UUID from the portable table identity.
#[must_use]
pub fn deterministic_table_id(identity: &TableIdentity) -> Uuid {
    Uuid::new_v5(&TABLE_NAMESPACE, identity.to_string().as_bytes())
}

/// Derives a stable field UUID from its table and never-reused path.
#[must_use]
pub fn deterministic_field_id(table_id: Uuid, path: &FieldPath) -> Uuid {
    Uuid::new_v5(&table_id, path.as_str().as_bytes())
}

fn arrow_schema_from_fields(fields: &[CatalogField]) -> CatalogResult<Schema> {
    fields
        .iter()
        .map(CatalogField::to_arrow)
        .collect::<CatalogResult<Vec<_>>>()
        .map(Schema::new)
}

fn validate_sibling_fields(fields: &[CatalogField], allow_partition: bool) -> CatalogResult<()> {
    let mut names = std::collections::BTreeSet::new();
    let mut positions = std::collections::BTreeSet::new();
    let mut numbers = std::collections::BTreeSet::new();
    let mut partition_ordinals = std::collections::BTreeSet::new();
    for field in fields {
        field.mapping.validate()?;
        if !names.insert(field.name.clone()) {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                format!("duplicate sibling field name {}", field.name),
            ));
        }
        if !positions.insert(field.position) {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                format!("duplicate sibling field position {}", field.position),
            ));
        }
        if !numbers.insert(field.field_number) {
            return Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                format!("duplicate sibling protobuf field number {}", field.field_number),
            ));
        }
        if let Some(partition) = field.partition {
            if !allow_partition {
                return Err(CatalogError::new(
                    CatalogErrorCode::CorruptCatalogState,
                    "nested fields cannot be partition keys",
                ));
            }
            if !partition_ordinals.insert(partition.ordinal) {
                return Err(CatalogError::new(
                    CatalogErrorCode::CorruptCatalogState,
                    format!("duplicate partition ordinal {}", partition.ordinal),
                ));
            }
        }
        validate_nested_type(field.mapping.arrow())?;
    }
    Ok(())
}

fn validate_nested_type(data_type: &CatalogType) -> CatalogResult<()> {
    match data_type {
        CatalogType::Struct { fields } => validate_sibling_fields(fields, false),
        CatalogType::List { list_kind, element } => {
            if matches!(list_kind, ListKind::FixedSize(size) if *size <= 0) {
                return Err(CatalogError::new(
                    CatalogErrorCode::CorruptCatalogState,
                    "fixed-size list length must be positive",
                ));
            }
            validate_sibling_fields(std::slice::from_ref(element.as_ref()), false)
        }
        CatalogType::Map { key, value, .. } => {
            if key.is_nullable() {
                return Err(CatalogError::new(
                    CatalogErrorCode::CorruptCatalogState,
                    "map key must be non-nullable",
                ));
            }
            validate_sibling_fields(&[key.as_ref().clone(), value.as_ref().clone()], false)
        }
        CatalogType::Dictionary { key, value, .. } => {
            if !matches!(
                key.as_ref(),
                CatalogType::Int8
                    | CatalogType::Int16
                    | CatalogType::Int32
                    | CatalogType::Int64
                    | CatalogType::UInt8
                    | CatalogType::UInt16
                    | CatalogType::UInt32
                    | CatalogType::UInt64
            ) {
                return Err(CatalogError::new(
                    CatalogErrorCode::CorruptCatalogState,
                    "dictionary key type must be an integer",
                ));
            }
            validate_nested_type(value)
        }
        CatalogType::FixedSizeBinary { bytes } if *bytes <= 0 => Err(CatalogError::new(
            CatalogErrorCode::CorruptCatalogState,
            "fixed-size binary width must be positive",
        )),
        CatalogType::Decimal128 { precision, scale }
            if *precision == 0 || *precision > 38 || scale.unsigned_abs() > *precision =>
        {
            Err(CatalogError::new(
                CatalogErrorCode::CorruptCatalogState,
                "decimal precision or scale is invalid",
            ))
        }
        _ => Ok(()),
    }
}

fn escape_path_component(value: &str) -> String {
    value.replace('%', "%25").replace('/', "%2F")
}

fn field_counter_exhausted() -> CatalogError {
    CatalogError::new(
        CatalogErrorCode::CorruptCatalogState,
        "field position or protobuf number space is exhausted",
    )
}
