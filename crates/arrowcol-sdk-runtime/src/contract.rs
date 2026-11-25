//! Immutable, validated runtime contracts. DDL options are annotations, never client policy.
use std::{collections::HashSet, fs, path::Path, sync::Arc};

use arrow_array::{builder::FixedSizeBinaryBuilder, *};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage, Value,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{Error, Result, Routing, decimal128, fingerprint::fingerprint};

#[derive(Clone, Debug)]
pub struct ArrowcolContract {
    schema: SchemaRef,
    descriptor: MessageDescriptor,
    fields: Vec<FieldDescriptor>,
    identity: [String; 3],
    version: u64,
    fingerprint: String,
    bundle_digest: String,
    manifest: serde_json::Value,
}

#[derive(Deserialize)]
struct Manifest {
    artifact_manifest_version: u32,
    schema: ManifestSchema,
}
#[derive(Deserialize)]
struct ManifestSchema {
    table_id: String,
    identity: Identity,
    version: u64,
    fingerprint: String,
    fields: Vec<ManifestField>,
}
#[derive(Deserialize)]
struct Identity {
    catalog: String,
    schema: String,
    table: String,
}
#[derive(Deserialize)]
struct ManifestField {
    id: String,
    name: String,
    position: usize,
    field_number: u32,
    nullable: bool,
    mapping: Mapping,
}
#[derive(Deserialize)]
struct Mapping {
    arrow: serde_json::Value,
    protobuf: String,
}

impl ArrowcolContract {
    /// Reads the three canonical binary/JSON artifacts. Sources and optional DDL annotations
    /// do not configure client behavior. No network is contacted.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let read = |name| -> Result<Vec<u8>> {
            let file = path.as_ref().join(name);
            if fs::metadata(&file).map_err(Error::invalid)?.len() > 16 * 1024 * 1024 {
                return Err(Error::invalid("contract artifact exceeds 16 MiB"));
            }
            fs::read(file).map_err(Error::invalid)
        };
        Self::from_artifacts(&read("manifest.json")?, &read("schema.arrow")?, &read("descriptor.pb")?)
    }

    pub fn from_artifacts(manifest: &[u8], arrow_bytes: &[u8], descriptors: &[u8]) -> Result<Self> {
        let manifest_value = serde_json::from_slice(manifest).map_err(Error::invalid)?;
        let parsed: Manifest = serde_json::from_slice(manifest).map_err(Error::invalid)?;
        if parsed.artifact_manifest_version != 1 || parsed.schema.version == 0 || !uuid_shape(&parsed.schema.table_id) {
            return Err(Error::invalid(
                "invalid contract manifest version, schema version, or table identity",
            ));
        }
        let identity = [
            parsed.schema.identity.catalog,
            parsed.schema.identity.schema,
            parsed.schema.identity.table,
        ];
        if identity
            .iter()
            .any(|name| name.is_empty() || name.len() > 128 || !name.bytes().all(|b| (0x20..=0x7e).contains(&b)))
        {
            return Err(Error::invalid(
                "table identity cannot be represented by routing metadata",
            ));
        }
        let schema =
            Arc::new(arrow_ipc::convert::try_schema_from_flatbuffer_bytes(arrow_bytes).map_err(Error::invalid)?);
        if fingerprint(&schema)? != parsed.schema.fingerprint {
            return Err(Error::invalid("manifest fingerprint does not match schema.arrow"));
        }
        let pool = DescriptorPool::decode(descriptors).map_err(Error::invalid)?;
        let full_name = format!(
            "arrowcol.{}.{}.{}",
            identifier(&identity[0], false),
            identifier(&identity[1], false),
            identifier(&identity[2], true)
        );
        let descriptor = pool
            .get_message_by_name(&full_name)
            .ok_or_else(|| Error::invalid("descriptor does not match manifest table identity"))?;
        if descriptor.parent_file().name() != "schema.proto"
            || descriptor.fields().len() != parsed.schema.fields.len()
            || schema.fields().len() != parsed.schema.fields.len()
        {
            return Err(Error::invalid(
                "descriptor/schema/manifest field count or source mismatch",
            ));
        }
        Self::validate_annotations(&manifest_value, &descriptor)?;
        let mut fields = Vec::new();
        let mut ids = HashSet::new();
        let mut numbers = HashSet::new();
        for (position, (column, field)) in schema.fields().iter().zip(&parsed.schema.fields).enumerate() {
            if field.position != position
                || !uuid_shape(&field.id)
                || !ids.insert(&field.id)
                || !numbers.insert(field.field_number)
                || column.name() != &field.name
                || column.is_nullable() != field.nullable
                || column.data_type() != &mapping_type(&field.mapping.arrow)?
            {
                return Err(Error::invalid("manifest field does not match Arrow schema"));
            }
            let proto = descriptor
                .get_field(field.field_number)
                .ok_or_else(|| Error::invalid("missing protobuf field number"))?;
            if proto.name() != identifier(&field.name, false)
                || proto.json_name() != field.name
                || proto.kind() != protobuf_kind(column.data_type())?
                || field.mapping.protobuf != kind_name(&proto.kind())?
                || !proto.supports_presence()
                || proto.cardinality() != Cardinality::Optional
                || proto.containing_oneof().is_some()
                || proto.field_descriptor_proto().default_value.is_some()
            {
                return Err(Error::invalid(format!(
                    "protobuf mapping/presence mismatch for {}",
                    field.name
                )));
            }
            fields.push(proto);
        }
        let mut digest = Sha256::new();
        for bytes in [manifest, arrow_bytes, descriptors] {
            // Framing prevents ambiguous concatenations; this is NOT the schema fingerprint.
            digest.update((bytes.len() as u64).to_le_bytes());
            digest.update(bytes);
        }
        Ok(Self {
            schema,
            descriptor,
            fields,
            identity,
            version: parsed.schema.version,
            fingerprint: parsed.schema.fingerprint,
            bundle_digest: format!("sha256:{:x}", digest.finalize()),
            manifest: manifest_value,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    pub fn descriptor(&self) -> MessageDescriptor {
        self.descriptor.clone()
    }
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
    /// SHA-256 of manifest, IPC schema and descriptor bytes, each framed by its
    /// little-endian u64 length, in that order. Distinct from the schema fingerprint.
    pub fn bundle_digest(&self) -> &str {
        &self.bundle_digest
    }
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn identity(&self) -> &[String; 3] {
        &self.identity
    }
    pub fn manifest(&self) -> &serde_json::Value {
        &self.manifest
    }

    /// Compare the actual compiled message, not a caller-supplied schema fingerprint.
    /// The sole alternate name is the C++ package-component `public` -> `public_` rewrite.
    pub fn validate_model(&self, model: &MessageDescriptor) -> Result<()> {
        let canonical_name = self.descriptor.full_name();
        let cpp_package = self
            .descriptor
            .parent_file()
            .package_name()
            .split('.')
            .map(|part| if part == "public" { "public_" } else { part })
            .collect::<Vec<_>>()
            .join(".");
        let cpp_name = format!("{cpp_package}.{}", self.descriptor.name());
        if (model.full_name() != canonical_name && model.full_name() != cpp_name)
            || model.fields().len() != self.fields.len()
            || model.parent_file().syntax() != self.descriptor.parent_file().syntax()
        {
            return Err(Error::invalid(
                "compiled model identity or syntax differs from contract",
            ));
        }
        for expected in &self.fields {
            let actual = model
                .get_field(expected.number())
                .ok_or_else(|| Error::invalid("compiled model is missing a field"))?;
            if actual.name() != expected.name()
                || actual.json_name() != expected.json_name()
                || actual.kind() != expected.kind()
                || actual.cardinality() != expected.cardinality()
                || actual.supports_presence() != expected.supports_presence()
                || actual.containing_oneof().is_some()
                || actual.field_descriptor_proto().default_value != expected.field_descriptor_proto().default_value
            {
                return Err(Error::invalid(format!(
                    "compiled model field {} differs from contract",
                    expected.name()
                )));
            }
        }
        Ok(())
    }

    pub fn decode(&self, bytes: &[u8]) -> Result<DynamicMessage> {
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(Error::invalid("encoded event exceeds 16 MiB"));
        }
        validate_wire(bytes, &self.descriptor)?;
        let message = DynamicMessage::decode(self.descriptor.clone(), bytes).map_err(Error::invalid)?;
        self.validate_message(&message)?;
        Ok(message)
    }

    fn validate_message(&self, message: &DynamicMessage) -> Result<()> {
        self.validate_model(&message.descriptor())?;
        if message.unknown_fields().next().is_some() {
            return Err(Error::invalid("unknown protobuf field"));
        }
        for (field, column) in self.fields.iter().zip(self.schema.fields()) {
            let actual = message
                .descriptor()
                .get_field(field.number())
                .ok_or_else(|| Error::invalid("missing model field"))?;
            if !message.has_field(&actual) {
                if !column.is_nullable() {
                    return Err(Error::invalid(format!(
                        "required column {} was not supplied",
                        column.name()
                    )));
                }
            } else {
                validate_value(message.get_field(&actual).as_ref(), column.data_type())?;
            }
        }
        Ok(())
    }

    pub fn record_batch(&self, events: &[DynamicMessage]) -> Result<RecordBatch> {
        for event in events {
            self.validate_message(event)?;
        }
        let mut columns = Vec::with_capacity(self.fields.len());
        for (field, column) in self.fields.iter().zip(self.schema.fields()) {
            let values = events
                .iter()
                .map(|event| {
                    let actual = event
                        .descriptor()
                        .get_field(field.number())
                        .ok_or_else(|| Error::invalid("missing model field"))?;
                    Ok(event.has_field(&actual).then(|| event.get_field(&actual).into_owned()))
                })
                .collect::<Result<Vec<_>>>()?;
            columns.push(array(&values, column.data_type())?);
        }
        RecordBatch::try_new(self.schema(), columns).map_err(Error::invalid)
    }

    fn validate_annotations(manifest: &serde_json::Value, descriptor: &MessageDescriptor) -> Result<()> {
        let Some(metadata) = manifest["schema"].get("metadata") else {
            return Ok(());
        };
        let pool = descriptor.parent_pool();
        let extension = pool
            .get_extension_by_name("arrowcol.metadata.v1.table")
            .ok_or_else(|| Error::invalid("missing table annotation descriptor"))?;
        let options = descriptor.parent_file().options();
        if !options.has_extension(&extension) {
            return Err(Error::invalid("missing table annotations"));
        }
        let value = options.get_extension(&extension);
        let table = value
            .as_message()
            .ok_or_else(|| Error::invalid("invalid table annotation"))?;
        for key in ["provider", "location", "comment"] {
            let expected = metadata.get(key).unwrap_or(&serde_json::Value::Null);
            let actual = table
                .get_field_by_name(key)
                .ok_or_else(|| Error::invalid("missing table annotation field"))?;
            if expected.is_null() {
                if table.has_field_by_name(key) {
                    return Err(Error::invalid("table annotations differ from manifest"));
                }
            } else if actual.as_str() != expected.as_str() {
                return Err(Error::invalid("table annotations differ from manifest"));
            }
        }
        let partition_value = table
            .get_field_by_name("partition_columns")
            .ok_or_else(|| Error::invalid("missing partition annotation"))?;
        let partitions = partition_value
            .as_list()
            .ok_or_else(|| Error::invalid("invalid partitions"))?;
        let expected_partitions = metadata["partition_columns"]
            .as_array()
            .ok_or_else(|| Error::invalid("invalid manifest partitions"))?;
        if partitions.len() != expected_partitions.len()
            || partitions
                .iter()
                .zip(expected_partitions)
                .any(|(actual, expected)| actual.as_str() != expected.as_str())
        {
            return Err(Error::invalid("partition annotations differ from manifest"));
        }
        let properties_value = table
            .get_field_by_name("properties")
            .ok_or_else(|| Error::invalid("missing property annotations"))?;
        let properties = properties_value
            .as_list()
            .ok_or_else(|| Error::invalid("invalid property annotations"))?;
        let expected = metadata["properties"]
            .as_object()
            .ok_or_else(|| Error::invalid("invalid manifest properties"))?;
        let mut keys = HashSet::new();
        for property in properties {
            let property = property
                .as_message()
                .ok_or_else(|| Error::invalid("invalid property annotation"))?;
            let key = property
                .get_field_by_name("key")
                .ok_or_else(|| Error::invalid("missing property key"))?;
            let key = key.as_str().ok_or_else(|| Error::invalid("invalid property key"))?;
            let value = property
                .get_field_by_name("value")
                .ok_or_else(|| Error::invalid("missing property value"))?;
            if !keys.insert(key.to_owned()) || expected.get(key).and_then(serde_json::Value::as_str) != value.as_str() {
                return Err(Error::invalid("property annotations differ from manifest"));
            }
        }
        if keys.len() != expected.len() {
            return Err(Error::invalid("property annotations differ from manifest"));
        }
        Ok(())
    }

    pub(crate) fn routing(&self) -> Routing {
        let mut descriptor = self.identity.to_vec();
        descriptor.push(format!("v{}", self.version));
        Routing {
            schema: self.schema(),
            descriptor,
            metadata: vec![
                ("x-arrowcol-protocol-version".into(), "1".into()),
                ("x-arrowcol-implementation-pool".into(), "dataflow".into()),
                ("x-arrowcol-catalog".into(), self.identity[0].clone()),
                ("x-arrowcol-schema".into(), self.identity[1].clone()),
                ("x-arrowcol-table".into(), self.identity[2].clone()),
                ("x-arrowcol-schema-fingerprint".into(), self.fingerprint.clone()),
            ],
            fingerprint: self.fingerprint.clone(),
            version: self.version,
        }
    }
}

fn uuid_shape(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}

fn identifier(value: &str, pascal: bool) -> String {
    let words: Vec<_> = value
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    let words = if words.is_empty() { vec!["field".into()] } else { words };
    let mut result = if pascal {
        words
            .iter()
            .map(|word| format!("{}{}", word[..1].to_ascii_uppercase(), &word[1..]))
            .collect::<String>()
    } else {
        words.join("_")
    };
    if result.starts_with(|ch: char| ch.is_ascii_digit()) {
        result.insert_str(0, if pascal { "F" } else { "f_" });
    }
    result
}

fn mapping_type(value: &serde_json::Value) -> Result<DataType> {
    use DataType::*;
    let integer = |key| {
        value[key]
            .as_i64()
            .ok_or_else(|| Error::invalid("missing mapping integer"))
    };
    let unit = || match value["unit"].as_str() {
        Some("second") => Ok(TimeUnit::Second),
        Some("millisecond") => Ok(TimeUnit::Millisecond),
        Some("microsecond") => Ok(TimeUnit::Microsecond),
        Some("nanosecond") => Ok(TimeUnit::Nanosecond),
        _ => Err(Error::invalid("invalid temporal unit")),
    };
    Ok(match value["kind"].as_str() {
        Some("boolean") => Boolean,
        Some("int8") => Int8,
        Some("int16") => Int16,
        Some("int32") => Int32,
        Some("int64") => Int64,
        Some("u_int8") => UInt8,
        Some("u_int16") => UInt16,
        Some("u_int32") => UInt32,
        Some("u_int64") => UInt64,
        Some("float32") => Float32,
        Some("float64") => Float64,
        Some("utf8") => Utf8,
        Some("large_utf8") => LargeUtf8,
        Some("utf8_view") => Utf8View,
        Some("binary") => Binary,
        Some("large_binary") => LargeBinary,
        Some("binary_view") => BinaryView,
        Some("fixed_size_binary") => {
            let width = i32::try_from(integer("bytes")?).map_err(Error::invalid)?;
            if width <= 0 {
                return Err(Error::invalid("fixed binary width must be positive"));
            }
            FixedSizeBinary(width)
        }
        Some("decimal128") => {
            let precision = u8::try_from(integer("precision")?).map_err(Error::invalid)?;
            let scale = i8::try_from(integer("scale")?).map_err(Error::invalid)?;
            if precision == 0 || precision > 38 || scale.unsigned_abs() > precision {
                return Err(Error::invalid("invalid decimal precision/scale"));
            }
            Decimal128(precision, scale)
        }
        Some("date32") => Date32,
        Some("date64") => Date64,
        Some("timestamp") => Timestamp(
            unit()?,
            match &value["timezone"] {
                serde_json::Value::Null => None,
                serde_json::Value::String(zone) => Some(zone.clone().into()),
                _ => return Err(Error::invalid("invalid timezone")),
            },
        ),
        Some("time32") => {
            let unit = unit()?;
            if !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) {
                return Err(Error::invalid("invalid time32 unit"));
            }
            Time32(unit)
        }
        Some("time64") => {
            let unit = unit()?;
            if !matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond) {
                return Err(Error::invalid("invalid time64 unit"));
            }
            Time64(unit)
        }
        Some("duration") => Duration(unit()?),
        _ => {
            return Err(Error::invalid(
                "unsupported catalog type; nested/list/map/dictionary are not supported",
            ));
        }
    })
}

fn protobuf_kind(ty: &DataType) -> Result<Kind> {
    use DataType::*;
    Ok(match ty {
        Boolean => Kind::Bool,
        Int8 | Int16 | Int32 | Date32 | Time32(_) => Kind::Sint32,
        Int64 | Date64 | Timestamp(..) | Time64(_) | Duration(_) => Kind::Int64,
        UInt8 | UInt16 | UInt32 => Kind::Uint32,
        UInt64 => Kind::Uint64,
        Float32 => Kind::Float,
        Float64 => Kind::Double,
        Utf8 | LargeUtf8 | Utf8View | Decimal128(..) => Kind::String,
        Binary | LargeBinary | BinaryView | FixedSizeBinary(_) => Kind::Bytes,
        _ => return Err(Error::invalid("unsupported Arrow type")),
    })
}
fn kind_name(kind: &Kind) -> Result<&'static str> {
    Ok(match kind {
        Kind::Bool => "bool",
        // The catalogue's logical "int32" mapping uses zigzag sint32 on the wire.
        Kind::Sint32 => "int32",
        Kind::Int64 => "int64",
        Kind::Uint32 => "uint32",
        Kind::Uint64 => "uint64",
        Kind::Float => "float",
        Kind::Double => "double",
        Kind::String => "string",
        Kind::Bytes => "bytes",
        _ => return Err(Error::invalid("unsupported protobuf kind")),
    })
}

fn validate_value(value: &Value, ty: &DataType) -> Result<()> {
    use DataType::*;
    let valid = match (ty, value) {
        (Boolean, Value::Bool(_))
        | (Int32 | Date32, Value::I32(_))
        | (Int64 | Timestamp(..) | Duration(_), Value::I64(_))
        | (UInt32, Value::U32(_))
        | (UInt64, Value::U64(_))
        | (Float32, Value::F32(_))
        | (Float64, Value::F64(_))
        | (Utf8 | LargeUtf8 | Utf8View, Value::String(_))
        | (Binary | LargeBinary | BinaryView, Value::Bytes(_)) => true,
        (Int8, Value::I32(v)) => i8::try_from(*v).is_ok(),
        (Int16, Value::I32(v)) => i16::try_from(*v).is_ok(),
        (UInt8, Value::U32(v)) => u8::try_from(*v).is_ok(),
        (UInt16, Value::U32(v)) => u16::try_from(*v).is_ok(),
        (Date64, Value::I64(v)) => v % 86_400_000 == 0,
        (Time32(unit), Value::I32(v)) => (0..day_units(*unit)).contains(&i64::from(*v)),
        (Time64(unit), Value::I64(v)) => (0..day_units(*unit)).contains(v),
        (FixedSizeBinary(width), Value::Bytes(v)) => v.len() == *width as usize,
        (Decimal128(precision, scale), Value::String(v)) => {
            decimal128(v, *precision, *scale)?;
            true
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(Error::invalid(format!("value does not fit {ty}")))
    }
}
fn day_units(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 86_400,
        TimeUnit::Millisecond => 86_400_000,
        TimeUnit::Microsecond => 86_400_000_000,
        TimeUnit::Nanosecond => 86_400_000_000_000,
    }
}

fn array(values: &[Option<Value>], ty: &DataType) -> Result<ArrayRef> {
    macro_rules! primitive {
        ($array:ident, $variant:ident) => {{
            let values = values
                .iter()
                .map(|value| match value {
                    Some(Value::$variant(v)) => Ok(Some(*v)),
                    None => Ok(None),
                    _ => Err(Error::invalid("protobuf value kind mismatch")),
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new($array::from(values).with_data_type(ty.clone())) as ArrayRef
        }};
        ($array:ident, $variant:ident, $native:ty) => {{
            let values = values
                .iter()
                .map(|value| match value {
                    Some(Value::$variant(v)) => <$native>::try_from(*v).map(Some).map_err(Error::invalid),
                    None => Ok(None),
                    _ => Err(Error::invalid("protobuf value kind mismatch")),
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new($array::from(values)) as ArrayRef
        }};
    }
    macro_rules! variable {
        ($array:ident, $variant:ident) => {{
            let values = values
                .iter()
                .map(|value| match value {
                    Some(Value::$variant(v)) => Ok(Some(v.as_ref())),
                    None => Ok(None),
                    _ => Err(Error::invalid("protobuf value kind mismatch")),
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new($array::from(values)) as ArrayRef
        }};
    }
    use DataType::*;
    Ok(match ty {
        Boolean => Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|v| v.as_ref().and_then(Value::as_bool))
                .collect::<Vec<_>>(),
        )),
        Int8 => primitive!(Int8Array, I32, i8),
        Int16 => primitive!(Int16Array, I32, i16),
        Int32 => primitive!(Int32Array, I32),
        Date32 => primitive!(Date32Array, I32),
        Time32(TimeUnit::Second) => primitive!(Time32SecondArray, I32),
        Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondArray, I32),
        Int64 => primitive!(Int64Array, I64),
        Date64 => primitive!(Date64Array, I64),
        Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondArray, I64),
        Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondArray, I64),
        Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondArray, I64),
        Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondArray, I64),
        Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondArray, I64),
        Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondArray, I64),
        Duration(TimeUnit::Second) => primitive!(DurationSecondArray, I64),
        Duration(TimeUnit::Millisecond) => primitive!(DurationMillisecondArray, I64),
        Duration(TimeUnit::Microsecond) => primitive!(DurationMicrosecondArray, I64),
        Duration(TimeUnit::Nanosecond) => primitive!(DurationNanosecondArray, I64),
        UInt8 => primitive!(UInt8Array, U32, u8),
        UInt16 => primitive!(UInt16Array, U32, u16),
        UInt32 => primitive!(UInt32Array, U32),
        UInt64 => primitive!(UInt64Array, U64),
        Float32 => primitive!(Float32Array, F32),
        Float64 => primitive!(Float64Array, F64),
        Utf8 => variable!(StringArray, String),
        LargeUtf8 => variable!(LargeStringArray, String),
        Utf8View => variable!(StringViewArray, String),
        Binary => variable!(BinaryArray, Bytes),
        LargeBinary => variable!(LargeBinaryArray, Bytes),
        BinaryView => variable!(BinaryViewArray, Bytes),
        FixedSizeBinary(width) => {
            let mut builder = FixedSizeBinaryBuilder::new(*width);
            for value in values {
                match value {
                    None => builder.append_null(),
                    Some(Value::Bytes(v)) => builder.append_value(v).map_err(Error::invalid)?,
                    _ => return Err(Error::invalid("expected bytes")),
                }
            }
            Arc::new(builder.finish())
        }
        Decimal128(precision, scale) => {
            let values = values
                .iter()
                .map(|value| match value {
                    None => Ok(None),
                    Some(Value::String(v)) => decimal128(v, *precision, *scale).map(Some),
                    _ => Err(Error::invalid("expected decimal text")),
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(Error::invalid)?,
            )
        }
        _ => return Err(Error::invalid("unsupported Arrow type")),
    })
}

// prost's scalar decoder intentionally accepts over-wide varints by truncating. Check
// the original wire first, including unknown/duplicate fields, before decoding values.
fn validate_wire(mut bytes: &[u8], descriptor: &MessageDescriptor) -> Result<()> {
    let mut seen = HashSet::new();
    while !bytes.is_empty() {
        let key = varint(&mut bytes)?;
        let number = u32::try_from(key >> 3).map_err(Error::invalid)?;
        let field = descriptor
            .get_field(number)
            .ok_or_else(|| Error::invalid("unknown protobuf field"))?;
        if !seen.insert(number) {
            return Err(Error::invalid("duplicate protobuf field"));
        }
        let wire = key & 7;
        match field.kind() {
            Kind::Bool | Kind::Sint32 | Kind::Int64 | Kind::Uint32 | Kind::Uint64 => {
                if wire != 0 {
                    return Err(Error::invalid("incorrect protobuf wire type"));
                }
                let value = varint(&mut bytes)?;
                let valid = match field.kind() {
                    Kind::Bool => value <= 1,
                    Kind::Sint32 | Kind::Uint32 => u32::try_from(value).is_ok(),
                    _ => true,
                };
                if !valid {
                    return Err(Error::invalid("protobuf integer is out of range"));
                }
            }
            Kind::Float | Kind::Double => {
                let (expected, size) = if field.kind() == Kind::Float { (5, 4) } else { (1, 8) };
                if wire != expected {
                    return Err(Error::invalid("incorrect protobuf wire type"));
                }
                take(&mut bytes, size)?;
            }
            Kind::String | Kind::Bytes => {
                if wire != 2 {
                    return Err(Error::invalid("incorrect protobuf wire type"));
                }
                let size = usize::try_from(varint(&mut bytes)?).map_err(Error::invalid)?;
                take(&mut bytes, size)?;
            }
            _ => return Err(Error::invalid("unsupported protobuf field")),
        }
    }
    Ok(())
}
fn take(bytes: &mut &[u8], size: usize) -> Result<()> {
    *bytes = bytes
        .get(size..)
        .ok_or_else(|| Error::invalid("truncated protobuf value"))?;
    Ok(())
}
fn varint(bytes: &mut &[u8]) -> Result<u64> {
    let mut result = 0;
    for shift in (0..70).step_by(7) {
        let byte = *bytes
            .first()
            .ok_or_else(|| Error::invalid("truncated protobuf varint"))?;
        *bytes = &bytes[1..];
        if shift == 63 && byte > 1 {
            return Err(Error::invalid("overflowing protobuf varint"));
        }
        result |= u64::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return Ok(result);
        }
    }
    Err(Error::invalid("invalid protobuf varint"))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
    use arrow_schema::{Field, Schema};
    use prost::Message;
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto::{Label, Type},
    };
    use serde_json::json;

    fn artifacts(columns: Vec<(DataType, serde_json::Value)>) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let fields = columns
            .iter()
            .enumerate()
            .map(|(index, (ty, _))| Field::new(format!("f{index}"), ty.clone(), index != 0))
            .collect::<Vec<_>>();
        let schema = Schema::new(fields);
        let manifest = json!({
            "artifact_manifest_version": 1,
            "schema": {
                "table_id": "9b1e9392-b345-5573-b368-f0d7b52fb93e",
                "identity": {"catalog":"demo", "schema":"public", "table":"events"},
                "version":1, "fingerprint":fingerprint(&schema).expect("fingerprint"),
                "fields":columns.iter().enumerate().map(|(index, (ty, mapping))| json!({
                    "id":format!("00000000-0000-0000-0000-{index:012}"), "name":format!("f{index}"),
                    "position":index, "field_number":index + 1, "nullable":index != 0,
                    "mapping":{"arrow":mapping, "protobuf":kind_name(&protobuf_kind(ty).expect("kind")).expect("name")}
                })).collect::<Vec<_>>()
            }
        });
        let fields = columns
            .iter()
            .enumerate()
            .map(|(index, (ty, _))| FieldDescriptorProto {
                name: Some(format!("f{index}")),
                json_name: Some(format!("f{index}")),
                number: Some(index as i32 + 1),
                label: Some(Label::Optional as i32),
                r#type: Some(match protobuf_kind(ty).expect("kind") {
                    Kind::Bool => Type::Bool,
                    Kind::Sint32 => Type::Sint32,
                    Kind::Int64 => Type::Int64,
                    Kind::Uint32 => Type::Uint32,
                    Kind::Uint64 => Type::Uint64,
                    Kind::Float => Type::Float,
                    Kind::Double => Type::Double,
                    Kind::String => Type::String,
                    Kind::Bytes => Type::Bytes,
                    _ => panic!("unexpected type"),
                } as i32),
                ..Default::default()
            })
            .collect();
        let descriptors = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("schema.proto".into()),
                package: Some("arrowcol.demo.public".into()),
                syntax: Some("proto2".into()),
                message_type: vec![DescriptorProto {
                    name: Some("Events".into()),
                    field: fields,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec();
        let arrow = IpcDataGenerator::default()
            .schema_to_bytes_with_dictionary_tracker(
                &schema,
                &mut DictionaryTracker::new(false),
                &IpcWriteOptions::default(),
            )
            .ipc_message;
        (serde_json::to_vec(&manifest).expect("manifest"), arrow, descriptors)
    }

    fn load(artifacts: &(Vec<u8>, Vec<u8>, Vec<u8>)) -> ArrowcolContract {
        ArrowcolContract::from_artifacts(&artifacts.0, &artifacts.1, &artifacts.2).expect("contract")
    }
    fn event(contract: &ArrowcolContract, values: Vec<Option<Value>>) -> DynamicMessage {
        let mut event = DynamicMessage::new(contract.descriptor());
        for (field, value) in contract.fields.iter().zip(values) {
            if let Some(value) = value {
                event.set_field(field, value);
            }
        }
        event
    }

    #[test]
    fn canonical_scalar_fingerprint_matches_catalog() {
        let schema = Schema::new(vec![
            Field::new("event_id", DataType::Int64, false),
            Field::new(
                "observed_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("payload", DataType::Utf8, true),
        ]);
        assert_eq!(
            fingerprint(&schema).expect("fingerprint"),
            "sha256:75c465746aa29c29b38b9edf9d639fd3f160cb81bda38e2cf20dd1f458498e6c"
        );
    }

    #[test]
    fn rejects_inconsistent_artifacts_and_compiled_models() {
        let original = artifacts(vec![(DataType::Int64, json!({"kind":"int64"}))]);
        let other = artifacts(vec![(DataType::Utf8, json!({"kind":"utf8"}))]);
        assert!(ArrowcolContract::from_artifacts(&original.0, &other.1, &original.2).is_err());
        assert!(ArrowcolContract::from_artifacts(&original.0, &original.1, &other.2).is_err());
        let mut manifest: serde_json::Value = serde_json::from_slice(&original.0).expect("json");
        manifest["schema"]["fields"][0]["nullable"] = json!(true);
        assert!(
            ArrowcolContract::from_artifacts(&serde_json::to_vec(&manifest).expect("json"), &original.1, &original.2)
                .is_err()
        );
        manifest["schema"]["fields"][0]["nullable"] = json!(false);
        manifest["schema"]["fields"][0]["mapping"]["arrow"] = json!({"kind":"dictionary"});
        assert!(
            ArrowcolContract::from_artifacts(&serde_json::to_vec(&manifest).expect("json"), &original.1, &original.2)
                .is_err()
        );
        let contract = load(&original);
        assert!(contract.validate_model(&load(&other).descriptor()).is_err());
        for (package, valid) in [("arrowcol.demo.public_", true), ("arrowcol.other.public", false)] {
            let mut descriptors = FileDescriptorSet::decode(original.2.as_slice()).expect("descriptors");
            descriptors.file[0].package = Some(package.into());
            let pool = DescriptorPool::decode(descriptors.encode_to_vec().as_slice()).expect("pool");
            let model = pool.get_message_by_name(&format!("{package}.Events")).expect("model");
            assert_eq!(contract.validate_model(&model).is_ok(), valid);
        }
        for change in 0..4 {
            let mut descriptors = FileDescriptorSet::decode(original.2.as_slice()).expect("descriptors");
            let field = &mut descriptors.file[0].message_type[0].field[0];
            match change {
                0 => field.number = Some(2),
                1 => field.r#type = Some(Type::Sint64 as i32),
                2 => field.default_value = Some("12".into()),
                _ => descriptors.file[0].syntax = Some("proto3".into()),
            }
            let pool = DescriptorPool::decode(descriptors.encode_to_vec().as_slice()).expect("pool");
            assert!(
                contract
                    .validate_model(&pool.get_message_by_name("arrowcol.demo.public.Events").expect("model"))
                    .is_err()
            );
        }
    }

    #[test]
    fn validates_null_ranges_decimal_width_and_wire_before_conversion() {
        let fixture = artifacts(vec![
            (DataType::Int8, json!({"kind":"int8"})),
            (
                DataType::Decimal128(5, 2),
                json!({"kind":"decimal128","precision":5,"scale":2}),
            ),
            (
                DataType::FixedSizeBinary(2),
                json!({"kind":"fixed_size_binary","bytes":2}),
            ),
            (DataType::UInt8, json!({"kind":"u_int8"})),
        ]);
        let contract = load(&fixture);
        let valid = event(
            &contract,
            vec![
                Some(Value::I32(-128)),
                Some(Value::String("-123.45".into())),
                Some(Value::Bytes(vec![1, 2].into())),
                Some(Value::U32(255)),
            ],
        );
        let batch = contract.record_batch(std::slice::from_ref(&valid)).expect("batch");
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("decimal")
                .value(0),
            -12345
        );
        assert!(contract.decode(&valid.encode_to_vec()).is_ok());
        let optional_null = event(&contract, vec![Some(Value::I32(0))]);
        assert_eq!(
            contract
                .record_batch(&[optional_null])
                .expect("nulls")
                .column(1)
                .null_count(),
            1
        );
        let mut invalid = valid.clone();
        invalid.clear_field(&contract.fields[0]);
        assert!(contract.record_batch(&[invalid]).is_err());
        for (index, value) in [
            (0, Value::I32(128)),
            (0, Value::I32(-129)),
            (1, Value::String("1000.00".into())),
            (1, Value::String("1.234".into())),
            (1, Value::String("NaN".into())),
            (2, Value::Bytes(vec![1].into())),
            (3, Value::U32(256)),
        ] {
            let mut invalid = valid.clone();
            invalid.set_field(&contract.fields[index], value);
            assert!(contract.record_batch(&[invalid]).is_err());
        }
        for bytes in [
            &[0x08, 0x80][..],
            &[0x08, 1, 0x08, 2],
            &[0x28, 1],
            &[0x0a, 0],
            &[0x08, 0xff, 0xff, 0xff, 0xff, 0x0f],
        ] {
            assert!(contract.decode(bytes).is_err(), "{bytes:?}");
        }
        assert_ne!(contract.bundle_digest(), contract.fingerprint());
    }

    #[test]
    fn all_scalar_representations_and_temporal_units_build_exact_arrays() {
        let mut columns = vec![
            (DataType::Boolean, json!({"kind":"boolean"}), Value::Bool(false)),
            (DataType::Int16, json!({"kind":"int16"}), Value::I32(-123)),
            (DataType::Int32, json!({"kind":"int32"}), Value::I32(i32::MIN)),
            (DataType::Int64, json!({"kind":"int64"}), Value::I64(i64::MIN)),
            (DataType::UInt16, json!({"kind":"u_int16"}), Value::U32(65535)),
            (DataType::UInt32, json!({"kind":"u_int32"}), Value::U32(u32::MAX)),
            (DataType::UInt64, json!({"kind":"u_int64"}), Value::U64(u64::MAX)),
            (DataType::Float32, json!({"kind":"float32"}), Value::F32(1.5)),
            (DataType::Float64, json!({"kind":"float64"}), Value::F64(-2.5)),
            (DataType::Date32, json!({"kind":"date32"}), Value::I32(-1)),
            (DataType::Date64, json!({"kind":"date64"}), Value::I64(-86_400_000)),
        ];
        for (ty, kind) in [
            (DataType::Utf8, "utf8"),
            (DataType::LargeUtf8, "large_utf8"),
            (DataType::Utf8View, "utf8_view"),
        ] {
            columns.push((ty, json!({"kind":kind}), Value::String("hello".into())));
        }
        for (ty, kind) in [
            (DataType::Binary, "binary"),
            (DataType::LargeBinary, "large_binary"),
            (DataType::BinaryView, "binary_view"),
        ] {
            columns.push((ty, json!({"kind":kind}), Value::Bytes(vec![0, 255].into())));
        }
        for (unit, name) in [
            (TimeUnit::Second, "second"),
            (TimeUnit::Millisecond, "millisecond"),
            (TimeUnit::Microsecond, "microsecond"),
            (TimeUnit::Nanosecond, "nanosecond"),
        ] {
            columns.push((
                DataType::Timestamp(unit, Some("UTC".into())),
                json!({"kind":"timestamp","unit":name,"timezone":"UTC"}),
                Value::I64(-1),
            ));
            columns.push((
                DataType::Duration(unit),
                json!({"kind":"duration","unit":name}),
                Value::I64(-2),
            ));
            if matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) {
                columns.push((
                    DataType::Time32(unit),
                    json!({"kind":"time32","unit":name}),
                    Value::I32(123),
                ));
            } else {
                columns.push((
                    DataType::Time64(unit),
                    json!({"kind":"time64","unit":name}),
                    Value::I64(123),
                ));
            }
        }
        let contract = load(&artifacts(
            columns
                .iter()
                .map(|(ty, mapping, _)| (ty.clone(), mapping.clone()))
                .collect(),
        ));
        let message = event(
            &contract,
            columns.iter().map(|(_, _, value)| Some(value.clone())).collect(),
        );
        let batch = contract.record_batch(std::slice::from_ref(&message)).expect("batch");
        assert_eq!(batch.schema(), contract.schema());
        assert_eq!(batch.num_rows(), 1);
        assert!(contract.decode(&message.encode_to_vec()).is_ok());
        assert!(validate_value(&Value::I32(86_400), &DataType::Time32(TimeUnit::Second)).is_err());
        assert!(validate_value(&Value::I64(-1), &DataType::Time64(TimeUnit::Nanosecond)).is_err());
        assert!(validate_value(&Value::I64(1), &DataType::Date64).is_err());
    }
}
