use std::collections::{BTreeMap, BTreeSet};

use arrow_ipc::{
    convert::try_schema_from_flatbuffer_bytes,
    writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
};
use prost::Message;
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    field_descriptor_proto::{Label, Type},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    CatalogError, CatalogErrorCode, CatalogField, CatalogResult, CatalogSchemaVersion, CatalogType, ProtobufType,
    TableMetadata,
};

const ARTIFACT_MANIFEST_VERSION: u32 = 1;
const MAX_PROTOBUF_FIELD_NUMBER: u32 = 536_870_911;

// Compiled by build.rs from the authoritative, versioned proto2 vocabulary.
// Keep the raw wire bytes: prost-types discards custom option extension fields.
pub(crate) const OPTION_DESCRIPTORS: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/options.pb"));
const OPTION_SOURCE: &str = include_str!("../proto/options.proto");
const DESCRIPTOR_SOURCE: &str = include_str!("../proto/google/protobuf/descriptor.proto");

#[derive(Clone, PartialEq, Message)]
struct RawDescriptorSet {
    #[prost(bytes = "vec", repeated, tag = "1")]
    file: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct FileOptionCarrier {
    #[prost(message, optional, tag = "8")]
    options: Option<TableOptions>,
}

#[derive(Clone, PartialEq, Message)]
struct RawFileMessages {
    #[prost(bytes = "vec", repeated, tag = "4")]
    message_type: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct MessageOptionCarrier {
    #[prost(bytes = "vec", optional, tag = "7")]
    options: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct TableOptions {
    #[prost(message, optional, tag = "50000")]
    table: Option<PortableTableMetadata>,
}

#[derive(Clone, PartialEq, Message)]
struct PortableTableMetadata {
    #[prost(string, optional, tag = "1")]
    provider: Option<String>,
    #[prost(string, optional, tag = "2")]
    location: Option<String>,
    #[prost(string, optional, tag = "3")]
    comment: Option<String>,
    #[prost(string, repeated, tag = "4")]
    partition_columns: Vec<String>,
    #[prost(message, repeated, tag = "5")]
    properties: Vec<PortableTableProperty>,
}

#[derive(Clone, PartialEq, Message)]
struct PortableTableProperty {
    #[prost(string, optional, tag = "1")]
    key: Option<String>,
    #[prost(string, optional, tag = "2")]
    value: Option<String>,
}

fn descriptor_error(error: impl std::fmt::Display) -> CatalogError {
    CatalogError::new(CatalogErrorCode::DescriptorGeneration, error.to_string())
}

/// Recovers table metadata solely from the custom option in an exported descriptor.
///
/// This decodes annotations, not runtime policy. Reserved-property validation
/// remains the responsibility of the central property registry at admission.
///
/// # Errors
///
/// Rejects malformed descriptors, missing or ambiguous table options, and
/// incomplete or duplicate property entries.
pub fn protobuf_table_metadata(bytes: &[u8]) -> CatalogResult<TableMetadata> {
    let descriptors = RawDescriptorSet::decode(bytes).map_err(descriptor_error)?;
    let mut metadata = None;
    for file in descriptors.file {
        let carrier = FileOptionCarrier::decode(file.as_slice()).map_err(descriptor_error)?;
        if let Some(table) = carrier.options.and_then(|options| options.table) {
            if metadata.is_some() {
                return Err(descriptor_error(
                    "descriptor contains more than one table metadata option",
                ));
            }
            let mut properties = BTreeMap::new();
            for property in table.properties {
                let key = property
                    .key
                    .ok_or_else(|| descriptor_error("table property has no key"))?;
                let value = property
                    .value
                    .ok_or_else(|| descriptor_error("table property has no value"))?;
                if properties.insert(key, value).is_some() {
                    return Err(descriptor_error("duplicate table property key"));
                }
            }
            metadata = Some(TableMetadata {
                provider: table
                    .provider
                    .ok_or_else(|| descriptor_error("table metadata has no provider"))?,
                location: table.location,
                comment: table.comment,
                partition_columns: table.partition_columns,
                properties,
            });
        }
    }
    metadata.ok_or_else(|| descriptor_error("descriptor has no table metadata option"))
}

// Protobuf strings are bytes with C escapes, not JSON strings: JSON's \u0000
// escape is not supported by every protoc/Buf parser. Fixed-width octal also
// prevents adjacent digits from becoming part of an escape.
pub(crate) fn protobuf_string(value: &str) -> String {
    let mut output = String::from("\"");
    for byte in value.bytes() {
        match byte {
            b'"' => output.push_str("\\\""),
            b'\\' => output.push_str("\\\\"),
            0x20..=0x7e => output.push(char::from(byte)),
            _ => output.push_str(&format!("\\{byte:03o}")),
        }
    }
    output.push('"');
    output
}

fn metadata_source(metadata: &TableMetadata) -> String {
    let mut source = format!(
        "option (arrowcol.metadata.v1.table) = {{\n  provider: {}\n",
        protobuf_string(&metadata.provider)
    );
    if let Some(location) = &metadata.location {
        source.push_str(&format!("  location: {}\n", protobuf_string(location)));
    }
    if let Some(comment) = &metadata.comment {
        source.push_str(&format!("  comment: {}\n", protobuf_string(comment)));
    }
    for column in &metadata.partition_columns {
        source.push_str(&format!("  partition_columns: {}\n", protobuf_string(column)));
    }
    for (key, value) in &metadata.properties {
        source.push_str(&format!(
            "  properties: {{ key: {} value: {} }}\n",
            protobuf_string(key),
            protobuf_string(value)
        ));
    }
    source.push_str("};\n\n");
    source
}

/// Persisted deterministic descriptor artifact kind.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    /// Arrow IPC flatbuffer schema message.
    ArrowSchema,
    /// Encoded protobuf `FileDescriptorSet`.
    ProtobufDescriptor,
    /// Canonical JSON manifest retaining Arrow-specific logical details.
    Manifest,
}

impl ArtifactKind {
    /// Stable SQL storage name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ArrowSchema => "arrow_schema",
            Self::ProtobufDescriptor => "protobuf_descriptor",
            Self::Manifest => "manifest",
        }
    }
}

/// Deterministically serialized SDK-generation artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptorArtifacts {
    arrow_schema: Vec<u8>,
    protobuf_descriptor: Vec<u8>,
    manifest: Vec<u8>,
}

impl DescriptorArtifacts {
    /// Returns the Arrow IPC flatbuffer schema message.
    #[must_use]
    pub fn arrow_schema(&self) -> &[u8] {
        &self.arrow_schema
    }

    /// Returns the encoded protobuf descriptor set.
    #[must_use]
    pub fn protobuf_descriptor(&self) -> &[u8] {
        &self.protobuf_descriptor
    }

    /// Returns deterministic proto2 source reconstructed from the persisted descriptor.
    ///
    /// # Errors
    ///
    /// Returns a descriptor error if persisted protobuf bytes are malformed.
    pub fn protobuf_source(&self) -> CatalogResult<String> {
        let descriptors = FileDescriptorSet::decode(self.protobuf_descriptor()).map_err(|error| {
            CatalogError::new(
                CatalogErrorCode::DescriptorGeneration,
                format!("protobuf descriptor did not decode: {error}"),
            )
        })?;
        let mut source = String::from(
            "syntax = \"proto2\";\n\n// TIMESTAMP: UTC microseconds; DECIMAL: base-10 string. Arrow Flight remains the wire.\n",
        );
        let raw = RawDescriptorSet::decode(self.protobuf_descriptor()).map_err(descriptor_error)?;
        let mut found = false;
        for (file, wire_file) in descriptors
            .file
            .into_iter()
            .zip(raw.file)
            .filter(|(file, _)| file.name() == "schema.proto")
        {
            if found {
                return Err(descriptor_error("descriptor contains more than one schema.proto"));
            }
            found = true;
            let raw_messages = RawFileMessages::decode(wire_file.as_slice()).map_err(descriptor_error)?;
            source.push_str(&format!("package {};\n\n", file.package.unwrap_or_default()));
            source.push_str("import \"options.proto\";\n\n");
            source.push_str(&metadata_source(&protobuf_table_metadata(self.protobuf_descriptor())?));
            for (message, wire_message) in file.message_type.into_iter().zip(raw_messages.message_type) {
                source.push_str(&format!("message {} {{\n", message.name.unwrap_or_default()));
                if let Some(options) = MessageOptionCarrier::decode(wire_message.as_slice())
                    .map_err(descriptor_error)?
                    .options
                {
                    source.push_str(&crate::options::render_options(&options)?);
                }
                source.push_str("  // Protobuf implementation-reserved numbers 19000-19999 are never allocated.\n");
                for field in message.field {
                    let label = if field.label == Some(Label::Repeated as i32) {
                        "repeated"
                    } else {
                        "optional"
                    };
                    let data_type = if let Some(type_name) = &field.type_name {
                        type_name.clone()
                    } else {
                        field
                            .r#type()
                            .as_str_name()
                            .trim_start_matches("TYPE_")
                            .to_ascii_lowercase()
                    };
                    let json_name = protobuf_string(&field.json_name.unwrap_or_default());
                    source.push_str(&format!(
                        "  {label} {data_type} {} = {} [json_name = {json_name}];\n",
                        field.name.unwrap_or_default(),
                        field.number.unwrap_or_default(),
                    ));
                }
                if !found {
                    return Err(descriptor_error("descriptor is missing schema.proto"));
                }
                source.push_str("}\n\n");
            }
        }
        Ok(source)
    }

    /// Returns every source needed to compile `schema.proto` without the manifest.
    ///
    /// Paths are portable protobuf import paths relative to the export directory.
    /// The standard descriptor source is vendored from protobuf v21.12, retaining
    /// its license; the build compiles these exact sources with imports.
    ///
    /// # Errors
    ///
    /// Returns an error if the persisted schema descriptor cannot be reconstructed.
    pub fn protobuf_sources(&self) -> CatalogResult<BTreeMap<String, String>> {
        Ok(BTreeMap::from([
            ("schema.proto".to_owned(), self.protobuf_source()?),
            ("options.proto".to_owned(), OPTION_SOURCE.to_owned()),
            (
                "google/protobuf/descriptor.proto".to_owned(),
                DESCRIPTOR_SOURCE.to_owned(),
            ),
        ]))
    }

    /// Returns the canonical JSON manifest.
    #[must_use]
    pub fn manifest(&self) -> &[u8] {
        &self.manifest
    }

    /// Returns an artifact by kind.
    #[must_use]
    pub fn get(&self, kind: ArtifactKind) -> &[u8] {
        match kind {
            ArtifactKind::ArrowSchema => self.arrow_schema(),
            ArtifactKind::ProtobufDescriptor => self.protobuf_descriptor(),
            ArtifactKind::Manifest => self.manifest(),
        }
    }

    /// Returns a SHA-256 digest for an artifact.
    #[must_use]
    pub fn sha256(&self, kind: ArtifactKind) -> [u8; 32] {
        Sha256::digest(self.get(kind)).into()
    }

    pub(crate) fn from_parts(arrow_schema: Vec<u8>, protobuf_descriptor: Vec<u8>, manifest: Vec<u8>) -> Self {
        Self {
            arrow_schema,
            protobuf_descriptor,
            manifest,
        }
    }
}

/// Generates deterministic Arrow, protobuf, and manifest artifacts.
///
/// # Errors
///
/// Returns a structured descriptor error when the schema cannot be represented
/// by protobuf, serialized, or round-tripped through Arrow IPC exactly.
pub fn generate_descriptor_artifacts(schema: &CatalogSchemaVersion) -> CatalogResult<DescriptorArtifacts> {
    // Admission materializes defaults explicitly; generation still rejects
    // misspelled option keys rather than silently exporting unknown vocabulary.
    crate::materialize_protobuf_properties(&schema.metadata().properties)?;
    let arrow_schema = schema.arrow_schema()?;
    let mut dictionary_tracker = DictionaryTracker::new(false);
    let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        &arrow_schema,
        &mut dictionary_tracker,
        &IpcWriteOptions::default(),
    );
    if !encoded.arrow_data.is_empty() {
        return Err(CatalogError::new(
            CatalogErrorCode::DescriptorGeneration,
            "Arrow schema serialization unexpectedly produced a data body",
        ));
    }
    let decoded = try_schema_from_flatbuffer_bytes(&encoded.ipc_message).map_err(|error| {
        CatalogError::new(
            CatalogErrorCode::DescriptorGeneration,
            format!("Arrow schema artifact did not decode: {error}"),
        )
    })?;
    if decoded != arrow_schema {
        return Err(CatalogError::new(
            CatalogErrorCode::DescriptorGeneration,
            "Arrow schema artifact did not round-trip exactly",
        ));
    }

    let protobuf_descriptor = protobuf_descriptor(schema)?;
    let manifest = serde_json::to_vec(&ArtifactManifest {
        artifact_manifest_version: ARTIFACT_MANIFEST_VERSION,
        schema,
    })
    .map_err(|error| {
        CatalogError::new(
            CatalogErrorCode::DescriptorGeneration,
            format!("catalog manifest serialization failed: {error}"),
        )
    })?;

    Ok(DescriptorArtifacts::from_parts(
        encoded.ipc_message,
        protobuf_descriptor,
        manifest,
    ))
}

#[derive(Serialize)]
struct ArtifactManifest<'a> {
    artifact_manifest_version: u32,
    schema: &'a CatalogSchemaVersion,
}

fn protobuf_descriptor(schema: &CatalogSchemaVersion) -> CatalogResult<Vec<u8>> {
    let package = format!(
        "arrowcol.{}.{}",
        protobuf_identifier(schema.identity().catalog().as_str(), IdentifierStyle::Snake),
        protobuf_identifier(schema.identity().schema().as_str(), IdentifierStyle::Snake)
    );
    let root_name = protobuf_identifier(schema.identity().table().as_str(), IdentifierStyle::Pascal);
    let mut messages = Vec::new();
    build_message(&package, &root_name, schema.fields(), &mut messages)?;
    let options = crate::options::encode_options(&schema.metadata().properties)?;
    let messages = messages
        .into_iter()
        .map(|message| {
            let is_root = message.name() == root_name;
            let mut bytes = message.encode_to_vec();
            if is_root && !options.is_empty() {
                bytes.extend(
                    MessageOptionCarrier {
                        options: Some(options.clone()),
                    }
                    .encode_to_vec(),
                );
            }
            bytes
        })
        .collect();
    let mut file = FileDescriptorProto {
        name: Some("schema.proto".to_owned()),
        package: Some(package),
        dependency: vec!["options.proto".to_owned()],
        public_dependency: Vec::new(),
        weak_dependency: Vec::new(),
        message_type: Vec::new(),
        enum_type: Vec::new(),
        service: Vec::new(),
        extension: Vec::new(),
        options: None,
        source_code_info: None,
        // protoc represents proto2 by absence, including when explicitly declared.
        syntax: None,
    }
    .encode_to_vec();
    file.extend(RawFileMessages { message_type: messages }.encode_to_vec());
    let metadata = schema.metadata();
    file.extend(
        FileOptionCarrier {
            options: Some(TableOptions {
                table: Some(PortableTableMetadata {
                    provider: Some(metadata.provider.clone()),
                    location: metadata.location.clone(),
                    comment: metadata.comment.clone(),
                    partition_columns: metadata.partition_columns.clone(),
                    properties: metadata
                        .properties
                        .iter()
                        .map(|(key, value)| PortableTableProperty {
                            key: Some(key.clone()),
                            value: Some(value.clone()),
                        })
                        .collect(),
                }),
            }),
        }
        .encode_to_vec(),
    );
    let mut descriptors = RawDescriptorSet::decode(OPTION_DESCRIPTORS).map_err(descriptor_error)?;
    descriptors.file.push(file);
    Ok(descriptors.encode_to_vec())
}

fn build_message(
    package: &str,
    message_name: &str,
    fields: &[CatalogField],
    output: &mut Vec<DescriptorProto>,
) -> CatalogResult<()> {
    let mut field_names = BTreeSet::new();
    let mut descriptor_fields = Vec::with_capacity(fields.len());
    for field in fields {
        let field_name = protobuf_identifier(field.name(), IdentifierStyle::Snake);
        if !field_names.insert(field_name.clone()) {
            return Err(CatalogError::new(
                CatalogErrorCode::DescriptorGeneration,
                format!("protobuf field-name collision for {}", field.name()),
            ));
        }
        validate_protobuf_number(field.field_number())?;
        descriptor_fields.push(protobuf_field(package, message_name, &field_name, field, output)?);
    }
    output.push(DescriptorProto {
        name: Some(message_name.to_owned()),
        field: descriptor_fields,
        extension: Vec::new(),
        nested_type: Vec::new(),
        enum_type: Vec::new(),
        extension_range: Vec::new(),
        oneof_decl: Vec::new(),
        options: None,
        reserved_range: Vec::new(),
        reserved_name: Vec::new(),
    });
    Ok(())
}

fn protobuf_field(
    package: &str,
    parent_message: &str,
    field_name: &str,
    field: &CatalogField,
    output: &mut Vec<DescriptorProto>,
) -> CatalogResult<FieldDescriptorProto> {
    let (field_type, type_name, repeated) =
        protobuf_type_for_catalog(package, parent_message, field_name, field.mapping().arrow(), output)?;
    let label = if repeated { Label::Repeated } else { Label::Optional };
    Ok(FieldDescriptorProto {
        name: Some(field_name.to_owned()),
        number: Some(i32::try_from(field.field_number()).map_err(|_| {
            CatalogError::new(
                CatalogErrorCode::DescriptorGeneration,
                format!("protobuf field number {} exceeds i32", field.field_number()),
            )
        })?),
        label: Some(label as i32),
        r#type: Some(field_type as i32),
        type_name,
        extendee: None,
        default_value: None,
        oneof_index: None,
        json_name: Some(field.name().to_owned()),
        options: None,
        proto3_optional: None,
    })
}

fn protobuf_type_for_catalog(
    package: &str,
    parent_message: &str,
    field_name: &str,
    data_type: &CatalogType,
    output: &mut Vec<DescriptorProto>,
) -> CatalogResult<(Type, Option<String>, bool)> {
    match data_type {
        CatalogType::Struct { fields } => {
            let message_name = format!(
                "{parent_message}{}",
                protobuf_identifier(field_name, IdentifierStyle::Pascal)
            );
            build_message(package, &message_name, fields, output)?;
            Ok((Type::Message, Some(format!(".{package}.{message_name}")), false))
        }
        CatalogType::List { element, .. } => {
            let (element_type, type_name, nested_repeated) = protobuf_type_for_catalog(
                package,
                parent_message,
                &format!("{field_name}_item"),
                element.mapping().arrow(),
                output,
            )?;
            if nested_repeated {
                let wrapper_name = format!(
                    "{parent_message}{}Item",
                    protobuf_identifier(field_name, IdentifierStyle::Pascal)
                );
                let wrapper_field = CatalogField::new(
                    element.id(),
                    "value",
                    0,
                    1,
                    element.is_nullable(),
                    None,
                    element.mapping().clone(),
                )?;
                build_message(package, &wrapper_name, &[wrapper_field], output)?;
                Ok((Type::Message, Some(format!(".{package}.{wrapper_name}")), true))
            } else {
                Ok((element_type, type_name, true))
            }
        }
        CatalogType::Map { key, value, .. } => {
            let entry_name = format!(
                "{parent_message}{}Entry",
                protobuf_identifier(field_name, IdentifierStyle::Pascal)
            );
            build_message(
                package,
                &entry_name,
                &[key.as_ref().clone(), value.as_ref().clone()],
                output,
            )?;
            Ok((Type::Message, Some(format!(".{package}.{entry_name}")), true))
        }
        CatalogType::Dictionary { value, .. } => {
            protobuf_type_for_catalog(package, parent_message, field_name, value, output)
        }
        CatalogType::Decimal128 { .. } => Ok((Type::String, None, false)),
        scalar => Ok((protobuf_scalar_type(scalar.protobuf_type()), None, false)),
    }
}

const fn protobuf_scalar_type(data_type: ProtobufType) -> Type {
    match data_type {
        ProtobufType::Bool => Type::Bool,
        ProtobufType::Int32 => Type::Sint32,
        ProtobufType::Int64 => Type::Int64,
        ProtobufType::Uint32 => Type::Uint32,
        ProtobufType::Uint64 => Type::Uint64,
        ProtobufType::Float => Type::Float,
        ProtobufType::Double => Type::Double,
        ProtobufType::String => Type::String,
        ProtobufType::Bytes => Type::Bytes,
        ProtobufType::Message | ProtobufType::Repeated => Type::Message,
    }
}

fn validate_protobuf_number(number: u32) -> CatalogResult<()> {
    if number == 0 || number > MAX_PROTOBUF_FIELD_NUMBER || (19_000..=19_999).contains(&number) {
        return Err(CatalogError::new(
            CatalogErrorCode::DescriptorGeneration,
            format!("protobuf field number {number} is reserved or out of range"),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum IdentifierStyle {
    Snake,
    Pascal,
}

fn protobuf_identifier(value: &str, style: IdentifierStyle) -> String {
    let mut words = Vec::new();
    let mut current = String::new();
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            current.push(character.to_ascii_lowercase());
        } else if !current.is_empty() {
            words.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    if words.is_empty() {
        words.push("field".to_owned());
    }
    let mut output = match style {
        IdentifierStyle::Snake => words.join("_"),
        IdentifierStyle::Pascal => words
            .into_iter()
            .map(|word| {
                let mut characters = word.chars();
                match characters.next() {
                    Some(first) => format!("{}{}", first.to_ascii_uppercase(), characters.as_str()),
                    None => String::new(),
                }
            })
            .collect(),
    };
    if output.starts_with(|character: char| character.is_ascii_digit()) {
        output.insert_str(
            0,
            match style {
                IdentifierStyle::Snake => "f_",
                IdentifierStyle::Pascal => "F",
            },
        );
    }
    output
}
