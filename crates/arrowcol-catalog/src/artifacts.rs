use std::collections::BTreeSet;

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
};

const ARTIFACT_MANIFEST_VERSION: u32 = 1;
const MAX_PROTOBUF_FIELD_NUMBER: u32 = 536_870_911;

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
        for file in descriptors.file {
            source.push_str(&format!("package {};\n\n", file.package.unwrap_or_default()));
            for message in file.message_type {
                source.push_str(&format!("message {} {{\n", message.name.unwrap_or_default()));
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
                    let json_name = serde_json::to_string(&field.json_name.unwrap_or_default()).map_err(|error| {
                        CatalogError::new(
                            CatalogErrorCode::DescriptorGeneration,
                            format!("protobuf JSON name serialization failed: {error}"),
                        )
                    })?;
                    source.push_str(&format!(
                        "  {label} {data_type} {} = {} [json_name = {json_name}];\n",
                        field.name.unwrap_or_default(),
                        field.number.unwrap_or_default(),
                    ));
                }
                source.push_str("}\n\n");
            }
        }
        Ok(source)
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

    let protobuf_descriptor = protobuf_descriptor(schema)?.encode_to_vec();
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

fn protobuf_descriptor(schema: &CatalogSchemaVersion) -> CatalogResult<FileDescriptorSet> {
    let package = format!(
        "arrowcol.{}.{}",
        protobuf_identifier(schema.identity().catalog().as_str(), IdentifierStyle::Snake),
        protobuf_identifier(schema.identity().schema().as_str(), IdentifierStyle::Snake)
    );
    let root_name = protobuf_identifier(schema.identity().table().as_str(), IdentifierStyle::Pascal);
    let mut messages = Vec::new();
    build_message(&package, &root_name, schema.fields(), &mut messages)?;
    let file_name = format!(
        "{}/{}/{}_v{}.proto",
        protobuf_identifier(schema.identity().catalog().as_str(), IdentifierStyle::Snake),
        protobuf_identifier(schema.identity().schema().as_str(), IdentifierStyle::Snake),
        protobuf_identifier(schema.identity().table().as_str(), IdentifierStyle::Snake),
        schema.version().get()
    );
    Ok(FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some(file_name),
            package: Some(package),
            dependency: Vec::new(),
            public_dependency: Vec::new(),
            weak_dependency: Vec::new(),
            message_type: messages,
            enum_type: Vec::new(),
            service: Vec::new(),
            extension: Vec::new(),
            options: None,
            source_code_info: None,
            syntax: Some("proto2".to_owned()),
        }],
    })
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
