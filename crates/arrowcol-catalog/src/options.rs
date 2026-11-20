use std::collections::BTreeMap;

use prost::{Message, encoding};
use prost_types::{
    EnumDescriptorProto, FieldDescriptorProto, FileDescriptorSet,
    field_descriptor_proto::{Label, Type},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{CatalogError, CatalogErrorCode, CatalogResult, artifacts::OPTION_DESCRIPTORS};

/// Descriptor-derived, versioned vocabulary for portable table annotations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProtobufOptionDefinitions {
    /// SHA-256 of the complete compiled vocabulary and its descriptor dependencies.
    pub definitions_sha256: String,
    /// Supported optional scalar/enum MessageOptions, sorted by full option name.
    pub options: Vec<ProtobufOptionDefinition>,
}

/// One native proto2 option exposed as a DDL property.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProtobufOptionDefinition {
    /// DDL key: `protobuf.<fully-qualified-extension-name>`.
    pub key: String,
    /// Fully qualified extension name without a leading dot.
    pub option_name: String,
    /// Whether the descriptor declares an optional field.
    pub optional: bool,
    /// Native protobuf scalar kind, or `enum`.
    pub kind: String,
    /// Enum value names in descriptor declaration order; empty for scalar options.
    pub enum_values: Vec<String>,
    /// Explicit proto2 default, if declared in the authoritative source.
    pub declared_default: Option<String>,
}

struct NativeOption {
    definition: ProtobufOptionDefinition,
    field: FieldDescriptorProto,
    enumeration: Option<EnumDescriptorProto>,
}

fn invalid(message: impl std::fmt::Display) -> CatalogError {
    CatalogError::new(CatalogErrorCode::DescriptorGeneration, message.to_string())
}

fn native_options() -> CatalogResult<Vec<NativeOption>> {
    let descriptors = FileDescriptorSet::decode(OPTION_DESCRIPTORS).map_err(invalid)?;
    let enums = descriptors
        .file
        .iter()
        .flat_map(|file| {
            file.enum_type
                .iter()
                .map(|value| (format!(".{}.{}", file.package(), value.name()), value.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let mut options = Vec::new();
    for file in &descriptors.file {
        for field in &file.extension {
            if field.extendee() != ".google.protobuf.MessageOptions" {
                continue;
            }
            if field.label() != Label::Optional || matches!(field.r#type(), Type::Message | Type::Group | Type::Bytes) {
                return Err(invalid(format!(
                    "option {} must be an optional scalar or enum",
                    field.name()
                )));
            }
            let name = format!("{}.{}", file.package(), field.name());
            let enumeration = if field.r#type() == Type::Enum {
                Some(
                    enums
                        .get(field.type_name())
                        .ok_or_else(|| invalid(format!("missing enum for {name}")))?
                        .clone(),
                )
            } else {
                None
            };
            options.push(NativeOption {
                definition: ProtobufOptionDefinition {
                    key: format!("protobuf.{name}"),
                    option_name: name,
                    optional: true,
                    kind: field
                        .r#type()
                        .as_str_name()
                        .trim_start_matches("TYPE_")
                        .to_ascii_lowercase(),
                    enum_values: enumeration
                        .as_ref()
                        .map(|value| value.value.iter().map(|value| value.name().to_owned()).collect())
                        .unwrap_or_default(),
                    declared_default: field.default_value.clone(),
                },
                field: field.clone(),
                enumeration,
            });
        }
    }
    options.sort_by(|left, right| left.definition.option_name.cmp(&right.definition.option_name));
    Ok(options)
}

/// Reads the authoritative vocabulary from compiled descriptors, not source text.
///
/// # Errors
///
/// Rejects malformed descriptors or unsupported non-optional/message/bytes options.
pub fn protobuf_option_definitions() -> CatalogResult<ProtobufOptionDefinitions> {
    Ok(ProtobufOptionDefinitions {
        definitions_sha256: format!("sha256:{}", hex::encode(Sha256::digest(OPTION_DESCRIPTORS))),
        options: native_options()?.into_iter().map(|option| option.definition).collect(),
    })
}

/// Validates `protobuf.*` properties and materializes native proto2 defaults.
///
/// All other keys and original explicit values are preserved verbatim. Callers
/// must also use the existing `arrowcol-props` registry for reserved Arrowcol and
/// Delta properties. These options are annotations and never select runtime policy.
///
/// # Errors
///
/// Rejects unknown `protobuf.*` keys, invalid scalar/enum values, or malformed
/// definitions. Enum values must use their exact declared names.
pub fn materialize_protobuf_properties(
    properties: &BTreeMap<String, String>,
) -> CatalogResult<BTreeMap<String, String>> {
    let options = native_options()?;
    for key in properties.keys().filter(|key| key.starts_with("protobuf.")) {
        if !options.iter().any(|option| option.definition.key == *key) {
            return Err(CatalogError::new(
                CatalogErrorCode::TypeMappingUnsupported,
                format!("unknown protobuf property {key}"),
            ));
        }
    }
    let mut output = properties.clone();
    for option in options {
        let value = output
            .entry(option.definition.key.clone())
            .or_insert_with(|| option.default_value());
        option
            .encode(value, &mut Vec::new())
            .map_err(|error| CatalogError::new(CatalogErrorCode::TypeMappingUnsupported, error.message()))?;
    }
    Ok(output)
}

impl NativeOption {
    fn default_value(&self) -> String {
        self.definition
            .declared_default
            .clone()
            .unwrap_or_else(|| match self.field.r#type() {
                Type::Enum => self.definition.enum_values.first().cloned().unwrap_or_default(),
                Type::String => String::new(),
                Type::Bool => "false".to_owned(),
                _ => "0".to_owned(),
            })
    }

    fn encode(&self, value: &str, output: &mut Vec<u8>) -> CatalogResult<()> {
        let number = u32::try_from(self.field.number()).map_err(invalid)?;
        macro_rules! scalar {
            ($module:ident, $ty:ty) => {{
                let parsed = value
                    .parse::<$ty>()
                    .map_err(|error| invalid(format!("{}: {error}", self.definition.key)))?;
                encoding::$module::encode(number, &parsed, output);
            }};
        }
        match self.field.r#type() {
            Type::Enum => {
                let value = self
                    .enumeration
                    .as_ref()
                    .and_then(|enumeration| enumeration.value.iter().find(|item| item.name() == value))
                    .ok_or_else(|| {
                        invalid(format!(
                            "{} must be one of {}",
                            self.definition.key,
                            self.definition.enum_values.join(", ")
                        ))
                    })?;
                encoding::int32::encode(number, &value.number(), output);
            }
            Type::String => encoding::string::encode(number, &value.to_owned(), output),
            Type::Bool => scalar!(bool, bool),
            Type::Int32 => scalar!(int32, i32),
            Type::Int64 => scalar!(int64, i64),
            Type::Uint32 => scalar!(uint32, u32),
            Type::Uint64 => scalar!(uint64, u64),
            Type::Sint32 => scalar!(sint32, i32),
            Type::Sint64 => scalar!(sint64, i64),
            Type::Fixed32 => scalar!(fixed32, u32),
            Type::Fixed64 => scalar!(fixed64, u64),
            Type::Sfixed32 => scalar!(sfixed32, i32),
            Type::Sfixed64 => scalar!(sfixed64, i64),
            Type::Float => scalar!(float, f32),
            Type::Double => scalar!(double, f64),
            _ => return Err(invalid("unsupported protobuf option kind")),
        }
        Ok(())
    }

    fn decode(&self, wire: encoding::WireType, input: &mut &[u8]) -> CatalogResult<String> {
        macro_rules! scalar {
            ($module:ident, $ty:ty) => {{
                let mut value = <$ty>::default();
                encoding::$module::merge(wire, &mut value, input, encoding::DecodeContext::default())
                    .map_err(invalid)?;
                value.to_string()
            }};
        }
        Ok(match self.field.r#type() {
            Type::Enum => {
                let mut value = 0;
                encoding::int32::merge(wire, &mut value, input, encoding::DecodeContext::default()).map_err(invalid)?;
                self.enumeration
                    .as_ref()
                    .and_then(|enumeration| enumeration.value.iter().find(|item| item.number() == value))
                    .ok_or_else(|| invalid("unknown encoded enum option value"))?
                    .name()
                    .to_owned()
            }
            Type::String => scalar!(string, String),
            Type::Bool => scalar!(bool, bool),
            Type::Int32 => scalar!(int32, i32),
            Type::Int64 => scalar!(int64, i64),
            Type::Uint32 => scalar!(uint32, u32),
            Type::Uint64 => scalar!(uint64, u64),
            Type::Sint32 => scalar!(sint32, i32),
            Type::Sint64 => scalar!(sint64, i64),
            Type::Fixed32 => scalar!(fixed32, u32),
            Type::Fixed64 => scalar!(fixed64, u64),
            Type::Sfixed32 => scalar!(sfixed32, i32),
            Type::Sfixed64 => scalar!(sfixed64, i64),
            Type::Float => scalar!(float, f32).to_ascii_lowercase(),
            Type::Double => scalar!(double, f64).to_ascii_lowercase(),
            _ => return Err(invalid("unsupported protobuf option kind")),
        })
    }
}

pub(crate) fn encode_options(properties: &BTreeMap<String, String>) -> CatalogResult<Vec<u8>> {
    let mut output = Vec::new();
    let mut options = native_options()?;
    options.sort_by_key(|option| option.field.number());
    for option in options {
        if let Some(value) = properties.get(&option.definition.key) {
            option.encode(value, &mut output)?;
        }
    }
    Ok(output)
}

pub(crate) fn render_options(mut input: &[u8]) -> CatalogResult<String> {
    let definitions = native_options()?;
    let mut source = String::new();
    while !input.is_empty() {
        let (tag, wire) = encoding::decode_key(&mut input).map_err(invalid)?;
        let option = definitions
            .iter()
            .find(|option| u32::try_from(option.field.number()).ok() == Some(tag))
            .ok_or_else(|| invalid(format!("unknown encoded message option {tag}")))?;
        let value = option.decode(wire, &mut input)?;
        let value = if option.field.r#type() == Type::String {
            crate::artifacts::protobuf_string(&value)
        } else {
            value
        };
        source.push_str(&format!("  option ({}) = {value};\n", option.definition.option_name));
    }
    Ok(source)
}
