use std::collections::{BTreeMap, BTreeSet};

use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};
use serde::Serialize;

use crate::{ArtifactBundle, SdkGenError, SdkGenResult};

/// Successful protobuf and physical-layout compatibility verification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CompatibilityReport {
    /// Fully qualified table identity.
    pub table: String,
    /// Previous immutable schema version.
    pub previous_version: u64,
    /// Candidate immutable schema version.
    pub current_version: u64,
    /// Number of preserved top-level fields.
    pub preserved_fields: usize,
    /// Number of nullable tail-appended fields.
    pub appended_fields: usize,
    /// Number of protobuf messages checked under WIRE_JSON rules.
    pub protobuf_messages_checked: usize,
}

/// Verifies Buf `WIRE_JSON`-equivalent protobuf safety plus catalog Arrow/Delta layout rules.
///
/// The authoritative catalog policy is intentionally stricter than general
/// protobuf compatibility: existing fields and physical Arrow layout are
/// immutable, and evolution is limited to nullable, non-partition,
/// tail-appended top-level fields.
///
/// # Errors
///
/// Returns a compatibility error for identity changes, field removal/reorder,
/// number/name/type/nullability changes, non-nullable additions, partition
/// changes, or protobuf wire/JSON breakage.
pub fn verify_compatibility(previous: &ArtifactBundle, current: &ArtifactBundle) -> SdkGenResult<CompatibilityReport> {
    let previous_schema = previous.schema();
    let current_schema = current.schema();
    if previous_schema.table_id() != current_schema.table_id()
        || previous_schema.identity() != current_schema.identity()
    {
        return incompatible("table identity or deterministic table ID changed");
    }
    if current_schema.version() < previous_schema.version() {
        return incompatible("schema version regressed");
    }
    if current_schema.version() == previous_schema.version() && current_schema != previous_schema {
        return incompatible("one immutable schema version has differing contents");
    }
    if !current_schema.is_nullable_append_only_superset_of(previous_schema) {
        return incompatible("candidate is not an exact nullable tail-append-only superset under the catalog policy");
    }
    if current_schema.fields()[previous_schema.fields().len()..]
        .iter()
        .any(|field| field.partition().is_some())
    {
        return incompatible("new partition columns are forbidden");
    }

    let previous_arrow = previous_schema
        .arrow_schema()
        .map_err(|error| SdkGenError::Artifact(error.to_string()))?;
    let current_arrow = current_schema
        .arrow_schema()
        .map_err(|error| SdkGenError::Artifact(error.to_string()))?;
    if previous_arrow.fields().len() > current_arrow.fields().len()
        || previous_arrow
            .fields()
            .iter()
            .zip(current_arrow.fields())
            .any(|(left, right)| left != right)
    {
        return incompatible("existing physical Arrow field order or layout changed");
    }
    if current_arrow.fields()[previous_arrow.fields().len()..]
        .iter()
        .any(|field| !field.is_nullable())
    {
        return incompatible("new physical Arrow fields must be nullable");
    }

    let protobuf_messages_checked = verify_wire_json(previous.descriptors(), current.descriptors())?;
    Ok(CompatibilityReport {
        table: current_schema.identity().to_string(),
        previous_version: previous_schema.version().get(),
        current_version: current_schema.version().get(),
        preserved_fields: previous_schema.fields().len(),
        appended_fields: current_schema.fields().len() - previous_schema.fields().len(),
        protobuf_messages_checked,
    })
}

fn verify_wire_json(previous: &FileDescriptorSet, current: &FileDescriptorSet) -> SdkGenResult<usize> {
    let previous_messages = descriptor_messages(previous)?;
    let current_messages = descriptor_messages(current)?;
    for (message_name, previous_message) in &previous_messages {
        let Some(current_message) = current_messages.get(message_name) else {
            return incompatible(format!("protobuf message {message_name} was removed"));
        };
        verify_message(message_name, previous_message, current_message)?;
    }
    Ok(previous_messages.len())
}

fn descriptor_messages(descriptors: &FileDescriptorSet) -> SdkGenResult<BTreeMap<String, DescriptorProto>> {
    let mut output = BTreeMap::new();
    // The pinned option/WKT imports are validated byte-for-byte on bundle load.
    // Compatibility policy concerns the table, not the descriptor metamodel.
    for file in descriptors.file.iter().filter(|file| file.name() == "schema.proto") {
        validate_file(file)?;
        let package = file.package.as_deref().unwrap_or_default();
        for message in &file.message_type {
            collect_message(package, None, message, &mut output)?;
        }
    }
    Ok(output)
}

fn validate_file(file: &FileDescriptorProto) -> SdkGenResult<()> {
    if file.syntax.as_deref().is_some_and(|syntax| syntax != "proto2") {
        return Err(SdkGenError::Artifact(format!(
            "generated descriptor {} must use proto2 syntax",
            file.name.as_deref().unwrap_or("<unnamed>")
        )));
    }
    Ok(())
}

fn collect_message(
    package: &str,
    parent: Option<&str>,
    message: &DescriptorProto,
    output: &mut BTreeMap<String, DescriptorProto>,
) -> SdkGenResult<()> {
    let name = message
        .name
        .as_deref()
        .ok_or_else(|| SdkGenError::Artifact("protobuf message is missing a name".to_owned()))?;
    let local_name = parent.map_or_else(|| name.to_owned(), |parent| format!("{parent}.{name}"));
    let full_name = if package.is_empty() {
        local_name.clone()
    } else {
        format!("{package}.{local_name}")
    };
    if output.insert(full_name.clone(), message.clone()).is_some() {
        return Err(SdkGenError::Artifact(format!("duplicate protobuf message {full_name}")));
    }
    for nested in &message.nested_type {
        collect_message(package, Some(&local_name), nested, output)?;
    }
    Ok(())
}

fn verify_message(name: &str, previous: &DescriptorProto, current: &DescriptorProto) -> SdkGenResult<()> {
    let previous_fields = fields_by_number(name, &previous.field)?;
    let current_fields = fields_by_number(name, &current.field)?;
    for (number, previous_field) in previous_fields {
        let Some(current_field) = current_fields.get(&number) else {
            return incompatible(format!("protobuf field {name}#{number} was removed"));
        };
        compare_field(name, number, previous_field, current_field)?;
    }
    for field in &current.field {
        if !previous.field.iter().any(|old| old.number == field.number)
            && field.label == Some(prost_types::field_descriptor_proto::Label::Required as i32)
        {
            return incompatible(format!(
                "new protobuf field {name}#{} is required",
                field.number.unwrap_or_default()
            ));
        }
    }

    let previous_reserved = reserved_numbers(previous);
    for field in &current.field {
        if field.number.is_some_and(|number| previous_reserved.contains(&number)) {
            return incompatible(format!(
                "protobuf field {name}#{} reuses a reserved number",
                field.number.unwrap_or_default()
            ));
        }
    }
    Ok(())
}

fn fields_by_number<'a>(
    message: &str,
    fields: &'a [FieldDescriptorProto],
) -> SdkGenResult<BTreeMap<i32, &'a FieldDescriptorProto>> {
    let mut output = BTreeMap::new();
    for field in fields {
        let number = field
            .number
            .ok_or_else(|| SdkGenError::Artifact(format!("protobuf field in {message} is missing a number")))?;
        if output.insert(number, field).is_some() {
            return Err(SdkGenError::Artifact(format!(
                "duplicate protobuf field number {message}#{number}"
            )));
        }
    }
    Ok(output)
}

fn compare_field(
    message: &str,
    number: i32,
    previous: &FieldDescriptorProto,
    current: &FieldDescriptorProto,
) -> SdkGenResult<()> {
    let stable = previous.name == current.name
        && previous.json_name == current.json_name
        && previous.label == current.label
        && previous.r#type == current.r#type
        && previous.type_name == current.type_name
        && previous.oneof_index == current.oneof_index
        && previous.proto3_optional == current.proto3_optional;
    if !stable {
        return incompatible(format!(
            "protobuf field {message}#{number} changed its WIRE_JSON name, cardinality, or type"
        ));
    }
    Ok(())
}

fn reserved_numbers(message: &DescriptorProto) -> BTreeSet<i32> {
    message
        .reserved_range
        .iter()
        .flat_map(|range| {
            let start = range.start.unwrap_or_default();
            let end = range.end.unwrap_or(start);
            start..end
        })
        .collect()
}

fn incompatible<T>(message: impl Into<String>) -> SdkGenResult<T> {
    Err(SdkGenError::Compatibility(message.into()))
}
