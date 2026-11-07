use std::{
    collections::BTreeMap,
    fmt::Write,
    io::Write as IoWrite,
    path::PathBuf,
    process::{Command, Stdio},
};

use arrowcol_catalog::{CatalogField, CatalogType, ListKind, TimeUnit};

use crate::{SdkGenError, SdkGenResult};

use super::{NamedKind, RenderContext, rust_identifier};

const RUSTFMT_ARGUMENTS: [&str; 8] = [
    "--emit",
    "stdout",
    "--edition",
    "2024",
    "--config",
    "max_width=120,use_field_init_shorthand=true",
    "--quiet",
    "--",
];

pub(super) fn render(context: &RenderContext<'_>) -> SdkGenResult<BTreeMap<PathBuf, Vec<u8>>> {
    let mut files = BTreeMap::new();
    files.insert(PathBuf::from("rust/Cargo.toml"), cargo_manifest(context).into_bytes());
    files.insert(
        PathBuf::from("rust/src/lib.rs"),
        format_rust(&library(context)?)?.into_bytes(),
    );
    files.insert(
        PathBuf::from("rust/tests/generated.rs"),
        format_rust(&generated_test(context)?)?.into_bytes(),
    );
    files.insert(PathBuf::from("rust/README.md"), readme(context).into_bytes());
    Ok(files)
}

fn cargo_manifest(context: &RenderContext<'_>) -> String {
    format!(
        "[package]\n\
name = \"{}\"\n\
version = \"0.1.0\"\n\
edition = \"2024\"\n\
publish = false\n\n\
[workspace]\n\n\
[dependencies]\n\
arrow-array = \"=58.4.0\"\n\
arrow-buffer = \"=58.4.0\"\n\
arrow-flight = {{ version = \"=58.4.0\", default-features = false }}\n\
arrow-ipc = {{ version = \"=58.4.0\", default-features = false }}\n\
arrow-schema = \"=58.4.0\"\n\
futures = \"=0.3.34\"\n\
prost = \"0.14\"\n\
serde = {{ version = \"=1.0.229\", features = [\"derive\"] }}\n\
serde_json = \"=1.0.154\"\n\
thiserror = \"=2.0.21\"\n\
tonic = {{ version = \"=0.14.6\", features = [\"transport\"] }}\n\
[dev-dependencies]\n\
tokio = {{ version = \"=1.53.2\", features = [\"macros\", \"rt-multi-thread\"] }}\n\n\
[lints.rust]\n\
unsafe_code = \"forbid\"\n\n\
[lints.clippy]\n\
expect_used = \"deny\"\n\
unwrap_used = \"deny\"\n",
        context.rust_package
    )
}

fn library(context: &RenderContext<'_>) -> SdkGenResult<String> {
    let mut output = String::new();
    writeln!(
        output,
        "//! Generated typed Arrow and Flight client surface for `{}`.\n\
#![forbid(unsafe_code)]\n\
#![deny(clippy::expect_used, clippy::unwrap_used)]\n",
        context.schema.identity()
    )
    .map_err(format_error)?;
    output.push_str(include_str!("templates/rust_imports.rs.txt"));
    output.push_str("\n/// Prost-generated schema contract types. Arrow Flight remains the wire.\npub mod proto;\n");
    writeln!(
        output,
        "\npub const TABLE_CATALOG: &str = {:?};\n\
pub const TABLE_SCHEMA: &str = {:?};\n\
pub const TABLE_NAME: &str = {:?};\n\
pub const TABLE_ID: &str = {:?};\n\
pub const SCHEMA_VERSION: u64 = {};\n\
pub const SCHEMA_FINGERPRINT: &str = {:?};\n\
pub const DO_PUT_PATH: &str = \"/arrow.flight.protocol.FlightService/DoPut\";\n",
        context.schema.identity().catalog().as_str(),
        context.schema.identity().schema().as_str(),
        context.schema.identity().table().as_str(),
        context.schema.table_id().to_string(),
        context.schema.version().get(),
        context.schema.fingerprint().to_string(),
    )
    .map_err(format_error)?;
    output.push_str(
        "\n/// Stable catalog metadata for one generated top-level field.\n\
#[derive(Clone, Copy, Debug, Eq, PartialEq)]\n\
pub struct FieldMetadata {\n\
    pub id: &'static str,\n\
    pub name: &'static str,\n\
    pub position: u32,\n\
    pub protobuf_field_number: u32,\n\
    pub nullable: bool,\n\
    pub partition_ordinal: Option<u32>,\n\
}\n\n\
/// Ordered top-level field identities and protobuf numbers.\n\
pub const FIELD_METADATA: &[FieldMetadata] = &[\n",
    );
    for field in context.schema.fields() {
        writeln!(
            output,
            "    FieldMetadata {{ id: {:?}, name: {:?}, position: {}, protobuf_field_number: {}, nullable: {}, partition_ordinal: {} }},",
            field.id().to_string(),
            field.name(),
            field.position(),
            field.field_number(),
            field.is_nullable(),
            field.partition().map_or_else(|| "None".to_owned(), |partition| format!("Some({})", partition.ordinal())),
        )
        .map_err(format_error)?;
    }
    output.push_str("];\n");

    for definition in &context.named {
        match &definition.kind {
            NamedKind::Struct(fields) => {
                render_struct(&mut output, context, &definition.name, fields)?;
                render_builder(&mut output, context, &definition.name, fields)?;
            }
            NamedKind::Map { key, value } => {
                let fields = [key.as_ref().clone(), value.as_ref().clone()];
                render_struct(&mut output, context, &definition.name, &fields)?;
                render_builder(&mut output, context, &definition.name, &fields)?;
            }
        }
    }
    render_struct(&mut output, context, &context.row_name, context.schema.fields())?;
    render_builder(&mut output, context, &context.row_name, context.schema.fields())?;

    writeln!(
        output,
        "\n/// Returns the exact registered Arrow schema.\n\
#[must_use]\n\
pub fn arrow_schema() -> SchemaRef {{\n    Arc::new(Schema::new(vec!["
    )
    .map_err(format_error)?;
    for field in context.schema.fields() {
        writeln!(output, "        {},", arrow_field(field)?).map_err(format_error)?;
    }
    output.push_str("    ]))\n}\n\n");

    writeln!(
        output,
        "/// Constructs an exact-schema Arrow record batch from typed rows.\n\
///\n\
/// # Errors\n\
///\n\
/// Returns an error for invalid fixed-size values, Arrow construction failures,\n\
/// or any generated model/type mismatch.\n\
pub fn record_batch(rows: &[{}]) -> Result<RecordBatch, SdkError> {{\n\
    let schema = arrow_schema();\n\
    let mut columns = Vec::with_capacity(schema.fields().len());",
        context.row_name
    )
    .map_err(format_error)?;
    for (index, field) in context.schema.fields().iter().enumerate() {
        let row_field = rust_identifier(field.name());
        let cell = optional_cell_expression(field, &format!("row.{row_field}"), "row")?;
        writeln!(
            output,
            "    {{\n\
        let values = rows.iter().map(|row| {cell}).collect::<Vec<_>>();\n\
        columns.push(build_array(schema.field({index}).data_type(), &values)?);\n\
    }}"
        )
        .map_err(format_error)?;
    }
    output.push_str("    RecordBatch::try_new(schema, columns).map_err(SdkError::from)\n}\n\n");
    output.push_str(include_str!("templates/rust_runtime.rs.txt"));
    Ok(output)
}

fn render_struct(
    output: &mut String,
    context: &RenderContext<'_>,
    name: &str,
    fields: &[CatalogField],
) -> SdkGenResult<()> {
    writeln!(
        output,
        "\n#[derive(Clone, Debug, PartialEq)]\n\
pub struct {name} {{"
    )
    .map_err(format_error)?;
    for field in fields {
        writeln!(
            output,
            "    pub {}: {},",
            rust_identifier(field.name()),
            rust_field_type(context, field)?
        )
        .map_err(format_error)?;
    }
    output.push_str("}\n");
    Ok(())
}

fn render_builder(
    output: &mut String,
    context: &RenderContext<'_>,
    name: &str,
    fields: &[CatalogField],
) -> SdkGenResult<()> {
    let builder = format!("{name}Builder");
    writeln!(
        output,
        "\n#[derive(Clone, Debug, Default)]\n\
pub struct {builder} {{"
    )
    .map_err(format_error)?;
    for field in fields {
        writeln!(
            output,
            "    {}: Option<{}>,",
            rust_identifier(field.name()),
            rust_base_type(context, field)?
        )
        .map_err(format_error)?;
    }
    writeln!(output, "}}\n\nimpl {builder} {{").map_err(format_error)?;
    for field in fields {
        let identifier = rust_identifier(field.name());
        let base_type = rust_base_type(context, field)?;
        writeln!(
            output,
            "    #[must_use]\n\
    pub fn set_{identifier}(mut self, value: {base_type}) -> Self {{\n\
        self.{identifier} = Some(value);\n\
        self\n\
    }}"
        )
        .map_err(format_error)?;
        if field.is_nullable() {
            writeln!(
                output,
                "\n    #[must_use]\n\
    pub fn set_{identifier}_option(mut self, value: Option<{base_type}>) -> Self {{\n\
        self.{identifier} = value;\n\
        self\n\
    }}"
            )
            .map_err(format_error)?;
        }
    }
    writeln!(
        output,
        "\n    /// Builds the typed row value.\n\
    ///\n\
    /// # Errors\n\
    ///\n\
    /// Returns an error when a required field was not supplied.\n\
    pub fn build(self) -> Result<{name}, SdkError> {{\n\
        Ok({name} {{"
    )
    .map_err(format_error)?;
    for field in fields {
        let identifier = rust_identifier(field.name());
        if field.is_nullable() {
            writeln!(output, "            {identifier}: self.{identifier},").map_err(format_error)?;
        } else {
            writeln!(
                output,
                "            {identifier}: self.{identifier}.ok_or(SdkError::MissingRequiredField({:?}))?,",
                field.name()
            )
            .map_err(format_error)?;
        }
    }
    output.push_str("        })\n    }\n}\n");
    Ok(())
}

fn rust_field_type(context: &RenderContext<'_>, field: &CatalogField) -> SdkGenResult<String> {
    let base = rust_base_type(context, field)?;
    Ok(if field.is_nullable() {
        format!("Option<{base}>")
    } else {
        base
    })
}

fn rust_base_type(context: &RenderContext<'_>, field: &CatalogField) -> SdkGenResult<String> {
    let output = match field.mapping().arrow() {
        CatalogType::Boolean => "bool".to_owned(),
        CatalogType::Int8 => "i8".to_owned(),
        CatalogType::Int16 => "i16".to_owned(),
        CatalogType::Int32 | CatalogType::Date32 | CatalogType::Time32 { .. } => "i32".to_owned(),
        CatalogType::Int64
        | CatalogType::UInt64
        | CatalogType::Timestamp { .. }
        | CatalogType::Date64
        | CatalogType::Time64 { .. }
        | CatalogType::Duration { .. } => {
            if matches!(field.mapping().arrow(), CatalogType::UInt64) {
                "u64".to_owned()
            } else {
                "i64".to_owned()
            }
        }
        CatalogType::UInt8 => "u8".to_owned(),
        CatalogType::UInt16 => "u16".to_owned(),
        CatalogType::UInt32 => "u32".to_owned(),
        CatalogType::Float32 => "f32".to_owned(),
        CatalogType::Float64 => "f64".to_owned(),
        CatalogType::Binary
        | CatalogType::FixedSizeBinary { .. }
        | CatalogType::LargeBinary
        | CatalogType::BinaryView => "Vec<u8>".to_owned(),
        CatalogType::Utf8 | CatalogType::LargeUtf8 | CatalogType::Utf8View => "String".to_owned(),
        CatalogType::Decimal128 { .. } => "i128".to_owned(),
        CatalogType::Struct { .. } => context.named_type(field)?.to_owned(),
        CatalogType::List { element, .. } => {
            format!("Vec<{}>", rust_field_type(context, element)?)
        }
        CatalogType::Map { .. } => {
            format!("Vec<{}>", context.named_type(field)?)
        }
        CatalogType::Dictionary { .. } => {
            return Err(SdkGenError::UnsupportedType {
                path: field.name().to_owned(),
                reason: "dictionary type reached Rust rendering".to_owned(),
            });
        }
    };
    Ok(output)
}

fn optional_cell_expression(field: &CatalogField, access: &str, owner: &str) -> SdkGenResult<String> {
    if field.is_nullable() {
        let expression = cell_expression(field, "value", true)?;
        let access = access.strip_prefix(&format!("{owner}.")).unwrap_or(access);
        Ok(format!("{owner}.{access}.as_ref().map(|value| {expression})"))
    } else {
        cell_expression(field, access, false).map(|expression| format!("Some({expression})"))
    }
}

fn cell_expression(field: &CatalogField, value: &str, scalar_reference: bool) -> SdkGenResult<String> {
    let scalar = |variant: &str| {
        if scalar_reference {
            format!("Cell::{variant}(*{value})")
        } else {
            format!("Cell::{variant}({value})")
        }
    };
    let output = match field.mapping().arrow() {
        CatalogType::Boolean => scalar("Boolean"),
        CatalogType::Int8 => scalar("Int8"),
        CatalogType::Int16 => scalar("Int16"),
        CatalogType::Int32 => scalar("Int32"),
        CatalogType::Int64 => scalar("Int64"),
        CatalogType::UInt8 => scalar("UInt8"),
        CatalogType::UInt16 => scalar("UInt16"),
        CatalogType::UInt32 => scalar("UInt32"),
        CatalogType::UInt64 => scalar("UInt64"),
        CatalogType::Float32 => scalar("Float32"),
        CatalogType::Float64 => scalar("Float64"),
        CatalogType::Timestamp { .. }
        | CatalogType::Date64
        | CatalogType::Time64 { .. }
        | CatalogType::Duration { .. } => scalar("Int64"),
        CatalogType::Date32 | CatalogType::Time32 { .. } => scalar("Int32"),
        CatalogType::Binary
        | CatalogType::FixedSizeBinary { .. }
        | CatalogType::LargeBinary
        | CatalogType::BinaryView => format!("Cell::Bytes({value}.clone())"),
        CatalogType::Utf8 | CatalogType::LargeUtf8 | CatalogType::Utf8View => {
            format!("Cell::String({value}.clone())")
        }
        CatalogType::Decimal128 { .. } => scalar("Decimal128"),
        CatalogType::Struct { fields } => {
            let mut cells = Vec::new();
            for child in fields {
                let identifier = rust_identifier(child.name());
                let access = format!("{value}.{identifier}");
                cells.push(optional_nested_cell_expression(child, &access)?);
            }
            format!("Cell::Struct(vec![{}])", cells.join(", "))
        }
        CatalogType::List { element, .. } => {
            let item = if element.is_nullable() {
                let inner = cell_expression(element, "item", true)?;
                format!("item.as_ref().map(|item| {inner})")
            } else {
                let inner = cell_expression(element, "item", true)?;
                format!("Some({inner})")
            };
            format!("Cell::List({value}.iter().map(|item| {item}).collect::<Vec<_>>())")
        }
        CatalogType::Map {
            key, value: map_value, ..
        } => {
            let key_expression = cell_expression(key, "entry.key", false)?;
            let value_expression = if map_value.is_nullable() {
                let inner = cell_expression(map_value, "value", true)?;
                format!("entry.value.as_ref().map(|value| {inner})")
            } else {
                let inner = cell_expression(map_value, "entry.value", false)?;
                format!("Some({inner})")
            };
            format!("Cell::Map({value}.iter().map(|entry| ({key_expression}, {value_expression})).collect::<Vec<_>>())")
        }
        CatalogType::Dictionary { .. } => {
            return Err(SdkGenError::UnsupportedType {
                path: field.name().to_owned(),
                reason: "dictionary type reached cell rendering".to_owned(),
            });
        }
    };
    Ok(output)
}

fn optional_nested_cell_expression(field: &CatalogField, access: &str) -> SdkGenResult<String> {
    if field.is_nullable() {
        let inner = cell_expression(field, "value", true)?;
        Ok(format!("{access}.as_ref().map(|value| {inner})"))
    } else {
        let inner = cell_expression(field, access, false)?;
        Ok(format!("Some({inner})"))
    }
}

fn arrow_field(field: &CatalogField) -> SdkGenResult<String> {
    let base = format!(
        "Field::new({:?}, {}, {})",
        field.name(),
        arrow_type(field.mapping().arrow())?,
        field.is_nullable()
    );
    Ok(base)
}

fn arrow_type(data_type: &CatalogType) -> SdkGenResult<String> {
    let output = match data_type {
        CatalogType::Boolean => "DataType::Boolean".to_owned(),
        CatalogType::Int8 => "DataType::Int8".to_owned(),
        CatalogType::Int16 => "DataType::Int16".to_owned(),
        CatalogType::Int32 => "DataType::Int32".to_owned(),
        CatalogType::Int64 => "DataType::Int64".to_owned(),
        CatalogType::UInt8 => "DataType::UInt8".to_owned(),
        CatalogType::UInt16 => "DataType::UInt16".to_owned(),
        CatalogType::UInt32 => "DataType::UInt32".to_owned(),
        CatalogType::UInt64 => "DataType::UInt64".to_owned(),
        CatalogType::Float32 => "DataType::Float32".to_owned(),
        CatalogType::Float64 => "DataType::Float64".to_owned(),
        CatalogType::Timestamp { unit, timezone } => format!(
            "DataType::Timestamp({}, {})",
            arrow_time_unit(*unit),
            timezone
                .as_ref()
                .map_or_else(|| "None".to_owned(), |zone| format!("Some({zone:?}.into())"))
        ),
        CatalogType::Date32 => "DataType::Date32".to_owned(),
        CatalogType::Date64 => "DataType::Date64".to_owned(),
        CatalogType::Time32 { unit } => format!("DataType::Time32({})", arrow_time_unit(*unit)),
        CatalogType::Time64 { unit } => format!("DataType::Time64({})", arrow_time_unit(*unit)),
        CatalogType::Duration { unit } => {
            format!("DataType::Duration({})", arrow_time_unit(*unit))
        }
        CatalogType::Binary => "DataType::Binary".to_owned(),
        CatalogType::FixedSizeBinary { bytes } => {
            format!("DataType::FixedSizeBinary({bytes})")
        }
        CatalogType::LargeBinary => "DataType::LargeBinary".to_owned(),
        CatalogType::BinaryView => "DataType::BinaryView".to_owned(),
        CatalogType::Utf8 => "DataType::Utf8".to_owned(),
        CatalogType::LargeUtf8 => "DataType::LargeUtf8".to_owned(),
        CatalogType::Utf8View => "DataType::Utf8View".to_owned(),
        CatalogType::Decimal128 { precision, scale } => {
            format!("DataType::Decimal128({precision}, {scale})")
        }
        CatalogType::Struct { fields } => format!(
            "DataType::Struct(Fields::from(vec![{}]))",
            fields
                .iter()
                .map(arrow_field)
                .collect::<SdkGenResult<Vec<_>>>()?
                .join(", ")
        ),
        CatalogType::List { list_kind, element } => {
            let element = arrow_field(element)?;
            match list_kind {
                ListKind::List => format!("DataType::List(Arc::new({element}))"),
                ListKind::LargeList => format!("DataType::LargeList(Arc::new({element}))"),
                ListKind::FixedSize(size) => {
                    format!("DataType::FixedSizeList(Arc::new({element}), {size})")
                }
                ListKind::ListView | ListKind::LargeListView => {
                    return Err(SdkGenError::UnsupportedType {
                        path: "list".to_owned(),
                        reason: "list view reached Rust rendering".to_owned(),
                    });
                }
            }
        }
        CatalogType::Map {
            key,
            value,
            keys_sorted,
        } => format!(
            "DataType::Map(Arc::new(Field::new(\"entries\", DataType::Struct(Fields::from(vec![{}, {}])), false)), {})",
            arrow_field(key)?,
            arrow_field(value)?,
            keys_sorted
        ),
        CatalogType::Dictionary { .. } => {
            return Err(SdkGenError::UnsupportedType {
                path: "dictionary".to_owned(),
                reason: "dictionary reached Rust rendering".to_owned(),
            });
        }
    };
    Ok(output)
}

fn arrow_time_unit(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "TimeUnit::Second",
        TimeUnit::Millisecond => "TimeUnit::Millisecond",
        TimeUnit::Microsecond => "TimeUnit::Microsecond",
        TimeUnit::Nanosecond => "TimeUnit::Nanosecond",
    }
}

fn generated_test(context: &RenderContext<'_>) -> SdkGenResult<String> {
    let mut builder = format!("{}Builder::default()", context.row_name);
    for field in context.schema.fields() {
        builder.push_str(&format!(
            "\n        .set_{}({})",
            rust_identifier(field.name()),
            sample_value(context, field)?
        ));
    }
    builder.push_str("\n        .build()?");
    let nested_imports = context
        .named
        .iter()
        .map(|definition| format!("    {},\n", definition.name))
        .collect::<String>();
    Ok(format!(
        "use std::error::Error;\n\n\
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;\n\
use futures::StreamExt;\n\
use {crate_name}::{{\n\
    ImplementationPool, SCHEMA_FINGERPRINT, SCHEMA_VERSION, TABLE_NAME, arrow_schema,\n\
    do_put_request, record_batch, routing_metadata,\n\
{nested_imports}    {builder_name},\n\
}};\n\
\n\
#[tokio::test]\n\
async fn generated_fixture_builds_exact_batch_and_client_surfaces() -> Result<(), Box<dyn Error>> {{\n\
    let row = {builder};\n\
    let batch = record_batch(&[row])?;\n\
    assert_eq!(batch.schema(), arrow_schema());\n\
    let artifact_schema = try_schema_from_flatbuffer_bytes(include_bytes!(\"../../descriptors/schema.arrow\"))?;\n\
    assert_eq!(artifact_schema, arrow_schema().as_ref().clone());\n\
    assert_eq!(batch.num_rows(), 1);\n\
    assert_eq!(SCHEMA_VERSION, {version});\n\
    assert!(SCHEMA_FINGERPRINT.starts_with(\"sha256:\"));\n\
    assert_eq!(TABLE_NAME, {table:?});\n\n\
    let metadata = routing_metadata(ImplementationPool::Dataflow)?;\n\
    assert_eq!(metadata.ascii_pairs().len(), 6);\n\
    let request = do_put_request(vec![batch], ImplementationPool::Dataflow).await?;\n\
    assert_eq!(request.metadata().get(\"x-arrowcol-table\").and_then(|value| value.to_str().ok()), Some(TABLE_NAME));\n\
    assert!(request.into_inner().count().await > 0);\n\n\
    Ok(())\n\
}}\n",
        crate_name = context.rust_package.replace('-', "_"),
        nested_imports = nested_imports,
        builder_name = context.builder_name,
        builder = builder,
        version = context.schema.version().get(),
        table = context.schema.identity().table().as_str(),
    ))
}

fn sample_value(context: &RenderContext<'_>, field: &CatalogField) -> SdkGenResult<String> {
    let value = match field.mapping().arrow() {
        CatalogType::Boolean => "true".to_owned(),
        CatalogType::Int8 => "1_i8".to_owned(),
        CatalogType::Int16 => "2_i16".to_owned(),
        CatalogType::Int32 => "3_i32".to_owned(),
        CatalogType::Int64 => "4_i64".to_owned(),
        CatalogType::UInt8 => "5_u8".to_owned(),
        CatalogType::UInt16 => "6_u16".to_owned(),
        CatalogType::UInt32 => "7_u32".to_owned(),
        CatalogType::UInt64 => "8_u64".to_owned(),
        CatalogType::Float32 => "1.25_f32".to_owned(),
        CatalogType::Float64 => "2.5_f64".to_owned(),
        CatalogType::Timestamp { unit, .. } => match unit {
            TimeUnit::Second => "1_700_000_000_i64".to_owned(),
            TimeUnit::Millisecond => "1_700_000_000_000_i64".to_owned(),
            TimeUnit::Microsecond => "1_700_000_000_000_000_i64".to_owned(),
            TimeUnit::Nanosecond => "1_700_000_000_000_000_000_i64".to_owned(),
        },
        CatalogType::Date32 => "20_000_i32".to_owned(),
        CatalogType::Date64 => "1_728_000_000_i64".to_owned(),
        CatalogType::Time32 { .. } => "12_345_i32".to_owned(),
        CatalogType::Time64 { .. } | CatalogType::Duration { .. } => "123_456_i64".to_owned(),
        CatalogType::Binary | CatalogType::LargeBinary | CatalogType::BinaryView => "vec![1_u8, 2, 3]".to_owned(),
        CatalogType::FixedSizeBinary { bytes } => {
            format!("vec![0x11_u8; {bytes}_usize]")
        }
        CatalogType::Utf8 | CatalogType::LargeUtf8 | CatalogType::Utf8View => "\"sample\".to_owned()".to_owned(),
        CatalogType::Decimal128 { .. } => "12_345_i128".to_owned(),
        CatalogType::Struct { fields } => {
            let name = context.named_type(field)?;
            let values = fields
                .iter()
                .map(|child| {
                    let value = sample_value(context, child)?;
                    let value = if child.is_nullable() {
                        format!("Some({value})")
                    } else {
                        value
                    };
                    Ok(format!("{}: {value}", rust_identifier(child.name())))
                })
                .collect::<SdkGenResult<Vec<_>>>()?;
            format!("{name} {{ {} }}", values.join(", "))
        }
        CatalogType::List { list_kind, element } => {
            let item = sample_value(context, element)?;
            let item = if element.is_nullable() {
                format!("Some({item})")
            } else {
                item
            };
            match list_kind {
                ListKind::FixedSize(size) => format!("vec![{item}; {size}_usize]"),
                _ if element.is_nullable() => format!("vec![{item}, None]"),
                _ => format!("vec![{item}]"),
            }
        }
        CatalogType::Map { key, value, .. } => {
            let name = context.named_type(field)?;
            let key = sample_value(context, key)?;
            let value = sample_value(context, value)?;
            if value_field_nullable(field) {
                format!("vec![{name} {{ key: {key}, value: Some({value}) }}, {name} {{ key: {key}, value: None }}]")
            } else {
                format!("vec![{name} {{ key: {key}, value: {value} }}]")
            }
        }
        CatalogType::Dictionary { .. } => {
            return Err(SdkGenError::UnsupportedType {
                path: field.name().to_owned(),
                reason: "dictionary reached Rust fixture rendering".to_owned(),
            });
        }
    };
    Ok(value)
}

fn value_field_nullable(field: &CatalogField) -> bool {
    match field.mapping().arrow() {
        CatalogType::Map { value, .. } => value.is_nullable(),
        _ => false,
    }
}

fn readme(context: &RenderContext<'_>) -> String {
    format!(
        "# {}\n\n\
Generated Rust model and Arrow Flight helper package for `{}` version `{}`.\n\n\
```rust\n\
use {}::{{{}Builder, ImplementationPool, do_put_request, record_batch}};\n\
\n\
# async fn example() -> Result<(), Box<dyn std::error::Error>> {{\n\
let row = {}Builder::default();\n\
// Set required fields, then call `build()` and `record_batch(&[row])`.\n\
let _pool = ImplementationPool::Dataflow;\n\
# Ok(())\n\
# }}\n\
```\n\n\
`do_put_request` is a bounded convenience helper for finite batches. For large\n\
uploads, use `encode_do_put` with a streaming source and apply\n\
`decorate_do_put_request` before invoking an Arrow Flight client.\n",
        context.rust_package,
        context.schema.identity(),
        context.schema.version(),
        context.rust_package.replace('-', "_"),
        context.row_name,
        context.row_name,
    )
}

fn format_error(_: std::fmt::Error) -> SdkGenError {
    SdkGenError::Artifact("generated Rust formatting failed".to_owned())
}

pub(super) fn format_rust(source: &str) -> SdkGenResult<String> {
    let syntax = syn::parse_file(source)
        .map_err(|error| SdkGenError::Artifact(format!("generated Rust did not parse before formatting: {error}")))?;
    let source = prettyplease::unparse(&syntax);
    let mut child = Command::new("rustfmt")
        .args(RUSTFMT_ARGUMENTS)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| SdkGenError::io("rustfmt", error))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| SdkGenError::Artifact("rustfmt child process did not expose stdin".to_owned()))?;
    stdin
        .write_all(source.as_bytes())
        .map_err(|error| SdkGenError::io("rustfmt stdin", error))?;
    drop(stdin);
    let output = child
        .wait_with_output()
        .map_err(|error| SdkGenError::io("rustfmt", error))?;
    if !output.status.success() {
        return Err(SdkGenError::Artifact(format!(
            "rustfmt failed for generated source: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| SdkGenError::Artifact(format!("rustfmt emitted non-UTF-8 output: {error}")))
}

#[cfg(test)]
mod tests {
    use super::RUSTFMT_ARGUMENTS;

    #[test]
    fn generated_rustfmt_arguments_are_relocatable() {
        assert!(!RUSTFMT_ARGUMENTS.iter().any(|argument| argument.starts_with('/')));
        assert!(!RUSTFMT_ARGUMENTS.contains(&"--config-path"));
    }
}
