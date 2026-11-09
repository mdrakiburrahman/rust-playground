use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    path::PathBuf,
};

use arrowcol_catalog::{CatalogField, CatalogType, ListKind, TimeUnit};

use crate::{SdkGenError, SdkGenResult};

use super::{NamedKind, RenderContext, pascal_identifier};

pub(super) fn render(context: &RenderContext<'_>) -> SdkGenResult<BTreeMap<PathBuf, Vec<u8>>> {
    let mut files = BTreeMap::new();
    files.insert(
        PathBuf::from("typescript/package.json"),
        package_json(context)?.into_bytes(),
    );
    files.insert(
        PathBuf::from("typescript/tsconfig.json"),
        tsconfig().as_bytes().to_vec(),
    );
    files.insert(PathBuf::from("typescript/src/index.ts"), library(context)?.into_bytes());
    files.insert(
        PathBuf::from("typescript/test/generated.test.ts"),
        generated_test(context)?.into_bytes(),
    );
    files.insert(PathBuf::from("typescript/README.md"), readme(context).into_bytes());
    Ok(files)
}

fn package_json(context: &RenderContext<'_>) -> SdkGenResult<String> {
    let value = serde_json::json!({
        "name": context.typescript_package,
        "version": "0.1.0",
        "private": true,
        "type": "module",
        "exports": "./src/index.ts",
        "scripts": {
            "typecheck": "tsc --project tsconfig.json --noEmit",
            "lint": "tsc --project tsconfig.json --noEmit --noUnusedLocals --noUnusedParameters",
            "test": "tsx --test test/generated.test.ts"
        },
        "peerDependencies": {
            "apache-arrow": "21.2.0"
        },
        "devDependencies": {
            "@types/node": "24.7.0",
            "tsx": "4.20.6",
            "typescript": "5.9.2"
        }
    });
    let mut output = serde_json::to_string_pretty(&value)?;
    output.push('\n');
    Ok(output)
}

fn tsconfig() -> &'static str {
    "{\n\
  \"compilerOptions\": {\n\
    \"target\": \"ES2022\",\n\
    \"module\": \"NodeNext\",\n\
    \"moduleResolution\": \"NodeNext\",\n\
    \"strict\": true,\n\
    \"noEmit\": true,\n\
    \"esModuleInterop\": true,\n\
    \"forceConsistentCasingInFileNames\": true,\n\
    \"skipLibCheck\": true,\n\
    \"types\": [\"node\"]\n\
  },\n\
  \"include\": [\"src/**/*.ts\", \"test/**/*.ts\"]\n\
}\n"
}

fn library(context: &RenderContext<'_>) -> SdkGenResult<String> {
    let mut output = String::new();
    writeln!(
        output,
        "/** Generated typed Arrow and Flight client surface for `{}`. */",
        context.schema.identity()
    )
    .map_err(format_error)?;
    output.push_str(&typescript_imports(context));
    writeln!(
        output,
        "\nexport const TABLE_CATALOG = {} as const;\n\
export const TABLE_SCHEMA = {} as const;\n\
export const TABLE_NAME = {} as const;\n\
export const TABLE_ID = {} as const;\n\
export const SCHEMA_VERSION = {} as const;\n\
export const SCHEMA_FINGERPRINT = {} as const;\n\
export const DO_PUT_PATH = '/arrow.flight.protocol.FlightService/DoPut' as const;\n",
        ts_string(context.schema.identity().catalog().as_str())?,
        ts_string(context.schema.identity().schema().as_str())?,
        ts_string(context.schema.identity().table().as_str())?,
        ts_string(&context.schema.table_id().to_string())?,
        context.schema.version().get(),
        ts_string(&context.schema.fingerprint().to_string())?,
    )
    .map_err(format_error)?;
    output.push_str(
        "\nexport interface FieldMetadata {\n\
  readonly id: string;\n\
  readonly name: string;\n\
  readonly position: number;\n\
  readonly protobufFieldNumber: number;\n\
  readonly nullable: boolean;\n\
  readonly partitionOrdinal: number | null;\n\
}\n\n\
export const FIELD_METADATA = [\n",
    );
    for field in context.schema.fields() {
        writeln!(
            output,
            "  Object.freeze({{ id: {}, name: {}, position: {}, protobufFieldNumber: {}, nullable: {}, partitionOrdinal: {} }}),",
            ts_string(&field.id().to_string())?,
            ts_string(field.name())?,
            field.position(),
            field.field_number(),
            field.is_nullable(),
            field.partition().map_or_else(|| "null".to_owned(), |partition| partition.ordinal().to_string()),
        )
        .map_err(format_error)?;
    }
    output.push_str("] as const satisfies readonly FieldMetadata[];\n");

    for definition in &context.named {
        match &definition.kind {
            NamedKind::Struct(fields) => {
                render_interface(&mut output, context, &definition.name, fields)?;
                render_builder(&mut output, context, &definition.name, fields)?;
            }
            NamedKind::Map { key, value } => {
                let fields = [key.as_ref().clone(), value.as_ref().clone()];
                render_interface(&mut output, context, &definition.name, &fields)?;
                render_builder(&mut output, context, &definition.name, &fields)?;
            }
        }
    }
    render_interface(&mut output, context, &context.row_name, context.schema.fields())?;
    render_builder(&mut output, context, &context.row_name, context.schema.fields())?;

    output.push_str("\nexport const ARROW_SCHEMA = new Schema([\n");
    for field in context.schema.fields() {
        writeln!(output, "  {},", arrow_field(field)?).map_err(format_error)?;
    }
    output.push_str("]);\n\n");
    writeln!(
        output,
        "export function recordBatch(rows: readonly {}[]): RecordBatch {{\n\
  const columns = ARROW_SCHEMA.fields.map((field) =>\n\
    buildField(field, rows.map((row) => row[field.name as keyof {}]), rows.map(() => true)),\n\
  );\n\
  const data = makeData({{\n\
    type: new Struct(ARROW_SCHEMA.fields),\n\
    length: rows.length,\n\
    children: columns.map(singleData),\n\
  }});\n\
  return new RecordBatch(ARROW_SCHEMA, data);\n\
}}\n",
        context.row_name, context.row_name
    )
    .map_err(format_error)?;
    output.push_str(include_str!("templates/typescript_runtime.ts.txt"));
    Ok(output)
}

fn render_interface(
    output: &mut String,
    context: &RenderContext<'_>,
    name: &str,
    fields: &[CatalogField],
) -> SdkGenResult<()> {
    writeln!(output, "\nexport interface {name} {{").map_err(format_error)?;
    for field in fields {
        writeln!(
            output,
            "  readonly {}: {};",
            ts_string(field.name())?,
            ts_field_type(context, field)?
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
    writeln!(output, "\nexport class {builder} {{").map_err(format_error)?;
    for field in fields {
        let property = ts_private_identifier(field.name());
        let base = ts_base_type(context, field)?;
        let nullable = if field.is_nullable() { " | null" } else { "" };
        writeln!(output, "  private {property}: {base}{nullable} | undefined;").map_err(format_error)?;
    }
    for field in fields {
        let property = ts_private_identifier(field.name());
        let method = pascal_identifier(field.name());
        let base = ts_base_type(context, field)?;
        writeln!(
            output,
            "\n  set{method}(value: {base}): this {{\n\
    this.{property} = value;\n\
    return this;\n\
  }}"
        )
        .map_err(format_error)?;
        if field.is_nullable() {
            writeln!(
                output,
                "\n  set{method}Option(value: {base} | null): this {{\n\
    this.{property} = value;\n\
    return this;\n\
  }}"
            )
            .map_err(format_error)?;
        }
    }
    output.push_str("\n  build(): ");
    output.push_str(name);
    output.push_str(" {\n");
    for field in fields.iter().filter(|field| !field.is_nullable()) {
        let property = ts_private_identifier(field.name());
        writeln!(
            output,
            "    if (this.{property} === undefined) {{\n\
      throw new Error({});\n\
    }}",
            ts_string(&format!("required field {} was not supplied", field.name()))?
        )
        .map_err(format_error)?;
    }
    output.push_str("    return Object.freeze({\n");
    for field in fields {
        let property = ts_private_identifier(field.name());
        let value = if field.is_nullable() {
            format!("this.{property} ?? null")
        } else {
            format!("this.{property}")
        };
        writeln!(output, "      {}: {value},", ts_string(field.name())?).map_err(format_error)?;
    }
    output.push_str("    });\n  }\n}\n");
    Ok(())
}

fn ts_field_type(context: &RenderContext<'_>, field: &CatalogField) -> SdkGenResult<String> {
    let base = ts_base_type(context, field)?;
    Ok(if field.is_nullable() {
        format!("{base} | null")
    } else {
        base
    })
}

fn ts_base_type(context: &RenderContext<'_>, field: &CatalogField) -> SdkGenResult<String> {
    let output = match field.mapping().arrow() {
        CatalogType::Boolean => "boolean".to_owned(),
        CatalogType::Int8
        | CatalogType::Int16
        | CatalogType::Int32
        | CatalogType::UInt8
        | CatalogType::UInt16
        | CatalogType::UInt32
        | CatalogType::Float32
        | CatalogType::Float64
        | CatalogType::Date32
        | CatalogType::Time32 { .. } => "number".to_owned(),
        CatalogType::Int64
        | CatalogType::UInt64
        | CatalogType::Timestamp { .. }
        | CatalogType::Date64
        | CatalogType::Time64 { .. }
        | CatalogType::Duration { .. }
        | CatalogType::Decimal128 { .. } => "bigint".to_owned(),
        CatalogType::Binary
        | CatalogType::FixedSizeBinary { .. }
        | CatalogType::LargeBinary
        | CatalogType::BinaryView => "Uint8Array".to_owned(),
        CatalogType::Utf8 | CatalogType::LargeUtf8 | CatalogType::Utf8View => "string".to_owned(),
        CatalogType::Struct { .. } => context.named_type(field)?.to_owned(),
        CatalogType::List { element, .. } => {
            format!("ReadonlyArray<{}>", ts_field_type(context, element)?)
        }
        CatalogType::Map { .. } => {
            format!("ReadonlyArray<{}>", context.named_type(field)?)
        }
        CatalogType::Dictionary { .. } => {
            return Err(SdkGenError::UnsupportedType {
                path: field.name().to_owned(),
                reason: "dictionary reached TypeScript rendering".to_owned(),
            });
        }
    };
    Ok(output)
}

fn arrow_field(field: &CatalogField) -> SdkGenResult<String> {
    Ok(format!(
        "new Field({}, {}, {})",
        ts_string(field.name())?,
        arrow_type(field.mapping().arrow())?,
        field.is_nullable()
    ))
}

fn arrow_type(data_type: &CatalogType) -> SdkGenResult<String> {
    let output = match data_type {
        CatalogType::Boolean => "new Bool()".to_owned(),
        CatalogType::Int8 => "new Int8()".to_owned(),
        CatalogType::Int16 => "new Int16()".to_owned(),
        CatalogType::Int32 => "new Int32()".to_owned(),
        CatalogType::Int64 => "new Int64()".to_owned(),
        CatalogType::UInt8 => "new Uint8()".to_owned(),
        CatalogType::UInt16 => "new Uint16()".to_owned(),
        CatalogType::UInt32 => "new Uint32()".to_owned(),
        CatalogType::UInt64 => "new Uint64()".to_owned(),
        CatalogType::Float32 => "new Float32()".to_owned(),
        CatalogType::Float64 => "new Float64()".to_owned(),
        CatalogType::Timestamp { unit, timezone } => {
            let timezone = match timezone {
                Some(zone) => ts_string(zone)?,
                None => "undefined".to_owned(),
            };
            format!("new {}({timezone})", timestamp_class(*unit))
        }
        CatalogType::Date32 => "new DateDay()".to_owned(),
        CatalogType::Date64 => "new DateMillisecond()".to_owned(),
        CatalogType::Time32 { unit } => format!("new {}()", time_class(*unit)),
        CatalogType::Time64 { unit } => format!("new {}()", time_class(*unit)),
        CatalogType::Duration { unit } => format!("new {}()", duration_class(*unit)),
        CatalogType::Binary => "new Binary()".to_owned(),
        CatalogType::FixedSizeBinary { bytes } => {
            format!("new FixedSizeBinary({bytes})")
        }
        CatalogType::LargeBinary => "new LargeBinary()".to_owned(),
        CatalogType::BinaryView => "new BinaryView()".to_owned(),
        CatalogType::Utf8 => "new Utf8()".to_owned(),
        CatalogType::LargeUtf8 => "new LargeUtf8()".to_owned(),
        CatalogType::Utf8View => "new Utf8View()".to_owned(),
        CatalogType::Decimal128 { precision, scale } => {
            format!("new Decimal({scale}, {precision}, 128)")
        }
        CatalogType::Struct { fields } => format!(
            "structType([{}])",
            fields
                .iter()
                .map(arrow_field)
                .collect::<SdkGenResult<Vec<_>>>()?
                .join(", ")
        ),
        CatalogType::List { list_kind, element } => {
            let element = arrow_field(element)?;
            match list_kind {
                ListKind::List => format!("new List({element})"),
                ListKind::LargeList => format!("new LargeList({element})"),
                ListKind::FixedSize(size) => format!("new FixedSizeList({size}, {element})"),
                ListKind::ListView | ListKind::LargeListView => {
                    return Err(SdkGenError::UnsupportedType {
                        path: "list".to_owned(),
                        reason: "list view reached TypeScript rendering".to_owned(),
                    });
                }
            }
        }
        CatalogType::Map {
            key,
            value,
            keys_sorted,
        } => format!(
            "new Map_(new Field('entries', structType([{}, {}]), false), {})",
            arrow_field(key)?,
            arrow_field(value)?,
            keys_sorted
        ),
        CatalogType::Dictionary { .. } => {
            return Err(SdkGenError::UnsupportedType {
                path: "dictionary".to_owned(),
                reason: "dictionary reached TypeScript rendering".to_owned(),
            });
        }
    };
    Ok(output)
}

fn timestamp_class(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "TimestampSecond",
        TimeUnit::Millisecond => "TimestampMillisecond",
        TimeUnit::Microsecond => "TimestampMicrosecond",
        TimeUnit::Nanosecond => "TimestampNanosecond",
    }
}

fn time_class(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "TimeSecond",
        TimeUnit::Millisecond => "TimeMillisecond",
        TimeUnit::Microsecond => "TimeMicrosecond",
        TimeUnit::Nanosecond => "TimeNanosecond",
    }
}

fn duration_class(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "DurationSecond",
        TimeUnit::Millisecond => "DurationMillisecond",
        TimeUnit::Microsecond => "DurationMicrosecond",
        TimeUnit::Nanosecond => "DurationNanosecond",
    }
}

fn typescript_imports(context: &RenderContext<'_>) -> String {
    let mut imports = BTreeSet::from([
        "Data",
        "DataType",
        "DateUnit",
        "Decimal",
        "Field",
        "Int",
        "RecordBatch",
        "Schema",
        "Struct",
        "Vector",
        "makeData",
        "makeVector",
        "vectorFromArray",
    ]);
    for field in context.schema.fields() {
        collect_type_imports(field.mapping().arrow(), &mut imports);
    }
    let names = imports
        .into_iter()
        .map(|name| format!("  {name},"))
        .collect::<Vec<_>>()
        .join("\n");
    format!("\nimport {{\n{names}\n}} from 'apache-arrow';\n")
}

fn collect_type_imports<'a>(data_type: &'a CatalogType, imports: &mut BTreeSet<&'a str>) {
    match data_type {
        CatalogType::Boolean => {
            imports.insert("Bool");
        }
        CatalogType::Int8 => {
            imports.insert("Int8");
        }
        CatalogType::Int16 => {
            imports.insert("Int16");
        }
        CatalogType::Int32 => {
            imports.insert("Int32");
        }
        CatalogType::Int64 => {
            imports.insert("Int64");
        }
        CatalogType::UInt8 => {
            imports.insert("Uint8");
        }
        CatalogType::UInt16 => {
            imports.insert("Uint16");
        }
        CatalogType::UInt32 => {
            imports.insert("Uint32");
        }
        CatalogType::UInt64 => {
            imports.insert("Uint64");
        }
        CatalogType::Float32 => {
            imports.insert("Float32");
        }
        CatalogType::Float64 => {
            imports.insert("Float64");
        }
        CatalogType::Timestamp { unit, .. } => {
            imports.insert(timestamp_class(*unit));
        }
        CatalogType::Date32 => {
            imports.insert("DateDay");
        }
        CatalogType::Date64 => {
            imports.insert("DateMillisecond");
        }
        CatalogType::Time32 { unit } | CatalogType::Time64 { unit } => {
            imports.insert(time_class(*unit));
        }
        CatalogType::Duration { unit } => {
            imports.insert(duration_class(*unit));
        }
        CatalogType::Binary => {
            imports.insert("Binary");
        }
        CatalogType::FixedSizeBinary { .. } => {
            imports.insert("FixedSizeBinary");
        }
        CatalogType::LargeBinary => {
            imports.insert("LargeBinary");
        }
        CatalogType::BinaryView => {
            imports.insert("BinaryView");
        }
        CatalogType::Utf8 => {
            imports.insert("Utf8");
        }
        CatalogType::LargeUtf8 => {
            imports.insert("LargeUtf8");
        }
        CatalogType::Utf8View => {
            imports.insert("Utf8View");
        }
        CatalogType::Decimal128 { .. } => {
            imports.insert("Decimal");
        }
        CatalogType::Struct { fields } => {
            imports.insert("Struct");
            for field in fields {
                collect_type_imports(field.mapping().arrow(), imports);
            }
        }
        CatalogType::List { list_kind, element } => {
            imports.insert(match list_kind {
                ListKind::List => "List",
                ListKind::LargeList => "LargeList",
                ListKind::FixedSize(_) => "FixedSizeList",
                ListKind::ListView | ListKind::LargeListView => "List",
            });
            collect_type_imports(element.mapping().arrow(), imports);
        }
        CatalogType::Map { key, value, .. } => {
            imports.insert("Map_");
            imports.insert("Struct");
            collect_type_imports(key.mapping().arrow(), imports);
            collect_type_imports(value.mapping().arrow(), imports);
        }
        CatalogType::Dictionary { key, value, .. } => {
            collect_type_imports(key, imports);
            collect_type_imports(value, imports);
        }
    }
}

fn generated_test(context: &RenderContext<'_>) -> SdkGenResult<String> {
    let mut builder = format!("new {}()", context.builder_name);
    for field in context.schema.fields() {
        builder.push_str(&format!(
            "\n    .set{}({})",
            pascal_identifier(field.name()),
            sample_value(field)?
        ));
    }
    builder.push_str("\n    .build()");
    Ok(format!(
        "import assert from 'node:assert/strict';\n\
import test from 'node:test';\n\n\
import {{\n\
  ARROW_SCHEMA,\n\
  SCHEMA_FINGERPRINT,\n\
  SCHEMA_VERSION,\n\
  TABLE_NAME,\n\
  {builder_name},\n\
  createDoPutPlan,\n\
  recordBatch,\n\
}} from '../src/index.js';\n\n\
test('generated fixture builds exact batch and client surfaces', () => {{\n\
  const row = {builder};\n\
  const batch = recordBatch([row]);\n\
  assert.equal(batch.numRows, 1);\n\
  assert.deepEqual(batch.schema.fields.map((field) => field.name), ARROW_SCHEMA.fields.map((field) => field.name));\n\
  assert.equal(SCHEMA_VERSION, {version});\n\
  assert.match(SCHEMA_FINGERPRINT, /^sha256:[0-9a-f]{{64}}$/u);\n\
  assert.equal(TABLE_NAME, {table});\n\n\
  const plan = createDoPutPlan(batch);\n\
  assert.equal(plan.headers['x-arrowcol-table'], TABLE_NAME);\n\
  assert.deepEqual(plan.descriptorPath.slice(0, 3), [{catalog}, {schema}, TABLE_NAME]);\n\n\
}});\n",
        builder_name = context.builder_name,
        builder = builder,
        version = context.schema.version().get(),
        table = ts_string(context.schema.identity().table().as_str())?,
        catalog = ts_string(context.schema.identity().catalog().as_str())?,
        schema = ts_string(context.schema.identity().schema().as_str())?,
    ))
}

fn sample_value(field: &CatalogField) -> SdkGenResult<String> {
    let value = match field.mapping().arrow() {
        CatalogType::Boolean => "true".to_owned(),
        CatalogType::Int8
        | CatalogType::Int16
        | CatalogType::Int32
        | CatalogType::UInt8
        | CatalogType::UInt16
        | CatalogType::UInt32 => "7".to_owned(),
        CatalogType::Int64 => "4n".to_owned(),
        CatalogType::UInt64 => "8n".to_owned(),
        CatalogType::Float32 | CatalogType::Float64 => "2.5".to_owned(),
        CatalogType::Timestamp { unit, .. } => match unit {
            TimeUnit::Second => "1_700_000_000n".to_owned(),
            TimeUnit::Millisecond => "1_700_000_000_000n".to_owned(),
            TimeUnit::Microsecond => "1_700_000_000_000_000n".to_owned(),
            TimeUnit::Nanosecond => "1_700_000_000_000_000_000n".to_owned(),
        },
        CatalogType::Date32 => "20_000".to_owned(),
        CatalogType::Date64 => "1_728_000_000n".to_owned(),
        CatalogType::Time32 { .. } => "12_345".to_owned(),
        CatalogType::Time64 { .. } | CatalogType::Duration { .. } => "123_456n".to_owned(),
        CatalogType::Binary | CatalogType::LargeBinary | CatalogType::BinaryView => {
            "new Uint8Array([1, 2, 3])".to_owned()
        }
        CatalogType::FixedSizeBinary { bytes } => {
            format!("new Uint8Array({bytes}).fill(0x11)")
        }
        CatalogType::Utf8 | CatalogType::LargeUtf8 | CatalogType::Utf8View => "'sample'".to_owned(),
        CatalogType::Decimal128 { .. } => "12_345n".to_owned(),
        CatalogType::Struct { fields } => {
            let values = fields
                .iter()
                .map(|child| {
                    let value = sample_value(child)?;
                    Ok(format!("{}: {value}", ts_string(child.name())?))
                })
                .collect::<SdkGenResult<Vec<_>>>()?;
            format!("{{ {} }}", values.join(", "))
        }
        CatalogType::List { list_kind, element } => {
            let item = sample_value(element)?;
            match list_kind {
                ListKind::FixedSize(size) => {
                    let size = usize::try_from(*size).map_err(|_| SdkGenError::UnsupportedType {
                        path: field.name().to_owned(),
                        reason: "fixed-size list length must be positive".to_owned(),
                    })?;
                    let items = std::iter::repeat_n(item, size).collect::<Vec<_>>().join(", ");
                    format!("[{items}]")
                }
                _ if element.is_nullable() => format!("[{item}, null]"),
                _ => format!("[{item}]"),
            }
        }
        CatalogType::Map { key, value, .. } => {
            let key = sample_value(key)?;
            let value_sample = sample_value(value)?;
            if value.is_nullable() {
                format!("[{{ key: {key}, value: {value_sample} }}, {{ key: {key}, value: null }}]")
            } else {
                format!("[{{ key: {key}, value: {value_sample} }}]")
            }
        }
        CatalogType::Dictionary { .. } => {
            return Err(SdkGenError::UnsupportedType {
                path: field.name().to_owned(),
                reason: "dictionary reached TypeScript fixture rendering".to_owned(),
            });
        }
    };
    Ok(value)
}

fn readme(context: &RenderContext<'_>) -> String {
    format!(
        "# {}\n\n\
Generated TypeScript model and Apache Arrow helper package for `{}` version `{}`.\n\n\
```ts\n\
import {{ {}Builder, createDoPutPlan, recordBatch }} from '{}';\n\n\
const builder = new {}Builder();\n\
// Set required fields, then call `build()` and `recordBatch([row])`.\n\
```\n\n\
`createDoPutPlan` returns the exact Flight descriptor path, validated routing\n\
headers, and `RecordBatch` for integration with any Arrow Flight transport.\n",
        context.typescript_package,
        context.schema.identity(),
        context.schema.version(),
        context.row_name,
        context.typescript_package,
        context.row_name,
    )
}

fn ts_private_identifier(value: &str) -> String {
    let pascal = pascal_identifier(value);
    let mut characters = pascal.chars();
    characters.next().map_or_else(
        || "fieldValue".to_owned(),
        |first| format!("{}{}", first.to_ascii_lowercase(), characters.as_str()),
    )
}

fn ts_string(value: &str) -> SdkGenResult<String> {
    serde_json::to_string(value).map_err(SdkGenError::from)
}

fn format_error(_: std::fmt::Error) -> SdkGenError {
    SdkGenError::Artifact("generated TypeScript formatting failed".to_owned())
}
