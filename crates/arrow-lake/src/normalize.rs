//! Explicit, lossless Arrow-to-Delta representation rules.

use anyhow::{Context, Result, bail};
use arrow::array::{Array, ArrayRef, Int64Array, ListArray, MapArray, RecordBatch, StructArray, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use std::sync::Arc;

/// Metadata key recording the original logical Arrow type.
pub const ORIGINAL_TYPE: &str = "arrow_lake.original_type";
/// Reserved suffix for an exact epoch-nanosecond timestamp companion.
pub const NANOS_SUFFIX: &str = "__unix_nanos";

fn target_type(ty: &DataType) -> Result<DataType> {
    Ok(match ty {
        DataType::Dictionary(_, value) => target_type(value)?,
        DataType::UInt8 => DataType::Int16,
        DataType::UInt16 => DataType::Int32,
        DataType::UInt32 => DataType::Int64,
        DataType::UInt64 => DataType::Decimal128(20, 0),
        DataType::FixedSizeBinary(_) => DataType::Binary,
        DataType::Duration(_) => DataType::Int64,
        DataType::Timestamp(_, _) => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Struct(fields) => DataType::Struct(normalized_fields(fields)?),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            DataType::List(Arc::new(list_element(field)?))
        }
        DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        DataType::Map(field, _) => DataType::Map(Arc::new(map_entries(field)?), false),
        DataType::Null
        | DataType::Union(_, _)
        | DataType::Interval(_)
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Float16
        | DataType::Decimal256(_, _)
        | DataType::RunEndEncoded(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => bail!("unsupported Arrow type {ty}"),
        other => other.clone(),
    })
}

fn logical_type(ty: &DataType) -> DataType {
    let fields = |fields: &Fields| -> Fields {
        fields
            .iter()
            .map(|field| Arc::new(field.as_ref().clone().with_data_type(logical_type(field.data_type()))))
            .collect()
    };
    let field = |field: &Arc<Field>| Arc::new(field.as_ref().clone().with_data_type(logical_type(field.data_type())));
    match ty {
        DataType::Dictionary(_, value) => logical_type(value),
        DataType::Struct(value) => DataType::Struct(fields(value)),
        DataType::List(value) => DataType::List(field(value)),
        DataType::LargeList(value) => DataType::LargeList(field(value)),
        DataType::FixedSizeList(value, size) => DataType::FixedSizeList(field(value), *size),
        DataType::Map(value, sorted) => DataType::Map(field(value), *sorted),
        other => other.clone(),
    }
}

fn list_element(field: &Field) -> Result<Field> {
    if timestamp_unit(field.data_type()).is_some() {
        bail!("timestamp list elements require an explicit struct with a timestamp field");
    }
    // Delta arrays/maps have no element field metadata. Keep the logical
    // container type on the enclosing field instead, which Delta persists.
    Ok(Field::new(
        "element",
        target_type(field.data_type())?,
        field.is_nullable(),
    ))
}

fn map_entries(field: &Field) -> Result<Field> {
    let DataType::Struct(fields) = field.data_type() else {
        bail!("map entries must be a struct");
    };
    if fields.len() != 2 || fields.iter().any(|field| timestamp_unit(field.data_type()).is_some()) {
        bail!("map needs two key/value fields; bare timestamps require an explicit struct");
    }
    let fields = vec![
        Field::new("key", target_type(fields[0].data_type())?, false),
        Field::new("value", target_type(fields[1].data_type())?, fields[1].is_nullable()),
    ];
    Ok(Field::new("key_value", DataType::Struct(fields.into()), false))
}

fn normalized_field(field: &Field) -> Result<Field> {
    let ty = target_type(field.data_type())?;
    let mut metadata = field.metadata().clone();
    let logical = logical_type(field.data_type());
    if ty != logical && !matches!(logical, DataType::Struct(_)) {
        metadata.insert(ORIGINAL_TYPE.into(), logical.to_string());
    }
    Ok(Field::new(field.name(), ty, field.is_nullable()).with_metadata(metadata))
}

fn normalized_fields(fields: &Fields) -> Result<Fields> {
    let mut output = Vec::new();
    for field in fields {
        if field.name().ends_with(NANOS_SUFFIX) {
            bail!("reserved timestamp companion name {}", field.name());
        }
        output.push(Arc::new(normalized_field(field)?));
        if timestamp_unit(field.data_type()).is_some() {
            output.push(Arc::new(Field::new(
                format!("{}{NANOS_SUFFIX}", field.name()),
                DataType::Int64,
                field.is_nullable(),
            )));
        }
    }
    Ok(output.into())
}

/// Convert an input schema to its deterministic Delta-compatible representation.
///
/// # Errors
/// Rejects unsupported types and reserved companion names.
pub fn schema(schema: &Schema) -> Result<SchemaRef> {
    Ok(Arc::new(
        Schema::new(normalized_fields(schema.fields())?).with_metadata(schema.metadata().clone()),
    ))
}

fn timestamp_unit(ty: &DataType) -> Option<&TimeUnit> {
    match ty {
        DataType::Timestamp(unit, _) => Some(unit),
        DataType::Dictionary(_, value) => timestamp_unit(value),
        _ => None,
    }
}

fn normalized_columns(fields: &Fields, columns: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
    let mut output = Vec::new();
    for (field, column) in fields.iter().zip(columns) {
        if let Some(unit) = timestamp_unit(field.data_type()) {
            let decoded = match column.data_type() {
                DataType::Dictionary(_, value) => cast(column, value)?,
                _ => Arc::clone(column),
            };
            let integers = cast(&decoded, &DataType::Int64)?;
            let values = integers
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("timestamp integer representation")?;
            let multiplier: i64 = match unit {
                TimeUnit::Second => 1_000_000_000,
                TimeUnit::Millisecond => 1_000_000,
                TimeUnit::Microsecond => 1_000,
                TimeUnit::Nanosecond => 1,
            };
            let nanos = values
                .iter()
                .map(|v| {
                    v.map(|v| v.checked_mul(multiplier).context("epoch nanoseconds overflow"))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            let micros = Int64Array::from(nanos.iter().map(|v| v.map(|n| n.div_euclid(1_000))).collect::<Vec<_>>());
            output.push(cast(
                &micros,
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            )?);
            output.push(Arc::new(Int64Array::from(nanos)));
        } else {
            output.push(normalized_array(column)?);
        }

        fn normalized_array(column: &ArrayRef) -> Result<ArrayRef> {
            match column.data_type() {
                DataType::Dictionary(_, value) => normalized_array(&cast(column, value)?),
                DataType::Struct(fields) => {
                    let value = column
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .context("expected struct")?;
                    Ok(Arc::new(StructArray::try_new(
                        normalized_fields(fields)?,
                        normalized_columns(fields, value.columns())?,
                        value.nulls().cloned(),
                    )?))
                }
                DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
                    if timestamp_unit(field.data_type()).is_some() {
                        bail!("timestamp list elements require an explicit struct with a timestamp field");
                    }
                    let source = cast(column, &DataType::List(Arc::clone(field)))?;
                    let list = source.as_any().downcast_ref::<ListArray>().context("expected list")?;
                    let target = Arc::new(list_element(field)?);
                    Ok(Arc::new(ListArray::try_new(
                        target,
                        list.offsets().clone(),
                        normalized_array(list.values())?,
                        list.nulls().cloned(),
                    )?))
                }
                DataType::Map(field, _) => {
                    let map = column.as_any().downcast_ref::<MapArray>().context("expected map")?;
                    if map
                        .entries()
                        .fields()
                        .iter()
                        .any(|field| timestamp_unit(field.data_type()).is_some())
                    {
                        bail!("timestamp map keys/values require an explicit struct");
                    }
                    let target = map_entries(field)?;
                    let DataType::Struct(fields) = target.data_type() else {
                        bail!("normalized map entries must be a struct");
                    };
                    let entries = StructArray::try_new(
                        fields.clone(),
                        normalized_columns(map.entries().fields(), map.entries().columns())?,
                        map.entries().nulls().cloned(),
                    )?;
                    Ok(Arc::new(MapArray::try_new(
                        Arc::new(target),
                        map.offsets().clone(),
                        entries,
                        map.nulls().cloned(),
                        false,
                    )?))
                }
                ty => Ok(cast(column, &target_type(ty)?)?),
            }
        }
    }
    Ok(output)
}

/// Normalize a batch without losing unsigned values, binary bytes, or timestamp precision.
///
/// # Errors
/// Returns unsupported-type, overflow, conversion, and schema errors.
pub fn batch(batch: &RecordBatch) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema(batch.schema_ref())?,
        normalized_columns(batch.schema_ref().fields(), batch.columns())?,
    )
    .context("normalize Arrow batch for Delta")
}

/// Align columns to a table schema, filling only absent nullable columns.
///
/// # Errors
/// Rejects type changes, nested additions, and absent required fields.
pub fn align(batch: &RecordBatch, target: SchemaRef) -> Result<RecordBatch> {
    let mut columns = Vec::new();
    for field in target.fields() {
        match batch.schema_ref().fields().find(field.name()) {
            Some((index, incoming)) => {
                if incoming.data_type() != field.data_type() {
                    bail!(
                        "incompatible type for {}: {} vs {}",
                        field.name(),
                        incoming.data_type(),
                        field.data_type()
                    );
                }
                if incoming.metadata().get(ORIGINAL_TYPE) != field.metadata().get(ORIGINAL_TYPE) {
                    bail!("incompatible logical Arrow type for {}", field.name());
                }
                let column = Arc::clone(batch.column(index));
                if !field.is_nullable() && column.null_count() > 0 {
                    bail!("null in required column {}", field.name());
                }
                columns.push(column);
            }
            None if field.is_nullable() => columns.push(new_null_array(field.data_type(), batch.num_rows())),
            None => bail!("missing required column {}", field.name()),
        }
    }
    for field in batch.schema_ref().fields() {
        if target.fields().find(field.name()).is_none() {
            bail!("column {} not present in canonical schema", field.name());
        }
    }
    Ok(RecordBatch::try_new(target, columns)?)
}
