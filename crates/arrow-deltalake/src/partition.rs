use std::collections::BTreeMap;

use anyhow::{Context, Result};
use arrow::array::{RecordBatch, UInt64Array};
use delta_kernel::engine::arrow_conversion::scalar::extract_primitive_scalar;
use delta_kernel::partition::serialization::serialize_partition_value;
use sha2::{Digest, Sha256};

pub(crate) type PartitionValues = BTreeMap<String, Option<String>>;

pub(crate) fn split(batch: &RecordBatch, names: &[String]) -> Result<Vec<(RecordBatch, PartitionValues)>> {
    split_with_new_partition(batch, names, |_| Ok(()))
}

pub(crate) fn split_with_new_partition(
    batch: &RecordBatch,
    names: &[String],
    mut on_new_partition: impl FnMut(&PartitionValues) -> Result<()>,
) -> Result<Vec<(RecordBatch, PartitionValues)>> {
    let columns = names
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .with_context(|| format!("missing partition column {name}"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut groups: BTreeMap<PartitionValues, Vec<u64>> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        let mut values = BTreeMap::new();
        for (name, column) in names.iter().zip(&columns) {
            values.insert(name.clone(), value(batch.column(*column).as_ref(), row)?);
        }
        if !groups.contains_key(&values) {
            on_new_partition(&values)?;
        }
        groups.entry(values).or_default().push(u64::try_from(row)?);
    }
    groups
        .into_iter()
        .map(|(values, rows)| {
            let rows = UInt64Array::from(rows);
            let arrays = batch
                .columns()
                .iter()
                .map(|column| arrow::compute::take(column.as_ref(), &rows, None).map_err(Into::into))
                .collect::<Result<Vec<_>>>()?;
            Ok((RecordBatch::try_new(batch.schema(), arrays)?, values))
        })
        .collect()
}

pub(crate) fn value(array: &dyn arrow::array::Array, row: usize) -> Result<Option<String>> {
    Ok(serialize_partition_value(&extract_primitive_scalar(array, row)?)?)
}

pub(crate) fn publication_id(
    app_id: &str,
    values: &PartitionValues,
    schema: &arrow::datatypes::Schema,
) -> Result<String> {
    let typed = values
        .iter()
        .map(|(name, value)| Ok((name, schema.field_with_name(name)?.data_type().to_string(), value)))
        .collect::<Result<Vec<_>>>()?;
    let digest = Sha256::digest(serde_json::to_vec(&typed)?);
    Ok(format!("{app_id}.partition.{digest:x}"))
}

pub(crate) fn component(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.".contains(byte) {
            encoded.push(char::from(*byte));
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 15)]));
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn groups_actual_multi_column_values_and_nulls_without_reordering_rows() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("bucket", DataType::Int64, false),
            Field::new("row_id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a/b"),
                    None,
                    Some("a/b"),
                    Some("a/b"),
                    None,
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 1, 1, 2, 1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10, 11, 12, 13, 14])) as ArrayRef,
            ],
        )?;
        let groups = split(&batch, &["region".to_owned(), "bucket".to_owned()])?;
        assert_eq!(groups.len(), 3);
        assert_eq!(groups.iter().map(|(batch, _)| batch.num_rows()).sum::<usize>(), 5);
        let nulls = groups
            .iter()
            .find(|(_, values)| values["region"].is_none())
            .context("null partition")?;
        assert_eq!(
            nulls
                .0
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("ids")?
                .values(),
            &[11, 14]
        );
        assert_eq!(component("a/b%=x"), "a%2Fb%25%3Dx");
        assert_ne!(
            publication_id("stable", &groups[0].1, batch.schema().as_ref())?,
            publication_id("stable", &groups[1].1, batch.schema().as_ref())?
        );
        assert_eq!(
            publication_id("stable", &groups[0].1, batch.schema().as_ref())?,
            publication_id("stable", &groups[0].1, batch.schema().as_ref())?
        );
        Ok(())
    }
}
