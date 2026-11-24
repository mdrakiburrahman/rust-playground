use std::collections::BTreeMap;

use crate::PartitionGrouping;
use anyhow::{Context, Result};
use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use delta_kernel::engine::arrow_conversion::scalar::extract_primitive_scalar;
use delta_kernel::partition::serialization::serialize_partition_value;
use sha2::{Digest, Sha256};

pub(crate) type PartitionValues = BTreeMap<String, Option<String>>;

#[cfg(test)]
fn split(batch: &RecordBatch, names: &[String]) -> Result<Vec<(RecordBatch, PartitionValues)>> {
    split_with_new_partition(batch, names, PartitionGrouping::ScalarMap, |_| Ok(()))
}

pub(crate) fn split_with_generated_partition(
    batch: &RecordBatch,
    names: &[String],
    generated: Option<&str>,
    grouping: PartitionGrouping,
    mut on_new_partition: impl FnMut(&PartitionValues) -> Result<()>,
) -> Result<Vec<(RecordBatch, PartitionValues)>> {
    let Some(name) = generated.filter(|name| names == [*name]) else {
        return split_with_new_partition(batch, names, grouping, on_new_partition);
    };
    let column = batch
        .schema()
        .index_of(name)
        .with_context(|| format!("missing partition column {name}"))?;
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let values = BTreeMap::from([(name.to_owned(), value(batch.column(column).as_ref(), 0)?)]);
    on_new_partition(&values)?;
    Ok(vec![(batch.clone(), values)])
}

pub(crate) fn split_with_new_partition(
    batch: &RecordBatch,
    names: &[String],
    grouping: PartitionGrouping,
    on_new_partition: impl FnMut(&PartitionValues) -> Result<()>,
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
    if grouping == PartitionGrouping::TypedRuns
        && let [column] = columns.as_slice()
    {
        let array = batch.column(*column).as_any();
        if let Some(keys) = array.downcast_ref::<StringArray>() {
            return split_runs(batch, names, *column, keys.iter(), on_new_partition);
        }
        if let Some(keys) = array.downcast_ref::<Int32Array>() {
            return split_runs(batch, names, *column, keys.iter(), on_new_partition);
        }
        if let Some(keys) = array.downcast_ref::<Int64Array>() {
            return split_runs(batch, names, *column, keys.iter(), on_new_partition);
        }
    }
    scalar_tail(batch, names, &columns, 0, BTreeMap::new(), on_new_partition)
}

const MAX_TYPED_RUNS: usize = 64;
type Groups = Vec<(RecordBatch, PartitionValues)>;

fn split_runs<T: Eq>(
    batch: &RecordBatch,
    names: &[String],
    column: usize,
    keys: impl Iterator<Item = T>,
    mut admit: impl FnMut(&PartitionValues) -> Result<()>,
) -> Result<Groups> {
    let mut runs = BTreeMap::new();
    let mut keys = keys.enumerate().peekable();
    while let Some((row, key)) = keys.next() {
        let values = BTreeMap::from([(names[0].clone(), value(batch.column(column).as_ref(), row)?)]);
        if runs.len() == MAX_TYPED_RUNS || runs.contains_key(&values) {
            // Resume the scalar algorithm at this row. Prefix keys have already been
            // admitted in first-seen order; never replay their callbacks on fallback.
            let prefix = runs
                .into_iter()
                .map(|(values, range)| Ok((values, indices(range)?)))
                .collect::<Result<_>>()?;
            return scalar_tail(batch, names, &[column], row, prefix, admit);
        }
        admit(&values)?;
        while keys.next_if(|(_, next)| next == &key).is_some() {}
        let end = keys.peek().map_or(batch.num_rows(), |(index, _)| *index);
        runs.insert(values, row..end);
    }
    runs.into_iter()
        .map(|(values, range)| Ok((take_rows(batch, indices(range)?)?, values)))
        .collect()
}

fn indices(range: std::ops::Range<usize>) -> Result<Vec<u64>> {
    Ok((u64::try_from(range.start)?..u64::try_from(range.end)?).collect())
}

fn scalar_tail(
    batch: &RecordBatch,
    names: &[String],
    columns: &[usize],
    start: usize,
    mut groups: BTreeMap<PartitionValues, Vec<u64>>,
    mut on_new_partition: impl FnMut(&PartitionValues) -> Result<()>,
) -> Result<Groups> {
    for row in start..batch.num_rows() {
        let mut values = BTreeMap::new();
        for (name, column) in names.iter().zip(columns) {
            values.insert(name.clone(), value(batch.column(*column).as_ref(), row)?);
        }
        if !groups.contains_key(&values) {
            on_new_partition(&values)?;
        }
        groups.entry(values).or_default().push(u64::try_from(row)?);
    }
    groups
        .into_iter()
        .map(|(values, rows)| Ok((take_rows(batch, rows)?, values)))
        .collect()
}

// Use the same materialization for either grouping mode. A small partition must
// not pin its parent batch or change the exact downstream sorted-file charge.
fn take_rows(batch: &RecordBatch, rows: Vec<u64>) -> Result<RecordBatch> {
    let rows = UInt64Array::from(rows);
    let arrays = batch
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column.as_ref(), &rows, None))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), arrays)?)
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
    fn typed_runs_match_scalar_groups_admission_and_owned_size() -> Result<()> {
        use arrow::array::BooleanArray;
        let keys: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("z/x"), Some("z/x"), None, None, Some("")])),
            Arc::new(StringArray::from(vec![Some("z"), None, Some("z"), Some("a")])),
            Arc::new(Int64Array::from(vec![None, None, Some(-1), Some(11)])),
            Arc::new(Int32Array::from_iter_values(0..67)),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(true)])),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
        ];
        for key in keys {
            let payload = Arc::new(Int64Array::from_iter_values(0..i64::try_from(key.len())?));
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", key.data_type().clone(), true),
                Field::new("row", DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(schema, vec![key, payload])?;
            let names = ["k".to_owned()];
            for limit in [usize::MAX, 1, MAX_TYPED_RUNS] {
                let run = |mode| {
                    let mut seen = Vec::new();
                    let result = split_with_new_partition(&batch, &names, mode, |values| {
                        seen.push(values.clone());
                        anyhow::ensure!(seen.len() <= limit, "partition admission denied");
                        Ok(())
                    });
                    (result.map_err(|error| error.to_string()), seen)
                };
                let (scalar, admitted) = run(PartitionGrouping::ScalarMap);
                let (typed, registered) = run(PartitionGrouping::TypedRuns);
                assert_eq!(registered, admitted);
                assert_eq!(typed, scalar);
                if let (Ok(typed), Ok(scalar)) = (typed, scalar) {
                    for ((group, key), (reference, reference_key)) in typed.iter().zip(&scalar) {
                        assert_eq!(group.get_array_memory_size(), reference.get_array_memory_size());
                        assert_eq!(
                            publication_id("app", key, &batch.schema())?,
                            publication_id("app", reference_key, &batch.schema())?
                        );
                        let rows = group.column(1).as_any().downcast_ref::<Int64Array>().context("rows")?;
                        let original = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .context("input rows")?;
                        assert!(!original.values().as_ptr_range().contains(&rows.values().as_ptr()));
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn generated_partition_reuses_input_and_preserves_grouping() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("partition", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("one"), Some("one"), Some("one")])),
                Arc::new(Int64Array::from(vec![3, 1, 2])),
            ],
        )?;
        let names = ["partition".to_owned()];
        let expected = split(&batch, &names)?;
        let mut registrations = Vec::new();
        let actual = split_with_generated_partition(
            &batch,
            &names,
            Some("partition"),
            PartitionGrouping::ScalarMap,
            |values| {
                registrations.push(values.clone());
                Ok(())
            },
        )?;
        assert_eq!(actual, expected);
        assert_eq!(registrations, vec![expected[0].1.clone()]);
        assert!(Arc::ptr_eq(actual[0].0.column(1), batch.column(1)));
        assert!(
            split_with_generated_partition(
                &batch.slice(0, 0),
                &names,
                Some("partition"),
                PartitionGrouping::ScalarMap,
                |_| { anyhow::bail!("unexpected empty-partition callback") }
            )?
            .is_empty()
        );
        assert!(
            split_with_generated_partition(&batch, &names, Some("partition"), PartitionGrouping::ScalarMap, |_| {
                anyhow::bail!("registration failed")
            })
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn unproven_and_multi_column_partitions_use_full_grouping() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("partition", DataType::Utf8, true),
            Field::new("bucket", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), None, Some("a")])),
                Arc::new(Int64Array::from(vec![1, 2, 1])),
            ],
        )?;
        for grouping in [PartitionGrouping::ScalarMap, PartitionGrouping::TypedRuns] {
            let names = ["partition".to_owned()];
            assert_eq!(
                split_with_generated_partition(&batch, &names, None, grouping, |_| Ok(()))?,
                split(&batch, &names)?
            );
            let rejected = split_with_generated_partition(&batch, &names, None, grouping, |values| {
                anyhow::ensure!(
                    values["partition"].as_deref() == Some("a"),
                    "mismatched admission stamp"
                );
                Ok(())
            });
            assert_eq!(
                rejected.err().context("later row was not validated")?.to_string(),
                "mismatched admission stamp"
            );
            let names = ["partition".to_owned(), "bucket".to_owned()];
            assert_eq!(
                split_with_generated_partition(&batch, &names, Some("bucket"), grouping, |_| Ok(()))?,
                split(&batch, &names)?
            );
        }
        Ok(())
    }

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
