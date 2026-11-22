use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, Int32Array, ListArray, RecordBatch, StringArray};
use arrow::compute::{SortOptions, concat_batches, sort_to_indices, take};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow_deltalake::layout::{FileSortBuffer, LayoutPolicy};

#[test]
fn identity_permutation_preserves_nulls_nested_values_and_metadata() -> Result<()> {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("customer_id", DataType::Utf8, true),
            Field::new("payload", DataType::Int32, true),
            Field::new_list("nested", Field::new_list_field(DataType::Int32, true), true),
        ],
        HashMap::from([("source".into(), "fixture".into())]),
    ));
    let input = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![Some("a"), Some("a"), None, None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(3), None, Some(1), Some(2)])),
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(vec![Some(1), None]),
                None,
                Some(vec![]),
                Some(vec![Some(4)]),
            ])),
        ],
    )?;
    let batches = vec![input.slice(0, 2), input.slice(2, 2)];
    let combined = concat_batches(&schema, &batches)?;
    let indices = sort_to_indices(
        combined.column(0),
        Some(SortOptions {
            descending: false,
            nulls_first: false,
        }),
        None,
    )?;
    assert!(
        indices
            .values()
            .iter()
            .enumerate()
            .all(|(i, value)| *value as usize == i)
    );
    let expected = RecordBatch::try_new(
        Arc::clone(&schema),
        combined
            .columns()
            .iter()
            .map(|column| take(column, &indices, None))
            .collect::<arrow::error::Result<Vec<_>>>()?,
    )?;
    let mut buffer = FileSortBuffer::new(
        schema,
        LayoutPolicy {
            bloom_columns: vec![],
            ..LayoutPolicy::default()
        },
    )?;
    for batch in &batches {
        assert!(buffer.try_push(batch)?);
    }
    let actual = buffer.finish()?;
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual, expected);
    Ok(())
}
