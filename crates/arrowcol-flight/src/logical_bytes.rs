use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::DataType;

/// Errors while measuring the canonical flat Arrow payload.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum CanonicalLogicalBytesError {
    /// The batch contains a type outside the flat benchmark accounting contract.
    #[error("unsupported data type for canonical logical-byte accounting: {0}")]
    UnsupportedDataType(String),
    /// An offset buffer is inconsistent with its UTF-8 array.
    #[error("invalid UTF-8 offsets for canonical logical-byte accounting")]
    InvalidUtf8Offsets,
    /// A length multiplication or byte total exceeded `u64`.
    #[error("canonical logical byte count overflow")]
    Overflow,
}

/// Count canonical logical bytes for each supported flat column, independent of backing buffers.
///
/// Booleans count packed values, UTF-8 counts visible payload and rebased 32-bit offsets, and
/// fixed-width values count their logical width. A validity bitmap counts only when the column
/// contains nulls. This is a qualification metric, not a memory-residency or Flight-wire size.
///
/// # Errors
/// Returns [`CanonicalLogicalBytesError::UnsupportedDataType`] for nested, dictionary, binary,
/// large-offset, or other unsupported types, and [`CanonicalLogicalBytesError::Overflow`] if the
/// checked byte total cannot fit in `u64`.
pub fn canonical_record_batch_logical_bytes(batch: &RecordBatch) -> Result<u64, CanonicalLogicalBytesError> {
    batch.columns().iter().try_fold(0_u64, |total, column| {
        let len = u64::try_from(column.len()).map_err(|_| CanonicalLogicalBytesError::Overflow)?;
        let value_bytes = match column.data_type() {
            DataType::Boolean => packed_bytes(len)?,
            DataType::Utf8 => {
                let strings = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or(CanonicalLogicalBytesError::InvalidUtf8Offsets)?;
                let offsets = strings.value_offsets();
                let start = offsets
                    .first()
                    .copied()
                    .ok_or(CanonicalLogicalBytesError::InvalidUtf8Offsets)?;
                let end = offsets
                    .last()
                    .copied()
                    .ok_or(CanonicalLogicalBytesError::InvalidUtf8Offsets)?;
                let payload = end
                    .checked_sub(start)
                    .filter(|payload| *payload >= 0)
                    .ok_or(CanonicalLogicalBytesError::InvalidUtf8Offsets)?;
                let offsets = len
                    .checked_add(1)
                    .and_then(|offsets| offsets.checked_mul(4))
                    .ok_or(CanonicalLogicalBytesError::Overflow)?;
                let payload = u64::try_from(payload).map_err(|_| CanonicalLogicalBytesError::InvalidUtf8Offsets)?;
                offsets
                    .checked_add(payload)
                    .ok_or(CanonicalLogicalBytesError::Overflow)?
            }
            data_type => {
                let byte_width = match data_type {
                    DataType::FixedSizeBinary(width) => usize::try_from(*width).ok(),
                    _ => data_type.primitive_width(),
                }
                .ok_or_else(|| CanonicalLogicalBytesError::UnsupportedDataType(data_type.to_string()))?;
                len.checked_mul(u64::try_from(byte_width).map_err(|_| CanonicalLogicalBytesError::Overflow)?)
                    .ok_or(CanonicalLogicalBytesError::Overflow)?
            }
        };
        let validity_bytes = if column.null_count() == 0 {
            0
        } else {
            packed_bytes(len)?
        };
        total
            .checked_add(value_bytes)
            .and_then(|total| total.checked_add(validity_bytes))
            .ok_or(CanonicalLogicalBytesError::Overflow)
    })
}

fn packed_bytes(len: u64) -> Result<u64, CanonicalLogicalBytesError> {
    len.checked_add(7)
        .map(|bits| bits / 8)
        .ok_or(CanonicalLogicalBytesError::Overflow)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use anyhow::Context;
    use arrow_array::{ArrayRef, BooleanArray, Int32Array};
    use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
    use arrow_schema::{Field, Schema};

    use super::*;

    #[test]
    fn sliced_columns_match_their_ipc_round_trip() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("number", DataType::Int32, true),
            Field::new("flag", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("prefix"), Some("a"), None, Some("suffix")])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])),
                Arc::new(BooleanArray::from(vec![Some(false), Some(true), None, Some(false)])),
            ],
        )
        .context("record batch")?;
        let sliced = batch.slice(1, 2);
        let measured = canonical_record_batch_logical_bytes(&sliced).context("canonical bytes")?;
        assert_eq!(measured, 25);

        let mut encoded = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut encoded, sliced.schema().as_ref()).context("writer")?;
            writer.write(&sliced).context("write batch")?;
            writer.finish().context("finish stream")?;
        }
        let decoded = StreamReader::try_new(Cursor::new(encoded), None)
            .context("reader")?
            .next()
            .context("batch present")?
            .context("decode batch")?;
        assert_eq!(canonical_record_batch_logical_bytes(&decoded), Ok(measured));
        Ok(())
    }

    #[test]
    fn validity_is_counted_only_when_nulls_exist_and_empty_utf8_has_one_offset() -> anyhow::Result<()> {
        let values = Int32Array::from(vec![Some(1), None, Some(3)]);
        let all_valid_with_bitmap = values.slice(0, 1);
        let all_valid_array = all_valid_with_bitmap
            .as_any()
            .downcast_ref::<Int32Array>()
            .context("int32 slice")?;
        assert_eq!(all_valid_array.null_count(), 0);
        assert!(all_valid_array.nulls().is_some());

        let empty_text = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("empty_text", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef],
        )
        .context("empty record batch")?;
        assert_eq!(canonical_record_batch_logical_bytes(&empty_text), Ok(4));

        let all_valid = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("all_valid", DataType::Int32, false)])),
            vec![Arc::new(all_valid_with_bitmap)],
        )
        .context("all-valid record batch")?;
        assert_eq!(canonical_record_batch_logical_bytes(&all_valid), Ok(4));
        Ok(())
    }

    #[test]
    fn canonical_flat_byte_vectors_include_only_logical_payload_and_present_nulls() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("number", DataType::Int32, true),
            Field::new("flag", DataType::Boolean, true),
        ]));
        let non_null = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "bb", "ccc"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
            ],
        )
        .context("non-null record batch")?;
        assert_eq!(canonical_record_batch_logical_bytes(&non_null), Ok(35));

        let nullable = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), None, Some("ccc")])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            ],
        )
        .context("nullable record batch")?;
        assert_eq!(canonical_record_batch_logical_bytes(&nullable), Ok(36));
        Ok(())
    }

    #[test]
    fn unsupported_nested_or_binary_columns_fail_measurement() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("binary", DataType::Binary, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::BinaryArray::from(vec![&b"value"[..]])) as ArrayRef],
        )
        .context("record batch")?;
        assert!(matches!(
            canonical_record_batch_logical_bytes(&batch),
            Err(CanonicalLogicalBytesError::UnsupportedDataType(_))
        ));
        Ok(())
    }

    #[test]
    fn packed_byte_rounding_is_checked() {
        assert_eq!(packed_bytes(0), Ok(0));
        assert_eq!(packed_bytes(1), Ok(1));
        assert_eq!(packed_bytes(8), Ok(1));
        assert_eq!(packed_bytes(9), Ok(2));
        assert_eq!(packed_bytes(u64::MAX), Err(CanonicalLogicalBytesError::Overflow));
    }
}
