//! Scalar subset of arrowcol-arrow-schema-sha256-v1. Unsupported types fail closed.
use crate::{Error, Result};
use arrow_schema::{DataType, Schema, TimeUnit};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub(crate) fn fingerprint(schema: &Schema) -> Result<String> {
    let mut encoder = Encoder(Sha256::new());
    encoder.text("arrowcol-arrow-schema-sha256-v1");
    encoder.len(schema.fields().len());
    for field in schema.fields() {
        encoder.bytes([0xf0]);
        encoder.text(field.name());
        encoder.bytes([u8::from(field.is_nullable())]);
        encoder.bytes([match field.dict_is_ordered() {
            None => 0,
            Some(false) => 1,
            Some(true) => 2,
        }]);
        encoder.data_type(field.data_type())?;
        encoder.metadata(field.metadata());
        encoder.bytes([0xf1]);
    }
    encoder.metadata(schema.metadata());
    Ok(format!("sha256:{:x}", encoder.0.finalize()))
}

struct Encoder(Sha256);
impl Encoder {
    fn bytes(&mut self, value: impl AsRef<[u8]>) {
        self.0.update(value);
    }
    fn len(&mut self, value: usize) {
        self.bytes(format!("{value}:"));
    }
    fn text(&mut self, value: &str) {
        self.len(value.len());
        self.bytes(value);
    }
    fn metadata(&mut self, values: &HashMap<String, String>) {
        let mut entries: Vec<_> = values.iter().collect();
        entries.sort_unstable_by_key(|(key, _)| *key);
        self.len(entries.len());
        for (key, value) in entries {
            self.text(key);
            self.text(value);
        }
    }
    fn unit(&mut self, unit: TimeUnit) {
        self.bytes([match unit {
            TimeUnit::Second => 0,
            TimeUnit::Millisecond => 1,
            TimeUnit::Microsecond => 2,
            TimeUnit::Nanosecond => 3,
        }]);
    }
    fn data_type(&mut self, ty: &DataType) -> Result<()> {
        use DataType::*;
        let tag = match ty {
            Boolean => 1,
            Int8 => 2,
            Int16 => 3,
            Int32 => 4,
            Int64 => 5,
            UInt8 => 6,
            UInt16 => 7,
            UInt32 => 8,
            UInt64 => 9,
            Float32 => 11,
            Float64 => 12,
            Timestamp(..) => 13,
            Date32 => 14,
            Date64 => 15,
            Time32(_) => 16,
            Time64(_) => 17,
            Duration(_) => 18,
            Binary => 20,
            FixedSizeBinary(_) => 21,
            LargeBinary => 22,
            BinaryView => 23,
            Utf8 => 24,
            LargeUtf8 => 25,
            Utf8View => 26,
            Decimal128(..) => 37,
            _ => {
                return Err(Error::invalid(format!(
                    "unsupported Arrow type: {ty} (only flat scalars are supported)"
                )));
            }
        };
        self.bytes([tag]);
        match ty {
            Timestamp(unit, timezone) => {
                self.unit(*unit);
                self.bytes([u8::from(timezone.is_some())]);
                if let Some(timezone) = timezone {
                    self.text(timezone);
                }
            }
            Time32(unit) | Time64(unit) | Duration(unit) => self.unit(*unit),
            FixedSizeBinary(width) => self.bytes(width.to_le_bytes()),
            Decimal128(precision, scale) => {
                self.bytes([*precision]);
                self.bytes(scale.to_le_bytes());
            }
            _ => {}
        }
        Ok(())
    }
}
