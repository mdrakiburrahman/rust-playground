//! ArrowCol writer declarations are not standard Parquet compression-level fields.
use anyhow::{Context, Result, ensure};
use parquet::{
    basic::Compression,
    file::{
        metadata::ParquetMetaData,
        properties::{WriterProperties, WriterPropertiesBuilder},
    },
    schema::types::SchemaDescriptor,
};
use serde::Serialize;

// A valued file key-value would become user-visible Arrow Schema.metadata on read.
const MARKER: &str = "; arrowcol.writer.codec_levels.v2=";

pub(crate) fn codec(compression: Compression) -> (&'static str, Option<i64>) {
    match compression {
        Compression::UNCOMPRESSED => ("uncompressed", None),
        Compression::SNAPPY => ("snappy", None),
        Compression::GZIP(level) => ("gzip", Some(level.compression_level().into())),
        Compression::BROTLI(level) => ("brotli", Some(level.compression_level().into())),
        Compression::LZ4 => ("lz4", None),
        Compression::LZ4_RAW => ("lz4_raw", None),
        Compression::ZSTD(level) => ("zstd", Some(level.compression_level().into())),
        Compression::LZO => ("lzo", None),
    }
}

pub(crate) fn stamp(schema: &SchemaDescriptor, properties: WriterProperties) -> Result<WriterProperties> {
    let columns: Vec<_> = schema
        .columns()
        .iter()
        .map(|c| codec(properties.compression(c.path())))
        .collect();
    if columns.iter().all(|(_, level)| level.is_none()) {
        return Ok(properties);
    }
    ensure!(
        !properties.created_by().contains(MARKER),
        "reserved writer codec marker already present"
    );
    let creator = format!(
        "{}{MARKER}{}",
        properties.created_by(),
        serde_json::to_string(&columns)?
    );
    Ok(WriterPropertiesBuilder::from(properties)
        .set_created_by(creator)
        .build())
}

/// Bounded summary. Every chunk is checked, including those beyond the sample cap.
#[derive(Default, Serialize)]
pub struct FooterEvidence {
    files: usize,
    files_with_writer_declaration: usize,
    files_missing_writer_declaration: usize,
    unrelated_key_value_entries: usize,
    row_groups: usize,
    column_chunks: usize,
    samples: Vec<Chunk>,
}

#[derive(Serialize)]
struct Chunk {
    file: usize,
    row_group: usize,
    leaf: usize,
    physical_codec: &'static str,
    writer_declared_level: Option<i64>,
    writer_declaration_present: bool,
}

impl FooterEvidence {
    /// Inspect one file in deterministic inventory order. Unrelated metadata stays opaque.
    ///
    /// # Errors
    /// Rejects malformed, duplicate or physically inconsistent declarations.
    pub fn observe(&mut self, metadata: &ParquetMetaData) -> Result<()> {
        let file = metadata.file_metadata();
        let width = file.schema_descr().num_columns();
        let entries = file.key_value_metadata().map(Vec::as_slice).unwrap_or_default();
        let declared = file
            .created_by()
            .and_then(|creator| creator.split_once(MARKER))
            .map(|(_, value)| -> Result<Vec<(String, Option<i64>)>> {
                // Canonical tuples contain only a fixed codec name and an integer or null.
                ensure!(
                    value.len() <= 2 + width.saturating_mul(32),
                    "oversized writer codec declaration"
                );
                let columns: Vec<(String, Option<i64>)> = serde_json::from_str(value)
                    .ok()
                    .context("invalid writer codec declaration")?;
                ensure!(columns.len() == width, "writer codec declaration width differs");
                Ok(columns)
            })
            .transpose()?;
        let present = declared.is_some();
        self.unrelated_key_value_entries += entries.len();
        for (row_group, group) in metadata.row_groups().iter().enumerate() {
            ensure!(group.num_columns() == width, "physical footer width differs");
            for (leaf, chunk) in group.columns().iter().enumerate() {
                let (physical_codec, _) = codec(chunk.compression());
                let mut writer_declared_level = None;
                if let Some(columns) = &declared {
                    let (name, level) = &columns[leaf];
                    ensure!(name == physical_codec, "declared codec differs from physical footer");
                    let valid = match physical_codec {
                        "zstd" => level.is_some_and(|v| {
                            i32::try_from(v).is_ok_and(|n| parquet::basic::ZstdLevel::try_new(n).is_ok())
                        }),
                        "brotli" => matches!(level, Some(0..=11)),
                        "gzip" => matches!(level, Some(0..=9)),
                        _ => level.is_none(),
                    };
                    ensure!(valid, "invalid writer-declared compression level");
                    writer_declared_level = *level;
                }
                if self.samples.len() < 32 {
                    self.samples.push(Chunk {
                        file: self.files,
                        row_group,
                        leaf,
                        physical_codec,
                        writer_declared_level,
                        writer_declaration_present: present,
                    });
                }
                self.column_chunks += 1;
            }
        }
        self.row_groups += metadata.num_row_groups();
        self.files += 1;
        self.files_with_writer_declaration += usize::from(present);
        self.files_missing_writer_declaration += usize::from(!present);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    use parquet::basic::ZstdLevel;
    use std::{fs::File, sync::Arc};

    #[test]
    fn real_footers_preserve_base_level_redact_and_fail_closed() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("i", DataType::Int32, false)],
            [
                ("arrowcol.writer.codec_levels.v1".into(), "owned-by-user".into()),
                ("private-key".into(), "private-value".into()),
            ]
            .into(),
        ));
        let descriptor = parquet::arrow::ArrowSchemaConverter::new().convert(&schema)?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int32Array::from_iter_values(0..35))])?;
        let read = |creator: &str, resolve: bool| -> Result<FooterEvidence> {
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(1))
                .set_created_by(creator.to_owned())
                .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
                    "private-key".into(),
                    "private-value".to_owned(),
                )]))
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(8)?))
                .build();
            let props = if resolve { stamp(&descriptor, props)? } else { props };
            let path = dir.path().join("fixture.parquet");
            let mut writer = ArrowWriter::try_new(File::create(&path)?, Arc::clone(&schema), Some(props))?;
            writer.write(&batch)?;
            writer.close()?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
            assert!(
                reader.schema().as_ref() == schema.as_ref(),
                "decoded user schema changed"
            );
            let mut evidence = FooterEvidence::default();
            evidence.observe(reader.metadata())?;
            Ok(evidence)
        };
        let evidence = read("fixture", true)?;
        assert_eq!(evidence.files_with_writer_declaration, 1);
        assert_eq!(evidence.unrelated_key_value_entries, 2); // caller plus Arrow schema
        assert_eq!(evidence.column_chunks, 35);
        assert_eq!(evidence.samples.len(), 32);
        assert!(
            evidence
                .samples
                .iter()
                .all(|s| s.physical_codec == "zstd" && s.writer_declared_level == Some(8))
        );
        assert!(!serde_json::to_string(&evidence)?.contains("private"));
        assert_eq!(read("fixture", false)?.files_missing_writer_declaration, 1);
        for payload in [
            "private-value",
            r#"[["gzip",8]]"#,
            "[]",
            &format!(r#"[["zstd",8]]{MARKER}[["zstd",8]]"#),
        ] {
            let err = read(&format!("fixture{MARKER}{payload}"), false)
                .err()
                .context("invalid footer was accepted")?;
            assert!(!err.to_string().contains("private"));
        }
        Ok(())
    }
}
