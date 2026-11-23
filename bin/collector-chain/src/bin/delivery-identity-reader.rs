use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use arrow_array::{LargeStringArray, StringArray};
use arrow_schema::DataType;
use clap::Parser;
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    file: PathBuf,
    #[arg(long, default_value_t = 8192, value_parser = clap::value_parser!(u32).range(1..=65_536))]
    batch_rows: u32,
}

fn emit_strings<'a>(values: impl Iterator<Item = Option<&'a str>>, output: &mut impl Write) -> Result<u64> {
    let mut rows = 0_u64;
    for value in values {
        let value = value.context("null delivery identity")?;
        if value.len() > 160 || !value.is_ascii() || value.contains(['\r', '\n']) {
            bail!("delivery identity cannot be represented in the bounded line protocol");
        }
        output.write_all(value.as_bytes())?;
        output.write_all(b"\n")?;
        rows = rows.checked_add(1).context("decoded row count overflow")?;
    }
    Ok(rows)
}

fn decode(path: &Path, batch_rows: usize, output: &mut impl Write) -> Result<u64> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let fields = builder
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name() == "endpoint_url")
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if fields.len() != 1 {
        bail!("expected exactly one endpoint_url field");
    }
    if !matches!(
        builder.schema().field(fields[0]).data_type(),
        DataType::Utf8 | DataType::LargeUtf8
    ) {
        bail!("endpoint_url must be a UTF-8 string column");
    }
    let expected = u64::try_from(builder.metadata().file_metadata().num_rows())?;
    let projection = ProjectionMask::roots(builder.parquet_schema(), fields);
    let reader = builder
        .with_projection(projection)
        .with_batch_size(batch_rows)
        .build()?;
    let mut rows = 0_u64;
    for batch in reader {
        let batch = batch?;
        let column = batch.column(0);
        let decoded = if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            emit_strings(values.iter(), output)?
        } else if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
            emit_strings(values.iter(), output)?
        } else {
            bail!("unsupported decoded endpoint_url array");
        };
        rows = rows.checked_add(decoded).context("decoded row count overflow")?;
    }
    if rows != expected {
        bail!("decoded identity rows differ from Parquet footer rows: {rows} != {expected}");
    }
    Ok(rows)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut output = BufWriter::with_capacity(1_048_576, io::stdout().lock());
    let rows = decode(&args.file, args.batch_rows as usize, &mut output)?;
    output.flush()?;
    eprintln!("DELIVERY_IDENTITY_READER_COMPLETED rows={rows}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{Field, Schema};
    use parquet::arrow::ArrowWriter;

    use super::*;

    fn fixture(values: Vec<Option<&str>>) -> Result<PathBuf> {
        let path = std::env::temp_dir().join(format!("delivery-reader-{}.parquet", uuid::Uuid::new_v4()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("business_value", DataType::Int32, false),
            Field::new("endpoint_url", DataType::Utf8, true),
        ]));
        let rows = values.len();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![7; rows])),
                Arc::new(StringArray::from(values)),
            ],
        )?;
        let mut writer = ArrowWriter::try_new(File::create(&path)?, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(path)
    }

    #[test]
    fn projects_only_identity_and_decodes_all_bounded_batches() -> Result<()> {
        let path = fixture(vec![Some("first"), Some("second"), Some("third")])?;
        let mut output = Vec::new();
        let result = decode(&path, 1, &mut output);
        fs::remove_file(path)?;
        assert_eq!(result?, 3);
        assert_eq!(output, b"first\nsecond\nthird\n");
        Ok(())
    }

    #[test]
    fn rejects_nulls_and_line_injection() -> Result<()> {
        for values in [vec![None], vec![Some("first\nsecond")]] {
            let path = fixture(values)?;
            let result = decode(&path, 1, &mut Vec::new());
            fs::remove_file(path)?;
            assert!(result.is_err());
        }
        Ok(())
    }
}
