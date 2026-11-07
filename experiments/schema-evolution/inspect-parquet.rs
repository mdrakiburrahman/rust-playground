use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
};

use arrow_array::{Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;

#[derive(Default)]
struct Counts {
    rows: u64,
    nulls: u64,
    missing: u64,
    filled: u64,
    bad_sentinel: u64,
    values: BTreeSet<i64>,
}

fn inspect(files: &[String]) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    if files.is_empty() {
        return Err("usage: inspect-parquet ACTIVE_DELTA_FILE [ACTIVE_DELTA_FILE ...]".into());
    }
    let mut counts = BTreeMap::<(String, String), Counts>::new();
    for file in files {
        for batch in ParquetRecordBatchReaderBuilder::try_new(File::open(file)?)?.build()? {
            let batch = batch?;
            let strings = |name: &str| -> Result<&StringArray, Box<dyn std::error::Error>> {
                batch
                    .column_by_name(name)
                    .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                    .ok_or_else(|| format!("missing or non-string column {name} in {file}").into())
            };
            let clients = strings("client")?;
            let phases = strings("phase")?;
            let values = batch
                .column_by_name("value")
                .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                .ok_or("missing or non-long value column")?;
            let bar = if batch.column_by_name("bar").is_some() {
                Some(strings("bar")?)
            } else {
                None
            };
            for row in 0..batch.num_rows() {
                if clients.is_null(row) || phases.is_null(row) || values.is_null(row) {
                    return Err("required fixture columns unexpectedly contain null".into());
                }
                let client = clients.value(row);
                let phase = phases.value(row);
                if !matches!(client, "old" | "new") || !matches!(phase, "before" | "after") {
                    return Err("unknown client or phase in Delta file".into());
                }
                let tally = counts.entry((client.to_owned(), phase.to_owned())).or_default();
                let value = values.value(row);
                if value < 0 || !tally.values.insert(value) {
                    return Err("negative or duplicate fixture value in an active Delta cohort".into());
                }
                tally.rows += 1;
                match bar {
                    None => {
                        tally.nulls += 1;
                        tally.missing += 1;
                    }
                    Some(column) if column.is_null(row) => tally.nulls += 1,
                    Some(column) => {
                        tally.filled += 1;
                        if column.value(row) != format!("bar-{value}") {
                            tally.bad_sentinel += 1;
                        }
                    }
                }
            }
        }
    }
    Ok(json!({
        "files": files,
        "counts": counts.iter().map(|((client, phase), counts)| json!({
            "client": client, "phase": phase, "total": counts.rows, "nulls": counts.nulls,
            "missingColumn": counts.missing, "filled": counts.filled, "badSentinel": counts.bad_sentinel,
            "values": counts.values.iter().copied().collect::<Vec<_>>()
        })).collect::<Vec<_>>()
    }))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", inspect(&std::env::args().skip(1).collect::<Vec<_>>())?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use parquet::arrow::ArrowWriter;

    use super::*;

    #[test]
    fn distinguishes_absent_historical_column_from_true_null_and_filled_sentinel()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let mut files = Vec::new();
        for (client, phase, bar) in [
            ("old", "before", None),
            ("old", "after", Some(vec![None, None])),
            ("new", "after", Some(vec![Some("bar-0"), Some("bar-1")])),
        ] {
            let mut columns: Vec<(&str, Arc<dyn Array>)> = vec![
                ("client", Arc::new(StringArray::from(vec![client; 2]))),
                ("phase", Arc::new(StringArray::from(vec![phase; 2]))),
                ("value", Arc::new(Int64Array::from(vec![0, 1]))),
            ];
            if let Some(bar) = bar {
                columns.push(("bar", Arc::new(StringArray::from(bar))));
            }
            let batch = RecordBatch::try_from_iter(columns)?;
            let filename = directory.path().join(format!("{client}-{phase}.parquet"));
            let mut writer = ArrowWriter::try_new(File::create(&filename)?, batch.schema(), None)?;
            writer.write(&batch)?;
            writer.close()?;
            files.push(filename.to_string_lossy().into_owned());
        }
        let result = inspect(&files)?;
        assert_eq!(
            result["counts"],
            json!([
                {"client":"new","phase":"after","total":2,"nulls":0,"missingColumn":0,"filled":2,"badSentinel":0,"values":[0,1]},
                {"client":"old","phase":"after","total":2,"nulls":2,"missingColumn":0,"filled":0,"badSentinel":0,"values":[0,1]},
                {"client":"old","phase":"before","total":2,"nulls":2,"missingColumn":2,"filled":0,"badSentinel":0,"values":[0,1]}
            ])
        );
        assert!(inspect(&[]).is_err());
        let mut duplicate = files.clone();
        duplicate.push(files[0].clone());
        assert!(inspect(&duplicate).is_err());
        Ok(())
    }
}
