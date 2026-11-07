use std::{collections::BTreeMap, fs::File};

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
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = std::env::args().skip(1).collect::<Vec<_>>();
    if files.is_empty() {
        return Err("usage: inspect-parquet ACTIVE_DELTA_FILE [ACTIVE_DELTA_FILE ...]".into());
    }
    let mut counts = BTreeMap::<(String, String), Counts>::new();
    for file in &files {
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
                tally.rows += 1;
                match bar {
                    None => {
                        tally.nulls += 1;
                        tally.missing += 1;
                    }
                    Some(column) if column.is_null(row) => tally.nulls += 1,
                    Some(column) => {
                        tally.filled += 1;
                        if column.value(row) != format!("bar-{}", values.value(row)) {
                            tally.bad_sentinel += 1;
                        }
                    }
                }
            }
        }
    }
    println!(
        "{}",
        json!({
            "files": files,
            "counts": counts.iter().map(|((client, phase), counts)| json!({
                "client": client, "phase": phase, "total": counts.rows, "nulls": counts.nulls,
                "missingColumn": counts.missing, "filled": counts.filled, "badSentinel": counts.bad_sentinel
            })).collect::<Vec<_>>()
        })
    );
    Ok(())
}
