use arrow_array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).ok_or("expected committed-files.json")?;
    let files: Vec<String> = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut rows = Vec::new();
    for file in files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(file)?)?.build()?;
        for batch in reader {
            let batch = batch?;
            let ids = batch.column_by_name("event_id").ok_or("missing event_id")?.as_any()
                .downcast_ref::<Int64Array>().ok_or("wrong event_id type")?;
            let times = batch.column_by_name("observed_at").ok_or("missing observed_at")?.as_any()
                .downcast_ref::<TimestampMicrosecondArray>().ok_or("wrong timestamp type")?;
            let payloads = batch.column_by_name("payload").ok_or("missing payload")?.as_any()
                .downcast_ref::<StringArray>().ok_or("wrong payload type")?;
            for index in 0..batch.num_rows() {
                rows.push((ids.value(index), times.value(index),
                    (!payloads.is_null(index)).then(|| payloads.value(index).to_owned())));
            }
        }
    }
    rows.sort();
    println!("{}", serde_json::to_string(&rows)?);
    Ok(())
}
