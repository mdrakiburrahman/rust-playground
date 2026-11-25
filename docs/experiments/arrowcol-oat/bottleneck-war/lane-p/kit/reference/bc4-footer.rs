use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{error::Error, fs::File};

fn main() -> Result<(), Box<dyn Error>> {
    let p = std::env::args().nth(1).ok_or("expected a Parquet file")?;
    let f = File::open(&p)?;
    let bytes = f.metadata()?.len();
    let reader = SerializedFileReader::new(f)?;
    let meta = reader.metadata();
    let groups = meta.num_row_groups();
    let rows = meta.file_metadata().num_rows();
    let mut summed_rows = 0;
    let mut cols = Vec::new();
    for rg in meta.row_groups() {
        summed_rows += rg.num_rows();
        for c in rg.columns() {
            cols.push(serde_json::json!({
                "path": c.column_path().string(),
                "physical": format!("{:?}", c.column_type()),
                "codec": format!("{:?}", c.compression()),
                "encodings": format!("{:?}", c.encodings().collect::<Vec<_>>()),
                "dictionary":c.dictionary_page_offset().is_some(),
                "stats":c.statistics().is_some(),
                "bloom":c.bloom_filter_offset().is_some(),
                "bytes":c.compressed_size(), "uncompressed":c.uncompressed_size(),
                "values":c.num_values(),
            }));
        }
    }
    if rows <= 0 || summed_rows != rows { return Err("footer row count mismatch".into()); }
    println!("{}",serde_json::json!({"rows":rows,"bytes":bytes,"row_groups":groups,"columns":cols}));
    Ok(())
}
