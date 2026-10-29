use arrow::ipc::reader::StreamReader;
fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: adr-fingerprint ARROW_IPC_STREAM"))?;
    let reader = StreamReader::try_new(std::fs::File::open(path)?, None)?;
    println!("{}", arrowcol_protocol::fingerprint_schema(reader.schema().as_ref()));
    Ok(())
}
