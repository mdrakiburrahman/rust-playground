use std::io::{Read, Write};

use arrowcol_protoc::{CodeGeneratorRequest, CodeGeneratorResponse};
use prost::Message;

fn main() {
    if let Err(error) = run() {
        eprintln!("protoc-gen-arrowcol-sdk: {error}");
        std::process::exit(1);
    }
}

fn run() -> arrowcol_protoc::Result<()> {
    const LIMIT: u64 = 64 * 1024 * 1024;
    let mut input = Vec::new();
    std::io::stdin().take(LIMIT + 1).read_to_end(&mut input)?;
    if input.len() as u64 > LIMIT {
        return Err("plugin input exceeds 64 MiB".into());
    }
    let request = CodeGeneratorRequest::decode(input.as_slice())?;
    let response = arrowcol_protoc::generate(&request).unwrap_or_else(|error| CodeGeneratorResponse {
        error: Some(error.to_string()),
        file: Vec::new(),
    });
    std::io::stdout().write_all(&response.encode_to_vec())?;
    Ok(())
}
