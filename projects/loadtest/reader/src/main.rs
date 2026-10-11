use anyhow::{Context, Result};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<()> {
    let directory = std::env::args()
        .nth(1)
        .context("usage: delta-inspect <table parent directory>")?;
    println!(
        "{}",
        serde_json::to_string(&delta_inspect::inspect(Path::new(&directory)).await?)?
    );
    Ok(())
}
