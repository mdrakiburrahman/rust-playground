//! Fixed local component case. Run the release executable with `--result PATH`,
//! where PATH must not exist; its JSON is separate from application stdout.
//! Accept the result only on exit zero; a failed run may leave an empty file.
//! Timings include input generation through durable receipts, not DFE ACKs.
//! The byte bound covers retained Arrow inputs, not total process memory.

#[path = "../tests/darwin_commit_pipeline.rs"]
mod fixture;

use std::io::Write;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args_os().skip(1);
    anyhow::ensure!(
        args.next().as_deref() == Some(std::ffi::OsStr::new("--result")),
        "expected --result PATH"
    );
    let path = args.next().ok_or_else(|| anyhow::anyhow!("missing result path"))?;
    anyhow::ensure!(args.next().is_none(), "unexpected arguments");
    let mut output = std::fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    let result = fixture::continuous_component().await?;
    serde_json::to_writer(&mut output, &result)?;
    writeln!(output)?;
    output.sync_all()?;
    Ok(())
}
