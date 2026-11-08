use collector_chain::dmv;

use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[command(about = "Generate deterministic DMV Arrow batches and upload them to ArrowCol")]
struct Args {
    #[arg(long)]
    endpoint: String,
    #[arg(long, default_value = "oat-benchmark")]
    client_id: String,
    #[arg(long, default_value_t = 1_000)]
    rows: usize,
    #[arg(long, default_value_t = 4)]
    concurrency: usize,
    #[arg(long)]
    table: Option<String>,
    #[arg(long)]
    edge_secret_file: Option<std::path::PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let runtime = tokio::runtime::Runtime::new()?;
    let table_count = runtime.block_on(dmv::send_authenticated(
        &args.endpoint,
        &args.client_id,
        args.table.as_deref(),
        args.rows,
        args.concurrency,
        args.edge_secret_file.as_deref(),
    ))?;
    println!(
        "uploaded {table_count} DMV tables with {} rows each using concurrency {}",
        args.rows, args.concurrency
    );
    Ok(())
}
