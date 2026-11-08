#[path = "../dmv.rs"]
mod dmv;

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
}

fn main() -> Result<()> {
    let args = Args::parse();
    let runtime = tokio::runtime::Runtime::new()?;
    let table_count = if let Some(table) = &args.table {
        runtime.block_on(dmv::send_table_with_concurrency(
            &args.endpoint,
            &args.client_id,
            table,
            args.rows,
            args.concurrency,
        ))?
    } else if args.concurrency == 1 {
        runtime.block_on(dmv::send(&args.endpoint, &args.client_id, args.rows))?
    } else {
        runtime.block_on(dmv::send_with_concurrency(
            &args.endpoint,
            &args.client_id,
            args.rows,
            args.concurrency,
        ))?
    };
    println!(
        "uploaded {table_count} DMV tables with {} rows each using concurrency {}",
        args.rows, args.concurrency
    );
    Ok(())
}
