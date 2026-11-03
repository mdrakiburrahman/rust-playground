mod dmv;
mod otlp;

use anyhow::Result;
use clap::Parser;
use serde_json::json;
use uuid::Uuid;

/// Sends DMV Arrow batches and OTLP signals tagged with a freshly generated client ID.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Middle ArrowCol Flight endpoint, for example `http://arrowcol-middle-1:32310`.
    #[arg(long)]
    arrow_endpoint: String,
    /// Middle OTAPCol OTLP/HTTP base URL, for example `http://otapcol-middle-1:4318`.
    #[arg(long)]
    otlp_endpoint: String,
    /// Rows per DMV table.
    #[arg(long, default_value_t = 100)]
    rows: usize,
    /// Logs, spans, and counter increments per signal.
    #[arg(long, default_value_t = 10)]
    signals: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let client_id = Uuid::new_v4().to_string();
    let tables = tokio::runtime::Runtime::new()?.block_on(dmv::send(&cli.arrow_endpoint, &client_id, cli.rows))?;
    otlp::send(&cli.otlp_endpoint, &client_id, cli.signals)?;
    println!(
        "{}",
        json!({
            "client_id": client_id,
            "dmv_tables": tables,
            "dmv_rows_per_table": cli.rows,
            "otlp_signals": cli.signals,
        })
    );
    Ok(())
}
