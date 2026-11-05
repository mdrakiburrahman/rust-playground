mod dmv;
mod otlp;

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use serde_json::json;
use tokio::runtime::Runtime;
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
    /// Reuse this client ID instead of generating a new one.
    #[arg(long)]
    client_id: Option<String>,
    /// Send another round every this many seconds until stopped; failed rounds are logged and retried.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    interval: Option<u64>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let client_id = cli.client_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
    let runtime = Runtime::new()?;
    let Some(interval) = cli.interval else {
        return round(&cli, &client_id, &runtime);
    };
    loop {
        if let Err(error) = round(&cli, &client_id, &runtime) {
            eprintln!("{client_id}: {error:#}");
        }
        std::thread::sleep(Duration::from_secs(interval));
    }
}

fn round(cli: &Cli, client_id: &str, runtime: &Runtime) -> Result<()> {
    let tables = runtime.block_on(dmv::send(&cli.arrow_endpoint, client_id, cli.rows))?;
    otlp::send(&cli.otlp_endpoint, client_id, cli.signals)?;
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
