mod dmv;
mod kusto;
mod otlp;

use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use serde_json::json;
use uuid::Uuid;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Sends DMV Arrow batches and OTLP signals tagged with a freshly generated client ID.
    Client {
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
    },
    /// Runs one Kusto command and prints the raw JSON response.
    Kusto {
        #[arg(long)]
        endpoint: String,
        #[arg(long, default_value = "NetDefaultDB")]
        database: String,
        /// Use the management endpoint instead of the query endpoint.
        #[arg(long)]
        mgmt: bool,
        csl: String,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Client {
            arrow_endpoint,
            otlp_endpoint,
            rows,
            signals,
        } => {
            let client_id = Uuid::new_v4().to_string();
            let tables = tokio::runtime::Runtime::new()?.block_on(dmv::send(&arrow_endpoint, &client_id, rows))?;
            otlp::send(&otlp_endpoint, &client_id, signals)?;
            println!(
                "{}",
                json!({
                    "client_id": client_id,
                    "dmv_tables": tables,
                    "dmv_rows_per_table": rows,
                    "otlp_signals": signals,
                })
            );
        }
        Command::Kusto {
            endpoint,
            database,
            mgmt,
            csl,
        } => println!(
            "{}",
            kusto::execute(&endpoint, &database, &csl, mgmt, Duration::from_secs(120))?
        ),
    }
    Ok(())
}
