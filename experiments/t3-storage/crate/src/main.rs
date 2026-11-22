use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use serde_json::json;
use storage_spike::{Samples, blob, compare, quiver, read_receipts};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Write(quiver::WriteArgs),
    Recover(quiver::RecoverArgs),
    Blob(blob::Args),
    Raw {
        #[arg(long)]
        path: PathBuf,
        #[arg(long, default_value_t = 1_048_576)]
        bytes: usize,
        #[arg(long, default_value_t = 1000)]
        iterations: u64,
        #[arg(long)]
        metadata: bool,
    },
    VerifyOracle {
        #[arg(long)]
        run: String,
        #[arg(long)]
        oracle: PathBuf,
        #[arg(long)]
        observed: PathBuf,
    },
}

async fn run() -> Result<()> {
    match Cli::parse().command {
        Command::Write(args) => quiver::write(args).await,
        Command::Recover(args) => quiver::recover(args).await,
        Command::Blob(args) => blob::run(args).await,
        Command::Raw {
            path,
            bytes,
            iterations,
            metadata,
        } => {
            ensure!(
                (1..=16 * 1024 * 1024).contains(&bytes) && (1..=1_000_000).contains(&iterations),
                "invalid raw benchmark bounds"
            );
            let mut samples = Samples::default();
            if metadata {
                for _ in 0..iterations {
                    let start = Instant::now();
                    std::fs::symlink_metadata(&path)?;
                    samples.record(start.elapsed());
                }
            } else {
                let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
                let data = vec![0x5a; bytes];
                for _ in 0..iterations {
                    let start = Instant::now();
                    file.write_all(&data)?;
                    file.sync_all()?;
                    samples.record(start.elapsed());
                }
            }
            println!(
                "{}",
                json!({"kind":"raw", "operation":if metadata {"symlink_metadata"} else {"write+fsync"}, "bytes":bytes, "latency":samples.summary()})
            );
            Ok(())
        }
        Command::VerifyOracle { run, oracle, observed } => {
            let receipts = read_receipts(&std::fs::read(oracle)?)?;
            let ids: Vec<i64> = serde_json::from_slice(&std::fs::read(observed)?)?;
            let result = compare(&receipts, &ids, &run)?;
            println!("{result}");
            ensure!(
                result["passed"] == true,
                "ACKNOWLEDGED LOSS: stop the experiment and preserve evidence"
            );
            Ok(())
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let start = Instant::now();
    if let Err(error) = run().await {
        let fenced = error
            .downcast_ref::<otel_arrow_dfe_quiver::QuiverError>()
            .is_some_and(|e| e.is_fenced())
            || error
                .downcast_ref::<otel_arrow_dfe_quiver::SubscriberError>()
                .is_some_and(|e| matches!(e, otel_arrow_dfe_quiver::SubscriberError::Fenced { .. }));
        eprintln!(
            "{}",
            json!({"kind":"error", "fenced":fenced, "error":format!("{error:#}"), "elapsed_ms":start.elapsed().as_secs_f64() * 1000.0})
        );
        std::process::exit(if fenced { 20 } else { 2 });
    }
    tokio::time::sleep(Duration::ZERO).await;
}
