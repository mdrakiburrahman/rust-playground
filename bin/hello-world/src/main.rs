use anyhow::{Result, anyhow};
use clap::Parser;
use greeting::greet;
use std::{num::NonZeroU64, sync::mpsc, time::Duration};
use tracing::info;
use tracing_subscriber::EnvFilter;

mod telemetry;

#[derive(Debug, Parser)]
#[command(about = "Print a friendly greeting", version)]
struct Cli {
    /// Name to greet.
    #[arg(long, default_value = "World")]
    name: String,

    /// Export logs, metrics, and traces using OTLP HTTP protobuf.
    #[arg(long)]
    telemetry: bool,

    /// Number of greetings to generate.
    #[arg(long, default_value = "1", conflicts_with = "repeat")]
    count: NonZeroU64,

    /// Generate greetings until interrupted.
    #[arg(long)]
    repeat: bool,

    /// Delay between greetings in milliseconds.
    #[arg(long, default_value = "1000")]
    interval_ms: NonZeroU64,

    /// Marker attached to each telemetry signal.
    #[arg(long, default_value = "demo", value_parser = nonblank)]
    run_id: String,
}

fn main() -> Result<()> {
    init_tracing()?;

    let cli = Cli::parse();
    let message = greet(&cli.name)?;

    let (interrupt_tx, interrupt_rx) = mpsc::channel();
    ctrlc::set_handler(move || {
        let _ = interrupt_tx.send(());
    })?;
    let telemetry = cli.telemetry.then(telemetry::Telemetry::new).transpose()?;
    let result = run(&cli, &message, telemetry.as_ref(), &interrupt_rx);
    let shutdown = telemetry.map_or(Ok(()), |telemetry| telemetry.shutdown());
    result.and(shutdown)
}

fn run(
    cli: &Cli,
    message: &str,
    telemetry: Option<&telemetry::Telemetry>,
    interrupt: &mpsc::Receiver<()>,
) -> Result<()> {
    let mut remaining = cli.count.get();
    loop {
        if interrupt.try_recv().is_ok() {
            break;
        }
        info!(name = cli.name.trim(), "greeting generated");
        println!("{message}");
        if let Some(telemetry) = telemetry {
            telemetry.greeting(cli.name.trim(), &cli.run_id)?;
        }
        if !cli.repeat {
            remaining -= 1;
            if remaining == 0 {
                break;
            }
        }
        match interrupt.recv_timeout(Duration::from_millis(cli.interval_ms.get())) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }

    Ok(())
}

fn nonblank(value: &str) -> std::result::Result<String, String> {
    if value.trim().is_empty() {
        Err("run ID must not be empty".into())
    } else {
        Ok(value.into())
    }
}

fn init_tracing() -> Result<()> {
    let filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => EnvFilter::new("info"),
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow!("failed to initialize tracing: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_preserve_single_greeting() {
        let cli = Cli::parse_from(["hello-world"]);
        assert_eq!(cli.name, "World");
        assert_eq!(cli.count.get(), 1);
        assert_eq!(cli.interval_ms.get(), 1000);
        assert_eq!(cli.run_id, "demo");
        assert!(!cli.telemetry);
        assert!(!cli.repeat);
    }

    #[test]
    fn run_id_must_contain_nonwhitespace() {
        assert!(nonblank("").is_err());
        assert!(nonblank(" \t\n").is_err());
        assert_eq!(nonblank("marker").ok().as_deref(), Some("marker"));
    }
}
