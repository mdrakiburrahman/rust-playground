use anyhow::{Result, anyhow};
use clap::Parser;
use greeting::greet;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(about = "Print a friendly greeting", version)]
struct Cli {
    /// Name to greet.
    #[arg(long, default_value = "World")]
    name: String,
}

fn main() -> Result<()> {
    init_tracing()?;

    let cli = Cli::parse();
    let message = greet(&cli.name)?;

    info!(name = cli.name.trim(), "greeting generated");
    println!("{message}");

    Ok(())
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
