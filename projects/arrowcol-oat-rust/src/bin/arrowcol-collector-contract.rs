use anyhow::{Context, Result, ensure};
use arrowcol_oat_rust::runner::{CollectorPromotionRequest, collector_promotion_contract};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    input: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        std::fs::metadata(&args.input)?.len() <= 8 * 1024 * 1024,
        "contract input is too large"
    );
    let source = std::fs::read_to_string(&args.input).context("read collector contract")?;
    let request: CollectorPromotionRequest = serde_json::from_str(&source).context("parse collector contract")?;
    println!("{}", collector_promotion_contract(request)?);
    Ok(())
}
