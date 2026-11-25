use std::path::PathBuf;

use arrowcol_sdk_gen::{ArtifactBundle, SafeOutputPath};
use clap::Parser;

#[derive(Parser)]
#[command(about = "Export an opt-in self-contained Arrowcol protoc compiler contract")]
struct Args {
    #[arg(long)]
    artifacts: PathBuf,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, default_value = "schema.proto")]
    entry: String,
}

fn main() {
    if let Err(error) = run(Args::parse()) {
        eprintln!("arrowcol-protoc: {error}");
        std::process::exit(1);
    }
}

fn run(args: Args) -> arrowcol_protoc::Result<()> {
    let bundle = ArtifactBundle::load_directory(args.artifacts)?;
    let output = SafeOutputPath::new(std::env::current_dir()?, args.out)?;
    arrowcol_protoc::export(&bundle, output.absolute(), &args.entry)?;
    Ok(())
}
