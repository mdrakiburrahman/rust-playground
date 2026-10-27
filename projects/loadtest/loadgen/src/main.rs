use anyhow::Result;
use arrowcol_loadgen::{Cli, Command, runner, validation};
use clap::Parser;
use std::io::Write;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run(arguments) => runner::run(*arguments).await,
        Command::Validate(arguments) => validation::validate(arguments).await,
        Command::Drain(arguments) => validation::drain(arguments).await,
        Command::PublisherRelease(arguments) => arrowcol_loadgen::release::publisher_release(arguments).await,
        Command::ObserveQap(arguments) => {
            anyhow::ensure!(
                !arguments.timeout.get().is_zero(),
                "QAP observation deadline must be positive"
            );
            let mut tables = arguments.tables;
            if tables.is_empty() {
                for pool in ["direct", "dataflow"] {
                    for statement in arrowcol_loadgen::schema::schema_statements()? {
                        tables.push(format!("{pool}_{}", statement.identity.table()));
                    }
                }
            }
            let observation = arrowcol_loadgen::observe_qap(
                &arguments.endpoint,
                &arguments.results,
                arguments.database.as_deref(),
                &tables,
                arguments.timeout.get(),
            )
            .await?;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(arguments.output)?;
            file.write_all(&serde_json::to_vec(&observation)?)?;
            file.sync_all()?;
            Ok(())
        }
        Command::InitializeAzure(arguments) => arrowcol_loadgen::initialize::initialize(arguments).await,
        Command::AzureManifest(arguments) => arrowcol_loadgen::initialize::write_manifest(arguments).await,
        Command::PrintSchemas => {
            print!("{}", arrowcol_loadgen::schema::schema_ddl());
            Ok(())
        }
    }
}
