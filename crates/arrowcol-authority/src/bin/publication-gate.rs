//! Bounded executable proof driver; not a collector/controller integration.
use anyhow::{Context, Result, ensure};
use arrow::{
    array::{ArrayRef, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use arrowcol_authority::sql_publication::{Identity, Proposal, PublicationHooks, Publisher, stage_batch};
use arrowcol_sql_server::ConnectionConfig;
use clap::{Parser, Subcommand};
use std::{fs, path::PathBuf, sync::Arc, time::Duration};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "target/publication-gate/private")]
    root: PathBuf,
    #[arg(long, default_value = "127.0.0.1")]
    sql_host: String,
    #[arg(long, default_value_t = 1433)]
    sql_port: u16,
    #[arg(long)]
    password_file: Option<PathBuf>,
    #[arg(long)]
    pause: Option<String>,
    #[arg(long)]
    marker: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Stage {
        path: PathBuf,
        #[arg(long, default_value_t = 1)]
        first: i64,
        #[arg(long, default_value_t = 3)]
        rows: usize,
        #[arg(long)]
        evolve: bool,
    },
    Bootstrap {
        table: String,
        identity: PathBuf,
    },
    Prepare {
        table: String,
        operation: String,
        identity: PathBuf,
        output: PathBuf,
        #[arg(long)]
        stage: Option<PathBuf>,
        #[arg(long)]
        sha256: Option<String>,
        #[arg(long)]
        evolve: bool,
    },
    Accept {
        proposal: PathBuf,
        #[arg(long)]
        lose_reply: bool,
    },
    Resolve {
        proposal: PathBuf,
    },
    Transfer {
        table: String,
        from: PathBuf,
        to: PathBuf,
    },
    Activate {
        table: String,
        identity: PathBuf,
    },
    Materialize {
        table: String,
    },
    Status {
        table: String,
    },
}

struct Hooks {
    pause: Option<String>,
    marker: Option<PathBuf>,
}
impl PublicationHooks for Hooks {
    fn at(&self, phase: &str) -> Result<()> {
        if self.pause.as_deref() == Some(phase) {
            let marker = self.marker.as_ref().context("pause needs --marker")?;
            fs::create_dir_all(marker)?;
            fs::write(marker.join("paused"), phase)?;
            println!("PAUSED {phase}");
            let started = std::time::Instant::now();
            while !marker.join("resume").exists() {
                ensure!(started.elapsed() < Duration::from_secs(180), "pause timeout");
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        Ok(())
    }
}

fn schema(evolve: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("marker", DataType::Utf8, true),
    ];
    if evolve {
        fields.push(Field::new("extra", DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

fn read_identity(path: &PathBuf) -> Result<Identity> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args = Args::parse();
    if let Command::Stage {
        path,
        first,
        rows,
        evolve,
    } = args.command
    {
        ensure!(rows <= 65536, "row bound");
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values((0..rows).map(|n| first + n as i64))),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|n| format!("row-{}", first + n as i64)),
            )),
        ];
        if evolve {
            columns.push(Arc::new(StringArray::from_iter_values((0..rows).map(|_| "evolved"))));
        }
        let batch = RecordBatch::try_new(schema(evolve), columns)?;
        println!(
            "{}",
            serde_json::json!({"sha256":stage_batch(&path, &batch).await?,"rows":rows})
        );
        return Ok(());
    }
    let password = fs::read_to_string(args.password_file.context("trusted role needs --password-file")?)?;
    let sql = ConnectionConfig::new(args.sql_host, args.sql_port, "master", "sa", password.trim())
        .trust_server_certificate(true)
        .with_timeouts(Duration::from_secs(5), Duration::from_secs(45));
    let publisher = Publisher::new(&args.root, sql)?;
    let hooks = Hooks {
        pause: args.pause,
        marker: args.marker,
    };
    match args.command {
        Command::Bootstrap { table, identity } => {
            publisher.bootstrap(&table, &read_identity(&identity)?).await?;
        }
        Command::Prepare {
            table,
            operation,
            identity,
            output,
            stage,
            sha256,
            evolve,
        } => {
            let stage = match (stage.as_deref(), sha256.as_deref()) {
                (Some(path), Some(digest)) => Some((path, digest)),
                (None, None) => None,
                _ => anyhow::bail!("stage and sha256 must be specified together"),
            };
            let proposal = publisher
                .prepare(&table, &operation, read_identity(&identity)?, schema(evolve), stage)
                .await?;
            use std::io::Write;
            let mut file = fs::OpenOptions::new().write(true).create_new(true).open(&output)?;
            file.write_all(&serde_json::to_vec_pretty(&proposal)?)?;
            file.sync_all()?;
            println!(
                "{}",
                serde_json::json!({"prepared":true,"digest":proposal.digest()?,"version":proposal.version})
            );
            return Ok(());
        }
        Command::Accept { proposal, lose_reply } => {
            let proposal: Proposal = serde_json::from_slice(&fs::read(proposal)?)?;
            publisher.accept(&proposal, &hooks, lose_reply).await?;
            println!(
                "{}",
                serde_json::json!({"accepted":true,"final":false,"version":proposal.version})
            );
            return Ok(());
        }
        Command::Resolve { proposal } => {
            let proposal: Proposal = serde_json::from_slice(&fs::read(proposal)?)?;
            println!(
                "{}",
                serde_json::json!({"accepted":publisher.resolve(&proposal).await?,"final":false})
            );
            return Ok(());
        }
        Command::Transfer { table, from, to } => {
            publisher
                .transfer(&table, &read_identity(&from)?, &read_identity(&to)?)
                .await?;
        }
        Command::Activate { table, identity } => {
            publisher.activate(&table, &read_identity(&identity)?).await?;
        }
        Command::Materialize { table } => {
            publisher.materialize(&table, &hooks).await?;
            println!("{}", publisher.status(&table).await?);
            return Ok(());
        }
        Command::Status { table } => {
            println!("{}", publisher.status(&table).await?);
            return Ok(());
        }
        Command::Stage { .. } => unreachable!(),
    }
    println!("{}", serde_json::json!({"ok":true}));
    Ok(())
}
