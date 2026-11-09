mod bulk;
mod oracle;
mod scale_oracle;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use arrow_deltalake::delta::object_store_for_url;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use object_store::ObjectStoreScheme;
use serde_json::{Value, json};
use tokio::sync::Notify;
use url::Url;

#[derive(Parser)]
#[command(about = "Bounded Azure QAP fixture publisher (JSON stdout)")]
struct Cli {
    /// Parent directory of the fixture table; the table is written to <root>/<table>/.
    #[arg(long)]
    root: String,
    /// Explicit table allowlist; must equal --table.
    #[arg(long)]
    allow_table: String,
    #[arg(long)]
    table: String,
    /// Hard action deadline; interrupted writes are never reported as cleanly closed.
    #[arg(long, default_value_t = 900, value_parser = clap::value_parser!(u64).range(1..=3600))]
    timeout_seconds: u64,
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Bounded QAP fixture; copies are native synchronous Azure server-side puts.
    SeedScale(bulk::SeedOptions),
    /// Project only keys, not copied payload, to build bounded expected groups.
    InspectScale,
}

impl Cli {
    fn table_url(&self) -> Result<Url> {
        ensure!(self.allow_table == self.table, "table is not allowlisted");
        ensure!(
            self.table.len() <= 63
                && self.table.starts_with(|first: char| first.is_ascii_lowercase())
                && self
                    .table
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
            "table must be a lowercase identifier"
        );
        ensure!(
            std::env::var("ARROWCOL_PARTITION_GRANULARITY").as_deref() == Ok("minute"),
            "ARROWCOL_PARTITION_GRANULARITY=minute is required"
        );
        // A cloud root is admitted only when it exactly matches the generated private environment.
        let mut url = if self.root.contains("://") {
            let url = Url::parse(&self.root)?;
            ensure!(matches!(url.scheme(), "abfss" | "file"), "unsupported root scheme");
            if url.scheme() == "abfss" {
                ensure!(
                    std::env::var("ARROWCOL_DELTA_ROOT").as_deref() == Ok(self.root.as_str()),
                    "cloud root does not match ARROWCOL_DELTA_ROOT allowlist"
                );
            }
            url
        } else {
            let root = PathBuf::from(&self.root);
            ensure!(root.is_absolute(), "local root must be absolute");
            Url::from_directory_path(root).map_err(|()| anyhow::anyhow!("invalid local root"))?
        };
        ensure!(
            url.password().is_none() && url.query().is_none() && url.fragment().is_none(),
            "root contains secret URL components"
        );
        ensure!(
            url.scheme() != "file" || url.host_str().is_none(),
            "file root must be local"
        );
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        // Same layout as delta-maintenance: the table lives directly at <root>/<table>/.
        Ok(url.join(&format!("{}/", self.table))?)
    }
}

async fn require_empty(url: &Url) -> Result<()> {
    let store = object_store_for_url(url)?;
    let (_, prefix) = ObjectStoreScheme::parse(url)?;
    let log = prefix.join("_delta_log");
    let mut entries = store.list(Some(&log));
    if let Some(entry) = entries.next().await {
        entry?;
        anyhow::bail!("seed requires a fresh experiment table");
    }
    Ok(())
}

struct Stop {
    requested: AtomicBool,
    notify: Notify,
}

impl Stop {
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        self.notify.notify_one();
    }
    fn requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

async fn run(cli: &Cli, stop: &Stop) -> Result<Value> {
    let url = cli.table_url().context("validate_configuration")?;
    match &cli.action {
        Action::SeedScale(options) => bulk::seed(cli, url, options, stop).await.context("scale_fixture"),
        Action::InspectScale => scale_oracle::inspect(url, cli.table.clone())
            .await
            .context("scale_oracle"),
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) {
                let _ = error.print();
                return;
            }
            println!(
                "{}",
                json!({"ok":false,"error":"invalid_arguments","publisherClosed":false})
            );
            std::process::exit(2);
        }
    };
    let stop = Arc::new(Stop {
        requested: AtomicBool::new(false),
        notify: Notify::new(),
    });
    let started = Instant::now();
    let watcher_stop = Arc::clone(&stop);
    let soft_deadline = Duration::from_secs(cli.timeout_seconds.saturating_sub(30).max(1));
    let watcher = tokio::spawn(async move {
        tokio::select! {
            _ = shutdown_signal() => {}
            () = tokio::time::sleep(soft_deadline) => {}
        }
        watcher_stop.request();
    });
    let action = run(&cli, &stop);
    tokio::pin!(action);
    let result = tokio::select! {
        result = &mut action => result,
        () = tokio::time::sleep(Duration::from_secs(cli.timeout_seconds)) => {
            // Do not cancel a storage future and then claim a drained publisher.
            println!("{}", json!({"ok":false,"error":"action_deadline","publisherClosed":false}));
            std::process::exit(1);
        }
    };
    watcher.abort();
    let _ = watcher.await;
    match result {
        Ok(mut output) => {
            output["ok"] = json!(true);
            output["elapsedMs"] = json!(started.elapsed().as_millis());
            let cancelled = output["cancelled"] == true;
            println!("{output}");
            if cancelled {
                std::process::exit(130);
            }
        }

        Err(error) => {
            // Error chains may contain signed URLs, credentials or private storage endpoints.
            let mut output = json!({"ok":false,"error":"experiment_action_failed","stage":failure_stage(&error),"publisherClosed":false});
            // Opt-in local diagnosis only; callers that persist evidence must leave this unset.
            if std::env::var_os("QAP_FIXTURE_DEBUG_ERRORS").is_some() {
                eprintln!("{error:?}");
            }
            if let Some(partial) = error.downcast_ref::<bulk::PartialFailure>() {
                output["orphanInventory"] = partial.inventory.clone();
            }
            println!("{output}");
            std::process::exit(1);
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

fn failure_stage(error: &anyhow::Error) -> &'static str {
    for cause in error.chain() {
        let message = cause.to_string();
        for stage in [
            "validate_configuration",
            "capture_generation",
            "seed_target",
            "capture_oracle",
            "drain",
            "publish",
            "close_publisher",
            "scale_fixture",
            "scale_oracle",
        ] {
            if message == stage {
                return stage;
            }
        }
    }
    "setup"
}
