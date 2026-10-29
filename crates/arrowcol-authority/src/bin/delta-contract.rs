//! Approved live backend CAS probe against a task-owned runtime deployment.
use anyhow::{Context, Result, ensure};
use arrowcol_authority::{Config, delta_runtime::PrivateConfig, delta_store::Command};
use clap::Parser;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: String,
    #[arg(long)]
    private: String,
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::load(&args.config)?;
    let private = PrivateConfig::load(&args.private)?;
    let a = private.store(&config).await?;
    let b = private.store(&config).await?;
    let snapshot = a.snapshot().await?;
    let controller = snapshot.controller.context("controller")?;
    let first = snapshot
        .members
        .get("collector1")
        .context("member1")?
        .member
        .writer
        .clone();
    let second = snapshot
        .members
        .get("collector2")
        .context("member2")?
        .member
        .writer
        .clone();
    let claim_a = Command::Claim {
        controller: controller.clone(),
        key: "cas".into(),
        target: first,
        operation: "claim-a".into(),
    };
    let claim_b = Command::Claim {
        controller: controller.clone(),
        key: "cas".into(),
        target: second,
        operation: "claim-b".into(),
    };
    let (ra, rb) = tokio::join!(a.execute(claim_a.clone()), b.execute(claim_b.clone()));
    ensure!(ra.is_ok() != rb.is_ok(), "exactly one backend claim must win");
    let winner = if ra.is_ok() { claim_a } else { claim_b };
    a.execute(winner).await?;
    let state = b.snapshot().await?;
    ensure!(
        state.tables["cas"].record.epoch == 1,
        "idempotent claim must not allocate epoch"
    );
    ensure!(
        b.execute(Command::Start {
            id: "competing-controller".into()
        })
        .await
        .is_err(),
        "live controller contention"
    );
    let mut stale = controller;
    stale.generation += 1;
    ensure!(
        b.execute(Command::Renew { controller: stale }).await.is_err(),
        "stale control generation"
    );
    println!(
        "{}",
        serde_json::json!({"backend":private.backend,"concurrent_claims":"one-winner",
        "operation_reconciliation":true,"controller_contention":true,"stale_controller_rejected":true,
        "snapshot_revision":state.revision})
    );
    Ok(())
}
