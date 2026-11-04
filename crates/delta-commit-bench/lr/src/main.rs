//! Long-retention Delta benchmark. See `../long-retention-handoff.md`.

mod dl;
mod maint;
#[cfg(feature = "kernel")]
mod kernel;
mod schema;
mod stats;
mod store;
mod synth;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Lib {
    /// deltalake-core 1.1.1
    Dl1,
    /// deltalake-core main@460c904 (2.0.0)
    Dl2,
    /// delta_kernel 0.30.1
    Kernel,
}

#[derive(clap::Args, Debug, Clone)]
pub struct CommitArgs {
    #[arg(long, value_enum)]
    pub lib: Lib,
    #[arg(long)]
    pub table_url: url::Url,
    #[arg(long, default_value_t = 200)]
    pub commits: u32,
    #[arg(long, default_value_t = 1)]
    pub files_per_commit: u32,
    #[arg(long, default_value = "inst-bench/t#e1")]
    pub app_id: String,
    /// Abort (exit 3) when RSS passes this many GiB. 0 disables.
    #[arg(long, default_value_t = 20.0)]
    pub rss_cap_gib: f64,
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
    /// Kernel only: concurrent writers on the same table, each with appId `<app_id>-w<i>`.
    #[arg(long, default_value_t = 1)]
    pub writers: u32,
    /// Print one JSON line per commit to stderr.
    #[arg(long)]
    pub trace: bool,
    /// delta-rs only: let the post-commit hook write checkpoints (table interval, default 100), with no reload.
    #[arg(long)]
    pub dl_checkpoint: bool,
    /// Kernel only: sleep between commits of one writer (0 = back to back).
    #[arg(long, default_value_t = 0)]
    pub interval_ms: u64,
    /// Kernel only: stats generated per file (kernel keeps the table's stats columns).
    #[arg(long, value_enum, default_value_t = crate::schema::StatsMode::Default)]
    pub stats: crate::schema::StatsMode,
}

#[derive(clap::Args, Debug, Clone)]
pub struct LoadArgs {
    #[arg(long, value_enum)]
    pub lib: Lib,
    #[arg(long)]
    pub table_url: url::Url,
    #[arg(long, default_value = "inst-0005/t#e9")]
    pub app_id: String,
    #[arg(long, default_value_t = 20.0)]
    pub rss_cap_gib: f64,
}

#[derive(clap::Args, Debug, Clone)]
pub struct MultiArgs {
    /// Hot table (one).
    #[arg(long)]
    pub hot: Option<url::Url>,
    /// Warm tables are `<prefix>000` .. `<prefix>{count-1}`.
    #[arg(long)]
    pub warm_prefix: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub warm_count: usize,
    #[arg(long, default_value_t = 300)]
    pub duration_s: u64,
    #[arg(long, default_value_t = 100)]
    pub hot_interval_ms: u64,
    #[arg(long, default_value_t = 5000)]
    pub warm_interval_ms: u64,
    /// Checkpoint every N commits per table (0 = never), staggered across warm tables.
    #[arg(long, default_value_t = 0)]
    pub hot_ckpt_every: u64,
    #[arg(long, default_value_t = 100)]
    pub warm_ckpt_every: u64,
    /// Cap on concurrent commits (and snapshot loads) across all tables.
    #[arg(long, default_value_t = 16)]
    pub max_commits: usize,
    /// Cap on concurrent checkpoints across all tables.
    #[arg(long, default_value_t = 2)]
    pub max_ckpts: usize,
    #[arg(long, value_enum, default_value_t = crate::schema::StatsMode::Default)]
    pub stats: crate::schema::StatsMode,
    #[arg(long, default_value_t = 20.0)]
    pub rss_cap_gib: f64,
}

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Write a synthetic table: checkpoint with N adds plus a JSON tail.
    Gen(synth::GenArgs),
    /// Q1/Q2: blind-append commits with txn; commit latency and RSS.
    Commit(CommitArgs),
    /// Q3: load protocol, metadata and one appId's txn version.
    Load(LoadArgs),
    /// Q6: write a partition-drop or OPTIMIZE commit next to running appends.
    Maint(maint::MaintArgs),
    /// Q2: kernel, one hot plus N warm tables in one process.
    Multi(MultiArgs),
    /// Q4: write a checkpoint with the library.
    Checkpoint {
        #[command(flatten)]
        args: LoadArgs,
        /// Kernel only: v1 | v2 | v2-sidecar-N. Default: kernel auto-detect.
        #[arg(long)]
        spec: Option<String>,
    },
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().cmd {
        Cmd::Gen(a) => synth::run(a).await,
        Cmd::Maint(a) => maint::run(a).await,
        Cmd::Commit(a) => match a.lib {
            Lib::Dl1 => dl::v1::commit(&a, "dl1").await,
            Lib::Dl2 => dl::v2::commit(&a, "dl2").await,
            Lib::Kernel => kernel::commit(a).await,
        },
        Cmd::Load(a) => match a.lib {
            Lib::Dl1 => dl::v1::load(&a, "dl1").await,
            Lib::Dl2 => dl::v2::load(&a, "dl2").await,
            Lib::Kernel => kernel::load(a).await,
        },
        Cmd::Multi(a) => kernel::multi(a).await,
        Cmd::Checkpoint { args: a, spec } => match a.lib {
            Lib::Dl1 => dl::v1::checkpoint(&a, "dl1").await,
            Lib::Dl2 => dl::v2::checkpoint(&a, "dl2").await,
            Lib::Kernel => kernel::checkpoint(a, spec).await,
        },
    }
}
