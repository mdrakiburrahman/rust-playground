//! The per-table commit loop under test.

use crate::backend::{Backend, CommitReq, DataOpts};
use crate::store::{CounterSnapshot, Counters};
use crate::table::now_millis;
use crate::wal::{FileReady, Manifest, Wal};
use anyhow::{Context, Result};
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

/// Where the separate checkpoint's synchronous kernel call runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, clap::ValueEnum)]
pub enum CheckpointExec {
    /// `deltalake_core::checkpoints::create_checkpoint`: the kernel checkpoint write runs on the
    /// async worker that polls the loop (finding F14).
    Worker,
    /// The same kernel calls inside one `spawn_blocking`.
    Blocking,
}

/// Process-wide caps on concurrent Delta operations. Each held permit bounds the tokio blocking
/// threads an operation can occupy.
#[derive(Clone, Debug, Default)]
pub struct Limits {
    /// Table open and recovery, and each commit (pre-check, commit, post-commit snapshot update).
    pub ops: Option<Arc<Semaphore>>,
    /// Each separate checkpoint, including the snapshot reload after it.
    pub checkpoints: Option<Arc<Semaphore>>,
}

/// Waits for a permit when `sem` is set. Returns the permit and the wait in ms.
pub async fn acquire(sem: Option<&Arc<Semaphore>>) -> (Option<OwnedSemaphorePermit>, f64) {
    let Some(sem) = sem else { return (None, 0.0) };
    let started = Instant::now();
    // The semaphores are never closed, so this is always a permit.
    let permit = Arc::clone(sem).acquire_owned().await.ok();
    (permit, ms(started.elapsed()))
}

/// How appIds map to ownership (follow-up Q1 and Q2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, clap::ValueEnum)]
pub enum EpochMode {
    /// One appId `<id>/<table>` for everything; a fenced loop stops (report model).
    Off,
    /// Owner commits under `<id>/<table>`; a fenced owner drains its pending manifests under a
    /// fresh epoch appId `<id>/<table>#e<n>` (follow-up doc as written).
    Drain,
    /// Every ownership tenure and every drain claims a fresh epoch appId.
    Tenure,
}

/// Which appIds a loop fences when it takes ownership.
#[derive(Clone, Debug, Default)]
pub enum FenceOn {
    #[default]
    None,
    /// Signaled: fence the current appId of each listed identity.
    Explicit(Vec<String>),
    /// Inferred: fence the most recent other identity that committed data to the table.
    Inferred,
}

/// Commit-info key that marks what a commit is for (`owner`, `drain`, `fence`, `claim`).
pub const KIND_KEY: &str = "dcbKind";

fn epoch_app_id(base: &str, epoch: i64) -> String {
    format!("{base}#e{epoch}")
}

fn epoch_counter_id(base: &str) -> String {
    format!("{base}#epoch")
}

/// The `<id>/<table>` part of an appId.
fn base_of(app_id: &str) -> &str {
    app_id.split_once('#').map_or(app_id, |(base, _)| base)
}

/// The epoch counter's `txn` version packs the latest claimed epoch and the identity's
/// committed floor at claim time: `epoch << 40 | (floor + 1)`. Claims are serialized because
/// every claim writes this same appId.
const CLAIM_SHIFT: u32 = 40;

fn encode_claim(epoch: i64, floor: i64) -> i64 {
    (epoch << CLAIM_SHIFT) | (floor + 1)
}

fn decode_claim(raw: i64) -> (i64, i64) {
    (raw >> CLAIM_SHIFT, (raw & ((1 << CLAIM_SHIFT) - 1)) - 1)
}

/// When the loop cuts a batch.
#[derive(Clone, Copy, Debug)]
pub enum Pacing {
    /// Commit everything waiting; files that arrive during a commit form the next batch.
    Group,
    /// Commit everything waiting on a fixed interval.
    Interval(Duration),
}

/// How the loop decides whether a batch already landed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, clap::ValueEnum)]
pub enum Idempotency {
    /// `txn` action with this loop's appId; landed when the table's txn version >= batch version.
    Txn,
    /// No `txn`; landed when a recent log entry adds the batch's first file.
    ScanLog,
}

/// Loop settings.
#[derive(Clone, Debug)]
pub struct LoopConfig {
    pub app_id: String,
    pub table_name: String,
    pub pacing: Pacing,
    pub idempotency: Idempotency,
    /// delta-rs internal retry limit (VersionAlreadyExists).
    pub library_max_retries: usize,
    /// Attempts by the loop itself after a commit error.
    pub loop_max_attempts: u32,
    /// Let delta-rs write checkpoints inside the commit call.
    pub inline_checkpoint: bool,
    /// Post-commit expired-log cleanup. `None` follows `delta.enableExpiredLogCleanup`.
    pub cleanup_expired_logs: Option<bool>,
    /// Rebuild the snapshot from each checkpoint the loop writes.
    pub reload_after_checkpoint: bool,
    /// Write a checkpoint after the commit is acknowledged, every N table versions.
    pub separate_checkpoint_every: Option<u64>,
    pub checkpoint_exec: CheckpointExec,
    pub limits: Limits,
    pub max_files_per_commit: usize,
    /// Log versions scanned back by [`Idempotency::ScanLog`] and by inferred fencing.
    pub scan_window: u64,
    pub epochs: EpochMode,
    pub fence_on: FenceOn,
    /// Only commit this identity's pending manifests, then stop.
    pub drain_only: bool,
    /// Abort the process right before the Nth epoch claim commit attempt (process-wide count).
    pub crash_before_claim: Option<u64>,
    /// Abort the process right after the Nth successful epoch claim commit (process-wide count).
    pub crash_after_claim: Option<u64>,
}

static CLAIM_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static CLAIMS_COMMITTED: AtomicU64 = AtomicU64::new(0);

/// One JSONL record per commit attempt sequence.
#[derive(Clone, Debug, Serialize)]
pub struct CommitRecord {
    pub kind: &'static str,
    pub ts_ms: i64,
    pub table: String,
    pub app_id: String,
    pub txn_version: i64,
    pub delta_version: Option<u64>,
    pub files: usize,
    pub rows: i64,
    pub recovered: bool,
    /// Oldest file: upload done to commit start.
    pub queue_ms: f64,
    /// Wait for an operation permit before the commit (included in visibility).
    pub permit_wait_ms: f64,
    /// Wait for a checkpoint permit (not included in this commit's visibility).
    pub checkpoint_permit_wait_ms: Option<f64>,
    /// Manifest write time.
    pub manifest_ms: f64,
    /// Commit call(s) until success.
    pub commit_ms: f64,
    /// Oldest file: upload done to commit acknowledged (writer-side visibility).
    pub visible_oldest_ms: f64,
    /// Newest file: upload done to commit acknowledged.
    pub visible_newest_ms: f64,
    pub library_retries: u64,
    pub loop_attempts: u32,
    pub outcome: &'static str,
    pub errors: Vec<String>,
    pub storage: CounterSnapshot,
    pub checkpoint_ms: Option<f64>,
    /// Time to rebuild the in-memory snapshot from the new checkpoint.
    pub checkpoint_reload_ms: Option<f64>,
    /// Ownership events (`fence`, `claim`, `lost`, `locked_out`, `drain_done`): what happened.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// `txn` versions at or above this mark an appId as fenced. The fence stores the fenced
/// owner's last committed version as `FENCE_OFFSET + last + 1` (`last` = -1 if none).
pub const FENCE_OFFSET: i64 = 1 << 62;

fn decode_fence(raw: i64) -> Option<i64> {
    let last = raw - FENCE_OFFSET - 1;
    (last >= 0).then_some(last)
}

/// Another identity's WAL to take over. Its appId is fenced before any manifest moves.
pub struct Adopt {
    pub wal: Wal,
    pub app_id: String,
}

/// Whether a manifest is in the table, from this loop's point of view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Landed {
    Yes,
    No,
    /// This appId was fenced (or its `txn` entry vanished) and the batch is not known to have
    /// landed. The loop must stop; the manifest stays pending for the adopting owner.
    Fenced,
}

/// The commit loop for one table.
pub struct CommitLoop {
    table: Backend,
    counters: Arc<Counters>,
    wal: Wal,
    cfg: LoopConfig,
    /// appId of the current tenure or drain. Equals `cfg.app_id` unless epochs are in use.
    app_id: String,
    /// Highest version this identity committed under any earlier appId, frozen by the claim
    /// that fenced those appIds. Manifests at or below it already landed.
    own_floor: Option<i64>,
    /// Whether this loop may commit new data (owner) or only drain.
    owner: bool,
    next_version: i64,
    /// Last `txn` version this loop knows it committed. Used to detect fencing and expiry.
    last_committed: Option<i64>,
    /// Snapshot version whose log has been checked for this loop's batches and fences. Every
    /// commit up to it is either this loop's own or passed the delta-rs conflict checker, which
    /// rejects winning commits that carry this loop's appId. With kernel there is no checker,
    /// but none is needed: a kernel commit only lands at exactly `snapshot + 1` (put-if-absent,
    /// no rebase), so after a commit at `verified + 1` nothing sits between the verified
    /// snapshot and this loop's own commit. While the snapshot stays at this
    /// version, the pre-commit landed check is skipped: the kernel's `txn` lookup replays the
    /// whole log since the last checkpoint, and the scan-log check reads up to `scan_window`
    /// commits one by one.
    verified_through: Option<u64>,
    out: mpsc::UnboundedSender<CommitRecord>,
}

impl CommitLoop {
    /// Build the loop and replay any pending manifests. With `adopt`, fence the previous
    /// identity in the table log, then recommit its unlanded manifests under this loop's appId.
    pub async fn start(
        table: Backend,
        counters: Arc<Counters>,
        wal: Wal,
        cfg: LoopConfig,
        adopt: Option<Adopt>,
        out: mpsc::UnboundedSender<CommitRecord>,
    ) -> Result<Self> {
        let app_id = cfg.app_id.clone();
        let mut this = Self {
            table,
            counters,
            wal,
            cfg,
            app_id,
            own_floor: None,
            owner: false,
            next_version: 0,
            last_committed: None,
            verified_through: None,
            out,
        };
        if this.cfg.drain_only {
            this.drain().await?;
            return Ok(this);
        }
        match (this.cfg.epochs, &this.cfg.fence_on) {
            (EpochMode::Off, FenceOn::Inferred) => {
                this.recover(adopt).await?;
                return Ok(this);
            }
            (_, FenceOn::Inferred) => {
                // Ownership starts with the first file routed here. Pending work from a previous
                // life is drained now.
                if !this.wal.pending()?.is_empty() {
                    this.drain().await?;
                }
                return Ok(this);
            }
            (EpochMode::Tenure, _) => {
                this.fence_others().await?;
                this.claim("owner").await?;
                this.owner = true;
                if !this.recommit_pending().await? {
                    anyhow::bail!("fenced during startup replay");
                }
            }
            _ => {
                this.fence_others().await?;
                this.recover(adopt).await?;
            }
        }
        this.owner = true;
        Ok(this)
    }

    fn event(&self, kind: &'static str, outcome: &'static str, started: Instant, detail: String) {
        let mut record = self.blank_record(kind, outcome);
        record.commit_ms = ms(started.elapsed());
        record.detail = Some(detail);
        let _ = self.out.send(record);
    }

    fn blank_record(&self, kind: &'static str, outcome: &'static str) -> CommitRecord {
        CommitRecord {
            kind,
            ts_ms: now_millis(),
            table: self.cfg.table_name.clone(),
            app_id: self.app_id.clone(),
            txn_version: -1,
            delta_version: self.table.version(),
            files: 0,
            rows: 0,
            recovered: false,
            queue_ms: 0.0,
            permit_wait_ms: 0.0,
            checkpoint_permit_wait_ms: None,
            manifest_ms: 0.0,
            commit_ms: 0.0,
            visible_oldest_ms: 0.0,
            visible_newest_ms: 0.0,
            library_retries: 0,
            loop_attempts: 0,
            outcome,
            errors: Vec::new(),
            storage: CounterSnapshot::default(),
            checkpoint_ms: None,
            checkpoint_reload_ms: None,
            detail: None,
        }
    }

    /// Commit `txns` with no data, tagged with `kind`.
    async fn commit_txns(&mut self, txns: Vec<(String, i64)>, kind: &str) -> Result<()> {
        self.table
            .commit(CommitReq {
                txns,
                files: &[],
                kind,
                data: None,
            })
            .await
            .map(|_| ())
    }

    /// Claim the next epoch appId for this identity. One commit fences the identity's previous
    /// appIds and advances its epoch counter, so concurrent claims (two holders of one WAL)
    /// conflict and serialize. Afterwards no earlier appId of this identity can commit, and
    /// `own_floor` is the highest version any of them committed.
    ///
    /// delta-rs serializes concurrent claims through its conflict checker (both write the
    /// `#epoch` appId). Kernel serializes them more strictly: a claim built on snapshot `v` can
    /// only land as version `v + 1`, so every landed claim read the counter and the fenced
    /// appIds from the immediately preceding version. A loser gets `Conflicted`, and the next
    /// attempt re-reads everything from a refreshed snapshot.
    async fn claim(&mut self, kind: &'static str) -> Result<()> {
        let started = Instant::now();
        let base = self.cfg.app_id.clone();
        let counter = epoch_counter_id(&base);
        let mut delay = Duration::from_millis(100);
        for attempt in 1..=30 {
            self.refresh().await?;
            let (last_epoch, mut floor) = self.txn_version_of(&counter).await?.map_or((0, -1), decode_claim);
            let mut txns = Vec::new();
            let mut previous = vec![base.clone()];
            if last_epoch > 0 {
                previous.push(epoch_app_id(&base, last_epoch));
            }
            for app in previous {
                match self.txn_version_of(&app).await? {
                    // An empty appId must still be fenced: another holder may have claimed it and
                    // be about to commit. Naming it here makes that commit and this claim conflict.
                    None if app != base || self.cfg.epochs == EpochMode::Drain => {
                        txns.push((app, FENCE_OFFSET));
                    }
                    None => {}
                    Some(v) if v >= FENCE_OFFSET => floor = floor.max(decode_fence(v).unwrap_or(-1)),
                    Some(v) => {
                        floor = floor.max(v);
                        txns.push((app, FENCE_OFFSET + v + 1));
                    }
                }
            }
            let epoch = last_epoch + 1;
            txns.push((counter.clone(), encode_claim(epoch, floor)));
            let n = CLAIM_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
            if self.cfg.crash_before_claim == Some(n) {
                eprintln!("fault: aborting before claim commit {n} ({kind} epoch={epoch})");
                std::process::abort();
            }
            match self.commit_txns(txns, "claim").await {
                Ok(()) => {
                    let n = CLAIMS_COMMITTED.fetch_add(1, Ordering::SeqCst) + 1;
                    if self.cfg.crash_after_claim == Some(n) {
                        eprintln!(
                            "fault: aborting after claim commit {n} ({kind} epoch={epoch} claim_version={:?})",
                            self.table.version()
                        );
                        std::process::abort();
                    }
                    self.app_id = epoch_app_id(&base, epoch);
                    self.own_floor = (floor >= 0).then_some(floor);
                    self.last_committed = None;
                    self.verified_through = None;
                    let wal_max = self.wal.max_version()?.unwrap_or(-1);
                    self.next_version = self.next_version.max(floor + 1).max(wal_max + 1);
                    self.event(
                        "claim",
                        "ok",
                        started,
                        format!("{kind} epoch={epoch} floor={floor} attempts={attempt}"),
                    );
                    return Ok(());
                }
                Err(error) => {
                    tracing::warn!(%error, attempt, "epoch claim failed; re-reading");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
            }
        }
        anyhow::bail!("could not claim an epoch for {base}")
    }

    /// The appId an identity base currently commits under.
    async fn current_app_id_of(&self, base: &str) -> Result<String> {
        if self.cfg.epochs != EpochMode::Tenure {
            return Ok(base.to_owned());
        }
        let epoch = self
            .txn_version_of(&epoch_counter_id(base))
            .await?
            .map_or(0, |raw| decode_claim(raw).0);
        Ok(if epoch > 0 {
            epoch_app_id(base, epoch)
        } else {
            base.to_owned()
        })
    }

    /// The appId of the most recent data commit by an owner with another identity, searching
    /// back `scan_window` versions and stopping at this identity's own owner commits.
    async fn previous_owner(&self) -> Result<Option<String>> {
        let latest = self.table.version().unwrap_or(0);
        let from = latest.saturating_sub(self.cfg.scan_window);
        let mine = base_of(&self.cfg.app_id).to_owned();
        for version in (from..=latest).rev() {
            let Some(bytes) = self.table.read_commit_entry(version).await? else {
                continue;
            };
            let mut kind = None;
            let mut apps = Vec::new();
            for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
                let Ok(value) = serde_json::from_slice::<serde_json::Value>(line) else {
                    continue;
                };
                if let Some(k) = value
                    .pointer(&format!("/commitInfo/{KIND_KEY}"))
                    .and_then(|v| v.as_str())
                {
                    kind = Some(k.to_owned());
                }
                if let (Some(app), Some(v)) = (
                    value.pointer("/txn/appId").and_then(|v| v.as_str()),
                    value.pointer("/txn/version").and_then(serde_json::Value::as_i64),
                ) && v < FENCE_OFFSET
                {
                    apps.push(app.to_owned());
                }
            }
            if kind.as_deref() != Some("owner") {
                continue;
            }
            if let Some(app) = apps.into_iter().next() {
                return Ok((base_of(&app) != mine).then_some(app));
            }
        }
        Ok(None)
    }

    /// Fence other owners per `fence_on`.
    async fn fence_others(&mut self) -> Result<()> {
        let targets = match &self.cfg.fence_on {
            FenceOn::None => return Ok(()),
            FenceOn::Explicit(ids) => {
                let mut targets = Vec::new();
                for id in ids.clone() {
                    targets.push(self.current_app_id_of(&format!("{id}/{}", self.cfg.table_name)).await?);
                }
                targets
            }
            FenceOn::Inferred => {
                self.refresh().await?;
                self.previous_owner().await?.into_iter().collect()
            }
        };
        for target in targets {
            let started = Instant::now();
            let last = self.fence(&target).await?;
            self.event("fence", "ok", started, format!("target={target} last={last:?}"));
        }
        Ok(())
    }

    /// Commit pending manifests under the current appId, in version order. `Ok(false)` means
    /// the appId was fenced part way.
    async fn recommit_pending(&mut self) -> Result<bool> {
        for manifest in self.wal.pending()? {
            match self.landed(&manifest).await? {
                Landed::Yes => {
                    self.wal.release(manifest.version)?;
                    self.emit_replay(&manifest, "replay_already_landed");
                }
                Landed::No => {
                    let record = self.commit_manifest(&manifest, true).await;
                    let outcome = record.outcome;
                    let _ = self.out.send(record);
                    match outcome {
                        "fenced" => return Ok(false),
                        "failed" | "unresolved" => {
                            anyhow::bail!("replay of txn version {} did not complete: {outcome}", manifest.version)
                        }
                        _ => self.wal.release(manifest.version)?,
                    }
                }
                Landed::Fenced => return Ok(false),
            }
        }
        Ok(true)
    }

    /// Self-drain: claim a drain epoch and commit this identity's pending manifests. A drain
    /// that is itself fenced (another holder of the same WAL claimed) starts over.
    async fn drain(&mut self) -> Result<()> {
        let started = Instant::now();
        let pending = self.wal.pending()?.len();
        for round in 1..=20 {
            self.claim("drain").await?;
            let mut fenced = false;
            let mut committed = 0;
            for manifest in self.wal.pending()? {
                match self.landed(&manifest).await? {
                    Landed::Yes => {
                        self.wal.release(manifest.version)?;
                        self.emit_replay(&manifest, "drain_already_landed");
                    }
                    Landed::Fenced => {
                        fenced = true;
                        break;
                    }
                    Landed::No => {
                        let mut record = self.commit_manifest(&manifest, true).await;
                        record.detail = Some("drain".to_owned());
                        let outcome = record.outcome;
                        let _ = self.out.send(record);
                        match outcome {
                            "fenced" => {
                                fenced = true;
                                break;
                            }
                            "failed" | "unresolved" => {
                                anyhow::bail!("drain of txn version {} did not complete: {outcome}", manifest.version)
                            }
                            _ => {
                                committed += 1;
                                self.wal.release(manifest.version)?;
                            }
                        }
                    }
                }
            }
            if !fenced {
                self.event(
                    "drain_done",
                    "ok",
                    started,
                    format!("pending={pending} committed={committed} rounds={round}"),
                );
                return Ok(());
            }
        }
        anyhow::bail!("drain fenced 20 times")
    }

    /// Take ownership when a file arrives while not the owner. `Ok(false)` means this identity's
    /// owner appId is fenced and cannot be reused (epochs off or drain-only).
    async fn acquire_owner(&mut self) -> Result<bool> {
        let started = Instant::now();
        if self.cfg.epochs == EpochMode::Tenure {
            // Another owner can fence the new epoch before its replay finishes; take it again.
            for attempt in 1..=10 {
                self.fence_others().await?;
                self.claim("owner").await?;
                self.owner = true;
                if self.recommit_pending().await? {
                    self.event("acquire", "ok", started, format!("attempts={attempt}"));
                    return Ok(true);
                }
                self.owner = false;
            }
            anyhow::bail!("could not take ownership in 10 attempts");
        } else {
            self.refresh().await?;
            self.app_id = self.cfg.app_id.clone();
            self.own_floor = None;
            self.last_committed = None;
            self.verified_through = None;
            if self.committed_txn_version().await?.is_some_and(|v| v >= FENCE_OFFSET) {
                return Ok(false);
            }
            self.fence_others().await?;
            self.last_committed = self.committed_txn_version().await?;
        }
        self.owner = true;
        if !self.recommit_pending().await? {
            self.owner = false;
            return Ok(false);
        }
        self.event("acquire", "ok", started, String::new());
        Ok(true)
    }

    async fn txn_version_of(&self, app_id: &str) -> Result<Option<i64>> {
        self.table.txn_version_of(app_id).await
    }

    async fn committed_txn_version(&self) -> Result<Option<i64>> {
        self.txn_version_of(&self.app_id).await
    }

    /// Commit a `txn`-only entry that fences `old_app_id`, retrying until it lands or the appId
    /// is already fenced. Returns the fenced owner's last committed version.
    ///
    /// The fence carries the same appId as the old owner's commits, so the delta-rs conflict
    /// checker rejects whichever of the two loses the race with ConcurrentTransaction. Every
    /// old-owner commit is therefore either below the fence (and counted in the returned
    /// version) or rejected.
    ///
    /// Kernel has no conflict checker; the same holds because every kernel commit is a
    /// put-if-absent at exactly its snapshot's version + 1. If the fence lands first at `v`, an
    /// old-owner commit built on a snapshot below `v` targets a version at or below `v` and gets
    /// `Conflicted`; the old owner's `resolve()` then refreshes and its landed check sees the
    /// fence (`Landed::Fenced`). An old-owner commit built on a snapshot at or above `v` sees the
    /// fence in its pre-commit landed check. If the old-owner commit lands first, this fence gets
    /// `Conflicted`, and the next attempt re-reads the appId's version from a refreshed snapshot.
    async fn fence(&mut self, old_app_id: &str) -> Result<Option<i64>> {
        let mut delay = Duration::from_millis(100);
        for attempt in 1..=30 {
            self.refresh().await?;
            let raw = self.txn_version_of(old_app_id).await?;
            if let Some(raw) = raw.filter(|v| *v >= FENCE_OFFSET) {
                return Ok(decode_fence(raw));
            }
            let marker = FENCE_OFFSET + raw.unwrap_or(-1) + 1;
            match self.commit_txns(vec![(old_app_id.to_owned(), marker)], "fence").await {
                Ok(()) => {
                    tracing::warn!(old_app_id, last = ?raw, fence_version = ?self.table.version(), "fenced old owner");
                }
                Err(error) => {
                    tracing::warn!(%error, attempt, "fence commit failed; re-reading");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
            }
        }
        anyhow::bail!("could not fence {old_app_id}")
    }

    async fn refresh(&mut self) -> Result<()> {
        let mut delay = Duration::from_millis(50);
        for _ in 0..20 {
            match self.table.refresh().await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(%error, "refresh failed");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
            }
        }
        self.table.refresh().await.context("refresh after retries")
    }

    async fn scan_log_for(&self, path: &str) -> Result<bool> {
        let latest = self.table.version().unwrap_or(0);
        let from = latest.saturating_sub(self.cfg.scan_window);
        let needle = format!("\"path\":\"{path}\"");
        for version in (from..=latest).rev() {
            if let Some(bytes) = self.table.read_commit_entry(version).await?
                && bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn landed(&self, manifest: &Manifest) -> Result<Landed> {
        match self.cfg.idempotency {
            Idempotency::Txn => {
                if self.own_floor.is_some_and(|floor| manifest.version <= floor) {
                    return Ok(Landed::Yes);
                }
                let raw = self.committed_txn_version().await?;
                Ok(match raw {
                    Some(v) if v >= FENCE_OFFSET => {
                        if decode_fence(v).is_some_and(|last| last >= manifest.version) {
                            Landed::Yes
                        } else {
                            Landed::Fenced
                        }
                    }
                    Some(v) if v >= manifest.version => Landed::Yes,
                    // The entry went backwards or vanished (retention expiry): someone else owns
                    // this appId's history now, so committing could duplicate.
                    v if self.last_committed.is_some_and(|last| v.is_none_or(|v| v < last)) => Landed::Fenced,
                    _ => Landed::No,
                })
            }
            Idempotency::ScanLog => match manifest.files.first() {
                Some(file) => Ok(if self.scan_log_for(&file.path).await? {
                    Landed::Yes
                } else {
                    Landed::No
                }),
                None => Ok(Landed::Yes),
            },
        }
    }

    async fn recover(&mut self, adopt: Option<Adopt>) -> Result<()> {
        let raw = self.committed_txn_version().await?;
        if raw.is_some_and(|v| v >= FENCE_OFFSET) {
            anyhow::bail!(
                "fenced: appId {} was taken over; pending manifests belong to the new owner",
                self.app_id
            );
        }
        self.last_committed = raw;
        let wal_max = self.wal.max_version()?.unwrap_or(-1);
        self.next_version = raw.unwrap_or(-1).max(wal_max) + 1;
        for manifest in self.wal.pending()? {
            match self.landed(&manifest).await? {
                Landed::Yes => {
                    self.wal.release(manifest.version)?;
                    self.emit_replay(&manifest, "replay_already_landed");
                }
                Landed::No => self.commit_recovered(&manifest).await?,
                Landed::Fenced => anyhow::bail!("fenced during replay of txn version {}", manifest.version),
            }
        }
        if let Some(Adopt {
            wal: old_wal,
            app_id: old_app_id,
        }) = adopt
        {
            let old_last = self.fence(&old_app_id).await?;
            // A restart in the middle of adoption may find a batch in both WALs.
            let mine: std::collections::HashSet<String> = self
                .wal
                .pending()?
                .into_iter()
                .chain(self.wal.released()?)
                .filter_map(|m| m.files.first().map(|f| f.path.clone()))
                .collect();
            for old in old_wal.pending()? {
                let old_version = old.version;
                let already_landed = old_last.is_some_and(|last| old_version <= last);
                let already_mine = old.files.first().is_some_and(|f| mine.contains(&f.path));
                if already_landed || already_mine {
                    old_wal.release(old_version)?;
                    if already_landed {
                        self.emit_replay(&old, "adopt_already_landed");
                    }
                    continue;
                }
                let manifest = Manifest {
                    app_id: self.app_id.clone(),
                    version: self.next_version,
                    ..old
                };
                self.next_version += 1;
                self.wal.write(&manifest).await?;
                old_wal.release(old_version)?;
                self.commit_recovered(&manifest).await?;
            }
        }
        Ok(())
    }

    async fn commit_recovered(&mut self, manifest: &Manifest) -> Result<()> {
        let record = self.commit_manifest(manifest, true).await;
        let outcome = record.outcome;
        let _ = self.out.send(record);
        if matches!(outcome, "failed" | "unresolved" | "fenced") {
            anyhow::bail!("replay of txn version {} did not complete: {outcome}", manifest.version);
        }
        self.wal.release(manifest.version)
    }

    fn emit_replay(&self, manifest: &Manifest, outcome: &'static str) {
        let mut record = self.blank_record("commit", outcome);
        record.txn_version = manifest.version;
        record.files = manifest.files.len();
        record.rows = manifest.files.iter().map(|f| f.rows).sum();
        record.recovered = true;
        let _ = self.out.send(record);
    }

    /// After a failed commit, decide whether the batch landed. Only a fresh snapshot and a
    /// successful check count as an answer. `None` means storage never gave one; the caller
    /// must not recommit, because a blind retry can duplicate the batch.
    async fn resolve(&mut self, manifest: &Manifest, errors: &mut Vec<String>) -> Option<Landed> {
        let mut delay = Duration::from_millis(100);
        for _ in 0..20 {
            match self.refresh().await {
                Ok(()) => match self.landed(manifest).await {
                    Ok(answer) => return Some(answer),
                    Err(error) => errors.push(format!("landed check: {error}")),
                },
                Err(error) => errors.push(format!("refresh: {error}")),
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(5));
        }
        None
    }

    async fn commit_manifest(&mut self, manifest: &Manifest, recovered: bool) -> CommitRecord {
        let started = Instant::now();
        let before = self.counters.snapshot();
        let mut errors = Vec::new();
        let mut attempts = 0;
        let mut library_retries = 0;
        let mut outcome = "failed";
        let mut delay = Duration::from_millis(100);
        while attempts < self.cfg.loop_max_attempts {
            attempts += 1;
            // The conflict checker only sees commits after our snapshot. A fence already inside
            // the snapshot must be caught here, unless the snapshot is already verified. With
            // kernel this check is the only guard: the commit below lands at exactly
            // `snapshot + 1` or fails with `Conflicted`, and every failure goes through
            // `resolve()` (refresh + landed check) before the next attempt.
            let verified = self.verified_through.is_some() && self.verified_through == self.table.version();
            let pre_check = if verified {
                Ok(Landed::No)
            } else {
                self.landed(manifest).await
            };
            match pre_check {
                Ok(Landed::No) => self.verified_through = self.table.version(),
                Ok(Landed::Yes) => {
                    outcome = "landed_after_error";
                    break;
                }
                Ok(Landed::Fenced) => {
                    outcome = "fenced";
                    break;
                }
                Err(error) => {
                    errors.push(format!("pre-commit check: {error}"));
                    match self.resolve(manifest, &mut errors).await {
                        Some(Landed::No) => {}
                        Some(Landed::Yes) => {
                            outcome = "landed_after_error";
                            break;
                        }
                        Some(Landed::Fenced) => {
                            outcome = "fenced";
                            break;
                        }
                        None => {
                            outcome = "unresolved";
                            break;
                        }
                    }
                }
            }
            let txns = if self.cfg.idempotency == Idempotency::Txn {
                vec![(self.app_id.clone(), manifest.version)]
            } else {
                Vec::new()
            };
            let kind = if self.owner { "owner" } else { "drain" };
            let result = self
                .table
                .commit(CommitReq {
                    txns,
                    files: &manifest.files,
                    kind,
                    data: Some(DataOpts {
                        inline_checkpoint: self.cfg.inline_checkpoint,
                        cleanup_expired_logs: self.cfg.cleanup_expired_logs,
                        max_retries: self.cfg.library_max_retries,
                    }),
                })
                .await;
            match result {
                Ok(retries) => {
                    library_retries += retries;
                    self.verified_through = self.table.version();
                    outcome = if attempts == 1 { "ok" } else { "ok_after_loop_retry" };
                    break;
                }
                Err(error) => {
                    errors.push(error.to_string());
                    match self.resolve(manifest, &mut errors).await {
                        Some(Landed::Yes) => {
                            outcome = "landed_after_error";
                            break;
                        }
                        Some(Landed::Fenced) => {
                            outcome = "fenced";
                            break;
                        }
                        Some(Landed::No) => {}
                        None => {
                            outcome = "unresolved";
                            break;
                        }
                    }
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
            }
        }
        if matches!(outcome, "ok" | "ok_after_loop_retry" | "landed_after_error") {
            self.last_committed = Some(
                self.last_committed
                    .map_or(manifest.version, |v| v.max(manifest.version)),
            );
        }
        let commit_ms = ms(started.elapsed());
        let now = now_millis();
        let oldest = manifest.files.iter().map(|f| f.upload_done_ms).min().unwrap_or(now);
        let newest = manifest.files.iter().map(|f| f.upload_done_ms).max().unwrap_or(now);
        CommitRecord {
            kind: "commit",
            ts_ms: now,
            table: self.cfg.table_name.clone(),
            app_id: self.app_id.clone(),
            txn_version: manifest.version,
            delta_version: self.table.version(),
            files: manifest.files.len(),
            rows: manifest.files.iter().map(|f| f.rows).sum(),
            recovered,
            queue_ms: 0.0,
            permit_wait_ms: 0.0,
            checkpoint_permit_wait_ms: None,
            manifest_ms: 0.0,
            commit_ms,
            #[allow(clippy::cast_precision_loss)]
            visible_oldest_ms: (now - oldest) as f64,
            #[allow(clippy::cast_precision_loss)]
            visible_newest_ms: (now - newest) as f64,
            library_retries,
            loop_attempts: attempts,
            outcome,
            errors,
            storage: self.counters.snapshot().since(&before),
            checkpoint_ms: None,
            checkpoint_reload_ms: None,
            detail: None,
        }
    }

    /// The checkpoint write, by [`CheckpointExec`]. Both build a fresh snapshot at `version` and
    /// write the checkpoint with the kernel, as `create_checkpoint` does.
    async fn write_checkpoint(&self, version: u64) -> Result<()> {
        self.table.write_checkpoint(version, self.cfg.checkpoint_exec).await
    }

    /// Returns (checkpoint write ms, snapshot reload ms, permit wait ms).
    async fn maybe_checkpoint(&mut self) -> Option<(f64, Option<f64>, f64)> {
        let every = self.cfg.separate_checkpoint_every?;
        let version = self.table.version()?;
        if version == 0 || version % every != 0 {
            return None;
        }
        let (_permit, wait_ms) = acquire(self.cfg.limits.checkpoints.as_ref()).await;
        let started = Instant::now();
        match self.write_checkpoint(version).await {
            Ok(()) => {
                let write_ms = ms(started.elapsed());
                // An incremental snapshot update carries every earlier file batch forward and adds
                // one per commit; batches are never compacted, so per-commit work and memory grow
                // with the version. A fresh load from the checkpoint resets both.
                if !self.cfg.reload_after_checkpoint {
                    return Some((write_ms, None, wait_ms));
                }
                let started = Instant::now();
                match self.table.reload_at(version).await {
                    Ok(()) => Some((write_ms, Some(ms(started.elapsed())), wait_ms)),
                    Err(error) => {
                        tracing::warn!(%error, "snapshot reload after checkpoint failed");
                        Some((write_ms, None, wait_ms))
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "checkpoint failed");
                None
            }
        }
    }

    /// Run until `rx` closes and drains.
    pub async fn run(mut self, mut rx: mpsc::Receiver<FileReady>) -> Result<()> {
        let mut next_tick = tokio::time::Instant::now();
        loop {
            let mut batch = Vec::new();
            match self.cfg.pacing {
                Pacing::Group => match rx.recv().await {
                    Some(file) => batch.push(file),
                    None => break,
                },
                Pacing::Interval(interval) => {
                    next_tick += interval;
                    tokio::time::sleep_until(next_tick).await;
                    if rx.is_closed() && rx.is_empty() {
                        break;
                    }
                }
            }
            while batch.len() < self.cfg.max_files_per_commit {
                match rx.try_recv() {
                    Ok(file) => batch.push(file),
                    Err(_) => break,
                }
            }
            if batch.is_empty() {
                continue;
            }
            if !self.owner && !self.acquire_owner().await? {
                // The owner appId is fenced for good. The files stay pending: nothing may commit them.
                let manifest = Manifest {
                    app_id: self.app_id.clone(),
                    version: self.next_version,
                    created_ms: now_millis(),
                    files: batch,
                };
                self.next_version += 1;
                self.wal.write(&manifest).await?;
                let mut record = self.blank_record("locked_out", "locked_out");
                record.txn_version = manifest.version;
                record.files = manifest.files.len();
                let _ = self.out.send(record);
                continue;
            }
            let commit_start = now_millis();
            let manifest_started = Instant::now();
            let manifest = Manifest {
                app_id: self.cfg.app_id.clone(),
                version: self.next_version,
                created_ms: commit_start,
                files: batch,
            };
            self.next_version += 1;
            self.wal.write(&manifest).await?;
            let manifest_ms = ms(manifest_started.elapsed());
            let (permit, permit_wait_ms) = acquire(self.cfg.limits.ops.as_ref()).await;
            let mut record = self.commit_manifest(&manifest, false).await;
            drop(permit);
            record.manifest_ms = manifest_ms;
            record.permit_wait_ms = permit_wait_ms;
            #[allow(clippy::cast_precision_loss)]
            {
                record.queue_ms = (commit_start
                    - manifest
                        .files
                        .iter()
                        .map(|f| f.upload_done_ms)
                        .min()
                        .unwrap_or(commit_start)) as f64;
            }
            if record.outcome == "fenced" && self.cfg.epochs != EpochMode::Off {
                let _ = self.out.send(record);
                // Ownership moved. Files already routed here are acked: write them to the WAL so
                // the drain covers them, then drain and wait for traffic to return.
                self.owner = false;
                let lost = Instant::now();
                let mut rest = Vec::new();
                while let Ok(file) = rx.try_recv() {
                    rest.push(file);
                }
                if !rest.is_empty() {
                    let manifest = Manifest {
                        app_id: self.app_id.clone(),
                        version: self.next_version,
                        created_ms: now_millis(),
                        files: rest,
                    };
                    self.next_version += 1;
                    self.wal.write(&manifest).await?;
                }
                self.event("lost", "fenced", lost, format!("pending={}", self.wal.pending()?.len()));
                self.drain().await?;
                continue;
            }
            if matches!(record.outcome, "failed" | "unresolved" | "fenced") {
                let _ = self.out.send(record);
                anyhow::bail!("commit of txn version {} did not complete", manifest.version);
            }
            self.wal.release(manifest.version)?;
            if let Some((write_ms, reload_ms, wait_ms)) = self.maybe_checkpoint().await {
                record.checkpoint_ms = Some(write_ms);
                record.checkpoint_reload_ms = reload_ms;
                record.checkpoint_permit_wait_ms = Some(wait_ms);
            }
            let _ = self.out.send(record);
        }
        Ok(())
    }
}
