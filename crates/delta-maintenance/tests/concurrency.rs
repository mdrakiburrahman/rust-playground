//! Process-level concurrency proof: a separate blind-append writer process runs
//! while the standalone service binary applies OPTIMIZE, PURGE and VACUUM LITE.
#![cfg(target_os = "linux")]

use anyhow::{Context, Result, anyhow, bail, ensure};
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_deltalake::delta::{DeltaTable, INGESTION_PARTITION, ingestion_partition_value};
use arrow_deltalake::maintenance::{DISPOSABLE_TEST_MARKER, MAX_COMMIT_ATTEMPTS, now_ms};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use url::Url;

const TEST: &str = "separate_writer_process_with_service_optimize_purge_vacuum";
const ROLE: &str = "DELTA_MAINTENANCE_CONCURRENCY_ROLE";
const ROOT: &str = "DELTA_MAINTENANCE_CONCURRENCY_ROOT";
const BASE: &str = "DELTA_MAINTENANCE_CONCURRENCY_BASE";
/// Writer receipts, outside the table directory.
const RECEIPTS: &str = "writer-receipts";
const MINUTE: i64 = 60_000;
/// Predeclared bounds: writer publications (each replayed once) and service cycles.
const PUBLICATIONS: i64 = 24;
const CYCLES: usize = 6;
/// Partitions are minute offsets from `base` (30 minutes before start). With
/// purge retention 26.5 minutes and a run shorter than 5 minutes, the purge cutoff
/// stays inside (base+3, base+9]: 0..=2 always expire and 10..=20 never do.
const EXPIRED: [i64; 3] = [0, 1, 2];
const RETAINED: std::ops::RangeInclusive<i64> = 10..=20;
const WRITER: [i64; 4] = [16, 17, 18, 19];
/// Latest 2 sealed partitions below the protected newest (20).
const LATEST: [i64; 2] = [18, 19];
const PURGE_RETENTION_SECS: &str = "1590";

/// Hard deadline for the driver; every child wait is bounded by it.
const DRIVER_DEADLINE: Duration = Duration::from_secs(300);
/// The outer process allows the driver its deadline plus time to start and reap.
const OUTER_DEADLINE: Duration = Duration::from_secs(360);

type Row = (i64, String);

/// A spawned child owned by this test: dropping it without a successful wait kills
/// and reaps that exact process, so a failed assertion never leaves it running
/// while the temporary root is removed.
struct OwnedChild {
    child: Child,
    reaped: bool,
}

impl OwnedChild {
    fn spawn(command: &mut Command) -> Result<Self> {
        Ok(Self {
            child: command.spawn()?,
            reaped: false,
        })
    }

    fn running(&mut self) -> Result<bool> {
        match self.child.try_wait()? {
            Some(_) => {
                self.reaped = true;
                Ok(false)
            }
            None => Ok(true),
        }
    }

    /// Poll until exit or `deadline`; on timeout kill and reap, then fail.
    fn wait_until(&mut self, deadline: Instant, what: &str) -> Result<ExitStatus> {
        loop {
            if let Some(status) = self.child.try_wait()? {
                self.reaped = true;
                return Ok(status);
            }
            if Instant::now() >= deadline {
                let pid = self.child.id();
                let cleanup = self.kill_and_reap();
                bail!("{what} (pid {pid}) exceeded its deadline; kill and reap: {cleanup:?}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Kill and reap this exact child. Cleanup counts as confirmed only once `wait`
    /// returns its exit status; a kill error alone is reported, not assumed fatal.
    fn kill_and_reap(&mut self) -> Result<()> {
        if self.reaped {
            return Ok(());
        }
        let killed = self.child.kill();
        self.child
            .wait()
            .with_context(|| format!("reap pid {} after kill {killed:?}", self.child.id()))?;
        self.reaped = true;
        Ok(())
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Err(error) = self.kill_and_reap() {
            eprintln!("child cleanup unconfirmed: {error:#}");
        }
    }
}

/// The outer test process's custody of a driver and every descendant: the driver
/// leads its own process group (inherited by writer/service children), and this
/// process is a child subreaper, so descendants orphaned by a killed driver are
/// reparented here and reaped. Cleanup is confirmed only when the group is empty.
struct ProcessTree {
    driver: Child,
    group: rustix::process::Pid,
    settled: bool,
}

impl ProcessTree {
    fn spawn(command: &mut Command) -> Result<Self> {
        use std::os::unix::process::CommandExt;
        rustix::process::set_child_subreaper(Some(rustix::process::getpid()))?;
        let driver = command.process_group(0).spawn()?;
        let group = rustix::process::Pid::from_child(&driver);
        Ok(Self {
            driver,
            group,
            settled: false,
        })
    }

    /// Wait for the driver until `deadline`, then sweep its whole group. On timeout
    /// the group is killed first and the call fails.
    fn wait_until(&mut self, deadline: Instant) -> Result<ExitStatus> {
        loop {
            if let Some(status) = self.driver.try_wait()? {
                self.sweep()?;
                return Ok(status);
            }
            if Instant::now() >= deadline {
                let killed = self.kill_group();
                let reaped = self.driver.wait().map(|_| ());
                let swept = self.sweep();
                bail!(
                    "driver group {} exceeded its deadline; kill {killed:?} reap {reaped:?} sweep {swept:?}",
                    self.group.as_raw_nonzero()
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn kill_group(&self) -> Result<()> {
        match rustix::process::kill_process_group(self.group, rustix::process::Signal::KILL) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
            Err(error) => Err(anyhow!("kill process group: {error}")),
        }
    }

    /// Kill any surviving member, reap every reparented member, and confirm that no
    /// process remains in the group.
    fn sweep(&mut self) -> Result<()> {
        self.kill_group()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match rustix::process::waitpgid(self.group, rustix::process::WaitOptions::NOHANG) {
                Ok(Some(_)) => continue,
                Ok(None) | Err(rustix::io::Errno::CHILD) => {}
                Err(error) => bail!("reap process group: {error}"),
            }
            match rustix::process::test_kill_process_group(self.group) {
                Err(rustix::io::Errno::SRCH) => {
                    self.settled = true;
                    return Ok(());
                }
                Ok(()) | Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                other => bail!("process group still present after kill: {other:?}"),
            }
        }
    }
}

impl Drop for ProcessTree {
    fn drop(&mut self) {
        if !self.settled {
            let killed = self.kill_group();
            let reaped = self.driver.wait().map(|_| ());
            if let Err(error) = self.sweep() {
                eprintln!("driver group cleanup unconfirmed: kill {killed:?} reap {reaped:?}: {error:#}");
            }
        }
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(INGESTION_PARTITION, DataType::Utf8, false),
    ]))
}

fn table_dir(root: &Path) -> PathBuf {
    root.join("events")
}

fn open(root: &Path) -> Result<Arc<DeltaTable>> {
    let url = Url::from_directory_path(fs::canonicalize(table_dir(root))?).map_err(|()| anyhow!("URL"))?;
    DeltaTable::open_url(url, schema(), vec![INGESTION_PARTITION.into()])
}

fn stamp(base: i64, offset: i64) -> i64 {
    base + offset * MINUTE + 1
}

fn partition(base: i64, offset: i64) -> Result<String> {
    ingestion_partition_value(stamp(base, offset))
}

fn publish(table: &Arc<DeltaTable>, base: i64, offset: i64, ids: &[i64], app_id: Option<&str>) -> Result<u64> {
    let value = partition(base, offset)?;
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vec![value.as_str(); ids.len()])),
        ],
    )?;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let mut file = table
            .new_file_partitioned(
                schema(),
                BTreeMap::from([(INGESTION_PARTITION.to_owned(), Some(value.clone()))]),
            )?
            .with_ingestion_time(stamp(base, offset))?;
        if let Some(app_id) = app_id {
            file = file.with_transaction(app_id.to_owned(), 0)?;
        }
        file.write(&batch).await?;
        Ok(file.finish().await?.version)
    })
}

fn writer_ids(index: i64) -> Vec<i64> {
    (0..5).map(|row| 100_000 + index * 10 + row).collect()
}

fn writer_offset(index: i64) -> i64 {
    WRITER[usize::try_from(index).unwrap_or(0) % WRITER.len()]
}

fn writer_app(index: i64) -> String {
    format!("conc.writer.{index}")
}

/// Durably append one `app,first,replay` receipt line for the driver's log oracle.
fn record_receipt(root: &Path, app_id: &str, first: u64, replay: u64) -> Result<()> {
    use std::io::Write;
    let mut receipts = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join(RECEIPTS))?;
    writeln!(receipts, "{app_id},{first},{replay}")?;
    receipts.sync_all()?;
    Ok(())
}

/// Child role: independent publications, each followed by a replay of the same identity.
/// A deduplicated replay reports the snapshot version it observed, which is later than
/// the original commit whenever the concurrent service committed in between; the driver
/// proves from the log that the replay itself never committed.
fn writer(root: &Path, base: i64) -> Result<()> {
    let table = open(root)?;
    for index in 0..PUBLICATIONS {
        let app_id = writer_app(index);
        let ids = writer_ids(index);
        let first = publish(&table, base, writer_offset(index), &ids, Some(&app_id))?;
        if index == 0 {
            fs::write(root.join("writer-started"), b"1")?;
        }
        let replay = publish(&table, base, writer_offset(index), &ids, Some(&app_id))?;
        ensure!(
            replay >= first,
            "{app_id}: replay observed version {replay} before its original commit {first}"
        );
        record_receipt(root, &app_id, first, replay)?;
    }
    Ok(())
}

/// Writer receipts by application ID: (original commit version, replay observed version).
fn receipts(root: &Path) -> Result<BTreeMap<String, (u64, u64)>> {
    let mut receipts = BTreeMap::new();
    for line in fs::read_to_string(root.join(RECEIPTS))?.lines() {
        let mut fields = line.split(',');
        let (Some(app), Some(first), Some(replay), None) = (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            bail!("malformed writer receipt {line:?}");
        };
        ensure!(
            receipts
                .insert(app.to_owned(), (first.parse()?, replay.parse()?))
                .is_none(),
            "duplicate writer receipt for {app}"
        );
    }
    Ok(receipts)
}

/// Every writer publication commits its transaction exactly once, at version 0, in the
/// exact commit its original call reported; its replay observed a version in
/// `[first, last]` and no commit in `(first, replay]` carries that identity.
fn check_writer_receipts(
    receipts: &BTreeMap<String, (u64, u64)>,
    txns: &BTreeMap<String, Vec<(u64, Option<i64>)>>,
    last: u64,
) -> Result<()> {
    let expected: BTreeSet<String> = (0..PUBLICATIONS).map(writer_app).collect();
    let received: BTreeSet<String> = receipts.keys().cloned().collect();
    ensure!(
        received == expected,
        "writer receipts {} of {PUBLICATIONS}: {received:?}",
        received.len()
    );
    let committed: BTreeSet<String> = txns.keys().cloned().collect();
    ensure!(committed == expected, "writer txn identities {committed:?}");
    for (app, (first, replay)) in receipts {
        let commits = txns.get(app).context("writer txn missing")?;
        ensure!(
            commits.len() == 1,
            "{app}: txn committed {} times: {commits:?}",
            commits.len()
        );
        ensure!(
            commits.iter().all(|(_, version)| *version == Some(0)),
            "{app}: txn version {commits:?}"
        );
        ensure!(
            commits.iter().all(|(commit, _)| commit == first),
            "{app}: original receipt {first} but txn commits {commits:?}"
        );
        ensure!(
            first <= replay && *replay <= last,
            "{app}: replay receipt {replay} outside [{first}, {last}]"
        );
        ensure!(
            !commits.iter().any(|(commit, _)| commit > first && commit <= replay),
            "{app}: replay committed its identity again: {commits:?}"
        );
    }
    Ok(())
}

fn service(root: &Path, operations: &[&str], deadline: Instant) -> Result<Vec<serde_json::Value>> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_delta-maintenance"));
    command.env_remove("OTEL_EXPORTER_OTLP_ENDPOINT").args([
        "--root",
        Url::from_directory_path(root).map_err(|()| anyhow!("URL"))?.as_str(),
        "--table",
        "events",
        "--granularity",
        "minute",
        "--purge-retention-secs",
        PURGE_RETENTION_SECS,
        "--latest-partitions",
        "2",
        "--vacuum-retention-secs",
        "0",
        "--unsafe-short-vacuum-for-tests",
        "--no-metrics-listener",
        "--apply",
    ]);
    for operation in operations {
        command.args(["--operation", operation]);
    }
    // Files, not pipes: a child blocked on a full pipe could never reach the deadline.
    let stdout_path = root.join("service.stdout");
    let stderr_path = root.join("service.stderr");
    command
        .stdin(Stdio::null())
        .stdout(fs::File::create(&stdout_path)?)
        .stderr(fs::File::create(&stderr_path)?);
    let status = OwnedChild::spawn(&mut command)?.wait_until(deadline, "service")?;
    let stdout = fs::read(&stdout_path)?;
    let stderr = fs::read(&stderr_path)?;
    let reports = std::str::from_utf8(&stdout)?
        .lines()
        .map(|line| Ok(serde_json::from_str::<serde_json::Value>(line)?))
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        reports.len() == operations.len(),
        "service exit {:?}: {}",
        status.code(),
        String::from_utf8_lossy(&stderr)
    );
    let failed = reports.iter().any(|report| !report["error_kind"].is_null());
    ensure!(status.success() != failed, "exit status must reflect report errors");
    Ok(reports)
}

fn log_actions(root: &Path) -> Result<Vec<(u64, serde_json::Value)>> {
    let log = table_dir(root).join("_delta_log");
    let mut actions = Vec::new();
    for entry in fs::read_dir(&log)? {
        let path = entry?.path();
        let name = path.file_name().and_then(|name| name.to_str()).unwrap_or_default();
        if let Some(version) = name.strip_suffix(".json") {
            let version: u64 = version.parse()?;
            for line in fs::read_to_string(&path)?.lines() {
                actions.push((version, serde_json::from_str(line)?));
            }
        }
    }
    actions.sort_by_key(|(version, _)| *version);
    Ok(actions)
}

fn active(actions: &[(u64, serde_json::Value)]) -> Result<BTreeMap<String, String>> {
    let mut files = BTreeMap::new();
    for (_, action) in actions {
        if let Some(add) = action.get("add") {
            let value = add["partitionValues"][INGESTION_PARTITION]
                .as_str()
                .context("partition")?;
            files.insert(add["path"].as_str().context("path")?.to_owned(), value.to_owned());
        } else if let Some(remove) = action.get("remove") {
            files.remove(remove["path"].as_str().context("path")?);
        }
    }
    Ok(files)
}

fn physical_parquet(dir: &Path, prefix: &Path, found: &mut BTreeSet<String>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path.file_name().and_then(|name| name.to_str()).unwrap_or_default();
        if name == "_delta_log" {
            continue;
        }
        if path.is_dir() {
            physical_parquet(&path, prefix, found)?;
        } else if name.ends_with(".parquet") {
            found.insert(path.strip_prefix(prefix)?.to_string_lossy().into_owned());
        }
    }
    Ok(())
}

fn decode(relative: &str) -> String {
    relative.replace("%3A", ":").replace("%2D", "-")
}

fn rows(root: &Path, files: &BTreeMap<String, String>) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    for (relative, value) in files {
        let path = table_dir(root).join(decode(relative));
        for batch in ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&path)?)?.build()? {
            let batch = batch?;
            let ids = batch
                .column_by_name("id")
                .context("id")?
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("id type")?;
            rows.extend(ids.values().iter().map(|id| (*id, value.clone())));
        }
    }
    rows.sort();
    Ok(rows)
}

fn driver(root: &Path) -> Result<()> {
    let base = (now_ms()? / MINUTE) * MINUTE - 30 * MINUTE;
    let started = Instant::now();
    let deadline = started + DRIVER_DEADLINE;
    fs::create_dir_all(table_dir(root))?;
    let table = open(root)?;
    let mut expected = Vec::new();
    for offset in EXPIRED.into_iter().chain(RETAINED) {
        for part in 0..2 {
            let ids = [offset * 100 + part];
            publish(&table, base, offset, &ids, None)?;
            if RETAINED.contains(&offset) {
                expected.push((ids[0], partition(base, offset)?));
            }
        }
    }
    fs::write(table_dir(root).join(DISPOSABLE_TEST_MARKER), b"")?;
    for index in 0..PUBLICATIONS {
        let value = partition(base, writer_offset(index))?;
        expected.extend(writer_ids(index).into_iter().map(|id| (id, value.clone())));
    }
    expected.sort();

    let mut child = OwnedChild::spawn(
        Command::new(std::env::current_exe()?)
            .args([TEST, "--exact", "--nocapture", "--test-threads=1"])
            .env(ROLE, "writer")
            .env(ROOT, root)
            .env(BASE, base.to_string()),
    )?;
    let start_deadline = (started + Duration::from_secs(120)).min(deadline);
    while !root.join("writer-started").exists() {
        if Instant::now() >= start_deadline || !child.running()? {
            let cleanup = child.kill_and_reap();
            bail!("writer never started; kill and reap: {cleanup:?}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let operations = ["optimize", "purge", "vacuum"];
    let mut outcomes: BTreeMap<String, usize> = BTreeMap::new();
    let mut max_attempts = 0_u64;
    let mut collisions = 0_u64;
    let mut concurrent_cycles = 0;
    for _ in 0..CYCLES {
        for report in service(root, &operations, deadline)? {
            let label = match report["error_kind"].as_str() {
                Some(kind) => {
                    ensure!(
                        matches!(kind, "stale_plan" | "exhausted"),
                        "unexpected concurrent failure: {report}"
                    );
                    format!("{}:{kind}", report["operation"].as_str().unwrap_or_default())
                }
                None => {
                    let inner = &report["report"];
                    max_attempts = max_attempts.max(inner["attempts"].as_u64().unwrap_or(0));
                    collisions += inner["collisions"].as_u64().unwrap_or(0);
                    format!(
                        "{}:{}",
                        report["operation"].as_str().unwrap_or_default(),
                        inner["outcome"].as_str().unwrap_or_default()
                    )
                }
            };
            *outcomes.entry(label).or_default() += 1;
        }
        if child.running()? {
            concurrent_cycles += 1;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    ensure!(child.wait_until(deadline, "writer")?.success(), "writer process failed");
    ensure!(concurrent_cycles > 0, "no service cycle overlapped the writer");
    ensure!(
        max_attempts <= u64::from(MAX_COMMIT_ATTEMPTS),
        "attempts {max_attempts}"
    );

    std::thread::sleep(Duration::from_millis(20));
    for report in service(root, &operations, deadline)? {
        ensure!(report["error_kind"].is_null(), "quiescent apply failed: {report}");
    }
    std::thread::sleep(Duration::from_millis(20));
    let settled = service(root, &["vacuum"], deadline)?;
    ensure!(settled[0]["error_kind"].is_null(), "{}", settled[0]);
    let version = log_actions(root)?.last().map(|(version, _)| *version);
    for report in service(root, &operations, deadline)? {
        let inner = &report["report"];
        // VACUUM LITE never commits; its repeat must find nothing left to delete.
        let idle = if report["operation"] == "vacuum" {
            inner["files_deleted"] == 0
        } else {
            inner["outcome"] == "noop"
        };
        ensure!(idle, "repeat must be a no-op: {report}");
    }
    ensure!(
        log_actions(root)?.last().map(|(version, _)| *version) == version,
        "no-op repeat changed the version"
    );

    let actions = log_actions(root)?;
    let files = active(&actions)?;
    let mut physical = BTreeSet::new();
    physical_parquet(&table_dir(root), &table_dir(root), &mut physical)?;
    let active_paths: BTreeSet<String> = files.keys().map(|path| decode(path)).collect();
    ensure!(active_paths.is_subset(&physical), "active Add missing on disk");
    // Each replay stages its own file; a deduplicated new delivery reclaims it, so
    // no Parquet file may exist outside the active set.
    let orphans: BTreeSet<&String> = physical.difference(&active_paths).collect();
    ensure!(orphans.is_empty(), "unreferenced Parquet files remain: {orphans:?}");
    ensure!(rows(root, &files)? == expected, "row multiset changed");

    let latest: BTreeSet<String> = LATEST
        .iter()
        .map(|offset| partition(base, *offset))
        .collect::<Result<_>>()?;
    let expired: BTreeSet<String> = EXPIRED
        .iter()
        .map(|offset| partition(base, *offset))
        .collect::<Result<_>>()?;
    let mut optimized = BTreeSet::new();
    let mut txns: BTreeMap<String, Vec<(u64, Option<i64>)>> = BTreeMap::new();
    for (commit, action) in &actions {
        if let Some(remove) = action.get("remove") {
            let value = remove["partitionValues"][INGESTION_PARTITION]
                .as_str()
                .context("remove partition")?
                .to_owned();
            if remove["dataChange"] == false {
                ensure!(latest.contains(&value), "OPTIMIZE touched {value} outside latest 2");
                optimized.insert(value);
            } else {
                ensure!(expired.contains(&value), "PURGE removed retained {value}");
            }
        }
        if let Some(txn) = action.get("txn") {
            let app = txn["appId"].as_str().context("appId")?;
            if app.starts_with("conc.writer.") {
                txns.entry(app.to_owned())
                    .or_default()
                    .push((*commit, txn["version"].as_i64()));
            }
        }
    }
    ensure!(
        optimized == latest,
        "OPTIMIZE never compacted {latest:?}: {optimized:?}"
    );
    // Every commit is still a JSON file (no checkpoint or log pruning runs here), so
    // the txn scan covers the whole history.
    let last = actions.last().map(|(version, _)| *version).context("empty log")?;
    let versions: BTreeSet<u64> = actions.iter().map(|(version, _)| *version).collect();
    ensure!(
        versions == (0..=last).collect::<BTreeSet<_>>(),
        "log versions are not contiguous through {last}"
    );
    check_writer_receipts(&receipts(root)?, &txns, last)?;
    ensure!(Instant::now() < deadline, "run exceeded the partition model window");
    println!(
        "concurrency proof: cycles={CYCLES} overlapping={concurrent_cycles} publications={PUBLICATIONS} \
         max_attempts={max_attempts} collisions={collisions} outcomes={outcomes:?} rows={} files={} replay_orphans={}",
        expected.len(),
        files.len(),
        orphans.len()
    );
    Ok(())
}

/// Scenario: a separate OS process makes 24 independent blind-append publications
/// (each replayed with the same identity) into retained minute partitions while the
/// actual service binary runs 6 fixed OPTIMIZE+PURGE+VACUUM LITE apply cycles on a
/// marked disposable root, then one quiescent cycle and a repeat.
/// Guarantees: every writer publication and replay succeeds; each identity's txn is
/// committed exactly once, at version 0, in the commit its original call reported, and
/// its replay (which may observe later maintenance commits) never commits it again;
/// maintenance failures are only bounded stale-plan/exhausted outcomes within
/// `MAX_COMMIT_ATTEMPTS`; OPTIMIZE touches only the latest 2 sealed partitions and
/// PURGE only expired ones; the final row multiset equals retained seed rows plus
/// all writer rows; every active Add exists on disk and the only other Parquet files
/// are none: deduplicated replay stagings are reclaimed; the repeat is a no-op with no
/// new version. Every child process is owned, waited with a finite deadline, and
/// killed and reaped on failure. The root lives under Cargo's per-target temporary
/// directory (the driver tree's `TMPDIR`, so the service still sees a marked root
/// under its temp directory); on any failure after the root exists (driver spawn,
/// writer, driver or deadline) it is kept and its path printed.
#[test]
fn separate_writer_process_with_service_optimize_purge_vacuum() -> Result<()> {
    match std::env::var(ROLE).as_deref() {
        Ok("writer") => {
            let root = PathBuf::from(std::env::var(ROOT)?);
            writer(&root, std::env::var(BASE)?.parse()?)
        }
        Ok("driver") => driver(&PathBuf::from(std::env::var(ROOT)?)),
        _ => run_driver(&target_tmp()?, &std::env::current_exe()?),
    }
}

const ROOT_PREFIX: &str = "delta-maintenance-concurrency-";

/// Cargo's per-target temporary directory, created and canonicalized.
fn target_tmp() -> Result<PathBuf> {
    let temp = Path::new(env!("CARGO_TARGET_TMPDIR"));
    fs::create_dir_all(temp)?;
    Ok(fs::canonicalize(temp)?)
}

/// Create the run root under `temp`, run `exe` as the driver tree with `TMPDIR=temp`
/// and wait for it with the outer deadline. Every failure after the root exists
/// (spawn, custody/deadline, or an unsuccessful driver) drops the driver tree first,
/// so its process group is killed and reaped, then keeps the root and prints its path.
fn run_driver(temp: &Path, exe: &Path) -> Result<()> {
    let root = tempfile::Builder::new().prefix(ROOT_PREFIX).tempdir_in(temp)?;
    let outcome = (|| -> Result<()> {
        let mut driver = ProcessTree::spawn(
            Command::new(exe)
                .args([TEST, "--exact", "--nocapture", "--test-threads=1"])
                .env(ROLE, "driver")
                .env(ROOT, root.path())
                .env("TMPDIR", temp)
                .env("ARROWCOL_PARTITION_GRANULARITY", "minute"),
        )
        .context("spawn driver")?;
        let status = driver
            .wait_until(Instant::now() + OUTER_DEADLINE)
            .context("driver custody failed")?;
        ensure!(status.success(), "driver failed ({status})");
        Ok(())
    })();
    outcome.map_err(|error| {
        let kept = root.keep();
        eprintln!("concurrency failure root retained: {}", kept.display());
        error.context(format!("root retained at {}", kept.display()))
    })
}

/// Scenario (fault injection): the outer harness cannot start the driver because its
/// executable does not exist, after the run root was created.
/// Guarantees: the failure is reported, no child exists, and the root is kept (not
/// removed by `TempDir`) with its path in the error.
#[test]
fn outer_failure_before_driver_start_retains_root() -> Result<()> {
    let parent = tempfile::Builder::new()
        .prefix("delta-maintenance-custody-")
        .tempdir_in(target_tmp()?)?;
    let Err(error) = run_driver(parent.path(), &parent.path().join("missing-test-binary")) else {
        bail!("a missing driver executable must fail");
    };
    let message = format!("{error:#}");
    ensure!(
        error.chain().any(|cause| cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)),
        "spawn must fail before any child exists: {message}"
    );
    let roots = fs::read_dir(parent.path())?
        .map(|entry| Ok(entry?.path()))
        .collect::<Result<Vec<_>>>()?;
    ensure!(roots.len() == 1, "exactly one run root: {roots:?}");
    let kept = &roots[0];
    ensure!(
        kept.is_dir()
            && kept
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(ROOT_PREFIX)),
        "kept run root: {}",
        kept.display()
    );
    ensure!(
        message.contains(&kept.display().to_string()),
        "error names the kept root: {message}"
    );
    Ok(())
}

const CUSTODY_TEST: &str = "outer_custody_kills_and_reaps_hung_driver_descendants";

/// Scenario (fault injection): the driver spawns a grandchild that sleeps, then hangs
/// itself past a short outer deadline; the outer kills the driver with SIGKILL, so
/// the driver's own child guards never run.
/// Guarantees: the outer deadline fails the run, and the grandchild is killed and
/// reaped through the driver's process group, not left running after the driver.
#[test]
fn outer_custody_kills_and_reaps_hung_driver_descendants() -> Result<()> {
    let sleep = || std::thread::sleep(Duration::from_secs(600));
    match std::env::var(ROLE).as_deref() {
        Ok("sleeper") => {
            sleep();
            Ok(())
        }
        Ok("hang") => {
            let root = PathBuf::from(std::env::var(ROOT)?);
            let grandchild = Command::new(std::env::current_exe()?)
                .args([CUSTODY_TEST, "--exact", "--test-threads=1"])
                .env(ROLE, "sleeper")
                .spawn()?;
            fs::write(root.join("grandchild.pid"), grandchild.id().to_string())?;
            // Deliberately never waits: only the outer custody can reclaim it.
            std::mem::forget(grandchild);
            sleep();
            Ok(())
        }
        _ => {
            let root = tempfile::tempdir()?;
            let mut tree = ProcessTree::spawn(
                Command::new(std::env::current_exe()?)
                    .args([CUSTODY_TEST, "--exact", "--test-threads=1"])
                    .env(ROLE, "hang")
                    .env(ROOT, root.path()),
            )?;
            let pid_file = root.path().join("grandchild.pid");
            let spawned = Instant::now() + Duration::from_secs(60);
            while !pid_file.exists() {
                ensure!(Instant::now() < spawned, "grandchild never started");
                std::thread::sleep(Duration::from_millis(10));
            }
            let raw: i32 = fs::read_to_string(&pid_file)?.trim().parse()?;
            let grandchild = rustix::process::Pid::from_raw(raw).context("grandchild pid")?;
            ensure!(
                rustix::process::test_kill_process(grandchild).is_ok(),
                "grandchild not running"
            );
            let error = tree
                .wait_until(Instant::now() + Duration::from_secs(2))
                .err()
                .context("hung driver must exceed its deadline")?;
            let message = format!("{error:#}");
            ensure!(message.contains("sweep Ok(())"), "{message}");
            ensure!(
                rustix::process::test_kill_process(grandchild) == Err(rustix::io::Errno::SRCH),
                "grandchild {raw} survived the killed driver"
            );
            println!("custody proof: {message}; grandchild {raw} reaped");
            Ok(())
        }
    }
}
