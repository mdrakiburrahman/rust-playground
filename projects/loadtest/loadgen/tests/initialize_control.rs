//! Offline proof of the shared `initialize-azure` body (`initialize_tables_with`): all 26
//! canonical tables go through the real bootstrap initializer over injected control metadata and a
//! disposable local store, and the manifest is published only after every table drains. Uses the
//! dev-only `arrow-deltalake/test-control` seam; no Azure endpoint, token or process environment.
//! The parent-chain case composes the real Python parent's child environment, the
//! `initialize-delta.sh` contract, the real CLI parser and the parent's manifest validation; it does
//! not execute the release binary, whose control transport is Azure-only by design.
//! Compiled only in debug test builds, matching the seam gate in arrow-deltalake.
#![cfg(debug_assertions)]

use anyhow::{Context, Result};
use arrow_deltalake::delta::DeltaTable;
use arrow_deltalake::publisher::{
    CasOutcome, ControlMetadata, MetadataSnapshot, PublisherSession, TestBootstrapControl,
};
use arrowcol_loadgen::cli::InitializeAzureArgs;
use arrowcol_loadgen::initialize::initialize_tables_with;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult,
};
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

const SANDBOX: &str = "0123456789ab";
const ACCOUNT: &str = "acct";
const CONTAINER: &str = "tables";
const ROOT: &str = "abfss://tables@acct.dfs.core.windows.net/sandbox";
const GENERATION: &str = "00000000-0000-4000-8000-000000000001";
const ROTATED: &str = "00000000-0000-4000-8000-000000000002";
const TABLES: usize = 26;

#[derive(Default)]
struct Memory(Mutex<(BTreeMap<String, String>, u64)>);

impl fmt::Debug for Memory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Memory")
    }
}

impl Memory {
    fn new(metadata: BTreeMap<String, String>) -> Arc<Self> {
        Arc::new(Self(Mutex::new((metadata, 0))))
    }

    fn metadata(&self) -> BTreeMap<String, String> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).0.clone()
    }

    fn members(&self) -> Result<usize> {
        let raw = self.metadata().get("publishers").cloned().context("publishers")?;
        Ok(serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&raw)?.len())
    }
}

#[async_trait]
impl ControlMetadata for Memory {
    async fn read(&self) -> Result<MetadataSnapshot> {
        let state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(MetadataSnapshot {
            etag: state.1.to_string(),
            metadata: state.0.clone(),
        })
    }

    async fn compare_and_set(&self, expected: &MetadataSnapshot, metadata: &BTreeMap<String, String>) -> CasOutcome {
        let mut state = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if state.0 != expected.metadata {
            return CasOutcome::Conflict;
        }
        state.0 = metadata.clone();
        state.1 += 1;
        CasOutcome::Applied
    }
}

fn authority(state: &str) -> BTreeMap<String, String> {
    [
        ("sandboxid", SANDBOX),
        ("resetstate", state),
        ("resetgeneration", GENERATION),
        ("publisherregistryversion", "1"),
        ("publishers", "{}"),
        ("occupancy", "released"),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value.to_owned()))
    .collect()
}

/// Disposable owned root; removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!("loadgen-initialize-control-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(path.join("data"))?;
        Ok(Self(path))
    }

    fn data(&self) -> PathBuf {
        self.0.join("data")
    }

    fn manifest(&self) -> PathBuf {
        self.0.join("manifest.json")
    }

    fn args(&self) -> InitializeAzureArgs {
        InitializeAzureArgs {
            manifest: self.manifest(),
        }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn walk(root: &Path, visit: &mut dyn FnMut(&Path, bool)) -> Result<()> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let directory = entry.file_type()?.is_dir();
            visit(&entry.path(), directory);
            if directory {
                pending.push(entry.path());
            }
        }
    }
    Ok(())
}

fn files(root: &Path) -> Result<usize> {
    let mut count = 0;
    walk(root, &mut |_, directory| count += usize::from(!directory))?;
    Ok(count)
}

/// Every `_delta_log` under `root` (relative path) with its file names and bytes.
fn logs(root: &Path) -> Result<BTreeMap<String, BTreeMap<String, Vec<u8>>>> {
    let mut dirs = Vec::new();
    walk(root, &mut |path, directory| {
        if directory && path.file_name().is_some_and(|name| name == "_delta_log") {
            dirs.push(path.to_path_buf());
        }
    })?;
    let mut logs = BTreeMap::new();
    for dir in dirs {
        let mut contents = BTreeMap::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                contents.insert(
                    entry.file_name().to_string_lossy().into_owned(),
                    std::fs::read(entry.path())?,
                );
            }
        }
        let relative = dir.strip_prefix(root)?.to_string_lossy().into_owned();
        logs.insert(relative, contents);
    }
    Ok(logs)
}

/// Runs the shared loadgen body with the real bootstrap initializer over injected control.
async fn run(scratch: &Scratch, metadata: &Arc<Memory>, expected: &str, data: Arc<dyn ObjectStore>) -> Result<usize> {
    run_root(scratch, ROOT, metadata, expected, data).await
}

async fn run_root(
    scratch: &Scratch,
    root: &str,
    metadata: &Arc<Memory>,
    expected: &str,
    data: Arc<dyn ObjectStore>,
) -> Result<usize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let control = Arc::clone(metadata) as Arc<dyn ControlMetadata>;
    let expected = expected.to_owned();
    initialize_tables_with(
        scratch.args(),
        root,
        ACCOUNT,
        CONTAINER,
        move |url, schema, partitions| {
            counted.fetch_add(1, Ordering::SeqCst);
            DeltaTable::initialize_url_for_bootstrap_with_control(
                url,
                schema,
                partitions,
                TestBootstrapControl {
                    metadata: Arc::clone(&control),
                    sandbox_id: SANDBOX.into(),
                    account: ACCOUNT.into(),
                    expected_generation: expected.clone(),
                    data: Arc::clone(&data),
                },
            )
        },
    )
    .await
    .map(|()| calls.load(Ordering::SeqCst))
}

fn local(scratch: &Scratch) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(LocalFileSystem::new_with_prefix(scratch.data())?))
}

/// Guarantees: every canonical table (both pools) is initialized under fenced-v1 by a bootstrap
/// member that drains and unregisters; the manifest lists exactly those tables and appears only
/// after the last one; a rerun preserves every existing log byte-for-byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_canonical_tables_drain_before_manifest_and_rerun_is_idempotent() -> Result<()> {
    let scratch = Scratch::new()?;
    let metadata = Memory::new(authority("fenced-v1"));
    assert_eq!(run(&scratch, &metadata, GENERATION, local(&scratch)?).await?, TABLES);
    assert_eq!(metadata.members()?, 0);
    assert_eq!(
        metadata.metadata().get("resetstate").map(String::as_str),
        Some("fenced-v1")
    );
    let first = logs(&scratch.data())?;
    assert_eq!(first.len(), TABLES, "{:?}", first.keys().collect::<Vec<_>>());
    assert!(first.values().all(|log| !log.is_empty()));
    let manifest: Vec<serde_json::Value> = serde_json::from_slice(&std::fs::read(scratch.manifest())?)?;
    assert_eq!(manifest.len(), TABLES);
    for entry in &manifest {
        let relative = entry["relative_path"].as_str().context("relative_path")?;
        assert!(
            first.contains_key(&format!("sandbox/{relative}/_delta_log")),
            "{relative}"
        );
    }

    std::fs::remove_file(scratch.manifest())?;
    assert_eq!(run(&scratch, &metadata, GENERATION, local(&scratch)?).await?, TABLES);
    assert_eq!(logs(&scratch.data())?, first, "rerun rewrote an existing log");
    assert_eq!(metadata.members()?, 0);
    assert!(scratch.manifest().exists());
    Ok(())
}

/// Guarantees: a stale generation, cleaning-v1, ready-v1, stopping occupancy or a live member
/// refuses the first table before any blob, leaves metadata untouched and publishes no manifest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_admission_stops_at_first_table_with_no_blobs_or_manifest() -> Result<()> {
    let mut cases: Vec<(&str, BTreeMap<String, String>, &str)> =
        vec![("stale generation", authority("fenced-v1"), ROTATED)];
    for state in ["cleaning-v1", "ready-v1"] {
        cases.push((state, authority(state), GENERATION));
    }
    let mut stopping = authority("fenced-v1");
    stopping.insert("occupancy".into(), "stopping".into());
    stopping.insert("occupancyid".into(), ROTATED.into());
    cases.push(("stopping", stopping, GENERATION));
    for (name, initial, expected) in cases {
        let scratch = Scratch::new()?;
        let metadata = Memory::new(initial.clone());
        let error = run(&scratch, &metadata, expected, local(&scratch)?)
            .await
            .err()
            .with_context(|| format!("{name} admitted"))?;
        assert!(
            format!("{error:#}").contains("bootstrap canonical table direct_"),
            "{name}: {error:#}"
        );
        assert_eq!(metadata.metadata(), initial, "{name}: metadata unchanged");
        assert_eq!(files(&scratch.data())?, 0, "{name}: no blobs");
        assert!(!scratch.manifest().exists(), "{name}: no manifest");
    }

    let scratch = Scratch::new()?;
    let metadata = Memory::new(authority("fenced-v1"));
    let live = PublisherSession::open_bootstrap_with_control(
        Arc::clone(&metadata) as Arc<dyn ControlMetadata>,
        SANDBOX,
        ACCOUNT,
        GENERATION,
    )?;
    let held = metadata.metadata();
    assert!(
        run(&scratch, &metadata, GENERATION, local(&scratch)?).await.is_err(),
        "live member"
    );
    assert_eq!(metadata.metadata(), held, "live member: metadata unchanged");
    assert_eq!(files(&scratch.data())?, 0);
    assert!(!scratch.manifest().exists());
    live.close_after_drain_async().await?;
    assert_eq!(metadata.members()?, 0);
    Ok(())
}

/// Guarantees: an unsafe root fails before the initializer is invoked: no control transition,
/// no blob and no manifest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsafe_root_is_rejected_before_any_table() -> Result<()> {
    for root in [
        "abfss://tables@other.dfs.core.windows.net/sandbox",
        "abfss://other@acct.dfs.core.windows.net/sandbox",
        "abfss://tables@acct.dfs.core.windows.net/sandbox?sig=x",
        "abfss://tables@acct.dfs.core.windows.net/a/../b",
        "file:///tmp/sandbox",
    ] {
        let scratch = Scratch::new()?;
        let initial = authority("fenced-v1");
        let metadata = Memory::new(initial.clone());
        assert!(
            run_root(&scratch, root, &metadata, GENERATION, local(&scratch)?)
                .await
                .is_err(),
            "{root}"
        );
        assert_eq!(metadata.metadata(), initial, "{root}");
        assert_eq!(files(&scratch.data())?, 0, "{root}");
        assert!(!scratch.manifest().exists(), "{root}");
    }
    Ok(())
}

/// Local store whose puts under `dataflow/` have an unknown (timed-out) outcome.
#[derive(Debug)]
struct DataflowPutsTimeOut(LocalFileSystem);

impl fmt::Display for DataflowPutsTimeOut {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DataflowPutsTimeOut")
    }
}

#[async_trait]
impl ObjectStore for DataflowPutsTimeOut {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().contains("/dataflow/") {
            return Err(object_store::Error::Generic {
                store: "DataflowPutsTimeOut",
                source: "operation timed out".into(),
            });
        }
        self.0.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> object_store::Result<GetResult> {
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.0.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> object_store::Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &ObjectPath, to: &ObjectPath, options: CopyOptions) -> object_store::Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}

/// Guarantees: an unknown-outcome write on the first dataflow table stops the run there; the
/// already-drained direct tables persist, that table's bootstrap member stays registered as a
/// reset blocker, the authority stays fenced-v1, and no manifest is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_write_mid_run_retains_member_and_withholds_manifest() -> Result<()> {
    let scratch = Scratch::new()?;
    let metadata = Memory::new(authority("fenced-v1"));
    let store = Arc::new(DataflowPutsTimeOut(LocalFileSystem::new_with_prefix(scratch.data())?));
    let error = run(&scratch, &metadata, GENERATION, store)
        .await
        .err()
        .context("unknown write succeeded")?;
    let message = format!("{error:#}");
    assert!(message.contains("bootstrap canonical table dataflow_"), "{message}");
    assert!(message.contains("registration retained"), "{message}");
    assert_eq!(metadata.members()?, 1);
    assert_eq!(
        metadata.metadata().get("resetstate").map(String::as_str),
        Some("fenced-v1")
    );
    let written = logs(&scratch.data())?;
    assert_eq!(written.len(), TABLES / 2);
    assert!(written.keys().all(|dir| dir.starts_with("sandbox/direct/")));
    assert!(!scratch.manifest().exists());
    Ok(())
}

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

const PARENT_DEADLINE: Duration = Duration::from_secs(120);
const REAP_DEADLINE: Duration = Duration::from_secs(5);

enum Bounded {
    Finished(std::process::ExitStatus, Vec<u8>, Vec<u8>),
    TimedOut(u32),
}

/// Runs an owned child with output in private files under `dir` (no pipe waits), an absolute
/// `deadline`, and on expiry a kill followed by a checked reap within [`REAP_DEADLINE`].
fn run_bounded(mut command: std::process::Command, dir: &Path, deadline: Duration) -> Result<Bounded> {
    let id = uuid::Uuid::new_v4();
    let (out, err) = (dir.join(format!("child-{id}.out")), dir.join(format!("child-{id}.err")));
    let mut child = command
        .stdin(std::process::Stdio::null())
        .stdout(std::fs::File::create(&out)?)
        .stderr(std::fs::File::create(&err)?)
        .spawn()
        .context("spawn bounded child")?;
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Bounded::Finished(status, std::fs::read(&out)?, std::fs::read(&err)?));
        }
        if started.elapsed() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let pid = child.id();
    let _ = child.kill();
    let killed = Instant::now();
    while child.try_wait()?.is_none() {
        anyhow::ensure!(
            killed.elapsed() < REAP_DEADLINE,
            "child {pid} was not reaped after kill"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(Bounded::TimedOut(pid))
}

/// Runs the test-only Python driver of the real `bootstrap.py` parent and parses its JSON.
fn parent(dir: &Path, arguments: &[&std::ffi::OsStr]) -> Result<serde_json::Value> {
    let mut command = std::process::Command::new("python3");
    command
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/parent_chain.py"))
        .args(arguments)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default());
    match run_bounded(command, dir, PARENT_DEADLINE)? {
        Bounded::Finished(status, stdout, stderr) => {
            anyhow::ensure!(
                status.success(),
                "parent driver failed: {}",
                String::from_utf8_lossy(&stderr)
            );
            Ok(serde_json::from_slice(&stdout)?)
        }
        Bounded::TimedOut(pid) => anyhow::bail!("parent driver {pid} exceeded its deadline; killed and reaped"),
    }
}

/// Guarantees: a hanging child is killed at the deadline and reaped (its PID no longer exists),
/// and the caller gets a failure well before the child would have exited.
#[cfg(target_os = "linux")]
#[test]
fn hanging_child_is_killed_and_reaped_at_deadline() -> Result<()> {
    let scratch = Scratch::new()?;
    let mut command = std::process::Command::new("sleep");
    command.arg("60");
    let started = Instant::now();
    let Bounded::TimedOut(pid) = run_bounded(command, &scratch.0, Duration::from_millis(300))? else {
        anyhow::bail!("sleep 60 finished inside a 300ms deadline");
    };
    assert!(started.elapsed() < Duration::from_secs(10), "{:?}", started.elapsed());
    assert!(!Path::new(&format!("/proc/{pid}")).exists(), "child {pid} not reaped");
    Ok(())
}

/// Guarantees: the environment the real Python parent composes for the child satisfies every
/// precondition of `initialize-delta.sh`; the script's exact argv parses through the real CLI;
/// the shared env parser and 26-table body then initialize every table over the guarded store
/// and drain; and the parent's real manifest/Delta-log validation accepts that manifest and
/// those logs, reaching its first ARM call with no Kusto command. Removing one log makes the
/// parent report NotReady without Kusto.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parent_child_env_cli_body_and_parent_validation_compose() -> Result<()> {
    use arrowcol_loadgen::initialize::InitializeAzureConfig;
    use arrowcol_loadgen::{Cli, Command};
    use clap::Parser;

    let scratch = Scratch::new()?;
    let values = scratch.0.join("values.json");
    std::fs::write(
        &values,
        serde_json::to_vec(&serde_json::json!({
            "AZURE_SUBSCRIPTION_ID": "11111111-2222-4333-8444-555555555555",
            "AZURE_STORAGE_ACCOUNT": ACCOUNT,
            "AZURE_STORAGE_CONTAINER": CONTAINER,
            "ARROWCOL_AZURE_TABLE_ROOT": ROOT,
        }))?,
    )?;
    let child: BTreeMap<String, String> = serde_json::from_value(parent(
        &scratch.0,
        &[
            "child-env".as_ref(),
            values.as_os_str(),
            scratch.manifest().as_os_str(),
            GENERATION.as_ref(),
        ],
    )?)?;

    let script = std::fs::read_to_string(repository().join("infra/azure/initialize-delta.sh"))?;
    let required: Vec<&str> = script
        .lines()
        .filter_map(|line| line.strip_prefix(": \"${")?.split_once(":?").map(|(key, _)| key))
        .collect();
    assert_eq!(required.len(), 6, "{required:?}");
    for key in &required {
        assert!(child.get(*key).is_some_and(|value| !value.is_empty()), "{key}");
    }
    assert!(script.contains("initialize-azure --manifest \"$ARROWCOL_AZURE_TABLE_MANIFEST\""));
    let manifest = child["ARROWCOL_AZURE_TABLE_MANIFEST"].as_str();
    let cli = Cli::try_parse_from(["arrowcol-loadgen", "initialize-azure", "--manifest", manifest])?;
    let Command::InitializeAzure(args) = cli.command else {
        anyhow::bail!("initialize-delta.sh argv did not dispatch to initialize-azure");
    };
    assert_eq!(args.manifest, PathBuf::from(manifest));

    let config = InitializeAzureConfig::from_lookup(|key| child.get(key).cloned())?;
    let expected = child["ARROWCOL_EXPECTED_RESET_GENERATION"].clone();
    arrow_deltalake::publisher::canonical_uuid(&expected)?;
    let metadata = Memory::new(authority("fenced-v1"));
    let control = Arc::clone(&metadata) as Arc<dyn ControlMetadata>;
    let data = local(&scratch)?;
    let account = config.account.clone();
    initialize_tables_with(
        args,
        &config.root,
        &config.account,
        &config.container,
        move |url, schema, partitions| {
            DeltaTable::initialize_url_for_bootstrap_with_control(
                url,
                schema,
                partitions,
                TestBootstrapControl {
                    metadata: Arc::clone(&control),
                    sandbox_id: SANDBOX.into(),
                    account: account.clone(),
                    expected_generation: expected.clone(),
                    data: Arc::clone(&data),
                },
            )
        },
    )
    .await?;
    assert_eq!(metadata.members()?, 0);
    assert_eq!(logs(&scratch.data())?.len(), TABLES);

    let reconcile = || {
        parent(
            &scratch.0,
            &[
                "reconcile".as_ref(),
                values.as_os_str(),
                scratch.manifest().as_os_str(),
                scratch.data().as_os_str(),
            ],
        )
    };
    let accepted = reconcile()?;
    assert_eq!(accepted["outcome"], "stopped-before-kusto", "{accepted}");
    assert_eq!(accepted["validated"], TABLES);
    assert_eq!(accepted["kql"], 0);

    let first = logs(&scratch.data())?.into_keys().next().context("log")?;
    std::fs::remove_file(scratch.data().join(first).join("00000000000000000000.json"))?;
    let missing = reconcile()?;
    assert_eq!(missing["outcome"], "not-ready", "{missing}");
    assert!(
        missing["detail"].as_str().is_some_and(|detail| detail.contains(": 1;")),
        "{missing}"
    );
    assert_eq!(missing["kql"], 0);
    Ok(())
}

/// Guarantees: the shared env parser fails on each key `initialize()` requires before any table.
#[test]
fn shared_env_parser_requires_every_initializer_key() {
    use arrowcol_loadgen::initialize::InitializeAzureConfig;
    let keys = [
        "ARROWCOL_AZURE_TABLE_ROOT",
        "AZURE_STORAGE_ACCOUNT",
        "AZURE_STORAGE_CONTAINER",
        "AZURE_STORAGE_CHECKPOINT_CONTAINER",
    ];
    for absent in keys {
        let error = InitializeAzureConfig::from_lookup(|key| (key != absent).then(|| "x".to_owned()))
            .err()
            .map(|error| error.to_string());
        assert_eq!(error, Some(format!("missing {absent}")));
    }
}
