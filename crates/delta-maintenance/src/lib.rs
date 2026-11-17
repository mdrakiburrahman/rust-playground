//! Standalone Delta maintenance service: bounded OPTIMIZE, PURGE, VACUUM LITE,
//! checkpointing and proven `_delta_log` pruning over an allowlist of tables,
//! dry-run by default, with Prometheus metrics.

pub mod catalog;

use anyhow::{Context, Result, bail};
use arrow_deltalake::delta::DeltaTable;
use arrow_deltalake::maintenance::{
    DEFAULT_OPTIMIZE_UPLOAD_BUFFER_BYTES, Granularity, MaintenanceControl, MaintenanceError, MaintenanceErrorKind,
    MaintenancePlan, MaintenanceReport, NegativeProbeConfig, OptimizeConfig, OptimizePlan, Outcome, PassProbeInventory,
    ProbeOperation, PruneConfig, PurgeConfig, TableFingerprint, VacuumConfig, attach_error_report, error_details,
    error_report, now_ms,
};
use arrowcol_props::{PropValue, ResolvedProps, resolve};
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use url::Url;

/// Maximum number of allowlisted tables per service instance.
pub const MAX_TABLES: usize = 256;
/// Maximum concurrently maintained tables.
pub const MAX_CONCURRENT_TABLES: usize = 16;
const MAX_HTTP_REQUEST: usize = 8 * 1024;
const MAX_STALE_DISCOVERY_RETRIES: usize = 2;

/// Maintenance operation, run one at a time per table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Optimize,
    Purge,
    Vacuum,
    /// Kernel checkpoint once enough commits accumulate; never deletes history.
    Checkpoint,
    /// Delete `_delta_log` history proven equivalent to a checkpoint.
    PruneLog,
}

impl Operation {
    const ALL: [Self; 5] = [
        Self::Purge,
        Self::Optimize,
        Self::Vacuum,
        Self::Checkpoint,
        Self::PruneLog,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::Optimize => "optimize",
            Self::Purge => "purge",
            Self::Vacuum => "vacuum",
            Self::Checkpoint => "checkpoint",
            Self::PruneLog => "prune-log",
        }
    }
}

/// OPTIMIZE's upload path. Buffered multipart cannot provide atomic create-only writes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptimizeUploadMode {
    /// Preserve the original single-object conditional create.
    #[default]
    PutCreate,
    /// Opt in to bounded multipart upload to UUID-named outputs.
    BufferedMultipart,
}

impl std::str::FromStr for OptimizeUploadMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "put_create" => Ok(Self::PutCreate),
            "buffered_multipart" => Ok(Self::BufferedMultipart),
            _ => bail!("invalid OPTIMIZE upload mode {value:?}; expected put_create or buffered_multipart"),
        }
    }
}

/// Process-level bounds for existing non-OPTIMIZE operations.
#[derive(Clone, Debug, Serialize)]
pub struct OperationLimits {
    pub purge_max_files_per_commit: usize,
    pub purge_max_plan_bytes: usize,
    pub optimize_upload_mode: OptimizeUploadMode,
    pub optimize_upload_buffer_bytes: Option<usize>,
    pub vacuum_delete_chunk: usize,
    pub vacuum_max_plan_bytes: usize,
    pub replay_max_log_files: usize,
    pub replay_max_log_file_bytes: u64,
    pub replay_max_checkpoint_bytes: u64,
    pub operation_max_active_files: usize,
    pub discovery_max_log_files: usize,
    pub discovery_max_log_file_bytes: u64,
}

impl Default for OperationLimits {
    fn default() -> Self {
        Self {
            purge_max_files_per_commit: 10_000,
            purge_max_plan_bytes: 4 * 1024 * 1024,
            optimize_upload_mode: OptimizeUploadMode::PutCreate,
            optimize_upload_buffer_bytes: None,
            vacuum_delete_chunk: 256,
            vacuum_max_plan_bytes: 8 * 1024 * 1024,
            replay_max_log_files: 100_000,
            replay_max_log_file_bytes: 64 * 1024 * 1024,
            replay_max_checkpoint_bytes: 256 * 1024 * 1024,
            operation_max_active_files: 1_000_000,
            discovery_max_log_files: 100_000,
            discovery_max_log_file_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Validated service settings.
#[derive(Clone, Debug)]
pub struct Settings {
    pub root: Url,
    pub tables: Vec<String>,
    pub operations: Vec<Operation>,
    pub apply: bool,
    pub interval: Option<Duration>,
    pub max_idle_backoff: Duration,
    pub max_cycles: Option<u64>,
    /// Consecutive typed stale plans per table/operation before scheduled health fails.
    pub max_consecutive_stale_plans: u32,
    pub max_concurrent_tables: usize,
    /// Optional aggregate budget for per-bin decoded-byte limits, not an RSS cap.
    pub max_concurrent_decoded_bytes: Option<u64>,
    pub operation_timeout: Duration,
    pub commit_attempts: u32,
    pub commit_budget: Duration,
    pub catalog_options: catalog::CatalogOptions,
    pub granularity: Option<Granularity>,
    pub purge_retention_ms: Option<i64>,
    pub anchor_ms: Option<i64>,
    pub vacuum_retention_ms: i64,
    /// Explicit acknowledgement of old snapshot/QAP reader risk; VACUUM only.
    pub allow_short_vacuum_retention: bool,
    pub unsafe_short_vacuum_for_tests: bool,
    pub optimize: OptimizeConfig,
    /// Explicit process defaults for registered table properties.
    pub process_properties: BTreeMap<String, String>,
    /// Minimum commits after the hinted checkpoint before `checkpoint` writes one.
    pub checkpoint_every_commits: u64,
    /// `prune-log` commit-age retention; `None` uses `delta.logRetentionDuration`
    /// (default 30 days). Shorter values need the unsafe test override.
    pub log_retention_ms: Option<i64>,
    /// Maximum log objects one `prune-log` apply deletes.
    pub max_prune_files: usize,
    pub operation_limits: OperationLimits,
}

impl Settings {
    /// Reject unbounded or unsafe settings before touching any table.
    ///
    /// # Errors
    /// Returns a description of the first invalid setting.
    pub fn validate(&self) -> Result<()> {
        if self.optimize.sealed_final
            && (self.interval.is_some() || self.max_cycles.is_some() || self.operations != [Operation::Optimize])
        {
            bail!("--sealed-final requires one-shot optimize only, without --interval-secs or --max-cycles");
        }
        if !matches!(self.root.scheme(), "file" | "abfss" | "az" | "azure" | "https") {
            bail!("unsupported root URL scheme {}", self.root.scheme());
        }
        if self.tables.len() > MAX_TABLES || (self.tables.is_empty() && self.root.scheme() != "file") {
            bail!("at most {MAX_TABLES} tables; non-filesystem roots require a catalog or explicit table allowlist");
        }
        for name in &self.tables {
            let valid = !name.is_empty()
                && name.len() <= 128
                && name != "."
                && name != ".."
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
            if !valid {
                bail!("invalid table name {name:?}: use [A-Za-z0-9_.-] without traversal");
            }
        }
        if self.checkpoint_every_commits == 0 {
            bail!("--checkpoint-every-commits must be nonzero");
        }
        if !(1..=1024).contains(&self.max_consecutive_stale_plans) {
            bail!("--max-consecutive-stale-plans must be within 1..=1024");
        }
        if self.max_prune_files == 0 {
            bail!("--max-prune-files must be nonzero");
        }
        let limits = &self.operation_limits;
        if limits.optimize_upload_mode == OptimizeUploadMode::PutCreate && limits.optimize_upload_buffer_bytes.is_some()
        {
            bail!("OPTIMIZE upload buffer applies only to buffered_multipart mode");
        }
        if limits.optimize_upload_mode == OptimizeUploadMode::BufferedMultipart {
            let buffer_bytes = limits
                .optimize_upload_buffer_bytes
                .unwrap_or(DEFAULT_OPTIMIZE_UPLOAD_BUFFER_BYTES);
            if !(arrow_deltalake::maintenance::MIN_OPTIMIZE_UPLOAD_BUFFER_BYTES
                ..=arrow_deltalake::maintenance::MAX_OPTIMIZE_UPLOAD_BUFFER_BYTES)
                .contains(&buffer_bytes)
            {
                bail!("OPTIMIZE upload buffer must be within 4..=64 MiB");
            }
        }
        if limits.purge_max_files_per_commit == 0
            || limits.purge_max_plan_bytes < 1024
            || limits.vacuum_delete_chunk == 0
            || limits.vacuum_max_plan_bytes < 1024
            || limits.replay_max_log_files == 0
            || limits.replay_max_log_file_bytes == 0
            || limits.replay_max_checkpoint_bytes == 0
            || limits.operation_max_active_files == 0
            || limits.discovery_max_log_files == 0
            || limits.discovery_max_log_file_bytes == 0
        {
            bail!("operation limits must be positive; plan byte limits must be at least 1024");
        }
        if self.log_retention_ms.is_some_and(|retention| retention < 0) {
            bail!("log retention must not be negative");
        }
        if self.operations.is_empty() {
            bail!("at least one --operation is required");
        }
        if self.max_concurrent_tables == 0 || self.max_concurrent_tables > MAX_CONCURRENT_TABLES {
            bail!("max concurrent tables must be within 1..={MAX_CONCURRENT_TABLES}");
        }
        if self.operation_timeout.is_zero() || self.interval.is_some_and(|interval| interval.is_zero()) {
            bail!("timeouts and intervals must be positive");
        }
        if self.interval.is_some_and(|interval| self.max_idle_backoff < interval) {
            bail!("max idle backoff must be at least the interval");
        }
        if self.operations.contains(&Operation::Purge)
            && (self.granularity.is_none() || self.purge_retention_ms.is_none())
        {
            bail!("purge requires --granularity and --purge-retention-secs");
        }
        if self.purge_retention_ms.is_some_and(|retention| retention <= 0) {
            bail!("purge retention must be positive");
        }
        if self.vacuum_retention_ms < 0 {
            bail!("VACUUM retention must not be negative");
        }
        self.optimize.validate()?;
        self.catalog_options.validate()?;
        self.validate_decoded_budget(self.optimize.max_decoded_bytes_per_bin)?;
        MaintenanceControl::default().with_commit_retry_limits(self.commit_attempts, self.commit_budget)?;
        resolve(&BTreeMap::new(), &process_defaults(self))?;
        Ok(())
    }

    fn validate_decoded_budget(&self, per_bin: u64) -> Result<()> {
        if let Some(budget) = self.max_concurrent_decoded_bytes {
            let concurrency = u64::try_from(self.max_concurrent_tables)?;
            let total = per_bin
                .checked_mul(concurrency)
                .context("concurrent decode budget overflow")?;
            if budget == 0 || total > budget {
                bail!("per-bin decoded limit times concurrent tables exceeds the aggregate decode budget");
            }
        }
        Ok(())
    }
}

/// Current catalog identity and properties for one table.
#[derive(Clone, Debug)]
pub struct DiscoveredTable {
    pub name: String,
    pub url: Url,
    pub properties: BTreeMap<String, String>,
    /// Filesystem property reads are bound to a log version; catalog reads may
    /// carry their own stronger event-stream consistency contract instead.
    pub log_token: Option<String>,
}

/// Catalog identity used by maintenance target readback.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct TargetTable {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl TargetTable {
    fn from_name(name: &str) -> Self {
        let parts = name.split('.').collect::<Vec<_>>();
        if let [catalog, schema, table] = parts.as_slice() {
            Self {
                catalog: (*catalog).into(),
                schema: (*schema).into(),
                table: (*table).into(),
            }
        } else {
            Self {
                catalog: String::new(),
                schema: String::new(),
                table: name.into(),
            }
        }
    }

    fn complete(&self) -> bool {
        !self.catalog.is_empty() && !self.schema.is_empty() && !self.table.is_empty()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TargetState {
    Unresolved,
    Resolved,
    Conflicting,
}

#[derive(Serialize)]
struct DeltaTarget {
    table: TargetTable,
    #[serde(skip_serializing_if = "Option::is_none")]
    normalized_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta_table_id: Option<String>,
    state: TargetState,
    #[serde(skip)]
    observed: Option<(String, String)>,
}

impl DeltaTarget {
    fn new(table: TargetTable) -> Self {
        Self {
            table,
            normalized_root: None,
            delta_table_id: None,
            state: TargetState::Unresolved,
            observed: None,
        }
    }

    fn observe(&mut self, identity: Option<(String, String)>) {
        self.normalized_root = None;
        self.delta_table_id = None;
        if self.state == TargetState::Conflicting {
            return;
        }
        let Some((root, id)) = identity.filter(|(root, id)| !root.is_empty() && !id.is_empty()) else {
            self.state = TargetState::Unresolved;
            return;
        };
        if self
            .observed
            .as_ref()
            .is_some_and(|previous| previous != &(root.clone(), id.clone()))
        {
            self.state = TargetState::Conflicting;
            return;
        }
        self.observed = Some((root.clone(), id.clone()));
        if self.table.complete() {
            self.normalized_root = Some(root);
            self.delta_table_id = Some(id);
            self.state = TargetState::Resolved;
        }
    }
}

/// The catalog client implements this interface without filesystem discovery.
pub trait TableDiscovery: Sync {
    /// List table identities for this service's bounded pass.
    ///
    /// # Errors
    /// Returns catalog or filesystem discovery failures.
    fn tables(&self) -> Result<Vec<String>>;

    /// Fetch current table location and properties.
    ///
    /// # Errors
    /// Returns missing-table, corrupt metadata or authority failures.
    fn table(&self, name: &str) -> Result<DiscoveredTable>;

    /// Return the logical identity from discovery, without consulting requested locations.
    ///
    /// # Errors
    /// Returns catalog identity access failures.
    fn table_identity(&self, name: &str) -> Result<TargetTable> {
        Ok(TargetTable::from_name(name))
    }

    /// Return the wall-clock anchor for the current operation.
    ///
    /// Tests and custom discovery implementations may provide a deterministic clock.
    ///
    /// # Errors
    /// Returns clock acquisition failures.
    fn maintenance_now_ms(&self) -> Result<i64> {
        now_ms()
    }

    /// Open the read-only table transport used by maintenance probes.
    ///
    /// Implementations may override this to provide an instrumented store.
    ///
    /// # Errors
    /// Returns table access and bounded probe setup failures.
    fn open_probe_table(&self, url: &Url, max_log_files: usize) -> Result<Arc<DeltaTable>> {
        DeltaTable::open_for_maintenance_probe_with_limit(url, max_log_files)
    }

    /// Open the table used by a full maintenance planner.
    ///
    /// # Errors
    /// Returns table access and snapshot loading failures.
    fn open_maintenance_table(&self, url: &Url) -> Result<Arc<DeltaTable>> {
        DeltaTable::open_for_maintenance(url)
    }

    /// Return allowlisted tables that are not ready for maintenance yet.
    ///
    /// # Errors
    /// Returns pending-state access failures.
    fn pending_tables(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

type PropertyCache = BTreeMap<String, (String, BTreeMap<String, String>)>;

/// Compatibility discovery for local roots and explicit cloud allowlists.
/// Only metadata is cached, fenced by the current log version each pass.
pub struct FilesystemDiscovery {
    root: Url,
    allowlist: Vec<String>,
    cache: Mutex<PropertyCache>,
    pending_tables: Mutex<Vec<String>>,
    max_log_files: usize,
    max_log_file_bytes: u64,
}

impl FilesystemDiscovery {
    #[must_use]
    pub fn new(root: Url, allowlist: Vec<String>) -> Self {
        Self {
            root,
            allowlist,
            cache: Mutex::new(BTreeMap::new()),
            pending_tables: Mutex::new(Vec::new()),
            max_log_files: 100_000,
            max_log_file_bytes: 64 * 1024 * 1024,
        }
    }

    #[must_use]
    pub fn with_log_limits(mut self, max_log_files: usize, max_log_file_bytes: u64) -> Self {
        self.max_log_files = max_log_files;
        self.max_log_file_bytes = max_log_file_bytes;
        self
    }

    fn url(&self, name: &str) -> Result<Url> {
        let mut root = self.root.clone();
        if !root.path().ends_with('/') {
            root.set_path(&format!("{}/", root.path()));
        }
        let components = name.split('.').collect::<Vec<_>>();
        let relative = if components.len() == 3 && components.iter().all(|component| !component.is_empty()) {
            components.join("/")
        } else {
            name.to_owned()
        };
        Ok(root.join(&format!("{relative}/"))?)
    }
}

impl TableDiscovery for FilesystemDiscovery {
    fn tables(&self) -> Result<Vec<String>> {
        if !self.allowlist.is_empty() {
            if self.root.scheme() != "file" {
                return Ok(self.allowlist.clone());
            }
            let mut names = Vec::new();
            let mut pending = Vec::new();
            for name in &self.allowlist {
                let table = self.url(name)?;
                let table_path = table
                    .to_file_path()
                    .map_err(|()| anyhow::anyhow!("local table URL is not a filesystem path"))?;
                let log_path = table_path.join("_delta_log");
                if !log_path.is_dir() {
                    pending.push(name.clone());
                    continue;
                }
                let mut entries = std::fs::read_dir(log_path)?;
                let mut has_delta_state = false;
                for entry in &mut entries {
                    let path = entry?.path();
                    if path
                        .extension()
                        .is_some_and(|extension| extension == "json" || extension == "parquet")
                    {
                        has_delta_state = true;
                        break;
                    }
                }
                if has_delta_state {
                    names.push(name.clone());
                } else {
                    pending.push(name.clone());
                }
            }
            pending.sort();
            pending.dedup();
            *self
                .pending_tables
                .lock()
                .map_err(|error| anyhow::anyhow!("pending tables mutex poisoned: {error}"))? = pending;
            return Ok(names);
        }
        let root = self
            .root
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("catalog required for cloud discovery"))?;
        let mut names = Vec::new();
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() && entry.path().join("_delta_log").is_dir() {
                names.push(
                    entry
                        .file_name()
                        .into_string()
                        .map_err(|_| anyhow::anyhow!("non-UTF8 table name"))?,
                );
            }
            if names.len() > MAX_TABLES {
                bail!("discovery exceeds {MAX_TABLES} tables; configure an allowlist");
            }
        }
        names.sort();
        Ok(names)
    }

    fn pending_tables(&self) -> Result<Vec<String>> {
        self.pending_tables
            .lock()
            .map(|pending| pending.clone())
            .map_err(|error| anyhow::anyhow!("pending tables mutex poisoned: {error}"))
    }

    fn table(&self, name: &str) -> Result<DiscoveredTable> {
        retry_stale_table_discovery(name, || {
            let url = self.url(name)?;
            let probe = DeltaTable::open_for_maintenance_probe_with_limit(&url, self.max_log_files)?;
            let token = probe.probe_log_token(self.max_log_files)?;
            let mut cache = self
                .cache
                .lock()
                .map_err(|_| anyhow::anyhow!("property cache poisoned"))?;
            let properties = match cache.get(name) {
                Some((cached_token, properties)) if *cached_token == token => properties.clone(),
                _ => {
                    let properties =
                        probe.maintenance_properties_with_limits(self.max_log_files, self.max_log_file_bytes)?;
                    ensure_property_token_unchanged(&token, &probe.probe_log_token(self.max_log_files)?)?;
                    if cache.len() >= MAX_TABLES && !cache.contains_key(name) {
                        cache.clear();
                    }
                    cache.insert(name.into(), (token.clone(), properties.clone()));
                    properties
                }
            };
            Ok(DiscoveredTable {
                name: name.into(),
                url,
                properties,
                log_token: Some(token),
            })
        })
    }
}

fn ensure_property_token_unchanged(before: &str, after: &str) -> Result<()> {
    if before != after {
        return Err(MaintenanceError {
            kind: MaintenanceErrorKind::StalePlan,
            message: "table advanced during property discovery; retry".into(),
            report: None,
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod property_discovery_tests {
    use super::*;

    #[test]
    fn allowlisted_local_tables_are_discovered_after_their_delta_log_is_created() -> Result<()> {
        let root = tempfile::tempdir()?;
        let name = "telemetry.sqlserver.sqlserver_active_sessions";
        let url = Url::from_directory_path(root.path())
            .map_err(|()| anyhow::anyhow!("temporary root is not a filesystem URL"))?;
        let discovery = FilesystemDiscovery::new(url, vec![name.to_owned()]);
        assert!(discovery.tables()?.is_empty());
        let table_path = root
            .path()
            .join("telemetry/sqlserver/sqlserver_active_sessions/_delta_log");
        std::fs::create_dir_all(&table_path)?;
        std::fs::write(table_path.join("00000000000000000000.json"), "{}")?;
        assert_eq!(discovery.tables()?, vec![name]);
        Ok(())
    }

    #[test]
    fn filesystem_discovery_resolves_qualified_table_identities_as_nested_paths() -> Result<()> {
        let discovery = FilesystemDiscovery::new(Url::parse("file:///tmp/arrowcol/dataflow")?, Vec::new());
        assert_eq!(
            discovery.url("telemetry.sqlserver.sqlserver_active_sessions")?.as_str(),
            "file:///tmp/arrowcol/dataflow/telemetry/sqlserver/sqlserver_active_sessions/"
        );
        assert_eq!(
            discovery.url("legacy_table")?.as_str(),
            "file:///tmp/arrowcol/dataflow/legacy_table/"
        );
        Ok(())
    }

    #[test]
    fn token_change_is_stale_plan_not_a_successful_probe() -> Result<()> {
        ensure_property_token_unchanged("version-1", "version-1")?;
        let Err(error) = ensure_property_token_unchanged("version-1", "version-2") else {
            bail!("changed log must require retry");
        };
        assert_eq!(
            error_details(&error).map(|detail| detail.kind),
            Some(MaintenanceErrorKind::StalePlan)
        );
        Ok(())
    }

    #[test]
    fn stale_negative_probe_falls_back_to_full_planner() -> Result<()> {
        let error = MaintenanceError {
            kind: MaintenanceErrorKind::StalePlan,
            message: "table advanced during negative probe; retry".into(),
            report: None,
        };
        let (report, stale) = negative_probe_result::<()>(Err(error.into()))?;
        assert!(report.is_none());
        assert!(stale);

        let (report, stale) = negative_probe_result(Ok(Some(7)))?;
        assert_eq!(report, Some(7));
        assert!(!stale);

        let error = MaintenanceError {
            kind: MaintenanceErrorKind::MetadataChanged,
            message: "table metadata changed".into(),
            report: None,
        };
        assert!(negative_probe_result::<()>(Err(error.into())).is_err());
        Ok(())
    }

    #[test]
    fn stale_table_discovery_retries_only_typed_stale_plan() -> Result<()> {
        let attempts = std::cell::Cell::new(0);
        let value = retry_stale_table_discovery("test_table", || {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 1 {
                Err(MaintenanceError {
                    kind: MaintenanceErrorKind::StalePlan,
                    message: "table advanced during property discovery".into(),
                    report: None,
                }
                .into())
            } else {
                Ok(attempts.get())
            }
        })?;
        assert_eq!(value, 2);
        assert_eq!(attempts.get(), 2);

        let attempts = std::cell::Cell::new(0);
        let Err(error) = retry_stale_table_discovery::<()>("test_table", || {
            attempts.set(attempts.get() + 1);
            Err(anyhow::anyhow!("invalid configuration"))
        }) else {
            bail!("non-stale discovery error was swallowed");
        };
        assert!(error.to_string().contains("invalid configuration"));
        assert_eq!(attempts.get(), 1);
        Ok(())
    }
}

fn process_defaults(settings: &Settings) -> BTreeMap<String, String> {
    let mut props = BTreeMap::new();
    for (key, value) in [
        (
            "arrowcol.optimize.enabled",
            settings.operations.contains(&Operation::Optimize).to_string(),
        ),
        (
            "arrowcol.purge.enabled",
            settings.operations.contains(&Operation::Purge).to_string(),
        ),
        (
            "arrowcol.vacuum.enabled",
            settings.operations.contains(&Operation::Vacuum).to_string(),
        ),
        (
            "arrowcol.vacuum.allowShortRetention",
            settings.allow_short_vacuum_retention.to_string(),
        ),
        (
            "arrowcol.optimize.targetFileSize",
            settings.optimize.target_file_bytes.to_string(),
        ),
        (
            "arrowcol.optimize.smallFileSize",
            settings.optimize.small_file_bytes.to_string(),
        ),
        (
            "arrowcol.optimize.maxFilesPerBin",
            settings.optimize.max_files_per_bin.to_string(),
        ),
        (
            "arrowcol.optimize.maxBinsPerCommit",
            settings.optimize.max_bins_per_commit.to_string(),
        ),
        (
            "arrowcol.optimize.maxActionsPerCommit",
            settings.optimize.max_actions_per_commit.to_string(),
        ),
        (
            "arrowcol.optimize.maxPlanSize",
            settings.optimize.max_plan_bytes.to_string(),
        ),
        (
            "arrowcol.optimize.maxDecodedSizePerBin",
            settings.optimize.max_decoded_bytes_per_bin.to_string(),
        ),
        (
            "arrowcol.optimize.maxOutputFileSize",
            settings.optimize.max_output_file_bytes.to_string(),
        ),
        (
            "arrowcol.optimize.maxActiveFiles",
            settings.optimize.max_active_files.to_string(),
        ),
        (
            "arrowcol.optimize.maxRowGroupSize",
            settings.optimize.max_row_group_bytes.to_string(),
        ),
        (
            "arrowcol.optimize.maxBatchSize",
            settings.optimize.max_batch_bytes.to_string(),
        ),
        ("arrowcol.optimize.batchRows", settings.optimize.batch_rows.to_string()),
        (
            "arrowcol.optimize.latestPartitions",
            settings.optimize.latest_partitions.unwrap_or(0).to_string(),
        ),
        (
            "arrowcol.optimize.sealedPartitionsOnly",
            (!settings.optimize.sealed_final).to_string(),
        ),
        (
            "delta.checkpointInterval",
            settings.checkpoint_every_commits.to_string(),
        ),
        (
            "arrowcol.maintenance.operationTimeout",
            settings.operation_timeout.as_millis().to_string(),
        ),
    ] {
        props.insert(key.into(), value);
    }
    if let Some(granularity) = settings.granularity {
        props.insert(
            "arrowcol.partition.granularity".into(),
            match granularity {
                Granularity::Minute => "minute",
                Granularity::Hour => "hour",
                Granularity::Day => "day",
            }
            .into(),
        );
    }
    if settings.vacuum_retention_ms >= arrow_deltalake::maintenance::MIN_VACUUM_RETENTION_MS {
        props.insert(
            "arrowcol.vacuum.retention".into(),
            settings.vacuum_retention_ms.to_string(),
        );
        props.insert(
            "delta.deletedFileRetentionDuration".into(),
            settings.vacuum_retention_ms.to_string(),
        );
    }
    if let Some(retention) = settings.purge_retention_ms {
        props.insert("arrowcol.purge.retention".into(), retention.to_string());
    }
    if let Some(retention) = settings.log_retention_ms
        && retention >= arrow_deltalake::maintenance::MIN_VACUUM_RETENTION_MS
    {
        props.insert("delta.logRetentionDuration".into(), retention.to_string());
    }
    if let Some(interval) = settings.interval {
        props.insert("arrowcol.maintenance.interval".into(), interval.as_millis().to_string());
    }
    if let Some(layout) = &settings.optimize.layout {
        for (key, value) in [
            ("arrowcol.layout.enabled", "true".into()),
            ("arrowcol.layout.sortColumns", layout.sort_columns.join(",")),
            ("arrowcol.layout.bloomColumns", layout.bloom_columns.join(",")),
            ("arrowcol.layout.maxRows", layout.max_rows.to_string()),
            ("arrowcol.layout.maxInputSize", layout.max_input_bytes.to_string()),
            ("arrowcol.layout.rowGroupRows", layout.row_group_rows.to_string()),
            ("arrowcol.layout.pageRows", layout.page_rows.to_string()),
        ] {
            props.insert(key.into(), value);
        }
    }
    props.extend(settings.process_properties.clone());
    props
}

fn effective_settings(settings: &Settings, properties: &BTreeMap<String, String>) -> Result<(Settings, ResolvedProps)> {
    let mut defaults = process_defaults(settings);
    if (properties.contains_key("arrowcol.optimize.targetFileSize") || properties.contains_key("delta.targetFileSize"))
        && !properties.contains_key("arrowcol.optimize.smallFileSize")
        && !settings
            .process_properties
            .contains_key("arrowcol.optimize.smallFileSize")
    {
        // The CLI small-file default is derived from its target, not an
        // independently supplied policy. Re-derive it from a table target.
        defaults.remove("arrowcol.optimize.smallFileSize");
    }
    let props = resolve(properties, &defaults)?;
    let mut effective = settings.clone();
    if !props.boolean("arrowcol.maintenance.enabled")? {
        effective.operations.clear();
        return Ok((effective, props));
    }
    let optimize_enabled = props.boolean("arrowcol.optimize.enabled")?;
    let purge_enabled = props.boolean("arrowcol.purge.enabled")?;
    let vacuum_enabled = props.boolean("arrowcol.vacuum.enabled")?;
    effective.operations.retain(|operation| match operation {
        Operation::Optimize => optimize_enabled,
        Operation::Purge => purge_enabled,
        Operation::Vacuum => vacuum_enabled,
        _ => true,
    });
    effective.granularity = Some(Granularity::parse_name(props.text("arrowcol.partition.granularity")?)?);
    effective.purge_retention_ms = Some(i64::try_from(props.unsigned("arrowcol.purge.retention")?)?);
    // CLI-only disposable-test overrides remain explicit invocation controls.
    // They cannot be enabled, persisted or weakened by table properties.
    if properties.contains_key("arrowcol.vacuum.retention")
        || settings.vacuum_retention_ms >= arrow_deltalake::maintenance::MIN_VACUUM_RETENTION_MS
    {
        effective.vacuum_retention_ms = i64::try_from(props.unsigned("arrowcol.vacuum.retention")?)?;
    }
    if properties.contains_key("delta.logRetentionDuration")
        || settings
            .log_retention_ms
            .is_none_or(|retention| retention >= arrow_deltalake::maintenance::MIN_VACUUM_RETENTION_MS)
    {
        effective.log_retention_ms = Some(i64::try_from(props.unsigned("delta.logRetentionDuration")?)?);
    }
    effective.checkpoint_every_commits = props.unsigned("delta.checkpointInterval")?;
    effective.allow_short_vacuum_retention = props.boolean("arrowcol.vacuum.allowShortRetention")?;
    effective.operation_timeout = Duration::from_millis(props.unsigned("arrowcol.maintenance.operationTimeout")?);
    effective.optimize.target_file_bytes = props.unsigned("arrowcol.optimize.targetFileSize")?;
    effective.optimize.small_file_bytes = props.unsigned("arrowcol.optimize.smallFileSize")?;
    effective.optimize.max_files_per_bin = usize::try_from(props.unsigned("arrowcol.optimize.maxFilesPerBin")?)?;
    effective.optimize.max_bins_per_commit = usize::try_from(props.unsigned("arrowcol.optimize.maxBinsPerCommit")?)?;
    effective.optimize.max_actions_per_commit =
        usize::try_from(props.unsigned("arrowcol.optimize.maxActionsPerCommit")?)?;
    effective.optimize.max_plan_bytes = usize::try_from(props.unsigned("arrowcol.optimize.maxPlanSize")?)?;
    effective.optimize.max_decoded_bytes_per_bin = props.unsigned("arrowcol.optimize.maxDecodedSizePerBin")?;
    effective.validate_decoded_budget(effective.optimize.max_decoded_bytes_per_bin)?;
    effective.optimize.max_output_file_bytes = props.unsigned("arrowcol.optimize.maxOutputFileSize")?;
    effective.optimize.max_active_files = usize::try_from(props.unsigned("arrowcol.optimize.maxActiveFiles")?)?;
    effective.optimize.max_row_group_bytes = props.unsigned("arrowcol.optimize.maxRowGroupSize")?;
    effective.optimize.max_batch_bytes = usize::try_from(props.unsigned("arrowcol.optimize.maxBatchSize")?)?;
    effective.optimize.batch_rows = usize::try_from(props.unsigned("arrowcol.optimize.batchRows")?)?;
    let latest = usize::try_from(props.unsigned("arrowcol.optimize.latestPartitions")?)?;
    effective.optimize.latest_partitions = (latest > 0).then_some(latest);
    effective.optimize.sealed_final = !props.boolean("arrowcol.optimize.sealedPartitionsOnly")?;
    effective.optimize.layout = if props.boolean("arrowcol.layout.enabled")? {
        Some(arrow_deltalake::layout::LayoutPolicy::from_properties(&props)?)
    } else {
        None
    };
    if properties.contains_key("arrowcol.layout.enabled")
        || settings.process_properties.contains_key("arrowcol.layout.enabled")
    {
        effective.optimize.use_default_layout = false;
    }
    effective.optimize.validate()?;
    Ok((effective, props))
}

fn configuration_evidence(startup: &Settings, settings: &Settings, props: &ResolvedProps) -> Result<serde_json::Value> {
    let mut policy = serde_json::Map::new();
    for key in arrowcol_props::REGISTRY {
        let name = key.name;
        if !(name.starts_with("arrowcol.maintenance.")
            || name.starts_with("arrowcol.optimize.")
            || name.starts_with("arrowcol.layout.")
            || name.starts_with("arrowcol.vacuum.")
            || name.starts_with("arrowcol.purge.")
            || name.starts_with("arrowcol.partition.")
            || matches!(
                name,
                "delta.targetFileSize"
                    | "delta.deletedFileRetentionDuration"
                    | "delta.logRetentionDuration"
                    | "delta.checkpointInterval"
            ))
        {
            continue;
        }
        let value = match props
            .get(name)
            .with_context(|| format!("resolved property missing: {name}"))?
        {
            PropValue::Bool(value) => serde_json::json!(value),
            PropValue::Unsigned(value) => serde_json::json!(value),
            PropValue::Float(value) => serde_json::json!(value),
            PropValue::Text(value) => serde_json::json!(value),
            PropValue::StringList(value) => serde_json::json!(value),
        };
        policy.insert(name.to_owned(), value);
    }
    let enabled = props.boolean("arrowcol.maintenance.enabled")?;
    let layout_mode = if settings.optimize.layout.is_some() {
        "explicit"
    } else if settings.optimize.use_default_layout {
        "automatic"
    } else {
        "disabled"
    };
    let optimize_upload_buffer_bytes_effective = match startup.operation_limits.optimize_upload_mode {
        OptimizeUploadMode::PutCreate => None,
        OptimizeUploadMode::BufferedMultipart => Some(
            startup
                .operation_limits
                .optimize_upload_buffer_bytes
                .unwrap_or(DEFAULT_OPTIMIZE_UPLOAD_BUFFER_BYTES),
        ),
    };
    let sealed_partitions_only_requested = startup
        .process_properties
        .get("arrowcol.optimize.sealedPartitionsOnly")
        .map(|value| value.parse::<bool>())
        .transpose()?;
    Ok(serde_json::json!({
        "startup": {
            "apply": startup.apply,
            "anchor_ms": startup.anchor_ms,
            "operations": startup.operations,
            "interval_secs": startup.interval.map(|value| value.as_secs()),
            "max_idle_backoff_secs": startup.max_idle_backoff.as_secs(),
            "max_cycles": startup.max_cycles,
            "max_consecutive_stale_plans": startup.max_consecutive_stale_plans,
            "sealed_partitions_only": sealed_partitions_only_requested,
            "max_concurrent_tables": startup.max_concurrent_tables,
            "max_concurrent_decoded_bytes": startup.max_concurrent_decoded_bytes,
            "commit_attempts": startup.commit_attempts,
            "commit_budget_secs": startup.commit_budget.as_secs(),
            "catalog_runtime_threads": startup.catalog_options.runtime_threads,
            "catalog_rpc_timeout_secs": startup.catalog_options.rpc_timeout.as_secs(),
            "catalog_max_decoding_message_bytes": startup.catalog_options.max_decoding_message_bytes,
            "max_prune_files": startup.max_prune_files,
            "output_row_group_rows": startup.optimize.output_row_group_rows,
            "input_read_strategy": startup.optimize.input_read_strategy,
            "operation_limits": startup.operation_limits,
        },
        "table_policy": policy,
        "parquet_writer_overrides": arrow_deltalake::writer::process_config()?,
        "optimize_upload": {
            "mode": startup.operation_limits.optimize_upload_mode,
            "buffer_bytes_requested": startup.operation_limits.optimize_upload_buffer_bytes,
            "buffer_bytes_effective": optimize_upload_buffer_bytes_effective,
        },
        "active": enabled,
        "operation_settings": enabled.then(|| serde_json::json!({
            "max_consecutive_stale_plans": settings.max_consecutive_stale_plans,
            "sealed_partitions_only": !settings.optimize.sealed_final,
            "layout_mode": layout_mode,
            "vacuum_retention_ms": settings.vacuum_retention_ms,
            "log_retention_ms": settings.log_retention_ms,
            "allow_short_vacuum_retention": settings.allow_short_vacuum_retention,
            "unsafe_short_vacuum_for_tests": settings.unsafe_short_vacuum_for_tests,
        })),
    }))
}

/// Validated startup defaults, before any table-level overrides.
///
/// # Errors
/// Returns invalid setting or property errors.
pub fn startup_configuration(settings: &Settings) -> Result<serde_json::Value> {
    settings.validate()?;
    let (effective, props) = effective_settings(settings, &BTreeMap::new())?;
    configuration_evidence(settings, &effective, &props)
}

fn policy_matches(settings: &Settings, actual: &BTreeMap<String, String>, expected: &ResolvedProps) -> Result<bool> {
    let (_, actual) = effective_settings(settings, actual)?;
    Ok(arrowcol_props::REGISTRY
        .iter()
        .all(|key| actual.get(key.name) == expected.get(key.name)))
}

type ScheduledTables = BTreeMap<String, (Instant, Vec<arrowcol_props::PropValue>)>;

fn policy_values(props: &ResolvedProps) -> Result<Vec<arrowcol_props::PropValue>> {
    arrowcol_props::REGISTRY
        .iter()
        .map(|key| {
            props
                .get(key.name)
                .cloned()
                .with_context(|| format!("resolved property missing: {}", key.name))
        })
        .collect()
}

fn table_gate(name: &str) -> &'static Mutex<()> {
    use std::hash::{Hash, Hasher};
    static GATES: OnceLock<[Mutex<()>; MAX_TABLES]> = OnceLock::new();
    let gates = GATES.get_or_init(|| std::array::from_fn(|_| Mutex::new(())));
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    name.trim_end_matches('/').hash(&mut hash);
    &gates[(hash.finish() % MAX_TABLES as u64) as usize]
}

/// Result of one operation on one table.
#[derive(Clone, Debug, Serialize)]
pub struct OperationResult {
    pub table: String,
    pub operation: Operation,
    pub applied: bool,
    /// This pass proved no useful work without loading a Kernel snapshot.
    pub negative_probe: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<MaintenanceReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optimize: Option<OptimizeEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vacuum: Option<VacuumEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub duration_ms: u64,
    pub probe_duration_ms: u64,
    pub plan_apply_duration_ms: u64,
}

/// Effective VACUUM retention and the caller's per-invocation acknowledgements.
#[derive(Clone, Debug, Serialize)]
pub struct VacuumEvidence {
    pub retention_ms: i64,
    pub allow_short_vacuum_retention: bool,
    pub unsafe_short_vacuum_for_tests: bool,
}

/// Exact planner inputs and effective bounds, retained even if applying fails.
#[derive(Clone, Debug, Serialize)]
pub struct OptimizeEvidence {
    pub config: serde_json::Value,
    pub plan: OptimizePlan,
}

fn layout_policy_sources(settings: &Settings, table_properties: &BTreeMap<String, String>) -> serde_json::Value {
    let keys = [
        "arrowcol.layout.enabled",
        "arrowcol.layout.sortColumns",
        "arrowcol.layout.bloomColumns",
        "arrowcol.layout.maxRows",
        "arrowcol.layout.maxInputSize",
        "arrowcol.layout.rowGroupRows",
        "arrowcol.layout.pageRows",
    ];
    let sources: BTreeMap<&str, &str> = keys
        .into_iter()
        .map(|key| {
            let source = if table_properties.contains_key(key) {
                "table"
            } else if settings.process_properties.contains_key(key) {
                "process"
            } else {
                "registry"
            };
            (key, source)
        })
        .collect();
    serde_json::json!(sources)
}

impl OptimizeEvidence {
    fn new(config: &OptimizeConfig, plan: OptimizePlan, layout_sources: serde_json::Value) -> Self {
        let resolved_layout = config.layout.as_ref().or(config.automatic_layout.as_ref());
        let layout_mode = if config.layout.is_some() && !config.use_default_layout {
            "explicit"
        } else if config.automatic_layout.is_some() || config.use_default_layout {
            "automatic"
        } else {
            "disabled"
        };
        Self {
            config: serde_json::json!({
                "sealed_final": config.sealed_final,
                "granularity": config.granularity,
                "now_ms": config.now_ms,
                "latest_partitions": config.latest_partitions,
                "target_file_bytes": config.target_file_bytes,
                "small_file_bytes": config.small_file_bytes,
                "max_files_per_bin": config.max_files_per_bin,
                "min_files_per_bin": config.min_files_per_bin,
                "max_bins_per_commit": config.max_bins_per_commit,
                "max_actions_per_commit": config.max_actions_per_commit,
                "max_plan_bytes": config.max_plan_bytes,
                "max_decoded_bytes_per_bin": config.max_decoded_bytes_per_bin,
                "max_row_group_bytes": config.max_row_group_bytes,
                "max_batch_bytes": config.max_batch_bytes,
                "batch_rows": config.batch_rows,
                "output_row_group_rows": config.output_row_group_rows,
                "use_default_layout": config.use_default_layout,
                "max_output_file_bytes": config.max_output_file_bytes,
                "max_active_files": config.max_active_files,
                "layout_mode": layout_mode,
                "layout_applied": plan.layout_fingerprint.is_some(),
                "layout_fingerprint": plan.layout_fingerprint,
                "output_policy_fingerprint": plan.output_policy_fingerprint,
                "layout_sources": layout_sources,
                "layout": plan.layout_fingerprint.as_ref().and_then(|_| resolved_layout.map(|layout| serde_json::json!({
                    "sort_columns": layout.sort_columns,
                    "max_rows": layout.max_rows,
                    "max_input_bytes": layout.max_input_bytes,
                    "row_group_rows": layout.row_group_rows,
                    "page_rows": layout.page_rows,
                    "bloom_columns": layout.bloom_columns,
                }))),
            }),
            plan,
        }
    }
}

impl OperationResult {
    fn outcome(&self) -> &'static str {
        match (&self.report, self.error_kind) {
            (_, Some(kind)) => kind,
            (Some(report), None) => report.outcome.label(),
            (None, None) => "noop",
        }
    }

    fn did_work(&self) -> bool {
        self.report.as_ref().is_some_and(|report| {
            report.files_removed > 0
                || report.files_added > 0
                || report.files_deleted > 0
                || report.backlog_files > 0
                || report.outcome == Outcome::Checkpointed
        })
    }
}

struct Instruments {
    runs: opentelemetry::metrics::Counter<u64>,
    totals: opentelemetry::metrics::Counter<u64>,
    backlog: opentelemetry::metrics::Gauge<u64>,
    pending_tables: opentelemetry::metrics::Gauge<u64>,
    cutoff: opentelemetry::metrics::Gauge<i64>,
    cycles: opentelemetry::metrics::Counter<u64>,
}

impl Instruments {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("delta-maintenance");
        Self {
            runs: meter.u64_counter("delta_maintenance.runs").build(),
            totals: meter.u64_counter("delta_maintenance.items").build(),
            backlog: meter.u64_gauge("delta_maintenance.backlog_files").build(),
            pending_tables: meter.u64_gauge("delta_maintenance.pending_tables").build(),
            cutoff: meter
                .i64_gauge("delta_maintenance.retention_cutoff")
                .with_unit("ms")
                .build(),
            cycles: meter.u64_counter("delta_maintenance.cycles").build(),
        }
    }
}

#[derive(Default)]
struct Counters {
    runs: BTreeMap<(&'static str, &'static str), u64>,
    totals: BTreeMap<(&'static str, &'static str), u64>,
    gauges: BTreeMap<(&'static str, &'static str), i64>,
    pending_tables: u64,
    heartbeat_ms: i64,
    cycles: u64,
    delta_targets: BTreeMap<String, DeltaTarget>,
}

/// Prometheus and OpenTelemetry metrics. Labels are restricted to the bounded
/// `operation` and `outcome` sets (item counters use the item name as outcome).
#[derive(Clone)]
pub struct Metrics(Arc<Mutex<Counters>>, Arc<Instruments>);

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create metrics bound to the current global OTel meter provider.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::default(), Arc::new(Instruments::new()))
    }

    fn invalidate_targets(&self) -> Result<()> {
        let mut counters = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("target readback mutex poisoned"))?;
        for target in counters.delta_targets.values_mut() {
            target.observe(None);
        }
        Ok(())
    }

    fn target_tables(&self, names: &[String], discovery: &dyn TableDiscovery) -> Result<()> {
        let mut counters = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("target readback mutex poisoned"))?;
        counters.delta_targets.retain(|name, _| names.contains(name));
        for name in names {
            let identity = discovery.table_identity(name)?;
            let target = counters
                .delta_targets
                .entry(name.clone())
                .or_insert_with(|| DeltaTarget::new(identity.clone()));
            if target.table != identity {
                target.state = TargetState::Conflicting;
            }
            target.observe(None);
        }
        Ok(())
    }

    fn observe_target(&self, name: &str, identity: Option<(String, String)>) -> Result<()> {
        let mut counters = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("target readback mutex poisoned"))?;
        counters
            .delta_targets
            .get_mut(name)
            .context("target absent from maintenance discovery")?
            .observe(identity);
        Ok(())
    }

    /// Serialize actual loaded Delta destination observations on the metrics listener.
    ///
    /// # Errors
    /// Returns readback synchronization or serialization errors.
    pub fn health_json(&self) -> Result<String> {
        let counters = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("target readback mutex poisoned"))?;
        Ok(serde_json::to_string(&serde_json::json!({
            "delta_targets": counters.delta_targets.values().collect::<Vec<_>>(),
        }))?)
    }

    fn with(&self, update: impl FnOnce(&mut Counters)) {
        if let Ok(mut counters) = self.0.lock() {
            update(&mut counters);
        }
    }

    fn heartbeat(&self) {
        let now = now_ms().unwrap_or_default();
        self.with(|counters| {
            counters.heartbeat_ms = now;
            counters.cycles += 1;
        });
        self.1.cycles.add(1, &[]);
    }

    fn pending_tables(&self, count: usize) {
        let count = u64::try_from(count).unwrap_or(u64::MAX);
        self.1.pending_tables.record(count, &[]);
        self.with(|counters| counters.pending_tables = count);
    }

    fn record(&self, result: &OperationResult) {
        let operation = result.operation.label();
        let outcome = result.outcome();
        let op = opentelemetry::KeyValue::new("operation", operation);
        self.1
            .runs
            .add(1, &[op.clone(), opentelemetry::KeyValue::new("outcome", outcome)]);
        let stale_plan = result.error_kind == Some(MaintenanceErrorKind::StalePlan.label());
        if stale_plan {
            self.1
                .totals
                .add(1, &[op.clone(), opentelemetry::KeyValue::new("outcome", "stale_plans")]);
        }
        if result.negative_probe {
            self.1.totals.add(
                1,
                &[op.clone(), opentelemetry::KeyValue::new("outcome", "negative_probes")],
            );
        }
        if let Some(report) = result
            .report
            .as_ref()
            .filter(|report| report.outcome != Outcome::Planned)
        {
            for (name, value) in report_items(report) {
                self.1
                    .totals
                    .add(value, &[op.clone(), opentelemetry::KeyValue::new("outcome", name)]);
            }
        }
        if let Some(report) = &result.report {
            self.1.backlog.record(report.backlog_files, std::slice::from_ref(&op));
            if let Some(cutoff) = report.cutoff_ms {
                self.1.cutoff.record(cutoff, &[op]);
            }
        }
        self.with(|counters| {
            *counters.runs.entry((operation, outcome)).or_default() += 1;
            if stale_plan {
                *counters.totals.entry((operation, "stale_plans")).or_default() += 1;
            }
            if result.negative_probe {
                *counters.totals.entry((operation, "negative_probes")).or_default() += 1;
            }
            let report = result.report.as_ref();
            if let Some(report) = report {
                for (name, value) in report_items(report) {
                    if report.outcome != Outcome::Planned {
                        *counters.totals.entry((operation, name)).or_default() += value;
                    }
                }
                counters.gauges.insert(
                    (operation, "backlog_files"),
                    i64::try_from(report.backlog_files).unwrap_or(i64::MAX),
                );
                if let Some(cutoff) = report.cutoff_ms {
                    counters.gauges.insert((operation, "retention_cutoff_ms"), cutoff);
                }
                if let Some(watermark) = report.watermark_ms {
                    counters.gauges.insert((operation, "purge_watermark_ms"), watermark);
                }
            }
        });
    }

    /// Render the text exposition format.
    #[must_use]
    pub fn render(&self) -> String {
        let mut text = String::new();
        let Ok(counters) = self.0.lock() else {
            return text;
        };
        let _ = writeln!(text, "# TYPE delta_maintenance_runs_total counter");
        for ((operation, outcome), value) in &counters.runs {
            let _ = writeln!(
                text,
                "delta_maintenance_runs_total{{operation=\"{operation}\",outcome=\"{outcome}\"}} {value}"
            );
        }
        let mut last = "";
        for ((operation, name), value) in &counters.totals {
            if *name != last {
                let _ = writeln!(text, "# TYPE delta_maintenance_{name}_total counter");
            }
            last = name;
            let _ = writeln!(
                text,
                "delta_maintenance_{name}_total{{operation=\"{operation}\"}} {value}"
            );
        }
        for ((operation, name), value) in &counters.gauges {
            let _ = writeln!(text, "# TYPE delta_maintenance_{name} gauge");
            let _ = writeln!(text, "delta_maintenance_{name}{{operation=\"{operation}\"}} {value}");
        }
        let _ = writeln!(text, "# TYPE delta_maintenance_pending_tables gauge");
        let _ = writeln!(text, "delta_maintenance_pending_tables {}", counters.pending_tables);
        let _ = writeln!(text, "# TYPE delta_maintenance_heartbeat_timestamp_ms gauge");
        let _ = writeln!(
            text,
            "delta_maintenance_heartbeat_timestamp_ms {}",
            counters.heartbeat_ms
        );
        let _ = writeln!(text, "# TYPE delta_maintenance_cycles_total counter");
        let _ = writeln!(text, "delta_maintenance_cycles_total {}", counters.cycles);
        text
    }
}

fn report_items(report: &MaintenanceReport) -> [(&'static str, u64); 10] {
    [
        ("files_removed", report.files_removed),
        ("files_added", report.files_added),
        ("rows_removed", report.rows_removed),
        ("rows_added", report.rows_added),
        ("bytes_removed", report.bytes_removed),
        ("bytes_added", report.bytes_added),
        ("files_deleted", report.files_deleted),
        ("files_already_absent", report.files_already_absent),
        ("collisions", u64::from(report.collisions)),
        ("retries", u64::from(report.retries)),
    ]
}

/// Serve `GET /metrics` and JSON `GET /health` on a bounded listener until cancelled.
///
/// # Errors
/// Returns bind errors.
pub fn serve_metrics(
    listen: SocketAddr,
    metrics: Metrics,
    cancel: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>> {
    let listener = TcpListener::bind(listen).with_context(|| format!("bind metrics listener {listen}"))?;
    listener.set_nonblocking(true)?;
    Ok(std::thread::spawn(move || {
        while !cancel.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let _ = respond(stream, &metrics);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => std::thread::sleep(Duration::from_millis(200)),
            }
        }
    }))
}

fn respond(mut stream: TcpStream, metrics: &Metrics) -> Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") && request.len() < MAX_HTTP_REQUEST {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
    }
    let (status, content_type, body) = if request.starts_with(b"GET /metrics ") {
        ("200 OK", "text/plain; version=0.0.4", metrics.render())
    } else if request.starts_with(b"GET /health ") {
        match metrics.health_json() {
            Ok(body) => ("200 OK", "application/json", body),
            Err(error) => {
                eprintln!("delta-maintenance health readback failed: {error:#}");
                (
                    "503 Service Unavailable",
                    "application/json",
                    String::from(r#"{"delta_targets":[],"error":"target readback unavailable"}"#),
                )
            }
        }
    } else {
        ("404 Not Found", "text/plain", String::from("not found\n"))
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    Ok(())
}

#[cfg(test)]
mod target_readback_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn target(metrics: &Metrics) -> Result<serde_json::Value> {
        let health: serde_json::Value = serde_json::from_str(&metrics.health_json()?)?;
        Ok(health["delta_targets"][0].clone())
    }

    #[test]
    fn target_readback_requires_actual_initialized_metadata_without_data_commits() -> Result<()> {
        let root = tempfile::tempdir()?;
        let root_url = Url::from_directory_path(root.path()).map_err(|()| anyhow::anyhow!("temporary root URL"))?;
        let name = "telemetry.sqlserver.events";
        let discovery = FilesystemDiscovery::new(root_url, vec![name.into()]);
        let metrics = Metrics::new();
        metrics.target_tables(&[name.into()], &discovery)?;
        assert_eq!(target(&metrics)?["state"], "unresolved");
        assert!(target(&metrics)?.get("normalized_root").is_none());
        assert!(target(&metrics)?.get("delta_table_id").is_none());

        let url = discovery.url(name)?;
        DeltaTable::initialize_url_for_bootstrap(
            url.clone(),
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            Vec::new(),
        )?;
        let mut sensitive_url = url.clone();
        sensitive_url.set_query(Some("sig=secret"));
        sensitive_url.set_fragment(Some("fragment"));
        let probe = discovery.open_probe_table(&sensitive_url, 100)?;
        metrics.observe_target(name, probe.maintenance_target_with_limits(100, 1024 * 1024)?)?;
        let readback = target(&metrics)?;
        assert_eq!(
            readback["table"],
            serde_json::json!({
                "catalog": "telemetry", "schema": "sqlserver", "table": "events",
            })
        );
        assert_eq!(readback["state"], "resolved");
        assert_eq!(readback["normalized_root"], url.as_str().trim_end_matches('/'));
        let log = url
            .to_file_path()
            .map_err(|()| anyhow::anyhow!("table path"))?
            .join("_delta_log");
        let metadata_id = std::fs::read_to_string(log.join("00000000000000000000.json"))?
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .find_map(|action| action["metaData"]["id"].as_str().map(str::to_owned))
            .context("initialized metadata ID")?;
        assert_eq!(readback["delta_table_id"], metadata_id);
        assert!(!metrics.health_json()?.contains("secret"));
        assert!(!metrics.health_json()?.contains("fragment"));
        assert!(!log.join("00000000000000000001.json").exists());
        Ok(())
    }

    #[test]
    fn target_readback_conflicts_latch_and_unresolved_never_exposes_prior_identity() -> Result<()> {
        for changed in [("file:///tables/other", "first"), ("file:///tables/events", "second")] {
            let mut readback = DeltaTarget::new(TargetTable::from_name("telemetry.sqlserver.events"));
            readback.observe(Some(("file:///tables/events".into(), "first".into())));
            readback.observe(None);
            let unresolved = serde_json::to_value(&readback)?;
            assert_eq!(unresolved["state"], "unresolved");
            assert!(unresolved.get("normalized_root").is_none());
            assert!(unresolved.get("delta_table_id").is_none());
            readback.observe(Some((changed.0.into(), changed.1.into())));
            readback.observe(Some(("file:///tables/events".into(), "first".into())));
            let conflicting = serde_json::to_value(&readback)?;
            assert_eq!(conflicting["state"], "conflicting");
            assert!(conflicting.get("normalized_root").is_none());
            assert!(conflicting.get("delta_table_id").is_none());
        }
        Ok(())
    }

    #[test]
    fn target_readback_redacts_actual_root_and_rejects_unsafe_roots() -> Result<()> {
        let mut readback = DeltaTarget::new(TargetTable::from_name("telemetry.sqlserver.events"));
        let root = arrow_deltalake::delta::normalize_delta_target_root(
            "https://user:password@account.blob.core.windows.net/container/events?sig=secret#fragment",
        )
        .context("safe normalized root")?;
        readback.observe(Some((root, "actual-metadata-id".into())));
        let json = serde_json::to_string(&readback)?;
        assert!(json.contains("https://account.blob.core.windows.net/container/events"));
        for forbidden in ["user", "password", "secret", "fragment", "sig=", "?"] {
            assert!(!json.contains(forbidden), "{json}");
        }
        readback
            .observe(arrow_deltalake::delta::normalize_delta_target_root("not a URI").map(|root| (root, "id".into())));
        assert_eq!(serde_json::to_value(&readback)?["state"], "unresolved");
        Ok(())
    }

    #[test]
    fn health_route_serializes_target_state_as_json() -> Result<()> {
        let metrics = Metrics::new();
        let discovery = FilesystemDiscovery::new(Url::parse("file:///unused/")?, Vec::new());
        metrics.target_tables(&["telemetry.sqlserver.events".into()], &discovery)?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let mut client = TcpStream::connect(listener.local_addr()?)?;
        client.write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
        respond(listener.accept()?.0, &metrics)?;
        let mut response = String::new();
        client.read_to_string(&mut response)?;
        assert!(response.starts_with("HTTP/1.1 200 OK\r\nContent-Type: application/json"));
        let (_, body) = response.split_once("\r\n\r\n").context("HTTP body")?;
        let health: serde_json::Value = serde_json::from_str(body)?;
        assert_eq!(health["delta_targets"][0]["state"], "unresolved");
        Ok(())
    }
}

struct OperationProperties<'a> {
    table: &'a BTreeMap<String, String>,
    resolved: &'a ResolvedProps,
}

fn run_operation(
    settings: &Settings,
    table: &DeltaTable,
    properties: OperationProperties<'_>,
    operation: Operation,
    anchor: i64,
    control: &MaintenanceControl,
    optimize: &mut Option<OptimizeEvidence>,
) -> Result<MaintenanceReport> {
    let props = properties.resolved;
    let plan = match operation {
        Operation::Checkpoint => {
            return table.checkpoint_if_due_at(settings.checkpoint_every_commits, settings.apply, anchor);
        }
        Operation::PruneLog => {
            let config = PruneConfig {
                log_retention_ms: settings.log_retention_ms,
                unsafe_allow_short_retention_for_tests: settings.unsafe_short_vacuum_for_tests,
                max_prune_files: settings.max_prune_files,
                max_log_files: settings.operation_limits.replay_max_log_files,
                max_log_file_bytes: settings.operation_limits.replay_max_log_file_bytes,
                max_checkpoint_bytes: settings.operation_limits.replay_max_checkpoint_bytes,
                max_active_files: settings.operation_limits.operation_max_active_files,
                ..PruneConfig::new(anchor)
            };
            let plan = table.plan_prune_log(&config)?;
            if settings.apply {
                return table.apply_prune_log(&plan, &config, control);
            }
            return Ok(plan.report());
        }
        Operation::Optimize => {
            let automatic_layout = if settings.optimize.layout.is_none() && settings.optimize.use_default_layout {
                Some(arrow_deltalake::layout::LayoutPolicy::from_properties(props)?)
            } else {
                None
            };
            let automatic_layout_sort_columns_explicit = props.raw.contains_key("arrowcol.layout.sortColumns");
            let config = OptimizeConfig {
                automatic_layout,
                automatic_layout_sort_columns_explicit,
                granularity: settings.granularity,
                now_ms: Some(anchor),
                min_files_per_bin: usize::try_from(props.unsigned("arrowcol.optimize.minFiles")?)?,
                ..settings.optimize.clone()
            };
            let writer_config = arrow_deltalake::writer::process_config()?;
            let prepared = table.prepare_optimize_with_writer_config(&config, writer_config)?;
            *optimize = Some(OptimizeEvidence::new(
                &config,
                prepared.plan.clone(),
                layout_policy_sources(settings, properties.table),
            ));
            if settings.apply && (!prepared.plan.bins.is_empty() || config.sealed_final) {
                let upload_buffer_bytes =
                    if settings.operation_limits.optimize_upload_mode == OptimizeUploadMode::BufferedMultipart {
                        Some(
                            settings
                                .operation_limits
                                .optimize_upload_buffer_bytes
                                .unwrap_or(DEFAULT_OPTIMIZE_UPLOAD_BUFFER_BYTES),
                        )
                    } else {
                        None
                    };
                return table.apply_prepared_optimize_with_writer_config(
                    &prepared,
                    &config,
                    control,
                    upload_buffer_bytes,
                    writer_config,
                );
            }
            MaintenancePlan::Optimize(prepared.plan)
        }
        Operation::Purge => {
            let granularity = settings.granularity.context("purge granularity")?;
            let retention = settings.purge_retention_ms.context("purge retention")?;
            let config = PurgeConfig {
                max_files_per_commit: settings.operation_limits.purge_max_files_per_commit,
                max_plan_bytes: settings.operation_limits.purge_max_plan_bytes,
                max_active_files: settings.operation_limits.operation_max_active_files,
                ..PurgeConfig::new(retention, anchor, granularity)
            };
            if settings.apply {
                let mut expected_fingerprint = None;
                return retry_stale_purge(
                    settings.commit_attempts,
                    settings.commit_budget,
                    control,
                    |attempt_control| {
                        attempt_control.check()?;
                        let prepared = table.prepare_purge(&config)?;
                        require_purge_fingerprint(&mut expected_fingerprint, &prepared.plan.header.fingerprint)?;
                        attempt_control.check()?;
                        table.apply_prepared_purge(&prepared, &config, attempt_control)
                    },
                );
            }
            let prepared = table.prepare_purge(&config)?;
            MaintenancePlan::Purge(prepared.plan)
        }
        Operation::Vacuum => {
            let mut config = VacuumConfig::new(settings.vacuum_retention_ms, anchor);
            config.allow_short_vacuum_retention = settings.allow_short_vacuum_retention;
            config.unsafe_allow_short_retention_for_tests = settings.unsafe_short_vacuum_for_tests;
            config.max_deletes = usize::try_from(props.unsigned("arrowcol.vacuum.maxDeletes")?)?;
            config.delete_chunk = settings.operation_limits.vacuum_delete_chunk;
            config.max_plan_bytes = settings.operation_limits.vacuum_max_plan_bytes;
            config.max_log_files = settings.operation_limits.replay_max_log_files;
            config.max_log_file_bytes = settings.operation_limits.replay_max_log_file_bytes;
            config.max_checkpoint_bytes = settings.operation_limits.replay_max_checkpoint_bytes;
            config.max_active_files = settings.operation_limits.operation_max_active_files;
            return table.vacuum_once(&config, settings.apply, control);
        }
    };
    let mut report = plan.report();
    report.version = report.read_version;
    if settings.apply {
        report.outcome = Outcome::Noop;
    }
    Ok(report)
}

fn negative_probe_result<T>(result: Result<Option<T>>) -> Result<(Option<T>, bool)> {
    match result {
        Ok(report) => Ok((report, false)),
        Err(error) if is_stale_plan(&error) => Ok((None, true)),
        Err(error) => Err(error),
    }
}

fn is_stale_plan(error: &anyhow::Error) -> bool {
    error_details(error).is_some_and(|details| details.kind == MaintenanceErrorKind::StalePlan)
}

#[derive(Clone, Copy, Debug, Default)]
struct PurgeRetryCounters {
    attempts: u32,
    collisions: u32,
    retries: u32,
    replans: u32,
}

impl PurgeRetryCounters {
    fn include(&mut self, report: &MaintenanceReport) {
        self.attempts = self.attempts.saturating_add(report.attempts);
        self.collisions = self.collisions.saturating_add(report.collisions);
        self.retries = self.retries.saturating_add(report.retries);
        self.replans = self.replans.saturating_add(report.replans);
    }

    fn write_to(self, report: &mut MaintenanceReport) {
        report.attempts = report.attempts.saturating_add(self.attempts);
        report.collisions = report.collisions.saturating_add(self.collisions);
        report.retries = report.retries.saturating_add(self.retries);
        report.replans = report.replans.saturating_add(self.replans);
    }
}

fn require_purge_fingerprint(expected: &mut Option<TableFingerprint>, current: &TableFingerprint) -> Result<()> {
    if expected.as_ref().is_some_and(|fingerprint| fingerprint != current) {
        return Err(anyhow::Error::new(MaintenanceError {
            kind: MaintenanceErrorKind::MetadataChanged,
            message: "table metadata changed while replanning PURGE".to_owned(),
            report: None,
        }));
    }
    expected.get_or_insert_with(|| current.clone());
    Ok(())
}

fn retry_stale_purge(
    max_attempts: u32,
    budget: Duration,
    control: &MaintenanceControl,
    mut apply: impl FnMut(&MaintenanceControl) -> Result<MaintenanceReport>,
) -> Result<MaintenanceReport> {
    let started = Instant::now();
    let max_attempts = max_attempts.max(1);
    let mut retries = 0;
    let mut replan_pending = false;
    let mut counters = PurgeRetryCounters::default();
    loop {
        control
            .check()
            .map_err(|error| with_purge_retry_counts(error, counters))?;
        let remaining_budget = budget.saturating_sub(started.elapsed());
        let remaining_attempts = max_attempts.saturating_sub(counters.attempts);
        if remaining_attempts == 0 {
            return Err(purge_retry_budget_error(
                counters,
                MaintenanceErrorKind::Exhausted,
                "PURGE commit-attempt budget exhausted while replanning",
            ));
        }
        if remaining_budget.is_zero() {
            return Err(purge_retry_budget_error(
                counters,
                MaintenanceErrorKind::DeadlineExceeded,
                "PURGE end-to-end retry budget expired",
            ));
        }
        let attempt_control = control
            .clone()
            .with_deadline(started + budget)
            .with_commit_retry_limits(remaining_attempts, remaining_budget)?;
        attempt_control
            .check()
            .map_err(|error| with_purge_retry_counts(error, counters))?;
        if replan_pending {
            counters.replans = counters.replans.saturating_add(1);
        }
        match apply(&attempt_control) {
            Ok(mut report) => {
                counters.write_to(&mut report);
                return Ok(report);
            }
            Err(error) => {
                if let Some(report) = error_report(&error) {
                    counters.include(report);
                }
                if !is_purge_membership_stale(&error) {
                    return Err(with_purge_retry_counts(error, counters));
                }
                let backoff = Duration::from_millis(5_u64 << retries.min(5));
                if counters.attempts >= max_attempts {
                    return Err(with_purge_retry_counts(error, counters));
                }
                if started.elapsed().saturating_add(backoff) >= budget {
                    return Err(purge_retry_budget_error(
                        counters,
                        MaintenanceErrorKind::DeadlineExceeded,
                        "PURGE end-to-end retry budget expired during membership replan",
                    ));
                }
                retries += 1;
                std::thread::sleep(backoff);
                replan_pending = true;
            }
        }
    }
}

fn is_purge_membership_stale(error: &anyhow::Error) -> bool {
    error_details(error).is_some_and(|details| {
        details.kind == MaintenanceErrorKind::StalePlan
            && matches!(
                details.message.as_str(),
                "planned partition is now the newest partition" | "expired partitions changed since planning; replan"
            )
    })
}

fn with_purge_retry_counts(error: anyhow::Error, counters: PurgeRetryCounters) -> anyhow::Error {
    let mut report = error_report(&error).cloned().unwrap_or_default();
    report.operation = "purge".to_owned();
    report.attempts = counters.attempts;
    report.collisions = counters.collisions;
    report.retries = counters.retries;
    report.replans = counters.replans;
    attach_error_report(error, &report)
}

fn purge_retry_budget_error(counters: PurgeRetryCounters, kind: MaintenanceErrorKind, message: &str) -> anyhow::Error {
    let mut report = MaintenanceReport {
        operation: "purge".to_owned(),
        ..MaintenanceReport::default()
    };
    report.attempts = counters.attempts;
    report.collisions = counters.collisions;
    report.retries = counters.retries;
    report.replans = counters.replans;
    anyhow::Error::new(MaintenanceError {
        kind,
        message: message.to_owned(),
        report: Some(Box::new(report)),
    })
}

#[cfg(test)]
mod purge_replan_tests {
    use super::*;

    fn membership_stale(report: MaintenanceReport) -> anyhow::Error {
        anyhow::Error::new(MaintenanceError {
            kind: MaintenanceErrorKind::StalePlan,
            message: "expired partitions changed since planning; replan".to_owned(),
            report: Some(Box::new(report)),
        })
    }

    fn fingerprint(policy: &str) -> TableFingerprint {
        TableFingerprint {
            table_id: "table-id".to_owned(),
            schema_string: "{}".to_owned(),
            partition_columns: vec!["ingestion".to_owned()],
            configuration: BTreeMap::from([("maintenance.policy".to_owned(), policy.to_owned())]),
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
        }
    }

    fn report(attempts: u32) -> MaintenanceReport {
        MaintenanceReport {
            operation: "purge".to_owned(),
            attempts,
            ..MaintenanceReport::default()
        }
    }

    fn require_error<T>(result: Result<T>, message: &str) -> Result<anyhow::Error> {
        match result {
            Err(error) => Ok(error),
            Ok(_) => Err(anyhow::anyhow!("{message}")),
        }
    }

    #[test]
    fn purge_replans_after_collision_and_keeps_replan_count_separate() -> Result<()> {
        let calls = std::cell::Cell::new(0);
        let mut collided = report(2);
        collided.collisions = 1;
        collided.retries = 1;
        let result = retry_stale_purge(4, Duration::from_secs(1), &MaintenanceControl::default(), |_| {
            let call = calls.get() + 1;
            calls.set(call);
            if call == 1 {
                return Err(membership_stale(collided.clone()));
            }
            Ok(report(1))
        })?;

        assert_eq!(calls.get(), 2);
        assert_eq!(result.attempts, 3);
        assert_eq!(result.collisions, 1);
        assert_eq!(result.retries, 1);
        assert_eq!(result.replans, 1);
        Ok(())
    }

    #[test]
    fn purge_replanning_is_bounded_by_total_publish_attempts() -> Result<()> {
        let calls = std::cell::Cell::new(0);
        let error = require_error(
            retry_stale_purge(2, Duration::from_secs(1), &MaintenanceControl::default(), |_| {
                calls.set(calls.get() + 1);
                Err(membership_stale(report(1)))
            }),
            "persistent membership churn must exhaust its attempt budget",
        )?;

        assert_eq!(calls.get(), 2);
        let details = error_details(&error).ok_or_else(|| anyhow::anyhow!("classified maintenance error"))?;
        assert_eq!(details.kind, MaintenanceErrorKind::StalePlan);
        let report = error_report(&error).ok_or_else(|| anyhow::anyhow!("aggregated counters"))?;
        assert_eq!(report.attempts, 2);
        assert_eq!(report.replans, 1);
        assert_eq!(report.retries, 0);
        Ok(())
    }

    #[test]
    fn purge_policy_change_after_membership_stale_is_a_hard_failure() -> Result<()> {
        let calls = std::cell::Cell::new(0);
        let applies = std::cell::Cell::new(0);
        let mut expected = None;
        let error = require_error(
            retry_stale_purge(4, Duration::from_secs(1), &MaintenanceControl::default(), |_| {
                let call = calls.get() + 1;
                calls.set(call);
                let current = if call == 1 {
                    fingerprint("retention=30d")
                } else {
                    fingerprint("retention=90d")
                };
                require_purge_fingerprint(&mut expected, &current)?;
                applies.set(applies.get() + 1);
                if call == 1 {
                    Err(membership_stale(report(1)))
                } else {
                    Ok(report(1))
                }
            }),
            "changed policy must not be adopted by a replan",
        )?;

        assert_eq!(calls.get(), 2);
        assert_eq!(applies.get(), 1, "the changed-policy plan must not be applied");
        let details = error_details(&error).ok_or_else(|| anyhow::anyhow!("classified metadata error"))?;
        assert_eq!(details.kind, MaintenanceErrorKind::MetadataChanged);
        let report = error_report(&error).ok_or_else(|| anyhow::anyhow!("first attempt counters"))?;
        assert_eq!(report.attempts, 1);
        assert_eq!(report.replans, 1);
        Ok(())
    }

    #[test]
    fn purge_deadline_includes_membership_backoff() -> Result<()> {
        let calls = std::cell::Cell::new(0);
        let error = require_error(
            retry_stale_purge(4, Duration::from_millis(1), &MaintenanceControl::default(), |_| {
                calls.set(calls.get() + 1);
                Err(membership_stale(report(1)))
            }),
            "the shared budget cannot fit another replan",
        )?;

        assert_eq!(calls.get(), 1);
        let details = error_details(&error).ok_or_else(|| anyhow::anyhow!("classified deadline error"))?;
        assert_eq!(details.kind, MaintenanceErrorKind::DeadlineExceeded);
        let report = details
            .report
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("attempt counters"))?;
        assert_eq!(report.attempts, 1);
        assert_eq!(report.replans, 0, "budget rejection did not admit another rebuild");
        Ok(())
    }

    #[test]
    fn purge_conflict_then_cancel_preserves_partial_report_and_source() -> Result<()> {
        let mut partial = report(2);
        partial.collisions = 1;
        let calls = std::cell::Cell::new(0);
        let error = require_error(
            retry_stale_purge(4, Duration::from_secs(1), &MaintenanceControl::default(), |_| {
                calls.set(calls.get() + 1);
                let source = anyhow::Error::new(MaintenanceError {
                    kind: MaintenanceErrorKind::Cancelled,
                    message: "maintenance cancelled".to_owned(),
                    report: None,
                })
                .context(std::io::Error::other("injected cancellation source"));
                Err(attach_error_report(source, &partial))
            }),
            "terminal cancellation should retain the completed conflict attempt",
        )?;

        assert_eq!(calls.get(), 1);
        let details = error_details(&error).ok_or_else(|| anyhow::anyhow!("classified cancellation"))?;
        assert_eq!(details.kind, MaintenanceErrorKind::Cancelled);
        let report = error_report(&error).ok_or_else(|| anyhow::anyhow!("partial publish report"))?;
        assert_eq!(report.attempts, 2);
        assert_eq!(report.collisions, 1);
        assert_eq!(report.replans, 0);
        assert!(
            error
                .chain()
                .any(|cause| cause.to_string() == "injected cancellation source")
        );
        Ok(())
    }

    #[test]
    fn purge_preparation_consumes_the_shared_retry_deadline() -> Result<()> {
        let calls = std::cell::Cell::new(0);
        let error = require_error(
            retry_stale_purge(
                4,
                Duration::from_millis(1),
                &MaintenanceControl::default(),
                |attempt_control| {
                    calls.set(calls.get() + 1);
                    std::thread::sleep(Duration::from_millis(2));
                    attempt_control.check()?;
                    Ok(report(1))
                },
            ),
            "slow preparation must consume the shared deadline",
        )?;

        assert_eq!(calls.get(), 1);
        let details = error_details(&error).ok_or_else(|| anyhow::anyhow!("classified deadline error"))?;
        assert_eq!(details.kind, MaintenanceErrorKind::DeadlineExceeded);
        Ok(())
    }

    #[test]
    fn purge_does_not_replan_for_other_stale_reasons() -> Result<()> {
        let calls = std::cell::Cell::new(0);
        let error = require_error(
            retry_stale_purge(4, Duration::from_secs(1), &MaintenanceControl::default(), |_| {
                calls.set(calls.get() + 1);
                Err(anyhow::Error::new(MaintenanceError {
                    kind: MaintenanceErrorKind::StalePlan,
                    message: "plan cutoff is later than the apply configuration allows".to_owned(),
                    report: None,
                }))
            }),
            "retention changes must remain a hard stale-plan failure",
        )?;

        assert_eq!(calls.get(), 1);
        assert!(is_stale_plan(&error));
        Ok(())
    }
}

fn retry_stale_table_discovery<T>(table: &str, mut discover: impl FnMut() -> Result<T>) -> Result<T> {
    let mut retries = 0;
    loop {
        match discover() {
            Err(error) if retries < MAX_STALE_DISCOVERY_RETRIES && is_stale_plan(&error) => {
                retries += 1;
                eprintln!("delta-maintenance table discovery became stale; retrying table={table} attempt={retries}");
            }
            result => return result,
        }
    }
}

fn maintain_table(
    settings: &Settings,
    discovery: &dyn TableDiscovery,
    name: &str,
    force_abort: &Arc<AtomicBool>,
    emit: &dyn Fn(&OperationResult),
    schedule: Option<&Mutex<ScheduledTables>>,
    metrics: &Metrics,
) -> Vec<OperationResult> {
    let mut results = Vec::new();
    let setup = (|| -> Result<_> {
        let mut discovered = discovery.table(name)?;
        let gate = table_gate(discovered.url.as_str())
            .lock()
            .map_err(|_| anyhow::anyhow!("per-table maintenance gate poisoned"))?;
        let limits = &settings.operation_limits;
        let probe = discovery.open_probe_table(&discovered.url, limits.discovery_max_log_files);
        // Storage errors may include credential-bearing URIs.
        let metadata = match &probe {
            Ok(probe) => match probe
                .maintenance_metadata_with_limits(limits.discovery_max_log_files, limits.discovery_max_log_file_bytes)
            {
                Ok(metadata) => Some(metadata),
                Err(_) => {
                    eprintln!("delta-maintenance target metadata readback failed; identity unresolved table={name}");
                    None
                }
            },
            Err(_) => {
                eprintln!("delta-maintenance target open failed; identity unresolved table={name}");
                None
            }
        };
        if let Err(error) = metrics.observe_target(name, metadata.as_ref().and_then(|metadata| metadata.target.clone()))
        {
            eprintln!("delta-maintenance target readback update failed: {error:#}");
        }
        let (effective, props) = effective_settings(settings, &discovered.properties)?;
        if let Some(schedule) = schedule {
            let mut schedule = schedule
                .lock()
                .map_err(|_| anyhow::anyhow!("maintenance schedule poisoned"))?;
            let now = Instant::now();
            let delay = Duration::from_millis(props.unsigned("arrowcol.maintenance.interval")?);
            let policy = policy_values(&props)?;
            if let Some((last_run, previous)) = schedule.get(name)
                && *previous == policy
                && now
                    .checked_duration_since(*last_run)
                    .context("maintenance clock moved backwards")?
                    < delay
            {
                return Ok(None);
            }
            schedule.insert(name.into(), (now, policy));
        }
        let probe = probe?;
        let token = probe.probe_log_token(limits.discovery_max_log_files)?;
        if discovered.log_token.as_ref().is_none_or(|previous| *previous != token) {
            let current_properties = match metadata.filter(|metadata| metadata.log_token == token) {
                Some(metadata) => metadata.properties,
                None => probe.maintenance_properties_with_limits(
                    limits.discovery_max_log_files,
                    limits.discovery_max_log_file_bytes,
                )?,
            };
            if !policy_matches(settings, &current_properties, &props)? {
                bail!("table properties changed after discovery; retry");
            }
        }
        discovered.log_token = Some(token);
        let config = configuration_evidence(settings, &effective, &props)?;
        eprintln!(
            "arrowcol_maintenance_table_config {}",
            serde_json::json!({ "table": name, "config": config })
        );
        Ok(Some((discovered, effective, props, probe, gate)))
    })();
    if matches!(&setup, Ok(None)) {
        return results;
    }
    let operations = match &setup {
        Ok(Some((_, effective, _, _, _))) => &effective.operations,
        _ => &settings.operations,
    };
    let mut table = None;
    let mut inventory = PassProbeInventory::default();
    let mut observed_token = match &setup {
        Ok(Some((discovered, _, _, _, _))) => discovered.log_token.clone(),
        _ => None,
    };
    for operation in Operation::ALL
        .into_iter()
        .filter(|operation| operations.contains(operation))
    {
        if force_abort.load(Ordering::SeqCst) {
            break;
        }
        let started = Instant::now();
        let mut optimize = None;
        let mut negative_probe = false;
        let mut probe_duration_ms = 0;
        let mut plan_apply_duration_ms = 0;
        let effective = match &setup {
            Ok(Some((_, effective, _, _, _))) => effective,
            _ => settings,
        };
        let outcome = match &setup {
            Ok(Some((discovered, effective, props, probe, _))) => (|| {
                let anchor = match effective.anchor_ms {
                    Some(anchor) => anchor,
                    None => discovery.maintenance_now_ms()?,
                };
                let control =
                    MaintenanceControl::new(Arc::clone(force_abort), Some(started + effective.operation_timeout))
                        .with_commit_retry_limits(effective.commit_attempts, effective.commit_budget)?;
                let probe_started = Instant::now();
                let probe_result = (|| -> Result<Option<MaintenanceReport>> {
                    let token = probe.probe_log_token(settings.operation_limits.discovery_max_log_files)?;
                    if observed_token.as_ref().is_some_and(|previous| *previous != token)
                        && !policy_matches(
                            settings,
                            &probe.maintenance_properties_with_limits(
                                settings.operation_limits.discovery_max_log_files,
                                settings.operation_limits.discovery_max_log_file_bytes,
                            )?,
                            props,
                        )?
                    {
                        bail!("table properties changed during the maintenance pass; retry discovery");
                    }
                    observed_token = Some(token);
                    let config = NegativeProbeConfig {
                        now_ms: anchor,
                        granularity: effective.granularity.context("effective partition granularity")?,
                        purge_retention_ms: effective.purge_retention_ms.context("effective purge retention")?,
                        vacuum_retention_ms: effective.vacuum_retention_ms,
                        log_retention_ms: effective
                            .log_retention_ms
                            .unwrap_or(arrow_deltalake::maintenance::DEFAULT_LOG_RETENTION_MS),
                        checkpoint_interval: effective.checkpoint_every_commits,
                        optimize_small_file_bytes: effective.optimize.small_file_bytes,
                        optimize_target_file_bytes: effective.optimize.target_file_bytes,
                        optimize_min_files: usize::try_from(props.unsigned("arrowcol.optimize.minFiles")?)?,
                        sealed_partitions_only: !effective.optimize.sealed_final,
                        max_listed_files: usize::try_from(props.unsigned("arrowcol.vacuum.maxListedFiles")?)?,
                    };
                    let probe_operation = match operation {
                        Operation::Purge => ProbeOperation::Purge,
                        Operation::Optimize => ProbeOperation::Optimize,
                        Operation::Vacuum => ProbeOperation::Vacuum,
                        Operation::Checkpoint => ProbeOperation::Checkpoint,
                        Operation::PruneLog => ProbeOperation::PruneLog,
                    };
                    let full_validation = (operation == Operation::Optimize && effective.optimize.sealed_final)
                        || (operation == Operation::Vacuum
                            && effective.vacuum_retention_ms < arrow_deltalake::maintenance::MIN_VACUUM_RETENTION_MS)
                        || (operation == Operation::PruneLog
                            && effective.log_retention_ms.is_some_and(|retention| {
                                retention < arrow_deltalake::maintenance::MIN_VACUUM_RETENTION_MS
                            }));
                    if full_validation {
                        return Ok(None);
                    }
                    let (report, stale) = negative_probe_result(probe.probe_no_work_with_inventory(
                        probe_operation,
                        &config,
                        &control,
                        &mut inventory,
                    ))?;
                    if stale {
                        eprintln!(
                            "delta-maintenance negative probe became stale; using full planner table={name} operation={}",
                            operation.label()
                        );
                    }
                    Ok(report)
                })();
                probe_duration_ms = u64::try_from(probe_started.elapsed().as_millis()).unwrap_or(u64::MAX);
                if let Some(report) = probe_result? {
                    negative_probe = true;
                    return Ok(report);
                }
                let planner_started = Instant::now();
                if table.is_none() {
                    table = Some(discovery.open_maintenance_table(&discovered.url)?);
                }
                inventory.invalidate();
                let report = run_operation(
                    effective,
                    table.as_ref().context("maintenance table")?,
                    OperationProperties {
                        table: &discovered.properties,
                        resolved: props,
                    },
                    operation,
                    anchor,
                    &control,
                    &mut optimize,
                );
                plan_apply_duration_ms = u64::try_from(planner_started.elapsed().as_millis()).unwrap_or(u64::MAX);
                report
            })(),
            Err(error) => Err(anyhow::anyhow!("discovery/configuration failed: {error:#}")),
            Ok(None) => unreachable!(),
        };
        let mut result = OperationResult {
            table: name.to_owned(),
            operation,
            applied: effective.apply,
            negative_probe,
            report: None,
            optimize,
            vacuum: (operation == Operation::Vacuum).then_some(VacuumEvidence {
                retention_ms: effective.vacuum_retention_ms,
                allow_short_vacuum_retention: effective.allow_short_vacuum_retention,
                unsafe_short_vacuum_for_tests: effective.unsafe_short_vacuum_for_tests,
            }),
            error_kind: None,
            error: None,
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            probe_duration_ms,
            plan_apply_duration_ms,
        };
        match outcome {
            Ok(report) => result.report = Some(report),
            Err(error) => {
                let details = match &setup {
                    Err(setup_error) => error_details(setup_error),
                    _ => error_details(&error),
                };
                result.error_kind = Some(details.map_or("failure", |details| details.kind.label()));
                result.report = error_report(&error).cloned();
                result.error = Some(format!("{error:#}"));
            }
        }
        emit(&result);
        let stop = result.error_kind == Some(MaintenanceErrorKind::Cancelled.label());
        results.push(result);
        if stop {
            break;
        }
    }
    if results.iter().any(|result| result.error.is_some())
        && let Some(schedule) = schedule
    {
        match schedule.lock() {
            Ok(mut schedule) => {
                schedule.remove(name);
            }
            Err(error) => eprintln!("maintenance retry schedule poisoned for {name}: {error}"),
        }
    }
    results
}

/// Run one maintenance cycle over every allowlisted table with bounded concurrency.
#[must_use]
pub fn run_cycle(settings: &Settings, metrics: &Metrics, cancel: &Arc<AtomicBool>) -> Vec<OperationResult> {
    let source = FilesystemDiscovery::new(settings.root.clone(), settings.tables.clone()).with_log_limits(
        settings.operation_limits.discovery_max_log_files,
        settings.operation_limits.discovery_max_log_file_bytes,
    );
    let force_abort = Arc::new(AtomicBool::new(false));
    match cycle_with_discovery(settings, &source, metrics, cancel, &force_abort, None) {
        Ok(results) => results,
        Err(error) => {
            let result = discovery_failure(settings, error);
            metrics.record(&result);
            vec![result]
        }
    }
}

/// Run one bounded pass over the catalog discovery adapter.
///
/// # Errors
/// Returns discovery-level failures; individual table failures are explicit results.
pub fn run_cycle_with_discovery(
    settings: &Settings,
    discovery: &dyn TableDiscovery,
    metrics: &Metrics,
    cancel: &Arc<AtomicBool>,
) -> Result<Vec<OperationResult>> {
    let force_abort = Arc::new(AtomicBool::new(false));
    cycle_with_discovery(settings, discovery, metrics, cancel, &force_abort, None)
}

fn discovery_failure(settings: &Settings, error: anyhow::Error) -> OperationResult {
    OperationResult {
        table: String::from("<discovery>"),
        operation: Operation::Optimize,
        applied: settings.apply,
        negative_probe: false,
        report: None,
        optimize: None,
        vacuum: None,
        error_kind: Some("failure"),
        error: Some(format!("{error:#}")),
        duration_ms: 0,
        probe_duration_ms: 0,
        plan_apply_duration_ms: 0,
    }
}

fn cycle_with_discovery(
    settings: &Settings,
    discovery: &dyn TableDiscovery,
    metrics: &Metrics,
    stop_admission: &Arc<AtomicBool>,
    force_abort: &Arc<AtomicBool>,
    schedule: Option<&Mutex<ScheduledTables>>,
) -> Result<Vec<OperationResult>> {
    if let Err(error) = metrics.invalidate_targets() {
        eprintln!("delta-maintenance target invalidation failed: {error:#}");
    }
    let names = discovery.tables()?;
    let mut target_names = names.clone();
    match discovery.pending_tables() {
        Ok(pending) => target_names.extend(pending),
        Err(error) => {
            eprintln!("delta-maintenance pending-target readback failed: {error:#}");
            target_names.extend(settings.tables.iter().cloned());
        }
    }
    target_names.sort();
    target_names.dedup();
    if target_names.len() > MAX_TABLES {
        bail!("target readback exceeds {MAX_TABLES} tables");
    }
    if let Err(error) = metrics.target_tables(&target_names, discovery) {
        eprintln!("delta-maintenance target inventory readback failed: {error:#}");
    }
    if settings.interval.is_none() && !settings.tables.is_empty() {
        let missing = settings
            .tables
            .iter()
            .filter(|requested| {
                names
                    .iter()
                    .filter(|name| *name == *requested || name.rsplit('.').next() == Some(requested.as_str()))
                    .count()
                    != 1
            })
            .map(String::as_str)
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            bail!("allowlisted tables were missing or ambiguous: {}", missing.join(", "));
        }
    }
    if names.len() > MAX_TABLES {
        bail!("catalog discovery exceeds {MAX_TABLES} tables");
    }
    if let Some(schedule) = schedule {
        let mut schedule = schedule
            .lock()
            .map_err(|_| anyhow::anyhow!("maintenance schedule poisoned"))?;
        schedule.retain(|name, _| names.contains(name));
    }
    let queue = Mutex::new(names.iter().cloned().collect::<VecDeque<_>>());
    let results = Mutex::new(Vec::new());
    let emit = |result: &OperationResult| {
        metrics.record(result);
        if let Ok(line) = serde_json::to_string(result) {
            println!("{line}");
        }
    };
    let workers = settings.max_concurrent_tables.min(names.len());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    if stop_admission.load(Ordering::SeqCst) || force_abort.load(Ordering::SeqCst) {
                        break;
                    }
                    let next = queue.lock().ok().and_then(|mut queue| queue.pop_front());
                    let Some(name) = next else { break };
                    if stop_admission.load(Ordering::SeqCst) || force_abort.load(Ordering::SeqCst) {
                        break;
                    }
                    let mut done = maintain_table(settings, discovery, &name, force_abort, &emit, schedule, metrics);
                    if let Ok(mut results) = results.lock() {
                        results.append(&mut done);
                    }
                }
            });
        }
    });
    metrics.heartbeat();
    results
        .into_inner()
        .map_err(|_| anyhow::anyhow!("maintenance results mutex poisoned"))
}

/// Run once or on a schedule until graceful stop or force-abort is requested.
/// Returns `false` for operation failures, force-abort, or bounded stale-plan starvation.
#[must_use]
pub fn run(settings: &Settings, metrics: &Metrics, stop_admission: &Arc<AtomicBool>) -> bool {
    let force_abort = Arc::new(AtomicBool::new(false));
    run_with_shutdown(settings, metrics, stop_admission, &force_abort)
}

/// Run filesystem discovery with separate graceful-stop and force-abort signals.
#[must_use]
pub fn run_with_shutdown(
    settings: &Settings,
    metrics: &Metrics,
    stop_admission: &Arc<AtomicBool>,
    force_abort: &Arc<AtomicBool>,
) -> bool {
    let source = FilesystemDiscovery::new(settings.root.clone(), settings.tables.clone()).with_log_limits(
        settings.operation_limits.discovery_max_log_files,
        settings.operation_limits.discovery_max_log_file_bytes,
    );
    run_with_discovery_shutdown(settings, &source, metrics, stop_admission, force_abort)
}

fn cycle_results_are_healthy(
    results: &[OperationResult],
    stale_plans: &mut BTreeMap<(String, &'static str), u32>,
    stale_plan_limit: u32,
) -> bool {
    let mut healthy = true;
    for result in results {
        let key = (result.table.clone(), result.operation.label());
        if result.error_kind.is_none() {
            stale_plans.remove(&key);
        } else if result.error_kind == Some(MaintenanceErrorKind::StalePlan.label()) {
            let consecutive = stale_plans.entry(key).or_default();
            *consecutive = consecutive.saturating_add(1);
            if *consecutive >= stale_plan_limit {
                healthy = false;
                eprintln!(
                    "delta-maintenance stale-plan starvation table={} operation={} consecutive={} limit={stale_plan_limit}",
                    result.table,
                    result.operation.label(),
                    consecutive,
                );
            }
        } else {
            healthy = false;
        }
    }
    healthy
}

/// Run the same bounded scheduler over catalog-client discovery.
#[must_use]
pub fn run_with_discovery(
    settings: &Settings,
    discovery: &dyn TableDiscovery,
    metrics: &Metrics,
    cancel: &Arc<AtomicBool>,
) -> bool {
    let force_abort = Arc::new(AtomicBool::new(false));
    run_with_discovery_shutdown(settings, discovery, metrics, cancel, &force_abort)
}

/// Run the bounded scheduler with distinct graceful-stop and force-abort signals.
#[must_use]
pub fn run_with_discovery_shutdown(
    settings: &Settings,
    discovery: &dyn TableDiscovery,
    metrics: &Metrics,
    stop_admission: &Arc<AtomicBool>,
    force_abort: &Arc<AtomicBool>,
) -> bool {
    let mut healthy = true;
    let mut stale_plans = BTreeMap::new();
    let schedule = Mutex::new(BTreeMap::new());
    let mut delay = settings.interval;
    let mut cycles = 0_u64;
    let mut previous_pending = Some(Vec::new());
    loop {
        let results = match cycle_with_discovery(
            settings,
            discovery,
            metrics,
            stop_admission,
            force_abort,
            Some(&schedule),
        ) {
            Ok(results) => results,
            Err(error) => {
                let result = discovery_failure(settings, error);
                metrics.record(&result);
                eprintln!(
                    "delta-maintenance discovery failed: {}",
                    result.error.as_deref().unwrap_or("unknown")
                );
                vec![result]
            }
        };
        cycles += 1;
        healthy &= cycle_results_are_healthy(
            &results,
            &mut stale_plans,
            if settings.interval.is_some() {
                settings.max_consecutive_stale_plans
            } else {
                1
            },
        );
        let pending_count = match discovery.pending_tables() {
            Ok(mut pending) => {
                pending.sort();
                pending.dedup();
                if previous_pending.as_ref() != Some(&pending) {
                    let names = if pending.is_empty() {
                        "<none>".to_owned()
                    } else {
                        pending.join(",")
                    };
                    eprintln!("delta-maintenance pending tables={names}");
                    previous_pending = Some(pending.clone());
                }
                metrics.pending_tables(pending.len());
                Some(pending.len())
            }
            Err(error) => {
                healthy = false;
                eprintln!("delta-maintenance pending-table status failed: {error:#}");
                None
            }
        };
        let Some(interval) = settings.interval else { break };
        if settings.max_cycles.is_some_and(|max| cycles >= max)
            || stop_admission.load(Ordering::SeqCst)
            || force_abort.load(Ordering::SeqCst)
        {
            break;
        }
        let idle =
            results.iter().all(|result| result.error_kind.is_none()) && !results.iter().any(OperationResult::did_work);
        let current = if idle { delay.unwrap_or(interval) } else { interval };
        eprintln!(
            "delta-maintenance heartbeat cycle={cycles} idle={idle} pending_tables={} next_in_ms={}",
            pending_count.map_or_else(|| "unknown".to_owned(), |count| count.to_string()),
            current.as_millis()
        );
        delay = Some(if idle {
            current.saturating_mul(2).min(settings.max_idle_backoff)
        } else {
            interval
        });
        let wake = Instant::now() + current;
        while Instant::now() < wake && !stop_admission.load(Ordering::SeqCst) && !force_abort.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(100).min(wake.saturating_duration_since(Instant::now())));
        }
        if stop_admission.load(Ordering::SeqCst) || force_abort.load(Ordering::SeqCst) {
            break;
        }
    }
    if force_abort.load(Ordering::SeqCst) {
        eprintln!("delta-maintenance force-aborted after shutdown request");
        false
    } else {
        healthy
    }
}

#[cfg(test)]
mod signal_cancellation_tests {
    use super::*;

    fn result_with_error_kind(error_kind: Option<&'static str>) -> OperationResult {
        OperationResult {
            table: "test_table".to_owned(),
            operation: Operation::Optimize,
            applied: false,
            negative_probe: false,
            report: None,
            optimize: None,
            vacuum: None,
            error_kind,
            error: error_kind.map(|_| "operation failed".to_owned()),
            duration_ms: 0,
            probe_duration_ms: 0,
            plan_apply_duration_ms: 0,
        }
    }

    #[test]
    fn cancellation_and_failures_are_unhealthy_while_stale_plans_retry() {
        let cancelled = result_with_error_kind(Some(MaintenanceErrorKind::Cancelled.label()));
        let mut stale_plans = BTreeMap::new();
        assert!(!cycle_results_are_healthy(
            std::slice::from_ref(&cancelled),
            &mut stale_plans,
            8
        ));
        assert!(!cycle_results_are_healthy(
            std::slice::from_ref(&cancelled),
            &mut stale_plans,
            8
        ));

        let failed = result_with_error_kind(Some("commit_budget_exhausted"));
        assert!(!cycle_results_are_healthy(
            std::slice::from_ref(&failed),
            &mut stale_plans,
            8
        ));
    }

    #[test]
    fn typed_stale_plans_retry_without_hiding_per_operation_starvation() {
        let stale = result_with_error_kind(Some(MaintenanceErrorKind::StalePlan.label()));
        let success = result_with_error_kind(None);
        let mut counts = BTreeMap::new();
        let observe =
            |results: &[OperationResult], counts: &mut BTreeMap<_, _>| cycle_results_are_healthy(results, counts, 3);
        assert!(observe(std::slice::from_ref(&stale), &mut counts));
        assert!(observe(std::slice::from_ref(&success), &mut counts));
        assert!(observe(std::slice::from_ref(&stale), &mut counts));
        let mut other = success.clone();
        other.operation = Operation::Vacuum;
        assert!(observe(&[stale.clone(), other.clone()], &mut counts));
        assert!(observe(&[], &mut counts));
        assert!(!observe(&[stale.clone(), other], &mut counts));
        assert!(!cycle_results_are_healthy(
            std::slice::from_ref(&stale),
            &mut BTreeMap::new(),
            1
        ));
        let mut untyped = result_with_error_kind(Some(MaintenanceErrorKind::ResourceLimit.label()));
        untyped.error = Some("stale_plan: plain text is not a typed retry".into());
        assert!(!observe(&[untyped], &mut counts));
        let metrics = Metrics::new();
        metrics.record(&stale);
        assert!(
            metrics
                .render()
                .contains("delta_maintenance_stale_plans_total{operation=\"optimize\"} 1")
        );
    }
}
