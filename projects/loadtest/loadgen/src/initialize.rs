//! Fenced Azure table bootstrap using the same registry as the Flight fixtures.

use std::{
    collections::BTreeSet,
    env, fs,
    io::{Read, Write},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::{
    DeltaTable, FreshTableCommitPlan, FreshTableReceipt, FreshTableTransport, INGESTION_PARTITION, PublicationFence,
    PublicationFenceContext, PublicationFenceError,
};
use arrow_deltalake::publisher::{ControlMetadata, MutationRetries, PublisherSession, SessionMode, canonical_uuid};
use arrow_deltalake::qap_http::{StorageBridge, StorageBridgeAuthority};
use arrowcol_catalog::{DdlCommand, DdlParser};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use url::Url;

use crate::{
    cli::{InitializeAzureArgs, InitializeLocalArgs, InitializeOwnedRunTableArgs},
    schema::{SchemaStatement, schema_statements},
};

const INGESTION: &str = INGESTION_PARTITION;
const PLANNED_CONTROL_OPERATION_IDS: [&str; 13] = [
    "registration-read",
    "registration-acquire",
    "registration-compare-read",
    "registration-metadata-write",
    "registration-release",
    "open-publisher-check",
    "commit-authority-read",
    "terminal-read",
    "schema-commit-authority-read",
    "terminal-acquire",
    "terminal-compare-read",
    "terminal-metadata-write",
    "terminal-release",
];
const PLANNED_STORAGE_OPERATION_IDS: [&str; 5] = [
    "preflight-table-list",
    "preflight-log-list",
    "delta-v0-commit",
    "postwrite-table-list",
    "delta-v0-readback",
];

/// Strict input allocation for one fresh table, not permission to execute cloud operations.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedRunTablePlan {
    /// Exact selected ADLS account.
    pub account: String,
    /// Exact selected data container.
    pub container: String,
    /// Allocated campaign root, ending in `/campaigns/<campaign_id>/`.
    pub campaign_root: String,
    /// Exact run root: `<campaign_root>/runs/<run_id>/`.
    pub run_root: String,
    /// Canonical campaign identity.
    pub campaign_id: String,
    /// Canonical run identity.
    pub run_id: String,
    /// Captured normal-publisher reset generation.
    pub generation: String,
    /// Publisher identity included in the static effect envelope.
    pub publisher_id: String,
    /// Ordered registration and terminal control-container lease identities.
    pub control_lease_ids: Vec<String>,
    /// Ordered operation markers for every control request in admission, create and drain.
    pub control_operation_ids: Vec<String>,
    /// Ordered operation markers for preflight listing, the v0 commit and its actual readback.
    pub storage_operation_ids: Vec<String>,
    /// Frozen canonical identity; no arbitrary schema or table selection.
    pub table: String,
    /// Must select the existing ingestion partition.
    pub partition_by_ingestion_date: bool,
    /// Original publication owner; never replaced with an initializer owner.
    pub owner_id: String,
    /// Original campaign publication fencing token.
    pub fencing_token: u64,
    /// Captured exact version-zero Delta log body, required before envelope creation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_v0: Option<FreshTableCommitPlan>,
}

impl OwnedRunTablePlan {
    /// Validate the complete allocation before session admission or storage/authentication.
    ///
    /// # Errors
    /// Rejects unowned/noncanonical roots, missing identity/generation/authority or another table.
    pub fn validate(&self) -> Result<()> {
        self.validate_allocation()?;
        self.delta_v0
            .as_ref()
            .context("owned-run plan must capture exact Delta version-zero bytes before envelope creation")?
            .validate()
    }

    fn validate_allocation(&self) -> Result<()> {
        for value in [&self.campaign_id, &self.run_id, &self.generation, &self.owner_id] {
            canonical_uuid(value)?;
        }
        canonical_uuid_v4(&self.publisher_id)?;
        if self.control_lease_ids.len() != 2 {
            bail!("owned-run initialization requires exactly two ordered control lease identities");
        }
        let mut identities = BTreeSet::from([self.publisher_id.clone()]);
        for identity in &self.control_lease_ids {
            canonical_uuid_v4(identity)?;
            if !identities.insert(identity.clone()) {
                bail!("owned-run publisher and control lease identities must be distinct");
            }
        }
        if self
            .control_operation_ids
            .iter()
            .map(String::as_str)
            .ne(PLANNED_CONTROL_OPERATION_IDS)
        {
            bail!("owned-run plan must allocate the exact ordered control-operation markers before envelope creation");
        }
        if self
            .storage_operation_ids
            .iter()
            .map(String::as_str)
            .ne(PLANNED_STORAGE_OPERATION_IDS)
        {
            bail!(
                "owned-run plan must allocate the exact ordered object-store operation markers before envelope creation"
            );
        }
        if self.fencing_token == 0
            || !self.partition_by_ingestion_date
            || self.table != "telemetry.sqlserver.sqlserver_availability_replica_states"
        {
            bail!("owned-run initialization requires original authority, the frozen table and ingestion partition");
        }
        let campaign = checked_root(&self.campaign_root, &self.account, &self.container)?;
        let run = checked_root(&self.run_root, &self.account, &self.container)?;
        let suffix = format!("/campaigns/{}/", self.campaign_id);
        if !campaign.path().ends_with(&suffix)
            || self.campaign_root != campaign.as_str()
            || self.run_root != run.as_str()
            || campaign.path().contains("//")
            || campaign
                .path()
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte == b' ')
            || run != campaign.join(&format!("runs/{}/", self.run_id))?
        {
            bail!("owned-run root does not match the allocated campaign and run");
        }
        Ok(())
    }

    fn target(&self) -> Result<(Url, Arc<Schema>)> {
        self.validate()?;
        self.target_unchecked()
    }

    fn target_for_capture(&self) -> Result<(Url, Arc<Schema>)> {
        self.validate_allocation()?;
        self.target_unchecked()
    }

    fn target_unchecked(&self) -> Result<(Url, Arc<Schema>)> {
        let statement = schema_statements()?
            .into_iter()
            .find(|statement| statement.identity.to_string() == self.table)
            .context("frozen canonical table is absent")?;
        let (entry, schema) = canonical_entry(&statement)?;
        let root = checked_root(&self.run_root, &self.account, &self.container)?;
        Ok((root.join(&entry.relative_path)?, schema))
    }
}

fn canonical_uuid_v4(value: &str) -> Result<()> {
    let parsed = canonical_uuid(value)?;
    let bytes = parsed.as_bytes();
    if bytes[6] >> 4 != 4 || bytes[8] >> 6 != 2 {
        bail!("planned publisher and control lease identities must be UUID v4");
    }
    Ok(())
}

/// Successful setup readback; emitted only after the actual publisher cleanly drains.
#[derive(Debug, Serialize)]
pub struct OwnedRunTableReceipt {
    /// Original input binding, not fabricated provider evidence.
    pub allocation: OwnedRunTablePlan,
    /// Genuine Kernel schema-creation readback.
    pub delta: FreshTableReceipt,
    /// True only after `close_after_drain_async` returned success.
    pub publisher_drained: bool,
}

/// Original authority captured by the caller, checked again by its fence at schema commit.
pub struct OwnedRunTableAuthority {
    /// Original campaign.
    pub campaign_id: String,
    /// Original run.
    pub run_id: String,
    /// Original owner.
    pub owner_id: String,
    /// Original fencing token.
    pub fencing_token: u64,
    /// Guard executed at the real Kernel commit, not just during CLI validation.
    pub fence: Arc<dyn PublicationFence>,
}

/// Explicit parent-supplied transport binding; the deadline is never regenerated by the child.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedRunBridgeInput {
    pub authority: StorageBridgeAuthority,
    pub original_deadline_unix_ms: u64,
}

/// Unambiguous terminal frame, emitted only after real schema readback and normal drain.
#[derive(Debug, Serialize)]
pub struct OwnedRunBridgeReceipt {
    schema_version: u8,
    kind: &'static str,
    authority: StorageBridgeAuthority,
    original_deadline_unix_ms: u64,
    receipt: OwnedRunTableReceipt,
}

fn read_setup_input<T: serde::de::DeserializeOwned>(path: &Path, private: bool) -> Result<T> {
    const LIMIT: u64 = 64 * 1024;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > LIMIT {
        bail!("owned-run input must be a bounded regular non-symlink file");
    }
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("bridged owned-run input must be private to its owner");
        }
    }
    #[cfg(not(unix))]
    if private {
        bail!("bridged owned-run input requires Unix owner-private files");
    }
    let mut bytes = Vec::new();
    fs::File::open(path)?.take(LIMIT + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > LIMIT {
        bail!("owned-run input exceeds its byte limit");
    }
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(unix)]
fn bridge_socket(file: fs::File) -> Result<tokio::net::UnixStream> {
    use std::os::{fd::OwnedFd, unix::net::UnixStream};
    let socket = UnixStream::from(OwnedFd::from(file));
    socket.local_addr().context("bridge stdio requires local Unix IPC")?;
    socket.set_nonblocking(true)?;
    Ok(tokio::net::UnixStream::from_std(socket)?)
}

#[cfg(unix)]
fn bridge_reader() -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    use std::os::{fd::AsFd, unix::fs::FileTypeExt};
    let file = fs::File::from(std::io::stdin().as_fd().try_clone_to_owned()?);
    if file.metadata()?.file_type().is_socket() {
        Ok(Box::new(bridge_socket(file)?))
    } else {
        Ok(Box::new(
            tokio::net::unix::pipe::Receiver::from_file(file)
                .context("bridge stdin must be a Unix pipe or Unix socket")?,
        ))
    }
}

#[cfg(unix)]
fn bridge_writer() -> Result<Box<dyn AsyncWrite + Unpin + Send>> {
    use std::os::{fd::AsFd, unix::fs::FileTypeExt};
    let file = fs::File::from(std::io::stdout().as_fd().try_clone_to_owned()?);
    if file.metadata()?.file_type().is_socket() {
        Ok(Box::new(bridge_socket(file)?))
    } else {
        Ok(Box::new(
            tokio::net::unix::pipe::Sender::from_file(file)
                .context("bridge stdout must be a Unix pipe or Unix socket")?,
        ))
    }
}

/// Produce an immutable plan containing the exact fresh Kernel version-zero body.
/// This uses an isolated in-memory object store and never creates provider effects.
pub fn capture_owned_run_table_plan(mut plan: OwnedRunTablePlan) -> Result<OwnedRunTablePlan> {
    plan.validate_allocation()?;
    if plan.delta_v0.is_some() {
        bail!("plan capture input must not already contain a Delta version-zero body");
    }
    let (url, schema) = plan.target_for_capture()?;
    plan.delta_v0 = Some(DeltaTable::capture_fresh_table_commit(
        url,
        schema,
        vec![INGESTION.to_owned()],
    )?);
    plan.validate()?;
    Ok(plan)
}

pub fn plan_owned_run_table(args: crate::cli::PlanOwnedRunTableArgs) -> Result<()> {
    let plan: OwnedRunTablePlan = read_setup_input(&args.plan, false)?;
    let plan = capture_owned_run_table_plan(plan)?;
    let bytes = serde_json::to_vec(&plan)?;
    let mut output_options = fs::OpenOptions::new();
    output_options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        output_options.mode(0o600);
    }
    let mut output = output_options.open(args.output)?;
    output.write_all(&bytes)?;
    output.sync_all()?;
    Ok(())
}

/// Initialize only through explicit, validated dedicated StorageBridge stdio.
///
/// # Errors
/// Missing/invalid bridge inputs fail before authentication or any bridge request.
/// All registration, storage and drain effects require actual parent responses.
pub async fn initialize_owned_run_table(args: InitializeOwnedRunTableArgs) -> Result<()> {
    let plan: OwnedRunTablePlan = read_setup_input(&args.plan, args.storage_bridge_stdio)?;
    plan.validate()?;
    let Some(input) = args.storage_bridge_input.filter(|_| args.storage_bridge_stdio) else {
        bail!(
            "owned-run setup HOLD: per-operation auth/control/storage bridge is not wired for this invocation; nothing registered"
        );
    };
    let input: OwnedRunBridgeInput = read_setup_input(&input, true)?;
    #[cfg(unix)]
    {
        use std::time::{Duration, SystemTime, UNIX_EPOCH};
        use tokio::{io::AsyncWriteExt, time::Instant};

        let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        let remaining = input
            .original_deadline_unix_ms
            .checked_sub(now)
            .filter(|remaining| *remaining > 0)
            .context("owned-run original deadline expired")?;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(remaining))
            .context("owned-run original deadline is out of range")?;
        // Tokio stdio uses uncancellable blocking tasks; retain readiness-driven local IPC.
        let reader = bridge_reader()?;
        let writer = bridge_writer()?;
        let mut output = bridge_writer()?;
        let receipt = initialize_owned_run_table_bridged(plan, input, reader, writer).await?;
        let mut bytes = serde_json::to_vec(&receipt)?;
        bytes.push(b'\n');
        anyhow::ensure!(
            Instant::now() < deadline,
            "owned-run original deadline expired before terminal receipt"
        );
        tokio::time::timeout_at(deadline, async {
            output.write_all(&bytes).await?;
            output.flush().await
        })
        .await
        .context("owned-run terminal receipt exceeded original deadline")??;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (plan, input);
        bail!("bridged owned-run setup requires cancellable Unix pipes");
    }
}

struct BridgedSetupFence {
    session: Arc<PublisherSession>,
}

impl PublicationFence for BridgedSetupFence {
    fn check_before_commit(&self, context: &PublicationFenceContext<'_>) -> Result<(), PublicationFenceError> {
        if context.read_version() != 0 || context.proposed_version() != 0 || context.file_count() != 0 {
            return Err(PublicationFenceError::stale(
                "owned-run bridge permits fresh version0 schema only",
            ));
        }
        // This uncached authority check uses the same immutable parent binding as the log PUT.
        self.session.check().map_err(|error| {
            PublicationFenceError::check_failed(format!("bridged publisher authority check failed: {error:#}"))
        })
    }
}

/// Connect the reviewed bridge to the existing normal publisher and strict Kernel creator.
///
/// # Errors
/// Refuses plan/authority/root/deadline mismatches before session admission. No direct
/// credentials or network fallback exists; uncertain outcomes retain normal registration.
pub async fn initialize_owned_run_table_bridged<R, W>(
    plan: OwnedRunTablePlan,
    input: OwnedRunBridgeInput,
    reader: R,
    writer: W,
) -> Result<OwnedRunBridgeReceipt>
where
    R: AsyncRead + Send + 'static,
    W: AsyncWrite + Send + 'static,
{
    let (mut url, schema) = plan.target()?;
    url.set_path(&format!("{}/", url.path().trim_end_matches('/')));
    let authority = &input.authority;
    if authority.campaign_id != plan.campaign_id
        || authority.cell_run_id != plan.run_id
        || authority.owner_id != plan.owner_id
        || authority.fencing_token != plan.fencing_token
        || authority.storage_account != plan.account
        || authority.reset_generation != plan.generation
        || authority.table_root_url != url.as_str()
    {
        bail!("storage bridge authority differs from the original owned-run table allocation");
    }
    let bridge = StorageBridge::new_with_planned_object_operation_ids(
        input.authority.clone(),
        input.original_deadline_unix_ms,
        plan.storage_operation_ids.clone(),
        reader,
        writer,
    )?;
    let publisher_id = canonical_uuid(&plan.publisher_id)?;
    let control_lease_ids = [
        canonical_uuid(&plan.control_lease_ids[0])?,
        canonical_uuid(&plan.control_lease_ids[1])?,
    ];
    let session = PublisherSession::open_bridged_with_plan_async(
        &url,
        bridge,
        publisher_id,
        control_lease_ids,
        plan.control_operation_ids.clone(),
    )
    .await?;
    let data = session.store_for_url(&url).map(|store| FreshTableTransport {
        store,
        mutation_retries: MutationRetries::Disabled,
    });
    let fence = Arc::new(BridgedSetupFence {
        session: Arc::clone(&session),
    });
    let receipt = create_owned_run_table(plan, url, schema, session, data, fence).await?;
    Ok(OwnedRunBridgeReceipt {
        schema_version: 1,
        kind: "owned_run_table_receipt",
        authority: input.authority,
        original_deadline_unix_ms: input.original_deadline_unix_ms,
        receipt,
    })
}

/// Production Kernel/session body with explicit control, data and original publication authority.
/// Offline callers must label their transports as fixtures; no result proves live Azure coverage.
///
/// # Errors
/// Refuses invalid allocations before admission, stale/non-ready authority, nonempty targets,
/// conflicts and unknown outcomes. Drain failures never become successful setup receipts.
pub async fn initialize_owned_run_table_with(
    plan: OwnedRunTablePlan,
    sandbox_id: &str,
    control: Arc<dyn ControlMetadata>,
    data: FreshTableTransport,
    authority: OwnedRunTableAuthority,
) -> Result<OwnedRunTableReceipt> {
    let (url, schema) = plan.target()?;
    if authority.campaign_id != plan.campaign_id
        || authority.run_id != plan.run_id
        || authority.owner_id != plan.owner_id
        || authority.fencing_token != plan.fencing_token
    {
        bail!("owned-run table plan differs from the original publication authority");
    }
    let session = PublisherSession::open_with_control(
        control,
        sandbox_id,
        &plan.account,
        SessionMode::Publisher,
        Some(&plan.generation),
    )
    .await?;
    create_owned_run_table(plan, url, schema, session, Ok(data), authority.fence).await
}

async fn create_owned_run_table(
    plan: OwnedRunTablePlan,
    url: Url,
    schema: Arc<Schema>,
    session: Arc<PublisherSession>,
    data: Result<FreshTableTransport>,
    fence: Arc<dyn PublicationFence>,
) -> Result<OwnedRunTableReceipt> {
    let owned_session = Arc::clone(&session);
    let planned_commit = plan
        .delta_v0
        .clone()
        .context("owned-run plan lost its captured Delta version-zero body")?;
    let creation = match data {
        Ok(data) => {
            tokio::task::spawn_blocking(move || {
                DeltaTable::initialize_fresh_url_with_planned_session(
                    url,
                    schema,
                    vec![INGESTION.to_owned()],
                    owned_session,
                    fence,
                    data,
                    planned_commit,
                )
            })
            .await
        }
        Err(error) => Ok(Err(error)),
    };
    let drain = session.close_after_drain_async().await;
    let delta = match (creation, drain) {
        (Ok(Ok(delta)), Ok(())) => delta,
        (creation, drain) => bail!("owned-run setup did not complete: creation={creation:?}; drain={drain:?}"),
    };
    session.verify_planned_control_identities()?;
    Ok(OwnedRunTableReceipt {
        allocation: plan,
        delta,
        publisher_drained: true,
    })
}

#[derive(Debug, Serialize)]
struct ManifestEntry {
    name: String,
    relative_path: String,
}

fn checked_root(raw: &str, account: &str, container: &str) -> Result<Url> {
    let url = Url::parse(raw).context("invalid ARROWCOL_AZURE_TABLE_ROOT")?;
    if account.is_empty()
        || !account.bytes().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        || container.is_empty()
        || !container
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        || url.scheme() != "abfss"
        || url.username() != container
        || url.host_str() != Some(format!("{account}.dfs.core.windows.net").as_str())
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || raw.contains('%')
        || raw.contains('\\')
        || raw.split('/').any(|part| matches!(part, "." | ".."))
    {
        bail!("Azure bootstrap root must be a credential-free URL inside the configured account/container");
    }
    let mut url = url;
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

fn partitioned_schema(statement: &SchemaStatement) -> Result<Arc<Schema>> {
    let parser = DdlParser::default();
    let DdlCommand::CreateTable(create) = parser.parse(&statement.ddl)? else {
        bail!("canonical fixture contains a non-CREATE statement");
    };
    let mut fields = create
        .fields()
        .iter()
        .map(arrowcol_catalog::CatalogField::to_arrow)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    fields.push(Field::new(INGESTION, DataType::Utf8, false));
    Ok(Arc::new(Schema::new(fields)))
}

fn entries() -> Result<Vec<(ManifestEntry, Arc<Schema>)>> {
    schema_statements()?.iter().map(canonical_entry).collect()
}

fn canonical_entry(statement: &SchemaStatement) -> Result<(ManifestEntry, Arc<Schema>)> {
    let identity = &statement.identity;
    Ok((
        ManifestEntry {
            name: format!("dataflow_{}", identity.table()),
            relative_path: format!(
                "dataflow/{}/{}/{}",
                identity.catalog(),
                identity.schema(),
                identity.table()
            ),
        },
        partitioned_schema(statement)?,
    ))
}

/// Initialize only selected, fresh local table directories; existing tables are never opened.
pub(crate) fn initialize_fresh_local(root: &Path, selected: &[String], statements: &[SchemaStatement]) -> Result<()> {
    let requested = selected.iter().cloned().collect::<BTreeSet<_>>();
    if requested.is_empty() || requested.len() != selected.len() {
        bail!("partitioned local initialization requires explicit, unique --table selections");
    }
    let targets = statements
        .iter()
        .filter(|statement| requested.contains(&statement.identity.to_string()))
        .collect::<Vec<_>>();
    if targets.len() != requested.len() {
        bail!("partitioned local initialization contains an unknown table");
    }
    let root_metadata = fs::symlink_metadata(root).context("partitioned local Dataflow root must already exist")?;
    if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
        bail!("partitioned local Dataflow root must be a real directory");
    }
    let root = fs::canonicalize(root)?;
    let mut prepared = Vec::new();
    for statement in targets {
        let schema = partitioned_schema(statement)?;
        let mut parent = root.clone();
        for component in [
            statement.identity.catalog().as_str(),
            statement.identity.schema().as_str(),
        ] {
            parent.push(component);
            match fs::create_dir(&parent) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
            let metadata = fs::symlink_metadata(&parent)?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                bail!("local table parent must be a real directory: {}", parent.display());
            }
        }
        let path = parent.join(statement.identity.table().as_str());
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => bail!("refusing to initialize existing local table: {}", path.display()),
            Err(error) => return Err(error.into()),
        }
        prepared.push((path, schema));
    }
    for (path, schema) in prepared {
        fs::create_dir(&path).with_context(|| format!("claim fresh local table {}", path.display()))?;
        let url = Url::from_directory_path(&path).map_err(|()| anyhow::anyhow!("invalid local table directory"))?;
        DeltaTable::initialize_url_for_bootstrap(url, schema, vec![INGESTION.to_owned()])
            .with_context(|| format!("initialize fresh local table {}", path.display()))?;
    }
    Ok(())
}

/// Initialize exactly one fresh local table without Flight, catalog registration, or load generation.
///
/// # Errors
///
/// Requires the explicit partition flag, an absolute existing local root, and one known table whose
/// destination does not already exist.
pub fn initialize_local(args: InitializeLocalArgs) -> Result<()> {
    if !args.partition_by_ingestion_date {
        bail!("setup-only local initialization requires --partition-by-ingestion-date");
    }
    if !args.dataflow_root.is_absolute() {
        bail!("setup-only local initialization requires an absolute --dataflow-root");
    }
    initialize_fresh_local(&args.dataflow_root, &[args.table], &schema_statements()?)
}

/// Initializes existing-or-empty roots and publishes a manifest only after every table succeeds.
///
/// # Errors
///
/// Fails closed for missing configuration, unsafe roots, unfenced storage, incompatible existing
/// tables, or manifest write failures. Never emits storage URLs or credentials to stdout.
pub async fn initialize(args: InitializeAzureArgs) -> Result<()> {
    let config = InitializeAzureConfig::from_lookup(|key| env::var(key).ok())?;
    initialize_tables_with(
        args,
        &config.root,
        &config.account,
        &config.container,
        DeltaTable::initialize_url_for_bootstrap,
    )
    .await
}

/// Storage configuration [`initialize`] reads; production looks keys up in the process environment.
#[derive(Clone, Debug)]
pub struct InitializeAzureConfig {
    /// Canonical ADLS table root.
    pub root: String,
    /// Owned storage account.
    pub account: String,
    /// Owned data container.
    pub container: String,
}

impl InitializeAzureConfig {
    /// Reads every required initializer key, in order, from `lookup`.
    ///
    /// # Errors
    ///
    /// Fails with `missing <KEY>` for the first absent key; the checkpoint container is required
    /// even though only its presence is checked here.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let required = |key: &str| lookup(key).with_context(|| format!("missing {key}"));
        let root = required("ARROWCOL_AZURE_TABLE_ROOT")?;
        let account = required("AZURE_STORAGE_ACCOUNT")?;
        let container = required("AZURE_STORAGE_CONTAINER")?;
        required("AZURE_STORAGE_CHECKPOINT_CONTAINER")?;
        Ok(Self {
            root,
            account,
            container,
        })
    }
}

/// Shared body of [`initialize`]: validates the root, initializes every canonical table through
/// `bootstrap` in order, and writes the manifest only after all of them succeed. Production passes
/// [`DeltaTable::initialize_url_for_bootstrap`]; offline proofs pass the injected-control variant.
///
/// # Errors
///
/// Fails closed on an unsafe root before any table I/O, stops at the first failing table, and
/// writes no manifest unless every table succeeded.
pub async fn initialize_tables_with<F>(
    args: InitializeAzureArgs,
    root: &str,
    account: &str,
    container: &str,
    mut bootstrap: F,
) -> Result<()>
where
    F: FnMut(Url, Arc<Schema>, Vec<String>) -> Result<()>,
{
    let root = checked_root(root, account, container)?;
    let entries = entries()?;
    for (entry, schema) in &entries {
        let url = root
            .join(&entry.relative_path)
            .context("invalid canonical table path")?;
        bootstrap(url, Arc::clone(schema), vec![INGESTION.to_owned()])
            .with_context(|| format!("bootstrap canonical table {}", entry.name))?;
    }
    write_manifest(args).await
}

/// Writes canonical names/relative paths only, without contacting storage or claiming readiness.
///
/// # Errors
///
/// Returns fixture validation or manifest filesystem errors.
pub async fn write_manifest(args: InitializeAzureArgs) -> Result<()> {
    let manifest = entries()?.into_iter().map(|(entry, _)| entry).collect::<Vec<_>>();
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    tokio::fs::write(&args.manifest, bytes)
        .await
        .context("write Azure table manifest")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::TABLE_COUNT;
    use arrow_deltalake::maintenance::{Granularity, MaintenanceControl, Outcome, PurgeConfig, now_ms};

    #[test]
    fn fresh_local_partition_is_purgeable_and_existing_table_is_untouched() -> Result<()> {
        let root = tempfile::tempdir()?;
        let fixture = schema_statements()?;
        let table = "telemetry.sqlserver.sqlserver_availability_replica_states".to_owned();
        initialize_fresh_local(root.path(), std::slice::from_ref(&table), &fixture)?;
        let path = root
            .path()
            .join("telemetry/sqlserver/sqlserver_availability_replica_states");
        let log = path.join("_delta_log/00000000000000000000.json");
        let original = fs::read(&log)?;
        let actions = std::str::from_utf8(&original)?
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let metadata = actions
            .iter()
            .find_map(|action| action.get("metaData"))
            .context("missing metadata")?;
        assert_eq!(metadata["partitionColumns"], serde_json::json!([INGESTION]));
        let url = Url::from_directory_path(&path).map_err(|()| anyhow::anyhow!("invalid test directory"))?;
        let maintenance = DeltaTable::open_for_maintenance(&url)?;
        let version = maintenance.version()?;
        let config = PurgeConfig::new(86_400_000, now_ms()?, Granularity::Hour);
        let plan = maintenance.plan_purge(&config)?;
        assert!(plan.partitions.is_empty() && plan.watermark_ms.is_none());
        assert_eq!(
            maintenance
                .apply_purge(&plan, &config, &MaintenanceControl::default())?
                .outcome,
            Outcome::Noop
        );
        assert_eq!(maintenance.version()?, version);
        assert!(initialize_fresh_local(root.path(), &[table], &fixture).is_err());
        assert_eq!(fs::read(log)?, original);
        Ok(())
    }

    #[test]
    fn local_partition_requires_known_explicit_tables() -> Result<()> {
        let root = tempfile::tempdir()?;
        let fixture = schema_statements()?;
        assert!(initialize_fresh_local(root.path(), &[], &fixture).is_err());
        assert!(initialize_fresh_local(root.path(), &["unknown".to_owned()], &fixture).is_err());
        assert_eq!(fs::read_dir(root.path())?.count(), 0);
        Ok(())
    }

    #[test]
    fn actual_bootstrap_preserves_existing_logs_and_normalizes_timestamp_companions_once() -> Result<()> {
        let root = env::temp_dir().join(format!("loadgen-bootstrap-{}", uuid::Uuid::new_v4()));
        for (entry, schema) in entries()? {
            let path = root.join(entry.relative_path);
            std::fs::create_dir_all(&path)?;
            let url = Url::from_directory_path(&path).map_err(|()| anyhow::anyhow!("invalid test directory"))?;
            DeltaTable::initialize_url_for_bootstrap(url.clone(), Arc::clone(&schema), vec![INGESTION.to_owned()])?;
            let log = path.join("_delta_log/00000000000000000000.json");
            let original = std::fs::read_to_string(&log)?;
            DeltaTable::initialize_url_for_bootstrap(url, Arc::clone(&schema), vec![INGESTION.to_owned()])?;
            assert_eq!(std::fs::read_to_string(&log)?, original);
            assert!(!path.join("_delta_log/00000000000000000001.json").exists());
            let actions = original
                .lines()
                .map(serde_json::from_str::<serde_json::Value>)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let metadata = actions
                .iter()
                .find_map(|action| action.get("metaData"))
                .context("missing metadata")?;
            assert_eq!(metadata["partitionColumns"], serde_json::json!([INGESTION]));
            let stored: serde_json::Value =
                serde_json::from_str(metadata["schemaString"].as_str().context("missing schema")?)?;
            let fields = stored["fields"].as_array().context("missing fields")?;
            let timestamps = schema
                .fields()
                .iter()
                .filter(|field| matches!(field.data_type(), DataType::Timestamp(_, _)))
                .count();
            let companions = fields
                .iter()
                .filter(|field| {
                    field["name"]
                        .as_str()
                        .is_some_and(|name| name.ends_with("__unix_nanos"))
                })
                .count();
            assert_eq!(companions, timestamps);
            assert_eq!(fields.len(), schema.fields().len() + timestamps);
        }
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn dataflow_uses_raw_registry_and_required_partition() -> Result<()> {
        let entries = entries()?;
        assert_eq!(entries.len(), TABLE_COUNT);
        let names = entries
            .iter()
            .map(|(entry, _)| &entry.name)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names.len(), TABLE_COUNT);
        for (index, (entry, schema)) in entries.into_iter().enumerate() {
            assert!(entry.relative_path.starts_with("dataflow/telemetry/sqlserver/"));
            assert_eq!(schema.field(0).name(), "customer_id");
            let field = schema.field_with_name(INGESTION)?;
            assert!(!field.is_nullable());
            assert_eq!(field.data_type(), &DataType::Utf8);
            assert_eq!(schema.fields().len(), schema_statements()?[index].field_count + 1);
        }
        Ok(())
    }
    #[test]
    fn root_is_bounded_to_configured_account_and_container() -> Result<()> {
        let base = "abfss://tables@account.dfs.core.windows.net/sandbox";
        assert_eq!(checked_root(base, "account", "tables")?.path(), "/sandbox/");
        for raw in [
            "abfss://other@account.dfs.core.windows.net/sandbox",
            "abfss://tables@other.dfs.core.windows.net/sandbox",
            "abfss://tables@account.dfs.core.windows.net/a/../b",
            "abfss://tables@account.dfs.core.windows.net/%2e%2e",
            "abfss://tables@account.dfs.core.windows.net/sandbox?sig=secret",
            "file:///tmp/table",
        ] {
            assert!(checked_root(raw, "account", "tables").is_err());
        }
        Ok(())
    }
}
