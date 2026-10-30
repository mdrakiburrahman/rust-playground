//! Opt-in SQL-authorized, ordinary-Delta local publication.
//!
//! Collectors receive only a staging mount, never this role's root or SQL
//! credentials. SQL acceptance is the ownership linearization point. Native
//! Kernel commits on a private preparation table are NOT publication or ACK.
//! Only durable accepted bytes may be installed in the reader-visible table.

use anyhow::{Context, Result, bail, ensure};
use arrow::datatypes::SchemaRef;
use arrow_deltalake::delta::DeltaTable;
use arrowcol_sql_server::{ConnectionConfig, SqlConnection};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

const MAX_DATA: u64 = 8 * 1024 * 1024;
const MAX_ACTIONS: usize = 256 * 1024;
const MAX_HISTORY: i64 = 128;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Identity {
    pub controller: String,
    pub controller_generation: i64,
    pub membership: String,
    pub process: String,
    pub local_generation: i64,
    pub epoch: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataReference {
    pub name: String,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Proposal {
    pub table: String,
    pub operation: String,
    pub identity: Identity,
    pub predecessor: i64,
    pub version: i64,
    pub actions: String,
    pub data: Vec<DataReference>,
}

impl Proposal {
    /// # Errors
    /// Rejects oversized or invalid decision identity and action bytes.
    pub fn digest(&self) -> Result<String> {
        id(&self.table)?;
        id(&self.operation)?;
        ensure!(
            Some(self.version) == self.predecessor.checked_add(1) && self.version >= 0,
            "invalid predecessor"
        );
        ensure!(
            self.actions.len() <= MAX_ACTIONS && self.actions.ends_with('\n'),
            "action bound"
        );
        ensure!(self.data.len() <= 1, "data reference bound");
        Ok(hash(&serde_json::to_vec(self)?))
    }
}

fn id(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 100
            && value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
        "invalid identity"
    );
    Ok(())
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn mkdir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    sync_dir(path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

pub(crate) fn bounded_read(path: &Path, limit: u64) -> Result<Vec<u8>> {
    // Read one opened descriptor into private memory. Later staging changes
    // cannot change validated/committed bytes; metadata is not the size fence.
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    ensure!(file.metadata()?.is_file(), "input must be a regular file");
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= limit, "input byte bound");
    Ok(bytes)
}

/// Exact, no-overwrite, complete-file visibility on one local filesystem.
/// Private incomplete candidates are deliberately retained for crash evidence.
fn install(
    path: &Path,
    bytes: &[u8],
    scratch: &Path,
    during_write: &dyn Fn() -> Result<()>,
    before_link: &dyn Fn() -> Result<()>,
) -> Result<()> {
    let candidate = scratch.join(format!("{}.candidate", uuid::Uuid::new_v4()));
    let mut file = OpenOptions::new().write(true).create_new(true).open(&candidate)?;
    let middle = bytes.len() / 2;
    file.write_all(&bytes[..middle])?;
    during_write()?;
    file.write_all(&bytes[middle..])?;
    file.set_permissions(fs::Permissions::from_mode(0o444))?;
    file.sync_all()?;
    sync_dir(scratch)?;
    before_link()?;
    match fs::hard_link(&candidate, path) {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            ensure!(
                bounded_read(path, bytes.len() as u64)? == bytes,
                "conflicting/incomplete exact-version object"
            );
            File::open(path)?.sync_all()?;
        }
        Err(error) => return Err(error.into()),
    }
    sync_dir(path.parent().context("missing object parent")?)?;
    fs::remove_file(candidate)?;
    sync_dir(scratch)
}

/// Hooks are inert unless explicitly supplied by the gate harness.
pub trait PublicationHooks: Send + Sync {
    /// # Errors
    /// A fault hook may deliberately interrupt publication.
    fn at(&self, _phase: &str) -> Result<()> {
        Ok(())
    }
}

pub struct NoHooks;
impl PublicationHooks for NoHooks {}

/// This object belongs exclusively to the trusted publication process.
pub struct Publisher {
    root: PathBuf,
    sql: ConnectionConfig,
    authority: Option<std::sync::Arc<dyn crate::delta_store::Store>>,
}

impl Publisher {
    /// # Errors
    /// Returns private storage setup errors. The caller must enforce mount isolation.
    pub fn new(root: &Path, sql: ConnectionConfig) -> Result<Self> {
        mkdir(root)?;
        let root = fs::canonicalize(root)?;
        for part in ["tables", "work", "atomic"] {
            mkdir(&root.join(part))?;
        }
        Ok(Self {
            root,
            sql,
            authority: None,
        })
    }

    /// Opt-in runtime authority; legacy bounded gate SQL remains unchanged.
    pub fn with_authority(mut self, store: std::sync::Arc<dyn crate::delta_store::Store>) -> Self {
        self.authority = Some(store);
        self
    }

    async fn connect(&self) -> Result<SqlConnection> {
        Ok(SqlConnection::connect(&self.sql).await?)
    }

    /// # Errors
    /// Returns schema/bootstrap errors. Use only for a new task-owned database.
    pub async fn bootstrap(&self, table: &str, identity: &Identity) -> Result<()> {
        id(table)?;
        let mut sql = self.connect().await?;
        sql.simple_query(include_str!("sql_publication.sql"))
            .await?
            .into_results()
            .await?;
        let identity = serde_json::to_string(identity)?;
        sql.execute(
            "INSERT INTO publication_gate.owners(table_id, identity_json, active, expires)
             VALUES (@P1,@P2,1,DATEADD(second,3600,SYSUTCDATETIME()))",
            &[&table, &identity],
        )
        .await?;
        sql.finish().await?;
        mkdir(&self.table(table)?)?;
        Ok(())
    }

    fn table(&self, table: &str) -> Result<PathBuf> {
        id(table)?;
        Ok(self.root.join("tables").join(table))
    }

    /// The durable accepted prefix, including decisions without final receipts.
    ///
    /// # Errors
    /// Returns SQL/corrupt decision errors.
    pub async fn decisions(&self, table: &str) -> Result<Vec<Proposal>> {
        id(table)?;
        if let Some(store) = &self.authority {
            return Ok(store
                .snapshot()
                .await?
                .tables
                .get(table)
                .context("unplaced table")?
                .decisions
                .clone());
        }
        let mut sql = self.connect().await?;
        let rows = sql
            .query(
                "SELECT payload, digest FROM publication_gate.decisions WHERE table_id=@P1 ORDER BY version",
                &[&table],
            )
            .await?
            .into_first_result()
            .await?;
        sql.finish().await?;
        let mut proposals = Vec::new();
        for row in rows {
            let value: Proposal = serde_json::from_str(row.get::<&str, _>(0).context("missing payload")?)?;
            ensure!(
                value.table == table && value.version == proposals.len() as i64,
                "noncontiguous SQL decisions"
            );
            ensure!(
                Some(value.digest()?.as_str()) == row.get::<&str, _>(1),
                "decision digest mismatch"
            );
            proposals.push(value);
        }
        Ok(proposals)
    }

    /// Resolve an uncertain response by exact operation AND decision digest.
    ///
    /// # Errors
    /// Returns a collision or SQL error, never reinterprets unknown as rejected.
    pub async fn resolve(&self, proposal: &Proposal) -> Result<bool> {
        for accepted in self.decisions(&proposal.table).await? {
            if accepted.operation == proposal.operation {
                ensure!(accepted == *proposal, "operation identity collision");
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Generate genuine Kernel actions in a private table copied from the
    /// verified visible prefix. A schema-only proposal uses `stage = None`.
    ///
    /// # Errors
    /// Rejects missing prefix, incompatible schema, oversized or mutated staging.
    pub async fn prepare(
        &self,
        table: &str,
        operation: &str,
        identity: Identity,
        schema: SchemaRef,
        stage: Option<(&Path, &str)>,
    ) -> Result<Proposal> {
        id(operation)?;
        mkdir(&self.table(table)?)?;
        let history = self.decisions(table).await?;
        ensure!(history.len() < MAX_HISTORY as usize, "history backpressure");
        self.verify_prefix(table, &history)?;
        let predecessor = history.len() as i64 - 1;
        let work = self.root.join("work");
        ensure!(fs::read_dir(&work)?.count() < 128, "preparation workspace backpressure");
        let work = work.join(operation);
        fs::create_dir(&work)?;
        if !history.is_empty() {
            mkdir(&work.join("_delta_log"))?;
            for item in &history {
                install(
                    &work.join(log_name(item.version)),
                    item.actions.as_bytes(),
                    &self.root.join("atomic"),
                    &|| Ok(()),
                    &|| Ok(()),
                )?;
            }
        }
        let native = DeltaTable::open(&work)?;
        let mut references = Vec::new();
        if let Some((path, expected_hash)) = stage {
            ensure!(!history.is_empty(), "initialize schema first");
            let bytes = bounded_read(path, MAX_DATA)?;
            ensure!(hash(&bytes) == expected_hash, "staging digest changed");
            let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
            ensure!(builder.metadata().file_metadata().num_rows() <= 65536, "row bound");
            let decoded = builder.metadata().row_groups().iter().try_fold(0_i64, |total, group| {
                let size = group.total_byte_size();
                ensure!(size >= 0, "negative decoded size");
                total.checked_add(size).context("decoded size overflow")
            })?;
            ensure!((0..=32 * 1024 * 1024).contains(&decoded), "decoded data bound");
            ensure!(builder.schema().as_ref() == schema.as_ref(), "staged schema mismatch");
            let mut writer = native.new_file(schema)?;
            ensure!(
                native.version()? as i64 == predecessor,
                "schema changes require separate acceptance"
            );
            for batch in builder.with_batch_size(256).build()? {
                writer.write(&batch?).await?;
            }
            let result = writer.finish().await?;
            ensure!(result.version as i64 == predecessor + 1, "unexpected native version");
            let name = result
                .file
                .path
                .file_name()
                .context("data filename")?
                .to_str()
                .context("data UTF8")?
                .to_owned();
            let data = bounded_read(&result.file.path, MAX_DATA)?;
            let reference = DataReference {
                name: name.clone(),
                sha256: hash(&data),
                bytes: data.len() as u64,
            };
            install(
                &self.table(table)?.join(name),
                &data,
                &self.root.join("atomic"),
                &|| Ok(()),
                &|| Ok(()),
            )?;
            references.push(reference);
        } else {
            native.ensure_schema(schema)?;
            ensure!(
                native.version()? as i64 == predecessor + 1,
                "schema proposal must change exactly one version"
            );
        }
        let actions = bounded_read(&work.join(log_name(predecessor + 1)), MAX_ACTIONS as u64)?;
        let mut canonical = String::new();
        for line in std::str::from_utf8(&actions)?.lines() {
            let action: serde_json::Value = serde_json::from_str(line)?;
            canonical.push_str(&serde_json::to_string(&action)?);
            canonical.push('\n');
        }
        let proposal = Proposal {
            table: table.into(),
            operation: operation.into(),
            identity,
            predecessor,
            version: predecessor + 1,
            actions: canonical,
            data: references,
        };
        proposal.digest()?;
        self.verify_data(&proposal)?;
        Ok(proposal)
    }

    fn verify_data(&self, proposal: &Proposal) -> Result<()> {
        let mut added = Vec::new();
        for line in proposal.actions.lines() {
            let action: serde_json::Value = serde_json::from_str(line)?;
            ensure!(action.as_object().is_some_and(|a| a.len() == 1), "invalid action");
            let key = action
                .as_object()
                .context("action object")?
                .keys()
                .next()
                .context("empty action")?;
            ensure!(
                ["add", "metaData", "protocol", "commitInfo"].contains(&key.as_str()),
                "unsupported action"
            );
            if let Some(add) = action.get("add") {
                added.push((
                    add["path"].as_str().context("add path")?.to_owned(),
                    add["size"].as_u64().context("add size")?,
                ));
            }
        }
        ensure!(added.len() == proposal.data.len(), "unbound data action");
        for reference in &proposal.data {
            ensure!(
                reference.name.ends_with(".parquet")
                    && reference
                        .name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.'),
                "unsafe data name"
            );
            ensure!(
                added.contains(&(reference.name.clone(), reference.bytes)),
                "data action mismatch"
            );
            let bytes = bounded_read(&self.table(&proposal.table)?.join(&reference.name), MAX_DATA)?;
            ensure!(
                bytes.len() as u64 == reference.bytes && hash(&bytes) == reference.sha256,
                "immutable data mismatch"
            );
        }
        Ok(())
    }

    fn verify_prefix(&self, table: &str, history: &[Proposal]) -> Result<()> {
        let root = self.table(table)?;
        let log = root.join("_delta_log");
        if log.exists() {
            for entry in fs::read_dir(&log)? {
                let name = entry?.file_name().to_string_lossy().into_owned();
                ensure!(
                    history.iter().any(|p| format!("{:020}.json", p.version) == name),
                    "unratified Delta log object"
                );
            }
        }
        for proposal in history {
            self.verify_data(proposal)?;
            ensure!(
                bounded_read(&root.join(log_name(proposal.version)), MAX_ACTIONS as u64)?
                    == proposal.actions.as_bytes(),
                "prefix missing/corrupt"
            );
        }
        Ok(())
    }

    /// SQL acceptance is the fence. Expiry uses DB time, and transfer locks
    /// exactly the same row. The optional WAITFOR is a real lost-response probe.
    ///
    /// # Errors
    /// Returns stale identity/version, pressure, collision, or uncertain SQL I/O.
    pub async fn accept(&self, proposal: &Proposal, hooks: &dyn PublicationHooks, lose_reply: bool) -> Result<()> {
        let digest = proposal.digest()?;
        self.verify_data(proposal)?;
        let payload = serde_json::to_string(proposal)?;
        let identity = serde_json::to_string(&proposal.identity)?;
        hooks.at("before-sql")?;
        if let Some(store) = &self.authority {
            ensure!(!lose_reply, "runtime lost replies are transport faults");
            store
                .execute(crate::delta_store::Command::Accept(proposal.clone()))
                .await?;
            return hooks.at("after-sql");
        }
        let mut sql = self.connect().await?;
        let delay = if lose_reply { "WAITFOR DELAY '00:00:30';" } else { "" };
        let query = format!(
            "SET XACT_ABORT ON; BEGIN TRANSACTION;
             DECLARE @head bigint, @published bigint, @current nvarchar(2000), @active bit, @expires datetime2;
             SELECT @head=head,@published=published,@current=identity_json,@active=active,@expires=expires
             FROM publication_gate.owners WITH (UPDLOCK,HOLDLOCK) WHERE table_id=@P1;
             IF @head IS NULL THROW 51000,'unknown table',1;
             IF EXISTS(SELECT 1 FROM publication_gate.decisions WHERE table_id=@P1 AND operation_id=@P2)
             BEGIN
               IF NOT EXISTS(SELECT 1 FROM publication_gate.decisions WHERE table_id=@P1 AND operation_id=@P2
                 AND digest=@P3 AND payload=@P4) THROW 51000,'operation collision',1;
             END
             ELSE BEGIN
               IF @current <> @P5 OR @active <> 1 OR @expires <= SYSUTCDATETIME()
                 THROW 51000,'stale or expired writer identity',1;
               IF @head <> @P6 THROW 51000,'expected version mismatch',1;
               IF @head >= 127 OR @head-@published >= 16 THROW 51000,'decision backpressure',1;
               INSERT INTO publication_gate.decisions(table_id,version,operation_id,digest,payload)
                 VALUES(@P1,@P7,@P2,@P3,@P4);
               UPDATE publication_gate.owners SET head=@P7 WHERE table_id=@P1;
             END;
             COMMIT TRANSACTION; {delay} SELECT 1 AS accepted;"
        );
        sql.query(
            query,
            &[
                &proposal.table,
                &proposal.operation,
                &digest,
                &payload,
                &identity,
                &proposal.predecessor,
                &proposal.version,
            ],
        )
        .await?
        .into_results()
        .await?;
        sql.finish().await?;
        hooks.at("after-sql")
    }

    /// Cut over authority immediately; the successor remains non-admitting.
    ///
    /// # Errors
    /// Returns stale transition or SQL errors.
    pub async fn transfer(&self, table: &str, from: &Identity, to: &Identity) -> Result<()> {
        ensure!(
            Some(to.epoch) == from.epoch.checked_add(1),
            "transition epoch must advance"
        );
        let from = serde_json::to_string(from)?;
        let to = serde_json::to_string(to)?;
        let mut sql = self.connect().await?;
        sql.query(
            "SET XACT_ABORT ON; BEGIN TRANSACTION;
             DECLARE @current nvarchar(2000);
             SELECT @current=identity_json FROM publication_gate.owners WITH(UPDLOCK,HOLDLOCK) WHERE table_id=@P1;
             IF @current IS NULL OR @current<>@P2 THROW 51000,'stale transition',1;
             UPDATE publication_gate.owners SET identity_json=@P3,active=0,expires=DATEADD(second,3600,SYSUTCDATETIME())
               WHERE table_id=@P1;
             COMMIT TRANSACTION;",
            &[&table, &from, &to],
        )
        .await?
        .into_results()
        .await?;
        sql.finish().await?;
        Ok(())
    }

    /// Recover the entire accepted prefix, then persist final receipts.
    ///
    /// # Errors
    /// Rejects absent decisions, conflicting/partial logs, bad data and SQL errors.
    pub async fn materialize(&self, table: &str, hooks: &dyn PublicationHooks) -> Result<()> {
        crate::timing::measure("materialize-accepted-prefix", "lifecycle", async {
        let history = self.decisions(table).await?;
        ensure!(!history.is_empty(), "no accepted decisions");
        let root = self.table(table)?;
        mkdir(&root)?;
        mkdir(&root.join("_delta_log"))?;
        for proposal in &history {
            crate::timing::measure("accepted-version-materialize", "lifecycle", async {
            self.verify_data(proposal)?;
            crate::timing::measure("delta-log-install", "downstream_wait", async {
            install(
                &root.join(log_name(proposal.version)),
                proposal.actions.as_bytes(),
                &self.root.join("atomic"),
                &|| hooks.at(&format!("during-log:{}", proposal.version)),
                &|| hooks.at(&format!("before-log:{}", proposal.version)),
            )?;
            Ok::<_, anyhow::Error>(())
            }).await?;
            hooks.at(&format!("after-log:{}", proposal.version))?;
            hooks.at(&format!("before-receipt:{}", proposal.version))?;
            let digest = proposal.digest()?;
            if let Some(store) = &self.authority {
                crate::timing::measure("sql-final-receipt", "downstream_wait", crate::delta_backends::SqlStore {
                    connection: self.sql.clone(),
                }
                .final_receipt(proposal)).await?;
                store
                    .execute(crate::delta_store::Command::Final {
                        table: table.into(),
                        version: proposal.version,
                        digest,
                    })
                    .await?;
                hooks.at(&format!("after-receipt:{}", proposal.version))?;
                return Ok(());
            }
            let mut sql = self.connect().await?;
            sql.query(
                "SET XACT_ABORT ON; BEGIN TRANSACTION;
                 DECLARE @published bigint;
                 SELECT @published=published FROM publication_gate.owners WITH(UPDLOCK,HOLDLOCK) WHERE table_id=@P1;
                 IF @published < @P2-1 THROW 51000,'receipt prefix gap',1;
                 IF NOT EXISTS(SELECT 1 FROM publication_gate.decisions WHERE table_id=@P1 AND version=@P2 AND digest=@P3)
                   THROW 51000,'missing exact decision',1;
                 IF NOT EXISTS(SELECT 1 FROM publication_gate.receipts WHERE table_id=@P1 AND version=@P2)
                   INSERT INTO publication_gate.receipts(table_id,version,digest) VALUES(@P1,@P2,@P3);
                 UPDATE publication_gate.owners SET published=CASE WHEN published<@P2 THEN @P2 ELSE published END WHERE table_id=@P1;
                 COMMIT TRANSACTION;",
                &[&table, &proposal.version, &digest],
            ).await?.into_results().await?;
            sql.finish().await?;
            hooks.at(&format!("after-receipt:{}", proposal.version))?;
            Ok::<_, anyhow::Error>(())
            }).await?;
        }
        self.verify_prefix(table, &history)
        }).await
    }

    /// Activate only after standard native snapshot reconstruction and final
    /// receipt recovery. A competing transfer is checked again in SQL.
    ///
    /// # Errors
    /// Returns incomplete recovery, stale identity or native snapshot errors.
    pub async fn activate(&self, table: &str, successor: &Identity) -> Result<()> {
        let history = self.decisions(table).await?;
        self.verify_prefix(table, &history)?;
        // A controller can die after bootstrap but before accepting CREATE.
        // An empty, verified log has the empty prefix; the successor may create
        // the table only after the same SQL compare-and-activate below.
        let version = if history.is_empty() {
            -1
        } else {
            let snapshot = DeltaTable::open(self.table(table)?)?;
            i64::try_from(snapshot.version()?)?
        };
        ensure!(
            version == history.len() as i64 - 1,
            "snapshot does not include accepted prefix"
        );
        if let Some(store) = &self.authority {
            store
                .execute(crate::delta_store::Command::Activate {
                    table: table.into(),
                    identity: successor.clone(),
                    version,
                })
                .await?;
            return Ok(());
        }
        let identity = serde_json::to_string(successor)?;
        let mut sql = self.connect().await?;
        sql.query(
            "SET XACT_ABORT ON; BEGIN TRANSACTION;
             DECLARE @head bigint,@published bigint,@identity nvarchar(2000),@expires datetime2;
             SELECT @head=head,@published=published,@identity=identity_json,@expires=expires
               FROM publication_gate.owners WITH(UPDLOCK,HOLDLOCK) WHERE table_id=@P1;
             IF @head IS NULL OR @identity<>@P2 OR @head<>@P3 OR @published<>@head OR @expires<=SYSUTCDATETIME()
               THROW 51000,'successor waits for exact recovered prefix',1;
             UPDATE publication_gate.owners SET active=1 WHERE table_id=@P1;
             COMMIT TRANSACTION;",
            &[&table, &identity, &version],
        )
        .await?
        .into_results()
        .await?;
        sql.finish().await?;
        Ok(())
    }

    /// Final status is a projection of materialization, never of acceptance.
    ///
    /// # Errors
    /// Returns SQL errors.
    pub async fn status(&self, table: &str) -> Result<serde_json::Value> {
        if let Some(store) = &self.authority {
            let snapshot = store.snapshot().await?;
            let table = snapshot.tables.get(table).context("unknown table")?;
            return Ok(serde_json::json!({"head":table.decisions.len() as i64-1,
                "published":table.published, "identity":table.identity, "active":table.record.lifecycle == crate::Lifecycle::Active}));
        }
        let mut sql = self.connect().await?;
        let row = sql
            .query(
                "SELECT head,published,active,identity_json,
             (SELECT COUNT(*) FROM publication_gate.receipts r WHERE r.table_id=o.table_id) AS receipts
             FROM publication_gate.owners o WHERE table_id=@P1",
                &[&table],
            )
            .await?
            .into_row()
            .await?
            .context("unknown table")?;
        let result = serde_json::json!({
            "head": row.get::<i64,_>(0), "published": row.get::<i64,_>(1),
            "active": row.get::<bool,_>(2), "identity": row.get::<&str,_>(3),
            "receipts": row.get::<i32,_>(4),
        });
        sql.finish().await?;
        Ok(result)
    }
}

fn log_name(version: i64) -> PathBuf {
    PathBuf::from(format!("_delta_log/{version:020}.json"))
}

/// Collector-side bounded staging, with no authority or destination handle.
///
/// # Errors
/// Rejects excessive rows/bytes or Parquet/durability failures.
pub async fn stage_batch(path: &Path, batch: &arrow::array::RecordBatch) -> Result<String> {
    ensure!(batch.num_rows() <= 65536, "stage row bound");
    ensure!(
        arrow_deltalake::record_batch_logical_bytes(batch)? <= 32 * 1024 * 1024,
        "stage logical byte bound"
    );
    let mut writer = arrow_deltalake::ParquetFileWriter::create(path, batch.schema())?;
    writer.write(batch).await?;
    let file = writer.finish().await?;
    if file.bytes > MAX_DATA {
        bail!("stage encoded byte bound");
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o444))?;
    File::open(path)?.sync_all()?;
    Ok(hash(&bounded_read(path, MAX_DATA)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Result<PathBuf> {
        let root = PathBuf::from("target/publication-unit").join(uuid::Uuid::new_v4().to_string());
        mkdir(&root)?;
        Ok(root)
    }

    #[test]
    fn incomplete_private_candidate_never_appears_as_delta_log() -> Result<()> {
        let root = fixture()?;
        let path = root.join("00000000000000000000.json");
        let fail = || bail!("simulated writer interruption");
        assert!(install(&path, b"{\"complete\":true}\n", &root, &fail, &|| Ok(())).is_err());
        assert!(!path.exists());
        install(&path, b"{\"complete\":true}\n", &root, &|| Ok(()), &|| Ok(()))?;
        assert_eq!(fs::read(path)?, b"{\"complete\":true}\n");
        Ok(())
    }

    #[test]
    fn concurrent_workers_are_exact_content_idempotent() -> Result<()> {
        let root = fixture()?;
        let path = root.join("00000000000000000000.json");
        std::thread::scope(|scope| -> Result<()> {
            let mut handles = Vec::new();
            for _ in 0..4 {
                handles.push(scope.spawn(|| install(&path, b"accepted\n", &root, &|| Ok(()), &|| Ok(()))));
            }
            for handle in handles {
                handle.join().map_err(|_| anyhow::anyhow!("worker panic"))??;
            }
            Ok(())
        })?;
        assert!(install(&path, b"conflicting\n", &root, &|| Ok(()), &|| Ok(())).is_err());
        assert_eq!(fs::read(path)?, b"accepted\n");
        Ok(())
    }

    #[test]
    fn preexisting_partial_log_fails_closed_without_replacement() -> Result<()> {
        let root = fixture()?;
        let path = root.join("00000000000000000000.json");
        fs::write(&path, b"{")?;
        assert!(install(&path, b"{\"complete\":true}\n", &root, &|| Ok(()), &|| Ok(())).is_err());
        assert_eq!(fs::read(path)?, b"{");
        Ok(())
    }

    #[test]
    #[cfg(unix)]
    fn staged_reads_reject_symlinks_nonfiles_and_oversized_content() -> Result<()> {
        let root = fixture()?;
        let regular = root.join("stage.parquet");
        fs::write(&regular, b"bounded bytes")?;
        assert_eq!(bounded_read(&regular, 13)?, b"bounded bytes");
        assert!(bounded_read(&regular, 4).is_err());
        let link = root.join("stage-link.parquet");
        std::os::unix::fs::symlink(fs::canonicalize(&regular)?, &link)?;
        assert!(bounded_read(&link, 13).is_err());
        assert!(bounded_read(&root, 13).is_err());
        Ok(())
    }
}
