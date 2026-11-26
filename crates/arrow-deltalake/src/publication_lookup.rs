//! Bounded transaction lookup for one immutable snapshot, pinned to Kernel 0.29.0.
//!
//! At-version CRC hits (and complete-CRC misses) are authoritative. Unresolved
//! IDs share one Kernel-projected replay, including its checkpoint/sidecar handling.
//! Optional memoization is limited to the previous requested set; the caller refreshes and fences the snapshot.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use delta_kernel::actions::LOG_TXN_SCHEMA;
use delta_kernel::actions::visitors::SetTransactionVisitor;
use delta_kernel::crc::SetTransactionState;
use delta_kernel::engine_data::{GetData, TypedGetData};
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::DataType;
use delta_kernel::{DeltaResult, Engine, Error, RowVisitor, Snapshot};

/// Resolve deduplicated IDs against exactly `snapshot`, using O(requested IDs)
/// auxiliary state and at most one transaction replay.
///
/// The returned map includes every requested ID, including authoritative misses.
/// A fresh snapshot is required to observe an external append. Kernel's stale-CRC
/// accessor is private, so only its accessible at-version fast paths are reused.
///
/// # Errors
/// Returns Kernel replay/row errors and checked clock/retention conversion errors.
pub(crate) fn get_app_id_versions<'a>(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    application_ids: impl IntoIterator<Item = &'a str>,
) -> DeltaResult<HashMap<&'a str, Option<i64>>> {
    #[cfg(test)]
    LOOKUPS.with(|calls| calls.set(calls.get() + 1));
    let mut lookups = HashMap::new();
    for application_id in application_ids {
        lookups.entry(application_id).or_insert_with(Lookup::default);
    }
    if lookups.is_empty() {
        return Ok(HashMap::new());
    }
    let mut unresolved = lookups.len();

    if let Some(crc) = snapshot.crc_at_version() {
        let (transactions, complete) = match &crc.set_transaction_state {
            SetTransactionState::Complete(map) => (map, true),
            SetTransactionState::Partial(map) => (map, false),
        };
        for (id, lookup) in &mut lookups {
            if complete || transactions.contains_key(*id) {
                lookup.resolved = true;
                // CRC transaction fields are private even with internal-api.
                // This known-cache-only call preserves Kernel's retention rules at zero I/O.
                lookup.version = snapshot.get_app_id_version(id, engine)?;
                unresolved -= 1;
            }
        }
    }

    if unresolved != 0 {
        let expiration_timestamp = transaction_expiration_timestamp(snapshot)?;
        let mut visitor = RequestedTransactions {
            lookups: &mut lookups,
            unresolved,
            expiration_timestamp,
        };
        for batch in snapshot.log_segment().read_actions(engine, LOG_TXN_SCHEMA.clone())? {
            visitor.visit_rows_of(batch?.actions.as_ref())?;
            if visitor.unresolved == 0 {
                break;
            }
        }
    }
    Ok(lookups.into_iter().map(|(id, lookup)| (id, lookup.version)).collect())
}

#[derive(Default)]
struct Lookup {
    resolved: bool,
    version: Option<i64>,
}

struct RequestedTransactions<'a, 'ids> {
    lookups: &'a mut HashMap<&'ids str, Lookup>,
    unresolved: usize,
    expiration_timestamp: Option<i64>,
}

impl RowVisitor for RequestedTransactions<'_, '_> {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        SetTransactionVisitor::default().selected_column_names_and_types()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for row in 0..row_count {
            let Some(id) = getters[0].get_str(row, "txn.appId")? else {
                continue;
            };
            if let Some(lookup) = self.lookups.get_mut(id)
                && !lookup.resolved
            {
                let version: i64 = getters[1].get(row, "txn.version")?;
                let last_updated: Option<i64> = getters[2].get_opt(row, "txn.lastUpdated")?;
                // Kernel supplies reverse log order: resolve BEFORE retention, even
                // when the newest action has a smaller version or older timestamp.
                lookup.version = non_expired_version(version, last_updated, self.expiration_timestamp);
                lookup.resolved = true;
                self.unresolved -= 1;
            }
        }
        Ok(())
    }
}

// These helpers are private in pinned Kernel 0.29.0. Match its checked clock and
// duration conversions and its inclusive expiration boundary, once per lookup.
fn transaction_expiration_timestamp(snapshot: &Snapshot) -> DeltaResult<Option<i64>> {
    snapshot
        .table_properties()
        .set_transaction_retention_duration
        .map(|duration| {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| Error::generic(format!("System time before Unix epoch: {error}")))?;
            let now_ms = i64::try_from(now.as_millis())
                .map_err(|_| Error::generic("Current timestamp exceeds i64 millisecond range"))?;
            let retention_ms = i64::try_from(duration.as_millis())
                .map_err(|_| Error::generic("Retention duration exceeds i64 millisecond range"))?;
            Ok(now_ms - retention_ms)
        })
        .transpose()
}

fn non_expired_version(version: i64, last_updated: Option<i64>, expiration_timestamp: Option<i64>) -> Option<i64> {
    (!matches!(
        (expiration_timestamp, last_updated),
        (Some(expiration), Some(updated)) if updated <= expiration
    ))
    .then_some(version)
}

/// One bounded lookup working set and one authoritative successful commit receipt.
/// A receipt can explain a replay hit but is never used to decide a fence or skip.
#[derive(Default)]
pub(crate) struct PublicationCache {
    snapshot: std::sync::Weak<Snapshot>,
    versions: HashMap<String, Option<i64>>,
    receipt: Option<CommitReceipt>,
}

struct CommitReceipt {
    table: String,
    commit: u64,
    ids: HashMap<String, i64>,
}

impl PublicationCache {
    pub(crate) fn lookup<'a>(
        &mut self,
        snapshot: &delta_kernel::SnapshotRef,
        engine: &dyn Engine,
        ids: &[&'a str],
    ) -> DeltaResult<HashMap<&'a str, Option<i64>>> {
        // Arc identity additionally protects against same-version local log rewrites.
        // Only the normal snapshot refresh may certify reuse. Expiry is wall-clock
        // dependent even at the same log version, so never memoize expiring entries.
        let reusable = snapshot.table_properties().set_transaction_retention_duration.is_none();
        if !reusable || !self.snapshot.ptr_eq(&std::sync::Arc::downgrade(snapshot)) {
            self.versions.clear();
        }
        let missing = ids.iter().copied().filter(|id| !self.versions.contains_key(*id));
        let mut found = get_app_id_versions(snapshot, engine, missing)?;
        for &id in ids {
            if let Some(&version) = self.versions.get(id) {
                found.insert(id, version);
            }
        }
        if reusable {
            self.versions = found.iter().map(|(&id, &v)| (id.to_owned(), v)).collect();
            self.snapshot = std::sync::Arc::downgrade(snapshot);
        }
        Ok(found)
    }

    pub(crate) fn remember(&mut self, table: String, commit: u64, ids: impl Iterator<Item = (String, i64)>) {
        self.receipt = Some(CommitReceipt {
            table,
            commit,
            ids: ids.collect(),
        });
    }

    pub(crate) fn proven(&self, snapshot: &Snapshot, id: &str, version: i64) -> Option<u64> {
        let CommitReceipt { table, commit, ids } = self.receipt.as_ref()?;
        (table == snapshot.table_configuration().metadata().id()
            && *commit == snapshot.version()
            && ids.get(id) == Some(&version))
        .then_some(*commit)
    }
}

/// Every transaction of one snapshot lineage at `version`, so misses are authoritative.
/// Snapshots of one lineage extend one certified history; later snapshots only add the
/// commits after `version`, which are read directly instead of replaying the log.
pub(crate) struct TxnIndex {
    table: String,
    lineage: u64,
    version: u64,
    transactions: HashMap<Box<str>, i64>,
}

impl TxnIndex {
    pub(crate) fn lookup<'a>(
        slot: &mut Option<Self>,
        snapshot: &Snapshot,
        lineage: Option<u64>,
        engine: &dyn Engine,
        ids: &[&'a str],
    ) -> DeltaResult<HashMap<&'a str, Option<i64>>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        // Expiry is wall-clock dependent, so expiring tables always replay.
        let Some(lineage) =
            lineage.filter(|_| snapshot.table_properties().set_transaction_retention_duration.is_none())
        else {
            return get_app_id_versions(snapshot, engine, ids.iter().copied());
        };
        let table: &str = snapshot.table_configuration().metadata().id();
        let version = snapshot.version();
        let index = match slot {
            Some(index) if index.table == table && index.lineage == lineage => {
                if index.version > version {
                    return get_app_id_versions(snapshot, engine, ids.iter().copied());
                }
                if !index.advance(snapshot, engine)? {
                    *index = Self::seed(snapshot, engine, table, lineage)?;
                }
                index
            }
            _ => slot.insert(Self::seed(snapshot, engine, table, lineage)?),
        };
        Ok(ids
            .iter()
            .map(|&id| (id, index.transactions.get(id).copied()))
            .collect())
    }

    fn seed(snapshot: &Snapshot, engine: &dyn Engine, table: &str, lineage: u64) -> DeltaResult<Self> {
        let mut visitor = AllTransactions::default();
        for batch in snapshot.log_segment().read_actions(engine, LOG_TXN_SCHEMA.clone())? {
            visitor.visit_rows_of(batch?.actions.as_ref())?;
        }
        Ok(Self {
            table: table.to_owned(),
            lineage,
            version: snapshot.version(),
            transactions: visitor.transactions,
        })
    }

    /// Read only the published commits after the index; any other tail shape reseeds.
    fn advance(&mut self, snapshot: &Snapshot, engine: &dyn Engine) -> DeltaResult<bool> {
        let target = snapshot.version();
        if self.version == target {
            return Ok(true);
        }
        // Even our own last receipt is not proof that the newly listed file still
        // contains those transactions. Advance only from the certified snapshot.
        let commits = &snapshot.log_segment().listed.ascending_commit_files;
        let tail = &commits[commits.partition_point(|commit| commit.version <= self.version)..];
        let contiguous = tail.len() as u64 == target - self.version
            && tail.iter().zip(self.version + 1..).all(|(commit, version)| {
                commit.version == version && commit.file_type == delta_kernel::path::LogPathFileType::Commit
            });
        if !contiguous {
            return Ok(false);
        }
        let files = tail
            .iter()
            .rev()
            .map(|commit| commit.location.clone())
            .collect::<Vec<_>>();
        let mut visitor = AllTransactions::default();
        for batch in engine
            .json_handler()
            .read_json_files(&files, LOG_TXN_SCHEMA.clone(), None)?
        {
            visitor.visit_rows_of(batch?.as_ref())?;
        }
        self.transactions.extend(visitor.transactions);
        self.version = target;
        Ok(true)
    }
}

/// Newest-first transactions: callers supply reverse log order, so the first action wins.
#[derive(Default)]
struct AllTransactions {
    transactions: HashMap<Box<str>, i64>,
}

impl RowVisitor for AllTransactions {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        SetTransactionVisitor::default().selected_column_names_and_types()
    }

    fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
        for row in 0..row_count {
            let Some(id) = getters[0].get_str(row, "txn.appId")? else {
                continue;
            };
            if !self.transactions.contains_key(id) {
                let version: i64 = getters[1].get(row, "txn.version")?;
                self.transactions.insert(id.into(), version);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) use tests::LOOKUPS;

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::Result;
    use arrow::array::{ArrayRef, RecordBatch, StringArray};
    use delta_kernel::actions::{LOG_METADATA_SCHEMA, LOG_PROTOCOL_SCHEMA};
    use delta_kernel::engine::arrow_data::{ArrowEngineData, EngineDataArrowExt};
    use delta_kernel::expressions::PredicateRef;
    use delta_kernel::schema::{SchemaRef, StructType};
    use delta_kernel::snapshot::IncrementalReplay;
    use delta_kernel::{
        DeltaResultIterator, EngineData, EvaluationHandler, FileDataReadResultIterator, FileMeta, FileSize,
        FilteredEngineData, JsonHandler, ParquetHandler, StorageHandler,
    };
    use delta_kernel_default_engine::{DefaultEngine, DefaultEngineBuilder};
    use serde_json::{Value, json};
    use url::Url;

    thread_local! { pub(crate) static LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

    #[test]
    fn cache_invalidates_hits_and_misses_and_never_memoizes_expiring_state() -> Result<()> {
        for retention in [false, true] {
            let fixture = Fixture::new(retention)?;
            fixture.append(1, &[txn("old", 971, None)])?;
            let snapshot = fixture.snapshot()?;
            let engine = CountingEngine::new(&fixture.engine);
            let mut cache = PublicationCache::default();
            let ids = ["old", "new"];
            let first = cache.lookup(&snapshot, &engine, &ids)?;
            assert_eq!(first["old"], Some(971));
            assert_eq!(first["new"], None);
            let reads = engine.files();
            assert!(reads > 0);
            assert_eq!(first, cache.lookup(&snapshot, &engine, &ids)?);
            assert_eq!(engine.files() > reads, retention);
            fixture.append(2, &[txn("old", 972, None), txn("new", 544, None)])?;
            let advanced = fixture.snapshot()?;
            let changed = cache.lookup(&advanced, &engine, &ids)?;
            assert_eq!(changed["old"], Some(972));
            assert_eq!(changed["new"], Some(544));
        }
        Ok(())
    }

    type TestEngine = DefaultEngine<crate::delta_io::SharedExecutor>;

    struct Fixture {
        _directory: tempfile::TempDir,
        log: PathBuf,
        url: Url,
        engine: TestEngine,
        metadata: Value,
        protocol: Value,
    }

    impl Fixture {
        fn new(retention: bool) -> Result<Self> {
            let directory = tempfile::tempdir()?;
            let log = directory.path().join("_delta_log");
            fs::create_dir(&log)?;
            let url = Url::from_directory_path(directory.path()).map_err(|()| anyhow::anyhow!("table URL"))?;
            let configuration = if retention {
                json!({"delta.setTransactionRetentionDuration": "interval 1 day"})
            } else {
                json!({})
            };
            let metadata = json!({
                "id": uuid::Uuid::new_v4().to_string(),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": r#"{"type":"struct","fields":[{"name":"value","type":"long","nullable":true,"metadata":{}}]}"#,
                "partitionColumns": [], "configuration": configuration, "createdTime": 0
            });
            let protocol = json!({"minReaderVersion": 1, "minWriterVersion": 2});
            let engine = DefaultEngineBuilder::new(Arc::new(object_store::local::LocalFileSystem::new()))
                .with_task_executor(crate::delta_io::shared_executor()?)
                .build();
            let fixture = Self {
                _directory: directory,
                log,
                url,
                engine,
                metadata,
                protocol,
            };
            fixture.append(
                0,
                &[
                    json!({"protocol": fixture.protocol}),
                    json!({"metaData": fixture.metadata}),
                ],
            )?;
            Ok(fixture)
        }

        fn append(&self, version: u64, actions: &[Value]) -> Result<()> {
            let lines = actions.iter().map(Value::to_string).collect::<Vec<_>>().join("\n") + "\n";
            fs::write(self.log.join(format!("{version:020}.json")), lines)?;
            Ok(())
        }

        fn snapshot(&self) -> DeltaResult<Arc<Snapshot>> {
            Snapshot::builder_for(self.url.clone()).build(&self.engine)
        }

        fn crc(&self, version: u64, transactions: Option<&[Value]>) -> Result<()> {
            let mut crc = json!({
                "tableSizeBytes": 0, "numFiles": 0, "numMetadata": 1, "numProtocol": 1,
                "metadata": self.metadata, "protocol": self.protocol
            });
            if let Some(transactions) = transactions {
                crc["setTransactions"] = json!(transactions);
            }
            fs::write(self.log.join(format!("{version:020}.crc")), crc.to_string())?;
            Ok(())
        }
    }

    fn txn(id: &str, version: i64, updated: Option<i64>) -> Value {
        let mut transaction = json!({"appId": id, "version": version});
        if let Some(updated) = updated {
            transaction["lastUpdated"] = json!(updated);
        }
        json!({"txn": transaction})
    }

    fn assert_matches_kernel(snapshot: &Snapshot, engine: &dyn Engine, ids: &[&str]) -> Result<()> {
        let actual = get_app_id_versions(snapshot, engine, ids.iter().copied())?;
        for id in ids {
            assert_eq!(actual[id], snapshot.get_app_id_version(id, engine)?, "{id}");
        }
        Ok(())
    }

    struct CountingJson {
        inner: Arc<dyn JsonHandler>,
        files: AtomicUsize,
    }

    impl JsonHandler for CountingJson {
        fn parse_json(&self, data: Box<dyn EngineData>, schema: SchemaRef) -> DeltaResult<Box<dyn EngineData>> {
            self.inner.parse_json(data, schema)
        }

        fn read_json_files(
            &self,
            files: &[FileMeta],
            schema: SchemaRef,
            predicate: Option<PredicateRef>,
        ) -> DeltaResult<FileDataReadResultIterator> {
            self.files.fetch_add(files.len(), Ordering::Relaxed);
            self.inner.read_json_files(files, schema, predicate)
        }

        fn write_json_file(
            &self,
            path: &Url,
            data: DeltaResultIterator<'_, FilteredEngineData>,
            overwrite: bool,
        ) -> DeltaResult<FileSize> {
            self.inner.write_json_file(path, data, overwrite)
        }
    }

    struct CountingEngine {
        evaluation: Arc<dyn EvaluationHandler>,
        storage: Arc<dyn StorageHandler>,
        parquet: Arc<dyn ParquetHandler>,
        json: Arc<CountingJson>,
        handlers: AtomicUsize,
    }

    impl CountingEngine {
        fn new(inner: &dyn Engine) -> Self {
            Self {
                evaluation: inner.evaluation_handler(),
                storage: inner.storage_handler(),
                parquet: inner.parquet_handler(),
                json: Arc::new(CountingJson {
                    inner: inner.json_handler(),
                    files: AtomicUsize::new(0),
                }),
                handlers: AtomicUsize::new(0),
            }
        }

        fn files(&self) -> usize {
            self.json.files.load(Ordering::Relaxed)
        }
    }

    impl Engine for CountingEngine {
        fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
            self.handlers.fetch_add(1, Ordering::Relaxed);
            Arc::clone(&self.evaluation)
        }

        fn storage_handler(&self) -> Arc<dyn StorageHandler> {
            self.handlers.fetch_add(1, Ordering::Relaxed);
            Arc::clone(&self.storage)
        }

        fn json_handler(&self) -> Arc<dyn JsonHandler> {
            self.handlers.fetch_add(1, Ordering::Relaxed);
            Arc::clone(&self.json) as Arc<dyn JsonHandler>
        }

        fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
            self.handlers.fetch_add(1, Ordering::Relaxed);
            Arc::clone(&self.parquet)
        }
    }

    #[test]
    fn log_order_duplicates_and_external_append_match_kernel() -> Result<()> {
        let fixture = Fixture::new(false)?;
        fixture.append(1, &[txn("alpha", 99, Some(9_000_000_000_000)), txn("beta", 7, None)])?;
        fixture.append(2, &[txn("alpha", 2, Some(100))])?;
        let before = fixture.snapshot()?;
        let ids = ["alpha", "beta", "missing", "alpha"];
        assert_matches_kernel(&before, &fixture.engine, &ids)?;
        let versions = get_app_id_versions(&before, &fixture.engine, ids)?;
        assert_eq!(versions.len(), 3);
        assert_eq!(versions["alpha"], Some(2));
        assert_eq!(versions["beta"], Some(7));
        assert_eq!(versions["missing"], None);

        fixture.append(3, &[txn("alpha", 1, Some(10)), txn("external", 3, None)])?;
        let after = fixture.snapshot()?;
        assert_eq!(before.version(), 2);
        assert_eq!(after.version(), 3);
        assert_matches_kernel(&before, &fixture.engine, &["alpha", "external"])?;
        assert_matches_kernel(&after, &fixture.engine, &["alpha", "external"])?;
        assert_eq!(
            get_app_id_versions(&before, &fixture.engine, ["external"])?["external"],
            None
        );
        assert_eq!(
            get_app_id_versions(&after, &fixture.engine, ["external"])?["external"],
            Some(3)
        );
        Ok(())
    }

    #[test]
    fn transaction_index_reads_only_the_tail_and_never_answers_history_from_the_future() -> Result<()> {
        let fixture = Fixture::new(false)?;
        fixture.append(1, &[txn("first", 7, None)])?;
        let before = fixture.snapshot()?;
        let engine = CountingEngine::new(&fixture.engine);
        let ids = ["first", "foreign"];
        let mut index = None;
        let old = TxnIndex::lookup(&mut index, &before, Some(1), &engine, &ids)?;
        assert_eq!(old["first"], Some(7));
        assert_eq!(old["foreign"], None);
        fixture.append(2, &[txn("first", 9, None), txn("foreign", 4, None)])?;
        fixture.append(3, &[txn("first", 2, None)])?;
        let after = fixture.snapshot()?;
        let seed_reads = engine.files();
        let new = TxnIndex::lookup(&mut index, &after, Some(1), &engine, &ids)?;
        assert_eq!(new["first"], Some(2));
        assert_eq!(new["foreign"], Some(4));
        assert_eq!(engine.files() - seed_reads, 2);
        assert_eq!(TxnIndex::lookup(&mut index, &after, Some(1), &engine, &ids)?, new);
        assert_eq!(engine.files() - seed_reads, 2);
        assert_eq!(TxnIndex::lookup(&mut index, &before, Some(1), &engine, &ids)?, old);
        fixture.append(4, &[txn("first", 6, None), json!({"txn":{"appId":"broken"}})])?;
        let broken = fixture.snapshot()?;
        assert!(TxnIndex::lookup(&mut index, &broken, Some(1), &engine, &ids).is_err());
        assert_eq!(TxnIndex::lookup(&mut index, &after, Some(1), &engine, &ids)?, new);
        Ok(())
    }

    #[test]
    fn retention_never_resurrects_an_older_transaction() -> Result<()> {
        let fixture = Fixture::new(true)?;
        fixture.append(1, &[txn("expired", 99, Some(9_000_000_000_000))])?;
        fixture.append(
            2,
            &[
                txn("expired", 2, Some(1)),
                txn("undated", 3, None),
                txn("active", 4, Some(9_000_000_000_000)),
            ],
        )?;
        let snapshot = fixture.snapshot()?;
        let ids = ["expired", "undated", "active", "missing"];
        assert_matches_kernel(&snapshot, &fixture.engine, &ids)?;
        let versions = get_app_id_versions(&snapshot, &fixture.engine, ids)?;
        assert_eq!(versions["expired"], None);
        assert_eq!(versions["undated"], Some(3));
        assert_eq!(versions["active"], Some(4));
        Ok(())
    }

    #[test]
    fn checkpoint_backed_history_matches_kernel() -> Result<()> {
        let fixture = Fixture::new(false)?;
        let actions = [
            json!({"protocol": fixture.protocol}),
            json!({"metaData": fixture.metadata}),
            txn("alpha", 99, Some(999)),
            txn("checkpoint-only", 7, None),
        ];
        let schema = Arc::new(StructType::try_new(
            [&*LOG_PROTOCOL_SCHEMA, &*LOG_METADATA_SCHEMA, &*LOG_TXN_SCHEMA]
                .into_iter()
                .flat_map(|schema| schema.fields().cloned()),
        )?);
        let strings = Arc::new(StringArray::from_iter_values(actions.iter().map(Value::to_string))) as ArrayRef;
        let data = RecordBatch::try_from_iter([("json", strings)])?;
        let checkpoint = fixture
            .engine
            .json_handler()
            .parse_json(Box::new(ArrowEngineData::new(data)), schema)?
            .try_into_record_batch()?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            fs::File::create(fixture.log.join("00000000000000000001.checkpoint.parquet"))?,
            checkpoint.schema(),
            None,
        )?;
        writer.write(&checkpoint)?;
        writer.close()?;
        fs::write(fixture.log.join("_last_checkpoint"), r#"{"version":1,"size":4}"#)?;
        fs::remove_file(fixture.log.join("00000000000000000000.json"))?;
        fixture.append(2, &[txn("alpha", 2, Some(1))])?;
        let snapshot = fixture.snapshot()?;
        assert_matches_kernel(&snapshot, &fixture.engine, &["alpha", "checkpoint-only", "missing"])?;
        let versions = get_app_id_versions(&snapshot, &fixture.engine, ["alpha", "checkpoint-only"])?;
        assert_eq!(versions["alpha"], Some(2));
        assert_eq!(versions["checkpoint-only"], Some(7));
        Ok(())
    }

    #[test]
    fn authoritative_crc_hits_and_misses_do_not_replay() -> Result<()> {
        let fixture = Fixture::new(true)?;
        let transactions = [txn("present", 3, None), txn("expired", 4, Some(1))];
        fixture.append(1, &transactions)?;
        let crc_transactions = transactions
            .iter()
            .map(|value| value["txn"].clone())
            .collect::<Vec<_>>();
        fixture.crc(1, Some(&crc_transactions))?;
        let snapshot = fixture.snapshot()?;
        assert!(matches!(
            snapshot.crc_at_version().map(|crc| &crc.set_transaction_state),
            Some(SetTransactionState::Complete(_))
        ));
        let engine = CountingEngine::new(&fixture.engine);
        assert_matches_kernel(&snapshot, &engine, &["present", "expired", "missing", "present"])?;
        assert_eq!(engine.handlers.load(Ordering::Relaxed), 0);
        assert_eq!(engine.files(), 0);
        Ok(())
    }

    #[test]
    fn partial_crc_misses_replay_but_hits_do_not() -> Result<()> {
        let fixture = Fixture::new(false)?;
        fixture.append(1, &[txn("older", 1, None)])?;
        fixture.crc(1, None)?;
        fixture.append(2, &[txn("known", 2, None)])?;
        let snapshot = Snapshot::builder_for(fixture.url.clone())
            .with_incremental_crc_replay(IncrementalReplay::UpToCommits(1))
            .build(&fixture.engine)?;
        assert!(matches!(
            snapshot.crc_at_version().map(|crc| &crc.set_transaction_state),
            Some(SetTransactionState::Partial(map)) if map.contains_key("known") && !map.contains_key("older")
        ));
        let engine = CountingEngine::new(&fixture.engine);
        assert_matches_kernel(&snapshot, &engine, &["known"])?;
        assert_eq!(engine.handlers.load(Ordering::Relaxed), 0);
        assert_matches_kernel(&snapshot, &engine, &["known", "older", "missing"])?;
        assert!(engine.files() > 0);
        assert_eq!(get_app_id_versions(&snapshot, &engine, ["older"])?["older"], Some(1));
        Ok(())
    }

    #[test]
    fn lookup_io_is_not_multiplied_by_requested_id_count() -> Result<()> {
        let fixture = Fixture::new(false)?;
        let ids = (0..64).map(|id| format!("app-{id}")).collect::<Vec<_>>();
        for version in 1..=16 {
            let actions = ids.iter().map(|id| txn(id, version, None)).collect::<Vec<_>>();
            fixture.append(u64::try_from(version)?, &actions)?;
        }
        let snapshot = fixture.snapshot()?;
        let single = CountingEngine::new(&fixture.engine);
        assert_eq!(get_app_id_versions(&snapshot, &single, ["missing"])?["missing"], None);
        let batched = CountingEngine::new(&fixture.engine);
        let requests = ids.iter().map(String::as_str).chain(["missing", "app-0"]);
        let versions = get_app_id_versions(&snapshot, &batched, requests)?;
        assert_eq!(versions.len(), 65);
        assert!(ids.iter().all(|id| versions[id.as_str()] == Some(16)));
        assert_eq!(batched.files(), single.files());
        assert!(batched.files() > 0);
        let repeated = CountingEngine::new(&fixture.engine);
        for id in ids.iter().map(String::as_str).chain(["missing"]) {
            assert_eq!(snapshot.get_app_id_version(id, &repeated)?, versions[id]);
        }
        assert!(repeated.files() > batched.files() * 32);
        eprintln!(
            "lookup IO: 65 unique IDs + duplicate; single missing={}, batch={}, repeated={}",
            single.files(),
            batched.files(),
            repeated.files()
        );
        Ok(())
    }

    #[test]
    fn empty_input_does_no_work_and_replay_errors_are_not_misses() -> Result<()> {
        let fixture = Fixture::new(false)?;
        fixture.append(1, &[txn("present", 1, None)])?;
        let snapshot = fixture.snapshot()?;
        fs::remove_file(fixture.log.join("00000000000000000001.json"))?;
        let engine = CountingEngine::new(&fixture.engine);
        assert!(get_app_id_versions(&snapshot, &engine, [])?.is_empty());
        assert_eq!(engine.handlers.load(Ordering::Relaxed), 0);
        assert!(get_app_id_versions(&snapshot, &engine, ["missing"]).is_err());
        assert!(snapshot.get_app_id_version("missing", &engine).is_err());

        fixture.append(1, &[json!({"txn": {"appId": "broken"}})])?;
        let snapshot = fixture.snapshot()?;
        let batch_error = get_app_id_versions(&snapshot, &engine, ["broken"]).err();
        let kernel_error = snapshot.get_app_id_version("broken", &engine).err();
        assert!(batch_error.is_some());
        assert_eq!(
            batch_error.map(|error| error.to_string()),
            kernel_error.map(|error| error.to_string())
        );
        Ok(())
    }
}
