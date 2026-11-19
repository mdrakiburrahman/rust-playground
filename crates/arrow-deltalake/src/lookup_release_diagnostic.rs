use super::*;
use std::io::{BufWriter, Write};
use std::time::Instant;

const HISTORY: u64 = 512;
const CHECKPOINT_TXNS: usize = 1_000_000;
const FUTURE: i64 = 9_000_000_000_000;
const TRIALS: usize = 6;

#[derive(Clone, Copy, Debug)]
enum LookupMode {
    Scalar,
    Batch,
}

fn resolve_diagnostic<'a>(
    mode: LookupMode,
    snapshot: &Snapshot,
    engine: &dyn Engine,
    ids: &[&'a str],
) -> Result<HashMap<&'a str, Option<i64>>> {
    match mode {
        LookupMode::Batch => Ok(get_app_id_versions(snapshot, engine, ids.iter().copied())?),
        LookupMode::Scalar => {
            let mut answer = HashMap::new();
            for &id in ids {
                if let std::collections::hash_map::Entry::Vacant(slot) = answer.entry(id) {
                    slot.insert(snapshot.get_app_id_version(id, engine)?);
                }
            }
            Ok(answer)
        }
    }
}

fn proc_cpu_ticks() -> Result<u64> {
    let stat = fs::read_to_string("/proc/self/stat")?;
    let (_, fields) = stat
        .rsplit_once(") ")
        .ok_or_else(|| anyhow::anyhow!("invalid proc stat"))?;
    let fields = fields.split_whitespace().collect::<Vec<_>>();
    // fields[0] is field 3 (state); user and system process CPU are fields 14 and 15.
    Ok(fields[11].parse::<u64>()? + fields[12].parse::<u64>()?)
}

fn proc_memory() -> Result<(u64, u64)> {
    fn kib(status: &str, name: &str) -> Result<u64> {
        let value = status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|line| line.split_whitespace().next())
            .ok_or_else(|| anyhow::anyhow!("missing {name}"))?;
        Ok(value.parse()?)
    }
    let status = fs::read_to_string("/proc/self/status")?;
    Ok((kib(&status, "VmRSS:")?, kib(&status, "VmHWM:")?))
}

fn requested_ids(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| {
            if index % 4 == 3 {
                format!("missing-{index:04}")
            } else {
                format!("app-{index:04}")
            }
        })
        .chain(["app-0000".to_owned(), "app-0001".to_owned()])
        .collect()
}

fn short_recent_tail(fixture: &Fixture, version: u64) -> Result<()> {
    // Updated after history/checkpoint: lower version and expired timestamp
    // must not resurrect the live version 1000.
    fixture.append(version, &[txn("app-0000", 1, Some(1)), txn("tail-noise", 23, None)])?;
    fixture.append(version + 1, &[txn("app-0001", 2, None)])?;
    Ok(())
}

fn diagnostic_checkpoint(fixture: &Fixture) -> Result<()> {
    // Synthetic V1 fixture with real Parquet; stream row groups so fixture build
    // memory is bounded independently of total action count.
    let schema = Arc::new(StructType::try_new(
        [&*LOG_PROTOCOL_SCHEMA, &*LOG_METADATA_SCHEMA, &*LOG_TXN_SCHEMA]
            .into_iter()
            .flat_map(|schema| schema.fields().cloned()),
    )?);
    let checkpoint = fixture.log.join(format!("{HISTORY:020}.checkpoint.parquet"));
    let decode = |rows: Vec<String>| -> Result<RecordBatch> {
        let strings = Arc::new(StringArray::from(rows)) as ArrayRef;
        let data = RecordBatch::try_from_iter([("json", strings)])?;
        Ok(fixture
            .engine
            .json_handler()
            .parse_json(Box::new(ArrowEngineData::new(data)), Arc::clone(&schema))?
            .try_into_record_batch()?)
    };
    let first = decode(vec![
        json!({"protocol": fixture.protocol}).to_string(),
        json!({"metaData": fixture.metadata}).to_string(),
    ])?;
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(16_384)
        .build();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(fs::File::create(&checkpoint)?, first.schema(), Some(properties))?;
    writer.write(&first)?;

    // Inject a protocol-shaped complete CRC that contains the same transaction
    // set as the checkpoint. Fixtures are not evidence of live CRC writes.
    let mut crc = BufWriter::new(fs::File::create(fixture.log.join(format!("{HISTORY:020}.crc")))?);
    write!(
        crc,
        r#"{{"tableSizeBytes":0,"numFiles":0,"numMetadata":1,"numProtocol":1,"metadata":{},"protocol":{},"setTransactions":["#,
        fixture.metadata, fixture.protocol
    )?;
    for start in (0..CHECKPOINT_TXNS).step_by(16_384) {
        let mut rows = Vec::new();
        for index in start..(start + 16_384).min(CHECKPOINT_TXNS) {
            let id = if index < 256 {
                format!("app-{index:04}")
            } else {
                // Stable high-cardinality synthetic publication IDs.
                format!(
                    "retained-{:016x}-{:08x}",
                    (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15),
                    index
                )
            };
            let action = txn(&id, 1000, Some(FUTURE));
            if index != 0 {
                write!(crc, ",")?;
            }
            serde_json::to_writer(&mut crc, &action["txn"])?;
            rows.push(action.to_string());
        }
        writer.write(&decode(rows)?)?;
    }
    writer.close()?;
    writeln!(crc, "]}}")?;
    crc.flush()?;
    fs::write(
        fixture.log.join("_last_checkpoint"),
        json!({"version": HISTORY, "size": CHECKPOINT_TXNS + 2}).to_string(),
    )?;
    println!(
        "fixture=stale_checkpoint,txn_rows={CHECKPOINT_TXNS},checkpoint_bytes={},crc_bytes={},tail_commits=2",
        fs::metadata(checkpoint)?.len(),
        fs::metadata(fixture.log.join(format!("{HISTORY:020}.crc")))?.len()
    );
    fs::remove_file(fixture.log.join("00000000000000000000.json"))?;
    short_recent_tail(fixture, HISTORY + 1)?;
    Ok(())
}

fn diagnostic_history(fixture: &Fixture) -> Result<Vec<Value>> {
    let mut latest = HashMap::new();
    for version in 1..=HISTORY {
        let mut actions = Vec::new();
        for offset in 0..24 {
            let id = format!("history-{version:04}-{offset:02}");
            let action = txn(&id, 1000, Some(FUTURE));
            latest.insert(id, action["txn"].clone());
            actions.push(action);
        }
        if version <= 256 {
            let id = format!("app-{:04}", version - 1);
            let action = txn(&id, 1000, Some(FUTURE));
            latest.insert(id, action["txn"].clone());
            actions.push(action);
        }
        fixture.append(version, &actions)?;
    }
    short_recent_tail(fixture, HISTORY + 1)?;
    let undated = txn("app-0001", 2, None);
    // CRC is the active transaction set at the current version. Omit the
    // expired latest action; do not keep the older live value.
    latest.remove("app-0000");
    latest.insert("app-0001".to_owned(), undated["txn"].clone());
    latest.insert("tail-noise".to_owned(), txn("tail-noise", 23, None)["txn"].clone());
    let mut latest = latest.into_iter().collect::<Vec<_>>();
    latest.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    Ok(latest.into_iter().map(|(_, action)| action).collect())
}

fn check_diagnostic_answers(actual: &HashMap<&str, Option<i64>>, ids: &[&str], unique_count: usize) {
    assert_eq!(actual.len(), unique_count);
    for &id in ids {
        let expected = match id {
            "app-0000" => None,
            "app-0001" => Some(2),
            other if other.starts_with("missing-") => None,
            _ => Some(1000),
        };
        assert_eq!(actual[id], expected, "lookup disagreed on {id}");
    }
}

struct DiagnosticParquet {
    inner: Arc<dyn ParquetHandler>,
    files: AtomicUsize,
    footers: AtomicUsize,
}

impl ParquetHandler for DiagnosticParquet {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        self.files.fetch_add(files.len(), Ordering::Relaxed);
        self.inner.read_parquet_files(files, schema, predicate)
    }

    fn write_parquet_file(
        &self,
        location: Url,
        data: delta_kernel::DeltaResultIteratorStatic<Box<dyn EngineData>>,
    ) -> DeltaResult<()> {
        self.inner.write_parquet_file(location, data)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> DeltaResult<delta_kernel::ParquetFooter> {
        self.footers.fetch_add(1, Ordering::Relaxed);
        self.inner.read_parquet_footer(file)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CrcCase {
    Absent,
    Current,
    Stale,
}

fn diagnostic_pairs(fixture: &Fixture, case: &str, crc: CrcCase, unique: usize) -> Result<()> {
    let ids = requested_ids(unique);
    let ids = ids.iter().map(String::as_str).collect::<Vec<_>>();
    // Warmup is on a separate snapshot. Each measured pair shares a new
    // snapshot; alternating order controls for effects of first lookup.
    for pair in 0..=TRIALS {
        let snapshot = Snapshot::builder_for(fixture.url.clone())
            .with_incremental_crc_replay(IncrementalReplay::Disabled)
            .build(&fixture.engine)?;
        assert_eq!(snapshot.version(), HISTORY + 2);
        match crc {
            CrcCase::Current => assert!(matches!(
                snapshot.crc_at_version().map(|crc| &crc.set_transaction_state),
                Some(SetTransactionState::Complete(_))
            )),
            _ => assert!(snapshot.crc_at_version().is_none()),
        }
        let order = if pair % 2 == 0 {
            [LookupMode::Scalar, LookupMode::Batch]
        } else {
            [LookupMode::Batch, LookupMode::Scalar]
        };
        let mut reference = None;
        for (position, mode) in order.into_iter().enumerate() {
            let parquet = Arc::new(DiagnosticParquet {
                inner: fixture.engine.parquet_handler(),
                files: AtomicUsize::new(0),
                footers: AtomicUsize::new(0),
            });
            let mut engine = CountingEngine::new(&fixture.engine);
            engine.parquet = Arc::clone(&parquet) as Arc<dyn ParquetHandler>;
            let iterations = if crc == CrcCase::Current { 2000 } else { 1 };
            let (rss_before, _) = proc_memory()?;
            let cpu_before = proc_cpu_ticks()?;
            let start = Instant::now();
            let mut answer = None;
            for _ in 0..iterations {
                answer = Some(std::hint::black_box(resolve_diagnostic(
                    mode, &snapshot, &engine, &ids,
                )?));
            }
            let elapsed = start.elapsed().as_nanos();
            let cpu_ticks = proc_cpu_ticks()? - cpu_before;
            let (rss_after, rss_peak) = proc_memory()?;
            let answer = answer.unwrap();
            check_diagnostic_answers(&answer, &ids, unique);
            if let Some(first) = &reference {
                assert_eq!(first, &answer);
            } else {
                reference = Some(answer);
            }
            let json = engine.files();
            let checkpoint = parquet.files.load(Ordering::Relaxed);
            let footers = parquet.footers.load(Ordering::Relaxed);
            println!(
                "sample,{case},{unique},{pair},{position},{mode:?},{iterations},{elapsed},{cpu_ticks},{rss_before},{rss_after},{rss_peak},{json},{checkpoint},{footers}"
            );
            match (crc, mode) {
                (CrcCase::Current, _) => assert_eq!((json, checkpoint, footers), (0, 0, 0)),
                (CrcCase::Stale, LookupMode::Scalar) => {
                    // Proven stale CRC root: authoritative checkpoint-only hit(s)
                    // despite no Parquet submissions, and precisely two tail entries per ID.
                    assert_eq!((json, checkpoint, footers), (2 * unique, 0, 0));
                }
                (CrcCase::Stale, LookupMode::Batch) => assert_eq!((json, checkpoint), (2, 1)),
                (CrcCase::Absent, LookupMode::Batch) => assert_eq!((json, checkpoint), (HISTORY as usize + 3, 0)),
                (CrcCase::Absent, LookupMode::Scalar) => {
                    assert_eq!((json, checkpoint), ((HISTORY as usize + 3) * unique, 0))
                }
            }
        }
    }
    Ok(())
}

#[test]
#[ignore = "release-only local disk diagnostic"]
fn release_lookup_diagnostic() -> Result<()> {
    assert!(!cfg!(debug_assertions), "run in release profile");
    println!(
        "columns,case,unique_ids,pair_0_warmup,position,mode,iterations,elapsed_ns,process_cpu_ticks,rss_before_kib,rss_after_kib,process_hwm_kib,json_entries,parquet_entries,footer_entries"
    );
    let history = Fixture::new(true)?;
    let current_transactions = diagnostic_history(&history)?;
    let txns = current_transactions.len();
    println!(
        "fixture=no_crc,transaction_commits={},current_active_transactions={txns}",
        HISTORY + 2
    );
    diagnostic_pairs(&history, "no_crc", CrcCase::Absent, 256)?;
    history.crc(HISTORY + 2, Some(&current_transactions))?;
    println!("fixture=current_crc,version={},transactions={txns}", HISTORY + 2);
    drop(current_transactions);
    diagnostic_pairs(&history, "current_crc", CrcCase::Current, 256)?;
    drop(history);

    let checkpoint = Fixture::new(true)?;
    diagnostic_checkpoint(&checkpoint)?;
    diagnostic_pairs(&checkpoint, "stale_short_tail_small", CrcCase::Stale, 8)?;
    diagnostic_pairs(&checkpoint, "stale_short_tail_bulk", CrcCase::Stale, 256)?;
    Ok(())
}
