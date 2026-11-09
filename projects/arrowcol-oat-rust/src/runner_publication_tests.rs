mod publication_fault_tests {
    use super::*;
    use async_trait::async_trait;
    use futures_util::stream::BoxStream;
    use object_store::{
        CopyOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
        PutResult, memory::InMemory,
    };
    use std::collections::VecDeque;
    use std::fmt;
    use std::sync::Mutex;
    use tokio::sync::Notify;

    #[derive(Debug, Default)]
    struct ReadGate {
        captured: Notify,
        resume: Notify,
    }

    #[derive(Debug)]
    enum Fault {
        RivalWrite(Vec<u8>),
        Conflict,
        FailureBeforeCommit,
        FailureAfterCommit,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Attempt {
        Create,
        Update { e_tag: Option<String>, version: Option<String> },
        Overwrite,
    }

    #[derive(Debug)]
    struct ControlledStore {
        inner: InMemory,
        faults: Mutex<VecDeque<Fault>>,
        attempts: Mutex<Vec<Attempt>>,
        reads: Mutex<usize>,
        read_gate: Mutex<Option<Arc<ReadGate>>>,
    }

    impl fmt::Display for ControlledStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "synthetic-publication-fault-store")
        }
    }

    impl ControlledStore {
        fn new(faults: Vec<Fault>) -> Arc<Self> {
            Arc::new(Self {
                inner: InMemory::new(),
                faults: Mutex::new(faults.into()),
                attempts: Mutex::new(Vec::new()),
                reads: Mutex::new(0),
                read_gate: Mutex::new(None),
            })
        }

        fn attempts(&self) -> Vec<Attempt> {
            self.attempts.lock().expect("attempt mutex").clone()
        }

        fn reads(&self) -> usize {
            *self.reads.lock().expect("read mutex")
        }

        fn pause_next_read(&self) -> Arc<ReadGate> {
            let gate = Arc::new(ReadGate::default());
            *self.read_gate.lock().expect("gate mutex") = Some(gate.clone());
            gate
        }

        async fn seed(&self, location: &ObjectPath, bytes: Vec<u8>) -> Result<()> {
            self.inner.put_opts(location, PutPayload::from(bytes), PutOptions::default()).await?;
            Ok(())
        }

        async fn stored_rows(&self, location: &ObjectPath) -> Result<Vec<(String, String, u64)>> {
            let bytes = self.inner.get_opts(location, GetOptions::default()).await?.bytes().await?;
            decoded_rows(bytes.to_vec())
        }
    }

    fn injected_error(message: &'static str) -> object_store::Error {
        object_store::Error::Generic {
            store: "synthetic-publication-fault",
            source: std::io::Error::other(message).into(),
        }
    }

    #[async_trait]
    impl ObjectStore for ControlledStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            let attempt = match &options.mode {
                PutMode::Create => Attempt::Create,
                PutMode::Update(version) => Attempt::Update {
                    e_tag: version.e_tag.clone(),
                    version: version.version.clone(),
                },
                PutMode::Overwrite => Attempt::Overwrite,
            };
            self.attempts.lock().expect("attempt mutex").push(attempt);
            let fault = self.faults.lock().expect("fault mutex").pop_front();
            match fault {
                Some(Fault::RivalWrite(bytes)) => {
                    // The rival commits between the real helper's GET and conditional PUT.
                    self.inner.put_opts(location, PutPayload::from(bytes), PutOptions::default()).await?;
                }
                Some(Fault::Conflict) => {
                    return Err(object_store::Error::Precondition {
                        path: location.to_string(),
                        source: std::io::Error::other("synthetic conditional conflict").into(),
                    });
                }
                Some(Fault::FailureBeforeCommit) => return Err(injected_error("synthetic transport failure")),
                Some(Fault::FailureAfterCommit) => {
                    self.inner.put_opts(location, payload, options).await?;
                    return Err(injected_error("synthetic lost acknowledgement after commit"));
                }
                None => {}
            }
            // InMemory implements the actual create/update precondition check.
            self.inner.put_opts(location, payload, options).await
        }

        async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> object_store::Result<GetResult> {
            *self.reads.lock().expect("read mutex") += 1;
            let object = self.inner.get_opts(location, options).await;
            let gate = self.read_gate.lock().expect("gate mutex").take();
            if let Some(gate) = gate {
                gate.captured.notify_one();
                gate.resume.notified().await;
            }
            object
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn decoded_rows(bytes: Vec<u8>) -> Result<Vec<(String, String, u64)>> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        let mut output = Vec::new();
        for batch in reader {
            let batch = batch?;
            let runs = batch.column_by_name("run_id").context("run_id")?
                .as_any().downcast_ref::<StringArray>().context("UTF-8 run_id")?;
            let tables = batch.column_by_name("table").context("table")?
                .as_any().downcast_ref::<StringArray>().context("UTF-8 table")?;
            let rows = batch.column_by_name("rows").context("rows")?
                .as_any().downcast_ref::<UInt64Array>().context("uint64 rows")?;
            for index in 0..batch.num_rows() {
                assert!(!runs.is_null(index) && !tables.is_null(index) && !rows.is_null(index));
                output.push((runs.value(index).to_owned(), tables.value(index).to_owned(), rows.value(index)));
            }
        }
        output.sort();
        Ok(output)
    }

    fn batch(run: &str, table: &str, count: u64) -> Result<RecordBatch> {
        parquet_batch(&[sample_result(run, table, count)])
    }

    fn expected(run: &str, table: &str, count: u64) -> (String, String, u64) {
        (run.to_owned(), table.to_owned(), count)
    }

    #[tokio::test]
    async fn competing_create_rereads_and_preserves_the_rivals_actual_rows() -> Result<()> {
        let location = ObjectPath::from("synthetic/results.parquet");
        let rival = encode_parquet(&batch("rival", "sqlserver_rival", 41)?)?;
        let store = ControlledStore::new(vec![Fault::RivalWrite(rival)]);
        put_parquet(store.clone(), location.clone(), batch("ours", "sqlserver_ours", 19)?).await?;
        assert_eq!(store.stored_rows(&location).await?,
            vec![expected("ours", "sqlserver_ours", 19), expected("rival", "sqlserver_rival", 41)]);
        let attempts = store.attempts();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0], Attempt::Create);
        assert!(matches!(&attempts[1], Attempt::Update { e_tag: Some(_), .. }));
        assert_eq!(store.reads(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn stale_etag_update_rereads_rival_version_and_never_regresses_rows() -> Result<()> {
        let location = ObjectPath::from("synthetic/results.parquet");
        let original = batch("baseline", "sqlserver_baseline", 7)?;
        let rival = merge_parquet_bytes(&encode_parquet(&original)?, &batch("rival", "sqlserver_rival", 13)?)?;
        let store = ControlledStore::new(vec![Fault::RivalWrite(rival)]);
        store.seed(&location, encode_parquet(&original)?).await?;
        let original_version = store.inner.get_opts(&location, GetOptions::default()).await?.meta;
        put_parquet(store.clone(), location.clone(), batch("ours", "sqlserver_ours", 23)?).await?;
        assert_eq!(store.stored_rows(&location).await?, vec![
            expected("baseline", "sqlserver_baseline", 7),
            expected("ours", "sqlserver_ours", 23),
            expected("rival", "sqlserver_rival", 13),
        ]);
        let attempts = store.attempts();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0], Attempt::Update {
            e_tag: original_version.e_tag, version: original_version.version,
        });
        assert!(matches!(&attempts[1], Attempt::Update { e_tag: Some(_), .. }));
        assert_ne!(attempts[0], attempts[1], "Retry must use the newly observed rival version.");
        assert_eq!(store.reads(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn repeated_publication_deduplicates_exact_run_table_and_keeps_other_keys() -> Result<()> {
        let location = ObjectPath::from("synthetic/results.parquet");
        let store = ControlledStore::new(vec![]);
        let initial = parquet_batch(&[
            sample_result("same-run", "sqlserver_a", 1),
            sample_result("same-run", "sqlserver_b", 2),
            sample_result("other-run", "sqlserver_a", 3),
        ])?;
        put_parquet(store.clone(), location.clone(), initial).await?;
        let incoming = batch("same-run", "sqlserver_a", 99)?;
        put_parquet(store.clone(), location.clone(), incoming.clone()).await?;
        put_parquet(store.clone(), location.clone(), incoming).await?;
        assert_eq!(store.stored_rows(&location).await?, vec![
            expected("other-run", "sqlserver_a", 3),
            expected("same-run", "sqlserver_a", 99),
            expected("same-run", "sqlserver_b", 2),
        ]);
        let attempts = store.attempts();
        assert_eq!(attempts.len(), 3);
        assert_eq!(attempts[0], Attempt::Create);
        assert!(attempts[1..].iter().all(|mode| matches!(mode, Attempt::Update { e_tag: Some(_), .. })));
        Ok(())
    }

    #[tokio::test]
    async fn conflict_exhaustion_stops_at_eight_conditional_updates_and_preserves_destination() -> Result<()> {
        let location = ObjectPath::from("synthetic/results.parquet");
        let store = ControlledStore::new((0..8).map(|_| Fault::Conflict).collect());
        store.seed(&location, encode_parquet(&batch("rival", "sqlserver_rival", 31)?)?).await?;
        assert!(put_parquet(store.clone(), location.clone(), batch("ours", "sqlserver_ours", 17)?).await.is_err());
        assert_eq!(store.stored_rows(&location).await?, vec![expected("rival", "sqlserver_rival", 31)]);
        assert_eq!(store.reads(), 8);
        let attempts = store.attempts();
        assert_eq!(attempts.len(), 8);
        assert!(attempts.iter().all(|mode| matches!(mode, Attempt::Update { e_tag: Some(_), .. })));
        assert!(attempts.windows(2).all(|pair| pair[0] == pair[1]));
        Ok(())
    }

    #[tokio::test]
    async fn nonconflict_failure_before_commit_is_not_blindly_replayed() -> Result<()> {
        let location = ObjectPath::from("synthetic/results.parquet");
        let store = ControlledStore::new(vec![Fault::FailureBeforeCommit]);
        store.seed(&location, encode_parquet(&batch("rival", "sqlserver_rival", 43)?)?).await?;
        assert!(put_parquet(store.clone(), location.clone(), batch("ours", "sqlserver_ours", 29)?).await.is_err());
        assert_eq!(store.stored_rows(&location).await?, vec![expected("rival", "sqlserver_rival", 43)]);
        assert_eq!(store.reads(), 1);
        assert_eq!(store.attempts().len(), 1);
        assert!(matches!(&store.attempts()[0], Attempt::Update { e_tag: Some(_), .. }));
        Ok(())
    }

    #[tokio::test]
    async fn lost_acknowledgement_after_commit_returns_error_without_replay() -> Result<()> {
        let location = ObjectPath::from("synthetic/results.parquet");
        let store = ControlledStore::new(vec![Fault::FailureAfterCommit]);
        assert!(put_parquet(store.clone(), location.clone(), batch("ours", "sqlserver_ours", 59)?).await.is_err());
        assert_eq!(store.stored_rows(&location).await?, vec![expected("ours", "sqlserver_ours", 59)]);
        assert_eq!(store.reads(), 1);
        assert_eq!(store.attempts(), vec![Attempt::Create]);
        Ok(())
    }

    async fn mirror(
        destination: Arc<ControlledStore>,
        source: Arc<ControlledStore>,
        location: ObjectPath,
        snapshot: DestinationSnapshot,
    ) -> Result<bool> {
        let source_location = location.clone();
        write_cloud_exact_destination(destination, location, &snapshot,
            || read_cloud_destination_versioned(source, source_location)).await
    }

    async fn wait_for_capture(gate: &ReadGate) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), gate.captured.notified())
            .await.context("controlled read was not reached")?;
        Ok(())
    }

    #[tokio::test]
    async fn newer_cloud_source_and_mirror_overtake_before_stale_source_validation() -> Result<()> {
        let location = ObjectPath::from("synthetic/results.parquet");
        let source = ControlledStore::new(vec![]);
        let destination = ControlledStore::new(vec![]);
        let old = encode_parquet(&batch("old", "sqlserver_old", 11)?)?;
        source.seed(&location, old.clone()).await?;
        destination.seed(&location, old.clone()).await?;
        let stale = read_cloud_destination_versioned(source.clone(), location.clone()).await?;
        let gate = destination.pause_next_read();
        let caller = tokio::spawn(mirror(destination.clone(), source.clone(), location.clone(), stale));
        wait_for_capture(&gate).await?;
        // Destination version is captured before this newer authoritative publication.
        let new = merge_parquet_bytes(&old, &batch("new", "sqlserver_new", 37)?)?;
        source.seed(&location, new.clone()).await?;
        let fresh = read_cloud_destination_versioned(source.clone(), location.clone()).await?;
        assert!(mirror(destination.clone(), source.clone(), location.clone(), fresh).await?);
        gate.resume.notify_one();
        assert!(!caller.await??);
        assert_eq!(destination.stored_rows(&location).await?,
            vec![expected("new", "sqlserver_new", 37), expected("old", "sqlserver_old", 11)]);
        assert_eq!(destination.attempts().len(), 1, "Stale source must cause no PUT.");
        assert!(matches!(&destination.attempts()[0], Attempt::Update { e_tag: Some(_), .. }));
        assert_eq!(source.reads(), 4);
        assert_eq!(destination.reads(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn newer_mirror_after_stale_source_capture_is_protected_by_original_destination_etag() -> Result<()> {
        let location = ObjectPath::from("synthetic/results.parquet");
        let source = ControlledStore::new(vec![]);
        let destination = ControlledStore::new(vec![]);
        let old = encode_parquet(&batch("old", "sqlserver_old", 5)?)?;
        source.seed(&location, old.clone()).await?;
        destination.seed(&location, old.clone()).await?;
        let stale = read_cloud_destination_versioned(source.clone(), location.clone()).await?;
        let old_destination = destination.inner.get_opts(&location, GetOptions::default()).await?.meta;
        let gate = source.pause_next_read();
        let caller = tokio::spawn(mirror(destination.clone(), source.clone(), location.clone(), stale));
        wait_for_capture(&gate).await?;
        assert_eq!(destination.reads(), 1, "Destination version must precede fresh source GET.");
        let new = merge_parquet_bytes(&old, &batch("new", "sqlserver_new", 47)?)?;
        source.seed(&location, new).await?;
        let fresh = read_cloud_destination_versioned(source.clone(), location.clone()).await?;
        assert!(mirror(destination.clone(), source.clone(), location.clone(), fresh).await?);
        gate.resume.notify_one();
        assert!(!caller.await??, "Stale PUT must be a conditional conflict, not success.");
        assert_eq!(destination.stored_rows(&location).await?,
            vec![expected("new", "sqlserver_new", 47), expected("old", "sqlserver_old", 5)]);
        let attempts = destination.attempts();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[1], Attempt::Update { e_tag: old_destination.e_tag, version: old_destination.version });
        assert_eq!(destination.reads(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn competing_initial_mirror_create_cannot_overwrite_rival_parquet() -> Result<()> {
        let location = ObjectPath::from("synthetic/results.parquet");
        let source = ControlledStore::new(vec![]);
        let rival = encode_parquet(&batch("rival", "sqlserver_rival", 61)?)?;
        let destination = ControlledStore::new(vec![Fault::RivalWrite(rival)]);
        source.seed(&location, encode_parquet(&batch("ours", "sqlserver_ours", 71)?)?).await?;
        let snapshot = read_cloud_destination_versioned(source.clone(), location.clone()).await?;
        assert!(!mirror(destination.clone(), source, location.clone(), snapshot).await?);
        assert_eq!(destination.attempts(), vec![Attempt::Create]);
        assert_eq!(destination.stored_rows(&location).await?, vec![expected("rival", "sqlserver_rival", 61)]);
        Ok(())
    }

    #[tokio::test]
    async fn mirror_conflict_is_false_but_uncertain_and_nonconflict_errors_are_errors_without_replay() -> Result<()> {
        for fault in [Fault::Conflict, Fault::FailureBeforeCommit, Fault::FailureAfterCommit] {
            let conflict = matches!(&fault, Fault::Conflict);
            let committed = matches!(&fault, Fault::FailureAfterCommit);
            let location = ObjectPath::from("synthetic/results.parquet");
            let source = ControlledStore::new(vec![]);
            let destination = ControlledStore::new(vec![fault]);
            destination.seed(&location, encode_parquet(&batch("old", "sqlserver_old", 17)?)?).await?;
            source.seed(&location, encode_parquet(&batch("new", "sqlserver_new", 83)?)?).await?;
            let snapshot = read_cloud_destination_versioned(source.clone(), location.clone()).await?;
            let outcome = mirror(destination.clone(), source, location.clone(), snapshot).await;
            if conflict { assert!(!outcome?); } else { assert!(outcome.is_err()); }
            assert_eq!(destination.attempts().len(), 1);
            assert!(matches!(&destination.attempts()[0], Attempt::Update { e_tag: Some(_), .. }));
            assert_eq!(destination.stored_rows(&location).await?, if committed {
                vec![expected("new", "sqlserver_new", 83)]
            } else {
                vec![expected("old", "sqlserver_old", 17)]
            });
            assert_eq!(destination.reads(), 1);
        }
        Ok(())
    }
}
