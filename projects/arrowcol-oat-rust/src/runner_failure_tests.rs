#[cfg(test)]
mod failure_evidence_tests {
    use super::*;

    #[test]
    fn ownership_transport_failure_is_not_a_confirmed_pinning_violation() {
        let report = failed_client_pinning_report(
            &anyhow::anyhow!("DMV source connection failed"),
            Err(anyhow::anyhow!("metrics transport unavailable")),
            &["sqlserver_wait_stats".to_owned()],
            PinningReport::default(),
        );
        assert!(
            report.violation.is_none(),
            "an unavailable observation is not evidence that ownership changed"
        );
        assert!(
            report
                .verification_error
                .as_deref()
                .is_some_and(|error| error.contains("metrics transport"))
        );
    }

    #[test]
    fn failure_measurements_have_a_nullable_parquet_representation() -> Result<()> {
        let bytes = encode_parquet(&parquet_batch(&[])?)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
        for field in ["rows", "duration_ms", "rows_per_second"] {
            assert!(
                reader.schema().field_with_name(field)?.is_nullable(),
                "unknown {field} must not require a fabricated zero"
            );
        }
        Ok(())
    }

    fn config() -> Result<OatConfig> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.example.json"
        ))?;
        config.tables = vec!["sqlserver_wait_stats".to_owned()];
        config.replica_counts = vec![1, 2, 3];
        config.dfe_worker_concurrency = vec![2];
        Ok(config)
    }

    fn completed(config: &OatConfig, run_id: &str, replicas: u32, rows: u64) -> Result<Vec<OatResult>> {
        result_rows(
            config,
            "orchestration",
            run_id,
            replicas,
            2,
            CellMeasurements {
                clients: ClientRun {
                    metrics: HashMap::from([(
                        config.tables[0].clone(),
                        ClientMetrics {
                            rows,
                            duration_ms: 60_000,
                            rows_per_second: rows as f64 / 60.0,
                        },
                    )]),
                    pinning: PinningReport {
                        owners: HashMap::from([(config.tables[0].clone(), "collector-a".to_owned())]),
                        ..Default::default()
                    },
                    qap_tables: Vec::new(),
                    failure: None,
                },
                started_at_ms: 1_794_173_400_000,
                qap: QapMetrics::default(),
                resources: None,
            },
        )
    }

    fn read_batch(bytes: Vec<u8>) -> Result<RecordBatch> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
        let schema = Arc::clone(reader.schema());
        let batches = reader.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(concat_batches(&schema, &batches)?)
    }

    #[test]
    fn unmeasured_failure_is_null_but_measured_zero_remains_zero() -> Result<()> {
        let config = config()?;
        let failure = finish_matrix_cell(
            &config,
            "orchestration",
            "failed",
            1,
            2,
            Err(anyhow::anyhow!("source disconnected")),
            Ok(()),
        );
        assert!(failure.error.is_some());
        assert_eq!(failure.rows[0].status, "failed");
        assert_eq!(failure.rows[0].rows, None);
        let mut rows = completed(&config, "measured-zero", 1, 0)?;
        rows.extend(failure.rows);
        let batch = read_batch(encode_parquet(&parquet_batch(&rows)?)?)?;
        for field in ["rows", "duration_ms", "rows_per_second"] {
            let column = batch.column_by_name(field).context("measurement column")?;
            assert!(column.is_valid(0), "completed {field} remains measured");
            assert!(column.is_null(1), "unmeasured {field} is null");
        }
        let measured = batch
            .column_by_name("rows")
            .context("rows")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("uint64")?;
        assert_eq!(measured.value(0), 0);
        assert!(rows[1].tunables_json.contains("source disconnected"));
        Ok(())
    }

    #[test]
    fn observation_failures_keep_measurements_without_claiming_an_ownership_violation() -> Result<()> {
        let config = config()?;
        let mut pinning = PinningReport::default();
        let healthy = HashMap::from([(config.tables[0].clone(), vec!["collector-a".to_owned()])]);
        record_owner_observation(&mut pinning, Ok(healthy.clone()), &config.tables, false);
        record_owner_observation(
            &mut pinning,
            Err(anyhow::anyhow!("HTTP timeout")),
            &config.tables,
            false,
        );
        record_owner_observation(&mut pinning, Ok(healthy), &config.tables, true);
        assert!(pinning.violation.is_none());
        let rows = result_rows(
            &config,
            "orchestration",
            "unknown",
            1,
            2,
            CellMeasurements {
                clients: ClientRun {
                    metrics: HashMap::from([(
                        config.tables[0].clone(),
                        ClientMetrics {
                            rows: 60,
                            duration_ms: 60_000,
                            rows_per_second: 1.0,
                        },
                    )]),
                    pinning,
                    qap_tables: Vec::new(),
                    failure: None,
                },
                started_at_ms: 0,
                qap: QapMetrics::default(),
                resources: None,
            },
        )?;
        assert_eq!(rows[0].status, "failed");
        assert!(!rows[0].pinning_ok);
        assert!(rows[0].pinning_error.is_none());
        assert_eq!(rows[0].rows, Some(60));
        assert!(rows[0].tunables_json.contains("HTTP timeout"));
        assert!(
            finish_matrix_cell(&config, "orchestration", "unknown", 1, 2, Ok(rows), Ok(()))
                .error
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn client_failure_resampling_retains_earlier_ownership_and_verification_evidence() -> Result<()> {
        let config = config()?;
        let tables = &config.tables;
        let mut report = PinningReport::default();
        record_owner_observation(
            &mut report,
            Ok(HashMap::from([(tables[0].clone(), vec!["collector-a".to_owned()])])),
            tables,
            false,
        );
        record_owner_observation(
            &mut report,
            Ok(HashMap::from([(tables[0].clone(), vec!["collector-b".to_owned()])])),
            tables,
            false,
        );
        let original = report.violation.clone().context("owner change")?;
        let report = failed_client_pinning_report(
            &anyhow::anyhow!("source disconnected"),
            Err(anyhow::anyhow!("final observation unavailable")),
            tables,
            report,
        );
        assert_eq!(report.violation.as_deref(), Some(original.as_str()));
        assert!(
            report
                .verification_error
                .as_deref()
                .is_some_and(|error| error.contains("final observation unavailable"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn later_cell_and_cleanup_failures_publish_prior_rows_and_keep_both_errors() -> Result<()> {
        for cleanup_only in [false, true] {
            let directory = std::env::temp_dir().join(format!("oat-failed-matrix-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&directory)?;
            let mut config = config()?;
            config.result_url = Url::from_file_path(directory.join("results.parquet"))
                .map_err(|()| anyhow::anyhow!("result URL"))?
                .to_string();
            config.website_url = Url::from_file_path(directory.join("website.parquet"))
                .map_err(|()| anyhow::anyhow!("website URL"))?
                .to_string();
            let mut calls = 0;
            let outcome = run_matrix_with(
                &config,
                "orchestration",
                &AppState::default(),
                |cell, id, replicas, concurrency| {
                    calls += 1;
                    let result = if replicas == 1 || cleanup_only {
                        completed(&cell, &id, replicas, 100)
                    } else {
                        Err(anyhow::anyhow!("second cell failed before completion"))
                    };
                    let cleanup = if replicas == 2 {
                        Err(anyhow::anyhow!("cleanup custody unresolved"))
                    } else {
                        Ok(())
                    };
                    std::future::ready(finish_matrix_cell(
                        &cell,
                        "orchestration",
                        &id,
                        replicas,
                        concurrency,
                        result,
                        cleanup,
                    ))
                },
            )
            .await;
            assert_eq!(calls, 2, "do not run a third cell after failure");
            assert_eq!(outcome.rows.len(), 2);
            assert_eq!(outcome.rows[0].status, "succeeded");
            assert_eq!(outcome.rows[1].status, "failed");
            assert_eq!(outcome.rows[1].rows, cleanup_only.then_some(100));
            let failed_run = outcome.rows[1].run_id.clone();
            assert_ne!(outcome.rows[0].run_id, failed_run);
            let error = publish_matrix_outcome(&config, outcome)
                .await
                .expect_err("overall failure remains fatal");
            let detail = format!("{error:#}");
            assert!(detail.contains("cleanup custody unresolved"));
            if !cleanup_only {
                assert!(detail.contains("second cell failed"));
            }
            let bytes = fs::read(directory.join("results.parquet"))?;
            assert_eq!(bytes, fs::read(directory.join("website.parquet"))?);
            let batch = read_batch(bytes)?;
            assert_eq!(batch.num_rows(), 2);
            let run_ids = batch
                .column_by_name("run_id")
                .context("run ID")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("string")?;
            assert_eq!(run_ids.value(1), failed_run);
            let tunables = batch
                .column_by_name("tunables_json")
                .context("tunables")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("string")?;
            assert!(tunables.value(1).contains("cleanup custody unresolved"));
            for name in [
                "results.parquet",
                "website.parquet",
                "results.parquet.lock",
                "website.parquet.lock",
            ] {
                fs::remove_file(directory.join(name))?;
            }
            fs::remove_dir(directory)?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn publication_failure_does_not_mask_run_or_cleanup_errors() -> Result<()> {
        let directory = std::env::temp_dir().join(format!("oat-failed-publication-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory)?;
        let mut config = config()?;
        config.result_url = Url::from_file_path(&directory)
            .map_err(|()| anyhow::anyhow!("result URL"))?
            .to_string();
        let outcome = finish_matrix_cell(
            &config,
            "orchestration",
            "failed",
            1,
            2,
            Err(anyhow::anyhow!("client evidence failure")),
            Err(anyhow::anyhow!("cleanup failure")),
        );
        let error = publish_matrix_outcome(&config, outcome)
            .await
            .expect_err("publication must fail");
        let detail = format!("{error:#}");
        for expected in [
            "client evidence failure",
            "cleanup failure",
            "publishing retained matrix evidence also failed",
        ] {
            assert!(detail.contains(expected), "{detail}");
        }
        fs::remove_file(directory.with_extension("lock"))?;
        fs::remove_dir(directory)?;
        Ok(())
    }

    #[test]
    fn exact_legacy_v2_schema_upgrades_without_reinterpreting_old_zero_values() -> Result<()> {
        let config = config()?;
        let measured = parquet_batch(&completed(&config, "old-zero", 1, 0)?)?;
        let legacy = RecordBatch::try_new(legacy_result_schema(), measured.columns().to_vec())?;
        let failure = finish_matrix_cell(
            &config,
            "orchestration",
            "new-unknown",
            2,
            2,
            Err(anyhow::anyhow!("not measured")),
            Ok(()),
        );
        let incoming = parquet_batch(&failure.rows)?;
        let bytes = merge_parquet_bytes(&encode_parquet(&legacy)?, &incoming)?;
        let batch = read_batch(bytes)?;
        assert_eq!(batch.schema(), result_schema());
        let rows = batch
            .column_by_name("rows")
            .context("rows")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("uint64")?;
        assert_eq!(rows.value(0), 0);
        assert!(rows.is_valid(0));
        assert!(rows.is_null(1));
        let mut columns = legacy.columns().to_vec();
        columns[0] = Arc::new(Int32Array::from(vec![3]));
        let unknown_version = RecordBatch::try_new(legacy.schema(), columns)?;
        assert!(merge_parquet_bytes(&encode_parquet(&unknown_version)?, &incoming).is_err());
        let empty_legacy = RecordBatch::new_empty(legacy_result_schema());
        assert_eq!(
            read_batch(merge_parquet_bytes(&encode_parquet(&empty_legacy)?, &incoming)?)?.num_rows(),
            1
        );
        Ok(())
    }

    #[test]
    fn legacy_upgrade_rejects_unrelated_name_type_order_nullability_or_metadata_changes() -> Result<()> {
        let config = config()?;
        let batch = parquet_batch(&completed(&config, "existing", 1, 1)?)?;
        let expected = legacy_result_schema();
        for variant in 0..6 {
            let mut fields = expected
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            let mut schema_metadata = HashMap::new();
            match variant {
                0 => fields.swap(3, 4),
                1 => fields[3] = Field::new("other_host", DataType::Utf8, false),
                2 => fields[3] = fields[3].clone().with_nullable(true),
                3 => {
                    fields[3] = fields[3]
                        .clone()
                        .with_metadata(HashMap::from([("unrelated".to_owned(), "value".to_owned())]))
                }
                4 => {
                    schema_metadata.insert("unrelated".to_owned(), "value".to_owned());
                }
                _ => fields[0] = Field::new("schema_version", DataType::Int64, false),
            }
            let schema = Arc::new(Schema::new_with_metadata(fields, schema_metadata));
            let empty = RecordBatch::new_empty(schema);
            let bytes = encode_parquet(&empty)?;
            assert!(merge_parquet_bytes(&bytes, &batch).is_err(), "variant {variant}");
        }
        Ok(())
    }
}
