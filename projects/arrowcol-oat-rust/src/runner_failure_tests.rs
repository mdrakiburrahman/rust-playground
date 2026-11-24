#[cfg(test)]
mod failure_evidence_tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn local_setup_streams_both_outputs_before_exit_and_retains_failure_diagnostics() -> Result<()> {
        let directory = std::env::temp_dir().join(format!("oat-progress-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory)?;
        let result = async {
            for exit_code in [0, 7] {
                let stdout_path = directory.join("stdout");
                let stderr_path = directory.join("stderr");
                let mut command = Command::new("sh");
                command.args([
                    "-c",
                    "printf 'build progress\\n'; printf 'build diagnostic\\n' >&2; while [ ! -s \"$1\" ] || [ ! -s \"$2\" ]; do sleep 0.01; done; exit \"$3\"",
                    "oat-progress", stdout_path.to_str().context("stdout path")?,
                    stderr_path.to_str().context("stderr path")?, &exit_code.to_string(),
                ]);
                let result = tokio::time::timeout(
                    Duration::from_secs(10),
                    run_streaming_command_to(
                        command,
                        "build test image",
                        fs::File::create(&stdout_path)?,
                        fs::File::create(&stderr_path)?,
                    ),
                )
                .await
                .context("output was buffered until process exit")?;
                assert_eq!(fs::read_to_string(stdout_path)?, "build progress\n");
                assert_eq!(fs::read_to_string(stderr_path)?, "build diagnostic\n");
                if exit_code == 0 {
                    result?;
                } else {
                    let error = result.err().context("nonzero command must fail")?.to_string();
                    assert!(error.contains("build test image failed"));
                    assert!(error.contains("7"));
                    assert!(error.contains("build diagnostic"));
                }
            }
            Ok(())
        }
        .await;
        fs::remove_dir_all(&directory)?;
        result
    }

    #[test]
    fn compose_project_keeps_full_cell_uuid_even_when_prefixes_collide() {
        let first = scoped_compose_project("arrowcol-oat-quick", "123e4567-e89b-42d3-a456-426614174000");
        let second = scoped_compose_project("arrowcol-oat-quick", "123e4567-e89b-42d3-a456-426614174001");
        assert_eq!(first, "arrowcol-oat-qu-123e4567e89b42d3a456426614174000");
        assert_eq!(second, "arrowcol-oat-qu-123e4567e89b42d3a456426614174001");
        assert_ne!(first, second);
        assert_eq!(first.len(), 48);
        assert!(first.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'));
    }

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

    #[test]
    fn host_publisher_assertion_command_uses_fixed_service_account_and_operator_identity() -> Result<()> {
        let mut config = config()?;
        config.kubectl_context = Some("selected-cluster".to_owned());
        let command = host_publisher_assertion_command(&config);
        let arguments = command
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(arguments, [
            "--context", "selected-cluster", "--request-timeout=60s", "create", "token",
            "arrow-collector", "--namespace", "arrow-collector", "--audience",
            "api://AzureADTokenExchange", "--duration", "1h",
        ]);
        for variable in ["AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE"] {
            assert!(command.as_std().get_envs().any(|(key, value)| key == variable && value.is_none()));
        }
        Ok(())
    }

    #[tokio::test]
    async fn refreshed_host_publisher_assertion_replaces_private_file_atomically() -> Result<()> {
        let path = std::env::temp_dir().join(format!("oat-host-publisher-{}.jwt", uuid::Uuid::new_v4()));
        write_private_secret(&path, "old.header.signature")?;
        let assertion = validated_host_publisher_assertion("new.header.signature\n")?;
        replace_private_secret(&path, assertion)?;
        assert_eq!(fs::read_to_string(&path)?, "new.header.signature\n");
        assert!(validated_host_publisher_assertion("not-a-jwt").is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o600);
        }
        fs::remove_file(&path)?;
        Ok(())
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
                            completions: vec![CollectorCompletion {
                                best_effort_streams: 3,
                                durable_streams: 2,
                                admission_accounting_complete: false,
                                completed_at_unix_ms: 1_794_173_460_000,
                            }],
                        },
                    )]),
                    pinning: PinningReport {
                        owners: HashMap::from([(config.tables[0].clone(), "collector-a".to_owned())]),
                        ..Default::default()
                    },
                    qap_tables: Vec::new(),
                    failure: None,
                    evidence: CellEvidence::default(),
                },
                started_at_ms: 1_794_173_400_000,
                qap: QapMetrics::default(),
                resources: None,
                resource_evidence: None,
            },
        )
    }

    #[test]
    fn results_separate_collector_completion_from_delta_delivery() -> Result<()> {
        let rows = completed(&config()?, "completed", 1, 100)?;
        assert_eq!(rows[0].status, "succeeded");
        let tunables: Value = serde_json::from_str(&rows[0].tunables_json)?;
        assert_eq!(tunables["delta_delivery"], "not_established");
        assert_eq!(tunables["collector_completion"]["evidence"], "reported");
        assert_eq!(tunables["collector_completion"]["clients"][0]["best_effort_streams"], 3);
        assert_eq!(tunables["collector_completion"]["clients"][0]["admission_accounting_complete"], false);
        assert_eq!(tunables["collector_completion"]["clients"][0]["durable_streams"], 2);
        Ok(())
    }

    #[test]
    fn streams_mode_serializes_legacy_concurrency_per_core_as_null() -> Result<()> {
        let mut config = config()?;
        config.platform.kind = crate::config::OatMode::Aks;
        config.dmv_concurrency = Some(4);
        let legacy = completed(&config, "legacy", 1, 100)?;
        let tunables: Value = serde_json::from_str(&legacy[0].tunables_json)?;
        assert!(tunables["cpu_quota"]["client"]["concurrency_per_cpu_core"].as_f64().is_some());
        config.dmv_concurrency = None;
        config.tunables.client = Some(arrowcol_tunables::ClientTunables {
            streams_per_worker: Some(2), ..Default::default()
        });
        let streams = completed(&config, "streams", 1, 100)?;
        let tunables: Value = serde_json::from_str(&streams[0].tunables_json)?;
        assert!(tunables["cpu_quota"]["client"]["concurrency_per_cpu_core"].is_null());
        Ok(())
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
        assert!(
            rows[1]
                .failure
                .as_deref()
                .is_some_and(|error| error.contains("source disconnected"))
        );
        assert!(rows[1].tunables_json.contains("measurement_or_observation_failed"));
        assert!(!rows[1].tunables_json.contains("source disconnected"));
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
                            completions: Vec::new(),
                        },
                    )]),
                    pinning,
                    qap_tables: Vec::new(),
                    failure: None,
                    evidence: CellEvidence::default(),
                },
                started_at_ms: 0,
                qap: QapMetrics::default(),
                resources: None,
                resource_evidence: None,
            },
        )?;
        assert_eq!(rows[0].status, "failed");
        assert!(!rows[0].pinning_ok);
        assert!(rows[0].pinning_error.is_none());
        assert_eq!(rows[0].rows, Some(60));
        assert!(rows[0].tunables_json.contains("ownership_verification_failed"));
        assert!(!rows[0].tunables_json.contains("HTTP timeout"));
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
                .err()
                .context("overall failure remains fatal")?;
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
            let public_failure: Value = serde_json::from_str(tunables.value(1))?;
            assert_eq!(public_failure["cleanup_failed"], true);
            assert!(!tunables.value(1).contains("cleanup custody unresolved"));
            assert!(!tunables.value(1).contains("second cell failed"));
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
            .err()
            .context("publication must fail")?;
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
        let mut columns = measured.columns()[..31].to_vec();
        columns[0] = Arc::new(Int32Array::from(vec![2]));
        let legacy = RecordBatch::try_new(legacy_result_schema(), columns)?;
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

#[cfg(test)]
mod router_metrics_tunnel_tests {
    use super::*;

    #[tokio::test]
    async fn exited_owned_tunnel_is_restarted_before_owner_sampling() -> Result<()> {
        let script_path = std::env::temp_dir().join(format!("oat-fake-kubectl-{}.js", uuid::Uuid::new_v4()));
        fs::write(
            &script_path,
            r#"const http = require('node:http');
const port = Number(process.argv.at(-1).split(':')[0]);
http.createServer((request, response) => {
  if (request.url === '/readyz') return response.writeHead(200).end('ok');
  if (request.url === '/metrics') {
    response.writeHead(200, {'content-type': 'text/plain'});
    return response.end('arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"c\",schema=\"s\",table=\"sqlserver_wait_stats\",collector=\"collector-a\",state=\"active\"} 1\n');
  }
  response.writeHead(404).end();
}).listen(port, '127.0.0.1');"#,
        )?;
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.example.json"
        ))?;
        config.platform.kind = crate::config::OatMode::Aks;
        config.namespace = "arrow-collector-test".to_owned();
        config.kubectl_context = None;
        let mut platform = AksPlatform {
            run_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            resource_readback: Vec::new(),
            cpu_throttling_start: None,
            cpu_throttling_evidence: None,
            namespace_created: false,
            namespace_create_attempted: false,
            namespace_owner_token: "test-owner".to_owned(),
            namespace_uid: None,
            runtime_dir: None,
            qap_relative_data_root: None,
            delta_root: None,
            router_metrics_url: None,
            router_tunnel: None,
            fic_record_path: None,
            fic_ready_snapshot_path: None,
            fic_prepare_attempted: false,
            qap_record_path: None,
            qap_bind_attempted: false,
            qap_cleanup_confirmed: false,
            qap_outcome_unresolved: false,
            run_table_initialization_attempted: false,
            run_table_initialization_drained: false,
            workload_stop_record_path: None,
            schedule_configmap_name: "oat-schedule-test".to_owned(),
            service_account: None,
            router_yaml: None,
            shutdown_drain_evidence: None,
            campaign_cli_paths: None,
            maintenance_cell_baseline: Vec::new(),
            stopped_collector_logs: None,
            shutdown_inputs: None,
        };
        let script = script_path.clone();
        let mut command = || {
            let mut command = Command::new("node");
            command.arg(&script);
            command
        };
        start_router_metrics_tunnel_with(&config, &mut platform, command()).await?;
        let endpoint = platform.router_metrics_endpoint(&config)?;
        let client = reqwest::Client::new();
        let tables = vec!["sqlserver_wait_stats".to_owned()];
        let owners = sample_table_owners(&client, &endpoint, &tables).await?;
        assert_eq!(owners["sqlserver_wait_stats"], vec!["collector-a".to_owned()]);

        platform
            .router_tunnel
            .as_mut()
            .context("fake router metrics tunnel was not started")?
            .child
            .start_kill()?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if platform
                    .router_tunnel
                    .as_mut()
                    .context("fake router metrics tunnel disappeared")?
                    .child
                    .try_wait()?
                    .is_some()
                {
                    return Ok::<(), anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("fake router metrics tunnel did not exit after termination")??;

        assert!(ensure_router_metrics_tunnel_with(&config, &mut platform, &mut command).await?);
        let endpoint = platform.router_metrics_endpoint(&config)?;
        let owners = sample_table_owners(&client, &endpoint, &tables).await?;
        assert_eq!(owners["sqlserver_wait_stats"], vec!["collector-a".to_owned()]);
        cleanup_aks_platform(&config, &mut platform).await?;
        fs::remove_file(script_path)?;
        Ok(())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod qap_helper_lifetime_tests {
    use super::*;

    struct Fixture {
        root: PathBuf,
        owned: Vec<(u32, u64)>,
    }

    fn process_status(pid: u32) -> Result<Option<(char, u32, u64)>> {
        let stat = match fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => stat,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let (_, fields) = stat.rsplit_once(')').context("fixture process stat")?;
        let fields: Vec<_> = fields.split_whitespace().collect();
        Ok(Some((
            fields
                .first()
                .and_then(|state| state.chars().next())
                .context("fixture process state")?,
            fields.get(1).context("fixture parent PID")?.parse()?,
            fields.get(19).context("fixture process start time")?.parse()?,
        )))
    }

    impl Fixture {
        fn new() -> Result<Self> {
            let root = std::env::temp_dir().join(format!("oat-qap-helper-lifetime-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&root)?;
            Ok(Self {
                root,
                owned: Vec::new(),
            })
        }

        fn path(&self, name: &str) -> PathBuf {
            self.root.join(name)
        }

        fn command(&self) -> Result<Command> {
            let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/qap-helper-lifetime.ts");
            let mut command = oat_qap_helper_launcher(&script)?;
            command
                .env_clear()
                .env("PATH", std::env::var_os("PATH").context("fixture PATH")?)
                .env("HOME", &self.root)
                .env("npm_config_offline", "true")
                .env("npm_config_update_notifier", "false")
                .arg("helper")
                .arg(self.path("ready"))
                .arg(self.path("release"))
                .arg(self.path("receipt"));
            Ok(command)
        }

        fn capture_owned_pids(&mut self) -> Result<()> {
            let ready: Value = serde_json::from_slice(&fs::read(self.path("ready"))?)?;
            let helper = u32::try_from(ready["helperPid"].as_u64().context("fixture helper PID")?)?;
            let worker = u32::try_from(ready["workerPid"].as_u64().context("fixture worker PID")?)?;
            for mut pid in [helper, worker] {
                while pid != std::process::id() {
                    anyhow::ensure!(pid > 1, "fixture ancestry must terminate at its owning test");
                    let (_, parent, start) = process_status(pid)?.context("fixture process already exited")?;
                    if !self.owned.iter().any(|(owned, _)| *owned == pid) {
                        self.owned.push((pid, start));
                    }
                    pid = parent;
                }
            }
            eprintln!("owned QAP fixture PIDs: {:?}", self.owned);
            Ok(())
        }

        fn running(&self) -> Result<Vec<u32>> {
            self.owned.iter().try_fold(Vec::new(), |mut running, (pid, start)| {
                if let Some((state, _, observed_start)) = process_status(*pid)?
                    && observed_start == *start
                    && !matches!(state, 'Z' | 'X')
                {
                    running.push(*pid);
                }
                Ok(running)
            })
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            for (pid, start) in &self.owned {
                match process_status(*pid) {
                    Ok(Some((state, _, observed_start))) if observed_start == *start && !matches!(state, 'Z' | 'X') => {
                        let Some(owned_pid) = i32::try_from(*pid).ok().and_then(rustix::process::Pid::from_raw) else {
                            eprintln!("invalid owned fixture PID: {pid}");
                            continue;
                        };
                        if let Err(error) = rustix::process::kill_process(owned_pid, rustix::process::Signal::KILL)
                            && error != rustix::io::Errno::SRCH
                        {
                            eprintln!("owned fixture PID cleanup failed: {pid:?}: {error}");
                        }
                    }
                    Err(error) => eprintln!("owned fixture PID inspection failed: {error:#}"),
                    _ => {}
                }
            }
            if let Err(error) = fs::remove_dir_all(&self.root) {
                eprintln!("owned fixture directory cleanup failed: {error}");
            }
        }
    }

    async fn ready(path: PathBuf) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(8), async {
            while !path.is_file() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("real npx/tsx fixture did not publish descendant readiness")
    }

    #[tokio::test]
    async fn oat_qap_helper_normal_completion_retains_descendant_receipt() -> Result<()> {
        let mut fixture = Fixture::new()?;
        let mut operation = Box::pin(run_oat_qap_helper_output(fixture.command()?));
        tokio::select! {
            result = &mut operation => bail!("QAP fixture exited before readiness: {result:?}"),
            result = ready(fixture.path("ready")) => result?,
        }
        fixture.capture_owned_pids()?;
        fs::write(fixture.path("release"), b"complete")?;
        let output = tokio::time::timeout(Duration::from_secs(5), operation).await??;
        anyhow::ensure!(output.status.success(), "QAP fixture failed: {output:?}");
        let receipt = fs::read_to_string(fixture.path("receipt"))?;
        eprintln!("normal QAP descendant receipt: {receipt}");
        assert!(
            fixture.running()?.is_empty(),
            "normal QAP helper left live owned processes"
        );
        Ok(())
    }

    #[tokio::test]
    async fn oat_qap_helper_timeout_prevents_descendant_receipt() -> Result<()> {
        let mut fixture = Fixture::new()?;
        let mut operation = Box::pin(run_oat_qap_helper_output(fixture.command()?));
        tokio::select! {
            result = &mut operation => bail!("QAP fixture exited before readiness: {result:?}"),
            result = ready(fixture.path("ready")) => result?,
        }
        fixture.capture_owned_pids()?;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), operation)
                .await
                .is_err()
        );
        eprintln!("QAP caller timeout returned at {}", unix_time_ms());
        fs::write(fixture.path("release"), b"after caller timeout")?;
        tokio::time::timeout(Duration::from_secs(2), async {
            while !fixture.path("receipt").exists() && !fixture.running()?.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        let receipt = if fixture.path("receipt").exists() {
            Some(fs::read_to_string(fixture.path("receipt"))?)
        } else {
            None
        };
        let running = fixture.running()?;
        eprintln!("after QAP cancellation: receipt={receipt:?}, live_owned_pids={running:?}");
        assert!(
            receipt.is_none(),
            "QAP descendant wrote its receipt after caller timeout: {receipt:?}"
        );
        assert!(running.is_empty(), "QAP cancellation left live owned PIDs: {running:?}");
        Ok(())
    }
}

#[cfg(test)]
mod offline_finalization_tests {
    use super::*;

    const ROOT: &str = "abfss://data@example.dfs.core.windows.net/campaign/run/dataflow";
    const CAMPAIGN: &str = "c0ffee00-0000-4000-8000-000000000001";
    const OWNER: &str = "c0ffee00-0000-4000-8000-000000000002";
    const CELL: &str = "c0ffee00-0000-4000-8000-000000000003";
    const RUN: &str = "c0ffee00-0000-4000-8000-000000000004";

    struct TestDirectory(PathBuf);
    impl Drop for TestDirectory {
        fn drop(&mut self) {
            if let Err(error) = fs::remove_dir_all(&self.0) {
                eprintln!("could not remove offline Delta fixture: {error}");
            }
        }
    }

    #[derive(Clone, Copy)]
    enum Fault {
        Missing,
        WrongPod,
    }

    struct FixturePlatform {
        inventory: RunInventory,
        inputs: ShutdownFinalizationInputs,
        fault: Fault,
        events: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl Platform for FixturePlatform {
        fn kind(&self) -> crate::config::OatMode { crate::config::OatMode::Aks }
        fn workload_endpoint(&self, _: &OatConfig) -> String { unreachable!() }
        fn router_metrics_endpoint(&self, _: &OatConfig) -> Result<String> { unreachable!() }
        fn qap_endpoint(&self, _: &OatConfig) -> String { unreachable!() }
        fn delta_root(&self) -> Option<&str> { Some(ROOT) }
        async fn ensure_stack(&mut self, _: &OatConfig, _: &str, _: u32) -> Result<()> { unreachable!() }
        async fn scale_arrowcols(&mut self, _: &OatConfig, _: u32, _: u32) -> Result<()> { unreachable!() }
        async fn initialize_run_table_pins(&mut self, _: &OatConfig, _: &str, _: &str, _: u64, _: u64) -> Result<()> { unreachable!() }
        fn qap_observation_mode(&self) -> arrowcol_loadgen::ObservationMode { unreachable!() }
        fn qap_database(&self, _: &OatConfig) -> Result<String> { unreachable!() }
        async fn initialize_qap(&mut self, _: &OatConfig, _: &str, _: &[String]) -> Result<Vec<String>> { unreachable!() }
        async fn prepare_client_fleet(&mut self, _: &OatConfig, _: &str) -> Result<()> { unreachable!() }
        async fn start_client(&mut self, _: &OatConfig, _: &str, _: &str, _: usize, _: &[String]) -> Result<ClientHandle> { unreachable!() }
        async fn poll_client(&self, _: &OatConfig, _: &ClientHandle) -> Result<ClientProgress> { unreachable!() }
        async fn arm_clients(&mut self, _: &OatConfig, _: &str, _: u64) -> Result<()> { unreachable!() }
        async fn finish_client(&self, _: &OatConfig, _: ClientHandle) -> Result<String> { unreachable!() }
        async fn cancel_client(&self, _: &OatConfig, _: &ClientHandle) -> Result<()> { unreachable!() }
        async fn cleanup(&mut self, _: &OatConfig) -> Result<()> {
            self.events.lock().unwrap().push("freeze");
            Ok(())
        }
        async fn capture_shutdown_inputs(&mut self) -> Option<ShutdownFinalizationInputs> {
            self.events.lock().unwrap().push("capture");
            Some(self.inputs.clone())
        }
        async fn finish_cleanup(&mut self, _: &OatConfig, rows: &[OatResult], frozen: bool) -> Result<()> {
            self.events.lock().unwrap().push("finish_cleanup_attempted");
            ensure!(frozen, "freeze not confirmed");
            let actor = CampaignCleanupActor { mode: CampaignActorMode::Producer, owner_id: OWNER.into(), fencing_token: 1 };
            let reconciliation = shutdown_reconciliation_request(rows);
            let receipt = ShutdownFinalizationReceipt {
                schema_version: 1,
                blob: format!("_arrowcol-custody/campaigns/{CAMPAIGN}/runs/{RUN}/cells/{CELL}/shutdown-finalization.json"),
                etag: "\"receipt\"".into(), sha256: "a".repeat(64), inventory_etag: "\"inventory\"".into(),
                actor: actor.clone(),
                origin: ShutdownFinalizationOrigin { campaign_id: CAMPAIGN.into(), owner_id: OWNER.into(), fencing_token: 1,
                    orchestration_run_id: RUN.into(), cell_run_id: CELL.into(), namespace: self.inventory.namespace.clone() },
                terminal_capture: self.inputs.terminal_capture.clone(), delta_before: self.inputs.delta_before.clone(),
                delta_terminal: self.inputs.delta_terminal.clone(),
                verdicts: vec![ShutdownFinalVerdict { pod_name: "collector-0".into(), pod_uid: "original-pod".into(),
                    container: "collector".into(), restart_count: Some(0), classification: "exit_0".into() }],
                status: match self.fault { Fault::Missing => "unavailable", Fault::WrongPod => "validated" }.into(),
                reason_code: matches!(self.fault, Fault::Missing).then(|| "terminal_identity_unavailable".into()),
                release_allowed: matches!(self.fault, Fault::WrongPod), reconciliation: reconciliation.clone(),
            };
            validate_shutdown_finalization(&receipt, &self.inventory, "\"inventory\"", &actor, &self.inputs, &reconciliation)?;
            ensure_shutdown_release(&receipt, rows)?;
            self.events.lock().unwrap().push("release");
            Ok(())
        }
        fn shutdown_drain_evidence(&self) -> Option<Value> {
            Some(json!({"status":"unavailable","cleanup_attempted":true,"cleanup_confirmed":false}))
        }
    }

    struct LocalDeltaReader {
        delta: Value,
        events: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }
    #[async_trait::async_trait]
    impl FinalAksReconciler for LocalDeltaReader {
        async fn attach(&self, config: &OatConfig, _: Option<&str>, rows: &mut [OatResult], frozen: bool) {
            self.events.lock().unwrap().push("delta_reconciliation");
            for row in rows {
                apply_final_aks_delta_reconciliation(config, row, Some(&self.delta), frozen).unwrap();
            }
        }
    }

    fn setup(fault: Fault, config: &OatConfig) -> Result<FixturePlatform> {
        let identity = CampaignIdentity { campaign_id: CAMPAIGN.into(), owner_id: OWNER.into(), fencing_token: 1 };
        let inventory = initial_campaign_inventory(&identity, RUN, CELL, &format!("oat-{}", CELL.replace('-', "")),
            "arrowcollector12345", "checkpoints", CampaignCellBudget {
                effective_stop_budgets: config.maintenance_stop_budget()?,
                planned_cell_duration_ms: config.timing_budget()?.planned_cell_duration_ms,
                drain_budget: InventoryDrainBudget { budget_ms: config.platform.aks_drain_budget_ms,
                    collector_grace_ms: config.collector_stop_grace_seconds * 1_000,
                    pre_stop_capture_margin_ms: crate::config::AKS_PRE_STOP_CAPTURE_MARGIN_MS },
            })?;
        let reference = |blob: String| ShutdownSourceReference { blob, etag: "\"source\"".into(), sha256: "a".repeat(64) };
        let prefix = format!("_arrowcol-custody/campaigns/{CAMPAIGN}/runs/{RUN}/cells/{CELL}");
        let capture = format!("{}/custody/{CELL}/terminal_capture.json", inventory.evidence_destination.as_ref().unwrap().blob_prefix);
        let summary = ShutdownTerminalPodSummary { pod_name: "collector-0".into(), pod_uid: "replacement-pod".into(),
            container: config.dfe_container.clone(), restart_count: 0, exit_code: 0,
            summary: ShutdownTerminalSummary { schema_version: 1, admitted: 1, committed: 1, failed: 0,
                cancelled_ambiguous: 0, pending: 0, committed_rows: 300, incomplete: 0,
                commits: vec![ShutdownTerminalCommit { table_id: fixture_sha256(format!("{ROOT}/telemetry/sqlserver/{}/", config.tables[0]).as_bytes()),
                    version: 0, rows: 300, files: 1, parquet_bytes: 900 }] } };
        let inputs = ShutdownFinalizationInputs { schema_version: 1, inventory_etag: "\"inventory\"".into(),
            terminal_capture: Some(reference(capture)), delta_before: Some(reference(format!("{prefix}/delta-before.json"))),
            delta_terminal: Some(reference(format!("{prefix}/delta-terminal.json"))),
            unavailable_reason: matches!(fault, Fault::Missing).then(|| "terminal_identity_unavailable".into()),
            terminal_summaries: matches!(fault, Fault::WrongPod).then(|| vec![summary]) };
        Ok(FixturePlatform { inventory, inputs, fault, events: Arc::new(std::sync::Mutex::new(Vec::new())) })
    }

    #[tokio::test]
    async fn insufficient_original_pod_evidence_withholds_release_but_keeps_verified_window() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!("../../../docs/experiments/arrowcol-oat/oat-config.quick.json"))?;
        config.tables = vec!["sqlserver_wait_stats".into()];
        config.clients_per_table = 1;
        config.dfe_container = "collector".into();
        config.platform.kind = crate::config::OatMode::Aks;
        config.campaign_profile = Some(crate::config::CampaignProfile::SingleHotTable);
        let directory = TestDirectory(std::env::temp_dir().join(format!("oat-offline-finalize-{}", uuid::Uuid::new_v4())));
        fs::create_dir(&directory.0)?;
        let add = json!({"add":{"path":"part.parquet","size":900,"dataChange":true,"stats":json!({"numRecords":300}).to_string()}});
        let info = json!({"commitInfo":{"timestamp":2_000,"operation":"WRITE"}});
        fs::write(directory.0.join("00000000000000000000.json"), format!("{info}\n{add}\n"))?;
        let receipt = json!({"delta_version":0,"rows":300,"files":1,"input_bytes":1200,"parquet_bytes":900,
            "min_ingested_at_ms":1_500,"max_ingested_at_ms":1_900,"finished_at_unix_ms":2_000});
        let durable = json!({"output_throughput":{"measurement_start_unix_ms":1_000,"measurement_end_unix_ms":31_000,
            "rows":300,"files":1,"input_bytes":1200,"parquet_bytes":900},
            "run_cumulative_output":{"rows":300,"commits_by_version":[receipt],"settled":true}});
        let measured = reconcile_local_delta_log(&directory.0, &durable);
        assert_eq!(measured["status"], "matched");
        assert_eq!(measured["observed"]["durable_rows_per_s"], 10.0);
        assert_eq!(measured["observed"]["bytes_per_row"], 3.0);
        let mut zero_durable = durable.clone();
        zero_durable["output_throughput"] = json!({"measurement_start_unix_ms":5_000,
            "measurement_end_unix_ms":35_000,"rows":0,"files":0,"input_bytes":0,"parquet_bytes":0});
        let zero = reconcile_local_delta_log(&directory.0, &zero_durable);
        assert_eq!(zero["status"], "matched");
        assert_eq!(zero["observed"]["durable_rows"], 0);
        assert_eq!(zero["observed"]["durable_rows_per_s"], 0.0);
        let delta = read_local_delta_log_commits(&directory.0)?;
        for (fault, window) in [(Fault::Missing, 1), (Fault::WrongPod, 1), (Fault::Missing, 0), (Fault::Missing, 2)] {
            let mut rows = result_rows(&config, RUN, CELL, 1, 1, CellMeasurements {
                clients: ClientRun { metrics: HashMap::from([(config.tables[0].clone(), ClientMetrics {
                    rows: 300, duration_ms: 30_000, rows_per_second: 10.0, completions: Vec::new(),
                })]), pinning: PinningReport::default(), qap_tables: Vec::new(),
                    failure: None, evidence: CellEvidence::default() }, started_at_ms: 1_000,
                qap: QapMetrics::default(), resources: None, resource_evidence: None,
            })?;
            assert_eq!(rows[0].status, "succeeded");
            let mut input = if window == 2 { zero_durable.clone() } else { durable.clone() };
            let observation = if window == 2 { &zero["observed"] } else { &measured["observed"] };
            input["output_throughput"]["rows_per_second"] = observation["durable_rows_per_s"].clone();
            input["output_throughput"]["bytes_per_row"] = observation["bytes_per_row"].clone();
            if window == 0 {
                input.as_object_mut().unwrap().remove("output_throughput");
            }
            rows[0].evidence_json = Some(json!({"client_phase_reports":[{"phase_counters":{"process_cumulative":{
                "completed_rows":300,"ambiguous_rows":0}}}],"durable_commits":input}).to_string());
            let mut platform = setup(fault, &config)?;
            let events = Arc::clone(&platform.events);
            let reader = LocalDeltaReader { delta: delta.clone(), events: Arc::clone(&events) };
            let (outcome, confirmed) = finalize_isolated_cell(&config, RUN, CELL, (1,1), (Ok(rows), false), &mut platform, &reader).await;
            assert!(!confirmed);
            assert!(!outcome.aks_cleanup_confirmed);
            assert_eq!(*events.lock().unwrap(), ["freeze","capture","delta_reconciliation","finish_cleanup_attempted"]);
            let row = &outcome.rows[0];
            assert_eq!(row.status, "failed");
            assert!(row.failure.as_deref().unwrap().contains("clean up isolated matrix-cell platform"));
            let failure = row.failure.as_deref().unwrap();
            match fault {
                Fault::Missing => assert!(failure.contains("finalization is unavailable or failed")),
                Fault::WrongPod => assert!(failure.contains("validated collector verdicts differ from terminal summary identities")),
            }
            let evidence: Value = serde_json::from_str(row.evidence_json.as_deref().unwrap())?;
            assert_eq!(evidence["teardown_outcome"], "failed");
            assert_eq!(evidence["commit_window_output_usage"], "diagnostic_only");
            if window != 0 {
                assert_eq!(evidence["commit_window_output_throughput"]["rows"], observation["durable_rows"]);
                assert_eq!(evidence["commit_window_output_throughput"]["rows_per_second"], observation["durable_rows_per_s"]);
                assert_eq!(evidence["commit_window_output_throughput"]["bytes_per_row"], observation["bytes_per_row"]);
            } else {
                assert!(evidence["commit_window_output_throughput"]["rows"].is_null());
                assert!(evidence["commit_window_output_throughput"]["rows_per_second"].is_null());
                assert!(evidence["commit_window_output_throughput"]["bytes_per_row"].is_null());
                assert_eq!(evidence["commit_window_output_throughput"]["unavailable_reason"], "measurement_window_evidence_unavailable");
            }
            assert!(evidence["screen_v1"].is_null());
            if matches!(fault, Fault::Missing) {
                assert!(evidence["durable_commits"]["collector_teardown_admissions"]["cancelled_ambiguous"].is_null());
            }
        }
        Ok(())
    }
}
