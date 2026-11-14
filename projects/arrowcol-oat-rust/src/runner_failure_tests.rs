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
            namespace_created: false,
            namespace_create_attempted: false,
            namespace_owner_token: "test-owner".to_owned(),
            namespace_uid: None,
            runtime_dir: None,
            qap_relative_data_root: None,
            router_metrics_url: None,
            router_tunnel: None,
            fic_record_path: None,
            fic_ready_snapshot_path: None,
            fic_prepare_attempted: false,
            qap_record_path: None,
            qap_bind_attempted: false,
            qap_outcome_unresolved: false,
            workload_stop_record_path: None,
            schedule_configmap_name: "oat-schedule-test".to_owned(),
            service_account: None,
            router_yaml: None,
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
