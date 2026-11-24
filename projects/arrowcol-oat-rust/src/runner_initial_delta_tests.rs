mod initial_delta_tests {
    use super::*;

    fn properties() -> InitialDeltaProperties {
        InitialDeltaProperties {
            target_file_size_bytes: Some(67108864),
            checkpoint_interval: Some(17),
        }
    }

    fn installed_consumer_fixture() -> Result<(OatConfig, Value, Value)> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tables = vec!["sqlserver_availability_replica_states".to_owned()];
        config.tunables = serde_json::from_value(json!({
            "delta":{"target_file_size_bytes":67108864,"checkpoint_interval":17},
            "collector":{"batching":{"adaptive_file":{"target_parquet_bytes":134217728}}},
            "maintenance":{"enabled":true,"operations":["checkpoint","optimize"],"apply":true,
                "checkpoint_every_commits":42,"target_file_mib":17},
        }))?;
        let table = canonical_initializer_table_name(&config.tables[0])?;
        let batching: Value = serde_json::from_str(
            &role_environment(&config, ComponentRole::Collector)?["ARROWCOL_COLLECTOR_BATCHING_CONFIG"],
        )?;
        let adaptive = arrow_deltalake::sizing::AdaptiveFileSizeConfig::default();
        let collector_log = format!(
            "arrowcol_effective_config {}\narrowcol_adaptive_config {}\n",
            json!({"component":"receiver","config":{"batching":batching}}),
            json!({"table_name":table,"scope":"installed_estimator_not_full_run_history","config":{
                "target_parquet_bytes":67108864,"min_input_bytes":adaptive.min_input_bytes,
                "max_input_bytes":adaptive.max_input_bytes,"cold_start_parquet_ratio":adaptive.cold_start_parquet_ratio,
                "ewma_weight":adaptive.ewma_weight,"min_parquet_ratio":adaptive.min_parquet_ratio,
                "max_parquet_ratio":adaptive.max_parquet_ratio,
            }})
        );
        let policy = json!({
            "active":true,
            "startup":{"apply":true,"operations":["checkpoint","optimize"],"interval_secs":60,
                "checkpoint_every_commits":42,"optimize_target_file_bytes":17825792},
            "operation_settings":{"checkpoint_every_commits":17,"optimize_target_file_bytes":67108864},
            "table_policy":{"delta.checkpointInterval":17,"arrowcol.optimize.targetFileSize":67108864,
                "arrowcol.maintenance.operationTimeout":300000},
        });
        let maintenance_log = format!(
            "arrowcol_maintenance_effective_config {}\narrowcol_maintenance_table_config {}\n",
            policy,
            json!({"table":table,"config":policy})
        );
        let collector_config = process_configurations(&collector_log)?;
        let maintenance_config = process_configurations(&maintenance_log)?;
        verify_process_configuration(&config, ComponentRole::Collector, &collector_config)?;
        verify_process_configuration(&config, ComponentRole::Maintenance, &maintenance_config)?;
        let processes = json!([
            {"component":"collector","instance_id":"collector-one","log_complete":true,
                "startup_tunables_verified":true,"effective_config_source":"consumer_startup_readback",
                "effective_config":collector_config,"adaptive_table_policies":collector_adaptive_records(&config,&collector_log)?},
            {"component":"maintenance","instance_id":"maintenance-one","log_complete":true,
                "startup_tunables_verified":true,"effective_config_source":"consumer_startup_readback",
                "effective_config":maintenance_config,"maintenance_table_policies":maintenance_table_evidence(&config,&maintenance_log)?},
        ]);
        let preload =
            json!({"table":table,"initial_delta_properties":{"committed_properties":properties().properties()?}});
        Ok((config, processes, preload))
    }

    fn classified_literal_consumers(
        config: &OatConfig,
        processes: Option<&Value>,
        preload: Option<&Value>,
    ) -> Result<Value> {
        let mut evidence = tunable_leaf_evidence(config)?;
        verify_literal_delta_consumers(config, &mut evidence, processes, preload, 1, &config.tables[0])?;
        Ok(evidence)
    }

    #[test]
    fn literal_consumers_match_parsed_logs_and_reach_rows_for_both_metadata_shapes() -> Result<()> {
        let (config, processes, local_preload) = installed_consumer_fixture()?;
        let table = canonical_initializer_table_name(&config.tables[0])?;
        let aks_preload = json!({"table":table,"initial_delta_properties":{
            "catalog_registration_receipt":{"properties":properties().properties()?},
        }});
        for preload in [&local_preload, &aks_preload] {
            let classified = classified_literal_consumers(&config, Some(&processes), Some(preload))?;
            let verified = classified["consumer_verified"].as_array().context("verified array")?;
            for path in ["delta.target_file_size_bytes", "delta.checkpoint_interval"] {
                let entry = verified
                    .iter()
                    .find(|entry| entry["path"] == path)
                    .context("literal consumer missing")?;
                assert_eq!(entry["per_table_consumer_readback"]["table"], table);
                assert_eq!(
                    entry["per_table_consumer_readback"]["maintenance_operation_matched"],
                    true
                );
                assert!(
                    classified["projection_only"]
                        .as_array()
                        .context("projection array")?
                        .iter()
                        .all(|entry| entry["path"] != path)
                );
            }
            let cell = CellEvidence {
                preload_delta: Some(preload.clone()),
                processes: Some(processes.clone()),
                ..Default::default()
            };
            let mut row = cell.for_table(&config.tables[0], None);
            row["consumer_verified"] = classified["consumer_verified"].clone();
            let serialized: Value = serde_json::from_str(&serde_json::to_string(&row)?)?;
            assert_eq!(serialized["consumer_verified"], classified["consumer_verified"]);
            assert_eq!(serialized["preload_delta"], *preload);
        }
        assert_eq!(
            processes[1]["effective_config"]["startup"]["checkpoint_every_commits"],
            42
        );
        assert_eq!(
            processes[1]["maintenance_table_policies"]["tables"][&table]["operation_settings"]["checkpoint_every_commits"],
            17
        );
        Ok(())
    }

    #[test]
    fn missing_conflicting_overridden_inactive_or_partial_consumers_stay_projection_only() -> Result<()> {
        let (config, processes, preload) = installed_consumer_fixture()?;
        let target = "delta.target_file_size_bytes";
        let checkpoint = "delta.checkpoint_interval";
        for (processes, preload) in [(None, Some(&preload)), (Some(&processes), None)] {
            let classified = classified_literal_consumers(&config, processes, preload)?;
            assert!(
                classified["consumer_verified"]
                    .as_array()
                    .context("verified array")?
                    .is_empty()
            );
        }
        let canonical = canonical_initializer_table_name(&config.tables[0])?;
        let mut cases = Vec::new();
        let mut missing = processes.clone();
        missing[0]["adaptive_table_policies"] = json!([]);
        cases.push((config.clone(), missing, preload.clone(), target));
        let mut truncated = processes.clone();
        truncated[0]["log_complete"] = json!(false);
        cases.push((config.clone(), truncated, preload.clone(), target));
        let mut history = processes.clone();
        let mut conflicting = history[0]["adaptive_table_policies"][0].clone();
        conflicting["config"]["target_parquet_bytes"] = json!(33554432);
        history[0]["adaptive_table_policies"]
            .as_array_mut()
            .context("adaptive records")?
            .push(conflicting);
        cases.push((config.clone(), history, preload.clone(), target));
        let mut overridden = preload.clone();
        overridden["initial_delta_properties"]["committed_properties"]["arrowcol.writer.targetFileSize"] =
            json!("67108864");
        cases.push((config.clone(), processes.clone(), overridden, target));
        let mut optimize_override = preload.clone();
        optimize_override["initial_delta_properties"]["committed_properties"]["arrowcol.optimize.targetFileSize"] =
            json!("67108864");
        cases.push((config.clone(), processes.clone(), optimize_override, target));
        let mut inactive = processes.clone();
        inactive[1]["maintenance_table_policies"]["tables"][&canonical]["active"] = json!(false);
        cases.push((config.clone(), inactive, preload.clone(), checkpoint));
        let mut missing_installed = processes.clone();
        missing_installed[1]["maintenance_table_policies"]["tables"][&canonical]["operation_settings"]["checkpoint_every_commits"] =
            Value::Null;
        cases.push((config.clone(), missing_installed, preload.clone(), checkpoint));
        let mut dry_run = config.clone();
        dry_run.tunables.maintenance.as_mut().context("maintenance")?.apply = Some(false);
        let mut dry_processes = processes.clone();
        dry_processes[1]["effective_config"]["startup"]["apply"] = json!(false);
        dry_processes[1]["maintenance_table_policies"]["tables"][&canonical]["startup"]["apply"] = json!(false);
        cases.push((dry_run, dry_processes, preload.clone(), checkpoint));
        for (config, processes, preload, path) in cases {
            let classified = classified_literal_consumers(&config, Some(&processes), Some(&preload))?;
            assert!(
                classified["projection_only"]
                    .as_array()
                    .context("projection array")?
                    .iter()
                    .any(|entry| entry["path"] == path),
                "{path}"
            );
            assert!(
                classified["consumer_verified"]
                    .as_array()
                    .context("verified array")?
                    .iter()
                    .all(|entry| entry["path"] != path),
                "{path}"
            );
        }
        Ok(())
    }

    #[test]
    fn literal_delta_metadata_never_promotes_effective_consumers_or_disabled_operations() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = serde_json::from_value(json!({"delta":{
            "target_file_size_bytes":67108864,"checkpoint_interval":17,
        }}))?;
        let mut evidence = tunable_leaf_evidence(&config)?;
        evidence["consumer_verified"]
            .as_array_mut()
            .context("verified array")?
            .push(json!({
                "path":"delta.target_file_size_bytes","reason":"metadata_readback",
            }));
        enforce_delta_projection_only(&config, &mut evidence)?;
        for path in ["delta.target_file_size_bytes", "delta.checkpoint_interval"] {
            assert!(
                evidence["consumer_verified"]
                    .as_array()
                    .context("verified array")?
                    .iter()
                    .all(|entry| entry["path"] != path)
            );
            assert!(
                evidence["runner_controls"]
                    .as_array()
                    .context("controls array")?
                    .iter()
                    .all(|entry| entry["path"] != path)
            );
            let projection = evidence["projection_only"]
                .as_array()
                .context("projection array")?
                .iter()
                .find(|entry| entry["path"] == path)
                .context("literal projection missing")?;
            assert_eq!(projection["maintenance_operation"], "disabled");
        }
        config.tunables.maintenance = serde_json::from_value(json!({
            "enabled":true,"operations":["optimize"],"apply":false,
        }))?;
        enforce_delta_projection_only(&config, &mut evidence)?;
        let projection = evidence["projection_only"].as_array().context("projection array")?;
        assert_eq!(
            projection
                .iter()
                .find(|entry| entry["path"] == "delta.target_file_size_bytes")
                .context("target projection")?["maintenance_operation"],
            "plan_only_not_effective_readback"
        );
        assert_eq!(
            projection
                .iter()
                .find(|entry| entry["path"] == "delta.checkpoint_interval")
                .context("checkpoint projection")?["maintenance_operation"],
            "disabled"
        );
        config.tunables.maintenance = serde_json::from_value(json!({
            "enabled":true,"operations":["optimize","checkpoint"],"apply":true,
        }))?;
        enforce_delta_projection_only(&config, &mut evidence)?;
        assert!(
            evidence["projection_only"]
                .as_array()
                .context("projection array")?
                .iter()
                .filter(|entry| entry["path"].as_str().is_some_and(|path| path.starts_with("delta.")))
                .all(|entry| entry["maintenance_operation"] == "configured_apply_not_effective_readback")
        );
        assert!(
            evidence["consumer_verified"]
                .as_array()
                .context("verified array")?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    #[ignore = "subprocess probe for actual initializer receipt"]
    fn actual_local_initializer_receipt_probe() -> Result<()> {
        let root = PathBuf::from(std::env::var("OAT_DELTA_INITIALIZER_PROBE_ROOT")?);
        arrowcol_loadgen::initialize::initialize_local(arrowcol_loadgen::cli::InitializeLocalArgs {
            table: "telemetry.sqlserver.sqlserver_availability_replica_states".to_owned(),
            dataflow_root: root.join("dataflow"),
            partition_by_ingestion_date: true,
            unpartitioned: false,
            delta_target_file_size_bytes: properties().target_file_size_bytes,
            delta_checkpoint_interval: properties().checkpoint_interval,
        })
    }

    #[test]
    fn actual_local_receipt_target_and_row_readbacks_agree_for_qualified_names() -> Result<()> {
        let root = std::env::temp_dir().join(format!("oat-actual-initial-properties-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        fs::create_dir(root.join("dataflow"))?;
        let result = (|| -> Result<()> {
            let table = "telemetry.sqlserver.sqlserver_availability_replica_states";
            let output = std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "runner::initial_delta_tests::actual_local_initializer_receipt_probe",
                    "--ignored",
                    "--nocapture",
                ])
                .env("OAT_DELTA_INITIALIZER_PROBE_ROOT", &root)
                .output()?;
            ensure!(
                output.status.success(),
                "actual initializer failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let receipt =
                verify_local_initial_delta_receipt(std::str::from_utf8(&output.stdout)?, table, true, &properties())?;
            let target = local_delta_table_target(&root, table)?;
            assert_eq!(
                target,
                local_delta_table_target(&root, "sqlserver_availability_replica_states")?
            );
            assert_eq!(
                target,
                arrowcol_loadgen::initialize::canonical_table_target(
                    &Url::from_directory_path(root.join("dataflow"))
                        .map_err(|()| anyhow::anyhow!("fixture root URL"))?,
                    table,
                )?
                .0
            );
            assert_eq!(
                local_partition_initializer_args(table, &root.join("dataflow"))?,
                local_partition_initializer_args("sqlserver_availability_replica_states", &root.join("dataflow"))?
            );
            assert!(local_delta_table_target(&root, "telemetry.sqlserver.foreign").is_err());
            let observed =
                arrow_deltalake::delta::DeltaTable::open_for_maintenance_probe(&target)?.maintenance_properties()?;
            verify_initial_delta_property_readback(&json!(observed), &properties())?;
            let initial = json!({
                "table":table, "requested_initial_properties":properties().properties()?,
                "committed_properties":observed, "initial_receipt":receipt,
                "source":"fresh_checkpoint_aware_delta_metadata_read",
            });
            fs::write(root.join("initial-delta-before-admission.json"), initial.to_string())?;
            let retained = read_local_initial_delta_evidence(&root, table, &properties())?;
            let partition = json!({"table":table,"partition_columns":["__arrowcol_ingestion"],"granularity":"hour","logs":[{"version":0}]});
            let combined = combine_preload_delta_evidence(Some(partition.clone()), Some(retained.clone()))
                .context("combined evidence missing")?;
            for key in ["table", "partition_columns", "granularity", "logs"] {
                assert_eq!(combined[key], partition[key], "{key} lost when properties enabled");
            }
            assert_eq!(
                combine_preload_delta_evidence(Some(partition.clone()), None),
                Some(partition)
            );
            let cell = CellEvidence {
                preload_delta: Some(combined),
                ..Default::default()
            };
            let row = cell.for_table("sqlserver_availability_replica_states", None);
            assert_eq!(
                row["preload_delta"]["initial_delta_properties"]["committed_properties"],
                retained["committed_properties"]
            );
            assert_eq!(
                row["preload_delta"]["initial_delta_properties"]["initial_receipt"]["delta"]["properties"],
                receipt["delta"]["properties"]
            );
            Ok(())
        })();
        fs::remove_dir_all(&root)?;
        result
    }

    #[test]
    fn aks_row_evidence_retains_validated_initialization_and_catalog_property_maps() -> Result<()> {
        let root = std::env::temp_dir().join(format!("oat-aks-property-receipts-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        let result = (|| -> Result<()> {
            let run = "00000000-0000-0000-0000-000000000001";
            let table = "telemetry.sqlserver.sqlserver_availability_replica_states";
            let delta_root = format!("abfss://tables@account.dfs.core.windows.net/runs/{run}/dataflow");
            let init = json!({
                "kind":"oat_run_table_initialized", "run_id":run, "table":table, "publisher_drained":true,
                "delta":{"table_id":"550e8400-e29b-41d4-a716-446655440000","version":0,"active_files":0,
                    "partition_columns":["__arrowcol_ingestion"],
                    "schema_json":serde_json::to_string(&arrowcol_loadgen::initialize::canonical_table_delta_schema(table)?)?,
                    "properties":properties().properties()?}
            });
            let logical = collector_chain::dmv::tables()?
                .into_iter()
                .find(|definition| definition.identity().to_string() == table)
                .context("fixture table missing")?;
            let mut catalog = json!({
                "kind":"oat_run_table_registered", "run_id":run, "table":table, "provider":"delta",
                "location":arrowcol_loadgen::initialize::canonical_table_target(&Url::parse(&delta_root)?,table)?.0.as_str(),
                "schema_version":1,"schema_fingerprint":arrowcol_protocol::fingerprint_schema(logical.schema()).to_string(),
                "catalog_table_count":1,"properties":properties().properties()?,
            });
            let initialization_path = root.join("run-table-initial-delta-properties.json");
            let catalog_path = root.join("run-table-catalog-registration.json");
            fs::write(
                &initialization_path,
                json!({"receipt":init,"requested_initial_properties":properties().properties()?}).to_string(),
            )?;
            fs::write(&catalog_path, json!({"receipt":catalog}).to_string())?;
            let observed = read_aks_initial_delta_evidence(&root, run, table, &delta_root, &properties())?;
            let cell = CellEvidence {
                preload_delta: Some(observed),
                ..Default::default()
            };
            let row = cell.for_table("sqlserver_availability_replica_states", None);
            let recorded = &row["preload_delta"]["initial_delta_properties"];
            assert_eq!(
                recorded["requested_initial_properties"],
                json!(properties().properties()?)
            );
            assert_eq!(
                recorded["initialization_receipt"]["delta"]["properties"],
                init["delta"]["properties"]
            );
            assert_eq!(
                recorded["catalog_registration_receipt"]["properties"],
                catalog["properties"]
            );
            catalog["properties"]["delta.checkpointInterval"] = json!("1");
            fs::write(&catalog_path, json!({"receipt":catalog}).to_string())?;
            assert!(read_aks_initial_delta_evidence(&root, run, table, &delta_root, &properties()).is_err());
            Ok(())
        })();
        fs::remove_dir_all(&root)?;
        result
    }

    #[test]
    fn local_initializer_flags_preserve_defaults_and_reject_invalid_values() -> Result<()> {
        let base = local_initializer_args("sqlserver_wait_stats", &std::env::current_dir()?, false)?;
        let mut args = base.clone();
        append_initial_delta_initializer_arguments(&mut args, &InitialDeltaProperties::default())?;
        assert_eq!(args, base);
        append_initial_delta_initializer_arguments(&mut args, &properties())?;
        assert_eq!(
            &args[base.len()..],
            [
                "--delta-target-file-size-bytes",
                "67108864",
                "--delta-checkpoint-interval",
                "17",
            ]
        );
        let before_invalid = args.clone();
        assert!(
            append_initial_delta_initializer_arguments(
                &mut args,
                &InitialDeltaProperties {
                    checkpoint_interval: Some(0),
                    ..Default::default()
                },
            )
            .is_err()
        );
        assert_eq!(args, before_invalid);
        Ok(())
    }

    #[test]
    fn pin_properties_require_fresh_run_table_and_preserve_unset_config() -> Result<()> {
        let original = r#"{ "run_table": {"delta_root":"abfss://tables@account.dfs.core.windows.net/fresh", "table":"telemetry.sqlserver.sqlserver_wait_stats"} }"#;
        assert_eq!(
            pin_config_with_initial_delta_properties(original, &InitialDeltaProperties::default())?,
            original
        );
        let configured: Value =
            serde_json::from_str(&pin_config_with_initial_delta_properties(original, &properties())?)?;
        assert_eq!(
            configured["run_table"]["delta_properties"],
            serde_json::to_value(properties())?
        );
        assert!(pin_config_with_initial_delta_properties("{}", &properties()).is_err());
        assert!(pin_config_with_initial_delta_properties(r#"{"run_table":null}"#, &properties()).is_err());
        Ok(())
    }

    #[test]
    fn local_receipt_requires_actual_properties_and_fresh_canonical_identity() -> Result<()> {
        let table = "telemetry.sqlserver.sqlserver_availability_replica_states";
        let receipt = json!({
            "kind":"local_delta_table_initialized", "table":table, "publisher_drained":true,
            "delta":{
                "table_id":"550e8400-e29b-41d4-a716-446655440000", "version":0, "active_files":0,
                "partition_columns":["__arrowcol_ingestion"],
                "schema_json":serde_json::to_string(&arrowcol_loadgen::initialize::canonical_table_delta_schema(table)?)?,
                "properties":properties().properties()?,
            }
        });
        assert_eq!(
            verify_local_initial_delta_receipt(&receipt.to_string(), table, true, &properties())?,
            receipt
        );
        for (pointer, value) in [
            ("/delta/properties", Value::Null),
            ("/delta/properties/delta.targetFileSize", json!("1")),
            ("/delta/properties/delta.checkpointInterval", json!(17)),
            ("/publisher_drained", json!(false)),
            ("/delta/version", json!(1)),
            ("/delta/active_files", json!(1)),
            ("/delta/table_id", json!("00000000-0000-0000-0000-000000000000")),
            ("/delta/partition_columns", json!([])),
            ("/delta/schema_json", json!("{}")),
        ] {
            let mut invalid = receipt.clone();
            *invalid.pointer_mut(pointer).context("fixture field missing")? = value;
            assert!(
                verify_local_initial_delta_receipt(&invalid.to_string(), table, true, &properties()).is_err(),
                "{pointer}"
            );
        }
        assert!(
            verify_local_initial_delta_receipt(&format!("{receipt}\n{receipt}"), table, true, &properties()).is_err()
        );
        assert!(verify_local_initial_delta_receipt("", table, true, &properties()).is_err());
        Ok(())
    }

    #[test]
    fn aks_initialization_requires_unique_committed_property_receipt() -> Result<()> {
        let run = "00000000-0000-0000-0000-000000000001";
        let table = "telemetry.sqlserver.sqlserver_availability_replica_states";
        let receipt = json!({
            "kind":"oat_run_table_initialized", "table":table, "run_id":run, "publisher_drained":true,
            "delta":{
                "table_id":"550e8400-e29b-41d4-a716-446655440000", "version":0, "active_files":0,
                "partition_columns":["__arrowcol_ingestion"],
                "schema_json":serde_json::to_string(&arrowcol_loadgen::initialize::canonical_table_delta_schema(table)?)?,
                "properties":properties().properties()?,
            }
        });
        assert_eq!(
            validate_run_table_init_receipt_with_properties(&receipt.to_string(), run, table, &properties())?,
            receipt
        );
        assert!(
            validate_run_table_init_receipt_with_properties(
                &format!("{receipt}\n{receipt}"),
                run,
                table,
                &properties()
            )
            .is_err()
        );
        let mut mismatch = receipt.clone();
        mismatch["delta"]["properties"]["delta.checkpointInterval"] = json!("1");
        assert!(
            validate_run_table_init_receipt_with_properties(&mismatch.to_string(), run, table, &properties()).is_err()
        );
        mismatch["delta"]["properties"] = Value::Null;
        assert!(
            validate_run_table_init_receipt_with_properties(&mismatch.to_string(), run, table, &properties()).is_err()
        );
        Ok(())
    }

    #[test]
    fn retained_local_readback_binds_table_request_and_actual_properties() -> Result<()> {
        let root = std::env::temp_dir().join(format!("oat-initial-properties-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        let result = (|| -> Result<()> {
            let table = "telemetry.sqlserver.sqlserver_wait_stats";
            let mut observed = json!({
                "table":table,
                "requested_initial_properties":properties().properties()?,
                "committed_properties":properties().properties()?,
                "source":"fresh_checkpoint_aware_delta_metadata_read",
                "consumer_evidence_scope":"committed_metadata_only_not_ingest_estimator_or_maintenance_operation",
            });
            let path = root.join("initial-delta-before-admission.json");
            fs::write(&path, observed.to_string())?;
            assert_eq!(
                read_local_initial_delta_evidence(&root, table, &properties())?,
                observed
            );
            assert!(read_local_initial_delta_evidence(&root, "telemetry.sqlserver.foreign", &properties()).is_err());
            observed["committed_properties"]["delta.checkpointInterval"] = json!("1");
            fs::write(&path, observed.to_string())?;
            assert!(read_local_initial_delta_evidence(&root, table, &properties()).is_err());
            Ok(())
        })();
        fs::remove_dir_all(&root)?;
        result
    }
}
