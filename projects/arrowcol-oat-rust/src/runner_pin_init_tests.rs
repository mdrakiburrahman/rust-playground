#[cfg(test)]
mod pin_init_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    const RUN: &str = "11111111-1111-4111-8111-111111111111";
    const JOB_UID: &str = "22222222-2222-4222-8222-222222222222";

    fn executable(path: &Path, text: &str) -> Result<()> {
        fs::write(path, text)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        Ok(())
    }

    fn fixture(root: &Path) -> Result<(OatConfig, AksPlatform, Value)> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.platform.kind = crate::config::OatMode::Aks;
        config.campaign_profile = Some(crate::config::CampaignProfile::ScaleOutSingleTable);
        config.tables = vec!["sqlserver_availability_replica_states".to_owned()];
        config.namespace = "oat-pin-fixture".to_owned();
        config.stamp = Some("89152".to_owned());
        config.dfe_deployment = "arrowcol".to_owned();
        config.dfe_container = "dataflow".to_owned();
        let images = ["sqlserver", "catalog", "router", "dataflow", "maintenance"]
            .into_iter()
            .map(|role| {
                (
                    role.to_owned(),
                    format!("arrowcollector89152.azurecr.io/{role}@sha256:{}", "a".repeat(64)),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut runtime = json!({
            "namespace":config.namespace,"oatRunId":RUN,
            "workloadIdentityClientId":"33333333-3333-4333-8333-333333333333",
            "subscriptionId":"44444444-4444-4444-8444-444444444444","stamp":"89152",
            "deltaRoot":format!("abfss://onelake@arrowcollector89152.dfs.core.windows.net/oat/runs/{RUN}/dataflow"),
            "images":images,"quiver":false,
            "effective_stop_budgets":{"maintenance_grace_seconds":4800,"maintenance_stop_timeout_ms":5100000},
            "maintenance":{"tables":["telemetry.sqlserver.sqlserver_availability_replica_states"],
                "operations":["optimize","checkpoint","purge","vacuum","prune-log"],"apply":true}
        });
        project_scale_out_runtime(&mut runtime, &config, 1)?;
        fs::write(root.join("runtime-config.json"), serde_json::to_vec(&runtime)?)?;
        let mut router: Value = serde_yaml::from_str(include_str!("../../arrowcol-router-rust/config.docker.yaml"))?;
        router["routing"]["oat_run_id"] = json!(RUN);
        let platform = AksPlatform {
            run_id: RUN.to_owned(),
            resource_readback: Vec::new(),
            cpu_throttling_start: None,
            cpu_throttling_evidence: None,
            namespace_created: true,
            namespace_create_attempted: true,
            namespace_owner_token: RUN.to_owned(),
            namespace_uid: Some(JOB_UID.to_owned()),
            runtime_dir: Some(root.to_path_buf()),
            qap_relative_data_root: None,
            delta_root: runtime["deltaRoot"].as_str().map(str::to_owned),
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
            schedule_configmap_name: "schedule".to_owned(),
            service_account: Some("arrowcol-oat".to_owned()),
            router_yaml: Some(serde_yaml::to_string(&router)?),
            shutdown_drain_evidence: None,
            campaign_cli_paths: None,
            maintenance_cell_baseline: Vec::new(),
            stopped_collector_logs: None,
            shutdown_inputs: None,
        };
        Ok((config, platform, runtime))
    }

    fn child(root: &Path, mode: &str) -> Result<std::process::Command> {
        let mut command = std::process::Command::new(std::env::current_exe()?);
        command.args([
            "runner::pin_init_tests::isolated_pin_init_probe",
            "--exact",
            "--nocapture",
        ]);
        command
            .env_clear()
            .env("PATH", format!("{}:/usr/bin:/bin", root.display()))
            .env("HOME", root)
            .env("PIN_INIT_PROBE_ROOT", root)
            .env("PIN_INIT_PROBE_MODE", mode);
        Ok(command)
    }

    #[test]
    fn fresh_initializer_receives_only_explicit_delta_stats_selection() -> Result<()> {
        let root = std::env::temp_dir().join(format!("pin-stats-env-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        let (mut config, platform, _) = fixture(&root)?;
        let absent = run_table_initializer_cloud_env(&config, &platform, RUN)?;
        assert!(!absent.as_array().context("initializer environment")?.iter()
            .any(|entry| entry["name"] == "ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS"));
        for columns in [json!([]), json!(["replica_id"])] {
            config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
                "collector": {"commit": {"delta_stats_columns": columns}}
            }))?;
            let actual = run_table_initializer_cloud_env(&config, &platform, RUN)?;
            let field = actual.as_array().context("initializer environment")?.iter()
                .find(|entry| entry["name"] == "ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS")
                .context("missing creation-time stats policy")?;
            assert_eq!(field["value"], columns.to_string());
        }
        fs::remove_dir_all(&root)?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires a built arrowcol-loadgen binary; creates empty local tables without load"]
    async fn local_setup_initializer_commits_only_explicit_delta_stats_metadata() -> Result<()> {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize()?;
        let table = "sqlserver_availability_replica_states";
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        for partitioned in [false, true] {
            for columns in [None, Some(json!([])), Some(json!(["customer_id"]))] {
                let root = std::env::temp_dir().join(format!("local-stats-init-{}", uuid::Uuid::new_v4()));
                fs::create_dir(&root)?;
                let mut commit = json!({});
                if let Some(columns) = &columns {
                    commit["delta_stats_columns"] = columns.clone();
                }
                config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
                    "collector": {"commit":commit, "runtime":{"parquet_column_workers":4}}
                }))?;
                let arguments = local_initializer_args(table, &root, partitioned)?;
                let mut initialize = local_table_initializer_command(&config, &workspace, arguments.clone())?;
                assert!(!initialize.as_std().get_envs()
                    .any(|(name, _)| name == "ARROWCOL_PARQUET_COLUMN_WORKERS"));
                let output = initialize.output().await?;
                ensure!(output.status.success(), "local creation failed: {}{}",
                    String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
                let table_root = root.join("telemetry/sqlserver").join(table);
                let path = table_root.join("_delta_log/00000000000000000000.json");
                let original = fs::read(&path)?;
                let log = std::str::from_utf8(&original)?;
                let actions = log.lines().map(serde_json::from_str::<Value>)
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let metadata = actions.iter().find_map(|action| action.get("metaData")).context("committed metadata missing")?;
                let properties = &metadata["configuration"];
                if let Some(columns) = &columns {
                    let columns = serde_json::from_value::<Vec<String>>(columns.clone())?;
                    ensure!(delta_stats_properties_match(&columns, properties), "local committed stats selection differs");
                    let text = String::from_utf8(output.stderr)?;
                    config.tables = vec![table.to_owned()];
                    assert_eq!(collector_delta_stats_records(&config, &text)?.len(), 1);
                } else {
                    assert!(properties.get("delta.dataSkippingStatsColumns").is_none());
                    assert!(properties.get("delta.dataSkippingNumIndexedCols").is_none());
                }
                assert_eq!(metadata["partitionColumns"].as_array().context("partition columns missing")?.len(),
                    usize::from(partitioned));
                assert_eq!(fs::read_dir(table_root.join("_delta_log"))?.count(), 1);
                let retry = local_table_initializer_command(&config, &workspace, arguments)?.output().await?;
                assert!(!retry.status.success());
                assert_eq!(fs::read(&path)?, original);
                fs::remove_dir_all(&root)?;
            }
        }
        Ok(())
    }

    #[test]
    fn emitted_pin_init_environment_passes_real_ownership_gate() -> Result<()> {
        let root = std::env::temp_dir().join(format!("pin-env-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        executable(
            &root.join("kubectl"),
            &format!(
                "#!/bin/sh\nif [ \"$1\" = create ]; then cat > '{}/manifest.json'; exit 42; fi\nexit 43\n",
                root.display()
            ),
        )?;
        let output = child(&root, "emit")?.output()?;
        ensure!(
            output.status.success(),
            "actual Job render failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let manifest: Value = serde_json::from_slice(&fs::read(root.join("manifest.json"))?)?;
        let env = manifest
            .pointer("/items/1/spec/template/spec/containers/0/env")
            .and_then(Value::as_array)
            .context("actual emitted pin-init env missing")?;
        executable(
            &root.join("az"),
            &format!(
                "#!/bin/sh\nprintf first-credential-step > '{}/credential-step'\nexit 91\n",
                root.display()
            ),
        )?;
        let mut ownership = child(&root, "ownership")?;
        for entry in env {
            let name = entry["name"].as_str().context("env name")?;
            let value = if name == "ARROWCOL_RESET_GENERATION" {
                RUN
            } else {
                entry["value"].as_str().context("literal env value")?
            };
            ownership.env(name, value);
        }
        let output = ownership.output()?;
        println!(
            "actual PublisherSession ownership probe: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        ensure!(
            root.join("credential-step").is_file(),
            "real ownership gate rejected emitted env before the offline credential boundary"
        );
        ensure!(output.status.success(), "ownership probe failed");
        fixture(&root)?;
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../arrowcol-oat/runtime.ts");
        let mut render = std::process::Command::new("npx");
        render
            .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
            .args(["--no-install", "tsx"])
            .arg(script)
            .arg("render")
            .arg(root.join("runtime-config.json"))
            .arg("workload")
            .arg(root.join("workload.json"));
        let output = render.output()?;
        ensure!(
            output.status.success(),
            "runtime renderer: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let list: Value = serde_json::from_slice(&fs::read(root.join("workload.json"))?)?;
        let resources = list["items"].as_array().context("runtime resource List missing")?;
        let collector = resources
            .iter()
            .find(|r| r["kind"] == "Deployment" && r["metadata"]["name"] == "arrowcol")
            .context("collector")?;
        let collector_env = collector
            .pointer("/spec/template/spec/containers/0/env")
            .and_then(Value::as_array)
            .context("collector env")?;
        for name in [
            "ARROWCOL_DELTA_ROOT",
            "ARROWCOL_AZURE_MODE",
            "AZURE_SUBSCRIPTION_ID",
            "AZURE_STAMP",
            "AZURE_RESOURCE_GROUP",
            "AZURE_STORAGE_ACCOUNT",
            "AZURE_STORAGE_CHECKPOINT_CONTAINER",
            "ARROWCOL_RESET_GENERATION",
        ] {
            let actual = env
                .iter()
                .find(|e| e["name"] == name)
                .context("missing pin cloud key")?;
            let expected = collector_env
                .iter()
                .find(|e| e["name"] == name)
                .context("missing collector cloud key")?;
            assert_eq!(actual, expected, "{name}");
        }
        let pod = &manifest["items"][1]["spec"]["template"];
        assert_eq!(
            pod["metadata"]["labels"]["azure.workload.identity/use"],
            true.to_string()
        );
        assert_eq!(
            pod["spec"]["serviceAccountName"],
            collector["spec"]["template"]["spec"]["serviceAccountName"]
        );
        assert_eq!(pod["spec"]["automountServiceAccountToken"], false);
        assert_eq!(pod["spec"]["securityContext"]["runAsUser"], 10001);
        assert_eq!(
            pod["spec"]["containers"][0]["securityContext"]["readOnlyRootFilesystem"],
            true
        );
        assert_eq!(pod["spec"]["volumes"][2]["emptyDir"]["sizeLimit"], "64Mi");
        assert_eq!(pod["spec"]["containers"][0]["volumeMounts"][2]["mountPath"], "/tmp");
        assert_eq!(pod["spec"]["nodeSelector"]["arrow-collector/role"], "arrowcol");
        assert_eq!(pod["spec"]["containers"][0]["resources"]["limits"]["memory"], "512Mi");
        fs::remove_dir_all(&root)?;
        Ok(())
    }

    fn failed_job() -> Value {
        json!({"kind":"Job","metadata":{"uid":JOB_UID,
            "name":format!("oat-pin-init-{}",RUN.replace('-', "")),
            "namespace":"oat-pin-fixture","labels":{"arrowcol-oat-run":RUN.replace('-', "")}
        },"status":{"ready":0,"failed":1,
            "conditions":[{"type":"Failed","status":"True"}]}})
    }

    fn terminated_pods() -> Value {
        json!({"items":[{
            "metadata":{"uid":"55555555-5555-4555-8555-555555555555",
                "namespace":"oat-pin-fixture","labels":{"controller-uid":JOB_UID},
                "ownerReferences":[{"kind":"Job","name":format!("oat-pin-init-{}",RUN.replace('-', "")),"uid":JOB_UID,"controller":true}]},
            "spec":{"containers":[{"name":"pin-init"}],"initContainers":[{"name":"identity-init"}]},
            "status":{"phase":"Failed",
                "containerStatuses":[{"name":"pin-init","state":{"terminated":{"containerID":"containerd://app",
                    "exitCode":1,"finishedAt":"2026-11-18T11:00:00Z"}}}],
                "initContainerStatuses":[{"name":"identity-init","state":{"terminated":{"containerID":"containerd://init",
                    "exitCode":0,"finishedAt":"2026-11-18T10:59:59Z"}}}]}
        }]})
    }

    #[test]
    fn failed_initializer_termination_confirms_cleanup_without_initializing() -> Result<()> {
        for variant in [
            "kubernetes-wire",
            "ready-absent",
            "terminating-absent",
            "uncounted-empty",
            "uncounted-fields-absent",
        ] {
            let root = std::env::temp_dir().join(format!("pin-stop-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&root)?;
            let mut job = failed_job();
            match variant {
                "ready-absent" => {
                    job["status"].as_object_mut().context("fixture status")?.remove("ready");
                    job["status"]["terminating"] = json!(0);
                }
                "terminating-absent" => job["status"]["active"] = json!(0),
                "uncounted-empty" => job["status"]["uncountedTerminatedPods"] = json!({"succeeded":[],"failed":[]}),
                "uncounted-fields-absent" => job["status"]["uncountedTerminatedPods"] = json!({}),
                _ => {}
            }
            fs::write(root.join("job.json"), serde_json::to_vec(&job)?)?;
            fs::write(root.join("created-job.json"), serde_json::to_vec(&failed_job())?)?;
            fs::write(root.join("pods.json"), serde_json::to_vec(&terminated_pods())?)?;
            executable(
                &root.join("kubectl"),
                &format!(
                    "#!/bin/sh\ncase \"$1 $2\" in create*) cat > '{0}/manifest.json'; cat '{0}/created-job.json';; 'get job') cat '{0}/job.json';; 'get pods') cat '{0}/pods.json';; logs*) printf 'initializer failed before table receipt\\n';; *) exit 44;; esac\n",
                    root.display()
                ),
            )?;
            let output = child(&root, "termination")?.output()?;
            println!(
                "failed initializer cleanup probe {variant}: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            ensure!(
                output.status.success(),
                "terminated failed initializer was not positively resolved"
            );
            fs::remove_dir_all(&root)?;
        }
        Ok(())
    }

    #[test]
    fn uncertain_initializer_termination_never_confirms_cleanup() -> Result<()> {
        let cases = [
            "active",
            "ready",
            "active-nonint",
            "active-null",
            "ready-nonint",
            "ready-null",
            "terminating-1",
            "terminating-nonint",
            "terminating-null",
            "uncounted-nonempty",
            "uncounted-failed-nonempty",
            "uncounted-nonobject",
            "uncounted-null",
            "uncounted-list-null",
            "uncounted-list-nonarray",
            "uncounted-extra-field",
            "foreign-job",
            "no-failed-condition",
            "running-pod",
            "missing-phase",
            "foreign-label",
            "foreign-owner",
            "missing-owner",
            "missing-app-status",
            "missing-init-status",
            "waiting",
            "running",
            "missing-container-id",
            "missing-exit-code",
            "missing-finished-at",
            "empty-pods",
            "second-live-pod",
            "second-live-container",
            "api-error",
            "parse-error",
            "pods-api-error",
            "pods-parse-error",
            "timeout",
        ];
        let root = std::env::temp_dir().join(format!("pin-uncertain-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        fs::write(root.join("created-job.json"), serde_json::to_vec(&failed_job())?)?;
        for case in cases {
            let mut job = failed_job();
            let mut pods = terminated_pods();
            match case {
                "active" => job["status"]["active"] = json!(1),
                "ready" => job["status"]["ready"] = json!(1),
                "active-nonint" => job["status"]["active"] = json!("0"),
                "active-null" => job["status"]["active"] = Value::Null,
                "ready-nonint" => job["status"]["ready"] = json!(0.0),
                "ready-null" => job["status"]["ready"] = Value::Null,
                "terminating-1" => job["status"]["terminating"] = json!(1),
                "terminating-nonint" => job["status"]["terminating"] = json!("0"),
                "terminating-null" => job["status"]["terminating"] = Value::Null,
                "uncounted-nonempty" => job["status"]["uncountedTerminatedPods"] = json!({"succeeded":[RUN]}),
                "uncounted-failed-nonempty" => job["status"]["uncountedTerminatedPods"] = json!({"failed":[RUN]}),
                "uncounted-nonobject" => job["status"]["uncountedTerminatedPods"] = json!([]),
                "uncounted-null" => job["status"]["uncountedTerminatedPods"] = Value::Null,
                "uncounted-list-null" => job["status"]["uncountedTerminatedPods"] = json!({"failed":null}),
                "uncounted-list-nonarray" => job["status"]["uncountedTerminatedPods"] = json!({"failed":""}),
                "uncounted-extra-field" => job["status"]["uncountedTerminatedPods"] = json!({"other":[]}),
                "foreign-job" => job["metadata"]["uid"] = json!(RUN),
                "no-failed-condition" => job["status"]["conditions"] = json!([]),
                "running-pod" => pods["items"][0]["status"]["phase"] = json!("Running"),
                "missing-phase" => {
                    pods["items"][0]["status"]
                        .as_object_mut()
                        .context("fixture pod status")?
                        .remove("phase");
                }
                "foreign-label" => pods["items"][0]["metadata"]["labels"]["controller-uid"] = json!(RUN),
                "foreign-owner" => pods["items"][0]["metadata"]["ownerReferences"][0]["uid"] = json!(RUN),
                "missing-owner" => pods["items"][0]["metadata"]["ownerReferences"] = json!([]),
                "missing-app-status" => {
                    pods["items"][0]["status"]
                        .as_object_mut()
                        .context("fixture pod status")?
                        .remove("containerStatuses");
                }
                "missing-init-status" => {
                    pods["items"][0]["status"]
                        .as_object_mut()
                        .context("fixture pod status")?
                        .remove("initContainerStatuses");
                }
                "waiting" | "running" => pods["items"][0]["status"]["containerStatuses"][0]["state"] = json!({case:{}}),
                "missing-container-id" => {
                    pods["items"][0]["status"]["containerStatuses"][0]["state"]["terminated"]
                        .as_object_mut()
                        .context("fixture terminated state")?
                        .remove("containerID");
                }
                "missing-exit-code" => {
                    pods["items"][0]["status"]["containerStatuses"][0]["state"]["terminated"]
                        .as_object_mut()
                        .context("fixture terminated state")?
                        .remove("exitCode");
                }
                "missing-finished-at" => {
                    pods["items"][0]["status"]["containerStatuses"][0]["state"]["terminated"]
                        .as_object_mut()
                        .context("fixture terminated state")?
                        .remove("finishedAt");
                }
                "empty-pods" => pods["items"] = json!([]),
                "second-live-pod" => {
                    let mut other = pods["items"][0].clone();
                    other["metadata"]["uid"] = json!(RUN);
                    other["status"]["phase"] = json!("Running");
                    pods["items"].as_array_mut().context("fixture pods")?.push(other);
                }
                "second-live-container" => {
                    pods["items"][0]["spec"]["containers"]
                        .as_array_mut()
                        .context("fixture containers")?
                        .push(json!({"name":"other"}));
                    pods["items"][0]["status"]["containerStatuses"]
                        .as_array_mut()
                        .context("fixture statuses")?
                        .push(json!({"name":"other","state":{"running":{}}}));
                }
                "timeout" => {
                    job["status"]["failed"] = json!(0);
                    job["status"]["conditions"] = json!([]);
                }
                "api-error" | "parse-error" | "pods-api-error" | "pods-parse-error" => {}
                _ => unreachable!(),
            }
            fs::write(root.join("job.json"), serde_json::to_vec(&job)?)?;
            fs::write(root.join("pods.json"), serde_json::to_vec(&pods)?)?;
            let script = match case {
                "api-error" => "#!/bin/sh\nexit 45\n".to_owned(),
                _ => format!(
                    "#!/bin/sh\ncase \"$1 $2\" in create*) cat > '{0}/manifest.json'; cat '{0}/created-job.json';; 'get job') {1};; 'get pods') {2};; logs*) printf 'failed\\n';; *) exit 44;; esac\n",
                    root.display(),
                    if case == "parse-error" {
                        "printf not-json".to_owned()
                    } else {
                        format!("cat '{}/job.json'", root.display())
                    },
                    match case {
                        "pods-api-error" => "exit 45".to_owned(),
                        "pods-parse-error" => "printf not-json".to_owned(),
                        _ => format!("cat '{}/pods.json'", root.display()),
                    }
                ),
            };
            executable(&root.join("kubectl"), &script)?;
            let output = child(&root, "termination")?
                .env("PIN_INIT_EXPECT_UNCONFIRMED", case)
                .output()?;
            ensure!(
                output.status.success(),
                "{case} incorrectly confirmed cleanup: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            ensure!(
                !root.join("run-table-initializer-termination.json").exists(),
                "{case} wrote positive evidence"
            );
        }
        fs::remove_dir_all(&root)?;
        Ok(())
    }

    #[test]
    fn initializer_ownership_rejects_missing_or_inconsistent_runtime_identity() -> Result<()> {
        let root = std::env::temp_dir().join(format!("pin-identity-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        let (config, platform, valid) = fixture(&root)?;
        for (key, bad) in [
            ("subscriptionId", Value::Null),
            ("subscriptionId", json!("not-a-uuid")),
            ("stamp", Value::Null),
            ("stamp", json!("99999")),
            ("stamp", json!("abcde")),
            ("deltaRoot", json!("abfss://onelake@foreign.dfs.core.windows.net/table")),
            ("namespace", json!("foreign")),
            ("oatRunId", json!(JOB_UID)),
        ] {
            let mut runtime = valid.clone();
            runtime[key] = bad;
            fs::write(root.join("runtime-config.json"), serde_json::to_vec(&runtime)?)?;
            assert!(
                run_table_initializer_cloud_env(&config, &platform, RUN).is_err(),
                "{key}"
            );
        }
        let mut mismatched_platform = platform;
        mismatched_platform.delta_root =
            Some("abfss://onelake@arrowcollector99999.dfs.core.windows.net/table".to_owned());
        let mut runtime = valid;
        runtime["deltaRoot"] = json!(mismatched_platform.delta_root);
        fs::write(root.join("runtime-config.json"), serde_json::to_vec(&runtime)?)?;
        assert!(run_table_initializer_cloud_env(&config, &mismatched_platform, RUN).is_err());
        fs::remove_dir_all(&root)?;
        Ok(())
    }

    #[tokio::test]
    async fn isolated_pin_init_probe() -> Result<()> {
        let Some(root) = std::env::var_os("PIN_INIT_PROBE_ROOT").map(PathBuf::from) else {
            return Ok(());
        };
        match std::env::var("PIN_INIT_PROBE_MODE")?.as_str() {
            "emit" => {
                let (config, mut platform, _) = fixture(&root)?;
                let result = run_aks_pin_setup_job(
                    &config,
                    &mut platform,
                    RUN,
                    &expected_pin_keys_json(&config.tables)?,
                    60_001,
                    1,
                )
                .await;
                ensure!(result.is_err(), "capture boundary unexpectedly succeeded");
                ensure!(
                    root.join("manifest.json").is_file(),
                    "controller did not reach actual Job emission: {result:?}"
                );
            }
            "ownership" => {
                let root = Url::parse(&std::env::var("ARROWCOL_DELTA_ROOT")?)?;
                let result = arrow_deltalake::publisher::PublisherSession::open_async(&root).await;
                let error = result
                    .err()
                    .context("offline credentials unexpectedly registered publisher")?;
                println!("REAL_OWNERSHIP_GATE_ERROR={error:#}");
                ensure!(root.scheme() == "abfss");
            }
            "termination" => {
                let (config, mut platform, _) = fixture(&root)?;
                platform.run_table_initialization_attempted = true;
                let negative = std::env::var("PIN_INIT_EXPECT_UNCONFIRMED").ok();
                let deadline = tokio::time::Instant::now()
                    + if negative.as_deref() == Some("timeout") {
                        Duration::ZERO
                    } else {
                        Duration::from_secs(200)
                    };
                let result = if negative.as_deref() == Some("timeout") {
                    wait_for_aks_pin_setup(
                        &config,
                        &mut platform,
                        &format!("oat-pin-init-{}", uuid::Uuid::parse_str(RUN)?.simple()),
                        Some(JOB_UID),
                        RUN,
                        Some("telemetry.sqlserver.sqlserver_availability_replica_states"),
                        deadline,
                    )
                    .await
                } else {
                    run_aks_pin_setup_job(
                        &config,
                        &mut platform,
                        RUN,
                        &expected_pin_keys_json(&config.tables)?,
                        60_001,
                        1,
                    )
                    .await
                };
                ensure!(result.is_err(), "failed benchmark was incorrectly initialized");
                platform.namespace_created = false;
                platform.namespace_create_attempted = false;
                let cleanup = cleanup_aks_platform(&config, &mut platform).await;
                let aks_cleanup_confirmed = config.platform.kind == crate::config::OatMode::Aks && cleanup.is_ok();
                println!("AKS_CLEANUP_CONFIRMED={aks_cleanup_confirmed}");
                if negative.is_some() {
                    ensure!(!platform.run_table_initialization_drained);
                    ensure!(!aks_cleanup_confirmed);
                } else {
                    ensure!(
                        aks_cleanup_confirmed,
                        "physically-clean failed initialization must confirm cleanup: {cleanup:?}"
                    );
                    let evidence: Value =
                        serde_json::from_slice(&fs::read(root.join("run-table-initializer-termination.json"))?)?;
                    assert_eq!(evidence["kind"], "initializer_terminated_without_receipt");
                    assert_eq!(evidence["job_uid"], JOB_UID);
                    assert_eq!(evidence["pods"][0]["containers"][0]["exit_code"], 1);
                    ensure!(!serde_json::to_string(&evidence)?.contains("oat_run_table_initialized"));
                    ensure!(
                        root.join("run-table-initializer-termination.json").is_file(),
                        "normal successful cleanup deleted its positive termination evidence"
                    );
                    ensure!(
                        !root.join("runtime-config.json").exists(),
                        "private runtime config was not pruned"
                    );
                }
            }
            _ => bail!("unknown isolated probe mode"),
        }
        Ok(())
    }
}
