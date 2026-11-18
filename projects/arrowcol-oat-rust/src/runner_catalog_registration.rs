fn build_aks_catalog_registration_job(spec: AksClientJobSpec<'_>, config_map_name: &str) -> Result<Value> {
    let mut job = build_aks_client_job(spec);
    job["metadata"]["labels"]["arrowcol-oat-role"] = json!("catalog-registration");
    let pod = job
        .pointer_mut("/spec/template")
        .context("catalog registration Job template is missing")?;
    pod["metadata"]["labels"]["arrowcol-oat-role"] = json!("catalog-registration");
    let pod_spec = pod["spec"]
        .as_object_mut()
        .context("catalog registration Pod spec is missing")?;
    pod_spec.remove("topologySpreadConstraints");
    pod_spec.insert("enableServiceLinks".to_owned(), json!(false));
    pod_spec.insert(
        "nodeSelector".to_owned(),
        json!({
            "arrow-collector/role":"arrowcol","kubernetes.io/arch":"amd64"
        }),
    );
    pod_spec.insert(
        "tolerations".to_owned(),
        json!([{
            "key":"arrow-collector/role","operator":"Equal","value":"arrowcol","effect":"NoSchedule"
        }]),
    );
    pod_spec.insert(
        "securityContext".to_owned(),
        json!({
            "runAsNonRoot":true,"runAsUser":10001,"runAsGroup":10001,"fsGroup":10001,
            "seccompProfile":{"type":"RuntimeDefault"}
        }),
    );
    let volumes = pod_spec
        .get_mut("volumes")
        .and_then(Value::as_array_mut)
        .context("catalog registration volumes are missing")?;
    volumes.retain(|volume| volume["name"] != "oat-schedule");
    volumes.push(json!({
        "name":"registration-config",
        "configMap":{"name":config_map_name,"items":[{"key":"register.json","path":"register.json"}]}
    }));
    let container = &mut pod_spec
        .get_mut("containers")
        .and_then(Value::as_array_mut)
        .context("catalog registration containers are missing")?[0];
    container["name"] = json!("catalog-registration");
    container["command"] = json!(["/usr/local/bin/arrowcol-oat-pin-init"]);
    container["args"] = json!([
        "--register-only-config",
        "/run/arrowcol-oat/register.json",
        "--edge-secret-file",
        "/run/secrets/edge-hmac",
        "--internal-secret-file",
        "/run/secrets/internal-hmac"
    ]);
    container["resources"] = json!({
        "requests":{"cpu":"100m","memory":"128Mi"},"limits":{"cpu":"500m","memory":"512Mi"}
    });
    container["securityContext"] = json!({
        "allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true
    });
    let mounts = container["volumeMounts"]
        .as_array_mut()
        .context("catalog registration mounts are missing")?;
    mounts.retain(|mount| mount["name"] != "oat-schedule");
    mounts.push(json!({"name":"registration-config","mountPath":"/run/arrowcol-oat","readOnly":true}));
    Ok(job)
}

fn validate_catalog_registration_receipt(logs: &str, run_id: &str, table: &str, delta_root: &str) -> Result<Value> {
    let receipts = logs
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|value| value["kind"] == "oat_run_table_registered")
        .collect::<Vec<_>>();
    let [receipt] = receipts.as_slice() else {
        bail!("catalog registration Job must emit exactly one registration receipt");
    };
    let expected_location =
        arrowcol_loadgen::initialize::canonical_table_target(&url::Url::parse(delta_root)?, table)?.0;
    let logical = collector_chain::dmv::tables()?
        .into_iter()
        .find(|candidate| candidate.identity().to_string() == table)
        .context("catalog registration table is not a canonical DMV identity")?;
    let fingerprint = arrowcol_protocol::fingerprint_schema(logical.schema().as_ref()).to_string();
    if receipt["run_id"] != run_id
        || receipt["table"] != table
        || receipt["location"] != expected_location.as_str()
        || receipt["provider"] != "delta"
        || receipt["schema_version"] != 1
        || receipt["schema_fingerprint"] != fingerprint
        || receipt["catalog_table_count"] != 1
    {
        bail!(
            "catalog registration receipt does not prove the exact canonical table/schema at the run-scoped location"
        );
    }
    Ok((*receipt).clone())
}

fn verify_catalog_registration_pods_absent(output: &str) -> Result<()> {
    let pods: Value = serde_json::from_str(output).context("parse catalog registration Pod absence readback")?;
    if !pods
        .get("items")
        .and_then(Value::as_array)
        .context("catalog registration Pod absence readback has no items array")?
        .is_empty()
    {
        bail!("catalog registration Pods still exist; maintenance and load admission are forbidden");
    }
    Ok(())
}

async fn run_aks_catalog_registration_job(config: &OatConfig, platform: &mut AksPlatform, run_id: &str) -> Result<()> {
    if config.campaign_profile != Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
        return Ok(());
    }
    let [table] = config.tables.as_slice() else {
        bail!("catalog registration requires exactly one scale-out hot table");
    };
    let table = canonical_dmv_table_name(table)?;
    let delta_root = platform
        .delta_root
        .clone()
        .context("catalog registration run-scoped Delta root is missing")?;
    let suffix = uuid::Uuid::parse_str(run_id)?.simple().to_string();
    let job_name = format!("oat-register-{suffix}");
    let config_map_name = format!("oat-register-config-{suffix}");
    let registration = json!({
        "schema_version":1,"run_id":run_id,"table":table,"delta_root":delta_root,
        "endpoint":platform.workload_endpoint(config),"catalog_endpoint":"http://arrowcol-catalog:8817"
    });
    let job = build_aks_catalog_registration_job(
        AksClientJobSpec {
            namespace: &config.namespace,
            client_image: &config.dmv_client_image,
            router_auth_secret_name: &config.edge_secret_name,
            schedule_configmap_name: &config_map_name,
            run_id,
            run_label: &suffix,
            job_name: &job_name,
            args: &[],
            run_seconds: i64::try_from(crate::config::CATALOG_REGISTRATION_SETUP_TIMEOUT_SECONDS)?,
        },
        &config_map_name,
    )?;
    let mut manifest = json!({
        "apiVersion":"v1","kind":"List","items":[{
            "apiVersion":"v1","kind":"ConfigMap",
            "metadata":{"name":config_map_name,"namespace":config.namespace,
                "labels":{"arrowcol-oat-run":suffix,"arrowcol-oat-role":"catalog-registration"}},
            "data":{"register.json":serde_json::to_string(&registration)?}
        },job]
    });
    let items = manifest["items"]
        .as_array_mut()
        .context("catalog registration resources are missing")?;
    prepare_run_owned_resources(config, platform, items).await?;
    let planned = items.clone();
    let created = kubectl(
        config,
        vec!["create", "-f", "-", "-o", "json"],
        Some(serde_json::to_vec(&manifest)?),
    )
    .await
    .context("create finite catalog registration Job")?;
    let uid = created_pin_init_job_uid(&created, &config.namespace, &job_name, run_id)
        .context("bind catalog registration Job UID")?;
    record_applied_resource_uids(config, platform, &planned).await?;
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(crate::config::CATALOG_REGISTRATION_SETUP_TIMEOUT_SECONDS);
    loop {
        let output = kubectl(
            config,
            vec!["get", "job", &job_name, "--namespace", &config.namespace, "-o", "json"],
            None,
        )
        .await
        .context("inspect catalog registration Job")?;
        let job: Value = serde_json::from_str(&output).context("parse catalog registration Job status")?;
        if job.pointer("/metadata/uid").and_then(Value::as_str) != Some(uid.as_str()) {
            bail!("catalog registration Job UID changed; registration remains unconfirmed");
        }
        let condition = |kind| {
            job.pointer("/status/conditions")
                .and_then(Value::as_array)
                .is_some_and(|conditions| {
                    conditions
                        .iter()
                        .any(|entry| entry["type"] == kind && entry["status"] == "True")
                })
        };
        let failed = condition("Failed")
            || job
                .pointer("/status/failed")
                .and_then(Value::as_u64)
                .is_some_and(|n| n > 0);
        let completed = condition("Complete") && job.pointer("/status/succeeded").and_then(Value::as_u64) == Some(1);
        if failed || completed || tokio::time::Instant::now() >= deadline {
            let logs = kubectl(
                config,
                vec!["logs", "--namespace", &config.namespace, &format!("job/{job_name}")],
                None,
            )
            .await
            .context("read catalog registration Job logs")?;
            if failed || !completed {
                bail!(
                    "catalog registration Job {job_name} {} before maintenance admission: {logs}",
                    if failed { "failed" } else { "timed out" }
                );
            }
            let receipt = validate_catalog_registration_receipt(&logs, run_id, &table, &delta_root)?;
            kubectl(
                config,
                vec![
                    "delete",
                    "job",
                    &job_name,
                    "--namespace",
                    &config.namespace,
                    "--cascade=foreground",
                    "--wait=true",
                ],
                None,
            )
            .await
            .context("delete completed catalog registration Job and dependent Pods")?;
            let remaining_job = kubectl(
                config,
                vec![
                    "get",
                    "job",
                    &job_name,
                    "--namespace",
                    &config.namespace,
                    "--ignore-not-found",
                    "-o",
                    "json",
                ],
                None,
            )
            .await
            .context("confirm catalog registration Job is absent")?;
            if !remaining_job.trim().is_empty() {
                bail!("catalog registration Job still exists; maintenance and load admission are forbidden");
            }
            let pods = kubectl(
                config,
                vec![
                    "get",
                    "pods",
                    "--namespace",
                    &config.namespace,
                    "--selector",
                    &format!("controller-uid={uid}"),
                    "-o",
                    "json",
                ],
                None,
            )
            .await
            .context("confirm catalog registration Pods are absent before maintenance admission")?;
            verify_catalog_registration_pods_absent(&pods)?;
            let runtime_dir = platform
                .runtime_dir
                .as_deref()
                .context("catalog registration evidence directory is missing")?;
            fs::write(
                runtime_dir.join("run-table-catalog-registration.json"),
                serde_json::to_vec_pretty(&json!({
                    "receipt":receipt,"job_name":job_name,"job_uid":uid,"job_deleted":true,"remaining_pods":0
                }))?,
            )
            .context("persist verified catalog registration and Pod absence receipt")?;
            kubectl(
                config,
                vec![
                    "delete",
                    "configmap",
                    &config_map_name,
                    "--namespace",
                    &config.namespace,
                    "--wait=true",
                ],
                None,
            )
            .await
            .context("delete completed catalog registration ConfigMap")?;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod catalog_registration_tests {
    use super::*;

    #[test]
    fn registration_job_lifetime_is_reserved_in_the_scale_out_run_pin_budget() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.platform.kind = crate::config::OatMode::Aks;
        let baseline = config.timing_budget()?;
        config.campaign_profile = Some(crate::config::CampaignProfile::ScaleOutSingleTable);
        assert_eq!(
            config.timing_budget()?.minimum_pin_ttl_ms,
            baseline.minimum_pin_ttl_ms + crate::config::CATALOG_REGISTRATION_SETUP_TIMEOUT_SECONDS * 1_000
        );
        config.platform.kind = crate::config::OatMode::Local;
        assert_eq!(config.timing_budget()?.minimum_pin_ttl_ms, baseline.minimum_pin_ttl_ms);
        Ok(())
    }

    #[test]
    fn registration_receipt_rejects_foreign_root_schema_and_ambiguous_results() -> Result<()> {
        let run_id = "11111111-1111-4111-8111-111111111111";
        let table = "telemetry.sqlserver.sqlserver_availability_replica_states";
        let root = format!("abfss://tables@arrowcollector1.dfs.core.windows.net/runs/{run_id}/dataflow");
        let logical = collector_chain::dmv::tables()?
            .into_iter()
            .find(|candidate| candidate.identity().to_string() == table)
            .context("canonical fixture table missing")?;
        let receipt = json!({
            "kind":"oat_run_table_registered","run_id":run_id,"table":table,"provider":"delta",
            "location":format!("{root}/telemetry/sqlserver/sqlserver_availability_replica_states"),
            "schema_version":1,"schema_fingerprint":arrowcol_protocol::fingerprint_schema(logical.schema()).to_string(),
            "catalog_table_count":1
        });
        validate_catalog_registration_receipt(&receipt.to_string(), run_id, table, &root)?;
        for (key, value) in [
            ("run_id", json!("22222222-2222-4222-8222-222222222222")),
            ("table", json!("telemetry.sqlserver.sqlserver_wait_stats")),
            ("location", json!(format!("{root}/foreign/table"))),
            ("schema_version", json!(2)),
            ("schema_fingerprint", json!("wrong")),
            ("catalog_table_count", json!(2)),
            ("provider", json!("parquet")),
        ] {
            let mut changed = receipt.clone();
            changed[key] = value;
            assert!(
                validate_catalog_registration_receipt(&changed.to_string(), run_id, table, &root).is_err(),
                "{key}"
            );
        }
        assert!(validate_catalog_registration_receipt("", run_id, table, &root).is_err());
        assert!(validate_catalog_registration_receipt(&format!("{receipt}\n{receipt}"), run_id, table, &root).is_err());
        verify_catalog_registration_pods_absent(r#"{"items":[]}"#)?;
        for output in [
            r#"{"items":[{"status":{"phase":"Succeeded"}}]}"#,
            r#"{"items":null}"#,
            "{}",
            "invalid",
        ] {
            assert!(verify_catalog_registration_pods_absent(output).is_err(), "{output}");
        }
        Ok(())
    }

    #[test]
    fn registration_job_has_only_client_signers_and_no_load_barrier_or_storage_identity() -> Result<()> {
        let job = build_aks_catalog_registration_job(
            AksClientJobSpec {
                namespace: "cell",
                client_image: "client:test",
                router_auth_secret_name: "auth",
                schedule_configmap_name: "unused",
                run_id: "run",
                run_label: "register",
                job_name: "register",
                args: &[],
                run_seconds: 180,
            },
            "register-config",
        )?;
        let pod = &job["spec"]["template"]["spec"];
        assert_eq!(
            job["spec"]["template"]["metadata"]["labels"]["app"],
            "arrowcol-oat-client"
        );
        assert_eq!(
            job["spec"]["template"]["metadata"]["labels"]["arrowcol-oat-role"],
            "catalog-registration"
        );
        assert_eq!(pod["automountServiceAccountToken"], false);
        assert!(pod.get("serviceAccountName").is_none());
        let volumes = pod["volumes"].as_array().context("volumes missing")?;
        assert_eq!(volumes.len(), 3);
        assert_eq!(volumes.iter().filter(|v| v.get("secret").is_some()).count(), 2);
        let keys = volumes
            .iter()
            .filter_map(|v| v.pointer("/secret/items/0/key").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert_eq!(keys, ["edge-hmac", "internal-hmac"]);
        for key in ["edge-hmac", "internal-hmac"] {
            let volume = volumes
                .iter()
                .find(|volume| volume["name"] == key)
                .context("signer volume missing")?;
            assert_eq!(volume["secret"]["secretName"], "auth");
            let mount = pod["containers"][0]["volumeMounts"]
                .as_array()
                .context("signer mounts missing")?
                .iter()
                .find(|mount| mount["name"] == key)
                .context("signer mount missing")?;
            assert_eq!(mount["mountPath"], format!("/run/secrets/{key}"));
            assert_eq!(mount["subPath"], key);
            assert_eq!(mount["readOnly"], true);
        }
        let text = serde_json::to_string(&job)?;
        for forbidden in [
            "oat-schedule",
            "migration-password",
            "azure.workload.identity",
            "AZURE_",
            "--rows",
            "--duration-secs",
        ] {
            assert!(!text.contains(forbidden), "{forbidden}");
        }
        Ok(())
    }
}
