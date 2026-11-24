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

#[cfg(test)]
fn validate_catalog_registration_receipt(logs: &str, run_id: &str, table: &str, delta_root: &str) -> Result<Value> {
    validate_catalog_registration_receipt_with_properties(
        logs,
        run_id,
        table,
        delta_root,
        &InitialDeltaProperties::default(),
    )
}

fn validate_catalog_registration_receipt_with_properties(
    logs: &str,
    run_id: &str,
    table: &str,
    delta_root: &str,
    properties: &InitialDeltaProperties,
) -> Result<Value> {
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
    verify_initial_delta_property_readback(&receipt["properties"], properties)?;
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

#[derive(Clone, Copy)]
struct CatalogRegistrationDeadline(tokio::time::Instant);

impl CatalogRegistrationDeadline {
    async fn run<T>(&self, future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        let expired = || {
            anyhow::anyhow!(
                "catalog registration whole-stage deadline exceeded; maintenance and load admission are forbidden"
            )
        };
        if tokio::time::Instant::now() >= self.0 {
            return Err(expired());
        }
        let result = tokio::time::timeout_at(self.0, future).await.map_err(|_| expired())?;
        if tokio::time::Instant::now() >= self.0 {
            return Err(expired());
        }
        result
    }

    async fn kubectl(&self, config: &OatConfig, args: Vec<&str>, stdin: Option<Vec<u8>>) -> Result<String> {
        self.run(kubectl(config, args, stdin)).await
    }
}

fn registration_pod_identity(pod: &Value, namespace: &str, job_name: &str, job_uid: &str) -> Result<(String, String)> {
    if pod["metadata"]["namespace"] != namespace
        || !pod
            .pointer("/metadata/ownerReferences")
            .and_then(Value::as_array)
            .is_some_and(|owners| {
                owners.iter().any(|owner| {
                    owner["kind"] == "Job"
                        && owner["name"] == job_name
                        && owner["uid"] == job_uid
                        && owner["controller"] == true
                })
            })
        || !pod
            .pointer("/spec/containers")
            .and_then(Value::as_array)
            .is_some_and(|containers| {
                containers
                    .iter()
                    .any(|container| container["name"] == "catalog-registration")
            })
    {
        bail!("catalog registration Pod is not controlled by the recorded Job UID");
    }
    let name = pod
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .context("catalog registration Pod name missing")?;
    let uid = pod
        .pointer("/metadata/uid")
        .and_then(Value::as_str)
        .context("catalog registration Pod UID missing")?;
    uuid::Uuid::parse_str(uid).context("catalog registration Pod UID invalid")?;
    Ok((name.to_owned(), uid.to_owned()))
}

async fn registration_job_logs(
    deadline: CatalogRegistrationDeadline,
    config: &OatConfig,
    job_name: &str,
    job_uid: &str,
) -> Result<(String, String)> {
    let selector = format!("controller-uid={job_uid}");
    let output = deadline
        .kubectl(
            config,
            vec![
                "get",
                "pods",
                "--namespace",
                &config.namespace,
                "--selector",
                &selector,
                "-o",
                "json",
            ],
            None,
        )
        .await
        .context("select catalog registration Pod by recorded Job UID")?;
    let list: Value = serde_json::from_str(&output).context("parse catalog registration Pod selection")?;
    let pods = list
        .get("items")
        .and_then(Value::as_array)
        .context("registration Pod list missing")?;
    let [pod] = pods.as_slice() else {
        bail!("catalog registration requires exactly one Pod belonging to the recorded Job UID");
    };
    let (name, uid) = registration_pod_identity(pod, &config.namespace, job_name, job_uid)?;
    let logs = deadline
        .kubectl(
            config,
            vec![
                "logs",
                "--namespace",
                &config.namespace,
                "--selector",
                &selector,
                "--container",
                "catalog-registration",
                "--tail=-1",
                "--max-log-requests=1",
            ],
            None,
        )
        .await
        .context("read UID-bound catalog registration Pod logs")?;
    let observed = deadline
        .kubectl(
            config,
            vec![
                "get",
                "pods",
                "--namespace",
                &config.namespace,
                "--selector",
                &selector,
                "-o",
                "json",
            ],
            None,
        )
        .await
        .context("recheck catalog registration Pod identity after log read")?;
    let observed: Value = serde_json::from_str(&observed).context("parse registration Pod identity recheck")?;
    let remaining = observed
        .get("items")
        .and_then(Value::as_array)
        .context("registration Pod recheck list missing")?;
    let [observed] = remaining.as_slice() else {
        bail!("catalog registration Pod selection changed during log read; receipt remains unconfirmed");
    };
    if registration_pod_identity(observed, &config.namespace, job_name, job_uid)? != (name, uid.clone()) {
        bail!("catalog registration Pod was replaced during log read; receipt remains unconfirmed");
    }
    Ok((logs, uid))
}

async fn delete_catalog_registration_job_with_uid(
    api_url: &str,
    namespace: &str,
    job_name: &str,
    uid: &str,
) -> Result<()> {
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .no_proxy()
        .build()?
        .delete(format!(
            "{}/apis/batch/v1/namespaces/{namespace}/jobs/{job_name}",
            api_url.trim_end_matches('/')
        ))
        .json(&json!({
            "apiVersion":"v1", "kind":"DeleteOptions",
            "propagationPolicy":"Foreground", "preconditions":{"uid":uid},
        }))
        .send()
        .await
        .context("send UID-preconditioned catalog registration Job deletion")?;
    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .context("read catalog registration deletion failure")?;
        bail!("UID-preconditioned catalog registration Job deletion returned HTTP {status}: {body}");
    }
    Ok(())
}

async fn run_aks_catalog_registration_job(config: &OatConfig, platform: &mut AksPlatform, run_id: &str) -> Result<()> {
    if config.campaign_profile != Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
        return Ok(());
    }
    let deadline = CatalogRegistrationDeadline(
        tokio::time::Instant::now() + Duration::from_secs(crate::config::CATALOG_REGISTRATION_SETUP_TIMEOUT_SECONDS),
    );
    deadline
        .run(run_aks_catalog_registration_stage(config, platform, run_id, deadline))
        .await
}

async fn run_aks_catalog_registration_stage(
    config: &OatConfig,
    platform: &mut AksPlatform,
    run_id: &str,
    deadline: CatalogRegistrationDeadline,
) -> Result<()> {
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
            stop_grace_seconds: config.client_stop_grace_seconds(),
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
    deadline
        .run(prepare_run_owned_resources(config, platform, items))
        .await?;
    let planned = items.clone();
    let created = deadline
        .kubectl(
            config,
            vec!["create", "-f", "-", "-o", "json"],
            Some(serde_json::to_vec(&manifest)?),
        )
        .await
        .context("create finite catalog registration Job")?;
    let uid = created_pin_init_job_uid(&created, &config.namespace, &job_name, run_id)
        .context("bind catalog registration Job UID")?;
    deadline
        .run(record_applied_resource_uids(config, platform, &planned))
        .await?;
    loop {
        let output = deadline
            .kubectl(
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
        if failed || completed {
            let (logs, pod_uid) = registration_job_logs(deadline, config, &job_name, &uid).await?;
            if failed || !completed {
                bail!(
                    "catalog registration Job {job_name} {} before maintenance admission: {logs}",
                    if failed { "failed" } else { "timed out" }
                );
            }
            let receipt = validate_catalog_registration_receipt_with_properties(
                &logs,
                run_id,
                &table,
                &delta_root,
                &config.initial_delta_properties()?,
            )?;
            let (proxy, api_url) = if FIXTURE_BOUNDARY.try_with(|_| ()).is_ok() {
                (
                    None,
                    FIXTURE_BOUNDARY.with(|boundary| boundary.authority.server.clone()),
                )
            } else {
                let (proxy, api_url) = deadline.run(start_kubectl_api_proxy(config)).await?;
                (Some(proxy), api_url)
            };
            let deleted = deadline
                .run(delete_catalog_registration_job_with_uid(
                    &api_url,
                    &config.namespace,
                    &job_name,
                    &uid,
                ))
                .await;
            let stopped = if let Some(proxy) = proxy {
                deadline
                    .run(async {
                        let stderr = stop_kubectl_process(proxy).await?;
                        collect_kubectl_stderr(stderr).await?;
                        Ok(())
                    })
                    .await
            } else {
                Ok(())
            };
            match (deleted, stopped) {
                (Err(error), Err(stop_error)) => {
                    return Err(error.context(format!(
                        "also failed to stop catalog registration deletion API proxy: {stop_error:#}"
                    )));
                }
                (Err(error), _) => return Err(error),
                (Ok(()), Err(error)) => return Err(error).context("stop catalog registration deletion API proxy"),
                (Ok(()), Ok(())) => {}
            }
            loop {
                let remaining_job = deadline
                    .kubectl(
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
                if remaining_job.trim().is_empty() {
                    break;
                }
                let remaining: Value =
                    serde_json::from_str(&remaining_job).context("parse deleting catalog registration Job")?;
                if remaining.pointer("/metadata/uid").and_then(Value::as_str) != Some(uid.as_str()) {
                    bail!("catalog registration Job was replaced during deletion; admission remains forbidden");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let pods = deadline
                .kubectl(
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
                    "receipt":receipt,"job_name":job_name,"job_uid":uid,"pod_uid":pod_uid,
                    "job_deleted":true,"remaining_pods":0
                }))?,
            )
            .context("persist verified catalog registration and Pod absence receipt")?;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod catalog_registration_tests {
    use super::*;

    #[tokio::test]
    async fn registration_deadline_cancels_stalled_work_without_resetting_between_calls() -> Result<()> {
        struct Cancelled(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Cancelled {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guard = Cancelled(Arc::clone(&cancelled));
        let deadline = CatalogRegistrationDeadline(tokio::time::Instant::now() + Duration::from_millis(30));
        let result = deadline
            .run(async {
                deadline.run(async { Ok(()) }).await?;
                deadline
                    .run(async move {
                        let _guard = guard;
                        std::future::pending::<Result<()>>().await
                    })
                    .await
            })
            .await;
        let error = result.err().context("stalled registration unexpectedly completed")?;
        assert!(format!("{error:#}").contains("whole-stage"));
        assert!(cancelled.load(std::sync::atomic::Ordering::SeqCst));
        assert!(deadline.run(async { Ok(()) }).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn registration_deadline_rejects_ready_completion_returned_after_expiry() -> Result<()> {
        let deadline = CatalogRegistrationDeadline(tokio::time::Instant::now() + Duration::from_millis(50));
        let result = deadline
            .run(async {
                std::thread::sleep(Duration::from_millis(100));
                Ok(())
            })
            .await;
        assert!(result.is_err(), "late ready completion must not authorize maintenance");
        Ok(())
    }

    #[test]
    fn registration_logs_require_the_recorded_controller_and_pod_identity() -> Result<()> {
        let pod = json!({
            "metadata": {
                "namespace":"cell", "name":"register-pod",
                "uid":"22222222-2222-4222-8222-222222222222",
                "ownerReferences":[{"kind":"Job","name":"register","uid":"original","controller":true}]
            },
            "spec":{"containers":[{"name":"catalog-registration"}]}
        });
        registration_pod_identity(&pod, "cell", "register", "original")?;
        assert!(registration_pod_identity(&pod, "cell", "register", "replacement").is_err());
        let mut foreign = pod.clone();
        foreign["metadata"]["ownerReferences"][0]["controller"] = json!(false);
        assert!(registration_pod_identity(&foreign, "cell", "register", "original").is_err());
        foreign = pod.clone();
        foreign["spec"]["containers"][0]["name"] = json!("client");
        assert!(registration_pod_identity(&foreign, "cell", "register", "original").is_err());
        foreign = pod.clone();
        foreign["metadata"]["uid"] = json!("invalid");
        assert!(registration_pod_identity(&foreign, "cell", "register", "original").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn registration_delete_preserves_a_replacement_job_on_uid_conflict() -> Result<()> {
        for replacement in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await?;
                let mut request = Vec::new();
                let mut buffer = [0_u8; 2048];
                let (header_end, length) = loop {
                    let read = stream.read(&mut buffer).await?;
                    if read == 0 {
                        bail!("incomplete registration deletion request");
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&request[..end]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().ok())
                                    .flatten()
                            })
                            .context("registration deletion Content-Length missing")?;
                        if request.len() >= end + 4 + length {
                            break (end, length);
                        }
                    }
                };
                assert!(request.starts_with(b"DELETE /apis/batch/v1/namespaces/cell/jobs/register HTTP/1.1\r\n"));
                let options: Value = serde_json::from_slice(&request[header_end + 4..header_end + 4 + length])?;
                assert_eq!(options["preconditions"]["uid"], "original");
                assert_eq!(options["propagationPolicy"], "Foreground");
                let current_uid = if replacement { "replacement" } else { "original" };
                let deleted = options["preconditions"]["uid"] == current_uid;
                let status = if deleted { "200 OK" } else { "409 Conflict" };
                let body = "{}";
                stream.write_all(format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ).as_bytes()).await?;
                Ok::<_, anyhow::Error>(deleted)
            });
            let result =
                delete_catalog_registration_job_with_uid(&format!("http://{address}"), "cell", "register", "original")
                    .await;
            let deleted = tokio::time::timeout(Duration::from_secs(5), server).await???;
            assert_eq!(deleted, !replacement);
            assert_eq!(result.is_ok(), !replacement);
            if replacement {
                let error = result.err().context("replacement deletion unexpectedly succeeded")?;
                assert!(format!("{error:#}").contains("409 Conflict"));
            }
        }
        Ok(())
    }

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
        let properties = InitialDeltaProperties {
            target_file_size_bytes: Some(67108864),
            checkpoint_interval: Some(17),
        };
        assert!(
            validate_catalog_registration_receipt_with_properties(
                &receipt.to_string(),
                run_id,
                table,
                &root,
                &properties,
            )
            .is_err()
        );
        let mut committed = receipt.clone();
        committed["properties"] = json!(properties.properties()?);
        let observed = validate_catalog_registration_receipt_with_properties(
            &committed.to_string(),
            run_id,
            table,
            &root,
            &properties,
        )?;
        assert_eq!(observed["properties"], committed["properties"]);
        for key in ["delta.targetFileSize", "delta.checkpointInterval"] {
            let mut wrong = committed.clone();
            wrong["properties"][key] = json!("1");
            assert!(
                validate_catalog_registration_receipt_with_properties(
                    &wrong.to_string(),
                    run_id,
                    table,
                    &root,
                    &properties,
                )
                .is_err()
            );
        }
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
                stop_grace_seconds: 30,
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
