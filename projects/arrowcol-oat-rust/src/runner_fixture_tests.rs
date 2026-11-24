#[cfg(test)]
mod fixture_setup_tests {
    use super::*;

    fn input() -> Result<FixtureSetupInput> {
        let identity = CampaignIdentity {
            campaign_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            owner_id: "00000000-0000-0000-0000-000000000002".to_owned(),
            fencing_token: 7,
        };
        let mut inventory = initial_campaign_inventory(
            &identity,
            "00000000-0000-0000-0000-000000000003",
            "00000000-0000-0000-0000-000000000004",
            "fixture-00000000000000000000000000000004",
            "offlinestorage",
            "checkpoints",
            CampaignCellBudget {
                effective_stop_budgets: EffectiveStopBudgets {
                    maintenance_grace_seconds: 4_800,
                    maintenance_stop_timeout_ms: 5_100_000,
                },
                planned_cell_duration_ms: 0,
                drain_budget: InventoryDrainBudget {
                    budget_ms: 120_000,
                    collector_grace_ms: 180_000,
                    pre_stop_capture_margin_ms: crate::config::AKS_PRE_STOP_CAPTURE_MARGIN_MS,
                },
            },
        )?;
        inventory.fic = Some(serde_json::from_value(json!({
            "resource_id":null,"name":null,"state":"planned",
            "plan":{
                "version":1,"runId":inventory.cell_run_id,"namespace":inventory.namespace.name,
                "serviceAccount":"fixture-sa","identityId":"/subscriptions/offline/identities/fixture",
                "resourceId":"/subscriptions/offline/identities/fixture/federatedIdentityCredentials/fixture-fic",
                "clientId":"00000000-0000-0000-0000-000000000005",
                "tenantId":"00000000-0000-0000-0000-000000000006",
                "aksId":"/subscriptions/offline/clusters/fixture","server":"https://offline.invalid",
                "credentialName":"fixture-fic","issuer":"https://offline.invalid/issuer",
                "subject":format!("system:serviceaccount:{}:fixture-sa",inventory.namespace.name),
                "audience":"api://AzureADTokenExchange"
            }
        }))?);
        let authority = FixtureAuthority {
            campaign_id: inventory.campaign_id.clone(),
            cell_run_id: inventory.cell_run_id.clone(),
            owner_id: inventory.owner_id.clone(),
            fencing_token: inventory.fencing_token,
            namespace: inventory.namespace.name.clone(),
            server: "https://offline.invalid".to_owned(),
            context: "reviewed-offline-context".to_owned(),
        };
        let mut manifests = vec![
            json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":authority.namespace}}),
            json!({"apiVersion":"v1","kind":"ServiceAccount","metadata":{"name":"fixture-sa","namespace":authority.namespace},
                "automountServiceAccountToken":false}),
        ];
        let image = format!("registry.k8s.io/pause@sha256:{}", "a".repeat(64));
        manifests.extend(
            ["client", "collector", "maintenance"]
                .map(|role| fixture_deployment(&authority.namespace, role, "fixture-sa", &image)),
        );
        Ok(FixtureSetupInput {
            schema_version: 1,
            authority,
            original_deadline_unix_ms: u64::try_from(unix_time_ms())? + 120_000,
            inventory,
            service_account: "fixture-sa".to_owned(),
            manifests,
            runtime_directory: std::env::temp_dir().join(format!("oat-fixture-{}", uuid::Uuid::new_v4())),
            stamp_config_path: PathBuf::from("/offline/stamp.yaml"),
            bootstrap_env_file: PathBuf::from("/offline/private.env"),
        })
    }

    #[derive(Default)]
    struct RecordingState {
        requests: Vec<FixtureCommandRequest>,
        inventory: Option<RunInventory>,
        created: Vec<Value>,
        fault: &'static str,
    }

    #[derive(Default)]
    struct RecordingTransport(std::sync::Mutex<RecordingState>);

    fn observed(mut value: Value, index: usize) -> Value {
        value["metadata"]["uid"] = json!(format!("00000000-0000-0000-0000-{:012}", 100 + index));
        value["metadata"]["resourceVersion"] = json!("17");
        if value["kind"] == "Deployment" {
            value["metadata"]["generation"] = json!(1);
        }
        value
    }

    #[async_trait::async_trait]
    impl FixtureCommandTransport for RecordingTransport {
        async fn exchange(&self, request: &FixtureCommandRequest) -> Result<FixtureCommandResponse> {
            let mut state = self.0.lock().map_err(|error| anyhow::anyhow!("{error}"))?;
            assert_eq!(request.request_sha256, request.digest()?);
            assert_eq!(
                request.stdin_sha256,
                fixture_sha256(request.stdin.as_deref().unwrap_or_default().as_bytes())
            );
            let body = request
                .stdin
                .as_deref()
                .map(serde_json::from_str::<Value>)
                .transpose()?;
            let stdout = if request.executable == "npx" {
                assert!(request.argv.iter().any(|arg| arg == "publish-inventory"));
                let inventory: RunInventory = serde_json::from_value(body.context("inventory stdin missing")?)?;
                assert_eq!(inventory.load_admission, InventoryLoadAdmission::NotAuthorized);
                let receipt = json!({"ok":true,"receipt":{
                    "status":"persisted","campaign_id":inventory.campaign_id,"owner_id":inventory.owner_id,
                    "fencing_token":inventory.fencing_token,"orchestration_run_id":inventory.orchestration_run_id,
                    "cell_run_id":inventory.cell_run_id,"namespace_uid":inventory.namespace.uid,
                    "resource_count":inventory.resources.len(),"fic_state":inventory.fic.as_ref().map(|fic|fic.state),
                    "qap_state":inventory.qap.as_ref().map(|qap|qap.state),"etag":"\"offline-only-etag\""
                }});
                state.inventory = Some(inventory);
                serde_json::to_string(&receipt)?
            } else {
                assert_eq!(request.executable, "kubectl");
                assert_eq!(
                    &request.argv[..4],
                    [
                        "--context",
                        "reviewed-offline-context",
                        "--server",
                        "https://offline.invalid"
                    ]
                );
                let args = &request.argv[4..];
                match args[0].as_str() {
                    "get" if args[1] == "namespace" => {
                        assert!(state.inventory.is_some(), "planned inventory must precede Kubernetes");
                        if let Some(namespace) = state.created.first() {
                            serde_json::to_string(namespace)?
                        } else if state.fault == "namespace_collision" {
                            "{}".to_owned()
                        } else {
                            String::new()
                        }
                    }
                    "get" if args.iter().any(|arg| arg == "--ignore-not-found") => {
                        if state.fault == "resource_collision" {
                            serde_json::to_string(&json!({"items":[{"kind":"Deployment"}]}))?
                        } else {
                            "{\"items\":[]}".to_owned()
                        }
                    }
                    "create" => {
                        let inventory = state.inventory.as_ref().context("create before planned inventory")?;
                        let body = body.context("create body missing")?;
                        if body["kind"] == "Namespace" {
                            assert!(inventory.namespace.uid.is_none());
                            let actual = observed(body, 0);
                            state.created.push(actual.clone());
                            serde_json::to_string(&actual)?
                        } else {
                            assert!(inventory.namespace.uid.is_some());
                            assert_eq!(inventory.resources.len(), 4);
                            assert!(
                                inventory
                                    .resources
                                    .iter()
                                    .all(|item| item.state == InventoryResourceState::Planned)
                            );
                            let actual = body["items"]
                                .as_array()
                                .context("List expected")?
                                .iter()
                                .enumerate()
                                .map(|(index, item)| observed(item.clone(), index + 1))
                                .collect::<Vec<_>>();
                            state.created.extend(actual.clone());
                            actual
                                .iter()
                                .map(serde_json::to_string_pretty)
                                .collect::<std::result::Result<Vec<_>, _>>()?
                                .join("\n")
                                + "\n"
                        }
                    }
                    "get" => {
                        let mut actual = state.created[1..].to_vec();
                        if state.fault == "uid_drift" {
                            actual[0]["metadata"]["uid"] = json!("00000000-0000-0000-0000-000000000999");
                        }
                        serde_json::to_string(&json!({"items":actual}))?
                    }
                    "apply" => "normal apply unchanged".to_owned(),
                    _ => bail!("unexpected offline command"),
                }
            };
            let mut response = FixtureCommandResponse {
                schema_version: 1,
                kind: "command_response".to_owned(),
                request_id: request.request_id,
                request_sha256: request.request_sha256.clone(),
                exit_code: 0,
                stdout,
                stderr: String::new(),
                truncated: false,
            };
            if state.fault == "body_binding" {
                response.request_sha256 = "wrong".to_owned();
            }
            state.requests.push(request.clone());
            Ok(response)
        }
    }

    #[tokio::test]
    async fn shared_fixture_path_publishes_plans_then_binds_real_transport_responses_without_admission() -> Result<()> {
        let input = input()?;
        let root = input.runtime_directory.clone();
        let transport = Arc::new(RecordingTransport::default());
        let result =
            fixture_setup_with_transport(input, Some(Arc::clone(&transport) as Arc<dyn FixtureCommandTransport>)).await;
        fs::remove_dir_all(&root)?;
        let receipt = result?;
        assert_eq!(receipt["load_admitted"], false);
        let inventory: RunInventory = serde_json::from_value(receipt["inventory"].clone())?;
        assert_eq!(inventory.load_admission, InventoryLoadAdmission::NotAuthorized);
        assert_eq!(inventory.resources.len(), 4);
        assert_eq!(inventory.resources[1].role, InventoryResourceRole::Client);
        assert!(
            inventory
                .resources
                .iter()
                .all(|item| item.state == InventoryResourceState::Observed)
        );
        let state = transport.0.lock().map_err(|error| anyhow::anyhow!("{error}"))?;
        assert_eq!(
            state
                .requests
                .iter()
                .filter(|request| request.argv.iter().any(|arg| arg == "create"))
                .count(),
            2
        );
        assert!(state.requests.iter().all(|request| {
            !request
                .argv
                .iter()
                .any(|arg| matches!(arg.as_str(), "arm" | "assert-ready" | "apply"))
        }));
        println!(
            "E1_FIXTURE_COMMAND_CONTRACT={}",
            serde_json::to_string(&state.requests[0])?
        );
        Ok(())
    }

    #[tokio::test]
    async fn refuses_collisions_and_unbound_or_changed_responses_without_adopting_resources() -> Result<()> {
        for fault in ["namespace_collision", "resource_collision", "body_binding", "uid_drift"] {
            let input = input()?;
            let root = input.runtime_directory.clone();
            let transport = Arc::new(RecordingTransport::default());
            transport.0.lock().map_err(|error| anyhow::anyhow!("{error}"))?.fault = fault;
            assert!(
                fixture_setup_with_transport(input, Some(Arc::clone(&transport) as Arc<dyn FixtureCommandTransport>))
                    .await
                    .is_err(),
                "{fault}"
            );
            fs::remove_dir_all(&root)?;
            let state = transport.0.lock().map_err(|error| anyhow::anyhow!("{error}"))?;
            let creates = state
                .requests
                .iter()
                .filter(|request| request.argv.iter().any(|arg| arg == "create"))
                .count();
            if fault == "namespace_collision" || fault == "body_binding" {
                assert_eq!(creates, 0);
            }
            if fault == "resource_collision" {
                assert_eq!(creates, 1);
            }
            if fault == "uid_drift" {
                assert!(
                    state
                        .inventory
                        .as_ref()
                        .is_some_and(|inventory| inventory.resources.iter().all(|resource| resource.uid.is_none()))
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn rejects_invalid_scope_or_missing_bridge_before_any_dispatch() -> Result<()> {
        let input = input()?;
        let root = input.runtime_directory.clone();
        assert!(fixture_setup_with_transport(input, None).await.is_err());
        assert!(!root.exists());
        for mutation in [
            "foreign_namespace",
            "unknown_kind",
            "load",
            "server",
            "owner",
            "extra",
            "workers",
        ] {
            let mut input = self::input()?;
            match mutation {
                "foreign_namespace" => input.manifests[2]["metadata"]["namespace"] = json!("foreign"),
                "unknown_kind" => input.manifests[2]["kind"] = json!("Job"),
                "load" => input.inventory.load_admission = InventoryLoadAdmission::Authorized,
                "server" => input.authority.server = "https://foreign.invalid".to_owned(),
                "owner" => input.authority.owner_id = "00000000-0000-0000-0000-000000000009".to_owned(),
                "extra" => input.manifests.push(json!({"kind":"Secret"})),
                "workers" => input.manifests[2]["spec"]["replicas"] = json!(2),
                _ => unreachable!(),
            }
            let transport = Arc::new(RecordingTransport::default());
            assert!(
                fixture_setup_with_transport(input, Some(Arc::clone(&transport) as Arc<dyn FixtureCommandTransport>))
                    .await
                    .is_err(),
                "{mutation}"
            );
            assert!(
                transport
                    .0
                    .lock()
                    .map_err(|error| anyhow::anyhow!("{error}"))?
                    .requests
                    .is_empty()
            );
        }
        Ok(())
    }

    #[test]
    fn response_binding_refuses_reordering_truncation_and_late_results() -> Result<()> {
        let mut request = FixtureCommandRequest {
            schema_version: 1,
            kind: "command_request".to_owned(),
            request_id: 1,
            authority: input()?.authority,
            original_deadline_unix_ms: u64::try_from(unix_time_ms())? + 60_000,
            remaining_timeout_ms: 60_000,
            executable: "kubectl".to_owned(),
            argv: vec!["create".to_owned()],
            current_dir: None,
            stdin: Some("{}".to_owned()),
            stdin_sha256: fixture_sha256(b"{}"),
            request_sha256: String::new(),
        };
        request.request_sha256 = request.digest()?;
        let mut response = FixtureCommandResponse {
            schema_version: 1,
            kind: "command_response".to_owned(),
            request_id: 1,
            request_sha256: request.request_sha256.clone(),
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
            truncated: false,
        };
        request.validate_response(&response)?;
        response.request_id = 2;
        assert!(request.validate_response(&response).is_err());
        response.request_id = 1;
        response.truncated = true;
        assert!(request.validate_response(&response).is_err());
        response.truncated = false;
        request.original_deadline_unix_ms = 1;
        assert!(request.validate_response(&response).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn ordinary_runtime_helper_retains_apply_verb_and_body() -> Result<()> {
        let input = input()?;
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.namespace = input.authority.namespace.clone();
        config.kubectl_context = Some(input.authority.context.clone());
        config.campaign_profile = None;
        let mut platform = fixture_platform(&input);
        let transport = Arc::new(RecordingTransport::default());
        let boundary = FixtureBoundary {
            authority: input.authority.clone(),
            deadline_unix_ms: input.original_deadline_unix_ms,
            sequence: std::sync::atomic::AtomicU64::new(1),
            transport: Arc::clone(&transport) as Arc<dyn FixtureCommandTransport>,
            responses: tokio::sync::Mutex::new(Vec::new()),
        };
        let resources = vec![input.manifests[2].clone()];
        FIXTURE_BOUNDARY
            .scope(
                boundary,
                apply_runtime_resources(&config, &mut platform, resources.clone()),
            )
            .await?;
        let state = transport.0.lock().map_err(|error| anyhow::anyhow!("{error}"))?;
        assert_eq!(state.requests.len(), 1);
        let request = &state.requests[0];
        assert_eq!(
            &request.argv[4..],
            ["apply", "--namespace", &config.namespace, "-f", "-"]
        );
        let actual: Value = serde_json::from_str(request.stdin.as_deref().context("manifest missing")?)?;
        let mut expected = resources;
        stamp_run_origin_markers(
            &mut expected[0],
            &config.namespace,
            &platform.run_id,
            &platform.namespace_owner_token,
        )?;
        assert_eq!(actual, json!({"apiVersion":"v1","kind":"List","items":expected}));
        Ok(())
    }

    #[test]
    fn command_digest_matches_typescript_canonical_fixture() -> Result<()> {
        let fixture: Value = serde_json::from_str(include_str!("../tests/fixtures/setup-bridge.json"))?;
        assert_eq!(fixture["offline_protocol_fixture_only"], true);
        let request: FixtureCommandRequest = serde_json::from_value(fixture["request"].clone())?;
        let response: FixtureCommandResponse = serde_json::from_value(fixture["response"].clone())?;
        assert_eq!(request.digest()?, request.request_sha256);
        assert_eq!(
            request.stdin_sha256,
            fixture_sha256(request.stdin.as_deref().unwrap_or_default().as_bytes())
        );
        assert_eq!(response.request_sha256, request.request_sha256);
        let mut changed = request.clone();
        changed.argv.push("--unexpected".to_owned());
        assert_ne!(changed.digest()?, request.request_sha256);
        changed = request.clone();
        changed.authority.context = "other-context".to_owned();
        assert_ne!(changed.digest()?, request.request_sha256);
        changed = request.clone();
        changed.stdin = Some("different manifest".to_owned());
        assert_ne!(changed.digest()?, request.request_sha256);
        Ok(())
    }
}
