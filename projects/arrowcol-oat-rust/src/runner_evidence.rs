use arrowcol_tunables::ClientDataProfile;

const OFFICIAL_MEASUREMENT_MAX_BYTES: usize = 4_194_304;
const OFFICIAL_MEASUREMENT_SCOPE: &str = "original_official_cell_window_not_paired_scale_authorization";
const OFFICIAL_MEASUREMENT_PUBLICATION_SECONDS: u64 = 20;
const OFFICIAL_MEASUREMENT_RETENTION_RESERVE_SECONDS: u64 = 10;

#[derive(Clone, Debug)]
struct SubmittedConfigIdentity {
    orchestration_run_id: String,
    sha256: String,
}

async fn capture_submitted_config_identity(state: &AppState, orchestration_run_id: &str) -> Option<SubmittedConfigIdentity> {
    let runs = state.runs.lock().await;
    let status = runs.get(orchestration_run_id);
    match status {
        Some(status) if status.id == orchestration_run_id && valid_submitted_config_sha256(&status.config_sha256) => {
            Some(SubmittedConfigIdentity { orchestration_run_id: status.id.clone(), sha256: status.config_sha256.clone() })
        }
        _ => {
            tracing::warn!("original server run has no matching authoritative submitted config identity; identity remains UNKNOWN");
            None
        }
    }
}

fn valid_submitted_config_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn official_measurement_blob(inventory: &RunInventory) -> Result<String> {
    let destination = inventory.evidence_destination.as_ref().context("official measurement has no custody destination")?;
    Ok(format!("{}/official-cell-measurement-v1.json", destination.blob_prefix))
}

fn validate_official_measurement_reference(inventory: &RunInventory) -> Result<()> {
    let Some(reference) = &inventory.official_measurement else { return Ok(()) };
    let sha = |value: &str| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if reference.schema_version != 1
        || reference.blob != official_measurement_blob(inventory)?
        || !sha(&reference.sha256)
        || !sha(&reference.effective_config_sha256)
        || reference.submitted_config_sha256.as_deref().is_some_and(|value| !sha(value))
        || reference.source_revision.as_deref().is_some_and(|value| {
            !matches!(value.len(), 40 | 64) || !value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        || reference.byte_length == 0
        || reference.byte_length > OFFICIAL_MEASUREMENT_MAX_BYTES as u64
        || reference.version.as_deref().is_some_and(|value| value.is_empty() || value.len() > 1024 || value.chars().any(char::is_control))
        || match (reference.measurement_start_unix_ms, reference.measurement_end_unix_ms) {
            (None, None) => false,
            (Some(start), Some(end)) => start == 0 || end <= start || end > 9_007_199_254_740_991,
            _ => true,
        }
    {
        bail!("official cell measurement reference is malformed or belongs to another destination");
    }
    validate_campaign_etag(&reference.etag)?;
    Ok(())
}

fn official_measurement_bytes(
    inventory: &RunInventory, config: &OatConfig, rows: &[OatResult], submitted: Option<&SubmittedConfigIdentity>,
) -> Result<Vec<u8>> {
    if config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
        bail!("exploratory scale-out evidence is not custody-qualified");
    }
    let [row] = rows else { bail!("official campaign measurement requires exactly one original hot-table result") };
    let tunables: Value = serde_json::from_str(&row.tunables_json)?;
    if row.run_id != inventory.cell_run_id
        || tunables["orchestration_run_id"] != inventory.orchestration_run_id
        || config.tables.as_slice() != std::slice::from_ref(&row.table)
    {
        bail!("official cell result does not match the original inventory and table");
    }
    let evidence: Value = serde_json::from_str(row.evidence_json.as_deref().context("official result has no evidence")?)?;
    let teardown = evidence["teardown_outcome"].as_str().context("official result has no final teardown outcome")?;
    if !matches!(teardown, "succeeded" | "failed" | "not_started")
        || evidence["durable_commits"]["run_cumulative_output"]["teardown_outcome"] != teardown
        || evidence["commit_window_output_throughput"]["teardown_outcome"] != teardown
        || !evidence["durable_commits"]["complete_30_second_commit_windows"].as_array()
            .is_some_and(|windows| windows.iter().all(|window| window["teardown_outcome"] == teardown))
    {
        bail!("official result lacks consistent final teardown and commit-window evidence");
    }
    if row.status == "succeeded" && (teardown != "succeeded"
        || evidence["durable_commits"]["post_teardown_delta_reconciliation"]["status"] != "matched"
        || evidence["reconciliation"]["status"] != "within_bound"
        || evidence["reconciliation"]["settled"] != true
        || evidence["reconciliation"]["ambiguous_rows"] != 0
        || evidence["durable_commits"]["collector_teardown_admissions"]["status"] != "clear"
        || evidence["durable_commits"]["collector_teardown_admissions"]["cancelled_ambiguous"] != 0)
    {
        bail!("successful official result lacks final settled unambiguous delivery evidence");
    }
    let effective = json!({
        "typed_oat_config": config,
        "resolved_aks_resources": config.effective_aks_resources(),
        "result_url": config.effective_result_url()?,
        "website_url": config.effective_website_url()?,
    });
    let effective_config_json = fixture_canonical_json(&effective)?;
    let source_revision = ((row.git_sha.len() == 40 || row.git_sha.len() == 64)
        && row.git_sha.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
        .then_some(row.git_sha.as_str());
    let payload = json!({
        "schema_version": 1, "scope": OFFICIAL_MEASUREMENT_SCOPE,
        "origin": {
            "campaign_id": inventory.campaign_id, "owner_id": inventory.owner_id, "fencing_token": inventory.fencing_token,
            "orchestration_run_id": inventory.orchestration_run_id, "cell_run_id": inventory.cell_run_id,
            "namespace_uid": inventory.namespace.uid,
        },
        "submitted_config_sha256": submitted.filter(|identity| {
            identity.orchestration_run_id == inventory.orchestration_run_id && valid_submitted_config_sha256(&identity.sha256)
        }).map(|identity| identity.sha256.as_str()),
        "effective_config_json": effective_config_json,
        "effective_config_sha256": fixture_sha256(effective_config_json.as_bytes()),
        "source_revision": source_revision,
        "official_result": {
            "run_id": row.run_id, "table": row.table, "status": row.status,
            "git_sha": row.git_sha, "tunables": tunables, "evidence": evidence,
        },
    });
    let bytes = fixture_canonical_json(&payload)?.into_bytes();
    if bytes.len() > OFFICIAL_MEASUREMENT_MAX_BYTES {
        bail!("official cell measurement exceeds its fixed byte bound");
    }
    Ok(bytes)
}

async fn publish_immutable_official_bytes(
    store: Arc<dyn ObjectStore>,
    path: &ObjectPath,
    bytes: &[u8],
) -> Result<(String, Option<String>)> {
    use futures_util::TryStreamExt;
    if bytes.is_empty() || bytes.len() > OFFICIAL_MEASUREMENT_MAX_BYTES {
        bail!("official measurement publication exceeds its byte bound");
    }
    let before = match store.get_opts(path, GetOptions::default()).await {
        Ok(existing) => Some(existing),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(error) => return Err(error).context("observe original immutable measurement before creation"),
    };
    let object = match before {
        Some(existing) => existing,
        None => {
            store.put_opts(path, PutPayload::from(Bytes::copy_from_slice(bytes)),
                PutOptions { mode: PutMode::Create, ..PutOptions::default() }).await
                .context("create original immutable official measurement once; uncertain outcome is not replayed")?;
            store.get_opts(path, GetOptions::default()).await.context("read back original immutable official measurement")?
        }
    };
    if object.meta.size != bytes.len() as u64 {
        bail!("immutable official measurement length differs from original bytes");
    }
    let etag = object.meta.e_tag.clone().context("immutable measurement has no ETag")?;
    validate_campaign_etag(&etag)?;
    let version = object.meta.version.clone();
    let mut stream = object.into_stream();
    let mut offset = 0;
    while let Some(chunk) = stream.try_next().await? {
        if chunk.len() > bytes.len() - offset || chunk.as_ref() != &bytes[offset..offset + chunk.len()] {
            bail!("immutable official measurement bytes differ; refusing adoption or overwrite");
        }
        offset += chunk.len();
    }
    if offset != bytes.len() {
        bail!("immutable official measurement response was truncated");
    }
    Ok((etag, version))
}

async fn publish_campaign_official_measurement(
    platform: &AksPlatform,
    config: &OatConfig,
    rows: &[OatResult],
    submitted: Option<&SubmittedConfigIdentity>,
    deadline: tokio::time::Instant,
) -> Result<()> {
    if config.campaign_profile.is_none()
        || config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
        return Ok(());
    }
    require_lifecycle_remaining(
        deadline,
        Duration::from_secs(OFFICIAL_MEASUREMENT_PUBLICATION_SECONDS + OFFICIAL_MEASUREMENT_RETENTION_RESERVE_SECONDS),
        "immutable official measurement publication and failure retention",
    )?;
    let publication_deadline = tokio::time::Instant::now() + Duration::from_secs(OFFICIAL_MEASUREMENT_PUBLICATION_SECONDS);
    tokio::time::timeout_at(publication_deadline, async {
        let cached = campaign_inventory_for_setup(platform).await?;
        let identity = CampaignIdentity::from_lookup(|name| std::env::var(name).ok())?;
        let (current, _) = read_authoritative_campaign_inventory(
            platform, &identity, &cached.orchestration_run_id, publication_deadline.saturating_duration_since(tokio::time::Instant::now()),
        ).await?;
        cached.validate_successor(&current)?;
        if rows.iter().any(|row| row.status == "succeeded") {
            for action in [CampaignCleanupAction::StopAdmission, CampaignCleanupAction::BoundedDrain,
                CampaignCleanupAction::EvidenceCapture, CampaignCleanupAction::StopMaintenance,
                CampaignCleanupAction::TerminalCapture, CampaignCleanupAction::RemoveNamespace,
                CampaignCleanupAction::RemoveQap, CampaignCleanupAction::RemoveFic, CampaignCleanupAction::Handoff] {
                campaign_cleanup_step_complete(&action, &current)?;
            }
        }
        let bytes = official_measurement_bytes(&current, config, rows, submitted)?;
        let payload: Value = serde_json::from_slice(&bytes)?;
        let destination = current.evidence_destination.as_ref().context("official measurement destination is missing")?;
        let blob = official_measurement_blob(&current)?;
        let url = Url::parse(&format!("abfss://{}@{}.dfs.core.windows.net/{blob}", destination.container, destination.account))?;
        let store = object_store_for_url(&url)?;
        let (etag, version) = publish_immutable_official_bytes(store, &ObjectPath::from_url_path(url.path())?, &bytes).await?;
        let output = &payload["official_result"]["evidence"]["commit_window_output_throughput"];
        let mut next = current.clone();
        next.official_measurement = Some(InventoryOfficialMeasurement {
            schema_version: 1, blob, etag, version, sha256: fixture_sha256(&bytes), byte_length: bytes.len() as u64,
            effective_config_sha256: payload["effective_config_sha256"].as_str().context("effective config digest missing")?.to_owned(),
            submitted_config_sha256: payload["submitted_config_sha256"].as_str().map(str::to_owned),
            source_revision: payload["source_revision"].as_str().map(str::to_owned),
            measurement_start_unix_ms: output["measurement_start_unix_ms"].as_u64(),
            measurement_end_unix_ms: output["measurement_end_unix_ms"].as_u64(),
        });
        current.validate_successor(&next)?;
        persist_campaign_inventory(platform, &next).await
    }).await.context("bounded publication budget inside the original cell deadline expired; publication/custody outcome is UNKNOWN")?
}

#[cfg(test)]
mod official_cell_measurement_tests {
    use super::*;

    fn original_measurement() -> Result<(OatConfig, RunInventory, Vec<OatResult>)> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        original_measurement_for_config(config, "c0ffee00-0000-4000-8000-000000000004")
    }

    fn original_measurement_for_config(mut config: OatConfig, orchestration_id: &str) -> Result<(OatConfig, RunInventory, Vec<OatResult>)> {
        config.tables.truncate(1);
        let identity = CampaignIdentity {
            campaign_id: "c0ffee00-0000-4000-8000-000000000001".to_owned(),
            owner_id: "c0ffee00-0000-4000-8000-000000000002".to_owned(),
            fencing_token: 1,
        };
        let cell = "c0ffee00-0000-4000-8000-000000000003";
        let inventory = initial_campaign_inventory(
            &identity, orchestration_id, cell,
            &format!("oat-{}", cell.replace('-', "")), "arrowcollector12345", "checkpoints",
            CampaignCellBudget {
                effective_stop_budgets: config.maintenance_stop_budget()?,
                planned_cell_duration_ms: config.timing_budget()?.planned_cell_duration_ms,
            },
        )?;
        let mut rows = result_rows(&config, &inventory.orchestration_run_id, cell, 1, 1, CellMeasurements {
            clients: ClientRun {
                metrics: HashMap::new(), pinning: PinningReport::default(), qap_tables: Vec::new(),
                failure: Some("synthetic_offline_fixture".to_owned()), evidence: CellEvidence::default(),
            },
            started_at_ms: 1_000, qap: QapMetrics::default(), resources: None, resource_evidence: None,
        })?;
        let mut output = commit_window_output(&BTreeMap::new(), &config.tables[0], 1_000, 31_000, None, None)?;
        output["teardown_outcome"] = json!("failed");
        rows[0].evidence_json = Some(serde_json::to_string(&json!({
            "version": 1, "teardown_outcome": "failed",
            "commit_window_output_throughput": output,
            "durable_commits": {"run_cumulative_output": {"rows": null, "teardown_outcome": "failed"},
                "complete_30_second_commit_windows": []},
        }))?);
        rows[0].git_sha = "a".repeat(40);
        Ok((config, inventory, rows))
    }

    #[tokio::test]
    async fn submitted_identity_is_captured_once_from_the_exact_server_run_and_published_immutably() -> Result<()> {
        let external = std::env::var_os("OAT_OFFLINE_RAW_SUBMISSION_FIXTURE");
        let cases: Vec<Value> = if let Some(path) = &external {
            serde_json::from_slice(&fs::read(path)?)?
        } else {
            let raw = include_str!("../../../docs/experiments/arrowcol-oat/oat-config.quick.json");
            vec![json!({
                "submitted_config_body": raw,
                "post": {"id": "c0ffee00-0000-4000-8000-000000000004", "config_sha256": fixture_sha256(raw.as_bytes())},
                "get": {"id": "c0ffee00-0000-4000-8000-000000000004", "config_sha256": fixture_sha256(raw.as_bytes())},
            })]
        };
        assert!(!cases.is_empty());
        let mut proofs = Vec::new();
        for case in cases {
            let raw = case["submitted_config_body"].as_str().context("missing exact submitted body")?;
            let id = case["post"]["id"].as_str().context("missing actual server run id")?;
            let server_sha = case["post"]["config_sha256"].as_str().context("missing server hash")?;
            assert_eq!(case["get"]["id"], id);
            assert_eq!(case["get"]["config_sha256"], server_sha);
            assert_eq!(fixture_sha256(raw.as_bytes()), server_sha);
            let submitted: OatConfig = serde_json::from_str(raw)?;
            submitted.validate()?;
            let (config, inventory, mut rows) = original_measurement_for_config(submitted, id)?;
            rows[0].git_sha = std::env::var("GIT_SHA").unwrap_or_else(|_| "unknown".to_owned());
            let state = AppState::default();
            assert!(capture_submitted_config_identity(&state, id).await.is_none());
            state.runs.lock().await.insert(id.to_owned(), crate::model::RunStatus {
                id: id.to_owned(), config_sha256: server_sha.to_owned(), state: crate::model::RunState::Running,
                progress: "synthetic offline original-cell proof".to_owned(), result_count: 0,
                error: None, started_at_unix_ms: 1_000, finished_at_unix_ms: None,
                local_teardown: None,
                aks_cleanup_confirmed: None,
                aks_cleanup_resolved: false,
            });
            let identity = capture_submitted_config_identity(&state, id).await.context("missing captured identity")?;
            assert!(capture_submitted_config_identity(&state, &inventory.cell_run_id).await.is_none());
            {
                let mut runs = state.runs.lock().await;
                let status = runs.get_mut(id).context("missing original status")?;
                status.id = inventory.cell_run_id.clone();
            }
            assert!(capture_submitted_config_identity(&state, id).await.is_none());
            for malformed in ["", "not-a-hash", &"A".repeat(64)] {
                let mut runs = state.runs.lock().await;
                let status = runs.get_mut(id).context("missing original status")?;
                status.id = id.to_owned();
                status.config_sha256 = malformed.to_owned();
                drop(runs);
                assert!(capture_submitted_config_identity(&state, id).await.is_none());
            }
            let bytes = official_measurement_bytes(&inventory, &config, &rows, Some(&identity))?;
            let payload: Value = serde_json::from_slice(&bytes)?;
            assert_eq!(payload["submitted_config_sha256"], server_sha);
            assert_ne!(payload["effective_config_sha256"], server_sha);
            let mut foreign = identity.clone();
            foreign.orchestration_run_id = inventory.cell_run_id.clone();
            let refused: Value = serde_json::from_slice(&official_measurement_bytes(&inventory, &config, &rows, Some(&foreign))?)?;
            assert!(refused["submitted_config_sha256"].is_null());
            let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
            let path = ObjectPath::from(official_measurement_blob(&inventory)?);
            publish_immutable_official_bytes(Arc::clone(&store), &path, &bytes).await?;
            let retained = store.get_opts(&path, GetOptions::default()).await?.bytes().await?;
            assert_eq!(retained.as_ref(), bytes);
            let other: Value = serde_json::from_slice(&official_measurement_bytes(&inventory, &config, &rows, None)?)?;
            assert!(publish_immutable_official_bytes(store, &path, fixture_canonical_json(&other)?.as_bytes()).await.is_err());
            proofs.push(json!({ "submitted_config_body": raw, "official_measurement_utf8": String::from_utf8(bytes)? }));
        }
        println!("D46_RAW_IDENTITY_PRODUCER_BYTES {}", serde_json::to_string(&json!({
            "fixture_source": if external.is_some() { "retained_loopback_post_get" } else { "synthetic_state_invariant" },
            "cases": proofs,
        }))?);
        Ok(())
    }

    #[test]
    fn official_producer_retains_the_original_window_and_missing_submitted_identity() -> Result<()> {
        let (mut config, inventory, rows) = original_measurement()?;
        let bytes = official_measurement_bytes(&inventory, &config, &rows, None)?;
        let payload: Value = serde_json::from_slice(&bytes)?;
        let original: Value = serde_json::from_str(rows[0].evidence_json.as_deref().context("missing evidence")?)?;
        assert_eq!(payload["official_result"]["evidence"]["commit_window_output_throughput"], original["commit_window_output_throughput"]);
        assert!(payload["submitted_config_sha256"].is_null());
        let effective = payload["effective_config_json"].as_str().context("missing effective config")?;
        assert_eq!(payload["effective_config_sha256"], fixture_sha256(effective.as_bytes()));
        config.schedule_lead_seconds += 1;
        let changed: Value = serde_json::from_slice(&official_measurement_bytes(&inventory, &config, &rows, None)?)?;
        assert_ne!(payload["effective_config_sha256"], changed["effective_config_sha256"]);
        config.campaign_profile = Some(crate::config::CampaignProfile::ScaleOutSingleTable);
        assert!(official_measurement_bytes(&inventory, &config, &rows, None).is_err());
        config.campaign_profile = Some(crate::config::CampaignProfile::SingleHotTable);
        let mut wrong = inventory;
        wrong.cell_run_id = "c0ffee00-0000-4000-8000-000000000099".to_owned();
        assert!(official_measurement_bytes(&wrong, &config, &rows, None).is_err());
        println!("D46_OFFICIAL_PRODUCER_BYTES {}", String::from_utf8(bytes)?);
        Ok(())
    }

    #[test]
    fn final_official_result_requires_teardown_and_reconciled_delivery() -> Result<()> {
        let (mut config, inventory, mut rows) = original_measurement()?;
        config.clients_per_table = 1;
        let row = &mut rows[0];
        row.status = "succeeded".to_owned();
        row.failure = None;
        row.evidence_json = Some(json!({
            "client_phase_reports": [{"phase_counters": {"process_cumulative": {"completed_rows": 10, "ambiguous_rows": 0}}}],
            "durable_commits": {
                "collector_teardown_admissions": {"status": "clear", "cancelled_ambiguous": 0},
                "run_cumulative_output": {"rows": 10, "settled": true, "commits_by_version": [{"delta_version": 1, "rows": 10, "files": 1, "parquet_bytes": 50}]},
                "output_throughput": {"rows": 10}, "complete_30_second_commit_windows": [{"rows": 10}]
            }
        }).to_string());
        let delta = json!({"rows": 10, "versions": [{"delta_version": 1, "rows": 10, "files": 1, "parquet_bytes": 50}]});
        for status in ["unavailable", "ambiguous"] {
            let mut candidate = row.clone();
            let mut evidence: Value = serde_json::from_str(candidate.evidence_json.as_deref().context("missing fixture evidence")?)?;
            evidence["durable_commits"]["collector_teardown_admissions"] = json!({"status": status, "cancelled_ambiguous": null});
            candidate.evidence_json = Some(evidence.to_string());
            apply_final_aks_delta_reconciliation(&config, &mut candidate, Some(&delta), true)?;
            let evidence: Value = serde_json::from_str(candidate.evidence_json.as_deref().context("missing final evidence")?)?;
            assert_eq!(evidence["durable_commits"]["post_teardown_delta_reconciliation"]["status"], "matched");
            assert_eq!(candidate.status, "failed");
        }
        apply_final_aks_delta_reconciliation(&config, row, Some(&delta), true)?;
        let mut outcome = finish_matrix_cell(&config, &inventory.orchestration_run_id, &inventory.cell_run_id, 1, 1, Ok(rows), Ok(()));
        attach_teardown_outcome(&mut outcome, "succeeded")?;
        let payload: Value = serde_json::from_slice(&official_measurement_bytes(&inventory, &config, &outcome.rows, None)?)?;
        let evidence = &payload["official_result"]["evidence"];
        assert_eq!(evidence["teardown_outcome"], "succeeded");
        assert_eq!(evidence["durable_commits"]["run_cumulative_output"]["rows"], 10);
        assert_eq!(evidence["durable_commits"]["complete_30_second_commit_windows"][0]["teardown_outcome"], "succeeded");
        assert_eq!(evidence["durable_commits"]["post_teardown_delta_reconciliation"]["status"], "matched");
        let mut pending = outcome.rows.clone();
        let mut changed: Value = serde_json::from_str(pending[0].evidence_json.as_deref().context("missing pending evidence")?)?;
        changed["teardown_outcome"] = json!("pending");
        pending[0].evidence_json = Some(changed.to_string());
        assert!(official_measurement_bytes(&inventory, &config, &pending, None).is_err());
        let mut failed = outcome.rows;
        apply_final_aks_delta_reconciliation(&config, &mut failed[0], None, false)?;
        let mut outcome = finish_matrix_cell(&config, &inventory.orchestration_run_id, &inventory.cell_run_id, 1, 1, Ok(failed), Err(anyhow::anyhow!("drain failed")));
        attach_teardown_outcome(&mut outcome, "failed")?;
        let payload: Value = serde_json::from_slice(&official_measurement_bytes(&inventory, &config, &outcome.rows, None)?)?;
        assert_eq!(payload["official_result"]["status"], "failed");
        assert_eq!(payload["official_result"]["evidence"]["teardown_outcome"], "failed");
        assert_eq!(payload["official_result"]["evidence"]["durable_commits"]["run_cumulative_output"]["rows"], 10);
        assert_eq!(payload["official_result"]["evidence"]["commit_window_output_usage"], "diagnostic_only");
        Ok(())
    }

    #[tokio::test]
    async fn official_publication_is_create_only_and_never_adopts_different_bytes() -> Result<()> {
        let (config, inventory, rows) = original_measurement()?;
        let bytes = official_measurement_bytes(&inventory, &config, &rows, None)?;
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let path = ObjectPath::from(official_measurement_blob(&inventory)?);
        let first = publish_immutable_official_bytes(Arc::clone(&store), &path, &bytes).await?;
        let same = publish_immutable_official_bytes(Arc::clone(&store), &path, &bytes).await?;
        assert_eq!(first, same);
        let mut changed = bytes.clone();
        changed[0] = b'[';
        assert!(publish_immutable_official_bytes(Arc::clone(&store), &path, &changed).await.is_err());
        assert_eq!(store.get_opts(&path, GetOptions::default()).await?.bytes().await?.as_ref(), bytes);
        let mut bound = inventory.clone();
        bound.official_measurement = Some(InventoryOfficialMeasurement {
            schema_version: 1, blob: official_measurement_blob(&inventory)?, etag: first.0, version: first.1,
            sha256: fixture_sha256(&bytes), byte_length: bytes.len() as u64, effective_config_sha256: "a".repeat(64),
            submitted_config_sha256: None, source_revision: Some("a".repeat(40)),
            measurement_start_unix_ms: Some(1_000), measurement_end_unix_ms: Some(31_000),
        });
        inventory.validate_successor(&bound)?;
        let mut dropped = bound.clone();
        dropped.official_measurement = None;
        assert!(bound.validate_successor(&dropped).is_err());
        assert!(publish_immutable_official_bytes(store, &path, &vec![0; OFFICIAL_MEASUREMENT_MAX_BYTES + 1]).await.is_err());
        Ok(())
    }
}

#[derive(Clone, Debug, Default, serde::Serialize)]
struct CellEvidence {
    cpu_quota: Option<Value>,
    overload_guard: Option<Value>,
    overload_stop: Option<Value>,
    downstream_comment: Option<String>,
    qap_observation: Option<Value>,
    native_telemetry: Option<Value>,
    resource_samples: Vec<ResourceSample>,
    resource_sampling_failures: u64,
    resource_samples_dropped: u64,
    backlog_scrape_configured: bool,
    backlog_samples: Vec<PublicationBacklogSample>,
    backlog_sampling_failures: u64,
    backlog_samples_dropped: u64,
    client_reports: HashMap<String, Vec<Value>>,
    processes: Option<Value>,
    measurement_window: Option<(u64, u64)>,
    client_schedule: Option<Value>,
    preload_delta: Option<Value>,
    parity_admission_readback: Option<Value>,
    durable_commits: HashMap<String, Value>,
    router_sql: Option<Value>,
    router_cpu_pressure: Option<Value>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct ResourceSample {
    sampled_at_unix_ms: u64,
    metrics: ResourceMetrics,
}

#[derive(Clone, Debug, serde::Serialize)]
struct PublicationBacklogSample {
    request_start_unix_ms: u64,
    request_end_unix_ms: u64,
    request_timeout_ms: u64,
    available: Option<bool>,
    publications: Option<u64>,
    rows: Option<u64>,
    logical_bytes: Option<u64>,
    oldest_age_seconds: Option<f64>,
    status: Option<String>,
    unavailable_reason: Option<String>,
    pipeline_stage_metrics: Option<Value>,
    pipeline_stage_metrics_unavailable_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct PublicationBacklogMetrics {
    available: bool,
    publications: Option<u64>,
    rows: Option<u64>,
    logical_bytes: Option<u64>,
    oldest_age_seconds: Option<f64>,
    status: String,
    pipeline_stage_metrics: Option<Value>,
    pipeline_stage_metrics_unavailable_reason: Option<String>,
}

fn parse_pipeline_histogram(text: &str, name: &str, stage: Option<&str>) -> Option<Value> {
    let bucket_name = format!("{name}_bucket");
    let count_name = format!("{name}_count");
    let sum_name = format!("{name}_sum");
    let mut finite_buckets: Vec<(f64, String, u64)> = Vec::new();
    let mut infinite_count = None;
    let mut count = None;
    let mut sum = None;
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty() && !line.starts_with('#')) {
        let mut fields = line.split_whitespace();
        let Some(sample) = fields.next() else { continue };
        let Some(value) = fields.next() else { continue };
        if fields.next().is_some() {
            continue;
        }
        let (metric, labels) = match sample.split_once('{') {
            Some((metric, labels)) => (metric, labels.strip_suffix('}')?),
            None => (sample, ""),
        };
        if metric != bucket_name && metric != count_name && metric != sum_name {
            continue;
        }
        if !labels.contains("component=\"arrowcol-dataflow-rust\"")
            || stage.is_some_and(|stage| !labels.contains(&format!("stage=\"{stage}\"")))
            || stage.is_none() && labels.contains("stage=\"")
        {
            continue;
        }
        if metric == bucket_name {
            let le = labels.split(",le=\"").nth(1)?.split('\"').next()?;
            let bucket = value.parse::<u64>().ok()?;
            if le == "+Inf" {
                if infinite_count.replace(bucket).is_some() {
                    return None;
                }
            } else {
                let bound = le.parse::<f64>().ok()?;
                if !bound.is_finite() || bound < 0.0 || finite_buckets.iter().any(|(existing, _, _)| *existing == bound) {
                    return None;
                }
                finite_buckets.push((bound, le.to_owned(), bucket));
            }
        } else if metric == count_name {
            if count.replace(value.parse::<u64>().ok()?).is_some() {
                return None;
            }
        } else if metric == sum_name {
            let parsed = value.parse::<f64>().ok()?;
            if !parsed.is_finite() || parsed < 0.0 || sum.replace(parsed).is_some() {
                return None;
            }
        }
    }
    let count = count?;
    let sum = sum?;
    if infinite_count? != count {
        return None;
    }
    finite_buckets.sort_by(|left, right| left.0.total_cmp(&right.0));
    let mut previous_count = 0;
    for (_, _, bucket_count) in &finite_buckets {
        if *bucket_count < previous_count || *bucket_count > count {
            return None;
        }
        previous_count = *bucket_count;
    }
    let mut buckets = finite_buckets.into_iter()
        .map(|(_, le, bucket_count)| json!({"le":le,"count":bucket_count}))
        .collect::<Vec<_>>();
    buckets.push(json!({"le":"+Inf","count":count}));
    Some(json!({"count":count,"sum_seconds":sum,"buckets":buckets}))
}

fn parse_pipeline_stage_metrics(text: &str) -> Option<Value> {
    let stages = ["collect", "build", "sort", "encode", "upload", "commit"]
        .into_iter()
        .map(|stage| Some((stage.to_owned(), parse_pipeline_histogram(text, "arrowcol_pipeline_stage_duration_seconds", Some(stage))?)))
        .collect::<Option<serde_json::Map<String, Value>>>()?;
    let submission_wait = parse_pipeline_histogram(text, "arrowcol_pipeline_submission_wait_seconds", None)?;
    Some(json!({"temporality":"cumulative_process_lifetime_snapshot","stage_duration_seconds":stages,"submission_wait_seconds":submission_wait}))
}

fn parse_publication_backlog_metrics(text: &str) -> Result<PublicationBacklogMetrics> {
    let mut samples = BTreeMap::new();
    let mut status = None;
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty() && !line.starts_with('#')) {
        let mut fields = line.split_whitespace();
        let sample = fields.next().context("publication backlog metric sample is empty")?;
        let metric = sample.split('{').next().unwrap_or(sample);
        if !metric.starts_with("arrowcol_publication_backlog_") {
            continue;
        }
        let value = fields.next().context("publication backlog metric has no value")?;
        if fields.next().is_some() {
            bail!("publication backlog metric has an unsupported timestamp or extra field");
        }
        match metric {
            "arrowcol_publication_backlog_available"
            | "arrowcol_publication_backlog_publications"
            | "arrowcol_publication_backlog_rows"
            | "arrowcol_publication_backlog_bytes"
            | "arrowcol_publication_backlog_oldest_age_seconds" => {
                if sample != metric {
                    bail!("publication backlog gauge has unexpected labels");
                }
                if samples.insert(metric, value).is_some() {
                    bail!("publication backlog exposition contains a duplicate gauge");
                }
            }
            "arrowcol_publication_backlog_status" => {
                let label = sample
                    .strip_prefix("arrowcol_publication_backlog_status{status=\"")
                    .and_then(|value| value.strip_suffix("\"}"))
                    .context("publication backlog status has unexpected labels")?;
                if label.is_empty()
                    || label.len() > 64
                    || !label.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
                    || value != "1"
                {
                    bail!("publication backlog status sample is invalid");
                }
                if status.replace(label).is_some() {
                    bail!("publication backlog exposition contains duplicate status samples");
                }
            }
            _ if metric.starts_with("arrowcol_publication_backlog_") => {
                bail!("publication backlog exposition contains an unsupported metric");
            }
            _ => {}
        }
    }

    let get = |name| {
        samples
            .get(name)
            .copied()
            .with_context(|| format!("publication backlog exposition omitted {name}"))
    };
    let available = match get("arrowcol_publication_backlog_available")? {
        "0" => false,
        "1" => true,
        _ => bail!("publication backlog availability must be 0 or 1"),
    };
    let raw_publications = get("arrowcol_publication_backlog_publications")?;
    let raw_rows = get("arrowcol_publication_backlog_rows")?;
    let raw_bytes = get("arrowcol_publication_backlog_bytes")?;
    let raw_age = get("arrowcol_publication_backlog_oldest_age_seconds")?;
    let status = status.context("publication backlog exposition omitted status")?.to_owned();
    let pipeline_stage_metrics = parse_pipeline_stage_metrics(text);
    let pipeline_stage_metrics_unavailable_reason = pipeline_stage_metrics.is_none()
        .then(|| "exporter_stage_histograms_missing_or_malformed".to_owned());

    if available {
        if status != "available" {
            bail!("available publication backlog must have available status");
        }
        let publications = raw_publications.parse::<u64>().context("invalid publication backlog count")?;
        let rows = raw_rows.parse::<u64>().context("invalid publication backlog row count")?;
        let logical_bytes = raw_bytes.parse::<u64>().context("invalid publication backlog byte count")?;
        let oldest_age_seconds = raw_age.parse::<f64>().context("invalid publication backlog age")?;
        if !oldest_age_seconds.is_finite() || oldest_age_seconds < 0.0 {
            bail!("available publication backlog age must be finite and non-negative");
        }
        Ok(PublicationBacklogMetrics {
            available,
            publications: Some(publications),
            rows: Some(rows),
            logical_bytes: Some(logical_bytes),
            oldest_age_seconds: Some(oldest_age_seconds),
            status,
            pipeline_stage_metrics,
            pipeline_stage_metrics_unavailable_reason,
        })
    } else {
        if status == "available"
            || [raw_publications, raw_rows, raw_bytes, raw_age]
                .iter()
                .any(|value| !value.eq_ignore_ascii_case("nan"))
        {
            bail!("unavailable publication backlog must use NaN gauges and an unavailable status");
        }
        Ok(PublicationBacklogMetrics {
            available,
            publications: None,
            rows: None,
            logical_bytes: None,
            oldest_age_seconds: None,
            status,
            pipeline_stage_metrics,
            pipeline_stage_metrics_unavailable_reason,
        })
    }
}

fn failed_publication_backlog_sample(
    start: u64,
    end: u64,
    request_timeout_ms: u64,
    reason: &str,
) -> PublicationBacklogSample {
    PublicationBacklogSample {
        request_start_unix_ms: start,
        request_end_unix_ms: end,
        request_timeout_ms,
        available: None,
        publications: None,
        rows: None,
        logical_bytes: None,
        oldest_age_seconds: None,
        status: None,
        unavailable_reason: Some(reason.to_owned()),
        pipeline_stage_metrics: None,
        pipeline_stage_metrics_unavailable_reason: Some(reason.to_owned()),
    }
}

fn publication_backlog_sample(
    metrics: PublicationBacklogMetrics,
    request_start_unix_ms: u64,
    request_end_unix_ms: u64,
    request_timeout_ms: u64,
) -> PublicationBacklogSample {
    PublicationBacklogSample {
        request_start_unix_ms,
        request_end_unix_ms,
        request_timeout_ms,
        available: Some(metrics.available),
        publications: metrics.publications,
        rows: metrics.rows,
        logical_bytes: metrics.logical_bytes,
        oldest_age_seconds: metrics.oldest_age_seconds,
        unavailable_reason: (!metrics.available).then(|| metrics.status.clone()),
        status: Some(metrics.status),
        pipeline_stage_metrics: metrics.pipeline_stage_metrics,
        pipeline_stage_metrics_unavailable_reason: metrics.pipeline_stage_metrics_unavailable_reason,
    }
}

fn valid_backlog_request_interval(start: u64, end: u64, window_start: u64, window_end: u64) -> bool {
    start >= window_start && end <= window_end && end >= start
}

fn local_downstream_optional(kind: crate::config::OatMode) -> bool {
    kind == crate::config::OatMode::Local
}

#[derive(Debug)]
struct LocalDownstreamUnavailable;

impl std::fmt::Display for LocalDownstreamUnavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("local Kustainer observation is unavailable")
    }
}

impl std::error::Error for LocalDownstreamUnavailable {}

fn classify_local_qap_error(error: anyhow::Error) -> anyhow::Error {
    if error.is::<arrowcol_loadgen::QapUnavailable>() {
        error.context(LocalDownstreamUnavailable)
    } else {
        error
    }
}

impl CellEvidence {
    fn downstream_unavailable(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        tracing::warn!(
            reason = %reason,
            "downstream observation unavailable; preserving local collector evidence"
        );
        self.downstream_comment.get_or_insert(reason);
    }

    fn resource_summary(&self) -> Option<ResourceMetrics> {
        if self.resource_samples.is_empty() {
            return None;
        }
        Some(ResourceMetrics {
            cpu_millicores: self
                .resource_samples
                .iter()
                .map(|sample| sample.metrics.cpu_millicores)
                .sum::<f64>()
                / self.resource_samples.len() as f64,
            memory_bytes: self
                .resource_samples
                .iter()
                .map(|sample| sample.metrics.memory_bytes)
                .max()?,
        })
    }

    fn record_client(&mut self, config: &OatConfig, table: &str, client_id: &str, output: &str) -> Result<()> {
        let effective = process_configurations(output)?;
        crate::runner::verify_oat_client_build_profile_readback(&effective)?;
        verify_process_configuration(config, ComponentRole::Client, &effective)?;
        let data_profile = config.tunables.client.as_ref().and_then(|client| client.data_profile).unwrap_or_default();
        let counters = client_phase_evidence(output, data_profile)?;
        if let Some(window) = counters.get("measurement_window").filter(|value| !value.is_null()) {
            let report = parse_client_metrics(output)?;
            if self.measurement_window != window["start_unix_ms"].as_u64().zip(window["end_unix_ms"].as_u64())
                || window["completed_rows"].as_u64() != Some(report.rows)
            {
                bail!("client phase counters disagree with the armed window or measured-row report");
            }
        }
        self.client_reports.entry(table.to_owned()).or_default().push(json!({
            "client_id": client_id,
            "table": table,
            "phase_counters": counters,
            "consumer_effective_config": effective,
            "execution_image": client_image_evidence(output)?,
        }));
        Ok(())
    }

    fn record_cancelled_client(&mut self, table: &str, client_id: &str, output: Option<&str>) {
        let terminal = output.and_then(|output| cancelled_client_window(output, self.measurement_window));
        let phase_counters = match terminal {
            Some((phase, window)) => json!({"terminal_phase": phase, "measurement_window": window}),
            None => json!({"measurement_window": null, "unavailable_reason": "terminal_client_window_unavailable"}),
        };
        let effective = output.and_then(|output| process_configurations(output).ok());
        self.client_reports.entry(table.to_owned()).or_default().push(json!({
            "client_id": client_id, "table": table, "status": "diagnostic_only",
            "phase_counters": phase_counters, "consumer_effective_config": effective,
        }));
    }

    fn client_snapshot_total(&self, table: &str, field: &str) -> Option<u64> {
        let reports = self.client_reports.get(table)?;
        if reports.is_empty() {
            return None;
        }
        reports.iter().try_fold(0_u64, |total, report| {
            report
                .pointer("/phase_counters/workers")?
                .as_array()?
                .iter()
                .try_fold(total, |total, worker| total.checked_add(worker.get(field)?.as_u64()?))
        })
    }

    fn client_cumulative_total(&self, table: &str, field: &str) -> Option<u64> {
        let reports = self.client_reports.get(table)?;
        if reports.is_empty() {
            return None;
        }
        reports.iter().try_fold(0_u64, |total, report| {
            total.checked_add(report.pointer(&format!("/phase_counters/process_cumulative/{field}"))?.as_u64()?)
        })
    }

    fn client_window_payload_bytes(&self, table: &str, client: Option<&ClientMetrics>) -> Option<u64> {
        let client = client?;
        let window = self.measurement_window?;
        let reports = self.client_reports.get(table)?;
        if reports.is_empty() {
            return None;
        }
        let (rows, bytes) = reports.iter().try_fold((0_u64, 0_u64), |(rows, bytes), report| {
            let measurement = report.pointer("/phase_counters/measurement_window")?;
            if measurement["start_unix_ms"].as_u64().zip(measurement["end_unix_ms"].as_u64()) != Some(window) {
                return None;
            }
            Some((
                rows.checked_add(measurement["completed_rows"].as_u64()?)?,
                bytes.checked_add(measurement["completed_bytes"].as_u64()?)?,
            ))
        })?;
        (rows == client.rows).then_some(bytes)
    }

    fn reconciliation(&self, table: &str, durable: Option<&Value>) -> Value {
        let delta_evidence = durable.and_then(|value| value.get("run_cumulative_output"));
        let Some(delta_rows) = delta_evidence.and_then(|value| value.get("rows")).and_then(Value::as_u64) else {
            return json!({
                "status": "unavailable",
                "reason": delta_evidence
                    .and_then(|value| value.get("unavailable_reason"))
                    .and_then(Value::as_str)
                    .unwrap_or("cumulative_delta_rows_unavailable"),
            });
        };
        let Some(completed_rows) = self.client_cumulative_total(table, "completed_rows") else {
            return json!({"status": "unavailable", "reason": "client_cumulative_completed_rows_unavailable"});
        };
        let Some(ambiguous_rows) = self.client_cumulative_total(table, "ambiguous_rows") else {
            return json!({"status": "unavailable", "reason": "client_cumulative_ambiguous_rows_unavailable"});
        };
        let Some(maximum_delta_rows) = completed_rows.checked_add(ambiguous_rows) else {
            return json!({"status": "unavailable", "reason": "client_reconciliation_bound_overflow"});
        };
        let lower_bound_met = completed_rows <= delta_rows;
        let upper_bound_met = delta_rows <= maximum_delta_rows;
        let settle = delta_evidence.and_then(|value| value.get("settle_seconds")).and_then(Value::as_f64);
        let settled = delta_evidence.and_then(|value| value.get("settled")).and_then(Value::as_bool).unwrap_or(false);
        let status = if !upper_bound_met || (settled && !lower_bound_met) {
            "outside_bound"
        } else if lower_bound_met {
            "within_bound"
        } else {
            "unsettled"
        };
        json!({
            "status": status,
            "reason": (status == "unsettled").then_some("delta_commit_drain_timeout"),
            "settle_seconds": settle,
            "settle_timeout_seconds": delta_evidence.and_then(|value| value.get("settle_timeout_seconds")),
            "settled": settled,
            "cell_scope": delta_evidence.and_then(|value| value.get("cell_scope")),
            "delta_rows": delta_rows,
            "completed_rows": completed_rows,
            "ambiguous_rows": ambiguous_rows,
            "gap_rows": completed_rows.saturating_sub(delta_rows),
            "minimum_delta_rows": completed_rows,
            "maximum_delta_rows": maximum_delta_rows,
            "lower_bound_met": lower_bound_met,
            "upper_bound_met": upper_bound_met,
            "exact_match_required": ambiguous_rows == 0,
            "exact_match": delta_rows == completed_rows,
            "scope": "process_cumulative_client_rows_vs_this_cells_new_ingestion_commits_including_warmup",
        })
    }

    fn for_table(&self, table: &str, client: Option<&ClientMetrics>) -> Value {
        let durable = self.durable_commits.get(table);
        let output = durable.and_then(|value| value.get("output_throughput"));
        let observed_cores = self.resource_summary().map(|metrics| metrics.cpu_millicores / 1000.0)
            .filter(|cores| cores.is_finite() && *cores > 0.0);
        let committed_rate = output.and_then(|value| value["rows_per_second"].as_f64());
        let input_rate = output.and_then(|value| value["input_bytes_per_second"].as_f64());
        let parquet_rate = output.and_then(|value| value["parquet_bytes_per_second"].as_f64());
        let payload_bytes = self.client_window_payload_bytes(table, client);
        let window_seconds = self.measurement_window
            .and_then(|(start, end)| end.checked_sub(start))
            .filter(|duration| *duration > 0)
            .map(|duration| duration as f64 / 1000.0);
        let payload_rate = payload_bytes.zip(window_seconds).map(|(bytes, seconds)| bytes as f64 / seconds);
        let backlog = json!({
            "schema_version": 1,
            "source": "arrowcol_publication_backlog_*",
            "scope": "unique_table_owner_admissions_not_confirmed_durable_commit",
            "timestamp_basis": "UTC request start/end around direct /metrics GET; each is a scrape-time snapshot",
            "sampling_interval_ms": PUBLICATION_BACKLOG_SAMPLING_INTERVAL.as_millis(),
            "request_timeout_ms": PUBLICATION_BACKLOG_REQUEST_TIMEOUT.as_millis(),
            "measurement_window": self.measurement_window.map(|(start_unix_ms, end_unix_ms)| json!({
                "start_unix_ms": start_unix_ms, "end_unix_ms": end_unix_ms,
            })),
            "scraper_configured": self.backlog_scrape_configured,
            "samples": self.backlog_samples,
            "sampling_failures": self.backlog_sampling_failures,
            "samples_dropped": self.backlog_samples_dropped,
            "retention": "most_recent_4096_samples",
            "cadence_seconds": 1,
            "last_unavailable_reason": self.backlog_samples.last().and_then(|sample| sample.unavailable_reason.as_deref()),
            "unavailable_reason": if !self.backlog_scrape_configured {
                Some("scraper_not_configured")
            } else if self.backlog_samples.is_empty() {
                Some("no_successful_scrape_samples")
            } else {
                None
            },
        });
        json!({
            "version": 1,
            "client_schedule": self.client_schedule,
            "preload_delta": self.preload_delta,
            "parity_admission_readback": self.parity_admission_readback,
            "overload_guard": self.overload_guard,
            "overload_stop": self.overload_stop,
            "processes": self.processes,
            "router_sql": self.router_sql,
            "router_cpu_pressure": self.router_cpu_pressure,
            "durable_commits": self.durable_commits.get(table),
            "reconciliation": self.reconciliation(table, durable),
            "commit_window_output_throughput": output,
            "client_phase_reports": self.client_reports.get(table),
            "downstream_status": if self.downstream_comment.is_some() { "unavailable" } else { "observed_or_not_attempted" },
            "downstream_comment": self.downstream_comment,
            "qap_observation": self.qap_observation,
            "native_telemetry": self.native_telemetry,
            "resources": {
                "scope": "collector_fleet_measurement_window_only",
                "cpu_summary": "arithmetic_mean_of_observed_samples",
                "memory_summary": "maximum_observed_sample_not_peak_rss",
                "samples": self.resource_samples,
                "sampling_failures": self.resource_sampling_failures,
                "samples_dropped": self.resource_samples_dropped,
                "retention": "most_recent_4096_samples",
                "cadence_seconds": 1,
                "sample_timeout_seconds": 3,
                "unavailable_reason": self.resource_samples.is_empty().then_some("no_measurement_window_resource_samples"),
            },
            "rows": {
                "attempted": self.client_snapshot_total(table, "attempted_rows"),
                "attempted_scope": "measurement_start_to_completed_process_snapshot_interval_not_exact_window",
                "sent": null,
                "sent_unavailable_reason": "no_transport_write_completion_counter",
                "client_completed": client.map(|metrics| metrics.rows),
                "wal_acked": null,
                "delta_committed": output.and_then(|value| value["rows"].as_u64()),
                "delta_committed_scope": "ingestion_commit_finish_window_includes_warmup_backlog_not_client_cohort",
                "unique_delivered": null,
                "unique_delivery_unavailable_reason": "commit_counts_do_not_prove_unique_client_delivery",
                "wal_ack_unavailable_reason": "admission_response_is_not_a_wal_fsync_receipt",
                "retry_semantics": "upload_completion_does_not_establish_exactly_once_delivery",
            },
            "durable_rows_per_second": output.and_then(|value| value["rows_per_second"].as_f64()),
            "rate_evidence": {
                "collector_input_bytes_per_second": input_rate,
                "committed_parquet_bytes_per_second": parquet_rate,
                "completed_flight_payload_bytes_per_second": payload_rate,
                "payload_scope": "protobuf_flight_data_encoded_lengths_for_admission_completions_excludes_grpc_and_http2_overhead",
                "observed_collector_cores": observed_cores,
                "committed_rows_per_second_per_observed_collector_core": committed_rate.zip(observed_cores).map(|(rate, cores)| rate / cores),
                "collector_input_bytes_per_second_per_observed_collector_core": input_rate.zip(observed_cores).map(|(rate, cores)| rate / cores),
                "committed_parquet_bytes_per_second_per_observed_collector_core": parquet_rate.zip(observed_cores).map(|(rate, cores)| rate / cores),
                "completed_flight_payload_bytes_per_second_per_observed_collector_core": payload_rate.zip(observed_cores).map(|(rate, cores)| rate / cores),
                "core_scope": "arithmetic_mean_of_observed_collector_fleet_cpu_samples_not_time_weighted",
                "allocated_collector_cores": null,
            },
            "durable_files": output.and_then(|value| value["files"].as_u64()),
            "durable_bytes": output.and_then(|value| value["parquet_bytes"].as_u64()),
            "durable_bytes_per_row": output.and_then(|value| value["bytes_per_row"].as_f64()),
            "compression_ratio": output.and_then(|value| value["compression_ratio"].as_f64()),
            "compression_ratio_denominator": "committed_parquet_file_bytes_including_metadata",
            "stage_latency_percentiles": { "commit_seconds": output.and_then(|value| value.get("commit_latency_seconds")) },
            "backlog": backlog,
        })
    }
}

#[derive(Clone)]
enum ResourceSamplerSource {
    Aks,
    AksNative(Arc<NativeTelemetrySource>),
    Local {
        run_id: String,
        environment: HashMap<String, String>,
        collector_metrics_url: Option<String>,
    },
}

#[derive(Clone)]
struct NativeTelemetrySource {
    server: String,
    context: Option<String>,
    targets: Vec<AksResourcePodTarget>,
    qap_endpoint: String,
    qap_database: String,
    qap_tables: Vec<String>,
    results: PathBuf,
    binding: Value,
}

const NATIVE_TELEMETRY_INTERVAL_MS: u64 = 10_000;
const NATIVE_TELEMETRY_MAX_SAMPLES: usize = 1024;

async fn read_native_stream(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    retained: Arc<std::sync::atomic::AtomicUsize>,
    limit: usize,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(limit.min(8192));
    let mut chunk = vec![0; limit.clamp(1, 8192)];
    loop {
        let length = reader.read(&mut chunk).await.context("read bounded native source stream")?;
        if length == 0 {
            return Ok(bytes);
        }
        let previous = retained.fetch_add(length, std::sync::atomic::Ordering::AcqRel);
        anyhow::ensure!(length <= limit.saturating_sub(previous), "native source exceeded its byte bound");
        bytes.extend_from_slice(&chunk[..length]);
    }
}

async fn bounded_native_child_output(mut child: Child, limit: usize) -> Result<std::process::Output> {
    let stdout = child.stdout.take().context("native child stdout is not piped")?;
    let stderr = child.stderr.take().context("native child stderr is not piped")?;
    let retained = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stderr_retained = Arc::clone(&retained);
    let (status, stdout, stderr) = tokio::try_join!(
        child.wait(),
        async move { read_native_stream(stdout, retained, limit).await.map_err(std::io::Error::other) },
        async move { read_native_stream(stderr, stderr_retained, limit).await.map_err(std::io::Error::other) },
    ).context("bounded native child failed")?;
    Ok(std::process::Output { status, stdout, stderr })
}

async fn read_native_file(path: &Path, limit: usize) -> Result<Vec<u8>> {
    read_native_stream(tokio::fs::File::open(path).await?,
        Arc::new(std::sync::atomic::AtomicUsize::new(0)), limit).await
}

fn native_time_ns(value: &Value) -> Option<String> {
    DateTime::parse_from_rfc3339(value.as_str()?).ok()?.timestamp_nanos_opt()
        .filter(|value| *value > 0).map(|value| value.to_string())
}

fn native_counter(value: &Value) -> Option<String> {
    value.as_u64().map(|value| value.to_string())
}

fn native_container_id<'a>(pod: &'a Value, container: &str) -> Option<&'a str> {
    let mut statuses = pod.pointer("/status/containerStatuses")?.as_array()?.iter()
        .filter(|status| status["name"] == container);
    let id = statuses.next()?["containerID"].as_str().filter(|id| !id.is_empty())?;
    if statuses.next().is_some() { None } else { Some(id) }
}

fn native_pod_sample(target: &AksResourcePodTarget, namespace: &str, pod: &Value, summary: &Value) -> Result<Value> {
    anyhow::ensure!(
        pod.pointer("/metadata/name").and_then(Value::as_str) == Some(&target.pod)
            && pod.pointer("/metadata/namespace").and_then(Value::as_str) == Some(namespace)
            && pod.pointer("/metadata/uid").and_then(Value::as_str) == Some(&target.uid)
            && pod.pointer("/spec/nodeName").and_then(Value::as_str) == Some(&target.node)
            && pod.pointer("/metadata/deletionTimestamp").is_none()
            && summary.pointer("/node/nodeName").and_then(Value::as_str) == Some(&target.node),
        "native pod/node identity is missing or changed"
    );
    let unique = |values: Option<&Vec<Value>>, name: &str| -> Result<Value> {
        let selected = values.into_iter().flatten().filter(|value| value["name"] == name).collect::<Vec<_>>();
        let [value] = selected.as_slice() else { bail!("native container identity is missing or ambiguous") };
        Ok((*value).clone())
    };
    let spec = unique(pod.pointer("/spec/containers").and_then(Value::as_array), &target.container)?;
    let status = unique(pod.pointer("/status/containerStatuses").and_then(Value::as_array), &target.container)?;
    let container_id = status["containerID"].as_str().filter(|id| !id.is_empty())
        .context("native container ID unavailable")?;
    anyhow::ensure!(target.container_id.as_deref() == Some(container_id),
        "native container ID differs from the original already-observed pod readback");
    let summaries = summary["pods"].as_array().context("native pod summaries unavailable")?
        .iter().filter(|entry| entry["podRef"]["uid"] == target.uid && entry["podRef"]["name"] == target.pod
            && entry["podRef"]["namespace"] == namespace).collect::<Vec<_>>();
    let [pod_summary] = summaries.as_slice() else { bail!("native summary pod UID is missing or ambiguous") };
    let container = unique(pod_summary["containers"].as_array(), &target.container)?;
    let started = native_time_ns(&container["startTime"]).context("native summary container start time unavailable")?;
    anyhow::ensure!(
        status.pointer("/state/running/startedAt").or_else(|| status.pointer("/state/terminated/startedAt"))
            .and_then(native_time_ns).as_deref() == Some(&started),
        "native container ID/start-time binding is unavailable or changed"
    );
    Ok(json!({
        "identity": {"role": target.role, "namespace": namespace, "pod": target.pod, "uid": target.uid,
            "node": target.node, "container": target.container, "container_id": container_id, "started_unix_ns": started},
        "limits": spec.pointer("/resources/limits"),
        "cpu": {"time_unix_ns": native_time_ns(&container["cpu"]["time"]), "time_resolution_ns": "1000000000",
            "usage_core_nanoseconds": native_counter(&container["cpu"]["usageCoreNanoSeconds"])},
        "memory": {"time_unix_ns": native_time_ns(&container["memory"]["time"]), "time_resolution_ns": "1000000000",
            "working_set_bytes": native_counter(&container["memory"]["workingSetBytes"])},
        "network": {"time_unix_ns": native_time_ns(&pod_summary["network"]["time"]), "time_resolution_ns": "1000000000",
            "rx_bytes": native_counter(&pod_summary["network"]["rxBytes"]),
            "tx_bytes": native_counter(&pod_summary["network"]["txBytes"]),
            "scope": "pod_network_not_container_or_link_capacity"},
        "throttling": {"value": null, "unavailable_reason": "existing_cadvisor_has_no_original_window_uid_container_id_source_timestamp_binding"},
    }))
}

fn native_cfs_sample(target: &AksResourcePodTarget, namespace: &str, metrics: &str) -> Result<Value> {
    let container_id = target.container_id.as_deref().context("original container ID unavailable")?;
    let (runtime, hash) = container_id.split_once("://").context("container runtime ID unavailable")?;
    anyhow::ensure!(hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "container runtime hash unavailable");
    let scope_prefix = match runtime {
        "containerd" => "cri-containerd-", "docker" => "docker-", "cri-o" => "crio-",
        _ => bail!("unrecognized container runtime cgroup binding"),
    };
    let pod_group = format!("pod{}", target.uid);
    let systemd_pod = format!("-pod{}.slice", target.uid.replace('-', "_"));
    let scope = format!("{scope_prefix}{hash}.scope");
    let mut found = BTreeMap::new();
    for line in metrics.lines() {
        let Some((name, sample)) = line.split_once('{') else { continue };
        if !matches!(name, "container_cpu_cfs_periods_total" | "container_cpu_cfs_throttled_periods_total") { continue; }
        let (labels, values) = sample.rsplit_once('}').context("malformed native cAdvisor sample")?;
        let labels = parse_metric_labels(labels)?;
        if labels.get("namespace").map(String::as_str) != Some(namespace)
            || labels.get("pod").map(String::as_str) != Some(&target.pod)
            || labels.get("container").map(String::as_str) != Some(&target.container) { continue; }
        let id = labels.get("id").context("native cAdvisor cgroup identity unavailable")?;
        let groups = id.split('/').filter(|group| !group.is_empty()).collect::<Vec<_>>();
        anyhow::ensure!(
            groups.iter().any(|group| *group == pod_group || group.ends_with(&systemd_pod))
                && groups.last().is_some_and(|group| *group == hash || *group == scope),
            "native cAdvisor cgroup does not match the original pod UID and container ID"
        );
        let fields = values.split_whitespace().collect::<Vec<_>>();
        anyhow::ensure!(fields.len() == 2, "native cAdvisor counter has no explicit source timestamp");
        let count = fields[0].parse::<f64>()?;
        anyhow::ensure!(count.is_finite() && count >= 0.0 && count.fract() == 0.0
            && count <= 9_007_199_254_740_991.0, "native cAdvisor counter is not an exact bounded integer");
        let timestamp = fields[1].parse::<u64>()?.checked_mul(1_000_000)
            .filter(|value| *value > 0).context("native cAdvisor source timestamp invalid")?;
        anyhow::ensure!(found.insert(name, (count as u64, timestamp)).is_none(), "native cAdvisor counter is ambiguous");
    }
    let (periods, timestamp) = found.get("container_cpu_cfs_periods_total").context("native CPU periods unavailable")?;
    let (throttled, throttled_timestamp) = found.get("container_cpu_cfs_throttled_periods_total")
        .context("native throttled CPU periods unavailable")?;
    anyhow::ensure!(timestamp == throttled_timestamp && throttled <= periods,
        "native cAdvisor timestamps or counter values are inconsistent");
    Ok(json!({"time_unix_ns": timestamp.to_string(), "time_resolution_ns": "1000000", "periods": periods.to_string(),
        "throttled_periods": throttled.to_string()}))
}

async fn native_kubernetes_text(config: &OatConfig, source: &NativeTelemetrySource, args: Vec<&str>) -> Result<String> {
    let context = source.context.as_deref().context("native source has no explicit selected kube context")?;
    anyhow::ensure!(config.kubectl_context.as_deref() == Some(context), "native selected kube context changed");
    let mut exact = vec!["--server", &source.server, "--insecure-skip-tls-verify=false"];
    exact.extend(args);
    kubectl_with_output_bound(config, exact, None, Some(OFFICIAL_MEASUREMENT_MAX_BYTES)).await
}

async fn native_kubernetes_get(config: &OatConfig, source: &NativeTelemetrySource, args: Vec<&str>) -> Result<Value> {
    let body = native_kubernetes_text(config, source, args).await?;
    serde_json::from_str(&body).context("native Kubernetes source is not valid JSON")
}

async fn run_native_observation<F, O>(observe: F) -> Result<Value>
where
    F: FnOnce(Duration) -> O + Send + 'static,
    O: std::future::Future<Output = Result<Value>>,
{
    let deadline = std::time::Instant::now() + PUBLICATION_BACKLOG_REQUEST_TIMEOUT;
    let (_cancel, mut cancelled) = watch::channel(());
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        anyhow::ensure!(!remaining.is_zero(), "native QAP original sample deadline expired before dispatch");
        runtime.block_on(async {
            tokio::select! {
                biased;
                _ = cancelled.changed() => bail!("native QAP source capture cancelled"),
                observation = observe(remaining) => observation,
            }
        })
    }).await.context("join existing native QAP observation runtime")?
}

async fn capture_native_telemetry(source: &Arc<NativeTelemetrySource>, config: &OatConfig) -> Result<Value> {
    let started = unix_time_ms_u64()?;
    let qap = async {
        tokio::fs::create_dir_all(&source.results).await?;
        let observer = Arc::clone(source);
        let observation = run_native_observation(move |remaining| async move {
            arrowcol_loadgen::observe_qap_with_mode(
                &observer.qap_endpoint, &observer.results, arrowcol_loadgen::ObservationMode::Azure,
                &observer.qap_database, &observer.qap_tables, remaining,
            ).await
        }).await?;
        Ok::<_, anyhow::Error>(json!({
            "observed_unix_ms": observation["observed_unix_ms"],
            "tables": source.qap_tables.iter().map(|name| {
                let table = &observation["tables"][name];
                (name.clone(), json!({"available": table["available"], "statistics": table["statistics"],
                    "state": table["state"]}))
            }).collect::<BTreeMap<_, _>>(),
        }))
    };
    let kubernetes = async {
        let nodes = source.targets.iter().map(|target| target.node.clone()).collect::<BTreeSet<_>>();
        let summaries = join_all(nodes.iter().map(|node| async move {
            if node.is_empty() || !node.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.')) {
                return (node.clone(), (None, None));
            }
            let path = format!("/api/v1/nodes/{node}/proxy/stats/summary");
            let cadvisor_path = format!("/api/v1/nodes/{node}/proxy/metrics/cadvisor");
            let (summary, cadvisor) = tokio::join!(
                native_kubernetes_get(config, source, vec!["get", "--raw", &path, "--request-timeout=3s"]),
                native_kubernetes_text(config, source, vec!["get", "--raw", &cadvisor_path, "--request-timeout=3s"]),
            );
            (node.clone(), (summary.ok(), cadvisor.ok()))
        })).await.into_iter().collect::<HashMap<_, _>>();
        join_all(source.targets.iter().map(|target| {
            let summaries = &summaries;
            async move {
                let observation = async {
                    let summary = summaries.get(&target.node).and_then(|(summary, _)| summary.as_ref())
                        .context("native exact-node summary unavailable")?;
                    let pod = native_kubernetes_get(config, source, vec!["get", "pod", &target.pod,
                        "--namespace", &config.namespace, "-o", "json", "--request-timeout=3s"]).await?;
                    let mut sample = native_pod_sample(target, &config.namespace, &pod, summary)?;
                    if let Some((_, Some(metrics))) = summaries.get(&target.node) {
                        sample["throttling"] = native_cfs_sample(target, &config.namespace, metrics)
                            .unwrap_or_else(|_| json!({"value": null,
                                "unavailable_reason": "native_cadvisor_source_timestamp_or_exact_cgroup_binding_unavailable"}));
                    }
                    Ok::<_, anyhow::Error>(sample)
                }.await;
                observation.unwrap_or_else(|_| json!({
                    "identity": target, "unavailable_reason": "native_exact_pod_identity_credentials_or_summary_unavailable"
                }))
            }
        })).await
    };
    let (qap, pods) = tokio::join!(
        tokio::time::timeout(PUBLICATION_BACKLOG_REQUEST_TIMEOUT, qap),
        tokio::time::timeout(PUBLICATION_BACKLOG_REQUEST_TIMEOUT, kubernetes),
    );
    Ok(json!({
        "request_start_unix_ms": started, "request_end_unix_ms": unix_time_ms_u64()?,
        "qap": qap.ok().and_then(Result::ok)
            .unwrap_or_else(|| json!({"unavailable_reason": "native_qap_observation_unavailable"})),
        "pods": pods.unwrap_or_else(|_| source.targets.iter().map(|target| json!({
            "identity": target, "unavailable_reason": "native_exact_kubernetes_observation_deadline_expired"
        })).collect::<Vec<_>>()),
    }))
}

struct ResourceSampler {
    task: Option<JoinHandle<Result<CellEvidence>>>,
    signal: watch::Receiver<GuardSignal>,
    stop: watch::Sender<bool>,
}

const PUBLICATION_BACKLOG_SAMPLING_INTERVAL: Duration = Duration::from_secs(1);
const PUBLICATION_BACKLOG_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

fn publication_backlog_timeout_ms(request_start: u64, window_start: u64, window_end: u64) -> u64 {
    let original = PUBLICATION_BACKLOG_REQUEST_TIMEOUT.as_millis() as u64;
    if (window_start..window_end).contains(&request_start) {
        original.min(window_end - request_start)
    } else {
        original
    }
}

async fn bounded_publication_backlog_scrape(
    scraper: Option<&PublicationBacklogScraper>,
    deadline: tokio::time::Instant,
) -> Result<(u64, Result<PublicationBacklogMetrics>)> {
    tokio::time::timeout_at(deadline, async {
        let result = match scraper {
            Some(scraper) => scraper.scrape().await,
            None => Err(anyhow::anyhow!("publication backlog metrics endpoint unavailable")),
        };
        Ok((unix_time_ms_u64()?, result))
    }).await.context("publication backlog scrape exceeded its bounded deadline")?
}

async fn sample_resources(source: &ResourceSamplerSource, config: &OatConfig) -> Result<ResourceMetrics> {
    match source {
        ResourceSamplerSource::Aks | ResourceSamplerSource::AksNative(_) => collect_aks_metrics(config).await,
        ResourceSamplerSource::Local { run_id, environment, .. } => collect_local_metrics(config, run_id, environment).await,
    }
}

fn retain_resource_sample(evidence: &mut CellEvidence, sample: ResourceSample) {
    if evidence.resource_samples.len() == 4096 {
        evidence.resource_samples.remove(0);
        evidence.resource_samples_dropped += 1;
    }
    evidence.resource_samples.push(sample);
}

impl Drop for ResourceSampler {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl ResourceSampler {
    fn start(source: Option<ResourceSamplerSource>, config: OatConfig, start: u64, end: u64,
        prepared: Option<PreparedGuard>) -> Self {
        let native = match &source {
            Some(ResourceSamplerSource::AksNative(native)) => Some(Arc::clone(native)),
            _ => None,
        };
        let (signal, receiver) = watch::channel(if prepared.is_some() {
            GuardSignal::Monitoring
        } else {
            GuardSignal::Disabled
        });
        let (stop, mut stop_receiver) = watch::channel(false);
        let task = source.map(|source| {
            tokio::spawn(async move {
                let mut evidence = CellEvidence {
                    backlog_scrape_configured: true,
                    ..Default::default()
                };
                let mut native_samples = Vec::new();
                let mut native_failures = 0_u64;
                let mut native_dropped = 0_u64;
                let mut native_tasks = tokio::task::JoinSet::<Result<Value>>::new();
                let mut last_native = None;
                let (backlog_scraper, mut guard) = if let Some(prepared) = prepared {
                    (Some(prepared.scraper), Some((prepared.probe, prepared.engine)))
                } else {
                    (match PublicationBacklogScraper::start(&source, &config).await {
                        Ok(scraper) => Some(scraper),
                        Err(error) => {
                            tracing::warn!(%error, "publication backlog metrics endpoint unavailable");
                            None
                        }
                    }, None)
                };
                let mut interval = tokio::time::interval(PUBLICATION_BACKLOG_SAMPLING_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut resource_tasks = tokio::task::JoinSet::<Result<ResourceSample>>::new();
                let mut quota_start = None;
                let mut quota_end = None;
                loop {
                    tokio::select! {
                        _ = interval.tick() => {}
                        _ = stop_receiver.changed() => break,
                    }
                    let now = unix_time_ms_u64()?;
                    if let ResourceSamplerSource::Local { run_id, environment, .. } = &source {
                        if now >= start && now < end && quota_start.is_none() {
                            quota_start = Some(local_cpu_quota_observation(&config, run_id, environment).await);
                        } else if now >= end && quota_end.is_none() {
                            quota_end = Some(local_cpu_quota_observation(&config, run_id, environment).await);
                        }
                    }
                    while let Some(sample) = native_tasks.try_join_next() {
                        match sample {
                            Ok(Ok(sample)) if native_samples.len() < NATIVE_TELEMETRY_MAX_SAMPLES => native_samples.push(sample),
                            Ok(Ok(_)) => native_dropped += 1,
                            _ => native_failures += 1,
                        }
                    }
                    if now >= end && guard.is_none() {
                        break;
                    }
                    if let Some(source) = &native
                        && now.saturating_add(NATIVE_TELEMETRY_INTERVAL_MS) >= start
                            && last_native.is_none_or(|last| now.saturating_sub(last) >= NATIVE_TELEMETRY_INTERVAL_MS)
                            && native_tasks.is_empty()
                        {
                            last_native = Some(now);
                            let source = Arc::clone(source);
                            let config = config.clone();
                            native_tasks.spawn(async move {
                                capture_native_telemetry(&source, &config).await
                            });
                    }
                    if now < start && guard.is_none() {
                        continue;
                    }
                    while let Some(sample) = resource_tasks.try_join_next() {
                        match sample {
                            Ok(Ok(sample)) if sample.sampled_at_unix_ms >= start && sample.sampled_at_unix_ms < end => {
                                retain_resource_sample(&mut evidence, sample);
                            }
                            _ => {
                                evidence.resource_sampling_failures += 1;
                                tracing::warn!("asynchronous resource sample unavailable or outside measurement window");
                            }
                        }
                    }
                    let asynchronous_resources = guard.is_some();
                    if asynchronous_resources && resource_tasks.is_empty() && now >= start && now < end {
                        let source = source.clone();
                        let config = config.clone();
                        resource_tasks.spawn(async move {
                            let metrics = tokio::time::timeout(PUBLICATION_BACKLOG_REQUEST_TIMEOUT, sample_resources(&source, &config))
                                .await.context("resource sample deadline exceeded")??;
                            Ok::<_, anyhow::Error>(ResourceSample { sampled_at_unix_ms:unix_time_ms_u64()?, metrics })
                        });
                    }
                    let request_anchor = tokio::time::Instant::now();
                    let request_start_unix_ms = unix_time_ms_u64()?;
                    if request_start_unix_ms >= end && guard.is_none() {
                        break;
                    }
                    let sample = async {
                        if asynchronous_resources {
                            Ok(None)
                        } else {
                            sample_resources(&source, &config).await.map(Some)
                        }
                    };
                    let timeout_ms = publication_backlog_timeout_ms(request_start_unix_ms, start, end);
                    let other_timeout_ms = if guard.is_some() {
                        PUBLICATION_BACKLOG_REQUEST_TIMEOUT.as_millis() as u64
                    } else {
                        timeout_ms
                    };
                    let expected_identity = guard.as_ref()
                        .and_then(|(probe, _)| probe.expected_scale_out_identity.clone());
                    let guard_api = guard.as_ref().and_then(|(probe, _)| probe.api.clone());
                    #[cfg(unix)]
                    let local_memory = guard.as_ref().and_then(|(probe, _)| probe.local_memory.clone());
                    let deadline = request_anchor + Duration::from_millis(other_timeout_ms);
                    let guard_reading = async {
                        match (&mut guard, &backlog_scraper) {
                            (Some((probe, engine)), Some(scraper)) => probe
                                .read(&config, scraper, &mut engine.proof["admission_diagnostics"], deadline)
                                .await
                                .map(Some),
                            _ => Ok(None),
                        }
                    };
                    let (resource_result, (guard_result, backlog_result, identity)) = tokio::join!(
                        tokio::time::timeout(Duration::from_millis(other_timeout_ms), sample),
                        guarded_composite_observation(
                            guard_reading,
                            bounded_publication_backlog_scrape(backlog_scraper.as_ref(),
                                request_anchor + Duration::from_millis(timeout_ms)),
                            async {
                                verify_guard_identity(&config, expected_identity.as_ref(), guard_api.as_deref()).await?;
                                #[cfg(unix)]
                                if let Some(local) = &local_memory {
                                    local.inspect(&local.get("/json").await?)?;
                                }
                                Ok(())
                            },
                            deadline,
                        ),
                    );
                    if let Some((_, engine)) = &mut guard {
                        record_guard_read_leg(&mut engine.proof["admission_diagnostics"], "collector_identity_after_backlog", &identity);
                    }
                    let guard_result = match guard_result {
                        Ok(Some(reading)) => identity.into_result().map(|_| Some(reading)),
                        Ok(None) => Ok(None),
                        Err(error) => Err(error),
                    };
                    if let Err(error) = &guard_result
                        && let Some((_, engine)) = &mut guard
                    {
                        record_guard_read_error(&mut engine.proof["admission_diagnostics"], error);
                    }
                    match resource_result {
                        Ok(Ok(Some(metrics))) => {
                            let sampled_at_unix_ms = unix_time_ms_u64()?;
                            if sampled_at_unix_ms >= start && sampled_at_unix_ms < end {
                                retain_resource_sample(&mut evidence, ResourceSample {
                                    sampled_at_unix_ms,
                                    metrics,
                                });
                            } else if guard.is_none() {
                                evidence.resource_sampling_failures += 1;
                            }
                        }
                        Ok(Ok(None)) => {}
                        _ => {
                            evidence.resource_sampling_failures += 1;
                            tracing::warn!("resource sample unavailable or exceeded its bounded deadline");
                        }
                    }
                    let (request_end_unix_ms, backlog_result) = match backlog_result {
                        Ok((request_end, result)) => (request_end, result),
                        _ => (unix_time_ms_u64()?, Err(anyhow::anyhow!("publication backlog scrape exceeded its bounded deadline"))),
                    };
                    let backlog_sample = match backlog_result {
                        Ok(metrics) => publication_backlog_sample(
                            metrics,
                            request_start_unix_ms,
                            request_end_unix_ms,
                            timeout_ms,
                        ),
                        _ => {
                            evidence.backlog_sampling_failures += 1;
                            failed_publication_backlog_sample(
                                request_start_unix_ms,
                                request_end_unix_ms,
                                timeout_ms,
                                if backlog_scraper.is_some() {
                                    "scrape_failed"
                                } else {
                                    "endpoint_unavailable"
                                },
                            )
                        }
                    };
                    if let Some((_, engine)) = &mut guard {
                        let observation = match guard_result {
                            Ok(Some(reading)) => Ok(reading),
                            Ok(None) => Err(anyhow::anyhow!("live cap or ingestion commit readback unavailable")),
                            Err(error) => Err(error),
                        };
                        engine.record(observation, &backlog_sample);
                        if let Some(reason) = engine.failure() {
                            signal.send_replace(GuardSignal::Stop(reason));
                            break;
                        }
                        if now >= end {
                            engine.proof["status"] = json!("complete");
                            signal.send_replace(GuardSignal::Complete);
                            break;
                        }
                    }
                    if valid_backlog_request_interval(request_start_unix_ms, request_end_unix_ms, start, end) {
                        if evidence.backlog_samples.len() == 4096 {
                            evidence.backlog_samples.remove(0);
                            evidence.backlog_samples_dropped += 1;
                        }
                        evidence.backlog_samples.push(backlog_sample);
                    } else if guard.is_none() || now >= start && now < end {
                        evidence.backlog_sampling_failures += 1;
                    }
                }
                if matches!(&source, ResourceSamplerSource::Local { .. }) {
                    evidence.cpu_quota = Some(local_cpu_quota_window(&config, start, end, quota_start.as_ref(), quota_end.as_ref()));
                }
                evidence.resource_sampling_failures += resource_tasks.len() as u64;
                if let Some(source) = native {
                    native_failures += native_tasks.len() as u64;
                    native_tasks.abort_all();
                    while native_tasks.join_next().await.is_some() {}
                    let stopped = *stop_receiver.borrow() || matches!(&*signal.borrow(), GuardSignal::Stop(_));
                    if unix_time_ms_u64()? >= end && !stopped
                    {
                        match capture_native_telemetry(&source, &config).await {
                            Ok(sample) if native_samples.len() < NATIVE_TELEMETRY_MAX_SAMPLES => native_samples.push(sample),
                            Ok(_) => native_dropped += 1,
                            Err(_) => native_failures += 1,
                        }
                    }
                    evidence.native_telemetry = Some(json!({
                        "schema_version": 1, "scope": "original_cell_source_observations_not_scale_authorization",
                        "binding": source.binding, "pod_targets": source.targets,
                        "window_start_unix_ms": start, "window_end_unix_ms": end,
                        "cadence_ms": NATIVE_TELEMETRY_INTERVAL_MS, "sample_timeout_ms": 3000,
                        "samples": native_samples, "sampling_failures": native_failures,
                        "samples_dropped": native_dropped,
                        "throttling_scope": "explicit_native_cadvisor_source_timestamps_and_exact_pod_uid_container_id_only_not_legacy_whole_load_ratio",
                        "network_saturation_unavailable_reason": "native_pod_network_has_no_authenticated_link_capacity",
                    }));
                }
                if let Some((_, engine)) = guard {
                    evidence.overload_guard = Some(engine.proof);
                    evidence.overload_stop = engine.stop;
                }
                Ok(evidence)
            })
        });
        Self { task, signal: receiver, stop }
    }

    async fn wait_for_overload_stop(&mut self) -> String {
        loop {
            let signal = self.signal.borrow().clone();
            match signal {
                GuardSignal::Stop(reason) => return reason,
                GuardSignal::Complete | GuardSignal::Disabled => return std::future::pending().await,
                GuardSignal::Monitoring => {}
            }
            if self.signal.changed().await.is_err() {
                return "overload_guard monitor ended without complete protection evidence".to_owned();
            }
        }
    }

    async fn finish_early(self, evidence: &mut CellEvidence) -> Result<()> {
        self.stop.send_replace(true);
        self.finish(evidence).await
    }

    async fn finish(mut self, evidence: &mut CellEvidence) -> Result<()> {
        if let Some(task) = self.task.as_mut() {
            let sampled = task.await.context("resource sampler task failed")??;
            self.task.take();
            if sampled.overload_guard.is_some() {
                evidence.overload_guard = sampled.overload_guard;
                evidence.overload_stop = sampled.overload_stop;
            }
            evidence.resource_samples = sampled.resource_samples;
            evidence.cpu_quota = sampled.cpu_quota;
            if sampled.native_telemetry.is_some() {
                evidence.native_telemetry = sampled.native_telemetry;
            }
            evidence.resource_sampling_failures = sampled.resource_sampling_failures;
            evidence.resource_samples_dropped = sampled.resource_samples_dropped;
            evidence.backlog_scrape_configured = sampled.backlog_scrape_configured;
            evidence.backlog_samples = sampled.backlog_samples;
            evidence.backlog_sampling_failures = sampled.backlog_sampling_failures;
            evidence.backlog_samples_dropped = sampled.backlog_samples_dropped;
        }
        Ok(())
    }
}

#[cfg(test)]
mod measurement_evidence_tests {
    use super::*;

    #[tokio::test]
    async fn native_observation_runtime_cancels_pending_source_when_sampler_is_dropped() -> Result<()> {
        struct NotifyDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for NotifyDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() { let _ = sender.send(()); }
            }
        }
        let (ready, started) = tokio::sync::oneshot::channel();
        let (dropped, observed_drop) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(run_native_observation(move |remaining| async move {
            assert!(remaining <= PUBLICATION_BACKLOG_REQUEST_TIMEOUT);
            let _guard = NotifyDrop(Some(dropped));
            ready.send(()).map_err(|_| anyhow::anyhow!("source start receiver closed"))?;
            std::future::pending::<Result<Value>>().await
        }));
        tokio::time::timeout(Duration::from_secs(2), started).await??;
        task.abort();
        assert!(task.await.err().context("aborted observer must cancel")?.is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), observed_drop).await??;
        Ok(())
    }

    #[tokio::test]
    async fn native_child_cap_counts_combined_raw_bytes_before_utf8_and_exit() -> Result<()> {
        for limit in [2, 3] {
            let child = Command::new("sh").args(["-c", "printf '\\303\\251'; printf x >&2"])
                .stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true).spawn()?;
            let output = bounded_native_child_output(child, limit).await;
            if limit == 3 {
                let output = output?;
                assert!(output.status.success());
                assert_eq!(output.stdout, [0xc3, 0xa9]);
                assert_eq!(output.stderr, b"x");
            } else {
                assert!(format!("{:#}", output.err().context("oversized child must fail")?).contains("byte bound"));
            }
        }
        let child = Command::new("sh").args(["-c", "printf 12345; printf 67890 >&2; exit 0"])
            .stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true).spawn()?;
        assert!(bounded_native_child_output(child, 9).await.is_err(), "a successful process exit cannot hide overflow");
        Ok(())
    }

    #[test]
    fn native_cfs_requires_source_timestamps_and_exact_cgroup_uid_and_container_hash() -> Result<()> {
        let hash = "a".repeat(64);
        let target = AksResourcePodTarget {
            role: "collector".to_owned(), pod: "collector-pod".to_owned(), uid: "pod-uid".to_owned(),
            container: "collector".to_owned(), node: "node-1".to_owned(),
            container_id: Some(format!("containerd://{hash}")),
        };
        let sample = |timestamp: &str, uid: &str, hash: &str| format!(
            "container_cpu_cfs_periods_total{{namespace=\"oat\",pod=\"collector-pod\",container=\"collector\",id=\"/kubepods/pod{uid}/{hash}\",extra=\"escaped,comma\"}} 100{timestamp}\n\
             container_cpu_cfs_throttled_periods_total{{namespace=\"oat\",pod=\"collector-pod\",container=\"collector\",id=\"/kubepods/pod{uid}/{hash}\"}} 10{timestamp}\n"
        );
        let valid = sample(" 1767225660123", &target.uid, &hash);
        let observed = native_cfs_sample(&target, "oat", &valid)?;
        assert_eq!(observed["time_unix_ns"], "1767225660123000000");
        assert_eq!(observed["periods"], "100");
        for invalid in [sample("", &target.uid, &hash), sample(" 1767225660123", "other-uid", &hash),
            sample(" 1767225660123", &target.uid, &"b".repeat(64)), format!("{valid}{valid}")] {
            assert!(native_cfs_sample(&target, "oat", &invalid).is_err());
        }
        Ok(())
    }

    #[test]
    fn native_pod_source_binds_uid_node_container_epoch_and_exact_large_counters() -> Result<()> {
        let target = AksResourcePodTarget {
            role: "collector".to_owned(), pod: "collector-pod".to_owned(), uid: "pod-uid".to_owned(),
            container: "collector".to_owned(), node: "node-1".to_owned(),
            container_id: Some("containerd://original".to_owned()),
        };
        let pod = json!({
            "metadata": {"name": "collector-pod", "namespace": "oat", "uid": "pod-uid"},
            "spec": {"nodeName": "node-1", "containers": [{"name": "collector", "resources": {"limits": {"cpu": "2", "memory": "1Gi"}}}]},
            "status": {"containerStatuses": [{"name": "collector", "containerID": "containerd://original",
                "state": {"running": {"startedAt": "2026-01-01T00:00:00Z"}}}]},
        });
        let summary = json!({
            "node": {"nodeName": "node-1"}, "pods": [{
                "podRef": {"uid": "pod-uid", "name": "collector-pod", "namespace": "oat"},
                "containers": [{"name": "collector", "startTime": "2026-01-01T00:00:00Z",
                    "cpu": {"time": "2026-01-01T00:01:00.123456789Z", "usageCoreNanoSeconds": 9_007_199_254_740_993_u64},
                    "memory": {"time": "2026-01-01T00:01:00.123456789Z", "workingSetBytes": 123}}],
                "network": {"time": "2026-01-01T00:01:00.123456789Z", "rxBytes": 345, "txBytes": 678},
            }],
        });
        let observed = native_pod_sample(&target, "oat", &pod, &summary)?;
        assert_eq!(observed["cpu"]["usage_core_nanoseconds"], "9007199254740993");
        assert_eq!(observed["cpu"]["time_unix_ns"], "1767225660123456789");
        assert!(observed["throttling"]["value"].is_null());
        for pointer in ["/metadata/uid", "/spec/nodeName", "/status/containerStatuses/0/containerID",
            "/status/containerStatuses/0/state/running/startedAt"] {
            let mut changed = pod.clone();
            *changed.pointer_mut(pointer).context("fixture field")? = json!("changed");
            assert!(native_pod_sample(&target, "oat", &changed, &summary).is_err(), "{pointer}");
        }
        let mut changed = summary.clone();
        changed["pods"][0]["podRef"]["uid"] = json!("other-pod");
        assert!(native_pod_sample(&target, "oat", &pod, &changed).is_err());
        let mut missing = target.clone();
        missing.container_id = None;
        assert!(native_pod_sample(&missing, "oat", &pod, &summary).is_err());
        let start = 1_800_000_000_000_i64 - 60_000;
        let namespace = "synthetic-offline-c0ffee00000040008000000000000003";
        let mut native_target = target.clone();
        native_target.uid = "synthetic-pod-uid".to_owned();
        native_target.node = "synthetic-node".to_owned();
        native_target.container_id = Some(format!("containerd://{}", "a".repeat(64)));
        let mut native_pod = pod.clone();
        native_pod["metadata"]["namespace"] = json!(namespace);
        native_pod["metadata"]["uid"] = json!(native_target.uid);
        native_pod["spec"]["nodeName"] = json!(native_target.node);
        native_pod["status"]["containerStatuses"][0]["containerID"] = json!(native_target.container_id);
        let mut native_summary = summary.clone();
        native_summary["node"]["nodeName"] = json!(native_target.node);
        native_summary["pods"][0]["podRef"]["namespace"] = json!(namespace);
        native_summary["pods"][0]["podRef"]["uid"] = json!(native_target.uid);
        let mut produced = Vec::new();
        for (index, at) in [start - 1000, start + 9000, start + 19_000, start + 31_000].into_iter().enumerate() {
            let time = DateTime::from_timestamp_millis(at + 50).context("synthetic native timestamp")?
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            native_summary["pods"][0]["containers"][0]["cpu"]["time"] = json!(time);
            native_summary["pods"][0]["containers"][0]["cpu"]["usageCoreNanoSeconds"] =
                json!(9_007_199_254_740_993_u64 + (at + 50 - start + 1000) as u64 * 2_000_000);
            native_summary["pods"][0]["containers"][0]["memory"] = json!({"time":time,"workingSetBytes":1_000_000_000});
            native_summary["pods"][0]["network"] = json!({"time":time,
                "rxBytes":(at + 50 - start + 1000) as u64 * 1000,"txBytes":(at + 50 - start + 1000) as u64 * 2000});
            let mut observed = native_pod_sample(&native_target, namespace, &native_pod, &native_summary)?;
            let labels = format!("namespace=\"{namespace}\",pod=\"collector-pod\",container=\"collector\",id=\"/kubepods/pod{}/{}\"",
                native_target.uid, "a".repeat(64));
            let metrics = format!("container_cpu_cfs_periods_total{{{labels}}} {} {}\ncontainer_cpu_cfs_throttled_periods_total{{{labels}}} {} {}\n",
                1000 + index * 100, at + 50, 100 + index * 20, at + 50);
            observed["throttling"] = native_cfs_sample(&native_target, namespace, &metrics)?;
            produced.push(observed);
        }
        println!("D46_NATIVE_PRODUCER_PODS {}", serde_json::to_string(&produced)?);
        Ok(())
    }

    fn publication_backlog_exposition(available: u8, publications: &str, rows: &str, bytes: &str, age: &str, status: &str) -> String {
        format!(
            "# HELP arrowcol_publication_backlog_available availability\n\
             # TYPE arrowcol_publication_backlog_available gauge\n\
             arrowcol_publication_backlog_available {available}\n\
             arrowcol_publication_backlog_publications {publications}\n\
             arrowcol_publication_backlog_rows {rows}\n\
             arrowcol_publication_backlog_bytes {bytes}\n\
             arrowcol_publication_backlog_oldest_age_seconds {age}\n\
             arrowcol_publication_backlog_status{{status=\"{status}\"}} 1\n"
        )
    }

    fn pipeline_histogram_exposition() -> String {
        let mut output = String::from("process_cpu_seconds_total 123.0\n");
        for stage in ["collect", "build", "sort", "encode", "upload", "commit"] {
            let labels = format!("run=\"r\",variant=\"v\",component=\"arrowcol-dataflow-rust\",stage=\"{stage}\"");
            output.push_str(&format!("arrowcol_pipeline_stage_duration_seconds_bucket{{{labels},le=\"0.5\"}} 1\n"));
            output.push_str(&format!("arrowcol_pipeline_stage_duration_seconds_bucket{{{labels},le=\"1.0\"}} 2\n"));
            output.push_str(&format!("arrowcol_pipeline_stage_duration_seconds_bucket{{{labels},le=\"+Inf\"}} 2\n"));
            output.push_str(&format!("arrowcol_pipeline_stage_duration_seconds_sum{{{labels}}} 0.125\n"));
            output.push_str(&format!("arrowcol_pipeline_stage_duration_seconds_count{{{labels}}} 2\n"));
        }
        let labels = "run=\"r\",variant=\"v\",component=\"arrowcol-dataflow-rust\"";
        output.push_str(&format!("arrowcol_pipeline_submission_wait_seconds_bucket{{{labels},le=\"0.5\"}} 1\n"));
        output.push_str(&format!("arrowcol_pipeline_submission_wait_seconds_bucket{{{labels},le=\"1.0\"}} 2\n"));
        output.push_str(&format!("arrowcol_pipeline_submission_wait_seconds_bucket{{{labels},le=\"+Inf\"}} 2\n"));
        output.push_str(&format!("arrowcol_pipeline_submission_wait_seconds_sum{{{labels}}} 0.25\n"));
        output.push_str(&format!("arrowcol_pipeline_submission_wait_seconds_count{{{labels}}} 2\n"));
        output
    }

    #[tokio::test]
    async fn backlog_scrape_honors_the_actual_remaining_window_deadline() -> Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        assert_eq!(publication_backlog_timeout_ms(1_000, 1_000, 10_000), 3_000);
        assert_eq!(publication_backlog_timeout_ms(9_000, 1_000, 10_000), 1_000);
        assert_eq!(publication_backlog_timeout_ms(10_000, 1_000, 10_000), 3_000);
        for delay in [10_u64, 500] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let body = publication_backlog_exposition(1, "2", "7", "256", "1.25", "available");
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await?;
                let mut request = [0; 1024];
                if stream.read(&mut request).await? == 0 {
                    return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                }
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
                stream.write_all(response.as_bytes()).await
            });
            let scraper = PublicationBacklogScraper {
                endpoint: format!("http://{address}/metrics"),
                _tunnel: None,
                client: reqwest::Client::builder().timeout(PUBLICATION_BACKLOG_REQUEST_TIMEOUT).build()?,
            };
            let anchor = tokio::time::Instant::now();
            let request_start = unix_time_ms_u64()?;
            let budget = publication_backlog_timeout_ms(request_start, request_start, request_start + 200);
            let result = bounded_publication_backlog_scrape(Some(&scraper), anchor + Duration::from_millis(budget)).await;
            if delay < budget {
                let (request_end, metrics) = result?;
                assert!(request_end <= request_start + budget);
                assert_eq!(metrics?.rows, Some(7));
                server.await??;
            } else {
                assert!(result.is_err());
                server.abort();
            }
        }
        Ok(())
    }

    #[test]
    fn pipeline_histogram_snapshots_preserve_all_stages_and_submission_wait() -> Result<()> {
        let metrics = parse_pipeline_stage_metrics(&pipeline_histogram_exposition())
            .context("pipeline histograms were not parsed")?;
        assert_eq!(metrics["stage_duration_seconds"]["encode"]["count"], 2);
        assert_eq!(metrics["stage_duration_seconds"]["upload"]["sum_seconds"], 0.125);
        assert_eq!(metrics["stage_duration_seconds"]["commit"]["buckets"][2]["le"], "+Inf");
        assert_eq!(metrics["temporality"], "cumulative_process_lifetime_snapshot");
        assert_eq!(metrics["submission_wait_seconds"]["count"], 2);
        assert_eq!(metrics["submission_wait_seconds"]["sum_seconds"], 0.25);
        Ok(())
    }

    #[test]
    fn pipeline_histogram_rejects_malformed_bucket_distributions() {
        let valid = pipeline_histogram_exposition();
        let corrupt = |old: &str, new: &str| {
            assert!(valid.contains(old), "fixture did not contain {old}");
            valid.replacen(old, new, 1)
        };
        let negative_sum = corrupt("stage=\"collect\"} 0.125", "stage=\"collect\"} -0.125");
        let decreasing = corrupt("stage=\"collect\",le=\"0.5\"} 1", "stage=\"collect\",le=\"0.5\"} 2")
            .replacen("stage=\"collect\",le=\"1.0\"} 2", "stage=\"collect\",le=\"1.0\"} 1", 1);
        let finite_exceeds_count = corrupt("stage=\"collect\",le=\"1.0\"} 2", "stage=\"collect\",le=\"1.0\"} 3");
        let duplicate_numeric_bound = corrupt("stage=\"collect\",le=\"1.0\"", "stage=\"collect\",le=\"0.50\"");
        let infinite_mismatch = corrupt("stage=\"collect\",le=\"+Inf\"} 2", "stage=\"collect\",le=\"+Inf\"} 3");
        for malformed in [negative_sum, decreasing, finite_exceeds_count, duplicate_numeric_bound, infinite_mismatch] {
            assert!(parse_pipeline_stage_metrics(&malformed).is_none());
        }
    }

    #[test]
    fn publication_backlog_parser_preserves_unavailable_and_rejects_bad_gauges() -> Result<()> {
        let exposition = format!(
            "unrelated_metric 123 1700000000\n{}",
            publication_backlog_exposition(1, "2", "7", "256", "1.25", "available")
        );
        let available = parse_publication_backlog_metrics(&exposition)?;
        assert_eq!(
            available,
            PublicationBacklogMetrics {
                available: true,
                publications: Some(2),
                rows: Some(7),
                logical_bytes: Some(256),
                oldest_age_seconds: Some(1.25),
                status: "available".to_owned(),
                pipeline_stage_metrics: None,
                pipeline_stage_metrics_unavailable_reason: Some("exporter_stage_histograms_missing_or_malformed".to_owned()),
            }
        );
        let unavailable = parse_publication_backlog_metrics(&publication_backlog_exposition(
            0, "NaN", "NaN", "NaN", "NaN", "unknown_outcome",
        ))?;
        assert_eq!(unavailable.publications, None);
        assert_eq!(unavailable.rows, None);
        assert_eq!(unavailable.logical_bytes, None);
        assert_eq!(unavailable.oldest_age_seconds, None);
        assert_eq!(unavailable.status, "unknown_outcome");
        assert!(
            parse_publication_backlog_metrics(&publication_backlog_exposition(
                0, "0", "0", "0", "0", "unconfigured",
            ))
            .is_err()
        );
        assert!(
            parse_publication_backlog_metrics(&publication_backlog_exposition(
                1, "NaN", "7", "256", "1.25", "available",
            ))
            .is_err()
        );
        assert!(
            parse_publication_backlog_metrics(&publication_backlog_exposition(
                1, "2", "7", "256", "-0.1", "available",
            ))
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn publication_backlog_fixture_covers_one_timestamped_150_second_cell() -> Result<()> {
        let start = 1_700_000_000_000_u64;
        let end = start + 150_000;
        let mut evidence = CellEvidence {
            measurement_window: Some((start, end)),
            backlog_scrape_configured: true,
            ..Default::default()
        };
        for second in 0..150_u64 {
            let offset = second * 1_000;
            let request_start = start + offset;
            let exposition = publication_backlog_exposition(
                1,
                "3",
                &(120 - (second % 30)).to_string(),
                "8192",
                "2.5",
                "available",
            );
            let metrics = parse_publication_backlog_metrics(&exposition)?;
            evidence.backlog_samples.push(publication_backlog_sample(
                metrics,
                request_start,
                request_start + 20,
                3_000.min(end - request_start),
            ));
        }
        let evidence = evidence.for_table("table", None)["backlog"].clone();
        assert_eq!(evidence["schema_version"], 1);
        assert_eq!(evidence["measurement_window"]["start_unix_ms"], start);
        assert_eq!(evidence["measurement_window"]["end_unix_ms"], end);
        assert_eq!(evidence["sampling_interval_ms"], 1_000);
        assert_eq!(evidence["request_timeout_ms"], 3_000);
        assert_eq!(evidence["scope"], "unique_table_owner_admissions_not_confirmed_durable_commit");
        assert_eq!(evidence["samples"].as_array().context("backlog samples")?.len(), 150);
        let mut bucket_counts = [0_u64; 5];
        for sample in evidence["samples"].as_array().context("backlog samples")? {
            let sample_start = sample["request_start_unix_ms"].as_u64().context("sample start")?;
            let sample_end = sample["request_end_unix_ms"].as_u64().context("sample end")?;
            assert!(sample_start >= start && sample_end <= end && sample_end >= sample_start);
            assert_eq!(sample["request_timeout_ms"], 3_000.min(end - sample_start));
            assert_eq!(sample["available"], true);
            let bucket_index = ((sample_start - start) / 30_000).min(4);
            let bucket_start = start + bucket_index * 30_000;
            let bucket_end = bucket_start + 30_000;
            assert!(sample_start >= bucket_start && sample_end <= bucket_end);
            bucket_counts[bucket_index as usize] += 1;
        }
        assert_eq!(bucket_counts, [30, 30, 30, 30, 30]);
        assert!(!valid_backlog_request_interval(start + 2, start + 1, start, end));
        assert_eq!(evidence["sampling_failures"], 0);
        assert_eq!(evidence["samples_dropped"], 0);
        if let Some(path) = std::env::var_os("OAT_BACKLOG_FIXTURE_OUT") {
            std::fs::write(path, serde_json::to_vec_pretty(&evidence)?)?;
        }
        Ok(())
    }

    #[test]
    fn downstream_degradation_is_local_only() {
        assert!(local_downstream_optional(crate::config::OatMode::Local));
        assert!(!local_downstream_optional(crate::config::OatMode::Aks));
        let mut evidence = CellEvidence::default();
        evidence.downstream_unavailable("local_qap_initialization_unavailable");
        let public = evidence.for_table("table", None);
        assert_eq!(public["downstream_status"], "unavailable");
        assert!(public["rows"]["delta_committed"].is_null());
        assert_eq!(public["reconciliation"]["status"], "unavailable");
        assert_eq!(public["reconciliation"]["reason"], "cumulative_delta_rows_unavailable");
        assert!(public["compression_ratio"].is_null());
    }

    #[test]
    fn window_payload_and_sampled_core_rates_preserve_evidence_boundaries() -> Result<()> {
        let mut evidence = CellEvidence {
            measurement_window: Some((1000, 3000)),
            ..Default::default()
        };
        evidence.client_reports.insert("table".to_owned(), vec![json!({
            "phase_counters": {"measurement_window": {
                "start_unix_ms": 1000, "end_unix_ms": 3000, "completed_rows": 10, "completed_bytes": 200
            }}
        })]);
        evidence.durable_commits.insert("table".to_owned(), json!({
            "output_throughput": {"rows_per_second": 8.0, "input_bytes_per_second": 100.0, "parquet_bytes_per_second": 50.0}
        }));
        evidence.resource_samples.push(ResourceSample {
            sampled_at_unix_ms: 2000,
            metrics: ResourceMetrics { cpu_millicores: 2000.0, memory_bytes: 100 },
        });
        let client = ClientMetrics { rows: 10, duration_ms: 2000, rows_per_second: 5.0, completions: Vec::new() };
        let result = evidence.for_table("table", Some(&client));
        assert_eq!(result["rate_evidence"]["completed_flight_payload_bytes_per_second"], 100.0);
        assert_eq!(result["rate_evidence"]["committed_rows_per_second_per_observed_collector_core"], 4.0);
        assert_eq!(result["rate_evidence"]["collector_input_bytes_per_second_per_observed_collector_core"], 50.0);
        assert_eq!(result["rate_evidence"]["committed_parquet_bytes_per_second_per_observed_collector_core"], 25.0);
        assert_eq!(result["rate_evidence"]["completed_flight_payload_bytes_per_second_per_observed_collector_core"], 50.0);
        evidence
            .client_reports
            .get_mut("table")
            .context("measurement client report missing")?[0]["phase_counters"]["measurement_window"]["end_unix_ms"] = json!(4000);
        assert!(evidence.for_table("table", Some(&client))["rate_evidence"]["completed_flight_payload_bytes_per_second"].is_null());
        assert!(evidence.for_table("table", Some(&client))["rate_evidence"]["completed_flight_payload_bytes_per_second_per_observed_collector_core"].is_null());
        evidence.resource_samples.clear();
        assert!(evidence.for_table("table", Some(&client))["rate_evidence"]["committed_rows_per_second_per_observed_collector_core"].is_null());
        assert!(evidence.for_table("table", Some(&client))["rate_evidence"]["collector_input_bytes_per_second_per_observed_collector_core"].is_null());
        Ok(())
    }


    #[test]
    fn client_snapshots_validate_prepared_generation_by_data_profile() -> Result<()> {
        let worker = json!({
            "worker": 0, "attempted_rows": 5, "attempted_bytes": 25,
            "completed_rows": 5, "completed_bytes": 25, "error_rows": 0,
            "error_bytes": 0, "cancelled_rows": 0, "cancelled_bytes": 0,
            "best_effort_streams": 0, "durable_streams": 5, "send_latency_us": null,
        });
        let start = json!({
            "version": 1, "phase": "measurement_start", "scope": "process_cumulative",
            "elapsed_seconds": 0.0, "workers": [worker.clone()],
        });
        let end = json!({
            "version": 1, "phase": "completed", "scope": "process_cumulative",
            "elapsed_seconds": 1.0, "workers": [worker],
            "measurement_window": {
                "start_unix_ms": 10, "end_unix_ms": 20, "completed_rows": 5,
                "completed_bytes": 25, "worker_completed_rows": [5], "pilot_window_rows": [],
                "windows": [{"completed_sends": 2, "timed_out_sends": 1, "ambiguous_sends": 1}],
                "scope": "per_stream_admission_completion_observed_in_half_open_shared_unix_window",
            },
        });
        let mut snapshots = [start, end]
            .into_iter()
            .map(serde_json::from_value::<ClientCounterSnapshot>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let output = client_snapshot_delta(&snapshots, ClientDataProfile::Diagnostic)?;
        assert_eq!(output["workers"][0]["rejected_rows"], 0);
        assert_eq!(output["workers"][0]["retried_streams"], 0);
        assert_eq!(output["measurement_window"]["ambiguous_rows"], 0);
        assert_eq!(output["measurement_window"]["windows"][0]["completed_sends"], 2);
        assert_eq!(output["measurement_window"]["windows"][0]["timed_out_sends"], 1);
        assert_eq!(output["measurement_window"]["windows"][0]["ambiguous_sends"], 1);
        assert_eq!(output["process_cumulative"]["ambiguous_rows"], 0);
        assert!(output["delivery_cumulative"]["retried_streams"].is_null());
        snapshots[1].workers[0].retried_streams = Some(2);
        snapshots[1].workers[0].retried_rows = Some(10);
        snapshots[1].workers[0].ambiguous_streams = Some(0);
        snapshots[1].workers[0].ambiguous_rows = Some(0);
        let output = client_snapshot_delta(&snapshots, ClientDataProfile::Diagnostic)?;
        assert_eq!(output["delivery_cumulative"]["retried_streams"], 2);
        assert_eq!(output["delivery_cumulative"]["retried_rows"], 10);
        assert_eq!(output["delivery_cumulative"]["ambiguous_rows"], 0);
        let hash = "a".repeat(64);
        let prepared = json!({
            "data_profile": "realistic",
            "payload_corpus_sha256": hash,
            "payload_corpus_hash_version": 2,
            "payload_corpus_hash_scope": "arrow-logical-v2-excluding-endpoint_url",
            "corpus_rows": 65_536,
            "corpus_uncompressed_flight_data_bytes": 535_000,
            "corpus_configured_ipc_compression_flight_data_bytes": 500_000,
            "corpus_uncompressed_ipc_bytes": 535_000,
            "corpus_encoded_bytes": 500_000,
            "retained_encoded_bytes": 500_000,
            "memory_reservation_bytes": 1_000_000,
            "corpus_memory_reservation_bytes": 700_000,
            "live_identity_batch_bytes": 150_000,
            "live_identity_batch_reservation_bytes": 300_000,
            "configured_live_slots": 2,
            "admitted_live_slots": 2,
            "max_memory_bytes": 2_000_000,
            "generation_wall_seconds": 0.1,
            "generation_cpu_seconds": 0.05,
            "encoding_cpu_seconds": 0.04,
            "preparation_cpu_seconds": 0.09,
            "generation_cpu_scope": "caller_thread_linux_schedstat_null_on_other_platforms",
            "encoding_wall_seconds": 0.1,
            "preparation_wall_seconds": 0.3,
            "generator_rows_per_second": 655_360.0,
            "steady_state_generation": true,
        });
        let realistic_manifest = json!({
            "data_profile": "realistic",
            "payload_corpus_sha256": hash,
            "payload_corpus_hash_version": 2,
            "payload_corpus_hash_scope": "arrow-logical-v2-excluding-endpoint_url",
            "identity_status": "identity_bearing",
            "eligible_for_identity_reconciliation": true,
            "complete": true,
            "truncated": false,
            "run_uuid": "c784d07ceefe4191aaf92a269f272ecf",
            "incarnation_uuid": "1ae94edfe4044f0894fa78d4467f568a",
            "ranges": [{"table": "example", "worker": 0, "first_sequence": 1, "last_sequence": 2,
                "batch_count": 2, "rows_per_batch": 5, "cohort": "measurement",
                "attempts": 1, "outcome": "acknowledged"}],
        });
        snapshots[1].preparation = Some(prepared.clone());
        snapshots[1].logical_batch_manifest = Some(realistic_manifest.clone());
        let realistic = client_snapshot_delta(&snapshots, ClientDataProfile::Realistic)?;
        assert_eq!(realistic["preparation"]["steady_state_generation"], true);
        assert_eq!(realistic["preparation"]["corpus_rows"], 65_536);
        for key in [
            "configured_live_slots",
            "admitted_live_slots",
            "live_identity_batch_bytes",
            "live_identity_batch_reservation_bytes",
            "corpus_memory_reservation_bytes",
            "memory_reservation_bytes",
            "max_memory_bytes",
        ] {
            assert_eq!(realistic["preparation"][key], prepared[key], "{key}");
            let mut invalid = prepared.clone();
            invalid[key] = json!("not an unsigned number");
            assert!(client_preparation(Some(&invalid), ClientDataProfile::Realistic).is_err());
        }
        assert!(client_snapshot_delta(&snapshots, ClientDataProfile::Diagnostic).is_err());
        snapshots[1].measurement_window.as_mut().context("test measurement window")?.client_resources =
            Some(json!({"cpu_core_equivalents": 2.5}));
        let evidence = client_snapshot_delta(&snapshots, ClientDataProfile::Realistic)?;
        assert_eq!(evidence["payload_manifest"], realistic_manifest);
        assert_eq!(evidence["measurement_window"]["client_resources"]["cpu_core_equivalents"], 2.5);
        snapshots[1].logical_batch_manifest.as_mut().context("test manifest")?["payload_corpus_sha256"] = json!("b".repeat(64));
        assert!(client_snapshot_delta(&snapshots, ClientDataProfile::Realistic).is_err());
        snapshots[1].preparation = Some(prepared.clone());
        snapshots[1].logical_batch_manifest = Some(realistic_manifest.clone());

        let diagnostic_preparation = json!({
            "data_profile": "diagnostic",
            "payload_corpus_sha256": hash,
            "payload_corpus_hash_version": 2,
            "payload_corpus_hash_scope": "arrow-logical-v2-all-columns",
            "steady_state_generation": false,
        });
        let diagnostic_manifest = json!({
            "data_profile": "diagnostic",
            "payload_corpus_sha256": hash,
            "payload_corpus_hash_version": 2,
            "payload_corpus_hash_scope": "arrow-logical-v2-all-columns",
            "identity_status": "diagnostic_only",
            "eligible_for_identity_reconciliation": false,
            "complete": false,
            "truncated": false,
        });
        snapshots[1].preparation = Some(diagnostic_preparation.clone());
        snapshots[1].logical_batch_manifest = Some(diagnostic_manifest);
        let diagnostic = client_snapshot_delta(&snapshots, ClientDataProfile::Diagnostic)?;
        assert_eq!(diagnostic["payload_manifest"]["identity_status"], "diagnostic_only");
        assert_eq!(diagnostic["payload_manifest"]["complete"], false);
        assert_eq!(diagnostic["payload_manifest"]["eligible_for_identity_reconciliation"], false);

        for (flag, valid_diagnostic) in [(json!(false), true), (json!("true"), false)] {
            let mut diagnostic_preparation = diagnostic_preparation.clone();
            diagnostic_preparation["steady_state_generation"] = flag;
            snapshots[1].preparation = Some(diagnostic_preparation);
            assert!(client_snapshot_delta(&snapshots, ClientDataProfile::Realistic).is_err());
            assert_eq!(client_snapshot_delta(&snapshots, ClientDataProfile::Diagnostic).is_ok(), valid_diagnostic);
        }
        snapshots[1].logical_batch_manifest = Some(realistic_manifest.clone());
        snapshots[1].preparation = Some(json!({"corpus_rows": 65_536}));
        assert!(client_snapshot_delta(&snapshots, ClientDataProfile::Realistic).is_err());
        snapshots[1].preparation = None;
        assert!(client_snapshot_delta(&snapshots, ClientDataProfile::Realistic).is_err());
        assert!(client_snapshot_delta(&snapshots, ClientDataProfile::Diagnostic)?["preparation"].is_null());
        let mut accepted = Vec::new();
        snapshots[1].preparation = Some(json!({"steady_state_generation": true}));
        if client_snapshot_delta(&snapshots, ClientDataProfile::Realistic).is_ok() {
            accepted.push("flag only".to_owned());
        }
        for key in ["corpus_rows", "corpus_encoded_bytes", "retained_encoded_bytes", "memory_reservation_bytes"] {
            let mut missing = prepared.clone();
            missing.as_object_mut().context("test preparation")?.remove(key);
            let mut zero = prepared.clone();
            zero[key] = json!(0);
            for (case, preparation) in [("missing", missing), ("zero", zero)] {
                snapshots[1].preparation = Some(preparation);
                if client_snapshot_delta(&snapshots, ClientDataProfile::Realistic).is_ok() {
                    accepted.push(format!("{case} {key}"));
                }
            }
        }
        if client_phase_evidence("", ClientDataProfile::Realistic).is_ok() { accepted.push("no phase counters".to_owned()); }
        assert!(accepted.is_empty(), "accepted invalid Realistic preparation: {accepted:?}");
        Ok(())
    }

    #[test]
    fn reconciliation_uses_cumulative_rows_and_ambiguity_bound() {
        let mut evidence = CellEvidence::default();
        evidence.client_reports.insert("table".to_owned(), vec![json!({
            "phase_counters": {"process_cumulative": {"completed_rows": 10, "ambiguous_rows": 2}}
        })]);
        evidence.durable_commits.insert("table".to_owned(), json!({
            "run_cumulative_output": {"rows": 12, "settled": true, "settle_seconds": 1.0, "settle_timeout_seconds": 120}
        }));
        let reconciliation = evidence.for_table("table", None)["reconciliation"].clone();
        assert_eq!(reconciliation["status"], "within_bound");
        assert_eq!(reconciliation["minimum_delta_rows"], 10);
        assert_eq!(reconciliation["maximum_delta_rows"], 12);
        assert_eq!(reconciliation["exact_match_required"], false);

        evidence.client_reports.insert("table".to_owned(), vec![json!({
            "phase_counters": {"process_cumulative": {"completed_rows": 10, "ambiguous_rows": 0}}
        })]);
        evidence.durable_commits.insert("table".to_owned(), json!({
            "run_cumulative_output": {"rows": 10, "settled": true, "settle_seconds": 1.0, "settle_timeout_seconds": 120}
        }));
        let reconciliation = evidence.for_table("table", None)["reconciliation"].clone();
        assert_eq!(reconciliation["status"], "within_bound");
        assert_eq!(reconciliation["exact_match_required"], true);
        assert_eq!(reconciliation["exact_match"], true);

        evidence.client_reports.insert("table".to_owned(), vec![json!({
            "phase_counters": {"process_cumulative": {"completed_rows": 10, "ambiguous_rows": 2}}
        })]);
        evidence.durable_commits.insert("table".to_owned(), json!({
            "run_cumulative_output": {"rows": 8, "settled": false, "settle_seconds": 120.0, "settle_timeout_seconds": 120}
        }));
        let reconciliation = evidence.for_table("table", None)["reconciliation"].clone();
        assert_eq!(reconciliation["status"], "unsettled");
        assert_eq!(reconciliation["gap_rows"], 2);
        assert_eq!(reconciliation["settle_seconds"], 120.0);

        evidence.durable_commits.insert("table".to_owned(), json!({
            "run_cumulative_output": {"rows": 8, "settled": true, "settle_seconds": 120.0, "settle_timeout_seconds": 120}
        }));
        let reconciliation = evidence.for_table("table", None)["reconciliation"].clone();
        assert_eq!(reconciliation["status"], "outside_bound");
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct WorkerCounterSnapshot {
    worker: u64,
    attempted_rows: u64,
    attempted_bytes: u64,
    completed_rows: u64,
    completed_bytes: u64,
    error_rows: u64,
    error_bytes: u64,
    cancelled_rows: u64,
    cancelled_bytes: u64,
    best_effort_streams: u64,
    durable_streams: u64,
    #[serde(default)]
    rejected_streams: u64,
    #[serde(default)]
    rejected_rows: u64,
    #[serde(default)]
    retried_streams: Option<u64>,
    #[serde(default)]
    retried_rows: Option<u64>,
    #[serde(default)]
    given_up_streams: u64,
    #[serde(default)]
    given_up_rows: u64,
    #[serde(default)]
    ambiguous_streams: Option<u64>,
    #[serde(default)]
    ambiguous_rows: Option<u64>,
    send_latency_us: Option<ClientLatency>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct ClientLatency {
    p50: Option<u64>,
    p95: Option<u64>,
    p99: Option<u64>,
    samples: u64,
    precision: String,
}

fn cancelled_client_window(output: &str, expected: Option<(u64, u64)>) -> Option<(String, Value)> {
    let (start, end) = expected?;
    if start >= end { return None; }
    let mut terminal = None;
    for line in output.lines().filter_map(|line| line.strip_prefix("OAT_CLIENT_METRICS ")) {
        let snapshot: Value = serde_json::from_str(line).ok()?;
        if matches!(snapshot["phase"].as_str(), Some("failed" | "completed")) {
            if terminal.is_some() { return None; }
            terminal = Some(snapshot);
        }
    }
    let snapshot = terminal?;
    if snapshot["version"].as_u64() != Some(1) || snapshot["scope"].as_str() != Some("process_cumulative") { return None; }
    let window = snapshot.get("measurement_window")?;
    if window["start_unix_ms"].as_u64() != Some(start)
        || window["end_unix_ms"].as_u64() != Some(end)
        || window["scope"].as_str() != Some("per_stream_admission_completion_observed_in_half_open_shared_unix_window") { return None; }
    let windows = window["windows"].as_array()?;
    if u64::try_from(windows.len()).ok()? != (end - start).div_ceil(30_000) { return None; }
    for (index, item) in windows.iter().enumerate() {
        let bucket_start = start.checked_add(u64::try_from(index).ok()?.checked_mul(30_000)?)?;
        if item["start_unix_ms"].as_u64() != Some(bucket_start)
            || item["end_unix_ms"].as_u64() != Some(end.min(bucket_start.saturating_add(30_000)))
            || item["send_outcome_scope"].as_str() != Some("attempts_started_in_window_including_post_window_outcomes_timeout_is_ambiguous")
            || ["completed_sends", "timed_out_sends", "ambiguous_sends"].iter().any(|key| item[*key].as_u64().is_none())
            || item["timed_out_sends"].as_u64()? > item["ambiguous_sends"].as_u64()? { return None; }
    }
    Some((snapshot["phase"].as_str()?.to_owned(), window.clone()))
}

#[derive(serde::Deserialize)]
struct ClientCounterSnapshot {
    version: u64,
    phase: String,
    scope: String,
    elapsed_seconds: f64,
    workers: Vec<WorkerCounterSnapshot>,
    preparation: Option<Value>,
    measurement_window: Option<ClientMeasurementWindow>,
    logical_batch_manifest: Option<Value>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct ClientMeasurementWindow {
    start_unix_ms: u64,
    end_unix_ms: u64,
    completed_rows: u64,
    completed_bytes: u64,
    #[serde(default)]
    rejected_streams: u64,
    #[serde(default)]
    rejected_rows: u64,
    #[serde(default)]
    retried_streams: u64,
    #[serde(default)]
    retried_rows: u64,
    #[serde(default)]
    given_up_streams: u64,
    #[serde(default)]
    given_up_rows: u64,
    #[serde(default)]
    ambiguous_streams: u64,
    #[serde(default)]
    ambiguous_rows: u64,
    worker_completed_rows: Vec<u64>,
    pilot_window_rows: Vec<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    windows: Option<Vec<Value>>,
    scope: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_resources: Option<Value>,
}

fn client_profile_name(profile: ClientDataProfile) -> &'static str {
    match profile {
        ClientDataProfile::Diagnostic => "diagnostic",
        ClientDataProfile::Realistic => "realistic",
    }
}

fn client_payload_hash_scope(profile: ClientDataProfile) -> &'static str {
    match profile {
        ClientDataProfile::Diagnostic => "arrow-logical-v2-all-columns",
        ClientDataProfile::Realistic => "arrow-logical-v2-excluding-endpoint_url",
    }
}

fn client_preparation(value: Option<&Value>, data_profile: ClientDataProfile) -> Result<Value> {
    let Some(value) = value else {
        if data_profile == ClientDataProfile::Realistic {
            bail!("missing preparation evidence for realistic client");
        }
        return Ok(Value::Null);
    };
    let object = value.as_object().context("client preparation must be an object")?;
    let mut result = serde_json::Map::new();
    if let Some(hash) = object.get("payload_corpus_sha256") {
        if hash.as_str().is_none_or(|value| {
            value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }) {
            bail!("invalid client payload corpus hash");
        }
        if object.get("data_profile").and_then(Value::as_str) != Some(client_profile_name(data_profile))
            || object.get("payload_corpus_hash_version").and_then(Value::as_u64) != Some(2)
            || object.get("payload_corpus_hash_scope").and_then(Value::as_str) != Some(client_payload_hash_scope(data_profile))
        {
            bail!("client preparation profile or payload hash contract mismatch");
        }
        result.insert("payload_corpus_sha256".to_owned(), hash.clone());
        for key in ["data_profile", "payload_corpus_hash_version", "payload_corpus_hash_scope"] {
            result.insert(key.to_owned(), object[key].clone());
        }
    } else if data_profile == ClientDataProfile::Realistic {
        bail!("missing realistic client payload corpus hash");
    }
    for key in [
        "corpus_rows",
        "corpus_decoded_bytes",
        "corpus_encoded_bytes",
        "corpus_memory_reservation_bytes",
        "retained_encoded_bytes",
        "memory_reservation_bytes",
        "configured_live_slots",
        "admitted_live_slots",
        "live_identity_batch_bytes",
        "live_identity_batch_reservation_bytes",
        "max_memory_bytes",
    ] {
        if let Some(value) = object.get(key) {
            if value.as_u64().is_none() {
                bail!("client preparation byte/row evidence must be unsigned");
            }
            result.insert(key.to_owned(), value.clone());
        }
    }
    if let Some(applicability) = object.get("generator_applicability") {
        let tables = applicability.get("tables").and_then(Value::as_array)
            .context("client generator_applicability tables must be an array")?;
        if applicability["version"] != 1 || tables.is_empty() {
            bail!("invalid client generator_applicability version or tables");
        }
        let mut names = HashSet::new();
        let mut total_rows = 0_u64;
        for table in tables {
            let name = table["table"].as_str().filter(|name| !name.is_empty())
                .context("client generator_applicability table is missing")?;
            if !names.insert(name.to_ascii_lowercase()) {
                bail!("duplicate client generator_applicability table");
            }
            let rows = table["rows"].as_u64().filter(|rows| *rows > 0)
                .context("client generator_applicability rows must be positive")?;
            let fields = table["schema_fields"].as_u64().filter(|fields| *fields > 0)
                .context("client generator_applicability schema_fields must be positive")?;
            let nulls = &table["null_fraction"];
            let columns = nulls["eligible_columns"].as_u64()
                .context("client generator_applicability eligible_columns must be unsigned")?;
            let eligible_rows = nulls["eligible_rows"].as_u64()
                .context("client generator_applicability eligible_rows must be unsigned")?;
            let null_rows = nulls["null_rows"].as_u64()
                .context("client generator_applicability null_rows must be unsigned")?;
            if columns > fields || rows.checked_mul(columns) != Some(eligible_rows) || null_rows > eligible_rows {
                bail!("inconsistent client generator_applicability null counts");
            }
            for name in ["pod_ordinal", "guid_uniqueness", "value_variation", "string_width", "entropy"] {
              if let Some(guid) = table.get(name) {
                let columns = guid["eligible_columns"].as_u64()
                    .with_context(|| format!("client generator_applicability {name} eligible_columns must be unsigned"))?;
                let eligible_rows = guid["eligible_rows"].as_u64()
                    .with_context(|| format!("client generator_applicability {name} eligible_rows must be unsigned"))?;
                if columns > fields || rows.checked_mul(columns).is_none_or(|maximum| eligible_rows > maximum)
                    || (columns == 0) != (eligible_rows == 0)
                {
                    bail!("inconsistent client generator_applicability {name} counts");
                }
              }
            }
            total_rows = total_rows.checked_add(rows).context("client generator_applicability row overflow")?;
        }
        if object.get("corpus_rows").and_then(Value::as_u64) != Some(total_rows) {
            bail!("client generator_applicability does not match prepared corpus rows");
        }
        result.insert("generator_applicability".to_owned(), applicability.clone());
    }

    for key in [
        "generation_wall_seconds",
        "generation_cpu_seconds",
        "encoding_wall_seconds",
        "encoding_cpu_seconds",
        "preparation_wall_seconds",
        "preparation_cpu_seconds",
        "generator_rows_per_second",
    ] {
        if let Some(value) = object.get(key) {
            if !value.is_null() && value.as_f64().is_none_or(|value| !value.is_finite() || value < 0.0) {
                bail!("client preparation timing/rate evidence must be finite and nonnegative");
            }
            result.insert(key.to_owned(), value.clone());
        }
    }
    match (data_profile, object.get("steady_state_generation")) {
        (ClientDataProfile::Diagnostic, None) => {}
        (ClientDataProfile::Diagnostic, Some(value)) if value.as_bool() == Some(false) => {
            result.insert("steady_state_generation".to_owned(), value.clone());
        }
        (ClientDataProfile::Realistic, Some(value)) if value.as_bool() == Some(true) => {
            for key in ["corpus_rows", "corpus_encoded_bytes", "retained_encoded_bytes", "memory_reservation_bytes"] {
                if result.get(key).and_then(Value::as_u64).is_none_or(|value| value == 0) {
                    bail!("realistic client must report positive prepared-corpus {key}");
                }
            }
            result.insert("steady_state_generation".to_owned(), value.clone());
        }
        (ClientDataProfile::Diagnostic, _) => bail!("unexpected hot-path generation in prepared client"),
        (ClientDataProfile::Realistic, _) => bail!("realistic client must report steady-state identity generation"),
    }
    Ok(Value::Object(result))
}

fn client_phase_evidence(output: &str, data_profile: ClientDataProfile) -> Result<Value> {
    let snapshots = output
        .lines()
        .filter_map(|line| line.strip_prefix("OAT_CLIENT_METRICS "))
        .map(serde_json::from_str::<ClientCounterSnapshot>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("invalid client counter snapshot")?;
    if snapshots.is_empty() {
        if data_profile == ClientDataProfile::Realistic {
            bail!("realistic client did not report prepared phase counters");
        }
        return Ok(json!({ "unavailable_reason": "client_did_not_report_phase_counters" }));
    }
    client_snapshot_delta(&snapshots, data_profile)
}

#[cfg(test)]
mod downstream_runner_tests {
    use super::*;

    #[test]
    fn cancelled_client_retains_window_outcomes_only_as_diagnostic_evidence() {
        let bucket = |start, end, completed, timed_out, ambiguous| json!({
            "start_unix_ms": start, "end_unix_ms": end,
            "completed_sends": completed, "timed_out_sends": timed_out, "ambiguous_sends": ambiguous,
            "send_outcome_scope": "attempts_started_in_window_including_post_window_outcomes_timeout_is_ambiguous"
        });
        let terminal = json!({"version": 1, "phase": "failed", "scope": "process_cumulative",
            "measurement_window": {"start_unix_ms": 1000, "end_unix_ms": 61000,
                "scope": "per_stream_admission_completion_observed_in_half_open_shared_unix_window",
                "windows": [bucket(1000, 31000, 2, 1, 1), bucket(31000, 61000, 3, 2, 4)]}});
        let output = format!("OAT_CLIENT_METRICS {terminal}\n");
        let mut evidence = CellEvidence { measurement_window: Some((1000, 61000)), ..Default::default() };
        evidence.record_cancelled_client("table", "client-1", Some(&output));
        let report = &evidence.client_reports["table"][0];
        assert_eq!(report["status"], "diagnostic_only");
        assert_eq!(report["phase_counters"]["terminal_phase"], "failed");
        assert_eq!(report["phase_counters"]["measurement_window"]["windows"][0]["completed_sends"], 2);
        assert_eq!(report["phase_counters"]["measurement_window"]["windows"][1]["timed_out_sends"], 2);
        assert_eq!(report["phase_counters"]["measurement_window"]["windows"][1]["ambiguous_sends"], 4);
        assert_eq!(evidence.client_snapshot_total("table", "client_completed_rows"), None);
        assert_eq!(evidence.client_cumulative_total("table", "completed_rows"), None);
        evidence.record_cancelled_client("table", "client-2", None);
        assert_eq!(evidence.client_reports["table"][1]["phase_counters"]["unavailable_reason"], "terminal_client_window_unavailable");
        assert!(cancelled_client_window(&output, Some((1000, 61001))).is_none());
        assert!(cancelled_client_window(&format!("{output}{output}"), Some((1000, 61000))).is_none());
        let mut malformed = terminal;
        malformed["measurement_window"]["windows"][0]["ambiguous_sends"] = json!(0);
        assert!(cancelled_client_window(&format!("OAT_CLIENT_METRICS {malformed}\n"), Some((1000, 61000))).is_none());
    }

    #[derive(Clone, Copy, Debug)]
    enum Fault {
        InitUnavailable,
        InitRequired,
        InitTimeout,
        PinInitFailed,
        GuardUnavailable,
        ObservationUnavailable,
        InitBindingRejected,
        ObservationBindingRejected,
        Collector,
        Pinning,
        ScheduleArmBlocked,
        ScheduleAckBlocked,
        ScheduleAckCancellationFailed,
        CancellationCapture,
    }

    struct FaultPlatform {
        kind: crate::config::OatMode,
        fault: Fault,
        endpoint: String,
        start: std::sync::atomic::AtomicU64,
        clients_started: usize,
        pin_init_calls: usize,
        qap_initialization_calls: usize,
        client_fleet_prepared: bool,
        client_schedule_wait_seconds: Option<u64>,
        arm_attempts: usize,
        arm_started: Option<tokio::time::Instant>,
        delivery_drop_ms: std::sync::atomic::AtomicU64,
        cancellations: std::sync::Mutex<Vec<String>>,
        unresolved: bool,
    }

    struct PendingDelivery<'a> {
        started: tokio::time::Instant,
        dropped_after_ms: &'a std::sync::atomic::AtomicU64,
    }

    impl Drop for PendingDelivery<'_> {
        fn drop(&mut self) {
            self.dropped_after_ms.store(
                u64::try_from(self.started.elapsed().as_millis())
                    .unwrap_or_else(|error| panic!("bounded fixture duration overflow: {error}")),
                std::sync::atomic::Ordering::SeqCst,
            );
            tokio::time::resume();
        }
    }

    #[async_trait::async_trait]
    impl Platform for FaultPlatform {
        fn kind(&self) -> crate::config::OatMode {
            self.kind
        }
        fn workload_endpoint(&self, _: &OatConfig) -> String {
            self.endpoint.clone()
        }
        fn router_metrics_endpoint(&self, _: &OatConfig) -> Result<String> {
            Ok(format!("{}/metrics", self.endpoint))
        }
        fn qap_endpoint(&self, _: &OatConfig) -> String {
            self.endpoint.clone()
        }
        fn resource_sampler_source(&self) -> Option<ResourceSamplerSource> {
            matches!(self.fault, Fault::GuardUnavailable).then(|| ResourceSamplerSource::Local {
                run_id: "stack-fixture".to_owned(),
                environment: HashMap::new(),
                collector_metrics_url: Some(format!("{}/metrics", self.endpoint)),
            })
        }
        async fn ensure_stack(&mut self, _: &OatConfig, _: &str, _: u32) -> Result<()> {
            Ok(())
        }
        async fn scale_arrowcols(&mut self, _: &OatConfig, _: u32, _: u32) -> Result<()> {
            Ok(())
        }
        async fn initialize_run_table_pins(&mut self, _: &OatConfig, _: &str, _: &str, _: u64, _: u64) -> Result<()> {
            self.pin_init_calls += 1;
            if matches!(self.fault, Fault::PinInitFailed) {
                bail!("fixture run-table initialization failed");
            }
            Ok(())
        }
        fn qap_observation_mode(&self) -> arrowcol_loadgen::ObservationMode {
            arrowcol_loadgen::ObservationMode::Local
        }
        fn qap_database(&self, _: &OatConfig) -> Result<String> {
            Ok("loadtest".to_owned())
        }
        async fn initialize_qap(&mut self, _: &OatConfig, _: &str, _: &[String]) -> Result<Vec<String>> {
            self.qap_initialization_calls += 1;
            match self.fault {
                Fault::InitRequired => bail!("required_binding_setup_failed"),
                Fault::InitTimeout => std::future::pending().await,
                Fault::InitUnavailable | Fault::InitBindingRejected => {
                    let results = std::env::temp_dir().join(format!("oat-qap-init-{}", uuid::Uuid::new_v4()));
                    fs::create_dir(&results)?;
                    let outcome = arrowcol_loadgen::initialize_local_qap(
                        &self.endpoint,
                        &results,
                        "loadtest",
                        &[(
                            "observed".to_owned(),
                            "/delta/dataflow/telemetry/sqlserver/sqlserver_active_sessions".to_owned(),
                        )],
                        Duration::from_secs(5),
                    )
                    .await;
                    fs::remove_dir_all(&results)?;
                    outcome.map_err(classify_local_qap_error)?;
                    Ok(vec!["observed".to_owned()])
                }
                Fault::ObservationUnavailable | Fault::ObservationBindingRejected => Ok(vec!["observed".to_owned()]),
                _ => Err(anyhow::anyhow!(LocalDownstreamUnavailable)),
            }
        }
        fn mark_qap_outcome_unresolved(&mut self) {
            self.unresolved = true;
        }
        async fn prepare_client_fleet(&mut self, _: &OatConfig, _: &str) -> Result<()> {
            self.client_fleet_prepared = true;
            Ok(())
        }
        async fn start_client(
            &mut self,
            _: &OatConfig,
            _: &str,
            table: &str,
            _: usize,
            arguments: &[String],
        ) -> Result<ClientHandle> {
            self.clients_started += 1;
            self.client_schedule_wait_seconds = arguments.windows(2)
                .find(|pair| pair[0] == "--schedule-timeout-secs")
                .map(|pair| pair[1].parse())
                .transpose()?;
            Ok(ClientHandle {
                client_id: format!("client-{}", self.clients_started),
                table: table.to_owned(),
                process: ClientProcess::Local {
                    container_id: "fake".to_owned(),
                },
            })
        }
        async fn poll_client(&self, _: &OatConfig, handle: &ClientHandle) -> Result<ClientProgress> {
            let armed = self.start.load(std::sync::atomic::Ordering::SeqCst) != 0;
            if armed && handle.client_id == "client-3"
                && matches!(self.fault, Fault::ScheduleAckBlocked | Fault::ScheduleAckCancellationFailed)
            {
                let _pending = PendingDelivery {
                    started: self.arm_started.context("fixture arm was not started")?,
                    dropped_after_ms: &self.delivery_drop_ms,
                };
                return std::future::pending().await;
            }
            Ok(ClientProgress {
                ready: true,
                schedule_received: armed,
                completed: armed,
                failure: matches!(self.fault, Fault::Collector).then(|| "collector_failed".to_owned()),
                output: String::new(),
            })
        }
        async fn arm_clients(&mut self, config: &OatConfig, _: &str, start: u64) -> Result<()> {
            self.arm_attempts += 1;
            self.arm_started = Some(tokio::time::Instant::now());
            self.start.store(start, std::sync::atomic::Ordering::SeqCst);
            match self.fault {
                Fault::ScheduleArmBlocked => {
                    tokio::time::pause();
                    let _pending = PendingDelivery {
                        started: self.arm_started.context("fixture arm was not started")?,
                        dropped_after_ms: &self.delivery_drop_ms,
                    };
                    return std::future::pending().await;
                }
                Fault::ScheduleAckBlocked | Fault::ScheduleAckCancellationFailed => {
                    tokio::time::pause();
                    // Leave only one quarter of the original lead for the blocked final ACK.
                    tokio::time::advance(Duration::from_millis(config.schedule_lead_seconds * 750)).await;
                }
                _ => {}
            }
            Ok(())
        }
        async fn finish_client(&self, config: &OatConfig, _: ClientHandle) -> Result<String> {
            let start = self.start.load(std::sync::atomic::Ordering::SeqCst) + config.warmup_seconds * 1000;
            let end = start + config.measure_seconds * 1000;
            Ok(format!(
                "OAT_CLIENT_BUILD_PROFILE {{\"lto\":\"off\",\"codegen_units\":16,\"opt_level\":3,\"target_cpu\":\"generic\"}}\nmeasured_rows=100 measured_seconds={} rows_per_second={} measurement_window_start_unix_ms={start} measurement_window_end_unix_ms={end} measurement_started_unix_ms={start}\n",
                config.measure_seconds,
                100.0 / config.measure_seconds as f64
            ))
        }
        async fn cancel_client(&self, _: &OatConfig, handle: &ClientHandle) -> Result<()> {
            self.cancellations.lock()
                .map_err(|error| anyhow::anyhow!("fixture cancellations poisoned: {error}"))?
                .push(handle.client_id.clone());
            if matches!(self.fault, Fault::ScheduleAckCancellationFailed) && handle.client_id == "client-1" {
                bail!("fixture_cancel_failed");
            }
            Ok(())
        }
        async fn cancel_client_with_output(&self, config: &OatConfig, handle: &ClientHandle) -> Result<Option<String>> {
            self.cancel_client(config, handle).await?;
            if !matches!(self.fault, Fault::CancellationCapture) { return Ok(None); }
            if handle.client_id == "client-2" { bail!("fixture terminal logs unavailable"); }
            let terminal = json!({"version":1,"phase":"failed","scope":"process_cumulative",
                "measurement_window":{"start_unix_ms":1000,"end_unix_ms":31000,
                    "scope":"per_stream_admission_completion_observed_in_half_open_shared_unix_window",
                    "windows":[{"start_unix_ms":1000,"end_unix_ms":31000,"completed_sends":3,
                        "timed_out_sends":1,"ambiguous_sends":2,
                        "send_outcome_scope":"attempts_started_in_window_including_post_window_outcomes_timeout_is_ambiguous"}]}});
            Ok(Some(format!("OAT_CLIENT_METRICS {terminal}\n")))
        }
        async fn process_logs(&self, _: &OatConfig) -> Result<Vec<ProcessLog>> {
            Ok(vec![ProcessLog {
                role: ComponentRole::Router,
                instance_id: "router-fault-test".to_owned(),
                image: None,
                image_id: None,
                source_revision: None,
                complete: true,
                raw_log_bytes: 0,
                raw_log_lines: 1,
                stdout: String::new(),
                stderr: String::new(),
                text: "arrowcol_router_effective_config {\"log_filter\":\"warn,tiberius=error\"}\n".to_owned(),
            }])
        }
        async fn process_logs_for_role(&self, _: &OatConfig, role: ComponentRole) -> Result<Vec<ProcessLog>> {
            if matches!(self.fault, Fault::GuardUnavailable) && role == ComponentRole::Collector {
                let text = concat!(
                    "arrowcol_effective_config ",
                    "{\"commit_telemetry\":{\"schema_version\":1,\"scope\":\"new_ingestion_addfiles_after_sync\",",
                    "\"clock\":\"system_unix_ms\"},\"pressure\":{\"rss_limit_bytes\":1000,\"buffered_bytes_limit\":1000}}\n"
                ).to_owned();
                return Ok(vec![ProcessLog {
                    role, instance_id: "pod/collector".to_owned(), image: None, image_id: None,
                    source_revision: None, complete: true, raw_log_bytes: text.len() as u64,
                    raw_log_lines: 1, stdout: text.clone(), stderr: String::new(), text,
                }]);
            }
            Ok(Vec::new())
        }
        async fn cleanup(&mut self, _: &OatConfig) -> Result<()> {
            Ok(())
        }
    }

    async fn run_fault(fault: Fault, kind: crate::config::OatMode) -> Result<(Vec<OatResult>, FaultPlatform)> {
        run_fault_with_lead(fault, kind, None).await
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "subprocess fixture for default Tokio worker stack"]
    fn matrix_default_worker_stack_child() -> Result<()> {
        anyhow::ensure!(std::env::var_os("OAT_STACK_FIXTURE_CHILD").is_some(), "run the parent stack fixture");
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build()?;
        runtime.block_on(async {
            tokio::spawn(async {
                let listener = TcpListener::bind("127.0.0.1:0").await?;
                let endpoint = format!("http://{}", listener.local_addr()?);
                let server = tokio::spawn(async move {
                    axum::serve(listener, axum::Router::new().fallback(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE })).await
                });
                let platform = FaultPlatform {
                    kind: crate::config::OatMode::Aks, fault: Fault::GuardUnavailable, endpoint,
                    start: std::sync::atomic::AtomicU64::new(0), clients_started: 0,
                    pin_init_calls: 0, qap_initialization_calls: 0, client_fleet_prepared: false,
                    client_schedule_wait_seconds: None, arm_attempts: 0, arm_started: None,
                    delivery_drop_ms: std::sync::atomic::AtomicU64::new(0),
                    cancellations: std::sync::Mutex::new(Vec::new()), unresolved: false,
                };
                let mut config: OatConfig = serde_json::from_str(include_str!(
                    "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
                ))?;
                config.platform.kind = crate::config::OatMode::Aks;
                config.platform.compose_file = None;
                config.platform.compose_override_file = None;
                config.platform.compose_project = None;
                config.platform.compose_state_root = None;
                std::fs::write("runtime.json", "")?;
                std::fs::write("bootstrap.env", "")?;
                config.platform.runtime_config_file = Some("runtime.json".to_owned());
                config.platform.bootstrap_env_file = Some("bootstrap.env".to_owned());
                config.stamp = Some("00000".to_owned());
                config.edge_secret_name = "arrowcol-router-auth".to_owned();
                config.dmv_client_image = "fixture.invalid/client:fixture".to_owned();
                config.result_url = "abfss://experiments@fixture.dfs.core.windows.net/results.parquet".to_owned();
                config.website_url = "abfss://$web@fixture.dfs.core.windows.net/results.parquet".to_owned();
                config.overload_guard.enabled = true;
                config.replica_counts = vec![1];
                config.dfe_worker_concurrency = vec![1];
                let state = Arc::new(AppState::default());
                let (_cancel, cancellation) = watch::channel(false);
                let outcome = STACK_TEST_PLATFORM.scope(
                    std::sync::Mutex::new(Some(Box::new(platform))),
                    run_matrix(&config, "stack-fixture", state, cancellation),
                ).await;
                server.abort();
                let error = outcome.error.context("matrix unexpectedly admitted the unprotected fixture")?;
                anyhow::ensure!(error.to_string().contains("overload_guard required protection unavailable before admission"), "unexpected matrix outcome: {error:#}");
                anyhow::ensure!(outcome.aks_cleanup_confirmed, "fixture platform cleanup did not complete");
                Ok::<_, anyhow::Error>(())
            }).await?
        })
    }

    #[cfg(unix)]
    #[test]
    fn matrix_runs_on_default_tokio_worker_stack() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("oat-stack-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root)?;
        let kubectl = root.join("kubectl");
        fs::write(&kubectl, "#!/bin/sh\nprintf x > \"$0.hit\"\nexit 23\n")?;
        fs::set_permissions(&kubectl, fs::Permissions::from_mode(0o700))?;
        let output = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", "runner::downstream_runner_tests::matrix_default_worker_stack_child", "--ignored", "--nocapture"])
            .env_remove("RUST_MIN_STACK")
            .env("OAT_STACK_FIXTURE_CHILD", "1")
            .env("PATH", &root)
            .current_dir(&root)
            .output()?;
        let reached_guard = root.join("kubectl.hit").is_file();
        fs::remove_dir_all(&root)?;
        anyhow::ensure!(output.status.success(), "matrix worker subprocess failed:\n{}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        anyhow::ensure!(reached_guard, "guard did not reach its live readback commands");
        Ok(())
    }

    async fn run_fault_with_lead(
        fault: Fault,
        kind: crate::config::OatMode,
        lead: Option<u64>,
    ) -> Result<(Vec<OatResult>, FaultPlatform)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let metric = if matches!(fault, Fault::Pinning) {
            "arrowcol_router_failures_total{reason=\"pinning_violation\"} 1\n"
        } else {
            "arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"telemetry\",schema=\"sqlserver\",table=\"sqlserver_active_sessions\",collector=\"collector-a\",state=\"active\"} 1\n"
        };
        let metric = format!("{metric}arrowcol_router_sql_connections_opened_total 0\n");
        let downstream_status = if matches!(fault, Fault::InitBindingRejected | Fault::ObservationBindingRejected) {
            axum::http::StatusCode::BAD_REQUEST
        } else {
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        };
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new()
                    .route("/metrics", axum::routing::get(move || async move { metric }))
                    .fallback(move || async move { downstream_status }),
            )
            .await
        });
        let mut platform = FaultPlatform {
            kind,
            fault,
            endpoint,
            start: std::sync::atomic::AtomicU64::new(0),
            clients_started: 0,
            pin_init_calls: 0,
            qap_initialization_calls: 0,
            client_fleet_prepared: false,
            client_schedule_wait_seconds: None,
            arm_attempts: 0,
            arm_started: None,
            delivery_drop_ms: std::sync::atomic::AtomicU64::new(0),
            cancellations: std::sync::Mutex::new(Vec::new()),
            unresolved: false,
        };
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.platform.kind = kind;
        config.overload_guard.enabled = false;
        config.qap_timeout_seconds = if matches!(fault, Fault::InitTimeout) { 1 } else { 10 };
        if let Some(lead) = lead {
            config.schedule_lead_seconds = lead;
        }
        if matches!(fault, Fault::ScheduleArmBlocked | Fault::ScheduleAckBlocked | Fault::ScheduleAckCancellationFailed) {
            config.clients_per_table = 3;
        }
        let run_id = uuid::Uuid::new_v4().to_string();
        let result = run_matrix_cell(
            &config,
            "orchestration",
            &run_id,
            1,
            1,
            &mut platform,
            tokio::time::Instant::now() + Duration::from_millis(config.timing_budget()?.planned_cell_duration_ms),
        )
        .await;
        server.abort();
        let results = Path::new("out/arrowcol-oat/qap").join(&run_id);
        if results.exists() {
            fs::remove_dir_all(&results)?;
        }
        if matches!(fault, Fault::PinInitFailed) {
            anyhow::ensure!(result.is_err(), "injected run-table initialization failure was ignored");
            return Ok((Vec::new(), platform));
        }
        Ok((result?, platform))
    }

    #[tokio::test]
    async fn cancellation_preserves_each_terminal_report_when_another_capture_fails() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!("../../../docs/experiments/arrowcol-oat/oat-config.quick.json"))?;
        let platform = FaultPlatform {
            kind: crate::config::OatMode::Local, fault: Fault::CancellationCapture,
            endpoint: "http://127.0.0.1:1".to_owned(),
            start: std::sync::atomic::AtomicU64::new(0), clients_started: 0,
            pin_init_calls: 0, qap_initialization_calls: 0, client_fleet_prepared: false,
            client_schedule_wait_seconds: None, unresolved: false, arm_attempts: 0, arm_started: None,
            delivery_drop_ms: std::sync::atomic::AtomicU64::new(0), cancellations: std::sync::Mutex::new(Vec::new()),
        };
        let handles = (1..=2).map(|id| ClientHandle { client_id: format!("client-{id}"),
            table: "table".to_owned(), process: ClientProcess::Local { container_id: format!("container-{id}") } }).collect::<Vec<_>>();
        let mut evidence = CellEvidence { measurement_window: Some((1000, 31000)), ..Default::default() };
        let result = cancel_active_clients_with_evidence(&platform, &config, &handles, &[true, true], &mut evidence).await;
        assert!(result.is_err());
        let reports = &evidence.client_reports["table"];
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0]["phase_counters"]["measurement_window"]["windows"][0]["completed_sends"], 3);
        assert_eq!(reports[0]["phase_counters"]["measurement_window"]["windows"][0]["ambiguous_sends"], 2);
        assert_eq!(reports[1]["phase_counters"]["unavailable_reason"], "terminal_client_window_unavailable");
        assert_eq!(platform.cancellations.lock().map_err(|_| anyhow::anyhow!("cancellation log mutex poisoned"))?.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn overload_unavailable_pre_admission_readback_never_creates_clients() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let mut platform = FaultPlatform {
            kind: crate::config::OatMode::Local, fault: Fault::InitUnavailable,
            endpoint: "http://127.0.0.1:1".to_owned(),
            start: std::sync::atomic::AtomicU64::new(0), clients_started: 0,
            pin_init_calls: 0, qap_initialization_calls: 0, client_fleet_prepared: false,
            client_schedule_wait_seconds: None, unresolved: false,
            arm_attempts: 0, arm_started: None,
            delivery_drop_ms: std::sync::atomic::AtomicU64::new(0),
            cancellations: std::sync::Mutex::new(Vec::new()),
        };
        let report = run_clients(&config, &uuid::Uuid::new_v4().to_string(), &config.tables, &mut platform, 1).await?;
        assert_eq!(platform.clients_started, 0);
        assert!(report.failure.as_deref().is_some_and(|reason| reason.contains("protection unavailable")));
        assert_eq!(report.evidence.overload_guard.as_ref().map(|guard| &guard["status"]), Some(&json!("unavailable")));
        assert!(report.evidence.overload_stop.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn run_table_initialization_failure_stops_before_client_admission() -> Result<()> {
        let (_, platform) = run_fault(Fault::PinInitFailed, crate::config::OatMode::Local).await?;
        assert_eq!(platform.pin_init_calls, 1);
        assert_eq!(platform.clients_started, 0);
        assert!(!platform.client_fleet_prepared);
        assert_eq!(platform.qap_initialization_calls, 0);
        assert_eq!(platform.arm_attempts, 0);
        Ok(())
    }

    #[tokio::test]
    async fn schedule_lead_reaches_actual_arm_client_wait_budgets_and_acknowledgement_guard() -> Result<()> {
        let mut default_budgets = None;
        for (requested, selected) in [(None, 180), (Some(17), 17)] {
            let (rows, platform) = run_fault_with_lead(
                Fault::ObservationUnavailable, crate::config::OatMode::Local, requested
            ).await?;
            assert_eq!(rows[0].status, "succeeded", "{:?}", rows[0].failure);
            let tunables: Value = serde_json::from_str(&rows[0].tunables_json)?;
            assert_eq!(tunables["schedule_lead_seconds"], selected);
            let evidence: Value = serde_json::from_str(rows[0].evidence_json.as_deref().context("missing evidence")?)?;
            let schedule = &evidence["client_schedule"];
            assert_eq!(schedule["requested"]["schedule_lead_seconds"], selected);
            assert_eq!(schedule["effective"]["schedule_lead_seconds"], selected);
            let effective = &schedule["effective"];
            let start = effective["run_start_unix_ms"].as_u64().context("missing actual start")?;
            assert_eq!(platform.start.load(std::sync::atomic::Ordering::SeqCst), start);
            assert_eq!(start - effective["schedule_created_at_unix_ms"].as_u64().context("missing creation time")?, selected * 1_000);
            assert!(effective["acknowledged_at_unix_ms"].as_u64().context("missing acknowledgement")? < start);
            let measurement_start = effective["measurement_start_unix_ms"].as_u64().context("measurement start")?;
            let measurement_end = effective["measurement_end_unix_ms"].as_u64().context("measurement end")?;
            assert_eq!(measurement_start - start, tunables["warmup_seconds"].as_u64().context("warmup")? * 1_000);
            assert_eq!(measurement_end - measurement_start, tunables["measure_seconds"].as_u64().context("measurement")? * 1_000);
            let schedule_wait = effective["schedule_wait_seconds"].as_u64().context("client wait")?;
            assert_eq!(platform.client_schedule_wait_seconds, Some(schedule_wait));
            let budgets = [
                schedule_wait * 1_000,
                effective["client_job_lifetime_seconds"].as_u64().context("client lifetime")? * 1_000,
                effective["minimum_pin_ttl_ms"].as_u64().context("pin TTL")?,
                effective["planned_cell_duration_ms"].as_u64().context("cell duration")?,
            ];
            if let Some(defaults) = default_budgets {
                for (default, configured) in std::iter::zip(defaults, budgets) {
                    assert_eq!(default - configured, (180 - selected) * 1_000);
                }
            } else {
                default_budgets = Some(budgets);
            }
            let progress = |received| ClientProgress {
                ready: true, schedule_received: received, ..ClientProgress::default()
            };
            assert!(!validate_client_schedule(&[progress(false)], start - 1, start)?);
            assert!(validate_client_schedule(&[progress(false)], start, start).is_err());
            assert!(validate_client_schedule(&[progress(true)], start, start).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn schedule_delivery_timeout_drops_blocked_work_without_renewal_or_partial_success() -> Result<()> {
        for fault in [Fault::ScheduleArmBlocked, Fault::ScheduleAckBlocked, Fault::ScheduleAckCancellationFailed] {
            let (rows, platform) = tokio::time::timeout(
                Duration::from_secs(10),
                run_fault_with_lead(fault, crate::config::OatMode::Local, Some(2)),
            ).await.context("production delivery did not terminate within the bounded witness")??;
            assert_eq!(platform.arm_attempts, 1, "{fault:?}");
            assert_eq!(platform.clients_started, 3, "{fault:?}");
            let dropped_after_ms = platform.delivery_drop_ms.load(std::sync::atomic::Ordering::SeqCst);
            assert!((1_900..=2_100).contains(&dropped_after_ms), "{fault:?}: dropped after {dropped_after_ms}ms");
            let cancelled = platform.cancellations.lock()
                .map_err(|error| anyhow::anyhow!("fixture cancellations poisoned: {error}"))?;
            assert_eq!(cancelled.len(), 3, "{fault:?}");
            assert_eq!(cancelled.iter().cloned().collect::<HashSet<_>>(),
                ["client-1", "client-2", "client-3"].map(str::to_owned).into_iter().collect::<HashSet<_>>());
            assert_eq!(rows[0].status, "failed", "{fault:?}");
            let failure = rows[0].failure.as_deref().context("missing timeout failure")?;
            assert!(failure.contains("schedule delivery/acknowledgement timed out"), "{failure}");
            assert!(failure.contains("not retried"), "{failure}");
            if matches!(fault, Fault::ScheduleAckCancellationFailed) {
                assert!(failure.contains("fixture_cancel_failed"), "{failure}");
                assert!(failure.contains("cancelling clients after schedule delivery failure also failed"), "{failure}");
            }
            assert!(rows[0].rows.is_none() && rows[0].duration_ms.is_none() && rows[0].rows_per_second.is_none());
            let evidence: Value = serde_json::from_str(rows[0].evidence_json.as_deref().context("missing failure evidence")?)?;
            assert!(evidence["client_schedule"].is_null());
            assert!(evidence["backlog"]["measurement_window"].is_null());
            eprintln!("delivery_fault={fault:?} dropped_after_ms={dropped_after_ms} arm_attempts={} cancelled={:?}; measurement/schedule receipt absent", platform.arm_attempts, *cancelled);
        }
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_initialization_and_observation_preserve_local_measurements_only() -> Result<()> {
        for fault in [
            Fault::InitUnavailable,
            Fault::InitTimeout,
            Fault::ObservationUnavailable,
        ] {
            let (rows, platform) = run_fault(fault, crate::config::OatMode::Local).await?;
            assert_eq!(rows[0].status, "succeeded", "failure: {:?}", rows[0].failure);
            assert_eq!(rows[0].rows, Some(100));
            assert!(rows[0].qap.completion_percent.is_none());
            let evidence: Value = serde_json::from_str(rows[0].evidence_json.as_deref().context("evidence")?)?;
            assert_eq!(evidence["downstream_status"], "unavailable");
            if matches!(fault, Fault::InitTimeout) {
                assert!(platform.unresolved);
            }
            let (rows, _) = run_fault(fault, crate::config::OatMode::Aks).await?;
            assert_eq!(rows[0].status, "failed");
        }
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_downstream_fault_runs_keep_qap_artifacts_isolated() -> Result<()> {
        let (unavailable, rejected) = tokio::join!(
            run_fault(Fault::ObservationUnavailable, crate::config::OatMode::Local),
            run_fault(Fault::ObservationBindingRejected, crate::config::OatMode::Local),
        );
        let (unavailable, _) = unavailable?;
        let (rejected, _) = rejected?;
        assert_ne!(unavailable[0].run_id, rejected[0].run_id);
        assert_eq!(unavailable[0].status, "succeeded", "{:?}", unavailable[0].failure);
        assert_eq!(rejected[0].status, "failed");
        Ok(())
    }

    #[tokio::test]
    async fn local_degradation_never_hides_required_setup_collector_or_pinning_failures() -> Result<()> {
        for fault in [
            Fault::InitRequired,
            Fault::InitBindingRejected,
            Fault::ObservationBindingRejected,
            Fault::Collector,
            Fault::Pinning,
        ] {
            let (rows, _) = run_fault(fault, crate::config::OatMode::Local).await?;
            assert_ne!(rows[0].status, "succeeded");
            assert!(rows[0].failure.is_some());
        }
        Ok(())
    }
}
fn client_snapshot_delta(snapshots: &[ClientCounterSnapshot], data_profile: ClientDataProfile) -> Result<Value> {
    let phase = |name| -> Result<&ClientCounterSnapshot> {
        let mut matches = snapshots.iter().filter(|snapshot| snapshot.phase == name);
        let snapshot = matches.next().context("missing client counter phase")?;
        if matches.next().is_some() {
            bail!("duplicate client counter phase");
        }
        Ok(snapshot)
    };
    for snapshot in snapshots {
        if snapshot.version != 1
            || snapshot.scope != "process_cumulative"
            || !snapshot.elapsed_seconds.is_finite()
            || snapshot.elapsed_seconds < 0.0
        {
            bail!("unsupported client counter contract");
        }
        let mut workers = HashSet::new();
        if snapshot.workers.is_empty() || snapshot.workers.iter().any(|worker| !workers.insert(worker.worker)) {
            bail!("empty or duplicate client counter workers");
        }
    }
    let start = phase("measurement_start")?;
    let end = phase("completed")?;
    if let Some(window) = &end.measurement_window
        && (window.scope != "per_stream_admission_completion_observed_in_half_open_shared_unix_window"
            || window.start_unix_ms >= window.end_unix_ms
            || window.worker_completed_rows.len() != end.workers.len()
            || window
                .worker_completed_rows
                .iter()
                .try_fold(0_u64, |sum, rows| sum.checked_add(*rows))
                != Some(window.completed_rows))
    {
        bail!("invalid exact client measurement-window accounting");
    }
    if end.elapsed_seconds < start.elapsed_seconds || end.workers.len() != start.workers.len() {
        bail!("client counter interval or worker count changed");
    }
    let mut deltas = Vec::new();
    for worker in &end.workers {
        let before = start
            .workers
            .iter()
            .find(|before| before.worker == worker.worker)
            .context("client counter worker identity changed")?;
        let difference = |after: u64, before: u64| after.checked_sub(before).context("client counter reset");
        if let Some(latency) = &worker.send_latency_us
            && (latency.precision != "power_of_two_upper_bound"
                || [latency.p50, latency.p95, latency.p99]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .windows(2)
                    .any(|pair| pair[0] > pair[1]))
        {
            bail!("invalid client latency evidence");
        }
        deltas.push(json!({
            "worker": worker.worker,
            "attempted_rows": difference(worker.attempted_rows, before.attempted_rows)?,
            "attempted_bytes": difference(worker.attempted_bytes, before.attempted_bytes)?,
            "client_completed_rows": difference(worker.completed_rows, before.completed_rows)?,
            "client_completed_bytes": difference(worker.completed_bytes, before.completed_bytes)?,
            "error_rows": difference(worker.error_rows, before.error_rows)?,
            "error_bytes": difference(worker.error_bytes, before.error_bytes)?,
            "cancelled_rows": difference(worker.cancelled_rows, before.cancelled_rows)?,
            "cancelled_bytes": difference(worker.cancelled_bytes, before.cancelled_bytes)?,
            "best_effort_streams": difference(worker.best_effort_streams, before.best_effort_streams)?,
            "durable_streams": difference(worker.durable_streams, before.durable_streams)?,
            "rejected_streams": difference(worker.rejected_streams, before.rejected_streams)?,
            "rejected_rows": difference(worker.rejected_rows, before.rejected_rows)?,
            "retried_streams": difference(worker.retried_streams.unwrap_or(0), before.retried_streams.unwrap_or(0))?,
            "retried_rows": difference(worker.retried_rows.unwrap_or(0), before.retried_rows.unwrap_or(0))?,
            "given_up_streams": difference(worker.given_up_streams, before.given_up_streams)?,
            "given_up_rows": difference(worker.given_up_rows, before.given_up_rows)?,
            "ambiguous_streams": difference(worker.ambiguous_streams.unwrap_or(0), before.ambiguous_streams.unwrap_or(0))?,
            "ambiguous_rows": difference(worker.ambiguous_rows.unwrap_or(0), before.ambiguous_rows.unwrap_or(0))?,
            "process_cumulative_send_latency_us": worker.send_latency_us,
        }));
    }
    let cumulative_total = |field: fn(&WorkerCounterSnapshot) -> u64| -> Result<u64> {
        end.workers
            .iter()
            .try_fold(0_u64, |total, worker| total.checked_add(field(worker)))
            .context("client cumulative counter overflow")
    };
    let cumulative_proven = |field: fn(&WorkerCounterSnapshot) -> Option<u64>| -> Option<u64> {
        end.workers.iter().try_fold(0_u64, |total, worker| total.checked_add(field(worker)?))
    };
    let preparation = client_preparation(end.preparation.as_ref(), data_profile)?;
    let mut result = json!({
        "scope": "measurement_start_to_completed_snapshot_delta_not_unique_delivery",
        "process_cumulative": {
            "completed_rows": cumulative_total(|worker| worker.completed_rows)?,
            "ambiguous_rows": cumulative_total(|worker| worker.ambiguous_rows.unwrap_or(0))?,
        },
        "delivery_cumulative": {
            "scope": "completed_process_lifetime_counters_not_unique_delivery",
            "retried_streams": cumulative_proven(|worker| worker.retried_streams),
            "retried_rows": cumulative_proven(|worker| worker.retried_rows),
            "ambiguous_streams": cumulative_proven(|worker| worker.ambiguous_streams),
            "ambiguous_rows": cumulative_proven(|worker| worker.ambiguous_rows),
        },
        "elapsed_seconds": end.elapsed_seconds - start.elapsed_seconds,
        "completion_semantics": "validated_admission_response_not_delta_commit",
        "bytes_semantics": "sum_of_protobuf_flight_data_encoded_lengths_excluding_grpc_http2",
        "latency_scope": "process_cumulative_includes_preflight_and_warmup_not_phase_percentiles",
        "workers": deltas,
        "preparation": preparation,
        "measurement_window": end.measurement_window,
    });
    if let Some(hash) = preparation.get("payload_corpus_sha256") {
        let manifest = end.logical_batch_manifest.as_ref().context("payload hash lacks a terminal manifest")?;
        let (identity_status, identity_eligible, complete) = match data_profile {
            ClientDataProfile::Diagnostic => ("diagnostic_only", false, false),
            ClientDataProfile::Realistic => ("identity_bearing", true, true),
        };
        if manifest["payload_corpus_sha256"] != *hash
            || manifest["data_profile"].as_str() != Some(client_profile_name(data_profile))
            || manifest["payload_corpus_hash_version"].as_u64() != Some(2)
            || manifest["payload_corpus_hash_scope"].as_str() != Some(client_payload_hash_scope(data_profile))
            || manifest["identity_status"].as_str() != Some(identity_status)
            || manifest["eligible_for_identity_reconciliation"].as_bool() != Some(identity_eligible)
            || manifest["complete"].as_bool() != Some(complete)
            || manifest["truncated"].as_bool() != Some(false)
        {
            bail!("client payload hash disagrees with terminal manifest");
        }
        result["payload_manifest"] = manifest.clone();
    }
    Ok(result)
}

#[test]
fn resource_summary_is_observed_mean_and_max_not_an_end_snapshot() -> Result<()> {
    let mut evidence = CellEvidence::default();
    assert!(evidence.resource_summary().is_none());
    for (cpu_millicores, memory_bytes) in [(100.0, 200), (300.0, 100)] {
        evidence.resource_samples.push(ResourceSample {
            sampled_at_unix_ms: 1,
            metrics: ResourceMetrics {
                cpu_millicores,
                memory_bytes,
            },
        });
    }
    let metrics = evidence
        .resource_summary()
        .context("missing observed resource summary")?;
    assert_eq!(metrics.cpu_millicores, 200.0);
    assert_eq!(metrics.memory_bytes, 200);
    Ok(())
}

#[test]
fn cell_evidence_includes_router_cpu_pressure() {
    let mut evidence = CellEvidence::default();
    let cpu_pressure = json!({"status": "observed", "max_utilization_bps": 731});
    evidence.router_cpu_pressure = Some(cpu_pressure.clone());

    let cell = evidence.for_table("table", None);

    assert_eq!(cell["router_cpu_pressure"], cpu_pressure);
}
