const ADMISSION_PRESSURE_MAX_JSON_BYTES: usize = 4096;

fn sanitized_scale_out_identity(identity: &ScaleOutCollectorIdentity) -> Value {
    let pod = &identity.pod;
    let pod = (pod.len() <= 253
        && !pod.is_empty()
        && pod.bytes().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-' || c == b'.'))
        .then_some(pod);
    let uid = |value: &str| uuid::Uuid::parse_str(value).ok();
    let container = identity.container_id.split_once("://").filter(|(runtime, id)| {
        matches!(*runtime, "containerd" | "docker" | "cri-o")
            && id.len() == 64
            && id.bytes().all(|c| c.is_ascii_hexdigit())
    });
    json!({
        "pod": pod,
        "pod_uid": uid(&identity.pod_uid),
        "replica_set_uid": uid(&identity.replica_set_uid),
        "container_id": container.map(|_| &identity.container_id),
    })
}

fn pressure_body_excerpt(body: &str) -> Value {
    use sha2::{Digest, Sha256};

    let parsed = serde_json::from_str::<Value>(body);
    let mut excerpt = serde_json::Map::new();
    if let Ok(value) = &parsed {
        for field in [
            "actual_rss_bytes", "dfe_channel_capacity", "dfe_channel_occupancy",
            "native_quiver_enabled", "native_quiver_buffered_bytes", "native_quiver_buffered_rows",
            "native_quiver_buffered_batches", "replayed_batches", "rebound_batches", "replay_deferred_batches",
        ] {
            if let Some(raw) = value.get(field) {
                excerpt.insert(field.to_owned(), safe_diagnostic_scalar(Some(raw), false));
            }
        }
        if let Some(mode) = value.get("resolved_buffering_mode") {
            let mode = match mode.as_str() {
                Some(mode @ ("memory_only" | "unknown" | "quiver" | "mixed" | "spill_recovery")) => json!(mode),
                _ => safe_diagnostic_scalar(Some(mode), false),
            };
            excerpt.insert("resolved_buffering_mode".to_owned(), mode);
        }
        if let Some(rich) = value["rich"].as_object() {
            let mut selected = serde_json::Map::new();
            if let Some(raw) = rich.get("instance_id") {
                selected.insert(
                    "instance_id".to_owned(),
                    json!(raw.as_str().and_then(|id| uuid::Uuid::parse_str(id).ok())),
                );
            }
            for field in [
                "observed_at_unix_ms", "cpu_utilization_bps", "rss_bytes", "rss_limit_bytes",
                "received_bytes", "received_rows", "received_batches", "oldest_buffer_age_ms",
                "delta_pressure_bps", "quiver_pressure_bps", "dfe_pressure_bps",
            ] {
                if let Some(raw) = rich.get(field) {
                    selected.insert(field.to_owned(), safe_diagnostic_scalar(Some(raw), false));
                }
            }
            for field in ["active_streams", "buffered_bytes", "buffered_rows", "buffered_batches", "available_permits", "pending_commits"] {
                if let Some(raw) = rich.get(field) {
                    selected.insert(field.to_owned(), json!({
                        "current": safe_diagnostic_scalar(raw.get("current"), false),
                        "limit": safe_diagnostic_scalar(raw.get("limit"), false),
                    }));
                }
            }
            excerpt.insert("rich".to_owned(), json!(selected));
        }
    }
    let excerpt = json!(excerpt);
    let excerpt_bytes = excerpt.to_string().len();
    json!({
        "body_bytes": body.len(),
        "body_sha256": format!("{:x}", Sha256::digest(body)),
        "valid_json": parsed.is_ok(),
        "excerpt_policy": "allowlisted_pressure_fields",
        "excerpt": (excerpt_bytes <= ADMISSION_PRESSURE_MAX_JSON_BYTES && parsed.is_ok()).then_some(excerpt),
    })
}

fn safe_diagnostic_scalar(value: Option<&Value>, duration: bool) -> Value {
    match value {
        None => json!({"missing": true}),
        Some(value) if value.is_boolean() || value.is_number() || value.is_null() => value.clone(),
        Some(Value::String(text))
            if duration && text.len() <= 64
                && text.bytes().all(|c| c.is_ascii_digit() || b" .nushmdw".contains(&c))
                && arrowcol_tunables::parse_tunable_duration("pressure evidence", text).is_ok() =>
        {
            json!(text)
        }
        Some(_) => json!({"redacted": true}),
    }
}

fn startup_mismatch(role: ComponentRole, name: &str, requested: &Value, observed: Option<&Value>) -> Value {
    let pressure = name == "ARROWCOL_COLLECTOR_PRESSURE_CONFIG";
    let comparison = |field: &str, requested: &Value, observed: Option<&Value>| {
        let duration = pressure && matches!(field, "pressure_interval" | "heartbeat_interval" | "registration_ttl");
        json!({
            "field":field,
            "requested":safe_diagnostic_scalar(Some(requested), duration),
            "observed":safe_diagnostic_scalar(observed, duration),
        })
    };
    let fields = if pressure {
        [
            "pressure_interval", "heartbeat_interval", "registration_ttl", "rss_limit_bytes",
            "buffered_bytes_limit", "buffered_rows_limit", "buffered_batches_limit", "max_concurrent_streams",
        ].into_iter().filter_map(|field| {
            let requested = requested.get(field)?;
            let actual = observed.and_then(|value| value.get(field));
            let matches = actual.is_some_and(|actual| {
                matches_collector_pressure(&json!({field: actual}), &json!({field: requested})).unwrap_or(false)
            });
            (!matches).then(|| comparison(field, requested, actual))
        }).collect::<Vec<_>>()
    } else {
        vec![comparison("$", requested, observed)]
    };
    json!({"component":role_name(role),"tunable":name,"fields":fields})
}

fn retain_admission_diagnostics(
    root: &Path,
    orchestration_id: &str,
    cell_id: &str,
    guard: Option<&mut Value>,
    startup_readback_diff: Option<Value>,
) {
    if startup_readback_diff.is_none()
        && guard.as_ref().is_none_or(|guard| guard["admission_diagnostics"].as_object().is_none_or(serde_json::Map::is_empty))
    {
        return;
    }
    let mut details = guard.as_ref().and_then(|guard| guard.get("admission_diagnostics")).cloned().unwrap_or_else(|| json!({}));
    if let Some(diff) = startup_readback_diff {
        if details["startup_readback_diff"].is_null() {
            details["startup_readback_diff"] = diff;
        } else if details["startup_readback_diff"] != diff {
            details["post_client_startup_readback_diff"] = diff;
        }
    }
    if let Some(guard) = guard {
        guard["admission_diagnostics"] = details.clone();
    }
    let save = || -> Result<()> {
        let orchestration_id = uuid::Uuid::parse_str(orchestration_id)?;
        let cell_id = uuid::Uuid::parse_str(cell_id)?;
        let directory = root.join(orchestration_id.to_string());
        fs::create_dir_all(&directory)?;
        let path = directory.join(format!("{cell_id}.json"));
        write_private_secret(&path, &json!({
            "version":1, "run_id":orchestration_id, "cell_id":cell_id, "admission_diagnostics":details,
        }).to_string())
    };
    if let Err(error) = save() {
        tracing::warn!(%error, "could not retain OAT admission diagnostics");
    }
}

#[cfg(test)]
mod admission_diagnostics_tests {
    use super::*;

    #[test]
    fn rejected_startup_retains_pressure_values_and_selected_identity() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = serde_json::from_value(json!({"collector":{"pressure":{"registration_ttl":"15m"}}}))?;
        let identity = ScaleOutCollectorIdentity {
            pod: "collector-demo".to_owned(),
            pod_uid: uuid::Uuid::new_v4().to_string(),
            replica_set_uid: uuid::Uuid::new_v4().to_string(),
            container_id: format!("containerd://{}", "a".repeat(64)),
        };
        let mut diff = None;
        let log = ProcessLog {
            role: ComponentRole::Collector,
            instance_id: "collector-demo/collector".to_owned(),
            image: None,
            image_id: None,
            source_revision: None,
            raw_log_bytes: 0,
            raw_log_lines: 1,
            stdout: String::new(),
            stderr: String::new(),
            text: "arrowcol_effective_config {\"component\":\"receiver\",\"config\":{\"pressure\":{\"registration_ttl\":\"899s\"}}}\n".to_owned(),
            complete: true,
        };
        let router = ProcessLog {
            role: ComponentRole::Router,
            instance_id: "router-demo/router".to_owned(),
            text: format!(
                "arrowcol_router_effective_config {}\n",
                json!({"log_filter": role_environment(&config, ComponentRole::Router)?["RUST_LOG"]})
            ),
            ..log.clone()
        };
        let failure = process_evidence_for_cell(&config, &[router, log], &[], &mut diff)
            .err().context("startup mismatch was accepted")?;
        assert!(failure.to_string().contains("startup readback does not match applied tunable"), "{failure:#}");
        let pressure = r#"{"resolved_buffering_mode":"unknown","rich":{"rss_bytes":42}}"#;
        let mut guard = json!({"status":"unavailable","admission_diagnostics":{
            "selected_collector":sanitized_scale_out_identity(&identity),
            "last_pressure":pressure_body_excerpt(pressure)
        }});
        let run = uuid::Uuid::new_v4().to_string();
        let cell = uuid::Uuid::new_v4().to_string();
        let root = std::env::temp_dir().join(format!("oat-admission-{run}"));
        retain_admission_diagnostics(&root, &run, &cell, Some(&mut guard), diff);
        let path = root.join(&run).join(format!("{cell}.json"));
        let artifact: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;
        fs::remove_file(path)?;
        fs::remove_dir(root.join(run))?;
        fs::remove_dir(root)?;
        let details = &artifact["admission_diagnostics"];
        assert_eq!(details, &guard["admission_diagnostics"]);
        assert_eq!(guard["status"], "unavailable");
        assert_eq!(details["selected_collector"], serde_json::to_value(identity)?);
        assert_eq!(details["last_pressure"]["excerpt"]["rich"]["rss_bytes"], 42);
        assert_eq!(details["startup_readback_diff"]["fields"], json!([
            {"field":"registration_ttl","requested":"15m","observed":"899s"}
        ]));
        Ok(())
    }

    #[test]
    fn post_client_mismatch_cannot_overwrite_first_admission_diff() -> Result<()> {
        let first = startup_mismatch(
            ComponentRole::Collector,
            "ARROWCOL_COLLECTOR_PRESSURE_CONFIG",
            &json!({"registration_ttl":"15m"}),
            Some(&json!({"registration_ttl":"899s"})),
        );
        let later = startup_mismatch(
            ComponentRole::Router,
            "ARROWCOL_ROUTER_UPSTREAM_PING_INTERVAL_SECONDS",
            &json!(5),
            Some(&json!(6)),
        );
        let mut guard = json!({"status":"unavailable","admission_diagnostics":{"startup_readback_diff":first}});
        let run = uuid::Uuid::new_v4().to_string();
        let cell = uuid::Uuid::new_v4().to_string();
        let root = std::env::temp_dir().join(format!("oat-admission-{run}"));
        retain_admission_diagnostics(&root, &run, &cell, Some(&mut guard), Some(later.clone()));
        let path = root.join(&run).join(format!("{cell}.json"));
        let artifact: Value = serde_json::from_str(&fs::read_to_string(&path)?)?;
        fs::remove_file(path)?;
        fs::remove_dir(root.join(run))?;
        fs::remove_dir(root)?;
        let details = &artifact["admission_diagnostics"];
        assert_eq!(details["startup_readback_diff"], first);
        assert_eq!(details["post_client_startup_readback_diff"], later);
        assert_eq!(details, &guard["admission_diagnostics"]);
        Ok(())
    }

    #[test]
    fn malformed_or_oversized_content_cannot_leak_into_admission_diagnostics() {
        let raw = json!({
            "resolved_buffering_mode":"private-token",
            "rich":{"instance_id":"private-token","rss_bytes":12,"buffered_bytes":{"current":1,"limit":8},"table_pressure":"private-token"},
            "opaque":"private-token".repeat(8192),
        }).to_string();
        let outcome = pressure_body_excerpt(&raw);
        assert_eq!(outcome["body_bytes"], raw.len());
        assert_eq!(outcome["excerpt"]["rich"]["buffered_bytes"]["limit"], 8);
        assert!(outcome["excerpt"].to_string().len() <= ADMISSION_PRESSURE_MAX_JSON_BYTES);
        assert!(!outcome.to_string().contains("private-token"));
        let malformed = pressure_body_excerpt("not JSON with private-token");
        assert_eq!(malformed["valid_json"], false);
        assert!(malformed["excerpt"].is_null());
        assert!(!malformed.to_string().contains("private-token"));
        let readback = startup_mismatch(
            ComponentRole::Collector, "ARROWCOL_COLLECTOR_PRESSURE_CONFIG",
            &json!({"registration_ttl":"15m"}), Some(&json!({"registration_ttl":"private-token"})),
        );
        assert_eq!(readback["fields"][0]["observed"], json!({"redacted":true}));
        assert!(!readback.to_string().contains("private-token"));
        let identity = sanitized_scale_out_identity(&ScaleOutCollectorIdentity {
            pod: "bad/private-token".into(), pod_uid: "private-token".into(),
            replica_set_uid: "private-token".into(), container_id: "containerd://private-token".into(),
        });
        assert!(identity.as_object().is_some_and(|fields| fields.values().all(Value::is_null)));
    }
}
