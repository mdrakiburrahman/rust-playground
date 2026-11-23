//! Catalog-driven projection regression, not a proof that the catalog lists every typed leaf.
//! Each catalogued input is isolated apart from validation prerequisites. The runner consumes
//! maintenance.enabled, operations and apply as controls rather than environment variables.
//! A role can also be disabled, making its requested logging setting inert. This test checks
//! request and environment projections, not consumer startup readbacks or runner execution.

use arrowcol_tunables::{ApplyBoundary, ComponentRole, Tunables, capability_table};
use serde_json::{Value, json};
use std::time::Duration;

fn set(root: &mut Value, path: &[&str], value: Value) {
    if path.len() == 1 {
        root[path[0]] = value;
    } else {
        if !root[path[0]].is_object() {
            root[path[0]] = json!({});
        }
        set(&mut root[path[0]], &path[1..], value);
    }
}

fn fixture(path: &str, value: Value) -> Value {
    let mut input = json!({});
    let concrete = path.replace("*", "customer_id");
    let parts: Vec<_> = concrete.split('.').collect();
    if path.starts_with("maintenance.") {
        input["maintenance"] =
            json!({"enabled": true, "operations": ["optimize", "purge", "vacuum", "checkpoint", "prune_log"]});
    }
    if path == "router.sql_pool_acquire_timeout_ms" {
        input["router"] = json!({"sql_pool_max_size": 1});
    }
    if path == "collector.upload.buffer_capacity_bytes" || path == "collector.upload.request_concurrency" {
        input["collector"] = json!({"upload": {"mode": "buffered_multipart"}});
    }
    if path == "maintenance.optimize_upload_buffer_bytes" {
        input["maintenance"]["optimize_upload_mode"] = json!("buffered_multipart");
    }
    if path.ends_with(".compression_level") {
        let mut parent = parts[..parts.len() - 1].to_vec();
        parent.push("compression");
        set(&mut input, &parent, json!("gzip"));
    }
    set(&mut input, &parts, value);
    input
}

fn candidates(path: &str) -> Vec<Value> {
    if path.ends_with("sorting_columns") {
        return vec![json!([{ "path": "customer_id", "descending": false, "nulls_first": false }])];
    }
    if path.ends_with("sort_columns") || path.ends_with("bloom_columns") {
        return vec![json!(["customer_id", "timestamp"])];
    }
    if path.ends_with("excluded_admission_gauges") {
        return vec![json!(["rss_bytes", "active_streams"])];
    }
    if path == "collector.pressure.registration_ttl" {
        return vec![json!("30s")];
    }
    if path == "maintenance.max_concurrent_decoded_bytes" {
        return vec![json!(1_073_741_824_u64)];
    }
    if path == "maintenance.max_output_file_bytes" {
        return vec![json!(134_217_728_u64)];
    }
    if path == "maintenance.allow_short_vacuum_retention" {
        return vec![json!(false)];
    }
    // Candidate discovery must find an accepted non-null value for every catalog entry;
    // it does not skip entries merely because the guessed sample is invalid.
    vec![
        json!(0.25),
        json!(2),
        json!(1),
        json!(1024),
        json!(16384),
        json!(1_048_576),
        json!(4_194_304),
        json!(604_800),
        json!(16_777_216),
        json!(536_870_912),
        json!(1_073_741_824_u64),
        json!(true),
        json!(false),
        json!("1s"),
        json!("10s"),
        json!("customer_id"),
        json!("shared"),
        json!("diagnostic"),
        json!("fixed"),
        json!("cardinality"),
        json!("none"),
        json!("hydrate"),
        json!("drain_ready"),
        json!("process"),
        json!("buffered_multipart"),
        json!("backpressure"),
        json!("pass_through"),
        json!("memory"),
        json!("hour"),
        json!("info"),
        json!("snappy"),
        json!("plain"),
        json!("chunk"),
        json!("v1"),
    ]
}

fn scalar(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Array(values) => values.iter().map(scalar).collect::<Vec<_>>().join(","),
        _ => value.to_string(),
    }
}

fn destination(path: &str, value: &Value) -> (ComponentRole, String, Option<String>, String) {
    let parts: Vec<_> = path.split('.').collect();
    let (role, key, pointer) = match parts[0] {
        "client" | "transport" => (
            ComponentRole::Client,
            format!("ARROWCOL_CLIENT_{}", parts[1].to_ascii_uppercase()),
            None,
        ),
        "arrow_ipc" => (ComponentRole::Client, "ARROWCOL_CLIENT_IPC_COMPRESSION".into(), None),
        "router" => (
            ComponentRole::Router,
            format!("ARROWCOL_ROUTER_{}", parts[1].to_ascii_uppercase()),
            None,
        ),
        "logging" => (
            match parts[1] {
                "router" => ComponentRole::Router,
                "collector" => ComponentRole::Collector,
                "maintenance" => ComponentRole::Maintenance,
                _ => unreachable!(),
            },
            "RUST_LOG".into(),
            None,
        ),
        "collector"
            if [
                "batching", "catalog", "commit", "exporter", "limits", "pressure", "routing",
            ]
            .contains(&parts[1]) =>
        {
            (
                ComponentRole::Collector,
                format!("ARROWCOL_COLLECTOR_{}_CONFIG", parts[1].to_ascii_uppercase()),
                Some(format!("/{}", parts[2..].join("/"))),
            )
        }
        "collector" if parts[1] == "layout" => (
            ComponentRole::Collector,
            format!(
                "ARROWCOL_COLLECTOR_{}{}",
                if ["sort_columns", "bloom_columns"].contains(&parts[2]) {
                    ""
                } else {
                    "LAYOUT_"
                },
                parts[2].to_ascii_uppercase()
            ),
            None,
        ),
        "collector" if parts[1] == "runtime" => (
            ComponentRole::Collector,
            format!(
                "ARROWCOL_COLLECTOR_{}{}",
                if parts[2] == "workers" { "RUNTIME_" } else { "" },
                parts[2].to_ascii_uppercase()
            ),
            None,
        ),
        "collector" if parts[1] == "upload" => (
            ComponentRole::Collector,
            format!("ARROWCOL_COLLECTOR_UPLOAD_{}", parts[2].to_ascii_uppercase()),
            None,
        ),
        "collector" => (
            ComponentRole::Collector,
            format!("ARROWCOL_COLLECTOR_{}", parts[1].to_ascii_uppercase()),
            None,
        ),
        "parquet_writer" => (
            ComponentRole::Collector,
            "ARROWCOL_PARQUET_CONFIG".into(),
            Some(format!("/{}", parts[1..].join("/").replace('*', "customer_id"))),
        ),
        "maintenance" => (
            ComponentRole::Maintenance,
            format!("ARROWCOL_MAINT_{}", parts[1].to_ascii_uppercase()),
            None,
        ),
        "wal" => {
            let suffix = match parts[1] {
                "flush_interval_ms" => "FLUSH_INTERVAL",
                "retention_size_cap_bytes" => "RETENTION_SIZE_CAP",
                "max_age_secs" => "MAX_AGE",
                "max_segment_open_duration_ms" => "MAX_SEGMENT_OPEN_DURATION",
                "poll_interval_ms" => "POLL_INTERVAL",
                "initial_retry_interval_ms" => "INITIAL_RETRY_INTERVAL",
                "max_retry_interval_ms" => "MAX_RETRY_INTERVAL",
                other => {
                    return (
                        ComponentRole::Collector,
                        format!("ARROWCOL_WAL_{}", other.to_ascii_uppercase()),
                        None,
                        scalar(value),
                    );
                }
            };
            (ComponentRole::Collector, format!("ARROWCOL_WAL_{suffix}"), None)
        }
        _ => panic!("unknown catalog section {path}"),
    };
    let expected = if path == "logging.router" {
        format!("{},tiberius=error", scalar(value))
    } else if path == "wal.retention_size_cap_bytes" {
        format!("{} B", value.as_u64().unwrap())
    } else if parts[0] == "wal" && (parts[1].ends_with("_ms") || parts[1] == "max_age_secs") {
        let amount = value.as_u64().unwrap();
        humantime::format_duration(if parts[1] == "max_age_secs" {
            Duration::from_secs(amount)
        } else {
            Duration::from_millis(amount)
        })
        .to_string()
    } else {
        scalar(value)
    };
    (role, key, pointer, expected)
}

#[test]
fn catalogued_oat_leaves_reach_their_role_and_destination() {
    let mut errors = Vec::new();
    for capability in capability_table() {
        let path = capability.name;
        if capability.boundary == ApplyBoundary::RebuildOnly {
            continue;
        }
        // These are runner controls or an entire nested writer, covered separately below.
        if [
            "maintenance.enabled",
            "maintenance.operations",
            "maintenance.apply",
            "maintenance.parquet_writer",
        ]
        .contains(&path)
        {
            continue;
        }
        let Some((sample, tunables)) = candidates(path).into_iter().find_map(|value| {
            Tunables::parse_oat_json(fixture(path, value.clone()))
                .ok()
                .map(|settings| (value, settings))
        }) else {
            errors.push(format!("{path}: no accepted candidate"));
            continue;
        };
        let (role, key, pointer, expected) = destination(path, &sample);
        let env = tunables.environment_for(role).unwrap();
        let actual = env.get(&key).and_then(|raw| {
            if let Some(pointer) = &pointer {
                serde_json::from_str::<Value>(raw)
                    .ok()
                    .and_then(|value| value.pointer(pointer).cloned())
                    .map(|value| scalar(&value))
            } else {
                Some(raw.clone())
            }
        });
        if actual.as_deref() != Some(expected.as_str()) {
            errors.push(format!(
                "{path}: expected {role:?} {key} {pointer:?} = {expected:?}; got {actual:?}"
            ));
        }
        if key != "RUST_LOG" {
            for other in [
                ComponentRole::Client,
                ComponentRole::Router,
                ComponentRole::Collector,
                ComponentRole::Maintenance,
            ] {
                if other != role
                    && !(path.starts_with("parquet_writer.") && other == ComponentRole::Maintenance)
                    && tunables.environment_for(other).unwrap().contains_key(&key)
                {
                    errors.push(format!("{path}: {key} leaked to {other:?}"));
                }
            }
        }
    }
    assert!(errors.is_empty(), "projection mismatches:\n{}", errors.join("\n"));
}

#[test]
fn upload_settings_project_without_a_runtime_section() {
    for (upload, expected) in [
        (
            json!({"mode":"put_create"}),
            vec![("ARROWCOL_COLLECTOR_UPLOAD_MODE", "put_create")],
        ),
        (
            json!({"mode":"buffered_multipart", "buffer_capacity_bytes":1048576, "request_concurrency":3}),
            vec![
                ("ARROWCOL_COLLECTOR_UPLOAD_MODE", "buffered_multipart"),
                ("ARROWCOL_COLLECTOR_UPLOAD_BUFFER_CAPACITY_BYTES", "1048576"),
                ("ARROWCOL_COLLECTOR_UPLOAD_REQUEST_CONCURRENCY", "3"),
            ],
        ),
    ] {
        let tunables = Tunables::parse_oat_json(json!({"collector": {"upload": upload}})).unwrap();
        let env = tunables.environment_for(ComponentRole::Collector).unwrap();
        for (key, value) in expected {
            assert_eq!(env.get(key).map(String::as_str), Some(value), "{key}");
        }
    }
}

#[test]
fn ipc_aliases_have_the_same_destination_and_conflicts_are_rejected() {
    for input in [
        json!({"transport":{"ipc_compression":"zstd"}}),
        json!({"arrow_ipc":{"compression":"zstd"}}),
    ] {
        let tunables = Tunables::parse_oat_json(input).unwrap();
        assert_eq!(
            tunables
                .environment_for(ComponentRole::Client)
                .unwrap()
                .get("ARROWCOL_CLIENT_IPC_COMPRESSION")
                .map(String::as_str),
            Some("zstd")
        );
    }
    assert!(
        Tunables::parse_oat_json(json!({"transport":{"ipc_compression":"zstd"},"arrow_ipc":{"compression":"zstd"}}))
            .is_err()
    );
}

#[test]
fn maintenance_controls_and_writer_override_are_distinct() {
    let input = json!({"parquet_writer":{"max_row_group_rows":101}, "maintenance":{"enabled":true,"operations":["optimize"],"apply":true,"parquet_writer":{"max_row_group_rows":202}}});
    let tunables = Tunables::parse_oat_json(input).unwrap();
    let maintenance = tunables.environment_for(ComponentRole::Maintenance).unwrap();
    for key in [
        "ARROWCOL_MAINT_ENABLED",
        "ARROWCOL_MAINT_OPERATIONS",
        "ARROWCOL_MAINT_APPLY",
    ] {
        assert!(!maintenance.contains_key(key));
    }
    let collector = tunables.environment_for(ComponentRole::Collector).unwrap();
    for (env, expected) in [(&collector, 101), (&maintenance, 202)] {
        let writer: Value = serde_json::from_str(&env["ARROWCOL_PARQUET_CONFIG"]).unwrap();
        assert_eq!(writer["max_row_group_rows"], expected);
    }
    let readback: Value = serde_json::from_str(&tunables.canonical_json().unwrap()).unwrap();
    assert_eq!(readback.pointer("/maintenance/enabled"), Some(&json!(true)));
    assert_eq!(readback.pointer("/maintenance/operations"), Some(&json!(["optimize"])));
    assert_eq!(readback.pointer("/maintenance/apply"), Some(&json!(true)));
}

#[test]
fn collector_json_envelopes_preserve_nested_values() {
    let input = json!({"collector": {
        "batching": {"global_capacity": {"rows": 12000000}, "max_age": "2m"},
        "catalog": {"maximum_cache_age": "15s"},
        "commit": {"max_files_per_commit": 7},
        "exporter": {"authority_timeout": "12s"},
        "limits": {"batch_channel_capacity": 7},
        "pressure": {"heartbeat_interval": "3s"},
        "routing": {"command_timeout": "12s"}
    }});
    let settings = Tunables::parse_oat_json(input).unwrap();
    let env = settings.environment_for(ComponentRole::Collector).unwrap();
    for (key, pointer, expected) in [
        (
            "ARROWCOL_COLLECTOR_BATCHING_CONFIG",
            "/global_capacity/rows",
            json!(12000000),
        ),
        ("ARROWCOL_COLLECTOR_BATCHING_CONFIG", "/max_age", json!("2m")),
        ("ARROWCOL_COLLECTOR_CATALOG_CONFIG", "/maximum_cache_age", json!("15s")),
        ("ARROWCOL_COLLECTOR_COMMIT_CONFIG", "/max_files_per_commit", json!(7)),
        ("ARROWCOL_COLLECTOR_EXPORTER_CONFIG", "/authority_timeout", json!("12s")),
        ("ARROWCOL_COLLECTOR_LIMITS_CONFIG", "/batch_channel_capacity", json!(7)),
        ("ARROWCOL_COLLECTOR_PRESSURE_CONFIG", "/heartbeat_interval", json!("3s")),
        ("ARROWCOL_COLLECTOR_ROUTING_CONFIG", "/command_timeout", json!("12s")),
    ] {
        let envelope: Value = serde_json::from_str(&env[key]).unwrap();
        assert_eq!(envelope.pointer(pointer), Some(&expected), "{key}{pointer}");
    }
}

#[test]
fn disabled_maintenance_logging_is_accepted_and_inert() {
    let tunables = Tunables::parse_oat_json(json!({"logging":{"maintenance":"debug"}})).unwrap();
    assert!(
        !tunables
            .maintenance
            .as_ref()
            .is_some_and(|settings| settings.enabled == Some(true))
    );
    let readback: Value = serde_json::from_str(&tunables.canonical_json().unwrap()).unwrap();
    assert_eq!(readback.pointer("/logging/maintenance"), Some(&json!("debug")));
    // An environment can be prepared for this role, but the runner does not start it.
    assert_eq!(
        tunables
            .environment_for(ComponentRole::Maintenance)
            .unwrap()
            .get("RUST_LOG")
            .map(String::as_str),
        Some("debug")
    );
}
