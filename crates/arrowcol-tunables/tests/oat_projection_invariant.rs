//! Projection regression driven by the typed Rust input structs, not the capability catalog.
//! Each input is isolated apart from validation prerequisites. Env projections here are not
//! consumer startup readback evidence; runner execution is not exercised by this crate test.
//! Maintenance controls and disabled-role logging have explicit non-env classifications.

use anyhow::{Context, Result};
use arrowcol_tunables::{ComponentRole, Tunables};
use serde_json::{Value, json};
use std::time::Duration;

const TUNABLES_SOURCE: &str = include_str!("../src/lib.rs");
const WRITER_SOURCE: &str = include_str!("../../arrow-deltalake/src/writer.rs");

#[test]
fn delta_literal_properties_are_strict_and_match_initialization_bounds() -> Result<()> {
    use arrowcol_tunables::DeltaTunables;
    use std::collections::BTreeMap;

    let empty = Tunables::parse_oat_json(json!({}))?;
    assert!(empty.delta.is_none());
    assert!(serde_json::to_value(&empty)?.get("delta").is_none());
    let configured = Tunables::parse_oat_json(json!({"delta":{
        "target_file_size_bytes": 67_108_864, "checkpoint_interval": 17
    }}))?;
    let delta = configured.delta.as_ref().context("missing delta")?;
    assert_eq!(delta.target_file_size_bytes, Some(67_108_864));
    assert_eq!(delta.checkpoint_interval, Some(17));
    assert_eq!(configured.environment()?, empty.environment()?);

    for field in ["target_file_size_bytes", "checkpoint_interval"] {
        for bad in [
            json!(null),
            json!(0),
            json!(-1),
            json!(1.5),
            json!(1.0),
            json!("1"),
            json!(true),
        ] {
            let input = json!({"delta": {(field): bad}});
            let error = Tunables::parse_oat_json(input)
                .err()
                .context("accepted invalid Delta property")?;
            assert!(
                format!("{error:#}").contains(&format!("delta.{field}")),
                "wrong error path: {error:#}"
            );
        }
    }
    for input in [
        json!({"delta":null}),
        json!({"delta":"invalid"}),
        json!({"delta":{"unknown":1}}),
    ] {
        let error = Tunables::parse_oat_json(input)
            .err()
            .context("accepted invalid delta section")?;
        assert!(format!("{error:#}").contains("delta"), "wrong error path: {error:#}");
    }
    let error = Tunables::parse_oat_json(json!({"delta":{"unknown":1}}))
        .err()
        .context("accepted unknown field")?;
    assert!(format!("{error:#}").contains("delta.unknown"));

    for (field, property, values) in [
        (
            "target_file_size_bytes",
            "delta.targetFileSize",
            vec![0, 1, 67_108_864, 1_072_693_248, 1_072_693_249, u64::MAX],
        ),
        (
            "checkpoint_interval",
            "delta.checkpointInterval",
            vec![0, 1, 17, u64::MAX],
        ),
    ] {
        for value in values {
            let properties = BTreeMap::from([(property.to_owned(), value.to_string())]);
            let expected = arrowcol_props::validate_table_properties(&properties).is_ok();
            let actual = Tunables::parse_oat_json(json!({"delta": {(field): value}})).is_ok();
            assert_eq!(actual, expected, "initialization bound mismatch for {field}={value}");
        }
    }
    let direct = Tunables {
        delta: Some(DeltaTunables {
            target_file_size_bytes: Some(0),
            checkpoint_interval: None,
        }),
        ..Default::default()
    };
    assert!(
        format!(
            "{:#}",
            direct.validate().err().context("zero passed direct validation")?
        )
        .contains("delta.target_file_size_bytes")
    );
    Ok(())
}

#[test]
fn delta_literals_are_table_initialization_runner_controls() -> Result<()> {
    use arrowcol_tunables::{ApplyBoundary, capability_table};
    let settings = Tunables::parse_oat_json(json!({"delta":{
        "target_file_size_bytes": 67_108_864, "checkpoint_interval": 17
    }}))?;
    let evidence = settings.classify_oat_leaves()?;
    assert_eq!(
        evidence["runner_controls"],
        json!([
            {"path":"delta.checkpoint_interval", "reason":"delta_table_initialization"},
            {"path":"delta.target_file_size_bytes", "reason":"delta_table_initialization"}
        ])
    );
    assert_eq!(evidence["projection_only"], json!([]));
    for path in ["delta.target_file_size_bytes", "delta.checkpoint_interval"] {
        assert_eq!(
            capability_table()
                .iter()
                .find(|entry| entry.name == path)
                .context("missing delta capability")?
                .boundary,
            ApplyBoundary::ProcessStartup
        );
    }
    Ok(())
}

#[test]
fn connection_backoff_is_inert_only_when_retries_are_disabled() -> Result<()> {
    for (retries, expected) in [(None, true), (Some(0), true), (Some(2), false)] {
        let mut transport = json!({"retry_backoff_ms": 37});
        if let Some(retries) = retries {
            transport["connect_retries"] = json!(retries);
        }
        let tunables = Tunables::parse_oat_json(json!({"transport": transport}))?;
        let evidence = tunables.classify_oat_leaves()?;
        let inert = evidence["inert_fields"].as_array().context("missing inert fields")?;
        assert_eq!(
            inert.iter().any(|entry| entry["path"] == "transport.retry_backoff_ms"
                && entry["reason"] == "connection_retries_disabled"),
            expected
        );
        assert_eq!(
            evidence["projection_only"]
                .as_array()
                .context("missing projected fields")?
                .iter()
                .any(|entry| entry["path"] == "transport.retry_backoff_ms"),
            !expected
        );
    }
    Ok(())
}

#[test]
fn http2_options_are_inert_only_when_their_feature_is_disabled_or_overridden() -> Result<()> {
    for (adaptive, interval) in [(true, 0), (true, 10_000), (false, 0), (false, 10_000)] {
        let tunables = Tunables::parse_oat_json(json!({"transport": {
            "adaptive_window": adaptive, "stream_window_bytes": 1_048_576,
            "connection_window_bytes": 16_777_216, "keepalive_interval_ms": interval,
            "keepalive_timeout_ms": 2000, "keepalive_while_idle": false
        }}))?;
        let evidence = tunables.classify_oat_leaves()?;
        for (path, reason) in [
            ("transport.stream_window_bytes", "adaptive_http2_window"),
            ("transport.connection_window_bytes", "adaptive_http2_window"),
            ("transport.keepalive_timeout_ms", "http2_keepalive_disabled"),
            ("transport.keepalive_while_idle", "http2_keepalive_disabled"),
        ] {
            let expected_inert = if reason == "adaptive_http2_window" {
                adaptive
            } else {
                interval == 0
            };
            assert_eq!(
                evidence["inert_fields"]
                    .as_array()
                    .context("missing inert fields")?
                    .iter()
                    .any(|entry| entry["path"] == path && entry["reason"] == reason),
                expected_inert
            );
            assert_eq!(
                evidence["projection_only"]
                    .as_array()
                    .context("missing projected fields")?
                    .iter()
                    .any(|entry| entry["path"] == path),
                !expected_inert
            );
        }
    }
    Ok(())
}

#[test]
fn tcp_keepalive_options_follow_the_enable_setting() -> Result<()> {
    for (keepalive, expected_inert) in [(0, true), (60_000, false)] {
        let tunables = Tunables::parse_oat_json(json!({"transport": {
            "tcp_keepalive_ms": keepalive, "tcp_keepalive_interval_ms": 500, "tcp_keepalive_retries": 3
        }}))?;
        let evidence = tunables.classify_oat_leaves()?;
        for path in ["transport.tcp_keepalive_interval_ms", "transport.tcp_keepalive_retries"] {
            assert_eq!(
                evidence["inert_fields"]
                    .as_array()
                    .context("missing inert fields")?
                    .iter()
                    .any(|entry| entry["path"] == path && entry["reason"] == "tcp_keepalive_disabled"),
                expected_inert
            );
            assert_eq!(
                evidence["projection_only"]
                    .as_array()
                    .context("missing projected fields")?
                    .iter()
                    .any(|entry| entry["path"] == path),
                !expected_inert
            );
        }
    }
    Ok(())
}

#[test]
fn stream_backoff_is_inert_when_no_retry_is_allowed() -> Result<()> {
    for (attempts, inert) in [(1, true), (2, false)] {
        let tunables = Tunables::parse_oat_json(json!({"client":{
            "stream_retry_max_attempts":attempts,
            "stream_retry_base_backoff_ms":13,
            "stream_retry_max_backoff_ms":300
        }}))?;
        let evidence = tunables.classify_oat_leaves()?;
        for path in [
            "client.stream_retry_base_backoff_ms",
            "client.stream_retry_max_backoff_ms",
        ] {
            assert_eq!(
                evidence["inert_fields"]
                    .as_array()
                    .context("missing inert fields")?
                    .iter()
                    .any(|entry| entry["path"] == path && entry["reason"] == "stream_retries_disabled"),
                inert
            );
            assert_eq!(
                evidence["projection_only"]
                    .as_array()
                    .context("missing projected fields")?
                    .iter()
                    .any(|entry| entry["path"] == path),
                !inert
            );
        }
    }
    Ok(())
}

#[test]
fn rate_limit_period_is_strict_and_inert_when_disabled() -> Result<()> {
    let unchanged = Tunables::parse_oat_json(json!({"transport":{"rate_limit_rps": 2}}))?;
    assert!(
        serde_json::to_value(unchanged)?["transport"]
            .get("rate_limit_period_ms")
            .is_none()
    );
    for bad in [json!(0), json!(86_400_001), json!(-1), json!(1.5), json!("1000")] {
        assert!(Tunables::parse_oat_json(json!({"transport":{"rate_limit_period_ms":bad}})).is_err());
    }
    for (rps, inert) in [(0, true), (2, false)] {
        let tunables =
            Tunables::parse_oat_json(json!({"transport":{"rate_limit_rps":rps,"rate_limit_period_ms":250}}))?;
        assert_eq!(
            tunables.environment_for(ComponentRole::Client)?["ARROWCOL_CLIENT_RATE_LIMIT_PERIOD_MS"],
            "250"
        );
        let evidence = tunables.classify_oat_leaves()?;
        assert_eq!(
            evidence["inert_fields"]
                .as_array()
                .context("missing inert fields")?
                .iter()
                .any(|entry| entry["path"] == "transport.rate_limit_period_ms"
                    && entry["reason"] == "rate_limit_disabled"),
            inert
        );
    }
    Ok(())
}

// The scanner deliberately fails on unfamiliar field syntax rather than silently skipping it.
// Vecs of scalar values and sorting_columns are treated as atomic values. The latter's
// element schema is guarded separately below.
fn struct_fields(name: &str) -> Result<Option<Vec<(String, String)>>> {
    let declaration = format!("pub struct {name} {{");
    let Some(source) = [TUNABLES_SOURCE, WRITER_SOURCE]
        .into_iter()
        .find(|source| source.lines().any(|line| line.trim() == declaration))
    else {
        return Ok(None);
    };
    let mut lines = source.lines().skip_while(|line| line.trim() != declaration).skip(1);
    let mut fields = Vec::new();
    let mut closed = false;
    for line in &mut lines {
        let line = line.trim();
        if line == "}" {
            closed = true;
            break;
        }
        if let Some(field) = line.strip_prefix("pub ") {
            let (field_name, field_type) = field
                .split_once(':')
                .with_context(|| format!("unrecognized public field in {name}: {line}"))?;
            let field_type = field_type
                .trim()
                .strip_suffix(',')
                .with_context(|| format!("multiline field type requires scanner update in {name}: {line}"))?;
            assert!(
                !field_name.is_empty() && !field_type.is_empty(),
                "invalid field in {name}"
            );
            fields.push((field_name.to_owned(), field_type.to_owned()));
        } else if line.starts_with("pub(")
            || line.split_once(':').is_some_and(|(name, _)| {
                !name.is_empty() && name.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            })
        {
            panic!("non-public or unfamiliar field syntax in {name}: {line}");
        }
    }
    assert!(closed && !fields.is_empty(), "cannot scan {name}");
    Ok(Some(fields))
}

fn collect_leaves(type_name: &str, prefix: &str, leaves: &mut Vec<String>) -> Result<()> {
    for (field, field_type) in
        struct_fields(type_name)?.with_context(|| format!("missing typed input struct {type_name}"))?
    {
        let path = if prefix.is_empty() {
            field
        } else {
            format!("{prefix}.{field}")
        };
        let inner = field_type
            .strip_prefix("Option<")
            .and_then(|s| s.strip_suffix('>'))
            .unwrap_or(&field_type);
        if inner == "BTreeMap<String, ColumnOptions>" {
            collect_leaves("ColumnOptions", &format!("{path}.*"), leaves)?;
        } else if struct_fields(inner)?.is_some() {
            collect_leaves(inner, &path, leaves)?;
        } else {
            leaves.push(path);
        }
    }
    Ok(())
}

fn typed_leaves() -> Result<Vec<String>> {
    let mut leaves = Vec::new();
    collect_leaves("Tunables", "", &mut leaves)?;
    Ok(leaves)
}

#[derive(Debug, PartialEq, Eq)]
enum LeafClass {
    Environment,
    RunnerControl,
    EnvironmentOrInertDisabledRole,
    EnvironmentOrInheritedFilter,
}

fn classify(path: &str) -> LeafClass {
    match path {
        "collector.allocator"
        | "collector.lto"
        | "collector.codegen_units"
        | "collector.opt_level"
        | "collector.target_cpu" => LeafClass::RunnerControl,
        "delta.target_file_size_bytes" | "delta.checkpoint_interval" => LeafClass::RunnerControl,
        "client.build_profile.lto"
        | "client.build_profile.codegen_units"
        | "client.build_profile.opt_level"
        | "client.build_profile.target_cpu" => LeafClass::RunnerControl,
        // The OAT runner uses these to decide whether to start maintenance and its arguments.
        "maintenance.enabled" | "maintenance.operations" | "maintenance.apply" => LeafClass::RunnerControl,
        // Maintenance logging is inert when the role is disabled.
        "logging.maintenance" => LeafClass::EnvironmentOrInertDisabledRole,
        // Router inherit deliberately leaves the underlying YAML filter unchanged.
        "logging.router" => LeafClass::EnvironmentOrInheritedFilter,
        _ => match path.split_once('.').map_or(path, |(section, _)| section) {
            "client" | "transport" | "arrow_ipc" | "router" | "collector" | "parquet_writer" | "wal"
            | "maintenance" | "logging" => LeafClass::Environment,
            section => panic!("unclassified typed section {section}"),
        },
    }
}

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
    if path.starts_with("maintenance.") || path == "logging.maintenance" {
        input["maintenance"] =
            json!({"enabled": true, "operations": ["optimize", "purge", "vacuum", "checkpoint", "prune_log"]});
    }
    if matches!(path, "collector.batching.stage_file_concurrency" | "collector.batching.stage_files_per_group") {
        input["collector"] = json!({"batching":{"coalesce_publications":true}});
    }
    if path == "collector.runtime.parquet_encode_mode" {
        input["collector"] = json!({"runtime":{"parquet_column_workers":4}});
    }
    if path == "router.sql_pool_acquire_timeout_ms" {
        input["router"] = json!({"sql_pool_max_size": 1});
    }
    if path == "client.identity_builder" {
        input["client"] = json!({"data_profile": "realistic"});
    }
    if matches!(
        path,
        "collector.upload.buffer_capacity_bytes"
            | "collector.upload.request_concurrency"
            | "collector.upload.staging_mode"
    ) {
        input["collector"] = json!({"upload": {"mode": "buffered_multipart"}});
    }
    if matches!(
        path,
        "collector.layout.sort_options" | "collector.layout.sort_execution"
    ) {
        input["collector"] = json!({"layout":{"enabled":true,"sort_columns":["customer_id"]}});
    }
    if path == "maintenance.layout_sort_options" {
        input["maintenance"]["layout_enabled"] = json!(true);
        input["maintenance"]["layout_sort_columns"] = json!(["customer_id"]);
    }
    if matches!(
        path,
        "maintenance.optimize_upload_buffer_bytes" | "maintenance.optimize_upload_concurrency"
    ) {
        input["maintenance"]["optimize_upload_mode"] = json!("buffered_multipart");
    }
    if path == "collector.layout.sort_options" {
        set(
            &mut input,
            &["collector", "layout", "sort_columns"],
            json!(["customer_id"]),
        );
    }
    if path == "maintenance.layout_sort_options" {
        set(&mut input, &["maintenance", "layout_enabled"], json!(true));
        set(
            &mut input,
            &["maintenance", "layout_sort_columns"],
            json!(["customer_id"]),
        );
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
    match path {
        "collector.upload.staging_mode" => return vec![json!("streaming")],
        "collector.allocator" => return vec![json!("system")],
        "collector.lto" => return vec![json!("off")],
        "collector.codegen_units" => return vec![json!(16)],
        "collector.opt_level" => return vec![json!(3)],
        "collector.target_cpu" => return vec![json!("generic")],
        "client.build_profile.lto" => return vec![json!("off")],
        "client.build_profile.codegen_units" => return vec![json!(16)],
        "client.build_profile.opt_level" => return vec![json!(3)],
        "client.build_profile.target_cpu" => return vec![json!("generic")],
        "collector.commit.txn_lookup_mode" => return vec![json!("batched_cached"), json!("per_id"), json!("batched")],
        "collector.runtime.cpu_dispatch" => return vec![json!("bounded_shards")],
        "collector.runtime.parquet_encode_mode" => return vec![json!("column_pipeline"), json!("column_waves")],
        "collector.runtime.cpu_placement" => return vec![json!("scatter_allowed")],
        "collector.batching.stage_file_concurrency" => return vec![json!(16)],
        "collector.batching.stage_files_per_group" => return vec![json!(16)],
        _ => {}
    }
    if path == "collector.commit.delta_stats_columns" {
        return vec![json!(["customer_id", "timestamp"])];
    }
    if path == "collector.runtime.timestamp_kernel" {
        return vec![json!("fused_checked")];
    }
    if path == "collector.layout.sort_execution" {
        return vec![json!("verify_order")];
    }
    if matches!(
        path,
        "collector.layout.sort_options" | "maintenance.layout_sort_options"
    ) {
        return vec![json!([{"column":"customer_id","direction":"descending","nulls":"first"}])];
    }
    if path == "collector.runtime.generated_partition_binding" {
        return vec![json!("proven_constant")];
    }
    if path == "arrow_ipc.alignment" {
        return vec![json!(8)];
    }
    if path == "collector.layout.sort_options" || path == "maintenance.layout_sort_options" {
        return vec![json!([{"column":"customer_id","direction":"descending","nulls":"first"}])];
    }
    if path == "collector.pressure.rss_limit_bytes" {
        return vec![json!(4_294_967_296_u64)];
    }
    if path.ends_with("sorting_columns") {
        return vec![json!([{ "path": "customer_id", "descending": false, "nulls_first": false }])];
    }
    if path.ends_with("sort_options") {
        return vec![json!([{
            "column": "customer_id",
            "direction": "descending",
            "nulls": "last"
        }])];
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
        json!(4_294_967_296_u64),
        json!(true),
        json!(false),
        json!("1s"),
        json!("10s"),
        json!("customer_id"),
        json!("shared"),
        json!("diagnostic"),
        json!("clone_vec"),
        json!("formatted_rows"),
        json!("fixed"),
        json!("cardinality"),
        json!("none"),
        json!("hydrate"),
        json!("drain_ready"),
        json!("typed_runs"),
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

fn destination(path: &str, value: &Value) -> Result<(ComponentRole, String, Option<String>, String)> {
    if path == "collector.commit.delta_stats_columns" {
        return Ok((
            ComponentRole::Collector,
            "ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS".to_owned(),
            None,
            value.to_string(),
        ));
    }
    let parts: Vec<_> = path.split('.').collect();
    let (role, key, pointer) = match parts[0] {
        "client" | "transport" => (
            ComponentRole::Client,
            format!("ARROWCOL_CLIENT_{}", parts[1].to_ascii_uppercase()),
            None,
        ),
        "arrow_ipc" => (
            ComponentRole::Client,
            format!("ARROWCOL_CLIENT_IPC_{}", parts[1].to_ascii_uppercase()),
            None,
        ),
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
                "batching", "catalog", "commit", "exporter", "limits", "pressure", "routing", "socket",
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
                if ["sort_columns", "sort_options", "bloom_columns"].contains(&parts[2]) {
                    ""
                } else {
                    "LAYOUT_"
                },
                parts[2].to_ascii_uppercase()
            ),
            None,
        ),
        "collector" if parts[1] == "channel_capacity" => (
            ComponentRole::Collector,
            format!("ARROWCOL_COLLECTOR_CHANNEL_{}", parts[2].to_ascii_uppercase()),
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
        "maintenance" if parts[1] == "parquet_writer" => (
            ComponentRole::Maintenance,
            "ARROWCOL_PARQUET_CONFIG".into(),
            Some(format!("/{}", parts[2..].join("/").replace('*', "customer_id"))),
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
                    return Ok((
                        ComponentRole::Collector,
                        format!("ARROWCOL_WAL_{}", other.to_ascii_uppercase()),
                        None,
                        scalar(value),
                    ));
                }
            };
            (ComponentRole::Collector, format!("ARROWCOL_WAL_{suffix}"), None)
        }
        _ => panic!("unknown typed section {path}"),
    };
    let expected = if path == "collector.layout.sort_options" || path == "maintenance.layout_sort_options" {
        "customer_id:descending:first".to_owned()
    } else if path == "logging.router" {
        format!("{},tiberius=error", scalar(value))
    } else if matches!(
        path,
        "collector.layout.sort_options" | "maintenance.layout_sort_options"
    ) {
        value
            .as_array()
            .context("sort options array")?
            .iter()
            .map(|key| {
                Ok(format!(
                    "{}:{}:{}",
                    key["column"].as_str().context("column")?,
                    key["direction"].as_str().context("direction")?,
                    key["nulls"].as_str().context("nulls")?
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(",")
    } else if path == "wal.retention_size_cap_bytes" {
        format!(
            "{} B",
            value
                .as_u64()
                .with_context(|| format!("{path}: expected an unsigned integer, got {value}"))?
        )
    } else if parts[0] == "wal" && (parts[1].ends_with("_ms") || parts[1] == "max_age_secs") {
        let amount = value
            .as_u64()
            .with_context(|| format!("{path}: expected an unsigned integer, got {value}"))?;
        humantime::format_duration(if parts[1] == "max_age_secs" {
            Duration::from_secs(amount)
        } else {
            Duration::from_millis(amount)
        })
        .to_string()
    } else if path.ends_with("sort_options") {
        value
            .as_array()
            .context("sort options must be an array")?
            .iter()
            .map(|option| {
                Ok(format!(
                    "{}:{}:{}",
                    option["column"].as_str().context("sort option column missing")?,
                    option["direction"].as_str().context("sort option direction missing")?,
                    option["nulls"].as_str().context("sort option null placement missing")?
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(",")
    } else {
        scalar(value)
    };
    Ok((role, key, pointer, expected))
}

#[test]
fn duration_run_classifies_one_shot_max_rows_as_inert() -> Result<()> {
    let tunables = Tunables::parse_oat_json(json!({"client":{"max_rows":129}}))?;
    let evidence = tunables.classify_oat_leaves()?;
    assert_eq!(
        evidence["inert_fields"],
        json!([{
            "path": "client.max_rows", "reason": "duration_based_run"
        }])
    );
    assert_eq!(evidence["projection_only"], json!([]));
    Ok(())
}

#[test]
fn cpu_modes_and_stage_concurrency_require_typed_named_values() -> Result<()> {
    for (path, invalid) in [
        (
            "collector.runtime.cpu_dispatch",
            vec![
                Value::Null,
                json!(true),
                json!("bounded"),
                json!({"bounded_shards":null}),
            ],
        ),
        (
            "collector.runtime.cpu_placement",
            vec![Value::Null, json!(1), json!("compact"), json!({"os":null})],
        ),
        (
            "collector.batching.stage_file_concurrency",
            vec![Value::Null, json!("1"), json!(1.5), json!(-1), json!(0), json!(17)],
        ),
    ] {
        for bad in invalid {
            let error = Tunables::parse_oat_json(fixture(path, bad))
                .err()
                .context("invalid value accepted")?;
            assert!(format!("{error:#}").contains(path), "{error:#}");
        }
    }
    let empty = Tunables::parse_oat_json(json!({"collector":{"runtime":{},"batching":{}}}))?;
    let environment = empty.environment_for(ComponentRole::Collector)?;
    assert!(!environment.contains_key("ARROWCOL_COLLECTOR_CPU_DISPATCH"));
    assert!(!environment.contains_key("ARROWCOL_COLLECTOR_CPU_PLACEMENT"));
    assert!(!environment.contains_key("ARROWCOL_COLLECTOR_BATCHING_CONFIG"));
    for batching in [
        json!({"stage_file_concurrency":2}),
        json!({"stage_file_concurrency":16,"coalesce_publications":false}),
    ] {
        assert!(Tunables::parse_oat_json(json!({"collector":{"batching":batching}})).is_err());
    }
    for (dispatch, placement, concurrency) in [
        ("shared_fifo", "os", 1),
        ("bounded_shards", "compact_allowed", 8),
        ("bounded_shards", "scatter_allowed", 16),
        ("bounded_shards", "physical_first", 4),
    ] {
        let input = json!({"collector":{"runtime":{"cpu_dispatch":dispatch,"cpu_placement":placement},
                                       "batching":{"coalesce_publications":true,"stage_file_concurrency":concurrency}}});
        let env = Tunables::parse_oat_json(input)?.environment_for(ComponentRole::Collector)?;
        assert_eq!(env["ARROWCOL_COLLECTOR_CPU_DISPATCH"], dispatch);
        assert_eq!(env["ARROWCOL_COLLECTOR_CPU_PLACEMENT"], placement);
        assert_eq!(
            serde_json::from_str::<Value>(&env["ARROWCOL_COLLECTOR_BATCHING_CONFIG"])?,
            json!({"coalesce_publications":true, "stage_file_concurrency": concurrency})
        );
    }
    Ok(())
}

#[test]
fn generated_partition_binding_requires_a_named_string_mode() -> Result<()> {
    let empty = Tunables::parse_oat_json(json!({"collector":{"runtime":{}}}))?;
    assert!(
        !empty
            .environment_for(ComponentRole::Collector)?
            .contains_key("ARROWCOL_COLLECTOR_GENERATED_PARTITION_BINDING")
    );
    assert!(
        serde_json::to_value(empty)?["collector"]["runtime"]
            .get("generated_partition_binding")
            .is_none()
    );
    for bad in [
        Value::Null,
        json!(true),
        json!(1),
        json!(""),
        json!("PROVEN_CONSTANT"),
        json!("proven_constant "),
        json!({"proven_constant":null}),
    ] {
        let error = Tunables::parse_oat_json(fixture("collector.runtime.generated_partition_binding", bad))
            .err()
            .context("invalid binding unexpectedly accepted")?;
        assert!(format!("{error:#}").contains("collector.runtime.generated_partition_binding"));
    }
    Ok(())
}

#[test]
fn shutdown_timeout_is_inert_for_the_captured_measurement() -> Result<()> {
    let tunables = Tunables::parse_oat_json(json!({
        "collector":{"shutdown_drain_timeout_seconds":17}
    }))?;
    let evidence = tunables.classify_oat_leaves()?;
    assert_eq!(
        evidence["inert_fields"],
        json!([{
            "path": "collector.shutdown_drain_timeout_seconds",
            "reason": "collector_shutdown_after_measurement"
        }])
    );
    Ok(())
}

#[test]
fn typed_oat_leaves_have_a_classification_and_reach_their_role() -> Result<()> {
    let mut errors = Vec::new();
    for path in typed_leaves()? {
        let path = path.as_str();
        match classify(path) {
            LeafClass::RunnerControl => continue,
            LeafClass::Environment
            | LeafClass::EnvironmentOrInertDisabledRole
            | LeafClass::EnvironmentOrInheritedFilter => {}
        }
        let Some((sample, tunables)) = candidates(path).into_iter().find_map(|value| {
            Tunables::parse_oat_json(fixture(path, value.clone()))
                .ok()
                .map(|settings| (value, settings))
        }) else {
            errors.push(format!("{path}: no accepted candidate"));
            continue;
        };
        let (role, key, pointer, expected) = destination(path, &sample)?;
        let env = tunables
            .environment_for(role)
            .with_context(|| format!("{path}: project {role:?}"))?;
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
                    && tunables
                        .environment_for(other)
                        .with_context(|| format!("{path}: project {other:?}"))?
                        .contains_key(&key)
                {
                    errors.push(format!("{path}: {key} leaked to {other:?}"));
                }
            }
        }
    }
    assert!(errors.is_empty(), "projection mismatches:\n{}", errors.join("\n"));
    Ok(())
}

#[test]
fn accepted_typed_leaves_have_one_evidence_classification() -> Result<()> {
    let mut errors = Vec::new();
    for path in typed_leaves()? {
        let mut values = candidates(&path);
        if path == "maintenance.operations" {
            values.push(json!(["optimize"]));
        }
        let Some(input) = values
            .into_iter()
            .map(|value| fixture(&path, value))
            .find(|input| Tunables::parse_oat_json(input.clone()).is_ok())
        else {
            errors.push(format!("{path}: no accepted candidate"));
            continue;
        };
        let evidence = Tunables::parse_oat_json(input)?
            .classify_oat_leaves()
            .with_context(|| format!("{path}: classify accepted input"))?;
        let concrete = path.replace('*', "customer_id");
        let mut count = 0;
        for key in [
            "inert_fields",
            "projection_only",
            "runner_controls",
            "consumer_verified",
        ] {
            count += evidence[key]
                .as_array()
                .context("classification array missing")?
                .iter()
                .filter(|item| item["path"] == concrete)
                .count();
        }
        if count != 1 {
            errors.push(format!("{path}: classified {count} times"));
        }
    }
    assert!(errors.is_empty(), "classification mismatches:\n{}", errors.join("\n"));
    Ok(())
}

#[test]
fn evidence_classification_uses_only_requested_non_null_leaves() -> Result<()> {
    let input = json!({
        "client": {"interval_us": 10, "max_rows": null, "build_profile": {
            "lto": "thin-local", "codegen_units": 16, "opt_level": 3, "target_cpu": "generic"
        }},
        "collector": {"upload": {"mode": "buffered_multipart", "request_concurrency": 2}},
        "logging": {"maintenance": "warn", "router": "inherit"},
        "maintenance": {"enabled": false},
        "parquet_writer": {"defaults": {"compression": "gzip", "compression_level": 5,
            "dictionary_enabled": false, "bloom_filter_fpp": 0.1}}
    });
    let evidence = Tunables::parse_oat_json(input)?.classify_oat_leaves()?;
    assert_eq!(
        evidence["inert_fields"],
        json!([{
            "path": "logging.maintenance", "reason": "maintenance_disabled"
        }])
    );
    let projected = evidence["projection_only"]
        .as_array()
        .context("projection_only missing")?;
    assert!(projected.iter().any(|entry| {
        entry
            == &json!({"path":"parquet_writer.defaults.bloom_filter_fpp",
            "reason":"consumer_startup_proof_unverified"})
    }));
    assert!(projected.iter().any(|entry| {
        entry
            == &json!({"path":"collector.upload.request_concurrency",
            "reason":"consumer_startup_proof_unverified"})
    }));
    assert!(!evidence.to_string().contains("client.max_rows"));
    let controls = evidence["runner_controls"]
        .as_array()
        .context("runner_controls missing")?;
    for field in ["lto", "codegen_units", "opt_level", "target_cpu"] {
        assert!(
            controls
                .iter()
                .any(|entry| entry["path"] == format!("client.build_profile.{field}")
                    && entry["reason"] == "rebuild_only_oat_client_profile")
        );
    }
    assert_eq!(
        Tunables::parse_oat_json(json!({}))?.classify_oat_leaves()?,
        json!({
            "inert_fields": [], "projection_only": [], "runner_controls": [], "consumer_verified": []
        })
    );
    assert_eq!(
        Tunables::parse_oat_json(json!({"parquet_writer": {}}))?.classify_oat_leaves()?,
        json!({
            "inert_fields": [], "projection_only": [], "runner_controls": [], "consumer_verified": []
        })
    );
    assert!(
        Tunables::parse_oat_json(json!({
            "collector": {"upload": {"mode":"put_create", "request_concurrency":2}}
        }))
        .is_err()
    );
    Ok(())
}

#[test]
fn disabled_maintenance_controls_are_classified_without_a_worker() -> Result<()> {
    let tunables = Tunables::parse_oat_json(json!({
        "maintenance": {"enabled":false, "apply":false, "operations":[]},
        "logging": {"maintenance":"warn"}
    }))?;
    let evidence = tunables.classify_oat_leaves()?;
    assert_eq!(
        evidence["inert_fields"],
        json!([
            {"path":"logging.maintenance", "reason":"maintenance_disabled"},
            {"path":"maintenance.apply", "reason":"maintenance_disabled"},
            {"path":"maintenance.operations", "reason":"maintenance_disabled"}
        ])
    );
    assert_eq!(
        evidence["runner_controls"],
        json!([
            {"path":"maintenance.enabled", "reason":"maintenance_runner_control"}
        ])
    );
    let disabled = Tunables::parse_oat_json(json!({
        "maintenance":{"enabled":false,"max_cycles":1,"granularity":"hour"}
    }))?;
    assert_eq!(
        disabled.classify_oat_leaves()?["inert_fields"],
        json!([
            {"path":"maintenance.granularity", "reason":"maintenance_disabled"},
            {"path":"maintenance.max_cycles", "reason":"maintenance_disabled"}
        ])
    );
    assert!(disabled.environment_for(ComponentRole::Maintenance)?.is_empty());
    assert!(
        !disabled
            .environment()?
            .keys()
            .any(|key| key.starts_with("ARROWCOL_MAINT_"))
    );
    assert!(
        Tunables::parse_oat_json(json!({
            "maintenance":{"enabled":false,"max_cycles":0}
        }))
        .is_err()
    );
    assert!(
        Tunables::parse_oat_json(json!({
            "maintenance":{"enabled":true,"max_cycles":1}
        }))
        .is_err()
    );
    Ok(())
}

#[test]
fn disabled_column_dictionary_makes_its_page_limit_inert() -> Result<()> {
    let tunables = Tunables::parse_oat_json(json!({
        "parquet_writer": {"columns": {"customer_id": {
            "dictionary_enabled": false, "dictionary_page_size_limit": 4096
        }}}
    }))?;
    assert_eq!(
        tunables.classify_oat_leaves()?["inert_fields"],
        json!([
            {"path": "parquet_writer.columns.customer_id.dictionary_page_size_limit", "reason": "dictionary_disabled"}
        ])
    );
    Ok(())
}

#[test]
fn dictionary_page_limits_follow_global_and_column_overrides() -> Result<()> {
    for (enabled_override, default_inert) in [(false, true), (true, false)] {
        let tunables = Tunables::parse_oat_json(json!({
            "parquet_writer": {
                "defaults":{"dictionary_enabled":false,"dictionary_page_size_limit":4096},
                "columns":{
                    "inherited":{"dictionary_page_size_limit":1024},
                    "override":{"dictionary_enabled":enabled_override,"dictionary_page_size_limit":2048}
                }
            }
        }))?;
        let evidence = tunables.classify_oat_leaves()?;
        let inert = evidence["inert_fields"].as_array().context("missing inert fields")?;
        let has = |path: &str| {
            inert
                .iter()
                .any(|entry| entry["path"] == path && entry["reason"] == "dictionary_disabled")
        };
        assert_eq!(has("parquet_writer.defaults.dictionary_page_size_limit"), default_inert);
        assert!(has("parquet_writer.columns.inherited.dictionary_page_size_limit"));
        assert_eq!(
            has("parquet_writer.columns.override.dictionary_page_size_limit"),
            !enabled_override
        );
    }
    Ok(())
}

#[test]
fn typed_serialization_null_defaults_do_not_hide_or_invent_requested_leaves() -> Result<()> {
    let tunables = Tunables::parse_oat_json(json!({
        "client":{"interval_us":0,"stream_retry_max_attempts":1},
        "collector":{"runtime":{"cpu_workers":1}}
    }))?;
    let evidence = tunables.classify_oat_leaves()?;
    assert_eq!(evidence["inert_fields"], json!([]));
    assert_eq!(evidence["runner_controls"], json!([]));
    assert_eq!(
        evidence["projection_only"],
        json!([
            {"path":"client.interval_us", "reason":"consumer_startup_proof_unverified"},
            {"path":"client.stream_retry_max_attempts", "reason":"consumer_startup_proof_unverified"},
            {"path":"collector.runtime.cpu_workers", "reason":"consumer_startup_proof_unverified"}
        ])
    );
    Ok(())
}

#[test]
fn partition_grouping_only_accepts_explicit_enum_strings() -> Result<()> {
    for invalid in [
        Value::Null,
        json!(true),
        json!(1),
        json!({"typed_runs":null}),
        json!("TypedRuns"),
        json!("auto"),
    ] {
        let error = Tunables::parse_oat_json(json!({"collector":{"runtime":{"partition_grouping":invalid}}}))
            .err()
            .context("invalid partition_grouping unexpectedly parsed")?;
        assert!(
            format!("{error:#}").contains("collector.runtime.partition_grouping"),
            "{error:#}"
        );
    }
    for mode in ["scalar_map", "typed_runs"] {
        let tunables = Tunables::parse_oat_json(json!({"collector":{"runtime":{"partition_grouping":mode}}}))?;
        assert_eq!(
            tunables.environment_for(ComponentRole::Collector)?["ARROWCOL_COLLECTOR_PARTITION_GROUPING"],
            mode
        );
    }
    Ok(())
}

#[test]
fn streaming_staging_requires_a_named_mode_and_compatible_writer() {
    for invalid in [Value::Null, json!(true), json!("Streaming"), json!({"streaming":null})] {
        assert!(Tunables::parse_oat_json(fixture("collector.upload.staging_mode", invalid)).is_err());
    }
    for invalid in [
        json!({"upload":{"staging_mode":"streaming"}}),
        json!({"upload":{"staging_mode":"streaming","mode":"buffered_multipart"},"runtime":{"parquet_column_workers":2}}),
    ] {
        assert!(Tunables::parse_oat_json(json!({"collector":invalid})).is_err());
    }
}

#[test]
fn upload_settings_project_without_a_runtime_section() -> Result<()> {
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
        let input = json!({"collector": {"upload": upload}});
        let tunables =
            Tunables::parse_oat_json(input.clone()).with_context(|| format!("parse upload fixture {input}"))?;
        let env = tunables
            .environment_for(ComponentRole::Collector)
            .context("project upload fixture")?;
        for (key, value) in expected {
            assert_eq!(env.get(key).map(String::as_str), Some(value), "{key}");
        }

        #[test]
        fn column_pipeline_and_file_splits_are_strict_and_projected() -> Result<()> {
            for invalid in [Value::Null, json!(true), json!(1), json!("ColumnPipeline"), json!("pipeline")] {
                assert!(Tunables::parse_oat_json(fixture("collector.runtime.parquet_encode_mode", invalid)).is_err());
            }
            for workers in [Value::Null, json!(0), json!(1), json!(17)] {
                assert!(Tunables::parse_oat_json(json!({"collector":{"runtime":{
                    "parquet_encode_mode":"column_pipeline", "parquet_column_workers":workers
                }}})).is_err());
            }
            for invalid in [Value::Null, json!("4"), json!(1.5), json!(0), json!(17)] {
                assert!(Tunables::parse_oat_json(fixture("collector.batching.stage_files_per_group", invalid)).is_err());
            }
            assert!(Tunables::parse_oat_json(json!({"collector":{"batching":{"stage_files_per_group":2}}})).is_err());
            assert!(Tunables::parse_oat_json(json!({"collector":{
                "runtime":{"parquet_encode_mode":"column_pipeline","parquet_column_workers":4}
            },"parquet_writer":{"max_row_group_bytes":2048}})).is_err());
            let tunables = Tunables::parse_oat_json(json!({"collector":{
                "runtime":{"parquet_encode_mode":"column_pipeline","parquet_column_workers":4},
                "batching":{"coalesce_publications":true,"stage_files_per_group":4,"stage_file_concurrency":2},
                "upload":{"mode":"buffered_multipart","staging_mode":"streaming"}
            }}))?;
            let env = tunables.environment_for(ComponentRole::Collector)?;
            assert_eq!(env["ARROWCOL_COLLECTOR_PARQUET_ENCODE_MODE"], "column_pipeline");
            let batching: Value = serde_json::from_str(&env["ARROWCOL_COLLECTOR_BATCHING_CONFIG"])?;
            assert_eq!(batching["stage_files_per_group"], 4);
            assert_eq!(batching["stage_file_concurrency"], 2);
            Ok(())
        }
    }
    Ok(())
}

#[test]
fn ipc_aliases_have_the_same_destination_and_conflicts_are_rejected() -> Result<()> {
    for input in [
        json!({"transport":{"ipc_compression":"zstd"}}),
        json!({"arrow_ipc":{"compression":"zstd"}}),
    ] {
        let tunables =
            Tunables::parse_oat_json(input.clone()).with_context(|| format!("parse IPC alias fixture {input}"))?;
        assert_eq!(
            tunables
                .environment_for(ComponentRole::Client)
                .context("project IPC alias")?
                .get("ARROWCOL_CLIENT_IPC_COMPRESSION")
                .map(String::as_str),
            Some("zstd")
        );
    }
    assert!(
        Tunables::parse_oat_json(json!({"transport":{"ipc_compression":"zstd"},"arrow_ipc":{"compression":"zstd"}}))
            .is_err()
    );
    Ok(())
}

#[test]
fn maintenance_controls_and_writer_override_are_distinct() -> Result<()> {
    let input = json!({"parquet_writer":{"max_row_group_rows":101}, "maintenance":{"enabled":true,"operations":["optimize"],"apply":true,"parquet_writer":{"max_row_group_rows":202}}});
    let tunables = Tunables::parse_oat_json(input)?;
    let maintenance = tunables.environment_for(ComponentRole::Maintenance)?;
    for key in [
        "ARROWCOL_MAINT_ENABLED",
        "ARROWCOL_MAINT_OPERATIONS",
        "ARROWCOL_MAINT_APPLY",
    ] {
        assert!(!maintenance.contains_key(key));
    }
    let collector = tunables.environment_for(ComponentRole::Collector)?;
    for (env, expected) in [(&collector, 101), (&maintenance, 202)] {
        let writer: Value = serde_json::from_str(
            env.get("ARROWCOL_PARQUET_CONFIG")
                .context("missing writer environment")?,
        )
        .context("invalid writer environment JSON")?;
        assert_eq!(writer["max_row_group_rows"], expected);
    }
    let readback: Value = serde_json::from_str(&tunables.canonical_json()?)?;
    assert_eq!(readback.pointer("/maintenance/enabled"), Some(&json!(true)));
    assert_eq!(readback.pointer("/maintenance/operations"), Some(&json!(["optimize"])));
    assert_eq!(readback.pointer("/maintenance/apply"), Some(&json!(true)));
    Ok(())
}

#[test]
fn collector_json_envelopes_preserve_nested_values() -> Result<()> {
    let input = json!({"collector": {
        "batching": {"global_capacity": {"rows": 12000000}, "max_age": "2m"},
        "catalog": {"maximum_cache_age": "15s"},
        "commit": {"max_files_per_commit": 7},
        "exporter": {"authority_timeout": "12s"},
        "limits": {"batch_channel_capacity": 7},
        "pressure": {"heartbeat_interval": "3s"},
        "routing": {"command_timeout": "12s"}
    }});
    let settings = Tunables::parse_oat_json(input)?;
    let env = settings.environment_for(ComponentRole::Collector)?;
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
        let envelope: Value = serde_json::from_str(env.get(key).with_context(|| format!("missing {key}"))?)
            .with_context(|| format!("invalid JSON in {key}"))?;
        assert_eq!(envelope.pointer(pointer), Some(&expected), "{key}{pointer}");
    }
    Ok(())
}

#[test]
fn maintenance_logging_is_inert_only_when_the_role_is_disabled() -> Result<()> {
    for input in [
        json!({"logging":{"maintenance":"debug"}}),
        json!({"maintenance":{"enabled":false}, "logging":{"maintenance":"debug"}}),
    ] {
        let tunables = Tunables::parse_oat_json(input)?;
        let requested: Value = serde_json::from_str(&tunables.canonical_json()?)?;
        assert_eq!(requested.pointer("/logging/maintenance"), Some(&json!("debug")));
        assert!(tunables.environment_for(ComponentRole::Maintenance)?.is_empty());
    }
    let enabled = Tunables::parse_oat_json(json!({
        "maintenance":{"enabled":true,"operations":["optimize"]},
        "logging":{"maintenance":"debug"}
    }))?;
    assert_eq!(
        enabled
            .environment_for(ComponentRole::Maintenance)?
            .get("RUST_LOG")
            .map(String::as_str),
        Some("debug")
    );
    Ok(())
}

#[test]
fn sorting_column_element_schema_is_covered_by_the_projection_fixture() -> Result<()> {
    let mut fields = struct_fields("SortColumn")?.context("missing SortColumn input struct")?;
    fields.sort();
    assert_eq!(
        fields,
        vec![
            ("descending".to_owned(), "bool".to_owned()),
            ("nulls_first".to_owned(), "bool".to_owned()),
            ("path".to_owned(), "String".to_owned()),
        ]
    );
    for path in [
        "parquet_writer.sorting_columns",
        "maintenance.parquet_writer.sorting_columns",
    ] {
        let value = json!([{"path":"customer_id", "descending":true, "nulls_first":true}]);
        let tunables = Tunables::parse_oat_json(fixture(path, value.clone()))
            .with_context(|| format!("{path}: parse sorting fixture"))?;
        let (role, key, pointer, _) = destination(path, &value)?;
        let environment = tunables
            .environment_for(role)
            .with_context(|| format!("{path}: project {role:?}"))?;
        let writer: Value = serde_json::from_str(
            environment
                .get(&key)
                .with_context(|| format!("{path}: missing {key}"))?,
        )
        .with_context(|| format!("{path}: invalid JSON in {key}"))?;
        assert_eq!(
            writer.pointer(
                pointer
                    .as_deref()
                    .with_context(|| format!("{path}: missing writer JSON pointer"))?
            ),
            Some(&value),
            "{path}"
        );
    }
    Ok(())
}

#[test]
fn router_inherit_intentionally_leaves_its_filter_unset() -> Result<()> {
    let settings = Tunables::parse_oat_json(json!({"logging": {"router": "inherit"}}))?;
    assert!(
        !settings
            .environment_for(ComponentRole::Router)?
            .contains_key("RUST_LOG")
    );
    let request: Value = serde_json::from_str(&settings.canonical_json()?)?;
    assert_eq!(request.pointer("/logging/router"), Some(&json!("inherit")));
    Ok(())
}
