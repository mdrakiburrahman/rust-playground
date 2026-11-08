use crate::config::OatConfig;
use crate::model::{AppState, RunState, unix_time_ms};
use anyhow::{Context, Result, bail};
use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::object_store_for_url;
use chrono::DateTime;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path as ObjectPath};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use url::Url;

#[derive(Clone, Debug)]
struct OatResult {
    run_id: String,
    replicas: u64,
    dfe_worker_concurrency: u64,
    table: String,
    rows: u64,
    duration_ms: u64,
    rows_per_second: f64,
    cpu_millicores: Option<f64>,
    memory_bytes: Option<u64>,
    qap: QapMetrics,
}

#[derive(Clone, Copy, Debug, Default)]
struct QapMetrics {
    observation_complete: u64,
    observed_tables: u64,
    completion_percent: Option<f64>,
    pending_artifacts: Option<u64>,
    cached_bytes: Option<u64>,
    unhealthy_catalogs: Option<u64>,
    lag_seconds: Option<f64>,
}

#[derive(Clone, Copy, Debug, Default)]
struct ResourceMetrics {
    cpu_millicores: f64,
    memory_bytes: u64,
}

pub async fn execute(config: OatConfig, id: &str, state: Arc<AppState>) -> Result<usize> {
    config.validate()?;
    state
        .update(id, |status| {
            status.state = RunState::Running;
            status.progress = "capturing deployment state".to_owned();
            status.started_at_unix_ms = unix_time_ms();
        })
        .await;

    let arrow_original = deployment(&config, &config.arrowcol_deployment).await?;
    let dfe_original = deployment(&config, &config.dfe_deployment).await?;
    let matrix_result = run_matrix(&config, id, &state).await;
    let restore_result = restore_deployments(&config, arrow_original, dfe_original).await;

    let results = match (matrix_result, restore_result) {
        (Ok(results), Ok(())) => results,
        (Err(run_error), Ok(())) => return Err(run_error),
        (Ok(_), Err(restore_error)) => return Err(restore_error).context("restore original deployments"),
        (Err(run_error), Err(restore_error)) => {
            return Err(run_error.context(format!("deployment restoration also failed: {restore_error:#}")));
        }
    };

    state
        .update(id, |status| {
            status.progress = "writing compact Parquet results".to_owned()
        })
        .await;
    publish_results(&config, &results).await?;
    Ok(results.len())
}

async fn run_matrix(config: &OatConfig, id: &str, state: &AppState) -> Result<Vec<OatResult>> {
    let mut results = Vec::new();
    let total = config.replica_counts.len() * config.dfe_worker_concurrency.len();
    let mut completed = 0;

    for replicas in &config.replica_counts {
        for concurrency in &config.dfe_worker_concurrency {
            completed += 1;
            state
                .update(id, |status| {
                    status.progress = format!(
                        "experiment {completed}/{total}: ArrowCol replicas={replicas}, DFE --num-cores={concurrency}"
                    );
                })
                .await;
            configure_deployments(config, *replicas, *concurrency).await?;
            rollout(config, &config.arrowcol_deployment).await?;
            rollout(config, &config.dfe_deployment).await?;

            let assigned_tables = config
                .tables
                .iter()
                .take(*replicas as usize)
                .cloned()
                .collect::<Vec<_>>();
            let run_id = format!("{id}-r{replicas}-c{concurrency}");
            let client_durations = run_clients(config, id, &assigned_tables).await?;
            let qap = observe_qap(config, &run_id, &assigned_tables).await?;
            let metrics = collect_metrics(config).await?;
            for table in assigned_tables {
                let duration_ms = client_durations
                    .get(&table)
                    .copied()
                    .with_context(|| format!("DMV client did not report a duration for {table}"))?;
                let rows = u64::try_from(config.rows_per_table)?;
                results.push(OatResult {
                    run_id: run_id.clone(),
                    replicas: u64::from(*replicas),
                    dfe_worker_concurrency: u64::from(*concurrency),
                    table,
                    rows,
                    duration_ms,
                    rows_per_second: rows as f64 / (duration_ms as f64 / 1000.0),
                    cpu_millicores: Some(metrics.cpu_millicores),
                    memory_bytes: Some(metrics.memory_bytes),
                    qap,
                });
            }
        }
    }
    Ok(results)
}

async fn run_clients(config: &OatConfig, id: &str, tables: &[String]) -> Result<HashMap<String, u64>> {
    let mut tasks = tokio::task::JoinSet::new();
    for table in tables {
        let binary = config.dmv_client_binary.clone();
        let endpoint = config.arrowcol_endpoint.clone();
        let table = table.clone();
        let rows = config.rows_per_table.to_string();
        let concurrency = config.dmv_concurrency.to_string();
        let client_id = format!("oat-{id}-{table}");
        tasks.spawn(async move {
            let started = Instant::now();
            let output = Command::new(binary)
                .args([
                    "--endpoint",
                    &endpoint,
                    "--client-id",
                    &client_id,
                    "--rows",
                    &rows,
                    "--concurrency",
                    &concurrency,
                    "--table",
                    &table,
                ])
                .output()
                .await
                .with_context(|| format!("start DMV client for {table}"))?;
            if !output.status.success() {
                bail!(
                    "DMV client for {table} exited with {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            let duration_ms = u64::try_from(started.elapsed().as_millis())?.max(1);
            Ok::<(String, u64), anyhow::Error>((table, duration_ms))
        });
    }
    let mut durations = HashMap::with_capacity(tables.len());
    let mut first_error = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok((table, duration_ms))) => {
                durations.insert(table, duration_ms);
            }
            Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
            Err(error) if first_error.is_none() => first_error = Some(error.into()),
            _ => {}
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(durations)
}

async fn observe_qap(config: &OatConfig, run_id: &str, tables: &[String]) -> Result<QapMetrics> {
    let qap_tables = tables
        .iter()
        .map(|table| format!("dataflow_{table}"))
        .collect::<Vec<_>>();
    let results = std::env::temp_dir().join("arrowcol-oat").join(run_id);
    let endpoint = config.qap_endpoint.clone();
    let database = config.qap_database.clone();
    let observer_tables = qap_tables.clone();
    let timeout = Duration::from_secs(config.qap_timeout_seconds);
    let observation = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&results)
            .with_context(|| format!("create QAP results directory {}", results.display()))?;
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        runtime
            .block_on(arrowcol_loadgen::observe_qap(
                &endpoint,
                &results,
                database.as_deref(),
                &observer_tables,
                timeout,
            ))
            .context("observe Kusto Query Acceleration Policy")
    })
    .await
    .context("join Kusto QAP observer")??;
    aggregate_qap(&observation, &qap_tables)
}

fn aggregate_qap(observation: &Value, expected_tables: &[String]) -> Result<QapMetrics> {
    let expected = observation
        .get("expected_tables")
        .and_then(Value::as_array)
        .context("QAP observation has no expected_tables array")?
        .iter()
        .map(|table| table.as_str().context("QAP expected table name must be a string"))
        .collect::<Result<Vec<_>>>()?;
    if expected != expected_tables.iter().map(String::as_str).collect::<Vec<_>>() {
        bail!("QAP observation returned an unexpected table set");
    }
    let tables = observation
        .get("tables")
        .and_then(Value::as_object)
        .context("QAP observation has no tables object")?;
    let observed_unix_ms = observation
        .get("observed_unix_ms")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .context("QAP observation timestamp is invalid")?;
    let mut available = Vec::new();
    for name in expected_tables {
        let table = tables
            .get(name)
            .with_context(|| format!("QAP observation omitted expected table {name}"))?;
        if table.get("available").and_then(Value::as_bool) == Some(true) {
            available.push(table);
        }
    }
    let all_available = available.len() == expected_tables.len();
    let completion_values = available
        .iter()
        .map(|table| {
            table
                .pointer("/statistics/AccelerationCompletePercentage")
                .and_then(Value::as_f64)
        })
        .collect::<Option<Vec<_>>>();
    let completion_percent = completion_values
        .and_then(|values| {
            values
                .iter()
                .all(|value| (0.0..=100.0).contains(value))
                .then(|| values.into_iter().fold(f64::INFINITY, f64::min))
        })
        .filter(|_| all_available);
    let pending_artifacts =
        sum_qap_values(&available, "/statistics/AccelerationPendingDataFilesCount").filter(|_| all_available);
    let cached_bytes = sum_qap_values(&available, "/statistics/HotSize").filter(|_| all_available);
    let health = available
        .iter()
        .map(|table| table.pointer("/state/is_healthy").and_then(Value::as_bool))
        .collect::<Option<Vec<_>>>();
    let unhealthy_catalogs = health
        .map(|values| values.into_iter().filter(|healthy| !healthy).count() as u64)
        .filter(|_| all_available);
    let lag_values = available
        .iter()
        .map(|table| {
            let updated = table
                .pointer("/statistics/LastUpdatedDateTime")
                .and_then(Value::as_str)?;
            let timestamp = DateTime::parse_from_rfc3339(updated).ok()?.timestamp_millis();
            Some((observed_unix_ms as f64 - timestamp as f64) / 1000.0)
        })
        .collect::<Option<Vec<_>>>();
    let lag_seconds = lag_values
        .filter(|values| values.iter().all(|value| value.is_finite() && *value >= 0.0))
        .map(|values| values.into_iter().fold(0.0, f64::max))
        .filter(|_| all_available);
    let observation_complete = u64::from(
        completion_percent.is_some()
            && pending_artifacts.is_some()
            && cached_bytes.is_some()
            && unhealthy_catalogs.is_some()
            && lag_seconds.is_some(),
    );
    Ok(QapMetrics {
        observation_complete,
        observed_tables: available.len() as u64,
        completion_percent,
        pending_artifacts,
        cached_bytes,
        unhealthy_catalogs,
        lag_seconds,
    })
}

fn sum_qap_values(tables: &[&Value], pointer: &str) -> Option<u64> {
    tables
        .iter()
        .try_fold(0_u64, |sum, table| sum.checked_add(table.pointer(pointer)?.as_u64()?))
}

async fn configure_deployments(config: &OatConfig, replicas: u32, concurrency: u32) -> Result<()> {
    let mut arrowcol = deployment(config, &config.arrowcol_deployment).await?;
    arrowcol["spec"]["replicas"] = json!(replicas);
    apply_deployment(config, arrowcol).await?;

    let mut dfe = deployment(config, &config.dfe_deployment).await?;
    let containers = dfe
        .pointer_mut("/spec/template/spec/containers")
        .and_then(Value::as_array_mut)
        .context("DFE Deployment has no container list")?;
    let container = containers
        .iter_mut()
        .find(|container| container.get("name").and_then(Value::as_str) == Some(config.dfe_container.as_str()))
        .with_context(|| format!("DFE container {:?} was not found", config.dfe_container))?;
    let args = container
        .get_mut("args")
        .and_then(Value::as_array_mut)
        .context("DFE container args must be an array")?;
    let mut rewritten = Vec::with_capacity(args.len() + 2);
    let mut index = 0;
    while index < args.len() {
        let value = args[index].as_str().context("DFE args must be strings")?;
        if value == "--num-cores" {
            index += 2;
        } else if value.starts_with("--num-cores=") {
            index += 1;
        } else {
            rewritten.push(args[index].clone());
            index += 1;
        }
    }
    rewritten.push(json!("--num-cores"));
    rewritten.push(json!(concurrency.to_string()));
    container["args"] = Value::Array(rewritten);
    apply_deployment(config, dfe).await
}

async fn restore_deployments(config: &OatConfig, arrowcol: Value, dfe: Value) -> Result<()> {
    let mut failures = Vec::new();
    if let Err(error) = apply_deployment(config, arrowcol).await {
        failures.push(format!("restore ArrowCol Deployment: {error:#}"));
    }
    if let Err(error) = apply_deployment(config, dfe).await {
        failures.push(format!("restore DFE Deployment: {error:#}"));
    }
    if let Err(error) = rollout(config, &config.arrowcol_deployment).await {
        failures.push(format!("wait for ArrowCol restore: {error:#}"));
    }
    if let Err(error) = rollout(config, &config.dfe_deployment).await {
        failures.push(format!("wait for DFE restore: {error:#}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "deployment restoration had failures: {}",
            failures.join("; ")
        ))
    }
}

async fn deployment(config: &OatConfig, name: &str) -> Result<Value> {
    let output = kubectl(
        config,
        vec![
            "get",
            "deployment",
            name,
            "--namespace",
            &config.namespace,
            "-o",
            "json",
        ],
        None,
    )
    .await?;
    serde_json::from_str(&output).with_context(|| format!("parse Deployment {name} JSON"))
}

async fn apply_deployment(config: &OatConfig, mut deployment: Value) -> Result<()> {
    let metadata = deployment
        .get_mut("metadata")
        .and_then(Value::as_object_mut)
        .context("Deployment metadata must be an object")?;
    for field in [
        "uid",
        "resourceVersion",
        "generation",
        "creationTimestamp",
        "managedFields",
        "selfLink",
    ] {
        metadata.remove(field);
    }
    deployment
        .as_object_mut()
        .context("Deployment must be an object")?
        .remove("status");
    let payload = serde_json::to_vec(&deployment)?;
    kubectl(
        config,
        vec!["apply", "--namespace", &config.namespace, "-f", "-"],
        Some(payload),
    )
    .await?;
    Ok(())
}

async fn rollout(config: &OatConfig, name: &str) -> Result<()> {
    kubectl(
        config,
        vec![
            "rollout",
            "status",
            &format!("deployment/{name}"),
            "--namespace",
            &config.namespace,
            "--timeout=180s",
        ],
        None,
    )
    .await
    .with_context(|| format!("wait for Deployment {name} rollout"))?;
    Ok(())
}

async fn collect_metrics(config: &OatConfig) -> Result<ResourceMetrics> {
    let arrowcol = top_metrics(config, &config.arrowcol_pod_selector).await?;
    let dfe = top_metrics(config, &config.dfe_pod_selector).await?;
    Ok(ResourceMetrics {
        cpu_millicores: arrowcol.cpu_millicores + dfe.cpu_millicores,
        memory_bytes: arrowcol.memory_bytes.saturating_add(dfe.memory_bytes),
    })
}

async fn top_metrics(config: &OatConfig, selector: &str) -> Result<ResourceMetrics> {
    let output = kubectl(
        config,
        vec![
            "top",
            "pods",
            "--namespace",
            &config.namespace,
            "--selector",
            selector,
            "--no-headers",
        ],
        None,
    )
    .await?;
    let mut metrics = ResourceMetrics::default();
    let mut count = 0_u64;
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let columns = line.split_whitespace().collect::<Vec<_>>();
        if columns.len() < 3 {
            bail!("unexpected kubectl top output row: {line}");
        }
        metrics.cpu_millicores += parse_cpu_millicores(columns[1])?;
        metrics.memory_bytes = metrics.memory_bytes.saturating_add(parse_memory_bytes(columns[2])?);
        count += 1;
    }
    if count == 0 {
        bail!("kubectl top returned no pods for selector {selector:?}");
    }
    Ok(metrics)
}

fn parse_cpu_millicores(value: &str) -> Result<f64> {
    if let Some(millicores) = value.strip_suffix('m') {
        Ok(millicores.parse()?)
    } else {
        Ok(value.parse::<f64>()? * 1000.0)
    }
}

fn parse_memory_bytes(value: &str) -> Result<u64> {
    let (number, factor) = [
        ("Ki", 1024_u64),
        ("Mi", 1024_u64.pow(2)),
        ("Gi", 1024_u64.pow(3)),
        ("Ti", 1024_u64.pow(4)),
        ("K", 1000_u64),
        ("M", 1000_u64.pow(2)),
        ("G", 1000_u64.pow(3)),
        ("T", 1000_u64.pow(4)),
        ("B", 1_u64),
    ]
    .into_iter()
    .find_map(|(suffix, factor)| value.strip_suffix(suffix).map(|number| (number, factor)))
    .unwrap_or((value, 1));
    Ok((number.parse::<f64>()? * factor as f64) as u64)
}

async fn kubectl(config: &OatConfig, args: Vec<&str>, stdin: Option<Vec<u8>>) -> Result<String> {
    let mut command = Command::new("kubectl");
    if let Some(context) = &config.kubectl_context {
        command.args(["--context", context]);
    }
    command.args(&args).stdout(Stdio::piped()).stderr(Stdio::piped());
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    }
    let mut child = command.spawn().context("start kubectl")?;
    if let Some(payload) = stdin {
        let mut input = child.stdin.take().context("open kubectl stdin")?;
        input.write_all(&payload).await.context("write kubectl manifest")?;
    }
    let output = child.wait_with_output().await.context("wait for kubectl")?;
    if !output.status.success() {
        bail!(
            "kubectl {} failed with {}: {}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8(output.stdout).context("kubectl output was not UTF-8")?)
}

async fn publish_results(config: &OatConfig, results: &[OatResult]) -> Result<()> {
    let bytes = parquet_bytes(results)?;
    for destination in [&config.result_url, &config.website_url] {
        let url = Url::parse(destination)?;
        let path = ObjectPath::from_url_path(url.path())?;
        let store = object_store_for_url(&url)?;
        put_parquet(store, path, bytes.clone()).await?;
    }
    Ok(())
}

async fn put_parquet(store: Arc<dyn ObjectStore>, path: ObjectPath, bytes: Vec<u8>) -> Result<()> {
    store
        .put(&path, PutPayload::from(bytes))
        .await
        .with_context(|| format!("write benchmark Parquet object {path}"))?;
    Ok(())
}

fn parquet_bytes(results: &[OatResult]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("replicas", DataType::UInt64, false),
        Field::new("dfe_worker_concurrency", DataType::UInt64, false),
        Field::new("table", DataType::Utf8, false),
        Field::new("rows", DataType::UInt64, false),
        Field::new("duration_ms", DataType::UInt64, false),
        Field::new("rows_per_second", DataType::Float64, false),
        Field::new("cpu_millicores", DataType::Float64, true),
        Field::new("memory_bytes", DataType::UInt64, true),
        Field::new("qap_observation_complete", DataType::UInt64, false),
        Field::new("qap_observed_tables", DataType::UInt64, false),
        Field::new("qap_completion_percent", DataType::Float64, true),
        Field::new("qap_pending_artifacts", DataType::UInt64, true),
        Field::new("qap_cached_bytes", DataType::UInt64, true),
        Field::new("qap_unhealthy_catalogs", DataType::UInt64, true),
        Field::new("qap_lag_seconds", DataType::Float64, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.run_id.as_str()),
        )),
        Arc::new(UInt64Array::from_iter_values(results.iter().map(|row| row.replicas))),
        Arc::new(UInt64Array::from_iter_values(
            results.iter().map(|row| row.dfe_worker_concurrency),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.table.as_str()),
        )),
        Arc::new(UInt64Array::from_iter_values(results.iter().map(|row| row.rows))),
        Arc::new(UInt64Array::from_iter_values(results.iter().map(|row| row.duration_ms))),
        Arc::new(Float64Array::from_iter_values(
            results.iter().map(|row| row.rows_per_second),
        )),
        Arc::new(Float64Array::from(
            results.iter().map(|row| row.cpu_millicores).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            results.iter().map(|row| row.memory_bytes).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from_iter_values(
            results.iter().map(|row| row.qap.observation_complete),
        )),
        Arc::new(UInt64Array::from_iter_values(
            results.iter().map(|row| row.qap.observed_tables),
        )),
        Arc::new(Float64Array::from(
            results.iter().map(|row| row.qap.completion_percent).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            results.iter().map(|row| row.qap.pending_artifacts).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            results.iter().map(|row| row.qap.cached_bytes).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            results.iter().map(|row| row.qap.unhealthy_catalogs).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            results.iter().map(|row| row.qap.lag_seconds).collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kubernetes_cpu_and_memory_units() -> Result<()> {
        assert_eq!(parse_cpu_millicores("250m")?, 250.0);
        assert_eq!(parse_cpu_millicores("1.5")?, 1500.0);
        assert_eq!(parse_memory_bytes("2Mi")?, 2 * 1024 * 1024);
        assert_eq!(parse_memory_bytes("512")?, 512);
        Ok(())
    }

    #[test]
    fn aggregates_qap_metrics_only_when_all_expected_tables_are_measured() -> Result<()> {
        let names = ["dataflow_sqlserver_wait_stats", "dataflow_sqlserver_cpu_utilization"].map(str::to_owned);
        let observation = serde_json::json!({
            "observed_unix_ms": 10_000,
            "expected_tables": names,
            "tables": {
                "dataflow_sqlserver_wait_stats": {
                    "available": true,
                    "statistics": {
                        "AccelerationCompletePercentage": 70.0,
                        "AccelerationPendingDataFilesCount": 2,
                        "HotSize": 10,
                        "LastUpdatedDateTime": "1970-01-01T00:00:00Z"
                    },
                    "state": {"is_healthy": true}
                },
                "dataflow_sqlserver_cpu_utilization": {
                    "available": true,
                    "statistics": {
                        "AccelerationCompletePercentage": 50.0,
                        "AccelerationPendingDataFilesCount": 3,
                        "HotSize": 20,
                        "LastUpdatedDateTime": "1970-01-01T00:00:00Z"
                    },
                    "state": {"is_healthy": false}
                }
            }
        });
        let metrics = aggregate_qap(&observation, &names)?;
        assert_eq!(metrics.observation_complete, 1);
        assert_eq!(metrics.observed_tables, 2);
        assert_eq!(metrics.completion_percent, Some(50.0));
        assert_eq!(metrics.pending_artifacts, Some(5));
        assert_eq!(metrics.cached_bytes, Some(30));
        assert_eq!(metrics.unhealthy_catalogs, Some(1));
        assert_eq!(metrics.lag_seconds, Some(10.0));

        let partial = serde_json::json!({
            "observed_unix_ms": 10_000,
            "expected_tables": names,
            "tables": {
                "dataflow_sqlserver_wait_stats": {"available": false},
                "dataflow_sqlserver_cpu_utilization": {"available": false}
            }
        });
        let metrics = aggregate_qap(&partial, &names)?;
        assert_eq!(metrics.observation_complete, 0);
        assert_eq!(metrics.observed_tables, 0);
        assert_eq!(metrics.completion_percent, None);
        Ok(())
    }

    #[test]
    fn writes_one_compact_parquet_file_with_viewer_schema() -> Result<()> {
        let results = vec![OatResult {
            run_id: "run-1".to_owned(),
            replicas: 1,
            dfe_worker_concurrency: 2,
            table: "sqlserver_wait_stats".to_owned(),
            rows: 100,
            duration_ms: 10,
            rows_per_second: 10_000.0,
            cpu_millicores: Some(50.0),
            memory_bytes: Some(1024),
            qap: QapMetrics::default(),
        }];
        let bytes = parquet_bytes(&results)?;
        assert!(bytes.starts_with(b"PAR1"));
        assert!(bytes.ends_with(b"PAR1"));
        Ok(())
    }
}
