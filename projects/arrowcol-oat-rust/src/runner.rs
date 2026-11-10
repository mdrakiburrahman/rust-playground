use crate::config::OatConfig;
use crate::model::{AppState, RunState, unix_time_ms};
use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray, UInt64Array,
};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_deltalake::delta::object_store_for_url;
use arrowcol_protocol::ImplementationPool;
use arrowcol_routing::{LeaseKey, PressurePolicy, RoutingPolicy, run_table_pin_set_digest};
use bytes::Bytes;
use chrono::DateTime;
use fs2::FileExt;
use futures_util::future::join_all;
use object_store::{GetOptions, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion, path::Path as ObjectPath};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use url::Url;

#[cfg(test)]
include!("runner_failure_tests.rs");

#[derive(Clone, Debug)]
struct OatResult {
    run_id: String,
    mode: String,
    stamp: Option<String>,
    host: String,
    experiment: String,
    started_at_ms: i64,
    measure_seconds: f64,
    arrowcols: i32,
    clients: i32,
    tables: i32,
    table_owner: Option<String>,
    pinning_ok: bool,
    pinning_error: Option<String>,
    failure: Option<String>,
    status: String,
    git_sha: String,
    tunables_json: String,
    replicas: u64,
    dfe_worker_concurrency: u64,
    table: String,
    rows: Option<u64>,
    duration_ms: Option<u64>,
    rows_per_second: Option<f64>,
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

struct QapBinding {
    logical_table: String,
    external_table: String,
}

#[derive(Clone, Debug)]
struct ClientHandle {
    client_id: String,
    table: String,
    process: ClientProcess,
}

#[derive(Clone, Debug)]
enum ClientProcess {
    Local { container_id: String },
    Aks { job_name: String },
}

#[derive(Clone, Default)]
struct ClientProgress {
    ready: bool,
    schedule_received: bool,
    completed: bool,
    failure: Option<String>,
    output: String,
}

#[async_trait::async_trait]
trait Platform: Send + Sync {
    fn kind(&self) -> crate::config::OatMode;
    fn workload_endpoint(&self, config: &OatConfig) -> String;
    fn router_metrics_endpoint(&self, config: &OatConfig) -> Result<String>;
    fn qap_endpoint(&self, config: &OatConfig) -> String;
    async fn ensure_stack(&mut self, config: &OatConfig) -> Result<()>;
    async fn scale_arrowcols(&mut self, config: &OatConfig, replicas: u32, concurrency: u32) -> Result<()>;
    async fn initialize_run_table_pins(
        &self,
        config: &OatConfig,
        run_id: &str,
        expected_keys_json: &str,
        expires_at_unix_ms: u64,
        now_unix_ms: u64,
    ) -> Result<()>;
    fn qap_observation_mode(&self) -> arrowcol_loadgen::ObservationMode;
    fn qap_database(&self, config: &OatConfig) -> Result<String>;
    async fn initialize_qap(&mut self, config: &OatConfig, run_id: &str, tables: &[String]) -> Result<Vec<String>>;
    fn mark_qap_outcome_unresolved(&mut self) {}
    async fn prepare_client_fleet(&self, config: &OatConfig, run_id: &str) -> Result<()>;
    async fn start_client(
        &self,
        config: &OatConfig,
        run_id: &str,
        table: &str,
        client_index: usize,
        args: &[String],
    ) -> Result<ClientHandle>;
    async fn poll_client(&self, config: &OatConfig, handle: &ClientHandle) -> Result<ClientProgress>;
    async fn poll_client_fleet(&self, config: &OatConfig, handles: &[ClientHandle]) -> Result<Vec<ClientProgress>> {
        join_all(handles.iter().map(|handle| self.poll_client(config, handle)))
            .await
            .into_iter()
            .collect()
    }
    async fn arm_clients(&self, config: &OatConfig, run_id: &str, run_start_unix_ms: u64) -> Result<()>;
    async fn finish_client(&self, config: &OatConfig, handle: ClientHandle) -> Result<String>;
    async fn cancel_client(&self, config: &OatConfig, handle: &ClientHandle) -> Result<()>;
    async fn sample_resources(&self, config: &OatConfig) -> Result<ResourceMetrics>;
    async fn cleanup(&mut self, config: &OatConfig) -> Result<()>;
}

struct AksPlatform {
    run_id: String,
    namespace_created: bool,
    namespace_create_attempted: bool,
    namespace_owner_token: String,
    namespace_uid: Option<String>,
    runtime_dir: Option<PathBuf>,
    qap_relative_data_root: Option<String>,
    router_metrics_url: Option<String>,
    router_tunnel: Option<OwnedKubectlProcess>,
    fic_record_path: Option<PathBuf>,
    fic_ready_snapshot_path: Option<PathBuf>,
    fic_prepare_attempted: bool,
    qap_record_path: Option<PathBuf>,
    qap_bind_attempted: bool,
    qap_outcome_unresolved: bool,
    workload_stop_record_path: Option<PathBuf>,
    schedule_configmap_name: String,
    service_account: Option<String>,
    router_yaml: Option<String>,
}

struct OwnedKubectlProcess {
    child: Child,
    stderr: JoinHandle<std::io::Result<Vec<u8>>>,
}

struct LocalPlatform {
    run_id: String,
    state_dir: PathBuf,
    environment: HashMap<String, String>,
    control_dir: PathBuf,
    client_progress: std::sync::Mutex<HashMap<String, ClientProgress>>,
    router_metrics_url: Option<String>,
    qap_url: Option<String>,
    daemon_endpoint: Option<String>,
}

#[async_trait::async_trait]
impl Platform for AksPlatform {
    fn kind(&self) -> crate::config::OatMode {
        crate::config::OatMode::Aks
    }

    fn workload_endpoint(&self, config: &OatConfig) -> String {
        config.arrowcol_endpoint.clone()
    }

    fn router_metrics_endpoint(&self, _config: &OatConfig) -> Result<String> {
        self.router_metrics_url
            .clone()
            .context("AKS router metrics tunnel is not ready")
    }

    fn qap_endpoint(&self, config: &OatConfig) -> String {
        config.qap_endpoint.clone()
    }

    async fn ensure_stack(&mut self, config: &OatConfig) -> Result<()> {
        prepare_aks_platform(config, self).await?;
        if let Some(overlay) = config.platform.kustomize_overlay.as_deref() {
            apply_aks_overlay(config, overlay).await?;
        }
        Ok(())
    }

    async fn scale_arrowcols(&mut self, config: &OatConfig, replicas: u32, concurrency: u32) -> Result<()> {
        configure_deployments(config, replicas, concurrency).await?;
        for name in config.deployment_names() {
            rollout(config, name).await?;
        }
        Ok(())
    }

    async fn initialize_run_table_pins(
        &self,
        config: &OatConfig,
        run_id: &str,
        expected_keys_json: &str,
        expires_at_unix_ms: u64,
        now_unix_ms: u64,
    ) -> Result<()> {
        run_aks_pin_setup_job(
            config,
            self,
            run_id,
            expected_keys_json,
            expires_at_unix_ms,
            now_unix_ms,
        )
        .await
    }

    fn qap_observation_mode(&self) -> arrowcol_loadgen::ObservationMode {
        arrowcol_loadgen::ObservationMode::Azure
    }

    fn qap_database(&self, config: &OatConfig) -> Result<String> {
        config
            .qap_database
            .clone()
            .or_else(|| std::env::var("AZURE_KUSTO_EXPERIMENT_DATABASE").ok())
            .context("AKS QAP requires qap_database or AZURE_KUSTO_EXPERIMENT_DATABASE")
    }

    async fn initialize_qap(&mut self, config: &OatConfig, run_id: &str, tables: &[String]) -> Result<Vec<String>> {
        let bindings = qap_bindings(run_id, tables)?;
        let database = self.qap_database(config)?;
        let runtime_dir = self
            .runtime_dir
            .as_deref()
            .context("AKS runtime directory is not available")?;
        let manifest_path = runtime_dir.join("oat-qap-manifest.json");
        let record_path = runtime_dir.join("oat-qap-record.json");
        let manifest = json!({
            "schema_version": 1,
            "run_id": run_id,
            "database": database,
            "tables": bindings.iter().map(|binding| Ok(json!({
                "external_table": binding.external_table,
                "relative_path": aks_qap_relative_path(
                    self.qap_relative_data_root.as_deref().context("AKS run-scoped data root is missing")?,
                    &binding.logical_table,
                )?,
            }))).collect::<Result<Vec<_>>>()?,
        });
        tokio::fs::write(&manifest_path, serde_json::to_vec(&manifest)?)
            .await
            .with_context(|| format!("write run-owned AKS QAP manifest {}", manifest_path.display()))?;
        self.qap_record_path = Some(record_path.clone());
        self.qap_bind_attempted = true;
        let workload_record = self
            .fic_ready_snapshot_path
            .as_deref()
            .context("AKS QAP binding requires the verified run FIC ownership record")?;
        run_oat_qap_helper(
            "bind-oat-qap",
            run_id,
            Some(&manifest_path),
            Some(workload_record),
            None,
            &record_path,
        )
        .await
        .context("bind run-owned AKS QAP external tables")?;
        require_nonempty_record(&record_path).await?;
        run_oat_qap_helper("verify-oat-qap", run_id, None, None, None, &record_path)
            .await
            .context("verify run-owned AKS QAP bindings")?;
        Ok(bindings.iter().map(|binding| binding.external_table.clone()).collect())
    }

    fn mark_qap_outcome_unresolved(&mut self) {
        self.qap_outcome_unresolved = true;
    }

    async fn prepare_client_fleet(&self, config: &OatConfig, _run_id: &str) -> Result<()> {
        let payload = serde_json::to_vec(&json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": self.schedule_configmap_name,
                "namespace": config.namespace,
                "labels": {
                    "app": "arrowcol-oat-schedule",
                    "arrowcol-oat-run": self.run_id.chars()
                        .filter(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
                        .take(8)
                        .collect::<String>(),
                },
            },
            "data": { "schedule": "pending" },
        }))?;
        kubectl(config, vec!["create", "-f", "-"], Some(payload))
            .await
            .context("create unique AKS client schedule ConfigMap")
            .map(|_| ())
    }

    async fn start_client(
        &self,
        config: &OatConfig,
        run_id: &str,
        table: &str,
        client_index: usize,
        args: &[String],
    ) -> Result<ClientHandle> {
        start_aks_client_job(config, self, run_id, table, client_index, args).await
    }

    async fn poll_client(&self, config: &OatConfig, handle: &ClientHandle) -> Result<ClientProgress> {
        poll_aks_client_job(config, handle).await
    }

    async fn poll_client_fleet(&self, config: &OatConfig, handles: &[ClientHandle]) -> Result<Vec<ClientProgress>> {
        poll_aks_client_fleet(config, self, handles).await
    }

    async fn arm_clients(&self, config: &OatConfig, _run_id: &str, run_start_unix_ms: u64) -> Result<()> {
        let payload = serde_json::to_vec(&json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": self.schedule_configmap_name,
                "namespace": config.namespace,
                "labels": {
                    "app": "arrowcol-oat-schedule",
                    "arrowcol-oat-run": self.run_id.chars()
                        .filter(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
                        .take(8)
                        .collect::<String>(),
                },
            },
            "data": { "schedule": run_start_unix_ms.to_string() },
        }))?;
        kubectl(
            config,
            vec!["apply", "--namespace", &config.namespace, "-f", "-"],
            Some(payload),
        )
        .await
        .context("arm the common AKS client measurement schedule")
        .map(|_| ())
    }

    async fn finish_client(&self, config: &OatConfig, handle: ClientHandle) -> Result<String> {
        finish_aks_client_job(config, handle).await
    }

    async fn cancel_client(&self, config: &OatConfig, handle: &ClientHandle) -> Result<()> {
        cancel_aks_client_job(config, handle).await
    }

    async fn sample_resources(&self, config: &OatConfig) -> Result<ResourceMetrics> {
        collect_aks_metrics(config).await
    }

    async fn cleanup(&mut self, config: &OatConfig) -> Result<()> {
        cleanup_aks_platform(config, self).await
    }
}

#[async_trait::async_trait]
impl Platform for LocalPlatform {
    fn kind(&self) -> crate::config::OatMode {
        crate::config::OatMode::Local
    }

    fn workload_endpoint(&self, _config: &OatConfig) -> String {
        "http://router:50050".to_owned()
    }

    fn router_metrics_endpoint(&self, _config: &OatConfig) -> Result<String> {
        self.router_metrics_url
            .clone()
            .context("local router metrics endpoint is not ready")
    }

    fn qap_endpoint(&self, config: &OatConfig) -> String {
        self.qap_url.clone().unwrap_or_else(|| config.qap_endpoint.clone())
    }

    async fn ensure_stack(&mut self, config: &OatConfig) -> Result<()> {
        self.daemon_endpoint = Some(local_docker_daemon().await?);
        prepare_local_platform(config, self).await?;
        self.router_metrics_url = Some(local_compose_endpoint(config, self, "router", "50050", "/metrics").await?);
        self.qap_url = Some(local_compose_endpoint(config, self, "kusto", "8080", "").await?);
        Ok(())
    }

    async fn scale_arrowcols(&mut self, config: &OatConfig, replicas: u32, concurrency: u32) -> Result<()> {
        scale_local_arrowcols(config, self, replicas, concurrency).await
    }

    async fn initialize_run_table_pins(
        &self,
        config: &OatConfig,
        run_id: &str,
        expected_keys_json: &str,
        expires_at_unix_ms: u64,
        now_unix_ms: u64,
    ) -> Result<()> {
        run_local_pin_setup(
            config,
            self,
            run_id,
            expected_keys_json,
            expires_at_unix_ms,
            now_unix_ms,
        )
        .await
    }

    fn qap_observation_mode(&self) -> arrowcol_loadgen::ObservationMode {
        arrowcol_loadgen::ObservationMode::Local
    }

    fn qap_database(&self, config: &OatConfig) -> Result<String> {
        Ok(config.qap_database.clone().unwrap_or_else(|| "loadtest".to_owned()))
    }

    async fn initialize_qap(&mut self, config: &OatConfig, run_id: &str, tables: &[String]) -> Result<Vec<String>> {
        let bindings = qap_bindings(run_id, tables)?;
        let local_bindings = bindings
            .iter()
            .map(|binding| {
                Ok((
                    binding.external_table.clone(),
                    format!("/delta/dataflow/telemetry/sqlserver/{}", binding.logical_table),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let results = std::path::PathBuf::from("out/arrowcol-oat/qap").join(run_id);
        std::fs::create_dir_all(&results)
            .with_context(|| format!("create local QAP result directory {}", results.display()))?;
        let database = self.qap_database(config)?;
        arrowcol_loadgen::initialize_local_qap(
            &self.qap_endpoint(config),
            &results,
            &database,
            &local_bindings,
            Duration::from_secs(config.qap_timeout_seconds),
        )
        .await
        .context("initialize run-local Kusto QAP tables and policies")?;
        Ok(bindings.iter().map(|binding| binding.external_table.clone()).collect())
    }

    async fn prepare_client_fleet(&self, _config: &OatConfig, _run_id: &str) -> Result<()> {
        tokio::fs::write(self.control_dir.join("schedule"), "pending\n")
            .await
            .context("reset local OAT client schedule barrier")
    }

    async fn start_client(
        &self,
        config: &OatConfig,
        run_id: &str,
        table: &str,
        client_index: usize,
        args: &[String],
    ) -> Result<ClientHandle> {
        start_local_client(config, self, run_id, table, client_index, args).await
    }

    async fn poll_client(&self, _config: &OatConfig, handle: &ClientHandle) -> Result<ClientProgress> {
        poll_local_client(handle).await
    }

    async fn poll_client_fleet(&self, _config: &OatConfig, handles: &[ClientHandle]) -> Result<Vec<ClientProgress>> {
        poll_local_client_fleet(handles, &self.client_progress).await
    }

    async fn arm_clients(&self, _config: &OatConfig, _run_id: &str, run_start_unix_ms: u64) -> Result<()> {
        let temporary = self.control_dir.join("schedule.tmp");
        tokio::fs::write(&temporary, format!("{run_start_unix_ms}\n"))
            .await
            .context("write local OAT measurement schedule")?;
        tokio::fs::rename(&temporary, self.control_dir.join("schedule"))
            .await
            .context("atomically arm local OAT measurement schedule")
    }

    async fn finish_client(&self, _config: &OatConfig, handle: ClientHandle) -> Result<String> {
        finish_local_client(handle).await
    }

    async fn cancel_client(&self, _config: &OatConfig, handle: &ClientHandle) -> Result<()> {
        cancel_local_client(handle).await
    }

    async fn sample_resources(&self, config: &OatConfig) -> Result<ResourceMetrics> {
        collect_local_metrics(config, self).await
    }

    async fn cleanup(&mut self, config: &OatConfig) -> Result<()> {
        cleanup_local_platform(config, self).await
    }
}

impl LocalPlatform {
    fn new(config: &OatConfig, run_id: &str) -> Result<Self> {
        let state_root = config
            .platform
            .compose_state_root
            .as_deref()
            .context("local compose state root is missing")?;
        let state_dir = std::env::current_dir()?.join(state_root).join(run_id);
        crate::local::absolute_directory(&state_dir)?;
        if let Some(destination) = &config.platform.local.snapshot_root {
            let destination = Path::new(destination);
            crate::local::absolute_directory(destination)?;
            if destination.starts_with(&state_dir) || state_dir.starts_with(destination) {
                bail!("snapshot destination must be separate from generated state");
            }
        }
        if state_dir.exists() {
            bail!(
                "run-scoped local OAT state directory already exists: {}",
                state_dir.display()
            );
        }
        fs::create_dir_all(state_dir.parent().context("local OAT state directory has no parent")?)?;
        fs::create_dir(&state_dir)
            .with_context(|| format!("create isolated local OAT state directory {}", state_dir.display()))?;
        let environment = match prepare_local_state(config, &state_dir, run_id) {
            Ok(environment) => environment,
            Err(error) => {
                return match fs::remove_dir_all(&state_dir) {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "removing incomplete generated state also failed: {cleanup_error}"
                    ))),
                };
            }
        };
        let control_dir = state_dir.join("control");
        Ok(Self {
            run_id: run_id.to_owned(),
            state_dir,
            environment,
            control_dir,
            client_progress: std::sync::Mutex::new(HashMap::new()),
            router_metrics_url: None,
            qap_url: None,
            daemon_endpoint: None,
        })
    }

    fn add_compose_environment(&self, command: &mut Command) {
        command.envs(&self.environment);
    }
}

fn prepare_local_state(config: &OatConfig, state_dir: &Path, run_id: &str) -> Result<HashMap<String, String>> {
    for directory in ["catalog-state", "dataflow", "router-state", "control"] {
        fs::create_dir(state_dir.join(directory))
            .with_context(|| format!("create local OAT state directory {directory}"))?;
    }
    fs::write(state_dir.join("control/schedule"), "pending\n")
        .context("initialize local OAT client schedule barrier")?;
    fs::create_dir(state_dir.join("secrets"))?;
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let compose_path = std::env::current_dir()?.join(compose_file);
    let router_config = compose_path
        .parent()
        .context("local Compose file has no parent directory")?
        .join("config/router.yaml");
    let router_text = fs::read_to_string(&router_config)
        .with_context(|| format!("read local router config from {}", router_config.display()))?;
    let router_text = run_scoped_local_router_config(&router_text, run_id)?;
    fs::write(state_dir.join("router-state/config.yaml"), router_text)
        .context("write run-scoped local OAT router config")?;

    let workspace = std::env::current_dir()?;
    let (uid, gid) = local_owner_ids(&workspace)?;

    let secret_paths = [
        ("SQLSERVER_SA_PASSWORD_FILE", "sqlserver-sa-password"),
        ("ARROWCOL_APP_PASSWORD_FILE", "arrowcol-app-password"),
        ("ARROWCOL_MIGRATOR_PASSWORD_FILE", "arrowcol-migrator-password"),
        ("LOADTEST_EDGE_HMAC_FILE", "edge-hmac"),
        ("LOADTEST_INTERNAL_HMAC_FILE", "internal-hmac"),
    ];
    let mut environment = HashMap::from([
        ("LOADTEST_UID".to_owned(), uid),
        ("LOADTEST_GID".to_owned(), gid),
        (
            "LOADTEST_WORKSPACE".to_owned(),
            workspace.to_string_lossy().into_owned(),
        ),
        (
            "LOADTEST_BIN_HOST_PATH".to_owned(),
            workspace.to_string_lossy().into_owned(),
        ),
        ("ONELAKE_HOST_PATH".to_owned(), state_dir.to_string_lossy().into_owned()),
        ("SQLSERVER_PORT".to_owned(), "0".to_owned()),
        ("LOADTEST_CATALOG_PORT".to_owned(), "0".to_owned()),
        ("GRAFANA_PORT".to_owned(), "0".to_owned()),
        ("LOADTEST_ROUTER_PORT".to_owned(), "0".to_owned()),
        ("KUSTO_PORT".to_owned(), "0".to_owned()),
        ("LOADTEST_DATAFLOW_DELTA_ROOT".to_owned(), "/delta/dataflow".to_owned()),
        ("LOADTEST_TRACES_ENDPOINT".to_owned(), "http://lgtm:4318".to_owned()),
        (
            "LOADTEST_BIND_HOST".to_owned(),
            match config.platform.local.bind_host {
                Some(std::net::IpAddr::V6(host)) => format!("[{host}]"),
                Some(host) => host.to_string(),
                None => "127.0.0.1".to_owned(),
            },
        ),
    ]);
    environment.extend(local_control_mount_environment(state_dir));
    for (variable, name) in secret_paths {
        let path = state_dir.join("secrets").join(name);
        write_private_secret(&path, &random_secret())?;
        environment.insert(variable.to_owned(), path.to_string_lossy().into_owned());
    }
    Ok(environment)
}

fn local_control_mount_environment(state_dir: &Path) -> HashMap<String, String> {
    HashMap::from([(
        "OAT_CONTROL_HOST_PATH".to_owned(),
        state_dir.join("control").to_string_lossy().into_owned(),
    )])
}

fn local_client_compose_args(run_id: &str, container_name: &str, client_args: &[String]) -> Vec<String> {
    let mut args = vec![
        "run".to_owned(),
        "--detach".to_owned(),
        "--no-deps".to_owned(),
        "-T".to_owned(),
        "--name".to_owned(),
        container_name.to_owned(),
        "--env".to_owned(),
        format!("ARROWCOL_OAT_RUN_ID={run_id}"),
        "oat-client".to_owned(),
    ];
    args.extend_from_slice(client_args);
    args
}

async fn start_local_client(
    config: &OatConfig,
    platform: &LocalPlatform,
    run_id: &str,
    table: &str,
    client_index: usize,
    args: &[String],
) -> Result<ClientHandle> {
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let compact_run_id: String = run_id.chars().filter(char::is_ascii_alphanumeric).take(12).collect();
    let safe_table: String = table
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let container_name = format!("oat-client-{compact_run_id}-{safe_table}-c{client_index}");
    let mut command = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "oat",
    );
    platform.add_compose_environment(&mut command);
    command.args(local_client_compose_args(run_id, &container_name, args));
    let output = run_command(command, "start local OAT client container").await?;
    let container_id = String::from_utf8(output.stdout)
        .context("local OAT client container ID was not UTF-8")?
        .trim()
        .to_owned();
    if container_id.is_empty() {
        bail!("Compose returned an empty local OAT client container ID");
    }
    Ok(ClientHandle {
        client_id: format!("{run_id}:{table}:{client_index}"),
        table: table.to_owned(),
        process: ClientProcess::Local { container_id },
    })
}

async fn poll_local_client(handle: &ClientHandle) -> Result<ClientProgress> {
    let ClientProcess::Local { container_id } = &handle.process else {
        bail!("local client poll received a non-local process handle");
    };
    let inspect = run_command(
        {
            let mut command = Command::new("docker");
            command.args([
                "inspect",
                "--format",
                "{{.State.Status}}|{{.State.ExitCode}}",
                container_id,
            ]);
            command
        },
        "inspect local OAT client container",
    )
    .await?;
    let state = String::from_utf8(inspect.stdout).context("local OAT container state was not UTF-8")?;
    let (state, exit_code) = state
        .trim()
        .split_once('|')
        .context("parse local OAT container state")?;
    let exit_code = exit_code
        .parse::<i32>()
        .context("parse local OAT container exit code")?;
    let output = run_command(
        {
            let mut command = Command::new("docker");
            command.args(["logs", container_id]);
            command
        },
        "read local OAT client logs",
    )
    .await?;
    let mut logs = String::from_utf8_lossy(&output.stdout).into_owned();
    logs.push_str(&String::from_utf8_lossy(&output.stderr));
    let completed = state == "exited";
    let failure = (completed && exit_code != 0)
        .then(|| format!("local DMV client {} exited {exit_code}: {logs}", handle.client_id));
    Ok(ClientProgress {
        ready: logs.contains("OAT_CLIENT_READY "),
        schedule_received: logs.contains("OAT_SCHEDULE_RECEIVED "),
        completed,
        failure,
        output: logs,
    })
}

async fn poll_local_client_fleet(
    handles: &[ClientHandle],
    cache: &std::sync::Mutex<HashMap<String, ClientProgress>>,
) -> Result<Vec<ClientProgress>> {
    let mut inspect = Command::new("docker");
    inspect.args(["inspect", "--format", "{{.Id}}|{{.State.Status}}|{{.State.ExitCode}}"]);
    for handle in handles {
        let ClientProcess::Local { container_id } = &handle.process else {
            bail!("local client fleet poll received a non-local process handle");
        };
        inspect.arg(container_id);
    }
    let inspect = run_command(inspect, "inspect local OAT client fleet").await?;
    let inspect = String::from_utf8(inspect.stdout).context("local OAT client fleet state was not UTF-8")?;
    let mut states = HashMap::new();
    for line in inspect.lines() {
        let mut fields = line.split('|');
        let id = fields.next().context("local OAT inspect row has no container ID")?;
        let status = fields.next().context("local OAT inspect row has no container status")?;
        let exit_code = fields
            .next()
            .context("local OAT inspect row has no exit code")?
            .parse::<i32>()
            .context("parse local OAT container exit code")?;
        states.insert(id.to_owned(), (status.to_owned(), exit_code));
    }
    let previous = {
        let cache = cache
            .lock()
            .map_err(|_| anyhow::anyhow!("local OAT client progress cache is poisoned"))?;
        handles
            .iter()
            .map(|handle| cache.get(&handle.client_id).cloned().unwrap_or_default())
            .collect::<Vec<_>>()
    };
    let log_indexes = handles
        .iter()
        .enumerate()
        .filter_map(|(index, handle)| {
            let ClientProcess::Local { container_id } = &handle.process else {
                return None;
            };
            let exited = states.get(container_id).is_some_and(|(status, _)| status == "exited");
            (exited || !previous[index].ready || !previous[index].schedule_received).then_some(index)
        })
        .collect::<Vec<_>>();
    let log_results = join_all(log_indexes.iter().map(|index| async move {
        let container_id = match &handles[*index].process {
            ClientProcess::Local { container_id } => container_id,
            ClientProcess::Aks { .. } => {
                return (
                    *index,
                    Err(anyhow::anyhow!(
                        "local client fleet poll received a non-local process handle"
                    )),
                );
            }
        };
        let mut command = Command::new("docker");
        command.args(["logs", container_id]);
        let result = run_command(command, "read local OAT client logs").await;
        (*index, result)
    }))
    .await;
    let mut logs_by_index = HashMap::with_capacity(log_results.len());
    for (index, result) in log_results {
        let output = result?;
        let mut logs = String::from_utf8_lossy(&output.stdout).into_owned();
        logs.push_str(&String::from_utf8_lossy(&output.stderr));
        logs_by_index.insert(index, logs);
    }
    let mut progress = Vec::with_capacity(handles.len());
    for (index, handle) in handles.iter().enumerate() {
        let ClientProcess::Local { container_id } = &handle.process else {
            bail!("local client fleet poll received a non-local process handle");
        };
        let (status, exit_code) = states
            .iter()
            .find(|(id, _)| id.starts_with(container_id.as_str()) || container_id.starts_with(id.as_str()))
            .map(|(_, state)| state)
            .with_context(|| format!("local OAT client {} is missing from docker inspect", handle.client_id))?;
        let output = logs_by_index
            .remove(&index)
            .unwrap_or_else(|| previous[index].output.clone());
        let completed = status == "exited";
        let current = ClientProgress {
            ready: previous[index].ready || output.contains("OAT_CLIENT_READY "),
            schedule_received: previous[index].schedule_received || output.contains("OAT_SCHEDULE_RECEIVED "),
            completed,
            failure: (completed && *exit_code != 0)
                .then(|| format!("local DMV client {} exited {exit_code}: {output}", handle.client_id)),
            output,
        };
        progress.push(current.clone());
        cache
            .lock()
            .map_err(|_| anyhow::anyhow!("local OAT client progress cache is poisoned"))?
            .insert(handle.client_id.clone(), current);
    }
    Ok(progress)
}

async fn finish_local_client(handle: ClientHandle) -> Result<String> {
    let ClientProcess::Local { container_id } = &handle.process else {
        bail!("local client finish received a non-local process handle");
    };
    let progress = poll_local_client(&handle).await?;
    if let Some(failure) = progress.failure {
        bail!("{failure}");
    }
    if !progress.completed {
        bail!(
            "local OAT client container {} was not complete at finish",
            handle.client_id
        );
    }
    let mut remove = Command::new("docker");
    remove.args(["rm", container_id]);
    run_command(remove, "remove completed local OAT client container").await?;
    Ok(progress.output)
}

async fn cancel_local_client(handle: &ClientHandle) -> Result<()> {
    let ClientProcess::Local { container_id } = &handle.process else {
        bail!("local client cancel received a non-local process handle");
    };
    let mut remove = Command::new("docker");
    remove.args(["rm", "--force", container_id]);
    run_command(remove, "cancel local OAT client container").await?;
    Ok(())
}

fn run_scoped_local_router_config(router_text: &str, run_id: &str) -> Result<String> {
    let router_text = router_text.replacen("lease_ttl_ms: 86400000", "lease_ttl_ms: 3600000", 1);
    if router_text.matches("lease_ttl_ms: 3600000").count() != 1 || router_text.contains("oat_run_id:") {
        bail!("local router config did not contain its expected 1-hour lease TTL");
    }
    let run_id_yaml = serde_json::to_string(run_id)?;
    Ok(router_text.replacen(
        "lease_ttl_ms: 3600000",
        &format!("lease_ttl_ms: 3600000\n  oat_run_id: {run_id_yaml}"),
        1,
    ))
}

#[cfg(unix)]
fn local_owner_ids(workspace: &Path) -> Result<(String, String)> {
    use std::os::unix::fs::MetadataExt;
    let owner = fs::metadata(workspace).context("inspect local workspace ownership")?;
    Ok((owner.uid().to_string(), owner.gid().to_string()))
}

#[cfg(not(unix))]
fn local_owner_ids(_workspace: &Path) -> Result<(String, String)> {
    bail!("local OAT Compose mode requires a Unix devcontainer")
}

fn random_secret() -> String {
    format!(
        "Aa1!{}",
        (0..3)
            .map(|_| uuid::Uuid::new_v4().simple().to_string())
            .collect::<String>()
    )
}

fn write_private_secret(path: &Path, contents: &str) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("create private local OAT secret {}", path.display()))?;
        file.write_all(contents.as_bytes())
            .with_context(|| format!("write local OAT secret {}", path.display()))?;
        file.write_all(b"\n")
            .with_context(|| format!("finish local OAT secret {}", path.display()))?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (path, contents);
        bail!("local OAT secrets require a Unix devcontainer")
    }
}

async fn start_aks_client_job(
    config: &OatConfig,
    platform: &AksPlatform,
    run_id: &str,
    table: &str,
    client_index: usize,
    args: &[String],
) -> Result<ClientHandle> {
    let run_label = run_id
        .chars()
        .filter(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
        .take(8)
        .collect::<String>();
    let table_label = table.to_ascii_lowercase().replace('_', "-");
    let job_name = format!("oat-{run_label}-{table_label}-c{client_index}");
    let run_seconds = i64::try_from(config.timing_budget()?.client_job_lifetime_seconds)
        .context("OAT client Job lifetime exceeds Kubernetes range")?;
    let labels = json!({
        "app": "arrowcol-oat-client",
        "arrowcol-oat-run": run_label,
    });
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": config.namespace,
            "labels": labels,
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": run_seconds,
            "template": {
                "metadata": { "labels": labels },
                "spec": {
                    "restartPolicy": "Never",
                    "terminationGracePeriodSeconds": 10,
                    "automountServiceAccountToken": false,
                    "nodeSelector": { "arrow-collector/role": "client" },
                    "tolerations": [{
                        "key": "arrow-collector/role",
                        "operator": "Equal",
                        "value": "client",
                        "effect": "NoSchedule",
                    }],
                    "topologySpreadConstraints": [{
                        "maxSkew": 1,
                        "topologyKey": "kubernetes.azure.com/agentpool",
                        "whenUnsatisfiable": "ScheduleAnyway",
                        "labelSelector": { "matchLabels": labels },
                    }],
                    "containers": [{
                        "name": "client",
                        "image": config.dmv_client_image,
                        "imagePullPolicy": "IfNotPresent",
                        "args": args,
                        "env": [{
                            "name": "ARROWCOL_OAT_RUN_ID",
                            "value": run_id,
                        }],
                        "resources": {
                            "requests": { "cpu": "250m", "memory": "256Mi" },
                            "limits": { "cpu": "2", "memory": "2Gi" },
                        },
                        "volumeMounts": [{
                            "name": "edge-hmac",
                            "mountPath": "/run/secrets/edge-hmac",
                            "subPath": "edge-hmac",
                            "readOnly": true,
                        }, {
                            "name": "oat-schedule",
                            "mountPath": "/oat-schedule",
                            "readOnly": true,
                        }],
                    }],
                    "volumes": [{
                        "name": "edge-hmac",
                        "secret": {
                            "secretName": config.edge_secret_name,
                            "items": [{ "key": "edge-hmac", "path": "edge-hmac" }],
                        },
                    }, {
                        "name": "oat-schedule",
                        "configMap": {
                            "name": platform.schedule_configmap_name,
                            "items": [{ "key": "schedule", "path": "schedule" }],
                        },
                    }],
                },
            },
        },
    });
    let payload = serde_json::to_vec(&job)?;
    kubectl(
        config,
        vec!["apply", "--namespace", &config.namespace, "-f", "-"],
        Some(payload),
    )
    .await
    .with_context(|| format!("create AKS DMV client Job {job_name}"))?;
    Ok(ClientHandle {
        client_id: format!("{run_id}:{table}:{client_index}"),
        table: table.to_owned(),
        process: ClientProcess::Aks { job_name },
    })
}

async fn poll_aks_client_job(config: &OatConfig, handle: &ClientHandle) -> Result<ClientProgress> {
    let ClientProcess::Aks { job_name } = &handle.process else {
        bail!("AKS client poll received a non-AKS process handle");
    };
    let job = kubectl(
        config,
        vec!["get", "job", job_name, "--namespace", &config.namespace, "-o", "json"],
        None,
    )
    .await
    .with_context(|| format!("inspect AKS DMV client Job {job_name}"))?;
    let job: Value = serde_json::from_str(&job).context("parse AKS DMV client Job status")?;
    let succeeded = job.pointer("/status/succeeded").and_then(Value::as_u64).unwrap_or(0) > 0;
    let failed = job.pointer("/status/failed").and_then(Value::as_u64).unwrap_or(0) > 0
        || job
            .pointer("/status/conditions")
            .and_then(Value::as_array)
            .is_some_and(|conditions| {
                conditions.iter().any(|condition| {
                    condition.get("type").and_then(Value::as_str) == Some("Failed")
                        && condition.get("status").and_then(Value::as_str) == Some("True")
                })
            });
    let logs_result = kubectl(
        config,
        vec!["logs", "--namespace", &config.namespace, &format!("job/{job_name}")],
        None,
    )
    .await;
    let output = match logs_result {
        Ok(output) => output,
        Err(error) if succeeded || failed => {
            return Err(error).with_context(|| format!("read completed AKS client Job {job_name} logs"));
        }
        Err(_) => String::new(),
    };
    let ready = output.contains("OAT_CLIENT_READY ");
    let schedule_received = output.contains("OAT_SCHEDULE_RECEIVED ");
    let failure = failed.then(|| format!("AKS DMV client Job {job_name} failed: {output}"));
    Ok(ClientProgress {
        ready,
        schedule_received,
        completed: succeeded,
        failure,
        output,
    })
}

async fn poll_aks_client_fleet(
    config: &OatConfig,
    platform: &AksPlatform,
    handles: &[ClientHandle],
) -> Result<Vec<ClientProgress>> {
    let run_label: String = platform
        .run_id
        .chars()
        .filter(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
        .take(8)
        .collect();
    let selector = format!("app=arrowcol-oat-client,arrowcol-oat-run={run_label}");
    let jobs_output = kubectl(
        config,
        vec![
            "get",
            "jobs",
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
    .context("inspect AKS OAT client fleet Jobs")?;
    let jobs: Value = serde_json::from_str(&jobs_output).context("parse AKS OAT client fleet Job statuses")?;
    let items = jobs
        .get("items")
        .and_then(Value::as_array)
        .context("AKS OAT client Job list has no items array")?;
    let mut states = HashMap::with_capacity(items.len());
    for job in items {
        let Some(name) = job.pointer("/metadata/name").and_then(Value::as_str) else {
            bail!("AKS OAT client Job has no metadata.name");
        };
        let succeeded = job.pointer("/status/succeeded").and_then(Value::as_u64).unwrap_or(0) > 0;
        let failed = job.pointer("/status/failed").and_then(Value::as_u64).unwrap_or(0) > 0
            || job
                .pointer("/status/conditions")
                .and_then(Value::as_array)
                .is_some_and(|conditions| {
                    conditions.iter().any(|condition| {
                        condition.get("type").and_then(Value::as_str) == Some("Failed")
                            && condition.get("status").and_then(Value::as_str) == Some("True")
                    })
                });
        states.insert(name.to_owned(), (succeeded, failed));
    }
    let logs_result = kubectl(
        config,
        vec![
            "logs",
            "--namespace",
            &config.namespace,
            "--selector",
            &selector,
            "--all-containers=true",
            "--prefix=true",
            "--tail=-1",
            "--ignore-errors=true",
        ],
        None,
    )
    .await;
    let logs = match logs_result {
        Ok(logs) => logs,
        Err(error) if states.values().any(|(succeeded, failed)| *succeeded || *failed) => {
            return Err(error).context("read completed AKS client fleet logs");
        }
        Err(_) => String::new(),
    };
    let mut progress = Vec::with_capacity(handles.len());
    for handle in handles {
        let ClientProcess::Aks { job_name } = &handle.process else {
            bail!("AKS client fleet poll received a non-AKS process handle");
        };
        let (completed, failed) = states.get(job_name).copied().unwrap_or_default();
        let output = logs
            .lines()
            .filter(|line| line.contains(job_name))
            .collect::<Vec<_>>()
            .join("\n");
        progress.push(ClientProgress {
            ready: output.contains("OAT_CLIENT_READY "),
            schedule_received: output.contains("OAT_SCHEDULE_RECEIVED "),
            completed,
            failure: failed.then(|| format!("AKS DMV client Job {job_name} failed: {output}")),
            output,
        });
    }
    Ok(progress)
}

async fn finish_aks_client_job(config: &OatConfig, handle: ClientHandle) -> Result<String> {
    let ClientProcess::Aks { job_name } = &handle.process else {
        bail!("AKS client finish received a non-AKS process handle");
    };
    let progress = poll_aks_client_job(config, &handle).await?;
    if let Some(failure) = progress.failure {
        bail!("{failure}");
    }
    if !progress.completed {
        bail!("AKS DMV client Job {job_name} was not complete at finish");
    }
    kubectl(
        config,
        vec![
            "delete",
            "job",
            job_name,
            "--namespace",
            &config.namespace,
            "--ignore-not-found",
            "--wait=true",
            "--timeout=30s",
        ],
        None,
    )
    .await
    .context("delete completed AKS DMV client Job")?;
    Ok(progress.output)
}

async fn cancel_aks_client_job(config: &OatConfig, handle: &ClientHandle) -> Result<()> {
    let ClientProcess::Aks { job_name } = &handle.process else {
        bail!("AKS client cancel received a non-AKS process handle");
    };
    kubectl(
        config,
        vec![
            "delete",
            "job",
            job_name,
            "--namespace",
            &config.namespace,
            "--ignore-not-found",
            "--wait=true",
            "--timeout=30s",
        ],
        None,
    )
    .await
    .with_context(|| format!("cancel AKS DMV client Job {job_name}"))?;
    Ok(())
}

/// Runs the configured benchmark matrix and publishes its compact results.
///
/// # Errors
///
/// Returns an error if configuration, Kubernetes operations, measurement, restoration,
/// or result publication fails.
pub async fn execute(
    config: OatConfig,
    id: &str,
    state: Arc<AppState>,
    cancellation: watch::Receiver<bool>,
) -> Result<usize> {
    config.validate()?;
    state
        .update(id, |status| {
            status.state = RunState::Running;
            status.progress = "preparing benchmark platform".to_owned();
            status.started_at_unix_ms = unix_time_ms();
        })
        .await;

    let outcome = run_matrix(&config, id, Arc::clone(&state), cancellation).await;

    state
        .update(id, |status| {
            status.progress = "writing compact Parquet results".to_owned()
        })
        .await;
    let result_count = outcome.rows.len();
    publish_matrix_outcome(&config, outcome).await?;
    Ok(result_count)
}

struct MatrixOutcome {
    rows: Vec<OatResult>,
    error: Option<anyhow::Error>,
}

async fn publish_matrix_outcome(config: &OatConfig, outcome: MatrixOutcome) -> Result<()> {
    let publication = publish_results(config, &outcome.rows).await;
    match (outcome.error, publication) {
        (None, result) => result,
        (Some(error), Ok(())) => Err(error),
        (Some(error), Err(publication_error)) => Err(error.context(format!(
            "publishing retained matrix evidence also failed: {publication_error:#}"
        ))),
    }
}

async fn run_matrix(
    config: &OatConfig,
    orchestration_id: &str,
    state: Arc<AppState>,
    cancellation: watch::Receiver<bool>,
) -> MatrixOutcome {
    run_matrix_with(
        config,
        orchestration_id,
        &state,
        move |cell_config, run_id, replicas, concurrency| {
            run_isolated_cell(
                cell_config,
                orchestration_id,
                run_id,
                replicas,
                concurrency,
                cancellation.clone(),
            )
        },
    )
    .await
}

async fn run_matrix_with<F, Fut>(
    config: &OatConfig,
    orchestration_id: &str,
    state: &AppState,
    mut run_cell: F,
) -> MatrixOutcome
where
    F: FnMut(OatConfig, String, u32, u32) -> Fut,
    Fut: std::future::Future<Output = MatrixOutcome>,
{
    let mut results = Vec::new();
    let cells = matrix_cell_specs(config);
    let total = cells.len();
    for (completed, (replicas, concurrency, run_id)) in cells.into_iter().enumerate() {
        state
            .update(orchestration_id, |status| {
                status.progress = format!(
                    "experiment {}/{total}: ArrowCol replicas={replicas}, DFE --num-cores={concurrency}",
                    completed + 1
                );
            })
            .await;
        let mut cell = match config_for_run(config, &run_id) {
            Ok(cell_config) => run_cell(cell_config, run_id.clone(), replicas, concurrency).await,
            Err(error) => finish_matrix_cell(
                config,
                orchestration_id,
                &run_id,
                replicas,
                concurrency,
                Err(error),
                Ok(()),
            ),
        };
        results.append(&mut cell.rows);
        if cell.error.is_some() {
            return MatrixOutcome {
                rows: results,
                error: cell.error,
            };
        }
    }
    MatrixOutcome {
        rows: results,
        error: None,
    }
}

async fn run_isolated_cell(
    config: OatConfig,
    orchestration_id: &str,
    run_id: String,
    replicas: u32,
    concurrency: u32,
    mut cancellation: watch::Receiver<bool>,
) -> MatrixOutcome {
    let mut platform = match platform_for_run(&config, &run_id) {
        Ok(platform) => platform,
        Err(error) => {
            return finish_matrix_cell(
                &config,
                orchestration_id,
                &run_id,
                replicas,
                concurrency,
                Err(error),
                Ok(()),
            );
        }
    };
    let result = tokio::select! {
        result = async {
            match platform.ensure_stack(&config).await {
                Ok(()) => {
                    run_matrix_cell(
                        &config,
                        orchestration_id,
                        &run_id,
                        replicas,
                        concurrency,
                        platform.as_mut(),
                    )
                    .await
                }
                Err(error) => Err(error).context("prepare isolated OAT matrix-cell platform"),
            }
        } => result,
        _ = wait_for_cancellation(&mut cancellation) => Err(anyhow::anyhow!("local OAT run cancelled")),
    };
    let cleanup = platform.cleanup(&config).await;
    finish_matrix_cell(
        &config,
        orchestration_id,
        &run_id,
        replicas,
        concurrency,
        result,
        cleanup,
    )
}

async fn wait_for_cancellation(cancellation: &mut watch::Receiver<bool>) {
    loop {
        if *cancellation.borrow() {
            return;
        }
        if cancellation.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

fn finish_matrix_cell(
    config: &OatConfig,
    orchestration_id: &str,
    run_id: &str,
    replicas: u32,
    concurrency: u32,
    result: Result<Vec<OatResult>>,
    cleanup: Result<()>,
) -> MatrixOutcome {
    let (mut rows, mut error) = match result {
        Ok(rows) => {
            let error = rows
                .iter()
                .find_map(|row| row.failure.as_deref())
                .map(|error| anyhow::anyhow!("{error}"));
            (rows, error)
        }
        Err(error) => {
            let rows = (|| {
                let started_at_ms =
                    i64::try_from(unix_time_ms()).context("failed cell timestamp exceeds Arrow timestamp range")?;
                result_rows(
                    config,
                    orchestration_id,
                    run_id,
                    replicas,
                    concurrency,
                    CellMeasurements {
                        clients: ClientRun {
                            metrics: HashMap::new(),
                            pinning: PinningReport::default(),
                            qap_tables: Vec::new(),
                            failure: Some(format!("{error:#}")),
                        },
                        started_at_ms,
                        qap: QapMetrics::default(),
                        resources: None,
                    },
                )
            })();
            match rows {
                Ok(rows) => (rows, Some(error)),
                Err(record_error) => (
                    Vec::new(),
                    Some(error.context(format!("recording failed cell also failed: {record_error:#}"))),
                ),
            }
        }
    };
    if let Err(cleanup_error) = cleanup {
        let detail = format!("clean up isolated matrix-cell platform: {cleanup_error:#}");
        error = Some(match error {
            Some(error) => error.context(detail.clone()),
            None => anyhow::anyhow!("{detail}"),
        });
        for row in &mut rows {
            row.status = if row.pinning_error.is_some() {
                "pinning_violation"
            } else {
                "failed"
            }
            .to_owned();
            let failure = row
                .failure
                .as_ref()
                .map_or_else(|| detail.clone(), |failure| format!("{failure}; {detail}"));
            row.failure = Some(failure);
            // Public Parquet carries fixed categories, never raw command output.
            match serde_json::from_str::<Value>(&row.tunables_json) {
                Ok(mut tunables) => {
                    tunables["cleanup_failed"] = Value::Bool(true);
                    row.tunables_json = tunables.to_string();
                }
                Err(cause) => {
                    if let Some(original) = error.take() {
                        error = Some(original.context(format!(
                            "attaching cleanup failure to result tunables also failed: {cause}"
                        )));
                    }
                }
            }
        }
    }
    MatrixOutcome { rows, error }
}

fn matrix_cell_specs(config: &OatConfig) -> Vec<(u32, u32, String)> {
    config
        .replica_counts
        .iter()
        .flat_map(|replicas| {
            config
                .dfe_worker_concurrency
                .iter()
                .map(move |concurrency| (*replicas, *concurrency, uuid::Uuid::new_v4().to_string()))
        })
        .collect()
}

fn platform_for_run(config: &OatConfig, run_id: &str) -> Result<Box<dyn Platform>> {
    match config.platform.kind {
        crate::config::OatMode::Local => Ok(Box::new(LocalPlatform::new(config, run_id)?)),
        crate::config::OatMode::Aks => Ok(Box::new(AksPlatform {
            run_id: run_id.to_owned(),
            namespace_created: false,
            namespace_create_attempted: false,
            namespace_owner_token: uuid::Uuid::new_v4().to_string(),
            namespace_uid: None,
            runtime_dir: None,
            qap_relative_data_root: None,
            router_metrics_url: None,
            router_tunnel: None,
            fic_record_path: None,
            fic_ready_snapshot_path: None,
            fic_prepare_attempted: false,
            qap_record_path: None,
            qap_bind_attempted: false,
            qap_outcome_unresolved: false,
            workload_stop_record_path: None,
            schedule_configmap_name: format!(
                "oat-schedule-{}",
                run_id
                    .chars()
                    .filter(|character| character.is_ascii_alphanumeric())
                    .take(16)
                    .collect::<String>()
            ),
            service_account: None,
            router_yaml: None,
        })),
    }
}

fn config_for_run(config: &OatConfig, run_id: &str) -> Result<OatConfig> {
    let mut cell_config = config.clone();
    if cell_config.platform.kind == crate::config::OatMode::Aks {
        cell_config.namespace = scoped_aks_namespace(&config.namespace, run_id)?;
        cell_config.arrowcol_endpoint = format!(
            "http://arrowcol-router.{}.svc.cluster.local:50050",
            cell_config.namespace
        );
        cell_config.router_metrics_endpoint = format!(
            "http://arrowcol-router.{}.svc.cluster.local:50050/metrics",
            cell_config.namespace
        );
    }
    cell_config.validate()?;
    Ok(cell_config)
}

fn expected_pin_keys_json(tables: &[String]) -> Result<String> {
    let available = collector_chain::dmv::tables()?;
    let mut keys = Vec::with_capacity(tables.len());
    let mut lease_keys = Vec::with_capacity(tables.len());
    for table in tables {
        let definition = available
            .iter()
            .find(|candidate| candidate.identity().table().as_str().eq_ignore_ascii_case(table))
            .with_context(|| format!("DMV table {table} has no canonical identity"))?;
        let identity = definition.identity();
        keys.push((
            "dataflow",
            identity.catalog().as_str(),
            identity.schema().as_str(),
            identity.table().as_str(),
        ));
        lease_keys.push(LeaseKey::new(ImplementationPool::Dataflow, (*identity).clone()));
    }
    keys.sort_unstable();
    lease_keys.sort();
    if keys.windows(2).any(|pair| pair[0] == pair[1]) {
        bail!("OAT pin set contains duplicate canonical table identities");
    }
    let values = keys
        .into_iter()
        .map(|(pool, catalog, schema, table)| {
            json!({
                "implementation_pool": pool,
                "catalog_name": catalog,
                "schema_name": schema,
                "table_name": table,
            })
        })
        .collect::<Vec<_>>();
    let expected_digest = run_table_pin_set_digest(&lease_keys)?;
    serde_json::to_string(&json!({
        "expected_keys": values,
        "expected_digest": expected_digest,
    }))
    .context("serialize canonical OAT table pin set")
}

#[derive(serde::Deserialize)]
struct RouterYaml {
    routing: RouterRoutingYaml,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RouterRoutingYaml {
    #[serde(rename = "refresh_interval_ms")]
    _refresh_interval_ms: u64,
    max_snapshot_age_ms: u64,
    max_future_skew_ms: u64,
    max_table_entries: usize,
    max_oldest_buffer_age_ms: u64,
    hard_pressure_bps: u16,
    warmup_ms: u64,
    minimum_warmup_bps: u16,
    lease_ttl_ms: u64,
    #[serde(default = "default_oat_run_pin_ttl_ms")]
    oat_run_pin_ttl_ms: u64,
    #[serde(default)]
    oat_run_id: Option<String>,
    readiness_max_staleness_ms: u64,
    #[serde(default)]
    distinct_table_owners: bool,
}

fn default_oat_run_pin_ttl_ms() -> u64 {
    3_600_000
}

fn routing_policy_from_yaml(router_yaml: &str, run_id: &str) -> Result<RoutingPolicy> {
    let router: RouterYaml = serde_yaml::from_str(router_yaml).context("parse exact rendered router routing config")?;
    let routing = router.routing;
    let policy = RoutingPolicy {
        pressure: PressurePolicy {
            max_snapshot_age_ms: routing.max_snapshot_age_ms,
            max_future_skew_ms: routing.max_future_skew_ms,
            max_table_entries: routing.max_table_entries,
            max_oldest_buffer_age_ms: routing.max_oldest_buffer_age_ms,
        },
        hard_pressure_bps: routing.hard_pressure_bps,
        warmup_ms: routing.warmup_ms,
        minimum_warmup_bps: routing.minimum_warmup_bps,
        lease_ttl_ms: routing.lease_ttl_ms,
        oat_run_pin_ttl_ms: routing.oat_run_pin_ttl_ms,
        readiness_max_staleness_ms: routing.readiness_max_staleness_ms,
        distinct_table_owners: routing.distinct_table_owners,
        oat_run_id: routing.oat_run_id,
    };
    if policy.oat_run_id.as_deref() != Some(run_id) {
        bail!("rendered router policy does not authorize the exact OAT run ID");
    }
    policy
        .validate()
        .map_err(anyhow::Error::msg)
        .context("validate rendered router routing policy")?;
    Ok(policy)
}

fn rendered_router_yaml(resources: &[Value]) -> Result<String> {
    let mut matches = resources.iter().filter(|resource| {
        resource.get("kind").and_then(Value::as_str) == Some("ConfigMap")
            && resource.pointer("/metadata/name").and_then(Value::as_str) == Some("arrowcol-config")
    });
    let config = matches
        .next()
        .context("rendered AKS workload has no arrowcol-config ConfigMap")?;
    if matches.next().is_some() {
        bail!("rendered AKS workload contains multiple arrowcol-config ConfigMaps");
    }
    config
        .pointer("/data/router.yaml")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .context("rendered AKS arrowcol-config has no router.yaml")
}

async fn run_matrix_cell(
    config: &OatConfig,
    orchestration_id: &str,
    run_id: &str,
    replicas: u32,
    concurrency: u32,
    platform: &mut dyn Platform,
) -> Result<Vec<OatResult>> {
    platform.scale_arrowcols(config, replicas, concurrency).await?;
    let assigned_tables = config.tables.clone();
    let expected_keys_json = expected_pin_keys_json(&assigned_tables)?;
    let pin_now_unix_ms = unix_time_ms_u64()?;
    let pin_expires_at_unix_ms = pin_now_unix_ms
        .checked_add(config.run_pin_ttl_ms)
        .context("OAT run pin expiry overflow")?;
    platform
        .initialize_run_table_pins(
            config,
            run_id,
            &expected_keys_json,
            pin_expires_at_unix_ms,
            pin_now_unix_ms,
        )
        .await?;
    let started_at_ms = i64::try_from(unix_time_ms()).context("run start timestamp exceeds Arrow timestamp range")?;
    let mut client_run = run_clients(config, run_id, &assigned_tables, platform).await?;
    let pinning_ok = client_run.pinning.violation.is_none()
        && client_run.pinning.verification_error.is_none()
        && client_run.failure.is_none();
    let mut qap = QapMetrics::default();
    let mut metrics = None;
    if pinning_ok {
        let observation = async {
            qap = observe_qap(
                &platform.qap_endpoint(config),
                &platform.qap_observation_mode(),
                &platform.qap_database(config)?,
                run_id,
                &client_run.qap_tables,
                config.qap_timeout_seconds,
            )
            .await?;
            metrics = Some(platform.sample_resources(config).await?);
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = observation {
            client_run.failure = Some(format!("observe completed matrix cell: {error:#}"));
        }
    }
    result_rows(
        config,
        orchestration_id,
        run_id,
        replicas,
        concurrency,
        CellMeasurements {
            clients: client_run,
            started_at_ms,
            qap,
            resources: metrics,
        },
    )
}

struct CellMeasurements {
    clients: ClientRun,
    started_at_ms: i64,
    qap: QapMetrics,
    resources: Option<ResourceMetrics>,
}

fn result_rows(
    config: &OatConfig,
    orchestration_id: &str,
    run_id: &str,
    replicas: u32,
    concurrency: u32,
    measurements: CellMeasurements,
) -> Result<Vec<OatResult>> {
    let CellMeasurements {
        clients: client_run,
        started_at_ms,
        qap,
        resources: metrics,
    } = measurements;
    let assigned_tables = config.tables.clone();
    let client_count = config
        .clients_per_table
        .checked_mul(assigned_tables.len())
        .context("OAT client count overflow")?;
    let observed_owners = client_run.pinning.owners.values().collect::<HashSet<_>>().len();
    let tunables_json = serde_json::to_string(&json!({
        "arrowcols": replicas,
        "clients": client_count,
        "clients_per_table": config.clients_per_table,
        "dfe_worker_concurrency": concurrency,
        "dmv_concurrency": config.dmv_concurrency,
        "idle_arrowcols": replicas.saturating_sub(observed_owners as u32),
        "measure_seconds": config.measure_seconds,
        "orchestration_run_id": orchestration_id,
        "pilot_mode": config.pilot_mode,
        "run_pin_ttl_ms": config.run_pin_ttl_ms,
        "rows_per_table": config.rows_per_table,
        "tables": &assigned_tables,
        "warmup_seconds": config.warmup_seconds,
        "failure": client_run.failure.as_ref().map(|_| "measurement_or_observation_failed"),
        "pinning_violation": client_run.pinning.violation.is_some(),
        "pinning_verification_error": client_run.pinning.verification_error.as_ref().map(|_| "ownership_verification_failed"),
    }))?;
    let details = [
        client_run.failure.clone(),
        client_run
            .pinning
            .violation
            .as_ref()
            .map(|error| format!("pinning_violation: {error}")),
        client_run.pinning.verification_error.clone(),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    let failure = (!details.is_empty()).then(|| details.join("; "));
    let pinning_ok = client_run.pinning.violation.is_none()
        && client_run.pinning.verification_error.is_none()
        && assigned_tables.iter().all(|table| {
            client_run.metrics.contains_key(table)
                && client_run.pinning.owners.contains_key(&canonical_table_key(table))
        });
    let mut rows = Vec::with_capacity(assigned_tables.len());
    for table in assigned_tables {
        let client = client_run.metrics.get(&table);
        if client.is_none() && failure.is_none() {
            bail!("DMV client did not report measurements for {table}");
        }
        let owner = client_run.pinning.owners.get(&canonical_table_key(&table));
        rows.push(OatResult {
            run_id: run_id.to_owned(),
            mode: match config.platform.kind {
                crate::config::OatMode::Local => "local",
                crate::config::OatMode::Aks => "aks",
            }
            .to_owned(),
            stamp: config.stamp.clone(),
            host: platform_host_label(config.platform.kind).to_owned(),
            experiment: format!("arrowcols-{replicas}-dfe-{concurrency}"),
            started_at_ms,
            measure_seconds: config.measure_seconds as f64,
            arrowcols: i32::try_from(replicas).context("ArrowCol count exceeds Parquet schema range")?,
            clients: i32::try_from(client_count).context("client count exceeds Parquet schema range")?,
            tables: i32::try_from(config.tables.len()).context("table count exceeds Parquet schema range")?,
            table_owner: owner.cloned(),
            pinning_ok,
            pinning_error: client_run.pinning.violation.clone(),
            failure: failure.clone(),
            status: if client_run.pinning.violation.is_some() {
                "pinning_violation"
            } else if failure.is_some() {
                "failed"
            } else {
                "succeeded"
            }
            .to_owned(),
            git_sha: std::env::var("GIT_SHA")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "unknown".to_owned()),
            tunables_json: tunables_json.clone(),
            replicas: u64::from(replicas),
            dfe_worker_concurrency: u64::from(concurrency),
            table,
            rows: client.map(|client| client.rows),
            duration_ms: client.map(|client| client.duration_ms),
            rows_per_second: client.map(|client| client.rows_per_second),
            cpu_millicores: metrics.map(|metrics| metrics.cpu_millicores),
            memory_bytes: metrics.map(|metrics| metrics.memory_bytes),
            qap,
        });
    }
    Ok(rows)
}

#[derive(Clone, Debug)]
struct ClientMetrics {
    rows: u64,
    duration_ms: u64,
    rows_per_second: f64,
}

struct ClientMeasurement {
    metrics: ClientMetrics,
    pilot_windows: Vec<PilotWindow>,
}

#[derive(Clone, Copy, Debug)]
struct PilotWindow {
    index: u64,
    rows: u64,
    start_unix_ms: u64,
}

struct ClientRun {
    metrics: HashMap<String, ClientMetrics>,
    pinning: PinningReport,
    qap_tables: Vec<String>,
    failure: Option<String>,
}

#[derive(Default)]
struct PinningReport {
    owners: HashMap<String, String>,
    violation: Option<String>,
    verification_error: Option<String>,
}

async fn run_clients(
    config: &OatConfig,
    project_run_id: &str,
    tables: &[String],
    platform: &mut dyn Platform,
) -> Result<ClientRun> {
    let mut pinning = PinningReport::default();
    match run_clients_inner(config, project_run_id, tables, platform, &mut pinning).await {
        Ok(report) => Ok(report),
        Err(error) => {
            let sample = async {
                let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
                let endpoint = platform.router_metrics_endpoint(config)?;
                sample_table_owners(&client, &endpoint, tables).await
            }
            .await;
            let pinning = failed_client_pinning_report(&error, sample, tables, pinning);
            tracing::error!(run_id = project_run_id, error = %error, "OAT client run failed; incomplete measurements are unavailable");
            Ok(ClientRun {
                metrics: HashMap::new(),
                pinning,
                qap_tables: Vec::new(),
                failure: Some(format!("{error:#}")),
            })
        }
    }
}

fn failed_client_pinning_report(
    error: &anyhow::Error,
    sample: Result<HashMap<String, Vec<String>>>,
    tables: &[String],
    mut report: PinningReport,
) -> PinningReport {
    let detail = format!("{error:#}");
    record_owner_observation(&mut report, sample, tables, true);
    if detail.contains("pinning_violation") {
        report.violation.get_or_insert(detail);
    }
    report
}

fn record_owner_observation(
    report: &mut PinningReport,
    sample: Result<HashMap<String, Vec<String>>>,
    tables: &[String],
    require_owner: bool,
) {
    match sample {
        Ok(sample) => {
            let violation = check_owner_sample(tables, &sample, &mut report.owners).or_else(|| {
                require_owner
                    .then(|| {
                        tables
                            .iter()
                            .find(|table| !report.owners.contains_key(&canonical_table_key(table)))
                            .map(|table| format!("table {table} has no active ArrowCol owner"))
                    })
                    .flatten()
            });
            if let Some(violation) = violation {
                report.violation.get_or_insert(violation);
            }
        }
        Err(error) => {
            let detail = format!("{error:#}");
            if detail.contains("pinning_violation") {
                report.violation.get_or_insert(detail);
            } else {
                report
                    .verification_error
                    .get_or_insert_with(|| format!("ownership verification failed: {detail}"));
            }
        }
    }
}

async fn run_clients_inner(
    config: &OatConfig,
    project_run_id: &str,
    tables: &[String],
    platform: &mut dyn Platform,
    pinning: &mut PinningReport,
) -> Result<ClientRun> {
    let timing_budget = config.timing_budget()?;
    let metrics_endpoint = platform.router_metrics_endpoint(config)?;
    let metrics_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("create router metrics client")?;
    let readiness_started = tokio::time::Instant::now();
    platform.prepare_client_fleet(config, project_run_id).await?;
    let schedule_path = match platform.kind() {
        crate::config::OatMode::Local => "/oat-control/schedule",
        crate::config::OatMode::Aks => "/oat-schedule/schedule",
    };
    let mut handles = Vec::with_capacity(tables.len().saturating_mul(config.clients_per_table));
    for table in tables {
        for client_index in 0..config.clients_per_table {
            let mut arguments = vec![
                "--endpoint".to_owned(),
                platform.workload_endpoint(config),
                "--client-id".to_owned(),
                format!("oat-{project_run_id}-{table}-c{client_index}"),
                "--rows".to_owned(),
                config.rows_per_table.to_string(),
                "--concurrency".to_owned(),
                config.dmv_concurrency.to_string(),
                "--table".to_owned(),
                table.clone(),
                "--edge-secret-file".to_owned(),
                if platform.kind() == crate::config::OatMode::Local {
                    "/run/secrets/edge-hmac".to_owned()
                } else {
                    config.edge_secret_file.clone()
                },
                "--warmup-secs".to_owned(),
                config.warmup_seconds.to_string(),
                "--duration-secs".to_owned(),
                config.measure_seconds.to_string(),
                "--schedule-file".to_owned(),
                schedule_path.to_owned(),
                "--schedule-timeout-secs".to_owned(),
                timing_budget.schedule_wait_seconds.to_string(),
            ];
            if config.pilot_mode {
                arguments.push("--pilot".to_owned());
            }
            match platform
                .start_client(config, project_run_id, table, client_index + 1, &arguments)
                .await
            {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    let cleanup = cancel_clients(platform, config, &handles).await;
                    return match cleanup {
                        Ok(()) => Err(error).context("start complete OAT client fleet"),
                        Err(cleanup_error) => Err(error.context(format!(
                            "cancelling partially started OAT client fleet also failed: {cleanup_error:#}"
                        ))),
                    };
                }
            }
        }
    }

    loop {
        let progress = match poll_clients(platform, config, &handles).await {
            Ok(progress) => progress,
            Err(error) => {
                let cleanup = cancel_clients(platform, config, &handles).await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "cancelling clients after readiness polling failed also failed: {cleanup_error:#}"
                    ))),
                };
            }
        };
        match validate_client_readiness(
            &progress,
            readiness_started.elapsed(),
            Duration::from_secs(config.client_ready_timeout_seconds),
        ) {
            Ok(true) => break,
            Ok(false) => {}
            Err(error) => {
                let cleanup = cancel_clients(platform, config, &handles).await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "cancelling clients after readiness failure also failed: {cleanup_error:#}"
                    ))),
                };
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Delta metadata is committed during preflight, so QAP binding must precede arming.
    let qap_result = tokio::time::timeout(
        Duration::from_secs(config.qap_timeout_seconds),
        platform.initialize_qap(config, project_run_id, tables),
    )
    .await;
    let qap_result = match qap_result {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!(
            "run-scoped QAP preparation exceeded qap_timeout_seconds; operation outcome may be indeterminate and is not retried"
        )),
    };
    let qap_tables = match qap_result {
        Ok(qap_tables) => qap_tables,
        Err(error) => {
            platform.mark_qap_outcome_unresolved();
            let cleanup = cancel_clients(platform, config, &handles).await;
            return match cleanup {
                Ok(()) => Err(error).context("initialize run-scoped QAP before client schedule"),
                Err(cleanup_error) => Err(error.context(format!(
                    "cancelling prepared OAT clients after QAP initialization failure also failed: {cleanup_error:#}"
                ))),
            };
        }
    };
    let schedule_deadline =
        tokio::time::Instant::now() + Duration::from_secs(crate::config::SCHEDULE_DELIVERY_TIMEOUT_SECONDS);
    let run_start_unix_ms = unix_time_ms_u64()?
        .checked_add(crate::config::SCHEDULE_DELIVERY_TIMEOUT_SECONDS.saturating_mul(1_000))
        .context("shared OAT run start timestamp overflow")?;
    if let Err(error) = platform.arm_clients(config, project_run_id, run_start_unix_ms).await {
        let cleanup = cancel_clients(platform, config, &handles).await;
        return match cleanup {
            Ok(()) => Err(error).context("arm common OAT client schedule"),
            Err(cleanup_error) => Err(error.context(format!(
                "cancelling clients after schedule-arm failure also failed: {cleanup_error:#}"
            ))),
        };
    }

    let mut schedule_acknowledged = false;
    while tokio::time::Instant::now() < schedule_deadline {
        let progress = match poll_clients(platform, config, &handles).await {
            Ok(progress) => progress,
            Err(error) => {
                let cleanup = cancel_clients(platform, config, &handles).await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "cancelling clients after schedule polling failed also failed: {cleanup_error:#}"
                    ))),
                };
            }
        };
        match validate_client_schedule(&progress, unix_time_ms_u64()?, run_start_unix_ms) {
            Ok(true) => {
                schedule_acknowledged = true;
                break;
            }
            Ok(false) => {}
            Err(error) => {
                let cleanup = cancel_clients(platform, config, &handles).await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "cancelling clients after schedule failure also failed: {cleanup_error:#}"
                    ))),
                };
            }
        }
        record_owner_observation(
            pinning,
            sample_table_owners(&metrics_client, &metrics_endpoint, tables).await,
            tables,
            false,
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    if !schedule_acknowledged {
        let error =
            anyhow::anyhow!("OAT client schedule acknowledgement timed out or the final armed window was missed");
        let cleanup = cancel_clients(platform, config, &handles).await;
        return match cleanup {
            Ok(()) => Err(error),
            Err(cleanup_error) => Err(error.context(format!(
                "cancelling clients after schedule acknowledgement failure also failed: {cleanup_error:#}"
            ))),
        };
    }
    let measurement_start_unix_ms = run_start_unix_ms
        .checked_add(config.warmup_seconds.saturating_mul(1_000))
        .context("shared OAT measurement start timestamp overflow")?;
    let measurement_end_unix_ms = measurement_start_unix_ms
        .checked_add(config.measure_seconds.saturating_mul(1_000))
        .context("shared OAT measurement end timestamp overflow")?;
    let mut client_metrics = HashMap::<String, Vec<ClientMeasurement>>::with_capacity(tables.len());
    let mut completed_clients = HashSet::new();
    let mut active_clients = vec![true; handles.len()];
    while completed_clients.len() < handles.len() {
        let pending_indices = handles
            .iter()
            .enumerate()
            .filter_map(|(index, handle)| (!completed_clients.contains(&handle.client_id)).then_some(index))
            .collect::<Vec<_>>();
        let pending_handles = pending_indices
            .iter()
            .map(|index| handles[*index].clone())
            .collect::<Vec<_>>();
        let progress = match poll_clients(platform, config, &pending_handles).await {
            Ok(progress) => progress,
            Err(error) => {
                let cleanup = cancel_active_clients(platform, config, &handles, &active_clients).await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "cancelling OAT clients after polling failure also failed: {cleanup_error:#}"
                    ))),
                };
            }
        };
        for (index, state) in pending_indices.into_iter().zip(progress) {
            let handle = &handles[index];
            if let Some(failure) = state.failure {
                let error = anyhow::anyhow!("OAT client {} failed: {failure}", handle.client_id);
                let cleanup = cancel_active_clients(platform, config, &handles, &active_clients).await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "cancelling remaining OAT clients also failed: {cleanup_error:#}"
                    ))),
                };
            }
            if state.completed && !completed_clients.contains(&handle.client_id) {
                let output = match platform.finish_client(config, handle.clone()).await {
                    Ok(output) => output,
                    Err(error) => {
                        let cleanup = cancel_active_clients(platform, config, &handles, &active_clients).await;
                        return match cleanup {
                            Ok(()) => Err(error),
                            Err(cleanup_error) => Err(error.context(format!(
                                "cancelling clients after client collection failed also failed: {cleanup_error:#}"
                            ))),
                        };
                    }
                };
                active_clients[index] = false;
                let measurement = (|| {
                    let report = parse_client_metrics(&output)?;
                    validate_client_measurement_interval(
                        &report,
                        measurement_start_unix_ms,
                        measurement_end_unix_ms,
                        config.pilot_mode,
                    )
                    .with_context(|| format!("validate client measurement interval for {}", handle.table))?;
                    if report.rows == 0 || !report.rows_per_second.is_finite() || report.rows_per_second <= 0.0 {
                        bail!("DMV client for {} returned invalid measured throughput", handle.table);
                    }
                    let duration_ms = u64::try_from((report.duration_seconds * 1000.0).round() as u128)?.max(1);
                    Ok::<_, anyhow::Error>(ClientMeasurement {
                        metrics: ClientMetrics {
                            rows: report.rows,
                            duration_ms,
                            rows_per_second: report.rows_per_second,
                        },
                        pilot_windows: report.pilot_windows,
                    })
                })();
                let measurement = match measurement {
                    Ok(measurement) => measurement,
                    Err(error) => {
                        let cleanup = cancel_active_clients(platform, config, &handles, &active_clients).await;
                        return match cleanup {
                            Ok(()) => Err(error),
                            Err(cleanup_error) => Err(error.context(format!(
                                "cancelling clients after invalid measurement also failed: {cleanup_error:#}"
                            ))),
                        };
                    }
                };
                client_metrics
                    .entry(handle.table.clone())
                    .or_default()
                    .push(measurement);
                completed_clients.insert(handle.client_id.clone());
            }
        }
        record_owner_observation(
            pinning,
            sample_table_owners(&metrics_client, &metrics_endpoint, tables).await,
            tables,
            false,
        );
        if completed_clients.len() < handles.len() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    record_owner_observation(
        pinning,
        sample_table_owners(&metrics_client, &metrics_endpoint, tables).await,
        tables,
        true,
    );
    let mut metrics = HashMap::with_capacity(tables.len());
    for table in tables {
        let reports = client_metrics
            .remove(table)
            .with_context(|| format!("no DMV clients reported measurements for {table}"))?;
        if reports.len() != config.clients_per_table {
            bail!(
                "DMV table {table} returned {} of {} client measurements",
                reports.len(),
                config.clients_per_table
            );
        }
        let rows = reports
            .iter()
            .try_fold(0_u64, |sum, report| sum.checked_add(report.metrics.rows))
            .context("aggregated DMV row count overflow")?;
        let rows_per_second = reports.iter().map(|report| report.metrics.rows_per_second).sum::<f64>();
        if !rows_per_second.is_finite() || rows_per_second <= 0.0 {
            bail!("aggregated DMV throughput for {table} is invalid");
        }
        let mut windows = std::collections::BTreeMap::<u64, u64>::new();
        for report in &reports {
            for window in &report.pilot_windows {
                let rows = windows.entry(window.index).or_default();
                *rows = rows
                    .checked_add(window.rows)
                    .context("aggregated pilot row count overflow")?;
            }
        }
        if config.pilot_mode {
            let expected_windows = config.measure_seconds / 30;
            if windows.len() != expected_windows as usize || windows.keys().copied().ne(0..expected_windows) {
                bail!("pilot clients returned an incomplete 30-second window set for {table}");
            }
            let mut rates = Vec::with_capacity(windows.len());
            for (index, rows) in &windows {
                let rate = *rows as f64 / 30.0;
                rates.push(rate);
                println!(
                    "pilot_table={table} window={} window_seconds=30 rows={rows} rows_per_second={rate:.3}",
                    index + 1
                );
            }
            let stable_horizon =
                stable_window_horizon(&rates).map_or_else(|| "none".to_owned(), |seconds| seconds.to_string());
            println!("pilot_table={table} stable_from_seconds={stable_horizon}");
        } else if !windows.is_empty() {
            bail!("non-pilot DMV client unexpectedly returned pilot windows for {table}");
        }
        metrics.insert(
            table.clone(),
            ClientMetrics {
                rows,
                duration_ms: reports
                    .iter()
                    .map(|report| report.metrics.duration_ms)
                    .max()
                    .unwrap_or(1),
                rows_per_second,
            },
        );
    }
    Ok(ClientRun {
        metrics,
        pinning: std::mem::take(pinning),
        failure: None,
        qap_tables,
    })
}

fn validate_client_readiness(clients: &[ClientProgress], elapsed: Duration, timeout: Duration) -> Result<bool> {
    if let Some(failure) = clients.iter().find_map(|client| client.failure.as_deref()) {
        bail!("OAT client failed during preflight: {failure}");
    }
    if clients.iter().any(|client| client.completed && !client.ready) {
        bail!("an OAT client completed before reporting preflight readiness");
    }
    if elapsed >= timeout {
        bail!("OAT client readiness timed out; no measurement schedule was armed");
    }
    if clients.iter().all(|client| client.ready) {
        return Ok(true);
    }
    Ok(false)
}

fn validate_client_schedule(clients: &[ClientProgress], now_unix_ms: u64, run_start_unix_ms: u64) -> Result<bool> {
    if let Some(failure) = clients.iter().find_map(|client| client.failure.as_deref()) {
        bail!("OAT client failed while receiving armed schedule: {failure}");
    }
    if clients
        .iter()
        .any(|client| client.completed && !client.schedule_received)
    {
        bail!("an OAT client completed without receiving the final armed schedule");
    }
    if clients.iter().all(|client| client.schedule_received) {
        if now_unix_ms >= run_start_unix_ms {
            bail!("the full OAT fleet received its schedule after the final armed start");
        }
        return Ok(true);
    }
    if now_unix_ms >= run_start_unix_ms {
        bail!("an OAT client missed the final armed schedule; no partial window is accepted");
    }
    Ok(false)
}

fn checked_unix_time_ms(value: u128) -> Result<u64> {
    u64::try_from(value).context("Unix-millisecond timestamp exceeds the supported u64 range")
}

fn unix_time_ms_u64() -> Result<u64> {
    checked_unix_time_ms(unix_time_ms())
}

async fn poll_clients(
    platform: &dyn Platform,
    config: &OatConfig,
    handles: &[ClientHandle],
) -> Result<Vec<ClientProgress>> {
    platform.poll_client_fleet(config, handles).await
}

async fn cancel_clients(platform: &dyn Platform, config: &OatConfig, handles: &[ClientHandle]) -> Result<()> {
    let active = vec![true; handles.len()];
    cancel_active_clients(platform, config, handles, &active).await
}

async fn cancel_active_clients(
    platform: &dyn Platform,
    config: &OatConfig,
    handles: &[ClientHandle],
    active: &[bool],
) -> Result<()> {
    let results = join_all(
        handles
            .iter()
            .zip(active.iter())
            .filter(|(_, active)| **active)
            .map(|(handle, _)| platform.cancel_client(config, handle)),
    )
    .await;
    let errors = results
        .into_iter()
        .filter_map(Result::err)
        .map(|error| format!("{error:#}"))
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        bail!("one or more OAT clients could not be cancelled: {}", errors.join("; "));
    }
    Ok(())
}

async fn sample_table_owners(
    client: &reqwest::Client,
    endpoint: &str,
    tables: &[String],
) -> Result<HashMap<String, Vec<String>>> {
    let response = client
        .get(endpoint)
        .send()
        .await
        .with_context(|| format!("GET router metrics {endpoint}"))?
        .error_for_status()
        .context("router metrics endpoint returned an error status")?;
    let body = response.text().await.context("read router metrics response")?;
    parse_owner_metrics(&body, tables)
}

fn parse_owner_metrics(body: &str, tables: &[String]) -> Result<HashMap<String, Vec<String>>> {
    for line in body
        .lines()
        .filter(|line| line.starts_with("arrowcol_router_failures_total{"))
    {
        let close = line.find('}').context("malformed routing failure metric labels")?;
        let labels = parse_metric_labels(&line["arrowcol_router_failures_total{".len()..close])?;
        if labels.get("reason").map(String::as_str) == Some("pinning_violation") {
            let count: u64 = line[close + 1..]
                .trim()
                .parse()
                .context("invalid pinning failure counter")?;
            if count > 0 {
                bail!("pinning_violation: router rejected {count} requests for immutable ownership loss");
            }
        }
    }
    let mut owners = tables
        .iter()
        .map(|table| (canonical_table_key(table), Vec::new()))
        .collect::<HashMap<_, _>>();
    for line in body
        .lines()
        .filter(|line| line.starts_with("arrowcol_router_table_owners{"))
    {
        let close = line.find('}').context("malformed table-owner metric labels")?;
        let labels = parse_metric_labels(&line["arrowcol_router_table_owners{".len()..close])?;
        if labels.get("state").map(String::as_str) != Some("active") {
            continue;
        }
        let Some(table_name) = labels.get("table") else {
            bail!("table-owner metric omitted table label");
        };
        let Some(found) = owners.get_mut(&canonical_table_key(table_name)) else {
            continue;
        };
        let value = line[close + 1..].trim();
        if value != "1" {
            bail!("table-owner gauge for {table_name} had unexpected value {value:?}");
        }
        let collector = labels
            .get("collector")
            .context("table-owner metric omitted collector label")?;
        found.push(collector.clone());
    }
    Ok(owners)
}

fn parse_metric_labels(input: &str) -> Result<HashMap<String, String>> {
    let mut labels = HashMap::new();
    for part in split_prometheus_labels(input)? {
        let (key, value) = part.split_once('=').context("malformed table-owner metric label")?;
        let value = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .context("table-owner metric label value must be quoted")?;
        let value = unescape_prometheus_label(value)?;
        if key.is_empty() || labels.insert(key.to_owned(), value).is_some() {
            bail!("table-owner metric contains an empty or duplicate label");
        }
    }
    Ok(labels)
}

fn split_prometheus_labels(input: &str) -> Result<Vec<&str>> {
    let bytes = input.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in bytes.iter().enumerate() {
        if escaped {
            escaped = false;
        } else if quoted && *byte == b'\\' {
            escaped = true;
        } else if *byte == b'"' {
            quoted = !quoted;
        } else if *byte == b',' && !quoted {
            parts.push(&input[start..index]);
            start = index + 1;
        }
    }
    if quoted || escaped {
        bail!("unterminated escape or quote in table-owner metric labels");
    }
    if start < input.len() {
        parts.push(&input[start..]);
    }
    Ok(parts)
}

fn unescape_prometheus_label(value: &str) -> Result<String> {
    let mut unescaped = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            unescaped.push(character);
            continue;
        }
        match chars.next().context("unfinished escape in table-owner metric label")? {
            '\\' => unescaped.push('\\'),
            '"' => unescaped.push('"'),
            'n' => unescaped.push('\n'),
            other => bail!("unsupported escape \\{other} in table-owner metric label"),
        }
    }
    Ok(unescaped)
}

fn canonical_table_key(table: &str) -> String {
    table.to_ascii_lowercase()
}

fn check_owner_sample(
    tables: &[String],
    sample: &HashMap<String, Vec<String>>,
    owners: &mut HashMap<String, String>,
) -> Option<String> {
    for table in tables {
        let key = canonical_table_key(table);
        let current = sample.get(&key).map(Vec::as_slice).unwrap_or_default();
        if current.is_empty() && !owners.contains_key(&key) {
            continue;
        }

        if current.len() != 1 {
            return Some(format!(
                "table {table} has {} active ArrowCol owners; expected exactly one",
                current.len()
            ));
        }
        if let Some(previous) = owners.get(&key) {
            let current = &current[0];
            if previous != current {
                return Some(format!(
                    "table {table} changed ArrowCol owner from {previous} to {current}"
                ));
            }
        } else {
            let current = &current[0];
            owners.insert(key, current.clone());
        }
    }
    None
}

#[derive(Clone, Debug)]
struct ClientReport {
    rows: u64,
    duration_seconds: f64,
    rows_per_second: f64,
    measurement_window_start_unix_ms: u64,
    measurement_window_end_unix_ms: u64,
    measurement_started_unix_ms: u64,
    pilot_windows: Vec<PilotWindow>,
}

fn parse_client_metrics(output: &str) -> Result<ClientReport> {
    let line = output
        .lines()
        .find(|line| line.starts_with("measured_rows="))
        .context("DMV client did not emit measured throughput")?;
    let mut fields = line.split_whitespace();
    let rows = fields
        .next()
        .and_then(|value| value.strip_prefix("measured_rows="))
        .context("DMV client measured_rows field is missing")?
        .parse::<u64>()?;
    let duration_seconds = fields
        .next()
        .and_then(|value| value.strip_prefix("measured_seconds="))
        .context("DMV client measured_seconds field is missing")?
        .parse::<f64>()?;
    let rows_per_second = fields
        .next()
        .and_then(|value| value.strip_prefix("rows_per_second="))
        .context("DMV client rows_per_second field is missing")?
        .parse::<f64>()?;
    let measurement_window_start_unix_ms = parse_metric_value(fields.next(), "measurement_window_start_unix_ms")?;
    let measurement_window_end_unix_ms = parse_metric_value(fields.next(), "measurement_window_end_unix_ms")?;
    let measurement_started_unix_ms = parse_metric_value(fields.next(), "measurement_started_unix_ms")?;
    if fields.next().is_some()
        || !duration_seconds.is_finite()
        || duration_seconds <= 0.0
        || !rows_per_second.is_finite()
        || rows_per_second < 0.0
        || measurement_window_end_unix_ms <= measurement_window_start_unix_ms
    {
        bail!("DMV client emitted invalid measured throughput or shared measurement interval");
    }
    let mut pilot_windows = Vec::new();
    for line in output.lines().filter(|line| line.starts_with("pilot_window=")) {
        let mut fields = line.split_whitespace();
        let index = parse_metric_value::<u64>(fields.next(), "pilot_window")?;
        let seconds = parse_metric_value::<u64>(fields.next(), "window_seconds")?;
        let window_rows = parse_metric_value(fields.next(), "rows")?;
        let window_rate = parse_metric_value::<f64>(fields.next(), "rows_per_second")?;
        let start_unix_ms = parse_metric_value(fields.next(), "window_start_unix_ms")?;
        if fields.next().is_some() || seconds != 30 || !window_rate.is_finite() || window_rate < 0.0 {
            bail!("DMV client emitted an invalid pilot window");
        }
        pilot_windows.push(PilotWindow {
            index: index.checked_sub(1).context("pilot window numbering starts at 1")?,
            rows: window_rows,
            start_unix_ms,
        });
    }
    if pilot_windows.windows(2).any(|pair| pair[0].index >= pair[1].index) {
        bail!("DMV client emitted duplicate or out-of-order pilot windows");
    }
    Ok(ClientReport {
        rows,
        duration_seconds,
        rows_per_second,
        measurement_window_start_unix_ms,
        measurement_window_end_unix_ms,
        measurement_started_unix_ms,
        pilot_windows,
    })
}

fn validate_client_measurement_interval(
    report: &ClientReport,
    expected_start_unix_ms: u64,
    expected_end_unix_ms: u64,
    pilot_mode: bool,
) -> Result<()> {
    if report.measurement_window_start_unix_ms != expected_start_unix_ms
        || report.measurement_window_end_unix_ms != expected_end_unix_ms
        || report.measurement_started_unix_ms.abs_diff(expected_start_unix_ms) > 250
        || (report.duration_seconds - expected_end_unix_ms.saturating_sub(expected_start_unix_ms) as f64 / 1_000.0)
            .abs()
            > 0.0005
    {
        bail!("client did not cover the shared measurement interval");
    }
    if pilot_mode {
        let expected_windows = expected_end_unix_ms.saturating_sub(expected_start_unix_ms) / 30_000;
        if report.pilot_windows.len() != expected_windows as usize
            || report.pilot_windows.iter().enumerate().any(|(index, window)| {
                window.index != index as u64
                    || window.start_unix_ms
                        != expected_start_unix_ms.saturating_add((index as u64).saturating_mul(30_000))
            })
        {
            bail!("pilot client returned incomplete or misaligned 30-second windows");
        }
    } else if !report.pilot_windows.is_empty() {
        bail!("non-pilot client unexpectedly returned pilot windows");
    }
    Ok(())
}

fn parse_metric_value<T>(field: Option<&str>, name: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    field
        .and_then(|field| field.strip_prefix(&format!("{name}=")))
        .with_context(|| format!("DMV client pilot output is missing {name}"))?
        .parse::<T>()
        .with_context(|| format!("DMV client pilot {name} value is invalid"))
}

fn stable_window_horizon(rates: &[f64]) -> Option<usize> {
    if rates.len() < 2 {
        return None;
    }
    let observed_minimum = rates.iter().copied().fold(f64::INFINITY, f64::min);
    let observed_maximum = rates.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let observed_mean = rates.iter().sum::<f64>() / rates.len() as f64;
    if observed_mean <= 0.0 || (observed_maximum - observed_minimum) / observed_mean >= 0.05 {
        return None;
    }
    let mut minimum = f64::INFINITY;
    let mut maximum = f64::NEG_INFINITY;
    let mut sum = 0.0;
    for (index, rate) in rates.iter().copied().enumerate() {
        minimum = minimum.min(rate);
        maximum = maximum.max(rate);
        sum += rate;
        let mean = sum / (index + 1) as f64;
        if index >= 1 && mean > 0.0 && (maximum - minimum) / mean < 0.05 {
            return Some((index + 1) * 30);
        }
    }
    None
}

async fn observe_qap(
    endpoint: &str,
    mode: &arrowcol_loadgen::ObservationMode,
    database: &str,
    run_id: &str,
    qap_tables: &[String],
    timeout_seconds: u64,
) -> Result<QapMetrics> {
    let results = std::path::PathBuf::from("out/arrowcol-oat/qap").join(run_id);
    let endpoint = endpoint.to_owned();
    let database = database.to_owned();
    let mode = *mode;
    let observer_tables = qap_tables.to_vec();
    let timeout = Duration::from_secs(timeout_seconds);
    let observation = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&results)
            .with_context(|| format!("create QAP results directory {}", results.display()))?;
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        runtime
            .block_on(arrowcol_loadgen::observe_qap_with_mode(
                &endpoint,
                &results,
                mode,
                &database,
                &observer_tables,
                timeout,
            ))
            .context("observe Kusto Query Acceleration Policy")
    })
    .await
    .context("join Kusto QAP observer")??;
    aggregate_qap(&observation, qap_tables)
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

fn scoped_compose_project(base: &str, run_id: &str) -> String {
    let suffix = run_id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .take(8)
        .collect::<String>()
        .to_ascii_lowercase();
    let prefix_limit = 48_usize.saturating_sub(suffix.len() + 1);
    format!(
        "{}-{suffix}",
        base.chars()
            .take(prefix_limit)
            .collect::<String>()
            .trim_end_matches(['-', '_'])
    )
}

fn compose_command(compose_file: &str, override_file: Option<&str>, project: &str, profile: &str) -> Command {
    let mut command = Command::new("docker");
    command.args(["compose", "--env-file", "/dev/null", "--file", compose_file]);
    if let Some(override_file) = override_file {
        command.args(["--file", override_file]);
    }
    command.args(["--project-name", project, "--profile", profile]);
    command
}

fn set_local_pin_init_config(command: &mut Command, config_path: &Path) {
    command.env("OAT_PIN_INIT_CONFIG_HOST_PATH", config_path);
}

async fn run_command(mut command: Command, action: &str) -> Result<std::process::Output> {
    let output = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .output()
        .await
        .with_context(|| format!("{action}: start process"))?;
    if !output.status.success() {
        bail!(
            "{action} failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output)
}

fn pin_init_config_json(
    run_id: &str,
    expected_keys_json: &str,
    expires_at_unix_ms: u64,
    now_unix_ms: u64,
    sql_host: &str,
    router_yaml: &str,
) -> Result<String> {
    let parsed = uuid::Uuid::parse_str(run_id).context("pin initialization requires a UUID run ID")?;
    if parsed.to_string() != run_id {
        bail!("pin initialization requires a canonical lowercase UUID run ID");
    }
    if expires_at_unix_ms <= now_unix_ms {
        bail!("pin initialization expiry must be later than its start time");
    }
    let pin_set: Value =
        serde_json::from_str(expected_keys_json).context("parse expected OAT table pin-set configuration")?;
    let expected_keys = pin_set
        .get("expected_keys")
        .and_then(Value::as_array)
        .filter(|keys| !keys.is_empty())
        .context("expected OAT pin set has no expected_keys")?;
    let expected_digest = pin_set
        .get("expected_digest")
        .and_then(Value::as_str)
        .context("expected OAT pin set has no expected_digest")?;
    if pin_set.as_object().is_none_or(|object| object.len() != 2)
        || expected_digest.len() != 64
        || !expected_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("expected OAT pin set must contain only expected_keys and lowercase SHA-256 expected_digest");
    }
    let routing_policy = routing_policy_from_yaml(router_yaml, run_id)?;
    if expires_at_unix_ms - now_unix_ms > routing_policy.oat_run_pin_ttl_ms {
        bail!("OAT pin expiry exceeds the selected router's configured run-pin lifetime");
    }
    Ok(serde_json::to_string(&json!({
        "schema_version": 1,
        "run_id": run_id,
        "expires_at_unix_ms": expires_at_unix_ms,
        "expected_keys": expected_keys,
        "expected_digest": expected_digest,
        "routing_policy": routing_policy,
        "sql_server": {
            "host": sql_host,
            "port": 1433,
            "trust_server_certificate": true,
        },
    }))?)
}

async fn run_local_pin_setup(
    config: &OatConfig,
    platform: &LocalPlatform,
    run_id: &str,
    expected_keys_json: &str,
    expires_at_unix_ms: u64,
    now_unix_ms: u64,
) -> Result<()> {
    let router_yaml_path = platform.state_dir.join("router-state/config.yaml");
    let router_yaml = tokio::fs::read_to_string(&router_yaml_path)
        .await
        .with_context(|| format!("read rendered local router config {}", router_yaml_path.display()))?;
    let pin_config = pin_init_config_json(
        run_id,
        expected_keys_json,
        expires_at_unix_ms,
        now_unix_ms,
        "sqlserver",
        &router_yaml,
    )?;
    let config_path = platform.state_dir.join("pin-init.json");
    write_private_secret(&config_path, &pin_config).context("write private local OAT pin initialization config")?;
    let project = scoped_compose_project(
        config
            .platform
            .compose_project
            .as_deref()
            .context("local compose project name is missing")?,
        &platform.run_id,
    );
    let mut command = compose_command(
        config
            .platform
            .compose_file
            .as_deref()
            .context("local compose file is missing")?,
        config.platform.compose_override_file.as_deref(),
        &project,
        "oat",
    );
    platform.add_compose_environment(&mut command);
    set_local_pin_init_config(&mut command, &config_path);
    command.args(["run", "--rm", "--no-deps", "oat-pin-init"]);
    match tokio::time::timeout(
        Duration::from_secs(crate::config::PIN_INIT_SETUP_TIMEOUT_SECONDS),
        run_command(command, "initialize local immutable OAT table pin set"),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!(
            "local OAT pin initialization exceeded its bounded setup lifetime; outcome may be indeterminate and is not retried"
        )),
    }
    .context("run local OAT pin initialization with the separate migrator credential")
    .map(|_| ())
}

async fn run_aks_pin_setup_job(
    config: &OatConfig,
    platform: &AksPlatform,
    run_id: &str,
    expected_keys_json: &str,
    expires_at_unix_ms: u64,
    now_unix_ms: u64,
) -> Result<()> {
    let router_yaml = platform
        .router_yaml
        .as_deref()
        .context("AKS pin initialization requires the exact rendered router config")?;
    let pin_config = pin_init_config_json(
        run_id,
        expected_keys_json,
        expires_at_unix_ms,
        now_unix_ms,
        "arrowcol-sql",
        router_yaml,
    )?;
    let run_suffix = uuid::Uuid::parse_str(run_id)?.simple().to_string();
    let config_map_name = format!("oat-pin-init-config-{run_suffix}");
    let job_name = format!("oat-pin-init-{run_suffix}");
    let labels = json!({
        "app": "arrowcol-oat-pin-init",
        "arrowcol-oat-run": run_suffix,
    });
    let service_account = platform
        .service_account
        .as_deref()
        .context("AKS pin initialization requires the actual rendered runtime ServiceAccount")?;
    let resources = json!({
        "requests": { "cpu": "100m", "memory": "128Mi" },
        "limits": { "cpu": "500m", "memory": "512Mi" },
    });
    let payload = serde_json::to_vec(&json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [{
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": config_map_name,
                "namespace": config.namespace,
                "labels": labels,
            },
            "data": { "pin-init.json": pin_config },
        }, {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": config.namespace,
                "labels": labels,
            },
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": crate::config::PIN_INIT_SETUP_TIMEOUT_SECONDS,
                "template": {
                    "metadata": { "labels": labels },
                    "spec": {
                        "serviceAccountName": service_account,
                        "automountServiceAccountToken": false,
                        "enableServiceLinks": false,
                        "restartPolicy": "Never",
                        "terminationGracePeriodSeconds": 10,
                        "nodeSelector": {
                            "arrow-collector/role": "arrowcol",
                            "kubernetes.io/arch": "amd64",
                        },
                        "tolerations": [{
                            "key": "arrow-collector/role",
                            "operator": "Equal",
                            "value": "arrowcol",
                            "effect": "NoSchedule",
                        }],
                        "securityContext": {
                            "runAsNonRoot": true,
                            "runAsUser": 10001,
                            "runAsGroup": 10001,
                            "fsGroup": 10001,
                            "fsGroupChangePolicy": "OnRootMismatch",
                            "seccompProfile": { "type": "RuntimeDefault" },
                        },
                        "containers": [{
                            "name": "pin-init",
                            "image": config.dmv_client_image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/usr/local/bin/arrowcol-oat-pin-init"],
                            "args": [
                                "--config",
                                "/run/arrowcol-oat/pin-init.json",
                                "--migrator-password-file",
                                "/run/migration/password",
                            ],
                            "resources": resources,
                            "securityContext": {
                                "allowPrivilegeEscalation": false,
                                "capabilities": { "drop": ["ALL"] },
                                "readOnlyRootFilesystem": true,
                            },
                            "volumeMounts": [{
                                "name": "pin-config",
                                "mountPath": "/run/arrowcol-oat",
                                "readOnly": true,
                            }, {
                                "name": "migration-password",
                                "mountPath": "/run/migration",
                                "readOnly": true,
                            }],
                        }],
                        "volumes": [{
                            "name": "pin-config",
                            "configMap": {
                                "name": config_map_name,
                                "items": [{ "key": "pin-init.json", "path": "pin-init.json" }],
                            },
                        }, {
                            "name": "migration-password",
                            "secret": {
                                "secretName": "arrowcol-sql-migration",
                                "defaultMode": 288,
                                "items": [{ "key": "password", "path": "password" }],
                            },
                        }],
                    },
                },
            },
        }],
    }))?;
    kubectl(config, vec!["create", "-f", "-"], Some(payload))
        .await
        .context("create finite AKS OAT pin initialization Job")?;

    wait_for_aks_pin_setup(config, &job_name, run_id).await?;
    kubectl(
        config,
        vec![
            "delete",
            "job",
            &job_name,
            "--namespace",
            &config.namespace,
            "--wait=true",
        ],
        None,
    )
    .await
    .context("remove completed AKS OAT pin initialization Job")?;
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
    .context("remove completed AKS OAT pin initialization ConfigMap")?;
    Ok(())
}

async fn wait_for_aks_pin_setup(config: &OatConfig, job_name: &str, run_id: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(200);
    loop {
        let output = kubectl(
            config,
            vec!["get", "job", job_name, "--namespace", &config.namespace, "-o", "json"],
            None,
        )
        .await
        .with_context(|| format!("inspect AKS OAT pin initialization Job {job_name}"))?;
        let job: Value = serde_json::from_str(&output).context("parse AKS OAT pin initialization Job")?;
        if job.pointer("/status/succeeded").and_then(Value::as_u64).unwrap_or(0) > 0 {
            break;
        }
        let failed = job.pointer("/status/failed").and_then(Value::as_u64).unwrap_or(0) > 0
            || job
                .pointer("/status/conditions")
                .and_then(Value::as_array)
                .is_some_and(|conditions| {
                    conditions.iter().any(|condition| {
                        condition.get("type").and_then(Value::as_str) == Some("Failed")
                            && condition.get("status").and_then(Value::as_str) == Some("True")
                    })
                });
        if failed || tokio::time::Instant::now() >= deadline {
            let logs = kubectl(
                config,
                vec!["logs", "--namespace", &config.namespace, &format!("job/{job_name}")],
                None,
            )
            .await
            .unwrap_or_else(|error| format!("<could not read setup Job logs: {error:#}>"));
            bail!(
                "AKS OAT pin initialization Job {job_name} {}: {logs}",
                if failed { "failed" } else { "timed out" }
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let logs = kubectl(
        config,
        vec!["logs", "--namespace", &config.namespace, &format!("job/{job_name}")],
        None,
    )
    .await
    .with_context(|| format!("read successful AKS OAT pin initialization Job {job_name} logs"))?;
    if !logs.contains(&format!("initialized OAT pin set run_id={run_id}")) {
        bail!("AKS OAT pin initialization Job {job_name} did not confirm the requested run ID");
    }
    Ok(())
}

async fn prepare_local_platform(config: &OatConfig, platform: &LocalPlatform) -> Result<()> {
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let mut build_client = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "oat",
    );
    platform.add_compose_environment(&mut build_client);
    build_client.args(["build", "oat-client"]);
    run_command(build_client, "build OAT client container").await?;
    verify_local_bind_mount(config, platform).await?;

    let mut up = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut up);
    up.args([
        "up",
        "--detach",
        "--build",
        "--wait",
        "--wait-timeout",
        "600",
        "sqlserver",
    ]);
    run_command(up, "start local OAT SQL Server")
        .await
        .context("start local OAT stack")?;

    for service in ["database-bootstrap", "arrowcol-migrate"] {
        let mut migration = compose_command(
            compose_file,
            config.platform.compose_override_file.as_deref(),
            &project,
            "benchmark",
        );
        platform.add_compose_environment(&mut migration);
        migration.args(["run", "--rm", "--no-deps", service]);
        run_command(migration, &format!("run local OAT {service}")).await?;
    }

    let mut up = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut up);
    up.args([
        "up",
        "--detach",
        "--build",
        "--wait",
        "--wait-timeout",
        "600",
        "arrowcol-catalog",
        "dataflow-1",
        "kusto",
        "lgtm",
        "router",
    ]);
    run_command(up, "start remaining local OAT stack")
        .await
        .context("local compose stack is a per-run project; results are not touched by stack cleanup")?;
    Ok(())
}

async fn scale_local_arrowcols(
    config: &OatConfig,
    platform: &LocalPlatform,
    replicas: u32,
    concurrency: u32,
) -> Result<()> {
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let mut up = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut up);
    up.env("LOADTEST_CPUS", concurrency.to_string());
    up.args([
        "up",
        "--detach",
        "--wait",
        "--wait-timeout",
        "600",
        "--scale",
        &format!("dataflow-1={replicas}"),
        "dataflow-1",
    ]);
    run_command(up, "scale local ArrowCol containers").await?;
    Ok(())
}

async fn local_compose_endpoint(
    config: &OatConfig,
    platform: &LocalPlatform,
    service: &str,
    container_port: &str,
    path: &str,
) -> Result<String> {
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let mut command = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut command);
    command.args(["port", service, container_port]);
    let output = run_command(command, &format!("discover local {service} endpoint")).await?;
    let binding = String::from_utf8(output.stdout)?;
    let binding = binding
        .lines()
        .next()
        .context("Compose returned no published binding")?;
    let endpoint = crate::local::published_endpoint(
        binding,
        config.platform.local.endpoint_host.as_deref(),
        platform
            .daemon_endpoint
            .as_deref()
            .context("Docker daemon endpoint not resolved")?,
        path,
    )?;
    let url = Url::parse(&endpoint)?;
    let host = url
        .host_str()
        .context("local endpoint has no host")?
        .trim_matches(['[', ']']);
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect((host, url.port_or_known_default().context("local endpoint has no port")?)),
    )
    .await
    .with_context(|| format!("local {service} endpoint {endpoint} is unreachable; configure platform.local.bind_host/endpoint_host, not a loopback fallback"))?
    .with_context(|| format!("connect local {service} endpoint {endpoint}"))?;
    Ok(endpoint)
}

async fn local_docker_daemon() -> Result<String> {
    if std::env::var_os("DOCKER_CONTEXT").is_none()
        && let Ok(host) = std::env::var("DOCKER_HOST")
    {
        Url::parse(&host).context("invalid DOCKER_HOST")?;
        return Ok(host);
    }
    let mut command = Command::new("docker");
    command.args(["context", "inspect", "--format", "{{.Endpoints.docker.Host}}"]);
    let output = run_command(command, "resolve active Docker daemon endpoint").await?;
    let endpoint = String::from_utf8(output.stdout)?.trim().to_owned();
    Url::parse(&endpoint).context("Docker context returned an invalid daemon endpoint")?;
    Ok(endpoint)
}

async fn verify_local_bind_mount(config: &OatConfig, platform: &LocalPlatform) -> Result<()> {
    let nonce = uuid::Uuid::new_v4().to_string();
    let probe = platform.state_dir.join("control/daemon-bind-probe");
    fs::write(&probe, &nonce)?;
    let state = platform.state_dir.to_str().context("local state path is not UTF-8")?;
    let compose = std::env::current_dir()?.join(
        config
            .platform
            .compose_file
            .as_deref()
            .context("missing local compose file")?,
    );
    let observability = compose
        .parent()
        .context("local compose file has no parent")?
        .join("observability");
    crate::local::absolute_directory(&observability)?;
    let source = observability.to_str().context("observability path is not UTF-8")?;
    if state.contains(',') || source.contains(',') {
        bail!("local bind source paths cannot contain a Docker --mount separator");
    }
    let mut checksums = String::new();
    for name in [
        "prometheus.yaml",
        "dashboards.yaml",
        "arrowcol.json",
        "delta-operations.json",
    ] {
        let (_, digest) = crate::local::file_digest(&observability.join(name))?;
        checksums.push_str(&format!("{digest}  /oat-observability/{name}\n"));
    }
    let uid = platform
        .environment
        .get("LOADTEST_UID")
        .context("local state UID missing")?;
    let gid = platform
        .environment
        .get("LOADTEST_GID")
        .context("local state GID missing")?;
    let user = format!("{uid}:{gid}");
    let mut command = Command::new("docker");
    command.args([
        "run", "--rm", "--network", "none", "--read-only",
        "--user", &user,
        "--mount", &format!("type=bind,source={state},target=/oat-bind,readonly"),
        "--mount", &format!("type=bind,source={source},target=/oat-observability,readonly"),
        "--entrypoint", "/bin/sh", "arrowcol-oat-client:local", "-ec",
        "test \"$(cat /oat-bind/control/daemon-bind-probe)\" = \"$1\" && test -f /oat-bind/router-state/config.yaml && printf '%s' \"$2\" | sha256sum -c -",
        "oat-bind-probe", &nonce, &checksums,
    ]);
    run_command(command, "verify Docker daemon sees the same absolute generated-state path and bytes; remote path remapping is unsupported").await?;
    fs::remove_file(probe)?;
    Ok(())
}

async fn stop_local_snapshot_writers(config: &OatConfig, platform: &LocalPlatform) -> Result<Vec<Value>> {
    let project = scoped_compose_project(
        config
            .platform
            .compose_project
            .as_deref()
            .context("missing local project")?,
        &platform.run_id,
    );
    let mut list = Command::new("docker");
    list.args([
        "ps",
        "--all",
        "--quiet",
        "--no-trunc",
        "--filter",
        &format!("label=com.docker.compose.project={project}"),
    ]);
    let output = run_command(list, "list owned local containers before snapshot").await?;
    let ids = String::from_utf8(output.stdout)?;
    let ids = ids.split_whitespace().map(str::to_owned).collect::<Vec<_>>();
    if ids.is_empty() {
        bail!("no owned containers to establish stopped snapshot custody");
    }
    let mut inspect = Command::new("docker");
    inspect.arg("inspect").args(&ids);
    let before: Vec<Value> =
        serde_json::from_slice(&run_command(inspect, "inspect local snapshot ownership").await?.stdout)?;
    validate_snapshot_container_ownership(&before, &project)?;
    for phase in snapshot_stop_phases(&before)? {
        if !phase.is_empty() {
            let mut stop = Command::new("docker");
            stop.args(["stop", "--time", "300"]).args(phase);
            run_command(
                stop,
                "stop owned local ingress, then collectors, then catalog, then dependencies",
            )
            .await?;
        }
    }
    let mut inspect = Command::new("docker");
    inspect.arg("inspect").args(&ids);
    let after: Vec<Value> = serde_json::from_slice(
        &run_command(inspect, "verify definite stopped local container outcomes")
            .await?
            .stdout,
    )?;
    let receipt = json!({
        "schema_version": 1, "run_id": platform.run_id, "project": project,
        "containers": after.iter().map(|container| json!({
            "id": container["Id"],
            "service": container.pointer("/Config/Labels/com.docker.compose.service"),
            "state": {
                "Running": container["State"]["Running"], "Paused": container["State"]["Paused"],
                "Dead": container["State"]["Dead"], "Status": container["State"]["Status"],
                "Error": container["State"]["Error"], "ExitCode": container["State"]["ExitCode"],
                "OOMKilled": container["State"]["OOMKilled"], "FinishedAt": container["State"]["FinishedAt"],
            },
        })).collect::<Vec<_>>(),
    });
    let mut receipt_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(platform.control_dir.join("snapshot-stop.json"))?;
    receipt_file.write_all(&serde_json::to_vec_pretty(&receipt)?)?;
    receipt_file.sync_all()?;
    let mut list = Command::new("docker");
    list.args([
        "ps",
        "--all",
        "--quiet",
        "--no-trunc",
        "--filter",
        &format!("label=com.docker.compose.project={project}"),
    ]);
    let listed = run_command(list, "verify no new owned containers appeared during snapshot stop").await?;
    let listed = String::from_utf8(listed.stdout)?;
    if listed.split_whitespace().collect::<HashSet<_>>() != ids.iter().map(String::as_str).collect::<HashSet<_>>() {
        bail!("owned local container set changed while stopping; preserving state");
    }
    snapshot_container_outcomes(&before, &after, &project)
}

fn snapshot_container_outcomes(before: &[Value], after: &[Value], project: &str) -> Result<Vec<Value>> {
    validate_snapshot_container_ownership(before, project)?;
    validate_snapshot_container_ownership(after, project)?;
    let before_by_id = before
        .iter()
        .map(|container| {
            Ok((
                container
                    .get("Id")
                    .and_then(Value::as_str)
                    .context("container ID missing")?,
                container,
            ))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    if before.len() != after.len() || before_by_id.len() != before.len() {
        bail!("owned local container set changed while stopping");
    }
    let mut writers = Vec::new();
    let mut seen = HashSet::new();
    for container in after {
        let id = container
            .get("Id")
            .and_then(Value::as_str)
            .context("container ID missing")?;
        if !seen.insert(id) {
            bail!("duplicate container in stopped snapshot receipt");
        }
        let original = before_by_id
            .get(id)
            .context("owned container identity changed while stopping")?;
        let state = container.get("State").context("container has no stop state")?;
        if state.get("Running").and_then(Value::as_bool) != Some(false)
            || state.get("Paused").and_then(Value::as_bool) != Some(false)
            || state.get("Dead").and_then(Value::as_bool) != Some(false)
            || state.get("Status").and_then(Value::as_str) != Some("exited")
            || state.get("Error").and_then(Value::as_str) != Some("")
        {
            bail!("uncertain local container stop outcome; preserving state");
        }
        let service = container
            .pointer("/Config/Labels/com.docker.compose.service")
            .and_then(Value::as_str)
            .context("container service label missing")?;
        if original
            .pointer("/Config/Labels/com.docker.compose.service")
            .and_then(Value::as_str)
            != Some(service)
        {
            bail!("owned container service changed while stopping");
        }
        let was_running = original
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .context("original container running state missing")?;
        let exit = state
            .get("ExitCode")
            .and_then(Value::as_i64)
            .context("container exit code missing")?;
        if !(exit == 0 || (was_running && exit == 143))
            || state.get("OOMKilled").and_then(Value::as_bool) != Some(false)
        {
            bail!("owned container {service} failed or was forcibly stopped ({exit}); preserving data");
        }
        let finished_at = state
            .get("FinishedAt")
            .and_then(Value::as_str)
            .context("container finish time missing")?;
        if chrono::DateTime::parse_from_rfc3339(finished_at)?.timestamp() <= 0 {
            bail!("owned container has no definite finish time");
        }
        writers.push(json!({
            "id": id, "service": service, "was_running": was_running, "exit_code": exit,
            "finished_at": finished_at, "oom_killed": state["OOMKilled"],
        }));
    }
    Ok(writers)
}

fn validate_snapshot_container_ownership(containers: &[Value], project: &str) -> Result<()> {
    let mut services = HashSet::new();
    for container in containers {
        if container
            .pointer("/Config/Labels/com.docker.compose.project")
            .and_then(Value::as_str)
            != Some(project)
        {
            bail!("refusing snapshot stop outside the exact owned Compose project");
        }
        services.insert(
            container
                .pointer("/Config/Labels/com.docker.compose.service")
                .and_then(Value::as_str)
                .context("missing owned Compose service")?,
        );
    }
    if !services.contains("dataflow-1") || !services.contains("arrowcol-catalog") {
        bail!("snapshot requires the owned collector and catalog, not a partially prepared stack");
    }
    Ok(())
}

fn snapshot_stop_phases(containers: &[Value]) -> Result<[Vec<String>; 4]> {
    let mut phases: [Vec<String>; 4] = std::array::from_fn(|_| Vec::new());
    for container in containers {
        let service = container
            .pointer("/Config/Labels/com.docker.compose.service")
            .and_then(Value::as_str)
            .context("missing owned Compose service")?;
        let phase = match service {
            "dataflow-1" => 1,
            "arrowcol-catalog" => 2,
            "sqlserver" | "kusto" | "lgtm" => 3,
            _ => 0,
        };
        phases[phase].push(
            container
                .get("Id")
                .and_then(Value::as_str)
                .context("container ID missing")?
                .to_owned(),
        );
    }
    Ok(phases)
}

async fn cleanup_local_platform(config: &OatConfig, platform: &LocalPlatform) -> Result<()> {
    if let Some(destination) = &config.platform.local.snapshot_root {
        return crate::local::snapshot_then_cleanup(
            async {
                tokio::time::timeout(Duration::from_secs(1_440), stop_local_snapshot_writers(config, platform))
                    .await
                    .context("local stop deadline exceeded; outcome uncertain, retaining owned data")?
            },
            |writers| crate::local::export_stopped(
                &platform.state_dir, Path::new(destination), &platform.run_id, &config.tables, &writers,
            ),
            destroy_local_platform(config, platform),
        )
        .await
        .with_context(|| format!(
            "local snapshot/cleanup failed; source recovery path: {} (stop/export failures retain it; destructive-cleanup failures may be partial)",
            platform.state_dir.display()
        ));
    }
    destroy_local_platform(config, platform).await
}

fn local_oat_client_list_args(project: &str) -> Vec<String> {
    vec![
        "ps".to_owned(),
        "-aq".to_owned(),
        "--filter".to_owned(),
        format!("label=com.docker.compose.project={project}"),
        "--filter".to_owned(),
        "label=com.docker.compose.service=oat-client".to_owned(),
    ]
}

fn local_oat_client_remove_args(container_ids: &[String]) -> Vec<String> {
    let mut args = vec!["rm".to_owned(), "--force".to_owned()];
    args.extend(container_ids.iter().cloned());
    args
}

async fn remove_local_oat_client_containers(project: &str) -> Result<bool> {
    let mut list = Command::new("docker");
    list.args(local_oat_client_list_args(project));
    let output = run_command(list, "list local OAT client containers for cleanup").await?;
    let ids = String::from_utf8(output.stdout)
        .context("local OAT client container IDs were not UTF-8")?
        .lines()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(false);
    }
    let mut remove = Command::new("docker");
    remove.args(local_oat_client_remove_args(&ids));
    run_command(remove, "remove local OAT client containers").await?;
    Ok(true)
}

async fn destroy_local_platform(config: &OatConfig, platform: &LocalPlatform) -> Result<()> {
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let mut stack_clean = false;
    for _ in 0..3 {
        remove_local_oat_client_containers(&project).await?;
        let mut down = compose_command(
            compose_file,
            config.platform.compose_override_file.as_deref(),
            &project,
            "benchmark",
        );
        down.envs(&platform.environment);
        down.args(["down", "--volumes", "--remove-orphans"]);
        run_command(down, "clean up local OAT stack").await?;
        if !remove_local_oat_client_containers(&project).await? {
            stack_clean = true;
            break;
        }
    }
    if !stack_clean {
        bail!("local OAT client containers continued to appear during cleanup");
    }
    if platform.state_dir.exists() {
        fs::remove_dir_all(&platform.state_dir)
            .with_context(|| format!("remove generated local OAT state {}", platform.state_dir.display()))?;
    }
    Ok(())
}

async fn collect_local_metrics(config: &OatConfig, platform: &LocalPlatform) -> Result<ResourceMetrics> {
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let mut list = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    list.envs(&platform.environment);
    list.args(["ps", "--quiet", "dataflow-1"]);
    let ids = String::from_utf8(run_command(list, "list local ArrowCol containers").await?.stdout)?;
    let ids = ids
        .lines()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .collect::<Vec<_>>();
    if ids.is_empty() {
        bail!("local Compose project has no running ArrowCol containers");
    }
    let mut stats = Command::new("docker");
    stats.args([
        "stats",
        "--no-stream",
        "--format",
        "{{.Name}} {{.CPUPerc}} {{.MemUsage}}",
    ]);
    stats.args(&ids);
    let output = run_command(stats, "sample local ArrowCol resource metrics").await?;
    let mut pods = HashMap::new();
    for line in String::from_utf8(output.stdout)?
        .lines()
        .filter(|line| !line.trim().is_empty())
    {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 4 {
            bail!("unexpected docker stats output row: {line}");
        }
        let cpu_percent = fields[1]
            .strip_suffix('%')
            .context("docker stats CPU value omitted percent suffix")?
            .parse::<f64>()?;
        if !cpu_percent.is_finite() || cpu_percent < 0.0 {
            bail!("docker stats returned invalid CPU value {cpu_percent}");
        }
        let memory_bytes = parse_memory_bytes(fields[2])?;
        pods.insert(
            fields[0].to_owned(),
            ResourceMetrics {
                cpu_millicores: cpu_percent * 10.0,
                memory_bytes,
            },
        );
    }
    if pods.len() != ids.len() {
        bail!(
            "docker stats returned {} of {} current ArrowCol containers",
            pods.len(),
            ids.len()
        );
    }
    Ok(sum_metrics(&pods))
}

async fn configure_deployments(config: &OatConfig, replicas: u32, concurrency: u32) -> Result<()> {
    for name in config.deployment_names() {
        let mut desired = deployment(config, name).await?;
        if name == config.arrowcol_deployment {
            desired["spec"]["replicas"] = json!(replicas);
        }
        if name == config.dfe_deployment {
            configure_workers(&mut desired, &config.dfe_container, concurrency)?;
        }
        apply_deployment(config, desired).await?;
    }
    Ok(())
}

fn configure_workers(dfe: &mut Value, container_name: &str, concurrency: u32) -> Result<()> {
    let containers = dfe
        .pointer_mut("/spec/template/spec/containers")
        .and_then(Value::as_array_mut)
        .context("DFE Deployment has no container list")?;
    let container = containers
        .iter_mut()
        .find(|container| container.get("name").and_then(Value::as_str) == Some(container_name))
        .with_context(|| format!("DFE container {container_name:?} was not found"))?;
    let args = container
        .get_mut("args")
        .and_then(Value::as_array_mut)
        .context("DFE container args must be an array")?;
    let mut rewritten = Vec::with_capacity(args.len() + 2);
    let mut index = 0;
    while index < args.len() {
        let value = args[index].as_str().context("DFE args must be strings")?;
        if value == "--num-cores" {
            if args
                .get(index + 1)
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<u32>().ok())
                .is_none()
            {
                bail!("--num-cores must have a numeric value");
            }
            index += 2;
        } else if value == "--core-id-range" || value.starts_with("--core-id-range=") {
            bail!("DFE --core-id-range conflicts with benchmark --num-cores");
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
    Ok(())
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

async fn apply_aks_overlay(config: &OatConfig, overlay: &str) -> Result<()> {
    let rendered = kubectl(config, vec!["kustomize", overlay], None)
        .await
        .context("render configured AKS Kustomize overlay")?;
    if rendered.trim().is_empty() {
        bail!("configured AKS Kustomize overlay rendered no resources");
    }

    let validated = kubectl(
        config,
        vec![
            "apply",
            "--namespace",
            &config.namespace,
            "--dry-run=client",
            "-f",
            "-",
            "-o",
            "json",
        ],
        Some(rendered.as_bytes().to_vec()),
    )
    .await
    .context("validate rendered AKS Kustomize overlay")?;
    let mut resources = Vec::new();
    for value in serde_json::Deserializer::from_str(&validated).into_iter::<Value>() {
        let value = value.context("parse validated AKS Kustomize resources")?;
        if let Some(items) = value.get("items").and_then(Value::as_array) {
            resources.extend(items.iter().cloned());
        } else {
            resources.push(value);
        }
    }
    let allowed_names = config.deployment_names().into_iter().map(str::to_owned).collect();
    validate_aks_overlay_resources(&resources, &allowed_names, &config.namespace)?;

    kubectl(
        config,
        vec!["apply", "--namespace", &config.namespace, "-f", "-"],
        Some(rendered.into_bytes()),
    )
    .await
    .context("apply configured AKS Kustomize overlay")?;
    Ok(())
}

fn validate_aks_overlay_resources(resources: &[Value], allowed_names: &HashSet<String>, namespace: &str) -> Result<()> {
    if resources.is_empty() {
        bail!("configured AKS Kustomize overlay rendered no resources");
    }
    for resource in resources {
        let kind = resource.get("kind").and_then(Value::as_str);
        let name = resource.pointer("/metadata/name").and_then(Value::as_str);
        let resource_namespace = resource.pointer("/metadata/namespace").and_then(Value::as_str);
        if kind != Some("Deployment")
            || !name.is_some_and(|name| allowed_names.contains(name))
            || resource_namespace.is_some_and(|resource_namespace| resource_namespace != namespace)
        {
            bail!(
                "configured AKS Kustomize overlay may only include the configured ArrowCol and DFE Deployments in the target namespace"
            );
        }
    }
    Ok(())
}

fn scoped_aks_namespace(prefix: &str, run_id: &str) -> Result<String> {
    let suffix = uuid::Uuid::parse_str(run_id)
        .context("AKS run ID must be a UUID to derive a collision-resistant namespace")?
        .simple()
        .to_string();
    let prefix_limit = 63 - suffix.len() - 1;
    let prefix = prefix
        .chars()
        .take(prefix_limit)
        .collect::<String>()
        .trim_end_matches('-')
        .to_owned();
    if prefix.is_empty() {
        bail!("AKS namespace prefix is too long to leave room for the run UUID");
    }
    let namespace = format!("{prefix}-{suffix}");
    Ok(namespace)
}

fn run_delta_root(shared_root: &str, run_id: &str) -> Result<String> {
    let parsed_run = uuid::Uuid::parse_str(run_id).context("OAT data root requires a UUID run ID")?;
    if parsed_run.to_string() != run_id {
        bail!("OAT data root requires a canonical lowercase UUID run ID");
    }
    let mut root = Url::parse(shared_root).context("parse deployed-stamp deltaRoot")?;
    if root.scheme() != "abfss" || root.query().is_some() || root.fragment().is_some() {
        bail!("deployed-stamp deltaRoot must be a credential-free abfss URL without query or fragment");
    }
    let path = root.path().trim_end_matches('/');
    let parent = path
        .strip_suffix("/dataflow")
        .context("deployed-stamp deltaRoot must end with /dataflow")?;
    root.set_path(&format!("{parent}/runs/{run_id}/dataflow"));
    Ok(root.to_string())
}

fn qap_relative_data_root(run_root: &str) -> Result<String> {
    let path = run_root
        .split_once("://")
        .and_then(|(_, authority_and_path)| authority_and_path.split_once('/'))
        .map(|(_, path)| path)
        .context("run-scoped ArrowCol data root must include a path")?;
    if path.is_empty()
        || path
            .split('/')
            .any(|segment| segment.is_empty() || matches!(segment, "." | "..") || segment.contains('\\'))
        || path.contains('%')
        || path
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        bail!("run-scoped ArrowCol data root contains an unsafe path component");
    }
    let root = Url::parse(run_root).context("parse run-scoped ArrowCol data root")?;
    if root.scheme() != "abfss" || root.query().is_some() || root.fragment().is_some() {
        bail!("run-scoped ArrowCol data root must be a plain abfss URL");
    }
    if root.path().strip_prefix('/') != Some(path) {
        bail!("run-scoped ArrowCol data root must preserve its literal path");
    }
    Ok(path.to_owned())
}

fn qap_bindings(run_id: &str, tables: &[String]) -> Result<Vec<QapBinding>> {
    let parsed = uuid::Uuid::parse_str(run_id).context("QAP binding requires a UUID run ID")?;
    if parsed.to_string() != run_id || tables.is_empty() {
        bail!("QAP binding requires a canonical UUID and at least one table");
    }
    let run_prefix = parsed.simple().to_string();
    let mut seen = HashSet::new();
    tables
        .iter()
        .map(|table| {
            if !table.starts_with("sqlserver_")
                || table.is_empty()
                || !table.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                bail!("OAT QAP table name {table:?} is not a canonical SQL Server DMV identity");
            }
            if !seen.insert(table.as_str()) {
                bail!("OAT QAP table set contains duplicate identity {table}");
            }
            Ok(QapBinding {
                logical_table: table.clone(),
                external_table: format!("oat_{run_prefix}_{table}"),
            })
        })
        .collect()
}

fn aks_qap_relative_path(run_root: &str, table: &str) -> Result<String> {
    if !table.starts_with("sqlserver_") || !table.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
        bail!("OAT QAP table path has an invalid table identity");
    }
    Ok(format!("{run_root}/telemetry/sqlserver/{table}"))
}

fn namespace_matches_run(namespace: &Value, run_id: &str, owner_token: &str, expected_uid: Option<&str>) -> bool {
    let actual_uid = namespace.pointer("/metadata/uid").and_then(Value::as_str);
    let actual_run_id = namespace
        .pointer("/metadata/labels/arrowcol-oat~1run-id")
        .and_then(Value::as_str);
    let actual_owner_token = namespace
        .pointer("/metadata/labels/arrowcol-oat~1owner-token")
        .and_then(Value::as_str);
    let annotated_run_id = namespace
        .pointer("/metadata/annotations/arrowcol-oat~1run-id")
        .and_then(Value::as_str);
    let annotated_owner_token = namespace
        .pointer("/metadata/annotations/arrowcol-oat~1owner-token")
        .and_then(Value::as_str);
    actual_uid.is_some()
        && actual_run_id == Some(run_id)
        && actual_owner_token == Some(owner_token)
        && annotated_run_id == Some(run_id)
        && annotated_owner_token == Some(owner_token)
        && expected_uid.is_none_or(|uid| actual_uid == Some(uid))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NamespaceCleanupAction {
    Delete,
    Ignore,
    Refuse,
}

fn namespace_cleanup_action(namespace_created: bool, namespace_matches: bool) -> NamespaceCleanupAction {
    if namespace_matches {
        NamespaceCleanupAction::Delete
    } else if namespace_created {
        NamespaceCleanupAction::Refuse
    } else {
        NamespaceCleanupAction::Ignore
    }
}

fn namespace_uid_for_cleanup(namespace: &Value, expected_uid: Option<&str>) -> Result<String> {
    let uid = namespace
        .pointer("/metadata/uid")
        .and_then(Value::as_str)
        .context("run-owned AKS namespace UID is missing before deletion")?;
    if expected_uid.is_some_and(|expected| expected != uid) {
        bail!("run-owned AKS namespace UID changed before deletion");
    }
    Ok(uid.to_owned())
}

async fn prepare_aks_platform(config: &OatConfig, platform: &mut AksPlatform) -> Result<()> {
    let parent = std::env::temp_dir().join("arrowcol-oat");
    tokio::fs::create_dir_all(&parent)
        .await
        .with_context(|| format!("create private OAT runtime directory {}", parent.display()))?;
    let runtime_dir = parent.join(&platform.run_id);
    tokio::fs::create_dir(&runtime_dir)
        .await
        .with_context(|| format!("create run-specific OAT runtime directory {}", runtime_dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&runtime_dir, std::fs::Permissions::from_mode(0o700))
            .await
            .context("restrict run-specific AKS runtime directory permissions")?;
    }
    platform.runtime_dir = Some(runtime_dir.clone());

    let runtime_config_path = PathBuf::from(
        config
            .platform
            .runtime_config_file
            .as_deref()
            .context("AKS platform runtime_config_file is missing")?,
    );
    let runtime_bytes = tokio::fs::read(&runtime_config_path)
        .await
        .with_context(|| format!("read deployed-stamp runtime config {}", runtime_config_path.display()))?;
    let mut runtime_config: Value =
        serde_json::from_slice(&runtime_bytes).context("parse deployed-stamp runtime config JSON")?;
    if runtime_config.get("stamp").and_then(Value::as_str) != config.stamp.as_deref() {
        bail!("deployed-stamp runtime config does not match the OAT-selected stamp");
    }
    let shared_data_root = runtime_config
        .get("deltaRoot")
        .and_then(Value::as_str)
        .context("deployed-stamp runtime config has no deltaRoot")?;
    let run_data_root = run_delta_root(shared_data_root, &platform.run_id)?;
    platform.qap_relative_data_root = Some(qap_relative_data_root(&run_data_root)?);
    runtime_config["deltaRoot"] = json!(run_data_root);
    runtime_config["namespace"] = json!(config.namespace);
    runtime_config["oatRunId"] = json!(platform.run_id);
    let generated_config = runtime_dir.join("runtime-config.json");
    tokio::fs::write(&generated_config, serde_json::to_vec(&runtime_config)?)
        .await
        .context("write run-scoped AKS runtime config")?;

    let bootstrap_env = config
        .platform
        .bootstrap_env_file
        .as_deref()
        .context("AKS platform bootstrap_env_file is missing")?;
    let secrets_dir = runtime_dir.join("secrets");
    let runtime_script = std::env::current_dir()?.join("projects/arrowcol-oat/runtime.ts");
    run_npx_runtime(
        &runtime_script,
        &["prepare-secrets", bootstrap_env, secrets_dir.to_string_lossy().as_ref()],
    )
    .await
    .context("prepare private run-specific AKS secrets")?;

    let identity = render_runtime_phase(&runtime_script, &generated_config, "identity", &runtime_dir).await?;
    let namespace_resources = identity
        .iter()
        .filter(|resource| resource.get("kind").and_then(Value::as_str) == Some("Namespace"))
        .cloned()
        .collect::<Vec<_>>();
    if namespace_resources.len() != 1
        || namespace_resources[0].pointer("/metadata/name").and_then(Value::as_str) != Some(config.namespace.as_str())
    {
        bail!("runtime identity phase did not render exactly the expected run namespace");
    }
    let mut namespace_resource = namespace_resources[0].clone();
    namespace_resource["metadata"]["labels"]["arrowcol-oat/run-id"] = json!(platform.run_id);
    namespace_resource["metadata"]["annotations"]["arrowcol-oat/run-id"] = json!(platform.run_id);
    namespace_resource["metadata"]["labels"]["arrowcol-oat/owner-token"] = json!(platform.namespace_owner_token);
    namespace_resource["metadata"]["annotations"]["arrowcol-oat/owner-token"] = json!(platform.namespace_owner_token);
    let namespace_payload = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [namespace_resource],
    }))?;
    platform.namespace_create_attempted = true;
    kubectl(config, vec!["create", "-f", "-"], Some(namespace_payload))
        .await
        .context("create unique OAT-owned namespace without mutating an existing namespace")?;
    platform.namespace_created = true;
    let namespace = kubectl(config, vec!["get", "namespace", &config.namespace, "-o", "json"], None)
        .await
        .context("verify newly created OAT namespace ownership")?;
    let namespace: Value = serde_json::from_str(&namespace).context("parse created OAT namespace")?;
    let uid = namespace
        .pointer("/metadata/uid")
        .and_then(Value::as_str)
        .context("created OAT namespace has no UID")?;
    if namespace
        .pointer("/metadata/labels/arrowcol-oat~1run-id")
        .and_then(Value::as_str)
        != Some(platform.run_id.as_str())
    {
        bail!("created AKS namespace is missing this run's ownership label");
    }
    platform.namespace_uid = Some(uid.to_owned());

    let service_accounts = identity
        .into_iter()
        .filter(|resource| resource.get("kind").and_then(Value::as_str) == Some("ServiceAccount"))
        .collect::<Vec<_>>();
    if service_accounts.len() != 1 {
        bail!("runtime identity phase must render exactly one workload identity ServiceAccount");
    }
    let service_account = service_accounts[0]
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .context("rendered workload identity ServiceAccount has no name")?
        .to_owned();
    platform.service_account = Some(service_account.clone());
    apply_runtime_resources(config, service_accounts).await?;
    let fic_record = runtime_dir.join(format!("run-fic-{}.json", platform.run_id));
    platform.fic_record_path = Some(fic_record.clone());
    platform.fic_prepare_attempted = true;
    run_run_fic_helper(
        "prepare-run-fic",
        &platform.run_id,
        Some(&config.namespace),
        Some(&service_account),
        &fic_record,
        None,
    )
    .await
    .context("prepare exact run-owned workload identity credential")?;
    require_nonempty_record(&fic_record).await?;
    run_run_fic_helper(
        "probe-run-fic",
        &platform.run_id,
        Some(&config.namespace),
        Some(&service_account),
        &fic_record,
        Some("300"),
    )
    .await
    .context("wait for run-owned workload identity token exchange before workload rollout")?;
    let ready_snapshot = runtime_dir.join(format!("run-fic-{}-ready.json", platform.run_id));
    let ready_record = tokio::fs::read_to_string(&fic_record)
        .await
        .context("read ready run-owned workload identity snapshot")?;
    write_private_secret(&ready_snapshot, &ready_record)
        .context("persist immutable ready run-owned workload identity snapshot")?;
    platform.fic_ready_snapshot_path = Some(ready_snapshot);
    create_runtime_secrets(config, &secrets_dir).await?;

    let foundation = render_runtime_phase(&runtime_script, &generated_config, "foundation", &runtime_dir).await?;
    apply_runtime_resources(config, without_identity_resources(foundation)).await?;
    rollout(config, "arrowcol-sql").await?;

    let bootstrap = render_runtime_phase(&runtime_script, &generated_config, "bootstrap", &runtime_dir).await?;
    apply_runtime_resources(config, without_identity_resources(bootstrap)).await?;
    kubectl(
        config,
        vec![
            "wait",
            "--namespace",
            &config.namespace,
            "--for=condition=complete",
            "job/arrowcol-migrate-v11",
            "--timeout=600s",
        ],
        None,
    )
    .await
    .context("wait for run-scoped SQL bootstrap and migration Job")?;
    kubectl(
        config,
        vec![
            "delete",
            "job",
            "arrowcol-migrate-v11",
            "--namespace",
            &config.namespace,
            "--wait=true",
        ],
        None,
    )
    .await
    .context("remove completed run-scoped bootstrap Job")?;

    let workload = render_runtime_phase(&runtime_script, &generated_config, "workload", &runtime_dir).await?;
    platform.router_yaml = Some(rendered_router_yaml(&workload)?);
    apply_runtime_resources(config, without_identity_resources(workload)).await?;
    for name in ["arrowcol-catalog", "arrowcol", "arrowcol-router"] {
        rollout(config, name).await?;
    }
    start_router_metrics_tunnel(config, platform).await?;
    Ok(())
}

async fn run_run_fic_helper(
    operation: &str,
    run_id: &str,
    namespace: Option<&str>,
    service_account: Option<&str>,
    record_path: &Path,
    wait_seconds: Option<&str>,
) -> Result<()> {
    let script = std::env::current_dir()?.join("infra/azure/bootstrap.ts");
    let mut command = Command::new("npx");
    command
        .current_dir(std::env::current_dir()?)
        .args(["--no-install", "tsx"])
        .kill_on_drop(true);
    command.arg(script);
    command.args([operation, "--run-id", run_id]);
    if let Some(namespace) = namespace {
        command.args(["--namespace", namespace]);
    }
    if let Some(service_account) = service_account {
        command.args(["--service-account", service_account]);
    }
    command.arg("--record");
    command.arg(record_path);
    if let Some(wait_seconds) = wait_seconds {
        command.args(["--wait-seconds", wait_seconds]);
    }
    let output = command
        .output()
        .await
        .with_context(|| format!("start {operation} helper"))?;
    if !output.status.success() {
        bail!(
            "{operation} helper failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

async fn run_oat_qap_helper(
    operation: &str,
    run_id: &str,
    manifest_path: Option<&Path>,
    workload_record_path: Option<&Path>,
    workload_stop_record_path: Option<&Path>,
    record_path: &Path,
) -> Result<()> {
    let script = std::env::current_dir()?.join("infra/azure/bootstrap.ts");
    let mut command = Command::new("npx");
    command
        .current_dir(std::env::current_dir()?)
        .args(["--no-install", "tsx"])
        .kill_on_drop(true);
    command.arg(script).arg(operation);
    if operation == "bind-oat-qap" {
        let manifest_path = manifest_path.context("QAP binding requires its exact manifest path")?;
        command.args(["--run-id", run_id, "--manifest"]);
        command.arg(manifest_path);
        command.arg("--workload-record");
        command.arg(workload_record_path.context("QAP binding requires the verified workload ownership record")?);
    }
    if operation == "cleanup-oat-qap" {
        command.arg("--workload-stop-record");
        command.arg(workload_stop_record_path.context("QAP cleanup requires the exact-run workload stop receipt")?);
    }
    command.arg("--record");
    command.arg(record_path);
    let output = command
        .output()
        .await
        .with_context(|| format!("start {operation} helper"))?;
    if !output.status.success() {
        bail!(
            "{operation} helper failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

async fn require_nonempty_record(record_path: &Path) -> Result<()> {
    let metadata = tokio::fs::metadata(record_path)
        .await
        .with_context(|| format!("inspect workload identity ownership record {}", record_path.display()))?;
    if !metadata.is_file() || metadata.len() == 0 {
        bail!("workload identity helper did not persist a nonempty ownership record");
    }
    Ok(())
}

async fn start_router_metrics_tunnel(config: &OatConfig, platform: &mut AksPlatform) -> Result<()> {
    let reservation = TcpListener::bind("127.0.0.1:0")
        .await
        .context("reserve a loopback port for the AKS router metrics tunnel")?;
    let port = reservation.local_addr()?.port();
    drop(reservation);

    let port_mapping = format!("{port}:50050");
    let mut command = Command::new("kubectl");
    if let Some(context) = &config.kubectl_context {
        command.args(["--context", context]);
    }
    command
        .args([
            "port-forward",
            "--address",
            "127.0.0.1",
            "--namespace",
            &config.namespace,
            "service/arrowcol-router",
            &port_mapping,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().context("start owned AKS router metrics port-forward")?;
    let stderr = child
        .stderr
        .take()
        .context("capture AKS router port-forward diagnostics")?;
    let stderr = tokio::spawn(async move {
        let mut bytes = Vec::new();
        let mut stderr = stderr;
        stderr.read_to_end(&mut bytes).await.map(|_| bytes)
    });
    let mut forward = OwnedKubectlProcess { child, stderr };
    let metrics_url = format!("http://127.0.0.1:{port}/metrics");
    let ready_url = format!("http://127.0.0.1:{port}/readyz");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("build AKS router tunnel readiness client")?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = forward
            .child
            .try_wait()
            .context("check AKS router port-forward process")?
        {
            let stderr = collect_kubectl_stderr(forward.stderr).await?;
            bail!(
                "AKS router port-forward exited before readiness with {status}: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        if client
            .get(&ready_url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            platform.router_metrics_url = Some(metrics_url);
            platform.router_tunnel = Some(forward);
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let stderr = collect_kubectl_stderr(stop_kubectl_process(forward).await?).await?;
            bail!(
                "AKS router metrics tunnel did not become ready within 60 seconds: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn stop_kubectl_process(mut forward: OwnedKubectlProcess) -> Result<JoinHandle<std::io::Result<Vec<u8>>>> {
    if forward
        .child
        .try_wait()
        .context("check owned kubectl process before stopping")?
        .is_none()
    {
        forward.child.start_kill().context("stop owned kubectl process")?;
    }
    forward
        .child
        .wait()
        .await
        .context("wait for owned kubectl process to stop")?;
    Ok(forward.stderr)
}

async fn collect_kubectl_stderr(stderr: JoinHandle<std::io::Result<Vec<u8>>>) -> Result<Vec<u8>> {
    stderr
        .await
        .context("join kubectl process diagnostics reader")?
        .context("read kubectl process diagnostics")
}

async fn start_kubectl_api_proxy(config: &OatConfig) -> Result<(OwnedKubectlProcess, String)> {
    let reservation = TcpListener::bind("127.0.0.1:0")
        .await
        .context("reserve a local port for the authenticated Kubernetes API proxy")?;
    let port = reservation
        .local_addr()
        .context("read reserved Kubernetes API proxy port")?
        .port();
    drop(reservation);

    let port_arg = port.to_string();
    let mut command = Command::new("kubectl");
    if let Some(context) = config.kubectl_context.as_deref() {
        command.args(["--context", context]);
    }
    command
        .args(["proxy", "--address", "127.0.0.1", "--port", &port_arg])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().context("start authenticated Kubernetes API proxy")?;
    let stderr = child
        .stderr
        .take()
        .context("capture Kubernetes API proxy diagnostics")?;
    let stderr = tokio::spawn(async move {
        let mut bytes = Vec::new();
        let mut stderr = stderr;
        stderr.read_to_end(&mut bytes).await.map(|_| bytes)
    });
    let mut proxy = OwnedKubectlProcess { child, stderr };
    let api_url = format!("http://127.0.0.1:{port}");
    let readiness_url = format!("{api_url}/api");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .no_proxy()
        .build()
        .context("build Kubernetes API proxy readiness client")?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        if let Some(status) = proxy.child.try_wait().context("check Kubernetes API proxy process")? {
            let stderr = collect_kubectl_stderr(proxy.stderr).await?;
            bail!(
                "authenticated Kubernetes API proxy exited before readiness with {status}: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        if client
            .get(&readiness_url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok((proxy, api_url));
        }
        if tokio::time::Instant::now() >= deadline {
            let stderr = collect_kubectl_stderr(stop_kubectl_process(proxy).await?).await?;
            bail!(
                "authenticated Kubernetes API proxy did not become ready within 30 seconds: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn delete_namespace_with_uid_precondition(api_url: &str, namespace: &str, uid: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .no_proxy()
        .build()
        .context("build Kubernetes namespace deletion client")?;
    let endpoint = format!("{}/api/v1/namespaces/{namespace}", api_url.trim_end_matches('/'));
    let response = client
        .delete(endpoint)
        .json(&serde_json::json!({
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "preconditions": { "uid": uid }
        }))
        .send()
        .await
        .context("send UID-preconditioned Kubernetes namespace deletion")?;
    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .context("read Kubernetes namespace deletion failure response")?;
        bail!(
            "UID-preconditioned namespace deletion returned HTTP {status}: {}",
            body.trim()
        );
    }
    Ok(())
}

async fn delete_run_owned_namespace_with_uid(config: &OatConfig, namespace_uid: &str) -> Result<()> {
    let (proxy, api_url) = start_kubectl_api_proxy(config).await?;
    let delete_result = delete_namespace_with_uid_precondition(&api_url, &config.namespace, namespace_uid).await;
    let stop_result = match stop_kubectl_process(proxy).await {
        Ok(stderr) => collect_kubectl_stderr(stderr).await.map(|_| ()),
        Err(error) => Err(error),
    };
    match (delete_result, stop_result) {
        (Err(error), Err(stop_error)) => {
            Err(error.context(format!("also failed to stop Kubernetes API proxy: {stop_error:#}")))
        }
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error).context("stop Kubernetes API proxy after namespace deletion request"),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn wait_for_run_owned_namespace_absence(config: &OatConfig) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
    loop {
        let output = kubectl(
            config,
            vec![
                "get",
                "namespace",
                &config.namespace,
                "--ignore-not-found=true",
                "-o",
                "name",
                "--request-timeout=30s",
            ],
            None,
        )
        .await
        .context("confirm run-owned AKS namespace absence after deletion")?;
        if output.trim().is_empty() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "run-owned AKS namespace still exists after bounded deletion wait: {}",
                output.trim()
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn without_identity_resources(resources: Vec<Value>) -> Vec<Value> {
    resources
        .into_iter()
        .filter(|resource| {
            !matches!(
                resource.get("kind").and_then(Value::as_str),
                Some("Namespace" | "ServiceAccount")
            )
        })
        .collect()
}

async fn render_runtime_phase(
    runtime_script: &Path,
    config_path: &Path,
    phase: &str,
    runtime_dir: &Path,
) -> Result<Vec<Value>> {
    let output_path = runtime_dir.join(format!("{phase}.json"));
    run_npx_runtime(
        runtime_script,
        &[
            "render",
            config_path.to_string_lossy().as_ref(),
            phase,
            output_path.to_string_lossy().as_ref(),
        ],
    )
    .await
    .with_context(|| format!("render AKS runtime phase {phase}"))?;
    let output = tokio::fs::read(&output_path)
        .await
        .with_context(|| format!("read rendered AKS {phase} resources"))?;
    let list: Value = serde_json::from_slice(&output).context("parse rendered AKS resource list")?;
    let resources = list
        .get("items")
        .and_then(Value::as_array)
        .context("runtime phase output is not a Kubernetes List")?
        .clone();
    if resources.is_empty() {
        bail!("AKS runtime phase {phase} rendered no resources");
    }
    Ok(resources)
}

async fn run_npx_runtime(runtime_script: &Path, args: &[&str]) -> Result<()> {
    let workspace = std::env::current_dir().context("resolve repository root for OAT runtime helper")?;
    let mut command = Command::new("npx");
    command
        .current_dir(&workspace)
        .args(["--no-install", "tsx"])
        .arg(runtime_script)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    run_command(command, "run existing OAT runtime helper").await?;
    Ok(())
}

async fn apply_runtime_resources(config: &OatConfig, resources: Vec<Value>) -> Result<()> {
    if resources.is_empty() {
        bail!("refusing to apply an empty AKS runtime phase");
    }
    let payload = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": resources,
    }))?;
    kubectl(
        config,
        vec!["apply", "--namespace", &config.namespace, "-f", "-"],
        Some(payload),
    )
    .await
    .context("apply run-scoped AKS runtime resources")?;
    Ok(())
}

async fn create_runtime_secrets(config: &OatConfig, secrets_dir: &Path) -> Result<()> {
    let secret_specs: [(&str, &[(&str, &str)]); 5] = [
        ("arrowcol-sql-admin", &[("password", "sa-password")]),
        ("arrowcol-sql-runtime", &[("password", "app-password")]),
        ("arrowcol-sql-migration", &[("password", "migration-password")]),
        (
            "arrowcol-router-auth",
            &[("edge-hmac", "edge-hmac"), ("internal-hmac", "internal-hmac")],
        ),
        ("arrowcol-generation", &[("generation", "generation")]),
    ];
    for (name, keys) in secret_specs {
        let mut arguments = vec!["create", "secret", "generic", name, "--namespace", &config.namespace];
        let from_files = keys
            .iter()
            .map(|(key, file)| format!("--from-file={key}={}", secrets_dir.join(file).display()))
            .collect::<Vec<_>>();
        arguments.extend(from_files.iter().map(String::as_str));
        kubectl(config, arguments, None)
            .await
            .with_context(|| format!("create run-scoped Kubernetes Secret {name} from private files"))?;
    }
    Ok(())
}

async fn write_workload_stop_receipt(
    config: &OatConfig,
    platform: &mut AksPlatform,
    namespace_uid: &str,
) -> Result<()> {
    let workload_record_path = platform
        .fic_ready_snapshot_path
        .as_deref()
        .context("verified AKS workload ownership record is missing")?;
    let workload_record: Value = serde_json::from_slice(
        &tokio::fs::read(workload_record_path)
            .await
            .context("read verified AKS workload ownership record")?,
    )
    .context("parse verified AKS workload ownership record")?;
    let field = |name| {
        workload_record
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .with_context(|| format!("workload ownership record has no {name}"))
    };
    let aks_id = field("aksId")?;
    let kubectl_server = field("server")?;
    let server = Url::parse(kubectl_server).context("parse workload ownership kubectl server")?;
    if workload_record.get("phase").and_then(Value::as_str) != Some("ready")
        || workload_record.get("runId").and_then(Value::as_str) != Some(platform.run_id.as_str())
        || workload_record.get("namespace").and_then(Value::as_str) != Some(config.namespace.as_str())
        || workload_record.get("namespaceUid").and_then(Value::as_str) != Some(namespace_uid)
        || workload_record.get("namespaceOwner").and_then(Value::as_str)
            != Some(platform.namespace_owner_token.as_str())
        || server.scheme() != "https"
        || server.host_str().is_none()
        || !server.username().is_empty()
        || server.password().is_some()
        || server.port().is_some()
        || server.path() != "/"
        || server.query().is_some()
        || server.fragment().is_some()
    {
        bail!("workload ownership record does not match the deleted run namespace and AKS server");
    }
    let receipt_path = platform
        .runtime_dir
        .as_deref()
        .context("AKS runtime directory is not available")?
        .join("workload-stop-receipt.json");
    let receipt = json!({
        "schema_version": 1,
        "workload_record": workload_record,
        "run_id": platform.run_id,
        "namespace": config.namespace,
        "namespace_uid": namespace_uid,
        "namespace_run_id": platform.run_id,
        "namespace_owner_token": platform.namespace_owner_token,
        "aks_id": aks_id,
        "kubectl_server": kubectl_server,
        "delete_succeeded": true,
        "absence_confirmed": true,
        "observed_at_unix_ms": unix_time_ms(),
    });
    write_private_secret(&receipt_path, &serde_json::to_string(&receipt)?)
        .context("persist exact-run AKS workload stop receipt")?;
    platform.workload_stop_record_path = Some(receipt_path);
    Ok(())
}

async fn cleanup_aks_platform(config: &OatConfig, platform: &mut AksPlatform) -> Result<()> {
    let mut failures = Vec::new();
    let mut namespace_removed = false;
    if let Some(forward) = platform.router_tunnel.take() {
        match stop_kubectl_process(forward).await {
            Ok(stderr) => {
                if let Err(error) = collect_kubectl_stderr(stderr).await {
                    failures.push(format!(
                        "collect AKS router tunnel diagnostics during cleanup: {error:#}"
                    ));
                }
            }
            Err(error) => failures.push(format!("stop owned AKS router metrics tunnel: {error:#}")),
        }
        platform.router_metrics_url = None;
    }
    if platform.namespace_created || platform.namespace_create_attempted {
        match kubectl(config, vec!["get", "namespace", &config.namespace, "-o", "json"], None).await {
            Ok(output) => {
                let namespace: Value = serde_json::from_str(&output).context("parse OAT namespace during cleanup")?;
                let ours = namespace_matches_run(
                    &namespace,
                    &platform.run_id,
                    &platform.namespace_owner_token,
                    platform.namespace_uid.as_deref(),
                );
                match namespace_cleanup_action(platform.namespace_created, ours) {
                    NamespaceCleanupAction::Delete => {
                        let namespace_uid = namespace_uid_for_cleanup(&namespace, platform.namespace_uid.as_deref())?;
                        platform.namespace_uid = Some(namespace_uid.clone());
                        if let Err(error) = delete_run_owned_namespace_with_uid(config, &namespace_uid).await {
                            failures.push(format!(
                                "delete run-owned AKS namespace with UID precondition: {error:#}"
                            ));
                        } else if let Err(error) = wait_for_run_owned_namespace_absence(config).await {
                            failures.push(format!(
                                "confirm run-owned AKS namespace absence after deletion: {error:#}"
                            ));
                        } else if let Err(error) = write_workload_stop_receipt(config, platform, &namespace_uid).await {
                            failures.push(format!("write exact-run workload stop receipt: {error:#}"));
                        } else {
                            namespace_removed = true;
                            platform.namespace_created = false;
                            platform.namespace_create_attempted = false;
                            platform.namespace_uid = None;
                        }
                    }
                    NamespaceCleanupAction::Ignore => {
                        platform.namespace_create_attempted = false;
                    }
                    NamespaceCleanupAction::Refuse => {
                        failures
                            .push("refusing AKS cleanup because namespace run ownership markers changed".to_owned());
                    }
                }
                if namespace_removed && platform.qap_bind_attempted {
                    match (
                        platform.qap_record_path.as_deref(),
                        platform.workload_stop_record_path.as_deref(),
                    ) {
                        (Some(record_path), Some(stop_record_path))
                            if tokio::fs::try_exists(record_path).await?
                                && tokio::fs::try_exists(stop_record_path).await? =>
                        {
                            if let Err(error) = run_oat_qap_helper(
                                "cleanup-oat-qap",
                                &platform.run_id,
                                None,
                                None,
                                Some(stop_record_path),
                                record_path,
                            )
                            .await
                            {
                                failures.push(format!("remove exact run-owned QAP bindings: {error:#}"));
                            }
                        }
                        _ => failures.push("QAP cleanup requires both ownership and workload-stop records".to_owned()),
                    }
                }
            }
            Err(error) => failures.push(format!("verify run-owned namespace before cleanup: {error:#}")),
        }
    }
    if namespace_removed && platform.fic_prepare_attempted {
        if let Some(record_path) = platform.fic_record_path.as_deref() {
            if let Err(error) =
                run_run_fic_helper("cleanup-run-fic", &platform.run_id, None, None, record_path, None).await
            {
                failures.push(format!(
                    "remove exact run-owned workload identity credential: {error:#}"
                ));
            }
        } else {
            failures.push("workload identity cleanup has no durable run record path".to_owned());
        }
    }
    if let Some(runtime_dir) = platform.runtime_dir.take() {
        let retention = runtime_evidence_retention(
            platform.fic_prepare_attempted,
            platform.fic_record_path.as_deref(),
            platform.fic_ready_snapshot_path.as_deref(),
            platform.qap_bind_attempted,
            platform.qap_record_path.as_deref(),
            platform.workload_stop_record_path.as_deref(),
            platform.qap_outcome_unresolved || !failures.is_empty(),
        );
        if let Err(error) = apply_runtime_evidence_retention(&runtime_dir, retention).await {
            failures.push(format!(
                "remove private AKS runtime files {}: {error:#}",
                runtime_dir.display()
            ));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("AKS cleanup was incomplete: {}", failures.join("; ")))
    }
}

enum RuntimeEvidenceRetention {
    PreserveAll,
    PruneExcept(Vec<PathBuf>),
}

fn runtime_evidence_retention(
    fic_prepare_attempted: bool,
    fic_record_path: Option<&Path>,
    fic_ready_snapshot_path: Option<&Path>,
    qap_bind_attempted: bool,
    qap_record_path: Option<&Path>,
    workload_stop_record_path: Option<&Path>,
    unresolved_outcome: bool,
) -> RuntimeEvidenceRetention {
    if unresolved_outcome {
        return RuntimeEvidenceRetention::PreserveAll;
    }

    let mut preserve = Vec::new();
    if fic_prepare_attempted {
        preserve.extend(fic_record_path.map(Path::to_owned));
        preserve.extend(fic_ready_snapshot_path.map(Path::to_owned));
    }
    if qap_bind_attempted {
        if let Some(record_path) = qap_record_path {
            preserve.push(record_path.to_owned());
            preserve.push(qap_lease_sidecar_path(record_path));
        }
        preserve.extend(workload_stop_record_path.map(Path::to_owned));
    }
    RuntimeEvidenceRetention::PruneExcept(preserve)
}

fn qap_lease_sidecar_path(record_path: &Path) -> PathBuf {
    let mut path = record_path.as_os_str().to_os_string();
    path.push(".lease");
    PathBuf::from(path)
}

async fn apply_runtime_evidence_retention(directory: &Path, retention: RuntimeEvidenceRetention) -> Result<()> {
    match retention {
        RuntimeEvidenceRetention::PreserveAll => Ok(()),
        RuntimeEvidenceRetention::PruneExcept(preserve) => remove_runtime_dir_except(directory, &preserve).await,
    }
}

async fn remove_runtime_dir_except(directory: &Path, preserve: &[PathBuf]) -> Result<()> {
    let mut entries = tokio::fs::read_dir(directory)
        .await
        .with_context(|| format!("read private runtime directory {}", directory.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if preserve.iter().any(|preserved| preserved == &path) {
            continue;
        }
        if entry.file_type().await?.is_dir() {
            tokio::fs::remove_dir_all(&path)
                .await
                .with_context(|| format!("remove private runtime directory {}", path.display()))?;
        } else {
            tokio::fs::remove_file(&path)
                .await
                .with_context(|| format!("remove private runtime file {}", path.display()))?;
        }
    }
    if preserve.is_empty() {
        tokio::fs::remove_dir(directory)
            .await
            .with_context(|| format!("remove empty private runtime directory {}", directory.display()))?;
    }
    Ok(())
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
            "--timeout=600s",
        ],
        None,
    )
    .await
    .with_context(|| format!("wait for Deployment {name} rollout"))?;
    Ok(())
}

async fn collect_aks_metrics(config: &OatConfig) -> Result<ResourceMetrics> {
    let mut pods = top_metrics(config, &config.arrowcol_pod_selector).await?;
    if config.arrowcol_pod_selector != config.dfe_pod_selector {
        pods.extend(top_metrics(config, &config.dfe_pod_selector).await?);
    }
    Ok(sum_metrics(&pods))
}

fn sum_metrics(pods: &HashMap<String, ResourceMetrics>) -> ResourceMetrics {
    ResourceMetrics {
        cpu_millicores: pods.values().map(|pod| pod.cpu_millicores).sum(),
        memory_bytes: pods
            .values()
            .fold(0_u64, |sum, pod| sum.saturating_add(pod.memory_bytes)),
    }
}

async fn top_metrics(config: &OatConfig, selector: &str) -> Result<HashMap<String, ResourceMetrics>> {
    let snapshot = kubectl(
        config,
        vec![
            "get",
            "pods",
            "--namespace",
            &config.namespace,
            "--selector",
            selector,
            "-o",
            "json",
        ],
        None,
    )
    .await?;
    let expected = active_pod_names(&serde_json::from_str(&snapshot)?)?;
    let wait = async {
        loop {
            let result = kubectl(
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
            .await;
            match result {
                Ok(output) => {
                    let metrics = parse_top_metrics(&output)?;
                    if metrics_complete(&expected, &metrics) {
                        return Ok(metrics);
                    }
                    tracing::warn!(selector, expected = ?expected, observed = ?metrics.keys(), "waiting for all current pod metrics");
                }
                Err(error) if error.to_string().contains("metrics not available yet") => {
                    tracing::warn!(selector, %error, "waiting for metrics-server scrape after rollout");
                }
                Err(error) => return Err(error),
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(120), wait)
        .await
        .with_context(|| {
            format!("timed out waiting for complete metrics for selector {selector:?}, pods {expected:?}")
        })?
}

fn active_pod_names(snapshot: &Value) -> Result<HashSet<String>> {
    let names = snapshot
        .get("items")
        .and_then(Value::as_array)
        .context("pod snapshot has no items array")?
        .iter()
        .filter(|pod| pod.pointer("/metadata/deletionTimestamp").is_none_or(Value::is_null))
        .map(|pod| {
            pod.pointer("/metadata/name")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .context("pod snapshot has an unnamed pod")
        })
        .collect::<Result<HashSet<_>>>()?;
    if names.is_empty() {
        bail!("pod snapshot contains no active pods to measure");
    }
    Ok(names)
}

fn metrics_complete(expected: &HashSet<String>, metrics: &HashMap<String, ResourceMetrics>) -> bool {
    metrics.len() == expected.len() && expected.iter().all(|name| metrics.contains_key(name))
}

fn parse_top_metrics(output: &str) -> Result<HashMap<String, ResourceMetrics>> {
    let mut metrics = HashMap::new();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let columns = line.split_whitespace().collect::<Vec<_>>();
        if columns.len() < 3 {
            bail!("unexpected kubectl top output row: {line}");
        }
        if columns[1] == "<unknown>" || columns[2] == "<unknown>" {
            continue;
        }
        metrics.insert(
            columns[0].to_owned(),
            ResourceMetrics {
                cpu_millicores: parse_cpu_millicores(columns[1])?,
                memory_bytes: parse_memory_bytes(columns[2])?,
            },
        );
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
    let value = value.strip_suffix('B').unwrap_or(value);
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
    command
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
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
    String::from_utf8(output.stdout).context("kubectl output was not UTF-8")
}

async fn publish_results(config: &OatConfig, results: &[OatResult]) -> Result<()> {
    let batch = parquet_batch(results)?;
    let authoritative = Url::parse(&config.effective_result_url()?)?;
    let mirror = Url::parse(&config.effective_website_url()?)?;
    if authoritative.scheme() == "file" {
        let target = local_file_path(&authoritative)?;
        merge_local_results(&target, &batch).await?;
    } else {
        let path = ObjectPath::from_url_path(authoritative.path())?;
        let store = object_store_for_url(&authoritative)?;
        put_parquet(store, path, batch).await?;
    }

    for _ in 0..8 {
        let current_authoritative = read_destination_versioned(&authoritative)
            .await
            .context("read authoritative benchmark Parquet after merge")?;
        if !write_exact_destination(&mirror, &authoritative, &current_authoritative)
            .await
            .context("publish authoritative benchmark Parquet to website destination")?
        {
            continue;
        }
        let verified_authoritative = read_destination(&authoritative)
            .await
            .context("verify authoritative benchmark Parquet")?;
        let verified_mirror = read_destination(&mirror)
            .await
            .context("verify website benchmark Parquet copy")?;
        if verified_authoritative == verified_mirror {
            return Ok(());
        }
    }
    bail!("authoritative and website Parquet objects did not converge after 8 verification attempts")
}

fn local_file_path(url: &Url) -> Result<PathBuf> {
    url.to_file_path()
        .map_err(|()| anyhow::anyhow!("invalid local Parquet file URL {url}"))
}

async fn merge_local_results(target: &Path, incoming: &RecordBatch) -> Result<()> {
    let parent = target.parent().context("local results file has no parent directory")?;
    tokio::fs::create_dir_all(parent)
        .await
        .with_context(|| format!("create local results directory {}", parent.display()))?;
    let _lock = acquire_local_publication_lock(target).await?;
    let existing = match tokio::fs::read(target).await {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).with_context(|| format!("read local results file {}", target.display())),
    };
    let bytes = match existing {
        Some(existing) => merge_parquet_bytes(&existing, incoming)?,
        None => encode_parquet(incoming)?,
    };
    write_local_file_unlocked(target, &bytes).await
}

async fn write_local_file_unlocked(target: &Path, bytes: &[u8]) -> Result<()> {
    let parent = target.parent().context("local results file has no parent directory")?;
    let temporary = parent.join(format!(".results-{}.tmp", uuid::Uuid::new_v4()));
    if let Err(error) = tokio::fs::write(&temporary, bytes).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error).with_context(|| format!("write temporary Parquet results {}", temporary.display()));
    }
    if let Err(error) = tokio::fs::rename(&temporary, target).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error).with_context(|| format!("publish local Parquet results {}", target.display()));
    }
    Ok(())
}

struct LocalPublicationLock(std::fs::File);

impl Drop for LocalPublicationLock {
    fn drop(&mut self) {
        if let Err(error) = FileExt::unlock(&self.0) {
            tracing::warn!(error = %error, "could not release local Parquet publication lock");
        }
    }
}

async fn acquire_local_publication_lock(target: &Path) -> Result<LocalPublicationLock> {
    let parent = target.parent().context("local results file has no parent directory")?;
    let file_name = target.file_name().context("local results file has no file name")?;
    let lock_path = parent.join(format!("{}.lock", file_name.to_string_lossy()));
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("open local results lock {}", lock_path.display()))?;
    for _ in 0..150 {
        match FileExt::try_lock_exclusive(&lock) {
            Ok(()) => return Ok(LocalPublicationLock(lock)),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("lock local results file {}", target.display()));
            }
        }
    }
    bail!(
        "timed out waiting 15 seconds for local results lock {}",
        target.display()
    )
}

async fn read_destination(url: &Url) -> Result<Vec<u8>> {
    Ok(read_destination_versioned(url).await?.bytes)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DestinationVersion {
    e_tag: Option<String>,
    version: Option<String>,
}

#[derive(Clone, Debug)]
struct DestinationSnapshot {
    bytes: Vec<u8>,
    version: Option<DestinationVersion>,
}

async fn read_destination_versioned(url: &Url) -> Result<DestinationSnapshot> {
    if url.scheme() == "file" {
        let path = local_file_path(url)?;
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read local Parquet destination {}", path.display()));
        return Ok(DestinationSnapshot {
            bytes: bytes?,
            version: None,
        });
    }
    let path = ObjectPath::from_url_path(url.path())?;
    let store = object_store_for_url(url)?;
    read_cloud_destination_versioned(store, path).await
}

async fn read_cloud_destination_versioned(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
) -> Result<DestinationSnapshot> {
    let object = store
        .get_opts(&path, GetOptions::default())
        .await
        .with_context(|| format!("read Parquet object {path}"))?;
    let version = DestinationVersion {
        e_tag: object.meta.e_tag.clone(),
        version: object.meta.version.clone(),
    };
    if version.e_tag.is_none() && version.version.is_none() {
        bail!("object store returned no conditional version for Parquet object {path}");
    }
    Ok(DestinationSnapshot {
        bytes: object.bytes().await?.to_vec(),
        version: Some(version),
    })
}

async fn write_exact_destination(url: &Url, authoritative: &Url, snapshot: &DestinationSnapshot) -> Result<bool> {
    if url.scheme() == "file" {
        let target = local_file_path(url)?;
        let parent = target.parent().context("local results file has no parent directory")?;
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create local results directory {}", parent.display()))?;
        let _lock = acquire_local_publication_lock(&target).await?;
        if read_destination(authoritative).await? != snapshot.bytes {
            return Ok(false);
        }
        write_local_file_unlocked(&target, &snapshot.bytes).await?;
        return Ok(true);
    }
    let path = ObjectPath::from_url_path(url.path())?;
    let store = object_store_for_url(url)?;
    write_cloud_exact_destination(store, path, snapshot, || read_destination_versioned(authoritative)).await
}

async fn write_cloud_exact_destination<F, Fut>(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    snapshot: &DestinationSnapshot,
    read_authoritative: F,
) -> Result<bool>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<DestinationSnapshot>>,
{
    let mode = match store.get_opts(&path, GetOptions::default()).await {
        Ok(object) => {
            let version = UpdateVersion {
                e_tag: object.meta.e_tag.clone(),
                version: object.meta.version.clone(),
            };
            if version.e_tag.is_none() && version.version.is_none() {
                bail!("object store returned no conditional version for website Parquet object {path}");
            }
            PutMode::Update(version)
        }
        Err(object_store::Error::NotFound { .. }) => PutMode::Create,
        Err(error) => return Err(error).with_context(|| format!("read website Parquet object {path}")),
    };
    let current_authoritative = read_authoritative().await?;
    if current_authoritative.bytes != snapshot.bytes || current_authoritative.version != snapshot.version {
        return Ok(false);
    }
    match store
        .put_opts(&path, PutPayload::from(snapshot.bytes.clone()), PutOptions::from(mode))
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(error) => Err(error).with_context(|| format!("write website Parquet object {path}")),
    }
}

async fn put_parquet(store: Arc<dyn ObjectStore>, path: ObjectPath, incoming: RecordBatch) -> Result<()> {
    for _ in 0..8 {
        let current = match store.get_opts(&path, GetOptions::default()).await {
            Ok(object) => {
                let version = UpdateVersion {
                    e_tag: object.meta.e_tag.clone(),
                    version: object.meta.version.clone(),
                };
                if version.e_tag.is_none() && version.version.is_none() {
                    bail!("object store returned no conditional version for existing benchmark Parquet object {path}");
                }
                let existing_bytes = object.bytes().await?;
                Some((
                    merge_parquet_bytes(&existing_bytes, &incoming)?,
                    PutMode::Update(version),
                ))
            }
            Err(object_store::Error::NotFound { .. }) => Some((encode_parquet(&incoming)?, PutMode::Create)),
            Err(error) => return Err(error).with_context(|| format!("read benchmark Parquet object {path}")),
        };
        let (payload, mode) = current.context("Parquet merge unexpectedly produced no update")?;
        match store
            .put_opts(&path, PutPayload::from(payload), PutOptions::from(mode))
            .await
        {
            Ok(_) => return Ok(()),
            Err(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. }) => continue,
            Err(error) => return Err(error).with_context(|| format!("write benchmark Parquet object {path}")),
        }
    }
    bail!("benchmark Parquet object {path} changed during all 8 merge attempts")
}

fn merge_parquet_bytes(existing_bytes: &[u8], incoming: &RecordBatch) -> Result<Vec<u8>> {
    let existing_reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(existing_bytes))
        .context("read existing benchmark Parquet object")?;
    let schema = result_schema();
    let existing_schema = existing_reader.schema();
    if incoming.schema().as_ref() != schema.as_ref()
        || (existing_schema.as_ref() != schema.as_ref() && existing_schema.as_ref() != legacy_result_schema().as_ref())
    {
        bail!("existing benchmark Parquet schema changed");
    }
    let mut batches = existing_reader
        .build()?
        .map(|batch| -> Result<_> {
            let batch = batch?;
            let versions = batch
                .column_by_name("schema_version")
                .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
                .context("result Parquet has no int32 schema_version")?;
            if versions.iter().any(|version| version != Some(2)) {
                bail!("existing benchmark Parquet contains an unsupported schema_version");
            }
            Ok(RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())?)
        })
        .collect::<Result<Vec<_>>>()?;
    batches.push(incoming.clone());
    let merged = concat_batches(&schema, &batches)?;
    encode_parquet(&merged)
}

fn deduplicate_results(batch: &RecordBatch) -> Result<RecordBatch> {
    let run_ids = batch
        .column_by_name("run_id")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .context("result Parquet has no UTF-8 run_id column")?;
    let tables = batch
        .column_by_name("table")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .context("result Parquet has no UTF-8 table column")?;
    let mut latest = HashMap::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        if run_ids.is_null(index) || tables.is_null(index) {
            bail!("result Parquet contains a null run_id or table key");
        }
        let run_id = run_ids.value(index);
        let table = tables.value(index);
        latest.insert((run_id, table), index);
    }
    let indices = Int64Array::from_iter_values(
        (0..batch.num_rows())
            .filter(|index| latest.get(&(run_ids.value(*index), tables.value(*index))) == Some(index))
            .map(|index| index as i64),
    );
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None).map_err(anyhow::Error::from))
        .collect::<Result<Vec<ArrayRef>>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

fn parquet_batch(results: &[OatResult]) -> Result<RecordBatch> {
    let schema = result_schema();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from_iter_values(results.iter().map(|_| 2))),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.mode.as_str()),
        )),
        Arc::new(StringArray::from(
            results.iter().map(|row| row.stamp.as_deref()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.host.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.experiment.as_str()),
        )),
        Arc::new(
            TimestampMillisecondArray::from_iter_values(results.iter().map(|row| row.started_at_ms))
                .with_timezone("UTC"),
        ),
        Arc::new(Float64Array::from_iter_values(
            results.iter().map(|row| row.measure_seconds),
        )),
        Arc::new(Int32Array::from_iter_values(results.iter().map(|row| row.arrowcols))),
        Arc::new(Int32Array::from_iter_values(results.iter().map(|row| row.clients))),
        Arc::new(Int32Array::from_iter_values(results.iter().map(|row| row.tables))),
        Arc::new(StringArray::from(
            results.iter().map(|row| row.table_owner.as_deref()).collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            results.iter().map(|row| row.pinning_ok).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.status.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.git_sha.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.tunables_json.as_str()),
        )),
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
        Arc::new(UInt64Array::from(
            results.iter().map(|row| row.rows).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            results.iter().map(|row| row.duration_ms).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            results.iter().map(|row| row.rows_per_second).collect::<Vec<_>>(),
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
    Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
}

fn result_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("schema_version", DataType::Int32, false),
        Field::new("mode", DataType::Utf8, false),
        Field::new("stamp", DataType::Utf8, true),
        Field::new("host", DataType::Utf8, false),
        Field::new("experiment", DataType::Utf8, false),
        Field::new(
            "started_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("measure_seconds", DataType::Float64, false),
        Field::new("arrowcols", DataType::Int32, false),
        Field::new("clients", DataType::Int32, false),
        Field::new("tables", DataType::Int32, false),
        Field::new("table_owner", DataType::Utf8, true),
        Field::new("pinning_ok", DataType::Boolean, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("git_sha", DataType::Utf8, false),
        Field::new("tunables_json", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("replicas", DataType::UInt64, false),
        Field::new("dfe_worker_concurrency", DataType::UInt64, false),
        Field::new("table", DataType::Utf8, false),
        Field::new("rows", DataType::UInt64, true),
        Field::new("duration_ms", DataType::UInt64, true),
        Field::new("rows_per_second", DataType::Float64, true),
        Field::new("cpu_millicores", DataType::Float64, true),
        Field::new("memory_bytes", DataType::UInt64, true),
        Field::new("qap_observation_complete", DataType::UInt64, false),
        Field::new("qap_observed_tables", DataType::UInt64, false),
        Field::new("qap_completion_percent", DataType::Float64, true),
        Field::new("qap_pending_artifacts", DataType::UInt64, true),
        Field::new("qap_cached_bytes", DataType::UInt64, true),
        Field::new("qap_unhealthy_catalogs", DataType::UInt64, true),
        Field::new("qap_lag_seconds", DataType::Float64, true),
    ]))
}

fn legacy_result_schema() -> Arc<Schema> {
    Arc::new(Schema::new(
        result_schema()
            .fields()
            .iter()
            .map(|field| {
                if matches!(field.name().as_str(), "rows" | "duration_ms" | "rows_per_second") {
                    field.as_ref().clone().with_nullable(false)
                } else {
                    field.as_ref().clone()
                }
            })
            .collect::<Vec<_>>(),
    ))
}

fn encode_parquet(batch: &RecordBatch) -> Result<Vec<u8>> {
    let batch = deduplicate_results(batch)?;
    let schema = batch.schema();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}

fn platform_host_label(kind: crate::config::OatMode) -> &'static str {
    match kind {
        crate::config::OatMode::Local => "local",
        crate::config::OatMode::Aks => "aks",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_client_cleanup_is_scoped_to_the_cell_project_and_service() {
        assert_eq!(
            local_oat_client_list_args("arrowcol-oat-quick-123e4567"),
            [
                "ps",
                "-aq",
                "--filter",
                "label=com.docker.compose.project=arrowcol-oat-quick-123e4567",
                "--filter",
                "label=com.docker.compose.service=oat-client",
            ]
        );
        assert_eq!(
            local_oat_client_remove_args(&["client-a".to_owned(), "client-b".to_owned()]),
            ["rm", "--force", "client-a", "client-b"]
        );
    }

    #[test]
    fn failed_clients_preserve_pinning_evidence_without_masking_unrelated_failures() -> Result<()> {
        let tables = vec!["SQLServer_Wait_Stats".to_owned()];
        let healthy = HashMap::from([("sqlserver_wait_stats".to_owned(), vec!["collector-a".to_owned()])]);
        let unrelated = anyhow::anyhow!("DMV source connection failed");
        assert!(
            failed_client_pinning_report(&unrelated, Ok(healthy.clone()), &tables, PinningReport::default())
                .violation
                .is_none()
        );
        let missing = failed_client_pinning_report(&unrelated, Ok(HashMap::new()), &tables, PinningReport::default());
        assert!(
            missing
                .violation
                .as_deref()
                .is_some_and(|message| message.contains("no active ArrowCol"))
        );
        let upstream = anyhow::anyhow!("gRPC failed: pinning_violation: pinned collector became unavailable");
        let failed = failed_client_pinning_report(&upstream, Ok(healthy), &tables, PinningReport::default());
        assert_eq!(
            failed.owners.get("sqlserver_wait_stats").map(String::as_str),
            Some("collector-a")
        );
        assert!(
            failed
                .violation
                .as_deref()
                .is_some_and(|message| message.contains("pinning_violation"))
        );

        let metrics =
            "arrowcol_router_failures_total{operation=\"doput\",pool=\"dataflow\",reason=\"pinning_violation\"} 1\n";
        assert!(parse_owner_metrics(metrics, &tables).is_err());
        assert!(parse_owner_metrics(&metrics.replace("} 1", "} 0"), &tables).is_ok());
        let authority_loss = failed_client_pinning_report(
            &unrelated,
            parse_owner_metrics(metrics, &tables),
            &tables,
            PinningReport::default(),
        );
        assert!(
            authority_loss
                .violation
                .as_deref()
                .is_some_and(|message| message.contains("router rejected 1"))
        );
        Ok(())
    }

    #[test]
    fn local_credentials_fit_sql_server_password_limits() {
        let secret = random_secret();
        assert!((32..=128).contains(&secret.len()));
        assert!(secret.bytes().all(|byte| byte.is_ascii_graphic()));
        assert!(secret.bytes().any(|byte| byte.is_ascii_uppercase()));
        assert!(secret.bytes().any(|byte| byte.is_ascii_lowercase()));
        assert!(secret.bytes().any(|byte| byte.is_ascii_digit()));
        assert!(secret.bytes().any(|byte| byte.is_ascii_punctuation()));
    }

    #[test]
    fn snapshot_stop_keeps_writer_dependencies_alive_and_rejects_uncertain_custody() -> Result<()> {
        let project = "owned-run";
        let services = ["sqlserver", "arrowcol-catalog", "dataflow-1", "router", "oat-client"];
        let before = services.iter().enumerate().map(|(index, service)| json!({
            "Id": format!("id-{index}"),
            "Config": {"Labels": {"com.docker.compose.project": project, "com.docker.compose.service": service}},
            "State": {"Running": true}
        })).collect::<Vec<_>>();
        assert_eq!(
            snapshot_stop_phases(&before)?,
            [
                vec!["id-3".to_owned(), "id-4".to_owned()],
                vec!["id-2".to_owned()],
                vec!["id-1".to_owned()],
                vec!["id-0".to_owned()]
            ]
        );
        let mut after = before.clone();
        for container in &mut after {
            container["State"] = json!({
                "Running": false, "Paused": false, "Dead": false, "Status": "exited",
                "Error": "", "ExitCode": 0, "OOMKilled": false, "FinishedAt": "2026-01-01T00:00:00Z"
            });
        }
        after[2]["State"]["ExitCode"] = json!(143);
        assert_eq!(
            snapshot_container_outcomes(&before, &after, project)?.len(),
            services.len()
        );
        let mut invalid = after.clone();
        invalid[2]["State"]["ExitCode"] = json!(137);
        assert!(snapshot_container_outcomes(&before, &invalid, project).is_err());
        invalid = after.clone();
        invalid[2]["State"]["OOMKilled"] = json!(true);
        assert!(snapshot_container_outcomes(&before, &invalid, project).is_err());
        invalid = after.clone();
        invalid[2]["Id"] = json!("replacement");
        assert!(snapshot_container_outcomes(&before, &invalid, project).is_err());
        invalid = after.clone();
        invalid[2]["Config"]["Labels"]["com.docker.compose.project"] = json!("other-run");
        assert!(snapshot_container_outcomes(&before, &invalid, project).is_err());
        let mut already_failed = before;
        already_failed[2]["State"]["Running"] = json!(false);
        assert!(snapshot_container_outcomes(&already_failed, &after, project).is_err());
        Ok(())
    }
    include!("runner_publication_tests.rs");

    fn router_yaml_for_pin_test(run_id: &str) -> String {
        format!(
            "routing:\n  refresh_interval_ms: 10000\n  max_snapshot_age_ms: 15000\n  max_future_skew_ms: 2000\n  max_table_entries: 256\n  max_oldest_buffer_age_ms: 86400000\n  hard_pressure_bps: 9500\n  warmup_ms: 1000\n  minimum_warmup_bps: 1000\n  lease_ttl_ms: 3600000\n  oat_run_pin_ttl_ms: 3600000\n  oat_run_id: \"{run_id}\"\n  readiness_max_staleness_ms: 30000\n  distinct_table_owners: false\n"
        )
    }

    #[test]
    fn matrix_cells_use_distinct_canonical_run_ids_for_multiple_combinations() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.example.json"
        ))?;
        let cells = matrix_cell_specs(&config);
        assert_eq!(
            cells.len(),
            config.replica_counts.len() * config.dfe_worker_concurrency.len(),
            "every matrix combination needs its own isolated run"
        );
        assert!(cells.len() > 1, "the regression must cover more than one matrix cell");
        let ids = cells.iter().map(|(_, _, run_id)| run_id).collect::<HashSet<_>>();
        assert_eq!(ids.len(), cells.len(), "matrix cells must not share run identities");
        for (_, _, run_id) in &cells {
            let parsed = uuid::Uuid::parse_str(run_id)?;
            assert_eq!(parsed.to_string(), *run_id, "run IDs use canonical lowercase UUID form");
            let local_router = run_scoped_local_router_config("routing:\n  lease_ttl_ms: 86400000\n", run_id)?;
            assert!(local_router.contains(&format!("oat_run_id: \"{run_id}\"")));
        }
        Ok(())
    }

    #[test]
    fn pin_init_config_is_secret_free_and_binds_expected_table_digest() -> Result<()> {
        let run_id = "00000000-0000-0000-0000-000000000001";
        let expected = expected_pin_keys_json(&["sqlserver_wait_stats".to_owned()])?;
        let router_yaml = router_yaml_for_pin_test(run_id);
        let config: Value = serde_json::from_str(&pin_init_config_json(
            run_id,
            &expected,
            20_000,
            10_000,
            "sqlserver",
            &router_yaml,
        )?)?;
        assert_eq!(config["schema_version"], 1);
        assert_eq!(config["run_id"], run_id);
        assert_eq!(config["expires_at_unix_ms"], 20_000);
        assert_eq!(config["sql_server"]["host"], "sqlserver");
        assert_eq!(config["sql_server"]["port"], 1433);
        assert_eq!(config["expected_keys"].as_array().map(|keys| keys.len()), Some(1));
        assert_eq!(config["expected_digest"].as_str().map(str::len), Some(64));
        assert_eq!(config["routing_policy"]["oat_run_id"], run_id);
        assert_eq!(config["routing_policy"]["distinct_table_owners"], false);
        assert_eq!(config["routing_policy"]["warmup_ms"], 1000);
        assert!(config.get("password").is_none());
        assert!(config.get("username").is_none());
        Ok(())
    }

    #[test]
    fn pin_init_config_rejects_expired_or_incomplete_pin_sets() -> Result<()> {
        let expected = expected_pin_keys_json(&["sqlserver_wait_stats".to_owned()])?;
        let run_id = "00000000-0000-0000-0000-000000000001";
        let router_yaml = router_yaml_for_pin_test(run_id);
        assert!(pin_init_config_json(run_id, &expected, 10_000, 10_000, "sqlserver", &router_yaml,).is_err());
        assert!(pin_init_config_json(
            run_id,
            r#"{"expected_keys":[],"expected_digest":"0000000000000000000000000000000000000000000000000000000000000000"}"#,
            20_000,
            10_000,
            "sqlserver",
            &router_yaml,
        )
        .is_err());
        let mismatched_run_yaml = router_yaml_for_pin_test("00000000-0000-0000-0000-000000000002");
        assert!(pin_init_config_json(run_id, &expected, 20_000, 10_000, "sqlserver", &mismatched_run_yaml).is_err());
        Ok(())
    }

    #[test]
    fn local_pin_init_compose_build_has_parse_default_and_selected_run_supplies_private_config() {
        let compose = include_str!("../../loadtest/compose.yaml");
        assert!(compose.contains("source: ${OAT_PIN_INIT_CONFIG_HOST_PATH:-/dev/null}"));
        assert!(!compose.contains("OAT_PIN_INIT_CONFIG_HOST_PATH:?"));
        assert!(compose.contains("source: ${OAT_CONTROL_HOST_PATH:-/dev/null}"));
        assert!(!compose.contains("OAT_CONTROL_HOST_PATH:?"));

        assert_eq!(
            local_control_mount_environment(Path::new("/private/oat/run"))["OAT_CONTROL_HOST_PATH"],
            "/private/oat/run/control",
            "actual OAT setup must supply its run-scoped control directory"
        );

        let expected = Path::new("/private/oat/run/pin-init.json");
        let mut command = Command::new("docker");
        set_local_pin_init_config(&mut command, expected);
        assert!(
            command
                .as_std()
                .get_envs()
                .any(|(key, value)| key == "OAT_PIN_INIT_CONFIG_HOST_PATH" && value == Some(expected.as_os_str()))
        );
    }

    #[test]
    fn checked_unix_millisecond_conversion_rejects_values_outside_supported_range() {
        let maximum = u128::from(u64::MAX);
        assert_eq!(
            checked_unix_time_ms(maximum).unwrap_or_else(|error| panic!("u64 maximum must fit: {error}")),
            u64::MAX
        );
        assert!(checked_unix_time_ms(maximum + 1).is_err());
    }

    #[tokio::test]
    async fn unresolved_qap_cleanup_preserves_runtime_directory_and_lease_sidecar() -> Result<()> {
        let directory = std::env::temp_dir().join(format!("oat-evidence-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory)?;
        let record = directory.join("oat-qap-record.json");
        let sidecar = qap_lease_sidecar_path(&record);
        let stop_record = directory.join("workload-stop.json");
        let private_manifest = directory.join("oat-qap-manifest.json");
        for path in [&record, &sidecar, &stop_record, &private_manifest] {
            std::fs::write(path, b"private test evidence")?;
        }

        let result = async {
            let unresolved =
                runtime_evidence_retention(true, None, None, true, Some(&record), Some(&stop_record), true);
            apply_runtime_evidence_retention(&directory, unresolved).await?;
            for path in [&record, &sidecar, &stop_record, &private_manifest] {
                anyhow::ensure!(
                    tokio::fs::try_exists(path).await?,
                    "unresolved outcome lost {}",
                    path.display()
                );
            }

            let completed =
                runtime_evidence_retention(false, None, None, true, Some(&record), Some(&stop_record), false);
            apply_runtime_evidence_retention(&directory, completed).await?;
            for path in [&record, &sidecar, &stop_record] {
                anyhow::ensure!(
                    tokio::fs::try_exists(path).await?,
                    "successful cleanup lost {}",
                    path.display()
                );
            }
            anyhow::ensure!(
                !tokio::fs::try_exists(&private_manifest).await?,
                "successful cleanup retained an unrelated private manifest"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let cleanup = tokio::fs::remove_dir_all(&directory).await;
        result?;
        cleanup?;
        Ok(())
    }

    #[test]
    fn startup_wait_budget_covers_early_ready_fleet_qap_delay_and_final_delivery() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.example.json"
        ))?;
        config.client_ready_timeout_seconds = 30;
        config.qap_timeout_seconds = 30;
        config.warmup_seconds = 60;
        config.measure_seconds = 60;
        let budget = config.timing_budget()?;

        let first_client_ready_at = 1_u64;
        let whole_fleet_ready_at = 29_u64;
        let qap_preparation_seconds = 29_u64;
        let final_schedule_delivery_seconds = crate::config::SCHEDULE_DELIVERY_TIMEOUT_SECONDS;
        assert!(whole_fleet_ready_at <= config.client_ready_timeout_seconds);
        assert!(qap_preparation_seconds <= config.qap_timeout_seconds);
        let earliest_client_schedule_wait = whole_fleet_ready_at
            .saturating_sub(first_client_ready_at)
            .saturating_add(qap_preparation_seconds)
            .saturating_add(final_schedule_delivery_seconds);
        assert_eq!(budget.schedule_wait_seconds, 120);
        assert!(
            earliest_client_schedule_wait <= budget.schedule_wait_seconds,
            "a client that became ready early must remain alive through readiness, QAP, and final delivery"
        );
        assert_eq!(
            budget.client_job_lifetime_seconds,
            budget.schedule_wait_seconds + config.warmup_seconds + config.measure_seconds + 300
        );
        assert_eq!(
            budget.minimum_pin_ttl_ms,
            (budget.client_job_lifetime_seconds + crate::config::PIN_INIT_SETUP_TIMEOUT_SECONDS) * 1_000,
            "the immutable pin lifetime starts before bounded pin setup and must outlive the entire client Job"
        );
        Ok(())
    }

    #[test]
    fn qap_paths_and_external_names_are_isolated_by_run_and_keep_table_prefix() -> Result<()> {
        let run_a = "00000000-0000-0000-0000-000000000001";
        let run_b = "00000000-0000-0000-0000-000000000002";
        let shared_root = "abfss://data@account.dfs.core.windows.net/oat-prefix/dataflow";
        let root_a = run_delta_root(shared_root, run_a)?;
        let root_b = run_delta_root(shared_root, run_b)?;
        assert_eq!(
            root_a,
            format!("abfss://data@account.dfs.core.windows.net/oat-prefix/runs/{run_a}/dataflow")
        );
        assert_eq!(
            root_b,
            format!("abfss://data@account.dfs.core.windows.net/oat-prefix/runs/{run_b}/dataflow")
        );

        let relative_root_a = qap_relative_data_root(&root_a)?;
        let relative_root_b = qap_relative_data_root(&root_b)?;
        assert_eq!(relative_root_a, format!("oat-prefix/runs/{run_a}/dataflow"));
        assert_eq!(relative_root_b, format!("oat-prefix/runs/{run_b}/dataflow"));
        let table = "sqlserver_dmv_os_performance_counters";
        assert_eq!(
            aks_qap_relative_path(&relative_root_a, table)?,
            format!("{relative_root_a}/telemetry/sqlserver/{table}")
        );

        let bindings_a = qap_bindings(run_a, &[table.to_owned()])?;
        let bindings_b = qap_bindings(run_b, &[table.to_owned()])?;
        assert_eq!(bindings_a[0].logical_table, table);
        assert_eq!(bindings_b[0].logical_table, table);
        assert_eq!(
            bindings_a[0].external_table,
            format!("oat_{}_{}", uuid::Uuid::parse_str(run_a)?.simple(), table)
        );
        assert_ne!(bindings_a[0].external_table, bindings_b[0].external_table);
        Ok(())
    }

    #[test]
    fn qap_run_paths_reject_noncanonical_run_ids_and_unsafe_roots() {
        assert!(
            run_delta_root(
                "abfss://data@account.dfs.core.windows.net/oat-prefix/dataflow",
                "00000000-0000-0000-0000-000000000001/"
            )
            .is_err()
        );
        assert!(
            qap_relative_data_root("abfss://data@account.dfs.core.windows.net/oat-prefix/runs/../dataflow").is_err()
        );
        assert!(
            qap_relative_data_root("abfss://data@account.dfs.core.windows.net/oat-prefix/runs/%2F..%2F/dataflow")
                .is_err()
        );
        assert!(aks_qap_relative_path("oat-prefix/runs/run/dataflow", "../other-table").is_err());
    }

    #[test]
    fn qap_data_root_rejects_literal_and_encoded_path_normalization() -> Result<()> {
        let authority = "abfss://data@account.dfs.core.windows.net";
        for path in [
            "/oat-prefix/runs/./dataflow",
            "/oat-prefix/runs/../dataflow",
            "/oat-prefix/runs/%2e/dataflow",
            "/oat-prefix/runs/%2E%2e/dataflow",
            "/oat-prefix/runs/.%2e/dataflow",
            "/oat-prefix/runs/%2e./dataflow",
            "/oat-prefix/runs/%2f/dataflow",
            "/oat-prefix/runs\\..\\dataflow",
            "//oat-prefix/runs/dataflow",
            "/oat-prefix//runs/dataflow",
            "/oat-prefix/runs/dataflow/",
            "/oat-prefix/runs/\t../dataflow",
            "/oat-prefix/runs/\n../dataflow",
            "/oat-prefix/runs/ dataflow",
        ] {
            assert!(
                qap_relative_data_root(&format!("{authority}{path}")).is_err(),
                "unsafe literal path was accepted: {path:?}"
            );
        }
        for path in ["/oat-prefix/runs/run/dataflow", "/dataflow"] {
            assert_eq!(
                qap_relative_data_root(&format!("{authority}{path}"))?,
                path.strip_prefix('/').context("test path must be absolute")?
            );
        }
        Ok(())
    }

    #[test]
    fn local_compose_run_id_is_passed_as_compose_option_before_service() {
        let args = local_client_compose_args(
            "00000000-0000-0000-0000-000000000001",
            "oat-client-test-c1",
            &["--rows".to_owned()],
        );
        let service = args
            .iter()
            .position(|arg| arg == "oat-client")
            .unwrap_or_else(|| panic!("missing oat-client argument"));
        let env = args
            .iter()
            .position(|arg| arg == "--env")
            .unwrap_or_else(|| panic!("missing --env argument"));
        assert!(env < service);
        assert_eq!(
            args[env + 1],
            "ARROWCOL_OAT_RUN_ID=00000000-0000-0000-0000-000000000001"
        );
        let name = args
            .iter()
            .position(|arg| arg == "--name")
            .unwrap_or_else(|| panic!("missing --name argument"));
        assert!(name < service);
        assert_eq!(args[name + 1], "oat-client-test-c1");
        assert_eq!(args[service + 1], "--rows");
    }

    fn client_progress(ready: bool, schedule_received: bool) -> ClientProgress {
        ClientProgress {
            ready,
            schedule_received,
            completed: false,
            failure: None,
            output: String::new(),
        }
    }

    #[test]
    fn readiness_barrier_accepts_local_and_aks_cold_start_after_old_fixed_leads() -> Result<()> {
        let ready_fleet = [client_progress(true, false), client_progress(true, false)];
        assert!(validate_client_readiness(
            &ready_fleet,
            Duration::from_secs(61),
            Duration::from_secs(600)
        )?);
        assert!(validate_client_readiness(
            &ready_fleet,
            Duration::from_secs(16),
            Duration::from_secs(600)
        )?);
        Ok(())
    }

    #[test]
    fn readiness_barrier_rejects_timeout_and_partial_fleet() {
        let partial = [client_progress(true, false), client_progress(false, false)];
        assert!(matches!(
            validate_client_readiness(&partial, Duration::from_secs(10), Duration::from_secs(600)),
            Ok(false)
        ));
        assert!(validate_client_readiness(&partial, Duration::from_secs(600), Duration::from_secs(600)).is_err());
        let fully_ready_too_late = [client_progress(true, false), client_progress(true, false)];
        assert!(
            validate_client_readiness(
                &fully_ready_too_late,
                Duration::from_secs(600),
                Duration::from_secs(600)
            )
            .is_err()
        );
        let failed = [
            client_progress(true, false),
            ClientProgress {
                failure: Some("preflight failed".to_owned()),
                ..client_progress(false, false)
            },
        ];
        assert!(validate_client_readiness(&failed, Duration::from_secs(1), Duration::from_secs(600)).is_err());
    }

    #[test]
    fn final_armed_schedule_rejects_late_or_partial_acknowledgement() -> Result<()> {
        let start = 10_000;
        assert!(validate_client_schedule(
            &[client_progress(true, true), client_progress(true, true)],
            start - 1,
            start
        )?);
        assert!(matches!(
            validate_client_schedule(
                &[client_progress(true, true), client_progress(true, false)],
                start - 1,
                start
            ),
            Ok(false)
        ));
        assert!(
            validate_client_schedule(
                &[client_progress(true, true), client_progress(true, false)],
                start,
                start
            )
            .is_err()
        );
        assert!(
            validate_client_schedule(
                &[client_progress(true, true), client_progress(true, true)],
                start,
                start
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn parquet_schema_v2_keeps_legacy_fields_and_uses_utc_milliseconds() -> Result<()> {
        let schema = result_schema();
        let fields = schema.fields();
        let added = [
            "schema_version",
            "mode",
            "stamp",
            "host",
            "experiment",
            "started_at",
            "measure_seconds",
            "arrowcols",
            "clients",
            "tables",
            "table_owner",
            "pinning_ok",
            "status",
            "git_sha",
            "tunables_json",
        ];
        assert_eq!(
            fields
                .iter()
                .take(added.len())
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            added
        );
        assert_eq!(
            schema.field_with_name("started_at")?.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        for (name, nullable) in [
            ("stamp", true),
            ("table_owner", true),
            ("pinning_ok", false),
            ("status", false),
            ("run_id", false),
            ("table", false),
        ] {
            assert_eq!(schema.field_with_name(name)?.is_nullable(), nullable, "{name}");
        }
        assert!(schema.field_with_name("run_id").is_ok());
        assert!(schema.field_with_name("table").is_ok());
        Ok(())
    }

    #[test]
    fn aks_namespaces_are_run_unique_and_cleanup_requires_the_run_owner_token() -> Result<()> {
        let run_a = "00000000-0000-0000-0000-000000000001";
        let run_b = "00000000-0000-0000-0000-000000000002";
        let namespace_a = scoped_aks_namespace("arrowcol-oat", run_a)?;
        let namespace_b = scoped_aks_namespace("arrowcol-oat", run_b)?;
        assert_ne!(namespace_a, namespace_b);
        assert!(namespace_a.len() <= 63);
        assert!(scoped_aks_namespace("arrowcol-oat", "not-a-uuid").is_err());

        let empty_run_namespace = json!({
            "metadata": {
                "uid": "namespace-uid-a",
                "labels": {
                    "arrowcol-oat/run-id": run_a,
                    "arrowcol-oat/owner-token": "owner-a",
                },
                "annotations": {
                    "arrowcol-oat/run-id": run_a,
                    "arrowcol-oat/owner-token": "owner-a",
                },
            },
        });
        assert!(namespace_matches_run(&empty_run_namespace, run_a, "owner-a", None));
        assert!(namespace_matches_run(
            &empty_run_namespace,
            run_a,
            "owner-a",
            Some("namespace-uid-a")
        ));
        assert!(!namespace_matches_run(
            &empty_run_namespace,
            run_a,
            "owner-a",
            Some("namespace-uid-b")
        ));
        assert!(!namespace_matches_run(&empty_run_namespace, run_b, "owner-b", None));
        assert!(!namespace_matches_run(&empty_run_namespace, run_a, "owner-b", None));
        assert_eq!(
            namespace_cleanup_action(false, true),
            NamespaceCleanupAction::Delete,
            "a later prepare failure must clean the namespace created earlier in this run"
        );
        assert_eq!(
            namespace_cleanup_action(false, false),
            NamespaceCleanupAction::Ignore,
            "an ambiguous create must not delete an existing namespace with another owner token"
        );
        assert_eq!(
            namespace_uid_for_cleanup(&empty_run_namespace, None)?,
            "namespace-uid-a"
        );
        assert_eq!(
            namespace_uid_for_cleanup(&empty_run_namespace, Some("namespace-uid-a"))?,
            "namespace-uid-a"
        );
        assert!(namespace_uid_for_cleanup(&empty_run_namespace, Some("namespace-uid-b")).is_err());
        assert_eq!(
            namespace_cleanup_action(true, false),
            NamespaceCleanupAction::Refuse,
            "an ownership change after creation must fail closed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn replacement_namespace_uid_conflict_preserves_replacement() -> Result<()> {
        let observed_namespace = json!({ "metadata": { "uid": "namespace-uid-a" } });
        let observed_uid = namespace_uid_for_cleanup(&observed_namespace, None)?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            let mut buffer = [0_u8; 2048];
            let (header_end, content_length) = loop {
                let read = stream.read(&mut buffer).await?;
                if read == 0 {
                    bail!("Kubernetes API test server received an incomplete request");
                }
                request.extend_from_slice(&buffer[..read]);
                if let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .context("namespace delete request must include Content-Length")?;
                    if request.len() >= header_end + 4 + content_length {
                        break (header_end, content_length);
                    }
                }
            };
            let request_line = String::from_utf8_lossy(&request)
                .lines()
                .next()
                .context("namespace delete request must include a request line")?
                .to_owned();
            let body_start = header_end + 4;
            let options: Value = serde_json::from_slice(&request[body_start..body_start + content_length])?;
            let requested_uid = options
                .pointer("/preconditions/uid")
                .and_then(Value::as_str)
                .context("namespace delete must include a UID precondition")?
                .to_owned();
            let replacement_uid = "namespace-uid-b".to_owned();
            let response_body = r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Conflict","message":"UID precondition does not match replacement namespace","code":409}"#;
            let response = format!(
                "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream.write_all(response.as_bytes()).await?;
            Ok::<_, anyhow::Error>((request_line, requested_uid, replacement_uid))
        });

        let result =
            delete_namespace_with_uid_precondition(&format!("http://{address}"), "arrowcol-oat-run", &observed_uid)
                .await;
        let (request_line, requested_uid, surviving_uid) = server.await??;
        assert_eq!(observed_uid, "namespace-uid-a");
        assert_eq!(request_line, "DELETE /api/v1/namespaces/arrowcol-oat-run HTTP/1.1");
        assert_eq!(requested_uid, "namespace-uid-a");
        assert_eq!(surviving_uid, "namespace-uid-b");
        let error = result
            .context("a UID conflict must fail closed without a success receipt")
            .err();
        let error = error.context("replacement namespace deletion unexpectedly succeeded")?;
        assert!(format!("{error:#}").contains("409 Conflict"));
        Ok(())
    }

    #[test]
    fn local_router_configuration_authorizes_only_its_run() -> Result<()> {
        let generated = run_scoped_local_router_config(include_str!("../../loadtest/config/router.yaml"), "run-one")?;
        let generated: serde_yaml::Value = serde_yaml::from_str(&generated)?;
        assert_eq!(generated["auth"]["authenticator"].as_str(), Some("dev_only_allow_all"));
        assert_eq!(generated["routing"]["oat_run_id"].as_str(), Some("run-one"));

        let configured = run_scoped_local_router_config(
            "routing:\n  lease_ttl_ms: 86400000\n  readiness_max_staleness_ms: 30000\n",
            "run-one",
        )?;
        assert!(configured.contains("  lease_ttl_ms: 3600000\n  oat_run_id: \"run-one\"\n"));
        assert!(run_scoped_local_router_config("routing:\n  lease_ttl_ms: 30000\n", "run-two").is_err());
        assert!(
            run_scoped_local_router_config(
                "routing:\n  lease_ttl_ms: 86400000\n  oat_run_id: run-existing\n",
                "run-one",
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn parquet_schema_v2_matches_the_full_frozen_column_contract() -> Result<()> {
        let expected = vec![
            ("schema_version", DataType::Int32, false),
            ("mode", DataType::Utf8, false),
            ("stamp", DataType::Utf8, true),
            ("host", DataType::Utf8, false),
            ("experiment", DataType::Utf8, false),
            (
                "started_at",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            ("measure_seconds", DataType::Float64, false),
            ("arrowcols", DataType::Int32, false),
            ("clients", DataType::Int32, false),
            ("tables", DataType::Int32, false),
            ("table_owner", DataType::Utf8, true),
            ("pinning_ok", DataType::Boolean, false),
            ("status", DataType::Utf8, false),
            ("git_sha", DataType::Utf8, false),
            ("tunables_json", DataType::Utf8, false),
            ("run_id", DataType::Utf8, false),
            ("replicas", DataType::UInt64, false),
            ("dfe_worker_concurrency", DataType::UInt64, false),
            ("table", DataType::Utf8, false),
            ("rows", DataType::UInt64, true),
            ("duration_ms", DataType::UInt64, true),
            ("rows_per_second", DataType::Float64, true),
            ("cpu_millicores", DataType::Float64, true),
            ("memory_bytes", DataType::UInt64, true),
            ("qap_observation_complete", DataType::UInt64, false),
            ("qap_observed_tables", DataType::UInt64, false),
            ("qap_completion_percent", DataType::Float64, true),
            ("qap_pending_artifacts", DataType::UInt64, true),
            ("qap_cached_bytes", DataType::UInt64, true),
            ("qap_unhealthy_catalogs", DataType::UInt64, true),
            ("qap_lag_seconds", DataType::Float64, true),
        ];
        let schema = result_schema();
        let actual = schema
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.data_type().clone(), field.is_nullable()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        Ok(())
    }
    #[test]
    fn pinning_guard_accepts_stable_single_owner_and_rejects_loss_change_or_duplicates() {
        let tables = vec!["sqlserver_wait_stats".to_owned()];
        let mut owners = HashMap::new();
        let one = HashMap::from([("sqlserver_wait_stats".to_owned(), vec!["collector-a".to_owned()])]);
        assert_eq!(check_owner_sample(&tables, &HashMap::new(), &mut owners), None);
        assert_eq!(check_owner_sample(&tables, &one, &mut owners), None);
        assert_eq!(
            owners.get("sqlserver_wait_stats").map(String::as_str),
            Some("collector-a")
        );
        assert!(check_owner_sample(&tables, &HashMap::new(), &mut owners).is_some());

        let mut owners = HashMap::new();
        assert_eq!(check_owner_sample(&tables, &one, &mut owners), None);
        let changed = HashMap::from([("sqlserver_wait_stats".to_owned(), vec!["collector-b".to_owned()])]);
        assert!(check_owner_sample(&tables, &changed, &mut owners).is_some());

        let duplicate = HashMap::from([(
            "sqlserver_wait_stats".to_owned(),
            vec!["collector-a".to_owned(), "collector-b".to_owned()],
        )]);
        assert!(check_owner_sample(&tables, &duplicate, &mut HashMap::new()).is_some());
        assert!(check_owner_sample(&tables, &HashMap::new(), &mut HashMap::new()).is_none());
    }

    #[test]
    fn owner_lookup_uses_the_same_canonical_table_key_as_metrics() -> Result<()> {
        let configured_tables = vec!["SQLSERVER_WAIT_STATS".to_owned()];
        let owners = parse_owner_metrics(
            "arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"c\",schema=\"s\",table=\"sqlserver_wait_stats\",collector=\"collector-a\",state=\"active\"} 1\n",
            &configured_tables,
        )?;
        let mut stable = HashMap::new();
        assert_eq!(check_owner_sample(&configured_tables, &owners, &mut stable), None);
        assert_eq!(
            stable
                .get(&canonical_table_key(&configured_tables[0]))
                .map(String::as_str),
            Some("collector-a")
        );
        Ok(())
    }

    #[test]
    fn aks_overlay_only_allows_configured_deployments_in_target_namespace() -> Result<()> {
        let allowed = HashSet::from(["arrowcol".to_owned(), "arrowcol-dataflow".to_owned()]);
        let arrowcol = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "arrowcol", "namespace": "arrowcol"},
        });
        let dfe_without_namespace = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "arrowcol-dataflow"},
        });
        validate_aks_overlay_resources(&[arrowcol.clone(), dfe_without_namespace], &allowed, "arrowcol")?;
        assert!(validate_aks_overlay_resources(&[], &allowed, "arrowcol").is_err());
        assert!(
            validate_aks_overlay_resources(
                &[json!({"kind": "ConfigMap", "metadata": {"name": "settings"}})],
                &allowed,
                "arrowcol",
            )
            .is_err()
        );
        assert!(
            validate_aks_overlay_resources(
                &[json!({"kind": "Deployment", "metadata": {"name": "unconfigured"}})],
                &allowed,
                "arrowcol",
            )
            .is_err()
        );
        assert!(
            validate_aks_overlay_resources(
                &[json!({"kind": "Deployment", "metadata": {"name": "arrowcol", "namespace": "other"}})],
                &allowed,
                "arrowcol",
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn owner_metric_parser_ignores_other_tables_and_draining_leases() -> Result<()> {
        let tables = vec!["sqlserver_wait_stats".to_owned()];
        let owners = parse_owner_metrics(
            "arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"c\",schema=\"s\",table=\"sqlserver_wait_stats\",collector=\"collector-a\",state=\"active\"} 1\n\
             arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"c\",schema=\"s\",table=\"sqlserver_wait_stats\",collector=\"collector-old\",state=\"draining\"} 1\n\
             arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"c\",schema=\"s\",table=\"another_table\",collector=\"collector-b\",state=\"active\"} 1\n",
            &tables,
        )?;
        assert_eq!(
            owners.get("sqlserver_wait_stats"),
            Some(&vec!["collector-a".to_owned()])
        );
        Ok(())
    }

    #[test]
    fn owner_metric_parser_handles_escaped_commas_in_collector_labels() -> Result<()> {
        let tables = vec!["sqlserver_wait_stats".to_owned()];
        let owners = parse_owner_metrics(
            "arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"c\",schema=\"s\",table=\"sqlserver_wait_stats\",collector=\"collector,west\",state=\"active\"} 1\n",
            &tables,
        )?;
        assert_eq!(
            owners.get("sqlserver_wait_stats"),
            Some(&vec!["collector,west".to_owned()])
        );
        Ok(())
    }

    #[test]
    fn parses_only_complete_positive_client_throughput_reports() -> Result<()> {
        let report = parse_client_metrics(
            "uploaded 1 DMV tables with 100 rows each using concurrency 4\nmeasured_rows=12000 measured_seconds=60.250 rows_per_second=199.17 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=160250 measurement_started_unix_ms=100200\n",
        )?;
        assert_eq!(report.rows, 12_000);
        assert_eq!(report.duration_seconds, 60.25);
        assert_eq!(report.rows_per_second, 199.17);
        assert_eq!(report.measurement_window_start_unix_ms, 100_000);
        assert_eq!(report.measurement_window_end_unix_ms, 160_250);
        assert_eq!(report.measurement_started_unix_ms, 100_200);
        assert!(parse_client_metrics("measured_rows=1 measured_seconds=0 rows_per_second=0").is_err());
        assert!(parse_client_metrics(
            "measured_rows=1 measured_seconds=60 rows_per_second=1 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=160000"
        )
        .is_err());
        assert!(parse_client_metrics("measured_rows=1").is_err());
        Ok(())
    }

    #[test]
    fn parses_complete_pilot_windows_and_finds_the_earliest_stable_horizon() -> Result<()> {
        let report = parse_client_metrics(
            "measured_rows=9100 measured_seconds=90 rows_per_second=101.11 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=190000 measurement_started_unix_ms=100000\n\
             pilot_window=1 window_seconds=30 rows=3000 rows_per_second=100.000 window_start_unix_ms=100000\n\
             pilot_window=2 window_seconds=30 rows=3090 rows_per_second=103.000 window_start_unix_ms=130000\n\
             pilot_window=3 window_seconds=30 rows=3030 rows_per_second=101.000 window_start_unix_ms=160000\n",
        )?;
        assert_eq!(report.pilot_windows.len(), 3);
        assert_eq!(report.pilot_windows[1].index, 1);
        assert_eq!(report.pilot_windows[1].rows, 3090);
        assert_eq!(report.pilot_windows[1].start_unix_ms, 130_000);
        assert_eq!(stable_window_horizon(&[100.0, 103.0, 101.0]), Some(60));
        assert_eq!(stable_window_horizon(&[100.0, 100.0, 10.0, 10.0]), None);
        assert!(parse_client_metrics("measured_rows=1 measured_seconds=60 rows_per_second=0.01").is_err());
        Ok(())
    }

    #[test]
    fn rejects_staggered_clients_and_misaligned_pilot_windows() -> Result<()> {
        let report = parse_client_metrics(
            "measured_rows=100 measured_seconds=60 rows_per_second=1.67 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=160000 measurement_started_unix_ms=100251\n\
             pilot_window=1 window_seconds=30 rows=50 rows_per_second=1.67 window_start_unix_ms=100000\n\
             pilot_window=2 window_seconds=30 rows=50 rows_per_second=1.67 window_start_unix_ms=130000\n",
        )?;
        assert!(validate_client_measurement_interval(&report, 100_000, 160_000, true).is_err());
        let aligned = parse_client_metrics(
            "measured_rows=100 measured_seconds=60 rows_per_second=1.67 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=160000 measurement_started_unix_ms=100200\n\
             pilot_window=1 window_seconds=30 rows=50 rows_per_second=1.67 window_start_unix_ms=100000\n\
             pilot_window=2 window_seconds=30 rows=50 rows_per_second=1.67 window_start_unix_ms=130000\n",
        )?;
        assert!(validate_client_measurement_interval(&aligned, 100_000, 160_000, true).is_ok());
        Ok(())
    }

    #[test]
    fn integrated_patch_preserves_replicas_config_and_replaces_all_core_arguments() -> Result<()> {
        let mut deployment = json!({"spec":{"replicas":2,"template":{"spec":{"containers":[{
            "name":"dataflow","args":["--config","/etc/oat/dataflow.yaml","--num-cores=1","--num-cores","3"]
        }]}}}});
        configure_workers(&mut deployment, "dataflow", 2)?;
        assert_eq!(deployment["spec"]["replicas"], 2);
        assert_eq!(
            deployment.pointer("/spec/template/spec/containers/0/args"),
            Some(&json!(["--config", "/etc/oat/dataflow.yaml", "--num-cores", "2"]))
        );
        deployment["spec"]["template"]["spec"]["containers"][0]["args"] = json!(["--num-cores"]);
        assert!(configure_workers(&mut deployment, "dataflow", 2).is_err());
        Ok(())
    }

    #[test]
    fn overlapping_metrics_selectors_count_each_pod_once() -> Result<()> {
        let mut pods = parse_top_metrics("arrowcol-a 100m 2Mi\narrowcol-b 200m 3Mi\n")?;
        pods.extend(parse_top_metrics("arrowcol-a 100m 2Mi\n")?);
        let total = sum_metrics(&pods);
        assert_eq!(total.cpu_millicores, 300.0);
        assert_eq!(total.memory_bytes, 5 * 1024 * 1024);
        Ok(())
    }

    #[test]
    fn metrics_require_every_current_pod_and_reject_stale_or_unknown_samples() -> Result<()> {
        let expected = active_pod_names(&json!({"items": [
            {"metadata": {"name": "arrowcol-a"}},
            {"metadata": {"name": "arrowcol-b"}},
            {"metadata": {"name": "arrowcol-old", "deletionTimestamp": "2026-11-08T00:00:00Z"}}
        ]}))?;
        assert_eq!(expected.len(), 2);
        assert!(!metrics_complete(
            &expected,
            &parse_top_metrics("arrowcol-a 100m 2Mi\n")?
        ));
        assert!(!metrics_complete(
            &expected,
            &parse_top_metrics("arrowcol-a 100m 2Mi\narrowcol-b <unknown> <unknown>\n")?
        ));
        assert!(!metrics_complete(
            &expected,
            &parse_top_metrics("arrowcol-a 100m 2Mi\narrowcol-old 100m 2Mi\n")?
        ));
        assert!(metrics_complete(
            &expected,
            &parse_top_metrics("arrowcol-a 100m 2Mi\narrowcol-b 200m 3Mi\n")?
        ));
        assert!(active_pod_names(&json!({"items": []})).is_err());
        Ok(())
    }

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
    fn rejects_reported_duration_shorter_than_the_shared_window() -> Result<()> {
        let report = parse_client_metrics(
            "measured_rows=100 measured_seconds=1 rows_per_second=100 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=160000 measurement_started_unix_ms=100000",
        )?;
        assert!(validate_client_measurement_interval(&report, 100_000, 160_000, false).is_err());
        let aligned = ClientReport {
            duration_seconds: 60.0,
            ..report
        };
        assert!(validate_client_measurement_interval(&aligned, 100_000, 160_000, false).is_ok());
        Ok(())
    }

    #[test]
    fn initial_parquet_write_deduplicates_run_table_keys() -> Result<()> {
        let batch = parquet_batch(&[
            sample_result("run-1", "table-a", 100),
            sample_result("run-1", "table-a", 200),
        ])?;
        let bytes = encode_parquet(&batch)?;
        let batches = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?
            .build()?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let rows = batches[0]
            .column_by_name("rows")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .context("missing UInt64 rows")?;
        assert_eq!(rows.value(0), 200);
        Ok(())
    }

    #[test]
    fn zero_row_parquet_preserves_schema_and_accepts_a_later_append() -> Result<()> {
        let empty = encode_parquet(&parquet_batch(&[])?)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&empty))?;
        assert_eq!(reader.schema().as_ref(), result_schema().as_ref());
        assert_eq!(reader.metadata().file_metadata().num_rows(), 0);
        assert_eq!(reader.build()?.count(), 0);
        let appended = merge_parquet_bytes(&empty, &parquet_batch(&[sample_result("run-1", "table-a", 100)])?)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(appended))?;
        assert_eq!(reader.metadata().file_metadata().num_rows(), 1);
        Ok(())
    }

    #[test]
    fn writes_one_compact_parquet_file_with_viewer_schema() -> Result<()> {
        let results = vec![sample_result("run-1", "sqlserver_wait_stats", 100)];
        let bytes = encode_parquet(&parquet_batch(&results)?)?;
        assert!(bytes.starts_with(b"PAR1"));
        assert!(bytes.ends_with(b"PAR1"));
        Ok(())
    }

    #[test]
    fn parquet_merges_are_idempotent_by_run_and_table() -> Result<()> {
        let current = parquet_batch(&[
            sample_result("run-1", "sqlserver_wait_stats", 100),
            sample_result("run-1", "sqlserver_cpu_utilization", 200),
        ])?;
        let retry = parquet_batch(&[
            sample_result("run-1", "sqlserver_wait_stats", 150),
            sample_result("run-2", "sqlserver_wait_stats", 300),
        ])?;
        let bytes = merge_parquet_bytes(&encode_parquet(&current)?, &retry)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
        let batches = reader.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        let rows = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("rows")
                    .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                    .unwrap_or_else(|| panic!("schema must contain UInt64 rows"))
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![200, 150, 300]);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_local_writer_processes_preserve_both_distinct_rows() -> Result<()> {
        let directory = std::env::temp_dir().join(format!("oat-parquet-lock-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory)?;
        let target = directory.join("results.parquet");
        let gate = directory.join("start-writers");
        let executable = std::env::current_exe()?;
        let mut writers = Vec::new();
        for (run_id, table) in [("writer-a", "table-a"), ("writer-b", "table-b")] {
            writers.push(
                std::process::Command::new(&executable)
                    .args(["--exact", "runner::tests::local_file_writer_child"])
                    .env("OAT_FILE_WRITER_TARGET", &target)
                    .env("OAT_FILE_WRITER_GATE", &gate)
                    .env("OAT_FILE_WRITER_RUN_ID", run_id)
                    .env("OAT_FILE_WRITER_TABLE", table)
                    .spawn()?,
            );
        }
        std::fs::write(&gate, b"go")?;
        for mut writer in writers {
            let status = writer.wait()?;
            assert!(status.success(), "local writer child exited with {status}");
        }
        let bytes = std::fs::read(&target)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
        let batches = reader.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let rows = batches
            .iter()
            .flat_map(|batch| {
                let run_id = batch
                    .column_by_name("run_id")
                    .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                    .unwrap_or_else(|| panic!("schema must contain UTF-8 run_id"));
                let table = batch
                    .column_by_name("table")
                    .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                    .unwrap_or_else(|| panic!("schema must contain UTF-8 table"));
                (0..batch.num_rows())
                    .map(|index| (run_id.value(index).to_owned(), table.value(index).to_owned()))
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        assert_eq!(
            rows,
            HashSet::from([
                ("writer-a".to_owned(), "table-a".to_owned()),
                ("writer-b".to_owned(), "table-b".to_owned()),
            ])
        );
        std::fs::remove_file(&target)?;
        std::fs::remove_file(directory.join("results.parquet.lock"))?;
        std::fs::remove_file(&gate)?;
        std::fs::remove_dir(&directory)?;
        Ok(())
    }

    #[tokio::test]
    async fn local_file_writer_child() -> Result<()> {
        let (Some(target), Some(gate), Some(run_id), Some(table)) = (
            std::env::var_os("OAT_FILE_WRITER_TARGET"),
            std::env::var_os("OAT_FILE_WRITER_GATE"),
            std::env::var("OAT_FILE_WRITER_RUN_ID").ok(),
            std::env::var("OAT_FILE_WRITER_TABLE").ok(),
        ) else {
            return Ok(());
        };
        let gate = PathBuf::from(gate);
        for _ in 0..200 {
            if gate.is_file() {
                let incoming = parquet_batch(&[sample_result(&run_id, &table, 100)])?;
                return merge_local_results(&PathBuf::from(target), &incoming).await;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        bail!("timed out waiting for concurrent writer test gate")
    }

    #[tokio::test]
    async fn stale_authoritative_snapshot_cannot_regress_a_successful_local_mirror() -> Result<()> {
        let directory = std::env::temp_dir().join(format!("oat-parquet-mirror-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory)?;
        let authoritative = directory.join("authoritative.parquet");
        let mirror = directory.join("website.parquet");
        let old_snapshot = b"authoritative-snapshot-S1";
        let newer_snapshot = b"authoritative-and-mirror-S2";
        std::fs::write(&authoritative, newer_snapshot)?;
        std::fs::write(&mirror, newer_snapshot)?;
        let authoritative_url = Url::from_file_path(&authoritative)
            .map_err(|()| anyhow::anyhow!("could not create authoritative file URL"))?;
        let mirror_url =
            Url::from_file_path(&mirror).map_err(|()| anyhow::anyhow!("could not create mirror file URL"))?;
        let stale = DestinationSnapshot {
            bytes: old_snapshot.to_vec(),
            version: None,
        };

        let copied = write_exact_destination(&mirror_url, &authoritative_url, &stale).await?;
        assert!(!copied);
        assert_eq!(std::fs::read(&mirror)?, newer_snapshot);

        std::fs::remove_file(authoritative)?;
        std::fs::remove_file(mirror)?;
        std::fs::remove_file(directory.join("website.parquet.lock"))?;
        std::fs::remove_dir(directory)?;
        Ok(())
    }

    fn sample_result(run_id: &str, table: &str, rows: u64) -> OatResult {
        OatResult {
            run_id: run_id.to_owned(),
            mode: "local".to_owned(),
            stamp: None,
            host: "local".to_owned(),
            experiment: "arrowcols-1-dfe-2".to_owned(),
            started_at_ms: 1_794_173_400_000,
            measure_seconds: 60.0,
            arrowcols: 1,
            clients: 1,
            tables: 1,
            table_owner: Some("collector-a".to_owned()),
            pinning_ok: true,
            pinning_error: None,
            failure: None,
            status: "succeeded".to_owned(),
            git_sha: "test".to_owned(),
            tunables_json: "{}".to_owned(),
            replicas: 1,
            dfe_worker_concurrency: 2,
            table: table.to_owned(),
            rows: Some(rows),
            duration_ms: Some(10),
            rows_per_second: Some(rows as f64 / 60.0),
            cpu_millicores: Some(50.0),
            memory_bytes: Some(1024),
            qap: QapMetrics::default(),
        }
    }
}
