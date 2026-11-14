use arrowcol_tunables::Tunables;
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;
use url::Url;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OatConfig {
    #[serde(default)]
    pub tunables: Tunables,
    pub platform: PlatformConfig,
    pub stamp: Option<String>,
    pub warmup_seconds: u64,
    pub measure_seconds: u64,
    #[serde(default = "default_client_ready_timeout_seconds")]
    pub client_ready_timeout_seconds: u64,
    #[serde(default = "default_run_pin_ttl_ms")]
    pub run_pin_ttl_ms: u64,
    pub pilot_mode: bool,
    #[serde(default)]
    pub namespace: String,
    pub kubectl_context: Option<String>,
    #[serde(default)]
    pub arrowcol_deployment: String,
    #[serde(default)]
    pub arrowcol_container: String,
    #[serde(default)]
    pub dfe_deployment: String,
    #[serde(default)]
    pub dfe_container: String,
    #[serde(default)]
    pub arrowcol_pod_selector: String,
    #[serde(default)]
    pub dfe_pod_selector: String,
    pub arrowcol_endpoint: String,
    pub router_metrics_endpoint: String,
    #[serde(default)]
    pub dmv_client_binary: String,
    #[serde(default)]
    pub edge_secret_file: String,
    #[serde(default)]
    pub dmv_client_image: String,
    #[serde(default)]
    pub edge_secret_name: String,
    pub tables: Vec<String>,
    pub rows_per_table: usize,
    #[serde(default = "default_clients_per_table")]
    pub clients_per_table: usize,
    pub dmv_concurrency: usize,
    pub qap_endpoint: String,
    pub qap_database: Option<String>,
    pub qap_timeout_seconds: u64,
    pub replica_counts: Vec<u32>,
    pub dfe_worker_concurrency: Vec<u32>,
    #[serde(default)]
    pub result_url: String,
    #[serde(default)]
    pub website_url: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformConfig {
    pub kind: OatMode,
    pub compose_file: Option<String>,
    pub compose_override_file: Option<String>,
    pub compose_project: Option<String>,
    pub compose_state_root: Option<String>,
    pub kustomize_overlay: Option<String>,
    pub runtime_config_file: Option<String>,
    pub bootstrap_env_file: Option<String>,
    #[serde(default)]
    pub local: LocalOptions,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalOptions {
    pub bind_host: Option<std::net::IpAddr>,
    pub endpoint_host: Option<String>,
    pub snapshot_root: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OatMode {
    Local,
    Aks,
}

fn default_client_ready_timeout_seconds() -> u64 {
    600
}

pub(crate) const SCHEDULE_DELIVERY_TIMEOUT_SECONDS: u64 = 180;
pub(crate) const PIN_INIT_SETUP_TIMEOUT_SECONDS: u64 = 180;
const CLIENT_JOB_CLEANUP_GRACE_SECONDS: u64 = 300;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RunTimingBudget {
    pub(crate) schedule_wait_seconds: u64,
    pub(crate) client_job_lifetime_seconds: u64,
    pub(crate) minimum_pin_ttl_ms: u64,
}

impl OatConfig {
    pub(crate) fn timing_budget(&self) -> Result<RunTimingBudget> {
        let schedule_wait_seconds = self
            .client_ready_timeout_seconds
            .checked_add(self.qap_timeout_seconds)
            .and_then(|seconds| seconds.checked_add(SCHEDULE_DELIVERY_TIMEOUT_SECONDS))
            .context("OAT startup schedule wait budget overflow")?;
        let client_job_lifetime_seconds = schedule_wait_seconds
            .checked_add(self.warmup_seconds)
            .and_then(|seconds| seconds.checked_add(self.measure_seconds))
            .and_then(|seconds| seconds.checked_add(CLIENT_JOB_CLEANUP_GRACE_SECONDS))
            .context("OAT client Job lifetime budget overflow")?;
        let minimum_pin_ttl_ms = client_job_lifetime_seconds
            .checked_add(PIN_INIT_SETUP_TIMEOUT_SECONDS)
            .context("OAT pin initialization lifetime budget overflow")?
            .checked_mul(1_000)
            .context("OAT run pin lifetime budget overflow")?;
        Ok(RunTimingBudget {
            schedule_wait_seconds,
            client_job_lifetime_seconds,
            minimum_pin_ttl_ms,
        })
    }

    /// Validates the benchmark inputs before any cluster or storage operation.
    ///
    /// # Errors
    ///
    /// Returns an error when a setting is invalid or a requested DMV table is unsupported.
    pub fn validate(&self) -> Result<()> {
        self.tunables.validate()?;
        if self.warmup_seconds > 300 || !(1..=600).contains(&self.measure_seconds) {
            bail!("warmup_seconds must be at most 300 and measure_seconds must be between 1 and 600");
        }
        if !(30..=1_800).contains(&self.client_ready_timeout_seconds) {
            bail!("client_ready_timeout_seconds must be between 30 and 1800");
        }
        if !(1..=300).contains(&self.qap_timeout_seconds) {
            bail!("qap_timeout_seconds must be between 1 and 300");
        }
        let minimum_pin_ttl_ms = self.timing_budget()?.minimum_pin_ttl_ms;
        if !(60_000..=86_400_000).contains(&self.run_pin_ttl_ms) || self.run_pin_ttl_ms < minimum_pin_ttl_ms {
            bail!(
                "run_pin_ttl_ms must cover pin setup, readiness, QAP preparation, schedule delivery, workload, and Job cleanup margin, and be between 60,000 and 86,400,000"
            );
        }
        if self.pilot_mode
            && (!(60..=600).contains(&self.measure_seconds)
                || !self.measure_seconds.is_multiple_of(30)
                || self.warmup_seconds + self.measure_seconds > 600)
        {
            bail!(
                "pilot_mode requires a 30-second-multiple measurement from 60 to 600 seconds and a total warm-up plus measurement of at most 600 seconds"
            );
        }
        if self.stamp.as_ref().is_some_and(|stamp| stamp.trim().is_empty()) {
            bail!("stamp must be omitted or nonempty");
        }
        if self.platform.kind == OatMode::Aks
            && !self
                .stamp
                .as_deref()
                .is_some_and(|stamp| stamp.len() == 5 && stamp.bytes().all(|byte| byte.is_ascii_digit()))
        {
            bail!("AKS platform requires a five-digit stamp");
        }
        match self.platform.kind {
            OatMode::Local => {
                if let Some(host) = self.platform.local.bind_host
                    && (host.is_multicast() || (host.is_unspecified() && self.platform.local.endpoint_host.is_none()))
                {
                    bail!("local bind_host must be unicast; wildcard binds require an explicit endpoint_host");
                }
                if let Some(host) = &self.platform.local.endpoint_host {
                    crate::local::endpoint_host(host)?;
                }
                if let Some(root) = &self.platform.local.snapshot_root {
                    crate::local::absolute_directory(Path::new(root))?;
                }
                if self
                    .qap_database
                    .as_deref()
                    .is_some_and(|database| database != "loadtest")
                {
                    bail!("local platform uses the isolated loadtest Kusto database");
                }
                if self.platform.kustomize_overlay.is_some() {
                    bail!("kustomize_overlay is only valid for the AKS platform");
                }
                if self.platform.runtime_config_file.is_some() || self.platform.bootstrap_env_file.is_some() {
                    bail!("AKS runtime settings are only valid for the AKS platform");
                }
                let compose_file = self
                    .platform
                    .compose_file
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .context("local platform requires compose_file")?;
                if compose_file.contains('\0') {
                    bail!("compose_file contains an invalid NUL byte");
                }
                let project = self
                    .platform
                    .compose_project
                    .as_deref()
                    .context("local platform requires a run-scoped compose_project")?;
                if project.len() > 48
                    || !project
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_')
                    || !project
                        .as_bytes()
                        .first()
                        .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
                {
                    bail!(
                        "compose_project must be a run-scoped lowercase Compose project name of at most 48 characters"
                    );
                }
                let state_root = self
                    .platform
                    .compose_state_root
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .context("local platform requires compose_state_root")?;
                if state_root.contains('\0') {
                    bail!("compose_state_root contains an invalid NUL byte");
                }
                let override_file = self
                    .platform
                    .compose_override_file
                    .as_deref()
                    .filter(|path| !path.trim().is_empty())
                    .context("local platform requires compose_override_file")?;
                if override_file.contains('\0') {
                    bail!("compose_override_file contains an invalid NUL byte");
                }
            }
            OatMode::Aks
                if self.platform.compose_file.is_some()
                    || self.platform.compose_override_file.is_some()
                    || self.platform.compose_project.is_some()
                    || self.platform.compose_state_root.is_some()
                    || self.platform.local.bind_host.is_some()
                    || self.platform.local.endpoint_host.is_some()
                    || self.platform.local.snapshot_root.is_some() =>
            {
                bail!("compose settings are only valid for the local platform");
            }
            OatMode::Aks => {
                for (field, path) in [
                    ("runtime_config_file", self.platform.runtime_config_file.as_deref()),
                    ("bootstrap_env_file", self.platform.bootstrap_env_file.as_deref()),
                ] {
                    let path = path
                        .filter(|value| !value.trim().is_empty())
                        .with_context(|| format!("AKS platform requires platform.{field}"))?;
                    if path.contains('\0') || !Path::new(path).is_file() {
                        bail!("platform.{field} must identify an existing file");
                    }
                }
                if let Some(overlay) = self.platform.kustomize_overlay.as_deref()
                    && (overlay.trim().is_empty() || overlay.contains('\0') || !Path::new(overlay).is_dir())
                {
                    bail!("kustomize_overlay must identify an existing local directory");
                }
                validate_dns_label(&self.namespace, "namespace")?;
                if self.arrowcol_deployment != "arrowcol"
                    || self.arrowcol_container != "dataflow"
                    || self.dfe_deployment != "arrowcol"
                    || self.dfe_container != "dataflow"
                    || self.arrowcol_pod_selector != "app=arrowcol"
                    || self.dfe_pod_selector != "app=arrowcol"
                    || self.edge_secret_name != "arrowcol-router-auth"
                {
                    bail!(
                        "fresh AKS runs use runtime-rendered names: namespace prefix, arrowcol/dataflow and app=arrowcol, with edge_secret_name arrowcol-router-auth"
                    );
                }
            }
        }
        let platform_fields = if self.platform.kind == OatMode::Aks {
            vec![
                ("namespace", self.namespace.as_str()),
                ("arrowcol_deployment", self.arrowcol_deployment.as_str()),
                ("arrowcol_container", self.arrowcol_container.as_str()),
                ("dfe_deployment", self.dfe_deployment.as_str()),
                ("dfe_container", self.dfe_container.as_str()),
                ("arrowcol_pod_selector", self.arrowcol_pod_selector.as_str()),
                ("dfe_pod_selector", self.dfe_pod_selector.as_str()),
                ("edge_secret_file", self.edge_secret_file.as_str()),
                ("dmv_client_image", self.dmv_client_image.as_str()),
                ("edge_secret_name", self.edge_secret_name.as_str()),
            ]
        } else {
            Vec::new()
        };
        for (field, value) in platform_fields.into_iter().chain([
            ("arrowcol_endpoint", self.arrowcol_endpoint.as_str()),
            ("router_metrics_endpoint", self.router_metrics_endpoint.as_str()),
            ("qap_endpoint", self.qap_endpoint.as_str()),
        ]) {
            if value.trim().is_empty() {
                bail!("{field} must not be empty");
            }
            if self.platform.kind == OatMode::Aks {
                if self.edge_secret_file != "/run/secrets/edge-hmac" {
                    bail!("AKS edge_secret_file must be /run/secrets/edge-hmac in the client Job");
                }
                if self.edge_secret_name.len() > 63
                    || !self
                        .edge_secret_name
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                    || !self
                        .edge_secret_name
                        .as_bytes()
                        .first()
                        .is_some_and(u8::is_ascii_alphanumeric)
                    || !self
                        .edge_secret_name
                        .as_bytes()
                        .last()
                        .is_some_and(u8::is_ascii_alphanumeric)
                {
                    bail!("edge_secret_name must be a Kubernetes DNS-label Secret name");
                }
            }
            if self.platform.kind == OatMode::Aks
                && self.arrowcol_deployment == self.dfe_deployment
                && (self.arrowcol_container != self.dfe_container
                    || self.arrowcol_pod_selector != self.dfe_pod_selector)
            {
                bail!("integrated ArrowCol/DFE must use the same container and pod selector");
            }
        }
        let endpoint = Url::parse(&self.arrowcol_endpoint).context("arrowcol_endpoint must be a URL")?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
            bail!("arrowcol_endpoint must be an HTTP(S) URL with a host");
        }
        let metrics_endpoint =
            Url::parse(&self.router_metrics_endpoint).context("router_metrics_endpoint must be a URL")?;
        if !matches!(metrics_endpoint.scheme(), "http" | "https") || metrics_endpoint.host_str().is_none() {
            bail!("router_metrics_endpoint must be an HTTP(S) URL with a host");
        }
        if self.rows_per_table == 0 || self.rows_per_table > 1_000_000 || !(1..=64).contains(&self.dmv_concurrency) {
            bail!("rows_per_table must be 1..=1000000 and dmv_concurrency 1..=64");
        }
        if !(1..=4).contains(&self.clients_per_table)
            || self
                .clients_per_table
                .checked_mul(self.tables.len())
                .is_none_or(|total| total > 128)
        {
            bail!("clients_per_table must be between 1 and 4 and the client fleet may not exceed 128");
        }
        let qap_endpoint = Url::parse(&self.qap_endpoint).context("qap_endpoint must be a URL")?;
        if !matches!(qap_endpoint.scheme(), "http" | "https")
            || qap_endpoint.host_str().is_none()
            || !qap_endpoint.username().is_empty()
            || qap_endpoint.password().is_some()
            || qap_endpoint.query().is_some()
            || qap_endpoint.fragment().is_some()
        {
            bail!("qap_endpoint must be a credential-free HTTP(S) URL with a host");
        }
        if self
            .qap_database
            .as_ref()
            .is_some_and(|database| database.trim().is_empty())
        {
            bail!("qap_database must be omitted or nonempty");
        }
        if self.replica_counts.is_empty()
            || self.replica_counts.len() > 16
            || self.replica_counts.iter().any(|count| *count == 0 || *count > 16)
            || self.replica_counts.iter().copied().collect::<HashSet<_>>().len() != self.replica_counts.len()
        {
            bail!("replica_counts must contain 1 to 16 distinct values from 1 through 16");
        }
        if self.dfe_worker_concurrency.is_empty()
            || self.dfe_worker_concurrency.len() > 16
            || self
                .dfe_worker_concurrency
                .iter()
                .any(|count| *count == 0 || *count > 256)
            || self
                .dfe_worker_concurrency
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != self.dfe_worker_concurrency.len()
        {
            bail!("dfe_worker_concurrency must contain 1 to 16 distinct values from 1 through 256");
        }
        if self.replica_counts.len() * self.dfe_worker_concurrency.len() > 25 {
            bail!("the OAT experiment matrix may contain at most 25 combinations");
        }
        if self.tables.is_empty() || self.tables.len() > 32 {
            bail!("tables must contain between 1 and 32 distinct DMV table names");
        }
        let available_tables = collector_chain::dmv::table_names()?;
        let available = available_tables.iter().map(String::as_str).collect::<HashSet<_>>();
        let mut table_names = HashSet::new();
        for table in &self.tables {
            if table.is_empty() || !table.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
                bail!("invalid DMV table name {table:?}");
            }
            if !table_names.insert(table.to_ascii_lowercase()) {
                bail!("tables must not contain duplicate names: {table}");
            }
            if !available.contains(table.to_ascii_lowercase().as_str()) {
                bail!(
                    "unknown DMV table {table:?}; available tables: {}",
                    available_tables.join(", ")
                );
            }
        }
        let result_url = self.effective_result_url()?;
        let website_url = self.effective_website_url()?;
        let allow_file = self.platform.kind == OatMode::Local;
        validate_output_url(&result_url, "result_url", "experiments", true, allow_file)?;
        validate_output_url(&website_url, "website_url", "$web", false, allow_file)?;
        if result_url == website_url {
            bail!("result_url and website_url must be different object paths");
        }
        Ok(())
    }

    pub(crate) fn effective_result_url(&self) -> Result<String> {
        self.effective_output_url(
            &self.result_url,
            "abfss://experiments@arrowcollector{stamp}.dfs.core.windows.net/arrowcol-oat/results.parquet",
            "result_url",
        )
    }

    pub(crate) fn effective_website_url(&self) -> Result<String> {
        self.effective_output_url(
            &self.website_url,
            "abfss://$web@arrowcollector{stamp}.dfs.core.windows.net/results.parquet",
            "website_url",
        )
    }

    fn effective_output_url(&self, configured: &str, template: &str, field: &str) -> Result<String> {
        if !configured.trim().is_empty() {
            return Ok(configured.to_owned());
        }
        let stamp = self
            .stamp
            .as_deref()
            .with_context(|| format!("{field} is omitted and no default destination is available without a stamp"))?;
        Ok(template.replace("{stamp}", stamp))
    }

    pub(crate) fn deployment_names(&self) -> Vec<&str> {
        if self.platform.kind == OatMode::Local {
            return Vec::new();
        }
        if self.arrowcol_deployment == self.dfe_deployment {
            vec![&self.arrowcol_deployment]
        } else {
            vec![&self.arrowcol_deployment, &self.dfe_deployment]
        }
    }
}

fn default_clients_per_table() -> usize {
    1
}

fn default_run_pin_ttl_ms() -> u64 {
    3_600_000
}

fn validate_dns_label(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 54
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || !value.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        || !value.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
        || value.starts_with("kube-")
        || value == "default"
    {
        bail!("{field} must be a non-reserved Kubernetes DNS label of at most 54 characters");
    }
    Ok(())
}

fn validate_output_url(
    value: &str,
    field: &str,
    expected_container: &str,
    allow_subdirectories: bool,
    allow_file: bool,
) -> Result<()> {
    let url = Url::parse(value).with_context(|| format!("{field} must be a URL"))?;
    if allow_file && url.scheme() == "file" {
        if !url.username().is_empty()
            || url.password().is_some()
            || url.host_str().is_some_and(|host| !host.is_empty())
            || url.query().is_some()
            || url.fragment().is_some()
            || url.to_file_path().is_err()
        {
            bail!("{field} must be an absolute local file URL without credentials, host, query, or fragment");
        }
        return Ok(());
    }
    validate_storage_url(value, field, expected_container, allow_subdirectories)
}

fn validate_storage_url(value: &str, field: &str, expected_container: &str, allow_subdirectories: bool) -> Result<()> {
    let url = Url::parse(value).with_context(|| format!("{field} must be a URL"))?;
    if !matches!(url.scheme(), "abfs" | "abfss" | "az") {
        bail!("{field} must use an ADLS Gen2 URL scheme (abfs, abfss, or az)");
    }
    if url.host_str().is_none() || url.path().trim_matches('/').is_empty() {
        bail!("{field} must identify a storage account and object path");
    }
    let container = match url.scheme() {
        "abfs" | "abfss" => url.username(),
        "az" => url.host_str().unwrap_or(""),
        _ => unreachable!("scheme validated above"),
    };
    if container != expected_container {
        bail!("{field} must target the {expected_container} container");
    }
    let object_path = url.path().trim_matches('/');
    if !allow_subdirectories && object_path.contains('/') {
        bail!("{field} must place the Parquet object directly under the {expected_container} container");
    }
    if (url.scheme() == "az" && !url.username().is_empty())
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("{field} must not embed credentials or query/fragment data");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestConfig {
        config: OatConfig,
        directory: std::path::PathBuf,
    }

    impl std::ops::Deref for TestConfig {
        type Target = OatConfig;

        fn deref(&self) -> &Self::Target {
            &self.config
        }
    }

    impl std::ops::DerefMut for TestConfig {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.config
        }
    }

    impl Drop for TestConfig {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(&self.directory) {
                eprintln!(
                    "remove isolated OAT config fixture {}: {error}",
                    self.directory.display()
                );
                assert!(std::thread::panicking(), "OAT config fixture cleanup failed");
            }
        }
    }

    fn valid() -> TestConfig {
        let directory = std::env::temp_dir().join(format!("arrowcol-oat-config-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory)
            .unwrap_or_else(|error| panic!("create isolated OAT config fixture {}: {error}", directory.display()));
        let runtime_config = directory.join("runtime.json");
        let bootstrap_env = directory.join("bootstrap.env");
        for path in [&runtime_config, &bootstrap_env] {
            std::fs::write(path, "")
                .unwrap_or_else(|error| panic!("write isolated OAT config fixture {}: {error}", path.display()));
        }
        let config = OatConfig {
            tunables: Tunables::default(),
            platform: PlatformConfig {
                kind: OatMode::Aks,
                compose_file: None,
                compose_override_file: None,
                compose_project: None,
                compose_state_root: None,
                kustomize_overlay: None,
                runtime_config_file: Some(runtime_config.to_string_lossy().into_owned()),
                bootstrap_env_file: Some(bootstrap_env.to_string_lossy().into_owned()),
                local: LocalOptions::default(),
            },
            stamp: Some("00000".to_owned()),
            warmup_seconds: 30,
            measure_seconds: 60,
            client_ready_timeout_seconds: 600,
            run_pin_ttl_ms: 3_600_000,
            pilot_mode: false,
            namespace: "arrowcol".to_owned(),
            kubectl_context: None,
            arrowcol_deployment: "arrowcol".to_owned(),
            arrowcol_container: "dataflow".to_owned(),
            dfe_deployment: "arrowcol".to_owned(),
            dfe_container: "dataflow".to_owned(),
            arrowcol_pod_selector: "app=arrowcol".to_owned(),
            dfe_pod_selector: "app=arrowcol".to_owned(),
            arrowcol_endpoint: "http://arrowcol-router.arrowcol.svc:50050".to_owned(),
            router_metrics_endpoint: "http://router.arrowcol.svc:50050/metrics".to_owned(),
            dmv_client_binary: "target/debug/arrowcol-dmv".to_owned(),
            edge_secret_file: "/run/secrets/edge-hmac".to_owned(),
            dmv_client_image: "ghcr.io/mdrakiburrahman/rust-playground/arrowcol-oat-client:dev".to_owned(),
            edge_secret_name: "arrowcol-router-auth".to_owned(),
            tables: vec![
                "sqlserver_wait_stats".to_owned(),
                "sqlserver_cpu_utilization".to_owned(),
            ],
            rows_per_table: 1000,
            clients_per_table: 1,
            dmv_concurrency: 2,
            qap_endpoint: "http://kusto:8080".to_owned(),
            qap_database: Some("experimentqap".to_owned()),
            qap_timeout_seconds: 30,
            replica_counts: vec![1, 2],
            dfe_worker_concurrency: vec![2, 4],
            result_url: "abfss://experiments@account.dfs.core.windows.net/arrowcol-oat/results.parquet".to_owned(),
            website_url: "abfss://$web@account.dfs.core.windows.net/results.parquet".to_owned(),
        };
        TestConfig { config, directory }
    }

    #[test]
    fn accepts_the_initial_matrix_and_separate_adls_paths() -> Result<()> {
        valid().validate()
    }

    #[test]
    fn accepts_all_supported_tables_and_rejects_unknown_and_case_insensitive_duplicates() -> Result<()> {
        let mut config = valid();
        config.tables = collector_chain::dmv::table_names()?;
        assert_eq!(config.tables.len(), 32);
        config.validate()?;

        config.tables[31] = "sqlserver_not_supported".to_owned();
        assert!(config.validate().is_err());

        config.tables[31] = config.tables[0].to_ascii_uppercase();
        assert!(config.validate().is_err());
        Ok(())
    }

    #[test]
    fn pin_ttl_covers_the_full_startup_and_client_lifetime_envelope() -> Result<()> {
        let mut config = valid();
        config.client_ready_timeout_seconds = 1_800;
        config.qap_timeout_seconds = 300;
        config.warmup_seconds = 300;
        config.measure_seconds = 600;
        config.run_pin_ttl_ms = 3_660_000;
        config.validate()?;

        config.run_pin_ttl_ms -= 1;
        assert!(
            config.validate().is_err(),
            "a pin expiring before the bounded Job lifetime must be rejected"
        );
        Ok(())
    }

    #[test]
    fn rejects_incomplete_matrix_and_duplicate_tables() {
        let mut config = valid();
        config.replica_counts = vec![1, 1];
        assert!(config.validate().is_err());

        let mut config = valid();
        config.tables[1] = "SQLSERVER_WAIT_STATS".to_owned();
        assert!(config.validate().is_err());
    }

    #[test]
    fn integrated_topology_has_one_restore_target_and_consistent_selector() -> Result<()> {
        let mut config = valid();
        config.dfe_deployment = config.arrowcol_deployment.clone();
        config.dfe_container = config.arrowcol_container.clone();
        config.dfe_pod_selector = config.arrowcol_pod_selector.clone();
        config.validate()?;
        assert_eq!(config.deployment_names(), vec!["arrowcol"]);
        config.dfe_pod_selector = "app=unused".to_owned();
        assert!(config.validate().is_err());
        Ok(())
    }

    #[test]
    fn accepts_canonical_environment_scoped_az_urls() -> Result<()> {
        let mut config = valid();
        config.result_url = "az://experiments/arrowcol-oat/results.parquet".to_owned();
        config.website_url = "az://$web/results.parquet".to_owned();
        config.validate()
    }

    #[test]
    fn omitted_aks_destinations_default_to_the_stamp_storage_account() -> Result<()> {
        let mut config = valid();
        config.result_url.clear();
        config.website_url.clear();
        config.validate()?;
        assert_eq!(
            config.effective_result_url()?,
            "abfss://experiments@arrowcollector00000.dfs.core.windows.net/arrowcol-oat/results.parquet"
        );
        assert_eq!(
            config.effective_website_url()?,
            "abfss://$web@arrowcollector00000.dfs.core.windows.net/results.parquet"
        );
        Ok(())
    }

    #[test]
    fn allows_one_to_sixteen_replica_cases_and_tables() -> Result<()> {
        let mut config = valid();
        config.replica_counts = (1..=16).collect();
        config.dfe_worker_concurrency.truncate(1);
        config.tables.truncate(1);
        config.validate()
    }

    #[test]
    fn local_mode_accepts_file_result_sinks_without_adls_credentials() -> Result<()> {
        let mut config = valid();
        config.platform = PlatformConfig {
            kind: OatMode::Local,
            compose_file: Some("projects/loadtest/compose.yaml".to_owned()),
            compose_override_file: Some("projects/loadtest/compose.oat.yaml".to_owned()),
            compose_project: Some("oat-test-run".to_owned()),
            compose_state_root: Some("out/arrowcol-oat/local".to_owned()),
            kustomize_overlay: None,
            runtime_config_file: None,
            bootstrap_env_file: None,
            local: LocalOptions::default(),
        };
        config.result_url = "file:///tmp/arrowcol-oat/results.parquet".to_owned();
        config.website_url = "file:///tmp/arrowcol-oat/site/results.parquet".to_owned();
        config.qap_database = None;
        config.validate()
    }

    #[test]
    fn local_mode_can_publish_one_result_to_adls_and_one_to_a_file() -> Result<()> {
        let mut config = valid();
        config.platform = PlatformConfig {
            kind: OatMode::Local,
            compose_file: Some("projects/loadtest/compose.yaml".to_owned()),
            compose_override_file: Some("projects/loadtest/compose.oat.yaml".to_owned()),
            compose_project: Some("oat-test-run".to_owned()),
            compose_state_root: Some("out/arrowcol-oat/local".to_owned()),
            kustomize_overlay: None,
            runtime_config_file: None,
            bootstrap_env_file: None,
            local: LocalOptions::default(),
        };
        config.result_url = "file:///tmp/arrowcol-oat/results.parquet".to_owned();
        config.website_url = "abfss://$web@account.dfs.core.windows.net/results.parquet".to_owned();
        config.qap_database = Some("loadtest".to_owned());
        config.validate()
    }

    #[test]
    fn kustomize_overlay_is_aks_only_and_must_be_a_local_directory() -> Result<()> {
        let mut config = valid();
        config.platform.kustomize_overlay = Some(String::new());
        assert!(config.validate().is_err());

        config.platform.kustomize_overlay = Some(config.directory.to_string_lossy().into_owned());
        config.validate()?;

        config.platform.kind = OatMode::Local;
        config.platform.runtime_config_file = None;
        config.platform.bootstrap_env_file = None;
        config.qap_database = None;
        config.platform.compose_file = Some("projects/loadtest/compose.yaml".to_owned());
        config.platform.compose_override_file = Some("projects/loadtest/compose.oat.yaml".to_owned());
        config.platform.compose_project = Some("oat-test-run".to_owned());
        config.platform.compose_state_root = Some("out/arrowcol-oat/local".to_owned());
        assert!(config.validate().is_err());
        Ok(())
    }

    #[test]
    fn aks_fixture_files_are_isolated_and_missing_files_are_rejected() -> Result<()> {
        let mut first = valid();
        let second = valid();
        assert_ne!(first.directory, second.directory);
        first.validate()?;
        second.validate()?;
        for field in ["runtime_config_file", "bootstrap_env_file"] {
            let original = match field {
                "runtime_config_file" => first.platform.runtime_config_file.clone(),
                _ => first.platform.bootstrap_env_file.clone(),
            };
            let missing = Some(first.directory.join("missing").to_string_lossy().into_owned());
            match field {
                "runtime_config_file" => first.platform.runtime_config_file = missing,
                _ => first.platform.bootstrap_env_file = missing,
            }
            let error = first
                .validate()
                .err()
                .context("AKS validation accepted a missing fixture file")?;
            assert!(error.to_string().contains(field));
            match field {
                "runtime_config_file" => first.platform.runtime_config_file = original,
                _ => first.platform.bootstrap_env_file = original,
            }
        }
        first.validate()?;
        second.validate()?;
        Ok(())
    }

    #[test]
    fn local_mode_rejects_a_nonisolated_qap_database() -> Result<()> {
        let mut config = valid();
        config.platform = PlatformConfig {
            kind: OatMode::Local,
            compose_file: Some("projects/loadtest/compose.yaml".to_owned()),
            compose_override_file: Some("projects/loadtest/compose.oat.yaml".to_owned()),
            compose_project: Some("oat-test-run".to_owned()),
            compose_state_root: Some("out/arrowcol-oat/local".to_owned()),
            kustomize_overlay: None,
            runtime_config_file: None,
            bootstrap_env_file: None,
            local: LocalOptions::default(),
        };
        config.result_url = "file:///tmp/arrowcol-oat/results.parquet".to_owned();
        config.website_url = "file:///tmp/arrowcol-oat/site/results.parquet".to_owned();
        let error = config
            .validate()
            .err()
            .context("local validation accepted the AKS QAP database")?;
        assert!(error.to_string().contains("isolated loadtest Kusto database"));
        config.qap_database = None;
        config.validate()?;
        config.qap_database = Some("loadtest".to_owned());
        config.validate()
    }

    #[test]
    fn aks_mode_rejects_file_result_sinks() {
        let mut config = valid();
        config.result_url = "file:///tmp/arrowcol-oat/results.parquet".to_owned();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_measurements_over_ten_minutes() {
        let mut config = valid();
        config.measure_seconds = 601;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unbounded_client_workload_and_experiment_matrices() {
        let mut config = valid();
        config.dmv_concurrency = 65;
        assert!(config.validate().is_err());

        let mut config = valid();
        config.rows_per_table = 1_000_001;
        assert!(config.validate().is_err());

        let mut config = valid();
        config.replica_counts = (1..=6).collect();
        config.dfe_worker_concurrency = (1..=5).collect();
        assert!(config.validate().is_err());
    }

    #[test]
    fn pilot_duration_uses_full_thirty_second_windows_and_ten_minute_total() {
        let mut config = valid();
        config.pilot_mode = true;
        config.measure_seconds = 90;
        assert!(config.validate().is_ok());

        config.measure_seconds = 91;
        assert!(config.validate().is_err());

        config.measure_seconds = 600;
        config.warmup_seconds = 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_credentials_and_public_non_adls_destinations() {
        let mut config = valid();
        config.result_url = "abfss://user:secret@account.dfs.core.windows.net/path/result.parquet".to_owned();
        assert!(config.validate().is_err());

        let mut config = valid();
        config.website_url = "https://public.example/results.parquet".to_owned();
        assert!(config.validate().is_err());

        let mut config = valid();
        config.qap_endpoint = "https://user:password@kusto.example".to_owned();
        assert!(config.validate().is_err());

        let mut config = valid();
        config.result_url = "abfss://$web@account.dfs.core.windows.net/results.parquet".to_owned();
        assert!(config.validate().is_err());

        let mut config = valid();
        config.website_url = "abfss://$web@account.dfs.core.windows.net/experiments/results.parquet".to_owned();
        assert!(config.validate().is_err());
    }
}
