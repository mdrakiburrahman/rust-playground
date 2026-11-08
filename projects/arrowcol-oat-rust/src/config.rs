use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::collections::HashSet;
use url::Url;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OatConfig {
    pub namespace: String,
    pub kubectl_context: Option<String>,
    pub arrowcol_deployment: String,
    pub arrowcol_container: String,
    pub dfe_deployment: String,
    pub dfe_container: String,
    pub arrowcol_pod_selector: String,
    pub dfe_pod_selector: String,
    pub arrowcol_endpoint: String,
    pub dmv_client_binary: String,
    pub tables: Vec<String>,
    pub rows_per_table: usize,
    pub dmv_concurrency: usize,
    pub qap_endpoint: String,
    pub qap_database: Option<String>,
    pub qap_timeout_seconds: u64,
    pub replica_counts: Vec<u32>,
    pub dfe_worker_concurrency: Vec<u32>,
    pub result_url: String,
    pub website_url: String,
}

impl OatConfig {
    pub fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("namespace", self.namespace.as_str()),
            ("arrowcol_deployment", self.arrowcol_deployment.as_str()),
            ("arrowcol_container", self.arrowcol_container.as_str()),
            ("dfe_deployment", self.dfe_deployment.as_str()),
            ("dfe_container", self.dfe_container.as_str()),
            ("arrowcol_pod_selector", self.arrowcol_pod_selector.as_str()),
            ("dfe_pod_selector", self.dfe_pod_selector.as_str()),
            ("dmv_client_binary", self.dmv_client_binary.as_str()),
            ("qap_endpoint", self.qap_endpoint.as_str()),
        ] {
            if value.trim().is_empty() {
                bail!("{field} must not be empty");
            }
        }
        let endpoint = Url::parse(&self.arrowcol_endpoint).context("arrowcol_endpoint must be a URL")?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
            bail!("arrowcol_endpoint must be an HTTP(S) URL with a host");
        }
        if self.rows_per_table == 0 || self.dmv_concurrency == 0 || self.qap_timeout_seconds == 0 {
            bail!("rows_per_table, dmv_concurrency, and qap_timeout_seconds must be positive");
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
        if self.replica_counts.iter().copied().collect::<HashSet<_>>().len() != 2
            || !self.replica_counts.contains(&1)
            || !self.replica_counts.contains(&2)
        {
            bail!("replica_counts must contain exactly the initial values 1 and 2");
        }
        if self.dfe_worker_concurrency.len() != 2
            || self.dfe_worker_concurrency.iter().any(|value| *value == 0)
            || self
                .dfe_worker_concurrency
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != 2
        {
            bail!("dfe_worker_concurrency must contain two distinct positive values");
        }
        if self.tables.len() < 2 {
            bail!("tables must contain at least two distinct DMV table names");
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
        validate_storage_url(&self.result_url, "result_url", "experiments", true)?;
        validate_storage_url(&self.website_url, "website_url", "$web", false)?;
        if self.result_url == self.website_url {
            bail!("result_url and website_url must be different object paths");
        }
        Ok(())
    }
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

    fn valid() -> OatConfig {
        OatConfig {
            namespace: "arrowcol".to_owned(),
            kubectl_context: None,
            arrowcol_deployment: "arrowcol".to_owned(),
            arrowcol_container: "arrowcol".to_owned(),
            dfe_deployment: "arrowcol-dataflow".to_owned(),
            dfe_container: "dataflow".to_owned(),
            arrowcol_pod_selector: "app=arrowcol".to_owned(),
            dfe_pod_selector: "app=arrowcol-dataflow".to_owned(),
            arrowcol_endpoint: "http://router.arrowcol.svc:32010".to_owned(),
            dmv_client_binary: "target/debug/arrowcol-dmv".to_owned(),
            tables: vec![
                "sqlserver_wait_stats".to_owned(),
                "sqlserver_cpu_utilization".to_owned(),
            ],
            rows_per_table: 1000,
            dmv_concurrency: 2,
            qap_endpoint: "http://kusto:8080".to_owned(),
            qap_database: Some("experimentqap".to_owned()),
            qap_timeout_seconds: 30,
            replica_counts: vec![1, 2],
            dfe_worker_concurrency: vec![2, 4],
            result_url: "abfss://experiments@account.dfs.core.windows.net/arrowcol-oat/results.parquet".to_owned(),
            website_url: "abfss://$web@account.dfs.core.windows.net/results.parquet".to_owned(),
        }
    }

    #[test]
    fn accepts_the_initial_matrix_and_separate_adls_paths() -> Result<()> {
        valid().validate()
    }

    #[test]
    fn rejects_incomplete_matrix_and_duplicate_tables() {
        let mut config = valid();
        config.replica_counts = vec![1];
        assert!(config.validate().is_err());

        let mut config = valid();
        config.tables[1] = "SQLSERVER_WAIT_STATS".to_owned();
        assert!(config.validate().is_err());
    }

    #[test]
    fn accepts_canonical_environment_scoped_az_urls() -> Result<()> {
        let mut config = valid();
        config.result_url = "az://experiments/arrowcol-oat/results.parquet".to_owned();
        config.website_url = "az://$web/results.parquet".to_owned();
        config.validate()
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
