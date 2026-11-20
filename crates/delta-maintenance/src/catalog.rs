//! Maintenance discovery through the catalog client's authoritative gRPC contract.

use crate::{DiscoveredTable, MAX_TABLES, TableDiscovery};
use anyhow::{Context, Result, bail};
use arrowcol_catalog_client::rpc::schema_catalog_client::SchemaCatalogClient;
use arrowcol_catalog_client::{InternalRequestSigner, rpc};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;
use tonic::transport::Channel;
use url::Url;

/// Startup limits for catalog discovery.
#[derive(Clone, Debug)]
pub struct CatalogOptions {
    pub runtime_threads: usize,
    pub rpc_timeout: Duration,
    pub max_decoding_message_bytes: usize,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            runtime_threads: 2,
            rpc_timeout: Duration::from_secs(30),
            max_decoding_message_bytes: 32 * 1024 * 1024,
        }
    }
}

impl CatalogOptions {
    /// Validate bounded catalog transport resources.
    ///
    /// # Errors
    /// Rejects zero or excessive runtime, timeout and message limits.
    pub fn validate(&self) -> Result<()> {
        if !(1..=64).contains(&self.runtime_threads)
            || self.rpc_timeout.is_zero()
            || self.rpc_timeout > Duration::from_secs(3600)
            || !(1024..=1024 * 1024 * 1024).contains(&self.max_decoding_message_bytes)
        {
            bail!("invalid catalog runtime threads, RPC timeout or decoding message limit");
        }
        Ok(())
    }
}

/// One atomic catalog snapshot per scheduling pass, shared by all table workers.
/// Uses the generated catalog-client protocol, not a separate discovery endpoint.
pub struct CatalogDiscovery {
    runtime: tokio::runtime::Runtime,
    client: Mutex<SchemaCatalogClient<Channel>>,
    signer: InternalRequestSigner,
    allowlist: Vec<String>,
    tables: Mutex<BTreeMap<String, DiscoveredTable>>,
    rpc_timeout: Duration,
}

impl CatalogDiscovery {
    /// Connect to the shared catalog/Flight gRPC listener.
    ///
    /// # Errors
    /// Returns endpoint, timeout or transport failures without falling back to disk.
    pub fn connect(endpoint: &str, allowlist: Vec<String>) -> Result<Self> {
        Self::connect_with_options(endpoint, allowlist, CatalogOptions::default())
    }

    /// Connect with explicit catalog transport resource limits.
    ///
    /// # Errors
    /// Returns invalid options, identity, timeout or transport failures.
    pub fn connect_with_options(endpoint: &str, allowlist: Vec<String>, options: CatalogOptions) -> Result<Self> {
        let secret_file = std::env::var_os("ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE")
            .context("ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE is required for catalog reads")?;
        let signer = InternalRequestSigner::from_file(secret_file)?;
        Self::connect_with_signer_and_options(endpoint, allowlist, signer, options)
    }

    /// Connects with an explicitly provided internal request signer.
    ///
    /// # Errors
    /// Returns endpoint, timeout or transport failures without falling back to disk.
    pub fn connect_with_signer(endpoint: &str, allowlist: Vec<String>, signer: InternalRequestSigner) -> Result<Self> {
        Self::connect_with_signer_and_options(endpoint, allowlist, signer, CatalogOptions::default())
    }

    /// Connect with an explicit signer and catalog resource limits.
    ///
    /// # Errors
    /// Returns invalid options, timeout or transport failures.
    pub fn connect_with_signer_and_options(
        endpoint: &str,
        allowlist: Vec<String>,
        signer: InternalRequestSigner,
        options: CatalogOptions,
    ) -> Result<Self> {
        options.validate()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(options.runtime_threads)
            .enable_all()
            .build()?;
        let client = runtime
            .block_on(async {
                tokio::time::timeout(options.rpc_timeout, SchemaCatalogClient::connect(endpoint.to_owned())).await
            })
            .context("catalog connection timeout")??;
        Ok(Self {
            runtime,
            client: Mutex::new(client.max_decoding_message_size(options.max_decoding_message_bytes)),
            signer,
            allowlist,
            tables: Mutex::new(BTreeMap::new()),
            rpc_timeout: options.rpc_timeout,
        })
    }
}

fn selected_tables(snapshot: rpc::CatalogSnapshot, allowlist: &[String]) -> Result<BTreeMap<String, DiscoveredTable>> {
    let mut all = BTreeMap::new();
    for table in snapshot.tables {
        if table.provider != "delta" {
            continue;
        }
        let identity = table.table.context("catalog table identity missing")?;
        if identity.catalog.is_empty() || identity.database.is_empty() || identity.table.is_empty() {
            bail!("catalog table identity is incomplete");
        }
        let name = format!("{}.{}.{}", identity.catalog, identity.database, identity.table);
        let discovered = DiscoveredTable {
            name: name.clone(),
            url: if std::path::Path::new(&table.location).is_absolute() {
                Url::from_directory_path(&table.location)
                    .map_err(|()| anyhow::anyhow!("catalog table location is not an absolute directory"))?
            } else {
                Url::parse(&table.location).context("catalog table location is not a URL or absolute path")?
            },
            properties: table.properties.into_iter().collect(),
            log_token: None,
        };
        if all.insert(name.clone(), discovered).is_some() {
            bail!("catalog snapshot repeats table {name}");
        }
    }
    let selected = if allowlist.is_empty() {
        all
    } else {
        let mut selected = BTreeMap::new();
        for requested in allowlist {
            let matches: Vec<_> = all
                .iter()
                .filter(|(name, _)| *name == requested || name.rsplit('.').next() == Some(requested.as_str()))
                .collect();
            if matches.len() != 1 {
                bail!("catalog table {requested:?} is missing or ambiguous; use catalog.database.table");
            }
            let (name, table) = matches[0];
            selected.insert(name.clone(), table.clone());
        }
        selected
    };
    if selected.len() > MAX_TABLES {
        bail!("catalog discovery exceeds {MAX_TABLES} tables; configure --table");
    }
    Ok(selected)
}

impl TableDiscovery for CatalogDiscovery {
    fn tables(&self) -> Result<Vec<String>> {
        let mut client = self
            .client
            .lock()
            .map_err(|_| anyhow::anyhow!("catalog client mutex poisoned"))?;
        let mut request = tonic::Request::new(rpc::Empty {});
        self.signer
            .sign_message(&mut request, "/arrowcol.catalog.v1.SchemaCatalog/GetSnapshot")?;
        request.set_timeout(self.rpc_timeout);
        let snapshot = self
            .runtime
            .block_on(async { tokio::time::timeout(self.rpc_timeout, client.get_snapshot(request)).await })
            .context("catalog snapshot timeout")??
            .into_inner();
        let selected = selected_tables(snapshot, &self.allowlist)?;
        let names = selected.keys().cloned().collect();
        *self
            .tables
            .lock()
            .map_err(|_| anyhow::anyhow!("catalog snapshot mutex poisoned"))? = selected;
        Ok(names)
    }

    fn table(&self, name: &str) -> Result<DiscoveredTable> {
        self.tables
            .lock()
            .map_err(|_| anyhow::anyhow!("catalog snapshot mutex poisoned"))?
            .get(name)
            .cloned()
            .with_context(|| format!("table {name} is absent from the current catalog snapshot"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(database: &str, location: &str) -> rpc::TableSnapshot {
        rpc::TableSnapshot {
            table: Some(rpc::TableRequest {
                catalog: "arrowcol".into(),
                database: database.into(),
                table: "events".into(),
            }),
            location: location.into(),
            provider: "delta".into(),
            properties: std::collections::HashMap::from([("arrowcol.optimize.minFiles".into(), "3".into())]),
            ..Default::default()
        }
    }

    #[test]
    fn catalog_snapshot_preserves_locations_properties_and_requires_unambiguous_names() -> Result<()> {
        let snapshot = || rpc::CatalogSnapshot {
            tables: vec![table("a", "file:///tmp/a/"), table("b", "file:///tmp/b/")],
            ..Default::default()
        };
        assert!(selected_tables(snapshot(), &["events".into()]).is_err());
        let selected = selected_tables(snapshot(), &["arrowcol.a.events".into()])?;
        let table = selected.get("arrowcol.a.events").context("selected table")?;
        assert_eq!(table.url.as_str(), "file:///tmp/a/");
        assert_eq!(
            table.properties.get("arrowcol.optimize.minFiles").map(String::as_str),
            Some("3")
        );
        assert!(
            selected_tables(
                rpc::CatalogSnapshot {
                    tables: vec![super::tests::table("a", "not-a-url")],
                    ..Default::default()
                },
                &[]
            )
            .is_err()
        );
        Ok(())
    }
}
