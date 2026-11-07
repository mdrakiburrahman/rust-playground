//! Maintenance discovery through the catalog client's authoritative gRPC contract.

use crate::{DiscoveredTable, MAX_TABLES, TableDiscovery};
use anyhow::{Context, Result, bail};
use arrowcol_catalog_client::rpc::{self, schema_catalog_client::SchemaCatalogClient};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;
use tonic::transport::Channel;
use url::Url;

const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// One atomic catalog snapshot per scheduling pass, shared by all table workers.
/// Uses the generated catalog-client protocol, not a separate discovery endpoint.
pub struct CatalogDiscovery {
    runtime: tokio::runtime::Runtime,
    client: Mutex<SchemaCatalogClient<Channel>>,
    allowlist: Vec<String>,
    tables: Mutex<BTreeMap<String, DiscoveredTable>>,
}

impl CatalogDiscovery {
    /// Connect to the shared catalog/Flight gRPC listener.
    ///
    /// # Errors
    /// Returns endpoint, timeout or transport failures without falling back to disk.
    pub fn connect(endpoint: &str, allowlist: Vec<String>) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let client = runtime
            .block_on(async {
                tokio::time::timeout(RPC_TIMEOUT, SchemaCatalogClient::connect(endpoint.to_owned())).await
            })
            .context("catalog connection timeout")??;
        Ok(Self {
            runtime,
            client: Mutex::new(client.max_decoding_message_size(32 * 1024 * 1024)),
            allowlist,
            tables: Mutex::new(BTreeMap::new()),
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
            url: Url::parse(&table.location).context("catalog table location is not a URL")?,
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
        let snapshot = self
            .runtime
            .block_on(async {
                let mut request = tonic::Request::new(rpc::Empty {});
                request.set_timeout(RPC_TIMEOUT);
                tokio::time::timeout(RPC_TIMEOUT, client.get_snapshot(request)).await
            })
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
