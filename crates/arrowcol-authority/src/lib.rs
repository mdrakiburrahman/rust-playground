//! Explicit no-WAL benchmark adapter. The original SQL/Delta adapters are unchanged.
pub mod authority;
pub mod delta_backends;
pub mod delta_runtime;
pub mod delta_store;
pub mod etcd;
pub mod flight;
pub mod local;
pub mod observation;
pub mod placement;
pub mod sql_publication;
pub mod storage;
pub mod timing;

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf, time::SystemTime};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Table {
    pub key: String,
    pub path: Vec<String>,
    pub fingerprints: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub delta: Option<delta_runtime::DeltaConfig>,
    pub tenant: String,
    pub catalog_id: String,
    pub tables: Vec<Table>,
    pub storage: PathBuf,
    pub controller: String,
    pub operator_token: String,
    pub gateway_token: String,
    pub edge_secret: String,
    pub internal_secret: String,
    pub collectors: BTreeMap<String, String>,
    pub etcd: Vec<String>,
    pub etcd_user: String,
    pub etcd_password: String,
}

impl Config {
    /// # Errors
    /// Rejects unreadable, oversized or invalid trust/catalog configuration.
    pub fn load(path: &str) -> Result<Self> {
        ensure!(std::fs::metadata(path)?.len() <= 1024 * 1024, "configuration bound");
        let value: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        ensure!(
            value.edge_secret.len() >= 32 && value.internal_secret.len() >= 32,
            "short trust keys"
        );
        ensure!(
            !value.tenant.is_empty() && !value.catalog_id.is_empty(),
            "missing authenticated catalog scope"
        );
        ensure!(value.tables.len() <= 1024, "catalog bound");
        let mut keys = std::collections::BTreeSet::new();
        for table in &value.tables {
            ensure!(
                safe_id(&table.key) && keys.insert(&table.key),
                "invalid/duplicate immutable identity"
            );
            ensure!(
                table.path.len() == 3 && !table.fingerprints.is_empty(),
                "invalid registry entry"
            );
        }
        Ok(value)
    }

    /// # Errors
    /// Rejects immutable identities outside this authenticated catalog.
    pub fn table(&self, key: &str) -> Result<&Table> {
        self.tables
            .iter()
            .find(|t| t.key == key)
            .ok_or_else(|| anyhow::anyhow!("table unauthorized"))
    }
}

pub fn safe_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Writer {
    pub collector: String,
    pub process: String,
    pub generation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Member {
    pub writer: Writer,
    pub endpoint: String,
    pub pressure: f64,
    pub rss_bytes: u64,
    pub active: usize,
    pub eligible: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Lifecycle {
    Active,
    Draining,
    Recovering,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Head {
    pub commit: String,
    pub manifest: String,
    pub sha256: String,
    pub sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Migration {
    pub id: String,
    pub source: Writer,
    pub target: Writer,
    pub started_ms: u64,
    pub deadline_ms: u64,
    pub phase: String,
    pub cutover_ms: Option<u64>,
    pub activated_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    pub key: String,
    pub epoch: u64,
    pub writer: Writer,
    pub endpoint: String,
    pub lifecycle: Lifecycle,
    pub head: Option<Head>,
    pub pending: Vec<String>,
    pub migration: Option<Migration>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Assignment {
    pub revision: i64,
    pub record: Record,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub table: String,
    pub writer: Writer,
    pub epoch: u64,
    pub commit: String,
    pub unit: String,
    pub parent: Option<Head>,
    pub parquet: String,
    pub parquet_sha256: String,
    pub rows: usize,
    pub arrow_bytes: usize,
    pub fingerprint: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Operation {
    CatalogPolicy {
        key: String,
        revision: i64,
        fingerprints: Vec<String>,
        demand: placement::Demand,
    },
    Policy {
        collector: String,
        writer: Writer,
        revision: i64,
        operation: String,
        excluded: bool,
        block_streams: bool,
        retiring: bool,
    },
    Observe {
        writer: Writer,
        observation: placement::Observation,
        applied_revision: i64,
    },
    OpenStream {
        key: String,
        writer: Writer,
        epoch: u64,
        stream: String,
        fingerprint: String,
    },
    CloseStream {
        key: String,
        writer: Writer,
        stream: String,
    },
    AdmitStream {
        key: String,
        writer: Writer,
        epoch: u64,
        stream: String,
        unit: String,
    },
    PublishDelta {
        key: String,
        writer: Writer,
        epoch: u64,
        unit: String,
        sha256: String,
    },
    RecoverDelta {
        key: String,
        writer: Writer,
        epoch: u64,
    },
    DeltaReceipt {
        key: String,
        unit: String,
    },
    View,
    Watch {
        after: i64,
    },
    Resolve {
        key: String,
    },
    Register {
        member: Member,
    },
    ReplaceSession {
        writer: Writer,
    },
    Admit {
        key: String,
        writer: Writer,
        epoch: u64,
        unit: String,
    },
    Commit {
        key: String,
        writer: Writer,
        epoch: u64,
        head: Head,
    },
    Activate {
        key: String,
        writer: Writer,
        epoch: u64,
        observed: Option<Head>,
    },
    Migrate {
        key: String,
    },
    DrainCollector {
        collector: String,
    },
    Revoke {
        collector: String,
    },
}

#[derive(Clone)]
pub struct Client {
    pub endpoint: String,
    pub token: String,
    pub http: reqwest::Client,
}

impl Client {
    /// # Errors
    /// Returns HTTP client configuration errors.
    pub fn new(endpoint: String, token: String) -> Result<Self> {
        Ok(Self {
            endpoint,
            token,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()?,
        })
    }
    /// # Errors
    /// Returns transport, authorization, schema or authoritative-operation failures.
    pub async fn call(&self, operation: &Operation) -> Result<serde_json::Value> {
        timing::measure("authority-roundtrip", "downstream_wait", async {
            let mut request = self
                .http
                .post(format!("{}/v1/authority", self.endpoint))
                .bearer_auth(&self.token)
                .json(operation);
            if let Some(context) = timing::current() {
                request = request.header("x-adr-trace", context.trace);
                if let Some(parent) = context.parent {
                    request = request.header("x-adr-parent", parent);
                }
            }
            let response = request
                .send()
                .await
                .map_err(|error| anyhow::anyhow!("authority unavailable: {error}"))?;
            let status = response.status();
            let value: serde_json::Value = response.json().await?;
            ensure!(status.is_success(), "authority {}: {}", status, value);
            Ok(value)
        })
        .await
    }
}
