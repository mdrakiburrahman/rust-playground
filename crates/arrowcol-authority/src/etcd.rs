//! Small authenticated v3 JSON client; all safety decisions use linearizable transactions.
use anyhow::{Result, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use futures::StreamExt;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, RwLock};

#[derive(Clone)]
pub struct Etcd {
    endpoints: Vec<String>,
    user: String,
    password: String,
    token: Arc<RwLock<String>>,
    authenticated_at: Arc<Mutex<std::time::Instant>>,
    http: reqwest::Client,
}
#[derive(Clone, Debug)]
pub struct Kv {
    pub key: String,
    pub value: String,
    pub revision: i64,
}

pub fn b64(value: impl AsRef<[u8]>) -> String {
    STANDARD.encode(value)
}
fn text(value: &Value) -> Result<String> {
    Ok(String::from_utf8(STANDARD.decode(value.as_str().unwrap_or(""))?)?)
}
pub fn number(value: &Value) -> i64 {
    value
        .as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| value.as_i64())
        .unwrap_or(0)
}
pub fn compare(kv: &Kv) -> Vec<Value> {
    vec![
        json!({"key": b64(&kv.key), "target": "MOD", "result": "EQUAL", "mod_revision": kv.revision.to_string()}),
        json!({"key": b64(&kv.key), "target": "VALUE", "result": "EQUAL", "value": b64(&kv.value)}),
    ]
}
pub fn absent(key: &str) -> Value {
    json!({"key": b64(key), "target": "VERSION", "version": "0", "result":"EQUAL"})
}
pub fn put(key: &str, value: &str, lease: Option<i64>) -> Value {
    let mut request = json!({"key":b64(key), "value":b64(value)});
    if let Some(lease) = lease {
        request["lease"] = json!(lease.to_string());
    }
    json!({"request_put":request})
}
impl Etcd {
    /// # Errors
    /// Returns HTTP client configuration or authentication errors.
    pub async fn new(config: &crate::Config) -> Result<Self> {
        let client = Self {
            endpoints: config.etcd.clone(),
            user: config.etcd_user.clone(),
            password: config.etcd_password.clone(),
            token: Arc::new(RwLock::new(String::new())),
            authenticated_at: Arc::new(Mutex::new(std::time::Instant::now())),
            http: reqwest::Client::builder().timeout(Duration::from_secs(2)).build()?,
        };
        client.login().await?;
        Ok(client)
    }
    async fn login(&self) -> Result<()> {
        for endpoint in &self.endpoints {
            if let Ok(response) = self
                .http
                .post(format!("{endpoint}/v3/auth/authenticate"))
                .json(&json!({"name":self.user,"password":self.password}))
                .send()
                .await
                && let Ok(value) = response.json::<Value>().await
                && let Some(token) = value["token"].as_str()
            {
                *self.token.write().await = token.to_owned();
                return Ok(());
            }
        }
        bail!("etcd authentication unavailable")
    }
    async fn refresh(&self) -> Result<()> {
        let mut authenticated = self.authenticated_at.lock().await;
        if authenticated.elapsed() > Duration::from_secs(120) {
            self.login().await?;
            *authenticated = std::time::Instant::now();
        }
        Ok(())
    }
    /// # Errors
    /// Returns unavailable consensus, authentication or malformed-response errors.
    pub async fn rpc(&self, path: &str, body: Value) -> Result<Value> {
        self.refresh().await?;
        let mut errors = Vec::new();
        for endpoint in &self.endpoints {
            match self
                .http
                .post(format!("{endpoint}/v3/{path}"))
                .header("Authorization", self.token.read().await.as_str())
                .json(&body)
                .send()
                .await
            {
                Ok(response) => {
                    let status = response.status();
                    let value: Value = response.json().await?;
                    if status.is_success() && value.get("error").is_none() {
                        return Ok(value);
                    }
                    errors.push(format!("{status}:{value}"));
                }
                Err(error) => errors.push(error.to_string()),
            }
        }
        bail!("etcd unavailable: {}", errors.join(";"))
    }
    /// # Errors
    /// Returns RPC, decoding or bounded-directory overflow errors.
    pub async fn range(&self, key: &str, prefix: bool) -> Result<(i64, Vec<Kv>)> {
        let mut body = json!({"key":b64(key), "limit":"2048"});
        if prefix {
            let mut end = key.as_bytes().to_vec();
            let last = end.last_mut().ok_or_else(|| anyhow::anyhow!("empty prefix"))?;
            *last += 1;
            body["range_end"] = json!(b64(end));
        }
        let value = self.rpc("kv/range", body).await?;
        ensure!(value["more"] != true, "authority directory capacity exceeded");
        let rows = value["kvs"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|v| {
                Ok(Kv {
                    key: text(&v["key"])?,
                    value: text(&v["value"])?,
                    revision: number(&v["mod_revision"]),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((number(&value["header"]["revision"]), rows))
    }
    /// # Errors
    /// Returns linearizable read or decoding errors.
    pub async fn get(&self, key: &str) -> Result<Option<Kv>> {
        Ok(self.range(key, false).await?.1.into_iter().next())
    }
    /// # Errors
    /// Returns transaction transport or response errors; an uncertain result is not success.
    pub async fn txn(&self, comparisons: Vec<Value>, success: Vec<Value>) -> Result<(bool, i64)> {
        let start = std::time::Instant::now();
        let started_ms = crate::now_ms();
        let response = self
            .rpc("kv/txn", json!({"compare":comparisons,"success":success,"failure":[]}))
            .await?;
        eprintln!(
            "{}",
            json!({"event":"etcd_cas","elapsed_us":start.elapsed().as_micros(),
                "started_ms":started_ms,"observed_ms":crate::now_ms(),
                "revision":number(&response["header"]["revision"]),"succeeded":response["succeeded"]})
        );
        Ok((
            response["succeeded"].as_bool().unwrap_or(false),
            number(&response["header"]["revision"]),
        ))
    }
    /// # Errors
    /// Returns lease grant errors.
    pub async fn grant(&self, ttl: u64) -> Result<i64> {
        Ok(number(
            &self.rpc("lease/grant", json!({"TTL":ttl.to_string()})).await?["ID"],
        ))
    }
    /// # Errors
    /// Rejects expired leases and unavailable renewal.
    pub async fn keepalive(&self, lease: i64) -> Result<()> {
        self.refresh().await?;
        for endpoint in &self.endpoints {
            if let Ok(response) = self
                .http
                .post(format!("{endpoint}/v3/lease/keepalive"))
                .header("Authorization", self.token.read().await.as_str())
                .json(&json!({"ID":lease.to_string()}))
                .send()
                .await
            {
                let mut stream = response.bytes_stream();
                if let Some(Ok(bytes)) = stream.next().await {
                    let value: Value = serde_json::from_slice(&bytes)?;
                    ensure!(number(&value["result"]["TTL"]) > 0, "expired session");
                    return Ok(());
                }
            }
        }
        bail!("lease renewal unavailable")
    }
    /// # Errors
    /// Returns consensus or lease-revocation errors.
    pub async fn revoke(&self, lease: i64) -> Result<()> {
        self.rpc("lease/revoke", json!({"ID":lease.to_string()})).await?;
        Ok(())
    }
    /// # Errors
    /// Returns stream gaps, timeout or oversized/malformed event errors; callers must resnapshot.
    pub async fn watch(&self, after: i64) -> Result<Value> {
        self.refresh().await?;
        let endpoint = self
            .endpoints
            .first()
            .ok_or_else(|| anyhow::anyhow!("no etcd endpoint"))?;
        let response = self
            .http
            .post(format!("{endpoint}/v3/watch"))
            .header("Authorization", self.token.read().await.as_str())
            .json(
                &json!({"create_request":{"key":b64("/adr/tables/"),"range_end":b64("/adr/tables0"),
                "start_revision":(after+1).to_string(),"progress_notify":true}}),
            )
            .send()
            .await?;
        ensure!(response.status().is_success(), "watch unavailable");
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::new();
        while let Some(bytes) = stream.next().await {
            buffer.extend_from_slice(&bytes?);
            ensure!(buffer.len() <= 1024 * 1024, "watch event bound");
            while let Some(end) = buffer.iter().position(|b| *b == b'\n') {
                let line = buffer.drain(..=end).collect::<Vec<_>>();
                let value: Value = serde_json::from_slice(&line)?;
                let result = &value["result"];
                if result["canceled"] == true || number(&result["compact_revision"]) > 0 {
                    return Ok(json!({"reset":true,"compact_revision":result["compact_revision"]}));
                }
                if let Some(events) = result["events"].as_array()
                    && !events.is_empty()
                {
                    let mut assignments = Vec::new();
                    let mut revision = after;
                    for event in events {
                        ensure!(event["type"] != "DELETE", "directory deletion requires resnapshot");
                        let current = number(&event["kv"]["mod_revision"]);
                        revision = revision.max(current);
                        let record: crate::Record = serde_json::from_str(&text(&event["kv"]["value"])?)?;
                        assignments.push(crate::Assignment {
                            revision: current,
                            record,
                        });
                    }
                    return Ok(json!({"reset":false,"revision":revision,"assignments":assignments}));
                }
            }
        }
        bail!("watch stream ended; resnapshot required")
    }
}
