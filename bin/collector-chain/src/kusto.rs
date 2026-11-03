use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::json;

pub fn execute(endpoint: &str, database: &str, csl: &str, mgmt: bool, timeout: Duration) -> Result<String> {
    let path = if mgmt { "v1/rest/mgmt" } else { "v2/rest/query" };
    let url = format!("{}/{path}", endpoint.trim_end_matches('/'));
    let response = reqwest::blocking::Client::builder()
        .timeout(timeout)
        .build()?
        .post(&url)
        .json(&json!({ "db": database, "csl": csl }))
        .send()
        .with_context(|| format!("POST {url}"))?;
    let status = response.status();
    let body = response.text()?;
    if !status.is_success() {
        bail!("Kusto returned {status}: {body}");
    }
    Ok(body)
}
