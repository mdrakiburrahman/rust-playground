//! Container metadata survives a zero-blob sandbox reset.

use anyhow::{Context, Result, bail};
use object_store::azure::{AzureCredential, AzureCredentialProvider};
use sha2::{Digest, Sha256};
use std::time::Duration;

pub(crate) struct AzureResetFence {
    credentials: AzureCredentialProvider,
    client: reqwest::Client,
    endpoint: String,
    sandbox_id: String,
    generation: String,
}

impl AzureResetFence {
    pub(crate) fn generation(&self) -> &str {
        &self.generation
    }

    pub(crate) async fn open(account: &str, credentials: AzureCredentialProvider, bootstrap: bool) -> Result<Self> {
        let container = std::env::var("AZURE_STORAGE_CHECKPOINT_CONTAINER").unwrap_or_else(|_| "checkpoints".into());
        if !account.bytes().all(|byte| byte.is_ascii_alphanumeric())
            || container.is_empty()
            || !container
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            bail!("invalid Azure control container configuration");
        }
        let subscription =
            std::env::var("AZURE_SUBSCRIPTION_ID").context("Azure sandbox ownership configuration missing")?;
        let sandbox_id = format!("{:x}", Sha256::digest(format!("{subscription}:rust-playground")));
        let mut fence = Self {
            credentials,
            client: reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?,
            endpoint: format!("https://{account}.blob.core.windows.net/{container}?restype=container"),
            sandbox_id: sandbox_id[..12].to_owned(),
            generation: String::new(),
        };
        fence.generation = fence.read_generation(bootstrap).await?;
        Ok(fence)
    }

    async fn read_generation(&self, bootstrap: bool) -> Result<String> {
        let credential = self
            .credentials
            .get_credential()
            .await
            .map_err(|_| anyhow::anyhow!("Azure reset fence authentication failed"))?;
        let AzureCredential::BearerToken(token) = credential.as_ref() else {
            bail!("Azure reset fence requires refreshable bearer identity");
        };
        let response = self
            .client
            .head(&self.endpoint)
            .bearer_auth(token)
            .header("x-ms-version", "2023-11-03")
            .header(
                "x-ms-date",
                chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
            )
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("Azure reset fence request failed"))?;
        if !response.status().is_success() {
            bail!("Azure reset fence metadata unavailable (status {})", response.status());
        }
        let header = |name| -> Result<&str> {
            response
                .headers()
                .get(name)
                .context("Azure reset fence metadata missing")?
                .to_str()
                .context("Azure reset fence metadata invalid")
        };
        if header("x-ms-meta-sandboxid")? != self.sandbox_id {
            bail!("Azure reset fence sandbox ownership mismatch");
        }
        let expected = if bootstrap { "fenced" } else { "ready" };
        if header("x-ms-meta-resetstate")? != expected {
            bail!("Azure sandbox is fenced or not ready for this operation");
        }
        let generation = header("x-ms-meta-resetgeneration")?;
        uuid::Uuid::parse_str(generation).context("invalid Azure reset generation")?;
        header("etag")?;
        Ok(generation.to_owned())
    }

    pub(crate) async fn check(&self) -> Result<()> {
        self.check_mode(false).await
    }

    pub(crate) async fn check_mode(&self, bootstrap: bool) -> Result<()> {
        if self.read_generation(bootstrap).await? != self.generation {
            bail!("Azure reset generation changed; reopen under the new authority");
        }
        Ok(())
    }
}
