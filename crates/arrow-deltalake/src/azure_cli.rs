//! Azure CLI identity with a hard, cancellable subprocess acquisition bound.

use object_store::azure::AzureCredential;
use object_store::{CredentialProvider, Error, Result};
use std::ffi::OsString;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

struct Cached {
    token: Arc<AzureCredential>,
    until: Instant,
}

pub(crate) struct BoundedAzureCli {
    program: OsString,
    deadline: Duration,
    cache: Mutex<Option<Cached>>,
}

impl std::fmt::Debug for BoundedAzureCli {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BoundedAzureCli")
    }
}

fn failure(message: &'static str) -> Error {
    Error::Generic {
        store: "bounded_azure_cli",
        source: Box::new(std::io::Error::other(message)),
    }
}

impl Default for BoundedAzureCli {
    fn default() -> Self {
        Self {
            program: OsString::from("az"),
            deadline: Duration::from_secs(5),
            cache: Mutex::new(None),
        }
    }
}

impl BoundedAzureCli {
    async fn acquire(&self) -> Result<Arc<AzureCredential>> {
        let mut cache = self.cache.lock().await;
        if let Some(cached) = cache.as_ref()
            && Instant::now() < cached.until
        {
            return Ok(Arc::clone(&cached.token));
        }
        let mut child = tokio::process::Command::new(&self.program)
            .args([
                "account",
                "get-access-token",
                "--output",
                "json",
                "--scope",
                "https://storage.azure.com/.default",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| failure("Azure CLI could not start"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| failure("Azure CLI stdout unavailable"))?;
        let mut bytes = Vec::new();
        let result = tokio::time::timeout(self.deadline, async {
            stdout.take(65537).read_to_end(&mut bytes).await?;
            child.wait().await
        })
        .await;
        let status = match result {
            Ok(Ok(status)) if bytes.len() <= 65536 => status,
            _ => {
                child
                    .kill()
                    .await
                    .map_err(|_| failure("Azure CLI cancellation failed"))?;
                return Err(failure("Azure CLI acquisition timed out or exceeded output bound"));
            }
        };
        if !status.success() {
            return Err(failure("Azure CLI acquisition failed"));
        }
        let response: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| failure("Azure CLI returned invalid token metadata"))?;
        if !response
            .get("tokenType")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|kind| kind.eq_ignore_ascii_case("bearer"))
        {
            return Err(failure("Azure CLI returned unsupported credential type"));
        }
        let token = response
            .get("accessToken")
            .and_then(serde_json::Value::as_str)
            .filter(|token| !token.is_empty())
            .ok_or_else(|| failure("Azure CLI returned no token"))?;
        let expires = response
            .get("expires_on")
            .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
            .ok_or_else(|| failure("Azure CLI must report expires_on epoch seconds"))?;
        let remaining = expires
            .checked_sub(chrono::Utc::now().timestamp())
            .and_then(|seconds| seconds.checked_sub(30))
            .ok_or_else(|| failure("Azure CLI token lifetime is invalid"))?;
        if remaining <= 0 {
            return Err(failure("Azure CLI token lifetime is too short"));
        }
        let credential = Arc::new(AzureCredential::BearerToken(token.to_owned()));
        *cache = Some(Cached {
            token: Arc::clone(&credential),
            until: Instant::now()
                + Duration::from_secs(
                    u64::try_from(remaining)
                        .map_err(|_| failure("invalid lifetime"))?
                        .min(60),
                ),
        });
        Ok(credential)
    }
}

#[async_trait::async_trait]
impl CredentialProvider for BoundedAzureCli {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> Result<Arc<AzureCredential>> {
        tokio::time::timeout(self.deadline + Duration::from_secs(1), self.acquire())
            .await
            .map_err(|_| failure("Azure CLI credential admission deadline exceeded"))?
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// Scenario: CLI refresh stalls after authority was checked.
    /// Guarantees: cancellation kills the child; its delayed publication side effect cannot occur.
    #[tokio::test]
    async fn stalled_cli_cannot_resume_after_acquisition_deadline() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let executable = root.path().join("fake-az");
        let late_write = root.path().join("late-publication");
        std::fs::write(
            &executable,
            format!(
                "#!/bin/sh\nsleep 0.3\nprintf late > '{}'\nprintf '{{}}'\n",
                late_write.display(),
            ),
        )?;
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))?;
        let provider = BoundedAzureCli {
            program: executable.into_os_string(),
            deadline: Duration::from_millis(20),
            cache: Mutex::new(None),
        };
        assert!(provider.get_credential().await.is_err());
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!late_write.exists());
        Ok(())
    }

    /// Scenario: a writer passes its fence and authentication then stalls across reset.
    /// Guarantees: cancellation ends the publication future and no data/log write resumes.
    #[tokio::test]
    async fn stalled_authentication_cannot_publish_after_reset() -> anyhow::Result<()> {
        use object_store::{ObjectStore, PutMode};
        use std::sync::atomic::{AtomicBool, Ordering};
        let root = tempfile::tempdir()?;
        let executable = root.path().join("fake-az");
        let started = root.path().join("auth-started");
        std::fs::write(
            &executable,
            format!(
                "#!/bin/sh\nprintf started > '{}'\nsleep 0.3\nprintf '{{\"tokenType\":\"Bearer\",\"accessToken\":\"test-token\",\"expires_on\":4102444800}}'\n",
                started.display(),
            ),
        )?;
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))?;
        let provider = BoundedAzureCli {
            program: executable.into_os_string(),
            deadline: Duration::from_millis(100),
            cache: Mutex::new(None),
        };
        let ready = Arc::new(AtomicBool::new(true));
        let table = root.path().join("table");
        std::fs::create_dir(&table)?;
        let store = object_store::local::LocalFileSystem::new_with_prefix(&table)?;
        let task_ready = Arc::clone(&ready);
        let publication = tokio::spawn(async move {
            assert!(task_ready.load(Ordering::SeqCst));
            provider.get_credential().await?;
            store
                .put_opts(
                    &object_store::path::Path::from("part.parquet"),
                    b"unpublished".to_vec().into(),
                    PutMode::Create.into(),
                )
                .await?;
            store
                .put_opts(
                    &object_store::path::Path::from("_delta_log/00000000000000000001.json"),
                    b"{}".to_vec().into(),
                    PutMode::Create.into(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !started.exists() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        ready.store(false, Ordering::SeqCst);
        assert!(publication.await?.is_err());
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!table.join("part.parquet").exists());
        assert!(!table.join("_delta_log/00000000000000000001.json").exists());
        Ok(())
    }

    /// Scenario: valid CLI credentials are reused, while invalid metadata must fail closed.
    /// Guarantees: cache reuse never launches the CLI again and malformed lifetimes cannot overflow.
    #[tokio::test]
    async fn cache_reuses_valid_identity_and_rejects_bad_token_metadata() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let executable = root.path().join("fake-az");
        std::fs::write(
            &executable,
            "#!/bin/sh\nprintf '{\"tokenType\":\"Bearer\",\"accessToken\":\"test-token\",\"expires_on\":4102444800}'\n",
        )?;
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))?;
        let provider = BoundedAzureCli {
            program: executable.clone().into_os_string(),
            deadline: Duration::from_secs(1),
            cache: Mutex::new(None),
        };
        let first = provider.get_credential().await?;
        std::fs::remove_file(&executable)?;
        assert!(Arc::ptr_eq(&first, &provider.get_credential().await?));
        for metadata in [
            "{\"tokenType\":\"Basic\",\"accessToken\":\"test-token\",\"expires_on\":4102444800}",
            "{\"tokenType\":\"Bearer\",\"accessToken\":\"test-token\",\"expires_on\":-9223372036854775808}",
            "{\"tokenType\":\"Bearer\",\"accessToken\":\"test-token\",\"expires_on\":0}",
            "{}",
        ] {
            std::fs::write(&executable, format!("#!/bin/sh\nprintf '%s' '{metadata}'\n"))?;
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))?;
            let invalid = BoundedAzureCli {
                program: executable.clone().into_os_string(),
                deadline: Duration::from_secs(1),
                cache: Mutex::new(None),
            };
            assert!(invalid.get_credential().await.is_err());
        }
        Ok(())
    }
}
