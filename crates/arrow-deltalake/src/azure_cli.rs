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

#[cfg(unix)]
struct OwnedProcessGroup(rustix::process::Pid);

#[cfg(unix)]
impl OwnedProcessGroup {
    fn kill(&self) -> Result<()> {
        match rustix::process::kill_process_group(self.0, rustix::process::Signal::KILL) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
            Err(_) => Err(failure("Azure CLI process-group cancellation failed")),
        }
    }
}

#[cfg(unix)]
impl Drop for OwnedProcessGroup {
    fn drop(&mut self) {
        if let Err(error) = self.kill() {
            tracing::error!(%error, "Azure CLI owned process-group cleanup failed");
        }
    }
}

pub(crate) struct BoundedAzureCli {
    program: OsString,
    /// Arguments placed before the fixed CLI arguments; empty in production.
    leading_args: Vec<OsString>,
    scope: String,
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

const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";

impl Default for BoundedAzureCli {
    fn default() -> Self {
        Self {
            program: OsString::from("az"),
            leading_args: Vec::new(),
            scope: STORAGE_SCOPE.to_owned(),
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
        let mut command = tokio::process::Command::new(&self.program);
        command
            .args(&self.leading_args)
            .args(["account", "get-access-token", "--output", "json", "--scope"])
            .arg(&self.scope)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        #[cfg(not(unix))]
        return Err(failure("bounded Azure CLI process-tree cancellation requires Unix"));
        let mut child = command.spawn().map_err(|_| failure("Azure CLI could not start"))?;
        #[cfg(unix)]
        let group = OwnedProcessGroup(
            rustix::process::Pid::from_raw(
                i32::try_from(child.id().ok_or_else(|| failure("Azure CLI PID unavailable"))?)
                    .map_err(|_| failure("Azure CLI PID invalid"))?,
            )
            .ok_or_else(|| failure("Azure CLI PID invalid"))?,
        );
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
                #[cfg(unix)]
                group.kill()?;
                child
                    .wait()
                    .await
                    .map_err(|_| failure("Azure CLI child reaping failed"))?;
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

/// Azure CLI bearer-token source for one OAuth scope, sharing the storage provider's
/// bounded acquisition: owned process-group kill on deadline or cancellation, 64 KiB
/// output bound, no stdout/stderr text in errors and a short token cache.
pub struct AzureCliTokenProvider(BoundedAzureCli);

impl std::fmt::Debug for AzureCliTokenProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AzureCliTokenProvider")
    }
}

impl AzureCliTokenProvider {
    /// Bind a provider to `scope`, which must be exactly `https://<host>/.default`.
    ///
    /// # Errors
    /// Rejects scopes with another scheme or path, a port, credentials, query or fragment.
    pub fn new(scope: &str, deadline: Duration) -> anyhow::Result<Self> {
        let url = url::Url::parse(scope).map_err(|_| anyhow::anyhow!("invalid Azure CLI scope"))?;
        anyhow::ensure!(
            url.scheme() == "https"
                && url.host_str().is_some_and(|host| !host.is_empty())
                && url.port().is_none()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.path() == "/.default"
                && url.as_str() == scope,
            "Azure CLI scope must be exactly https://<host>/.default"
        );
        anyhow::ensure!(
            !deadline.is_zero() && deadline <= Duration::from_secs(60),
            "Azure CLI deadline must be within (0, 60s]"
        );
        Ok(Self(BoundedAzureCli {
            scope: scope.to_owned(),
            deadline,
            ..BoundedAzureCli::default()
        }))
    }

    /// Return a bearer token for the bound scope.
    ///
    /// # Errors
    /// Returns bounded acquisition, process or token-metadata failures without CLI output.
    pub async fn token(&self) -> anyhow::Result<String> {
        match self.0.get_credential().await?.as_ref() {
            AzureCredential::BearerToken(token) => Ok(token.clone()),
            _ => anyhow::bail!("Azure CLI returned an unsupported credential"),
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// Scenario: the actual CLI shape is a shell wrapper with a Python descendant.
    /// Guarantees: acquisition timeout kills descendant work, not only the wrapper.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn wrapper_descendants_stop_on_acquisition_timeout() -> anyhow::Result<()> {
        wrapper_descendants_stop(false).await
    }

    /// Scenario: the caller cancels an in-progress credential request.
    /// Guarantees: dropping the future kills only its owned group, including descendants.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn wrapper_descendants_stop_on_outer_future_cancellation() -> anyhow::Result<()> {
        wrapper_descendants_stop(true).await
    }

    #[cfg(target_os = "linux")]
    async fn wrapper_descendants_stop(cancel: bool) -> anyhow::Result<()> {
        // The descendant sleeps well past both the provider deadline and the
        // start-readiness bound, so a slow fork under load cannot be mistaken for
        // cleanup, and a missed kill always produces the late side effect.
        let (deadline, descendant_sleep) = if cancel {
            (Duration::from_secs(30), Duration::from_secs(3))
        } else {
            (Duration::from_secs(2), Duration::from_secs(5))
        };
        let root = tempfile::tempdir()?;
        let script = root.path().join("fake-az");
        let pid_path = root.path().join("descendant-pid");
        let late = root.path().join("descendant-work");
        std::fs::write(
            &script,
            format!(
                "sh -c 'echo $$ > \"{pid}.tmp\" && mv \"{pid}.tmp\" \"{pid}\"; sleep {sleep}; echo late > \"{late}\"' &\nwait\n",
                pid = pid_path.display(),
                sleep = descendant_sleep.as_secs_f64(),
                late = late.display(),
            ),
        )?;
        let provider = script_provider(&script, deadline);
        let started_at = Instant::now();
        let request = tokio::spawn(async move { provider.get_credential().await });
        let ready = tokio::time::timeout(deadline.min(Duration::from_secs(10)), async {
            while !pid_path.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        if ready.is_err() {
            let outcome = if request.is_finished() {
                format!("{:?}", request.await.map(|result| result.map(|_| ())))
            } else {
                request.abort();
                "still running".to_owned()
            };
            anyhow::bail!("start phase: descendant pid not published within readiness bound; request {outcome}");
        }
        let pid = std::fs::read_to_string(&pid_path)?.trim().parse::<u32>()?;
        if cancel {
            request.abort();
            let joined = request.await;
            anyhow::ensure!(
                joined.as_ref().is_err_and(|error| error.is_cancelled()),
                "cancel phase: request was not cancelled: {:?}",
                joined.map(|result| result.map(|_| ()))
            );
        } else {
            let outcome = request.await?;
            anyhow::ensure!(outcome.is_err(), "timeout phase: provider unexpectedly succeeded");
            anyhow::ensure!(
                started_at.elapsed() < descendant_sleep,
                "timeout phase: provider returned only after descendant work could finish"
            );
        }
        let settle = (started_at + descendant_sleep + Duration::from_secs(1)).saturating_duration_since(Instant::now());
        tokio::time::sleep(settle).await;
        anyhow::ensure!(!late.exists(), "cleanup phase: descendant resumed delayed token work");
        if let Ok(status) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            let state = status.rsplit_once(") ").map(|(_, rest)| rest.chars().next());
            anyhow::ensure!(
                state == Some(Some('Z')),
                "cleanup phase: descendant is still alive: {status}"
            );
        }
        Ok(())
    }

    /// Scenario: a non-storage scope is requested through the public wrapper.
    /// Guarantees: only exact `https://<host>/.default` scopes are accepted and the scope reaches the CLI.
    #[tokio::test]
    async fn public_provider_binds_exact_scope() -> anyhow::Result<()> {
        for invalid in [
            "http://c.kusto.windows.net/.default",
            "https://c.kusto.windows.net/other",
            "https://u:p@c.kusto.windows.net/.default",
            "https://c.kusto.windows.net:444/.default",
            "https://c.kusto.windows.net/.default?x=1",
            "not a url",
        ] {
            assert!(
                AzureCliTokenProvider::new(invalid, Duration::from_secs(1)).is_err(),
                "{invalid}"
            );
        }
        let root = tempfile::tempdir()?;
        let script = root.path().join("fake-az");
        std::fs::write(
            &script,
            "[ \"$6\" = https://c.kusto.windows.net/.default ] || exit 3\nprintf '{\"tokenType\":\"Bearer\",\"accessToken\":\"kusto-token\",\"expires_on\":4102444800}'\n",
        )?;
        let mut provider = AzureCliTokenProvider::new("https://c.kusto.windows.net/.default", Duration::from_secs(5))?;
        provider.0.program = OsString::from("/bin/sh");
        provider.0.leading_args = vec![script.into_os_string()];
        assert_eq!(provider.token().await?, "kusto-token");
        Ok(())
    }

    /// Fixtures are interpreted by `/bin/sh` rather than exec'd: a concurrently
    /// forking test can briefly inherit the fixture's write descriptor, making
    /// `execve` of a freshly written script fail with ETXTBSY.
    fn script_provider(script: &std::path::Path, deadline: Duration) -> BoundedAzureCli {
        BoundedAzureCli {
            program: OsString::from("/bin/sh"),
            leading_args: vec![script.as_os_str().to_owned()],
            deadline,
            ..BoundedAzureCli::default()
        }
    }

    /// Scenario: CLI refresh stalls after authority was checked.
    /// Guarantees: cancellation kills the child; its delayed publication side effect cannot occur.
    #[tokio::test]
    async fn stalled_cli_cannot_resume_after_acquisition_deadline() -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let executable = root.path().join("fake-az");
        let late_write = root.path().join("late-publication");
        std::fs::write(
            &executable,
            format!("sleep 0.3\nprintf late > '{}'\nprintf '{{}}'\n", late_write.display(),),
        )?;
        let provider = script_provider(&executable, Duration::from_millis(20));
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
                "printf started > '{}'\nsleep 0.3\nprintf '{{\"tokenType\":\"Bearer\",\"accessToken\":\"test-token\",\"expires_on\":4102444800}}'\n",
                started.display(),
            ),
        )?;
        let provider = script_provider(&executable, Duration::from_millis(100));
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
            "printf '{\"tokenType\":\"Bearer\",\"accessToken\":\"test-token\",\"expires_on\":4102444800}'\n",
        )?;
        let provider = script_provider(&executable, Duration::from_secs(1));
        let first = provider.get_credential().await?;
        std::fs::remove_file(&executable)?;
        assert!(Arc::ptr_eq(&first, &provider.get_credential().await?));
        for metadata in [
            "{\"tokenType\":\"Basic\",\"accessToken\":\"test-token\",\"expires_on\":4102444800}",
            "{\"tokenType\":\"Bearer\",\"accessToken\":\"test-token\",\"expires_on\":-9223372036854775808}",
            "{\"tokenType\":\"Bearer\",\"accessToken\":\"test-token\",\"expires_on\":0}",
            "{}",
        ] {
            std::fs::write(&executable, format!("printf '%s' '{metadata}'\n"))?;
            let invalid = script_provider(&executable, Duration::from_secs(1));
            assert!(invalid.get_credential().await.is_err());
        }
        Ok(())
    }
}
