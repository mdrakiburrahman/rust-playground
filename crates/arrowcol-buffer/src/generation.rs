use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

static PROCESS_GENERATION: OnceLock<Option<CloudGeneration>> = OnceLock::new();

/// Validated cloud reset epoch, captured before durable admission or recovery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CloudGeneration {
    root: String,
    generation: String,
}

impl CloudGeneration {
    /// Validates the configured epoch against ready cloud authority, without opening a writer.
    ///
    /// Local roots return `None`; cloud roots require `ARROWCOL_RESET_GENERATION`.
    ///
    /// # Errors
    /// Rejects invalid configuration, authentication failures, and stale or unready authority.
    pub fn capture(root: &str) -> Result<Option<Self>> {
        if !root.contains("://") {
            return Ok(None);
        }
        let url = url::Url::parse(root).context("invalid Delta root URI")?;
        if url.scheme() == "file" {
            return Ok(None);
        }
        ensure!(url.scheme() == "abfss", "collector cloud root must use abfss");
        let configured =
            std::env::var("ARROWCOL_RESET_GENERATION").context("cloud admission requires ARROWCOL_RESET_GENERATION")?;
        uuid::Uuid::parse_str(&configured).context("ARROWCOL_RESET_GENERATION must be a UUID")?;
        let authority = arrow_deltalake::delta::StorageAuthority::open(&url)?;
        authority.check()?;
        ensure!(
            authority.generation() == Some(configured.as_str()),
            "configured cloud generation does not match ready storage authority"
        );
        Ok(Some(Self {
            root: root.to_owned(),
            generation: configured,
        }))
    }

    /// Validates the process cloud root and epoch before a DFE controller opens WAL.
    ///
    /// # Errors
    /// Rejects an epoch without a root and any cloud authority validation failure.
    pub fn from_environment() -> Result<Option<Self>> {
        match std::env::var("ARROWCOL_DELTA_ROOT") {
            Ok(root) => Self::capture(&root),
            Err(_) => {
                ensure!(
                    std::env::var_os("ARROWCOL_RESET_GENERATION").is_none(),
                    "ARROWCOL_RESET_GENERATION requires ARROWCOL_DELTA_ROOT"
                );
                Ok(None)
            }
        }
    }

    /// Validates and freezes the cloud routing epoch before starting DFE.
    ///
    /// # Errors
    /// Rejects invalid authority or an attempted process-epoch change.
    pub fn initialize_from_environment() -> Result<()> {
        let generation = Self::from_environment()?;
        if let Some(existing) = PROCESS_GENERATION.get() {
            ensure!(existing == &generation, "process cloud generation cannot change");
        } else {
            PROCESS_GENERATION
                .set(generation)
                .map_err(|_| anyhow::anyhow!("process cloud generation concurrently initialized"))?;
        }
        Ok(())
    }

    /// Returns the validated, immutable DFE admission epoch.
    #[must_use]
    pub fn process() -> Option<&'static Self> {
        PROCESS_GENERATION.get().and_then(Option::as_ref)
    }

    /// Returns the validated cloud epoch UUID.
    #[must_use]
    pub fn generation(&self) -> &str {
        &self.generation
    }

    /// Binds an existing durable deployment key to this epoch.
    pub fn bind_instance_key(&self, key: &str) -> String {
        format!("{key}|cloud={}", self.generation)
    }

    /// Refuses stale or unbound WAL before opening the durable engine.
    ///
    /// The immutable marker is fsynced before admission. Existing unbound backlog
    /// requires explicit operator reconciliation, never automatic epoch adoption.
    ///
    /// # Errors
    /// Rejects stale/unbound backlog, invalid markers, or durable filesystem failures.
    pub fn bind_durable_directory(&self, directory: &Path) -> Result<()> {
        std::fs::create_dir_all(directory)?;
        let marker = directory.join(".arrowcol-cloud-generation.json");
        match std::fs::read(&marker) {
            Ok(bytes) => return self.validate_marker(&bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        ensure!(
            std::fs::read_dir(directory)?.next().is_none(),
            "unbound durable backlog requires cloud-generation reconciliation"
        );
        match OpenOptions::new().write(true).create_new(true).open(&marker) {
            Ok(mut file) => {
                file.write_all(&serde_json::to_vec(self)?)?;
                file.sync_all()?;
                File::open(directory)?.sync_all()?;
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.validate_marker(&std::fs::read(marker)?)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn validate_marker(&self, bytes: &[u8]) -> Result<()> {
        let persisted: Self = serde_json::from_slice(bytes)
            .context("invalid durable cloud-generation marker; reconciliation required")?;
        ensure!(&persisted == self, "stale durable cloud-generation key; replay refused");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_wal_cannot_be_rebound_to_same_uri_after_reset() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let g1 = CloudGeneration {
            root: "abfss://container@account.dfs.core.windows.net/root".into(),
            generation: uuid::Uuid::new_v4().to_string(),
        };
        let g2 = CloudGeneration {
            generation: uuid::Uuid::new_v4().to_string(),
            ..g1.clone()
        };
        g1.bind_durable_directory(directory.path())?;
        std::fs::write(directory.path().join("wal"), b"generation-one-receipt")?;
        g1.bind_durable_directory(directory.path())?;
        assert_ne!(g1.bind_instance_key("collector"), g2.bind_instance_key("collector"));
        let Err(error) = g2.bind_durable_directory(directory.path()) else {
            anyhow::bail!("stale epoch was accepted");
        };
        assert!(error.to_string().contains("stale durable"));
        assert_eq!(std::fs::read(directory.path().join("wal"))?, b"generation-one-receipt");
        assert!(!directory.path().join("_delta_log").exists());
        Ok(())
    }

    #[test]
    fn nonempty_legacy_wal_is_not_silently_assigned_current_epoch() -> Result<()> {
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("wal"), b"unbound")?;
        let generation = CloudGeneration {
            root: "abfss://container@account.dfs.core.windows.net/root".into(),
            generation: uuid::Uuid::new_v4().to_string(),
        };
        assert!(generation.bind_durable_directory(directory.path()).is_err());
        assert!(!directory.path().join(".arrowcol-cloud-generation.json").exists());
        Ok(())
    }
}
