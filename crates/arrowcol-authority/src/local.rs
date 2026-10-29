//! Runtime retirement shares the admission/publication barrier, not a second authority epoch.
use anyhow::{Result, ensure};
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

#[derive(Clone, Default)]
pub struct RuntimeGate(Arc<RwLock<bool>>);

impl RuntimeGate {
    /// # Errors
    /// A retired runtime cannot resume admission, heartbeat or publication.
    pub async fn enter(&self) -> Result<Arc<OwnedRwLockReadGuard<bool>>> {
        let guard = Arc::clone(&self.0).read_owned().await;
        ensure!(!*guard, "NOT_OWNER local runtime retired");
        Ok(Arc::new(guard))
    }

    /// # Errors
    /// Concurrent replacement of an already retired runtime is rejected.
    pub async fn replace(&self) -> Result<Self> {
        let mut retired = self.0.write().await;
        ensure!(!*retired, "NOT_OWNER runtime already replaced");
        *retired = true;
        Ok(Self::default())
    }
}
