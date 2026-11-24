use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, watch};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Serialize)]
pub struct LocalTeardown {
    pub cell_id: String,
    pub schedule_unix_ms: u64,
    pub measurement_end_unix_ms: u64,
    pub started_at_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct RunStatus {
    pub id: String,
    pub config_sha256: String,
    pub state: RunState,
    pub progress: String,
    pub result_count: usize,
    pub error: Option<String>,
    pub started_at_unix_ms: u128,
    pub finished_at_unix_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_teardown: Option<LocalTeardown>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aks_cleanup_confirmed: Option<bool>,
    #[serde(skip)]
    pub(crate) aks_cleanup_resolved: bool,
}

impl RunStatus {
    pub fn queued(id: String, config_sha256: String) -> Self {
        Self {
            id,
            config_sha256,
            state: RunState::Queued,
            progress: "queued".to_owned(),
            result_count: 0,
            error: None,
            started_at_unix_ms: unix_time_ms(),
            finished_at_unix_ms: None,
            local_teardown: None,
            aks_cleanup_confirmed: None,
            aks_cleanup_resolved: false,
        }
    }
}

pub struct AppState {
    pub runs: Mutex<HashMap<String, RunStatus>>,
    pub run_slot: Arc<Semaphore>,
    cancel_senders: Mutex<HashMap<String, watch::Sender<bool>>>,
    local_runs: Mutex<HashSet<String>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            runs: Mutex::default(),
            run_slot: Arc::new(Semaphore::new(1)),
            cancel_senders: Mutex::default(),
            local_runs: Mutex::default(),
        }
    }
}

impl AppState {
    pub fn try_start_run(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.run_slot).try_acquire_owned().ok()
    }

    pub async fn register_run_cancel(&self, id: &str, local: bool) -> watch::Receiver<bool> {
        let (sender, receiver) = watch::channel(false);
        self.cancel_senders.lock().await.insert(id.to_owned(), sender);
        if local {
            self.local_runs.lock().await.insert(id.to_owned());
        }
        receiver
    }

    /// Requests cancellation of a locally owned run.
    ///
    /// # Errors
    /// Returns [`CancelRunError::NotFound`] if the run is missing, [`CancelRunError::NotLocal`] if it is not locally
    /// owned, or [`CancelRunError::NotActive`] if cancellation is no longer available.
    pub async fn request_local_cancel(&self, id: &str) -> Result<(), CancelRunError> {
        if !self.local_runs.lock().await.contains(id) {
            return if self.runs.lock().await.contains_key(id) {
                Err(CancelRunError::NotLocal)
            } else {
                Err(CancelRunError::NotFound)
            };
        }
        let runs = self.runs.lock().await;
        let status = runs.get(id).ok_or(CancelRunError::NotFound)?;
        if !matches!(status.state, RunState::Queued | RunState::Running) {
            return Err(CancelRunError::NotActive);
        }
        drop(runs);
        let sender = self
            .cancel_senders
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or(CancelRunError::NotActive)?;
        sender.send_replace(true);
        Ok(())
    }

    pub async fn update(&self, id: &str, update: impl FnOnce(&mut RunStatus)) {
        let started = std::time::Instant::now();
        eprintln!("OAT_UPDATE_DIAGNOSTIC_BEGIN at_ms={} id={}", unix_time_ms(), id);
        let mut runs = self.runs.lock().await;
        let acquired = started.elapsed();
        let mut resulting_state = None;
        if let Some(status) = runs.get_mut(id) {
            update(status);
            resulting_state = Some(format!("{:?}", status.state));
        }
        drop(runs);
        eprintln!(
            "OAT_UPDATE_DIAGNOSTIC at_ms={} id={} state={:?} lock_wait_us={} lock_hold_us={}",
            unix_time_ms(),
            id,
            resulting_state,
            acquired.as_micros(),
            started.elapsed().saturating_sub(acquired).as_micros()
        );
    }

    pub async fn finish_failed_run(&self, id: &str, error: String) {
        self.update(id, |status| {
            status.state = RunState::Failed;
            status.progress = "failed".to_owned();
            status.error = Some(error);
            status.aks_cleanup_confirmed = status.aks_cleanup_resolved.then_some(true);
            status.finished_at_unix_ms = Some(unix_time_ms());
        })
        .await;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelRunError {
    NotFound,
    NotLocal,
    NotActive,
}

pub fn unix_time_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_only_one_active_run_and_releases_the_slot() {
        let state = AppState::default();
        let first = state.try_start_run();
        assert!(first.is_some());
        assert!(state.try_start_run().is_none());
        drop(first);
        assert!(state.try_start_run().is_some());
    }

    #[tokio::test]
    async fn failed_run_exposes_cleanup_confirmation_only_after_positive_resolution() -> anyhow::Result<()> {
        let state = AppState::default();
        let confirmed = RunStatus {
            id: "confirmed".to_owned(),
            config_sha256: "0".repeat(64),
            state: RunState::Queued,
            progress: "queued".to_owned(),
            result_count: 0,
            error: None,
            started_at_unix_ms: unix_time_ms(),
            finished_at_unix_ms: None,
            local_teardown: None,
            aks_cleanup_confirmed: None,
            aks_cleanup_resolved: true,
        };
        let unconfirmed = RunStatus {
            id: "unconfirmed".to_owned(),
            aks_cleanup_resolved: false,
            ..confirmed.clone()
        };
        let queued = serde_json::to_value(&confirmed)?;
        assert!(queued.get("aks_cleanup_confirmed").is_none());
        state.runs.lock().await.insert(confirmed.id.clone(), confirmed);
        state.runs.lock().await.insert(unconfirmed.id.clone(), unconfirmed);

        state
            .finish_failed_run("confirmed", "benchmark failed".to_owned())
            .await;
        state
            .finish_failed_run("unconfirmed", "benchmark failed".to_owned())
            .await;
        let runs = state.runs.lock().await;
        let confirmed = runs
            .get("confirmed")
            .ok_or_else(|| anyhow::anyhow!("confirmed run missing"))?;
        assert!(matches!(confirmed.state, RunState::Failed));
        assert_eq!(confirmed.aks_cleanup_confirmed, Some(true));
        assert_eq!(serde_json::to_value(confirmed)?["aks_cleanup_confirmed"], true);
        assert!(confirmed.finished_at_unix_ms.is_some());
        let unconfirmed = runs
            .get("unconfirmed")
            .ok_or_else(|| anyhow::anyhow!("unconfirmed run missing"))?;
        assert!(matches!(unconfirmed.state, RunState::Failed));
        assert_eq!(unconfirmed.aks_cleanup_confirmed, None);
        Ok(())
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    fn status(id: &str) -> RunStatus {
        RunStatus {
            id: id.to_owned(),
            config_sha256: "0".repeat(64),
            state: RunState::Queued,
            progress: "queued".to_owned(),
            result_count: 0,
            error: None,
            started_at_unix_ms: unix_time_ms(),
            finished_at_unix_ms: None,
            local_teardown: None,
            aks_cleanup_confirmed: None,
            aks_cleanup_resolved: false,
        }
    }

    #[tokio::test]
    async fn cancellation_is_local_only_and_signals_the_run() {
        let state = AppState::default();
        state.runs.lock().await.insert("local".to_owned(), status("local"));
        state.runs.lock().await.insert("aks".to_owned(), status("aks"));
        let local = state.register_run_cancel("local", true).await;
        let aks = state.register_run_cancel("aks", false).await;

        assert!(state.request_local_cancel("local").await.is_ok());
        assert!(*local.borrow());
        assert_eq!(state.request_local_cancel("aks").await, Err(CancelRunError::NotLocal));
        assert!(!*aks.borrow());
        assert_eq!(
            state.request_local_cancel("missing").await,
            Err(CancelRunError::NotFound)
        );
    }

    #[tokio::test]
    async fn completed_local_run_cannot_be_cancelled_again() {
        let state = AppState::default();
        let mut completed = status("done");
        completed.state = RunState::Succeeded;
        state.runs.lock().await.insert("done".to_owned(), completed);
        state.register_run_cancel("done", true).await;
        assert_eq!(state.request_local_cancel("done").await, Err(CancelRunError::NotActive));
    }
}
