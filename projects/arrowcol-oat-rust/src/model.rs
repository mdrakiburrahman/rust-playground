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
pub struct RunStatus {
    pub id: String,
    pub state: RunState,
    pub progress: String,
    pub result_count: usize,
    pub error: Option<String>,
    pub started_at_unix_ms: u128,
    pub finished_at_unix_ms: Option<u128>,
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
        let sender = self.cancel_senders.lock().await.get(id).cloned().ok_or(CancelRunError::NotActive)?;
        sender.send_replace(true);
        Ok(())
    }

    pub async fn update(&self, id: &str, update: impl FnOnce(&mut RunStatus)) {
        if let Some(status) = self.runs.lock().await.get_mut(id) {
            update(status);
        }
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
}
