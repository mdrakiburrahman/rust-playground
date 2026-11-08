use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Queued,
    Running,
    Succeeded,
    Failed,
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
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            runs: Mutex::default(),
            run_slot: Arc::new(Semaphore::new(1)),
        }
    }
}

impl AppState {
    pub fn try_start_run(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.run_slot).try_acquire_owned().ok()
    }

    pub async fn update(&self, id: &str, update: impl FnOnce(&mut RunStatus)) {
        if let Some(status) = self.runs.lock().await.get_mut(id) {
            update(status);
        }
    }
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
