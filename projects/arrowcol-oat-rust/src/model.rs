use serde::Serialize;
use std::collections::HashMap;
use tokio::sync::Mutex;

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

#[derive(Default)]
pub struct AppState {
    pub runs: Mutex<HashMap<String, RunStatus>>,
}

impl AppState {
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
