use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct ActiveFile {
    pub path: String,
    pub bytes: u64,
    pub partition: String,
}
