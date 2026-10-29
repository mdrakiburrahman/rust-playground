use arrowcol_protocol::{CollectorInstanceId, FencingToken, UnixMillis};
use serde::{Deserialize, Serialize};

use crate::LeaseKey;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    Preparing,
    Draining,
    Activating,
    Complete,
}

/// One explicitly requested move, not an autonomous workflow.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TableMigration {
    pub key: LeaseKey,
    pub source: CollectorInstanceId,
    pub target: CollectorInstanceId,
    pub source_epoch: FencingToken,
    pub phase: MigrationPhase,
    pub started_at: UnixMillis,
    pub updated_at: UnixMillis,
    pub reason: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationAck {
    Prepared,
    SourceReleased,
    TargetActive,
}
