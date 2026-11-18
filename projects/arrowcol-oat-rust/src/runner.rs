use crate::config::{
    AKS_RESOURCE_COUNTER_SNAPSHOT_TIMEOUT_MS, EffectiveStopBudgets, OatConfig, ResourceRequirements, ResourceValues,
    parse_cpu_millicores as parse_resource_cpu_millicores, parse_memory_bytes as parse_resource_memory_bytes,
};
use crate::model::{AppState, RunState, unix_time_ms};
use anyhow::{Context, Result, bail, ensure};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray, UInt64Array,
};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_deltalake::delta::object_store_for_url;
use arrowcol_protocol::ImplementationPool;
use arrowcol_routing::{AdmissionPressureGauge, LeaseKey, PressurePolicy, RoutingPolicy, run_table_pin_set_digest};
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime};
use fs2::FileExt;
use futures_util::future::join_all;
use object_store::{GetOptions, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion, path::Path as ObjectPath};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use url::Url;

#[cfg(test)]
include!("runner_failure_tests.rs");
#[cfg(all(test, unix))]
include!("runner_pin_init_tests.rs");
include!("runner_evidence.rs");
include!("runner_tunables.rs");
include!("runner_process_evidence.rs");
include!("runner_commit_evidence.rs");
include!("runner_maintenance.rs");
include!("runner_wal.rs");
include!("runner_overload.rs");
include!("runner_fixture.rs");

#[derive(Clone, Debug, Eq, PartialEq)]
struct CampaignIdentity {
    campaign_id: String,
    owner_id: String,
    fencing_token: u64,
}

impl CampaignIdentity {
    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let campaign_id = lookup("CAMPAIGN_ID").context("CAMPAIGN_ID is required for AKS campaign custody")?;
        let owner_id = lookup("CAMPAIGN_OWNER_ID").context("CAMPAIGN_OWNER_ID is required for AKS campaign custody")?;
        let raw_token =
            lookup("CAMPAIGN_FENCING_TOKEN").context("CAMPAIGN_FENCING_TOKEN is required for AKS campaign custody")?;
        for value in [&campaign_id, &owner_id] {
            let id = uuid::Uuid::parse_str(value).context("campaign custody identity must be a canonical UUID")?;
            if id.is_nil() || id.to_string() != *value {
                bail!("campaign custody identity must be a nonzero canonical UUID");
            }
        }
        if raw_token.is_empty() || !raw_token.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("CAMPAIGN_FENCING_TOKEN must be a positive decimal integer");
        }
        let fencing_token = raw_token
            .parse::<u64>()
            .context("CAMPAIGN_FENCING_TOKEN is out of range")?;
        if fencing_token == 0 || fencing_token > 9_007_199_254_740_991 {
            bail!("CAMPAIGN_FENCING_TOKEN must be a positive JSON-safe integer");
        }
        Ok(Self {
            campaign_id,
            owner_id,
            fencing_token,
        })
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunInventory {
    pub schema_version: u32,
    pub campaign_id: String,
    pub owner_id: String,
    pub fencing_token: u64,
    pub orchestration_run_id: String,
    pub cell_run_id: String,
    pub load_admission: InventoryLoadAdmission,
    pub namespace: InventoryNamespace,
    pub resources: Vec<InventoryResource>,
    pub fic: Option<InventoryFic>,
    pub qap: Option<InventoryQap>,
    #[serde(default)]
    pub evidence_destination: Option<InventoryEvidenceDestination>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub official_measurement: Option<InventoryOfficialMeasurement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_stop_budgets: Option<EffectiveStopBudgets>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planned_cell_duration_ms: Option<u64>,
    pub cleanup: InventoryCleanup,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InventoryLoadAdmission {
    NotAuthorized,
    AuthorizationPending,
    Authorized,
}

impl InventoryLoadAdmission {
    fn can_advance_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::NotAuthorized, Self::AuthorizationPending) | (Self::AuthorizationPending, Self::Authorized)
            )
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryNamespace {
    pub name: String,
    pub uid: Option<String>,
    pub owner_token: String,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InventoryResourceState {
    Planned,
    Observed,
    Removed,
    Absent,
    Uncertain,
}

impl InventoryResourceState {
    fn can_advance_to(self, next: Self) -> bool {
        match self {
            Self::Planned => true,
            Self::Observed | Self::Uncertain => next != Self::Planned,
            Self::Removed | Self::Absent => next == self,
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InventoryResourceRole {
    Collector,
    Client,
    Maintenance,
    Router,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryResource {
    pub api_version: String,
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub uid: Option<String>,
    pub role: InventoryResourceRole,
    pub state: InventoryResourceState,
    #[serde(default)]
    pub absence: Option<InventoryAbsenceReceipt>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryAbsenceReceipt {
    pub api_version: String,
    pub kind: String,
    pub namespace: String,
    pub namespace_uid: Option<String>,
    pub name: String,
    pub observed_at_unix_ms: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryFic {
    pub resource_id: Option<String>,
    pub name: Option<String>,
    pub state: InventoryResourceState,
    #[serde(default)]
    pub absence: Option<InventoryFicAbsenceReceipt>,
    #[serde(default)]
    pub plan: Option<InventoryFicPlan>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InventoryFicPlan {
    pub version: u32,
    pub run_id: String,
    pub namespace: String,
    pub service_account: String,
    pub identity_id: String,
    pub resource_id: String,
    pub client_id: String,
    pub tenant_id: String,
    pub aks_id: String,
    pub server: String,
    pub credential_name: String,
    pub issuer: String,
    pub subject: String,
    pub audience: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryFicAbsenceReceipt {
    pub resource_id: String,
    pub name: String,
    pub observed_at_unix_ms: u64,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryQapBinding {
    pub external_table: String,
    pub relative_path: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryQap {
    pub database: String,
    pub bindings: Vec<InventoryQapBinding>,
    pub state: InventoryResourceState,
    #[serde(default)]
    pub absence: Vec<InventoryQapAbsenceReceipt>,
    #[serde(default)]
    pub reference: Option<InventoryQapReference>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryQapReference {
    pub schema_version: u32,
    pub account: String,
    pub container: String,
    pub blob: String,
    pub run_id: String,
    pub nonce: String,
    pub manifest_sha256: String,
    pub namespace_uid: String,
    pub namespace_owner_token: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryEvidenceDestination {
    pub account: String,
    pub container: String,
    pub blob_prefix: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryOfficialMeasurement {
    pub schema_version: u32,
    pub blob: String,
    pub etag: String,
    pub version: Option<String>,
    pub sha256: String,
    pub byte_length: u64,
    pub effective_config_sha256: String,
    pub submitted_config_sha256: Option<String>,
    pub source_revision: Option<String>,
    pub measurement_start_unix_ms: Option<u64>,
    pub measurement_end_unix_ms: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryQapAbsenceReceipt {
    pub database: String,
    pub external_table: String,
    pub relative_path: String,
    pub observed_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InventoryStepState {
    NotStarted,
    Pending,
    Running,
    Complete,
    Failed,
    Uncertain,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryStep {
    pub state: InventoryStepState,
    pub deadline_unix_ms: Option<u64>,
    pub reason_code: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InventoryCleanup {
    pub stop_admission: InventoryStep,
    pub bounded_drain: InventoryStep,
    pub evidence_capture: InventoryStep,
    pub stop_maintenance: InventoryStep,
    pub terminal_capture: InventoryStep,
    pub handoff: InventoryStep,
}

fn valid_storage_account(value: &str) -> bool {
    (3..=24).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

fn valid_storage_container(value: &str) -> bool {
    (3..=63).contains(&value.len())
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn valid_blob_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 1_024
        && !value.starts_with('/')
        && !value.contains(['?', '#', '@', '\\'])
        && value
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

impl RunInventory {
    /// Validate identities before publishing to the fenced external custody store.
    ///
    /// # Errors
    /// Rejects malformed, ambiguous or success-shaped incomplete identities.
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1 || self.fencing_token == 0 {
            bail!("invalid custody inventory version or fencing token");
        }
        validate_official_measurement_reference(self)?;
        if let Some(budgets) = self.effective_stop_budgets {
            budgets.validate()?;
        }
        if let Some(duration_ms) = self.planned_cell_duration_ms {
            let no_row_rehearsal = duration_ms == 0 && self.load_admission == InventoryLoadAdmission::NotAuthorized;
            if duration_ms > 9_007_199_254_740_991 || (duration_ms == 0 && !no_row_rehearsal) {
                bail!(
                    "planned_cell_duration_ms must be a positive JSON-safe integer except for not-authorized no-row rehearsal inventories"
                );
            }
        }
        for value in [
            &self.campaign_id,
            &self.owner_id,
            &self.orchestration_run_id,
            &self.cell_run_id,
            &self.namespace.owner_token,
        ] {
            let id = uuid::Uuid::parse_str(value).context("invalid custody inventory UUID")?;
            if id.is_nil() || id.to_string() != *value {
                bail!("custody inventory UUID must be nonzero and canonical");
            }
        }
        if self.namespace.owner_token != self.owner_id {
            bail!("custody namespace owner token must be the immutable producer owner");
        }
        let suffix = format!("-{}", self.cell_run_id.replace('-', ""));
        if !self.namespace.name.ends_with(&suffix)
            || self.namespace.name.len() <= suffix.len()
            || self.namespace.name.len() > 63
            || !self
                .namespace
                .name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            bail!("custody namespace must contain the exact cell identity");
        }
        let valid_uid =
            |uid: &str| uuid::Uuid::parse_str(uid).is_ok_and(|parsed| !parsed.is_nil() && parsed.to_string() == uid);
        if self.namespace.uid.as_deref().is_some_and(|uid| !valid_uid(uid)) {
            bail!("custody namespace UID must be a nonzero canonical UUID");
        }
        if let Some(destination) = &self.evidence_destination
            && (!valid_storage_account(&destination.account)
                || !valid_storage_container(&destination.container)
                || !valid_blob_path(&destination.blob_prefix))
        {
            bail!("invalid credential-free custody evidence destination");
        }
        let mut keys = BTreeSet::new();
        for resource in &self.resources {
            if resource.namespace != self.namespace.name
                || resource.name.is_empty()
                || resource.name.len() > 253
                || !resource
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'.')
                || !matches!(
                    (resource.api_version.as_str(), resource.kind.as_str()),
                    ("apps/v1", "Deployment" | "StatefulSet" | "DaemonSet")
                        | ("batch/v1", "Job")
                        | (
                            "v1",
                            "Pod" | "Service" | "ConfigMap" | "ServiceAccount" | "PersistentVolumeClaim" | "Secret",
                        )
                )
                || !keys.insert((&resource.api_version, &resource.kind, &resource.name))
            {
                bail!("invalid or duplicate custody Kubernetes resource");
            }
            if resource.uid.as_deref().is_some_and(|uid| !valid_uid(uid))
                || matches!(
                    resource.state,
                    InventoryResourceState::Observed | InventoryResourceState::Removed
                ) && (resource.uid.is_none() || self.namespace.uid.is_none())
            {
                bail!("observed or removed custody resource requires exact resource and namespace UIDs");
            }
            if resource.state == InventoryResourceState::Absent {
                let receipt = resource
                    .absence
                    .as_ref()
                    .context("absent resource requires a not-found receipt")?;
                if resource.uid.is_some()
                    || receipt.api_version != resource.api_version
                    || receipt.kind != resource.kind
                    || receipt.namespace != resource.namespace
                    || receipt.namespace_uid != self.namespace.uid
                    || receipt.name != resource.name
                    || receipt.observed_at_unix_ms == 0
                {
                    bail!("absent resource receipt must match its exact identity and observation time");
                }
            } else if resource.absence.is_some() {
                bail!("not-found receipt requires absent resource state");
            }
        }
        if let Some(fic) = &self.fic {
            if let Some(plan) = &fic.plan {
                let valid_text = |value: &str, limit: usize| {
                    !value.is_empty() && value.len() <= limit && !value.bytes().any(|byte| byte.is_ascii_control())
                };
                let valid_resource_id =
                    |value: &str| value.starts_with("/subscriptions/") && !value.contains(['?', '#']);
                let valid_uuid = |value: &str| {
                    uuid::Uuid::parse_str(value).is_ok_and(|parsed| !parsed.is_nil() && parsed.to_string() == value)
                };
                let valid_service_account = |value: &str| {
                    (1..=63).contains(&value.len())
                        && value
                            .bytes()
                            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                        && value.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
                        && value.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
                };
                if plan.version != 1
                    || plan.run_id != self.cell_run_id
                    || plan.namespace != self.namespace.name
                    || !valid_service_account(&plan.service_account)
                    || !valid_resource_id(&plan.identity_id)
                    || !valid_resource_id(&plan.resource_id)
                    || !valid_resource_id(&plan.aks_id)
                    || !valid_uuid(&plan.client_id)
                    || !valid_uuid(&plan.tenant_id)
                    || !valid_text(&plan.server, 2_048)
                    || !valid_text(&plan.issuer, 2_048)
                    || !valid_text(&plan.subject, 512)
                    || !valid_text(&plan.audience, 2_048)
                    || !valid_text(&plan.credential_name, 120)
                    || !plan
                        .resource_id
                        .ends_with(&format!("/federatedIdentityCredentials/{}", plan.credential_name))
                    || plan.subject != format!("system:serviceaccount:{}:{}", plan.namespace, plan.service_account)
                    || fic.resource_id.as_ref().is_some_and(|value| value != &plan.resource_id)
                    || fic.name.as_ref().is_some_and(|value| value != &plan.credential_name)
                {
                    bail!(
                        "FIC plan must match the exact cell, namespace, service account and recorded credential identity"
                    );
                }
            }
            if fic.state == InventoryResourceState::Absent {
                let receipt = fic
                    .absence
                    .as_ref()
                    .context("absent FIC requires a not-found receipt")?;
                if fic.resource_id.as_deref() != Some(receipt.resource_id.as_str())
                    || fic.name.as_deref() != Some(receipt.name.as_str())
                    || receipt.observed_at_unix_ms == 0
                {
                    bail!("FIC absence receipt must match its exact identity and observation time");
                }
            } else if fic.absence.is_some() {
                bail!("FIC not-found receipt requires absent state");
            }
            if fic.resource_id.is_some()
                || matches!(
                    fic.state,
                    InventoryResourceState::Observed | InventoryResourceState::Removed
                )
            {
                let name = fic
                    .name
                    .as_deref()
                    .filter(|name| !name.is_empty())
                    .context("FIC name is unknown")?;
                let id = fic.resource_id.as_deref().context("FIC resource ID is unknown")?;
                if !id.starts_with("/subscriptions/")
                    || !id.ends_with(&format!("/federatedIdentityCredentials/{name}"))
                    || id.contains(['?', '#'])
                {
                    bail!("FIC resource ID does not match the exact credential name");
                }
            }
        }
        if let Some(qap) = &self.qap {
            let mut seen = BTreeSet::new();
            if qap.absence.iter().any(|receipt| {
                receipt.database != qap.database
                    || receipt.observed_at_unix_ms == 0
                    || !qap.bindings.iter().any(|binding| {
                        binding.external_table == receipt.external_table
                            && binding.relative_path == receipt.relative_path
                    })
                    || !seen.insert((&receipt.external_table, &receipt.relative_path))
            }) || (qap.state == InventoryResourceState::Absent
                && (qap.bindings.is_empty() || qap.absence.len() != qap.bindings.len()))
                || (qap.state == InventoryResourceState::Planned && !qap.absence.is_empty())
            {
                bail!("QAP absence requires exact, unique not-found receipts for the planned bindings");
            }
            if let Some(reference) = &qap.reference {
                let valid_nonce = |value: &str| {
                    (1..=128).contains(&value.len())
                        && value.bytes().all(|byte| {
                            byte.is_ascii_lowercase()
                                || byte.is_ascii_uppercase()
                                || byte.is_ascii_digit()
                                || byte == b'-'
                        })
                };
                if reference.schema_version != 1
                    || reference.run_id != self.cell_run_id
                    || self.namespace.uid.as_deref() != Some(reference.namespace_uid.as_str())
                    || reference.namespace_owner_token != self.namespace.owner_token
                    || !valid_storage_account(&reference.account)
                    || !valid_storage_container(&reference.container)
                    || !valid_blob_path(&reference.blob)
                    || !valid_nonce(&reference.nonce)
                    || reference.manifest_sha256.len() != 64
                    || !reference
                        .manifest_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                    || !valid_uid(&reference.namespace_uid)
                {
                    bail!(
                        "QAP reference must match the exact run, namespace UID, immutable owner and safe blob identity"
                    );
                }
            }
            let safe_identifier = |value: &str| {
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            };
            let mut binding_keys = BTreeSet::new();
            if !safe_identifier(&qap.database)
                || matches!(
                    qap.state,
                    InventoryResourceState::Observed | InventoryResourceState::Removed
                ) && qap.bindings.is_empty()
                || qap.bindings.iter().any(|binding| {
                    !safe_identifier(&binding.external_table)
                        || !binding_keys.insert((&binding.external_table, &binding.relative_path))
                        || binding.relative_path.is_empty()
                        || binding.relative_path.contains(['?', '#', '@', '\\', ':'])
                        || binding.relative_path.starts_with('/')
                        || binding
                            .relative_path
                            .split('/')
                            .any(|part| part.is_empty() || part == "." || part == "..")
                })
            {
                bail!("invalid custody QAP binding identity");
            }
        }
        for step in [
            &self.cleanup.stop_admission,
            &self.cleanup.bounded_drain,
            &self.cleanup.evidence_capture,
            &self.cleanup.stop_maintenance,
            &self.cleanup.terminal_capture,
            &self.cleanup.handoff,
        ] {
            if step.reason_code.as_ref().is_some_and(|reason| {
                reason.is_empty()
                    || reason.len() > 64
                    || !reason
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            }) {
                bail!("custody failure reason must be a bounded public reason code");
            }
            if matches!(step.state, InventoryStepState::Failed | InventoryStepState::Uncertain)
                && step.reason_code.is_none()
            {
                bail!("failed or uncertain custody step requires a reason");
            }
        }
        if self.cleanup.stop_admission.state == InventoryStepState::Running
            && self
                .cleanup
                .stop_admission
                .deadline_unix_ms
                .is_none_or(|deadline| deadline == 0)
        {
            bail!("running admission stop requires a bounded deadline");
        }
        if self.cleanup.bounded_drain.state == InventoryStepState::Running
            && self
                .cleanup
                .bounded_drain
                .deadline_unix_ms
                .is_none_or(|deadline| deadline == 0)
        {
            bail!("running drain requires a bounded deadline");
        }
        if self.cleanup.stop_maintenance.state == InventoryStepState::Running
            && self
                .cleanup
                .stop_maintenance
                .deadline_unix_ms
                .is_none_or(|deadline| deadline == 0)
        {
            bail!("running maintenance stop requires a bounded deadline");
        }
        if self.cleanup.handoff.state == InventoryStepState::Complete
            && ([
                &self.cleanup.stop_admission,
                &self.cleanup.bounded_drain,
                &self.cleanup.evidence_capture,
                &self.cleanup.stop_maintenance,
                &self.cleanup.terminal_capture,
            ]
            .iter()
            .any(|step| step.state != InventoryStepState::Complete)
                || self.resources.iter().any(|resource| {
                    !matches!(
                        resource.state,
                        InventoryResourceState::Removed | InventoryResourceState::Absent
                    )
                })
                || self.fic.as_ref().is_some_and(|fic| {
                    !matches!(
                        fic.state,
                        InventoryResourceState::Removed | InventoryResourceState::Absent
                    )
                })
                || self.qap.as_ref().is_some_and(|qap| {
                    !matches!(
                        qap.state,
                        InventoryResourceState::Removed | InventoryResourceState::Absent
                    )
                }))
        {
            bail!("custody handoff cannot complete with unresolved steps or resources");
        }
        Ok(())
    }

    /// Validate that an active owner never changes or forgets established resource identity.
    ///
    /// # Errors
    /// Rejects ownership, UID and binding changes or incomplete updates.
    pub fn validate_successor(&self, next: &Self) -> Result<()> {
        self.validate()?;
        next.validate()?;
        if self
            .official_measurement
            .as_ref()
            .is_some_and(|value| next.official_measurement.as_ref() != Some(value))
        {
            bail!("original official cell measurement binding is immutable");
        }
        if self.campaign_id != next.campaign_id
            || self.owner_id != next.owner_id
            || self.fencing_token != next.fencing_token
            || self.orchestration_run_id != next.orchestration_run_id
            || self.cell_run_id != next.cell_run_id
            || !self.load_admission.can_advance_to(next.load_admission)
            || self.effective_stop_budgets != next.effective_stop_budgets
            || self.planned_cell_duration_ms != next.planned_cell_duration_ms
            || self.namespace.name != next.namespace.name
            || self.namespace.owner_token != next.namespace.owner_token
            || self
                .namespace
                .uid
                .as_ref()
                .is_some_and(|uid| next.namespace.uid.as_ref() != Some(uid))
        {
            bail!("custody ownership or established namespace UID changed");
        }
        for resource in &self.resources {
            let updated = next
                .resources
                .iter()
                .find(|candidate| {
                    candidate.api_version == resource.api_version
                        && candidate.kind == resource.kind
                        && candidate.name == resource.name
                })
                .context("custody update dropped a recorded resource")?;
            if updated.role != resource.role
                || resource
                    .uid
                    .as_ref()
                    .is_some_and(|uid| updated.uid.as_ref() != Some(uid))
                || !resource.state.can_advance_to(updated.state)
                || resource
                    .absence
                    .as_ref()
                    .is_some_and(|receipt| updated.absence.as_ref() != Some(receipt))
            {
                bail!("custody update changed an established resource identity");
            }
        }
        if self
            .evidence_destination
            .as_ref()
            .is_some_and(|value| next.evidence_destination.as_ref() != Some(value))
        {
            bail!("custody update changed its evidence destination");
        }
        if let Some(old) = &self.fic {
            let new = next.fic.as_ref().context("custody update dropped the FIC")?;
            if old.plan.as_ref().is_some_and(|plan| new.plan.as_ref() != Some(plan)) {
                bail!("custody update changed an established FIC plan");
            }
            if old.name.as_ref().is_some_and(|value| new.name.as_ref() != Some(value))
                || old
                    .resource_id
                    .as_ref()
                    .is_some_and(|value| new.resource_id.as_ref() != Some(value))
                || !old.state.can_advance_to(new.state)
                || old
                    .absence
                    .as_ref()
                    .is_some_and(|receipt| new.absence.as_ref() != Some(receipt))
            {
                bail!("custody update changed an established FIC identity");
            }
        }
        if let Some(old) = &self.qap {
            let new = next.qap.as_ref().context("custody update dropped QAP bindings")?;
            if old.database != new.database
                || old.bindings.iter().any(|binding| !new.bindings.contains(binding))
                || !old.state.can_advance_to(new.state)
                || old.absence.iter().any(|receipt| !new.absence.contains(receipt))
                || old
                    .reference
                    .as_ref()
                    .is_some_and(|reference| new.reference.as_ref() != Some(reference))
            {
                bail!("custody update changed an established QAP identity or reference");
            }
        }
        for (old, new) in [
            (&self.cleanup.stop_admission, &next.cleanup.stop_admission),
            (&self.cleanup.bounded_drain, &next.cleanup.bounded_drain),
            (&self.cleanup.evidence_capture, &next.cleanup.evidence_capture),
            (&self.cleanup.stop_maintenance, &next.cleanup.stop_maintenance),
            (&self.cleanup.terminal_capture, &next.cleanup.terminal_capture),
            (&self.cleanup.handoff, &next.cleanup.handoff),
        ] {
            if old.state == InventoryStepState::Complete && new.state != InventoryStepState::Complete {
                bail!("custody update regressed a completed cleanup receipt");
            }
        }
        Ok(())
    }

    fn validate_custody_receipt(&self, output: &[u8], ready: bool) -> Result<Option<String>> {
        let response: Value = serde_json::from_slice(output).context("invalid custody receipt JSON")?;
        if response["ok"] != true {
            let code = match response["code"].as_str() {
                Some("campaign_fenced") => "campaign_fenced",
                Some("write_outcome_unknown") => "write_outcome_unknown",
                Some("invalid_or_storage_error") => "invalid_or_storage_error",
                _ => "invalid_response",
            };
            bail!("custody command failed: {code}");
        }
        let receipt = response["receipt"].as_object().context("missing custody receipt")?;
        let status = if ready { "ready" } else { "persisted" };
        if receipt.get("status").and_then(Value::as_str) != Some(status)
            || receipt.get("campaign_id").and_then(Value::as_str) != Some(self.campaign_id.as_str())
            || receipt.get("owner_id").and_then(Value::as_str) != Some(self.owner_id.as_str())
            || receipt.get("fencing_token").and_then(Value::as_u64) != Some(self.fencing_token)
            || receipt.get("orchestration_run_id").and_then(Value::as_str) != Some(self.orchestration_run_id.as_str())
            || receipt.get("cell_run_id").and_then(Value::as_str) != Some(self.cell_run_id.as_str())
            || receipt.get("namespace_uid") != Some(&json!(self.namespace.uid))
        {
            bail!("custody receipt does not match exact campaign, cell, fence and namespace identity");
        }
        if ready && self.namespace.uid.is_none() {
            bail!("custody readiness requires an observed namespace UID");
        }
        if !ready
            && (receipt.get("resource_count").and_then(Value::as_u64) != Some(self.resources.len() as u64)
                || receipt.get("fic_state") != Some(&json!(self.fic.as_ref().map(|fic| fic.state)))
                || receipt.get("qap_state") != Some(&json!(self.qap.as_ref().map(|qap| qap.state)))
                || receipt.get("etag").and_then(Value::as_str).is_none_or(str::is_empty))
        {
            bail!("custody persistence receipt is incomplete or does not match inventory");
        }
        let etag = if ready {
            None
        } else {
            Some(
                receipt
                    .get("etag")
                    .and_then(Value::as_str)
                    .context("custody persistence receipt omitted ETag")?
                    .to_owned(),
            )
        };
        Ok(etag)
    }

    /// Publish this inventory and require the fenced store's matching persistence receipt.
    ///
    /// # Errors
    /// Refuses unconfirmed writes and mismatched or fenced receipts without retrying.
    pub(crate) async fn publish_to_custody(&self, config_paths: AksHelperConfigPaths<'_>) -> Result<String> {
        self.custody_command(false, config_paths)
            .await?
            .context("custody publisher did not return an ETag")
    }

    /// Verify that the fenced store has armed this exact cell before load starts.
    ///
    /// # Errors
    /// Refuses an unready or mismatched receipt.
    pub(crate) async fn assert_custody_ready(&self, config_paths: AksHelperConfigPaths<'_>) -> Result<()> {
        self.custody_command(true, config_paths).await.map(|_| ())
    }

    #[cfg(unix)]
    async fn custody_command(&self, ready: bool, config_paths: AksHelperConfigPaths<'_>) -> Result<Option<String>> {
        self.validate()?;
        let payload = if ready {
            None
        } else {
            let bytes = serde_json::to_vec(self).context("serialize custody inventory")?;
            if bytes.len() > 1024 * 1024 {
                bail!("custody inventory exceeds publisher input limit");
            }
            Some(bytes)
        };
        let action = if ready { "assert-ready" } else { "publish-inventory" };
        let action_args = if ready {
            vec![self.owner_id.clone(), self.fencing_token.to_string()]
        } else {
            Vec::new()
        };
        let mut command = aks_campaign_cli_command(config_paths, action, &action_args)?;
        if let Some((code, stdout)) = fixture_command_output(&command, payload.as_deref()).await? {
            if code != 0 {
                bail!("fixture custody command failed; outcome may be unknown and must not be replayed");
            }
            return self.validate_custody_receipt(stdout.as_bytes(), ready);
        }
        command
            .stdin(if ready { Stdio::null() } else { Stdio::piped() })
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(60), async {
            let mut child = command.spawn().context("start fenced custody command")?;
            let pid = i32::try_from(child.id().context("custody command PID unavailable")?)
                .context("custody command PID invalid")?;
            let _group =
                OwnedQapHelperGroup(rustix::process::Pid::from_raw(pid).context("custody command PID invalid")?);
            if let Some(payload) = payload {
                let mut stdin = child.stdin.take().context("custody publisher stdin unavailable")?;
                stdin.write_all(&payload).await.context("write custody inventory")?;
                drop(stdin);
            }
            child
                .wait_with_output()
                .await
                .context("wait for fenced custody command")
        })
        .await
        .context("custody command timed out; write outcome may be unknown and must not be retried")??;
        if !output.status.success() {
            return self.validate_custody_receipt(&output.stdout, ready).and_then(|_| {
                Err(anyhow::anyhow!(
                    "custody command exited unsuccessfully despite a success-shaped receipt"
                ))
            });
        }
        self.validate_custody_receipt(&output.stdout, ready)
    }

    #[cfg(not(unix))]
    async fn custody_command(&self, _ready: bool, _config_paths: AksHelperConfigPaths<'_>) -> Result<Option<String>> {
        bail!("fenced custody requires Unix process-group cancellation")
    }
}

#[derive(Clone, Debug)]
struct OatResult {
    run_id: String,
    mode: String,
    stamp: Option<String>,
    host: String,
    experiment: String,
    started_at_ms: i64,
    measure_seconds: f64,
    arrowcols: i32,
    clients: i32,
    tables: i32,
    table_owner: Option<String>,
    pinning_ok: bool,
    pinning_error: Option<String>,
    failure: Option<String>,
    status: String,
    git_sha: String,
    tunables_json: String,
    evidence_json: Option<String>,
    replicas: u64,
    dfe_worker_concurrency: u64,
    table: String,
    rows: Option<u64>,
    duration_ms: Option<u64>,
    rows_per_second: Option<f64>,
    cpu_millicores: Option<f64>,
    memory_bytes: Option<u64>,
    qap: QapMetrics,
}

#[derive(Clone, Copy, Debug, Default)]
struct QapMetrics {
    observation_complete: u64,
    observed_tables: u64,
    completion_percent: Option<f64>,
    pending_artifacts: Option<u64>,
    cached_bytes: Option<u64>,
    unhealthy_catalogs: Option<u64>,
    lag_seconds: Option<f64>,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
struct ResourceMetrics {
    cpu_millicores: f64,
    memory_bytes: u64,
}

struct QapBinding {
    logical_table: String,
    external_table: String,
}

#[derive(Clone, Debug)]
struct ClientHandle {
    client_id: String,
    table: String,
    process: ClientProcess,
}

#[derive(Clone, Debug)]
enum ClientProcess {
    Local { container_id: String },
    Aks { job_name: String },
}

#[derive(Clone, Default)]
struct ClientProgress {
    ready: bool,
    schedule_received: bool,
    completed: bool,
    failure: Option<String>,
    output: String,
}

#[async_trait::async_trait]
trait Platform: Send + Sync {
    fn kind(&self) -> crate::config::OatMode;
    fn workload_endpoint(&self, config: &OatConfig) -> String;
    fn delta_root(&self) -> Option<&str> {
        None
    }
    fn router_metrics_endpoint(&self, config: &OatConfig) -> Result<String>;
    async fn ensure_router_metrics_endpoint(&mut self, config: &OatConfig) -> Result<(String, bool)> {
        Ok((self.router_metrics_endpoint(config)?, false))
    }
    fn qap_endpoint(&self, config: &OatConfig) -> String;
    async fn ensure_stack(&mut self, config: &OatConfig, orchestration_run_id: &str, replicas: u32) -> Result<()>;
    async fn scale_arrowcols(&mut self, config: &OatConfig, replicas: u32, concurrency: u32) -> Result<()>;
    async fn initialize_run_table_pins(
        &mut self,
        config: &OatConfig,
        run_id: &str,
        expected_keys_json: &str,
        expires_at_unix_ms: u64,
        now_unix_ms: u64,
    ) -> Result<()>;
    fn qap_observation_mode(&self) -> arrowcol_loadgen::ObservationMode;
    fn qap_database(&self, config: &OatConfig) -> Result<String>;
    async fn initialize_qap(&mut self, config: &OatConfig, run_id: &str, tables: &[String]) -> Result<Vec<String>>;
    async fn prepare_campaign_load_admission(
        &mut self,
        _config: &OatConfig,
        _run_id: &str,
        _tables: &[String],
    ) -> Result<()> {
        Ok(())
    }
    async fn finalize_campaign_load_admission(
        &mut self,
        _config: &OatConfig,
        _run_id: &str,
        _tables: &[String],
    ) -> Result<()> {
        Ok(())
    }
    async fn publish_official_measurement(
        &mut self,
        _config: &OatConfig,
        _rows: &[OatResult],
        _submitted: Option<&SubmittedConfigIdentity>,
        _deadline: tokio::time::Instant,
    ) -> Result<()> {
        Ok(())
    }
    fn mark_qap_outcome_unresolved(&mut self) {}
    async fn prepare_client_fleet(&mut self, config: &OatConfig, run_id: &str) -> Result<()>;
    async fn start_client(
        &mut self,
        config: &OatConfig,
        run_id: &str,
        table: &str,
        client_index: usize,
        args: &[String],
    ) -> Result<ClientHandle>;
    async fn poll_client(&self, config: &OatConfig, handle: &ClientHandle) -> Result<ClientProgress>;
    async fn poll_client_fleet(&self, config: &OatConfig, handles: &[ClientHandle]) -> Result<Vec<ClientProgress>> {
        join_all(handles.iter().map(|handle| self.poll_client(config, handle)))
            .await
            .into_iter()
            .collect()
    }
    async fn arm_clients(&mut self, config: &OatConfig, run_id: &str, run_start_unix_ms: u64) -> Result<()>;
    async fn finish_client(&self, config: &OatConfig, handle: ClientHandle) -> Result<String>;
    async fn cancel_client(&self, config: &OatConfig, handle: &ClientHandle) -> Result<()>;
    fn resource_sampler_source(&self) -> Option<ResourceSamplerSource> {
        None
    }
    async fn native_telemetry_source(
        &self,
        _config: &OatConfig,
        _qap_tables: &[String],
    ) -> Result<Option<NativeTelemetrySource>> {
        Ok(None)
    }
    async fn process_logs(&self, _config: &OatConfig) -> Result<Vec<ProcessLog>> {
        Ok(Vec::new())
    }
    fn retain_cell_process_baseline(&mut self, _logs: &[ProcessLog]) {}
    async fn process_logs_for_role(&self, config: &OatConfig, role: ComponentRole) -> Result<Vec<ProcessLog>> {
        Ok(self
            .process_logs(config)
            .await?
            .into_iter()
            .filter(|log| log.role == role)
            .collect())
    }
    fn shutdown_drain_evidence(&self) -> Option<Value> {
        None
    }
    fn resource_evidence(&self) -> Option<Value> {
        None
    }
    async fn finish_resource_measurement(&mut self, _config: &OatConfig) -> Result<()> {
        Ok(())
    }
    async fn cleanup(&mut self, config: &OatConfig) -> Result<()>;
}

struct AksPlatform {
    run_id: String,
    resource_readback: Vec<Value>,
    cpu_throttling_start: Option<Vec<CpuThrottleSample>>,
    cpu_throttling_evidence: Option<Value>,
    namespace_created: bool,
    namespace_create_attempted: bool,
    namespace_owner_token: String,
    namespace_uid: Option<String>,
    runtime_dir: Option<PathBuf>,
    qap_relative_data_root: Option<String>,
    delta_root: Option<String>,
    router_metrics_url: Option<String>,
    router_tunnel: Option<OwnedKubectlProcess>,
    fic_record_path: Option<PathBuf>,
    fic_ready_snapshot_path: Option<PathBuf>,
    fic_prepare_attempted: bool,
    qap_record_path: Option<PathBuf>,
    qap_bind_attempted: bool,
    qap_cleanup_confirmed: bool,
    qap_outcome_unresolved: bool,
    run_table_initialization_attempted: bool,
    run_table_initialization_drained: bool,
    workload_stop_record_path: Option<PathBuf>,
    schedule_configmap_name: String,
    service_account: Option<String>,
    router_yaml: Option<String>,
    shutdown_drain_evidence: Option<Value>,
    campaign_cli_paths: Option<OwnedAksCampaignConfigPaths>,
}

struct OwnedKubectlProcess {
    child: Child,
    stderr: JoinHandle<std::io::Result<Vec<u8>>>,
}

struct PublicationBacklogScraper {
    endpoint: String,
    _tunnel: Option<OwnedKubectlProcess>,
    client: reqwest::Client,
}

impl PublicationBacklogScraper {
    async fn start(source: &ResourceSamplerSource, config: &OatConfig) -> Result<Self> {
        let (endpoint, tunnel) = match source {
            ResourceSamplerSource::Aks | ResourceSamplerSource::AksNative(_) => {
                let (endpoint, tunnel) = start_aks_collector_metrics_tunnel(config).await?;
                (endpoint, Some(tunnel))
            }
            ResourceSamplerSource::Local {
                collector_metrics_url, ..
            } => (
                collector_metrics_url
                    .clone()
                    .context("local collector metrics endpoint is unavailable")?,
                None,
            ),
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .context("build publication backlog metrics client")?;
        Ok(Self {
            endpoint,
            _tunnel: tunnel,
            client,
        })
    }

    async fn scrape(&self) -> Result<PublicationBacklogMetrics> {
        parse_publication_backlog_metrics(&self.read_endpoint(&self.endpoint).await?)
    }

    async fn read_endpoint(&self, endpoint: &str) -> Result<String> {
        const MAX_METRICS_BYTES: u64 = 1_048_576;
        let response = self
            .client
            .get(endpoint)
            .send()
            .await
            .context("request collector admin metrics")?;
        if !response.status().is_success() {
            bail!("collector admin metrics returned HTTP {}", response.status());
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_METRICS_BYTES)
        {
            bail!("collector admin metrics exceeded the evidence size limit");
        }
        let mut response = response;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.context("read collector admin metrics")? {
            if body.len().saturating_add(chunk.len()) > MAX_METRICS_BYTES as usize {
                bail!("collector admin metrics exceeded the evidence size limit");
            }
            body.extend_from_slice(&chunk);
        }
        String::from_utf8(body).context("collector admin metrics are not UTF-8")
    }
}

struct LocalPlatform {
    run_id: String,
    state_dir: PathBuf,
    environment: HashMap<String, String>,
    control_dir: PathBuf,
    client_progress: std::sync::Mutex<HashMap<String, ClientProgress>>,
    router_metrics_url: Option<String>,
    collector_metrics_url: Option<String>,
    qap_url: Option<String>,
    daemon_endpoint: Option<String>,
    shutdown_drain_evidence: Option<Value>,
    maintenance_cell_baseline: Vec<ProcessLog>,
}

#[async_trait::async_trait]
impl Platform for AksPlatform {
    fn delta_root(&self) -> Option<&str> {
        self.delta_root.as_deref()
    }

    fn resource_evidence(&self) -> Option<Value> {
        Some(json!({
            "readbacks": self.resource_readback,
            "cpu_throttling": self.cpu_throttling_evidence,
        }))
    }

    async fn finish_resource_measurement(&mut self, config: &OatConfig) -> Result<()> {
        let end = capture_aks_cpu_throttling(config, &self.resource_readback).await?;
        self.cpu_throttling_evidence = Some(compare_cpu_throttling(
            self.cpu_throttling_start.take().as_deref().unwrap_or_default(),
            &end,
        ));
        Ok(())
    }

    fn kind(&self) -> crate::config::OatMode {
        crate::config::OatMode::Aks
    }

    async fn process_logs(&self, config: &OatConfig) -> Result<Vec<ProcessLog>> {
        aks_process_logs(config).await
    }

    async fn process_logs_for_role(&self, config: &OatConfig, role: ComponentRole) -> Result<Vec<ProcessLog>> {
        aks_process_logs_for_role(config, Some(role)).await
    }

    fn workload_endpoint(&self, config: &OatConfig) -> String {
        config.arrowcol_endpoint.clone()
    }

    fn router_metrics_endpoint(&self, _config: &OatConfig) -> Result<String> {
        self.router_metrics_url
            .clone()
            .context("AKS router metrics tunnel is not ready")
    }

    async fn ensure_router_metrics_endpoint(&mut self, config: &OatConfig) -> Result<(String, bool)> {
        let restarted = ensure_router_metrics_tunnel(config, self).await?;
        Ok((self.router_metrics_endpoint(config)?, restarted))
    }

    fn qap_endpoint(&self, config: &OatConfig) -> String {
        config.qap_endpoint.clone()
    }

    async fn ensure_stack(&mut self, config: &OatConfig, orchestration_run_id: &str, replicas: u32) -> Result<()> {
        prepare_aks_platform(config, self, orchestration_run_id, replicas).await?;
        if let Some(overlay) = config.platform.kustomize_overlay.as_deref() {
            apply_aks_overlay(config, overlay, self).await?;
        }
        Ok(())
    }

    async fn scale_arrowcols(&mut self, config: &OatConfig, replicas: u32, concurrency: u32) -> Result<()> {
        configure_deployments(config, replicas, concurrency).await?;
        for name in config.deployment_names() {
            rollout(config, name).await?;
        }
        self.resource_readback = aks_deployment_resource_evidence(config).await?;
        Ok(())
    }

    async fn initialize_run_table_pins(
        &mut self,
        config: &OatConfig,
        run_id: &str,
        expected_keys_json: &str,
        expires_at_unix_ms: u64,
        now_unix_ms: u64,
    ) -> Result<()> {
        run_aks_pin_setup_job(
            config,
            self,
            run_id,
            expected_keys_json,
            expires_at_unix_ms,
            now_unix_ms,
        )
        .await?;
        start_aks_maintenance(config).await?;
        if maintenance_enabled(config) {
            let resources = config.effective_aks_resources();
            self.resource_readback.push(
                aks_deployment_resource_readback(
                    config,
                    "maintenance",
                    "delta-maintenance",
                    "maintenance",
                    &resources.maintenance,
                )
                .await?,
            );
        }
        verify_maintenance_stop_budget_before_admission(config, self).await
    }

    fn qap_observation_mode(&self) -> arrowcol_loadgen::ObservationMode {
        arrowcol_loadgen::ObservationMode::Azure
    }

    fn qap_database(&self, config: &OatConfig) -> Result<String> {
        config
            .qap_database
            .clone()
            .or_else(|| std::env::var("AZURE_KUSTO_EXPERIMENT_DATABASE").ok())
            .context("AKS QAP requires qap_database or AZURE_KUSTO_EXPERIMENT_DATABASE")
    }

    async fn initialize_qap(&mut self, config: &OatConfig, run_id: &str, tables: &[String]) -> Result<Vec<String>> {
        let database = self.qap_database(config)?;
        let relative_root = self
            .qap_relative_data_root
            .as_deref()
            .context("AKS run-scoped data root is missing")?;
        let (manifest, inventory_bindings) = campaign_qap_manifest(run_id, &database, relative_root, tables)?;
        let runtime_dir = self
            .runtime_dir
            .as_deref()
            .context("AKS runtime directory is not available")?;
        let manifest_path = runtime_dir.join("oat-qap-manifest.json");
        let record_path = runtime_dir.join("oat-qap-record.json");
        if config.campaign_profile == Some(crate::config::CampaignProfile::SingleHotTable) {
            let saved_manifest: Value = serde_json::from_slice(
                &tokio::fs::read(&manifest_path)
                    .await
                    .context("read pre-admission QAP manifest")?,
            )
            .context("parse pre-admission QAP manifest")?;
            if saved_manifest != manifest {
                bail!("QAP manifest changed after its exact planned bindings were persisted");
            }
            let inventory = campaign_inventory_for_setup(self).await?;
            let qap = inventory
                .qap
                .as_ref()
                .context("campaign QAP bindings were not planned before admission")?;
            if qap.database != database
                || qap.bindings != inventory_bindings
                || qap.state != InventoryResourceState::Planned
                || qap.reference.is_some()
            {
                bail!("campaign inventory does not contain the exact planned QAP manifest");
            }
            let mut uncertain = qap.clone();
            uncertain.state = InventoryResourceState::Uncertain;
            update_campaign_qap_inventory(self, uncertain).await?;
        } else {
            tokio::fs::write(&manifest_path, serde_json::to_vec(&manifest)?)
                .await
                .with_context(|| format!("write run-owned AKS QAP manifest {}", manifest_path.display()))?;
        }
        self.qap_record_path = Some(record_path.clone());
        self.qap_bind_attempted = true;
        let bootstrap_env_file = configured_bootstrap_env_file(config)?;
        let workload_record = self
            .fic_ready_snapshot_path
            .as_deref()
            .context("AKS QAP binding requires the verified run FIC ownership record")?;
        run_oat_qap_helper(
            "bind-oat-qap",
            run_id,
            Some(&manifest_path),
            Some(workload_record),
            None,
            &record_path,
            bootstrap_env_file,
        )
        .await
        .context("bind run-owned AKS QAP external tables")?;
        require_nonempty_record(&record_path).await?;
        run_oat_qap_helper(
            "verify-oat-qap",
            run_id,
            None,
            None,
            None,
            &record_path,
            bootstrap_env_file,
        )
        .await
        .context("verify run-owned AKS QAP bindings")?;
        if config.campaign_profile == Some(crate::config::CampaignProfile::SingleHotTable) {
            let reference = run_oat_qap_reference(&record_path, bootstrap_env_file).await?;
            let mut observed = campaign_inventory_for_setup(self)
                .await?
                .qap
                .context("campaign QAP bindings disappeared after verification")?;
            observed.state = InventoryResourceState::Observed;
            observed.reference = Some(reference);
            update_campaign_qap_inventory(self, observed).await?;
        }
        Ok(inventory_bindings
            .into_iter()
            .map(|binding| binding.external_table)
            .collect())
    }

    async fn prepare_campaign_load_admission(
        &mut self,
        config: &OatConfig,
        run_id: &str,
        tables: &[String],
    ) -> Result<()> {
        if config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
            return verify_maintenance_stop_budget_before_admission(config, self).await;
        }
        if config.campaign_profile != Some(crate::config::CampaignProfile::SingleHotTable) {
            return Ok(());
        }
        let [hot_table] = tables else {
            bail!("campaign load admission requires exactly one frozen hot table");
        };
        let database = self.qap_database(config)?;
        let relative_root = self
            .qap_relative_data_root
            .as_deref()
            .context("AKS run-scoped data root is missing")?;
        let (manifest, bindings) = campaign_qap_manifest(run_id, &database, relative_root, tables)?;
        let hot_path = aks_qap_relative_path(relative_root, hot_table)?;
        let matching_bindings = bindings
            .iter()
            .filter(|binding| binding.relative_path == hot_path)
            .count();
        if matching_bindings != 1 {
            bail!("campaign hot Delta path must match exactly one persisted QAP binding");
        }
        let runtime_dir = self
            .runtime_dir
            .as_deref()
            .context("campaign runtime directory is not available")?;
        let manifest_path = runtime_dir.join("oat-qap-manifest.json");
        tokio::fs::write(&manifest_path, serde_json::to_vec(&manifest)?)
            .await
            .with_context(|| format!("write pre-admission AKS QAP manifest {}", manifest_path.display()))?;
        let mut inventory = campaign_inventory_for_setup(self).await?;
        if inventory.load_admission != InventoryLoadAdmission::NotAuthorized
            || inventory.namespace.uid.is_none()
            || inventory.qap.is_some()
        {
            bail!("campaign load admission is not in its initial pre-provisioned state");
        }
        update_campaign_qap_inventory(
            self,
            InventoryQap {
                database,
                bindings: bindings.clone(),
                state: InventoryResourceState::Planned,
                absence: Vec::new(),
                reference: None,
            },
        )
        .await?;
        inventory = campaign_inventory_for_setup(self).await?;
        if inventory.load_admission != InventoryLoadAdmission::NotAuthorized {
            bail!("campaign baseline can only be captured before load authorization");
        }
        let expected_etag = tokio::fs::read_to_string(runtime_dir.join("campaign-inventory-etag"))
            .await
            .context("read current campaign inventory ETag before baseline capture")?;
        validate_campaign_etag(expected_etag.trim())?;
        let identity = CampaignIdentity::from_lookup(|name| std::env::var(name).ok())?;
        let actor = CampaignCleanupActor {
            mode: CampaignActorMode::Producer,
            owner_id: identity.owner_id.clone(),
            fencing_token: identity.fencing_token,
        };
        let request = CampaignCellBaselineRequest {
            schema_version: 1,
            campaign_id: &identity.campaign_id,
            orchestration_run_id: &inventory.orchestration_run_id,
            cell_run_id: run_id,
            expected_etag: expected_etag.trim(),
            hot_delta_relative_path: &hot_path,
            actor: &actor,
        };
        let output =
            run_campaign_custody_json(self, "capture-cell-baseline", None, &request, Duration::from_secs(90)).await?;
        let response: CampaignCellBaselineResponse =
            serde_json::from_slice(&output).context("parse immutable cell baseline receipt")?;
        if !response.ok {
            let code = response
                .code
                .as_deref()
                .filter(|code| {
                    matches!(
                        *code,
                        "campaign_fenced" | "stale_version" | "write_outcome_unknown" | "invalid_or_storage_error"
                    )
                })
                .context("cell baseline failure returned no recognized error code")?;
            if response.message.is_none() {
                bail!("cell baseline failure omitted its required diagnostic message");
            }
            bail!("cell baseline capture failed: {code}");
        }
        if response.code.is_some() || response.message.is_some() {
            bail!("successful cell baseline response included error fields");
        }
        let receipt = response.receipt.context("cell baseline capture returned no receipt")?;
        validate_campaign_cell_baseline(&receipt, &inventory, &identity, run_id, &hot_path)?;
        tokio::fs::write(
            runtime_dir.join("campaign-cell-baseline.json"),
            serde_json::to_vec(&receipt)?,
        )
        .await
        .context("persist immutable cell baseline receipt")?;

        let mut pending = inventory.clone();
        pending.load_admission = InventoryLoadAdmission::AuthorizationPending;
        inventory.validate_successor(&pending)?;
        persist_campaign_inventory(self, &pending).await?;
        let (confirmed, etag) = read_authoritative_campaign_inventory(
            self,
            &identity,
            &inventory.orchestration_run_id,
            Duration::from_secs(60),
        )
        .await?;
        if confirmed.load_admission != InventoryLoadAdmission::AuthorizationPending
            || confirmed
                .qap
                .as_ref()
                .is_none_or(|qap| qap.state != InventoryResourceState::Planned || qap.bindings != bindings)
        {
            bail!("authoritative campaign inventory did not confirm the pending load authorization");
        }
        pending.validate_successor(&confirmed)?;
        persist_campaign_inventory_snapshot(self, &confirmed, &etag).await?;
        Ok(())
    }

    async fn finalize_campaign_load_admission(
        &mut self,
        config: &OatConfig,
        run_id: &str,
        tables: &[String],
    ) -> Result<()> {
        if config.campaign_profile != Some(crate::config::CampaignProfile::SingleHotTable) {
            return Ok(());
        }
        let [_hot_table] = tables else {
            bail!("campaign load admission requires exactly one frozen hot table");
        };
        let database = self.qap_database(config)?;
        let relative_root = self
            .qap_relative_data_root
            .as_deref()
            .context("AKS run-scoped data root is missing")?;
        let (_, expected_bindings) = campaign_qap_manifest(run_id, &database, relative_root, tables)?;
        let identity = CampaignIdentity::from_lookup(|name| std::env::var(name).ok())?;
        let cached = campaign_inventory_for_setup(self).await?;
        let orchestration_run_id = cached.orchestration_run_id.clone();
        let (pending, _) =
            read_authoritative_campaign_inventory(self, &identity, &orchestration_run_id, Duration::from_secs(60))
                .await?;
        validate_campaign_inventory_load_admission(
            &pending,
            InventoryLoadAdmission::AuthorizationPending,
            &database,
            &expected_bindings,
        )?;
        let mut authorized = pending.clone();
        authorized.load_admission = InventoryLoadAdmission::Authorized;
        pending.validate_successor(&authorized)?;
        persist_campaign_inventory(self, &authorized).await?;
        let (final_inventory, final_etag) =
            read_authoritative_campaign_inventory(self, &identity, &orchestration_run_id, Duration::from_secs(60))
                .await?;
        if final_inventory.load_admission != InventoryLoadAdmission::Authorized {
            bail!("authoritative campaign inventory did not positively confirm load authorization");
        }
        validate_campaign_inventory_load_admission(
            &final_inventory,
            InventoryLoadAdmission::Authorized,
            &database,
            &expected_bindings,
        )?;
        authorized.validate_successor(&final_inventory)?;
        persist_campaign_inventory_snapshot(self, &final_inventory, &final_etag).await?;
        final_inventory.assert_custody_ready(self.campaign_cli_paths()?).await?;
        Ok(())
    }

    async fn publish_official_measurement(
        &mut self,
        config: &OatConfig,
        rows: &[OatResult],
        submitted: Option<&SubmittedConfigIdentity>,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        publish_campaign_official_measurement(self, config, rows, submitted, deadline).await
    }

    fn mark_qap_outcome_unresolved(&mut self) {
        self.qap_outcome_unresolved = true;
    }

    async fn prepare_client_fleet(&mut self, config: &OatConfig, _run_id: &str) -> Result<()> {
        let mut configmap = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": self.schedule_configmap_name,
                "namespace": config.namespace,
                "labels": {
                    "app": "arrowcol-oat-schedule",
                    "arrowcol-oat-run": self.run_id.chars()
                        .filter(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
                        .take(8)
                        .collect::<String>(),
                },
            },
            "data": { "schedule": "pending" },
        });
        prepare_run_owned_resources(config, self, std::slice::from_mut(&mut configmap)).await?;
        let payload = serde_json::to_vec(&configmap)?;
        kubectl(config, vec!["create", "-f", "-"], Some(payload))
            .await
            .context("create unique AKS client schedule ConfigMap")?;
        record_applied_resource_uids(config, self, std::slice::from_ref(&configmap)).await
    }

    async fn start_client(
        &mut self,
        config: &OatConfig,
        run_id: &str,
        table: &str,
        client_index: usize,
        args: &[String],
    ) -> Result<ClientHandle> {
        start_aks_client_job(config, self, run_id, table, client_index, args).await
    }

    async fn poll_client(&self, config: &OatConfig, handle: &ClientHandle) -> Result<ClientProgress> {
        poll_aks_client_job(config, handle).await
    }

    async fn poll_client_fleet(&self, config: &OatConfig, handles: &[ClientHandle]) -> Result<Vec<ClientProgress>> {
        poll_aks_client_fleet(config, self, handles).await
    }

    async fn arm_clients(&mut self, config: &OatConfig, _run_id: &str, run_start_unix_ms: u64) -> Result<()> {
        self.cpu_throttling_start = Some(capture_aks_cpu_throttling(config, &self.resource_readback).await?);
        let mut configmap = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": self.schedule_configmap_name,
                "namespace": config.namespace,
                "labels": {
                    "app": "arrowcol-oat-schedule",
                    "arrowcol-oat-run": self.run_id.chars()
                        .filter(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
                        .take(8)
                        .collect::<String>(),
                },
            },
            "data": { "schedule": run_start_unix_ms.to_string() },
        });
        prepare_run_owned_resources(config, self, std::slice::from_mut(&mut configmap)).await?;
        let payload = serde_json::to_vec(&configmap)?;
        kubectl(
            config,
            vec!["apply", "--namespace", &config.namespace, "-f", "-"],
            Some(payload),
        )
        .await
        .context("arm the common AKS client measurement schedule")?;
        record_applied_resource_uids(config, self, std::slice::from_ref(&configmap)).await
    }

    async fn finish_client(&self, config: &OatConfig, handle: ClientHandle) -> Result<String> {
        finish_aks_client_job(config, handle).await
    }

    async fn cancel_client(&self, config: &OatConfig, handle: &ClientHandle) -> Result<()> {
        cancel_aks_client_job(config, handle).await
    }

    fn resource_sampler_source(&self) -> Option<ResourceSamplerSource> {
        Some(ResourceSamplerSource::Aks)
    }

    async fn native_telemetry_source(
        &self,
        config: &OatConfig,
        qap_tables: &[String],
    ) -> Result<Option<NativeTelemetrySource>> {
        if self.campaign_cli_paths.is_none() {
            return Ok(None);
        }
        anyhow::ensure!(
            FIXTURE_BOUNDARY.try_with(|_| ()).is_err(),
            "fixture boundary has no authenticated native telemetry transport"
        );
        let inventory = campaign_inventory_for_setup(self).await?;
        let plan = inventory
            .fic
            .as_ref()
            .and_then(|fic| fic.plan.as_ref())
            .context("native telemetry has no exact API-server binding")?;
        let database = self.qap_database(config)?;
        let qap = inventory
            .qap
            .as_ref()
            .context("native telemetry has no admitted QAP table bindings")?;
        let expected = qap
            .bindings
            .iter()
            .map(|binding| binding.external_table.clone())
            .collect::<BTreeSet<_>>();
        anyhow::ensure!(
            inventory.load_admission == InventoryLoadAdmission::Authorized
                && inventory.namespace.name == config.namespace
                && qap.database == database
                && expected == qap_tables.iter().cloned().collect()
                && expected.len() == qap_tables.len()
                && !expected.is_empty(),
            "native telemetry differs from the admitted original cell and QAP tables"
        );
        let targets = aks_resource_pod_targets(&self.resource_readback)?;
        anyhow::ensure!(
            !targets.is_empty() && targets.len() <= 64,
            "native telemetry pod identity set is unavailable or oversized"
        );
        Ok(Some(NativeTelemetrySource {
            server: plan.server.clone(),
            context: config.kubectl_context.clone(),
            targets,
            qap_endpoint: self.qap_endpoint(config),
            qap_database: database,
            qap_tables: qap_tables.to_vec(),
            results: self
                .runtime_dir
                .as_ref()
                .context("native telemetry runtime directory unavailable")?
                .join("measurement-qap"),
            binding: json!({
                "campaign_id": inventory.campaign_id, "owner_id": inventory.owner_id,
                "fencing_token": inventory.fencing_token, "cell_run_id": inventory.cell_run_id,
                "namespace_uid": inventory.namespace.uid, "namespace": inventory.namespace.name,
                "server": plan.server, "kube_context": config.kubectl_context,
                "qap_database": qap.database, "qap_bindings": qap.bindings,
                "qap_reference": qap.reference, "qap_endpoint": self.qap_endpoint(config),
            }),
        }))
    }

    async fn cleanup(&mut self, config: &OatConfig) -> Result<()> {
        let result = cleanup_aks_platform(config, self).await;
        if self.shutdown_drain_evidence.is_none() {
            self.shutdown_drain_evidence =
                Some(json!({"status":"unavailable", "reason":"ordered_aks_shutdown_not_completed"}));
        }
        result
    }

    fn shutdown_drain_evidence(&self) -> Option<Value> {
        self.shutdown_drain_evidence.clone()
    }
}

#[async_trait::async_trait]
impl Platform for LocalPlatform {
    fn retain_cell_process_baseline(&mut self, logs: &[ProcessLog]) {
        self.maintenance_cell_baseline = logs
            .iter()
            .filter(|log| log.role == ComponentRole::Maintenance)
            .cloned()
            .collect();
    }

    fn kind(&self) -> crate::config::OatMode {
        crate::config::OatMode::Local
    }

    async fn process_logs(&self, config: &OatConfig) -> Result<Vec<ProcessLog>> {
        local_process_logs(config, self).await
    }

    async fn process_logs_for_role(&self, config: &OatConfig, role: ComponentRole) -> Result<Vec<ProcessLog>> {
        local_process_logs_for_role(config, self, Some(role)).await
    }

    fn workload_endpoint(&self, _config: &OatConfig) -> String {
        "http://router:50050".to_owned()
    }

    fn router_metrics_endpoint(&self, _config: &OatConfig) -> Result<String> {
        self.router_metrics_url
            .clone()
            .context("local router metrics endpoint is not ready")
    }

    fn qap_endpoint(&self, config: &OatConfig) -> String {
        self.qap_url.clone().unwrap_or_else(|| config.qap_endpoint.clone())
    }

    async fn ensure_stack(&mut self, config: &OatConfig, _orchestration_run_id: &str, _replicas: u32) -> Result<()> {
        self.daemon_endpoint = Some(local_docker_daemon().await?);
        prepare_local_platform(config, self).await?;
        self.refresh_endpoints(config).await
    }

    async fn scale_arrowcols(&mut self, config: &OatConfig, replicas: u32, concurrency: u32) -> Result<()> {
        scale_local_arrowcols(config, self, replicas, concurrency).await?;
        self.refresh_endpoints(config).await
    }

    async fn initialize_run_table_pins(
        &mut self,
        config: &OatConfig,
        run_id: &str,
        expected_keys_json: &str,
        expires_at_unix_ms: u64,
        now_unix_ms: u64,
    ) -> Result<()> {
        run_local_pin_setup(
            config,
            self,
            run_id,
            expected_keys_json,
            expires_at_unix_ms,
            now_unix_ms,
        )
        .await?;
        start_local_maintenance(config, self).await
    }

    async fn prepare_campaign_load_admission(
        &mut self,
        config: &OatConfig,
        _run_id: &str,
        tables: &[String],
    ) -> Result<()> {
        verify_maintenance_stop_budget_before_admission(config, self).await?;
        verify_local_partition_policy_before_admission(config, self, tables).await
    }

    fn qap_observation_mode(&self) -> arrowcol_loadgen::ObservationMode {
        arrowcol_loadgen::ObservationMode::Local
    }

    fn qap_database(&self, config: &OatConfig) -> Result<String> {
        Ok(config.qap_database.clone().unwrap_or_else(|| "loadtest".to_owned()))
    }

    async fn initialize_qap(&mut self, config: &OatConfig, run_id: &str, tables: &[String]) -> Result<Vec<String>> {
        let bindings = qap_bindings(run_id, tables)?;
        let local_bindings = bindings
            .iter()
            .map(|binding| {
                Ok((
                    binding.external_table.clone(),
                    format!("/delta/dataflow/telemetry/sqlserver/{}", binding.logical_table),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let results = std::path::PathBuf::from("out/arrowcol-oat/qap").join(run_id);
        std::fs::create_dir_all(&results)
            .with_context(|| format!("create local QAP result directory {}", results.display()))?;
        let database = self.qap_database(config)?;
        arrowcol_loadgen::initialize_local_qap(
            self.qap_url.as_deref().context(LocalDownstreamUnavailable)?,
            &results,
            &database,
            &local_bindings,
            Duration::from_secs(config.qap_timeout_seconds),
        )
        .await
        .map_err(classify_local_qap_error)
        .context("initialize run-local Kusto QAP tables and policies")?;
        Ok(bindings.iter().map(|binding| binding.external_table.clone()).collect())
    }

    async fn prepare_client_fleet(&mut self, _config: &OatConfig, _run_id: &str) -> Result<()> {
        tokio::fs::write(self.control_dir.join("schedule"), "pending\n")
            .await
            .context("reset local OAT client schedule barrier")
    }

    async fn start_client(
        &mut self,
        config: &OatConfig,
        run_id: &str,
        table: &str,
        client_index: usize,
        args: &[String],
    ) -> Result<ClientHandle> {
        start_local_client(config, self, run_id, table, client_index, args).await
    }

    async fn poll_client(&self, _config: &OatConfig, handle: &ClientHandle) -> Result<ClientProgress> {
        poll_local_client(handle).await
    }

    async fn poll_client_fleet(&self, _config: &OatConfig, handles: &[ClientHandle]) -> Result<Vec<ClientProgress>> {
        poll_local_client_fleet(handles, &self.client_progress).await
    }

    async fn arm_clients(&mut self, _config: &OatConfig, _run_id: &str, run_start_unix_ms: u64) -> Result<()> {
        let temporary = self.control_dir.join("schedule.tmp");
        tokio::fs::write(&temporary, format!("{run_start_unix_ms}\n"))
            .await
            .context("write local OAT measurement schedule")?;
        tokio::fs::rename(&temporary, self.control_dir.join("schedule"))
            .await
            .context("atomically arm local OAT measurement schedule")
    }

    async fn finish_client(&self, _config: &OatConfig, handle: ClientHandle) -> Result<String> {
        finish_local_client(handle).await
    }

    async fn cancel_client(&self, _config: &OatConfig, handle: &ClientHandle) -> Result<()> {
        cancel_local_client(handle).await
    }

    fn resource_sampler_source(&self) -> Option<ResourceSamplerSource> {
        Some(ResourceSamplerSource::Local {
            run_id: self.run_id.clone(),
            environment: self.environment.clone(),
            collector_metrics_url: self.collector_metrics_url.clone(),
        })
    }

    async fn cleanup(&mut self, config: &OatConfig) -> Result<()> {
        match cleanup_local_platform(config, self).await {
            Ok(evidence) => {
                self.shutdown_drain_evidence = Some(evidence);
                Ok(())
            }
            Err(error) => {
                self.shutdown_drain_evidence = Some(json!({"status":"unavailable", "reason":"local_cleanup_failed"}));
                Err(error)
            }
        }
    }

    fn shutdown_drain_evidence(&self) -> Option<Value> {
        self.shutdown_drain_evidence.clone()
    }
}

impl LocalPlatform {
    async fn refresh_endpoints(&mut self, config: &OatConfig) -> Result<()> {
        self.router_metrics_url = None;
        self.collector_metrics_url = None;
        self.qap_url = None;
        self.router_metrics_url = Some(local_compose_endpoint(config, self, "router", "50050", "/metrics").await?);
        match local_compose_endpoint(config, self, "dataflow-1", "32011", "/metrics").await {
            Ok(endpoint) => self.collector_metrics_url = Some(endpoint),
            Err(error) => {
                tracing::warn!(%error, "local collector metrics endpoint unavailable; publication backlog evidence remains unknown");
            }
        }
        match local_compose_endpoint(config, self, "kusto", "8080", "").await {
            Ok(endpoint) => self.qap_url = Some(endpoint),
            Err(error) => {
                tracing::warn!(%error, "local downstream unavailable; collector-only measurement remains enabled")
            }
        }
        Ok(())
    }

    fn new(config: &OatConfig, run_id: &str) -> Result<Self> {
        let state_root = config
            .platform
            .compose_state_root
            .as_deref()
            .context("local compose state root is missing")?;
        let state_dir = std::env::current_dir()?.join(state_root).join(run_id);
        crate::local::absolute_directory(&state_dir)?;
        if let Some(destination) = &config.platform.local.snapshot_root {
            let destination = Path::new(destination);
            crate::local::absolute_directory(destination)?;
            if destination.starts_with(&state_dir) || state_dir.starts_with(destination) {
                bail!("snapshot destination must be separate from generated state");
            }
        }
        if state_dir.exists() {
            bail!(
                "run-scoped local OAT state directory already exists: {}",
                state_dir.display()
            );
        }
        fs::create_dir_all(state_dir.parent().context("local OAT state directory has no parent")?)?;
        fs::create_dir(&state_dir)
            .with_context(|| format!("create isolated local OAT state directory {}", state_dir.display()))?;
        let environment = match prepare_local_state(config, &state_dir, run_id) {
            Ok(environment) => environment,
            Err(error) => {
                return match fs::remove_dir_all(&state_dir) {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "removing incomplete generated state also failed: {cleanup_error}"
                    ))),
                };
            }
        };
        let control_dir = state_dir.join("control");
        Ok(Self {
            run_id: run_id.to_owned(),
            state_dir,
            environment,
            control_dir,
            client_progress: std::sync::Mutex::new(HashMap::new()),
            router_metrics_url: None,
            collector_metrics_url: None,
            qap_url: None,
            daemon_endpoint: None,
            shutdown_drain_evidence: None,
            maintenance_cell_baseline: Vec::new(),
        })
    }

    fn add_compose_environment(&self, command: &mut Command) {
        command.envs(&self.environment);
        command.arg("--file").arg(self.state_dir.join("tunables.compose.json"));
    }
}

fn prepare_local_state(config: &OatConfig, state_dir: &Path, run_id: &str) -> Result<HashMap<String, String>> {
    let mut overlay = compose_tunables(config)?;
    local_dataflow_overlay(config, state_dir, &mut overlay)?;
    fs::write(state_dir.join("tunables.compose.json"), serde_json::to_vec(&overlay)?)
        .context("write typed run-owned Compose tunable overlay")?;
    for directory in ["catalog-state", "dataflow", "router-state", "control"] {
        fs::create_dir(state_dir.join(directory))
            .with_context(|| format!("create local OAT state directory {directory}"))?;
    }
    if config.tunables.wal.is_some() {
        fs::create_dir(state_dir.join("quiver")).context("create run-owned per-instance WAL state")?;
    }
    fs::write(state_dir.join("control/schedule"), "pending\n")
        .context("initialize local OAT client schedule barrier")?;
    fs::create_dir(state_dir.join("secrets"))?;
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let compose_path = std::env::current_dir()?.join(compose_file);
    let router_config = compose_path
        .parent()
        .context("local Compose file has no parent directory")?
        .join("config/router.yaml");
    let router_text = fs::read_to_string(&router_config)
        .with_context(|| format!("read local router config from {}", router_config.display()))?;
    let router_text = run_scoped_local_router_config(&router_text, run_id, config.run_pin_ttl_ms)?;
    fs::write(state_dir.join("router-state/config.yaml"), router_text)
        .context("write run-scoped local OAT router config")?;

    let workspace = std::env::current_dir()?;
    let (uid, gid) = local_owner_ids(&workspace)?;

    let secret_paths = [
        ("SQLSERVER_SA_PASSWORD_FILE", "sqlserver-sa-password"),
        ("ARROWCOL_APP_PASSWORD_FILE", "arrowcol-app-password"),
        ("ARROWCOL_MIGRATOR_PASSWORD_FILE", "arrowcol-migrator-password"),
        ("LOADTEST_EDGE_HMAC_FILE", "edge-hmac"),
        ("LOADTEST_INTERNAL_HMAC_FILE", "internal-hmac"),
    ];
    let mut environment = HashMap::from([
        ("LOADTEST_UID".to_owned(), uid),
        ("LOADTEST_GID".to_owned(), gid),
        (
            "LOADTEST_WORKSPACE".to_owned(),
            workspace.to_string_lossy().into_owned(),
        ),
        (
            "LOADTEST_BIN_HOST_PATH".to_owned(),
            workspace.to_string_lossy().into_owned(),
        ),
        ("ONELAKE_HOST_PATH".to_owned(), state_dir.to_string_lossy().into_owned()),
        ("SQLSERVER_PORT".to_owned(), "0".to_owned()),
        ("LOADTEST_CATALOG_PORT".to_owned(), "0".to_owned()),
        ("GRAFANA_PORT".to_owned(), "0".to_owned()),
        ("LOADTEST_ROUTER_PORT".to_owned(), "0".to_owned()),
        ("KUSTO_PORT".to_owned(), "0".to_owned()),
        ("LOADTEST_DATAFLOW_DELTA_ROOT".to_owned(), "/delta/dataflow".to_owned()),
        ("LOADTEST_TRACES_ENDPOINT".to_owned(), "http://lgtm:4318".to_owned()),
        (
            "LOADTEST_BIND_HOST".to_owned(),
            match config.platform.local.bind_host {
                Some(std::net::IpAddr::V6(host)) => format!("[{host}]"),
                Some(host) => host.to_string(),
                None => "127.0.0.1".to_owned(),
            },
        ),
    ]);
    environment.extend(local_control_mount_environment(state_dir));
    for (variable, name) in secret_paths {
        let path = state_dir.join("secrets").join(name);
        write_private_secret(&path, &random_secret())?;
        environment.insert(variable.to_owned(), path.to_string_lossy().into_owned());
    }
    Ok(environment)
}

fn local_control_mount_environment(state_dir: &Path) -> HashMap<String, String> {
    HashMap::from([(
        "OAT_CONTROL_HOST_PATH".to_owned(),
        state_dir.join("control").to_string_lossy().into_owned(),
    )])
}

fn local_client_compose_args(run_id: &str, container_name: &str, client_args: &[String]) -> Vec<String> {
    let mut args = vec![
        "run".to_owned(),
        "--detach".to_owned(),
        "--no-deps".to_owned(),
        "-T".to_owned(),
        "--name".to_owned(),
        container_name.to_owned(),
        "--env".to_owned(),
        format!("ARROWCOL_OAT_RUN_ID={run_id}"),
        "oat-client".to_owned(),
    ];
    args.extend_from_slice(client_args);
    args
}

async fn start_local_client(
    config: &OatConfig,
    platform: &LocalPlatform,
    run_id: &str,
    table: &str,
    client_index: usize,
    args: &[String],
) -> Result<ClientHandle> {
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let compact_run_id: String = run_id.chars().filter(char::is_ascii_alphanumeric).take(12).collect();
    let safe_table: String = table
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let container_name = format!("oat-client-{compact_run_id}-{safe_table}-c{client_index}");
    let mut command = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "oat",
    );
    platform.add_compose_environment(&mut command);
    command.args(local_client_compose_args(run_id, &container_name, args));
    let output = run_command(command, "start local OAT client container").await?;
    let container_id = String::from_utf8(output.stdout)
        .context("local OAT client container ID was not UTF-8")?
        .trim()
        .to_owned();
    if container_id.is_empty() {
        bail!("Compose returned an empty local OAT client container ID");
    }
    Ok(ClientHandle {
        client_id: format!("{run_id}:{table}:{client_index}"),
        table: table.to_owned(),
        process: ClientProcess::Local { container_id },
    })
}

async fn poll_local_client(handle: &ClientHandle) -> Result<ClientProgress> {
    let ClientProcess::Local { container_id } = &handle.process else {
        bail!("local client poll received a non-local process handle");
    };
    let inspect = run_command(
        {
            let mut command = Command::new("docker");
            command.args([
                "inspect",
                "--format",
                "{{.State.Status}}|{{.State.ExitCode}}",
                container_id,
            ]);
            command
        },
        "inspect local OAT client container",
    )
    .await?;
    let state = String::from_utf8(inspect.stdout).context("local OAT container state was not UTF-8")?;
    let (state, exit_code) = state
        .trim()
        .split_once('|')
        .context("parse local OAT container state")?;
    let exit_code = exit_code
        .parse::<i32>()
        .context("parse local OAT container exit code")?;
    let output = run_command(
        {
            let mut command = Command::new("docker");
            command.args(["logs", container_id]);
            command
        },
        "read local OAT client logs",
    )
    .await?;
    let mut logs = String::from_utf8_lossy(&output.stdout).into_owned();
    logs.push_str(&String::from_utf8_lossy(&output.stderr));
    let completed = state == "exited";
    let failure = (completed && exit_code != 0)
        .then(|| format!("local DMV client {} exited {exit_code}: {logs}", handle.client_id));
    Ok(ClientProgress {
        ready: logs.contains("OAT_CLIENT_READY "),
        schedule_received: logs.contains("OAT_SCHEDULE_RECEIVED "),
        completed,
        failure,
        output: logs,
    })
}

async fn poll_local_client_fleet(
    handles: &[ClientHandle],
    cache: &std::sync::Mutex<HashMap<String, ClientProgress>>,
) -> Result<Vec<ClientProgress>> {
    let mut inspect = Command::new("docker");
    inspect.args(["inspect", "--format", "{{.Id}}|{{.State.Status}}|{{.State.ExitCode}}"]);
    for handle in handles {
        let ClientProcess::Local { container_id } = &handle.process else {
            bail!("local client fleet poll received a non-local process handle");
        };
        inspect.arg(container_id);
    }
    let inspect = run_command(inspect, "inspect local OAT client fleet").await?;
    let inspect = String::from_utf8(inspect.stdout).context("local OAT client fleet state was not UTF-8")?;
    let mut states = HashMap::new();
    for line in inspect.lines() {
        let mut fields = line.split('|');
        let id = fields.next().context("local OAT inspect row has no container ID")?;
        let status = fields.next().context("local OAT inspect row has no container status")?;
        let exit_code = fields
            .next()
            .context("local OAT inspect row has no exit code")?
            .parse::<i32>()
            .context("parse local OAT container exit code")?;
        states.insert(id.to_owned(), (status.to_owned(), exit_code));
    }
    let previous = {
        let cache = cache
            .lock()
            .map_err(|_| anyhow::anyhow!("local OAT client progress cache is poisoned"))?;
        handles
            .iter()
            .map(|handle| cache.get(&handle.client_id).cloned().unwrap_or_default())
            .collect::<Vec<_>>()
    };
    let log_indexes = handles
        .iter()
        .enumerate()
        .filter_map(|(index, handle)| {
            let ClientProcess::Local { container_id } = &handle.process else {
                return None;
            };
            let exited = states.get(container_id).is_some_and(|(status, _)| status == "exited");
            (exited || !previous[index].ready || !previous[index].schedule_received).then_some(index)
        })
        .collect::<Vec<_>>();
    let log_results = join_all(log_indexes.iter().map(|index| async move {
        let container_id = match &handles[*index].process {
            ClientProcess::Local { container_id } => container_id,
            ClientProcess::Aks { .. } => {
                return (
                    *index,
                    Err(anyhow::anyhow!(
                        "local client fleet poll received a non-local process handle"
                    )),
                );
            }
        };
        let mut command = Command::new("docker");
        command.args(["logs", container_id]);
        let result = run_command(command, "read local OAT client logs").await;
        (*index, result)
    }))
    .await;
    let mut logs_by_index = HashMap::with_capacity(log_results.len());
    for (index, result) in log_results {
        let output = result?;
        let mut logs = String::from_utf8_lossy(&output.stdout).into_owned();
        logs.push_str(&String::from_utf8_lossy(&output.stderr));
        logs_by_index.insert(index, logs);
    }
    let mut progress = Vec::with_capacity(handles.len());
    for (index, handle) in handles.iter().enumerate() {
        let ClientProcess::Local { container_id } = &handle.process else {
            bail!("local client fleet poll received a non-local process handle");
        };
        let (status, exit_code) = states
            .iter()
            .find(|(id, _)| id.starts_with(container_id.as_str()) || container_id.starts_with(id.as_str()))
            .map(|(_, state)| state)
            .with_context(|| format!("local OAT client {} is missing from docker inspect", handle.client_id))?;
        let output = logs_by_index
            .remove(&index)
            .unwrap_or_else(|| previous[index].output.clone());
        let completed = status == "exited";
        let current = ClientProgress {
            ready: previous[index].ready || output.contains("OAT_CLIENT_READY "),
            schedule_received: previous[index].schedule_received || output.contains("OAT_SCHEDULE_RECEIVED "),
            completed,
            failure: (completed && *exit_code != 0)
                .then(|| format!("local DMV client {} exited {exit_code}: {output}", handle.client_id)),
            output,
        };
        progress.push(current.clone());
        cache
            .lock()
            .map_err(|_| anyhow::anyhow!("local OAT client progress cache is poisoned"))?
            .insert(handle.client_id.clone(), current);
    }
    Ok(progress)
}

async fn finish_local_client(handle: ClientHandle) -> Result<String> {
    let ClientProcess::Local { container_id } = &handle.process else {
        bail!("local client finish received a non-local process handle");
    };
    let progress = poll_local_client(&handle).await?;
    if let Some(failure) = progress.failure {
        bail!("{failure}");
    }
    if !progress.completed {
        bail!(
            "local OAT client container {} was not complete at finish",
            handle.client_id
        );
    }
    let mut remove = Command::new("docker");
    let provenance = local_client_image_evidence(container_id).await?;
    remove.args(["rm", container_id]);
    run_command(remove, "remove completed local OAT client container").await?;
    Ok(format!(
        "{}\nOAT_CLIENT_IMAGE_PROVENANCE {provenance}\n",
        progress.output
    ))
}

async fn cancel_local_client(handle: &ClientHandle) -> Result<()> {
    let ClientProcess::Local { container_id } = &handle.process else {
        bail!("local client cancel received a non-local process handle");
    };
    let mut remove = Command::new("docker");
    remove.args(["rm", "--force", container_id]);
    run_command(remove, "cancel local OAT client container").await?;
    Ok(())
}

fn run_scoped_local_router_config(router_text: &str, run_id: &str, run_pin_ttl_ms: u64) -> Result<String> {
    let lease_ttl_ms = run_pin_ttl_ms.max(3_600_000);
    let router_text = router_text.replacen("lease_ttl_ms: 86400000", &format!("lease_ttl_ms: {lease_ttl_ms}"), 1);
    if router_text.matches(&format!("lease_ttl_ms: {lease_ttl_ms}")).count() != 1
        || router_text.contains("oat_run_id:")
        || router_text.contains("oat_run_pin_ttl_ms:")
    {
        bail!("local router config did not contain its expected one-day lease TTL");
    }
    let run_id_yaml = serde_json::to_string(run_id)?;
    Ok(router_text.replacen(
        &format!("lease_ttl_ms: {lease_ttl_ms}"),
        &format!("lease_ttl_ms: {lease_ttl_ms}\n  oat_run_pin_ttl_ms: {run_pin_ttl_ms}\n  oat_run_id: {run_id_yaml}"),
        1,
    ))
}

#[cfg(unix)]
fn local_owner_ids(workspace: &Path) -> Result<(String, String)> {
    use std::os::unix::fs::MetadataExt;
    let owner = fs::metadata(workspace).context("inspect local workspace ownership")?;
    Ok((owner.uid().to_string(), owner.gid().to_string()))
}

#[cfg(not(unix))]
fn local_owner_ids(_workspace: &Path) -> Result<(String, String)> {
    bail!("local OAT Compose mode requires a Unix devcontainer")
}

fn random_secret() -> String {
    format!(
        "Aa1!{}",
        (0..3)
            .map(|_| uuid::Uuid::new_v4().simple().to_string())
            .collect::<String>()
    )
}

fn write_private_secret(path: &Path, contents: &str) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("create private local OAT secret {}", path.display()))?;
        file.write_all(contents.as_bytes())
            .with_context(|| format!("write local OAT secret {}", path.display()))?;
        file.write_all(b"\n")
            .with_context(|| format!("finish local OAT secret {}", path.display()))?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (path, contents);
        bail!("local OAT secrets require a Unix devcontainer")
    }
}

async fn start_aks_client_job(
    config: &OatConfig,
    platform: &mut AksPlatform,
    run_id: &str,
    table: &str,
    client_index: usize,
    args: &[String],
) -> Result<ClientHandle> {
    let run_label = run_id
        .chars()
        .filter(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
        .take(8)
        .collect::<String>();
    let table_label = table.to_ascii_lowercase().replace('_', "-");
    let job_name = format!("oat-{run_label}-{table_label}-c{client_index}");
    let run_seconds = i64::try_from(config.timing_budget()?.client_job_lifetime_seconds)
        .context("OAT client Job lifetime exceeds Kubernetes range")?;
    let mut job = build_aks_client_job(AksClientJobSpec {
        namespace: &config.namespace,
        client_image: &config.dmv_client_image,
        router_auth_secret_name: &config.edge_secret_name,
        schedule_configmap_name: &platform.schedule_configmap_name,
        run_id,
        run_label: &run_label,
        job_name: &job_name,
        args,
        run_seconds,
    });
    let client_resources = config.effective_aks_resources().client;
    configure_client_job_resources(&mut job, &client_resources)?;
    inject_container_environment(
        job.pointer_mut("/spec/template/spec/containers/0")
            .context("AKS client container is missing")?,
        &role_environment(config, ComponentRole::Client)?,
    )?;
    prepare_run_owned_resources(config, platform, std::slice::from_mut(&mut job)).await?;
    let payload = serde_json::to_vec(&job)?;
    kubectl(
        config,
        vec!["apply", "--namespace", &config.namespace, "-f", "-"],
        Some(payload),
    )
    .await
    .with_context(|| format!("create AKS DMV client Job {job_name}"))?;
    record_applied_resource_uids(config, platform, std::slice::from_ref(&job)).await?;
    let applied_job_json = kubectl(
        config,
        vec!["get", "job", &job_name, "--namespace", &config.namespace, "-o", "json"],
        None,
    )
    .await
    .context("read applied AKS client Job resource spec")?;
    let applied_job: Value = serde_json::from_str(&applied_job_json).context("parse applied AKS client Job")?;
    let job_containers = applied_job
        .pointer("/spec/template/spec/containers")
        .and_then(Value::as_array)
        .context("applied AKS client Job has no containers")?;
    let job_resources = verify_container_resources(job_containers, "client", "client", &client_resources)?;
    let pod = wait_for_aks_client_pod_resource_readback(config, &job_name, &client_resources).await?;
    platform.resource_readback.push(json!({
        "role": "client",
        "job": job_name,
        "requested": client_resources,
        "effective": {"job_template": job_resources["effective"], "pod": pod["resources"]},
        "scheduled_pod": {"name": pod["name"], "uid": pod["uid"], "node": pod["node"],
            "container": "client", "container_id": pod["container_id"]},
    }));
    Ok(ClientHandle {
        client_id: format!("{run_id}:{table}:{client_index}"),
        table: table.to_owned(),
        process: ClientProcess::Aks { job_name },
    })
}

struct AksClientJobSpec<'a> {
    namespace: &'a str,
    client_image: &'a str,
    router_auth_secret_name: &'a str,
    schedule_configmap_name: &'a str,
    run_id: &'a str,
    run_label: &'a str,
    job_name: &'a str,
    args: &'a [String],
    run_seconds: i64,
}

fn build_aks_client_job(spec: AksClientJobSpec<'_>) -> Value {
    let AksClientJobSpec {
        namespace,
        client_image,
        router_auth_secret_name,
        schedule_configmap_name,
        run_id,
        run_label,
        job_name,
        args,
        run_seconds,
    } = spec;
    let labels = json!({
        "app": "arrowcol-oat-client",
        "arrowcol-oat-run": run_label,
    });
    json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": namespace,
            "labels": labels,
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": run_seconds,
            "template": {
                "metadata": { "labels": labels },
                "spec": {
                    "restartPolicy": "Never",
                    "terminationGracePeriodSeconds": 10,
                    "automountServiceAccountToken": false,
                    "nodeSelector": { "arrow-collector/role": "client" },
                    "tolerations": [{
                        "key": "arrow-collector/role",
                        "operator": "Equal",
                        "value": "client",
                        "effect": "NoSchedule",
                    }],
                    "topologySpreadConstraints": [{
                        "maxSkew": 1,
                        "topologyKey": "kubernetes.azure.com/agentpool",
                        "whenUnsatisfiable": "ScheduleAnyway",
                        "labelSelector": { "matchLabels": labels },
                    }],
                    "containers": [{
                        "name": "client",
                        "image": client_image,
                        "imagePullPolicy": "IfNotPresent",
                        "args": args,
                        "env": [
                            { "name": "ARROWCOL_OAT_RUN_ID", "value": run_id },
                            { "name": "LOADTEST_INTERNAL_SECRET_FILE", "value": "/run/secrets/internal-hmac" },
                        ],
                        "resources": {
                            "requests": { "cpu": "250m", "memory": "256Mi" },
                            "limits": { "cpu": "2", "memory": "2Gi" },
                        },
                        "volumeMounts": [{
                            "name": "edge-hmac",
                            "mountPath": "/run/secrets/edge-hmac",
                            "subPath": "edge-hmac",
                            "readOnly": true,
                        }, {
                            "name": "internal-hmac",
                            "mountPath": "/run/secrets/internal-hmac",
                            "subPath": "internal-hmac",
                            "readOnly": true,
                        }, {
                            "name": "oat-schedule",
                            "mountPath": "/oat-schedule",
                            "readOnly": true,
                        }],
                    }],
                    "volumes": [{
                        "name": "edge-hmac",
                        "secret": {
                            "secretName": router_auth_secret_name,
                            "items": [{ "key": "edge-hmac", "path": "edge-hmac" }],
                        },
                    }, {
                        "name": "internal-hmac",
                        "secret": {
                            "secretName": router_auth_secret_name,
                            "items": [{ "key": "internal-hmac", "path": "internal-hmac" }],
                        },
                    }, {
                        "name": "oat-schedule",
                        "configMap": {
                            "name": schedule_configmap_name,
                            "items": [{ "key": "schedule", "path": "schedule" }],
                        },
                    }],
                },
            },
        },
    })
}

async fn wait_for_aks_client_pod_resource_readback(
    config: &OatConfig,
    job_name: &str,
    expected: &ResourceRequirements,
) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(config.client_ready_timeout_seconds), async {
        loop {
            let pod_json = kubectl(
                config,
                vec![
                    "get",
                    "pods",
                    "--namespace",
                    &config.namespace,
                    "--selector",
                    &format!("job-name={job_name}"),
                    "-o",
                    "json",
                ],
                None,
            )
            .await
            .context("read AKS client pod placement and resource spec")?;
            let pod_list: Value = serde_json::from_str(&pod_json).context("parse AKS client pod list")?;
            let pods = pod_list
                .get("items")
                .and_then(Value::as_array)
                .context("AKS client pod list omitted items")?;
            for pod in pods {
                let phase = pod.pointer("/status/phase").and_then(Value::as_str);
                if phase == Some("Failed") {
                    bail!("AKS client Job pod failed before load authorization");
                }
                let Some(node) = pod
                    .pointer("/spec/nodeName")
                    .and_then(Value::as_str)
                    .filter(|node| !node.is_empty())
                else {
                    continue;
                };
                let metadata = pod.get("metadata").context("AKS client pod metadata is missing")?;
                let containers = pod
                    .pointer("/spec/containers")
                    .and_then(Value::as_array)
                    .context("AKS client pod has no containers")?;
                let resources = verify_container_resources(containers, "client", "client", expected)?;
                return Ok(json!({
                    "name": metadata.get("name"),
                    "uid": metadata.get("uid"),
                    "node": node,
                    "container_id": native_container_id(pod, "client"),
                    "resources": resources["effective"],
                }));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await
    .context("AKS client pod did not schedule before the configured readiness deadline")?
}

async fn poll_aks_client_job(config: &OatConfig, handle: &ClientHandle) -> Result<ClientProgress> {
    let ClientProcess::Aks { job_name } = &handle.process else {
        bail!("AKS client poll received a non-AKS process handle");
    };
    let job = kubectl(
        config,
        vec!["get", "job", job_name, "--namespace", &config.namespace, "-o", "json"],
        None,
    )
    .await
    .with_context(|| format!("inspect AKS DMV client Job {job_name}"))?;
    let job: Value = serde_json::from_str(&job).context("parse AKS DMV client Job status")?;
    let succeeded = job.pointer("/status/succeeded").and_then(Value::as_u64).unwrap_or(0) > 0;
    let failed = job.pointer("/status/failed").and_then(Value::as_u64).unwrap_or(0) > 0
        || job
            .pointer("/status/conditions")
            .and_then(Value::as_array)
            .is_some_and(|conditions| {
                conditions.iter().any(|condition| {
                    condition.get("type").and_then(Value::as_str) == Some("Failed")
                        && condition.get("status").and_then(Value::as_str) == Some("True")
                })
            });
    let logs_result = kubectl(
        config,
        vec!["logs", "--namespace", &config.namespace, &format!("job/{job_name}")],
        None,
    )
    .await;
    let output = match logs_result {
        Ok(output) => output,
        Err(error) if succeeded || failed => {
            return Err(error).with_context(|| format!("read completed AKS client Job {job_name} logs"));
        }
        Err(_) => String::new(),
    };
    let ready = output.contains("OAT_CLIENT_READY ");
    let schedule_received = output.contains("OAT_SCHEDULE_RECEIVED ");
    let failure = failed.then(|| format!("AKS DMV client Job {job_name} failed: {output}"));
    Ok(ClientProgress {
        ready,
        schedule_received,
        completed: succeeded,
        failure,
        output,
    })
}

async fn poll_aks_client_fleet(
    config: &OatConfig,
    platform: &AksPlatform,
    handles: &[ClientHandle],
) -> Result<Vec<ClientProgress>> {
    let run_label: String = platform
        .run_id
        .chars()
        .filter(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
        .take(8)
        .collect();
    let selector = format!("app=arrowcol-oat-client,arrowcol-oat-run={run_label}");
    let jobs_output = kubectl(
        config,
        vec![
            "get",
            "jobs",
            "--namespace",
            &config.namespace,
            "--selector",
            &selector,
            "-o",
            "json",
        ],
        None,
    )
    .await
    .context("inspect AKS OAT client fleet Jobs")?;
    let jobs: Value = serde_json::from_str(&jobs_output).context("parse AKS OAT client fleet Job statuses")?;
    let items = jobs
        .get("items")
        .and_then(Value::as_array)
        .context("AKS OAT client Job list has no items array")?;
    let mut states = HashMap::with_capacity(items.len());
    for job in items {
        let Some(name) = job.pointer("/metadata/name").and_then(Value::as_str) else {
            bail!("AKS OAT client Job has no metadata.name");
        };
        let succeeded = job.pointer("/status/succeeded").and_then(Value::as_u64).unwrap_or(0) > 0;
        let failed = job.pointer("/status/failed").and_then(Value::as_u64).unwrap_or(0) > 0
            || job
                .pointer("/status/conditions")
                .and_then(Value::as_array)
                .is_some_and(|conditions| {
                    conditions.iter().any(|condition| {
                        condition.get("type").and_then(Value::as_str) == Some("Failed")
                            && condition.get("status").and_then(Value::as_str) == Some("True")
                    })
                });
        states.insert(name.to_owned(), (succeeded, failed));
    }
    let logs_result = kubectl(
        config,
        vec![
            "logs",
            "--namespace",
            &config.namespace,
            "--selector",
            &selector,
            "--all-containers=true",
            "--prefix=true",
            "--tail=-1",
            "--ignore-errors=true",
        ],
        None,
    )
    .await;
    let logs = match logs_result {
        Ok(logs) => logs,
        Err(error) if states.values().any(|(succeeded, failed)| *succeeded || *failed) => {
            return Err(error).context("read completed AKS client fleet logs");
        }
        Err(_) => String::new(),
    };
    let mut progress = Vec::with_capacity(handles.len());
    for handle in handles {
        let ClientProcess::Aks { job_name } = &handle.process else {
            bail!("AKS client fleet poll received a non-AKS process handle");
        };
        let (completed, failed) = states.get(job_name).copied().unwrap_or_default();
        let output = logs
            .lines()
            .filter(|line| line.contains(job_name))
            .collect::<Vec<_>>()
            .join("\n");
        progress.push(ClientProgress {
            ready: output.contains("OAT_CLIENT_READY "),
            schedule_received: output.contains("OAT_SCHEDULE_RECEIVED "),
            completed,
            failure: failed.then(|| format!("AKS DMV client Job {job_name} failed: {output}")),
            output,
        });
    }
    Ok(progress)
}

async fn finish_aks_client_job(config: &OatConfig, handle: ClientHandle) -> Result<String> {
    let ClientProcess::Aks { job_name } = &handle.process else {
        bail!("AKS client finish received a non-AKS process handle");
    };
    let progress = poll_aks_client_job(config, &handle).await?;
    if let Some(failure) = progress.failure {
        bail!("{failure}");
    }
    if !progress.completed {
        bail!("AKS DMV client Job {job_name} was not complete at finish");
    }
    let provenance = aks_client_image_evidence(config, job_name).await?;
    kubectl(
        config,
        vec![
            "delete",
            "job",
            job_name,
            "--namespace",
            &config.namespace,
            "--ignore-not-found",
            "--wait=true",
            "--timeout=30s",
        ],
        None,
    )
    .await
    .context("delete completed AKS DMV client Job")?;
    Ok(format!(
        "{}\nOAT_CLIENT_IMAGE_PROVENANCE {provenance}\n",
        progress.output
    ))
}

async fn cancel_aks_client_job(config: &OatConfig, handle: &ClientHandle) -> Result<()> {
    let ClientProcess::Aks { job_name } = &handle.process else {
        bail!("AKS client cancel received a non-AKS process handle");
    };
    kubectl(
        config,
        vec![
            "delete",
            "job",
            job_name,
            "--namespace",
            &config.namespace,
            "--ignore-not-found",
            "--wait=true",
            "--timeout=30s",
        ],
        None,
    )
    .await
    .with_context(|| format!("cancel AKS DMV client Job {job_name}"))?;
    Ok(())
}

/// Runs the configured benchmark matrix and publishes its compact results.
///
/// # Errors
///
/// Returns an error if configuration, Kubernetes operations, measurement, restoration,
/// or result publication fails.
pub async fn execute(
    config: OatConfig,
    id: &str,
    state: Arc<AppState>,
    cancellation: watch::Receiver<bool>,
) -> Result<RunOutcome> {
    config.validate()?;
    state
        .update(id, |status| {
            status.state = RunState::Running;
            status.progress = "preparing benchmark platform".to_owned();
            status.started_at_unix_ms = unix_time_ms();
        })
        .await;

    let outcome = run_matrix(&config, id, Arc::clone(&state), cancellation).await;

    state
        .update(id, |status| {
            status.progress = "writing compact Parquet results".to_owned();
            if outcome.aks_cleanup_confirmed {
                status.aks_cleanup_resolved = true;
            }
        })
        .await;
    let result_count = outcome.rows.len();
    let cancelled = outcome.cancelled;
    publish_matrix_outcome(&config, outcome).await?;
    Ok(RunOutcome {
        result_count,
        cancelled,
    })
}

pub struct RunOutcome {
    pub result_count: usize,
    pub cancelled: bool,
}

struct MatrixOutcome {
    rows: Vec<OatResult>,
    error: Option<anyhow::Error>,
    cancelled: bool,
    aks_cleanup_confirmed: bool,
}

async fn publish_matrix_outcome(config: &OatConfig, outcome: MatrixOutcome) -> Result<()> {
    let publication = async {
        if config.platform.kind == crate::config::OatMode::Aks {
            refresh_host_publisher_assertion(config).await?;
        }
        publish_results(config, &outcome.rows).await
    }
    .await;
    match (outcome.error, publication) {
        (None, result) => result,
        (Some(error), Ok(())) => Err(error),
        (Some(error), Err(publication_error)) => Err(error.context(format!(
            "publishing retained matrix evidence also failed: {publication_error:#}"
        ))),
    }
}

fn host_publisher_assertion_command(config: &OatConfig) -> Command {
    let mut command = Command::new("kubectl");
    if let Some(context) = &config.kubectl_context {
        command.args(["--context", context]);
    }
    command.args([
        "--request-timeout=60s",
        "create",
        "token",
        "arrow-collector",
        "--namespace",
        "arrow-collector",
        "--audience",
        "api://AzureADTokenExchange",
        "--duration",
        "1h",
    ]);
    use_operator_cli_identity(&mut command);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command
}

async fn refresh_host_publisher_assertion(config: &OatConfig) -> Result<()> {
    let client_id = std::env::var_os("AZURE_CLIENT_ID").filter(|value| !value.is_empty());
    let tenant_id = std::env::var_os("AZURE_TENANT_ID").filter(|value| !value.is_empty());
    let token_file = std::env::var_os("AZURE_FEDERATED_TOKEN_FILE").filter(|value| !value.is_empty());
    if client_id.is_none() || tenant_id.is_none() {
        bail!("AKS publisher workload identity configuration is incomplete");
    }
    let token_file = token_file.context("AKS publisher assertion file is not configured")?;
    let output = tokio::time::timeout(
        Duration::from_secs(90),
        host_publisher_assertion_command(config).output(),
    )
    .await
    .context("timed out refreshing the AKS host publisher assertion")?
    .context("start kubectl to refresh the AKS host publisher assertion")?;
    if !output.status.success() {
        bail!(
            "kubectl could not refresh the AKS host publisher assertion ({})",
            output.status
        );
    }
    let assertion =
        String::from_utf8(output.stdout).context("kubectl returned a non-UTF-8 AKS host publisher assertion")?;
    let assertion = validated_host_publisher_assertion(&assertion)?;
    replace_private_secret(Path::new(&token_file), assertion)
}

fn validated_host_publisher_assertion(assertion: &str) -> Result<&str> {
    let assertion = assertion.trim();
    let parts = assertion.split('.').collect::<Vec<_>>();
    if assertion.len() > 16_384
        || parts.len() != 3
        || parts.iter().any(|part| {
            part.is_empty()
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'='))
        })
    {
        bail!("kubectl returned an invalid AKS host publisher assertion");
    }
    Ok(assertion)
}

fn replace_private_secret(path: &Path, contents: &str) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let metadata = fs::symlink_metadata(path).context("inspect existing AKS host publisher assertion file")?;
        if !metadata.file_type().is_file() {
            bail!("AKS host publisher assertion path is not a regular file");
        }
        let parent = path
            .parent()
            .context("AKS host publisher assertion has no parent directory")?;
        let name = path
            .file_name()
            .context("AKS host publisher assertion has no file name")?;
        let temporary = parent.join(format!(".{}.{}.tmp", name.to_string_lossy(), uuid::Uuid::new_v4()));
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)
                .context("create refreshed private AKS host publisher assertion")?;
            file.write_all(contents.as_bytes())
                .context("write refreshed private AKS host publisher assertion")?;
            file.write_all(b"\n")
                .context("finish refreshed private AKS host publisher assertion")?;
            file.sync_all()
                .context("sync refreshed private AKS host publisher assertion")?;
            fs::rename(&temporary, path).context("replace expired private AKS host publisher assertion")?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
    #[cfg(not(unix))]
    {
        let _ = (path, contents);
        bail!("AKS host publisher assertions require a Unix devcontainer")
    }
}

async fn run_matrix(
    config: &OatConfig,
    orchestration_id: &str,
    state: Arc<AppState>,
    cancellation: watch::Receiver<bool>,
) -> MatrixOutcome {
    let submitted_config = capture_submitted_config_identity(&state, orchestration_id).await;
    let cell_state = Arc::clone(&state);
    run_matrix_with(
        config,
        orchestration_id,
        &state,
        move |cell_config, run_id, replicas, concurrency| {
            run_isolated_cell(
                cell_config,
                orchestration_id,
                run_id,
                (replicas, concurrency),
                cancellation.clone(),
                submitted_config.clone(),
                Arc::clone(&cell_state),
            )
        },
    )
    .await
}

async fn run_matrix_with<F, Fut>(
    config: &OatConfig,
    orchestration_id: &str,
    state: &AppState,
    mut run_cell: F,
) -> MatrixOutcome
where
    F: FnMut(OatConfig, String, u32, u32) -> Fut,
    Fut: std::future::Future<Output = MatrixOutcome>,
{
    let mut results = Vec::new();
    let mut saw_cell = false;
    let mut aks_cleanup_confirmed = true;
    let cells = matrix_cell_specs(config);
    let total = cells.len();
    for (completed, (replicas, concurrency, clients, run_id)) in cells.into_iter().enumerate() {
        state
            .update(orchestration_id, |status| {
                status.local_teardown = None;
                status.progress = format!(
                    "experiment {}/{total}: ArrowCol replicas={replicas}, DFE --num-cores={concurrency}, clients/table={clients}",
                    completed + 1
                );
            })
            .await;
        let mut cell = match config_for_run(config, &run_id) {
            Ok(mut cell_config) => {
                cell_config.clients_per_table = clients;
                run_cell(cell_config, run_id.clone(), replicas, concurrency).await
            }
            Err(error) => {
                let mut cell = finish_matrix_cell(
                    config,
                    orchestration_id,
                    &run_id,
                    replicas,
                    concurrency,
                    Err(error),
                    Ok(()),
                );
                cell.aks_cleanup_confirmed = false;
                cell
            }
        };
        saw_cell = true;
        aks_cleanup_confirmed &= cell.aks_cleanup_confirmed;
        results.append(&mut cell.rows);
        if cell.cancelled {
            return MatrixOutcome {
                rows: results,
                error: None,
                cancelled: true,
                aks_cleanup_confirmed: saw_cell && aks_cleanup_confirmed,
            };
        }
        if cell.error.is_some() {
            return MatrixOutcome {
                rows: results,
                error: cell.error,
                cancelled: false,
                aks_cleanup_confirmed: saw_cell && aks_cleanup_confirmed,
            };
        }
    }
    MatrixOutcome {
        rows: results,
        error: None,
        cancelled: false,
        aks_cleanup_confirmed: saw_cell && aks_cleanup_confirmed,
    }
}

fn local_teardown_transition(
    config: &OatConfig,
    cell_id: &str,
    rows: &[OatResult],
) -> Result<Option<crate::model::LocalTeardown>> {
    if rows.is_empty()
        || rows
            .iter()
            .any(|row| row.run_id != cell_id || row.status != "succeeded")
    {
        return Ok(None);
    }
    let mut schedule = None;
    for row in rows {
        let evidence: Value = serde_json::from_str(row.evidence_json.as_deref().context("missing cell evidence")?)?;
        let published = &evidence["client_schedule"];
        ensure!(
            published["scope"] == "orchestrator_assigned_schedule_with_observed_full_fleet_acknowledgement",
            "missing acknowledged cell schedule"
        );
        let start = published["effective"]["run_start_unix_ms"]
            .as_u64()
            .context("missing schedule start")?;
        let acknowledged = published["effective"]["acknowledged_at_unix_ms"]
            .as_u64()
            .context("missing schedule acknowledgement")?;
        let measurement_start = published["effective"]["measurement_start_unix_ms"]
            .as_u64()
            .context("missing measurement start")?;
        let end = published["effective"]["measurement_end_unix_ms"]
            .as_u64()
            .context("missing measurement end")?;
        let expected_start = start
            .checked_add(config.warmup_seconds.checked_mul(1_000).context("warmup overflow")?)
            .context("measurement start overflow")?;
        let expected_end = start
            .checked_add(
                config
                    .warmup_seconds
                    .checked_add(config.measure_seconds)
                    .context("duration overflow")?
                    .checked_mul(1_000)
                    .context("duration milliseconds overflow")?,
            )
            .context("measurement end overflow")?;
        ensure!(
            start > 0 && acknowledged < start && measurement_start == expected_start && end == expected_end,
            "measurement window identity mismatch"
        );
        ensure!(
            schedule.is_none_or(|previous| previous == (start, end)),
            "cell schedules differ"
        );
        schedule = Some((start, end));
    }
    let (start, end) = schedule.context("missing cell schedule")?;
    let root = config
        .platform
        .compose_state_root
        .as_deref()
        .context("local state root missing")?;
    let path = std::env::current_dir()?
        .join(root)
        .join(cell_id)
        .join("control/schedule");
    let raw = fs::read_to_string(path).context("read authoritative schedule before local teardown")?;
    ensure!(
        raw == format!("{start}\n"),
        "authoritative schedule changed before local teardown"
    );
    let now = unix_time_ms_u64()?;
    ensure!(now >= end, "measurement window has not ended");
    Ok(Some(crate::model::LocalTeardown {
        cell_id: cell_id.to_owned(),
        schedule_unix_ms: start,
        measurement_end_unix_ms: end,
        started_at_unix_ms: now,
    }))
}

async fn publish_local_teardown(
    state: &AppState,
    orchestration_id: &str,
    config: &OatConfig,
    run_id: &str,
    rows: &[OatResult],
) {
    match local_teardown_transition(config, run_id, rows) {
        Ok(Some(transition)) => {
            state
                .update(orchestration_id, |status| {
                    if matches!(status.state, crate::model::RunState::Running) {
                        status.local_teardown = Some(transition);
                    }
                })
                .await
        }
        Ok(None) => {}
        Err(error) => tracing::warn!(%error, "local teardown transition unavailable"),
    }
}

async fn run_isolated_cell(
    config: OatConfig,
    orchestration_id: &str,
    run_id: String,
    dimensions: (u32, u32),
    mut cancellation: watch::Receiver<bool>,
    submitted_config: Option<SubmittedConfigIdentity>,
    state: Arc<AppState>,
) -> MatrixOutcome {
    let (replicas, concurrency) = dimensions;
    let mut platform = match platform_for_run(&config, &run_id) {
        Ok(platform) => platform,
        Err(error) => {
            let mut outcome = finish_matrix_cell(
                &config,
                orchestration_id,
                &run_id,
                replicas,
                concurrency,
                Err(error),
                Ok(()),
            );
            outcome.aks_cleanup_confirmed = false;
            return outcome;
        }
    };
    let timing_budget = match config.timing_budget() {
        Ok(budget) => budget,
        Err(error) => {
            let mut outcome = finish_matrix_cell(
                &config,
                orchestration_id,
                &run_id,
                replicas,
                concurrency,
                Err(error),
                Ok(()),
            );
            outcome.aks_cleanup_confirmed = false;
            return outcome;
        }
    };
    let lifecycle_started_at = tokio::time::Instant::now();
    let lifecycle_deadline = lifecycle_started_at + Duration::from_millis(timing_budget.planned_cell_duration_ms);
    let setup_deadline = lifecycle_started_at + Duration::from_millis(timing_budget.pre_cell_setup_timeout_ms);
    let (result, cancelled) = tokio::select! {
        result = async {
            match tokio::time::timeout_at(setup_deadline, platform.ensure_stack(&config, orchestration_id, replicas)).await {
                Ok(result) => result.context("prepare isolated OAT matrix-cell platform")?,
                Err(_) => bail!("planned pre-cell setup deadline expired; normal platform cleanup follows"),
            }
            let timing = config.timing_budget()?;
            require_lifecycle_remaining_at(
                tokio::time::Instant::now(),
                lifecycle_deadline,
                Duration::from_millis(timing.after_setup_budget_ms),
                "ArrowCol scale",
            )?;
            match tokio::time::timeout_at(lifecycle_deadline, async {
                let mut rows = run_matrix_cell(
                    &config,
                    orchestration_id,
                    &run_id,
                    replicas,
                    concurrency,
                    platform.as_mut(),
                    lifecycle_deadline,
                ).await?;
                if let Err(error) = platform.publish_official_measurement(
                    &config, &rows, submitted_config.as_ref(), lifecycle_deadline,
                ).await {
                    tracing::error!(%error, "official cell measurement publication failed; retaining original rows for normal cleanup");
                    for row in &mut rows {
                        row.status = "failed".to_owned();
                        row.failure = Some(match row.failure.take() {
                            Some(prior) => format!("{prior}; official_measurement_publication_failed"),
                            None => "official_measurement_publication_failed".to_owned(),
                        });
                    }
                }
                Ok(rows)
            }).await {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!("planned cell lifecycle duration expired; normal platform cleanup follows")),
            }
        } => (result, false),
        _ = wait_for_cancellation(&mut cancellation) => (Err(anyhow::anyhow!("local OAT run cancelled")), true),
    };
    if !cancelled
        && config.platform.kind == crate::config::OatMode::Local
        && let Ok(rows) = &result
    {
        publish_local_teardown(&state, orchestration_id, &config, &run_id, rows).await;
    }
    let cleanup = platform.cleanup(&config).await;
    let shutdown_drain_evidence = platform.shutdown_drain_evidence();
    let aks_cleanup_confirmed = config.platform.kind == crate::config::OatMode::Aks && cleanup.is_ok();
    if cancelled && cleanup.is_ok() {
        return MatrixOutcome {
            rows: Vec::new(),
            error: None,
            cancelled: true,
            aks_cleanup_confirmed,
        };
    }
    let mut outcome = finish_matrix_cell(
        &config,
        orchestration_id,
        &run_id,
        replicas,
        concurrency,
        result,
        cleanup,
    );
    if let Some(evidence) = shutdown_drain_evidence
        && let Err(error) = attach_shutdown_drain_evidence(&mut outcome, evidence)
    {
        for row in &mut outcome.rows {
            row.status = "failed".to_owned();
            let message = format!("serialize collector shutdown drain evidence: {error:#}");
            row.failure = Some(match row.failure.take() {
                Some(previous) => format!("{previous}; {message}"),
                None => message.clone(),
            });
        }
        outcome.error = Some(match outcome.error.take() {
            Some(previous) => previous.context(format!("serialize collector shutdown drain evidence: {error:#}")),
            None => error,
        });
    }
    outcome
}

fn attach_shutdown_drain_evidence(outcome: &mut MatrixOutcome, shutdown_drain: Value) -> Result<()> {
    for row in &mut outcome.rows {
        let evidence_json = row.evidence_json.as_deref().context("cell evidence is missing")?;
        let mut evidence: Value =
            serde_json::from_str(evidence_json).context("parse cell evidence before shutdown update")?;
        evidence["kusto_snapshot_coverage"] = kusto_snapshot_coverage(
            &row.run_id,
            &row.table,
            &evidence["qap_observation"],
            &shutdown_drain["stopped_delta_snapshots"],
        )?;
        evidence["shutdown_drain"] = shutdown_drain.clone();
        row.evidence_json =
            Some(serde_json::to_string(&evidence).context("serialize cell evidence with shutdown drain")?);
    }
    Ok(())
}

fn kusto_snapshot_coverage(run_id: &str, table: &str, observation: &Value, snapshots: &Value) -> Result<Value> {
    let binding = qap_bindings(run_id, &[table.to_owned()])?.remove(0);
    let snapshot = &snapshots[table];
    let observed = &observation["tables"][&binding.external_table];
    let identity_matches = observation["run_id"] == run_id
        && observation["observed_unix_ms"].as_u64().is_some_and(|time| time > 0)
        && observation["expected_tables"]
            .as_array()
            .is_some_and(|tables| tables.contains(&json!(binding.external_table)))
        && snapshot["run_id"] == run_id
        && snapshot["table"] == table
        && snapshot["source"] == "verified_stopped_snapshot_manifest"
        && snapshot["manifest"] == format!("{run_id}/{table}/manifest.json")
        && snapshot["manifest_sha256"]
            .as_str()
            .is_some_and(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()));
    let counts = snapshot["row_count"]
        .as_u64()
        .zip(snapshot["table_version"].as_u64())
        .zip(observed["row_count"].as_u64().zip(observed["table_version"].as_u64()));
    let status = match counts {
        Some((delta, kusto)) if identity_matches && observed["available"] == true => {
            if delta == kusto {
                "matched"
            } else {
                "mismatch"
            }
        }
        _ => "unknown",
    };
    Ok(json!({
        "status": status, "run_id": run_id, "table": table, "external_table": binding.external_table,
        "delta_snapshot": snapshot, "observed_row_count": observed["row_count"],
        "observed_table_version": observed["table_version"], "observed_unix_ms": observation["observed_unix_ms"],
        "scope": "exact_stopped_snapshot_count_and_version_coverage_only",
        "unique_payload_delivery": "unknown", "queryable_delivery": "unknown", "delivery_latency": null,
        "comment": "Matching counts/version establish snapshot coverage, not unique payload delivery or per-row queryability/latency.",
    }))
}

async fn wait_for_cancellation(cancellation: &mut watch::Receiver<bool>) {
    loop {
        if *cancellation.borrow() {
            return;
        }
        if cancellation.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

fn finish_matrix_cell(
    config: &OatConfig,
    orchestration_id: &str,
    run_id: &str,
    replicas: u32,
    concurrency: u32,
    result: Result<Vec<OatResult>>,
    cleanup: Result<()>,
) -> MatrixOutcome {
    let aks_cleanup_confirmed = config.platform.kind == crate::config::OatMode::Aks && cleanup.is_ok();
    let (mut rows, mut error) = match result {
        Ok(rows) => {
            let error = rows
                .iter()
                .find_map(|row| row.failure.as_deref())
                .map(|error| anyhow::anyhow!("{error}"));
            (rows, error)
        }
        Err(error) => {
            let rows = (|| {
                let started_at_ms =
                    i64::try_from(unix_time_ms()).context("failed cell timestamp exceeds Arrow timestamp range")?;
                result_rows(
                    config,
                    orchestration_id,
                    run_id,
                    replicas,
                    concurrency,
                    CellMeasurements {
                        clients: ClientRun {
                            metrics: HashMap::new(),
                            pinning: PinningReport::default(),
                            qap_tables: Vec::new(),
                            failure: Some(format!("{error:#}")),
                            evidence: CellEvidence::default(),
                        },
                        started_at_ms,
                        qap: QapMetrics::default(),
                        resources: None,
                        resource_evidence: None,
                    },
                )
            })();
            match rows {
                Ok(rows) => (rows, Some(error)),
                Err(record_error) => (
                    Vec::new(),
                    Some(error.context(format!("recording failed cell also failed: {record_error:#}"))),
                ),
            }
        }
    };
    if let Err(cleanup_error) = cleanup {
        let detail = format!("clean up isolated matrix-cell platform: {cleanup_error:#}");
        error = Some(match error {
            Some(error) => error.context(detail.clone()),
            None => anyhow::anyhow!("{detail}"),
        });
        for row in &mut rows {
            row.status = if row.pinning_error.is_some() {
                "pinning_violation"
            } else {
                "failed"
            }
            .to_owned();
            let failure = row
                .failure
                .as_ref()
                .map_or_else(|| detail.clone(), |failure| format!("{failure}; {detail}"));
            row.failure = Some(failure);
            // Public Parquet carries fixed categories, never raw command output.
            match serde_json::from_str::<Value>(&row.tunables_json) {
                Ok(mut tunables) => {
                    tunables["cleanup_failed"] = Value::Bool(true);
                    row.tunables_json = tunables.to_string();
                }
                Err(cause) => {
                    if let Some(original) = error.take() {
                        error = Some(original.context(format!(
                            "attaching cleanup failure to result tunables also failed: {cause}"
                        )));
                    }
                }
            }
        }
    }
    MatrixOutcome {
        rows,
        error,
        cancelled: false,
        aks_cleanup_confirmed,
    }
}

fn matrix_cell_specs(config: &OatConfig) -> Vec<(u32, u32, usize, String)> {
    let default_clients = [config.clients_per_table];
    let client_counts = config.clients_per_table_sweep.as_deref().unwrap_or(&default_clients);
    let mut cells = Vec::new();
    for replicas in &config.replica_counts {
        for concurrency in &config.dfe_worker_concurrency {
            for clients in client_counts {
                cells.push((*replicas, *concurrency, *clients, uuid::Uuid::new_v4().to_string()));
            }
        }
    }
    cells
}

fn platform_for_run(config: &OatConfig, run_id: &str) -> Result<Box<dyn Platform>> {
    if config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable)
        && config.platform.kind == crate::config::OatMode::Local
    {
        bail!("scale_out_single_table exploratory runs require AKS");
    }
    if config.platform.kind == crate::config::OatMode::Aks
        && config.campaign_profile == Some(crate::config::CampaignProfile::SingleHotTable)
    {
        CampaignIdentity::from_lookup(|name| std::env::var(name).ok())?;
        bail!(
            "AKS campaign is blocked pending end-to-end runtime qualification, including custody lifecycle and collector-visible shared storage"
        );
    }
    match config.platform.kind {
        crate::config::OatMode::Local => Ok(Box::new(LocalPlatform::new(config, run_id)?)),
        crate::config::OatMode::Aks => Ok(Box::new(AksPlatform {
            run_id: run_id.to_owned(),
            resource_readback: Vec::new(),
            cpu_throttling_start: None,
            cpu_throttling_evidence: None,
            namespace_created: false,
            namespace_create_attempted: false,
            namespace_owner_token: uuid::Uuid::new_v4().to_string(),
            namespace_uid: None,
            runtime_dir: None,
            qap_relative_data_root: None,
            delta_root: None,
            router_metrics_url: None,
            router_tunnel: None,
            fic_record_path: None,
            fic_ready_snapshot_path: None,
            fic_prepare_attempted: false,
            qap_record_path: None,
            qap_bind_attempted: false,
            qap_cleanup_confirmed: false,
            qap_outcome_unresolved: false,
            run_table_initialization_attempted: false,
            run_table_initialization_drained: false,
            workload_stop_record_path: None,
            schedule_configmap_name: format!(
                "oat-schedule-{}",
                run_id
                    .chars()
                    .filter(|character| character.is_ascii_alphanumeric())
                    .take(16)
                    .collect::<String>()
            ),
            service_account: None,
            router_yaml: None,
            shutdown_drain_evidence: None,
            campaign_cli_paths: None,
        })),
    }
}

fn config_for_run(config: &OatConfig, run_id: &str) -> Result<OatConfig> {
    let mut cell_config = config.clone();
    if cell_config.platform.kind == crate::config::OatMode::Aks {
        cell_config.namespace = scoped_aks_namespace(&config.namespace, run_id)?;
        cell_config.arrowcol_endpoint = format!(
            "http://arrowcol-router.{}.svc.cluster.local:50050",
            cell_config.namespace
        );
        cell_config.router_metrics_endpoint = format!(
            "http://arrowcol-router.{}.svc.cluster.local:50050/metrics",
            cell_config.namespace
        );
    }
    cell_config.validate()?;
    Ok(cell_config)
}

fn expected_pin_keys_json(tables: &[String]) -> Result<String> {
    let available = collector_chain::dmv::tables()?;
    let mut keys = Vec::with_capacity(tables.len());
    let mut lease_keys = Vec::with_capacity(tables.len());
    for table in tables {
        let definition = available
            .iter()
            .find(|candidate| candidate.identity().table().as_str().eq_ignore_ascii_case(table))
            .with_context(|| format!("DMV table {table} has no canonical identity"))?;
        let identity = definition.identity();
        keys.push((
            "dataflow",
            identity.catalog().as_str(),
            identity.schema().as_str(),
            identity.table().as_str(),
        ));
        lease_keys.push(LeaseKey::new(ImplementationPool::Dataflow, (*identity).clone()));
    }
    keys.sort_unstable();
    lease_keys.sort();
    if keys.windows(2).any(|pair| pair[0] == pair[1]) {
        bail!("OAT pin set contains duplicate canonical table identities");
    }
    let values = keys
        .into_iter()
        .map(|(pool, catalog, schema, table)| {
            json!({
                "implementation_pool": pool,
                "catalog_name": catalog,
                "schema_name": schema,
                "table_name": table,
            })
        })
        .collect::<Vec<_>>();
    let expected_digest = run_table_pin_set_digest(&lease_keys)?;
    serde_json::to_string(&json!({
        "expected_keys": values,
        "expected_digest": expected_digest,
    }))
    .context("serialize canonical OAT table pin set")
}

#[derive(serde::Deserialize)]
struct RouterYaml {
    routing: RouterRoutingYaml,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RouterRoutingYaml {
    #[serde(rename = "refresh_interval_ms")]
    _refresh_interval_ms: u64,
    max_snapshot_age_ms: u64,
    max_future_skew_ms: u64,
    max_table_entries: usize,
    max_oldest_buffer_age_ms: u64,
    hard_pressure_bps: u16,
    #[serde(default)]
    excluded_admission_gauges: BTreeSet<AdmissionPressureGauge>,
    warmup_ms: u64,
    minimum_warmup_bps: u16,
    lease_ttl_ms: u64,
    #[serde(default = "default_oat_run_pin_ttl_ms")]
    oat_run_pin_ttl_ms: u64,
    #[serde(default)]
    oat_run_id: Option<String>,
    readiness_max_staleness_ms: u64,
    #[serde(default)]
    distinct_table_owners: bool,
}

fn default_oat_run_pin_ttl_ms() -> u64 {
    3_600_000
}

fn routing_policy_from_yaml(router_yaml: &str, run_id: &str) -> Result<RoutingPolicy> {
    let router: RouterYaml = serde_yaml::from_str(router_yaml).context("parse exact rendered router routing config")?;
    let routing = router.routing;
    let policy = RoutingPolicy {
        pressure: PressurePolicy {
            max_snapshot_age_ms: routing.max_snapshot_age_ms,
            max_future_skew_ms: routing.max_future_skew_ms,
            max_table_entries: routing.max_table_entries,
            max_oldest_buffer_age_ms: routing.max_oldest_buffer_age_ms,
        },
        hard_pressure_bps: routing.hard_pressure_bps,
        excluded_admission_gauges: routing.excluded_admission_gauges,
        warmup_ms: routing.warmup_ms,
        minimum_warmup_bps: routing.minimum_warmup_bps,
        lease_ttl_ms: routing.lease_ttl_ms,
        oat_run_pin_ttl_ms: routing.oat_run_pin_ttl_ms,
        readiness_max_staleness_ms: routing.readiness_max_staleness_ms,
        distinct_table_owners: routing.distinct_table_owners,
        oat_run_id: routing.oat_run_id,
    };
    if policy.oat_run_id.as_deref() != Some(run_id) {
        bail!("rendered router policy does not authorize the exact OAT run ID");
    }
    policy
        .validate()
        .map_err(anyhow::Error::msg)
        .context("validate rendered router routing policy")?;
    Ok(policy)
}

fn inject_router_oat_pin_ttl(resources: &mut [Value], ttl_ms: u64) -> Result<()> {
    let config_map = resources
        .iter_mut()
        .find(|resource| {
            resource.get("kind").and_then(Value::as_str) == Some("ConfigMap")
                && resource.pointer("/metadata/name").and_then(Value::as_str) == Some("arrowcol-config")
        })
        .context("rendered AKS workload has no arrowcol-config ConfigMap")?;
    let router_yaml = config_map
        .pointer("/data/router.yaml")
        .and_then(Value::as_str)
        .context("rendered AKS arrowcol-config has no router.yaml")?
        .to_owned();
    let mut router: serde_yaml::Value = serde_yaml::from_str(&router_yaml).context("parse AKS router YAML")?;
    let routing = router
        .get_mut("routing")
        .and_then(serde_yaml::Value::as_mapping_mut)
        .context("AKS router YAML has no routing mapping")?;
    let lease_key = serde_yaml::Value::String("lease_ttl_ms".to_owned());
    let current_lease_ttl = routing.get(&lease_key).and_then(serde_yaml::Value::as_u64).unwrap_or(0);
    routing.insert(
        lease_key,
        serde_yaml::Value::Number(current_lease_ttl.max(ttl_ms).into()),
    );
    routing.insert(
        serde_yaml::Value::String("oat_run_pin_ttl_ms".to_owned()),
        serde_yaml::Value::Number(ttl_ms.into()),
    );
    config_map["data"]["router.yaml"] = json!(serde_yaml::to_string(&router)?);
    Ok(())
}

fn rendered_router_yaml(resources: &[Value]) -> Result<String> {
    let mut matches = resources.iter().filter(|resource| {
        resource.get("kind").and_then(Value::as_str) == Some("ConfigMap")
            && resource.pointer("/metadata/name").and_then(Value::as_str) == Some("arrowcol-config")
    });
    let config = matches
        .next()
        .context("rendered AKS workload has no arrowcol-config ConfigMap")?;
    if matches.next().is_some() {
        bail!("rendered AKS workload contains multiple arrowcol-config ConfigMaps");
    }
    config
        .pointer("/data/router.yaml")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .context("rendered AKS arrowcol-config has no router.yaml")
}

async fn settle_cell_commit_evidence(
    config: &OatConfig,
    platform: &dyn Platform,
    baseline_logs: &[ProcessLog],
    evidence: &CellEvidence,
    cell_start_unix_ms: u64,
) -> Result<(Vec<ProcessLog>, HashMap<String, Value>)> {
    let timeout = Duration::from_secs(config.reconciliation_settle_timeout_seconds);
    let started = tokio::time::Instant::now();
    let targets = config
        .tables
        .iter()
        .map(|table| (table.clone(), evidence.client_cumulative_total(table, "completed_rows")))
        .collect::<HashMap<_, _>>();
    let can_settle = evidence.measurement_window.is_some() && targets.values().all(Option::is_some);
    loop {
        let logs = platform.process_logs_for_role(config, ComponentRole::Collector).await?;
        let (cell_commit_logs, used_log_offsets) = cell_scoped_commit_logs(&logs, baseline_logs)?;
        let cell_scope = if used_log_offsets {
            "process_log_offset_and_ingress_time"
        } else {
            "ingress_time_only"
        };
        let mut commits = durable_commit_evidence_for_cell(
            config,
            &logs,
            &cell_commit_logs,
            evidence.measurement_window,
            Some(cell_start_unix_ms),
        )?;
        let elapsed = started.elapsed();
        let evidence_unavailable = config.tables.iter().any(|table| {
            commits
                .get(table)
                .and_then(|value| value.pointer("/run_cumulative_output/unavailable_reason"))
                .is_some()
        });
        let settled = can_settle
            && config.tables.iter().all(|table| {
                targets
                    .get(table)
                    .copied()
                    .flatten()
                    .zip(
                        commits
                            .get(table)
                            .and_then(|value| value.pointer("/run_cumulative_output/rows"))
                            .and_then(Value::as_u64),
                    )
                    .is_some_and(|(completed, delta)| delta >= completed)
            });
        if settled || evidence_unavailable || !can_settle || timeout.is_zero() || elapsed >= timeout {
            for table in &config.tables {
                let target = targets.get(table).copied().flatten();
                let output = commits
                    .get_mut(table)
                    .and_then(|value| value.get_mut("run_cumulative_output"));
                if let Some(output) = output {
                    let delta = output.get("rows").and_then(Value::as_u64);
                    output["cell_scope"] = json!(cell_scope);
                    output["settle_seconds"] = json!(elapsed.as_secs_f64());
                    output["settle_timeout_seconds"] = json!(timeout.as_secs());
                    output["settled"] = json!(target.zip(delta).is_some_and(|(completed, delta)| delta >= completed));
                    output["client_completed_rows"] = target.map_or(Value::Null, |rows| json!(rows));
                    output["settle_gap_rows"] = target
                        .zip(delta)
                        .map_or(Value::Null, |(completed, delta)| json!(completed.saturating_sub(delta)));
                }
                if let Some(output) = commits
                    .get_mut(table)
                    .and_then(|value| value.get_mut("output_throughput"))
                {
                    output["cell_scope"] = json!(cell_scope);
                }
            }
            if config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
                for table in &config.tables {
                    let output = commits
                        .get_mut(table)
                        .and_then(|value| value.get_mut("run_cumulative_output"));
                    let event_rows = output.and_then(|value| value["rows"].as_u64());
                    let acknowledged = targets.get(table).copied().flatten();
                    let delta_result = match platform.delta_root() {
                        Some(root) => {
                            match tokio::time::timeout(Duration::from_secs(30), read_delta_log_commits(root, table))
                                .await
                            {
                                Ok(result) => result,
                                Err(_) => Err(anyhow::anyhow!("Delta-log scan exceeded its 30-second bound")),
                            }
                        }
                        None => Err(anyhow::anyhow!("run-scoped Delta root is unavailable")),
                    };
                    let (delta_log, unavailable_reason) = match delta_result {
                        Ok(delta_log) => (Some(delta_log), None),
                        Err(error) => {
                            tracing::warn!(table, %error, "exploratory Delta-log reconciliation evidence is unavailable");
                            (None, Some("delta_log_read_failed"))
                        }
                    };
                    let commit_output = commits.get(table).and_then(|value| value.get("run_cumulative_output"));
                    let reconciliation = exploratory_delta_reconciliation(
                        delta_log.as_ref(),
                        commit_output,
                        acknowledged,
                        unavailable_reason,
                    );
                    if let Some(commit) = commits.get_mut(table) {
                        commit["delta_log_reconciliation"] = reconciliation;
                        if let Some(output) = commit.get_mut("run_cumulative_output") {
                            output["collector_event_rows"] = event_rows.map_or(Value::Null, |rows| json!(rows));
                            if let Some(delta_log) = delta_log {
                                output["rows"] = delta_log["rows"].clone();
                                output["delta_log_files"] = delta_log["files"].clone();
                                output["delta_log_parquet_bytes"] = delta_log["parquet_bytes"].clone();
                                output["delta_log_versions"] = delta_log["versions"].clone();
                                output["source"] = delta_log["scope"].clone();
                            } else {
                                output["rows"] = Value::Null;
                                output["delta_log_files"] = Value::Null;
                                output["delta_log_parquet_bytes"] = Value::Null;
                                output["delta_log_versions"] = Value::Null;
                                output["unavailable_reason"] =
                                    json!(unavailable_reason.unwrap_or("delta_log_read_failed"));
                            }
                            let delta_rows = output["rows"].as_u64();
                            output["settled"] = acknowledged
                                .zip(delta_rows)
                                .map_or(Value::Null, |(completed, delta)| json!(delta >= completed));
                            output["client_completed_rows"] = acknowledged.map_or(Value::Null, |rows| json!(rows));
                            output["settle_gap_rows"] = acknowledged
                                .zip(delta_rows)
                                .map_or(Value::Null, |(completed, delta)| json!(completed.saturating_sub(delta)));
                        }
                    }
                }
            }
            return Ok((logs, commits));
        }
        tokio::time::sleep(Duration::from_millis(250).min(timeout.saturating_sub(elapsed))).await;
    }
}

#[cfg(test)]
#[test]
fn local_lifecycle_budget_allows_positive_setup_elapsed_and_rejects_exhaustion() -> Result<()> {
    let mut config: OatConfig = serde_json::from_str(include_str!(
        "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
    ))?;
    config.platform.kind = crate::config::OatMode::Local;
    let budget = config.timing_budget()?;
    assert_eq!(budget.after_setup_budget_ms, budget.minimum_pin_ttl_ms);
    assert_eq!(
        budget.planned_cell_duration_ms,
        budget.pre_cell_setup_timeout_ms + budget.after_setup_budget_ms
    );

    let started = tokio::time::Instant::now();
    let setup_elapsed = Duration::from_secs(30);
    let deadline = started + Duration::from_millis(budget.planned_cell_duration_ms);
    let post_setup_now = started + setup_elapsed;
    assert!(
        require_lifecycle_remaining_at(
            post_setup_now,
            deadline,
            Duration::from_millis(budget.after_setup_budget_ms),
            "Local admission"
        )
        .is_ok()
    );

    let exhausted_now = deadline - Duration::from_millis(budget.after_setup_budget_ms) + Duration::from_millis(1);
    assert!(
        require_lifecycle_remaining_at(
            exhausted_now,
            deadline,
            Duration::from_millis(budget.after_setup_budget_ms),
            "Local admission"
        )
        .is_err()
    );
    Ok(())
}

fn require_lifecycle_remaining_at(
    now: tokio::time::Instant,
    deadline: tokio::time::Instant,
    required: Duration,
    next_stage: &str,
) -> Result<()> {
    if deadline.saturating_duration_since(now) < required {
        bail!("planned cell lifecycle deadline would expire before {next_stage}");
    }
    Ok(())
}

fn require_lifecycle_remaining(deadline: tokio::time::Instant, required: Duration, next_stage: &str) -> Result<()> {
    require_lifecycle_remaining_at(tokio::time::Instant::now(), deadline, required, next_stage)
}

async fn run_matrix_cell(
    config: &OatConfig,
    orchestration_id: &str,
    run_id: &str,
    replicas: u32,
    concurrency: u32,
    platform: &mut dyn Platform,
    lifecycle_deadline: tokio::time::Instant,
) -> Result<Vec<OatResult>> {
    let timing_budget = config.timing_budget()?;
    let rollout_ms = if config.platform.kind == crate::config::OatMode::Aks {
        config
            .aks_rollout_timeout_seconds
            .checked_mul(1_000)
            .context("AKS rollout deadline overflow")?
    } else {
        0
    };
    let scale_rollout_ms = rollout_ms
        .checked_mul(u64::try_from(config.deployment_names().len())?)
        .context("AKS scale rollout deadline overflow")?;
    let maintenance_rollout_ms = if config.platform.kind == crate::config::OatMode::Aks && maintenance_enabled(config) {
        rollout_ms
    } else {
        0
    };
    let pre_scale_budget = timing_budget
        .minimum_pin_ttl_ms
        .checked_add(scale_rollout_ms)
        .and_then(|value| value.checked_add(maintenance_rollout_ms))
        .context("post-setup lifecycle budget overflow")?;
    require_lifecycle_remaining(
        lifecycle_deadline,
        Duration::from_millis(pre_scale_budget),
        "ArrowCol scale",
    )?;
    platform.scale_arrowcols(config, replicas, concurrency).await?;
    require_lifecycle_remaining(
        lifecycle_deadline,
        Duration::from_millis(timing_budget.minimum_pin_ttl_ms.saturating_add(maintenance_rollout_ms)),
        "run table-pin and maintenance initialization",
    )?;
    let assigned_tables = config.tables.clone();
    let expected_keys_json = expected_pin_keys_json(&assigned_tables)?;
    let pin_now_unix_ms = unix_time_ms_u64()?;
    let pin_expires_at_unix_ms = pin_now_unix_ms
        .checked_add(config.run_pin_ttl_ms)
        .context("OAT run pin expiry overflow")?;
    platform
        .initialize_run_table_pins(
            config,
            run_id,
            &expected_keys_json,
            pin_expires_at_unix_ms,
            pin_now_unix_ms,
        )
        .await?;
    let before_clients_ms = timing_budget
        .minimum_pin_ttl_ms
        .saturating_sub(crate::config::PIN_INIT_SETUP_TIMEOUT_SECONDS.saturating_mul(1_000));
    require_lifecycle_remaining(
        lifecycle_deadline,
        Duration::from_millis(before_clients_ms),
        "client admission",
    )?;
    let cell_start_unix_ms = unix_time_ms_u64()?;
    let started_at_ms =
        i64::try_from(cell_start_unix_ms).context("run start timestamp exceeds Arrow timestamp range")?;
    let baseline_logs = platform.process_logs(config).await?;
    platform.retain_cell_process_baseline(&baseline_logs);
    let mut client_run = run_clients(config, run_id, &assigned_tables, platform, replicas).await?;
    platform.finish_resource_measurement(config).await?;
    let guard_stopped = client_run.evidence.overload_stop.is_some()
        || client_run
            .evidence
            .overload_guard
            .as_ref()
            .is_some_and(|guard| guard["status"] == "unavailable");
    let capture = async {
        let post_client_ms = config
            .reconciliation_settle_timeout_seconds
            .checked_add(config.qap_timeout_seconds)
            .and_then(|seconds| seconds.checked_mul(1_000))
            .context("post-client lifecycle deadline overflow")?;
        require_lifecycle_remaining(
            lifecycle_deadline,
            Duration::from_millis(post_client_ms),
            "commit reconciliation",
        )?;
        let (_collector_logs, durable_commits) = settle_cell_commit_evidence(
            config,
            platform,
            &baseline_logs,
            &client_run.evidence,
            cell_start_unix_ms,
        )
        .await?;
        client_run.evidence.durable_commits = durable_commits;
        let process_logs = platform.process_logs(config).await?;
        client_run.evidence.processes = Some(process_evidence_for_cell(config, &process_logs, &baseline_logs)?);
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Err(error) = capture {
        if !guard_stopped {
            return Err(error);
        }
        tracing::error!(%error, "post-overload output/process capture failed; retaining stop evidence for cleanup");
        client_run.failure = Some(format!(
            "{}; post_overload_stop_capture_unavailable",
            client_run
                .failure
                .as_deref()
                .unwrap_or("overload_guard stopped the cell")
        ));
        if let Some(guard) = &mut client_run.evidence.overload_guard {
            guard["post_stop_capture"] = json!("unavailable");
        }
    }
    let pinning_ok = client_run.pinning.violation.is_none()
        && client_run.pinning.verification_error.is_none()
        && client_run.failure.is_none();
    let mut qap = QapMetrics::default();
    let metrics = client_run.evidence.resource_summary();
    if pinning_ok {
        local_progress(config, "clients finished; observing Kusto QAP");
        let observation = async {
            if client_run.qap_tables.is_empty() {
                return Err(anyhow::anyhow!(LocalDownstreamUnavailable));
            }
            require_lifecycle_remaining(
                lifecycle_deadline,
                Duration::from_secs(config.qap_timeout_seconds),
                "final QAP observation",
            )?;
            let (metrics, observation) = observe_qap(
                &platform.qap_endpoint(config),
                &platform.qap_observation_mode(),
                &platform.qap_database(config)?,
                run_id,
                &client_run.qap_tables,
                config.qap_timeout_seconds,
            )
            .await?;
            qap = apply_qap_observation(platform.kind(), metrics, observation, &mut client_run.evidence);
            if qap.observation_complete != 1 && client_run.evidence.downstream_comment.is_none() {
                bail!("QAP observation omitted required table statistics");
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = observation {
            if local_downstream_optional(platform.kind()) && error.is::<LocalDownstreamUnavailable>() {
                client_run
                    .evidence
                    .downstream_unavailable("local_qap_observation_unavailable");
            } else {
                client_run.failure = Some(format!("observe completed matrix cell: {error:#}"));
            }
        }
    }
    result_rows(
        config,
        orchestration_id,
        run_id,
        replicas,
        concurrency,
        CellMeasurements {
            clients: client_run,
            started_at_ms,
            qap,
            resources: metrics,
            resource_evidence: platform.resource_evidence(),
        },
    )
}

struct CellMeasurements {
    clients: ClientRun,
    started_at_ms: i64,
    qap: QapMetrics,
    resources: Option<ResourceMetrics>,
    resource_evidence: Option<Value>,
}

fn result_rows(
    config: &OatConfig,
    orchestration_id: &str,
    run_id: &str,
    replicas: u32,
    concurrency: u32,
    measurements: CellMeasurements,
) -> Result<Vec<OatResult>> {
    let CellMeasurements {
        clients: client_run,
        started_at_ms,
        qap,
        resources: metrics,
        resource_evidence,
    } = measurements;
    let assigned_tables = config.tables.clone();
    let client_count = config
        .clients_per_table
        .checked_mul(assigned_tables.len())
        .context("OAT client count overflow")?;
    let observed_owners = client_run.pinning.owners.values().collect::<HashSet<_>>().len();
    let cpu_quota_evidence = if config.platform.kind == crate::config::OatMode::Aks {
        let requested = config.effective_aks_resources();
        let collector_limit_millicores = requested
            .collector
            .limits
            .cpu
            .as_deref()
            .map(parse_resource_cpu_millicores)
            .transpose()?;
        let client_limit_millicores = parse_resource_cpu_millicores(
            requested
                .client
                .limits
                .cpu
                .as_deref()
                .context("client CPU limit is missing")?,
        )?;
        let throttling = resource_evidence
            .as_ref()
            .and_then(|evidence| evidence.get("cpu_throttling"))
            .cloned()
            .unwrap_or_else(|| {
                json!({
                    "status": "unavailable",
                    "reason": "AKS cAdvisor throttling evidence is unavailable"
                })
            });
        Some(json!({
            "collector": {
                "requested_workers": concurrency,
                "limit_cpu": requested.collector.limits.cpu,
                "limit_millicores": collector_limit_millicores,
                "workers_per_cpu_core": collector_limit_millicores.map(|limit| f64::from(concurrency) * 1_000.0 / limit as f64),
            },
            "client": {
                "requested_concurrency_per_process": config.dmv_concurrency,
                "limit_cpu": requested.client.limits.cpu,
                "limit_millicores": client_limit_millicores,
                "concurrency_per_cpu_core": config.dmv_concurrency as f64 * 1_000.0 / client_limit_millicores as f64,
            },
            "throttling": throttling,
        }))
    } else {
        None
    };
    let mut tunables = json!({
        "arrowcols": replicas,
        "clients": client_count,
        "clients_per_table": config.clients_per_table,
        "dfe_worker_concurrency": concurrency,
        "dmv_concurrency": config.dmv_concurrency,
        "idle_arrowcols": replicas.saturating_sub(observed_owners as u32),
        "measure_seconds": config.measure_seconds,
        "schedule_lead_seconds": config.schedule_lead_seconds,
        "orchestration_run_id": orchestration_id,
        "pilot_mode": config.pilot_mode,
        "run_pin_ttl_ms": config.run_pin_ttl_ms,
        "rows_per_table": config.rows_per_table,
        "tables": &assigned_tables,
        "warmup_seconds": config.warmup_seconds,
        "failure": client_run.failure.as_ref().map(|_| "measurement_or_observation_failed"),
        "pinning_violation": client_run.pinning.violation.is_some(),
        "pinning_verification_error": client_run.pinning.verification_error.as_ref().map(|_| "ownership_verification_failed"),
        "delta_delivery": "not_established",
        "effective_stop_budgets": config.maintenance_stop_budget()?,
        "planned_cell_duration_ms": config.timing_budget()?.planned_cell_duration_ms,
        "requested_tunables": serde_json::from_str::<Value>(&config.tunables.canonical_json()?)?,
        "requested_aks_resources": if config.platform.kind == crate::config::OatMode::Aks { Some(config.effective_aks_resources()) } else { None },
        "cpu_quota": cpu_quota_evidence,
        "requested_durability_class": config.tunables.wal.as_ref().map(|_| config.tunables.durability_class()),
        "capabilities": capability_evidence(),
    });
    if config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
        tunables["evidence_class"] = json!("d53_exploratory");
        tunables["qualification"] = json!("not_qualifying");
    }
    let details = [
        client_run.failure.clone(),
        client_run
            .pinning
            .violation
            .as_ref()
            .map(|error| format!("pinning_violation: {error}")),
        client_run.pinning.verification_error.clone(),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    let failure = (!details.is_empty()).then(|| details.join("; "));
    let pinning_ok = client_run.pinning.violation.is_none()
        && client_run.pinning.verification_error.is_none()
        && assigned_tables.iter().all(|table| {
            client_run.metrics.contains_key(table)
                && client_run.pinning.owners.contains_key(&canonical_table_key(table))
        });
    let mut rows = Vec::with_capacity(assigned_tables.len());
    for table in assigned_tables {
        let client = client_run.metrics.get(&table);
        let reconciliation_mismatch = config.campaign_profile
            == Some(crate::config::CampaignProfile::ScaleOutSingleTable)
            && client_run
                .evidence
                .durable_commits
                .get(&table)
                .and_then(|commit| commit.pointer("/delta_log_reconciliation/status"))
                .and_then(Value::as_str)
                == Some("mismatch");
        let table_failure = if reconciliation_mismatch {
            Some(match failure.as_deref() {
                Some(failure) => format!("{failure}; exploratory_delta_log_reconciliation_mismatch"),
                None => "exploratory_delta_log_reconciliation_mismatch".to_owned(),
            })
        } else {
            failure.clone()
        };
        if client.is_none() && table_failure.is_none() {
            bail!("DMV client did not report measurements for {table}");
        }
        let owner = client_run.pinning.owners.get(&canonical_table_key(&table));
        let mut table_tunables = tunables.clone();
        if reconciliation_mismatch {
            table_tunables["failure"] = json!("exploratory_reconciliation_mismatch");
        }
        table_tunables["collector_completion"] = json!({
            "evidence": if client.is_some_and(|client| client.completions.len() == config.clients_per_table) {
                "reported"
            } else {
                "unavailable"
            },
            "admission_scope": "completed_upload_calls_across_preflight_warmup_measurement",
            "clients": client.map(|client| &client.completions),
        });
        rows.push(OatResult {
            run_id: run_id.to_owned(),
            mode: match config.platform.kind {
                crate::config::OatMode::Local => "local",
                crate::config::OatMode::Aks => "aks",
            }
            .to_owned(),
            stamp: config.stamp.clone(),
            host: platform_host_label(config.platform.kind).to_owned(),
            experiment: format!(
                "arrowcols-{replicas}-dfe-{concurrency}-clients-{}",
                config.clients_per_table
            ),
            started_at_ms,
            measure_seconds: config.measure_seconds as f64,
            arrowcols: i32::try_from(replicas).context("ArrowCol count exceeds Parquet schema range")?,
            clients: i32::try_from(client_count).context("client count exceeds Parquet schema range")?,
            tables: i32::try_from(config.tables.len()).context("table count exceeds Parquet schema range")?,
            table_owner: owner.cloned(),
            pinning_ok,
            pinning_error: client_run.pinning.violation.clone(),
            failure: table_failure.clone(),
            status: if client_run.pinning.violation.is_some() {
                "pinning_violation"
            } else if table_failure.is_some() {
                "failed"
            } else {
                "succeeded"
            }
            .to_owned(),
            git_sha: std::env::var("GIT_SHA")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "unknown".to_owned()),
            tunables_json: serde_json::to_string(&table_tunables)?,
            evidence_json: {
                let mut evidence = client_run.evidence.for_table(&table, client);
                if config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
                    let output = evidence["commit_window_output_throughput"].clone();
                    let seconds = output["window_seconds"].as_f64();
                    let input_rate = output["input_bytes_per_second"].as_f64();
                    let parquet_rate = output["parquet_bytes_per_second"].as_f64();
                    let commits_per_second = output["commits"]
                        .as_u64()
                        .zip(seconds)
                        .filter(|(_, seconds)| *seconds > 0.0)
                        .map(|(commits, seconds)| commits as f64 / seconds);
                    let mean_file_bytes = output["parquet_bytes"]
                        .as_u64()
                        .zip(output["files"].as_u64())
                        .filter(|(_, files)| *files > 0)
                        .map(|(bytes, files)| bytes as f64 / files as f64);
                    evidence["evidence_class"] = json!("d53_exploratory");
                    evidence["qualification"] = json!("not_qualifying");
                    evidence["exploratory_delta_log_reconciliation"] =
                        evidence["durable_commits"]["delta_log_reconciliation"].clone();
                    evidence["steady_state_window"] = json!({
                        "warmup_seconds": config.warmup_seconds,
                        "measure_seconds": config.measure_seconds,
                        "measurement_start_unix_ms": output["measurement_start_unix_ms"],
                        "measurement_end_unix_ms": output["measurement_end_unix_ms"],
                        "window_seconds": seconds,
                        "scope": "collector_commit_finish_window_not_client_cohort",
                    });
                    evidence["ranking_metrics"] = json!({
                        "committed_rows_per_second": output["rows_per_second"],
                        "uncompressed_arrow_decimal_gb_per_second": input_rate.map(|rate| rate / 1_000_000_000.0),
                        "uncompressed_arrow_bytes_per_second": input_rate,
                        "parquet_bytes_per_second": parquet_rate,
                        "compression_ratio": output["compression_ratio"],
                        "commits_per_second": commits_per_second,
                        "mean_parquet_file_bytes": mean_file_bytes,
                        "per_pod_cpu_millicores": metrics.map(|metrics| metrics.cpu_millicores),
                        "per_pod_cpu_scope": "one_collector_pod_profile_arithmetic_mean_of_measurement_window_samples",
                    });
                }
                if reconciliation_mismatch {
                    evidence["ranking_metrics"] = Value::Null;
                    evidence["ranking_eligible"] = Value::Bool(false);
                    evidence["ranking_ineligible_reason"] = json!("exploratory_delta_log_reconciliation_mismatch");
                }
                if let Some(resource_evidence) = &resource_evidence {
                    evidence["aks_resources"] = resource_evidence.clone();
                }
                Some(serde_json::to_string(&evidence)?)
            },
            replicas: u64::from(replicas),
            dfe_worker_concurrency: u64::from(concurrency),
            table,
            rows: client.map(|client| client.rows),
            duration_ms: client.map(|client| client.duration_ms),
            rows_per_second: if reconciliation_mismatch {
                None
            } else {
                client.map(|client| client.rows_per_second)
            },
            cpu_millicores: metrics.map(|metrics| metrics.cpu_millicores),
            memory_bytes: metrics.map(|metrics| metrics.memory_bytes),
            qap,
        });
    }
    Ok(rows)
}

#[derive(Clone, Debug)]
struct ClientMetrics {
    rows: u64,
    duration_ms: u64,
    rows_per_second: f64,
    completions: Vec<CollectorCompletion>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CollectorCompletion {
    best_effort_streams: u64,
    durable_streams: u64,
    admission_accounting_complete: bool,
    completed_at_unix_ms: u64,
}

struct ClientMeasurement {
    metrics: ClientMetrics,
    pilot_windows: Vec<PilotWindow>,
}

#[derive(Clone, Copy, Debug)]
struct PilotWindow {
    index: u64,
    rows: u64,
    start_unix_ms: u64,
}

struct ClientRun {
    metrics: HashMap<String, ClientMetrics>,
    pinning: PinningReport,
    qap_tables: Vec<String>,
    failure: Option<String>,
    evidence: CellEvidence,
}

#[derive(Default)]
struct PinningReport {
    owners: HashMap<String, String>,
    violation: Option<String>,
    verification_error: Option<String>,
}

async fn run_clients(
    config: &OatConfig,
    project_run_id: &str,
    tables: &[String],
    platform: &mut dyn Platform,
    replicas: u32,
) -> Result<ClientRun> {
    let mut pinning = PinningReport::default();
    match run_clients_inner(config, project_run_id, tables, platform, &mut pinning, replicas).await {
        Ok(report) => Ok(report),
        Err(error) => {
            let sample = async {
                let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
                sample_router_table_owners(platform, config, &client, tables).await
            }
            .await;
            let pinning = failed_client_pinning_report(&error, sample, tables, pinning);
            tracing::error!(run_id = project_run_id, error = %error, "OAT client run failed; incomplete measurements are unavailable");
            Ok(ClientRun {
                metrics: HashMap::new(),
                pinning,
                qap_tables: Vec::new(),
                failure: Some(format!("{error:#}")),
                evidence: CellEvidence::default(),
            })
        }
    }
}

fn failed_client_pinning_report(
    error: &anyhow::Error,
    sample: Result<HashMap<String, Vec<String>>>,
    tables: &[String],
    mut report: PinningReport,
) -> PinningReport {
    let detail = format!("{error:#}");
    record_owner_observation(&mut report, sample, tables, true);
    if detail.contains("pinning_violation") {
        report.violation.get_or_insert(detail);
    }
    report
}

fn record_owner_observation(
    report: &mut PinningReport,
    sample: Result<HashMap<String, Vec<String>>>,
    tables: &[String],
    require_owner: bool,
) {
    match sample {
        Ok(sample) => {
            let violation = check_owner_sample(tables, &sample, &mut report.owners).or_else(|| {
                require_owner
                    .then(|| {
                        tables
                            .iter()
                            .find(|table| !report.owners.contains_key(&canonical_table_key(table)))
                            .map(|table| format!("table {table} has no active ArrowCol owner"))
                    })
                    .flatten()
            });
            if let Some(violation) = violation {
                report.violation.get_or_insert(violation);
            }
        }
        Err(error) => {
            let detail = format!("{error:#}");
            if detail.contains("pinning_violation") {
                report.violation.get_or_insert(detail);
            } else {
                report
                    .verification_error
                    .get_or_insert_with(|| format!("ownership verification failed: {detail}"));
            }
        }
    }
}

async fn run_clients_inner(
    config: &OatConfig,
    project_run_id: &str,
    tables: &[String],
    platform: &mut dyn Platform,
    pinning: &mut PinningReport,
    replicas: u32,
) -> Result<ClientRun> {
    require_single_scale_out_collector(config, replicas)?;
    let mut evidence = CellEvidence::default();
    let mut prepared_guard = if config.overload_guard.enabled {
        let timeout = if config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
            Duration::from_secs(30)
        } else {
            Duration::from_secs(5)
        };
        match tokio::time::timeout(timeout, PreparedGuard::prepare(config, platform)).await {
            Ok(Ok(prepared)) => Some(prepared),
            result => {
                let error = match result {
                    Ok(Err(error)) => format!("{error:#}"),
                    Err(error) => error.to_string(),
                    Ok(Ok(_)) => unreachable!("successful guard preparation returned above"),
                };
                tracing::error!(%error, "overload hard-cap/readback protection unavailable before admission");
                evidence.overload_guard = Some(json!({
                    "status":"unavailable","settings":config.overload_guard,
                    "unavailable_reason":"pre_admission_cap_backlog_or_commit_readback_unavailable",
                    "requested_container_memory_limit_bytes":config.platform.local.collector_memory_limit_bytes,
                    "error":error,
                }));
                return Ok(ClientRun {
                    metrics: HashMap::new(),
                    pinning: std::mem::take(pinning),
                    qap_tables: Vec::new(),
                    failure: Some("overload_guard required protection unavailable before admission".to_owned()),
                    evidence,
                });
            }
        }
    } else {
        evidence.overload_guard = Some(json!({"status":"disabled","settings":config.overload_guard}));
        None
    };
    if let Some(prepared) = &prepared_guard {
        evidence.overload_guard = Some(prepared.engine.proof.clone());
        evidence.overload_stop = prepared.engine.stop.clone();
        if let Some(reason) = prepared.engine.failure() {
            return Ok(ClientRun {
                metrics: HashMap::new(),
                pinning: std::mem::take(pinning),
                qap_tables: Vec::new(),
                failure: Some(reason),
                evidence,
            });
        }
    }
    platform
        .prepare_campaign_load_admission(config, project_run_id, tables)
        .await?;
    let timing_budget = config.timing_budget()?;
    local_progress(
        config,
        &format!(
            "starting clients and waiting for DMV preflight (timeout {}s)",
            config.client_ready_timeout_seconds
        ),
    );
    platform.ensure_router_metrics_endpoint(config).await?;
    let metrics_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("create router metrics client")?;
    let readiness_started = tokio::time::Instant::now();
    platform.prepare_client_fleet(config, project_run_id).await?;
    let schedule_path = match platform.kind() {
        crate::config::OatMode::Local => "/oat-control/schedule",
        crate::config::OatMode::Aks => "/oat-schedule/schedule",
    };
    let mut handles = Vec::with_capacity(tables.len().saturating_mul(config.clients_per_table));
    for table in tables {
        for client_index in 0..config.clients_per_table {
            let mut arguments = vec![
                "--endpoint".to_owned(),
                platform.workload_endpoint(config),
                "--client-id".to_owned(),
                format!("oat-{project_run_id}-{table}-c{client_index}"),
                "--rows".to_owned(),
                config.rows_per_table.to_string(),
                "--concurrency".to_owned(),
                config.dmv_concurrency.to_string(),
                "--table".to_owned(),
                table.clone(),
                "--edge-secret-file".to_owned(),
                if platform.kind() == crate::config::OatMode::Local {
                    "/run/secrets/edge-hmac".to_owned()
                } else {
                    config.edge_secret_file.clone()
                },
                "--warmup-secs".to_owned(),
                config.warmup_seconds.to_string(),
                "--duration-secs".to_owned(),
                config.measure_seconds.to_string(),
                "--schedule-file".to_owned(),
                schedule_path.to_owned(),
                "--schedule-timeout-secs".to_owned(),
                timing_budget.schedule_wait_seconds.to_string(),
            ];
            if config.pilot_mode {
                arguments.push("--pilot".to_owned());
            }
            match platform
                .start_client(config, project_run_id, table, client_index + 1, &arguments)
                .await
            {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    let cleanup = cancel_clients(platform, config, &handles).await;
                    return match cleanup {
                        Ok(()) => Err(error).context("start complete OAT client fleet"),
                        Err(cleanup_error) => Err(error.context(format!(
                            "cancelling partially started OAT client fleet also failed: {cleanup_error:#}"
                        ))),
                    };
                }
            }
        }
    }

    loop {
        let progress = match poll_clients(platform, config, &handles).await {
            Ok(progress) => progress,
            Err(error) => {
                let cleanup = cancel_clients(platform, config, &handles).await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "cancelling clients after readiness polling failed also failed: {cleanup_error:#}"
                    ))),
                };
            }
        };
        match validate_client_readiness(
            &progress,
            readiness_started.elapsed(),
            Duration::from_secs(config.client_ready_timeout_seconds),
        ) {
            Ok(true) => break,
            Ok(false) => {}
            Err(error) => {
                let cleanup = cancel_clients(platform, config, &handles).await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "cancelling clients after readiness failure also failed: {cleanup_error:#}"
                    ))),
                };
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Delta metadata is committed during preflight, so QAP binding must precede arming.
    local_progress(
        config,
        &format!(
            "clients ready; initializing Kusto QAP (timeout {}s)",
            config.qap_timeout_seconds
        ),
    );
    let qap_result = tokio::time::timeout(
        Duration::from_secs(config.qap_timeout_seconds),
        platform.initialize_qap(config, project_run_id, tables),
    )
    .await;
    let qap_result = match qap_result {
        Ok(result) => result,
        Err(_) if local_downstream_optional(platform.kind()) => {
            platform.mark_qap_outcome_unresolved();
            Err(anyhow::anyhow!(LocalDownstreamUnavailable))
        }
        Err(_) => Err(anyhow::anyhow!(
            "run-scoped QAP preparation exceeded qap_timeout_seconds; operation outcome may be indeterminate and is not retried"
        )),
    };
    let qap_tables = match qap_result {
        Ok(qap_tables) => qap_tables,
        Err(error) if local_downstream_optional(platform.kind()) && error.is::<LocalDownstreamUnavailable>() => {
            platform.mark_qap_outcome_unresolved();
            evidence.downstream_unavailable("local_qap_initialization_unavailable");
            Vec::new()
        }
        Err(error) => {
            platform.mark_qap_outcome_unresolved();
            let cleanup = cancel_clients(platform, config, &handles).await;
            return match cleanup {
                Ok(()) => Err(error).context("initialize run-scoped QAP before client schedule"),
                Err(cleanup_error) => Err(error.context(format!(
                    "cancelling prepared OAT clients after QAP initialization failure also failed: {cleanup_error:#}"
                ))),
            };
        }
    };
    platform
        .finalize_campaign_load_admission(config, project_run_id, tables)
        .await?;
    let schedule_deadline = tokio::time::Instant::now() + Duration::from_secs(config.schedule_lead_seconds);
    let schedule_created_at_unix_ms = unix_time_ms_u64()?;
    let run_start_unix_ms = schedule_created_at_unix_ms
        .checked_add(
            config
                .schedule_lead_seconds
                .checked_mul(1_000)
                .context("OAT schedule lead overflow")?,
        )
        .context("shared OAT run start timestamp overflow")?;
    let measurement_start_unix_ms = run_start_unix_ms
        .checked_add(
            config
                .warmup_seconds
                .checked_mul(1_000)
                .context("OAT warm-up duration overflow")?,
        )
        .context("shared OAT measurement start timestamp overflow")?;
    let measurement_end_unix_ms = measurement_start_unix_ms
        .checked_add(
            config
                .measure_seconds
                .checked_mul(1_000)
                .context("OAT measurement duration overflow")?,
        )
        .context("shared OAT measurement end timestamp overflow")?;
    let delivery = tokio::time::timeout_at(schedule_deadline, async {
        platform
            .arm_clients(config, project_run_id, run_start_unix_ms)
            .await
            .context("arm common OAT client schedule")?;
        local_progress(
            config,
            &format!(
                "clients armed; schedule lead {}s, warm-up {}s, measurement {}s",
                config.schedule_lead_seconds, config.warmup_seconds, config.measure_seconds
            ),
        );
        loop {
            let progress = poll_clients(platform, config, &handles)
                .await
                .context("poll OAT client schedule acknowledgement")?;
            let observed_at = unix_time_ms_u64()?;
            let acknowledged = validate_client_schedule(&progress, observed_at, run_start_unix_ms)?;
            anyhow::ensure!(
                tokio::time::Instant::now() < schedule_deadline,
                "OAT client schedule acknowledgement exceeded the original delivery deadline"
            );
            if acknowledged {
                break Ok::<_, anyhow::Error>(observed_at);
            }
            record_owner_observation(
                pinning,
                sample_router_table_owners(platform, config, &metrics_client, tables).await,
                tables,
                false,
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await;
    let delivery = delivery.unwrap_or_else(|_| Err(anyhow::anyhow!(
        "OAT client schedule delivery/acknowledgement timed out; the arm outcome may be indeterminate and is not retried"
    )));
    let acknowledged_at_unix_ms = match delivery {
        Ok(observed_at) => observed_at,
        Err(error) => {
            let cleanup = cancel_clients(platform, config, &handles).await;
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(error.context(format!(
                    "cancelling clients after schedule delivery failure also failed: {cleanup_error:#}"
                ))),
            };
        }
    };
    evidence.client_schedule = Some(json!({
        "requested": {"schedule_lead_seconds": config.schedule_lead_seconds},
        "effective": {
            "schedule_lead_seconds": (run_start_unix_ms - schedule_created_at_unix_ms) / 1_000,
            "schedule_created_at_unix_ms": schedule_created_at_unix_ms,
            "run_start_unix_ms": run_start_unix_ms,
            "measurement_start_unix_ms": measurement_start_unix_ms,
            "measurement_end_unix_ms": measurement_end_unix_ms,
            "acknowledged_at_unix_ms": acknowledged_at_unix_ms,
            "schedule_wait_seconds": timing_budget.schedule_wait_seconds,
            "client_job_lifetime_seconds": timing_budget.client_job_lifetime_seconds,
            "minimum_pin_ttl_ms": timing_budget.minimum_pin_ttl_ms,
            "planned_cell_duration_ms": timing_budget.planned_cell_duration_ms,
        },
        "scope": "orchestrator_assigned_schedule_with_observed_full_fleet_acknowledgement",
    }));
    evidence.measurement_window = Some((measurement_start_unix_ms, measurement_end_unix_ms));
    if let Some(prepared) = &mut prepared_guard {
        prepared.engine.arm(run_start_unix_ms, measurement_end_unix_ms);
        prepared.engine.proof["run_id"] = json!(project_run_id);
        prepared.probe.since_ms = unix_time_ms_u64()?.saturating_sub(GUARD_MAX_GAP_MS);
    }
    let native = match platform.native_telemetry_source(config, &qap_tables).await {
        Ok(source) => source,
        Err(_) => {
            evidence.native_telemetry = Some(json!({
                "schema_version": 1, "unavailable_reason": "original_cell_native_source_binding_unavailable",
                "samples": [],
            }));
            None
        }
    };
    let sampler_source = match native {
        Some(native) => Some(ResourceSamplerSource::AksNative(Arc::new(native))),
        None => platform.resource_sampler_source(),
    };
    let mut sampler = ResourceSampler::start(
        sampler_source,
        config.clone(),
        measurement_start_unix_ms,
        measurement_end_unix_ms,
        prepared_guard,
    );
    let mut client_metrics = HashMap::<String, Vec<ClientMeasurement>>::with_capacity(tables.len());
    let mut completed_clients = HashSet::new();
    let mut active_clients = vec![true; handles.len()];
    let mut router_sql_start = None;
    let mut router_sql_end = None;
    let mut router_cpu_pressure_samples = Vec::new();
    let mut router_cpu_pressure_parse_failures = 0_u64;
    while completed_clients.len() < handles.len() {
        let pending_indices = handles
            .iter()
            .enumerate()
            .filter_map(|(index, handle)| (!completed_clients.contains(&handle.client_id)).then_some(index))
            .collect::<Vec<_>>();
        let pending_handles = pending_indices
            .iter()
            .map(|index| handles[*index].clone())
            .collect::<Vec<_>>();
        let progress = tokio::select! {
            biased;
            reason = sampler.wait_for_overload_stop() => {
                let cancellation = cancel_active_clients(platform, config, &handles, &active_clients).await;
                let capture = sampler.finish_early(&mut evidence).await;
                let mut failure = reason;
                if let Err(error) = cancellation {
                    failure.push_str(&format!("; client cancellation failed: {error:#}"));
                }
                if let Err(error) = capture {
                    tracing::error!(%error, "overload stop sampler evidence unavailable");
                    evidence.overload_guard = Some(json!({"status":"unavailable",
                        "unavailable_reason":"monitor_terminal_capture_failed","settings":config.overload_guard}));
                    failure.push_str("; terminal guard capture failed");
                }
                return Ok(ClientRun {
                    metrics: HashMap::new(), pinning: std::mem::take(pinning), qap_tables,
                    failure: Some(failure), evidence,
                });
            }
            progress = poll_clients(platform, config, &pending_handles) => progress,
        };
        let progress = match progress {
            Ok(progress) => progress,
            Err(error) => {
                let cleanup = cancel_active_clients(platform, config, &handles, &active_clients).await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "cancelling OAT clients after polling failure also failed: {cleanup_error:#}"
                    ))),
                };
            }
        };
        for (index, state) in pending_indices.into_iter().zip(progress) {
            let handle = &handles[index];
            if let Some(failure) = state.failure {
                let error = anyhow::anyhow!("OAT client {} failed: {failure}", handle.client_id);
                let cleanup = cancel_active_clients(platform, config, &handles, &active_clients).await;
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(error.context(format!(
                        "cancelling remaining OAT clients also failed: {cleanup_error:#}"
                    ))),
                };
            }
            if state.completed && !completed_clients.contains(&handle.client_id) {
                let output = match platform.finish_client(config, handle.clone()).await {
                    Ok(output) => output,
                    Err(error) => {
                        let cleanup = cancel_active_clients(platform, config, &handles, &active_clients).await;
                        return match cleanup {
                            Ok(()) => Err(error),
                            Err(cleanup_error) => Err(error.context(format!(
                                "cancelling clients after client collection failed also failed: {cleanup_error:#}"
                            ))),
                        };
                    }
                };
                active_clients[index] = false;
                let measurement = (|| {
                    let report = parse_client_metrics(&output)?;
                    validate_client_measurement_interval(
                        &report,
                        measurement_start_unix_ms,
                        measurement_end_unix_ms,
                        config.pilot_mode,
                    )
                    .with_context(|| format!("validate client measurement interval for {}", handle.table))?;
                    if report.rows == 0 || !report.rows_per_second.is_finite() || report.rows_per_second <= 0.0 {
                        bail!("DMV client for {} returned invalid measured throughput", handle.table);
                    }
                    let duration_ms = u64::try_from((report.duration_seconds * 1000.0).round() as u128)?.max(1);
                    Ok::<_, anyhow::Error>(ClientMeasurement {
                        metrics: ClientMetrics {
                            rows: report.rows,
                            duration_ms,
                            rows_per_second: report.rows_per_second,
                            completions: report.collector_completion.into_iter().collect(),
                        },
                        pilot_windows: report.pilot_windows,
                    })
                })();
                let measurement = match measurement {
                    Ok(measurement) => measurement,
                    Err(error) => {
                        let cleanup = cancel_active_clients(platform, config, &handles, &active_clients).await;
                        return match cleanup {
                            Ok(()) => Err(error),
                            Err(cleanup_error) => Err(error.context(format!(
                                "cancelling clients after invalid measurement also failed: {cleanup_error:#}"
                            ))),
                        };
                    }
                };
                if let Err(error) = evidence.record_client(config, &handle.table, &output) {
                    let cleanup = cancel_active_clients(platform, config, &handles, &active_clients).await;
                    return match cleanup {
                        Ok(()) => Err(error),
                        Err(cleanup_error) => Err(error.context(format!(
                            "cancelling clients after invalid effective-config evidence failed: {cleanup_error:#}"
                        ))),
                    };
                }
                client_metrics
                    .entry(handle.table.clone())
                    .or_default()
                    .push(measurement);
                completed_clients.insert(handle.client_id.clone());
            }
        }
        let sample = sample_router_metrics(platform, config, &metrics_client, tables).await;
        if let Ok(sample) = &sample {
            capture_router_sql_boundary(&mut router_sql_start, sample, measurement_start_unix_ms);
            capture_router_sql_boundary(&mut router_sql_end, sample, measurement_end_unix_ms);
            record_router_cpu_pressure_sample(
                &mut router_cpu_pressure_samples,
                &mut router_cpu_pressure_parse_failures,
                &sample.cpu_pressure,
            );
        }
        record_owner_observation(pinning, sample.map(|sample| sample.owners), tables, false);
        if completed_clients.len() < handles.len() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    let final_sample = sample_router_metrics(platform, config, &metrics_client, tables).await;
    if let Ok(sample) = &final_sample {
        capture_router_sql_boundary(&mut router_sql_start, sample, measurement_start_unix_ms);
        capture_router_sql_boundary(&mut router_sql_end, sample, measurement_end_unix_ms);
        record_router_cpu_pressure_sample(
            &mut router_cpu_pressure_samples,
            &mut router_cpu_pressure_parse_failures,
            &sample.cpu_pressure,
        );
    }
    record_owner_observation(pinning, final_sample.map(|sample| sample.owners), tables, true);
    evidence.router_cpu_pressure = Some(router_cpu_pressure_evidence(
        config
            .tunables
            .collector
            .as_ref()
            .and_then(|collector| collector.cpu_pressure)
            .unwrap_or_default(),
        &router_cpu_pressure_samples,
        router_cpu_pressure_parse_failures,
    ));
    evidence.router_sql = Some(router_sql_evidence(
        router_sql_start.as_ref(),
        router_sql_end.as_ref(),
        measurement_start_unix_ms,
        measurement_end_unix_ms,
    ));
    let mut metrics = HashMap::with_capacity(tables.len());
    sampler.finish(&mut evidence).await?;
    if evidence.overload_stop.is_some()
        || evidence
            .overload_guard
            .as_ref()
            .is_some_and(|guard| guard["status"] == "unavailable")
    {
        return Ok(ClientRun {
            metrics: HashMap::new(),
            pinning: std::mem::take(pinning),
            qap_tables,
            failure: Some("above_ceiling or overload_guard protection unavailable at terminal observation".to_owned()),
            evidence,
        });
    }
    for table in tables {
        let reports = client_metrics
            .remove(table)
            .with_context(|| format!("no DMV clients reported measurements for {table}"))?;
        if reports.len() != config.clients_per_table {
            bail!(
                "DMV table {table} returned {} of {} client measurements",
                reports.len(),
                config.clients_per_table
            );
        }
        let rows = reports
            .iter()
            .try_fold(0_u64, |sum, report| sum.checked_add(report.metrics.rows))
            .context("aggregated DMV row count overflow")?;
        let rows_per_second = reports.iter().map(|report| report.metrics.rows_per_second).sum::<f64>();
        if !rows_per_second.is_finite() || rows_per_second <= 0.0 {
            bail!("aggregated DMV throughput for {table} is invalid");
        }
        let mut windows = std::collections::BTreeMap::<u64, u64>::new();
        for report in &reports {
            for window in &report.pilot_windows {
                let rows = windows.entry(window.index).or_default();
                *rows = rows
                    .checked_add(window.rows)
                    .context("aggregated pilot row count overflow")?;
            }
        }
        if config.pilot_mode {
            let expected_windows = config.measure_seconds / 30;
            if windows.len() != expected_windows as usize || windows.keys().copied().ne(0..expected_windows) {
                bail!("pilot clients returned an incomplete 30-second window set for {table}");
            }
            let mut rates = Vec::with_capacity(windows.len());
            for (index, rows) in &windows {
                let rate = *rows as f64 / 30.0;
                rates.push(rate);
                println!(
                    "pilot_table={table} window={} window_seconds=30 rows={rows} rows_per_second={rate:.3}",
                    index + 1
                );
            }
            let stable_horizon =
                stable_window_horizon(&rates).map_or_else(|| "none".to_owned(), |seconds| seconds.to_string());
            println!("pilot_table={table} stable_from_seconds={stable_horizon}");
        } else if !windows.is_empty() {
            bail!("non-pilot DMV client unexpectedly returned pilot windows for {table}");
        }
        metrics.insert(
            table.clone(),
            ClientMetrics {
                rows,
                duration_ms: reports
                    .iter()
                    .map(|report| report.metrics.duration_ms)
                    .max()
                    .unwrap_or(1),
                rows_per_second,
                completions: reports
                    .iter()
                    .flat_map(|report| report.metrics.completions.clone())
                    .collect(),
            },
        );
    }
    Ok(ClientRun {
        metrics,
        pinning: std::mem::take(pinning),
        failure: None,
        qap_tables,
        evidence,
    })
}

fn validate_client_readiness(clients: &[ClientProgress], elapsed: Duration, timeout: Duration) -> Result<bool> {
    if let Some(failure) = clients.iter().find_map(|client| client.failure.as_deref()) {
        bail!("OAT client failed during preflight: {failure}");
    }
    if clients.iter().any(|client| client.completed && !client.ready) {
        bail!("an OAT client completed before reporting preflight readiness");
    }
    if elapsed >= timeout {
        bail!("OAT client readiness timed out; no measurement schedule was armed");
    }
    if clients.iter().all(|client| client.ready) {
        return Ok(true);
    }
    Ok(false)
}

fn validate_client_schedule(clients: &[ClientProgress], now_unix_ms: u64, run_start_unix_ms: u64) -> Result<bool> {
    if let Some(failure) = clients.iter().find_map(|client| client.failure.as_deref()) {
        bail!("OAT client failed while receiving armed schedule: {failure}");
    }
    if clients
        .iter()
        .any(|client| client.completed && !client.schedule_received)
    {
        bail!("an OAT client completed without receiving the final armed schedule");
    }
    if clients.iter().all(|client| client.schedule_received) {
        if now_unix_ms >= run_start_unix_ms {
            bail!("the full OAT fleet received its schedule after the final armed start");
        }
        return Ok(true);
    }
    if now_unix_ms >= run_start_unix_ms {
        bail!("an OAT client missed the final armed schedule; no partial window is accepted");
    }
    Ok(false)
}

fn checked_unix_time_ms(value: u128) -> Result<u64> {
    u64::try_from(value).context("Unix-millisecond timestamp exceeds the supported u64 range")
}

fn unix_time_ms_u64() -> Result<u64> {
    checked_unix_time_ms(unix_time_ms())
}

async fn poll_clients(
    platform: &dyn Platform,
    config: &OatConfig,
    handles: &[ClientHandle],
) -> Result<Vec<ClientProgress>> {
    platform.poll_client_fleet(config, handles).await
}

async fn cancel_clients(platform: &dyn Platform, config: &OatConfig, handles: &[ClientHandle]) -> Result<()> {
    let active = vec![true; handles.len()];
    cancel_active_clients(platform, config, handles, &active).await
}

async fn cancel_active_clients(
    platform: &dyn Platform,
    config: &OatConfig,
    handles: &[ClientHandle],
    active: &[bool],
) -> Result<()> {
    let results = join_all(
        handles
            .iter()
            .zip(active.iter())
            .filter(|(_, active)| **active)
            .map(|(handle, _)| platform.cancel_client(config, handle)),
    )
    .await;
    let errors = results
        .into_iter()
        .filter_map(Result::err)
        .map(|error| format!("{error:#}"))
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        bail!("one or more OAT clients could not be cancelled: {}", errors.join("; "));
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct RouterMetricSample {
    owners: HashMap<String, Vec<String>>,
    sql: RouterSqlMetricObservation,
    cpu_pressure: RouterCpuMetricObservation,
}

#[derive(Clone, Debug, Default)]
struct RouterCpuMetricObservation {
    utilization_bps: Option<u16>,
    parse_error: Option<String>,
}

fn parse_router_cpu_pressure_metrics(body: &str) -> Result<Option<u16>> {
    let mut maximum = None;
    for line in body.lines() {
        let mut fields = line.split_whitespace();
        let Some(metric) = fields.next() else { continue };
        if !metric.starts_with("arrowcol_router_collector_pressure_gauge_bps{")
            || !metric.contains("gauge=\"cpu_utilization_bps\"")
        {
            continue;
        }
        let value = fields
            .next()
            .context("CPU pressure gauge has no value")?
            .parse::<f64>()
            .context("CPU pressure gauge is not numeric")?;
        if !value.is_finite() || !(0.0..=10_000.0).contains(&value) {
            bail!("CPU pressure gauge is outside 0..=10000 basis points");
        }
        let value = value.round() as u16;
        maximum = Some(maximum.map_or(value, |current: u16| current.max(value)));
    }
    Ok(maximum)
}

fn record_router_cpu_pressure_sample(
    samples: &mut Vec<u16>,
    parse_failures: &mut u64,
    observation: &RouterCpuMetricObservation,
) {
    if observation.parse_error.is_some() {
        *parse_failures = parse_failures.saturating_add(1);
    } else if let Some(value) = observation.utilization_bps {
        samples.push(value);
    }
}

fn router_cpu_pressure_evidence(
    mode: arrowcol_tunables::CpuPressureMode,
    samples: &[u16],
    parse_failures: u64,
) -> Value {
    if mode == arrowcol_tunables::CpuPressureMode::Off {
        return json!({
            "status": "not_sampled",
            "mode": mode.as_str(),
            "cpu_utilization_bps": null,
        });
    }
    if parse_failures > 0 {
        return json!({
            "status": "unavailable",
            "mode": mode.as_str(),
            "reason": "cpu_pressure_metric_malformed",
            "parse_failures": parse_failures,
            "cpu_utilization_bps": null,
        });
    }
    let Some(maximum) = samples.iter().copied().max() else {
        return json!({
            "status": "unavailable",
            "mode": mode.as_str(),
            "reason": "cpu_pressure_metric_missing",
            "cpu_utilization_bps": null,
        });
    };
    json!({
        "status": "observed",
        "mode": mode.as_str(),
        "sample_count": samples.len(),
        "peak_cpu_utilization_bps": maximum,
    })
}

#[derive(Clone, Debug, Default)]
struct RouterSqlMetricObservation {
    snapshot: Option<RouterSqlMetricSnapshot>,
    parse_error: Option<String>,
    pre_scrape_unix_ms: u64,
    post_scrape_unix_ms: u64,
}

impl RouterSqlMetricObservation {
    fn midpoint_unix_ms(&self) -> u64 {
        self.pre_scrape_unix_ms
            .saturating_add(self.post_scrape_unix_ms.saturating_sub(self.pre_scrape_unix_ms) / 2)
    }
}

#[derive(Clone, Debug, Default)]
struct RouterSqlMetricSnapshot {
    connections_opened: u64,
    operations: BTreeMap<(String, String), u64>,
    latency_buckets: BTreeMap<(String, String), u64>,
    latency_counts: BTreeMap<String, u64>,
    latency_sums: BTreeMap<String, f64>,
    request_buckets: BTreeMap<String, u64>,
    request_count: u64,
    request_sum: f64,
}

impl RouterSqlMetricSnapshot {
    fn delta_json(&self, before: &Self) -> Option<Value> {
        let connections_opened = self.connections_opened.checked_sub(before.connections_opened)?;
        let mut operations = Vec::new();
        for (key, value) in &self.operations {
            let count = value.checked_sub(*before.operations.get(key).unwrap_or(&0))?;
            operations.push(json!({ "operation": key.0, "outcome": key.1, "count": count }));
        }
        if before.operations.keys().any(|key| !self.operations.contains_key(key)) {
            return None;
        }
        let mut operation_names = self.latency_counts.keys().cloned().collect::<BTreeSet<_>>();
        operation_names.extend(before.latency_counts.keys().cloned());
        let mut latency = Vec::new();
        for operation in operation_names {
            let count = self
                .latency_counts
                .get(&operation)
                .copied()
                .unwrap_or(0)
                .checked_sub(before.latency_counts.get(&operation).copied().unwrap_or(0))?;
            let sum_seconds = self.latency_sums.get(&operation).copied().unwrap_or(0.0)
                - before.latency_sums.get(&operation).copied().unwrap_or(0.0);
            if !sum_seconds.is_finite() || sum_seconds < 0.0 {
                return None;
            }
            let mut buckets = Vec::new();
            for ((bucket_operation, le), value) in &self.latency_buckets {
                if bucket_operation != &operation {
                    continue;
                }
                let cumulative_count = value.checked_sub(
                    *before
                        .latency_buckets
                        .get(&(bucket_operation.clone(), le.clone()))
                        .unwrap_or(&0),
                )?;
                buckets.push(json!({ "le_seconds": le, "cumulative_count": cumulative_count }));
            }
            latency.push(json!({
                "operation": operation,
                "count": count,
                "sum_seconds": sum_seconds,
                "buckets": buckets,
            }));
        }
        if before
            .latency_buckets
            .keys()
            .any(|key| !self.latency_buckets.contains_key(key))
        {
            return None;
        }
        let request_count = self.request_count.checked_sub(before.request_count)?;
        let request_sum = self.request_sum - before.request_sum;
        if !request_sum.is_finite() || request_sum < 0.0 {
            return None;
        }
        let mut request_buckets = Vec::new();
        let mut request_bucket_deltas = BTreeMap::new();
        for (le, value) in &self.request_buckets {
            let cumulative_count = value.checked_sub(*before.request_buckets.get(le).unwrap_or(&0))?;
            request_bucket_deltas.insert(le.clone(), cumulative_count);
            request_buckets.push(json!({ "le_seconds": le, "cumulative_count": cumulative_count }));
        }
        if before
            .request_buckets
            .keys()
            .any(|key| !self.request_buckets.contains_key(key))
        {
            return None;
        }
        Some(json!({
            "connections_opened": connections_opened,
            "operations": operations,
            "latency_seconds": latency,
            "request_latency": {
                "count": request_count,
                "sum_seconds": request_sum,
                "p50_upper_bound_seconds": histogram_upper_bound(&request_bucket_deltas, request_count, 0.50),
                "p99_upper_bound_seconds": histogram_upper_bound(&request_bucket_deltas, request_count, 0.99),
                "percentile_method": "first_cumulative_bucket_upper_bound",
                "buckets": request_buckets,
                "scope": "request_filter_to_upstream_response_headers_or_proxy_failure",
            },
            "scope": "measurement_window_counter_and_histogram_deltas",
        }))
    }
}

fn histogram_upper_bound(buckets: &BTreeMap<String, u64>, count: u64, quantile: f64) -> Option<f64> {
    if count == 0 {
        return None;
    }
    let target = (count as f64 * quantile).ceil() as u64;
    let mut ordered = buckets
        .iter()
        .map(|(bound, cumulative)| {
            let numeric = if bound == "+Inf" {
                f64::INFINITY
            } else {
                bound.parse().ok()?
            };
            Some((numeric, *cumulative))
        })
        .collect::<Option<Vec<_>>>()?;
    ordered.sort_by(|left, right| left.0.total_cmp(&right.0));
    ordered
        .into_iter()
        .find(|(_, cumulative)| *cumulative >= target)
        .and_then(|(bound, _)| bound.is_finite().then_some(bound))
}

fn parse_router_metric_sample(
    body: &str,
    tables: &[String],
    pre_scrape_unix_ms: u64,
    post_scrape_unix_ms: u64,
) -> Result<RouterMetricSample> {
    let owners = parse_owner_metrics(body, tables)?;
    let (snapshot, parse_error) = match parse_router_sql_metrics(body) {
        Ok(snapshot) => (Some(snapshot), None),
        Err(error) => (None, Some(format!("{error:#}"))),
    };
    let (utilization_bps, cpu_parse_error) = match parse_router_cpu_pressure_metrics(body) {
        Ok(value) => (value, None),
        Err(error) => (None, Some(format!("{error:#}"))),
    };
    Ok(RouterMetricSample {
        owners,
        cpu_pressure: RouterCpuMetricObservation {
            utilization_bps,
            parse_error: cpu_parse_error,
        },
        sql: RouterSqlMetricObservation {
            snapshot,
            parse_error,
            pre_scrape_unix_ms,
            post_scrape_unix_ms,
        },
    })
}

fn capture_router_sql_boundary(
    boundary: &mut Option<RouterSqlMetricObservation>,
    sample: &RouterMetricSample,
    measurement_boundary_unix_ms: u64,
) {
    if boundary.is_none() && sample.sql.post_scrape_unix_ms >= measurement_boundary_unix_ms {
        *boundary = Some(sample.sql.clone());
    }
}

fn router_sql_boundary_json(
    observation: Option<&RouterSqlMetricObservation>,
    measurement_boundary_unix_ms: u64,
) -> Value {
    let Some(observation) = observation else {
        return json!({ "status": "missing", "measurement_boundary_unix_ms": measurement_boundary_unix_ms });
    };
    let midpoint_unix_ms = observation.midpoint_unix_ms();
    json!({
        "status": "observed",
        "measurement_boundary_unix_ms": measurement_boundary_unix_ms,
        "pre_scrape_unix_ms": observation.pre_scrape_unix_ms,
        "post_scrape_unix_ms": observation.post_scrape_unix_ms,
        "midpoint_unix_ms": midpoint_unix_ms,
        "skew_ms": midpoint_unix_ms.abs_diff(measurement_boundary_unix_ms),
    })
}

fn router_sql_evidence(
    start: Option<&RouterSqlMetricObservation>,
    end: Option<&RouterSqlMetricObservation>,
    measurement_start_unix_ms: u64,
    measurement_end_unix_ms: u64,
) -> Value {
    let start_boundary = router_sql_boundary_json(start, measurement_start_unix_ms);
    let end_boundary = router_sql_boundary_json(end, measurement_end_unix_ms);
    let window_ms = measurement_end_unix_ms.saturating_sub(measurement_start_unix_ms);
    let max_skew_ms = 2_000_u64.max(window_ms / 20);
    let boundary_evidence = json!({
        "start": start_boundary,
        "end": end_boundary,
        "max_skew_ms": max_skew_ms,
    });
    let unavailable = |reason: &str| {
        json!({
            "status": "unavailable",
            "reason": reason,
            "boundaries": boundary_evidence,
        })
    };
    let (Some(start), Some(end)) = (start, end) else {
        return unavailable("router_sql_measurement_boundary_scrape_missing");
    };
    if let Some(reason) = start.parse_error.as_ref().or(end.parse_error.as_ref()) {
        return json!({
            "status": "unavailable",
            "reason": "router_sql_metrics_parse_failed",
            "detail": reason,
            "boundaries": boundary_evidence,
        });
    }
    let start_midpoint = start.midpoint_unix_ms();
    let end_midpoint = end.midpoint_unix_ms();
    if start_midpoint.abs_diff(measurement_start_unix_ms) > max_skew_ms
        || end_midpoint.abs_diff(measurement_end_unix_ms) > max_skew_ms
    {
        return unavailable("router_sql_scrape_skew_exceeded_bound");
    }
    let snapshot_interval_ms = end_midpoint.saturating_sub(start_midpoint);
    if snapshot_interval_ms == 0 {
        return unavailable("router_sql_snapshot_interval_was_zero");
    }
    let (Some(start_snapshot), Some(end_snapshot)) = (start.snapshot.as_ref(), end.snapshot.as_ref()) else {
        return unavailable("router_sql_metrics_snapshot_missing");
    };
    let Some(mut metrics) = end_snapshot.delta_json(start_snapshot) else {
        return unavailable("router_sql_counters_reset_or_disappeared_during_measurement");
    };
    let interval_seconds = snapshot_interval_ms as f64 / 1_000.0;
    let connections_opened = metrics["connections_opened"].as_u64().unwrap_or(0);
    metrics["snapshot_interval_ms"] = json!(snapshot_interval_ms);
    metrics["connections_opened_per_second"] = json!(connections_opened as f64 / interval_seconds);
    if let Some(operations) = metrics["operations"].as_array_mut() {
        for operation in operations {
            let count = operation["count"].as_u64().unwrap_or(0);
            operation["per_second"] = json!(count as f64 / interval_seconds);
        }
    }
    json!({ "status": "observed", "metrics": metrics, "boundaries": boundary_evidence })
}

async fn sample_router_metrics(
    platform: &mut dyn Platform,
    config: &OatConfig,
    client: &reqwest::Client,
    tables: &[String],
) -> Result<RouterMetricSample> {
    let (endpoint, _) = platform.ensure_router_metrics_endpoint(config).await?;
    let mut pre_scrape_unix_ms = unix_time_ms_u64()?;
    let body = match fetch_router_metrics(client, &endpoint).await {
        Ok(body) => body,
        Err(error) => {
            if !error.chain().any(|cause| {
                cause
                    .downcast_ref::<reqwest::Error>()
                    .is_some_and(|cause| cause.is_connect())
            }) {
                return Err(error);
            }
            let (retry_endpoint, restarted) = match platform.ensure_router_metrics_endpoint(config).await {
                Ok(endpoint) => endpoint,
                Err(recovery_error) => {
                    return Err(error.context(format!("router metrics tunnel recovery failed: {recovery_error:#}")));
                }
            };
            if !restarted {
                return Err(error);
            }
            pre_scrape_unix_ms = unix_time_ms_u64()?;
            fetch_router_metrics(client, &retry_endpoint)
                .await
                .map_err(|retry_error| {
                    retry_error.context(format!("initial router metrics sample failed: {error:#}"))
                })?
        }
    };
    let post_scrape_unix_ms = unix_time_ms_u64()?;
    parse_router_metric_sample(&body, tables, pre_scrape_unix_ms, post_scrape_unix_ms)
}

async fn fetch_router_metrics(client: &reqwest::Client, endpoint: &str) -> Result<String> {
    let response = client
        .get(endpoint)
        .send()
        .await
        .with_context(|| format!("GET router metrics {endpoint}"))?
        .error_for_status()
        .context("router metrics endpoint returned an error status")?;
    response.text().await.context("read router metrics response")
}

#[cfg(test)]
async fn sample_table_owners(
    client: &reqwest::Client,
    endpoint: &str,
    tables: &[String],
) -> Result<HashMap<String, Vec<String>>> {
    parse_owner_metrics(&fetch_router_metrics(client, endpoint).await?, tables)
}

async fn sample_router_table_owners(
    platform: &mut dyn Platform,
    config: &OatConfig,
    client: &reqwest::Client,
    tables: &[String],
) -> Result<HashMap<String, Vec<String>>> {
    Ok(sample_router_metrics(platform, config, client, tables).await?.owners)
}

fn parse_router_sql_metrics(body: &str) -> Result<RouterSqlMetricSnapshot> {
    let mut snapshot = RouterSqlMetricSnapshot::default();
    let mut connections_seen = false;
    for line in body.lines() {
        let Some((name, value)) = line.split_once(' ') else {
            continue;
        };
        if name == "arrowcol_router_sql_connections_opened_total" {
            snapshot.connections_opened = value
                .split_whitespace()
                .next()
                .context("router SQL connections metric omitted value")?
                .parse()
                .context("invalid router SQL connections metric")?;
            connections_seen = true;
            continue;
        }
        if name == "arrowcol_router_request_seconds_count" {
            snapshot.request_count = value
                .split_whitespace()
                .next()
                .context("router request latency count omitted value")?
                .parse()
                .context("invalid router request latency count")?;
            continue;
        }
        if name == "arrowcol_router_request_seconds_sum" {
            let sum: f64 = value
                .split_whitespace()
                .next()
                .context("router request latency sum omitted value")?
                .parse()
                .context("invalid router request latency sum")?;
            if !sum.is_finite() {
                bail!("invalid router request latency sum");
            }
            snapshot.request_sum = sum;
            continue;
        }
        let Some(open) = name.find('{') else {
            continue;
        };
        let close = name.rfind('}').context("malformed router SQL metric labels")?;
        let labels = parse_metric_labels(&name[open + 1..close])?;
        let metric_value = value
            .split_whitespace()
            .next()
            .context("router SQL metric omitted value")?;
        match &name[..open] {
            "arrowcol_router_sql_operations_total" => {
                let operation = labels
                    .get("operation")
                    .context("SQL operation metric omitted operation")?;
                let outcome = labels.get("outcome").context("SQL operation metric omitted outcome")?;
                let count = metric_value.parse().context("invalid SQL operation counter")?;
                if snapshot
                    .operations
                    .insert((operation.clone(), outcome.clone()), count)
                    .is_some()
                {
                    bail!("duplicate SQL operation metric series");
                }
            }
            "arrowcol_router_sql_operation_seconds_bucket" => {
                let operation = labels
                    .get("operation")
                    .context("SQL latency bucket omitted operation")?;
                let le = labels.get("le").context("SQL latency bucket omitted bound")?;
                let count = metric_value.parse().context("invalid SQL latency bucket")?;
                if snapshot
                    .latency_buckets
                    .insert((operation.clone(), le.clone()), count)
                    .is_some()
                {
                    bail!("duplicate SQL latency bucket series");
                }
            }
            "arrowcol_router_sql_operation_seconds_count" => {
                let operation = labels.get("operation").context("SQL latency count omitted operation")?;
                let count = metric_value.parse().context("invalid SQL latency count")?;
                if snapshot.latency_counts.insert(operation.clone(), count).is_some() {
                    bail!("duplicate SQL latency count series");
                }
            }
            "arrowcol_router_sql_operation_seconds_sum" => {
                let operation = labels.get("operation").context("SQL latency sum omitted operation")?;
                let sum: f64 = metric_value.parse().context("invalid SQL latency sum")?;
                if !sum.is_finite() || snapshot.latency_sums.insert(operation.clone(), sum).is_some() {
                    bail!("invalid or duplicate SQL latency sum series");
                }
            }
            "arrowcol_router_request_seconds_bucket" => {
                let le = labels
                    .get("le")
                    .context("router request latency bucket omitted bound")?;
                let count = metric_value.parse().context("invalid router request latency bucket")?;
                if snapshot.request_buckets.insert(le.clone(), count).is_some() {
                    bail!("duplicate router request latency bucket series");
                }
            }
            "arrowcol_router_request_seconds_count" => {
                snapshot.request_count = metric_value.parse().context("invalid router request latency count")?;
            }
            "arrowcol_router_request_seconds_sum" => {
                let sum: f64 = metric_value.parse().context("invalid router request latency sum")?;
                if !sum.is_finite() {
                    bail!("invalid router request latency sum");
                }
                snapshot.request_sum = sum;
            }
            _ => {}
        }
    }
    if !connections_seen {
        bail!("router metrics omitted SQL connection counter");
    }
    Ok(snapshot)
}

#[cfg(test)]
mod router_sql_metrics_tests {
    use super::{
        RouterSqlMetricObservation, RouterSqlMetricSnapshot, parse_router_metric_sample, parse_router_sql_metrics,
        router_cpu_pressure_evidence, router_sql_evidence,
    };
    use anyhow::{Context, Result, bail};

    #[test]
    fn cpu_pressure_gauge_parses_without_promoting_parse_errors_to_pin_failures() -> Result<()> {
        let tables = vec!["sqlserver_active_sessions".to_owned()];
        let body = concat!(
            "arrowcol_router_table_owners{table=\"sqlserver_active_sessions\",collector=\"collector-a\",state=\"active\"} 1\n",
            "arrowcol_router_collector_pressure_gauge_bps{gauge=\"cpu_utilization_bps\",collector=\"collector-a\"} 731\n",
        );
        let sample = parse_router_metric_sample(body, &tables, 1_000, 1_010)?;
        assert_eq!(sample.cpu_pressure.utilization_bps, Some(731));
        assert!(sample.cpu_pressure.parse_error.is_none());

        let malformed = concat!(
            "arrowcol_router_table_owners{table=\"sqlserver_active_sessions\",collector=\"collector-a\",state=\"active\"} 1\n",
            "arrowcol_router_collector_pressure_gauge_bps{gauge=\"cpu_utilization_bps\",collector=\"collector-a\"} bad\n",
        );
        let sample = parse_router_metric_sample(malformed, &tables, 1_000, 1_010)?;
        assert_eq!(sample.owners.values().next().map(Vec::len), Some(1));
        assert!(sample.cpu_pressure.parse_error.is_some());
        assert!(sample.sql.snapshot.is_none());
        Ok(())
    }

    #[test]
    fn cpu_pressure_off_is_labeled_not_sampled() {
        let evidence = router_cpu_pressure_evidence(arrowcol_tunables::CpuPressureMode::Off, &[], 0);
        assert_eq!(evidence["status"], "not_sampled");
        assert!(evidence["cpu_utilization_bps"].is_null());
    }

    #[test]
    fn owner_evidence_survives_missing_sql_counter() -> Result<()> {
        let tables = vec!["sqlserver_active_sessions".to_owned()];
        let sample = parse_router_metric_sample(
            "arrowcol_router_table_owners{table=\"sqlserver_active_sessions\",collector=\"collector-a\",state=\"active\"} 1\n",
            &tables,
            1_000,
            1_010,
        )?;
        assert_eq!(
            sample.owners.values().next().map(Vec::as_slice),
            Some(["collector-a".to_owned()].as_slice())
        );
        assert!(sample.sql.snapshot.is_none());
        assert!(
            sample
                .sql
                .parse_error
                .as_deref()
                .is_some_and(|reason| reason.contains("SQL connection counter"))
        );
        Ok(())
    }

    #[test]
    fn router_sql_evidence_reports_scrape_boundaries_rates_and_skew() {
        let start = RouterSqlMetricObservation {
            snapshot: Some(RouterSqlMetricSnapshot {
                connections_opened: 2,
                ..Default::default()
            }),
            pre_scrape_unix_ms: 9_950,
            post_scrape_unix_ms: 10_050,
            ..Default::default()
        };
        let end = RouterSqlMetricObservation {
            snapshot: Some(RouterSqlMetricSnapshot {
                connections_opened: 12,
                ..Default::default()
            }),
            pre_scrape_unix_ms: 19_950,
            post_scrape_unix_ms: 20_050,
            ..Default::default()
        };
        let evidence = router_sql_evidence(Some(&start), Some(&end), 10_000, 20_000);
        assert_eq!(evidence["status"], "observed");
        assert_eq!(evidence["metrics"]["snapshot_interval_ms"], 10_000);
        assert_eq!(evidence["metrics"]["connections_opened_per_second"], 1.0);
        assert_eq!(evidence["boundaries"]["start"]["pre_scrape_unix_ms"], 9_950);
        let late_start = RouterSqlMetricObservation {
            pre_scrape_unix_ms: 7_000,
            post_scrape_unix_ms: 7_100,
            ..start
        };
        let unavailable = router_sql_evidence(Some(&late_start), Some(&end), 10_000, 20_000);
        assert_eq!(unavailable["status"], "unavailable");
        assert_eq!(unavailable["reason"], "router_sql_scrape_skew_exceeded_bound");
    }

    #[test]
    fn parses_and_deltas_router_sql_operation_histograms() -> Result<()> {
        let before = parse_router_sql_metrics(
            "arrowcol_router_sql_connections_opened_total 3\n\
             arrowcol_router_sql_operations_total{operation=\"get_run_table_pin\",outcome=\"success\"} 4\n\
             arrowcol_router_sql_operation_seconds_bucket{operation=\"get_run_table_pin\",le=\"0.1\"} 3\n\
             arrowcol_router_sql_operation_seconds_bucket{operation=\"get_run_table_pin\",le=\"+Inf\"} 4\n\
             arrowcol_router_sql_operation_seconds_count{operation=\"get_run_table_pin\"} 4\n\
             arrowcol_router_sql_operation_seconds_sum{operation=\"get_run_table_pin\"} 0.2\n\
             arrowcol_router_request_seconds_bucket{le=\"0.1\"} 3\n\
             arrowcol_router_request_seconds_bucket{le=\"+Inf\"} 4\n\
             arrowcol_router_request_seconds_count 4\n\
             arrowcol_router_request_seconds_sum 0.2\n",
        )?;
        let after = parse_router_sql_metrics(
            "arrowcol_router_sql_connections_opened_total 7\n\
             arrowcol_router_sql_operations_total{operation=\"get_run_table_pin\",outcome=\"success\"} 10\n\
             arrowcol_router_sql_operation_seconds_bucket{operation=\"get_run_table_pin\",le=\"0.1\"} 8\n\
             arrowcol_router_sql_operation_seconds_bucket{operation=\"get_run_table_pin\",le=\"+Inf\"} 10\n\
             arrowcol_router_sql_operation_seconds_count{operation=\"get_run_table_pin\"} 10\n\
             arrowcol_router_sql_operation_seconds_sum{operation=\"get_run_table_pin\"} 1.2\n\
             arrowcol_router_request_seconds_bucket{le=\"0.1\"} 8\n\
             arrowcol_router_request_seconds_bucket{le=\"+Inf\"} 10\n\
             arrowcol_router_request_seconds_count 10\n\
             arrowcol_router_request_seconds_sum 1.2\n",
        )?;
        let Some(delta) = after.delta_json(&before) else {
            bail!("monotonic SQL metrics did not produce a delta");
        };
        assert_eq!(delta["connections_opened"], 4);
        assert_eq!(delta["operations"][0]["count"], 6);
        assert_eq!(delta["latency_seconds"][0]["count"], 6);
        let bucket = delta["latency_seconds"][0]["buckets"]
            .as_array()
            .context("missing latency buckets")?
            .iter()
            .find(|bucket| bucket["le_seconds"] == "0.1")
            .context("missing 0.1-second latency bucket")?;
        assert_eq!(bucket["cumulative_count"], 5);
        assert_eq!(delta["request_latency"]["count"], 6);
        assert_eq!(delta["request_latency"]["p50_upper_bound_seconds"], 0.1);
        assert!(delta["request_latency"]["p99_upper_bound_seconds"].is_null());
        assert!(after.delta_json(&after).is_some());
        Ok(())
    }
}

fn parse_owner_metrics(body: &str, tables: &[String]) -> Result<HashMap<String, Vec<String>>> {
    for line in body
        .lines()
        .filter(|line| line.starts_with("arrowcol_router_failures_total{"))
    {
        let close = line.find('}').context("malformed routing failure metric labels")?;
        let labels = parse_metric_labels(&line["arrowcol_router_failures_total{".len()..close])?;
        if labels.get("reason").map(String::as_str) == Some("pinning_violation") {
            let count: u64 = line[close + 1..]
                .trim()
                .parse()
                .context("invalid pinning failure counter")?;
            if count > 0 {
                bail!("pinning_violation: router rejected {count} requests for immutable ownership loss");
            }
        }
    }
    let mut owners = tables
        .iter()
        .map(|table| (canonical_table_key(table), Vec::new()))
        .collect::<HashMap<_, _>>();
    for line in body
        .lines()
        .filter(|line| line.starts_with("arrowcol_router_table_owners{"))
    {
        let close = line.find('}').context("malformed table-owner metric labels")?;
        let labels = parse_metric_labels(&line["arrowcol_router_table_owners{".len()..close])?;
        if labels.get("state").map(String::as_str) != Some("active") {
            continue;
        }
        let Some(table_name) = labels.get("table") else {
            bail!("table-owner metric omitted table label");
        };
        let Some(found) = owners.get_mut(&canonical_table_key(table_name)) else {
            continue;
        };
        let value = line[close + 1..].trim();
        if value != "1" {
            bail!("table-owner gauge for {table_name} had unexpected value {value:?}");
        }
        let collector = labels
            .get("collector")
            .context("table-owner metric omitted collector label")?;
        found.push(collector.clone());
    }
    Ok(owners)
}

fn parse_metric_labels(input: &str) -> Result<HashMap<String, String>> {
    let mut labels = HashMap::new();
    for part in split_prometheus_labels(input)? {
        let (key, value) = part.split_once('=').context("malformed table-owner metric label")?;
        let value = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .context("table-owner metric label value must be quoted")?;
        let value = unescape_prometheus_label(value)?;
        if key.is_empty() || labels.insert(key.to_owned(), value).is_some() {
            bail!("table-owner metric contains an empty or duplicate label");
        }
    }
    Ok(labels)
}

fn split_prometheus_labels(input: &str) -> Result<Vec<&str>> {
    let bytes = input.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in bytes.iter().enumerate() {
        if escaped {
            escaped = false;
        } else if quoted && *byte == b'\\' {
            escaped = true;
        } else if *byte == b'"' {
            quoted = !quoted;
        } else if *byte == b',' && !quoted {
            parts.push(&input[start..index]);
            start = index + 1;
        }
    }
    if quoted || escaped {
        bail!("unterminated escape or quote in table-owner metric labels");
    }
    if start < input.len() {
        parts.push(&input[start..]);
    }
    Ok(parts)
}

fn unescape_prometheus_label(value: &str) -> Result<String> {
    let mut unescaped = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            unescaped.push(character);
            continue;
        }
        match chars.next().context("unfinished escape in table-owner metric label")? {
            '\\' => unescaped.push('\\'),
            '"' => unescaped.push('"'),
            'n' => unescaped.push('\n'),
            other => bail!("unsupported escape \\{other} in table-owner metric label"),
        }
    }
    Ok(unescaped)
}

fn canonical_table_key(table: &str) -> String {
    table.to_ascii_lowercase()
}

fn check_owner_sample(
    tables: &[String],
    sample: &HashMap<String, Vec<String>>,
    owners: &mut HashMap<String, String>,
) -> Option<String> {
    for table in tables {
        let key = canonical_table_key(table);
        let current = sample.get(&key).map(Vec::as_slice).unwrap_or_default();
        if current.is_empty() && !owners.contains_key(&key) {
            continue;
        }

        if current.len() != 1 {
            return Some(format!(
                "table {table} has {} active ArrowCol owners; expected exactly one",
                current.len()
            ));
        }
        if let Some(previous) = owners.get(&key) {
            let current = &current[0];
            if previous != current {
                return Some(format!(
                    "table {table} changed ArrowCol owner from {previous} to {current}"
                ));
            }
        } else {
            let current = &current[0];
            owners.insert(key, current.clone());
        }
    }
    None
}

#[derive(Clone, Debug)]
struct ClientReport {
    rows: u64,
    duration_seconds: f64,
    rows_per_second: f64,
    measurement_window_start_unix_ms: u64,
    measurement_window_end_unix_ms: u64,
    measurement_started_unix_ms: u64,
    pilot_windows: Vec<PilotWindow>,
    collector_completion: Option<CollectorCompletion>,
}

fn parse_client_metrics(output: &str) -> Result<ClientReport> {
    let line = output
        .lines()
        .find(|line| line.starts_with("measured_rows="))
        .context("DMV client did not emit measured throughput")?;
    let mut fields = line.split_whitespace();
    let rows = fields
        .next()
        .and_then(|value| value.strip_prefix("measured_rows="))
        .context("DMV client measured_rows field is missing")?
        .parse::<u64>()?;
    let duration_seconds = fields
        .next()
        .and_then(|value| value.strip_prefix("measured_seconds="))
        .context("DMV client measured_seconds field is missing")?
        .parse::<f64>()?;
    let rows_per_second = fields
        .next()
        .and_then(|value| value.strip_prefix("rows_per_second="))
        .context("DMV client rows_per_second field is missing")?
        .parse::<f64>()?;
    let measurement_window_start_unix_ms = parse_metric_value(fields.next(), "measurement_window_start_unix_ms")?;
    let measurement_window_end_unix_ms = parse_metric_value(fields.next(), "measurement_window_end_unix_ms")?;
    let measurement_started_unix_ms = parse_metric_value(fields.next(), "measurement_started_unix_ms")?;
    if fields.next().is_some()
        || !duration_seconds.is_finite()
        || duration_seconds <= 0.0
        || !rows_per_second.is_finite()
        || rows_per_second < 0.0
        || measurement_window_end_unix_ms <= measurement_window_start_unix_ms
    {
        bail!("DMV client emitted invalid measured throughput or shared measurement interval");
    }
    let mut pilot_windows = Vec::new();
    for line in output.lines().filter(|line| line.starts_with("pilot_window=")) {
        let mut fields = line.split_whitespace();
        let index = parse_metric_value::<u64>(fields.next(), "pilot_window")?;
        let seconds = parse_metric_value::<u64>(fields.next(), "window_seconds")?;
        let window_rows = parse_metric_value(fields.next(), "rows")?;
        let window_rate = parse_metric_value::<f64>(fields.next(), "rows_per_second")?;
        let start_unix_ms = parse_metric_value(fields.next(), "window_start_unix_ms")?;
        if fields.next().is_some() || seconds != 30 || !window_rate.is_finite() || window_rate < 0.0 {
            bail!("DMV client emitted an invalid pilot window");
        }
        pilot_windows.push(PilotWindow {
            index: index.checked_sub(1).context("pilot window numbering starts at 1")?,
            rows: window_rows,
            start_unix_ms,
        });
    }
    if pilot_windows.windows(2).any(|pair| pair[0].index >= pair[1].index) {
        bail!("DMV client emitted duplicate or out-of-order pilot windows");
    }
    let mut completions = output
        .lines()
        .filter_map(|line| line.strip_prefix("OAT_COLLECTOR_COMPLETION "));
    let collector_completion = completions
        .next()
        .map(serde_json::from_str::<CollectorCompletion>)
        .transpose()
        .context("invalid collector completion report")?;
    if completions.next().is_some()
        || collector_completion.as_ref().is_some_and(|report| {
            report
                .best_effort_streams
                .checked_add(report.durable_streams)
                .is_none_or(|count| count == 0)
                || report.completed_at_unix_ms < measurement_window_end_unix_ms
        })
    {
        bail!("invalid or duplicate collector completion evidence");
    }
    Ok(ClientReport {
        rows,
        duration_seconds,
        rows_per_second,
        measurement_window_start_unix_ms,
        measurement_window_end_unix_ms,
        measurement_started_unix_ms,
        pilot_windows,
        collector_completion,
    })
}

fn validate_client_measurement_interval(
    report: &ClientReport,
    expected_start_unix_ms: u64,
    expected_end_unix_ms: u64,
    pilot_mode: bool,
) -> Result<()> {
    if report.measurement_window_start_unix_ms != expected_start_unix_ms
        || report.measurement_window_end_unix_ms != expected_end_unix_ms
        || report.measurement_started_unix_ms.abs_diff(expected_start_unix_ms) > 250
        || (report.duration_seconds - expected_end_unix_ms.saturating_sub(expected_start_unix_ms) as f64 / 1_000.0)
            .abs()
            > 0.0005
    {
        bail!("client did not cover the shared measurement interval");
    }
    if pilot_mode {
        let expected_windows = expected_end_unix_ms.saturating_sub(expected_start_unix_ms) / 30_000;
        if report.pilot_windows.len() != expected_windows as usize
            || report.pilot_windows.iter().enumerate().any(|(index, window)| {
                window.index != index as u64
                    || window.start_unix_ms
                        != expected_start_unix_ms.saturating_add((index as u64).saturating_mul(30_000))
            })
        {
            bail!("pilot client returned incomplete or misaligned 30-second windows");
        }
    } else if !report.pilot_windows.is_empty() {
        bail!("non-pilot client unexpectedly returned pilot windows");
    }
    Ok(())
}

fn parse_metric_value<T>(field: Option<&str>, name: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    field
        .and_then(|field| field.strip_prefix(&format!("{name}=")))
        .with_context(|| format!("DMV client pilot output is missing {name}"))?
        .parse::<T>()
        .with_context(|| format!("DMV client pilot {name} value is invalid"))
}

fn stable_window_horizon(rates: &[f64]) -> Option<usize> {
    if rates.len() < 2 {
        return None;
    }
    let observed_minimum = rates.iter().copied().fold(f64::INFINITY, f64::min);
    let observed_maximum = rates.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let observed_mean = rates.iter().sum::<f64>() / rates.len() as f64;
    if observed_mean <= 0.0 || (observed_maximum - observed_minimum) / observed_mean >= 0.05 {
        return None;
    }
    let mut minimum = f64::INFINITY;
    let mut maximum = f64::NEG_INFINITY;
    let mut sum = 0.0;
    for (index, rate) in rates.iter().copied().enumerate() {
        minimum = minimum.min(rate);
        maximum = maximum.max(rate);
        sum += rate;
        let mean = sum / (index + 1) as f64;
        if index >= 1 && mean > 0.0 && (maximum - minimum) / mean < 0.05 {
            return Some((index + 1) * 30);
        }
    }
    None
}

async fn observe_qap(
    endpoint: &str,
    mode: &arrowcol_loadgen::ObservationMode,
    database: &str,
    run_id: &str,
    qap_tables: &[String],
    timeout_seconds: u64,
) -> Result<(QapMetrics, Value)> {
    let results = std::path::PathBuf::from("out/arrowcol-oat/qap").join(run_id);
    let endpoint = endpoint.to_owned();
    let database = database.to_owned();
    let mode = *mode;
    let observer_tables = qap_tables.to_vec();
    let timeout = Duration::from_secs(timeout_seconds);
    let observation = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&results)
            .with_context(|| format!("create QAP results directory {}", results.display()))?;
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        runtime
            .block_on(arrowcol_loadgen::observe_qap_until_ready(
                &endpoint,
                &results,
                mode,
                &database,
                &observer_tables,
                timeout,
            ))
            .map_err(|error| {
                if matches!(mode, arrowcol_loadgen::ObservationMode::Local) {
                    classify_local_qap_error(error)
                } else {
                    error
                }
            })
            .context("observe Kusto Query Acceleration Policy")
    })
    .await
    .context("join Kusto QAP observer")??;
    if let Some(tables) = observation.get("tables").and_then(Value::as_object) {
        let failures = tables
            .values()
            .filter(|table| table["available"] == false)
            .collect::<Vec<_>>();
        if !failures.is_empty() {
            if matches!(mode, arrowcol_loadgen::ObservationMode::Local)
                && failures.iter().all(|table| table["temporary_unavailable"] == true)
            {
                return Err(anyhow::anyhow!(LocalDownstreamUnavailable));
            }
            bail!("QAP table metadata/statistics observation failed; not a temporary service-availability failure");
        }
    }
    let metrics = aggregate_qap(&observation, qap_tables)?;
    let tables = qap_tables
        .iter()
        .map(|name| {
            (
                name.clone(),
                json!({
                    "available": observation["tables"][name]["available"],
                    "row_count": observation["tables"][name]["state"]["RowCount"],
                    "table_version": observation["tables"][name]["state"]["TableVersion"],
                    "completion_percent": observation["tables"][name]["statistics"]["AccelerationCompletePercentage"],
                    "pending_files": observation["tables"][name]["statistics"]["AccelerationPendingDataFilesCount"],
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    Ok((
        metrics,
        json!({
            "run_id": run_id, "expected_tables": qap_tables,
            "observed_unix_ms": observation["observed_unix_ms"],
            "catch_up_status": observation["catch_up_status"], "tables": tables,
        }),
    ))
}

fn apply_qap_observation(
    mode: crate::config::OatMode,
    metrics: QapMetrics,
    observation: Value,
    evidence: &mut CellEvidence,
) -> QapMetrics {
    if local_downstream_optional(mode) && observation["catch_up_status"] == "deadline_incomplete" {
        evidence.downstream_unavailable(format!(
            "Local QAP catch-up deadline expired: observed completion {}%, pending files {}. \
             Derived Kusto/QAP metrics are null; Delta commit measurements are unchanged.",
            metrics
                .completion_percent
                .map_or_else(|| "unknown".to_owned(), |value| value.to_string()),
            metrics
                .pending_artifacts
                .map_or_else(|| "unknown".to_owned(), |value| value.to_string()),
        ));
        evidence.qap_observation = Some(observation);
        return QapMetrics::default();
    }
    evidence.qap_observation = Some(observation);
    metrics
}

fn parse_qap_timestamp(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.timestamp_millis())
        .or_else(|_| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .map(|timestamp| timestamp.and_utc().timestamp_millis())
        })
        .ok()
}

fn aggregate_qap(observation: &Value, expected_tables: &[String]) -> Result<QapMetrics> {
    let expected = observation
        .get("expected_tables")
        .and_then(Value::as_array)
        .context("QAP observation has no expected_tables array")?
        .iter()
        .map(|table| table.as_str().context("QAP expected table name must be a string"))
        .collect::<Result<Vec<_>>>()?;
    if expected != expected_tables.iter().map(String::as_str).collect::<Vec<_>>() {
        bail!("QAP observation returned an unexpected table set");
    }
    let tables = observation
        .get("tables")
        .and_then(Value::as_object)
        .context("QAP observation has no tables object")?;
    let observed_unix_ms = observation
        .get("observed_unix_ms")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .context("QAP observation timestamp is invalid")?;
    let mut available = Vec::new();
    for name in expected_tables {
        let table = tables
            .get(name)
            .with_context(|| format!("QAP observation omitted expected table {name}"))?;
        if table.get("available").and_then(Value::as_bool) == Some(true) {
            available.push(table);
        }
    }
    let all_available = available.len() == expected_tables.len();
    let completion_values = available
        .iter()
        .map(|table| {
            table
                .pointer("/statistics/AccelerationCompletePercentage")
                .and_then(Value::as_f64)
        })
        .collect::<Option<Vec<_>>>();
    let completion_percent = completion_values
        .and_then(|values| {
            values
                .iter()
                .all(|value| (0.0..=100.0).contains(value))
                .then(|| values.into_iter().fold(f64::INFINITY, f64::min))
        })
        .filter(|_| all_available);
    let pending_artifacts =
        sum_qap_values(&available, "/statistics/AccelerationPendingDataFilesCount").filter(|_| all_available);
    let cached_bytes = sum_qap_values(&available, "/statistics/HotSize").filter(|_| all_available);
    let health = available
        .iter()
        .map(|table| table.pointer("/state/IsHealthy").and_then(Value::as_bool))
        .collect::<Option<Vec<_>>>();
    let unhealthy_catalogs = health
        .map(|values| values.into_iter().filter(|healthy| !healthy).count() as u64)
        .filter(|_| all_available);
    let lag_values = available
        .iter()
        .map(|table| {
            let updated = table
                .pointer("/statistics/LastUpdatedDateTime")
                .and_then(Value::as_str)?;
            let timestamp = parse_qap_timestamp(updated)?;
            Some((observed_unix_ms as f64 - timestamp as f64) / 1000.0)
        })
        .collect::<Option<Vec<_>>>();
    let lag_seconds = lag_values
        .filter(|values| values.iter().all(|value| value.is_finite() && *value >= 0.0))
        .map(|values| values.into_iter().fold(0.0, f64::max))
        .filter(|_| all_available);
    let observation_complete = u64::from(
        completion_percent.is_some()
            && pending_artifacts.is_some()
            && cached_bytes.is_some()
            && unhealthy_catalogs.is_some()
            && lag_seconds.is_some(),
    );
    Ok(QapMetrics {
        observation_complete,
        observed_tables: available.len() as u64,
        completion_percent,
        pending_artifacts,
        cached_bytes,
        unhealthy_catalogs,
        lag_seconds,
    })
}

fn sum_qap_values(tables: &[&Value], pointer: &str) -> Option<u64> {
    tables
        .iter()
        .try_fold(0_u64, |sum, table| sum.checked_add(table.pointer(pointer)?.as_u64()?))
}

fn scoped_compose_project(base: &str, run_id: &str) -> String {
    let suffix = run_id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();
    let prefix_limit = 48_usize.saturating_sub(suffix.len() + 1);
    format!(
        "{}-{suffix}",
        base.chars()
            .take(prefix_limit)
            .collect::<String>()
            .trim_end_matches(['-', '_'])
    )
}

fn compose_command(compose_file: &str, override_file: Option<&str>, project: &str, profile: &str) -> Command {
    let mut command = Command::new("docker");
    command.args([
        "compose",
        "--progress",
        "plain",
        "--ansi",
        "never",
        "--env-file",
        "/dev/null",
        "--file",
        compose_file,
    ]);
    if let Some(override_file) = override_file {
        command.args(["--file", override_file]);
    }
    command.args(["--project-name", project, "--profile", profile]);
    command
}

fn set_local_pin_init_config(command: &mut Command, config_path: &Path) {
    command.env("OAT_PIN_INIT_CONFIG_HOST_PATH", config_path);
}

async fn run_command(mut command: Command, action: &str) -> Result<std::process::Output> {
    let output = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .output()
        .await
        .with_context(|| format!("{action}: start process"))?;
    if !output.status.success() {
        bail!(
            "{action} failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output)
}

fn local_progress(config: &OatConfig, message: &str) {
    if config.platform.kind == crate::config::OatMode::Local {
        eprintln!("[oat] {message}");
    }
}

async fn stream_command_output(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    mut writer: impl Write,
) -> std::io::Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(output);
        }
        writer.write_all(&buffer[..count])?;
        writer.flush()?;
        output.extend_from_slice(&buffer[..count]);
    }
}

async fn run_streaming_command_to(
    mut command: Command,
    action: &str,
    stdout: impl Write,
    stderr: impl Write,
) -> Result<()> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("{action}: start process"))?;
    let child_stdout = child.stdout.take().context("command stdout is missing")?;
    let child_stderr = child.stderr.take().context("command stderr is missing")?;
    let (status, _, stderr) = tokio::try_join!(
        child.wait(),
        stream_command_output(child_stdout, stdout),
        stream_command_output(child_stderr, stderr),
    )
    .with_context(|| format!("{action}: stream process output"))?;
    if !status.success() {
        bail!(
            "{action} failed with {status}: {}",
            String::from_utf8_lossy(&stderr).trim()
        );
    }
    Ok(())
}

async fn run_streaming_command(command: Command, action: &str) -> Result<()> {
    eprintln!("[oat] {action}...");
    let started = tokio::time::Instant::now();
    run_streaming_command_to(command, action, std::io::stdout(), std::io::stderr()).await?;
    eprintln!("[oat] {action}: done ({:.1}s)", started.elapsed().as_secs_f64());
    Ok(())
}

fn pin_init_config_json(
    run_id: &str,
    expected_keys_json: &str,
    expires_at_unix_ms: u64,
    now_unix_ms: u64,
    sql_host: &str,
    router_yaml: &str,
    run_table: Option<(&str, &str)>,
) -> Result<String> {
    let parsed = uuid::Uuid::parse_str(run_id).context("pin initialization requires a UUID run ID")?;
    if parsed.to_string() != run_id {
        bail!("pin initialization requires a canonical lowercase UUID run ID");
    }
    if expires_at_unix_ms <= now_unix_ms {
        bail!("pin initialization expiry must be later than its start time");
    }
    let pin_set: Value =
        serde_json::from_str(expected_keys_json).context("parse expected OAT table pin-set configuration")?;
    let expected_keys = pin_set
        .get("expected_keys")
        .and_then(Value::as_array)
        .filter(|keys| !keys.is_empty())
        .context("expected OAT pin set has no expected_keys")?;
    let expected_digest = pin_set
        .get("expected_digest")
        .and_then(Value::as_str)
        .context("expected OAT pin set has no expected_digest")?;
    if pin_set.as_object().is_none_or(|object| object.len() != 2)
        || expected_digest.len() != 64
        || !expected_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("expected OAT pin set must contain only expected_keys and lowercase SHA-256 expected_digest");
    }
    let routing_policy = routing_policy_from_yaml(router_yaml, run_id)?;
    if expires_at_unix_ms - now_unix_ms > routing_policy.oat_run_pin_ttl_ms {
        bail!("OAT pin expiry exceeds the selected router's configured run-pin lifetime");
    }
    Ok(serde_json::to_string(&json!({
        "schema_version": 1,
        "run_id": run_id,
        "expires_at_unix_ms": expires_at_unix_ms,
        "expected_keys": expected_keys,
        "expected_digest": expected_digest,
        "routing_policy": routing_policy,
        "sql_server": {
            "host": sql_host,
            "port": 1433,
            "trust_server_certificate": true,
        },
        "run_table": run_table.map(|(delta_root, table)| json!({
            "delta_root": delta_root,
            "table": table,
        })),
    }))?)
}

async fn run_local_pin_setup(
    config: &OatConfig,
    platform: &LocalPlatform,
    run_id: &str,
    expected_keys_json: &str,
    expires_at_unix_ms: u64,
    now_unix_ms: u64,
) -> Result<()> {
    let router_yaml_path = platform.state_dir.join("router-state/config.yaml");
    let router_yaml = tokio::fs::read_to_string(&router_yaml_path)
        .await
        .with_context(|| format!("read rendered local router config {}", router_yaml_path.display()))?;
    let pin_config = pin_init_config_json(
        run_id,
        expected_keys_json,
        expires_at_unix_ms,
        now_unix_ms,
        "sqlserver",
        &router_yaml,
        None,
    )?;
    let config_path = platform.state_dir.join("pin-init.json");
    write_private_secret(&config_path, &pin_config).context("write private local OAT pin initialization config")?;
    let project = scoped_compose_project(
        config
            .platform
            .compose_project
            .as_deref()
            .context("local compose project name is missing")?,
        &platform.run_id,
    );
    let mut command = compose_command(
        config
            .platform
            .compose_file
            .as_deref()
            .context("local compose file is missing")?,
        config.platform.compose_override_file.as_deref(),
        &project,
        "oat",
    );
    platform.add_compose_environment(&mut command);
    set_local_pin_init_config(&mut command, &config_path);
    command.args(["run", "--rm", "--no-deps", "oat-pin-init"]);
    match tokio::time::timeout(
        Duration::from_secs(crate::config::PIN_INIT_SETUP_TIMEOUT_SECONDS),
        run_streaming_command(command, "initialize local immutable OAT table pin set"),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!(
            "local OAT pin initialization exceeded its bounded setup lifetime; outcome may be indeterminate and is not retried"
        )),
    }
    .context("run local OAT pin initialization with the separate migrator credential")
    .map(|_| ())
}

fn run_table_initializer_cloud_env(config: &OatConfig, platform: &AksPlatform, run_id: &str) -> Result<Value> {
    let runtime_path = platform
        .runtime_dir
        .as_ref()
        .context("run-table initializer requires the validated run runtime directory")?
        .join("runtime-config.json");
    let runtime: Value = serde_json::from_slice(&fs::read(runtime_path)?)
        .context("read validated run-table initializer runtime config")?;
    let stamp = runtime["stamp"].as_str().context("run runtime has no Azure stamp")?;
    let subscription = runtime["subscriptionId"]
        .as_str()
        .context("run runtime has no Azure subscription")?;
    let subscription_id =
        uuid::Uuid::parse_str(subscription).context("run runtime Azure subscription is not a UUID")?;
    if subscription_id.is_nil()
        || stamp.len() != 5
        || !stamp.bytes().all(|byte| byte.is_ascii_digit())
        || config.stamp.as_deref() != Some(stamp)
    {
        bail!("run-table initializer Azure ownership does not match the validated campaign stamp");
    }
    let root = platform
        .delta_root
        .as_deref()
        .context("run-table initializer lost its Delta root")?;
    if runtime["deltaRoot"].as_str() != Some(root)
        || runtime["namespace"].as_str() != Some(config.namespace.as_str())
        || runtime["oatRunId"].as_str() != Some(run_id)
        || platform.run_id != run_id
    {
        bail!("run-table initializer runtime identity does not match the rendered collector");
    }
    let account = format!("arrowcollector{stamp}");
    let url = Url::parse(root).context("parse run-table initializer Delta root")?;
    if url.scheme() != "abfss" || url.host_str() != Some(format!("{account}.dfs.core.windows.net").as_str()) {
        bail!("run-table initializer Delta account does not match the validated Azure stamp");
    }
    Ok(json!([
        { "name": "ARROWCOL_DELTA_ROOT", "value": root },
        { "name": "ARROWCOL_AZURE_MODE", "value": "stamped" },
        { "name": "AZURE_SUBSCRIPTION_ID", "value": subscription },
        { "name": "AZURE_STAMP", "value": stamp },
        { "name": "AZURE_RESOURCE_GROUP", "value": "arrow-collector-rg" },
        { "name": "AZURE_STORAGE_ACCOUNT", "value": account },
        { "name": "AZURE_STORAGE_CHECKPOINT_CONTAINER", "value": "checkpoints" },
        { "name": "ARROWCOL_RESET_GENERATION", "valueFrom": { "secretKeyRef": { "name": "arrowcol-generation", "key": "generation" } } },
    ]))
}

fn created_pin_init_job_uid(output: &str, namespace: &str, job_name: &str, run_id: &str) -> Result<String> {
    let mut resources = Vec::new();
    for value in serde_json::Deserializer::from_str(output).into_iter::<Value>() {
        let value = value.context("parse created pin initialization resources")?;
        if value["kind"] == "List" {
            resources.extend(
                value["items"]
                    .as_array()
                    .context("created resource List has no items")?
                    .clone(),
            );
        } else {
            resources.push(value);
        }
    }
    let jobs = resources
        .iter()
        .filter(|value| value["kind"] == "Job")
        .collect::<Vec<_>>();
    let [job] = jobs.as_slice() else {
        bail!("created pin initialization output must identify exactly one Job");
    };
    if job["metadata"]["name"] != job_name
        || job["metadata"]["namespace"] != namespace
        || job["metadata"]["labels"]["arrowcol-oat-run"] != uuid::Uuid::parse_str(run_id)?.simple().to_string()
    {
        bail!("created pin initialization Job identity does not match this run");
    }
    let uid = job["metadata"]["uid"]
        .as_str()
        .filter(|uid| !uid.is_empty())
        .context("created pin Job has no UID")?;
    uuid::Uuid::parse_str(uid).context("created pin Job UID is not a UUID")?;
    Ok(uid.to_owned())
}

async fn run_aks_pin_setup_job(
    config: &OatConfig,
    platform: &mut AksPlatform,
    run_id: &str,
    expected_keys_json: &str,
    expires_at_unix_ms: u64,
    now_unix_ms: u64,
) -> Result<()> {
    let router_yaml = platform
        .router_yaml
        .as_deref()
        .context("AKS pin initialization requires the exact rendered router config")?;
    let run_table_owned = if config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
        let [table] = config.tables.as_slice() else {
            bail!("scale_out_single_table pin initialization requires exactly one hot table");
        };
        Some((
            platform
                .delta_root
                .clone()
                .context("scale-out run-scoped Delta root is unavailable")?,
            canonical_dmv_table_name(table)?,
        ))
    } else {
        None
    };
    let run_table = run_table_owned
        .as_ref()
        .map(|(root, table)| (root.as_str(), table.as_str()));
    let pin_config = pin_init_config_json(
        run_id,
        expected_keys_json,
        expires_at_unix_ms,
        now_unix_ms,
        "arrowcol-sql",
        router_yaml,
        run_table,
    )?;
    let run_suffix = uuid::Uuid::parse_str(run_id)?.simple().to_string();
    let config_map_name = format!("oat-pin-init-config-{run_suffix}");
    let job_name = format!("oat-pin-init-{run_suffix}");
    let labels = json!({
        "app": "arrowcol-oat-pin-init",
        "arrowcol-oat-run": run_suffix,
    });
    let service_account = platform
        .service_account
        .as_deref()
        .context("AKS pin initialization requires the actual rendered runtime ServiceAccount")?;
    let resources = json!({
        "requests": { "cpu": "100m", "memory": "128Mi" },
        "limits": { "cpu": "500m", "memory": "512Mi" },
    });
    let run_table_identity = run_table.is_some();
    let pod_labels = if run_table_identity {
        json!({ "labels": { "app": "arrowcol-oat-pin-init", "arrowcol-oat-run": run_suffix, "azure.workload.identity/use": "true" } })
    } else {
        json!({ "labels": labels })
    };
    let pod_env = if run_table_identity {
        run_table_initializer_cloud_env(config, platform, run_id)?
    } else {
        json!([])
    };
    let mut payload_items = json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [{
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": config_map_name,
                "namespace": config.namespace,
                "labels": labels,
            },
            "data": { "pin-init.json": pin_config },
        }, {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": config.namespace,
                "labels": labels,
            },
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": crate::config::PIN_INIT_SETUP_TIMEOUT_SECONDS,
                "template": {
                    "metadata": { "labels": labels },
                    "spec": {
                        "serviceAccountName": service_account,
                        "automountServiceAccountToken": false,
                        "enableServiceLinks": false,
                        "restartPolicy": "Never",
                        "terminationGracePeriodSeconds": 10,
                        "nodeSelector": {
                            "arrow-collector/role": "arrowcol",
                            "kubernetes.io/arch": "amd64",
                        },
                        "tolerations": [{
                            "key": "arrow-collector/role",
                            "operator": "Equal",
                            "value": "arrowcol",
                            "effect": "NoSchedule",
                        }],
                        "securityContext": {
                            "runAsNonRoot": true,
                            "runAsUser": 10001,
                            "runAsGroup": 10001,
                            "fsGroup": 10001,
                            "fsGroupChangePolicy": "OnRootMismatch",
                            "seccompProfile": { "type": "RuntimeDefault" },
                        },
                        "containers": [{
                            "name": "pin-init",
                            "image": config.dmv_client_image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/usr/local/bin/arrowcol-oat-pin-init"],
                            "args": [
                                "--config",
                                "/run/arrowcol-oat/pin-init.json",
                                "--migrator-password-file",
                                "/run/migration/password",
                            ],
                            "resources": resources,
                            "securityContext": {
                                "allowPrivilegeEscalation": false,
                                "capabilities": { "drop": ["ALL"] },
                                "readOnlyRootFilesystem": true,
                            },
                            "volumeMounts": [{
                                "name": "pin-config",
                                "mountPath": "/run/arrowcol-oat",
                                "readOnly": true,
                            }, {
                                "name": "migration-password",
                                "mountPath": "/run/migration",
                                "readOnly": true,
                            }, {
                                "name": "pin-init-tmp",
                                "mountPath": "/tmp",
                            }],
                        }],
                        "volumes": [{
                            "name": "pin-config",
                            "configMap": {
                                "name": config_map_name,
                                "items": [{ "key": "pin-init.json", "path": "pin-init.json" }],
                            },
                        }, {
                            "name": "migration-password",
                            "secret": {
                                "secretName": "arrowcol-sql-migration",
                                "defaultMode": 288,
                                "items": [{ "key": "password", "path": "password" }],
                            },
                        }, {
                            "name": "pin-init-tmp",
                            "emptyDir": { "sizeLimit": "64Mi" },
                        }],
                    },
                },
            },
        }],
    });
    if run_table_identity {
        let template = payload_items
            .pointer_mut("/items/1/spec/template")
            .context("pin-init Job template is missing")?;
        template["metadata"] = pod_labels;
        template["spec"]["containers"][0]["env"] = pod_env;
    }
    if run_table.is_some() {
        platform.run_table_initialization_attempted = true;
    }
    let payload = serde_json::to_vec(&payload_items)?;
    let mut manifest: Value = serde_json::from_slice(&payload).context("parse pin initialization resources")?;
    let items = manifest["items"]
        .as_array_mut()
        .context("pin initialization resource list is missing")?;
    prepare_run_owned_resources(config, platform, items).await?;
    let planned_resources = items.clone();
    let created = kubectl(
        config,
        vec!["create", "-f", "-", "-o", "json"],
        Some(serde_json::to_vec(&manifest)?),
    )
    .await
    .context("create finite AKS OAT pin initialization Job")?;
    let job_uid = if run_table_identity {
        let uid = created_pin_init_job_uid(&created, &config.namespace, &job_name, run_id)?;
        let evidence_path = platform
            .runtime_dir
            .as_ref()
            .context("pin Job identity requires runtime directory")?
            .join("run-table-initializer-job.json");
        fs::write(
            evidence_path,
            serde_json::to_vec_pretty(&json!({
                "run_id":run_id,"namespace":config.namespace,"job_name":job_name,"job_uid":uid
            }))?,
        )?;
        Some(uid)
    } else {
        None
    };
    record_applied_resource_uids(config, platform, &planned_resources).await?;

    let expected_table = run_table.map(|(_, table)| table);
    wait_for_aks_pin_setup(
        config,
        platform,
        &job_name,
        job_uid.as_deref(),
        run_id,
        expected_table,
        tokio::time::Instant::now() + Duration::from_secs(200),
    )
    .await?;
    if run_table.is_some() {
        platform.run_table_initialization_drained = true;
    }
    kubectl(
        config,
        vec![
            "delete",
            "job",
            &job_name,
            "--namespace",
            &config.namespace,
            "--wait=true",
        ],
        None,
    )
    .await
    .context("remove completed AKS OAT pin initialization Job")?;
    kubectl(
        config,
        vec![
            "delete",
            "configmap",
            &config_map_name,
            "--namespace",
            &config.namespace,
            "--wait=true",
        ],
        None,
    )
    .await
    .context("remove completed AKS OAT pin initialization ConfigMap")?;
    Ok(())
}

fn pin_init_job_failure_diagnostics(job: &Value, pods: &Value) -> String {
    let mut details = Vec::new();
    if let Some(conditions) = job.pointer("/status/conditions").and_then(Value::as_array) {
        for condition in conditions.iter().filter(|condition| condition["status"] == "True") {
            details.push(format!(
                "Job {}: {} {}",
                condition["type"].as_str().unwrap_or("condition"),
                condition["reason"].as_str().unwrap_or("unknown"),
                condition["message"]
                    .as_str()
                    .unwrap_or("")
                    .chars()
                    .take(240)
                    .collect::<String>(),
            ));
        }
    }
    if let Some(items) = pods["items"].as_array() {
        for pod in items {
            let name = pod
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let phase = pod
                .pointer("/status/phase")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            details.push(format!("Pod {name}: phase={phase}"));
            if let Some(conditions) = pod.pointer("/status/conditions").and_then(Value::as_array) {
                for condition in conditions.iter().filter(|condition| condition["status"] == "False") {
                    details.push(format!(
                        "Pod {name} {}: {} {}",
                        condition["type"].as_str().unwrap_or("condition"),
                        condition["reason"].as_str().unwrap_or("unknown"),
                        condition["message"]
                            .as_str()
                            .unwrap_or("")
                            .chars()
                            .take(240)
                            .collect::<String>(),
                    ));
                }
            }
            if let Some(containers) = pod.pointer("/status/containerStatuses").and_then(Value::as_array) {
                for container in containers {
                    let name = container["name"].as_str().unwrap_or("container");
                    if let Some(waiting) = container.pointer("/state/waiting") {
                        details.push(format!(
                            "Container {name}: waiting={}",
                            waiting["reason"].as_str().unwrap_or("unknown")
                        ));
                    }
                    if let Some(terminated) = container.pointer("/state/terminated") {
                        details.push(format!(
                            "Container {name}: terminated={} exitCode={}",
                            terminated["reason"].as_str().unwrap_or("unknown"),
                            terminated["exitCode"].as_i64().unwrap_or_default()
                        ));
                    }
                }
            }
        }
    }
    if details.is_empty() {
        "no Kubernetes Job or Pod failure details were available".to_owned()
    } else {
        details.join("; ")
    }
}

fn failed_initializer_termination_evidence(
    namespace: &str,
    job_name: &str,
    job_uid: &str,
    job: &Value,
    pods: &Value,
) -> Result<Value> {
    if job.pointer("/metadata/uid").and_then(Value::as_str) != Some(job_uid)
        || job.pointer("/status/active").and_then(Value::as_u64) != Some(0)
        || job.pointer("/status/ready").and_then(Value::as_u64) != Some(0)
        || !job
            .pointer("/status/conditions")
            .and_then(Value::as_array)
            .is_some_and(|conditions| {
                conditions
                    .iter()
                    .any(|condition| condition["type"] == "Failed" && condition["status"] == "True")
            })
    {
        bail!("initializer Job lacks UID-bound Failed=True, active=0, ready=0 termination proof");
    }
    let pods = pods["items"]
        .as_array()
        .filter(|pods| !pods.is_empty())
        .context("initializer pod list is empty or missing")?;
    let mut evidence = Vec::new();
    for pod in pods {
        let pod_uid = pod
            .pointer("/metadata/uid")
            .and_then(Value::as_str)
            .filter(|uid| !uid.is_empty())
            .context("initializer pod UID missing")?;
        if pod.pointer("/metadata/namespace").and_then(Value::as_str) != Some(namespace)
            || pod.pointer("/metadata/labels/controller-uid").and_then(Value::as_str) != Some(job_uid)
            || !pod.pointer("/metadata/ownerReferences").and_then(Value::as_array).is_some_and(|owners| {
                let controllers = owners.iter().filter(|owner| owner["controller"] == true).collect::<Vec<_>>();
                matches!(controllers.as_slice(), [owner] if owner["kind"] == "Job" && owner["name"] == job_name && owner["uid"] == job_uid)
            })
            || !matches!(pod.pointer("/status/phase").and_then(Value::as_str), Some("Failed" | "Succeeded"))
            || pod.pointer("/spec/ephemeralContainers").is_some_and(|value| value.as_array().is_none_or(|items| !items.is_empty()))
        {
            bail!("initializer pod {pod_uid} lacks same-controller terminal phase proof");
        }
        let mut containers = Vec::new();
        for (spec_key, status_key, required) in [
            ("containers", "containerStatuses", true),
            ("initContainers", "initContainerStatuses", false),
        ] {
            let declared = match pod["spec"].get(spec_key) {
                Some(value) => value
                    .as_array()
                    .context("initializer container declarations are malformed")?
                    .as_slice(),
                None if !required => &[],
                None => bail!("initializer application containers are missing"),
            };
            if required && declared.is_empty() {
                bail!("initializer application container list is empty");
            }
            let statuses = match pod["status"].get(status_key) {
                Some(value) => value
                    .as_array()
                    .context("initializer container status list is malformed")?
                    .as_slice(),
                None if declared.is_empty() => &[],
                None => bail!("initializer container statuses are missing"),
            };
            if statuses.len() != declared.len() {
                bail!("initializer container status coverage is incomplete");
            }
            let mut names = BTreeSet::new();
            for container in declared {
                let name = container["name"]
                    .as_str()
                    .filter(|name| !name.is_empty())
                    .context("initializer container name missing")?;
                if !names.insert(name) {
                    bail!("initializer container declarations contain duplicate names");
                }
                let matches = statuses
                    .iter()
                    .filter(|status| status["name"] == name)
                    .collect::<Vec<_>>();
                let [status] = matches.as_slice() else {
                    bail!("initializer container {name} has no unique kubelet status");
                };
                let state = status["state"]
                    .as_object()
                    .context("initializer container state missing")?;
                if state.len() != 1 || !state.contains_key("terminated") {
                    bail!("initializer container {name} is not positively terminated");
                }
                let terminated = &state["terminated"];
                let container_id = terminated["containerID"]
                    .as_str()
                    .filter(|id| !id.is_empty())
                    .context("terminated initializer containerID missing")?;
                let exit_code = terminated["exitCode"]
                    .as_i64()
                    .context("terminated initializer exitCode missing")?;
                let finished_at = terminated["finishedAt"]
                    .as_str()
                    .context("terminated initializer finishedAt missing")?;
                let finished =
                    chrono::DateTime::parse_from_rfc3339(finished_at).context("initializer finishedAt is invalid")?;
                if finished.timestamp_millis() <= 0 {
                    bail!("initializer finishedAt is not a positive termination timestamp");
                }
                containers.push(
                    json!({"name":name,"container_id":container_id,"exit_code":exit_code,"finished_at":finished_at}),
                );
            }
        }
        evidence.push(json!({"pod_uid":pod_uid,"phase":pod["status"]["phase"],"containers":containers}));
    }
    Ok(json!({
        "kind":"initializer_terminated_without_receipt","namespace":namespace,
        "job_name":job_name,"job_uid":job_uid,"job_status":job["status"],"pods":evidence
    }))
}

async fn wait_for_aks_pin_setup(
    config: &OatConfig,
    platform: &mut AksPlatform,
    job_name: &str,
    job_uid: Option<&str>,
    run_id: &str,
    expected_table: Option<&str>,
    deadline: tokio::time::Instant,
) -> Result<()> {
    loop {
        let output = kubectl(
            config,
            vec!["get", "job", job_name, "--namespace", &config.namespace, "-o", "json"],
            None,
        )
        .await
        .with_context(|| format!("inspect AKS OAT pin initialization Job {job_name}"))?;
        let job: Value = serde_json::from_str(&output).context("parse AKS OAT pin initialization Job")?;
        if job_uid.is_some_and(|uid| job.pointer("/metadata/uid").and_then(Value::as_str) != Some(uid)) {
            bail!("AKS pin initialization Job UID changed; publisher termination is unconfirmed");
        }
        if job.pointer("/status/succeeded").and_then(Value::as_u64).unwrap_or(0) > 0 {
            break;
        }
        let failed = job.pointer("/status/failed").and_then(Value::as_u64).unwrap_or(0) > 0
            || job
                .pointer("/status/conditions")
                .and_then(Value::as_array)
                .is_some_and(|conditions| {
                    conditions.iter().any(|condition| {
                        condition.get("type").and_then(Value::as_str) == Some("Failed")
                            && condition.get("status").and_then(Value::as_str) == Some("True")
                    })
                });
        if failed || tokio::time::Instant::now() >= deadline {
            let logs = kubectl(
                config,
                vec!["logs", "--namespace", &config.namespace, &format!("job/{job_name}")],
                None,
            )
            .await
            .unwrap_or_else(|error| format!("<could not read setup Job logs: {error:#}>"));
            let pods = kubectl(
                config,
                vec![
                    "get",
                    "pods",
                    "--namespace",
                    &config.namespace,
                    "--selector",
                    &job_uid.map_or_else(|| format!("job-name={job_name}"), |uid| format!("controller-uid={uid}")),
                    "-o",
                    "json",
                ],
                None,
            )
            .await
            .and_then(|output| serde_json::from_str::<Value>(&output).context("parse pin initialization Pod status"));
            let diagnostics = match pods {
                Ok(pods) => {
                    if failed && let Some(uid) = job_uid {
                        match failed_initializer_termination_evidence(&config.namespace, job_name, uid, &job, &pods) {
                            Ok(mut evidence) => {
                                evidence["run_id"] = json!(run_id);
                                let path = platform
                                    .runtime_dir
                                    .as_ref()
                                    .context("initializer termination evidence requires the run runtime directory")?
                                    .join("run-table-initializer-termination.json");
                                fs::write(path, serde_json::to_vec_pretty(&evidence)?)
                                    .context("persist positive failed initializer termination evidence")?;
                                platform.run_table_initialization_drained = true;
                            }
                            Err(error) => {
                                tracing::warn!(%error, "failed initializer publisher termination remains unconfirmed");
                            }
                        }
                    }
                    pin_init_job_failure_diagnostics(&job, &pods)
                }
                Err(error) => format!("Pod diagnostics unavailable: {error:#}"),
            };
            let logs = if logs.trim().is_empty() {
                "<no container logs>"
            } else {
                logs.trim()
            };
            bail!(
                "AKS OAT pin initialization Job {job_name} {}: {logs}; {diagnostics}",
                if failed { "failed" } else { "timed out" }
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let logs = kubectl(
        config,
        vec!["logs", "--namespace", &config.namespace, &format!("job/{job_name}")],
        None,
    )
    .await
    .with_context(|| format!("read successful AKS OAT pin initialization Job {job_name} logs"))?;
    if !logs.contains(&format!("initialized OAT pin set run_id={run_id}")) {
        bail!("AKS OAT pin initialization Job {job_name} did not confirm the requested run ID");
    }
    if let Some(table) = expected_table {
        validate_run_table_init_receipt(&logs, run_id, table)?;
    }
    Ok(())
}

fn validate_run_table_init_receipt(logs: &str, run_id: &str, table: &str) -> Result<()> {
    let receipt = logs.lines().find_map(|line| {
        serde_json::from_str::<Value>(line)
            .ok()
            .filter(|value| value["kind"] == "oat_run_table_initialized")
    });
    let receipt = receipt.context("pin-init Job did not emit a run-table initialization receipt")?;
    let delta = receipt.get("delta").context("pin-init receipt has no Delta metadata")?;
    let table_id = delta
        .get("table_id")
        .and_then(Value::as_str)
        .context("pin-init receipt has no Delta table ID")?;
    let parsed_table_id = uuid::Uuid::parse_str(table_id).context("pin-init Delta table ID is not a UUID")?;
    let schema_json = delta
        .get("schema_json")
        .and_then(Value::as_str)
        .context("pin-init receipt has no Delta schema JSON")?;
    let schema: Value = serde_json::from_str(schema_json).context("pin-init Delta schema JSON is invalid")?;
    let expected_schema = arrowcol_loadgen::initialize::canonical_table_delta_schema(table)?;
    if receipt["run_id"] != run_id
        || receipt["table"] != table
        || receipt["publisher_drained"] != true
        || parsed_table_id.to_string() != table_id
        || delta["version"] != 0
        || delta["active_files"] != 0
        || delta["partition_columns"] != json!(["__arrowcol_ingestion"])
        || schema != expected_schema
    {
        bail!(
            "pin-init Job run-table receipt does not prove the exact empty canonical Delta table was initialized and drained"
        );
    }
    Ok(())
}

fn local_partition_initializer_args(table: &str, dataflow_root: &Path) -> Result<Vec<String>> {
    if !dataflow_root.is_absolute() || !dataflow_root.is_dir() {
        bail!("local partition initialization requires an existing absolute run-owned Dataflow directory");
    }
    let canonical_table = canonical_dmv_table_name(table)?;
    if canonical_table != "telemetry.sqlserver.sqlserver_availability_replica_states" {
        bail!("local partition initialization only supports the campaign hot table");
    }
    let dataflow_root = dataflow_root
        .to_str()
        .context("local run-owned Dataflow directory is not valid UTF-8")?;
    Ok(vec![
        "initialize-local".to_owned(),
        "--partition-by-ingestion-date".to_owned(),
        "--table".to_owned(),
        canonical_table,
        "--dataflow-root".to_owned(),
        dataflow_root.to_owned(),
    ])
}

async fn initialize_local_partitioned_campaign_table(config: &OatConfig, platform: &LocalPlatform) -> Result<()> {
    if config.campaign_profile != Some(crate::config::CampaignProfile::SingleHotTable) {
        return Ok(());
    }
    let [table] = config.tables.as_slice() else {
        bail!("local partition initialization requires exactly one campaign table");
    };
    let dataflow_root = platform.state_dir.join("dataflow");
    let arguments = local_partition_initializer_args(table, &dataflow_root)?;
    let workspace = std::env::current_dir()?;
    let mut build = Command::new("cargo");
    build.args([
        "build",
        "--locked",
        "--package",
        "arrowcol-loadgen",
        "--bin",
        "arrowcol-loadgen",
    ]);
    build.current_dir(&workspace);
    run_streaming_command(build, "build setup-only local partition initializer").await?;

    let mut initialize = Command::new(workspace.join("target/debug/arrowcol-loadgen"));
    initialize.args(arguments);
    initialize.current_dir(workspace);
    run_streaming_command(initialize, "initialize the fresh local campaign Delta table")
        .await
        .context("setup local partitioned table before starting Dataflow or any DMV client")
}

async fn prepare_local_platform(config: &OatConfig, platform: &LocalPlatform) -> Result<()> {
    run_npx_runtime(
        &std::env::current_dir()?.join("projects/arrowcol-oat/runtime.ts"),
        &[
            "render-dataflow",
            if config.tunables.wal.is_some() {
                "quiver"
            } else {
                "direct"
            },
            platform.state_dir.join("dataflow.yaml").to_string_lossy().as_ref(),
        ],
    )
    .await
    .context("render run-owned dataflow profile before starting local workloads")?;
    initialize_local_partitioned_campaign_table(config, platform).await?;
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let mut build_client = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "oat",
    );
    platform.add_compose_environment(&mut build_client);
    build_client.args(["build", "oat-client"]);
    if maintenance_enabled(config) && config.maintenance_image.is_none() {
        build_client.arg("delta-maintenance");
    }
    run_streaming_command(build_client, "build OAT client container").await?;
    pin_local_maintenance_image(config, platform).await?;
    verify_local_bind_mount(config, platform).await?;

    let mut up = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut up);
    up.args([
        "up",
        "--detach",
        "--build",
        "--wait",
        "--wait-timeout",
        "600",
        "sqlserver",
    ]);
    run_streaming_command(up, "start local OAT SQL Server")
        .await
        .context("start local OAT stack")?;

    for service in ["database-bootstrap", "arrowcol-migrate"] {
        let mut migration = compose_command(
            compose_file,
            config.platform.compose_override_file.as_deref(),
            &project,
            "benchmark",
        );
        platform.add_compose_environment(&mut migration);
        migration.args(["run", "--rm", "--no-deps", service]);
        run_streaming_command(migration, &format!("run local OAT {service}")).await?;
    }

    let mut up = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut up);
    up.args([
        "up",
        "--detach",
        "--build",
        "--wait",
        "--wait-timeout",
        "600",
        "arrowcol-catalog",
        "dataflow-1",
        "lgtm",
        "router",
    ]);
    run_streaming_command(up, "start remaining local OAT stack")
        .await
        .context("local compose stack is a per-run project; results are not touched by stack cleanup")?;
    let mut rendered_config = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut rendered_config);
    rendered_config.args(["config", "--format", "json"]);
    let rendered_config: Value = serde_json::from_slice(
        &run_command(rendered_config, "read effective local Compose configuration")
            .await?
            .stdout,
    )
    .context("parse effective local Compose configuration")?;
    verify_local_collector_stop_grace(&rendered_config, config.collector_stop_grace_seconds)?;
    let mut downstream = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut downstream);
    downstream.args(["up", "--detach", "--no-deps", "--wait", "--wait-timeout", "60", "kusto"]);
    if run_streaming_command(downstream, "start optional local Kustainer")
        .await
        .is_err()
    {
        tracing::warn!("local Kustainer did not become ready; collector-only measurements remain enabled");
    }
    Ok(())
}

async fn scale_local_arrowcols(
    config: &OatConfig,
    platform: &LocalPlatform,
    replicas: u32,
    concurrency: u32,
) -> Result<()> {
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let mut up = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut up);
    up.env("LOADTEST_CPUS", concurrency.to_string());
    up.args([
        "up",
        "--detach",
        "--wait",
        "--wait-timeout",
        "600",
        "--scale",
        &format!("dataflow-1={replicas}"),
        "dataflow-1",
    ]);
    run_streaming_command(up, "scale local ArrowCol containers").await?;
    Ok(())
}

async fn local_compose_endpoint(
    config: &OatConfig,
    platform: &LocalPlatform,
    service: &str,
    container_port: &str,
    path: &str,
) -> Result<String> {
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let mut command = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut command);
    command.args(["port", service, container_port]);
    let output = run_command(command, &format!("discover local {service} endpoint")).await?;
    let binding = String::from_utf8(output.stdout)?;
    let binding = binding
        .lines()
        .next()
        .context("Compose returned no published binding")?;
    let endpoint = crate::local::published_endpoint(
        binding,
        config.platform.local.endpoint_host.as_deref(),
        platform
            .daemon_endpoint
            .as_deref()
            .context("Docker daemon endpoint not resolved")?,
        path,
    )?;
    let url = Url::parse(&endpoint)?;
    let host = url
        .host_str()
        .context("local endpoint has no host")?
        .trim_matches(['[', ']']);
    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect((host, url.port_or_known_default().context("local endpoint has no port")?)),
    )
    .await
    .with_context(|| format!("local {service} endpoint {endpoint} is unreachable; configure platform.local.bind_host/endpoint_host, not a loopback fallback"))?
    .with_context(|| format!("connect local {service} endpoint {endpoint}"))?;
    Ok(endpoint)
}

async fn local_docker_daemon() -> Result<String> {
    if std::env::var_os("DOCKER_CONTEXT").is_none()
        && let Ok(host) = std::env::var("DOCKER_HOST")
    {
        Url::parse(&host).context("invalid DOCKER_HOST")?;
        return Ok(host);
    }
    let mut command = Command::new("docker");
    command.args(["context", "inspect", "--format", "{{.Endpoints.docker.Host}}"]);
    let output = run_command(command, "resolve active Docker daemon endpoint").await?;
    let endpoint = String::from_utf8(output.stdout)?.trim().to_owned();
    Url::parse(&endpoint).context("Docker context returned an invalid daemon endpoint")?;
    Ok(endpoint)
}

fn local_client_image(rendered: &Value) -> Result<&str> {
    rendered
        .pointer("/services/oat-client/image")
        .and_then(Value::as_str)
        .filter(|image| !image.is_empty() && !image.chars().any(char::is_whitespace))
        .context("effective local OAT client image missing or invalid")
}

fn local_client_image_id(image: &str) -> Result<&str> {
    let image = image.trim();
    if !image
        .strip_prefix("sha256:")
        .is_some_and(|digest| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        bail!("local OAT client image did not resolve to an immutable Docker content ID");
    }
    Ok(image)
}

async fn resolve_local_client_image(config: &OatConfig, platform: &LocalPlatform) -> Result<String> {
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local project is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let mut render = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "oat",
    );
    platform.add_compose_environment(&mut render);
    render.args(["config", "--format", "json"]);
    let rendered: Value = serde_json::from_slice(
        &run_command(render, "read effective local OAT client image")
            .await?
            .stdout,
    )
    .context("parse effective local OAT client image")?;
    let mut inspect = Command::new("docker");
    inspect.args([
        "image",
        "inspect",
        "--format",
        "{{.Id}}",
        local_client_image(&rendered)?,
    ]);
    let image = String::from_utf8(
        run_command(inspect, "resolve local OAT client image content identity")
            .await?
            .stdout,
    )?;
    Ok(local_client_image_id(&image)?.to_owned())
}

async fn verify_local_bind_mount(config: &OatConfig, platform: &LocalPlatform) -> Result<()> {
    let image = resolve_local_client_image(config, platform).await?;
    let nonce = uuid::Uuid::new_v4().to_string();
    let probe = platform.state_dir.join("control/daemon-bind-probe");
    fs::write(&probe, &nonce)?;
    let state = platform.state_dir.to_str().context("local state path is not UTF-8")?;
    let compose = std::env::current_dir()?.join(
        config
            .platform
            .compose_file
            .as_deref()
            .context("missing local compose file")?,
    );
    let observability = compose
        .parent()
        .context("local compose file has no parent")?
        .join("observability");
    crate::local::absolute_directory(&observability)?;
    let source = observability.to_str().context("observability path is not UTF-8")?;
    if state.contains(',') || source.contains(',') {
        bail!("local bind source paths cannot contain a Docker --mount separator");
    }
    let mut checksums = String::new();
    for name in [
        "prometheus.yaml",
        "dashboards.yaml",
        "arrowcol.json",
        "delta-operations.json",
    ] {
        let (_, digest) = crate::local::file_digest(&observability.join(name))?;
        checksums.push_str(&format!("{digest}  /oat-observability/{name}\n"));
    }
    let uid = platform
        .environment
        .get("LOADTEST_UID")
        .context("local state UID missing")?;
    let gid = platform
        .environment
        .get("LOADTEST_GID")
        .context("local state GID missing")?;
    let user = format!("{uid}:{gid}");
    let mut command = Command::new("docker");
    command.args([
        "run", "--rm", "--pull", "never", "--network", "none", "--read-only",
        "--user", &user,
        "--mount", &format!("type=bind,source={state},target=/oat-bind,readonly"),
        "--mount", &format!("type=bind,source={source},target=/oat-observability,readonly"),
        "--entrypoint", "/bin/sh", &image, "-ec",
        "test \"$(cat /oat-bind/control/daemon-bind-probe)\" = \"$1\" && test -f /oat-bind/router-state/config.yaml && printf '%s' \"$2\" | sha256sum -c -",
        "oat-bind-probe", &nonce, &checksums,
    ]);
    run_command(command, "verify Docker daemon sees the same absolute generated-state path and bytes; remote path remapping is unsupported").await?;
    fs::remove_file(probe)?;
    Ok(())
}

async fn stop_local_snapshot_writers(config: &OatConfig, platform: &LocalPlatform) -> Result<Vec<Value>> {
    let project = scoped_compose_project(
        config
            .platform
            .compose_project
            .as_deref()
            .context("missing local project")?,
        &platform.run_id,
    );
    let mut list = Command::new("docker");
    list.args([
        "ps",
        "--all",
        "--quiet",
        "--no-trunc",
        "--filter",
        &format!("label=com.docker.compose.project={project}"),
    ]);
    let output = run_command(list, "list owned local containers before snapshot").await?;
    let ids = String::from_utf8(output.stdout)?;
    let ids = ids.split_whitespace().map(str::to_owned).collect::<Vec<_>>();
    if ids.is_empty() {
        if config.platform.local.snapshot_root.is_some() {
            bail!("no owned containers to establish stopped snapshot custody");
        }
        return Ok(Vec::new());
    }
    let mut inspect = Command::new("docker");
    inspect.arg("inspect").args(&ids);
    let before: Vec<Value> =
        serde_json::from_slice(&run_command(inspect, "inspect local snapshot ownership").await?.stdout)?;
    if config.platform.local.snapshot_root.is_some() {
        validate_snapshot_container_ownership(&before, &project)?;
    } else {
        validate_owned_local_containers(&before, &project)?;
    }
    let has_collector = before.iter().any(|container| {
        container
            .pointer("/Config/Labels/com.docker.compose.service")
            .and_then(Value::as_str)
            == Some("dataflow-1")
    });
    let phases = snapshot_stop_phases(&before)?;
    let mut maintenance_logs = None;
    let mut collector_sigterm_at_unix_ms = None;
    for (phase_index, phase) in phases.into_iter().enumerate() {
        if phase_index == 2 && !phase.is_empty() {
            collector_sigterm_at_unix_ms = u64::try_from(unix_time_ms()).ok();
        }
        if !phase.is_empty() {
            let mut stop = Command::new("docker");
            let stop_budgets = config.maintenance_stop_budget()?;
            stop.args(local_phase_stop_args(
                phase_index,
                stop_budgets.maintenance_grace_seconds,
                config.collector_stop_grace_seconds,
                phase,
            ));
            let phase_description = match phase_index {
                0 => "stop owned local ingress before maintenance and collectors",
                1 => "stop owned local maintenance before collectors",
                2 => "send SIGTERM to owned local collectors and wait for drain",
                _ => "stop owned local catalog and dependencies after collectors",
            };
            run_command(stop, phase_description).await?;
        }
        if phase_index == 1 {
            maintenance_logs =
                Some(local_process_logs_for_role(config, platform, Some(ComponentRole::Maintenance)).await?);
        }
    }
    let collector_logs = if has_collector {
        local_process_logs_for_role(config, platform, Some(ComponentRole::Collector)).await?
    } else {
        Vec::new()
    };
    let mut inspect = Command::new("docker");
    inspect.arg("inspect").args(&ids);
    let after: Vec<Value> = serde_json::from_slice(
        &run_command(inspect, "verify definite stopped local container outcomes")
            .await?
            .stdout,
    )?;
    let receipt = json!({
        "schema_version": 1, "run_id": platform.run_id, "project": project,
        "containers": after.iter().map(|container| json!({
            "id": container["Id"],
            "service": container.pointer("/Config/Labels/com.docker.compose.service"),
            "state": {
                "Running": container["State"]["Running"], "Paused": container["State"]["Paused"],
                "Dead": container["State"]["Dead"], "Status": container["State"]["Status"],
                "Error": container["State"]["Error"], "ExitCode": container["State"]["ExitCode"],
                "OOMKilled": container["State"]["OOMKilled"], "FinishedAt": container["State"]["FinishedAt"],
            },
        })).collect::<Vec<_>>(),
    });
    let mut receipt_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(platform.control_dir.join("snapshot-stop.json"))?;
    receipt_file.write_all(&serde_json::to_vec_pretty(&receipt)?)?;
    receipt_file.sync_all()?;
    let mut list = Command::new("docker");
    list.args([
        "ps",
        "--all",
        "--quiet",
        "--no-trunc",
        "--filter",
        &format!("label=com.docker.compose.project={project}"),
    ]);
    let listed = run_command(list, "verify no new owned containers appeared during snapshot stop").await?;
    let listed = String::from_utf8(listed.stdout)?;
    if listed.split_whitespace().collect::<HashSet<_>>() != ids.iter().map(String::as_str).collect::<HashSet<_>>() {
        bail!("owned local container set changed while stopping; preserving state");
    }
    let mut outcomes = snapshot_container_outcomes(&before, &after, &project)?;
    for outcome in &mut outcomes {
        let service = outcome["service"]
            .as_str()
            .context("stopped container service missing")?;
        let id = outcome["id"].as_str().context("stopped container ID missing")?;
        if service == "dataflow-1" {
            let incomplete_log = ProcessLog {
                role: ComponentRole::Collector,
                instance_id: id.to_owned(),
                image: None,
                image_id: None,
                source_revision: None,
                complete: false,
                raw_log_bytes: 0,
                raw_log_lines: 0,
                stdout: String::new(),
                stderr: String::new(),
                text: String::new(),
            };
            let log = collector_logs
                .iter()
                .find(|log| log.instance_id == id)
                .unwrap_or(&incomplete_log);
            outcome["shutdown_drain"] = collector_shutdown_drain_evidence(
                log,
                outcome["was_running"]
                    .as_bool()
                    .unwrap_or(false)
                    .then_some(collector_sigterm_at_unix_ms)
                    .flatten(),
                outcome["finished_at"].as_str(),
                outcome["exit_code"].as_i64(),
                config.tunables.wal.is_some(),
            );
        } else if service == "delta-maintenance" {
            let incomplete_log = ProcessLog {
                role: ComponentRole::Maintenance,
                instance_id: id.to_owned(),
                image: None,
                image_id: None,
                source_revision: None,
                complete: false,
                raw_log_bytes: 0,
                raw_log_lines: 0,
                stdout: String::new(),
                stderr: String::new(),
                text: String::new(),
            };
            let log = maintenance_logs
                .as_deref()
                .unwrap_or_default()
                .iter()
                .find(|log| log.instance_id == id)
                .unwrap_or(&incomplete_log);
            let evidence = maintenance_cell_outcome_evidence(log, &platform.maintenance_cell_baseline);
            outcome["maintenance_operation_failure"] = evidence["operation_failure"].clone();
            outcome["maintenance_exit_classification"] =
                maintenance_exit_classification(log, outcome["exit_code"].as_i64());
            outcome["maintenance_outcomes"] = evidence;
        }
    }
    Ok(outcomes)
}
fn snapshot_container_outcomes(before: &[Value], after: &[Value], project: &str) -> Result<Vec<Value>> {
    validate_owned_local_containers(before, project)?;
    validate_owned_local_containers(after, project)?;
    let before_by_id = before
        .iter()
        .map(|container| {
            Ok((
                container
                    .get("Id")
                    .and_then(Value::as_str)
                    .context("container ID missing")?,
                container,
            ))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    if before.len() != after.len() || before_by_id.len() != before.len() {
        bail!("owned local container set changed while stopping");
    }
    let mut writers = Vec::new();
    let mut seen = HashSet::new();
    for container in after {
        let id = container
            .get("Id")
            .and_then(Value::as_str)
            .context("container ID missing")?;
        if !seen.insert(id) {
            bail!("duplicate container in stopped snapshot receipt");
        }
        let original = before_by_id
            .get(id)
            .context("owned container identity changed while stopping")?;
        let state = container.get("State").context("container has no stop state")?;
        if state.get("Running").and_then(Value::as_bool) != Some(false)
            || state.get("Paused").and_then(Value::as_bool) != Some(false)
            || state.get("Dead").and_then(Value::as_bool) != Some(false)
            || state.get("Status").and_then(Value::as_str) != Some("exited")
            || state.get("Error").and_then(Value::as_str) != Some("")
        {
            bail!("uncertain local container stop outcome; preserving state");
        }
        let service = container
            .pointer("/Config/Labels/com.docker.compose.service")
            .and_then(Value::as_str)
            .context("container service label missing")?;
        if original
            .pointer("/Config/Labels/com.docker.compose.service")
            .and_then(Value::as_str)
            != Some(service)
        {
            bail!("owned container service changed while stopping");
        }
        let was_running = original
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .context("original container running state missing")?;
        let exit = state
            .get("ExitCode")
            .and_then(Value::as_i64)
            .context("container exit code missing")?;
        let role = if service == "delta-maintenance" {
            ComponentRole::Maintenance
        } else {
            ComponentRole::Collector
        };
        shutdown_exit_outcome(
            role,
            was_running,
            Some(exit),
            state.get("OOMKilled").and_then(Value::as_bool) != Some(false),
        )
        .with_context(|| format!("owned container {service} failed while stopping"))?;
        let finished_at = state
            .get("FinishedAt")
            .and_then(Value::as_str)
            .context("container finish time missing")?;
        if chrono::DateTime::parse_from_rfc3339(finished_at)?.timestamp() <= 0 {
            bail!("owned container has no definite finish time");
        }
        writers.push(json!({
            "id": id, "service": service, "was_running": was_running, "exit_code": exit,
            "finished_at": finished_at, "oom_killed": state["OOMKilled"],
            "maintenance_operation_failure": null,
            "comment": (service == "delta-maintenance" && exit == 1).then_some(
                "Cumulative maintenance health exit retained; cell records and health diagnostics determine failure, retry-only, or UNKNOWN.",
            ),
        }));
    }
    Ok(writers)
}

fn validate_owned_local_containers(containers: &[Value], project: &str) -> Result<()> {
    if containers.is_empty() {
        bail!("no owned local containers were found");
    }
    for container in containers {
        if container
            .pointer("/Config/Labels/com.docker.compose.project")
            .and_then(Value::as_str)
            != Some(project)
        {
            bail!("refusing local container operation outside the exact owned Compose project");
        }
        container
            .pointer("/Config/Labels/com.docker.compose.service")
            .and_then(Value::as_str)
            .context("missing owned Compose service")?;
    }
    Ok(())
}

fn validate_snapshot_container_ownership(containers: &[Value], project: &str) -> Result<()> {
    validate_owned_local_containers(containers, project)?;
    let services = containers
        .iter()
        .filter_map(|container| {
            container
                .pointer("/Config/Labels/com.docker.compose.service")
                .and_then(Value::as_str)
        })
        .collect::<HashSet<_>>();
    if !services.contains("dataflow-1") || !services.contains("arrowcol-catalog") {
        bail!("snapshot requires the owned collector and catalog, not a partially prepared stack");
    }
    Ok(())
}

fn snapshot_stop_phases(containers: &[Value]) -> Result<[Vec<String>; 4]> {
    let mut phases: [Vec<String>; 4] = std::array::from_fn(|_| Vec::new());
    for container in containers {
        let service = container
            .pointer("/Config/Labels/com.docker.compose.service")
            .and_then(Value::as_str)
            .context("missing owned Compose service")?;
        let phase = match service {
            "delta-maintenance" => 1,
            "dataflow-1" => 2,
            "arrowcol-catalog" | "sqlserver" | "kusto" | "lgtm" => 3,
            _ => 0,
        };
        phases[phase].push(
            container
                .get("Id")
                .and_then(Value::as_str)
                .context("container ID missing")?
                .to_owned(),
        );
    }
    Ok(phases)
}

fn build_shutdown_drain_evidence(containers: &[Value]) -> Value {
    json!({
        "schema_version": 1,
        "cleanup_order": ["ingress", "maintenance", "collectors"],
        "collectors": containers.iter().filter(|container| container["service"] == "dataflow-1").map(|container| json!({
            "container_id": container["id"],
            "drain": container["shutdown_drain"],
        })).collect::<Vec<_>>(),
        "maintenance": containers.iter().filter(|container| container["service"] == "delta-maintenance").map(|container| json!({
            "container_id": container["id"],
            "outcomes": container["maintenance_outcomes"],
            "exit_code": container["exit_code"],
            "exit_classification": container["maintenance_exit_classification"],
        })).collect::<Vec<_>>(),
    })
}

fn local_phase_stop_args(
    phase: usize,
    maintenance_grace: u64,
    collector_grace: u64,
    containers: Vec<String>,
) -> Vec<String> {
    let grace = match phase {
        1 => maintenance_grace,
        2 => collector_grace,
        _ => 300,
    };
    let mut args = vec!["stop".to_owned(), "--time".to_owned(), grace.to_string()];
    args.extend(containers);
    args
}

fn local_stop_deadline(collector_grace: u64, maintenance_timeout_ms: u64) -> Duration {
    Duration::from_millis(maintenance_timeout_ms) + Duration::from_secs(collector_grace + 2 * 300 + 120)
}

async fn stop_then_destroy<T>(
    stop: impl std::future::Future<Output = Result<T>>,
    destroy: impl std::future::Future<Output = Result<()>>,
) -> Result<T> {
    let stopped = stop.await.context(
        "stop failed or drain is uncertain; retaining owned resources for recovery, operator intervention required",
    )?;
    destroy.await?;
    Ok(stopped)
}

async fn cleanup_local_platform(config: &OatConfig, platform: &LocalPlatform) -> Result<Value> {
    let stop_budgets = config.maintenance_stop_budget()?;
    let stop = async {
        tokio::time::timeout(
            local_stop_deadline(
                config.collector_stop_grace_seconds,
                stop_budgets.maintenance_stop_timeout_ms,
            ),
            stop_local_snapshot_writers(config, platform),
        )
        .await
        .context("local stop deadline exceeded; stopped outcome uncertain")?
    };
    let containers = if let Some(destination) = &config.platform.local.snapshot_root {
        crate::local::snapshot_then_cleanup(
            stop,
            |writers| crate::local::export_stopped(
                &platform.state_dir, Path::new(destination), &platform.run_id, &config.tables, &writers,
            ),
            destroy_local_platform(config, platform),
        )
        .await
        .with_context(|| format!(
            "local snapshot/cleanup failed; source recovery path: {} (stop/export failures retain it; destructive-cleanup failures may be partial)",
            platform.state_dir.display()
        ))?
    } else {
        stop_then_destroy(stop, destroy_local_platform(config, platform)).await?
    };
    let mut evidence = build_shutdown_drain_evidence(&containers);
    if let Some(destination) = &config.platform.local.snapshot_root {
        evidence["stopped_delta_snapshots"] =
            crate::local::stopped_snapshot_references(Path::new(destination), &platform.run_id, &config.tables)?;
    }
    Ok(evidence)
}

fn local_oat_client_list_args(project: &str) -> Vec<String> {
    vec![
        "ps".to_owned(),
        "-aq".to_owned(),
        "--filter".to_owned(),
        format!("label=com.docker.compose.project={project}"),
        "--filter".to_owned(),
        "label=com.docker.compose.service=oat-client".to_owned(),
    ]
}

fn local_oat_client_remove_args(container_ids: &[String]) -> Vec<String> {
    let mut args = vec!["rm".to_owned(), "--force".to_owned()];
    args.extend(container_ids.iter().cloned());
    args
}

async fn remove_local_oat_client_containers(project: &str) -> Result<bool> {
    let mut list = Command::new("docker");
    list.args(local_oat_client_list_args(project));
    let output = run_command(list, "list local OAT client containers for cleanup").await?;
    let ids = String::from_utf8(output.stdout)
        .context("local OAT client container IDs were not UTF-8")?
        .lines()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(false);
    }
    let mut remove = Command::new("docker");
    remove.args(local_oat_client_remove_args(&ids));
    run_command(remove, "remove local OAT client containers").await?;
    Ok(true)
}

async fn destroy_local_platform(config: &OatConfig, platform: &LocalPlatform) -> Result<()> {
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let mut stack_clean = false;
    for _ in 0..3 {
        remove_local_oat_client_containers(&project).await?;
        let mut down = compose_command(
            compose_file,
            config.platform.compose_override_file.as_deref(),
            &project,
            "benchmark",
        );
        if maintenance_enabled(config) {
            down.args(["--profile", "maintenance"]);
        }
        down.envs(&platform.environment);
        down.args(["down", "--volumes", "--remove-orphans"]);
        run_streaming_command(down, "clean up local OAT stack").await?;
        if !remove_local_oat_client_containers(&project).await? {
            stack_clean = true;
            break;
        }
    }
    if !stack_clean {
        bail!("local OAT client containers continued to appear during cleanup");
    }
    if platform.state_dir.exists() {
        fs::remove_dir_all(&platform.state_dir)
            .with_context(|| format!("remove generated local OAT state {}", platform.state_dir.display()))?;
    }
    Ok(())
}

async fn collect_local_metrics(
    config: &OatConfig,
    run_id: &str,
    environment: &HashMap<String, String>,
) -> Result<ResourceMetrics> {
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let project = scoped_compose_project(base_project, run_id);
    let mut list = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    list.envs(environment);
    list.args(["ps", "--quiet", "dataflow-1"]);
    let ids = String::from_utf8(run_command(list, "list local ArrowCol containers").await?.stdout)?;
    let ids = ids
        .lines()
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .collect::<Vec<_>>();
    if ids.is_empty() {
        bail!("local Compose project has no running ArrowCol containers");
    }
    let mut stats = Command::new("docker");
    stats.args([
        "stats",
        "--no-stream",
        "--format",
        "{{.Name}} {{.CPUPerc}} {{.MemUsage}}",
    ]);
    stats.args(&ids);
    let output = run_command(stats, "sample local ArrowCol resource metrics").await?;
    let mut pods = HashMap::new();
    for line in String::from_utf8(output.stdout)?
        .lines()
        .filter(|line| !line.trim().is_empty())
    {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 4 {
            bail!("unexpected docker stats output row: {line}");
        }
        let cpu_percent = fields[1]
            .strip_suffix('%')
            .context("docker stats CPU value omitted percent suffix")?
            .parse::<f64>()?;
        if !cpu_percent.is_finite() || cpu_percent < 0.0 {
            bail!("docker stats returned invalid CPU value {cpu_percent}");
        }
        let memory_bytes = parse_memory_bytes(fields[2])?;
        pods.insert(
            fields[0].to_owned(),
            ResourceMetrics {
                cpu_millicores: cpu_percent * 10.0,
                memory_bytes,
            },
        );
    }
    if pods.len() != ids.len() {
        bail!(
            "docker stats returned {} of {} current ArrowCol containers",
            pods.len(),
            ids.len()
        );
    }
    Ok(sum_metrics(&pods))
}

async fn configure_deployments(config: &OatConfig, replicas: u32, concurrency: u32) -> Result<()> {
    for name in config.deployment_names() {
        let mut desired = deployment(config, name).await?;
        if name == config.arrowcol_deployment {
            desired["spec"]["replicas"] = json!(replicas);
        }
        if name == config.dfe_deployment {
            configure_collector_deployment(config, &mut desired, concurrency)?;
        }
        apply_deployment(config, desired).await?;
        if name == config.dfe_deployment {
            let applied = deployment(config, name).await?;
            verify_aks_collector_stop_grace(&applied, config.collector_stop_grace_seconds)?;
            let containers = applied
                .pointer("/spec/template/spec/containers")
                .and_then(Value::as_array)
                .context("applied collector Deployment has no containers")?;
            verify_container_resources(
                containers,
                &config.dfe_container,
                "collector",
                &config.effective_aks_resources().collector,
            )?;
        }
    }
    Ok(())
}

fn configure_collector_deployment(config: &OatConfig, desired: &mut Value, concurrency: u32) -> Result<()> {
    configure_workers(desired, &config.dfe_container, concurrency)?;
    configure_deployment_container_resources(
        desired,
        &config.dfe_container,
        &config.effective_aks_resources().collector,
    )?;
    desired["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] = json!(config.collector_stop_grace_seconds);
    let variables = role_environment(config, ComponentRole::Collector)?;
    if config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
        validate_collector_exporter_override(desired, &config.dfe_container, &variables)?;
    }
    inject_deployment_environment(desired, &config.dfe_container, &variables)
}

fn verify_aks_collector_stop_grace(deployment: &Value, expected_seconds: u64) -> Result<()> {
    let actual = deployment
        .pointer("/spec/template/spec/terminationGracePeriodSeconds")
        .and_then(Value::as_u64)
        .context("AKS collector readback omitted terminationGracePeriodSeconds")?;
    if actual != expected_seconds {
        bail!("AKS collector termination grace readback differs from requested value");
    }
    Ok(())
}

fn configure_workers(dfe: &mut Value, container_name: &str, concurrency: u32) -> Result<()> {
    let containers = dfe
        .pointer_mut("/spec/template/spec/containers")
        .and_then(Value::as_array_mut)
        .context("DFE Deployment has no container list")?;
    let container = containers
        .iter_mut()
        .find(|container| container.get("name").and_then(Value::as_str) == Some(container_name))
        .with_context(|| format!("DFE container {container_name:?} was not found"))?;
    let args = container
        .get_mut("args")
        .and_then(Value::as_array_mut)
        .context("DFE container args must be an array")?;
    let mut rewritten = Vec::with_capacity(args.len() + 2);
    let mut index = 0;
    while index < args.len() {
        let value = args[index].as_str().context("DFE args must be strings")?;
        if value == "--num-cores" {
            if args
                .get(index + 1)
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<u32>().ok())
                .is_none()
            {
                bail!("--num-cores must have a numeric value");
            }
            index += 2;
        } else if value == "--core-id-range" || value.starts_with("--core-id-range=") {
            bail!("DFE --core-id-range conflicts with benchmark --num-cores");
        } else if value.starts_with("--num-cores=") {
            index += 1;
        } else {
            rewritten.push(args[index].clone());
            index += 1;
        }
    }
    rewritten.push(json!("--num-cores"));
    rewritten.push(json!(concurrency.to_string()));
    container["args"] = Value::Array(rewritten);
    Ok(())
}

async fn deployment(config: &OatConfig, name: &str) -> Result<Value> {
    let output = kubectl(
        config,
        vec![
            "get",
            "deployment",
            name,
            "--namespace",
            &config.namespace,
            "-o",
            "json",
        ],
        None,
    )
    .await?;
    serde_json::from_str(&output).with_context(|| format!("parse Deployment {name} JSON"))
}

async fn apply_aks_overlay(config: &OatConfig, overlay: &str, platform: &mut AksPlatform) -> Result<()> {
    let rendered = kubectl(config, vec!["kustomize", overlay], None)
        .await
        .context("render configured AKS Kustomize overlay")?;
    if rendered.trim().is_empty() {
        bail!("configured AKS Kustomize overlay rendered no resources");
    }

    let validated = kubectl(
        config,
        vec![
            "apply",
            "--namespace",
            &config.namespace,
            "--dry-run=client",
            "-f",
            "-",
            "-o",
            "json",
        ],
        Some(rendered.as_bytes().to_vec()),
    )
    .await
    .context("validate rendered AKS Kustomize overlay")?;
    let mut resources = Vec::new();
    for value in serde_json::Deserializer::from_str(&validated).into_iter::<Value>() {
        let value = value.context("parse validated AKS Kustomize resources")?;
        if let Some(items) = value.get("items").and_then(Value::as_array) {
            resources.extend(items.iter().cloned());
        } else {
            resources.push(value);
        }
    }
    let allowed_names = config.deployment_names().into_iter().map(str::to_owned).collect();
    validate_aks_overlay_resources(&resources, &allowed_names, &config.namespace)?;
    prepare_run_owned_resources(config, platform, &mut resources).await?;

    kubectl(
        config,
        vec!["apply", "--namespace", &config.namespace, "-f", "-"],
        Some(serde_json::to_vec(
            &json!({"apiVersion":"v1", "kind":"List", "items":resources}),
        )?),
    )
    .await
    .context("apply configured AKS Kustomize overlay")?;
    record_applied_resource_uids(config, platform, &resources).await?;
    Ok(())
}

fn validate_aks_overlay_resources(resources: &[Value], allowed_names: &HashSet<String>, namespace: &str) -> Result<()> {
    if resources.is_empty() {
        bail!("configured AKS Kustomize overlay rendered no resources");
    }
    for resource in resources {
        let kind = resource.get("kind").and_then(Value::as_str);
        let name = resource.pointer("/metadata/name").and_then(Value::as_str);
        let resource_namespace = resource.pointer("/metadata/namespace").and_then(Value::as_str);
        if kind != Some("Deployment")
            || !name.is_some_and(|name| allowed_names.contains(name))
            || resource_namespace.is_some_and(|resource_namespace| resource_namespace != namespace)
        {
            bail!(
                "configured AKS Kustomize overlay may only include the configured ArrowCol and DFE Deployments in the target namespace"
            );
        }
    }
    Ok(())
}

fn scoped_aks_namespace(prefix: &str, run_id: &str) -> Result<String> {
    let suffix = uuid::Uuid::parse_str(run_id)
        .context("AKS run ID must be a UUID to derive a collision-resistant namespace")?
        .simple()
        .to_string();
    let prefix_limit = 63 - suffix.len() - 1;
    let prefix = prefix
        .chars()
        .take(prefix_limit)
        .collect::<String>()
        .trim_end_matches('-')
        .to_owned();
    if prefix.is_empty() {
        bail!("AKS namespace prefix is too long to leave room for the run UUID");
    }
    let namespace = format!("{prefix}-{suffix}");
    Ok(namespace)
}

fn run_delta_root(shared_root: &str, run_id: &str) -> Result<String> {
    let parsed_run = uuid::Uuid::parse_str(run_id).context("OAT data root requires a UUID run ID")?;
    if parsed_run.to_string() != run_id {
        bail!("OAT data root requires a canonical lowercase UUID run ID");
    }
    let mut root = Url::parse(shared_root).context("parse deployed-stamp deltaRoot")?;
    if root.scheme() != "abfss" || root.query().is_some() || root.fragment().is_some() {
        bail!("deployed-stamp deltaRoot must be a credential-free abfss URL without query or fragment");
    }
    let path = root.path().trim_end_matches('/');
    let parent = path
        .strip_suffix("/dataflow")
        .context("deployed-stamp deltaRoot must end with /dataflow")?;
    root.set_path(&format!("{parent}/runs/{run_id}/dataflow"));
    Ok(root.to_string())
}

fn qap_relative_data_root(run_root: &str) -> Result<String> {
    let path = run_root
        .split_once("://")
        .and_then(|(_, authority_and_path)| authority_and_path.split_once('/'))
        .map(|(_, path)| path)
        .context("run-scoped ArrowCol data root must include a path")?;
    if path.is_empty()
        || path
            .split('/')
            .any(|segment| segment.is_empty() || matches!(segment, "." | "..") || segment.contains('\\'))
        || path.contains('%')
        || path
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        bail!("run-scoped ArrowCol data root contains an unsafe path component");
    }
    let root = Url::parse(run_root).context("parse run-scoped ArrowCol data root")?;
    if root.scheme() != "abfss" || root.query().is_some() || root.fragment().is_some() {
        bail!("run-scoped ArrowCol data root must be a plain abfss URL");
    }
    if root.path().strip_prefix('/') != Some(path) {
        bail!("run-scoped ArrowCol data root must preserve its literal path");
    }
    Ok(path.to_owned())
}

fn qap_bindings(run_id: &str, tables: &[String]) -> Result<Vec<QapBinding>> {
    let parsed = uuid::Uuid::parse_str(run_id).context("QAP binding requires a UUID run ID")?;
    if parsed.to_string() != run_id || tables.is_empty() {
        bail!("QAP binding requires a canonical UUID and at least one table");
    }
    let run_prefix = parsed.simple().to_string();
    let mut seen = HashSet::new();
    tables
        .iter()
        .map(|table| {
            if !table.starts_with("sqlserver_")
                || table.is_empty()
                || !table.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                bail!("OAT QAP table name {table:?} is not a canonical SQL Server DMV identity");
            }
            if !seen.insert(table.as_str()) {
                bail!("OAT QAP table set contains duplicate identity {table}");
            }
            Ok(QapBinding {
                logical_table: table.clone(),
                external_table: format!("oat_{run_prefix}_{table}"),
            })
        })
        .collect()
}

fn campaign_qap_manifest(
    run_id: &str,
    database: &str,
    run_root: &str,
    tables: &[String],
) -> Result<(Value, Vec<InventoryQapBinding>)> {
    let bindings = qap_bindings(run_id, tables)?;
    let inventory_bindings = bindings
        .iter()
        .map(|binding| {
            Ok(InventoryQapBinding {
                external_table: binding.external_table.clone(),
                relative_path: aks_qap_relative_path(run_root, &binding.logical_table)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let manifest = json!({
        "schema_version": 1,
        "run_id": run_id,
        "database": database,
        "tables": inventory_bindings,
    });
    Ok((manifest, inventory_bindings))
}

fn aks_qap_relative_path(run_root: &str, table: &str) -> Result<String> {
    if !table.starts_with("sqlserver_") || !table.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
        bail!("OAT QAP table path has an invalid table identity");
    }
    Ok(format!("{run_root}/telemetry/sqlserver/{table}"))
}

fn namespace_matches_run(namespace: &Value, run_id: &str, owner_token: &str, expected_uid: Option<&str>) -> bool {
    let actual_uid = namespace.pointer("/metadata/uid").and_then(Value::as_str);
    let actual_run_id = namespace
        .pointer("/metadata/labels/arrowcol-oat~1run-id")
        .and_then(Value::as_str);
    let actual_owner_token = namespace
        .pointer("/metadata/labels/arrowcol-oat~1owner-token")
        .and_then(Value::as_str);
    let annotated_run_id = namespace
        .pointer("/metadata/annotations/arrowcol-oat~1run-id")
        .and_then(Value::as_str);
    let annotated_owner_token = namespace
        .pointer("/metadata/annotations/arrowcol-oat~1owner-token")
        .and_then(Value::as_str);
    actual_uid.is_some()
        && actual_run_id == Some(run_id)
        && actual_owner_token == Some(owner_token)
        && annotated_run_id == Some(run_id)
        && annotated_owner_token == Some(owner_token)
        && expected_uid.is_none_or(|uid| actual_uid == Some(uid))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NamespaceCleanupAction {
    Delete,
    Ignore,
    Refuse,
}

fn namespace_cleanup_action(namespace_created: bool, namespace_matches: bool) -> NamespaceCleanupAction {
    if namespace_matches {
        NamespaceCleanupAction::Delete
    } else if namespace_created {
        NamespaceCleanupAction::Refuse
    } else {
        NamespaceCleanupAction::Ignore
    }
}

fn namespace_uid_for_cleanup(namespace: &Value, expected_uid: Option<&str>) -> Result<String> {
    let uid = namespace
        .pointer("/metadata/uid")
        .and_then(Value::as_str)
        .context("run-owned AKS namespace UID is missing before deletion")?;
    if expected_uid.is_some_and(|expected| expected != uid) {
        bail!("run-owned AKS namespace UID changed before deletion");
    }
    Ok(uid.to_owned())
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CampaignCleanupAction {
    StopAdmission,
    BoundedDrain,
    EvidenceCapture,
    StopMaintenance,
    TerminalCapture,
    RemoveNamespace,
    RemoveQap,
    RemoveFic,
    Handoff,
}

impl CampaignCleanupAction {
    fn selector(&self) -> &'static str {
        match self {
            Self::StopAdmission => "stop_admission",
            Self::BoundedDrain => "bounded_drain",
            Self::EvidenceCapture => "evidence_capture",
            Self::StopMaintenance => "stop_maintenance",
            Self::TerminalCapture => "terminal_capture",
            Self::RemoveNamespace => "remove_namespace",
            Self::RemoveQap => "remove_qap",
            Self::RemoveFic => "remove_fic",
            Self::Handoff => "handoff",
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CampaignActorMode {
    Producer,
    Controller,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CampaignCleanupActor {
    mode: CampaignActorMode,
    owner_id: String,
    fencing_token: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CampaignInventoryReadRequest<'a> {
    schema_version: u8,
    campaign_id: &'a str,
    orchestration_run_id: &'a str,
    cell_run_id: &'a str,
    actor: &'a CampaignCleanupActor,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CampaignCleanupRequest<'a> {
    schema_version: u8,
    inventory: &'a RunInventory,
    expected_etag: &'a str,
    actor: &'a CampaignCleanupActor,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CampaignCellBaselineRequest<'a> {
    schema_version: u8,
    campaign_id: &'a str,
    orchestration_run_id: &'a str,
    cell_run_id: &'a str,
    expected_etag: &'a str,
    hot_delta_relative_path: &'a str,
    actor: &'a CampaignCleanupActor,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CampaignDeltaState {
    Table,
    ObservedAbsent,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CampaignCellBaselineReceipt {
    status: String,
    campaign_id: String,
    owner_id: String,
    fencing_token: u64,
    orchestration_run_id: String,
    cell_run_id: String,
    namespace_uid: String,
    hot_delta_relative_path: String,
    blob: String,
    etag: String,
    delta_state: CampaignDeltaState,
    table_id: Option<String>,
    delta_version: Option<u64>,
    rows: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignCellBaselineResponse {
    ok: bool,
    receipt: Option<CampaignCellBaselineReceipt>,
    code: Option<String>,
    message: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CampaignBaselineReceipt {
    status: String,
    campaign_id: String,
    owner_id: String,
    fencing_token: u64,
    blob: String,
    etag: String,
    captured_at_unix_ms: u64,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignBaselineResponse {
    ok: bool,
    receipt: Option<CampaignBaselineReceipt>,
    code: Option<String>,
    message: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignInventoryReadResponse {
    ok: bool,
    inventory: RunInventory,
    etag: String,
}

fn deserialize_present_campaign_option<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CampaignCleanupReceipt {
    status: String,
    step: String,
    campaign_id: String,
    owner_id: String,
    fencing_token: u64,
    orchestration_run_id: String,
    cell_run_id: String,
    #[serde(deserialize_with = "deserialize_present_campaign_option")]
    namespace_uid: Option<String>,
    actor: CampaignCleanupActor,
    completed_at_unix_ms: u64,
    etag: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignCleanupResponse {
    ok: bool,
    receipt: CampaignCleanupReceipt,
}

fn validate_campaign_etag(etag: &str) -> Result<()> {
    if etag.is_empty()
        || etag.len() > 512
        || etag == "*"
        || etag.starts_with("W/")
        || etag.bytes().any(|byte| byte.is_ascii_control())
    {
        bail!("campaign inventory requires a strong nonempty ETag");
    }
    Ok(())
}

const MAX_CAMPAIGN_CLEANUP_ATTEMPTS: usize = 3;

#[derive(Debug)]
struct StaleCampaignVersionError {
    action: String,
}

impl std::fmt::Display for StaleCampaignVersionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "campaign cleanup {} rejected a stale version before effects",
            self.action
        )
    }
}

impl std::error::Error for StaleCampaignVersionError {}

fn campaign_custody_helper_failure(action: &str, stdout: &[u8]) -> anyhow::Error {
    let code = serde_json::from_slice::<Value>(stdout)
        .ok()
        .and_then(|response| response.get("code").and_then(Value::as_str).map(str::to_owned))
        .filter(|code| {
            matches!(
                code.as_str(),
                "campaign_fenced" | "stale_version" | "write_outcome_unknown" | "invalid_or_storage_error"
            )
        })
        .unwrap_or_else(|| "invalid_response".to_owned());
    if action == "cleanup-step" && code == "stale_version" {
        anyhow::Error::new(StaleCampaignVersionError {
            action: action.to_owned(),
        })
    } else {
        anyhow::anyhow!("campaign custody helper {action} failed: {code}")
    }
}

#[cfg(unix)]
async fn run_campaign_custody_json<T: serde::Serialize>(
    platform: &AksPlatform,
    action: &str,
    selector: Option<&str>,
    request: &T,
    timeout: Duration,
) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(request)?;
    if payload.len() > 1024 * 1024 {
        bail!("campaign custody request exceeds the helper input limit");
    }
    let action_args = selector.map(|value| vec![value.to_owned()]).unwrap_or_default();
    let mut command = aks_campaign_cli_command(platform.campaign_cli_paths()?, action, &action_args)?;
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    let output = match tokio::time::timeout(timeout, async {
        let mut child = command.spawn().context("start campaign custody helper")?;
        let pid = i32::try_from(child.id().context("campaign custody helper PID unavailable")?)
            .context("campaign custody helper PID invalid")?;
        let _group =
            OwnedQapHelperGroup(rustix::process::Pid::from_raw(pid).context("campaign custody helper PID invalid")?);
        let mut stdin = child.stdin.take().context("open campaign custody helper input")?;
        stdin
            .write_all(&payload)
            .await
            .context("write campaign custody request")?;
        drop(stdin);
        child
            .wait_with_output()
            .await
            .context("wait for campaign custody helper")
    })
    .await
    {
        Ok(result) => result?,
        Err(_) if matches!(action, "cleanup-step" | "capture-cell-baseline" | "capture-baseline") => {
            bail!("campaign {action} helper timed out; effect outcome is UNKNOWN and must not be replayed")
        }
        Err(_) => bail!("campaign inventory helper timed out; no authoritative readback was obtained"),
    };
    if !output.status.success() {
        return Err(campaign_custody_helper_failure(action, &output.stdout));
    }
    Ok(output.stdout)
}

#[cfg(not(unix))]
async fn run_campaign_custody_json<T: serde::Serialize>(
    _platform: &AksPlatform,
    _action: &str,
    _selector: Option<&str>,
    _request: &T,
    _timeout: Duration,
) -> Result<Vec<u8>> {
    bail!("campaign custody requires Unix process-group cancellation")
}

fn parse_campaign_baseline_response(output: &[u8], identity: &CampaignIdentity) -> Result<CampaignBaselineReceipt> {
    let response: CampaignBaselineResponse =
        serde_json::from_slice(output).context("parse immutable campaign baseline receipt")?;
    if !response.ok {
        let code = response
            .code
            .as_deref()
            .filter(|code| !code.is_empty())
            .context("campaign baseline failure returned no error code")?;
        bail!("campaign baseline capture failed: {code}");
    }
    if response.code.is_some() || response.message.is_some() {
        bail!("successful campaign baseline response included error fields");
    }
    let receipt = response
        .receipt
        .context("campaign baseline response omitted its receipt")?;
    validate_campaign_baseline_receipt(&receipt, identity)?;
    Ok(receipt)
}

async fn load_campaign_baseline_receipt(
    platform: &AksPlatform,
    identity: &CampaignIdentity,
) -> Result<Option<CampaignBaselineReceipt>> {
    let runtime_dir = platform
        .runtime_dir
        .as_deref()
        .context("campaign runtime directory is not available")?;
    let path = runtime_dir.join("campaign-baseline.json");
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("read campaign baseline receipt {}", path.display())),
    };
    let receipt: CampaignBaselineReceipt =
        serde_json::from_slice(&bytes).context("parse persisted immutable campaign baseline receipt")?;
    validate_campaign_baseline_receipt(&receipt, identity)?;
    Ok(Some(receipt))
}

#[cfg(unix)]
async fn capture_campaign_baseline(
    platform: &AksPlatform,
    identity: &CampaignIdentity,
) -> Result<CampaignBaselineReceipt> {
    if let Some(receipt) = load_campaign_baseline_receipt(platform, identity).await? {
        return Ok(receipt);
    }
    let action_args = vec![identity.owner_id.clone(), identity.fencing_token.to_string()];
    let mut command = aks_campaign_cli_command(platform.campaign_cli_paths()?, "capture-baseline", &action_args)?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(90), async {
        let child = command.spawn().context("start campaign baseline capture")?;
        let pid = i32::try_from(child.id().context("campaign baseline helper PID unavailable")?)
            .context("campaign baseline helper PID invalid")?;
        let _group =
            OwnedQapHelperGroup(rustix::process::Pid::from_raw(pid).context("campaign baseline helper PID invalid")?);
        child
            .wait_with_output()
            .await
            .context("wait for campaign baseline helper")
    })
    .await
    .context("campaign baseline capture timed out; outcome is UNKNOWN and must not be replayed")??;
    if !output.status.success() {
        return Err(campaign_custody_helper_failure("capture-baseline", &output.stdout));
    }
    let receipt = parse_campaign_baseline_response(&output.stdout, identity)?;
    if let Some(existing) = load_campaign_baseline_receipt(platform, identity).await? {
        if existing != receipt {
            bail!("persisted campaign baseline proof changed; refusing to replace the original receipt");
        }
        return Ok(existing);
    }
    let runtime_dir = platform
        .runtime_dir
        .as_deref()
        .context("campaign runtime directory is not available")?;
    tokio::fs::write(
        runtime_dir.join("campaign-baseline.json"),
        serde_json::to_vec(&receipt)?,
    )
    .await
    .context("persist immutable campaign baseline receipt")?;
    Ok(receipt)
}

#[cfg(not(unix))]
async fn capture_campaign_baseline(
    _platform: &AksPlatform,
    _identity: &CampaignIdentity,
) -> Result<CampaignBaselineReceipt> {
    bail!("campaign custody requires Unix process-group cancellation")
}

fn validate_campaign_cell_baseline(
    receipt: &CampaignCellBaselineReceipt,
    inventory: &RunInventory,
    identity: &CampaignIdentity,
    run_id: &str,
    hot_delta_relative_path: &str,
) -> Result<()> {
    validate_campaign_etag(&receipt.etag)?;
    let expected_blob = format!(
        "_arrowcol-custody/campaigns/{}/runs/{}/cells/{}/delta-before.json",
        inventory.campaign_id, inventory.orchestration_run_id, run_id
    );
    let planned_binding_count = inventory
        .qap
        .as_ref()
        .filter(|qap| qap.state == InventoryResourceState::Planned)
        .map(|qap| {
            qap.bindings
                .iter()
                .filter(|binding| binding.relative_path == hot_delta_relative_path)
                .count()
        })
        .unwrap_or(0);
    if inventory.load_admission != InventoryLoadAdmission::NotAuthorized
        || planned_binding_count != 1
        || receipt.status != "captured"
        || receipt.campaign_id != identity.campaign_id
        || receipt.owner_id != identity.owner_id
        || receipt.fencing_token != identity.fencing_token
        || receipt.orchestration_run_id != inventory.orchestration_run_id
        || receipt.cell_run_id != run_id
        || receipt.namespace_uid != inventory.namespace.uid.as_deref().unwrap_or_default()
        || receipt.hot_delta_relative_path != hot_delta_relative_path
        || receipt.blob != expected_blob
        || !valid_blob_path(&receipt.blob)
    {
        bail!("cell baseline receipt does not match the exact campaign, cell, namespace and hot Delta path");
    }
    match receipt.delta_state {
        CampaignDeltaState::Table => {
            if receipt.table_id.as_deref().is_none_or(str::is_empty)
                || receipt.delta_version.is_none()
                || receipt.rows.is_none()
            {
                bail!("existing-table baseline requires its exact Delta table ID, version and row count");
            }
        }
        CampaignDeltaState::ObservedAbsent => {
            if receipt.table_id.is_some() || receipt.delta_version.is_some() || receipt.rows.is_some() {
                bail!("observed-absent baseline must not invent table identity, version or row count");
            }
        }
    }
    Ok(())
}

fn validate_campaign_inventory_identity(
    inventory: &RunInventory,
    campaign_id: &str,
    owner_id: &str,
    fencing_token: u64,
    orchestration_run_id: &str,
    cell_run_id: &str,
) -> Result<()> {
    inventory.validate()?;
    if inventory.campaign_id != campaign_id
        || inventory.owner_id != owner_id
        || inventory.fencing_token != fencing_token
        || inventory.orchestration_run_id != orchestration_run_id
        || inventory.cell_run_id != cell_run_id
    {
        bail!("authoritative campaign inventory identity does not match this OAT cell");
    }
    Ok(())
}

fn campaign_baseline_blob(campaign_id: &str) -> String {
    format!("_arrowcol-custody/campaigns/{campaign_id}/baseline.json")
}

fn validate_campaign_baseline_receipt(receipt: &CampaignBaselineReceipt, identity: &CampaignIdentity) -> Result<()> {
    validate_campaign_etag(&receipt.etag)?;
    let expected_blob = campaign_baseline_blob(&identity.campaign_id);
    if receipt.status != "captured"
        || receipt.campaign_id != identity.campaign_id
        || receipt.owner_id != identity.owner_id
        || receipt.fencing_token != identity.fencing_token
        || receipt.blob != expected_blob
        || !valid_blob_path(&receipt.blob)
        || receipt.captured_at_unix_ms == 0
    {
        bail!("campaign baseline receipt does not match the exact campaign, producer fence, and immutable blob");
    }
    Ok(())
}

fn validate_campaign_baseline_capture_inventory(inventory: &RunInventory, identity: &CampaignIdentity) -> Result<()> {
    validate_campaign_inventory_identity(
        inventory,
        &identity.campaign_id,
        &identity.owner_id,
        identity.fencing_token,
        &inventory.orchestration_run_id,
        &inventory.cell_run_id,
    )?;
    if inventory.load_admission != InventoryLoadAdmission::NotAuthorized
        || inventory.namespace.uid.is_some()
        || !inventory.resources.is_empty()
        || inventory.fic.is_some()
        || inventory.qap.is_some()
    {
        bail!("campaign baseline capture requires the initial not_authorized inventory");
    }
    Ok(())
}

async fn read_authoritative_campaign_inventory(
    platform: &AksPlatform,
    identity: &CampaignIdentity,
    orchestration_run_id: &str,
    timeout: Duration,
) -> Result<(RunInventory, String)> {
    let actor = CampaignCleanupActor {
        mode: CampaignActorMode::Producer,
        owner_id: identity.owner_id.clone(),
        fencing_token: identity.fencing_token,
    };
    let request = CampaignInventoryReadRequest {
        schema_version: 1,
        campaign_id: &identity.campaign_id,
        orchestration_run_id,
        cell_run_id: &platform.run_id,
        actor: &actor,
    };
    let output = run_campaign_custody_json(platform, "read-inventory", None, &request, timeout).await?;
    let response: CampaignInventoryReadResponse =
        serde_json::from_slice(&output).context("parse authoritative campaign inventory")?;
    if !response.ok {
        bail!("campaign inventory read was not positively confirmed");
    }
    validate_campaign_etag(&response.etag)?;
    validate_campaign_inventory_identity(
        &response.inventory,
        &identity.campaign_id,
        &identity.owner_id,
        identity.fencing_token,
        orchestration_run_id,
        &platform.run_id,
    )?;
    Ok((response.inventory, response.etag))
}

fn campaign_cleanup_step<'a>(action: &CampaignCleanupAction, inventory: &'a RunInventory) -> Option<&'a InventoryStep> {
    match action {
        CampaignCleanupAction::StopAdmission => Some(&inventory.cleanup.stop_admission),
        CampaignCleanupAction::BoundedDrain => Some(&inventory.cleanup.bounded_drain),
        CampaignCleanupAction::EvidenceCapture => Some(&inventory.cleanup.evidence_capture),
        CampaignCleanupAction::StopMaintenance => Some(&inventory.cleanup.stop_maintenance),
        CampaignCleanupAction::TerminalCapture => Some(&inventory.cleanup.terminal_capture),
        CampaignCleanupAction::Handoff => Some(&inventory.cleanup.handoff),
        CampaignCleanupAction::RemoveNamespace
        | CampaignCleanupAction::RemoveQap
        | CampaignCleanupAction::RemoveFic => None,
    }
}

fn campaign_cleanup_step_complete(action: &CampaignCleanupAction, inventory: &RunInventory) -> Result<()> {
    let complete = match action {
        CampaignCleanupAction::StopAdmission => inventory.cleanup.stop_admission.state == InventoryStepState::Complete,
        CampaignCleanupAction::BoundedDrain => inventory.cleanup.bounded_drain.state == InventoryStepState::Complete,
        CampaignCleanupAction::EvidenceCapture => {
            inventory.cleanup.evidence_capture.state == InventoryStepState::Complete
        }
        CampaignCleanupAction::StopMaintenance => {
            inventory.cleanup.stop_maintenance.state == InventoryStepState::Complete
        }
        CampaignCleanupAction::TerminalCapture => {
            inventory.cleanup.terminal_capture.state == InventoryStepState::Complete
        }
        CampaignCleanupAction::RemoveNamespace => inventory.resources.iter().all(|resource| {
            matches!(
                resource.state,
                InventoryResourceState::Removed | InventoryResourceState::Absent
            )
        }),
        CampaignCleanupAction::RemoveQap => inventory.qap.as_ref().is_none_or(|qap| {
            matches!(
                qap.state,
                InventoryResourceState::Removed | InventoryResourceState::Absent
            )
        }),
        CampaignCleanupAction::RemoveFic => inventory.fic.as_ref().is_none_or(|fic| {
            matches!(
                fic.state,
                InventoryResourceState::Removed | InventoryResourceState::Absent
            )
        }),
        CampaignCleanupAction::Handoff => inventory.cleanup.handoff.state == InventoryStepState::Complete,
    };
    if !complete {
        bail!(
            "campaign cleanup {} lacks authoritative durable completion",
            action.selector()
        );
    }
    Ok(())
}

fn campaign_cleanup_action_needs_execution(action: &CampaignCleanupAction, inventory: &RunInventory) -> Result<bool> {
    if campaign_cleanup_step_complete(action, inventory).is_ok() {
        return Ok(false);
    }
    if let Some(step) = campaign_cleanup_step(action, inventory) {
        match step.state {
            InventoryStepState::NotStarted => {}
            InventoryStepState::Complete => {
                bail!(
                    "campaign cleanup {} has incomplete completion evidence",
                    action.selector()
                );
            }
            InventoryStepState::Pending
            | InventoryStepState::Running
            | InventoryStepState::Failed
            | InventoryStepState::Uncertain => {
                bail!(
                    "campaign cleanup {} is already running or unresolved; refusing replay",
                    action.selector()
                );
            }
        }
    }
    let uncertain_resource = match action {
        CampaignCleanupAction::RemoveNamespace => inventory
            .resources
            .iter()
            .any(|resource| resource.state == InventoryResourceState::Uncertain),
        CampaignCleanupAction::RemoveQap => inventory
            .qap
            .as_ref()
            .is_some_and(|qap| qap.state == InventoryResourceState::Uncertain),
        CampaignCleanupAction::RemoveFic => inventory
            .fic
            .as_ref()
            .is_some_and(|fic| fic.state == InventoryResourceState::Uncertain),
        _ => false,
    };
    if uncertain_resource {
        bail!(
            "campaign cleanup {} has uncertain resource state; refusing replay",
            action.selector()
        );
    }
    Ok(true)
}

const CAMPAIGN_CLEANUP_DEFAULT_TIMEOUT_MS: u64 = 60_000;

fn campaign_cleanup_operation_deadline_ms(
    action: &CampaignCleanupAction,
    inventory: &RunInventory,
    now_unix_ms: u64,
) -> Result<u64> {
    let deadline =
        if let Some(deadline) = campaign_cleanup_step(action, inventory).and_then(|step| step.deadline_unix_ms) {
            deadline
        } else {
            let timeout_ms = match action {
                CampaignCleanupAction::StopAdmission => 60_000,
                CampaignCleanupAction::BoundedDrain => 120_000,
                CampaignCleanupAction::StopMaintenance => {
                    inventory
                        .effective_stop_budgets
                        .context("new campaign inventory omitted effective maintenance stop budgets")?
                        .validate()?
                        .maintenance_stop_timeout_ms
                }
                _ => CAMPAIGN_CLEANUP_DEFAULT_TIMEOUT_MS,
            };
            now_unix_ms
                .checked_add(timeout_ms)
                .context("campaign cleanup deadline overflow")?
        };
    if deadline <= now_unix_ms {
        bail!("campaign cleanup {} operation deadline has expired", action.selector());
    }
    Ok(deadline)
}

fn campaign_cleanup_remaining_budget(
    deadline: tokio::time::Instant,
    action: &CampaignCleanupAction,
) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        bail!("campaign cleanup {} operation deadline has expired", action.selector());
    }
    Ok(remaining)
}

async fn execute_campaign_cleanup_action(
    platform: &AksPlatform,
    identity: &CampaignIdentity,
    orchestration_run_id: &str,
    action: &CampaignCleanupAction,
    inventory: &RunInventory,
    etag: &str,
) -> Result<(RunInventory, String, Option<CampaignCleanupReceipt>)> {
    let mut current_inventory = inventory.clone();
    let mut current_etag = etag.to_owned();
    let now_unix_ms = unix_time_ms_u64()?;
    let deadline_unix_ms = campaign_cleanup_operation_deadline_ms(action, &current_inventory, now_unix_ms)?;
    let operation_deadline = tokio::time::Instant::now() + Duration::from_millis(deadline_unix_ms - now_unix_ms);
    let actor = CampaignCleanupActor {
        mode: CampaignActorMode::Producer,
        owner_id: identity.owner_id.clone(),
        fencing_token: identity.fencing_token,
    };
    for attempt in 0..MAX_CAMPAIGN_CLEANUP_ATTEMPTS {
        if !campaign_cleanup_action_needs_execution(action, &current_inventory)? {
            return Ok((current_inventory, current_etag, None));
        }
        validate_campaign_etag(&current_etag)?;
        let output = {
            let request = CampaignCleanupRequest {
                schema_version: 1,
                inventory: &current_inventory,
                expected_etag: &current_etag,
                actor: &actor,
            };
            let timeout = campaign_cleanup_remaining_budget(operation_deadline, action)?;
            run_campaign_custody_json(platform, "cleanup-step", Some(action.selector()), &request, timeout).await
        };
        let output = match output {
            Ok(output) => output,
            Err(error) if error.downcast_ref::<StaleCampaignVersionError>().is_some() => {
                let timeout = campaign_cleanup_remaining_budget(operation_deadline, action)?;
                let (refreshed_inventory, refreshed_etag) =
                    read_authoritative_campaign_inventory(platform, identity, orchestration_run_id, timeout).await?;
                current_inventory.validate_successor(&refreshed_inventory)?;
                if refreshed_etag == current_etag {
                    bail!(
                        "campaign cleanup {} stale-version refresh returned the unchanged ETag; refusing retry",
                        action.selector()
                    );
                }
                persist_campaign_inventory_snapshot(platform, &refreshed_inventory, &refreshed_etag).await?;
                current_inventory = refreshed_inventory;
                current_etag = refreshed_etag;
                if !campaign_cleanup_action_needs_execution(action, &current_inventory)? {
                    return Ok((current_inventory, current_etag, None));
                }
                if attempt + 1 == MAX_CAMPAIGN_CLEANUP_ATTEMPTS {
                    bail!(
                        "campaign cleanup {} exhausted {} attempts after stale inventory versions",
                        action.selector(),
                        MAX_CAMPAIGN_CLEANUP_ATTEMPTS
                    );
                }
                continue;
            }
            Err(error) => return Err(error),
        };
        let response: CampaignCleanupResponse =
            serde_json::from_slice(&output).context("parse campaign cleanup receipt")?;
        if !response.ok {
            bail!("campaign cleanup {} was not positively confirmed", action.selector());
        }
        let receipt = response.receipt;
        validate_campaign_etag(&receipt.etag)?;
        if receipt.status != "complete"
            || receipt.step != action.selector()
            || receipt.campaign_id != current_inventory.campaign_id
            || receipt.owner_id != current_inventory.owner_id
            || receipt.fencing_token != current_inventory.fencing_token
            || receipt.orchestration_run_id != current_inventory.orchestration_run_id
            || receipt.cell_run_id != current_inventory.cell_run_id
            || receipt.namespace_uid != current_inventory.namespace.uid
            || receipt.actor != actor
            || receipt.completed_at_unix_ms == 0
        {
            bail!(
                "campaign cleanup {} returned a mismatched or incomplete receipt",
                action.selector()
            );
        }
        let timeout = campaign_cleanup_remaining_budget(operation_deadline, action)?;
        let (next_inventory, next_etag) =
            read_authoritative_campaign_inventory(platform, identity, orchestration_run_id, timeout).await?;
        if next_etag != receipt.etag {
            bail!("campaign cleanup receipt ETag does not match authoritative inventory readback");
        }
        current_inventory.validate_successor(&next_inventory)?;
        campaign_cleanup_step_complete(action, &next_inventory)?;
        persist_campaign_inventory_snapshot(platform, &next_inventory, &next_etag).await?;
        return Ok((next_inventory, next_etag, Some(receipt)));
    }
    bail!(
        "campaign cleanup {} exhausted {} attempts",
        action.selector(),
        MAX_CAMPAIGN_CLEANUP_ATTEMPTS
    )
}

async fn persist_campaign_inventory_snapshot(
    platform: &AksPlatform,
    inventory: &RunInventory,
    etag: &str,
) -> Result<()> {
    let runtime_dir = platform
        .runtime_dir
        .as_deref()
        .context("campaign runtime directory is not available")?;
    validate_campaign_etag(etag)?;
    tokio::fs::write(
        runtime_dir.join("campaign-inventory.json"),
        serde_json::to_vec(inventory)?,
    )
    .await
    .context("persist authoritative campaign inventory snapshot")?;
    tokio::fs::write(runtime_dir.join("campaign-inventory-etag"), etag.as_bytes())
        .await
        .context("persist authoritative campaign inventory ETag")?;
    Ok(())
}

async fn cleanup_campaign_aks_platform(config: &OatConfig, platform: &AksPlatform) -> Result<Value> {
    let identity = CampaignIdentity::from_lookup(|name| std::env::var(name).ok())?;
    let campaign_baseline = load_campaign_baseline_receipt(platform, &identity).await?;
    let runtime_dir = platform
        .runtime_dir
        .as_deref()
        .context("campaign runtime directory is not available")?;
    let cached: RunInventory = serde_json::from_slice(
        &tokio::fs::read(runtime_dir.join("campaign-inventory.json"))
            .await
            .context("read persisted campaign inventory")?,
    )
    .context("parse persisted campaign inventory")?;
    let cached_etag = tokio::fs::read_to_string(runtime_dir.join("campaign-inventory-etag"))
        .await
        .context("read persisted campaign inventory ETag")?;
    validate_campaign_etag(cached_etag.trim())?;
    let orchestration_run_id = cached.orchestration_run_id.clone();
    validate_campaign_inventory_identity(
        &cached,
        &identity.campaign_id,
        &identity.owner_id,
        identity.fencing_token,
        &orchestration_run_id,
        &platform.run_id,
    )?;
    if cached.namespace.name != config.namespace || cached.namespace.owner_token != platform.namespace_owner_token {
        bail!("persisted campaign inventory does not match the OAT namespace origin");
    }
    let (mut inventory, mut etag) =
        read_authoritative_campaign_inventory(platform, &identity, &orchestration_run_id, Duration::from_secs(60))
            .await?;
    cached.validate_successor(&inventory)?;
    persist_campaign_inventory_snapshot(platform, &inventory, &etag).await?;
    let actions = [
        CampaignCleanupAction::StopAdmission,
        CampaignCleanupAction::BoundedDrain,
        CampaignCleanupAction::EvidenceCapture,
        CampaignCleanupAction::StopMaintenance,
        CampaignCleanupAction::TerminalCapture,
        CampaignCleanupAction::RemoveNamespace,
        CampaignCleanupAction::RemoveQap,
        CampaignCleanupAction::RemoveFic,
        CampaignCleanupAction::Handoff,
    ];
    let mut receipts = Vec::with_capacity(actions.len());
    for action in actions {
        if !campaign_cleanup_action_needs_execution(&action, &inventory)? {
            receipts.push(
                json!({"step": action.selector(), "status": "already_complete", "source": "authoritative_inventory"}),
            );
            continue;
        }
        let (next_inventory, next_etag, receipt) =
            execute_campaign_cleanup_action(platform, &identity, &orchestration_run_id, &action, &inventory, &etag)
                .await
                .with_context(|| format!("execute custody cleanup {}", action.selector()))?;
        inventory = next_inventory;
        etag = next_etag;
        if let Some(receipt) = receipt {
            receipts.push(serde_json::to_value(receipt)?);
        } else {
            receipts.push(
                json!({"step": action.selector(), "status": "already_complete", "source": "authoritative_inventory"}),
            );
        }
    }
    Ok(json!({"status":"observed", "campaign_baseline":campaign_baseline, "cleanup_receipts":receipts}))
}

async fn campaign_inventory_for_setup(platform: &AksPlatform) -> Result<RunInventory> {
    let runtime_dir = platform
        .runtime_dir
        .as_deref()
        .context("campaign runtime directory is not available")?;
    let inventory: RunInventory = serde_json::from_slice(
        &read_native_file(
            &runtime_dir.join("campaign-inventory.json"),
            OFFICIAL_MEASUREMENT_MAX_BYTES,
        )
        .await
        .context("read persisted campaign inventory for setup")?,
    )
    .context("parse persisted campaign inventory for setup")?;
    let etag = String::from_utf8(
        read_native_file(&runtime_dir.join("campaign-inventory-etag"), 4096)
            .await
            .context("read persisted campaign inventory ETag for setup")?,
    )?;
    validate_campaign_etag(etag.trim())?;
    let identity = CampaignIdentity::from_lookup(|name| std::env::var(name).ok())?;
    validate_campaign_inventory_identity(
        &inventory,
        &identity.campaign_id,
        &identity.owner_id,
        identity.fencing_token,
        &inventory.orchestration_run_id,
        &platform.run_id,
    )?;
    if inventory.namespace.owner_token != platform.namespace_owner_token
        || inventory.namespace.uid != platform.namespace_uid
    {
        bail!("campaign setup inventory does not match the exact namespace owner and UID");
    }
    Ok(inventory)
}

fn validate_campaign_inventory_load_admission(
    inventory: &RunInventory,
    expected_admission: InventoryLoadAdmission,
    expected_database: &str,
    expected_bindings: &[InventoryQapBinding],
) -> Result<()> {
    let qap_is_observed = inventory.qap.as_ref().is_some_and(|qap| {
        qap.database == expected_database
            && qap.bindings.as_slice() == expected_bindings
            && !qap.bindings.is_empty()
            && qap.state == InventoryResourceState::Observed
            && qap.absence.is_empty()
            && qap.reference.is_some()
    });
    let resources_are_observed = !inventory.resources.is_empty()
        && inventory
            .resources
            .iter()
            .all(|resource| resource.state == InventoryResourceState::Observed && resource.uid.is_some());
    let fic_is_observed = inventory
        .fic
        .as_ref()
        .is_some_and(|fic| fic.state == InventoryResourceState::Observed);
    if inventory.load_admission != expected_admission
        || (expected_admission != InventoryLoadAdmission::NotAuthorized
            && (inventory.planned_cell_duration_ms.is_none_or(|duration| duration == 0)
                || inventory.effective_stop_budgets.is_none()))
        || inventory.namespace.uid.is_none()
        || !resources_are_observed
        || !fic_is_observed
        || !qap_is_observed
        || inventory.cleanup.handoff.state == InventoryStepState::Complete
    {
        bail!("campaign load admission requires complete observed namespace, resources, FIC and exact QAP proof");
    }
    inventory.validate()
}

async fn update_campaign_fic_inventory(platform: &AksPlatform, fic: InventoryFic) -> Result<()> {
    let current = campaign_inventory_for_setup(platform).await?;
    let mut next = current.clone();
    next.fic = Some(fic);
    current.validate_successor(&next)?;
    persist_campaign_inventory(platform, &next).await
}

async fn update_campaign_qap_inventory(platform: &AksPlatform, qap: InventoryQap) -> Result<()> {
    let current = campaign_inventory_for_setup(platform).await?;
    let mut next = current.clone();
    next.qap = Some(qap);
    current.validate_successor(&next)?;
    persist_campaign_inventory(platform, &next).await
}

async fn persist_campaign_inventory(platform: &AksPlatform, inventory: &RunInventory) -> Result<()> {
    let runtime_dir = platform
        .runtime_dir
        .as_deref()
        .context("campaign runtime directory is not available")?;
    let etag = inventory.publish_to_custody(platform.campaign_cli_paths()?).await?;
    tokio::fs::write(
        runtime_dir.join("campaign-inventory.json"),
        serde_json::to_vec(inventory)?,
    )
    .await
    .context("persist confirmed campaign inventory")?;
    tokio::fs::write(runtime_dir.join("campaign-inventory-etag"), etag.as_bytes())
        .await
        .context("persist confirmed campaign inventory ETag")?;
    Ok(())
}

#[derive(Clone, Copy)]
struct CampaignCellBudget {
    effective_stop_budgets: EffectiveStopBudgets,
    planned_cell_duration_ms: u64,
}

fn initial_campaign_inventory(
    identity: &CampaignIdentity,
    orchestration_run_id: &str,
    cell_run_id: &str,
    namespace: &str,
    account: &str,
    container: &str,
    budget: CampaignCellBudget,
) -> Result<RunInventory> {
    let step = || InventoryStep {
        state: InventoryStepState::NotStarted,
        deadline_unix_ms: None,
        reason_code: None,
    };
    let inventory = RunInventory {
        schema_version: 1,
        campaign_id: identity.campaign_id.clone(),
        owner_id: identity.owner_id.clone(),
        fencing_token: identity.fencing_token,
        orchestration_run_id: orchestration_run_id.to_owned(),
        cell_run_id: cell_run_id.to_owned(),
        load_admission: InventoryLoadAdmission::NotAuthorized,
        namespace: InventoryNamespace {
            name: namespace.to_owned(),
            uid: None,
            owner_token: identity.owner_id.clone(),
        },
        resources: Vec::new(),
        fic: None,
        qap: None,
        evidence_destination: Some(InventoryEvidenceDestination {
            account: account.to_owned(),
            container: container.to_owned(),
            blob_prefix: format!("campaigns/{}/cells/{}/evidence", identity.campaign_id, cell_run_id),
        }),
        official_measurement: None,
        effective_stop_budgets: Some(budget.effective_stop_budgets.validate()?),
        planned_cell_duration_ms: Some(budget.planned_cell_duration_ms),
        cleanup: InventoryCleanup {
            stop_admission: step(),
            bounded_drain: step(),
            evidence_capture: step(),
            stop_maintenance: step(),
            terminal_capture: step(),
            handoff: step(),
        },
    };
    inventory.validate()?;
    Ok(inventory)
}

async fn initialize_campaign_inventory(
    config: &OatConfig,
    platform: &mut AksPlatform,
    orchestration_run_id: &str,
) -> Result<()> {
    let identity = CampaignIdentity::from_lookup(|name| std::env::var(name).ok())?;
    platform.namespace_owner_token = identity.owner_id.clone();
    let account = std::env::var("CAMPAIGN_STORAGE_ACCOUNT")
        .context("CAMPAIGN_STORAGE_ACCOUNT is required for campaign evidence custody")?;
    let container = std::env::var("CAMPAIGN_STATE_CONTAINER")
        .context("CAMPAIGN_STATE_CONTAINER is required for campaign evidence custody")?;
    let inventory = initial_campaign_inventory(
        &identity,
        orchestration_run_id,
        &platform.run_id,
        &config.namespace,
        &account,
        &container,
        CampaignCellBudget {
            effective_stop_budgets: config.maintenance_stop_budget()?,
            planned_cell_duration_ms: config.timing_budget()?.planned_cell_duration_ms,
        },
    )?;
    persist_campaign_inventory(platform, &inventory).await?;
    validate_campaign_baseline_capture_inventory(&inventory, &identity)?;
    capture_campaign_baseline(platform, &identity).await?;
    Ok(())
}

async fn record_campaign_namespace_uid(config: &OatConfig, platform: &AksPlatform) -> Result<()> {
    if config.campaign_profile != Some(crate::config::CampaignProfile::SingleHotTable) {
        return Ok(());
    }
    let runtime_dir = platform
        .runtime_dir
        .as_deref()
        .context("campaign runtime directory is not available")?;
    let path = runtime_dir.join("campaign-inventory.json");
    let current: RunInventory = serde_json::from_slice(
        &tokio::fs::read(&path)
            .await
            .context("read last confirmed campaign inventory")?,
    )
    .context("parse last confirmed campaign inventory")?;
    let uid = platform
        .namespace_uid
        .as_deref()
        .context("created campaign namespace has no UID")?;
    if current.namespace.name != config.namespace || current.namespace.owner_token != platform.namespace_owner_token {
        bail!("campaign inventory does not match the created namespace ownership");
    }
    if current.namespace.uid.as_deref().is_some_and(|existing| existing != uid) {
        bail!("campaign namespace UID changed after inventory publication");
    }
    let mut next = current.clone();
    next.namespace.uid = Some(uid.to_owned());
    current.validate_successor(&next)?;
    persist_campaign_inventory(platform, &next).await
}

fn set_runtime_replicas(runtime_config: &mut Value, replicas: u32) -> Result<()> {
    if !(1..=16).contains(&replicas) {
        bail!("AKS cell replicas must be between 1 and 16");
    }
    let object = runtime_config
        .as_object_mut()
        .context("runtime config must be an object")?;
    object.insert("collectorReplicas".to_owned(), json!(replicas));
    object.insert("routerReplicas".to_owned(), json!(replicas));
    Ok(())
}

fn require_single_scale_out_collector(config: &OatConfig, replicas: u32) -> Result<()> {
    if config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable) && replicas != 1 {
        bail!("scale_out_single_table runtime requires exactly one collector replica");
    }
    Ok(())
}

fn project_scale_out_runtime(runtime_config: &mut Value, config: &OatConfig, replicas: u32) -> Result<()> {
    if config.campaign_profile != Some(crate::config::CampaignProfile::ScaleOutSingleTable) {
        return Ok(());
    }
    require_single_scale_out_collector(config, replicas)?;
    let [hot_table] = config.tables.as_slice() else {
        bail!("scale_out_single_table runtime requires exactly one hot table");
    };
    if hot_table != "sqlserver_availability_replica_states" {
        bail!("scale_out_single_table runtime hot table is invalid");
    }
    set_runtime_replicas(runtime_config, replicas)?;
    runtime_config["campaignProfile"] = serde_json::to_value(config.campaign_profile)?;
    runtime_config["hotTable"] = json!(hot_table);
    Ok(())
}

async fn prepare_aks_platform(
    config: &OatConfig,
    platform: &mut AksPlatform,
    orchestration_run_id: &str,
    replicas: u32,
) -> Result<()> {
    require_single_scale_out_collector(config, replicas)?;
    let parent = std::env::temp_dir().join("arrowcol-oat");
    tokio::fs::create_dir_all(&parent)
        .await
        .with_context(|| format!("create private OAT runtime directory {}", parent.display()))?;
    let runtime_dir = parent.join(&platform.run_id);
    tokio::fs::create_dir(&runtime_dir)
        .await
        .with_context(|| format!("create run-specific OAT runtime directory {}", runtime_dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&runtime_dir, std::fs::Permissions::from_mode(0o700))
            .await
            .context("restrict run-specific AKS runtime directory permissions")?;
    }
    platform.runtime_dir = Some(runtime_dir.clone());
    if config.campaign_profile == Some(crate::config::CampaignProfile::SingleHotTable) {
        platform.campaign_cli_paths = Some(OwnedAksCampaignConfigPaths::from_config(config)?);
        initialize_campaign_inventory(config, platform, orchestration_run_id).await?;
    }

    let runtime_config_path = PathBuf::from(
        config
            .platform
            .runtime_config_file
            .as_deref()
            .context("AKS platform runtime_config_file is missing")?,
    );
    let runtime_bytes = tokio::fs::read(&runtime_config_path)
        .await
        .with_context(|| format!("read deployed-stamp runtime config {}", runtime_config_path.display()))?;
    let mut runtime_config: Value =
        serde_json::from_slice(&runtime_bytes).context("parse deployed-stamp runtime config JSON")?;
    if runtime_config.get("stamp").and_then(Value::as_str) != config.stamp.as_deref() {
        bail!("deployed-stamp runtime config does not match the OAT-selected stamp");
    }
    let shared_data_root = runtime_config
        .get("deltaRoot")
        .and_then(Value::as_str)
        .context("deployed-stamp runtime config has no deltaRoot")?;
    let run_data_root = run_delta_root(shared_data_root, &platform.run_id)?;
    platform.qap_relative_data_root = Some(qap_relative_data_root(&run_data_root)?);
    platform.delta_root = Some(run_data_root.clone());
    runtime_config["deltaRoot"] = json!(run_data_root);
    runtime_config["namespace"] = json!(config.namespace);
    runtime_config["oatRunId"] = json!(platform.run_id);
    runtime_config["quiver"] = json!(config.tunables.wal.is_some());
    runtime_config["effective_stop_budgets"] = serde_json::to_value(config.maintenance_stop_budget()?)?;
    runtime_config["resources"] = serde_json::to_value(config.effective_aks_resources())?;
    project_scale_out_runtime(&mut runtime_config, config, replicas)?;
    runtime_config["collectorWorkers"] = json!(
        config
            .dfe_worker_concurrency
            .iter()
            .copied()
            .max()
            .context("empty worker concurrency sweep")?
    );
    if let Some(maintenance) = maintenance_runtime(config)? {
        runtime_config["maintenance"] = maintenance;
        runtime_config["images"]["maintenance"] = json!(
            config
                .maintenance_image
                .as_deref()
                .context("enabled AKS maintenance requires its pinned image")?
        );
    } else {
        runtime_config
            .as_object_mut()
            .context("runtime config must be an object")?
            .remove("maintenance");
    }
    let generated_config = runtime_dir.join("runtime-config.json");
    tokio::fs::write(&generated_config, serde_json::to_vec(&runtime_config)?)
        .await
        .context("write run-scoped AKS runtime config")?;

    let bootstrap_env = config
        .platform
        .bootstrap_env_file
        .as_deref()
        .context("AKS platform bootstrap_env_file is missing")?;
    let bootstrap_env_file = Path::new(bootstrap_env);
    let secrets_dir = runtime_dir.join("secrets");
    let runtime_script = std::env::current_dir()?.join("projects/arrowcol-oat/runtime.ts");
    run_npx_runtime(
        &runtime_script,
        &["prepare-secrets", bootstrap_env, secrets_dir.to_string_lossy().as_ref()],
    )
    .await
    .context("prepare private run-specific AKS secrets")?;

    let identity = render_runtime_phase(&runtime_script, &generated_config, "identity", &runtime_dir).await?;
    let namespace_resources = identity
        .iter()
        .filter(|resource| resource.get("kind").and_then(Value::as_str) == Some("Namespace"))
        .cloned()
        .collect::<Vec<_>>();
    if namespace_resources.len() != 1
        || namespace_resources[0].pointer("/metadata/name").and_then(Value::as_str) != Some(config.namespace.as_str())
    {
        bail!("runtime identity phase did not render exactly the expected run namespace");
    }
    let mut namespace_resource = namespace_resources[0].clone();
    namespace_resource["metadata"]["labels"]["arrowcol-oat/run-id"] = json!(platform.run_id);
    namespace_resource["metadata"]["annotations"]["arrowcol-oat/run-id"] = json!(platform.run_id);
    namespace_resource["metadata"]["labels"]["arrowcol-oat/owner-token"] = json!(platform.namespace_owner_token);
    namespace_resource["metadata"]["annotations"]["arrowcol-oat/owner-token"] = json!(platform.namespace_owner_token);
    let namespace_payload = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [namespace_resource],
    }))?;
    platform.namespace_create_attempted = true;
    kubectl(config, vec!["create", "-f", "-"], Some(namespace_payload))
        .await
        .context("create unique OAT-owned namespace without mutating an existing namespace")?;
    platform.namespace_created = true;
    let namespace = kubectl(config, vec!["get", "namespace", &config.namespace, "-o", "json"], None)
        .await
        .context("verify newly created OAT namespace ownership")?;
    let namespace: Value = serde_json::from_str(&namespace).context("parse created OAT namespace")?;
    let uid = namespace
        .pointer("/metadata/uid")
        .and_then(Value::as_str)
        .context("created OAT namespace has no UID")?;
    if namespace
        .pointer("/metadata/labels/arrowcol-oat~1run-id")
        .and_then(Value::as_str)
        != Some(platform.run_id.as_str())
    {
        bail!("created AKS namespace is missing this run's ownership label");
    }
    platform.namespace_uid = Some(uid.to_owned());
    record_campaign_namespace_uid(config, platform).await?;

    let service_accounts = identity
        .into_iter()
        .filter(|resource| resource.get("kind").and_then(Value::as_str) == Some("ServiceAccount"))
        .collect::<Vec<_>>();
    if service_accounts.len() != 1 {
        bail!("runtime identity phase must render exactly one workload identity ServiceAccount");
    }
    let service_account = service_accounts[0]
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .context("rendered workload identity ServiceAccount has no name")?
        .to_owned();
    platform.service_account = Some(service_account.clone());
    apply_runtime_resources(config, platform, service_accounts).await?;
    let fic_plan = run_run_fic_plan(
        &platform.run_id,
        &config.namespace,
        &service_account,
        bootstrap_env_file,
    )
    .await?;
    let fic_plan_path = runtime_dir.join(format!("run-fic-{}-identity-plan.json", platform.run_id));
    write_private_secret(&fic_plan_path, &serde_json::to_string(&fic_plan)?)
        .context("persist immutable run-owned workload identity plan")?;
    if config.campaign_profile == Some(crate::config::CampaignProfile::SingleHotTable) {
        update_campaign_fic_inventory(
            platform,
            InventoryFic {
                resource_id: Some(fic_plan.resource_id.clone()),
                name: Some(fic_plan.credential_name.clone()),
                state: InventoryResourceState::Planned,
                absence: None,
                plan: Some(fic_plan.clone()),
            },
        )
        .await?;
        update_campaign_fic_inventory(
            platform,
            InventoryFic {
                resource_id: Some(fic_plan.resource_id.clone()),
                name: Some(fic_plan.credential_name.clone()),
                state: InventoryResourceState::Uncertain,
                absence: None,
                plan: Some(fic_plan.clone()),
            },
        )
        .await?;
    }
    let fic_record = runtime_dir.join(format!("run-fic-{}.json", platform.run_id));
    platform.fic_record_path = Some(fic_record.clone());
    platform.fic_prepare_attempted = true;
    run_run_fic_helper(
        "prepare-run-fic",
        &platform.run_id,
        Some(&config.namespace),
        Some(&service_account),
        &fic_record,
        FicHelperOptions {
            wait_seconds: None,
            identity_plan_path: Some(&fic_plan_path),
        },
        bootstrap_env_file,
    )
    .await
    .context("prepare exact run-owned workload identity credential")?;
    require_nonempty_record(&fic_record).await?;
    run_run_fic_helper(
        "probe-run-fic",
        &platform.run_id,
        Some(&config.namespace),
        Some(&service_account),
        &fic_record,
        FicHelperOptions {
            wait_seconds: Some("300"),
            identity_plan_path: None,
        },
        bootstrap_env_file,
    )
    .await
    .context("wait for run-owned workload identity token exchange before workload rollout")?;
    let ready_snapshot = runtime_dir.join(format!("run-fic-{}-ready.json", platform.run_id));
    let ready_record = tokio::fs::read_to_string(&fic_record)
        .await
        .context("read ready run-owned workload identity snapshot")?;
    write_private_secret(&ready_snapshot, &ready_record)
        .context("persist immutable ready run-owned workload identity snapshot")?;
    platform.fic_ready_snapshot_path = Some(ready_snapshot);
    if config.campaign_profile == Some(crate::config::CampaignProfile::SingleHotTable) {
        update_campaign_fic_inventory(
            platform,
            InventoryFic {
                resource_id: Some(fic_plan.resource_id.clone()),
                name: Some(fic_plan.credential_name.clone()),
                state: InventoryResourceState::Observed,
                absence: None,
                plan: Some(fic_plan),
            },
        )
        .await?;
    }
    create_runtime_secrets(config, platform, &secrets_dir).await?;

    let foundation = render_runtime_phase(&runtime_script, &generated_config, "foundation", &runtime_dir).await?;
    apply_runtime_resources(config, platform, without_identity_resources(foundation)).await?;
    rollout(config, "arrowcol-sql").await?;

    let bootstrap = render_runtime_phase(&runtime_script, &generated_config, "bootstrap", &runtime_dir).await?;
    apply_runtime_resources(config, platform, without_identity_resources(bootstrap)).await?;
    kubectl(
        config,
        vec![
            "wait",
            "--namespace",
            &config.namespace,
            "--for=condition=complete",
            "job/arrowcol-migrate-v11",
            "--timeout=600s",
        ],
        None,
    )
    .await
    .context("wait for run-scoped SQL bootstrap and migration Job")?;
    kubectl(
        config,
        vec![
            "delete",
            "job",
            "arrowcol-migrate-v11",
            "--namespace",
            &config.namespace,
            "--wait=true",
        ],
        None,
    )
    .await
    .context("remove completed run-scoped bootstrap Job")?;

    let mut workload = render_runtime_phase(&runtime_script, &generated_config, "workload", &runtime_dir).await?;
    inject_workload_environment(config, &mut workload)?;
    inject_router_oat_pin_ttl(&mut workload, config.run_pin_ttl_ms)?;
    let router_yaml = rendered_router_yaml(&workload)?;
    arrowcol_router_rust::RouterConfig::from_yaml(router_yaml.as_bytes())
        .context("validate rendered router YAML with the router config validator")?;
    platform.router_yaml = Some(router_yaml);
    let (catalog, data_plane) = partition_aks_workload_resources(without_identity_resources(workload))?;
    apply_runtime_resources(config, platform, catalog).await?;
    rollout(config, "arrowcol-catalog").await?;
    apply_runtime_resources(config, platform, data_plane).await?;
    for name in ["arrowcol", "arrowcol-router"] {
        rollout(config, name).await?;
    }
    start_router_metrics_tunnel(config, platform).await?;
    Ok(())
}

fn partition_aks_workload_resources(resources: Vec<Value>) -> Result<(Vec<Value>, Vec<Value>)> {
    if resources
        .iter()
        .filter(|resource| {
            resource["kind"] == "Deployment"
                && resource.pointer("/metadata/name").and_then(Value::as_str) == Some("arrowcol-catalog")
        })
        .count()
        != 1
    {
        bail!("AKS workload requires exactly one catalog Deployment before data-plane startup");
    }
    Ok(resources.into_iter().partition(|resource| {
        resource["kind"] != "Deployment"
            || resource.pointer("/metadata/name").and_then(Value::as_str) == Some("arrowcol-catalog")
    }))
}

fn use_operator_cli_identity(command: &mut Command) {
    for variable in ["AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE"] {
        command.env_remove(variable);
    }
}

fn selected_aks_stamp_config_path(workspace: &Path) -> Result<PathBuf> {
    let configured = std::env::var_os("ARROWCOL_AKS_STAMP_CONFIG_FILE")
        .context("selected AKS stamp configuration was not forwarded to the owned server")?;
    resolve_aks_stamp_config_path(workspace, &PathBuf::from(configured))
}

fn resolve_aks_stamp_config_path(workspace: &Path, configured: &Path) -> Result<PathBuf> {
    let path = if configured.is_absolute() {
        configured.to_path_buf()
    } else {
        workspace.join(configured)
    };
    let workspace = workspace
        .canonicalize()
        .context("resolve OAT workspace for AKS stamp configuration")?;
    let path = path
        .canonicalize()
        .context("resolve selected AKS stamp configuration")?;
    if !path.starts_with(&workspace) {
        bail!("selected AKS stamp configuration is outside the OAT workspace");
    }
    Ok(path)
}

fn configured_bootstrap_env_file(config: &OatConfig) -> Result<&Path> {
    let path = config
        .platform
        .bootstrap_env_file
        .as_deref()
        .filter(|path| !path.trim().is_empty())
        .map(Path::new)
        .context("AKS platform bootstrap_env_file is missing")?;
    if !path.is_file() {
        bail!("AKS platform bootstrap_env_file must identify an existing file");
    }
    Ok(path)
}

#[derive(Clone, Copy)]
pub(crate) struct AksHelperConfigPaths<'a> {
    stamp_config_path: &'a Path,
    bootstrap_env_file: &'a Path,
}

#[derive(Clone, Debug)]
struct OwnedAksCampaignConfigPaths {
    stamp_config_path: PathBuf,
    bootstrap_env_file: PathBuf,
}

impl OwnedAksCampaignConfigPaths {
    fn from_config(config: &OatConfig) -> Result<Self> {
        let workspace = std::env::current_dir().context("resolve workspace for campaign custody config")?;
        let stamp_config_path = selected_aks_stamp_config_path(&workspace)?;
        let env_path = configured_bootstrap_env_file(config)?;
        let bootstrap_env_file = if env_path.is_absolute() {
            env_path.to_path_buf()
        } else {
            workspace.join(env_path)
        }
        .canonicalize()
        .context("resolve selected private campaign environment file")?;
        Ok(Self {
            stamp_config_path,
            bootstrap_env_file,
        })
    }

    fn as_borrowed(&self) -> AksHelperConfigPaths<'_> {
        AksHelperConfigPaths {
            stamp_config_path: &self.stamp_config_path,
            bootstrap_env_file: &self.bootstrap_env_file,
        }
    }
}

impl AksPlatform {
    fn campaign_cli_paths(&self) -> Result<AksHelperConfigPaths<'_>> {
        self.campaign_cli_paths
            .as_ref()
            .map(OwnedAksCampaignConfigPaths::as_borrowed)
            .context("campaign custody CLI paths were not initialized before use")
    }
}

fn aks_campaign_cli_command(
    config_paths: AksHelperConfigPaths<'_>,
    action: &str,
    action_args: &[String],
) -> Result<Command> {
    let workspace = std::env::current_dir().context("resolve repository root for campaign custody helper")?;
    let mut command = Command::new("npx");
    command
        .current_dir(workspace)
        .args(["--no-install", "tsx", "projects/arrowcol-oat/aks-campaign.ts", action])
        .args(action_args)
        .arg("--env-file")
        .arg(config_paths.bootstrap_env_file)
        .arg("--stamp-config")
        .arg(config_paths.stamp_config_path)
        .kill_on_drop(true);
    Ok(command)
}

#[derive(Clone, Copy)]
struct FicHelperOptions<'a> {
    wait_seconds: Option<&'a str>,
    identity_plan_path: Option<&'a Path>,
}

async fn run_run_fic_plan(
    run_id: &str,
    namespace: &str,
    service_account: &str,
    bootstrap_env_file: &Path,
) -> Result<InventoryFicPlan> {
    let workspace = std::env::current_dir()?;
    let script = workspace.join("infra/azure/bootstrap.ts");
    let stamp_config_path = selected_aks_stamp_config_path(&workspace)?;
    let mut command = oat_qap_helper_launcher(&script)?;
    command.args([
        "plan-run-fic",
        "--read-only",
        "--run-id",
        run_id,
        "--namespace",
        namespace,
        "--service-account",
        service_account,
    ]);
    command.arg("--env-file").arg(bootstrap_env_file);
    command.arg("--stamp-config").arg(&stamp_config_path);
    let output = tokio::time::timeout(Duration::from_secs(60), run_oat_qap_helper_output(command))
        .await
        .context("run FIC planning exceeded its read-only deadline")??;
    if !output.status.success() {
        bail!(
            "plan-run-fic helper failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    serde_json::from_slice(&output.stdout).context("parse authoritative run FIC plan")
}

fn run_fic_helper_command(
    operation: &str,
    run_id: &str,
    namespace: Option<&str>,
    service_account: Option<&str>,
    record_path: &Path,
    options: FicHelperOptions<'_>,
    config_paths: AksHelperConfigPaths<'_>,
) -> Result<Command> {
    let workspace = std::env::current_dir()?;
    let script = workspace.join("infra/azure/bootstrap.ts");
    let mut command = Command::new("npx");
    command
        .current_dir(workspace)
        .args(["--no-install", "tsx"])
        .kill_on_drop(true);
    use_operator_cli_identity(&mut command);
    command.arg(script);
    command.args([operation, "--run-id", run_id]);
    command.arg("--env-file").arg(config_paths.bootstrap_env_file);
    command.arg("--stamp-config").arg(config_paths.stamp_config_path);
    if let Some(namespace) = namespace {
        command.args(["--namespace", namespace]);
    }
    if let Some(service_account) = service_account {
        command.args(["--service-account", service_account]);
    }
    command.arg("--record");
    command.arg(record_path);
    if let Some(wait_seconds) = options.wait_seconds {
        command.args(["--wait-seconds", wait_seconds]);
    }
    match (operation, options.identity_plan_path) {
        ("prepare-run-fic", Some(path)) => {
            command.arg("--identity-plan").arg(path);
        }
        ("prepare-run-fic", None) => bail!("run FIC preparation requires its persisted identity plan"),
        (_, Some(_)) => bail!("identity plans are only accepted when preparing a run FIC"),
        (_, None) => {}
    }
    Ok(command)
}

async fn run_run_fic_helper(
    operation: &str,
    run_id: &str,
    namespace: Option<&str>,
    service_account: Option<&str>,
    record_path: &Path,
    options: FicHelperOptions<'_>,
    bootstrap_env_file: &Path,
) -> Result<()> {
    let workspace = std::env::current_dir()?;
    let stamp_config_path = selected_aks_stamp_config_path(&workspace)?;
    let output = run_fic_helper_command(
        operation,
        run_id,
        namespace,
        service_account,
        record_path,
        options,
        AksHelperConfigPaths {
            stamp_config_path: &stamp_config_path,
            bootstrap_env_file,
        },
    )?
    .output()
    .await
    .with_context(|| format!("start {operation} helper"))?;
    if !output.status.success() {
        bail!(
            "{operation} helper failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn oat_qap_helper_launcher(script: &Path) -> Result<Command> {
    let mut command = Command::new("npx");
    command
        .current_dir(std::env::current_dir()?)
        .args(["--no-install", "tsx"])
        .kill_on_drop(true);
    use_operator_cli_identity(&mut command);
    command.arg(script);
    Ok(command)
}

#[cfg(unix)]
struct OwnedQapHelperGroup(rustix::process::Pid);

#[cfg(unix)]
impl Drop for OwnedQapHelperGroup {
    fn drop(&mut self) {
        match rustix::process::kill_process_group(self.0, rustix::process::Signal::KILL) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => {}
            Err(error) => tracing::error!(%error, group = ?self.0, "QAP helper process-group cleanup failed"),
        }
    }
}

#[cfg(unix)]
async fn run_oat_qap_helper_output(mut command: Command) -> Result<std::process::Output> {
    command
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = command.spawn().context("spawn owned QAP helper")?;
    let pid = i32::try_from(child.id().context("QAP helper PID unavailable")?).context("QAP helper PID invalid")?;
    let _group = OwnedQapHelperGroup(rustix::process::Pid::from_raw(pid).context("QAP helper PID invalid")?);
    Ok(child.wait_with_output().await?)
}

#[cfg(not(unix))]
async fn run_oat_qap_helper_output(_command: Command) -> Result<std::process::Output> {
    bail!("owned QAP helper process-group cancellation requires Unix")
}

fn run_oat_qap_helper_command(
    operation: &str,
    run_id: &str,
    manifest_path: Option<&Path>,
    workload_record_path: Option<&Path>,
    workload_stop_record_path: Option<&Path>,
    record_path: &Path,
    config_paths: AksHelperConfigPaths<'_>,
) -> Result<Command> {
    let script = std::env::current_dir()?.join("infra/azure/bootstrap.ts");
    let mut command = oat_qap_helper_launcher(&script)?;
    command.arg(operation);
    command.arg("--env-file").arg(config_paths.bootstrap_env_file);
    command.arg("--stamp-config").arg(config_paths.stamp_config_path);
    if operation == "bind-oat-qap" {
        let manifest_path = manifest_path.context("QAP binding requires its exact manifest path")?;
        command.args(["--run-id", run_id, "--manifest"]);
        command.arg(manifest_path);
        command.arg("--workload-record");
        command.arg(workload_record_path.context("QAP binding requires the verified workload ownership record")?);
    }
    if operation == "cleanup-oat-qap" {
        command.arg("--workload-stop-record");
        command.arg(workload_stop_record_path.context("QAP cleanup requires the exact-run workload stop receipt")?);
    }
    command.arg("--record");
    command.arg(record_path);
    Ok(command)
}

async fn run_oat_qap_helper(
    operation: &str,
    run_id: &str,
    manifest_path: Option<&Path>,
    workload_record_path: Option<&Path>,
    workload_stop_record_path: Option<&Path>,
    record_path: &Path,
    bootstrap_env_file: &Path,
) -> Result<()> {
    let workspace = std::env::current_dir()?;
    let stamp_config_path = selected_aks_stamp_config_path(&workspace)?;
    let command = run_oat_qap_helper_command(
        operation,
        run_id,
        manifest_path,
        workload_record_path,
        workload_stop_record_path,
        record_path,
        AksHelperConfigPaths {
            stamp_config_path: &stamp_config_path,
            bootstrap_env_file,
        },
    )?;
    let output = run_oat_qap_helper_output(command)
        .await
        .with_context(|| format!("start {operation} helper"))?;
    if !output.status.success() {
        bail!(
            "{operation} helper failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}
async fn run_oat_qap_reference(record_path: &Path, bootstrap_env_file: &Path) -> Result<InventoryQapReference> {
    let workspace = std::env::current_dir()?;
    let stamp_config_path = selected_aks_stamp_config_path(&workspace)?;
    let command = run_oat_qap_helper_command(
        "oat-qap-reference",
        "",
        None,
        None,
        None,
        record_path,
        AksHelperConfigPaths {
            stamp_config_path: &stamp_config_path,
            bootstrap_env_file,
        },
    )?;
    let output = tokio::time::timeout(Duration::from_secs(60), run_oat_qap_helper_output(command))
        .await
        .context("QAP reference capture exceeded its deadline")??;
    if !output.status.success() {
        bail!(
            "oat-qap-reference helper failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    serde_json::from_slice(&output.stdout).context("parse immutable QAP reference")
}

async fn require_nonempty_record(record_path: &Path) -> Result<()> {
    let metadata = tokio::fs::metadata(record_path)
        .await
        .with_context(|| format!("inspect workload identity ownership record {}", record_path.display()))?;
    if !metadata.is_file() || metadata.len() == 0 {
        bail!("workload identity helper did not persist a nonempty ownership record");
    }
    Ok(())
}

async fn start_router_metrics_tunnel(config: &OatConfig, platform: &mut AksPlatform) -> Result<()> {
    start_router_metrics_tunnel_with(config, platform, Command::new("kubectl")).await
}

async fn start_router_metrics_tunnel_with(
    config: &OatConfig,
    platform: &mut AksPlatform,
    mut command: Command,
) -> Result<()> {
    let reservation = TcpListener::bind("127.0.0.1:0")
        .await
        .context("reserve a loopback port for the AKS router metrics tunnel")?;
    let port = reservation.local_addr()?.port();
    drop(reservation);

    let port_mapping = format!("{port}:50050");
    if let Some(context) = &config.kubectl_context {
        command.args(["--context", context]);
    }
    command
        .args([
            "port-forward",
            "--address",
            "127.0.0.1",
            "--namespace",
            &config.namespace,
            "service/arrowcol-router",
            &port_mapping,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().context("start owned AKS router metrics port-forward")?;
    let stderr = child
        .stderr
        .take()
        .context("capture AKS router port-forward diagnostics")?;
    let stderr = tokio::spawn(async move {
        let mut bytes = Vec::new();
        let mut stderr = stderr;
        stderr.read_to_end(&mut bytes).await.map(|_| bytes)
    });
    let mut forward = OwnedKubectlProcess { child, stderr };
    let metrics_url = format!("http://127.0.0.1:{port}/metrics");
    let ready_url = format!("http://127.0.0.1:{port}/readyz");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("build AKS router tunnel readiness client")?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = forward
            .child
            .try_wait()
            .context("check AKS router port-forward process")?
        {
            let stderr = collect_kubectl_stderr(forward.stderr).await?;
            bail!(
                "AKS router port-forward exited before readiness with {status}: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        if client
            .get(&ready_url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            platform.router_metrics_url = Some(metrics_url);
            platform.router_tunnel = Some(forward);
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let stderr = collect_kubectl_stderr(stop_kubectl_process(forward).await?).await?;
            bail!(
                "AKS router metrics tunnel did not become ready within 60 seconds: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn start_aks_collector_metrics_tunnel(config: &OatConfig) -> Result<(String, OwnedKubectlProcess)> {
    let snapshot = kubectl(
        config,
        vec![
            "get",
            "pods",
            "--namespace",
            &config.namespace,
            "--selector",
            &config.arrowcol_pod_selector,
            "-o",
            "json",
        ],
        None,
    )
    .await?;
    let pods = active_pod_names(&serde_json::from_str(&snapshot)?)?;
    if pods.len() != 1 {
        bail!("publication backlog scrape requires exactly one active ArrowCol collector");
    }
    let pod = pods
        .into_iter()
        .next()
        .context("active ArrowCol collector pod is missing")?;
    let target = format!("pod/{pod}");
    let reservation = TcpListener::bind("127.0.0.1:0")
        .await
        .context("reserve a loopback port for the collector metrics tunnel")?;
    let port = reservation.local_addr()?.port();
    drop(reservation);
    let port_mapping = format!("{port}:32011");
    let mut command = Command::new("kubectl");
    if let Some(context) = &config.kubectl_context {
        command.args(["--context", context]);
    }
    command
        .args([
            "port-forward",
            "--address",
            "127.0.0.1",
            "--namespace",
            &config.namespace,
            &target,
            &port_mapping,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().context("start owned collector metrics port-forward")?;
    let stderr = child
        .stderr
        .take()
        .context("capture collector metrics port-forward diagnostics")?;
    let stderr = tokio::spawn(async move {
        let mut bytes = Vec::new();
        let mut stderr = stderr;
        stderr.read_to_end(&mut bytes).await.map(|_| bytes)
    });
    let mut forward = OwnedKubectlProcess { child, stderr };
    let metrics_url = format!("http://127.0.0.1:{port}/metrics");
    let ready_url = format!("http://127.0.0.1:{port}/readyz");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("build collector metrics tunnel readiness client")?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = forward
            .child
            .try_wait()
            .context("check collector metrics port-forward process")?
        {
            let stderr = collect_kubectl_stderr(forward.stderr).await?;
            bail!(
                "collector metrics port-forward exited before readiness with {status}: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        if client
            .get(&ready_url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok((metrics_url, forward));
        }
        if tokio::time::Instant::now() >= deadline {
            let stderr = collect_kubectl_stderr(stop_kubectl_process(forward).await?).await?;
            bail!(
                "collector metrics tunnel did not become ready within 10 seconds: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn ensure_router_metrics_tunnel(config: &OatConfig, platform: &mut AksPlatform) -> Result<bool> {
    ensure_router_metrics_tunnel_with(config, platform, || Command::new("kubectl")).await
}

async fn ensure_router_metrics_tunnel_with<F>(
    config: &OatConfig,
    platform: &mut AksPlatform,
    mut command: F,
) -> Result<bool>
where
    F: FnMut() -> Command,
{
    if let Some(mut forward) = platform.router_tunnel.take() {
        match forward
            .child
            .try_wait()
            .context("check owned AKS router metrics tunnel")?
        {
            None => {
                platform.router_tunnel = Some(forward);
                return Ok(false);
            }
            Some(status) => {
                platform.router_metrics_url = None;
                let stderr = collect_kubectl_stderr(forward.stderr).await?;
                tracing::warn!(
                    exit_status = %status,
                    stderr = %String::from_utf8_lossy(&stderr),
                    "owned AKS router metrics port-forward exited; restarting it"
                );
            }
        }
    }
    platform.router_metrics_url = None;
    start_router_metrics_tunnel_with(config, platform, command()).await?;
    Ok(true)
}

async fn stop_kubectl_process(mut forward: OwnedKubectlProcess) -> Result<JoinHandle<std::io::Result<Vec<u8>>>> {
    if forward
        .child
        .try_wait()
        .context("check owned kubectl process before stopping")?
        .is_none()
    {
        forward.child.start_kill().context("stop owned kubectl process")?;
    }
    forward
        .child
        .wait()
        .await
        .context("wait for owned kubectl process to stop")?;
    Ok(forward.stderr)
}

async fn collect_kubectl_stderr(stderr: JoinHandle<std::io::Result<Vec<u8>>>) -> Result<Vec<u8>> {
    stderr
        .await
        .context("join kubectl process diagnostics reader")?
        .context("read kubectl process diagnostics")
}

async fn start_kubectl_api_proxy(config: &OatConfig) -> Result<(OwnedKubectlProcess, String)> {
    let reservation = TcpListener::bind("127.0.0.1:0")
        .await
        .context("reserve a local port for the authenticated Kubernetes API proxy")?;
    let port = reservation
        .local_addr()
        .context("read reserved Kubernetes API proxy port")?
        .port();
    drop(reservation);

    let port_arg = port.to_string();
    let mut command = Command::new("kubectl");
    if let Some(context) = config.kubectl_context.as_deref() {
        command.args(["--context", context]);
    }
    command
        .args(["proxy", "--address", "127.0.0.1", "--port", &port_arg])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().context("start authenticated Kubernetes API proxy")?;
    let stderr = child
        .stderr
        .take()
        .context("capture Kubernetes API proxy diagnostics")?;
    let stderr = tokio::spawn(async move {
        let mut bytes = Vec::new();
        let mut stderr = stderr;
        stderr.read_to_end(&mut bytes).await.map(|_| bytes)
    });
    let mut proxy = OwnedKubectlProcess { child, stderr };
    let api_url = format!("http://127.0.0.1:{port}");
    let readiness_url = format!("{api_url}/api");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .no_proxy()
        .build()
        .context("build Kubernetes API proxy readiness client")?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        if let Some(status) = proxy.child.try_wait().context("check Kubernetes API proxy process")? {
            let stderr = collect_kubectl_stderr(proxy.stderr).await?;
            bail!(
                "authenticated Kubernetes API proxy exited before readiness with {status}: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        if client
            .get(&readiness_url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok((proxy, api_url));
        }
        if tokio::time::Instant::now() >= deadline {
            let stderr = collect_kubectl_stderr(stop_kubectl_process(proxy).await?).await?;
            bail!(
                "authenticated Kubernetes API proxy did not become ready within 30 seconds: {}",
                String::from_utf8_lossy(&stderr).trim()
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn delete_namespace_with_uid_precondition(api_url: &str, namespace: &str, uid: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .no_proxy()
        .build()
        .context("build Kubernetes namespace deletion client")?;
    let endpoint = format!("{}/api/v1/namespaces/{namespace}", api_url.trim_end_matches('/'));
    let response = client
        .delete(endpoint)
        .json(&serde_json::json!({
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "preconditions": { "uid": uid }
        }))
        .send()
        .await
        .context("send UID-preconditioned Kubernetes namespace deletion")?;
    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .context("read Kubernetes namespace deletion failure response")?;
        bail!(
            "UID-preconditioned namespace deletion returned HTTP {status}: {}",
            body.trim()
        );
    }
    Ok(())
}

async fn delete_run_owned_namespace_with_uid(config: &OatConfig, namespace_uid: &str) -> Result<()> {
    let (proxy, api_url) = start_kubectl_api_proxy(config).await?;
    let delete_result = delete_namespace_with_uid_precondition(&api_url, &config.namespace, namespace_uid).await;
    let stop_result = match stop_kubectl_process(proxy).await {
        Ok(stderr) => collect_kubectl_stderr(stderr).await.map(|_| ()),
        Err(error) => Err(error),
    };
    match (delete_result, stop_result) {
        (Err(error), Err(stop_error)) => {
            Err(error.context(format!("also failed to stop Kubernetes API proxy: {stop_error:#}")))
        }
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error).context("stop Kubernetes API proxy after namespace deletion request"),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn wait_for_run_owned_namespace_absence(config: &OatConfig) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
    loop {
        let output = kubectl(
            config,
            vec![
                "get",
                "namespace",
                &config.namespace,
                "--ignore-not-found=true",
                "-o",
                "name",
                "--request-timeout=30s",
            ],
            None,
        )
        .await
        .context("confirm run-owned AKS namespace absence after deletion")?;
        if output.trim().is_empty() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "run-owned AKS namespace still exists after bounded deletion wait: {}",
                output.trim()
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn without_identity_resources(resources: Vec<Value>) -> Vec<Value> {
    resources
        .into_iter()
        .filter(|resource| {
            !matches!(
                resource.get("kind").and_then(Value::as_str),
                Some("Namespace" | "ServiceAccount")
            )
        })
        .collect()
}

async fn render_runtime_phase(
    runtime_script: &Path,
    config_path: &Path,
    phase: &str,
    runtime_dir: &Path,
) -> Result<Vec<Value>> {
    let output_path = runtime_dir.join(format!("{phase}.json"));
    run_npx_runtime(
        runtime_script,
        &[
            "render",
            config_path.to_string_lossy().as_ref(),
            phase,
            output_path.to_string_lossy().as_ref(),
        ],
    )
    .await
    .with_context(|| format!("render AKS runtime phase {phase}"))?;
    let output = tokio::fs::read(&output_path)
        .await
        .with_context(|| format!("read rendered AKS {phase} resources"))?;
    let list: Value = serde_json::from_slice(&output).context("parse rendered AKS resource list")?;
    let resources = list
        .get("items")
        .and_then(Value::as_array)
        .context("runtime phase output is not a Kubernetes List")?
        .clone();
    if resources.is_empty() {
        bail!("AKS runtime phase {phase} rendered no resources");
    }
    Ok(resources)
}

async fn run_npx_runtime(runtime_script: &Path, args: &[&str]) -> Result<()> {
    let workspace = std::env::current_dir().context("resolve repository root for OAT runtime helper")?;
    let mut command = Command::new("npx");
    command
        .current_dir(&workspace)
        .args(["--no-install", "tsx"])
        .arg(runtime_script)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    run_command(command, "run existing OAT runtime helper").await?;
    Ok(())
}

fn set_origin_markers(metadata: &mut Value, run_id: &str, owner_token: &str) -> Result<()> {
    let metadata = metadata
        .as_object_mut()
        .context("run-owned Kubernetes metadata must be an object")?;
    for key in ["labels", "annotations"] {
        let values = metadata.entry(key).or_insert_with(|| json!({}));
        let values = values
            .as_object_mut()
            .context("run-owned Kubernetes metadata labels and annotations must be objects")?;
        values.insert("arrowcol-oat/run-id".to_owned(), json!(run_id));
        values.insert("arrowcol-oat/owner-token".to_owned(), json!(owner_token));
    }
    Ok(())
}

fn stamp_run_origin_markers(
    resource: &mut Value,
    expected_namespace: &str,
    run_id: &str,
    owner_token: &str,
) -> Result<()> {
    let kind = resource
        .get("kind")
        .and_then(Value::as_str)
        .context("run-owned Kubernetes resource has no kind")?
        .to_owned();
    let metadata = resource
        .get("metadata")
        .and_then(Value::as_object)
        .context("run-owned Kubernetes resource has no metadata object")?
        .clone();
    if metadata.get("namespace").and_then(Value::as_str) != Some(expected_namespace)
        || metadata.get("name").and_then(Value::as_str).is_none_or(str::is_empty)
    {
        bail!("run-owned Kubernetes resource must have its exact namespace and name before apply");
    }
    let mut metadata_value = Value::Object(metadata);
    set_origin_markers(&mut metadata_value, run_id, owner_token)?;
    resource["metadata"] = metadata_value;
    if matches!(kind.as_str(), "Deployment" | "StatefulSet" | "DaemonSet" | "Job") {
        let template_metadata = resource
            .pointer_mut("/spec/template/metadata")
            .context("pod-bearing run-owned resource has no pod template metadata")?;
        set_origin_markers(template_metadata, run_id, owner_token)?;
    }
    Ok(())
}

fn planned_inventory_resource(resource: &Value, namespace: &str) -> Result<InventoryResource> {
    let api_version = resource
        .get("apiVersion")
        .and_then(Value::as_str)
        .context("resource apiVersion is missing")?;
    let kind = resource
        .get("kind")
        .and_then(Value::as_str)
        .context("resource kind is missing")?;
    let metadata = resource.get("metadata").context("resource metadata is missing")?;
    let resource_namespace = metadata
        .get("namespace")
        .and_then(Value::as_str)
        .context("resource namespace is missing")?;
    let name = metadata
        .get("name")
        .and_then(Value::as_str)
        .context("resource name is missing")?;
    if resource_namespace != namespace {
        bail!("run-owned resource namespace does not match the OAT cell");
    }
    let app = metadata
        .pointer("/labels/app")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let role = if (kind == "Job" || (kind == "Deployment" && FIXTURE_BOUNDARY.try_with(|_| ()).is_ok()))
        && app == "arrowcol-oat-client"
    {
        InventoryResourceRole::Client
    } else if name == "delta-maintenance" {
        InventoryResourceRole::Maintenance
    } else if name.contains("router") || app.contains("router") {
        InventoryResourceRole::Router
    } else {
        InventoryResourceRole::Collector
    };
    Ok(InventoryResource {
        api_version: api_version.to_owned(),
        kind: kind.to_owned(),
        namespace: resource_namespace.to_owned(),
        name: name.to_owned(),
        uid: None,
        role,
        state: InventoryResourceState::Planned,
        absence: None,
    })
}

fn has_origin_markers(metadata: &Value, run_id: &str, owner_token: &str) -> bool {
    ["labels", "annotations"].into_iter().all(|field| {
        metadata.get(field).and_then(Value::as_object).is_some_and(|values| {
            values.get("arrowcol-oat/run-id").and_then(Value::as_str) == Some(run_id)
                && values.get("arrowcol-oat/owner-token").and_then(Value::as_str) == Some(owner_token)
        })
    })
}

async fn record_applied_resource_uids(config: &OatConfig, platform: &AksPlatform, resources: &[Value]) -> Result<()> {
    record_resource_uids_with_creation(config, platform, resources, None).await
}

async fn record_resource_uids_with_creation(
    config: &OatConfig,
    platform: &AksPlatform,
    resources: &[Value],
    created: Option<&[Value]>,
) -> Result<()> {
    if config.campaign_profile != Some(crate::config::CampaignProfile::SingleHotTable) {
        return Ok(());
    }
    let payload = serde_json::to_vec(&json!({"apiVersion":"v1", "kind":"List", "items":resources}))?;
    let output = kubectl(
        config,
        vec!["get", "--namespace", &config.namespace, "-f", "-", "-o", "json"],
        Some(payload),
    )
    .await
    .context("read back exact run-owned Kubernetes resources after apply")?;
    let response: Value = serde_json::from_str(&output).context("parse applied Kubernetes resource readback")?;
    let observed = response
        .get("items")
        .and_then(Value::as_array)
        .context("Kubernetes resource readback is not a List")?;
    if observed.len() != resources.len() {
        bail!("Kubernetes resource readback did not return every planned identity");
    }
    let runtime_dir = platform
        .runtime_dir
        .as_deref()
        .context("campaign runtime directory is not available")?;
    let inventory_path = runtime_dir.join("campaign-inventory.json");
    let current: RunInventory = serde_json::from_slice(
        &tokio::fs::read(&inventory_path)
            .await
            .context("read last confirmed campaign inventory")?,
    )
    .context("parse last confirmed campaign inventory")?;
    if current.namespace.uid.as_deref() != platform.namespace_uid.as_deref()
        || current.namespace.owner_token != platform.namespace_owner_token
    {
        bail!("campaign inventory namespace identity changed before resource UID readback");
    }
    let mut next = current.clone();
    let mut seen = BTreeSet::new();
    for rendered in resources {
        let planned = planned_inventory_resource(rendered, &config.namespace)?;
        let identity = (
            planned.api_version.clone(),
            planned.kind.clone(),
            planned.namespace.clone(),
            planned.name.clone(),
        );
        if !seen.insert(identity) {
            bail!("duplicate run-owned Kubernetes identity in applied resource set");
        }
        let actual = observed
            .iter()
            .find(|resource| {
                resource.get("apiVersion").and_then(Value::as_str) == Some(planned.api_version.as_str())
                    && resource.get("kind").and_then(Value::as_str) == Some(planned.kind.as_str())
                    && resource.pointer("/metadata/namespace").and_then(Value::as_str)
                        == Some(planned.namespace.as_str())
                    && resource.pointer("/metadata/name").and_then(Value::as_str) == Some(planned.name.as_str())
            })
            .context("Kubernetes readback identity differs from the planned resource")?;
        if let Some(created) = created {
            fixture_response_identity(actual, rendered)?;
            let original = created
                .iter()
                .find(|resource| {
                    resource["apiVersion"] == actual["apiVersion"]
                        && resource["kind"] == actual["kind"]
                        && resource["metadata"]["namespace"] == actual["metadata"]["namespace"]
                        && resource["metadata"]["name"] == actual["metadata"]["name"]
                })
                .context("readback identity was not returned by the original create")?;
            for field in ["uid", "generation"] {
                if original["metadata"][field] != actual["metadata"][field] {
                    bail!("fresh-create readback changed original {field}; no identity adoption");
                }
            }
        }
        let uid = actual
            .pointer("/metadata/uid")
            .and_then(Value::as_str)
            .context("Kubernetes readback omitted the resource UID")?;
        if !has_origin_markers(&actual["metadata"], &platform.run_id, &platform.namespace_owner_token) {
            bail!("Kubernetes readback resource is missing the exact origin markers");
        }
        if matches!(
            planned.kind.as_str(),
            "Deployment" | "StatefulSet" | "DaemonSet" | "Job"
        ) && !actual
            .pointer("/spec/template/metadata")
            .is_some_and(|metadata| has_origin_markers(metadata, &platform.run_id, &platform.namespace_owner_token))
        {
            bail!("Kubernetes readback pod template is missing the exact origin markers");
        }
        let stored = next
            .resources
            .iter_mut()
            .find(|resource| {
                resource.api_version == planned.api_version
                    && resource.kind == planned.kind
                    && resource.namespace == planned.namespace
                    && resource.name == planned.name
            })
            .context("planned Kubernetes identity is missing from campaign inventory")?;
        if stored.state == InventoryResourceState::Removed || stored.state == InventoryResourceState::Absent {
            bail!("removed Kubernetes identity cannot be applied again");
        }
        stored.uid = Some(uid.to_owned());
        stored.state = InventoryResourceState::Observed;
    }
    if created.is_some() {
        verify_fresh_namespace_readback(config, platform).await?;
    }
    current.validate_successor(&next)?;
    persist_campaign_inventory(platform, &next).await
}

async fn prepare_run_owned_resources(
    config: &OatConfig,
    platform: &mut AksPlatform,
    resources: &mut [Value],
) -> Result<()> {
    for resource in resources.iter_mut() {
        stamp_run_origin_markers(
            resource,
            &config.namespace,
            &platform.run_id,
            &platform.namespace_owner_token,
        )?;
    }
    if config.campaign_profile != Some(crate::config::CampaignProfile::SingleHotTable) {
        return Ok(());
    }
    let runtime_dir = platform
        .runtime_dir
        .as_deref()
        .context("campaign runtime directory is not available")?;
    let inventory_path = runtime_dir.join("campaign-inventory.json");
    let current: RunInventory = serde_json::from_slice(
        &tokio::fs::read(&inventory_path)
            .await
            .context("read last confirmed campaign inventory")?,
    )
    .context("parse last confirmed campaign inventory")?;
    if current.namespace.name != config.namespace
        || current.namespace.uid.as_deref() != platform.namespace_uid.as_deref()
        || current.namespace.owner_token != platform.namespace_owner_token
        || current.cell_run_id != platform.run_id
    {
        bail!("campaign inventory does not match the live cell namespace and owner");
    }
    let mut next = current.clone();
    for resource in resources {
        let planned = planned_inventory_resource(resource, &config.namespace)?;
        if let Some(existing) = next.resources.iter().find(|item| {
            item.api_version == planned.api_version && item.kind == planned.kind && item.name == planned.name
        }) {
            if existing.role != planned.role {
                bail!("planned resource identity changed role within the run inventory");
            }
        } else {
            next.resources.push(planned);
        }
    }
    current.validate_successor(&next)?;
    persist_campaign_inventory(platform, &next).await
}

#[derive(Clone, Copy)]
enum RuntimeResourceMode {
    Apply,
    FreshCreate,
}

async fn apply_runtime_resources(config: &OatConfig, platform: &mut AksPlatform, resources: Vec<Value>) -> Result<()> {
    apply_runtime_resources_with_mode(config, platform, resources, RuntimeResourceMode::Apply).await
}

async fn apply_runtime_resources_with_mode(
    config: &OatConfig,
    platform: &mut AksPlatform,
    mut resources: Vec<Value>,
    mode: RuntimeResourceMode,
) -> Result<()> {
    if resources.is_empty() {
        bail!("refusing to apply an empty AKS runtime phase");
    }
    prepare_run_owned_resources(config, platform, &mut resources).await?;
    let payload = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": resources,
    }))?;
    let created = match mode {
        RuntimeResourceMode::Apply => {
            kubectl(
                config,
                vec!["apply", "--namespace", &config.namespace, "-f", "-"],
                Some(payload),
            )
            .await
            .context("apply run-scoped AKS runtime resources")?;
            None
        }
        RuntimeResourceMode::FreshCreate => {
            let collision = kubectl(
                config,
                vec![
                    "get",
                    "--namespace",
                    &config.namespace,
                    "-f",
                    "-",
                    "--ignore-not-found",
                    "-o",
                    "json",
                ],
                Some(payload.clone()),
            )
            .await?;
            let collision: Value = serde_json::from_str(&collision)?;
            if collision
                .get("items")
                .and_then(Value::as_array)
                .is_none_or(|items| !items.is_empty())
            {
                bail!("fresh-create resource collision or incomplete absence readback");
            }
            let output = kubectl(
                config,
                vec!["create", "--namespace", &config.namespace, "-f", "-", "-o", "json"],
                Some(payload),
            )
            .await
            .context("fresh-create run-scoped AKS runtime resources; do not replay uncertain creation")?;
            Some(parse_fresh_create_responses(&output, &resources)?)
        }
    };
    record_resource_uids_with_creation(config, platform, &resources, created.as_deref()).await?;
    Ok(())
}

async fn create_runtime_secrets(config: &OatConfig, platform: &mut AksPlatform, secrets_dir: &Path) -> Result<()> {
    let secret_specs: [(&str, &[(&str, &str)]); 5] = [
        ("arrowcol-sql-admin", &[("password", "sa-password")]),
        ("arrowcol-sql-runtime", &[("password", "app-password")]),
        ("arrowcol-sql-migration", &[("password", "migration-password")]),
        (
            "arrowcol-router-auth",
            &[("edge-hmac", "edge-hmac"), ("internal-hmac", "internal-hmac")],
        ),
        ("arrowcol-generation", &[("generation", "generation")]),
    ];
    for (name, keys) in secret_specs {
        let mut arguments = vec!["create", "secret", "generic", name, "--namespace", &config.namespace];
        let from_files = keys
            .iter()
            .map(|(key, file)| format!("--from-file={key}={}", secrets_dir.join(file).display()))
            .collect::<Vec<_>>();
        arguments.extend(from_files.iter().map(String::as_str));
        arguments.extend(["--dry-run=client", "-o", "json"]);
        let output = kubectl(config, arguments, None)
            .await
            .with_context(|| format!("render run-scoped Kubernetes Secret {name} from private files"))?;
        let mut secret: Value = serde_json::from_str(&output).context("parse rendered OAT Kubernetes Secret")?;
        prepare_run_owned_resources(config, platform, std::slice::from_mut(&mut secret)).await?;
        kubectl(config, vec!["create", "-f", "-"], Some(serde_json::to_vec(&secret)?))
            .await
            .with_context(|| format!("create run-scoped Kubernetes Secret {name}"))?;
        record_applied_resource_uids(config, platform, std::slice::from_ref(&secret)).await?;
    }
    Ok(())
}

async fn write_workload_stop_receipt(
    config: &OatConfig,
    platform: &mut AksPlatform,
    namespace_uid: &str,
) -> Result<()> {
    let workload_record_path = platform
        .fic_ready_snapshot_path
        .as_deref()
        .context("verified AKS workload ownership record is missing")?;
    let workload_record: Value = serde_json::from_slice(
        &tokio::fs::read(workload_record_path)
            .await
            .context("read verified AKS workload ownership record")?,
    )
    .context("parse verified AKS workload ownership record")?;
    let field = |name| {
        workload_record
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .with_context(|| format!("workload ownership record has no {name}"))
    };
    let aks_id = field("aksId")?;
    let kubectl_server = field("server")?;
    let server = Url::parse(kubectl_server).context("parse workload ownership kubectl server")?;
    if workload_record.get("phase").and_then(Value::as_str) != Some("ready")
        || workload_record.get("runId").and_then(Value::as_str) != Some(platform.run_id.as_str())
        || workload_record.get("namespace").and_then(Value::as_str) != Some(config.namespace.as_str())
        || workload_record.get("namespaceUid").and_then(Value::as_str) != Some(namespace_uid)
        || workload_record.get("namespaceOwner").and_then(Value::as_str)
            != Some(platform.namespace_owner_token.as_str())
        || server.scheme() != "https"
        || server.host_str().is_none()
        || !server.username().is_empty()
        || server.password().is_some()
        || server.port().is_some()
        || server.path() != "/"
        || server.query().is_some()
        || server.fragment().is_some()
    {
        bail!("workload ownership record does not match the deleted run namespace and AKS server");
    }
    let receipt_path = platform
        .runtime_dir
        .as_deref()
        .context("AKS runtime directory is not available")?
        .join("workload-stop-receipt.json");
    let receipt = json!({
        "schema_version": 1,
        "workload_record": workload_record,
        "run_id": platform.run_id,
        "namespace": config.namespace,
        "namespace_uid": namespace_uid,
        "namespace_run_id": platform.run_id,
        "namespace_owner_token": platform.namespace_owner_token,
        "aks_id": aks_id,
        "kubectl_server": kubectl_server,
        "delete_succeeded": true,
        "absence_confirmed": true,
        "observed_at_unix_ms": unix_time_ms(),
    });
    write_private_secret(&receipt_path, &serde_json::to_string(&receipt)?)
        .context("persist exact-run AKS workload stop receipt")?;
    platform.workload_stop_record_path = Some(receipt_path);
    Ok(())
}

async fn aks_pods_by_selector(config: &OatConfig, selector: &str) -> Result<Value> {
    let output = kubectl(
        config,
        vec![
            "get",
            "pods",
            "--namespace",
            &config.namespace,
            "--selector",
            selector,
            "-o",
            "json",
        ],
        None,
    )
    .await?;
    serde_json::from_str::<Value>(&output).context("parse run-owned workload pods")
}

async fn stop_aks_deployment_for_cleanup(
    config: &OatConfig,
    deployment_name: &str,
    role: ComponentRole,
    grace_seconds: u64,
) -> Result<Value> {
    timed_aks_cleanup_step(
        &format!("stop AKS Deployment {deployment_name}"),
        stop_aks_deployment_until_stopped(config, deployment_name, role, grace_seconds),
    )
    .await
}

async fn timed_aks_cleanup_step<T>(
    description: &str,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let started = std::time::Instant::now();
    eprintln!("[oat] {description}...");
    let result = operation.await;
    let seconds = started.elapsed().as_secs_f64();
    eprintln!(
        "[oat] {description}: {} ({seconds:.1}s)",
        if result.is_ok() { "done" } else { "failed" }
    );
    result.with_context(|| format!("{description} after {seconds:.1}s"))
}

async fn stop_aks_deployment_until_stopped(
    config: &OatConfig,
    deployment_name: &str,
    role: ComponentRole,
    grace_seconds: u64,
) -> Result<Value> {
    let output = kubectl(
        config,
        vec![
            "get",
            "deployment",
            deployment_name,
            "--namespace",
            &config.namespace,
            "--ignore-not-found",
            "-o",
            "json",
        ],
        None,
    )
    .await?;
    let Some(deployment) = optional_shutdown_deployment(&output)? else {
        return Ok(json!({"deployment": deployment_name, "status": "nothing_to_stop", "processes": []}));
    };
    let selector_labels = deployment
        .pointer("/spec/selector/matchLabels")
        .and_then(Value::as_object)
        .context("run-owned deployment has no stable pod selector")?;
    if selector_labels.is_empty() {
        bail!("run-owned deployment selector is empty");
    }
    let selector = selector_labels
        .iter()
        .map(|(key, value)| {
            value
                .as_str()
                .map(|value| format!("{key}={value}"))
                .context("deployment selector label is not a string")
        })
        .collect::<Result<Vec<_>>>()?
        .join(",");
    let initial_pods = aks_pods_by_selector(config, &selector).await?;
    let mut followers = Vec::new();
    let mut terminated = HashMap::<String, Value>::new();
    for pod in initial_pods["items"].as_array().into_iter().flatten() {
        let pod_name = pod
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .context("run-owned pod has no name")?;
        for status in pod
            .pointer("/status/containerStatuses")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let container = status
                .get("name")
                .and_then(Value::as_str)
                .context("run-owned pod container has no name")?;
            let role_container = match role {
                ComponentRole::Router => container == "router",
                ComponentRole::Collector => container == config.dfe_container,
                ComponentRole::Maintenance => container == "maintenance",
                ComponentRole::Client => false,
            };
            if !role_container {
                continue;
            }
            let pod_name = pod_name.to_owned();
            let container = container.to_owned();
            let instance_id = format!("{pod_name}/{container}");
            if let Some(state) = status.pointer("/state/terminated") {
                terminated.insert(instance_id.clone(), state.clone());
            }
            let namespace = config.namespace.clone();
            let context = config.kubectl_context.clone();
            let image = status.get("image").and_then(Value::as_str).map(str::to_owned);
            let image_id = status.get("imageID").and_then(Value::as_str).map(str::to_owned);
            let action = format!("stream shutdown logs for {instance_id}");
            let task_pod_name = pod_name.clone();
            let task_container = container.clone();
            let task = tokio::spawn(async move {
                let mut command = Command::new("kubectl");
                if let Some(context) = context {
                    command.args(["--context", &context]);
                }
                command.args([
                    "logs",
                    &task_pod_name,
                    "--namespace",
                    &namespace,
                    "--container",
                    &task_container,
                    "--follow",
                ]);
                let capture = filtered_process_command_output(command, &action).await?;
                let text = format!("{}\n{}", capture.stdout, capture.stderr);
                let complete = process_log_complete(&role, &text);
                Ok::<_, anyhow::Error>(ProcessLog {
                    role,
                    instance_id,
                    image,
                    image_id,
                    source_revision: None,
                    complete,
                    raw_log_bytes: capture.raw_log_bytes,
                    raw_log_lines: capture.raw_log_lines,
                    stdout: capture.stdout,
                    stderr: capture.stderr,
                    text,
                })
            });
            followers.push((pod_name, container, status.pointer("/state/running").is_some(), task));
        }
    }
    let sigterm_at_unix_ms = (role == ComponentRole::Collector)
        .then(|| u64::try_from(unix_time_ms()).ok())
        .flatten();
    kubectl(
        config,
        vec![
            "scale",
            "deployment",
            deployment_name,
            "--replicas=0",
            "--namespace",
            &config.namespace,
        ],
        None,
    )
    .await
    .with_context(|| format!("scale run-owned deployment {deployment_name} to zero"))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(grace_seconds.saturating_add(120));
    loop {
        let pods = aks_pods_by_selector(config, &selector).await?;
        for pod in pods["items"].as_array().into_iter().flatten() {
            let pod_name = pod
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .context("run-owned pod has no name")?;
            for status in pod
                .pointer("/status/containerStatuses")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(state) = status.pointer("/state/terminated") {
                    let container = status
                        .get("name")
                        .and_then(Value::as_str)
                        .context("terminated container has no name")?;
                    terminated.insert(format!("{pod_name}/{container}"), state.clone());
                }
            }
        }
        let current: Value = serde_json::from_str(
            &kubectl(
                config,
                vec![
                    "get",
                    "deployment",
                    deployment_name,
                    "--namespace",
                    &config.namespace,
                    "-o",
                    "json",
                ],
                None,
            )
            .await?,
        )?;
        let desired = current.pointer("/spec/replicas").and_then(Value::as_u64).unwrap_or(1);
        let replicas = current.pointer("/status/replicas").and_then(Value::as_u64).unwrap_or(0);
        let observed = rollout_generation_observed(&current);
        let no_pods = pods["items"].as_array().is_some_and(Vec::is_empty);
        if desired == 0 && replicas == 0 && observed && no_pods {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("run-owned deployment {deployment_name} did not stop within its bounded grace period");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let mut processes = Vec::new();
    let mut outcome_failures = Vec::new();
    for (pod_name, container, was_running, mut task) in followers {
        let instance_id = format!("{pod_name}/{container}");
        let log = match tokio::time::timeout(Duration::from_secs(30), &mut task).await {
            Ok(Ok(Ok(log))) => Some(log),
            _ => {
                task.abort();
                None
            }
        };
        let terminated_state = terminated.get(&instance_id);
        let exit_code = terminated_state
            .and_then(|state| state.get("exitCode"))
            .and_then(Value::as_i64);
        let finished_at = terminated_state
            .and_then(|state| state.get("finishedAt"))
            .and_then(Value::as_str);
        let outcome = shutdown_exit_outcome(
            role,
            was_running,
            exit_code,
            terminated_state
                .and_then(|state| state.get("reason"))
                .and_then(Value::as_str)
                == Some("OOMKilled"),
        );
        let exit_evidence = match outcome {
            Ok(evidence) => evidence,
            Err(error) => {
                outcome_failures.push(format!("{instance_id}: {error:#}"));
                json!({"status": "failed", "exit_code": exit_code})
            }
        };
        let process_evidence = match (role, log.as_ref()) {
            (ComponentRole::Collector, Some(log)) => collector_shutdown_drain_evidence(
                log,
                was_running.then_some(sigterm_at_unix_ms).flatten(),
                finished_at,
                exit_code,
                config.tunables.wal.is_some(),
            ),
            (ComponentRole::Collector, None) => json!({
                "status": "unavailable", "unavailable_reason": "collector_shutdown_log_stream_unavailable",
                "sigterm_sent": was_running, "sigterm_to_exit_seconds": null,
                "commits_after_sigterm": null, "committed_rows_after_sigterm": null,
                "quiver": {"status": "unavailable", "bundles_drained": null, "force_dropped_bundles": null},
                "exit_code": exit_code,
            }),
            (ComponentRole::Maintenance, Some(log)) => {
                let baseline = ProcessLog {
                    role: ComponentRole::Maintenance,
                    instance_id: log.instance_id.clone(),
                    image: None,
                    image_id: None,
                    source_revision: None,
                    complete: true,
                    raw_log_bytes: 0,
                    raw_log_lines: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                    text: String::new(),
                };
                maintenance_cell_outcome_evidence(log, &[baseline])
            }
            (ComponentRole::Maintenance, None) => {
                json!({"status":"unavailable", "error_kind_counts": null, "comment":"Maintenance log stream was unavailable; raw error text was not retained."})
            }
            _ => json!({"status":"observed"}),
        };
        processes.push(json!({
            "pod": pod_name,
            "container": container,
            "was_running": was_running,
            "exit_code": exit_code,
            "exit_outcome": exit_evidence,
            "finished_at": finished_at,
            "log_complete": log.as_ref().is_some_and(|log| log.complete),
            "drain": (role == ComponentRole::Collector).then_some(process_evidence.clone()),
            "operation_outcomes": (role == ComponentRole::Maintenance).then_some(process_evidence),
        }));
    }
    if !outcome_failures.is_empty() {
        bail!(
            "run-owned deployment {deployment_name} failed during shutdown: {}",
            outcome_failures.join("; ")
        );
    }
    Ok(json!({
        "deployment": deployment_name,
        "role": format!("{role:?}").to_ascii_lowercase(),
        "sigterm_at_unix_ms": sigterm_at_unix_ms,
        "processes": processes,
    }))
}

fn optional_shutdown_deployment(output: &str) -> Result<Option<Value>> {
    if output.trim().is_empty() {
        Ok(None)
    } else {
        Ok(Some(
            serde_json::from_str(output).context("parse deployment during shutdown")?,
        ))
    }
}

fn shutdown_exit_outcome(
    role: ComponentRole,
    was_running: bool,
    exit_code: Option<i64>,
    oom_killed: bool,
) -> Result<Value> {
    if oom_killed {
        bail!("container was OOMKilled during shutdown");
    }
    let Some(exit) = exit_code else {
        return Ok(
            json!({"status": "unavailable", "reason": "terminated_container_state_not_observed", "exit_code": null}),
        );
    };
    let maintenance_health_exit = role == ComponentRole::Maintenance && exit == 1;
    if !(exit == 0 || (was_running && exit == 143) || maintenance_health_exit) {
        bail!("container failed or was forcibly stopped ({exit})");
    }
    Ok(json!({
        "status": "observed", "exit_code": exit,
        "maintenance_operation_failure": null,
        "classification": if maintenance_health_exit { "requires_operation_records" } else { "clean_shutdown" },
    }))
}

async fn stop_aks_workloads_in_order(config: &OatConfig) -> Result<Value> {
    let mut ingress = Vec::new();
    let mut maintenance = Vec::new();
    let mut collectors = Vec::new();
    if config.arrowcol_deployment != config.dfe_deployment {
        ingress.push(
            stop_aks_deployment_for_cleanup(config, &config.arrowcol_deployment, ComponentRole::Router, 60).await?,
        );
    }
    if maintenance_enabled(config) {
        let grace_seconds = config.maintenance_stop_budget()?.maintenance_grace_seconds;
        maintenance.push(
            stop_aks_deployment_for_cleanup(config, "delta-maintenance", ComponentRole::Maintenance, grace_seconds)
                .await?,
        );
    }
    collectors.push(
        stop_aks_deployment_for_cleanup(
            config,
            &config.dfe_deployment,
            ComponentRole::Collector,
            config.collector_stop_grace_seconds,
        )
        .await?,
    );
    if config.arrowcol_deployment == config.dfe_deployment {
        ingress.push(json!({"deployment": config.arrowcol_deployment, "status": "shared_with_collector", "comment": "Ingress and collector share one Deployment and cannot be stopped independently."}));
    }
    Ok(json!({
        "schema_version": 1,
        "cleanup_order": ["ingress", "maintenance", "collectors"],
        "ingress": ingress,
        "maintenance": maintenance,
        "collectors": collectors,
    }))
}

fn require_aks_cleanup_confirmation(platform: &AksPlatform) -> Result<()> {
    if platform.run_table_initialization_attempted && !platform.run_table_initialization_drained {
        bail!("run-table initializer publisher drain was not positively confirmed");
    }
    if platform.qap_bind_attempted && !platform.qap_cleanup_confirmed {
        bail!("run-owned QAP cleanup was not positively confirmed");
    }
    Ok(())
}

async fn cleanup_aks_platform(config: &OatConfig, platform: &mut AksPlatform) -> Result<()> {
    let mut failures = Vec::new();
    let mut namespace_removed = false;
    if let Some(forward) = platform.router_tunnel.take() {
        match stop_kubectl_process(forward).await {
            Ok(stderr) => {
                if let Err(error) = collect_kubectl_stderr(stderr).await {
                    failures.push(format!(
                        "collect AKS router tunnel diagnostics during cleanup: {error:#}"
                    ));
                }
            }
            Err(error) => failures.push(format!("stop owned AKS router metrics tunnel: {error:#}")),
        }
        platform.router_metrics_url = None;
    }
    if config.campaign_profile == Some(crate::config::CampaignProfile::SingleHotTable) {
        match cleanup_campaign_aks_platform(config, platform).await {
            Ok(evidence) => {
                platform.shutdown_drain_evidence = Some(evidence);
                platform.qap_cleanup_confirmed = true;
            }
            Err(error) => {
                let detail = format!("{error:#}");
                platform.shutdown_drain_evidence = Some(json!({
                    "status": "unavailable",
                    "reason": "custody_cleanup_failed_or_uncertain",
                    "failure": detail,
                }));
                failures.push(format!("custody-managed AKS cleanup failed: {detail}"));
            }
        }
        if let Err(error) = require_aks_cleanup_confirmation(platform) {
            failures.push(format!("AKS cleanup confirmation withheld: {error:#}"));
        }
        if let Some(runtime_dir) = platform.runtime_dir.take() {
            let retention = runtime_evidence_retention(
                platform.fic_prepare_attempted,
                platform.fic_record_path.as_deref(),
                platform.fic_ready_snapshot_path.as_deref(),
                platform.qap_bind_attempted,
                platform.qap_record_path.as_deref(),
                platform.workload_stop_record_path.as_deref(),
                platform.qap_outcome_unresolved || !failures.is_empty(),
            );
            if let Err(error) = apply_runtime_evidence_retention(&runtime_dir, retention).await {
                failures.push(format!(
                    "remove private AKS runtime files {}: {error:#}",
                    runtime_dir.display()
                ));
            }
        }
        if failures.is_empty() {
            return Ok(());
        }
        return Err(anyhow::anyhow!("AKS cleanup was incomplete: {}", failures.join("; ")));
    }
    if platform.namespace_created || platform.namespace_create_attempted {
        match kubectl(config, vec!["get", "namespace", &config.namespace, "-o", "json"], None).await {
            Ok(output) => {
                let namespace: Value = serde_json::from_str(&output).context("parse OAT namespace during cleanup")?;
                let ours = namespace_matches_run(
                    &namespace,
                    &platform.run_id,
                    &platform.namespace_owner_token,
                    platform.namespace_uid.as_deref(),
                );
                match namespace_cleanup_action(platform.namespace_created, ours) {
                    NamespaceCleanupAction::Delete => {
                        let namespace_uid = namespace_uid_for_cleanup(&namespace, platform.namespace_uid.as_deref())?;
                        platform.namespace_uid = Some(namespace_uid.clone());
                        let cleanup = async {
                            timed_aks_cleanup_step(
                                "delete run-owned AKS namespace",
                                delete_run_owned_namespace_with_uid(config, &namespace_uid),
                            )
                            .await
                            .context("delete run-owned AKS namespace with UID precondition")?;
                            timed_aks_cleanup_step(
                                "wait for run-owned AKS namespace absence",
                                wait_for_run_owned_namespace_absence(config),
                            )
                            .await
                            .context("confirm run-owned AKS namespace absence after deletion")?;
                            namespace_removed = true;
                            platform.namespace_created = false;
                            platform.namespace_create_attempted = false;
                            platform.namespace_uid = None;
                            timed_aks_cleanup_step(
                                "write exact-run workload stop receipt",
                                write_workload_stop_receipt(config, platform, &namespace_uid),
                            )
                            .await
                            .context("write exact-run workload stop receipt")
                        };
                        match stop_then_destroy(stop_aks_workloads_in_order(config), cleanup).await {
                            Ok(evidence) => platform.shutdown_drain_evidence = Some(evidence),
                            Err(error) => failures.push(format!("ordered AKS shutdown/cleanup failed: {error:#}")),
                        }
                    }
                    NamespaceCleanupAction::Ignore => {
                        platform.namespace_create_attempted = false;
                    }
                    NamespaceCleanupAction::Refuse => {
                        failures
                            .push("refusing AKS cleanup because namespace run ownership markers changed".to_owned());
                    }
                }
                if namespace_removed && platform.qap_bind_attempted {
                    match (
                        platform.qap_record_path.as_deref(),
                        platform.workload_stop_record_path.as_deref(),
                    ) {
                        (Some(record_path), Some(stop_record_path))
                            if tokio::fs::try_exists(record_path).await?
                                && tokio::fs::try_exists(stop_record_path).await? =>
                        {
                            let cleanup_result = match configured_bootstrap_env_file(config) {
                                Ok(bootstrap_env_file) => {
                                    timed_aks_cleanup_step(
                                        "clean run-owned QAP bindings",
                                        run_oat_qap_helper(
                                            "cleanup-oat-qap",
                                            &platform.run_id,
                                            None,
                                            None,
                                            Some(stop_record_path),
                                            record_path,
                                            bootstrap_env_file,
                                        ),
                                    )
                                    .await
                                }
                                Err(error) => Err(error),
                            };
                            match cleanup_result {
                                Ok(()) => platform.qap_cleanup_confirmed = true,
                                Err(error) => failures.push(format!("remove exact run-owned QAP bindings: {error:#}")),
                            }
                        }
                        _ => failures.push("QAP cleanup requires both ownership and workload-stop records".to_owned()),
                    }
                }
            }
            Err(error) => failures.push(format!("verify run-owned namespace before cleanup: {error:#}")),
        }
    }
    if namespace_removed && platform.fic_prepare_attempted {
        if let Some(record_path) = platform.fic_record_path.as_deref() {
            let cleanup_result = match configured_bootstrap_env_file(config) {
                Ok(bootstrap_env_file) => {
                    timed_aks_cleanup_step(
                        "clean run-owned federated credential",
                        run_run_fic_helper(
                            "cleanup-run-fic",
                            &platform.run_id,
                            None,
                            None,
                            record_path,
                            FicHelperOptions {
                                wait_seconds: None,
                                identity_plan_path: None,
                            },
                            bootstrap_env_file,
                        ),
                    )
                    .await
                }
                Err(error) => Err(error),
            };
            if let Err(error) = cleanup_result {
                failures.push(format!(
                    "remove exact run-owned workload identity credential: {error:#}"
                ));
            }
        } else {
            failures.push("workload identity cleanup has no durable run record path".to_owned());
        }
    }
    if let Err(error) = require_aks_cleanup_confirmation(platform) {
        failures.push(format!("AKS cleanup confirmation withheld: {error:#}"));
    }
    if let Some(runtime_dir) = platform.runtime_dir.take() {
        let retention = runtime_evidence_retention(
            platform.fic_prepare_attempted,
            platform.fic_record_path.as_deref(),
            platform.fic_ready_snapshot_path.as_deref(),
            platform.qap_bind_attempted,
            platform.qap_record_path.as_deref(),
            platform.workload_stop_record_path.as_deref(),
            platform.qap_outcome_unresolved || !failures.is_empty(),
        );
        if let Err(error) = apply_runtime_evidence_retention(&runtime_dir, retention).await {
            failures.push(format!(
                "remove private AKS runtime files {}: {error:#}",
                runtime_dir.display()
            ));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("AKS cleanup was incomplete: {}", failures.join("; ")))
    }
}

enum RuntimeEvidenceRetention {
    PreserveAll,
    PruneExcept(Vec<PathBuf>),
}

fn runtime_evidence_retention(
    fic_prepare_attempted: bool,
    fic_record_path: Option<&Path>,
    fic_ready_snapshot_path: Option<&Path>,
    qap_bind_attempted: bool,
    qap_record_path: Option<&Path>,
    workload_stop_record_path: Option<&Path>,
    unresolved_outcome: bool,
) -> RuntimeEvidenceRetention {
    if unresolved_outcome {
        return RuntimeEvidenceRetention::PreserveAll;
    }

    let mut preserve = Vec::new();
    if fic_prepare_attempted {
        preserve.extend(fic_record_path.map(Path::to_owned));
        preserve.extend(fic_ready_snapshot_path.map(Path::to_owned));
    }
    if qap_bind_attempted {
        if let Some(record_path) = qap_record_path {
            preserve.push(record_path.to_owned());
            preserve.push(qap_lease_sidecar_path(record_path));
        }
        preserve.extend(workload_stop_record_path.map(Path::to_owned));
    }
    RuntimeEvidenceRetention::PruneExcept(preserve)
}

fn qap_lease_sidecar_path(record_path: &Path) -> PathBuf {
    let mut path = record_path.as_os_str().to_os_string();
    path.push(".lease");
    PathBuf::from(path)
}

async fn apply_runtime_evidence_retention(directory: &Path, retention: RuntimeEvidenceRetention) -> Result<()> {
    match retention {
        RuntimeEvidenceRetention::PreserveAll => Ok(()),
        RuntimeEvidenceRetention::PruneExcept(mut preserve) => {
            for name in [
                "run-table-initializer-job.json",
                "run-table-initializer-termination.json",
            ] {
                let path = directory.join(name);
                if path.try_exists().context("inspect retained initializer evidence")? {
                    preserve.push(path);
                }
            }
            remove_runtime_dir_except(directory, &preserve).await
        }
    }
}

async fn remove_runtime_dir_except(directory: &Path, preserve: &[PathBuf]) -> Result<()> {
    let mut entries = tokio::fs::read_dir(directory)
        .await
        .with_context(|| format!("read private runtime directory {}", directory.display()))?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if preserve.iter().any(|preserved| preserved == &path) {
            continue;
        }
        if entry.file_type().await?.is_dir() {
            tokio::fs::remove_dir_all(&path)
                .await
                .with_context(|| format!("remove private runtime directory {}", path.display()))?;
        } else {
            tokio::fs::remove_file(&path)
                .await
                .with_context(|| format!("remove private runtime file {}", path.display()))?;
        }
    }
    if preserve.is_empty() {
        tokio::fs::remove_dir(directory)
            .await
            .with_context(|| format!("remove empty private runtime directory {}", directory.display()))?;
    }
    Ok(())
}

fn configure_client_job_resources(job: &mut Value, requirements: &ResourceRequirements) -> Result<()> {
    let container = job
        .pointer_mut("/spec/template/spec/containers/0")
        .context("AKS client container is missing")?;
    container["resources"] = serde_json::to_value(requirements)?;
    Ok(())
}

fn configure_deployment_container_resources(
    deployment: &mut Value,
    container_name: &str,
    requirements: &ResourceRequirements,
) -> Result<()> {
    let containers = deployment
        .pointer_mut("/spec/template/spec/containers")
        .and_then(Value::as_array_mut)
        .context("Deployment has no container list")?;
    let mut selected = containers
        .iter_mut()
        .filter(|container| container.get("name").and_then(Value::as_str) == Some(container_name));
    let container = selected
        .next()
        .with_context(|| format!("container {container_name:?} was not found"))?;
    if selected.next().is_some() {
        bail!("container {container_name:?} is ambiguous in Deployment");
    }
    let mut resources = serde_json::to_value(requirements)?;
    if requirements.limits.cpu.is_none() {
        resources["limits"]
            .as_object_mut()
            .context("resource limits must be an object")?
            .remove("cpu");
    }
    container["resources"] = resources;
    Ok(())
}

fn verify_container_resources(
    containers: &[Value],
    container_name: &str,
    role: &str,
    expected: &ResourceRequirements,
) -> Result<Value> {
    let mut selected = containers
        .iter()
        .filter(|container| container.get("name").and_then(Value::as_str) == Some(container_name));
    let container = selected
        .next()
        .with_context(|| format!("{role} container {container_name:?} is missing"))?;
    if selected.next().is_some() {
        bail!("{role} container {container_name:?} is ambiguous");
    }
    let resources = container
        .get("resources")
        .context("container resource readback is missing")?;
    let quantity = |side: &str, name: &str| -> Result<String> {
        resources
            .pointer(&format!("/{side}/{name}"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .with_context(|| format!("{role} resource readback omitted {side}.{name}"))
    };
    let limits = resources
        .get("limits")
        .and_then(Value::as_object)
        .context("resource readback omitted limits")?;
    let limit_cpu = match limits.get("cpu") {
        None => None,
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => bail!("{role} resource CPU limit readback has an invalid type"),
    };
    let actual = ResourceRequirements {
        requests: ResourceValues {
            cpu: Some(quantity("requests", "cpu")?),
            memory: quantity("requests", "memory")?,
        },
        limits: ResourceValues {
            cpu: limit_cpu,
            memory: quantity("limits", "memory")?,
        },
    };
    let actual_request = actual.requests.cpu.as_deref().context("actual CPU request missing")?;
    let expected_request = expected
        .requests
        .cpu
        .as_deref()
        .context("expected CPU request missing")?;
    let quantities_match = parse_resource_cpu_millicores(actual_request)?
        == parse_resource_cpu_millicores(expected_request)?
        && actual.limits.cpu.as_deref().map(parse_cpu_millicores).transpose()?
            == expected.limits.cpu.as_deref().map(parse_cpu_millicores).transpose()?
        && parse_resource_memory_bytes(&actual.requests.memory)?
            == parse_resource_memory_bytes(&expected.requests.memory)?
        && parse_memory_bytes(&actual.limits.memory)? == parse_memory_bytes(&expected.limits.memory)?;
    if !quantities_match {
        bail!("{role} effective pod resource readback differs from requested values");
    }
    Ok(json!({"requested": expected, "effective": actual}))
}

fn deployment_selector(deployment: &Value) -> Result<String> {
    let labels = deployment
        .pointer("/spec/selector/matchLabels")
        .and_then(Value::as_object)
        .context("Deployment selector.matchLabels is missing")?;
    let selector = labels
        .iter()
        .map(|(key, value)| {
            value
                .as_str()
                .map(|value| format!("{key}={value}"))
                .context("Deployment selector labels must be strings")
        })
        .collect::<Result<Vec<_>>>()?
        .join(",");
    anyhow::ensure!(!selector.is_empty(), "Deployment selector must not be empty");
    Ok(selector)
}

async fn aks_deployment_resource_readback(
    config: &OatConfig,
    role: &str,
    deployment_name: &str,
    container_name: &str,
    expected: &ResourceRequirements,
) -> Result<Value> {
    let deployment = deployment(config, deployment_name).await?;
    let template_containers = deployment
        .pointer("/spec/template/spec/containers")
        .and_then(Value::as_array)
        .context("Deployment pod template has no containers")?;
    let template = verify_container_resources(template_containers, container_name, role, expected)?;
    let selector = deployment_selector(&deployment)?;
    let pod_json = kubectl(
        config,
        vec![
            "get",
            "pods",
            "--namespace",
            &config.namespace,
            "--selector",
            &selector,
            "-o",
            "json",
        ],
        None,
    )
    .await
    .with_context(|| format!("read {role} pod resource and placement readbacks"))?;
    let pod_list: Value = serde_json::from_str(&pod_json).context("parse resource-readback pod list")?;
    let pods = pod_list
        .get("items")
        .and_then(Value::as_array)
        .context("resource-readback pod list omitted items")?;
    let mut scheduled = Vec::new();
    for pod in pods
        .iter()
        .filter(|pod| pod.pointer("/metadata/deletionTimestamp").is_none())
    {
        let metadata = pod.get("metadata").context("pod metadata is missing")?;
        let pod_name = metadata
            .get("name")
            .and_then(Value::as_str)
            .context("pod name is missing")?;
        let node_name = pod
            .pointer("/spec/nodeName")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .with_context(|| format!("{role} pod {pod_name} has no scheduled node"))?;
        let containers = pod
            .pointer("/spec/containers")
            .and_then(Value::as_array)
            .context("pod spec has no containers")?;
        let resources = verify_container_resources(containers, container_name, role, expected)?;
        scheduled.push(json!({
            "pod": pod_name,
            "uid": metadata.get("uid"),
            "container": container_name,
            "container_id": native_container_id(pod, container_name),
            "node": node_name,
            "resources": resources["effective"],
        }));
    }
    anyhow::ensure!(
        !scheduled.is_empty(),
        "{role} has no active pod with resource and scheduled-node readback"
    );
    Ok(json!({
        "role": role,
        "deployment": deployment_name,
        "requested": expected,
        "effective": {"template": template["effective"], "pods": scheduled},
    }))
}

async fn aks_deployment_resource_evidence(config: &OatConfig) -> Result<Vec<Value>> {
    let resources = config.effective_aks_resources();
    let evidence = vec![
        aks_deployment_resource_readback(
            config,
            "collector",
            &config.dfe_deployment,
            &config.dfe_container,
            &resources.collector,
        )
        .await?,
        aks_deployment_resource_readback(config, "router", "arrowcol-router", "router", &resources.router).await?,
        aks_deployment_resource_readback(config, "catalog", "arrowcol-catalog", "catalog", &resources.catalog).await?,
    ];
    Ok(evidence)
}

#[derive(Clone, Debug)]
struct CpuThrottleCounters {
    periods: u64,
    throttled_periods: u64,
}

#[derive(Clone, Debug)]
struct CpuThrottleSample {
    role: String,
    pod: String,
    uid: String,
    container: String,
    node: String,
    counters: Option<CpuThrottleCounters>,
    unavailable_reason: Option<String>,
}

fn parse_cadvisor_cpu_counters(
    metrics: &str,
    namespace: &str,
    pod: &str,
    container: &str,
) -> Result<Option<CpuThrottleCounters>> {
    let mut periods = None;
    let mut throttled_periods = None;
    for line in metrics.lines().filter(|line| !line.starts_with('#')) {
        let Some((name, remainder)) = line.split_once('{') else {
            continue;
        };
        if !matches!(
            name,
            "container_cpu_cfs_periods_total" | "container_cpu_cfs_throttled_periods_total"
        ) {
            continue;
        }
        let Some((labels_text, value_text)) = remainder.split_once('}') else {
            continue;
        };
        let mut labels = HashMap::new();
        for label in labels_text.split(',') {
            let Some((key, value)) = label.split_once('=') else {
                continue;
            };
            labels.insert(key.trim(), value.trim().trim_matches('"'));
        }
        if labels.get("namespace").copied() != Some(namespace)
            || labels.get("pod").or_else(|| labels.get("pod_name")).copied() != Some(pod)
            || labels.get("container").copied() != Some(container)
        {
            continue;
        }
        let value_text = value_text
            .split_whitespace()
            .next()
            .context("cAdvisor metric value is missing")?;
        let value = value_text.parse::<f64>().context("cAdvisor counter is not numeric")?;
        if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value > u64::MAX as f64 {
            bail!("cAdvisor throttling counters must be nonnegative integers");
        }
        let counter = value as u64;
        match name {
            "container_cpu_cfs_periods_total" if periods.is_some() => {
                bail!("cAdvisor returned ambiguous CPU period counters for {pod}/{container}");
            }
            "container_cpu_cfs_periods_total" => periods = Some(counter),
            "container_cpu_cfs_throttled_periods_total" if throttled_periods.is_some() => {
                bail!("cAdvisor returned ambiguous throttling counters for {pod}/{container}");
            }
            "container_cpu_cfs_throttled_periods_total" => throttled_periods = Some(counter),
            _ => {}
        }
    }
    match (periods, throttled_periods) {
        (Some(periods), Some(throttled_periods)) => Ok(Some(CpuThrottleCounters {
            periods,
            throttled_periods,
        })),
        _ => Ok(None),
    }
}

#[derive(Clone, serde::Serialize)]
struct AksResourcePodTarget {
    role: String,
    pod: String,
    uid: String,
    container: String,
    node: String,
    container_id: Option<String>,
}

fn aks_resource_pod_targets(readbacks: &[Value]) -> Result<Vec<AksResourcePodTarget>> {
    let mut targets = Vec::new();
    for readback in readbacks {
        let role = readback
            .get("role")
            .and_then(Value::as_str)
            .context("AKS resource evidence omitted role")?;
        let mut add_pod = |pod: &Value, pod_field: &str| -> Result<()> {
            let name = pod
                .get(pod_field)
                .and_then(Value::as_str)
                .context("AKS resource evidence omitted pod name")?;
            let uid = pod
                .get("uid")
                .and_then(Value::as_str)
                .context("AKS resource evidence omitted pod UID")?;
            let node = pod
                .get("node")
                .and_then(Value::as_str)
                .context("AKS resource evidence omitted node")?;
            let container = pod
                .get("container")
                .and_then(Value::as_str)
                .context("AKS resource evidence omitted container")?;
            targets.push(AksResourcePodTarget {
                role: role.to_owned(),
                pod: name.to_owned(),
                uid: uid.to_owned(),
                container: container.to_owned(),
                node: node.to_owned(),
                container_id: pod.get("container_id").and_then(Value::as_str).map(str::to_owned),
            });
            Ok(())
        };
        if role == "client" {
            add_pod(
                readback
                    .get("scheduled_pod")
                    .context("client resource evidence omitted scheduled pod")?,
                "name",
            )?;
        } else {
            for pod in readback
                .pointer("/effective/pods")
                .and_then(Value::as_array)
                .context("AKS resource evidence omitted deployment pods")?
            {
                add_pod(pod, "pod")?;
            }
        }
    }
    Ok(targets)
}

async fn capture_aks_cpu_throttling(config: &OatConfig, readbacks: &[Value]) -> Result<Vec<CpuThrottleSample>> {
    let targets = aks_resource_pod_targets(readbacks)?;
    let nodes = targets
        .iter()
        .map(|target| target.node.clone())
        .collect::<BTreeSet<_>>();
    let scrape = tokio::time::timeout(
        Duration::from_millis(AKS_RESOURCE_COUNTER_SNAPSHOT_TIMEOUT_MS),
        join_all(nodes.iter().map(|node| async move {
            let path = format!("/api/v1/nodes/{node}/proxy/metrics/cadvisor");
            let result = kubectl(config, vec!["get", "--raw", &path, "--request-timeout=5s"], None).await;
            (node.clone(), result.map_err(|error| format!("{error:#}")))
        })),
    )
    .await;
    let node_metrics = match scrape {
        Ok(results) => results.into_iter().collect::<HashMap<_, _>>(),
        Err(_) => nodes
            .iter()
            .cloned()
            .map(|node| {
                (
                    node,
                    Err("cAdvisor snapshot exceeded its five-second budget".to_owned()),
                )
            })
            .collect::<HashMap<_, _>>(),
    };
    let mut samples = Vec::with_capacity(targets.len());
    for target in targets {
        let (counters, unavailable_reason) = match node_metrics.get(&target.node) {
            Some(Ok(metrics)) => {
                match parse_cadvisor_cpu_counters(metrics, &config.namespace, &target.pod, &target.container) {
                    Ok(Some(counters)) => (Some(counters), None),
                    Ok(None) => (None, Some("cAdvisor cgroup CPU counters are unavailable".to_owned())),
                    Err(error) => (
                        None,
                        Some(format!("cAdvisor counters are ambiguous or invalid: {error:#}")),
                    ),
                }
            }
            Some(Err(error)) => (None, Some(format!("cAdvisor scrape failed: {error}"))),
            None => (None, Some("cAdvisor node scrape was not attempted".to_owned())),
        };
        samples.push(CpuThrottleSample {
            role: target.role,
            pod: target.pod,
            uid: target.uid,
            container: target.container,
            node: target.node,
            counters,
            unavailable_reason,
        });
    }
    Ok(samples)
}

fn compare_cpu_throttling(start: &[CpuThrottleSample], end: &[CpuThrottleSample]) -> Value {
    let start_by_identity = start
        .iter()
        .map(|sample| {
            (
                (
                    sample.role.as_str(),
                    sample.pod.as_str(),
                    sample.uid.as_str(),
                    sample.container.as_str(),
                ),
                sample,
            )
        })
        .collect::<HashMap<_, _>>();
    let mut measured = 0;
    let mut pod_evidence = Vec::with_capacity(end.len());
    for sample in end {
        let key = (
            sample.role.as_str(),
            sample.pod.as_str(),
            sample.uid.as_str(),
            sample.container.as_str(),
        );
        let result = start_by_identity.get(&key).and_then(|before| {
            let before = before.counters.as_ref()?;
            let after = sample.counters.as_ref()?;
            Some((
                after.periods.checked_sub(before.periods)?,
                after.throttled_periods.checked_sub(before.throttled_periods)?,
            ))
        });
        match result {
            Some((periods, throttled_periods)) if periods > 0 => {
                measured += 1;
                pod_evidence.push(json!({
                    "role": sample.role,
                    "pod": sample.pod,
                    "uid": sample.uid,
                    "container": sample.container,
                    "node": sample.node,
                    "status": "measured",
                    "periods": periods,
                    "throttled_periods": throttled_periods,
                    "throttled_period_fraction": throttled_periods as f64 / periods as f64,
                }));
            }
            _ => {
                let start_sample = start_by_identity.get(&key).copied();
                let initial_unavailable_reason = start_sample
                    .filter(|before| before.counters.is_none())
                    .and_then(|before| before.unavailable_reason.as_deref());
                let reason = initial_unavailable_reason
                    .or(sample.unavailable_reason.as_deref())
                    .unwrap_or(if start_sample.is_some() {
                        "counter reset, identity changed, or no CPU periods elapsed"
                    } else {
                        "initial cAdvisor snapshot omitted this pod identity"
                    });
                pod_evidence.push(json!({
                    "role": sample.role,
                    "pod": sample.pod,
                    "uid": sample.uid,
                    "container": sample.container,
                    "node": sample.node,
                    "status": "unavailable",
                    "reason": reason,
                }));
            }
        }
    }
    let status = if measured == end.len() && measured > 0 {
        "measured"
    } else if measured > 0 {
        "partial"
    } else {
        "unavailable"
    };
    json!({
        "status": status,
        "scope": "scheduled_load_to_client_completion",
        "source": "kubelet cAdvisor container CPU CFS counters",
        "pods": pod_evidence,
    })
}

async fn apply_deployment(config: &OatConfig, mut deployment: Value) -> Result<()> {
    let metadata = deployment
        .get_mut("metadata")
        .and_then(Value::as_object_mut)
        .context("Deployment metadata must be an object")?;
    for field in [
        "uid",
        "resourceVersion",
        "generation",
        "creationTimestamp",
        "managedFields",
        "selfLink",
    ] {
        metadata.remove(field);
    }
    deployment
        .as_object_mut()
        .context("Deployment must be an object")?
        .remove("status");
    let payload = serde_json::to_vec(&deployment)?;
    kubectl(
        config,
        vec!["apply", "--namespace", &config.namespace, "-f", "-"],
        Some(payload),
    )
    .await?;
    Ok(())
}

async fn rollout(config: &OatConfig, name: &str) -> Result<()> {
    let result = tokio::time::timeout(
        Duration::from_secs(config.aks_rollout_timeout_seconds),
        rollout_until_ready(config, name),
    )
    .await;
    let result = match result {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!(
            "Deployment {name} did not become ready within {} seconds",
            config.aks_rollout_timeout_seconds
        )),
    };
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            let diagnostics = rollout_failure_diagnostics(config).await;
            Err(error.context(diagnostics))
        }
    }
}

async fn rollout_until_ready(config: &OatConfig, name: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(config.aks_rollout_timeout_seconds);
    loop {
        let deployment_json = kubectl(
            config,
            vec![
                "get",
                "deployment",
                name,
                "--namespace",
                &config.namespace,
                "-o",
                "json",
            ],
            None,
        )
        .await
        .with_context(|| format!("read Deployment {name} rollout state"))?;
        let deployment: Value = serde_json::from_str(&deployment_json).context("parse Deployment rollout state")?;
        let labels = deployment
            .pointer("/spec/selector/matchLabels")
            .and_then(Value::as_object)
            .context("Deployment selector.matchLabels is missing")?;
        let selector = labels
            .iter()
            .map(|(key, value)| {
                value
                    .as_str()
                    .map(|value| format!("{key}={value}"))
                    .context("Deployment selector labels must be strings")
            })
            .collect::<Result<Vec<_>>>()?
            .join(",");
        anyhow::ensure!(!selector.is_empty(), "Deployment selector must not be empty");
        let desired_replicas = deployment
            .pointer("/spec/replicas")
            .and_then(Value::as_u64)
            .unwrap_or(1);
        anyhow::ensure!(
            desired_replicas > 0,
            "Deployment {name} must request at least one replica"
        );

        let replica_sets_json = kubectl(
            config,
            vec![
                "get",
                "replicasets",
                "--namespace",
                &config.namespace,
                "--selector",
                &selector,
                "-o",
                "json",
            ],
            None,
        )
        .await
        .with_context(|| format!("read ReplicaSets for Deployment {name}"))?;
        let replica_sets: Value = serde_json::from_str(&replica_sets_json).context("parse Deployment ReplicaSets")?;
        let current_hash = rollout_generation_observed(&deployment)
            .then(|| rollout_replica_set_hash(&deployment, &replica_sets))
            .flatten();
        if let Some(hash) = current_hash.as_deref() {
            let pod_selector = format!("{selector},pod-template-hash={hash}");
            let pods_json = kubectl(
                config,
                vec![
                    "get",
                    "pods",
                    "--namespace",
                    &config.namespace,
                    "--selector",
                    &pod_selector,
                    "-o",
                    "json",
                ],
                None,
            )
            .await
            .with_context(|| format!("read current-revision pods for Deployment {name}"))?;
            let pods: Value = serde_json::from_str(&pods_json).context("parse Deployment pod state")?;
            let pod_items = pods
                .get("items")
                .and_then(Value::as_array)
                .context("kubectl pod response has no items array")?;
            for pod in pod_items {
                if let Some(reason) = rollout_pod_failure_for_hash(pod, hash) {
                    let pod_name = pod
                        .pointer("/metadata/name")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown");
                    bail!("Deployment {name} pod {pod_name} failed readiness: {reason}");
                }
            }
        }

        if current_hash.is_some() && rollout_generation_ready(&deployment) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "Deployment {name} did not become ready within {} seconds",
                config.aks_rollout_timeout_seconds
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn rollout_generation_observed(deployment: &Value) -> bool {
    deployment
        .pointer("/metadata/generation")
        .and_then(Value::as_u64)
        .zip(deployment.pointer("/status/observedGeneration").and_then(Value::as_u64))
        .is_some_and(|(generation, observed)| observed >= generation)
}

fn rollout_generation_ready(deployment: &Value) -> bool {
    if !rollout_generation_observed(deployment) {
        return false;
    }
    let Some(desired) = deployment.pointer("/spec/replicas").and_then(Value::as_u64) else {
        return false;
    };
    let status = &deployment["status"];
    let updated = status["updatedReplicas"].as_u64().unwrap_or(0);
    let replicas = status["replicas"].as_u64().unwrap_or(0);
    let available = status["availableReplicas"].as_u64().unwrap_or(0);
    updated == desired && replicas == updated && available >= updated
}

fn rollout_replica_set_hash(deployment: &Value, replica_sets: &Value) -> Option<String> {
    let revision = deployment
        .pointer("/metadata/annotations/deployment.kubernetes.io~1revision")?
        .as_str()?;
    let deployment_name = deployment.pointer("/metadata/name")?.as_str()?;
    let deployment_uid = deployment.pointer("/metadata/uid").and_then(Value::as_str);
    replica_sets.get("items")?.as_array()?.iter().find_map(|replica_set| {
        let matching_owner = replica_set
            .pointer("/metadata/ownerReferences")
            .and_then(Value::as_array)
            .is_some_and(|owners| {
                owners.iter().any(|owner| {
                    owner["kind"].as_str() == Some("Deployment")
                        && owner["name"].as_str() == Some(deployment_name)
                        && deployment_uid.is_none_or(|uid| owner["uid"].as_str() == Some(uid))
                })
            });
        if !matching_owner
            || replica_set
                .pointer("/metadata/annotations/deployment.kubernetes.io~1revision")
                .and_then(Value::as_str)
                != Some(revision)
        {
            return None;
        }
        replica_set
            .pointer("/spec/selector/matchLabels/pod-template-hash")
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
}

fn rollout_pod_failure_for_hash(pod: &Value, expected_hash: &str) -> Option<String> {
    if pod
        .pointer("/metadata/deletionTimestamp")
        .and_then(Value::as_str)
        .is_some()
        || pod
            .pointer("/metadata/labels/pod-template-hash")
            .and_then(Value::as_str)
            != Some(expected_hash)
    {
        return None;
    }
    rollout_pod_failure(pod)
}

fn rollout_pod_failure(pod: &Value) -> Option<String> {
    const IMMEDIATE_FAILURE_REASONS: [&str; 3] = ["ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError"];
    for field in [
        "initContainerStatuses",
        "containerStatuses",
        "ephemeralContainerStatuses",
    ] {
        let Some(statuses) = pod.pointer(&format!("/status/{field}")).and_then(Value::as_array) else {
            continue;
        };
        for status in statuses {
            let container = status.get("name").and_then(Value::as_str).unwrap_or("unknown");
            let restarts = status.get("restartCount").and_then(Value::as_u64).unwrap_or(0);
            for path in ["/state/waiting/reason", "/state/terminated/reason"] {
                if let Some(reason) = status
                    .pointer(path)
                    .and_then(Value::as_str)
                    .filter(|reason| IMMEDIATE_FAILURE_REASONS.contains(reason))
                {
                    return Some(format!("container {container} entered {reason}"));
                }
            }
            if restarts >= 4 && status.get("ready").and_then(Value::as_bool) != Some(true) {
                return Some(format!(
                    "container {container} restarted {restarts} times without becoming ready"
                ));
            }
        }
    }
    None
}

fn rollout_diagnostic_commands(config: &OatConfig) -> Vec<(String, Vec<String>)> {
    let mut commands = vec![
        (
            "namespace pods/services/endpoints".to_owned(),
            vec![
                "get".to_owned(),
                "pods,svc,endpoints".to_owned(),
                "-o".to_owned(),
                "wide".to_owned(),
            ],
        ),
        (
            "namespace events".to_owned(),
            vec![
                "get".to_owned(),
                "events".to_owned(),
                "-o".to_owned(),
                "wide".to_owned(),
            ],
        ),
    ];
    let mut deployments = BTreeSet::from([
        "arrowcol-catalog".to_owned(),
        "arrowcol-sql".to_owned(),
        "arrowcol-router".to_owned(),
        "arrowcol".to_owned(),
        config.arrowcol_deployment.clone(),
        config.dfe_deployment.clone(),
    ]);
    if maintenance_enabled(config) {
        deployments.insert("delta-maintenance".to_owned());
    }
    for deployment in deployments {
        let selector = format!("app={deployment}");
        commands.push((
            format!("Deployment {deployment}"),
            vec!["describe".to_owned(), "deployment".to_owned(), deployment.clone()],
        ));
        commands.push((
            format!("Deployment {deployment}: all pods"),
            vec![
                "describe".to_owned(),
                "pods".to_owned(),
                "--selector".to_owned(),
                selector.clone(),
            ],
        ));
        for previous in [false, true] {
            let mut args = vec![
                "logs".to_owned(),
                "--selector".to_owned(),
                selector.clone(),
                "--all-containers=true".to_owned(),
                "--prefix=true".to_owned(),
                "--tail=200".to_owned(),
                "--limit-bytes=65536".to_owned(),
            ];
            if previous {
                args.push("--previous".to_owned());
            }
            commands.push((
                format!(
                    "Deployment {deployment}: {} logs (bounded diagnostic tail)",
                    if previous { "previous" } else { "current" }
                ),
                args,
            ));
        }
    }
    commands
}

async fn rollout_failure_diagnostics(config: &OatConfig) -> String {
    let details = join_all(
        rollout_diagnostic_commands(config)
            .into_iter()
            .map(|(label, args)| async move {
                let mut command = Command::new("kubectl");
                if let Some(context) = &config.kubectl_context {
                    command.args(["--context", context]);
                }
                command
                    .args(["--request-timeout=5s", "--namespace", &config.namespace])
                    .args(args);
                command.kill_on_drop(true);
                let output = match tokio::time::timeout(Duration::from_secs(10), command.output()).await {
                    Ok(Ok(output)) => format!(
                        "exit={}\n{}\n{}",
                        output.status,
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    ),
                    Ok(Err(error)) => format!("diagnostic command unavailable: {error}"),
                    Err(_) => "diagnostic command exceeded 10-second deadline".to_owned(),
                };
                let mut boundary = output.len().min(65_536);
                while !output.is_char_boundary(boundary) {
                    boundary -= 1;
                }
                format!(
                    "{label}:\n{}{}",
                    &output[..boundary],
                    if boundary < output.len() {
                        "\n[diagnostic truncated at retained-byte cap]"
                    } else {
                        ""
                    }
                )
            }),
    )
    .await;
    details.join("\n")
}

async fn collect_aks_metrics(config: &OatConfig) -> Result<ResourceMetrics> {
    let mut pods = top_metrics(config, &config.arrowcol_pod_selector).await?;
    if config.arrowcol_pod_selector != config.dfe_pod_selector {
        pods.extend(top_metrics(config, &config.dfe_pod_selector).await?);
    }
    Ok(sum_metrics(&pods))
}

fn sum_metrics(pods: &HashMap<String, ResourceMetrics>) -> ResourceMetrics {
    ResourceMetrics {
        cpu_millicores: pods.values().map(|pod| pod.cpu_millicores).sum(),
        memory_bytes: pods
            .values()
            .fold(0_u64, |sum, pod| sum.saturating_add(pod.memory_bytes)),
    }
}

async fn top_metrics(config: &OatConfig, selector: &str) -> Result<HashMap<String, ResourceMetrics>> {
    let snapshot = kubectl(
        config,
        vec![
            "get",
            "pods",
            "--namespace",
            &config.namespace,
            "--selector",
            selector,
            "-o",
            "json",
        ],
        None,
    )
    .await?;
    let expected = active_pod_names(&serde_json::from_str(&snapshot)?)?;
    let wait = async {
        loop {
            let result = kubectl(
                config,
                vec![
                    "top",
                    "pods",
                    "--namespace",
                    &config.namespace,
                    "--selector",
                    selector,
                    "--no-headers",
                ],
                None,
            )
            .await;
            match result {
                Ok(output) => {
                    let metrics = parse_top_metrics(&output)?;
                    if metrics_complete(&expected, &metrics) {
                        return Ok(metrics);
                    }
                    tracing::warn!(selector, expected = ?expected, observed = ?metrics.keys(), "waiting for all current pod metrics");
                }
                Err(error) if error.to_string().contains("metrics not available yet") => {
                    tracing::warn!(selector, %error, "waiting for metrics-server scrape after rollout");
                }
                Err(error) => return Err(error),
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(120), wait)
        .await
        .with_context(|| {
            format!("timed out waiting for complete metrics for selector {selector:?}, pods {expected:?}")
        })?
}

fn active_pod_names(snapshot: &Value) -> Result<HashSet<String>> {
    let names = snapshot
        .get("items")
        .and_then(Value::as_array)
        .context("pod snapshot has no items array")?
        .iter()
        .filter(|pod| pod.pointer("/metadata/deletionTimestamp").is_none_or(Value::is_null))
        .map(|pod| {
            pod.pointer("/metadata/name")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .context("pod snapshot has an unnamed pod")
        })
        .collect::<Result<HashSet<_>>>()?;
    if names.is_empty() {
        bail!("pod snapshot contains no active pods to measure");
    }
    Ok(names)
}

fn metrics_complete(expected: &HashSet<String>, metrics: &HashMap<String, ResourceMetrics>) -> bool {
    metrics.len() == expected.len() && expected.iter().all(|name| metrics.contains_key(name))
}

fn parse_top_metrics(output: &str) -> Result<HashMap<String, ResourceMetrics>> {
    let mut metrics = HashMap::new();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let columns = line.split_whitespace().collect::<Vec<_>>();
        if columns.len() < 3 {
            bail!("unexpected kubectl top output row: {line}");
        }
        if columns[1] == "<unknown>" || columns[2] == "<unknown>" {
            continue;
        }
        metrics.insert(
            columns[0].to_owned(),
            ResourceMetrics {
                cpu_millicores: parse_cpu_millicores(columns[1])?,
                memory_bytes: parse_memory_bytes(columns[2])?,
            },
        );
    }
    Ok(metrics)
}

fn parse_cpu_millicores(value: &str) -> Result<f64> {
    if let Some(millicores) = value.strip_suffix('m') {
        Ok(millicores.parse()?)
    } else {
        Ok(value.parse::<f64>()? * 1000.0)
    }
}

fn parse_memory_bytes(value: &str) -> Result<u64> {
    let value = value.strip_suffix('B').unwrap_or(value);
    let (number, factor) = [
        ("Ki", 1024_u64),
        ("Mi", 1024_u64.pow(2)),
        ("Gi", 1024_u64.pow(3)),
        ("Ti", 1024_u64.pow(4)),
        ("K", 1000_u64),
        ("M", 1000_u64.pow(2)),
        ("G", 1000_u64.pow(3)),
        ("T", 1000_u64.pow(4)),
        ("B", 1_u64),
    ]
    .into_iter()
    .find_map(|(suffix, factor)| value.strip_suffix(suffix).map(|number| (number, factor)))
    .unwrap_or((value, 1));
    Ok((number.parse::<f64>()? * factor as f64) as u64)
}

async fn kubectl(config: &OatConfig, args: Vec<&str>, stdin: Option<Vec<u8>>) -> Result<String> {
    kubectl_with_output_bound(config, args, stdin, None).await
}

async fn kubectl_with_output_bound(
    config: &OatConfig,
    args: Vec<&str>,
    stdin: Option<Vec<u8>>,
    max_output_bytes: Option<usize>,
) -> Result<String> {
    if max_output_bytes.is_some_and(|limit| limit == 0 || limit > OFFICIAL_MEASUREMENT_MAX_BYTES) {
        bail!("invalid native kubectl output byte bound");
    }
    let mut command = Command::new("kubectl");
    if let Some(context) = &config.kubectl_context {
        command.args(["--context", context]);
    }
    if FIXTURE_BOUNDARY.try_with(|_| ()).is_ok() {
        let server = FIXTURE_BOUNDARY.with(|boundary| boundary.authority.server.clone());
        command.args(["--server", &server]);
    }
    command.args(&args);
    if let Some((code, stdout)) = fixture_command_output(&command, stdin.as_deref()).await? {
        if max_output_bytes.is_some_and(|limit| stdout.len() > limit) {
            bail!("native kubectl output exceeded its byte bound");
        }
        if code != 0 {
            bail!("fixture kubectl command failed; outcome may be unknown and must not be replayed");
        }
        return Ok(stdout);
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    }
    let mut child = command.spawn().context("start kubectl")?;
    if let Some(payload) = stdin {
        let mut input = child.stdin.take().context("open kubectl stdin")?;
        input.write_all(&payload).await.context("write kubectl manifest")?;
    }
    let output = match max_output_bytes {
        Some(limit) => bounded_native_child_output(child, limit).await?,
        None => child.wait_with_output().await.context("wait for kubectl")?,
    };
    if !output.status.success() {
        bail!(
            "kubectl {} failed with {}: {}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    String::from_utf8(output.stdout).context("kubectl output was not UTF-8")
}

async fn publish_results(config: &OatConfig, results: &[OatResult]) -> Result<()> {
    let batch = parquet_batch(results)?;
    let authoritative = Url::parse(&config.effective_result_url()?)?;
    let mirror = Url::parse(&config.effective_website_url()?)?;
    if authoritative.scheme() == "file" {
        let target = local_file_path(&authoritative)?;
        merge_local_results(&target, &batch).await?;
    } else {
        let path = ObjectPath::from_url_path(authoritative.path())?;
        let store = object_store_for_url(&authoritative)?;
        put_parquet(store, path, batch).await?;
    }

    for _ in 0..8 {
        let current_authoritative = read_destination_versioned(&authoritative)
            .await
            .context("read authoritative benchmark Parquet after merge")?;
        if !write_exact_destination(&mirror, &authoritative, &current_authoritative)
            .await
            .context("publish authoritative benchmark Parquet to website destination")?
        {
            continue;
        }
        let verified_authoritative = read_destination(&authoritative)
            .await
            .context("verify authoritative benchmark Parquet")?;
        let verified_mirror = read_destination(&mirror)
            .await
            .context("verify website benchmark Parquet copy")?;
        if verified_authoritative == verified_mirror {
            return Ok(());
        }
    }
    bail!("authoritative and website Parquet objects did not converge after 8 verification attempts")
}

fn local_file_path(url: &Url) -> Result<PathBuf> {
    url.to_file_path()
        .map_err(|()| anyhow::anyhow!("invalid local Parquet file URL {url}"))
}

async fn merge_local_results(target: &Path, incoming: &RecordBatch) -> Result<()> {
    let parent = target.parent().context("local results file has no parent directory")?;
    tokio::fs::create_dir_all(parent)
        .await
        .with_context(|| format!("create local results directory {}", parent.display()))?;
    let _lock = acquire_local_publication_lock(target).await?;
    let existing = match tokio::fs::read(target).await {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).with_context(|| format!("read local results file {}", target.display())),
    };
    let bytes = match existing {
        Some(existing) => merge_parquet_bytes(&existing, incoming)?,
        None => encode_parquet(incoming)?,
    };
    write_local_file_unlocked(target, &bytes).await
}

async fn write_local_file_unlocked(target: &Path, bytes: &[u8]) -> Result<()> {
    let parent = target.parent().context("local results file has no parent directory")?;
    let temporary = parent.join(format!(".results-{}.tmp", uuid::Uuid::new_v4()));
    if let Err(error) = tokio::fs::write(&temporary, bytes).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error).with_context(|| format!("write temporary Parquet results {}", temporary.display()));
    }
    if let Err(error) = tokio::fs::rename(&temporary, target).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error).with_context(|| format!("publish local Parquet results {}", target.display()));
    }
    Ok(())
}

struct LocalPublicationLock(std::fs::File);

impl Drop for LocalPublicationLock {
    fn drop(&mut self) {
        if let Err(error) = FileExt::unlock(&self.0) {
            tracing::warn!(error = %error, "could not release local Parquet publication lock");
        }
    }
}

async fn acquire_local_publication_lock(target: &Path) -> Result<LocalPublicationLock> {
    let parent = target.parent().context("local results file has no parent directory")?;
    let file_name = target.file_name().context("local results file has no file name")?;
    let lock_path = parent.join(format!("{}.lock", file_name.to_string_lossy()));
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("open local results lock {}", lock_path.display()))?;
    for _ in 0..150 {
        match FileExt::try_lock_exclusive(&lock) {
            Ok(()) => return Ok(LocalPublicationLock(lock)),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("lock local results file {}", target.display()));
            }
        }
    }
    bail!(
        "timed out waiting 15 seconds for local results lock {}",
        target.display()
    )
}

async fn read_destination(url: &Url) -> Result<Vec<u8>> {
    Ok(read_destination_versioned(url).await?.bytes)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DestinationVersion {
    e_tag: Option<String>,
    version: Option<String>,
}

#[derive(Clone, Debug)]
struct DestinationSnapshot {
    bytes: Vec<u8>,
    version: Option<DestinationVersion>,
}

async fn read_destination_versioned(url: &Url) -> Result<DestinationSnapshot> {
    if url.scheme() == "file" {
        let path = local_file_path(url)?;
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read local Parquet destination {}", path.display()));
        return Ok(DestinationSnapshot {
            bytes: bytes?,
            version: None,
        });
    }
    let path = ObjectPath::from_url_path(url.path())?;
    let store = object_store_for_url(url)?;
    read_cloud_destination_versioned(store, path).await
}

async fn read_cloud_destination_versioned(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
) -> Result<DestinationSnapshot> {
    let object = store
        .get_opts(&path, GetOptions::default())
        .await
        .with_context(|| format!("read Parquet object {path}"))?;
    let version = DestinationVersion {
        e_tag: object.meta.e_tag.clone(),
        version: object.meta.version.clone(),
    };
    if version.e_tag.is_none() && version.version.is_none() {
        bail!("object store returned no conditional version for Parquet object {path}");
    }
    Ok(DestinationSnapshot {
        bytes: object.bytes().await?.to_vec(),
        version: Some(version),
    })
}

async fn write_exact_destination(url: &Url, authoritative: &Url, snapshot: &DestinationSnapshot) -> Result<bool> {
    if url.scheme() == "file" {
        let target = local_file_path(url)?;
        let parent = target.parent().context("local results file has no parent directory")?;
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create local results directory {}", parent.display()))?;
        let _lock = acquire_local_publication_lock(&target).await?;
        if read_destination(authoritative).await? != snapshot.bytes {
            return Ok(false);
        }
        write_local_file_unlocked(&target, &snapshot.bytes).await?;
        return Ok(true);
    }
    let path = ObjectPath::from_url_path(url.path())?;
    let store = object_store_for_url(url)?;
    write_cloud_exact_destination(store, path, snapshot, || read_destination_versioned(authoritative)).await
}

async fn write_cloud_exact_destination<F, Fut>(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    snapshot: &DestinationSnapshot,
    read_authoritative: F,
) -> Result<bool>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<DestinationSnapshot>>,
{
    let mode = match store.get_opts(&path, GetOptions::default()).await {
        Ok(object) => {
            let version = UpdateVersion {
                e_tag: object.meta.e_tag.clone(),
                version: object.meta.version.clone(),
            };
            if version.e_tag.is_none() && version.version.is_none() {
                bail!("object store returned no conditional version for website Parquet object {path}");
            }
            PutMode::Update(version)
        }
        Err(object_store::Error::NotFound { .. }) => PutMode::Create,
        Err(error) => return Err(error).with_context(|| format!("read website Parquet object {path}")),
    };
    let current_authoritative = read_authoritative().await?;
    if current_authoritative.bytes != snapshot.bytes || current_authoritative.version != snapshot.version {
        return Ok(false);
    }
    match store
        .put_opts(&path, PutPayload::from(snapshot.bytes.clone()), PutOptions::from(mode))
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(error) => Err(error).with_context(|| format!("write website Parquet object {path}")),
    }
}

async fn put_parquet(store: Arc<dyn ObjectStore>, path: ObjectPath, incoming: RecordBatch) -> Result<()> {
    for _ in 0..8 {
        let current = match store.get_opts(&path, GetOptions::default()).await {
            Ok(object) => {
                let version = UpdateVersion {
                    e_tag: object.meta.e_tag.clone(),
                    version: object.meta.version.clone(),
                };
                if version.e_tag.is_none() && version.version.is_none() {
                    bail!("object store returned no conditional version for existing benchmark Parquet object {path}");
                }
                let existing_bytes = object.bytes().await?;
                Some((
                    merge_parquet_bytes(&existing_bytes, &incoming)?,
                    PutMode::Update(version),
                ))
            }
            Err(object_store::Error::NotFound { .. }) => Some((encode_parquet(&incoming)?, PutMode::Create)),
            Err(error) => return Err(error).with_context(|| format!("read benchmark Parquet object {path}")),
        };
        let (payload, mode) = current.context("Parquet merge unexpectedly produced no update")?;
        match store
            .put_opts(&path, PutPayload::from(payload), PutOptions::from(mode))
            .await
        {
            Ok(_) => return Ok(()),
            Err(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. }) => continue,
            Err(error) => return Err(error).with_context(|| format!("write benchmark Parquet object {path}")),
        }
    }
    bail!("benchmark Parquet object {path} changed during all 8 merge attempts")
}

fn merge_parquet_bytes(existing_bytes: &[u8], incoming: &RecordBatch) -> Result<Vec<u8>> {
    let existing_reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(existing_bytes))
        .context("read existing benchmark Parquet object")?;
    let schema = result_schema();
    let existing_schema = existing_reader.schema();
    let existing_v3 = existing_schema.as_ref() == schema.as_ref();
    if incoming.schema().as_ref() != schema.as_ref()
        || (existing_schema.as_ref() != schema.as_ref()
            && existing_schema.as_ref() != v2_result_schema().as_ref()
            && existing_schema.as_ref() != legacy_result_schema().as_ref())
    {
        bail!("existing benchmark Parquet schema changed");
    }
    let mut batches = existing_reader
        .build()?
        .map(|batch| -> Result<_> {
            let batch = batch?;
            let versions = batch
                .column_by_name("schema_version")
                .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
                .context("result Parquet has no int32 schema_version")?;
            if versions
                .iter()
                .any(|version| version != Some(2) && !(existing_v3 && version == Some(3)))
            {
                bail!("existing benchmark Parquet contains an unsupported schema_version");
            }
            let mut columns = batch.columns().to_vec();
            if columns.len() < schema.fields().len() {
                columns.push(Arc::new(StringArray::from(vec![None::<&str>; batch.num_rows()])));
            }
            Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
        })
        .collect::<Result<Vec<_>>>()?;
    batches.push(incoming.clone());
    let merged = concat_batches(&schema, &batches)?;
    encode_parquet(&merged)
}

fn deduplicate_results(batch: &RecordBatch) -> Result<RecordBatch> {
    let run_ids = batch
        .column_by_name("run_id")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .context("result Parquet has no UTF-8 run_id column")?;
    let tables = batch
        .column_by_name("table")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .context("result Parquet has no UTF-8 table column")?;
    let mut latest = HashMap::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        if run_ids.is_null(index) || tables.is_null(index) {
            bail!("result Parquet contains a null run_id or table key");
        }
        let run_id = run_ids.value(index);
        let table = tables.value(index);
        latest.insert((run_id, table), index);
    }
    let indices = Int64Array::from_iter_values(
        (0..batch.num_rows())
            .filter(|index| latest.get(&(run_ids.value(*index), tables.value(*index))) == Some(index))
            .map(|index| index as i64),
    );
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None).map_err(anyhow::Error::from))
        .collect::<Result<Vec<ArrayRef>>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

fn parquet_batch(results: &[OatResult]) -> Result<RecordBatch> {
    let schema = result_schema();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from_iter_values(results.iter().map(|_| 3))),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.mode.as_str()),
        )),
        Arc::new(StringArray::from(
            results.iter().map(|row| row.stamp.as_deref()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.host.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.experiment.as_str()),
        )),
        Arc::new(
            TimestampMillisecondArray::from_iter_values(results.iter().map(|row| row.started_at_ms))
                .with_timezone("UTC"),
        ),
        Arc::new(Float64Array::from_iter_values(
            results.iter().map(|row| row.measure_seconds),
        )),
        Arc::new(Int32Array::from_iter_values(results.iter().map(|row| row.arrowcols))),
        Arc::new(Int32Array::from_iter_values(results.iter().map(|row| row.clients))),
        Arc::new(Int32Array::from_iter_values(results.iter().map(|row| row.tables))),
        Arc::new(StringArray::from(
            results.iter().map(|row| row.table_owner.as_deref()).collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            results.iter().map(|row| row.pinning_ok).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.status.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.git_sha.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.tunables_json.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.run_id.as_str()),
        )),
        Arc::new(UInt64Array::from_iter_values(results.iter().map(|row| row.replicas))),
        Arc::new(UInt64Array::from_iter_values(
            results.iter().map(|row| row.dfe_worker_concurrency),
        )),
        Arc::new(StringArray::from_iter_values(
            results.iter().map(|row| row.table.as_str()),
        )),
        Arc::new(UInt64Array::from(
            results.iter().map(|row| row.rows).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            results.iter().map(|row| row.duration_ms).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            results.iter().map(|row| row.rows_per_second).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            results.iter().map(|row| row.cpu_millicores).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            results.iter().map(|row| row.memory_bytes).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from_iter_values(
            results.iter().map(|row| row.qap.observation_complete),
        )),
        Arc::new(UInt64Array::from_iter_values(
            results.iter().map(|row| row.qap.observed_tables),
        )),
        Arc::new(Float64Array::from(
            results.iter().map(|row| row.qap.completion_percent).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            results.iter().map(|row| row.qap.pending_artifacts).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            results.iter().map(|row| row.qap.cached_bytes).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            results.iter().map(|row| row.qap.unhealthy_catalogs).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            results.iter().map(|row| row.qap.lag_seconds).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            results
                .iter()
                .map(|row| row.evidence_json.as_deref())
                .collect::<Vec<_>>(),
        )),
    ];
    Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
}

fn result_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("schema_version", DataType::Int32, false),
        Field::new("mode", DataType::Utf8, false),
        Field::new("stamp", DataType::Utf8, true),
        Field::new("host", DataType::Utf8, false),
        Field::new("experiment", DataType::Utf8, false),
        Field::new(
            "started_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("measure_seconds", DataType::Float64, false),
        Field::new("arrowcols", DataType::Int32, false),
        Field::new("clients", DataType::Int32, false),
        Field::new("tables", DataType::Int32, false),
        Field::new("table_owner", DataType::Utf8, true),
        Field::new("pinning_ok", DataType::Boolean, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("git_sha", DataType::Utf8, false),
        Field::new("tunables_json", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("replicas", DataType::UInt64, false),
        Field::new("dfe_worker_concurrency", DataType::UInt64, false),
        Field::new("table", DataType::Utf8, false),
        Field::new("rows", DataType::UInt64, true),
        Field::new("duration_ms", DataType::UInt64, true),
        Field::new("rows_per_second", DataType::Float64, true),
        Field::new("cpu_millicores", DataType::Float64, true),
        Field::new("memory_bytes", DataType::UInt64, true),
        Field::new("qap_observation_complete", DataType::UInt64, false),
        Field::new("qap_observed_tables", DataType::UInt64, false),
        Field::new("qap_completion_percent", DataType::Float64, true),
        Field::new("qap_pending_artifacts", DataType::UInt64, true),
        Field::new("qap_cached_bytes", DataType::UInt64, true),
        Field::new("qap_unhealthy_catalogs", DataType::UInt64, true),
        Field::new("qap_lag_seconds", DataType::Float64, true),
        Field::new("evidence_json", DataType::Utf8, true),
    ]))
}

fn v2_result_schema() -> Arc<Schema> {
    Arc::new(Schema::new(result_schema().fields()[..31].to_vec()))
}

fn legacy_result_schema() -> Arc<Schema> {
    Arc::new(Schema::new(
        v2_result_schema()
            .fields()
            .iter()
            .map(|field| {
                if matches!(field.name().as_str(), "rows" | "duration_ms" | "rows_per_second") {
                    field.as_ref().clone().with_nullable(false)
                } else {
                    field.as_ref().clone()
                }
            })
            .collect::<Vec<_>>(),
    ))
}

fn encode_parquet(batch: &RecordBatch) -> Result<Vec<u8>> {
    let batch = deduplicate_results(batch)?;
    let schema = batch.schema();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}

fn platform_host_label(kind: crate::config::OatMode) -> &'static str {
    match kind {
        crate::config::OatMode::Local => "local",
        crate::config::OatMode::Aks => "aks",
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn local_bind_probe_uses_effective_client_image() -> anyhow::Result<()> {
        let rendered = serde_json::json!({"services": {
            "oat-client": {"image": "sha256:0123456789abcdef"},
            "dataflow-1": {"image": "other"}
        }});
        assert_eq!(super::local_client_image(&rendered)?, "sha256:0123456789abcdef");
        for invalid in [
            serde_json::json!({"services": {"oat-client": {}}}),
            serde_json::json!({"services": {"oat-client": {"image": ""}}}),
            serde_json::json!({"services": {"oat-client": {"image": "not valid"}}}),
        ] {
            assert!(super::local_client_image(&invalid).is_err());
        }
        let valid_id = format!("sha256:{}", "a".repeat(64));
        assert_eq!(super::local_client_image_id(&format!("{valid_id}\n"))?, valid_id);
        for invalid in [
            String::from("arrowcol-oat-client:local"),
            String::from("sha256:1234"),
            format!("sha256:{}", "g".repeat(64)),
        ] {
            assert!(super::local_client_image_id(&invalid).is_err());
        }
        Ok(())
    }

    #[test]
    fn local_partition_initializer_uses_campaign_table_and_shared_run_root() -> anyhow::Result<()> {
        let root = std::env::temp_dir().join(format!("oat-partition-root-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root)?;
        let args = super::local_partition_initializer_args("sqlserver_availability_replica_states", &root)?;
        assert_eq!(
            args,
            [
                "initialize-local",
                "--partition-by-ingestion-date",
                "--table",
                "telemetry.sqlserver.sqlserver_availability_replica_states",
                "--dataflow-root",
                root.to_str().context("temp root is not valid UTF-8")?,
            ]
        );
        assert!(super::local_partition_initializer_args("sqlserver_wait_stats", &root).is_err());
        assert!(
            super::local_partition_initializer_args(
                "sqlserver_availability_replica_states",
                Path::new("relative/dataflow")
            )
            .is_err()
        );
        std::fs::remove_dir(&root)?;
        Ok(())
    }

    #[test]
    fn rollout_readiness_waits_for_current_generation_and_removal_of_old_replicas() {
        let stale_generation = serde_json::json!({
            "metadata":{"generation":4},"spec":{"replicas":2},
            "status":{"observedGeneration":3,"updatedReplicas":2,"replicas":2,"availableReplicas":2}
        });
        assert!(!super::rollout_generation_ready(&stale_generation));
        let old_replicas_remain = serde_json::json!({
            "metadata":{"generation":4},"spec":{"replicas":2},
            "status":{"observedGeneration":4,"updatedReplicas":2,"replicas":3,"availableReplicas":2}
        });
        assert!(!super::rollout_generation_ready(&old_replicas_remain));
        let current = serde_json::json!({
            "metadata":{"generation":4},"spec":{"replicas":2},
            "status":{"observedGeneration":4,"updatedReplicas":2,"replicas":2,"availableReplicas":2}
        });
        assert!(super::rollout_generation_ready(&current));
    }

    #[test]
    fn rollout_failure_scan_waits_for_the_new_generation_to_be_observed() {
        let deployment = serde_json::json!({"metadata":{"generation":4},"status":{"observedGeneration":3}});
        assert!(!super::rollout_generation_observed(&deployment));
    }

    #[test]
    fn rollout_failure_scan_uses_current_replica_set_and_skips_terminating_pods() {
        let deployment = serde_json::json!({
            "metadata":{"name":"router","uid":"deployment-uid","annotations":{"deployment.kubernetes.io/revision":"8"}}
        });
        let replica_sets = serde_json::json!({"items":[
            {"metadata":{"annotations":{"deployment.kubernetes.io/revision":"7"},"ownerReferences":[
                {"kind":"Deployment","name":"router","uid":"deployment-uid"}]},
             "spec":{"selector":{"matchLabels":{"pod-template-hash":"old-hash"}}}},
            {"metadata":{"annotations":{"deployment.kubernetes.io/revision":"8"},"ownerReferences":[
                {"kind":"Deployment","name":"router","uid":"deployment-uid"}]},
             "spec":{"selector":{"matchLabels":{"pod-template-hash":"current-hash"}}}}
        ]});
        let current_hash = super::rollout_replica_set_hash(&deployment, &replica_sets);
        assert_eq!(current_hash.as_deref(), Some("current-hash"));
        let old_crash = serde_json::json!({
            "metadata":{"labels":{"pod-template-hash":"old-hash"}},
            "status":{"containerStatuses":[{"name":"router","restartCount":0,
                "state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}
        });
        assert!(super::rollout_pod_failure_for_hash(&old_crash, "current-hash").is_none());
        let terminating = serde_json::json!({
            "metadata":{"deletionTimestamp":"2026-11-15T00:00:00Z","labels":{"pod-template-hash":"current-hash"}},
            "status":{"containerStatuses":[{"name":"router","restartCount":0,
                "state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}
        });
        assert!(super::rollout_pod_failure_for_hash(&terminating, "current-hash").is_none());
        let new_crash = serde_json::json!({
            "metadata":{"labels":{"pod-template-hash":"current-hash"}},
            "status":{"containerStatuses":[{"name":"router","restartCount":4,
                "state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}
        });
        assert!(super::rollout_pod_failure_for_hash(&new_crash, "current-hash").is_some());
    }

    #[test]
    fn rollout_pod_failure_detects_expected_states_and_restart_threshold() {
        for reason in ["ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError"] {
            let pod = serde_json::json!({"status":{"containerStatuses":[{
                "name":"dataflow","restartCount":0,"state":{"waiting":{"reason":reason}}
            }]}});
            let failure = super::rollout_pod_failure(&pod).unwrap_or_default();
            assert!(failure.contains(reason), "unexpected failure for {reason}: {failure}");
        }
        let terminated = serde_json::json!({"status":{"initContainerStatuses":[{
            "name":"setup","restartCount":0,"state":{"terminated":{"reason":"Error"}}
        }]}});
        assert!(super::rollout_pod_failure(&terminated).is_none());
        let restarted = serde_json::json!({"status":{"containerStatuses":[{
            "name":"dataflow","restartCount":2,"state":{"running":{}}
        }]}});
        assert!(super::rollout_pod_failure(&restarted).is_none());
        for restarts in 0..4 {
            let recovering = serde_json::json!({"status":{"containerStatuses":[{
                "name":"dataflow","restartCount":restarts,"state":{"waiting":{"reason":"CrashLoopBackOff"}}
            }]}});
            assert!(super::rollout_pod_failure(&recovering).is_none());
        }
        let persistent = serde_json::json!({"status":{"containerStatuses":[{
            "name":"router","restartCount":4,"ready":false,"state":{"waiting":{"reason":"CrashLoopBackOff"}}
        }]}});
        assert!(super::rollout_pod_failure(&persistent).is_some_and(|failure| failure.contains("restarted 4 times")));
        let recovered = serde_json::json!({"status":{"containerStatuses":[{
            "name":"router","restartCount":4,"ready":true,"state":{"running":{}}
        }]}});
        assert!(super::rollout_pod_failure(&recovered).is_none());
        let healthy = serde_json::json!({"status":{"containerStatuses":[{
            "name":"dataflow","restartCount":1,"state":{"running":{}}
        }]}});
        assert!(super::rollout_pod_failure(&healthy).is_none());
    }

    #[test]
    fn selected_aks_stamp_config_is_workspace_contained() -> Result<()> {
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let selected = resolve_aks_stamp_config_path(&workspace, Path::new("infra/azure/stamp.example.yaml"))?;
        assert!(selected.starts_with(workspace.canonicalize()?));
        assert!(resolve_aks_stamp_config_path(&workspace, Path::new("../")).is_err());
        Ok(())
    }

    use super::*;

    #[tokio::test]
    async fn rendered_aks_workload_waits_for_catalog_before_starting_data_plane() -> Result<()> {
        let directory = std::env::temp_dir().join(format!("oat-rollout-order-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory)?;
        let result = async {
            let images = ["sqlserver", "catalog", "router", "dataflow", "maintenance"].into_iter().map(|role| {
                (role.to_owned(), json!(format!("arrowcollector89152.azurecr.io/{role}@sha256:{}", "a".repeat(64))))
            }).collect::<serde_json::Map<_, _>>();
            let input = json!({
                "namespace": "fixture-owned", "oatRunId": "00000000-0000-0000-0000-000000000003",
                "workloadIdentityClientId": "00000000-0000-0000-0000-000000000001",
                "subscriptionId": "00000000-0000-0000-0000-000000000002", "stamp": "89152",
                "deltaRoot": "abfss://onelake@arrowcollector89152.dfs.core.windows.net/dataflow", "images": images,
                "effective_stop_budgets": {"maintenance_grace_seconds": 4_800, "maintenance_stop_timeout_ms": 5_100_000},
                "maintenance": {"tables": ["telemetry.sqlserver.sqlserver_active_sessions"], "operations": ["optimize"], "apply": false},
            });
            let input_path = directory.join("runtime.json");
            fs::write(&input_path, serde_json::to_vec(&input)?)?;
            let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../arrowcol-oat/runtime.ts");
            let output_path = directory.join("workload.json");
            let mut command = Command::new("npx");
            command.current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."));
            command.args(["--no-install", "tsx"]).arg(&script).arg("render")
                .arg(&input_path).arg("workload").arg(&output_path);
            run_command(command, "render workload from the real runtime author").await?;
            let list: Value = serde_json::from_slice(&fs::read(output_path)?)?;
            let rendered = list["items"].as_array().context("rendered workload items missing")?.clone();
            let original_len = rendered.len();
            let (first, second) = partition_aks_workload_resources(rendered)?;
            assert_eq!(first.len() + second.len(), original_len);
            assert_eq!(first.iter().filter(|resource| resource["kind"] == "Deployment").map(|resource| resource["metadata"]["name"].as_str()).collect::<Vec<_>>(), [Some("arrowcol-catalog")]);
            for kind in ["ConfigMap", "Service", "NetworkPolicy"] {
                assert!(first.iter().any(|resource| resource["kind"] == kind));
                assert!(!second.iter().any(|resource| resource["kind"] == kind));
            }
            assert_eq!(second.iter().map(|resource| resource["metadata"]["name"].as_str()).collect::<BTreeSet<_>>(),
                BTreeSet::from([Some("arrowcol"), Some("arrowcol-router"), Some("delta-maintenance")]));
            assert!(second.iter().all(|resource| resource["kind"] == "Deployment"));
            let worker = second.iter().find(|resource| resource["metadata"]["name"] == "delta-maintenance").context("rendered maintenance missing")?;
            assert_eq!(worker["spec"]["replicas"], 0);
            assert!(partition_aks_workload_resources(second).is_err());
            Ok(())
        }.await;
        fs::remove_dir_all(directory)?;
        result
    }

    #[test]
    fn rollout_diagnostics_cover_all_workload_pods_and_namespace_network_state() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.example.json"
        ))?;
        let commands = rollout_diagnostic_commands(&config);
        assert!(
            commands
                .iter()
                .any(|(_, args)| args.iter().any(|arg| arg == "pods,svc,endpoints"))
        );
        assert!(commands.iter().any(|(_, args)| args.iter().any(|arg| arg == "events")));
        for deployment in ["arrowcol-sql", "arrowcol-catalog", "arrowcol-router", "arrowcol"] {
            let selector = format!("app={deployment}");
            assert!(
                commands
                    .iter()
                    .any(|(_, args)| args.first().is_some_and(|arg| arg == "describe")
                        && args.iter().any(|arg| arg == &selector))
            );
            for previous in [false, true] {
                assert!(commands.iter().any(|(label, args)| {
                    label.contains(deployment)
                        && args.first().is_some_and(|arg| arg == "logs")
                        && args.iter().any(|arg| arg == &selector)
                        && args.iter().any(|arg| arg == "--previous") == previous
                        && args.iter().any(|arg| arg == "--all-containers=true")
                        && args.iter().any(|arg| arg == "--tail=200")
                }));
            }
        }
        Ok(())
    }

    #[test]
    fn run_fic_helper_uses_operator_cli_identity_for_every_phase() -> Result<()> {
        for operation in ["prepare-run-fic", "probe-run-fic", "cleanup-run-fic"] {
            let command = run_fic_helper_command(
                operation,
                "00000000-0000-0000-0000-000000000001",
                Some("arrow-collector-00000000000000000000000000000001"),
                Some("arrowcol-oat"),
                Path::new("/tmp/run-fic.json"),
                FicHelperOptions {
                    wait_seconds: None,
                    identity_plan_path: (operation == "prepare-run-fic").then_some(Path::new("/tmp/run-fic-plan.json")),
                },
                AksHelperConfigPaths {
                    stamp_config_path: Path::new("/tmp/stamp-87513.yaml"),
                    bootstrap_env_file: Path::new("/tmp/oat-private-bootstrap.env"),
                },
            )?;
            assert!(
                command.as_std().get_args().collect::<Vec<_>>().windows(2).any(
                    |pair| pair[0] == "--stamp-config" && pair[1] == Path::new("/tmp/stamp-87513.yaml").as_os_str()
                )
            );
            assert!(
                command
                    .as_std()
                    .get_args()
                    .collect::<Vec<_>>()
                    .windows(2)
                    .any(|pair| pair[0] == "--env-file"
                        && pair[1] == Path::new("/tmp/oat-private-bootstrap.env").as_os_str()),
                "{operation} must use the configured private bootstrap env file"
            );
            for variable in ["AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE"] {
                assert!(
                    command
                        .as_std()
                        .get_envs()
                        .any(|(key, value)| key == variable && value.is_none()),
                    "{operation} must clear inherited {variable}"
                );
            }
            assert_eq!(
                command.as_std().get_args().any(|arg| arg == "--identity-plan"),
                operation == "prepare-run-fic"
            );
        }
        assert!(
            run_fic_helper_command(
                "prepare-run-fic",
                "00000000-0000-0000-0000-000000000001",
                Some("oat-test"),
                Some("arrowcol-oat"),
                Path::new("/tmp/run-fic.json"),
                FicHelperOptions {
                    wait_seconds: None,
                    identity_plan_path: None,
                },
                AksHelperConfigPaths {
                    stamp_config_path: Path::new("/tmp/stamp-87513.yaml"),
                    bootstrap_env_file: Path::new("/tmp/oat-private-bootstrap.env"),
                },
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn oat_qap_helpers_use_operator_cli_identity_for_every_phase() -> Result<()> {
        let run_id = "00000000-0000-0000-0000-000000000001";
        let manifest = Path::new("/tmp/oat-qap-manifest.json");
        let workload = Path::new("/tmp/oat-qap-workload.json");
        let stop = Path::new("/tmp/oat-qap-stop.json");
        let record = Path::new("/tmp/oat-qap-record.json");
        let bootstrap_env_file = Path::new("/tmp/oat-private-bootstrap.env");
        for (operation, manifest_path, workload_record_path, workload_stop_record_path) in [
            ("bind-oat-qap", Some(manifest), Some(workload), None),
            ("verify-oat-qap", None, None, None),
            ("oat-qap-reference", None, None, None),
            ("cleanup-oat-qap", None, None, Some(stop)),
        ] {
            let command = run_oat_qap_helper_command(
                operation,
                run_id,
                manifest_path,
                workload_record_path,
                workload_stop_record_path,
                record,
                AksHelperConfigPaths {
                    stamp_config_path: Path::new("/tmp/stamp-87513.yaml"),
                    bootstrap_env_file,
                },
            )?;
            assert!(
                command.as_std().get_args().collect::<Vec<_>>().windows(2).any(
                    |pair| pair[0] == "--stamp-config" && pair[1] == Path::new("/tmp/stamp-87513.yaml").as_os_str()
                )
            );
            assert!(
                command
                    .as_std()
                    .get_args()
                    .collect::<Vec<_>>()
                    .windows(2)
                    .any(|pair| pair[0] == "--env-file" && pair[1] == bootstrap_env_file.as_os_str()),
                "{operation} must use the configured private bootstrap env file"
            );
            for variable in ["AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE"] {
                assert!(
                    command
                        .as_std()
                        .get_envs()
                        .any(|(key, value)| key == variable && value.is_none()),
                    "{operation} must clear inherited {variable}"
                );
            }
            if operation == "cleanup-oat-qap" {
                assert!(command.as_std().get_args().any(|arg| arg == "--workload-stop-record"));
            }
        }
        assert!(
            run_oat_qap_helper_command(
                "cleanup-oat-qap",
                run_id,
                None,
                None,
                None,
                record,
                AksHelperConfigPaths {
                    stamp_config_path: Path::new("/tmp/stamp-87513.yaml"),
                    bootstrap_env_file,
                },
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn aks_client_job_mounts_internal_identity_key_for_direct_catalog_ddl() -> Result<()> {
        let job = build_aks_client_job(AksClientJobSpec {
            namespace: "oat-test",
            client_image: "example.invalid/client@sha256:abc",
            router_auth_secret_name: "arrowcol-router-auth",
            schedule_configmap_name: "oat-test-schedule",
            run_id: "00000000-0000-0000-0000-000000000001",
            run_label: "00000000",
            job_name: "oat-00000000-table-c0",
            args: &["--endpoint".to_owned(), "http://arrowcol-router:50050".to_owned()],
            run_seconds: 3_600,
        });
        let container = &job["spec"]["template"]["spec"]["containers"][0];
        let mounts = container["volumeMounts"].as_array().context("client volume mounts")?;
        let identity_mount = mounts
            .iter()
            .find(|mount| mount["mountPath"] == "/run/secrets/internal-hmac")
            .context("mounted internal identity key")?;
        assert_eq!(identity_mount["name"], "internal-hmac");
        assert_eq!(identity_mount["subPath"], "internal-hmac");
        assert_eq!(identity_mount["readOnly"], true);
        assert_eq!(
            container["env"]
                .as_array()
                .context("client environment")?
                .iter()
                .find(|entry| entry["name"] == "LOADTEST_INTERNAL_SECRET_FILE")
                .context("loadgen internal identity key path")?["value"],
            "/run/secrets/internal-hmac"
        );
        let volumes = job["spec"]["template"]["spec"]["volumes"]
            .as_array()
            .context("client volumes")?;
        let identity_volume = volumes
            .iter()
            .find(|volume| volume["name"] == "internal-hmac")
            .context("internal identity secret volume")?;
        assert_eq!(identity_volume["secret"]["secretName"], "arrowcol-router-auth");
        assert_eq!(
            identity_volume["secret"]["items"][0],
            json!({ "key": "internal-hmac", "path": "internal-hmac" })
        );
        Ok(())
    }

    #[test]
    fn local_client_cleanup_is_scoped_to_the_cell_project_and_service() {
        assert_eq!(
            local_oat_client_list_args("arrowcol-oat-quick-123e4567"),
            [
                "ps",
                "-aq",
                "--filter",
                "label=com.docker.compose.project=arrowcol-oat-quick-123e4567",
                "--filter",
                "label=com.docker.compose.service=oat-client",
            ]
        );
        assert_eq!(
            local_oat_client_remove_args(&["client-a".to_owned(), "client-b".to_owned()]),
            ["rm", "--force", "client-a", "client-b"]
        );
    }

    #[test]
    fn cleanup_failure_after_cancellation_is_not_reported_as_cleaned() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let outcome = finish_matrix_cell(
            &config,
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
            1,
            1,
            Err(anyhow::anyhow!("local OAT run cancelled")),
            Err(anyhow::anyhow!("fixture cleanup failure")),
        );
        assert!(!outcome.cancelled);
        assert!(outcome.error.is_some());
        assert!(outcome.rows.iter().any(|row| {
            row.failure
                .as_deref()
                .is_some_and(|failure| failure.contains("fixture cleanup failure"))
        }));
        Ok(())
    }

    #[test]
    fn aks_runtime_projects_exact_cell_replicas() -> Result<()> {
        for replicas in [1, 2, 16] {
            let mut runtime = json!({"collectorReplicas":7,"routerReplicas":8,"other":"retained"});
            set_runtime_replicas(&mut runtime, replicas)?;
            assert_eq!(runtime["collectorReplicas"], replicas);
            assert_eq!(runtime["routerReplicas"], replicas);
            assert_eq!(runtime["other"], "retained");
        }
        assert!(set_runtime_replicas(&mut json!({}), 0).is_err());
        assert!(set_runtime_replicas(&mut json!({}), 17).is_err());
        assert!(set_runtime_replicas(&mut Value::Null, 1).is_err());
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let original = json!({"other":"retained","collectorReplicas":7});
        let mut runtime = original.clone();
        project_scale_out_runtime(&mut runtime, &config, 2)?;
        assert_eq!(runtime, original);
        config.campaign_profile = Some(crate::config::CampaignProfile::ScaleOutSingleTable);
        config.tables = vec!["sqlserver_availability_replica_states".to_owned()];
        for replicas in [0, 2, 16] {
            let before = runtime.clone();
            assert!(project_scale_out_runtime(&mut runtime, &config, replicas).is_err());
            assert_eq!(runtime, before);
        }
        project_scale_out_runtime(&mut runtime, &config, 1)?;
        assert_eq!(runtime["campaignProfile"], "scale_out_single_table");
        assert_eq!(runtime["hotTable"], config.tables[0]);
        assert_eq!(runtime["collectorReplicas"], 1);
        assert_eq!(runtime["routerReplicas"], 1);
        for cpu in [Some("2"), None] {
            config.platform.resources = Some(serde_json::from_value(json!({
                "collector": {"requests": {"cpu": "2", "memory": "4Gi"},
                              "limits": {"cpu": cpu, "memory": "6Gi"}}
            }))?);
            runtime["resources"] = serde_json::to_value(config.effective_aks_resources())?;
            assert_eq!(runtime.pointer("/resources/collector/limits/cpu"), Some(&json!(cpu)));
        }
        config.tables.push("sqlserver_wait_stats".to_owned());
        assert!(project_scale_out_runtime(&mut runtime, &config, 1).is_err());
        Ok(())
    }

    #[test]
    fn scale_out_profile_allows_aks_and_rejects_local_before_platform_creation() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.campaign_profile = Some(crate::config::CampaignProfile::ScaleOutSingleTable);
        config.platform.kind = crate::config::OatMode::Local;
        let error = platform_for_run(&config, "test-run")
            .err()
            .context("local scale-out was admitted")?;
        assert!(error.to_string().contains("exploratory runs require AKS"));
        config.platform.kind = crate::config::OatMode::Aks;
        let platform = platform_for_run(&config, "test-run")?;
        assert_eq!(platform.kind(), crate::config::OatMode::Aks);
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_stops_matrix_after_the_cleaned_cell() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.replica_counts = vec![1, 2];
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let call_counter = std::sync::Arc::clone(&calls);
        let outcome = run_matrix_with(
            &config,
            "11111111-1111-4111-8111-111111111111",
            &AppState::default(),
            move |_, _, _, _| {
                call_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::future::ready(MatrixOutcome {
                    rows: Vec::new(),
                    error: None,
                    cancelled: true,
                    aks_cleanup_confirmed: false,
                })
            },
        )
        .await;
        assert!(outcome.cancelled);
        assert!(outcome.error.is_none());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_confirmation_requires_every_attempted_matrix_cell() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.replica_counts = vec![1, 2];
        config.dfe_worker_concurrency = vec![1];
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let call_counter = std::sync::Arc::clone(&calls);
        let outcome = run_matrix_with(
            &config,
            "11111111-1111-4111-8111-111111111111",
            &AppState::default(),
            move |_, _, _, _| {
                let cell = call_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::future::ready(MatrixOutcome {
                    rows: Vec::new(),
                    error: (cell == 1).then(|| anyhow::anyhow!("second cell failed")),
                    cancelled: false,
                    aks_cleanup_confirmed: cell == 0,
                })
            },
        )
        .await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(outcome.error.is_some());
        assert!(!outcome.aks_cleanup_confirmed);
        Ok(())
    }

    #[test]
    fn failed_clients_preserve_pinning_evidence_without_masking_unrelated_failures() -> Result<()> {
        let tables = vec!["SQLServer_Wait_Stats".to_owned()];
        let healthy = HashMap::from([("sqlserver_wait_stats".to_owned(), vec!["collector-a".to_owned()])]);
        let unrelated = anyhow::anyhow!("DMV source connection failed");
        assert!(
            failed_client_pinning_report(&unrelated, Ok(healthy.clone()), &tables, PinningReport::default())
                .violation
                .is_none()
        );
        let missing = failed_client_pinning_report(&unrelated, Ok(HashMap::new()), &tables, PinningReport::default());
        assert!(
            missing
                .violation
                .as_deref()
                .is_some_and(|message| message.contains("no active ArrowCol"))
        );
        let upstream = anyhow::anyhow!("gRPC failed: pinning_violation: pinned collector became unavailable");
        let failed = failed_client_pinning_report(&upstream, Ok(healthy), &tables, PinningReport::default());
        assert_eq!(
            failed.owners.get("sqlserver_wait_stats").map(String::as_str),
            Some("collector-a")
        );
        assert!(
            failed
                .violation
                .as_deref()
                .is_some_and(|message| message.contains("pinning_violation"))
        );

        let metrics =
            "arrowcol_router_failures_total{operation=\"doput\",pool=\"dataflow\",reason=\"pinning_violation\"} 1\n";
        assert!(parse_owner_metrics(metrics, &tables).is_err());
        assert!(parse_owner_metrics(&metrics.replace("} 1", "} 0"), &tables).is_ok());
        let authority_loss = failed_client_pinning_report(
            &unrelated,
            parse_owner_metrics(metrics, &tables),
            &tables,
            PinningReport::default(),
        );
        assert!(
            authority_loss
                .violation
                .as_deref()
                .is_some_and(|message| message.contains("router rejected 1"))
        );
        Ok(())
    }

    #[test]
    fn local_credentials_fit_sql_server_password_limits() {
        let secret = random_secret();
        assert!((32..=128).contains(&secret.len()));
        assert!(secret.bytes().all(|byte| byte.is_ascii_graphic()));
        assert!(secret.bytes().any(|byte| byte.is_ascii_uppercase()));
        assert!(secret.bytes().any(|byte| byte.is_ascii_lowercase()));
        assert!(secret.bytes().any(|byte| byte.is_ascii_digit()));
        assert!(secret.bytes().any(|byte| byte.is_ascii_punctuation()));
    }

    #[test]
    fn snapshot_stop_keeps_writer_dependencies_alive_and_rejects_uncertain_custody() -> Result<()> {
        let project = "owned-run";
        let services = ["sqlserver", "arrowcol-catalog", "dataflow-1", "router", "oat-client"];
        let before = services.iter().enumerate().map(|(index, service)| json!({
            "Id": format!("id-{index}"),
            "Config": {"Labels": {"com.docker.compose.project": project, "com.docker.compose.service": service}},
            "State": {"Running": true}
        })).collect::<Vec<_>>();
        assert_eq!(
            snapshot_stop_phases(&before)?,
            [
                vec!["id-3".to_owned(), "id-4".to_owned()],
                vec![],
                vec!["id-2".to_owned()],
                vec!["id-0".to_owned(), "id-1".to_owned()]
            ]
        );
        let mut after = before.clone();
        for container in &mut after {
            container["State"] = json!({
                "Running": false, "Paused": false, "Dead": false, "Status": "exited",
                "Error": "", "ExitCode": 0, "OOMKilled": false, "FinishedAt": "2026-01-01T00:00:00Z"
            });
        }
        after[2]["State"]["ExitCode"] = json!(143);
        assert_eq!(
            snapshot_container_outcomes(&before, &after, project)?.len(),
            services.len()
        );
        let mut invalid = after.clone();
        invalid[2]["State"]["ExitCode"] = json!(137);
        assert!(snapshot_container_outcomes(&before, &invalid, project).is_err());
        invalid = after.clone();
        invalid[2]["State"]["OOMKilled"] = json!(true);
        assert!(snapshot_container_outcomes(&before, &invalid, project).is_err());
        invalid = after.clone();
        invalid[2]["Id"] = json!("replacement");
        assert!(snapshot_container_outcomes(&before, &invalid, project).is_err());
        invalid = after.clone();
        invalid[2]["Config"]["Labels"]["com.docker.compose.project"] = json!("other-run");
        assert!(snapshot_container_outcomes(&before, &invalid, project).is_err());
        let mut already_failed = before;
        already_failed[2]["State"]["Running"] = json!(false);
        assert!(snapshot_container_outcomes(&already_failed, &after, project).is_err());
        Ok(())
    }
    #[test]
    fn aks_collector_stop_grace_readback_requires_exact_value() {
        let deployment = json!({"spec":{"template":{"spec":{"terminationGracePeriodSeconds":300}}}});
        assert!(verify_aks_collector_stop_grace(&deployment, 300).is_ok());
        assert!(verify_aks_collector_stop_grace(&deployment, 301).is_err());
        assert!(verify_aks_collector_stop_grace(&json!({}), 300).is_err());
    }

    #[test]
    fn run_owned_job_origin_markers_cover_metadata_and_template_without_changing_selectors() -> Result<()> {
        let mut job = json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": "oat-run-client-c0",
                "namespace": "oat-run-1",
                "labels": {"app": "arrowcol-oat-client"},
            },
            "spec": {
                "selector": {"matchLabels": {"batch.kubernetes.io/controller-uid": "selector-value"}},
                "template": {
                    "metadata": {"labels": {"app": "arrowcol-oat-client"}},
                    "spec": {"containers": []},
                },
            },
        });
        let selector = job["spec"]["selector"].clone();
        super::stamp_run_origin_markers(&mut job, "oat-run-1", "run-1", "owner-1")?;
        for metadata in [&job["metadata"], &job["spec"]["template"]["metadata"]] {
            for field in ["labels", "annotations"] {
                assert_eq!(metadata[field]["arrowcol-oat/run-id"], "run-1");
                assert_eq!(metadata[field]["arrowcol-oat/owner-token"], "owner-1");
            }
        }
        assert_eq!(job["spec"]["selector"], selector);
        let planned = super::planned_inventory_resource(&job, "oat-run-1")?;
        assert_eq!(planned.api_version, "batch/v1");
        assert_eq!(planned.kind, "Job");
        assert_eq!(planned.name, "oat-run-client-c0");
        assert_eq!(planned.namespace, "oat-run-1");
        Ok(())
    }

    #[test]
    fn campaign_identity_requires_complete_canonical_fenced_context() -> Result<()> {
        let mut values = HashMap::from([
            ("CAMPAIGN_ID", "00000000-0000-0000-0000-000000000001"),
            ("CAMPAIGN_OWNER_ID", "00000000-0000-0000-0000-000000000002"),
            ("CAMPAIGN_FENCING_TOKEN", "1"),
        ]);
        let parse = |values: &HashMap<&str, &str>| {
            CampaignIdentity::from_lookup(|name| values.get(name).map(|value| (*value).to_owned()))
        };
        assert_eq!(parse(&values)?.fencing_token, 1);
        for token in ["0", "-1", "1x", "9007199254740992"] {
            values.insert("CAMPAIGN_FENCING_TOKEN", token);
            assert!(parse(&values).is_err());
        }
        values.insert("CAMPAIGN_FENCING_TOKEN", "1");
        values.insert("CAMPAIGN_OWNER_ID", "00000000-0000-0000-0000-000000000000");
        assert!(parse(&values).is_err());
        values.remove("CAMPAIGN_ID");
        assert!(parse(&values).is_err());
        Ok(())
    }

    #[test]
    fn rust_initial_campaign_inventory_serializes_for_the_final_ts_predicate() -> anyhow::Result<()> {
        let identity = super::CampaignIdentity {
            campaign_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            owner_id: "00000000-0000-0000-0000-000000000002".to_owned(),
            fencing_token: 7,
        };
        let inventory = super::initial_campaign_inventory(
            &identity,
            "00000000-0000-0000-0000-000000000003",
            "00000000-0000-0000-0000-000000000004",
            "arrow-collector-00000000000000000000000000000004",
            "campaignstorage",
            "checkpoints",
            CampaignCellBudget {
                effective_stop_budgets: EffectiveStopBudgets {
                    maintenance_grace_seconds: 1_200,
                    maintenance_stop_timeout_ms: 1_500_000,
                },
                planned_cell_duration_ms: 1_000,
            },
        )?;
        super::validate_campaign_baseline_capture_inventory(&inventory, &identity)?;
        let value = serde_json::to_value(&inventory)?;
        assert_eq!(value["load_admission"], "not_authorized");
        assert_eq!(value["namespace"]["uid"], serde_json::Value::Null);
        assert_eq!(value["resources"], serde_json::json!([]));
        assert_eq!(value["fic"], serde_json::Value::Null);
        assert_eq!(value["qap"], serde_json::Value::Null);
        assert_eq!(value["effective_stop_budgets"]["maintenance_grace_seconds"], 1_200);
        assert_eq!(
            value["effective_stop_budgets"]["maintenance_stop_timeout_ms"],
            1_500_000
        );
        assert_eq!(value["planned_cell_duration_ms"], 1_000);
        let mut no_row_rehearsal = inventory.clone();
        no_row_rehearsal.planned_cell_duration_ms = Some(0);
        no_row_rehearsal.resources.push(InventoryResource {
            api_version: "apps/v1".to_owned(),
            kind: "Deployment".to_owned(),
            namespace: no_row_rehearsal.namespace.name.clone(),
            name: "arrowcol".to_owned(),
            uid: None,
            role: InventoryResourceRole::Collector,
            state: InventoryResourceState::Planned,
            absence: None,
        });
        no_row_rehearsal.validate()?;
        println!("E1_RUST_INITIAL_INVENTORY={}", serde_json::to_string(&inventory)?);
        Ok(())
    }

    #[test]
    fn campaign_cli_actions_always_receive_the_selected_private_env_and_stamp_paths() -> Result<()> {
        let paths = AksHelperConfigPaths {
            stamp_config_path: Path::new("/workspace/selected-stamp.yaml"),
            bootstrap_env_file: Path::new("/private/campaign.env"),
        };
        let actions = [
            "publish-inventory",
            "assert-ready",
            "read-inventory",
            "capture-cell-baseline",
            "cleanup-step",
            "capture-baseline",
        ];
        for action in actions {
            let action_args = if action == "capture-baseline" {
                vec!["00000000-0000-0000-0000-000000000002".to_owned(), "7".to_owned()]
            } else {
                Vec::new()
            };
            let command = aks_campaign_cli_command(paths, action, &action_args)?;
            let args = command
                .as_std()
                .get_args()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            assert_eq!(args[0], "--no-install");
            assert_eq!(args[1], "tsx");
            assert_eq!(args[2], "projects/arrowcol-oat/aks-campaign.ts");
            assert_eq!(args[3], action);
            assert_eq!(
                &args[args.len() - 4..],
                [
                    "--env-file",
                    "/private/campaign.env",
                    "--stamp-config",
                    "/workspace/selected-stamp.yaml"
                ]
            );
            if action == "capture-baseline" {
                assert_eq!(args[4], "00000000-0000-0000-0000-000000000002");
                assert_eq!(args[5], "7");
            }
        }
        Ok(())
    }

    #[test]
    fn campaign_baseline_response_requires_exact_producer_and_blob_proof() -> Result<()> {
        let identity = CampaignIdentity {
            campaign_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            owner_id: "00000000-0000-0000-0000-000000000002".to_owned(),
            fencing_token: 7,
        };
        let success = json!({"ok":true,"receipt":{
            "status":"captured",
            "campaign_id":identity.campaign_id,
            "owner_id":identity.owner_id,
            "fencing_token":identity.fencing_token,
            "blob":"_arrowcol-custody/campaigns/00000000-0000-0000-0000-000000000001/baseline.json",
            "etag":"\"baseline-etag\"",
            "captured_at_unix_ms":1731600000000u64
        }});
        let bytes = serde_json::to_vec(&success)?;
        let receipt = parse_campaign_baseline_response(&bytes, &identity)?;
        assert_eq!(receipt.blob, campaign_baseline_blob(&identity.campaign_id));
        for (field, value) in [
            ("campaign_id", json!("00000000-0000-0000-0000-000000000009")),
            ("owner_id", json!("00000000-0000-0000-0000-000000000009")),
            ("fencing_token", json!(8)),
            ("blob", json!("https://storage.example/baseline.json")),
            ("captured_at_unix_ms", json!(0)),
        ] {
            let mut changed = success.clone();
            changed["receipt"][field] = value;
            assert!(parse_campaign_baseline_response(&serde_json::to_vec(&changed)?, &identity).is_err());
        }
        let mut missing = success.clone();
        missing["receipt"]
            .as_object_mut()
            .context("baseline receipt is not an object")?
            .remove("etag");
        assert!(parse_campaign_baseline_response(&serde_json::to_vec(&missing)?, &identity).is_err());
        let failure = json!({"ok":false,"code":"write_outcome_unknown","message":"not confirmed"});
        let error = match parse_campaign_baseline_response(&serde_json::to_vec(&failure)?, &identity) {
            Err(error) => error,
            Ok(_) => anyhow::anyhow!("failed baseline response was accepted"),
        };
        assert!(error.to_string().contains("write_outcome_unknown"));
        Ok(())
    }

    #[test]
    fn campaign_custody_helper_classifies_pre_effect_staleness_only_for_cleanup() -> Result<()> {
        let stale = campaign_custody_helper_failure("cleanup-step", br#"{"ok":false,"code":"stale_version"}"#);
        assert!(stale.downcast_ref::<StaleCampaignVersionError>().is_some());
        let stale_read = campaign_custody_helper_failure("read-inventory", br#"{"ok":false,"code":"stale_version"}"#);
        assert!(stale_read.downcast_ref::<StaleCampaignVersionError>().is_none());
        for code in ["campaign_fenced", "write_outcome_unknown", "invalid_or_storage_error"] {
            let response = json!({"ok":false,"code":code});
            let response_bytes = serde_json::to_vec(&response)?;
            let error = campaign_custody_helper_failure("cleanup-step", &response_bytes);
            assert!(error.downcast_ref::<StaleCampaignVersionError>().is_none());
            assert!(error.to_string().contains(code));
        }
        Ok(())
    }

    #[test]
    fn campaign_load_admission_waits_for_observed_qap_and_serializes_each_stage() -> Result<()> {
        let identity = CampaignIdentity {
            campaign_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            owner_id: "00000000-0000-0000-0000-000000000002".to_owned(),
            fencing_token: 7,
        };
        let cell_run_id = "00000000-0000-0000-0000-000000000004";
        let namespace = "arrow-collector-00000000000000000000000000000004";
        let mut inventory = initial_campaign_inventory(
            &identity,
            "00000000-0000-0000-0000-000000000003",
            cell_run_id,
            namespace,
            "campaignstorage",
            "checkpoints",
            CampaignCellBudget {
                effective_stop_budgets: EffectiveStopBudgets {
                    maintenance_grace_seconds: 1_200,
                    maintenance_stop_timeout_ms: 1_500_000,
                },
                planned_cell_duration_ms: 1_000,
            },
        )?;
        inventory.namespace.uid = Some("00000000-0000-0000-0000-000000000005".to_owned());
        inventory.resources.push(InventoryResource {
            api_version: "apps/v1".to_owned(),
            kind: "Deployment".to_owned(),
            namespace: namespace.to_owned(),
            name: "arrowcol".to_owned(),
            uid: Some("00000000-0000-0000-0000-000000000006".to_owned()),
            role: InventoryResourceRole::Collector,
            state: InventoryResourceState::Observed,
            absence: None,
        });
        inventory.fic = Some(InventoryFic {
            resource_id: Some("/subscriptions/00000000-0000-0000-0000-000000000007/resourceGroups/perf69/providers/Microsoft.ManagedIdentity/userAssignedIdentities/arrow-collector/federatedIdentityCredentials/oat-run".to_owned()),
            name: Some("oat-run".to_owned()),
            state: InventoryResourceState::Observed,
            absence: None,
            plan: None,
        });
        let binding = InventoryQapBinding {
            external_table: "arrowcol_data".to_owned(),
            relative_path: format!(
                "oat-prefix/runs/{cell_run_id}/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states"
            ),
        };
        inventory.qap = Some(InventoryQap {
            database: "perf69_qap".to_owned(),
            bindings: vec![binding.clone()],
            state: InventoryResourceState::Planned,
            absence: Vec::new(),
            reference: None,
        });
        inventory.load_admission = InventoryLoadAdmission::AuthorizationPending;
        inventory.validate()?;
        assert!(
            validate_campaign_inventory_load_admission(
                &inventory,
                InventoryLoadAdmission::AuthorizationPending,
                "perf69_qap",
                std::slice::from_ref(&binding),
            )
            .is_err()
        );
        let preflight = inventory.clone();
        let qap = inventory.qap.as_mut().context("planned QAP inventory is missing")?;
        qap.state = InventoryResourceState::Observed;
        qap.reference = Some(InventoryQapReference {
            schema_version: 1,
            account: "campaignstorage".to_owned(),
            container: "checkpoints".to_owned(),
            blob: "campaigns/00000000-0000-0000-0000-000000000001/qap.json".to_owned(),
            run_id: cell_run_id.to_owned(),
            nonce: "00000000-0000-0000-0000-000000000008".to_owned(),
            manifest_sha256: "a".repeat(64),
            namespace_uid: "00000000-0000-0000-0000-000000000005".to_owned(),
            namespace_owner_token: identity.owner_id.clone(),
        });
        inventory.validate()?;
        validate_campaign_inventory_load_admission(
            &inventory,
            InventoryLoadAdmission::AuthorizationPending,
            "perf69_qap",
            std::slice::from_ref(&binding),
        )?;
        let qap_observed_pending = inventory.clone();
        inventory.load_admission = InventoryLoadAdmission::Authorized;
        qap_observed_pending.validate_successor(&inventory)?;
        validate_campaign_inventory_load_admission(
            &inventory,
            InventoryLoadAdmission::Authorized,
            "perf69_qap",
            std::slice::from_ref(&binding),
        )?;
        println!(
            "E1_RUST_LOAD_SEQUENCE={}",
            serde_json::to_string(&serde_json::json!({
                "preflight": preflight,
                "qap_observed_pending": qap_observed_pending,
                "authorized": inventory,
            }))?
        );
        Ok(())
    }

    #[test]
    fn campaign_cell_baseline_matches_the_frozen_envelope_and_absence_contract() -> Result<()> {
        let step = json!({"state":"not_started","deadline_unix_ms":null,"reason_code":null});
        let hot_path = "oat-prefix/runs/00000000-0000-0000-0000-000000000004/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states";
        let inventory: RunInventory = serde_json::from_value(json!({
            "schema_version":1,
            "campaign_id":"00000000-0000-0000-0000-000000000001",
            "owner_id":"00000000-0000-0000-0000-000000000002",
            "fencing_token":1,
            "orchestration_run_id":"00000000-0000-0000-0000-000000000003",
            "cell_run_id":"00000000-0000-0000-0000-000000000004",
            "load_admission":"not_authorized",
            "namespace":{"name":"oat-00000000000000000000000000000004","uid":"00000000-0000-0000-0000-000000000005",
                "owner_token":"00000000-0000-0000-0000-000000000002"},
            "resources":[{"api_version":"apps/v1","kind":"Deployment","namespace":"oat-00000000000000000000000000000004",
                "name":"arrowcol","uid":null,"role":"collector","state":"planned"}],
            "fic":null,
            "qap":{"database":"OatDatabase","bindings":[{"external_table":"oat_00000000000000000000000000000004_sqlserver_availability_replica_states",
                "relative_path":hot_path}],"state":"planned","absence":[],"reference":null},
            "cleanup":{"stop_admission":step,"bounded_drain":step,"evidence_capture":step,
                "stop_maintenance":step,"terminal_capture":step,"handoff":step}
        }))?;
        inventory.validate()?;
        let identity = CampaignIdentity {
            campaign_id: inventory.campaign_id.clone(),
            owner_id: inventory.owner_id.clone(),
            fencing_token: inventory.fencing_token,
        };
        let mut initial_inventory = inventory.clone();
        initial_inventory.resources.clear();
        initial_inventory.namespace.uid = None;
        initial_inventory.qap = None;
        validate_campaign_baseline_capture_inventory(&initial_inventory, &identity)?;
        assert!(validate_campaign_baseline_capture_inventory(&inventory, &identity).is_err());
        let actor = CampaignCleanupActor {
            mode: CampaignActorMode::Producer,
            owner_id: identity.owner_id.clone(),
            fencing_token: identity.fencing_token,
        };
        let request = CampaignCellBaselineRequest {
            schema_version: 1,
            campaign_id: &identity.campaign_id,
            orchestration_run_id: &inventory.orchestration_run_id,
            cell_run_id: &inventory.cell_run_id,
            expected_etag: "\"inventory-etag\"",
            hot_delta_relative_path: hot_path,
            actor: &actor,
        };
        assert_eq!(
            serde_json::to_value(&request)?,
            json!({
                "schema_version":1,
                "campaign_id":"00000000-0000-0000-0000-000000000001",
                "orchestration_run_id":"00000000-0000-0000-0000-000000000003",
                "cell_run_id":"00000000-0000-0000-0000-000000000004",
                "expected_etag":"\"inventory-etag\"",
                "hot_delta_relative_path":hot_path,
                "actor":{"mode":"producer","owner_id":"00000000-0000-0000-0000-000000000002","fencing_token":1}
            })
        );
        let output = json!({"ok":true,"receipt":{
            "status":"captured","campaign_id":identity.campaign_id,"owner_id":identity.owner_id,
            "fencing_token":identity.fencing_token,"orchestration_run_id":inventory.orchestration_run_id,
            "cell_run_id":inventory.cell_run_id,"namespace_uid":"00000000-0000-0000-0000-000000000005",
            "hot_delta_relative_path":hot_path,"blob":"_arrowcol-custody/campaigns/00000000-0000-0000-0000-000000000001/runs/00000000-0000-0000-0000-000000000003/cells/00000000-0000-0000-0000-000000000004/delta-before.json",
            "etag":"\"baseline-etag\"","delta_state":"observed_absent","table_id":null,"delta_version":null,"rows":null
        }});
        let response: CampaignCellBaselineResponse = serde_json::from_value(output.clone())?;
        let receipt = response.receipt.context("baseline receipt missing")?;
        assert!(response.ok);
        validate_campaign_cell_baseline(&receipt, &inventory, &identity, &inventory.cell_run_id, hot_path)?;
        let mut loss_shaped_absence = output;
        loss_shaped_absence["receipt"]["rows"] = json!(0);
        let response: CampaignCellBaselineResponse = serde_json::from_value(loss_shaped_absence)?;
        let receipt = response.receipt.context("baseline receipt missing")?;
        assert!(
            validate_campaign_cell_baseline(&receipt, &inventory, &identity, &inventory.cell_run_id, hot_path).is_err()
        );
        Ok(())
    }

    #[test]
    fn campaign_cleanup_preserves_the_approved_maintenance_and_evidence_order() {
        let actions = [
            CampaignCleanupAction::StopAdmission,
            CampaignCleanupAction::BoundedDrain,
            CampaignCleanupAction::EvidenceCapture,
            CampaignCleanupAction::StopMaintenance,
            CampaignCleanupAction::TerminalCapture,
            CampaignCleanupAction::RemoveNamespace,
            CampaignCleanupAction::RemoveQap,
            CampaignCleanupAction::RemoveFic,
            CampaignCleanupAction::Handoff,
        ];
        assert_eq!(
            actions.map(|action| action.selector()),
            [
                "stop_admission",
                "bounded_drain",
                "evidence_capture",
                "stop_maintenance",
                "terminal_capture",
                "remove_namespace",
                "remove_qap",
                "remove_fic",
                "handoff",
            ]
        );
    }

    #[test]
    fn custody_inventory_never_replaces_exact_ownership_or_accepts_unknown_observations() -> Result<()> {
        let step = json!({"state":"not_started","deadline_unix_ms":null,"reason_code":null});
        let mut inventory: RunInventory = serde_json::from_value(json!({
            "schema_version": 1,
            "campaign_id":"00000000-0000-0000-0000-000000000001",
            "owner_id":"00000000-0000-0000-0000-000000000002",
            "fencing_token":1,
            "orchestration_run_id":"00000000-0000-0000-0000-000000000003",
            "cell_run_id":"00000000-0000-0000-0000-000000000004",
            "load_admission":"not_authorized",
            "effective_stop_budgets":{"maintenance_grace_seconds":1200,"maintenance_stop_timeout_ms":1500000},
            "planned_cell_duration_ms":1000,
            "namespace":{"name":"oat-00000000000000000000000000000004", "uid":null,
                "owner_token":"00000000-0000-0000-0000-000000000002"},
            "resources":[{"api_version":"apps/v1","kind":"Deployment",
                "namespace":"oat-00000000000000000000000000000004","name":"arrowcol",
                "uid":null,"role":"collector","state":"planned"}],
            "fic":null,"qap":null,
            "cleanup":{"stop_admission":step,"bounded_drain":step,"evidence_capture":step,
                "stop_maintenance":step,"terminal_capture":step,"handoff":step}
        }))?;
        let mut legacy_schema = serde_json::to_value(&inventory)?;
        legacy_schema["cleanup"]
            .as_object_mut()
            .context("serialized cleanup object missing")?
            .remove("stop_maintenance");
        assert!(serde_json::from_value::<RunInventory>(legacy_schema).is_err());
        let mut legacy_admission = serde_json::to_value(&inventory)?;
        legacy_admission
            .as_object_mut()
            .context("serialized inventory object missing")?
            .remove("load_admission");
        assert!(serde_json::from_value::<RunInventory>(legacy_admission).is_err());
        inventory.validate()?;
        let mut authorization_pending = inventory.clone();
        authorization_pending.load_admission = InventoryLoadAdmission::AuthorizationPending;
        inventory.validate_successor(&authorization_pending)?;
        let mut authorized = authorization_pending.clone();
        authorized.load_admission = InventoryLoadAdmission::Authorized;
        authorization_pending.validate_successor(&authorized)?;
        assert!(inventory.validate_successor(&authorized).is_err());
        assert!(authorized.validate_successor(&authorization_pending).is_err());
        assert!(campaign_cleanup_action_needs_execution(
            &CampaignCleanupAction::StopAdmission,
            &inventory
        )?);
        assert_eq!(
            campaign_cleanup_operation_deadline_ms(&CampaignCleanupAction::StopAdmission, &inventory, 1_000)?,
            61_000
        );
        let mut declared_deadline = inventory.clone();
        declared_deadline.cleanup.bounded_drain.deadline_unix_ms = Some(5_000);
        assert_eq!(
            campaign_cleanup_operation_deadline_ms(&CampaignCleanupAction::BoundedDrain, &declared_deadline, 1_000)?,
            5_000
        );
        assert_eq!(
            campaign_cleanup_operation_deadline_ms(&CampaignCleanupAction::StopMaintenance, &inventory, 1_000)?,
            inventory
                .effective_stop_budgets
                .context("inventory stop budgets")?
                .maintenance_stop_timeout_ms
                + 1_000
        );
        let mut legacy_cleanup = inventory.clone();
        legacy_cleanup.effective_stop_budgets = None;
        legacy_cleanup.planned_cell_duration_ms = None;
        legacy_cleanup.cleanup.stop_maintenance.deadline_unix_ms = Some(5_000);
        assert_eq!(
            campaign_cleanup_operation_deadline_ms(&CampaignCleanupAction::StopMaintenance, &legacy_cleanup, 1_000)?,
            5_000,
            "legacy cleanup must retain its originally persisted deadline without extending it"
        );
        let mut expired_deadline = inventory.clone();
        expired_deadline.cleanup.stop_admission.deadline_unix_ms = Some(61_000);
        assert!(
            campaign_cleanup_operation_deadline_ms(&CampaignCleanupAction::StopAdmission, &expired_deadline, 61_000)
                .is_err()
        );
        for state in [
            InventoryStepState::Pending,
            InventoryStepState::Running,
            InventoryStepState::Failed,
            InventoryStepState::Uncertain,
        ] {
            let mut unresolved = inventory.clone();
            unresolved.cleanup.stop_admission.state = state;
            assert!(
                campaign_cleanup_action_needs_execution(&CampaignCleanupAction::StopAdmission, &unresolved).is_err()
            );
        }
        let mut complete_step = inventory.clone();
        complete_step.cleanup.stop_admission.state = InventoryStepState::Complete;
        assert!(!campaign_cleanup_action_needs_execution(
            &CampaignCleanupAction::StopAdmission,
            &complete_step
        )?);
        let mut uncertain_resource = inventory.clone();
        uncertain_resource.resources[0].state = InventoryResourceState::Uncertain;
        assert!(
            campaign_cleanup_action_needs_execution(&CampaignCleanupAction::RemoveNamespace, &uncertain_resource)
                .is_err()
        );
        validate_campaign_etag("\\\"strong-etag\\\"")?;
        assert!(validate_campaign_etag("W/\"weak-etag\"").is_err());
        assert!(validate_campaign_etag("*").is_err());
        let actor = CampaignCleanupActor {
            mode: CampaignActorMode::Producer,
            owner_id: inventory.owner_id.clone(),
            fencing_token: inventory.fencing_token,
        };
        let read_request = serde_json::to_value(CampaignInventoryReadRequest {
            schema_version: 1,
            campaign_id: &inventory.campaign_id,
            orchestration_run_id: &inventory.orchestration_run_id,
            cell_run_id: &inventory.cell_run_id,
            actor: &actor,
        })?;
        let read_fields = read_request
            .as_object()
            .context("read request is not an object")?
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            read_fields,
            BTreeSet::from([
                "schema_version",
                "campaign_id",
                "orchestration_run_id",
                "cell_run_id",
                "actor",
            ])
        );
        assert_eq!(read_request["actor"]["mode"], "producer");
        let cleanup_request = serde_json::to_value(CampaignCleanupRequest {
            schema_version: 1,
            inventory: &inventory,
            expected_etag: "\"versioned-etag\"",
            actor: &actor,
        })?;
        let cleanup_fields = cleanup_request
            .as_object()
            .context("cleanup request is not an object")?
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            cleanup_fields,
            BTreeSet::from(["schema_version", "inventory", "expected_etag", "actor"])
        );
        assert_eq!(
            cleanup_request["inventory"]["cleanup"]["stop_maintenance"]["state"],
            "not_started"
        );
        let receipt = CampaignCleanupReceipt {
            status: "complete".to_owned(),
            step: "stop_maintenance".to_owned(),
            campaign_id: inventory.campaign_id.clone(),
            owner_id: inventory.owner_id.clone(),
            fencing_token: inventory.fencing_token,
            orchestration_run_id: inventory.orchestration_run_id.clone(),
            cell_run_id: inventory.cell_run_id.clone(),
            namespace_uid: None,
            actor: actor.clone(),
            completed_at_unix_ms: 1_000,
            etag: "\"receipt-etag\"".to_owned(),
        };
        let mut receipt_without_namespace_uid = serde_json::to_value(receipt)?;
        receipt_without_namespace_uid
            .as_object_mut()
            .context("receipt is not an object")?
            .remove("namespace_uid");
        assert!(serde_json::from_value::<CampaignCleanupReceipt>(receipt_without_namespace_uid).is_err());
        let mut missing_stop_deadline = inventory.clone();
        missing_stop_deadline.cleanup.stop_maintenance.state = InventoryStepState::Running;
        assert!(missing_stop_deadline.validate().is_err());
        missing_stop_deadline.cleanup.stop_maintenance.deadline_unix_ms = Some(0);
        assert!(missing_stop_deadline.validate().is_err());
        missing_stop_deadline.cleanup.stop_maintenance.deadline_unix_ms = Some(1_000);
        missing_stop_deadline.validate()?;
        let mut wrong_owner = inventory.clone();
        wrong_owner.namespace.owner_token = "00000000-0000-0000-0000-000000000005".to_owned();
        assert!(wrong_owner.validate().is_err());
        wrong_owner = inventory.clone();
        wrong_owner.namespace.uid = Some("not-a-uid".to_owned());
        assert!(wrong_owner.validate().is_err());
        let persisted = json!({"ok":true,"receipt":{
            "status":"persisted","campaign_id":inventory.campaign_id,"owner_id":inventory.owner_id,
            "fencing_token":inventory.fencing_token,"orchestration_run_id":inventory.orchestration_run_id,
            "cell_run_id":inventory.cell_run_id,"namespace_uid":null,"resource_count":1,
            "fic_state":null,"qap_state":null,"etag":"\"etag\""
        }});
        inventory.validate_custody_receipt(&serde_json::to_vec(&persisted)?, false)?;
        let mut bad_receipt = persisted.clone();
        bad_receipt["receipt"]["fencing_token"] = json!(2);
        assert!(
            inventory
                .validate_custody_receipt(&serde_json::to_vec(&bad_receipt)?, false)
                .is_err()
        );
        bad_receipt = persisted.clone();
        bad_receipt["receipt"]["resource_count"] = json!(0);
        assert!(
            inventory
                .validate_custody_receipt(&serde_json::to_vec(&bad_receipt)?, false)
                .is_err()
        );
        assert!(
            inventory
                .validate_custody_receipt(br#"{"ok":false,"code":"write_outcome_unknown"}"#, false)
                .is_err()
        );
        let mut absent = inventory.clone();
        absent.resources[0].state = InventoryResourceState::Absent;
        assert!(absent.validate().is_err());
        absent.resources[0].absence = Some(InventoryAbsenceReceipt {
            api_version: "apps/v1".to_owned(),
            kind: "Deployment".to_owned(),
            namespace: inventory.namespace.name.clone(),
            namespace_uid: None,
            name: "arrowcol".to_owned(),
            observed_at_unix_ms: 123,
        });
        inventory.validate_successor(&absent)?;
        let mut invalid_absence = absent.clone();
        invalid_absence.resources[0]
            .absence
            .as_mut()
            .context("missing absence receipt")?
            .name = "other".to_owned();
        assert!(invalid_absence.validate().is_err());
        invalid_absence = absent.clone();
        invalid_absence.resources[0].uid = Some("unexpected".to_owned());
        assert!(invalid_absence.validate().is_err());
        invalid_absence = absent.clone();
        invalid_absence.resources[0]
            .absence
            .as_mut()
            .context("missing absence receipt")?
            .observed_at_unix_ms = 124;
        assert!(absent.validate_successor(&invalid_absence).is_err());
        invalid_absence = absent.clone();
        invalid_absence.resources[0].state = InventoryResourceState::Planned;
        invalid_absence.resources[0].absence = None;
        assert!(absent.validate_successor(&invalid_absence).is_err());
        let mut external = inventory.clone();
        external.fic = Some(serde_json::from_value(json!({
            "resource_id":"/subscriptions/a/federatedIdentityCredentials/fic",
            "name":"fic","state":"absent",
            "absence":{"resource_id":"/subscriptions/a/federatedIdentityCredentials/fic",
                "name":"fic","observed_at_unix_ms":1}
        }))?);
        external.qap = Some(serde_json::from_value(json!({
            "database":"db","bindings":[
                {"external_table":"table_a","relative_path":"run/a"},
                {"external_table":"table_b","relative_path":"run/b"}
            ],"state":"absent","absence":[
                {"database":"db","external_table":"table_a","relative_path":"run/a","observed_at_unix_ms":1},
                {"database":"db","external_table":"table_b","relative_path":"run/b","observed_at_unix_ms":2}
            ]
        }))?);
        external.validate()?;
        let mut partial = external.clone();
        partial.qap.as_mut().context("missing QAP inventory")?.state = InventoryResourceState::Uncertain;
        partial.qap.as_mut().context("missing QAP inventory")?.absence.pop();
        partial.validate()?;
        partial.validate_successor(&external)?;
        assert!(external.validate_successor(&partial).is_err());
        let mut complete = external.clone();
        complete.cleanup.handoff.state = InventoryStepState::Complete;
        assert!(complete.validate().is_err());
        complete.cleanup.stop_admission.state = InventoryStepState::Complete;
        complete.cleanup.bounded_drain.state = InventoryStepState::Complete;
        complete.cleanup.evidence_capture.state = InventoryStepState::Complete;
        complete.cleanup.stop_maintenance.state = InventoryStepState::Complete;
        complete.cleanup.terminal_capture.state = InventoryStepState::Complete;
        assert!(complete.validate().is_err());
        complete.resources = absent.resources.clone();
        complete.validate()?;
        complete.cleanup.stop_maintenance.state = InventoryStepState::NotStarted;
        assert!(complete.validate().is_err());
        complete.cleanup.stop_maintenance.state = InventoryStepState::Complete;
        complete.validate()?;
        let mut invalid_external = external.clone();
        invalid_external
            .fic
            .as_mut()
            .context("missing FIC inventory")?
            .absence
            .as_mut()
            .context("missing FIC absence")?
            .name = "other".to_owned();
        assert!(invalid_external.validate().is_err());
        invalid_external = external.clone();
        let qap = invalid_external.qap.as_mut().context("missing QAP inventory")?;
        qap.absence[1] = qap.absence[0].clone();
        assert!(invalid_external.validate().is_err());
        invalid_external = external.clone();
        invalid_external.qap.as_mut().context("missing QAP inventory")?.absence[0].observed_at_unix_ms = 9;
        assert!(external.validate_successor(&invalid_external).is_err());
        let mut observed = inventory.clone();
        observed.resources[0].state = InventoryResourceState::Observed;
        assert!(observed.validate().is_err());
        observed.namespace.uid = Some("00000000-0000-0000-0000-000000000005".to_owned());
        observed.resources[0].uid = Some("00000000-0000-0000-0000-000000000006".to_owned());
        let ready = json!({"ok":true,"receipt":{
            "status":"ready","campaign_id":inventory.campaign_id,"owner_id":inventory.owner_id,
            "fencing_token":inventory.fencing_token,"orchestration_run_id":inventory.orchestration_run_id,
            "cell_run_id":inventory.cell_run_id,"namespace_uid":"00000000-0000-0000-0000-000000000005"
        }});
        observed.validate_custody_receipt(&serde_json::to_vec(&ready)?, true)?;
        assert!(
            inventory
                .validate_custody_receipt(&serde_json::to_vec(&ready)?, true)
                .is_err()
        );
        inventory.validate_successor(&observed)?;
        inventory = observed;
        let mut replacement = inventory.clone();
        replacement.resources[0].uid = Some("00000000-0000-0000-0000-000000000007".to_owned());
        assert!(inventory.validate_successor(&replacement).is_err());
        replacement = inventory.clone();
        replacement.resources[0].state = InventoryResourceState::Planned;
        assert!(inventory.validate_successor(&replacement).is_err());
        replacement = inventory.clone();
        replacement.cleanup.stop_admission.state = InventoryStepState::Running;
        assert!(replacement.validate().is_err());
        replacement.cleanup.stop_admission.deadline_unix_ms = Some(1_000);
        replacement.validate()?;
        replacement.cleanup.evidence_capture.state = InventoryStepState::Uncertain;
        assert!(replacement.validate().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn non_snapshot_cleanup_retains_resources_after_stop_failure() -> Result<()> {
        let actions = std::cell::RefCell::new(Vec::new());
        let result = stop_then_destroy(
            async {
                actions.borrow_mut().push("stop");
                Err::<(), _>(anyhow::anyhow!("stop failed with exit 137"))
            },
            async {
                actions.borrow_mut().push("destroy");
                Err(anyhow::anyhow!("owned volume deletion failed"))
            },
        )
        .await;
        assert_eq!(*actions.borrow(), ["stop"]);
        let error = format!("{:#}", result.err().context("stop must fail")?);
        assert!(error.contains("exit 137"));
        assert!(error.contains("retaining owned resources"));
        Ok(())
    }

    #[tokio::test]
    async fn non_snapshot_cleanup_retains_resources_after_stop_timeout() -> Result<()> {
        let destroyed = std::sync::atomic::AtomicBool::new(false);
        let result = stop_then_destroy(
            async {
                tokio::time::timeout(Duration::from_millis(1), std::future::pending::<Result<()>>())
                    .await
                    .context("stop deadline exceeded")?
            },
            async {
                destroyed.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
        assert!(!destroyed.load(std::sync::atomic::Ordering::SeqCst));
        assert!(format!("{:#}", result.err().context("stop must time out")?).contains("stop deadline exceeded"));
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cleanup_failure_paths_retain_owned_resources_and_gate_aks_exits() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let fixture = match std::env::var_os("OAT_CLEANUP_FIXTURE_ROOT") {
            Some(root) => PathBuf::from(root),
            None => {
                let root = std::env::temp_dir().join(format!("oat-cleanup-{}", uuid::Uuid::new_v4()));
                fs::create_dir(&root)?;
                fs::create_dir(root.join("bin"))?;
                let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cleanup-platform.ts");
                for tool in ["docker", "kubectl"] {
                    let path = root.join("bin").join(tool);
                    fs::write(
                        &path,
                        format!(
                            "#!/bin/sh\nexec node --import tsx '{}' {tool} \"$@\"\n",
                            script.display()
                        ),
                    )?;
                    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
                }
                let mut command = Command::new(std::env::current_exe()?);
                command.args([
                    "--exact",
                    "runner::tests::cleanup_failure_paths_retain_owned_resources_and_gate_aks_exits",
                    "--nocapture",
                ]);
                command.env("OAT_CLEANUP_FIXTURE_ROOT", &root);
                let mut paths = vec![root.join("bin")];
                paths.extend(std::env::split_paths(
                    &std::env::var_os("PATH").context("fixture PATH")?,
                ));
                command.env("PATH", std::env::join_paths(paths)?);
                let result = tokio::time::timeout(
                    Duration::from_secs(60),
                    run_command(command, "isolated cleanup regression fixture"),
                )
                .await;
                fs::remove_dir_all(&root)?;
                let output = result.context("cleanup regression timed out")??;
                print!("{}", String::from_utf8_lossy(&output.stdout));
                return Ok(());
            }
        };
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.example.json"
        ))?;
        config.platform.local.snapshot_root = None;
        let state = fixture.join("local-state");
        fs::create_dir(&state)?;
        let mut local = LocalPlatform {
            run_id: "fixture-run".to_owned(),
            state_dir: state.clone(),
            environment: HashMap::new(),
            control_dir: state.clone(),
            client_progress: std::sync::Mutex::new(HashMap::new()),
            router_metrics_url: None,
            collector_metrics_url: None,
            qap_url: None,
            daemon_endpoint: None,
            shutdown_drain_evidence: None,
            maintenance_cell_baseline: Vec::new(),
        };
        fs::write(fixture.join("mode"), "stop_failure")?;
        let error = cleanup_local_platform(&config, &local)
            .await
            .err()
            .context("local stop must fail")?;
        assert!(format!("{error:#}").contains("fixture stop listing failed"));
        assert!(state.exists(), "local state must be retained after stop failure");
        assert!(
            !fixture.join("compose-destroyed").exists(),
            "Compose down must be skipped"
        );
        fs::write(fixture.join("mode"), "maintenance_cell")?;
        fs::write(
            fixture.join("project"),
            scoped_compose_project(
                config.platform.compose_project.as_deref().context("fixture project")?,
                &local.run_id,
            ),
        )?;
        let baseline = local.process_logs(&config).await?;
        local.retain_cell_process_baseline(&baseline);
        fs::write(fixture.join("measurement-finished"), "")?;
        let outcomes = stop_local_snapshot_writers(&config, &local).await?;
        assert_eq!(
            outcomes[0]["maintenance_outcomes"]["operation_count"], 155,
            "cell scope must include warm-up/measurement and the final drain cycle"
        );
        assert_eq!(
            outcomes[0]["maintenance_outcomes"]["retry_kind_counts"]["stale_plan"],
            25
        );
        assert_eq!(outcomes[0]["maintenance_operation_failure"], false);
        assert_eq!(outcomes[0]["maintenance_exit_classification"], "nonzero_health_exit");
        assert_eq!(outcomes[0]["exit_code"], 1);
        assert!(!serde_json::to_string(&outcomes)?.contains("private"));
        fs::write(fixture.join("mode"), "overload_success")?;
        local.run_id = "123e4567-e89b-42d3-a456-426614174002".to_owned();
        local.state_dir = fixture.join(&local.run_id);
        local.control_dir = local.state_dir.clone();
        fs::create_dir(&local.state_dir)?;
        config.platform.compose_state_root = Some(fixture.to_string_lossy().into_owned());
        fs::create_dir(local.state_dir.join("control"))?;
        fs::write(
            fixture.join("project"),
            scoped_compose_project(
                config.platform.compose_project.as_deref().context("fixture project")?,
                &local.run_id,
            ),
        )?;
        let baseline = local.process_logs(&config).await?;
        local.retain_cell_process_baseline(&baseline);
        let run_id = uuid::Uuid::new_v4().to_string();
        let mut row = sample_result(&run_id, "sqlserver_wait_stats", 0);
        row.status = "failed".to_owned();
        row.failure = Some("above_ceiling: overload_stop hard_cap_fraction".to_owned());
        let stop = json!({"classification":"above_ceiling","reason":"hard_cap_fraction","metric":"container_memory_bytes",
            "observed_value":900,"cap":1000,"fraction":0.9,"window_index":0,"timestamp_ms":1});
        row.evidence_json = Some(json!({"version":1,"overload_stop":stop}).to_string());
        config.tunables.maintenance = Some(serde_json::from_value(json!({
            "enabled": true, "operations": ["optimize", "purge", "vacuum", "checkpoint", "prune_log"]
        }))?);
        let duration = (config.warmup_seconds + config.measure_seconds) * 1_000;
        let scheduled = unix_time_ms_u64()?
            .checked_sub(duration + 1_000)
            .context("fixture clock")?;
        let mut successful = sample_result(&local.run_id, "sqlserver_wait_stats", 1);
        successful.evidence_json = Some(
            json!({"client_schedule":{
                "scope":"orchestrator_assigned_schedule_with_observed_full_fleet_acknowledgement",
                "effective":{"run_start_unix_ms":scheduled,"acknowledged_at_unix_ms":scheduled - 1,
                    "measurement_start_unix_ms":scheduled + config.warmup_seconds * 1_000,
                    "measurement_end_unix_ms":scheduled + duration}
            }})
            .to_string(),
        );
        let schedule_path = local.state_dir.join("control/schedule");
        fs::write(&schedule_path, format!("{scheduled}\n"))?;
        let state = AppState::default();
        let orchestration = "123e4567-e89b-42d3-a456-426614174000";
        state.runs.lock().await.insert(
            orchestration.to_owned(),
            crate::model::RunStatus {
                id: orchestration.to_owned(),
                config_sha256: "0".repeat(64),
                state: crate::model::RunState::Running,
                progress: "measurement complete".to_owned(),
                result_count: 0,
                error: None,
                started_at_unix_ms: 1,
                finished_at_unix_ms: None,
                local_teardown: None,
                aks_cleanup_confirmed: None,
                aks_cleanup_resolved: false,
            },
        );
        fs::write(&schedule_path, format!("{}\n", scheduled + 1))?;
        assert!(local_teardown_transition(&config, &local.run_id, std::slice::from_ref(&successful)).is_err());
        fs::write(&schedule_path, format!("{scheduled}\n"))?;
        publish_local_teardown(
            &state,
            orchestration,
            &config,
            &local.run_id,
            std::slice::from_ref(&successful),
        )
        .await;
        local.cleanup(&config).await.context("guard-triggered normal cleanup")?;
        assert!(!schedule_path.exists());
        let status = state
            .runs
            .lock()
            .await
            .get(orchestration)
            .cloned()
            .context("missing status")?;
        assert!(matches!(status.state, crate::model::RunState::Running));
        assert!(status.finished_at_unix_ms.is_none());
        println!("D49_CLEANUP_GAP_STATUS {}", serde_json::to_string(&status)?);
        let transition = status.local_teardown.context("missing pre-cleanup transition")?;
        assert_eq!(transition.cell_id, local.run_id);
        assert_eq!(transition.schedule_unix_ms, scheduled);
        assert_eq!(transition.measurement_end_unix_ms, scheduled + duration);
        let receipt = local
            .shutdown_drain_evidence()
            .context("normal lifecycle receipt missing")?;
        let mut outcome = finish_matrix_cell(&config, "orchestration", &run_id, 1, 1, Ok(vec![row]), Ok(()));
        attach_shutdown_drain_evidence(&mut outcome, receipt)?;
        assert!(outcome.error.is_some());
        assert_eq!(outcome.rows[0].status, "failed");
        let retained: Value = serde_json::from_str(
            outcome.rows[0]
                .evidence_json
                .as_deref()
                .context("guard evidence missing")?,
        )?;
        assert_eq!(retained["overload_stop"], stop);
        assert_eq!(
            retained["shutdown_drain"]["collectors"][0]["drain"]["status"],
            "observed"
        );
        assert_eq!(
            retained["shutdown_drain"]["maintenance"][0]["outcomes"]["operation_count"],
            5
        );
        assert_eq!(retained["shutdown_drain"]["maintenance"][0]["exit_code"], 0);
        assert!(fixture.join("compose-destroyed").exists());
        assert!(fixture.join("guard-maintenance-removed").exists());
        let profiles: Vec<String> = serde_json::from_slice(&fs::read(fixture.join("down-profiles"))?)?;
        assert_eq!(profiles, ["benchmark", "maintenance"]);
        let commands = fs::read_to_string(fixture.join("commands"))?;
        let ingress = commands
            .find("stop --time 300 guard-client")
            .context("ingress was not stopped")?;
        let maintenance_grace = config.maintenance_stop_budget()?.maintenance_grace_seconds;
        let maintenance_command = format!("stop --time {maintenance_grace} guard-maintenance");
        let maintenance = commands
            .find(&maintenance_command)
            .context("maintenance was not stopped with its derived grace")?;
        let collector = commands
            .find("stop --time 300 guard-collector")
            .context("collector was not stopped")?;
        assert!(ingress < maintenance && maintenance < collector);
        fs::write(fixture.join("mode"), "stop_failure")?;

        config.platform.kind = crate::config::OatMode::Aks;
        config.namespace = "fixture-owned".to_owned();
        config.kubectl_context = None;
        config.arrowcol_deployment = "fixture".to_owned();
        config.dfe_deployment = "fixture".to_owned();
        config.dfe_container = "dataflow".to_owned();
        config.tunables.maintenance = None;
        let runtime = fixture.join("runtime");
        fs::create_dir(&runtime)?;
        let ownership = runtime.join("ready.json");
        fs::write(
            &ownership,
            serde_json::to_vec(&json!({
                "phase": "ready", "runId": "fixture-run", "namespace": config.namespace,
                "namespaceUid": "fixture-uid", "namespaceOwner": "fixture-owner",
                "aksId": "/fixture/aks", "server": "https://fixture.invalid",
            }))?,
        )?;
        fs::write(
            fixture.join("namespace.json"),
            serde_json::to_vec(&json!({
                "metadata": {"uid": "fixture-uid",
                    "labels": {"arrowcol-oat/run-id": "fixture-run", "arrowcol-oat/owner-token": "fixture-owner"},
                    "annotations": {"arrowcol-oat/run-id": "fixture-run", "arrowcol-oat/owner-token": "fixture-owner"},
                },
            }))?,
        )?;
        let mut platform = AksPlatform {
            run_id: "fixture-run".to_owned(),
            resource_readback: Vec::new(),
            cpu_throttling_start: None,
            cpu_throttling_evidence: None,
            namespace_created: true,
            namespace_create_attempted: true,
            namespace_owner_token: "fixture-owner".to_owned(),
            namespace_uid: Some("fixture-uid".to_owned()),
            runtime_dir: Some(runtime.clone()),
            qap_relative_data_root: None,
            delta_root: None,
            router_metrics_url: None,
            router_tunnel: None,
            fic_record_path: None,
            fic_ready_snapshot_path: Some(ownership),
            fic_prepare_attempted: true,
            qap_record_path: None,
            qap_bind_attempted: true,
            qap_cleanup_confirmed: false,
            qap_outcome_unresolved: true,
            run_table_initialization_attempted: false,
            run_table_initialization_drained: false,
            workload_stop_record_path: None,
            schedule_configmap_name: String::new(),
            service_account: None,
            router_yaml: None,
            shutdown_drain_evidence: None,
            campaign_cli_paths: None,
        };
        platform.run_table_initialization_attempted = true;
        assert!(require_aks_cleanup_confirmation(&platform).is_err());
        platform.run_table_initialization_drained = true;
        assert!(require_aks_cleanup_confirmation(&platform).is_err());
        platform.qap_cleanup_confirmed = true;
        require_aks_cleanup_confirmation(&platform)?;
        platform.qap_cleanup_confirmed = false;
        let error = cleanup_aks_platform(&config, &mut platform)
            .await
            .err()
            .context("AKS stop must fail")?;
        let error = format!("{error:#}");
        assert!(error.contains("fixture deployment stop failed"));
        assert!(error.contains("run-owned QAP cleanup was not positively confirmed"));
        assert!(error.contains("retaining owned resources"));
        assert!(platform.namespace_created);
        assert!(fixture.join("namespace.json").exists(), "namespace must be retained");
        assert!(!fixture.join("delete-options.json").exists());
        assert!(!runtime.join("workload-stop-receipt.json").exists());

        fs::write(fixture.join("mode"), "absent_deployment")?;
        let missing = stop_aks_deployment_for_cleanup(&config, "fixture", ComponentRole::Collector, 30).await?;
        assert_eq!(missing["status"], "nothing_to_stop");
        fs::write(fixture.join("mode"), "exit_137")?;
        let error = stop_aks_deployment_for_cleanup(&config, "fixture", ComponentRole::Collector, 30)
            .await
            .err()
            .context("AKS exit 137 must fail")?;
        assert!(format!("{error:#}").contains("forcibly stopped (137)"));
        fs::remove_file(fixture.join("pods-read"))?;
        fs::write(fixture.join("mode"), "missing_exit")?;
        let evidence = stop_aks_deployment_for_cleanup(&config, "fixture", ComponentRole::Collector, 30).await?;
        assert_eq!(evidence["processes"][0]["exit_outcome"]["status"], "unavailable");
        assert_eq!(evidence["processes"][0]["drain"]["exit_code"], Value::Null);
        fs::write(fixture.join("mode"), "rollout_recovers")?;
        config.aks_rollout_timeout_seconds = 30;
        rollout(&config, "fixture").await?;
        fs::write(fixture.join("mode"), "rollout_persistent")?;
        let started = std::time::Instant::now();
        let failure = rollout(&config, "fixture")
            .await
            .err()
            .context("persistent crash must fail")?;
        let detail = format!("{failure:#}");
        assert!(detail.contains("restarted 4 times"));
        assert!(detail.contains("namespace pods/services/endpoints"));
        assert!(detail.contains("namespace events"));
        for role in ["arrowcol-catalog", "arrowcol-router", "arrowcol-sql", "arrowcol"] {
            assert!(detail.contains(&format!("Deployment {role}: previous logs")));
        }
        assert!(started.elapsed() < Duration::from_secs(15));
        fs::write(fixture.join("mode"), "rollout_hang")?;
        config.aks_rollout_timeout_seconds = 1;
        let started = std::time::Instant::now();
        let failure = rollout(&config, "fixture")
            .await
            .err()
            .context("hung API request must time out")?;
        assert!(format!("{failure:#}").contains("did not become ready within 1 seconds"));
        assert!(started.elapsed() < Duration::from_secs(15));
        Ok(())
    }

    #[test]
    fn local_stop_arguments_use_the_shared_maintenance_and_collector_graces() {
        for (phase, grace) in [(0, 300), (1, 1200), (2, 3600), (3, 300)] {
            assert_eq!(
                local_phase_stop_args(phase, 1200, 3600, vec!["owned-container".to_owned()]),
                vec![
                    "stop".to_owned(),
                    "--time".to_owned(),
                    grace.to_string(),
                    "owned-container".to_owned()
                ],
            );
        }
    }

    #[test]
    fn local_stop_deadline_covers_derived_maintenance_budget_and_other_phases() {
        let deadline = local_stop_deadline(3600, 900_000);
        assert_eq!(
            deadline,
            Duration::from_millis(900_000) + Duration::from_secs(3600 + 2 * 300 + 120)
        );
        assert!(deadline > Duration::from_secs(3600 + 3 * 300));
    }

    #[test]
    fn absent_shutdown_deployment_is_nothing_to_stop_not_a_parse_failure() -> Result<()> {
        assert!(optional_shutdown_deployment("")?.is_none());
        assert!(optional_shutdown_deployment(" \n")?.is_none());
        assert_eq!(
            optional_shutdown_deployment(r#"{"spec":{"replicas":1}}"#)?.context("deployment was present")?["spec"]["replicas"],
            1
        );
        assert!(optional_shutdown_deployment("invalid JSON").is_err());
        Ok(())
    }

    #[test]
    fn aks_shutdown_exit_gate_rejects_forced_death_and_marks_missing_state_unavailable() -> Result<()> {
        for role in [
            ComponentRole::Collector,
            ComponentRole::Router,
            ComponentRole::Maintenance,
        ] {
            assert!(shutdown_exit_outcome(role, true, Some(137), false).is_err());
            assert!(shutdown_exit_outcome(role, true, Some(2), false).is_err());
            assert!(shutdown_exit_outcome(role, true, Some(0), true).is_err());
            assert!(shutdown_exit_outcome(role, true, None, true).is_err());
            assert_eq!(shutdown_exit_outcome(role, true, None, false)?["status"], "unavailable");
            assert_eq!(
                shutdown_exit_outcome(role, true, Some(143), false)?["status"],
                "observed"
            );
            assert!(shutdown_exit_outcome(role, false, Some(143), false).is_err());
        }
        assert!(shutdown_exit_outcome(ComponentRole::Collector, true, Some(1), false).is_err());
        assert_eq!(
            shutdown_exit_outcome(ComponentRole::Maintenance, true, Some(1), false)?["classification"],
            "requires_operation_records",
        );
        Ok(())
    }

    #[test]
    fn collector_shutdown_evidence_records_commits_and_quiver_drain() -> Result<()> {
        let signal = 1_700_000_000_000_u64;
        let event = json!({
            "schema_version": 1,
            "outcome": "committed",
            "rows": 120,
            "finished_at_unix_ms": signal as i64 + 250,
        });
        let text = format!(
            "arrowcol_data_commit {}\ndurable_buffer.shutdown.drained bundles_drained = 3\nquiver.engine.stop force_dropped_bundles = 0\n",
            serde_json::to_string(&event)?
        );
        let log = ProcessLog {
            role: ComponentRole::Collector,
            instance_id: "pod/dataflow".to_owned(),
            image: None,
            image_id: None,
            source_revision: None,
            complete: true,
            raw_log_bytes: text.len() as u64,
            raw_log_lines: 3,
            stdout: text.clone(),
            stderr: String::new(),
            text,
        };
        let evidence =
            collector_shutdown_drain_evidence(&log, Some(signal), Some("2023-11-14T22:13:21Z"), Some(0), true);
        assert_eq!(evidence["status"], "observed");
        assert_eq!(evidence["sigterm_to_exit_seconds"], 1.0);
        assert_eq!(evidence["commits_after_sigterm"], 1);
        assert_eq!(evidence["committed_rows_after_sigterm"], 120);
        assert_eq!(evidence["quiver"]["bundles_drained"], 3);
        assert_eq!(evidence["quiver"]["status"], "observed");
        assert_eq!(evidence["quiver"]["force_dropped_bundles"], 0);
        assert_eq!(evidence["exit_code"], 0);
        Ok(())
    }

    #[test]
    fn collector_shutdown_evidence_records_zero_drain_after_completed_shutdown() -> Result<()> {
        let text = concat!(
            "otel.processor.durable_buffer::durable_buffer.shutdown.start\n",
            "otel.processor.durable_buffer::durable_buffer.shutdown.complete\n",
            "otel-arrow-dfe-quiver::quiver.engine.stop [force_dropped_bundles=0]\n",
        );
        let log = ProcessLog {
            role: ComponentRole::Collector,
            instance_id: "pod/dataflow".to_owned(),
            image: None,
            image_id: None,
            source_revision: None,
            complete: true,
            raw_log_bytes: text.len() as u64,
            raw_log_lines: 3,
            stdout: text.to_owned(),
            stderr: String::new(),
            text: text.to_owned(),
        };

        let evidence = collector_shutdown_drain_evidence(
            &log,
            Some(1_700_000_000_000),
            Some("2023-11-14T22:13:21Z"),
            Some(0),
            true,
        );

        assert_eq!(evidence["status"], "observed");
        assert_eq!(evidence["quiver"]["bundles_drained"], 0);
        assert_eq!(evidence["quiver"]["status"], "inferred");
        assert_eq!(
            evidence["quiver"]["bundles_drained_source"],
            "matched_shutdown_start_complete"
        );
        assert_eq!(evidence["quiver"]["force_dropped_bundles"], 0);
        Ok(())
    }

    #[test]
    fn snapshot_stop_accepts_only_maintenance_exit_one() -> Result<()> {
        let project = "owned-maintenance-run";
        let services = [
            "delta-maintenance",
            "sqlserver",
            "arrowcol-catalog",
            "dataflow-1",
            "router",
            "oat-client",
        ];
        let before = services
            .iter()
            .enumerate()
            .map(|(index, service)| {
                json!({
                    "Id": format!("maintenance-id-{index}"),
                    "Config": {"Labels": {
                        "com.docker.compose.project": project,
                        "com.docker.compose.service": service
                    }},
                    "State": {"Running": true}
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            snapshot_stop_phases(&before)?,
            [
                vec!["maintenance-id-4".to_owned(), "maintenance-id-5".to_owned()],
                vec!["maintenance-id-0".to_owned()],
                vec!["maintenance-id-3".to_owned()],
                vec!["maintenance-id-1".to_owned(), "maintenance-id-2".to_owned()],
            ]
        );
        let mut after = before.clone();
        for container in &mut after {
            container["State"] = json!({
                "Running": false, "Paused": false, "Dead": false, "Status": "exited",
                "Error": "", "ExitCode": 0, "OOMKilled": false,
                "FinishedAt": "2026-01-01T00:00:00Z"
            });
        }

        after[0]["State"]["ExitCode"] = json!(1);
        let outcomes = snapshot_container_outcomes(&before, &after, project)?;
        assert!(outcomes[0]["maintenance_operation_failure"].is_null());
        assert!(outcomes[0]["comment"].as_str().is_some());

        for exit in [2, 137] {
            let mut invalid = after.clone();
            invalid[0]["State"]["ExitCode"] = json!(exit);
            assert!(snapshot_container_outcomes(&before, &invalid, project).is_err());
        }
        let mut invalid = after.clone();
        invalid[0]["State"]["OOMKilled"] = json!(true);
        assert!(snapshot_container_outcomes(&before, &invalid, project).is_err());
        invalid = after.clone();
        invalid[3]["State"]["ExitCode"] = json!(1);
        assert!(snapshot_container_outcomes(&before, &invalid, project).is_err());
        Ok(())
    }

    include!("runner_publication_tests.rs");

    fn router_yaml_for_pin_test(run_id: &str) -> String {
        format!(
            "routing:\n  refresh_interval_ms: 10000\n  max_snapshot_age_ms: 15000\n  max_future_skew_ms: 2000\n  max_table_entries: 256\n  max_oldest_buffer_age_ms: 86400000\n  hard_pressure_bps: 9500\n  warmup_ms: 1000\n  minimum_warmup_bps: 1000\n  lease_ttl_ms: 3600000\n  oat_run_pin_ttl_ms: 3600000\n  oat_run_id: \"{run_id}\"\n  readiness_max_staleness_ms: 30000\n  distinct_table_owners: false\n"
        )
    }

    #[test]
    fn client_sweep_expands_distinct_matrix_cells() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.campaign_profile = Some(crate::config::CampaignProfile::ScaleOutSingleTable);
        config.clients_per_table_sweep = Some(vec![1, 2, 4, 8, 16, 32, 64, 128]);
        let cells = matrix_cell_specs(&config);
        assert_eq!(
            cells.iter().map(|(_, _, clients, _)| *clients).collect::<Vec<_>>(),
            vec![1, 2, 4, 8, 16, 32, 64, 128]
        );
        Ok(())
    }

    #[test]
    fn matrix_cells_use_distinct_canonical_run_ids_for_multiple_combinations() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.example.json"
        ))?;
        let cells = matrix_cell_specs(&config);
        assert_eq!(
            cells.len(),
            config.replica_counts.len() * config.dfe_worker_concurrency.len(),
            "every matrix combination needs its own isolated run"
        );
        assert!(cells.len() > 1, "the regression must cover more than one matrix cell");
        let ids = cells.iter().map(|(_, _, _, run_id)| run_id).collect::<HashSet<_>>();
        assert_eq!(ids.len(), cells.len(), "matrix cells must not share run identities");
        for (_, _, _, run_id) in &cells {
            let parsed = uuid::Uuid::parse_str(run_id)?;
            assert_eq!(parsed.to_string(), *run_id, "run IDs use canonical lowercase UUID form");
            let local_router =
                run_scoped_local_router_config("routing:\n  lease_ttl_ms: 86400000\n", run_id, config.run_pin_ttl_ms)?;
            assert!(local_router.contains(&format!("oat_run_id: \"{run_id}\"")));
        }
        Ok(())
    }

    #[test]
    fn pin_init_config_is_secret_free_and_binds_expected_table_digest() -> Result<()> {
        let run_id = "00000000-0000-0000-0000-000000000001";
        let expected = expected_pin_keys_json(&["sqlserver_wait_stats".to_owned()])?;
        let router_yaml = router_yaml_for_pin_test(run_id);
        let config: Value = serde_json::from_str(&pin_init_config_json(
            run_id,
            &expected,
            20_000,
            10_000,
            "sqlserver",
            &router_yaml,
            None,
        )?)?;
        assert_eq!(config["schema_version"], 1);
        assert_eq!(config["run_id"], run_id);
        assert_eq!(config["expires_at_unix_ms"], 20_000);
        assert_eq!(config["sql_server"]["host"], "sqlserver");
        assert_eq!(config["sql_server"]["port"], 1433);
        assert_eq!(config["expected_keys"].as_array().map(|keys| keys.len()), Some(1));
        assert_eq!(config["expected_digest"].as_str().map(str::len), Some(64));
        assert_eq!(config["routing_policy"]["oat_run_id"], run_id);
        assert_eq!(config["routing_policy"]["distinct_table_owners"], false);
        assert_eq!(config["routing_policy"]["warmup_ms"], 1000);
        assert!(config.get("password").is_none());
        assert!(config.get("username").is_none());
        let delta_root =
            format!("abfss://tables@arrowcollector1.dfs.core.windows.net/oat-prefix/runs/{run_id}/dataflow");
        let config: Value = serde_json::from_str(&pin_init_config_json(
            run_id,
            &expected,
            20_000,
            10_000,
            "sqlserver",
            &router_yaml,
            Some((&delta_root, "telemetry.sqlserver.sqlserver_availability_replica_states")),
        )?)?;
        assert_eq!(config["run_table"]["delta_root"], delta_root);
        assert_eq!(
            config["run_table"]["table"],
            "telemetry.sqlserver.sqlserver_availability_replica_states"
        );
        Ok(())
    }

    #[test]
    fn run_table_init_receipt_requires_canonical_schema_and_table_id() -> Result<()> {
        let run_id = "00000000-0000-0000-0000-000000000001";
        let table = "telemetry.sqlserver.sqlserver_availability_replica_states";
        let schema = arrowcol_loadgen::initialize::canonical_table_delta_schema(table)?;
        let receipt = json!({
            "kind": "oat_run_table_initialized",
            "run_id": run_id,
            "table": table,
            "publisher_drained": true,
            "delta": {
                "table_id": "550e8400-e29b-41d4-a716-446655440000",
                "version": 0,
                "active_files": 0,
                "partition_columns": ["__arrowcol_ingestion"],
                "schema_json": serde_json::to_string(&schema)?,
            },
        });
        let logs = format!("{}\ninitialized OAT pin set run_id={run_id}", receipt);
        validate_run_table_init_receipt(&logs, run_id, table)?;
        assert!(validate_run_table_init_receipt("initialized OAT pin set", run_id, table).is_err());
        assert!(validate_run_table_init_receipt(&logs, "00000000-0000-0000-0000-000000000002", table).is_err());

        let mut invalid_schema = receipt.clone();
        invalid_schema["delta"]["schema_json"] = Value::String("{}".to_owned());
        let invalid_logs = format!("{}\ninitialized OAT pin set run_id={run_id}", invalid_schema);
        assert!(validate_run_table_init_receipt(&invalid_logs, run_id, table).is_err());

        let mut invalid_table_id = receipt;
        invalid_table_id["delta"]["table_id"] = Value::String("not-a-table-id".to_owned());
        let invalid_logs = format!("{}\ninitialized OAT pin set run_id={run_id}", invalid_table_id);
        assert!(validate_run_table_init_receipt(&invalid_logs, run_id, table).is_err());
        Ok(())
    }

    #[test]
    fn pin_init_config_rejects_expired_or_incomplete_pin_sets() -> Result<()> {
        let expected = expected_pin_keys_json(&["sqlserver_wait_stats".to_owned()])?;
        let run_id = "00000000-0000-0000-0000-000000000001";
        let router_yaml = router_yaml_for_pin_test(run_id);
        assert!(pin_init_config_json(run_id, &expected, 10_000, 10_000, "sqlserver", &router_yaml, None).is_err());
        assert!(pin_init_config_json(
            run_id,
            r#"{"expected_keys":[],"expected_digest":"0000000000000000000000000000000000000000000000000000000000000000"}"#,
            20_000,
            10_000,
            "sqlserver",
            &router_yaml,
            None,
        )
        .is_err());
        let mismatched_run_yaml = router_yaml_for_pin_test("00000000-0000-0000-0000-000000000002");
        assert!(
            pin_init_config_json(
                run_id,
                &expected,
                20_000,
                10_000,
                "sqlserver",
                &mismatched_run_yaml,
                None
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn pin_init_job_failure_diagnostics_include_job_and_pod_reasons() {
        let job = json!({"status":{"conditions":[{"type":"Failed","status":"True","reason":"DeadlineExceeded"}]}});
        let pods = json!({"items":[{"metadata":{"name":"oat-pin-init-pod"},"status":{"phase":"Failed","conditions":[{"type":"PodScheduled","status":"False","reason":"Unschedulable","message":"no matching nodes"}],"containerStatuses":[{"name":"pin-init","state":{"waiting":{"reason":"ImagePullBackOff"}}}]}}]});
        let details = pin_init_job_failure_diagnostics(&job, &pods);
        assert!(details.contains("DeadlineExceeded"));
        assert!(details.contains("Unschedulable"));
        assert!(details.contains("ImagePullBackOff"));
    }

    #[test]
    fn local_pin_init_compose_build_has_parse_default_and_selected_run_supplies_private_config() {
        let compose = include_str!("../../loadtest/compose.yaml");
        assert!(compose.contains("source: ${OAT_PIN_INIT_CONFIG_HOST_PATH:-/dev/null}"));
        assert!(!compose.contains("OAT_PIN_INIT_CONFIG_HOST_PATH:?"));
        assert!(compose.contains("source: ${OAT_CONTROL_HOST_PATH:-/dev/null}"));
        assert!(!compose.contains("OAT_CONTROL_HOST_PATH:?"));

        assert_eq!(
            local_control_mount_environment(Path::new("/private/oat/run"))["OAT_CONTROL_HOST_PATH"],
            "/private/oat/run/control",
            "actual OAT setup must supply its run-scoped control directory"
        );

        let expected = Path::new("/private/oat/run/pin-init.json");
        let mut command = Command::new("docker");
        set_local_pin_init_config(&mut command, expected);
        assert!(
            command
                .as_std()
                .get_envs()
                .any(|(key, value)| key == "OAT_PIN_INIT_CONFIG_HOST_PATH" && value == Some(expected.as_os_str()))
        );
    }

    #[test]
    fn checked_unix_millisecond_conversion_rejects_values_outside_supported_range() {
        let maximum = u128::from(u64::MAX);
        assert_eq!(
            checked_unix_time_ms(maximum).unwrap_or_else(|error| panic!("u64 maximum must fit: {error}")),
            u64::MAX
        );
        assert!(checked_unix_time_ms(maximum + 1).is_err());
    }

    #[tokio::test]
    async fn unresolved_qap_cleanup_preserves_runtime_directory_and_lease_sidecar() -> Result<()> {
        let directory = std::env::temp_dir().join(format!("oat-evidence-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory)?;
        let record = directory.join("oat-qap-record.json");
        let sidecar = qap_lease_sidecar_path(&record);
        let stop_record = directory.join("workload-stop.json");
        let private_manifest = directory.join("oat-qap-manifest.json");
        for path in [&record, &sidecar, &stop_record, &private_manifest] {
            std::fs::write(path, b"private test evidence")?;
        }

        let result = async {
            let unresolved =
                runtime_evidence_retention(true, None, None, true, Some(&record), Some(&stop_record), true);
            apply_runtime_evidence_retention(&directory, unresolved).await?;
            for path in [&record, &sidecar, &stop_record, &private_manifest] {
                anyhow::ensure!(
                    tokio::fs::try_exists(path).await?,
                    "unresolved outcome lost {}",
                    path.display()
                );
            }

            let completed =
                runtime_evidence_retention(false, None, None, true, Some(&record), Some(&stop_record), false);
            apply_runtime_evidence_retention(&directory, completed).await?;
            for path in [&record, &sidecar, &stop_record] {
                anyhow::ensure!(
                    tokio::fs::try_exists(path).await?,
                    "successful cleanup lost {}",
                    path.display()
                );
            }
            anyhow::ensure!(
                !tokio::fs::try_exists(&private_manifest).await?,
                "successful cleanup retained an unrelated private manifest"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let cleanup = tokio::fs::remove_dir_all(&directory).await;
        result?;
        cleanup?;
        Ok(())
    }

    #[test]
    fn startup_wait_budget_covers_early_ready_fleet_qap_delay_and_final_delivery() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.example.json"
        ))?;
        config.client_ready_timeout_seconds = 30;
        config.qap_timeout_seconds = 30;
        config.warmup_seconds = 60;
        config.measure_seconds = 60;
        let budget = config.timing_budget()?;

        let first_client_ready_at = 1_u64;
        let whole_fleet_ready_at = 29_u64;
        let qap_preparation_seconds = 29_u64;
        let final_schedule_delivery_seconds = config.schedule_lead_seconds;
        assert!(whole_fleet_ready_at <= config.client_ready_timeout_seconds);
        assert!(qap_preparation_seconds <= config.qap_timeout_seconds);
        let earliest_client_schedule_wait = whole_fleet_ready_at
            .saturating_sub(first_client_ready_at)
            .saturating_add(qap_preparation_seconds)
            .saturating_add(final_schedule_delivery_seconds);
        assert_eq!(budget.schedule_wait_seconds, 240);
        assert!(
            earliest_client_schedule_wait <= budget.schedule_wait_seconds,
            "a client that became ready early must remain alive through readiness, QAP, and final delivery"
        );
        assert_eq!(
            budget.client_job_lifetime_seconds,
            budget.schedule_wait_seconds + config.warmup_seconds + config.measure_seconds + 300
        );
        assert_eq!(
            budget.minimum_pin_ttl_ms,
            (budget.client_job_lifetime_seconds
                + crate::config::PIN_INIT_SETUP_TIMEOUT_SECONDS
                + config.reconciliation_settle_timeout_seconds
                + config.collector_stop_grace_seconds
                + config.qap_timeout_seconds)
                * 1_000,
            "the immutable pin lifetime must cover setup, the client Job, settling, collector drain, and final QAP observation"
        );
        Ok(())
    }

    #[test]
    fn qap_paths_and_external_names_are_isolated_by_run_and_keep_table_prefix() -> Result<()> {
        let run_a = "00000000-0000-0000-0000-000000000001";
        let run_b = "00000000-0000-0000-0000-000000000002";
        let shared_root = "abfss://data@account.dfs.core.windows.net/oat-prefix/dataflow";
        let root_a = run_delta_root(shared_root, run_a)?;
        let root_b = run_delta_root(shared_root, run_b)?;
        assert_eq!(
            root_a,
            format!("abfss://data@account.dfs.core.windows.net/oat-prefix/runs/{run_a}/dataflow")
        );
        assert_eq!(
            root_b,
            format!("abfss://data@account.dfs.core.windows.net/oat-prefix/runs/{run_b}/dataflow")
        );

        let relative_root_a = qap_relative_data_root(&root_a)?;
        let relative_root_b = qap_relative_data_root(&root_b)?;
        assert_eq!(relative_root_a, format!("oat-prefix/runs/{run_a}/dataflow"));
        assert_eq!(relative_root_b, format!("oat-prefix/runs/{run_b}/dataflow"));
        let table = "sqlserver_dmv_os_performance_counters";
        assert_eq!(
            aks_qap_relative_path(&relative_root_a, table)?,
            format!("{relative_root_a}/telemetry/sqlserver/{table}")
        );

        let bindings_a = qap_bindings(run_a, &[table.to_owned()])?;
        let bindings_b = qap_bindings(run_b, &[table.to_owned()])?;
        assert_eq!(bindings_a[0].logical_table, table);
        assert_eq!(bindings_b[0].logical_table, table);
        assert_eq!(
            bindings_a[0].external_table,
            format!("oat_{}_{}", uuid::Uuid::parse_str(run_a)?.simple(), table)
        );
        assert_ne!(bindings_a[0].external_table, bindings_b[0].external_table);
        Ok(())
    }

    #[test]
    fn qap_run_paths_reject_noncanonical_run_ids_and_unsafe_roots() {
        assert!(
            run_delta_root(
                "abfss://data@account.dfs.core.windows.net/oat-prefix/dataflow",
                "00000000-0000-0000-0000-000000000001/"
            )
            .is_err()
        );
        assert!(
            qap_relative_data_root("abfss://data@account.dfs.core.windows.net/oat-prefix/runs/../dataflow").is_err()
        );
        assert!(
            qap_relative_data_root("abfss://data@account.dfs.core.windows.net/oat-prefix/runs/%2F..%2F/dataflow")
                .is_err()
        );
        assert!(aks_qap_relative_path("oat-prefix/runs/run/dataflow", "../other-table").is_err());
    }

    #[test]
    fn qap_data_root_rejects_literal_and_encoded_path_normalization() -> Result<()> {
        let authority = "abfss://data@account.dfs.core.windows.net";
        for path in [
            "/oat-prefix/runs/./dataflow",
            "/oat-prefix/runs/../dataflow",
            "/oat-prefix/runs/%2e/dataflow",
            "/oat-prefix/runs/%2E%2e/dataflow",
            "/oat-prefix/runs/.%2e/dataflow",
            "/oat-prefix/runs/%2e./dataflow",
            "/oat-prefix/runs/%2f/dataflow",
            "/oat-prefix/runs\\..\\dataflow",
            "//oat-prefix/runs/dataflow",
            "/oat-prefix//runs/dataflow",
            "/oat-prefix/runs/dataflow/",
            "/oat-prefix/runs/\t../dataflow",
            "/oat-prefix/runs/\n../dataflow",
            "/oat-prefix/runs/ dataflow",
        ] {
            assert!(
                qap_relative_data_root(&format!("{authority}{path}")).is_err(),
                "unsafe literal path was accepted: {path:?}"
            );
        }
        for path in ["/oat-prefix/runs/run/dataflow", "/dataflow"] {
            assert_eq!(
                qap_relative_data_root(&format!("{authority}{path}"))?,
                path.strip_prefix('/').context("test path must be absolute")?
            );
        }
        Ok(())
    }

    #[test]
    fn local_compose_run_id_is_passed_as_compose_option_before_service() {
        let args = local_client_compose_args(
            "00000000-0000-0000-0000-000000000001",
            "oat-client-test-c1",
            &["--rows".to_owned()],
        );
        let service = args
            .iter()
            .position(|arg| arg == "oat-client")
            .unwrap_or_else(|| panic!("missing oat-client argument"));
        let env = args
            .iter()
            .position(|arg| arg == "--env")
            .unwrap_or_else(|| panic!("missing --env argument"));
        assert!(env < service);
        assert_eq!(
            args[env + 1],
            "ARROWCOL_OAT_RUN_ID=00000000-0000-0000-0000-000000000001"
        );
        let name = args
            .iter()
            .position(|arg| arg == "--name")
            .unwrap_or_else(|| panic!("missing --name argument"));
        assert!(name < service);
        assert_eq!(args[name + 1], "oat-client-test-c1");
        assert_eq!(args[service + 1], "--rows");
    }

    fn client_progress(ready: bool, schedule_received: bool) -> ClientProgress {
        ClientProgress {
            ready,
            schedule_received,
            completed: false,
            failure: None,
            output: String::new(),
        }
    }

    #[test]
    fn readiness_barrier_accepts_local_and_aks_cold_start_after_old_fixed_leads() -> Result<()> {
        let ready_fleet = [client_progress(true, false), client_progress(true, false)];
        assert!(validate_client_readiness(
            &ready_fleet,
            Duration::from_secs(61),
            Duration::from_secs(600)
        )?);
        assert!(validate_client_readiness(
            &ready_fleet,
            Duration::from_secs(16),
            Duration::from_secs(600)
        )?);
        Ok(())
    }

    #[test]
    fn readiness_barrier_rejects_timeout_and_partial_fleet() {
        let partial = [client_progress(true, false), client_progress(false, false)];
        assert!(matches!(
            validate_client_readiness(&partial, Duration::from_secs(10), Duration::from_secs(600)),
            Ok(false)
        ));
        assert!(validate_client_readiness(&partial, Duration::from_secs(600), Duration::from_secs(600)).is_err());
        let fully_ready_too_late = [client_progress(true, false), client_progress(true, false)];
        assert!(
            validate_client_readiness(
                &fully_ready_too_late,
                Duration::from_secs(600),
                Duration::from_secs(600)
            )
            .is_err()
        );
        let failed = [
            client_progress(true, false),
            ClientProgress {
                failure: Some("preflight failed".to_owned()),
                ..client_progress(false, false)
            },
        ];
        assert!(validate_client_readiness(&failed, Duration::from_secs(1), Duration::from_secs(600)).is_err());
    }

    #[test]
    fn final_armed_schedule_rejects_late_or_partial_acknowledgement() -> Result<()> {
        let start = 10_000;
        assert!(validate_client_schedule(
            &[client_progress(true, true), client_progress(true, true)],
            start - 1,
            start
        )?);
        assert!(matches!(
            validate_client_schedule(
                &[client_progress(true, true), client_progress(true, false)],
                start - 1,
                start
            ),
            Ok(false)
        ));
        assert!(
            validate_client_schedule(
                &[client_progress(true, true), client_progress(true, false)],
                start,
                start
            )
            .is_err()
        );
        assert!(
            validate_client_schedule(
                &[client_progress(true, true), client_progress(true, true)],
                start,
                start
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn parquet_schema_v2_keeps_legacy_fields_and_uses_utc_milliseconds() -> Result<()> {
        let schema = result_schema();
        let fields = schema.fields();
        let added = [
            "schema_version",
            "mode",
            "stamp",
            "host",
            "experiment",
            "started_at",
            "measure_seconds",
            "arrowcols",
            "clients",
            "tables",
            "table_owner",
            "pinning_ok",
            "status",
            "git_sha",
            "tunables_json",
        ];
        assert_eq!(
            fields
                .iter()
                .take(added.len())
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            added
        );
        assert_eq!(
            schema.field_with_name("started_at")?.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        for (name, nullable) in [
            ("stamp", true),
            ("table_owner", true),
            ("pinning_ok", false),
            ("status", false),
            ("run_id", false),
            ("table", false),
        ] {
            assert_eq!(schema.field_with_name(name)?.is_nullable(), nullable, "{name}");
        }
        assert!(schema.field_with_name("run_id").is_ok());
        assert!(schema.field_with_name("table").is_ok());
        Ok(())
    }

    #[test]
    fn aks_namespaces_are_run_unique_and_cleanup_requires_the_run_owner_token() -> Result<()> {
        let run_a = "00000000-0000-0000-0000-000000000001";
        let run_b = "00000000-0000-0000-0000-000000000002";
        let namespace_a = scoped_aks_namespace("arrowcol-oat", run_a)?;
        let namespace_b = scoped_aks_namespace("arrowcol-oat", run_b)?;
        assert_ne!(namespace_a, namespace_b);
        assert!(namespace_a.len() <= 63);
        assert!(scoped_aks_namespace("arrowcol-oat", "not-a-uuid").is_err());

        let empty_run_namespace = json!({
            "metadata": {
                "uid": "namespace-uid-a",
                "labels": {
                    "arrowcol-oat/run-id": run_a,
                    "arrowcol-oat/owner-token": "owner-a",
                },
                "annotations": {
                    "arrowcol-oat/run-id": run_a,
                    "arrowcol-oat/owner-token": "owner-a",
                },
            },
        });
        assert!(namespace_matches_run(&empty_run_namespace, run_a, "owner-a", None));
        assert!(namespace_matches_run(
            &empty_run_namespace,
            run_a,
            "owner-a",
            Some("namespace-uid-a")
        ));
        assert!(!namespace_matches_run(
            &empty_run_namespace,
            run_a,
            "owner-a",
            Some("namespace-uid-b")
        ));
        assert!(!namespace_matches_run(&empty_run_namespace, run_b, "owner-b", None));
        assert!(!namespace_matches_run(&empty_run_namespace, run_a, "owner-b", None));
        assert_eq!(
            namespace_cleanup_action(false, true),
            NamespaceCleanupAction::Delete,
            "a later prepare failure must clean the namespace created earlier in this run"
        );
        assert_eq!(
            namespace_cleanup_action(false, false),
            NamespaceCleanupAction::Ignore,
            "an ambiguous create must not delete an existing namespace with another owner token"
        );
        assert_eq!(
            namespace_uid_for_cleanup(&empty_run_namespace, None)?,
            "namespace-uid-a"
        );
        assert_eq!(
            namespace_uid_for_cleanup(&empty_run_namespace, Some("namespace-uid-a"))?,
            "namespace-uid-a"
        );
        assert!(namespace_uid_for_cleanup(&empty_run_namespace, Some("namespace-uid-b")).is_err());
        assert_eq!(
            namespace_cleanup_action(true, false),
            NamespaceCleanupAction::Refuse,
            "an ownership change after creation must fail closed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn replacement_namespace_uid_conflict_preserves_replacement() -> Result<()> {
        let observed_namespace = json!({ "metadata": { "uid": "namespace-uid-a" } });
        let observed_uid = namespace_uid_for_cleanup(&observed_namespace, None)?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            let mut buffer = [0_u8; 2048];
            let (header_end, content_length) = loop {
                let read = stream.read(&mut buffer).await?;
                if read == 0 {
                    bail!("Kubernetes API test server received an incomplete request");
                }
                request.extend_from_slice(&buffer[..read]);
                if let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .context("namespace delete request must include Content-Length")?;
                    if request.len() >= header_end + 4 + content_length {
                        break (header_end, content_length);
                    }
                }
            };
            let request_line = String::from_utf8_lossy(&request)
                .lines()
                .next()
                .context("namespace delete request must include a request line")?
                .to_owned();
            let body_start = header_end + 4;
            let options: Value = serde_json::from_slice(&request[body_start..body_start + content_length])?;
            let requested_uid = options
                .pointer("/preconditions/uid")
                .and_then(Value::as_str)
                .context("namespace delete must include a UID precondition")?
                .to_owned();
            let replacement_uid = "namespace-uid-b".to_owned();
            let response_body = r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Conflict","message":"UID precondition does not match replacement namespace","code":409}"#;
            let response = format!(
                "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream.write_all(response.as_bytes()).await?;
            Ok::<_, anyhow::Error>((request_line, requested_uid, replacement_uid))
        });

        let result =
            delete_namespace_with_uid_precondition(&format!("http://{address}"), "arrowcol-oat-run", &observed_uid)
                .await;
        let (request_line, requested_uid, surviving_uid) = server.await??;
        assert_eq!(observed_uid, "namespace-uid-a");
        assert_eq!(request_line, "DELETE /api/v1/namespaces/arrowcol-oat-run HTTP/1.1");
        assert_eq!(requested_uid, "namespace-uid-a");
        assert_eq!(surviving_uid, "namespace-uid-b");
        let error = result
            .context("a UID conflict must fail closed without a success receipt")
            .err();
        let error = error.context("replacement namespace deletion unexpectedly succeeded")?;
        assert!(format!("{error:#}").contains("409 Conflict"));
        Ok(())
    }

    #[test]
    fn local_router_configuration_authorizes_only_its_run() -> Result<()> {
        let generated =
            run_scoped_local_router_config(include_str!("../../loadtest/config/router.yaml"), "run-one", 4_080_000)?;
        let generated: serde_yaml::Value = serde_yaml::from_str(&generated)?;
        assert_eq!(generated["auth"]["authenticator"].as_str(), Some("dev_only_allow_all"));
        assert_eq!(generated["routing"]["oat_run_id"].as_str(), Some("run-one"));
        assert_eq!(generated["routing"]["lease_ttl_ms"].as_u64(), Some(4_080_000));
        assert_eq!(generated["routing"]["oat_run_pin_ttl_ms"].as_u64(), Some(4_080_000));

        let configured = run_scoped_local_router_config(
            "routing:\n  lease_ttl_ms: 86400000\n  readiness_max_staleness_ms: 30000\n",
            "run-one",
            3_600_000,
        )?;
        assert!(
            configured.contains("  lease_ttl_ms: 3600000\n  oat_run_pin_ttl_ms: 3600000\n  oat_run_id: \"run-one\"\n")
        );
        assert!(run_scoped_local_router_config("routing:\n  lease_ttl_ms: 30000\n", "run-two", 3_600_000).is_err());
        assert!(
            run_scoped_local_router_config(
                "routing:\n  lease_ttl_ms: 86400000\n  oat_run_id: run-existing\n",
                "run-one",
                3_600_000,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn aks_router_pin_ttl_is_injected_without_changing_run_authorization() -> Result<()> {
        let mut resources = vec![json!({
            "kind":"ConfigMap",
            "metadata":{"name":"arrowcol-config"},
            "data":{"router.yaml":"routing:\n  oat_run_id: run-one\n"}
        })];
        inject_router_oat_pin_ttl(&mut resources, 4_080_000)?;
        let yaml = resources[0]
            .pointer("/data/router.yaml")
            .and_then(Value::as_str)
            .context("router yaml missing")?;
        let router: serde_yaml::Value = serde_yaml::from_str(yaml)?;
        assert_eq!(router["routing"]["oat_run_id"].as_str(), Some("run-one"));
        assert_eq!(router["routing"]["lease_ttl_ms"].as_u64(), Some(4_080_000));
        assert_eq!(router["routing"]["oat_run_pin_ttl_ms"].as_u64(), Some(4_080_000));
        Ok(())
    }

    #[test]
    fn parquet_schema_v3_preserves_v2_and_appends_nullable_evidence() -> Result<()> {
        let expected = vec![
            ("schema_version", DataType::Int32, false),
            ("mode", DataType::Utf8, false),
            ("stamp", DataType::Utf8, true),
            ("host", DataType::Utf8, false),
            ("experiment", DataType::Utf8, false),
            (
                "started_at",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            ("measure_seconds", DataType::Float64, false),
            ("arrowcols", DataType::Int32, false),
            ("clients", DataType::Int32, false),
            ("tables", DataType::Int32, false),
            ("table_owner", DataType::Utf8, true),
            ("pinning_ok", DataType::Boolean, false),
            ("status", DataType::Utf8, false),
            ("git_sha", DataType::Utf8, false),
            ("tunables_json", DataType::Utf8, false),
            ("run_id", DataType::Utf8, false),
            ("replicas", DataType::UInt64, false),
            ("dfe_worker_concurrency", DataType::UInt64, false),
            ("table", DataType::Utf8, false),
            ("rows", DataType::UInt64, true),
            ("duration_ms", DataType::UInt64, true),
            ("rows_per_second", DataType::Float64, true),
            ("cpu_millicores", DataType::Float64, true),
            ("memory_bytes", DataType::UInt64, true),
            ("qap_observation_complete", DataType::UInt64, false),
            ("qap_observed_tables", DataType::UInt64, false),
            ("qap_completion_percent", DataType::Float64, true),
            ("qap_pending_artifacts", DataType::UInt64, true),
            ("qap_cached_bytes", DataType::UInt64, true),
            ("qap_unhealthy_catalogs", DataType::UInt64, true),
            ("qap_lag_seconds", DataType::Float64, true),
            ("evidence_json", DataType::Utf8, true),
        ];
        let schema = result_schema();
        let actual = schema
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.data_type().clone(), field.is_nullable()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        Ok(())
    }
    #[test]
    fn pinning_guard_accepts_stable_single_owner_and_rejects_loss_change_or_duplicates() {
        let tables = vec!["sqlserver_wait_stats".to_owned()];
        let mut owners = HashMap::new();
        let one = HashMap::from([("sqlserver_wait_stats".to_owned(), vec!["collector-a".to_owned()])]);
        assert_eq!(check_owner_sample(&tables, &HashMap::new(), &mut owners), None);
        assert_eq!(check_owner_sample(&tables, &one, &mut owners), None);
        assert_eq!(
            owners.get("sqlserver_wait_stats").map(String::as_str),
            Some("collector-a")
        );
        assert!(check_owner_sample(&tables, &HashMap::new(), &mut owners).is_some());

        let mut owners = HashMap::new();
        assert_eq!(check_owner_sample(&tables, &one, &mut owners), None);
        let changed = HashMap::from([("sqlserver_wait_stats".to_owned(), vec!["collector-b".to_owned()])]);
        assert!(check_owner_sample(&tables, &changed, &mut owners).is_some());

        let duplicate = HashMap::from([(
            "sqlserver_wait_stats".to_owned(),
            vec!["collector-a".to_owned(), "collector-b".to_owned()],
        )]);
        assert!(check_owner_sample(&tables, &duplicate, &mut HashMap::new()).is_some());
        assert!(check_owner_sample(&tables, &HashMap::new(), &mut HashMap::new()).is_none());
    }

    #[test]
    fn owner_lookup_uses_the_same_canonical_table_key_as_metrics() -> Result<()> {
        let configured_tables = vec!["SQLSERVER_WAIT_STATS".to_owned()];
        let owners = parse_owner_metrics(
            "arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"c\",schema=\"s\",table=\"sqlserver_wait_stats\",collector=\"collector-a\",state=\"active\"} 1\n",
            &configured_tables,
        )?;
        let mut stable = HashMap::new();
        assert_eq!(check_owner_sample(&configured_tables, &owners, &mut stable), None);
        assert_eq!(
            stable
                .get(&canonical_table_key(&configured_tables[0]))
                .map(String::as_str),
            Some("collector-a")
        );
        Ok(())
    }

    #[test]
    fn aks_overlay_only_allows_configured_deployments_in_target_namespace() -> Result<()> {
        let allowed = HashSet::from(["arrowcol".to_owned(), "arrowcol-dataflow".to_owned()]);
        let arrowcol = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "arrowcol", "namespace": "arrowcol"},
        });
        let dfe_without_namespace = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "arrowcol-dataflow"},
        });
        validate_aks_overlay_resources(&[arrowcol.clone(), dfe_without_namespace], &allowed, "arrowcol")?;
        assert!(validate_aks_overlay_resources(&[], &allowed, "arrowcol").is_err());
        assert!(
            validate_aks_overlay_resources(
                &[json!({"kind": "ConfigMap", "metadata": {"name": "settings"}})],
                &allowed,
                "arrowcol",
            )
            .is_err()
        );
        assert!(
            validate_aks_overlay_resources(
                &[json!({"kind": "Deployment", "metadata": {"name": "unconfigured"}})],
                &allowed,
                "arrowcol",
            )
            .is_err()
        );
        assert!(
            validate_aks_overlay_resources(
                &[json!({"kind": "Deployment", "metadata": {"name": "arrowcol", "namespace": "other"}})],
                &allowed,
                "arrowcol",
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn owner_metric_parser_ignores_other_tables_and_draining_leases() -> Result<()> {
        let tables = vec!["sqlserver_wait_stats".to_owned()];
        let owners = parse_owner_metrics(
            "arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"c\",schema=\"s\",table=\"sqlserver_wait_stats\",collector=\"collector-a\",state=\"active\"} 1\n\
             arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"c\",schema=\"s\",table=\"sqlserver_wait_stats\",collector=\"collector-old\",state=\"draining\"} 1\n\
             arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"c\",schema=\"s\",table=\"another_table\",collector=\"collector-b\",state=\"active\"} 1\n",
            &tables,
        )?;
        assert_eq!(
            owners.get("sqlserver_wait_stats"),
            Some(&vec!["collector-a".to_owned()])
        );
        Ok(())
    }

    #[test]
    fn owner_metric_parser_handles_escaped_commas_in_collector_labels() -> Result<()> {
        let tables = vec!["sqlserver_wait_stats".to_owned()];
        let owners = parse_owner_metrics(
            "arrowcol_router_table_owners{pool=\"dataflow\",catalog=\"c\",schema=\"s\",table=\"sqlserver_wait_stats\",collector=\"collector,west\",state=\"active\"} 1\n",
            &tables,
        )?;
        assert_eq!(
            owners.get("sqlserver_wait_stats"),
            Some(&vec!["collector,west".to_owned()])
        );
        Ok(())
    }

    #[test]
    fn parses_only_complete_positive_client_throughput_reports() -> Result<()> {
        let report = parse_client_metrics(
            "uploaded 1 DMV tables with 100 rows each using concurrency 4\nmeasured_rows=12000 measured_seconds=60.250 rows_per_second=199.17 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=160250 measurement_started_unix_ms=100200\n",
        )?;
        assert_eq!(report.rows, 12_000);
        assert_eq!(report.duration_seconds, 60.25);
        assert_eq!(report.rows_per_second, 199.17);
        assert_eq!(report.measurement_window_start_unix_ms, 100_000);
        assert_eq!(report.measurement_window_end_unix_ms, 160_250);
        assert_eq!(report.measurement_started_unix_ms, 100_200);
        assert!(parse_client_metrics("measured_rows=1 measured_seconds=0 rows_per_second=0").is_err());
        assert!(parse_client_metrics(
            "measured_rows=1 measured_seconds=60 rows_per_second=1 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=160000"
        )
        .is_err());
        assert!(parse_client_metrics("measured_rows=1").is_err());
        Ok(())
    }

    #[test]
    fn parses_complete_pilot_windows_and_finds_the_earliest_stable_horizon() -> Result<()> {
        let report = parse_client_metrics(
            "measured_rows=9100 measured_seconds=90 rows_per_second=101.11 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=190000 measurement_started_unix_ms=100000\n\
             pilot_window=1 window_seconds=30 rows=3000 rows_per_second=100.000 window_start_unix_ms=100000\n\
             pilot_window=2 window_seconds=30 rows=3090 rows_per_second=103.000 window_start_unix_ms=130000\n\
             pilot_window=3 window_seconds=30 rows=3030 rows_per_second=101.000 window_start_unix_ms=160000\n",
        )?;
        assert_eq!(report.pilot_windows.len(), 3);
        assert_eq!(report.pilot_windows[1].index, 1);
        assert_eq!(report.pilot_windows[1].rows, 3090);
        assert_eq!(report.pilot_windows[1].start_unix_ms, 130_000);
        assert_eq!(stable_window_horizon(&[100.0, 103.0, 101.0]), Some(60));
        assert_eq!(stable_window_horizon(&[100.0, 100.0, 10.0, 10.0]), None);
        assert!(parse_client_metrics("measured_rows=1 measured_seconds=60 rows_per_second=0.01").is_err());
        Ok(())
    }

    #[test]
    fn rejects_staggered_clients_and_misaligned_pilot_windows() -> Result<()> {
        let report = parse_client_metrics(
            "measured_rows=100 measured_seconds=60 rows_per_second=1.67 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=160000 measurement_started_unix_ms=100251\n\
             pilot_window=1 window_seconds=30 rows=50 rows_per_second=1.67 window_start_unix_ms=100000\n\
             pilot_window=2 window_seconds=30 rows=50 rows_per_second=1.67 window_start_unix_ms=130000\n",
        )?;
        assert!(validate_client_measurement_interval(&report, 100_000, 160_000, true).is_err());
        let aligned = parse_client_metrics(
            "measured_rows=100 measured_seconds=60 rows_per_second=1.67 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=160000 measurement_started_unix_ms=100200\n\
             pilot_window=1 window_seconds=30 rows=50 rows_per_second=1.67 window_start_unix_ms=100000\n\
             pilot_window=2 window_seconds=30 rows=50 rows_per_second=1.67 window_start_unix_ms=130000\n",
        )?;
        assert!(validate_client_measurement_interval(&aligned, 100_000, 160_000, true).is_ok());
        Ok(())
    }

    #[test]
    fn integrated_patch_preserves_replicas_config_and_replaces_all_core_arguments() -> Result<()> {
        let mut deployment = json!({"spec":{"replicas":2,"template":{"spec":{"containers":[{
            "name":"dataflow","args":["--config","/etc/oat/dataflow.yaml","--num-cores=1","--num-cores","3"]
        }]}}}});
        configure_workers(&mut deployment, "dataflow", 2)?;
        assert_eq!(deployment["spec"]["replicas"], 2);
        assert_eq!(
            deployment.pointer("/spec/template/spec/containers/0/args"),
            Some(&json!(["--config", "/etc/oat/dataflow.yaml", "--num-cores", "2"]))
        );
        deployment["spec"]["template"]["spec"]["containers"][0]["args"] = json!(["--num-cores"]);
        assert!(configure_workers(&mut deployment, "dataflow", 2).is_err());
        Ok(())
    }

    #[test]
    fn overlapping_metrics_selectors_count_each_pod_once() -> Result<()> {
        let mut pods = parse_top_metrics("arrowcol-a 100m 2Mi\narrowcol-b 200m 3Mi\n")?;
        pods.extend(parse_top_metrics("arrowcol-a 100m 2Mi\n")?);
        let total = sum_metrics(&pods);
        assert_eq!(total.cpu_millicores, 300.0);
        assert_eq!(total.memory_bytes, 5 * 1024 * 1024);
        Ok(())
    }

    #[test]
    fn metrics_require_every_current_pod_and_reject_stale_or_unknown_samples() -> Result<()> {
        let expected = active_pod_names(&json!({"items": [
            {"metadata": {"name": "arrowcol-a"}},
            {"metadata": {"name": "arrowcol-b"}},
            {"metadata": {"name": "arrowcol-old", "deletionTimestamp": "2026-11-08T00:00:00Z"}}
        ]}))?;
        assert_eq!(expected.len(), 2);
        assert!(!metrics_complete(
            &expected,
            &parse_top_metrics("arrowcol-a 100m 2Mi\n")?
        ));
        assert!(!metrics_complete(
            &expected,
            &parse_top_metrics("arrowcol-a 100m 2Mi\narrowcol-b <unknown> <unknown>\n")?
        ));
        assert!(!metrics_complete(
            &expected,
            &parse_top_metrics("arrowcol-a 100m 2Mi\narrowcol-old 100m 2Mi\n")?
        ));
        assert!(metrics_complete(
            &expected,
            &parse_top_metrics("arrowcol-a 100m 2Mi\narrowcol-b 200m 3Mi\n")?
        ));
        assert!(active_pod_names(&json!({"items": []})).is_err());
        Ok(())
    }

    #[test]
    fn parses_kubernetes_cpu_and_memory_units() -> Result<()> {
        assert_eq!(parse_cpu_millicores("250m")?, 250.0);
        assert_eq!(parse_cpu_millicores("1.5")?, 1500.0);
        assert_eq!(parse_memory_bytes("2Mi")?, 2 * 1024 * 1024);
        assert_eq!(parse_memory_bytes("512")?, 512);
        Ok(())
    }

    #[test]
    fn aggregates_qap_metrics_only_when_all_expected_tables_are_measured() -> Result<()> {
        assert_eq!(parse_qap_timestamp("1970-01-01T00:00:00Z"), Some(0));
        let names = ["dataflow_sqlserver_wait_stats", "dataflow_sqlserver_cpu_utilization"].map(str::to_owned);
        let observation = serde_json::json!({
            "observed_unix_ms": 10_000,
            "expected_tables": names,
            "tables": {
                "dataflow_sqlserver_wait_stats": {
                    "available": true,
                    "statistics": {
                        "AccelerationCompletePercentage": 70.0,
                        "AccelerationPendingDataFilesCount": 2,
                        "HotSize": 10,
                        "LastUpdatedDateTime": "1970-01-01 00:00:00.0000000"
                    },
                    "state": {"IsHealthy": true}
                },
                "dataflow_sqlserver_cpu_utilization": {
                    "available": true,
                    "statistics": {
                        "AccelerationCompletePercentage": 50.0,
                        "AccelerationPendingDataFilesCount": 3,
                        "HotSize": 20,
                        "LastUpdatedDateTime": "1970-01-01 00:00:00.0000000"
                    },
                    "state": {"IsHealthy": false}
                }
            }
        });
        let metrics = aggregate_qap(&observation, &names)?;
        assert_eq!(metrics.observation_complete, 1);
        assert_eq!(metrics.observed_tables, 2);
        assert_eq!(metrics.completion_percent, Some(50.0));
        assert_eq!(metrics.pending_artifacts, Some(5));
        assert_eq!(metrics.cached_bytes, Some(30));
        assert_eq!(metrics.unhealthy_catalogs, Some(1));
        assert_eq!(metrics.lag_seconds, Some(10.0));

        let partial = serde_json::json!({
            "observed_unix_ms": 10_000,
            "expected_tables": names,
            "tables": {
                "dataflow_sqlserver_wait_stats": {"available": false},
                "dataflow_sqlserver_cpu_utilization": {"available": false}
            }
        });
        let metrics = aggregate_qap(&partial, &names)?;
        assert_eq!(metrics.observation_complete, 0);
        assert_eq!(metrics.observed_tables, 0);
        assert_eq!(metrics.completion_percent, None);
        Ok(())
    }

    #[test]
    fn local_qap_deadline_keeps_progress_and_exact_snapshot_coverage_without_changing_delta_results() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let run = "fdba69d6-ae99-49e6-9c7a-f1a1044b8241";
        let table = config.tables[0].clone();
        let external = qap_bindings(run, std::slice::from_ref(&table))?
            .remove(0)
            .external_table;
        let observation = json!({
            "run_id": run, "observed_unix_ms": 1_794_842_000_000_u64,
            "expected_tables": [external], "catch_up_status": "deadline_incomplete",
            "tables": {&external: {"available": true, "row_count": 3_915_776, "table_version": 585,
                "completion_percent": 84.81, "pending_files": 89}},
        });
        let mut evidence = CellEvidence::default();
        evidence
            .durable_commits
            .insert(table.clone(), json!({"output_throughput":{"rows_per_second":123.0}}));
        let measured = QapMetrics {
            observation_complete: 1,
            observed_tables: 1,
            completion_percent: Some(84.81),
            pending_artifacts: Some(89),
            cached_bytes: Some(1_121_271_383),
            unhealthy_catalogs: Some(0),
            lag_seconds: Some(1.0),
        };
        let qap = apply_qap_observation(
            crate::config::OatMode::Local,
            measured,
            observation.clone(),
            &mut evidence,
        );
        assert!(qap.completion_percent.is_none() && qap.pending_artifacts.is_none() && qap.cached_bytes.is_none());
        assert!(qap.unhealthy_catalogs.is_none() && qap.lag_seconds.is_none());
        let comment = evidence.downstream_comment.as_deref().context("deadline explanation")?;
        assert!(comment.contains("84.81%") && comment.contains("89"));
        assert_eq!(evidence.qap_observation.as_ref(), Some(&observation));
        let aks = apply_qap_observation(
            crate::config::OatMode::Aks,
            measured,
            observation.clone(),
            &mut CellEvidence::default(),
        );
        assert_eq!(aks.completion_percent, Some(84.81), "AKS verdict remains strict");
        let mut snapshots = json!({&table: {
            "source": "verified_stopped_snapshot_manifest", "run_id": run, "table": table,
            "manifest": format!("{run}/{table}/manifest.json"), "manifest_sha256": "a".repeat(64),
            "table_version": 585, "row_count": 3_915_776,
        }});
        let coverage = kusto_snapshot_coverage(run, &table, &observation, &snapshots)?;
        assert_eq!(coverage["status"], "matched");
        assert_eq!(coverage["unique_payload_delivery"], "unknown");
        assert_eq!(coverage["queryable_delivery"], "unknown");
        assert!(coverage["delivery_latency"].is_null());
        snapshots[&table]["table_version"] = json!(586);
        assert_eq!(
            kusto_snapshot_coverage(run, &table, &observation, &snapshots)?["status"],
            "mismatch"
        );
        snapshots[&table]["run_id"] = json!(uuid::Uuid::new_v4().to_string());
        assert_eq!(
            kusto_snapshot_coverage(run, &table, &observation, &snapshots)?["status"],
            "unknown"
        );
        let mut pinning = PinningReport::default();
        pinning
            .owners
            .insert(canonical_table_key(&table), "collector".to_owned());
        let clients = ClientRun {
            metrics: HashMap::from([(
                table.clone(),
                ClientMetrics {
                    rows: 100,
                    duration_ms: 1000,
                    rows_per_second: 100.0,
                    completions: Vec::new(),
                },
            )]),
            pinning,
            qap_tables: vec![external],
            failure: None,
            evidence,
        };
        let rows = result_rows(
            &config,
            "orchestration",
            run,
            1,
            1,
            CellMeasurements {
                clients,
                started_at_ms: 1,
                qap,
                resources: None,
                resource_evidence: None,
            },
        )?;
        assert_eq!(rows[0].status, "succeeded");
        let result: Value = serde_json::from_str(rows[0].evidence_json.as_deref().context("evidence")?)?;
        assert_eq!(result["durable_commits"]["output_throughput"]["rows_per_second"], 123.0);
        assert_eq!(result["downstream_status"], "unavailable");
        Ok(())
    }

    #[test]
    fn exploratory_delta_mismatch_fails_result_and_excludes_ranking() -> Result<()> {
        let config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.aks.max-rows.json"
        ))?;
        let run = "fdba69d6-ae99-49e6-9c7a-f1a1044b8241";
        let table = config.tables[0].clone();
        let mut pinning = PinningReport::default();
        pinning
            .owners
            .insert(canonical_table_key(&table), "collector".to_owned());
        let mut evidence = CellEvidence::default();
        evidence.durable_commits.insert(
            table.clone(),
            json!({
                "run_cumulative_output": {"rows": 5, "commits": 1, "files": 1, "parquet_bytes": 500, "settled": true},
                "delta_log_reconciliation": {
                    "evidence_class": "d53_exploratory",
                    "status": "mismatch",
                    "delta_log_committed_rows": 5,
                    "collector_event_rows": 5,
                    "client_acknowledged_rows": 6,
                    "delta_vs_acknowledged_rows": -1,
                    "per_version": [{"delta_version": 0, "matched": true}],
                },
                "output_throughput": {
                    "rows_per_second": 5.0,
                    "input_bytes_per_second": 1_000_000_000.0,
                    "parquet_bytes_per_second": 500.0,
                    "compression_ratio": 2.0,
                    "commits": 1,
                    "window_seconds": 1.0,
                    "parquet_bytes": 500,
                    "files": 1,
                    "measurement_start_unix_ms": 1,
                    "measurement_end_unix_ms": 1001,
                },
            }),
        );
        let clients = ClientRun {
            metrics: HashMap::from([(
                table.clone(),
                ClientMetrics {
                    rows: 6,
                    duration_ms: 1000,
                    rows_per_second: 6.0,
                    completions: Vec::new(),
                },
            )]),
            pinning,
            qap_tables: Vec::new(),
            failure: None,
            evidence,
        };
        let rows = result_rows(
            &config,
            "orchestration",
            run,
            1,
            1,
            CellMeasurements {
                clients,
                started_at_ms: 1,
                qap: QapMetrics::default(),
                resources: None,
                resource_evidence: None,
            },
        )?;
        assert_eq!(rows[0].status, "failed");
        assert!(
            rows[0]
                .failure
                .as_deref()
                .is_some_and(|failure| failure.contains("exploratory_delta_log_reconciliation_mismatch"))
        );
        assert_eq!(rows[0].rows, Some(6));
        assert!(rows[0].rows_per_second.is_none());
        let result: Value = serde_json::from_str(rows[0].evidence_json.as_deref().context("result evidence")?)?;
        assert_eq!(result["exploratory_delta_log_reconciliation"]["status"], "mismatch");
        assert_eq!(
            result["exploratory_delta_log_reconciliation"]["delta_log_committed_rows"],
            5
        );
        assert_eq!(
            result["exploratory_delta_log_reconciliation"]["client_acknowledged_rows"],
            6
        );
        assert!(result["exploratory_delta_log_reconciliation"]["per_version"].is_array());
        assert!(result["ranking_metrics"].is_null());
        assert_eq!(result["ranking_eligible"], false);
        let outcome = finish_matrix_cell(&config, "orchestration", run, 1, 1, Ok(rows), Ok(()));
        assert!(outcome.error.is_some());
        assert_eq!(outcome.rows[0].status, "failed");
        Ok(())
    }

    #[test]
    fn client_completion_keeps_actual_admission_kinds_and_rejects_bad_evidence() -> Result<()> {
        let metrics = "measured_rows=100 measured_seconds=60 rows_per_second=1.667 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=160000 measurement_started_unix_ms=100000";
        let completion = r#"OAT_COLLECTOR_COMPLETION {"best_effort_streams":3,"durable_streams":2,"admission_accounting_complete":false,"completed_at_unix_ms":160001}"#;
        let report = parse_client_metrics(&format!("{metrics}\n{completion}"))?;
        let evidence = report.collector_completion.context("completion evidence missing")?;
        assert_eq!((evidence.best_effort_streams, evidence.durable_streams), (3, 2));
        assert!(!evidence.admission_accounting_complete);
        assert!(
            parse_client_metrics(&format!("{metrics}\n{}", completion.replace("false", "true")))?
                .collector_completion
                .context("complete accounting evidence missing")?
                .admission_accounting_complete
        );
        assert!(parse_client_metrics(metrics)?.collector_completion.is_none());
        assert!(parse_client_metrics(&format!("{metrics}\n{completion}\n{completion}")).is_err());
        assert!(parse_client_metrics(&format!("{metrics}\n{}", completion.replace("160001", "159999"))).is_err());
        Ok(())
    }

    #[test]
    fn rejects_reported_duration_shorter_than_the_shared_window() -> Result<()> {
        let report = parse_client_metrics(
            "measured_rows=100 measured_seconds=1 rows_per_second=100 measurement_window_start_unix_ms=100000 measurement_window_end_unix_ms=160000 measurement_started_unix_ms=100000",
        )?;
        assert!(validate_client_measurement_interval(&report, 100_000, 160_000, false).is_err());
        let aligned = ClientReport {
            duration_seconds: 60.0,
            ..report
        };
        assert!(validate_client_measurement_interval(&aligned, 100_000, 160_000, false).is_ok());
        Ok(())
    }

    #[test]
    fn initial_parquet_write_deduplicates_run_table_keys() -> Result<()> {
        let batch = parquet_batch(&[
            sample_result("run-1", "table-a", 100),
            sample_result("run-1", "table-a", 200),
        ])?;
        let bytes = encode_parquet(&batch)?;
        let batches = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?
            .build()?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let rows = batches[0]
            .column_by_name("rows")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .context("missing UInt64 rows")?;
        assert_eq!(rows.value(0), 200);
        Ok(())
    }

    #[test]
    fn zero_row_parquet_preserves_schema_and_accepts_a_later_append() -> Result<()> {
        let empty = encode_parquet(&parquet_batch(&[])?)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&empty))?;
        assert_eq!(reader.schema().as_ref(), result_schema().as_ref());
        assert_eq!(reader.metadata().file_metadata().num_rows(), 0);
        assert_eq!(reader.build()?.count(), 0);
        let appended = merge_parquet_bytes(&empty, &parquet_batch(&[sample_result("run-1", "table-a", 100)])?)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(appended))?;
        assert_eq!(reader.metadata().file_metadata().num_rows(), 1);
        Ok(())
    }

    #[test]
    fn writes_one_compact_parquet_file_with_viewer_schema() -> Result<()> {
        let results = vec![sample_result("run-1", "sqlserver_wait_stats", 100)];
        let bytes = encode_parquet(&parquet_batch(&results)?)?;
        assert!(bytes.starts_with(b"PAR1"));
        assert!(bytes.ends_with(b"PAR1"));
        Ok(())
    }

    #[test]
    fn parquet_merges_are_idempotent_by_run_and_table() -> Result<()> {
        let current = parquet_batch(&[
            sample_result("run-1", "sqlserver_wait_stats", 100),
            sample_result("run-1", "sqlserver_cpu_utilization", 200),
        ])?;
        let retry = parquet_batch(&[
            sample_result("run-1", "sqlserver_wait_stats", 150),
            sample_result("run-2", "sqlserver_wait_stats", 300),
        ])?;
        let bytes = merge_parquet_bytes(&encode_parquet(&current)?, &retry)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
        let batches = reader.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        let rows = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("rows")
                    .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                    .unwrap_or_else(|| panic!("schema must contain UInt64 rows"))
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![200, 150, 300]);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_local_writer_processes_preserve_both_distinct_rows() -> Result<()> {
        let directory = std::env::temp_dir().join(format!("oat-parquet-lock-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory)?;
        let target = directory.join("results.parquet");
        let gate = directory.join("start-writers");
        let executable = std::env::current_exe()?;
        let mut writers = Vec::new();
        for (run_id, table) in [("writer-a", "table-a"), ("writer-b", "table-b")] {
            writers.push(
                std::process::Command::new(&executable)
                    .args(["--exact", "runner::tests::local_file_writer_child"])
                    .env("OAT_FILE_WRITER_TARGET", &target)
                    .env("OAT_FILE_WRITER_GATE", &gate)
                    .env("OAT_FILE_WRITER_RUN_ID", run_id)
                    .env("OAT_FILE_WRITER_TABLE", table)
                    .spawn()?,
            );
        }
        std::fs::write(&gate, b"go")?;
        for mut writer in writers {
            let status = writer.wait()?;
            assert!(status.success(), "local writer child exited with {status}");
        }
        let bytes = std::fs::read(&target)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
        let batches = reader.build()?.collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let rows = batches
            .iter()
            .flat_map(|batch| {
                let run_id = batch
                    .column_by_name("run_id")
                    .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                    .unwrap_or_else(|| panic!("schema must contain UTF-8 run_id"));
                let table = batch
                    .column_by_name("table")
                    .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                    .unwrap_or_else(|| panic!("schema must contain UTF-8 table"));
                (0..batch.num_rows())
                    .map(|index| (run_id.value(index).to_owned(), table.value(index).to_owned()))
                    .collect::<Vec<_>>()
            })
            .collect::<HashSet<_>>();
        assert_eq!(
            rows,
            HashSet::from([
                ("writer-a".to_owned(), "table-a".to_owned()),
                ("writer-b".to_owned(), "table-b".to_owned()),
            ])
        );
        std::fs::remove_file(&target)?;
        std::fs::remove_file(directory.join("results.parquet.lock"))?;
        std::fs::remove_file(&gate)?;
        std::fs::remove_dir(&directory)?;
        Ok(())
    }

    #[tokio::test]
    async fn local_file_writer_child() -> Result<()> {
        let (Some(target), Some(gate), Some(run_id), Some(table)) = (
            std::env::var_os("OAT_FILE_WRITER_TARGET"),
            std::env::var_os("OAT_FILE_WRITER_GATE"),
            std::env::var("OAT_FILE_WRITER_RUN_ID").ok(),
            std::env::var("OAT_FILE_WRITER_TABLE").ok(),
        ) else {
            return Ok(());
        };
        let gate = PathBuf::from(gate);
        for _ in 0..200 {
            if gate.is_file() {
                let incoming = parquet_batch(&[sample_result(&run_id, &table, 100)])?;
                return merge_local_results(&PathBuf::from(target), &incoming).await;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        bail!("timed out waiting for concurrent writer test gate")
    }

    #[tokio::test]
    async fn stale_authoritative_snapshot_cannot_regress_a_successful_local_mirror() -> Result<()> {
        let directory = std::env::temp_dir().join(format!("oat-parquet-mirror-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory)?;
        let authoritative = directory.join("authoritative.parquet");
        let mirror = directory.join("website.parquet");
        let old_snapshot = b"authoritative-snapshot-S1";
        let newer_snapshot = b"authoritative-and-mirror-S2";
        std::fs::write(&authoritative, newer_snapshot)?;
        std::fs::write(&mirror, newer_snapshot)?;
        let authoritative_url = Url::from_file_path(&authoritative)
            .map_err(|()| anyhow::anyhow!("could not create authoritative file URL"))?;
        let mirror_url =
            Url::from_file_path(&mirror).map_err(|()| anyhow::anyhow!("could not create mirror file URL"))?;
        let stale = DestinationSnapshot {
            bytes: old_snapshot.to_vec(),
            version: None,
        };

        let copied = write_exact_destination(&mirror_url, &authoritative_url, &stale).await?;
        assert!(!copied);
        assert_eq!(std::fs::read(&mirror)?, newer_snapshot);

        std::fs::remove_file(authoritative)?;
        std::fs::remove_file(mirror)?;
        std::fs::remove_file(directory.join("website.parquet.lock"))?;
        std::fs::remove_dir(directory)?;
        Ok(())
    }

    fn sample_result(run_id: &str, table: &str, rows: u64) -> OatResult {
        OatResult {
            run_id: run_id.to_owned(),
            mode: "local".to_owned(),
            stamp: None,
            host: "local".to_owned(),
            experiment: "arrowcols-1-dfe-2".to_owned(),
            started_at_ms: 1_794_173_400_000,
            measure_seconds: 60.0,
            arrowcols: 1,
            clients: 1,
            tables: 1,
            table_owner: Some("collector-a".to_owned()),
            pinning_ok: true,
            pinning_error: None,
            failure: None,
            status: "succeeded".to_owned(),
            git_sha: "test".to_owned(),
            tunables_json: "{}".to_owned(),
            evidence_json: None,
            replicas: 1,
            dfe_worker_concurrency: 2,
            table: table.to_owned(),
            rows: Some(rows),
            duration_ms: Some(10),
            rows_per_second: Some(rows as f64 / 60.0),
            cpu_millicores: Some(50.0),
            memory_bytes: Some(1024),
            qap: QapMetrics::default(),
        }
    }
}

#[cfg(test)]
mod aks_resource_readback_tests {
    use super::*;

    fn requirements() -> ResourceRequirements {
        ResourceRequirements {
            requests: ResourceValues {
                cpu: Some("2".to_owned()),
                memory: "4Gi".to_owned(),
            },
            limits: ResourceValues {
                cpu: Some("2".to_owned()),
                memory: "6Gi".to_owned(),
            },
        }
    }

    #[test]
    fn resource_readback_accepts_equivalent_kubernetes_quantity_spellings() -> Result<()> {
        let containers = vec![json!({
            "name": "dataflow",
            "resources": {
                "requests": {"cpu": "2000m", "memory": "4096Mi"},
                "limits": {"cpu": "2", "memory": "6Gi"}
            }
        })];
        let evidence = verify_container_resources(&containers, "dataflow", "collector", &requirements())?;
        assert_eq!(evidence["requested"]["requests"]["cpu"], "2");
        assert_eq!(evidence["effective"]["requests"]["cpu"], "2000m");
        Ok(())
    }

    #[test]
    fn uncapped_collector_omits_cpu_limit_and_requires_matching_readback() -> Result<()> {
        let mut expected = requirements();
        expected.limits.cpu = None;
        let mut deployment = json!({"spec":{"template":{"spec":{"containers":[{"name":"dataflow"}]}}}});
        configure_deployment_container_resources(&mut deployment, "dataflow", &expected)?;
        let container = &deployment["spec"]["template"]["spec"]["containers"][0];
        assert!(container.pointer("/resources/limits/cpu").is_none());
        verify_container_resources(std::slice::from_ref(container), "dataflow", "collector", &expected)?;
        let mut with_cap = container.clone();
        with_cap["resources"]["limits"]["cpu"] = json!("2");
        assert!(verify_container_resources(&[with_cap], "dataflow", "collector", &expected).is_err());
        let mut with_null = container.clone();
        with_null["resources"]["limits"]["cpu"] = Value::Null;
        assert!(verify_container_resources(&[with_null], "dataflow", "collector", &expected).is_err());
        Ok(())
    }

    #[test]
    fn resource_readback_rejects_mismatch_and_missing_limit() {
        let expected = requirements();
        let mismatch = vec![json!({
            "name": "dataflow",
            "resources": {
                "requests": {"cpu": "2", "memory": "4Gi"},
                "limits": {"cpu": "1", "memory": "6Gi"}
            }
        })];
        assert!(verify_container_resources(&mismatch, "dataflow", "collector", &expected).is_err());

        let missing = vec![json!({"name": "dataflow", "resources": {"requests": {"cpu": "2", "memory": "4Gi"}}})];
        assert!(verify_container_resources(&missing, "dataflow", "collector", &expected).is_err());
    }

    #[test]
    fn client_job_configuration_projects_resource_requirements() -> Result<()> {
        let mut job = json!({"spec": {"template": {"spec": {"containers": [{"name": "client"}]}}}});
        let expected: ResourceRequirements = serde_json::from_value(json!({
            "requests": {"cpu": "1", "memory": "512Mi"},
            "limits": {"cpu": "3", "memory": "3Gi"}
        }))?;
        configure_client_job_resources(&mut job, &expected)?;
        assert_eq!(
            job.pointer("/spec/template/spec/containers/0/resources"),
            Some(&serde_json::to_value(expected)?)
        );
        Ok(())
    }
}

#[cfg(test)]
mod aks_cpu_throttling_tests {
    use super::*;

    #[test]
    fn parses_cadvisor_cfs_counters_for_exact_pod_identity() -> Result<()> {
        let metrics = concat!(
            "container_cpu_cfs_periods_total{namespace=\"oat-test\",pod=\"worker-a\",container=\"dataflow\"} 500\n",
            "container_cpu_cfs_throttled_periods_total{namespace=\"oat-test\",pod=\"worker-a\",container=\"dataflow\"} 75\n",
            "container_cpu_cfs_periods_total{namespace=\"other\",pod=\"worker-a\",container=\"dataflow\"} 999\n",
        );
        let counters = parse_cadvisor_cpu_counters(metrics, "oat-test", "worker-a", "dataflow")?
            .context("matching cAdvisor counters missing")?;
        assert_eq!(counters.periods, 500);
        assert_eq!(counters.throttled_periods, 75);
        Ok(())
    }

    #[test]
    fn ignores_unrelated_fractional_and_nan_metrics_when_reading_cfs_counters() -> Result<()> {
        let metrics = concat!(
            "container_cpu_usage_seconds_total{namespace=\"oat-test\",pod=\"worker-a\",container=\"dataflow\"} 3.14159\n",
            "container_cpu_cfs_throttled_seconds_total{namespace=\"oat-test\",pod=\"worker-a\",container=\"dataflow\"} 0.125\n",
            "container_memory_usage_bytes{namespace=\"oat-test\",pod=\"worker-a\",container=\"dataflow\"} NaN\n",
            "container_cpu_cfs_periods_total{namespace=\"oat-test\",pod=\"worker-a\",container=\"dataflow\"} 500\n",
            "container_cpu_cfs_throttled_periods_total{namespace=\"oat-test\",pod=\"worker-a\",container=\"dataflow\"} 75\n",
        );
        let counters = parse_cadvisor_cpu_counters(metrics, "oat-test", "worker-a", "dataflow")?
            .context("matching cAdvisor counters missing")?;
        assert_eq!(counters.periods, 500);
        assert_eq!(counters.throttled_periods, 75);
        Ok(())
    }

    #[test]
    fn preserves_initial_scrape_reason_when_end_counters_are_available() {
        let start = CpuThrottleSample {
            role: "collector".to_owned(),
            pod: "worker-a".to_owned(),
            uid: "uid-a".to_owned(),
            container: "dataflow".to_owned(),
            node: "node-a".to_owned(),
            counters: None,
            unavailable_reason: Some("start cAdvisor scrape failed: forbidden".to_owned()),
        };
        let end = CpuThrottleSample {
            counters: Some(CpuThrottleCounters {
                periods: 200,
                throttled_periods: 20,
            }),
            unavailable_reason: None,
            ..start.clone()
        };
        let evidence = compare_cpu_throttling(&[start], &[end]);
        assert_eq!(evidence["status"], "unavailable");
        assert_eq!(evidence["pods"][0]["reason"], "start cAdvisor scrape failed: forbidden");
    }

    #[test]
    fn reports_measured_throttling_delta_without_inferring_from_worker_count() {
        let sample = |periods, throttled_periods| CpuThrottleSample {
            role: "collector".to_owned(),
            pod: "worker-a".to_owned(),
            uid: "uid-a".to_owned(),
            container: "dataflow".to_owned(),
            node: "node-a".to_owned(),
            counters: Some(CpuThrottleCounters {
                periods,
                throttled_periods,
            }),
            unavailable_reason: None,
        };
        let evidence = compare_cpu_throttling(&[sample(100, 10)], &[sample(300, 70)]);
        assert_eq!(evidence["status"], "measured");
        assert_eq!(evidence["pods"][0]["periods"], 200);
        assert_eq!(evidence["pods"][0]["throttled_periods"], 60);
        assert_eq!(evidence["pods"][0]["throttled_period_fraction"], 0.3);
    }
}

#[cfg(test)]
mod aks_resource_budget_tests {
    use super::*;

    #[test]
    fn aks_lifecycle_budget_includes_both_bounded_throttling_snapshots() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.platform.kind = crate::config::OatMode::Aks;
        let budget = config.timing_budget()?;
        let maintenance_rollouts = u64::from(maintenance_enabled(&config));
        let scale_rollouts = u64::try_from(config.deployment_names().len())? + maintenance_rollouts;
        let expected = budget.minimum_pin_ttl_ms
            + scale_rollouts * config.aks_rollout_timeout_seconds * 1_000
            + AKS_RESOURCE_COUNTER_SNAPSHOT_TIMEOUT_MS * 2;
        assert_eq!(budget.after_setup_budget_ms, expected);
        assert_eq!(
            budget.planned_cell_duration_ms,
            budget.pre_cell_setup_timeout_ms + budget.after_setup_budget_ms
        );
        Ok(())
    }
}
