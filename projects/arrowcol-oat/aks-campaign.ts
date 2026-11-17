import { spawnSync } from "node:child_process";
import { canonical_json } from '../../infra/azure/bootstrap.js';
import path from "node:path";
import { pathToFileURL } from "node:url";
import type { RunFicPlan } from '../../infra/azure/run_fic.js';
import type { OatQapReference } from '../../infra/azure/oat_qap.js';
import { loadCustodyConfig } from './aks-custody-config.js';
import { dispatchExternalEffect, isEffectBoundaryActive } from '../../infra/azure/effect-boundary.js';
import { ADMISSION_STOP_BUDGET_MS, DRAIN_BUDGET_MS, TERMINAL_CAPTURE_BUDGET_MS } from './aks-cleanup-budget.js';
import { parseEffectiveStopBudgets } from './stop-budgets.js';
export const HEARTBEAT_INTERVAL_MS = 60_000;
export const MISSED_HEARTBEAT_LIMIT = 3;
export const MAX_CAMPAIGN_LIFETIME_MS = 6 * 60 * 60 * 1000;
const MAX_ARM_OPERATIONS = 128;
export const CUSTODY_CLEANUP_STEPS = ["stop-admission", "bounded-drain", "preserve-evidence", "stop-maintenance", "record-terminal-state", "remove-run-namespace", "remove-run-qap", "remove-run-fic", "restore-safe-idle", "verify-safe-idle"] as const;
export const KNOWN_LIFECYCLE_CLEANUP_RESERVE_MS = ADMISSION_STOP_BUDGET_MS + DRAIN_BUDGET_MS + TERMINAL_CAPTURE_BUDGET_MS;
export const UNBOUNDED_CLEANUP_RESERVE_STEPS = ['preserve-evidence', 'remove-run-namespace', 'remove-run-qap',
  'remove-run-fic', 'restore-safe-idle', 'verify-safe-idle', 'handoff'] as const;
export type CellLifetimeAssessment =
  | { status: 'ready'; required_ms: number }
  | { status: 'insufficient'; required_ms: number }
  | { status: 'unknown'; required_ms: number; reason: string }
  | { status: 'invalid'; reason: string };
export function assessCellLifetimeBudget(remainingMs: number, durationMs: number, maintenanceStopMs: number,
  cleanupReserveMs?: number): CellLifetimeAssessment {
  if (![remainingMs, durationMs, maintenanceStopMs].every(nonnegative) ||
      cleanupReserveMs !== undefined && !nonnegative(cleanupReserveMs))
    return { status: 'invalid', reason: 'Cell admission budget values must be nonnegative safe integers.' };
  const reserve = cleanupReserveMs ?? KNOWN_LIFECYCLE_CLEANUP_RESERVE_MS;
  const required = durationMs + maintenanceStopMs + reserve;
  if (!Number.isSafeInteger(required)) return { status: 'invalid', reason: 'Cell admission budget sum exceeds safe integer bounds.' };
  if (remainingMs < required) return { status: 'insufficient', required_ms: required };
  return cleanupReserveMs === undefined ? { status: 'unknown', required_ms: required,
    reason: 'Cleanup completion has unbounded steps: ' + UNBOUNDED_CLEANUP_RESERVE_STEPS.join(', ') } :
    { status: 'ready', required_ms: required };
}
export type CleanupStepName = typeof CUSTODY_CLEANUP_STEPS[number];
export const DRAIN_UNSETTLED_REASON = 'drain_deadline_expired_unsettled';
export interface DrainExpiryReceipt {
  schemaVersion: 1; campaignId: string; producerOwnerId: string; producerFencingToken: number;
  actorOwnerId: string; actorFencingToken: number; orchestrationRunId: string; cellRunId: string; namespaceName: string; namespaceUid: string;
  deadlineUnixMs: number; observedAtUnixMs: number; reason: typeof DRAIN_UNSETTLED_REASON; deltaRows: null; kustoRows: null;
}
export interface CleanupReceipt { step: CleanupStepName; idempotencyKey: string; status: "pending" | "running" | "complete" | "unsettled";
  unsettled?: DrainExpiryReceipt; }
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const storageName = /^[a-z0-9]{3,24}$/;
const containerName = /^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$/;
export type CampaignMode = 'observe' | 'armed';
export type CampaignStatus = 'active' | 'fenced' | 'cleaning' | 'complete' | 'complete_unsettled';
export type ArmOperationStatus = 'pending' | 'succeeded' | 'failed' | 'indeterminate';
export type CleanupArmTarget =
  { kind: 'aks-pool'; count: number; autoscale: boolean; minimum: number | null; maximum: number | null } |
  { kind: 'kusto'; sku: 'Standard_E4ads_v5'; capacity: 2; minimum: 2; maximum: 4; autoStop: true };
export interface ArmOperationReceipt { id: string; resourceId: string; operationUrl: string; submittedAtMs: number;
  status: ArmOperationStatus; lastCheckedAtMs?: number; cleanupTarget?: CleanupArmTarget; }
export const SCALE_RESTORE_TIMEOUT_MS = 5 * 60 * 1000;
export type ScaleDimension = 'loadgen-max' | 'collector-cpu-limit' | 'collector-memory-limit' |
  'collector-node-max' | 'kusto-capacity' | 'kusto-sku' | 'system-pool-count';
export interface ScaleTarget { dimension: ScaleDimension; before: number | string; after: number | string }
export interface ScaleResourceIdentity {
  transport: 'azure-rest' | 'kubernetes'; resource_id: string; api_version: string | null; kind: string;
  namespace: string | null; name: string; uid: string | null;
}
export type ScaleTrialPhase = 'apply-intent' | 'apply-dispatch' | 'applied' | 'kept' | 'restore-intent' |
  'restore-dispatch' | 'restored' | 'restore-stopped' | 'reverted' | 'unknown';
export interface ScaleEffectDispatch {
  state: 'forwarding' | 'confirmed' | 'unknown'; transport: 'azure-rest' | 'kubernetes'; method: 'PUT' | 'PATCH' | 'kubectl-json-patch';
  target: ScaleTarget; before: number | string; started_at_ms: number; request_sha256: string;
  resource_version?: string; etag?: string; response_status?: number; returncode?: number; operation_url?: string;
}
export interface ScaleTrialReceipt {
  schema_version: 1; trial_id: string; campaign_id: string; owner_id: string; fencing_token: number; cell_run_id: string;
  started_at_ms: number; deadline_at_ms: number; restore_deadline_at_ms: number; target: ScaleTarget; restore: ScaleTarget; evidence: string[];
  baseline: { committed_rows_per_second: number; qap_pending_backlog: number };
  resource: ScaleResourceIdentity; phase: ScaleTrialPhase; operation_url?: string;
  dispatches?: { apply?: ScaleEffectDispatch; restore?: ScaleEffectDispatch };
  result?: { outcome: 'keep' | 'revert' | 'stop' | 'unknown'; committed_rows_per_second: number | null;
    qap_pending_backlog: number | null; ineffective_increments: number; reason: string };
}
export function validateScaleTarget(value: unknown): ScaleTarget {
  if (!record(value) || !exactRecord(value, ['dimension', 'before', 'after']) ||
      !['loadgen-max', 'collector-cpu-limit', 'collector-memory-limit', 'collector-node-max', 'kusto-capacity',
        'kusto-sku', 'system-pool-count'].includes(String(value.dimension)) ||
      typeof value.before !== typeof value.after || !['number', 'string'].includes(typeof value.before) || value.before === value.after)
    throw new Error('Scale target is invalid or has an unsupported dimension.');
  const dimension = value.dimension as ScaleDimension;
  if (typeof value.before === 'number' && (!Number.isSafeInteger(value.before) || value.before < 0 ||
      !Number.isSafeInteger(value.after) || (value.after as number) < 0))
    throw new Error('Scale target numeric values must be nonnegative safe integers.');
  if (dimension === 'kusto-sku' && (!['Standard_E4ads_v5', 'Standard_E8ads_v5'].includes(String(value.before)) ||
      !['Standard_E4ads_v5', 'Standard_E8ads_v5'].includes(String(value.after))))
    throw new Error('Kusto SKU target is not an approved one-step size.');
  const bounds: Partial<Record<ScaleDimension, { min: number; max: number; delta: number }>> = {
    'loadgen-max': { min: 1, max: 4, delta: 1 }, 'collector-cpu-limit': { min: 1, max: 8, delta: 1 },
    'collector-memory-limit': { min: 1, max: 24, delta: 2 }, 'collector-node-max': { min: 0, max: 4, delta: 1 },
    'kusto-capacity': { min: 2, max: 4, delta: 1 }, 'system-pool-count': { min: 2, max: 4, delta: 1 },
  };
  const bound = bounds[dimension];
  if (bound && (typeof value.before !== 'number' || typeof value.after !== 'number' ||
      value.before < bound.min || value.before > bound.max || value.after < bound.min || value.after > bound.max ||
      Math.abs(value.after - value.before) !== bound.delta))
    throw new Error('Scale target exceeds the approved one-step range.');
  return value as unknown as ScaleTarget;
}
function validScaleTrial(value: unknown, state: CampaignState): value is ScaleTrialReceipt {
  if (!record(value) || !optionalRecord(value, ['schema_version', 'trial_id', 'campaign_id', 'owner_id', 'fencing_token', 'cell_run_id',
      'started_at_ms', 'deadline_at_ms', 'restore_deadline_at_ms', 'target', 'restore', 'evidence', 'baseline', 'resource', 'phase'],
      ['operation_url', 'result', 'dispatches'])) return false;
  let target: ScaleTarget, restore: ScaleTarget;
  try { target = validateScaleTarget(value.target); restore = validateScaleTarget(value.restore); } catch { return false; }
  const resource = value.resource as Record<string, unknown>;
  if (!record(resource) || !exactRecord(resource, ['transport', 'resource_id', 'api_version', 'kind', 'namespace', 'name', 'uid']) ||
      !['azure-rest', 'kubernetes'].includes(String(resource.transport)) || !nonempty(resource.resource_id) ||
      !nonempty(resource.kind) || !nonempty(resource.name) ||
      !(resource.api_version === null || nonempty(resource.api_version)) ||
      !(resource.namespace === null || k8sName(resource.namespace)) ||
      !(resource.uid === null || uuid.test(String(resource.uid))) ||
      target.dimension !== restore.dimension || target.before !== restore.after || target.after !== restore.before ||
      !Array.isArray(value.evidence) || value.evidence.length < 1 || value.evidence.length > 8 ||
      value.evidence.some(item => typeof item !== 'string' || item.length > 64 || !/^[a-z0-9-]+$/.test(item)) ||
      !record(value.baseline) || !exactRecord(value.baseline, ['committed_rows_per_second', 'qap_pending_backlog']) ||
      typeof value.baseline.committed_rows_per_second !== 'number' || !Number.isFinite(value.baseline.committed_rows_per_second) || value.baseline.committed_rows_per_second < 0 ||
      !nonnegative(value.baseline.qap_pending_backlog) ||
      value.schema_version !== 1 || !uuid.test(String(value.trial_id)) || value.campaign_id !== state.campaignId ||
      value.owner_id !== state.ownerId || !nonnegative(value.fencing_token) || value.fencing_token < 1 ||
      value.fencing_token > state.fencingToken || !state.runInventory || state.mode !== 'armed' ||
      !uuid.test(String(value.cell_run_id)) || !nonnegative(value.started_at_ms) || !nonnegative(value.deadline_at_ms) ||
      !nonnegative(value.restore_deadline_at_ms) || !Number.isSafeInteger(value.deadline_at_ms + SCALE_RESTORE_TIMEOUT_MS) ||
      value.restore_deadline_at_ms !== value.deadline_at_ms + SCALE_RESTORE_TIMEOUT_MS ||
      value.deadline_at_ms < value.started_at_ms || value.deadline_at_ms > state.expiresAtMs || value.restore_deadline_at_ms > state.expiresAtMs ||
      !['apply-intent', 'apply-dispatch', 'applied', 'kept', 'restore-intent', 'restore-dispatch', 'restored',
        'restore-stopped', 'reverted', 'unknown'].includes(String(value.phase))) return false;
  if (value.dispatches !== undefined) {
    const dispatches = value.dispatches;
    if (!record(dispatches) || !optionalRecord(dispatches, [], ['apply', 'restore'])) return false;
    for (const [kind, dispatch, expectedTarget] of [['apply', dispatches.apply, target], ['restore', dispatches.restore, restore]] as const) {
      if (dispatch === undefined) continue;
      if (!record(dispatch)) return false;
      const responseStatus = dispatch.response_status, returncode = dispatch.returncode, operationUrl = dispatch.operation_url;
      if (!optionalRecord(dispatch, ['state', 'transport', 'method', 'target', 'before', 'started_at_ms', 'request_sha256'],
          ['resource_version', 'etag', 'response_status', 'returncode', 'operation_url']) ||
          !['forwarding', 'confirmed', 'unknown'].includes(String(dispatch.state)) ||
          !['azure-rest', 'kubernetes'].includes(String(dispatch.transport)) ||
          !['PUT', 'PATCH', 'kubectl-json-patch'].includes(String(dispatch.method)) ||
          canonical_json(dispatch.target) !== canonical_json(expectedTarget) || dispatch.before !== expectedTarget.before ||
          !nonnegative(dispatch.started_at_ms) || dispatch.started_at_ms < value.started_at_ms ||
          dispatch.started_at_ms >= (kind === 'apply' ? value.deadline_at_ms : value.restore_deadline_at_ms) ||
          typeof dispatch.request_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(dispatch.request_sha256) ||
          dispatch.transport === 'kubernetes' && (dispatch.method !== 'kubectl-json-patch' || !nonempty(dispatch.resource_version) || dispatch.etag !== undefined) ||
          dispatch.transport === 'azure-rest' && (!['PUT', 'PATCH'].includes(String(dispatch.method)) || !nonempty(dispatch.etag) || dispatch.resource_version !== undefined) ||
          responseStatus !== undefined && (typeof responseStatus !== 'number' || !Number.isSafeInteger(responseStatus) || responseStatus < 100 || responseStatus > 599) ||
          returncode !== undefined && (typeof returncode !== 'number' || !Number.isSafeInteger(returncode)) ||
          dispatch.state === 'confirmed' && (dispatch.transport === 'azure-rest' ? typeof responseStatus !== 'number' || responseStatus < 200 || responseStatus >= 300 : typeof returncode !== 'number' || returncode !== 0) ||
          operationUrl !== undefined && (dispatch.transport !== 'azure-rest' || typeof operationUrl !== 'string' || !operationUrl.startsWith('https://management.azure.com/'))) return false;
    }
    const applyState = record(dispatches.apply) ? dispatches.apply.state : undefined;
    const restoreState = record(dispatches.restore) ? dispatches.restore.state : undefined;
    if (value.phase === 'apply-dispatch' && !['forwarding', 'confirmed'].includes(String(applyState)) ||
        value.phase === 'restore-dispatch' && !['forwarding', 'confirmed'].includes(String(restoreState)) ||
        value.phase === 'restore-stopped' && (target.dimension !== 'loadgen-max' || dispatches.restore !== undefined)) return false;
  }
  if (value.result !== undefined) {
    const result = value.result;
    if (!record(result) || !exactRecord(result, ['outcome', 'committed_rows_per_second', 'qap_pending_backlog', 'ineffective_increments', 'reason']) ||
        !['keep', 'revert', 'stop', 'unknown'].includes(String(result.outcome)) ||
        !(result.committed_rows_per_second === null || typeof result.committed_rows_per_second === 'number' && Number.isFinite(result.committed_rows_per_second) && result.committed_rows_per_second >= 0) ||
        !(result.qap_pending_backlog === null || nonnegative(result.qap_pending_backlog)) || typeof result.ineffective_increments !== 'number' || !Number.isSafeInteger(result.ineffective_increments) || result.ineffective_increments < 0 ||
        typeof result.reason !== 'string' || result.reason.length === 0 || result.reason.length > 128 || /[\u0000-\u001f]/.test(result.reason) ||
        result.outcome === 'unknown' !== [result.committed_rows_per_second, result.qap_pending_backlog].every(item => item === null)) return false;
  }
  if (resource.transport === 'azure-rest') {
    if (!String(resource.resource_id).startsWith('/subscriptions/') || resource.namespace !== null || resource.uid !== null ||
        typeof resource.api_version !== 'string') return false;
    if (value.operation_url !== undefined) {
      try {
        const url = new URL(String(value.operation_url));
        const subscription = /^\/subscriptions\/([0-9a-f-]{36})\//i.exec(String(resource.resource_id))?.[1];
        if (!subscription || url.protocol !== 'https:' || url.hostname !== 'management.azure.com' || url.username || url.password || url.hash ||
            !url.pathname.toLowerCase().includes('/subscriptions/' + subscription.toLowerCase())) return false;
      } catch { return false; }
    }
  } else if (resource.transport === 'kubernetes' && (resource.api_version !== 'apps/v1' || resource.kind !== 'Deployment' ||
      resource.namespace === null || resource.uid === null || value.operation_url !== undefined ||
      resource.resource_id !== 'apps/v1/Deployment/' + resource.namespace + '/' + resource.name + '/' + resource.uid)) return false;
  return true;
}
export function validCleanupArmTarget(value: unknown): value is CleanupArmTarget {
  if (!record(value)) return false;
  if (value.kind === 'aks-pool') return exactRecord(value, ['kind', 'count', 'autoscale', 'minimum', 'maximum']) &&
    nonnegative(value.count) && typeof value.autoscale === 'boolean' &&
    (value.autoscale ? value.count === 0 && value.minimum === 0 && value.maximum === 1 :
      [2, 3].includes(Number(value.count)) && value.minimum === null && value.maximum === null);
  return exactRecord(value, ['kind', 'sku', 'capacity', 'minimum', 'maximum', 'autoStop']) && value.kind === 'kusto' &&
    value.sku === 'Standard_E4ads_v5' && value.capacity === 2 && value.minimum === 2 && value.maximum === 4 && value.autoStop === true;
}
export interface CampaignState {
  schemaVersion: 1; campaignId: string; ownerId: string; fencingToken: number; mode: CampaignMode; status: CampaignStatus;
  startedAtMs: number; heartbeatAtMs: number; heartbeatSequence: number; expiresAtMs: number; lastEvaluatedAtMs?: number;
  lastMissedHeartbeatCount?: number; fenceReason?: 'heartbeat-expired' | 'max-lifetime'; fencedAtMs?: number;
  cleanupStatus: 'not-started' | 'pending' | 'running' | 'verified' | 'verified_with_unsettled'; cleanupSteps: CleanupReceipt[];
  runInventory?: RunInventory; operations: ArmOperationReceipt[]; scaleTrials?: ScaleTrialReceipt[];
}
export interface StoredCampaign { state: CampaignState; etag: string; }
export interface CampaignStore { read(): Promise<StoredCampaign | undefined>; write(state: CampaignState, expectedEtag?: string): Promise<string>; }
export type TokenProvider = (resource: string) => Promise<string>;
export type Fetcher = typeof fetch;
function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function nonnegative(value: unknown): value is number { return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0; }
function nonempty(value: unknown): value is string { return typeof value === 'string' && value.length > 0 && value.length <= 2048 && !/[\u0000-\u001f]/.test(value); }
function k8sName(value: unknown): value is string { return typeof value === 'string' && value.length <= 63 && /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(value); }
function exactRecord(value: unknown, keys: string[]): value is Record<string, unknown> {
  return record(value) && Object.keys(value).length === keys.length && Object.keys(value).every(key => keys.includes(key));
}
function optionalRecord(value: unknown, required: string[], optional: string[]): value is Record<string, unknown> {
  return record(value) && required.every(key => Object.hasOwn(value, key)) &&
    Object.keys(value).every(key => required.includes(key) || optional.includes(key));
}
function sameFlatRecord(before: unknown, after: unknown): boolean {
  return record(before) && record(after) && Object.keys(before).length === Object.keys(after).length &&
    Object.entries(before).every(([key, value]) => after[key] === value);
}
export type RunInventoryResourceState = 'planned' | 'observed' | 'removed' | 'absent' | 'uncertain';
export type InventoryResourceRole = 'collector' | 'client' | 'maintenance' | 'router';
export type InventoryStepState = 'not_started' | 'pending' | 'running' | 'complete' | 'failed' | 'uncertain';
export interface InventoryStep { state: InventoryStepState; deadline_unix_ms: number | null; reason_code: string | null; }
export interface InventoryAbsenceReceipt { api_version: string; kind: string; namespace: string; name: string; namespace_uid: string | null; observed_at_unix_ms: number; }
export interface InventoryFicAbsenceReceipt { resource_id: string; name: string; observed_at_unix_ms: number; }
export interface InventoryQapAbsenceReceipt { database: string; external_table: string; relative_path: string; observed_at_unix_ms: number; }
export interface RunInventoryResource {
  api_version: string;
  kind: string;
  namespace: string;
  name: string;
  uid: string | null;
  absence: InventoryAbsenceReceipt | null;
  role: InventoryResourceRole;
  state: RunInventoryResourceState;
}
export interface RunInventory {
  schema_version: 1;
  campaign_id: string;
  owner_id: string;
  fencing_token: number;
  orchestration_run_id: string;
  cell_run_id: string;
  load_admission: 'not_authorized' | 'authorization_pending' | 'authorized';
  namespace: { name: string; uid: string | null; owner_token: string };
  resources: RunInventoryResource[];
  fic: { resource_id: string | null; name: string | null; state: RunInventoryResourceState; absence: InventoryFicAbsenceReceipt | null; plan?: RunFicPlan | null } | null;
  qap: { database: string; bindings: Array<{ external_table: string; relative_path: string }>; state: RunInventoryResourceState; absence: InventoryQapAbsenceReceipt[]; reference?: OatQapReference | null } | null;
  evidence_destination?: { account: string; container: string; blob_prefix: string } | null;
  planned_cell_duration_ms?: number;
  effective_stop_budgets?: { maintenance_grace_seconds: number; maintenance_stop_timeout_ms: number };
  cleanup: { stop_admission: InventoryStep; bounded_drain: InventoryStep; evidence_capture: InventoryStep; stop_maintenance: InventoryStep;
    terminal_capture: InventoryStep; handoff: InventoryStep };
}
function stateValue(value: unknown): value is RunInventoryResourceState { return ['planned', 'observed', 'removed', 'absent', 'uncertain'].includes(String(value)); }
function stepStateValue(value: unknown): value is InventoryStepState { return ['not_started', 'pending', 'running', 'complete', 'failed', 'uncertain'].includes(String(value)); }
function canonicalUuid(value: unknown): value is string {
  return typeof value === 'string' && uuid.test(value) && value === value.toLowerCase() && value !== '00000000-0000-0000-0000-000000000000';
}
function validStep(value: unknown): value is InventoryStep {
  return exactRecord(value, ['state', 'deadline_unix_ms', 'reason_code']) && stepStateValue(value.state) &&
    (value.deadline_unix_ms === null || nonnegative(value.deadline_unix_ms)) &&
    (value.reason_code === null || typeof value.reason_code === 'string' && value.reason_code.length > 0 && value.reason_code.length <= 64 && /^[a-z0-9_]+$/.test(value.reason_code)) &&
    (!['failed', 'uncertain'].includes(String(value.state)) || value.reason_code !== null);
}
function validateRunInventory(value: unknown, state: CampaignState): RunInventory {
  const stopBudgets = value && typeof value === 'object' && !Array.isArray(value) ?
    (value as Record<string, unknown>).effective_stop_budgets : undefined;
  if (stopBudgets !== undefined) parseEffectiveStopBudgets(stopBudgets);
  const fields = ['schema_version', 'campaign_id', 'owner_id', 'fencing_token', 'orchestration_run_id', 'cell_run_id', 'load_admission',
    'namespace', 'resources', 'fic', 'qap', 'cleanup'];
  if (!optionalRecord(value, fields, ['evidence_destination', 'planned_cell_duration_ms', 'effective_stop_budgets']) || value.schema_version !== 1 || value.campaign_id !== state.campaignId ||
      value.owner_id !== state.ownerId || value.fencing_token !== (state.status === 'active' ? state.fencingToken : state.runInventory?.fencing_token) || !nonnegative(value.fencing_token) || value.fencing_token === 0 ||
      !canonicalUuid(value.campaign_id) || !canonicalUuid(value.owner_id) || !canonicalUuid(value.orchestration_run_id) || !canonicalUuid(value.cell_run_id) ||
      !['not_authorized', 'authorization_pending', 'authorized'].includes(String(value.load_admission)) ||
      (value.planned_cell_duration_ms === undefined) !== (value.effective_stop_budgets === undefined) ||
      value.planned_cell_duration_ms !== undefined && (!nonnegative(value.planned_cell_duration_ms) ||
        value.planned_cell_duration_ms === 0 && value.load_admission !== 'not_authorized') ||
      !exactRecord(value.namespace, ['name', 'uid', 'owner_token']) || !k8sName(value.namespace.name) ||
      !String(value.namespace.name).endsWith('-' + value.cell_run_id.replace(/-/g, '')) ||
      !(value.namespace.uid === null || canonicalUuid(value.namespace.uid)) || !canonicalUuid(value.namespace.owner_token) ||
      value.namespace.owner_token !== state.ownerId || !Array.isArray(value.resources) || value.resources.length > 256 ||
      !(value.fic === null || optionalRecord(value.fic, ['resource_id', 'name', 'state', 'absence'], ['plan']) && ['planned', 'observed', 'removed', 'absent', 'uncertain'].includes(String(value.fic.state))) ||
      !(value.qap === null || optionalRecord(value.qap, ['database', 'bindings', 'state', 'absence'], ['reference']) && nonempty(value.qap.database) && Array.isArray(value.qap.bindings) && ['planned', 'observed', 'removed', 'absent', 'uncertain'].includes(String(value.qap.state)) && Array.isArray(value.qap.absence)) ||
      !exactRecord(value.cleanup, ['stop_admission', 'bounded_drain', 'evidence_capture', 'stop_maintenance', 'terminal_capture', 'handoff']) ||
      Object.values(value.cleanup).some(item => !validStep(item))) {
    throw new Error('RunInventory is malformed or fenced to another owner.');
  }
  const inventory = value as unknown as RunInventory;
  if (state.status !== 'active' && state.runInventory && inventory.load_admission !== state.runInventory.load_admission)
    throw new Error('Fenced cleanup cannot change the producer load-admission disposition.');
  const safePath = (path: unknown): path is string => typeof path === 'string' && path.length > 0 && path.length <= 2048 &&
    path.split('/').every(part => /^[A-Za-z0-9_.-]+$/.test(part) && part !== '.' && part !== '..');
  if (inventory.evidence_destination) {
    const destination = inventory.evidence_destination;
    if (!exactRecord(destination, ['account', 'container', 'blob_prefix']) || !storageName.test(destination.account) ||
        !containerName.test(destination.container) || !safePath(destination.blob_prefix))
      throw new Error('RunInventory evidence destination must be an exact credential-free Blob prefix.');
  } else if (inventory.evidence_destination !== undefined && inventory.evidence_destination !== null)
    throw new Error('RunInventory evidence destination is malformed.');
  if (inventory.fic !== null) {
    const fic = inventory.fic;
    if (fic.plan) {
      const plan = fic.plan;
      const safeHttps = (input: unknown, originOnly = false) => {
        if (typeof input !== 'string') return false;
        try { const url = new URL(input); return url.protocol === 'https:' && !url.username && !url.password &&
          !url.search && !url.hash && (!originOnly || url.pathname === '/'); } catch { return false; }
      };
      if (!exactRecord(plan, ['version', 'runId', 'namespace', 'serviceAccount', 'identityId', 'resourceId', 'clientId',
        'tenantId', 'aksId', 'server', 'credentialName', 'issuer', 'subject', 'audience']) || plan.version !== 1 ||
          plan.runId !== inventory.cell_run_id || plan.namespace !== inventory.namespace.name || !k8sName(plan.serviceAccount) ||
          plan.resourceId !== fic.resource_id || plan.credentialName !== fic.name ||
          plan.credentialName !== 'arrowcol-run-' + inventory.cell_run_id ||
          plan.resourceId !== plan.identityId + '/federatedIdentityCredentials/' + plan.credentialName ||
          !canonicalUuid(plan.clientId) || !canonicalUuid(plan.tenantId) || !safeHttps(plan.server, true) || !safeHttps(plan.issuer) ||
          !/^\/subscriptions\/[a-f0-9-]{36}\/resourceGroups\/[^/?#]+\/providers\/Microsoft\.ManagedIdentity\/userAssignedIdentities\/[^/?#]+$/i.test(plan.identityId) ||
          !/^\/subscriptions\/[a-f0-9-]{36}\/resourceGroups\/[^/?#]+\/providers\/Microsoft\.ContainerService\/managedClusters\/[^/?#]+$/i.test(plan.aksId) ||
          plan.subject !== `system:serviceaccount:${plan.namespace}:${plan.serviceAccount}` || plan.audience !== 'api://AzureADTokenExchange')
        throw new Error('RunInventory FIC plan differs from its exact immutable run identity.');
    } else if (fic.plan !== undefined && fic.plan !== null) throw new Error('RunInventory FIC plan is malformed.');
    if (!(fic.resource_id === null || nonempty(fic.resource_id)) || !(fic.name === null || k8sName(fic.name)) ||
        (fic.resource_id !== null || fic.state === 'observed' || fic.state === 'removed' || fic.state === 'absent') && (!fic.resource_id || !fic.name ||
          !fic.resource_id.toLowerCase().startsWith('/subscriptions/') ||
          !fic.resource_id.toLowerCase().endsWith('/federatedidentitycredentials/' + fic.name.toLowerCase()) || /[?#]/.test(fic.resource_id)) ||
        ['observed', 'removed'].includes(fic.state) && (!fic.resource_id || !fic.name)) {
      throw new Error('RunInventory FIC identity is malformed.');
    }
    if (fic.state === 'absent') {
      if (!exactRecord(fic.absence, ['resource_id', 'name', 'observed_at_unix_ms']) || fic.absence.resource_id !== fic.resource_id ||
          fic.absence.name !== fic.name || !nonnegative(fic.absence.observed_at_unix_ms) || fic.absence.observed_at_unix_ms === 0) {
        throw new Error('Absent RunInventory FIC requires an exact not-found receipt.');
      }
    } else if (fic.absence !== null) throw new Error('RunInventory FIC absence receipt requires absent state.');
  }
  if (inventory.qap !== null) {
    const qap = inventory.qap;
    if (qap.reference) {
      const reference = qap.reference;
      if (!exactRecord(reference, ['schema_version', 'account', 'container', 'blob', 'run_id', 'nonce', 'manifest_sha256',
        'namespace_uid', 'namespace_owner_token']) || reference.schema_version !== 1 ||
          !storageName.test(reference.account) || !containerName.test(reference.container) || !safePath(reference.blob) ||
          reference.run_id !== inventory.cell_run_id || !canonicalUuid(reference.nonce) ||
          !/^[a-f0-9]{64}$/.test(reference.manifest_sha256) || reference.namespace_uid !== inventory.namespace.uid ||
          reference.namespace_owner_token !== inventory.namespace.owner_token)
        throw new Error('RunInventory QAP reference differs from its original namespace/run.');
    } else if (qap.reference !== undefined && qap.reference !== null) throw new Error('RunInventory QAP reference is malformed.');
    const safeIdentifier = (item: string) => /^[a-zA-Z0-9_-]+$/.test(item);
    if (!safeIdentifier(qap.database)) throw new Error('RunInventory QAP database is malformed.');
    const bindingKeys = new Set<string>();
    for (const binding of qap.bindings) {
      if (!exactRecord(binding, ['external_table', 'relative_path']) || !nonempty(binding.external_table) ||
          !safeIdentifier(binding.external_table) || !nonempty(binding.relative_path) ||
          /[?#@\\:]/.test(binding.relative_path) || binding.relative_path.startsWith('/') ||
          binding.relative_path.split('/').some(part => part === '' || part === '.' || part === '..')) {
        throw new Error('RunInventory QAP binding is malformed.');
      }
      const key = binding.external_table + '|' + binding.relative_path;
      if (bindingKeys.has(key)) throw new Error('RunInventory QAP contains duplicate bindings.');
      bindingKeys.add(key);
    }
    const absentKeys = new Set<string>();
    for (const receipt of qap.absence) {
      if (!exactRecord(receipt, ['database', 'external_table', 'relative_path', 'observed_at_unix_ms']) ||
          receipt.database !== qap.database || !bindingKeys.has(receipt.external_table + '|' + receipt.relative_path) ||
          !nonnegative(receipt.observed_at_unix_ms) || receipt.observed_at_unix_ms === 0) {
        throw new Error('RunInventory QAP absence receipt does not match an exact binding.');
      }
      const key = receipt.external_table + '|' + receipt.relative_path;
      if (absentKeys.has(key)) throw new Error('RunInventory QAP contains duplicate absence receipts.');
      absentKeys.add(key);
    }
    if (qap.state === 'absent' && (qap.bindings.length === 0 || absentKeys.size !== bindingKeys.size) ||
        qap.state === 'planned' && qap.absence.length > 0) {
      throw new Error('Absent RunInventory QAP requires exactly one not-found receipt per planned binding.');
    }
    if (['observed', 'removed'].includes(qap.state) && !qap.bindings.length) throw new Error('Observed RunInventory QAP requires an exact binding.');
  }
  const resourceKeys = new Set<string>();
  for (const resource of inventory.resources) {
    if (!exactRecord(resource, ['api_version', 'kind', 'namespace', 'name', 'uid', 'absence', 'role', 'state']) ||
        !nonempty(resource.api_version) || !nonempty(resource.kind) || resource.namespace !== inventory.namespace.name ||
        typeof resource.name !== 'string' || resource.name.length === 0 || resource.name.length > 253 ||
        !/^[a-z0-9.-]+$/.test(resource.name) || !['apps/v1:Deployment', 'batch/v1:Job', 'v1:Pod'].includes(resource.api_version + ':' + resource.kind) ||
        !['collector', 'client', 'maintenance', 'router'].includes(String(resource.role)) || !stateValue(resource.state) ||
        !(resource.uid === null || canonicalUuid(resource.uid)) ||
        ['observed', 'removed'].includes(resource.state) && (resource.uid === null || inventory.namespace.uid === null) ||
        resource.state === 'absent' && (resource.uid !== null || !exactRecord(resource.absence, ['api_version', 'kind', 'namespace', 'name', 'namespace_uid', 'observed_at_unix_ms']) ||
          resource.absence.api_version !== resource.api_version || resource.absence.kind !== resource.kind ||
          resource.absence.namespace !== resource.namespace || resource.absence.name !== resource.name ||
          resource.absence.namespace_uid !== inventory.namespace.uid || !nonnegative(resource.absence.observed_at_unix_ms) || resource.absence.observed_at_unix_ms === 0) ||
        resource.state !== 'absent' && resource.absence !== null) {
      throw new Error('RunInventory resource identity is malformed.');
    }
    const key = [resource.api_version, resource.kind, resource.name].join('|');
    if (resourceKeys.has(key)) throw new Error('RunInventory contains duplicate resource identities.');
    resourceKeys.add(key);
  }
  for (const name of ['stop_admission', 'bounded_drain', 'stop_maintenance'] as const) {
    const step = inventory.cleanup[name];
    if (step.state === 'running' && (step.deadline_unix_ms === null ||
        name === 'stop_maintenance' && step.deadline_unix_ms <= state.startedAtMs))
      throw new Error('Running RunInventory admission, drain and maintenance steps require an absolute deadline.');
  }
  if (inventory.cleanup.handoff.state === 'complete' &&
      (Object.entries(inventory.cleanup).some(([key, item]) => key !== 'handoff' && item.state !== 'complete') ||
       inventory.resources.some(resource => resource.state !== 'removed' && resource.state !== 'absent') || inventory.fic !== null && inventory.fic.state !== 'removed' && inventory.fic.state !== 'absent' ||
       inventory.qap !== null && inventory.qap.state !== 'removed' && inventory.qap.state !== 'absent')) {
    throw new Error('RunInventory handoff cannot complete while owned resources or cleanup actions remain unresolved.');
  }
  return inventory;
}
function mergeResourceState(before: RunInventoryResourceState, after: RunInventoryResourceState): RunInventoryResourceState {
  if (before === 'removed' && after !== 'removed' || before === 'absent' && after !== 'absent') throw new Error('Terminal resource state cannot be reopened.');
  if ((before === 'observed' || before === 'uncertain') && after === 'planned') return before;
  return after;
}
function mergeStep(before: InventoryStep, after: InventoryStep): InventoryStep {
  if (before.state === 'complete' && after.state !== 'complete') throw new Error('Completed cleanup state cannot be reopened.');
  return { ...after, deadline_unix_ms: before.deadline_unix_ms ?? after.deadline_unix_ms,
    reason_code: after.reason_code ?? (after.state === before.state ? before.reason_code : null) };
}
export function isInitialRunInventory(inventory: RunInventory | undefined): boolean {
  return Boolean(inventory && inventory.load_admission === 'not_authorized' && inventory.namespace.uid === null &&
    nonnegative(inventory.planned_cell_duration_ms) && inventory.effective_stop_budgets !== undefined &&
    inventory.resources.every(resource => resource.state === 'planned' && resource.uid === null && resource.absence === null) &&
    (!inventory.fic || inventory.fic.state === 'planned' && inventory.fic.absence === null) &&
    (!inventory.qap || inventory.qap.state === 'planned' && inventory.qap.absence.length === 0 && !inventory.qap.reference) &&
    Object.values(inventory.cleanup).every(step =>
      step.state === 'not_started' && step.deadline_unix_ms === null && step.reason_code === null));
}
export function validateInitialRunInventory(value: unknown, campaignId: string, ownerId: string): RunInventory {
  const inventory = validateRunInventory(value, newCampaignState(campaignId, ownerId, 0));
  if (!isInitialRunInventory(inventory)) throw new Error('Initial RunInventory must remain not_authorized with persisted maintenance budgets.');
  return inventory;
}
function mergeRunInventory(before: RunInventory | undefined, after: RunInventory): RunInventory {
  const initial = () => {
    if (!isInitialRunInventory(after))
      throw new Error('Initial RunInventory must positively record not_authorized before external provisioning.');
    return after;
  };
  if (!before) return initial();
  const sameCell = before.campaign_id === after.campaign_id && before.owner_id === after.owner_id &&
    before.fencing_token === after.fencing_token && before.orchestration_run_id === after.orchestration_run_id &&
    before.cell_run_id === after.cell_run_id;
  if (!sameCell) {
    if (before.cleanup.handoff.state !== 'complete') throw new Error('An unresolved RunInventory already owns the campaign.');
    return initial();
  }
  const admission = ['not_authorized', 'authorization_pending', 'authorized'];
  const from = admission.indexOf(before.load_admission), to = admission.indexOf(after.load_admission);
  if (to < from || to > from + 1) throw new Error('RunInventory load admission must advance monotonically through authorization_pending.');
  if (before.planned_cell_duration_ms !== after.planned_cell_duration_ms ||
      (before.effective_stop_budgets === undefined) !== (after.effective_stop_budgets === undefined) ||
      before.effective_stop_budgets && after.effective_stop_budgets &&
        !sameFlatRecord(before.effective_stop_budgets, after.effective_stop_budgets))
    throw new Error('RunInventory cell duration and maintenance stop budget are immutable.');
  if (before.namespace.name !== after.namespace.name || before.namespace.owner_token !== after.namespace.owner_token ||
      before.namespace.uid && after.namespace.uid && before.namespace.uid !== after.namespace.uid) {
    throw new Error('RunInventory immutable ownership fields changed.');
  }
  if (before.evidence_destination && !sameFlatRecord(before.evidence_destination, after.evidence_destination))
    throw new Error('RunInventory evidence destination is immutable.');
  const resources = new Map(before.resources.map(resource => [[resource.api_version, resource.kind, resource.name].join('|'), resource]));
  for (const next of after.resources) {
    const key = [next.api_version, next.kind, next.name].join('|');
    const previous = resources.get(key);
    if (!previous) { resources.set(key, next); continue; }
    if (previous.role !== next.role || previous.uid && next.uid && previous.uid !== next.uid ||
        previous.absence && JSON.stringify(previous.absence) !== JSON.stringify(next.absence)) throw new Error('RunInventory resource identity changed.');
    resources.set(key, { ...previous, ...next, uid: previous.uid ?? next.uid, absence: previous.absence ?? next.absence,
      state: mergeResourceState(previous.state, next.state) });
  }
  const mergeFic = () => {
    if (!before.fic) return after.fic;
    if (!after.fic || before.fic.name && after.fic.name !== before.fic.name ||
        before.fic.resource_id && after.fic.resource_id !== before.fic.resource_id ||
        before.fic.plan && !sameFlatRecord(before.fic.plan, after.fic.plan) ||
        before.fic.absence && JSON.stringify(before.fic.absence) !== JSON.stringify(after.fic.absence)) throw new Error('RunInventory FIC identity changed.');
    return { ...before.fic, ...after.fic, name: before.fic.name ?? after.fic.name,
      resource_id: before.fic.resource_id ?? after.fic.resource_id, absence: before.fic.absence ?? after.fic.absence,
      state: mergeResourceState(before.fic.state, after.fic.state) };
  };
  const mergeQap = () => {
    if (!before.qap) return after.qap;
    if (!after.qap || before.qap.database !== after.qap.database || before.qap.bindings.some(old =>
      !after.qap!.bindings.some(next => next.external_table === old.external_table && next.relative_path === old.relative_path))) {
      throw new Error('RunInventory QAP identity changed.');
    }
    if (before.qap.reference && (!sameFlatRecord(before.qap.reference, after.qap.reference) ||
        before.qap.bindings.length !== after.qap.bindings.length))
      throw new Error('RunInventory QAP reference is immutable with its exact planned bindings.');
    const absence = new Map(after.qap.absence.map(receipt => [receipt.external_table + '|' + receipt.relative_path, receipt]));
    for (const receipt of before.qap.absence) {
      const next = absence.get(receipt.external_table + '|' + receipt.relative_path);
      if (!next || JSON.stringify(next) !== JSON.stringify(receipt)) throw new Error('RunInventory QAP absence receipt changed.');
    }
    for (const receipt of after.qap.absence) absence.set(receipt.external_table + '|' + receipt.relative_path, receipt);
    return { ...after.qap, bindings: [...new Map([...before.qap.bindings, ...after.qap.bindings].map(binding =>
      [binding.external_table + '|' + binding.relative_path, binding])).values()], absence: [...absence.values()],
      state: mergeResourceState(before.qap.state, after.qap.state) };
  };
  const cleanup = Object.fromEntries(Object.keys(before.cleanup).map(key => [key,
    mergeStep(before.cleanup[key as keyof RunInventory['cleanup']], after.cleanup[key as keyof RunInventory['cleanup']])])) as RunInventory['cleanup'];
  return { ...after, namespace: { ...after.namespace, uid: before.namespace.uid ?? after.namespace.uid },
    resources: [...resources.values()], fic: mergeFic(), qap: mergeQap(), cleanup };
}
function httpsManagementUrl(value: string, subscriptionId: string): string {
  let parsed: URL;
  try { parsed = new URL(value); } catch { throw new Error('ARM operation URL is invalid.'); }
  if (parsed.protocol !== 'https:' || parsed.hostname !== 'management.azure.com' || parsed.username || parsed.password ||
      parsed.hash || !parsed.pathname.toLowerCase().includes(`/subscriptions/${subscriptionId.toLowerCase()}/`)) {
    throw new Error('ARM operation URL must be an HTTPS URL for this subscription on management.azure.com.');
  }
  return parsed.toString();
}

export function hasVerifiedUnsettledDrain(state: CampaignState): boolean {
  const receipt = state.cleanupSteps.find(step => step.status === 'unsettled');
  if (!receipt) return false;
  const proof = receipt.unsettled, inventory = state.runInventory, drain = inventory?.cleanup.bounded_drain;
  if (receipt.step !== 'bounded-drain' || !proof || !inventory || !drain ||
      !exactRecord(proof, ['schemaVersion', 'campaignId', 'producerOwnerId', 'producerFencingToken', 'actorOwnerId',
        'actorFencingToken', 'orchestrationRunId', 'cellRunId', 'namespaceName', 'namespaceUid', 'deadlineUnixMs', 'observedAtUnixMs', 'reason', 'deltaRows', 'kustoRows']) ||
      !['cleaning', 'complete_unsettled'].includes(state.status) || state.mode !== 'armed' || proof.schemaVersion !== 1 ||
      proof.campaignId !== state.campaignId || proof.producerOwnerId !== inventory.owner_id ||
      proof.producerFencingToken !== inventory.fencing_token || proof.actorOwnerId !== state.ownerId ||
      proof.actorFencingToken !== state.fencingToken || inventory.fencing_token >= state.fencingToken ||
      proof.orchestrationRunId !== inventory.orchestration_run_id || proof.cellRunId !== inventory.cell_run_id ||
      proof.namespaceName !== inventory.namespace.name || !inventory.namespace.uid || proof.namespaceUid !== inventory.namespace.uid ||
      proof.deadlineUnixMs !== drain.deadline_unix_ms || !nonnegative(proof.deadlineUnixMs) ||
      proof.deadlineUnixMs <= state.startedAtMs || !nonnegative(proof.observedAtUnixMs) ||
      proof.observedAtUnixMs < proof.deadlineUnixMs || proof.observedAtUnixMs > Date.now() ||
      proof.observedAtUnixMs < (state.fencedAtMs ?? Infinity) ||
      !['failed', 'uncertain'].includes(drain.state) || drain.reason_code !== DRAIN_UNSETTLED_REASON ||
      proof.reason !== DRAIN_UNSETTLED_REASON || proof.deltaRows !== null || proof.kustoRows !== null ||
      inventory.cleanup.stop_admission.state !== 'complete' ||
      state.cleanupSteps[0]?.step !== 'stop-admission' || state.cleanupSteps[0].status !== 'complete' ||
      state.cleanupSteps.filter(step => step.status === 'unsettled').length !== 1)
    throw new Error('Terminal unsettled drain lacks exact current controller, immutable origin or expired-deadline proof.');
  return true;
}

export function validateCampaignState(value: unknown): CampaignState {
  if (!record(value) || Object.keys(value).some(key => ![
    'schemaVersion', 'campaignId', 'ownerId', 'fencingToken', 'mode', 'status', 'startedAtMs', 'heartbeatAtMs',
    'heartbeatSequence', 'expiresAtMs', 'lastEvaluatedAtMs', 'lastMissedHeartbeatCount', 'fenceReason', 'fencedAtMs',
    'cleanupStatus', 'cleanupSteps', 'runInventory', 'operations', 'scaleTrials',
  ].includes(key))) throw new Error('Campaign state has unknown fields or is not an object.');
  const state = value as unknown as CampaignState;
  if (state.schemaVersion !== 1 || !uuid.test(state.campaignId) || !uuid.test(state.ownerId) ||
      !Number.isSafeInteger(state.fencingToken) || state.fencingToken < 1 ||
      !['observe', 'armed'].includes(state.mode) || !['active', 'fenced', 'cleaning', 'complete', 'complete_unsettled'].includes(state.status) ||
      !nonnegative(state.startedAtMs) || !nonnegative(state.heartbeatAtMs) || state.heartbeatAtMs < state.startedAtMs ||
      !Number.isSafeInteger(state.heartbeatSequence) || state.heartbeatSequence < 0 ||
      state.expiresAtMs !== state.startedAtMs + MAX_CAMPAIGN_LIFETIME_MS ||
      !['not-started', 'pending', 'running', 'verified', 'verified_with_unsettled'].includes(state.cleanupStatus) ||
      !Array.isArray(state.operations) || state.operations.length > MAX_ARM_OPERATIONS || !Array.isArray(state.cleanupSteps) ||
      state.scaleTrials !== undefined && (!Array.isArray(state.scaleTrials) || state.scaleTrials.length > 64))  {
    throw new Error('Campaign state is malformed or violates the fixed custody limits.');
  }
  if (state.lastEvaluatedAtMs !== undefined && !nonnegative(state.lastEvaluatedAtMs) ||
      state.lastMissedHeartbeatCount !== undefined && (!Number.isSafeInteger(state.lastMissedHeartbeatCount) || state.lastMissedHeartbeatCount < 0) ||
      state.fencedAtMs !== undefined && !nonnegative(state.fencedAtMs) ||
      state.fenceReason !== undefined && !['heartbeat-expired', 'max-lifetime'].includes(state.fenceReason)) {
    throw new Error('Campaign evaluation receipt is malformed.');
  }
  const expectedCleanupSteps = state.status === "active" ? [] : [...CUSTODY_CLEANUP_STEPS];
  if (state.cleanupSteps.length !== expectedCleanupSteps.length || state.cleanupSteps.some((receipt, index) => !record(receipt) ||
      receipt.step !== expectedCleanupSteps[index] || receipt.idempotencyKey !== state.campaignId + ":" + state.fencingToken + ":" + receipt.step ||
      !["pending", "running", "complete", "unsettled"].includes(receipt.status) ||
      !exactRecord(receipt, receipt.status === 'unsettled' ? ['step', 'idempotencyKey', 'status', 'unsettled'] :
        ['step', 'idempotencyKey', 'status']))) throw new Error("Campaign cleanup receipts are malformed.");
  if (state.status === "active" && state.cleanupStatus !== "not-started" || state.status === "fenced" && state.cleanupStatus !== "pending" ||
      state.status === "cleaning" && state.cleanupStatus !== "running" || state.status === "complete" &&
      (state.cleanupStatus !== "verified" || state.cleanupSteps.some(receipt => receipt.status !== "complete"))) {
    throw new Error("Campaign cleanup state does not match its durable receipts.");
  }
  let unfinishedCleanup = false;
  let runningCleanup = false;
  for (const receipt of state.cleanupSteps) {
    if (['complete', 'unsettled'].includes(receipt.status) && unfinishedCleanup) throw new Error('Campaign cleanup receipts are out of order.');
    if (receipt.status === 'running') {
      if (runningCleanup) throw new Error('Campaign has multiple running cleanup receipts.');
      runningCleanup = true;
    }
    if (!['complete', 'unsettled'].includes(receipt.status)) unfinishedCleanup = true;
  }
  if (state.runInventory !== undefined) {
    validateRunInventory(state.runInventory, state);
    if (state.status === 'active' && state.runInventory.fencing_token !== state.fencingToken ||
        state.status !== 'active' && state.runInventory.fencing_token >= state.fencingToken) {
      throw new Error('RunInventory producer fencing identity does not match the campaign lifecycle.');
    }
  }
  if (state.mode === 'armed' && !state.runInventory || state.status !== 'active' && !state.runInventory) {
    throw new Error('Campaign cannot be armed or fenced without a persisted RunInventory.');
  }
  const scaleTrials = state.scaleTrials ?? [];
  if (!Array.isArray(scaleTrials) || scaleTrials.length > 64) throw new Error('Scale trial journal exceeds its fixed receipt bound.');
  const trialIds = new Set<string>();
  let unresolvedTrials = 0;
  for (const trial of scaleTrials) {
    if (!validScaleTrial(trial, state) || trialIds.has(trial.trial_id)) throw new Error('Scale trial receipt is malformed or duplicated.');
    trialIds.add(trial.trial_id);
    if (['apply-intent', 'apply-dispatch', 'applied', 'restore-intent', 'restore-dispatch', 'unknown'].includes(trial.phase)) unresolvedTrials++;
  }
  if (unresolvedTrials > 1) throw new Error('Campaign has multiple unresolved scale trials.');
  if (['complete', 'complete_unsettled'].includes(state.status) && scaleTrials.some(trial => !['restored', 'restore-stopped', 'reverted'].includes(trial.phase)))
    throw new Error('Campaign cannot finalize while scale effects are not restored.');
  const ids = new Set<string>();
  for (const operation of state.operations) {
    if (!record(operation) || !uuid.test(operation.id) || ids.has(operation.id) ||
        typeof operation.resourceId !== 'string' || !operation.resourceId.startsWith(`/subscriptions/`) ||
        typeof operation.operationUrl !== 'string' || !nonnegative(operation.submittedAtMs) ||
        !['pending', 'succeeded', 'failed', 'indeterminate'].includes(operation.status) ||
        operation.lastCheckedAtMs !== undefined && !nonnegative(operation.lastCheckedAtMs) ||
        operation.cleanupTarget !== undefined && !validCleanupArmTarget(operation.cleanupTarget)) {
      throw new Error('ARM operation receipt is malformed.');
    }
    const operationSubscription = /^\/subscriptions\/([0-9a-f-]{36})\//i.exec(operation.resourceId)?.[1];
    if (!operationSubscription) throw new Error('ARM operation resource ID has no subscription scope.');
    httpsManagementUrl(operation.operationUrl, operationSubscription);
    ids.add(operation.id);
  }
  if (state.status === 'active' && state.fenceReason !== undefined ||
      state.status !== 'active' && (!state.fenceReason || state.fencedAtMs === undefined || state.fencingToken < 2)) {
    throw new Error('Campaign terminal state is missing its fencing receipt.');
  }
  if (state.status === 'complete' && state.cleanupStatus !== 'verified') throw new Error('Campaign cannot complete before cleanup verification.');
  const unsettled = hasVerifiedUnsettledDrain(state);
  if (state.status === 'complete_unsettled') {
    const inventory = state.runInventory!;
    if (!unsettled || state.cleanupStatus !== 'verified_with_unsettled' ||
        state.cleanupSteps.some(step => !['complete', 'unsettled'].includes(step.status)) ||
        inventory.cleanup.handoff.state !== 'failed' || inventory.cleanup.handoff.reason_code !== DRAIN_UNSETTLED_REASON ||
        Object.entries(inventory.cleanup).some(([name, step]) => !['bounded_drain', 'handoff'].includes(name) && step.state !== 'complete') ||
        inventory.resources.some(resource => !['removed', 'absent'].includes(resource.state)) ||
        inventory.fic && !['removed', 'absent'].includes(inventory.fic.state) ||
        inventory.qap && !['removed', 'absent'].includes(inventory.qap.state))
      throw new Error('Unsettled campaign cannot finalize without failed-cell evidence and every other cleanup/safe-idle proof.');
  } else if (state.cleanupStatus === 'verified_with_unsettled') throw new Error('Unsettled verification requires its distinct terminal campaign state.');
  if (['complete', 'complete_unsettled'].includes(state.status) &&
      state.operations.some(operation => ['pending', 'indeterminate'].includes(operation.status)))
    throw new Error('Campaign cannot finalize while ARM operations remain unresolved.');
  return state;
}

export function newCampaignState(campaignId: string, ownerId: string, now = Date.now()): CampaignState {
  if (!uuid.test(campaignId) || !uuid.test(ownerId) || !nonnegative(now) || !Number.isSafeInteger(now + MAX_CAMPAIGN_LIFETIME_MS))
    throw new Error('Campaign and owner IDs must be UUIDs and the clock must be valid.');
  return { schemaVersion: 1, campaignId, ownerId, fencingToken: 1, mode: 'observe', status: 'active',
    startedAtMs: now, heartbeatAtMs: now, heartbeatSequence: 0, expiresAtMs: now + MAX_CAMPAIGN_LIFETIME_MS,
    cleanupStatus: 'not-started', cleanupSteps: [], operations: [] };
}

export interface CampaignStartReceipt {
  status: 'started'; campaign_id: string; owner_id: string; fencing_token: number; expires_at_ms: number;
  environment: { CAMPAIGN_ID: string; CAMPAIGN_OWNER_ID: string; CAMPAIGN_FENCING_TOKEN: string };
}
export function campaignStartReceipt(state: CampaignState): CampaignStartReceipt {
  return { status: 'started', campaign_id: state.campaignId, owner_id: state.ownerId, fencing_token: state.fencingToken,
    expires_at_ms: state.expiresAtMs, environment: { CAMPAIGN_ID: state.campaignId, CAMPAIGN_OWNER_ID: state.ownerId,
      CAMPAIGN_FENCING_TOKEN: String(state.fencingToken) } };
}
export function assertCampaignEnvironment(env: NodeJS.ProcessEnv, state: CampaignState): void {
  if (env.CAMPAIGN_ID !== state.campaignId || env.CAMPAIGN_OWNER_ID !== state.ownerId ||
      env.CAMPAIGN_FENCING_TOKEN !== String(state.fencingToken)) {
    throw new CampaignFencedError('Campaign environment identity is missing or does not match the durable fence.');
  }
}
export class CampaignConflictError extends Error {
  constructor() { super('Campaign state changed concurrently; retry using a fresh read.'); this.name = 'CampaignConflictError'; }
}
export function cleanupFailureCode(error: unknown): 'stale_version' | 'write_outcome_unknown' | 'campaign_fenced' | 'invalid_or_storage_error' {
  if (error instanceof CampaignWriteOutcomeUnknownError) return 'write_outcome_unknown';
  if (error instanceof CampaignConflictError) return 'stale_version';
  if (error instanceof CampaignFencedError || error instanceof Error && /fenced|stale|heartbeat is expired/i.test(error.message))
    return 'campaign_fenced';
  return 'invalid_or_storage_error';
}
export interface CampaignStoreConfig { account: string; container: string; subscriptionId: string; recordPath?: string }

export async function managedIdentityToken(resource: string, env: NodeJS.ProcessEnv = process.env, fetcher: Fetcher = fetch): Promise<string> {
  const endpoint = env.IDENTITY_ENDPOINT;
  const header = env.IDENTITY_HEADER;
  if (!endpoint || !header || !env.AZURE_CLIENT_ID) throw new Error('Container Apps managed identity endpoint is unavailable.');
  const url = new URL(endpoint);
  url.searchParams.set('api-version', '2019-08-01');
  url.searchParams.set('resource', resource);
  url.searchParams.set('client_id', env.AZURE_CLIENT_ID);
  const options: RequestInit = { headers: { 'X-IDENTITY-HEADER': header } };
  if (isEffectBoundaryActive()) options.redirect = 'error';
  const response = await dispatchExternalEffect({ transport: 'azure-auth', method: 'GET', resource: url.origin,
    path: url.pathname + url.search }, () => fetcher(url, options));
  if (!response.ok) throw new Error(`Managed identity token request failed with HTTP ${response.status}.`);
  const body: unknown = await response.json();
  if (!record(body) || typeof body.access_token !== 'string' || !body.access_token) throw new Error('Managed identity returned an invalid token response.');
  return body.access_token;
}

async function localToken(resource: string, subscriptionId: string): Promise<string> {
  const args = ['account', 'get-access-token', '--subscription', subscriptionId,
    '--resource', resource, '--query', 'accessToken', '--output', 'tsv', '--only-show-errors'];
  const result = await dispatchExternalEffect({ transport: 'process', method: 'AZURE_ACCOUNT_GET-ACCESS-TOKEN',
    resource: 'az', path: JSON.stringify(args) }, async () => spawnSync('az', args,
    { encoding: 'utf8', timeout: 30_000, windowsHide: true }));
  const token = typeof result.stdout === 'string' ? result.stdout.trim() : '';
  if (result.error || result.status !== 0 || !token || /[[:space:]]/.test(token))
    throw new Error('Azure CLI could not provide a token for the campaign store.');
  return token;
}

export function createBlobCampaignStore(config: CampaignStoreConfig, fetcher: Fetcher = fetch,
  token: TokenProvider = resource => getAccessToken(resource, config.subscriptionId, process.env, fetcher)): CampaignStore {
  const recordPath = config.recordPath ?? 'active.json';
  const segments = recordPath.split('/');
  if (!storageName.test(config.account) || !containerName.test(config.container) || !uuid.test(config.subscriptionId)) {
    throw new Error('Campaign store requires a storage account, container and subscription ID.');
  }
  if (recordPath.length > 1024 || segments.some(segment => !/^[A-Za-z0-9._-]+$/.test(segment) || segment === '.' || segment === '..'))
    throw new Error('Campaign store record path is invalid.');
  const endpoint = `https://${config.account}.blob.core.windows.net/${config.container}/` +
    segments.map(segment => encodeURIComponent(segment)).join('/');
  const blobFetch: Fetcher = (input, init = {}) => {
    const url = new URL(String(input)), method = (init.method ?? 'GET').toUpperCase();
    const body = typeof init.body === 'string' || Buffer.isBuffer(init.body) ? init.body : undefined;
    const requestInit: RequestInit = isEffectBoundaryActive() ? { ...init, redirect: 'error' } : init;
    return dispatchExternalEffect({ transport: 'campaign-blob', method, resource: url.origin + '/' + config.container,
      path: url.pathname + url.search, body }, () => fetcher(input, requestInit));
  };
  const headers = async () => ({ authorization: `Bearer ${await token('https://storage.azure.com/')}`,
    'x-ms-date': new Date().toUTCString(), 'x-ms-version': '2023-11-03' });
  return {
    async read() {
      const response = await blobFetch(endpoint, { headers: await headers() });
      if (response.status === 404) return undefined;
      if (!response.ok) throw new Error(`Campaign state read failed with HTTP ${response.status}.`);
      const etag = response.headers.get('etag');
      if (!etag) throw new Error('Campaign state response has no ETag.');
      let value: unknown;
      try { value = await response.json(); } catch { throw new Error('Campaign state blob is not valid JSON.'); }
      return { state: validateCampaignState(value), etag };
    },
    async write(state, expectedEtag) {
      const condition: Record<string, string> = expectedEtag ? { 'if-match': expectedEtag } : { 'if-none-match': '*' };
      const response = await blobFetch(endpoint, { method: 'PUT', headers: { ...(await headers()), ...condition,
        'content-type': 'application/json', 'content-length': String(Buffer.byteLength(JSON.stringify(state))),
        'x-ms-blob-type': 'BlockBlob' }, body: JSON.stringify(validateCampaignState(state)) });
      if (response.status === 412) throw new CampaignConflictError();
      if (!response.ok) throw new Error(`Campaign state write failed with HTTP ${response.status}.`);
      const etag = response.headers.get('etag');
      if (!etag) throw new Error('Campaign state write response has no ETag.');
      return etag;
    },
  };
}

export async function mutateCampaign(store: CampaignStore, change: (state: CampaignState | undefined) => CampaignState,
  retries = 5): Promise<StoredCampaign> {
  if (!Number.isSafeInteger(retries) || retries < 1 || retries > 10) throw new Error('Campaign CAS retry bound must be 1..10.');
  for (let attempt = 0; attempt < retries; attempt++) {
    const current = await store.read();
    const next = validateCampaignState(change(current?.state));
    try { return { state: next, etag: await store.write(next, current?.etag) }; }
    catch (error) { if (!(error instanceof CampaignConflictError) || attempt === retries - 1) throw error; }
  }
  throw new Error('Campaign state update exhausted its bounded CAS retries.');
}

function requireOwner(state: CampaignState | undefined, ownerId: string, fencingToken: number, now: number): CampaignState {
  if (!state || state.status !== 'active' || state.ownerId !== ownerId || state.fencingToken !== fencingToken) {
    throw new Error('Campaign owner is fenced, stale or does not match the active run.');
  }
  if (!nonnegative(now) || now < state.heartbeatAtMs || now >= state.expiresAtMs ||
      Math.floor((now - state.heartbeatAtMs) / HEARTBEAT_INTERVAL_MS) >= MISSED_HEARTBEAT_LIMIT) {
    throw new Error('Campaign owner heartbeat is expired; mutation rejected.');
  }
  return state;
}
export async function startCampaign(store: CampaignStore, campaignId: string, ownerId = campaignId, now = Date.now()): Promise<StoredCampaign> {
  return mutateCampaign(store, current => {
    if (current && !['complete', 'complete_unsettled'].includes(current.status)) throw new Error('An unfinished campaign still owns the stamp.');
    return newCampaignState(campaignId, ownerId, now);
  });
}
export class CampaignFencedError extends Error {
  constructor(message = 'Campaign owner is fenced, stale or does not match the durable fencing token.') { super(message); this.name = 'CampaignFencedError'; }
}
export class CampaignWriteOutcomeUnknownError extends Error {
  constructor() { super('RunInventory write outcome could not be confirmed by a strong readback.'); this.name = 'CampaignWriteOutcomeUnknownError'; }
}
export async function compareAndSwapRunInventory(store: CampaignStore, expected: StoredCampaign,
  value: RunInventory, assertCurrent: () => Promise<void>): Promise<StoredCampaign> {
  await assertCurrent();
  const current = await store.read();
  if (!current || current.etag !== expected.etag || JSON.stringify(current.state.runInventory) !== JSON.stringify(expected.state.runInventory))
    throw new CampaignConflictError();
  const merged = mergeRunInventory(current.state.runInventory, validateRunInventory(value, current.state));
  const next = validateCampaignState({ ...current.state, runInventory: merged });
  let etag: string;
  try { etag = await store.write(next, current.etag); }
  catch (error) {
    if (error instanceof CampaignConflictError) throw error;
    throw new CampaignWriteOutcomeUnknownError();
  }
  let confirmed: StoredCampaign | undefined;
  try { confirmed = await store.read(); } catch { throw new CampaignWriteOutcomeUnknownError(); }
  if (!etag || !confirmed || confirmed.etag !== etag || JSON.stringify(confirmed.state) !== JSON.stringify(next))
    throw new CampaignWriteOutcomeUnknownError();
  await assertCurrent();
  return confirmed;
}

async function writeInventoryWithReadback(store: CampaignStore, current: StoredCampaign, nextState: CampaignState,
  merged: RunInventory, expectedStatus: CampaignStatus, expectedFencingToken: number): Promise<StoredCampaign | undefined> {
  try { return { state: nextState, etag: await store.write(nextState, current.etag) }; }
  catch (error) {
    if (error instanceof CampaignConflictError) return undefined;
    let readback: StoredCampaign | undefined;
    try { readback = await store.read(); } catch { throw new CampaignWriteOutcomeUnknownError(); }
    if (!readback) throw new CampaignWriteOutcomeUnknownError();
    if (readback.state.status !== expectedStatus || readback.state.fencingToken !== expectedFencingToken) throw new CampaignFencedError();
    if (JSON.stringify(readback.state.runInventory) === JSON.stringify(merged)) return readback;
    throw new CampaignWriteOutcomeUnknownError();
  }
}
function runInventoryReady(inventory: RunInventory | undefined): inventory is RunInventory {
  return Boolean(inventory && inventory.load_admission === 'authorized' && nonnegative(inventory.planned_cell_duration_ms) &&
    inventory.planned_cell_duration_ms > 0 && inventory.effective_stop_budgets && inventory.namespace.uid && inventory.resources.length > 0 &&
    inventory.resources.every(resource => resource.state === 'observed' && resource.uid) &&
    inventory.fic !== null && inventory.fic.state === 'observed' && inventory.qap !== null && inventory.qap.state === 'observed' && inventory.qap.bindings.length > 0 &&
    inventory.cleanup.handoff.state !== 'complete');
}
export async function publishRunInventory(store: CampaignStore, value: unknown, now = Date.now()): Promise<StoredCampaign> {
  for (let attempt = 0; attempt < 5; attempt++) {
    const current = await store.read();
    if (!current) throw new Error('No active campaign state exists.');
    if (current.state.status !== 'active') throw new CampaignFencedError('Producer inventory updates are blocked after campaign fencing.');
    const inventory = validateRunInventory(value, current.state);
    requireOwner(current.state, inventory.owner_id, inventory.fencing_token, now);
    const merged = mergeRunInventory(current.state.runInventory, inventory);
    const previous = current.state.runInventory;
    const newCell = !previous || previous.campaign_id !== inventory.campaign_id || previous.owner_id !== inventory.owner_id ||
      previous.orchestration_run_id !== inventory.orchestration_run_id || previous.cell_run_id !== inventory.cell_run_id;
    const authorizingClients = inventory.load_admission === 'authorized' && previous?.load_admission !== 'authorized';
    if (newCell || authorizingClients) assertCellLifetimeBudget(current.state, merged, now);
    const next = validateCampaignState({ ...current.state, runInventory: merged });
    const written = await writeInventoryWithReadback(store, current, next, merged, 'active', current.state.fencingToken);
    if (written) return written;
  }
  function assertCellLifetimeBudget(state: CampaignState, inventory: RunInventory, now: number): void {
    const duration = inventory.planned_cell_duration_ms;
    const stopTimeout = inventory.effective_stop_budgets?.maintenance_stop_timeout_ms;
    if (!nonnegative(now) || now < state.startedAtMs || !nonnegative(duration) || !nonnegative(stopTimeout))
      throw new Error('Cell admission requires explicit planned duration and persisted maintenance stop budget.');
    const remaining = state.expiresAtMs - now;
    const assessment = assessCellLifetimeBudget(remaining, duration, stopTimeout);
    if (assessment.status === 'invalid') throw new Error(assessment.reason);
    if (assessment.status === 'insufficient')
      throw new Error('Campaign lifetime cannot cover the planned cell, maintenance stop, and bounded cleanup reserves.');
    if (assessment.status === 'unknown') console.warn(JSON.stringify({ event: 'cell_cleanup_lifetime_reserve',
      status: assessment.status, lower_bound_required_ms: assessment.required_ms, uncovered_steps: UNBOUNDED_CLEANUP_RESERVE_STEPS }));
  }
  throw new CampaignConflictError();
}

/** Controller-only inventory writes retain the producer's immutable origin token and require the current cleanup fence. */
export async function publishCleanupRunInventory(store: CampaignStore, value: unknown, ownerId: string,
  cleanupFencingToken: number): Promise<StoredCampaign> {
  for (let attempt = 0; attempt < 5; attempt++) {
    const current = await store.read();
    if (!current || current.state.status !== 'cleaning' || current.state.ownerId !== ownerId ||
        current.state.fencingToken !== cleanupFencingToken || !current.state.runInventory) throw new CampaignFencedError();
    const inventory = validateRunInventory(value, current.state);
    if (inventory.owner_id !== ownerId || inventory.fencing_token !== current.state.runInventory.fencing_token) throw new CampaignFencedError();
    const merged = mergeRunInventory(current.state.runInventory, inventory);
    const next = validateCampaignState({ ...current.state, runInventory: merged });
    const written = await writeInventoryWithReadback(store, current, next, merged, 'cleaning', cleanupFencingToken);
    if (written) return written;
  }
  throw new CampaignConflictError();
}
export interface RunInventoryPublishReceipt {
  status: 'persisted'; campaign_id: string; owner_id: string; fencing_token: number; orchestration_run_id: string;
  cell_run_id: string; namespace_uid: string | null; resource_count: number;
  fic_state: RunInventoryResourceState | null; qap_state: RunInventoryResourceState | null; etag: string;
}
export function runInventoryPublishReceipt(stored: StoredCampaign): RunInventoryPublishReceipt {
  const inventory = stored.state.runInventory;
  if (!inventory) throw new Error('Persisted campaign has no RunInventory.');
  return { status: 'persisted', campaign_id: inventory.campaign_id, owner_id: inventory.owner_id,
    fencing_token: inventory.fencing_token, orchestration_run_id: inventory.orchestration_run_id,
    cell_run_id: inventory.cell_run_id, namespace_uid: inventory.namespace.uid,
    resource_count: inventory.resources.length, fic_state: inventory.fic?.state ?? null,
    qap_state: inventory.qap?.state ?? null, etag: stored.etag };
}
export async function assertRunInventoryReady(store: CampaignStore, ownerId: string, fencingToken: number,
  now = Date.now()): Promise<CampaignState> {
  const stored = await store.read();
  const state = requireOwner(stored?.state, ownerId, fencingToken, now);
  if (state.mode !== 'armed' || !runInventoryReady(state.runInventory)) {
    throw new Error('Load is blocked until every planned RunInventory identity is observed and owned.');
  }
  validateRunInventory(state.runInventory, state);
  return state;
}

export async function heartbeatCampaign(store: CampaignStore, ownerId: string, fencingToken: number, now = Date.now()): Promise<StoredCampaign> {
  return mutateCampaign(store, current => {
    const state = requireOwner(current, ownerId, fencingToken, now);
    if (!nonnegative(now) || now >= state.expiresAtMs) throw new Error('Campaign maximum lifetime has expired; heartbeat rejected.');
    if (now < state.heartbeatAtMs) throw new Error('Campaign heartbeat clock moved backwards.');
    return { ...state, heartbeatAtMs: now, heartbeatSequence: state.heartbeatSequence + 1 };
  });
}
export async function armCampaign(store: CampaignStore, ownerId: string, fencingToken: number, now = Date.now()): Promise<StoredCampaign> {
  return mutateCampaign(store, current => {
    const state = requireOwner(current, ownerId, fencingToken, now);
    if (state.mode !== 'observe') throw new Error('Campaign is already armed.');
    if (!state.runInventory) throw new Error('RunInventory must be persisted before arming.');
    if (!nonnegative(state.runInventory.planned_cell_duration_ms) || !state.runInventory.effective_stop_budgets)
      throw new Error('Campaign cannot be armed without a new-cell duration and maintenance stop budget.');
    return { ...state, mode: 'armed' };
  });
}
export async function setCleanupVerified(store: CampaignStore, ownerId: string, fencingToken: number): Promise<StoredCampaign> {
  return mutateCampaign(store, current => {
    if (!current || current.status !== 'cleaning' || current.ownerId !== ownerId || current.fencingToken !== fencingToken ||
        current.cleanupStatus !== 'running' || current.cleanupSteps.some(step => !['complete', 'unsettled'].includes(step.status)))
      throw new Error('Cleanup verification requires the fenced owner and a running cleanup.');
    const unsettled = hasVerifiedUnsettledDrain(current);
    return { ...current, status: unsettled ? 'complete_unsettled' : 'complete',
      cleanupStatus: unsettled ? 'verified_with_unsettled' : 'verified' };
  });
}
export async function recordArmOperation(store: CampaignStore, ownerId: string, fencingToken: number,
  operation: Omit<ArmOperationReceipt, 'status'>, subscriptionId: string, now = Date.now()): Promise<StoredCampaign> {
  const operationUrl = httpsManagementUrl(operation.operationUrl, subscriptionId);
  return mutateCampaign(store, current => {
    const state = requireOwner(current, ownerId, fencingToken, now);
    if (state.mode !== 'armed' || !runInventoryReady(state.runInventory)) throw new Error('ARM mutation is blocked until RunInventory identities are observed.');
    validateRunInventory(state.runInventory, state);
    if (!uuid.test(operation.id) || state.operations.some(existing => existing.id === operation.id) ||
        !operation.resourceId.toLowerCase().startsWith(`/subscriptions/${subscriptionId.toLowerCase()}/`) ||
        !nonnegative(operation.submittedAtMs) || state.operations.length >= MAX_ARM_OPERATIONS) {
      throw new Error('ARM operation receipt is invalid, duplicate, out of scope or exceeds the receipt bound.');
    }
    return { ...state, operations: [...state.operations, { ...operation, operationUrl, status: 'pending' }] };
  });
}

export async function recordCleanupArmOperation(store: CampaignStore, ownerId: string, fencingToken: number,
  operation: Omit<ArmOperationReceipt, 'status'> & { cleanupTarget: CleanupArmTarget }, subscriptionId: string): Promise<StoredCampaign> {
  const operationUrl = httpsManagementUrl(operation.operationUrl, subscriptionId);
  return mutateCampaign(store, current => {
    if (!current || current.status !== 'cleaning' || current.ownerId !== ownerId || current.fencingToken !== fencingToken ||
        !validCleanupArmTarget(operation.cleanupTarget) || current.operations.some(existing => existing.id === operation.id) ||
        current.operations.length >= MAX_ARM_OPERATIONS ||
        !operation.resourceId.toLowerCase().startsWith(`/subscriptions/${subscriptionId.toLowerCase()}/`) ||
        new URL(operationUrl).pathname.toLowerCase() !== operation.resourceId.toLowerCase())
      throw new CampaignFencedError('Cleanup ARM intent requires current cleaning authority and an exact resource URL.');
    return { ...current, operations: [...current.operations, { ...operation, operationUrl, status: 'pending' }] };
  });
}

async function readRunInventoryStdin(): Promise<unknown> {
  let input = '';
  for await (const chunk of process.stdin) {
    input += chunk.toString();
    if (Buffer.byteLength(input) > 1024 * 1024) throw new Error('RunInventory input exceeds the 1 MiB limit.');
  }
  try { return JSON.parse(input) as unknown; } catch { throw new Error('RunInventory stdin is not valid JSON.'); }
}

export async function campaignMain(args: string[], dependencies: {
  store?: CampaignStore; azure?: import('../../infra/azure/bootstrap.js').Azure; env?: NodeJS.ProcessEnv; cleanupEnvelope?: unknown;
  runInventoryInput?: unknown;
} = {}): Promise<void> {
  if (args[0] === 'cleanup-step') {
    const cell = await import('./aks-cell-cleanup.js');
    if (args[1] && !cell.isCellCleanupSelector(args[1])) throw new Error('Unknown cleanup-step selector.');
  }
  const selected = loadCustodyConfig(args, false, dependencies.env);
  args = selected.positionals;
  const [action, campaignId, fencingTokenText] = args;
  const subscriptionId = selected.config.subscription, env = selected.env;
  const store = dependencies.store ?? createBlobCampaignStore({ account: selected.config.account,
    container: selected.config.checkpoints, subscriptionId });
  const { Azure } = await import('../../infra/azure/bootstrap.js');
  const azure = dependencies.azure ?? new Azure(selected.config);
  const requireBaseline = async (current: StoredCampaign) => {
    const { readCleanupBaseline } = await import('./aks-default-providers.js');
    await readCleanupBaseline(azure, current.state);
    if ((await store.read())?.etag !== current.etag) throw new CampaignConflictError();
  };
  if (action === 'start' && (args.length === 2 || args.length === 3)) {
    const result = await startCampaign(store, campaignId!, args[2] ?? campaignId!);
    console.log(JSON.stringify({ ok: true, receipt: campaignStartReceipt(result.state) }));
  } else if (action === 'heartbeat' && args.length === 3 && /^\d+$/.test(fencingTokenText!)) {
    const state = await store.read();
    if (!state) throw new Error('No active campaign state exists.');
    if (campaignId !== state.state.ownerId) throw new CampaignFencedError();
    assertCampaignEnvironment(env, state.state);
    const result = await heartbeatCampaign(store, state.state.ownerId, Number(fencingTokenText), Date.now());
    console.log(JSON.stringify({ campaignId: result.state.campaignId, fencingToken: result.state.fencingToken,
      heartbeatSequence: result.state.heartbeatSequence, heartbeatAtMs: result.state.heartbeatAtMs }));
  } else if (action === 'arm' && args.length === 3 && /^\d+$/.test(fencingTokenText!)) {
    const current = await store.read();
    if (!current) throw new Error('No active campaign state exists.');
    if (campaignId !== current.state.ownerId) throw new CampaignFencedError();
    assertCampaignEnvironment(env, current.state);
    await requireBaseline(current);
    const result = await armCampaign(store, current.state.ownerId, Number(fencingTokenText));
    console.log(JSON.stringify({ campaignId: result.state.campaignId, mode: result.state.mode }));
  } else if (action === 'publish-inventory' && args.length === 1) {
    const input = dependencies.runInventoryInput ?? await readRunInventoryStdin();
    if (!record(input)) throw new Error('RunInventory input must be an object.');
    const current = await store.read();
    if (!current) throw new Error('No active campaign state exists.');
    assertCampaignEnvironment(env, current.state);
    const inventory = validateRunInventory(input, current.state);
    if (!isInitialRunInventory(mergeRunInventory(current.state.runInventory, inventory))) await requireBaseline(current);
    const result = await publishRunInventory(store, input);
    console.log(JSON.stringify({ ok: true, receipt: runInventoryPublishReceipt(result) }));
  } else if (action === 'assert-ready' && args.length === 3 && /^\d+$/.test(fencingTokenText!)) {
    const current = await store.read();
    if (!current) throw new Error('No active campaign state exists.');
    if (campaignId !== current.state.ownerId) throw new CampaignFencedError();
    assertCampaignEnvironment(env, current.state);
    await requireBaseline(current);
    const state = await assertRunInventoryReady(store, current.state.ownerId, Number(fencingTokenText));
    console.log(JSON.stringify({ ok: true, receipt: { status: 'ready', campaign_id: state.campaignId,
      owner_id: state.ownerId, fencing_token: state.fencingToken, orchestration_run_id: state.runInventory!.orchestration_run_id,
      cell_run_id: state.runInventory!.cell_run_id, namespace_uid: state.runInventory!.namespace.uid } }));
  } else if (action === 'status' && args.length === 1) {
    console.log(JSON.stringify((await store.read())?.state ?? { status: 'idle' }));
  } else if (action === 'read-inventory' && args.length === 1) {
    const { readCellRunInventory } = await import('./aks-cell-cleanup.js');
    console.log(JSON.stringify(await readCellRunInventory(store, await readRunInventoryStdin(), env)));
  } else if (action === 'capture-baseline' && args.length === 3 && /^\d+$/.test(fencingTokenText!)) {
    const current = await store.read();
    if (!current || campaignId !== current.state.ownerId) throw new CampaignFencedError();
    assertCampaignEnvironment(env, current.state);
    const { captureCleanupBaseline } = await import('./aks-default-providers.js');
    const log = console.log;
    let receipt: import('./aks-default-providers.js').CampaignBaselineReceipt;
    try {
      console.log = (...values: unknown[]) => console.error(...values);
      receipt = await captureCleanupBaseline(store, azure, campaignId!, Number(fencingTokenText));
    } finally { console.log = log; }
    console.log(JSON.stringify({ ok: true, receipt }));
  } else if (action === 'capture-cell-baseline' && args.length === 1) {
    const current = await store.read();
    if (!current) throw new CampaignFencedError();
    assertCampaignEnvironment(env, current.state);
    await requireBaseline(current);
    const { captureCellBaseline } = await import('./aks-data-safety.js');
    const input = await readRunInventoryStdin(), log = console.log;
    let receipt: import('./aks-data-safety.js').CellBaselineReceipt;
    try {
      console.log = (...values: unknown[]) => console.error(...values);
      receipt = await captureCellBaseline(store, azure, input);
    } finally { console.log = log; }
    console.log(JSON.stringify({ ok: true, receipt }));
  } else if (action === 'cleanup-step' && args.length === 2) {
    const cell = await import('./aks-cell-cleanup.js');
    const cleanupSelector = args[1];
    if (!cleanupSelector || !cell.isCellCleanupSelector(cleanupSelector)) throw new Error('Unknown cleanup-step selector.');
    const input = cell.parseCellCleanupEnvelope(dependencies.cleanupEnvelope ?? await readRunInventoryStdin());
    await cell.readCellRunInventory(store, { schema_version: 1, campaign_id: input.inventory.campaign_id,
      orchestration_run_id: input.inventory.orchestration_run_id, cell_run_id: input.inventory.cell_run_id, actor: input.actor }, env);
    const { createDefaultCleanupProviders, createDefaultCellCleanupProviders } = await import('./aks-default-providers.js');
    const fs = await import('node:fs'), os = await import('node:os');
    const workDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'oat-cell-cleanup-'));
    const actions = createDefaultCleanupProviders(store, { azure, workDirectory });
    const providers = createDefaultCellCleanupProviders(actions);
    const log = console.log;
    let receipt: import('./aks-cell-cleanup.js').CellCleanupReceipt;
    try {
      console.log = (...values: unknown[]) => console.error(...values);
      receipt = await cell.executeCellCleanupStep(store, input, cleanupSelector,
        providers, assertCurrent => cell.createBlobCellStepJournal(azure, input, assertCurrent));
    } finally {
      console.log = log;
      fs.rmSync(workDirectory, { recursive: true });
    }
    console.log(JSON.stringify({ ok: true, receipt }));
  } else {
    throw new Error('Usage: aks-campaign.ts <start|heartbeat|capture-baseline|arm|publish-inventory|assert-ready|read-inventory|capture-cell-baseline|cleanup-step|status> [arguments] --env-file PATH --stamp-config YAML');
  }
}
if (process.argv[1] && ['aks-campaign.ts', 'aks-campaign.js'].includes(path.basename(process.argv[1])) && import.meta.url === pathToFileURL(process.argv[1]).href) {
  campaignMain(process.argv.slice(2)).catch((error: unknown) => {
    const jsonActions = ['publish-inventory', 'assert-ready', 'cleanup-step', 'read-inventory', 'capture-cell-baseline', 'capture-baseline'];
    if (process.argv.slice(2).some(action => jsonActions.includes(action))) {
      const message = error instanceof Error ? error.message : 'Campaign custody failed.';
      const code = cleanupFailureCode(error);
      console.log(JSON.stringify({ ok: false, code, message }));
    } else console.error(error instanceof Error ? error.message : 'Campaign custody failed');
    process.exitCode = 1;
  });
}

export async function readArmOperationStatus(operationUrl: string, subscriptionId: string, fetcher: Fetcher = fetch,
  env: NodeJS.ProcessEnv = process.env): Promise<{ status: number; body?: unknown }> {
  const url = new URL(httpsManagementUrl(operationUrl, subscriptionId));
  const accessToken = await getAccessToken('https://management.azure.com/', subscriptionId, env, fetcher);
  const options: RequestInit = { headers: { authorization: `Bearer ${accessToken}` }, signal: AbortSignal.timeout(10_000) };
  if (isEffectBoundaryActive()) options.redirect = 'error';
  const response = await dispatchExternalEffect({ transport: 'azure-rest', method: 'GET',
    resource: url.origin + '|https://management.azure.com/', path: url.pathname + url.search }, () => fetcher(url, options));
  if (response.status === 202) return { status: 202 };
  const text = await response.text();
  if (!text) return { status: response.status };
  let body: unknown;
  try { body = JSON.parse(text) as unknown; } catch { throw new Error('ARM operation status response is not valid JSON.'); }
  return { status: response.status, body };
}

export async function getAccessToken(resource: string, subscriptionId: string, env: NodeJS.ProcessEnv = process.env,
  fetcher: Fetcher = fetch): Promise<string> {
  return env.IDENTITY_ENDPOINT ? managedIdentityToken(resource, env, fetcher) : localToken(resource, subscriptionId);
}
