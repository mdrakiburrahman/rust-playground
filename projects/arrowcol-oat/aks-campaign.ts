import { spawnSync } from "node:child_process";
import path from "node:path";
import { pathToFileURL } from "node:url";
export const HEARTBEAT_INTERVAL_MS = 60_000;
export const MISSED_HEARTBEAT_LIMIT = 3;
export const MAX_CAMPAIGN_LIFETIME_MS = 6 * 60 * 60 * 1000;
const MAX_ARM_OPERATIONS = 128;
export const CUSTODY_CLEANUP_STEPS = ["stop-admission", "bounded-drain", "preserve-evidence", "stop-maintenance", "record-terminal-state", "remove-run-namespace", "remove-run-qap", "remove-run-fic", "restore-safe-idle", "verify-safe-idle"] as const;
export type CleanupStepName = typeof CUSTODY_CLEANUP_STEPS[number];
export interface CleanupReceipt { step: CleanupStepName; idempotencyKey: string; status: "pending" | "running" | "complete"; }
export type RunInventoryResourceState = 'planned' | 'observed' | 'removed' | 'absent' | 'uncertain';
export type InventoryResourceRole = 'collector' | 'client' | 'maintenance' | 'router';
export type InventoryStepState = 'not_started' | 'pending' | 'running' | 'complete' | 'failed' | 'uncertain';
export interface InventoryStep { state: InventoryStepState; deadline_unix_ms: number | null; reason_code: string | null; }
export interface InventoryAbsenceReceipt { api_version: string; kind: string; namespace: string; name: string; namespace_uid: string | null; observed_at_unix_ms: number; }
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
  namespace: { name: string; uid: string | null; owner_token: string };
  resources: RunInventoryResource[];
  fic: { resource_id: string | null; name: string | null; state: RunInventoryResourceState } | null;
  qap: { database: string; bindings: Array<{ external_table: string; relative_path: string }>; state: RunInventoryResourceState } | null;
  cleanup: { stop_admission: InventoryStep; bounded_drain: InventoryStep; evidence_capture: InventoryStep;
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
  const fields = ['schema_version', 'campaign_id', 'owner_id', 'fencing_token', 'orchestration_run_id', 'cell_run_id',
    'namespace', 'resources', 'fic', 'qap', 'cleanup'];
  if (!exactRecord(value, fields) || value.schema_version !== 1 || value.campaign_id !== state.campaignId ||
      value.owner_id !== state.ownerId || value.fencing_token !== (state.status === 'active' ? state.fencingToken : state.runInventory?.fencing_token) || !nonnegative(value.fencing_token) || value.fencing_token === 0 ||
      !canonicalUuid(value.campaign_id) || !canonicalUuid(value.owner_id) || !canonicalUuid(value.orchestration_run_id) || !canonicalUuid(value.cell_run_id) ||
      !exactRecord(value.namespace, ['name', 'uid', 'owner_token']) || !k8sName(value.namespace.name) ||
      !String(value.namespace.name).endsWith('-' + value.cell_run_id.replace(/-/g, '')) ||
      !(value.namespace.uid === null || canonicalUuid(value.namespace.uid)) || !canonicalUuid(value.namespace.owner_token) ||
      value.namespace.owner_token !== state.ownerId || !Array.isArray(value.resources) || value.resources.length > 256 ||
      !(value.fic === null || exactRecord(value.fic, ['resource_id', 'name', 'state']) && ['planned', 'observed', 'removed', 'uncertain'].includes(String(value.fic.state))) ||
      !(value.qap === null || exactRecord(value.qap, ['database', 'bindings', 'state']) && nonempty(value.qap.database) && Array.isArray(value.qap.bindings) && ['planned', 'observed', 'removed', 'uncertain'].includes(String(value.qap.state))) ||
      !exactRecord(value.cleanup, ['stop_admission', 'bounded_drain', 'evidence_capture', 'terminal_capture', 'handoff']) ||
      Object.values(value.cleanup).some(item => !validStep(item))) {
    throw new Error('RunInventory is malformed or fenced to another owner.');
  }
  if (value.fic !== null) {
    const fic = value.fic;
    if (!(fic.resource_id === null || nonempty(fic.resource_id)) || !(fic.name === null || k8sName(fic.name)) ||
        (fic.resource_id !== null || fic.state === 'observed' || fic.state === 'removed') && (!fic.resource_id || !fic.name ||
          !fic.resource_id.toLowerCase().startsWith('/subscriptions/') ||
          !fic.resource_id.toLowerCase().endsWith('/federatedidentitycredentials/' + fic.name.toLowerCase()) || /[?#]/.test(fic.resource_id)) ||
        ['observed', 'removed'].includes(fic.state) && (!fic.resource_id || !fic.name)) {
      throw new Error('RunInventory FIC identity is malformed.');
    }
  }
  if (value.qap !== null) {
    const safeIdentifier = (item: string) => /^[a-zA-Z0-9_-]+$/.test(item);
    if (!safeIdentifier(value.qap.database)) throw new Error('RunInventory QAP database is malformed.');
    for (const binding of value.qap.bindings) {
      if (!exactRecord(binding, ['external_table', 'relative_path']) || !nonempty(binding.external_table) ||
          !safeIdentifier(binding.external_table) || !nonempty(binding.relative_path) ||
          /[?#@\\:]/.test(binding.relative_path) || binding.relative_path.startsWith('/') ||
          binding.relative_path.split('/').some(part => part === '' || part === '.' || part === '..')) {
        throw new Error('RunInventory QAP binding is malformed.');
      }
    }
    if (['observed', 'removed'].includes(value.qap.state) && !value.qap.bindings.length) throw new Error('Observed RunInventory QAP requires an exact binding.');
  }
  const resourceKeys = new Set<string>();
  for (const resource of value.resources) {
    if (!exactRecord(resource, ['api_version', 'kind', 'namespace', 'name', 'uid', 'absence', 'role', 'state']) ||
        !nonempty(resource.api_version) || !nonempty(resource.kind) || resource.namespace !== value.namespace.name ||
        typeof resource.name !== 'string' || resource.name.length === 0 || resource.name.length > 253 ||
        !/^[a-z0-9.-]+$/.test(resource.name) || !['apps/v1:Deployment', 'batch/v1:Job', 'v1:Pod'].includes(resource.api_version + ':' + resource.kind) ||
        !['collector', 'client', 'maintenance', 'router'].includes(String(resource.role)) || !stateValue(resource.state) ||
        !(resource.uid === null || canonicalUuid(resource.uid)) ||
        ['observed', 'removed'].includes(resource.state) && (resource.uid === null || value.namespace.uid === null) ||
        resource.state === 'absent' && (resource.uid !== null || !exactRecord(resource.absence, ['api_version', 'kind', 'namespace', 'name', 'namespace_uid', 'observed_at_unix_ms']) ||
          resource.absence.api_version !== resource.api_version || resource.absence.kind !== resource.kind ||
          resource.absence.namespace !== resource.namespace || resource.absence.name !== resource.name ||
          resource.absence.namespace_uid !== value.namespace.uid || !nonnegative(resource.absence.observed_at_unix_ms) || resource.absence.observed_at_unix_ms === 0) ||
        resource.state !== 'absent' && resource.absence !== null) {
      throw new Error('RunInventory resource identity is malformed.');
    }
    const key = [resource.api_version, resource.kind, resource.name].join('|');
    if (resourceKeys.has(key)) throw new Error('RunInventory contains duplicate resource identities.');
    resourceKeys.add(key);
  }
  for (const name of ['stop_admission', 'bounded_drain'] as const) {
    const step = value.cleanup[name];
    if (step.state === 'running' && step.deadline_unix_ms === null) throw new Error('Running RunInventory admission and drain steps require a deadline.');
  }
  if (value.cleanup.handoff.state === 'complete' &&
      (Object.entries(value.cleanup).some(([key, item]) => key !== 'handoff' && item.state !== 'complete') ||
       value.resources.some(resource => resource.state !== 'removed' && resource.state !== 'absent') || value.fic !== null && value.fic.state !== 'removed' ||
       value.qap !== null && value.qap.state !== 'removed')) {
    throw new Error('RunInventory handoff cannot complete while owned resources or cleanup actions remain unresolved.');
  }
  return value as unknown as RunInventory;
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
function mergeRunInventory(before: RunInventory | undefined, after: RunInventory): RunInventory {
  if (!before) return after;
  const sameCell = before.campaign_id === after.campaign_id && before.owner_id === after.owner_id &&
    before.fencing_token === after.fencing_token && before.orchestration_run_id === after.orchestration_run_id &&
    before.cell_run_id === after.cell_run_id;
  if (!sameCell) {
    if (before.cleanup.handoff.state !== 'complete') throw new Error('An unresolved RunInventory already owns the campaign.');
    return after;
  }
  if (before.namespace.name !== after.namespace.name || before.namespace.owner_token !== after.namespace.owner_token ||
      before.namespace.uid && after.namespace.uid && before.namespace.uid !== after.namespace.uid) {
    throw new Error('RunInventory immutable ownership fields changed.');
  }
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
        before.fic.resource_id && after.fic.resource_id !== before.fic.resource_id) throw new Error('RunInventory FIC identity changed.');
    return { ...before.fic, ...after.fic, name: before.fic.name ?? after.fic.name,
      resource_id: before.fic.resource_id ?? after.fic.resource_id, state: mergeResourceState(before.fic.state, after.fic.state) };
  };
  const mergeQap = () => {
    if (!before.qap) return after.qap;
    if (!after.qap || before.qap.database !== after.qap.database || before.qap.bindings.some(old =>
      !after.qap!.bindings.some(next => next.external_table === old.external_table && next.relative_path === old.relative_path))) {
      throw new Error('RunInventory QAP identity changed.');
    }
    return { ...after.qap, bindings: [...new Map([...before.qap.bindings, ...after.qap.bindings].map(binding =>
      [binding.external_table + '|' + binding.relative_path, binding])).values()],
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

export function validateCampaignState(value: unknown): CampaignState {
  if (!record(value) || Object.keys(value).some(key => ![
    'schemaVersion', 'campaignId', 'ownerId', 'fencingToken', 'mode', 'status', 'startedAtMs', 'heartbeatAtMs',
    'heartbeatSequence', 'expiresAtMs', 'lastEvaluatedAtMs', 'lastMissedHeartbeatCount', 'fenceReason', 'fencedAtMs',
    'cleanupStatus', 'cleanupSteps', 'runInventory', 'operations',
  ].includes(key))) throw new Error('Campaign state has unknown fields or is not an object.');
  const state = value as unknown as CampaignState;
  if (state.schemaVersion !== 1 || !uuid.test(state.campaignId) || !uuid.test(state.ownerId) ||
      !Number.isSafeInteger(state.fencingToken) || state.fencingToken < 1 ||
      !['observe', 'armed'].includes(state.mode) || !['active', 'fenced', 'cleaning', 'complete'].includes(state.status) ||
      !nonnegative(state.startedAtMs) || !nonnegative(state.heartbeatAtMs) || state.heartbeatAtMs < state.startedAtMs ||
      !Number.isSafeInteger(state.heartbeatSequence) || state.heartbeatSequence < 0 ||
      state.expiresAtMs !== state.startedAtMs + MAX_CAMPAIGN_LIFETIME_MS ||
      !['not-started', 'pending', 'running', 'verified'].includes(state.cleanupStatus) ||
      !Array.isArray(state.operations) || state.operations.length > MAX_ARM_OPERATIONS || !Array.isArray(state.cleanupSteps))  {
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
      !["pending", "running", "complete"].includes(receipt.status))) throw new Error("Campaign cleanup receipts are malformed.");
  if (state.status === "active" && state.cleanupStatus !== "not-started" || state.status === "fenced" && state.cleanupStatus !== "pending" ||
      state.status === "cleaning" && state.cleanupStatus !== "running" || state.status === "complete" &&
      (state.cleanupStatus !== "verified" || state.cleanupSteps.some(receipt => receipt.status !== "complete"))) {
    throw new Error("Campaign cleanup state does not match its durable receipts.");
  }
  let unfinishedCleanup = false;
  let runningCleanup = false;
  for (const receipt of state.cleanupSteps) {
    if (receipt.status === 'complete' && unfinishedCleanup) throw new Error('Campaign cleanup receipts are out of order.');
    if (receipt.status === 'running') {
      if (runningCleanup) throw new Error('Campaign has multiple running cleanup receipts.');
      runningCleanup = true;
    }
    if (receipt.status !== 'complete') unfinishedCleanup = true;
  }
  if (state.runInventory !== undefined) {
    validateRunInventory(state.runInventory, state);
    if (state.status === 'active' && state.runInventory.fencing_token !== state.fencingToken ||
        ['fenced', 'cleaning', 'complete'].includes(state.status) && state.runInventory.fencing_token >= state.fencingToken) {
      throw new Error('RunInventory producer fencing identity does not match the campaign lifecycle.');
    }
  }
  if (state.mode === 'armed' && !state.runInventory || ['fenced', 'cleaning', 'complete'].includes(state.status) && !state.runInventory) {
    throw new Error('Campaign cannot be armed or fenced without a persisted RunInventory.');
  }
  const ids = new Set<string>();
  for (const operation of state.operations) {
    if (!record(operation) || !uuid.test(operation.id) || ids.has(operation.id) ||
        typeof operation.resourceId !== 'string' || !operation.resourceId.startsWith(`/subscriptions/`) ||
        typeof operation.operationUrl !== 'string' || !nonnegative(operation.submittedAtMs) ||
        !['pending', 'succeeded', 'failed', 'indeterminate'].includes(operation.status) ||
        operation.lastCheckedAtMs !== undefined && !nonnegative(operation.lastCheckedAtMs)) {
      throw new Error('ARM operation receipt is malformed.');
    }
    const operationSubscription = /^\/subscriptions\/([0-9a-f-]{36})\//i.exec(operation.resourceId)?.[1];
    if (!operationSubscription) throw new Error('ARM operation resource ID has no subscription scope.');
    httpsManagementUrl(operation.operationUrl, operationSubscription);
    ids.add(operation.id);
  }
  if (state.status === 'active' && state.fenceReason !== undefined ||
      ['fenced', 'cleaning', 'complete'].includes(state.status) && (!state.fenceReason || state.fencedAtMs === undefined || state.fencingToken < 2)) {
    throw new Error('Campaign terminal state is missing its fencing receipt.');
  }
  if (state.status === 'complete' && state.cleanupStatus !== 'verified') throw new Error('Campaign cannot complete before cleanup verification.');
  return state;
}

export function newCampaignState(campaignId: string, ownerId: string, now = Date.now()): CampaignState {
  if (!uuid.test(campaignId) || !uuid.test(ownerId) || !nonnegative(now)) throw new Error('Campaign and owner IDs must be UUIDs and the clock must be valid.');
  return { schemaVersion: 1, campaignId, ownerId, fencingToken: 1, mode: 'observe', status: 'active',
    startedAtMs: now, heartbeatAtMs: now, heartbeatSequence: 0, expiresAtMs: now + MAX_CAMPAIGN_LIFETIME_MS,
    cleanupStatus: 'not-started', cleanupSteps: [], operations: [] };
}

export class CampaignConflictError extends Error {
  constructor() { super('Campaign state changed concurrently; retry using a fresh read.'); this.name = 'CampaignConflictError'; }
}
export interface CampaignStoreConfig { account: string; container: string; subscriptionId: string }

export async function managedIdentityToken(resource: string, env: NodeJS.ProcessEnv = process.env, fetcher: Fetcher = fetch): Promise<string> {
  const endpoint = env.IDENTITY_ENDPOINT;
  const header = env.IDENTITY_HEADER;
  if (!endpoint || !header || !env.AZURE_CLIENT_ID) throw new Error('Container Apps managed identity endpoint is unavailable.');
  const url = new URL(endpoint);
  url.searchParams.set('api-version', '2019-08-01');
  url.searchParams.set('resource', resource);
  url.searchParams.set('client_id', env.AZURE_CLIENT_ID);
  const response = await fetcher(url, { headers: { 'X-IDENTITY-HEADER': header } });
  if (!response.ok) throw new Error(`Managed identity token request failed with HTTP ${response.status}.`);
  const body: unknown = await response.json();
  if (!record(body) || typeof body.access_token !== 'string' || !body.access_token) throw new Error('Managed identity returned an invalid token response.');
  return body.access_token;
}

async function localToken(resource: string, subscriptionId: string): Promise<string> {
  const result = spawnSync("az", ["account", "get-access-token", "--subscription", subscriptionId,
    "--resource", resource, "--query", "accessToken", "--output", "tsv", "--only-show-errors"],
    { encoding: "utf8", timeout: 30_000, windowsHide: true });
  const token = typeof result.stdout === "string" ? result.stdout.trim() : "";
  if (result.error || result.status !== 0 || !token || /[[:space:]]/.test(token))
    throw new Error("Azure CLI could not provide a token for the campaign store.");
  return token;
}

export function createBlobCampaignStore(config: CampaignStoreConfig, fetcher: Fetcher = fetch,
  token: TokenProvider = resource => getAccessToken(resource, config.subscriptionId, process.env, fetcher)): CampaignStore {
  if (!storageName.test(config.account) || !containerName.test(config.container) || !uuid.test(config.subscriptionId)) {
    throw new Error('Campaign store requires a storage account, container and subscription ID.');
  }
  const endpoint = `https://${config.account}.blob.core.windows.net/${config.container}/active.json`;
  const headers = async () => ({ authorization: `Bearer ${await token('https://storage.azure.com/')}`,
    'x-ms-date': new Date().toUTCString(), 'x-ms-version': '2023-11-03' });
  return {
    async read() {
      const response = await fetcher(endpoint, { headers: await headers() });
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
      const response = await fetcher(endpoint, { method: 'PUT', headers: { ...(await headers()), ...condition,
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
    if (current && current.status !== 'complete') throw new Error('An unfinished campaign still owns the stamp.');
    return newCampaignState(campaignId, ownerId, now);
  });
}
export class CampaignFencedError extends Error {
  constructor(message = 'Campaign owner is fenced, stale or does not match the durable fencing token.') { super(message); this.name = 'CampaignFencedError'; }
}
export class CampaignWriteOutcomeUnknownError extends Error {
  constructor() { super('RunInventory write outcome could not be confirmed by a strong readback.'); this.name = 'CampaignWriteOutcomeUnknownError'; }
}
function runInventoryReady(inventory: RunInventory | undefined): inventory is RunInventory {
  return Boolean(inventory && inventory.namespace.uid && inventory.resources.length > 0 &&
    inventory.resources.every(resource => resource.state === 'observed' && resource.uid) &&
    inventory.fic?.state === 'observed' && inventory.qap?.state === 'observed' && inventory.qap.bindings.length > 0 &&
    inventory.cleanup.handoff.state !== 'complete');
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
  return Boolean(inventory && inventory.namespace.uid && inventory.resources.length > 0 &&
    inventory.resources.every(resource => resource.state === 'observed' && resource.uid) &&
    inventory.fic?.state === 'observed' && inventory.qap?.state === 'observed' && inventory.qap.bindings.length > 0 &&
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
    const next = validateCampaignState({ ...current.state, runInventory: merged });
    const written = await writeInventoryWithReadback(store, current, next, merged, 'active', current.state.fencingToken);
    if (written) return written;
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
    return { ...state, mode: 'armed' };
  });
}
export async function setCleanupVerified(store: CampaignStore, ownerId: string, fencingToken: number): Promise<StoredCampaign> {
  return mutateCampaign(store, current => {
    if (!current || current.status !== 'cleaning' || current.ownerId !== ownerId || current.fencingToken !== fencingToken ||
        current.cleanupStatus !== 'running' || current.cleanupSteps.some(step => step.status !== 'complete')) throw new Error('Cleanup verification requires the fenced owner and a running cleanup.');
    return { ...current, status: 'complete', cleanupStatus: 'verified' };
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

async function readRunInventoryStdin(): Promise<unknown> {
  let input = '';
  for await (const chunk of process.stdin) {
    input += chunk.toString();
    if (Buffer.byteLength(input) > 1024 * 1024) throw new Error('RunInventory input exceeds the 1 MiB limit.');
  }
  try { return JSON.parse(input) as unknown; } catch { throw new Error('RunInventory stdin is not valid JSON.'); }
}

async function main(args: string[]): Promise<void> {
  const [action, campaignId, fencingTokenText] = args;
  const subscriptionId = process.env.ARM_SUBSCRIPTION_ID ?? '';
  const store = createBlobCampaignStore({ account: process.env.CAMPAIGN_STORAGE_ACCOUNT ?? '',
    container: process.env.CAMPAIGN_STATE_CONTAINER ?? '', subscriptionId });
  if (action === 'start' && args.length === 2) {
    const result = await startCampaign(store, campaignId!);
    console.log(JSON.stringify({ campaignId: result.state.campaignId, ownerId: result.state.ownerId,
      fencingToken: result.state.fencingToken, expiresAtMs: result.state.expiresAtMs, mode: result.state.mode }));
  } else if (action === 'heartbeat' && args.length === 3 && /^\d+$/.test(fencingTokenText!)) {
    const state = await store.read();
    if (!state) throw new Error('No active campaign state exists.');
    const result = await heartbeatCampaign(store, campaignId!, Number(fencingTokenText), Date.now());
    console.log(JSON.stringify({ campaignId: result.state.campaignId, fencingToken: result.state.fencingToken,
      heartbeatSequence: result.state.heartbeatSequence, heartbeatAtMs: result.state.heartbeatAtMs }));
  } else if (action === 'arm' && args.length === 3 && /^\d+$/.test(fencingTokenText!)) {
    const result = await armCampaign(store, campaignId!, Number(fencingTokenText));
    console.log(JSON.stringify({ campaignId: result.state.campaignId, mode: result.state.mode }));
  } else if (action === 'publish-inventory' && args.length === 1) {
    const result = await publishRunInventory(store, await readRunInventoryStdin());
    const inventory = result.state.runInventory!;
    console.log(JSON.stringify({ ok: true, receipt: { status: 'persisted', campaign_id: inventory.campaign_id,
      owner_id: inventory.owner_id, fencing_token: inventory.fencing_token, orchestration_run_id: inventory.orchestration_run_id,
      cell_run_id: inventory.cell_run_id, namespace_uid: inventory.namespace.uid,
      resource_count: inventory.resources.length, fic_state: inventory.fic.state, qap_state: inventory.qap.state, etag: result.etag } }));
  } else if (action === 'assert-ready' && args.length === 3 && /^\d+$/.test(fencingTokenText!)) {
    const state = await assertRunInventoryReady(store, campaignId!, Number(fencingTokenText));
    console.log(JSON.stringify({ ok: true, receipt: { status: 'ready', campaign_id: state.campaignId,
      owner_id: state.ownerId, fencing_token: state.fencingToken, orchestration_run_id: state.runInventory!.orchestration_run_id,
      cell_run_id: state.runInventory!.cell_run_id, namespace_uid: state.runInventory!.namespace.uid } }));
  } else if (action === 'status' && args.length === 1) {
    console.log(JSON.stringify((await store.read())?.state ?? { status: 'idle' }));
  } else {
    throw new Error('Usage: aks-campaign.ts start <campaign-uuid> | heartbeat <owner-uuid> <fencing-token> | arm <owner-uuid> <fencing-token> | publish-inventory (JSON stdin) | assert-ready <owner-uuid> <fencing-token> | status');
  }
}
if (process.argv[1] && ['aks-campaign.ts', 'aks-campaign.js'].includes(path.basename(process.argv[1])) && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    const action = process.argv[2];
    if (action === 'publish-inventory' || action === 'assert-ready') {
      const message = error instanceof Error ? error.message : 'Campaign custody failed.';
      const code = error instanceof CampaignWriteOutcomeUnknownError ? 'write_outcome_unknown' :
        error instanceof CampaignFencedError || /fenced|stale|heartbeat is expired/i.test(message) ? 'campaign_fenced' : 'invalid_or_storage_error';
      console.log(JSON.stringify({ ok: false, code, message }));
    } else console.error(error instanceof Error ? error.message : 'Campaign custody failed');
    process.exitCode = 1;
  });
}

export async function readArmOperationStatus(operationUrl: string, subscriptionId: string, fetcher: Fetcher = fetch): Promise<{ status: number; body?: unknown }> {
  const url = httpsManagementUrl(operationUrl, subscriptionId);
  const accessToken = await getAccessToken('https://management.azure.com/', subscriptionId, process.env, fetcher);
  const response = await fetcher(url, { headers: { authorization: `Bearer ${accessToken}` }, signal: AbortSignal.timeout(10_000) });
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
