import { spawnSync } from "node:child_process";
import path from "node:path";
import { pathToFileURL } from "node:url";
export const HEARTBEAT_INTERVAL_MS = 60_000;
export const MISSED_HEARTBEAT_LIMIT = 3;
export const MAX_CAMPAIGN_LIFETIME_MS = 6 * 60 * 60 * 1000;
const MAX_ARM_OPERATIONS = 128;
export const CUSTODY_CLEANUP_STEPS = ["stop-admission", "bounded-drain", "preserve-evidence", "stop-maintenance", "record-terminal-state", "remove-run-namespace", "remove-run-fic", "restore-safe-idle", "verify-safe-idle"] as const;
export type CleanupStepName = typeof CUSTODY_CLEANUP_STEPS[number];
export interface CleanupReceipt { step: CleanupStepName; idempotencyKey: string; status: "pending" | "running" | "complete"; }
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const storageName = /^[a-z0-9]{3,24}$/;
const containerName = /^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$/;

export type CampaignMode = 'observe' | 'armed';
export type CampaignStatus = 'active' | 'fenced' | 'cleaning' | 'complete';
export type ArmOperationStatus = 'pending' | 'succeeded' | 'failed' | 'indeterminate';
export interface ArmOperationReceipt {
  id: string;
  resourceId: string;
  operationUrl: string;
  submittedAtMs: number;
  status: ArmOperationStatus;
  lastCheckedAtMs?: number;
}
export interface CampaignState {
  schemaVersion: 1;
  campaignId: string;
  ownerId: string;
  fencingToken: number;
  mode: CampaignMode;
  status: CampaignStatus;
  startedAtMs: number;
  heartbeatAtMs: number;
  heartbeatSequence: number;
  expiresAtMs: number;
  lastEvaluatedAtMs?: number;
  lastMissedHeartbeatCount?: number;
  fenceReason?: 'heartbeat-expired' | 'max-lifetime';
  fencedAtMs?: number;
  cleanupStatus: 'not-started' | 'pending' | 'running' | 'verified';
  cleanupSteps: CleanupReceipt[];
  operations: ArmOperationReceipt[];
}
export interface StoredCampaign {
  state: CampaignState;
  etag: string;
}
export interface CampaignStore {
  read(): Promise<StoredCampaign | undefined>;
  write(state: CampaignState, expectedEtag?: string): Promise<string>;
}
export type TokenProvider = (resource: string) => Promise<string>;
export type Fetcher = typeof fetch;

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function nonnegative(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
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
    'cleanupStatus', 'cleanupSteps', 'operations',
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
    if (!uuid.test(operation.id) || state.operations.some(existing => existing.id === operation.id) ||
        !operation.resourceId.toLowerCase().startsWith(`/subscriptions/${subscriptionId.toLowerCase()}/`) ||
        !nonnegative(operation.submittedAtMs) || state.operations.length >= MAX_ARM_OPERATIONS) {
      throw new Error('ARM operation receipt is invalid, duplicate, out of scope or exceeds the receipt bound.');
    }
    return { ...state, operations: [...state.operations, { ...operation, operationUrl, status: 'pending' }] };
  });
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
  } else if (action === 'status' && args.length === 1) {
    console.log(JSON.stringify((await store.read())?.state ?? { status: 'idle' }));
  } else {
    throw new Error('Usage: aks-campaign.ts start <campaign-uuid> | heartbeat <owner-uuid> <fencing-token> | arm <owner-uuid> <fencing-token> | status');
  }
}
if (process.argv[1] && ['aks-campaign.ts', 'aks-campaign.js'].includes(path.basename(process.argv[1])) && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => { console.error(error instanceof Error ? error.message : 'Campaign custody failed'); process.exitCode = 1; });
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
