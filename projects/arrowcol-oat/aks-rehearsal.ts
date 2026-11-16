import { createHash, randomUUID } from 'node:crypto';
import { appendFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { Azure } from '../../infra/azure/bootstrap.js';
import { loadCustodyConfig } from './aks-custody-config.js';
import { CampaignConflictError, createBlobCampaignStore, heartbeatCampaign, newCampaignState, validateCampaignState,
  type CampaignState, type CampaignStore, type CleanupStepName, type RunInventory, type StoredCampaign,
  HEARTBEAT_INTERVAL_MS, MISSED_HEARTBEAT_LIMIT, isInitialRunInventory } from './aks-campaign.js';
import { watchdogOnce, type WatchdogReadOnlyAzure } from './aks-watchdog.js';

export const REHEARSAL_BLOB_ROOT = '_arrowcol-custody-rehearsal';
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export interface RehearsalEvidenceRecord { observed_at: string; event: string; [key: string]: unknown }
export type RehearsalEvidenceSink = (record: RehearsalEvidenceRecord) => void;

function recordEvidence(sink: RehearsalEvidenceSink, event: string, values: Record<string, unknown> = {}): void {
  sink({ ...values, observed_at: new Date().toISOString(), event, rehearsal_fixture: 'synthetic custody fixture',
    cleanup_effects: 'simulated' });
}

export function createJsonLinesEvidenceSink(filename: string): RehearsalEvidenceSink {
  const target = resolve(filename);
  mkdirSync(dirname(target), { recursive: true });
  return value => appendFileSync(target, JSON.stringify(value) + '\n', { encoding: 'utf8' });
}

export function rehearsalCampaignRecordPath(rehearsalId: string): string {
  if (!uuid.test(rehearsalId)) throw new Error('Rehearsal ID must be a UUID.');
  return `${REHEARSAL_BLOB_ROOT}/${rehearsalId}/active.json`;
}

function pathsOverlap(left: string, right: string): boolean {
  return left === right || left.startsWith(right + '/') || right.startsWith(left + '/');
}

export function assertRehearsalPathIsolated(recordPath: string, realRecordPath = 'active.json'): void {
  const segments = recordPath.split('/');
  if (!recordPath.startsWith(REHEARSAL_BLOB_ROOT + '/') ||
      segments.some(segment => !segment || segment === '.' || segment === '..') ||
      pathsOverlap(recordPath, realRecordPath) || pathsOverlap(recordPath, '_arrowcol-custody')) {
    throw new Error('Rehearsal Blob location is not isolated from real campaign custody paths.');
  }
}

export type ReadOnlyCampaignStore = Pick<CampaignStore, 'read'>;

function createReadOnlyEvidenceCampaignStore(store: CampaignStore, evidence: RehearsalEvidenceSink): ReadOnlyCampaignStore {
  return {
    async read() {
      const current = await store.read();
      recordEvidence(evidence, 'blob_read', { path: 'active.json', etag: current?.etag ?? null,
        campaign_id: current?.state.campaignId ?? null, status: current?.state.status ?? null,
        fencing_token: current?.state.fencingToken ?? null, record_scope: 'real-read-only' });
      return current;
    },
  };
}

function createEvidenceCampaignStore(store: CampaignStore, recordPath: string, evidence: RehearsalEvidenceSink,
  realStore: ReadOnlyCampaignStore): CampaignStore {
  assertRehearsalPathIsolated(recordPath);
  let previous: StoredCampaign | undefined;
  return {
    async read() {
      const current = await store.read();
      previous = current;
      recordEvidence(evidence, 'blob_read', { path: recordPath, etag: current?.etag ?? null,
        campaign_id: current?.state.campaignId ?? null, status: current?.state.status ?? null,
        fencing_token: current?.state.fencingToken ?? null });
      return current;
    },
    async write(state, expectedEtag) {
      await assertRealCampaignTerminal(realStore, evidence);
      const before = previous;
      try {
        const etag = await store.write(state, expectedEtag);
        previous = { state, etag };
        recordEvidence(evidence, 'campaign_transition', { path: recordPath, previous_etag: expectedEtag ?? null,
          etag, campaign_id: state.campaignId, from_status: before?.state.status ?? null, to_status: state.status,
          from_fencing_token: before?.state.fencingToken ?? null, fencing_token: state.fencingToken });
        return etag;
      } catch (error) {
        recordEvidence(evidence, error instanceof CampaignConflictError ? 'blob_write_cas_conflict' : 'blob_write_outcome_unknown',
          { path: recordPath, previous_etag: expectedEtag ?? null, campaign_id: state.campaignId,
            status: state.status, fencing_token: state.fencingToken,
            error_name: error instanceof Error ? error.name : 'unknown' });
        throw error;
      }
    },
  };
}

export async function assertRealCampaignTerminal(store: ReadOnlyCampaignStore, evidence: RehearsalEvidenceSink): Promise<void> {
  const stored = await store.read();
  recordEvidence(evidence, 'real_campaign_read', { path: 'active.json', etag: stored?.etag ?? null,
    campaign_id: stored?.state.campaignId ?? null, status: stored?.state.status ?? null,
    fencing_token: stored?.state.fencingToken ?? null, record_scope: 'real-read-only' });
  if (stored && !['complete', 'complete_unsettled'].includes(stored.state.status))
    throw new Error('Real campaign record is non-terminal; refusing rehearsal startup.');
}

export interface ReadOnlyArmResponse { status: number; body: Buffer | null; requestId?: string }
export type ReadOnlyArmSender = (method: 'GET', url: string) => Promise<ReadOnlyArmResponse>;

export class ReadOnlyAzureAdapter implements WatchdogReadOnlyAzure {
  constructor(private readonly send: ReadOnlyArmSender, private readonly evidence: RehearsalEvidenceSink) {}

  async request(method: string, url: string): Promise<{ status: number; body?: unknown }> {
    const parsed = new URL(url);
    if (method !== 'GET') throw new Error('Rehearsal ARM adapter refuses non-GET requests before send.');
    if (parsed.protocol !== 'https:' || parsed.hostname !== 'management.azure.com' || parsed.username || parsed.password || parsed.hash)
      throw new Error('Rehearsal ARM adapter refuses non-management endpoints before send.');
    const response = await this.send('GET', url);
    const bytes = response.body ?? Buffer.alloc(0);
    const bodySha256 = createHash('sha256').update(bytes).digest('hex');
    let body: unknown;
    if (bytes.length) {
      try { body = JSON.parse(bytes.toString('utf8')) as unknown; }
      catch { throw new Error('Rehearsal ARM GET returned invalid JSON.'); }
    }
    recordEvidence(this.evidence, 'arm_get', { method: 'GET', url, status: response.status,
      request_id: response.requestId ?? null, response_body_sha256: bodySha256 });
    return { status: response.status, ...(body === undefined ? {} : { body }) };
  }

  async readArmStatus(url: string): Promise<{ status: number; body?: unknown }> {
    return this.request('GET', url);
  }
}

export function createReadOnlyAzureAdapter(azure: Azure, evidence: RehearsalEvidenceSink): ReadOnlyAzureAdapter {
  return new ReadOnlyAzureAdapter(async (_method, url) => {
    const token = await azure.token('https://management.azure.com/');
    const response = await fetch(url, { method: 'GET', headers: { authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(10_000), redirect: 'error' });
    return { status: response.status, body: Buffer.from(await response.arrayBuffer()),
      requestId: response.headers.get('x-ms-request-id') ?? response.headers.get('x-ms-correlation-request-id') ?? undefined };
  }, evidence);
}

const SYNTHETIC_SUBSCRIPTION_ID = '00000000-0000-4000-8000-000000000001';

export function createSyntheticArmedCampaignState(rehearsalId: string, ownerId: string): CampaignState {
  const state = newCampaignState(rehearsalId, ownerId);
  const cellRunId = randomUUID();
  const namespace = 'd40a-synthetic-' + cellRunId.replace(/-/g, '');
  const ficName = 'arrowcol-run-' + cellRunId;
  const notStarted = () => ({ state: 'not_started' as const, deadline_unix_ms: null, reason_code: null });
  const inventory: RunInventory = {
    schema_version: 1,
    campaign_id: rehearsalId,
    owner_id: ownerId,
    fencing_token: state.fencingToken,
    orchestration_run_id: randomUUID(),
    cell_run_id: cellRunId,
    load_admission: 'not_authorized',
    namespace: { name: namespace, uid: null, owner_token: ownerId },
    resources: [{ api_version: 'apps/v1', kind: 'Deployment', namespace, name: 'synthetic-collector', uid: null,
      absence: null, role: 'collector', state: 'planned' }],
    fic: { resource_id: '/subscriptions/' + SYNTHETIC_SUBSCRIPTION_ID + '/resourceGroups/d40a-synthetic/providers/Microsoft.ManagedIdentity/userAssignedIdentities/d40a-synthetic/federatedIdentityCredentials/' + ficName,
      name: ficName, state: 'planned', absence: null },
    qap: { database: 'd40a_synthetic', bindings: [], state: 'planned', absence: [] },
    cleanup: { stop_admission: notStarted(), bounded_drain: notStarted(), evidence_capture: notStarted(),
      stop_maintenance: notStarted(), terminal_capture: notStarted(), handoff: notStarted() },
  };
  if (!isInitialRunInventory(inventory)) throw new Error('Synthetic custody inventory must remain unprovisioned and not authorized.');
  return validateCampaignState({ ...state, mode: 'armed', runInventory: inventory });
}

function assertSyntheticRehearsalState(state: CampaignState, rehearsalId: string, ownerId: string): void {
  const inventory = state.runInventory;
  if (state.campaignId !== rehearsalId || state.ownerId !== ownerId || state.mode !== 'armed' || !inventory ||
      inventory.campaign_id !== rehearsalId || inventory.owner_id !== ownerId || inventory.load_admission !== 'not_authorized' ||
      inventory.fencing_token !== 1 || inventory.namespace.uid !== null ||
      inventory.namespace.name !== 'd40a-synthetic-' + inventory.cell_run_id.replace(/-/g, '') || state.operations.length !== 0 ||
      inventory.resources.length !== 1 || inventory.resources.some(resource => resource.api_version !== 'apps/v1' ||
        resource.kind !== 'Deployment' || resource.namespace !== inventory.namespace.name || resource.name !== 'synthetic-collector' ||
        resource.role !== 'collector' || resource.state !== 'planned' || resource.uid !== null) ||
      !inventory.fic || inventory.fic.state !== 'planned' || inventory.fic.name !== 'arrowcol-run-' + inventory.cell_run_id ||
      inventory.fic.resource_id !== '/subscriptions/' + SYNTHETIC_SUBSCRIPTION_ID +
        '/resourceGroups/d40a-synthetic/providers/Microsoft.ManagedIdentity/userAssignedIdentities/d40a-synthetic/federatedIdentityCredentials/' + inventory.fic.name ||
      !inventory.qap || inventory.qap.database !== 'd40a_synthetic' || inventory.qap.state !== 'planned' ||
      inventory.qap.bindings.length !== 0 || !isInitialRunInventory(inventory)) {
    throw new Error('Isolated record is not the exact unprovisioned synthetic armed fixture.');
  }
}

async function seedSyntheticRehearsalCampaign(store: CampaignStore, rehearsalId: string, ownerId: string,
  evidence: RehearsalEvidenceSink): Promise<StoredCampaign> {
  if (await store.read()) throw new Error('Isolated rehearsal record already exists; refusing to overwrite it.');
  const state = createSyntheticArmedCampaignState(rehearsalId, ownerId);
  const etag = await store.write(state, undefined);
  const confirmed = await store.read();
  if (!confirmed || confirmed.etag !== etag || JSON.stringify(confirmed.state) !== JSON.stringify(state))
    throw new Error('Synthetic fixture write could not be proven by exact ETag and state readback; refusing replay.');
  recordEvidence(evidence, 'synthetic_fixture_seeded', { campaign_id: rehearsalId, owner_id: ownerId,
    fencing_token: state.fencingToken, etag });
  return confirmed;
}

export function recordingCleanupExecutor(evidence: RehearsalEvidenceSink) {
  return async (step: CleanupStepName, state: CampaignState, idempotencyKey: string): Promise<'complete'> => {
    recordEvidence(evidence, 'recording_cleanup_noop', { campaign_id: state.campaignId,
      fencing_token: state.fencingToken, step, idempotency_key: idempotencyKey });
    return 'complete';
  };
}

export async function runRehearsalWatchdogOnce(options: {
  env: NodeJS.ProcessEnv; rehearsalId: string; ownerId: string; realStore: ReadOnlyCampaignStore;
  isolatedStore: CampaignStore; readOnlyAzure: ReadOnlyAzureAdapter; evidence: RehearsalEvidenceSink;
}): Promise<Record<string, unknown>> {
  const recordPath = rehearsalCampaignRecordPath(options.rehearsalId);
  assertRehearsalPathIsolated(recordPath);
  await assertRealCampaignTerminal(options.realStore, options.evidence);
  const store = createEvidenceCampaignStore(options.isolatedStore, recordPath, options.evidence, options.realStore);
  const current = await store.read();
  if (!current) throw new Error('Isolated rehearsal campaign record does not exist.');
  assertSyntheticRehearsalState(current.state, options.rehearsalId, options.ownerId);
  const result = await watchdogOnce(options.env, recordingCleanupExecutor(options.evidence), store, options.readOnlyAzure);
  recordEvidence(options.evidence, 'watchdog_result', { campaign_id: options.rehearsalId, ...result });
  return { ...result, rehearsal_fixture: 'synthetic custody fixture', cleanup_effects: 'simulated' };
}

export async function runRehearsalOwner(options: {
  rehearsalId: string; ownerId: string; realStore: ReadOnlyCampaignStore; isolatedStore: CampaignStore;
  evidence: RehearsalEvidenceSink; signal: AbortSignal;
}): Promise<void> {
  const recordPath = rehearsalCampaignRecordPath(options.rehearsalId);
  const store = createEvidenceCampaignStore(options.isolatedStore, recordPath, options.evidence, options.realStore);
  await assertRealCampaignTerminal(options.realStore, options.evidence);
  const started = await seedSyntheticRehearsalCampaign(store, options.rehearsalId, options.ownerId, options.evidence);
  recordEvidence(options.evidence, 'owner_started', { campaign_id: started.state.campaignId,
    owner_id: started.state.ownerId, fencing_token: started.state.fencingToken, etag: started.etag });
  if (options.signal.aborted) {
    recordEvidence(options.evidence, 'owner_stopped', { campaign_id: options.rehearsalId, reason: 'abort-signal' });
    return;
  }
  while (!options.signal.aborted) {
    try { await delay(60_000, undefined, { signal: options.signal }); }
    catch (error) {
      if (options.signal.aborted && error instanceof Error && error.name === 'AbortError') {
        recordEvidence(options.evidence, 'owner_stopped', { campaign_id: options.rehearsalId, reason: 'abort-signal' });
        return;
      }
      throw error;
    }
    if (options.signal.aborted) {
      recordEvidence(options.evidence, 'owner_stopped', { campaign_id: options.rehearsalId, reason: 'abort-signal' });
      return;
    }
    await runRehearsalOwnerHeartbeatOnce(options);
  }
}

export async function runRehearsalOwnerHeartbeatOnce(options: {
  rehearsalId: string; ownerId: string; realStore: ReadOnlyCampaignStore; isolatedStore: CampaignStore;
  evidence: RehearsalEvidenceSink;
}): Promise<StoredCampaign> {
  const recordPath = rehearsalCampaignRecordPath(options.rehearsalId);
  const store = createEvidenceCampaignStore(options.isolatedStore, recordPath, options.evidence, options.realStore);
  const current = await store.read();
  if (!current) throw new Error('Isolated rehearsal campaign record does not exist.');
  assertSyntheticRehearsalState(current.state, options.rehearsalId, options.ownerId);
  if (current.state.status !== 'active' || current.state.fencingToken !== current.state.runInventory!.fencing_token)
    throw new Error('Owner heartbeat requires the active synthetic producer authority.');
  const heartbeat = await heartbeatCampaign(store, options.ownerId, current.state.runInventory!.fencing_token);
  recordEvidence(options.evidence, 'owner_heartbeat', { campaign_id: heartbeat.state.campaignId,
    fencing_token: heartbeat.state.fencingToken, heartbeat_sequence: heartbeat.state.heartbeatSequence,
    heartbeat_at_unix_ms: heartbeat.state.heartbeatAtMs, etag: heartbeat.etag });
  return heartbeat;
}

export async function runRehearsalStaleOwnerProbe(options: {
  rehearsalId: string; ownerId: string; phase: 'pre-fence' | 'post-fence'; realStore: ReadOnlyCampaignStore;
  isolatedStore: CampaignStore; evidence: RehearsalEvidenceSink;
}): Promise<Record<string, unknown>> {
  const recordPath = rehearsalCampaignRecordPath(options.rehearsalId);
  const store = createEvidenceCampaignStore(options.isolatedStore, recordPath, options.evidence, options.realStore);
  await assertRealCampaignTerminal(options.realStore, options.evidence);
  const current = await store.read();
  if (!current) throw new Error('Isolated rehearsal campaign record does not exist.');
  assertSyntheticRehearsalState(current.state, options.rehearsalId, options.ownerId);
  const state = current.state, producerToken = state.runInventory!.fencing_token, now = Date.now();
  if (options.phase === 'pre-fence' && (state.status !== 'active' ||
      now < state.expiresAtMs && Math.floor((now - state.heartbeatAtMs) / HEARTBEAT_INTERVAL_MS) < MISSED_HEARTBEAT_LIMIT))
    throw new Error('Pre-fence stale-owner probe requires an expired active synthetic owner.');
  if (options.phase === 'post-fence' && (state.status === 'active' || state.fencingToken <= producerToken))
    throw new Error('Post-fence stale-owner probe requires a controller-issued fencing token.');
  let failure: unknown;
  try { await heartbeatCampaign(store, options.ownerId, producerToken, now); }
  catch (error) { failure = error; }
  if (!failure) throw new Error('Stale synthetic owner heartbeat unexpectedly succeeded.');
  const message = failure instanceof Error ? failure.message : 'Unexpected owner heartbeat rejection.';
  const expected = options.phase === 'pre-fence'
    ? /Campaign owner heartbeat is expired; mutation rejected\.|Campaign maximum lifetime has expired; heartbeat rejected\./.test(message)
    : /Campaign owner is fenced, stale or does not match the active run\./.test(message);
  if (!expected) throw failure;
  recordEvidence(options.evidence, 'stale_owner_write_rejected', { campaign_id: options.rehearsalId, phase: options.phase,
    status: state.status, producer_fencing_token: producerToken, current_fencing_token: state.fencingToken, error: message });
  return { status: 'owner_write_rejected', phase: options.phase, campaign_id: options.rehearsalId,
    producer_fencing_token: producerToken, current_fencing_token: state.fencingToken,
    rehearsal_fixture: 'synthetic custody fixture', cleanup_effects: 'simulated' };
}

async function main(args: string[]): Promise<void> {
  const parsed = parseArgs({ args, allowPositionals: false, options: {
    mode: { type: 'string' }, 'rehearsal-id': { type: 'string' }, 'owner-id': { type: 'string' },
    'env-file': { type: 'string' }, 'stamp-config': { type: 'string' }, 'evidence-file': { type: 'string' },
    once: { type: 'boolean' }, phase: { type: 'string' },
  } });
  const { mode, 'rehearsal-id': rehearsalId, 'owner-id': ownerId, 'env-file': envFile,
    'stamp-config': stampConfig, 'evidence-file': evidenceFile } = parsed.values;
  if (!['owner', 'watchdog', 'stale-owner'].includes(mode ?? '') || !rehearsalId || !uuid.test(rehearsalId) || !envFile || !stampConfig || !evidenceFile)
    throw new Error('Usage: aks-rehearsal.ts --mode owner|watchdog|stale-owner --rehearsal-id UUID --env-file PATH --stamp-config YAML --evidence-file PATH --owner-id UUID [--once] [--phase pre-fence|post-fence].');
  if (mode === 'owner' && (!ownerId || !uuid.test(ownerId) || parsed.values.once || parsed.values.phase))
    throw new Error('Owner mode requires --owner-id UUID and does not accept watchdog mode flags.');
  if (mode === 'watchdog' && (!ownerId || !uuid.test(ownerId) || !parsed.values.once || parsed.values.phase))
    throw new Error('Watchdog mode requires --owner-id UUID and --once.');
  if (mode === 'stale-owner' && (!ownerId || !uuid.test(ownerId) || !['pre-fence', 'post-fence'].includes(parsed.values.phase ?? '') ||
      parsed.values.once))
    throw new Error('Stale-owner mode requires --owner-id UUID and exactly one --phase pre-fence|post-fence.');
  const selected = loadCustodyConfig(['--env-file', envFile, '--stamp-config', stampConfig]);
  const evidence = createJsonLinesEvidenceSink(evidenceFile);
  const config = { account: selected.config.account, container: selected.config.checkpoints,
    subscriptionId: selected.config.subscription };
  const realStore = createReadOnlyEvidenceCampaignStore(createBlobCampaignStore(config), evidence);
  const recordPath = rehearsalCampaignRecordPath(rehearsalId);
  assertRehearsalPathIsolated(recordPath);
  const isolatedStore = createBlobCampaignStore({ ...config, recordPath });
  if (mode === 'owner') {
    const controller = new AbortController();
    const stop = () => controller.abort();
    process.once('SIGINT', stop);
    process.once('SIGTERM', stop);
    await runRehearsalOwner({ rehearsalId, ownerId: ownerId!, realStore, isolatedStore, evidence, signal: controller.signal });
    return;
  }
  if (mode === 'stale-owner') {
    console.log(JSON.stringify(await runRehearsalStaleOwnerProbe({ rehearsalId, ownerId: ownerId!,
      phase: parsed.values.phase as 'pre-fence' | 'post-fence', realStore, isolatedStore, evidence })));
    return;
  }
  const azure = new Azure(selected.config), readOnlyAzure = createReadOnlyAzureAdapter(azure, evidence);
  const evaluate = () => runRehearsalWatchdogOnce({ env: selected.env, rehearsalId, ownerId: ownerId!, realStore, isolatedStore, readOnlyAzure, evidence });
  console.log(JSON.stringify(await evaluate()));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : 'AKS custody rehearsal failed'); process.exitCode = 1;
  });
}
