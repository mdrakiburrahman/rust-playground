import { createHash } from 'node:crypto';
import { appendFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { Azure } from '../../infra/azure/bootstrap.js';
import { loadCustodyConfig } from './aks-custody-config.js';
import { createBlobCampaignStore, heartbeatCampaign, startCampaign, type CampaignState, type CampaignStore,
  type CleanupStepName, type StoredCampaign } from './aks-campaign.js';
import { runWatchdog, watchdogOnce, type WatchdogReadOnlyAzure } from './aks-watchdog.js';

export const REHEARSAL_BLOB_ROOT = '_arrowcol-custody-rehearsal';
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export interface RehearsalEvidenceRecord { observed_at: string; event: string; [key: string]: unknown }
export type RehearsalEvidenceSink = (record: RehearsalEvidenceRecord) => void;

function recordEvidence(sink: RehearsalEvidenceSink, event: string, values: Record<string, unknown> = {}): void {
  sink({ observed_at: new Date().toISOString(), event, ...values });
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

export function createEvidenceCampaignStore(store: CampaignStore, recordPath: string,
  evidence: RehearsalEvidenceSink): CampaignStore {
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
      const before = previous;
      try {
        const etag = await store.write(state, expectedEtag);
        previous = { state, etag };
        recordEvidence(evidence, 'campaign_transition', { path: recordPath, previous_etag: expectedEtag ?? null,
          etag, campaign_id: state.campaignId, from_status: before?.state.status ?? null, to_status: state.status,
          from_fencing_token: before?.state.fencingToken ?? null, fencing_token: state.fencingToken });
        return etag;
      } catch (error) {
        recordEvidence(evidence, 'blob_write_unconfirmed', { path: recordPath, previous_etag: expectedEtag ?? null,
          campaign_id: state.campaignId, status: state.status, fencing_token: state.fencingToken });
        throw error;
      }
    },
  };
}

export async function assertRealCampaignTerminal(store: CampaignStore, evidence: RehearsalEvidenceSink): Promise<void> {
  const stored = await store.read();
  recordEvidence(evidence, 'real_campaign_read', { path: 'active.json', etag: stored?.etag ?? null,
    campaign_id: stored?.state.campaignId ?? null, status: stored?.state.status ?? null,
    fencing_token: stored?.state.fencingToken ?? null });
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

export function recordingCleanupExecutor(evidence: RehearsalEvidenceSink) {
  return async (step: CleanupStepName, state: CampaignState, idempotencyKey: string): Promise<'complete'> => {
    recordEvidence(evidence, 'recording_cleanup_noop', { campaign_id: state.campaignId,
      fencing_token: state.fencingToken, step, idempotency_key: idempotencyKey });
    return 'complete';
  };
}

export async function runRehearsalWatchdogOnce(options: {
  env: NodeJS.ProcessEnv; rehearsalId: string; ownerId: string; realStore: CampaignStore;
  isolatedStore: CampaignStore; readOnlyAzure: WatchdogReadOnlyAzure; evidence: RehearsalEvidenceSink;
}): Promise<Record<string, unknown>> {
  const recordPath = rehearsalCampaignRecordPath(options.rehearsalId);
  assertRehearsalPathIsolated(recordPath);
  await assertRealCampaignTerminal(options.realStore, options.evidence);
  const store = createEvidenceCampaignStore(options.isolatedStore, recordPath, options.evidence);
  const current = await store.read();
  if (!current || current.state.campaignId !== options.rehearsalId || current.state.ownerId !== options.ownerId)
    throw new Error('Isolated rehearsal campaign identity does not match the requested run.');
  const result = await watchdogOnce(options.env, recordingCleanupExecutor(options.evidence), store, options.readOnlyAzure);
  recordEvidence(options.evidence, 'watchdog_result', { campaign_id: options.rehearsalId, ...result });
  return result;
}

export async function runRehearsalOwner(options: {
  rehearsalId: string; ownerId: string; realStore: CampaignStore; isolatedStore: CampaignStore;
  evidence: RehearsalEvidenceSink; signal: AbortSignal;
}): Promise<void> {
  const recordPath = rehearsalCampaignRecordPath(options.rehearsalId);
  assertRehearsalPathIsolated(recordPath);
  await assertRealCampaignTerminal(options.realStore, options.evidence);
  const store = createEvidenceCampaignStore(options.isolatedStore, recordPath, options.evidence);
  if (await store.read()) throw new Error('Isolated rehearsal record already exists; refusing to overwrite it.');
  const started = await startCampaign(store, options.rehearsalId, options.ownerId);
  recordEvidence(options.evidence, 'owner_started', { campaign_id: started.state.campaignId,
    owner_id: started.state.ownerId, fencing_token: started.state.fencingToken, etag: started.etag });
  while (!options.signal.aborted) {
    try { await delay(60_000, undefined, { signal: options.signal }); }
    catch (error) {
      if (options.signal.aborted && error instanceof Error && error.name === 'AbortError') return;
      throw error;
    }
    if (options.signal.aborted) return;
    const heartbeat = await heartbeatCampaign(store, options.ownerId, started.state.fencingToken);
    recordEvidence(options.evidence, 'owner_heartbeat', { campaign_id: heartbeat.state.campaignId,
      fencing_token: heartbeat.state.fencingToken, heartbeat_sequence: heartbeat.state.heartbeatSequence,
      heartbeat_at_unix_ms: heartbeat.state.heartbeatAtMs, etag: heartbeat.etag });
  }
}

async function main(args: string[]): Promise<void> {
  const parsed = parseArgs({ args, allowPositionals: false, options: {
    mode: { type: 'string' }, 'rehearsal-id': { type: 'string' }, 'owner-id': { type: 'string' },
    'env-file': { type: 'string' }, 'stamp-config': { type: 'string' }, 'evidence-file': { type: 'string' },
    once: { type: 'boolean' }, watch: { type: 'boolean' },
  } });
  const { mode, 'rehearsal-id': rehearsalId, 'owner-id': ownerId, 'env-file': envFile,
    'stamp-config': stampConfig, 'evidence-file': evidenceFile } = parsed.values;
  if (!['owner', 'watchdog'].includes(mode ?? '') || !rehearsalId || !uuid.test(rehearsalId) || !envFile || !stampConfig || !evidenceFile)
    throw new Error('Usage: aks-rehearsal.ts --mode owner|watchdog --rehearsal-id UUID --env-file PATH --stamp-config YAML --evidence-file PATH [--owner-id UUID] [--once|--watch].');
  if (mode === 'owner' && (!ownerId || !uuid.test(ownerId) || parsed.values.once || parsed.values.watch))
    throw new Error('Owner mode requires --owner-id UUID and does not accept watchdog mode flags.');
  if (mode === 'watchdog' && Boolean(parsed.values.once) === Boolean(parsed.values.watch))
    throw new Error('Watchdog mode requires exactly one of --once or --watch.');
  const selected = loadCustodyConfig(['--env-file', envFile, '--stamp-config', stampConfig]);
  const evidence = createJsonLinesEvidenceSink(evidenceFile);
  const config = { account: selected.config.account, container: selected.config.checkpoints,
    subscriptionId: selected.config.subscription };
  const realStore = createEvidenceCampaignStore(createBlobCampaignStore(config), 'active.json', evidence);
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
  if (mode !== 'watchdog' || !ownerId || !uuid.test(ownerId))
    throw new Error('Watchdog mode requires an explicit --owner-id UUID.');
  const azure = new Azure(selected.config), readOnlyAzure = createReadOnlyAzureAdapter(azure, evidence);
  const evaluate = () => runRehearsalWatchdogOnce({ env: selected.env, rehearsalId, ownerId, realStore, isolatedStore, readOnlyAzure, evidence });
  if (parsed.values.once) {
    console.log(JSON.stringify(await evaluate()));
    return;
  }
  const controller = new AbortController();
  const stop = () => controller.abort();
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  await runWatchdog(controller.signal, evaluate);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : 'AKS custody rehearsal failed'); process.exitCode = 1;
  });
}
