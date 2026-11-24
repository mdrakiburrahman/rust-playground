import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { Azure, Config, runtime } from '../../infra/azure/bootstrap.js';
import { EffectBoundary, withEffectBoundary } from '../../infra/azure/effect-boundary.js';
import { CampaignConflictError, HEARTBEAT_INTERVAL_MS, MAX_CAMPAIGN_LIFETIME_MS, newCampaignState,
  validateCampaignState, type CampaignState, type CampaignStore, type StoredCampaign } from './aks-campaign.js';
import { assertRehearsalPathIsolated, createReadOnlyAzureAdapter, createSyntheticArmedCampaignState, ReadOnlyAzureAdapter, rehearsalCampaignRecordPath,
  runRehearsalOwner, runRehearsalOwnerHeartbeatOnce, runRehearsalStaleOwnerProbe, runRehearsalWatchdogOnce,
  type ReadOnlyCampaignStore, type RehearsalEvidenceRecord } from './aks-rehearsal.js';
import { watchdogOnce } from './aks-watchdog.js';

const campaignId = 'c0ffee00-0000-4000-8000-000000000001';
const ownerId = 'c0ffee00-0000-4000-8000-000000000002';
const subscriptionId = 'c0ffee00-0000-4000-8000-000000000003';
const realCampaignId = 'c0ffee00-0000-4000-8000-000000000004';
const realOwnerId = 'c0ffee00-0000-4000-8000-000000000005';

class MemoryStore implements CampaignStore {
  current?: StoredCampaign;
  revision = 0;
  writes = 0;
  reads = 0;

  async read() {
    this.reads++;
    return this.current ? structuredClone(this.current) : undefined;
  }

  async write(state: CampaignState, etag?: string) {
    if (etag !== this.current?.etag) throw new CampaignConflictError();
    const next = 'etag-' + (++this.revision);
    this.writes++;
    this.current = { state: structuredClone(state), etag: next };
    return next;
  }
}

function readOnlyAdapter(evidence: RehearsalEvidenceRecord[], requests: string[] = []): ReadOnlyAzureAdapter {
  return new ReadOnlyAzureAdapter(async (method, url) => {
    requests.push(method + ' ' + url);
    return { status: 200, body: Buffer.from('{}'), requestId: 'request-1' };
  }, event => evidence.push(event));
}

async function startAndStopOwner(realStore = new MemoryStore(), rehearsal = campaignId, owner = ownerId) {
  const isolatedStore = new MemoryStore();
  const evidence: RehearsalEvidenceRecord[] = [];
  const controller = new AbortController();
  controller.abort();
  await runRehearsalOwner({ rehearsalId: rehearsal, ownerId: owner, realStore, isolatedStore,
    evidence: event => evidence.push(event), signal: controller.signal });
  return { realStore, isolatedStore, evidence };
}

function ageOwner(store: MemoryStore): void {
  assert.ok(store.current);
  const startedAtMs = Date.now() - 4 * HEARTBEAT_INTERVAL_MS;
  store.current = { ...store.current, state: validateCampaignState({ ...store.current.state,
    startedAtMs, heartbeatAtMs: startedAtMs, expiresAtMs: startedAtMs + MAX_CAMPAIGN_LIFETIME_MS }) };
}

function assertSyntheticEvidence(evidence: RehearsalEvidenceRecord[]): void {
  assert.ok(evidence.length > 0);
  assert.ok(evidence.every(event => event.rehearsal_fixture === 'synthetic custody fixture' && event.cleanup_effects === 'simulated'));
}

test('rehearsal record path is fixed to a disjoint prefix', () => {
  assert.equal(rehearsalCampaignRecordPath(campaignId), '_arrowcol-custody-rehearsal/' + campaignId + '/active.json');
  assert.doesNotThrow(() => assertRehearsalPathIsolated(rehearsalCampaignRecordPath(campaignId)));
  assert.throws(() => assertRehearsalPathIsolated('active.json'), /not isolated/);
  assert.throws(() => assertRehearsalPathIsolated('_arrowcol-custody/campaigns/run/active.json'), /not isolated/);
  assert.throws(() => assertRehearsalPathIsolated('_arrowcol-custody-rehearsal/../active.json'), /not isolated/);
});

test('synthetic armed fixture validates while preserving initial unprovisioned inventory', () => {
  const state = createSyntheticArmedCampaignState(campaignId, ownerId);
  assert.equal(state.mode, 'armed');
  assert.equal(state.status, 'active');
  assert.equal(state.runInventory?.load_admission, 'not_authorized');
  assert.equal(state.runInventory?.namespace.uid, null);
  assert.ok(state.runInventory?.resources.every(resource => resource.state === 'planned' && resource.uid === null));
  assert.equal(state.runInventory?.fic?.state, 'planned');
  assert.equal(state.runInventory?.qap?.state, 'planned');
  assert.equal(state.runInventory?.qap?.bindings.length, 0);
  assert.equal(state.operations.length, 0);
  assert.match(state.runInventory?.namespace.name ?? '', /^d40a-synthetic-/);
  assert.equal(validateCampaignState(state).campaignId, campaignId);
});

test('non-terminal real campaign blocks before any isolated write', async () => {
  const realStore = new MemoryStore(), isolatedStore = new MemoryStore();
  realStore.current = { state: newCampaignState(campaignId, ownerId), etag: 'real-active' };
  const evidence: RehearsalEvidenceRecord[] = [];
  await assert.rejects(runRehearsalWatchdogOnce({ env: { ARM_SUBSCRIPTION_ID: subscriptionId }, rehearsalId: campaignId,
    ownerId, realStore, isolatedStore, readOnlyAzure: readOnlyAdapter(evidence), evidence: event => evidence.push(event) }),
  /Real campaign record is non-terminal/);
  await assert.rejects(runRehearsalStaleOwnerProbe({ rehearsalId: campaignId, ownerId, phase: 'pre-fence', realStore,
    isolatedStore, evidence: event => evidence.push(event) }), /Real campaign record is non-terminal/);
  assert.equal(isolatedStore.writes, 0);
  assert.equal(evidence.some(event => event.event === 'real_campaign_read' && event.status === 'active'), true);
  assertSyntheticEvidence(evidence);
});

test('owner heartbeat is gated by a fresh real-record read and stop leaves no extra write', async () => {
  const { realStore, isolatedStore, evidence } = await startAndStopOwner();
  assert.equal(isolatedStore.current?.state.mode, 'armed');
  assert.equal(evidence.some(event => event.event === 'owner_stopped'), true);
  const heartbeat = await runRehearsalOwnerHeartbeatOnce({ rehearsalId: campaignId, ownerId, realStore,
    isolatedStore, evidence: event => evidence.push(event) });
  assert.equal(heartbeat.state.heartbeatSequence, 1);
  const writesBeforeRefusal = isolatedStore.writes;
  realStore.current = { state: newCampaignState(realCampaignId, realOwnerId), etag: 'real-active' };
  await assert.rejects(runRehearsalOwnerHeartbeatOnce({ rehearsalId: campaignId, ownerId, realStore,
    isolatedStore, evidence: event => evidence.push(event) }), /Real campaign record is non-terminal/);
  assert.equal(isolatedStore.writes, writesBeforeRefusal);
  assertSyntheticEvidence(evidence);
});

test('watchdog reconciliation rechecks the real record immediately before its isolated CAS', async () => {
  const { isolatedStore } = await startAndStopOwner();
  ageOwner(isolatedStore);
  let reads = 0;
  const changingRealStore: ReadOnlyCampaignStore = { async read() {
    reads++;
    return reads === 1 ? undefined : { state: newCampaignState(realCampaignId, realOwnerId), etag: 'real-active' };
  } };
  const evidence: RehearsalEvidenceRecord[] = [];
  const writesBefore = isolatedStore.writes;
  await assert.rejects(runRehearsalWatchdogOnce({ env: { ARM_SUBSCRIPTION_ID: subscriptionId }, rehearsalId: campaignId,
    ownerId, realStore: changingRealStore, isolatedStore, readOnlyAzure: readOnlyAdapter(evidence),
    evidence: event => evidence.push(event) }), /Real campaign record is non-terminal/);
  assert.equal(reads, 2);
  assert.equal(isolatedStore.writes, writesBefore);
  assert.equal(evidence.some(event => event.event === 'real_campaign_read' && event.status === 'active'), true);
  assertSyntheticEvidence(evidence);
});

test('expired owner is rejected before and after fencing; terminal cleanup is idempotent', async () => {
  const { realStore, isolatedStore, evidence } = await startAndStopOwner();
  await runRehearsalOwnerHeartbeatOnce({ rehearsalId: campaignId, ownerId, realStore, isolatedStore,
    evidence: event => evidence.push(event) });
  ageOwner(isolatedStore);
  const preFence = await runRehearsalStaleOwnerProbe({ rehearsalId: campaignId, ownerId, phase: 'pre-fence',
    realStore, isolatedStore, evidence: event => evidence.push(event) });
  assert.equal(preFence.status, 'owner_write_rejected');

  const requests: string[] = [];
  const adapter = readOnlyAdapter(evidence, requests);
  const evaluate = () => runRehearsalWatchdogOnce({ env: { ARM_SUBSCRIPTION_ID: subscriptionId }, rehearsalId: campaignId,
    ownerId, realStore, isolatedStore, readOnlyAzure: adapter, evidence: event => evidence.push(event) });
  let result: Record<string, unknown> | undefined;
  for (let attempt = 0; attempt < 20; attempt++) {
    const readsBefore = realStore.reads, writesBefore = isolatedStore.writes;
    result = await evaluate();
    if (attempt === 0) assert.equal(realStore.reads - readsBefore, isolatedStore.writes - writesBefore + 1);
    if (result.campaignStatus === 'complete') break;
  }
  assert.equal(result?.campaignStatus, 'complete');
  const postFence = await runRehearsalStaleOwnerProbe({ rehearsalId: campaignId, ownerId, phase: 'post-fence',
    realStore, isolatedStore, evidence: event => evidence.push(event) });
  assert.equal(postFence.status, 'owner_write_rejected');
  const terminalWrites = isolatedStore.writes;
  const cleanupEffects = evidence.filter(event => event.event === 'recording_cleanup_noop').length;
  await evaluate();
  await evaluate();
  assert.equal(isolatedStore.current?.state.status, 'complete');
  assert.equal(isolatedStore.writes, terminalWrites);
  assert.equal(evidence.filter(event => event.event === 'recording_cleanup_noop').length, cleanupEffects);
  assert.equal(requests.length, 0);
  assertSyntheticEvidence(evidence);
});

test('CAS conflict is surfaced without retrying synthetic fixture creation', async () => {
  class ConflictStore implements CampaignStore {
    attempts = 0;
    async read() { return undefined; }
    async write(): Promise<string> { this.attempts++; throw new CampaignConflictError(); }
  }
  const store = new ConflictStore(), evidence: RehearsalEvidenceRecord[] = [], realStore = new MemoryStore();
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(runRehearsalOwner({ rehearsalId: campaignId, ownerId, realStore, isolatedStore: store,
    evidence: event => evidence.push(event), signal: controller.signal }), CampaignConflictError);
  assert.equal(store.attempts, 1);
  assert.equal(evidence.some(event => event.event === 'blob_write_cas_conflict'), true);
  assertSyntheticEvidence(evidence);
});

test('store override without injected cleanup executor refuses before default providers', async () => {
  const { isolatedStore, evidence } = await startAndStopOwner();
  ageOwner(isolatedStore);
  const requests: string[] = [];
  await assert.rejects(watchdogOnce({ ARM_SUBSCRIPTION_ID: subscriptionId }, undefined, isolatedStore,
    readOnlyAdapter(evidence, requests)), /requires an external cleanup executor/);
  assert.equal(isolatedStore.current?.state.status, 'fenced');
  assert.equal(requests.length, 0);
  assertSyntheticEvidence(evidence);
});

test('default rehearsal ARM fetch is refused by the campaign boundary before network forwarding', async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'oat-rehearsal-effect-boundary-'));
  fs.chmodSync(directory, 0o700);
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const config = new Config({ AZURE_SUBSCRIPTION_ID: subscriptionId, AZURE_STAMP: '12345', AZURE_LOCATION: 'canadacentral' });
  const azure = new Azure(config), resource = 'https://management.azure.com/';
  azure.tokens.set(resource, { at: runtime.now(), token: 'test-token', lifetime: 240 });
  const url = 'https://management.azure.com/subscriptions/' + subscriptionId + '/resourceGroups/' + config.group + '?api-version=2024-10-01';
  const envelope = { schema_version: 1 as const, campaign_id: campaignId, run_id: ownerId,
    subscription_id: config.subscription, resource_group: config.group, storage_account: config.account,
    state_container: config.checkpoints, effects: [{ id: 'other-read', transport: 'azure-rest' as const,
      method: 'GET', resource: 'https://management.azure.com|https://management.azure.com/', path: '/not-allocated', max_calls: 1 }] };
  const boundary = new EffectBoundary(envelope, path.join(directory, 'effects.jsonl'));
  const adapter = createReadOnlyAzureAdapter(azure, () => {});
  let fetches = 0;
  t.mock.method(globalThis, 'fetch', async () => { fetches++; return new Response('{}', { status: 200 }); });
  await assert.rejects(withEffectBoundary(boundary, () => adapter.readArmStatus(url)), /refused before forwarding/);
  assert.equal(fetches, 0);
  const rows = fs.readFileSync(path.join(directory, 'effects.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.deepEqual(rows.map(row => row.event), ['refused']);
});

test('read-only ARM adapter refuses non-GET and off-host requests before sending', async () => {
  const sent: string[] = [], evidence: RehearsalEvidenceRecord[] = [];
  const adapter = new ReadOnlyAzureAdapter(async (method, url) => {
    sent.push(method + ' ' + url);
    return { status: 200, body: Buffer.from('{"status":"Succeeded"}'), requestId: 'request-1' };
  }, event => evidence.push(event));
  const url = 'https://management.azure.com/subscriptions/' + subscriptionId + '/operations/one?api-version=2024-01-01';
  assert.deepEqual(await adapter.readArmStatus(url), { status: 200, body: { status: 'Succeeded' } });
  await assert.rejects(adapter.request('PUT', url), /refuses non-GET/);
  await assert.rejects(adapter.request('GET', 'https://example.com/resource'), /non-management/);
  assert.deepEqual(sent, ['GET ' + url]);
  assert.equal(evidence[0]?.request_id, 'request-1');
  assert.equal(evidence[0]?.response_body_sha256, createHash('sha256').update('{"status":"Succeeded"}').digest('hex'));
  assertSyntheticEvidence(evidence);
});
