import { EffectBoundary, withEffectBoundary, type EffectEnvelope } from '../../infra/azure/effect-boundary.js';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { chmodSync, mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { createHash } from 'node:crypto';
import type { Config } from '../../infra/azure/bootstrap.js';
import { CampaignConflictError, CUSTODY_CLEANUP_STEPS, MAX_CAMPAIGN_LIFETIME_MS, KNOWN_LIFECYCLE_CLEANUP_RESERVE_MS, assessCellLifetimeBudget,
  MISSED_HEARTBEAT_LIMIT, HEARTBEAT_INTERVAL_MS,
  createBlobCampaignStore,
  armCampaign, assertCampaignEnvironment, assertRunInventoryReady, campaignStartReceipt, heartbeatCampaign, mutateCampaign, newCampaignState, publishCleanupRunInventory, publishRunInventory, runInventoryPublishReceipt, startCampaign,
  validateCampaignState, type CampaignState, type CampaignStore, type RunInventory, type StoredCampaign,
  CampaignWriteOutcomeUnknownError, campaignMain, recordArmOperation } from './aks-campaign.js';
import { evaluateCustody, reconcileArmOperation, reconcileCleanupOnce, reconcileOnce, reconcilePendingArmOperations } from './aks-reconcile.js';
import { newScaleTrialReceipt, SCALE_RESTORE_TIMEOUT_MS, type ScaleStep } from './aks-scale.js';

const campaignId = 'c0ffee00-0000-4000-8000-000000000001';
const ownerId = 'c0ffee00-0000-4000-8000-000000000002';
const subscriptionId = 'c0ffee00-0000-4000-8000-000000000003';
const resourceId = `/subscriptions/${subscriptionId}/resourceGroups/perf69/providers/Microsoft.ContainerService/managedClusters/bench`;
const operationUrl = `https://management.azure.com/subscriptions/${subscriptionId}/providers/Microsoft.ContainerService/locations/westus/operations/123?api-version=2024-10-01`;

function chunkedResponse(chunks: Uint8Array[], headers: Record<string, string> = {}, close = true,
  status = 200, etag: string | null = '"state-v1"') {
  let cancelled = false, index = 0;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (index < chunks.length) {
        controller.enqueue(chunks[index++]!);
        if (close && index === chunks.length) controller.close();
      }
    },
    cancel() { cancelled = true; },
  });
  const responseHeaders = { ...headers };
  if (etag !== null) responseHeaders.etag = etag;
  return {
    response: new Response(body, { status, headers: responseHeaders }),
    wasCancelled: () => cancelled,
  };
}

class MemoryStore implements CampaignStore {
  current?: StoredCampaign;
  revision = 0;
  async read() { return this.current ? structuredClone(this.current) : undefined; }
  async write(state: CampaignState, etag?: string) {
    if (etag !== this.current?.etag) throw new CampaignConflictError();
    const next = `"etag-${++this.revision}"`;
    this.current = { state: structuredClone(validateCampaignState(state)), etag: next };
    return next;
  }
}

const seed = async (now = 1_000_000) => {
  const store = new MemoryStore();
  await startCampaign(store, campaignId, ownerId, now);
  return store;
};
function directSetupFixture(fault: 'none' | 'lost-pool-ack' | 'lost-intent-write' | 'lost-start-ack') {
  const store = new MemoryStore(), inventory = inventoryFor(newCampaignState(campaignId, ownerId));
  const { cell_run_id: runId, namespace } = inventory;
  const base = `/subscriptions/${subscriptionId}/resourceGroups/perf69`, identityId = `${base}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/arrow-collector`;
  const credentialName = `arrowcol-run-${runId}`;
  const ficId = `${identityId}/federatedIdentityCredentials/${credentialName}`;
  inventory.qap = null;
  inventory.fic = { resource_id: ficId, name: credentialName, state: 'planned', absence: null, plan: {
    version: 1, runId, namespace: namespace.name, serviceAccount: 'arrow-collector', identityId, resourceId: ficId,
    clientId: campaignId, tenantId: ownerId, aksId: resourceId, server: 'https://bench.example.com',
    credentialName, issuer: 'https://issuer.example.com/', subject: `system:serviceaccount:${namespace.name}:arrow-collector`,
    audience: 'api://AzureADTokenExchange',
  } };
  inventory.resources = inventory.resources.map(resource => resource.role === 'client' ?
    { ...resource, api_version: 'batch/v1', kind: 'Job', name: 'direct-load-confirmwide' } : resource);
  const config = { base, cluster: 'bench', identity_id: identityId, subscription: subscriptionId, account: 'fixtureaccount',
    checkpoints: 'checkpoints' } as Config;
  const calls: string[] = [];
  const request: Azure['request'] = async (method, url, _resource, body, headers, _missingOk, idempotent) => {
    calls.push(method);
    const id = resourceId + '/agentPools/arrowe192';
    assert.equal(url, `https://management.azure.com${id}?api-version=2024-10-01`);
    if (method === 'GET') return [Buffer.from(JSON.stringify({ id: id.replace('/resourceGroups/', '/resourcegroups/'), properties: { mode: 'User', vmSize: 'Standard_E192ids_v6',
      count: 0, enableAutoScaling: true, minCount: 0, maxCount: 1, provisioningState: 'Succeeded' } })),
    { ':status': '200', etag: '"pool-before"' }];
    assert.equal(method, 'PUT');
    assert.equal(idempotent, false);
    const intent = (await store.read())?.state.directSetup?.firstPool;
    assert.ok(intent && intent.response === null);
    assert.equal(headers?.['If-Match'], intent.expectedEtag);
    assert.equal(headers?.['x-ms-client-request-id'], intent.requestId);
    assert.equal(createHash('sha256').update(body!).digest('hex'), intent.bodySha256);
    assert.equal(JSON.parse(body!.toString()).properties.minCount, 1);
    if (fault === 'lost-pool-ack') throw new Error('injected unknown pool effect');
    return [null, { ':status': '202', 'x-ms-request-id': 'fixture-pool-ack' }];
  };
  const azure = { c: config, request } as Azure;
  const write = store.write.bind(store);
  store.write = async (state, etag) => {
    if (fault === 'lost-intent-write' && state.directSetup?.firstPool) throw new Error('injected unknown intent write without commit');
    const written = await write(state, etag);
    if (fault === 'lost-start-ack' && !etag) throw new Error('injected lost start acknowledgment after commit');
    return written;
  };
  const env = { CAMPAIGN_ID: campaignId, CAMPAIGN_OWNER_ID: ownerId, CAMPAIGN_FENCING_TOKEN: '1' };
  const cli = (action: string, ...args: string[]) => campaignMain([action, ...args], { store, azure, env,
    loadConfig: positionals => ({ config, env, positionals, once: false }), runInventoryInput: inventory,
    directSetupBaseline: async (_azure, state) => ({ status: 'captured', campaign_id: campaignId, owner_id: ownerId,
      fencing_token: 1, etag: '"original-baseline"', blob: 'fixture-baseline', captured_at_unix_ms: state.startedAtMs }) });
  return { cli, store, calls };
}

test('direct setup CLI retires original start-only loss without claiming cleanup or touching outside resources', async () => {
  const { cli, store, calls } = directSetupFixture('lost-start-ack');
  await cli('start-direct-setup', campaignId, ownerId);
  await cli('cancel-direct-setup', ownerId, '1');
  const retired = (await store.read())!.state;
  assert.equal(retired.status, 'setup_retired');
  assert.equal(retired.campaignId, campaignId);
  assert.equal(retired.runInventory?.load_admission, 'not_authorized');
  assert.equal(retired.cleanupStatus, 'not-started');
  assert.deepEqual(retired.cleanupSteps, []);
  await assert.rejects(cli('dispatch-direct-first-pool', ownerId, '1'), /fenced|stale/);
  await assert.rejects(cli('start-direct-setup', campaignId, ownerId), /original campaign/);
  await assert.rejects(cli('start', campaignId, ownerId), /reuse a retired/);
  assert.deepEqual(calls, []);
});

test('direct setup CLI refuses a legacy observe record and does not infer non-dispatch from an empty journal', async () => {
  const { cli, store, calls } = directSetupFixture('none');
  await startCampaign(store, campaignId, ownerId);
  await assert.rejects(cli('cancel-direct-setup', ownerId, '1'), /fenced|stale/);
  assert.equal((await store.read())!.state.status, 'active');
  assert.deepEqual(calls, []);
});

test('direct setup CLI preserves the pool acknowledgement without admitting load or permitting another dispatch', async () => {
  const { cli, store, calls } = directSetupFixture('none');
  await cli('start-direct-setup', campaignId, ownerId);
  await cli('dispatch-direct-first-pool', ownerId, '1');
  const state = (await store.read())!.state;
  assert.deepEqual(state.directSetup?.firstPool?.response, { status: 202, requestId: 'fixture-pool-ack' });
  assert.equal(state.runInventory?.load_admission, 'not_authorized');
  await assert.rejects(armCampaign(store, ownerId, 1), /cannot continue/);
  await assert.rejects(cli('dispatch-direct-first-pool', ownerId, '1'), /do not replay/);
  assert.deepEqual(calls, ['GET', 'PUT']);
});

for (const fault of ['lost-pool-ack', 'lost-intent-write'] as const) test(`direct setup CLI handles ${fault} without replaying a dispatched pool`, async () => {
  const { cli, store, calls } = directSetupFixture(fault);
  await cli('start-direct-setup', campaignId, ownerId);
  await assert.rejects(cli('dispatch-direct-first-pool', ownerId, '1'), fault === 'lost-pool-ack' ? /unknown pool effect/ : /readback/);
  const state = (await store.read())!.state;
  assert.equal(state.runInventory?.load_admission, 'not_authorized');
  if (fault === 'lost-pool-ack') {
    assert.equal(state.directSetup?.firstPool?.response, null);
    await assert.rejects(cli('dispatch-direct-first-pool', ownerId, '1'), /do not replay/);
    await assert.rejects(cli('cancel-direct-setup', ownerId, '1'), /zero provisioning dispatch/);
    assert.deepEqual(calls, ['GET', 'PUT']);
  } else {
    assert.equal(state.directSetup?.firstPool, null);
    assert.deepEqual(calls, ['GET']);
  }
});

const inventoryFor = (state: CampaignState, observed = false): RunInventory => {
  const cellRunId = 'c0ffee00-0000-4000-8000-000000000004';
  const orchestrationRunId = 'c0ffee00-0000-4000-8000-000000000005';
  const namespace = 'arrowcol-' + cellRunId.replace(/-/g, '');
  const names = ['arrowcol', 'arrowcol-client', 'delta-maintenance'];
  const uids = ['c0ffee00-0000-4000-8000-000000000006', 'c0ffee00-0000-4000-8000-000000000007',
    'c0ffee00-0000-4000-8000-000000000008'];
  const stateName = observed ? 'observed' : 'planned';
  const step = { state: 'not_started' as const, deadline_unix_ms: null, reason_code: null };
  return {
    schema_version: 1, campaign_id: state.campaignId, owner_id: state.ownerId, fencing_token: state.runInventory?.fencing_token ?? state.fencingToken,
    orchestration_run_id: orchestrationRunId, cell_run_id: cellRunId, load_admission: state.runInventory?.load_admission ?? 'not_authorized',
    planned_cell_duration_ms: 60_000, effective_stop_budgets: { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 1_200_000 },
    namespace: { name: namespace, uid: observed ? 'c0ffee00-0000-4000-8000-000000000009' : null, owner_token: state.ownerId },
    resources: names.map((name, index) => ({ api_version: 'apps/v1', kind: 'Deployment', namespace, name,
      uid: observed ? uids[index]! : null, absence: null, role: index === 0 ? 'collector' : index === 1 ? 'client' : 'maintenance', state: stateName })),
    fic: { resource_id: '/subscriptions/' + subscriptionId + '/resourceGroups/perf69/providers/Microsoft.ManagedIdentity/userAssignedIdentities/arrow-collector/federatedIdentityCredentials/arrowcol-run-' + cellRunId,
      name: 'arrowcol-run-' + cellRunId, state: stateName, absence: null },
    qap: { database: 'perf69-qap', bindings: [{ external_table: 'arrowcol_data', relative_path: 'perf69/' + cellRunId }], state: stateName, absence: [] },
    cleanup: { stop_admission: { ...step }, bounded_drain: { ...step }, evidence_capture: { ...step }, stop_maintenance: { ...step }, terminal_capture: { ...step }, handoff: { ...step } },
  };
};
const armForTest = async (store: MemoryStore, now = 1_000_000, ready = true) => {
  const state = (await store.read())!.state;
  await publishRunInventory(store, inventoryFor(state), now);
  await armCampaign(store, ownerId, state.fencingToken, now);
  const planned = (await store.read())!.state.runInventory!;
  await publishRunInventory(store, { ...planned, load_admission: 'authorization_pending' }, now);
  await publishRunInventory(store, { ...planned, load_admission: 'authorized' }, now);
  if (ready) await publishRunInventory(store, inventoryFor((await store.read())!.state, true), now + 1);
  return (await store.read())!.state;
};

test('load admission requires a pre-provisioning initial record and cannot skip or reopen authorization', async () => {
  const store = await seed(), started = (await store.read())!.state, planned = inventoryFor(started);
  const { load_admission: _legacy, ...legacy } = planned;
  await assert.rejects(publishRunInventory(store, legacy, started.startedAtMs), /malformed/);
  await assert.rejects(publishRunInventory(store, { ...planned, load_admission: 'invalid' }, started.startedAtMs), /malformed/);
  await assert.rejects(publishRunInventory(store, { ...planned, load_admission: 'authorized' }, started.startedAtMs), /before external provisioning/);
  await assert.rejects(publishRunInventory(store, inventoryFor(started, true), started.startedAtMs), /before external provisioning/);
  await publishRunInventory(store, planned, started.startedAtMs);
  await assert.rejects(publishRunInventory(store, { ...planned, load_admission: 'authorized' }, started.startedAtMs), /authorization_pending/);
  await publishRunInventory(store, { ...planned, load_admission: 'authorization_pending' }, started.startedAtMs);
  assert.equal((await store.read())!.state.runInventory!.load_admission, 'authorization_pending');
  await publishRunInventory(store, { ...planned, load_admission: 'authorized' }, started.startedAtMs);
  await assert.rejects(publishRunInventory(store, planned, started.startedAtMs), /monotonically/);
  assert.equal((await store.read())!.state.runInventory!.load_admission, 'authorized');
});

test('Rust RunInventory timing fields are strictly parsed and legacy inventory cannot arm', async () => {
  const store = await seed(), started = (await store.read())!.state;
  const rustInventory = JSON.parse(JSON.stringify(inventoryFor(started))) as RunInventory;
  await publishRunInventory(store, rustInventory, started.startedAtMs);
  const invalidBudgets = [
    { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 1_199_999 },
    { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 5_100_001 },
    { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 1_200_000, extra: true },
  ];
  for (const effective_stop_budgets of invalidBudgets)
    await assert.rejects(publishRunInventory(store, { ...rustInventory, effective_stop_budgets }, started.startedAtMs),
      /effective|timeout/);

  const legacyStore = await seed(), legacyStarted = (await legacyStore.read())!.state;
  const inventory = inventoryFor(legacyStarted);
  const { planned_cell_duration_ms: _duration, effective_stop_budgets: _budgets, ...legacy } = inventory;
  legacyStore.current = { state: validateCampaignState({ ...legacyStarted, runInventory: legacy }), etag: 'legacy-etag' };
  await assert.rejects(armCampaign(legacyStore, ownerId, legacyStarted.fencingToken, legacyStarted.startedAtMs),
    /without a new-cell duration and maintenance stop budget/);
});

test('bounded lifetime arithmetic admits equality and rejects a one-millisecond shortfall', async () => {
  const stopTimeout = 1_200_000;
  const duration = MAX_CAMPAIGN_LIFETIME_MS - stopTimeout - KNOWN_LIFECYCLE_CLEANUP_RESERVE_MS;
  assert.deepEqual(assessCellLifetimeBudget(MAX_CAMPAIGN_LIFETIME_MS, duration, stopTimeout,
    KNOWN_LIFECYCLE_CLEANUP_RESERVE_MS), { status: 'ready', required_ms: MAX_CAMPAIGN_LIFETIME_MS });
  assert.deepEqual(assessCellLifetimeBudget(MAX_CAMPAIGN_LIFETIME_MS - 1, duration, stopTimeout,
    KNOWN_LIFECYCLE_CLEANUP_RESERVE_MS), { status: 'insufficient', required_ms: MAX_CAMPAIGN_LIFETIME_MS });
});

test('cell admission retains UNKNOWN for unbounded cleanup completion stages', async () => {
  const state = newCampaignState(campaignId, ownerId, 100);
  const store = new MemoryStore(); store.current = { state, etag: 'etag-1' };
  const assessment = assessCellLifetimeBudget(MAX_CAMPAIGN_LIFETIME_MS, 0, 1_200_000);
  assert.equal(assessment.status, 'unknown');
  if (assessment.status === 'unknown') {
    assert.equal(assessment.required_ms, 1_200_000 + KNOWN_LIFECYCLE_CLEANUP_RESERVE_MS);
    assert.match(assessment.reason, /preserve-evidence.*remove-run-namespace/);
  }
  await publishRunInventory(store, { ...inventoryFor(state), planned_cell_duration_ms: 0 }, state.startedAtMs);
  assert.equal((await store.read())!.state.runInventory!.planned_cell_duration_ms, 0);
});

test('zero-duration rehearsal stays not-authorized and cell duration/stop budgets cannot change', async () => {
  const store = await seed(), started = (await store.read())!.state;
  const rowFree = { ...inventoryFor(started), planned_cell_duration_ms: 0 };
  await publishRunInventory(store, rowFree, started.startedAtMs);
  await assert.rejects(publishRunInventory(store, { ...rowFree, load_admission: 'authorization_pending' }, started.startedAtMs), /malformed/);
  await assert.rejects(publishRunInventory(store, { ...rowFree, planned_cell_duration_ms: 1 }, started.startedAtMs), /immutable/);
  await assert.rejects(publishRunInventory(store, { ...rowFree,
    effective_stop_budgets: { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 1_200_001 } }, started.startedAtMs), /immutable/);
});

test('maintenance milestone is required, bounded, monotonic and required for handoff', async () => {
  const store = await seed(), inventory = inventoryFor((await store.read())!.state);
  await publishRunInventory(store, inventory, 1_000_000);
  const { stop_maintenance: _omitted, ...legacy } = inventory.cleanup;
  await assert.rejects(publishRunInventory(store, { ...inventory, cleanup: legacy }, 1_000_000), /malformed/);
  for (const deadline of [null, 120_000]) {
    const invalid = structuredClone(inventory);
    invalid.cleanup.stop_maintenance = { state: 'running', deadline_unix_ms: deadline, reason_code: null };
    await assert.rejects(publishRunInventory(store, invalid, 1_000_000), /absolute deadline/);
  }
  const running = structuredClone(inventory);
  running.cleanup.stop_maintenance = { state: 'running', deadline_unix_ms: 1_120_000, reason_code: null };
  await publishRunInventory(store, running, 1_000_000);
  const complete = structuredClone(running);
  complete.cleanup.stop_maintenance.state = 'complete';
  await publishRunInventory(store, complete, 1_000_000);
  await assert.rejects(publishRunInventory(store, running, 1_000_000), /Completed cleanup state/);
  const unresolved = inventoryFor((await store.read())!.state);
  unresolved.resources = []; unresolved.fic = null; unresolved.qap = null;
  for (const [name, step] of Object.entries(unresolved.cleanup)) if (name !== 'stop_maintenance') step.state = 'complete';
  await assert.rejects(publishRunInventory(store, unresolved, 1_000_000), /handoff cannot complete/);
});

test('new campaign is observe-only with fixed heartbeat and lifetime bounds', () => {
  const state = newCampaignState(campaignId, ownerId, 100);
  assert.equal(state.mode, 'observe');
  assert.equal(state.expiresAtMs, 100 + MAX_CAMPAIGN_LIFETIME_MS);
  assert.equal(HEARTBEAT_INTERVAL_MS, 60_000);
  assert.equal(MISSED_HEARTBEAT_LIMIT, 3);
});

test('campaign start receipt provides strict runner environment identities', () => {
  const state = newCampaignState(campaignId, ownerId, 100);
  const receipt = campaignStartReceipt(state);
  assert.deepEqual(receipt.environment, { CAMPAIGN_ID: campaignId, CAMPAIGN_OWNER_ID: ownerId, CAMPAIGN_FENCING_TOKEN: '1' });
  assert.equal(receipt.status, 'started');
  assert.doesNotThrow(() => assertCampaignEnvironment(receipt.environment, state));
  assert.throws(() => assertCampaignEnvironment({ CAMPAIGN_ID: campaignId }, state), /does not match the durable fence/);
});

test('heartbeat cannot revive expired custody and stale fencing tokens are rejected', async () => {
  const store = await seed();
  const started = (await store.read())!.state;
  const beat = await heartbeatCampaign(store, ownerId, started.fencingToken, started.heartbeatAtMs + 1);
  assert.equal(beat.state.heartbeatSequence, 1);
  assert.equal(beat.state.expiresAtMs, started.expiresAtMs);
  await assert.rejects(heartbeatCampaign(store, ownerId, started.fencingToken, started.expiresAtMs), /expired/);
});

test('observer does not fence; armed controller fences once at three missed beats', async () => {
  const observedStore = await seed();
  const observedStart = (await observedStore.read())!.state;
  await publishRunInventory(observedStore, inventoryFor(observedStart), observedStart.startedAtMs);
  const expiredAt = observedStart.heartbeatAtMs + 3 * HEARTBEAT_INTERVAL_MS;
  assert.equal(evaluateCustody(observedStart, expiredAt).wouldFence, true);
  assert.equal((await reconcileOnce(observedStore, expiredAt)).status, 'observed');
  assert.equal((await observedStore.read())!.state.status, 'active');

  const armedStore = await seed();
  const started = await armForTest(armedStore);
  const fenced = await reconcileOnce(armedStore, expiredAt);
  assert.equal(fenced.status, 'fenced');
  const state = (await armedStore.read())!.state;
  assert.equal(state.status, 'fenced');
  assert.equal(state.fencingToken, started.fencingToken + 1);
  assert.equal(state.runInventory!.fencing_token, started.fencingToken);
  assert.equal(state.cleanupStatus, 'pending');
  assert.equal((await reconcileOnce(armedStore, expiredAt + HEARTBEAT_INTERVAL_MS)).status, 'terminal');
  assert.equal((await armedStore.read())!.state.fencingToken, started.fencingToken + 1);
});

test('max lifetime fences even when heartbeat is current', async () => {
  const store = await seed();
  const started = await armForTest(store);
  await heartbeatCampaign(store, ownerId, started.fencingToken, started.heartbeatAtMs + 10);
  const expired = (await store.read())!.state;
  const result = await reconcileOnce(store, expired.expiresAtMs);
  assert.equal(result.fenceReason, 'max-lifetime');
});

test('expired heartbeat blocks owner mutations before the watchdog fences', async () => {
  const store = await seed();
  const state = (await store.read())!.state;
  const expiredAt = state.heartbeatAtMs + MISSED_HEARTBEAT_LIMIT * HEARTBEAT_INTERVAL_MS;
  await assert.rejects(heartbeatCampaign(store, ownerId, state.fencingToken, expiredAt), /heartbeat is expired/);
  await assert.rejects(armCampaign(store, ownerId, state.fencingToken, expiredAt), /heartbeat is expired/);
});

test('campaign ownership cannot be replaced until cleanup is verified', async () => {
  const store = await seed();
  await assert.rejects(startCampaign(store, 'c0ffee00-0000-4000-8000-000000000004', ownerId, 1_000_100), /unfinished campaign/);
});

test('fenced cleanup retries the same idempotency key and completes ordered steps once', async () => {
  const store = await seed();
  await armForTest(store);
  const started = (await store.read())!.state;
  const expiredAt = started.heartbeatAtMs + MISSED_HEARTBEAT_LIMIT * HEARTBEAT_INTERVAL_MS;
  await reconcileOnce(store, expiredAt);
  await assert.rejects(heartbeatCampaign(store, ownerId, started.fencingToken, expiredAt + 1), /fenced, stale/);
  const seen: Array<{ step: string; key: string }> = [];
  let firstCall = true;
  const execute = async (step: typeof CUSTODY_CLEANUP_STEPS[number], _state: CampaignState, key: string) => {
    seen.push({ step, key });
    if (step === 'stop-admission' && firstCall) { firstCall = false; return 'pending' as const; }
    return 'complete' as const;
  };
  assert.equal(await reconcileCleanupOnce(store, execute), 'pending');
  assert.equal((await store.read())!.state.cleanupSteps[0]!.status, 'running');
  let progress = await reconcileCleanupOnce(store, execute);
  while (progress !== 'complete') progress = await reconcileCleanupOnce(store, execute);
  const completed = (await store.read())!.state;
  assert.equal(completed.status, 'complete');
  assert.deepEqual(completed.cleanupSteps.map(receipt => receipt.step), CUSTODY_CLEANUP_STEPS);
  assert.ok(completed.cleanupSteps.every(receipt => receipt.status === 'complete'));
  assert.equal(seen[0]!.key, seen[1]!.key);
  assert.equal(seen[0]!.key, completed.campaignId + ':' + completed.fencingToken + ':stop-admission');
  assert.deepEqual(seen.filter((item, index) => index === 0 || item.step !== seen[index - 1]!.step).map(item => item.step), CUSTODY_CLEANUP_STEPS);
});

test('ARM operation receipts reject out-of-scope URLs and reconcile terminal public status values', async () => {
  const store = await seed();
  const state = await armForTest(store);
  await assert.rejects(recordArmOperation(store, ownerId, state.fencingToken,
    { id: 'c0ffee00-0000-4000-8000-00000000000a', resourceId,
      operationUrl: 'https://example.com/operation', submittedAtMs: state.startedAtMs }, subscriptionId, state.startedAtMs + 2), /management.azure.com/);
  await recordArmOperation(store, ownerId, state.fencingToken,
    { id: 'c0ffee00-0000-4000-8000-00000000000b', resourceId, operationUrl, submittedAtMs: state.startedAtMs }, subscriptionId, state.startedAtMs + 2);
  const pending = (await store.read())!.state.operations[0]!;
  const completed = await reconcileArmOperation(pending, async url => {
    assert.equal(url, operationUrl);
    return { status: 200, body: { status: 'Succeeded' } };
  }, subscriptionId, state.startedAtMs + 3);
  assert.equal(completed.status, 'succeeded');
  const reconciled = await reconcilePendingArmOperations(store, async () => ({ status: 202 }), subscriptionId, state.startedAtMs + 3);
  assert.equal(reconciled!.operations[0]!.status, 'pending');
  const hostile = { ...pending, operationUrl: 'https://127.0.0.1/latest/meta-data' };
  await assert.rejects(reconcileArmOperation(hostile, async () => ({ status: 200 }), subscriptionId, state.startedAtMs + 3), /outside the configured subscription/);
});

test('RunInventory fenced merge preserves heartbeat and ARM receipts and blocks load until observed', async () => {
  const store = await seed();
  const started = (await store.read())!.state;
  const planned = inventoryFor(started);
  await publishRunInventory(store, planned, started.startedAtMs);
  await assert.rejects(assertRunInventoryReady(store, ownerId, started.fencingToken, started.startedAtMs), /blocked/);
  await armCampaign(store, ownerId, started.fencingToken, started.startedAtMs);
  await publishRunInventory(store, { ...planned, load_admission: 'authorization_pending' }, started.startedAtMs);
  await publishRunInventory(store, { ...planned, load_admission: 'authorized' }, started.startedAtMs);
  const observed = inventoryFor((await store.read())!.state, true);
  await publishRunInventory(store, observed, started.startedAtMs + 1);
  await assertRunInventoryReady(store, ownerId, started.fencingToken, started.startedAtMs + 2);
  await heartbeatCampaign(store, ownerId, started.fencingToken, started.startedAtMs + 3);
  await recordArmOperation(store, ownerId, started.fencingToken,
    { id: 'c0ffee00-0000-4000-8000-00000000000c', resourceId, operationUrl, submittedAtMs: started.startedAtMs },
    subscriptionId, started.startedAtMs + 4);
  const changed = { ...observed, cleanup: { ...observed.cleanup, stop_admission: { state: 'complete' as const, deadline_unix_ms: null, reason_code: null } } };
  await publishRunInventory(store, changed, started.startedAtMs + 5);
  const merged = (await store.read())!.state;
  assert.equal(merged.heartbeatSequence, 1);
  assert.equal(merged.operations.length, 1);
  assert.equal(merged.runInventory!.cleanup.stop_admission.state, 'complete');
  const nextCellId = 'c0ffee00-0000-4000-8000-00000000000d';
  const nextNamespace = 'arrowcol-' + nextCellId.replace(/-/g, '');
  const nextCell = { ...changed, cell_run_id: nextCellId, namespace: { ...changed.namespace, name: nextNamespace },
    resources: changed.resources.map(resource => ({ ...resource, namespace: nextNamespace })) };
  await assert.rejects(publishRunInventory(store, nextCell, started.startedAtMs + 6), /unresolved RunInventory/);
});

test('RunInventory rejects unknown identities and reports an unconfirmed Blob write', async () => {
  const store = await seed();
  const state = (await store.read())!.state;
  const planned = inventoryFor(state);
  await assert.rejects(publishRunInventory(store, { ...planned, extra: true }, state.startedAtMs), /malformed/);
  class FailsBeforeWrite extends MemoryStore {
    failNext = false;
    override async write(next: CampaignState, etag?: string): Promise<string> {
      if (this.failNext) { this.failNext = false; throw new Error('connection reset'); }
      return super.write(next, etag);
    }
  }
  const failing = new FailsBeforeWrite();
  await startCampaign(failing, campaignId, ownerId, state.startedAtMs);
  failing.failNext = true;
  await assert.rejects(publishRunInventory(failing, planned, state.startedAtMs), CampaignWriteOutcomeUnknownError);
});

test('successful inventory CAS receipt supports unknown FIC and QAP', async () => {
  const store = await seed();
  const state = (await store.read())!.state;
  const inventory = { ...inventoryFor(state), fic: null, qap: null };
  const written = await publishRunInventory(store, inventory, state.startedAtMs);
  const receipt = runInventoryPublishReceipt(written);
  assert.equal(receipt.status, 'persisted');
  assert.equal(receipt.fic_state, null);
  assert.equal(receipt.qap_state, null);
  assert.equal(receipt.resource_count, inventory.resources.length);
  await assert.rejects(assertRunInventoryReady(store, ownerId, state.fencingToken, state.startedAtMs), /blocked/);
});

test('RunInventory absent state requires immutable exact not-found receipts', async () => {
  const store = await seed();
  const state = (await store.read())!.state;
  const planned = inventoryFor(state);
  const first = planned.resources[0]!;
  await publishRunInventory(store, planned, state.startedAtMs);
  const absentResource = { ...first, state: 'absent' as const, absence: { api_version: first.api_version, kind: first.kind,
    namespace: first.namespace, name: first.name, namespace_uid: planned.namespace.uid, observed_at_unix_ms: state.startedAtMs + 1 } };
  const fic = planned.fic!;
  const qap = planned.qap!;
  const absent = { ...planned, resources: [absentResource, ...planned.resources.slice(1)],
    fic: { ...fic, state: 'absent' as const, absence: { resource_id: fic.resource_id!, name: fic.name!, observed_at_unix_ms: state.startedAtMs + 2 } },
    qap: { ...qap, state: 'absent' as const, absence: qap.bindings.map(binding => ({ database: qap.database,
      external_table: binding.external_table, relative_path: binding.relative_path, observed_at_unix_ms: state.startedAtMs + 3 })) } };
  await publishRunInventory(store, absent, state.startedAtMs + 4);
  const malformedResource = { ...absent, resources: [{ ...absentResource, absence: { ...absentResource.absence, name: 'other' } }, ...absent.resources.slice(1)] };
  await assert.rejects(publishRunInventory(await seed(), malformedResource, state.startedAtMs + 4), /malformed|FIC|QAP/);
  const missingQapReceipt = { ...absent, qap: { ...absent.qap!, absence: [] } };
  await assert.rejects(publishRunInventory(await seed(), missingQapReceipt, state.startedAtMs + 4), /not-found receipt/);
  const duplicateQapReceipt = { ...absent, qap: { ...absent.qap!, absence: [...absent.qap!.absence, absent.qap!.absence[0]!] } };
  await assert.rejects(publishRunInventory(await seed(), duplicateQapReceipt, state.startedAtMs + 4), /duplicate absence/);
});

test('watchdog cleanup CAS retains producer token and rejects stale producer writes', async () => {
  const store = await seed();
  const armed = await armForTest(store);
  const expiredAt = armed.heartbeatAtMs + MISSED_HEARTBEAT_LIMIT * HEARTBEAT_INTERVAL_MS;
  await reconcileOnce(store, expiredAt);
  const fenced = (await store.read())!.state;
  assert.equal(fenced.fencingToken, armed.fencingToken + 1);
  assert.equal(fenced.runInventory!.fencing_token, armed.fencingToken);
  await assert.rejects(publishRunInventory(store, fenced.runInventory, expiredAt + 1), /blocked after campaign fencing/);
  await reconcileCleanupOnce(store, async () => 'pending');
  const cleaning = (await store.read())!.state;
  const inventory = cleaning.runInventory!;
  const updated = { ...inventory, cleanup: { ...inventory.cleanup,
    stop_admission: { state: 'pending' as const, deadline_unix_ms: expiredAt + 60_000, reason_code: null } } };
  await publishCleanupRunInventory(store, updated, ownerId, cleaning.fencingToken);
  const merged = (await store.read())!.state;
  assert.equal(merged.runInventory!.fencing_token, armed.fencingToken);
  assert.equal(merged.fencingToken, cleaning.fencingToken);
  assert.equal(merged.runInventory!.cleanup.stop_admission.state, 'pending');
  await assert.rejects(publishCleanupRunInventory(store, updated, ownerId, armed.fencingToken), /fenced|cleaning token/);
});

test('scale trial journal rejects malformed target bindings and duplicate ids', () => {
  const state = newCampaignState(campaignId, ownerId, 1_000_000);
  const inventory = inventoryFor(state, true);
  const armed = { ...state, mode: 'armed' as const, runInventory: inventory };
  const target = { dimension: 'collector-cpu-limit' as const, before: 2, after: 3 };
  const step: ScaleStep = { ...target, restore: { dimension: target.dimension, before: 3, after: 2 }, evidence: ['collector-cpu-saturated'],
    baselineCommittedRowsPerSecond: 100, baselineQapPendingBacklog: 1 };
  const generated = newScaleTrialReceipt({ state: armed, step, resource: { transport: 'kubernetes',
    resource_id: 'apps/v1/Deployment/' + inventory.namespace.name + '/arrowcol/' + inventory.resources[0]!.uid, api_version: 'apps/v1',
    kind: 'Deployment', namespace: inventory.namespace.name, name: 'arrowcol', uid: inventory.resources[0]!.uid }, now: armed.startedAtMs });
  assert.equal(generated.restore_deadline_at_ms - generated.deadline_at_ms, SCALE_RESTORE_TIMEOUT_MS);
  assert.throws(() => newScaleTrialReceipt({ state: armed, step, resource: generated.resource,
    now: armed.expiresAtMs - SCALE_RESTORE_TIMEOUT_MS }), /bounded remaining lifetime/);
  const receipt = {
    schema_version: 1 as const, trial_id: 'c0ffee00-0000-4000-8000-000000000010', campaign_id: campaignId, owner_id: ownerId,
    fencing_token: 1, cell_run_id: inventory.cell_run_id, started_at_ms: armed.startedAtMs, deadline_at_ms: armed.startedAtMs + 60_000,
    restore_deadline_at_ms: armed.startedAtMs + 360_000,
    target, restore: { dimension: target.dimension, before: 3, after: 2 }, evidence: ['collector-cpu-saturated'],
    baseline: { committed_rows_per_second: 100, qap_pending_backlog: 1 },
    resource: { transport: 'kubernetes' as const, resource_id: 'apps/v1/Deployment/' + inventory.namespace.name + '/arrowcol/' + inventory.resources[0]!.uid,
      api_version: 'apps/v1', kind: 'Deployment', namespace: inventory.namespace.name, name: 'arrowcol', uid: inventory.resources[0]!.uid },
    phase: 'apply-intent' as const,
  };
  assert.equal(validateCampaignState({ ...armed, scaleTrials: [receipt] }).scaleTrials?.length, 1);
  const unrelatedUnknown = { ...receipt, baseline: { ...receipt.baseline, qap_pending_backlog: null }, phase: 'kept',
    result: { outcome: 'keep', committed_rows_per_second: 110, qap_pending_backlog: null, ineffective_increments: 0, reason: 'measured-collector-throughput' } };
  assert.equal(validateCampaignState({ ...armed, scaleTrials: [unrelatedUnknown] }).scaleTrials?.length, 1);
  const kusto = { ...receipt, target: { dimension: 'kusto-capacity', before: 2, after: 3 },
    restore: { dimension: 'kusto-capacity', before: 3, after: 2 },
    resource: { transport: 'azure-rest', resource_id: `/subscriptions/${campaignId}/resourceGroups/synthetic/providers/Microsoft.Kusto/clusters/synthetic`,
      api_version: '2024-04-13', kind: 'Microsoft.Kusto/clusters', namespace: null, name: 'synthetic', uid: null } };
  assert.equal(validateCampaignState({ ...armed, scaleTrials: [kusto] }).scaleTrials?.length, 1);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [{ ...kusto, baseline: unrelatedUnknown.baseline }] }), /malformed/);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [{ ...kusto, phase: 'kept', result: unrelatedUnknown.result }] }), /malformed/);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [{ ...receipt,
    baseline: { ...receipt.baseline, qap_p95_latency_ms: 10 } }] }), /malformed/);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [{ ...receipt, phase: 'kept',
    result: { outcome: 'keep', committed_rows_per_second: 110, qap_pending_backlog: 1,
      qap_p95_latency_ms: 10, ineffective_increments: 0, reason: 'legacy-row-latency-is-not-evidence' } }] }), /malformed/);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [receipt, receipt] }), /duplicated/);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [{ ...receipt, restore: { ...receipt.restore, after: 4 } }] }), /malformed/);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [{ ...receipt, restore_deadline_at_ms: receipt.deadline_at_ms }] }), /malformed/);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [{ ...receipt, restore_deadline_at_ms: receipt.restore_deadline_at_ms + 1 }] }), /malformed/);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [{ ...receipt, resource: { ...receipt.resource, uid: 'c0ffee00-0000-4000-8000-000000000099' } }] }), /malformed/);
});

test('official measurement reference is exact, optional for legacy inspection, and immutable after publication', async () => {
  const store = await seed();
  const state = await armForTest(store);
  const inventory = { ...state.runInventory!,
    evidence_destination: { account: 'arrowcollector12345', container: 'checkpoints', blob_prefix: 'synthetic-offline/cell-evidence' } };
  await publishRunInventory(store, inventory, state.startedAtMs + 2);
  const reference = { schema_version: 1 as const, blob: inventory.evidence_destination.blob_prefix + '/official-cell-measurement-v1.json',
    etag: '"original-etag"', version: null, sha256: 'a'.repeat(64), byte_length: 2048,
    effective_config_sha256: 'b'.repeat(64), submitted_config_sha256: null, source_revision: null,
    measurement_start_unix_ms: state.startedAtMs, measurement_end_unix_ms: state.startedAtMs + 30_000 };
  const bound = { ...inventory, official_measurement: reference };
  await publishRunInventory(store, bound, state.startedAtMs + 3);
  await assert.rejects(publishRunInventory(store, { ...bound, official_measurement: { ...reference, sha256: 'c'.repeat(64) } },
    state.startedAtMs + 4), /immutable/);
  await assert.rejects(publishRunInventory(store, { ...bound, official_measurement: null }, state.startedAtMs + 4), /immutable/);
  for (const invalid of [{ ...reference, blob: 'another-cell/official-cell-measurement-v1.json' },
    { ...reference, byte_length: 4_194_305 }, { ...reference, measurement_end_unix_ms: null },
    { ...reference, submitted_config_sha256: 'self-asserted-not-a-digest' }, { ...reference, etag: '*' }])
    assert.throws(() => validateCampaignState({ ...state, runInventory: { ...inventory, official_measurement: invalid } }), /measurement reference/);
});

test('strict state schema rejects additional persisted fields', async () => {
  const state = newCampaignState(campaignId, ownerId, 1);
  await assert.rejects(async () => validateCampaignState({ ...state, unexpected: true }), /unknown fields/);
  const store = new MemoryStore();
  await mutateCampaign(store, () => state);
  assert.equal((await store.read())!.state.campaignId, campaignId);
});


test('campaign Blob store keeps the default record path and supports isolated paths', async () => {
  const paths: string[] = [];
  const fetcher: typeof fetch = async input => {
    paths.push(new URL(String(input)).pathname);
    return new Response(null, { status: 404 });
  };
  const config = { account: 'arrowcollector12345', container: 'checkpoints', subscriptionId };
  await createBlobCampaignStore(config, fetcher, async () => 'fixture-token').read();
  await createBlobCampaignStore({ ...config, recordPath: '_arrowcol-custody-rehearsal/rehearsal-1/active.json' },
    fetcher, async () => 'fixture-token').read();
  assert.deepEqual(paths, ['/checkpoints/active.json',
    '/checkpoints/_arrowcol-custody-rehearsal/rehearsal-1/active.json']);
  assert.throws(() => createBlobCampaignStore({ ...config, recordPath: '../active.json' }, fetcher,
    async () => 'fixture-token'), /Campaign store record path is invalid/);
});

test('campaign store reads chunked response bytes exactly at the cap without Content-Length', async () => {
  const state = newCampaignState(campaignId, ownerId, Date.now());
  const bytes = Buffer.from(JSON.stringify(state), 'utf8');
  const split = Math.floor(bytes.length / 2);
  const body = chunkedResponse([bytes.subarray(0, split), bytes.subarray(split)]);
  assert.equal(body.response.headers.get('content-length'), null);
  const store = createBlobCampaignStore({ account: 'arrowcollector12345', container: 'checkpoints', subscriptionId },
    async () => body.response, async () => 'fixture-token');
  assert.deepEqual(await store.read({ maxResponseBodyBytes: bytes.length }), { state, etag: '"state-v1"' });
  assert.equal(body.wasCancelled(), false);
});

test('campaign store rejects chunked overflow despite a smaller Content-Length', async () => {
  const state = newCampaignState(campaignId, ownerId, Date.now());
  const bytes = Buffer.from(JSON.stringify(state), 'utf8');
  const overflow = Buffer.concat([bytes, Buffer.from(' ')]);
  const body = chunkedResponse([overflow], { 'content-length': String(bytes.length) }, false);
  const store = createBlobCampaignStore({ account: 'arrowcollector12345', container: 'checkpoints', subscriptionId },
    async () => body.response, async () => 'fixture-token');
  await assert.rejects(store.read({ maxResponseBodyBytes: bytes.length }), /byte cap/);
  assert.equal(body.wasCancelled(), true);
});

test('capped campaign reads cancel streaming bodies before 404, status, and missing ETag exits', async () => {
  const config = { account: 'arrowcollector12345', container: 'checkpoints', subscriptionId };
  const cases = [
    { status: 404, etag: '"state-v1"', error: undefined },
    { status: 503, etag: '"state-v1"', error: /HTTP 503/ },
    { status: 200, etag: null, error: /no ETag/ },
  ] as const;
  for (const item of cases) {
    const body = chunkedResponse([Buffer.from('pending')], {}, false, item.status, item.etag);
    const store = createBlobCampaignStore(config, async () => body.response, async () => 'fixture-token');
    if (item.error) await assert.rejects(store.read({ maxResponseBodyBytes: 32 }), item.error);
    else assert.equal(await store.read({ maxResponseBodyBytes: 32 }), undefined);
    assert.equal(body.wasCancelled(), true);
  }
});

test('capped campaign reads decode split UTF-8 and reject invalid UTF-8 and JSON', async () => {
  const state = newCampaignState(campaignId, ownerId, Date.now());
  const text = JSON.stringify({ ...state, unexpected: '💠' });
  const bytes = Buffer.from(text, 'utf8'), symbol = Buffer.from('💠', 'utf8');
  const symbolStart = bytes.indexOf(symbol), split = symbolStart + 2;
  const splitBody = chunkedResponse([bytes.subarray(0, split), bytes.subarray(split)]);
  const config = { account: 'arrowcollector12345', container: 'checkpoints', subscriptionId };
  const splitStore = createBlobCampaignStore(config, async () => splitBody.response, async () => 'fixture-token');
  await assert.rejects(splitStore.read({ maxResponseBodyBytes: bytes.length }), /unknown fields/);

  const invalidUtf8 = Buffer.concat([Buffer.from(JSON.stringify(state), 'utf8'), Buffer.from([0xff])]);
  const utf8Store = createBlobCampaignStore(config, async () => chunkedResponse([invalidUtf8]).response,
    async () => 'fixture-token');
  await assert.rejects(utf8Store.read({ maxResponseBodyBytes: invalidUtf8.length }), /not valid UTF-8/);

  const invalidJson = Buffer.from('{', 'utf8');
  const jsonStore = createBlobCampaignStore(config, async () => chunkedResponse([invalidJson]).response,
    async () => 'fixture-token');
  await assert.rejects(jsonStore.read({ maxResponseBodyBytes: invalidJson.length }), /not valid JSON/);
});

test('capped campaign store rejects invalid caps before token acquisition or fetch', async () => {
  let tokens = 0, fetches = 0;
  const store = createBlobCampaignStore({ account: 'arrowcollector12345', container: 'checkpoints', subscriptionId },
    async () => { fetches++; return new Response(null, { status: 404 }); },
    async () => { tokens++; return 'fixture-token'; });
  await assert.rejects(store.read({ maxResponseBodyBytes: 0 }), /positive safe integer/);
  assert.equal(tokens, 0);
  assert.equal(fetches, 0);
});

test('capped campaign read rechecks its original deadline after effect journaling', async () => {
  let fetches = 0;
  const envelope: EffectEnvelope = {
    schema_version: 1, campaign_id: campaignId, run_id: 'c0ffee00-0000-4000-8000-000000000004',
    subscription_id: subscriptionId, resource_group: 'perf69', storage_account: 'arrowcollector12345',
    state_container: 'checkpoints', effects: [{ id: 'campaign-read', transport: 'campaign-blob', method: 'GET',
      resource: 'https://arrowcollector12345.blob.core.windows.net/checkpoints', path: '/checkpoints/active.json', max_calls: 1 }],
  };
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-campaign-read-deadline-'));
  chmodSync(directory, 0o700);
  try {
    const journalPath = path.join(directory, 'effects.jsonl');
    const boundary = new EffectBoundary(envelope, journalPath);
    const store = createBlobCampaignStore({ account: 'arrowcollector12345', container: 'checkpoints', subscriptionId },
      async () => { fetches++; return new Response(null, { status: 404 }); }, async () => 'fixture-token');
    const originalFsync = fs.fsyncSync;
    fs.fsyncSync = (fd: number) => {
      const started = Date.now();
      while (Date.now() - started < 350) { /* deliberate synchronous journal delay */ }
      return originalFsync(fd);
    };
    const deadlineUnixMs = Date.now() + 200;
    try {
      await assert.rejects(withEffectBoundary(boundary, () => store.read({
        maxResponseBodyBytes: 4_194_304, deadlineUnixMs,
      })), /original deadline/);
    } finally { fs.fsyncSync = originalFsync; }
    assert.equal(fetches, 0);
    assert.match(fs.readFileSync(journalPath, 'utf8'), /"event":"dispatch"/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('capped campaign read cancels an unclosed body at its original deadline', async () => {
  const state = newCampaignState(campaignId, ownerId, Date.now());
  const body = chunkedResponse([Buffer.from(JSON.stringify(state), 'utf8')], {}, false);
  const store = createBlobCampaignStore({ account: 'arrowcollector12345', container: 'checkpoints', subscriptionId },
    async () => body.response, async () => 'fixture-token');
  await assert.rejects(store.read({ maxResponseBodyBytes: 4_194_304, deadlineUnixMs: Date.now() + 100 }),
    /original deadline/);
  assert.equal(body.wasCancelled(), true);
});

import { acquireFleetAuthority, FleetUnsettledDispatchError, type FleetBinding, type FleetPoolObservation } from './aks-fleet-custody.js';
import type { Azure } from '../../infra/azure/bootstrap.js';

class FleetBlobFake {
  c = { checkpoints: 'checkpoints' };
  blobs = new Map<string, { bytes: Buffer; etag: string }>();
  revision = 0;
  held?: string;
  journalLease?: string;
  beforeJournalUpdate?: () => void;
  beforeCampaignClaim?: () => void;
  failNextJournalWrite = false;
  async lease(action: string, id: string) {
    if (action === 'acquire') { if (this.held) throw new Error('lease held'); this.held = id; return; }
    if (this.held !== id) throw new Error('stale lease');
    if (action === 'release') this.held = undefined;
  }
  async blob(method: string, _container: string, name: string, query?: Record<string, string>, body?: string | Buffer | null,
    headers: Record<string, string> = {}) {
    if (method === 'PUT' && name === 'active.json') this.beforeCampaignClaim?.();
    const old = this.blobs.get(name), journal = name === 'oat-fleet/active.v1.json';
    if (query?.comp === 'lease') {
      if (method !== 'PUT' || !journal || !old) throw new Error('lease requires existing journal blob');
      const action = headers['x-ms-lease-action'];
      if (action === 'acquire') {
        if (this.journalLease || !headers['x-ms-proposed-lease-id'] || headers['if-match'] !== old.etag) throw new Error('lease held or ETag changed');
        this.journalLease = headers['x-ms-proposed-lease-id'];
      } else {
        if (!this.journalLease || this.journalLease !== headers['x-ms-lease-id']) throw new Error('stale lease');
        if (action === 'release') this.journalLease = undefined;
        else if (action !== 'renew') throw new Error('unexpected lease operation');
      }
      return [null, {}] as const;
    }
    if (method === 'GET') return [old?.bytes ?? null, old ? { etag: old.etag } : {}] as const;
    if (headers['if-match'] && headers['if-match'] !== old?.etag || headers['if-none-match'] === '*' && old) throw new Error('precondition failed');
    if (method === 'DELETE') { this.blobs.delete(name); return [null, {}] as const; }
    if (method !== 'PUT') throw new Error('unexpected operation');
    if (journal && old) {
      this.beforeJournalUpdate?.();
      if (!this.journalLease || headers['x-ms-lease-id'] !== this.journalLease) throw new Error('stale journal write lease');
    }
    const etag = `"${++this.revision}"`;
    this.blobs.set(name, { bytes: Buffer.from(body!), etag });
    if (name === 'oat-fleet/active.v1.json' && this.failNextJournalWrite) {
      this.failNextJournalWrite = false;
      throw new Error('write outcome unknown');
    }
    return [null, { etag }] as const;
  }
  azure() { return this as unknown as Azure; }
}
const fleetBinding: FleetBinding = { clusterId: resourceId, server: 'https://bench.example', pools: [
  { name: 'arrow', sku: 'Standard_D4s_v5', vcpu: 4, role: 'arrowcol', count: 1, maxCount: 3 },
  { name: 'client', sku: 'Standard_D2s_v5', vcpu: 2, role: 'client', count: 1, maxCount: 2 },
] };
const fleetObservation = (name: string, count = 0, etag = '"arm-0"'): FleetPoolObservation => ({ name,
  resourceId: resourceId + '/agentPools/' + name, sku: fleetBinding.pools.find(p => p.name === name)!.sku,
  count, provisioningState: 'Succeeded', etag, configurationHash: 'baseline-' + name });
const fleetBaseline = () => fleetBinding.pools.map(p => fleetObservation(p.name));
const fleetOwner = { ownerId, resume: false };

test('fleet custody excludes competing legacy campaigns and rejects nonzero baselines', async () => {
  const fake = new FleetBlobFake();
  const legacy = newCampaignState(campaignId, ownerId);
  await fake.blob('PUT', 'checkpoints', 'active.json', undefined, JSON.stringify(legacy));
  await assert.rejects(acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner), /Another campaign/);
  assert.equal(fake.blobs.has('oat-fleet/active.v1.json'), false);
  await fake.blob('DELETE', 'checkpoints', 'active.json');
  const authority = await acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner);
  assert.equal(validateCampaignState(JSON.parse(fake.blobs.get('active.json')!.bytes.toString())).mode, 'observe');
  await assert.rejects(authority.establishBaseline([fleetObservation('arrow', 1), fleetObservation('client')]), /baseline/i);
  await authority.establishBaseline(fleetBaseline());
  await assert.rejects(startCampaign({
    read: async () => ({ state: validateCampaignState(JSON.parse(fake.blobs.get('active.json')!.bytes.toString())), etag: fake.blobs.get('active.json')!.etag }),
    write: async () => { throw new Error('unexpected legacy write'); },
  }, campaignId, ownerId), /unfinished campaign/);
  await authority.complete();
  await authority.close();
  assert.equal(fake.blobs.has('active.json'), false);
});

test('fleet custody records intent before dispatch and recovers only settled owned pools', async () => {
  const fake = new FleetBlobFake();
  const first = await acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner);
  await first.establishBaseline(fleetBaseline());
  const [id, clientId] = await Promise.all([first.beforeDispatch('arrow', 1, '"arm-0"'), first.beforeDispatch('client', 1, '"arm-0"')]);
  const journal = JSON.parse(fake.blobs.get('oat-fleet/active.v1.json')!.bytes.toString());
  assert.equal(journal.pools.arrow.dispatch.id, id);
  assert.equal(journal.pools.client.dispatch.id, clientId);
  await first.afterDispatch('arrow', id, 'uncertain');
  await first.afterDispatch('client', clientId, 'conflict');
  await assert.rejects(first.recordObservation(fleetObservation('arrow')), /uncertain/);
  await first.recordObservation(fleetObservation('client'));
  await first.close();
  const mismatch = structuredClone(fleetBinding); mismatch.pools[0]!.maxCount++;
  await assert.rejects(acquireFleetAuthority(fake.azure(), mismatch, { ownerId, resume: true }), /binding/);
  const recovered = await acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId, resume: true });
  await assert.rejects(recovered.beforeDispatch('client', 1, '"arm-0"'), /not authorized/);
  await assert.rejects(recovered.beforeDispatch('arrow', 0, '"arm-0"'), /not authorized/);
  await recovered.recordObservation(fleetObservation('arrow', 1, '"arm-1"'));
  const down = await recovered.beforeDispatch('arrow', 0, '"arm-1"');
  await recovered.afterDispatch('arrow', down, 'accepted');
  await assert.rejects(recovered.complete(), /settled/);
  await recovered.recordObservation(fleetObservation('arrow', 0, '"arm-2"'));
  await recovered.complete();
  await recovered.close();
});

test('fleet custody requires the retained journal and campaign owner before any recovery mutation', async () => {
  const fake = new FleetBlobFake();
  const first = await acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner);
  await first.establishBaseline(fleetBaseline());
  await first.close();
  const journal = fake.blobs.get('oat-fleet/active.v1.json'), campaign = fake.blobs.get('active.json')!;
  let revision = fake.revision;
  await assert.rejects(acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId: campaignId, resume: true }), /retained owner/);
  assert.equal(fake.revision, revision);
  assert.equal(fake.blobs.get('oat-fleet/active.v1.json'), journal);
  assert.equal(fake.blobs.get('active.json'), campaign);
  assert.equal(fake.journalLease, undefined);
  await fake.blob('PUT', 'checkpoints', 'active.json', undefined,
    JSON.stringify({ ...JSON.parse(campaign.bytes.toString()), ownerId: campaignId }));
  const replaced = fake.blobs.get('active.json');
  revision = fake.revision;
  await assert.rejects(acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId, resume: true }), /Another campaign/);
  assert.equal(fake.revision, revision);
  assert.equal(fake.blobs.get('oat-fleet/active.v1.json'), journal);
  assert.equal(fake.blobs.get('active.json'), replaced);
  assert.equal(fake.journalLease, undefined);
  await fake.blob('PUT', 'checkpoints', 'active.json', undefined, campaign.bytes);
  const recovered = await acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId, resume: true });
  assert.ok(recovered.fencingToken > first.fencingToken);
  await recovered.complete(); await recovered.close();
  const fresh = await acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId: campaignId, resume: false });
  assert.equal(fresh.ownerId, campaignId);
  await fresh.establishBaseline(fleetBaseline());
  await fresh.complete(); await fresh.close();
});

test('fleet custody fences stale leases and uncertain journal writes', async () => {
  const fake = new FleetBlobFake();
  const authority = await acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner);
  await authority.establishBaseline(fleetBaseline());
  fake.failNextJournalWrite = true;
  await assert.rejects(authority.beforeDispatch('arrow', 1, '"arm-0"'), /unknown/);
  await assert.rejects(authority.beforeDispatch('client', 1, '"arm-0"'), /uncertain/);
  await authority.close();
  const recovered = await acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId, resume: true });
  await assert.rejects(recovered.recordObservation(fleetObservation('arrow')), /uncertain/);
  fake.journalLease = 'other-lease';
  await assert.rejects(recovered.assertCurrent(), /stale lease/);
  await assert.rejects(recovered.beforeDispatch('client', 0, '"arm-0"'), /fenced/);
  await assert.rejects(recovered.close(), /stale lease/);
});

test('fleet custody refuses configuration drift, unknown pools and corrupt journals', async () => {
  const fake = new FleetBlobFake();
  const first = await acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner);
  await first.establishBaseline(fleetBaseline());
  await assert.rejects(first.recordObservation({ ...fleetObservation('arrow'), configurationHash: 'changed' }), /configuration/);
  await assert.rejects(first.recordObservation({ ...fleetObservation('arrow'), name: 'unowned' }), /approved/);
  await assert.rejects(first.beforeDispatch('arrow', 1, '"old"'), /not authorized/);
  await first.close();
  const blob = fake.blobs.get('oat-fleet/active.v1.json')!;
  const value = JSON.parse(blob.bytes.toString()); value.unexpected = true;
  blob.bytes = Buffer.from(JSON.stringify(value));
  await assert.rejects(acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId, resume: true }), /Invalid fleet journal/);
});

test('fleet custody fences a replaced legacy campaign and preserves its journal', async () => {
  const fake = new FleetBlobFake();
  const authority = await acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner);
  await authority.establishBaseline(fleetBaseline());
  await fake.blob('PUT', 'checkpoints', 'active.json', undefined, JSON.stringify(newCampaignState(campaignId, ownerId)));
  await assert.rejects(authority.assertCurrent(), /fenced/);
  await assert.rejects(authority.beforeDispatch('arrow', 1, '"arm-0"'), /fenced/);
  await authority.close();
  await assert.rejects(acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId, resume: true }), /Another campaign/);
});

test('fleet custody accepts 70 and 128 unique client pools and enforces collector and client bounds', async () => {
  const bindingFor = (clients: number): FleetBinding => ({ ...fleetBinding, pools: [fleetBinding.pools[0]!,
    ...Array.from({ length: clients }, (_, i) => ({ name: `client${i}`, sku: 'Standard_D2s_v5', vcpu: 2,
      role: 'client' as const, count: 1, maxCount: 2 }))] });
  for (const count of [70, 128]) {
    const binding = bindingFor(count), fake = new FleetBlobFake();
    const authority = await acquireFleetAuthority(fake.azure(), binding, fleetOwner);
    await authority.establishBaseline(binding.pools.map(pool => ({ name: pool.name, resourceId: resourceId + '/agentPools/' + pool.name,
      sku: pool.sku, count: 0, provisioningState: 'Succeeded', etag: '"arm-0"', configurationHash: 'hash-' + pool.name })));
    await authority.complete(); await authority.close();
  }
  const malformed = [bindingFor(129), bindingFor(0), { ...fleetBinding, pools: [fleetBinding.pools[1]!, fleetBinding.pools[1]!] },
    { ...fleetBinding, pools: [{ ...fleetBinding.pools[0]!, count: 2 }, fleetBinding.pools[1]!] },
    { ...fleetBinding, pools: [fleetBinding.pools[0]!, { ...fleetBinding.pools[1]!, count: 129, maxCount: 129 }] },
    { ...fleetBinding, pools: [fleetBinding.pools[0]!, { ...fleetBinding.pools[0]!, name: 'arrow2' }, fleetBinding.pools[1]!] }];
  for (const binding of malformed) await assert.rejects(acquireFleetAuthority(new FleetBlobFake().azure(), binding, fleetOwner), /fleet|pool/i);
});

test('fleet custody refuses a valid terminal unsettled legacy cleanup and unknown state', async () => {
  const fake = new FleetBlobFake(), now = Date.now(), started = now - 300_000;
  const base = newCampaignState(campaignId, ownerId, started), inventory = inventoryFor(base, true);
  inventory.load_admission = 'authorized';
  inventory.resources = inventory.resources.map(resource => ({ ...resource, state: 'removed' }));
  inventory.fic = null; inventory.qap = null;
  const reason = 'drain_deadline_expired_unsettled' as const;
  const complete = { state: 'complete' as const, deadline_unix_ms: null, reason_code: null };
  inventory.cleanup = { stop_admission: complete, bounded_drain: { state: 'uncertain', deadline_unix_ms: now - 1000, reason_code: reason },
    evidence_capture: complete, stop_maintenance: complete, terminal_capture: complete,
    handoff: { state: 'failed', deadline_unix_ms: null, reason_code: reason } };
  const proof = { schemaVersion: 1 as const, campaignId, producerOwnerId: ownerId, producerFencingToken: 1,
    actorOwnerId: ownerId, actorFencingToken: 2, orchestrationRunId: inventory.orchestration_run_id,
    cellRunId: inventory.cell_run_id, namespaceName: inventory.namespace.name, namespaceUid: inventory.namespace.uid!,
    deadlineUnixMs: now - 1000, observedAtUnixMs: now, reason, deltaRows: null, kustoRows: null };
  const legacy: CampaignState = { ...base, mode: 'armed', status: 'complete_unsettled', fencingToken: 2,
    fenceReason: 'heartbeat-expired', fencedAtMs: now - 2000, cleanupStatus: 'verified_with_unsettled', runInventory: inventory,
    cleanupSteps: CUSTODY_CLEANUP_STEPS.map(step => ({ step, idempotencyKey: `${campaignId}:2:${step}`,
      ...(step === 'bounded-drain' ? { status: 'unsettled' as const, unsettled: proof } : { status: 'complete' as const }) })) };
  validateCampaignState(legacy);
  await fake.blob('PUT', 'checkpoints', 'active.json', undefined, JSON.stringify(legacy));
  await assert.rejects(acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner), /Another campaign/);
  assert.equal(fake.blobs.has('oat-fleet/active.v1.json'), false);
  await fake.blob('PUT', 'checkpoints', 'active.json', undefined, JSON.stringify({ ...legacy, status: 'unknown' }));
  await assert.rejects(acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner), /malformed|unknown/i);
});

test('fleet custody keeps uncertain dispatch intent across Updating, Failed and old-count readback', async () => {
  const fake = new FleetBlobFake(), authority = await acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner);
  await authority.establishBaseline(fleetBaseline());
  const id = await authority.beforeDispatch('arrow', 1, '"arm-0"');
  await authority.afterDispatch('arrow', id, 'accepted');
  for (const provisioningState of ['Updating', 'Failed', 'Succeeded']) {
    await assert.rejects(authority.recordObservation({ ...fleetObservation('arrow'), provisioningState }), FleetUnsettledDispatchError);
    await assert.rejects(authority.beforeDispatch('arrow', 1, '"arm-0"'), /not authorized/);
  }
  await assert.rejects(authority.recordObservation({ ...fleetObservation('arrow'), provisioningState: 'Updating', configurationHash: 'changed' }), /configuration/);
  await authority.recordObservation(fleetObservation('arrow', 1, '"arm-1"'));
  const down = await authority.beforeDispatch('arrow', 0, '"arm-1"');
  await authority.afterDispatch('arrow', down, 'accepted');
  await authority.recordObservation(fleetObservation('arrow', 0, '"arm-2"'));
  await authority.complete(); await authority.close();
});

test('fleet custody repairs terminal failed scale-up at old or target count to settled zero', async () => {
  for (const [status, count] of [['Failed', 0], ['Canceled', 1]] as const) {
    const fake = new FleetBlobFake();
    const initial = await acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner);
    await initial.establishBaseline(fleetBaseline());
    const up = await initial.beforeDispatch('arrow', 1, '"arm-0"');
    await initial.afterDispatch('arrow', up, 'uncertain');
    await assert.rejects(initial.recordObservation({ ...fleetObservation('arrow', 0, '"arm-0"'), provisioningState: status }), FleetUnsettledDispatchError);
    await assert.rejects(initial.recordObservation(fleetObservation('arrow', 0, '"arm-1"')), FleetUnsettledDispatchError);
    await assert.rejects(initial.recordObservation({ ...fleetObservation('arrow', 2, '"arm-1"'), provisioningState: status }), /count/);
    await initial.recordObservation({ ...fleetObservation('arrow', count, '"arm-1"'), provisioningState: status });
    await assert.rejects(initial.complete(), /settled/);
    await assert.rejects(initial.beforeDispatch('arrow', 1, '"arm-1"'), /not authorized/);
    await initial.close();
    const resumed = await acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId, resume: true });
    const down = await resumed.beforeDispatch('arrow', 0, '"arm-1"');
    await resumed.afterDispatch('arrow', down, 'accepted');
    if (count === 0) await assert.rejects(resumed.recordObservation(fleetObservation('arrow', 0, '"arm-1"')), FleetUnsettledDispatchError);
    await resumed.recordObservation(fleetObservation('arrow', 0, '"arm-2"'));
    await resumed.complete(); await resumed.close();
  }
});

test('fleet custody leaves publisher container lease independent and fences stale journal writes', async () => {
  const fake = new FleetBlobFake(), authority = await acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner);
  assert.ok(fake.journalLease);
  await fake.lease('acquire', 'publisher');
  await authority.establishBaseline(fleetBaseline());
  await assert.rejects(acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId, resume: true }), /lease held/);
  await authority.assertCurrent();
  await fake.lease('release', 'publisher');
  const prior = fake.blobs.get('oat-fleet/active.v1.json');
  fake.beforeJournalUpdate = () => { fake.journalLease = 'replacement'; };
  await assert.rejects(authority.beforeDispatch('arrow', 1, '"arm-0"'), /stale journal write lease/);
  assert.equal(fake.blobs.get('oat-fleet/active.v1.json'), prior);
  await assert.rejects(authority.assertCurrent(), /fenced/);
  await assert.rejects(authority.close(), /stale lease/);
});

test('fleet custody does not adopt a journal after unconfirmed initial creation', async () => {
  const fake = new FleetBlobFake();
  fake.failNextJournalWrite = true;
  await assert.rejects(acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner), /write outcome unknown/);
  assert.equal(fake.journalLease, undefined);
  assert.equal(fake.blobs.has('active.json'), false);
  await assert.rejects(acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner), /exact recovery/);
  const recovered = await acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId, resume: true });
  await assert.rejects(recovered.beforeDispatch('arrow', 0, '"arm-0"'), /not authorized/);
  await recovered.close();
});

test('fleet custody journal lease does not override a legacy contender that wins campaign CAS', async () => {
  const fake = new FleetBlobFake();
  const winner = newCampaignState(campaignId, ownerId);
  fake.beforeCampaignClaim = () => {
    fake.beforeCampaignClaim = undefined;
    fake.blobs.set('active.json', { bytes: Buffer.from(JSON.stringify(winner)), etag: '"legacy-winner"' });
  };
  await assert.rejects(acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner), /precondition failed/);
  assert.equal(fake.journalLease, undefined);
  assert.deepEqual(JSON.parse(fake.blobs.get('active.json')!.bytes.toString()), winner);
  await assert.rejects(acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId, resume: true }), /Another campaign/);
});

test('fleet custody resumes an empty journal only by proving its exact zero baseline', async () => {
  const fake = new FleetBlobFake();
  const initial = await acquireFleetAuthority(fake.azure(), fleetBinding, fleetOwner);
  await initial.close();
  const resumed = await acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId, resume: true });
  assert.equal(resumed.baselineEstablished, false);
  await assert.rejects(resumed.establishBaseline([fleetObservation('arrow', 1), fleetObservation('client')]), /must be unique, settled and zero/);
  assert.equal(resumed.baselineEstablished, false);
  await resumed.establishBaseline(fleetBaseline());
  await resumed.close();
  const established = await acquireFleetAuthority(fake.azure(), fleetBinding, { ownerId, resume: true });
  assert.equal(established.baselineEstablished, true);
  await assert.rejects(established.establishBaseline(fleetBaseline()), /Baseline already established/);
  await established.complete();
  await established.close();
});
