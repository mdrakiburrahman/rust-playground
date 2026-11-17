import assert from 'node:assert/strict';
import { test, type TestContext } from 'node:test';
import { Azure, Config } from '../../infra/azure/bootstrap.js';
import { CampaignConflictError, CUSTODY_CLEANUP_STEPS, armCampaign, publishRunInventory, startCampaign, validateCampaignState,
  type CampaignState, type CampaignStore, type RunInventory, type StoredCampaign, type ScaleTrialReceipt } from './aks-campaign.js';
import { createDefaultScaleAdapter, restoreCampaignScaleTrials } from './aks-scale-provider.js';
import { createDefaultCleanupProviders } from './aks-default-providers.js';
import { createExternalCleanupExecutor, type ExternalCleanupActions } from './aks-cleanup-executor.js';
import { newScaleTrialReceipt, persistScaleTrial, type ScaleStep, type ScaleTarget } from './aks-scale.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';
import type { CustodyCommandRunner } from './aks-setup-cleanup.js';

const campaignId = 'c0ffee00-0000-4000-8000-000000000001';
const ownerId = 'c0ffee00-0000-4000-8000-000000000002';
const cellId = 'c0ffee00-0000-4000-8000-000000000003';
const token = { ownerId, fencingToken: 1 };

class MemoryStore implements CampaignStore {
  current?: StoredCampaign;
  revision = 0;
  async read() { return this.current ? structuredClone(this.current) : undefined; }
  async write(state: CampaignState, etag?: string) {
    if (etag !== this.current?.etag) throw new CampaignConflictError();
    const next = `etag-${++this.revision}`;
    this.current = { state: structuredClone(validateCampaignState(state)), etag: next };
    return next;
  }
}

async function fixture() {
  const now = Date.now();
  const store = new MemoryStore();
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: campaignId }));
  await startCampaign(store, campaignId, ownerId, now);
  const state = (await store.read())!.state;
  const namespace = 'arrowcol-' + cellId.replaceAll('-', '');
  const identityId = `${azure.c.base}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/${azure.c.identity}`;
  const ficName = 'arrowcol-run-' + cellId;
  const aksId = azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster;
  const plan = { version: 1 as const, runId: cellId, namespace, serviceAccount: 'oat', identityId,
    resourceId: `${identityId}/federatedIdentityCredentials/${ficName}`, clientId: ownerId, tenantId: campaignId, aksId,
    server: 'https://fixture.azmk8s.io', credentialName: ficName, issuer: 'https://fixture.oic.prod-aks.azure.com/tenant/cluster/',
    subject: `system:serviceaccount:${namespace}:oat`, audience: 'api://AzureADTokenExchange' };
  const inventory: RunInventory = {
    schema_version: 1, campaign_id: campaignId, owner_id: ownerId, fencing_token: 1,
    orchestration_run_id: 'c0ffee00-0000-4000-8000-000000000004', cell_run_id: cellId,
    load_admission: 'not_authorized', planned_cell_duration_ms: 60_000,
    effective_stop_budgets: { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 1_200_000 },
    namespace: { name: namespace, uid: null, owner_token: ownerId },
    resources: [
      { api_version: 'apps/v1', kind: 'Deployment', namespace, name: 'arrowcol', uid: null, absence: null, role: 'collector', state: 'planned' },
      { api_version: 'apps/v1', kind: 'Deployment', namespace, name: 'arrowcol-client', uid: null, absence: null, role: 'client', state: 'planned' },
      { api_version: 'apps/v1', kind: 'Deployment', namespace, name: 'delta-maintenance', uid: null, absence: null, role: 'maintenance', state: 'planned' },
    ],
    fic: { resource_id: `${identityId}/federatedIdentityCredentials/${ficName}`, name: ficName, state: 'planned', absence: null, plan },
    qap: { database: 'perf69-qap', bindings: [{ external_table: 'arrowcol_data', relative_path: 'perf69/' + cellId }], state: 'planned', absence: [] },
    cleanup: Object.fromEntries(['stop_admission', 'bounded_drain', 'evidence_capture', 'stop_maintenance', 'terminal_capture', 'handoff']
      .map(key => [key, { state: 'not_started', deadline_unix_ms: null, reason_code: null }])) as RunInventory['cleanup'],
  };
  await publishRunInventory(store, inventory, now);
  await armCampaign(store, ownerId, token.fencingToken, now + 1);
  const planned = (await store.read())!.state.runInventory!;
  await publishRunInventory(store, { ...planned, load_admission: 'authorization_pending' }, now + 2);
  await publishRunInventory(store, { ...planned, load_admission: 'authorized' }, now + 3);
  const authorized = (await store.read())!.state.runInventory!;
  const observed: RunInventory = { ...authorized, namespace: { ...authorized.namespace, uid: 'c0ffee00-0000-4000-8000-000000000005' },
    resources: authorized.resources.map((resource, index) => ({ ...resource,
      uid: `c0ffee00-0000-4000-8000-00000000000${6 + index}`, state: 'observed' })),
    fic: { ...authorized.fic!, state: 'observed' }, qap: { ...authorized.qap!, state: 'observed' } };
  await publishRunInventory(store, observed, now + 4);
  return { store, azure, namespace };
}

function scaleRunner(clientDeployment?: unknown, collectorReplicas = 1, collectorStrategy = 'Recreate', physicalWriters = collectorReplicas): CustodyCommandRunner {
  const collector = { apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: 'arrowcol', namespace: 'arrowcol-' + cellId.replaceAll('-', ''),
    uid: `c0ffee00-0000-4000-8000-000000000006`, generation: 1, resourceVersion: 'rv-collector' },
    spec: { replicas: collectorReplicas, strategy: { type: collectorStrategy }, selector: { matchLabels: { app: 'arrowcol' } },
      template: { spec: { containers: [{ name: 'dataflow', resources: { requests: { cpu: '1', memory: '4Gi' }, limits: { cpu: '2', memory: '8Gi' } } }] } } },
    status: { observedGeneration: 1, availableReplicas: collectorReplicas, readyReplicas: collectorReplicas } };
  const replicaSets = Array.from({ length: physicalWriters }, (_, index) => ({
    metadata: { uid: `c0ffee00-0000-4000-8000-0000000000${11 + index}`, ownerReferences: [
      { kind: 'Deployment', uid: collector.metadata.uid, controller: true }] }, spec: { replicas: 1 } }));
  const pods = replicaSets.map((replicaSet, index) => ({ metadata: { name: `arrowcol-${index}`,
    ownerReferences: [{ kind: 'ReplicaSet', uid: replicaSet.metadata.uid, controller: true }] },
    status: { phase: 'Running', containerStatuses: [{ ready: true }] } }));
  return async args => {
    if (args.includes('patch')) return { returncode: 0, stdout: '', stderr: '' };
    if (args.includes('replicasets')) return { returncode: 0, stdout: JSON.stringify({ items: replicaSets }), stderr: '' };
    if (args.includes('pods')) return { returncode: 0, stdout: JSON.stringify({ items: pods }), stderr: '' };
    if (args.includes('deployment')) {
      const name = args[args.indexOf('deployment') + 1];
      return { returncode: 0, stdout: JSON.stringify(name === 'arrowcol' ? collector : clientDeployment ?? collector), stderr: '' };
    }
    throw new Error('Unexpected scale test command.');
  };
}

interface PoolApiOptions { beforeGet?: (count: number) => void | Promise<void>; onPut?: (properties: Record<string, unknown>) => void | Promise<void>;
  status?: number; commitPut?: boolean; operationUrl?: string }
function mockPoolApi(t: TestContext, azure: Azure, options: PoolApiOptions = {}) {
  const poolId = `${azure.c.base}/providers/Microsoft.ContainerService/managedClusters/${azure.c.cluster}/agentPools/benche64`;
  let maxCount = 1, etag = '"pool-v1"', puts = 0, forwards = 0, reads = 0;
  const resource = () => ({ id: poolId, name: 'benche64', properties: { count: 0, vmSize: 'Standard_D64ds_v6',
    mode: 'User', provisioningState: 'Succeeded', enableAutoScaling: true, minCount: 0, maxCount } });
  t.mock.method(azure, 'arm', async (method: string, _resourceId: string) => {
    if (method !== 'GET') throw new Error('Unconditional ARM writes are refused by this fixture.');
    return resource();
  });
  t.mock.method(azure, 'request', async (method: string, url: string, _resource: string, body?: string | Buffer | null,
    headers: Record<string, string> = {}) => {
    assert.equal(new URL(url).pathname, poolId);
    if (method === 'GET') { await options.beforeGet?.(++reads);
      return [Buffer.from(JSON.stringify(resource())), { ':status': '200', etag }]; }
    forwards++; assert.equal(method, 'PUT'); assert.equal(headers['If-Match'], etag); puts++;
    const properties = (JSON.parse(String(body)) as { properties: Record<string, unknown> }).properties;
    await options.onPut?.(properties);
    const status = options.status ?? 200;
    if (status < 200 || status >= 300) throw new Error('ARM request returned HTTP ' + status);
    if (options.commitPut !== false) { maxCount = Number(properties.maxCount); etag = '"pool-v2"'; }
    return [null, { ':status': String(status), ...(options.operationUrl ? { 'azure-asyncoperation': options.operationUrl } : {}) }];
  });
  return { get puts() { return puts; }, get forwards() { return forwards; }, get reads() { return reads; }, get maxCount() { return maxCount; }, setMaxCount(value: number) { maxCount = value; } };
}
function controllerState(current: CampaignState, receipt: ScaleTrialReceipt, now: number): CampaignState {
  const terminalIndex = CUSTODY_CLEANUP_STEPS.indexOf('record-terminal-state');
  return { ...current, status: 'cleaning', fencingToken: 2, fenceReason: 'heartbeat-expired', fencedAtMs: now, cleanupStatus: 'running',
    cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({ step, idempotencyKey: `${campaignId}:2:${step}`,
      status: index < terminalIndex ? 'complete' : index === terminalIndex ? 'running' : 'pending' })), scaleTrials: [receipt] };
}

const poolStep: ScaleStep = { dimension: 'collector-node-max', before: 1, after: 2, evidence: ['collector-placement-blocked'],
  restore: { dimension: 'collector-node-max', before: 2, after: 1 }, baselineCommittedRowsPerSecond: 1000,
  baselineQapPendingBacklog: 0, baselineQapP95LatencyMs: 100 };
const clientStep: ScaleStep = { dimension: 'loadgen-max', before: 2, after: 3, evidence: ['generator-cpu-saturated'],
  restore: { dimension: 'loadgen-max', before: 3, after: 2 }, baselineCommittedRowsPerSecond: 1000,
  baselineQapPendingBacklog: 0, baselineQapP95LatencyMs: 100 };
const cpuStep: ScaleStep = { dimension: 'collector-cpu-limit', before: 1, after: 2, evidence: ['collector-cpu-saturated'],
  restore: { dimension: 'collector-cpu-limit', before: 2, after: 1 }, baselineCommittedRowsPerSecond: 1000,
  baselineQapPendingBacklog: 0, baselineQapP95LatencyMs: 100 };

test('scale admission rejects live rolling updates and multiple physical collector writers', async t => {
  for (const [strategy, writers] of [['RollingUpdate', 1], ['Recreate', 2]] as const) {
    const { store, azure } = await fixture();
    const adapter = createDefaultScaleAdapter(store, { azure, token, runCommand: scaleRunner(undefined, 1, strategy, writers) });
    await assert.rejects(adapter.assertCustody(token), strategy === 'Recreate'
      ? /exactly one ready physical ArrowCol writer/ : /Recreate strategy/);
    assert.equal(store.current!.state.scaleTrials, undefined);
  }
});

test('controller recovers an apply intent at after-state and restores within the original restore deadline', async t => {
  const { store, azure } = await fixture(); const current = store.current!.state, now = Date.now();
  const resourceId = azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster + '/agentPools/benche64';
  const api = mockPoolApi(t, azure); api.setMaxCount(2);
  const receipt = newScaleTrialReceipt({ state: current, step: poolStep, now, resource: { transport: 'azure-rest', resource_id: resourceId,
    api_version: '2024-10-01', kind: 'Microsoft.ContainerService/managedClusters/agentPools', namespace: null, name: 'benche64', uid: null } });
  await persistScaleTrial(store, receipt, token, now);
  store.current!.state = controllerState(store.current!.state, receipt, now);
  const cleanupToken = { ownerId, fencingToken: 2 }, expiredTrialNow = receipt.deadline_at_ms + 1;
  const adapter = createDefaultScaleAdapter(store, { azure, token: cleanupToken, now: () => expiredTrialNow, runCommand: scaleRunner(undefined, 0),
    authority: { mode: 'controller', campaignId, ownerId, fencingToken: 2, assertCurrent: async () => {} } });
  assert.equal(await restoreCampaignScaleTrials(store, adapter, cleanupToken), 'complete');
  assert.equal(api.puts, 1);
  assert.equal(api.maxCount, 1);
  assert.equal(store.current!.state.scaleTrials?.[0]?.phase, 'restored');
  assert.equal(store.current!.state.scaleTrials?.[0]?.dispatches?.apply, undefined);
  assert.equal(store.current!.state.scaleTrials?.[0]?.dispatches?.restore?.state, 'confirmed');
});

test('controller reconciles a durable apply dispatch at exact after-state for safe restoration', async t => {
  const { store, azure } = await fixture(); const current = store.current!.state, now = Date.now();
  const resourceId = `${azure.c.base}/providers/Microsoft.ContainerService/managedClusters/${azure.c.cluster}/agentPools/benche64`;
  const receipt = newScaleTrialReceipt({ state: current, step: poolStep, now, resource: { transport: 'azure-rest', resource_id: resourceId,
    api_version: '2024-10-01', kind: 'Microsoft.ContainerService/managedClusters/agentPools', namespace: null, name: 'benche64', uid: null } });
  const dispatch = { state: 'forwarding' as const, transport: 'azure-rest' as const, method: 'PUT' as const, target: receipt.target,
    before: receipt.target.before, started_at_ms: now + 1, etag: '"pool-v1"', request_sha256: 'a'.repeat(64) };
  await persistScaleTrial(store, receipt, token, now);
  await persistScaleTrial(store, { ...receipt, phase: 'apply-dispatch', dispatches: { apply: dispatch } }, token, now + 1);
  const claimed = store.current!.state.scaleTrials![0]!;
  store.current!.state = controllerState(store.current!.state, claimed, now + 2);
  const api = mockPoolApi(t, azure); api.setMaxCount(2);
  const cleanupToken = { ownerId, fencingToken: 2 };
  const adapter = createDefaultScaleAdapter(store, { azure, token: cleanupToken,
    authority: { mode: 'controller', campaignId, ownerId, fencingToken: 2, assertCurrent: async () => {} }, runCommand: scaleRunner(undefined, 0) });
  assert.equal(await adapter.reconcile(receipt.target), 'after');
  assert.equal(store.current!.state.scaleTrials?.[0]?.phase, 'applied');
  assert.equal(store.current!.state.scaleTrials?.[0]?.dispatches?.apply?.state, 'unknown');
  assert.equal(api.puts, 0);
});

test('real scale adapter applies and reconciles only the inventory-bound stamp pool while load is authorized', async t => {
  const { store, azure } = await fixture();
  let maxCount = 1;
  const calls: Array<{ method: string; resourceId: string; body?: unknown }> = [];
  const aksId = `${azure.c.base}/providers/Microsoft.ContainerService/managedClusters/${azure.c.cluster}`;
  const poolId = `${aksId}/agentPools/benche64`;
  t.mock.method(azure, 'arm', async (method: string, resourceId: string) => {
    calls.push({ method, resourceId });
    if (method !== 'GET') throw new Error('Unconditional ARM writes are refused by this fixture.');
    return { id: poolId, name: 'benche64', properties: { count: 0, vmSize: 'Standard_D64ds_v6',
      mode: 'User', provisioningState: 'Succeeded', enableAutoScaling: true, minCount: 0, maxCount } };
  });
  t.mock.method(azure, 'request', async (method: string, url: string, _resource: string, body?: string | Buffer | null, headers: Record<string, string> = {}) => {
    const resourceId = new URL(url).pathname; calls.push({ method, resourceId, body });
    if (method === 'GET') return [Buffer.from(JSON.stringify({ id: poolId, name: 'benche64', properties: { count: 0, vmSize: 'Standard_D64ds_v6',
      mode: 'User', provisioningState: 'Succeeded', enableAutoScaling: true, minCount: 0, maxCount } })), { ':status': '200', etag: '"pool-v1"' }];
    assert.equal(method, 'PUT'); assert.equal(resourceId, poolId); assert.equal(headers['If-Match'], '"pool-v1"');
    const properties = (JSON.parse(String(body)) as Record<string, any>).properties;
    maxCount = Number(properties.maxCount);
    return [null, { ':status': '200' }];
  });
  const target: ScaleTarget = { dimension: poolStep.dimension, before: poolStep.before, after: poolStep.after };
  const adapter = createDefaultScaleAdapter(store, { azure, token, runCommand: scaleRunner() });
  await adapter.assertCustody(token);
  await adapter.persistIntent(target, token, poolStep);
  await adapter.apply(target, token);
  assert.equal(await adapter.reconcile(target), 'after');
  assert.equal(maxCount, 2);
  assert.deepEqual(calls.filter(call => call.method !== 'GET').map(call => [call.method, call.resourceId]), [['PUT', poolId]]);
  assert.equal(store.current!.state.scaleTrials?.[0]?.resource.resource_id, poolId);
});

test('accepted ARM operations stay unresolved until exact after-state readback', async t => {
  const { store, azure } = await fixture();
  const operationUrl = `https://management.azure.com/subscriptions/${campaignId}/providers/Microsoft.ContainerService/locations/centralus/operations/scale-1?api-version=2024-10-01`;
  const api = mockPoolApi(t, azure, { status: 202, commitPut: false, operationUrl });
  const target: ScaleTarget = { dimension: poolStep.dimension, before: poolStep.before, after: poolStep.after };
  const adapter = createDefaultScaleAdapter(store, { azure, token, runCommand: scaleRunner() });
  await adapter.persistIntent(target, token, poolStep);
  await adapter.apply(target, token);
  const accepted = store.current!.state.scaleTrials?.[0]!;
  assert.equal(accepted.phase, 'apply-dispatch');
  assert.equal(accepted.dispatches?.apply?.state, 'confirmed');
  assert.equal(accepted.dispatches?.apply?.operation_url, operationUrl);
  assert.equal(await adapter.reconcile(target), 'unknown');
  api.setMaxCount(2);
  assert.equal(await adapter.reconcile(target), 'after');
  assert.equal(store.current!.state.scaleTrials?.[0]?.phase, 'applied');
});

test('lost restore acknowledgement with a lagging ARM read is not replayed', async t => {
  const { store, azure } = await fixture(); const current = store.current!.state, now = Date.now();
  const resourceId = `${azure.c.base}/providers/Microsoft.ContainerService/managedClusters/${azure.c.cluster}/agentPools/benche64`;
  const receipt: ScaleTrialReceipt = { ...newScaleTrialReceipt({ state: current, step: poolStep, now, resource: {
    transport: 'azure-rest', resource_id: resourceId, api_version: '2024-10-01',
    kind: 'Microsoft.ContainerService/managedClusters/agentPools', namespace: null, name: 'benche64', uid: null } }), phase: 'kept', fencing_token: 1 };
  store.current!.state = controllerState(current, receipt, now);
  const api = mockPoolApi(t, azure, { onPut: async () => { throw new Error('injected lost restore acknowledgement'); } });
  api.setMaxCount(2);
  const cleanupToken = { ownerId, fencingToken: 2 };
  const adapter = createDefaultScaleAdapter(store, { azure, token: cleanupToken,
    authority: { mode: 'controller', campaignId, ownerId, fencingToken: 2, assertCurrent: async () => {} }, runCommand: scaleRunner(undefined, 0) });
  await adapter.persistIntent(receipt.restore, cleanupToken);
  await assert.rejects(adapter.apply(receipt.restore, cleanupToken), /lost restore acknowledgement/);
  assert.equal(store.current!.state.scaleTrials?.[0]?.dispatches?.restore?.state, 'unknown');
  assert.equal(await adapter.reconcile(receipt.restore), 'unknown');
  await assert.rejects(adapter.apply(receipt.restore, cleanupToken), /fresh durable intent/);
  assert.equal(api.puts, 1);
});
test('HTTP 5xx ARM response remains UNKNOWN and is not replayed', async t => {
  const { store, azure } = await fixture();
  const api = mockPoolApi(t, azure, { status: 500, commitPut: false });
  const target: ScaleTarget = { dimension: poolStep.dimension, before: poolStep.before, after: poolStep.after };
  const adapter = createDefaultScaleAdapter(store, { azure, token, runCommand: scaleRunner() });
  await adapter.persistIntent(target, token, poolStep);
  await assert.rejects(adapter.apply(target, token), /HTTP 500/);
  assert.equal(store.current!.state.scaleTrials?.[0]?.dispatches?.apply?.state, 'unknown');
  const restarted = createDefaultScaleAdapter(store, { azure, token, runCommand: scaleRunner() });
  assert.equal(await restarted.reconcile(target), 'unknown');
  await assert.rejects(restarted.apply(target, token), /fresh durable intent/);
  assert.equal(api.puts, 1);
});

test('expired persisted before-state intent cannot start an ARM request', async t => {
  const { store, azure } = await fixture(); const now = Date.now();
  const api = mockPoolApi(t, azure);
  const target: ScaleTarget = { dimension: poolStep.dimension, before: poolStep.before, after: poolStep.after };
  const adapter = createDefaultScaleAdapter(store, { azure, token, runCommand: scaleRunner(), now: () => now });
  await adapter.persistIntent(target, token, poolStep);
  store.current!.state.scaleTrials![0]!.deadline_at_ms = now - 1;
  await assert.rejects(adapter.apply(target, token), /outside the original persisted deadline/);
  assert.equal(api.reads, 0);
  assert.equal(api.puts, 0);
});
test('real provider does not replay a lost ARM acknowledgement after restart with a lagging read', async t => {
  const { store, azure } = await fixture();
  const api = mockPoolApi(t, azure, { onPut: async () => { throw new Error('injected lost ARM acknowledgement'); } });
  const target: ScaleTarget = { dimension: poolStep.dimension, before: poolStep.before, after: poolStep.after };
  const adapter = createDefaultScaleAdapter(store, { azure, token, runCommand: scaleRunner() });
  await adapter.persistIntent(target, token, poolStep);
  await assert.rejects(adapter.apply(target, token), /lost ARM acknowledgement/);
  const dispatch = store.current!.state.scaleTrials?.[0]?.dispatches?.apply;
  assert.equal(dispatch?.state, 'unknown');
  const restarted = createDefaultScaleAdapter(store, { azure, token, runCommand: scaleRunner() });
  await restarted.persistIntent(target, token, poolStep);
  assert.equal(await restarted.reconcile(target), 'unknown');
  await assert.rejects(restarted.apply(target, token), /fresh durable intent/);
  assert.equal(api.puts, 1);
});

test('concurrent same-millisecond dispatch claims admit only one actual ARM forward', async t => {
  const { store, azure } = await fixture();
  const api = mockPoolApi(t, azure), fixedNow = Date.now();
  let release!: () => void; const bothClaims = new Promise<void>(resolve => { release = resolve; });
  const claimBindings: Array<{ startedAt: number; requestHash: string }> = [];
  const originalWrite = store.write.bind(store);
  store.write = async (state, etag) => {
    const receipt = state.scaleTrials?.at(-1), dispatch = receipt?.dispatches?.apply;
    if (receipt?.phase === 'apply-dispatch' && dispatch?.state === 'forwarding') {
      claimBindings.push({ startedAt: dispatch.started_at_ms, requestHash: dispatch.request_sha256 });
      if (claimBindings.length === 2) release();
      await bothClaims;
    }
    return originalWrite(state, etag);
  };
  const target: ScaleTarget = { dimension: poolStep.dimension, before: poolStep.before, after: poolStep.after };
  const adapter = createDefaultScaleAdapter(store, { azure, token, runCommand: scaleRunner(), now: () => fixedNow });
  await adapter.persistIntent(target, token, poolStep);
  await Promise.allSettled([adapter.apply(target, token), adapter.apply(target, token)]);
  assert.ok(claimBindings.length >= 2);
  assert.equal(claimBindings[0]?.startedAt, fixedNow);
  assert.equal(claimBindings[1]?.startedAt, fixedNow);
  assert.equal(claimBindings[0]?.requestHash, claimBindings[1]?.requestHash);
  assert.equal(api.forwards, 1);
});

test('final ARM observation refuses target drift and fence change before forwarding', async t => {
  const drift = await fixture();
  let driftApi!: ReturnType<typeof mockPoolApi>;
  driftApi = mockPoolApi(t, drift.azure, { beforeGet: () => { driftApi.setMaxCount(2); } });
  const target: ScaleTarget = { dimension: poolStep.dimension, before: poolStep.before, after: poolStep.after };
  const driftAdapter = createDefaultScaleAdapter(drift.store, { azure: drift.azure, token, runCommand: scaleRunner() });
  await driftAdapter.persistIntent(target, token, poolStep);
  await assert.rejects(driftAdapter.apply(target, token), /before-state changed/);
  assert.equal(driftApi.puts, 0);
});

test('fencing during the final ARM read prevents forwarding', async t => {
  const { store, azure } = await fixture();
  const api = mockPoolApi(t, azure, { beforeGet: () => { store.current!.state.fencingToken = 2; } });
  const target: ScaleTarget = { dimension: poolStep.dimension, before: poolStep.before, after: poolStep.after };
  const adapter = createDefaultScaleAdapter(store, { azure, token, runCommand: scaleRunner() });
  await adapter.persistIntent(target, token, poolStep);
  await assert.rejects(adapter.apply(target, token), /custody changed|fenced/);
  assert.equal(api.puts, 0);
});

test('trial deadline crossing during final ARM read blocks dispatch', async t => {
  const { store, azure } = await fixture(); let now = Date.now();
  const api = mockPoolApi(t, azure, { beforeGet: () => { now = store.current!.state.scaleTrials![0]!.deadline_at_ms + 1; } });
  const target: ScaleTarget = { dimension: poolStep.dimension, before: poolStep.before, after: poolStep.after };
  const adapter = createDefaultScaleAdapter(store, { azure, token, runCommand: scaleRunner(), now: () => now });
  await adapter.persistIntent(target, token, poolStep);
  const receipt = store.current!.state.scaleTrials![0]!;
  receipt.deadline_at_ms = now + 120_000; receipt.restore_deadline_at_ms = receipt.deadline_at_ms + 5 * 60_000;
  now = Date.now() - 1;
  await assert.rejects(adapter.apply(target, token), /outside the original persisted deadline/);
  assert.equal(api.puts, 0);
});

test('stopped collector CPU restoration patches only the template limit and keeps replicas at zero', async t => {
  const { store, azure, namespace } = await fixture(); const current = store.current!.state, now = Date.now();
  const collector = current.runInventory!.resources.find(resource => resource.role === 'collector')!;
  const receipt: ScaleTrialReceipt = { ...newScaleTrialReceipt({ state: current, step: cpuStep, now, resource: {
    transport: 'kubernetes', resource_id: `apps/v1/Deployment/${current.runInventory!.namespace.name}/${collector.name}/${collector.uid}`,
    api_version: 'apps/v1', kind: 'Deployment', namespace: current.runInventory!.namespace.name, name: collector.name, uid: collector.uid } }),
    phase: 'applied', fencing_token: 1 };
  store.current!.state = controllerState(current, receipt, now);
  const cleanupToken = { ownerId, fencingToken: 2 }, commands: string[][] = []; let restoredLimit = '2';
  const runner: CustodyCommandRunner = async args => {
    commands.push(args);
    if (args.includes('patch')) {
      const patch = JSON.parse(args[args.indexOf('--patch') + 1]!) as Array<{ op: string; value?: unknown }>;
      restoredLimit = String(patch.at(-1)?.value);
      return { returncode: 0, stdout: '', stderr: '' };
    }
    if (args.includes('replicasets') || args.includes('pods')) return { returncode: 0, stdout: JSON.stringify({ items: [] }), stderr: '' };
    const deployment = { apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: collector.name, namespace, uid: collector.uid, generation: 1, resourceVersion: 'rv-collector' },
      spec: { replicas: 0, strategy: { type: 'Recreate' }, selector: { matchLabels: { app: 'arrowcol' } },
        template: { spec: { containers: [{ name: 'dataflow', resources: { requests: { cpu: '1', memory: '4Gi' }, limits: { cpu: restoredLimit, memory: '8Gi' } } }] } } },
      status: { observedGeneration: 1, availableReplicas: 0, readyReplicas: 0 } };
    return { returncode: 0, stdout: JSON.stringify(deployment), stderr: '' };
  };
  await persistScaleTrial(store, { ...receipt, phase: 'restore-intent' }, cleanupToken);
  const adapter = createDefaultScaleAdapter(store, { azure, token: cleanupToken, authority: { mode: 'controller', campaignId, ownerId, fencingToken: 2, assertCurrent: async () => {} }, runCommand: runner });
  await adapter.apply(receipt.restore, cleanupToken);
  assert.equal(await adapter.reconcile(receipt.restore), 'after');
  const patchArgs = commands.find(args => args.includes('patch'))!;
  const patch = JSON.parse(patchArgs[patchArgs.indexOf('--patch') + 1]!) as Array<Record<string, unknown>>;
  assert.equal(patch.some(op => op.path === '/spec/replicas'), false);
  assert.deepEqual(patch.at(-1), { op: 'replace', path: '/spec/template/spec/containers/0/resources/limits/cpu', value: '1' });
  assert.equal(store.current!.state.scaleTrials?.[0]?.phase, 'restored');
});

test('stopped load generator cleanup records restore-stopped without restarting it', async t => {
  const { store, azure, namespace } = await fixture(); const current = store.current!.state, now = Date.now();
  const client = current.runInventory!.resources.find(resource => resource.role === 'client')!;
  const receipt: ScaleTrialReceipt = { ...newScaleTrialReceipt({ state: current, step: clientStep, now, resource: {
    transport: 'kubernetes', resource_id: `apps/v1/Deployment/${namespace}/${client.name}/${client.uid}`,
    api_version: 'apps/v1', kind: 'Deployment', namespace, name: client.name, uid: client.uid } }), phase: 'kept', fencing_token: 1 };
  store.current!.state = controllerState(current, receipt, now);
  const cleanupToken = { ownerId, fencingToken: 2 };
  const stoppedClient = { apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: client.name, namespace, uid: client.uid, generation: 1, resourceVersion: 'rv-client' },
    spec: { replicas: 0, selector: { matchLabels: { app: 'arrowcol-client' } } }, status: { observedGeneration: 1, availableReplicas: 0 } };
  let patches = 0;
  const runner: CustodyCommandRunner = async args => { if (args.includes('patch')) patches++; return scaleRunner(stoppedClient, 0)(args, 30); };
  const adapter = createDefaultScaleAdapter(store, { azure, token: cleanupToken, authority: { mode: 'controller', campaignId, ownerId, fencingToken: 2, assertCurrent: async () => {} }, runCommand: runner });
  await adapter.persistIntent(receipt.restore, cleanupToken);
  assert.equal(await adapter.reconcile(receipt.restore), 'after');
  assert.equal(store.current!.state.scaleTrials?.[0]?.phase, 'restore-stopped');
  assert.equal(patches, 0);
});

test('stopped collector restore requires a resourceVersion before forwarding', async () => {
  const { store, azure, namespace } = await fixture();
  const current = store.current!.state;
  const inventory = current.runInventory!;
  const collector = inventory.resources.find(resource => resource.role === 'collector')!;
  const now = Date.now();
  const receipt: ScaleTrialReceipt = { ...newScaleTrialReceipt({ state: current, step: cpuStep, now, resource: {
    transport: 'kubernetes', resource_id: `apps/v1/Deployment/${namespace}/${collector.name}/${collector.uid}`,
    api_version: 'apps/v1', kind: 'Deployment', namespace, name: collector.name, uid: collector.uid } }), phase: 'applied', fencing_token: 2 };
  const terminalIndex = CUSTODY_CLEANUP_STEPS.indexOf('record-terminal-state');
  store.current!.state = { ...current, status: 'cleaning', fencingToken: 2, fenceReason: 'heartbeat-expired', fencedAtMs: now, cleanupStatus: 'running',
    cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({ step, idempotencyKey: `${campaignId}:2:${step}`,
      status: index < terminalIndex ? 'complete' : index === terminalIndex ? 'running' : 'pending' })), runInventory: inventory, scaleTrials: [receipt] };
  const authority: CleanupMutationAuthority = { mode: 'controller', campaignId, ownerId, fencingToken: 2, assertCurrent: async () => {} };
  const stoppedCollector = { apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: collector.name, namespace, uid: collector.uid, generation: 1 },
    spec: { replicas: 0, strategy: { type: 'Recreate' }, selector: { matchLabels: { app: 'arrowcol' } },
      template: { spec: { containers: [{ name: 'dataflow', resources: { requests: { cpu: '1', memory: '4Gi' }, limits: { cpu: '2', memory: '8Gi' } } }] } } },
    status: { observedGeneration: 1, availableReplicas: 0, readyReplicas: 0 } };
  let patchCalls = 0;
  const runCommand: CustodyCommandRunner = async args => {
    if (args.includes('patch')) { patchCalls++; return { returncode: 0, stdout: '', stderr: '' }; }
    if (args.includes('replicasets') || args.includes('pods')) return { returncode: 0, stdout: JSON.stringify({ items: [] }), stderr: '' };
    return { returncode: 0, stdout: JSON.stringify(stoppedCollector), stderr: '' };
  };
  const cleanupToken = { ownerId, fencingToken: 2 };
  await persistScaleTrial(store, { ...receipt, phase: 'restore-intent' }, cleanupToken);
  const adapter = createDefaultScaleAdapter(store, { azure, token: cleanupToken, authority, runCommand });
  await assert.rejects(adapter.apply(receipt.restore, cleanupToken), /resourceVersion changed/);
  assert.equal(patchCalls, 0);
});

test('default executor restart restores a kept scale trial before lifecycle recovery despite the blob ETag change', async t => {
  const { store, azure, namespace } = await fixture(); const current = store.current!.state, now = Date.now();
  const collector = current.runInventory!.resources.find(resource => resource.role === 'collector')!;
  const resourceId = `${azure.c.base}/providers/Microsoft.ContainerService/managedClusters/${azure.c.cluster}/agentPools/benche64`;
  const receipt: ScaleTrialReceipt = { ...newScaleTrialReceipt({ state: current, step: poolStep, now, resource: {
    transport: 'azure-rest', resource_id: resourceId, api_version: '2024-10-01',
    kind: 'Microsoft.ContainerService/managedClusters/agentPools', namespace: null, name: 'benche64', uid: null } }), phase: 'kept', fencing_token: 1 };
  store.current!.state = controllerState(current, receipt, now);
  const api = mockPoolApi(t, azure); api.setMaxCount(2);
  let strategy = 'RollingUpdate'; const order: string[] = [];
  const runCommand: CustodyCommandRunner = async args => {
    if (args.includes('replicasets') || args.includes('pods')) return { returncode: 0, stdout: JSON.stringify({ items: [] }), stderr: '' };
    if (args.includes('patch')) return { returncode: 0, stdout: '', stderr: '' };
    const deployment = { apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: collector.name, namespace, uid: collector.uid, generation: 1, resourceVersion: 'rv-collector' },
      spec: { replicas: 0, strategy: { type: strategy }, selector: { matchLabels: { app: 'arrowcol' } },
        template: { spec: { containers: [{ name: 'dataflow', resources: { requests: { cpu: '1', memory: '4Gi' }, limits: { cpu: '2', memory: '8Gi' } } }] } } },
      status: { observedGeneration: 1, availableReplicas: 0, readyReplicas: 0 } };
    return { returncode: 0, stdout: JSON.stringify(deployment), stderr: '' };
  };
  const complete = async (..._args: unknown[]) => 'complete' as const;
  const lifecycle: ExternalCleanupActions = {
    discoverExactInventory: async value => value, stopAdmission: complete, boundedDrain: complete, preserveEvidence: complete,
    stopMaintenanceIfUidAndOwnerMatch: complete, captureTerminalState: async () => { order.push('terminal-capture'); return 'complete'; },
    deleteNamespaceIfUidAndOwnerMatch: complete, deleteQapBindingIfOwned: complete, deleteFicIfExact: complete,
    verifyCellPreservation: complete, restoreSafeIdle: complete, verifySafeIdle: complete,
    recoverLifecycle: async () => { order.push('lifecycle-recovery'); return 'complete'; },
  };
  const defaults = createDefaultCleanupProviders(store, { azure, workDirectory: '.', lifecycle, runCommand });
  const actions: ExternalCleanupActions = { ...defaults, discoverExactInventory: async value => value };
  const execute = createExternalCleanupExecutor(store, actions,
    { maintenanceGraceSeconds: 900, maintenanceStopTimeoutMs: 1_200_000, budget_source: 'effective_stop_budgets' });
  await assert.rejects(execute('record-terminal-state', store.current!.state, `${campaignId}:2:record-terminal-state`), /Recreate/);
  assert.equal(api.puts, 0); assert.deepEqual(order, []);
  strategy = 'Recreate';
  assert.equal(await execute('record-terminal-state', store.current!.state, `${campaignId}:2:record-terminal-state`), 'complete');
  assert.equal(api.puts, 1);
  assert.equal(store.current!.state.scaleTrials?.[0]?.phase, 'restored');
  assert.deepEqual(order, ['lifecycle-recovery']);
});

test('terminal cleanup can persist a confirmed no-effect scale trial as reverted', async () => {
  const { store, azure } = await fixture();
  const active = store.current!.state;
  const now = Date.now();
  const resourceId = `${azure.c.base}/providers/Microsoft.ContainerService/managedClusters/${azure.c.cluster}/agentPools/benche64`;
  const receipt = newScaleTrialReceipt({ state: active, step: poolStep, now, resource: {
    transport: 'azure-rest', resource_id: resourceId, api_version: '2024-10-01',
    kind: 'Microsoft.ContainerService/managedClusters/agentPools', namespace: null, name: 'benche64', uid: null } });
  const terminalIndex = CUSTODY_CLEANUP_STEPS.indexOf('record-terminal-state');
  store.current!.state = { ...active, status: 'cleaning', fencingToken: 2, fenceReason: 'heartbeat-expired',
    fencedAtMs: now, cleanupStatus: 'running',
    cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({ step, idempotencyKey: `${campaignId}:2:${step}`,
      status: index < terminalIndex ? 'complete' : index === terminalIndex ? 'running' : 'pending' })), scaleTrials: [receipt] };
  await persistScaleTrial(store, { ...receipt, phase: 'reverted' }, { ownerId, fencingToken: 2 });
  assert.equal(store.current!.state.scaleTrials?.[0]?.phase, 'reverted');
});


function cleanupLifecycle(order: string[] = []): ExternalCleanupActions {
  const complete = async (..._args: unknown[]) => 'complete' as const;
  return {
    discoverExactInventory: async inventory => inventory, stopAdmission: complete, boundedDrain: complete, preserveEvidence: complete,
    stopMaintenanceIfUidAndOwnerMatch: complete, captureTerminalState: async () => { order.push('terminal-capture'); return 'complete'; },
    deleteNamespaceIfUidAndOwnerMatch: complete, deleteQapBindingIfOwned: complete, deleteFicIfExact: complete,
    verifyCellPreservation: complete, restoreSafeIdle: complete, verifySafeIdle: complete,
    recoverLifecycle: async () => { order.push('lifecycle-recovery'); return 'complete'; },
  };
}

function stoppedCollectorDeployment(name: string, namespace: string, uid: string) {
  return { apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name, namespace, uid, generation: 1, resourceVersion: 'rv-collector' },
    spec: { replicas: 0, strategy: { type: 'Recreate' }, selector: { matchLabels: { app: 'arrowcol' } },
      template: { spec: { containers: [{ name: 'dataflow', resources: { requests: { cpu: '1', memory: '4Gi' }, limits: { cpu: '2', memory: '8Gi' } } }] } } },
    status: { observedGeneration: 1, availableReplicas: 0, readyReplicas: 0 } };
}

test('cleanup treats a deleting but still-running collector pod as physically active', async t => {
  const { store, azure, namespace } = await fixture(); const current = store.current!.state, now = Date.now();
  const collector = current.runInventory!.resources.find(resource => resource.role === 'collector')!;
  const receipt: ScaleTrialReceipt = { ...newScaleTrialReceipt({ state: current, step: cpuStep, now, resource: {
    transport: 'kubernetes', resource_id: `apps/v1/Deployment/${namespace}/${collector.name}/${collector.uid}`,
    api_version: 'apps/v1', kind: 'Deployment', namespace, name: collector.name, uid: collector.uid } }), phase: 'applied', fencing_token: 1 };
  store.current!.state = controllerState(current, receipt, now);
  const pod = { metadata: { name: 'terminating-arrowcol', deletionTimestamp: new Date(now).toISOString() },
    status: { phase: 'Running', containerStatuses: [{ ready: false, state: { running: {} } }] } };
  const cleanupToken = { ownerId, fencingToken: 2 };
  const runner: CustodyCommandRunner = async args => {
    if (args.includes('patch')) return { returncode: 0, stdout: '', stderr: '' };
    if (args.includes('replicasets')) return { returncode: 0, stdout: JSON.stringify({ items: [] }), stderr: '' };
    if (args.includes('pods')) return { returncode: 0, stdout: JSON.stringify({ items: [pod] }), stderr: '' };
    if (args.includes('deployment')) return { returncode: 0, stdout: JSON.stringify(stoppedCollectorDeployment('arrowcol', namespace, collector.uid!)), stderr: '' };
    throw new Error('Unexpected collector termination proof command.');
  };
  const adapter = createDefaultScaleAdapter(store, { azure, token: cleanupToken,
    authority: { mode: 'controller', campaignId, ownerId, fencingToken: 2, assertCurrent: async () => {} }, runCommand: runner });
  await assert.rejects(adapter.assertCustody(cleanupToken), /quiescent collector with zero desired, available and physical writer pods/);
});

test('stopped load generator remains active while a deleting pod is still running', async t => {
  const { store, azure, namespace } = await fixture(); const current = store.current!.state, now = Date.now();
  const collector = current.runInventory!.resources.find(resource => resource.role === 'collector')!;
  const client = current.runInventory!.resources.find(resource => resource.role === 'client')!;
  const receipt: ScaleTrialReceipt = { ...newScaleTrialReceipt({ state: current, step: clientStep, now, resource: {
    transport: 'kubernetes', resource_id: `apps/v1/Deployment/${namespace}/${client.name}/${client.uid}`,
    api_version: 'apps/v1', kind: 'Deployment', namespace, name: client.name, uid: client.uid } }), phase: 'kept', fencing_token: 1 };
  store.current!.state = controllerState(current, receipt, now);
  const cleanupToken = { ownerId, fencingToken: 2 };
  const pod = { metadata: { name: 'terminating-client', deletionTimestamp: new Date(now).toISOString() },
    status: { phase: 'Running', containerStatuses: [{ state: { running: {} } }] } };
  const stoppedClient = { apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: client.name, namespace, uid: client.uid, generation: 1, resourceVersion: 'rv-client' },
    spec: { replicas: 0, selector: { matchLabels: { app: 'arrowcol-client' } } }, status: { observedGeneration: 1, availableReplicas: 0 } };
  const base = scaleRunner(stoppedClient, 0);
  let patches = 0;
  const runner: CustodyCommandRunner = async (args, timeout) => {
    if (args.includes('pods') && args[args.indexOf('--selector') + 1] === 'app=arrowcol-client')
      return { returncode: 0, stdout: JSON.stringify({ items: [pod] }), stderr: '' };
    if (args.includes('patch')) patches++;
    return base(args, timeout);
  };
  const adapter = createDefaultScaleAdapter(store, { azure, token: cleanupToken,
    authority: { mode: 'controller', campaignId, ownerId, fencingToken: 2, assertCurrent: async () => {} }, runCommand: runner });
  await adapter.persistIntent(receipt.restore, cleanupToken);
  await assert.rejects(adapter.reconcile(receipt.restore), /Stopped load generator still has physical pods/);
  assert.equal(patches, 0);
});

test('default executor reconciles a lost restore only after lagging read settles, without a second forward', async t => {
  const { store, azure, namespace } = await fixture(); const current = store.current!.state, now = Date.now();
  const collector = current.runInventory!.resources.find(resource => resource.role === 'collector')!;
  const receipt: ScaleTrialReceipt = { ...newScaleTrialReceipt({ state: current, step: poolStep, now, resource: {
    transport: 'azure-rest', resource_id: `${azure.c.base}/providers/Microsoft.ContainerService/managedClusters/${azure.c.cluster}/agentPools/benche64`,
    api_version: '2024-10-01', kind: 'Microsoft.ContainerService/managedClusters/agentPools', namespace: null, name: 'benche64', uid: null } }),
    phase: 'kept', fencing_token: 1 };
  store.current!.state = controllerState(current, receipt, now);
  const api = mockPoolApi(t, azure, { onPut: async () => { throw new Error('injected lost restore acknowledgement'); } });
  api.setMaxCount(2);
  const lifecycleOrder: string[] = [];
  const actions = createDefaultCleanupProviders(store, { azure, workDirectory: '.', lifecycle: cleanupLifecycle(lifecycleOrder),
    runCommand: scaleRunner(stoppedCollectorDeployment(collector.name, namespace, collector.uid!), 0) });
  const execute = createExternalCleanupExecutor(store, { ...actions, discoverExactInventory: async inventory => inventory },
    { maintenanceGraceSeconds: 900, maintenanceStopTimeoutMs: 1_200_000, budget_source: 'effective_stop_budgets' });
  const step = 'record-terminal-state', key = `${campaignId}:2:${step}`;
  await assert.rejects(execute(step, store.current!.state, key), /injected lost restore acknowledgement/);
  assert.equal(store.current!.state.scaleTrials?.[0]?.dispatches?.restore?.state, 'unknown');
  assert.equal(api.forwards, 1);
  assert.equal(await execute(step, store.current!.state, key), 'pending');
  assert.equal(api.forwards, 1);
  api.setMaxCount(1);
  assert.equal(await execute(step, store.current!.state, key), 'complete');
  assert.equal(api.forwards, 1);
  assert.equal(store.current!.state.scaleTrials?.[0]?.phase, 'restored');
  assert.deepEqual(lifecycleOrder, ['lifecycle-recovery']);
});

test('default terminal execute refuses an expired persisted deadline before restore reads or forwards', async t => {
  const { store, azure, namespace } = await fixture(); const current = store.current!.state, started = Date.now();
  const collector = current.runInventory!.resources.find(resource => resource.role === 'collector')!;
  const receipt: ScaleTrialReceipt = { ...newScaleTrialReceipt({ state: current, step: poolStep, now: started, resource: {
    transport: 'azure-rest', resource_id: `${azure.c.base}/providers/Microsoft.ContainerService/managedClusters/${azure.c.cluster}/agentPools/benche64`,
    api_version: '2024-10-01', kind: 'Microsoft.ContainerService/managedClusters/agentPools', namespace: null, name: 'benche64', uid: null } }),
    phase: 'kept', fencing_token: 1 };
  store.current!.state = controllerState(current, receipt, started);
  const api = mockPoolApi(t, azure); api.setMaxCount(2);
  let clock = started; t.mock.method(Date, 'now', () => clock);
  const originalWrite = store.write.bind(store);
  store.write = async (state, etag) => {
    const result = await originalWrite(state, etag), terminal = state.runInventory?.cleanup.terminal_capture;
    if (terminal?.state === 'running' && terminal.deadline_unix_ms !== null) clock = terminal.deadline_unix_ms + 1;
    return result;
  };
  const actions = createDefaultCleanupProviders(store, { azure, workDirectory: '.', lifecycle: cleanupLifecycle(),
    runCommand: scaleRunner(stoppedCollectorDeployment(collector.name, namespace, collector.uid!), 0) });
  const execute = createExternalCleanupExecutor(store, { ...actions, discoverExactInventory: async inventory => inventory },
    { maintenanceGraceSeconds: 900, maintenanceStopTimeoutMs: 1_200_000, budget_source: 'effective_stop_budgets' });
  const step = 'record-terminal-state', key = `${campaignId}:2:${step}`;
  const outcome = await execute(step, store.current!.state, key).then(() => null, error => error);
  assert.deepEqual({ refused: outcome instanceof Error && outcome.message.includes('Cleanup stop deadline expired'), reads: api.reads, forwards: api.forwards },
    { refused: true, reads: 0, forwards: 0 });
  assert.ok(store.current!.state.scaleTrials![0]!.restore_deadline_at_ms > clock);
});

test('default terminal recovery refuses an expired persisted deadline before restore reads or forwards', async t => {
  const { store, azure, namespace } = await fixture(); const current = store.current!.state, started = Date.now();
  const collector = current.runInventory!.resources.find(resource => resource.role === 'collector')!;
  const receipt: ScaleTrialReceipt = { ...newScaleTrialReceipt({ state: current, step: poolStep, now: started, resource: {
    transport: 'azure-rest', resource_id: `${azure.c.base}/providers/Microsoft.ContainerService/managedClusters/${azure.c.cluster}/agentPools/benche64`,
    api_version: '2024-10-01', kind: 'Microsoft.ContainerService/managedClusters/agentPools', namespace: null, name: 'benche64', uid: null } }),
    phase: 'kept', fencing_token: 1 };
  const campaign = controllerState(current, receipt, started), deadline = started + 120_000;
  campaign.runInventory = { ...campaign.runInventory!, cleanup: { ...campaign.runInventory!.cleanup,
    terminal_capture: { state: 'running', deadline_unix_ms: deadline, reason_code: null } } };
  store.current!.state = campaign;
  const api = mockPoolApi(t, azure); api.setMaxCount(2);
  t.mock.method(Date, 'now', () => deadline + 1);
  const actions = createDefaultCleanupProviders(store, { azure, workDirectory: '.', lifecycle: cleanupLifecycle(),
    runCommand: scaleRunner(stoppedCollectorDeployment(collector.name, namespace, collector.uid!), 0) });
  const execute = createExternalCleanupExecutor(store, { ...actions, discoverExactInventory: async inventory => inventory },
    { maintenanceGraceSeconds: 900, maintenanceStopTimeoutMs: 1_200_000, budget_source: 'effective_stop_budgets' });
  const step = 'record-terminal-state', key = `${campaignId}:2:${step}`;
  const outcome = await execute(step, store.current!.state, key).then(() => null, error => error);
  assert.deepEqual({ refused: outcome instanceof Error && outcome.message.includes('Cleanup stop deadline expired'), reads: api.reads, forwards: api.forwards },
    { refused: true, reads: 0, forwards: 0 });
  assert.ok(store.current!.state.scaleTrials![0]!.restore_deadline_at_ms > deadline + 1);
});
