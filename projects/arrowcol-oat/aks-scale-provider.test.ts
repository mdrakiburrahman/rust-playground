import assert from 'node:assert/strict';
import { test, type TestContext } from 'node:test';
import { Azure, Config } from '../../infra/azure/bootstrap.js';
import { defaultStampConfig } from '../../infra/azure/stamp.js';
import { CampaignConflictError, CUSTODY_CLEANUP_STEPS, armCampaign, publishRunInventory, startCampaign, validateCampaignState,
  type CampaignState, type CampaignStore, type RunInventory, type StoredCampaign, type ScaleTrialReceipt } from './aks-campaign.js';
import { assertWriterBoundary, createDefaultScaleAdapter, restoreCampaignScaleTrials } from './aks-scale-provider.js';
import { createDefaultCleanupProviders } from './aks-default-providers.js';
import { createExternalCleanupExecutor, type ExternalCleanupActions } from './aks-cleanup-executor.js';
import { newScaleTrialReceipt, persistScaleTrial, type ScaleStep, type ScaleTarget } from './aks-scale.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';
import type { CustodyCommandRunner } from './aks-setup-cleanup.js';
import { HttpFailure, TAGS, type HttpReceipt } from '../../infra/azure/bootstrap.js';
import { createFleetAdapter, observeFleetBinding, observeFleetImageArchitectures } from './aks-fleet-provider.js';
import { cleanupAksFleet } from './aks-fleet.js';
import { acquireFleetAuthority, FleetUnsettledDispatchError, type FleetBinding, type FleetIntentAuthority, type FleetPoolObservation } from './aks-fleet-custody.js';

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
    drain_budget: { budget_ms: 120_000, collector_grace_ms: 300_000, pre_stop_capture_margin_ms: 30_000 },
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
  baselineQapPendingBacklog: 0 };
const clientStep: ScaleStep = { dimension: 'loadgen-max', before: 2, after: 3, evidence: ['generator-cpu-saturated'],
  restore: { dimension: 'loadgen-max', before: 3, after: 2 }, baselineCommittedRowsPerSecond: 1000,
  baselineQapPendingBacklog: 0 };
const cpuStep: ScaleStep = { dimension: 'collector-cpu-limit', before: 1, after: 2, evidence: ['collector-cpu-saturated'],
  restore: { dimension: 'collector-cpu-limit', before: 2, after: 1 }, baselineCommittedRowsPerSecond: 1000,
  baselineQapPendingBacklog: 0 };

test('writer observation reuses the three existing GETs without coercing unavailable pod identities', async () => {
  const { store, namespace } = await fixture(), inventory = store.current!.state.runInventory!;
  const authority: CleanupMutationAuthority = { mode: 'producer', campaignId, ownerId, fencingToken: 1, assertCurrent: async () => {} };
  for (const identity of ['missing', 'observed', 'invalid'] as const) {
    const reads: string[][] = [], original = scaleRunner();
    const runner: CustodyCommandRunner = async (...args) => {
      reads.push(args[0]);
      assert.equal(args[0].includes('patch'), false);
      const result = await original(...args);
      if (identity !== 'missing' && args[0].includes('pods')) {
        const list = JSON.parse(result.stdout) as { items: Array<{ metadata: { uid?: unknown };
          spec?: { nodeName: unknown }; status: { containerStatuses: Array<{ ready: boolean; name?: unknown; containerID?: unknown }> } }> };
        list.items[0].metadata.uid = identity === 'observed' ? 'synthetic-pod-uid' : 7;
        list.items[0].spec = { nodeName: identity === 'observed' ? 'synthetic-node' : 7 };
        Object.assign(list.items[0].status.containerStatuses[0], {
          name: identity === 'observed' ? 'dataflow' : 7, containerID: identity === 'observed' ? 'containerd://synthetic' : 7 });
        result.stdout = JSON.stringify(list);
      }
      return result;
    };
    assert.deepEqual(await assertWriterBoundary(inventory, authority, runner), {
      server: inventory.fic!.plan!.server, namespace, deploymentUid: inventory.resources[0].uid, podName: 'arrowcol-0',
      podUid: identity === 'observed' ? 'synthetic-pod-uid' : null,
      nodeName: identity === 'observed' ? 'synthetic-node' : null,
      containerName: identity === 'observed' ? 'dataflow' : null,
      containerId: identity === 'observed' ? 'containerd://synthetic' : null });
    assert.equal(reads.length, 3);
    for (const kind of ['deployment', 'replicasets', 'pods']) assert.equal(reads.filter(args => args.includes(kind)).length, 1);
  }
  assert.equal(await assertWriterBoundary(inventory, { ...authority, mode: 'controller' },
    scaleRunner(undefined, 0, 'Recreate', 0)), undefined);
});

test('scale admission rejects live rolling updates and multiple physical collector writers', async t => {
  for (const [strategy, writers] of [['RollingUpdate', 1], ['Recreate', 2]] as const) {
    const { store, azure } = await fixture();
    const adapter = createDefaultScaleAdapter(store, { azure, token, runCommand: scaleRunner(undefined, 1, strategy, writers) });
    await assert.rejects(adapter.assertCustody(token), strategy === 'Recreate'
      ? /exactly one ready physical ArrowCol writer/ : /Recreate strategy/);
    assert.equal(store.current!.state.scaleTrials, undefined);
  }
});

test('producer admission rejects a deleting but ready collector pod', async t => {
  const { store, azure } = await fixture();
  const base = scaleRunner();
  const runner: CustodyCommandRunner = async (...args) => {
    const result = await base(...args);
    if (!args[0].includes('pods')) return result;
    const list = JSON.parse(result.stdout) as { items: Array<{ metadata: Record<string, unknown> }> };
    list.items[0]!.metadata.deletionTimestamp = '2026-11-17T00:00:00Z';
    return { ...result, stdout: JSON.stringify(list) };
  };
  const adapter = createDefaultScaleAdapter(store, { azure, token, runCommand: runner });
  await assert.rejects(adapter.assertCustody(token), /deleting ArrowCol pod/);
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

async function fleetProviderFixture(t: TestContext, beforePut?: (name: string, count: number, etag: string) => void,
  collectorPool = 'benche64') {
  const values = { AZURE_SUBSCRIPTION_ID: campaignId }, stamp = structuredClone(defaultStampConfig(values));
  if (collectorPool !== 'benche64') stamp.aks.userPools.push({
    ...structuredClone(stamp.aks.userPools.find(p => p.name === 'benche64')!), name: collectorPool,
  });
  const azure = new Azure(new Config(values, stamp));
  const id = azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster;
  const events: string[] = [], pools = new Map<string, Record<string, any>>();
  for (const p of azure.c.stampConfig.aks.userPools) pools.set(p.name, {
    id: id + '/agentPools/' + p.name, name: p.name, type: 'Microsoft.ContainerService/managedClusters/agentPools', etag: '"v1"',
    properties: { mode: 'User', osType: 'Linux', osSKU: 'AzureLinux', vmSize: p.vmSize, count: 0,
      enableAutoScaling: true, minCount: 0, maxCount: p.maxCount, provisioningState: 'Succeeded',
      nodeLabels: { type: 'benchmark', 'arrow-collector/role': p.role, 'arrow-collector/sku': p.vmSize },
      nodeTaints: ['arrow-collector/role=' + p.role + ':NoSchedule'], enableFIPS: true,
      upgradeSettings: { maxSurge: '33%' }, securityProfile: { sshAccess: 'Disabled' } },
  });
  const system = azure.c.stampConfig.aks.systemPool;
  pools.set(system.name, {
    id: id + '/agentPools/' + system.name, name: system.name, type: 'Microsoft.ContainerService/managedClusters/agentPools', etag: '"v1"',
    properties: { mode: 'System', osType: 'Linux', vmSize: system.vmSize, count: system.initialCount,
      enableAutoScaling: true, minCount: system.minCount, maxCount: system.maxCount,
      provisioningState: 'Succeeded', nodeLabels: { type: 'system' } },
  });
  let response = 'accepted', puts = 0;
  let nodeItems: unknown[] = [];
  t.mock.method(azure, 'arm', async (method: string, resourceId: string) => {
    assert.equal(method, 'GET'); assert.equal(resourceId, id);
    return { id, type: 'Microsoft.ContainerService/managedClusters', tags: { ...TAGS, sandboxId: azure.c.sandbox },
      properties: { fqdn: 'fleet.azmk8s.io' } };
  });
  t.mock.method(azure, 'collection', async (resourceId: string) => {
    assert.equal(resourceId, id + '/agentPools');
    return structuredClone([...pools.values()]);
  });
  t.mock.method(azure, 'cli', async (...args: string[]) => {
    assert.deepEqual(args.slice(0, 2), ['vm', 'list-skus']);
    return [{ name: args[args.indexOf('--size') + 1], resourceType: 'virtualMachines',
      capabilities: [{ name: 'vCPUs', value: '16' }] }];
  });
  t.mock.method(azure, 'request', async (method: string, url: string, _resource: string, body?: string | Buffer | null,
    headers: Record<string, string> = {}, missingOk?: boolean, idempotent?: boolean, capture?: (r: HttpReceipt) => void) => {
    const name = new URL(url).pathname.split('/').at(-1)!;
    const resource = pools.get(name);
    assert.ok(resource, 'No request may address a pool outside fake ARM inventory.');
    events.push(method + ':' + name);
    if (method === 'GET') {
      if (response === 'readback-timeout' && puts > 0) throw new Error('ARM readback timed out');
      return [Buffer.from(JSON.stringify(resource)), { etag: resource.etag, ':status': '200' }];
    }
    assert.equal(method, 'PUT'); puts++;
    const parsed = JSON.parse(String(body));
    if (beforePut) beforePut(name, parsed.properties.count, headers['If-Match']!);
    else assert.equal(events.at(-2), 'assert');
    assert.equal(headers['If-Match'], resource.etag);
    assert.equal(idempotent, false); assert.equal(missingOk, false);
    if (response === 'timeout') throw new Error('transport timeout: private diagnostic');
    const status = response === 'conflict' || response === 'other409' ? 409 : response === 'quota' ? 400 : 202;
    const code = response === 'conflict' ? 'OperationNotAllowed' : response === 'quota' ? 'QuotaExceeded' : 'OtherConflict';
    if (status === 202) {
      resource.properties = parsed.properties;
      resource.properties.provisioningState = response === 'updating' ? 'Updating' :
        response === 'failed' ? 'Failed' : response === 'canceled' ? 'Canceled' : 'Succeeded';
      if (response === 'failed' || response === 'canceled') resource.properties.count = 0;
      resource.etag = '"v' + (puts + 1) + '"';
    }
    if (response === 'lost') throw new Error('connection lost after accepted write');
    capture?.({ status, bodyBase64: Buffer.from(JSON.stringify({ error: { code, message: 'private resource ID' } })).toString('base64'), headers: {} });
    if (status >= 400) throw new Error('HTTP private diagnostic');
    return [null, { ':status': String(status) }];
  });
  const binding = await observeFleetBinding(azure, collectorPool);
  const observations = new Map<string, FleetPoolObservation>(), hashes = new Map<string, string>();
  const pending = new Map<string, { count: number; state: string; etag: string }>();
  const authority: FleetIntentAuthority = {
    campaignId, ownerId, fencingToken: 1, binding, mode: 'producer',
    async assertCurrent() { events.push('assert'); },
    async establishBaseline(values) {
      events.push('baseline');
      for (const o of values) { hashes.set(o.name, o.configurationHash); observations.set(o.name, o); }
    },
    async recordObservation(o) {
      if (hashes.get(o.name) !== o.configurationHash) throw new Error('configuration hash drift');
      const d = pending.get(o.name);
      const terminal = ['Failed', 'Canceled'].includes(o.provisioningState);
      if (terminal) {
        if (d && (!['conflict', 'rejected'].includes(d.state) && o.etag === d.etag ||
            o.count !== d.count && o.count !== observations.get(o.name)?.count))
          throw new FleetUnsettledDispatchError();
        if (!d) assert.equal(o.count, observations.get(o.name)?.count);
      } else if (o.provisioningState !== 'Succeeded' ||
          d && !['conflict', 'rejected'].includes(d.state) && (d.count !== o.count || o.etag === d.etag))
        throw new FleetUnsettledDispatchError();
      events.push('observation:' + o.name); observations.set(o.name, o); pending.delete(o.name);
    },
    async beforeDispatch(name, count, etag) {
      assert.equal(etag, observations.get(name)?.etag); assert.equal(pending.has(name), false);
      if (['Failed', 'Canceled'].includes(observations.get(name)!.provisioningState)) assert.equal(count, 0);
      events.push('intent:' + name);
      pending.set(name, { count, state: 'intent', etag }); return 'dispatch';
    },
    async afterDispatch(name, _id, state) { events.push('outcome:' + state); pending.get(name)!.state = state; },
    async complete() {
      assert.equal(pending.size, 0);
      assert.ok([...observations.values()].every(o => o.count === 0 && o.provisioningState === 'Succeeded'));
    },
    async close() {},
  };
  const runCommand: CustodyCommandRunner = async (args, timeout, _env, _stdin, _signal, cap) => {
    assert.deepEqual(args.slice(0, 5), ['kubectl', '--server', binding.server, '--insecure-skip-tls-verify=false', '--request-timeout']);
    assert.ok(timeout! <= 20); assert.equal(cap, 4_194_304);
    if (args.includes('pods')) assert.ok(args.includes('--all-namespaces'));
    return { returncode: 0, stdout: JSON.stringify({ items: args.includes('nodes') ? nodeItems : [] }), stderr: '' };
  };
  const adapter = createFleetAdapter(azure, binding, authority, runCommand);
  return { azure, binding, authority, adapter, pools, events, observations, pending, runCommand,
    get puts() { return puts; }, setResponse(value: string) { response = value; }, setNodes(value: unknown[]) { nodeItems = value; } };
}

test('fleet provider binds observed vCPU inventory and persists intent before exact conditional single-shot scaling', async t => {
  const f = await fleetProviderFixture(t);
  assert.ok(f.binding.pools.every(p => p.vcpu === 16));
  await f.adapter.establishBaseline();
  const original = structuredClone(f.pools.get('benche64')!.properties);
  assert.deepEqual(await f.adapter.scalePool('benche64', 1, f.authority), { state: 'accepted' });
  const properties = f.pools.get('benche64')!.properties;
  assert.deepEqual(properties, { ...original, count: 1, minCount: 1, maxCount: 1 });
  assert.ok(f.events.indexOf('intent:benche64') < f.events.indexOf('PUT:benche64'));
  assert.ok(f.events.indexOf('PUT:benche64') < f.events.indexOf('outcome:accepted'));
  const resumed = createFleetAdapter(f.azure, f.binding, f.authority, f.runCommand);
  assert.deepEqual(await resumed.scalePool('benche64', 0, f.authority), { state: 'accepted' });
  assert.equal(f.pools.get('benche64')!.properties.maxCount, f.binding.pools.find(p => p.name === 'benche64')!.maxCount);
  assert.equal(f.pools.get('benche64')!.properties.minCount, 0);
  assert.equal(f.pools.get('benche64')!.properties.enableAutoScaling, true);
  await resumed.listPods(f.authority);
});

test('fleet binding discovery permits owned pending and failed pools for kill recovery without admitting a fresh baseline', async t => {
  for (const provisioningState of ['Updating', 'Failed']) {
    const f = await fleetProviderFixture(t), properties = f.pools.get('benche64')!.properties;
    Object.assign(properties, { provisioningState, count: 1, minCount: 1, maxCount: 1 });
    assert.deepEqual(await observeFleetBinding(f.azure, 'benche64'), f.binding);
    await assert.rejects(f.adapter.establishBaseline(), /baseline/);
    assert.equal(f.puts, 0);
  }
  const f = await fleetProviderFixture(t);
  t.mock.method(f.azure, 'cli', async (...args: string[]) => [{
    name: args[args.indexOf('--size') + 1], resourceType: 'virtualMachines', capabilities: [{ name: 'vCPUs', value: true }],
  }]);
  await assert.rejects(observeFleetBinding(f.azure, 'benche64'), /vCPU count is UNKNOWN/);
});

test('fleet provider never retries uncertain dispatch and reconciles a committed lost response before cleanup', async t => {
  const f = await fleetProviderFixture(t);
  await f.adapter.establishBaseline();
  f.setResponse('timeout');
  assert.deepEqual(await f.adapter.scalePool('benche64', 1, f.authority), { state: 'uncertain' });
  const resumed = createFleetAdapter(f.azure, f.binding, f.authority, f.runCommand);
  await assert.rejects(resumed.scalePool('benche64', 1, f.authority), FleetUnsettledDispatchError);
  assert.equal(f.puts, 1);
  const g = await fleetProviderFixture(t);
  await g.adapter.establishBaseline();
  g.setResponse('lost');
  assert.deepEqual(await g.adapter.scalePool('benche64', 1, g.authority), { state: 'uncertain' });
  assert.equal((await g.adapter.waitForPool('benche64', 1, Date.now() + 100, g.authority)).count, 1);
  g.setResponse('accepted');
  await createFleetAdapter(g.azure, g.binding, g.authority, g.runCommand).scalePool('benche64', 0, g.authority);
  assert.equal(g.puts, 2);
  const h = await fleetProviderFixture(t);
  await h.adapter.establishBaseline(); h.setResponse('readback-timeout');
  assert.deepEqual(await h.adapter.scalePool('benche64', 1, h.authority), { state: 'uncertain' });
  assert.equal(h.puts, 1);
  h.setResponse('accepted');
  assert.equal((await h.adapter.waitForPool('benche64', 1, Date.now() + 100, h.authority)).count, 1);
});

test('fleet provider surfaces owned terminal failures and repairs Failed/Canceled zero-count pools without claiming idle', async t => {
  for (const response of ['failed', 'canceled']) {
    const f = await fleetProviderFixture(t);
    await f.adapter.establishBaseline(); f.setResponse(response);
    assert.deepEqual(await f.adapter.scalePool('benche64', 1, f.authority), { state: 'accepted' });
    const terminal = await f.adapter.waitForPool('benche64', 1, Date.now() + 100, f.authority);
    assert.equal(terminal.count, 0);
    assert.equal(terminal.provisioningState, response === 'failed' ? 'Failed' : 'Canceled');
    await assert.rejects(f.authority.complete());
    const resumed = createFleetAdapter(f.azure, f.binding, f.authority, f.runCommand);
    await assert.rejects(resumed.scalePool('benche64', 1, f.authority), /only an owned repair to zero/);
    const properties = f.pools.get('benche64')!.properties, approvedMaximum = f.binding.pools.find(p => p.name === 'benche64')!.maxCount;
    properties.maxCount = approvedMaximum + 1;
    await assert.rejects(resumed.listUserPools(f.authority), /autoscaler bounds/);
    await assert.rejects(resumed.scalePool('benche64', 0, f.authority), /autoscaler bounds/);
    properties.minCount = 0; properties.maxCount = approvedMaximum;
    assert.ok((await resumed.listUserPools(f.authority)).includes('benche64'));
    properties.minCount = 1; properties.maxCount = 1;
    f.setResponse('accepted');
    assert.deepEqual(await resumed.scalePool('benche64', 0, f.authority), { state: 'accepted' });
    const repaired = await resumed.waitForPool('benche64', 0, Date.now() + 100, f.authority);
    assert.equal(repaired.provisioningState, 'Succeeded'); assert.equal(repaired.count, 0);
    const restored = f.pools.get('benche64')!.properties;
    assert.equal(restored.enableAutoScaling, true); assert.equal(restored.minCount, 0);
    assert.equal(restored.maxCount, approvedMaximum);
    assert.equal(f.puts, 2);
    await f.authority.complete();
  }
});

test('fleet provider only classifies exact ARM OperationNotAllowed 409 and sanitizes definite rejections', async t => {
  for (const [response, expected] of [
    ['conflict', { state: 'conflict', status: 409, code: 'OperationNotAllowed' }],
    ['quota', { state: 'rejected', reason: 'quota_exceeded' }],
    ['other409', { state: 'rejected', reason: 'request_rejected' }],
  ] as const) {
    const f = await fleetProviderFixture(t);
    await f.adapter.establishBaseline(); f.setResponse(response);
    assert.deepEqual(await f.adapter.scalePool('benche64', 1, f.authority), expected);
    assert.equal(f.puts, 1);
  }
});

test('fleet provider refuses forbidden pool, ETag, writable drift and unknown writable settings before dispatch', async t => {
  for (const fault of ['system', 'etag', 'drift', 'unknown', 'role', 'sku', 'taint', 'bounds']) {
    const f = await fleetProviderFixture(t);
    await f.adapter.establishBaseline();
    const resource = f.pools.get('benche64')!;
    if (fault === 'etag') resource.etag = '*';
    if (fault === 'drift') resource.properties.enableFIPS = false;
    if (fault === 'unknown') resource.properties.futureWritableSetting = true;
    if (fault === 'role') resource.properties.nodeLabels['arrow-collector/role'] = 'client';
    if (fault === 'sku') resource.properties.nodeLabels['arrow-collector/sku'] = 'Standard_Fake';
    if (fault === 'taint') resource.properties.nodeTaints = [];
    if (fault === 'bounds') resource.properties.minCount = 1;
    await assert.rejects(f.adapter.scalePool(fault === 'system' ? 'systempool' : 'benche64', 1, f.authority));
    assert.equal(f.puts, 0);
  }
});

test('fleet provider fences after durable intent and preserves only the reviewed legacy pool label fallbacks', async t => {
  const f = await fleetProviderFixture(t);
  delete f.pools.get('benche64')!.properties.nodeLabels['arrow-collector/sku'];
  delete f.pools.get('benchd64')!.properties.nodeLabels['arrow-collector/sku'];
  await f.adapter.establishBaseline();
  t.mock.method(f.authority, 'assertCurrent', async () => {
    f.events.push('assert');
    if (f.events.includes('intent:benche64')) throw new Error('fenced');
  });
  await assert.rejects(f.adapter.scalePool('benche64', 1, f.authority), /fenced/);
  assert.equal(f.puts, 0);
  const g = await fleetProviderFixture(t);
  const configured = g.azure.c.stampConfig.aks.userPools.find(p => p.name === 'benchd64')!;
  const resource = g.pools.get('benchd64')!;
  try {
    configured.name = 'p123456789ab';
    resource.name = configured.name; resource.id = g.binding.clusterId + '/agentPools/' + configured.name;
    delete resource.properties.nodeLabels['arrow-collector/sku'];
    g.pools.delete('benchd64'); g.pools.set(configured.name, resource);
    await assert.rejects(observeFleetBinding(g.azure, 'benche64'), /identity|SKU/);
  } finally { configured.name = 'benchd64'; }
});

test('fleet provider requires full stamp membership and preserved zero User policy without adopting unselected pools', async t => {
  for (const fault of ['bounds', 'nodes', 'unselected', 'undeclared-zero', 'missing', 'system', 'missing-legacy',
    'missing-system', 'legacy-disabled', 'legacy-min', 'legacy-max', 'legacy-failed', 'legacy-nonzero', 'legacy-nodes']) {
    const f = await fleetProviderFixture(t, undefined, 'hugepool');
    if (fault === 'bounds') f.pools.get('benche64')!.properties.maxCount++;
    if (fault === 'nodes') f.setNodes([{ metadata: { labels: { agentpool: 'benche64' } } }]);
    if (fault === 'unselected') {
      const extra = structuredClone(f.pools.get('benchd64')!);
      extra.name = 'unselected'; extra.id = f.binding.clusterId + '/agentPools/unselected'; extra.properties.count = 1;
      f.pools.set('unselected', extra);
    }
    if (fault === 'undeclared-zero') {
      const extra = structuredClone(f.pools.get('benchd64')!);
      extra.name = 'unapproved'; extra.id = f.binding.clusterId + '/agentPools/unapproved';
      f.pools.set('unapproved', extra);
    }
    if (fault === 'missing') f.pools.delete('benchd64');
    if (fault === 'system') f.pools.get('benchd64')!.properties.mode = 'System';
    if (fault === 'missing-legacy') f.pools.delete('benche64');
    if (fault === 'missing-system') f.pools.delete(f.azure.c.stampConfig.aks.systemPool.name);
    if (fault === 'legacy-disabled') f.pools.get('benche64')!.properties.enableAutoScaling = false;
    if (fault === 'legacy-min') f.pools.get('benche64')!.properties.minCount = 1;
    if (fault === 'legacy-max') f.pools.get('benche64')!.properties.maxCount++;
    if (fault === 'legacy-failed') f.pools.get('benche64')!.properties.provisioningState = 'Failed';
    if (fault === 'legacy-nonzero') f.pools.get('benche64')!.properties.count = 1;
    if (fault === 'legacy-nodes') f.setNodes([{ metadata: { labels: { agentpool: 'benche64' } } }]);
    await assert.rejects(f.adapter.establishBaseline());
    assert.ok(!f.events.includes('baseline'));
    assert.equal(f.puts, 0);
  }
  const f = await fleetProviderFixture(t, undefined, 'hugepool');
  await f.adapter.establishBaseline();
  assert.deepEqual(await f.adapter.listUserPools(f.authority), f.binding.pools.map(p => p.name));
  assert.ok(!f.binding.pools.some(p => p.name === 'benche64'));
  await assert.rejects(f.adapter.scalePool('benche64', 1, f.authority), /not an approved User pool/);
  assert.equal(f.puts, 0);
});

test('fleet cleanup rechecks full stamp membership after selected pools reach zero', async t => {
  for (const fault of ['missing-legacy', 'extra-zero', 'legacy-disabled', 'selected-nonzero', 'legacy-nodes']) {
    const f = await fleetProviderFixture(t, undefined, 'hugepool');
    await f.adapter.establishBaseline();
    await f.adapter.scalePool('hugepool', 1, f.authority);
    let nodeReads = 0;
    const listNodes = f.adapter.listNodes;
    t.mock.method(f.adapter, 'listNodes', async (authority: CleanupMutationAuthority) => {
      const nodes = await listNodes(authority);
      if (++nodeReads === f.binding.pools.length) {
        if (fault === 'missing-legacy') f.pools.delete('benche64');
        if (fault === 'legacy-disabled') f.pools.get('benche64')!.properties.enableAutoScaling = false;
        if (fault === 'selected-nonzero') Object.assign(f.pools.get('hugepool')!.properties, { count: 1, minCount: 1, maxCount: 1 });
        if (fault === 'legacy-nodes') f.setNodes([{ metadata: { labels: { agentpool: 'benche64' } } }]);
        if (fault === 'extra-zero') {
          const extra = structuredClone(f.pools.get('benchd64')!);
          extra.name = 'unapproved'; extra.id = f.binding.clusterId + '/agentPools/unapproved';
          f.pools.set(extra.name, extra);
        }
      }
      return nodes;
    });
    const result = await cleanupAksFleet(f.adapter, f.authority,
      { deadlineAtMs: Date.now() + 1000, includePoolNames: f.binding.pools.map(p => p.name) });
    assert.equal(result.verified, false);
    assert.ok(result.failed.includes('final-user-pool-discovery'));
    if (fault !== 'selected-nonzero') assert.ok(f.binding.pools.every(p => f.pools.get(p.name)!.properties.count === 0));
    assert.equal(f.puts, 2);
    assert.ok(!f.events.includes('PUT:benche64'));
  }
});

test('fleet provider polls only exact GETs within the original deadline and does not swallow authority/config drift', async t => {
  const f = await fleetProviderFixture(t);
  await f.adapter.establishBaseline();
  f.pools.get('benche64')!.properties.provisioningState = 'Updating';
  await assert.rejects(f.adapter.waitForPool('benche64', 1, Date.now() + 10, f.authority), /original deadline/);
  assert.equal(f.puts, 0);
  f.pools.get('benche64')!.properties.provisioningState = 'Succeeded';
  f.pools.get('benche64')!.properties.securityProfile = { sshAccess: 'Enabled' };
  await assert.rejects(f.adapter.waitForPool('benche64', 0, Date.now() + 100, f.authority), /hash drift/);
});

test('fleet provider exposes pending snapshots to lifecycle polling but never treats them as mutation authority', async t => {
  const f = await fleetProviderFixture(t);
  await f.adapter.establishBaseline(); f.setResponse('updating');
  assert.deepEqual(await f.adapter.scalePool('benche64', 1, f.authority), { state: 'accepted' });
  assert.equal((await f.adapter.readPool('benche64', f.authority)).provisioningState, 'Updating');
  await assert.rejects(f.adapter.scalePool('benche64', 0, f.authority), FleetUnsettledDispatchError);
  assert.equal(f.puts, 1);
  f.pools.get('benche64')!.properties.enableFIPS = false;
  await assert.rejects(f.adapter.waitForPool('benche64', 1, Date.now() + 100, f.authority), /hash drift/);
});

test('fleet Kubernetes observations reject command failures and oversized output instead of treating errors as absence', async t => {
  const f = await fleetProviderFixture(t);
  for (const result of [{ returncode: 1, stdout: '', stderr: 'private permission diagnostic' },
    { returncode: 0, stdout: '{"items":[]}', stderr: 'x'.repeat(4_194_305) }]) {
    const adapter = createFleetAdapter(f.azure, f.binding, f.authority, async () => result);
    await assert.rejects(adapter.listPods(f.authority), /Kubernetes observation failed/);
  }
});

test('fleet image observations require exact stamp digest and explicit Linux manifest architecture', async t => {
  const f = await fleetProviderFixture(t);
  const digest = 'sha256:' + 'a'.repeat(64), image = f.azure.c.registryLoginServer + '/arrowcol@' + digest;
  let metadata: unknown = { digest, os: 'linux', architecture: 'arm64' }, observations = 0;
  t.mock.method(f.azure, 'cli', async (...args: string[]) => {
    assert.deepEqual(args.slice(0, 3), ['acr', 'manifest', 'show-metadata']);
    assert.equal(args[args.indexOf('--name') + 1], 'arrowcol@' + digest);
    observations++;
    return metadata;
  });
  assert.deepEqual(await observeFleetImageArchitectures(f.azure, [image]), [{ image, architectures: ['arm64'] }]);
  observations = 0;
  assert.deepEqual(await observeFleetImageArchitectures(f.azure, [image, image]), [{ image, architectures: ['arm64'] }]);
  assert.equal(observations, 1);
  metadata = { digest, manifests: [{ platform: { os: 'linux', architecture: 'amd64' } }, { platform: { os: 'linux', architecture: 'arm64' } }] };
  assert.deepEqual((await observeFleetImageArchitectures(f.azure, [image]))[0]!.architectures, ['amd64', 'arm64']);
  for (const bad of [{ digest: 'sha256:' + 'b'.repeat(64), os: 'linux', architecture: 'amd64' },
    { digest }, { digest, os: 'windows', architecture: 'amd64' }, { digest, os: 'linux', architecture: 'UNKNOWN' }]) {
    metadata = bad;
    await assert.rejects(observeFleetImageArchitectures(f.azure, [image]));
  }
  await assert.rejects(observeFleetImageArchitectures(f.azure, ['other.azurecr.io/arrowcol@' + digest]));
  await assert.rejects(observeFleetImageArchitectures(f.azure, [f.azure.c.registryLoginServer + '/arrowcol:latest']));
});

test('real fleet custody and provider scale, resume with blob-scoped fencing, restore every pool and complete', async t => {
  const journalPath = 'oat-fleet/active.v1.json';
  const blobs = new Map<string, { bytes: Buffer; etag: string }>();
  const f = await fleetProviderFixture(t, (name, count, etag) => {
    const journal = JSON.parse(blobs.get(journalPath)!.bytes.toString());
    assert.equal(journal.status, 'active');
    assert.equal(journal.pools[name].dispatch.state, 'intent');
    assert.equal(journal.pools[name].dispatch.count, count);
    assert.equal(journal.pools[name].dispatch.etag, etag);
  });
  let revision = 0, acquisitions = 0;
  let lease: { id: string; seconds: number; expiresAt: number } | undefined;
  t.mock.method(f.azure, 'lease', async () => { throw new Error('Container leases would block native publisher control.'); });
  t.mock.method(f.azure, 'blob', async (method: string, container: string, name = '', query?: Record<string, string>,
    body?: string | Buffer | null, headers: Record<string, string> = {}, missingOk = false) => {
    assert.equal(container, f.azure.c.checkpoints);
    if (lease && Date.now() >= lease.expiresAt) lease = undefined;
    const old = blobs.get(name);
    if (query?.comp === 'lease') {
      assert.equal(method, 'PUT'); assert.equal(name, journalPath); assert.ok(old);
      if (headers['x-ms-lease-action'] === 'acquire') {
        assert.equal(lease, undefined); assert.equal(headers['if-match'], old.etag);
        const seconds = Number(headers['x-ms-lease-duration']), id = headers['x-ms-proposed-lease-id'];
        assert.ok(seconds >= 15 && seconds <= 60); assert.match(id!, /^[a-f0-9-]{36}$/);
        lease = { id: id!, seconds, expiresAt: Date.now() + seconds * 1000 }; acquisitions++;
      } else {
        assert.ok(lease); assert.equal(headers['x-ms-lease-id'], lease.id);
        if (headers['x-ms-lease-action'] === 'release') lease = undefined;
        else {
          assert.equal(headers['x-ms-lease-action'], 'renew');
          lease.expiresAt = Date.now() + lease.seconds * 1000;
        }
      }
      return [null, { ':status': '200' }];
    }
    if (method === 'GET') {
      if (!old && !missingOk) throw new HttpFailure(404, 'missing fake blob');
      return [old?.bytes ?? null, old ? { etag: old.etag, ':status': '200' } : {}];
    }
    if (headers['if-match'] !== undefined && headers['if-match'] !== old?.etag ||
        headers['if-none-match'] === '*' && old) throw new HttpFailure(412, 'fake ETag conflict');
    assert.ok(headers['if-match'] || headers['if-none-match'] === '*', 'All durable writes must be conditional.');
    if (name === journalPath && old) {
      assert.ok(lease); assert.equal(headers['x-ms-lease-id'], lease.id);
    } else assert.equal(headers['x-ms-lease-id'], undefined);
    if (method === 'DELETE') {
      assert.equal(name, 'active.json'); blobs.delete(name);
      return [null, { ':status': '202' }];
    }
    assert.equal(method, 'PUT');
    const etag = `"blob-${++revision}"`;
    blobs.set(name, { bytes: Buffer.from(body!), etag });
    return [null, { etag, ':status': '201' }];
  });
  for (const resource of f.pools.values()) delete resource.properties.nodeLabels['arrow-collector/sku'];
  const binding = await observeFleetBinding(f.azure, 'benche64');
  const authorities: FleetIntentAuthority[] = [];
  t.after(async () => { for (const authority of authorities) await authority.close(); });
  const first = await acquireFleetAuthority(f.azure, binding, { ownerId, resume: false });
  authorities.push(first);
  const adapter = createFleetAdapter(f.azure, binding, first, f.runCommand);
  await adapter.establishBaseline();
  for (const pool of binding.pools) {
    assert.deepEqual(await adapter.scalePool(pool.name, pool.count, first), { state: 'accepted' });
    assert.equal((await adapter.waitForPool(pool.name, pool.count, Date.now() + 1000, first)).provisioningState, 'Succeeded');
    assert.equal((await adapter.readPool(pool.name, first)).count, pool.count);
  }
  await first.close();
  assert.equal(lease, undefined); assert.ok(blobs.has('active.json'));
  const resumedBinding = await observeFleetBinding(f.azure, 'benche64');
  const journalBefore = blobs.get(journalPath), campaignBefore = blobs.get('active.json');
  await assert.rejects(acquireFleetAuthority(f.azure, resumedBinding, { ownerId: cellId, resume: true }), /retained owner/);
  assert.equal(blobs.get(journalPath), journalBefore); assert.equal(blobs.get('active.json'), campaignBefore);
  assert.equal(f.puts, binding.pools.length);
  const resumed = await acquireFleetAuthority(f.azure, resumedBinding, { ownerId, resume: true });
  authorities.push(resumed);
  assert.ok(resumed.fencingToken > first.fencingToken);
  await assert.rejects(first.assertCurrent(), /closed|fenced/);
  const recoveredAdapter = createFleetAdapter(f.azure, resumedBinding, resumed, f.runCommand);
  for (const pool of resumedBinding.pools) {
    assert.deepEqual(await recoveredAdapter.scalePool(pool.name, 0, resumed), { state: 'accepted' });
    const observed = await recoveredAdapter.waitForPool(pool.name, 0, Date.now() + 1000, resumed);
    assert.equal(observed.count, 0); assert.equal(observed.provisioningState, 'Succeeded');
    const properties = f.pools.get(pool.name)!.properties;
    assert.equal(properties.enableAutoScaling, true);
    assert.equal(properties.minCount, 0); assert.equal(properties.maxCount, pool.maxCount);
  }
  assert.deepEqual(await recoveredAdapter.listNodes(resumed), { items: [] });
  await resumed.complete(); await resumed.close();
  assert.equal(blobs.has('active.json'), false);
  assert.equal(JSON.parse(blobs.get(journalPath)!.bytes.toString()).status, 'complete');
  assert.equal(lease, undefined); assert.equal(acquisitions, 2);
  assert.equal(f.puts, binding.pools.length * 2);
});
