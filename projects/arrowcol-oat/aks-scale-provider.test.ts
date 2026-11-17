import assert from 'node:assert/strict';
import { test, type TestContext } from 'node:test';
import { Azure, Config } from '../../infra/azure/bootstrap.js';
import { CampaignConflictError, CUSTODY_CLEANUP_STEPS, armCampaign, publishRunInventory, startCampaign, validateCampaignState,
  type CampaignState, type CampaignStore, type RunInventory, type StoredCampaign, type ScaleTrialReceipt } from './aks-campaign.js';
import { createDefaultScaleAdapter } from './aks-scale-provider.js';
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
    fic: { resource_id: `${identityId}/federatedIdentityCredentials/${ficName}`, name: ficName, state: 'planned', absence: null },
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

const poolStep: ScaleStep = { dimension: 'collector-node-max', before: 1, after: 2, evidence: ['collector-placement-blocked'],
  restore: { dimension: 'collector-node-max', before: 2, after: 1 }, baselineCommittedRowsPerSecond: 1000,
  baselineQapPendingBacklog: 0, baselineQapP95LatencyMs: 100 };
const clientStep: ScaleStep = { dimension: 'loadgen-max', before: 2, after: 3, evidence: ['generator-cpu-saturated'],
  restore: { dimension: 'loadgen-max', before: 3, after: 2 }, baselineCommittedRowsPerSecond: 1000,
  baselineQapPendingBacklog: 0, baselineQapP95LatencyMs: 100 };

test('real scale adapter applies and reconciles only the inventory-bound stamp pool while load is authorized', async t => {
  const { store, azure } = await fixture();
  let maxCount = 1;
  const calls: Array<{ method: string; resourceId: string; body?: unknown }> = [];
  const aksId = `${azure.c.base}/providers/Microsoft.ContainerService/managedClusters/${azure.c.cluster}`;
  const poolId = `${aksId}/agentPools/benche64`;
  t.mock.method(azure, 'arm', async (method: string, resourceId: string, body?: unknown) => {
    calls.push({ method, resourceId, body });
    if (method === 'GET') return { id: poolId, name: 'benche64', properties: { count: 0, vmSize: 'Standard_D64ds_v6',
      mode: 'User', provisioningState: 'Succeeded', enableAutoScaling: true, minCount: 0, maxCount } };
    assert.equal(method, 'PUT');
    assert.equal(resourceId, poolId);
    if (!body || typeof body !== 'object' || Array.isArray(body)) throw new Error('Scale pool payload is UNKNOWN.');
    const properties = (body as Record<string, unknown>).properties;
    if (!properties || typeof properties !== 'object' || Array.isArray(properties)) throw new Error('Scale pool properties are UNKNOWN.');
    maxCount = Number((properties as Record<string, unknown>).maxCount);
    return {};
  });
  const target: ScaleTarget = { dimension: poolStep.dimension, before: poolStep.before, after: poolStep.after };
  const adapter = createDefaultScaleAdapter(store, { azure, token });
  await adapter.assertCustody(token);
  await adapter.persistIntent(target, token, poolStep);
  await adapter.apply(target, token);
  assert.equal(await adapter.reconcile(target), 'after');
  assert.equal(maxCount, 2);
  assert.deepEqual(calls.filter(call => call.method !== 'GET').map(call => [call.method, call.resourceId]), [['PUT', poolId]]);
  assert.equal(store.current!.state.scaleTrials?.[0]?.resource.resource_id, poolId);
});

test('real scale adapter refuses a Kubernetes patch without resourceVersion before forwarding', async () => {
  const { store, azure, namespace } = await fixture();
  const current = store.current!.state;
  const inventory = current.runInventory!;
  const client = inventory.resources.find(resource => resource.role === 'client')!;
  const plan = { version: 1 as const, runId: cellId, namespace, serviceAccount: 'oat',
    identityId: `${azure.c.base}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/${azure.c.identity}`,
    resourceId: inventory.fic!.resource_id!, clientId: ownerId, tenantId: campaignId,
    aksId: `${azure.c.base}/providers/Microsoft.ContainerService/managedClusters/${azure.c.cluster}`,
    server: 'https://fixture.azmk8s.io', credentialName: inventory.fic!.name!,
    issuer: 'https://fixture.oic.prod-aks.azure.com/tenant/cluster/',
    subject: `system:serviceaccount:${namespace}:oat`, audience: 'api://AzureADTokenExchange' };
  inventory.fic = { ...inventory.fic!, plan };
  const now = Date.now();
  const target: ScaleTarget = { dimension: clientStep.dimension, before: clientStep.before, after: clientStep.after };
  const receipt: ScaleTrialReceipt = { ...newScaleTrialReceipt({ state: current, step: clientStep, now, resource: {
    transport: 'kubernetes', resource_id: `apps/v1/Deployment/${namespace}/${client.name}/${client.uid}`,
    api_version: 'apps/v1', kind: 'Deployment', namespace, name: client.name, uid: client.uid } }), phase: 'applied', fencing_token: 2 };
  const terminalIndex = CUSTODY_CLEANUP_STEPS.indexOf('record-terminal-state');
  const cleanupState: CampaignState = { ...current, status: 'cleaning', fencingToken: 2, fenceReason: 'heartbeat-expired',
    fencedAtMs: now, cleanupStatus: 'running',
    cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({ step, idempotencyKey: `${campaignId}:2:${step}`,
      status: index < terminalIndex ? 'complete' : index === terminalIndex ? 'running' : 'pending' })),
    runInventory: inventory, scaleTrials: [receipt] };
  store.current!.state = cleanupState;
  const authority: CleanupMutationAuthority = { mode: 'controller', campaignId, ownerId, fencingToken: 2, assertCurrent: async () => {} };
  const deployment = { apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name: client.name, namespace, uid: client.uid, generation: 1 },
    spec: { replicas: 3 }, status: { observedGeneration: 1, availableReplicas: 3 } };
  let patchCalls = 0;
  const runCommand: CustodyCommandRunner = async args => {
    if (args.includes('patch')) { patchCalls++; return { returncode: 0, stdout: '', stderr: '' }; }
    return { returncode: 0, stdout: JSON.stringify(deployment), stderr: '' };
  };
  const cleanupToken = { ownerId, fencingToken: 2 };
  await persistScaleTrial(store, { ...receipt, phase: 'restore-intent' }, cleanupToken);
  assert.equal(store.current!.state.scaleTrials?.[0]?.phase, 'restore-intent');
  const adapter = createDefaultScaleAdapter(store, { azure, token: cleanupToken, authority, runCommand });
  await assert.rejects(adapter.apply(receipt.restore, cleanupToken), /resourceVersion is UNKNOWN/);
  assert.equal(patchCalls, 0);
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
