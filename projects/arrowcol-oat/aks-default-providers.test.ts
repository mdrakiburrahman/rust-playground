import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test, type TestContext } from 'node:test';
import { Azure, Config, TAGS, type HttpReceipt } from '../../infra/azure/bootstrap.js';
import { CUSTODY_CLEANUP_STEPS, newCampaignState, publishRunInventory, type CampaignState, type CampaignStore,
  type RunInventory, type StoredCampaign } from './aks-campaign.js';
import { captureCleanupBaseline, createDefaultCleanupProviders, type StampHealthReceipt } from './aks-default-providers.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';

const campaignId = '11111111-1111-4111-8111-111111111111';
const ownerId = '22222222-2222-4222-8222-222222222222';
const cellId = '33333333-3333-4333-8333-333333333333';
class Store implements CampaignStore {
  revision = 1;
  afterWrite?: () => void;
  constructor(public state: CampaignState) {}
  async read(): Promise<StoredCampaign> { return { state: structuredClone(this.state), etag: `"${this.revision}"` }; }
  async write(state: CampaignState, etag?: string): Promise<string> {
    assert.equal(etag, `"${this.revision}"`);
    this.state = structuredClone(state);
    this.revision++;
    this.afterWrite?.();
    return `"${this.revision}"`;
  }
}
async function fixture(t: TestContext, capture = true) {
  const c = new Config({ AZURE_SUBSCRIPTION_ID: campaignId }), azure = new Azure(c);
  const aksId = c.base + '/providers/Microsoft.ContainerService/managedClusters/' + c.cluster;
  const namespace = 'oat-' + cellId.replaceAll('-', ''), identityId = c.identity_id;
  const step = { state: 'not_started' as const, deadline_unix_ms: null, reason_code: null };
  const plan = { version: 1 as const, runId: cellId, namespace, serviceAccount: 'oat', identityId,
    resourceId: identityId + '/federatedIdentityCredentials/arrowcol-run-' + cellId,
    clientId: campaignId, tenantId: campaignId, aksId, server: 'https://fixture.azmk8s.io',
    credentialName: 'arrowcol-run-' + cellId, issuer: 'https://fixture.oic.prod-aks.azure.com/tenant/cluster/',
    subject: `system:serviceaccount:${namespace}:oat`, audience: 'api://AzureADTokenExchange' };
  const inventory: RunInventory = { schema_version: 1, campaign_id: campaignId, owner_id: ownerId, fencing_token: 1,
    orchestration_run_id: campaignId, cell_run_id: cellId, namespace: { name: namespace, uid: cellId, owner_token: ownerId },
    resources: [], fic: { resource_id: plan.resourceId, name: plan.credentialName, state: 'planned', absence: null, plan },
    qap: null, evidence_destination: { account: c.account, container: c.checkpoints, blob_prefix: 'evidence/' + cellId },
    cleanup: { stop_admission: step, bounded_drain: step, evidence_capture: step, terminal_capture: step, handoff: step } };
  const store = new Store({ ...newCampaignState(campaignId, ownerId, Date.now() - 10_000), heartbeatAtMs: Date.now(), runInventory: inventory });
  const resources = new Map<string, Record<string, unknown>>([
    [aksId, { id: aksId, type: 'Microsoft.ContainerService/managedClusters', tags: { ...TAGS, sandboxId: c.sandbox },
      properties: { provisioningState: 'Succeeded', agentPoolProfiles: ['systempool', 'benchd64', 'benche64'].map(name => ({ name })) } }],
    [c.cluster_id, { id: c.cluster_id, type: 'Microsoft.Kusto/clusters', tags: { ...TAGS, sandboxId: c.sandbox },
      sku: { name: 'Standard_E4ads_v5', capacity: 2 }, properties: { provisioningState: 'Succeeded', state: 'Running',
        optimizedAutoscale: { isEnabled: true, minimum: 2, maximum: 4 }, enableAutoStop: true } }],
  ]);
  for (const name of ['systempool', 'benchd64', 'benche64']) {
    const id = aksId + '/agentPools/' + name;
    resources.set(id, { id, type: 'Microsoft.ContainerService/managedClusters/agentPools',
      properties: { count: name === 'systempool' ? 3 : 0, vmSize: name === 'systempool' ? 'Standard_D8ds_v6' : 'Standard_D64ds_v6',
        mode: name === 'systempool' ? 'System' : 'User', provisioningState: 'Succeeded', enableAutoScaling: name !== 'systempool',
        ...(name === 'systempool' ? {} : { minCount: 0, maxCount: 1 }) } });
  }
  const blobs = new Map<string, { bytes: Buffer; etag: string }>();
  const mutations: Array<{ method: string; resourceId: string; body: Record<string, unknown> }> = [];
  let fault: 'none' | 'before' | 'after' = 'none';
  t.mock.method(azure, 'request', async (method: string, url: string, _resource: string, body?: string | Buffer | null,
    _headers?: unknown, _missing?: boolean, _idempotent?: boolean, receipt?: (value: HttpReceipt) => void) => {
    const id = new URL(url).pathname, current = resources.get(id);
    assert.ok(current, 'all ARM work stays on the exact selected stamp resources');
    if (method !== 'GET') {
      assert.equal(store.state.operations.at(-1)?.resourceId, id, 'durable intent precedes ARM');
      const payload = JSON.parse(String(body)) as Record<string, unknown>;
      mutations.push({ method, resourceId: id, body: payload });
      if (fault === 'before') throw new Error('injected unknown request outcome before apply');
      if (payload.sku) current.sku = payload.sku;
      current.properties = { ...(current.properties as object), ...(payload.properties as object) };
      if (fault === 'after') throw new Error('injected lost response after apply');
    }
    const bytes = Buffer.from(JSON.stringify(current)), headers = { 'x-ms-request-id': 'read-provider-1' };
    receipt?.({ status: 200, headers, bodyBase64: bytes.toString('base64') });
    return [bytes, { ':status': '200', ...headers }];
  });
  t.mock.method(azure, 'blob', async (method: string, container: string, name: string, _query?: unknown,
    body?: string | Buffer | null, headers?: Record<string, string>) => {
    assert.equal(container, c.checkpoints);
    const previous = blobs.get(name);
    if (method === 'GET') return [previous?.bytes ?? null, previous ? { etag: previous.etag } : {}];
    assert.equal(method, 'PUT');
    assert.equal(headers?.['If-None-Match'], '*');
    assert.equal(previous, undefined);
    const bytes = Buffer.from(String(body)), etag = `"blob-${blobs.size + 1}"`;
    blobs.set(name, { bytes, etag });
    return [null, { ':status': '201', etag }];
  });
  if (capture) await captureCleanupBaseline(store, azure, ownerId, 1);
  store.state = { ...store.state, status: 'cleaning', mode: 'armed', fencingToken: 2, fenceReason: 'heartbeat-expired',
    fencedAtMs: Date.now(), cleanupStatus: 'running', cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({
      step, idempotencyKey: `${campaignId}:2:${step}`, status: index < 8 ? 'complete' : index === 8 ? 'running' : 'pending',
    })) };
  const authority: CleanupMutationAuthority = { campaignId, ownerId, fencingToken: 2, assertCurrent: async () => {
    if (store.state.status !== 'cleaning' || store.state.fencingToken !== 2) throw new Error('stale cleanup actor');
  } };
  const workDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'custody-provider-'));
  t.after(() => fs.rmSync(workDirectory, { recursive: true, force: true }));
  const complete = async () => 'complete' as const;
  const lifecycle = { stopAdmission: complete, boundedDrain: complete, preserveEvidence: complete,
    stopMaintenanceIfUidAndOwnerMatch: complete, captureTerminalState: complete };
  let health: () => StampHealthReceipt = () => ({ observed_at_unix_ms: Date.now(), pods: [{ name: 'stamp/pod', ready: true, scheduled: true }],
    pdb_blocked: true, versions: [{ namespace: c.kubernetesNamespace, kind: 'pods', resource_version: '1' }] });
  const options = { azure, workDirectory, lifecycle, readHealth: async () => health() };
  const providers = createDefaultCleanupProviders(store, options);
  return { c, azure, store, resources, blobs, mutations, inventory, aksId, authority, providers, options,
    fault: (value: typeof fault) => { fault = value; }, health: (value: typeof health) => { health = value; } };
}
const properties = (value: Record<string, unknown>) => value.properties as Record<string, unknown>;

test('default restoration is a true no-op at supported safe capacity and capacity alone cannot complete full safety', async t => {
  const f = await fixture(t);
  assert.equal(await f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority), 'complete');
  assert.equal(await f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority), 'complete');
  assert.deepEqual(f.mutations, []);
  await assert.rejects(f.providers.verifySafeIdle(f.store.state, 'verify'), /Full stamp safety is UNKNOWN/);
});

test('default restoration changes only one dimension at a time and reconciles exact ARM targets before continuing', async t => {
  const f = await fixture(t);
  f.health(() => ({ observed_at_unix_ms: Date.now(), pods: [{ name: 'stamp/pod', ready: true, scheduled: true }], pdb_blocked: false, versions: [] }));
  for (const name of ['benchd64', 'benche64']) Object.assign(properties(f.resources.get(f.aksId + '/agentPools/' + name)!), { count: 1, maxCount: 2 });
  const kusto = f.resources.get(f.c.cluster_id)!;
  kusto.sku = { name: 'Standard_E8ads_v5', capacity: 4 };
  let iterations = 0, previousMutations = 0;
  while (await f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority) !== 'complete') {
    assert.equal(f.mutations.length - previousMutations, 1);
    previousMutations = f.mutations.length;
    assert.ok(++iterations < 8);
  }
  assert.deepEqual(f.mutations.map(item => item.resourceId), [
    f.aksId + '/agentPools/benchd64', f.aksId + '/agentPools/benche64', f.aksId + '/agentPools/systempool', f.c.cluster_id,
  ]);
  assert.ok(f.store.state.operations.every(operation => operation.status === 'succeeded'));
  assert.ok(f.mutations.every(item => !item.resourceId.endsWith('/stop')));
  const full = createDefaultCleanupProviders(f.store, { ...f.options, readDataSafety: async () => ({
    campaign_id: campaignId, cell_run_id: cellId, observed_at_unix_ms: Date.now(), evidence_etag: '"verified-data-1"',
    measurements: { qapNotHealthyReasons: 0, qapPendingBacklog: 0, previousQapPendingBacklog: 0,
      deltaRowsBefore: 42, deltaRowsAfter: 42, kustoRowsBefore: 42, kustoRowsAfter: 42 },
  }) });
  assert.equal(await full.verifySafeIdle(f.store.state, 'verify'), 'complete');
  properties(f.resources.get(f.c.cluster_id)!).optimizedAutoscale = { isEnabled: false, minimum: 2, maximum: 4 };
  assert.equal(await full.verifySafeIdle(f.store.state, 'verify'), 'pending');
});

for (const fault of ['before', 'after'] as const) test(`unknown ARM ${fault}-apply outcome is reconciled without replay`, async t => {
  const f = await fixture(t);
  Object.assign(properties(f.resources.get(f.aksId + '/agentPools/benchd64')!), { count: 1 });
  f.fault(fault);
  await assert.rejects(f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority), /injected/);
  assert.equal(f.mutations.length, 1);
  f.fault('none');
  const result = await f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority);
  assert.equal(result, fault === 'after' ? 'complete' : 'pending');
  assert.equal(f.mutations.length, 1);
  assert.equal(f.store.state.operations[0].status, fault === 'after' ? 'succeeded' : 'indeterminate');
});

test('unschedulable two-node candidate falls back to original three nodes without oscillation', async t => {
  const f = await fixture(t);
  f.health(() => ({ observed_at_unix_ms: Date.now(), pods: [{ name: 'stamp/pod',
    ready: properties(f.resources.get(f.aksId + '/agentPools/systempool')!).count !== 2, scheduled: true }], pdb_blocked: false, versions: [] }));
  assert.equal(await f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority), 'pending');
  assert.equal(await f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority), 'pending');
  assert.equal(await f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority), 'complete');
  assert.equal(await f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority), 'complete');
  assert.deepEqual(f.mutations.map(item => properties(item.body).count), [2, 3]);
});

test('missing baseline and fencing after persisted intent never issue ARM mutations', async t => {
  const missing = await fixture(t, false);
  await assert.rejects(missing.providers.restoreSafeIdle(missing.store.state, 'restore', missing.authority), /baseline is UNKNOWN/);
  assert.deepEqual(missing.mutations, []);
  const f = await fixture(t);
  properties(f.resources.get(f.aksId + '/agentPools/benchd64')!).count = 1;
  f.store.afterWrite = () => { if (f.store.state.operations.length) f.store.state.fencingToken++; };
  await assert.rejects(f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority), /stale cleanup actor/);
  assert.deepEqual(f.mutations, []);
  assert.equal(f.store.state.operations.length, 1);
});

test('additive producer plan and evidence reference fields retain immutable identities', async t => {
  const f = await fixture(t);
  const state = { ...newCampaignState(campaignId, ownerId), runInventory: f.inventory };
  const store = new Store(state);
  const changed = structuredClone(f.inventory);
  changed.fic!.plan!.clientId = ownerId;
  await assert.rejects(publishRunInventory(store, changed), /FIC identity changed/);
  const redirected = structuredClone(f.inventory);
  redirected.evidence_destination!.blob_prefix = 'different-prefix';
  await assert.rejects(publishRunInventory(store, redirected), /evidence destination is immutable/);
  assert.equal(store.revision, 1);
  const attached = structuredClone(f.inventory);
  attached.qap = { database: 'qap', bindings: [{ external_table: 'collector', relative_path: 'runs/' + cellId }],
    state: 'observed', absence: [], reference: { schema_version: 1, account: f.c.account, container: f.c.checkpoints,
      blob: 'runs/' + cellId + '/custody.json', run_id: cellId, nonce: ownerId, manifest_sha256: 'a'.repeat(64),
      namespace_uid: cellId, namespace_owner_token: ownerId } };
  const withReference = new Store({ ...state, runInventory: attached }), changedReference = structuredClone(attached);
  changedReference.qap!.reference!.manifest_sha256 = 'b'.repeat(64);
  await assert.rejects(publishRunInventory(withReference, changedReference), /QAP reference is immutable/);
  const extraBinding = structuredClone(attached);
  extraBinding.qap!.bindings.push({ external_table: 'other', relative_path: 'runs/other' });
  await assert.rejects(publishRunInventory(withReference, extraBinding), /exact planned bindings/);
  const credential = structuredClone(attached);
  credential.qap!.reference!.blob += '?sig=not-allowed';
  await assert.rejects(publishRunInventory(withReference, credential), /QAP reference differs/);
  assert.equal(withReference.revision, 1);
});

test('fencing during ARM response reconciliation cannot publish stale progress or begin another mutation', async t => {
  const f = await fixture(t);
  properties(f.resources.get(f.aksId + '/agentPools/benchd64')!).count = 1;
  assert.equal(await f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority), 'pending');
  const request = f.azure.request.bind(f.azure);
  t.mock.method(f.azure, 'request', async (...args: Parameters<Azure['request']>) => {
    const result = await request(...args);
    if (args[0] === 'GET') f.store.state.fencingToken++;
    return result;
  });
  await assert.rejects(f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority), /stale cleanup actor/);
  assert.equal(f.store.state.operations[0].status, 'pending');
  assert.equal(f.mutations.length, 1);
});
