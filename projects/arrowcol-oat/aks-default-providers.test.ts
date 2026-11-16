import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test, type TestContext } from 'node:test';
import { stringify } from 'yaml';
import { Azure, Config, TAGS, type HttpReceipt } from '../../infra/azure/bootstrap.js';
import { defaultStampConfig } from '../../infra/azure/stamp.js';
import { CUSTODY_CLEANUP_STEPS, CampaignConflictError, CampaignWriteOutcomeUnknownError, campaignMain, newCampaignState, publishRunInventory, type CampaignState, type CampaignStore,
  type RunInventory, type StoredCampaign } from './aks-campaign.js';
import { captureCleanupBaseline, createDefaultCleanupProviders, readCleanupBaseline, type StampHealthReceipt } from './aks-default-providers.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';
import { loadCustodyConfig } from './aks-custody-config.js';
import { observeExactKubernetes, type CustodyCommandRunner } from './aks-setup-cleanup.js';
import { watchdogOnce } from './aks-watchdog.js';

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
async function fixture(t: TestContext, capture = true, c = new Config({ AZURE_SUBSCRIPTION_ID: campaignId })) {
  const azure = new Azure(c);
  const aksId = c.base + '/providers/Microsoft.ContainerService/managedClusters/' + c.cluster;
  const namespace = 'oat-' + cellId.replaceAll('-', ''), identityId = c.identity_id;
  const step = { state: 'not_started' as const, deadline_unix_ms: null, reason_code: null };
  const plan = { version: 1 as const, runId: cellId, namespace, serviceAccount: 'oat', identityId,
    resourceId: identityId + '/federatedIdentityCredentials/arrowcol-run-' + cellId,
    clientId: campaignId, tenantId: campaignId, aksId, server: 'https://fixture.azmk8s.io',
    credentialName: 'arrowcol-run-' + cellId, issuer: 'https://fixture.oic.prod-aks.azure.com/tenant/cluster/',
    subject: `system:serviceaccount:${namespace}:oat`, audience: 'api://AzureADTokenExchange' };
  const inventory: RunInventory = { schema_version: 1, campaign_id: campaignId, owner_id: ownerId, fencing_token: 1,
    orchestration_run_id: campaignId, cell_run_id: cellId, load_admission: 'not_authorized', namespace: { name: namespace, uid: null, owner_token: ownerId },
    planned_cell_duration_ms: 60_000, effective_stop_budgets: { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 1_200_000 },
    resources: [], fic: { resource_id: plan.resourceId, name: plan.credentialName, state: 'planned', absence: null, plan },
    qap: null, evidence_destination: { account: c.account, container: c.checkpoints, blob_prefix: 'evidence/' + cellId },
    cleanup: { stop_admission: step, bounded_drain: step, evidence_capture: step, stop_maintenance: step, terminal_capture: step, handoff: step } };
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
  t.mock.method(azure, 'kql', async (command: string) => {
    assert.ok(command === '.show tables' || command === '.show external tables');
    return { Tables: [{ Columns: [{ ColumnName: 'TableName', ColumnType: 'string' }], Rows: [] }] };
  });
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
  inventory.namespace.uid = cellId;
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

test('exact Kubernetes observations use the injected command boundary', async () => {
  const command: string[] = [], authority: CleanupMutationAuthority = {
    campaignId, ownerId, fencingToken: 1, assertCurrent: async () => {},
  };
  const runCommand: CustodyCommandRunner = async args => {
    command.push(...args);
    return { returncode: 0, stdout: JSON.stringify({ apiVersion: 'v1', kind: 'Namespace' }), stderr: '' };
  };
  const result = await observeExactKubernetes('https://fixture.azmk8s.io', ['namespace', 'oat-fixture'], authority, runCommand);
  assert.equal(result?.kind, 'Namespace');
  assert.deepEqual(command.slice(-5), ['namespace', 'oat-fixture', '--ignore-not-found', '--output', 'json']);
});

function selectedConfig(t: TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'custody-config-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const envFile = path.join(directory, 'private.env'), stampFile = path.join(directory, 'stamp.yaml');
  fs.writeFileSync(stampFile, stringify(defaultStampConfig({ AZURE_SUBSCRIPTION_ID: campaignId, AZURE_STAMP: '12345',
    AZURE_LOCATION: 'canadacentral' })));
  fs.writeFileSync(envFile, [`AZURE_SUBSCRIPTION_ID=${campaignId}`, `ARM_SUBSCRIPTION_ID=${campaignId}`,
    'CAMPAIGN_STORAGE_ACCOUNT=arrowcollector12345', 'CAMPAIGN_STATE_CONTAINER=checkpoints',
    `CAMPAIGN_ID=${campaignId}`, `CAMPAIGN_OWNER_ID=${ownerId}`, 'CAMPAIGN_FENCING_TOKEN=1'].join('\n'));
  const args = ['--env-file', envFile, '--stamp-config', stampFile];
  return { args, ...loadCustodyConfig(args, false, {}) };
}
test('stock custody selection loads explicit nondefault stamp and refuses missing or mismatched selection before access', async t => {
  const selected = selectedConfig(t);
  assert.equal(selected.config.stamp, '12345');
  assert.equal(selected.config.location, 'canadacentral');
  assert.equal(selected.config.account, 'arrowcollector12345');
  assert.equal(loadCustodyConfig(['--once', ...selected.args], true, {}).config.cluster, 'arrow-collector-12345');
  assert.throws(() => loadCustodyConfig([], false, selected.env), /explicit --env-file/);
  for (const mismatch of [{ ARM_SUBSCRIPTION_ID: ownerId }, { AZURE_SUBSCRIPTION_ID: ownerId },
    { CAMPAIGN_STORAGE_ACCOUNT: 'arrowcollector1' }, { CAMPAIGN_STATE_CONTAINER: 'other' }, { AZURE_STAMP: '1' }])
    assert.throws(() => loadCustodyConfig(selected.args, false, mismatch), /match/);
  assert.throws(() => loadCustodyConfig(['--once', '--watch', ...selected.args], true, {}), /exactly one/);
  await assert.rejects(watchdogOnce(selected.env), /explicit validated stamp/);
});
test('stock campaign capture freezes selected-stamp baseline, reuses its receipt, and gates arm on its full identity', async t => {
  const selected = selectedConfig(t), f = await fixture(t, false, selected.config);
  f.inventory.namespace.uid = null;
  f.inventory.resources.push({ api_version: 'apps/v1', kind: 'Deployment', namespace: f.inventory.namespace.name,
    name: 'collector', uid: null, absence: null, role: 'collector', state: 'planned' });
  f.store.state = { ...newCampaignState(campaignId, ownerId, f.store.state.startedAtMs), runInventory: f.inventory };
  const deps = { store: f.store, azure: f.azure, env: {} }, lines: string[] = [];
  t.mock.method(console, 'log', (line: string) => { lines.push(line); });
  const arm = () => campaignMain(['arm', ownerId, '1', ...selected.args], deps);
  await assert.rejects(arm(), /baseline is UNKNOWN/);
  assert.equal(f.store.state.mode, 'observe');
  await campaignMain(['capture-baseline', ownerId, '1', ...selected.args], deps);
  const receipt = JSON.parse(lines.at(-1)!).receipt;
  assert.equal(receipt.status, 'captured');
  assert.equal(receipt.campaign_id, campaignId);
  assert.equal(receipt.owner_id, ownerId);
  assert.equal(receipt.fencing_token, 1);
  const saved = f.blobs.get(receipt.blob)!;
  assert.equal(receipt.etag, saved.etag);
  assert.equal(receipt.captured_at_unix_ms, JSON.parse(saved.bytes.toString()).captured_at_unix_ms);
  await campaignMain(['capture-baseline', ownerId, '1', ...selected.args], deps);
  assert.deepEqual(JSON.parse(lines.at(-1)!).receipt, receipt);
  assert.equal(f.blobs.size, 1, 'retry never replaces or creates another baseline');
  const original = saved.bytes;
  type BaselineFixture = { campaign_id: string; capacity: { receipts: Array<{ resource_id: string }> };
    non_run_kusto?: { databases: string[] } };
  for (const mutate of [
    (value: BaselineFixture) => { value.campaign_id = ownerId; },
    (value: BaselineFixture) => { value.capacity.receipts[0]!.resource_id = value.capacity.receipts[0]!.resource_id.replace('12345', '1'); },
    (value: BaselineFixture) => { value.non_run_kusto!.databases = ['other']; },
    (value: BaselineFixture) => { delete value.non_run_kusto; },
  ]) {
    const changed = JSON.parse(original.toString());
    mutate(changed); saved.bytes = Buffer.from(JSON.stringify(changed));
    await assert.rejects(arm(), /baseline|Kusto|UNKNOWN/);
    assert.equal(f.store.state.mode, 'observe');
  }
  saved.bytes = original;
  await arm();
  assert.equal(f.store.state.mode, 'armed');
  const observed = structuredClone(f.inventory);
  observed.namespace.uid = cellId;
  await publishRunInventory(f.store, observed);
  const blob = f.azure.blob.bind(f.azure);
  t.mock.method(f.azure, 'blob', async (...args: Parameters<Azure['blob']>) => {
    assert.equal(args[0], 'GET', 'original proof retries must be read-only');
    return blob(...args);
  });
  t.mock.method(f.azure, 'request', async () => { throw new Error('Retry must not recapture ARM capacity.'); });
  t.mock.method(f.azure, 'kql', async () => { throw new Error('Retry must not recapture Kusto contents.'); });
  await campaignMain(['capture-baseline', ownerId, '1', ...selected.args], deps);
  assert.deepEqual(JSON.parse(lines.at(-1)!).receipt, receipt);
  f.store.state = { ...f.store.state, status: 'cleaning', fencingToken: 2, fencedAtMs: Date.now(),
    fenceReason: 'heartbeat-expired', cleanupStatus: 'running' };
  assert.deepEqual(await readCleanupBaseline(f.azure, f.store.state), receipt,
    'controller takeover must retain the immutable producer fence, not recapture with the cleanup fence');
  assert.deepEqual(await captureCleanupBaseline(f.store, f.azure, ownerId, 2), receipt,
    'existing proof lookup remains read-only under the current controller fence after provisioning');
  const rewritten = JSON.parse(original.toString());
  rewritten.producer_fencing_token = 2; saved.bytes = Buffer.from(JSON.stringify(rewritten));
  await assert.rejects(readCleanupBaseline(f.azure, f.store.state), /baseline/);
  assert.deepEqual(f.mutations, [], 'source qualification must never mutate ARM');
});
test('stock authorization and ready boundaries refuse missing baseline without advancing admission', async t => {
  const selected = selectedConfig(t), f = await fixture(t, false, selected.config);
  f.inventory.load_admission = 'authorization_pending';
  f.store.state = { ...newCampaignState(campaignId, ownerId, f.store.state.startedAtMs), mode: 'armed', runInventory: f.inventory };
  const deps = { store: f.store, azure: f.azure, env: {} };
  const authorized = { ...f.inventory, load_admission: 'authorized' };
  t.mock.method(process.stdin, Symbol.asyncIterator, async function* () { yield JSON.stringify(authorized); });
  await assert.rejects(campaignMain(['publish-inventory', ...selected.args], deps), /baseline is UNKNOWN/);
  assert.equal(f.store.state.runInventory!.load_admission, 'authorization_pending');
  await assert.rejects(campaignMain(['assert-ready', ownerId, '1', ...selected.args], deps), /baseline is UNKNOWN/);
  assert.deepEqual(f.mutations, []);
});

test('first campaign baseline rejects late namespace/resource identity and noninitial milestones before observation', async t => {
  const changes: Record<string, (state: CampaignState) => void> = {
    namespace: state => { state.runInventory!.namespace.uid = cellId; },
    resource_uid: state => { state.runInventory!.resources[0].uid = cellId; },
    observed_resource: state => { state.runInventory!.resources[0].state = 'observed'; },
    observed_fic: state => { state.runInventory!.fic!.state = 'observed'; },
    observed_qap: state => { state.runInventory!.qap = { database: 'qap', bindings: [
      { external_table: 'collector', relative_path: 'runs/' + cellId }], state: 'observed', absence: [] }; },
    cleanup: state => { state.runInventory!.cleanup.evidence_capture.state = 'complete'; },
    cleanup_deadline: state => { state.runInventory!.cleanup.terminal_capture.deadline_unix_ms = Date.now() + 120_000; },
    armed: state => { state.mode = 'armed'; },
  };
  for (const [name, change] of Object.entries(changes)) await t.test(name, async t => {
    const f = await fixture(t, false);
    f.inventory.namespace.uid = null;
    f.inventory.resources.push({ api_version: 'apps/v1', kind: 'Deployment', namespace: f.inventory.namespace.name,
      name: 'collector', uid: null, absence: null, role: 'collector', state: 'planned' });
    f.store.state = { ...newCampaignState(campaignId, ownerId, f.store.state.startedAtMs), runInventory: f.inventory };
    change(f.store.state);
    const request = f.azure.request.bind(f.azure), kql = f.azure.kql.bind(f.azure);
    let observations = 0;
    t.mock.method(f.azure, 'request', async (...args: Parameters<Azure['request']>) => { observations++; return request(...args); });
    t.mock.method(f.azure, 'kql', async (...args: Parameters<Azure['kql']>) => { observations++; return kql(...args); });
    await assert.rejects(captureCleanupBaseline(f.store, f.azure, ownerId, 1), /initial.*pre-provision/);
    assert.equal(observations, 0);
    assert.equal(f.blobs.size, 0);
    assert.deepEqual(f.mutations, []);
  });
});
test('first campaign baseline pins the current inventory version throughout observation', async t => {
  for (const changeInventory of [true, false]) await t.test(changeInventory ? 'same actor advances inventory' : 'same actor changes only version', async t => {
    const f = await fixture(t, false);
    f.inventory.namespace.uid = null;
    f.store.state = { ...newCampaignState(campaignId, ownerId, f.store.state.startedAtMs), runInventory: f.inventory };
    const kql = f.azure.kql.bind(f.azure);
    let raced = false;
    t.mock.method(f.azure, 'kql', async (...args: Parameters<Azure['kql']>) => {
      const result = await kql(...args);
      if (!raced) {
        raced = true;
        if (changeInventory) f.store.state.runInventory!.namespace.uid = cellId;
        f.store.revision++;
      }
      return result;
    });
    await assert.rejects(captureCleanupBaseline(f.store, f.azure, ownerId, 1), CampaignConflictError);
    assert.equal(f.blobs.size, 0);
    assert.deepEqual(f.mutations, []);
  });
});
test('stock publication cannot advance initial preparation before the immutable campaign baseline exists', async t => {
  const selected = selectedConfig(t), f = await fixture(t, false, selected.config);
  f.inventory.namespace.uid = null;
  f.store.state = newCampaignState(campaignId, ownerId, f.store.state.startedAtMs);
  const deps = { store: f.store, azure: f.azure, env: {} }, lines: string[] = [];
  t.mock.method(console, 'log', (line: string) => { lines.push(line); });
  let input = structuredClone(f.inventory);
  t.mock.method(process.stdin, Symbol.asyncIterator, async function* () { yield JSON.stringify(input); });
  const publish = () => campaignMain(['publish-inventory', ...selected.args], deps);
  await publish();
  assert.equal(f.blobs.size, 0);
  input.namespace.uid = cellId;
  await assert.rejects(publish(), /baseline is UNKNOWN/);
  assert.equal(f.store.state.runInventory!.namespace.uid, null);
  const kql = f.azure.kql.bind(f.azure);
  let raced = false;
  t.mock.method(f.azure, 'kql', async (...args: Parameters<Azure['kql']>) => {
    if (!raced) {
      raced = true;
      await assert.rejects(publish(), /baseline is UNKNOWN/);
      assert.equal(f.store.state.runInventory!.namespace.uid, null);
    }
    return kql(...args);
  });
  await campaignMain(['capture-baseline', ownerId, '1', ...selected.args], deps);
  const receipt = JSON.parse(lines.at(-1)!).receipt, original = f.blobs.get(receipt.blob)!.bytes;
  await publish();
  assert.equal(f.store.state.runInventory!.namespace.uid, cellId);
  await campaignMain(['capture-baseline', ownerId, '1', ...selected.args], deps);
  assert.deepEqual(JSON.parse(lines.at(-1)!).receipt, receipt);
  assert.deepEqual(f.blobs.get(receipt.blob)!.bytes, original);
  assert.deepEqual(f.mutations, []);
});
test('advancement after baseline write remains UNKNOWN and retry reads the original proof without another write', async t => {
  const f = await fixture(t, false);
  f.inventory.namespace.uid = null;
  f.store.state = { ...newCampaignState(campaignId, ownerId, f.store.state.startedAtMs), runInventory: f.inventory };
  const blob = f.azure.blob.bind(f.azure);
  let writes = 0;
  t.mock.method(f.azure, 'blob', async (...args: Parameters<Azure['blob']>) => {
    const result = await blob(...args);
    if (args[0] === 'PUT') {
      writes++;
      f.store.state.runInventory!.namespace.uid = cellId;
      f.store.revision++;
    }
    return result;
  });
  await assert.rejects(captureCleanupBaseline(f.store, f.azure, ownerId, 1), CampaignWriteOutcomeUnknownError);
  assert.equal(f.blobs.size, 1);
  const [key, saved] = [...f.blobs][0], original = saved.bytes;
  t.mock.method(f.azure, 'request', async () => { throw new Error('Uncertain capture cannot reobserve ARM.'); });
  t.mock.method(f.azure, 'kql', async () => { throw new Error('Uncertain capture cannot reobserve Kusto.'); });
  const receipt = await captureCleanupBaseline(f.store, f.azure, ownerId, 1);
  assert.equal(receipt.blob, key);
  assert.equal(receipt.etag, saved.etag);
  assert.deepEqual(saved.bytes, original);
  assert.equal(writes, 1);
});

test('simulated lost immutable baseline PUT acknowledgement retries the original proof without recapture or rewrite', async t => {
  const f = await fixture(t, false);
  f.inventory.namespace.uid = null;
  f.store.state = { ...newCampaignState(campaignId, ownerId, f.store.state.startedAtMs), runInventory: f.inventory };
  const blob = f.azure.blob.bind(f.azure);
  let writes = 0, loseAck = true;
  t.mock.method(f.azure, 'blob', async (...args: Parameters<Azure['blob']>) => {
    const result = await blob(...args);
    if (args[0] === 'PUT') {
      writes++;
      if (loseAck) { loseAck = false; throw new Error('Simulated lost baseline PUT acknowledgement.'); }
    }
    return result;
  });
  await assert.rejects(captureCleanupBaseline(f.store, f.azure, ownerId, 1), CampaignWriteOutcomeUnknownError);
  const [key, saved] = [...f.blobs][0]!, original = Buffer.from(saved.bytes), etag = saved.etag;
  t.mock.method(f.azure, 'kql', async () => { throw new Error('Baseline retry must not recapture Kusto.'); });
  t.mock.method(f.azure, 'request', async () => { throw new Error('Baseline retry must not reobserve capacity.'); });
  const receipt = await captureCleanupBaseline(f.store, f.azure, ownerId, 1);
  assert.equal(receipt.blob, key);
  assert.equal(receipt.etag, etag);
  assert.deepEqual(f.blobs.get(key)!.bytes, original);
  assert.equal(writes, 1);
});

test('default restoration is a true no-op at supported safe capacity and capacity alone cannot complete full safety', async t => {
  const f = await fixture(t);
  assert.equal(await f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority), 'complete');
  assert.equal(await f.providers.restoreSafeIdle(f.store.state, 'restore', f.authority), 'complete');
  assert.deepEqual(f.mutations, []);
  await assert.rejects(f.providers.verifySafeIdle(f.store.state, 'verify'), /positively completed exact removals/);
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
