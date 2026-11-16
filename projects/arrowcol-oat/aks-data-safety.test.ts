import assert from 'node:assert/strict';
import { test, type TestContext } from 'node:test';
import { Azure, Config, TAGS, type HttpReceipt } from '../../infra/azure/bootstrap.js';
import { CampaignConflictError, CampaignWriteOutcomeUnknownError, CUSTODY_CLEANUP_STEPS, newCampaignState, type CampaignState, type CampaignStore, type RunInventory } from './aks-campaign.js';
import { captureCellBaseline, captureTerminalData, observeDataPreservation } from './aks-data-safety.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';
import { createDefaultCleanupProviders } from './aks-default-providers.js';

const campaignId = '11111111-1111-4111-8111-111111111111';
const ownerId = '22222222-2222-4222-8222-222222222222';
const cellId = '33333333-3333-4333-8333-333333333333';
function fixture(t: TestContext) {
  const c = new Config({ AZURE_SUBSCRIPTION_ID: campaignId }), azure = new Azure(c);
  t.mock.method(azure, 'request', async () => { throw new Error('Mock QAP observations unavailable; no cloud requests permitted.'); });
  const hot = (c.table_prefix ? c.table_prefix + '/' : '') + 'explicit-hot-table';
  const step = { state: 'not_started' as const, deadline_unix_ms: null, reason_code: null };
  const inventory: RunInventory = { schema_version: 1, campaign_id: campaignId, owner_id: ownerId, fencing_token: 1,
    orchestration_run_id: campaignId, cell_run_id: cellId, load_admission: 'not_authorized',
    namespace: { name: 'oat-' + cellId.replaceAll('-', ''), uid: cellId, owner_token: ownerId }, resources: [], fic: null,
    qap: { database: c.experimentDatabase, bindings: [{ external_table: 'hot_table', relative_path: hot }], state: 'planned', absence: [], reference: null },
    cleanup: { stop_admission: step, bounded_drain: step, evidence_capture: step, stop_maintenance: step, terminal_capture: step, handoff: step } };
  let state: CampaignState = { ...newCampaignState(campaignId, ownerId, Date.now() - 10_000), heartbeatAtMs: Date.now(), runInventory: inventory };
  const store: CampaignStore = { read: async () => ({ state: structuredClone(state), etag: '"inventory-1"' }),
    write: async () => { throw new Error('Baseline capture must not write campaign inventory.'); } };
  const capturedAt = Date.now();
  const baseline = { schema_version: 1, campaign_id: campaignId, owner_id: ownerId, producer_fencing_token: 1,
    started_at_unix_ms: state.startedAtMs, captured_at_unix_ms: capturedAt,
    capacity: { scope: 'arm_capacity_only', status: 'capacity_idle',
      system_pool: { count: 3, vm_size: 'Standard_D8ds_v6', autoscale_enabled: false, autoscale_min: null, autoscale_max: null },
      kusto: { sku: 'Standard_E4ads_v5', capacity: 2, autoscale_enabled: true, autoscale_min: 2, autoscale_max: 4, auto_stop_enabled: true },
      receipts: [c.base + '/providers/Microsoft.ContainerService/managedClusters/' + c.cluster,
        ...['systempool', 'benchd64', 'benche64'].map(name =>
          c.base + '/providers/Microsoft.ContainerService/managedClusters/' + c.cluster + '/agentPools/' + name), c.cluster_id]
        .map(resource_id => ({ resource_id, http_status: 200, request_id: 'positive-idle', body_sha256: '1'.repeat(64), observed_at_unix_ms: capturedAt })) },
    non_run_kusto: { schema_version: 1, scope: 'configured_stamp_databases_non_current_run_tables', tables: [],
      databases: [...new Set([c.database, c.experimentDatabase])], observed_at_unix_ms: capturedAt,
      source: '.show tables and .show external tables with exact current run ownership exclusion',
      empty_reason: 'positive_inventory_contains_no_non_run_tables' } };
  const blobs = new Map<string, { bytes: Buffer; etag: string }>([
    [`_arrowcol-custody/campaigns/${campaignId}/baseline.json`, { bytes: Buffer.from(JSON.stringify(baseline)), etag: '"campaign-baseline"' }],
  ]);
  let requests = 0, denyStorage = false, loseWrite = false;
  t.mock.method(azure, 'blob', async (method: string, container: string, name: string, query?: Record<string, string>,
    body?: string | Buffer | null, headers?: Record<string, string>, _missing?: boolean, _idempotent?: boolean, capture?: (receipt: HttpReceipt) => void) => {
    if (container === c.data) {
      assert.equal(method, 'GET'); assert.equal(query?.prefix, hot + '/_delta_log/');
      if (denyStorage) throw new Error('403 permission denied');
      const requestId = 'empty-root-' + ++requests;
      capture?.({ status: 200, bodyBase64: '', headers: { 'x-ms-request-id': requestId } });
      return [Buffer.from(`<EnumerationResults><Prefix>${hot}/_delta_log/</Prefix><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>`),
        { 'x-ms-request-id': requestId }];
    }
    assert.equal(container, c.checkpoints);
    if (query?.comp === 'list') {
      capture?.({ status: 200, bodyBase64: '', headers: { 'x-ms-request-id': 'custody-list' } });
      return [Buffer.from(`<EnumerationResults><Prefix>${query.prefix}</Prefix><Blobs>${[...blobs.keys()]
        .filter(name => name.startsWith(query.prefix!)).map(name => `<Blob><Name>${name}</Name></Blob>`).join('')}</Blobs><NextMarker></NextMarker></EnumerationResults>`),
        { 'x-ms-request-id': 'custody-list' }];
    }
    const previous = blobs.get(name);
    if (method === 'GET') return [previous?.bytes ?? null, previous ? { etag: previous.etag } : {}];
    assert.equal(method, 'PUT'); assert.equal(headers?.['If-None-Match'], '*'); assert.equal(previous, undefined);
    const etag = `"data-${blobs.size + 1}"`;
    blobs.set(name, { bytes: Buffer.from(String(body)), etag });
    if (loseWrite) throw new Error('response lost after storage applied write');
    return [null, { ':status': '201', etag }];
  });
  const input = { schema_version: 1, campaign_id: campaignId, orchestration_run_id: campaignId, cell_run_id: cellId,
    expected_etag: '"inventory-1"', hot_delta_relative_path: hot, actor: { mode: 'producer', owner_id: ownerId, fencing_token: 1 } };
  return { azure, store, input, blobs, get requests() { return requests; }, get state() { return structuredClone(state); },
    change: (update: Partial<CampaignState>) => { state = { ...state, ...update }; },
    inventory, deny: () => { denyStorage = true; }, lose: () => { loseWrite = true; } };
}
test('capture-cell-baseline freezes positive exact absence, not zero rows, and reuses the original version', async t => {
  const f = fixture(t), receipt = await captureCellBaseline(f.store, f.azure, f.input);
  assert.equal(receipt.delta_state, 'observed_absent');
  assert.equal(receipt.table_id, null); assert.equal(receipt.delta_version, null); assert.equal(receipt.rows, null);
  assert.equal(f.requests, 2);
  assert.deepEqual(await captureCellBaseline(f.store, f.azure, f.input), receipt);
  assert.equal(f.requests, 2, 'idempotent retry must not replace the original pre-load observation');
});
test('capture-cell-baseline rejects stale, fenced, wrong-cell and non-exact path before data reads', async t => {
  const f = fixture(t);
  await assert.rejects(captureCellBaseline(f.store, f.azure, { ...f.input, expected_etag: '"old"' }), CampaignConflictError);
  await assert.rejects(captureCellBaseline(f.store, f.azure, { ...f.input, cell_run_id: ownerId }), /fenced/i);
  await assert.rejects(captureCellBaseline(f.store, f.azure, { ...f.input, hot_delta_relative_path: 'prefix-only' }), /exactly one/);
  f.change({ fencingToken: 2, status: 'fenced', fencedAtMs: Date.now(), fenceReason: 'heartbeat-expired' });
  await assert.rejects(captureCellBaseline(f.store, f.azure, f.input));
  assert.equal(f.requests, 0);
});
test('in-flight authorization and storage denial cannot create a pre-load absence receipt', async t => {
  const f = fixture(t);
  f.change({ runInventory: { ...f.inventory, load_admission: 'authorization_pending' } });
  await assert.rejects(captureCellBaseline(f.store, f.azure, f.input), /not_authorized/);
  f.change({ runInventory: f.inventory }); f.deny();
  await assert.rejects(captureCellBaseline(f.store, f.azure, f.input), /403/);
  assert.equal([...f.blobs.keys()].filter(name => name.endsWith('/delta-before.json')).length, 0);
});
test('a lost post-write response stays UNKNOWN and recovery reads the original durable snapshot without replay', async t => {
  const f = fixture(t); f.lose();
  await assert.rejects(captureCellBaseline(f.store, f.azure, f.input), CampaignWriteOutcomeUnknownError);
  assert.equal(f.requests, 2);
  const receipt = await captureCellBaseline(f.store, f.azure, f.input);
  assert.equal(receipt.delta_state, 'observed_absent');
  assert.equal(f.requests, 2);
});
test('terminal and independent final replay preserve explicit absence without manufacturing delivery or post-DROP QAP rows', async t => {
  const f = fixture(t);
  await captureCellBaseline(f.store, f.azure, f.input);
  const step = { state: 'complete' as const, deadline_unix_ms: null, reason_code: null };
  const terminalInventory = { ...f.inventory, load_admission: 'authorized' as const, cleanup: { ...f.inventory.cleanup,
    stop_admission: step, bounded_drain: step, evidence_capture: step, stop_maintenance: step } };
  f.change({ runInventory: terminalInventory });
  const authority: CleanupMutationAuthority = { mode: 'producer', campaignId, ownerId, fencingToken: 1, assertCurrent: async () => {} };
  let stoppedChecks = 0;
  await captureTerminalData(f.store, f.azure, authority, async () => { stoppedChecks++; });
  assert.ok(stoppedChecks > 0);
  f.change({ runInventory: { ...terminalInventory, qap: { ...terminalInventory.qap!, state: 'removed' },
    cleanup: { ...terminalInventory.cleanup, terminal_capture: step } } });
  const proof = await observeDataPreservation(f.store, f.azure, f.state, authority);
  assert.equal(proof.status, 'preserved');
  assert.equal(proof.delta.disposition, 'absence_preserved');
  assert.equal(proof.delta.current?.row_count, null);
  assert.deepEqual(proof.run_owned_qap_after, { rows: null, reason: 'run_owned_qap_removed_by_design' });
  assert.ok([...f.blobs.keys()].some(name => name.endsWith('/data-final.json')));
  const c = f.azure.c, aksId = c.base + '/providers/Microsoft.ContainerService/managedClusters/' + c.cluster;
  t.mock.method(f.azure, 'request', async (_method: string, url: string, _resource: string, _body?: string | Buffer | null,
    _headers?: unknown, _missing?: boolean, _idempotent?: boolean, receipt?: (value: HttpReceipt) => void) => {
    const id = new URL(url).pathname, tags = { ...TAGS, sandboxId: c.sandbox };
    const resource = id === aksId ? { id, type: 'Microsoft.ContainerService/managedClusters', tags, properties: { provisioningState: 'Succeeded',
      agentPoolProfiles: ['systempool', 'benchd64', 'benche64'].map(name => ({ name })) } } :
      id === c.cluster_id ? { id, type: 'Microsoft.Kusto/clusters', tags, sku: { name: 'Standard_E4ads_v5', capacity: 2 },
        properties: { provisioningState: 'Succeeded', state: 'Running', optimizedAutoscale: { isEnabled: true, minimum: 2, maximum: 4 }, enableAutoStop: true } } :
        { id, type: 'Microsoft.ContainerService/managedClusters/agentPools', properties: { count: id.endsWith('/systempool') ? 3 : 0, vmSize: id.endsWith('/systempool') ? 'Standard_D8ds_v6' : 'Standard_D64ds_v6',
          mode: id.endsWith('/systempool') ? 'System' : 'User', provisioningState: 'Succeeded',
          enableAutoScaling: !id.endsWith('/systempool'), ...(id.endsWith('/systempool') ? {} : { minCount: 0, maxCount: 1 }) } };
    const bytes = Buffer.from(JSON.stringify(resource));
    receipt?.({ status: 200, headers: { 'x-ms-request-id': 'capacity-read' }, bodyBase64: bytes.toString('base64') });
    return [bytes, { ':status': '200', 'x-ms-request-id': 'capacity-read' }];
  });
  f.change({ status: 'cleaning', mode: 'armed', fencingToken: 2, fenceReason: 'heartbeat-expired', fencedAtMs: Date.now(),
    cleanupStatus: 'running', cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({
      step, idempotencyKey: `${campaignId}:2:${step}`, status: index === CUSTODY_CLEANUP_STEPS.length - 1 ? 'running' : 'complete',
    })) });
  const defaults = createDefaultCleanupProviders(f.store, { azure: f.azure, workDirectory: '.',
    readHealth: async () => ({ observed_at_unix_ms: Date.now(), pods: [{ name: 'stamp/healthy', ready: true, scheduled: true }],
      pdb_blocked: false, versions: [] }) });
  assert.equal(await defaults.verifySafeIdle(f.state, 'verify'), 'complete', 'installed real data provider must compose with independent capacity, not numeric zero stand-ins');
  f.blobs.delete([...f.blobs.keys()].find(name => name.endsWith('/delta-terminal.json'))!);
  await assert.rejects(defaults.verifySafeIdle(f.state, 'verify'), /absent or unversioned/);
});
