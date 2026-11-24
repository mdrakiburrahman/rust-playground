import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { test, type TestContext } from 'node:test';
import { Azure, Config, TAGS, canonical_json, type HttpReceipt } from '../../infra/azure/bootstrap.js';
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
  let requests = 0, checkpointPuts = 0, denyStorage = false, loseWrite = false;
  let afterLostWrite: (() => void) | undefined;
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
    assert.equal(method, 'PUT'); checkpointPuts++; assert.equal(headers?.['If-None-Match'], '*'); assert.equal(previous, undefined);
    const etag = `"data-${blobs.size + 1}"`;
    blobs.set(name, { bytes: Buffer.from(String(body)), etag });
    if (loseWrite) { afterLostWrite?.(); throw new Error('response lost after storage applied write'); }
    return [null, { ':status': '201', etag }];
  });
  const input = { schema_version: 1, campaign_id: campaignId, orchestration_run_id: campaignId, cell_run_id: cellId,
    expected_etag: '"inventory-1"', hot_delta_relative_path: hot, actor: { mode: 'producer', owner_id: ownerId, fencing_token: 1 } };
  return { azure, store, input, blobs, get checkpointPuts() { return checkpointPuts; }, get requests() { return requests; }, get state() { return structuredClone(state); },
    change: (update: Partial<CampaignState>) => { state = { ...state, ...update }; },
    inventory, deny: () => { denyStorage = true; }, lose: (after?: () => void) => { loseWrite = true; afterLostWrite = after; } };
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
  const stamp = await shutdownFixture(t, 0);
  await installMatchedFinalization(t, stamp);
  stamp.inventory.qap = { ...stamp.inventory.qap!, state: 'removed' };
  stamp.inventory.resources = stamp.inventory.resources.map(resource => ({ ...resource, state: 'removed' }));
  stamp.change({ runInventory: stamp.inventory });
  const c = stamp.azure.c, aksId = c.base + '/providers/Microsoft.ContainerService/managedClusters/' + c.cluster;
  let countResponse: Buffer | undefined;
  t.mock.method(stamp.azure, 'request', async (_method: string, url: string, _resource: string, _body?: string | Buffer | null,
    _headers?: unknown, _missing?: boolean, _idempotent?: boolean, receipt?: (value: HttpReceipt) => void) => {
    if (url.endsWith('/v1/rest/query') && countResponse) return [countResponse, { ':status': '200' }];
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
  stamp.change({ status: 'cleaning', mode: 'armed', fencingToken: 2, fenceReason: 'heartbeat-expired', fencedAtMs: Date.now(),
    cleanupStatus: 'running', cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({
      step, idempotencyKey: `${campaignId}:2:${step}`, status: index === CUSTODY_CLEANUP_STEPS.length - 1 ? 'running' : 'complete',
    })) });
  const defaults = createDefaultCleanupProviders(stamp.store, { azure: stamp.azure, workDirectory: '.',
    readHealth: async () => ({ observed_at_unix_ms: Date.now(), pods: [{ name: 'stamp/healthy', ready: true, scheduled: true }],
      pdb_blocked: false, versions: [] }) });
  assert.equal(await defaults.verifySafeIdle(stamp.state, 'verify'), 'complete', 'installed real data provider must compose with independent capacity, not numeric zero stand-ins');
  const baselineBlob = `_arrowcol-custody/campaigns/${campaignId}/baseline.json`, baseline = stamp.blobs.get(baselineBlob)!;
  const originalBaseline = baseline.bytes;
  const table = 'preserved_external', database = c.database;
  const schema = { OrderedColumns: [{ Name: 'value', Type: 'long' }] };
  const definition = { TableName: table, TableType: 'Delta', DocString: 'preserved-stamp-table',
    ConnectionStrings: [`abfss://${c.data}@${c.account}.dfs.core.windows.net/preserved/table;managed_identity=${ownerId}`],
    Properties: JSON.stringify({ AutoUpdateSchema: false, Compressed: false, CompressionType: null, Encoding: null,
      FileExtension: null, Format: 'Parquet', IncludeHeaders: 'None', NamePrefix: null, Shortcut: false }) };
  const digest = (value: unknown) => createHash('sha256').update(canonical_json(value)).digest('hex');
  const snapshot = JSON.parse(originalBaseline.toString());
  snapshot.non_run_kusto.empty_reason = null;
  snapshot.non_run_kusto.tables = [{ database, name: table, kind: 'external',
    schema_sha256: digest(schema), definition_sha256: digest(definition),
    before: { database, table, kind: 'external', rows: 7, query_sha256: '1'.repeat(64),
      result_sha256: '2'.repeat(64), observed_at_unix_ms: snapshot.captured_at_unix_ms },
    qap_before: { observed_at_unix_ms: snapshot.captured_at_unix_ms, query_sha256: '3'.repeat(64),
      is_healthy: true, not_healthy_reason_present: false, pending_files: 0, row_count: 7, accelerated_row_count: 7, table_version: 1 } }];
  baseline.bytes = Buffer.from(JSON.stringify(snapshot));
  const result = (row: Record<string, unknown>) => ({ Tables: [{
    Columns: Object.keys(row).map(ColumnName => ({ ColumnName, ColumnType: 'string' })), Rows: [Object.values(row)],
  }] });
  countResponse = Buffer.from(JSON.stringify({ Tables: [
    { TableName: 'Table_0', Columns: [{ ColumnName: 'Count', ColumnType: 'long' }], Rows: [[7]] },
    { TableName: 'Table_1', Columns: ['Severity', 'SeverityName', 'StatusCode'].map(ColumnName =>
      ({ ColumnName, ColumnType: ColumnName === 'SeverityName' ? 'string' : 'long' })), Rows: [[4, 'Info', 0]] },
    { TableName: 'Table_2', Columns: [{ ColumnName: 'Value', ColumnType: 'string' }], Rows: [] },
    { TableName: 'Table_3', Columns: ['Ordinal', 'Kind', 'Name', 'Id', 'PrettyName'].map(ColumnName =>
      ({ ColumnName, ColumnType: ColumnName === 'Ordinal' ? 'long' : 'string' })),
      Rows: [[0, 'QueryResult', 'PrimaryResult', 'result', ''], [1, 'QueryStatus', 'QueryStatus', 'status', ''],
        [2, 'QueryProperties', '@ExtendedProperties', 'properties', '']] },
  ] }));
  for (const [label, enabled, healthy, pending] of [
    ['missing first sample and unhealthy final', [false, true], false, [0, 0]],
    ['missing first sample and healthy final', [false, true], true, [0, 0]],
    ['missing final sample', [true, false], true, [0, 0]],
    ['unhealthy final with both samples', [true, true], false, [0, 0]],
    ['growing backlog with both samples', [true, true], true, [0, 1]],
  ] as const) {
    await t.test(`default safe idle rejects ${label}`, async () => {
      let sample = -1;
      t.mock.method(stamp.azure, 'kql', async (command: string) => {
        if (command.endsWith('schema as json')) return result({ TableName: table, DatabaseName: database, Schema: JSON.stringify(schema) });
        if (command === '.show external tables') return result(definition);
        if (command.endsWith('policy query_acceleration')) {
          sample++;
          return result({ EntityName: `[${database}].[${table}]`, EntityType: 'ExternalTable',
            Policy: enabled[sample] ? JSON.stringify({ IsEnabled: true }) : null });
        }
        if (command.endsWith(' details')) return result({ QueryAccelerationState: { IsHealthy: healthy,
          NotHealthyReason: healthy ? null : 'not healthy', RowCount: 7, AcceleratedRowCount: 7, TableVersion: 1 } });
        if (command.includes('query_acceleration statistics')) return result({ ExternalTableName: table,
          AccelerationPendingDataFilesCount: pending[sample] });
        throw new Error('Unexpected mocked Kusto command: ' + command);
      });
      await assert.rejects(defaults.verifySafeIdle(stamp.state, 'verify'),
        /enabled policy changed|non_run_qap_unhealthy_or_backlog_growing/);
      assert.equal(sample, 1, 'both exact QAP samples must be observed');
    });
  }
  baseline.bytes = originalBaseline;
  stamp.blobs.delete([...stamp.blobs.keys()].find(name => name.endsWith('/delta-terminal.json'))!);
  await assert.rejects(defaults.verifySafeIdle(stamp.state, 'verify'), /finalization|absent or unversioned/);
});

async function shutdownFixture(t: TestContext, exitCode: number, marker = true, terminalRestart = 0) {
  const f = fixture(t);
  await captureCellBaseline(f.store, f.azure, f.input);
  const step = { state: 'complete' as const, deadline_unix_ms: null, reason_code: null };
  const drain = { state: 'complete' as const, deadline_unix_ms: Date.now() + 60_000, reason_code: null };
  const inventory = { ...f.inventory, load_admission: 'authorized' as const, evidence_destination: {
    account: f.azure.c.account, container: f.azure.c.checkpoints, blob_prefix: 'evidence/' + cellId },
    cleanup: { ...f.inventory.cleanup, stop_admission: step, bounded_drain: drain, evidence_capture: step, stop_maintenance: step } };
  f.change({ runInventory: inventory });
  const authority: CleanupMutationAuthority = { mode: 'producer', campaignId, ownerId, fencingToken: 1, assertCurrent: async () => {} };
  await captureTerminalData(f.store, f.azure, authority, async () => {});
  const resource = { api_version: 'apps/v1', kind: 'Deployment', namespace: inventory.namespace.name, name: 'collector', uid: campaignId,
    absence: null, role: 'collector' as const, state: 'observed' as const };
  inventory.resources = [resource]; inventory.cleanup.terminal_capture = step;
  f.change({ runInventory: inventory });
  const origin = { campaign_id: campaignId, owner_id: ownerId, fencing_token: 1, orchestration_run_id: campaignId,
    cell_run_id: cellId, namespace: inventory.namespace };
  const identity = { api_version: resource.api_version, kind: resource.kind, name: resource.name, namespace: resource.namespace, uid: resource.uid };
  const pod = { name: 'collector-pod', uid: ownerId, resource_version: '1', containers: [{ name: 'main', exit_code: null,
    finished_at: null, oom_killed: false, restart_count: 0 }] };
  const ended = { ...pod, containers: [{ name: 'main', exit_code: exitCode, finished_at: '2026-01-01T00:00:00Z',
    oom_killed: false, restart_count: terminalRestart }] };
  const stop = { schema_version: 1, origin, field: 'bounded_drain', resource: identity, phase: 'complete',
    deadline_unix_ms: drain.deadline_unix_ms, expected_resource_version: '1', initial_desired: 1,
    initial_pods: [pod], terminal_pods: [ended], response: { source: 'patch_response', uid: campaignId,
      resource_version: '2', generation: 2, body_sha256: 'a'.repeat(64) }, observed_at_unix_ms: Date.now(),
    logs: [{ pod_name: pod.name, pod_uid: pod.uid, container: 'main', phase: 'terminal', observed_at_unix_ms: Date.now(),
      body_sha256: 'b'.repeat(64), bytes: 42, numeric_samples: [], redacted_lines: 0, drain_complete_marker: marker }] };
  const terminal = { schema_version: 1, origin, field: 'terminal_capture', scope: 'exact_kubernetes_status_and_sanitized_log_receipts',
    workloads: [{ identity, original_stop: stop }] };
  const captureName = `evidence/${cellId}/custody/${cellId}/terminal_capture.json`;
  f.blobs.set(captureName, { bytes: Buffer.from(JSON.stringify(terminal)), etag: '"terminal-capture"' });
  const input = { schema_version: 1, campaign_id: campaignId, orchestration_run_id: campaignId, cell_run_id: cellId,
    expected_etag: '"inventory-1"', actor: { mode: 'producer' as const, owner_id: ownerId, fencing_token: 1 } };
  const env = { CAMPAIGN_ID: campaignId, CAMPAIGN_OWNER_ID: ownerId, CAMPAIGN_FENCING_TOKEN: '1' };
  return { ...f, get state() { return f.state; }, get checkpointPuts() { return f.checkpointPuts; }, inventory, authority, input, env, terminal, captureName };
}

test('guarded shutdown finalization never accepts a caller matched claim as 143 reconciliation proof', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize, shutdownFinalizationRead, requireFinalizedCollectorStop } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 143);
  const inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
  const request = { ...f.input, terminal_capture: inputs.terminal_capture, delta_before: inputs.delta_before,
    delta_terminal: inputs.delta_terminal, reconciliation: { profile: 'single_hot_table', proof: null } };
  await assert.rejects(shutdownFinalize(f.store, f.azure, { ...request, reconciliation: { profile: 'single_hot_table', proof: { status: 'matched' } } }, f.env), /measured reconciliation proof/);
  const before = [...f.blobs.entries()].map(([key, value]) => [key, value.bytes.toString()] as const);
  const result = await shutdownFinalize(f.store, f.azure, request, f.env);
  assert.equal(result.verdicts[0]?.classification, 'exit_143_terminal_reconciliation_unavailable');
  assert.equal(result.blob, `_arrowcol-custody/campaigns/${campaignId}/runs/${campaignId}/cells/${cellId}/shutdown-finalization.json`);
  assert.deepEqual(await shutdownFinalizationRead(f.store, f.azure, f.input, f.env), result);
  const puts = f.checkpointPuts;
  assert.deepEqual(await shutdownFinalize(f.store, f.azure, request, f.env), result);
  assert.equal(f.checkpointPuts, puts, 'read existing finalization before considering another create');
  for (const [key, value] of before) assert.equal(f.blobs.get(key)?.bytes.toString(), value);
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, f.inventory, f.authority), /release and handoff withheld/);
});

test('shutdown finalization resolves a lost create by guarded readback and rejects changed identity', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize, shutdownFinalizationRead, requireFinalizedCollectorStop } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 0);
  const inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
  const request = { ...f.input, terminal_capture: inputs.terminal_capture, delta_before: inputs.delta_before,
    delta_terminal: inputs.delta_terminal, reconciliation: { profile: 'single_hot_table', proof: null } };
  await assert.rejects(shutdownFinalize(f.store, f.azure, { ...request, delta_before: { ...inputs.delta_before, etag: '"wrong"' } }, f.env), /reference changed/);
  f.lose();
  const result = await shutdownFinalize(f.store, f.azure, request, f.env);
  assert.equal(result.verdicts[0]?.classification, 'exit_0');
  assert.deepEqual(await shutdownFinalizationRead(f.store, f.azure, f.input, f.env), result);
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, f.inventory, f.authority), /release and handoff withheld/);
  await assert.rejects(shutdownFinalizationRead(f.store, f.azure, { ...f.input, expected_etag: '"stale"' }, f.env), CampaignConflictError);
  await assert.rejects(shutdownFinalizationRead(f.store, f.azure, { ...f.input, actor: { ...f.input.actor, fencing_token: 2 } }, f.env));
});

test('shutdown finalization cannot use a different instance or an unrelated marker', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize } = await import('./aks-shutdown-finalization.js');
  for (const [marker, restart, expected] of [[false, 0, 'exit_143_without_drain_marker'], [true, 1, 'terminal_status_unavailable']] as const) {
    const f = await shutdownFixture(t, 143, marker, restart);
    const inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
    const result = await shutdownFinalize(f.store, f.azure, { ...f.input, terminal_capture: inputs.terminal_capture,
      delta_before: inputs.delta_before, delta_terminal: inputs.delta_terminal,
      reconciliation: { profile: 'other', proof: null } }, f.env);
    assert.equal(result.verdicts[0]?.classification, expected);
  }
});

test('shutdown finalization reads conflicts first and rejects fenced lost-write recovery', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 0);
  const inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
  const request = { ...f.input, terminal_capture: inputs.terminal_capture, delta_before: inputs.delta_before,
    delta_terminal: inputs.delta_terminal, reconciliation: { profile: 'single_hot_table', proof: null } };
  const name = `_arrowcol-custody/campaigns/${campaignId}/runs/${campaignId}/cells/${cellId}/shutdown-finalization.json`;
  f.blobs.set(name, { bytes: Buffer.from(JSON.stringify({ conflicting: true })), etag: '"conflict"' });
  const puts = f.checkpointPuts;
  await assert.rejects(shutdownFinalize(f.store, f.azure, request, f.env));
  assert.equal(f.checkpointPuts, puts);
  assert.equal(f.blobs.get(name)?.bytes.toString(), '{"conflicting":true}');
  f.blobs.delete(name);
  f.lose(() => f.change({ fencingToken: 2, status: 'fenced', fencedAtMs: Date.now(), fenceReason: 'heartbeat-expired' }));
  await assert.rejects(shutdownFinalize(f.store, f.azure, request, f.env));
  assert.ok(f.blobs.has(name), 'unknown write may have persisted even though fenced readback cannot establish success');
  assert.equal(f.checkpointPuts, puts + 1);
});

test('shutdown inputs retain nullable missing sources without fabricating a baseline', async t => {
  const { shutdownFinalizationInputs } = await import('./aks-shutdown-finalization.js');
  for (const [suffix, key, reason] of [
    ['/terminal_capture.json', 'terminal_capture', 'terminal_capture_unavailable'],
    ['/delta-before.json', 'delta_before', 'no_baseline'],
    ['/delta-terminal.json', 'delta_terminal', 'terminal_delta_unavailable'],
  ] as const) {
    const f = await shutdownFixture(t, 143);
    f.blobs.delete([...f.blobs.keys()].find(name => name.endsWith(suffix))!);
    const inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
    assert.equal(inputs[key], null);
    assert.equal(inputs.unavailable_reason, reason);
  }
});

test('exit 0 and legacy release require a persisted reconciliation finalization', async t => {
  const { requireFinalizedCollectorStop } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 0);
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, f.inventory, f.authority), /finalization/);
  assert.ok(![...f.blobs.keys()].some(name => name.endsWith('/shutdown-finalization.json')));
  const legacy: RunInventory = structuredClone(f.inventory);
  legacy.evidence_destination = null;
  f.change({ runInventory: legacy });
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, legacy, f.authority), /release and handoff withheld/);
});

test('handoff rejects an inventory version race while reading terminal evidence', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize, requireFinalizedCollectorStop } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 0);
  const inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
  await shutdownFinalize(f.store, f.azure, { ...f.input, terminal_capture: inputs.terminal_capture,
    delta_before: inputs.delta_before, delta_terminal: inputs.delta_terminal,
    reconciliation: { profile: 'single_hot_table', proof: null } }, f.env);
  const read = f.store.read.bind(f.store);
  let calls = 0;
  t.mock.method(f.store, 'read', async () => ({ ...(await read())!, etag: ++calls < 3 ? '"inventory-1"' : '"inventory-2"' }));
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, f.inventory, f.authority), CampaignConflictError);
});

test('finalization readback cannot succeed after its authority changes during the read', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize, shutdownFinalizationRead } = await import('./aks-shutdown-finalization.js');
  for (const action of ['read', 'existing'] as const) {
    await t.test(action, async t => {
      const f = await shutdownFixture(t, 0);
      const inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
      const request = { ...f.input, terminal_capture: inputs.terminal_capture, delta_before: inputs.delta_before,
        delta_terminal: inputs.delta_terminal, reconciliation: { profile: 'single_hot_table', proof: null } };
      await shutdownFinalize(f.store, f.azure, request, f.env);
      const read = f.azure.blob.bind(f.azure);
      t.mock.method(f.azure, 'blob', async (...args: Parameters<Azure['blob']>) => {
        const result = await read(...args);
        if (args[0] === 'GET' && args[2]?.endsWith('/shutdown-finalization.json'))
          f.change({ fencingToken: 2, status: 'fenced', fencedAtMs: Date.now(), fenceReason: 'heartbeat-expired' });
        return result;
      });
      await assert.rejects(action === 'read' ? shutdownFinalizationRead(f.store, f.azure, f.input, f.env) :
        shutdownFinalize(f.store, f.azure, request, f.env));
    });
  }
});


test('production release and handoff guards refuse 143 before namespace, QAP or FIC effects', async t => {
  const f = await shutdownFixture(t, 143);
  let commands = 0;
  const defaults = createDefaultCleanupProviders(f.store, { azure: f.azure, workDirectory: '.',
    runCommand: async () => { commands++; throw new Error('No Kubernetes effect allowed.'); } });
  const fic = { resource_id: f.azure.c.base + '/fixture-fic', name: 'fixture-fic', state: 'observed' as const, absence: null };
  for (const effect of [
    () => defaults.deleteNamespaceIfUidAndOwnerMatch(f.inventory.namespace, 'namespace', f.authority),
    () => defaults.deleteQapBindingIfOwned(f.inventory.qap!.database, f.inventory.qap!.bindings[0]!, campaignId, 'qap', f.authority),
    () => defaults.deleteFicIfExact(fic, campaignId, 'fic', f.authority),
    () => defaults.verifyCellPreservation!(f.inventory, f.authority),
  ]) await assert.rejects(effect(), /release and handoff withheld/);
  assert.equal(commands, 0);
  assert.ok(![...f.blobs.keys()].some(name => name.endsWith('/shutdown-finalization.json')));
});

test('controller safe-idle guard refuses replacement on 143 before independent capacity observations', async t => {
  const f = await shutdownFixture(t, 143);
  f.change({ status: 'cleaning', mode: 'armed', fencingToken: 2, fencedAtMs: Date.now(), fenceReason: 'heartbeat-expired',
    cleanupStatus: 'running', cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({
      step, status: index === 0 ? 'running' : 'pending', idempotencyKey: 'guard-' + index, reasonCode: null })) });
  const defaults = createDefaultCleanupProviders(f.store, { azure: f.azure, workDirectory: '.' });
  await assert.rejects(defaults.verifySafeIdle(f.state, 'verify'), /release and handoff withheld/);
});

test('legacy restart fields still identify zero exits, but no-baseline and early setup withhold release', async t => {
  const { requireFinalizedCollectorStop } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 0), stop = f.terminal.workloads[0]!.original_stop;
  Reflect.deleteProperty(stop.initial_pods[0]!.containers[0]!, 'restart_count');
  Reflect.deleteProperty(stop.terminal_pods[0]!.containers[0]!, 'restart_count');
  f.blobs.set(f.captureName, { bytes: Buffer.from(JSON.stringify(f.terminal)), etag: '"legacy-capture"' });
  for (const name of [...f.blobs.keys()]) if (name.endsWith('/delta-before.json') || name.endsWith('/delta-terminal.json')) f.blobs.delete(name);
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, f.inventory, f.authority), /release and handoff withheld/);
  const early: RunInventory = { ...f.inventory, load_admission: 'not_authorized', resources: [], evidence_destination: null };
  f.change({ runInventory: early });
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, early, f.authority), /release and handoff withheld/);
});

test('missing terminal capsule cannot hide an observed legacy 143 stop journal', async t => {
  const { requireFinalizedCollectorStop } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 143), stop = f.terminal.workloads[0]!.original_stop;
  const sha = createHash('sha256').update(canonical_json(stop.resource)).digest('hex');
  f.blobs.set(`_arrowcol-custody/campaigns/${campaignId}/cells/${cellId}/lifecycle/bounded_drain-${sha}.json`,
    { bytes: Buffer.from(JSON.stringify(stop)), etag: '"original-stop"' });
  f.blobs.delete(f.captureName);
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, f.inventory, f.authority), /release and handoff withheld/);
  const legacy: RunInventory = { ...f.inventory, evidence_destination: null };
  f.change({ runInventory: legacy });
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, legacy, f.authority), /release and handoff withheld/);
});

test('read-only finalization recovery retains the creation ETag after inventory progress', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize, shutdownFinalizationRead } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 143), inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
  const receipt = await shutdownFinalize(f.store, f.azure, { ...f.input, terminal_capture: inputs.terminal_capture,
    delta_before: inputs.delta_before, delta_terminal: inputs.delta_terminal, reconciliation: { profile: 'other', proof: null } }, f.env);
  const read = f.store.read.bind(f.store);
  t.mock.method(f.store, 'read', async () => ({ ...(await read())!, etag: '"inventory-2"' }));
  const puts = f.checkpointPuts;
  assert.deepEqual(await shutdownFinalizationRead(f.store, f.azure, { ...f.input, expected_etag: '"inventory-2"' }, f.env), receipt);
  assert.equal(f.checkpointPuts, puts);
});

test('contradictory current pod exits cannot be hidden by an earlier zero-exit stop', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize, requireFinalizedCollectorStop } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 0), workload = f.terminal.workloads[0]!;
  const observed = structuredClone(workload.original_stop.terminal_pods);
  observed[0]!.containers[0]!.exit_code = 143;
  const capsule = { ...f.terminal, workloads: [{ ...workload, pods: observed }] };
  f.blobs.set(f.captureName, { bytes: Buffer.from(JSON.stringify(capsule)), etag: '"contradictory-capture"' });
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, f.inventory, f.authority), /release and handoff withheld/);
  const inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
  const receipt = await shutdownFinalize(f.store, f.azure, { ...f.input, terminal_capture: inputs.terminal_capture,
    delta_before: inputs.delta_before, delta_terminal: inputs.delta_terminal, reconciliation: { profile: 'other', proof: null } }, f.env);
  assert.equal(receipt.verdicts[0]?.classification, 'terminal_status_unavailable');
});

test('expired incomplete freeze is recorded unavailable without restarting its deadline', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 143);
  f.inventory.cleanup.terminal_capture = { state: 'running', deadline_unix_ms: Date.now() - 1, reason_code: null };
  f.change({ runInventory: f.inventory });
  const deadline = f.inventory.cleanup.terminal_capture.deadline_unix_ms;
  const inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
  const receipt = await shutdownFinalize(f.store, f.azure, { ...f.input, terminal_capture: inputs.terminal_capture,
    delta_before: inputs.delta_before, delta_terminal: inputs.delta_terminal, reconciliation: { profile: 'other', proof: null } }, f.env);
  assert.equal(receipt.status, 'unavailable');
  assert.equal(receipt.reason_code, 'terminal_capture_unavailable');
  assert.equal(receipt.release_allowed, false);
  assert.equal(f.inventory.cleanup.terminal_capture.deadline_unix_ms, deadline);
});


async function installMatchedFinalization(t: TestContext, f: Awaited<ReturnType<typeof shutdownFixture>>, rows = 7) {
  const { readDeltaReference } = await import('./aks-delta-preservation.js');
  const hot = f.inventory.qap!.bindings[0]!.relative_path, prefix = hot + '/_delta_log/';
  const name = prefix + '00000000000000000000.json';
  const bytes = Buffer.from([
    { protocol: { minReaderVersion: 1, minWriterVersion: 2 } },
    { metaData: { id: campaignId, format: { provider: 'parquet' },
      schemaString: '{"type":"struct","fields":[{"name":"value","type":"long","nullable":true,"metadata":{}}]}',
      partitionColumns: [], configuration: {} } },
    { commitInfo: { operation: 'CREATE TABLE', timestamp: Date.parse('2026-11-16T00:00:00Z') } },
    { add: { path: 'part.parquet', size: 100, dataChange: true, partitionValues: {}, stats: JSON.stringify({ numRecords: rows }) } },
  ].map(value => JSON.stringify(value)).join('\n') + '\n');
  const etag = '"' + createHash('sha256').update(bytes).digest('hex') + '"', read = f.azure.blob.bind(f.azure);
  let listing = 0;
  t.mock.method(f.azure, 'blob', async (...args: Parameters<Azure['blob']>) => {
    if (args[1] !== f.azure.c.data) return read(...args);
    assert.equal(args[0], 'GET');
    args[8]?.({ status: 200, bodyBase64: '', headers: {} });
    if (args[3]) {
      assert.equal(args[3].prefix, prefix);
      return [Buffer.from(`<EnumerationResults><Prefix>${prefix}</Prefix><Blobs><Blob><Name>${name}</Name>` +
        `<Properties><Etag>${etag.replaceAll('"', '&quot;')}</Etag><Content-Length>${bytes.length}</Content-Length></Properties>` +
        '</Blob></Blobs><NextMarker></NextMarker></EnumerationResults>'), { 'x-ms-request-id': 'final-table-' + ++listing }];
    }
    assert.equal(args[2], name);
    assert.equal(args[5]?.['If-Match'], etag);
    return [bytes, { etag }];
  });
  const delta = await readDeltaReference(f.azure, hot, async () => {});
  const terminalName = `_arrowcol-custody/campaigns/${campaignId}/runs/${campaignId}/cells/${cellId}/delta-terminal.json`;
  const terminal = JSON.parse(f.blobs.get(terminalName)!.bytes.toString());
  terminal.delta = delta;
  f.blobs.set(terminalName, { bytes: Buffer.from(JSON.stringify(terminal)), etag: '"matched-terminal"' });
  const { shutdownFinalizationInputs, shutdownFinalize } = await import('./aks-shutdown-finalization.js');
  const inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
  const proof = { status: 'matched' as const, post_teardown_verification: 'matched_final_delta_log' as const,
    expected_rows: rows, observed_rows: rows, settled_zero_ambiguity: true, teardown_admissions_clear: true };
  const request = { ...f.input, terminal_capture: inputs.terminal_capture, delta_before: inputs.delta_before,
    delta_terminal: inputs.delta_terminal, reconciliation: { profile: 'single_hot_table' as const, proof } };
  const receipt = await shutdownFinalize(f.store, f.azure, request, f.env);
  return { request, receipt, proof };
}

test('matched measured reconciliation validates only zero exits and permits guarded release after ETag progress', async t => {
  const { requireFinalizedCollectorStop } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 0), { receipt } = await installMatchedFinalization(t, f);
  assert.equal(receipt.status, 'validated');
  assert.equal(receipt.reason_code, null);
  assert.equal(receipt.release_allowed, true);
  const read = f.store.read.bind(f.store);
  t.mock.method(f.store, 'read', async () => ({ ...(await read())!, etag: '"inventory-2"' }));
  await requireFinalizedCollectorStop(f.store, f.azure, f.inventory, f.authority);
});

test('matched reconciliation never promotes 143 or mismatched frozen row counts', async t => {
  const { shutdownFinalize } = await import('./aks-shutdown-finalization.js');
  const signalled = await shutdownFixture(t, 143), retained = await installMatchedFinalization(t, signalled);
  assert.equal(retained.receipt.status, 'unavailable');
  assert.equal(retained.receipt.reason_code, 'exit_143_unavailable');
  assert.equal(retained.receipt.release_allowed, false);
  const zero = await shutdownFixture(t, 0), matched = await installMatchedFinalization(t, zero);
  zero.blobs.delete(matched.receipt.blob);
  const mismatch = await shutdownFinalize(zero.store, zero.azure, { ...matched.request,
    reconciliation: { ...matched.request.reconciliation, proof: { ...matched.proof, expected_rows: 8 } } }, zero.env);
  assert.equal(mismatch.status, 'failed');
  assert.equal(mismatch.reason_code, 'reconciliation_mismatch');
  assert.equal(mismatch.release_allowed, false);
});

test('no-baseline early failure records immutable unavailable refs with an absent namespace UID', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize, shutdownFinalizationRead, requireFinalizedCollectorStop } = await import('./aks-shutdown-finalization.js');
  const f = fixture(t), step = { state: 'complete' as const, deadline_unix_ms: null, reason_code: null };
  const inventory: RunInventory = { ...f.inventory, namespace: { ...f.inventory.namespace, uid: null }, resources: [], qap: null,
    cleanup: { stop_admission: step, bounded_drain: step, evidence_capture: step, stop_maintenance: step, terminal_capture: step, handoff: f.inventory.cleanup.handoff } };
  f.change({ runInventory: inventory });
  const input = { schema_version: 1, campaign_id: campaignId, orchestration_run_id: campaignId, cell_run_id: cellId,
    expected_etag: '"inventory-1"', actor: { mode: 'producer' as const, owner_id: ownerId, fencing_token: 1 } };
  const env = { CAMPAIGN_ID: campaignId, CAMPAIGN_OWNER_ID: ownerId, CAMPAIGN_FENCING_TOKEN: '1' };
  const inputs = await shutdownFinalizationInputs(f.store, f.azure, input, env);
  assert.equal(inputs.unavailable_reason, 'no_baseline');
  assert.equal(inputs.terminal_capture, null); assert.equal(inputs.delta_before, null); assert.equal(inputs.delta_terminal, null);
  const request = { ...input, terminal_capture: null, delta_before: null, delta_terminal: null,
    reconciliation: { profile: 'other', proof: null } };
  const receipt = await shutdownFinalize(f.store, f.azure, request, env);
  assert.equal(receipt.origin.namespace.uid, null);
  assert.equal(receipt.status, 'unavailable'); assert.equal(receipt.reason_code, 'no_baseline'); assert.equal(receipt.release_allowed, false);
  assert.deepEqual(await shutdownFinalizationRead(f.store, f.azure, input, env), receipt);
  const puts = f.checkpointPuts;
  assert.deepEqual(await shutdownFinalize(f.store, f.azure, request, env), receipt);
  assert.equal(f.checkpointPuts, puts);
  const authority: CleanupMutationAuthority = { mode: 'producer', campaignId, ownerId, fencingToken: 1, assertCurrent: async () => {} };
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, inventory, authority), /release and handoff withheld/);
  await captureTerminalData(f.store, f.azure, authority, async () => {});
  assert.ok(![...f.blobs.keys()].some(name => name.endsWith('/delta-terminal.json')));
});

test('an existing baseline with no measured rows is reconciliation-unavailable, not no-baseline', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 0), inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
  assert.equal(inputs.unavailable_reason, null);
  const receipt = await shutdownFinalize(f.store, f.azure, { ...f.input, terminal_capture: inputs.terminal_capture,
    delta_before: inputs.delta_before, delta_terminal: inputs.delta_terminal, reconciliation: { profile: 'single_hot_table', proof: null } }, f.env);
  assert.equal(receipt.status, 'unavailable'); assert.equal(receipt.reason_code, 'reconciliation_unavailable');
  assert.equal(receipt.release_allowed, false);
});

test('source ETag changes before publication cannot create a finalization artifact', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 143), inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
  const read = f.azure.blob.bind(f.azure), puts = f.checkpointPuts;
  let reads = 0;
  t.mock.method(f.azure, 'blob', async (...args: Parameters<Azure['blob']>) => {
    const result = await read(...args);
    if (args[0] === 'GET' && args[2] === f.captureName && ++reads === 1)
      f.blobs.set(f.captureName, { ...f.blobs.get(f.captureName)!, etag: '"changed-capture"' });
    return result;
  });
  await assert.rejects(shutdownFinalize(f.store, f.azure, { ...f.input, terminal_capture: inputs.terminal_capture,
    delta_before: inputs.delta_before, delta_terminal: inputs.delta_terminal, reconciliation: { profile: 'other', proof: null } }, f.env),
    CampaignConflictError);
  assert.equal(f.checkpointPuts, puts);
});

test('storage denial is not an authoritative missing-baseline observation', async t => {
  const { shutdownFinalizationInputs } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 0), read = f.azure.blob.bind(f.azure), puts = f.checkpointPuts;
  t.mock.method(f.azure, 'blob', async (...args: Parameters<Azure['blob']>) => {
    if (args[0] === 'GET' && args[2]?.endsWith('/delta-before.json')) throw new Error('403 permission denied');
    return read(...args);
  });
  await assert.rejects(shutdownFinalizationInputs(f.store, f.azure, f.input, f.env), /403 permission denied/);
  assert.equal(f.checkpointPuts, puts);
});
