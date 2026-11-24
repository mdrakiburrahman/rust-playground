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
  let requests = 0, checkpointPuts = 0, denyStorage = false, loseWrite = false, onLostWrite: (() => void) | undefined;
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
    checkpointPuts++;
    const etag = `"data-${blobs.size + 1}"`;
    blobs.set(name, { bytes: Buffer.from(String(body)), etag });
    if (loseWrite) { onLostWrite?.(); throw new Error('response lost after storage applied write'); }
    return [null, { ':status': '201', etag }];
  });
  const input = { schema_version: 1, campaign_id: campaignId, orchestration_run_id: campaignId, cell_run_id: cellId,
    expected_etag: '"inventory-1"', hot_delta_relative_path: hot, actor: { mode: 'producer', owner_id: ownerId, fencing_token: 1 } };
  return { azure, store, input, blobs, get requests() { return requests; }, get checkpointPuts() { return checkpointPuts; }, get state() { return structuredClone(state); },
    change: (update: Partial<CampaignState>) => { state = { ...state, ...update }; },
    inventory, deny: () => { denyStorage = true; }, lose: (callback?: () => void) => { loseWrite = true; onLostWrite = callback; } };
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
  let countResponse: Buffer | undefined;
  t.mock.method(f.azure, 'request', async (_method: string, url: string, _resource: string, _body?: string | Buffer | null,
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
  f.change({ status: 'cleaning', mode: 'armed', fencingToken: 2, fenceReason: 'heartbeat-expired', fencedAtMs: Date.now(),
    cleanupStatus: 'running', cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({
      step, idempotencyKey: `${campaignId}:2:${step}`, status: index === CUSTODY_CLEANUP_STEPS.length - 1 ? 'running' : 'complete',
    })) });
  const defaults = createDefaultCleanupProviders(f.store, { azure: f.azure, workDirectory: '.',
    readHealth: async () => ({ observed_at_unix_ms: Date.now(), pods: [{ name: 'stamp/healthy', ready: true, scheduled: true }],
      pdb_blocked: false, versions: [] }) });
  assert.equal(await defaults.verifySafeIdle(f.state, 'verify'), 'complete', 'installed real data provider must compose with independent capacity, not numeric zero stand-ins');
  const baselineBlob = `_arrowcol-custody/campaigns/${campaignId}/baseline.json`, baseline = f.blobs.get(baselineBlob)!;
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
      t.mock.method(f.azure, 'kql', async (command: string) => {
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
      await assert.rejects(defaults.verifySafeIdle(f.state, 'verify'),
        /enabled policy changed|non_run_qap_unhealthy_or_backlog_growing/);
      assert.equal(sample, 1, 'both exact QAP samples must be observed');
    });
  }
  baseline.bytes = originalBaseline;
  f.blobs.delete([...f.blobs.keys()].find(name => name.endsWith('/delta-terminal.json'))!);
  await assert.rejects(defaults.verifySafeIdle(f.state, 'verify'), /absent or unversioned/);
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
  return { ...f, get checkpointPuts() { return f.checkpointPuts; }, inventory, authority, input, env, terminal, captureName };
}

test('guarded shutdown finalization never accepts a caller matched claim as 143 reconciliation proof', async t => {
  const { shutdownFinalizationInputs, shutdownFinalize, shutdownFinalizationRead, requireFinalizedCollectorStop } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 143);
  const inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
  const request = { ...f.input, terminal_capture: inputs.terminal_capture, delta_before: inputs.delta_before,
    delta_terminal: inputs.delta_terminal, reconciliation: { profile: 'single_hot_table', proof: { status: 'matched' } } };
  const before = [...f.blobs.entries()].map(([key, value]) => [key, value.bytes.toString()] as const);
  const result = await shutdownFinalize(f.store, f.azure, request, f.env);
  assert.equal(result.verdicts[0]?.classification, 'exit_143_terminal_reconciliation_unavailable');
  assert.equal(result.blob, `_arrowcol-custody/campaigns/${campaignId}/runs/${campaignId}/cells/${cellId}/shutdown-finalization.json`);
  assert.deepEqual(await shutdownFinalizationRead(f.store, f.azure, f.input, f.env), result);
  const puts = f.checkpointPuts;
  assert.deepEqual(await shutdownFinalize(f.store, f.azure, request, f.env), result);
  assert.equal(f.checkpointPuts, puts, 'read existing finalization before considering another create');
  for (const [key, value] of before) assert.equal(f.blobs.get(key)?.bytes.toString(), value);
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, f.inventory, f.authority), /do not hand off/);
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
  await requireFinalizedCollectorStop(f.store, f.azure, f.inventory, f.authority);
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

test('shutdown inputs require each immutable source', async t => {
  const { shutdownFinalizationInputs } = await import('./aks-shutdown-finalization.js');
  for (const suffix of ['/terminal_capture.json', '/delta-before.json', '/delta-terminal.json']) {
    const f = await shutdownFixture(t, 143);
    const name = [...f.blobs.keys()].find(key => key.endsWith(suffix))!;
    assert.ok(name);
    f.blobs.delete(name);
    await assert.rejects(shutdownFinalizationInputs(f.store, f.azure, f.input, f.env));
  }
});

test('exit 0 and legacy cleanup do not acquire a new finalization dependency', async t => {
  const { requireFinalizedCollectorStop } = await import('./aks-shutdown-finalization.js');
  const f = await shutdownFixture(t, 0);
  await requireFinalizedCollectorStop(f.store, f.azure, f.inventory, f.authority);
  assert.ok(![...f.blobs.keys()].some(name => name.endsWith('/shutdown-finalization.json')));
  const legacy: RunInventory = structuredClone(f.inventory);
  legacy.evidence_destination = null;
  f.change({ runInventory: legacy });
  await requireFinalizedCollectorStop(f.store, f.azure, legacy, f.authority);
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

test('guarded numeric captures bind pre-arm membership, preserve sources and never infer lossless continuity', async t => {
  const { shutdownMembershipCapture, shutdownReceiptCapture, shutdownFollowerPublish, shutdownFinalizationInputs, shutdownFinalize,
    shutdownFinalizationRead, requireFinalizedCollectorStop } = await import('./aks-shutdown-finalization.js');
  const { createDefaultLifecycleActions } = await import('./aks-lifecycle.js');
  const f = await shutdownFixture(t, 143);
  const clientUid = '44444444-4444-4444-8444-444444444444';
  const clientPodUid = '55555555-5555-4555-8555-555555555555';
  const client = { api_version: 'batch/v1', kind: 'Job', namespace: f.inventory.namespace.name, name: 'client-job',
    uid: clientUid, absence: null, role: 'client' as const, state: 'observed' as const };
  const schedule = { api_version: 'v1', kind: 'ConfigMap', namespace: f.inventory.namespace.name, name: 'oat-schedule',
    uid: cellId, absence: null, role: 'router' as const, state: 'observed' as const };
  f.inventory.resources.push(client);
  f.change({ runInventory: f.inventory });
  const original = f.terminal.workloads[0].original_stop;
  const clientIdentity = { api_version: client.api_version, kind: client.kind, namespace: client.namespace, name: client.name, uid: client.uid };
  const initialClient = { ...original.initial_pods[0], name: 'client-pod', uid: clientPodUid };
  const terminalClient = { ...initialClient, containers: [{ ...initialClient.containers[0], exit_code: 0,
    finished_at: '2026-01-01T00:00:00Z' }] };
  f.blobs.get(f.captureName)!.bytes = Buffer.from(JSON.stringify({ ...f.terminal, workloads: [
    ...f.terminal.workloads, { identity: clientIdentity, original_stop: { ...original, resource: clientIdentity,
      field: 'stop_admission', deadline_unix_ms: null, initial_pods: [initialClient], terminal_pods: [terminalClient] } },
  ] }));
  const collectorIdentity = f.terminal.workloads[0].identity;
  const members = [
    { kind: 'collector' as const, workload: collectorIdentity, pod_name: 'collector-pod', pod_uid: ownerId, container: 'main', restart_count: 0 },
    { kind: 'client' as const, workload: clientIdentity, pod_name: 'client-pod', pod_uid: clientPodUid, container: 'main', restart_count: 0 },
  ];
  const lines = (value: string) => '2026-01-01T00:00:00Z ' + value + '\n';
  const collectorLogs = lines('arrowcol_effective_config ' + JSON.stringify({ commit_telemetry:
    { schema_version: 1, scope: 'new_ingestion_addfiles_after_sync', clock: 'system_unix_ms' } })) +
    lines('arrowcol_data_commit ' + JSON.stringify({ schema_version: 1, outcome: 'committed', table_id: 'a'.repeat(64),
      table_name: 'telemetry.sqlserver.hot_table', delta_version: 1, rows: 13, files: 1, parquet_bytes: 100 })) +
    lines('arrowcol_shutdown {"event":"drain_complete","cancelled_ambiguous":0}') +
    lines('never-retain-this-secret=hidden');
  const clientLogs = lines('OAT_CLIENT_METRICS ' + JSON.stringify({ version: 1, scope: 'process_cumulative',
    phase: 'measurement_start', workers: [{ worker: 0, completed_rows: 0, ambiguous_rows: 0 }] })) +
    lines('OAT_CLIENT_METRICS ' + JSON.stringify({ version: 1, scope: 'process_cumulative',
      phase: 'completed', workers: [{ worker: 0, completed_rows: 13, ambiguous_rows: 0 }] }));
  let observations = 0;
  const lifecycle = { ...createDefaultLifecycleActions(f.store, f.azure),
    observeShutdownMembership: async () => { observations++; return { schedule: { name: schedule.name, uid: schedule.uid,
      resource_version: '1', body_sha256: 'b'.repeat(64) }, members }; },
    readShutdownLogs: async (_inventory: RunInventory, member: { pod_uid: string }) => {
      const text = member.pod_uid === ownerId ? collectorLogs : clientLogs;
      return { text, body_sha256: createHash('sha256').update(text).digest('hex'), bytes: Buffer.byteLength(text) };
    } };
  const membershipRequest = { ...f.input, schedule_name: schedule.name, table_root: f.inventory.qap!.bindings[0].relative_path,
    collector_table_id: 'a'.repeat(64), collector_table_name: 'telemetry.sqlserver.hot_table' };
  const membership = await shutdownMembershipCapture(f.store, f.azure, membershipRequest, f.env, lifecycle);
  assert.deepEqual(await shutdownMembershipCapture(f.store, f.azure, membershipRequest, f.env, lifecycle), membership);
  assert.equal(observations, 1, 'retry reads the immutable pre-arm observation');
  const diagnosticRequest = { ...f.input, membership, pod_uid: ownerId, container: 'main', restart_count: 0,
    source: { sha256: 'c'.repeat(64), bytes: 20, lines: 1 }, commits: [{ delta_version: 1, rows: 13, files: 1, parquet_bytes: 100 }],
    gaps: ['terminal_unbound'] };
  f.lose();
  const diagnostic = await shutdownFollowerPublish(f.store, f.azure, diagnosticRequest, f.env);
  const diagnosticValue = JSON.parse(f.blobs.get(diagnostic.blob)!.bytes.toString());
  assert.equal(diagnosticValue.status, 'diagnostic_only');
  assert.equal(diagnosticValue.continuity, 'unproven');
  assert.deepEqual(diagnosticValue.gaps, ['continuity_unproven', 'terminal_unbound']);
  const diagnosticPuts = f.checkpointPuts;
  assert.deepEqual(await shutdownFollowerPublish(f.store, f.azure, diagnosticRequest, f.env), diagnostic);
  assert.equal(f.checkpointPuts, diagnosticPuts, 'diagnostic unknown write resolved by readback without replay');
  await assert.rejects(shutdownFollowerPublish(f.store, f.azure,
    { ...diagnosticRequest, commits: [{ ...diagnosticRequest.commits[0], raw: 'secret' }] }, f.env), /unsupported fields/);
  await assert.rejects(shutdownFollowerPublish(f.store, f.azure,
    { ...diagnosticRequest, expected_etag: '"stale"' }, f.env), CampaignConflictError);

  const refs = [];
  for (const member of members) {
    const request = { ...f.input, membership, kind: member.kind, pod_uid: member.pod_uid,
      container: member.container, restart_count: member.restart_count };
    const ref = await shutdownReceiptCapture(f.store, f.azure, request, f.env, lifecycle);
    refs.push(ref);
    const puts = f.checkpointPuts;
    assert.deepEqual(await shutdownReceiptCapture(f.store, f.azure, request, f.env, lifecycle), ref);
    assert.equal(f.checkpointPuts, puts);
    const bytes = f.blobs.get(ref.blob)!.bytes.toString();
    assert.ok(!bytes.includes('never-retain-this-secret'));
    assert.equal(JSON.parse(bytes).projection.continuity, 'unproven');
  }
  const proof = { schema_version: 1, membership, collector_receipts: [refs[0]], client_receipts: [refs[1]] };
  const inputs = await shutdownFinalizationInputs(f.store, f.azure, f.input, f.env);
  const result = await shutdownFinalize(f.store, f.azure, { ...f.input, terminal_capture: inputs.terminal_capture,
    delta_before: inputs.delta_before, delta_terminal: inputs.delta_terminal,
    reconciliation: { profile: 'single_hot_table', proof } }, f.env);
  assert.equal(result.verdicts[0].classification, 'exit_143_terminal_reconciliation_unavailable');
  const puts = f.checkpointPuts;
  assert.deepEqual(await shutdownFinalizationRead(f.store, f.azure, f.input, f.env), result);
  assert.equal(f.checkpointPuts, puts, 'read-only reconciliation cannot publish or retry a source');
  await assert.rejects(requireFinalizedCollectorStop(f.store, f.azure, f.inventory, f.authority), /do not hand off/);
  await assert.rejects(shutdownReceiptCapture(f.store, f.azure, { ...f.input, membership, kind: 'client', pod_uid: clientPodUid,
    container: 'main', restart_count: 1 }, f.env, lifecycle), /instance unavailable/);
  await assert.rejects(shutdownMembershipCapture(f.store, f.azure,
    { ...membershipRequest, expected_etag: '"stale"' }, f.env, lifecycle), CampaignConflictError);
});

test('numeric reconciliation distinguishes exact, per-version mismatch and unavailable counter bounds', async () => {
  const { compareShutdownNumbers, projectShutdownLogs, parseShutdownProof } = await import('./aks-shutdown-proof.js');
  const version = { delta_version: 7, rows: 13, files: 1, parquet_bytes: 100 };
  assert.equal(compareShutdownNumbers([version], [[version]], [{ completed_rows: 13, ambiguous_rows: 0 }]), 'exact');
  assert.equal(compareShutdownNumbers([version], [[{ ...version, parquet_bytes: 101 }]],
    [{ completed_rows: 13, ambiguous_rows: 0 }]), 'mismatch');
  assert.equal(compareShutdownNumbers([version], [[version]], [{ completed_rows: 13, ambiguous_rows: 1 }]), 'mismatch');
  assert.equal(compareShutdownNumbers([version], [[version]], [{ completed_rows: Number.MAX_SAFE_INTEGER, ambiguous_rows: 0 },
    { completed_rows: 1, ambiguous_rows: 0 }]), 'unavailable');
  assert.throws(() => parseShutdownProof({ matched: true }));
  const projection = projectShutdownLogs('client',
    '2026-01-01T00:00:00Z OAT_CLIENT_METRICS {"version":1,"scope":"process_cumulative","phase":"completed","workers":[{"worker":0,"completed_rows":13}]}\n',
    'a'.repeat(64), 'telemetry.sqlserver.hot_table');
  assert.equal(projection.ambiguous_rows, null, 'missing ambiguity is not zero');
  assert.equal(projection.continuity, 'unproven');
});
