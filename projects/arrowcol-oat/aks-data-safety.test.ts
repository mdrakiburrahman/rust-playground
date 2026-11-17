import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { test, type TestContext } from 'node:test';
import { Azure, Config, TAGS, canonical_json, type HttpReceipt } from '../../infra/azure/bootstrap.js';
import { CampaignConflictError, CampaignWriteOutcomeUnknownError, CUSTODY_CLEANUP_STEPS, newCampaignState, type CampaignState, type CampaignStore, type RunInventory } from './aks-campaign.js';
 import { captureCellBaseline, captureTerminalData, observeDataPreservation } from './aks-data-safety.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';
import { createDefaultCleanupProviders } from './aks-default-providers.js';
import { PLANNED_CONTROL_OPERATION_IDS, PLANNED_STORAGE_OPERATION_IDS, type OwnedRunTablePlan } from './aks-d47-bridge.js';

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
function ownedRunTableReceipt(azure: Azure, deadline: number): { plan: OwnedRunTablePlan; parent: Record<string, unknown> } {
  const generation = '44444444-4444-4444-8444-444444444444', publisher = '55555555-5555-4555-8555-555555555555';
  const leases: [string, string] = ['66666666-6666-4666-8666-666666666666', '77777777-7777-4777-8777-777777777777'];
  const tableId = '88888888-8888-4888-8888-888888888888', txnId = '99999999-9999-4999-8999-999999999999';
  const relative = 'dataflow/telemetry/sqlserver/sqlserver_availability_replica_states/';
  const campaignRoot = 'abfss://' + azure.c.checkpoints + '@' + azure.c.account + '.dfs.core.windows.net/campaigns/' + campaignId + '/';
  const runRoot = campaignRoot + 'runs/' + cellId + '/';
  const timestamp = Date.now(), createdTime = timestamp - 1;
  const schema = JSON.stringify({ type: 'struct', fields: [{ name: '__arrowcol_ingestion', type: 'string', nullable: false, metadata: {} }],
    nullable: false, metadata: {} });
  const body = [
    { commitInfo: { engineInfo: 'offline', kernelVersion: 'v0.29.0', operation: 'CREATE TABLE', operationParameters: {}, timestamp, txnId } },
    { protocol: { minReaderVersion: 3, minWriterVersion: 7, readerFeatures: [], writerFeatures: ['invariants'] } },
    { metaData: { configuration: {}, createdTime, format: { provider: 'parquet', options: {} }, id: tableId,
      partitionColumns: ['__arrowcol_ingestion'], schemaString: schema } },
  ].map(action => JSON.stringify(action)).join('\n') + '\n';
  const plan: OwnedRunTablePlan = { account: azure.c.account, container: azure.c.checkpoints, campaign_root: campaignRoot, run_root: runRoot,
    campaign_id: campaignId, run_id: cellId, generation, publisher_id: publisher, control_lease_ids: leases,
    control_operation_ids: [...PLANNED_CONTROL_OPERATION_IDS], storage_operation_ids: [...PLANNED_STORAGE_OPERATION_IDS],
    table: 'telemetry.sqlserver.sqlserver_availability_replica_states', partition_by_ingestion_date: true, owner_id: ownerId,
    fencing_token: 1, delta_v0: { body, body_sha256: createHash('sha256').update(body).digest('hex'), timestamp, txn_id: txnId,
      table_id: tableId, created_time: createdTime } };
  const authority = { campaign_id: campaignId, cell_run_id: cellId, owner_id: ownerId, fencing_token: 1,
    storage_account: azure.c.account, table_root_url: new URL(relative, runRoot).href,
    control_container_url: 'https://' + azure.c.account + '.blob.core.windows.net/' + azure.c.checkpoints,
    sandbox_id: azure.c.sandbox, reset_generation: generation };
  const delta = { table_id: tableId, version: 0, schema_json: schema, partition_columns: ['__arrowcol_ingestion'], active_files: 0 };
  return { plan, parent: { schema_version: 1, kind: 'owned_run_table_parent_receipt',
    artifact_identity: { source_commit: 'acd49e82bd4ba6e053ddf4bd641407b35fa65ba5',
      executable_sha256: 'c02241f65bc8ed3b204a0ab5d082cad853f02a0fa725a445356d85c9de2e5e98' }, exchange_count: 1,
    bridge_receipt: { schema_version: 1, kind: 'owned_run_table_receipt', authority, original_deadline_unix_ms: deadline,
      receipt: { allocation: plan, delta, publisher_drained: true } } } };
}

test('producer no-load preservation accepts only the pinned fresh zero-file table and persisted absence receipts', async t => {
  const f = fixture(t), now = Date.now(), namespace = f.inventory.namespace.name;
  const deadline = Math.min(f.state.expiresAtMs, now + 60_000), owned = ownedRunTableReceipt(f.azure, deadline);
  const step = { state: 'complete' as const, deadline_unix_ms: deadline, reason_code: null };
  const ficPlan = { version: 1, runId: cellId, namespace, serviceAccount: 'fixture-sa',
    identityId: f.azure.c.identity_id, resourceId: f.azure.c.identity_id + '/federatedIdentityCredentials/arrowcol-run-' + cellId,
    clientId: ownerId, tenantId: ownerId, aksId: f.azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + f.azure.c.cluster,
    server: 'https://fixture.example', credentialName: 'arrowcol-run-' + cellId, issuer: 'https://issuer.example',
    subject: 'system:serviceaccount:' + namespace + ':fixture-sa', audience: 'api://AzureADTokenExchange' } satisfies NonNullable<RunInventory['fic']>['plan'];
  const fic: NonNullable<RunInventory['fic']> = { resource_id: ficPlan.resourceId, name: ficPlan.credentialName,
    state: 'absent', absence: { resource_id: ficPlan.resourceId, name: ficPlan.credentialName, observed_at_unix_ms: now }, plan: ficPlan };
  const resource = { api_version: 'apps/v1', kind: 'Deployment', namespace, name: 'arrowcol', uid: null, role: 'collector' as const,
    state: 'absent' as const, absence: { api_version: 'apps/v1', kind: 'Deployment', namespace, name: 'arrowcol',
      namespace_uid: cellId, observed_at_unix_ms: now } };
  const resources: RunInventory['resources'] = [resource];
  const inventory: RunInventory = { ...f.inventory, qap: null, namespace: { ...f.inventory.namespace, uid: cellId }, resources,
    evidence_destination: { account: f.azure.c.account, container: f.azure.c.checkpoints, blob_prefix: 'campaign/' + cellId },
    owned_run_table_initialization: owned.parent, fic, cleanup: { ...f.inventory.cleanup, bounded_drain: step } };
  const authority: CleanupMutationAuthority = { mode: 'producer', campaignId, ownerId, fencingToken: 1, assertCurrent: async () => {} };
  f.change({ runInventory: { ...inventory, resources: [{ ...resources[0]!, absence: null }] } });
  await assert.rejects(observeDataPreservation(f.store, f.azure, f.state, authority), /persisted inventory.*absence/);
  f.change({ runInventory: inventory });
  const proof = await observeDataPreservation(f.store, f.azure, f.state, authority);
  assert.equal(proof.status, 'preserved');
  assert.equal(proof.delta.reason, 'owned_run_table_empty_no_load');
  assert.deepEqual(proof.owned_run_table_initialization, owned.parent);
});

test('qap-null producer preservation refuses without the owned Delta receipt and persisted absence proofs', async t => {
  const f = fixture(t), namespace = f.inventory.namespace.name;
  const ficPlan = { version: 1, runId: cellId, namespace, serviceAccount: 'fixture-sa',
    identityId: f.azure.c.identity_id, resourceId: f.azure.c.identity_id + '/federatedIdentityCredentials/arrowcol-run-' + cellId,
    clientId: ownerId, tenantId: ownerId, aksId: f.azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + f.azure.c.cluster,
    server: 'https://fixture.example', credentialName: 'arrowcol-run-' + cellId, issuer: 'https://issuer.example',
    subject: 'system:serviceaccount:' + namespace + ':fixture-sa', audience: 'api://AzureADTokenExchange' } satisfies NonNullable<RunInventory['fic']>['plan'];
  const step = { state: 'complete' as const, deadline_unix_ms: Date.now() + 60_000, reason_code: null };
  const resourceId = ficPlan.resourceId, ficName = ficPlan.credentialName;
  const inventory: RunInventory = { ...f.inventory, qap: null, namespace: { ...f.inventory.namespace, uid: cellId },
    evidence_destination: { account: f.azure.c.account, container: f.azure.c.checkpoints, blob_prefix: 'campaign/' + cellId },
    fic: { resource_id: resourceId, name: ficName, state: 'absent', absence: { resource_id: resourceId, name: ficName,
      observed_at_unix_ms: Date.now() }, plan: ficPlan },
    cleanup: { ...f.inventory.cleanup, bounded_drain: step } };
  f.change({ runInventory: inventory });
  const authority: CleanupMutationAuthority = { mode: 'producer', campaignId, ownerId, fencingToken: 1, assertCurrent: async () => {} };
  await assert.rejects(observeDataPreservation(f.store, f.azure, f.state, authority), /owned Delta v0 receipt/);
});
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
