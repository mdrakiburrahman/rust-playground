import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { test, type TestContext } from 'node:test';
import { Azure, Config, TAGS, canonical_json, runtime, type HttpReceipt } from './bootstrap.js';
import { managementRows, parseKustoQueryResult, parseQualifiedDeltaDefinition,
  assertExactConnectionStrings, validateExternalMutationReceipt,
  cleanup_oat_qap, oat_qap_reference, parse_oat_qap_record, recover_oat_qap_record, recover_oat_workload_stop,
  validate_oat_workload_stop, validate_experiment_database_resource, validate_oat_manifest, type RecordV1 } from './oat_qap.js';

const id = '22222222-2222-4222-8222-222222222222';
const run = '9dcd6847-2a56-45a3-bd3c-54af839f451e';
const name = `oat_${run.replaceAll('-', '')}_sqlserver_synthetic_table`;
const relative = `synthetic/runs/${run}/dataflow/telemetry/sqlserver/sqlserver_synthetic_table`;
const manifest = { schema_version: 1, run_id: run, database: 'experimentqap', tables: [{ external_table: name, relative_path: relative }] };

function recoveryFixture(t: TestContext) {
  const c = new Config({ AZURE_SUBSCRIPTION_ID: id }), azure = new Azure(c);
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'oat-qap-recovery-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const prefix = [c.table_prefix, 'runs', run].filter(Boolean).join('/');
  const storagePath = `${prefix}/dataflow/telemetry/sqlserver/sqlserver_synthetic_table`;
  const originalManifest = { ...manifest, schema_version: 1 as const, database: c.experimentDatabase,
    tables: [{ external_table: name, relative_path: storagePath }] };
  const manifestHash = createHash('sha256').update(canonical_json(originalManifest)).digest('hex');
  const nonce = '22222222-2222-4222-8222-222222222223';
  const environment = { account: c.account, container: c.data, database: c.experimentDatabase,
    tablePrefix: c.table_prefix, sandbox: c.sandbox, clusterId: c.cluster_id, storageId: c.storage_id };
  const marker = { schema_version: 1, run_id: run, nonce, manifest_sha256: manifestHash, environment };
  const owner = `arrowcol-oat:${run}:${nonce}:${manifestHash}`;
  const connection = `abfss://${c.data}@${c.account}.dfs.core.windows.net/${storagePath};managed_identity=${id}`;
  const policy = { IsEnabled: true as const, Hot: `${c.hot}.00:00:00`, ManagedIdentity: id };
  const receipt = (rows: Record<string, unknown>[]): string => canonical_json({ status: 200, headers: {},
    bodyBase64: Buffer.from(JSON.stringify({ Tables: [{
      Columns: Object.keys(rows[0]).map(ColumnName => ({ ColumnName, ColumnType: 'string' })),
      Rows: rows.map(row => Object.values(row)),
    }] })).toString('base64') });
  const record: RecordV1 = { schema_version: 1, run_id: run, nonce, manifest: originalManifest,
    manifest_sha256: manifestHash, environment, identity: id, policy,
    rootMarker: { path: prefix + '/_arrowcol-oat-run.json', value: marker, verified: true },
    entries: [{ name, storagePath, connection, state: 'ready',
      snapshot: { kind: 'delta', name, connectionStrings: [connection], docString: owner,
        schema: { OrderedColumns: [{ Name: 'value', Type: 'long' }] },
        qapPolicy: policy, definition: externalDefinition({ DocString: owner, ConnectionStrings: [connection] }) } }],
    intent: null, receipts: [
      { operation: 'marker', name: prefix + '/_arrowcol-oat-run.json',
        evidence: canonical_json({ status: 201, headers: { etag: '"marker-1"' }, bodyBase64: '' }) },
      { operation: 'create', name, evidence: receipt([{ TableName: name, TableType: 'Delta', DocString: owner,
        Properties: JSON.stringify(deltaProperties) }]) },
      { operation: 'policy', name, evidence: receipt([{ EntityName: `[${c.experimentDatabase}].[${name}]`,
        EntityType: 'ExternalTable', Policy: JSON.stringify(policy) }]) },
    ], state: 'ready',
    workload: { version: 1, runId: run, ownershipNonce: nonce, namespace: 'oat-' + run.replaceAll('-', ''),
      namespaceUid: id, namespaceOwner: 'fixture-private-owner-token', serviceAccount: 'oat', serviceAccountUid: id,
      identityId: c.identity_id, clientId: id, tenantId: id,
      aksId: c.base + '/providers/Microsoft.ContainerService/managedClusters/' + c.cluster,
      server: 'https://fixture.azmk8s.io', credentialName: 'arrowcol-run-' + run,
      issuer: 'https://fixture.oic.prod-aks.azure.com/tenant/cluster/',
      subject: `system:serviceaccount:oat-${run.replaceAll('-', '')}:oat`,
      audience: 'api://AzureADTokenExchange', phase: 'ready' }, bound_at_unix_ms: 1 };
  const leaseId = '22222222-2222-4222-8222-222222222224';
  const shared = { schema_version: 1, owner: marker, custody: record, lease: {
    id: leaseId, stage: 'released', run_id: run,
    acquireReceipt: { ':status': '201', 'x-ms-lease-id': leaseId }, releaseReceipt: { ':status': '200' },
    originalAcquire: { status: 201, headers: { 'x-ms-lease-id': leaseId }, bodyBase64: '' },
    originalRelease: { status: 200, headers: {}, bodyBase64: '' },
  } };
  const reference = { schema_version: 1 as const, account: c.account, container: c.data,
    blob: prefix + '/_arrowcol-oat-qap-custody.json', run_id: run, nonce, manifest_sha256: manifestHash,
    namespace_uid: id, namespace_owner_token: record.workload.namespaceOwner };
  const blobs = new Map<string, unknown>([[reference.blob, shared], [record.rootMarker.path, marker]]);
  const writes: string[] = [];
  t.mock.method(azure, 'blob', async (method: string, _container: string, name: string,
    query?: Record<string, string>, body?: string | Buffer, requestHeaders?: Record<string, string>, _missing?: boolean, _idempotent?: boolean,
    capture?: (receipt: HttpReceipt) => void) => {
    if (method === 'HEAD') return [null, name === '' ?
      { ':status': '200', 'x-ms-meta-sandboxid': c.sandbox } : { 'x-ms-lease-status': 'locked', 'x-ms-lease-state': 'leased' }];
    if (query?.comp === 'lease') {
      const status = requestHeaders?.['x-ms-lease-action'] === 'acquire' ? 201 : 200;
      const headers = { ':status': String(status), 'x-ms-lease-id': requestHeaders?.['x-ms-proposed-lease-id'] ?? leaseId };
      capture?.({ status, headers, bodyBase64: '' });
      return [null, headers];
    }
    if (method === 'PUT') {
      writes.push(name);
      blobs.set(name, JSON.parse(String(body)));
      const headers = { ':status': '201', etag: '"v1"' };
      capture?.({ status: 201, headers, bodyBase64: '' });
      return [null, headers];
    }
    return [blobs.has(name) ? Buffer.from(JSON.stringify(blobs.get(name))) : null, { etag: '"v1"' }];
  });
  return { azure, c, record, reference, shared, blobs, writes, directory };
}

test('durable QAP recovery retains the original full record and rejects changed identity or uncertain lease', async t => {
  const f = recoveryFixture(t), recordPath = path.join(f.directory, 'recovered.json');
  await recover_oat_qap_record(f.azure, f.reference, recordPath);
  assert.deepEqual(JSON.parse(fs.readFileSync(recordPath, 'utf8')), f.record);
  assert.deepEqual(await oat_qap_reference(f.azure, recordPath), f.reference);
  assert.deepEqual(f.writes, []);
  await assert.rejects(recover_oat_qap_record(f.azure, { ...f.reference, namespace_uid: run }, recordPath), /ownership reference/);
  f.shared.lease.stage = 'release-intent';
  await assert.rejects(recover_oat_qap_record(f.azure, f.reference, recordPath), /lease operation remains unresolved/);
});

test('fenced recovery reuses original QAP cleanup under the held root lease and remains idempotent', async t => {
  const f = recoveryFixture(t), recordPath = path.join(f.directory, 'recovered.json'), stopPath = path.join(f.directory, 'stop.json');
  f.shared.lease.stage = 'held';
  await recover_oat_qap_record(f.azure, f.reference, recordPath);
  const original = f.record.workload;
  fs.writeFileSync(stopPath, JSON.stringify({ schema_version: 1, workload_record: original, run_id: run,
    namespace: original.namespace, namespace_uid: original.namespaceUid, namespace_run_id: run,
    namespace_owner_token: original.namespaceOwner, aks_id: original.aksId, kubectl_server: original.server,
    delete_succeeded: true, absence_confirmed: true, observed_at_unix_ms: Date.now() }), { mode: 0o600 });
  const tags = { ...TAGS, sandboxId: f.c.sandbox };
  t.mock.method(f.azure, 'arm', async (_method: string, id: string) => {
    if (id === f.c.storage_id) return { tags, name: f.c.account, properties: { isHnsEnabled: true } };
    if (id === f.c.cluster_id) return { tags, properties: { state: 'Running', uri: f.c.uri } };
    if (id === f.c.identity_id) return { tags, properties: { principalId: f.record.identity } };
    if (id === f.c.cluster_id + '/databases/' + f.c.experimentDatabase)
      return { name: f.c.experimentDatabase, type: 'Microsoft.Kusto/clusters/databases' };
    if (id === original.aksId) return { tags, properties: { fqdn: new URL(original.server).hostname } };
    throw new Error('Unexpected ARM read');
  });
  const table = (rows: Record<string, unknown>[], columns = ['TableName']) => ({ Tables: [{
    Columns: (rows.length ? Object.keys(rows[0]) : columns).map(ColumnName => ({ ColumnName, ColumnType: 'string' })),
    Rows: rows.map(row => Object.values(row)),
  }] });
  let present = true, mutations = 0, fenced = true;
  t.mock.method(f.azure, 'kql', async (command: string) => {
    if (command.includes('policy managed_identity')) return table([{ Policy: JSON.stringify([{
      ObjectId: f.record.identity, AllowedUsages: 'ExternalTable,AutomatedFlows',
    }]) }]);
    if (command === '.show tables') return table([]);
    if (command === '.show external tables') return table(present ? [f.record.entries[0].snapshot!.definition as Record<string, unknown>] : []);
    if (command.includes('schema as json')) return table([{ TableName: name, DatabaseName: f.c.experimentDatabase,
      Schema: JSON.stringify(f.record.entries[0].snapshot!.schema) }]);
    if (command.includes('policy query_acceleration')) return table([{ EntityName: `[${f.c.experimentDatabase}].[${name}]`,
      EntityType: 'ExternalTable', Policy: JSON.stringify(f.record.policy) }]);
    throw new Error('Unexpected Kusto query');
  });
  t.mock.method(runtime, 'run_tree', async () => ({ returncode: 0, stdout: '', stderr: '' }));
  t.mock.method(f.azure, 'request', async (method: string, _url: string, _resource: string, body: string,
    _headers: unknown, _missing: boolean, _idempotent: boolean, capture: (receipt: HttpReceipt) => void) => {
    assert.equal(method, 'POST');
    assert.equal(JSON.parse(body).csl, `.drop external table ['${name}']`);
    const shared = f.blobs.get(f.reference.blob) as { custody: RecordV1 };
    assert.equal(shared.custody.intent?.operation, 'drop');
    mutations++;
    present = false;
    const bytes = Buffer.from(JSON.stringify(table([{ TableName: name, TableType: null, DocString: null, Properties: null }])));
    capture({ status: 200, headers: {}, bodyBase64: bytes.toString('base64') });
    return [bytes, { ':status': '200' }];
  });
  const authority = { assertCurrent: async () => { if (fenced) throw new Error('stale cleanup actor'); } };
  await assert.rejects(cleanup_oat_qap(f.azure, recordPath, stopPath, authority), /stale cleanup actor/);
  assert.equal(mutations, 0);
  fenced = false;
  await cleanup_oat_qap(f.azure, recordPath, stopPath, authority);
  assert.equal(JSON.parse(fs.readFileSync(recordPath, 'utf8')).state, 'cleaned');
  await cleanup_oat_qap(f.azure, recordPath, stopPath, authority);
  assert.equal(mutations, 1);
});

test('QAP recovered snapshots reject nested credential-bearing connection fields', t => {
  const f = recoveryFixture(t), corrupted = structuredClone(f.record);
  corrupted.entries[0].snapshot!.definition = externalDefinition({ ConnectionStrings: ['https://storage.test/path?sig=secret'] });
  assert.throws(() => parse_oat_qap_record(f.azure, corrupted), /credential-bearing/);
  corrupted.entries[0].snapshot!.definition = externalDefinition();
  assert.throws(() => parse_oat_qap_record(f.azure, corrupted), /exact owned run URI/);
});

test('durable stop recovery requires a matching UID-guarded positive deletion, not listing absence', async t => {
  const f = recoveryFixture(t), original = f.record.workload;
  const receipt = { schema_version: 1, workload_record: original, run_id: run, namespace: original.namespace,
    namespace_uid: original.namespaceUid, namespace_run_id: run, namespace_owner_token: original.namespaceOwner,
    aks_id: original.aksId, kubectl_server: original.server, delete_succeeded: true, absence_confirmed: true,
    observed_at_unix_ms: Date.now() };
  const stop = { schema_version: 1, reference: f.reference, receipt, deletion: {
    intent: { workload_record: original, deletion: { apiVersion: 'v1', kind: 'DeleteOptions', preconditions: { uid: original.namespaceUid } } },
    response: { kind: 'Namespace', metadata: { name: original.namespace, uid: original.namespaceUid, deletionTimestamp: new Date().toISOString() } },
  } };
  const blob = f.reference.blob.replace('_arrowcol-oat-qap-custody.json', '_arrowcol-oat-workload-stop.json');
  const stopPath = path.join(f.directory, 'stop.json');
  f.blobs.set(blob, stop);
  await recover_oat_workload_stop(f.azure, f.reference, stopPath);
  assert.deepEqual(JSON.parse(fs.readFileSync(stopPath, 'utf8')), receipt);
  stop.deletion.response.metadata.uid = run;
  await assert.rejects(recover_oat_workload_stop(f.azure, f.reference, stopPath), /affirm the original UID/);
  assert.throws(() => validate_oat_workload_stop(f.record, { ...receipt, delete_succeeded: false }), /not affirmative/);
});

const deltaProperties = {
  AutoUpdateSchema: false, Compressed: false, CompressionType: null, Encoding: null,
  FileExtension: null, Format: 'Parquet', IncludeHeaders: 'None', NamePrefix: null, Shortcut: false,
};
const externalConnection = `abfss://synthetic@storage123.dfs.core.windows.net/${relative};managed_identity=${id}`;
function externalDefinition(patch: Record<string, unknown> = {}) {
  return { TableName: name, TableType: 'Delta', Folder: null, DocString: 'arrowcol-oat:synthetic-owner',
    Properties: JSON.stringify(deltaProperties), ConnectionStrings: [externalConnection],
    Partitions: null, PathFormat: '', ...patch };
}
function kustoTable(TableName: string, columns: { ColumnName: string; ColumnType: string }[], Rows: unknown[][]) {
  return { TableName, Columns: columns, Rows };
}
function queryResponse(statusRows: unknown[][] = [
  ['2026-11-10T00:00:00Z', 4, 'Info', 0, 'Query completed successfully'],
  ['2026-11-10T00:00:00Z', 3, 'Warning', -2133196383, 'Query acceleration was not applied; catalog is uninitialized.'],
  ['2026-11-10T00:00:00Z', 6, 'Stats', 0, null],
]) {
  const statusColumns = [
    { ColumnName: 'Timestamp', ColumnType: 'datetime' }, { ColumnName: 'Severity', ColumnType: 'int' },
    { ColumnName: 'SeverityName', ColumnType: 'string' }, { ColumnName: 'StatusCode', ColumnType: 'int' },
    { ColumnName: 'StatusDescription', ColumnType: 'string' },
  ];
  return { Tables: [
    kustoTable('Table_0', [{ ColumnName: 'Count', ColumnType: 'long' }], [[0]]),
    kustoTable('Table_1', [{ ColumnName: 'Value', ColumnType: 'string' }], [['extended']]),
    kustoTable('Table_2', statusColumns, statusRows),
    kustoTable('Table_3', [
      { ColumnName: 'Ordinal', ColumnType: 'long' }, { ColumnName: 'Kind', ColumnType: 'string' },
      { ColumnName: 'Name', ColumnType: 'string' }, { ColumnName: 'Id', ColumnType: 'string' },
      { ColumnName: 'PrettyName', ColumnType: 'string' },
    ], [[0, 'QueryResult', 'PrimaryResult', 'id0', ''], [1, 'QueryProperties', '@ExtendedProperties', 'id1', ''],
      [2, 'QueryStatus', 'QueryStatus', 'id2', '']]),
  ] };
}
test('validates the qualified Azure Kusto database resource name and case-insensitive type', () => {
  const cluster = 'arrow-collector-87513';
  assert.doesNotThrow(() => validate_experiment_database_resource({
    name: `${cluster}/experimentqap`, type: 'Microsoft.Kusto/Clusters/Databases',
  }, cluster, 'experimentqap'));
  assert.doesNotThrow(() => validate_experiment_database_resource({
    name: 'experimentqap', type: 'microsoft.kusto/clusters/databases',
  }, cluster, 'experimentqap'));
  for (const resource of [
    { name: 'arrow-collector-00001/experimentqap', type: 'Microsoft.Kusto/Clusters/Databases' },
    { name: 'arrow-collector-87513/other', type: 'Microsoft.Kusto/Clusters/Databases' },
    { name: 'arrow-collector-87513/experimentqap', type: 'Microsoft.Kusto/Clusters' },
  ]) assert.throws(() => validate_experiment_database_resource(resource, cluster, 'experimentqap'), error =>
    error instanceof Error && error.message === 'Existing experiment database resource name/type mismatch.');
});
test('qualified Delta provider shape parses typed connection and opaque Properties without desired-value substitution', () => {
  const parsed = parseQualifiedDeltaDefinition(externalDefinition(), name);
  assert.deepEqual(parsed.connectionStrings, [externalConnection]);
  assert.deepEqual(assertExactConnectionStrings(parsed.connectionStrings, externalConnection), [externalConnection]);
  assert.throws(() => assertExactConnectionStrings(parsed.connectionStrings,
    externalConnection.replace(id, '33333333-3333-4333-8333-333333333333')), /exact owned run URI and UAMI principal/);
  assert.throws(() => assertExactConnectionStrings([...parsed.connectionStrings, externalConnection], externalConnection),
    /exact owned run URI and UAMI principal/);
  assert.equal(parsed.docString, 'arrowcol-oat:synthetic-owner');
  assert.deepEqual(parsed.properties, deltaProperties);
  assert.throws(() => parseQualifiedDeltaDefinition(externalDefinition({ TableType: 'Parquet' }), name), /name\/type/);
  assert.throws(() => parseQualifiedDeltaDefinition(externalDefinition({ TableName: name + '_other' }), name), /name\/type/);
  assert.throws(() => parseQualifiedDeltaDefinition(externalDefinition({ ConnectionStrings: externalConnection }), name), /ConnectionStrings/);
  assert.throws(() => parseQualifiedDeltaDefinition(externalDefinition({
    ConnectionStrings: [`${externalConnection};client_id=${id}`],
  }), name), /ConnectionStrings/);
  assert.throws(() => parseQualifiedDeltaDefinition(externalDefinition({
    Properties: JSON.stringify({ ...deltaProperties, Unexpected: true }),
  }), name), /Properties differ/);
  assert.throws(() => parseQualifiedDeltaDefinition(externalDefinition({
    Properties: { ...deltaProperties },
  }), name), /JSON string shape/);
});
test('management results remain separate from TOC-mapped query results and enforce typed columns', () => {
  const management = { Tables: [kustoTable('Table_0', [
    { ColumnName: 'TableName', ColumnType: 'string' }, { ColumnName: 'TableType', ColumnType: 'string' },
  ], [[name, 'Delta']])] };
  assert.deepEqual(managementRows(management), [{ TableName: name, TableType: 'Delta' }]);
  assert.throws(() => managementRows(queryResponse()), /one Kusto primary/);
  const drop = { TableName: name, TableType: null, DocString: null, Properties: null };
  const dropManagement = { Tables: [kustoTable('Table_0', [
    { ColumnName: 'TableName', ColumnType: 'string' }, { ColumnName: 'TableType', ColumnType: 'string' },
    { ColumnName: 'DocString', ColumnType: 'string' }, { ColumnName: 'Properties', ColumnType: 'string' },
  ], [[name, null, null, null]])] };
  const parsedDrop = managementRows(dropManagement)[0];
  assert.deepEqual(parsedDrop, drop);
  assert.deepEqual(validateExternalMutationReceipt('drop', name, 'arrowcol-oat:synthetic-owner', parsedDrop), drop);
  const missingType = structuredClone(management);
  delete (missingType.Tables[0].Columns[0] as { ColumnType?: string }).ColumnType;
  assert.throws(() => managementRows(missingType), /ColumnType/);
});
test('query response maps primary results by TOC and distinguishes an unaccelerated completed query', () => {
  const parsed = parseKustoQueryResult(queryResponse());
  assert.deepEqual(parsed.rows, [{ Count: 0 }]);
  assert.equal(parsed.query_completed, true);
  assert.equal(parsed.query_acceleration_not_applied, true);
  assert.equal(parsed.query_acceleration_applied, false);
  assert.equal(parsed.accelerated_read_proven, false);
});
test('query response rejects malformed TOC, unknown statuses and partial failures', () => {
  const wrongIdType = structuredClone(queryResponse()); wrongIdType.Tables[3].Columns[3].ColumnType = 'guid';
  assert.throws(() => parseKustoQueryResult(wrongIdType), /unexpected column types or fields/);
  const wrongPrettyNameType = structuredClone(queryResponse()); wrongPrettyNameType.Tables[3].Columns[4].ColumnType = 'dynamic';
  assert.throws(() => parseKustoQueryResult(wrongPrettyNameType), /unexpected column types or fields/);
  const malformedId = structuredClone(queryResponse()); malformedId.Tables[3].Rows[0][3] = 0;
  assert.throws(() => parseKustoQueryResult(malformedId), /table of contents is malformed/);
  const missingId = structuredClone(queryResponse()); missingId.Tables[3].Rows[0][3] = '';
  assert.throws(() => parseKustoQueryResult(missingId), /table of contents is malformed/);
  const malformedPrettyName = structuredClone(queryResponse()); malformedPrettyName.Tables[3].Rows[1][4] = null;
  assert.throws(() => parseKustoQueryResult(malformedPrettyName), /table of contents is malformed/);
  const contradictoryIdentity = structuredClone(queryResponse());
  contradictoryIdentity.Tables[3].Rows[0][3] = null;
  contradictoryIdentity.Tables[3].Rows[0][4] = 0;
  assert.throws(() => parseKustoQueryResult(contradictoryIdentity), /table of contents is malformed/);
  const badOrdinal = structuredClone(queryResponse()); badOrdinal.Tables[3].Rows[2][0] = 9;
  assert.throws(() => parseKustoQueryResult(badOrdinal), /table of contents is malformed/);
  const duplicatePrimary = structuredClone(queryResponse()); duplicatePrimary.Tables[3].Rows.push([...duplicatePrimary.Tables[3].Rows[2]]);
  assert.throws(() => parseKustoQueryResult(duplicatePrimary), /table of contents is malformed/);
  const missingPrimary = structuredClone(queryResponse()); missingPrimary.Tables[3].Rows.pop();
  assert.throws(() => parseKustoQueryResult(missingPrimary), /does not map every result table/);
  const failed = queryResponse([['now', 2, 'Error', -1, 'failed'], ['now', 4, 'Info', 0, 'done']]);
  assert.throws(() => parseKustoQueryResult(failed), /partial query failure/);
  const unknownCode = queryResponse([['now', 4, 'Info', 0, 'done'], ['now', 3, 'Warning', -1, 'unknown']]);
  assert.throws(() => parseKustoQueryResult(unknownCode), /unknown or unsuccessful/);
  const malformedSeverity = queryResponse([['now', '3', 'Warning', -2133196383, 'typed incorrectly'], ['now', 4, 'Info', 0, 'done']]);
  assert.throws(() => parseKustoQueryResult(malformedSeverity), /malformed severity/);
  const noCompletion = queryResponse([['now', 6, 'Stats', 0, null]]);
  assert.throws(() => parseKustoQueryResult(noCompletion), /no successful completion/);
});
test('create receipts require exact Delta metadata; drop receipts accept only matching-name null tombstones', () => {
  const owner = 'arrowcol-oat:synthetic-owner';
  const create = externalDefinition({ DocString: owner });
  assert.equal(validateExternalMutationReceipt('create', name, owner, create).TableName, name);
  assert.throws(() => validateExternalMutationReceipt('create', name, owner + 'wrong', create), /owner shape/);
  assert.throws(() => validateExternalMutationReceipt('create', name, owner,
    { ...create, Properties: JSON.stringify({ ...deltaProperties, AutoUpdateSchema: true }) }), /Properties differ/);
  const drop = { TableName: name, TableType: null, DocString: null, Properties: null };
  assert.deepEqual(validateExternalMutationReceipt('drop', name, owner, drop), drop);
  assert.throws(() => validateExternalMutationReceipt('drop', name + '_other', owner, drop), /null tombstone/);
  assert.throws(() => validateExternalMutationReceipt('drop', name, owner, { ...drop, TableType: 'Delta' }), /null tombstone/);
});
test('locked run/path/name validation still rejects traversal, wrong identity and prefix aliases', () => {
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: id }));
  azure.c.table_prefix = 'synthetic';
  assert.deepEqual(validate_oat_manifest(azure, run, manifest), manifest);
  assert.throws(() => validate_oat_manifest(azure, run, { schema_version: 1, run_id: run, tables: manifest.tables }), /missing or unexpected fields/);
  assert.throws(() => validate_oat_manifest(azure, run, { ...manifest, database: 'arrowcol' }), /must match the configured experiment database/);
  for (const patch of [
    { run_id: id }, { schema_version: '1' }, { tables: [manifest.tables[0], manifest.tables[0]] },
    { tables: [{ external_table: name, relative_path: relative + '/child' }] },
    { tables: [{ external_table: name, relative_path: relative.replace('/runs/', '/runs/../') }] },
    { tables: [{ external_table: name + '_other', relative_path: relative }] },
    { tables: [{ external_table: name, relative_path: relative.slice('synthetic/'.length) }] }
  ]) assert.throws(() => validate_oat_manifest(azure, run, { ...manifest, ...patch }));
  assert.throws(() => validate_oat_manifest(azure, run.toUpperCase(), manifest));
  azure.c.table_prefix = '';
  const withoutPrefix = { ...manifest, tables: [{ external_table: name, relative_path: relative.slice('synthetic/'.length) }] };
  assert.deepEqual(validate_oat_manifest(azure, run, withoutPrefix), withoutPrefix);
});
