import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, mock, test, type TestContext } from 'node:test';
import { Azure, Config, Failure, NotReady, TAGS, canonical_json, main, publish_private, runtime, write_private } from './bootstrap.js';
import { type RunFicRecord } from './run_fic.js';
import { bind_oat_qap, validate_oat_manifest } from './oat_qap.js';

const id = '22222222-2222-4222-8222-222222222222';
const run = '9dcd6847-2a56-45a3-bd3c-54af839f451e';
const name = `oat_${run.replaceAll('-', '')}_sqlserver_synthetic_table`;
const relative = `synthetic/runs/${run}/dataflow/telemetry/sqlserver/sqlserver_synthetic_table`;
const manifest = { schema_version: 1, run_id: run, tables: [{ external_table: name, relative_path: relative }] };
afterEach(() => mock.restoreAll());

function fixture(t: TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'oat-qap-r1-synthetic-'));
  fs.chmodSync(directory, 0o700);
  const manifestPath = path.join(directory, 'manifest.json'), workloadPath = path.join(directory, 'workload.json');
  const recordPath = path.join(directory, 'custody.json');
  t.after(() => {
    for (const filename of ['manifest.json', 'workload.json', 'custody.json.lock', 'synthetic.env', 'stamp.yaml']) {
      const target = path.join(directory, filename);
      if (fs.existsSync(target)) fs.unlinkSync(target);
    }
    fs.rmdirSync(directory);
  });
  const c = new Config({ AZURE_SUBSCRIPTION_ID: id }), azure = new Azure(c);
  c.table_prefix = 'synthetic';
  const namespace = 'oat-' + run.replaceAll('-', ''), issuer = 'https://synthetic.oic.prod-aks.azure.com/tenant/cluster/';
  const workload: RunFicRecord = {
    version: 1, runId: run, ownershipNonce: id, namespace, namespaceUid: id,
    namespaceOwner: 'synthetic-private-owner-token', serviceAccount: 'arrowcol-oat', serviceAccountUid: id,
    identityId: c.identity_id, clientId: id, tenantId: id,
    aksId: c.base + '/providers/Microsoft.ContainerService/managedClusters/' + c.cluster,
    server: 'https://synthetic.azmk8s.io', credentialName: 'arrowcol-run-' + run, issuer,
    subject: `system:serviceaccount:${namespace}:arrowcol-oat`, audience: 'api://AzureADTokenExchange', phase: 'ready'
  };
  publish_private(workloadPath, workload);
  fs.writeFileSync(manifestPath, canonical_json(manifest), { mode: 0o600 });
  const calls: { resource: string; api: string | undefined }[] = [];
  const kqlReads: string[] = [];
  const tags = { ...TAGS, sandboxId: c.sandbox };
  mock.method(azure, 'arm', async (method: string, resource: string, body?: unknown, missingOk = false, api?: string) => {
    assert.equal(method, 'GET'); assert.equal(body, undefined); assert.equal(missingOk, false);
    calls.push({ resource, api });
    if (resource === c.storage_id) {
      assert.equal(api, '2023-05-01', 'Storage must not inherit the default Kusto API.');
      return { tags, name: c.account, properties: { isHnsEnabled: true } };
    }
    if (resource === c.cluster_id) {
      assert.equal(api, '2024-04-13');
      return { tags, properties: { state: 'Running', uri: c.uri } };
    }
    if (resource === c.cluster_id + '/databases/' + c.experimentDatabase) {
      assert.equal(api, '2024-04-13');
      return { name: c.experimentDatabase, type: 'Microsoft.Kusto/clusters/databases' };
    }
    if (resource === c.identity_id) {
      assert.equal(api, '2023-01-31');
      return { tags, properties: { clientId: id, tenantId: id, principalId: id } };
    }
    if (resource === workload.aksId) {
      assert.equal(api, '2024-10-01');
      return { tags, properties: { fqdn: 'synthetic.azmk8s.io',
        oidcIssuerProfile: { issuerURL: issuer, enabled: true }, securityProfile: { workloadIdentity: { enabled: true } } } };
    }
    if (resource === c.identity_id + '/federatedIdentityCredentials/' + workload.credentialName) {
      assert.equal(api, '2023-01-31');
      return { id: resource, name: workload.credentialName,
        properties: { issuer, subject: workload.subject, audiences: [workload.audience] } };
    }
    throw new Error('Unexpected ARM resource.');
  });
  mock.method(azure, 'collection', async (resource: string, api?: string) => {
    assert.equal(resource, c.identity_id + '/federatedIdentityCredentials');
    assert.equal(api, '2023-01-31');
    return [{ name: 'azure-qap-' + c.cluster, properties: { issuer, audiences: ['api://AzureADTokenExchange'],
      subject: `system:serviceaccount:${c.kubernetesNamespace}:${c.kubernetesServiceAccount}` } }];
  });
  mock.method(runtime, 'run_tree', async (args: string[]) => {
    assert.equal(args[0], 'kubectl'); assert.equal(args[args.indexOf('--server') + 1], workload.server);
    assert(args.includes('--insecure-skip-tls-verify=false'));
    const metadata = args.includes('namespace') ? {
      name: namespace, uid: id,
      labels: { 'arrowcol-oat/run-id': run, 'arrowcol-oat/owner-token': workload.namespaceOwner },
      annotations: { 'arrowcol-oat/run-id': run, 'arrowcol-oat/owner-token': workload.namespaceOwner }
    } : { name: workload.serviceAccount, namespace, uid: id, annotations: { 'azure.workload.identity/client-id': id } };
    return { returncode: 0, stdout: canonical_json({ metadata }), stderr: '' };
  });
  mock.method(azure, 'blob', async (method: string, container: string, blobName: string, query?: Record<string, string>) => {
    assert.equal(method, 'HEAD', 'Qualification must precede every Blob write/lease acquisition.');
    assert.equal(container, c.data); assert.equal(blobName, '');
    assert.deepEqual(query, { restype: 'container' });
    return [null, { ':status': '200', 'x-ms-meta-sandboxid': c.sandbox }];
  });
  mock.method(azure, 'kql', async (command: string, database?: string, idempotent?: boolean) => {
    assert.equal(database, c.experimentDatabase); assert.equal(idempotent, true);
    assert.equal(command, `.show database ['${c.experimentDatabase}'] policy managed_identity`,
      'No unsupported ConnectionStrings projection or mutating KQL is allowed.');
    kqlReads.push(command);
    return { Tables: [{ Columns: [{ ColumnName: 'Policy', ColumnType: 'string' }],
      Rows: [[canonical_json([{ ObjectId: id, AllowedUsages: 'ExternalTable,AutomatedFlows' }])]] }] };
  });
  mock.method(azure, 'request', async () => { throw new Error('Provider qualification must precede mutation requests.'); });
  return { c, calls, kqlReads, recordPath, directory,
    bind: () => bind_oat_qap(azure, run, manifestPath, recordPath, workloadPath),
    dispatch: async () => {
      const envPath = path.join(directory, 'synthetic.env'), stampPath = path.join(directory, 'stamp.yaml');
      write_private(envPath, { AZURE_SUBSCRIPTION_ID: id,
        ARROWCOL_AZURE_TABLE_ROOT: `abfss://${c.data}@${c.account}.dfs.core.windows.net/synthetic` });
      fs.writeFileSync(stampPath, canonical_json(c.stampConfig), { mode: 0o600 });
      mock.method(Azure.prototype, 'arm', azure.arm.bind(azure));
      mock.method(Azure.prototype, 'collection', azure.collection.bind(azure));
      mock.method(Azure.prototype, 'blob', azure.blob.bind(azure));
      mock.method(Azure.prototype, 'kql', azure.kql.bind(azure));
      mock.method(Azure.prototype, 'request', azure.request.bind(azure));
      return main(['bind-oat-qap', '--env-file', envPath, '--stamp-config', stampPath,
        '--run-id', run, '--manifest', manifestPath, '--record', recordPath, '--workload-record', workloadPath]);
    } };
}

test('provider-specific ARM versions are explicit; unknown Delta Properties blocks before every mutation', async t => {
  const f = fixture(t);
  await assert.rejects(f.bind, error => error instanceof Failure && !(error instanceof NotReady) &&
    /provider-read qualification required before mutation/.test(error.message));
  assert(f.calls.some(call => call.resource === f.c.storage_id && call.api === '2023-05-01'));
  assert(f.calls.some(call => call.resource === f.c.cluster_id && call.api === '2024-04-13'));
  assert(f.calls.some(call => call.resource.endsWith('/databases/' + f.c.experimentDatabase) && call.api === '2024-04-13'));
  assert.equal(fs.existsSync(f.recordPath), false);
  assert.deepEqual(fs.readdirSync(f.directory).sort(), ['custody.json.lock', 'manifest.json', 'workload.json']);
});
test('qualification is not overcome by desired connection configuration or another bind attempt', async t => {
  const f = fixture(t);
  await assert.rejects(f.bind, /No ConnectionStrings projection, aliases or desired-config substitution/);
  await assert.rejects(f.bind, /provider-read qualification/);
  assert.equal(fs.existsSync(f.recordPath), false);
});
test('QAP dispatch reports an unqualified provider contract as nonretryable, not absent Delta logs', async t => {
  const f = fixture(t), errors: string[] = [], environment = process.env;
  process.env = Object.fromEntries(Object.entries(environment).filter(([key]) => !/^(AZURE_|ARROWCOL_)/.test(key)));
  t.after(() => { process.env = environment; });
  mock.method(console, 'error', (message: string) => { errors.push(message); });
  assert.equal(await f.dispatch(), 1);
  assert.ok(f.kqlReads.length > 0, 'read-only KQL happened; kql_attempted=false would be false');
  assert.equal(errors.length, 1);
  assert.match(errors[0], /^ERROR: QAP provider-read qualification required before mutation/);
  assert.match(errors[0], /nonretryable provider-contract failure/);
  assert.doesNotMatch(errors[0], /DELTA_LOGS_ABSENT|"retryable":true|"kql_attempted":false/);
  assert.equal(fs.existsSync(f.recordPath), false);
});
test('locked run/path/name validation still rejects traversal, wrong identity and prefix aliases', () => {
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: id }));
  azure.c.table_prefix = 'synthetic';
  assert.deepEqual(validate_oat_manifest(azure, run, manifest), manifest);
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
