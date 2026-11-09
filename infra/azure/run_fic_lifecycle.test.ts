import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, mock, test, type TestContext } from 'node:test';
import { Azure, Config, Failure, HttpFailure, TAGS, runtime, type HttpReceipt } from './bootstrap.js';
import { cleanup_run_fic, prepare_run_fic, read_run_fic_record, run_fic_ledger_path } from './run_fic.js';

const id = '00000000-0000-0000-0000-000000000001';
const runId = '9dcd6847-2a56-45a3-bd3c-54af839f451e';
const namespace = 'oat-' + runId.replaceAll('-', '');
const serviceAccount = 'arrowcol-oat';
const nextRunId = '7acd8847-1a56-45a3-bd3c-54af839f451e';
const nextNamespace = 'oat-' + nextRunId.replaceAll('-', '');
const issuer = 'https://fixture.oic.prod-aks.azure.com/tenant/cluster/';
const ownerToken = 'fixture-private-owner-token';
afterEach(() => mock.restoreAll());

function fixture(t: TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'oat-fic-lifecycle-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const recordPath = path.join(directory, 'owned.json');
  const c = new Config({ AZURE_SUBSCRIPTION_ID: id }), azure = new Azure(c);
  const state = {
    body: null as string | null, revision: 0, lease: '', mutations: [] as string[],
    credentials: new Map<string, Record<string, unknown>>(), absentNamespaces: new Set<string>(),
    additionalCredentials: [] as Record<string, unknown>[],
    ambiguousAcquire: false, ambiguousArm: false, ambiguousLedger: false,
    ambiguousInitialWrite: false, ambiguousRelease: false, ambiguousCompletion: false,
    failCompletionBeforeCommit: false, releases: 0, acquisitions: 0,
    beforeAcquire: null as (() => void) | null,
    afterRelease: null as (() => Promise<void>) | null,
  };
  const native = { id: c.identity_id + '/federatedIdentityCredentials/azure-qap-' + c.cluster,
    name: 'azure-qap-' + c.cluster, properties: { issuer, audiences: ['api://AzureADTokenExchange'],
      subject: `system:serviceaccount:${c.kubernetesNamespace}:${c.kubernetesServiceAccount}` } };
  const unrelated = { id: c.identity_id + '/federatedIdentityCredentials/unrelated', name: 'unrelated',
    properties: { issuer: 'https://example.test/', subject: 'preserve-me', audiences: ['api://AzureADTokenExchange'] } };
  const tags = { ...TAGS, sandboxId: c.sandbox };
  mock.method(azure, 'arm', async (_method: string, resource: string) => {
    if (resource === c.identity_id) return { tags, properties: { clientId: id, tenantId: id } };
    if (resource.includes('/managedClusters/')) return { tags, properties: {
      fqdn: 'fixture.azmk8s.io', oidcIssuerProfile: { issuerURL: issuer, enabled: true },
      securityProfile: { workloadIdentity: { enabled: true } },
    } };
    if (resource.includes('/federatedIdentityCredentials/arrowcol-run-')) {
      const credential = state.credentials.get(resource.split('/arrowcol-run-')[1]!);
      return credential ? structuredClone(credential) : null;
    }
    if (resource === c.storage_id) return { tags };
    if (resource.endsWith('/containers/' + c.checkpoints)) return { properties: { metadata: { sandboxid: c.sandbox } } };
    throw new Error('Unexpected mocked ARM read');
  });
  mock.method(azure, 'collection', async () => [native, unrelated, ...state.additionalCredentials,
    ...Array.from(state.credentials.values(), credential => structuredClone(credential))]);
  mock.method(azure, 'blob', async (method: string, container: string, name: string,
    query?: Record<string, string>, body?: string | Buffer | null,
    headers: Record<string, string> = {}, _missingOk?: boolean, _idempotent?: boolean,
    capture?: (r: HttpReceipt) => void): Promise<[Buffer | null, Record<string, string>]> => {
    assert.equal(container, c.checkpoints);
    assert.equal(name, run_fic_ledger_path(c));
    const etag = `"${state.revision}"`;
    if (method === 'GET') {
      if (headers['x-ms-lease-id']) assert.equal(headers['x-ms-lease-id'], state.lease);
      return state.body === null ? [null, {}] : [Buffer.from(state.body), { etag }];
    }
    assert.equal(method, 'PUT');
    if (query?.comp === 'lease') {
      if (headers['x-ms-lease-action'] === 'acquire') {
        assert.equal(headers['x-ms-lease-duration'], '-1');
        state.beforeAcquire?.();
        if (headers['If-Match'] !== `"${state.revision}"`) throw new HttpFailure(412, 'stale acquisition version');
        if (state.lease) throw new HttpFailure(409, 'lease already present');
        state.acquisitions++;
        state.lease = headers['x-ms-proposed-lease-id'];
        if (state.ambiguousAcquire) throw new Failure('Injected lost acquisition response');
        const receiptHeaders = { 'x-ms-lease-id': state.lease, 'x-ms-request-id': 'fixture-acquire',
          'x-ms-client-request-id': headers['x-ms-client-request-id'] };
        capture?.({ status: 201, bodyBase64: '', headers: receiptHeaders });
        return [Buffer.alloc(0), { ':status': '201', ...receiptHeaders }];
      }
      assert.equal(headers['x-ms-lease-action'], 'release', 'never break/change a coordination lease');
      assert.equal(headers['x-ms-lease-id'], state.lease);
      state.lease = ''; state.releases++;
      if (state.ambiguousRelease) throw new Failure('Injected lost release response');
      await state.afterRelease?.();
      const receiptHeaders = { 'x-ms-request-id': 'fixture-release',
        'x-ms-client-request-id': headers['x-ms-client-request-id'] };
      capture?.({ status: 200, bodyBase64: '', headers: receiptHeaders });
      return [Buffer.alloc(0), { ':status': '200', ...receiptHeaders }];
    }
    if (headers['If-None-Match'] === '*') {
      if (state.body !== null) throw new HttpFailure(412, 'existing');
    } else assert.equal(headers['If-Match'], etag, 'every ledger update uses its captured version');
    if (state.lease) assert.equal(headers['x-ms-lease-id'], state.lease);
    const written = JSON.parse(String(body));
    if (state.failCompletionBeforeCommit && written.transaction.phase === 'complete')
      throw new Failure('Injected final write not committed');
    state.body = String(body); state.revision++;
    if (state.ambiguousInitialWrite && state.revision === 1) throw new Failure('Injected lost initial ledger response');
    if (state.ambiguousLedger && state.lease) throw new Failure('Injected lost ledger response');
    if (state.ambiguousCompletion && written.transaction.phase === 'complete')
      throw new Failure('Injected lost completion response');
    const receiptHeaders = { etag: `"${state.revision}"`, 'x-ms-request-id': 'fixture-ledger' };
    capture?.({ status: 201, bodyBase64: '', headers: receiptHeaders });
    return [Buffer.alloc(0), { ':status': '201', ...receiptHeaders }];
  });
  mock.method(azure, 'request', async (method: string, url: string, resource: string, body?: string | Buffer | null,
    headers: Record<string, string> = {}, missingOk?: boolean, idempotent?: boolean,
    capture?: (r: HttpReceipt) => void): Promise<[Buffer | null, Record<string, string>]> => {
    assert.equal(resource, 'https://management.azure.com/');
    const parsed = new URL(url);
    const credentialId = parsed.pathname;
    const requestRunId = credentialId.split('/arrowcol-run-')[1]!;
    assert.ok(credentialId.startsWith(c.identity_id + '/federatedIdentityCredentials/arrowcol-run-'));
    assert.equal(parsed.search, '?api-version=2023-01-31');
    assert.ok(state.lease, 'ARM mutation requires the infinite coordination lease');
    assert.equal(idempotent, false);
    if (method === 'GET') {
      const credential = state.credentials.get(requestRunId);
      const present = credential?.id === credentialId;
      assert.equal(missingOk, !present);
      const response: HttpReceipt = { status: present ? 200 : 404,
        bodyBase64: Buffer.from(JSON.stringify(present ? credential : { error: { code: 'ResourceNotFound' } })).toString('base64'),
        headers: { 'x-ms-request-id': 'fixture-postcondition' } };
      capture?.(response);
      return [present ? Buffer.from(response.bodyBase64, 'base64') : null, { ':status': String(response.status) }];
    }
    assert.ok(headers['x-ms-client-request-id']);
    assert.equal(missingOk, false);
    assert.equal(headers['If-Match'], undefined, 'no invented ARM CAS');
    assert.equal(headers['If-None-Match'], undefined, 'no invented ARM CAS');
    state.mutations.push(method);
    let response: HttpReceipt;
    if (method === 'PUT') {
      state.credentials.set(requestRunId, { id: credentialId,
        name: credentialId.split('/').at(-1)!, ...JSON.parse(String(body)),
        systemData: { createdAt: '2026-01-01T00:00:00Z' } });
      response = { status: 201, bodyBase64: Buffer.from(JSON.stringify(state.credentials.get(requestRunId))).toString('base64'),
        headers: { 'x-ms-request-id': 'fixture-create-request' } };
    } else {
      assert.equal(method, 'DELETE');
      state.credentials.delete(requestRunId);
      response = { status: 204, bodyBase64: '', headers: { 'x-ms-request-id': 'fixture-delete-request' } };
    }
    if (state.ambiguousArm) throw new Failure('Injected lost ARM response after side effect');
    capture?.(response);
    return [Buffer.from(response.bodyBase64, 'base64'), { ':status': String(response.status) }];
  });
  mock.method(runtime, 'run_tree', async (args: string[]) => {
    assert.equal(args[0], 'kubectl');
    assert.ok(args.includes('--insecure-skip-tls-verify=false'));
    const currentNamespace = args.includes('namespace') ? args[args.indexOf('namespace') + 1]!
      : args[args.indexOf('--namespace') + 1]!;
    const compact = currentNamespace.slice(-32);
    const currentRun = [compact.slice(0, 8), compact.slice(8, 12), compact.slice(12, 16),
      compact.slice(16, 20), compact.slice(20)].join('-');
    if (args.includes('namespace')) {
      if (args.includes('--ignore-not-found'))
        return { returncode: 0, stdout: state.absentNamespaces.has(currentNamespace) ? '' : 'namespace/' + currentNamespace, stderr: '' };
      return { returncode: 0, stdout: JSON.stringify({ metadata: { name: currentNamespace, uid: id,
        labels: { 'arrowcol-oat/run-id': currentRun, 'arrowcol-oat/owner-token': ownerToken },
        annotations: { 'arrowcol-oat/run-id': currentRun, 'arrowcol-oat/owner-token': ownerToken },
      } }), stderr: '' };
    }
    assert.ok(args.includes('serviceaccount'));
    return { returncode: 0, stdout: JSON.stringify({ metadata: { name: serviceAccount, namespace: currentNamespace, uid: id,
      annotations: { 'azure.workload.identity/client-id': id } } }), stderr: '' };
  });
  return { azure, c, state, recordPath, unrelated, native };
}

test('create and cleanup keep definitive receipts and preserve unrelated/native trusts', async t => {
  const f = fixture(t);
  await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
  assert.equal(read_run_fic_record(f.recordPath, f.c).phase, 'ready');
  assert.deepEqual(f.state.mutations, ['PUT']);
  assert.equal(f.state.lease, '');
  await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
  assert.deepEqual(f.state.mutations, ['PUT'], 'confirmed identical retry must not PUT again');
  await assert.rejects(cleanup_run_fic(f.azure, runId, f.recordPath), /namespace must be absent/);
  f.state.absentNamespaces.add(namespace);
  await cleanup_run_fic(f.azure, runId, f.recordPath);
  assert.deepEqual(f.state.mutations, ['PUT', 'DELETE']);
  assert.equal(read_run_fic_record(f.recordPath, f.c).phase, 'deleted');
  assert.equal(f.state.lease, '');
  assert.equal(f.state.releases, 2);
  assert.equal(JSON.parse(f.state.body!).transaction.phase, 'complete');
  assert.ok(fs.existsSync(f.recordPath + '.create-receipt.json'));
  assert.ok(fs.existsSync(f.recordPath + '.delete-receipt.json'));
  assert.deepEqual(await f.azure.collection(f.c.identity_id + '/federatedIdentityCredentials'), [f.native, f.unrelated]);
});
test('an ambiguous lease acquisition reserves the ledger without admitting an ARM request', async t => {
  const f = fixture(t); f.state.ambiguousAcquire = true;
  await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath), /lost acquisition/);
  assert.ok(f.state.lease);
  assert.equal(JSON.parse(f.state.body!).transaction.phase, 'acquiring');
  await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath), /no replay/);
  assert.deepEqual(f.state.mutations, []);
  assert.equal(f.state.releases, 0);
});
test('lost initial create-if-absent receipt leaves an acquisition reservation, not an available ledger', async t => {
  const f = fixture(t); f.state.ambiguousInitialWrite = true;
  await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath), /lost initial ledger/);
  assert.equal(JSON.parse(f.state.body!).transaction.phase, 'acquiring');
  assert.equal(f.state.lease, '');
  assert.deepEqual(f.state.mutations, []);
  await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath), /no replay/);
});
test('an admitted ARM mutation with a lost response is never replayed or cleaned automatically', async t => {
  const f = fixture(t); f.state.ambiguousArm = true;
  await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath), /lost ARM response/);
  assert.ok(f.state.credentials.has(runId));
  assert.ok(f.state.lease);
  assert.equal(read_run_fic_record(f.recordPath, f.c).phase, 'create-submitted');
  await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath), /no replay/);
  await assert.rejects(cleanup_run_fic(f.azure, runId, f.recordPath), /unresolved/);
  assert.deepEqual(f.state.mutations, ['PUT']);
  assert.equal(f.state.releases, 0);
});
test('an unknown ledger write retains the infinite mutex and does not start ARM work', async t => {
  const f = fixture(t); f.state.ambiguousLedger = true;
  await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath), /lost ledger response/);
  assert.ok(f.state.lease);
  assert.deepEqual(f.state.mutations, []);
  assert.equal(f.state.releases, 0);
});
test('a deleted resource with a lost DELETE receipt cannot be declared cleaned or deleted again', async t => {
  const f = fixture(t);
  await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
  f.state.absentNamespaces.add(namespace);
  f.state.ambiguousArm = true;
  await assert.rejects(cleanup_run_fic(f.azure, runId, f.recordPath), /lost ARM response/);
  assert.equal(f.state.credentials.has(runId), false);
  assert.equal(read_run_fic_record(f.recordPath, f.c).phase, 'delete-submitted');
  assert.equal(JSON.parse(f.state.body!).transaction.phase, 'active');
  assert.ok(f.state.lease);
  await assert.rejects(cleanup_run_fic(f.azure, runId, f.recordPath), /unresolved/);
  assert.deepEqual(f.state.mutations, ['PUT', 'DELETE']);
  assert.equal(f.state.releases, 1, 'only the prior definitive creation released its lease');
});
test('a later empty credential inventory cannot silently undercount native trust capacity', async t => {
  const f = fixture(t);
  let inventories = 0;
  mock.method(f.azure, 'collection', async () => ++inventories === 1 ? [f.native, f.unrelated] : []);
  await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath), /lost the required native trust/);
  assert.deepEqual(f.state.mutations, []);
  assert.equal(f.state.lease, '');
  assert.equal(f.state.body, null);
});
test('external resource drift blocks cleanup instead of repairing or deleting another version', async t => {
  const f = fixture(t);
  await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
  f.state.absentNamespaces.add(namespace);
  f.state.credentials.get(runId)!.systemData = { createdAt: '2026-01-02T00:00:00Z' };
  await assert.rejects(cleanup_run_fic(f.azure, runId, f.recordPath), /changed since definitive creation/);
  assert.deepEqual(f.state.mutations, ['PUT']);
  assert.equal(f.state.lease, '', 'definite external drift is refused before a coordination attempt');
  assert.equal(f.state.acquisitions, 1);
});
test('lost release receipt keeps the durable release-pending barrier even when the lease is absent', async t => {
  const f = fixture(t); f.state.ambiguousRelease = true;
  await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath), /lost release/);
  assert.equal(f.state.lease, '');
  assert.equal(JSON.parse(f.state.body!).transaction.phase, 'release-pending');
  await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath), /no replay/);
  await assert.rejects(cleanup_run_fic(f.azure, runId, f.recordPath), /unresolved/);
  assert.deepEqual(f.state.mutations, ['PUT']);
  assert.equal(f.state.releases, 1);
});
test('lost final acknowledgement can coexist with durable completion but does not replay ARM', async t => {
  const f = fixture(t); f.state.ambiguousCompletion = true;
  await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath), /lost completion/);
  assert.equal(JSON.parse(f.state.body!).transaction.phase, 'complete');
  assert.ok(fs.existsSync(f.recordPath + '.prepare-mutex.json.released.json'));
  assert.equal(f.state.lease, '');
  assert.deepEqual(f.state.mutations, ['PUT']);
  assert.equal(f.state.releases, 1);
  assert.ok(fs.existsSync(f.recordPath + '.prepare-mutex.json.completion-unknown.json'));
  f.state.ambiguousCompletion = false;
  f.state.absentNamespaces.add(namespace);
  await cleanup_run_fic(f.azure, runId, f.recordPath);
  assert.deepEqual(f.state.mutations, ['PUT', 'DELETE']);
  const completed = JSON.parse(f.state.body!);
  assert.notEqual(completed.entries[runId].createCompletion.leaseId, completed.entries[runId].deleteCompletion.leaseId);
  assert.ok(fs.existsSync(f.recordPath + '.prepare-mutex.json.completion-unknown.json'), 'never erase the caller uncertainty');
});
test('a different run accepts completed cleanup proof after its final acknowledgement was lost', async t => {
  const f = fixture(t);
  await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
  f.state.absentNamespaces.add(namespace);
  f.state.ambiguousCompletion = true;
  await assert.rejects(cleanup_run_fic(f.azure, runId, f.recordPath), /lost completion/);
  const old = JSON.parse(f.state.body!);
  assert.equal(old.transaction.operation, 'cleanup');
  assert.equal(old.entries[runId].record.phase, 'deleted');
  f.state.ambiguousCompletion = false;
  f.state.absentNamespaces.delete(nextNamespace);
  await prepare_run_fic(f.azure, nextRunId, nextNamespace, serviceAccount, f.recordPath + '.next');
  const current = JSON.parse(f.state.body!);
  assert.notEqual(current.transaction.leaseId, old.transaction.leaseId);
  assert.deepEqual(current.entries[runId], old.entries[runId], 'preserve the complete old receipt chain and tombstone');
  assert.deepEqual(f.state.mutations, ['PUT', 'DELETE', 'PUT']);
  assert.ok(fs.existsSync(f.recordPath + '.cleanup-mutex.json.completion-unknown.json'));
});
test('a final write that never committed blocks successors at release-pending without acquisition', async t => {
  const f = fixture(t); f.state.failCompletionBeforeCommit = true;
  await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath), /not committed/);
  const body = f.state.body;
  assert.equal(JSON.parse(body!).transaction.phase, 'release-pending');
  await assert.rejects(prepare_run_fic(f.azure, nextRunId, nextNamespace, serviceAccount, f.recordPath + '.next'), /unresolved/);
  assert.equal(f.state.body, body);
  assert.equal(f.state.acquisitions, 1);
  assert.deepEqual(f.state.mutations, ['PUT']);
});
test('a successor in the release and final-write gap cannot acquire or change the prior record', async t => {
  const f = fixture(t);
  f.state.afterRelease = async () => {
    const body = f.state.body;
    assert.equal(JSON.parse(body!).transaction.phase, 'release-pending');
    await assert.rejects(prepare_run_fic(f.azure, nextRunId, nextNamespace, serviceAccount, f.recordPath + '.racing'), /unresolved/);
    assert.equal(f.state.body, body);
    assert.equal(f.state.acquisitions, 1);
  };
  await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
  assert.equal(JSON.parse(f.state.body!).transaction.phase, 'complete');
  assert.deepEqual(f.state.mutations, ['PUT']);
});
test('complete flags with absent or mismatched release proofs never admit a successor', async t => {
  for (const corruption of ['absent', 'wrong-lease', 'wrong-owner', 'wrong-release-request']) {
    const f = fixture(t);
    await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
    const ledger = JSON.parse(f.state.body!);
    for (const tx of [ledger.transaction, ledger.entries[runId].createCompletion]) {
      if (corruption === 'absent') delete tx.releaseReceipt;
      if (corruption === 'wrong-lease') tx.leaseId = nextRunId;
      if (corruption === 'wrong-owner') tx.ownershipNonce = nextRunId;
      if (corruption === 'wrong-release-request') tx.releaseIntent.requestId = nextRunId;
    }
    f.state.body = JSON.stringify(ledger); f.state.revision++;
    const body = f.state.body;
    await assert.rejects(prepare_run_fic(f.azure, nextRunId, nextNamespace, serviceAccount, f.recordPath + '.next'),
      /receipt|release proof|owner generation/);
    assert.equal(f.state.body, body);
    assert.equal(f.state.acquisitions, 1);
    assert.deepEqual(f.state.mutations, ['PUT']);
  }
});
test('a stale conditional lease acquisition cannot overwrite the previously observed state', async t => {
  const f = fixture(t);
  await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
  f.state.absentNamespaces.add(namespace);
  f.state.beforeAcquire = () => { f.state.revision++; };
  const body = f.state.body;
  await assert.rejects(cleanup_run_fic(f.azure, runId, f.recordPath), /stale acquisition/);
  assert.equal(f.state.body, body);
  assert.equal(f.state.lease, '');
  assert.equal(f.state.acquisitions, 1);
  assert.deepEqual(f.state.mutations, ['PUT']);
});
test('capacity exhaustion, duplicate inventory and an unowned same-name credential never reach ARM', async t => {
  for (const kind of ['capacity', 'duplicate', 'same-name']) {
    const f = fixture(t);
    const extra = kind === 'capacity' ? Array.from({ length: 18 }, (_, i) => ({ name: `fixture-${i}` }))
      : kind === 'duplicate' ? [{ name: f.unrelated.name }]
      : [{ name: 'arrowcol-run-' + runId }];
    mock.method(f.azure, 'collection', async () => [f.native, f.unrelated, ...extra]);
    await assert.rejects(prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath),
      kind === 'capacity' ? /capacity is exhausted/ : kind === 'duplicate' ? /duplicate names/ : /no adoption/);
    assert.deepEqual(f.state.mutations, []);
    assert.equal(f.state.lease, '', 'deterministic preflight refusal must not acquire the mutex');
    assert.equal(f.state.body, null, 'preflight refusal must not reserve shared coordination');
    assert.equal(f.state.releases, 0);
  }
});
test('two fully proven live runs share the UAMI and clean up independently', async t => {
  const f = fixture(t), secondPath = f.recordPath + '.second';
  await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
  await prepare_run_fic(f.azure, nextRunId, nextNamespace, serviceAccount, secondPath);
  assert.equal(f.state.credentials.size, 2);
  assert.equal(read_run_fic_record(secondPath, f.c).phase, 'ready');
  const secondCredential = structuredClone(f.state.credentials.get(nextRunId));
  f.state.absentNamespaces.add(namespace);
  await cleanup_run_fic(f.azure, runId, f.recordPath);
  assert.equal(f.state.credentials.has(runId), false);
  assert.deepEqual(f.state.credentials.get(nextRunId), secondCredential);
  assert.equal(JSON.parse(f.state.body!).entries[nextRunId].record.phase, 'ready');
  f.state.absentNamespaces.add(nextNamespace);
  await cleanup_run_fic(f.azure, nextRunId, secondPath);
  assert.equal(f.state.credentials.size, 0);
  assert.equal(f.state.lease, '');
  assert.deepEqual(f.state.mutations, ['PUT', 'PUT', 'DELETE', 'DELETE']);
});
test('capacity and collision refusals leave existing ready-run cleanup available', async t => {
  for (const reason of ['capacity', 'collision']) {
    const f = fixture(t);
    await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
    f.state.additionalCredentials.push(...(reason === 'capacity'
      ? Array.from({ length: 17 }, (_, i) => ({ name: `fixture-extra-${i}` }))
      : [{ name: 'arrowcol-run-' + nextRunId }]));
    const before = f.state.body;
    await assert.rejects(prepare_run_fic(f.azure, nextRunId, nextNamespace, serviceAccount, f.recordPath + '.refused'),
      reason === 'capacity' ? /capacity is exhausted/ : /no adoption/);
    assert.equal(f.state.body, before);
    assert.equal(f.state.lease, '');
    assert.equal(f.state.acquisitions, 1);
    f.state.absentNamespaces.add(namespace);
    await cleanup_run_fic(f.azure, runId, f.recordPath);
    assert.deepEqual(f.state.mutations, ['PUT', 'DELETE']);
  }
});
test('a definite admission change under custody has only a no-ARM receipt-backed terminal path', async t => {
  const f = fixture(t);
  await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
  const entries = JSON.parse(f.state.body!).entries;
  f.state.beforeAcquire = () => {
    f.state.beforeAcquire = null;
    f.state.additionalCredentials.push(...Array.from({ length: 17 }, (_, i) => ({ name: `fixture-race-${i}` })));
  };
  await assert.rejects(prepare_run_fic(f.azure, nextRunId, nextNamespace, serviceAccount, f.recordPath + '.race'),
    /capacity is exhausted/);
  const refused = JSON.parse(f.state.body!);
  assert.equal(refused.transaction.kind, 'admission-refusal');
  assert.equal(refused.transaction.phase, 'complete');
  assert.equal(refused.transaction.refusal.code, 'capacity');
  assert.deepEqual(refused.entries, entries);
  assert.deepEqual(refused.refusals[refused.transaction.leaseId], refused.transaction);
  assert.deepEqual(f.state.mutations, ['PUT'], 'no ARM callback is reachable from an admission-only refusal');
  assert.equal(f.state.lease, '');
  f.state.absentNamespaces.add(namespace);
  await cleanup_run_fic(f.azure, runId, f.recordPath);
  assert.deepEqual(f.state.mutations, ['PUT', 'DELETE']);
  assert.deepEqual(JSON.parse(f.state.body!).refusals, refused.refusals);
});
test('an uncertain refusal release retains the barrier rather than poisoning it with false completion', async t => {
  const f = fixture(t);
  await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
  f.state.beforeAcquire = () => {
    f.state.beforeAcquire = null;
    f.state.additionalCredentials.push(...Array.from({ length: 17 }, (_, i) => ({ name: `fixture-race-${i}` })));
    f.state.ambiguousRelease = true;
  };
  await assert.rejects(prepare_run_fic(f.azure, nextRunId, nextNamespace, serviceAccount, f.recordPath + '.race'), /lost release/);
  assert.equal(JSON.parse(f.state.body!).transaction.phase, 'release-pending');
  assert.equal(JSON.parse(f.state.body!).transaction.kind, 'admission-refusal');
  await assert.rejects(cleanup_run_fic(f.azure, runId, f.recordPath), /unresolved/);
  assert.deepEqual(f.state.mutations, ['PUT']);
});
test('namespace reappearance under cleanup custody has no delete and retains exact ready authority', async t => {
  const f = fixture(t);
  await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
  const entries = JSON.parse(f.state.body!).entries;
  f.state.absentNamespaces.add(namespace);
  f.state.beforeAcquire = () => {
    f.state.beforeAcquire = null;
    f.state.absentNamespaces.delete(namespace);
  };
  await assert.rejects(cleanup_run_fic(f.azure, runId, f.recordPath), /namespace reappeared/);
  const ledger = JSON.parse(f.state.body!);
  assert.equal(ledger.transaction.kind, 'admission-refusal');
  assert.equal(ledger.transaction.operation, 'cleanup');
  assert.equal(ledger.transaction.phase, 'complete');
  assert.equal(ledger.transaction.refusal.code, 'workload');
  assert.deepEqual(ledger.entries, entries);
  assert.equal(read_run_fic_record(f.recordPath, f.c).phase, 'ready');
  assert.deepEqual(f.state.mutations, ['PUT']);
  assert.equal(f.state.lease, '');
  assert.equal(f.state.acquisitions, 2);
  assert.equal(f.state.releases, 2);
});
test('a genuinely unknown second-run ARM outcome still blocks new effects and preserves both records', async t => {
  const f = fixture(t);
  await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
  f.state.ambiguousArm = true;
  await assert.rejects(prepare_run_fic(f.azure, nextRunId, nextNamespace, serviceAccount, f.recordPath + '.unknown'), /lost ARM response/);
  assert.equal(f.state.credentials.size, 2);
  assert.equal(JSON.parse(f.state.body!).entries[runId].record.phase, 'ready');
  assert.equal(JSON.parse(f.state.body!).entries[nextRunId].record.phase, 'create-submitted');
  assert.equal(JSON.parse(f.state.body!).transaction.kind, 'mutation');
  assert.equal(JSON.parse(f.state.body!).transaction.phase, 'active');
  assert.ok(f.state.lease);
  await assert.rejects(cleanup_run_fic(f.azure, runId, f.recordPath), /unresolved/);
  assert.deepEqual(f.state.mutations, ['PUT', 'PUT']);
  assert.equal(f.state.releases, 1);
});
test('foreign, drifted and unproven ready-run state still blocks admission before acquisition', async t => {
  for (const corruption of ['foreign', 'drift', 'missing-proof']) {
    const f = fixture(t);
    await prepare_run_fic(f.azure, runId, namespace, serviceAccount, f.recordPath);
    if (corruption === 'foreign')
      f.state.additionalCredentials.push({ name: 'arrowcol-run-00000000-0000-0000-0000-000000000009' });
    if (corruption === 'drift') f.state.credentials.get(runId)!.systemData = { createdAt: '2026-01-03T00:00:00Z' };
    if (corruption === 'missing-proof') {
      const ledger = JSON.parse(f.state.body!);
      delete ledger.entries[runId].createCompletion;
      f.state.body = JSON.stringify(ledger); f.state.revision++;
    }
    const body = f.state.body;
    await assert.rejects(prepare_run_fic(f.azure, nextRunId, nextNamespace, serviceAccount, f.recordPath + '.blocked'),
      /foreign|changed|receipt-chain tombstone/);
    assert.equal(f.state.body, body);
    assert.equal(f.state.lease, '');
    assert.equal(f.state.acquisitions, 1);
    assert.deepEqual(f.state.mutations, ['PUT']);
  }
});
