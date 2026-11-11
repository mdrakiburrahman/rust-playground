import assert from 'node:assert/strict';
import { afterEach, mock, test } from 'node:test';
import { Config, runtime } from './bootstrap.js';
import { parse_run_fic_record, probe_fic_exchange, validate_run_identity, type RunFicRecord } from './run_fic.js';

const subscription = '00000000-0000-0000-0000-000000000001';
const c = new Config({ AZURE_SUBSCRIPTION_ID: subscription });
const runId = '9dcd6847-2a56-45a3-bd3c-54af839f451e';
const namespace = 'oat-' + runId.replaceAll('-', '');
const record = (): RunFicRecord => ({
  version: 1, runId, ownershipNonce: '00000000-0000-0000-0000-000000000005',
  namespace, namespaceUid: '00000000-0000-0000-0000-000000000002',
  namespaceOwner: 'fixture-private-owner-token', serviceAccount: 'arrowcol-oat',
  serviceAccountUid: '00000000-0000-0000-0000-000000000003',
  identityId: c.identity_id, clientId: subscription, tenantId: '00000000-0000-0000-0000-000000000004',
  aksId: c.base + '/providers/Microsoft.ContainerService/managedClusters/' + c.cluster,
  server: 'https://fixture.azmk8s.io', credentialName: 'arrowcol-run-' + runId,
  issuer: 'https://fixture.oic.prod-aks.azure.com/tenant/cluster/',
  subject: `system:serviceaccount:${namespace}:arrowcol-oat`, audience: 'api://AzureADTokenExchange', phase: 'ready',
});
const assertion = (owner = record(), patch: Record<string, unknown> = {}) => [
  Buffer.from('{"alg":"RS256"}').toString('base64url'),
  Buffer.from(JSON.stringify({ iss: owner.issuer, sub: owner.subject, aud: ['api://AzureADTokenExchange'],
    exp: Math.floor(Date.now() / 1000) + 600,
    'kubernetes.io': { namespace: owner.namespace, serviceaccount: { name: owner.serviceAccount, uid: owner.serviceAccountUid } },
    ...patch,
  })).toString('base64url'), 'fixture-signature',
].join('.');
afterEach(() => mock.restoreAll());

test('run identity rejects aliases, namespace collisions and implicit ServiceAccounts', () => {
  validate_run_identity(runId, namespace, 'arrowcol-oat');
  for (const candidate of [runId.toUpperCase(), runId.replaceAll('-', ''), '{' + runId + '}'])
    assert.throws(() => validate_run_identity(candidate, namespace, 'arrowcol-oat'), /canonical/);
  for (const candidate of ['oat', namespace + '-extra', 'wrong-' + '0'.repeat(32), 'a'.repeat(32) + '-' + runId.replaceAll('-', '')])
    assert.throws(() => validate_run_identity(runId, candidate, 'arrowcol-oat'), /namespace/);
  assert.throws(() => validate_run_identity(runId, namespace, ''), /ServiceAccount/);
});
test('custody parsing rejects foreign scope, run subject, unsafe server and unknown fields', () => {
  assert.deepEqual(parse_run_fic_record(record(), c), record());
  for (const patch of [
    { identityId: c.identity_id + '-other' },
    { aksId: record().aksId + '-other' },
    { subject: `system:serviceaccount:${namespace}:foreign` },
    { credentialName: `azure-qap-${c.cluster}` },
    { server: 'https://user:password@fixture.azmk8s.io' },
    { server: 'https://fixture.azmk8s.io/another-path' },
    { phase: 'assumed-ready' },
    { overwrite: true },
  ]) assert.throws(() => parse_run_fic_record({ ...record(), ...patch }, c));
});
test('probe exchanges an exact SA assertion only in memory and retries propagation within deadline', async () => {
  const owner = record(), token = assertion(owner);
  let elapsed = 0, requests = 0, tokens = 0;
  const output: unknown[][] = [];
  mock.method(runtime, 'now', () => elapsed);
  mock.method(runtime, 'sleep', async (seconds: number) => { elapsed += seconds; });
  mock.method(console, 'log', (...args: unknown[]) => { output.push(args); });
  mock.method(console, 'error', (...args: unknown[]) => { output.push(args); });
  mock.method(runtime, 'run_tree', async (args: string[]) => {
    tokens++;
    assert.equal(args[args.indexOf('--server') + 1], owner.server);
    assert.deepEqual(args.slice(args.indexOf('create'), args.indexOf('create') + 3), ['create', 'token', owner.serviceAccount]);
    assert.equal(args[args.indexOf('--namespace') + 1], owner.namespace);
    assert.equal(args[args.indexOf('--audience') + 1], 'api://AzureADTokenExchange');
    return { returncode: 0, stdout: token, stderr: '' };
  });
  mock.method(globalThis, 'fetch', async (_input: unknown, init?: RequestInit) => {
    const body = new URLSearchParams(init?.body as URLSearchParams);
    assert.equal(body.get('client_assertion'), token);
    assert.equal(body.get('client_id'), owner.clientId);
    requests++;
    return requests < 3
      ? new Response(JSON.stringify({ error_codes: [70021], error_description: token }), { status: requests === 1 ? 401 : 400 })
      : new Response(JSON.stringify({ access_token: 'fixture-access-token', token_type: 'Bearer', expires_in: 3600 }));
  });
  await probe_fic_exchange(owner, 30);
  assert.equal(requests, 3);
  assert.equal(tokens, 3);
  assert.equal(elapsed, 10);
  assert.equal(JSON.stringify(output).includes(token), false);
  assert.equal(JSON.stringify(output).includes('fixture-access-token'), false);
});
test('probe does not retry permanent identity-provider errors or expose their body', async () => {
  let requests = 0;
  mock.method(runtime, 'run_tree', async () => ({ returncode: 0, stdout: assertion(), stderr: '' }));
  mock.method(globalThis, 'fetch', async () => {
    requests++;
    return new Response('private failure details', { status: 403 });
  });
  await assert.rejects(probe_fic_exchange(record(), 30),
    error => error instanceof Error && error.message === 'Workload identity token exchange failed with HTTP 403.');
  assert.equal(requests, 1);
});
test('probe does not retry HTTP 401 responses with a different AADSTS code', async () => {
  let requests = 0;
  mock.method(runtime, 'run_tree', async () => ({ returncode: 0, stdout: assertion(), stderr: '' }));
  mock.method(globalThis, 'fetch', async () => {
    requests++;
    return new Response(JSON.stringify({ error_codes: [700213], error_description: 'private failure details' }), { status: 401 });
  });
  await assert.rejects(probe_fic_exchange(record(), 30),
    error => error instanceof Error && error.message === 'Workload identity token exchange failed with HTTP 401 (AADSTS 700213).');
  assert.equal(requests, 1);
});
test('probe rejects wrong assertion identity before sending it to Entra', async () => {
  mock.method(runtime, 'run_tree', async () => ({ returncode: 0, stdout: assertion(record(), { sub: 'other' }), stderr: '' }));
  mock.method(globalThis, 'fetch', async () => { throw new Error('must not exchange mismatched identity'); });
  await assert.rejects(probe_fic_exchange(record(), 30), /claims do not match/);
});
test('propagation timeout never permits workloads or retries indefinitely', async () => {
  let elapsed = 0, requests = 0;
  mock.method(runtime, 'now', () => elapsed);
  mock.method(runtime, 'sleep', async (seconds: number) => { elapsed += seconds; });
  mock.method(runtime, 'run_tree', async () => ({ returncode: 0, stdout: assertion(), stderr: '' }));
  mock.method(globalThis, 'fetch', async () => {
    requests++;
    return new Response(JSON.stringify({ error_codes: [70021] }), { status: 400 });
  });
  await assert.rejects(probe_fic_exchange(record(), 10), /bounded deadline/);
  assert.equal(elapsed, 10);
  assert.equal(requests, 2);
});
