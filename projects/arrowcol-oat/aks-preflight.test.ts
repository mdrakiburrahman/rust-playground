import assert from 'node:assert/strict';
import { chmod, mkdir, mkdtemp, rm, symlink, unlink, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { stringify } from 'yaml';
import { defaultStampConfig } from '../../infra/azure/stamp.js';
import { formatPreflightReport, loadAksEnvironment, permitsAction, runAksPreflight } from './aks-preflight.js';

const subscription = '68fa0545-661c-4152-93f2-fc8ae1c231b8';
const clientId = 'e23159c9-5be9-427c-9edf-dd08e026ec46';
const tenant = '10000000-0000-0000-0000-000000000001';
const principalId = '20000000-0000-0000-0000-000000000001';
const runId = '30000000-0000-0000-0000-000000000001';
const generation = '40000000-0000-0000-0000-000000000001';
const now = Date.parse('2026-11-09T23:00:00Z');
const server = 'https://arrow-collector-00000.example.test';
const identityId = `/subscriptions/${subscription}/resourceGroups/arrow-collector-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/arrow-collector`;
const storageId = `/subscriptions/${subscription}/resourceGroups/arrow-collector-rg/providers/Microsoft.Storage/storageAccounts/arrowcollector00000`;
const image = (name: string) => `arrowcollector00000.azurecr.io/${name}@sha256:${'a'.repeat(64)}`;

async function fixture() {
  const root = await mkdtemp(path.join(os.tmpdir(), 'oat-preflight-'));
  await mkdir(path.join(root, 'infra/azure'), { recursive: true });
  await mkdir(path.join(root, 'out/arrowcol-oat'), { recursive: true });
  const stamp = defaultStampConfig({ AZURE_STAMP: '00000', AZURE_SUBSCRIPTION_ID: subscription, AZURE_LOCATION: 'canadacentral' });
  await writeFile(path.join(root, 'infra/azure/stamp.yaml'), stringify(stamp));
  const runtime = {
    namespace: 'arrow-collector', oatRunId: runId, workloadIdentityClientId: clientId,
    subscriptionId: subscription, stamp: '00000',
    deltaRoot: 'abfss://arrowcol@arrowcollector00000.dfs.core.windows.net/dataflow',
    images: { sqlserver: image('sqlserver'), catalog: image('catalog'), router: image('router'), dataflow: image('dataflow') },
  };
  const config = {
    platform: { kind: 'aks', runtime_config_file: 'runtime.json', bootstrap_env_file: 'bootstrap.env' },
    stamp: '00000', qap_endpoint: 'https://arrow-collector-00000.canadacentral.kusto.windows.net',
    qap_database: 'experimentqap' as string | null, replica_counts: [1], dfe_worker_concurrency: [1],
    dmv_client_image: image('client'),
    result_url: 'abfss://experiments@arrowcollector00000.dfs.core.windows.net/results.parquet',
    website_url: 'abfss://$web@arrowcollector00000.dfs.core.windows.net/results.parquet',
  };
  const assertionPath = path.join(root, 'assertion.jwt');
  const assertion = `header.${Buffer.from(JSON.stringify({ exp: now / 1000 + 3600 })).toString('base64url')}.signature-canary`;
  await writeFile(assertionPath, assertion, { mode: 0o600 });
  await writeFile(path.join(root, 'runtime.json'), JSON.stringify(runtime), { mode: 0o600 });
  await writeFile(path.join(root, 'bootstrap.env'), `ARROWCOL_RESET_GENERATION=${generation}\n`, { mode: 0o600 });
  await writeFile(path.join(root, 'oat.json'), JSON.stringify(config));
  const environment: NodeJS.ProcessEnv = {
    ARROWCOL_MODE: 'azure', AZURE_CLIENT_ID: clientId, AZURE_TENANT_ID: tenant,
    AZURE_SUBSCRIPTION_ID: subscription, AZURE_FEDERATED_TOKEN_FILE: assertionPath,
    ARROWCOL_RESET_GENERATION: generation, AZURE_KUSTO_EXPERIMENT_DATABASE: 'experimentqap',
  };
  const calls: Array<{ command: string; args: string[] }> = [];
  const requests: Array<{ url: string; init?: RequestInit }> = [];
  const fixedCredential = { name: 'arrow-collector', issuer: 'https://issuer.example.test/',
    subject: 'system:serviceaccount:arrow-collector:arrow-collector', audiences: ['api://AzureADTokenExchange'] };
  const state = {
    credentials: [fixedCredential] as Record<string, unknown>[],
    permissions: [{ actions: ['Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials/*'], notActions: [] as string[] }],
    network: { networkDataplane: 'azure', networkPolicy: 'none' },
    nodes: [] as Record<string, unknown>[],
    namespaces: [] as Record<string, unknown>[],
    pools: ['arrowcol', 'client'].map((role) => ({
      name: role, nodeLabels: { 'arrow-collector/role': role }, osType: 'Linux', provisioningState: 'Succeeded',
      count: 0, enableAutoScaling: true, maxCount: 1,
    })),
    commandFailure: '',
    metricsReady: true,
    wrongDigest: false,
    writerReady: true,
    queryStatus: 200,
    qapAdmin: true,
    ciliumReady: true,
  };
  const output = (value: unknown) => ({ stdout: typeof value === 'string' ? value : JSON.stringify(value), exitCode: 0 });
  const runCommand = (command: string, args: string[]) => {
    calls.push({ command, args: [...args] });
    if (state.commandFailure && `${command} ${args.join(' ')}`.includes(state.commandFailure)) {
      return { exitCode: 7, stdout: assertion };
    }
    if (command === 'cargo') return output('');
    if (command === 'kubelogin') return output('v0.2.22');
    if (command === 'getent') return output('10.0.0.1 selected-qap');
    if (command === 'kubectl') {
      if (args[0] === 'config' && args[1] === 'current-context') return output('arrow-collector-00000');
      if (args[0] === 'config' && args[1] === 'view') return output({
        clusters: [{ cluster: { server } }],
        users: [{ user: { exec: { command: 'kubelogin', args: ['get-token', '--subscription', subscription, '--disable-environment-override'] } } }],
      });
      if (args.includes('--raw=/version')) return output({ major: '1' });
      if (args.includes('can-i')) return output('yes');
      if (args.includes('nodes')) return output({ items: state.nodes });
      if (args.includes('ns') && args.includes('arrow-collector')) return output({ status: { phase: 'Active' } });
      if (args.includes('ns')) return output({ items: state.namespaces });
      if (args.includes('sa')) return output({ metadata: { annotations: { 'azure.workload.identity/client-id': clientId } } });
      if (args.includes('apiservice')) return output({ status: { conditions: [{ type: 'Available', status: state.metricsReady ? 'True' : 'False' }] } });
      if (args.includes('ds')) return output({ status: { desiredNumberScheduled: 3, numberReady: state.ciliumReady ? 3 : 1 } });
    }
    if (command === 'az') {
      if (args[0] === 'account' && args[1] === 'show') return output({ id: subscription, tenantId: tenant });
      if (args[0] === 'aks' && args[1] === 'show') return output({
        fqdn: new URL(server).hostname, oidcIssuerProfile: { issuerUrl: 'https://issuer.example.test/' },
        networkProfile: state.network, agentPoolProfiles: state.pools,
      });
      if (args[0] === 'identity' && args[1] === 'show') return output({ clientId, principalId, id: identityId });
      if (args.includes('federated-credential')) return output(state.credentials);
      if (args[0] === 'acr') return output({ digest: state.wrongDigest ? `sha256:${'b'.repeat(64)}` : args[args.indexOf('--image') + 1]!.split('@')[1] });
      if (args[0] === 'storage' && args[1] === 'account') return output({ id: storageId });
      if (args[0] === 'storage' && args[1] === 'container') return output({});
      if (args.includes('service-properties')) return output({ staticWebsite: { enabled: true } });
      if (args[0] === 'storage' && args.includes('list')) return output([]);
      if (args[0] === 'role') return output(state.writerReady ? [{
        principalId, roleDefinitionName: 'Storage Blob Data Contributor', scope: storageId,
      }] : []);
    }
    throw new Error(`Unexpected fake command: ${command} ${args.join(' ')}`);
  };
  const fetcher: typeof fetch = async (input, init) => {
    const url = String(input);
    requests.push({ url, init });
    if (url.includes('/oauth2/v2.0/token')) {
      assert.equal(new URLSearchParams(String(init?.body)).get('client_id'), clientId);
      assert.equal(new URLSearchParams(String(init?.body)).get('client_assertion'), assertion);
      return Response.json({ access_token: 'access-token-canary' });
    }
    assert.equal(new Headers(init?.headers).get('authorization'), 'Bearer access-token-canary');
    if (url.includes('Microsoft.Authorization/permissions')) {
      assert.equal(init?.method ?? 'GET', 'GET');
      return Response.json({ value: state.permissions });
    }
    const body = JSON.parse(String(init?.body));
    assert.equal(body.db, 'experimentqap');
    if (url.endsWith('/v1/rest/query')) return Response.json({ Tables: [] }, { status: state.queryStatus });
    if (url.endsWith('/v1/rest/mgmt')) {
      assert.equal(body.csl, '.show database principals');
      return Response.json({ Tables: [{
        Columns: [{ ColumnName: 'Role' }, { ColumnName: 'PrincipalObjectId' }],
        Rows: state.qapAdmin ? [['Database Admin', principalId]] : [],
      }] });
    }
    throw new Error('Unexpected fake provider request');
  };
  const run = () => runAksPreflight('oat.json', { workspaceRoot: root, environment, runCommand, fetcher, now: () => now });
  return { root, config, runtime, environment, calls, requests, state, assertion, run,
    saveConfig: () => writeFile(path.join(root, 'oat.json'), JSON.stringify(config)),
    cleanup: () => rm(root, { recursive: true, force: true }) };
}

test('fresh-run preflight passes without staged workloads and exposes the no-provider risk', async () => {
  const f = await fixture();
  try {
    const report = await f.run();
    assert.deepEqual(report.checks.filter((entry) => !entry.ready), []);
    assert.equal(report.ready, true);
    assert.equal(report.checks.length, 13);
    assert.deepEqual(report.network, { provider: 'none', enforcing: false });
    assert.match(formatPreflightReport(report), /WARNING:.*passwordless port/);
    assert.equal(f.calls.filter((call) => call.command === 'az' && call.args[0] === 'acr').length, 5);
    assert.ok(f.calls.filter((call) => call.command === 'az').every((call) => call.args.includes('--subscription')));
    assert.ok(f.calls.filter((call) => call.command === 'kubectl' && call.args.includes('get'))
      .every((call) => call.args.includes('--request-timeout=30s')));
    assert.ok(!f.calls.some((call) => call.args.some((arg) => ['secret', 'deploy', 'svc', 'pods', 'port-forward', 'apply', 'scale'].includes(arg))));
    assert.ok(!f.calls.some((call) => ['create', 'delete'].some((verb) => call.args.includes(verb) && !call.args.includes('can-i'))));
    assert.ok(!f.calls.some((call) => call.command === 'az' &&
      (call.args[0] === 'login' || (call.args[0] === 'aks' && call.args[1] === 'get-credentials'))));
    assert.ok(!JSON.stringify(report).includes(f.assertion));
    assert.ok(!JSON.stringify(report).includes('access-token-canary'));
  } finally { await f.cleanup(); }
});

test('null QAP and invalid config do not mask independent readiness gates', async () => {
  const f = await fixture();
  try {
    f.config.qap_database = null;
    await f.saveConfig();
    let report = await f.run();
    assert.equal(report.ready, false);
    assert.match(report.checks.find((entry) => entry.name.startsWith('Selected QAP'))!.detail, /aks setup/);
    assert.equal(report.checks.find((entry) => entry.name === 'Metrics API availability')!.ready, true);
    await writeFile(path.join(f.root, 'oat.json'), '{}');
    report = await f.run();
    assert.equal(report.checks.length, 13);
    assert.match(report.checks[0]!.detail, /aks setup/);
    assert.ok(report.checks.some((entry) => entry.name === 'Metrics API availability'));
  } finally { await f.cleanup(); }
});

test('configuration mismatches, private input custody and bootstrap generation fail closed', async () => {
  const f = await fixture();
  try {
    f.config.stamp = '123456';
    await f.saveConfig();
    assert.equal((await f.run()).checks[0]!.ready, false);
    f.config.stamp = '00000';
    await f.saveConfig();
    await chmod(path.join(f.root, 'runtime.json'), 0o644);
    assert.equal((await f.run()).checks.find((entry) => entry.name.startsWith('Run-scoped runtime'))!.ready, false);
    await chmod(path.join(f.root, 'runtime.json'), 0o600);
    await writeFile(path.join(f.root, 'bootstrap.env'), `ARROWCOL_RESET_GENERATION=${runId}`);
    assert.equal((await f.run()).checks.find((entry) => entry.name.startsWith('Run-scoped runtime'))!.ready, false);
  } finally { await f.cleanup(); }
});

test('expired or mismatched publisher credentials block federation without disclosing assertions', async () => {
  const f = await fixture();
  try {
    const expired = `header.${Buffer.from(JSON.stringify({ exp: now / 1000 - 1 })).toString('base64url')}.secret-canary`;
    await writeFile(f.environment.AZURE_FEDERATED_TOKEN_FILE!, expired);
    const report = await f.run();
    assert.equal(report.checks.find((entry) => entry.name === 'Kubernetes context and credentials')!.ready, false);
    assert.match(report.checks.find((entry) => entry.name === 'Kubernetes context and credentials')!.detail, /expired.*aks setup/);
    assert.ok(!formatPreflightReport(report).includes(expired));
    assert.equal(f.requests.length, 0);
    f.environment.AZURE_CLIENT_ID = runId;
    assert.equal((await f.run()).checks.find((entry) => entry.name === 'Kubernetes context and credentials')!.ready, false);
  } finally { await f.cleanup(); }
});

test('publisher identity and exact fixed federation remain mandatory', async () => {
  const f = await fixture();
  try {
    f.state.commandFailure = 'get sa';
    assert.equal((await f.run()).checks.find((entry) => entry.name === 'Fixed publisher identity')!.ready, false);
    f.state.commandFailure = '';
    f.state.credentials[0]!.subject = 'system:serviceaccount:wrong:wrong';
    assert.equal((await f.run()).checks.find((entry) => entry.name === 'Federated credential')!.ready, false);
  } finally { await f.cleanup(); }
});

test('namespace permission and scale-from-zero pool headroom are checked without scaling', async () => {
  const f = await fixture();
  try {
    f.state.commandFailure = 'can-i create';
    assert.equal((await f.run()).checks.find((entry) => entry.name.startsWith('Namespace permissions'))!.ready, false);
    f.state.commandFailure = '';
    f.state.pools[0]!.maxCount = 0;
    assert.equal((await f.run()).checks.find((entry) => entry.name.startsWith('Namespace permissions'))!.ready, false);
    f.state.pools[0]!.maxCount = 1;
    f.state.nodes = [{ status: { conditions: [{ type: 'Ready', status: 'False' }] } }];
    assert.equal((await f.run()).checks.find((entry) => entry.name.startsWith('Namespace permissions'))!.ready, false);
  } finally { await f.cleanup(); }
});

test('image, metrics, QAP binding and storage failures are independent and meaningful', async () => {
  const f = await fixture();
  try {
    f.state.wrongDigest = true;
    f.state.metricsReady = false;
    f.state.qapAdmin = false;
    f.state.writerReady = false;
    const report = await f.run();
    for (const prefix of ['Immutable ACR', 'Metrics API', 'Selected QAP', 'Storage and']) {
      assert.equal(report.checks.find((entry) => entry.name.startsWith(prefix))!.ready, false);
    }
    assert.equal(report.checks.find((entry) => entry.name === 'Fixed publisher identity')!.ready, true);
  } finally { await f.cleanup(); }
});

test('Cilium requires healthy agents and a current exact-cluster behavioral receipt', async () => {
  const f = await fixture();
  try {
    f.state.network = { networkDataplane: 'cilium', networkPolicy: 'cilium' };
    assert.equal((await f.run()).checks.find((entry) => entry.name.startsWith('Network-policy'))!.ready, false);
    const proofPath = path.join(f.root, 'proof.json');
    f.environment.ARROWCOL_NETWORK_POLICY_RECEIPT = proofPath;
    const proof = { clusterServer: server, ownerApproved: true, positivePath: true, negativePath: true, recordedAt: new Date(now).toISOString() };
    await writeFile(proofPath, JSON.stringify(proof));
    let report = await f.run();
    assert.equal(report.ready, true);
    assert.deepEqual(report.network, { provider: 'cilium', enforcing: true });
    assert.deepEqual(report.warnings, []);
    await writeFile(proofPath, JSON.stringify({ ...proof, recordedAt: new Date(now + 1000).toISOString() }));
    report = await f.run();
    assert.equal(report.checks.find((entry) => entry.name.startsWith('Network-policy'))!.ready, false);
    await writeFile(proofPath, JSON.stringify(proof));
    f.state.ciliumReady = false;
    assert.equal((await f.run()).checks.find((entry) => entry.name.startsWith('Network-policy'))!.ready, false);
  } finally { await f.cleanup(); }
});

test('FIC effective permissions use the workload UAMI and require both write and delete', async () => {
  const f = await fixture();
  try {
    f.state.permissions[0]!.notActions = ['Microsoft.ManagedIdentity/*/federatedIdentityCredentials/delete'];
    const report = await f.run();
    assert.equal(report.checks.find((entry) => entry.name.startsWith('Run federated'))!.ready, false);
    assert.match(report.checks.find((entry) => entry.name.startsWith('Run federated'))!.detail, /delete permission/);
    assert.ok(f.requests.some((request) => request.url.includes(`${identityId}/providers/Microsoft.Authorization/permissions`)));
    assert.equal(report.checks.find((entry) => entry.name === 'Run federated-credential headroom')!.ready, true);
  } finally { await f.cleanup(); }
  assert.equal(permitsAction([{ actions: ['*'], notActions: ['Microsoft.ManagedIdentity/*'] }], 'Microsoft.ManagedIdentity/write'), false);
  assert.equal(permitsAction([{ actions: ['Microsoft.ManagedIdentity/*'], notActions: [] }], 'Microsoft.ManagedIdentity/write'), true);
});

test('FIC headroom counts all trusts while leaked run resources are warnings, not silent cleanup', async () => {
  const f = await fixture();
  try {
    f.state.credentials.push({ name: `arrowcol-run-${runId}` });
    f.state.namespaces.push({ metadata: { name: `arrow-collector-${runId.replaceAll('-', '')}` } });
    let report = await f.run();
    assert.equal(report.ready, true);
    assert.ok(report.warnings.some((warning) => warning.includes('orphan')));
    assert.ok(report.warnings.some((warning) => warning.includes('leaked')));
    for (let index = f.state.credentials.length; index < 20; index++) f.state.credentials.push({ name: `external-trust-${index}` });
    report = await f.run();
    assert.equal(report.checks.find((entry) => entry.name === 'Run federated-credential headroom')!.ready, false);
    assert.match(formatPreflightReport(report), /limit of 20/);
  } finally { await f.cleanup(); }
});

test('aks.env preserves generated child-helper fields, real overrides and never evaluates shell syntax', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'oat-aks-env-'));
  const filename = path.join(root, 'out/arrowcol-oat/aks.env');
  try {
    assert.deepEqual(await loadAksEnvironment(root, { PATH: '/bin' }), { PATH: '/bin' });
    await mkdir(path.dirname(filename), { recursive: true });
    await writeFile(filename, "ARROWCOL_MODE=azure\nAZURE_CLIENT_ID=from-file\nAZURE_KUSTO_EXPERIMENT_DATABASE=experimentqap\nARROWCOL_RESET_GENERATION=captured\nCUSTOM_VALUE='$(never-execute)'\n", { mode: 0o600 });
    const merged = await loadAksEnvironment(root, { AZURE_CLIENT_ID: 'real-env', PATH: '/bin', UNSET: undefined });
    assert.equal(merged.AZURE_CLIENT_ID, 'real-env');
    assert.equal(merged.AZURE_KUSTO_EXPERIMENT_DATABASE, 'experimentqap');
    assert.equal(merged.ARROWCOL_RESET_GENERATION, 'captured');
    assert.equal(merged.CUSTOM_VALUE, '$(never-execute)');
    assert.equal(merged.PATH, '/bin');
    await chmod(filename, 0o644);
    await assert.rejects(loadAksEnvironment(root, {}), /aks setup/);
    await chmod(filename, 0o600);
    await writeFile(filename, 'KEY=one\nKEY=two\n');
    await assert.rejects(loadAksEnvironment(root, {}), /duplicate/);
    await unlink(filename);
    const target = path.join(root, 'target');
    await writeFile(target, 'KEY=value', { mode: 0o600 });
    await symlink(target, filename);
    await assert.rejects(loadAksEnvironment(root, {}), /aks setup/);
  } finally { await rm(root, { recursive: true, force: true }); }
});
