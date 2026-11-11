import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { parse, stringify } from 'yaml';
import { ARM_PREVIEW_FAILURE_EXIT, ROOT, run_tree, TAGS } from '../../infra/azure/bootstrap.js';
import { deploy, deployInfrastructure as runDeployInfrastructure, main, pickStamp } from './aks-deploy.js';
import { renderStampConfig } from './aks-stamp.js';
import type { DeployAttempt } from './aks-types.js';

const subscription = '00000000-0000-0000-0000-000000000001';
const stamp = '89152';
const clusterName = `arrow-collector-${stamp}`;
type Fault = 'cluster' | 'other' | 'preview' | 'unverified-preview' | 'timeout' | 'quota' | 'identity' | 'database' | 'publisher' | 'native-federation' | 'network' | 'missing-target' | 'convergence' | 'run-fic-permission';

function deployInfrastructure(
  options: Parameters<typeof runDeployInfrastructure>[0] & { stateDirectory: string }, runner: typeof run_tree,
) {
  const { stateDirectory, ...deployOptions } = options;
  return runDeployInfrastructure(deployOptions, runner, stateDirectory);
}

function fixture(t: test.TestContext, faults: Fault[] = []) {
  fs.mkdirSync(path.join(ROOT, 'out'), { recursive: true });
  const directory = fs.mkdtempSync(path.join(ROOT, 'out', 'deploy-test-'));
  const stateDirectory = path.join(directory, 'state');
  const stampConfig = path.join(directory, 'stamp.yaml');
  const config = parse(fs.readFileSync(path.join(ROOT, 'infra', 'azure', 'stamp.example.yaml'), 'utf8'));
  config.subscription = subscription;
  config.stamp = '00000';
  config.publisherFederatedCredentials = [{ name: 'publisher', issuer: 'https://issuer.example/', subject: 'publisher' }];
  fs.writeFileSync(stampConfig, stringify(config));
  const original = fs.readFileSync(stampConfig, 'utf8');
  const receiptPath = path.join(stateDirectory, 'receipts', `deploy-${stamp}.yaml`);
  const calls: string[][] = [];
  let provisionCount = 0;
  let collision = false;
  let live = false;
  let deleted = false;
  let lastFault: Fault | undefined;
  let selectedNetwork = 'azure';
  const runner: typeof run_tree = async (command, timeout, _cwd, env) => {
    calls.push(command);
    assert.ok(timeout > 0 && timeout <= 3600);
    const success = (value: unknown) => ({ returncode: 0, stdout: JSON.stringify(value), stderr: '' });
    if (command[0] === 'git') return success(null);
    if (command[0] === 'npx') {
      assert.equal(env?.AZURE_STAMP, stamp);
      assert.equal(env?.AZURE_RESOURCE_GROUP, 'arrow-collector-rg');
      assert.equal(env?.AZURE_STORAGE_ACCOUNT, `arrowcollector${stamp}`);
      if (process.env.AZURE_KUSTO_CLUSTER_TAGS !== undefined)
        assert.equal(env?.AZURE_KUSTO_CLUSTER_TAGS, process.env.AZURE_KUSTO_CLUSTER_TAGS);
      assert.equal(env?.AZURE_UAMI_CLIENT_ID, '');
      assert.equal(env?.AZURE_CLIENT_ID, undefined);
      assert.equal(env?.AZURE_CLIENT_SECRET, undefined);
      const selectedPath = command[command.indexOf('--stamp-config') + 1]!;
      selectedNetwork = parse(fs.readFileSync(selectedPath, 'utf8')).aks.networkDataplane;
      if (command[4] === 'preflight') return faults.includes('quota')
        ? { returncode: 1, stdout: '', stderr: 'Azure VM-family quota is insufficient' } : success({});
      if (command[4] === 'what-if') return success({ changes: [{ changeType: 'Create', resourceId: '/subscriptions/' + subscription + '/resourceGroups/arrow-collector-rg/providers/Microsoft.Kusto/clusters/' + clusterName }] });
      assert.equal(command[4], 'provision');
      assert.ok(command.includes('--preview-before-create'));
      const previewMarker = 'ARM_PREVIEW_VERIFIED parametersSha256=' + 'a'.repeat(64) + ' templateSha256=' + 'b'.repeat(64) + ' changes=0\n';
      lastFault = faults[provisionCount++];
      if (lastFault === 'timeout') throw new Error('Command timed out; outcome unknown');
      if (lastFault === 'preview') return { returncode: ARM_PREVIEW_FAILURE_EXIT, stdout: '', stderr: 'ARM what-if failed; no deployment create was attempted.' };
      if (lastFault === 'unverified-preview') return { returncode: 0, stdout: JSON.stringify({}), stderr: '' };
      if (lastFault === 'cluster' || lastFault === 'other' || lastFault === 'missing-target')
        return { returncode: 1, stdout: previewMarker, stderr: 'DeploymentFailed ' + lastFault + ': ARM rejected creation' };
      if (lastFault === 'convergence') return { returncode: 3, stdout: previewMarker, stderr: 'DELTA_LOGS_ABSENT' };
      return { returncode: 0, stdout: previewMarker + JSON.stringify({}), stderr: '' };
    }
    assert.equal(command[0], 'az');
    assert.equal(command[command.indexOf('--subscription') + 1], subscription);
    if (command[1] === 'account') return success({ id: subscription });
    if (command[1] === 'group') return success(live);
    if (command[1] === 'storage' || command[1] === 'acr') return success({ nameAvailable: !collision });
    if (command[1] === 'rest') return command.includes('post')
      ? success({ nameAvailable: !collision }) : success({ value: faults.includes('database') ? [] : [{ name: 'arrowcol' }, { name: 'experimentqap' }] });
    if (command[1] === 'aks' && command[2] === 'delete') { deleted = true; return success({}); }
    if (command[1] === 'aks') return success(deleted && provisionCount === 1 ? [] : [{
      name: clusterName, provisioningState: live || ['cluster', 'other', 'missing-target'].includes(lastFault ?? '') ? 'Failed' : 'Succeeded',
      tags: { ...TAGS, sandboxId: `stamp-${stamp}` }, networkProfile: { networkDataplane: faults.includes('network') ? 'wrong' : selectedNetwork },
      oidcIssuerProfile: { issuerUrl: 'https://aks.example/' },
    }]);
    if (command[1] === 'deployment' && command[2] === 'sub') {
      assert.equal(command[3], 'show');
      assert.equal(command[command.indexOf('--name') + 1], `arrowcol-sandbox-stamp-${stamp}`);
      return success({ name: `arrowcol-sandbox-stamp-${stamp}`, properties: {
        provisioningState: lastFault === 'convergence' ? 'Succeeded' : 'Failed',
      } });
    }
    if (command[1] === 'deployment' && command[2] === 'group') return success([
      { name: `arrowcol-sandbox-stamp-${stamp}`, properties: { provisioningState: 'Failed' } },
      { name: `arrowcol-aks-stamp-${stamp}`, properties: { provisioningState: 'Failed' } },
    ]);
    if (command[1] === 'deployment') return success([{ properties: {
      provisioningState: 'Failed', targetResource: lastFault === 'missing-target' ? {} : {
        resourceType: lastFault === 'cluster' ? 'Microsoft.ContainerService/managedClusters' : 'Microsoft.Storage/storageAccounts',
        resourceName: lastFault === 'cluster' ? clusterName : `arrowcollector${stamp}`,
      },
    } }]);
    if (command[1] === 'resource') return success([
      { name: 'arrow-collector', type: 'Microsoft.ManagedIdentity/userAssignedIdentities' },
      ...(faults.includes('identity') ? [{ name: 'extra', type: 'Microsoft.ManagedIdentity/userAssignedIdentities' }] : []),
      { name: `arrowcollector${stamp}`, type: 'Microsoft.Storage/storageAccounts' },
      { name: `arrowcollector${stamp}`, type: 'Microsoft.ContainerRegistry/registries' },
      { name: clusterName, type: 'Microsoft.Kusto/clusters' },
    ]);
    if (command[1] === 'role') return success(faults.includes('run-fic-permission') ? [] : [{
      principalId: subscription,
      roleDefinitionId: `/subscriptions/${subscription}/providers/Microsoft.Authorization/roleDefinitions/7e559ce2-48d7-4b27-9128-fa1b247f1308`,
    }]);
    if (command[1] === 'identity' && command[2] === 'show') return success({
      id: `/subscriptions/${subscription}/resourceGroups/arrow-collector-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/arrow-collector`,
      principalId: subscription,
    });
    if (command[1] === 'identity') return success([
      ...(faults.includes('native-federation') ? [] : [{
        name: `azure-qap-${clusterName}`, issuer: 'https://aks.example/', subject: 'system:serviceaccount:arrow-collector:arrow-collector',
        audiences: ['api://AzureADTokenExchange'],
      }]),
      ...(faults.includes('publisher') ? [] : config.publisherFederatedCredentials.map((entry: object) =>
        ({ ...entry, audiences: ['api://AzureADTokenExchange'] }))),
    ]);
    throw new Error(`Unexpected command ${command.join(' ')}`);
  };
  t.after(() => {
    fs.rmSync(directory, { recursive: true, force: true });
  });
  return { options: { stamp, stampConfig, stateDirectory }, original, runner, calls, receiptPath,
    collide: () => { collision = true; }, live: () => { live = true; },
    receipt: () => parse(fs.readFileSync(receiptPath, 'utf8')) };
}

test('fresh success is one azure pass, records explicit postconditions and private receipt', async t => {
  const f = fixture(t);
  const other = fixture(t);
  assert.notEqual(f.options.stateDirectory, other.options.stateDirectory);
  fs.mkdirSync(other.options.stateDirectory, { mode: 0o700 });
  const sentinel = path.join(other.options.stateDirectory, 'runtime.json');
  fs.writeFileSync(sentinel, 'another private output', { mode: 0o600 });
  const result = await deployInfrastructure(f.options, f.runner);
  assert.equal(result.network, 'azure');
  assert.equal(result.attempts.length, 1);
  assert.equal(result.attempts[0]!.network, 'azure');
  assert.equal(result.attempts[0]!.outcome, 'succeeded');
  assert.deepEqual(result.attempts[0]!.preview, { outcome: 'verified', parametersSha256: 'a'.repeat(64), templateSha256: 'b'.repeat(64), changes: 0 });
  assert.equal(f.calls.filter(call => call[4] === 'provision').length, 1);
  assert.ok(Object.values(f.receipt().postconditions).every(value => value === true));
  assert.equal(fs.statSync(result.receiptPath).mode & 0o777, 0o600);
  assert.equal(parse(fs.readFileSync(f.options.stampConfig, 'utf8')).stamp, stamp);
  assert.equal(fs.readFileSync(f.options.stampConfig, 'utf8'), renderStampConfig(f.original, { stamp }));
  assert.equal(fs.readFileSync(sentinel, 'utf8'), 'another private output');
  assert.equal(fs.readFileSync(other.options.stampConfig, 'utf8'), other.original);
  assert.equal(fs.existsSync(other.receiptPath), false);
});

test('ARM preview failure is recorded and never triggers create or network fallback', async t => {
  const f = fixture(t, ['preview']);
  await assert.rejects(deployInfrastructure(f.options, f.runner), /ARM what-if failed; create was not attempted/);
  assert.equal(f.receipt().attempts[0].outcome, 'preview-failed');
  assert.deepEqual(f.receipt().attempts[0].preview, { outcome: 'failed' });
  assert.equal(f.calls.filter(call => call[0] === 'npx' && call[4] === 'provision').length, 1);
  assert.equal(f.calls.some(call => call[0] === 'az' && call[1] === 'deployment'), false);
  assert.equal(f.calls.some(call => call[0] === 'az' && call[1] === 'aks' && call[2] === 'delete'), false);
});

test('missing preview evidence fails closed even when bootstrap reports success', async t => {
  const f = fixture(t, ['unverified-preview']);
  await assert.rejects(deployInfrastructure(f.options, f.runner), /ARM what-if evidence is missing or unverifiable/);
  assert.equal(f.receipt().attempts[0].outcome, 'preview-unverified');
  assert.equal(f.calls.some(call => call[0] === 'az' && call[1] === 'deployment'), false);
  assert.equal(f.calls.some(call => call[0] === 'az' && call[1] === 'aks' && call[2] === 'delete'), false);
});

test('only proven AKS failure deletes that cluster and retries Cilium once', async t => {
  const f = fixture(t, ['cluster']);
  const result = await deployInfrastructure(f.options, f.runner);
  assert.equal(result.network, 'cilium');
  assert.deepEqual(result.attempts.map(attempt => attempt.outcome), ['cluster-create-failed', 'succeeded']);
  assert.equal(f.calls.filter(call => call[2] === 'delete').length, 1);
  assert.ok(f.calls.filter(call => call[0] === 'npx' && call[4] === 'provision').every(call => call.includes('--preview-before-create')));
  assert.ok(f.receipt().attempts.every((attempt: DeployAttempt) => attempt.preview?.outcome === 'verified'));
  assert.ok(f.calls.findIndex(call => call[1] === 'deployment' && call[2] === 'sub') <
    f.calls.findIndex(call => call[2] === 'delete'));
  assert.deepEqual(f.receipt().networkPath, ['azure', 'cilium']);
  assert.match(f.receipt().attempts[0].error, /ARM rejected creation/);
  assert.equal(fs.readFileSync(f.options.stampConfig, 'utf8'), renderStampConfig(f.original, { stamp }));
});

for (const parent of [
  ...['Running', 'Accepted', 'Creating', 'Succeeded', 'Unknown'].map(provisioningState => ({
    name: `arrowcol-sandbox-stamp-${stamp}`, properties: { provisioningState },
  })),
  null, {}, { name: `arrowcol-sandbox-stamp-${stamp}`, properties: {} },
  { name: 'another-deployment', properties: { provisioningState: 'Failed' } },
]) {
  test(`failed child deployments cannot trigger fallback with unproven terminal parent: ${JSON.stringify(parent)}`, async t => {
    const f = fixture(t, ['cluster']);
    const runner: typeof run_tree = async (...args) => {
      const result = await f.runner(...args);
      return args[0][1] === 'deployment' && args[0][2] === 'sub'
        ? { ...result, stdout: JSON.stringify(parent) } : result;
    };
    await assert.rejects(deployInfrastructure(f.options, runner), /ARM rejected creation/);
    assert.equal(f.calls.filter(call => call[4] === 'provision').length, 1);
    assert.equal(f.calls.some(call => call[2] === 'delete'), false);
    assert.match(f.receipt().attempts[0].error, /ARM rejected creation/);
  });
}

test('failed subscription deployment read cannot authorize fallback', async t => {
  const f = fixture(t, ['cluster']);
  const runner: typeof run_tree = async (...args) => {
    const result = await f.runner(...args);
    return args[0][1] === 'deployment' && args[0][2] === 'sub'
      ? { returncode: 1, stdout: '', stderr: 'Polling transport failed' } : result;
  };
  await assert.rejects(deployInfrastructure(f.options, runner), /ARM rejected creation.*Could not prove.*Polling transport failed/s);
  assert.equal(f.calls.some(call => call[2] === 'delete'), false);
  assert.match(f.receipt().attempts[0].error, /ARM rejected creation/);
});

for (const fault of ['other', 'missing-target', 'timeout'] as const) {
  test(`${fault} failure cannot trigger destructive fallback`, async t => {
    const f = fixture(t, [fault]);
    await assert.rejects(deployInfrastructure(f.options, f.runner), /destroy.*--confirm arrow-collector-rg/s);
    assert.equal(f.calls.filter(call => call[4] === 'provision').length, 1);
    assert.equal(f.calls.filter(call => call[2] === 'delete').length, 0);
  });
}

test('both ARM failures persist and no third pass is allowed', async t => {
  const f = fixture(t, ['cluster', 'cluster']);
  await assert.rejects(deployInfrastructure(f.options, f.runner), /azure:.*cilium:/s);
  assert.equal(f.calls.filter(call => call[4] === 'provision').length, 2);
  assert.equal(f.receipt().attempts.length, 2);
  assert.ok(f.receipt().attempts.every((attempt: { error: string }) => attempt.error.includes('ARM rejected creation')));
});

for (const network of ['azure', 'cilium'] as const) {
  test(`pinned ${network} never changes network`, async t => {
    const f = fixture(t, ['cluster']);
    await assert.rejects(deployInfrastructure({ ...f.options, network }, f.runner), /ARM rejected creation/);
    assert.equal(f.calls.filter(call => call[4] === 'provision').length, 1);
    assert.equal(f.calls.filter(call => call[2] === 'delete').length, 0);
    assert.equal(f.receipt().attempts[0].network, network);
  });
}

test('explicit name collision refuses before rewriting YAML or provisioning', async t => {
  const f = fixture(t);
  f.collide();
  await assert.rejects(deployInfrastructure(f.options, f.runner), /name collision/);
  assert.equal(fs.readFileSync(f.options.stampConfig, 'utf8'), f.original);
  assert.equal(f.calls.some(call => call[0] === 'npx'), false);
});

test('live stamp refuses with exact destroy command', async t => {
  const f = fixture(t);
  f.live();
  await assert.rejects(deployInfrastructure(f.options, f.runner),
    new RegExp(`npx --no-install arrowcol-oat aks destroy --subscription ${subscription} --confirm arrow-collector-rg`));
  assert.equal(f.calls.some(call => call[0] === 'npx'), false);
});

test('quota failure leaves YAML unchanged and provisions nothing', async t => {
  const f = fixture(t, ['quota']);
  await assert.rejects(deployInfrastructure(f.options, f.runner), /quota is insufficient/);
  assert.equal(fs.readFileSync(f.options.stampConfig, 'utf8'), f.original);
  assert.equal(f.calls.some(call => call[4] === 'provision'), false);
});

test('what-if previews first-attempt network and never invokes provision, delete, or setup', async t => {
  const f = fixture(t);
  const result = await deploy({ ...f.options, whatIf: true }, f.runner, f.options.stateDirectory);
  assert.deepEqual(result.attempts, []);
  assert.equal(result.network, 'azure');
  assert.equal(fs.readFileSync(f.options.stampConfig, 'utf8'), f.original);
  assert.equal(f.calls.filter(call => call[4] === 'what-if').length, 1);
  assert.equal(f.calls.some(call => call[4] === 'provision' || call[2] === 'delete' || call[0] === 'kubectl'), false);
});

test('Delta convergence retries once without changing network', async t => {
  const f = fixture(t, ['convergence']);
  const result = await deployInfrastructure(f.options, f.runner);
  assert.equal(result.network, 'azure');
  assert.equal(f.calls.filter(call => call[4] === 'provision').length, 2);
  assert.equal(f.calls.some(call => call[2] === 'delete'), false);
});

for (const fault of ['identity', 'database', 'publisher', 'native-federation', 'network'] as const) {
  test(`missing ${fault} postcondition cannot claim deployed`, async t => {
    const f = fixture(t, [fault]);
    await assert.rejects(deployInfrastructure(f.options, f.runner), /postconditions failed/);
    assert.ok(Object.values(f.receipt().postconditions).includes(false));
    assert.equal(f.calls.some(call => call[2] === 'delete'), false);
  });
}

test('removed run-FIC Contributor role is not a deployment postcondition', async t => {
  const f = fixture(t, ['run-fic-permission']);
  const result = await deployInfrastructure(f.options, f.runner);
  assert.equal(result.network, 'azure');
  assert.equal(f.receipt().postconditions.runFederationPermission, undefined);
  assert.equal(f.calls.some(call => call[1] === 'role' && call[2] === 'assignment'), false);
});

test('stamp picker always produces a five-digit fresh-range integer', () => {
  for (let index = 0; index < 1000; index++) assert.match(pickStamp(), /^[1-9]\d{4}$/);
});

test('generated candidate collisions are bounded at ten with no mutation', async t => {
  const f = fixture(t);
  f.collide();
  await assert.rejects(deployInfrastructure({ stampConfig: f.options.stampConfig, stateDirectory: f.options.stateDirectory }, f.runner), /10 candidates/);
  assert.equal(f.calls.filter(call => call[1] === 'storage' &&
    call[call.indexOf('--name') + 1]?.startsWith('arrowcollector')).length, 10);
  assert.equal(fs.readFileSync(f.options.stampConfig, 'utf8'), f.original);
});

test('CLI validates network and stamp before calling Azure', async () => {
  const runner: typeof run_tree = async () => { throw new Error('Must not execute'); };
  await assert.rejects(main(['--network', 'invalid'], runner), /--network/);
  await assert.rejects(main(['--stamp', '100000'], runner), /exactly five digits/);
  await assert.rejects(main(['--stamp', '0'], runner), /exactly five digits/);
});

test('unauthenticated CLI prints the device-code login command without credentials', async t => {
  const f = fixture(t);
  const runner: typeof run_tree = async (...args) => args[0][1] === 'account'
    ? { returncode: 1, stdout: '', stderr: 'private authentication diagnostics' } : f.runner(...args);
  await assert.rejects(deployInfrastructure(f.options, runner), error =>
    error instanceof Error && error.message.includes('az login --use-device-code') &&
    !error.message.includes('private authentication diagnostics'));
  assert.equal(fs.readFileSync(f.options.stampConfig, 'utf8'), f.original);
});

test('subscription mismatch is refused before Azure commands', async t => {
  const f = fixture(t);
  await assert.rejects(deployInfrastructure({ ...f.options, subscription: '00000000-0000-0000-0000-000000000002' }, f.runner), /must match/);
  assert.equal(f.calls.length, 0);
});

test('tracked stamp configuration is never rewritten', async t => {
  const f = fixture(t);
  const runner: typeof run_tree = async (...args) => args[0][0] === 'git'
    ? { returncode: 1, stdout: '', stderr: '' } : f.runner(...args);
  await assert.rejects(deployInfrastructure(f.options, runner), /gitignored private file/);
  assert.equal(fs.readFileSync(f.options.stampConfig, 'utf8'), f.original);
});

test('stale derived state refuses instead of adopting a previous stamp', async t => {
  const f = fixture(t);
  fs.mkdirSync(f.options.stateDirectory, { recursive: true });
  const stalePath = path.join(f.options.stateDirectory, 'runtime.json');
  fs.writeFileSync(stalePath, 'operator-owned private state');
  await assert.rejects(deployInfrastructure(f.options, f.runner), /Generated state.*aks destroy/s);
  assert.equal(f.calls.some(call => call[1] === 'storage'), false);
  assert.equal(fs.readFileSync(stalePath, 'utf8'), 'operator-owned private state');
});

test('mixed resource and cluster ARM failures never delete the cluster', async t => {
  const f = fixture(t, ['cluster']);
  const runner: typeof run_tree = async (...args) => {
    const result = await f.runner(...args);
    if (args[0][1] !== 'deployment' || args[0][2] !== 'operation') return result;
    const operations = JSON.parse(result.stdout);
    operations.push({ properties: { provisioningState: 'Failed',
      targetResource: { resourceType: 'Microsoft.Storage/storageAccounts', resourceName: 'storage' } } });
    return { ...result, stdout: JSON.stringify(operations) };
  };
  await assert.rejects(deployInfrastructure(f.options, runner), /ARM rejected creation/);
  assert.equal(f.calls.some(call => call[2] === 'delete'), false);
});

for (const state of ['Creating', 'Succeeded']) {
  test(`cluster in ${state} is not a failed cluster eligible for fallback`, async t => {
    const f = fixture(t, ['cluster']);
    const runner: typeof run_tree = async (...args) => {
      const result = await f.runner(...args);
      if (args[0][1] !== 'aks') return result;
      const clusters = JSON.parse(result.stdout);
      clusters[0].provisioningState = state;
      return { ...result, stdout: JSON.stringify(clusters) };
    };
    await assert.rejects(deployInfrastructure(f.options, runner), /ARM rejected creation/);
    assert.equal(f.calls.some(call => call[2] === 'delete'), false);
  });
}

test('classification failure retains the original ARM error in the durable receipt', async t => {
  const f = fixture(t, ['cluster']);
  const runner: typeof run_tree = async (...args) => args[0][1] === 'deployment'
    ? { returncode: 1, stdout: '', stderr: 'AuthorizationFailed for deployment inventory' } : f.runner(...args);
  await assert.rejects(deployInfrastructure(f.options, runner), /ARM rejected creation.*Could not prove.*AuthorizationFailed/s);
  assert.match(f.receipt().attempts[0].error, /ARM rejected creation/);
  assert.equal(f.calls.some(call => call[2] === 'delete'), false);
});

test('bootstrap receives fresh stamp-derived env and approved Kusto tags, not stale workload identity', async t => {
  const f = fixture(t);
  const previous = process.env;
  process.env = { ...previous, AZURE_STAMP: '00000', AZURE_CLIENT_ID: 'stale', AZURE_CLIENT_SECRET: 'not-a-real-secret',
    AZURE_KUSTO_CLUSTER_TAGS: '{"classification":"test"}' };
  t.after(() => { process.env = previous; });
  await deployInfrastructure(f.options, f.runner);
  assert.equal(process.env.AZURE_STAMP, '00000');
});

test('native publisher federation works without optional external publisher credentials', async t => {
  const f = fixture(t, ['publisher']);
  const config = parse(fs.readFileSync(f.options.stampConfig, 'utf8'));
  config.publisherFederatedCredentials = [];
  fs.writeFileSync(f.options.stampConfig, stringify(config));
  const result = await deployInfrastructure(f.options, f.runner);
  assert.equal(result.network, 'azure');
  assert.equal(f.receipt().postconditions.nativeFederation, true);
});

test('diagnostic storage name collision is checked before any provisioning', async t => {
  const f = fixture(t);
  const runner: typeof run_tree = async (...args) => args[0].includes(`arrowreadlogs${stamp}`)
    ? { returncode: 0, stdout: JSON.stringify({ nameAvailable: false }), stderr: '' } : f.runner(...args);
  await assert.rejects(deployInfrastructure(f.options, runner), /name collision/);
  assert.equal(f.calls.some(call => call[0] === 'npx'), false);
  assert.equal(fs.readFileSync(f.options.stampConfig, 'utf8'), f.original);
});
