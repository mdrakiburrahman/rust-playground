import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { parse, stringify } from 'yaml';
import { ROOT, run_tree, TAGS } from '../../infra/azure/bootstrap.js';
import { deployInfrastructure, main, pickStamp } from './aks-deploy.js';

const subscription = '00000000-0000-0000-0000-000000000001';
const stamp = '89152';
const clusterName = `arrow-collector-${stamp}`;
const state = path.join(ROOT, 'out', 'arrowcol-oat');
type Fault = 'cluster' | 'other' | 'timeout' | 'quota' | 'identity' | 'database' | 'publisher' | 'network' | 'missing-target' | 'convergence';

function fixture(t: test.TestContext, faults: Fault[] = []) {
  fs.mkdirSync(path.join(ROOT, 'out'), { recursive: true });
  const directory = fs.mkdtempSync(path.join(ROOT, 'out', 'deploy-test-'));
  const stampConfig = path.join(directory, 'stamp.yaml');
  const config = parse(fs.readFileSync(path.join(ROOT, 'infra', 'azure', 'stamp.example.yaml'), 'utf8'));
  config.subscription = subscription;
  config.stamp = '00000';
  config.publisherFederatedCredentials = [{ name: 'publisher', issuer: 'https://issuer.example/', subject: 'publisher' }];
  fs.writeFileSync(stampConfig, stringify(config));
  const original = fs.readFileSync(stampConfig, 'utf8');
  const receiptPath = path.join(state, 'receipts', `deploy-${stamp}.yaml`);
  const calls: string[][] = [];
  let provisionCount = 0;
  let collision = false;
  let live = false;
  let deleted = false;
  let lastFault: Fault | undefined;
  let selectedNetwork = 'azure';
  const runner: typeof run_tree = async (command, timeout) => {
    calls.push(command);
    assert.ok(timeout > 0 && timeout <= 3600);
    const success = (value: unknown) => ({ returncode: 0, stdout: JSON.stringify(value), stderr: '' });
    if (command[0] === 'npx') {
      const selectedPath = command[command.indexOf('--stamp-config') + 1]!;
      selectedNetwork = parse(fs.readFileSync(selectedPath, 'utf8')).aks.networkDataplane;
      if (command[4] === 'preflight') return faults.includes('quota')
        ? { returncode: 1, stdout: '', stderr: 'Azure VM-family quota is insufficient' } : success({});
      if (command[4] === 'what-if') return success({ changes: [{ changeType: 'Create' }] });
      assert.equal(command[4], 'provision');
      lastFault = faults[provisionCount++];
      if (lastFault === 'timeout') throw new Error('Command timed out; outcome unknown');
      if (lastFault === 'cluster' || lastFault === 'other' || lastFault === 'missing-target')
        return { returncode: 1, stdout: '', stderr: `DeploymentFailed ${lastFault}: ARM rejected creation` };
      if (lastFault === 'convergence') return { returncode: 3, stdout: '', stderr: 'DELTA_LOGS_ABSENT' };
      return success({});
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
    }]);
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
    if (command[1] === 'identity') return success([
      { name: `azure-qap-${clusterName}`, issuer: 'https://aks.example/', subject: 'system:serviceaccount:arrow-collector:arrow-collector' },
      ...(faults.includes('publisher') ? [] : config.publisherFederatedCredentials),
    ]);
    throw new Error(`Unexpected command ${command.join(' ')}`);
  };
  t.after(() => {
    fs.rmSync(directory, { recursive: true, force: true });
    fs.rmSync(receiptPath, { force: true });
  });
  return { options: { stamp, stampConfig }, original, runner, calls, receiptPath,
    collide: () => { collision = true; }, live: () => { live = true; },
    receipt: () => parse(fs.readFileSync(receiptPath, 'utf8')) };
}

test('fresh success is one azure pass, records explicit postconditions and private receipt', async t => {
  const f = fixture(t);
  const result = await deployInfrastructure(f.options, f.runner);
  assert.equal(result.network, 'azure');
  assert.deepEqual(result.attempts, [{ network: 'azure', outcome: 'succeeded' }]);
  assert.equal(f.calls.filter(call => call[4] === 'provision').length, 1);
  assert.ok(Object.values(f.receipt().postconditions).every(value => value === true));
  assert.equal(fs.statSync(result.receiptPath).mode & 0o777, 0o600);
  assert.equal(parse(fs.readFileSync(f.options.stampConfig, 'utf8')).stamp, stamp);
});

test('only proven AKS failure deletes that cluster and retries Cilium once', async t => {
  const f = fixture(t, ['cluster']);
  const result = await deployInfrastructure(f.options, f.runner);
  assert.equal(result.network, 'cilium');
  assert.deepEqual(result.attempts.map(attempt => attempt.outcome), ['cluster-create-failed', 'succeeded']);
  assert.equal(f.calls.filter(call => call[2] === 'delete').length, 1);
  assert.deepEqual(f.receipt().networkPath, ['azure', 'cilium']);
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
  const result = await deployInfrastructure({ ...f.options, whatIf: true }, f.runner);
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

for (const fault of ['identity', 'database', 'publisher', 'network'] as const) {
  test(`missing ${fault} postcondition cannot claim deployed`, async t => {
    const f = fixture(t, [fault]);
    await assert.rejects(deployInfrastructure(f.options, f.runner), /postconditions failed/);
    assert.ok(Object.values(f.receipt().postconditions).includes(false));
    assert.equal(f.calls.some(call => call[2] === 'delete'), false);
  });
}

test('stamp picker always produces a five-digit fresh-range integer', () => {
  for (let index = 0; index < 1000; index++) assert.match(pickStamp(), /^[1-9]\d{4}$/);
});

test('generated candidate collisions are bounded at ten with no mutation', async t => {
  const f = fixture(t);
  f.collide();
  await assert.rejects(deployInfrastructure({ stampConfig: f.options.stampConfig }, f.runner), /10 candidates/);
  assert.equal(f.calls.filter(call => call[1] === 'storage').length, 10);
  assert.equal(fs.readFileSync(f.options.stampConfig, 'utf8'), f.original);
});

test('CLI validates network and stamp before calling Azure', async () => {
  const runner: typeof run_tree = async () => { throw new Error('Must not execute'); };
  await assert.rejects(main(['--network', 'invalid'], runner), /--network/);
  await assert.rejects(main(['--stamp', '100000'], runner), /exactly five digits/);
  await assert.rejects(main(['--stamp', '0'], runner), /exactly five digits/);
});
