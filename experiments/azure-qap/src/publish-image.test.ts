import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { ROOT, run_tree } from '../../../infra/azure/bootstrap.js';
import { parseImageOptions, publishRuntimeImage } from './publish-image.js';

const SUBSCRIPTION = '00000000-0000-0000-0000-000000000001';
const CLIENT_ID = '00000000-0000-0000-0000-000000000002';
const DIGEST = 'sha256:' + 'a'.repeat(64);

test('requires an ACR name and validates image tags', () => {
  assert.deepEqual(parseImageOptions(['--registry', 'arrowcollector1']), {
    env: '.env', azureEnv: '.env.azure', stampConfig: 'infra/azure/stamp.yaml', registry: 'arrowcollector1',
  });
  assert.deepEqual(parseImageOptions(['--registry', 'arrowcollector1', '--env', '.private.env', '--azure-env', '.generated.env',
    '--stamp-config', '.private-stamp.yaml', '--tag', 'qap-a1b2c3']), {
    env: '.private.env', azureEnv: '.generated.env', stampConfig: '.private-stamp.yaml', registry: 'arrowcollector1', tag: 'qap-a1b2c3',
  });
  assert.throws(() => parseImageOptions(['--registry', 'bad_registry']), /registry/);
  assert.throws(() => parseImageOptions(['--registry', 'arrowcollector1', '--tag', 'UPPER']), /tag/);
});

function inputs(t: test.TestContext) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'azure-qap-image-test-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const env = path.join(directory, '.env');
  const azureEnv = path.join(directory, '.env.azure');
  const stampConfig = path.join(directory, 'stamp.yaml');
  fs.writeFileSync(env, '');
  fs.writeFileSync(azureEnv, `AZURE_UAMI_CLIENT_ID=${CLIENT_ID}\nAZURE_CONTAINER_REGISTRY_NAME=arrowcollector1\n` +
    'AZURE_CONTAINER_REGISTRY_SERVER=arrowcollector1.azurecr.io\n');
  const example = fs.readFileSync(path.join(ROOT, 'infra/azure/stamp.example.yaml'), 'utf8');
  fs.writeFileSync(stampConfig, example.replace('subscription: 00000000-0000-0000-0000-000000000000', `subscription: ${SUBSCRIPTION}`));
  return { env, azureEnv, stampConfig, registry: 'arrowcollector1' };
}

test('publishing refuses an Azure CLI identity other than the shared UAMI', async t => {
  const calls: string[][] = [];
  const runner: typeof run_tree = async command => {
    calls.push(command);
    if (command[0] === 'git' && command[1] === 'rev-parse') return { returncode: 0, stdout: 'deadbeef\n', stderr: '' };
    if (command[0] === 'git' && command[1] === 'status') return { returncode: 0, stdout: '', stderr: '' };
    if (command[0] === 'az' && command[1] === 'account') return { returncode: 0, stdout: '00000000-0000-0000-0000-000000000003\n', stderr: '' };
    throw new Error('Publishing continued beyond the identity check.');
  };
  await assert.rejects(() => publishRuntimeImage(inputs(t), runner), /single arrow-collector UAMI/);
  assert.ok(!calls.some(command => command[1] === 'acr' || command[0] === 'docker'));
});

test('publishing uses the stamped subscription and produces an immutable image reference', async t => {
  const calls: string[][] = [];
  const runner: typeof run_tree = async command => {
    calls.push(command);
    if (command[0] === 'git' && command[1] === 'rev-parse') return { returncode: 0, stdout: 'deadbeef\n', stderr: '' };
    if (command[0] === 'git' && command[1] === 'status') return { returncode: 0, stdout: '', stderr: '' };
    if (command[0] === 'az' && command[1] === 'account') return { returncode: 0, stdout: CLIENT_ID, stderr: '' };
    if (command[0] === 'az' && command[1] === 'acr' && command[2] === 'show') return { returncode: 0, stdout: 'arrowcollector1.azurecr.io', stderr: '' };
    if (command[0] === 'az' && command[1] === 'acr' && command[2] === 'manifest') return { returncode: 0, stdout: DIGEST, stderr: '' };
    return { returncode: 0, stdout: '', stderr: '' };
  };
  const image = await publishRuntimeImage(inputs(t), runner);
  assert.equal(image, `arrowcollector1.azurecr.io/azure-qap@${DIGEST}`);
  assert.ok(calls.every(command => !command.includes('--subscription') || command[command.indexOf('--subscription') + 1] === SUBSCRIPTION));
  assert.ok(calls.some(command => command[0] === 'docker' && command.includes('--push')));
});