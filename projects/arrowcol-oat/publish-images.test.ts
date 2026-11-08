import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { parse, stringify } from 'yaml';
import { ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { publishImages } from './publish-images.js';
import { validateRuntime } from './runtime.js';

const client = '00000000-0000-0000-0000-000000000002';
const subscription = '00000000-0000-0000-0000-000000000001';
const digest = `sha256:${'a'.repeat(64)}`;

function fixture(t: test.TestContext) {
  const root = path.join(ROOT, 'out', 'arrowcol-oat-publish-tests');
  fs.mkdirSync(root, { recursive: true });
  const directory = fs.mkdtempSync(path.join(root, 'case-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const stamp = parse(fs.readFileSync(path.join(ROOT, 'infra/azure/stamp.example.yaml'), 'utf8'));
  stamp.subscription = subscription;
  stamp.stamp = 89152;
  stamp.aks.namespace = stamp.aks.serviceAccount = 'arrow-collector';
  const registry = `arrowcollector${stamp.stamp}`;
  const options = {
    env: path.join(directory, '.env'), azureEnv: path.join(directory, '.env.azure'),
    stampConfig: path.join(directory, 'stamp.yaml'), runtimeConfig: path.join(directory, 'runtime.json'),
    registry, federated: true,
  };
  fs.writeFileSync(options.env, '');
  fs.writeFileSync(options.azureEnv, `AZURE_UAMI_CLIENT_ID=${client}\nAZURE_CONTAINER_REGISTRY_NAME=${registry}\nAZURE_CONTAINER_REGISTRY_SERVER=${registry}.azurecr.io\n`);
  fs.writeFileSync(options.stampConfig, stringify(stamp));
  fs.writeFileSync(options.runtimeConfig, JSON.stringify({
    namespace: 'arrow-collector', serviceAccount: 'arrow-collector', stamp: String(stamp.stamp),
    workloadIdentityClientId: client, subscriptionId: subscription,
    deltaRoot: `abfss://arrowcol@${registry}.dfs.core.windows.net/dataflow`,
    images: { sqlserver: 'unpublished', catalog: 'unpublished', router: 'unpublished', dataflow: 'unpublished' },
  }));
  return options;
}

function mock(registry: string, fault?: 'identity' | 'digest' | 'architecture') {
  const calls: string[][] = [];
  let azure = '';
  let docker = '';
  const runner: typeof run_tree = async (command, _timeout, _cwd, env) => {
    calls.push(command);
    let stdout = '';
    if (command[0] === 'git') stdout = command[1] === 'rev-parse' ? '0123456789ab' : '';
    else if (command[0] === 'docker' && command[1] === 'image') stdout = fault === 'architecture' ? 'linux/arm64' : 'linux/amd64';
    else if (command[0] === 'docker') {
      assert.ok(['tag', 'push'].includes(command[1]));
      assert.equal(env?.AZURE_CONFIG_DIR, azure);
      assert.equal(env?.DOCKER_CONFIG, docker);
    } else if (command[1] === 'aks') stdout = 'stamp.example.com';
    else if (command[0] === 'kubectl' && command[1] === 'config') stdout = 'https://stamp.example.com';
    else if (command[0] === 'kubectl' && command[1] === 'get') stdout = JSON.stringify({
      metadata: { name: 'arrow-collector', namespace: 'arrow-collector',
        annotations: { 'azure.workload.identity/client-id': fault === 'identity' ? subscription : client } },
    });
    else if (command[0] === 'kubectl' && command[1] === 'create') {
      assert.deepEqual(command.slice(1, 5), ['create', 'token', 'arrow-collector', '--namespace']);
      assert.ok(command.includes('api://AzureADTokenExchange'));
      stdout = 'header.payload.signature';
    } else if (command[1] === 'account') stdout = command.includes('tenantId') ? subscription : client;
    else if (command[1] === 'login') {
      azure = env?.AZURE_CONFIG_DIR ?? '';
      docker = env?.DOCKER_CONFIG ?? '';
      assert.ok(azure.startsWith(path.join(ROOT, 'out', 'image-publisher')));
      assert.equal(path.dirname(azure), path.dirname(docker));
    } else if (command[1] === 'acr') {
      assert.equal(env?.AZURE_CONFIG_DIR, azure);
      assert.equal(env?.DOCKER_CONFIG, docker);
      if (command[2] === 'show') stdout = `${registry}.azurecr.io`;
      if (command[2] === 'manifest') stdout = fault === 'digest' ? 'not-a-digest' : digest;
    } else throw new Error(`Unexpected operation ${command.slice(0, 3).join(' ')}`);
    return { returncode: 0, stdout, stderr: '' };
  };
  return { calls, runner, cache: () => path.dirname(azure) };
}

test('publishes four existing images as federated UAMI without applying identity and pins the ignored config', async t => {
  const options = fixture(t);
  const m = mock(options.registry);
  const originalAzure = process.env.AZURE_CONFIG_DIR;
  const originalDocker = process.env.DOCKER_CONFIG;
  const result = await publishImages(options, m.runner);
  assert.equal(validateRuntime(result), result);
  assert.equal(m.calls.filter(c => c[0] === 'docker' && c[1] === 'push').length, 4);
  assert.ok(m.calls.every(c => !c.includes('apply') && !c.includes('build') && !c.includes('federated-credential')));
  assert.ok(m.calls.every(c => !c.includes('--subscription') || c[c.indexOf('--subscription') + 1] === subscription));
  for (const image of Object.values(result.images)) assert.ok(image.endsWith(`@${digest}`));
  assert.deepEqual(JSON.parse(fs.readFileSync(options.runtimeConfig, 'utf8')), result);
  assert.equal(fs.statSync(options.runtimeConfig).mode & 0o777, 0o600);
  assert.equal(fs.existsSync(m.cache()), false);
  assert.equal(process.env.AZURE_CONFIG_DIR, originalAzure);
  assert.equal(process.env.DOCKER_CONFIG, originalDocker);
});

test('wrong existing ServiceAccount is rejected before minting a token or publishing', async t => {
  const options = fixture(t);
  const m = mock(options.registry, 'identity');
  await assert.rejects(publishImages(options, m.runner), /existing publisher ServiceAccount/);
  assert.ok(!m.calls.some(c => c[1] === 'create' || c[1] === 'login' || c[1] === 'push'));
});

test('invalid registry digest preserves the draft and removes isolated credential caches', async t => {
  const options = fixture(t);
  const original = fs.readFileSync(options.runtimeConfig, 'utf8');
  const m = mock(options.registry, 'digest');
  await assert.rejects(publishImages(options, m.runner), /invalid sqlserver manifest digest/);
  assert.equal(fs.readFileSync(options.runtimeConfig, 'utf8'), original);
  assert.equal(fs.existsSync(m.cache()), false);
});

test('requires federation, matching runtime subject and all amd64 images before Azure calls', async t => {
  const options = fixture(t);
  const m = mock(options.registry, 'architecture');
  await assert.rejects(publishImages({ ...options, federated: false }, m.runner), /requires --federated/);
  assert.equal(m.calls.length, 0);
  await assert.rejects(publishImages(options, m.runner), /linux\/amd64/);
  assert.ok(!m.calls.some(c => c[0] === 'az' || c[0] === 'kubectl'));
  const config = JSON.parse(fs.readFileSync(options.runtimeConfig, 'utf8'));
  config.serviceAccount = 'different';
  fs.writeFileSync(options.runtimeConfig, JSON.stringify(config));
  await assert.rejects(publishImages(options, m.runner), /reuse the selected stamp/);
});
