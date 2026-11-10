import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { parse, stringify } from 'yaml';
import { ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { buildLocalImages, MissingPublishedImagesError, publishClientImage, publishImages, readPublishedImages } from './publish-images.js';
import { validateRuntime } from './runtime.js';

const client = '00000000-0000-0000-0000-000000000002';
const subscription = '00000000-0000-0000-0000-000000000001';
const digest = `sha256:${'a'.repeat(64)}`;
const revision = '0123456789abcdef0123456789abcdef01234567';

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
    oatRunId: '00000000-0000-0000-0000-000000000003',
    workloadIdentityClientId: client, subscriptionId: subscription,
    deltaRoot: `abfss://arrowcol@${registry}.dfs.core.windows.net/dataflow`,
    images: { sqlserver: 'unpublished', catalog: 'unpublished', router: 'unpublished', dataflow: 'unpublished' },
  }));
  return options;
}

function mock(registry: string, fault?: 'identity' | 'digest' | 'architecture' | 'missing' | 'provenance' | 'local-provenance') {
  const calls: string[][] = [];
  let azure = '';
  let docker = '';
  const runner: typeof run_tree = async (command, _timeout, _cwd, env) => {
    calls.push(command);
    let stdout = '';
    let stderr = '';
    let returncode = 0;
    if (command[0] === 'git') {
      if (command[1] === 'rev-parse') stdout = command.includes('--short=12') ? revision.slice(0, 12) : revision;
    } else if (command[0] === 'docker' && command[1] === 'image') {
      if (command[2] === 'inspect' && command.includes('{{json .}}')) {
        const reference = command.at(-1)!;
        const repo = reference.startsWith(`${registry}.azurecr.io/`) ? reference.split('/').at(-1)!.split(':')[0] : '';
        stdout = JSON.stringify({
          Os: fault === 'architecture' ? 'linux' : 'linux', Architecture: fault === 'architecture' ? 'arm64' : 'amd64',
          Config: { Labels: { 'org.opencontainers.image.revision': fault === 'provenance' || (fault === 'local-provenance' && !repo) ? subscription : revision } },
          ...(repo ? { RepoDigests: [`${registry}.azurecr.io/${repo}@${digest}`] } : {}),
        });
      } else stdout = fault === 'architecture' ? 'linux/arm64' : 'linux/amd64';
    } else if (command[0] === 'docker' && command[1] === 'buildx') {
      assert.equal(command[2], 'build');
      assert.ok(command.includes('--load'));
    } else if (command[0] === 'docker') {
      assert.ok(['tag', 'push', 'pull'].includes(command[1]));
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
      if (command[2] === 'manifest') {
        const name = command[command.indexOf('--name') + 1];
        if (fault === 'missing' && name.startsWith('arrowcol-oat-client:')) {
          returncode = 1;
          stderr = `MANIFEST_UNKNOWN: manifest tagged by '${name}' is not found`;
        } else stdout = fault === 'digest' ? 'not-a-digest' : digest;
      }
    } else throw new Error(`Unexpected operation ${command.slice(0, 3).join(' ')}`);
    return { returncode, stdout, stderr };
  };
  return { calls, runner, cache: () => path.dirname(azure) };
}
test('publishes four existing images as federated UAMI without applying identity and pins the ignored config', async t => {
  const options = fixture(t);
  const m = mock(options.registry);
  const originalAzure = process.env.AZURE_CONFIG_DIR;
  const originalDocker = process.env.DOCKER_CONFIG;
  const result = await publishImages(options, m.runner);
  assert.equal(result.oatRunId, '00000000-0000-0000-0000-000000000003');
  assert.equal(validateRuntime(result), result);
  assert.equal(m.calls.filter(c => c[0] === 'docker' && c[1] === 'push').length, 4);
  assert.ok(m.calls.filter(c => c[0] === 'docker' && c[1] === 'tag').every(c => c.at(-1)?.endsWith(`:oat-${revision}`)));
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

test('stale local image provenance is rejected before Azure credentials are used', async t => {
  const options = fixture(t);
  const m = mock(options.registry, 'local-provenance');
  await assert.rejects(publishImages(options, m.runner), /Local sqlserver image revision provenance/);
  assert.ok(!m.calls.some(c => c[0] === 'az' || c[0] === 'kubectl'));
});

test('invalid registry digest preserves the draft and removes isolated credential caches', async t => {
  const options = fixture(t);
  const original = fs.readFileSync(options.runtimeConfig, 'utf8');
  const m = mock(options.registry, 'digest');
  await assert.rejects(publishImages(options, m.runner), /invalid sqlserver manifest digest/);
  assert.equal(fs.readFileSync(options.runtimeConfig, 'utf8'), original);
  assert.equal(fs.existsSync(m.cache()), false);
});

test('missing or malformed run identity is rejected before credentials or publication', async t => {
  const options = fixture(t);
  const valid = fs.readFileSync(options.runtimeConfig, 'utf8');
  for (const runId of [undefined, 'not-a-uuid']) {
    const config = JSON.parse(valid);
    if (runId === undefined) delete config.oatRunId;
    else config.oatRunId = runId;
    const original = JSON.stringify(config);
    fs.writeFileSync(options.runtimeConfig, original);
    const m = mock(options.registry);
    await assert.rejects(publishImages(options, m.runner),
      runId === undefined ? /missing or unknown fields/ : /oatRunId/);
    assert.ok(m.calls.every(c => c[0] === 'git' && c[1] === 'check-ignore'));
    assert.equal(fs.readFileSync(options.runtimeConfig, 'utf8'), original);
  }
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


test('builds all five local images for linux/amd64 with the full revision label', async () => {
  const calls: string[][] = [];
  const runner: typeof run_tree = async command => {
    if (command[0] === 'git') return { returncode: 0, stdout: command[1] === 'rev-parse' ? revision : '', stderr: '' };
    calls.push(command);
    return { returncode: 0, stdout: '', stderr: '' };
  };
  await buildLocalImages({ revision }, runner);
  assert.equal(calls.filter(c => c[0] === 'docker').length, 5);
  assert.ok(calls.every(c => c.slice(0, 3).join(' ') === 'docker buildx build' && c.includes('linux/amd64') && c.includes('--load')));
  assert.ok(calls.every(c => c.includes(`org.opencontainers.image.revision=${revision}`)));
  assert.deepEqual(calls.map(c => c[c.indexOf('--tag') + 1]), [
    'arrowcol-oat/sqlserver:local', 'arrowcol-oat/catalog:local', 'arrowcol-oat/router:local',
    'arrowcol-oat/dataflow:local', 'arrowcol-oat/client:local',
  ]);
  calls.length = 0;
  await assert.rejects(buildLocalImages({ revision: 'abc' }, runner), /full 40-character/);
  assert.equal(calls.length, 0);
  const dirtyRunner: typeof run_tree = async command => ({
    returncode: 0, stdout: command[1] === 'rev-parse' ? revision : ' M file.ts', stderr: '',
  });
  await assert.rejects(buildLocalImages({ revision }, dirtyRunner), /Commit the source changes/);
});

test('publishes the revision-proven client image as a digest-pinned ACR reference', async t => {
  const options = fixture(t);
  const m = mock(options.registry);
  const image = await publishClientImage(options, m.runner);
  assert.equal(image, `${options.registry}.azurecr.io/arrowcol-oat-client@${digest}`);
  assert.ok(m.calls.some(c => c[0] === 'docker' && c[1] === 'push' && c[2].endsWith(`:oat-${revision}`)));
  assert.equal(m.calls.filter(c => c[0] === 'docker' && c[1] === 'push').length, 1);
  assert.ok(!m.calls.some(c => c[1] === 'apply' || c[1] === 'buildx'));
  assert.equal(fs.existsSync(m.cache()), false);
});

test('reads and verifies all five revision tags without building or pushing', async t => {
  const options = fixture(t);
  const m = mock(options.registry);
  const result = await readPublishedImages({ ...options, revision }, m.runner);
  assert.deepEqual(result, {
    images: {
      sqlserver: `${options.registry}.azurecr.io/arrowcol-sqlserver@${digest}`,
      catalog: `${options.registry}.azurecr.io/arrowcol-catalog-rust@${digest}`,
      router: `${options.registry}.azurecr.io/arrowcol-router-rust@${digest}`,
      dataflow: `${options.registry}.azurecr.io/arrowcol-dataflow-rust@${digest}`,
    },
    client: `${options.registry}.azurecr.io/arrowcol-oat-client@${digest}`,
  });
  assert.equal(m.calls.filter(c => c[0] === 'docker' && c[1] === 'pull').length, 5);
  assert.ok(!m.calls.some(c => c[0] === 'docker' && ['buildx', 'push', 'tag'].includes(c[1])));
  assert.ok(m.calls.filter(c => c[0] === 'az' && c[2] === 'manifest').every(c => c.some(arg => arg.includes(`oat-${revision}`))));
});

test('reports absent revision tags distinctly while provenance mismatches fail closed', async t => {
  const options = fixture(t);
  const missing = mock(options.registry, 'missing');
  await assert.rejects(readPublishedImages({ ...options, revision }, missing.runner), error => {
    assert.ok(error instanceof MissingPublishedImagesError);
    assert.deepEqual(error.missingRoles, ['client']);
    return true;
  });
  assert.ok(!missing.calls.some(c => c[0] === 'docker' && ['buildx', 'push'].includes(c[1])));
  const badProvenance = mock(options.registry, 'provenance');
  await assert.rejects(readPublishedImages({ ...options, revision }, badProvenance.runner), /revision provenance does not match/);
  assert.ok(!badProvenance.calls.some(c => c[0] === 'docker' && ['buildx', 'push'].includes(c[1])));
});
