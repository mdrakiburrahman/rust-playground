import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { parse, stringify } from 'yaml';
import { ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import {
  buildLocalImages, MissingPublishedImagesError, publishClientImage, publishImages, publishViewer, readPublishedImages,
} from './publish-images.js';
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

const capturedMissingTagStderr = `WARNING: Command group 'acr manifest' is in preview and under development. Reference and support levels: https://aka.ms/CLI_refstatus
ERROR: 2026-11-10 03:20:12.297874 Error: the specified tag does not exist. Correlation ID: 7c9221a5-5f81-4cca-8000-4dcab1f95660.
`;
const capturedMissingDigestStderr = `WARNING: Command group 'acr manifest' is in preview and under development. Reference and support levels: https://aka.ms/CLI_refstatus
ERROR: 2026-11-10 03:23:57.226499 Error: manifest unknown. Correlation ID: 1d08beb8-49e6-40bf-8e20-ca304981052d.
`;
const capturedRegistryAbsentStderr = `WARNING: Command group 'acr manifest' is in preview and under development. Reference and support levels: https://aka.ms/CLI_refstatus
WARNING: The resource with name 'arrowcollector99999' and type 'Microsoft.ContainerRegistry/registries' could not be found in subscription 'ArcData Monitoring Dev (68fa0545-661c-4152-93f2-fc8ae1c231b8)'.
Using 'arrowcollector99999.azurecr.io' as the default registry login server.
ERROR: Could not connect to the registry login server 'arrowcollector99999.azurecr.io'. Please verify that the registry exists and the URL 'https://arrowcollector99999.azurecr.io/v2/' is reachable from your environment.
Try running 'az acr check-health -n arrowcollector99999 --yes' to diagnose this issue.
`;

type Fault = 'identity' | 'digest' | 'architecture' | 'missing-tag' | 'missing-digest' | 'older-tags' | 'tag-still-present' | 'unknown-not-found' | 'empty-registry' |
  'registry-absent' | 'auth' | 'dns' | 'timeout' | 'throttle' | 'server' | 'provenance' | 'local-provenance' | 'published-missing';

function mock(registry: string, fault?: Fault) {
  const calls: string[][] = [];
  const tagCalls = new Map<string, number>();
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
          Os: 'linux', Architecture: fault === 'architecture' ? 'arm64' : 'amd64',
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
    } else if (command[0] === 'az' && command[1] === 'storage') {
      assert.equal(env?.AZURE_CONFIG_DIR, azure);
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
      if (command[2] === 'repository' && command[3] === 'list') {
        if (fault === 'empty-registry') stdout = '[]';
        else if (fault === 'registry-absent') { returncode = 1; stderr = capturedRegistryAbsentStderr; }
        else if (fault === 'auth') { returncode = 1; stderr = 'ERROR: 2026-11-10 03:24:00.000000 DENIED: access forbidden (HTTP 403).\n'; }
        else if (fault === 'dns') { returncode = 1; stderr = 'ERROR: Could not resolve registry login server\n'; }
        else if (fault === 'timeout') { returncode = 1; stderr = 'ERROR: request timed out while contacting registry\n'; }
        else if (fault === 'throttle') { returncode = 1; stderr = 'ERROR: 429 Too Many Requests\n'; }
        else if (fault === 'server') { returncode = 1; stderr = 'ERROR: 503 Service Unavailable\n'; }
        else stdout = JSON.stringify(['arrowcol-sqlserver', 'arrowcol-catalog-rust', 'arrowcol-router-rust', 'arrowcol-dataflow-rust', 'arrowcol-oat-client', 'delta-maintenance']);
      }
      if (command[2] === 'repository' && command[3] === 'show-tags') {
        const repository = command[command.indexOf('--repository') + 1];
        const count = tagCalls.get(repository) ?? 0;
        tagCalls.set(repository, count + 1);
        stdout = fault === 'older-tags' ? JSON.stringify([`oat-${'f'.repeat(40)}`]) :
          ['missing-tag', 'missing-digest'].includes(fault ?? '') && repository === 'arrowcol-oat-client' && count > 0 ? '[]' : JSON.stringify([`oat-${revision}`]);
      }
      if (command[2] === 'manifest') {
        const name = command[command.indexOf('--name') + 1];
        if (fault === 'missing-tag' && name.startsWith('arrowcol-oat-client:')) { returncode = 3; stderr = capturedMissingTagStderr; }
        else if (fault === 'missing-digest' && name.startsWith('arrowcol-oat-client:')) { returncode = 3; stderr = capturedMissingDigestStderr; }
        else if (fault === 'tag-still-present' && name.startsWith('arrowcol-oat-client:')) { returncode = 3; stderr = capturedMissingTagStderr; }
        else if (fault === 'unknown-not-found' && name.startsWith('arrowcol-oat-client:')) { returncode = 3; stderr = 'ERROR: unrecognized resource was not found.\n'; }
        else if (fault === 'published-missing' && name.startsWith('arrowcol-oat-client:')) { returncode = 3; stderr = capturedMissingTagStderr; }
        else stdout = fault === 'digest' ? 'not-a-digest' : digest;
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
  const clientBuild = calls.find(c => c.includes('arrowcol-oat/client:local'))!;
  assert.equal(clientBuild[clientBuild.indexOf('--file') + 1], 'projects/arrowcol-oat-rust/Dockerfile.client');
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

test('publishes the static viewer to the stamped root website through the federated UAMI', async t => {
  const options = fixture(t);
  const m = mock(options.registry);
  await assert.rejects(publishViewer({ ...options, federated: false }, m.runner), /requires --federated/);
  assert.equal(m.calls.length, 0);
  await publishViewer(options, m.runner);
  const uploads = m.calls.filter(command =>
    command[0] === 'az' && command[1] === 'storage' && command[2] === 'blob' && command[3] === 'upload');
  assert.equal(uploads.length, 2);
  const argument = (command: string[], name: string) => command[command.indexOf(name) + 1];
  assert.deepEqual(uploads.map(command => [argument(command, '--name'), argument(command, '--content-type'),
    argument(command, '--file')]).sort((a, b) => a[0].localeCompare(b[0])), [
    ['architecture.svg', 'image/svg+xml', path.join(ROOT, 'docs/experiments/arrowcol-oat/architecture.svg')],
    ['index.html', 'text/html; charset=utf-8', path.join(ROOT, 'docs/experiments/arrowcol-oat/index.html')],
  ]);
  for (const upload of uploads) {
    assert.equal(argument(upload, '--account-name'), options.registry);
    assert.equal(argument(upload, '--container-name'), '$web');
    assert.equal(argument(upload, '--auth-mode'), 'login');
    assert.equal(argument(upload, '--overwrite'), 'true');
    assert.equal(argument(upload, '--content-cache-control'), 'no-cache');
    assert.equal(argument(upload, '--subscription'), subscription);
  }
  assert.ok(!m.calls.some(command => command[0] === 'docker'));
  assert.equal(fs.existsSync(m.cache()), false);
});

test('opt-in maintenance verifies a sixth immutable revision, while legacy setup still uses five', async t => {
  const options = fixture(t);
  const m = mock(options.registry);
  const result = await readPublishedImages({ ...options, revision, maintenance: true }, m.runner);
  assert.equal(result.images.maintenance, `${options.registry}.azurecr.io/delta-maintenance@${digest}`);
  assert.equal(m.calls.filter(c => c[0] === 'docker' && c[1] === 'pull').length, 6);
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

test('published image provenance mismatch fails without building or pushing', async t => {
  const options = fixture(t);
  const m = mock(options.registry, 'provenance');
  await assert.rejects(readPublishedImages({ ...options, revision }, m.runner), /revision provenance does not match/);
  assert.ok(!m.calls.some(c => c[0] === 'docker' && ['buildx', 'push'].includes(c[1])));
});

test('existing repositories with only older revision tags return exactly the missing roles before metadata lookup', async t => {
  const options = fixture(t);
  const m = mock(options.registry, 'older-tags');
  await assert.rejects(readPublishedImages({ ...options, revision }, m.runner), error => {
    assert.ok(error instanceof MissingPublishedImagesError);
    assert.deepEqual(error.missingRoles, ['sqlserver', 'catalog', 'router', 'dataflow', 'client']);
    return true;
  });
  assert.equal(m.calls.filter(c => c.slice(1, 4).join(' ') === 'acr repository show-tags').length, 5);
  assert.ok(!m.calls.some(c => c.slice(1, 4).join(' ') === 'acr manifest show-metadata'));
  assert.ok(!m.calls.some(c => c[0] === 'docker' && ['buildx', 'push'].includes(c[1])));
});

test('empty ACR inventory returns typed missing images without calling manifest lookup', async t => {
  const options = fixture(t);
  const m = mock(options.registry, 'empty-registry');
  await assert.rejects(readPublishedImages({ ...options, revision }, m.runner), error => {
    assert.ok(error instanceof MissingPublishedImagesError);
    assert.deepEqual(error.missingRoles, ['sqlserver', 'catalog', 'router', 'dataflow', 'client']);
    return true;
  });
  assert.ok(m.calls.some(c => c.slice(1, 4).join(' ') === 'acr repository list'));
  assert.ok(!m.calls.some(c => c.slice(1, 3).join(' ') === 'acr manifest'));
});

test('real az tag-missing stderr becomes typed only after structured tag recheck confirms absence', async t => {
  const options = fixture(t);
  const m = mock(options.registry, 'missing-tag');
  await assert.rejects(readPublishedImages({ ...options, revision }, m.runner), error => {
    assert.ok(error instanceof MissingPublishedImagesError);
    assert.deepEqual(error.missingRoles, ['client']);
    return true;
  });
  assert.ok(m.calls.some(c => c.slice(1, 3).join(' ') === 'acr manifest'));
  assert.equal(m.calls.filter(c => c.slice(1, 4).join(' ') === 'acr repository show-tags' && c.includes('arrowcol-oat-client')).length, 2);
  assert.ok(!m.calls.some(c => c[0] === 'docker' && ['buildx', 'push'].includes(c[1])));
});

test('real manifest-missing fixture becomes typed after structured tag recheck confirms absence', async t => {
  const options = fixture(t);
  const m = mock(options.registry, 'missing-digest');
  await assert.rejects(readPublishedImages({ ...options, revision }, m.runner), error => {
    assert.ok(error instanceof MissingPublishedImagesError);
    assert.deepEqual(error.missingRoles, ['client']);
    return true;
  });
  assert.equal(m.calls.filter(c => c.slice(1, 4).join(' ') === 'acr repository show-tags' && c.includes('arrowcol-oat-client')).length, 2);
  assert.ok(capturedMissingTagStderr.includes('7c9221a5-5f81-4cca-8000-4dcab1f95660'));
  assert.ok(capturedMissingDigestStderr.includes('1d08beb8-49e6-40bf-8e20-ca304981052d'));
});

test('real tag-missing stderr stays hard when the structured recheck still lists the tag', async t => {
  const options = fixture(t);
  const m = mock(options.registry, 'tag-still-present');
  await assert.rejects(readPublishedImages({ ...options, revision }, m.runner), error => {
    assert.ok(!(error instanceof MissingPublishedImagesError));
    assert.match(String(error), /the specified tag does not exist/);
    return true;
  });
  assert.equal(m.calls.filter(c => c.slice(1, 4).join(' ') === 'acr repository show-tags' && c.includes('arrowcol-oat-client')).length, 2);
  assert.ok(!m.calls.some(c => c[0] === 'docker' && ['buildx', 'push'].includes(c[1])));
});

test('exit code three with unrecognized wording stays hard without a tag recheck', async t => {
  const options = fixture(t);
  const m = mock(options.registry, 'unknown-not-found');
  await assert.rejects(readPublishedImages({ ...options, revision }, m.runner), error => {
    assert.ok(!(error instanceof MissingPublishedImagesError));
    assert.match(String(error), /unrecognized resource was not found/);
    return true;
  });
  assert.equal(m.calls.filter(c => c.slice(1, 4).join(' ') === 'acr repository show-tags' && c.includes('arrowcol-oat-client')).length, 1);
  assert.ok(!m.calls.some(c => c[0] === 'docker' && ['buildx', 'push'].includes(c[1])));
});

test('registry absence and authorization errors remain hard failures', async t => {
  const options = fixture(t);
  for (const [fault, pattern] of [
    ['registry-absent', 'Could not connect to the registry login server'], ['auth', 'DENIED: access forbidden'],
    ['dns', 'Could not resolve registry login server'], ['timeout', 'request timed out'],
    ['throttle', '429 Too Many Requests'], ['server', '503 Service Unavailable'],
  ] as const) {
    const m = mock(options.registry, fault);
    await assert.rejects(readPublishedImages({ ...options, revision }, m.runner), error => {
      assert.ok(!(error instanceof MissingPublishedImagesError));
      assert.match(String(error), new RegExp(pattern));
      return true;
    });
    assert.ok(!m.calls.some(c => c[0] === 'docker' && ['buildx', 'push'].includes(c[1])));
  }
});

test('post-push missing digest lookup is a hard failure, not a rebuild signal', async t => {
  const options = fixture(t);
  const m = mock(options.registry, 'published-missing');
  await assert.rejects(publishClientImage(options, m.runner), error => {
    assert.ok(!(error instanceof MissingPublishedImagesError));
    assert.match(String(error), /the specified tag does not exist/);
    return true;
  });
});
