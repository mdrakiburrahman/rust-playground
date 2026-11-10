import assert from 'node:assert/strict';
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';
import { parse, stringify } from 'yaml';
import { ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { discoverLiveStamp, main, setup } from './aks-setup.js';

const subscription = '00000000-0000-0000-0000-000000000001';
const clientId = '00000000-0000-0000-0000-000000000002';
const generation = '00000000-0000-0000-0000-000000000003';
const output = resolve(ROOT, 'out/arrowcol-oat');
const revision = '0123456789abcdef0123456789abcdef01234567';
const digest = `sha256:${'a'.repeat(64)}`;
const token = `header.${Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600 })).toString('base64url')}.signature`;
const tagFormImageMissing = `WARNING: Command group 'acr manifest' is in preview and under development. Reference and support levels: https://aka.ms/CLI_refstatus
ERROR: 2026-11-10 03:20:12.297874 Error: the specified tag does not exist. Correlation ID: 7c9221a5-5f81-4cca-8000-4dcab1f95660.
`;
const registryAbsent = `WARNING: Command group 'acr manifest' is in preview and under development. Reference and support levels: https://aka.ms/CLI_refstatus
WARNING: The resource with name 'arrowcollector99999' and type 'Microsoft.ContainerRegistry/registries' could not be found in subscription 'ArcData Monitoring Dev (68fa0545-661c-4152-93f2-fc8ae1c231b8)'.
Using 'arrowcollector99999.azurecr.io' as the default registry login server.
ERROR: Could not connect to the registry login server 'arrowcollector99999.azurecr.io'. Please verify that the registry exists and the URL 'https://arrowcollector99999.azurecr.io/v2/' is reachable from your environment.
Try running 'az acr check-health -n arrowcollector99999 --yes' to diagnose this issue.
`;

async function fixture(t: test.TestContext, stamp = '00000') {
  const parent = resolve(ROOT, 'out/aks-setup-tests');
  await mkdir(parent, { recursive: true });
  const directory = await mkdtemp(resolve(parent, 'case-'));
  t.after(() => rm(directory, { force: true, recursive: true }));
  const value = parse(await readFile(resolve(ROOT, 'infra/azure/stamp.example.yaml'), 'utf8'));
  value.subscription = subscription;
  value.stamp = stamp;
  value.aks.namespace = value.aks.serviceAccount = 'arrow-collector';
  const stampConfig = resolve(directory, 'stamp.yaml');
  await writeFile(stampConfig, `# Preserve operator settings\n${stringify(value)}`, { mode: 0o600 });
  return { stampConfig };
}

function fake(options: {
  clusters?: string[]; state?: string; preflightFailures?: number; fail?: string;
  missingImages?: boolean; registryAbsent?: boolean; badProvenance?: boolean; badDigest?: boolean;
} = {}) {
  const calls: string[][] = [];
  let preflights = 0;
  let published = !options.missingImages;
  const runner: typeof run_tree = async (command, _timeout, _cwd, env) => {
    calls.push(command);
    const text = command.join(' ');
    if (options.fail && text.includes(options.fail)) return { returncode: 1, stdout: '', stderr: 'simulated failure' };
    let stdout = '';
    if (text.startsWith('az aks list')) stdout = JSON.stringify((options.clusters ?? ['arrow-collector-89152']).map(name => ({ name })));
    else if (text.startsWith('az aks get-credentials') || command[0] === 'kubelogin') { /* credential setup */ }
    else if (text.startsWith('az aks show')) stdout = 'stamp.example.com';
    else if (text.startsWith('az identity show')) stdout = clientId;
    else if (text.startsWith('az account show')) stdout = command.includes('user.name') ? clientId : subscription;
    else if (text.startsWith('az login') || text.startsWith('az acr login')) { /* isolated publisher login */ }
    else if (text.startsWith('az acr show')) stdout = 'arrowcollector89152.azurecr.io';
    else if (text.startsWith('az storage container show')) stdout = JSON.stringify({
      properties: { etag: '"authority"' }, metadata: {
        sandboxid: 'stamp-89152', resetstate: options.state ?? 'ready-v1', resetgeneration: generation,
        occupancy: 'released', publisherregistryversion: '1', publishers: '{}',
      },
    });
    else if (command[0] === 'git' && command[1] === 'check-ignore') stdout = command.slice(2).join('\n');
    else if (command[0] === 'git' && command[1] === 'rev-parse') stdout = command.includes('--short=12') ? revision.slice(0, 12) : revision;
    else if (command[0] === 'git' && command[1] === 'status') stdout = '';
    else if (command[0] === 'kubectl' && command[1] === 'config') stdout = 'https://stamp.example.com';
    else if (command[0] === 'kubectl' && command[1] === 'get') stdout = JSON.stringify({
      metadata: { name: 'arrow-collector', namespace: 'arrow-collector',
        annotations: { 'azure.workload.identity/client-id': clientId } },
    });
    else if (command[0] === 'kubectl' && command.includes('apply')) {
      const file = command[command.indexOf('--filename') + 1]!;
      const manifest = await readFile(file, 'utf8');
      assert.match(manifest, /name: arrow-collector/);
      assert.match(manifest, new RegExp(clientId));
    } else if (command[0] === 'kubectl' && command.includes('token')) stdout = token;
    else if (text.startsWith('az acr manifest show-metadata')) {
      if (options.registryAbsent) return { returncode: 1, stdout: '', stderr: registryAbsent };
      if (!published) return { returncode: 3, stdout: '', stderr: tagFormImageMissing };
      stdout = options.badDigest ? 'invalid-digest' : digest;
    } else if (text.startsWith('docker image inspect')) {
      const reference = command.at(-1)!;
      const repository = reference.startsWith('arrowcollector89152.azurecr.io/') ? reference.split('/').at(-1)!.split(':')[0] : '';
      stdout = JSON.stringify({
        Os: 'linux', Architecture: 'amd64',
        Config: { Labels: { 'org.opencontainers.image.revision': options.badProvenance ? subscription : revision } },
        RepoDigests: repository ? [`arrowcollector89152.azurecr.io/${repository}@${digest}`] : [],
      });
    } else if (text.startsWith('docker buildx build') || text.startsWith('docker tag') || text.startsWith('docker pull')) {
      /* fake image operations */
    } else if (text.startsWith('docker push')) published = true;
    else if (command[0] === 'npx' && command.includes('--preflight-only')) {
      preflights++;
      assert.equal(env?.ARROWCOL_MODE, process.env.ARROWCOL_MODE ?? 'azure');
      assert.equal(env?.AZURE_CLIENT_ID, process.env.AZURE_CLIENT_ID ?? clientId);
      assert.ok(command.includes(resolve(output, 'oat-config.aks.json')));
      if (preflights <= (options.preflightFailures ?? 0)) return { returncode: 1, stdout: 'WAIT: propagation', stderr: '' };
      stdout = 'AKS preflight READY';
    } else throw new Error(`Unexpected fake-runner call: ${command.slice(0, 5).join(' ')}`);
    return { returncode: 0, stdout, stderr: '' };
  };
  return { runner, calls };
}

test('discovers exactly one live stamp instead of trusting stale YAML', async t => {
  const options = await fixture(t);
  const result = await discoverLiveStamp(options, fake().runner);
  assert.deepEqual(result, { stamp: '89152', subscription, resourceGroup: 'arrow-collector-rg', clusterName: 'arrow-collector-89152' });
  assert.equal(parse(await readFile(options.stampConfig, 'utf8')).stamp, '00000');
  await assert.rejects(discoverLiveStamp({ ...options, stamp: '00000' }, fake().runner), /disagrees.*aks setup --stamp 89152/);
});

test('missing, ambiguous and malformed live stamps fail with actionable guidance', async t => {
  const options = await fixture(t);
  await assert.rejects(discoverLiveStamp(options, fake({ clusters: [] }).runner), /aks deploy/);
  await assert.rejects(discoverLiveStamp(options, fake({ clusters: ['arrow-collector-89152', 'arrow-collector-00000'] }).runner),
    /arrow-collector-00000, arrow-collector-89152/);
  await assert.rejects(discoverLiveStamp(options, fake({ clusters: ['arrow-collector-123456'] }).runner), /10000..99999/);
  await assert.rejects(discoverLiveStamp(options, fake({ fail: 'az aks list' }).runner), /az login --use-device-code/);
  const absent: typeof run_tree = async () => ({ returncode: 1, stdout: '', stderr: 'ResourceGroupNotFound' });
  await assert.rejects(discoverLiveStamp(options, absent), /aks deploy/);
  const malformed: typeof run_tree = async () => ({ returncode: 0, stdout: '{bad', stderr: '' });
  await assert.rejects(discoverLiveStamp(options, malformed), /invalid AKS inventory JSON.*aks setup/);
});

test('CLI validates options before attempting any Azure operation', async () => {
  const m = fake();
  await assert.rejects(main(['--stamp', '1'], m.runner), /10000..99999/);
  await assert.rejects(main(['--unknown'], m.runner), /Unknown option/);
  await assert.rejects(main(['--tag', 'Bad Tag'], m.runner), /valid lowercase container tag/);
  assert.equal(m.calls.length, 0);
});

test('setup safely repairs YAML, reuses published images, refreshes private inputs and bounds preflight', async t => {
  // Never remove a developer's real generated inputs when running the fake-runner suite.
  await mkdir(output, { mode: 0o700 });
  t.after(() => rm(output, { force: true, recursive: true }));
  const options = await fixture(t);
  const m = fake({ preflightFailures: 1 });
  const result = await setup({ ...options, skipImages: true }, m.runner);
  assert.equal(result.stamp, '89152');
  assert.equal(result.preflightReady, true);
  assert.equal(Object.keys(result.images).length, 5);
  assert.equal(m.calls.filter(c => c.includes('--preflight-only')).length, 2);
  const yaml = await readFile(options.stampConfig, 'utf8');
  assert.match(yaml, /^# Preserve operator settings/);
  assert.equal(parse(yaml).stamp, '89152');
  assert.ok(m.calls.some(c => c[0] === 'kubelogin' && c.includes('--disable-environment-override') &&
    c[c.indexOf('--context') + 1] === 'arrow-collector-89152'));
  assert.ok(m.calls.every(c => c[0] !== 'docker' || !c.includes('build')));
  const originalPassword = await readFile(resolve(output, 'secrets/sa-password'), 'utf8');
  const again = fake();
  const second = await setup({ ...options, skipImages: true, skipPreflight: true }, again.runner);
  assert.equal(second.preflightReady, false);
  assert.equal(await readFile(resolve(output, 'secrets/sa-password'), 'utf8'), originalPassword);
  assert.equal(again.calls.some(c => c.includes('--preflight-only')), false);
  const reuse = fake();
  await setup({ ...options, skipPreflight: true }, reuse.runner);
  assert.ok(reuse.calls.every(c => c[0] !== 'docker' || !['buildx', 'push'].includes(c[1]!)));
  await chmod(options.stampConfig, 0o644);
  const fail = fake({ preflightFailures: 3 });
  await assert.rejects(setup({ ...options, skipImages: true }, fail.runner), /three bounded passes/);
  assert.equal(fail.calls.filter(c => c.includes('--preflight-only')).length, 3);
  const bad = JSON.parse(await readFile(result.runtimeConfigPath, 'utf8'));
  bad.stamp = '00000';
  await writeFile(result.runtimeConfigPath, JSON.stringify(bad));
  const stale = fake();
  await assert.rejects(setup({ ...options, skipImages: true }, stale.runner), /aks destroy --confirm arrow-collector-rg/);
  assert.equal(stale.calls.length, 1);
});

test('missing revision images build and publish all five once, then verify every registry digest', async t => {
  await mkdir(output, { mode: 0o700 });
  t.after(() => rm(output, { force: true, recursive: true }));
  const options = await fixture(t);
  const m = fake({ missingImages: true });
  const result = await setup({ ...options, skipPreflight: true }, m.runner);
  assert.equal(Object.keys(result.images).length, 5);
  assert.equal(m.calls.filter(c => c[0] === 'docker' && c[1] === 'buildx').length, 5);
  assert.equal(m.calls.filter(c => c[0] === 'docker' && c[1] === 'push').length, 5);
  assert.equal(m.calls.filter(c => c[0] === 'docker' && c[1] === 'pull').length, 5);
  for (const command of m.calls.filter(c => c[0] === 'docker' && c[1] === 'push')) {
    assert.ok(command.at(-1)!.endsWith(`:oat-${revision}`));
  }
  assert.ok(Object.values(result.images).every(image => image.endsWith(`@${digest}`)));
  await assert.rejects(readFile(resolve(output, 'runtime.publish.json')), { code: 'ENOENT' });
});

test('skip-images and hard provenance failures never trigger builds or overwrite a valid runtime', async t => {
  await mkdir(output, { mode: 0o700 });
  t.after(() => rm(output, { force: true, recursive: true }));
  const options = await fixture(t);
  const result = await setup({ ...options, skipImages: true, skipPreflight: true }, fake().runner);
  const original = await readFile(result.runtimeConfigPath, 'utf8');
  for (const [fault, pattern] of [
    [{ missingImages: true }, /without --skip-images/],
    [{ badProvenance: true }, /revision provenance/],
    [{ badDigest: true }, /invalid .* manifest digest/],
  ] as const) {
    const m = fake(fault);
    await assert.rejects(setup({ ...options, skipImages: true, skipPreflight: true }, m.runner), pattern);
    assert.ok(m.calls.every(c => c[0] !== 'docker' || !['buildx', 'push'].includes(c[1]!)));
    assert.equal(await readFile(result.runtimeConfigPath, 'utf8'), original);
  }
  const hard = fake({ badProvenance: true });
  await assert.rejects(setup({ ...options, skipPreflight: true }, hard.runner), /revision provenance/);
  assert.ok(hard.calls.every(c => c[0] !== 'docker' || !['buildx', 'push'].includes(c[1]!)));
  const failedBuild = fake({ missingImages: true, fail: 'docker buildx build' });
  await assert.rejects(setup({ ...options, skipPreflight: true }, failedBuild.runner), /Docker build failed/);
  assert.equal(await readFile(result.runtimeConfigPath, 'utf8'), original);
  await assert.rejects(readFile(resolve(output, 'runtime.publish.json')), { code: 'ENOENT' });
});

test('real absent-registry failure never becomes missing images or triggers build/publication', async t => {
  await mkdir(output, { mode: 0o700 });
  t.after(() => rm(output, { force: true, recursive: true }));
  const options = await fixture(t);
  const m = fake({ registryAbsent: true });
  await assert.rejects(setup({ ...options, skipPreflight: true }, m.runner), /Could not connect to the registry login server/);
  assert.ok(m.calls.some(c => c.slice(0, 4).join(' ') === 'az acr manifest show-metadata'));
  assert.ok(m.calls.every(c => c[0] !== 'docker' || !['buildx', 'push'].includes(c[1]!)));
  await assert.rejects(readFile(resolve(output, 'runtime.json')), { code: 'ENOENT' });
  await assert.rejects(readFile(resolve(output, 'runtime.publish.json')), { code: 'ENOENT' });
});

test('non-READY authority is refused before identity, assertion or image publication', async t => {
  await mkdir(output, { mode: 0o700 });
  t.after(() => rm(output, { force: true, recursive: true }));
  const options = await fixture(t);
  const m = fake({ state: 'fenced-v1' });
  await assert.rejects(setup({ ...options, skipImages: true }, m.runner), /ready-v1.*finish provisioning/);
  assert.ok(m.calls.every(c => !c.includes('apply') && !c.includes('token') && !c.includes('push')));
});

test('operator credential failure is actionable and does not fall back to UAMI or publish images', async t => {
  await mkdir(output, { mode: 0o700 });
  t.after(() => rm(output, { force: true, recursive: true }));
  const options = await fixture(t);
  const m = fake({ fail: 'az aks get-credentials' });
  await assert.rejects(setup({ ...options, skipImages: true }, m.runner), /Fetch AKS credentials failed.*aks setup/);
  assert.ok(m.calls.every(c => !c.includes('apply') && !c.includes('token') && !c.includes('login')));
});

test('tracked stamp configuration is refused before rewriting or requesting cluster credentials', async t => {
  const options = await fixture(t);
  const before = await readFile(options.stampConfig, 'utf8');
  const m = fake({ fail: 'git check-ignore --quiet' });
  await assert.rejects(setup({ ...options, skipImages: true }, m.runner), /Private stamp configuration ignore check failed/);
  assert.equal(await readFile(options.stampConfig, 'utf8'), before);
  assert.ok(m.calls.every(c => !c.includes('get-credentials') && !c.includes('apply') && !c.includes('token')));
});

test('generation drift is refused before changing the captured bootstrap file', async t => {
  await mkdir(output, { mode: 0o700 });
  t.after(() => rm(output, { force: true, recursive: true }));
  await mkdir(resolve(output, 'secrets'), { mode: 0o700 });
  await writeFile(resolve(output, 'secrets/generation'), subscription, { mode: 0o600 });
  const captured = `ARROWCOL_RESET_GENERATION=${subscription}\n`;
  await writeFile(resolve(output, 'bootstrap.env'), captured, { mode: 0o600 });
  const options = await fixture(t);
  const m = fake();
  await assert.rejects(setup({ ...options, skipImages: true }, m.runner), /different publisher generation/);
  assert.equal(await readFile(resolve(output, 'bootstrap.env'), 'utf8'), captured);
  assert.ok(m.calls.every(c => !c.includes('apply') && !c.includes('token') && !c.includes('push')));
});
