import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { chmod, lstat, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';
import { load_env, ROOT } from '../../infra/azure/bootstrap.js';
import { generatePrivateInputs, renderBootstrapEnv, type PrivateInputs, type RuntimeConfig } from './runtime.js';

const generation = '00000000-0000-0000-0000-000000000003';
const tenantId = '00000000-0000-0000-0000-000000000004';
const assertion = (exp = Math.floor(Date.now() / 1000) + 3600) =>
  `header.${Buffer.from(JSON.stringify({ exp })).toString('base64url')}.signature`;
const digest = 'a'.repeat(64);

async function fixture(t: test.TestContext): Promise<PrivateInputs> {
  const parent = resolve(ROOT, 'out/arrowcol-private-input-tests');
  await mkdir(parent, { recursive: true });
  const directory = await mkdtemp(resolve(parent, 'case-'));
  t.after(() => rm(directory, { force: true, recursive: true }));
  const registry = 'arrowcollector89152.azurecr.io';
  const runtime: RuntimeConfig = {
    namespace: 'arrow-collector', serviceAccount: 'arrow-collector', oatRunId: randomUUID(),
    workloadIdentityClientId: '00000000-0000-0000-0000-000000000002',
    subscriptionId: '00000000-0000-0000-0000-000000000001', stamp: '89152',
    deltaRoot: 'abfss://arrowcol@arrowcollector89152.dfs.core.windows.net/dataflow',
    images: {
      sqlserver: `${registry}/arrowcol-sqlserver@sha256:${digest}`,
      catalog: `${registry}/arrowcol-catalog-rust@sha256:${digest}`,
      router: `${registry}/arrowcol-router-rust@sha256:${digest}`,
      dataflow: `${registry}/arrowcol-dataflow-rust@sha256:${digest}`,
    },
  };
  return { directory, runtime, tenantId, resetGeneration: generation, assertion: assertion(),
    bootstrapValues: { AZURE_SUBSCRIPTION_ID: runtime.subscriptionId, AZURE_STAMP: '89152' },
    clientImage: `${registry}/arrowcol-oat-client@sha256:${digest}`,
    qapEndpoint: 'https://arrow-collector-89152.canadacentral.kusto.windows.net',
    oatTemplate: JSON.parse(await readFile(resolve(ROOT,
      'docs/experiments/arrowcol-oat/oat-config.aks.quick.json'), 'utf8')) as Record<string, unknown> };
}

test('generates private inputs from authoritative generation and pins the complete AKS configuration', async t => {
  const input = await fixture(t);
  const files = await generatePrivateInputs(input);
  assert.deepEqual(JSON.parse(await readFile(files.runtimeConfigPath, 'utf8')), input.runtime);
  const bootstrap = load_env(files.bootstrapEnvPath);
  assert.equal(bootstrap.ARROWCOL_RESET_GENERATION, generation);
  assert.equal(bootstrap.AZURE_SUBSCRIPTION_ID, input.runtime.subscriptionId);
  const env = load_env(resolve(input.directory, 'aks.env'));
  assert.equal(env.ARROWCOL_MODE, 'azure');
  assert.equal(env.AZURE_CLIENT_ID, input.runtime.workloadIdentityClientId);
  assert.equal(env.AZURE_TENANT_ID, tenantId);
  assert.equal(env.AZURE_STAMP, '89152');
  assert.equal(env.AZURE_SUBSCRIPTION_ID, input.runtime.subscriptionId);
  assert.equal(env.AZURE_FEDERATED_TOKEN_FILE, resolve(input.directory, 'secrets/publisher-assertion.jwt'));
  assert.equal((await readFile(env.AZURE_FEDERATED_TOKEN_FILE!, 'utf8')).trim(), input.assertion);
  const config = JSON.parse(await readFile(files.aksConfigPath, 'utf8'));
  assert.equal(config.qap_database, 'arrowcol');
  assert.equal(config.stamp, '89152');
  assert.equal(config.kubectl_context, 'arrow-collector-89152');
  assert.equal(config.dmv_client_image, input.clientImage);
  assert.equal(config.qap_endpoint, input.qapEndpoint);
  assert.equal(config.platform.runtime_config_file, files.runtimeConfigPath);
  assert.equal(config.platform.bootstrap_env_file, files.bootstrapEnvPath);
  assert.equal(config.website_url, 'abfss://$web@arrowcollector89152.dfs.core.windows.net/results.parquet');
  assert.equal(config.result_url, 'abfss://experiments@arrowcollector89152.dfs.core.windows.net/arrowcol-oat/results.parquet');
  for (const path of [input.directory, resolve(input.directory, 'secrets')]) {
    assert.equal((await lstat(path)).mode & 0o777, 0o700);
  }
  for (const name of ['runtime.json', 'bootstrap.env', 'aks.env', 'oat-config.aks.json',
    'secrets/sa-password', 'secrets/app-password', 'secrets/migration-password',
    'secrets/edge-hmac', 'secrets/internal-hmac', 'secrets/generation', 'secrets/publisher-assertion.jwt']) {
    assert.equal((await lstat(resolve(input.directory, name))).mode & 0o777, 0o600, name);
  }
});

test('rerunning refreshes the assertion without rotating existing SQL or HMAC secrets', async t => {
  const input = await fixture(t);
  await generatePrivateInputs(input);
  const password = await readFile(resolve(input.directory, 'secrets/sa-password'), 'utf8');
  const hmac = await readFile(resolve(input.directory, 'secrets/edge-hmac'), 'utf8');
  const refreshed = assertion(Math.floor(Date.now() / 1000) + 3700);
  await generatePrivateInputs({ ...input, assertion: refreshed });
  assert.equal(await readFile(resolve(input.directory, 'secrets/sa-password'), 'utf8'), password);
  assert.equal(await readFile(resolve(input.directory, 'secrets/edge-hmac'), 'utf8'), hmac);
  assert.equal((await readFile(resolve(input.directory, 'secrets/publisher-assertion.jwt'), 'utf8')).trim(), refreshed);
  await assert.rejects(generatePrivateInputs({ ...input, resetGeneration: randomUUID() }), /unsafe existing generation/);
});

test('rejects expired or malformed assertions, mutable images and mismatched stamp endpoints', async t => {
  const input = await fixture(t);
  await assert.rejects(generatePrivateInputs({ ...input, assertion: assertion(1) }), /run `npx --no-install arrowcol-oat aks setup`/);
  await assert.rejects(generatePrivateInputs({ ...input, assertion: 'do-not-echo-this-token' }), /invalid host federation/);
  await assert.rejects(generatePrivateInputs({ ...input, assertion: 'header.bad.signature' }), /invalid host assertion claims/);
  await assert.rejects(generatePrivateInputs({ ...input, clientImage: 'latest' }), /pinned by manifest digest/);
  await assert.rejects(generatePrivateInputs({ ...input, qapEndpoint: 'https://arrow-collector-00000.canadacentral.kusto.windows.net' }), /selected stamp/);
});

test('refuses another stamp, permissive permissions and symlinked private state without following it', async t => {
  const input = await fixture(t);
  await writeFile(resolve(input.directory, 'runtime.json'), JSON.stringify({ stamp: '00000', subscriptionId: input.runtime.subscriptionId }), { mode: 0o600 });
  await assert.rejects(generatePrivateInputs(input), /aks destroy --confirm arrow-collector-rg/);
  await rm(resolve(input.directory, 'runtime.json'));
  await chmod(input.directory, 0o755);
  await assert.rejects(generatePrivateInputs(input), /0700/);
  await chmod(input.directory, 0o700);
  const target = resolve(input.directory, 'target');
  await mkdir(target, { mode: 0o700 });
  await symlink(target, resolve(input.directory, 'secrets'));
  await assert.rejects(generatePrivateInputs(input), /real directory/);
});

test('bootstrap generation is never synthesized and environment entries cannot inject lines', () => {
  assert.throws(() => renderBootstrapEnv({}, ''), /never synthesize/);
  assert.throws(() => renderBootstrapEnv({ BAD: 'one\nSECRET=two' }, generation), /Invalid bootstrap/);
  assert.equal(renderBootstrapEnv({ ARROWCOL_RESET_GENERATION: randomUUID() }, generation),
    `ARROWCOL_RESET_GENERATION=${generation}\n`);
});
