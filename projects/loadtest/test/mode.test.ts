import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import { parseBenchmarkOptions } from '../src/cli.js';
import { azureTargets, modeEnvironment, stageAzureCliConfig } from '../src/mode.js';
import { buildBenchmarkComposeOverride } from '../src/runtime-config.js';

const config = {
  ARROWCOL_AZURE_TABLE_ROOT: 'abfss://arrowcol@publicfixture.dfs.core.windows.net/sandbox',
  AZURE_STORAGE_ACCOUNT: 'publicfixture',
  AZURE_STORAGE_CONTAINER: 'arrowcol',
  AZURE_STORAGE_CHECKPOINT_CONTAINER: 'checkpoints',
  AZURE_KUSTO_URI: 'https://fixture.eastus.kusto.windows.net',
  AZURE_KUSTO_DATABASE: 'loadtest',
};

test('one mode selects paired storage/query backends and defaults to local', () => {
  assert.equal(parseBenchmarkOptions([], {}).mode, 'local');
  assert.equal(parseBenchmarkOptions([], { ARROWCOL_MODE: 'azure' }).mode, 'azure');
  assert.equal(parseBenchmarkOptions(['--mode=local'], { ARROWCOL_MODE: 'azure' }).mode, 'local');
  assert.throws(() => parseBenchmarkOptions(['--mode', 'azur'], {}), /local or azure/u);
});

test('private config layering is explicit and local mode does not load it', (t) => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'loadtest-mode-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  writeFileSync(path.join(directory, '.env'), 'BASE=original\nOVERRIDE=base\n');
  writeFileSync(path.join(directory, '.env.azure'), 'OVERRIDE=generated\n');
  const azure = modeEnvironment(directory, 'azure', { BASE: 'explicit' });
  assert.equal(azure.BASE, 'explicit');
  assert.equal(azure.OVERRIDE, 'generated');
  assert.equal(modeEnvironment(directory, 'local', {}).BASE, undefined);
});

test('Azure roots are isolated and require complete configuration without local fallback', () => {
  const targets = azureTargets(config, 'run-one');
  assert.equal(targets.directRoot, 'abfss://arrowcol@publicfixture.dfs.core.windows.net/sandbox/loadtest/run-one/direct');
  assert.equal(targets.dataflowRoot, 'abfss://arrowcol@publicfixture.dfs.core.windows.net/sandbox/loadtest/run-one/dataflow');
  assert.equal(targets.kustoEndpoint, config.AZURE_KUSTO_URI);
  for (const key of Object.keys(config)) {
    const incomplete: NodeJS.ProcessEnv = { ...config };
    delete incomplete[key];
    assert.throws(() => azureTargets(incomplete, 'run'), /requires private configuration/u);
  }
  assert.throws(() => azureTargets(config, '../escape'), /isolated/u);
  for (const root of [
    'abfss://foreign@publicfixture.dfs.core.windows.net/sandbox',
    'abfss://arrowcol@foreign.dfs.core.windows.net/sandbox',
    `${config.ARROWCOL_AZURE_TABLE_ROOT}/../escape`,
    `${config.ARROWCOL_AZURE_TABLE_ROOT}/%2e%2e/escape`,
    `${config.ARROWCOL_AZURE_TABLE_ROOT}?token=secret`,
  ]) {
    assert.throws(() => azureTargets({ ...config, ARROWCOL_AZURE_TABLE_ROOT: root }, 'run'), /configured account/u);
  }
  for (const uri of ['http://fixture.eastus.kusto.windows.net', 'https://secret@fixture.eastus.kusto.windows.net',
    'https://fixture.eastus.kusto.windows.net?token=secret', 'https://fixture.eastus.kusto.windows.net/other']) {
    assert.throws(() => azureTargets({ ...config, AZURE_KUSTO_URI: uri }, 'run'), /HTTPS origin/u);
  }
});

test('Azure runtime CLI caches are separate writable private copies, never the host cache mount', t => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'loadtest-azure-runtime-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const source = path.join(directory, 'source');
  const run = path.join(directory, 'run');
  mkdirSync(source);
  mkdirSync(run);
  writeFileSync(path.join(source, 'msal_token_cache.json'), 'fixture-identity');
  stageAzureCliConfig(run, { AZURE_CONFIG_DIR: source });
  const direct = path.join(run, 'azure-cli/direct-1/msal_token_cache.json');
  const dataflow = path.join(run, 'azure-cli/dataflow-1/msal_token_cache.json');
  writeFileSync(direct, 'refreshed-fixture');
  assert.equal(readFileSync(path.join(source, 'msal_token_cache.json'), 'utf8'), 'fixture-identity');
  assert.equal(readFileSync(dataflow, 'utf8'), 'fixture-identity');
  assert.equal(statSync(direct).mode & 0o777, 0o600);
  assert.equal(statSync(path.dirname(direct)).mode & 0o777, 0o700);
  assert.throws(() => stageAzureCliConfig(run, { AZURE_CONFIG_DIR: path.join(directory, 'missing') }), /existing authenticated/u);
  const linked = path.join(directory, 'linked');
  mkdirSync(linked);
  symlinkSync(path.join(source, 'msal_token_cache.json'), path.join(linked, 'token'));
  const another = path.join(directory, 'another');
  mkdirSync(another);
  assert.throws(() => stageAzureCliConfig(another, { AZURE_CONFIG_DIR: linked }), /symbolic links/u);
});

test('cloud Compose config passes only runtime ownership inputs and isolated CLI binds', () => {
  const environment = { ...config, AZURE_SUBSCRIPTION_ID: 'fixture-subscription', UNRELATED_SECRET: 'not-for-runtime' };
  const text = buildBenchmarkComposeOverride('/private/run', {
    mode: 'azure', azureConfigHostDirectory: '/private/run/azure-cli', azureEnvironment: environment,
  });
  const override = JSON.parse(text);
  for (const service of ['direct-1', 'dataflow-1']) {
    const config = override.services[service];
    assert.equal(config.environment.AZURE_CONFIG_DIR, '/run/azure');
    assert.equal(config.environment.AZURE_SUBSCRIPTION_ID, environment.AZURE_SUBSCRIPTION_ID);
    const mount = config.volumes.find((volume: { target: string }) => volume.target === '/run/azure');
    assert.equal(mount.source, `/private/run/azure-cli/${service}`);
    assert.notEqual(mount.read_only, true);
    assert.equal(mount.bind.create_host_path, false);
  }
  assert(!text.includes('not-for-runtime'));
  assert.throws(() => buildBenchmarkComposeOverride('/private/run', { mode: 'azure' }), /isolated writable/u);
});
