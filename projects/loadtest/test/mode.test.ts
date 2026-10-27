import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import { parseBenchmarkOptions } from '../src/cli.js';
import { azureTargets, modeEnvironment } from '../src/mode.js';

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
