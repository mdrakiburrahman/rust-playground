import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdir, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';
import { fixtureTables, main, prepareExperiments } from './prepare-experiments.js';

const tables = fixtureTables(await readFile('projects/loadtest/schemas/sqlserver.sql', 'utf8'));
const base = {
  clients_per_table: 1, dfe_worker_concurrency: [2], rows_per_table: 1000, dmv_concurrency: 16,
  qap_database: 'configured-observation-db', platform: { kind: 'aks', runtime_config_file: 'private/runtime.json' },
  privateSetting: { retained: true }, warmup_seconds: 999, measure_seconds: 999, pilot_mode: true,
};
const binding = { warmupSeconds: 30, measureSeconds: 90, higherDfeConcurrency: 4 };

test('canonical fixture supplies 32 names; E1-E5 change only their declared comparison dimension', () => {
  const configs = prepareExperiments(base, tables, binding);
  assert.deepEqual(Object.keys(configs), ['E1', 'E2', 'E3', 'E4', 'E5']);
  const E3 = configs.E3;
  assert.equal(E3.tables && (E3.tables as string[]).length, 32);
  assert.deepEqual(E3, {
    ...base, tables, warmup_seconds: 30, measure_seconds: 90, pilot_mode: false, replica_counts: [16],
  });
  assert.deepEqual(configs.E1, { ...E3, replica_counts: [4] });
  assert.deepEqual(configs.E2, { ...E3, replica_counts: [8] });
  assert.deepEqual(configs.E4, { ...E3, clients_per_table: 4 });
  assert.deepEqual(configs.E5, { ...E3, dfe_worker_concurrency: [4] });
  (configs.E1.platform as Record<string, unknown>).kind = 'changed';
  assert.deepEqual(E3.platform, base.platform);
  assert.equal(base.warmup_seconds, 999);
});

test('binding refuses unknown/out-of-budget pilot settings, ambiguous baselines and invented tables', () => {
  for (const change of [
    { warmupSeconds: NaN }, { measureSeconds: 0 }, { measureSeconds: 91 },
    { warmupSeconds: 301 }, { warmupSeconds: 300, measureSeconds: 330 },
    { higherDfeConcurrency: 2 }, { higherDfeConcurrency: 257 },
  ]) assert.throws(() => prepareExperiments(base, tables, { ...binding, ...change }));
  for (const change of [
    { clients_per_table: 2 }, { dfe_worker_concurrency: [1, 2] }, { dfe_worker_concurrency: [] },
    { qap_database: null }, { rows_per_table: 0 }, { dmv_concurrency: 65 },
  ]) assert.throws(() => prepareExperiments({ ...base, ...change }, tables, binding));
  assert.throws(() => prepareExperiments(base, tables.slice(1), binding));
  assert.throws(() => prepareExperiments(base, tables.map(() => tables[0]), binding));
  assert.throws(() => fixtureTables('CREATE TABLE telemetry.sqlserver.only_one ('));
  assert.equal(prepareExperiments(base, tables, { ...binding, warmupSeconds: 1 }).E3.warmup_seconds, 1);
});

test('CLI fails before reading private settings when pilot values or private output scope are missing', async () => {
  await assert.rejects(main([]), /--base is required/);
  await assert.rejects(main(['--base', 'not-read', '--out', 'out/arrowcol-oat/new']), /warmup-seconds is required/);
  await assert.rejects(main([
    '--base', 'not-read', '--out', 'docs/published', '--warmup-seconds', '30',
    '--measure-seconds', '90', '--higher-dfe', '4',
  ]), /new subdirectory/);
});

test('offline CLI binds fake private AKS settings, preserves runtime and refuses replacement', async () => {
  const root = resolve('out/arrowcol-oat', `test-e1-e5-${randomUUID()}`);
  await mkdir(root, { recursive: true });
  try {
    const images = Object.fromEntries(['sqlserver', 'catalog', 'router', 'dataflow'].map((role) =>
      [role, `arrowcollector89152.azurecr.io/${role}@sha256:${'a'.repeat(64)}`]));
    const runtime = {
      namespace: 'arrowcol-oat', oatRunId: '00000000-0000-0000-0000-000000000003',
      workloadIdentityClientId: '00000000-0000-0000-0000-000000000001',
      subscriptionId: '00000000-0000-0000-0000-000000000002', stamp: '89152',
      deltaRoot: 'abfss://onelake@arrowcollector89152.dfs.core.windows.net/dataflow', images,
    };
    const platform = {
      kind: 'aks', runtime_config_file: resolve(root, 'runtime.json'),
      bootstrap_env_file: resolve(root, 'bootstrap.env'),
    };
    const config = { ...base, platform, stamp: '89152', dmv_client_image: images.dataflow };
    await writeFile(platform.runtime_config_file, JSON.stringify(runtime));
    await writeFile(platform.bootstrap_env_file, '# fake test input\n');
    await writeFile(resolve(root, 'base.json'), JSON.stringify(config));
    const args = ['--base', resolve(root, 'base.json'), '--out', resolve(root, 'bound'),
      '--warmup-seconds', '30', '--measure-seconds', '90', '--higher-dfe', '4'];
    await main(args);
    for (const [name, expected] of Object.entries(prepareExperiments(config, tables, binding))) {
      const file = resolve(root, 'bound', `${name}.config.json`);
      assert.deepEqual(JSON.parse(await readFile(file, 'utf8')), expected);
      if (process.platform !== 'win32') assert.equal((await stat(file)).mode & 0o777, 0o600);
    }
    await assert.rejects(main(args), /EEXIST/);
    assert.deepEqual(JSON.parse(await readFile(platform.runtime_config_file, 'utf8')), runtime);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
