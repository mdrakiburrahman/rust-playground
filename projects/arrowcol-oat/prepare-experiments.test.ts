import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdir, mkdtemp, readFile, readdir, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import test from 'node:test';
import { parseStampConfig } from '../../infra/azure/stamp.js';
import { assertExperimentPoolCapacity, experimentPoolPodCount, fixtureTables, main, prepareExperiments, prepareFullFleetExperiment } from './prepare-experiments.js';
import { parseFleetSizing, resolveFleetExperiment, type FleetNodeObservation } from './fleet-config.js';

const fleetSizing = () => ({
  collector_pool: 'hugepool',
  collector: { dfe_workers: 'auto', runtime_workers: 'auto', blocking_workers: 'auto', cpu_workers: 'auto' },
  client: { workers: 'auto', runtime_threads: 'auto', connections_per_worker: 'auto', streams_per_worker: 'auto',
    rows_per_table: 'auto', dmv_concurrency: 'auto', corpus_bytes: 'auto', max_memory_bytes: 'auto' },
});
const fleetNode = (node: string, pool: string, cores: number, gib: number): FleetNodeObservation => ({
  node, pool, sku: `Standard_D${cores}s_v6`, architecture: 'amd64',
  allocatableCpuMillicores: cores * 1000 - 100, allocatableMemoryBytes: gib * 1024 ** 3,
  requestedCpuMillicores: 500, requestedMemoryBytes: 512 * 1024 ** 2,
});
test('full fleet auto resolves each heterogeneous node after requests and support reservations', () => {
  const nodes = [fleetNode('collector', 'hugepool', 192, 1800),
    fleetNode('client-small', 'smallpool', 4, 16), fleetNode('client-big', 'bigpool', 64, 256)];
  const pools = nodes.map(node => ({ name: node.pool, sku: node.sku, vcpu: Math.round(node.allocatableCpuMillicores / 1000),
    state: 'ready' as const, reason: null }));
  const source = { platform: { kind: 'aks' }, campaign_profile: 'scale_out_single_table', tables: ['hot'],
    clients_per_table_sweep: [1, 128], tunables: { client: { corpus_rows: 123, interval_us: 0 } } };
  const resolved = resolveFleetExperiment(source, fleetSizing(), nodes, pools);
  const fleet = (resolved.platform as { fleet: { collector: { resources: { requests: { cpu: string; memory: string } } };
    clients: { placement: { node: string }; tunables: { workers: number; corpus_rows?: number; corpus_bytes: number } }[] } }).fleet;
  assert.deepEqual(resolved.dfe_worker_concurrency, [182]);
  assert.equal(fleet.collector.resources.requests.cpu, '182400m');
  assert.equal(Number(fleet.collector.resources.requests.memory), (1800 - 12.5) * 1024 ** 3);
  assert.deepEqual(fleet.clients.map(client => [client.placement.node, client.tunables.workers]),
    [['client-big', 63], ['client-small', 3]]);
  assert.notEqual(fleet.clients[0].tunables.corpus_bytes, fleet.clients[1].tunables.corpus_bytes);
  assert.ok(fleet.clients.every(client => client.tunables.corpus_rows === undefined));
  assert.equal(resolved.clients_per_table, 2);
  assert.equal(resolved.clients_per_table_sweep, undefined);
  assert.deepEqual(prepareFullFleetExperiment({ ...source, platform: { ...source.platform, fleet: fleetSizing() } },
    { nodes, pools }), resolved);
  assert.deepEqual(source.clients_per_table_sweep, [1, 128]);
  const overrides = fleetSizing();
  Object.assign(overrides.collector, { dfe_workers: 20 });
  Object.assign(overrides.client, { workers: 2, connections_per_worker: 128 });
  const overridden = resolveFleetExperiment(source, overrides, nodes, pools);
  assert.deepEqual(overridden.dfe_worker_concurrency, [20]);
  assert.equal(((overridden.platform as Record<string, any>).fleet.clients[0]).tunables.connections_per_worker, 128);
});
test('full fleet rejects malformed sentinels, incompatible architecture, duplicates and over-budget nodes', () => {
  for (const bad of [0, -1, 1.5, 'AUTO', '8', null, 257]) {
    const sizing = fleetSizing();
    Object.assign(sizing.collector, { dfe_workers: bad });
    assert.throws(() => parseFleetSizing(sizing), /auto or a bounded positive integer/);
  }
  const node = fleetNode('collector', 'hugepool', 192, 1800);
  const client = fleetNode('client', 'clientpool', 4, 16);
  const pools = [node, client].map(node => ({ name: node.pool, sku: node.sku, vcpu: 4, state: 'ready' as const, reason: null }));
  const base = { platform: { kind: 'aks' }, campaign_profile: 'scale_out_single_table', tables: ['hot'] };
  assert.throws(() => resolveFleetExperiment(base, fleetSizing(), [node, { ...client, architecture: 'arm64' }], pools), /amd64/);
  assert.throws(() => resolveFleetExperiment(base, fleetSizing(), [node, client, client], pools), /duplicate/);
  assert.throws(() => resolveFleetExperiment(base, fleetSizing(), [node, { ...client, requestedCpuMillicores: 4000 }], pools), /insufficient/);
  const sizing = fleetSizing();
  Object.assign(sizing.client, { corpus_bytes: 100_000_000_000 });
  assert.throws(() => resolveFleetExperiment(base, sizing, [node, client], pools), /exceeds/);
});

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

test('example stamp fits the default run, 32-table pilot, and documented E1-E5 pod counts', async () => {
  const stamp = parseStampConfig(await readFile('infra/azure/stamp.example.yaml', 'utf8'));
  const quick = JSON.parse(await readFile('docs/experiments/arrowcol-oat/oat-config.aks.quick.json', 'utf8')) as Record<string, unknown>;
  const defaultRun = experimentPoolPodCount('default AKS quick run', quick);
  assert.equal(defaultRun.clientPods, 1);
  assert.equal(defaultRun.arrowcolPods, 5);

  const pilot = { ...quick, tables, clients_per_table: 1, replica_counts: [1], pilot_mode: true };
  const pilotCounts = experimentPoolPodCount('32-table pilot', pilot);
  assert.equal(pilotCounts.clientPods, 32);
  assert.equal(pilotCounts.arrowcolPods, 5);

  const markdown = await readFile('docs/experiments/arrowcol-oat/EXPERIMENTS.md', 'utf8');
  const experimentDefinitions = markdown.split('## Capacity prerequisites')[0]!;
  const rows = experimentDefinitions.split(/\r?\n/).filter((line) => /^\|\s*E[1-5]\s*\|/.test(line));
  assert.equal(rows.length, 5);
  let previousClientsPerTable = 1;
  const documented = rows.map((line) => {
    const cells = line.split('|').slice(1, -1).map((cell) => cell.trim());
    const [name, arrowcols, tableDescription, clientDescription] = cells;
    const tableCount = Number(tableDescription?.match(/\d+/)?.[0]);
    const clientsPerTable = clientDescription === 'Same'
      ? previousClientsPerTable
      : Number(clientDescription?.match(/\d+/)?.[0]);
    previousClientsPerTable = clientsPerTable;
    return { name, arrowcols: Number(arrowcols), tableCount, clientsPerTable };
  });

  const configs = prepareExperiments(base, tables, binding);
  const experimentCounts = documented.map((row) => {
    assert.equal(row.tableCount, 32, row.name + ' table count in docs');
    const config = configs[row.name!];
    assert.ok(config, row.name + ' generated by binder');
    assert.equal((config.replica_counts as number[])[0], row.arrowcols, row.name + ' ArrowCols in docs');
    assert.equal(config.clients_per_table, row.clientsPerTable, row.name + ' clients/table in docs');
    const count = experimentPoolPodCount(row.name!, config);
    assert.equal(count.clientPods, row.tableCount * row.clientsPerTable, row.name + ' documented client pod count');
    assert.equal(count.arrowcolPods, 4 + row.arrowcols, row.name + ' documented ArrowCol pod count');
    return count;
  });

  assertExperimentPoolCapacity(stamp.aks.userPools, [defaultRun, pilotCounts, ...experimentCounts]);
  const E4 = experimentCounts.find((experiment) => experiment.name === 'E4');
  assert.ok(E4);
  const undersizedClientPools = stamp.aks.userPools.map((pool) =>
    pool.role === 'client' ? { ...pool, maxPods: 30, maxCount: 1 } : pool);
  assert.throws(() => assertExperimentPoolCapacity(undersizedClientPools, [E4]),
    /E4 requires 128 client pods.*only 0 combined workload slots.*raise aks\.userPools\[0\]\.maxPods to at least 160\./);
});

test('capacity sums same-role pool slots before rejecting an experiment', async () => {
  const stamp = parseStampConfig(await readFile('infra/azure/stamp.example.yaml', 'utf8'));
  const E4 = experimentPoolPodCount('E4', { tables, clients_per_table: 4, replica_counts: [16] });
  const splitPools = stamp.aks.userPools.flatMap((pool) => pool.role === 'client'
    ? [{ ...pool, name: `${pool.name}-a`, maxPods: 100, maxCount: 1 }, { ...pool, name: `${pool.name}-b`, maxPods: 100, maxCount: 1 }]
    : [pool]);
  const firstClientPool = splitPools.find((pool) => pool.role === 'client');
  assert.ok(firstClientPool);
  const firstOnlyClientPools = splitPools.filter((pool) => pool.role !== 'client' || pool.name === firstClientPool.name);
  assert.throws(() => assertExperimentPoolCapacity(firstOnlyClientPools, [E4]), /only 68 combined workload slots/);
  assert.doesNotThrow(() => assertExperimentPoolCapacity(splitPools, [E4]));
  const insufficientPools = stamp.aks.userPools.flatMap((pool) => pool.role === 'client'
    ? [{ ...pool, name: `${pool.name}-a`, maxPods: 60, maxCount: 1 }, { ...pool, name: `${pool.name}-b`, maxPods: 50, maxCount: 1 }]
    : [pool]);
  assert.throws(() => assertExperimentPoolCapacity(insufficientPools, [E4]),
    /E4 requires 128 client pods.*only 46 combined workload slots.*raise aks\.userPools\[0\]\.maxPods to at least 142\./);
});

test('capacity recommendations use maxCount safely beyond the per-node pod limit', async () => {
  const stamp = parseStampConfig(await readFile('infra/azure/stamp.example.yaml', 'utf8'));
  const largeRun = experimentPoolPodCount('512-client-pod run', { tables, clients_per_table: 16, replica_counts: [1] });
  const fullPods = stamp.aks.userPools.map((pool) => pool.role === 'client' ? { ...pool, maxPods: 250, maxCount: 1 } : pool);
  assert.throws(() => assertExperimentPoolCapacity(fullPods, [largeRun]),
    /512-client-pod run requires 512 client pods.*only 218 combined workload slots.*raise aks\.userPools\[0\]\.maxCount to at least 3 with maxPods=250\./);

  const noNodes = stamp.aks.userPools.map((pool) => pool.role === 'client' ? { ...pool, maxPods: 30, maxCount: 0 } : pool);
  assert.throws(() => assertExperimentPoolCapacity(noNodes, [largeRun]),
    /512-client-pod run requires 512 client pods.*only 0 combined workload slots.*raise aks\.userPools\[0\]\.maxCount to at least 3 with maxPods=250\./);
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

test('CLI rejects a symlinked private root before writing outside it', async () => {
  const workspace = await mkdtemp(resolve(tmpdir(), 'oat-linked-root-'));
  const outside = resolve(workspace, 'outside-root');
  await mkdir(resolve(workspace, 'out'));
  await mkdir(outside);
  const sentinel = resolve(outside, 'operator-state');
  await writeFile(sentinel, 'keep private state');
  await symlink(outside, resolve(workspace, 'out/arrowcol-oat'));
  try {
    await assert.rejects(main([
      '--base', 'not-read', '--out', resolve(workspace, 'out/arrowcol-oat/new'),
      '--warmup-seconds', '30', '--measure-seconds', '90', '--higher-dfe', '4',
    ], workspace), /symlink or non-directory component/);
    assert.equal(await readFile(sentinel, 'utf8'), 'keep private state');
    assert.deepEqual(await readdir(outside), ['operator-state']);
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
});

test('CLI rejects a symlinked intermediate output parent without changing its target', async () => {
  const workspace = await mkdtemp(resolve(tmpdir(), 'oat-linked-parent-'));
  const privateRoot = resolve(workspace, 'out/arrowcol-oat');
  const outside = resolve(workspace, 'outside-parent');
  await mkdir(privateRoot, { recursive: true });
  await mkdir(outside);
  const sentinel = resolve(outside, 'operator-state');
  await writeFile(sentinel, 'keep private state');
  await symlink(outside, resolve(privateRoot, 'linked'));
  try {
    await assert.rejects(main([
      '--base', 'not-read', '--out', resolve(privateRoot, 'linked/new'),
      '--warmup-seconds', '30', '--measure-seconds', '90', '--higher-dfe', '4',
    ], workspace), /symlink or non-directory component/);
    assert.equal(await readFile(sentinel, 'utf8'), 'keep private state');
    assert.deepEqual(await readdir(outside), ['operator-state']);
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
});

test('offline CLI selects matching stamp capacity inputs and rejects mismatches or missing files', async () => {
  const workspace = await mkdtemp(resolve(tmpdir(), 'oat-e1-e5-workspace-'));
  const root = resolve(workspace, 'out/arrowcol-oat', `test-e1-e5-${randomUUID()}`);
  await mkdir(root, { recursive: true });
  await mkdir(resolve(workspace, 'projects/loadtest/schemas'), { recursive: true });
  await writeFile(resolve(workspace, 'projects/loadtest/schemas/sqlserver.sql'),
    await readFile('projects/loadtest/schemas/sqlserver.sql', 'utf8'));
  await mkdir(resolve(workspace, 'infra/azure'), { recursive: true });
  const exampleStamp = await readFile('infra/azure/stamp.example.yaml', 'utf8');
  await writeFile(resolve(workspace, 'infra/azure/stamp.yaml'), exampleStamp.replace('stamp: 1', 'stamp: 89152'));
  try {
    const images = Object.fromEntries(['sqlserver', 'catalog', 'router', 'dataflow'].map((role) =>
      [role, `arrowcollector89152.azurecr.io/${role}@sha256:${'a'.repeat(64)}`]));
    const runtime = {
      namespace: 'arrowcol-oat', oatRunId: '00000000-0000-0000-0000-000000000003',
      workloadIdentityClientId: '00000000-0000-0000-0000-000000000001',
      subscriptionId: '00000000-0000-0000-0000-000000000002', stamp: '89152',
      deltaRoot: 'abfss://onelake@arrowcollector89152.dfs.core.windows.net/dataflow',
      effective_stop_budgets: { maintenance_grace_seconds: 4_800, maintenance_stop_timeout_ms: 5_100_000 }, images,
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
    const selectedStampPath = resolve(workspace, 'selected/stamp-89152.yaml');
    await mkdir(resolve(workspace, 'selected'), { recursive: true });
    await writeFile(selectedStampPath, exampleStamp.replace('stamp: 1', 'stamp: 89152'));
    await main([...args, '--out', resolve(root, 'bound-default')], workspace);
    await writeFile(resolve(workspace, 'infra/azure/stamp.yaml'), exampleStamp
      .replace('stamp: 1', 'stamp: 89152')
      .replace(/(- name: benchd64[\s\S]*?maxPods:) 250/, '$1 32'));
    await main([...args, '--stamp-config', selectedStampPath], workspace);
    for (const [name, expected] of Object.entries(prepareExperiments(config, tables, binding))) {
      const file = resolve(root, 'bound', `${name}.config.json`);
      assert.deepEqual(JSON.parse(await readFile(file, 'utf8')), expected);
      if (process.platform !== 'win32') assert.equal((await stat(file)).mode & 0o777, 0o600);
    }
    await assert.rejects(main([...args, '--stamp-config', selectedStampPath], workspace), /EEXIST/);
    const sizing = { ...fleetSizing(), collector_pool: 'benche64' };
    const fullFleet = { ...config, campaign_profile: 'scale_out_single_table', tables: [tables[0]],
      platform: { ...platform, fleet: sizing } };
    const nodes = [fleetNode('collector', 'benche64', 192, 1800), fleetNode('client', 'benchd64', 4, 16)];
    const pools = nodes.map(node => ({ name: node.pool, sku: node.sku, vcpu: Math.round(node.allocatableCpuMillicores / 1000),
      state: 'ready' as const, reason: null }));
    await writeFile(resolve(root, 'full-fleet-input.json'), JSON.stringify(fullFleet));
    await writeFile(resolve(root, 'observation.json'), JSON.stringify({ nodes, pools }));
    await main(['--base', resolve(root, 'full-fleet-input.json'), '--fleet-observation', resolve(root, 'observation.json'),
      '--out', resolve(root, 'full-fleet'), '--stamp-config', selectedStampPath], workspace);
    assert.deepEqual(JSON.parse(await readFile(resolve(root, 'full-fleet/full-fleet.config.json'), 'utf8')),
      prepareFullFleetExperiment(fullFleet, { nodes, pools }));
    await writeFile(resolve(workspace, 'infra/azure/stamp.yaml'), exampleStamp.replace('stamp: 1', 'stamp: 2'));
    await assert.rejects(main([...args, '--out', resolve(root, 'default-mismatch')], workspace),
      /Selected AKS stamp config stamp 2 does not match baseline config stamp 89152/);
    const mismatchedStampPath = resolve(workspace, 'selected/stamp-2.yaml');
    await writeFile(mismatchedStampPath, exampleStamp.replace('stamp: 1', 'stamp: 2'));
    await assert.rejects(main([...args, '--out', resolve(root, 'selected-mismatch'), '--stamp-config', mismatchedStampPath], workspace),
      /Selected AKS stamp config stamp 2 does not match baseline config stamp 89152/);
    await assert.rejects(main([...args, '--out', resolve(root, 'missing-selected'), '--stamp-config', 'selected/missing.yaml'], workspace),
      /selected stamp config is missing at .*selected\/missing\.yaml/);
    await rm(resolve(workspace, 'infra/azure/stamp.yaml'));
    await assert.rejects(main([...args, '--out', resolve(root, 'missing-default')], workspace),
      /selected stamp config is missing at .*infra\/azure\/stamp\.yaml/);
    assert.deepEqual(JSON.parse(await readFile(platform.runtime_config_file, 'utf8')), runtime);
  } finally {
    await rm(workspace, { recursive: true, force: true });
  }
});
