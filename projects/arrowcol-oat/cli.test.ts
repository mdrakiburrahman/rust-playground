import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { copyFile, mkdtemp, mkdir, readFile, readdir, rm, symlink, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { main } from './cli.js';
import { resolveOatConfigPath } from './launch.js';
import { main as fleetMain } from './aks-fleet-cli.js';
import { Config, ROOT } from '../../infra/azure/bootstrap.js';
import { defaultStampConfig } from '../../infra/azure/stamp.js';
import { stringify } from 'yaml';
import type { FleetBinding, FleetIntentAuthority } from './aks-fleet-custody.js';
import type { createFleetAdapter } from './aks-fleet-provider.js';

test('AKS subcommands lazily dispatch only the selected module with its arguments and runner', async () => {
  for (const subcommand of ['destroy', 'deploy', 'setup', 'scale', 'fleet']) {
    const calls: string[] = [];
    const runner = async () => { throw new Error('routing must not invoke a command'); };
    await main(['aks', subcommand, '--stamp', '12345'], runner, async (selected) => {
      calls.push(selected);
      return { main: async (args, injectedRunner) => {
        assert.deepEqual(args, ['--stamp', '12345']);
        assert.equal(injectedRunner, runner);
      } };
    });
    assert.deepEqual(calls, [subcommand]);
  }
});

test('missing and unknown AKS subcommands exit with usage instead of launching a benchmark', () => {
  for (const args of [['aks'], ['aks', 'unknown']]) {
    const result = spawnSync(process.execPath, ['projects/arrowcol-oat/cli.mjs', ...args], { encoding: 'utf8' });
    assert.equal(result.status, 2);
    assert.match(result.stderr, /Usage: arrowcol-oat aks <destroy\|deploy\|setup\|scale\|fleet>/);
    assert.equal(result.stdout, '');
  }
});

test('non-AKS arguments and the injected runner reach only the selected launcher', async () => {
  await assert.rejects(main(['--mode', 'invalid']), /--mode must be local or aks/);
  const args = ['--mode', 'local'];
  const runner = async () => { throw new Error('router must not run commands'); };
  const refusal = new Error('injected launcher refused');
  await assert.rejects(main(args, runner, undefined, async (actual, injected) => {
    assert.deepEqual(actual, args);
    assert.equal(injected, runner);
    throw refusal;
  }), (error: unknown) => error === refusal);
});

test('router refusal fixture cannot write outside its temporary ledger', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'oat-router-write-scope-'));
  try {
    const modules = 'projects/arrowcol-oat';
    await mkdir(path.join(root, modules), { recursive: true });
    for (const name of ['cli.ts', 'launch.ts']) await copyFile(path.join(ROOT, modules, name), path.join(root, modules, name));
    for (const name of (await readdir(path.join(ROOT, modules))).filter(name => name.endsWith('.ts') &&
      !name.endsWith('.test.ts') && name !== 'cli.ts' && name !== 'launch.ts')) {
      await symlink(path.join(ROOT, modules, name), path.join(root, modules, name));
    }
    for (const name of ['infra', 'node_modules', 'package.json']) await symlink(path.join(ROOT, name), path.join(root, name));
    const config = path.join(ROOT, 'docs/experiments/arrowcol-oat/oat-config.quick.json');
    const defaultLedger = path.join(root, 'out/arrowcol-oat/deadline-receipts');
    await mkdir(defaultLedger, { recursive: true });
    await writeFile(path.join(defaultLedger, 'untouched'), 'default-ledger sentinel');
    const script = `
      import assert from 'node:assert/strict';
      import { main } from './projects/arrowcol-oat/cli.ts';
      import { main as launch } from './projects/arrowcol-oat/launch.ts';
      const runner = async command => command[0] === 'git'
        ? {returncode:0, stdout:command[1] === 'rev-parse' ? 'a'.repeat(40) : '', stderr:''}
        : {returncode:1, stdout:'', stderr:'router test build refused'};
      await assert.rejects(main(['--mode','local','--config',${JSON.stringify(config)}], runner, undefined,
        (args, run) => launch(args, run, undefined, ${JSON.stringify(path.join(root, 'receipts'))})), /router test build refused/);
    `;
    const result = spawnSync(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', script], {
      cwd: root, encoding: 'utf8', timeout: 30_000, env: { ...process.env, TMPDIR: root, TSX_DISABLE_CACHE: '1' },
    });
    assert.equal(result.status, 0, result.error?.message ?? result.stderr);
    assert.deepEqual(await readdir(defaultLedger), ['untouched']);
    assert.equal(await readFile(path.join(defaultLedger, 'untouched'), 'utf8'), 'default-ledger sentinel');
    const receipts = await readdir(path.join(root, 'receipts'));
    assert.equal(receipts.length, 1);
    const receipt = JSON.parse(await readFile(path.join(root, 'receipts', receipts[0]!), 'utf8'));
    assert.equal(receipt.error, 'OAT cargo failed: router test build refused');
    assert.equal(receipt.creation_state, 'not_started');
    assert.equal(receipt.cleanup_state, 'confirmed');
    assert.equal(receipt.run_id, null);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('AKS defaults prefer generated private configuration but never override explicit or local configs', async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'oat-cli-config-'));
  try {
    const generated = path.join(root, 'out/arrowcol-oat/oat-config.aks.json');
    const template = path.join(root, 'docs/experiments/arrowcol-oat/oat-config.aks.quick.json');
    const local = path.join(root, 'docs/experiments/arrowcol-oat/oat-config.quick.json');
    assert.equal(resolveOatConfigPath('aks', undefined, root), template);
    assert.equal(resolveOatConfigPath('local', undefined, root), local);
    await mkdir(path.dirname(generated), { recursive: true });
    await writeFile(generated, '{}');
    assert.equal(resolveOatConfigPath('aks', undefined, root), generated);
    assert.equal(resolveOatConfigPath('local', undefined, root), local);
    assert.equal(resolveOatConfigPath('aks', 'explicit.json', root), 'explicit.json');
    assert.equal(resolveOatConfigPath('local', 'positional.json', root), 'positional.json');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('aks scale is reachable through the shared OAT CLI', async () => {
  let selected = '';
  let passed: string[] = [];
  await main(['aks', 'scale', 'owner', '2'], undefined, async subcommand => {
    selected = subcommand;
    return { main: async args => { passed = args; } };
  });
  assert.equal(selected, 'scale');
  assert.deepEqual(passed, ['owner', '2']);
});

test('fleet CLI resolves cells, tears down launch failures and recovers owned or unstarted kills', async () => {
    for (const scenario of ['success', 'source-only-maintenance', 'launch-failure', 'kill', 'kill-unstarted']) {
      const recovering = scenario.startsWith('kill');
      const root = await mkdtemp(path.join(os.tmpdir(), 'oat-fleet-cli-'));
      const out = path.resolve(ROOT, 'out/arrowcol-oat', `test-fleet-${randomUUID()}`);
      const subscription = '10000000-0000-4000-8000-000000000001';
      const stamp = defaultStampConfig({ AZURE_STAMP: '00000', AZURE_SUBSCRIPTION_ID: subscription });
      const config = new Config({ AZURE_SUBSCRIPTION_ID: subscription }, stamp);
      const collector = stamp.aks.userPools.find(pool => pool.role === 'arrowcol')!;
      const binding: FleetBinding = {
        clusterId: config.base + '/providers/Microsoft.ContainerService/managedClusters/' + config.cluster,
        server: 'https://fleet.example.test',
        pools: stamp.aks.userPools.map(pool => ({
          name: pool.name, sku: pool.vmSize, role: pool.role, vcpu: 64, count: 1, maxCount: pool.maxCount,
        })),
      };
      const counts = new Map(binding.pools.map(pool => [pool.name, scenario === 'kill' ? 1 : 0]));
      const events: string[] = [];
      let baselineEstablished = scenario === 'kill';
      const observation = (name: string) => {
        const pool = binding.pools.find(pool => pool.name === name)!;
        return { name, sku: pool.sku, vcpu: pool.vcpu, count: counts.get(name)!, provisioningState: 'Succeeded' };
      };
      const authority: FleetIntentAuthority = {
        binding, campaignId: randomUUID(), ownerId: randomUUID(), fencingToken: 1,
        get baselineEstablished() { return baselineEstablished; },
        assertCurrent: async () => {},
        establishBaseline: async () => { baselineEstablished = true; events.push('baseline'); },
        recordObservation: async () => {},
        beforeDispatch: async () => randomUUID(), afterDispatch: async () => {},
        complete: async () => { assert.ok([...counts.values()].every(count => count === 0)); events.push('complete'); },
        close: async () => { events.push('close'); },
      };
      const adapter: ReturnType<typeof createFleetAdapter> = {
        establishBaseline: () => authority.establishBaseline([]),
        listUserPools: async () => binding.pools.map(pool => pool.name),
        readPool: async name => observation(name),
        scalePool: async (name, count) => { counts.set(name, count); events.push(`scale:${count}`); return { state: 'accepted' }; },
        waitForPool: async name => observation(name),
        listNodes: async () => ({ items: binding.pools.filter(pool => counts.get(pool.name)).map(pool => ({
          metadata: { name: `${pool.name}-node`, labels: {
            'kubernetes.azure.com/agentpool': pool.name, 'arrow-collector/role': pool.role,
            'kubernetes.io/hostname': `${pool.name}-node`, 'kubernetes.io/arch': 'amd64',
            'node.kubernetes.io/instance-type': pool.sku,
          } },
          status: { nodeInfo: { architecture: 'amd64' }, allocatable: { cpu: '64', memory: '128Gi' },
            conditions: [{ type: 'Ready', status: 'True' }] },
        })) }),
        listPods: async () => ({ items: [] }),
      };
      try {
        const image = (name: string) => `arrowcollector00000.azurecr.io/${name}@sha256:${'a'.repeat(64)}`;
        const runtimePath = path.join(root, 'runtime.json');
        const bootstrapPath = path.join(root, 'bootstrap.env');
        await writeFile(runtimePath, JSON.stringify({
          namespace: 'arrowcol-oat', oatRunId: randomUUID(), workloadIdentityClientId: randomUUID(),
          subscriptionId: subscription, stamp: '00000', deltaRoot: 'abfss://arrowcol@arrowcollector00000.dfs.core.windows.net/dataflow',
          effective_stop_budgets: { maintenance_grace_seconds: 4_800, maintenance_stop_timeout_ms: 5_100_000 },
          images: Object.fromEntries(['sqlserver', 'catalog', 'router', 'dataflow',
            ...(scenario === 'source-only-maintenance' ? [] : ['maintenance'])].map(role => [role, image(role)])),
        }));
        await writeFile(bootstrapPath, '# fake test input\n');
        const template = JSON.parse(await readFile(path.resolve(ROOT, 'docs/experiments/arrowcol-oat/oat-config.aks.full-fleet.json'), 'utf8'));
        const source = { ...template, stamp: '00000', dmv_client_image: image('client'), maintenance_image: image('maintenance'),
          platform: { ...template.platform, runtime_config_file: runtimePath, bootstrap_env_file: bootstrapPath,
            fleet: { ...template.platform.fleet, collector_pool: collector.name } } };
        const sourcePath = path.join(root, 'source.json'), stampPath = path.join(root, 'stamp.yaml');
        await writeFile(sourcePath, JSON.stringify(source));
        await writeFile(stampPath, stringify(stamp));
        const invoke = () => fleetMain([recovering ? 'kill' : 'run', '--env-file', 'unused-test.env',
          '--stamp-config', stampPath, '--owner-id', authority.ownerId, '--collector-pool', collector.name,
          '--config', sourcePath, '--out', out], async () => { throw new Error('No external commands in CLI fixture'); }, {
          custody: () => ({ config, env: {}, positionals: [], once: false }),
          binding: async () => binding,
          acquire: async (_azure, _binding, options) => { assert.equal(options.resume, recovering); return authority; },
          adapter: () => adapter,
          images: async (_azure, images) => {
            assert.equal(new Set(images).size, images.length);
            assert.equal(images.filter(value => value === image('maintenance')).length, 1);
            return images.map(image => ({ image, architectures: ['amd64'] }));
          },
          launch: async (args, _runner, _cleanup, _receipts, _collect, _owned, signal) => {
            assert.ok(signal);
            const numeric = JSON.parse(await readFile(args[args.indexOf('--config') + 1]!, 'utf8'));
            assert.equal(numeric.platform.fleet.collector.pool, collector.name);
            assert.equal(numeric.platform.fleet.clients.length, 1);
            events.push('launch');
            if (scenario === 'launch-failure') throw new Error('fixture launch failure');
          },
        });
        if (scenario === 'launch-failure') await assert.rejects(invoke(), /fixture launch failure/);
        else await invoke();
        assert.ok([...counts.values()].every(count => count === 0));
        assert.equal(events.at(-1), 'close');
        assert.equal(events.includes('baseline'), scenario !== 'kill');
        assert.equal(events.includes('launch'), !recovering);
        if (scenario !== 'launch-failure') assert.ok(events.includes('complete'));
      } finally {
        await rm(out, { recursive: true, force: true });
        await rm(root, { recursive: true, force: true });
      }
    }
});
