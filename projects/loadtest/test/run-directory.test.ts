import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import { existsSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import path from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import { BENCHMARK_PROJECT } from '../src/cli.js';
import { deltaTableLocations } from '../src/main.js';
import { acquireBenchmarkController, benchmarkDirectories, checkedBenchmarkPath, discardUnsealedAttempts, resetBenchmark, stopBenchmark } from '../src/lifecycle.js';
import { attemptFixture } from './attempt-fixture.js';
import { fixtureDirectory } from './fixture-directory.js';

function fixture(t: { after: (callback: () => void) => void }): string {
  const workspace = fixtureDirectory('reset-');
  t.after(() => rmSync(workspace, { recursive: true, force: true }));
  return workspace;
}

interface Resource {
  id: string;
  name: string;
  labels: Record<string, string>;
  containers?: Record<string, { Name: string }>;
}

function mockDocker(workspace: string): { commands: string[][]; docker: (args: string[]) => string } {
  const commands: string[][] = [];
  const labelled = (project: string): Record<string, string> => ({
    'com.docker.compose.project': project,
    'com.docker.compose.project.config_files': path.join(workspace, 'projects/loadtest/compose.yaml'),
    'com.docker.compose.service': 'direct-1',
  });
  const containers: Resource[] = [
    { id: 'collector', name: 'collector', labels: labelled(BENCHMARK_PROJECT) },
    { id: 'unrelated', name: 'unrelated', labels: labelled('another-project') },
  ];
  const volumes: Resource[] = [
    ...['sqlserver-data', 'hive-warehouse', 'kusto-data', 'kusto-bridge-state'].map(volume => ({
      id: `${BENCHMARK_PROJECT}_${volume}`, name: `${BENCHMARK_PROJECT}_${volume}`,
      labels: { ...labelled(BENCHMARK_PROJECT), 'com.docker.compose.volume': volume,
        'io.arrowcol.loadtest.workspace': workspace },
    })),
    { id: 'other-volume', name: 'other-volume', labels: labelled('another-project') },
    { id: 'another-project_kusto-data', name: 'another-project_kusto-data',
      labels: { ...labelled('another-project'), 'com.docker.compose.volume': 'kusto-data',
        'io.arrowcol.loadtest.workspace': workspace } },
    { id: `${BENCHMARK_PROJECT}_not-allowlisted`, name: `${BENCHMARK_PROJECT}_not-allowlisted`,
      labels: { ...labelled(BENCHMARK_PROJECT), 'com.docker.compose.volume': 'not-allowlisted' } },
    { id: 'wrong-name-kusto', name: 'wrong-name-kusto',
      labels: { ...labelled(BENCHMARK_PROJECT), 'com.docker.compose.volume': 'kusto-data',
        'io.arrowcol.loadtest.workspace': workspace } },
  ];
  const networks: Resource[] = [
    { id: 'network', name: `${BENCHMARK_PROJECT}_default`, labels: labelled(BENCHMARK_PROJECT),
      containers: { collector: { Name: 'collector' } } },
    { id: 'other-network', name: 'another-project_default', labels: labelled('another-project') },
  ];
  const docker = (args: string[]): string => {
    commands.push(args);
    if (args[0] === 'ps') return containers.map(resource => resource.id).join('\n');
    if (args[1] === 'ls') return (args[0] === 'volume' ? volumes : networks).map(resource => resource.id).join('\n');
    if (args.includes('inspect')) {
      const resource = [...containers, ...volumes, ...networks].find(resource => resource.id === args.at(-1));
      assert(resource);
      if (args.some(arg => arg.includes('"mounts"'))) {
        return JSON.stringify({ ...resource, state: { Running: false, ExitCode: 0, OOMKilled: false }, mounts: [] });
      }
      return JSON.stringify(resource);
    }
    if (args[0] === 'stop') return '';
    if (args.includes('rm')) {
      const list = args[0] === 'volume' ? volumes : args[0] === 'network' ? networks : containers;
      for (let index = list.length - 1; index >= 0; index--) if (args.includes(list[index].id)) list.splice(index, 1);
      return '';
    }
    throw new Error(`Unexpected Docker command ${args.join(' ')}`);
  };
  return { commands, docker };
}

test('Delta output has one fixed location', () => {
  const directory = path.resolve('onelake/benchmark/latest');
  assert.equal(deltaTableLocations(directory), [
    'Delta tables on disk (workspace paths)',
    '-------------------------------------',
    `  Direct:   ${path.join(directory, 'direct')}`,
    `  Dataflow: ${path.join(directory, 'dataflow')}`,
  ].join('\n'));
});

test('stop halts only verified workspace services and preserves every file and volume', async t => {
  const workspace = fixture(t);
  const latest = path.join(workspace, 'onelake/benchmark/latest');
  mkdirSync(latest, { recursive: true });
  writeFileSync(path.join(latest, 'keep.txt'), 'durable');
  const { docker, commands } = mockDocker(workspace);
  await stopBenchmark(workspace, docker);
  assert(commands.some(args => args.join(' ') === 'stop --time 30 collector'));
  assert(commands.every(args => !args.includes('rm')));
  assert.equal(readFileSync(path.join(latest, 'keep.txt'), 'utf8'), 'durable');
  const foreign = mockDocker('/foreign');
  await assert.rejects(stopBenchmark(workspace, foreign.docker), /another workspace/u);
  assert(foreign.commands.every(args => args[0] !== 'stop' && !args.includes('rm')));
});

test('cloud release prerequisite drains all mutators and rejects forced, OOM or unknown exits', async t => {
  const workspace = fixture(t);
  for (const service of ['direct-1', 'dataflow-1', 'tenant-stream', 'delta-maintenance']) {
    for (const state of [
      { Running: false, ExitCode: 0, OOMKilled: false },
      { Running: false, ExitCode: 137, OOMKilled: false },
      { Running: false, ExitCode: 0, OOMKilled: true },
      { Running: true, ExitCode: 0, OOMKilled: false },
      {},
    ]) {
      const { docker, commands } = mockDocker(workspace);
      const strict = (args: string[]): string => {
        if (args.includes('{{json .State}}')) return JSON.stringify(state);
        const output = docker(args);
        if (args[0] === 'inspect') {
          const resource = JSON.parse(output);
          resource.labels['com.docker.compose.service'] = service;
          return JSON.stringify(resource);
        }
        return output;
      };
      const pending = stopBenchmark(workspace, strict, { requireCleanMutators: true });
      if ('ExitCode' in state && state.ExitCode === 0 && state.OOMKilled === false && state.Running === false) await pending;
      else await assert.rejects(pending, /retain occupancy and publisher registration/u);
      assert(commands.some(args => args.join(' ') === 'stop --time 300 collector'));
      assert(commands.every(args => !args.includes('rm')));
    }
  }
});

test('final capture stops every mutator but keeps Kusto and live-evidence services available', async t => {
  const workspace = fixture(t);
  const services = ['direct-1', 'dataflow-1', 'tenant-stream', 'delta-maintenance', 'kusto', 'lgtm'];
  const commands: string[][] = [];
  const docker = (args: string[]): string => {
    commands.push(args);
    if (args[0] === 'ps') return services.join('\n');
    if (args[0] === 'stop') return '';
    assert.equal(args[0], 'inspect');
    if (args.includes('{{json .State}}')) return JSON.stringify({ Running: false, ExitCode: 0, OOMKilled: false });
    const service = args.at(-1);
    assert(service && services.includes(service));
    return JSON.stringify({ id: service, name: service, labels: {
      'com.docker.compose.project': BENCHMARK_PROJECT,
      'com.docker.compose.service': service,
      'com.docker.compose.project.config_files': path.join(workspace, 'projects/loadtest/compose.yaml'),
    } });
  };
  assert.deepEqual(await stopBenchmark(workspace, docker, { mutatorsOnly: true, requireCleanMutators: true }), services.slice(0, 4));
  assert.deepEqual(commands.find(args => args[0] === 'stop'), ['stop', '--time', '300', ...services.slice(0, 4)]);
  const partial = mockDocker(workspace);
  await assert.rejects(stopBenchmark(workspace, partial.docker, { mutatorsOnly: true, requireCleanMutators: true }), /both positively identified/u);
  assert(partial.commands.every(args => args[0] !== 'stop'));
});

test('full stop drains mutators before SQL Server and other infrastructure', async t => {
  const workspace = fixture(t);
  const services = ['sqlserver', 'dataflow-1', 'router', 'direct-1'];
  const commands: string[][] = [];
  const docker = (args: string[]): string => {
    commands.push(args);
    if (args[0] === 'ps') return services.join('\n');
    if (args[0] === 'stop') return '';
    if (args.includes('{{json .State}}')) return JSON.stringify({ Running: false, ExitCode: 0, OOMKilled: false });
    const service = args.at(-1);
    return JSON.stringify({ id: service, name: service, labels: {
      'com.docker.compose.project': BENCHMARK_PROJECT,
      'com.docker.compose.service': service,
      'com.docker.compose.project.config_files': path.join(workspace, 'projects/loadtest/compose.yaml'),
    } });
  };
  await stopBenchmark(workspace, docker);
  assert.deepEqual(commands.filter(args => args[0] === 'stop'), [
    ['stop', '--time', '30', 'dataflow-1', 'direct-1'],
    ['stop', '--time', '30', 'sqlserver', 'router'],
  ]);
});

test('starting again removes only already sealed output and owned SQL, Hive and Kusto resources', async t => {
  const workspace = fixture(t);
  const latest = path.join(workspace, 'onelake/benchmark/latest');
  mkdirSync(latest, { recursive: true });
  writeFileSync(path.join(latest, 'old.json'), '{}');
  await attemptFixture(workspace).seal();
  const { docker, commands } = mockDocker(workspace);
  assert.equal(await resetBenchmark(workspace, docker), latest);
  assert(!existsSync(path.join(latest, 'old.json')));
  for (const volume of ['sqlserver-data', 'hive-warehouse', 'kusto-data', 'kusto-bridge-state']) {
    assert(commands.some(args => args.join(' ') === `volume rm ${BENCHMARK_PROJECT}_${volume}`));
  }
  assert(commands.some(args => args.join(' ') === 'rm collector'));
  assert(commands.every(args => !args.includes('unrelated') || args.includes('inspect')));
  assert(commands.every(args => !args.includes('other-volume') || args.includes('inspect')));
  for (const volume of ['another-project_kusto-data', `${BENCHMARK_PROJECT}_not-allowlisted`, 'wrong-name-kusto']) {
    assert(commands.every(args => !args.includes(volume) || args.includes('inspect')));
  }
  writeFileSync(path.join(latest, 'second.json'), '{}');
  await attemptFixture(workspace).seal();
  assert.equal(await resetBenchmark(workspace, docker), latest);
  assert(!existsSync(path.join(latest, 'second.json')));
});

test('discard quarantines only unsealed attempts intact, after which start may reset', async t => {
  const workspace = fixture(t);
  const latest = path.join(workspace, 'onelake/benchmark/latest');
  const unsealed = path.join(workspace, 'onelake/benchmark/azure-run');
  mkdirSync(path.join(unsealed, 'results'), { recursive: true });
  writeFileSync(path.join(unsealed, 'results/provenance.json'), '{"evidence":true}');
  await attemptFixture(workspace).seal();
  const { docker } = mockDocker(workspace);
  await assert.rejects(resetBenchmark(workspace, docker), /incomplete or unsafe/u);
  const moved = discardUnsealedAttempts(workspace, new Date('2026-01-02T03:04:05.678Z'));
  const quarantine = path.join(workspace, '.temp/loadtest-discarded/2026-01-02T03-04-05-678Z');
  assert.deepEqual(moved, [path.join(quarantine, 'azure-run')]);
  assert.equal(readFileSync(path.join(quarantine, 'azure-run/results/provenance.json'), 'utf8'), '{"evidence":true}');
  assert(!existsSync(unsealed));
  assert(existsSync(latest));
  assert.deepEqual(discardUnsealedAttempts(workspace), []);
  assert.equal(await resetBenchmark(workspace, docker), latest);
});

for (const owner of [undefined, '/another-workspace']) {
  test(`Kusto storage with ${owner ? 'foreign' : 'missing'} workspace ownership is rejected even without containers`, async t => {
    const workspace = fixture(t);
    const latest = path.join(workspace, 'onelake/benchmark/latest');
    mkdirSync(latest, { recursive: true });
    writeFileSync(path.join(latest, 'keep.txt'), 'keep');
    await attemptFixture(workspace).seal();
    const { docker, commands } = mockDocker(workspace);
    await assert.rejects(resetBenchmark(workspace, args => {
      if (args[0] === 'ps') return '';
      const text = docker(args);
      if (args[0] === 'volume' && args[1] === 'inspect' && args.at(-1) === `${BENCHMARK_PROJECT}_kusto-data`) {
        const resource = JSON.parse(text);
        if (owner) resource.labels['io.arrowcol.loadtest.workspace'] = owner;
        else delete resource.labels['io.arrowcol.loadtest.workspace'];
        return JSON.stringify(resource);
      }
      return text;
    }), /Kusto storage owned by another or unverifiable workspace/u);
    assert(commands.every(args => args[0] !== 'stop' && !args.includes('rm')));
    assert.equal(readFileSync(path.join(latest, 'keep.txt'), 'utf8'), 'keep');
  });
}

test('legacy benchmark data without an evidence marker blocks reset and unrelated directories survive', async t => {
  const workspace = fixture(t);
  const legacy = path.join(workspace, 'onelake/benchmark/previous');
  mkdirSync(path.join(legacy, 'router-state'), { recursive: true });
  writeFileSync(path.join(legacy, 'router-state/network.json'), '{}');
  const unrelated = path.join(workspace, 'onelake/benchmark/not-a-benchmark');
  mkdirSync(unrelated);
  writeFileSync(path.join(unrelated, 'keep.txt'), 'keep');
  assert.deepEqual(benchmarkDirectories(workspace), [legacy]);
  const { docker, commands } = mockDocker(workspace);
  await assert.rejects(resetBenchmark(workspace, docker), /reset refused/u);
  assert(existsSync(legacy));
  assert.deepEqual(commands, []);
  assert.equal(readFileSync(path.join(unrelated, 'keep.txt'), 'utf8'), 'keep');
});

test('symlinks and workspace escapes are rejected before Docker mutations', async t => {
  const workspace = fixture(t);
  const outside = path.join(workspace, 'outside');
  mkdirSync(outside);
  writeFileSync(path.join(outside, 'keep.txt'), 'keep');
  const root = path.join(workspace, 'onelake/benchmark');
  mkdirSync(root, { recursive: true });
  symlinkSync(outside, path.join(root, 'latest'));
  const { docker, commands } = mockDocker(workspace);
  await assert.rejects(resetBenchmark(workspace, docker), /symlink/u);
  assert.equal(commands.length, 0);
  assert.throws(() => checkedBenchmarkPath(workspace, path.dirname(workspace)), /outside/u);
  assert.equal(readFileSync(path.join(outside, 'keep.txt'), 'utf8'), 'keep');
});

test('another workspace ownership and cleanup failures cannot silently start a new benchmark', async t => {
  const workspace = fixture(t);
  const latest = path.join(workspace, 'onelake/benchmark/latest');
  mkdirSync(latest, { recursive: true });
  writeFileSync(path.join(latest, 'keep.txt'), 'keep');
  await attemptFixture(workspace).seal();
  const { docker, commands } = mockDocker('/another-workspace');
  await assert.rejects(resetBenchmark(workspace, docker), /another workspace/u);
  assert(commands.every(args => !args.includes('rm') && args[0] !== 'stop'));
  const valid = mockDocker(workspace);
  await assert.rejects(resetBenchmark(workspace, args => {
    if (args[0] === 'stop') throw new Error('Docker stop failed');
    return valid.docker(args);
  }), /Docker stop failed/u);
  assert.equal(readFileSync(path.join(latest, 'keep.txt'), 'utf8'), 'keep');
});

test('scratch symlinks are rejected before Docker mutations, even without previous output', async t => {
  const workspace = fixture(t);
  const outside = path.join(workspace, 'outside');
  mkdirSync(outside);
  writeFileSync(path.join(outside, 'keep.txt'), 'keep');
  const temporary = path.join(workspace, 'projects/loadtest/.temp');
  mkdirSync(temporary, { recursive: true });
  symlinkSync(outside, path.join(temporary, 'bin'));
  const { docker, commands } = mockDocker(workspace);
  await assert.rejects(resetBenchmark(workspace, docker), /symlink/u);
  assert.equal(commands.length, 0);
  assert.equal(readFileSync(path.join(outside, 'keep.txt'), 'utf8'), 'keep');
});

test('a stale controller PID is not signalled and concurrent same-process startup is rejected', async t => {
  const workspace = fixture(t);
  const temporary = path.join(workspace, 'projects/loadtest/.temp');
  mkdirSync(temporary, { recursive: true });
  writeFileSync(path.join(temporary, 'controller.json'), JSON.stringify({ pid: process.pid, started: '0' }));
  const controller = await acquireBenchmarkController(workspace);
  try {
    controller.checkRunning();
    await assert.rejects(acquireBenchmarkController(workspace), /unrelated process/u);
    assert.equal(JSON.parse(readFileSync(path.join(temporary, 'controller.json'), 'utf8')).pid, process.pid);
  } finally {
    controller.close();
  }
  assert(!existsSync(path.join(temporary, 'controller.json')));
});

test('a new start waits for the positively identified previous controller to finish', { timeout: 15_000 }, async t => {
  const workspace = fixture(t);
  const source = path.join(workspace, 'projects/loadtest/src/main.ts');
  mkdirSync(path.dirname(source), { recursive: true });
  const lifecycle = fileURLToPath(new URL('../src/lifecycle.ts', import.meta.url));
  writeFileSync(source, [
    `import { acquireBenchmarkController } from ${JSON.stringify(lifecycle)};`,
    `const controller = await acquireBenchmarkController(${JSON.stringify(workspace)});`,
    'const timer = setInterval(() => {',
    '  try { controller.checkRunning(); }',
    '  catch { clearInterval(timer); controller.close(); }',
    '}, 10);',
    'console.log("ready");',
  ].join('\n'));
  const child = spawn(process.execPath, [...process.execArgv, source], { stdio: ['ignore', 'pipe', 'pipe'] });
  const exited = new Promise<number | null>(resolve => child.once('exit', resolve));
  t.after(async () => {
    if (child.exitCode === null) child.kill('SIGTERM');
    await exited;
  });
  await new Promise<void>((resolve, reject) => {
    child.once('error', reject);
    child.stdout.on('data', data => { if (String(data).includes('ready')) resolve(); });
    child.once('exit', code => reject(new Error(`Fixture controller exited ${code} before becoming ready`)));
  });
  const controller = await acquireBenchmarkController(workspace);
  try {
    assert.equal(await exited, 0);
    controller.checkRunning();
    assert.equal(JSON.parse(readFileSync(path.join(workspace, 'projects/loadtest/.temp/controller.json'), 'utf8')).pid, process.pid);
  } finally {
    controller.close();
  }
});

test('foreign network endpoints are rejected before stopping containers', async t => {
  const workspace = fixture(t);
  const { docker, commands } = mockDocker(workspace);
  await assert.rejects(resetBenchmark(workspace, args => {
    const text = docker(args);
    if (args[0] === 'network' && args[1] === 'inspect' && args.at(-1) === 'network') {
      const resource = JSON.parse(text);
      resource.containers.foreign = { Name: 'do-not-stop' };
      return JSON.stringify(resource);
    }
    return text;
  }), /Another container/u);
  assert(commands.every(args => args[0] !== 'stop' && !args.includes('rm')));
});

for (const candidate of [false, true]) test(candidate
  ? 'unverifiable benchmark candidates prevent cleanup'
  : 'unrelated protected processes do not prevent a fresh start', async t => {
  const workspace = fixture(t);
  const source = 'console.log("ready"); setInterval(() => {}, 1000)';
  if (candidate) writeFileSync(path.join(workspace, 'run'), source);
  const child = spawn(process.execPath, candidate ? ['run'] : ['-e', source], {
    cwd: workspace,
    argv0: candidate ? path.join(workspace, 'target/debug/arrowcol-loadgen') : process.execPath,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const exited = new Promise<number | null>(resolve => child.once('exit', resolve));
  t.after(async () => {
    if (child.exitCode === null) child.kill('SIGTERM');
    await exited;
  });
  await new Promise<void>((resolve, reject) => {
    child.once('error', reject);
    child.stdout.once('data', () => resolve());
    child.once('exit', code => reject(new Error(`Fixture process exited ${code} before becoming ready`)));
  });
  const original = fs.readlinkSync;
  const denied = `/proc/${child.pid}/exe`;
  const mocked = t.mock.method(fs, 'readlinkSync', (...args: Parameters<typeof fs.readlinkSync>) => {
    if (args[0] === denied) throw Object.assign(new Error('Protected executable'), { code: 'EACCES' });
    return original(...args);
  });
  syncBuiltinESMExports();
  try {
    const { docker, commands } = mockDocker(workspace);
    if (candidate) {
      await assert.rejects(resetBenchmark(workspace, docker), /Protected executable/u);
      assert(commands.every(args => args[0] !== 'stop' && !args.includes('rm')));
    } else {
      await resetBenchmark(workspace, docker);
    }
    assert.equal(child.exitCode, null);
  } finally {
    mocked.mock.restore();
    syncBuiltinESMExports();
  }
});
