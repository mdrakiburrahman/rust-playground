import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { main } from './cli.js';
import { resolveOatConfigPath } from './launch.js';

test('AKS subcommands lazily dispatch only the selected module with its arguments and runner', async () => {
  for (const subcommand of ['destroy', 'deploy', 'setup']) {
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
    assert.match(result.stderr, /Usage: arrowcol-oat aks <destroy\|deploy\|setup>/);
    assert.equal(result.stdout, '');
  }
});

test('non-AKS arguments and the injected runner pass through to the existing launcher', async () => {
  const commands: string[][] = [];
  await assert.rejects(main(['--mode', 'invalid']), /--mode must be local or aks/);
  await assert.rejects(main(['--mode', 'local'], async (command) => {
    commands.push(command);
    return command[0] === 'git'
      ? { returncode: 0, stdout: command[1] === 'rev-parse' ? 'a'.repeat(40) : '', stderr: '' }
      : { returncode: 1, stdout: '', stderr: 'router test build refused' };
  }), /router test build refused/);
  assert.ok(commands.some((command) => command[0] === 'cargo' && command[1] === 'build'));
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
