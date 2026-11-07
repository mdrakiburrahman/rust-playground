import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { azCommand, capturedManagedGroups } from '../src/qap-sandbox-reset.js';

const subscription = '11111111-2222-3333-4444-555555555555';
const id = `/subscriptions/${subscription}/resourceGroups/arrow-collector-rg/providers/Microsoft.ContainerService/managedClusters/arrow-collector-42`;
const cluster = { id, name: 'arrow-collector-42', nodeResourceGroup: 'MC_arrow-collector-rg_arrow-collector-42_eastus2' };

test('Azure CLI command planning safely invokes Windows az.cmd', () => {
  const args = ['group', 'exists', '--subscription', subscription, '--name', 'arrow-collector-rg'];
  assert.deepEqual(azCommand(args, 'linux'), { executable: 'az', args });
  const windows = azCommand(args, 'win32');
  assert.match(windows.executable, /cmd\.exe$/i);
  assert.deepEqual(windows.args, ['/d', '/s', '/c', `az.cmd ${args.join(' ')}`]);
  assert.throws(() => azCommand(['group', 'exists', 'x&whoami'], 'win32'));
});

test('only accepts managed groups captured from the scoped AKS inventory', () => {
  assert.deepEqual(capturedManagedGroups([cluster, cluster], subscription), [cluster.nodeResourceGroup]);
  assert.throws(() => capturedManagedGroups([{ ...cluster, id: id.replace('arrow-collector-rg', 'rust-playground') }], subscription));
  assert.throws(() => capturedManagedGroups([{ ...cluster, nodeResourceGroup: 'rust-playground' }], subscription));
  assert.throws(() => capturedManagedGroups({}, subscription));
  assert.throws(() => capturedManagedGroups(Array(21).fill(cluster), subscription));
});

test('CLI fails closed, deletes only the sandbox, and waits only for inventoried managed groups', { skip: process.platform === 'win32' }, () => {
  const root = process.cwd();
  const fixture = path.resolve(root, `qap-reset-test-${process.pid}`);
  fs.mkdirSync(fixture, { recursive: false });
  fs.writeFileSync(path.join(fixture, 'package.json'), '{"type":"commonjs"}');
  const checkout = path.join(fixture, 'repo');
  fs.mkdirSync(checkout);
  const executable = path.join(fixture, 'az');
  const log = path.join(fixture, 'commands.jsonl');
  const receipt = path.join(fixture, 'receipt.json');
  fs.writeFileSync(executable, `#!/usr/bin/env node
const fs = require('node:fs');
const args = process.argv.slice(2);
fs.appendFileSync(process.env.FAKE_LOG, JSON.stringify(args) + '\\n');
if (process.env.FAKE_FAIL === args.slice(0, 2).join(' ')) { console.error('private-test-value'); process.exit(19); }
if (args[0] === 'account') console.log(JSON.stringify({id: process.env.FAKE_SUB}));
else if (args[0] === 'aks') console.log(process.env.FAKE_INVENTORY);
else if (args[0] === 'group' && args[1] === 'exists') process.stdout.write(String(args.includes('arrow-collector-rg') && !fs.existsSync(process.env.FAKE_DELETED)));
else if (args[0] === 'group' && args[1] === 'delete') fs.writeFileSync(process.env.FAKE_DELETED, '');
else process.exit(20);
`, { mode: 0o755 });
  const env = { ...process.env, PATH: `${fixture}:${process.env.PATH}`, FAKE_LOG: log, FAKE_SUB: subscription,
    FAKE_INVENTORY: JSON.stringify([cluster]), FAKE_DELETED: path.join(fixture, 'deleted'), FORCE_COLOR: '1' };
  const run = (args: string[], extra: NodeJS.ProcessEnv = {}) => spawnSync(process.execPath,
    ['--import', 'tsx', path.join(root, 'tools/scripts/src/qap-sandbox-reset.ts'), ...args], { cwd: checkout, env: { ...env, ...extra }, encoding: 'utf8', timeout: 15_000 });
  const args = ['--subscription', subscription, '--confirm-reset', 'arrow-collector-rg', '--receipt', receipt];
  try {
    assert.notEqual(run(args.slice(0, 2)).status, 0);
    assert.equal(fs.existsSync(log), false);
    assert.notEqual(run([...args.slice(0, 3), 'rust-playground', ...args.slice(4)]).status, 0);
    assert.equal(fs.existsSync(log), false);
    assert.notEqual(run([...args.slice(0, -1), path.join(checkout, 'receipt.json')]).status, 0);
    assert.equal(fs.existsSync(log), false);
    assert.notEqual(run([...args.slice(0, -1), `${fixture}/../receipt.json`]).status, 0);
    assert.equal(fs.existsSync(log), false);
    const success = run(args);
    assert.equal(success.status, 0, success.stderr);
    const commands = fs.readFileSync(log, 'utf8').trim().split('\n').map(line => JSON.parse(line) as string[]);
    const deletions = commands.filter(command => command[0] === 'group' && command[1] === 'delete');
    assert.equal(deletions.length, 1);
    assert.equal(deletions[0][deletions[0].indexOf('--name') + 1], 'arrow-collector-rg');
    const waits = commands.filter(command => command[0] === 'group' && command[1] === 'exists')
      .map(command => command[command.indexOf('--name') + 1]);
    assert.deepEqual(new Set(waits), new Set(['arrow-collector-rg', cluster.nodeResourceGroup]));
    assert.equal(JSON.parse(fs.readFileSync(receipt, 'utf8')).stage, 'complete');
    fs.unlinkSync(receipt);
    fs.unlinkSync(log);
    const absent = run(args);
    assert.equal(absent.status, 0, absent.stderr);
    assert.deepEqual(JSON.parse(fs.readFileSync(receipt, 'utf8')).managedGroups, []);
    assert.doesNotMatch(fs.readFileSync(log, 'utf8'), /"aks"|"delete"/);
    fs.unlinkSync(receipt);
    fs.unlinkSync(log);
    fs.unlinkSync(env.FAKE_DELETED);
    const failure = run(args, { FAKE_FAIL: 'aks list' });
    assert.notEqual(failure.status, 0);
    assert.doesNotMatch(failure.stderr, /private-test-value/);
    assert.equal(JSON.parse(fs.readFileSync(receipt, 'utf8')).stage, 'inventory');
    assert.doesNotMatch(fs.readFileSync(receipt, 'utf8'), /private-test-value/);
    assert.doesNotMatch(fs.readFileSync(log, 'utf8'), /"delete"/);
    fs.unlinkSync(receipt);
    fs.unlinkSync(log);
    const mismatch = run(args, { FAKE_SUB: '22222222-2222-2222-2222-222222222222' });
    assert.notEqual(mismatch.status, 0);
    assert.doesNotMatch(fs.readFileSync(log, 'utf8'), /"delete"/);
    fs.unlinkSync(receipt);
    fs.unlinkSync(log);
    const wrongInventory = run(args, { FAKE_INVENTORY: JSON.stringify([{ ...cluster, nodeResourceGroup: 'rust-playground' }]) });
    assert.notEqual(wrongInventory.status, 0);
    assert.doesNotMatch(fs.readFileSync(log, 'utf8'), /"delete"/);
  } finally {
    fs.rmSync(fixture, { recursive: true, force: true });
  }
});
