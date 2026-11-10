import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { destroy, main, removeDerivedState } from './aks-destroy.js';

const subscription = '11111111-2222-3333-4444-555555555555';
const group = 'arrow-collector-rg';
const managed = 'MC_arrow-collector-rg_arrow-collector-00000_canadacentral';
const cluster = { name: 'arrow-collector-00000', nodeResourceGroup: managed,
  id: `/subscriptions/${subscription}/resourceGroups/${group}/providers/Microsoft.ContainerService/managedClusters/arrow-collector-00000` };
const derived = ['.env.azure', '.env.azure.tables.json', 'out/arrowcol-oat/runtime.json',
  'out/arrowcol-oat/bootstrap.env', 'out/arrowcol-oat/aks.env', 'out/arrowcol-oat/oat-config.aks.json',
  'out/arrowcol-oat/secrets'];

function fixture(t: test.TestContext, present = true, fault?: 'reset' | 'unknown' | 'receipt' | 'remaining' | 'account') {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'oat-destroy-test-'));
  const receiptPath = path.join(directory, 'receipt.json');
  const calls: string[][] = [];
  const removed: string[] = [];
  const generated: string[] = [];
  let complete = false;
  t.mock.method(fs, 'rmSync', (filename: fs.PathLike) => { removed.push(String(filename)); });
  t.after(() => {
    t.mock.restoreAll();
    fs.rmSync(directory, { recursive: true, force: true });
    for (const filename of generated) fs.rmSync(filename, { force: true });
  });
  const runner: typeof run_tree = async (command, timeout, cwd) => {
    calls.push(command);
    assert.equal(cwd, ROOT);
    const success = (value: unknown) => ({ returncode: 0, stdout: JSON.stringify(value), stderr: '' });
    if (command[0] === 'npx') {
      assert.equal(timeout, Number(command[command.indexOf('--timeout-minutes') + 1]) * 60);
      assert.deepEqual(command.slice(0, 4), ['npx', '--no-install', 'tsx', 'tools/scripts/src/qap-sandbox-reset.ts']);
      assert.equal(command[command.indexOf('--confirm-reset') + 1], group);
      if (fault === 'reset') return { returncode: 1, stdout: '', stderr: 'private-reset-error' };
      if (fault === 'unknown') throw new Error('timed out');
      const filename = command[command.indexOf('--receipt') + 1]!;
      generated.push(filename);
      fs.writeFileSync(filename, JSON.stringify({
        stage: fault === 'receipt' ? 'wait' : 'complete', exitCode: 0, subscription, group, managedGroups: present ? [managed] : [],
      }), { mode: 0o600 });
      complete = true;
      return success(null);
    }
    assert.equal(command[0], 'az');
    assert.equal(timeout, 120);
    assert.equal(command[command.indexOf('--subscription') + 1], subscription);
    if (command[1] === 'account') return success({ id: fault === 'account' ? 'wrong-account' : subscription });
    if (command[1] === 'group') return success(complete ? fault === 'remaining' : present && command.includes(group));
    if (command[1] === 'aks') return success([cluster]);
    if (command[1] === 'resource') return success([{ name: 'arrowcollector00000', type: 'Microsoft.Storage/storageAccounts' }]);
    throw new Error(`Unexpected operation: ${command.join(' ')}`);
  };
  return { directory, receiptPath, calls, removed, runner, options: { subscription, receiptPath } };
}

for (const present of [true, false]) {
  test(`unconfirmed ${present ? 'live' : 'absent'} inventory is strictly read-only`, async t => {
    const f = fixture(t, present);
    const result = await destroy(f.options, f.runner);
    assert.equal(result.existed, present);
    assert.equal(result.deleted, false);
    assert.equal(result.dryRun, true);
    assert.equal(result.receiptPath, undefined);
    assert.equal(f.calls.some(command => command[0] !== 'az'), false);
    assert.equal(fs.existsSync(f.receiptPath), false);
    assert.deepEqual(f.removed, []);
  });

  test(`confirmed ${present ? 'live' : 'already absent'} group cleans only the derived-state allowlist`, async t => {
    const f = fixture(t, present);
    const result = await destroy({ ...f.options, confirm: group }, f.runner);
    assert.equal(result.existed, present);
    assert.equal(result.deleted, present);
    assert.equal(result.dryRun, false);
    assert.deepEqual(result.managedResourceGroups, present ? [managed] : []);
    assert.equal(f.calls.filter(command => command[0] === 'npx').length, 1);
    assert.deepEqual(f.removed, derived.map(relative => path.join(ROOT, relative)));
    assert.equal(result.receiptPath, f.receiptPath);
    assert.equal(fs.statSync(f.receiptPath).mode & 0o777, 0o600);
  });
}

test('default receipt is unique, private and outside the checkout; CLI forwards timeout exactly', async t => {
  const f = fixture(t, false);
  await main(['--subscription', subscription, '--confirm', group, '--timeout-minutes', '2'], f.runner);
  const reset = f.calls.find(command => command[0] === 'npx')!;
  const receipt = reset[reset.indexOf('--receipt') + 1]!;
  assert.equal(path.isAbsolute(receipt), true);
  assert.equal(receipt.startsWith(ROOT + path.sep), false);
  assert.equal(fs.statSync(receipt).mode & 0o777, 0o600);
  assert.equal(reset[reset.indexOf('--timeout-minutes') + 1], '2');
});

for (const fault of ['reset', 'unknown', 'receipt', 'remaining', 'account'] as const) {
  test(`${fault} failure preserves all local state`, async t => {
    const f = fixture(t, true, fault);
    await assert.rejects(destroy({ ...f.options, confirm: group }, f.runner));
    assert.deepEqual(f.removed, []);
    assert.ok(f.calls.filter(command => command[0] === 'npx').length <= 1);
  });
}

test('wrong confirmation, subscription, group and timeout reject before Azure calls', async t => {
  const f = fixture(t);
  for (const options of [{ confirm: 'wrong' }, { subscription: 'invalid' }, { resourceGroup: 'wrong' },
    { timeoutMinutes: 0 }, { timeoutMinutes: 1.5 }, { timeoutMinutes: Infinity }])
    await assert.rejects(destroy({ ...f.options, ...options }, f.runner));
  assert.deepEqual(f.calls, []);
  assert.deepEqual(f.removed, []);
});

test('confirmed receipt rejects relative, traversing, checkout, symlink and existing paths', async t => {
  const f = fixture(t);
  const existing = path.join(f.directory, 'existing.json');
  fs.writeFileSync(existing, 'preserved');
  const linked = path.join(f.directory, 'checkout');
  fs.symlinkSync(ROOT, linked, 'dir');
  for (const receiptPath of ['relative.json', path.join(ROOT, 'receipt.json'),
    `${f.directory}/../receipt.json`, path.join(linked, 'receipt.json'), existing])
    await assert.rejects(destroy({ ...f.options, confirm: group, receiptPath }, f.runner), /receipt/i);
  assert.equal(f.calls.some(command => command[0] === 'npx'), false);
  assert.deepEqual(f.removed, []);
  assert.equal(fs.readFileSync(existing, 'utf8'), 'preserved');
});

test('dry-run does not touch even an existing requested receipt', async t => {
  const f = fixture(t);
  fs.writeFileSync(f.receiptPath, 'preserved');
  await destroy(f.options, f.runner);
  assert.equal(fs.readFileSync(f.receiptPath, 'utf8'), 'preserved');
  assert.deepEqual(f.removed, []);
});

test('real cleanup removes generated files and secrets but preserves static config and siblings', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'oat-cleanup-test-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const kept = ['.env', 'infra/azure/stamp.yaml', 'out/arrowcol-oat/receipts/previous.yaml', 'out/unrelated/value'];
  for (const relative of [...derived.slice(0, -1), 'out/arrowcol-oat/secrets/secret', ...kept]) {
    const filename = path.join(directory, relative);
    fs.mkdirSync(path.dirname(filename), { recursive: true });
    fs.writeFileSync(filename, 'keep private');
  }
  removeDerivedState(directory);
  removeDerivedState(directory);
  for (const relative of derived) assert.equal(fs.existsSync(path.join(directory, relative)), false);
  for (const relative of kept) assert.equal(fs.readFileSync(path.join(directory, relative), 'utf8'), 'keep private');
});

test('cleanup refuses a symlink ancestor before removing any generated state', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'oat-cleanup-link-test-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const root = path.join(directory, 'repo');
  const external = path.join(directory, 'external');
  fs.mkdirSync(root);
  fs.mkdirSync(external);
  fs.writeFileSync(path.join(root, '.env.azure'), 'preserved');
  fs.writeFileSync(path.join(external, 'preserved'), 'outside checkout');
  fs.symlinkSync(external, path.join(root, 'out'), 'dir');
  assert.throws(() => removeDerivedState(root), /symlink/);
  assert.equal(fs.readFileSync(path.join(root, '.env.azure'), 'utf8'), 'preserved');
  assert.equal(fs.readFileSync(path.join(external, 'preserved'), 'utf8'), 'outside checkout');
});
