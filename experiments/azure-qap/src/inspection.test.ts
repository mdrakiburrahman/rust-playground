import assert from 'node:assert/strict';
import test from 'node:test';
import { observeInspection } from './inspection.js';
import { parseOptions, loadProfile } from './profile.js';

const target = { table: 'qap_owned_reload', version: 2, rows: 4,
  expectedGroups: [{ partition: 'control', id: 1, value: 2, payloadSha256: 'a'.repeat(64), multiplicity: 4 }] };
const sample = { at: '2026-01-01', version: 2, acceleratedRows: 4, completionPercent: 100, pendingFiles: 0, healthy: true };

test('inspection performs periodic real health and parity reads, publishes only after the first verified round, and is bounded', async () => {
  let time = 0;
  const calls: string[] = [];
  const events: string[] = [];
  await observeInspection({ targets: [target], deadline: 65_000, signal: new AbortController().signal,
    clock: { now: () => time, sleep: async ms => { calls.push(`sleep:${ms}`); time += ms; } },
    kusto: { sample: async table => { calls.push(`health:${table}`); return sample; },
      parity: async (table, groups) => { assert.deepEqual(groups, target.expectedGroups); calls.push(`parity:${table}`); } },
    onReady: async () => { calls.push('ready'); }, say: text => events.push(text), record: () => {} });
  assert.deepEqual(calls, ['health:qap_owned_reload', 'parity:qap_owned_reload', 'ready', 'sleep:30000',
    'health:qap_owned_reload', 'parity:qap_owned_reload', 'sleep:30000',
    'health:qap_owned_reload', 'parity:qap_owned_reload', 'sleep:5000']);
  assert.equal(time, 65_000);
  assert.ok(events.every(text => text.includes('READ-ONLY INSPECTION')));
});

test('unhealthy QAP or parity failure cannot publish an inspection handoff', async () => {
  for (const unhealthy of [false, true]) {
    let ready = false;
    const events: string[] = [];
    await assert.rejects(observeInspection({ targets: [target], deadline: 60_000, signal: new AbortController().signal,
      clock: { now: () => 0, sleep: async () => { throw new Error('must not sleep'); } },
      kusto: { sample: async () => ({ ...sample, healthy: !unhealthy }),
        parity: async () => { throw new Error('row mismatch'); } },
      onReady: async () => { ready = true; }, say: text => events.push(text), record: () => {} }),
    unhealthy ? /UNHEALTHY/ : /row mismatch/);
    assert.equal(ready, false);
    assert.match(events.at(-1)!, /FAILED.*cleanup/);
  }
});

test('TERM aborts inspection sleep promptly, allowing the caller to run scoped cleanup', async () => {
  const stop = new AbortController();
  let reads = 0;
  await observeInspection({ targets: [target], deadline: 60_000, signal: stop.signal,
    clock: { now: () => 0, sleep: async () => { stop.abort(new Error('SIGTERM')); throw stop.signal.reason; } },
    kusto: { sample: async () => { reads++; return sample; }, parity: async () => {} },
    onReady: async () => {}, say: () => {}, record: () => {} });
  assert.equal(reads, 1);
});

test('retention is opt-in and inspection durations are strictly bounded', () => {
  const defaults = loadProfile('experiments/azure-qap/scenario.json');
  assert.equal(parseOptions([], defaults).keepRunning, false);
  assert.equal(parseOptions(['--keep-running'], defaults).inspectionMs, 3600_000);
  assert.equal(parseOptions(['--keep-running', '--inspection-duration', '10m'], defaults).inspectionMs, 600_000);
  for (const duration of ['0', '59s', '61m'])
    assert.throws(() => parseOptions(['--keep-running', '--inspection-duration', duration], defaults), /between 1m and 1h/);
  assert.throws(() => parseOptions(['--inspection-duration', '10m'], defaults), /requires --keep-running/);
  assert.throws(() => parseOptions(['--keep-running', '--operations', 'optimize'], defaults), /requires reload/);
});
