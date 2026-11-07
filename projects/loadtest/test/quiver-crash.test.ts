import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import { QUIVER_RECOVERY_FILE, quiverRecoveryFailures, quiverRecoveryMetrics, runQuiverCrash, selectQuiverMetrics } from '../src/quiver-crash.js';

function exposition(values: Record<string, number>): string {
  return Object.entries(values).map(([name, value]) => `# TYPE ${name} counter\n${name} ${value}`).join('\n');
}
interface FakeDataflow { phase: 'running' | 'killed' | 'restarted'; events: string[] }
function fakeScrape(state: FakeDataflow, replay: boolean): (endpoint: string) => Promise<string> {
  return async () => {
    if (state.phase === 'killed') throw new Error('connection refused');
    const restarted = state.phase === 'restarted';
    return exposition({
      [quiverRecoveryMetrics.dataflow.persisted]: restarted ? 1 : 30,
      [quiverRecoveryMetrics.dataflow.backlog]: restarted ? 0 : 4,
      [quiverRecoveryMetrics.dataflow.recovered]: restarted && replay ? 4 : 0,
    });
  };
}
function crashOptions(results: string, state: FakeDataflow, replay: boolean, loadEnded = new AbortController().signal) {
  return {
    crashAfterSeconds: 0.01, dataflowAdmin: 'http://dataflow', results,
    kill: async () => { state.phase = 'killed'; state.events.push('kill'); },
    restart: async () => { state.phase = 'restarted'; state.events.push('restart'); },
    log: (message: string) => { state.events.push(`log:${message}`); },
    loadEnded, checkRunning: () => undefined, scrape: fakeScrape(state, replay),
    pollIntervalMs: 1, startTimeoutMs: 1_000, recoveryTimeoutMs: 50,
  };
}
test('crash demo restarts Dataflow and records Quiver replay', async () => {
  const results = mkdtempSync(path.join(tmpdir(), 'quiver-crash-'));
  try {
    const state: FakeDataflow = { phase: 'running', events: [] };
    const evidence = await runQuiverCrash(crashOptions(results, state, true));
    assert.deepEqual(state.events.filter(event => !event.startsWith('log:')), ['kill', 'restart']);
    assert.equal(evidence.passed, true);
    assert.deepEqual(evidence.recovered, { dataflow: true });
    assert.equal(evidence.beforeCrash.dataflow.backlog, 4);
    assert.equal(evidence.afterRecovery.dataflow.recovered, 4);
    assert.equal(JSON.parse(readFileSync(path.join(results, QUIVER_RECOVERY_FILE), 'utf8')).passed, true);
  } finally { rmSync(results, { recursive: true, force: true }); }
});
test('crash demo fails when restarted Dataflow replays nothing', async () => {
  const results = mkdtempSync(path.join(tmpdir(), 'quiver-crash-'));
  try {
    const state: FakeDataflow = { phase: 'running', events: [] };
    await assert.rejects(runQuiverCrash(crashOptions(results, state, false)), /Dataflow did not replay any Quiver batch/u);
    const written = JSON.parse(readFileSync(path.join(results, QUIVER_RECOVERY_FILE), 'utf8'));
    assert.equal(written.passed, false); assert.equal(written.recoveredAt, undefined);
  } finally { rmSync(results, { recursive: true, force: true }); }
});
test('crash demo refuses to crash after load generation has ended', async () => {
  const results = mkdtempSync(path.join(tmpdir(), 'quiver-crash-'));
  try {
    const state: FakeDataflow = { phase: 'running', events: [] };
    const ended = new AbortController(); ended.abort();
    await assert.rejects(runQuiverCrash(crashOptions(results, state, true, ended.signal)), /ended before the Quiver crash/u);
    assert.deepEqual(state.events, []);
  } finally { rmSync(results, { recursive: true, force: true }); }
});
test('recovery verdict requires replay from Dataflow', () => {
  const recovered = { dataflow: selectQuiverMetrics('dataflow', {}) };
  assert.deepEqual(quiverRecoveryFailures(recovered), ['Dataflow did not replay any Quiver batch after the crash']);
});
