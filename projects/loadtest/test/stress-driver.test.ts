import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { parseStressPlan, runStressDriver, type StressPlan } from '../src/stress-driver.js';
import { parseBenchmarkOptions } from '../src/cli.js';
import { evaluateStress } from '../src/stress-criteria.js';

const plan: StressPlan = {
  version: 1, durationSeconds: 9, waveIntervalSeconds: 3, operationTimeoutSeconds: 1,
  startupTimeoutSeconds: 2, sampleTimeoutSeconds: 1,
  fixture: { batchRows: 8192, rowGroupRows: 512, pageRows: 128 },
  limits: {
    minimumCycles: 3, minimumSamples: 4, minimumDriverRowsPerSecond: 10, maximumBacklog: 20,
    maximumRssBytes: 1000, maximumPhysicalBytes: 1000, maximumRelativeTrend: 0.1, maximumRelativeVariation: 0.2,
  },
};

test('requires a complete predeclared plan and separate correctness/crash gates', () => {
  assert.deepEqual(parseStressPlan(plan), plan);
  for (const patch of [
    { durationSeconds: 10 }, { operationTimeoutSeconds: 3 }, { version: 2 }, { surprise: true },
    { limits: { ...plan.limits, minimumCycles: 5 } }, { limits: { ...plan.limits, maximumBacklog: undefined } },
  ]) assert.throws(() => parseStressPlan({ ...plan, ...patch }));
  assert.throws(() => parseBenchmarkOptions(['--stress-plan', 'fixture.json']), /requires --validate/u);
  assert.throws(() => parseBenchmarkOptions(['--stress-plan', 'fixture.json', '--validate', '--quiver', '--crash-after', '1s']));
  assert.equal(parseBenchmarkOptions(['--validate', '--stress-plan', 'fixture.json']).stressPlan, 'fixture.json');
});

function fixture(t: { after: (callback: () => void) => void }): string {
  const directory = mkdtempSync(path.resolve('projects/loadtest/test/.fixture-stress-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

test('actual clock controller executes exactly the declared waves and persists measured timestamps', async t => {
  const results = fixture(t);
  const operations: number[] = [];
  const frameTimes: number[] = [];
  const verdict = await runStressDriver({
    plan, results, loadEnded: new AbortController().signal, checkRunning: () => {},
    waitForInput: async () => 'offline-controller-fixture',
    operate: async (wave, deadline) => { assert(Date.now() < deadline); operations.push(wave); },
    sample: async wave => {
      const timestamp = Date.now(); frameTimes.push(timestamp);
      return { timestamp, wave, phase: 'wave-boundary', driverRows: wave * 100,
        rssBytes: 100, physicalBytes: 200, backlog: 1, restarts: 0, oomEvents: 0, exhaustedRetries: 0 };
    },
  });
  assert.equal(verdict.status, 'passed');
  assert.deepEqual(operations, [1, 2, 3]);
  const window = JSON.parse(readFileSync(path.join(results, 'stress-window.json'), 'utf8'));
  const samples = readFileSync(path.join(results, 'stress-samples.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.deepEqual(samples.map(sample => sample.timestamp), frameTimes);
  assert(samples.at(-1).timestamp >= window.criteria.end);
  assert.equal(window.criteria.end - window.criteria.start, 9000);
  assert.equal(evaluateStress(samples, window.criteria).status, 'passed');
});

test('actual operation failure is persisted once and never retried until green', async t => {
  const results = fixture(t);
  let attempts = 0;
  await assert.rejects(runStressDriver({
    plan, results, loadEnded: new AbortController().signal, checkRunning: () => {},
    waitForInput: async () => 'offline-failure-fixture',
    operate: async () => { attempts++; throw new Error('fixture operation failed'); },
    sample: async wave => ({ timestamp: Date.now(), wave, phase: 'wave-boundary', driverRows: 100,
      rssBytes: 100, physicalBytes: 100, backlog: 1, restarts: 0, oomEvents: 0, exhaustedRetries: 0 }),
  }), /fixture operation failed/u);
  assert.equal(attempts, 1);
  assert.equal(JSON.parse(readFileSync(path.join(results, 'stress-verdict.json'), 'utf8')).status, 'failed');
});
