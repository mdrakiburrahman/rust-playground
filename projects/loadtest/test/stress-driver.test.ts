import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { parseStressPlan, runStressDriver, stressInputBudget, type StressPlan } from '../src/stress-driver.js';
import { parseBenchmarkOptions } from '../src/cli.js';
import { evaluateStress } from '../src/stress-criteria.js';
import { fixtureDirectory } from './fixture-directory.js';

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

test('a new immutable plan binds full generator/startup budgets without changing window or limits', () => {
  const next = parseStressPlan({ ...plan, startupTimeoutSeconds: 300,
    generator: { warmupSeconds: 120, measureSeconds: 480, captureSettleSeconds: 10 } });
  const budget = stressInputBudget(next, { warmupSeconds: 120, captureSettleSeconds: 10 });
  assert.equal(budget.totalGeneratorSeconds, 600);
  assert.equal(budget.totalDeclaredPhaseSeconds, 610);
  assert.equal(budget.helperProcessTimeoutSeconds, 1200);
  assert.equal(budget.persistedPlanSha256,
    createHash('sha256').update(`${JSON.stringify(next, null, 2)}\n`).digest('hex'));
  assert.notEqual(budget.persistedPlanSha256,
    stressInputBudget(plan, { warmupSeconds: 0, captureSettleSeconds: 0 }).persistedPlanSha256);
  assert.equal(budget.startupTimeoutSeconds, 300);
  assert.equal(budget.measureSeconds, 480);
  assert.deepEqual(next.limits, plan.limits);
  assert.equal(next.durationSeconds, plan.durationSeconds);
  assert.equal(next.waveIntervalSeconds, plan.waveIntervalSeconds);
  for (const patch of [{ warmupSeconds: 5 }, { captureSettleSeconds: 0 }, { measureSeconds: 481 }]) {
    assert.throws(() => stressInputBudget(next, { warmupSeconds: 120, captureSettleSeconds: 10, ...patch }),
      /immutable stress-plan declaration/u);
  }
  for (const generator of [
    { warmupSeconds: 120 }, { ...next.generator, unknown: 1 }, { ...next.generator, measureSeconds: 1 },
    { ...next.generator, warmupSeconds: 300 }, { ...next.generator, captureSettleSeconds: -1 },
  ]) assert.throws(() => parseStressPlan({ ...next, generator }));
  assert.equal(stressInputBudget(plan, { warmupSeconds: 0, captureSettleSeconds: 0 }).measureSeconds, 16);
});

test('real runner rejects a changed declared generator before any Docker/reset command', t => {
  const directory = fixture(t);
  const declared = { ...plan, startupTimeoutSeconds: 300,
    generator: { warmupSeconds: 120, measureSeconds: 480, captureSettleSeconds: 30 } };
  const file = path.join(directory, 'new-plan.json');
  writeFileSync(file, JSON.stringify(declared));
  const marker = path.join(directory, 'docker-called');
  writeFileSync(path.join(directory, 'docker'), `#!/bin/sh\nprintf called > '${marker}'\nexit 99\n`, { mode: 0o755 });
  const result = spawnSync(process.execPath, ['--import', 'tsx', 'projects/loadtest/src/main.ts',
    '--validate', '--warmup', '5s', '--batch-rows', String(plan.fixture.batchRows), '--stress-plan', file],
  { env: { ...process.env, PATH: `${directory}:${process.env.PATH}` }, encoding: 'utf8', timeout: 15_000 });
  assert.ifError(result.error);
  assert.equal(result.status, 1);
  assert.match(result.stderr, /immutable stress-plan declaration/u);
  assert(!existsSync(marker), 'mismatch must fail before any reset or Docker work');
});

function fixture(t: { after: (callback: () => void) => void }): string {
  const directory = fixtureDirectory('stress-');
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

test('actual clock controller executes exactly the declared waves and persists measured timestamps', async t => {
  const results = fixture(t);
  const operations: number[] = [];
  const frameTimes: number[] = [];
  const verdict = await runStressDriver({
    plan, results, loadEnded: new AbortController().signal, checkRunning: () => {},
    waitForInput: async () => ({ runId: 'offline-controller-fixture', inputDeadline: Date.now() + 60_000 }),
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
    waitForInput: async () => ({ runId: 'offline-failure-fixture', inputDeadline: Date.now() + 60_000 }),
    operate: async () => { attempts++; throw new Error('fixture operation failed'); },
    sample: async wave => ({ timestamp: Date.now(), wave, phase: 'wave-boundary', driverRows: 100,
      rssBytes: 100, physicalBytes: 100, backlog: 1, restarts: 0, oomEvents: 0, exhaustedRetries: 0 }),
  }), /fixture operation failed/u);
  assert.equal(attempts, 1);
  assert.equal(JSON.parse(readFileSync(path.join(results, 'stress-verdict.json'), 'utf8')).status, 'failed');
});

test('input coverage is rechecked at the actual start before creating any immutable window', async t => {
  const results = fixture(t);
  await assert.rejects(runStressDriver({
    plan, results, loadEnded: new AbortController().signal, checkRunning: () => {},
    waitForInput: async () => ({ runId: 'expired-input-fixture',
      inputDeadline: Date.now() + plan.durationSeconds * 1000 }),
    operate: async () => assert.fail('an insufficient input budget must not launch maintenance'),
    sample: async () => assert.fail('an insufficient input budget must not sample a pretend window'),
  }), /cannot cover the immutable window at its actual start/u);
  assert(!existsSync(path.join(results, 'stress-window.json')));
  const verdict = JSON.parse(readFileSync(path.join(results, 'stress-verdict.json'), 'utf8'));
  assert.equal(verdict.status, 'failed');
  assert.equal(verdict.cycles, 0);
});
