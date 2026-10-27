import assert from 'node:assert/strict';
import { test } from 'node:test';
import { evaluateStress, type StressCriteria, type WaveSample } from '../src/stress-criteria.js';

const criteria: StressCriteria = {
  start: 1000, end: 4000, phase: 'after-vacuum', minimumCycles: 3, minimumSamples: 4,
  minimumDriverRowsPerSecond: 50, maximumBacklog: 20, maximumRssBytes: 1000,
  maximumPhysicalBytes: 1000, maximumRelativeTrend: 0.1, maximumRelativeVariation: 0.2,
};
const samples = (): WaveSample[] => [0, 1, 2, 3].map(wave => ({
  timestamp: (wave + 1) * 1000, wave, phase: criteria.phase, driverRows: wave * 100,
  rssBytes: 100, physicalBytes: 100, backlog: 2, restarts: 0, oomEvents: 0, exhaustedRetries: 0,
}));

test('requires both completed operation cycles and independent export samples in the fixed phase/window', () => {
  assert.equal(evaluateStress(samples(), criteria).status, 'passed');
  assert.equal(evaluateStress(samples().slice(1), criteria).status, 'insufficient-evidence');
  assert.equal(evaluateStress(samples().map(sample => ({ ...sample, wave: 0 })), criteria).status, 'insufficient-evidence');
  assert.equal(evaluateStress(samples().map(sample => ({ ...sample, phase: 'before-optimize' })), criteria).status, 'insufficient-evidence');
});

test('smooth growth is not stable merely because variation is low', () => {
  const growing = samples().map((sample, index) => ({ ...sample, rssBytes: 100 + index * 6 }));
  const result = evaluateStress(growing, criteria);
  assert.equal(result.status, 'failed');
  assert(result.reasons.some(reason => reason.includes('rssBytes')));
});

test('counter reset, missing values, jumps and unordered time remain insufficient evidence', () => {
  for (const patch of [{ wave: 9 }, { wave: 0 }, { rssBytes: NaN }, { driverRows: 0 }, { timestamp: 1000 }]) {
    const rows = samples();
    rows[2] = { ...rows[2], ...patch };
    assert.equal(evaluateStress(rows, criteria).status, 'insufficient-evidence');
  }
});

test('dead/stuttering driver, ceilings, restarts, OOM and exhausted retries cannot pass', () => {
  for (const patch of [
    { rssBytes: 1001 }, { physicalBytes: 1001 }, { backlog: 21 }, { restarts: 1 },
    { oomEvents: 1 }, { exhaustedRetries: 1 }, { driverRows: 100 },
  ]) {
    const rows = samples();
    rows[2] = { ...rows[2], ...patch };
    assert.equal(evaluateStress(rows, criteria).status, 'failed');
  }
  assert.equal(evaluateStress(samples().map(sample => ({ ...sample, driverRows: sample.wave })), criteria).status, 'failed');
});

test('unexecuted later windows cannot rescue a failed declared window', () => {
  const rows = samples();
  rows[1].oomEvents = 1;
  rows.push(...samples().map(sample => ({ ...sample, timestamp: sample.timestamp + 10000, wave: sample.wave + 4 })));
  assert.equal(evaluateStress(rows, criteria).status, 'failed');
  assert.throws(() => evaluateStress(rows, { ...criteria, minimumSamples: 1 }), /Invalid/u);
});
