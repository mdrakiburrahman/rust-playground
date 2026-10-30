import assert from 'node:assert/strict';
import { test } from 'node:test';
import { assess, type DeltaFile, type Snapshot, type TrialEvidence } from './evidence.js';

const file = (path: string): DeltaFile => ({ path, bytes: 10, partition: path[0]! });
const snapshot = (paths: string[], version = 1): Snapshot =>
  ({ version, files: paths.map(file), rows: 4, fingerprint: 'exact' });
function trial(): TrialEvidence {
  return {
    before: snapshot(['a/1', 'b/1']),
    after: snapshot(['a/2', 'b/1'], 2),
    mutationStartedAt: '2026-01-01T00:00:00Z',
    observationEndedAt: '2026-01-01T00:01:00Z',
    samples: [{ at: '2026-01-01T00:00:00Z', completionPercent: 100 }],
    originEvents: [{ at: '2026-01-01T00:00:30Z', kind: 'ingestion', path: 'a/2' }],
    correctness: 'passed',
    capabilities: { originHistory: true, completeHistory: true, distinguishesIngestionFromMerge: true,
      positiveControlDetected: true, backgroundCpu: false, reason: 'Calibrated origin history.' },
  };
}

test('partial OPTIMIZE replacements are not unchanged-file reingestion', () => {
  const result = assess(trial());
  assert.equal(result.status, 'passed');
  assert.equal(result.addedFiles, 1);
  assert.equal(result.removedFiles, 1);
  assert.equal(result.unchangedFiles, 1);
  assert.equal(result.unchangedReingestedFiles, 0);
});
test('100 percent completion before and after cannot establish a no-reload verdict', () => {
  const input = trial();
  input.capabilities.originHistory = false;
  assert.equal(assess(input).status, 'inconclusive');
  assert.equal(assess(input).unchangedReingestedFiles, undefined);
});
test('uncalibrated or incomplete negative evidence is inconclusive', () => {
  for (const flag of ['completeHistory', 'positiveControlDetected'] as const) {
    const input = trial();
    input.capabilities[flag] = false;
    assert.equal(assess(input).status, 'inconclusive');
  }
});
test('extent merge does not imply ingestion, but verified repeated origin does', () => {
  const input = trial();
  input.originEvents.push({ at: '2026-01-01T00:00:20Z', path: 'b/1', kind: 'merge' });
  assert.equal(assess(input).status, 'passed');
  input.originEvents.push({ at: '2026-01-01T00:00:30Z', path: 'b/1', kind: 'ingestion' });
  assert.equal(assess(input).classification, 'unchanged-files-reingested');
});
test('known reload ingests all pre-existing files; metadata replay alone is not a reload', () => {
  const input = trial();
  input.after = snapshot(['a/1', 'b/1'], 3);
  input.originEvents = input.after.files.map(file => ({
    at: '2026-01-01T00:00:30Z', path: file.path, kind: 'ingestion',
  }));
  assert.equal(assess(input).classification, 'whole-cache-rebuilt');
  input.originEvents = [];
  assert.equal(assess(input).classification, 'only-added-or-rewritten-files');
});
test('broad OPTIMIZE legitimately replaces all data files', () => {
  const input = trial();
  input.after = snapshot(['a/2', 'b/2'], 2);
  input.originEvents.push({ at: '2026-01-01T00:00:30Z', path: 'b/2', kind: 'ingestion' });
  assert.equal(assess(input).status, 'passed');
  assert.equal(assess(input).unchangedFiles, 0);
});
test('PURGE removal and VACUUM physical deletion are not new cache ingestion', () => {
  const input = trial();
  input.after = snapshot(['b/1'], 2);
  input.originEvents = [{ at: '2026-01-01T00:00:30Z', kind: 'removal', path: 'a/1' }];
  assert.equal(assess(input).status, 'passed');
  input.before = input.after;
  assert.equal(assess(input).addedFiles, 0);
});
test('query correctness failure overrides a cache-behavior verdict', () => {
  const input = trial();
  input.correctness = 'failed';
  assert.equal(assess(input).status, 'failed');
});
test('immutable identity violations and duplicated inventories are rejected', () => {
  const input = trial();
  input.after.files[1]!.bytes = 11;
  assert.throws(() => assess(input), /changed size/);
  input.after = snapshot(['a/1', 'a/1']);
  assert.throws(() => assess(input), /duplicate/);
});
