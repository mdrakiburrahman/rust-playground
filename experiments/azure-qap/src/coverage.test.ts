import assert from 'node:assert/strict';
import test from 'node:test';
import { analyzeCoverage } from './coverage.js';
import type { CoverageInput, CoverageObservation, SourceSnapshot } from './coverage.js';

function observation(snapshot: SourceSnapshot, atMs: number): CoverageObservation {
  const state = { version: snapshot.version, completionPercent: 100, pendingFiles: 0, pendingBytes: 0,
    acceleratedRows: snapshot.rows, acceleratedFiles: snapshot.files, healthy: true };
  return { startedAtMs: atMs - 10, endedAtMs: atMs + 10, before: { ...state }, after: { ...state },
    provenance: { rows: snapshot.rows, rowsWithValidExtentId: snapshot.rows, distinctExtentCount: snapshot.files,
      fallbackDetected: false, externalDataRead: false, partial: false, warnings: [] } };
}

function trace(rowsPerCommit = 120_000_000): CoverageInput {
  const cohorts = Array.from({ length: 66 }, (_, index) => ({ version: index + 1, rows: rowsPerCommit, files: 1,
    bytes: 100_000_000, finishedAtMs: (index + 1) * 10_000 }));
  const sourceSnapshots = Array.from({ length: 67 }, (_, version): SourceSnapshot => ({ version,
    rows: version * rowsPerCommit, files: version, bytes: version * 100_000_000,
    maxFileBytes: version ? 100_000_000 : 0, removeCount: 0, inventoryComplete: true }));
  return { cohorts, sourceSnapshots,
    observations: sourceSnapshots.slice(1).map(snapshot => observation(snapshot, snapshot.version * 10_000 + 100)),
    warmupStartMs: 0, measurementStartMs: 60_000, measurementEndMs: 660_000, producerStoppedAtMs: 660_000,
    clockErrorMs: 5, sourceFinalized: true, terminalSource: { version: 66, rows: 66 * rowsPerCommit, files: 66 },
    policy: { steadyStateStartMs: 180_000, minCompleteWatermarks: 3, boundaryToleranceMs: 10_500,
      maxObservationGapMs: 10_100, maxSteadyStateDeficitRowsPerSecond: 1000,
      maxBacklogGrowthRowsPerSecond: 0, maxBacklogIncreaseRows: 0 } };
}

function rejects(input: CoverageInput, reason: RegExp): ReturnType<typeof analyzeCoverage> {
  const result = analyzeCoverage(input);
  assert.equal(result.qualified, false);
  assert.ok(result.reasons.some(value => reason.test(value)), JSON.stringify(result.reasons));
  return result;
}

test('eligible contiguous inventory qualifies with bounded cohort latency and separate post-stop certification', () => {
  const input = trace();
  const before = JSON.stringify(input);
  const result = analyzeCoverage(input);
  assert.equal(result.qualified, true, result.reasons.join('; '));
  assert.equal(result.source.rowsPerSecond, 12_000_000);
  assert.equal(result.source.measuredRows, 7_200_000_000);
  assert.deepEqual(result.latency.p95, { lowerMs: 0, upperMs: 115 });
  assert.equal(result.latency.count, 60);
  assert.deepEqual(result.censored, { cohorts: 0, rows: 0, files: 0 });
  assert.equal(result.steadyState.keepsUp, true);
  assert.equal(result.steadyState.backlogSlopeUpperRowsPerSecond, 0);
  assert.equal(result.terminalParity.matched, true);
  assert.equal(result.drain.postStopCertification, true);
  assert.deepEqual(result.drain.lateness, { lowerMs: 0, upperMs: 115 });
  assert.equal(JSON.stringify(input), before, 'analysis must not mutate caller evidence');
  assert.deepEqual(JSON.parse(JSON.stringify(result)), result, 'all output is JSON serializable');
});

test('changing independent aggregate versions and rows cannot certify any cohort', () => {
  const input = trace();
  input.observations = input.observations.map(value => ({ ...value,
    after: { ...value.after, version: value.after!.version! - 1 } }));
  const result = rejects(input, /NO_CERTIFIED_WATERMARK/);
  assert.equal(result.watermarks.length, 0);
  assert.equal(result.censored.cohorts, 60);
  assert.equal(result.latency.p95, null);
});

test('a complete later watermark proves earlier cohorts only across a complete canonical source prefix', () => {
  const complete = trace();
  complete.observations = complete.observations.filter(value => value.before!.version !== 32);
  const proven = rejects(complete, /CERTIFIED_GAP_TOO_LARGE/);
  assert.equal(proven.cohorts.find(cohort => cohort.version === 32)!.watermarkVersion, 33);
  assert.equal(proven.censored.cohorts, 0);
  const missing = trace();
  missing.cohorts = missing.cohorts.filter(cohort => cohort.version !== 32);
  const result = rejects(missing, /SOURCE_COHORT_MISMATCH/);
  assert.equal(result.terminalParity.matched, false);
  assert.ok(result.censored.cohorts > 0);
  const gap = trace();
  gap.sourceSnapshots = gap.sourceSnapshots.filter(snapshot => snapshot.version !== 32);
  rejects(gap, /SOURCE_VERSION_GAP/);
});

test('missing provenance, unknown pending bytes, and reported file-count mismatch fail closed', () => {
  for (const transform of [
    (value: CoverageObservation) => ({ ...value, provenance: undefined }),
    (value: CoverageObservation) => ({ ...value, before: { ...value.before, pendingBytes: undefined },
      after: { ...value.after, pendingBytes: undefined } }),
    (value: CoverageObservation) => ({ ...value,
      before: { ...value.before, acceleratedFiles: value.before!.acceleratedFiles! + 1 },
      after: { ...value.after, acceleratedFiles: value.after!.acceleratedFiles! + 1 } }),
    (value: CoverageObservation) => ({ ...value,
      provenance: { ...value.provenance!, rowsWithValidExtentId: value.provenance!.rows - 1 } }),
  ]) {
    const input = trace();
    input.observations = input.observations.map(value => transform(value));
    const result = rejects(input, /NO_CERTIFIED_WATERMARK/);
    assert.equal(result.terminalParity.matched, false);
  }
});

test('partial results, fallback, external reads, warnings, or telemetry errors cannot be washed away by later success', () => {
  for (const patch of [
    { partial: true }, { fallbackDetected: true }, { externalDataRead: true }, { warnings: ['query truncated'] },
  ]) {
    const input = trace();
    input.observations = input.observations.map((value, index) => index === 30 ?
      { ...value, provenance: { ...value.provenance!, ...patch } } : value);
    const result = rejects(input, /PROVENANCE_UNSAFE/);
    assert.equal(result.terminalParity.matched, true);
  }
  const input = trace();
  input.observations = input.observations.map((value, index) => index === 30 ?
    { ...value, telemetryErrors: ['details request timed out'] } : value);
  rejects(input, /TELEMETRY_ERROR/);
});

test('oversized files, removes, and incomplete inventory invalidate subsequent watermarks', () => {
  for (const patch of [
    { maxFileBytes: 1_000_000_001 }, { removeCount: 1 }, { inventoryComplete: false },
  ]) {
    const input = trace();
    input.sourceSnapshots = input.sourceSnapshots.map(snapshot => snapshot.version >= 32 ? { ...snapshot, ...patch } : snapshot);
    const result = rejects(input, /SOURCE_FILE_OVERSIZED|SOURCE_NOT_APPEND_ONLY|SOURCE_INVENTORY_INCOMPLETE/);
    assert.equal(result.terminalParity.matched, false);
  }
});

test('post-stop-only drain establishes terminal inventory parity but never steady-state keeping up', () => {
  const input = trace();
  input.observations = [observation(input.sourceSnapshots.at(-1)!, 720_000)];
  const result = rejects(input, /STEADY_STATE_WATERMARKS_INSUFFICIENT/);
  assert.equal(result.terminalParity.matched, true);
  assert.equal(result.censored.cohorts, 0);
  assert.equal(result.steadyState.acceleratedRows, 0);
  assert.equal(result.steadyState.acceleratedRowsPerSecondLower, null);
  assert.equal(result.drain.lateness!.upperMs, 60_015);
});

test('unknown clock permits inventory proof but neither bounded latency nor qualification', () => {
  const input = trace();
  input.clockErrorMs = null;
  const result = rejects(input, /CLOCK_BOUND_MISSING/);
  assert.equal(result.terminalParity.matched, true);
  assert.equal(result.latency.count, 0);
  assert.equal(result.latency.p50, null);
  assert.equal(result.drain.lateness, null);
});

test('latency lower bounds use the start of a prior proven noncovering request, not its completion', () => {
  const input = trace();
  input.observations = [...input.observations, observation(input.sourceSnapshots[65], 660_030)];
  const result = analyzeCoverage(input);
  assert.deepEqual(result.cohorts.at(-1)!.latency, { lowerMs: 15, upperMs: 115 });
  assert.deepEqual(result.drain.lateness, { lowerMs: 15, upperMs: 115 });
});

test('materially growing steady-state backlog fails even when terminal drain succeeds and rate tolerance is loose', () => {
  const input = trace();
  input.policy.maxSteadyStateDeficitRowsPerSecond = 5_000_000;
  input.observations = input.observations.map(value => {
    const version = value.before!.version!;
    return version >= 18 && version < 66 ?
      observation(input.sourceSnapshots[18 + Math.floor((version - 18) * 0.8)], version * 10_000 + 100) : value;
  });
  const result = rejects(input, /STEADY_STATE_BACKLOG_GROWING_OR_UNBOUNDED/);
  assert.equal(result.terminalParity.matched, true);
  assert.ok(result.steadyState.backlogSlopeRowsPerSecond! > 2_000_000);
  assert.ok(result.steadyState.backlogIncreaseUpperRows! > 1_000_000_000);
});

test('long gaps and many repeated old watermarks do not establish a complete steady-state trace', () => {
  const gap = trace();
  gap.observations = gap.observations.filter(value => value.startedAtMs < 300_000 || value.startedAtMs > 500_000);
  const result = rejects(gap, /STEADY_STATE_CERTIFIED_GAP_TOO_LARGE/);
  assert.ok(result.observations.maxPollingGapMs > 190_000);
  const repeated = trace();
  repeated.observations = repeated.observations.map(value => value.before!.version! >= 18 && value.before!.version! < 66 ?
    observation(repeated.sourceSnapshots[18], value.endedAtMs - 10) : value);
  rejects(repeated, /STEADY_STATE_WATERMARKS_INSUFFICIENT/);
});

test('slow source is reported independently of healthy QAP keeping up; warmup cannot pad the 600-second rate', () => {
  const input = trace(20_000_000);
  const result = rejects(input, /^SOURCE_RATE_BELOW_TARGET$/);
  assert.equal(result.source.rowsPerSecond, 2_000_000);
  assert.equal(result.steadyState.keepsUp, true);
  assert.equal(result.terminalParity.matched, true);
  input.measurementEndMs -= 1000;
  rejects(input, /MEASUREMENT_DURATION_NOT_600_SECONDS/);
});

test('terminal inventory requires the exact finalized source version, rows, and files', () => {
  for (const field of ['version', 'rows', 'files'] as const) {
    const input = trace();
    input.terminalSource = { ...input.terminalSource!, [field]: input.terminalSource![field] + 1 };
    assert.equal(rejects(input, /TERMINAL_SOURCE_MISMATCH_OR_MISSING/).terminalParity.matched, false);
  }
  const input = trace();
  input.sourceFinalized = false;
  assert.equal(rejects(input, /SOURCE_NOT_FINALIZED/).terminalParity.matched, false);
});

test('a ready-state inventory mismatch is not erased by a subsequent exact watermark', () => {
  const input = trace();
  input.observations = input.observations.map((value, index) => index === 30 ? { ...value,
    before: { ...value.before, acceleratedRows: value.before!.acceleratedRows! - 1 },
    after: { ...value.after, acceleratedRows: value.after!.acceleratedRows! - 1 },
  } : value);
  const result = rejects(input, /READY_INVENTORY_MISMATCH/);
  assert.equal(result.terminalParity.matched, true);
});

test('an unproven terminal cohort remains censored instead of disappearing from successful latency summaries', () => {
  const input = trace();
  input.observations = input.observations.slice(0, -1);
  const result = rejects(input, /MEASURED_COHORTS_CENSORED/);
  assert.equal(result.latency.count, 59);
  assert.deepEqual(result.censored, { cohorts: 1, rows: 120_000_000, files: 1 });
  assert.equal(result.cohorts.at(-1)!.latency, null);
  assert.equal(result.drain.complete, false);
  assert.equal(result.drain.lateness, null);
});

test('valid incomplete evidence returns a miss while malformed intervals, counters, and duplicate versions throw', () => {
  const empty = trace();
  empty.observations = [];
  assert.equal(rejects(empty, /NO_CERTIFIED_WATERMARK/).censored.cohorts, 60);
  const reversed = trace();
  reversed.observations = [{ ...reversed.observations[0], endedAtMs: 1 }];
  assert.throws(() => analyzeCoverage(reversed), /Reversed observation interval/);
  const duplicate = trace();
  duplicate.cohorts = [...duplicate.cohorts, duplicate.cohorts[0]];
  assert.throws(() => analyzeCoverage(duplicate), /Duplicate cohorts version/);
  const invalid = trace();
  invalid.clockErrorMs = Number.NaN;
  assert.throws(() => analyzeCoverage(invalid), /Invalid clockErrorMs/);
});
