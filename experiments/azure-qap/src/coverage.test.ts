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
    clockErrorMs: 5, sourceFinalized: true, sourceAppendOnly: true,
    terminalSource: { version: 66, rows: 66 * rowsPerCommit, files: 66 },
    policy: { steadyStateStartMs: 180_000, minCompleteWatermarks: 3, boundaryToleranceMs: 10_500,
      maxObservationGapMs: 10_100, maxSteadyStateDeficitRowsPerSecond: 1000,
      maxBacklogGrowthRowsPerSecond: 0, maxBacklogIncreaseRows: 0 } };
}

function boundedTrace(rowsPerCommit = 120_000_000, halfWidthMs = 20): CoverageInput {
  const input = trace(rowsPerCommit);
  input.clockErrorMs = null;
  input.producerMeasurementWindow = { startMs: input.measurementStartMs, endMs: input.measurementEndMs };
  input.cohorts = input.cohorts.map(cohort => ({ ...cohort,
    commitBounds: { lowerMs: Math.max(0, cohort.finishedAtMs - halfWidthMs), upperMs: cohort.finishedAtMs + halfWidthMs } }));
  return input;
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
  const rewritten = trace();
  rewritten.sourceAppendOnly = false;
  assert.equal(rejects(rewritten, /SOURCE_NOT_APPEND_ONLY/).watermarks.length, 0);
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

test('a short smoke with no acceleration reports censored evidence and an empty declared steady interval without throwing', () => {
  const input = trace();
  input.measurementEndMs = 120_000;
  input.producerStoppedAtMs = 120_000;
  input.cohorts = input.cohorts.slice(0, 12);
  input.sourceSnapshots = input.sourceSnapshots.slice(0, 13);
  input.terminalSource = { version: 12, rows: 1_440_000_000, files: 12 };
  input.observations = input.observations.slice(0, 12).map(value => ({ ...value, provenance: undefined,
    before: { ...value.before, completionPercent: 0, acceleratedRows: 0, pendingFiles: 1 },
    after: { ...value.after, completionPercent: 0, acceleratedRows: 0, pendingFiles: 1 } }));
  const result = rejects(input, /STEADY_STATE_WINDOW_EMPTY/);
  assert.ok(result.reasons.includes('MEASUREMENT_DURATION_NOT_600_SECONDS'));
  assert.equal(result.source.steadyStateRowsPerSecond, null);
  assert.equal(result.steadyState.acceleratedRowsPerSecondLower, null);
  assert.equal(result.steadyState.keepsUp, false);
  assert.equal(result.watermarks.length, 0);
  assert.equal(result.latency.p95, null);
  assert.deepEqual(result.censored, { cohorts: 6, rows: 720_000_000, files: 6 });
  assert.equal(result.terminalParity.matched, false);
  assert.deepEqual(JSON.parse(JSON.stringify(result)), result);
});

test('complete controller-clock commit bounds qualify without producer clock assumptions or writer-ACK anchoring', () => {
  const input = boundedTrace();
  input.cohorts = input.cohorts.map(cohort => ({ ...cohort, finishedAtMs: cohort.finishedAtMs + 9_000_000 }));
  input.producerMeasurementWindow = { startMs: 9_060_000, endMs: 9_660_000 };
  const before = JSON.stringify(input);
  const result = analyzeCoverage(input);
  assert.equal(result.qualified, true, result.reasons.join('; '));
  assert.equal(result.latency.anchor, 'durable-delta-log-visibility');
  assert.equal(result.latency.clockErrorMs, null);
  assert.deepEqual(result.latency.p95, { lowerMs: 0, upperMs: 130 });
  assert.equal(result.source.measurementBasis, 'durable-delta-log-visibility-bounds');
  assert.equal(result.source.rowsPerSecond, 11_800_000);
  assert.equal(result.source.canonicalProducer!.rowsPerSecond, 12_000_000);
  assert.equal(result.terminalParity.matched, true);
  assert.equal(JSON.stringify(input), before);
  assert.deepEqual(JSON.parse(JSON.stringify(result)), result);
  input.clockErrorMs = 3_600_000;
  assert.deepEqual(analyzeCoverage(input).latency.p95, result.latency.p95, 'producer uncertainty is irrelevant to controller-only bounds');
  assert.equal(analyzeCoverage(input).qualified, true);
});

test('bounded source qualification excludes boundary uncertainty even when the canonical producer rate meets target', () => {
  const result = rejects(boundedTrace(106_000_000), /^SOURCE_RATE_BELOW_TARGET$/);
  assert.equal(result.source.canonicalProducer!.rowsPerSecond, 10_600_000);
  assert.ok(result.source.rowsPerSecond < 10_600_000);
  assert.equal(result.source.measuredRows, 59 * 106_000_000);
  assert.deepEqual(result.source.boundaryUncertain, { cohorts: 2, rows: 212_000_000 });
  assert.equal(result.terminalParity.matched, true);
  assert.equal(result.censored.cohorts, 0);
});

test('physical source membership includes the start but requires the entire interval strictly before the end', () => {
  const input = boundedTrace();
  input.cohorts = input.cohorts.map(cohort => cohort.version === 6 ?
    { ...cohort, commitBounds: { lowerMs: 60_000, upperMs: 60_000 } } : cohort.version === 66 ?
      { ...cohort, commitBounds: { lowerMs: 659_000, upperMs: 660_000 } } : cohort);
  const result = analyzeCoverage(input);
  assert.equal(result.cohorts.find(cohort => cohort.version === 6)!.measurementMembership, 'inside');
  assert.equal(result.cohorts.at(-1)!.measurementMembership, 'boundary-uncertain');
  assert.equal(result.cohorts.at(-1)!.measured, false);
  assert.equal(result.source.measuredRows, 60 * 120_000_000);
  assert.deepEqual(result.source.boundaryUncertain, { cohorts: 1, rows: 120_000_000 });
});

test('physical latency uses commit upper for its lower bound and commit lower for its upper bound', () => {
  const input = boundedTrace();
  input.observations = [...input.observations, observation(input.sourceSnapshots[65], 660_070)];
  const result = analyzeCoverage(input);
  assert.deepEqual(result.cohorts.at(-1)!.latency, { lowerMs: 40, upperMs: 130 });
  assert.deepEqual(result.drain.lateness, { lowerMs: 60, upperMs: 110 });
});

test('listing uncertainty widens latency and definite/possible backlog rather than fabricating a commit instant', () => {
  const precise = analyzeCoverage(boundedTrace());
  const uncertain = rejects(boundedTrace(120_000_000, 20_000), /STEADY_STATE_BACKLOG_GROWING_OR_UNBOUNDED/);
  assert.equal(precise.steadyState.backlog[0].upperRows, 0);
  assert.equal(uncertain.steadyState.backlog[0].lowerRows, 0);
  assert.ok(uncertain.steadyState.backlog[0].upperRows >= 120_000_000);
  assert.ok(uncertain.latency.p95!.upperMs > precise.latency.p95!.upperMs);
});

test('partial bounds never qualify or substitute writer ACKs even with a supplied producer clock bound', () => {
  for (const clockErrorMs of [null, 0]) {
    const input = boundedTrace();
    input.clockErrorMs = clockErrorMs;
    input.cohorts = input.cohorts.map(cohort => cohort.version === 32 ? { ...cohort, commitBounds: undefined } : cohort);
    const result = rejects(input, /COMMIT_BOUNDS_INCOMPLETE/);
    assert.deepEqual(result.source.missingCommitBounds, { cohorts: 1, rows: 120_000_000 });
    const missing = result.cohorts.find(cohort => cohort.version === 32)!;
    assert.equal(missing.measurementMembership, 'unknown');
    assert.equal(missing.latency, null);
    assert.equal(missing.proven, true, 'row inventory proof is distinct from physical commit timing');
  }
});

test('bounded mode does not invent a canonical producer window or credit a post-stop-only acceleration drain', () => {
  const input = boundedTrace();
  input.producerMeasurementWindow = undefined;
  const result = analyzeCoverage(input);
  assert.equal(result.qualified, true, result.reasons.join('; '));
  assert.equal(result.source.canonicalProducer, null);
  input.observations = [observation(input.sourceSnapshots.at(-1)!, 720_000)];
  const drained = rejects(input, /STEADY_STATE_WATERMARKS_INSUFFICIENT/);
  assert.equal(drained.terminalParity.matched, true);
  assert.equal(drained.steadyState.acceleratedRowsPerSecondLower, null);
  assert.deepEqual(drained.drain.lateness, { lowerMs: 0, upperMs: 60_010 });
});

test('malformed and nonmonotonic physical bounds are rejected instead of repaired', () => {
  for (const bounds of [
    { lowerMs: Number.NaN, upperMs: 10_020 },
    { lowerMs: 10_020, upperMs: 9_980 },
    { lowerMs: 0, upperMs: 1 },
    { lowerMs: 9_000, upperMs: 20_020 },
  ]) {
    const input = boundedTrace();
    input.cohorts = input.cohorts.map(cohort => cohort.version === 2 ? { ...cohort, commitBounds: bounds } : cohort);
    assert.throws(() => analyzeCoverage(input), /Invalid commitBounds|Reversed commit bounds|monotonic source prefixes/);
  }
});
