export interface CoverageCohort {
  version: number;
  rows: number;
  files: number;
  bytes: number;
  /** Producer clock, also used by the measurement window. */
  finishedAtMs: number;
}

/** Reconstructed cumulative inventory, including metadata-only versions and warmup. */
export interface SourceSnapshot {
  version: number;
  rows: number;
  files: number;
  bytes: number;
  maxFileBytes: number;
  removeCount: number;
  inventoryComplete: boolean;
}

export interface QapCoverageState {
  version?: number;
  completionPercent?: number;
  pendingFiles?: number;
  pendingBytes?: number;
  acceleratedRows?: number;
  acceleratedFiles?: number;
  healthy?: boolean;
}

/** The query must count the entire source, not a sample or a filtered cohort. */
export interface ExtentProvenance {
  rows: number;
  rowsWithValidExtentId: number;
  distinctExtentCount: number;
  fallbackDetected: boolean;
  externalDataRead: boolean;
  partial: boolean;
  warnings: readonly string[];
}

export interface CoverageObservation {
  /** Controller-clock envelope: before-state read, provenance query, after-state read, in that order. */
  startedAtMs: number;
  endedAtMs: number;
  before?: QapCoverageState;
  after?: QapCoverageState;
  provenance?: ExtentProvenance;
  telemetryErrors?: readonly string[];
}

/** Trusted, predeclared tolerances; never take these from an optimizer candidate. */
export interface CoveragePolicy {
  steadyStateStartMs: number;
  minCompleteWatermarks: number;
  boundaryToleranceMs: number;
  maxObservationGapMs: number;
  maxSteadyStateDeficitRowsPerSecond: number;
  maxBacklogGrowthRowsPerSecond: number;
  maxBacklogIncreaseRows: number;
}

export interface CoverageInput {
  cohorts: readonly CoverageCohort[];
  sourceSnapshots: readonly SourceSnapshot[];
  observations: readonly CoverageObservation[];
  warmupStartMs: number;
  measurementStartMs: number;
  measurementEndMs: number;
  producerStoppedAtMs: number;
  /** Absolute producer/controller clock uncertainty, measured independently; null means unknown. */
  clockErrorMs: number | null;
  sourceFinalized: boolean;
  terminalSource?: { version: number; rows: number; files: number };
  policy: CoveragePolicy;
}

export interface LatencyBounds { lowerMs: number; upperMs: number }
export interface CertifiedWatermark {
  observationIndex: number;
  version: number;
  rows: number;
  files: number;
  startedAtMs: number;
  endedAtMs: number;
}
export interface CohortCoverage extends CoverageCohort {
  measured: boolean;
  proven: boolean;
  certifiedAtMs: number | null;
  watermarkVersion: number | null;
  latency: LatencyBounds | null;
}
export interface BacklogPoint {
  startedAtMs: number;
  endedAtMs: number;
  version: number;
  lowerRows: number;
  upperRows: number;
}
export interface CoverageResult {
  qualified: boolean;
  reasons: string[];
  scope: 'whole-source row/file/version inventory coverage; not content equality or producer identity';
  source: {
    measuredRows: number;
    durationMs: number;
    rowsPerSecond: number;
    steadyStateRowsPerSecond: number;
    meetsTarget: boolean;
    postStopRows: number;
  };
  cohorts: CohortCoverage[];
  censored: { cohorts: number; rows: number; files: number };
  latency: {
    weighting: 'cohort';
    method: 'nearest-rank interval bounds; proven measured cohorts only';
    clockErrorMs: number | null;
    count: number;
    p50: LatencyBounds | null;
    p95: LatencyBounds | null;
    p99: LatencyBounds | null;
  };
  watermarks: CertifiedWatermark[];
  observations: {
    rejected: { observationIndex: number; reason: string }[];
    partial: number;
    missingProvenance: number;
    maxPollingGapMs: number;
    maxRequestDurationMs: number;
  };
  steadyState: {
    startMs: number;
    endMs: number;
    completeWatermarks: number;
    distinctVersions: number;
    maxCertifiedGapMs: number | null;
    acceleratedRows: number;
    coveredDurationMs: number | null;
    acceleratedRowsPerSecondLower: number | null;
    backlog: BacklogPoint[];
    backlogSlopeRowsPerSecond: number | null;
    /** Deterministic worst-case regression bound, not a statistical confidence interval. */
    backlogSlopeUpperRowsPerSecond: number | null;
    backlogIncreaseUpperRows: number | null;
    keepsUp: boolean;
  };
  terminalParity: {
    matched: boolean;
    version: number | null;
    rows: number | null;
    files: number | null;
    certifiedAtMs: number | null;
  };
  drain: {
    complete: boolean;
    postStopCertification: boolean | null;
    lateness: LatencyBounds | null;
    lastObservedAtMs: number | null;
  };
}

const countFields = ['version', 'rows', 'files', 'bytes'] as const;
const stateFields = ['version', 'completionPercent', 'pendingFiles', 'pendingBytes',
  'acceleratedRows', 'acceleratedFiles', 'healthy'] as const;
const targetRowsPerSecond = 10_600_000;

function number(value: unknown, name: string, integer = false, minimum = 0): asserts value is number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < minimum ||
      (integer && !Number.isSafeInteger(value))) throw new TypeError(`Invalid ${name}`);
}

function boolean(value: unknown, name: string): void {
  if (typeof value !== 'boolean') throw new TypeError(`Invalid ${name}`);
}

function strings(value: unknown, name: string): void {
  if (!Array.isArray(value) || value.some(item => typeof item !== 'string')) throw new TypeError(`Invalid ${name}`);
}

function validate(input: CoverageInput): void {
  for (const field of ['cohorts', 'sourceSnapshots', 'observations'] as const)
    if (!Array.isArray(input[field])) throw new TypeError(`Invalid ${field}`);
  for (const field of ['warmupStartMs', 'measurementStartMs', 'measurementEndMs', 'producerStoppedAtMs'] as const)
    number(input[field], field);
  if (input.measurementEndMs <= input.measurementStartMs) throw new TypeError('Invalid measurement window');
  if (input.clockErrorMs !== null && input.clockErrorMs !== undefined) number(input.clockErrorMs, 'clockErrorMs');
  boolean(input.sourceFinalized, 'sourceFinalized');
  for (const [field, value] of Object.entries(input.policy)) number(value, `policy.${field}`);
  for (const field of ['steadyStateStartMs', 'minCompleteWatermarks', 'boundaryToleranceMs',
    'maxObservationGapMs', 'maxSteadyStateDeficitRowsPerSecond', 'maxBacklogGrowthRowsPerSecond',
    'maxBacklogIncreaseRows'] as const) number(input.policy[field], `policy.${field}`);
  number(input.policy.minCompleteWatermarks, 'minCompleteWatermarks', true, 3);
  if (input.policy.steadyStateStartMs < input.measurementStartMs ||
      input.policy.steadyStateStartMs >= input.measurementEndMs || input.policy.maxObservationGapMs <= 0 ||
      input.policy.boundaryToleranceMs > (input.measurementEndMs - input.policy.steadyStateStartMs) / 4)
    throw new TypeError('Invalid steady-state policy');
  for (const [name, entries] of [['cohorts', input.cohorts], ['sourceSnapshots', input.sourceSnapshots]] as const) {
    const seen = new Set<number>();
    for (const entry of entries) {
      for (const field of countFields) number(entry[field], `${name}.${field}`, true);
      if (seen.has(entry.version)) throw new TypeError(`Duplicate ${name} version ${entry.version}`);
      seen.add(entry.version);
    }
  }
  for (const cohort of input.cohorts) number(cohort.finishedAtMs, 'finishedAtMs');
  for (const snapshot of input.sourceSnapshots) {
    number(snapshot.maxFileBytes, 'maxFileBytes', true);
    number(snapshot.removeCount, 'removeCount', true);
    boolean(snapshot.inventoryComplete, 'inventoryComplete');
  }
  if (input.terminalSource)
    for (const field of ['version', 'rows', 'files'] as const) number(input.terminalSource[field], `terminalSource.${field}`, true);
  for (const observation of input.observations) {
    number(observation.startedAtMs, 'startedAtMs');
    number(observation.endedAtMs, 'endedAtMs');
    if (observation.endedAtMs < observation.startedAtMs) throw new TypeError('Reversed observation interval');
    if (observation.telemetryErrors !== undefined) strings(observation.telemetryErrors, 'telemetryErrors');
    for (const state of [observation.before, observation.after]) {
      if (!state) continue;
      for (const field of stateFields) {
        if (state[field] === undefined) continue;
        if (field === 'healthy') boolean(state[field], field);
        else number(state[field], field, field !== 'completionPercent', field === 'version' ? -1 : 0);
      }
      if (state.completionPercent !== undefined && state.completionPercent > 100)
        throw new TypeError('Invalid completionPercent');
    }
    if (observation.provenance) {
      for (const field of ['rows', 'rowsWithValidExtentId', 'distinctExtentCount'] as const)
        number(observation.provenance[field], `provenance.${field}`, true);
      for (const field of ['fallbackDetected', 'externalDataRead', 'partial'] as const)
        boolean(observation.provenance[field], `provenance.${field}`);
      strings(observation.provenance.warnings, 'provenance.warnings');
    }
  }
}

function sum(cohorts: readonly CoverageCohort[], field: 'rows' | 'files'): number {
  const result = cohorts.reduce((total, cohort) => total + cohort[field], 0);
  number(result, `total ${field}`, true);
  return result;
}

function percentile(values: readonly LatencyBounds[], percentile: number): LatencyBounds | null {
  if (!values.length) return null;
  const index = Math.ceil(values.length * percentile) - 1;
  return { lowerMs: values.map(value => value.lowerMs).sort((a, b) => a - b)[index],
    upperMs: values.map(value => value.upperMs).sort((a, b) => a - b)[index] };
}

function trend(points: readonly BacklogPoint[]): { slope: number | null; upper: number | null } {
  if (points.length < 2) return { slope: null, upper: null };
  let numerator = 0, denominator = 0, numeratorUpper = 0, denominatorLower = 0, denominatorUpper = 0;
  // Pairwise OLS identity permits a conservative bound over BOTH timing and backlog intervals.
  for (let i = 0; i < points.length; i++) for (let j = i + 1; j < points.length; j++) {
    const a = points[i], b = points[j];
    const dt = ((b.startedAtMs + b.endedAtMs) - (a.startedAtMs + a.endedAtMs)) / 2000;
    const dtLower = Math.max(0, b.startedAtMs - a.endedAtMs) / 1000;
    const dtUpper = Math.max(0, b.endedAtMs - a.startedAtMs) / 1000;
    const growthUpper = b.upperRows - a.lowerRows;
    numerator += dt * ((b.lowerRows + b.upperRows) - (a.lowerRows + a.upperRows)) / 2;
    denominator += dt * dt;
    numeratorUpper += growthUpper * (growthUpper >= 0 ? dtUpper : dtLower);
    denominatorLower += dtLower * dtLower;
    denominatorUpper += dtUpper * dtUpper;
  }
  const boundDenominator = numeratorUpper >= 0 ? denominatorLower : denominatorUpper;
  return { slope: denominator > 0 ? numerator / denominator : null,
    upper: boundDenominator > 0 ? numeratorUpper / boundDenominator : null };
}

/** No I/O. Malformed values throw; missing proof and legitimate benchmark misses are reportable results. */
export function analyzeCoverage(input: CoverageInput): CoverageResult {
  validate(input);
  const reasons = new Set<string>();
  const fail = (reason: string): void => { reasons.add(reason); };
  const cohorts = [...input.cohorts].sort((a, b) => a.version - b.version);
  const snapshots = [...input.sourceSnapshots].sort((a, b) => a.version - b.version);
  const byVersion = new Map(cohorts.map(cohort => [cohort.version, cohort]));
  const inventory = new Map<number, SourceSnapshot>();
  let prefixValid = true;
  let previous = { version: -1, rows: 0, files: 0, bytes: 0, maxFileBytes: 0 };
  for (const snapshot of snapshots) {
    const cohort = byVersion.get(snapshot.version);
    const delta = { rows: snapshot.rows - previous.rows, files: snapshot.files - previous.files,
      bytes: snapshot.bytes - previous.bytes };
    const problems: string[] = [];
    if (snapshot.version !== previous.version + 1) problems.push('SOURCE_VERSION_GAP');
    if (!snapshot.inventoryComplete) problems.push('SOURCE_INVENTORY_INCOMPLETE');
    if (snapshot.removeCount !== 0) problems.push('SOURCE_NOT_APPEND_ONLY');
    if (snapshot.maxFileBytes > 1_000_000_000) problems.push('SOURCE_FILE_OVERSIZED');
    if (snapshot.maxFileBytes < previous.maxFileBytes || snapshot.bytes < snapshot.maxFileBytes ||
        (snapshot.files === 0 && (snapshot.bytes !== 0 || snapshot.rows !== 0 || snapshot.maxFileBytes !== 0)) ||
        (snapshot.files > 0 && (snapshot.maxFileBytes === 0 || snapshot.bytes > snapshot.files * snapshot.maxFileBytes)))
      problems.push('SOURCE_FILE_INVENTORY_MISMATCH');
    if (delta.rows < 0 || delta.files < 0 || delta.bytes < 0) problems.push('SOURCE_NOT_APPEND_ONLY');
    if (delta.files === 0 && (delta.rows !== 0 || delta.bytes !== 0)) problems.push('SOURCE_FILE_INVENTORY_MISMATCH');
    if (cohort ? (['rows', 'files', 'bytes'] as const).some(field => cohort[field] !== delta[field]) :
      delta.rows !== 0 || delta.files !== 0 || delta.bytes !== 0) problems.push('SOURCE_COHORT_MISMATCH');
    for (const problem of problems) fail(`${problem}: version ${snapshot.version}`);
    prefixValid &&= problems.length === 0;
    if (prefixValid) inventory.set(snapshot.version, snapshot);
    previous = snapshot;
  }
  for (const cohort of cohorts)
    if (!snapshots.some(snapshot => snapshot.version === cohort.version)) fail(`SOURCE_COHORT_WITHOUT_VERSION: ${cohort.version}`);
  if (!snapshots.length) fail('SOURCE_INVENTORY_MISSING');
  if (!input.sourceFinalized) fail('SOURCE_NOT_FINALIZED');
  const terminal = input.terminalSource;
  const lastSnapshot = snapshots.at(-1);
  if (!terminal || !lastSnapshot || terminal.version !== lastSnapshot.version || terminal.rows !== lastSnapshot.rows ||
      terminal.files !== lastSnapshot.files) fail('TERMINAL_SOURCE_MISMATCH_OR_MISSING');
  const durationMs = input.measurementEndMs - input.measurementStartMs;
  if (durationMs !== 600_000) fail('MEASUREMENT_DURATION_NOT_600_SECONDS');
  if (input.measurementStartMs - input.warmupStartMs !== 60_000) fail('WARMUP_NOT_60_SECONDS');
  if (input.producerStoppedAtMs !== input.measurementEndMs) fail('PRODUCER_STOP_NOT_MEASUREMENT_END');
  const clockKnown = input.clockErrorMs !== null && input.clockErrorMs !== undefined;
  const clockErrorMs = input.clockErrorMs ?? 0;
  if (!clockKnown) fail('CLOCK_BOUND_MISSING');
  const measured = cohorts.filter(cohort => cohort.finishedAtMs > input.measurementStartMs &&
    cohort.finishedAtMs <= input.measurementEndMs);
  const steadyCohorts = measured.filter(cohort => cohort.finishedAtMs > input.policy.steadyStateStartMs);
  const measuredRows = sum(measured, 'rows');
  const rowsPerSecond = measuredRows * 1000 / durationMs;
  const steadySourceRate = sum(steadyCohorts, 'rows') * 1000 /
    (input.measurementEndMs - input.policy.steadyStateStartMs);
  const meetsTarget = rowsPerSecond >= targetRowsPerSecond && steadySourceRate >= targetRowsPerSecond;
  if (rowsPerSecond < targetRowsPerSecond) fail('SOURCE_RATE_BELOW_TARGET');
  if (steadySourceRate < targetRowsPerSecond) fail('SOURCE_STEADY_STATE_RATE_BELOW_TARGET');

  const observations = input.observations.map((value, index) => ({ ...value, index }))
    .sort((a, b) => a.endedAtMs - b.endedAtMs || a.startedAtMs - b.startedAtMs);
  const watermarks: CertifiedWatermark[] = [];
  const rejected: CoverageResult['observations']['rejected'] = [];
  let partial = 0, missingProvenance = 0;
  for (const observation of observations) {
    const before = observation.before, after = observation.after, provenance = observation.provenance;
    if (!provenance) missingProvenance++;
    if (provenance?.partial || [before, after].some(state => state &&
      ((state.completionPercent !== undefined && state.completionPercent < 100) || (state.pendingFiles ?? 0) > 0 ||
        (state.pendingBytes ?? 0) > 0))) partial++;
    let rejection = '';
    if (observation.telemetryErrors?.length) { rejection = 'TELEMETRY_ERROR'; fail(rejection); }
    if (provenance && (provenance.fallbackDetected || provenance.externalDataRead || provenance.partial ||
        provenance.warnings.length || provenance.rowsWithValidExtentId !== provenance.rows)) {
      rejection = 'PROVENANCE_UNSAFE'; fail(rejection);
    }
    const snapshot = before?.version === undefined ? undefined : inventory.get(before.version);
    if (!rejection && (!before || !after || !stateFields.every(field => before[field] === after[field])))
      rejection = 'STATE_NOT_STABLE';
    if (!rejection && snapshot && before?.healthy === true && before.completionPercent === 100 &&
        before.pendingFiles === 0 && before.pendingBytes === 0 &&
        ((before.acceleratedRows !== undefined && before.acceleratedRows !== snapshot.rows) ||
          (before.acceleratedFiles !== undefined && before.acceleratedFiles !== snapshot.files))) {
      rejection = 'READY_INVENTORY_MISMATCH'; fail(rejection);
    }
    if (!rejection && (!snapshot || before?.healthy !== true || before.completionPercent !== 100 ||
        before.pendingFiles !== 0 || before.pendingBytes !== 0 || before.acceleratedRows !== snapshot.rows ||
        (before.acceleratedFiles !== undefined && before.acceleratedFiles !== snapshot.files)))
      rejection = 'STATE_NOT_READY_TO_EXACT_INVENTORY';
    if (!rejection && (!provenance || provenance.rows !== snapshot!.rows || provenance.rowsWithValidExtentId !== snapshot!.rows ||
        provenance.rows <= 0 || provenance.distinctExtentCount <= 0 || provenance.distinctExtentCount > provenance.rows))
      rejection = 'WHOLE_SOURCE_PROVENANCE_MISSING_OR_MISMATCH';
    if (rejection) { rejected.push({ observationIndex: observation.index, reason: rejection }); continue; }
    if (watermarks.length && snapshot!.version < watermarks.at(-1)!.version) {
      fail('CERTIFIED_WATERMARK_REGRESSION');
      rejected.push({ observationIndex: observation.index, reason: 'CERTIFIED_WATERMARK_REGRESSION' });
      continue;
    }
    watermarks.push({ observationIndex: observation.index, version: snapshot!.version, rows: snapshot!.rows,
      files: snapshot!.files, startedAtMs: observation.startedAtMs, endedAtMs: observation.endedAtMs });
  }
  if (!watermarks.length) fail('NO_CERTIFIED_WATERMARK');
  const coverage = cohorts.map((cohort): CohortCoverage => {
    const proof = watermarks.find(watermark => watermark.version >= cohort.version && inventory.has(cohort.version));
    const prior = proof && watermarks.filter(watermark => watermark.version < cohort.version &&
      watermark.endedAtMs <= proof.startedAtMs).at(-1);
    const upperMs = proof ? proof.endedAtMs - cohort.finishedAtMs + clockErrorMs : null;
    if (clockKnown && upperMs !== null && upperMs < 0) fail(`CLOCK_OR_COHORT_TIME_INCONSISTENT: version ${cohort.version}`);
    return { ...cohort, measured: measured.includes(cohort), proven: Boolean(proof),
      certifiedAtMs: proof?.endedAtMs ?? null, watermarkVersion: proof?.version ?? null,
      latency: clockKnown && upperMs !== null && upperMs >= 0 ? {
        lowerMs: Math.max(0, (prior?.startedAtMs ?? cohort.finishedAtMs) - cohort.finishedAtMs - clockErrorMs), upperMs,
      } : null };
  });
  const censored = coverage.filter(cohort => cohort.measured && !cohort.proven);
  if (censored.length) fail('MEASURED_COHORTS_CENSORED');
  const latencies = coverage.filter(cohort => cohort.measured).flatMap(cohort => cohort.latency ? [cohort.latency] : []);
  if (!latencies.length) fail('NO_BOUNDED_MEASURED_LATENCY');
  const steady = clockKnown ? watermarks.filter(watermark => watermark.startedAtMs - clockErrorMs >= input.policy.steadyStateStartMs &&
    watermark.endedAtMs + clockErrorMs <= input.measurementEndMs) : [];
  const first = steady.at(0), last = steady.at(-1);
  const distinctVersions = new Set(steady.map(watermark => watermark.version)).size;
  let keepsUp = true;
  const steadyFail = (reason: string): void => { keepsUp = false; fail(reason); };
  if (distinctVersions < input.policy.minCompleteWatermarks) steadyFail('STEADY_STATE_WATERMARKS_INSUFFICIENT');
  if (!first || first.endedAtMs + clockErrorMs - input.policy.steadyStateStartMs > input.policy.boundaryToleranceMs ||
      !last || input.measurementEndMs - (last.startedAtMs - clockErrorMs) > input.policy.boundaryToleranceMs)
    steadyFail('STEADY_STATE_BOUNDARY_COVERAGE_MISSING');
  const gaps = steady.slice(1).map((watermark, i) => watermark.endedAtMs - steady[i].startedAtMs + 2 * clockErrorMs);
  const maxCertifiedGapMs = first && last ? Math.max(first.endedAtMs + clockErrorMs - input.policy.steadyStateStartMs,
    input.measurementEndMs - last.startedAtMs + clockErrorMs, ...gaps) : null;
  if (maxCertifiedGapMs === null || maxCertifiedGapMs > input.policy.maxObservationGapMs)
    steadyFail('STEADY_STATE_CERTIFIED_GAP_TOO_LARGE');
  const coveredDurationMs = first && last ? last.endedAtMs - first.startedAtMs + 2 * clockErrorMs : null;
  const acceleratedRows = first && last ? last.rows - first.rows : 0;
  const acceleratedRate = coveredDurationMs && coveredDurationMs > 0 ? acceleratedRows * 1000 / coveredDurationMs : null;
  if (acceleratedRate === null || acceleratedRate + input.policy.maxSteadyStateDeficitRowsPerSecond < steadySourceRate)
    steadyFail('STEADY_STATE_ACCELERATION_BELOW_SOURCE');
  const sourceRowsAt = (time: number): number => sum(cohorts.filter(cohort => cohort.finishedAtMs <= time), 'rows');
  const backlog = steady.map((watermark): BacklogPoint => ({
    startedAtMs: watermark.startedAtMs - clockErrorMs, endedAtMs: watermark.endedAtMs + clockErrorMs, version: watermark.version,
    lowerRows: Math.max(0, sourceRowsAt(watermark.startedAtMs - clockErrorMs) - watermark.rows),
    upperRows: Math.max(0, sourceRowsAt(watermark.endedAtMs + clockErrorMs) - watermark.rows),
  }));
  const slopes = trend(backlog);
  const increase = backlog.length > 1 ? backlog.at(-1)!.upperRows - backlog[0].lowerRows : null;
  if (slopes.upper === null || slopes.upper > input.policy.maxBacklogGrowthRowsPerSecond ||
      increase === null || increase > input.policy.maxBacklogIncreaseRows) steadyFail('STEADY_STATE_BACKLOG_GROWING_OR_UNBOUNDED');
  const terminalProof = terminal && terminal.version === lastSnapshot?.version && terminal.rows === lastSnapshot.rows &&
    terminal.files === lastSnapshot.files ? watermarks.find(watermark => watermark.version === terminal.version &&
      watermark.rows === terminal.rows && watermark.files === terminal.files) : undefined;
  const terminalMatched = input.sourceFinalized && Boolean(terminalProof);
  if (!terminalMatched) fail('TERMINAL_PARITY_NOT_PROVEN');
  const lastNonTerminal = terminalProof && watermarks.filter(watermark => watermark.version < terminal!.version &&
    watermark.endedAtMs <= terminalProof.startedAtMs).at(-1);
  const pollOrder = [...observations].sort((a, b) => a.startedAtMs - b.startedAtMs);
  let coveredUntil = input.measurementStartMs, maxPollingGapMs = 0;
  for (const observation of pollOrder) {
    if (observation.endedAtMs < input.measurementStartMs || observation.startedAtMs > input.measurementEndMs) continue;
    maxPollingGapMs = Math.max(maxPollingGapMs, observation.startedAtMs - coveredUntil);
    coveredUntil = Math.max(coveredUntil, observation.endedAtMs);
  }
  maxPollingGapMs = Math.max(maxPollingGapMs, input.measurementEndMs - coveredUntil);
  return {
    qualified: reasons.size === 0, reasons: [...reasons],
    scope: 'whole-source row/file/version inventory coverage; not content equality or producer identity',
    source: { measuredRows, durationMs, rowsPerSecond, steadyStateRowsPerSecond: steadySourceRate, meetsTarget,
      postStopRows: sum(cohorts.filter(cohort => cohort.finishedAtMs > input.producerStoppedAtMs), 'rows') },
    cohorts: coverage, censored: { cohorts: censored.length, rows: sum(censored, 'rows'), files: sum(censored, 'files') },
    latency: { weighting: 'cohort', method: 'nearest-rank interval bounds; proven measured cohorts only',
      clockErrorMs: input.clockErrorMs ?? null, count: latencies.length,
      p50: percentile(latencies, 0.5), p95: percentile(latencies, 0.95), p99: percentile(latencies, 0.99) },
    watermarks, observations: { rejected, partial, missingProvenance, maxPollingGapMs,
      maxRequestDurationMs: Math.max(0, ...observations.map(observation => observation.endedAtMs - observation.startedAtMs)) },
    steadyState: { startMs: input.policy.steadyStateStartMs, endMs: input.measurementEndMs,
      completeWatermarks: steady.length, distinctVersions, maxCertifiedGapMs, acceleratedRows, coveredDurationMs,
      acceleratedRowsPerSecondLower: acceleratedRate, backlog, backlogSlopeRowsPerSecond: slopes.slope,
      backlogSlopeUpperRowsPerSecond: slopes.upper, backlogIncreaseUpperRows: increase, keepsUp },
    terminalParity: { matched: terminalMatched, version: terminal?.version ?? null, rows: terminal?.rows ?? null,
      files: terminal?.files ?? null, certifiedAtMs: terminalProof?.endedAtMs ?? null },
    drain: { complete: terminalMatched, postStopCertification: terminalProof && clockKnown ?
      terminalProof.endedAtMs - clockErrorMs > input.producerStoppedAtMs ? true :
        terminalProof.endedAtMs + clockErrorMs <= input.producerStoppedAtMs ? false : null : null,
      lateness: terminalProof && clockKnown ? {
        lowerMs: Math.max(0, (lastNonTerminal?.startedAtMs ?? input.producerStoppedAtMs) - input.producerStoppedAtMs - clockErrorMs),
        upperMs: Math.max(0, terminalProof.endedAtMs - input.producerStoppedAtMs + clockErrorMs),
      } : null, lastObservedAtMs: observations.at(-1)?.endedAtMs ?? null },
  };
}
