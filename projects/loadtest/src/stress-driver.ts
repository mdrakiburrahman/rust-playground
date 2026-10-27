import { createHash } from 'node:crypto';
import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { evaluateStress, type StressCriteria, type StressVerdict, type WaveSample } from './stress-criteria.js';

export interface StressPlan {
  version: 1;
  durationSeconds: number;
  waveIntervalSeconds: number;
  operationTimeoutSeconds: number;
  startupTimeoutSeconds: number;
  sampleTimeoutSeconds: number;
  generator?: { warmupSeconds: number; measureSeconds: number; captureSettleSeconds: number };
  fixture: { batchRows: number; rowGroupRows: number; pageRows: number };
  limits: Omit<StressCriteria, 'start' | 'end' | 'phase' | 'endpointToleranceMs'>;
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Expected stress-plan object');
  return value as Record<string, unknown>;
}

export function parseStressPlan(value: unknown): StressPlan {
  const plan = object(value);
  const fields = ['version', 'durationSeconds', 'waveIntervalSeconds', 'operationTimeoutSeconds',
    'startupTimeoutSeconds', 'sampleTimeoutSeconds', 'fixture', 'limits', 'generator'];
  if (Object.keys(plan).some(key => !fields.includes(key)) || plan.version !== 1) throw new Error('Unsupported stress-plan fields/version');
  for (const key of ['durationSeconds', 'waveIntervalSeconds', 'operationTimeoutSeconds', 'startupTimeoutSeconds', 'sampleTimeoutSeconds']) {
    if (typeof plan[key] !== 'number' || !Number.isSafeInteger(plan[key]) || plan[key] <= 0) {
      throw new Error(`Invalid stress-plan ${key}`);
    }
  }
  const duration = Number(plan.durationSeconds), interval = Number(plan.waveIntervalSeconds);
  if (duration > 7 * 86400 || interval > 3600 || Number(plan.startupTimeoutSeconds) > 3600 ||
      Number(plan.sampleTimeoutSeconds) > 60 || duration % interval !== 0 ||
      Number(plan.operationTimeoutSeconds) + Number(plan.sampleTimeoutSeconds) >= interval ||
      Number(plan.sampleTimeoutSeconds) * 4 > duration) {
    throw new Error('Stress duration must be whole bounded waves with separate operation and sample budgets');
  }
  const limits = object(plan.limits);
  const fixture = object(plan.fixture);
  if (Object.keys(fixture).length !== 3 ||
      ['batchRows', 'rowGroupRows', 'pageRows'].some(key => !Number.isSafeInteger(fixture[key]) || Number(fixture[key]) <= 0) ||
      Number(fixture.batchRows) > 65536 || Number(fixture.rowGroupRows) >= Number(fixture.batchRows) ||
      Number(fixture.pageRows) > Number(fixture.rowGroupRows)) {
    throw new Error('Declare a bounded source batch with multiple row groups and valid page size');
  }
  const limitKeys = ['minimumCycles', 'minimumSamples', 'minimumDriverRowsPerSecond', 'maximumBacklog',
    'maximumRssBytes', 'maximumPhysicalBytes', 'maximumRelativeTrend', 'maximumRelativeVariation'];
  if (Object.keys(limits).length !== limitKeys.length ||
      limitKeys.some(key => typeof limits[key] !== 'number' || !Number.isFinite(limits[key]) || Number(limits[key]) < 0)) {
    throw new Error('Every predeclared stress limit is required; unknown/missing limits are rejected');
  }
  if (!Number.isInteger(limits.minimumCycles) || Number(limits.minimumCycles) < 1 ||
      !Number.isInteger(limits.minimumSamples) || Number(limits.minimumSamples) < 3 ||
      Number(limits.minimumCycles) > duration / interval || Number(limits.minimumSamples) > duration / interval + 1) {
    throw new Error('Insufficient declared operation waves for the requested sample/cycle gates');
  }
  let generator: StressPlan['generator'];
  if (plan.generator !== undefined) {
    const declared = object(plan.generator);
    const keys = ['warmupSeconds', 'measureSeconds', 'captureSettleSeconds'];
    if (Object.keys(declared).length !== keys.length || keys.some(key =>
      !Number.isSafeInteger(declared[key]) || Number(declared[key]) < 0 || Number(declared[key]) > 7 * 86400)) {
      throw new Error('Declare complete bounded generator warmup, measure and capture-settle budgets');
    }
    generator = { warmupSeconds: Number(declared.warmupSeconds), measureSeconds: Number(declared.measureSeconds),
      captureSettleSeconds: Number(declared.captureSettleSeconds) };
  }
  const parsed: StressPlan = {
    version: 1, durationSeconds: duration, waveIntervalSeconds: interval,
    operationTimeoutSeconds: Number(plan.operationTimeoutSeconds),
    startupTimeoutSeconds: Number(plan.startupTimeoutSeconds), sampleTimeoutSeconds: Number(plan.sampleTimeoutSeconds),
    fixture: { batchRows: Number(fixture.batchRows), rowGroupRows: Number(fixture.rowGroupRows), pageRows: Number(fixture.pageRows) },
    ...(generator ? { generator } : {}),
    limits: {
      minimumCycles: Number(limits.minimumCycles), minimumSamples: Number(limits.minimumSamples),
      minimumDriverRowsPerSecond: Number(limits.minimumDriverRowsPerSecond),
      maximumBacklog: Number(limits.maximumBacklog), maximumRssBytes: Number(limits.maximumRssBytes),
      maximumPhysicalBytes: Number(limits.maximumPhysicalBytes), maximumRelativeTrend: Number(limits.maximumRelativeTrend),
      maximumRelativeVariation: Number(limits.maximumRelativeVariation),
    },
  };
  if (generator) stressInputBudget(parsed, generator);
  return parsed;
}

export function stressInputBudget(
  plan: StressPlan,
  actual: { warmupSeconds: number; captureSettleSeconds: number; measureSeconds?: number },
) {
  const minimumMeasure = plan.durationSeconds + plan.sampleTimeoutSeconds * 2 + 5;
  const measureSeconds = actual.measureSeconds ?? plan.generator?.measureSeconds ?? minimumMeasure;
  if (plan.generator && (actual.warmupSeconds !== plan.generator.warmupSeconds ||
      actual.captureSettleSeconds !== plan.generator.captureSettleSeconds || measureSeconds !== plan.generator.measureSeconds)) {
    throw new Error('Actual generator options differ from the immutable stress-plan declaration');
  }
  if (actual.warmupSeconds + actual.captureSettleSeconds >= plan.startupTimeoutSeconds) {
    throw new Error('Stress startup budget must include capture settling and warmup');
  }
  if (measureSeconds < minimumMeasure) {
    throw new Error('--measure is shorter than the predeclared stress window and sample budgets');
  }
  return {
    persistedPlanSha256: createHash('sha256').update(`${JSON.stringify(plan, null, 2)}\n`).digest('hex'),
    warmupSeconds: actual.warmupSeconds, captureSettleSeconds: actual.captureSettleSeconds, measureSeconds,
    totalGeneratorSeconds: actual.warmupSeconds + measureSeconds,
    totalDeclaredPhaseSeconds: actual.captureSettleSeconds + actual.warmupSeconds + measureSeconds,
    helperProcessTimeoutSeconds: actual.warmupSeconds + measureSeconds + 600,
    excludedFromDeclaredPhaseSeconds: 'helper initialization, receipt drain and validation; bounded separately',
    startupTimeoutSeconds: plan.startupTimeoutSeconds, windowSeconds: plan.durationSeconds,
    finalSampleSeconds: plan.sampleTimeoutSeconds,
    remainingInputPolicy: 'actual remaining input rechecked after readiness and again before immutable window start',
  };
}

export function readStressPlan(file: string): StressPlan {
  return parseStressPlan(JSON.parse(readFileSync(file, 'utf8')) as unknown);
}

export interface StressDriverOptions {
  plan: StressPlan;
  results: string;
  loadEnded: AbortSignal;
  checkRunning: () => void;
  /** Resolves after BOTH actual compactable fixtures and sufficient remaining generator input are evidenced. */
  waitForInput: (deadline: number) => Promise<{ runId: string; inputDeadline: number }>;
  /** Runs real operations on both pools. The deadline is absolute and may not be extended. */
  operate: (wave: number, deadline: number) => Promise<void>;
  sample: (wave: number, runId: string, deadline: number) => Promise<WaveSample>;
}

/** One declared window, one verdict. No resampling until green, interpolation, or relaxed deadlines. */
export async function runStressDriver(options: StressDriverOptions): Promise<StressVerdict> {
  const { plan, results } = options;
  const samples: WaveSample[] = [];
  let criteria: StressCriteria | undefined;
  let runId: string | undefined;
  let failure: unknown;
  const check = (): void => {
    options.checkRunning();
    if (options.loadEnded.aborted) throw new Error('Source generation ended before the fixed stress window completed');
  };
  writeFileSync(path.join(results, 'stress-plan.json'), `${JSON.stringify(plan, null, 2)}\n`, { flag: 'wx' });
  try {
    const input = await options.waitForInput(Date.now() + plan.startupTimeoutSeconds * 1000);
    runId = input.runId;
    check();
    const start = Date.now();
    if (!Number.isSafeInteger(input.inputDeadline) ||
        input.inputDeadline < start + (plan.durationSeconds + plan.sampleTimeoutSeconds) * 1000) {
      throw new Error('Remaining generator input cannot cover the immutable window at its actual start');
    }
    criteria = { ...plan.limits, start, end: start + plan.durationSeconds * 1000,
      phase: 'wave-boundary', endpointToleranceMs: plan.sampleTimeoutSeconds * 1000 };
    writeFileSync(path.join(results, 'stress-window.json'), `${JSON.stringify({
      run_id: runId, input_deadline: input.inputDeadline, criteria, correctness: 'requires-final-source-and-native-validation',
      inputCounter: 'scheduled logical rows, not ACKs or a correctness oracle',
      corpusLimits: { maximumDistinctRowsPerPartition: 256, maximumDistinctRowsPerTable: 65536, generatedPayloadPeriodRows: 16 },
      backlogUnit: 'work items; component counters retained in resource evidence',
      physicalCoverage: 'complete source/tenant/WAL/staging roots; excludes SQL/Kusto/Docker layers',
      resourceCoverage: 'two source collector processes; not whole-workspace RSS',
      qualification: 'local-functional-rehearsal; not Azure stress/performance evidence',
    }, null, 2)}\n`, { flag: 'wx' });
    const sample = async (wave: number): Promise<void> => {
      check();
      const deadline = Date.now() + plan.sampleTimeoutSeconds * 1000;
      const frame = await options.sample(wave, runId!, deadline);
      if (Date.now() > deadline) throw new Error('Sample acquisition exceeded its predeclared budget');
      if (frame.wave !== wave || frame.phase !== criteria!.phase) throw new Error('Sampler returned another operation phase');
      samples.push(frame);
      appendFileSync(path.join(results, 'stress-samples.jsonl'), `${JSON.stringify(frame)}\n`);
    };
    await sample(0);
    for (let wave = 1; wave <= plan.durationSeconds / plan.waveIntervalSeconds; wave++) {
      check();
      const boundary = start + wave * plan.waveIntervalSeconds * 1000;
      const deadline = Math.min(boundary, Date.now() + plan.operationTimeoutSeconds * 1000);
      await options.operate(wave, deadline);
      if (Date.now() > deadline) throw new Error('Maintenance wave exceeded its fixed operation budget');
      check();
      await delay(Math.max(0, boundary - Date.now()), undefined, { signal: options.loadEnded });
      await sample(wave);
    }
  } catch (error) {
    failure = error;
  }
  const verdict: StressVerdict = criteria
    ? evaluateStress(samples, criteria)
    : { status: 'insufficient-evidence',
      reasons: ['Actual BOTH-pool fixture readiness and input coverage were not established before the window'], samples: 0, cycles: 0 };
  if (failure) {
    verdict.status = 'failed';
    verdict.reasons.push('Actual driver/operation/telemetry failure; window was not extended');
  }
  writeFileSync(path.join(results, 'stress-verdict.json'), `${JSON.stringify({
    run_id: runId, ...verdict, failureKind: failure instanceof Error ? failure.name : undefined,
    correctness: 'requires-final-source-and-native-validation',
    qualification: 'local-functional-rehearsal; not Azure stress/performance evidence',
  }, null, 2)}\n`);
  if (failure) throw failure;
  if (verdict.status !== 'passed') throw new Error(`Stress ${verdict.status}: ${verdict.reasons.join('; ')}`);
  return verdict;
}
