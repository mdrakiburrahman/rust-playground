import { parseMode, type LoadtestMode } from './mode.js';
import type { StressPlan } from './stress-driver.js';

export interface BenchmarkOptions {
  mode: LoadtestMode;
  warmupSeconds: number;
  measureSeconds?: number;
  captureSettleSeconds: number;
  drainTimeoutSeconds: number;
  requestTimeoutSeconds: number;
  concurrency: number;
  batchRows: number;
  seed: number;
  validate: boolean;
  /** Local validated runs compact after SQL publisher release unless explicitly disabled. */
  finalCompaction?: boolean;
  /** Persist every batch to Quiver on local disk before publishing it to Delta. */
  quiver: boolean;
  /** SIGKILL and restart both collectors this long after generation starts (requires quiver). */
  crashAfterSeconds?: number;
  stressPlan?: string;
  stressConfiguration?: StressPlan;
}

const MAX_DURATION_SECONDS = 2_147_483_647;
export const BENCHMARK_PROJECT = 'rust-playground-loadtest-latest';

export function parseDuration(value: string, name: string): number {
  const match = /^([0-9]+)([smh]?)$/iu.exec(value);
  if (!match) throw new Error(`${name} must look like 30, 30s, 2m, or 1h`);
  const unit = match[2].toLowerCase();
  const seconds = Number(match[1]) * (unit === 'h' ? 3600 : unit === 'm' ? 60 : 1);
  if (!Number.isSafeInteger(seconds) || seconds > MAX_DURATION_SECONDS) throw new Error(`${name} is out of range`);
  return seconds;
}

function positiveInteger(value: string, name: string): number {
  const parsed = Number(value);
  if (!/^[0-9]+$/u.test(value) || !Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

export function parseBenchmarkOptions(args: string[], env: NodeJS.ProcessEnv = process.env): BenchmarkOptions {
  let mode = env.ARROWCOL_MODE ?? 'local';
  let warmup = '30s';
  let concurrency = '4';
  let batchRows = '8192';
  let measure: string | undefined;
  let crashAfter: string | undefined;
  let validate = false;
  let quiver = false;
  let stressPlan: string | undefined;
  let finalCompaction: boolean | undefined;
  for (let index = 0; index < args.length; index++) {
    if (args[index] === '--validate') {
      validate = true;
      continue;
    }
    if (args[index] === '--quiver') {
      quiver = true;
      continue;
    }
    if (args[index] === '--final-compaction' || args[index] === '--no-final-compaction') {
      if (finalCompaction !== undefined) throw new Error('Specify final compaction only once');
      finalCompaction = args[index] === '--final-compaction';
      continue;
    }
    const inline = /^(--[a-z-]+)=(.*)$/u.exec(args[index]);
    const option = inline?.[1] ?? args[index];
    const value = inline?.[2] ?? args[++index];
    if (value === undefined || value === '') throw new Error(`${option} requires a value`);
    if (option === '--mode') mode = value;
    else if (option === '--warmup') warmup = value;
    else if (option === '--concurrency') concurrency = value;
    else if (option === '--measure') measure = value;
    else if (option === '--batch-rows') batchRows = value;
    else if (option === '--crash-after') crashAfter = value;
    else if (option === '--stress-plan') stressPlan = value;
    else throw new Error(`Unknown option '${option}'. Options: ${benchmarkUsage}`);
  }
  if (crashAfter !== undefined && !quiver) {
    throw new Error('--crash-after requires --quiver: only disk-persisted batches survive a collector crash');
  }
  if (stressPlan && (!validate || crashAfter !== undefined)) {
    throw new Error('--stress-plan requires --validate; planned crash recovery is a separate functional run');
  }
  const crashAfterSeconds = crashAfter === undefined ? undefined : parseDuration(crashAfter, '--crash-after');
  if (crashAfterSeconds === 0) throw new Error('--crash-after must be positive');
  const warmupSeconds = parseDuration(warmup, '--warmup');
  const measureSeconds = measure === undefined ? undefined : parseDuration(measure, '--measure');
  if (crashAfterSeconds !== undefined && measureSeconds !== undefined && crashAfterSeconds >= warmupSeconds + measureSeconds) {
    throw new Error('--crash-after must be shorter than --warmup + --measure so the crash interrupts live load');
  }
  const parsedMode = parseMode(mode);
  if (parsedMode === 'azure' && finalCompaction === true) {
    throw new Error('Explicit --final-compaction is unsupported in Azure mode; no cloud writes were started');
  }
  if (finalCompaction === true && !validate) throw new Error('--final-compaction requires --validate');
  return {
    mode: parsedMode,
    finalCompaction: finalCompaction ?? parsedMode === 'local',
    warmupSeconds,
    measureSeconds,
    captureSettleSeconds: 30,
    drainTimeoutSeconds: 480,
    requestTimeoutSeconds: 60,
    concurrency: positiveInteger(concurrency, '--concurrency'),
    batchRows: positiveInteger(batchRows, '--batch-rows'),
    seed: 20260923,
    validate,
    quiver,
    ...(crashAfterSeconds === undefined ? {} : { crashAfterSeconds }),
    ...(stressPlan === undefined ? {} : { stressPlan }),
  };
}

export const benchmarkUsage = '[--mode local|azure (default local)] [--warmup 30s] [--measure 1m (omit to run until Ctrl+C)] [--concurrency 4] [--batch-rows 8192] [--validate] ' +
  '[--quiver] [--crash-after 20s (requires --quiver)] [--stress-plan plan.json (requires --validate)] ' +
  '[--final-compaction|--no-final-compaction (validated local default on; Azure unsupported)]';
