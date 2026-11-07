import { writeFileSync } from 'node:fs';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

import { parseMetrics } from './resource-sampler.js';

export const QUIVER_RECOVERY_FILE = 'quiver-recovery.json';

/** Collector metrics that prove disk persistence and post-crash replay. */
export const quiverRecoveryMetrics = Object.freeze({
  dataflow: Object.freeze({
    persisted: 'arrowcol_dataflow_durable_admitted_batches_total',
    backlog: 'arrowcol_dataflow_durable_backlog_batches',
    released: 'arrowcol_dataflow_durable_released_batches_total',
    recovered: 'arrowcol_dataflow_replayed_batches_total',
    reboundBatches: 'arrowcol_dataflow_rebound_batches_total',
  }),
});

type Pool = keyof typeof quiverRecoveryMetrics;
type PoolSample = Record<string, number>;

export interface QuiverCrashOptions {
  crashAfterSeconds: number;
  dataflowAdmin: string;
  results: string;
  /** SIGKILLs the Dataflow publisher; no graceful drain runs. */
  kill: () => Promise<void>;
  /** Restarts Dataflow on the same Quiver directory and waits for health. */
  restart: () => Promise<void>;
  log: (message: string) => void;
  /** Aborted when load generation ends; the crash must interrupt live load. */
  loadEnded: AbortSignal;
  checkRunning: () => void;
  scrape?: (endpoint: string) => Promise<string>;
  pollIntervalMs?: number;
  startTimeoutMs?: number;
  recoveryTimeoutMs?: number;
  adoptionTimeoutMs?: number;
}

export interface QuiverRecoveryEvidence {
  crashAfterSeconds: number;
  persistenceObservedAt: string;
  killedAt: string;
  restartedAt: string;
  recoveredAt?: string;
  outageMs: number;
  beforeCrash: Record<Pool, PoolSample>;
  afterRecovery: Record<Pool, PoolSample>;
  recovered: Record<Pool, boolean>;
  passed: boolean;
}

export function selectQuiverMetrics(pool: Pool, metrics: Record<string, number>): PoolSample {
  return Object.fromEntries(Object.entries(quiverRecoveryMetrics[pool])
    .map(([name, metric]) => [name, metrics[metric] ?? 0]));
}

/** A restarted collector must replay at least one batch it acknowledged before the crash. */
export function quiverRecoveryFailures(afterRecovery: Record<Pool, PoolSample>): string[] {
  return afterRecovery.dataflow.recovered > 0 ? [] : ['Dataflow did not replay any Quiver batch after the crash'];
}

async function defaultScrape(endpoint: string): Promise<string> {
  const response = await fetch(`${endpoint}/metrics`, { signal: AbortSignal.timeout(5_000), redirect: 'error' });
  if (!response.ok) throw new Error(`Metrics HTTP ${response.status} from ${endpoint}`);
  return response.text();
}

export async function runQuiverCrash(options: QuiverCrashOptions): Promise<QuiverRecoveryEvidence> {
  const scrape = options.scrape ?? defaultScrape;
  const interval = options.pollIntervalMs ?? 1_000;
  const sample = async (): Promise<Record<Pool, PoolSample> | undefined> => {
    try {
      const dataflow = parseMetrics(await scrape(options.dataflowAdmin));
      return { dataflow: selectQuiverMetrics('dataflow', dataflow) };
    } catch {
      return undefined;
    }
  };
  const waitUntil = async (
    timeoutMs: number,
    ready: (current: Record<Pool, PoolSample>) => boolean,
    beforeKill: boolean,
  ): Promise<Record<Pool, PoolSample> | undefined> => {
    const deadline = Date.now() + timeoutMs;
    let last: Record<Pool, PoolSample> | undefined;
    for (;;) {
      options.checkRunning();
      if (beforeKill && options.loadEnded.aborted) {
        throw new Error('Load generation ended before the Quiver crash; use a --crash-after shorter than --warmup + --measure');
      }
      last = await sample() ?? last;
      if (last && ready(last)) return last;
      if (Date.now() >= deadline) return beforeKill ? undefined : last;
      await delay(interval);
    }
  };

  const persisting = await waitUntil(options.startTimeoutMs ?? 600_000,
    (current) => current.dataflow.persisted > 0, true);
  if (!persisting) throw new Error('Dataflow never persisted a batch to Quiver; cannot run the crash demo');
  const persistenceObservedAt = new Date().toISOString();
  options.log(`Quiver crash demo armed: SIGKILL in ${options.crashAfterSeconds}s`);
  const crashAt = Date.now() + options.crashAfterSeconds * 1_000;
  while (Date.now() < crashAt) {
    options.checkRunning();
    if (options.loadEnded.aborted) {
      throw new Error('Load generation ended before the Quiver crash; use a --crash-after shorter than --warmup + --measure');
    }
    await delay(Math.min(interval, Math.max(0, crashAt - Date.now())));
  }
  const beforeCrash = await sample();
  if (!beforeCrash) throw new Error('Collector metrics were unavailable immediately before the Quiver crash');
  options.log(
    `SIGKILL dataflow-1 with ${beforeCrash.dataflow.backlog} batches still on disk (not yet in Delta)`,
  );
  const killed = Date.now();
  await options.kill();
  options.log('Restarting Dataflow on its Quiver directory');
  await options.restart();
  const restarted = Date.now();
  options.log(`Dataflow healthy again after ${Math.round((restarted - killed) / 1000)}s; waiting for Quiver replay`);
  const replayed = await waitUntil(options.recoveryTimeoutMs ?? 300_000,
    (current) => quiverRecoveryFailures(current).length === 0, false);
  const afterRecovery = replayed ?? { dataflow: selectQuiverMetrics('dataflow', {}) };
  const failures = quiverRecoveryFailures(afterRecovery);
  const evidence: QuiverRecoveryEvidence = {
    crashAfterSeconds: options.crashAfterSeconds,
    persistenceObservedAt,
    killedAt: new Date(killed).toISOString(),
    restartedAt: new Date(restarted).toISOString(),
    ...(failures.length === 0 ? { recoveredAt: new Date().toISOString() } : {}),
    outageMs: restarted - killed,
    beforeCrash,
    afterRecovery,
    recovered: { dataflow: afterRecovery.dataflow.recovered > 0 },
    passed: failures.length === 0,
  };
  writeFileSync(path.join(options.results, QUIVER_RECOVERY_FILE), `${JSON.stringify(evidence, null, 2)}\n`);
  if (failures.length > 0) throw new Error(`Quiver crash recovery failed: ${failures.join('; ')}`);
  options.log(
    `Quiver replay after crash: Dataflow made ${afterRecovery.dataflow.recovered} ` +
    `replay export attempts (${afterRecovery.dataflow.reboundBatches} rebound to its new leases)`,
  );
  return evidence;
}
