import { describeSample, ready, type Kusto, type Target } from './kusto.js';
import type { ExpectedGroup } from './fixtures.js';
import type { Clock } from './scenario.js';

export interface InspectionTarget extends Target {
  table: string;
  expectedGroups: ExpectedGroup[];
}

export async function observeInspection(input: {
  targets: readonly InspectionTarget[];
  kusto: Pick<Kusto, 'sample' | 'parity'>;
  clock: Clock;
  deadline: number;
  signal: AbortSignal;
  onReady: () => Promise<void>;
  record: (table: string, kind: string, data: unknown) => void;
  say: (text: string) => void;
}): Promise<void> {
  if (!input.targets.length) throw new Error('Inspection requires retained run tables.');
  let first = true;
  while (input.clock.now() < input.deadline && !input.signal.aborted) {
    try {
      const signal = AbortSignal.any([input.signal,
        AbortSignal.timeout(Math.max(1, Math.min(30_000, input.deadline - input.clock.now())))]);
      for (const target of input.targets) {
        signal.throwIfAborted();
        const sample = await input.kusto.sample(target.table, signal);
        input.record(target.table, 'inspection-health', sample);
        if (!ready(sample, target)) throw new Error(`${target.table}: ${describeSample(sample, target)}`);
        await input.kusto.parity(target.table, target.expectedGroups, signal);
        input.record(target.table, 'inspection-parity', { status: 'ok' });
        input.say(`READ-ONLY INSPECTION ${target.table}: ${describeSample(sample, target)}; row parity OK.`);
      }
      signal.throwIfAborted();
      if (first) { await input.onReady(); first = false; }
      const delay = Math.min(30_000, input.deadline - input.clock.now());
      if (delay > 0) await input.clock.sleep(delay, input.signal);
    } catch (error) {
      if (input.signal.aborted || input.clock.now() >= input.deadline) return;
      input.record('run', 'inspection-failed', { error: error instanceof Error ? error.message : String(error) });
      input.say(`READ-ONLY INSPECTION FAILED: ${error instanceof Error ? error.message : String(error)}; stopping for cleanup.`);
      throw error;
    }
  }
}
