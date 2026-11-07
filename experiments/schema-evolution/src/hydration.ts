import { setTimeout as sleep } from 'node:timers/promises';
import { identifier, type Kusto } from '../../azure-qap/src/kusto.ts';

export interface Counts {
  client: 'old' | 'new';
  phase: 'before' | 'after';
  total: number;
  empty: number;
  filled: number;
}
export interface Observation {
  elapsedMs: number;
  counts?: Counts[];
  error?: string;
}
export interface PollOptions {
  deadline: number;
  signal: AbortSignal;
  rowsPerPhase: number;
  record: (observation: Observation) => void;
  intervalMs?: number;
  now?: () => number;
  wait?: (ms: number, signal: AbortSignal) => Promise<void>;
}

export function countQuery(table: string): string {
  identifier(table);
  return `external_table('${table}')\n` +
    '| summarize total=count(), empty=countif(isempty(bar)), filled=countif(isnotempty(bar)) by client, phase\n' +
    '| order by client asc, phase asc';
}

export function requestedNullQuery(table: string): string {
  identifier(table);
  return `external_table('${table}')\n` +
    '| summarize total=count(), nulls=countif(isnull(bar)), filled=countif(isnotnull(bar)) by client';
}

export function parseCounts(rows: unknown[][]): Counts[] {
  const seen = new Set<string>();
  return rows.map(row => {
    const [client, phase, total, empty, filled] = row;
    if (row.length !== 5 || (client !== 'old' && client !== 'new') ||
        (phase !== 'before' && phase !== 'after') || seen.has(`${client}:${phase}`) ||
        [total, empty, filled].some(value => typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) ||
        typeof total !== 'number' || typeof empty !== 'number' || typeof filled !== 'number' || total !== empty + filled)
      throw new Error('Malformed, duplicate or inconsistent schema evolution counts.');
    seen.add(`${client}:${phase}`);
    return { client, phase, total, empty, filled };
  });
}

export function complete(counts: Counts[], rows: number): boolean {
  return counts.length === 3 &&
    counts.some(c => c.client === 'old' && c.phase === 'before' && c.total === rows && c.empty === rows && c.filled === 0) &&
    counts.some(c => c.client === 'old' && c.phase === 'after' && c.total === rows && c.empty === rows && c.filled === 0) &&
    counts.some(c => c.client === 'new' && c.phase === 'after' && c.total === rows && c.empty === 0 && c.filled === rows);
}

function schemaNotVisible(error: unknown): error is Error {
  return error instanceof Error && /(?:Failed to resolve|does not refer to any known).*\bbar\b/i.test(error.message);
}

export async function observeHydration(kusto: Pick<Kusto, 'query'>, table: string, options: PollOptions): Promise<Counts[]> {
  const now = options.now ?? Date.now;
  const wait = options.wait ?? ((ms, signal) => sleep(ms, undefined, { signal }));
  const interval = options.intervalMs ?? 5000;
  if (!Number.isSafeInteger(options.rowsPerPhase) || options.rowsPerPhase < 1 ||
      !Number.isFinite(options.deadline) || !Number.isFinite(interval) || interval < 1)
    throw new Error('Hydration requires positive row and polling bounds and a finite deadline.');
  const start = now();
  let last: Observation | undefined;
  while (now() < options.deadline) {
    options.signal.throwIfAborted();
    try {
      const counts = parseCounts(await kusto.query(countQuery(table), options.signal));
      options.signal.throwIfAborted();
      last = { elapsedMs: now() - start, counts };
      options.record(last);
      if (now() < options.deadline && complete(counts, options.rowsPerPhase)) return counts;
    } catch (error) {
      if (!schemaNotVisible(error)) throw error;
      last = { elapsedMs: now() - start, error: error.message };
      options.record(last);
    }
    const remaining = options.deadline - now();
    if (remaining > 0) await wait(Math.min(interval, remaining), options.signal);
  }
  throw new Error(`Native Kusto schema/data hydration deadline exceeded; last observation: ${JSON.stringify(last ?? null)}`);
}
