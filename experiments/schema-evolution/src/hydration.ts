import { setTimeout as sleep } from 'node:timers/promises';
import { identifier, type Kusto } from '../../azure-qap/src/kusto.ts';

export interface Counts {
  client: 'old' | 'new';
  phase: 'before' | 'after';
  total: number;
  empty: number;
  filled: number;
  sentinelMismatches: number;
}
export interface Observation {
  elapsedMs: number;
  columns: string[];
  counts?: Counts[];
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
    '| summarize total=count(), empty=countif(isempty(bar)), filled=countif(isnotempty(bar)),\n' +
    "  sentinelMismatches=countif(client == 'new' and bar != strcat('bar-', tostring(value))) by client, phase\n" +
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
    const [client, phase, total, empty, filled, sentinelMismatches] = row;
    if (row.length !== 6 || (client !== 'old' && client !== 'new') ||
        (phase !== 'before' && phase !== 'after') || seen.has(`${client}:${phase}`) ||
        [total, empty, filled, sentinelMismatches].some(value => typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) ||
        typeof total !== 'number' || typeof empty !== 'number' || typeof filled !== 'number' ||
        typeof sentinelMismatches !== 'number' || sentinelMismatches > total || total !== empty + filled)
      throw new Error('Malformed, duplicate or inconsistent schema evolution counts.');
    seen.add(`${client}:${phase}`);
    return { client, phase, total, empty, filled, sentinelMismatches };
  });
}

export function complete(counts: Counts[], rows: number): boolean {
  return counts.length === 3 && counts.every(c => c.sentinelMismatches === 0) &&
    counts.some(c => c.client === 'old' && c.phase === 'before' && c.total === rows && c.empty === rows && c.filled === 0) &&
    counts.some(c => c.client === 'old' && c.phase === 'after' && c.total === rows && c.empty === rows && c.filled === 0) &&
    counts.some(c => c.client === 'new' && c.phase === 'after' && c.total === rows && c.empty === 0 && c.filled === rows);
}

export function schemaQuery(table: string): string {
  identifier(table);
  return `external_table('${table}') | getschema | project ColumnName`;
}

export function exactRowsQuery(table: string): string {
  identifier(table);
  return `external_table('${table}') | summarize multiplicity=count() by client, phase, value, bar`;
}

export function verifyExactRows(records: unknown[][], rows: number): void {
  if (!Number.isSafeInteger(rows) || rows < 1 || records.length !== rows * 3)
    throw new Error('Kusto exact fixture row groups are incomplete.');
  const seen = new Set<string>();
  for (const record of records) {
    const [client, phase, value, bar, multiplicity] = record;
    const key = `${client}:${phase}:${value}`;
    if (record.length !== 5 || (client !== 'old' && client !== 'new') ||
        (phase !== 'before' && phase !== 'after') || (client === 'new' && phase !== 'after') ||
        typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0 || value >= rows ||
        multiplicity !== 1 || bar !== (client === 'old' ? '' : `bar-${value}`) || seen.has(key))
      throw new Error('Kusto exact old/new rows contain wrong values, duplicates or unexpected sentinels.');
    seen.add(key);
  }
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
    const schema = await kusto.query(schemaQuery(table), options.signal);
    if (schema.some(row => row.length !== 1 || typeof row[0] !== 'string') ||
        new Set(schema.map(row => row[0])).size !== schema.length)
      throw new Error('Malformed or duplicate Kusto schema columns.');
    const columns = schema.map(row => String(row[0]));
    if (columns.includes('bar')) {
      const counts = parseCounts(await kusto.query(countQuery(table), options.signal));
      options.signal.throwIfAborted();
      last = { elapsedMs: now() - start, columns, counts };
      options.record(last);
      if (now() < options.deadline && complete(counts, options.rowsPerPhase)) return counts;
    } else {
      options.signal.throwIfAborted();
      last = { elapsedMs: now() - start, columns };
      options.record(last);
    }
    const remaining = options.deadline - now();
    if (remaining > 0) await wait(Math.min(interval, remaining), options.signal);
  }
  throw new Error(`Native Kusto schema/data hydration deadline exceeded; last observation: ${JSON.stringify(last ?? null)}`);
}
