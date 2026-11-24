import { createHash } from 'node:crypto';

export const COLLECTOR_TERMINAL_LOG_MAX_BYTES = 64 * 1024 * 1024;

export interface TerminalCommit {
  table_id: string;
  version: number;
  rows: number;
  files: number;
  parquet_bytes: number;
}
export interface TerminalSummary {
  schema_version: 1;
  admitted: number;
  committed: number;
  failed: number;
  cancelled_ambiguous: number;
  pending: number;
  committed_rows: number;
  incomplete: 0 | 1;
  commits: TerminalCommit[];
}
export interface BoundTerminalSummary {
  pod_name: string;
  pod_uid: string;
  container: string;
  restart_count: 0;
  exit_code: 0;
  summary: TerminalSummary;
}
const exact = (value: unknown, keys: string[]): value is Record<string, unknown> =>
  !!value && typeof value === 'object' && !Array.isArray(value) &&
  Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const count = (value: unknown): value is number => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
export function parseTerminalSummary(value: unknown): TerminalSummary {
  const keys = ['schema_version', 'admitted', 'committed', 'failed', 'cancelled_ambiguous', 'pending',
    'committed_rows', 'incomplete', 'commits'];
  if (!exact(value, keys) || value.schema_version !== 1 ||
      !keys.slice(1, 7).every(key => count(value[key])) || ![0, 1].includes(Number(value.incomplete)) ||
      typeof value.incomplete !== 'number' || !Array.isArray(value.commits) || value.commits.length > 20_000)
    throw new Error('Terminal summary schema/counts are unavailable.');
  for (const commit of value.commits) {
    if (!exact(commit, ['table_id', 'version', 'rows', 'files', 'parquet_bytes']) ||
        typeof commit.table_id !== 'string' || !/^[a-f0-9]{64}$/.test(commit.table_id) ||
        !['version', 'rows', 'files', 'parquet_bytes'].every(key => count(commit[key])))
      throw new Error('Terminal summary commit tuple is unavailable.');
  }
  return JSON.parse(JSON.stringify(value)) as TerminalSummary;
}
export function terminalSummaryClear(summary: TerminalSummary): boolean {
  const accounted = summary.committed + summary.failed + summary.cancelled_ambiguous + summary.pending;
  const rows = summary.commits.reduce((sum, commit) => sum + commit.rows, 0);
  return summary.incomplete === 0 && summary.failed === 0 && summary.cancelled_ambiguous === 0 && summary.pending === 0 &&
    Number.isSafeInteger(accounted) && accounted === summary.admitted &&
    Number.isSafeInteger(rows) && rows === summary.committed_rows &&
    new Set(summary.commits.map(commit => `${commit.table_id}:${commit.version}`)).size === summary.commits.length;
}
export function extractTerminalSummary(logs: string): { summary: TerminalSummary | null; status: 0 | 1 | 2 | 3 } {
  const lines = logs.split('\n').filter(line => line.includes('arrowcol_terminal_evidence'));
  if (lines.length === 0) return { summary: null, status: 1 };
  if (lines.length !== 1) return { summary: null, status: 2 };
  const match = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z arrowcol_terminal_evidence (\{.*\})$/.exec(lines[0]!);
  try {
    if (!match) throw new Error('Terminal summary log envelope is unavailable.');
    return { summary: parseTerminalSummary(JSON.parse(match[1]!)), status: 0 };
  } catch { return { summary: null, status: 3 }; }
}
export function collectorStartupObserved(logs: string): boolean {
  const lines = logs.split('\n'), terminal = lines.findIndex(line => line.includes('arrowcol_terminal_evidence'));
  if (terminal < 0 || !logs.endsWith('\n') || logs.includes('\uFFFD')) return false;
  return lines.slice(0, terminal).some(line => {
    const match = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z arrowcol_effective_config (\{.*\})$/.exec(line);
    try {
      const value: unknown = match ? JSON.parse(match[1]!) : null;
      return !!value && typeof value === 'object' && !Array.isArray(value) &&
        (value as Record<string, unknown>).component === 'receiver';
    } catch { return false; }
  });
}
export function terminalTableDigest(account: string, container: string, root: string): string {
  if (!/^[a-z0-9]+$/.test(account) || !/^[a-z0-9-]+$/.test(container) ||
      !/^[A-Za-z0-9_./-]+$/.test(root) || root.split('/').some(part => !part || part === '.' || part === '..'))
    throw new Error('Canonical configured terminal table URL is unavailable.');
  // Only the configured ABFSS binding is supported; other schemes are not equivalent hashes.
  return createHash('sha256').update(`abfss://${container}@${account}.dfs.core.windows.net/${root}/`).digest('hex');
}
