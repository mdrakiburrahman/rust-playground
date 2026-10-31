import { HttpFailure, Failure, type Azure } from '../../../infra/azure/bootstrap.ts';
import { object, type Sleep } from './kusto.js';

export type Phase = 'warm-up' | 'after-mutation' | 'late';
/** One summarized StorageBlobLogs group: requests by the Kusto identity for one file, operation and status. */
export interface ReadGroup {
  phase: Phase;
  path: string;
  operation: string;
  status: number;
  requests: number;
  bytes: number;
  last: string;
}
export interface ReadScope {
  workspace: string;
  account: string;
  container: string;
  requester: string;
  prefix: string;
}
export interface ReadWindow { from: number; mutationAt: number; windowEnd: number }
export interface ReadSummary { files: number; bytes: number; requests: number; paths: string[] }
export type LogsQuery = (query: string, signal: AbortSignal) => Promise<unknown>;

/** Late events must be this far past the watch window before an absence of window reads is trusted. */
export const LATE_MARGIN_MS = 60_000;

export function logsQuery(azure: Pick<Azure, 'request'>, customerId: string): LogsQuery {
  if (!/^[0-9a-f-]{36}$/i.test(customerId)) throw new Error('Invalid Log Analytics workspace customer ID.');
  return async (query, signal) => {
    signal.throwIfAborted();
    const [data] = await azure.request('POST', `https://api.loganalytics.azure.com/v1/workspaces/${customerId}/query`,
      'https://api.loganalytics.io', JSON.stringify({ query }), { 'Content-Type': 'application/json' });
    if (!data) throw new Error('Log Analytics returned an empty response.');
    return JSON.parse(data.toString()) as unknown;
  };
}

function quote(value: string): string {
  return JSON.stringify(value);
}

function iso(ms: number): string {
  return new Date(ms).toISOString();
}

export function validateScope(scope: ReadScope): void {
  if (!/^[a-z0-9]{3,24}$/.test(scope.account) || !/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(scope.container) ||
      !/^[0-9a-f-]{36}$/i.test(scope.requester) || !/^runs\/[a-z0-9_]+\/[a-z0-9_]+\/$/.test(scope.prefix))
    throw new Error('Invalid storage read scope.');
}

/** The Log Analytics query the demo runs (printed verbatim so it can be pasted into the portal). */
export function readsQuery(scope: ReadScope, window: ReadWindow): string {
  validateScope(scope);
  return `StorageBlobLogs
| where TimeGenerated >= datetime(${iso(window.from)})
| where AccountName == ${quote(scope.account)} and Category == "StorageRead"
| where RequesterObjectId =~ ${quote(scope.requester)}
| where Uri contains ${quote(`/${scope.container}/${scope.prefix}`)}
| distinct TimeGenerated, CorrelationId, OperationName, Uri, StatusCode, ResponseBodySize
| extend Phase = case(TimeGenerated < datetime(${iso(window.mutationAt)}), "warm-up",
                      TimeGenerated < datetime(${iso(window.windowEnd)}), "after-mutation", "late"),
         Path = tostring(split(Uri, "?")[0])
| summarize Requests = count(), Bytes = sum(ResponseBodySize), Last = max(TimeGenerated) by Phase, Path, OperationName, StatusCode
| order by Phase asc, Path asc`;
}

/** A shorter raw-event query for readers who want to eyeball individual requests. */
export function rawQuery(scope: ReadScope, fromMs: number): string {
  validateScope(scope);
  return `StorageBlobLogs
| where TimeGenerated >= datetime(${iso(fromMs)})
| where RequesterObjectId =~ ${quote(scope.requester)} and Uri contains ${quote(`/${scope.container}/${scope.prefix}`)}
| project TimeGenerated, OperationName, StatusCode, ResponseBodySize, Uri
| order by TimeGenerated asc`;
}

export function parseReads(response: unknown, scope: ReadScope): ReadGroup[] {
  const body = object(response);
  if (body.error || body.partialError || !Array.isArray(body.tables) || body.tables.length !== 1)
    throw new Error('Log Analytics returned a failed, partial or ambiguous result.');
  const table = object(body.tables[0]);
  if (!Array.isArray(table.columns) || !Array.isArray(table.rows)) throw new Error('Log Analytics returned a malformed table.');
  const columns = table.columns.map(column => object(column).name);
  const at = (name: string) => {
    const index = columns.indexOf(name);
    if (index < 0) throw new Error(`Log Analytics result has no ${name} column.`);
    return index;
  };
  const index = { phase: at('Phase'), path: at('Path'), operation: at('OperationName'), status: at('StatusCode'),
    requests: at('Requests'), bytes: at('Bytes'), last: at('Last') };
  const hosts = [`${scope.account}.blob.core.windows.net`, `${scope.account}.dfs.core.windows.net`];
  const root = `/${scope.container}/${scope.prefix}`;
  const groups: ReadGroup[] = [];
  for (const raw of table.rows) {
    if (!Array.isArray(raw) || raw.length !== columns.length) throw new Error('Malformed storage log row.');
    const phase = raw[index.phase];
    const url = new URL(String(raw[index.path]));
    const pathname = decodeURIComponent(url.pathname);
    // Listing calls carry the prefix in their query string; only per-file requests are evidence.
    if (!hosts.includes(url.hostname) || !pathname.startsWith(root)) continue;
    const requests = raw[index.requests];
    const bytes = raw[index.bytes] ?? 0;
    const status = Number(raw[index.status]);
    if ((phase !== 'warm-up' && phase !== 'after-mutation' && phase !== 'late') || typeof raw[index.operation] !== 'string' ||
        !Number.isInteger(status) || typeof requests !== 'number' || typeof bytes !== 'number' || bytes < 0 ||
        typeof raw[index.last] !== 'string')
      throw new Error('Storage log group is malformed.');
    groups.push({ phase, path: pathname.slice(root.length), operation: raw[index.operation] as string, status, requests, bytes,
      last: raw[index.last] as string });
  }
  return groups;
}

/** Successful whole-or-ranged reads of Parquet data files (not metadata probes or `_delta_log` reads). */
export function bodyReads(groups: ReadGroup[], phase: Phase, paths?: Set<string>): ReadSummary {
  const reads = groups.filter(group => group.phase === phase && /^(GetBlob|ReadFile)$/.test(group.operation) &&
    group.status >= 200 && group.status < 300 && group.bytes > 0 && group.path.endsWith('.parquet') &&
    (!paths || paths.has(group.path)));
  return { files: new Set(reads.map(group => group.path)).size, bytes: reads.reduce((sum, group) => sum + group.bytes, 0),
    requests: reads.reduce((sum, group) => sum + group.requests, 0), paths: [...new Set(reads.map(group => group.path))].sort() };
}

export function transientLogFailure(error: unknown): boolean {
  if (error instanceof HttpFailure)
    return error.code === 408 || error.code === 429 || error.code >= 500 ||
      (error.code === 400 && error.message.includes('StorageBlobLogs') && error.message.includes('Failed to resolve'));
  return error instanceof Failure || (error instanceof Error && /Log Analytics returned (?:a failed|an empty)/.test(error.message));
}

export type LogStatus = 'complete' | 'pending';
export interface LogWait {
  status: LogStatus;
  groups: ReadGroup[];
  polls: number;
  lastError?: string;
}
export interface LogWaitOptions {
  query: LogsQuery;
  scope: ReadScope;
  window: ReadWindow;
  deadline: number;
  pollMs: number;
  signal: AbortSignal;
  now?: () => number;
  sleep?: Sleep;
  /** Called once after the late margin passes, to provoke a fresh Delta-log read by Kusto. */
  nudge?: () => Promise<void>;
  onPoll?: (groups: ReadGroup[], status: string) => void;
}

/**
 * StorageBlobLogs arrive minutes late and out of order. Logs count as complete only when the Kusto identity's own
 * later traffic (Delta-log polling) has arrived well past the watch window and the in-window picture stopped changing.
 */
export async function waitForLogs(options: LogWaitOptions): Promise<LogWait> {
  const now = options.now ?? Date.now;
  const sleep = options.sleep ?? ((ms: number, signal: AbortSignal) => new Promise<void>((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener('abort', () => { clearTimeout(timer); reject(signal.reason); }, { once: true });
  }));
  const text = readsQuery(options.scope, options.window);
  const lateAfter = options.window.windowEnd + LATE_MARGIN_MS;
  let previous: string | undefined;
  let groups: ReadGroup[] = [];
  let lastError: string | undefined;
  let nudged = false;
  for (let polls = 1; ; polls++) {
    options.signal.throwIfAborted();
    if (!nudged && options.nudge && now() >= lateAfter) {
      nudged = true;
      await options.nudge().catch(() => undefined);
    }
    try {
      groups = parseReads(await options.query(text, options.signal), options.scope);
      lastError = undefined;
      const window = JSON.stringify(groups.filter(group => group.phase !== 'late')
        .map(group => [group.phase, group.path, group.operation, group.status, group.requests, group.bytes]).sort());
      const caughtUp = groups.some(group => group.phase === 'late' && Date.parse(group.last) >= lateAfter);
      const stable = window === previous;
      options.onPoll?.(groups, caughtUp ? (stable ? 'complete' : 'caught up, confirming nothing else arrives')
        : 'waiting for logs past the window');
      if (caughtUp && stable) return { status: 'complete', groups, polls };
      previous = caughtUp ? window : undefined;
    } catch (error) {
      if (!transientLogFailure(error)) throw error;
      lastError = error instanceof Error ? error.message : String(error);
      options.onPoll?.(groups, `transient Log Analytics failure, retrying: ${lastError}`);
    }
    if (now() + options.pollMs > options.deadline) return { status: 'pending', groups, polls, lastError };
    await sleep(options.pollMs, options.signal);
  }
}
