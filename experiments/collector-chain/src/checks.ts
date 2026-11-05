import path from 'node:path';
import { parseArgs } from 'node:util';
import type { TableLog } from './delta.js';
import type { Row as KustoRow } from './kusto.js';

/** Middle collectors per chain in `compose.yaml`; client `i` sends to middle `i % MIDDLES + 1`. */
export const MIDDLES = 3;

export interface Options {
  readonly clients: number;
  readonly rows: number;
  readonly signals: number;
  readonly out?: string;
  readonly keep: boolean;
  readonly stream: boolean;
  readonly interval: number;
  readonly kustoPort: number;
  readonly uiPort: number;
  readonly down: boolean;
  readonly help: boolean;
}

export interface Row {
  readonly check: string;
  readonly subject: string;
  readonly expected: string;
  readonly observed: string;
  readonly ok: boolean;
}

export const HELP = `Usage: npx nx run collector-chain-experiment:run -- [flags]

  --clients <n>     Concurrent clients, spread over ${MIDDLES} middles per chain (default 3, 1-12)
  --rows <n>        Rows per DMV table per client (default 100, 1-100000)
  --signals <n>     Logs, spans and counter adds per client (default 10, 1-10000)
  --out <dir>       Report and Delta directory (default .temp/experiments/collector-chain/<runId>)
  --keep            Leave the stack running; publish Kusto and its kusto-explorer browser UI on 127.0.0.1
  --stream          --keep, and keep the same clients sending a round every --interval seconds
  --interval <s>    Seconds between streaming rounds (default 10, 1-3600)
  --kusto-port <n>  Host port for Kusto with --keep or --stream (default 8080)
  --ui-port <n>     Host port for kusto-explorer with --keep or --stream (default 8090)
  --down            Stop the clients and remove every stack that --keep or --stream left behind, then exit.
                    Run it as 'npx nx run collector-chain-experiment:down' to skip the image and cargo builds.

A new --keep or --stream run replaces an earlier one that publishes the same host ports.`;

function bounded(name: string, value: string | undefined, fallback: number, max: number): number {
  if (value === undefined) return fallback;
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 1 || number > max) throw new Error(`--${name} must be an integer from 1 to ${max}`);
  return number;
}

export function parseOptions(argv: readonly string[]): Options {
  const { values } = parseArgs({
    args: [...argv],
    strict: true,
    options: {
      clients: { type: 'string' }, rows: { type: 'string' }, signals: { type: 'string' },
      out: { type: 'string' }, keep: { type: 'boolean', default: false }, help: { type: 'boolean', default: false },
      stream: { type: 'boolean', default: false }, interval: { type: 'string' }, 'kusto-port': { type: 'string' },
      'ui-port': { type: 'string' }, down: { type: 'boolean', default: false },
    },
  });
  if (values.down && Object.entries(values).some(([name, value]) => name !== 'down' && name !== 'help' && value !== false))
    throw new Error('--down takes no other flags');
  if (values.interval !== undefined && !values.stream) throw new Error('--interval needs --stream');
  const kustoPort = bounded('kusto-port', values['kusto-port'], 8080, 65_535);
  const uiPort = bounded('ui-port', values['ui-port'], 8090, 65_535);
  if (kustoPort === uiPort) throw new Error('--kusto-port and --ui-port must differ');
  return {
    clients: bounded('clients', values.clients, 3, 12),
    rows: bounded('rows', values.rows, 100, 100_000),
    signals: bounded('signals', values.signals, 10, 10_000),
    ...(values.out ? { out: values.out } : {}),
    keep: values.keep || values.stream,
    stream: values.stream,
    interval: bounded('interval', values.interval, 10, 3600),
    kustoPort,
    uiPort,
    down: values.down,
    help: values.help,
  };
}

/** Host path of `localPath` inside a container, from `docker inspect --format '{{json .Mounts}}'`. */
export function mapHostPath(localPath: string, mounts: unknown): string {
  if (!Array.isArray(mounts)) throw new Error('Docker did not return container mounts');
  const binding = mounts
    .filter((mount): mount is { Type: string; Source: string; Destination: string } =>
      typeof mount === 'object' && mount !== null && mount.Type === 'bind' &&
      typeof mount.Source === 'string' && typeof mount.Destination === 'string')
    .filter(mount => localPath === mount.Destination || localPath.startsWith(`${mount.Destination}/`))
    .sort((left, right) => right.Destination.length - left.Destination.length)[0];
  if (!binding) throw new Error(`No host bind mount covers ${localPath}`);
  return path.posix.join(binding.Source, path.posix.relative(binding.Destination, localPath));
}

export interface Expectation {
  readonly clients: number;
  readonly dmvTables: number;
  readonly rows: number;
  readonly signals: number;
}

/** What the gateways have not committed yet, judged from Delta `numRecords` statistics. */
export function pendingFlush(arrow: ReadonlyMap<string, TableLog>, otap: ReadonlyMap<string, TableLog>, expect: Expectation): string[] {
  const pending: string[] = [];
  if (arrow.size < expect.dmvTables) pending.push(`ArrowCol has ${arrow.size}/${expect.dmvTables} DMV tables`);
  for (const [table, log] of arrow)
    if (log.rows < expect.clients * expect.rows) pending.push(`${table} has ${log.rows}/${expect.clients * expect.rows} rows`);
  const minimum: Record<string, number> = {
    logs: expect.clients * expect.signals, spans: expect.clients * expect.signals, univariate_metrics: expect.clients, resource_attrs: 1,
  };
  for (const [table, rows] of Object.entries(minimum)) {
    const committed = otap.get(table)?.rows ?? 0;
    if (committed < rows) pending.push(`otap/${table} has ${committed}/${rows} rows`);
  }
  return pending;
}

function count(row: KustoRow): number {
  if (typeof row.rows !== 'number') throw new Error(`Kusto row has no numeric count: ${JSON.stringify(row)}`);
  return row.rows;
}

/** One report row per client: every DMV table and OTAP signal must hold exactly that client's rows. */
export function clientCoverage(
  clientIds: readonly string[], dmv: ReadonlyMap<string, readonly KustoRow[]>, otap: readonly KustoRow[], expect: Expectation,
): Row[] {
  const known = new Set(clientIds);
  const strangers = [...dmv.values(), otap].flat().map(row => String(row.client)).filter(client => !known.has(client));
  const rows: Row[] = clientIds.map(clientId => {
    const tables = [...dmv.values()].filter(rows => rows.some(row => row.client === clientId && count(row) === expect.rows)).length;
    const signal = (name: string) => otap.filter(row => row.client === clientId && row.signal === name).reduce((sum, row) => sum + count(row), 0);
    const [logs, spans, metrics] = [signal('logs'), signal('spans'), signal('metrics')];
    return {
      check: 'client in Kusto',
      subject: clientId,
      expected: `${expect.dmvTables} DMV tables x ${expect.rows} rows; ${expect.signals} logs, ${expect.signals} spans, metrics`,
      observed: `${tables} DMV tables x ${expect.rows} rows; ${logs} logs, ${spans} spans, ${metrics} metric rows`,
      ok: dmv.size === expect.dmvTables && tables === expect.dmvTables && logs === expect.signals && spans === expect.signals && metrics > 0,
    };
  });
  if (strangers.length > 0)
    rows.push({ check: 'client in Kusto', subject: 'unknown clients', expected: 'none', observed: [...new Set(strangers)].join(', '), ok: false });
  return rows;
}

/** A gateway batches across clients when each table needs fewer data commits than there were clients. */
export function batching(chain: string, logs: ReadonlyMap<string, TableLog>, clients: number): Row[] {
  return [...logs].map(([table, log]) => {
    const files = log.commits.reduce((sum, commit) => sum + commit.files, 0);
    const publications = Math.max(0, ...log.commits.map(commit => commit.transactions));
    return {
      check: `${chain} gateway batching`,
      subject: table,
      expected: `< ${clients} data commits for ${clients} clients`,
      observed: `${log.commits.length} data commits, ${files} files, ${log.rows} rows` +
        (publications > 0 ? `, up to ${publications} publications per commit` : ''),
      ok: log.commits.length > 0 && log.commits.length < clients,
    };
  });
}
