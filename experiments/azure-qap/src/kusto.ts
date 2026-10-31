import { randomUUID } from 'node:crypto';
import { HttpFailure, Failure, type Azure } from '../../../infra/azure/bootstrap.ts';
import type { ExpectedGroup } from './fixtures.js';

type Row = Record<string, unknown>;
export interface Table { kind: string; columns: string[]; rows: unknown[][] }
export interface AccelerationSample {
  at: string;
  version?: number;
  completionPercent?: number;
  pendingFiles?: number;
  pendingBytes?: number;
  acceleratedRows?: number;
  healthy?: boolean;
  lastIngestionAt?: string;
}
export interface Target { version: number; rows: number }
export interface ExtentRecord { partition: string; extent: string; rows: number }
export const UNACCELERATED_EXTENT = '00000000-0000-0000-0000-000000000000';

export function object(value: unknown): Row {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Expected an object response.');
  return value as Row;
}

/** Data tables from a v1 management response or a v2 query frame list; any error frame throws. */
export function tables(value: unknown): Table[] {
  const frames: unknown[] = Array.isArray(value) ? value : (() => {
    const response = object(value);
    if (response.error || response.HasErrors ||
        (Array.isArray(response.Exceptions) ? response.Exceptions.length > 0 : response.Exceptions))
      throw new Error('Kusto returned an error.');
    if (!Array.isArray(response.Tables)) throw new Error('Kusto response has no tables.');
    return response.Tables;
  })();
  const result: Table[] = [];
  for (const raw of frames) {
    const frame = object(raw);
    if (frame.HasErrors || frame.Cancelled || frame.Error ||
        (Array.isArray(frame.OneApiErrors) ? frame.OneApiErrors.length > 0 : frame.OneApiErrors))
      throw new Error('Kusto returned a partial, cancelled or failed result.');
    if (frame.FrameType && frame.FrameType !== 'DataTable') continue;
    if (!Array.isArray(frame.Columns) || !Array.isArray(frame.Rows)) throw new Error('Malformed Kusto data table.');
    const columns = frame.Columns.map(column => {
      const name = object(column).ColumnName;
      if (typeof name !== 'string') throw new Error('Kusto column name is absent.');
      return name;
    });
    const rows = frame.Rows.map(row => {
      if (!Array.isArray(row) || row.length !== columns.length) throw new Error('Malformed Kusto result row.');
      return row;
    });
    const table = { kind: typeof frame.TableKind === 'string' ? frame.TableKind : 'PrimaryResult', columns, rows };
    if (table.kind === 'QueryCompletionInformation') {
      const code = columns.indexOf('StatusCode');
      if (code >= 0 && rows.some(row => row[code] !== 0)) throw new Error('Kusto query completed with an error status.');
    }
    result.push(table);
  }
  return result;
}

export function tableRows(table: Table): Row[] {
  return table.rows.map(row => Object.fromEntries(table.columns.map((name, index) => [name, row[index]])));
}

export function primary(value: unknown): Table {
  const table = tables(value).find(table => table.kind === 'PrimaryResult');
  if (!table) throw new Error('Kusto primary result is absent.');
  return table;
}

function numeric(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

function dynamic(value: unknown): Row {
  return object(typeof value === 'string' ? JSON.parse(value) : value);
}

export function parseSample(details: unknown, statistics: unknown, at: string): AccelerationSample {
  const detail = tableRows(primary(details));
  const stats = tableRows(primary(statistics));
  if (detail.length !== 1 || stats.length !== 1) throw new Error('QAP returned missing or duplicate table state.');
  const state = dynamic(detail[0]!.QueryAccelerationState);
  const row = stats[0]!;
  return {
    at,
    version: numeric(state.TableVersion),
    completionPercent: numeric(row.AccelerationCompletePercentage ?? state.CompletionPercentage),
    pendingFiles: numeric(row.AccelerationPendingDataFilesCount ?? state.PendingArtifactCount),
    pendingBytes: numeric(row.AccelerationPendingDataFilesSize ?? state.PendingArtifactSize),
    acceleratedRows: numeric(state.AcceleratedRowCount),
    healthy: typeof state.IsHealthy === 'boolean' ? state.IsHealthy : undefined,
    lastIngestionAt: typeof state.LastIngestionDateTime === 'string' ? state.LastIngestionDateTime : undefined,
  };
}

/** Caught up means QAP accelerated exactly this Delta version and every one of its rows. */
export function ready(sample: AccelerationSample, target: Target): boolean {
  return sample.version === target.version && sample.completionPercent === 100 &&
    sample.pendingFiles === 0 && sample.healthy === true && sample.acceleratedRows === target.rows;
}

export function describeSample(sample: AccelerationSample, target: Target): string {
  // A freshly created table reports TableVersion -1 (and -1 counters) until QAP's first pass starts.
  if (sample.version === -1)
    return `QAP has not started yet (TableVersion -1); target Delta v${target.version}, ${target.rows} rows`;
  const show = (value: number | undefined) => value === undefined || value < 0 ? '?' : String(value);
  return `Delta v${show(sample.version)}/${target.version}, ${show(sample.completionPercent)}% accelerated, ` +
    `${show(sample.pendingFiles)} files pending, ${show(sample.acceleratedRows)}/${target.rows} rows, ` +
    (sample.healthy === undefined ? 'health unknown' : sample.healthy ? 'healthy' : 'UNHEALTHY');
}

export function identifier(name: string): string {
  return `['${checked(name)}']`;
}

function checked(name: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(name)) throw new Error(`Invalid Kusto table or column name: ${name}`);
  return name;
}

/** The exact KQL the demo runs; printed so readers can paste it into the Kusto web UI. */
export const kql = {
  create: (table: string, source: string) => `.create-or-alter external table ${identifier(table)} kind=delta (h@'${source}')`,
  policy: (table: string, principal: string) => `.alter external table ${identifier(table)} policy query_acceleration '${
    JSON.stringify({ IsEnabled: true, Hot: '7.00:00:00', MaxAgeOverride: '00:01:00', ManagedIdentity: principal })}'`,
  details: (table: string) => `.show external table ${identifier(table)} details`,
  statistics: (table: string) => `.show external table ${identifier(table)} operations query_acceleration statistics`,
  drop: (table: string) => `.drop external table ${identifier(table)} ifexists`,
  capacity: () => '.show capacity',
  extents: (table: string) => `external_table("${checked(table)}")
| summarize Rows=count() by Partition=__arrowcol_ingestion, Extent=tostring(extent_id())
| order by Partition asc, Extent asc`,
  parity: (table: string, groups: number) => `external_table("${checked(table)}")
| summarize multiplicity=count() by partition=__arrowcol_ingestion, id, value, payloadSha256=hash_sha256(payload)
| take ${groups + 1}
| project partition, id, value, payloadSha256, multiplicity`,
  marker: (table: string) => `external_table("${checked(table)}", timespan(1s)) | count`,
};

export function parseExtents(records: unknown[][]): ExtentRecord[] {
  const seen = new Set<string>();
  return records.map(row => {
    const [partition, extent, count] = row;
    if (row.length !== 3 || typeof partition !== 'string' || typeof extent !== 'string' ||
        !/^[0-9a-f-]{36}$/i.test(extent) || typeof count !== 'number' ||
        !Number.isSafeInteger(count) || count <= 0 || seen.has(`${partition}:${extent}`))
      throw new Error('Extent inventory returned malformed or duplicate rows.');
    seen.add(`${partition}:${extent}`);
    return { partition, extent: extent.toLowerCase(), rows: count };
  });
}

/** Rows of untouched partitions that still sit in the very same accelerated extent after the mutation. */
export function retainedExtentRows(before: ExtentRecord[], after: ExtentRecord[], partitions: Set<string>): { before: number; kept: number } {
  const old = before.filter(row => partitions.has(row.partition));
  const next = new Map(after.map(row => [`${row.partition}:${row.extent}`, row.rows]));
  return { before: old.reduce((total, row) => total + row.rows, 0),
    kept: old.reduce((total, row) => total +
      (row.extent !== UNACCELERATED_EXTENT && next.get(`${row.partition}:${row.extent}`) === row.rows ? row.rows : 0), 0) };
}

export function unacceleratedRows(extents: ExtentRecord[]): number {
  return extents.filter(row => row.extent === UNACCELERATED_EXTENT).reduce((total, row) => total + row.rows, 0);
}

export function groupKey(group: Omit<ExpectedGroup, 'multiplicity'>): string {
  return JSON.stringify([group.partition, group.id, group.value, group.payloadSha256]);
}

/** Compare Kusto's `summarize multiplicity=count() by partition, id, value, payloadSha256` rows with the oracle. */
export function compareGroups(expected: ExpectedGroup[], records: unknown[][]): void {
  const wanted = new Map(expected.map(group => [groupKey(group), group.multiplicity]));
  if (records.length !== expected.length)
    throw new Error(`Kusto returned ${records.length} row groups; the Delta oracle has ${expected.length}.`);
  for (const row of records) {
    const [partition, id, value, digest, multiplicity] = row;
    if (row.length !== 5 || typeof partition !== 'string' || typeof id !== 'number' || typeof value !== 'number' ||
        typeof digest !== 'string' || typeof multiplicity !== 'number')
      throw new Error('Kusto returned a malformed parity row.');
    const key = groupKey({ partition, id, value, payloadSha256: digest.toLowerCase() });
    if (wanted.get(key) !== multiplicity)
      throw new Error(`Kusto group ${key} has multiplicity ${multiplicity}; the oracle expects ${wanted.get(key) ?? 0}.`);
    wanted.delete(key);
  }
  if (wanted.size) throw new Error('Kusto omitted row groups the Delta oracle expects.');
}

export function transientKusto(error: unknown): boolean {
  if (error instanceof HttpFailure) return [408, 429, 500, 502, 503, 504].includes(error.code);
  return error instanceof Failure;
}

export class ParityFailure extends Error {}

export type KustoClient = Pick<Azure, 'request'> & { c: { uri: string } };
export type Sleep = (ms: number, signal: AbortSignal) => Promise<void>;
const defaultSleep: Sleep = (ms, signal) => new Promise((resolve, reject) => {
  const timer = setTimeout(resolve, ms);
  signal.addEventListener('abort', () => { clearTimeout(timer); reject(signal.reason); }, { once: true });
});

export class Kusto {
  constructor(readonly azure: KustoClient, readonly database: string, readonly runId: string,
    readonly sleep: Sleep = defaultSleep, readonly attempts = 3) {}

  async management(command: string, signal: AbortSignal): Promise<unknown> {
    const response = await this.post('/v1/rest/mgmt', { db: this.database, csl: command }, signal);
    tables(response);
    return response;
  }

  async query(csl: string, signal: AbortSignal): Promise<unknown[][]> {
    const response = await this.post('/v2/rest/query', { db: this.database, csl,
      properties: JSON.stringify({ Options: { query_results_cache_max_age: '00:00:00' } }) }, signal);
    return primary(response).rows;
  }

  async capacity(signal: AbortSignal): Promise<Row | undefined> {
    return tableRows(primary(await this.management(kql.capacity(), signal))).find(row => row.Resource === 'QueryAcceleration');
  }

  /** Create the Delta external table and enable QAP; retries 403 while new role assignments propagate. */
  async create(table: string, source: string, principal: string, signal: AbortSignal, deadline: number): Promise<void> {
    await this.untilAuthorized(() => this.management(kql.create(table, source), signal), signal, deadline);
    await this.untilAuthorized(() => this.management(kql.policy(table, principal), signal), signal, deadline);
  }

  async drop(table: string, signal: AbortSignal): Promise<void> {
    await this.management(kql.drop(table), signal);
  }

  async sample(table: string, signal: AbortSignal): Promise<AccelerationSample> {
    const [details, statistics] = await Promise.all([
      this.management(kql.details(table), signal), this.management(kql.statistics(table), signal)]);
    return parseSample(details, statistics, new Date().toISOString());
  }

  async extents(table: string, signal: AbortSignal): Promise<ExtentRecord[]> {
    return parseExtents(await this.query(kql.extents(table), signal));
  }

  async parity(table: string, expected: ExpectedGroup[], signal: AbortSignal): Promise<void> {
    const records = await this.query(kql.parity(table, expected.length), signal);
    try { compareGroups(expected, records); }
    catch (error) { throw new ParityFailure(`${table}: ${error instanceof Error ? error.message : String(error)}`); }
  }

  async marker(table: string, signal: AbortSignal): Promise<number> {
    const [row] = await this.query(kql.marker(table), signal);
    if (!row || typeof row[0] !== 'number') throw new Error('Freshness marker query returned no count.');
    return row[0];
  }

  private async untilAuthorized(action: () => Promise<unknown>, signal: AbortSignal, deadline: number): Promise<void> {
    for (;;) {
      try { await action(); return; }
      catch (error) {
        if (!(error instanceof HttpFailure) || error.code !== 403 || Date.now() + 5000 >= deadline) throw error;
        await this.sleep(5000, signal);
      }
    }
  }

  private async post(path: string, body: Row, signal: AbortSignal): Promise<unknown> {
    for (let attempt = 1; ; attempt++) {
      signal.throwIfAborted();
      try {
        const [data] = await this.azure.request('POST', this.azure.c.uri + path, 'https://kusto.kusto.windows.net',
          JSON.stringify(body), { 'Content-Type': 'application/json', Accept: 'application/json',
            'x-ms-client-request-id': `ArrowColQapDemo;${this.runId};${randomUUID()}` });
        if (!data) throw new Error('Empty Kusto response.');
        return JSON.parse(data.toString()) as unknown;
      } catch (error) {
        if (attempt >= this.attempts || !transientKusto(error)) throw error;
        await this.sleep(2000 * attempt, signal);
      }
    }
  }
}
