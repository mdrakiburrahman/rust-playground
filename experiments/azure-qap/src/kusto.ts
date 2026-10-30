import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { HttpFailure, type Azure } from '../../../infra/azure/bootstrap.ts';
import type { AccelerationSample, DiagnosticCapability, Snapshot } from './evidence.js';

type Row = Record<string, unknown>;
export type Emit = (type: string, value: unknown) => void;
export interface Table { kind: string; columns: string[]; rows: unknown[][] }
export interface QueryResult {
  records: unknown[][];
  requestId: string;
  queryCpuMs?: number;
  remoteDownloadedItems?: number;
  scannedExtents?: number;
}

export function object(value: unknown): Row {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Expected an object response.');
  return value as Row;
}

export function tables(value: unknown): Table[] {
  const frames: unknown[] = Array.isArray(value) ? value : (() => {
    const response = object(value);
    if (response.error || response.HasErrors ||
        (Array.isArray(response.Exceptions) ? response.Exceptions.length > 0 : response.Exceptions))
      throw new Error('Kusto returned a management error.');
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
    if (!Array.isArray(frame.Columns) || !Array.isArray(frame.Rows))
      throw new Error('Malformed Kusto data table.');
    const columns = frame.Columns.map(column => {
      const name = object(column).ColumnName;
      if (typeof name !== 'string') throw new Error('Kusto column name is absent.');
      return name;
    });
    const rows = frame.Rows.map(row => {
      if (!Array.isArray(row) || row.length !== columns.length)
        throw new Error('Malformed Kusto result row.');
      return row;
    });
    const table = { kind: typeof frame.TableKind === 'string' ? frame.TableKind : 'PrimaryResult', columns, rows };
    if (table.kind === 'QueryCompletionInformation') {
      const code = columns.indexOf('StatusCode');
      if (code < 0 || rows.some(row => typeof row[code] !== 'number' || row[code] !== 0))
        throw new Error('Kusto completion contains an error or missing status.');
    }
    result.push(table);
  }
  return result;
}

export function tableRows(table: Table): Row[] {
  return table.rows.map(row => Object.fromEntries(table.columns.map((name, index) => [name, row[index]])));
}

function primary(value: unknown): Row[] {
  const table = tables(value).find(table => table.kind === 'PrimaryResult');
  if (!table) throw new Error('Kusto primary result is absent.');
  return tableRows(table);
}

function numeric(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

function dynamic(value: unknown): Row {
  return object(typeof value === 'string' ? JSON.parse(value) : value);
}

export function parseSample(details: unknown, statistics: unknown, at: string): AccelerationSample {
  const detail = primary(details);
  const stats = primary(statistics);
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

export function ready(sample: AccelerationSample, snapshot: Snapshot): boolean {
  return sample.version === snapshot.version && sample.completionPercent === 100 &&
    sample.pendingFiles === 0 && sample.healthy === true && sample.acceleratedRows === snapshot.rows;
}

export function durationMs(value: unknown): number | undefined {
  if (typeof value !== 'string') return undefined;
  const match = /^(?:(\d+)\.)?(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)$/.exec(value);
  if (!match) return undefined;
  return ((Number(match[1] ?? 0) * 24 + Number(match[2])) * 3600 +
    Number(match[3]) * 60 + Number(match[4])) * 1000;
}

function optionalObject(value: unknown): Row | undefined {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Row : undefined;
}

function itemCount(value: unknown): number | undefined {
  return numeric(value) ?? numeric(optionalObject(value)?.count);
}

export function parseQuery(value: unknown, requestId: string): QueryResult {
  const response = tables(value);
  const data = response.find(table => table.kind === 'PrimaryResult');
  if (!data) throw new Error('Kusto query returned no primary result.');
  const completion = response.find(table => table.kind === 'QueryCompletionInformation');
  const diagnostic = completion ? tableRows(completion).find(row => row.EventTypeName === 'QueryResourceConsumption') : undefined;
  const payload = diagnostic ? dynamic(diagnostic.Payload) : undefined;
  const resources = optionalObject(payload?.resource_usage);
  const cpu = optionalObject(resources?.cpu);
  const inputs = optionalObject(payload?.input_dataset_statistics);
  const external = optionalObject(inputs?.external_data);
  return { requestId, records: data.rows,
    queryCpuMs: durationMs(cpu?.['total cpu']),
    remoteDownloadedItems: itemCount(external?.downloaded_items),
    scannedExtents: numeric(optionalObject(inputs?.extents)?.scanned) };
}

export function identifier(name: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(name)) throw new Error('Invalid experiment table or column name.');
  return `['${name}']`;
}

export async function waitForRolePropagation(action: () => Promise<void>, signal: AbortSignal,
  emit: Emit, options = { timeoutMs: 600_000, intervalMs: 5000 }): Promise<void> {
  const deadline = Date.now() + options.timeoutMs;
  for (let attempt = 1; ; attempt++) {
    signal.throwIfAborted();
    try {
      await action();
      return;
    } catch (error) {
      if (!(error instanceof HttpFailure) || error.code !== 403 || Date.now() >= deadline) throw error;
      emit('role-propagation-wait', { attempt, status: 403,
        reason: 'New experiment resource permissions are not yet usable by Kusto.' });
      await sleep(Math.min(options.intervalMs, deadline - Date.now()), undefined, { signal });
    }
  }
}

export function repointRequiresPolicyRemoval(error: unknown): boolean {
  if (!(error instanceof HttpFailure) || error.code !== 400 || !error.message.startsWith('HTTP 400: '))
    return false;
  let response: unknown;
  try { response = JSON.parse(error.message.slice('HTTP 400: '.length)); }
  catch { return false; }
  const reason = optionalObject(optionalObject(response)?.error)?.['@message'];
  return typeof reason === 'string' &&
    reason.includes('Cannot alter the connection string of external table ') &&
    reason.includes('as it has query_acceleration policy enabled.') &&
    reason.includes('Please remove the query acceleration policy before altering the connection string.');
}

export class Kusto {
  private identity?: string;
  readonly capabilities: DiagnosticCapability = {
    originHistory: false, completeHistory: false, distinguishesIngestionFromMerge: false,
    positiveControlDetected: false, backgroundCpu: false,
    reason: 'Public QAP details/statistics expose aggregate state, not complete per-source ingestion history or attributable background CPU.',
  };

  constructor(readonly azure: Azure, readonly runId: string, readonly emit: Emit) {}

  async management(command: string, signal: AbortSignal): Promise<unknown> {
    signal.throwIfAborted();
    const requestId = `ArrowColQapExperiment;${this.runId};${randomUUID()}`;
    this.emit('kusto-request', { at: new Date().toISOString(), requestId, kind: 'management', phase: 'started',
      command: this.azure.c.redact(command) });
    const [data] = await this.azure.request('POST', this.azure.c.uri + '/v1/rest/mgmt',
      'https://kusto.kusto.windows.net', JSON.stringify({ db: this.azure.c.database, csl: command }),
      { 'Content-Type': 'application/json', Accept: 'application/json', 'x-ms-client-request-id': requestId });
    if (!data) throw new Error('Empty Kusto management response.');
    const response: unknown = JSON.parse(data.toString());
    tables(response);
    this.emit('kusto-request', { at: new Date().toISOString(), requestId, kind: 'management', phase: 'completed' });
    signal.throwIfAborted();
    return response;
  }

  async initialize(signal: AbortSignal): Promise<void> {
    const value: unknown = await this.azure.arm('GET', this.azure.c.identity_id, undefined, false, '2023-01-31');
    const principal = object(object(value).properties).principalId;
    if (typeof principal !== 'string' || !/^[a-f0-9-]{36}$/i.test(principal))
      throw new Error('Kusto managed identity principal is absent.');
    this.identity = principal;
    await this.management(`.alter-merge database ['${this.azure.c.database}'] policy managed_identity '${JSON.stringify([
      { ObjectId: principal, AllowedUsages: 'ExternalTable,AutomatedFlows' },
    ])}'`, signal);
    const cluster = primary(await this.management('.show cluster', signal));
    const version = cluster[0]?.ProductVersion ?? cluster[0]?.BuildVersion;
    this.emit('capabilities', { ...this.capabilities,
      buildVersion: typeof version === 'string' ? version : null, observedAt: new Date().toISOString() });
  }

  async expose(name: string, signal: AbortSignal): Promise<void> {
    await waitForRolePropagation(async () => {
      await this.define(name, name, signal);
    }, signal, this.emit);
    await waitForRolePropagation(async () => {
      await this.enable(name, signal);
    }, signal, this.emit);
  }

  async define(name: string, sourceName: string, signal: AbortSignal): Promise<void> {
    const table = identifier(name);
    identifier(sourceName);
    if (!this.identity) throw new Error('Managed identity has not been configured.');
    const source = `${this.azure.c.table_root}experiment/fixtures/experiment_${sourceName};managed_identity=${this.identity}`;
    await this.management(`.create-or-alter external table ${table} kind=delta (h@'${source}')`, signal);
  }

  async enable(name: string, signal: AbortSignal): Promise<void> {
    if (!this.identity) throw new Error('Managed identity has not been configured.');
    await this.management(`.alter external table ${identifier(name)} policy query_acceleration '${JSON.stringify({
      IsEnabled: true, Hot: '7.00:00:00', MaxAgeOverride: '00:01:00', ManagedIdentity: this.identity,
    })}'`, signal);
  }

  async sample(name: string, signal: AbortSignal): Promise<AccelerationSample> {
    const table = identifier(name);
    const [details, statistics] = await Promise.all([
      this.management(`.show external table ${table} details`, signal),
      this.management(`.show external table ${table} operations query_acceleration statistics`, signal),
    ]);
    const sample = parseSample(details, statistics, new Date().toISOString());
    this.emit('qap-sample', { table: name, ...sample });
    return sample;
  }

  async wait(name: string, snapshot: Snapshot, deadline: number, signal: AbortSignal,
    intervalMs: number, controlName?: string): Promise<AccelerationSample[]> {
    const samples: AccelerationSample[] = [];
    for (;;) {
      const [sample] = await Promise.all([
        this.sample(name, signal), controlName ? this.sample(controlName, signal) : Promise.resolve(undefined),
      ]);
      samples.push(sample);
      if (ready(sample, snapshot)) return samples;
      if (Date.now() >= deadline) throw new Error('QAP did not reach the exact frozen Delta version before the deadline.');
      await sleep(intervalMs, undefined, { signal });
    }
  }

  async query(name: string, columns: string[], lane: 'default' | 'freshness-1s',
    signal: AbortSignal): Promise<QueryResult> {
    identifier(name);
    if (!columns.length) throw new Error('An exact query requires explicit oracle columns.');
    const fields = columns.map(identifier);
    const source = `external_table("${name}"${lane === 'freshness-1s' ? ', timespan(1s)' : ''})`;
    const result = await this.executeQuery(
      `${source} | project ${fields.join(', ')} | order by ${fields.map(field => `${field} asc`).join(', ')}`,
      lane, signal);
    this.emit('query', { table: name, lane, at: new Date().toISOString(), requestId: result.requestId,
      rows: result.records.length, queryCpuMs: result.queryCpuMs,
      remoteDownloadedItems: result.remoteDownloadedItems, scannedExtents: result.scannedExtents });
    return result;
  }

  async probeProvenance(name: string, signal: AbortSignal): Promise<void> {
    identifier(name);
    const query = await this.executeQuery(`external_table("${name}") | summarize Rows=count(), ` +
      'WithIngestionTime=countif(isnotnull(ingestion_time())), ' +
      'WithExtent=countif(extent_id() != guid(00000000-0000-0000-0000-000000000000))',
    'provenance-probe', signal);
    const row = query.records[0];
    if (query.records.length !== 1 || !row || row.length !== 3 ||
        row.some(value => typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0))
      throw new Error('Public row-provenance probe returned an invalid measurement.');
    this.emit('public-provenance-probe', { table: name, requestId: query.requestId,
      rows: row[0], rowsWithIngestionTime: row[1], rowsWithExtent: row[2],
      completeOriginHistory: false,
      reason: 'Point-in-time row metadata is not complete source-file ingestion history.' });
  }

  private async executeQuery(csl: string, lane: string, signal: AbortSignal): Promise<QueryResult> {
    const requestId = `ArrowColQapExperiment;${this.runId};${lane};${randomUUID()}`;
    signal.throwIfAborted();
    this.emit('kusto-request', { at: new Date().toISOString(), requestId, kind: 'query',
      phase: 'started', command: csl });
    const [data] = await this.azure.request('POST', this.azure.c.uri + '/v2/rest/query',
      'https://kusto.kusto.windows.net', JSON.stringify({ db: this.azure.c.database,
        csl,
        properties: JSON.stringify({ Options: { query_results_cache_max_age: '00:00:00',
          truncationmaxrecords: 100_000, truncationmaxsize: 32 * 1024 * 1024 } }) }),
      { 'Content-Type': 'application/json', 'x-ms-client-request-id': requestId });
    if (!data) throw new Error('Empty Kusto query response.');
    const parsed: unknown = JSON.parse(data.toString());
    const result = parseQuery(parsed, requestId);
    signal.throwIfAborted();
    return result;
  }

  async reloadControl(name: string, signal: AbortSignal): Promise<void> {
    await this.management(`.drop external table ${identifier(name)}`, signal);
    this.emit('known-cache-reset', { table: name, at: new Date().toISOString(),
      action: 'Drop and recreate the experiment-owned external table and its QAP policy; source Delta files are unchanged.' });
    await this.expose(name, signal);
  }
}
