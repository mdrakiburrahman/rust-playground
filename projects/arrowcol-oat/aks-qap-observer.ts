import { appendFile, readFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { resolve } from 'node:path';
import { Azure, identity_principal, parse_xml, type HttpReceipt } from '../../infra/azure/bootstrap.js';
import { Kusto, identifier, kql, object, parseSample, primary, ready, tableRows,
  type AccelerationSample, type QueryEvidence, type Target } from '../../experiments/azure-qap/src/kusto.js';
import { writePrivateFile } from './runtime.js';

export interface QapObservation {
  startedAtMs: number;
  endedAtMs: number;
  before?: AccelerationSample;
  after?: AccelerationSample;
  details?: unknown;
  statistics?: unknown;
  afterDetails?: unknown;
  afterStatistics?: unknown;
  query?: QueryEvidence;
  queryHistory?: unknown;
  capacity?: unknown;
  cluster?: unknown;
  error?: string;
}

export interface ClockObservation { startedAtMs: number; endedAtMs: number; producerAtMs: number }

export interface DeltaVisibilityObservation {
  startedAtMs: number;
  endedAtMs: number;
  version?: number;
  logFiles?: number;
  requestId?: string;
  responseSha256?: string;
  error?: string;
}

export function parseDeltaVisibility(receipt: HttpReceipt, prefix: string):
  Pick<DeltaVisibilityObservation, 'version' | 'logFiles' | 'requestId' | 'responseSha256'> {
  const bytes = Buffer.from(receipt.bodyBase64, 'base64');
  const requestId = receipt.headers['x-ms-request-id'];
  if (receipt.status !== 200 || bytes.length > 16 * 1024 * 1024 ||
      !requestId || !/^[A-Za-z0-9-]{1,128}$/.test(requestId))
    throw new Error('Delta visibility requires a bounded successful storage receipt.');
  const root = parse_xml(bytes.toString('utf8'));
  const one = (name: string) => {
    const matches = root.children.filter(child => child.name === name);
    if (matches.length !== 1) throw new Error('Delta visibility listing has missing or duplicate fields.');
    return matches[0]!;
  };
  if (root.name !== 'EnumerationResults' || one('Prefix').text !== prefix || one('NextMarker').text ||
      root.children.some(child => child.name === 'Delimiter' && child.text))
    throw new Error('Delta visibility listing is not a complete exact-prefix observation.');
  const versions: number[] = [];
  const names = new Set<string>();
  const blobs = one('Blobs').children;
  if (blobs.length > 5000) throw new Error('Delta visibility listing exceeds its single-page bound.');
  for (const blob of blobs) {
    const nameFields = blob.children.filter(child => child.name === 'Name');
    const name = nameFields[0]?.text;
    if (blob.name !== 'Blob' || nameFields.length !== 1 || !name?.startsWith(prefix) || names.has(name) ||
        blob.children.some(child => ['Snapshot', 'VersionId', 'Deleted'].includes(child.name)))
      throw new Error('Delta visibility listing contains ambiguous or historical objects.');
    names.add(name);
    const relative = name.slice(prefix.length);
    if (!/^[A-Za-z0-9_.-]+$/.test(relative)) throw new Error('Delta visibility object escaped the log directory.');
    const match = /^(\d{20})\.json$/.exec(relative);
    if (match) versions.push(Number(match[1]));
    else if (relative.endsWith('.json')) throw new Error('Unexpected Delta JSON log name.');
  }
  versions.sort((a, b) => a - b);
  if (!versions.length || versions.some((version, index) => !Number.isSafeInteger(version) || version !== index))
    throw new Error('Delta visibility does not prove a contiguous log prefix from version zero.');
  return { version: versions.at(-1)!, logFiles: versions.length, requestId,
    responseSha256: createHash('sha256').update(bytes).digest('hex') };
}

export async function captureWriterShutdown(followArgs: string[], stop: () => Promise<void>,
  timeoutMs = 7 * 60_000): Promise<string> {
  if (!followArgs.length || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1)
    throw new Error('Invalid writer log capture command or deadline.');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let output = '', errorOutput = '', bytes = 0;
  let markReady!: () => void, failReady!: (error: unknown) => void;
  const ready = new Promise<void>((resolve, reject) => { markReady = resolve; failReady = reject; });
  const child = spawn(followArgs[0]!, followArgs.slice(1), {
    stdio: ['ignore', 'pipe', 'pipe'], signal: controller.signal,
  });
  const finished = new Promise<void>((resolve, reject) => {
    child.once('error', reject);
    child.stdout.once('error', reject);
    child.stderr.once('error', reject);
    child.once('close', (code, signal) => {
      if (code !== 0 || signal) reject(new Error(`Writer log capture failed (${code ?? signal}): ${errorOutput}`));
      else if (!output.includes('\n')) reject(new Error('Writer log capture ended without any complete records; producer was not stopped.'));
      else resolve();
    });
    child.stdout.on('data', (chunk: Buffer) => {
      bytes += chunk.length;
      if (bytes > 16 * 1024 * 1024) {
        reject(new Error('Writer shutdown logs exceeded the 16 MiB capture limit.'));
        controller.abort();
        return;
      }
      output += chunk.toString('utf8');
      if (output.includes('\n')) markReady();
    });
    child.stderr.on('data', (chunk: Buffer) => { errorOutput = (errorOutput + chunk.toString('utf8')).slice(-8192); });
  }).then(() => ({ ok: true as const }), error => {
    failReady(error);
    return { ok: false as const, error };
  });
  try {
    await ready;
    await stop();
    const outcome = await finished;
    if (!outcome.ok) throw outcome.error;
    return output;
  } catch (error) {
    controller.abort();
    await finished;
    throw error;
  } finally { clearTimeout(timer); }
}

export function clockErrorBound(observations: ClockObservation[]): number | null {
  if (observations.length < 2) return null;
  let bound = 0;
  for (const observation of observations) {
    if (![observation.startedAtMs, observation.endedAtMs, observation.producerAtMs].every(Number.isSafeInteger) ||
        observation.startedAtMs > observation.endedAtMs) throw new Error('Invalid producer clock observation.');
    bound = Math.max(bound, Math.abs(observation.producerAtMs - observation.startedAtMs),
      Math.abs(observation.producerAtMs - observation.endedAtMs));
  }
  return bound;
}

export class DirectQapObserver {
  readonly table: string;
  readonly marker: string;
  readonly kusto: Kusto;
  readonly observations: QapObservation[] = [];
  readonly clocks: ClockObservation[] = [];
  readonly sourceVisibility: DeltaVisibilityObservation[] = [];
  private lastClusterAt = 0;
  private lastProbedVersion = -1;
  private readonly evidencePath: string;
  private sourcePrefix = '';
  private sourceLoop?: Promise<void>;
  private sourceLoopError?: unknown;
  private stopSourceLoop = false;

  constructor(readonly azure: Azure, readonly runId: string, readonly runDir: string) {
    if (!/^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/.test(runId)) throw new Error('QAP observer needs a run UUID.');
    this.table = `qap_ingest_${runId.replaceAll('-', '')}`;
    this.marker = `arrowcol-qap-benchmark:${runId}`;
    this.kusto = new Kusto(azure, azure.c.experimentDatabase, runId, undefined, 1, warning => {
      console.warn('QAP query warning retained in private evidence: ' + String(warning.StatusCode));
    });
    this.evidencePath = resolve(runDir, 'qap-observations.jsonl');
  }

  async create(deltaRoot: string): Promise<void> {
    const storageRoot = `abfss://${this.azure.c.data}@${this.azure.c.account}.dfs.core.windows.net/`;
    const relative = deltaRoot.startsWith(storageRoot) ? deltaRoot.slice(storageRoot.length) : '';
    if (!relative || relative.split('/').some(part => !/^[A-Za-z0-9_=-][A-Za-z0-9_.=-]*$/.test(part) ||
        part === '.' || part === '..')) throw new Error('QAP source must be the exact configured storage root.');
    this.sourcePrefix = `${relative}/_delta_log/`;
    const signal = AbortSignal.timeout(120_000);
    const existing = tableRows(primary(await this.kusto.management('.show external tables', signal)));
    if (existing.some(row => row.TableName === this.table)) throw new Error('QAP table already exists; refusing reuse.');
    const principal = await identity_principal(this.azure);
    const source = `${deltaRoot};managed_identity=${principal}`;
    await writePrivateFile(resolve(this.runDir, 'qap-owner.json'), JSON.stringify({
      runId: this.runId, table: this.table, marker: this.marker, source,
    }) + '\n');
    await this.kusto.management(kql.create(this.table, source).replace('.create-or-alter', '.create') +
      ` with (docstring='${this.marker}')`, signal);
    await this.kusto.management(kql.policy(this.table, principal), signal);
    const policy = await this.kusto.management(`.show external table ${identifier(this.table)} policy query_acceleration`, signal);
    await writePrivateFile(resolve(this.runDir, 'qap-policy.json'), JSON.stringify(policy, null, 2) + '\n');
    const rows = tableRows(primary(policy));
    const actual = object(JSON.parse(String(rows[0]?.Policy)));
    if (actual.IsEnabled !== true || actual.Hot !== '7.00:00:00' || actual.MaxAge !== '00:05:00' ||
        actual.ManagedIdentity !== principal) throw new Error('QAP policy readback differs from the requested policy.');
    await this.sampleSource();
    this.sourceLoop = this.runSourceLoop().catch(error => {
      this.sourceLoopError = error;
      console.warn('Delta visibility recorder failed; bounded commit timing is unavailable.');
    });
  }

  private async sampleSource(): Promise<void> {
    const observation: DeltaVisibilityObservation = { startedAtMs: Date.now(), endedAtMs: 0 };
    let receipt: HttpReceipt | undefined;
    try {
      await this.azure.blob('GET', this.azure.c.data, '', {
        restype: 'container', comp: 'list', prefix: this.sourcePrefix, maxresults: '5000',
      }, undefined, { 'Cache-Control': 'no-cache' }, false, false, response => { receipt = response; });
      if (!receipt) throw new Error('Delta visibility response receipt is missing.');
      const parsed = parseDeltaVisibility(receipt, this.sourcePrefix);
      const previous = this.sourceVisibility.filter(row => row.version !== undefined).at(-1)?.version;
      if (previous !== undefined && parsed.version! < previous) throw new Error('Delta visibility version regressed.');
      Object.assign(observation, parsed);
    } catch (error) {
      observation.error = this.azure.c.redact(error instanceof Error ? error.message : String(error));
      console.warn('Delta visibility observation incomplete; commit-time interval remains unknown.');
    }
    observation.endedAtMs = Date.now();
    await appendFile(resolve(this.runDir, 'qap-delta-visibility.jsonl'),
      JSON.stringify({ ...observation, receipt }) + '\n', { mode: 0o600 });
    this.sourceVisibility.push(observation);
  }

  private async runSourceLoop(): Promise<void> {
    while (!this.stopSourceLoop) {
      await new Promise(resolve => setTimeout(resolve, 2000));
      if (!this.stopSourceLoop) await this.sampleSource();
    }
  }

  async finishSourceSampling(): Promise<void> {
    if (!this.sourceLoop) return;
    this.stopSourceLoop = true;
    await this.sourceLoop;
    this.sourceLoop = undefined;
    if (this.sourceLoopError) throw this.sourceLoopError;
    await this.sampleSource();
  }

  async sample(forceProbe = false): Promise<QapObservation> {
    const observation: QapObservation = { startedAtMs: Date.now(), endedAtMs: 0 };
    const signal = AbortSignal.timeout(90_000);
    try {
      [observation.details, observation.statistics, observation.capacity] = await Promise.all([
        this.kusto.management(kql.details(this.table), signal),
        this.kusto.management(kql.statistics(this.table), signal),
        this.kusto.management(kql.capacity(), signal),
      ]);
      observation.before = parseSample(observation.details, observation.statistics, new Date().toISOString());
      if (Date.now() - this.lastClusterAt >= 30_000) {
        observation.cluster = await this.azure.arm('GET', this.azure.c.cluster_id);
        this.lastClusterAt = Date.now();
      }
      const before = observation.before;
      if (before.version !== undefined && before.version >= 0 && before.acceleratedRows !== undefined &&
          before.acceleratedRows > 0 && before.healthy === true && before.completionPercent === 100 &&
          before.pendingFiles === 0 && before.pendingBytes === 0 &&
          (forceProbe || before.version !== this.lastProbedVersion)) {
        this.lastProbedVersion = before.version;
        observation.query = await this.kusto.queryEvidence(
          `external_table("${this.table}", timespan(1d))\n| summarize Rows=count() by Extent=tostring(extent_id())`, signal);
        [observation.afterDetails, observation.afterStatistics] = await Promise.all([
          this.kusto.management(kql.details(this.table), signal),
          this.kusto.management(kql.statistics(this.table), signal),
        ]);
        observation.after = parseSample(observation.afterDetails, observation.afterStatistics, new Date().toISOString());
        observation.queryHistory = await this.kusto.management(
          `.show queries | where ClientActivityId == "${observation.query.clientRequestId}" ` +
          '| project ClientActivityId, StartedOn, LastUpdatedOn, State, FailureReason, TotalCpu, OverallQueryStats', signal);
      }
    } catch (error) {
      observation.error = this.azure.c.redact(error instanceof Error ? error.message : String(error));
      console.warn('QAP observation incomplete; retained as an unobservable interval.');
    }
    observation.endedAtMs = Date.now();
    this.observations.push(observation);
    await appendFile(this.evidencePath, JSON.stringify(observation) + '\n', { mode: 0o600 });
    return observation;
  }

  async drain(target: Target, timeoutMs: number): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const observation = await this.sample(true);
      if (!observation.error && observation.before && observation.after && observation.query &&
          ready(observation.before, target) && ready(observation.after, target) &&
          observation.before.pendingBytes === 0 && observation.after.pendingBytes === 0) return true;
      await new Promise(resolve => setTimeout(resolve, Math.min(10_000, Math.max(0, deadline - Date.now()))));
    }
    return false;
  }

  async captureMetrics(startedAtMs: number, endedAtMs: number): Promise<void> {
    const timespan = `${new Date(startedAtMs).toISOString()}/${new Date(endedAtMs).toISOString()}`;
    try {
      const response = await this.azure.arm('GET', this.azure.c.cluster_id +
        '/providers/microsoft.insights/metrics?metricnames=CPU,CacheUtilization,InstanceCount' +
        `&timespan=${encodeURIComponent(timespan)}&interval=PT1M&aggregation=Average,Minimum,Maximum`,
      undefined, false, '2018-01-01');
      await writePrivateFile(resolve(this.runDir, 'qap-azure-metrics.json'), JSON.stringify({
        requestedAt: new Date().toISOString(), startedAtMs, endedAtMs, response,
      }) + '\n');
    } catch (error) {
      console.warn('Azure CPU/cache/node metric receipt unavailable; no metric values will be inferred.');
      await writePrivateFile(resolve(this.runDir, 'qap-azure-metrics-error.txt'),
        this.azure.c.redact(error instanceof Error ? error.message : String(error)) + '\n');
    }
  }

  async close(): Promise<void> {
    const outcomes = await Promise.allSettled([this.finishSourceSampling(), this.dropOwnedTable()]);
    const errors = outcomes.flatMap(result => result.status === 'rejected' ? [result.reason] : []);
    if (errors.length) throw new AggregateError(errors, 'QAP observation or owned-table cleanup failed.');
  }

  private async dropOwnedTable(): Promise<void> {
    const signal = AbortSignal.timeout(120_000);
    const rows = tableRows(primary(await this.kusto.management('.show external tables', signal)));
    const existing = rows.find(row => row.TableName === this.table);
    if (!existing) return;
    const receipt = JSON.parse(await readFile(resolve(this.runDir, 'qap-owner.json'), 'utf8'));
    if (receipt.table !== this.table || receipt.marker !== this.marker ||
        existing.DocString !== this.marker) throw new Error('QAP table ownership changed; refusing cleanup.');
    await this.kusto.drop(this.table, signal);
    const remaining = tableRows(primary(await this.kusto.management('.show external tables', signal)));
    if (remaining.some(row => row.TableName === this.table)) throw new Error('Owned QAP table remains after cleanup.');
  }
}
