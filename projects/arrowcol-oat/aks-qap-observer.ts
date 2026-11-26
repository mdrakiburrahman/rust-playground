import { appendFile, readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { Azure, identity_principal } from '../../infra/azure/bootstrap.js';
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
  private lastClusterAt = 0;
  private lastProbedVersion = -1;
  private readonly evidencePath: string;

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

  async close(): Promise<void> {
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
