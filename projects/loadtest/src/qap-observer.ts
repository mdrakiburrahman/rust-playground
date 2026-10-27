import { appendFile, mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

import { OwnedProcessError, runOwnedProcess } from './owned-process.js';

type Pool = 'direct' | 'dataflow';
interface Point { name: string; pool: Pool; value: number; unit?: string }
type RecordValue = Record<string, unknown>;

interface HelperDiagnostic {
  code: 'completed' | 'deadline_exceeded' | 'helper_failed';
  elapsed_ms: number;
  queries_completed: number;
}

async function helperDiagnostic(directory: string): Promise<HelperDiagnostic | null> {
  let content: string;
  try {
    content = await readFile(path.join(directory, 'observation-diagnostic.json'), 'utf8');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw new Error('QAP helper diagnostic unreadable');
  }
  const value = record(JSON.parse(content) as unknown);
  if (!value || (value.code !== 'completed' && value.code !== 'deadline_exceeded' && value.code !== 'helper_failed') ||
      typeof value.elapsed_ms !== 'number' || !Number.isSafeInteger(value.elapsed_ms) || value.elapsed_ms < 0 ||
      typeof value.queries_completed !== 'number' || !Number.isSafeInteger(value.queries_completed) ||
      value.queries_completed < 0) throw new Error('QAP helper diagnostic invalid');
  return { code: value.code, elapsed_ms: value.elapsed_ms,
    queries_completed: value.queries_completed };
}

function record(value: unknown): RecordValue | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as RecordValue : undefined;
}

function measured(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0;
}

/** Missing tables/fields suppress the corresponding aggregate, never becoming measured zeroes. */
export function qapPoints(value: unknown): { observed: number; points: Point[] } {
  const observation = record(value);
  const tables = record(observation?.tables);
  const observed = observation?.observed_unix_ms;
  if (!tables || !Number.isSafeInteger(observed) || typeof observed !== 'number' || observed <= 0 ||
      observation?.backend !== 'native-qap' || !['local', 'azure'].includes(String(observation.mode))) {
    throw new Error('Malformed native QAP observation');
  }
  const points: Point[] = [];
  for (const pool of ['direct', 'dataflow'] as const) {
    const entries = Object.entries(tables).filter(([name]) => name.startsWith(`${pool}_sqlserver_`));
    if (entries.length !== 13) throw new Error(`QAP observation must cover all 13 ${pool} source tables`);
    const available = entries.map(([, table]) => record(table)).filter(table => table?.available === true);
    const aggregates: Point[] = [];
    for (const [name, field, reduce, unit] of [
      ['completion_percent', 'AccelerationCompletePercentage', 'min', '%'],
      ['pending_artifacts', 'AccelerationPendingDataFilesCount', 'sum', '1'],
      ['cached_bytes', 'HotSize', 'sum', 'By'],
    ] as const) {
      const values = available.map(table => record(table?.statistics)?.[field]);
      if (available.length === 13 && values.every(measured) &&
          (field !== 'AccelerationCompletePercentage' || values.every(value => value <= 100))) {
        aggregates.push({ name: `qap_${name}`, pool, value: reduce === 'min' ? Math.min(...values) :
          values.reduce((sum, value) => sum + value, 0), unit });
      }
    }
    const healthy = available.map(table => record(table?.state)?.is_healthy);
    if (available.length === 13 && healthy.every(value => typeof value === 'boolean')) {
      aggregates.push({ name: 'qap_unhealthy_catalogs', pool, value: healthy.filter(value => !value).length });
    }
    const updated = available.map(table => record(table?.statistics)?.LastUpdatedDateTime);
    const ages = updated.map(value => typeof value === 'string' ? (observed - Date.parse(value)) / 1000 : Number.NaN);
    if (available.length === 13 && ages.every(measured)) {
      aggregates.push({ name: 'qap_lag_seconds', pool, value: Math.max(...ages), unit: 's' });
    }
    points.push(...aggregates,
      { name: 'qap_observation_complete', pool, value: Number(aggregates.length === 5) },
      { name: 'qap_observed_tables', pool, value: available.length },
      { name: 'qap_observation_unix_seconds', pool, value: observed / 1000, unit: 's' });
  }
  if (Object.keys(tables).length !== 26) throw new Error('Unexpected non-source table in QAP observation');
  return { observed, points };
}

export async function exportQapPoints(endpoint: string, observed: number, points: Point[]): Promise<void> {
  const metrics = new Map<string, { name: string; unit: string; gauge: { dataPoints: unknown[] } }>();
  for (const point of points) {
    if (!measured(point.value)) throw new Error('Nonfinite QAP aggregate cannot be exported');
    let metric = metrics.get(point.name);
    if (!metric) {
      metric = { name: point.name, unit: point.unit ?? '1', gauge: { dataPoints: [] } };
      metrics.set(point.name, metric);
    }
    metric.gauge.dataPoints.push({
      attributes: [{ key: 'pool', value: { stringValue: point.pool } }],
      timeUnixNano: `${observed}000000`, asDouble: point.value,
    });
  }
  const response = await fetch(new URL('/v1/metrics', endpoint), {
    method: 'POST', headers: { 'content-type': 'application/json' }, signal: AbortSignal.timeout(5000),
    body: JSON.stringify({ resourceMetrics: [{
      resource: { attributes: [{ key: 'service.name', value: { stringValue: 'arrowcol-qap-observer' } }] },
      scopeMetrics: [{ scope: { name: 'arrowcol-loadtest-qap' }, metrics: [...metrics.values()] }],
    }] }),
  });
  if (!response.ok) throw new Error(`QAP OTLP export failed with HTTP ${response.status}`);
  const result = record(await response.json());
  if (!result) throw new Error('Malformed QAP OTLP export response');
  const partial = record(result.partialSuccess);
  if (partial && (Number(partial.rejectedDataPoints ?? 0) !== 0 || partial.errorMessage)) {
    throw new Error('QAP OTLP collector rejected observations');
  }
}

/** The helper is read-only and allowed to finish its fixed deadline when stopping. */
export function startQapObserver(options: {
  binary: string; cwd: string; env: NodeJS.ProcessEnv; results: string;
  endpoint: string; otlpEndpoint: string; intervalMs?: number;
}): () => Promise<void> {
  const intervalMs = options.intervalMs ?? 10_000;
  if (!Number.isSafeInteger(intervalMs) || intervalMs <= 0) throw new Error('QAP observation interval must be positive');
  const stop = new AbortController();
  let failure: unknown;
  let phase: 'artifact' | 'helper' | 'output' | 'export' = 'artifact';
  let sequence = -1;
  let started = Date.now();
  let provenance: string | undefined;
  const completed = (async () => {
    const observations = path.join(options.results, 'qap-observations');
    await mkdir(observations, { recursive: true });
    while (!stop.signal.aborted) {
      started = Date.now();
      phase = 'artifact';
      provenance = path.join(observations, String(++sequence));
      await mkdir(provenance);
      const output = path.join(provenance, 'observation.json');
      phase = 'helper';
      await runOwnedProcess(options.binary, ['observe-qap', '--endpoint', options.endpoint,
        '--results', provenance, '--output', output, '--timeout', '30s'],
      { cwd: options.cwd, env: options.env, timeoutMs: 40_000 });
      phase = 'output';
      const observation = qapPoints(JSON.parse(await readFile(output, 'utf8')) as unknown);
      phase = 'export';
      await exportQapPoints(options.otlpEndpoint, observation.observed, observation.points);
      phase = 'artifact';
      await appendFile(path.join(options.results, 'qap-observer.jsonl'), `${JSON.stringify({
        observed_unix_ms: observation.observed, artifact: path.relative(options.results, output),
        queries: path.relative(options.results, path.join(provenance, 'kusto-queries.jsonl')),
        complete: observation.points.filter(point => point.name === 'qap_observation_complete'),
        exported: true,
      })}\n`);
      try {
        await delay(Math.max(0, intervalMs - (Date.now() - started)), undefined, { signal: stop.signal });
      } catch (error) {
        if (!(stop.signal.aborted && error instanceof Error && error.name === 'AbortError')) throw error;
      }
    }
  })().catch(async error => {
    failure = error;
    const observed = Date.now();
    let diagnostic: HelperDiagnostic | null = null;
    let diagnosticInvalid = false;
    if (provenance) {
      try {
        diagnostic = await helperDiagnostic(provenance);
      } catch {
        diagnosticInvalid = true;
      }
    }
    await appendFile(path.join(options.results, 'qap-observer.jsonl'), `${JSON.stringify({
      observed_unix_ms: observed, exported: false, error: 'QAP observation or OTLP export failed',
      code: `${phase}_failed`, phase, sequence, elapsed_ms: Math.max(0, observed - started),
      helper_code: diagnostic?.code ?? null, queries_completed: diagnostic?.queries_completed ?? null,
      helper_elapsed_ms: diagnostic?.elapsed_ms ?? null, diagnostic_invalid: diagnosticInvalid,
      process: error instanceof OwnedProcessError ? error.outcome : null,
      supervisor_timed_out: error instanceof OwnedProcessError ? error.supervisor_timed_out : null,
    })}\n`);
    await exportQapPoints(options.otlpEndpoint, observed, ['direct', 'dataflow'].map(pool => ({
      name: 'qap_observation_complete', pool: pool as Pool, value: 0,
    })));
  }).catch(error => {
    failure = new AggregateError([failure, error], 'QAP failure reporting also failed');
  });
  return async () => {
    stop.abort();
    await completed;
    if (failure) throw new Error('Live QAP observation/export failed; qualification is incomplete', { cause: failure });
  };
}
