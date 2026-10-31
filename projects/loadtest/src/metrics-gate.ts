export interface RangeEvidence {
  series: number;
  samples: number;
  maximum: number;
  nan_samples?: number;
  sparse_series?: number;
}

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown> : undefined;
}

/** Bounded shape metadata only: never copy labels, expressions, URLs, errors or samples. */
export function panelDiagnostics(document: unknown, request: unknown): Record<string, unknown> {
  const results = record(record(document)?.results);
  const queries = record(request)?.queries;
  const frames: Array<Record<string, unknown>> = [];
  let frameCount = 0;
  let resultErrors = 0;
  for (const [resultIndex, value] of Object.values(results ?? {}).entries()) {
    const result = record(value);
    if (result?.error) resultErrors++;
    if (!Array.isArray(result?.frames)) continue;
    for (const [frameIndex, value] of result.frames.entries()) {
      frameCount++;
      if (frames.length === 8) continue;
      const frame = record(value);
      const schema = record(frame?.schema);
      const fields = Array.isArray(schema?.fields) ? schema.fields : [];
      const values = record(frame?.data)?.values;
      let numericSamples = 0;
      for (const [index, field] of fields.entries()) {
        const column: unknown = Array.isArray(values) ? values[index] : undefined;
        if (record(field)?.type === 'number' && Array.isArray(column)) {
          numericSamples += column.filter(value => typeof value === 'number' && Number.isFinite(value)).length;
        }
      }
      const kind = record(schema?.meta)?.type;
      frames.push({
        resultIndex, frameIndex, fieldCount: fields.length, numericSamples,
        kind: typeof kind === 'string' && ['timeseries-multi', 'timeseries-wide', 'timeseries-long', 'table', 'exemplar'].includes(kind)
          ? kind : 'other',
        fieldTypes: fields.slice(0, 8).map(field => {
          const type = record(field)?.type;
          return typeof type === 'string' && ['number', 'time', 'string', 'boolean'].includes(type) ? type : 'other';
        }),
      });
    }
  }
  const number = (value: unknown): number | null =>
    typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : null;
  return {
    status: results ? 'results' : 'unrecognized',
    resultCount: results ? Object.keys(results).length : 0,
    resultErrors, frameCount, omittedFrames: frameCount - frames.length, frames,
    queryCount: Array.isArray(queries) ? queries.length : 0,
    queryResolution: Array.isArray(queries) ? queries.slice(0, 4).map(value => {
      const query = record(value);
      return {
        refId: typeof query?.refId === 'string' && /^[A-Z]$/u.test(query.refId) ? query.refId : 'other',
        intervalMs: number(query?.intervalMs), maxDataPoints: number(query?.maxDataPoints),
      };
    }) : [],
  };
}

/**
 * Prometheus returns the literal value `NaN` from `histogram_quantile` for every step whose
 * bucket rates are all zero, such as an idle drain tail. No other operations panel can
 * produce NaN, so only these queries may opt in to treating NaN steps as absent.
 */
export function quantileQuery(expr: string): boolean {
  return /\bhistogram_quantile\s*\(/u.test(expr);
}

export interface FrameEvidence {
  frames: number;
  numericSamples: number;
  nanSamples?: number;
  sparseFrames?: number;
}

/** Indexes of `null` values that Grafana marks as Prometheus NaN; any other entity is invalid. */
function nanIndexes(entities: unknown, length: number): Set<number> {
  if (entities === undefined || entities === null) return new Set();
  const entity = record(entities);
  if (!entity || Object.keys(entity).some(key => key !== 'NaN') || !Array.isArray(entity.NaN) ||
      entity.NaN.some(index => !Number.isSafeInteger(index) || index < 0 || index >= length)) {
    throw new Error('Grafana panel numeric field has non-NaN special values');
  }
  return new Set(entity.NaN as number[]);
}

/**
 * Frames returned to the actual Grafana panel, not a separate probe query. With `allowNaN`,
 * a frame may contain only Grafana-encoded NaN steps (`null` listed in `entities[].NaN`), but
 * at least one frame must still carry three finite samples.
 */
export function panelFrameEvidence(document: unknown, allowNaN = false): FrameEvidence {
  const response = document as {
    results?: Record<string, {
      error?: unknown;
      frames?: Array<{ schema?: { fields?: Array<{ type?: string }> }; data?: { values?: unknown[][]; entities?: unknown[] } }>;
    }>;
  } | null;
  if (!response?.results || typeof response.results !== 'object') throw new Error('Grafana panel returned no datasource frames');
  let frames = 0;
  let numericSamples = 0;
  let nanSamples = 0;
  let sparseFrames = 0;
  for (const [resultIndex, result] of Object.values(response.results).entries()) {
    if (result.error || !Array.isArray(result.frames) || !result.frames.length) throw new Error('Grafana panel query failed or returned no frames');
    for (const [frameIndex, frame] of result.frames.entries()) {
      let samples = 0;
      for (const [index, field] of (frame.schema?.fields ?? []).entries()) {
        if (field.type !== 'number') continue;
        const values = frame.data?.values?.[index];
        if (!Array.isArray(values)) throw new Error('Grafana panel numeric field has no values');
        if (!allowNaN) {
          samples += values.filter(value => typeof value === 'number' && Number.isFinite(value)).length;
          continue;
        }
        const nan = nanIndexes(frame.data?.entities?.[index], values.length);
        for (const [position, value] of values.entries()) {
          if (typeof value === 'number' && Number.isFinite(value) && value >= 0 && !nan.has(position)) samples++;
          else if (value === null && nan.has(position)) nanSamples++;
          else throw new Error(`Grafana panel has an invalid numeric sample (result ${resultIndex}, frame ${frameIndex})`);
        }
      }
      if (samples < 3) {
        if (!allowNaN) {
          throw new Error(`Grafana panel did not receive three actual numeric samples (result ${resultIndex}, frame ${frameIndex}, samples ${samples})`);
        }
        sparseFrames++;
      }
      frames++;
      numericSamples += samples;
    }
  }
  if (frames === 0) throw new Error('Grafana panel returned no datasource frames');
  if (!allowNaN) return { frames, numericSamples };
  if (sparseFrames === frames) throw new Error('Grafana panel has no frame with three finite samples');
  return { frames, numericSamples, nanSamples, sparseFrames };
}

/**
 * Missing, malformed and all-zero data are not proof of working telemetry. With `allowNaN`
 * (quantile panels only), exact Prometheus `NaN` steps are counted and skipped, but at least
 * one series must still have `minimumSamples` finite non-negative samples.
 */
export function metricRangeEvidence(document: unknown, minimumSamples = 3, allowZero = false, allowNaN = false): RangeEvidence {
  if (!Number.isInteger(minimumSamples) || minimumSamples < 3) throw new Error('At least three metric samples are required');
  const response = document as {
    status?: unknown;
    data?: { resultType?: unknown; result?: Array<{ values?: Array<[number, string]> }> };
  } | null;
  if (response?.status !== 'success' || response.data?.resultType !== 'matrix' || !Array.isArray(response.data.result) ||
      response.data.result.length === 0) throw new Error('Grafana datasource returned no metric range');
  let samples = 0;
  let nanSamples = 0;
  let sparseSeries = 0;
  let maximum = Number.NEGATIVE_INFINITY;
  for (const series of response.data.result) {
    if (!Array.isArray(series.values) || series.values.length < minimumSamples) throw new Error('Insufficient metric samples');
    let previous = Number.NEGATIVE_INFINITY;
    let finite = 0;
    let latest = Number.NaN;
    for (const sample of series.values) {
      if (!Array.isArray(sample) || sample.length !== 2 || !Number.isFinite(sample[0]) || sample[0] <= previous ||
          typeof sample[1] !== 'string' || sample[1].trim() === '') {
        throw new Error('Invalid or non-monotonic metric samples');
      }
      previous = sample[0];
      if (allowNaN && sample[1] === 'NaN') {
        nanSamples++;
        continue;
      }
      if (!Number.isFinite(Number(sample[1])) || Number(sample[1]) < 0) throw new Error('Invalid or non-monotonic metric samples');
      latest = Number(sample[1]);
      maximum = Math.max(maximum, latest);
      finite++;
    }
    samples += finite;
    if (finite < minimumSamples) sparseSeries++;
    if (!allowZero && !(latest > 0)) throw new Error('Latest metric sample does not prove a healthy exercised source');
  }
  if (sparseSeries === response.data.result.length) throw new Error('No metric series has enough finite samples');
  if (!allowZero && maximum <= 0) throw new Error('All-zero metrics do not prove exercised instrumentation');
  const evidence: RangeEvidence = { series: response.data.result.length, samples, maximum };
  return allowNaN ? { ...evidence, nan_samples: nanSamples, sparse_series: sparseSeries } : evidence;
}
