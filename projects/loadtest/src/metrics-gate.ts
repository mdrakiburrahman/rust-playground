export interface RangeEvidence {
  series: number;
  samples: number;
  maximum: number;
}

/** Frames returned to the actual Grafana panel, not a separate probe query. */
export function panelFrameEvidence(document: unknown): { frames: number; numericSamples: number } {
  const response = document as {
    results?: Record<string, {
      error?: unknown;
      frames?: Array<{ schema?: { fields?: Array<{ type?: string }> }; data?: { values?: unknown[][] } }>;
    }>;
  } | null;
  if (!response?.results || typeof response.results !== 'object') throw new Error('Grafana panel returned no datasource frames');
  let frames = 0;
  let numericSamples = 0;
  for (const result of Object.values(response.results)) {
    if (result.error || !Array.isArray(result.frames) || !result.frames.length) throw new Error('Grafana panel query failed or returned no frames');
    for (const frame of result.frames) {
      let samples = 0;
      for (const [index, field] of (frame.schema?.fields ?? []).entries()) {
        if (field.type !== 'number') continue;
        const values = frame.data?.values?.[index];
        if (!Array.isArray(values)) throw new Error('Grafana panel numeric field has no values');
        samples += values.filter(value => typeof value === 'number' && Number.isFinite(value)).length;
      }
      if (samples < 3) throw new Error('Grafana panel did not receive three actual numeric samples');
      frames++;
      numericSamples += samples;
    }
  }
  if (frames === 0) throw new Error('Grafana panel returned no datasource frames');
  return { frames, numericSamples };
}

/** Missing, malformed and all-zero data are not proof of working telemetry. */
export function metricRangeEvidence(document: unknown, minimumSamples = 3, allowZero = false): RangeEvidence {
  if (!Number.isInteger(minimumSamples) || minimumSamples < 3) throw new Error('At least three metric samples are required');
  const response = document as {
    status?: unknown;
    data?: { resultType?: unknown; result?: Array<{ values?: Array<[number, string]> }> };
  } | null;
  if (response?.status !== 'success' || response.data?.resultType !== 'matrix' || !Array.isArray(response.data.result) ||
      response.data.result.length === 0) throw new Error('Grafana datasource returned no metric range');
  let samples = 0;
  let maximum = Number.NEGATIVE_INFINITY;
  for (const series of response.data.result) {
    if (!Array.isArray(series.values) || series.values.length < minimumSamples) throw new Error('Insufficient metric samples');
    let previous = Number.NEGATIVE_INFINITY;
    for (const sample of series.values) {
      if (!Array.isArray(sample) || sample.length !== 2 || !Number.isFinite(sample[0]) || sample[0] <= previous ||
          typeof sample[1] !== 'string' || sample[1].trim() === '' || !Number.isFinite(Number(sample[1])) || Number(sample[1]) < 0) {
        throw new Error('Invalid or non-monotonic metric samples');
      }
      previous = sample[0];
      maximum = Math.max(maximum, Number(sample[1]));
      samples++;
    }
    if (!allowZero && Number(series.values.at(-1)?.[1]) <= 0) throw new Error('Latest metric sample does not prove a healthy exercised source');
  }
  if (!allowZero && maximum <= 0) throw new Error('All-zero metrics do not prove exercised instrumentation');
  return { series: response.data.result.length, samples, maximum };
}
