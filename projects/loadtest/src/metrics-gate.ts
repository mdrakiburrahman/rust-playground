export interface RangeEvidence {
  series: number;
  samples: number;
  maximum: number;
}

/** Missing, malformed and all-zero data are not proof of working telemetry. */
export function metricRangeEvidence(document: unknown, minimumSamples = 3): RangeEvidence {
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
          typeof sample[1] !== 'string' || sample[1].trim() === '' || !Number.isFinite(Number(sample[1]))) {
        throw new Error('Invalid or non-monotonic metric samples');
      }
      previous = sample[0];
      maximum = Math.max(maximum, Number(sample[1]));
      samples++;
    }
    if (Number(series.values.at(-1)?.[1]) <= 0) throw new Error('Latest metric sample does not prove a healthy exercised source');
  }
  if (maximum <= 0) throw new Error('All-zero metrics do not prove exercised instrumentation');
  return { series: response.data.result.length, samples, maximum };
}
