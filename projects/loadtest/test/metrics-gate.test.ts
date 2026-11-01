import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

import { metricRangeEvidence, panelDiagnostics, panelFrameEvidence, quantileQuery } from '../src/metrics-gate.js';

const range = (values: Array<[number, string]>) => ({ status: 'success', data: { resultType: 'matrix', result: [{ values }] } });
const ranges = (...series: Array<Array<[number, string]>>) =>
  ({ status: 'success', data: { resultType: 'matrix', result: series.map(values => ({ values })) } });
const steps = (values: string[]): Array<[number, string]> => values.map((value, index) => [100 + index * 2, value]);

test('only the histogram_quantile panel opts in to Prometheus NaN steps', () => {
  const dashboard = JSON.parse(readFileSync(new URL('../observability/delta-operations.json', import.meta.url), 'utf8'));
  const optedIn = dashboard.panels
    .filter((panel: { targets: Array<{ expr: string }> }) => panel.targets.some(target => quantileQuery(target.expr)))
    .map((panel: { id: number }) => panel.id);
  // Every other panel sums, maxes or set-filters raw series; none divides, so none can yield NaN.
  assert.deepEqual(optedIn, [6]);
  for (const panel of dashboard.panels) {
    for (const target of panel.targets) assert.doesNotMatch(target.expr, /\/|\bquantile(_over_time)?\s*\(/u, `panel ${panel.id}`);
  }
});

test('CI quantile tail: NaN steps are skipped and counted only when the query opts in', () => {
  // Shape of the pinned-LGTM reproduction: finite p95 while busy, NaN once every bucket rate is zero.
  const tail = range(steps([...Array(47).fill('1.925'), ...Array(33).fill('NaN')]));
  assert.throws(() => metricRangeEvidence(tail, 3, true), /Invalid or non-monotonic/u);
  assert.deepEqual(metricRangeEvidence(tail, 3, true, true),
    { series: 1, samples: 47, maximum: 1.925, nan_samples: 33, sparse_series: 0 });
});

test('quantile panels need one series with three finite samples; fully idle series are recorded', () => {
  const busy = steps(['0.5', 'NaN', '0.7', '0.9']);
  const idle = steps(['NaN', 'NaN', 'NaN']);
  assert.deepEqual(metricRangeEvidence(ranges(busy, idle), 3, true, true),
    { series: 2, samples: 3, maximum: 0.9, nan_samples: 4, sparse_series: 1 });
  assert.throws(() => metricRangeEvidence(ranges(idle, idle), 3, true, true), /enough finite samples/u);
  // Three finite samples spread across series are not one plotted line.
  const one = steps(['0.5', 'NaN', 'NaN']);
  assert.throws(() => metricRangeEvidence(ranges(one, one, one), 3, true, true), /enough finite samples/u);
  // NaN still counts toward the structural step minimum but never toward the finite minimum.
  assert.throws(() => metricRangeEvidence(range(steps(['1', '2'])), 3, true, true), /Insufficient/u);
});

test('NaN opt-in keeps rejecting infinities, negatives, malformed values and timestamp disorder', () => {
  for (const bad of ['+Inf', '-Inf', 'Inf', 'nan', '-1', '', ' ', 'x']) {
    assert.throws(() => metricRangeEvidence(range(steps(['1', '2', '3', bad])), 3, true, true), /Invalid/u, bad);
  }
  for (const disordered of [[[1, '1'], [1, 'NaN'], [3, '2'], [4, '3']], [[1, '1'], [3, 'NaN'], [2, '2'], [4, '3']]]) {
    assert.throws(() => metricRangeEvidence(range(disordered as Array<[number, string]>), 3, true, true), /non-monotonic/u);
  }
  assert.throws(() => metricRangeEvidence(range([[1, '1'], [2, '2'], [3, '3'], [4, 'NaN', 'extra'] as unknown as [number, string]]),
    3, true, true), /Invalid/u);
});

test('Grafana NaN encoding (null listed in entities.NaN) is accepted only for opted-in quantile panels', () => {
  const frame = (values: unknown[], entities?: unknown) => ({
    schema: { fields: [{ type: 'time' }, { type: 'number' }] },
    data: { values: [values.map((_, index) => index), values], ...(entities === undefined ? {} : { entities: [null, entities] }) },
  });
  const panel = (...frames: unknown[]) => ({ results: { A: { frames } } });
  const busy = frame([0.5, 0.7, null, 0.9, null], { NaN: [2, 4] });
  const idle = frame([null, null, null], { NaN: [0, 1, 2] });
  assert.deepEqual(panelFrameEvidence(panel(busy, idle), true), { frames: 2, numericSamples: 3, nanSamples: 5, sparseFrames: 1 });
  assert.throws(() => panelFrameEvidence(panel(busy, idle)), /frame 1, samples 0/u);
  assert.throws(() => panelFrameEvidence(panel(idle, idle), true), /no frame with three finite samples/u);
  for (const invalid of [
    frame([0.5, 0.7, 0.9, null]),
    frame([0.5, 0.7, 0.9, null], { Inf: [3] }),
    frame([0.5, 0.7, 0.9, null], { NaN: [3], NegInf: [] }),
    frame([0.5, 0.7, 0.9, null], { NaN: [4] }),
    frame([0.5, 0.7, 0.9, 1], { NaN: [3] }),
    frame([0.5, 0.7, 0.9, -1]),
    frame([0.5, 0.7, 0.9, 'NaN']),
  ]) {
    assert.throws(() => panelFrameEvidence(panel(invalid), true), /Invalid|invalid|special/u);
  }
});

test('every rendered time-series panel overrides the datasource 60s floor with the actual 2s export cadence', () => {
  const dashboard = JSON.parse(readFileSync(new URL('../observability/delta-operations.json', import.meta.url), 'utf8'));
  const panels = dashboard.panels.filter((panel: { type: string }) => panel.type === 'timeseries');
  assert.deepEqual(panels.map((panel: { id: number }) => panel.id), [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15]);
  for (const panel of panels) {
    assert.equal(panel.interval, '2s', `panel ${panel.id} must set Grafana's panel query interval`);
    assert.equal(panel.datasource.uid, 'prometheus');
    assert.ok(panel.targets.length > 0);
  }
  const table = dashboard.panels.find((panel: { id: number }) => panel.id === 9);
  assert.equal(table.type, 'table');
  assert.equal(table.targets[0].instant, true);
  assert.equal(table.interval, undefined);
});

test('real samples, not JSON validity or a zero fallback, prove exercised metrics', () => {
  assert.deepEqual(metricRangeEvidence(range([[1, '0'], [2, '4'], [3, '8']])), { series: 1, samples: 3, maximum: 8 });
  for (const invalid of [null, {}, { status: 'error' }, range([]), range([[1, '1']]),
    range([[1, '0'], [2, '0'], [3, '0']]), range([[1, 'NaN'], [2, '2'], [3, '3']]),
    range([[1, '1'], [2, '1'], [3, '0']]),
    range([[1, '1'], [1, '2'], [3, '3']])]) {
    assert.throws(() => metricRangeEvidence(invalid));
  }
});

test('the CI eight-frame response fails even when four dense frames mask four sparse maintenance frames', () => {
  const frame = (samples: number) => ({
    schema: { fields: [{ type: 'number' }] },
    data: { values: [Array.from({ length: samples }, (_, index) => index + 1)] },
  });
  const response = { results: { A: { frames: Array.from({ length: 8 }, (_, index) => frame(index < 4 ? 3 : 1)) } } };
  const request = { queries: [{ refId: 'A', intervalMs: 60_000, maxDataPoints: 1600 }] };
  assert.throws(() => panelFrameEvidence(response), /frame 4, samples 1/u);
  assert.deepEqual(panelDiagnostics(response, request).queryResolution,
    [{ refId: 'A', intervalMs: 60_000, maxDataPoints: 1600 }]);
  // A finer request alone is not evidence that the sparse response was fixed.
  request.queries[0].intervalMs = 2000;
  assert.throws(() => panelFrameEvidence(response), /frame 4, samples 1/u);
  assert.deepEqual(panelFrameEvidence({ results: { A: { frames: Array.from({ length: 8 }, () => frame(3)) } } }),
    { frames: 8, numericSamples: 24 });
});

test('panel diagnostics identify a sparse failing frame without relaxing any frame threshold', () => {
  const frame = (values: unknown[]) => ({
    schema: { fields: [{ type: 'number' }] }, data: { values: [values] },
  });
  const document = { results: { A: { frames: [frame([1, 2, 3]), frame([null, '4', 5])] } } };
  assert.throws(() => panelFrameEvidence(document), /result 0, frame 1, samples 1/u);
  const diagnostic = panelDiagnostics(document, { queries: [{ refId: 'A', intervalMs: 1000, maxDataPoints: 1 }] });
  assert.deepEqual(diagnostic.frames, [
    { resultIndex: 0, frameIndex: 0, fieldCount: 1, numericSamples: 3, kind: 'other', fieldTypes: ['number'] },
    { resultIndex: 0, frameIndex: 1, fieldCount: 1, numericSamples: 1, kind: 'other', fieldTypes: ['number'] },
  ]);
  assert.deepEqual(diagnostic.queryResolution, [{ refId: 'A', intervalMs: 1000, maxDataPoints: 1 }]);
  assert.deepEqual(panelFrameEvidence({ results: { A: { frames: [frame([1, 2, 3])] } } }),
    { frames: 1, numericSamples: 3 });
  assert.throws(() => panelFrameEvidence({ results: { A: { frames: [frame([1, 2])] } } }));
  assert.throws(() => panelFrameEvidence({ results: { A: { frames: [frame([NaN, Infinity, null])] } } }));
});

test('public panel diagnostics are bounded and exclude private strings, labels, queries and samples', () => {
  const secret = 'privacy-sentinel-do-not-export';
  const frame = {
    schema: { meta: { type: secret }, fields: Array(100).fill({ type: 'number', name: secret, labels: { secret } }) },
    data: { values: Array(100).fill([1, 2, 3, secret]) },
  };
  const document = { results: { [secret]: { error: secret, frames: Array(100).fill(frame) } } };
  const request = { url: secret, queries: Array(100).fill({
    refId: secret, expr: secret, datasource: { url: secret }, maxDataPoints: secret, intervalMs: secret,
  }) };
  const diagnostic = panelDiagnostics(document, request);
  const serialized = JSON.stringify(diagnostic);
  assert.ok(!serialized.includes(secret));
  assert.ok(Buffer.byteLength(serialized, 'utf8') <= 4096);
  assert.equal(diagnostic.frameCount, 100);
  assert.equal(diagnostic.omittedFrames, 92);
  assert.equal(diagnostic.resultErrors, 1);
  assert.equal((diagnostic.frames as unknown[]).length, 8);
  assert.equal((diagnostic.queryResolution as unknown[]).length, 4);
  assert.equal(panelDiagnostics(null, null).status, 'unrecognized');
  assert.throws(() => panelFrameEvidence(document));
});
