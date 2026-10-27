import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

import { metricRangeEvidence, panelDiagnostics, panelFrameEvidence } from '../src/metrics-gate.js';

const range = (values: Array<[number, string]>) => ({ status: 'success', data: { resultType: 'matrix', result: [{ values }] } });

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
