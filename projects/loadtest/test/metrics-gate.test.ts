import assert from 'node:assert/strict';
import { test } from 'node:test';

import { metricRangeEvidence } from '../src/metrics-gate.js';

const range = (values: Array<[number, string]>) => ({ status: 'success', data: { resultType: 'matrix', result: [{ values }] } });

test('real samples, not JSON validity or a zero fallback, prove exercised metrics', () => {
  assert.deepEqual(metricRangeEvidence(range([[1, '0'], [2, '4'], [3, '8']])), { series: 1, samples: 3, maximum: 8 });
  for (const invalid of [null, {}, { status: 'error' }, range([]), range([[1, '1']]),
    range([[1, '0'], [2, '0'], [3, '0']]), range([[1, 'NaN'], [2, '2'], [3, '3']]),
    range([[1, '1'], [2, '1'], [3, '0']]),
    range([[1, '1'], [1, '2'], [3, '3']])]) {
    assert.throws(() => metricRangeEvidence(invalid));
  }
});
