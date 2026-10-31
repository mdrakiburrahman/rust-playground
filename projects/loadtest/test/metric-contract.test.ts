import assert from 'node:assert/strict';
import { test } from 'node:test';

import { requiredMetrics, sdkMetricFamilies } from '../src/metric-contract.js';
import { metricRangeEvidence, panelFrameEvidence } from '../src/metrics-gate.js';

test('baseline requires each real SDK family from both source providers, not any delta metric', () => {
  const metrics = requiredMetrics('source-sdk', false);
  assert.equal(metrics.length, 2 * sdkMetricFamilies.length + 1);
  for (const [pool, service] of [['direct', 'arrowcol-rust'], ['dataflow', 'arrowcol-dataflow-rust']]) {
    for (const family of sdkMetricFamilies) {
      const metric = metrics.find(metric => metric.name === `${pool}:${family}`);
      assert.equal(metric?.query, `sum(${family}{service_name="${service}"})`);
    }
  }
  assert(metrics.every(metric => !metric.query.includes('kusto_bridge_') && !metric.query.includes('qap_')));
});

test('a panel canvas alone cannot qualify without actual numeric frames delivered to that panel', () => {
  const frame = {
    schema: { fields: [{ type: 'time' }, { type: 'number' }] },
    data: { values: [[1, 2, 3], [0, 1, 2]] },
  };
  assert.deepEqual(panelFrameEvidence({ results: { A: { frames: [frame] } } }), { frames: 1, numericSamples: 3 });
  assert.throws(() => panelFrameEvidence({ results: { A: { frames: [] } } }), /no frames/u);
  assert.throws(() => panelFrameEvidence({ results: { A: { error: 'datasource failure', frames: [frame] } } }), /failed/u);
  assert.throws(() => panelFrameEvidence({ results: { A: { frames: [{
    schema: frame.schema, data: { values: [[1, 2, 3], [null, Number.NaN, 1]] },
  }] } } }), /three actual numeric/u);
});

test('service qualification cannot pass without real maintenance effects, skipping and per-pool QAP', () => {
  const metrics = requiredMetrics('service-qualification', false);
  for (const name of ['optimize:actual_removals', 'purge:actual_removals', 'delta_maintenance_files_deleted_total',
    'tenant_stream_datafusion_row_groups_pruned_statistics_total', 'direct:qap_completion_percent',
    'dataflow:qap_completion_percent']) {
    assert.equal(metrics.find(metric => metric.name === name)?.allowZero, false, name);
  }
  assert(!metrics.some(metric => metric.name === 'bridge_scrape'));
  assert(requiredMetrics('service-qualification', true).some(metric => metric.name === 'bridge_scrape'));
});

test('real zero backlog/retries are legal only for explicitly declared zero-valid families', () => {
  const zero = { status: 'success', data: { resultType: 'matrix', result: [{ values: [[1, '0'], [2, '0'], [3, '0']] }] } };
  assert.equal(metricRangeEvidence(zero, 3, true).maximum, 0);
  assert.throws(() => metricRangeEvidence(zero), /healthy|All-zero/u);
  assert.throws(() => metricRangeEvidence({ status: 'success', data: { resultType: 'matrix', result: [] } }, 3, true), /no metric range/u);
  zero.data.result[0].values[1][1] = '-1';
  assert.throws(() => metricRangeEvidence(zero, 3, true), /Invalid/u);
});
