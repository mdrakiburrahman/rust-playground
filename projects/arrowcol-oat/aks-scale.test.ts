import assert from 'node:assert/strict';
import { test } from 'node:test';
import { assessScaleTrial, executeScaleTrial, recommendScaleStep, validateScaleTarget, type ScaleEvidence, type ScaleTarget } from './aks-scale.js';
import { verifySafeIdle, type SafeIdleObservation } from './aks-reconcile.js';

function evidence(overrides: Partial<ScaleEvidence> = {}): ScaleEvidence {
  return {
    committedRowsPerSecond: 1000, offeredRowsPerSecond: 1200, estimatedCollectorCapacityRowsPerSecond: 2000,
    generatorCpuSaturated: false, generatorNetworkSaturated: false, collectorCpuSaturated: false,
    collectorThrottled: false, collectorMemorySaturated: false, collectorNodePoolAtMaximum: false,
    qapBacklogGrowing: false, committedRateStable: false, kustoSaturated: false,
    loadgenMax: 1, loadgenMaximum: 4, collectorCpuLimitCores: 2, collectorCpuLimitMaximum: 8,
    collectorMemoryLimitGiB: 6, collectorMemoryLimitMaximumGiB: 24, collectorNodeMax: 1,
    collectorNodeMaxMaximum: 4, kustoCapacity: 2, kustoCapacityMaximum: 4, kustoSku: 'Standard_E4ads_v5',
    systemPoolCount: 3, unschedulablePods: 0, pdbBlockedDrains: 0, ...overrides,
  };
}
function safeIdle(overrides: Partial<SafeIdleObservation> = {}): SafeIdleObservation {
  return {
    aksProvisioningState: 'Succeeded', kustoProvisioningState: 'Succeeded', systemPoolCount: 2,
    userPools: [{ name: 'benchd64', nodeCount: 0, autoscaleMin: 0, autoscaleMax: 1 },
      { name: 'benche64', nodeCount: 0, autoscaleMin: 0, autoscaleMax: 1 }],
    pods: [{ name: 'system', ready: true, scheduled: true }], qapNotHealthyReasons: 0,
    qapPendingBacklog: 0, previousQapPendingBacklog: 0, deltaRowsBefore: 10, deltaRowsAfter: 10,
    kustoRowsBefore: 20, kustoRowsAfter: 20, kustoSku: 'Standard_E4ads_v5', kustoCapacity: 2,
    kustoAutoscaleMin: 2, kustoAutoscaleMax: 4, kustoAutoStopEnabled: true, ...overrides,
  };
}

test('load-generator expansion requires generator saturation, collector headroom, and offered-load headroom', () => {
  const step = recommendScaleStep(evidence({ generatorCpuSaturated: true }));
  assert.equal(step?.dimension, 'loadgen-max');
  assert.deepEqual(step?.restore, { dimension: 'loadgen-max', before: 2, after: 1 });
  assert.equal(recommendScaleStep(evidence({ generatorCpuSaturated: true, collectorThrottled: true }))?.dimension, 'collector-cpu-limit');
  assert.equal(recommendScaleStep(evidence({ generatorCpuSaturated: true, estimatedCollectorCapacityRowsPerSecond: 1000 })), undefined);
});

test('collector and Kusto scale only on their measured bottleneck signals', () => {
  assert.equal(recommendScaleStep(evidence({ collectorThrottled: true }))?.dimension, 'collector-cpu-limit');
  assert.equal(recommendScaleStep(evidence({ collectorMemorySaturated: true }))?.dimension, 'collector-memory-limit');
  assert.equal(recommendScaleStep(evidence({ qapBacklogGrowing: true, committedRateStable: true }))?.dimension, undefined);
  const kusto = recommendScaleStep(evidence({ qapBacklogGrowing: true, committedRateStable: true, kustoSaturated: true }));
  assert.equal(kusto?.dimension, 'kusto-capacity');
  assert.deepEqual(kusto?.restore, { dimension: 'kusto-capacity', before: 3, after: 2 });
});

test('one-step trials revert without throughput gain and stop after two ineffective increments', async () => {
  const step = recommendScaleStep(evidence({ generatorNetworkSaturated: true }))!;
  assert.equal(assessScaleTrial(step, 1000).outcome, 'revert');
  assert.equal(assessScaleTrial(step, 1000, 1).outcome, 'stop');
  const applied: ScaleTarget[] = [];
  const result = await executeScaleTrial(step, { apply: async target => { applied.push(target); },
    measureCommittedRowsPerSecond: async () => 1000 });
  assert.equal(result.outcome, 'revert');
  assert.deepEqual(applied.map(target => target.after), [2, 1]);
});

test('scale input parser rejects unknown and nonreversible target shapes', () => {
  assert.throws(() => validateScaleTarget({ dimension: 'unvalidated', before: 1, after: 2 }), /invalid/);
  assert.throws(() => assessScaleTrial({ dimension: 'kusto-capacity', before: 2, after: 3,
    restore: { dimension: 'kusto-capacity', before: 3, after: 4 }, evidence: ['saturated'], baselineCommittedRowsPerSecond: 1 }, 2), /reversible/);
});

test('safe idle requires stable QAP, healthy pods, preserved rows, and recorded AKS/Kusto baselines', () => {
  assert.deepEqual(verifySafeIdle(safeIdle()), { safe: true, failures: [] });
  const result = verifySafeIdle(safeIdle({ systemPoolCount: 2, qapPendingBacklog: 1,
    previousQapPendingBacklog: 0, deltaRowsAfter: 9, pods: [{ name: 'system', ready: false, scheduled: true }] }));
  assert.equal(result.safe, false);
  assert.deepEqual(result.failures, ['stamp-pods-not-ready-and-scheduled', 'qap-backlog-growing', 'delta-row-count-changed']);
});
