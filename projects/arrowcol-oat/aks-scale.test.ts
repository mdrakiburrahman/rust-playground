import assert from 'node:assert/strict';
import { test } from 'node:test';
import { assessScaleTrial, executeScaleTrial, recommendScaleStep, validateScaleTarget, type ScaleEvidence, type ScaleTarget } from './aks-scale.js';
import { verifySafeIdle, ensureSafeIdle, type SafeIdleObservation } from './aks-reconcile.js';

function evidence(overrides: Partial<ScaleEvidence> = {}): ScaleEvidence {
  return {
    committedRowsPerSecond: 1000, offeredRowsPerSecond: 1200, estimatedCollectorCapacityRowsPerSecond: 2000,
    generatorCpuSaturated: false, generatorNetworkSaturated: false, collectorCpuSaturated: false,
    collectorThrottled: false, collectorMemorySaturated: false, collectorPlacementBlocked: false, collectorNodePoolAtMaximum: false,
    qapBacklogGrowing: false, committedRateStable: false, qapPendingBacklog: 0, previousQapPendingBacklog: 0,
    qapP95LatencyMs: 0, previousQapP95LatencyMs: 0, kustoSaturated: false,
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
  assert.equal(recommendScaleStep(evidence({ collectorCpuSaturated: true, collectorCpuLimitCores: 8,
    collectorPlacementBlocked: false }))?.dimension, undefined);
  assert.equal(recommendScaleStep(evidence({ collectorPlacementBlocked: true }))?.dimension, 'collector-node-max');
  assert.throws(() => recommendScaleStep(evidence({ loadgenMax: 1.5 })), /numeric measurements/);
});

test('collector and Kusto scale only on their measured bottleneck signals', () => {
  assert.equal(recommendScaleStep(evidence({ collectorThrottled: true }))?.dimension, 'collector-cpu-limit');
  assert.equal(recommendScaleStep(evidence({ collectorMemorySaturated: true }))?.dimension, 'collector-memory-limit');
  assert.equal(recommendScaleStep(evidence({ qapBacklogGrowing: true, committedRateStable: true }))?.dimension, undefined);
  const kusto = recommendScaleStep(evidence({ qapBacklogGrowing: true, committedRateStable: true, kustoSaturated: true,
    qapPendingBacklog: 12, qapP95LatencyMs: 80 }));
  assert.equal(kusto?.dimension, 'kusto-capacity');
  assert.deepEqual(kusto?.restore, { dimension: 'kusto-capacity', before: 3, after: 2 });
});

test('scale trials persist custody-fenced intent before mutation and restore when measurement fails', async () => {
  const step = recommendScaleStep(evidence({ generatorNetworkSaturated: true }))!;
  const token = { ownerId: 'c0ffee00-0000-4000-8000-000000000002', fencingToken: 2 };
  const sequence: string[] = [];
  const applied: ScaleTarget[] = [];
  const adapter = {
    assertCustody: async (actual: typeof token) => { assert.equal(actual, token); sequence.push('assert'); },
    persistIntent: async (target: ScaleTarget) => { sequence.push('intent:' + target.after); },
    apply: async (target: ScaleTarget) => { applied.push(target); sequence.push('apply:' + target.after); },
    measure: async () => { throw new Error('measurement failed'); },
  };
  await assert.rejects(executeScaleTrial(step, adapter, token), /measurement failed/);
  assert.deepEqual(applied.map(target => target.after), [2, 1]);
  assert.deepEqual(sequence, ['assert', 'intent:2', 'apply:2', 'assert', 'intent:1', 'apply:1']);
});

test('Kusto trials keep QAP improvement at stable ingress and revert without it', () => {
  const step = recommendScaleStep(evidence({ qapBacklogGrowing: true, committedRateStable: true, kustoSaturated: true,
    qapPendingBacklog: 20, qapP95LatencyMs: 100 }))!;
  assert.equal(assessScaleTrial(step, { committedRowsPerSecond: 1000, qapPendingBacklog: 18, qapP95LatencyMs: 94 }).outcome, 'keep');
  assert.equal(assessScaleTrial(step, { committedRowsPerSecond: 900, qapPendingBacklog: 10, qapP95LatencyMs: 80 }).outcome, 'revert');
  assert.equal(assessScaleTrial(step, { committedRowsPerSecond: 1000, qapPendingBacklog: 19, qapP95LatencyMs: 110 }).outcome, 'revert');
});

test('scale input parser rejects unknown and nonreversible target shapes', () => {
  assert.throws(() => validateScaleTarget({ dimension: 'unvalidated', before: 1, after: 2 }), /invalid/);
  assert.throws(() => validateScaleTarget({ dimension: 'loadgen-max', before: 1.5, after: 2 }), /integers/);
  assert.throws(() => assessScaleTrial({ dimension: 'kusto-capacity', before: 2, after: 3,
    restore: { dimension: 'kusto-capacity', before: 3, after: 4 }, evidence: ['saturated'], baselineCommittedRowsPerSecond: 1,
    baselineQapPendingBacklog: 1, baselineQapP95LatencyMs: 1 }, { committedRowsPerSecond: 2, qapPendingBacklog: 0, qapP95LatencyMs: 0 }), /reversible/);
});

test('safe idle requires stable QAP, healthy pods, preserved rows, and recorded AKS/Kusto baselines', () => {
  assert.deepEqual(verifySafeIdle(safeIdle()), { safe: true, failures: [] });
  const result = verifySafeIdle(safeIdle({ systemPoolCount: 2, qapPendingBacklog: 1,
    previousQapPendingBacklog: 0, deltaRowsAfter: 9, pods: [{ name: 'system', ready: false, scheduled: true }] }));
  assert.equal(result.safe, false);
  assert.deepEqual(result.failures, ['stamp-pods-not-ready-and-scheduled', 'qap-backlog-growing', 'delta-row-count-changed']);
  assert.ok(verifySafeIdle(safeIdle({ qapPendingBacklog: Number.NaN })).failures.includes('safe-idle-measurements-incomplete'));
});

test('safe-idle cleanup is a true no-op when the live state is already safe', async () => {
  let restoreCalls = 0;
  const result = await ensureSafeIdle(async () => safeIdle(), async () => { restoreCalls++; });
  assert.equal(result.status, 'already-safe');
  assert.equal(restoreCalls, 0);
});
