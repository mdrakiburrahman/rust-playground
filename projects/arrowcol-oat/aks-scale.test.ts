import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { assessScaleTrial, executeScaleTrial, main as scaleMain, readJsonFile, recommendScaleStep, validateScaleTarget,
  waitForScaleMeasurement, type ScaleEvidence, type ScaleEvidenceVerifier, type ScaleTarget } from './aks-scale.js';
import { newCampaignState } from './aks-campaign.js';
import { verifySafeIdle, ensureSafeIdle, type SafeIdleObservation } from './aks-reconcile.js';

function evidence(overrides: Partial<ScaleEvidence> = {}): ScaleEvidence {
  return {
    committedRowsPerSecond: 1000, offeredRowsPerSecond: 1200, estimatedCollectorCapacityRowsPerSecond: 2000,
    generatorCpuSaturated: false, generatorNetworkSaturated: false, collectorCpuSaturated: false,
    collectorThrottled: false, collectorMemorySaturated: false, collectorPlacementBlocked: false, collectorNodePoolAtMaximum: false,
    qapBacklogGrowing: false, committedRateStable: false, qapPendingBacklog: 0, previousQapPendingBacklog: 0,
    kustoSaturated: false,
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

test('scale JSON reader admits the exact byte bound and rejects overflow or truncated input', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'oat-scale-evidence-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const file = join(directory, 'evidence.json'), value = 'x'.repeat(262_142);
  await writeFile(file, JSON.stringify(value));
  assert.equal(await readJsonFile(file), value);
  await writeFile(file, JSON.stringify(value + 'x'));
  await assert.rejects(readJsonFile(file), /fixed size bound/);
  await writeFile(file, '{"truncated":');
  await assert.rejects(readJsonFile(file), /not valid JSON/);
});

test('real scale CLI refuses self-labeled telemetry before configuration, storage, or command access', async () => {
  let commandCalls = 0;
  await assert.rejects(scaleMain(['22222222-2222-4222-8222-222222222222', '1', '--env-file', 'missing.env',
    '--stamp-config', 'missing.yaml', '--runtime-config', 'missing.json', '--evidence-file', 'evidence.json',
    '--measurement-file', 'measurement.json'], async () => { commandCalls++; throw new Error('must not run'); }),
  /no trusted initial and post-scale evidence verifier/);
  assert.equal(commandCalls, 0);
});

test('UNKNOWN dimensions do not become false or block an independent measured bottleneck', () => {
  const partial = { qapPendingBacklog: null, previousQapPendingBacklog: null, qapBacklogGrowing: null,
    kustoSaturated: null, estimatedCollectorCapacityRowsPerSecond: null };
  const cpu = recommendScaleStep(evidence({ ...partial, collectorThrottled: true }));
  assert.equal(cpu?.dimension, 'collector-cpu-limit');
  assert.equal(cpu?.baselineQapPendingBacklog, null);
  assert.equal(assessScaleTrial(cpu!, { committedRowsPerSecond: 1100, qapPendingBacklog: null }).outcome, 'keep');
  assert.equal(recommendScaleStep(evidence({ ...partial, generatorCpuSaturated: true })), undefined);
  assert.equal(recommendScaleStep(evidence({ generatorCpuSaturated: true, collectorThrottled: null })), undefined);
  const kusto = recommendScaleStep(evidence({ collectorCpuSaturated: null, collectorThrottled: null,
    collectorMemorySaturated: null, qapBacklogGrowing: true, committedRateStable: true, kustoSaturated: true,
    qapPendingBacklog: 20, previousQapPendingBacklog: 10 }));
  assert.equal(kusto?.dimension, 'kusto-capacity');
  assert.throws(() => assessScaleTrial(kusto!, { committedRowsPerSecond: 1000, qapPendingBacklog: null }), /UNKNOWN/);
});

test('production measurement wait verifies a complete original post-convergence window and rechecks its deadline', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'oat-post-scale-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  let now = 1_800_000_000_000;
  t.mock.method(Date, 'now', () => now);
  const state = newCampaignState('c0ffee00-0000-4000-8000-000000000001', 'c0ffee00-0000-4000-8000-000000000002', now - 120_000);
  const step = () => ({ state: 'not_started' as const, deadline_unix_ms: null, reason_code: null });
  state.runInventory = { schema_version: 1, campaign_id: state.campaignId, owner_id: state.ownerId, fencing_token: 1,
    orchestration_run_id: 'c0ffee00-0000-4000-8000-000000000004', cell_run_id: 'c0ffee00-0000-4000-8000-000000000003',
    load_admission: 'authorized', namespace: { name: 'synthetic-offline', uid: null, owner_token: state.ownerId },
    resources: [], fic: null, qap: null,
    cleanup: { stop_admission: step(), bounded_drain: step(), evidence_capture: step(), stop_maintenance: step(), terminal_capture: step(), handoff: step() } };
  const file = join(directory, 'measurement.json'), deadline = now + 60_000, notBefore = now - 60_000;
  const input = { schema_version: 2, campaign_id: state.campaignId, cell_run_id: state.runInventory.cell_run_id,
    observed_at_ms: now, window_ms: 30_000, measurement: { committedRowsPerSecond: 999, qapPendingBacklog: 1 } };
  let calls = 0, expire = false, fenced = false;
  const verifier: ScaleEvidenceVerifier = {
    verifyInitial: async () => { throw new Error('not an initial request'); },
    verifyMeasurement: async (_envelope, _state, window) => {
      calls++;
      assert.equal(window.deadlineAtMs, deadline);
      assert.equal(window.notBeforeMs, notBefore);
      assert.equal(window.startMs, input.observed_at_ms - input.window_ms);
      if (fenced) throw new Error('authenticated custody changed');
      if (expire) now = deadline;
      return { committedRowsPerSecond: 1200, qapPendingBacklog: null };
    },
  };
  await writeFile(file, JSON.stringify(input));
  assert.deepEqual(await waitForScaleMeasurement(file, state, notBefore, deadline, verifier),
    { committedRowsPerSecond: 1200, qapPendingBacklog: null });
  assert.equal(calls, 1);
  for (const invalid of [{ ...input, sources: ['committed-window', 'qap'] },
    { ...input, cell_run_id: 'c0ffee00-0000-4000-8000-000000000099' },
    { ...input, window_ms: 90_000 }, { ...input, window_ms: 29_999 }, { ...input, observed_at_ms: now + 1 }]) {
    await writeFile(file, JSON.stringify(invalid));
    await assert.rejects(waitForScaleMeasurement(file, state, notBefore, deadline, verifier), /window|envelope|campaign\/run/);
  }
  assert.equal(calls, 1);
  await writeFile(file, JSON.stringify(input));
  fenced = true;
  await assert.rejects(waitForScaleMeasurement(file, state, notBefore, deadline, verifier), /custody changed/);
  fenced = false;
  expire = true;
  await assert.rejects(waitForScaleMeasurement(file, state, notBefore, deadline, verifier), /original deadline/);
  const expiredCalls = calls;
  await assert.rejects(waitForScaleMeasurement(file, state, notBefore, deadline, verifier), /bounded trial deadline/);
  assert.equal(calls, expiredCalls);
});

test('load-generator expansion requires generator saturation, collector headroom, and offered-load headroom', () => {
  const step = recommendScaleStep(evidence({ generatorCpuSaturated: true }));
  assert.equal(step?.dimension, 'loadgen-max');
  assert.deepEqual(step?.restore, { dimension: 'loadgen-max', before: 2, after: 1 });
  assert.equal(recommendScaleStep(evidence({ generatorCpuSaturated: true, collectorThrottled: true }))?.dimension, 'collector-cpu-limit');
  assert.equal(recommendScaleStep(evidence({ generatorCpuSaturated: true, estimatedCollectorCapacityRowsPerSecond: 1000 })), undefined);
  assert.equal(recommendScaleStep(evidence({ collectorCpuSaturated: true, collectorCpuLimitCores: 8,
    collectorPlacementBlocked: false }))?.dimension, undefined);
  assert.equal(recommendScaleStep(evidence({ collectorPlacementBlocked: true }))?.dimension, 'collector-node-max');
  assert.equal(recommendScaleStep(evidence({ unschedulablePods: 1 }))?.dimension, undefined);
  assert.throws(() => recommendScaleStep(evidence({ loadgenMax: 1.5 })), /numeric measurements/);
});

test('collector and Kusto scale only on their measured bottleneck signals', () => {
  assert.equal(recommendScaleStep(evidence({ collectorThrottled: true }))?.dimension, 'collector-cpu-limit');
  assert.equal(recommendScaleStep(evidence({ collectorMemorySaturated: true }))?.dimension, 'collector-memory-limit');
  assert.equal(recommendScaleStep(evidence({ qapBacklogGrowing: true, committedRateStable: true }))?.dimension, undefined);
  const kusto = recommendScaleStep(evidence({ qapBacklogGrowing: true, committedRateStable: true, kustoSaturated: true,
    qapPendingBacklog: 12 }));
  assert.equal(kusto?.dimension, 'kusto-capacity');
  assert.deepEqual(kusto?.restore, { dimension: 'kusto-capacity', before: 3, after: 2 });
});

test('scale trials persist intent and restore only after exact target readback', async () => {
  const step = recommendScaleStep(evidence({ generatorNetworkSaturated: true }))!;
  const token = { ownerId: 'c0ffee00-0000-4000-8000-000000000002', fencingToken: 2 };
  const sequence: string[] = [];
  const applied: ScaleTarget[] = [];
  let current = step.before;
  const adapter = {
    assertCustody: async (actual: typeof token) => { assert.equal(actual, token); sequence.push('assert'); },
    persistIntent: async (target: ScaleTarget) => { sequence.push('intent:' + target.after); },
    apply: async (target: ScaleTarget) => { applied.push(target); current = target.after; sequence.push('apply:' + target.after); },
    reconcile: async (target: ScaleTarget) => current === target.before ? 'before' as const :
      current === target.after ? 'after' as const : 'unknown' as const,
    measure: async () => { throw new Error('measurement failed'); },
  };
  await assert.rejects(executeScaleTrial(step, adapter, token), /measurement failed/);
  assert.deepEqual(applied.map(target => target.after), [2, 1]);
  assert.equal(current, 1);
  assert.ok(sequence.indexOf('intent:2') < sequence.indexOf('apply:2'));
  assert.ok(sequence.indexOf('intent:1') < sequence.indexOf('apply:1'));
});

test('scale restoration is attempted when measurement setup fails after an exact apply', async () => {
  const step = recommendScaleStep(evidence({ generatorCpuSaturated: true }))!;
  const token = { ownerId: 'c0ffee00-0000-4000-8000-000000000002', fencingToken: 2 };
  const applied: ScaleTarget[] = [];
  let current = step.before;
  const adapter = {
    assertCustody: async () => {}, persistIntent: async () => {},
    apply: async (target: ScaleTarget) => { applied.push(target); current = target.after; },
    reconcile: async (target: ScaleTarget) => current === target.before ? 'before' as const :
      current === target.after ? 'after' as const : 'unknown' as const,
    beginMeasurement: async () => { throw new Error('measurement setup failed'); },
    measure: async () => ({ committedRowsPerSecond: 1000, qapPendingBacklog: 0 }),
  };
  await assert.rejects(executeScaleTrial(step, adapter, token), /measurement setup failed/);
  assert.deepEqual(applied.map(target => target.after), [step.after, step.restore.after]);
  assert.equal(current, step.before);
});

test('an uncertain scale application is neither replayed nor rolled back', async () => {
  const step = recommendScaleStep(evidence({ generatorCpuSaturated: true }))!;
  const token = { ownerId: 'c0ffee00-0000-4000-8000-000000000002', fencingToken: 2 };
  const applied: ScaleTarget[] = [];
  let observations = 0;
  const adapter = {
    assertCustody: async () => {}, persistIntent: async () => {},
    apply: async (target: ScaleTarget) => { applied.push(target); throw new Error('lost response'); },
    reconcile: async () => observations++ === 0 ? 'before' as const : 'unknown' as const,
    measure: async () => ({ committedRowsPerSecond: 1000, qapPendingBacklog: 0 }),
  };
  await assert.rejects(executeScaleTrial(step, adapter, token), /refusing to replay or roll back/);
  assert.deepEqual(applied.map(target => target.after), [2]);
});

test('a lost apply response with exact after-state is measured without replay', async () => {
  const step = recommendScaleStep(evidence({ generatorNetworkSaturated: true }))!;
  const token = { ownerId: 'c0ffee00-0000-4000-8000-000000000002', fencingToken: 2 };
  let current = step.before;
  const applied: ScaleTarget[] = [];
  const adapter = {
    assertCustody: async () => {}, persistIntent: async () => {},
    apply: async (target: ScaleTarget) => { applied.push(target); current = target.after; throw new Error('lost response'); },
    reconcile: async (target: ScaleTarget) => current === target.before ? 'before' as const :
      current === target.after ? 'after' as const : 'unknown' as const,
    measure: async () => ({ committedRowsPerSecond: 1100, qapPendingBacklog: 0 }),
  };
  assert.equal((await executeScaleTrial(step, adapter, token)).outcome, 'keep');
  assert.deepEqual(applied.map(target => target.after), [2]);
});

test('an uncertain restore response is reconciled instead of replayed', async () => {
  const step = recommendScaleStep(evidence({ generatorCpuSaturated: true }))!;
  const token = { ownerId: 'c0ffee00-0000-4000-8000-000000000002', fencingToken: 2 };
  let current = step.before;
  const applied: ScaleTarget[] = [];
  const adapter = {
    assertCustody: async () => {}, persistIntent: async () => {},
    apply: async (target: ScaleTarget) => {
      applied.push(target); current = target.after;
      if (target === step.restore) throw new Error('restore response lost');
    },
    reconcile: async (target: ScaleTarget) => current === target.before ? 'before' as const :
      current === target.after ? 'after' as const : 'unknown' as const,
    measure: async () => ({ committedRowsPerSecond: 1000, qapPendingBacklog: 0 }),
  };
  assert.equal((await executeScaleTrial(step, adapter, token)).outcome, 'revert');
  assert.deepEqual(applied.map(target => target.after), [2, 1]);
});

test('Kusto trials keep QAP improvement at stable ingress and revert without it', () => {
  const step = recommendScaleStep(evidence({ qapBacklogGrowing: true, committedRateStable: true, kustoSaturated: true,
    qapPendingBacklog: 20 }))!;
  assert.equal(assessScaleTrial(step, { committedRowsPerSecond: 1000, qapPendingBacklog: 19 }).outcome, 'keep');
  assert.equal(assessScaleTrial(step, { committedRowsPerSecond: 950, qapPendingBacklog: 19 }).outcome, 'keep');
  assert.equal(assessScaleTrial(step, { committedRowsPerSecond: 1050, qapPendingBacklog: 19 }).outcome, 'keep');
  assert.equal(assessScaleTrial(step, { committedRowsPerSecond: 949, qapPendingBacklog: 10 }).outcome, 'revert');
  assert.equal(assessScaleTrial(step, { committedRowsPerSecond: 1051, qapPendingBacklog: 10 }).outcome, 'revert');
  assert.equal(assessScaleTrial(step, { committedRowsPerSecond: 1000, qapPendingBacklog: 20 }).outcome, 'revert');
  assert.equal(assessScaleTrial(step, { committedRowsPerSecond: 1000, qapPendingBacklog: 21 }, 1).outcome, 'stop');
});

test('scale input parser rejects unknown and nonreversible target shapes', () => {
  assert.throws(() => validateScaleTarget({ dimension: 'unvalidated', before: 1, after: 2 }), /invalid/);
  assert.throws(() => validateScaleTarget({ dimension: 'loadgen-max', before: 1.5, after: 2 }), /safe integers/);
  assert.throws(() => assessScaleTrial({ dimension: 'kusto-capacity', before: 2, after: 3,
    restore: { dimension: 'kusto-capacity', before: 3, after: 4 }, evidence: ['saturated'], baselineCommittedRowsPerSecond: 1,
    baselineQapPendingBacklog: 1 }, { committedRowsPerSecond: 2, qapPendingBacklog: 0 }), /reversible/);
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
