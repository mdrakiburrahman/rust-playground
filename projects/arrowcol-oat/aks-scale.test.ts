import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { assessScaleTrial, executeScaleTrial, main as scaleMain, readJsonFile, recommendScaleStep, validateScaleTarget,
  waitForScaleMeasurement, type ScaleEvidence, type ScaleEvidenceVerifier, type ScaleTarget } from './aks-scale.js';
import { newCampaignState } from './aks-campaign.js';
import { verifySafeIdle, ensureSafeIdle, type SafeIdleObservation } from './aks-reconcile.js';
import { cleanupAksFleet, parseAksNodeInventory, runAksFleet, type AksFleetAdapter, type AksFleetRequest,
  type FleetPoolSnapshot, type FleetScaleResult } from './aks-fleet.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';

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

function fleetFixture(overrides: Partial<AksFleetRequest> = {}): AksFleetRequest {
  return {
    enabled: true, collector: { pool: 'collectorp', sku: 'Standard_D4s_v5', vcpu: 4 },
    clients: [
      { pool: 'clienta', sku: 'Standard_D4s_v5', vcpu: 4 },
      { pool: 'clientb', sku: 'Standard_D4s_v5', vcpu: 4 },
    ],
    collectorSupportReservation: { cpu_millicores: 1_000, memory_bytes: 3 * 1024 ** 3 },
    deadlineAtMs: 100_000, cleanupDeadlineAtMs: 200_000, now: () => 1,
    ...overrides,
  };
}

function fleetNode(name: string, pool: string, arch = 'amd64',
  role = pool === 'collectorp' ? 'arrowcol' : 'client', sku = 'Standard_D4s_v5') {
  return { metadata: { name, labels: { 'kubernetes.azure.com/agentpool': pool, 'kubernetes.io/arch': arch,
    'kubernetes.io/hostname': name, 'arrow-collector/role': role, 'arrow-collector/sku': sku,
    'node.kubernetes.io/instance-type': sku } },
    spec: {}, status: { nodeInfo: { architecture: arch }, allocatable: { cpu: '4', memory: '16Gi' },
      conditions: [{ type: 'Ready', status: 'True' }] } };
}

function fleetAdapter(overrides: Partial<AksFleetAdapter> = {}) {
  const poolNames = ['collectorp', 'clienta', 'clientb', 'unselected'];
  const pools = new Map(poolNames.map(name => [name, {
    name, sku: 'Standard_D4s_v5', vcpu: 4, count: 0, provisioningState: 'Succeeded',
  } satisfies FleetPoolSnapshot]));
  const calls = new Map<string, number>();
  const authority = { mode: 'controller', campaignId: 'c', ownerId: 'o', fencingToken: 1, assertCurrent: async () => {} } as
    CleanupMutationAuthority;
  const adapter: AksFleetAdapter = {
    listUserPools: async () => poolNames,
    readPool: async name => structuredClone(pools.get(name)!),
    scalePool: async (name, count) => {
      calls.set(name, (calls.get(name) ?? 0) + 1);
      pools.get(name)!.count = count;
      pools.get(name)!.provisioningState = 'Succeeded';
      return { state: 'accepted' };
    },
    waitForPool: async name => structuredClone(pools.get(name)!),
    listNodes: async () => ({ items: [...pools.values()].flatMap(pool => Array.from({ length: pool.count }, (_, index) =>
      fleetNode('aks-' + pool.name + '-' + String(index + 1).padStart(6, '0'), pool.name))) }),
    listPods: async () => ({ items: [] }),
    ...overrides,
  };
  return { adapter, authority, pools, calls };
}

test('AKS fleet resolves the pinned amd64 inventory, allows partial client failure, and cleans every user pool', async () => {
  const fixture = fleetFixture(), state = fleetAdapter({
    scalePool: async (name, count) => {
      state.calls.set(name, (state.calls.get(name) ?? 0) + 1);
      if (name === 'clientb' && count === 1) return { state: 'uncertain' };
      state.pools.get(name)!.count = count;
      state.pools.get(name)!.provisioningState = 'Succeeded';
      return { state: 'accepted' };
    },
    listPods: async () => ({ items: ['collectorp', 'clienta'].map(name => ({
      spec: { nodeName: 'aks-' + name + '-000001', containers: [{ resources: { requests: { cpu: '200m', memory: '256Mi' } } }] },
      status: { phase: 'Running' },
    })) }),
  });
  let observedDuringRun: string[] = [];
  const result = await runAksFleet(fixture, state.adapter, state.authority, async (nodes, pools) => {
    observedDuringRun = [...state.pools.values()].filter(pool => pool.count === 1).map(pool => pool.name).sort();
    assert.deepEqual(nodes.map(node => node.pool), ['collectorp', 'clienta']);
    assert.equal(nodes[0]!.architecture, 'amd64');
    assert.equal(nodes[0]!.allocatableCpuMillicores, 4_000);
    assert.equal(nodes[0]!.requestedCpuMillicores, 200);
    assert.equal(nodes[0]!.requestedMemoryBytes, 256 * 1024 ** 2);
    assert.deepEqual(pools.map(pool => pool.state), ['ready', 'ready', 'failed']);
    return 'launched';
  });
  assert.deepEqual(observedDuringRun, ['clienta', 'collectorp']);
  assert.equal(result.resolved, 'launched');
  assert.deepEqual(result.nodes.map(node => node.pool), ['collectorp', 'clienta']);
  assert.equal(result.pools[2]!.reason, 'scale_outcome_uncertain');
  assert.deepEqual([...state.pools.values()].map(pool => pool.count), [0, 0, 0, 0]);
  assert.deepEqual(state.calls.get('clientb'), 1);
  assert.equal(result.cleanup.verified, true);
});

test('AKS fleet retries only an explicit conflict after reconciling the exact zero before-state', async () => {
  const state = fleetAdapter();
  let conflicted = false;
  let scaleUpCalls = 0;
  const originalScale = state.adapter.scalePool;
  state.adapter.scalePool = async (name, count, authority) => {
    if (name === 'clienta' && count === 1) {
      scaleUpCalls++;
      if (!conflicted) {
        conflicted = true;
        state.calls.set(name, (state.calls.get(name) ?? 0) + 1);
        return { state: 'conflict', status: 409, code: 'OperationNotAllowed' };
      }
    }
    return originalScale(name, count, authority);
  };
  const result = await runAksFleet(fleetFixture({ clients: fleetFixture().clients.slice(0, 1) }),
    state.adapter, state.authority, async () => undefined);
  assert.equal(result.pools.find(pool => pool.name === 'clienta')?.state, 'ready');
  assert.equal(scaleUpCalls, 2);
  assert.equal(state.calls.get('clientb'), undefined);
});

test('AKS fleet polls node readiness and admits every ready client node in a multi-node pool', async () => {
  const state = fleetAdapter();
  const listNodes = state.adapter.listNodes;
  let inventoryCalls = 0;
  state.adapter.listNodes = async authority => {
    inventoryCalls++;
    if (inventoryCalls < 3) return { items: [] };
    const inventory = await listNodes(authority) as { items: unknown[] };
    return { items: inventory.items.reverse() };
  };
  const request = fleetFixture({ clients: [{ pool: 'clienta', sku: 'Standard_D4s_v5', vcpu: 4, count: 3 }] });
  let admitted: { node: string; pool: string }[] = [];
  const result = await runAksFleet(request, state.adapter, state.authority, async nodes => {
    admitted = nodes;
    return 'ready';
  });
  assert.equal(result.resolved, 'ready');
  assert.ok(inventoryCalls >= 3);
  assert.deepEqual(admitted.map(node => node.pool), ['collectorp', 'clienta', 'clienta', 'clienta']);
  assert.deepEqual(admitted.filter(node => node.pool === 'clienta').map(node => node.node),
    ['aks-clienta-000001', 'aks-clienta-000002', 'aks-clienta-000003']);
  assert.deepEqual([...state.pools.values()].map(pool => pool.count), [0, 0, 0, 0]);
});

test('AKS fleet rejects nodes with the wrong role or observed VM SKU', async () => {
  const state = fleetAdapter();
  const listNodes = state.adapter.listNodes;
  state.adapter.listNodes = async authority => {
    const inventory = await listNodes(authority) as { items: Array<{ metadata: { labels: Record<string, string> } }> };
    for (const node of inventory.items) {
      if (node.metadata.labels['kubernetes.azure.com/agentpool'] === 'collectorp')
        node.metadata.labels['arrow-collector/role'] = 'client';
      if (node.metadata.labels['kubernetes.azure.com/agentpool'] === 'clienta') {
        node.metadata.labels['arrow-collector/sku'] = 'Standard_D8s_v5';
        node.metadata.labels['node.kubernetes.io/instance-type'] = 'Standard_D8s_v5';
      }
    }
    return inventory;
  };
  let launched = false;
  const result = await runAksFleet(fleetFixture(), state.adapter, state.authority, async () => {
    launched = true;
  });
  assert.equal(result.resolved, null);
  assert.equal(result.pools.find(pool => pool.name === 'collectorp')?.reason,
    'node_architecture_role_sku_or_identity_mismatch');
  assert.equal(result.pools.find(pool => pool.name === 'clienta')?.reason,
    'node_architecture_role_sku_or_identity_mismatch');
  assert.equal(launched, false);
});

test('AKS fleet preserves an allowlisted definite scale rejection reason', async () => {
  const state = fleetAdapter();
  const scale = state.adapter.scalePool;
  state.adapter.scalePool = async (name, count, authority) => {
    if (name === 'clientb' && count === 1) return { state: 'rejected', reason: 'sku_capacity_unavailable' };
    return scale(name, count, authority);
  };
  const result = await runAksFleet(fleetFixture(), state.adapter, state.authority, async () => 'partial');
  assert.equal(result.resolved, 'partial');
  assert.equal(result.pools.find(pool => pool.name === 'clientb')?.state, 'failed');
  assert.equal(result.pools.find(pool => pool.name === 'clientb')?.reason, 'sku_capacity_unavailable');
});

test('AKS fleet sanitizes unexpected scale rejection details', async () => {
  const state = fleetAdapter();
  const scale = state.adapter.scalePool;
  state.adapter.scalePool = async (name, count, authority) => {
    if (name === 'clientb' && count === 1)
      return { state: 'rejected', reason: 'ARM response body with sensitive details' } as unknown as FleetScaleResult;
    return scale(name, count, authority);
  };
  const result = await runAksFleet(fleetFixture(), state.adapter, state.authority, async () => 'partial');
  assert.equal(result.pools.find(pool => pool.name === 'clientb')?.reason, 'request_rejected');
  assert.equal(JSON.stringify(result).includes('sensitive details'), false);
});

test('AKS fleet holds a shared ARM lock until accepted operations settle across more than eight pools', async () => {
  const state = fleetAdapter();
  const clients = Array.from({ length: 9 }, (_, index) => {
    const pool = 'c' + String(index).padStart(3, '0');
    state.pools.set(pool, { name: pool, sku: 'Standard_D4s_v5', vcpu: 4, count: 0, provisioningState: 'Succeeded' });
    return { pool, sku: 'Standard_D4s_v5', vcpu: 4 };
  });
  state.adapter.listUserPools = async () => [...state.pools.keys()];
  const originalScale = state.adapter.scalePool;
  const originalWait = state.adapter.waitForPool;
  let firstScalePool = '', scaleUps = 0;
  let markFirstWaitReached!: () => void;
  let releaseFirstSettlement!: () => void;
  const firstWaitReached = new Promise<void>(resolve => { markFirstWaitReached = resolve; });
  const firstSettlement = new Promise<void>(resolve => { releaseFirstSettlement = resolve; });
  state.adapter.scalePool = async (name, count, authority) => {
    if (count === 1) {
      scaleUps++;
      if (scaleUps === 1) {
        firstScalePool = name;
        return { state: 'accepted' };
      }
    }
    return originalScale(name, count, authority);
  };
  state.adapter.waitForPool = async (name, count, deadlineAtMs, authority) => {
    if (count === 1 && name === firstScalePool) {
      markFirstWaitReached();
      await firstSettlement;
      state.pools.get(name)!.count = 1;
      state.pools.get(name)!.provisioningState = 'Succeeded';
      return structuredClone(state.pools.get(name)!);
    }
    return originalWait(name, count, deadlineAtMs, authority);
  };
  const pending = runAksFleet(fleetFixture({ clients }), state.adapter, state.authority, async () => 'ready');
  await firstWaitReached;
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(scaleUps, 1);
  releaseFirstSettlement();
  const result = await pending;
  assert.equal(result.resolved, 'ready');
  assert.equal(scaleUps, 10);
  assert.equal(result.cleanup.verified, true);
});

test('aborting fleet startup stops later scale-ups and callback but still completes cleanup', async () => {
  const state = fleetAdapter();
  const controller = new AbortController();
  const scale = state.adapter.scalePool;
  const wait = state.adapter.waitForPool;
  let scaleUps = 0, cleanupScaleUps = 0, callbackCalled = false;
  let markWaitEntered!: () => void;
  let releaseSettlement!: () => void;
  const waitEntered = new Promise<void>(resolve => { markWaitEntered = resolve; });
  const settlement = new Promise<void>(resolve => { releaseSettlement = resolve; });
  state.adapter.scalePool = async (name, count, authority) => {
    if (count === 1) {
      scaleUps++;
      state.pools.get(name)!.count = 1;
      state.pools.get(name)!.provisioningState = 'Updating';
      return { state: 'accepted' };
    }
    cleanupScaleUps++;
    return scale(name, count, authority);
  };
  state.adapter.waitForPool = async (name, count, deadlineAtMs, authority) => {
    if (name === 'collectorp' && count === 1) {
      markWaitEntered();
      await settlement;
      state.pools.get(name)!.provisioningState = 'Succeeded';
      return structuredClone(state.pools.get(name)!);
    }
    return wait(name, count, deadlineAtMs, authority);
  };
  const pending = runAksFleet(fleetFixture({ clients: fleetFixture().clients.slice(0, 1), signal: controller.signal }),
    state.adapter, state.authority, async () => {
      callbackCalled = true;
    });
  await waitEntered;
  controller.abort();
  releaseSettlement();
  let rejected = false;
  try { await pending; } catch { rejected = true; }
  assert.equal(rejected, true);
  assert.equal(callbackCalled, false);
  assert.equal(scaleUps, 1);
  assert.equal(cleanupScaleUps, 1);
  assert.deepEqual([...state.pools.values()].map(pool => pool.count), [0, 0, 0, 0]);
  assert.deepEqual(parseAksNodeInventory(await state.adapter.listNodes(state.authority)), []);
});

test('AKS fleet returns an explicit null collector result without admitting a non-amd64 node', async () => {
  const state = fleetAdapter();
  state.adapter.listNodes = async () => ({ items: [...state.pools.values()].filter(pool => pool.count === 1)
    .map(pool => fleetNode('node-' + pool.name, pool.name, pool.name === 'collectorp' ? 'arm64' : 'amd64')) });
  let launched = false;
  const result = await runAksFleet(fleetFixture({ clients: fleetFixture().clients.slice(0, 1) }),
    state.adapter, state.authority, async () => { launched = true; });
  assert.equal(result.resolved, null);
  assert.equal(result.pools[0]!.state, 'failed');
  assert.equal(result.pools[0]!.reason, 'node_architecture_role_sku_or_identity_mismatch');
  assert.equal(launched, false);
  assert.deepEqual([...state.pools.values()].map(pool => pool.count), [0, 0, 0, 0]);
});

test('AKS fleet kill switch rejects before custody or cloud effects and parses actual node allocatable inventory', async () => {
  const state = fleetAdapter();
  let custodyChecks = 0, cloudCalls = 0;
  state.authority.assertCurrent = async () => { custodyChecks++; };
  state.adapter.listUserPools = async () => { cloudCalls++; return []; };
  await assert.rejects(runAksFleet(fleetFixture({ enabled: false }), state.adapter, state.authority, async () => {}),
    /explicit fleet CLI switch/);
  assert.equal(custodyChecks, 0);
  assert.equal(cloudCalls, 0);
  assert.deepEqual(parseAksNodeInventory({ items: [fleetNode('node1', 'pool1')] }), [{
    node: 'node1', pool: 'pool1', role: 'client', observedSku: 'Standard_D4s_v5', skuLabelsConsistent: true,
    architecture: 'amd64', ready: true, amd64: true, placementLabelsValid: true,
    allocatable_cpu_millicores: 4_000, allocatable_memory_bytes: 16 * 1024 ** 3,
    requested_cpu_millicores: 0, requested_memory_bytes: 0,
  }]);
});

test('fleet callback failure still discovers and cleans late user pools', async () => {
  const state = fleetAdapter();
  state.pools.set('latepool', { name: 'latepool', sku: 'Standard_D4s_v5', vcpu: 4, count: 1, provisioningState: 'Succeeded' });
  let discoveryCalls = 0;
  const listUserPools = state.adapter.listUserPools;
  state.adapter.listUserPools = async authority => {
    discoveryCalls++;
    const names = await listUserPools(authority);
    return discoveryCalls === 1 ? names : [...names, 'latepool'];
  };
  await assert.rejects(runAksFleet(fleetFixture({ clients: fleetFixture().clients.slice(0, 1) }),
    state.adapter, state.authority, async () => { throw new Error('callback failed'); }), /callback failed/);
  assert.equal(discoveryCalls, 2);
  assert.equal(state.pools.get('latepool')?.count, 0);
});

test('a falsy callback throw is not converted into a success-shaped fleet result', async () => {
  const state = fleetAdapter();
  let completed = false;
  try {
    await runAksFleet(fleetFixture({ clients: fleetFixture().clients.slice(0, 1) }),
      state.adapter, state.authority, async () => { throw undefined; });
    completed = true;
  } catch (error) {
    assert.equal(error, undefined);
  }
  assert.equal(completed, false);
  assert.deepEqual([...state.pools.values()].map(pool => pool.count), [0, 0, 0, 0]);
});

test('cleanupAksFleet independently re-lists and verifies all user pools', async () => {
  const state = fleetAdapter();
  state.pools.get('unselected')!.count = 1;
  const result = await cleanupAksFleet(state.adapter, state.authority, { deadlineAtMs: 200_000, now: () => 1 });
  assert.deepEqual(result, { verified: true, failed: [] });
  assert.equal(state.pools.get('unselected')!.count, 0);
});

test('uncertain cleanup waits for an updating old-count pool without redispatch', async () => {
  const state = fleetAdapter();
  state.pools.get('unselected')!.count = 2;
  let scaleCalls = 0, waitsForZero = 0;
  const scale = state.adapter.scalePool;
  state.adapter.scalePool = async (name, count, authority) => {
    if (name === 'unselected' && count === 0) {
      scaleCalls++;
      state.pools.get(name)!.provisioningState = 'Updating';
      return { state: 'uncertain' };
    }
    return scale(name, count, authority);
  };
  const wait = state.adapter.waitForPool;
  state.adapter.waitForPool = async (name, count, deadlineAtMs, authority) => {
    if (name === 'unselected' && count === 0) {
      waitsForZero++;
      state.pools.get(name)!.count = 0;
      state.pools.get(name)!.provisioningState = 'Succeeded';
      return structuredClone(state.pools.get(name)!);
    }
    return wait(name, count, deadlineAtMs, authority);
  };
  const result = await cleanupAksFleet(state.adapter, state.authority, { deadlineAtMs: 200_000, now: () => 1 });
  assert.equal(result.verified, true);
  assert.equal(scaleCalls, 1);
  assert.equal(waitsForZero, 1);
});

test('cleanup repairs a failed zero-count pool and verifies the succeeded zero state', async () => {
  const state = fleetAdapter();
  state.pools.get('unselected')!.provisioningState = 'Failed';
  let repairCalls = 0;
  const scale = state.adapter.scalePool;
  state.adapter.scalePool = async (name, count, authority) => {
    if (name === 'unselected' && count === 0) {
      repairCalls++;
      state.pools.get(name)!.provisioningState = 'Succeeded';
      return { state: 'accepted' };
    }
    return scale(name, count, authority);
  };
  const result = await cleanupAksFleet(state.adapter, state.authority, { deadlineAtMs: 200_000, now: () => 1 });
  assert.deepEqual(result, { verified: true, failed: [] });
  assert.equal(repairCalls, 1);
  assert.equal(state.pools.get('unselected')!.provisioningState, 'Succeeded');
});

test('cleanup does not verify a failed zero-count repair', async () => {
  const state = fleetAdapter();
  state.pools.get('unselected')!.provisioningState = 'Failed';
  let repairCalls = 0;
  state.adapter.scalePool = async (name, count) => {
    if (name === 'unselected' && count === 0) {
      repairCalls++;
      return { state: 'accepted' };
    }
    return { state: 'accepted' };
  };
  const result = await cleanupAksFleet(state.adapter, state.authority, { deadlineAtMs: 200_000, now: () => 1 });
  assert.deepEqual(result, { verified: false, failed: ['unselected'] });
  assert.equal(repairCalls, 1);
  assert.equal(state.pools.get('unselected')!.provisioningState, 'Failed');
});

test('cleanup waits for an approved pending scale-up target before scaling the pool back to zero', async () => {
  const state = fleetAdapter();
  const scale = state.adapter.scalePool;
  state.adapter.scalePool = async (name, count, authority) => {
    if (name === 'collectorp' && count === 1) {
      state.pools.get(name)!.provisioningState = 'Updating';
      return { state: 'accepted' };
    }
    return scale(name, count, authority);
  };
  const wait = state.adapter.waitForPool;
  let collectorTargetWaits = 0;
  state.adapter.waitForPool = async (name, count, deadlineAtMs, authority) => {
    if (name === 'collectorp' && count === 1) {
      collectorTargetWaits++;
      if (collectorTargetWaits === 1) throw new Error('scale settlement response interrupted');
      state.pools.get(name)!.count = 1;
      state.pools.get(name)!.provisioningState = 'Succeeded';
      return structuredClone(state.pools.get(name)!);
    }
    return wait(name, count, deadlineAtMs, authority);
  };
  const result = await runAksFleet(fleetFixture({ clients: fleetFixture().clients.slice(0, 1) }),
    state.adapter, state.authority, async () => 'should-not-run');
  assert.equal(result.resolved, null);
  assert.equal(result.cleanup.verified, true);
  assert.equal(collectorTargetWaits, 2);
  assert.equal(state.pools.get('collectorp')!.count, 0);
});

test('fleet cleanup hard-stops when custody authority is lost', async () => {
  const state = fleetAdapter();
  let lost = false, cleanupMutations = 0;
  state.authority.assertCurrent = async () => { if (lost) throw new Error('custody authority lost'); };
  const scale = state.adapter.scalePool;
  state.adapter.scalePool = async (name, count, authority) => {
    if (count === 0) cleanupMutations++;
    return scale(name, count, authority);
  };
  await assert.rejects(runAksFleet(fleetFixture({ clients: fleetFixture().clients.slice(0, 1) }),
    state.adapter, state.authority, async () => { lost = true; }), /custody authority lost/);
  assert.equal(cleanupMutations, 0);
});
