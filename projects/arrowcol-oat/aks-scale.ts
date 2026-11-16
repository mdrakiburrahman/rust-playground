export type ScaleDimension = 'loadgen-max' | 'collector-cpu-limit' | 'collector-memory-limit' |
  'collector-node-max' | 'kusto-capacity' | 'kusto-sku' | 'system-pool-count';
export interface ScaleTarget { dimension: ScaleDimension; before: number | string; after: number | string }
export interface ScaleStep extends ScaleTarget {
  evidence: string[];
  restore: ScaleTarget;
  baselineCommittedRowsPerSecond: number;
  baselineQapPendingBacklog: number;
  baselineQapP95LatencyMs: number;
}
export interface ScaleEvidence {
  committedRowsPerSecond: number;
  offeredRowsPerSecond: number;
  estimatedCollectorCapacityRowsPerSecond: number;
  generatorCpuSaturated: boolean;
  generatorNetworkSaturated: boolean;
  collectorCpuSaturated: boolean;
  collectorThrottled: boolean;
  collectorMemorySaturated: boolean;
  collectorPlacementBlocked: boolean;
  collectorNodePoolAtMaximum: boolean;
  qapBacklogGrowing: boolean;
  committedRateStable: boolean;
  qapPendingBacklog: number;
  previousQapPendingBacklog: number;
  qapP95LatencyMs: number;
  previousQapP95LatencyMs: number;
  kustoSaturated: boolean;
  loadgenMax: number;
  loadgenMaximum: number;
  collectorCpuLimitCores: number;
  collectorCpuLimitMaximum: number;
  collectorMemoryLimitGiB: number;
  collectorMemoryLimitMaximumGiB: number;
  collectorNodeMax: number;
  collectorNodeMaxMaximum: number;
  kustoCapacity: number;
  kustoCapacityMaximum: number;
  kustoSku: string;
  systemPoolCount: number;
  unschedulablePods: number;
  pdbBlockedDrains: number;
}
export interface ScaleMeasurement {
  committedRowsPerSecond: number;
  qapPendingBacklog: number;
  qapP95LatencyMs: number;
}
export interface ScaleTrialResult {
  outcome: 'keep' | 'revert' | 'stop';
  committedRowsPerSecond: number;
  qapPendingBacklog: number;
  qapP95LatencyMs: number;
  ineffectiveIncrements: number;
  reason: string;
}
export interface ScaleCustodyToken { ownerId: string; fencingToken: number }

function nonnegative(value: number): boolean { return Number.isFinite(value) && value >= 0; }
function target(dimension: ScaleDimension, before: number | string, after: number | string): ScaleTarget {
  return { dimension, before, after };
}
function oneStep(step: ScaleStep): ScaleStep {
  if (!nonnegative(step.baselineCommittedRowsPerSecond) || !Number.isSafeInteger(step.baselineQapPendingBacklog) ||
      step.baselineQapPendingBacklog < 0 || !nonnegative(step.baselineQapP95LatencyMs) ||
      step.before === step.after || step.restore.dimension !== step.dimension || step.restore.before !== step.after ||
      step.restore.after !== step.before) throw new Error('Scale step must be a measured, reversible, single-dimension change.');
  return step;
}
function makeStep(evidence: ScaleEvidence, dimension: ScaleDimension, before: number | string, after: number | string,
  tags: string[]): ScaleStep {
  return oneStep({ ...target(dimension, before, after), restore: target(dimension, after, before), evidence: tags,
    baselineCommittedRowsPerSecond: evidence.committedRowsPerSecond, baselineQapPendingBacklog: evidence.qapPendingBacklog,
    baselineQapP95LatencyMs: evidence.qapP95LatencyMs });
}

/** Select one reversible step only when its corresponding bottleneck is measured. */
export function recommendScaleStep(evidence: ScaleEvidence): ScaleStep | undefined {
  const measurements = [evidence.committedRowsPerSecond, evidence.offeredRowsPerSecond, evidence.estimatedCollectorCapacityRowsPerSecond,
    evidence.collectorCpuLimitCores, evidence.collectorCpuLimitMaximum, evidence.collectorMemoryLimitGiB,
    evidence.collectorMemoryLimitMaximumGiB, evidence.qapP95LatencyMs, evidence.previousQapP95LatencyMs];
  const counts = [evidence.qapPendingBacklog, evidence.previousQapPendingBacklog, evidence.loadgenMax, evidence.loadgenMaximum,
    evidence.collectorNodeMax, evidence.collectorNodeMaxMaximum, evidence.kustoCapacity, evidence.kustoCapacityMaximum,
    evidence.systemPoolCount, evidence.unschedulablePods, evidence.pdbBlockedDrains];
  if (measurements.some(value => !nonnegative(value)) || counts.some(value => !Number.isSafeInteger(value) || value < 0)) {
    throw new Error('Scale evidence has invalid or incomplete numeric measurements.');
  }
  if (typeof evidence.kustoSku !== 'string' || !evidence.kustoSku ||
      [evidence.generatorCpuSaturated, evidence.generatorNetworkSaturated, evidence.collectorCpuSaturated,
        evidence.collectorThrottled, evidence.collectorMemorySaturated, evidence.collectorPlacementBlocked,
        evidence.collectorNodePoolAtMaximum, evidence.qapBacklogGrowing, evidence.committedRateStable, evidence.kustoSaturated]
        .some(value => typeof value !== 'boolean')) throw new Error('Scale evidence has invalid signal values.');

  if (evidence.unschedulablePods > 0 || evidence.pdbBlockedDrains > 0) {
    if (evidence.systemPoolCount < 3) return makeStep(evidence, 'system-pool-count', evidence.systemPoolCount,
      evidence.systemPoolCount + 1, ['system-pod-placement-blocked']);
    return undefined;
  }
  if (evidence.collectorPlacementBlocked && !evidence.collectorNodePoolAtMaximum &&
      evidence.collectorNodeMax < evidence.collectorNodeMaxMaximum) {
    return makeStep(evidence, 'collector-node-max', evidence.collectorNodeMax, evidence.collectorNodeMax + 1,
      ['collector-placement-blocked']);
  }
  if (evidence.collectorCpuSaturated || evidence.collectorThrottled) {
    if (evidence.collectorCpuLimitCores >= evidence.collectorCpuLimitMaximum) return undefined;
    return makeStep(evidence, 'collector-cpu-limit', evidence.collectorCpuLimitCores,
      Math.min(evidence.collectorCpuLimitMaximum, evidence.collectorCpuLimitCores + 1),
      [evidence.collectorThrottled ? 'collector-cpu-throttling' : 'collector-cpu-saturated']);
  }
  if (evidence.collectorMemorySaturated) {
    if (evidence.collectorMemoryLimitGiB >= evidence.collectorMemoryLimitMaximumGiB) return undefined;
    return makeStep(evidence, 'collector-memory-limit', evidence.collectorMemoryLimitGiB,
      Math.min(evidence.collectorMemoryLimitMaximumGiB, evidence.collectorMemoryLimitGiB + 2), ['collector-memory-saturated']);
  }
  const collectorNotSaturated = !evidence.collectorCpuSaturated && !evidence.collectorThrottled && !evidence.collectorMemorySaturated;
  const generatorSaturated = evidence.generatorCpuSaturated || evidence.generatorNetworkSaturated;
  if (collectorNotSaturated && generatorSaturated && evidence.offeredRowsPerSecond < evidence.estimatedCollectorCapacityRowsPerSecond &&
      evidence.loadgenMax < evidence.loadgenMaximum) {
    return makeStep(evidence, 'loadgen-max', evidence.loadgenMax, evidence.loadgenMax + 1,
      [evidence.generatorCpuSaturated ? 'generator-cpu-saturated' : 'generator-network-saturated', 'collector-has-headroom']);
  }
  if (evidence.qapBacklogGrowing && evidence.committedRateStable && evidence.kustoSaturated &&
      evidence.kustoCapacity < evidence.kustoCapacityMaximum) {
    return makeStep(evidence, 'kusto-capacity', evidence.kustoCapacity, evidence.kustoCapacity + 1,
      ['qap-backlog-growing', 'committed-rate-stable', 'kusto-side-saturated']);
  }
  return undefined;
}

function improvedByFivePercent(after: number, before: number): boolean { return before > 0 && after <= before * 0.95; }
function regressedByMoreThanFivePercent(after: number, before: number): boolean { return before === 0 ? after > 0 : after > before * 1.05; }
export function assessScaleTrial(step: ScaleStep, measurement: ScaleMeasurement, previousIneffectiveIncrements = 0): ScaleTrialResult {
  oneStep(step);
  if (!measurement || !nonnegative(measurement.committedRowsPerSecond) || !Number.isSafeInteger(measurement.qapPendingBacklog) ||
      measurement.qapPendingBacklog < 0 || !nonnegative(measurement.qapP95LatencyMs) ||
      !Number.isSafeInteger(previousIneffectiveIncrements) || previousIneffectiveIncrements < 0) {
    throw new Error('Scale trial result is malformed.');
  }
  let targetImproved = measurement.committedRowsPerSecond > step.baselineCommittedRowsPerSecond;
  let reason = 'committed-throughput-improved';
  if (step.dimension === 'kusto-capacity' || step.dimension === 'kusto-sku') {
    const baseline = step.baselineCommittedRowsPerSecond;
    const ingressStable = baseline === 0 ? measurement.committedRowsPerSecond === 0 :
      Math.abs(measurement.committedRowsPerSecond - baseline) <= baseline * 0.05;
    const qapImproved = improvedByFivePercent(measurement.qapPendingBacklog, step.baselineQapPendingBacklog) ||
      improvedByFivePercent(measurement.qapP95LatencyMs, step.baselineQapP95LatencyMs);
    const qapNotRegressed = !regressedByMoreThanFivePercent(measurement.qapPendingBacklog, step.baselineQapPendingBacklog) &&
      !regressedByMoreThanFivePercent(measurement.qapP95LatencyMs, step.baselineQapP95LatencyMs);
    targetImproved = ingressStable && qapImproved && qapNotRegressed;
    reason = targetImproved ? 'qap-pressure-improved-at-stable-ingress' : 'kusto-trial-did-not-improve-qap-pressure';
  }
  if (targetImproved) return { outcome: 'keep', ...measurement, ineffectiveIncrements: 0, reason };
  const ineffectiveIncrements = previousIneffectiveIncrements + 1;
  return { outcome: ineffectiveIncrements >= 2 ? 'stop' : 'revert', ...measurement, ineffectiveIncrements,
    reason: ineffectiveIncrements >= 2 ? 'two-ineffective-increments' : reason };
}

export interface ScaleAdapter {
  assertCustody(token: ScaleCustodyToken): Promise<void>;
  persistIntent(step: ScaleTarget, token: ScaleCustodyToken): Promise<void>;
  apply(step: ScaleTarget, token: ScaleCustodyToken): Promise<void>;
  measure(): Promise<ScaleMeasurement>;
}
export async function executeScaleTrial(step: ScaleStep, adapter: ScaleAdapter, token: ScaleCustodyToken,
  previousIneffectiveIncrements = 0): Promise<ScaleTrialResult> {
  oneStep(step);
  if (!token || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(token.ownerId) ||
      !Number.isSafeInteger(token.fencingToken) || token.fencingToken < 1) throw new Error('Scale custody token is invalid.');
  const restore = async () => {
    await adapter.assertCustody(token);
    await adapter.persistIntent(step.restore, token);
    await adapter.apply(step.restore, token);
  };
  let intentPersisted = false;
  try {
    await adapter.assertCustody(token);
    await adapter.persistIntent(step, token);
    intentPersisted = true;
    await adapter.apply(step, token);
    const result = assessScaleTrial(step, await adapter.measure(), previousIneffectiveIncrements);
    if (result.outcome === 'keep') return result;
    await restore();
    return result;
  } catch (error) {
    if (intentPersisted) {
      try { await restore(); }
      catch (restoreError) {
        throw new AggregateError([error, restoreError], 'Scale trial failed and restoration remains unresolved.');
      }
    }
    throw error;
  }
}

export function validateScaleTarget(value: unknown): ScaleTarget {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Scale target must be an object.');
  const targetValue = value as Record<string, unknown>;
  const allowed: ScaleDimension[] = ['loadgen-max', 'collector-cpu-limit', 'collector-memory-limit', 'collector-node-max',
    'kusto-capacity', 'kusto-sku', 'system-pool-count'];
  if (Object.keys(targetValue).some(key => !['dimension', 'before', 'after'].includes(key)) ||
      typeof targetValue.dimension !== 'string' || !allowed.includes(targetValue.dimension as ScaleDimension) ||
      typeof targetValue.before !== typeof targetValue.after || !['number', 'string'].includes(typeof targetValue.before) ||
      targetValue.before === targetValue.after) throw new Error('Scale target is invalid or has an unsupported dimension.');
  const dimension = targetValue.dimension as ScaleDimension;
  if (typeof targetValue.before === 'number' && (!nonnegative(targetValue.before) || !nonnegative(targetValue.after as number))) {
    throw new Error('Scale target numeric values must be finite and nonnegative.');
  }
  if (['loadgen-max', 'collector-node-max', 'kusto-capacity', 'system-pool-count'].includes(dimension) &&
      (typeof targetValue.before !== 'number' || typeof targetValue.after !== 'number' ||
       !Number.isSafeInteger(targetValue.before) || !Number.isSafeInteger(targetValue.after))) {
    throw new Error('Scale target counts must be nonnegative safe integers.');
  }
  if (dimension === 'kusto-sku' && (typeof targetValue.before !== 'string' || !targetValue.before ||
      typeof targetValue.after !== 'string' || !targetValue.after)) throw new Error('Kusto SKU targets must be nonempty strings.');
  return targetValue as unknown as ScaleTarget;
}
