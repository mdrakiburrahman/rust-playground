export type ScaleDimension = 'loadgen-max' | 'collector-cpu-limit' | 'collector-memory-limit' |
  'collector-node-max' | 'kusto-capacity' | 'kusto-sku' | 'system-pool-count';
export interface ScaleTarget { dimension: ScaleDimension; before: number | string; after: number | string }
export interface ScaleStep extends ScaleTarget {
  evidence: string[];
  restore: ScaleTarget;
  baselineCommittedRowsPerSecond: number;
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
  collectorNodePoolAtMaximum: boolean;
  qapBacklogGrowing: boolean;
  committedRateStable: boolean;
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
export interface ScaleTrialResult {
  outcome: 'keep' | 'revert' | 'stop';
  committedRowsPerSecond: number;
  ineffectiveIncrements: number;
  reason: string;
}

function positive(value: number): boolean { return Number.isFinite(value) && value >= 0; }
function target(dimension: ScaleDimension, before: number | string, after: number | string): ScaleTarget {
  return { dimension, before, after };
}
function oneStep(step: ScaleStep): ScaleStep {
  if (step.baselineCommittedRowsPerSecond < 0 || !Number.isFinite(step.baselineCommittedRowsPerSecond) ||
      step.before === step.after || step.restore.dimension !== step.dimension || step.restore.before !== step.after ||
      step.restore.after !== step.before) throw new Error('Scale step must be a reversible, single-dimension change.');
  if ((step.dimension === 'kusto-capacity' || step.dimension === 'kusto-sku') &&
      (step.restore.dimension !== step.dimension || step.restore.after !== step.before)) {
    throw new Error('Every Kusto scale-up must include its explicit scale-down restore step.');
  }
  return step;
}

/** Select exactly one next step, only where the corresponding bottleneck is measured. */
export function recommendScaleStep(evidence: ScaleEvidence): ScaleStep | undefined {
  const numeric = [evidence.committedRowsPerSecond, evidence.offeredRowsPerSecond, evidence.estimatedCollectorCapacityRowsPerSecond,
    evidence.loadgenMax, evidence.loadgenMaximum, evidence.collectorCpuLimitCores, evidence.collectorCpuLimitMaximum,
    evidence.collectorMemoryLimitGiB, evidence.collectorMemoryLimitMaximumGiB, evidence.collectorNodeMax,
    evidence.collectorNodeMaxMaximum, evidence.kustoCapacity, evidence.kustoCapacityMaximum, evidence.systemPoolCount,
    evidence.unschedulablePods, evidence.pdbBlockedDrains];
  if (numeric.some(value => !positive(value))) throw new Error('Scale evidence has invalid numeric measurements.');
  if (evidence.unschedulablePods > 0 || evidence.pdbBlockedDrains > 0) {
    if (evidence.systemPoolCount < 3) return oneStep({ ...target('system-pool-count', evidence.systemPoolCount, evidence.systemPoolCount + 1),
      restore: target('system-pool-count', evidence.systemPoolCount + 1, evidence.systemPoolCount),
      evidence: ['system-pod-placement-blocked'], baselineCommittedRowsPerSecond: evidence.committedRowsPerSecond });
    return undefined;
  }
  if (evidence.collectorCpuSaturated || evidence.collectorThrottled || evidence.collectorMemorySaturated) {
    if (evidence.collectorCpuSaturated || evidence.collectorThrottled) {
      if (evidence.collectorCpuLimitCores >= evidence.collectorCpuLimitMaximum) {
        if (evidence.collectorNodePoolAtMaximum || evidence.collectorNodeMax >= evidence.collectorNodeMaxMaximum) return undefined;
        return oneStep({ ...target('collector-node-max', evidence.collectorNodeMax, evidence.collectorNodeMax + 1),
          restore: target('collector-node-max', evidence.collectorNodeMax + 1, evidence.collectorNodeMax),
          evidence: ['collector-cpu-or-throttling-saturated'], baselineCommittedRowsPerSecond: evidence.committedRowsPerSecond });
      }
      return oneStep({ ...target('collector-cpu-limit', evidence.collectorCpuLimitCores, evidence.collectorCpuLimitCores + 1),
        restore: target('collector-cpu-limit', evidence.collectorCpuLimitCores + 1, evidence.collectorCpuLimitCores),
        evidence: [evidence.collectorThrottled ? 'collector-cpu-throttling' : 'collector-cpu-saturated'],
        baselineCommittedRowsPerSecond: evidence.committedRowsPerSecond });
    }
    if (evidence.collectorMemoryLimitGiB >= evidence.collectorMemoryLimitMaximumGiB) {
      if (evidence.collectorNodePoolAtMaximum || evidence.collectorNodeMax >= evidence.collectorNodeMaxMaximum) return undefined;
      return oneStep({ ...target('collector-node-max', evidence.collectorNodeMax, evidence.collectorNodeMax + 1),
        restore: target('collector-node-max', evidence.collectorNodeMax + 1, evidence.collectorNodeMax),
        evidence: ['collector-memory-saturated'], baselineCommittedRowsPerSecond: evidence.committedRowsPerSecond });
    }
    return oneStep({ ...target('collector-memory-limit', evidence.collectorMemoryLimitGiB, evidence.collectorMemoryLimitGiB + 2),
      restore: target('collector-memory-limit', evidence.collectorMemoryLimitGiB + 2, evidence.collectorMemoryLimitGiB),
      evidence: ['collector-memory-saturated'], baselineCommittedRowsPerSecond: evidence.committedRowsPerSecond });
  }
  const collectorNotSaturated = !evidence.collectorCpuSaturated && !evidence.collectorThrottled && !evidence.collectorMemorySaturated;
  const generatorSaturated = evidence.generatorCpuSaturated || evidence.generatorNetworkSaturated;
  if (collectorNotSaturated && generatorSaturated && evidence.offeredRowsPerSecond < evidence.estimatedCollectorCapacityRowsPerSecond &&
      evidence.loadgenMax < evidence.loadgenMaximum) {
    return oneStep({ ...target('loadgen-max', evidence.loadgenMax, evidence.loadgenMax + 1),
      restore: target('loadgen-max', evidence.loadgenMax + 1, evidence.loadgenMax),
      evidence: [evidence.generatorCpuSaturated ? 'generator-cpu-saturated' : 'generator-network-saturated', 'collector-has-headroom'],
      baselineCommittedRowsPerSecond: evidence.committedRowsPerSecond });
  }
  if (evidence.qapBacklogGrowing && evidence.committedRateStable && evidence.kustoSaturated &&
      evidence.kustoCapacity < evidence.kustoCapacityMaximum) {
    return oneStep({ ...target('kusto-capacity', evidence.kustoCapacity, evidence.kustoCapacity + 1),
      restore: target('kusto-capacity', evidence.kustoCapacity + 1, evidence.kustoCapacity),
      evidence: ['qap-backlog-growing', 'committed-rate-stable', 'kusto-side-saturated'],
      baselineCommittedRowsPerSecond: evidence.committedRowsPerSecond });
  }
  return undefined;
}

export function assessScaleTrial(step: ScaleStep, committedRowsPerSecond: number, previousIneffectiveIncrements = 0): ScaleTrialResult {
  oneStep(step);
  if (!positive(committedRowsPerSecond) || !Number.isSafeInteger(previousIneffectiveIncrements) || previousIneffectiveIncrements < 0) {
    throw new Error('Scale trial result is malformed.');
  }
  if (committedRowsPerSecond > step.baselineCommittedRowsPerSecond) {
    return { outcome: 'keep', committedRowsPerSecond, ineffectiveIncrements: 0, reason: 'committed-throughput-improved' };
  }
  const ineffectiveIncrements = previousIneffectiveIncrements + 1;
  return { outcome: ineffectiveIncrements >= 2 ? 'stop' : 'revert', committedRowsPerSecond, ineffectiveIncrements,
    reason: ineffectiveIncrements >= 2 ? 'two-ineffective-increments' : 'no-throughput-improvement' };
}

export interface ScaleAdapter {
  apply(step: ScaleTarget): Promise<void>;
  measureCommittedRowsPerSecond(): Promise<number>;
}
export async function executeScaleTrial(step: ScaleStep, adapter: ScaleAdapter, previousIneffectiveIncrements = 0): Promise<ScaleTrialResult> {
  oneStep(step);
  await adapter.apply(step);
  const outcome = assessScaleTrial(step, await adapter.measureCommittedRowsPerSecond(), previousIneffectiveIncrements);
  if (outcome.outcome !== 'keep') await adapter.apply(step.restore);
  return outcome;
}

export function validateScaleTarget(value: unknown): ScaleTarget {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Scale target must be an object.');
  const targetValue = value as Record<string, unknown>;
  const allowed: ScaleDimension[] = ['loadgen-max', 'collector-cpu-limit', 'collector-memory-limit', 'collector-node-max',
    'kusto-capacity', 'kusto-sku', 'system-pool-count'];
  if (Object.keys(targetValue).some(key => !['dimension', 'before', 'after'].includes(key)) ||
      !allowed.includes(targetValue.dimension as ScaleDimension) ||
      !['number', 'string'].includes(typeof targetValue.before) || typeof targetValue.before !== typeof targetValue.after ||
      targetValue.before === targetValue.after) throw new Error('Scale target is invalid or has an unsupported dimension.');
  return targetValue as unknown as ScaleTarget;
}
