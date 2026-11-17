import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { parseArgs } from 'node:util';
import { pathToFileURL } from 'node:url';
import { canonical_json, ROOT, runtime, type Azure } from '../../infra/azure/bootstrap.js';
import { assertCampaignEnvironment, createBlobCampaignStore, type CampaignState } from './aks-campaign.js';
import { loadCustodyConfig } from './aks-custody-config.js';
import type { CustodyCommandRunner } from './aks-setup-cleanup.js';
import { createDefaultScaleAdapter } from './aks-scale-provider.js';
import { validateRuntime } from './runtime.js';
import { CampaignFencedError, mutateCampaign, SCALE_RESTORE_TIMEOUT_MS, validateScaleTarget as validateCampaignScaleTarget, type CampaignStore, type ScaleDimension, type ScaleResourceIdentity, type ScaleTarget, type ScaleTrialReceipt } from './aks-campaign.js';
export { SCALE_RESTORE_TIMEOUT_MS } from './aks-campaign.js';
export { validateCampaignScaleTarget as validateScaleTarget };
export type { ScaleDimension, ScaleTarget } from './aks-campaign.js';
const SCALE_TRIAL_TIMEOUT_MS = 15 * 60 * 1000;
const inFlightPhases = new Set(['apply-intent', 'apply-dispatch', 'applied', 'restore-intent', 'restore-dispatch', 'unknown']);
const transitions: Record<ScaleTrialReceipt['phase'], ScaleTrialReceipt['phase'][]> = {
  'apply-intent': ['apply-dispatch', 'applied', 'reverted', 'unknown'], 'apply-dispatch': ['applied', 'unknown'],
  applied: ['kept', 'restore-intent', 'unknown'], kept: ['restore-intent'],
  'restore-intent': ['restore-dispatch', 'restored', 'restore-stopped', 'unknown'],
  'restore-dispatch': ['restored', 'unknown'], restored: [], 'restore-stopped': [], reverted: [],
  unknown: ['applied', 'restore-intent', 'restored', 'restore-stopped', 'reverted'],
};

export async function persistScaleTrial(store: CampaignStore, receipt: ScaleTrialReceipt, actor: ScaleCustodyToken,
  now = Date.now()): Promise<void> {
  await mutateCampaign(store, current => {
    if (!current || current.ownerId !== actor.ownerId || current.fencingToken !== actor.fencingToken)
      throw new CampaignFencedError('Scale trial actor no longer owns campaign custody.');
    const producer = current.status === 'active' && current.mode === 'armed' && current.runInventory &&
      current.runInventory.campaign_id === receipt.campaign_id && current.runInventory.owner_id === receipt.owner_id &&
      current.runInventory.fencing_token === receipt.fencing_token && current.runInventory.cell_run_id === receipt.cell_run_id &&
      now >= current.heartbeatAtMs && now < current.expiresAtMs && now - current.heartbeatAtMs < 180_000;
    const cleanup = current.status === 'cleaning' && current.cleanupStatus === 'running' &&
      current.cleanupSteps.some(step => ['record-terminal-state', 'restore-safe-idle'].includes(step.step) && step.status === 'running');
    if (!producer && !cleanup) throw new CampaignFencedError('Scale trial mutation requires live producer custody or the safe-idle cleanup actor.');
    if (receipt.owner_id !== current.ownerId || receipt.campaign_id !== current.campaignId ||
        !Number.isSafeInteger(receipt.fencing_token) || receipt.fencing_token < 1 || receipt.fencing_token > current.fencingToken)
      throw new CampaignFencedError('Scale trial journal identity differs from campaign custody.');
    const trials = current.scaleTrials ?? [];
    const index = trials.findIndex(item => item.trial_id === receipt.trial_id);
    if (index < 0) {
      if (!producer || receipt.phase !== 'apply-intent' || receipt.fencing_token !== current.fencingToken ||
          trials.length >= 64 || trials.some(item => inFlightPhases.has(item.phase)))
        throw new CampaignFencedError('A new scale trial is not admitted by the current campaign boundary.');
      return { ...current, scaleTrials: [...trials, receipt] };
    }
    const existing = trials[index]!;
    const immutable = (item: ScaleTrialReceipt) => ({ schema_version: item.schema_version, trial_id: item.trial_id,
      campaign_id: item.campaign_id, owner_id: item.owner_id, fencing_token: item.fencing_token, cell_run_id: item.cell_run_id,
      started_at_ms: item.started_at_ms, deadline_at_ms: item.deadline_at_ms, restore_deadline_at_ms: item.restore_deadline_at_ms, target: item.target, restore: item.restore,
      evidence: item.evidence, baseline: item.baseline, resource: item.resource });
    if (canonical_json(immutable(existing)) !== canonical_json(immutable(receipt)) ||
        existing.operation_url !== undefined && existing.operation_url !== receipt.operation_url ||
        existing.result !== undefined && canonical_json(existing.result) !== canonical_json(receipt.result) ||
        receipt.operation_url !== undefined && existing.resource.transport !== 'azure-rest')
      throw new CampaignFencedError('Scale trial immutable target/resource binding changed.');
    for (const kind of ['apply', 'restore'] as const) {
      const prior = existing.dispatches?.[kind], next = receipt.dispatches?.[kind];
      if (!prior && next) {
        if (next.state !== 'forwarding' || receipt.phase !== (kind === 'apply' ? 'apply-dispatch' : 'restore-dispatch'))
          throw new CampaignFencedError('Scale dispatch receipt is not an exclusive forward-and-outcome transition.');
        continue;
      }
      if (prior && !next) throw new CampaignFencedError('Scale dispatch receipt cannot be removed.');
      if (prior && next) {
        const { state: priorState, response_status: priorStatus, returncode: priorCode, operation_url: priorUrl, ...priorBinding } = prior;
        const { state: nextState, response_status: nextStatus, returncode: nextCode, operation_url: nextUrl, ...nextBinding } = next;
        if (canonical_json(priorBinding) !== canonical_json(nextBinding) ||
            priorState !== nextState && (priorState !== 'forwarding' || !['confirmed', 'unknown'].includes(nextState)) ||
            priorState !== 'forwarding' && canonical_json(prior) !== canonical_json(next))
          throw new CampaignFencedError('Scale dispatch outcome is immutable after its single forward.');
      }
    }
    if (existing.phase !== receipt.phase && !transitions[existing.phase].includes(receipt.phase))
      throw new CampaignFencedError('Scale trial phase transition is not permitted.');
    if (cleanup && !['applied', 'restore-intent', 'restore-dispatch', 'restored', 'restore-stopped', 'reverted', 'unknown'].includes(receipt.phase))
      throw new CampaignFencedError('Cleanup may only reconcile or restore a scale trial.');
    if (!cleanup && existing.phase === 'apply-intent' && receipt.phase === 'applied')
      throw new CampaignFencedError('Only cleanup may recover an apply intent at after-state.');
    if (cleanup && receipt.phase === 'applied' && !(existing.phase === 'apply-intent' && !existing.dispatches?.apply ||
        existing.phase === 'apply-dispatch' && existing.dispatches?.apply ||
        existing.phase === 'unknown' && existing.dispatches?.apply && !existing.dispatches.restore))
      throw new CampaignFencedError('Cleanup may publish applied only when reconciling a durable apply dispatch.');
    const updated = [...trials];
    updated[index] = receipt;
    return { ...current, scaleTrials: updated };
  });
}

export function newScaleTrialReceipt(input: {
  state: import('./aks-campaign.js').CampaignState; step: ScaleStep; resource: ScaleResourceIdentity;
  trialId?: string; now?: number; timeoutMs?: number;
}): ScaleTrialReceipt {
  const { state, step, resource } = input, now = input.now ?? Date.now(), timeoutMs = input.timeoutMs ?? SCALE_TRIAL_TIMEOUT_MS;
  oneStep(step);
  const validatedTarget = validateCampaignScaleTarget({ dimension: step.dimension, before: step.before, after: step.after });
  if (!state.runInventory || state.status !== 'active' || state.mode !== 'armed' ||
      !Number.isSafeInteger(now) || now < state.startedAtMs || now >= state.expiresAtMs ||
      !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || !Number.isSafeInteger(now + timeoutMs + SCALE_RESTORE_TIMEOUT_MS) ||
      now + timeoutMs + SCALE_RESTORE_TIMEOUT_MS > state.expiresAtMs)
    throw new Error('Scale trial requires an active armed campaign and bounded remaining lifetime.');
  return { schema_version: 1, trial_id: input.trialId ?? randomUUID(), campaign_id: state.campaignId, owner_id: state.ownerId,
    fencing_token: state.fencingToken, cell_run_id: state.runInventory.cell_run_id, started_at_ms: now,
    deadline_at_ms: now + timeoutMs, restore_deadline_at_ms: now + timeoutMs + SCALE_RESTORE_TIMEOUT_MS, target: validatedTarget,
    restore: validateCampaignScaleTarget({ dimension: validatedTarget.dimension, before: validatedTarget.after, after: validatedTarget.before }),
    evidence: [...step.evidence], baseline: { committed_rows_per_second: step.baselineCommittedRowsPerSecond,
      qap_pending_backlog: step.baselineQapPendingBacklog, qap_p95_latency_ms: step.baselineQapP95LatencyMs },
    resource: structuredClone(resource), phase: 'apply-intent' };
}

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
  validateCampaignScaleTarget({ dimension: step.dimension, before: step.before, after: step.after });
  validateCampaignScaleTarget(step.restore);
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
  if (!['Standard_E4ads_v5', 'Standard_E8ads_v5'].includes(evidence.kustoSku) ||
      [evidence.generatorCpuSaturated, evidence.generatorNetworkSaturated, evidence.collectorCpuSaturated,
        evidence.collectorThrottled, evidence.collectorMemorySaturated, evidence.collectorPlacementBlocked,
        evidence.collectorNodePoolAtMaximum, evidence.qapBacklogGrowing, evidence.committedRateStable, evidence.kustoSaturated]
        .some(value => typeof value !== 'boolean')) throw new Error('Scale evidence has invalid signal values.');

  if (evidence.unschedulablePods > 0 || evidence.pdbBlockedDrains > 0) return undefined;
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
  if (evidence.qapBacklogGrowing && evidence.committedRateStable && evidence.kustoSaturated) {
    if (evidence.kustoCapacity < evidence.kustoCapacityMaximum) return makeStep(evidence, 'kusto-capacity', evidence.kustoCapacity,
      evidence.kustoCapacity + 1, ['qap-backlog-growing', 'committed-rate-stable', 'kusto-side-saturated']);
    if (evidence.kustoSku === 'Standard_E4ads_v5') return makeStep(evidence, 'kusto-sku', evidence.kustoSku,
      'Standard_E8ads_v5', ['qap-backlog-growing', 'committed-rate-stable', 'kusto-capacity-at-maximum']);
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
  persistIntent(step: ScaleTarget, token: ScaleCustodyToken, trial?: ScaleStep): Promise<void>;
  finish?(result: ScaleTrialResult): Promise<void>;
  finishUnknown?(reason: string): Promise<void>;
  apply(step: ScaleTarget, token: ScaleCustodyToken): Promise<void>;
  reconcile(step: ScaleTarget): Promise<'before' | 'after' | 'pending' | 'unknown'>;
  deadlineFor?(step: ScaleTarget): Promise<{ trial: number; restore: number }>;
  measure(deadlineAtMs?: number): Promise<ScaleMeasurement>;
  beginMeasurement?(deadlineAtMs?: number): Promise<void>;
}
export async function executeScaleTrial(step: ScaleStep, adapter: ScaleAdapter, token: ScaleCustodyToken,
  previousIneffectiveIncrements = 0): Promise<ScaleTrialResult> {
  oneStep(step);
  if (!token || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(token.ownerId) ||
      !Number.isSafeInteger(token.fencingToken) || token.fencingToken < 1) throw new Error('Scale custody token is invalid.');
  const applyOnce = async (target: ScaleTarget): Promise<void> => {
    await adapter.assertCustody(token);
    await adapter.persistIntent(target, token, step);
    const before = await adapter.reconcile(target);
    if (before === 'after') return;
    if (before !== 'before') throw new Error('Scale target state is ' + before + '; refusing to apply without an exact before-state readback.');
    let applyError: unknown;
    try { await adapter.apply(target, token); } catch (error) { applyError = error; }
    const observed = await adapter.reconcile(target);
    if (observed === 'pending' || observed === 'unknown')
      throw new Error('Scale target acknowledgement is uncertain (' + observed + '); refusing to replay or roll back.');
    if (observed === 'before') {
      if (applyError) throw applyError;
      throw new Error('Scale target request returned without the exact after-state readback.');
    }
  };
  const restore = async (): Promise<void> => {
    await adapter.assertCustody(token);
    await adapter.persistIntent(step.restore, token, step);
    const before = await adapter.reconcile(step.restore);
    if (before === 'after') return;
    if (before !== 'before') throw new Error('Scale restore state is ' + before + '; refusing to restore over an uncertain target.');
    let applyError: unknown;
    try { await adapter.apply(step.restore, token); } catch (error) { applyError = error; }
    const observed = await adapter.reconcile(step.restore);
    if (observed !== 'after') {
      if (applyError) throw applyError;
      throw new Error('Scale restore acknowledgement is ' + observed + '; refusing to replay.');
    }
  };
  await adapter.assertCustody(token);
  await applyOnce(step);
  let result: ScaleTrialResult;
  try {
    const deadlines = await adapter.deadlineFor?.(step);
    const trialDeadline = deadlines?.trial ?? Date.now() + SCALE_TRIAL_TIMEOUT_MS;
    if (Date.now() >= trialDeadline) throw new Error('Scale trial deadline expired before measurement.');
    await adapter.beginMeasurement?.(trialDeadline);
    result = assessScaleTrial(step, await adapter.measure(trialDeadline), previousIneffectiveIncrements);
  } catch (error) {
    try {
      await restore();
      await adapter.finishUnknown?.('measurement-unavailable');
    } catch (restoreError) {
      throw new AggregateError([error, restoreError], 'Scale measurement failed and restoration remains unresolved.');
    }
    throw error;
  }
  if (result.outcome === 'keep') { await adapter.finish?.(result); return result; }
  await restore();
  await adapter.finish?.(result);
  return result;
}


const evidenceSources = ['committed-window', 'lgtm-prometheus', 'qap', 'runinventory'] as const;
export interface ScaleEvidenceEnvelope {
  schema_version: 1; campaign_id: string; cell_run_id: string; observed_at_ms: number; window_ms: number;
  sources: string[]; evidence: ScaleEvidence;
}
interface ScaleMeasurementEnvelope {
  schema_version: 1; campaign_id: string; cell_run_id: string; observed_at_ms: number; window_ms: number;
  sources: string[]; measurement: ScaleMeasurement;
}
function record(value: unknown): value is Record<string, unknown> { return typeof value === 'object' && value !== null && !Array.isArray(value); }
function exactKeys(value: Record<string, unknown>, keys: string[]): boolean {
  return Object.keys(value).sort().join() === [...keys].sort().join();
}
function validEvidenceSources(value: unknown): value is string[] {
  return Array.isArray(value) && value.length === evidenceSources.length && evidenceSources.every(source => value.includes(source));
}
function scaleEnvelope(value: unknown, state: CampaignState, now: number): ScaleEvidenceEnvelope {
  if (!record(value) || !exactKeys(value, ['schema_version', 'campaign_id', 'cell_run_id', 'observed_at_ms', 'window_ms', 'sources', 'evidence']) ||
      value.schema_version !== 1 || value.campaign_id !== state.campaignId || value.cell_run_id !== state.runInventory?.cell_run_id ||
      !Number.isSafeInteger(value.observed_at_ms) || (value.observed_at_ms as number) > now + 5_000 ||
      now - (value.observed_at_ms as number) > 300_000 || !Number.isSafeInteger(value.window_ms) ||
      (value.window_ms as number) < 30_000 || (value.window_ms as number) > 300_000 ||
      !validEvidenceSources(value.sources) || !record(value.evidence) ||
      !exactKeys(value.evidence, ['committedRowsPerSecond', 'offeredRowsPerSecond', 'estimatedCollectorCapacityRowsPerSecond',
        'generatorCpuSaturated', 'generatorNetworkSaturated', 'collectorCpuSaturated', 'collectorThrottled', 'collectorMemorySaturated',
        'collectorPlacementBlocked', 'collectorNodePoolAtMaximum', 'qapBacklogGrowing', 'committedRateStable', 'qapPendingBacklog',
        'previousQapPendingBacklog', 'qapP95LatencyMs', 'previousQapP95LatencyMs', 'kustoSaturated', 'loadgenMax', 'loadgenMaximum',
        'collectorCpuLimitCores', 'collectorCpuLimitMaximum', 'collectorMemoryLimitGiB', 'collectorMemoryLimitMaximumGiB',
        'collectorNodeMax', 'collectorNodeMaxMaximum', 'kustoCapacity', 'kustoCapacityMaximum', 'kustoSku', 'systemPoolCount',
        'unschedulablePods', 'pdbBlockedDrains'])) throw new Error('Scale evidence envelope is stale, incomplete or belongs to another campaign/run.');
  const evidence = value.evidence as unknown as ScaleEvidence;
  recommendScaleStep(evidence);
  return value as unknown as ScaleEvidenceEnvelope;
}
function measurementEnvelope(value: unknown, state: CampaignState, now: number): ScaleMeasurementEnvelope {
  if (!record(value) || !exactKeys(value, ['schema_version', 'campaign_id', 'cell_run_id', 'observed_at_ms', 'window_ms', 'sources', 'measurement']) ||
      value.schema_version !== 1 || value.campaign_id !== state.campaignId || value.cell_run_id !== state.runInventory?.cell_run_id ||
      !Number.isSafeInteger(value.observed_at_ms) || (value.observed_at_ms as number) > now + 5_000 ||
      now - (value.observed_at_ms as number) > 300_000 || !Number.isSafeInteger(value.window_ms) ||
      (value.window_ms as number) < 30_000 || (value.window_ms as number) > 300_000 ||
      !validEvidenceSources(value.sources) || !record(value.measurement) ||
      !exactKeys(value.measurement, ['committedRowsPerSecond', 'qapPendingBacklog', 'qapP95LatencyMs']))
    throw new Error('Scale measurement envelope is stale, incomplete or belongs to another campaign/run.');
  const measurement = value.measurement as unknown as ScaleMeasurement;
  if (!nonnegative(measurement.committedRowsPerSecond) || !Number.isSafeInteger(measurement.qapPendingBacklog) ||
      measurement.qapPendingBacklog < 0 || !nonnegative(measurement.qapP95LatencyMs)) throw new Error('Scale measurement is UNKNOWN.');
  return value as unknown as ScaleMeasurementEnvelope;
}
export type ScaleEvidenceVerifier = (evidence: ScaleEvidenceEnvelope, state: CampaignState) => Promise<void>;
async function readJsonFile(file: string): Promise<unknown> {
  const bytes = await readFile(resolve(ROOT, file));
  if (bytes.byteLength > 262_144) throw new Error('Scale evidence file exceeds its fixed size bound.');
  try { return JSON.parse(bytes.toString('utf8')) as unknown; } catch { throw new Error('Scale evidence file is not valid JSON.'); }
}
async function waitForScaleMeasurement(file: string, state: CampaignState, notBefore: number, deadline: number): Promise<ScaleMeasurement> {
  while (Date.now() < deadline) {
    let input: unknown;
    try { input = await readJsonFile(file); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      await new Promise(resolve => setTimeout(resolve, 1000));
      continue;
    }
    if (!record(input) || input.campaign_id !== state.campaignId || input.cell_run_id !== state.runInventory?.cell_run_id)
      throw new Error('Scale measurement belongs to another campaign/run.');
    if (typeof input.observed_at_ms === 'number' && input.observed_at_ms < notBefore) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      continue;
    }
    const envelope = measurementEnvelope(input, state, Date.now());
    return envelope.measurement;
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  throw new Error('No fresh post-scale measurement arrived before the bounded trial deadline.');
}

export async function main(args: string[], runner: typeof runtime.run_tree = runtime.run_tree, verifyEvidence?: ScaleEvidenceVerifier): Promise<void> {
  const parsed = parseArgs({ args, allowPositionals: true, options: {
    'env-file': { type: 'string' }, 'stamp-config': { type: 'string' }, 'runtime-config': { type: 'string' },
    'evidence-file': { type: 'string' }, 'measurement-file': { type: 'string' },
  } });
  const { values, positionals } = parsed;
  if (positionals.length !== 2 || !values['runtime-config'] || !values['evidence-file'] || !values['measurement-file'])
    throw new Error('Usage: arrowcol-oat aks scale <owner-id> <fencing-token> --env-file PATH --stamp-config PATH --runtime-config PATH --evidence-file PATH --measurement-file PATH');
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(positionals[0]!) || !/^\d+$/.test(positionals[1]!))
    throw new Error('Scale requires an exact owner UUID and numeric fencing token.');
  if (!verifyEvidence) throw new Error('Scale admission is closed: no trusted committed-window, Prometheus, QAP and RunInventory evidence verifier is configured.');
  const selected = loadCustodyConfig(['--env-file', String(values['env-file'] ?? ''), '--stamp-config', String(values['stamp-config'] ?? '')]);
  const store = createBlobCampaignStore({ account: selected.config.account, container: selected.config.checkpoints,
    subscriptionId: selected.config.subscription });
  const { Azure } = await import('../../infra/azure/bootstrap.js');
  const azure = new Azure(selected.config);
  const stored = await store.read();
  if (!stored) throw new Error('No active campaign state exists.');
  const state = stored.state;
  assertCampaignEnvironment(selected.env, state);
  const token = { ownerId: positionals[0]!, fencingToken: Number(positionals[1]) };
  if (state.status !== 'active' || state.mode !== 'armed' || state.ownerId !== token.ownerId || state.fencingToken !== token.fencingToken || !state.runInventory)
    throw new CampaignFencedError('Scale requires the exact active armed campaign owner and fence.');
  const evidence = scaleEnvelope(await readJsonFile(values['evidence-file']!), state, Date.now());
  await verifyEvidence(evidence, state);
  const step = recommendScaleStep(evidence.evidence);
  if (!step) { console.log(JSON.stringify({ status: 'no-measured-bottleneck', campaign_id: state.campaignId })); return; }
  const previous = [...(state.scaleTrials ?? [])].reverse().find(item => item.target.dimension === step.dimension)?.result?.ineffective_increments ?? 0;
  if (previous >= 2) { console.log(JSON.stringify({ status: 'stopped-after-ineffective-increments', dimension: step.dimension })); return; }
  const runtimeConfig = validateRuntime(await readJsonFile(values['runtime-config']));
  if (runtimeConfig.subscriptionId !== selected.config.subscription) throw new Error('Runtime configuration subscription differs from the selected stamp.');
  if (runtimeConfig.stamp !== selected.config.stamp) throw new Error('Runtime configuration stamp differs from the selected stamp.');
  let measurementStartedAt = 0, trialDeadlineAt = 0;
  const adapter = createDefaultScaleAdapter(store, { azure, token, runCommand: runner as CustodyCommandRunner, runtimeConfig,
    beginMeasurement: deadlineAtMs => { measurementStartedAt = Date.now(); trialDeadlineAt = deadlineAtMs; },
    measure: () => waitForScaleMeasurement(values['measurement-file']!, state, measurementStartedAt, trialDeadlineAt) });
  const result = await executeScaleTrial(step, adapter, token, previous);
  console.log(JSON.stringify({ campaign_id: state.campaignId, cell_run_id: state.runInventory.cell_run_id, step, result }));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
}
