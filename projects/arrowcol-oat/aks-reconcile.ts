import { pathToFileURL } from 'node:url';
import { CUSTODY_CLEANUP_STEPS, createBlobCampaignStore, mutateCampaign, type ArmOperationReceipt, type CampaignState, type CampaignStore, type CleanupStepName, readArmOperationStatus, setCleanupVerified } from './aks-campaign.js';

import { setTimeout as delay } from "node:timers/promises";
export type FenceReason = 'heartbeat-expired' | 'max-lifetime';
export interface CustodyEvaluation {
  missedHeartbeats: number;
  fenceReason?: FenceReason;
  wouldFence: boolean;
}
export interface ArmStatusResponse { status: number; body?: unknown }
export type ArmStatusReader = (url: string) => Promise<ArmStatusResponse>;

function object(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
}
export function evaluateCustody(state: CampaignState, now: number): CustodyEvaluation {
  if (!Number.isSafeInteger(now) || now < state.startedAtMs) throw new Error('Controller clock is invalid or precedes campaign start.');
  const missedHeartbeats = Math.floor(Math.max(0, now - state.heartbeatAtMs) / 60_000);
  const fenceReason = now >= state.expiresAtMs ? 'max-lifetime' :
    missedHeartbeats >= 3 ? 'heartbeat-expired' : undefined;
  return { missedHeartbeats, fenceReason, wouldFence: fenceReason !== undefined };
}

export async function reconcileArmOperation(operation: ArmOperationReceipt, read: ArmStatusReader, subscriptionId: string, now = Date.now()): Promise<ArmOperationReceipt> {
  if (operation.status === 'succeeded' || operation.status === 'failed') return operation;
  const url = new URL(operation.operationUrl);
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(subscriptionId) ||
      url.protocol !== 'https:' || url.hostname !== 'management.azure.com' || url.username || url.password || url.hash ||
      !url.pathname.toLowerCase().includes(`/subscriptions/${subscriptionId.toLowerCase()}/`)) {
    throw new Error('ARM operation URL is outside the configured subscription.');
  }
  if (!Number.isSafeInteger(now) || now < operation.submittedAtMs) throw new Error('ARM reconciliation clock is invalid.');
  const response = await read(url.toString());
  if ([202, 408, 429, 500, 502, 503, 504].includes(response.status)) return { ...operation, status: 'pending', lastCheckedAtMs: now };
  if (response.status < 200 || response.status >= 300) return { ...operation, status: 'indeterminate', lastCheckedAtMs: now };
  const body = object(response.body);
  if (operation.cleanupTarget) {
    const properties = object(body?.properties), target = operation.cleanupTarget;
    if (typeof body?.id !== 'string' || body.id.toLowerCase() !== operation.resourceId.toLowerCase() || !properties)
      return { ...operation, status: 'indeterminate', lastCheckedAtMs: now };
    const state = properties.provisioningState;
    if (state === 'Failed' || state === 'Canceled') return { ...operation, status: 'failed', lastCheckedAtMs: now };
    if (state !== 'Succeeded') return { ...operation, status: 'pending', lastCheckedAtMs: now };
    const sku = object(body.sku), autoscale = object(properties.optimizedAutoscale);
    const matches = target.kind === 'aks-pool' ?
      properties.count === target.count && properties.enableAutoScaling === target.autoscale &&
        (!target.autoscale || properties.minCount === target.minimum && properties.maxCount === target.maximum) :
      sku?.name === target.sku && sku.capacity === target.capacity && properties.enableAutoStop === target.autoStop &&
        autoscale?.isEnabled === true && autoscale.minimum === target.minimum && autoscale.maximum === target.maximum;
    return { ...operation, status: matches ? 'succeeded' : 'indeterminate', lastCheckedAtMs: now };
  }
  const status = body?.status;
  if (status === 'Succeeded') return { ...operation, status: 'succeeded', lastCheckedAtMs: now };
  if (status === 'Failed' || status === 'Canceled') return { ...operation, status: 'failed', lastCheckedAtMs: now };
  if (status === 'InProgress' || status === 'Running' || status === 'Accepted') return { ...operation, status: 'pending', lastCheckedAtMs: now };
  return { ...operation, status: 'indeterminate', lastCheckedAtMs: now };
}

export async function reconcileOnce(store: CampaignStore, now = Date.now()): Promise<{
  status: 'idle' | 'observed' | 'fenced' | 'terminal'; campaignId?: string; missedHeartbeats?: number; fenceReason?: FenceReason;
}> {
  const stored = await store.read();
  if (!stored) return { status: 'idle' };
  const evaluation = evaluateCustody(stored.state, now);
  if (stored.state.status !== 'active') return { status: 'terminal', campaignId: stored.state.campaignId };
  if (!evaluation.wouldFence || stored.state.mode === 'observe') {
    await mutateCampaign(store, current => {
      if (!current || current.campaignId !== stored.state.campaignId || current.fencingToken !== stored.state.fencingToken || current.status !== 'active') {
        throw new Error('Campaign changed while the controller was observing it.');
      }
      return { ...current, lastEvaluatedAtMs: now, lastMissedHeartbeatCount: evaluation.missedHeartbeats };
    });
    return { status: 'observed', campaignId: stored.state.campaignId, missedHeartbeats: evaluation.missedHeartbeats,
      ...(evaluation.fenceReason ? { fenceReason: evaluation.fenceReason } : {}) };
  }
  const result = await mutateCampaign(store, current => {
    if (!current || current.campaignId !== stored.state.campaignId || current.fencingToken !== stored.state.fencingToken || current.status !== 'active') {
      throw new Error('Campaign changed while the controller was fencing its owner.');
    }
    const currentEvaluation = evaluateCustody(current, now);
    if (!currentEvaluation.fenceReason) return { ...current, lastEvaluatedAtMs: now, lastMissedHeartbeatCount: currentEvaluation.missedHeartbeats };
    return { ...current, status: 'fenced', fencingToken: current.fencingToken + 1, fenceReason: currentEvaluation.fenceReason,
      fencedAtMs: now, lastEvaluatedAtMs: now, lastMissedHeartbeatCount: currentEvaluation.missedHeartbeats, cleanupStatus: "pending",
      cleanupSteps: CUSTODY_CLEANUP_STEPS.map(step => ({ step, idempotencyKey: current.campaignId + ":" + (current.fencingToken + 1) + ":" + step, status: "pending" })) };
  });
  return result.state.status === 'fenced'
    ? { status: 'fenced', campaignId: result.state.campaignId, missedHeartbeats: result.state.lastMissedHeartbeatCount,
      fenceReason: result.state.fenceReason }
    : { status: 'observed', campaignId: result.state.campaignId, missedHeartbeats: result.state.lastMissedHeartbeatCount };
}

export async function reconcilePendingArmOperations(store: CampaignStore, read: ArmStatusReader, subscriptionId: string, now = Date.now()): Promise<CampaignState | undefined> {
  const stored = await store.read();
  if (!stored) return undefined;
  if (!stored.state.operations.some(operation => operation.status === 'pending' || operation.status === 'indeterminate')) return stored.state;
  const updates = await Promise.all(stored.state.operations.map(operation => reconcileArmOperation(operation, read, subscriptionId, now)));
  return (await mutateCampaign(store, current => {
    if (!current || current.campaignId !== stored.state.campaignId || current.ownerId !== stored.state.ownerId ||
        current.fencingToken !== stored.state.fencingToken || current.status !== stored.state.status)
      throw new Error('Campaign authority changed during ARM operation reconciliation.');
    const updated = new Map(updates.map(operation => [operation.id, operation]));
    return { ...current, operations: current.operations.map(operation =>
      ['succeeded', 'failed'].includes(operation.status) ? operation : updated.get(operation.id) ?? operation) };
  })).state;
}

async function main(args: string[]): Promise<void> {
  if (args.length !== 1 || args[0] !== '--once') throw new Error('Usage: aks-reconcile.ts --once');
  const subscriptionId = process.env.ARM_SUBSCRIPTION_ID ?? '';
  const store = createBlobCampaignStore({ account: process.env.CAMPAIGN_STORAGE_ACCOUNT ?? '',
    container: process.env.CAMPAIGN_STATE_CONTAINER ?? '', subscriptionId });
  const operations = await reconcilePendingArmOperations(store, url => readArmOperationStatus(url, subscriptionId), subscriptionId);
  const result = await reconcileOnce(store);
  console.log(JSON.stringify({ event: 'campaign_custody_evaluation', ...result, pendingArmOperations: operations?.operations.filter(operation => operation.status === 'pending').length ?? 0, observedAtMs: Date.now() }));
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => { console.error(error instanceof Error ? error.message : 'Campaign reconciliation failed'); process.exitCode = 1; });
}

export interface SafeIdleObservation {
  aksProvisioningState: string;
  kustoProvisioningState: string;
  systemPoolCount: number;
  userPools: Array<{ name: string; nodeCount: number; autoscaleMin: number; autoscaleMax: number }>;
  pods: Array<{ name: string; ready: boolean; scheduled: boolean }>;
  qapNotHealthyReasons: number;
  qapPendingBacklog: number;
  previousQapPendingBacklog: number;
  deltaRowsBefore: number;
  deltaRowsAfter: number;
  kustoRowsBefore: number;
  kustoRowsAfter: number;
  kustoSku: string;
  kustoCapacity: number;
  kustoAutoscaleMin: number;
  kustoAutoscaleMax: number;
  kustoAutoStopEnabled: boolean;
}
export function verifySafeIdle(observation: SafeIdleObservation): { safe: boolean; failures: string[] } {
  const failures: string[] = [];
  const counts = [observation.systemPoolCount, observation.qapNotHealthyReasons, observation.qapPendingBacklog,
    observation.previousQapPendingBacklog, observation.deltaRowsBefore, observation.deltaRowsAfter,
    observation.kustoRowsBefore, observation.kustoRowsAfter, observation.kustoCapacity,
    observation.kustoAutoscaleMin, observation.kustoAutoscaleMax,
    ...(Array.isArray(observation.userPools) ? observation.userPools.flatMap(pool => [pool.nodeCount, pool.autoscaleMin, pool.autoscaleMax]) : [])];
  if (counts.some(value => !Number.isSafeInteger(value) || value < 0)) failures.push('safe-idle-measurements-incomplete');
  if (observation.aksProvisioningState !== 'Succeeded') failures.push('aks-provisioning-not-succeeded');
  if (observation.kustoProvisioningState !== 'Succeeded') failures.push('kusto-provisioning-not-succeeded');
  if (![2, 3].includes(observation.systemPoolCount)) failures.push('system-pool-outside-safe-idle-target');
  if (!Array.isArray(observation.userPools) || !observation.userPools.some(pool => pool.name === 'benchd64') ||
      !observation.userPools.some(pool => pool.name === 'benche64')) {
    failures.push('benchmark-pools-missing');
  }
  if (observation.userPools.some(pool => pool.nodeCount !== 0 || pool.autoscaleMin !== 0 || pool.autoscaleMax !== 1)) {
    failures.push('benchmark-pool-not-scaled-to-safe-idle');
  }
  if (!Array.isArray(observation.pods) || observation.pods.length === 0 || observation.pods.some(pod => !pod.ready || !pod.scheduled)) {
    failures.push('stamp-pods-not-ready-and-scheduled');
  }
  if (observation.qapNotHealthyReasons !== 0) failures.push('qap-not-healthy');
  if (observation.qapPendingBacklog > observation.previousQapPendingBacklog) failures.push('qap-backlog-growing');
  if (observation.deltaRowsAfter !== observation.deltaRowsBefore) failures.push('delta-row-count-changed');
  if (observation.kustoRowsAfter !== observation.kustoRowsBefore) failures.push('kusto-row-count-changed');
  if (observation.kustoSku !== 'Standard_E4ads_v5' || observation.kustoCapacity !== 2 ||
      observation.kustoAutoscaleMin !== 2 || observation.kustoAutoscaleMax !== 4 || !observation.kustoAutoStopEnabled) {
    failures.push('kusto-below-or-outside-recorded-safe-idle-baseline');
  }
  return { safe: failures.length === 0, failures };
}

export async function ensureSafeIdle(read: () => Promise<SafeIdleObservation>, restore: () => Promise<void>): Promise<{
  status: 'already-safe' | 'restored'; observation: SafeIdleObservation;
}> {
  const before = await read();
  if (verifySafeIdle(before).safe) return { status: 'already-safe', observation: before };
  await restore();
  const after = await read();
  const verification = verifySafeIdle(after);
  if (!verification.safe) throw new Error(`Safe-idle restoration did not verify: ${verification.failures.join(', ')}.`);
  return { status: 'restored', observation: after };
}

export type CleanupExecutor = (step: CleanupStepName, state: CampaignState, idempotencyKey: string) => Promise<'pending' | 'complete'>;
export type CleanupProgress = 'idle' | 'fenced' | 'pending' | 'progress' | 'complete';

/** Persist a stable step key before side effects so interrupted cleanup can safely retry. */
export async function reconcileCleanupOnce(store: CampaignStore, execute: CleanupExecutor): Promise<CleanupProgress> {
  let stored = await store.read();
  if (!stored) return 'idle';
  if (stored.state.status === 'complete') return 'complete';
  if (stored.state.status === 'active') return 'idle';
  if (stored.state.status === 'fenced') {
    stored = await mutateCampaign(store, current => {
      if (!current || current.campaignId !== stored!.state.campaignId || current.status === 'complete') {
        throw new Error('Campaign changed before cleanup could start.');
      }
      if (current.status === 'cleaning') return current;
      if (current.status !== 'fenced') throw new Error('Campaign is not fenced for cleanup.');
      return { ...current, status: 'cleaning', cleanupStatus: 'running' };
    });
  }
  if (stored.state.status !== 'cleaning') throw new Error('Cleanup requires a fenced campaign.');
  const next = stored.state.cleanupSteps.find(receipt => receipt.status !== 'complete');
  if (!next) {
    await setCleanupVerified(store, stored.state.ownerId, stored.state.fencingToken);
    return 'complete';
  }
  const prepared = await mutateCampaign(store, current => {
    if (!current || current.campaignId !== stored!.state.campaignId || current.status !== 'cleaning' ||
        current.fencingToken !== stored!.state.fencingToken) throw new Error('Campaign changed during cleanup preparation.');
    const receipt = current.cleanupSteps.find(item => item.step === next.step);
    if (!receipt || receipt.idempotencyKey !== next.idempotencyKey) throw new Error('Cleanup receipt changed unexpectedly.');
    if (receipt.status === 'complete') return current;
    return { ...current, cleanupSteps: current.cleanupSteps.map(item => item.step === next.step ? { ...item, status: 'running' } : item) };
  });
  const receipt = prepared.state.cleanupSteps.find(item => item.step === next.step)!;
  if (receipt.status === 'complete') return 'progress';
  const result = await execute(receipt.step, prepared.state, receipt.idempotencyKey);
  if (result === 'pending') return 'pending';
  if (result !== 'complete') throw new Error('Cleanup adapter returned an unsupported result.');
  const completed = await mutateCampaign(store, current => {
    if (!current || current.campaignId !== prepared.state.campaignId || current.status !== 'cleaning' ||
        current.fencingToken !== prepared.state.fencingToken) throw new Error('Campaign changed while cleanup was executing.');
    const step = current.cleanupSteps.find(item => item.step === receipt.step);
    if (!step || step.idempotencyKey !== receipt.idempotencyKey) throw new Error('Cleanup receipt changed while executing.');
    if (step.status === 'complete') return current;
    const cleanupSteps = current.cleanupSteps.map(item => item.step === receipt.step ? { ...item, status: 'complete' as const } : item);
    if (cleanupSteps.every(item => item.status === 'complete')) return { ...current, status: 'complete', cleanupStatus: 'verified', cleanupSteps };
    return { ...current, cleanupSteps };
  });
  return completed.state.status === 'complete' ? 'complete' : 'progress';
}
