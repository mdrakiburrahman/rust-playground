import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';

export const ADMISSION_STOP_BUDGET_MS = 60_000;
export const DRAIN_BUDGET_MS = 120_000;
export const PRE_STOP_CAPTURE_MARGIN_MS = 30_000;
export interface DrainBudget { budget_ms: number; collector_grace_ms: number; pre_stop_capture_margin_ms: number }
export function parseDrainBudget(value: unknown): DrainBudget {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('drain_budget must be an object');
  const v = value as Record<string, unknown>;
  const { budget_ms, collector_grace_ms, pre_stop_capture_margin_ms } = v;
  if (Object.keys(v).sort().join() !== 'budget_ms,collector_grace_ms,pre_stop_capture_margin_ms' ||
      typeof budget_ms !== 'number' || !Number.isSafeInteger(budget_ms) || budget_ms <= 0 ||
      typeof collector_grace_ms !== 'number' || !Number.isSafeInteger(collector_grace_ms) || collector_grace_ms < 30_000 ||
      collector_grace_ms > 3_600_000 || pre_stop_capture_margin_ms !== PRE_STOP_CAPTURE_MARGIN_MS ||
      budget_ms > collector_grace_ms - PRE_STOP_CAPTURE_MARGIN_MS)
    throw new Error('drain_budget.budget_ms must be positive and no greater than collector_grace_ms minus pre_stop_capture_margin_ms');
  return { budget_ms, collector_grace_ms, pre_stop_capture_margin_ms };
}
export function drainBudgetMs(inventory: { drain_budget?: DrainBudget }): number {
  return inventory.drain_budget === undefined ? DRAIN_BUDGET_MS : parseDrainBudget(inventory.drain_budget).budget_ms;
}
export class CleanupDeadlineExpiredError extends Error {
  readonly code = 'cleanup_deadline_expired';
  constructor(message: string) { super(message); this.name = 'CleanupDeadlineExpiredError'; }
}
export const TERMINAL_CAPTURE_BUDGET_MS = 120_000;
export const WATCHDOG_INTERVAL_MS = 60_000;

export interface CleanupBudget { deadlineUnixMs: number; remainingMs: number }
export function withCleanupDeadline(authority: CleanupMutationAuthority, deadline: number | null):
  CleanupMutationAuthority & { budget: CleanupBudget } {
  const remainingMs = deadline === null ? 0 : deadline - Date.now();
  if (deadline === null || !Number.isSafeInteger(deadline))
    throw new Error('Cleanup stop deadline is missing or malformed.');
  if (remainingMs <= 0)
    throw new CleanupDeadlineExpiredError('Cleanup stop deadline expired; preserve partial evidence, never admit another effect.');
  return { ...authority, budget: { deadlineUnixMs: deadline, remainingMs },
    assertEvidenceCurrent: () => authority.assertEvidenceCurrent ? authority.assertEvidenceCurrent() : authority.assertCurrent(),
    assertCurrent: async () => {
    await authority.assertCurrent();
    if (Date.now() >= deadline) throw new CleanupDeadlineExpiredError('Cleanup stop deadline expired; an unfinished effect remains UNKNOWN.');
  } };
}
export function cleanupBudget(authority: CleanupMutationAuthority): CleanupBudget | undefined {
  if (!('budget' in authority)) return undefined;
  const value = authority.budget;
  if (!value || typeof value !== 'object' || !('deadlineUnixMs' in value) || !('remainingMs' in value) ||
      typeof value.deadlineUnixMs !== 'number' || !Number.isSafeInteger(value.deadlineUnixMs) ||
      typeof value.remainingMs !== 'number' || !Number.isFinite(value.remainingMs) || value.remainingMs <= 0)
    throw new Error('Cleanup stop budget is malformed.');
  return { deadlineUnixMs: value.deadlineUnixMs, remainingMs: value.remainingMs };
}
