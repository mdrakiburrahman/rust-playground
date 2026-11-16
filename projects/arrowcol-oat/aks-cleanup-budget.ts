import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';

export interface CleanupBudget { deadlineUnixMs: number; remainingMs: number }
export function withCleanupDeadline(authority: CleanupMutationAuthority, deadline: number | null):
  CleanupMutationAuthority & { budget: CleanupBudget } {
  const remainingMs = deadline === null ? 0 : deadline - Date.now();
  if (deadline === null || !Number.isSafeInteger(deadline) || remainingMs <= 0)
    throw new Error('Cleanup stop deadline expired; preserve partial evidence, never admit another effect.');
  return { ...authority, budget: { deadlineUnixMs: deadline, remainingMs },
    assertEvidenceCurrent: () => authority.assertEvidenceCurrent ? authority.assertEvidenceCurrent() : authority.assertCurrent(),
    assertCurrent: async () => {
    await authority.assertCurrent();
    if (Date.now() >= deadline) throw new Error('Cleanup stop deadline expired; an unfinished effect remains UNKNOWN.');
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
