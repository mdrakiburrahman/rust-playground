export const MAINTENANCE_STOP_TIMEOUT_CEILING_MS = 5_100_000;

export interface EffectiveStopBudgets {
  maintenanceGraceSeconds: number;
  maintenanceStopTimeoutMs: number;
}

export const DEFAULT_EFFECTIVE_STOP_BUDGETS: EffectiveStopBudgets = Object.freeze({
  maintenanceGraceSeconds: 4_800,
  maintenanceStopTimeoutMs: MAINTENANCE_STOP_TIMEOUT_CEILING_MS,
});

export function parseEffectiveStopBudgets(value: unknown): EffectiveStopBudgets {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('effective_stop_budgets must be an object');
  const record = value as Record<string, unknown>;
  const expected = ['maintenance_grace_seconds', 'maintenance_stop_timeout_ms'];
  if (Object.keys(record).sort().join() !== expected.join()) throw new Error('effective_stop_budgets has missing or unknown fields');
  const maintenanceGraceSeconds = record.maintenance_grace_seconds;
  const maintenanceStopTimeoutMs = record.maintenance_stop_timeout_ms;
  if (!Number.isSafeInteger(maintenanceGraceSeconds) || (maintenanceGraceSeconds as number) <= 0 ||
      !Number.isSafeInteger(maintenanceStopTimeoutMs) || (maintenanceStopTimeoutMs as number) <= 0)
    throw new Error('effective stop budget values must be positive safe integers');
  if ((maintenanceStopTimeoutMs as number) > MAINTENANCE_STOP_TIMEOUT_CEILING_MS ||
      (maintenanceStopTimeoutMs as number) < (maintenanceGraceSeconds as number) * 1_000)
    throw new Error('effective maintenance stop timeout must cover grace and not exceed the fixed ceiling');
  return { maintenanceGraceSeconds: maintenanceGraceSeconds as number, maintenanceStopTimeoutMs: maintenanceStopTimeoutMs as number };
}
