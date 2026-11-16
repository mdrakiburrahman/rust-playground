import assert from 'node:assert/strict';
import test from 'node:test';
import { DEFAULT_EFFECTIVE_STOP_BUDGETS, MAINTENANCE_STOP_TIMEOUT_CEILING_MS, parseEffectiveStopBudgets } from './stop-budgets.js';

test('parses Rust-produced default and reduced maintenance budgets', () => {
  assert.deepEqual(parseEffectiveStopBudgets({ maintenance_grace_seconds: 4_800, maintenance_stop_timeout_ms: 5_100_000 }),
    DEFAULT_EFFECTIVE_STOP_BUDGETS);
  assert.deepEqual(parseEffectiveStopBudgets({ maintenance_grace_seconds: 600, maintenance_stop_timeout_ms: 900_000 }),
    { maintenanceGraceSeconds: 600, maintenanceStopTimeoutMs: 900_000 });
  assert.equal(MAINTENANCE_STOP_TIMEOUT_CEILING_MS, 5_100_000);
});

test('rejects malformed, undersized, over-ceiling, and extended maintenance budgets', () => {
  for (const value of [null, [], {},
    { maintenance_grace_seconds: 1, maintenance_stop_timeout_ms: 1, extra: true },
    { maintenance_grace_seconds: 0, maintenance_stop_timeout_ms: 1 },
    { maintenance_grace_seconds: 1.5, maintenance_stop_timeout_ms: 1_500 },
    { maintenance_grace_seconds: Number.MAX_SAFE_INTEGER + 1, maintenance_stop_timeout_ms: 5_100_000 },
    { maintenance_grace_seconds: 4_801, maintenance_stop_timeout_ms: 5_100_000 },
    { maintenance_grace_seconds: 4_800, maintenance_stop_timeout_ms: 5_100_001 }]) {
    assert.throws(() => parseEffectiveStopBudgets(value));
  }
});
