import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { DEFAULT_EFFECTIVE_STOP_BUDGETS, MAINTENANCE_STOP_TIMEOUT_CEILING_MS, parseEffectiveStopBudgets } from './stop-budgets.js';

const parityFixtures = JSON.parse(readFileSync(new URL('./stop-budget-fixtures.json', import.meta.url), 'utf8')) as
  Array<{ name: string; expected: unknown; rejected?: boolean }>;

test('Rust and TypeScript share derived stop-budget fixtures', () => {
  for (const fixture of parityFixtures) {
    if ('validation' in fixture) {
      assert.throws(() => parseEffectiveStopBudgets(fixture.validation), fixture.name);
      continue;
    }
    if (fixture.rejected) assert.throws(() => parseEffectiveStopBudgets(fixture.expected), fixture.name);
    else assert.deepEqual(parseEffectiveStopBudgets(fixture.expected), {
      maintenanceGraceSeconds: (fixture.expected as { maintenance_grace_seconds: number }).maintenance_grace_seconds,
      maintenanceStopTimeoutMs: (fixture.expected as { maintenance_stop_timeout_ms: number }).maintenance_stop_timeout_ms,
    }, fixture.name);
  }
});

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
    { maintenance_grace_seconds: 5_101, maintenance_stop_timeout_ms: 5_100_000 },
    { maintenance_grace_seconds: 4_800, maintenance_stop_timeout_ms: 5_100_001 }]) {
    assert.throws(() => parseEffectiveStopBudgets(value));
  }
});
