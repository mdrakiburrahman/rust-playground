import test from 'node:test';
import assert from 'node:assert/strict';
import { observedQapPressure, profileFromCluster, profileMatches, validateClusterProfile } from './aks-qap-cluster.js';
import { clockErrorBound } from './aks-qap-observer.js';

const fixed = { sku: 'Standard_E8ads_v5', nodes: 4, autoscale: { enabled: false, minimum: 4, maximum: 4 } };

test('cluster profiles enforce both node and vCPU ceilings including optimized autoscale maxima', () => {
  assert.equal(validateClusterProfile(fixed).nodes, 4);
  assert.equal(validateClusterProfile({ sku: 'Standard_E32ads_v5', nodes: 2,
    autoscale: { enabled: false, minimum: 2, maximum: 2 } }).sku, 'Standard_E32ads_v5');
  for (const invalid of [
    { ...fixed, sku: 'Standard_E64ads_v5' },
    { ...fixed, sku: 'Standard_E32ads_v5' },
    { ...fixed, nodes: 9, autoscale: { enabled: true, minimum: 2, maximum: 9 } },
    { ...fixed, nodes: 4, autoscale: { enabled: true, minimum: 2, maximum: 8.5 } },
    { ...fixed, sku: 'Standard_E16ads_v5', autoscale: { enabled: true, minimum: 2, maximum: 8 } },
    { ...fixed, autoscale: { enabled: false, minimum: 2, maximum: 4 } },
    { ...fixed, command: 'arbitrary cloud command' },
    { ...fixed, autoscale: { ...fixed.autoscale, coefficient: 2 } },
  ]) assert.throws(() => validateClusterProfile(invalid), /Unsupported|exceeds|pin|unexpected|integer/);
});

test('readback only permits node movement inside the exact configured optimized autoscale bounds', () => {
  const wanted = validateClusterProfile({ ...fixed, autoscale: { enabled: true, minimum: 2, maximum: 8 } });
  const actual = profileFromCluster({ sku: { name: wanted.sku, capacity: 6 },
    properties: { optimizedAutoscale: { isEnabled: true, minimum: 2, maximum: 8 } } });
  assert.equal(profileMatches(actual, wanted, true), true);
  assert.equal(profileMatches(actual, wanted, false), false);
  assert.equal(profileMatches({ ...actual, sku: 'Standard_E4ads_v5' }, wanted, true), false);
  assert.equal(profileMatches({ ...actual, autoscale: { enabled: true, minimum: 3, maximum: 8 } }, wanted, true), false);
});

function observation(at: number, pending: number, consumed: number, resource = 'QueryAcceleration') {
  return { endedAtMs: at, before: { pendingFiles: pending },
    capacity: { Tables: [{ Columns: ['Resource', 'Total', 'Consumed'].map(ColumnName => ({ ColumnName })),
      Rows: [[resource, 4, consumed]] }] } };
}

test('scale permission needs sustained actual QAP saturation or growing observed backlog, not ingestion capacity', () => {
  const saturated = [observation(0, 10, 4), observation(15_000, 10, 4), observation(30_000, 10, 4)];
  assert.equal(observedQapPressure(saturated), true);
  assert.equal(observedQapPressure([observation(0, 1, 0), observation(15_000, 3, 0), observation(30_000, 10, 0)]), true);
  assert.equal(observedQapPressure(saturated.slice(0, 2)), false);
  assert.equal(observedQapPressure(saturated.map(row => ({ ...row, error: 'observation unavailable' }))), false);
  assert.equal(observedQapPressure([observation(0, 0, 4), observation(15_000, 0, 4), observation(30_000, 0, 4)]), false);
  assert.equal(observedQapPressure([observation(0, 10, 4, 'Ingestions'), observation(15_000, 10, 4, 'Ingestions'),
    observation(30_000, 10, 4, 'Ingestions')]), false);
});

test('producer clock uncertainty includes both request endpoints and never manufactures a missing bound', () => {
  assert.equal(clockErrorBound([]), null);
  assert.equal(clockErrorBound([{ startedAtMs: 1000, endedAtMs: 1100, producerAtMs: 1050 }]), null);
  assert.equal(clockErrorBound([{ startedAtMs: 1000, endedAtMs: 1100, producerAtMs: 1050 },
    { startedAtMs: 2000, endedAtMs: 2180, producerAtMs: 1950 }]), 230);
  assert.throws(() => clockErrorBound([{ startedAtMs: 1000, endedAtMs: 999, producerAtMs: 1000 },
    { startedAtMs: 2000, endedAtMs: 2180, producerAtMs: 2100 }]), /Invalid/);
});
