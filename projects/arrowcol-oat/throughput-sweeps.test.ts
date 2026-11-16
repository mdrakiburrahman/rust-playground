import assert from 'node:assert/strict';
import test from 'node:test';
import { buildThroughputManifests } from './throughput-sweeps.js';

test('generates bounded local single-writer manifests from the tunables registry', () => {
  const manifests = buildThroughputManifests({ tunables: { client: { workers: 16 }, maintenance: { enabled: true } } });
  assert.deepEqual(manifests.map(manifest => manifest.cohort), ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8']);
  for (const manifest of manifests) {
    assert.equal(manifest.mode, 'local');
    assert.equal(manifest.campaign, 'single_hot_table_campaign');
    assert.equal(manifest.registry_status, 'pending-E1-registry');
    const invariants = manifest.invariants as { writer_replicas: number; maintenance: { enabled: boolean; apply: boolean; operations: string[] } };
    assert.equal(invariants.writer_replicas, 1);
    assert.equal(invariants.maintenance.enabled, true);
    assert.equal(invariants.maintenance.apply, true);
    assert.deepEqual(invariants.maintenance.operations, ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log']);
    const comparisons = manifest.comparisons as Array<{ candidates: unknown[] }>;
    assert.ok(comparisons.every(comparison => comparison.candidates.length <= 4));
    const baseline = manifest.baseline_tunables as { maintenance: { enabled: boolean; apply: boolean; operations: string[] }; client: { workers: number } };
    assert.equal(baseline.client.workers, 16);
    assert.equal(baseline.maintenance.enabled, true);
    assert.equal(baseline.maintenance.apply, true);
    assert.deepEqual(baseline.maintenance.operations, ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log']);
  }
  assert.throws(() => buildThroughputManifests({}), /registry must contain/);
});
