import assert from 'node:assert/strict';
import test from 'node:test';
import { buildThroughputManifests } from './throughput-sweeps.js';

function at(value: unknown, path: string[]): unknown {
  let current = value;
  for (const key of path) {
    if (typeof current !== 'object' || current === null || Array.isArray(current)) return undefined;
    current = (current as Record<string, unknown>)[key];
  }
  return current;
}

test('generates bounded local single-writer manifests from the tunables registry', () => {
  const manifests = buildThroughputManifests({ tunables: { client: { workers: 16 }, maintenance: { enabled: true } } });
  assert.deepEqual(manifests.map(manifest => manifest.cohort), ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8']);
  for (const manifest of manifests) {
    assert.equal(manifest.mode, 'local');
    assert.equal(manifest.campaign, 'single_hot_table_campaign');
    assert.equal(manifest.campaign_profile, 'single_hot_table');
    assert.equal(manifest.registry_status, 'pending-E1-registry');
    const invariants = manifest.invariants as { table: string; replicas: number[]; wal_enabled: boolean; maintenance: { enabled: boolean; apply: boolean; continuous: boolean; operations: string[] } };
    assert.equal(invariants.table, 'sqlserver_availability_replica_states');
    assert.deepEqual(invariants.replicas, [1]);
    assert.equal(invariants.wal_enabled, false);
    assert.equal(invariants.maintenance.enabled, true);
    assert.equal(invariants.maintenance.apply, true);
    assert.equal(invariants.maintenance.continuous, true);
    assert.deepEqual(invariants.maintenance.operations, ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log']);
    const comparisons = manifest.comparisons as Array<{ id: string; candidates: Array<{ id: string; overrides: Record<string, unknown> }>; candidate_limit: number }>;
    assert.ok(comparisons.every(comparison => comparison.candidates.length <= comparison.candidate_limit));
    assert.ok(comparisons.filter(comparison => !(manifest.cohort === 'C1' && comparison.id === 'codec')).every(comparison => comparison.candidate_limit === 4));
    if (manifest.cohort === 'C1') {
      const codec = comparisons.find(comparison => comparison.id === 'codec')!;
      assert.equal(codec.candidate_limit, 8);
      assert.deepEqual(codec.candidates.map(item => item.id), ['uncompressed', 'snappy', 'lz4-raw', 'gzip', 'brotli', 'zstd-1', 'zstd-3', 'zstd-6']);
      assert.deepEqual(codec.candidates.map(item => [
        at(item.overrides, ['tunables', 'parquet_writer', 'defaults', 'compression']),
        at(item.overrides, ['tunables', 'parquet_writer', 'defaults', 'compression_level']),
      ]), [
        ['uncompressed', null], ['snappy', null], ['lz4_raw', null], ['gzip', null], ['brotli', null],
        ['zstd', 1], ['zstd', 3], ['zstd', 6],
      ]);
      assert.equal((manifest.refinement as { max_candidates: number }).max_candidates, 4);
    }
    if (manifest.cohort === 'C2') {
      const columns = comparisons.find(comparison => comparison.id === 'dictionary-encoding')!;
      assert.deepEqual(columns.candidates.map(item => item.id), [
        'availability-group-name-dictionary-off', 'availability-group-name-dictionary-on',
        'last-connect-error-delta-binary-packed', 'last-connect-error-byte-stream-split',
      ]);
      assert.deepEqual(columns.candidates.map(item => Object.keys(at(item.overrides, ['tunables', 'parquet_writer', 'columns']) as object)), [
        ['availability_group_name'], ['availability_group_name'], ['last_connect_error_number'], ['last_connect_error_number'],
      ]);
    }
    if (manifest.cohort === 'C3') {
      const rows = comparisons.find(comparison => comparison.id === 'max-rows')!;
      assert.deepEqual(rows.candidates.map(item => at(item.overrides, ['tunables', 'collector', 'layout', 'max_rows'])), [16384, 65536, 131072, 1000000]);
      assert.deepEqual(rows.candidates.map(item => at(item.overrides, ['tunables', 'collector', 'batching', 'max_rows_per_flush'])), [16384, 65536, 131072, 1000000]);
      const groups = comparisons.find(comparison => comparison.id === 'row-groups')!;
      assert.deepEqual(groups.candidates.map(item => at(item.overrides, ['tunables', 'parquet_writer', 'max_row_group_rows'])), [16384, 65536, 131072, 1000000]);
      const caps = comparisons.find(comparison => comparison.id === 'rollover-cap')!;
      const capValues = [134217728, 268435456, 536870912];
      assert.deepEqual(caps.candidates.map(item => at(item.overrides, ['tunables', 'collector', 'layout', 'max_input_bytes'])), capValues);
      assert.deepEqual(caps.candidates.map(item => at(item.overrides, ['tunables', 'collector', 'batching', 'max_arrow_memory_per_flush'])), capValues);
      assert.deepEqual(caps.candidates.map(item => at(item.overrides, ['tunables', 'collector', 'batching', 'adaptive_file', 'max_input_bytes'])), capValues);
    }
    if (manifest.cohort === 'C4') {
      const sort = comparisons.find(comparison => comparison.id === 'sort-key')!;
      assert.deepEqual(sort.candidates.map(item => item.id), ['none', 'sample-time', 'replica-sample-time', 'group-replica-sample-time']);
      assert.equal(at(sort.candidates[0]!.overrides, ['tunables', 'collector', 'layout', 'enabled']), true);
      assert.deepEqual(sort.candidates.map(item => at(item.overrides, ['tunables', 'collector', 'layout', 'sort_columns'])), [
        [], ['sample_time_utc'], ['replica_server_name', 'sample_time_utc'],
        ['availability_group_id', 'replica_server_name', 'sample_time_utc'],
      ]);
      assert.deepEqual(at(sort.candidates[0]!.overrides, ['tunables', 'parquet_writer', 'sorting_columns']), []);
      assert.deepEqual(sort.candidates.map(item => (at(item.overrides, ['tunables', 'parquet_writer', 'sorting_columns']) as Array<{ path: string; descending: boolean; nulls_first: boolean }>).map(column => [column.path, column.descending, column.nulls_first])), [
        [], [['sample_time_utc', false, false]],
        [['replica_server_name', false, false], ['sample_time_utc', false, false]],
        [['availability_group_id', false, false], ['replica_server_name', false, false], ['sample_time_utc', false, false]],
      ]);
    }
    if (manifest.cohort === 'C8') {
      const pending = manifest.pending_candidate_sets as Array<{ id: string; status: string; candidates: unknown[] }>;
      assert.deepEqual(pending.map(item => [item.id, item.status]), [['staging-overlap', 'pending-L1b']]);
      assert.equal(comparisons.some(item => item.id === 'staging-overlap'), false);
    }
    const baseline = manifest.baseline_tunables as { maintenance: { enabled: boolean; apply: boolean; operations: string[] }; client: { workers: number } };
    assert.equal(baseline.client.workers, 16);
    assert.equal(baseline.maintenance.enabled, true);
    assert.equal(baseline.maintenance.apply, true);
    assert.deepEqual(baseline.maintenance.operations, ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log']);
  }
  assert.throws(() => buildThroughputManifests({}), /registry must contain/);
});
