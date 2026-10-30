import assert from 'node:assert/strict';
import { test } from 'node:test';
import { maintenanceArgs, maintenanceReport } from './maintenance.js';
import { profile } from './profile.js';

const config = profile({ format: 1, rowsPerFile: 2, filesPerPartition: 2, partitions: 4, appendBatches: 4,
  appendIntervalMs: 100, observeSeconds: 5, sampleIntervalMs: 1000, qapWaitSeconds: 30,
  vacuumRetentionSeconds: 0 }, true);

test('ordinary zero-retention CLI acknowledgement is explicit in dry-run and apply', () => {
  for (const apply of [false, true]) {
    const args = maintenanceArgs('file:///tmp/fixture/', 'fixture', 100_000, 'vacuum', config, apply);
    assert.ok(args.includes('--allow-short-vacuum-retention'));
    assert.equal(args[args.indexOf('--vacuum-retention-secs') + 1], '0');
    assert.equal(args.includes('--apply'), apply);
    assert.ok(!args.includes('--unsafe-short-vacuum-for-tests'));
  }
});
test('broad/concurrent maintenance never bypasses writer-frontier protection', () => {
  const broad = maintenanceArgs('file:///tmp/fixture/', 'fixture', 100_000, 'broad-optimize', config, true);
  assert.ok(!broad.includes('--sealed-final'));
  const partial = maintenanceArgs('file:///tmp/fixture/', 'fixture', 100_000, 'partial-optimize', config, true);
  assert.equal(partial[partial.indexOf('--latest-partitions') + 1], '2');
});
test('fresh tombstones use the real VACUUM clock while PURGE retains the fixed partition anchor', () => {
  const vacuum = maintenanceArgs('file:///tmp/fixture/', 'fixture', 100_000, 'vacuum', config, true, 250_000);
  const purge = maintenanceArgs('file:///tmp/fixture/', 'fixture', 100_000, 'purge', config, true, 250_000);
  assert.equal(vacuum[vacuum.indexOf('--anchor-ms') + 1], '250000');
  assert.equal(purge[purge.indexOf('--anchor-ms') + 1], '100000');
  assert.equal(purge[purge.indexOf('--purge-retention-secs') + 1], '86400');
});
test('physical deletion and logical removals stay distinct and missing counts fail', () => {
  const result = maintenanceReport(JSON.stringify({ report: {
    files_deleted: 2, files_removed: 5, outcome: 'complete', interrupted: false,
  } }));
  assert.equal(result.filesDeleted, 2);
  assert.equal(result.filesRemoved, 5);
  assert.throws(() => maintenanceReport('{"report":{"files_removed":2}}'), /outcome/);
  assert.throws(() => maintenanceReport('{}'), /exactly one/);
});
