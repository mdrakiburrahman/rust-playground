import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parseOracle, physicalTable } from './driver.js';
import { fingerprint } from './scenarios.js';

function oracle() {
  const records = [[0, 7, '2026-01-01-00-00'], [1, 17, '2026-01-01-00-00']];
  return { ok: true, action: 'inspect', table: 'experiment_fixture', version: 3,
    files: [{ path: '__arrowcol_ingestion=2026-01-01-00-00/a.parquet', bytes: 42, partition: '2026-01-01-00-00' }],
    rows: records.length, records, fingerprint: fingerprint(records) };
}
test('Rust Delta oracle is exact and file identity includes its source generation', () => {
  const input = oracle();
  const result = parseOracle(input, 'fixture');
  assert.deepEqual(result.columns, ['id', 'value', '__arrowcol_ingestion']);
  assert.equal(result.files[0]?.path, 'experiment_fixture/' + input.files[0]!.path);
  assert.equal(physicalTable('fixture'), 'experiment_fixture');
  assert.throws(() => parseOracle(input, 'other_source'), /mismatched/);
});
test('malformed, incomplete or over-budget oracles cannot become correctness evidence', () => {
  assert.throws(() => parseOracle({ ...oracle(), rows: 0 }, 'fixture'), /missing/);
  assert.throws(() => parseOracle({ ...oracle(), fingerprint: 'wrong' }, 'fixture'), /fingerprint/);
  assert.throws(() => parseOracle({ ...oracle(), records: [[1, 'not-a-number', 'p']] }, 'fixture'), /record value/);
  assert.throws(() => parseOracle({ ...oracle(), files: [{ path: '../escape', partition: 'p', bytes: 10 }] }, 'fixture'), /relative/);
  const input = oracle();
  assert.throws(() => parseOracle({ ...input, files: [...input.files, ...input.files] }, 'fixture'), /duplicate files/);
});
