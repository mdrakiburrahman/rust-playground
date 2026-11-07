import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { nullableBar, physicalCounts, snapshot } from './delta.ts';

test('active Delta replay rejects gaps and path escape rather than counting stale or foreign files', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'schema-evolution-'));
  t.after(() => fs.rmSync(root, { recursive: true }));
  const log = path.join(root, '_delta_log');
  fs.mkdirSync(log);
  const commit = (version: number, action: unknown) =>
    fs.writeFileSync(path.join(log, String(version).padStart(20, '0') + '.json'), JSON.stringify(action) + '\n');
  commit(0, { metaData: { schemaString: '{"type":"struct","fields":[]}', partitionColumns: ['tenant'],
    configuration: { 'delta.columnMapping.mode': 'none', 'delta.enableDeletionVectors': 'false' } } });
  commit(1, { add: { path: 'tenant=evo/old.parquet', stats: '{"numRecords":8}' } });
  commit(2, { remove: { path: 'tenant=evo/old.parquet' } });
  commit(3, { add: { path: 'tenant=evo/current.parquet', stats: '{"numRecords":24}' } });
  assert.equal(snapshot(root).rows, 24);
  assert.deepEqual(snapshot(root).files, [path.join(root, 'tenant=evo/current.parquet')]);
  fs.renameSync(path.join(log, '00000000000000000003.json'), path.join(log, '00000000000000000004.json'));
  assert.throws(() => snapshot(root), /gap/);
  fs.unlinkSync(path.join(log, '00000000000000000004.json'));
  commit(3, { add: { path: '%2e%2e/foreign.parquet', stats: '{"numRecords":24}' } });
  assert.throws(() => snapshot(root), /escaped/);
});

test('nullable STRING and physical old-after nulls cannot be replaced by absent columns or empty sentinels', () => {
  assert.equal(nullableBar({ fields: [] }), false);
  assert.equal(nullableBar({ fields: [{ name: 'bar', type: 'string', nullable: true }] }), true);
  assert.throws(() => nullableBar({ fields: [{ name: 'bar', type: 'string', nullable: false }] }), /nullable/);
  const counts = [
    { client: 'old', phase: 'before', total: 8, nulls: 8, missingColumn: 8, filled: 0, badSentinel: 0 },
    { client: 'old', phase: 'after', total: 8, nulls: 8, missingColumn: 0, filled: 0, badSentinel: 0 },
    { client: 'new', phase: 'after', total: 8, nulls: 0, missingColumn: 0, filled: 8, badSentinel: 0 },
  ];
  physicalCounts({ counts }, 8);
  assert.throws(() => physicalCounts({ counts: [counts[0], { ...counts[1], missingColumn: 8 }, counts[2]] }, 8), /proof failed/);
  assert.throws(() => physicalCounts({ counts: [counts[0], counts[1], { ...counts[2], badSentinel: 8 }] }, 8), /proof failed/);
});
