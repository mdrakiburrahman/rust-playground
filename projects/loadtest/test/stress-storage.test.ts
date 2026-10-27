import assert from 'node:assert/strict';
import { linkSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { physicalInventory } from '../src/stress-storage.js';

test('physical accounting includes JSON/checkpoints/WAL/staging/orphans and deduplicates hard links', async t => {
  const root = mkdtempSync(path.resolve('projects/loadtest/test/.fixture-physical-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  for (const directory of ['_delta_log', 'quiver', 'staging']) mkdirSync(path.join(root, directory));
  const files = {
    '_delta_log/000.json': 11, '_delta_log/000.checkpoint.parquet': 13,
    'quiver/file.wal': 17, 'staging/file.tmp': 19, 'orphan.parquet': 23,
  };
  for (const [file, bytes] of Object.entries(files)) writeFileSync(path.join(root, file), 'x'.repeat(bytes));
  linkSync(path.join(root, 'orphan.parquet'), path.join(root, 'same-inode.parquet'));
  const inventory = await physicalInventory({ direct: root }, Date.now() + 2000);
  assert.equal(inventory.files, 5);
  assert.equal(inventory.fileBytes, 83);
  assert.equal(inventory.byKind['delta-log-json'].bytes, 11);
  assert.equal(inventory.byKind['delta-checkpoints'].bytes, 13);
  assert.equal(inventory.byKind.wal.bytes, 17);
  assert.equal(inventory.byKind.staging.bytes, 19);
  assert.equal(inventory.byKind['data-parquet'].bytes, 23);
  assert(inventory.finishedAt >= inventory.startedAt);
  symlinkSync(path.join(root, 'orphan.parquet'), path.join(root, 'escape'));
  await assert.rejects(physicalInventory({ direct: root }, Date.now() + 2000), /symlink/u);
  await assert.rejects(physicalInventory({}, Date.now() + 2000), /No physical/u);
  await assert.rejects(physicalInventory({ direct: root }, Date.now() - 1), /deadline/u);
});
