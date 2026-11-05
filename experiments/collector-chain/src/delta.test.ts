import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { deltaTables, readLog } from './delta.js';

function table(root: string, name: string, commits: object[][]): string {
  const log = path.join(root, name, '_delta_log');
  fs.mkdirSync(log, { recursive: true });
  commits.forEach((actions, version) => fs.writeFileSync(
    path.join(log, `${String(version).padStart(20, '0')}.json`), actions.map(action => JSON.stringify(action)).join('\n')));
  return path.join(root, name);
}

const add = (file: string, rows: number) => ({ add: { path: file, stats: JSON.stringify({ numRecords: rows }) } });

test('readLog counts data commits, publications and active rows', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'delta-'));
  const location = table(root, 'a/b', [
    [{ protocol: {} }, { metaData: {} }],
    [add('1.parquet', 100), add('2.parquet', 100), { txn: { appId: 'p1' } }, { txn: { appId: 'p2' } }],
    [{ remove: { path: '1.parquet' } }, add('3.parquet', 100)],
  ]);
  assert.deepEqual(readLog(location), {
    commits: [{ version: 1, files: 2, rows: 200, transactions: 2 }, { version: 2, files: 1, rows: 100, transactions: 0 }],
    activeFiles: 2,
    rows: 200,
  });
  assert.deepEqual(deltaTables(root), ['a/b']);
});

test('readLog rejects add actions without row statistics', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'delta-'));
  assert.throws(() => readLog(table(root, 't', [[{ add: { path: 'x.parquet' } }]])), /numRecords/);
});