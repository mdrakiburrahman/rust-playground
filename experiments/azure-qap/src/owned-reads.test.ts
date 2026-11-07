import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { ownedReads } from './owned-reads.js';

const marker = 'arrowcol-qap/run_1/reload';
const clientRequestId = '11111111-1111-4111-8111-111111111111';
const serverRequestId = '22222222-2222-4222-8222-222222222222';
const time = '2026-11-07T11:00:00Z';
const request = { event: 'request', time, marker, clientRequestId, method: 'GET',
  url: 'https://arrowcollector1.blob.core.windows.net/experimentqap/runs/run_1/reload/data.parquet' };
const response = { event: 'response', time, marker, clientRequestId, serverRequestId, status: 200 };

test('owned receipts require complete per-request correlation and never expose query credentials', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'qap-owned-reads-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const file = path.join(directory, 'reads.jsonl');
  const write = (rows: unknown[]) => fs.writeFileSync(file, rows.map(row => JSON.stringify(row)).join('\n') + '\n');
  write([request, response]);
  assert.deepEqual(ownedReads(file, marker), { marker, requestIds: [clientRequestId, serverRequestId] });
  write([request]);
  assert.throws(() => ownedReads(file, marker), /incomplete/);
  write([response]);
  assert.throws(() => ownedReads(file, marker), /no matching request/);
  write([request, { ...response, serverRequestId: null }]);
  assert.throws(() => ownedReads(file, marker), /no server request ID/);
  write([request, response, response]);
  assert.throws(() => ownedReads(file, marker), /no matching request/);
  write([{ ...request, url: request.url + '?sig=not-a-secret' }, response]);
  assert.throws(() => ownedReads(file, marker), /must not contain credentials/);
  write([request, { ...response, marker: 'arrowcol-qap/other_1/reload' }]);
  assert.throws(() => ownedReads(file, marker), /another trial/);
  write([]);
  assert.throws(() => ownedReads(file, marker), /empty/);
});
