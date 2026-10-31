import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { runOwnedProcess } from '../src/owned-process.js';

test('reader capture preserves output, private permissions and bounded stderr tail', async t => {
  const root = mkdtempSync(path.join(process.cwd(), '.reader-capture-test-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const reader = path.join(root, 'reader.mjs');
  writeFileSync(reader, `#!${process.execPath}\nconsole.log(JSON.stringify({ok:true}));process.stderr.write('x'.repeat(100000)+'tail');`, { mode: 0o700 });
  const out = path.join(root, 'out');
  const err = path.join(root, 'err');
  await runOwnedProcess(process.execPath, [
    path.join(process.cwd(), 'projects/loadtest/scripts/capture-reader.ts'), reader, 'summarize', 'input', out, err], {
    cwd: root, env: process.env, timeoutMs: 5000,
  });
  assert.deepEqual(JSON.parse(readFileSync(out, 'utf8')), { ok: true });
  const tail = readFileSync(err, 'utf8');
  assert.equal(tail.length, 65536);
  assert(tail.endsWith('tail'));
  assert.equal(statSync(err).mode & 0o777, 0o600);
});
