import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { brotliDecompressSync } from 'node:zlib';
import { evidenceArchiveScript } from './observe.js';

test('evidence frames fit Run Command and preserve exact bytes and missing-file status', async context => {
  const root = await mkdtemp(path.join(tmpdir(), 'storage-evidence-'));
  context.after(() => rm(root, { recursive: true, force: true }));
  const payload = Buffer.concat(Array.from({ length: 512 }, (_, index) => createHash('sha256').update(String(index)).digest()));
  await writeFile(path.join(root, 'payload.bin'), payload);
  const output = execFileSync(process.execPath, ['-e', evidenceArchiveScript(root, ['payload.bin', 'missing.log'])], { encoding: 'utf8' }).trim();
  assert.ok(Buffer.byteLength(output) + 128 < 4096);
  const metadata = JSON.parse(output.slice('STORAGE_SPIKE_ARCHIVE:'.length)) as { count: number; total: number; sha256: string; part: string };
  const archive = await readFile(path.join(root, 'collected.json.br'));
  assert.ok(metadata.count > 1);
  assert.equal(archive.length, metadata.total);
  assert.equal(createHash('sha256').update(archive).digest('hex'), metadata.sha256);
  const first = Buffer.from(metadata.part, 'base64');
  assert.deepEqual(first, archive.subarray(0, first.length));
  const contents = JSON.parse(brotliDecompressSync(archive).toString()) as { data: Record<string, string>; unavailable: string[] };
  assert.deepEqual(Buffer.from(contents.data['payload.bin'], 'base64'), payload);
  assert.deepEqual(contents.unavailable, ['missing.log']);
});
