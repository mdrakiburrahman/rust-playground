import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { open } from 'node:fs/promises';
import { canonical_json, type Azure } from './bootstrap.js';
import { requireEvidence } from './benchmark-evidence-json.js';
import type { DirectStopOrigin } from './benchmark-stop-patch.js';
import type { RunCleanupAuthority } from './run_fic.js';

const CHUNK_BYTES = 4 * 1024 ** 2;
const sha256 = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex');
export interface CapturedEvidence {
  key: string;
  path: string;
  bytes: number;
  sha256: string;
}

/**
 * Persist the actual bytes, not just a local checksum. A failed transfer leaves
 * immutable unsealed evidence; it never creates a successful bundle.
 */
export async function persistDirectEvidence(azure: Azure, authority: RunCleanupAuthority,
  origin: DirectStopOrigin, files: readonly CapturedEvidence[]) {
  const uuid = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
  requireEvidence([origin.runId, origin.namespaceUid, origin.owner, origin.cloudGeneration].every(s => uuid.test(s)) &&
    Number.isSafeInteger(origin.fencingToken) && origin.fencingToken > 0 && /^[a-f0-9]{40}$/.test(origin.sourceRevision),
  'Direct evidence custody has no exact run origin');
  requireEvidence(files.length > 0 && new Set(files.map(item => item.key)).size === files.length, 'Direct evidence keys are incomplete or duplicated');
  const prefix = `_arrowcol-custody/direct/${origin.runId}/${origin.namespaceUid}/evidence`;
  const writeVerified = async (path: string, bytes: Buffer) => {
    await authority.assertCurrent();
    const [, response] = await azure.blob('PUT', azure.c.checkpoints, path, undefined, bytes,
      { 'x-ms-blob-type': 'BlockBlob', 'If-None-Match': '*' });
    const etag = response.etag;
    requireEvidence(response[':status'] === '201' && etag && /^"[^"\r\n]+"$/.test(etag), 'Evidence write outcome UNKNOWN');
    await authority.assertCurrent();
    const [received, rem] = await azure.blob('GET', azure.c.checkpoints, path, undefined, undefined, { 'If-Match': etag });
    requireEvidence(rem.etag === etag && received?.equals(bytes), 'Durable evidence readback mismatch');
    return { path, etag, bytes: bytes.length, sha256: sha256(bytes) };
  };
  const preserved = [];
  for (const input of files) {
    requireEvidence(/^[a-z0-9][a-z0-9_.-]{0,127}$/.test(input.key) && /^[a-f0-9]{64}$/.test(input.sha256) &&
      Number.isSafeInteger(input.bytes) && input.bytes >= 0 && input.bytes < 4 * 1024 ** 3, 'Invalid original evidence identity or size');
    await authority.assertCurrent();
    const reader = await open(input.path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    try {
      const stat = await reader.stat();
      requireEvidence(stat.isFile() && stat.size === input.bytes, 'Original evidence file size or type changed');
      const digest = createHash('sha256'), chunks = [];
      let total = 0;
      const block = Buffer.alloc(Math.min(Math.max(1, input.bytes), CHUNK_BYTES));
      do {
        const length = Math.min(input.bytes - total, block.length);
        let lengthRead = 0;
        while (lengthRead < length) {
          const next = await reader.read(block, lengthRead, length - lengthRead);
          requireEvidence(next.bytesRead > 0, 'Original evidence ended before its bound size');
          lengthRead += next.bytesRead;
        }
        const original = block.subarray(0, length);
        digest.update(original);
        chunks.push(await writeVerified(`${prefix}/${input.key}/${String(chunks.length).padStart(6, '0')}`, original));
        total += length;
      } while (total < input.bytes);
      requireEvidence((await reader.read(block, 0, 1)).bytesRead === 0 && digest.digest('hex') === input.sha256,
        'Original evidence bytes changed during durable transfer');
      preserved.push({ key: input.key, bytes: input.bytes, sha256: input.sha256, chunks });
    } finally { await reader.close(); }
  }
  const content = Buffer.from(canonical_json({ schema_version: 1, origin, files: preserved }));
  requireEvidence(content.length <= 8 * 1024 ** 2, 'Evidence directory is too large to seal');
  return writeVerified(`${prefix}/seal.json`, content);
}
