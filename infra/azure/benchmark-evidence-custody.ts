import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { open } from 'node:fs/promises';
import { canonical_json, type Azure } from './bootstrap.js';
import { evidenceList, evidenceObject, requireEvidence } from './benchmark-evidence-json.js';
import type { DirectStopOrigin } from './benchmark-stop-patch.js';
import type { RunCleanupAuthority } from './run_fic.js';

const CHUNK_BYTES = 4 * 1024 ** 2;
const sha256 = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex');
export const directEvidencePrefix = (origin: Pick<DirectStopOrigin, 'runId' | 'namespaceUid'>) =>
  `_arrowcol-custody/direct/${origin.runId}/${origin.namespaceUid}/evidence`;

export interface DirectEvidenceSeal { path: string; etag: string; bytes: number; sha256: string }

export async function readDirectEvidenceSeal(azure: Azure, authority: RunCleanupAuthority, origin: DirectStopOrigin,
  expected?: DirectEvidenceSeal): Promise<DirectEvidenceSeal | undefined> {
  const path = `${directEvidencePrefix(origin)}/seal.json`;
  await authority.assertCurrent();
  const [bytes, headers] = await azure.blob('GET', azure.c.checkpoints, path, undefined, undefined,
    expected ? { 'If-Match': expected.etag } : {}, !expected, true);
  if (!expected && bytes === null && Object.keys(headers).length === 0) return undefined; // Azure.blob missing_ok maps only 404 to this pair.
  requireEvidence(bytes && bytes.length <= 8 * 1024 ** 2 && headers[':status'] === '200' &&
    headers.etag && /^"[^"\r\n]+"$/.test(headers.etag), 'Terminal evidence seal read outcome is unknown');
  const seal = { path, etag: headers.etag, bytes: bytes.length, sha256: sha256(bytes) };
  if (expected) {
    requireEvidence(canonical_json(seal) === canonical_json(expected), 'Terminal evidence seal changed');
    return seal;
  }
  const manifest: unknown = JSON.parse(bytes.toString('utf8'));
  const body = evidenceObject(manifest);
  requireEvidence(body.schema_version === 2 && canonical_json(body.origin) === canonical_json(origin) &&
    bytes.equals(Buffer.from(canonical_json(body))), 'No verified seal for the original direct run');
  const files = evidenceList(body.files).map(evidenceObject);
  for (const file of files) {
    requireEvidence(typeof file.key === 'string' && /^[a-z0-9][a-z0-9_.-]{0,127}$/.test(file.key) &&
      typeof file.bytes === 'number' && Number.isSafeInteger(file.bytes) && file.bytes >= 0 &&
      file.bytes < 4 * 1024 ** 3 && typeof file.sha256 === 'string' && /^[a-f0-9]{64}$/.test(file.sha256),
    'Invalid sealed evidence file identity');
    const chunks = evidenceList(file.chunks).map(evidenceObject);
    requireEvidence(chunks.length === Math.max(1, Math.ceil(file.bytes / CHUNK_BYTES)) &&
      chunks.every((chunk, index) =>
        chunk.path === `${directEvidencePrefix(origin)}/${file.key}/${String(index).padStart(6, '0')}` &&
        chunk.bytes === Math.min(CHUNK_BYTES, (file.bytes as number) - index * CHUNK_BYTES) &&
        typeof chunk.etag === 'string' && /^"[^"\r\n]+"$/.test(chunk.etag) &&
        typeof chunk.sha256 === 'string' && /^[a-f0-9]{64}$/.test(chunk.sha256)),
    'Invalid sealed evidence chunk identities');
  }
  const keys = new Set(files.map(file => file.key));
  requireEvidence(keys.size === files.length && keys.has('direct-terminal.json') && keys.has('writer-stdout.raw') &&
    keys.has('writer-stderr.raw') && Array.from({ length: origin.clients }, (_, index) => index)
      .every(index => keys.has(`client-${index}-stdout.raw`) && keys.has(`client-${index}-stderr.raw`)),
  'The original seal does not identify all required evidence streams');
  await authority.assertCurrent();
  return seal;
}

export async function readSealedDirectTerminal(azure: Azure, authority: RunCleanupAuthority,
  origin: DirectStopOrigin, seal: DirectEvidenceSeal) {
  await authority.assertCurrent();
  const [manifest, headers] = await azure.blob('GET', azure.c.checkpoints, seal.path, undefined, undefined, { 'If-Match': seal.etag });
  requireEvidence(seal.path === `${directEvidencePrefix(origin)}/seal.json` && manifest &&
    headers.etag === seal.etag && manifest.length === seal.bytes && sha256(manifest) === seal.sha256,
  'Original terminal seal changed before reading its proof.');
  const files = evidenceList(evidenceObject(JSON.parse(manifest.toString('utf8'))).files).map(evidenceObject);
  const terminal = files.find(file => file.key === 'direct-terminal.json');
  requireEvidence(terminal && typeof terminal.bytes === 'number' && terminal.bytes > 0 && terminal.bytes <= 32 * 1024 ** 2,
    'Original terminal proof is missing or oversized.');
  const parts: Buffer[] = [];
  for (const [index, raw] of evidenceList(terminal.chunks).entries()) {
    const chunk = evidenceObject(raw);
    const key = `${directEvidencePrefix(origin)}/direct-terminal.json/${String(index).padStart(6, '0')}`;
    requireEvidence(chunk.path === key && typeof chunk.etag === 'string', 'Original terminal proof chunk identity changed.');
    await authority.assertCurrent();
    const [bytes, version] = await azure.blob('GET', azure.c.checkpoints, key, undefined, undefined, { 'If-Match': chunk.etag });
    requireEvidence(bytes && version.etag === chunk.etag && bytes.length === chunk.bytes && sha256(bytes) === chunk.sha256,
      'Original terminal proof chunk is missing or changed.');
    parts.push(bytes);
  }
  const bytes = Buffer.concat(parts);
  requireEvidence(bytes.length === terminal.bytes && sha256(bytes) === terminal.sha256, 'Original terminal proof content changed.');
  await authority.assertCurrent();
  return { raw: bytes.toString('utf8'), terminal: evidenceObject(JSON.parse(bytes.toString('utf8'))), delta: files.filter(file => /^delta-\d{20}\.json$/.test(String(file.key))) };
}

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
  origin: DirectStopOrigin, files: readonly CapturedEvidence[], verifyTerminal: () => Promise<void>) {
  const uuid = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
  requireEvidence([origin.runId, origin.namespaceUid, origin.owner, origin.cloudGeneration].every(s => uuid.test(s)) &&
    Number.isSafeInteger(origin.fencingToken) && origin.fencingToken > 0 && /^[a-f0-9]{40}$/.test(origin.sourceRevision),
  'Direct evidence custody has no exact run origin');
  requireEvidence(files.length > 0 && new Set(files.map(item => item.key)).size === files.length, 'Direct evidence keys are incomplete or duplicated');
  const prefix = directEvidencePrefix(origin);
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
  const content = Buffer.from(canonical_json({ schema_version: 2, origin, files: preserved }));
  requireEvidence(content.length <= 8 * 1024 ** 2, 'Evidence directory is too large to seal');
  await verifyTerminal();
  return writeVerified(`${prefix}/seal.json`, content);
}
