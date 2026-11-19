import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, open, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { evidenceObject, parseEvidence, requireEvidence, uint } from './benchmark-evidence-json.js';
import { collectorReceipt, terminalManifest } from './benchmark-extract.js';
import { auditManifest, MAX_MANIFEST_BYTES } from './benchmark-manifest-audit.js';
import { reconcilePublications, type DirectReconciliationTiming } from './benchmark-reconcile.js';

const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
const MAX_LINE_BYTES = 8 * 1024 * 1024;
type Hashes = Record<string, string>;

async function openEvidence(path: string) {
  requireEvidence((await lstat(path)).isFile(), `Refusing non-regular evidence file: ${path}`);
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  if (!(await file.stat()).isFile()) {
    await file.close();
    throw new Error(`Refusing non-regular evidence file: ${path}`);
  }
  return file;
}

async function readManifest(path: string, hashes: Hashes): Promise<unknown> {
  const file = await openEvidence(path);
  try {
    const bytes = Buffer.alloc(MAX_MANIFEST_BYTES + 1);
    let length = 0;
    while (length < bytes.length) {
      const { bytesRead } = await file.read(bytes, length, bytes.length - length);
      if (bytesRead === 0) break;
      length += bytesRead;
    }
    requireEvidence(length <= MAX_MANIFEST_BYTES, 'Manifest exceeds 1 MiB');
    const raw = bytes.subarray(0, length);
    hashes[path] = createHash('sha256').update(raw).digest('hex');
    return parseEvidence(decoder.decode(raw));
  } finally { await file.close(); }
}

async function* evidenceLines(path: string, hashes: Hashes) {
  const file = await openEvidence(path);
  const stream = file.createReadStream();
  const digest = createHash('sha256');
  let parts: Buffer[] = [], length = 0;
  const decode = () => decoder.decode(Buffer.concat(parts, length));
  for await (const buffer of stream) {
    const chunk = buffer as Buffer;
    digest.update(chunk);
    let start = 0;
    while (start < chunk.length) {
      const newline = chunk.indexOf(10, start), end = newline < 0 ? chunk.length : newline + 1;
      const part = chunk.subarray(start, end);
      length += part.length;
      requireEvidence(length <= MAX_LINE_BYTES, `Oversized JSON line in ${path}`);
      parts.push(part);
      if (newline >= 0) {
        yield decode();
        parts = [];
        length = 0;
      }
      start = end;
    }
  }
  if (length) yield decode();
  hashes[path] = digest.digest('hex');
}

async function* jsonLines(path: string, hashes: Hashes, rawCollector = false) {
  for await (const line of evidenceLines(path, hashes)) {
    if (!rawCollector || collectorReceipt(line)) yield parseEvidence(line);
  }
}

async function readRawClient(path: string, hashes: Hashes) {
  let manifest: unknown;
  for await (const line of evidenceLines(path, hashes)) {
    const raw = terminalManifest(line);
    if (raw === undefined) continue;
    requireEvidence(manifest === undefined, 'Multiple terminal client manifests');
    requireEvidence(Buffer.byteLength(raw) <= MAX_MANIFEST_BYTES, 'Manifest exceeds 1 MiB');
    const parsed = evidenceObject(parseEvidence(line.slice('OAT_CLIENT_METRICS '.length)));
    requireEvidence(parsed.phase === 'completed', 'Client reports terminal failure');
    manifest = parseEvidence(raw);
  }
  requireEvidence(manifest !== undefined, 'No terminal client manifest');
  return manifest;
}

export async function auditEvidenceFiles(manifests: string[], collector: string, deltaDirectory: string, baseline: bigint,
  raw?: { expectedFinalVersion: bigint; timing?: DirectReconciliationTiming | null }) {
  requireEvidence((await lstat(deltaDirectory)).isDirectory(), 'Expected a non-symlink Delta directory');
  const hashes: Hashes = Object.create(null);
  const inputs = [];
  for (const path of manifests) {
    requireEvidence(!Object.hasOwn(hashes, path), 'Duplicate manifest path');
    inputs.push(raw ? await readRawClient(path, hashes) : await readManifest(path, hashes));
  }
  const manifestHashes = { ...hashes };
  const delta = new Map<bigint, AsyncIterable<unknown>>();
  for (const name of (await readdir(deltaDirectory)).sort()) {
    if (!name.endsWith('.json')) continue;
    requireEvidence(/^\d{20}\.json$/.test(name), 'Delta evidence directory contains a non-version JSON file');
    const version = uint(BigInt(name.slice(0, 20)), 'Delta version');
    delta.set(version, jsonLines(join(deltaDirectory, name), hashes));
  }
  if (raw) requireEvidence(delta.size > 0 && BigInt(delta.size - 1) === raw.expectedFinalVersion,
    'Downloaded Delta history differs from the frozen final version');
  const report = await reconcilePublications(inputs, jsonLines(collector, hashes, raw !== undefined), delta, baseline,
    raw ? { diagnostic: true, timing: raw.timing ?? null } : undefined);
  return { ...report, ...(raw ? { client_log_sha256: manifestHashes } : { client_manifest_sha256: manifestHashes }), evidence_sha256: hashes };
}

export function evidenceJson(value: unknown): string {
  const { rawJSON } = JSON as JSON & { rawJSON?: (text: string) => unknown };
  requireEvidence(typeof rawJSON === 'function', 'This evidence tool requires Node.js with JSON.rawJSON support');
  return JSON.stringify(value, (_key, item: unknown) =>
    typeof item === 'bigint' ? rawJSON(item.toString()) : item, 2);
}

async function main(args: string[]) {
  if (args[0] === 'manifest' && args.length === 2) {
    const hashes: Hashes = Object.create(null);
    const report = auditManifest(await readManifest(args[1], hashes));
    console.log(evidenceJson({ ...report, input_sha256: hashes[args[1]] }));
    return;
  }
  const { values } = parseArgs({ args, options: {
    manifest: { type: 'string', multiple: true }, 'collector-jsonl': { type: 'string' },
    'delta-json-dir': { type: 'string' }, 'baseline-version': { type: 'string' },
  } });
  const { manifest, 'collector-jsonl': collector, 'delta-json-dir': directory, 'baseline-version': baseline } = values;
  requireEvidence(manifest?.length && collector && directory && baseline && /^(0|[1-9]\d*)$/.test(baseline),
    'Usage: benchmark-audit.ts --manifest PATH [--manifest PATH ...] --collector-jsonl PATH --delta-json-dir DIR --baseline-version N');
  console.log(evidenceJson(await auditEvidenceFiles(manifest, collector, directory, BigInt(baseline))));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main(process.argv.slice(2));
}
