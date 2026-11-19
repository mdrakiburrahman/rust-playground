import { createHash } from 'node:crypto';
import { lstat, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { parse_xml, type Azure } from './bootstrap.js';
import { evidenceObject, evidenceText, parseEvidence, requireEvidence, uint } from './benchmark-evidence-json.js';
import type { CapturedEvidence } from './benchmark-evidence-custody.js';
import type { RunCleanupAuthority } from './run_fic.js';

const sha = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex');
const strong = (value: string | undefined) => typeof value === 'string' && /^"[^"\r\n]+"$/.test(value);
const MAX_COMMIT_BYTES = 16 * 1024 ** 2;
export interface DirectDeltaBaseline {
  table_root: string;
  table_id: string;
  version: number;
  last_commit_sha256: string;
}

export function directDeltaReplay() {
  const active = new Map<string, { path: string; size: bigint }>();
  const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
  let tableId: string | undefined, protocol: string | undefined, version = -1;
  const path = (value: unknown) => {
    const text = evidenceText(value);
    requireEvidence(text.endsWith('.parquet') && text.split('/').every(part =>
      /^[A-Za-z0-9_.=+-]+$/.test(part) && part !== '.' && part !== '..'), 'Unsupported physical Delta data path');
    return text;
  };
  return {
    apply(bytes: Buffer) {
      const lines = decoder.decode(bytes).split('\n');
      if (lines.at(-1) === '') lines.pop();
      requireEvidence(lines.length && lines.length <= 1_000_000, 'Delta commit has no actions or exceeded its action bound');
      const removes = new Set<string>(), adds = new Map<string, { path: string; size: bigint }>();
      let nextTableId = tableId, nextProtocol = protocol;
      for (const line of lines) {
        const action = evidenceObject(parseEvidence(line)), keys = Object.keys(action);
        requireEvidence(keys.length === 1, 'Expected one Delta action per line');
        switch (keys[0]) {
          case 'metaData': {
            const next = evidenceText(evidenceObject(action.metaData).id);
            requireEvidence(/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(next) &&
              (nextTableId === undefined || next === nextTableId), 'Delta table identity changed');
            nextTableId = next;
            break;
          }
          case 'protocol': {
            const p = evidenceObject(action.protocol);
            const empty = (x: unknown) => x === undefined || Array.isArray(x) && x.length === 0;
            const simple = p.minReaderVersion === 1n && (p.minWriterVersion === 1n || p.minWriterVersion === 2n) && empty(p.writerFeatures);
            const invariants = p.minReaderVersion === 3n && p.minWriterVersion === 7n && Array.isArray(p.writerFeatures) &&
              p.writerFeatures.length === 1 && p.writerFeatures[0] === 'invariants';
            const id = `${p.minReaderVersion}/${p.minWriterVersion}/${invariants ? 'invariants' : 'legacy'}`;
            requireEvidence((simple || invariants) && empty(p.readerFeatures) &&
              Object.keys(p).every(k => ['minReaderVersion', 'minWriterVersion', 'readerFeatures', 'writerFeatures'].includes(k)) &&
              (!nextProtocol || nextProtocol === id), 'Unsupported or changed direct Delta protocol');
            nextProtocol = id;
            break;
          }
          case 'add': {
            const add = evidenceObject(action.add), file = path(add.path), size = uint(add.size, 'Delta physical file size');
            requireEvidence(!adds.has(file) && (add.deletionVector === undefined || add.deletionVector === null) && size > 0n,
              'Ambiguous or unsupported Delta add');
            adds.set(file, { path: file, size });
            break;
          }
          case 'remove': {
            const remove = evidenceObject(action.remove), file = path(remove.path);
            requireEvidence(active.has(file) && !removes.has(file) && (remove.deletionVector === undefined || remove.deletionVector === null),
              'Ambiguous or unsupported Delta remove');
            removes.add(file);
            break;
          }
          case 'txn':
          case 'commitInfo':
            break;
          default: throw new Error('Unsupported direct Delta action');
        }
      }
      requireEvidence(nextTableId && nextProtocol, 'Delta creation identity or protocol is absent');
      requireEvidence([...adds].every(([file]) => !active.has(file) && !removes.has(file)),
        'Delta reuses a physical path in the active set or same commit');
      requireEvidence(active.size - removes.size + adds.size <= 1_000_000, 'Delta active file evidence exceeded its bound');
      for (const file of removes) active.delete(file);
      for (const [file, add] of adds) active.set(file, add);
      tableId = nextTableId;
      protocol = nextProtocol;
      version++;
      return tableId;
    },
    get snapshot() { return { table_id: tableId, version, active_files: [...active.values()] }; },
  };
}

/** Call after the original writer and maintenance have positively stopped. */
export async function captureFrozenDelta(azure: Azure, authority: RunCleanupAuthority, runId: string,
  tableRoot: string, baseline: DirectDeltaBaseline, directory: string) {
  const expectedPrefix = `direct-baseline-1/runs/${runId}/dataflow/`;
  requireEvidence(/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(runId) && tableRoot.startsWith(expectedPrefix) &&
    tableRoot.split('/').every(part => /^[A-Za-z0-9_.-]+$/.test(part) && part !== '.' && part !== '..') &&
    baseline.table_root === tableRoot, 'Delta root differs from its original run/catalog binding');
  const dir = await lstat(directory);
  requireEvidence(dir.isDirectory() && (dir.mode & 0o077) === 0, 'Delta evidence directory is not private');
  const guard = () => authority.assertCurrent();
  const prefix = `${tableRoot}/_delta_log/`;
  async function inventory() {
    const records = new Map<string, string>(), seenMarkers = new Set<string>();
    let newestCheckpoint = -1n;
    let marker = '';
    do {
      await guard();
      const [data, headers] = await azure.blob('GET', azure.c.data, '', { restype: 'container', comp: 'list',
        prefix, marker, maxresults: '1000' }, undefined, { 'Cache-Control': 'no-cache' }, false, true);
      requireEvidence(headers[':status'] === '200' && headers['x-ms-request-id'] && data && data.length <= 8 * 1024 ** 2,
        'Delta JSON inventory has no definitive original response');
      const root = parse_xml(data.toString('utf8')), field = (name: string) => root.children.find(child => child.name === name);
      requireEvidence(root.name === 'EnumerationResults' && field('Prefix')?.text === prefix && field('NextMarker') &&
        field('Blobs'), 'Delta inventory response escaped its original root');
      for (const blob of field('Blobs')!.children) {
        const name = blob.children.find(child => child.name === 'Name')?.text;
        requireEvidence(blob.name === 'Blob' && typeof name === 'string' && name.startsWith(prefix), 'Delta inventory response returned an unrelated object');
        const basename = name.slice(prefix.length);
        const checkpoint = /^(\d{20})\.checkpoint(?:\.|$)/.exec(basename);
        if (checkpoint && BigInt(checkpoint[1]) > newestCheckpoint) newestCheckpoint = BigInt(checkpoint[1]);
        if (!/^\d{20}\.json$/.test(basename) && basename !== '_last_checkpoint' && !checkpoint) continue;
        const etag = blob.children.find(child => child.name === 'Properties')?.children.find(child => child.name === 'Etag')?.text;
        requireEvidence(etag && strong(etag) && !records.has(basename), 'Delta JSON object has no unique strong version');
        records.set(basename, etag);
      }
      marker = field('NextMarker')!.text;
      requireEvidence(!seenMarkers.has(marker) && seenMarkers.size < 100 && records.size <= 20_000, 'Delta inventory exceeded its custody bounds');
      seenMarkers.add(marker);
    } while (marker);
    const commits = new Map([...records].filter(([name]) => /^\d{20}\.json$/.test(name)));
    const pointer = records.get('_last_checkpoint');
    if (pointer) {
      await guard();
      const [data, headers] = await azure.blob('GET', azure.c.data, `${prefix}_last_checkpoint`, undefined, undefined,
        { 'If-Match': pointer, Range: 'bytes=0-65535' }, false, true);
      requireEvidence(data && data.length > 0 && data.length < 65536 && headers.etag === pointer &&
        (headers[':status'] === '200' || (headers[':status'] === '206' && headers['content-range'] === `bytes 0-${data.length - 1}/${data.length}`)),
      'Original Delta checkpoint pointer could not be downloaded completely');
      const pointedVersion = uint(evidenceObject(parseEvidence(new TextDecoder('utf-8', { fatal: true }).decode(data))).version,
        'Delta checkpoint version');
      if (pointedVersion > newestCheckpoint) newestCheckpoint = pointedVersion;
    }
    requireEvidence(newestCheckpoint < BigInt(commits.size), 'A Delta checkpoint extends beyond the complete JSON history');
    return { records, commits };
  }
  const before = await inventory();
  requireEvidence(Number.isSafeInteger(baseline.version) && baseline.version >= 0 && before.commits.size > baseline.version + 1,
    'Frozen Delta history does not extend its baseline');
  const evidence: CapturedEvidence[] = [];
  const replay = directDeltaReplay();
  let total = 0;
  for (let version = 0; version < before.commits.size; version++) {
    const name = `${String(version).padStart(20, '0')}.json`, etag = before.commits.get(name);
    requireEvidence(etag, 'Frozen Delta JSON history has a version gap');
    await guard();
    const [data, headers] = await azure.blob('GET', azure.c.data, prefix + name, undefined, undefined,
      { 'If-Match': etag, Range: `bytes=0-${MAX_COMMIT_BYTES - 1}` }, false, true);
    requireEvidence(data && headers.etag === etag && data.length > 0 && data.length < MAX_COMMIT_BYTES &&
      (headers[':status'] === '200' || (headers[':status'] === '206' && headers['content-range'] === `bytes 0-${data.length - 1}/${data.length}`)),
    'Original Delta JSON could not be downloaded completely');
    total += data.length;
    requireEvidence(total <= 256 * 1024 ** 2, 'Delta JSON evidence exceeded its custody byte limit');
    const path = join(directory, name);
    const hash = sha(data), tableId = replay.apply(data);
    if (version === baseline.version) requireEvidence(hash === baseline.last_commit_sha256 && tableId === baseline.table_id,
      'Original pre-load Delta baseline no longer matches');
    await writeFile(path, data, { mode: 0o600, flag: 'wx', flush: true });
    evidence.push({ key: `delta-${name}`, path, bytes: data.length, sha256: hash });
  }
  const snapshot = replay.snapshot;
  const physical = [];
  for (const file of snapshot.active_files) {
    await guard();
    const [, headers] = await azure.blob('HEAD', azure.c.data, `${tableRoot}/${file.path}`, undefined, undefined, {}, false, true);
    requireEvidence(headers[':status'] === '200' && headers['x-ms-request-id'] && strong(headers.etag) &&
      headers['content-length'] === String(file.size), 'A physical active Delta file is missing or differs in size');
    physical.push({ path: file.path, size: file.size, etag: headers.etag });
  }
  const after = await inventory();
  requireEvidence(before.records.size === after.records.size &&
    [...before.records].every(([name, version]) => after.records.get(name) === version),
    'Delta log changed during terminal capture');
  await guard();
  return { snapshot, evidence, physical };
}
