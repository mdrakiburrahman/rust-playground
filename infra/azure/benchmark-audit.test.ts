import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { auditEvidenceFiles, evidenceJson } from './benchmark-audit.js';
import { parseEvidence } from './benchmark-evidence-json.js';
import { auditManifest, decodeFacts, decodedRecords, ENCODING, MAX_RECORDS } from './benchmark-manifest-audit.js';
import { debugList, reconcilePublications, someUint } from './benchmark-reconcile.js';

const fact = (count: bigint, bytes: bigint) => ({
  decoded_physical_batch_count: count, canonical_logical_bytes: bytes, canonical_logical_bytes_unavailable_reason: null,
});
const facts = (count = 4n) => ({
  encoding: ENCODING, bit_order: 'least_significant_index_bits_first', index_count: count,
  index_bits: count === 4n ? 2n : 0n,
  dictionary: count === 4n ? [fact(1n, 100n), fact(2n, 200n), fact(3n, 300n)] : [fact(1n, 20n)],
  indices_base64url: count === 4n ? 'JA' : '',
});
function manifest(count = 1n) {
  const many = count === 4n;
  return {
    schema: 'arrowcol.logical_batch_manifest.v1', version: 1n, identity_status: 'identity_bearing',
    eligible_for_identity_reconciliation: true, complete: true, truncated: false, record_capacity_reached: false,
    batch_facts_encoding: ENCODING, run_uuid: '1'.repeat(32), incarnation_uuid: '2'.repeat(32),
    measurement_bounds: { start_unix_ms_inclusive: 1000n, end_unix_ms_exclusive: 601000n },
    record_count: count, record_cap: MAX_RECORDS,
    prepared_logical_batches: count, attempted_logical_batches: count, acknowledged_logical_batches: count,
    not_admitted_logical_batches: 0n, ambiguous_logical_batches: 0n, failed_logical_batches: 0n,
    pending_logical_batches: 0n, retry_attempts: 0n,
    ranges: [{ table: 'synthetic', worker: 0n, first_sequence: many ? 34n : 0n, last_sequence: many ? 37n : 0n,
      batch_count: count, rows_per_batch: many ? 10n : 2n, cohort: 'measurement', attempts: 1n, outcome: 'acknowledged',
      ...fact(many ? 7n : 1n, many ? 700n : 20n), first_send_unix_ns: 2_000_000_000n, last_send_unix_ns: 2_000_000_000n,
      first_admitted_unix_ms: 2000n, last_admitted_unix_ms: 2000n, first_admitted_cohort: 'measurement',
      first_outcome_unix_ms: 2000n, last_outcome_unix_ms: 2000n, batch_facts: facts(count) }],
  };
}
const physical = (n: number) => String(n).repeat(32);
const app = (n: number) => `arrowcol.publication.${physical(n)}`;
const receipt = (n = 3, name = 'a.parquet', ordinal = 0n, version = 1, timestamp = '1970-01-01T00:00:03Z') => ({
  timestamp, fields: {
    message: 'Delta logical publication audit receipt', logical_publication_id: `${physical(1)}/${physical(2)}/0/0`,
    physical_publication_id: physical(n), fragment_ordinal: ordinal, input_rows: 2n, canonical_logical_bytes: 'Some(20)',
    admitted_at_unix_ms: 2000n, qualification_state: 'committed', delta_outcome: 'Committed',
    authoritative_delta_version: `Some(${version})`, committed_files: `["0:${name}"]`,
    transaction_files: `[("${app(n)}", 1, 2000, [0])]`,
  },
});
const txn = (n = 3, updated = 2000n) => ({ txn: { appId: app(n), version: 1n, lastUpdated: updated } });
const add = (name = 'a.parquet', rows = 2, dataChange = true) => ({
  add: { path: `hour=0/${name}`, size: 40n, dataChange, stats: `{"numRecords":${rows}}` },
});
const history = (...commits: unknown[][]) => new Map([[0n, []], ...commits.map((actions, i) => [BigInt(i + 1), actions] as const)]);
const compare = (m = [manifest()], events = [receipt()], delta = history([txn(), add()])) =>
  reconcilePublications(m, events, delta, 0n);
function fragmented() {
  const m = manifest();
  Object.assign(m.ranges[0], { rows_per_batch: 4n, ...fact(2n, 40n) });
  m.ranges[0].batch_facts.dictionary = [fact(2n, 40n)];
  return m;
}

test('decodes exact fact order and base36 logical identity', () => {
  const records = [...decodedRecords(manifest(4n))];
  assert.deepEqual(records.map(r => [r.id.split('/').at(-1), r.canonical]), [['y', 100n], ['z', 200n], ['10', 300n], ['11', 100n]]);
  assert.equal(auditManifest(manifest(4n)).cohorts['measurement->measurement'].rows, 40n);
});
test('rejects overlapping logical identities across tables', () => {
  const m = manifest(4n);
  m.ranges.push({ ...m.ranges[0], table: 'other' });
  Object.assign(m, { record_count: 8n, prepared_logical_batches: 8n, attempted_logical_batches: 8n, acknowledged_logical_batches: 8n });
  assert.throws(() => auditManifest(m), /Overlapping/);
});
test('rejects changed facts, cohorts, retries and wrong scalar types', () => {
  for (const change of [ { canonical_logical_bytes: 701n }, { decoded_physical_batch_count: 8n }, { cohort: 'warmup' },
    { first_admitted_cohort: 'post_window' }, { attempts: 2n }, { rows_per_batch: true } ]) {
    const m = manifest(4n);
    Object.assign(m.ranges[0], change);
    assert.throws(() => auditManifest(m));
  }
});
test('rejects noncanonical or inconsistent fact shape', () => {
  for (const change of [{ indices_base64url: 'JA==' }, { indices_base64url: 'JAA' }, { indices_base64url: 'Jw' },
    { index_count: 3n }, { index_bits: 3n }]) assert.throws(() => [...decodeFacts({ ...facts(), ...change }, 4)]);
});
test('supports zero-width packed indexes and rejects nonzero pad bits', () => {
  assert.deepEqual([...decodeFacts({ ...facts(1n), index_count: 4n }, 4)], Array(4).fill({ fragments: 1n, canonical: 20n }));
  const x = { ...facts(), dictionary: facts().dictionary.slice(0, 2), index_count: 2n, index_bits: 1n, indices_base64url: 'Ag' };
  assert.deepEqual([...decodeFacts(x, 2)].map(f => f.canonical), [100n, 200n]);
  assert.throws(() => [...decodeFacts({ ...x, indices_base64url: 'gg' }, 2)], /padding/);
});
test('rejects unavailable facts and admission crossing an exclusive window bound', () => {
  const m = manifest(4n);
  Object.assign(m.ranges[0].batch_facts.dictionary[0], { canonical_logical_bytes: null });
  assert.throws(() => auditManifest(m));
  const crossed = manifest();
  Object.assign(crossed.ranges[0], { last_admitted_unix_ms: 601000n, last_outcome_unix_ms: 601000n });
  assert.throws(() => auditManifest(crossed), /cohort/);
});
test('rejects escaped duplicate JSON keys and never rounds u64', () => {
  assert.throws(() => parseEvidence('{"outer":{"rows":1,"r\\u006fws":2}}'), /Duplicate JSON/);
  const text = '{"n":18446744073709551615}';
  assert.deepEqual(evidenceJson(parseEvidence(text)).replace(/\s/g, ''), text);
  const m = manifest();
  const first = 1795000000000999999n, ms = first / 1_000_000n;
  Object.assign(m.measurement_bounds, { start_unix_ms_inclusive: ms, end_unix_ms_exclusive: ms + 1n });
  Object.assign(m.ranges[0], { first_send_unix_ns: first, last_send_unix_ns: first,
    first_admitted_unix_ms: ms, last_admitted_unix_ms: ms, first_outcome_unix_ms: ms, last_outcome_unix_ms: ms });
  assert.equal(auditManifest(parseEvidence(evidenceJson(m))).cohorts['measurement->measurement'].rows, 2n);
});
test('joins exact rows and bytes without crediting warmup', async () => {
  const first = await compare();
  assert.deepEqual([first.scored_rows, first.scored_canonical_bytes, first.all_run_physical_bytes], [2n, 20n, 40n]);
  const m = manifest();
  Object.assign(m.ranges[0], { first_send_unix_ns: 500000000n, last_send_unix_ns: 500000000n, cohort: 'warmup' });
  const second = await compare([m]);
  assert.equal(second.scored_rows, 0n);
  assert.equal(second.excluded['warmup:in_window'].rows, 2n);
});
test('reconciles all drain fragments but excludes the exclusive boundary', async () => {
  const result = await compare([fragmented()], [receipt(), receipt(4, 'b.parquet', 1n, 2, '1970-01-01T00:10:01Z')],
    history([txn(), add()], [txn(4), add('b.parquet')]));
  assert.equal(result.scored_rows, 2n);
  assert.equal(result.excluded['measurement:after_window'].rows, 2n);
});
test('uses transaction tuple field 3 as admission, independent of txn.lastUpdated', async () => {
  const delta = history([txn(3, 2940n), add()]);
  assert.equal((await compare(undefined, undefined, delta)).scored_rows, 2n);
  const wrong = receipt();
  wrong.fields.transaction_files = `[("${app(3)}", 1, 2940, [0])]`;
  await assert.rejects(compare(undefined, [wrong], delta), /ingestion stamp/);
});
test('credits a coalesced commit at its latest observation without double-counting physical bytes', async () => {
  const m = manifest(2n);
  Object.assign(m.ranges[0], { last_sequence: 1n, ...fact(2n, 40n) });
  const first = receipt(3, undefined, undefined, undefined, '1970-01-01T00:01:00.999Z');
  const second = receipt(4, undefined, undefined, undefined, '1970-01-01T00:01:01.001Z');
  second.fields.logical_publication_id = `${physical(1)}/${physical(2)}/0/1`;
  const result = await compare([m], [first, second], history([txn(), add('a.parquet', 4), txn(4)]));
  assert.deepEqual([result.all_run_physical_bytes, ...result.minutes.slice(0, 2).map(m => m.rows)], [40n, 0n, 4n]);
});
test('fails on duplicate evidence or a missing original receipt', async () => {
  await assert.rejects(compare(undefined, [receipt(), receipt()]), /Duplicate physical/);
  await assert.rejects(compare(undefined, []), /Missing parent/);
  await assert.rejects(compare([manifest(), manifest()]), /Duplicate logical identity/);
});
test('rejects mismatched rows, canonical bytes, versions and transaction IDs', async () => {
  for (const change of [{ input_rows: 3n }, { canonical_logical_bytes: 'Some(21)' }, { authoritative_delta_version: 'Some(2)' },
    { transaction_files: '[("wrong", 1, 2000, [0])]' }]) {
    const event = receipt();
    Object.assign(event.fields, change);
    await assert.rejects(compare(undefined, [event]));
  }
});
test('rejects missing history, transaction replay, extra adds and changing removals', async () => {
  for (const delta of [new Map([[1n, [txn(), add()]]]), new Map([[0n, [txn()]], [1n, [txn(), add()]]]),
    history([txn(), add(), add('b.parquet')]), history([txn(), add(), { remove: { dataChange: true } }]),
    history([txn(), add('a.parquet', 3)]), history([txn(), add(), txn(4)])]) {
    await assert.rejects(compare(undefined, undefined, delta));
  }
});
test('rejects different window bounds and unresolved or deduplicated publications', async () => {
  const m = manifest();
  m.measurement_bounds.end_unix_ms_exclusive++;
  await assert.rejects(compare([manifest(), m]), /share/);
  for (const state of ['deduplicated', 'partial_commit', 'unresolved_reconciliation']) {
    const event = receipt();
    event.fields.qualification_state = state;
    await assert.rejects(compare(undefined, [event]), /unresolved/);
  }
});
test('does not credit old/reused paths or dataChange:false maintenance', async () => {
  await assert.rejects(compare([fragmented()], [receipt(), receipt(4, undefined, 1n, 2)],
    history([txn(), add()], [txn(4), add()])), /added more than once/);
  for (const change of [true, false]) {
    await assert.rejects(compare(undefined, undefined,
      new Map([[0n, [add('a.parquet', 2, change)]], [1n, [txn(), add()]]])), /added more than once/);
  }
  const safe = await compare(undefined, undefined, history([txn(), add()], [add('a.parquet', 2, false), { remove: { dataChange: false } }]));
  assert.deepEqual([safe.scored_rows, safe.all_run_physical_bytes], [2n, 40n]);
});
test('rejects executable debug expressions and noncanonical debug Option integers', () => {
  for (const text of ['[open("secret")]', '[1+1]', '[x for x in []]', '[{}]']) assert.throws(() => debugList(text));
  for (const text of ['None', 'Some(-1)', 'Some(True)', 'Some(01)']) assert.throws(() => someUint(text, 'fixture'));
});
test('hashes the original evidence bytes and does not produce a result on audit failure', async () => {
  const root = await mkdtemp(join(tmpdir(), 'benchmark-audit-'));
  try {
    const dir = join(root, 'delta'), manifestPath = join(root, 'client.json'), collectorPath = join(root, 'collector.jsonl');
    await mkdir(dir);
    const oneLine = (value: unknown) => evidenceJson(value).replaceAll('\n', ' ');
    const input = {
      [manifestPath]: evidenceJson(manifest()), [collectorPath]: oneLine(receipt()) + '\r\n',
      [join(dir, '00000000000000000000.json')]: '{"protocol":{"minReaderVersion":1}}\n',
      [join(dir, '00000000000000000001.json')]: [txn(), add()].map(oneLine).join('\n'),
    };
    await Promise.all(Object.entries(input).map(([path, value]) => writeFile(path, value)));
    const result = await auditEvidenceFiles([manifestPath], collectorPath, dir, 0n);
    assert.deepEqual({ ...result.evidence_sha256 }, Object.fromEntries(Object.entries(input).map(
      ([path, value]) => [path, createHash('sha256').update(value).digest('hex')])));
    await writeFile(collectorPath, input[collectorPath].repeat(2));
    await assert.rejects(auditEvidenceFiles([manifestPath], collectorPath, dir, 0n), /Duplicate physical/);
    const raw = 'startup\nOAT_CLIENT_METRICS ' + oneLine({ phase: 'completed', logical_batch_manifest: manifest() }) + '\n';
    await writeFile(manifestPath, raw);
    await writeFile(collectorPath, 'startup\n' + input[collectorPath]);
    const direct = await auditEvidenceFiles([manifestPath], collectorPath, dir, 0n, { expectedFinalVersion: 1n });
    assert.equal(direct.scored_rows, 2n);
    assert.ok('client_log_sha256' in direct);
    assert.equal(direct.client_log_sha256[manifestPath], createHash('sha256').update(raw).digest('hex'));
    await assert.rejects(auditEvidenceFiles([manifestPath], collectorPath, dir, 0n, { expectedFinalVersion: 2n }), /frozen final/);
    await writeFile(manifestPath, raw.replace('"completed"', '"failed"'));
    await assert.rejects(auditEvidenceFiles([manifestPath], collectorPath, dir, 0n, { expectedFinalVersion: 1n }), /terminal failure/);
  } finally { await rm(root, { force: true, recursive: true }); }
});
